//! Endpoint coverage analysis (finding #2).
//!
//! Golden replay only checks endpoints that some scenario happens to exercise.
//! This module makes the gap measurable: it cross-references the endpoints the
//! official runner actually calls across the committed golden corpus against the
//! routes the preloop server implements, and reports implemented-but-untested
//! protocol surface.
//!
//! Both sides are reduced to `METHOD <canonical-path>` identities. Canonical
//! paths run through the *same* transport-prefix stripping as
//! [`crate::compare::normalize_path`] (via `strip_transport_prefixes`) so the
//! server's compatibility aliases (`/runner/server/…`, `/{org}/_apis/…`) line
//! up with the golden captures instead of showing as false gaps. Catch-all
//! routes (`*wild`) match by prefix. HTTP methods are preserved so distinct
//! operations on one path are not conflated.
//!
//! The verdict is advisory by default (`--strict` turns uncovered runner-facing
//! routes into a hard failure); an allowlist file suppresses routes that are
//! intentionally untested.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::LazyLock;

use anyhow::{Context, Result};
use regex::Regex;

/// Default prefixes considered part of the runner↔server protocol surface (as
/// opposed to preloop's native `/api/v1` admin API, the public UI, dispatch,
/// metrics, or GitHub-webhook endpoints, none of which the official runner
/// calls). A path is also treated as runner-facing when it contains `/_apis/`
/// anywhere, which catches the GHES org-prefixed (`/{org}/_apis/...`) forms.
pub const RUNNER_FACING_PREFIXES: &[&str] = &[
    "/_apis",
    "/runner",
    "/broker",
    "/twirp",
    "/session",
    "/message",
    "/acknowledge",
    "/actions/build",
    // Archive-ticket download the server hands to the runner (bearerless), so
    // its coverage regressions are worth detecting even though it lives under
    // the otherwise-native `/api/v1` prefix.
    "/api/v1/actions",
    "/api/v3",
    "/replay",
    "/oidc",
    "/.well-known",
];

/// HTTP method verbs recognised inside a route registration's handler chain.
const METHOD_VERBS: &[&str] = &[
    "get", "post", "put", "patch", "delete", "head", "options", "trace",
];

/// One implemented server route: an HTTP method (uppercase, or `*` when the
/// handler chain exposes no recognisable verb), a canonical path, and whether
/// the path was a catch-all (`*wild`) — in which case `path` is the fixed
/// prefix and matching is by prefix rather than exact equality.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ImplRoute {
    /// Uppercase HTTP method, or `*` for "any method".
    pub method: String,
    /// Canonical path (for catch-all routes, the fixed prefix).
    pub path: String,
    /// Whether this route was a `*wild` catch-all.
    pub catch_all: bool,
}

impl ImplRoute {
    /// `METHOD path` label for reports (catch-all shown with a trailing `/*`).
    pub fn label(&self) -> String {
        if self.catch_all {
            format!("{} {}/*", self.method, self.path)
        } else {
            format!("{} {}", self.method, self.path)
        }
    }

    /// Whether this route serves a golden `(method, path)` observation.
    fn covers(&self, golden_method: &str, golden_path: &str) -> bool {
        let method_ok = self.method == "*" || self.method == golden_method;
        let path_ok = if self.catch_all {
            // A root catch-all (`/{*path}` → prefix "/") matches everything;
            // otherwise match the prefix itself or any descendant, without
            // building a "//" prefix.
            self.path == "/"
                || golden_path == self.path
                || golden_path.starts_with(&format!("{}/", self.path))
        } else {
            golden_path == self.path
        };
        method_ok && path_ok
    }
}

/// Outcome of a coverage comparison. Each entry is a `METHOD path` label.
#[derive(Debug, Clone, Default)]
pub struct CoverageReport {
    /// Runner-facing implemented routes exercised by at least one golden.
    pub covered: Vec<String>,
    /// Runner-facing implemented routes no golden exercises.
    pub uncovered_impl: Vec<String>,
    /// Golden endpoints that match no implemented route (a real gap, or a
    /// normalisation mismatch — worth investigating either way).
    pub golden_without_route: Vec<String>,
}

static GUID_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
        .expect("static regex")
});

fn canon_segment(seg: &str) -> String {
    if seg.is_empty() {
        return String::new();
    }
    let is_param = seg.starts_with(':')
        || seg.starts_with('*')
        || (seg.starts_with('{') && seg.ends_with('}'));
    let is_num = seg.chars().all(|c| c.is_ascii_digit());
    if is_param || is_num || GUID_RE.is_match(seg) {
        "{p}".to_owned()
    } else {
        seg.to_owned()
    }
}

/// Collapse a route or golden path to a comparable canonical form: query
/// string dropped, transport prefixes (`/runner/server`, single random base
/// before `/_apis/`) stripped exactly as [`crate::compare::normalize_path`]
/// does, a leading org/base parameter before `/_apis/` removed, and every
/// remaining parameter / numeric / GUID segment mapped to `{p}`.
pub fn canonicalize(path: &str) -> String {
    let base = path.split('?').next().unwrap_or(path);
    let stripped = crate::compare::strip_transport_prefixes(base);
    let mut segs: Vec<String> = stripped.split('/').map(canon_segment).collect();
    // Drop a single leading *parameter* base segment before `/_apis` (the GHES
    // org prefix `/:org/_apis/…` on the route side, canonicalized to `{p}`).
    // Goldens already had their concrete base stripped by
    // `strip_transport_prefixes`, so only the parameter form remains — a
    // literal base (`/foo_bar/_apis/…`) is left intact rather than silently
    // collapsed onto the bare `/_apis/…` route.
    if segs.len() >= 3 && segs[2] == "_apis" && segs[1] == "{p}" {
        segs.remove(1);
    }
    segs.join("/")
}

/// True when a canonical path belongs to the runner↔server protocol surface.
pub fn is_runner_facing(canonical: &str) -> bool {
    canonical.contains("/_apis")
        || RUNNER_FACING_PREFIXES
            .iter()
            .any(|p| canonical.starts_with(p))
}

/// Reduce a route literal to its canonical path (or catch-all prefix) plus a
/// flag for whether it was a catch-all.
fn canonical_route(literal: &str) -> (String, bool) {
    let base = literal.split('?').next().unwrap_or(literal);
    let segs: Vec<&str> = base.split('/').collect();
    if let Some(idx) = segs
        .iter()
        .position(|s| s.starts_with('*') || s.starts_with("{*"))
    {
        let prefix = segs[..idx].join("/");
        let canon = canonicalize(&prefix);
        let canon = if canon.is_empty() {
            "/".to_owned()
        } else {
            canon
        };
        (canon, true)
    } else {
        (canonicalize(base), false)
    }
}

/// The first `"…"` string literal in `text` (respecting `\"` escapes).
fn first_string_literal(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let start = bytes.iter().position(|&b| b == b'"')?;
    let mut out = String::new();
    let mut esc = false;
    for &b in &bytes[start + 1..] {
        let c = b as char;
        if esc {
            out.push(c);
            esc = false;
        } else if c == '\\' {
            esc = true;
        } else if c == '"' {
            return Some(out);
        } else {
            out.push(c);
        }
    }
    None
}

/// Method verbs (uppercased) appearing as `verb(` in a route's handler chain.
fn parse_methods(body: &str) -> Vec<String> {
    let bytes = body.as_bytes();
    let mut out = Vec::new();
    for verb in METHOD_VERBS {
        let needle = format!("{verb}(");
        let mut from = 0;
        while let Some(rel) = body[from..].find(&needle) {
            let at = from + rel;
            let boundary_ok = at == 0 || {
                let prev = bytes[at - 1];
                !prev.is_ascii_alphanumeric() && prev != b'_'
            };
            if boundary_ok {
                out.push(verb.to_uppercase());
                break;
            }
            from = at + needle.len();
        }
    }
    out
}

/// Parse every `.route("…", …)` / `.route_service("…", …)` registration out of
/// a router source file into `ImplRoute`s. `&format!(…)`-built routes (whose
/// first literal does not start with `/`) are skipped — in the preloop router
/// those are all native `/api/v1/debug/…` admin routes, never runner-facing.
pub fn implemented_routes(routes_src: &Path) -> Result<BTreeSet<ImplRoute>> {
    let text = std::fs::read_to_string(routes_src)
        .with_context(|| format!("read {}", routes_src.display()))?;
    let bytes = text.as_bytes();
    let mut routes = BTreeSet::new();
    for kw in [".route(", ".route_service("] {
        let mut search = 0;
        while let Some(rel) = text[search..].find(kw) {
            let open = search + rel + kw.len() - 1; // index of '('
            search = open + 1;
            // Find the matching close paren, honouring string literals.
            let mut depth = 0i32;
            let mut end = None;
            let mut in_str = false;
            let mut esc = false;
            for (i, &b) in bytes.iter().enumerate().skip(open) {
                let c = b as char;
                if in_str {
                    if esc {
                        esc = false;
                    } else if c == '\\' {
                        esc = true;
                    } else if c == '"' {
                        in_str = false;
                    }
                } else {
                    match c {
                        '"' => in_str = true,
                        '(' => depth += 1,
                        ')' => {
                            depth -= 1;
                            if depth == 0 {
                                end = Some(i);
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            }
            let Some(end) = end else { break };
            let inner = &text[open + 1..end];
            let Some(literal) = first_string_literal(inner) else {
                continue;
            };
            if !literal.starts_with('/') {
                continue; // format!-built / non-literal route → native admin
            }
            let (path, catch_all) = canonical_route(&literal);
            let methods = parse_methods(inner);
            let methods = if methods.is_empty() {
                vec!["*".to_owned()]
            } else {
                methods
            };
            for method in methods {
                routes.insert(ImplRoute {
                    method,
                    path: path.clone(),
                    catch_all,
                });
            }
        }
    }
    Ok(routes)
}

/// Canonical `(METHOD, path)` set the golden corpus for `version` exercises.
pub fn golden_endpoints(golden_root: &Path, version: &str) -> Result<BTreeSet<(String, String)>> {
    let mut dir = golden_root.join(format!("v{}", version.trim_start_matches('v')));
    if !dir.exists() {
        dir = golden_root.join(version);
    }
    let mut out = BTreeSet::new();
    if !dir.exists() {
        return Ok(out);
    }
    let target_dir = if dir.join("gh-official").exists() {
        dir.join("gh-official")
    } else {
        dir
    };
    for entry in
        std::fs::read_dir(&target_dir).with_context(|| format!("read {}", target_dir.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        for key in crate::compare::load_endpoint_keys(&entry.path())? {
            // keys are "METHOD /path".
            if let Some((method, path)) = key.split_once(' ') {
                out.insert((method.to_uppercase(), canonicalize(path)));
            }
        }
    }
    Ok(out)
}

/// Compute coverage of the runner-facing implemented routes by the golden corpus.
pub fn compute(routes_src: &Path, golden_root: &Path, version: &str) -> Result<CoverageReport> {
    let impl_routes = implemented_routes(routes_src)?;
    let golden = golden_endpoints(golden_root, version)?;
    // Goldens capture traffic to every host the runner touches (GitHub codeload,
    // package mirrors, git, blob storage). Restrict coverage to the runner↔server
    // protocol surface so those external calls don't drown the signal.
    let golden_runner: Vec<(String, String)> = golden
        .iter()
        .filter(|(_, p)| is_runner_facing(p))
        .cloned()
        .collect();

    let mut covered = BTreeSet::new();
    let mut uncovered_impl = BTreeSet::new();
    for r in impl_routes.iter().filter(|r| is_runner_facing(&r.path)) {
        let hit = golden_runner.iter().any(|(gm, gp)| r.covers(gm, gp));
        if hit {
            covered.insert(r.label());
        } else {
            uncovered_impl.insert(r.label());
        }
    }

    let mut golden_without_route = BTreeSet::new();
    for (gm, gp) in &golden_runner {
        if !impl_routes.iter().any(|r| r.covers(gm, gp)) {
            golden_without_route.insert(format!("{gm} {gp}"));
        }
    }

    Ok(CoverageReport {
        covered: covered.into_iter().collect(),
        uncovered_impl: uncovered_impl.into_iter().collect(),
        golden_without_route: golden_without_route.into_iter().collect(),
    })
}

/// Render a human-readable markdown coverage report.
pub fn render_markdown(report: &CoverageReport, version: &str) -> String {
    let total = report.covered.len() + report.uncovered_impl.len();
    // Avoid a misleading "100%" on an empty route set, and a "vv…" title when
    // the caller passes an already-`v`-prefixed version.
    let coverage = if total == 0 {
        "0/0 (n/a)".to_owned()
    } else {
        format!(
            "{}/{} ({:.0}%)",
            report.covered.len(),
            total,
            report.covered.len() as f64 * 100.0 / total as f64
        )
    };
    let mut s = String::new();
    s.push_str(&format!(
        "# Endpoint coverage: v{}\n\n",
        version.trim_start_matches('v')
    ));
    s.push_str(&format!("Runner-facing routes covered: **{coverage}**\n\n"));
    s.push_str("## Uncovered runner-facing routes (implemented, no golden)\n\n");
    if report.uncovered_impl.is_empty() {
        s.push_str("_None._\n\n");
    } else {
        for r in &report.uncovered_impl {
            s.push_str(&format!("- `{r}`\n"));
        }
        s.push('\n');
    }
    s.push_str("## Golden endpoints with no matching route\n\n");
    if report.golden_without_route.is_empty() {
        s.push_str("_None._\n\n");
    } else {
        for r in &report.golden_without_route {
            s.push_str(&format!("- `{r}`\n"));
        }
        s.push('\n');
    }
    s.push_str("## Covered\n\n");
    for r in &report.covered {
        s.push_str(&format!("- `{r}`\n"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalize_collapses_params_digits_and_guids() {
        assert_eq!(
            canonicalize("/_apis/distributedtask/pools/:pool_id/agents"),
            "/_apis/distributedtask/pools/{p}/agents"
        );
        assert_eq!(
            canonicalize("/broker/{n}/acquirejob"),
            "/broker/{p}/acquirejob"
        );
        assert_eq!(
            canonicalize("/_apis/pipelines/workflows/42/artifacts?foo=bar"),
            "/_apis/pipelines/workflows/{p}/artifacts"
        );
    }

    #[test]
    fn canonicalize_unifies_transport_aliases() {
        // /runner/server prefix and GHES org prefix both collapse to the bare
        // /_apis form, matching golden normalization (finding #2 / review A1).
        assert_eq!(
            canonicalize("/runner/server/_apis/v1/AgentPools"),
            "/_apis/v1/AgentPools"
        );
        assert_eq!(
            canonicalize("/:org/_apis/v1/AgentPools"),
            "/_apis/v1/AgentPools"
        );
        assert_eq!(canonicalize("/_apis/v1/AgentPools"), "/_apis/v1/AgentPools");
    }

    #[test]
    fn root_catch_all_matches_descendants() {
        let r = ImplRoute {
            method: "GET".into(),
            path: "/".into(),
            catch_all: true,
        };
        assert!(r.covers("GET", "/anything"));
        assert!(r.covers("GET", "/a/b/c"));
        // Method is still enforced.
        assert!(!r.covers("POST", "/anything"));
    }

    #[test]
    fn runner_facing_classification() {
        assert!(is_runner_facing("/broker/{p}/acquirejob"));
        assert!(is_runner_facing("/_apis/v1/oauth2/token"));
        assert!(is_runner_facing("/twirp/foo/Bar"));
        assert!(!is_runner_facing("/api/v1/runs/{p}"));
        assert!(!is_runner_facing("/healthz"));
        assert!(!is_runner_facing("/metrics"));
    }

    #[test]
    fn implemented_routes_extracts_methods_and_skips_format() {
        let dir = tmp_dir("cov-impl");
        let src = dir.join("routes.rs");
        std::fs::write(
            &src,
            r##"
            let r = Router::new()
                .route("/_apis/artifactcache/cache", post(reserve))
                .route("/_apis/artifactcache/cache", get(lookup))
                .route("/broker/:runner_id/acquirejob", post(acquire))
                .route("/replay/results/*path", put(replay))
                .route(&format!("{DEBUG_SESSIONS_PATH}/:session_id"), get(z));
            "##,
        )
        .unwrap();
        let routes = implemented_routes(&src).unwrap();
        assert!(routes.contains(&ImplRoute {
            method: "POST".into(),
            path: "/_apis/artifactcache/cache".into(),
            catch_all: false
        }));
        assert!(routes.contains(&ImplRoute {
            method: "GET".into(),
            path: "/_apis/artifactcache/cache".into(),
            catch_all: false
        }));
        assert!(routes.contains(&ImplRoute {
            method: "PUT".into(),
            path: "/replay/results".into(),
            catch_all: true
        }));
        // format!-built route is skipped (native admin).
        assert!(!routes.iter().any(|r| r.path.contains("session_id")));
    }

    #[test]
    fn compute_matches_method_alias_and_catchall() {
        let dir = tmp_dir("cov-compute");
        let golden = dir.join("v9.9.9").join("01-scn");
        std::fs::create_dir_all(&golden).unwrap();
        std::fs::write(
            golden.join("flows.jsonl"),
            // Runner uses the /runner/server alias (A1), one method of a
            // two-method path (A2), a catch-all path (A3), and a genuinely
            // unserved endpoint.
            "{\"method\":\"GET\",\"path\":\"/runner/server/_apis/v1/AgentPools\"}\n\
             {\"method\":\"GET\",\"path\":\"/_apis/artifactcache/cache\"}\n\
             {\"method\":\"PUT\",\"path\":\"/replay/results/a/b/c\"}\n\
             {\"method\":\"GET\",\"path\":\"/_apis/mystery/endpoint\"}\n",
        )
        .unwrap();
        let src = dir.join("routes.rs");
        std::fs::write(
            &src,
            r#"
            let r = Router::new()
                .route("/_apis/v1/AgentPools", get(pools))
                .route("/_apis/artifactcache/cache", get(lookup))
                .route("/_apis/artifactcache/cache", post(reserve))
                .route("/replay/results/*path", put(replay));
            "#,
        )
        .unwrap();
        let report = compute(&src, &dir, "9.9.9").unwrap();
        // A1: /runner/server alias matches the bare route.
        assert!(report
            .covered
            .contains(&"GET /_apis/v1/AgentPools".to_owned()));
        // A2: GET on the cache path is covered, POST is not.
        assert!(report
            .covered
            .contains(&"GET /_apis/artifactcache/cache".to_owned()));
        assert!(report
            .uncovered_impl
            .contains(&"POST /_apis/artifactcache/cache".to_owned()));
        // A3: multi-segment path under the catch-all is covered.
        assert!(report.covered.contains(&"PUT /replay/results/*".to_owned()));
        // Genuinely unserved endpoint is reported.
        assert!(report
            .golden_without_route
            .contains(&"GET /_apis/mystery/endpoint".to_owned()));
    }

    fn tmp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rw-{tag}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
