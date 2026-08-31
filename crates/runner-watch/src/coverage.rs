//! Endpoint coverage analysis (finding #2).
//!
//! Golden replay only checks endpoints that some scenario happens to exercise.
//! This module makes the gap measurable: it cross-references the endpoints the
//! official runner actually calls across the committed golden corpus against the
//! routes the preloop server implements, and reports implemented-but-untested
//! protocol surface.
//!
//! The verdict is advisory by default (`--strict` turns uncovered runner-facing
//! routes into a hard failure) because route-parameter normalisation is
//! best-effort: axum `:param`/`*wild` placeholders and golden `{n}`/`{guid}`
//! tokens are both collapsed to `{p}`, and nested-router prefixes are not
//! composed. An allowlist file suppresses routes that are intentionally
//! untested.

use std::collections::BTreeSet;
use std::path::Path;

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
    "/api/v3",
    "/replay",
    "/oidc",
    "/.well-known",
];

/// Outcome of a coverage comparison.
#[derive(Debug, Clone, Default)]
pub struct CoverageReport {
    /// Runner-facing implemented routes exercised by at least one golden.
    pub covered: Vec<String>,
    /// Runner-facing implemented routes no golden exercises.
    pub uncovered_impl: Vec<String>,
    /// Golden endpoints that match no implemented route (normalisation gaps or
    /// genuinely missing routes — worth investigating either way).
    pub golden_without_route: Vec<String>,
}

fn guid_re() -> &'static Regex {
    use std::sync::LazyLock;
    static RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
            .expect("static regex")
    });
    &RE
}

fn canon_segment(seg: &str) -> String {
    if seg.is_empty() {
        return String::new();
    }
    let is_param = seg.starts_with(':')
        || seg.starts_with('*')
        || (seg.starts_with('{') && seg.ends_with('}'));
    let is_num = seg.chars().all(|c| c.is_ascii_digit());
    if is_param || is_num || guid_re().is_match(seg) {
        "{p}".to_owned()
    } else {
        seg.to_owned()
    }
}

/// Collapse a route or golden path to a comparable canonical form: query string
/// dropped, and every parameter / numeric / GUID segment mapped to `{p}`.
pub fn canonicalize(path: &str) -> String {
    let base = path.split('?').next().unwrap_or(path);
    base.split('/')
        .map(canon_segment)
        .collect::<Vec<_>>()
        .join("/")
}

/// True when a canonical path belongs to the runner↔server protocol surface.
pub fn is_runner_facing(canonical: &str) -> bool {
    canonical.contains("/_apis")
        || RUNNER_FACING_PREFIXES
            .iter()
            .any(|p| canonical.starts_with(p))
}

/// Extract the literal route paths registered in a server router source file.
/// `&format!(...)`-built routes are intentionally skipped; in the preloop router
/// those are all native `/api/v1/debug/...` admin routes, never runner-facing.
pub fn implemented_routes(routes_src: &Path) -> Result<BTreeSet<String>> {
    let text = std::fs::read_to_string(routes_src)
        .with_context(|| format!("read {}", routes_src.display()))?;
    let re = Regex::new(r#"\.route(?:_service)?\(\s*"([^"]+)""#).expect("static regex");
    Ok(re
        .captures_iter(&text)
        .map(|c| canonicalize(&c[1]))
        .collect())
}

/// Canonical set of endpoint paths the golden corpus for `version` exercises.
pub fn golden_paths(golden_root: &Path, version: &str) -> Result<BTreeSet<String>> {
    let dir = golden_root.join(format!("v{}", version.trim_start_matches('v')));
    let mut out = BTreeSet::new();
    if !dir.exists() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(&dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        for key in crate::compare::load_endpoint_keys(&entry.path())? {
            // keys are "METHOD /path"; coverage compares paths.
            let path = key.split_once(' ').map(|(_, p)| p).unwrap_or(&key);
            out.insert(canonicalize(path));
        }
    }
    Ok(out)
}

/// Compute coverage of the runner-facing implemented routes by the golden corpus.
pub fn compute(routes_src: &Path, golden_root: &Path, version: &str) -> Result<CoverageReport> {
    let impl_all = implemented_routes(routes_src)?;
    let impl_runner: BTreeSet<String> = impl_all
        .iter()
        .filter(|r| is_runner_facing(r))
        .cloned()
        .collect();
    let golden = golden_paths(golden_root, version)?;
    // Goldens capture traffic to every host the runner touches (GitHub codeload,
    // package mirrors, git, blob storage). Restrict coverage to the runner↔server
    // protocol surface so those external calls don't drown the signal.
    let golden_runner: BTreeSet<String> = golden
        .iter()
        .filter(|p| is_runner_facing(p))
        .cloned()
        .collect();

    let covered = impl_runner.intersection(&golden_runner).cloned().collect();
    let uncovered_impl = impl_runner.difference(&golden_runner).cloned().collect();
    let golden_without_route = golden_runner.difference(&impl_all).cloned().collect();
    Ok(CoverageReport {
        covered,
        uncovered_impl,
        golden_without_route,
    })
}

/// Render a human-readable markdown coverage report.
pub fn render_markdown(report: &CoverageReport, version: &str) -> String {
    let total = report.covered.len() + report.uncovered_impl.len();
    let pct = if total == 0 {
        100.0
    } else {
        report.covered.len() as f64 * 100.0 / total as f64
    };
    let mut s = String::new();
    s.push_str(&format!("# Endpoint coverage: v{version}\n\n"));
    s.push_str(&format!(
        "Runner-facing routes covered: **{}/{} ({:.0}%)**\n\n",
        report.covered.len(),
        total,
        pct
    ));
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
        assert_eq!(
            canonicalize("/api/v1/actions/download/:owner/:repo/*git_ref"),
            "/api/v1/actions/download/{p}/{p}/{p}"
        );
    }

    #[test]
    fn runner_facing_classification() {
        assert!(is_runner_facing("/broker/{p}/acquirejob"));
        assert!(is_runner_facing("/{p}/_apis/v1/oauth2/token"));
        assert!(is_runner_facing("/twirp/foo/Bar"));
        assert!(!is_runner_facing("/api/v1/runs/{p}"));
        assert!(!is_runner_facing("/healthz"));
        assert!(!is_runner_facing("/metrics"));
    }

    #[test]
    fn implemented_routes_extracts_literals_and_skips_format() {
        let dir = std::env::temp_dir().join(format!(
            "rw-cov-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let src = dir.join("routes.rs");
        std::fs::write(
            &src,
            r##"
            let r = Router::new()
                .route("/broker/:runner_id/acquirejob", post(x))
                .route("/_apis/v1/AgentPools", get(y))
                .route(&format!("{DEBUG_SESSIONS_PATH}/:session_id"), get(z));
            "##,
        )
        .unwrap();
        let routes = implemented_routes(&src).unwrap();
        assert!(routes.contains("/broker/{p}/acquirejob"));
        assert!(routes.contains("/_apis/v1/AgentPools"));
        // format!-built route is skipped (native admin, never runner-facing).
        assert!(!routes.iter().any(|r| r.contains("session_id")));
    }

    #[test]
    fn compute_flags_uncovered_and_unmatched() {
        // Two implemented runner-facing routes; golden exercises only one, plus
        // an endpoint with no matching route.
        let dir = std::env::temp_dir().join(format!(
            "rw-cov2-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let golden = dir.join("v9.9.9").join("01-scn");
        std::fs::create_dir_all(&golden).unwrap();
        std::fs::write(
            golden.join("flows.jsonl"),
            "{\"method\":\"POST\",\"path\":\"/broker/1/acquirejob\"}\n\
             {\"method\":\"GET\",\"path\":\"/_apis/mystery/endpoint\"}\n",
        )
        .unwrap();
        let src = dir.join("routes.rs");
        std::fs::write(
            &src,
            r#"
            let r = Router::new()
                .route("/broker/:runner_id/acquirejob", post(x))
                .route("/_apis/v1/Message/:pool_id", get(y));
            "#,
        )
        .unwrap();
        let report = compute(&src, &dir, "9.9.9").unwrap();
        assert!(report
            .covered
            .contains(&"/broker/{p}/acquirejob".to_owned()));
        assert!(report
            .uncovered_impl
            .contains(&"/_apis/v1/Message/{p}".to_owned()));
        assert!(report
            .golden_without_route
            .contains(&"/_apis/mystery/endpoint".to_owned()));
    }
}
