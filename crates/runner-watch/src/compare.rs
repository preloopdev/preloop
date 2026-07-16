//! Rust port of `experiments/mitm/bin/_compare.py`.
//!
//! Reads two `flows.jsonl` capture directories, groups flows by normalized
//! endpoint, and writes a markdown comparison report. Replaces the Python
//! subprocess call in `run_compare` so the full conformance pipeline is
//! pure Rust with no Python dependency at test time.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;

use anyhow::{bail, Context, Result};
use regex::Regex;
use serde_json::Value;
use similar::TextDiff;

// ── header noise filter ──────────────────────────────────────────────────────

const IGNORED_HEADERS: &[&str] = &[
    "date",
    "server",
    "content-length",
    "x-request-id",
    "x-vss-e2eid",
    "x-msedge-ref",
];

// ── path normalisation ───────────────────────────────────────────────────────

/// Normalise volatile parts of a URL path + query string for grouping.
///
/// Matches the Python `normalize_path` function exactly:
/// - strip `/runner/server` prefix
/// - strip a single-segment random base before `/_apis/`
/// - replace GUIDs with `{guid}`
/// - replace all-digit path segments with `{n}`
/// - replace all-digit or GUID-prefixed query values with `{n}` / `{guid}`
pub fn normalize_path(path: &str) -> String {
    // Strip /runner/server prefix when immediately followed by /.
    let path = if path.starts_with("/runner/server/") {
        &path["/runner/server".len()..]
    } else {
        path
    };

    // Strip single-segment random base before /_apis/
    // e.g. /BFN7BKz.../_apis/... → /_apis/...
    // Must be exactly one path segment (no embedded slashes).
    let path = if let Some(rest) = path.strip_prefix('/') {
        if let Some(slash) = rest.find('/') {
            let seg = &rest[..slash];
            let tail = &rest[slash..]; // starts with /
                                       // Only strip if the tail continues with /_apis/ and the segment
                                       // is purely alphanumeric+hyphen (no dots — avoids stripping hostnames).
            if tail.starts_with("/_apis/")
                && !seg.is_empty()
                && seg.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
            {
                tail
            } else {
                path
            }
        } else {
            path
        }
    } else {
        path
    };

    // Replace GUIDs (8-4-4-4-12 hex digits).
    let guid_re =
        Regex::new(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
            .expect("static regex");
    let path = guid_re.replace_all(path, "{guid}");

    // Split into base path and query string.
    let (base, qs) = if let Some(q) = path.find('?') {
        (&path[..q], Some(&path[q + 1..]))
    } else {
        (path.as_ref(), None)
    };

    // Replace all-digit path segments with {n}.
    let normalised_base = base
        .split('/')
        .map(|seg| {
            if seg.chars().all(|c| c.is_ascii_digit()) && !seg.is_empty() {
                "{n}"
            } else {
                seg
            }
        })
        .collect::<Vec<_>>()
        .join("/");

    // Normalise query string.
    let normalised_qs = qs.map(|qs| {
        let params: Vec<String> = qs
            .split('&')
            .map(|part| {
                if let Some(eq) = part.find('=') {
                    let k = &part[..eq];
                    let v = &part[eq + 1..];
                    let nv = if v.chars().all(|c| c.is_ascii_digit()) && !v.is_empty() {
                        "{n}".to_owned()
                    } else if v.len() >= 8 && v[..8].chars().all(|c| c.is_ascii_hexdigit()) {
                        // GUID-prefixed query value.
                        "{guid}".to_owned()
                    } else {
                        v.to_owned()
                    };
                    format!("{k}={nv}")
                } else {
                    part.to_owned()
                }
            })
            .collect();
        params.join("&")
    });

    match normalised_qs {
        Some(qs) => format!("{normalised_base}?{qs}"),
        None => normalised_base,
    }
}

// ── redaction ────────────────────────────────────────────────────────────────

/// Redact JWTs and long high-entropy tokens from a report string.
pub fn redact_report(s: &str) -> String {
    // Redact JWT tokens (three base64url segments separated by dots).
    let jwt_re = Regex::new(r"eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}")
        .expect("static regex");
    let s = jwt_re.replace_all(s, "***REDACTED***");

    // Redact long alphanumeric strings with high character diversity.
    let long_re = Regex::new(r"[A-Za-z0-9_]{30,}").expect("static regex");
    long_re
        .replace_all(&s, |caps: &regex::Captures| {
            let m = &caps[0];
            let unique: HashSet<char> = m.chars().collect();
            if unique.len() > 6 {
                "***REDACTED***".to_owned()
            } else {
                m.to_owned()
            }
        })
        .into_owned()
}

// ── flow loading ─────────────────────────────────────────────────────────────

fn load_flows(dir: &Path) -> Result<Vec<Value>> {
    let path = dir.join("flows.jsonl");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).with_context(|| format!("parse flow: {l}")))
        .collect()
}

fn load_summary(dir: &Path) -> Value {
    let path = dir.join("summary.json");
    if !path.exists() {
        return Value::Object(Default::default());
    }
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or(Value::Object(Default::default()))
}

// ── grouping ─────────────────────────────────────────────────────────────────

type Groups = BTreeMap<String, Vec<Value>>;

fn group_flows(flows: &[Value]) -> Groups {
    let mut map: Groups = BTreeMap::new();
    for f in flows {
        let method = f.get("method").and_then(Value::as_str).unwrap_or("?");
        let path = f.get("path").and_then(Value::as_str).unwrap_or("/");
        let key = format!("{method} {}", normalize_path(path));
        map.entry(key).or_default().push(f.clone());
    }
    map
}

// ── label abbreviation ───────────────────────────────────────────────────────

fn short_label(label: &str) -> String {
    let cleaned = label.to_lowercase().replace(['-', '_'], " ");
    let parts: Vec<&str> = cleaned.split_whitespace().collect();
    if parts.len() == 1 {
        parts[0].chars().take(4).collect()
    } else {
        parts.iter().map(|p| &p[..1]).collect()
    }
}

// ── JSON pretty-print with sorted keys ──────────────────────────────────────

fn sorted_json(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut pairs: Vec<(String, Value)> = map
                .iter()
                .map(|(k, v)| (k.clone(), sorted_json(v)))
                .collect();
            pairs.sort_by(|(a, _), (b, _)| a.cmp(b));
            let mut out = serde_json::Map::new();
            for (k, v) in pairs {
                out.insert(k, v);
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(sorted_json).collect()),
        other => other.clone(),
    }
}

fn json_pretty_sorted(v: &Value) -> String {
    serde_json::to_string_pretty(&sorted_json(v)).unwrap_or_default()
}

// ── unified diff ─────────────────────────────────────────────────────────────

fn json_diff(a: &Value, b: &Value, left_label: &str, right_label: &str) -> String {
    let old = json_pretty_sorted(a);
    let new = json_pretty_sorted(b);
    if old == new {
        return String::new();
    }
    let diff = TextDiff::from_lines(&old, &new);
    diff.unified_diff()
        .header(left_label, right_label)
        .to_string()
}

/// Convert a JSON Value to a value-agnostic schema shape.
pub fn to_schema_value(val: &Value) -> Value {
    match val {
        Value::Null => Value::String("null".to_owned()),
        Value::Bool(_) => Value::String("boolean".to_owned()),
        Value::Number(_) => Value::String("number".to_owned()),
        Value::String(_) => Value::String("string".to_owned()),
        Value::Array(arr) => {
            let mut unique_schemas = Vec::new();
            for item in arr {
                let item_schema = to_schema_value(item);
                if !unique_schemas.contains(&item_schema) {
                    unique_schemas.push(item_schema);
                }
            }
            unique_schemas.sort_by_key(|schema| serde_json::to_string(schema).unwrap_or_default());
            Value::Array(unique_schemas)
        }
        Value::Object(map) => {
            let mut new_map = serde_json::Map::new();
            for (k, v) in map {
                new_map.insert(k.clone(), to_schema_value(v));
            }
            Value::Object(new_map)
        }
    }
}

fn json_schema_diff(a: &Value, b: &Value, left_label: &str, right_label: &str) -> String {
    let schema_a = to_schema_value(a);
    let schema_b = to_schema_value(b);
    json_diff(&schema_a, &schema_b, left_label, right_label)
}

// ── header key collection ────────────────────────────────────────────────────

fn header_keys(flows: &[Value]) -> BTreeSet<String> {
    let ignored: HashSet<&str> = IGNORED_HEADERS.iter().copied().collect();
    let mut keys = BTreeSet::new();
    for f in flows {
        for field in ["request_headers", "response_headers"] {
            if let Some(headers) = f.get(field).and_then(Value::as_array) {
                for pair in headers {
                    if let Some(arr) = pair.as_array() {
                        if arr.len() == 2 {
                            if let Some(name) = arr[0].as_str() {
                                let lower = name.to_lowercase();
                                if !ignored.contains(lower.as_str()) {
                                    keys.insert(lower);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    keys
}

// ── statistics helpers ───────────────────────────────────────────────────────

fn mean_ms(flows: &[Value]) -> Option<f64> {
    if flows.is_empty() {
        return None;
    }
    let sum: f64 = flows
        .iter()
        .map(|f| f.get("duration_ms").and_then(Value::as_f64).unwrap_or(0.0))
        .sum();
    Some(sum / flows.len() as f64)
}

fn percentile(mut vals: Vec<f64>, p: f64) -> f64 {
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((vals.len() as f64 * p) as usize).min(vals.len().saturating_sub(1));
    vals[idx]
}

fn durations(flows: &[Value]) -> Vec<f64> {
    flows
        .iter()
        .map(|f| f.get("duration_ms").and_then(Value::as_f64).unwrap_or(0.0))
        .collect()
}

fn statuses_sorted(flows: &[Value]) -> Vec<String> {
    let mut s: Vec<String> = flows
        .iter()
        .map(|f| match f.get("status") {
            Some(Value::Number(n)) => n.to_string(),
            Some(Value::String(s)) => s.clone(),
            Some(Value::Null) | None => "None".to_owned(),
            Some(other) => other.to_string(),
        })
        .collect();
    s.sort();
    s
}

// ── public entry point ───────────────────────────────────────────────────────

/// Arguments mirroring the Python CLI of `_compare.py`.
#[allow(missing_docs)]
pub struct Args<'a> {
    pub scenario: &'a str,
    pub left_dir: &'a Path,
    pub right_dir: &'a Path,
    pub output: &'a Path,
    pub left_label: &'a str,
    pub right_label: &'a str,
}

/// Generate a markdown comparison report between two `flows.jsonl` captures.
///
/// Output format is identical to the Python `_compare.py` script so that
/// `status_mismatch_in_report` in main.rs continues to work unchanged.
pub fn render_report(args: &Args) -> Result<()> {
    let left_flows = load_flows(args.left_dir)?;
    let right_flows = load_flows(args.right_dir)?;

    // Guard: fail if one side is empty while the other has data.
    if !left_flows.is_empty() && right_flows.is_empty() {
        bail!(
            "{} has {} flows but {} has none — cannot compare",
            args.left_label,
            left_flows.len(),
            args.right_label,
        );
    }
    if !right_flows.is_empty() && left_flows.is_empty() {
        bail!(
            "{} has {} flows but {} has none — cannot compare",
            args.right_label,
            right_flows.len(),
            args.left_label,
        );
    }

    let left_summary = load_summary(args.left_dir);
    let right_summary = load_summary(args.right_dir);

    let l_groups = group_flows(&left_flows);
    let r_groups = group_flows(&right_flows);

    let all_keys: BTreeSet<String> = l_groups.keys().chain(r_groups.keys()).cloned().collect();
    let left_only: Vec<&String> = l_groups
        .keys()
        .filter(|k| !r_groups.contains_key(*k))
        .collect();
    let right_only: Vec<&String> = r_groups
        .keys()
        .filter(|k| !l_groups.contains_key(*k))
        .collect();
    let shared: Vec<&String> = all_keys
        .iter()
        .filter(|k| l_groups.contains_key(*k) && r_groups.contains_key(*k))
        .collect();

    let ls = short_label(args.left_label);
    let rs = short_label(args.right_label);

    let mut lines: Vec<String> = Vec::new();

    // ── header ───────────────────────────────────────────────────────────────
    lines.push(format!("# MITM comparison: {}", args.scenario));
    lines.push(String::new());
    lines.push(format!(
        "**{}**: {} — {} flows",
        args.left_label,
        left_summary
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("N/A"),
        left_flows.len()
    ));
    lines.push(format!(
        "**{}**: {} — {} flows",
        args.right_label,
        right_summary
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("N/A"),
        right_flows.len()
    ));
    lines.push(String::new());

    // ── endpoint matrix ───────────────────────────────────────────────────────
    lines.push("## Endpoint matrix".to_owned());
    lines.push(String::new());
    lines.push(format!(
        "| method | normalized path | {ls} # | {rs} # | {ls} mean ms | {rs} mean ms | {ls} statuses | {rs} statuses |"
    ));
    lines.push("|---|---|---|---|---|---|---|---|".to_owned());

    for key in &all_keys {
        let (method, path) = key.split_once(' ').unwrap_or(("?", key.as_str()));
        let lo = l_groups.get(key).map(Vec::as_slice).unwrap_or(&[]);
        let ro = r_groups.get(key).map(Vec::as_slice).unwrap_or(&[]);
        let lc = lo.len();
        let rc = ro.len();
        let ld = mean_ms(lo)
            .map(|m| format!("{:.1}", m))
            .unwrap_or_else(|| "-".to_owned());
        let rd = mean_ms(ro)
            .map(|m| format!("{:.1}", m))
            .unwrap_or_else(|| "-".to_owned());
        let ls_ = statuses_sorted(lo).join(", ");
        let rs_ = statuses_sorted(ro).join(", ");
        lines.push(format!(
            "| {method} | `{path}` | {lc} | {rc} | {ld} | {rd} | {ls_} | {rs_} |"
        ));
    }
    lines.push(String::new());

    // ── missing endpoints ─────────────────────────────────────────────────────
    lines.push("## Missing endpoints".to_owned());
    lines.push(String::new());
    if left_only.is_empty() {
        lines.push(format!(
            "_No endpoints present only in {}._",
            args.left_label
        ));
        lines.push(String::new());
    } else {
        lines.push(format!("### {} only", args.left_label));
        lines.push(String::new());
        for key in &left_only {
            lines.push(format!("- `{key}`"));
        }
        lines.push(String::new());
    }
    if right_only.is_empty() {
        lines.push(format!(
            "_No endpoints present only in {}._",
            args.right_label
        ));
        lines.push(String::new());
    } else {
        lines.push(format!("### {} only", args.right_label));
        lines.push(String::new());
        for key in &right_only {
            lines.push(format!("- `{key}`"));
        }
        lines.push(String::new());
    }

    // ── per-endpoint comparison ───────────────────────────────────────────────
    if !shared.is_empty() {
        lines.push("## Per-endpoint comparison".to_owned());
        lines.push(String::new());

        for key in &shared {
            lines.push(format!("### `{key}`"));
            lines.push(String::new());

            let lo = l_groups.get(*key).map(Vec::as_slice).unwrap_or(&[]);
            let ro = r_groups.get(*key).map(Vec::as_slice).unwrap_or(&[]);

            // Header key differences.
            let lhk = header_keys(lo);
            let rhk = header_keys(ro);
            if lhk != rhk {
                lines.push("**Header key differences:**".to_owned());
                lines.push(String::new());
                let left_extra: BTreeSet<&String> = lhk.difference(&rhk).collect();
                let right_extra: BTreeSet<&String> = rhk.difference(&lhk).collect();
                if !left_extra.is_empty() {
                    lines.push(format!(
                        "- {} only: `{{{}}}`",
                        args.left_label,
                        left_extra
                            .iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                if !right_extra.is_empty() {
                    lines.push(format!(
                        "- {} only: `{{{}}}`",
                        args.right_label,
                        right_extra
                            .iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                lines.push(String::new());
            }

            // Request body diff (first flow only, matching Python).
            let o_req = lo.first().and_then(|f| f.get("request_body_json"));
            let r_req = ro.first().and_then(|f| f.get("request_body_json"));
            if o_req.is_some() || r_req.is_some() {
                lines.push("**Request body diff:**".to_owned());
                lines.push(String::new());
                let empty = Value::Object(Default::default());
                let a = o_req.unwrap_or(&empty);
                let b = r_req.unwrap_or(&empty);
                let diff = json_diff(a, b, args.left_label, args.right_label);
                if diff.is_empty() {
                    lines.push("_identical_".to_owned());
                } else {
                    lines.push("```diff".to_owned());
                    lines.push(diff.trim_end().to_owned());
                    lines.push("```".to_owned());
                }
                lines.push(String::new());

                lines.push("**Request body schema diff:**".to_owned());
                lines.push(String::new());
                let schema_diff = json_schema_diff(a, b, args.left_label, args.right_label);
                if schema_diff.is_empty() {
                    lines.push("_identical_".to_owned());
                } else {
                    lines.push("```diff".to_owned());
                    lines.push(schema_diff.trim_end().to_owned());
                    lines.push("```".to_owned());
                }
                lines.push(String::new());
            }

            // Response body diff (first flow only).
            let o_resp = lo.first().and_then(|f| f.get("response_body_json"));
            let r_resp = ro.first().and_then(|f| f.get("response_body_json"));
            if o_resp.is_some() || r_resp.is_some() {
                lines.push("**Response body diff:**".to_owned());
                lines.push(String::new());
                let empty = Value::Object(Default::default());
                let a = o_resp.unwrap_or(&empty);
                let b = r_resp.unwrap_or(&empty);
                let diff = json_diff(a, b, args.left_label, args.right_label);
                if diff.is_empty() {
                    lines.push("_identical_".to_owned());
                } else {
                    lines.push("```diff".to_owned());
                    lines.push(diff.trim_end().to_owned());
                    lines.push("```".to_owned());
                }
                lines.push(String::new());

                lines.push("**Response body schema diff:**".to_owned());
                lines.push(String::new());
                let schema_diff = json_schema_diff(a, b, args.left_label, args.right_label);
                if schema_diff.is_empty() {
                    lines.push("_identical_".to_owned());
                } else {
                    lines.push("```diff".to_owned());
                    lines.push(schema_diff.trim_end().to_owned());
                    lines.push("```".to_owned());
                }
                lines.push(String::new());
            }

            // Status codes.
            let os = statuses_sorted(lo);
            let rs = statuses_sorted(ro);
            lines.push(format!(
                "**Status codes:** {}: [{}] | {}: [{}]",
                args.left_label,
                os.join(", "),
                args.right_label,
                rs.join(", ")
            ));
            lines.push(String::new());

            // Timing.
            let ods = durations(lo);
            let rds = durations(ro);
            if !ods.is_empty() && !rds.is_empty() {
                let op50 = percentile(ods.clone(), 0.5);
                let op95 = percentile(ods, 0.95);
                let rp50 = percentile(rds.clone(), 0.5);
                let rp95 = percentile(rds, 0.95);
                lines.push(format!(
                    "**Timing (ms):** p50: {} {:.1} / {} {:.1} | p95: {} {:.1} / {} {:.1}",
                    args.left_label,
                    op50,
                    args.right_label,
                    rp50,
                    args.left_label,
                    op95,
                    args.right_label,
                    rp95,
                ));
            }
            lines.push(String::new());
        }
    } else {
        lines.push("_No shared endpoints to compare._".to_owned());
        lines.push(String::new());
    }

    let text = lines.join("\n");
    let text = redact_report(&text);

    if let Some(parent) = args.output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create dir {}", parent.display()))?;
    }
    std::fs::write(args.output, &text)
        .with_context(|| format!("write {}", args.output.display()))?;
    println!("report written to {}", args.output.display());
    Ok(())
}

// ── unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_strips_runner_server_prefix() {
        assert_eq!(
            normalize_path("/runner/server/_apis/distributedtask/pools/1/agents"),
            "/_apis/distributedtask/pools/{n}/agents"
        );
    }

    #[test]
    fn normalize_strips_single_segment_base() {
        assert_eq!(
            normalize_path("/BFN7BKz/_apis/distributedtask/pools/0/agents"),
            "/_apis/distributedtask/pools/{n}/agents"
        );
    }

    #[test]
    fn normalize_does_not_strip_multi_segment_base() {
        // /runner/server/ is multi-segment — handled by the first prefix strip above.
        // A path like /foo/bar/_apis/ has two segments; must not be stripped.
        let p = "/foo/bar/_apis/distributedtask";
        // The function only strips if there is exactly one segment before /_apis/.
        // /foo/bar/_apis/ starts with /foo, and tail is /bar/_apis/... which does
        // NOT start with /_apis/, so no stripping happens.
        assert!(normalize_path(p).contains("foo"));
    }

    #[test]
    fn normalize_replaces_guids() {
        let p = "/_apis/distributedtask/pools/1/sessions/a1b2c3d4-e5f6-7890-abcd-ef1234567890";
        let n = normalize_path(p);
        assert!(n.contains("{guid}"), "expected {{guid}} in {n}");
        assert!(!n.contains("a1b2c3d4"));
    }

    #[test]
    fn normalize_replaces_digit_segments() {
        assert_eq!(
            normalize_path("/_apis/distributedtask/pools/42/agents"),
            "/_apis/distributedtask/pools/{n}/agents"
        );
    }

    #[test]
    fn normalize_query_digit_values() {
        let p = "/_apis/distributedtask/pools/1/messages?sessionId=abc&waitSeconds=30";
        let n = normalize_path(p);
        assert!(n.contains("waitSeconds={n}"), "{n}");
        assert!(n.contains("sessionId=abc"), "{n}");
    }

    #[test]
    fn short_label_single_word() {
        assert_eq!(short_label("official"), "offi");
    }

    #[test]
    fn short_label_multi_word() {
        assert_eq!(short_label("runner-server"), "rs");
        assert_eq!(short_label("my_backend"), "mb");
    }

    #[test]
    fn redact_removes_jwt() {
        let jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c";
        let out = redact_report(jwt);
        assert_eq!(out, "***REDACTED***");
    }

    #[test]
    fn redact_preserves_short_strings() {
        let s = "hello world 200 OK";
        assert_eq!(redact_report(s), s);
    }

    #[test]
    fn json_diff_identical_returns_empty() {
        let v = serde_json::json!({"a": 1, "b": 2});
        assert!(json_diff(&v, &v, "left", "right").is_empty());
    }

    #[test]
    fn json_diff_detects_difference() {
        let a = serde_json::json!({"a": 1});
        let b = serde_json::json!({"a": 2});
        let d = json_diff(&a, &b, "left", "right");
        assert!(!d.is_empty());
        assert!(d.contains("-") || d.contains("+"));
    }

    #[test]
    fn statuses_sorted_handles_none() {
        let flows = vec![
            serde_json::json!({"status": 200}),
            serde_json::json!({}), // missing status → "None"
            serde_json::json!({"status": serde_json::Value::Null}),
        ];
        let s = statuses_sorted(&flows);
        assert_eq!(s, vec!["200", "None", "None"]);
    }
    #[test]
    fn schema_arrays_union_heterogeneous_context_pairs_order_insensitively() {
        let boolean_pair = serde_json::json!({
            "key": "enabled",
            "value": true,
        });
        let object_pair = serde_json::json!({
            "key": "metadata",
            "value": {"source": "runner"},
        });

        let first_order =
            serde_json::json!([boolean_pair.clone(), object_pair.clone(), boolean_pair,]);
        let reverse_order = serde_json::json!([
            object_pair,
            serde_json::json!({"key": "enabled", "value": false}),
        ]);

        let normalized = to_schema_value(&first_order);
        assert_eq!(normalized, to_schema_value(&reverse_order));
        assert_eq!(
            normalized,
            serde_json::json!([
                {"key": "string", "value": "boolean"},
                {"key": "string", "value": {"source": "string"}},
            ])
        );
    }
}
