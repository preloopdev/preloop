//! File commands — GITHUB_ENV, GITHUB_PATH, GITHUB_OUTPUT, GITHUB_STATE, GITHUB_STEP_SUMMARY,
//! GITHUB_ARTIFACTS, GITHUB_ARTIFACTS_LIST.
//!
//! Before each step, create empty temp files and export the env vars.
//! After the step, parse the files and apply the values.
//!
//! `$GITHUB_ARTIFACTS` / `$GITHUB_ARTIFACTS_LIST` match actions/runner v2.336.0
//! (`CreateArtifactsFileCommand` / `ArtifactsListFileCommand`).

use anyhow::{bail, Context, Result};
use regex::Regex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tracing::debug;

/// Paths to the file command temp files for a step.
pub struct FileCommandPaths {
    pub env_file: PathBuf,
    pub path_file: PathBuf,
    pub output_file: PathBuf,
    pub state_file: PathBuf,
    pub summary_file: PathBuf,
    /// v2.336.0 (#4527): Write-side file for artifact subject declarations.
    pub artifacts_file: PathBuf,
    /// v2.336.0 (#4527): Read-side file populated with aggregated artifact subjects JSON.
    pub artifacts_list_file: PathBuf,
}

/// Create temp files for file commands and return the paths.
///
/// When `job` is provided, `$GITHUB_ARTIFACTS_LIST` is pre-populated with the
/// current job-scoped aggregate (official `PopulateInitialContents`).
pub fn create_file_commands(temp_dir: &Path) -> Result<FileCommandPaths> {
    create_file_commands_with_job(temp_dir, None)
}

/// Like [`create_file_commands`], optionally seeding `$GITHUB_ARTIFACTS_LIST`.
pub fn create_file_commands_with_job(
    temp_dir: &Path,
    job: Option<&super::contexts::JobContext>,
) -> Result<FileCommandPaths> {
    std::fs::create_dir_all(temp_dir)?;

    let paths = FileCommandPaths {
        env_file: temp_dir.join(format!("github_env_{}", uuid::Uuid::new_v4())),
        path_file: temp_dir.join(format!("github_path_{}", uuid::Uuid::new_v4())),
        output_file: temp_dir.join(format!("github_output_{}", uuid::Uuid::new_v4())),
        state_file: temp_dir.join(format!("github_state_{}", uuid::Uuid::new_v4())),
        summary_file: temp_dir.join(format!("github_step_summary_{}", uuid::Uuid::new_v4())),
        artifacts_file: temp_dir.join(format!("github_artifacts_{}", uuid::Uuid::new_v4())),
        artifacts_list_file: temp_dir
            .join(format!("github_artifacts_list_{}", uuid::Uuid::new_v4())),
    };

    for path in [
        &paths.env_file,
        &paths.path_file,
        &paths.output_file,
        &paths.state_file,
        &paths.summary_file,
        &paths.artifacts_file,
        &paths.artifacts_list_file,
    ] {
        std::fs::write(path, "")?;
    }

    // Official always creates the list file path; contents only when feature on.
    if let Some(job) = job {
        if artifacts_feature_enabled(job) {
            write_artifacts_list_file(&paths.artifacts_list_file, job)?;
        }
    }

    Ok(paths)
}

fn artifacts_feature_enabled(job: &super::contexts::JobContext) -> bool {
    job.variables
        .get("actions_runner_allow_artifacts_file")
        .and_then(|v| v.get("value"))
        .and_then(|v| v.as_str())
        .is_some_and(matches_official_bool)
        || std::env::var("ACTIONS_RUNNER_ALLOW_ARTIFACTS_FILE")
            .is_ok_and(|v| matches_official_bool(&v))
}

fn matches_official_bool(v: &str) -> bool {
    // StringUtil.ConvertToBoolean / GetBoolean truthy set used by the runner.
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "t" | "y" | "yes" | "on"
    )
}

fn write_artifacts_list_file(path: &Path, job: &super::contexts::JobContext) -> Result<()> {
    // Official ArtifactsListFileCommand: sort by name, compact JSON, UTF-8 no BOM.
    let mut subjects: Vec<_> = job.artifact_subjects.values().collect();
    subjects.sort_by(|a, b| a.name.cmp(&b.name));
    let subjects_json: Vec<serde_json::Value> = subjects
        .into_iter()
        .map(|s| {
            serde_json::json!({
                "name": s.name,
                "digest": s.digest,
                "kind": s.kind,
            })
        })
        .collect();
    let list_json = serde_json::json!({
        "version": 1,
        "subjects": subjects_json,
    });
    std::fs::write(path, serde_json::to_string(&list_json)?)
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

/// Get the env vars to export for file commands.
pub fn file_command_env(paths: &FileCommandPaths) -> HashMap<String, String> {
    let mut env = HashMap::new();
    env.insert(
        "GITHUB_ENV".to_string(),
        paths.env_file.to_string_lossy().to_string(),
    );
    env.insert(
        "GITHUB_PATH".to_string(),
        paths.path_file.to_string_lossy().to_string(),
    );
    env.insert(
        "GITHUB_OUTPUT".to_string(),
        paths.output_file.to_string_lossy().to_string(),
    );
    env.insert(
        "GITHUB_STATE".to_string(),
        paths.state_file.to_string_lossy().to_string(),
    );
    env.insert(
        "GITHUB_STEP_SUMMARY".to_string(),
        paths.summary_file.to_string_lossy().to_string(),
    );
    // v2.336.0 (#4527): Artifact file commands (feature-flagged)
    env.insert(
        "GITHUB_ARTIFACTS".to_string(),
        paths.artifacts_file.to_string_lossy().to_string(),
    );
    env.insert(
        "GITHUB_ARTIFACTS_LIST".to_string(),
        paths.artifacts_list_file.to_string_lossy().to_string(),
    );
    env
}

/// Parse a file containing `KEY=VALUE` lines and `KEY<<DELIM...DELIM` heredocs.
pub fn parse_kv_file(path: &Path) -> Result<HashMap<String, String>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;

    let mut result = HashMap::new();
    let mut index = 0;

    fn read_line(text: &str, index: &mut usize) -> (Option<String>, Option<String>) {
        if *index >= text.len() {
            return (None, None);
        }

        let original_index = *index;
        if let Some(lf_offset) = text[*index..].find('\n') {
            let lf_index = *index + lf_offset;

            // Check for CRLF
            if lf_offset > 0 && text.as_bytes()[lf_index - 1] == b'\r' {
                let cr_lf_index = lf_index - 1;
                *index = lf_index + 1;
                let line = text[original_index..cr_lf_index].to_string();
                return (Some(line), Some("\r\n".to_string()));
            }

            *index = lf_index + 1;
            let line = text[original_index..lf_index].to_string();
            (Some(line), Some("\n".to_string()))
        } else {
            *index = text.len();
            let line = text[original_index..].to_string();
            (Some(line), None)
        }
    }

    while let (Some(line), _) = read_line(&text, &mut index) {
        if line.is_empty() {
            continue;
        }

        let equals_index = line.find('=');
        let heredoc_index = line.find("<<");

        // Normal style NAME=VALUE
        if let Some(eq_pos) = equals_index {
            if heredoc_index.is_none() || eq_pos < heredoc_index.unwrap() {
                let key = line[..eq_pos].to_string();
                let value = line[eq_pos + 1..].to_string();
                if key.is_empty() {
                    anyhow::bail!("Invalid format '{}'. Name must not be empty", line);
                }
                result.insert(key, value);
                continue;
            }
        }

        // Heredoc style NAME<<EOF
        if let Some(heredoc_pos) = heredoc_index {
            if equals_index.is_none() || heredoc_pos < equals_index.unwrap() {
                let key = line[..heredoc_pos].to_string();
                let delimiter = line[heredoc_pos + 2..].to_string();
                if key.is_empty() || delimiter.is_empty() {
                    anyhow::bail!("Invalid format '{}'. Name must not be empty and delimiter must not be empty", line);
                }

                let start_index = index;
                let mut end_index = index;

                loop {
                    let (temp_line, newline) = read_line(&text, &mut index);
                    let Some(t_line) = &temp_line else {
                        anyhow::bail!("Invalid value. Matching delimiter not found '{}' (missing heredoc delimiter)", delimiter);
                    };

                    if t_line == &delimiter {
                        break;
                    }

                    let Some(nl) = &newline else {
                        anyhow::bail!("Invalid value. EOF marker missing new line.");
                    };

                    end_index = index - nl.len();
                }

                let output = if end_index > start_index {
                    text[start_index..end_index].to_string()
                } else {
                    "".to_string()
                };

                result.insert(key, output);
                continue;
            }
        }

        anyhow::bail!("Invalid format '{}' (Invalid file command line)", line);
    }

    Ok(result)
}

/// Parse a path file (one path per line).
pub fn parse_path_file(path: &Path) -> Result<Vec<String>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;

    Ok(content
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .map(|l| l.to_string())
        .collect())
}

/// Apply file command results to the job context.
pub fn apply_file_commands(
    paths: &FileCommandPaths,
    step_id: &str,
    job: &mut super::contexts::JobContext,
) -> Result<()> {
    // Apply GITHUB_ENV. Match the official runner security block: NODE_OPTIONS
    // must not be set through GITHUB_ENV because it can alter runner-hosted
    // Node action execution.
    let env_vars = parse_kv_file(&paths.env_file)?;
    for (k, v) in env_vars {
        if k.eq_ignore_ascii_case("NODE_OPTIONS") {
            tracing::warn!(
                "Can't store NODE_OPTIONS output parameter using '$GITHUB_ENV' command."
            );
            continue;
        }
        debug!("GITHUB_ENV: {k}={v}");
        job.env.insert(k, v);
    }

    // GitHub Actions runner v2.335.1 AddPathFileCommand.ProcessCommand appends
    // each non-empty file line in read order. Preserve that order while keeping
    // newer file-command batches ahead of older additions.
    let extra_paths = parse_path_file(&paths.path_file)?;
    for p in extra_paths.into_iter().rev() {
        debug!("GITHUB_PATH: {p}");
        job.extra_path.insert(0, p);
    }

    // Apply GITHUB_OUTPUT
    let outputs = parse_kv_file(&paths.output_file)?;
    if let Some(step_result) = job.steps.get_mut(step_id) {
        for (k, v) in &outputs {
            step_result.outputs.insert(k.clone(), v.clone());
        }
    }

    // Apply GITHUB_STATE. Lifecycle synthetic steps are named __pre_<id> /
    // __post_<id>, but their state belongs to the original action step id so
    // post actions receive STATE_* values written by pre/main.
    let state = parse_kv_file(&paths.state_file)?;
    if !state.is_empty() {
        let state_step_id = step_id
            .strip_prefix("__pre_")
            .or_else(|| step_id.strip_prefix("__post_"))
            .unwrap_or(step_id);
        let step_state = job.state.entry(state_step_id.to_string()).or_default();
        for (k, v) in state {
            step_state.insert(k, v);
        }
    }

    // Summary: just check size (cap at 1MiB)
    if let Ok(metadata) = std::fs::metadata(&paths.summary_file) {
        if metadata.len() > 1_048_576 {
            tracing::warn!("Step summary exceeds 1MiB limit, truncating");
        }
    }

    // v2.336.0 CreateArtifactsFileCommand — feature-gated; env path always set.
    // Failures throw (fail the step), matching official ProcessCommand.
    if artifacts_feature_enabled(job) && paths.artifacts_file.exists() {
        process_artifacts_file(paths, job)?;
    }

    Ok(())
}

const ARTIFACTS_MAX_FILE_BYTES: u64 = 1024 * 1024;
const ARTIFACTS_MAX_AGGREGATE: usize = 500;

fn process_artifacts_file(
    paths: &FileCommandPaths,
    job: &mut super::contexts::JobContext,
) -> Result<()> {
    let meta = std::fs::metadata(&paths.artifacts_file)
        .with_context(|| format!("reading {}", paths.artifacts_file.display()))?;
    if meta.len() == 0 {
        return Ok(());
    }
    if meta.len() > ARTIFACTS_MAX_FILE_BYTES {
        bail!(
            "$GITHUB_ARTIFACTS file exceeds the maximum size of {} KiB (got {} KiB).",
            ARTIFACTS_MAX_FILE_BYTES / 1024,
            meta.len() / 1024
        );
    }

    let content = std::fs::read_to_string(&paths.artifacts_file)
        .with_context(|| format!("reading {}", paths.artifacts_file.display()))?;

    // Parse fully before mutating the aggregate (official: fail without partial pollution).
    let mut parsed: Vec<(usize, super::contexts::ArtifactSubject)> = Vec::new();
    for (idx, raw) in content.lines().enumerate() {
        let line_number = idx + 1;
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        match parse_artifact_entry(trimmed, job.workspace.as_deref()) {
            Ok(subject) => parsed.push((line_number, subject)),
            Err(e) => {
                bail!("Invalid $GITHUB_ARTIFACTS entry on line {line_number}: {e}");
            }
        }
    }

    let mut added = 0usize;
    for (line_number, subject) in parsed {
        let name = subject.name.clone();
        if let Some(existing) = job.artifact_subjects.get(&name) {
            if existing.digest == subject.digest {
                // Identical declaration — silently deduplicate.
                continue;
            }
            bail!(
                "Invalid $GITHUB_ARTIFACTS entry on line {line_number}: Conflicting digest for artifact '{name}': previously declared as '{}', now declared as '{}'.",
                existing.digest,
                subject.digest
            );
        }
        if job.artifact_subjects.len() >= ARTIFACTS_MAX_AGGREGATE {
            bail!(
                "Invalid $GITHUB_ARTIFACTS entry on line {line_number}: The job has exceeded the maximum of {ARTIFACTS_MAX_AGGREGATE} declared artifacts."
            );
        }
        job.artifact_subjects.insert(name, subject);
        added += 1;
    }

    if added > 0 {
        tracing::info!(
            "Captured {added} artifact subject(s) from this step (job total: {})",
            job.artifact_subjects.len()
        );
    }

    // Next step's InitializeFiles rewrites the list; keep current file in sync too.
    write_artifacts_list_file(&paths.artifacts_list_file, job)?;
    Ok(())
}

/// Apply GITHUB_ENV and GITHUB_PATH from file commands to the job context.
/// Used between composite steps so env changes propagate to subsequent steps.
/// Does NOT apply outputs or state — those are handled by the composite handler.
pub fn apply_file_commands_to_job(paths: &FileCommandPaths, job: &mut super::contexts::JobContext) {
    // GitHub Actions runner v2.335.1 SetEnvFileCommand.ProcessCommand applies
    // the NODE_OPTIONS block list case-insensitively, including embedded steps.
    if let Ok(env_vars) = parse_kv_file(&paths.env_file) {
        for (k, v) in env_vars {
            if k.eq_ignore_ascii_case("NODE_OPTIONS") {
                tracing::warn!(
                    "Can't store NODE_OPTIONS output parameter using '$GITHUB_ENV' command."
                );
                continue;
            }
            debug!("GITHUB_ENV (composite): {k}={v}");
            job.env.insert(k, v);
        }
    }
    // Preserve AddPathFileCommand's file order for composite steps too.
    if let Ok(extra_paths) = parse_path_file(&paths.path_file) {
        for p in extra_paths.into_iter().rev() {
            debug!("GITHUB_PATH (composite): {p}");
            job.extra_path.insert(0, p);
        }
    }
}

/// Clean up file command temp files.
pub fn cleanup_file_commands(paths: &FileCommandPaths) {
    for path in [
        &paths.env_file,
        &paths.path_file,
        &paths.output_file,
        &paths.state_file,
        &paths.summary_file,
        &paths.artifacts_file,
        &paths.artifacts_list_file,
    ] {
        let _ = std::fs::remove_file(path);
    }
}

/// Parse one `$GITHUB_ARTIFACTS` line (official `CreateArtifactsFileCommand.ParseLine`).
///
/// Formats:
/// - `file://path` or bare path → file subject (sha256 of contents)
/// - `oci://ref@sha256|384|512:hex` or bare `ref@sha{256,384,512}:hex` → oci subject
fn parse_artifact_entry(
    line: &str,
    workspace: Option<&str>,
) -> Result<super::contexts::ArtifactSubject> {
    let trimmed = line.trim();
    if trimmed.contains('=') {
        bail!("entries containing '=' are reserved and not permitted");
    }

    static SCHEME_RE: OnceLock<Regex> = OnceLock::new();
    static OCI_RE: OnceLock<Regex> = OnceLock::new();
    let scheme_re = SCHEME_RE
        .get_or_init(|| Regex::new(r"^[A-Za-z][A-Za-z0-9+.\-]*://").expect("scheme regex"));
    let oci_re = OCI_RE.get_or_init(|| {
        Regex::new(r"^(?P<ref>.+)@(?P<algo>sha(?:256|384|512)):(?P<hex>[0-9a-fA-F]+)$")
            .expect("oci digest regex")
    });

    if let Some(path) = trimmed
        .strip_prefix("file://")
        .or_else(|| trimmed.strip_prefix("FILE://"))
    {
        if path.trim().is_empty() {
            bail!("file:// entries must include a path");
        }
        return make_file_subject(path, workspace);
    }
    if let Some(rest) = trimmed
        .strip_prefix("oci://")
        .or_else(|| trimmed.strip_prefix("OCI://"))
    {
        let Some(caps) = oci_re.captures(rest) else {
            bail!("oci:// entries must include an @sha{{256,384,512}}:<hex> digest");
        };
        return make_oci_subject(&caps);
    }
    if scheme_re.is_match(trimmed) {
        bail!("unsupported URI scheme");
    }
    if let Some(caps) = oci_re.captures(trimmed) {
        let algo = caps.name("algo").unwrap().as_str();
        let hex = caps.name("hex").unwrap().as_str();
        if expected_hex_len(algo) == Some(hex.len()) {
            return make_oci_subject(&caps);
        }
    }
    make_file_subject(trimmed, workspace)
}

fn expected_hex_len(algo: &str) -> Option<usize> {
    match algo.to_ascii_lowercase().as_str() {
        "sha256" => Some(64),
        "sha384" => Some(96),
        "sha512" => Some(128),
        _ => None,
    }
}

fn make_oci_subject(caps: &regex::Captures<'_>) -> Result<super::contexts::ArtifactSubject> {
    let ref_name = caps.name("ref").unwrap().as_str();
    let algo = caps.name("algo").unwrap().as_str().to_ascii_lowercase();
    let hex = caps.name("hex").unwrap().as_str().to_ascii_lowercase();
    let Some(expected) = expected_hex_len(&algo) else {
        bail!("unsupported digest algorithm");
    };
    if hex.len() != expected {
        bail!(
            "digest '{algo}' must be {expected} hex characters, got {}",
            hex.len()
        );
    }
    if ref_name.is_empty() {
        bail!("oci subject must include a reference");
    }
    Ok(super::contexts::ArtifactSubject {
        name: ref_name.to_string(),
        digest: format!("{algo}:{hex}"),
        kind: "oci".to_string(),
    })
}

fn make_file_subject(
    declared_path: &str,
    workspace: Option<&str>,
) -> Result<super::contexts::ArtifactSubject> {
    use sha2::Digest;

    let file_path = if Path::new(declared_path).is_absolute() {
        PathBuf::from(declared_path)
    } else {
        let base = workspace
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_default());
        base.join(declared_path)
    };

    if file_path.is_dir() {
        bail!("'{declared_path}' is a directory, not a regular file");
    }
    if !file_path.exists() {
        if !Path::new(declared_path).is_absolute() {
            let root = workspace.unwrap_or(".");
            bail!(
                "file '{declared_path}' does not exist (relative paths are resolved against the workspace root '{root}')"
            );
        }
        bail!("file '{declared_path}' does not exist");
    }

    let content = std::fs::read(&file_path)
        .with_context(|| format!("reading artifact file {}", file_path.display()))?;
    let digest = format!("sha256:{:x}", sha2::Sha256::digest(&content));
    let name = file_path
        .file_name()
        .map(|f| f.to_string_lossy().to_string())
        .unwrap_or_else(|| declared_path.to_string());

    Ok(super::contexts::ArtifactSubject {
        name,
        digest,
        kind: "file".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn parse_simple_kv() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("env");
        std::fs::write(&path, "FOO=bar\nBAZ=qux\n").unwrap();
        let result = parse_kv_file(&path).unwrap();
        assert_eq!(result.get("FOO").unwrap(), "bar");
        assert_eq!(result.get("BAZ").unwrap(), "qux");
    }

    #[test]
    fn parse_heredoc() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("env");
        std::fs::write(&path, "CERT<<EOF\nline1\nline2\nEOF\nSIMPLE=val\n").unwrap();
        let result = parse_kv_file(&path).unwrap();
        assert_eq!(result.get("CERT").unwrap(), "line1\nline2");
        assert_eq!(result.get("SIMPLE").unwrap(), "val");
    }

    #[test]
    fn parse_empty_values_and_multiple_values() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("env");
        std::fs::write(&path, "EMPTY=\nFOO=bar\nFOO=baz\n").unwrap();
        let result = parse_kv_file(&path).unwrap();
        assert_eq!(result.get("EMPTY").map(String::as_str), Some(""));
        assert_eq!(result.get("FOO").map(String::as_str), Some("baz"));
    }

    #[test]
    fn parse_heredoc_empty_value() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("env");
        std::fs::write(&path, "EMPTY<<EOF\nEOF\n").unwrap();
        let result = parse_kv_file(&path).unwrap();
        assert_eq!(result.get("EMPTY").map(String::as_str), Some(""));
    }

    #[test]
    fn parse_heredoc_requires_closing_delimiter() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("env");
        std::fs::write(&path, "BROKEN<<EOF\nunterminated\n").unwrap();
        let err = parse_kv_file(&path).unwrap_err();
        assert!(err.to_string().contains("missing heredoc delimiter"));
    }

    #[test]
    fn parse_rejects_invalid_lines() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("env");
        std::fs::write(&path, "not a command\n").unwrap();
        let err = parse_kv_file(&path).unwrap_err();
        assert!(err.to_string().contains("Invalid file command line"));
    }
    #[test]
    fn parse_path_file_lines() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("path");
        std::fs::write(&path, "/usr/local/bin\n/opt/bin\n").unwrap();
        let result = parse_path_file(&path).unwrap();
        assert_eq!(result, vec!["/usr/local/bin", "/opt/bin"]);
    }

    #[test]
    fn create_and_cleanup() {
        let dir = TempDir::new().unwrap();
        let paths = create_file_commands(dir.path()).unwrap();
        assert!(paths.env_file.exists());
        assert!(paths.output_file.exists());
        assert!(paths.artifacts_file.exists());
        assert!(paths.artifacts_list_file.exists());
        cleanup_file_commands(&paths);
        assert!(!paths.env_file.exists());
    }

    #[test]
    fn parse_artifact_oci_at_digest() {
        let s = parse_artifact_entry(
            "ghcr.io/octocat/myapp:1.0.0@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            None,
        )
        .unwrap();
        assert_eq!(s.name, "ghcr.io/octocat/myapp:1.0.0");
        assert_eq!(
            s.digest,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(s.kind, "oci");
    }

    #[test]
    fn parse_artifact_file_path() {
        let dir = TempDir::new().unwrap();
        let f = dir.path().join("binary.bin");
        std::fs::write(&f, b"hello").unwrap();
        let s = parse_artifact_entry("binary.bin", Some(dir.path().to_str().unwrap())).unwrap();
        assert_eq!(s.name, "binary.bin");
        assert_eq!(s.kind, "file");
        assert!(s.digest.starts_with("sha256:"));
    }

    #[test]
    fn parse_artifact_rejects_equals() {
        let err = parse_artifact_entry("name=value", None).unwrap_err();
        assert!(err.to_string().contains("reserved"));
    }

    #[test]
    fn artifacts_processing_requires_feature_flag() {
        let dir = TempDir::new().unwrap();
        let paths = create_file_commands(dir.path()).unwrap();
        let f = dir.path().join("a.bin");
        std::fs::write(&f, b"x").unwrap();
        std::fs::write(&paths.artifacts_file, "a.bin\n").unwrap();
        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        job.workspace = Some(dir.path().to_string_lossy().into());
        apply_file_commands(&paths, "step", &mut job).unwrap();
        assert!(job.artifact_subjects.is_empty());

        job.variables = serde_json::json!({
            "actions_runner_allow_artifacts_file": {"value": "true"}
        });
        apply_file_commands(&paths, "step", &mut job).unwrap();
        assert_eq!(job.artifact_subjects.len(), 1);
    }

    #[test]
    fn artifacts_conflict_fails_step() {
        let dir = TempDir::new().unwrap();
        let paths = create_file_commands(dir.path()).unwrap();
        std::fs::write(
            &paths.artifacts_file,
            "img@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n\
             img@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n",
        )
        .unwrap();
        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({
                "actions_runner_allow_artifacts_file": {"value": "true"}
            }),
            serde_json::json!({}),
        );
        let err = apply_file_commands(&paths, "step", &mut job).unwrap_err();
        assert!(err.to_string().contains("Conflicting digest"));
    }

    #[test]
    fn lifecycle_state_is_stored_under_original_step_id() {
        let dir = TempDir::new().unwrap();
        let paths = create_file_commands(dir.path()).unwrap();
        std::fs::write(&paths.state_file, "node_pre_case=alpha\n").unwrap();

        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );

        apply_file_commands(&paths, "__pre_main-action", &mut job).unwrap();

        assert_eq!(
            job.state
                .get("main-action")
                .and_then(|state| state.get("node_pre_case"))
                .map(String::as_str),
            Some("alpha")
        );
        assert!(!job.state.contains_key("__pre_main-action"));
    }

    #[test]
    fn github_env_blocks_node_options() {
        let dir = TempDir::new().unwrap();
        let paths = create_file_commands(dir.path()).unwrap();
        std::fs::write(&paths.env_file, "NODE_OPTIONS=--require bad.js\nOK=value\n").unwrap();

        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );

        apply_file_commands(&paths, "step", &mut job).unwrap();

        assert_eq!(job.env.get("OK").map(String::as_str), Some("value"));
        assert!(!job.env.contains_key("NODE_OPTIONS"));
    }
    #[test]
    fn parse_kv_file_gracefully_ignores_missing_file_or_directory() {
        let dir = TempDir::new().unwrap();
        // 1. Missing file
        let path = dir.path().join("does-not-exist");
        let result = parse_kv_file(&path).unwrap();
        assert!(result.is_empty());

        // 2. Missing directory
        let path = dir.path().join("missing-dir").join("env");
        let result = parse_kv_file(&path).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn parse_heredoc_missing_newline_error() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("env");
        std::fs::write(&path, "MY_ENV<<EOF line one line two line three EOF").unwrap();
        let err = parse_kv_file(&path).unwrap_err();
        assert!(err.to_string().contains("Matching delimiter not found"));
    }

    #[test]
    fn parse_heredoc_missing_newline_multiple_lines_error() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("env");
        std::fs::write(&path, "MY_ENV<<EOF line one\n                    line two\n                    line three EOF").unwrap();
        let err = parse_kv_file(&path).unwrap_err();
        assert!(err.to_string().contains("EOF marker missing new line"));
    }

    // --- P0 file command gap coverage ---

    #[test]
    fn parse_kv_equals_in_value() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("env");
        std::fs::write(&path, "CONN=host=localhost;port=5432\n").unwrap();
        let result = parse_kv_file(&path).unwrap();
        assert_eq!(
            result.get("CONN").map(String::as_str),
            Some("host=localhost;port=5432")
        );
    }

    #[test]
    fn parse_kv_unicode_value() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("env");
        std::fs::write(&path, "GREETING=こんにちは\nEMOJI=🎉\n").unwrap();
        let result = parse_kv_file(&path).unwrap();
        assert_eq!(
            result.get("GREETING").map(String::as_str),
            Some("こんにちは")
        );
        assert_eq!(result.get("EMOJI").map(String::as_str), Some("🎉"));
    }

    #[test]
    fn parse_kv_crlf_line_endings() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("env");
        std::fs::write(&path, "FOO=bar\r\nBAZ=qux\r\n").unwrap();
        let result = parse_kv_file(&path).unwrap();
        assert_eq!(result.get("FOO").map(String::as_str), Some("bar"));
        assert_eq!(result.get("BAZ").map(String::as_str), Some("qux"));
    }

    #[test]
    fn parse_heredoc_crlf_line_endings() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("env");
        std::fs::write(&path, "CERT<<EOF\r\nline1\r\nline2\r\nEOF\r\n").unwrap();
        let result = parse_kv_file(&path).unwrap();
        assert_eq!(
            result.get("CERT").map(String::as_str),
            Some("line1\r\nline2")
        );
    }

    #[test]
    fn parse_kv_empty_key_rejected() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("env");
        std::fs::write(&path, "=value\n").unwrap();
        let err = parse_kv_file(&path).unwrap_err();
        assert!(err.to_string().contains("Name must not be empty"));
    }

    #[test]
    fn parse_path_file_ignores_blank_lines() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("path");
        std::fs::write(&path, "/usr/bin\n\n  \n/opt/bin\n").unwrap();
        let result = parse_path_file(&path).unwrap();
        assert_eq!(result, vec!["/usr/bin", "/opt/bin"]);
    }

    #[test]
    fn apply_file_commands_attaches_outputs_and_prepends_path() {
        let dir = TempDir::new().unwrap();
        let paths = create_file_commands(dir.path()).unwrap();
        std::fs::write(&paths.output_file, "result=42\nstatus=ok\n").unwrap();
        std::fs::write(&paths.path_file, "/custom/bin\n").unwrap();
        std::fs::write(&paths.env_file, "MY_VAR=hello\n").unwrap();

        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        job.steps.insert(
            "step1".to_string(),
            crate::worker::contexts::StepResult {
                outcome: "Success".into(),
                conclusion: "Success".into(),
                outputs: std::collections::HashMap::new(),
            },
        );

        apply_file_commands(&paths, "step1", &mut job).unwrap();

        // Outputs attached to step
        let step = job.steps.get("step1").unwrap();
        assert_eq!(step.outputs.get("result").map(String::as_str), Some("42"));
        assert_eq!(step.outputs.get("status").map(String::as_str), Some("ok"));

        // Path prepended
        assert_eq!(
            job.extra_path.first().map(String::as_str),
            Some("/custom/bin")
        );

        // Env applied
        assert_eq!(job.env.get("MY_VAR").map(String::as_str), Some("hello"));
    }
    use proptest::prelude::*;
    use proptest::test_runner::RngSeed;
    use std::collections::HashMap;

    /// Bounded, reproducible generators use Proptest's default file failure
    /// persistence so a shrunk counterexample remains available between runs.
    fn file_command_config() -> ProptestConfig {
        ProptestConfig {
            cases: 1_000,
            rng_seed: RngSeed::Fixed(20250714),
            verbose: 1,
            ..ProptestConfig::default()
        }
    }

    fn arb_key() -> impl Strategy<Value = String> {
        prop::collection::vec(0u8..=25, 1..=8)
            .prop_map(|bytes| bytes.into_iter().map(|b| (b'A' + b) as char).collect())
    }

    fn arb_value() -> impl Strategy<Value = String> {
        prop::collection::vec(0u8..=5, 0..=24).prop_map(|bytes| {
            bytes
                .into_iter()
                .map(|b| match b {
                    0 => 'a',
                    1 => 'z',
                    2 => '=',
                    3 => '-',
                    4 => '0',
                    _ => '_',
                })
                .collect()
        })
    }

    fn arb_entries() -> impl Strategy<Value = Vec<(String, String)>> {
        prop::collection::vec((arb_key(), arb_value()), 1..=8)
    }

    fn new_job(step_ids: &[&str]) -> crate::worker::contexts::JobContext {
        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        for id in step_ids {
            job.steps.insert(
                (*id).to_string(),
                crate::worker::contexts::StepResult {
                    outcome: "Success".into(),
                    conclusion: "Success".into(),
                    outputs: HashMap::new(),
                },
            );
        }
        job
    }

    // Oracle: actions/runner v2.335.1 FileCommandManager.EnvFileKeyValuePairs
    // GetEnumerator, normal NAME=VALUE branch (pinned source lines 377-392):
    // https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/FileCommandManager.cs#L377-L392
    // Docs: environment files and file-command syntax:
    // https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-commands#environment-files
    proptest! {
        #![proptest_config(file_command_config())]

        #[test]
        fn key_value_preserves_every_equals_in_value((key, value) in (arb_key(), arb_value())) {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("env");
            std::fs::write(&path, format!("{key}={value}\n")).unwrap();

            let mut oracle = HashMap::new();
            oracle.insert(key, value);
            prop_assert_eq!(parse_kv_file(&path).unwrap(), oracle);
        }

        // Oracle: EnvFileKeyValuePairs yields pairs in file order and the
        // consuming dictionary assignment therefore makes the last duplicate win.
        // Same pinned source URL, GetEnumerator lines 377-451.
        #[test]
        fn duplicate_key_last_write_wins((key, values) in (arb_key(), prop::collection::vec(arb_value(), 1..=8))) {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("env");
            let text = values
                .iter()
                .map(|value| format!("{key}={value}\n"))
                .collect::<String>();
            std::fs::write(&path, text).unwrap();

            let mut oracle = HashMap::new();
            oracle.insert(key, values.last().cloned().unwrap());
            prop_assert_eq!(parse_kv_file(&path).unwrap(), oracle);
        }

        // Oracle: EnvFileKeyValuePairs.ReadLine preserves the exact newline
        // sequence in the heredoc payload (LF and CRLF), while matching only a
        // delimiter-only line (pinned source lines 407-438):
        // https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/FileCommandManager.cs#L407-L438
        // Docs: multiline environment-file values:
        // https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-commands#multiline-strings
        #[test]
        fn heredoc_preserves_lf_or_crlf_payload((key, lines, newline) in (
            arb_key(),
            prop::collection::vec(arb_value(), 0..=6),
            prop_oneof![Just("\n".to_string()), Just("\r\n".to_string())],
        )) {
            let delimiter = "EOF";
            let body = lines.join(&newline);
            let text = format!("{key}<<{delimiter}{newline}{body}{newline}{delimiter}{newline}");
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("env");
            std::fs::write(&path, text).unwrap();

            let mut oracle = HashMap::new();
            oracle.insert(key, body);
            prop_assert_eq!(parse_kv_file(&path).unwrap(), oracle);
        }

        // Oracle: EnvFileKeyValuePairs throws for a missing delimiter; this
        // production parser must surface an error, never panic (source lines
        // 431-438):
        // https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/FileCommandManager.cs#L431-L438
        #[test]
        fn missing_heredoc_delimiter_is_error_not_panic((key, lines, newline) in (
            arb_key(),
            prop::collection::vec(arb_value(), 0..=6),
            prop_oneof![Just("\n".to_string()), Just("\r\n".to_string())],
        )) {
            let mut text = format!("{key}<<EOF{newline}");
            for line in &lines {
                text.push_str(line);
                text.push_str(&newline);
            }
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("env");
            std::fs::write(&path, text).unwrap();

            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| parse_kv_file(&path)));
            prop_assert!(outcome.is_ok());
            prop_assert!(outcome.unwrap().is_err());
        }

        // Oracle: SetOutputFileCommand.ProcessCommand writes outputs on the
        // current step only (pinned source lines 331-350):
        // https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/FileCommandManager.cs#L331-L350
        // Docs: GITHUB_OUTPUT is the step output environment file:
        // https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-commands#setting-an-output-parameter
        #[test]
        fn output_is_attached_to_target_step_only(entries in arb_entries()) {
            let dir = TempDir::new().unwrap();
            let paths = create_file_commands(dir.path()).unwrap();
            let text = entries
                .iter()
                .map(|(key, value)| format!("{key}={value}\n"))
                .collect::<String>();
            std::fs::write(&paths.output_file, text).unwrap();

            let mut job = new_job(&["target", "other"]);
            job.steps
                .get_mut("other")
                .unwrap()
                .outputs
                .insert("untouched".into(), "keep".into());
            let oracle: HashMap<_, _> = entries.into_iter().collect();
            apply_file_commands(&paths, "target", &mut job).unwrap();

            prop_assert_eq!(&job.steps.get("target").unwrap().outputs, &oracle);
            prop_assert_eq!(
                job.steps.get("other").unwrap().outputs.get("untouched").map(String::as_str),
                Some("keep")
            );
        }

        // Oracle: SaveStateFileCommand stores embedded state under the original
        // action id, not synthetic __pre_/__post_ ids (pinned source lines 296-324):
        // https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/FileCommandManager.cs#L296-L324
        // Docs: GITHUB_STATE exposes values to the action's post step:
        // https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-commands#sending-values-to-the-pre-and-post-actions
        #[test]
        fn synthetic_pre_and_post_state_merges_under_original_action(
            (action, pre, post) in (arb_key(), arb_entries(), arb_entries())
        ) {
            let dir = TempDir::new().unwrap();
            let paths = create_file_commands(dir.path()).unwrap();
            let mut job = new_job(&[]);
            let mut oracle: HashMap<String, String> = HashMap::new();

            let pre_text = pre
                .iter()
                .map(|(key, value)| format!("{key}={value}\n"))
                .collect::<String>();
            std::fs::write(&paths.state_file, pre_text).unwrap();
            for (key, value) in pre {
                oracle.insert(key, value);
            }
            apply_file_commands(&paths, &format!("__pre_{action}"), &mut job).unwrap();

            let post_text = post
                .iter()
                .map(|(key, value)| format!("{key}={value}\n"))
                .collect::<String>();
            std::fs::write(&paths.state_file, post_text).unwrap();
            for (key, value) in post {
                oracle.insert(key, value);
            }
            apply_file_commands(&paths, &format!("__post_{action}"), &mut job).unwrap();

            prop_assert_eq!(job.state.get(&action), Some(&oracle));
            let pre_key = format!("__pre_{}", action);
            let post_key = format!("__post_{}", action);
            prop_assert!(!job.state.contains_key(&pre_key));
            prop_assert!(!job.state.contains_key(&post_key));
        }

        // Oracle: AddPathFileCommand.ProcessCommand reads non-empty lines and
        // appends them in file order (pinned source lines 113-145):
        // https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/FileCommandManager.cs#L113-L145
        // Docs: GITHUB_PATH adds entries to PATH:
        // https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-commands#adding-a-system-path
        #[test]
        fn path_entries_keep_official_file_order(paths_in in prop::collection::vec(arb_value(), 1..=8)) {
            let paths_in: Vec<String> = paths_in
                .into_iter()
                .enumerate()
                .map(|(index, path)| format!("/{index}-{path}"))
                .collect();
            let dir = TempDir::new().unwrap();
            let paths = create_file_commands(dir.path()).unwrap();
            std::fs::write(&paths.path_file, format!("{}\n", paths_in.join("\n"))).unwrap();
            let mut job = new_job(&[]);
            job.extra_path.push("/existing".into());

            apply_file_commands(&paths, "unused", &mut job).unwrap();
            let mut oracle = paths_in;
            oracle.push("/existing".into());
            prop_assert_eq!(job.extra_path, oracle);
        }

        // Oracle: SetEnvFileCommand.ProcessCommand compares the block list with
        // StringComparison.OrdinalIgnoreCase, and composite application must not
        // bypass that security rule (pinned source lines 157-209):
        // https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/FileCommandManager.cs#L157-L209
        // Docs: environment-file command names are case insensitive:
        // https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-commands#environment-files
        #[test]
        fn node_options_is_blocked_case_insensitively_in_normal_and_composite(
            blocked_key in prop_oneof![
                Just("NODE_OPTIONS".to_string()),
                Just("node_options".to_string()),
                Just("NoDe_OpTiOnS".to_string()),
            ], blocked_value in arb_value(), safe_value in arb_value()
        ) {
            let dir = TempDir::new().unwrap();
            let paths = create_file_commands(dir.path()).unwrap();
            std::fs::write(
                &paths.env_file,
                format!("{blocked_key}={blocked_value}\nSAFE={safe_value}\n"),
            )
            .unwrap();

            let mut normal = new_job(&["step"]);
            apply_file_commands(&paths, "step", &mut normal).unwrap();
            prop_assert_eq!(normal.env.get("SAFE").map(String::as_str), Some(safe_value.as_str()));
            prop_assert!(!normal.env.keys().any(|key| key.eq_ignore_ascii_case("NODE_OPTIONS")));

            let mut composite = new_job(&[]);
            apply_file_commands_to_job(&paths, &mut composite);
            prop_assert_eq!(composite.env.get("SAFE").map(String::as_str), Some(safe_value.as_str()));
            prop_assert!(!composite.env.keys().any(|key| key.eq_ignore_ascii_case("NODE_OPTIONS")));
        }
    }
}
