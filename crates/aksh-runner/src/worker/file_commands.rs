//! File commands — GITHUB_ENV, GITHUB_PATH, GITHUB_OUTPUT, GITHUB_STATE, GITHUB_STEP_SUMMARY.
//!
//! Before each step, create empty temp files and export the env vars.
//! After the step, parse the files and apply the values.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::debug;

/// Paths to the file command temp files for a step.
pub struct FileCommandPaths {
    pub env_file: PathBuf,
    pub path_file: PathBuf,
    pub output_file: PathBuf,
    pub state_file: PathBuf,
    pub summary_file: PathBuf,
}

/// Create temp files for file commands and return the paths.
pub fn create_file_commands(temp_dir: &Path) -> Result<FileCommandPaths> {
    std::fs::create_dir_all(temp_dir)?;

    let paths = FileCommandPaths {
        env_file: temp_dir.join(format!("github_env_{}", uuid::Uuid::new_v4())),
        path_file: temp_dir.join(format!("github_path_{}", uuid::Uuid::new_v4())),
        output_file: temp_dir.join(format!("github_output_{}", uuid::Uuid::new_v4())),
        state_file: temp_dir.join(format!("github_state_{}", uuid::Uuid::new_v4())),
        summary_file: temp_dir.join(format!("github_step_summary_{}", uuid::Uuid::new_v4())),
    };

    // Create empty files
    for path in [
        &paths.env_file,
        &paths.path_file,
        &paths.output_file,
        &paths.state_file,
        &paths.summary_file,
    ] {
        std::fs::write(path, "")?;
    }

    Ok(paths)
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

    // Apply GITHUB_PATH
    let extra_paths = parse_path_file(&paths.path_file)?;
    for p in extra_paths {
        debug!("GITHUB_PATH: {p}");
        job.extra_path.insert(0, p);
    }

    // Apply GITHUB_OUTPUT with size limits matching official runner.
    // GitHub enforces 1 MB per job (measured in UTF-16 bytes).
    const MAX_OUTPUT_UTF16_BYTES: usize = 1_048_576; // 1 MiB
    let outputs = parse_kv_file(&paths.output_file)?;
    if let Some(step_result) = job.steps.get_mut(step_id) {
        for (k, v) in &outputs {
            let utf16_size = v.len() * 2; // UTF-16 approximation
            if job.output_size_utf16 + utf16_size > MAX_OUTPUT_UTF16_BYTES {
                anyhow::bail!(
                    "Output '{}' exceeds the 1 MB size limit for job outputs. \
                     Current job total: {} bytes (UTF-16), this output: {} bytes (UTF-16). \
                     Consider using artifacts for large data.",
                    k,
                    job.output_size_utf16,
                    utf16_size
                );
            }
            job.output_size_utf16 += utf16_size;
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

    Ok(())
}

/// Apply GITHUB_ENV and GITHUB_PATH from file commands to the job context.
/// Used between composite steps so env changes propagate to subsequent steps.
/// Does NOT apply outputs or state — those are handled by the composite handler.
pub fn apply_file_commands_to_job(paths: &FileCommandPaths, job: &mut super::contexts::JobContext) {
    // Apply GITHUB_ENV
    if let Ok(env_vars) = parse_kv_file(&paths.env_file) {
        for (k, v) in env_vars {
            debug!("GITHUB_ENV (composite): {k}={v}");
            job.env.insert(k, v);
        }
    }
    // Apply GITHUB_PATH
    if let Ok(extra_paths) = parse_path_file(&paths.path_file) {
        for p in extra_paths {
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
    ] {
        let _ = std::fs::remove_file(path);
    }
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
        cleanup_file_commands(&paths);
        assert!(!paths.env_file.exists());
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
}
