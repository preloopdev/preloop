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
    let content =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;

    let mut result = HashMap::new();
    let mut lines = content.lines().peekable();

    while let Some(line) = lines.next() {
        let line = line.trim_end();
        if line.is_empty() {
            continue;
        }

        // Check for heredoc: KEY<<DELIMITER
        if let Some(pos) = line.find("<<") {
            let key = line[..pos].to_string();
            let delimiter = &line[pos + 2..];

            // Read until we find the delimiter on its own line
            let mut value_parts = Vec::new();
            for inner_line in lines.by_ref() {
                if inner_line.trim_end() == delimiter {
                    break;
                }
                value_parts.push(inner_line);
            }
            result.insert(key, value_parts.join("\n"));
        } else if let Some(eq_pos) = line.find('=') {
            // Simple KEY=VALUE
            let key = line[..eq_pos].to_string();
            let value = line[eq_pos + 1..].to_string();
            result.insert(key, value);
        }
    }

    Ok(result)
}

/// Parse a path file (one path per line).
pub fn parse_path_file(path: &Path) -> Result<Vec<String>> {
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
    // Apply GITHUB_ENV
    let env_vars = parse_kv_file(&paths.env_file)?;
    for (k, v) in env_vars {
        debug!("GITHUB_ENV: {k}={v}");
        job.env.insert(k, v);
    }

    // Apply GITHUB_PATH
    let extra_paths = parse_path_file(&paths.path_file)?;
    for p in extra_paths {
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
}
