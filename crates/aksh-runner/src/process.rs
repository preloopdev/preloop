//! Process invocation with process-group management.
//!
//! Wraps `command-group` for process-tree kill on cancel/timeout
//! without requiring `unsafe` code. Accepts a cancellation token
//! so that cancelled/timed-out steps actually kill their process tree.

use anyhow::{Context, Result};
use command_group::AsyncCommandGroup;
use std::collections::HashMap;
use std::path::Path;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::watch;

/// Result of a process invocation.
#[derive(Debug)]
pub struct ProcessOutput {
    /// Process exit code (0 = success).
    pub exit_code: i32,
    /// Collected stdout + stderr lines.
    pub lines: Vec<String>,
}

/// Callback for processing output lines.
pub type LineCallback = Box<dyn FnMut(&str) + Send>;

/// Invoke a process with the given environment, capturing output line by line.
///
/// If `cancel_rx` fires, the process group is killed and the function returns
/// an error. This ensures no orphaned processes after cancellation or timeout.
pub async fn invoke(
    program: &str,
    args: &[&str],
    cwd: &Path,
    env: &HashMap<String, String>,
    mut on_line: Option<LineCallback>,
    cancel_rx: Option<watch::Receiver<bool>>,
) -> Result<ProcessOutput> {
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(args)
        .current_dir(cwd)
        .envs(env)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    let mut child = cmd
        .group_spawn()
        .with_context(|| format!("spawning {program}"))?;

    let stdout = child.inner().stdout.take();
    let stderr = child.inner().stderr.take();

    // Spawn tasks to read stdout/stderr concurrently
    let stdout_handle = stdout.map(|s| {
        tokio::spawn(async move {
            let mut reader = BufReader::new(s).lines();
            let mut out = Vec::new();
            while let Ok(Some(line)) = reader.next_line().await {
                out.push(line);
            }
            out
        })
    });

    let stderr_handle = stderr.map(|s| {
        tokio::spawn(async move {
            let mut reader = BufReader::new(s).lines();
            let mut out = Vec::new();
            while let Ok(Some(line)) = reader.next_line().await {
                out.push(line);
            }
            out
        })
    });

    // Wait for process, racing against cancellation
    let status = if let Some(mut rx) = cancel_rx {
        tokio::select! {
            result = child.wait() => {
                result.with_context(|| format!("waiting for {program}"))?
            }
            _ = rx.changed() => {
                // Cancel received — kill the entire process group
                tracing::info!("Killing process group for {program} (cancelled)");
                if let Err(e) = child.kill().await {
                    tracing::warn!("Failed to kill process group: {e}");
                }
                // Reap the process after killing
                let _ = child.wait().await;

                // Still collect whatever output was produced
                let mut lines = Vec::new();
                if let Some(h) = stdout_handle {
                    lines.extend(h.await.unwrap_or_default());
                }
                if let Some(h) = stderr_handle {
                    lines.extend(h.await.unwrap_or_default());
                }
                for line in &lines {
                    if let Some(cb) = &mut on_line {
                        cb(line);
                    }
                }
                return Err(anyhow::anyhow!("process cancelled"));
            }
        }
    } else {
        child
            .wait()
            .await
            .with_context(|| format!("waiting for {program}"))?
    };

    let exit_code = status.code().unwrap_or(-1);

    // Collect output
    let mut lines = Vec::new();
    if let Some(h) = stdout_handle {
        let stdout_lines = h.await.unwrap_or_default();
        for line in &stdout_lines {
            if let Some(cb) = &mut on_line {
                cb(line);
            }
        }
        lines.extend(stdout_lines);
    }
    if let Some(h) = stderr_handle {
        let stderr_lines = h.await.unwrap_or_default();
        for line in &stderr_lines {
            if let Some(cb) = &mut on_line {
                cb(line);
            }
        }
        lines.extend(stderr_lines);
    }

    Ok(ProcessOutput { exit_code, lines })
}
