//! Process invocation with process-group management.
//!
//! Wraps `command-group` for process-tree management. On cancel/timeout it
//! follows the official runner sequence: SIGINT grace, SIGTERM grace, then
//! SIGKILL, while still reaping the process group.

use anyhow::{Context, Result};
use command_group::{AsyncCommandGroup, AsyncGroupChild};
#[cfg(unix)]
use command_group::{Signal, UnixChildExt};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::watch;
use tokio::task::JoinHandle;
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
/// If `cancel_rx` fires, the process group is first given the same graceful
/// signal sequence as the official runner (SIGINT, SIGTERM, SIGKILL). The
/// function then returns an error. This ensures no orphaned processes after
/// cancellation or timeout while still allowing cleanup handlers to run.
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

    // Wait for process, racing against cancellation.
    let status = if let Some(mut rx) = cancel_rx {
        loop {
            if let Some(status) = child
                .try_wait()
                .with_context(|| format!("checking status for {program}"))?
            {
                break status;
            }

            tokio::select! {
                res = rx.changed() => {
                    // P1.4: Only cancel if the value is actually true.
                    // Err(Closed) means the sender was dropped (e.g., grace timer task
                    // aborted) — treat as "no cancel" and keep waiting.
                    if res.is_ok() && *rx.borrow() {
                        tracing::info!("Cancelling process group for {program}");
                        terminate_process_group(&mut child, program).await;

                        // Still collect whatever output was produced.
                        let _ = collect_lines(stdout_handle, stderr_handle, &mut on_line).await;
                        return Err(anyhow::anyhow!("process cancelled"));
                    }
                }
                () = tokio::time::sleep(Duration::from_millis(100)) => {}
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
    let lines = collect_lines(stdout_handle, stderr_handle, &mut on_line).await;

    Ok(ProcessOutput { exit_code, lines })
}

#[cfg(not(test))]
const SIGINT_GRACE: Duration = Duration::from_millis(7500);
#[cfg(test)]
const SIGINT_GRACE: Duration = Duration::from_millis(250);

#[cfg(not(test))]
const SIGTERM_GRACE: Duration = Duration::from_millis(2500);
#[cfg(test)]
const SIGTERM_GRACE: Duration = Duration::from_millis(250);

async fn collect_lines(
    stdout_handle: Option<JoinHandle<Vec<String>>>,
    stderr_handle: Option<JoinHandle<Vec<String>>>,
    on_line: &mut Option<LineCallback>,
) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(h) = stdout_handle {
        lines.extend(h.await.unwrap_or_default());
    }
    if let Some(h) = stderr_handle {
        lines.extend(h.await.unwrap_or_default());
    }

    for line in &lines {
        if let Some(cb) = on_line {
            cb(line);
        }
    }

    lines
}

async fn terminate_process_group(child: &mut AsyncGroupChild, program: &str) {
    if graceful_signal(child, program, ProcessSignal::Interrupt, SIGINT_GRACE).await {
        tracing::info!("Process group for {program} exited after SIGINT");
        return;
    }

    if graceful_signal(child, program, ProcessSignal::Terminate, SIGTERM_GRACE).await {
        tracing::info!("Process group for {program} exited after SIGTERM");
        return;
    }

    tracing::info!("Killing process group for {program} after SIGINT/SIGTERM grace expired");
    if let Err(e) = child.kill().await {
        tracing::warn!("Failed to kill process group: {e}");
    }
}

#[derive(Copy, Clone)]
enum ProcessSignal {
    Interrupt,
    Terminate,
}

async fn graceful_signal(
    child: &mut AsyncGroupChild,
    program: &str,
    signal: ProcessSignal,
    timeout: Duration,
) -> bool {
    if let Err(e) = send_signal(child, signal) {
        tracing::warn!(
            "Failed to send {} to process group for {program}: {e}",
            signal.name()
        );
        return false;
    }

    wait_for_exit(child, timeout).await
}

impl ProcessSignal {
    fn name(self) -> &'static str {
        match self {
            ProcessSignal::Interrupt => "SIGINT",
            ProcessSignal::Terminate => "SIGTERM",
        }
    }
}

#[cfg(unix)]
fn send_signal(child: &AsyncGroupChild, signal: ProcessSignal) -> std::io::Result<()> {
    let signal = match signal {
        ProcessSignal::Interrupt => Signal::SIGINT,
        ProcessSignal::Terminate => Signal::SIGTERM,
    };
    child.signal(signal)
}

#[cfg(not(unix))]
fn send_signal(child: &mut AsyncGroupChild, _signal: ProcessSignal) -> std::io::Result<()> {
    child.start_kill()
}

async fn wait_for_exit(child: &mut AsyncGroupChild, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => {}
            Err(e) => {
                tracing::warn!("Failed to poll process group status after signal: {e}");
                return false;
            }
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return false;
        }

        tokio::time::sleep((deadline - now).min(Duration::from_millis(50))).await;
    }
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[tokio::test]
    async fn cancel_sends_sigint_before_hard_kill() {
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (line_tx, line_rx) = mpsc::channel();

        let cancel_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = cancel_tx.send(true);
        });

        let result = invoke(
            "sh",
            &[
                "-c",
                "trap 'echo got-int; exit 0' INT; echo ready; while true; do sleep 1; done",
            ],
            Path::new("."),
            &HashMap::new(),
            Some(Box::new(move |line| {
                let _ = line_tx.send(line.to_string());
            })),
            Some(cancel_rx),
        )
        .await;

        let _ = cancel_task.await;
        assert!(result.is_err(), "cancelled process should return an error");

        let lines: Vec<String> = line_rx.try_iter().collect();
        assert!(
            lines.iter().any(|line| line == "got-int"),
            "expected SIGINT trap output, got {lines:?}"
        );
    }

    #[tokio::test]
    async fn cancel_falls_back_to_sigterm_when_sigint_is_ignored() {
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (line_tx, line_rx) = mpsc::channel();

        let cancel_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = cancel_tx.send(true);
        });

        let result = invoke(
            "sh",
            &[
                "-c",
                "trap '' INT; trap 'echo got-term; exit 0' TERM; echo ready; while true; do sleep 1; done",
            ],
            Path::new("."),
            &HashMap::new(),
            Some(Box::new(move |line| {
                let _ = line_tx.send(line.to_string());
            })),
            Some(cancel_rx),
        )
        .await;

        let _ = cancel_task.await;
        assert!(result.is_err(), "cancelled process should return an error");

        let lines: Vec<String> = line_rx.try_iter().collect();
        assert!(
            lines.iter().any(|line| line == "got-term"),
            "expected SIGTERM trap output after ignored SIGINT, got {lines:?}"
        );
    }
}
