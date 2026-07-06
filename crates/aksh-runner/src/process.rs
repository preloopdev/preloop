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
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::sync::{mpsc, watch};
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

    let (line_tx, mut line_rx) = mpsc::unbounded_channel();

    let stdout_handle = stdout.map(|s| spawn_line_reader(s, line_tx.clone()));
    let stderr_handle = stderr.map(|s| spawn_line_reader(s, line_tx));
    let mut lines = Vec::new();

    // Wait for process, racing against cancellation while draining stdout/stderr
    // as it arrives. This preserves observed stream order instead of collecting
    // all stdout before stderr after process exit.
    let status = if let Some(mut rx) = cancel_rx {
        loop {
            if let Some(status) = child
                .try_wait()
                .with_context(|| format!("checking status for {program}"))?
            {
                break status;
            }

            tokio::select! {
                Some(line) = line_rx.recv() => push_line(line, &mut lines, &mut on_line),
                res = rx.changed() => {
                    // P1.4: Only cancel if the value is actually true.
                    // Err(Closed) means the sender was dropped (e.g., grace timer task
                    // aborted) — treat as "no cancel" and keep waiting.
                    if res.is_ok() && *rx.borrow() {
                        tracing::info!("Cancelling process group for {program}");
                        terminate_process_group(&mut child, program).await;

                        // Still collect whatever output was produced.
                        drain_lines(stdout_handle, stderr_handle, &mut line_rx, &mut lines, &mut on_line).await;
                        return Err(anyhow::anyhow!("process cancelled"));
                    }
                }
                () = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
    } else {
        loop {
            if let Some(status) = child
                .try_wait()
                .with_context(|| format!("checking status for {program}"))?
            {
                break status;
            }

            tokio::select! {
                Some(line) = line_rx.recv() => push_line(line, &mut lines, &mut on_line),
                () = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
    };

    let exit_code = status.code().unwrap_or(-1);

    // Collect output that arrived between the final status poll and pipe close.
    drain_lines(
        stdout_handle,
        stderr_handle,
        &mut line_rx,
        &mut lines,
        &mut on_line,
    )
    .await;

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

fn spawn_line_reader<R>(stream: R, tx: mpsc::UnboundedSender<String>) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut reader = BufReader::new(stream).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            if tx.send(line).is_err() {
                break;
            }
        }
    })
}

fn push_line(line: String, lines: &mut Vec<String>, on_line: &mut Option<LineCallback>) {
    if let Some(cb) = on_line {
        cb(&line);
    }
    lines.push(line);
}

async fn drain_lines(
    stdout_handle: Option<JoinHandle<()>>,
    stderr_handle: Option<JoinHandle<()>>,
    line_rx: &mut mpsc::UnboundedReceiver<String>,
    lines: &mut Vec<String>,
    on_line: &mut Option<LineCallback>,
) {
    if let Some(h) = stdout_handle {
        let _ = h.await;
    }
    if let Some(h) = stderr_handle {
        let _ = h.await;
    }

    while let Ok(line) = line_rx.try_recv() {
        push_line(line, lines, on_line);
    }
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
    use std::sync::mpsc as std_mpsc;

    #[tokio::test]
    async fn cancel_sends_sigint_before_hard_kill() {
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (line_tx, line_rx) = std_mpsc::channel();

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
        let (line_tx, line_rx) = std_mpsc::channel();

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

    #[tokio::test]
    async fn invoke_captures_stdout_and_stderr_in_observed_order() {
        let result = invoke(
            "sh",
            &[
                "-c",
                "echo out-1; sleep 0.05; echo err-1 >&2; sleep 0.05; echo out-2; sleep 0.05; echo err-2 >&2",
            ],
            Path::new("."),
            &HashMap::new(),
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.exit_code, 0);
        assert_eq!(result.lines, vec!["out-1", "err-1", "out-2", "err-2"]);
    }

    #[tokio::test]
    async fn invoke_streams_lines_to_callback_before_exit() {
        let (line_tx, mut line_rx) = mpsc::unbounded_channel();

        let result = invoke(
            "sh",
            &["-c", "echo alpha; echo beta >&2"],
            Path::new("."),
            &HashMap::new(),
            Some(Box::new(move |line| {
                let _ = line_tx.send(line.to_string());
            })),
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.exit_code, 0);
        let mut callback_lines = Vec::new();
        while let Ok(line) = line_rx.try_recv() {
            callback_lines.push(line);
        }
        assert_eq!(callback_lines, result.lines);
        assert_eq!(callback_lines, vec!["alpha", "beta"]);
    }

    #[tokio::test]
    async fn invoke_sets_working_directory() {
        let dir = tempfile::TempDir::new().unwrap();
        let result = invoke("pwd", &[], dir.path(), &HashMap::new(), None, None)
            .await
            .unwrap();

        assert_eq!(result.exit_code, 0);
        assert_eq!(
            result.lines,
            vec![std::fs::canonicalize(dir.path())
                .unwrap()
                .to_string_lossy()
                .to_string()]
        );
    }

    #[tokio::test]
    async fn invoke_propagates_environment() {
        let env = HashMap::from([("AKSH_PROCESS_TEST".to_string(), "visible".to_string())]);
        let result = invoke(
            "sh",
            &["-c", "printf '%s\n' \"$AKSH_PROCESS_TEST\""],
            Path::new("."),
            &env,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.exit_code, 0);
        assert_eq!(result.lines, vec!["visible"]);
    }

    #[tokio::test]
    async fn invoke_returns_nonzero_exit_code() {
        let result = invoke(
            "sh",
            &["-c", "echo before-exit; exit 42"],
            Path::new("."),
            &HashMap::new(),
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.exit_code, 42);
        assert_eq!(result.lines, vec!["before-exit"]);
    }

    #[tokio::test]
    async fn invoke_handles_long_output_without_loss() {
        let result = invoke(
            "sh",
            &[
                "-c",
                "i=0; while [ $i -lt 200 ]; do echo line-$i; i=$((i+1)); done",
            ],
            Path::new("."),
            &HashMap::new(),
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.exit_code, 0);
        assert_eq!(result.lines.len(), 200);
        assert_eq!(result.lines.first().map(String::as_str), Some("line-0"));
        assert_eq!(result.lines.last().map(String::as_str), Some("line-199"));
    }

    #[tokio::test]
    async fn cancelled_process_returns_error() {
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let cancel_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let _ = cancel_tx.send(true);
        });

        let result = invoke(
            "sh",
            &["-c", "echo started; while true; do sleep 1; done"],
            Path::new("."),
            &HashMap::new(),
            None,
            Some(cancel_rx),
        )
        .await;

        let _ = cancel_task.await;
        let err = result.unwrap_err().to_string();
        assert!(err.contains("process cancelled"));
    }
}
