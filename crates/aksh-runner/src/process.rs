//! Process invocation with process-group management.
//!
//! Wraps `command-group` for process-tree management. On cancel/timeout it
//! follows the official runner sequence: SIGINT grace, SIGTERM grace, then
//! SIGKILL, while still reaping the process group.
//!
//! ## Chunk-based output (matching the official runner)
//!
//! The official runner reads RAW BYTES from the child process's stdout/stderr
//! pipes and writes them directly into paged log files on disk — no per-line
//! `String` allocations, no UTF-8 validation, no line splitting in the hot
//! path. This implementation follows the same model:
//!
//!   bash ──stdout/stderr──> spawn_chunk_reader
//!                              │  reads raw bytes (no newline splitting)
//!                              │  sends Bytes chunks through mpsc
//!                              ▼
//!                         push_chunk
//!                              │
//!                              ├─> ChunkCallback(&[u8])  (handler writes to disk)
//!                              │
//!                              └─> lines: Vec<String>    (only when keep_lines=true)
//!
//! The OS pipe is the backpressure mechanism: when the consumer is slow, the
//! kernel pipe buffer fills, bash blocks on write(2). No Rust memory grows
//! beyond the bounded channel + the read buffer.

use anyhow::{Context, Result};
use bytes::Bytes;
use command_group::{AsyncCommandGroup, AsyncGroupChild};
#[cfg(unix)]
use command_group::{Signal, UnixChildExt};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tokio::io::AsyncRead;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

/// Bound the in-flight chunk queue. A 1024-entry channel × ~64 KB per chunk
/// caps the in-memory buffer at ~64 MB worst case. In practice the consumer
/// drains faster than the producer, so the queue rarely fills.
const CHUNK_CHANNEL_CAPACITY: usize = 1024;

/// Size of the read buffer for raw byte chunks from stdout/stderr.
const READ_BUF_SIZE: usize = 65536; // 64 KB

/// Result of a process invocation.
#[derive(Debug)]
pub struct ProcessOutput {
    /// Process exit code (0 = success).
    pub exit_code: i32,
    /// Collected stdout + stderr lines (only when keep_lines is true).
    pub lines: Vec<String>,
}

// ── Callback types ──────────────────────────────────────────────────────

/// Per-line callback (used by live_logs batch enqueue at page boundaries).
pub type LineCallback<'a> = Box<dyn FnMut(&str) + Send + 'a>;

/// Per-chunk callback for the hot path. Receives raw bytes from the child
/// process pipe — no String allocation, no UTF-8 check, no line splitting.
/// The handler writes these bytes directly to a paged log file on disk.
pub type ChunkCallback<'a> = Box<dyn FnMut(&[u8]) + Send + 'a>;

// ── invoke ──────────────────────────────────────────────────────────────

/// Invoke a process with the given environment.
///
/// In production mode (`keep_lines = false`), raw byte chunks are delivered
/// to `on_chunk` and no `String` is materialised per line. The handler writes
/// chunks straight to a paged log file on disk, matching the official runner.
///
/// In test/legacy mode (`keep_lines = true`), lines are split on newlines
/// and accumulated in `ProcessOutput::lines` for assertion.
///
/// If `cancel_rx` fires, the process group receives SIGINT → SIGTERM → SIGKILL
/// with graceful timeouts, matching the official runner sequence.
pub async fn invoke<'a>(
    program: &str,
    args: &[&str],
    cwd: &Path,
    env: &HashMap<String, String>,
    mut on_chunk: Option<ChunkCallback<'a>>,
    mut cancel_rx: Option<watch::Receiver<bool>>,
    keep_lines: bool,
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

    let (chunk_tx, mut chunk_rx) = mpsc::channel::<Bytes>(CHUNK_CHANNEL_CAPACITY);

    let stdout_handle = stdout.map(|s| spawn_chunk_reader(s, chunk_tx.clone()));
    let stderr_handle = stderr.map(|s| spawn_chunk_reader(s, chunk_tx));
    let mut lines = Vec::new();

    // Wait for process, racing against cancellation while draining chunks.
    let mut status_opt: Option<std::process::ExitStatus> = None;
    let mut cancel_requested = false;

    loop {
        if let Some(status) = child
            .try_wait()
            .with_context(|| format!("checking status for {program}"))?
        {
            status_opt = Some(status);
            break;
        }

        if let Some(rx) = cancel_rx.as_mut() {
            tokio::select! {
                chunk = chunk_rx.recv() => match chunk {
                    Some(bytes) => push_chunk(bytes, &mut lines, &mut on_chunk, keep_lines),
                    None => continue,
                },
                res = rx.changed() => {
                    if res.is_ok() && *rx.borrow() {
                        tracing::info!("Cancelling process group for {program}");
                        cancel_requested = true;
                        break;
                    }
                }
                () = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        } else {
            tokio::select! {
                chunk = chunk_rx.recv() => match chunk {
                    Some(bytes) => push_chunk(bytes, &mut lines, &mut on_chunk, keep_lines),
                    None => continue,
                },
                () = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
    }

    if cancel_requested {
        terminate_process_group(&mut child, program).await;
        drain_chunks(stdout_handle, stderr_handle, &mut chunk_rx, &mut lines, &mut on_chunk, keep_lines).await;
        return Err(anyhow::anyhow!("process cancelled"));
    }

    let status = status_opt.context("process did not exit")?;
    let exit_code = status.code().unwrap_or(-1);

    // Drain remaining chunks after process exit.
    drain_chunks(
        stdout_handle,
        stderr_handle,
        &mut chunk_rx,
        &mut lines,
        &mut on_chunk,
        keep_lines,
    )
    .await;

    Ok(ProcessOutput { exit_code, lines })
}

// ── Signal timeouts ─────────────────────────────────────────────────────

#[cfg(not(test))]
const SIGINT_GRACE: Duration = Duration::from_millis(7500);
#[cfg(test)]
const SIGINT_GRACE: Duration = Duration::from_millis(250);

#[cfg(not(test))]
const SIGTERM_GRACE: Duration = Duration::from_millis(2500);
#[cfg(test)]
const SIGTERM_GRACE: Duration = Duration::from_millis(250);

// ── Chunk reader ────────────────────────────────────────────────────────

/// Read raw bytes from a stdout/stderr pipe into a bounded mpsc.
///
/// Unlike the line-based reader, this does NOT split on newlines and does
/// NOT allocate per-line Strings. It mirrors the official runner's model:
/// read whatever the kernel gives us, send it as-is.
///
/// The bounded channel backpressures the producer when the consumer is slow.
fn spawn_chunk_reader<R>(mut stream: R, tx: mpsc::Sender<Bytes>) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut buf = vec![0u8; READ_BUF_SIZE];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    let chunk = Bytes::copy_from_slice(&buf[..n]);
                    if tx.send(chunk).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    })
}

// ── push_chunk / drain_chunks ───────────────────────────────────────────

/// Deliver a raw byte chunk to the consumer.
///
/// When `on_chunk` is present, it receives the raw bytes directly (zero
/// String allocation). When `keep_lines` is true, the chunk is split on
/// newlines and accumulated into `lines` for test assertions.
fn push_chunk(
    bytes: Bytes,
    lines: &mut Vec<String>,
    on_chunk: &mut Option<ChunkCallback<'_>>,
    keep_lines: bool,
) {
    if let Some(cb) = on_chunk.as_mut() {
        cb(&bytes);
    }
    if keep_lines {
        for segment in bytes.split(|&b| b == b'\n') {
            if !segment.is_empty() {
                match std::str::from_utf8(segment) {
                    Ok(s) => lines.push(s.to_string()),
                    Err(_) => {
                        lines.push(String::from_utf8_lossy(segment).into_owned());
                    }
                }
            }
        }
    }
}

/// Drain remaining chunks after the process has exited or been cancelled.
async fn drain_chunks(
    stdout_handle: Option<JoinHandle<()>>,
    stderr_handle: Option<JoinHandle<()>>,
    chunk_rx: &mut mpsc::Receiver<Bytes>,
    lines: &mut Vec<String>,
    on_chunk: &mut Option<ChunkCallback<'_>>,
    keep_lines: bool,
) {
    if let Some(h) = stdout_handle {
        let _ = h.await;
    }
    if let Some(h) = stderr_handle {
        let _ = h.await;
    }

    while let Some(bytes) = chunk_rx.recv().await {
        push_chunk(bytes, lines, on_chunk, keep_lines);
    }
}

// ── Process group termination ───────────────────────────────────────────

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

// ── Tests ───────────────────────────────────────────────────────────────

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
            Some(Box::new(move |chunk: &[u8]| {
                for seg in chunk.split(|&b| b == b'\n') {
                    if let Ok(s) = std::str::from_utf8(seg) {
                        if !s.is_empty() {
                            let _ = line_tx.send(s.to_string());
                        }
                    }
                }
            })),
            Some(cancel_rx),
            true,
        )
        .await;

        let _ = cancel_task.await;

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
            Some(Box::new(move |chunk: &[u8]| {
                for seg in chunk.split(|&b| b == b'\n') {
                    if let Ok(s) = std::str::from_utf8(seg) {
                        if !s.is_empty() {
                            let _ = line_tx.send(s.to_string());
                        }
                    }
                }
            })),
            Some(cancel_rx),
            true,
        )
        .await;

        let _ = cancel_task.await;

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
            true,
        )
        .await
        .unwrap();

        assert_eq!(result.exit_code, 0);
        assert_eq!(result.lines, vec!["out-1", "err-1", "out-2", "err-2"]);
    }

    #[tokio::test]
    async fn invoke_streams_chunks_to_callback_before_exit() {
        let (chunk_tx, chunk_rx) = std_mpsc::channel();
        let chunk_tx2 = chunk_tx.clone();

        let result = invoke(
            "sh",
            &["-c", "echo alpha; echo beta >&2"],
            Path::new("."),
            &HashMap::new(),
            Some(Box::new(move |chunk: &[u8]| {
                let _ = chunk_tx.send(chunk.to_vec());
            })),
            None,
            true,
        )
        .await
        .unwrap();

        assert_eq!(result.exit_code, 0);
        let callback_chunks: Vec<u8> = chunk_rx.try_iter().flatten().collect();
        let callback_str = String::from_utf8_lossy(&callback_chunks);
        assert!(
            callback_str.contains("alpha"),
            "expected alpha in chunk output, got: {callback_str}"
        );
        assert!(
            callback_str.contains("beta"),
            "expected beta in chunk output, got: {callback_str}"
        );
    }

    #[tokio::test]
    async fn invoke_sets_working_directory() {
        let dir = tempfile::TempDir::new().unwrap();
        let result = invoke("pwd", &[], dir.path(), &HashMap::new(), None, None, true)
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
            true,
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
            true,
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
            true,
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
            true,
        )
        .await;

        let _ = cancel_task.await;
        let err = result.unwrap_err().to_string();
        assert!(err.contains("process cancelled"));
    }
}
