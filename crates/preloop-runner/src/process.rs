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

// ProcessInvoker.cs v2.335.1 waits five seconds for redirected streams after
// the parent exits, then kills the remaining process tree.
#[cfg(not(test))]
const STREAM_DRAIN_GRACE: Duration = Duration::from_secs(5);
#[cfg(test)]
const STREAM_DRAIN_GRACE: Duration = Duration::from_millis(350);

/// Result of a process invocation.
#[derive(Debug)]
pub struct ProcessOutput {
    /// Process exit code (0 = success).
    pub exit_code: i32,
    /// Collected stdout + stderr lines (only when keep_lines is true).
    pub lines: Vec<String>,
    /// Collected stdout-only lines (only when keep_lines is true).
    ///
    /// The official runner keeps the two streams separate
    /// (`OutputDataReceived` vs `ErrorDataReceived`); the merged `lines` above
    /// is for log display, but callers that must parse stdout (e.g. the
    /// container ID from `docker create`) need the stdout-only view —
    /// docker's platform warning on stderr can otherwise interleave ahead of
    /// the ID.
    pub stdout_lines: Vec<String>,
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
        // A step never gets interactive input. The official runner leaves
        // stdin unredirected, so on a hosted runner a step inherits the
        // service's `/dev/null` and a prompting command fails fast. Here the
        // runner itself is a child of a guest `exec` whose stdin is a live
        // pipe that never delivers a byte and never reaches EOF, so
        // `sudo apt-get install musl-tools` (uv's musl cell, no `-y`) blocks
        // on its confirmation prompt until the job times out. Hand every step
        // an empty stdin instead.
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    let mut child = cmd
        .group_spawn()
        .with_context(|| format!("spawning {program}"))?;

    // Capture the group id now, while the handle still reports one: the wait
    // loop below reaps the leader the moment it exits, and a reaped handle
    // addresses nothing. The group outlives its leader for as long as any
    // member is alive, so cancellation has to escalate against this id.
    let group = process_group(&child);

    let stdout = child.inner().stdout.take();
    let stderr = child.inner().stderr.take();

    let (chunk_tx, mut chunk_rx) = mpsc::channel::<(bool, Bytes)>(CHUNK_CHANNEL_CAPACITY);

    let stdout_handle = stdout.map(|s| spawn_chunk_reader(s, chunk_tx.clone(), true));
    let stderr_handle = stderr.map(|s| spawn_chunk_reader(s, chunk_tx, false));
    let mut lines = Vec::new();
    let mut stdout_lines = Vec::new();

    // Wait for process, racing against cancellation while draining chunks.
    let mut status_opt: Option<std::process::ExitStatus> = None;
    let mut cancel_requested = false;
    let mut stream_deadline: Option<tokio::time::Instant> = None;
    let mut forced_stream_close = false;

    loop {
        if status_opt.is_none() {
            let status = child
                .try_wait()
                .with_context(|| format!("checking status for {program}"))?;
            if status.is_some() && !chunk_rx.is_closed() {
                stream_deadline = Some(tokio::time::Instant::now() + STREAM_DRAIN_GRACE);
            }
            status_opt = status;
        }
        // Runner.Worker keeps the process invocation active until redirected
        // stdout/stderr reach EOF. This also keeps cancellation live after the
        // shell exits while a background descendant retains either pipe.
        if status_opt.is_some() && chunk_rx.is_closed() {
            break;
        }

        if let Some(rx) = cancel_rx.as_mut() {
            tokio::select! {
                chunk = chunk_rx.recv() => match chunk {
                    Some((is_stdout, bytes)) => push_chunk(
                        bytes,
                        &mut lines,
                        &mut stdout_lines,
                        &mut on_chunk,
                        keep_lines,
                        is_stdout,
                    ),
                    None => continue,
                },
                res = rx.changed() => {
                    if res.is_ok() && *rx.borrow() {
                        tracing::info!("Cancelling process group for {program}");
                        cancel_requested = true;
                        break;
                    }
                }
                _ = tokio::time::sleep_until(stream_deadline.unwrap_or_else(tokio::time::Instant::now)), if stream_deadline.is_some() => {
                    tracing::info!("Killing process group for {program} after redirected streams remained open for {:?}", STREAM_DRAIN_GRACE);
                    force_kill_group(&mut child, group, program).await;
                    forced_stream_close = true;
                    break;
                }
                () = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        } else {
            tokio::select! {
                chunk = chunk_rx.recv() => match chunk {
                    Some((is_stdout, bytes)) => push_chunk(
                        bytes,
                        &mut lines,
                        &mut stdout_lines,
                        &mut on_chunk,
                        keep_lines,
                        is_stdout,
                    ),
                    None => continue,
                },
                _ = tokio::time::sleep_until(stream_deadline.unwrap_or_else(tokio::time::Instant::now)), if stream_deadline.is_some() => {
                    tracing::info!("Killing process group for {program} after redirected streams remained open for {:?}", STREAM_DRAIN_GRACE);
                    force_kill_group(&mut child, group, program).await;
                    forced_stream_close = true;
                    break;
                }
                () = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
    }

    if cancel_requested {
        terminate_process_group(&mut child, group, program).await;
        drain_chunks(
            stdout_handle,
            stderr_handle,
            &mut chunk_rx,
            &mut lines,
            &mut stdout_lines,
            &mut on_chunk,
            keep_lines,
            false,
        )
        .await;
        return Err(anyhow::anyhow!("process cancelled"));
    }

    let status = status_opt.context("process did not exit")?;
    let exit_code = status.code().unwrap_or(-1);

    // A forced stream cutoff is a successful parent-process completion, but
    // its escaped readers must be aborted rather than awaited indefinitely.
    drain_chunks(
        stdout_handle,
        stderr_handle,
        &mut chunk_rx,
        &mut lines,
        &mut stdout_lines,
        &mut on_chunk,
        keep_lines,
        !forced_stream_close,
    )
    .await;

    Ok(ProcessOutput {
        exit_code,
        lines,
        stdout_lines,
    })
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
fn spawn_chunk_reader<R>(
    mut stream: R,
    tx: mpsc::Sender<(bool, Bytes)>,
    is_stdout: bool,
) -> JoinHandle<()>
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
                    if tx.send((is_stdout, chunk)).await.is_err() {
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
    stdout_lines: &mut Vec<String>,
    on_chunk: &mut Option<ChunkCallback<'_>>,
    keep_lines: bool,
    is_stdout: bool,
) {
    if let Some(cb) = on_chunk.as_mut() {
        cb(&bytes);
    }
    if keep_lines {
        let target: &mut Vec<String> = if is_stdout { stdout_lines } else { lines };
        for segment in bytes.split(|&b| b == b'\n') {
            if !segment.is_empty() {
                match std::str::from_utf8(segment) {
                    Ok(s) => target.push(s.to_string()),
                    Err(_) => {
                        target.push(String::from_utf8_lossy(segment).into_owned());
                    }
                }
            }
        }
        if is_stdout {
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
}

/// Drain remaining chunks after the process has exited or been cancelled.
#[allow(clippy::too_many_arguments)]
async fn drain_chunks(
    mut stdout_handle: Option<JoinHandle<()>>,
    mut stderr_handle: Option<JoinHandle<()>>,
    chunk_rx: &mut mpsc::Receiver<(bool, Bytes)>,
    lines: &mut Vec<String>,
    stdout_lines: &mut Vec<String>,
    on_chunk: &mut Option<ChunkCallback<'_>>,
    keep_lines: bool,
    wait_for_eof: bool,
) {
    if wait_for_eof {
        // Runner.Worker does not complete a normally exited process until its
        // redirected stdout/stderr streams reach EOF. A background descendant
        // that inherited either pipe therefore keeps the step active and
        // remains cancellable through the process group.
        while let Some((is_stdout, bytes)) = chunk_rx.recv().await {
            push_chunk(bytes, lines, stdout_lines, on_chunk, keep_lines, is_stdout);
        }
        if let Some(handle) = stdout_handle.take() {
            let _ = handle.await;
        }
        if let Some(handle) = stderr_handle.take() {
            let _ = handle.await;
        }
        return;
    }

    // Cancellation/forced termination must not hang on a descendant that
    // escaped the process group while retaining a pipe.
    const DRAIN_GRACE: Duration = Duration::from_millis(250);
    tokio::time::sleep(DRAIN_GRACE).await;
    if let Some(handle) = stdout_handle.take() {
        handle.abort();
    }
    if let Some(handle) = stderr_handle.take() {
        handle.abort();
    }
    while let Ok((is_stdout, bytes)) = chunk_rx.try_recv() {
        push_chunk(bytes, lines, stdout_lines, on_chunk, keep_lines, is_stdout);
    }
}

// ── Process group termination ───────────────────────────────────────────

async fn terminate_process_group(child: &mut AsyncGroupChild, group: ProcessGroup, program: &str) {
    if graceful_signal(
        child,
        group,
        program,
        ProcessSignal::Interrupt,
        SIGINT_GRACE,
    )
    .await
    {
        tracing::info!("Process group for {program} exited after SIGINT");
        return;
    }

    if graceful_signal(
        child,
        group,
        program,
        ProcessSignal::Terminate,
        SIGTERM_GRACE,
    )
    .await
    {
        tracing::info!("Process group for {program} exited after SIGTERM");
        return;
    }

    tracing::info!("Killing process group for {program} after SIGINT/SIGTERM grace expired");
    force_kill_group(child, group, program).await;
}

/// Hard-kill every surviving member of the group, then reap the leader.
///
/// `AsyncGroupChild::kill` can only reach a leader that is still unreaped, so
/// on its own it leaves a backgrounded descendant running — reparented to
/// init and outliving the job that spawned it. Sweeping the group is what
/// makes this match `ProcessInvoker.cs`, which kills the remaining tree.
async fn force_kill_group(child: &mut AsyncGroupChild, group: ProcessGroup, program: &str) {
    #[cfg(unix)]
    if let Some(group) = group {
        signal_group(group, Signal::SIGKILL);
    }
    #[cfg(not(unix))]
    let _ = group;

    if let Err(error) = child.kill().await {
        // Routine once the wait loop has already collected the leader.
        tracing::debug!("Leader for {program} was already reaped: {error}");
    }
}

/// The process group cancellation escalates against, or `None` when none can
/// be addressed safely.
#[cfg(unix)]
type ProcessGroup = Option<nix::unistd::Pid>;
#[cfg(not(unix))]
type ProcessGroup = ();

#[cfg(unix)]
fn process_group(child: &AsyncGroupChild) -> ProcessGroup {
    let id = i32::try_from(child.id()?).ok()?;
    // `killpg` reads a non-positive id as "my own group", which would signal
    // the runner itself. `group_spawn` makes the child its own group leader,
    // so an id matching our own group means it never got one.
    if id <= 0 || id == nix::unistd::getpgrp().as_raw() {
        return None;
    }
    Some(nix::unistd::Pid::from_raw(id))
}

#[cfg(not(unix))]
fn process_group(_child: &AsyncGroupChild) -> ProcessGroup {}

/// Deliver `signal` to every member of `group`. A failure means the group has
/// already drained, which is the outcome the caller wanted anyway.
#[cfg(unix)]
fn signal_group(group: nix::unistd::Pid, signal: Signal) {
    let _ = nix::sys::signal::killpg(group, signal);
}

/// Whether any member of `group` is still alive. The null signal runs the
/// existence and permission checks without delivering anything.
#[cfg(unix)]
fn group_alive(group: nix::unistd::Pid) -> bool {
    nix::sys::signal::killpg(group, None).is_ok()
}

#[derive(Copy, Clone)]
enum ProcessSignal {
    Interrupt,
    Terminate,
}

async fn graceful_signal(
    child: &mut AsyncGroupChild,
    group: ProcessGroup,
    program: &str,
    signal: ProcessSignal,
    timeout: Duration,
) -> bool {
    if let Err(e) = send_signal(child, group, signal) {
        tracing::warn!(
            "Failed to send {} to process group for {program}: {e}",
            signal.name()
        );
        return false;
    }

    wait_for_group_exit(child, group, timeout).await
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
fn send_signal(
    child: &AsyncGroupChild,
    group: ProcessGroup,
    signal: ProcessSignal,
) -> std::io::Result<()> {
    let signal = match signal {
        ProcessSignal::Interrupt => Signal::SIGINT,
        ProcessSignal::Terminate => Signal::SIGTERM,
    };

    // Address the group id directly: once the leader is reaped the child
    // handle can no longer reach the members that are still running.
    if let Some(group) = group {
        signal_group(group, signal);
        return Ok(());
    }

    child.signal(signal)
}

#[cfg(not(unix))]
fn send_signal(
    child: &mut AsyncGroupChild,
    _group: ProcessGroup,
    _signal: ProcessSignal,
) -> std::io::Result<()> {
    child.start_kill()
}

/// Wait until the whole group has drained, not merely the leader.
///
/// The leader is reaped as soon as it exits so its zombie cannot keep the
/// group artificially alive, and only then is the group probed. Treating the
/// leader's exit as "the group is gone" is precisely what let a backgrounded
/// descendant outlive cancellation.
async fn wait_for_group_exit(
    child: &mut AsyncGroupChild,
    group: ProcessGroup,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let leader_gone = match child.try_wait() {
            Ok(status) => status.is_some(),
            Err(e) => {
                tracing::warn!("Failed to poll process group status after signal: {e}");
                return false;
            }
        };

        #[cfg(unix)]
        {
            match group {
                Some(group) if !group_alive(group) => return true,
                // With no addressable group the leader's status is all there is.
                None if leader_gone => return true,
                _ => {}
            }
        }

        #[cfg(not(unix))]
        {
            let _ = group;
            if leader_gone {
                return true;
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
    use std::io::Write;
    use std::sync::mpsc as std_mpsc;

    const SIGNAL_TARGET_ENV: &str = "PRELOOP_PROCESS_SIGNAL_TEST_TARGET";

    /// Runs as a subprocess for the cancellation tests below. Registering the
    /// handlers in the target process makes the tests independent of the
    /// surrounding test host's signal dispositions. In particular, the
    /// official Actions runner ignores SIGINT and that disposition survives
    /// exec, so a shell trap cannot observe the signal during dogfood.
    #[tokio::test]
    async fn cancellation_signal_target() {
        let Ok(target) = std::env::var(SIGNAL_TARGET_ENV) else {
            return;
        };
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .expect("register SIGINT handler");
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("register SIGTERM handler");

        println!("\nready");
        std::io::stdout().flush().expect("flush readiness");
        interrupt.recv().await.expect("receive SIGINT");

        match target.as_str() {
            "interrupt" => println!("got-int"),
            "terminate" => {
                terminate.recv().await.expect("receive SIGTERM");
                println!("got-term");
            }
            other => panic!("unknown signal test target {other}"),
        }
    }

    /// A step that reads stdin must see EOF, not block. The runner runs as a
    /// child of a guest `exec` whose stdin never closes, so an inherited stdin
    /// hangs any command that prompts — `sudo apt-get install <pkg>` without
    /// `-y` waits on its confirmation prompt until the job times out.
    #[tokio::test]
    async fn steps_read_eof_from_stdin() {
        let (line_tx, line_rx) = std_mpsc::channel();

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            invoke(
                "sh",
                &["-c", "cat; echo eof-reached"],
                Path::new("."),
                &HashMap::new(),
                Some(Box::new(move |chunk: &[u8]| {
                    if let Ok(text) = std::str::from_utf8(chunk) {
                        let _ = line_tx.send(text.to_owned());
                    }
                })),
                None,
                true,
            ),
        )
        .await
        .expect("a step reading stdin must not block")
        .expect("the step runs");

        assert_eq!(result.exit_code, 0);
        let output: String = line_rx.try_iter().collect();
        assert!(
            output.contains("eof-reached"),
            "`cat` must reach EOF, got {output:?}"
        );
    }

    #[tokio::test]
    async fn cancel_sends_sigint_before_hard_kill() {
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (line_tx, line_rx) = std_mpsc::channel();
        let test_binary = std::env::current_exe().expect("resolve test binary");
        let test_binary = test_binary.to_str().expect("UTF-8 test binary path");
        let env = HashMap::from([(SIGNAL_TARGET_ENV.to_owned(), "interrupt".to_owned())]);

        let _result = tokio::time::timeout(
            Duration::from_secs(5),
            invoke(
                test_binary,
                &[
                    "--exact",
                    "process::tests::cancellation_signal_target",
                    "--nocapture",
                ],
                Path::new("."),
                &env,
                Some(Box::new(move |chunk: &[u8]| {
                    for seg in chunk.split(|&b| b == b'\n') {
                        if let Ok(s) = std::str::from_utf8(seg) {
                            let s = s.trim();
                            if !s.is_empty() {
                                let _ = line_tx.send(s.to_string());
                                if s == "ready" {
                                    let _ = cancel_tx.send(true);
                                }
                            }
                        }
                    }
                })),
                Some(cancel_rx),
                true,
            ),
        )
        .await
        .expect("cancelled process should exit");

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
        let test_binary = std::env::current_exe().expect("resolve test binary");
        let test_binary = test_binary.to_str().expect("UTF-8 test binary path");
        let env = HashMap::from([(SIGNAL_TARGET_ENV.to_owned(), "terminate".to_owned())]);

        let _result = tokio::time::timeout(
            Duration::from_secs(5),
            invoke(
                test_binary,
                &[
                    "--exact",
                    "process::tests::cancellation_signal_target",
                    "--nocapture",
                ],
                Path::new("."),
                &env,
                Some(Box::new(move |chunk: &[u8]| {
                    for seg in chunk.split(|&b| b == b'\n') {
                        if let Ok(s) = std::str::from_utf8(seg) {
                            let s = s.trim();
                            if !s.is_empty() {
                                let _ = line_tx.send(s.to_string());
                                if s == "ready" {
                                    let _ = cancel_tx.send(true);
                                }
                            }
                        }
                    }
                })),
                Some(cancel_rx),
                true,
            ),
        )
        .await
        .expect("cancelled process should exit");

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
        assert_eq!(
            result.stdout_lines,
            vec!["out-1", "out-2"],
            "stdout-only view must exclude stderr regardless of interleaving \
             (the official runner parses container IDs from stdout alone)"
        );
    }

    #[tokio::test]
    async fn invoke_waits_for_background_child_to_close_inherited_output_pipe() {
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            invoke(
                "sh",
                &["-c", "(sleep 0.15) & echo marker"],
                Path::new("."),
                &HashMap::new(),
                None,
                None,
                true,
            ),
        )
        .await
        .expect("invoke did not observe inherited-pipe EOF")
        .expect("invoke failed");

        assert_eq!(result.exit_code, 0);
        assert!(
            started.elapsed() >= Duration::from_millis(100),
            "invoke returned before the background child closed stdout"
        );
        assert!(
            result.lines.iter().any(|line| line == "marker"),
            "expected marker in captured output, got {:?}",
            result.lines
        );
    }

    #[tokio::test]
    async fn cancellation_interrupts_background_child_after_shell_exit() {
        // The shell exits within a millisecond, so cancellation always lands
        // after the leader has been reaped, and the backgrounded child ignores
        // both graceful signals — SIGKILL against the group is the only thing
        // that can reap it. Assert the child actually died rather than
        // trusting the error string: a survivor is reparented to init and
        // silently outlives the job.
        let dir = tempfile::tempdir().expect("tempdir");
        let pid_path = dir.path().join("background.pid");
        let script = format!(
            "(trap '' TERM INT; while :; do sleep 1; done) & echo $! > {}; echo ready",
            pid_path.display()
        );

        let (cancel_tx, cancel_rx) = watch::channel(false);
        let cancel_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let _ = cancel_tx.send(true);
        });

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            invoke(
                "sh",
                &["-c", &script],
                Path::new("."),
                &HashMap::new(),
                None,
                Some(cancel_rx),
                true,
            ),
        )
        .await
        .expect("background child cancellation timed out");
        let _ = cancel_task.await;

        assert!(result
            .unwrap_err()
            .to_string()
            .contains("process cancelled"));

        let pid = nix::unistd::Pid::from_raw(
            std::fs::read_to_string(&pid_path)
                .expect("background pid file")
                .trim()
                .parse()
                .expect("background pid"),
        );

        // The sweep, the reparent, and init's reap all race this assertion.
        let mut survived = true;
        for _ in 0..100 {
            // The null signal only probes for existence.
            if nix::sys::signal::kill(pid, None).is_err() {
                survived = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!survived, "background child {pid} survived cancellation");
    }

    #[tokio::test]
    async fn invoke_forces_stream_close_after_official_grace_window() {
        // The shell exits at once while an orphaned child holds the inherited
        // pipe open far longer than the grace window: `invoke` must stop
        // waiting when the window elapses rather than block on the pipe
        // holder. The upper bound is deliberately far from the grace window
        // instead of a tight wall-clock deadline — this also runs on loaded
        // CI hosts, where a 1s ceiling over a 350ms window flakes on process
        // spawn alone.
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            invoke(
                "sh",
                &["-c", "(sleep 30) & echo marker"],
                Path::new("."),
                &HashMap::new(),
                None,
                None,
                true,
            ),
        )
        .await
        .expect("stream cutoff did not terminate the inherited-pipe process")
        .expect("invoke failed");

        assert_eq!(result.exit_code, 0);
        assert!(started.elapsed() >= STREAM_DRAIN_GRACE);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "invoke waited on the orphaned pipe holder: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn invoke_streams_chunks_to_callback_before_exit() {
        let (chunk_tx, chunk_rx) = std_mpsc::channel();
        let _chunk_tx2 = chunk_tx.clone();

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
        let env = HashMap::from([("PRELOOP_PROCESS_TEST".to_string(), "visible".to_string())]);
        let result = invoke(
            "sh",
            &["-c", "printf '%s\n' \"$PRELOOP_PROCESS_TEST\""],
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
