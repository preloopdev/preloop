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

/// Maximum lines retained per invocation when `keep_lines` is set
/// (docker invocations, e.g. `docker logs` at service teardown). Without a
/// cap, a chatty subprocess grows the line vecs without bound.
const MAX_KEPT_LINES: usize = 100_000;
/// Maximum retained bytes across both line vecs per invocation. The
/// line-count cap alone still admits 100k × 64 KiB newline-free chunks, and
/// stdout is mirrored into `lines`, doubling it again. Both vecs share one
/// budget; retention stops (silently, like the line cap) once exhausted.
const MAX_KEPT_BYTES: u64 = 64 * 1024 * 1024;

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

/// Job-message bookkeeping families that must never reach a step process.
///
/// `system.*` and `DistributedTask.*` match case-insensitively. `actions_*`
/// matches lower-case only: the job-message family (`actions_cache_mode`,
/// `actions_runner_…`) is lower-case, while the runner's legitimate
/// `ACTIONS_*` plumbing (`ACTIONS_RUNTIME_URL`, `ACTIONS_STEP_DEBUG`, …) is
/// something steps are entitled to see, matching GitHub.
///
/// The guest's terminal identity is filtered with them. A hosted runner's step
/// environment has no `TERM` and no `COLORTERM`: the runner is a systemd
/// service with no tty, and the `dumb` a bash step prints for `$TERM` is
/// bash's own default for an *unset* `TERM` (`variables.c`'s
/// `set_if_not ("TERM", "dumb")`) rather than an exported variable — a probe
/// of a real hosted runner reads `printenv TERM` unset and no `TERM` in `env`
/// or `/proc/self/environ`, while `bash -lc` prints `dumb` there too. A
/// preloop runner is started through the VM's exec channel instead, which
/// carries the terminal the guest booted with (`TERM=linux` from the smolvm
/// init, and from `envd` on AgentENV), and [`invoke`] inherits the runner's
/// machine environment into every step; without this the channel's terminal
/// rode into every step's environment. A workflow that declares
/// `TERM`/`COLORTERM` itself still wins: explicit job/step variables are
/// applied after the inherited set.
pub(crate) fn is_internal_step_env(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    lower.starts_with("system.")
        || lower.starts_with("distributedtask.")
        || key.starts_with("actions_")
        || matches!(lower.as_str(), "term" | "colorterm")
}

/// Credential-shaped names that must never be inherited by a step.
///
/// The runner starts from the operator's environment (shell, unit file, CI
/// agent). Self-hosted hosts routinely carry provider/cloud credentials there,
/// and a step is attacker-controlled code, so inheriting them turns any
/// workflow into a credential-disclosure primitive — an audit run printed the
/// host's `CLIPROXY_API_KEY`/`OPENROUTER_API_KEY` through a plain `env` dump.
///
/// This is a name-shape filter, not a value scan: it is deliberately
/// conservative about what it keeps (`PATH`, `HOME`, `LANG`, toolchain roots
/// are untouched) and is overridable with
/// `PRELOOP_RUNNER_INHERIT_HOST_ENV=1` for workflows that genuinely rely on a
/// host credential variable (which should instead be passed via `env:`).
/// Two exact names are settled before the marker scan: the benign
/// `TOKENIZERS_PARALLELISM` (kept) and the runner's own keypair
/// (`PRELOOP_RUNNER_RSA_PARAMS`, dropped).
pub(crate) fn is_sensitive_host_env(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    // Benign toolchain knobs whose name happens to trip a marker below.
    // `TOKENIZERS_PARALLELISM` (HuggingFace) carries a tokenizer
    // fork-parallelism preference, not a token; dropping it silently changes
    // tokenizer behaviour for every step that loads one. Exact names only:
    // `MY_TOKENIZERS_PARALLELISM_API_KEY` is still credential-shaped.
    if lower == "tokenizers_parallelism" {
        return false;
    }
    // The runner's own authentication key, whose name carries no marker.
    // `PRELOOP_RUNNER_RSA_PARAMS` is an `RSAParameters` JSON keypair that a
    // supervising orchestrator injects into the runner's environment
    // (`configure.rs`'s `RSA_PARAMS_ENV`) so the runner need not generate one
    // per job; a step that inherits it can print the complete registration key
    // with a plain `env`.
    if lower == "preloop_runner_rsa_params" {
        return true;
    }
    const MARKERS: &[&str] = &[
        "token",
        "secret",
        "password",
        "passwd",
        "api_key",
        "apikey",
        "private_key",
        "access_key",
        "credential",
        "auth_token",
        "session_key",
    ];
    MARKERS.iter().any(|marker| lower.contains(marker)) || lower.ends_with("_key")
}

/// Which of the runner's own environment variables a child process inherits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HostEnvPolicy {
    /// Drop credential-shaped host names, like a step's own environment does.
    /// Steps, scripts, actions, hooks, and every runner-owned command that
    /// does not have to authenticate through the host environment use this.
    Filtered,
    /// Keep credential-shaped host names (internal bookkeeping families are
    /// still dropped). Only runner-owned plumbing that delegates
    /// authentication to a helper process reading the host environment —
    /// Docker's registry credential helpers — may use this.
    Trusted,
}

/// Whether a variable from the runner's own environment reaches a child.
///
/// Internal bookkeeping families never do, under either policy. Under
/// [`HostEnvPolicy::Filtered`] credential-shaped names do not either, unless
/// `PRELOOP_RUNNER_INHERIT_HOST_ENV` opts back in; under
/// [`HostEnvPolicy::Trusted`] they are kept on purpose.
fn inherits_step_env(key: &str, policy: HostEnvPolicy, inherit_host_env: bool) -> bool {
    if is_internal_step_env(key) {
        return false;
    }
    match policy {
        HostEnvPolicy::Trusted => true,
        HostEnvPolicy::Filtered => inherit_host_env || !is_sensitive_host_env(key),
    }
}

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
///
/// The runner's own environment reaches the child under
/// [`HostEnvPolicy::Filtered`]; see [`invoke_with_host_env_policy`] for the
/// one class of caller that needs more.
pub async fn invoke<'a>(
    program: &str,
    args: &[&str],
    cwd: &Path,
    env: &HashMap<String, String>,
    on_chunk: Option<ChunkCallback<'a>>,
    cancel_rx: Option<watch::Receiver<bool>>,
    keep_lines: bool,
) -> Result<ProcessOutput> {
    invoke_with_host_env_policy(
        program,
        args,
        cwd,
        env,
        HostEnvPolicy::Filtered,
        on_chunk,
        cancel_rx,
        keep_lines,
    )
    .await
}

/// [`invoke`] with an explicit [`HostEnvPolicy`] for the runner's own
/// environment.
///
/// [`HostEnvPolicy::Trusted`] exists for runner-owned invocations whose
/// authentication is performed by a helper process that reads the host
/// environment by design — Docker's registry credential helpers
/// (`docker-credential-ecr-login` reads `AWS_ACCESS_KEY_ID` /
/// `AWS_SECRET_ACCESS_KEY`, the gcloud helper reads
/// `GOOGLE_APPLICATION_CREDENTIALS`). A step must never use it: workflow YAML
/// is attacker-controlled code, and an inherited provider key turns any
/// workflow into a credential-disclosure primitive.
#[allow(clippy::too_many_arguments)] // mirrors `invoke`'s shape plus the policy
pub(crate) async fn invoke_with_host_env_policy<'a>(
    program: &str,
    args: &[&str],
    cwd: &Path,
    env: &HashMap<String, String>,
    policy: HostEnvPolicy,
    mut on_chunk: Option<ChunkCallback<'a>>,
    mut cancel_rx: Option<watch::Receiver<bool>>,
    keep_lines: bool,
) -> Result<ProcessOutput> {
    // Children inherit the worker's machine environment (HOME, LANG, …) but
    // must not see job-message bookkeeping families that could ride along in
    // it: a workflow can observe any inherited key by running `env`.
    // Credential-shaped host variables (provider keys, cloud tokens) are
    // dropped too — the child is attacker-controlled code.
    // `PRELOOP_RUNNER_INHERIT_HOST_ENV` restores the old full inheritance for
    // workflows that rely on one. Explicit job/step variables are applied
    // afterwards and take precedence.
    let inherit_host_env = std::env::var("PRELOOP_RUNNER_INHERIT_HOST_ENV")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "on" | "yes"
            )
        })
        .unwrap_or(false);
    let inherited = std::env::vars_os().filter(move |(key, _)| {
        inherits_step_env(&key.to_string_lossy(), policy, inherit_host_env)
    });
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(args)
        .current_dir(cwd)
        .env_clear()
        .envs(inherited)
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

    let mut child = cmd.group_spawn().with_context(|| {
        let cwd_exists = cwd.exists();
        let path = env
            .get("PATH")
            .map(String::as_str)
            .unwrap_or("<PATH unset>");
        format!(
            "spawning {program} (cwd={} exists={cwd_exists} PATH={path})",
            cwd.display()
        )
    })?;

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
    // Shared retain budget for both vecs; see MAX_KEPT_BYTES.
    let mut kept_bytes = 0u64;

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
                        &mut kept_bytes,
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
                        &mut kept_bytes,
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
            &mut kept_bytes,
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
        &mut kept_bytes,
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

/// Retain one chunk segment against the line-count cap and the shared byte
/// budget. Either bound stops retention; an overshooting final segment is
/// kept whole, bounding overshoot to one read chunk.
fn retain_segment(segment: &[u8], target: &mut Vec<String>, kept_bytes: &mut u64) {
    if segment.is_empty() || target.len() >= MAX_KEPT_LINES || *kept_bytes >= MAX_KEPT_BYTES {
        return;
    }
    let s = match std::str::from_utf8(segment) {
        Ok(s) => s.to_string(),
        Err(_) => String::from_utf8_lossy(segment).into_owned(),
    };
    *kept_bytes += s.len() as u64;
    target.push(s);
}

fn push_chunk(
    bytes: Bytes,
    lines: &mut Vec<String>,
    stdout_lines: &mut Vec<String>,
    on_chunk: &mut Option<ChunkCallback<'_>>,
    keep_lines: bool,
    is_stdout: bool,
    kept_bytes: &mut u64,
) {
    if let Some(cb) = on_chunk.as_mut() {
        cb(&bytes);
    }
    if keep_lines {
        if is_stdout {
            // Stdout is retained twice (its own vec plus the combined one);
            // both draws come from the same byte budget.
            for segment in bytes.split(|&b| b == b'\n') {
                retain_segment(segment, stdout_lines, kept_bytes);
            }
        }
        // Combined vec always receives the segments (stdout additionally
        // keeps its own copy above).
        for segment in bytes.split(|&b| b == b'\n') {
            retain_segment(segment, lines, kept_bytes);
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
    kept_bytes: &mut u64,
) {
    if wait_for_eof {
        // Runner.Worker does not complete a normally exited process until its
        // redirected stdout/stderr streams reach EOF. A background descendant
        // that inherited either pipe therefore keeps the step active and
        // remains cancellable through the process group.
        while let Some((is_stdout, bytes)) = chunk_rx.recv().await {
            push_chunk(
                bytes,
                lines,
                stdout_lines,
                on_chunk,
                keep_lines,
                is_stdout,
                kept_bytes,
            );
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
        push_chunk(
            bytes,
            lines,
            stdout_lines,
            on_chunk,
            keep_lines,
            is_stdout,
            kept_bytes,
        );
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
        signal_group(group, nix::sys::signal::Signal::SIGKILL);
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
fn signal_group(group: nix::unistd::Pid, signal: nix::sys::signal::Signal) {
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
    let (nix_signal, command_group_signal) = match signal {
        ProcessSignal::Interrupt => (nix::sys::signal::Signal::SIGINT, Signal::SIGINT),
        ProcessSignal::Terminate => (nix::sys::signal::Signal::SIGTERM, Signal::SIGTERM),
    };

    // Address the group id directly: once the leader is reaped the child
    // handle can no longer reach the members that are still running.
    if let Some(group) = group {
        signal_group(group, nix_signal);
        return Ok(());
    }

    child.signal(command_group_signal)
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
        // `lines` is the *observed* merge of two pipes; the reader may drain
        // one stream twice before the other's first read under load, so exact
        // interleave order is not a contract. What is: every line arrives, and
        // each stream's entries keep their own order inside the merged view.
        let stdout_in_merged: Vec<&str> = result
            .lines
            .iter()
            .filter(|l| l.starts_with("out-"))
            .map(String::as_str)
            .collect();
        let stderr_in_merged: Vec<&str> = result
            .lines
            .iter()
            .filter(|l| l.starts_with("err-"))
            .map(String::as_str)
            .collect();
        assert_eq!(
            result.lines.len(),
            4,
            "all lines captured: {:?}",
            result.lines
        );
        assert_eq!(stdout_in_merged, ["out-1", "out-2"]);
        assert_eq!(stderr_in_merged, ["err-1", "err-2"]);
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
        // after the leader has been reaped. The trap is installed in the
        // *parent*, before the background job is forked: ignored dispositions
        // are inherited atomically at fork, so there is no window where the
        // shell-exit SIGHUP can land with a default disposition. (Installing
        // the trap inside the subshell lost that race on loaded hosts: the
        // child occasionally died before the 150 ms cancellation fired and
        // `invoke` correctly returned success.) SIGKILL against the group
        // must be the only thing that can reap the child. Assert the child
        // actually died rather than trusting the error string: a survivor is
        // reparented to init and silently outlives the job.
        let dir = tempfile::tempdir().expect("tempdir");
        let pid_path = dir.path().join("background.pid");
        let script = format!(
            "trap '' HUP TERM INT; (while :; do sleep 1; done) & echo $! > {}; echo ready",
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

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("process cancelled")
        );

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
            vec![
                std::fs::canonicalize(dir.path())
                    .unwrap()
                    .to_string_lossy()
                    .to_string()
            ]
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

    /// Inherited job-message bookkeeping (`system.*`, `DistributedTask.*`,
    /// `actions_*`) must not reach a step, while ordinary machine env and the
    /// runner's uppercase `ACTIONS_*` plumbing still do.
    ///
    /// The assertions run in a dedicated subprocess: seeding the families
    /// requires adding variables to a process environment, and doing that in
    /// the shared test binary would race other tests that read the
    /// environment concurrently. The child is this same test binary, filtered
    /// to this test and seeded through its own environment.
    #[tokio::test]
    async fn invoke_filters_inherited_job_bookkeeping() {
        const CHILD_MARKER: &str = "PRELOOP_STEP_ENV_FILTER_CHILD";
        const SEEDED: [(&str, &str); 8] = [
            ("system.leak.test", "1"),
            ("DistributedTask.LeakTest", "1"),
            ("actions_leak_test", "1"),
            ("TERM", "linux"),
            ("COLORTERM", "truecolor"),
            ("term", "xterm-256color"),
            ("ACTIONS_PLUMBING_KEEP", "1"),
            ("PRELOOP_HOST_ENV_KEEP", "1"),
        ];

        if std::env::var_os(CHILD_MARKER).is_none() {
            let exe = std::env::current_exe().expect("test binary path");
            let status = std::process::Command::new(exe)
                .args([
                    "--exact",
                    "process::tests::invoke_filters_inherited_job_bookkeeping",
                    "--nocapture",
                ])
                .env(CHILD_MARKER, "1")
                .envs(SEEDED)
                .status()
                .expect("spawning the seeded test subprocess");
            assert!(
                status.success(),
                "seeded subprocess failed ({status}); the step environment checks run there"
            );
            return;
        }

        // Invoke `env` directly, not through `sh -c env`: dash drops variable
        // names that are not valid shell identifiers when it execs a child,
        // which would mask a leak of the dotted families on distros whose
        // `/bin/sh` is dash.
        let result = invoke(
            "env",
            &[],
            Path::new("."),
            &HashMap::new(),
            None,
            None,
            true,
        )
        .await
        .unwrap();

        assert_eq!(result.exit_code, 0);
        let output = result.lines.join("\n");
        for (leaked, _) in &SEEDED[..6] {
            assert!(
                !output.contains(&format!("{leaked}=")),
                "inherited job bookkeeping or terminal identity {leaked} leaked into \
                 the step environment"
            );
        }
        for (kept, _) in &SEEDED[6..] {
            assert!(
                output.contains(&format!("{kept}=1")),
                "ordinary inherited env {kept} was dropped"
            );
        }
    }

    /// A workflow that declares `TERM` (or `COLORTERM`) itself keeps it: the
    /// filter only drops the *inherited* terminal identity, and explicit
    /// job/step variables are applied after the inherited set.
    #[tokio::test]
    async fn invoke_keeps_a_workflow_declared_terminal_identity() {
        const CHILD_MARKER: &str = "PRELOOP_STEP_ENV_TERM_CHILD";
        if std::env::var_os(CHILD_MARKER).is_none() {
            let exe = std::env::current_exe().expect("test binary path");
            let status = std::process::Command::new(exe)
                .args([
                    "--exact",
                    "process::tests::invoke_keeps_a_workflow_declared_terminal_identity",
                    "--nocapture",
                ])
                .env(CHILD_MARKER, "1")
                .env("TERM", "linux")
                .status()
                .expect("spawning the seeded test subprocess");
            assert!(status.success(), "seeded subprocess failed ({status})");
            return;
        }

        let declared = HashMap::from([
            ("TERM".to_owned(), "xterm-256color".to_owned()),
            ("COLORTERM".to_owned(), "truecolor".to_owned()),
        ]);
        let result = invoke("env", &[], Path::new("."), &declared, None, None, true)
            .await
            .unwrap();
        assert_eq!(result.exit_code, 0);
        let output = result.lines.join("\n");
        assert!(
            output.contains("TERM=xterm-256color"),
            "a workflow-declared TERM must reach the step: {output}"
        );
        assert!(
            output.contains("COLORTERM=truecolor"),
            "a workflow-declared COLORTERM must reach the step: {output}"
        );
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
    #[test]
    fn retained_output_bounded_by_bytes_not_just_lines() {
        // Newline-free flood: 2000 × 64 KiB chunks with no `\n` would
        // previously retain ~256 MiB (×2 mirrored) under the line-count cap.
        let mut lines = Vec::new();
        let mut stdout_lines = Vec::new();
        let mut kept_bytes = 0u64;
        let flood = Bytes::from(vec![b'A'; 64 * 1024]);
        for _ in 0..2000 {
            push_chunk(
                flood.clone(),
                &mut lines,
                &mut stdout_lines,
                &mut None,
                true,
                true,
                &mut kept_bytes,
            );
        }
        let total: u64 = lines.iter().map(|l| l.len() as u64).sum::<u64>()
            + stdout_lines.iter().map(|l| l.len() as u64).sum::<u64>();
        assert!(
            total <= MAX_KEPT_BYTES + 128 * 1024,
            "retained {total} bytes past the budget"
        );
        assert!(!lines.is_empty(), "budget must retain early output");
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

    #[test]
    fn credential_shaped_host_env_is_sensitive() {
        for key in [
            "CLIPROXY_API_KEY",
            "OPENROUTER_API_KEY",
            "DEEPSEEK_API_KEY",
            "OPENAI_API_KEY",
            "GITHUB_TOKEN",
            "ACTIONS_RUNTIME_TOKEN",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_ACCESS_KEY_ID",
            "SSH_PRIVATE_KEY",
            "DB_PASSWORD",
            "DB_PASSWD",
            "SERVICE_CREDENTIALS",
            "MY_SESSION_KEY",
            "GPG_KEY",
            // The runner's own keypair: JSON `RSAParameters` with no
            // credential marker in the name.
            "PRELOOP_RUNNER_RSA_PARAMS",
        ] {
            assert!(is_sensitive_host_env(key), "{key} must be filtered");
        }
    }

    #[test]
    fn ordinary_host_env_is_inherited() {
        for key in [
            "PATH",
            "HOME",
            "LANG",
            "LC_ALL",
            "TMPDIR",
            "SHELL",
            "RUSTUP_HOME",
            "CARGO_HOME",
            "JAVA_HOME",
            "GOPATH",
            "GITHUB_ACTIONS",
            "CI",
            "TERM_PROGRAM",
            // Exact-name exception: the substring `token` matches, but the
            // variable is HuggingFace's tokenizer parallelism knob and carries
            // no credential.
            "TOKENIZERS_PARALLELISM",
            "tokenizers_parallelism",
        ] {
            assert!(!is_sensitive_host_env(key), "{key} must be inherited");
        }
        // The exception is by exact name, not prefix: a credential variable
        // that merely contains it stays filtered.
        assert!(is_sensitive_host_env("TOKENIZERS_PARALLELISM_API_KEY"));
    }

    /// A workflow can observe every inherited key with `env`; a credential in
    /// the runner's own environment must not reach the step at all.
    #[test]
    fn step_env_inheritance_policy() {
        // Ordinary machine environment survives.
        assert!(inherits_step_env("PATH", HostEnvPolicy::Filtered, false));
        assert!(inherits_step_env("HOME", HostEnvPolicy::Filtered, false));
        assert!(inherits_step_env("LANG", HostEnvPolicy::Filtered, false));
        assert!(inherits_step_env(
            "RUSTUP_HOME",
            HostEnvPolicy::Filtered,
            false
        ));
        assert!(inherits_step_env(
            "TOKENIZERS_PARALLELISM",
            HostEnvPolicy::Filtered,
            false
        ));
        // Credential-shaped host values are dropped.
        assert!(!inherits_step_env(
            "CLIPROXY_API_KEY",
            HostEnvPolicy::Filtered,
            false
        ));
        assert!(!inherits_step_env(
            "OPENROUTER_API_KEY",
            HostEnvPolicy::Filtered,
            false
        ));
        assert!(!inherits_step_env(
            "GITHUB_TOKEN",
            HostEnvPolicy::Filtered,
            false
        ));
        assert!(!inherits_step_env(
            "AWS_SECRET_ACCESS_KEY",
            HostEnvPolicy::Filtered,
            false
        ));
        // The runner's own environment carries its pre-generated keypair as
        // `PRELOOP_RUNNER_RSA_PARAMS`; a step must never read it.
        assert!(!inherits_step_env(
            "PRELOOP_RUNNER_RSA_PARAMS",
            HostEnvPolicy::Filtered,
            false
        ));
        // Internal bookkeeping families are always dropped. `ACTIONS_*`
        // uppercase is deliberately kept: steps are entitled to see it, and
        // the per-job values are injected explicitly afterwards.
        assert!(inherits_step_env(
            "ACTIONS_RUNTIME_URL",
            HostEnvPolicy::Filtered,
            false
        ));
        assert!(!inherits_step_env("TERM", HostEnvPolicy::Filtered, false));
        assert!(!inherits_step_env(
            "system.github",
            HostEnvPolicy::Filtered,
            false
        ));
        // A host `ACTIONS_*` token is still credential-shaped and dropped;
        // the runner injects its own token through the job env map.
        assert!(!inherits_step_env(
            "ACTIONS_RUNTIME_TOKEN",
            HostEnvPolicy::Filtered,
            false
        ));
        // The documented escape hatch restores the old behaviour.
        assert!(inherits_step_env(
            "CLIPROXY_API_KEY",
            HostEnvPolicy::Filtered,
            true
        ));
        // The trusted policy — runner-owned pulls through a registry
        // credential helper — keeps credential-shaped names.
        assert!(inherits_step_env(
            "AWS_ACCESS_KEY_ID",
            HostEnvPolicy::Trusted,
            false
        ));
        assert!(inherits_step_env(
            "AWS_SECRET_ACCESS_KEY",
            HostEnvPolicy::Trusted,
            false
        ));
        // …but still never the job-message bookkeeping families.
        assert!(!inherits_step_env("TERM", HostEnvPolicy::Trusted, false));
        assert!(!inherits_step_env(
            "system.github",
            HostEnvPolicy::Trusted,
            false
        ));
        assert!(!inherits_step_env(
            "DistributedTask.LeakTest",
            HostEnvPolicy::Trusted,
            false
        ));
    }

    /// End to end, through a real child process: a credential-shaped variable
    /// in the runner's own environment must not reach a step. The variable is
    /// seeded by re-execing this test binary (`std::env::set_var` is `unsafe`
    /// and the crate denies `unsafe`), the same trick the signal tests use.
    #[tokio::test]
    async fn invoke_drops_credential_shaped_host_env_from_steps() {
        const CHILD_MARKER: &str = "PRELOOP_STEP_ENV_CREDENTIAL_CHILD";
        const SECRET: &str = "PRELOOP_TEST_CLIPROXY_API_KEY";
        const RUNNER_KEY: &str = "PRELOOP_RUNNER_RSA_PARAMS";
        const PLAIN: &str = "PRELOOP_TEST_HOST_ENV_KEEP";

        if std::env::var_os(CHILD_MARKER).is_none() {
            let exe = std::env::current_exe().expect("test binary path");
            let status = std::process::Command::new(exe)
                .args([
                    "--exact",
                    "process::tests::invoke_drops_credential_shaped_host_env_from_steps",
                    "--nocapture",
                ])
                // The escape hatch is documented, so a host configured with
                // `PRELOOP_RUNNER_INHERIT_HOST_ENV=1` would otherwise leak it
                // into the child, which would then correctly keep `SECRET` and
                // fail this test's own assertions.
                .env_remove("PRELOOP_RUNNER_INHERIT_HOST_ENV")
                .env(CHILD_MARKER, "1")
                .env(SECRET, "hunter2")
                .env(RUNNER_KEY, r#"{"d":"private-exponent"}"#)
                .env(PLAIN, "kept-value")
                .status()
                .expect("spawning the seeded test subprocess");
            assert!(
                status.success(),
                "seeded subprocess failed ({status}); the inherited-environment check runs there"
            );
            return;
        }

        let result = invoke(
            "sh",
            &[
                "-c",
                &format!(
                    "printenv {SECRET} || echo SECRET-MISSING; \
                     printenv {RUNNER_KEY} || echo RUNNER-KEY-MISSING; \
                     printenv {PLAIN} || echo PLAIN-MISSING"
                ),
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
        let output = result.lines.join("\n");
        assert!(
            output.contains("SECRET-MISSING"),
            "credential-shaped host env {SECRET} reached a step: {output}"
        );
        assert!(
            output.contains("RUNNER-KEY-MISSING"),
            "the runner's own keypair env {RUNNER_KEY} reached a step: {output}"
        );
        assert!(
            !output.contains("hunter2"),
            "the credential value itself reached a step: {output}"
        );
        assert!(
            !output.contains("private-exponent"),
            "the runner keypair value itself reached a step: {output}"
        );
        assert!(
            output.contains("kept-value"),
            "ordinary inherited env {PLAIN} was dropped: {output}"
        );
    }

    /// The trusted policy is what `pull_image` uses, so that a registry
    /// credential helper (ECR's reads `AWS_ACCESS_KEY_ID` /
    /// `AWS_SECRET_ACCESS_KEY`) still authenticates private images — while the
    /// job-message bookkeeping families stay filtered.
    #[tokio::test]
    async fn trusted_host_env_policy_keeps_credential_shaped_vars() {
        const CHILD_MARKER: &str = "PRELOOP_STEP_ENV_TRUSTED_CHILD";
        const SECRET: &str = "PRELOOP_TEST_AWS_SECRET_ACCESS_KEY";
        const BOOKKEEPING: &str = "system.leak.test";

        if std::env::var_os(CHILD_MARKER).is_none() {
            let exe = std::env::current_exe().expect("test binary path");
            let status = std::process::Command::new(exe)
                .args([
                    "--exact",
                    "process::tests::trusted_host_env_policy_keeps_credential_shaped_vars",
                    "--nocapture",
                ])
                .env(CHILD_MARKER, "1")
                .env(SECRET, "hunter2")
                .env(BOOKKEEPING, "1")
                .status()
                .expect("spawning the seeded test subprocess");
            assert!(status.success(), "seeded subprocess failed ({status})");
            return;
        }

        // `env` directly, not `sh -c env`: dash drops variable names that are
        // not valid shell identifiers when it execs a child, which would mask
        // a leak of the dotted bookkeeping family.
        let result = invoke_with_host_env_policy(
            "env",
            &[],
            Path::new("."),
            &HashMap::new(),
            HostEnvPolicy::Trusted,
            None,
            None,
            true,
        )
        .await
        .unwrap();

        assert_eq!(result.exit_code, 0);
        let output = result.lines.join("\n");
        assert!(
            output.contains(&format!("{SECRET}=hunter2")),
            "the trusted policy dropped {SECRET}, and registry credential helpers would fail: {output}"
        );
        assert!(
            !output.contains(&format!("{BOOKKEEPING}=")),
            "job bookkeeping {BOOKKEEPING} leaked under the trusted policy"
        );
    }
}
