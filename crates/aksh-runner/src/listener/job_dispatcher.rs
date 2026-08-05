//! Job dispatcher — spawns worker processes for incoming jobs.
//!
//! Mirrors `JobDispatcher.cs` from the official runner: one job at a time,
//! spawned as `aksh-runner worker` child process communicating via stdin NDJSON.
//!
//! The listener keeps polling for messages while the worker runs (with status=Busy),
//! so JobCancellation can arrive mid-job and be forwarded to the worker.

use anyhow::{Context, Result};
use std::collections::VecDeque;
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::process::Child;
use tracing::{error, info, warn};

use crate::cli::ProtocolPath;
use futures::future::BoxFuture;

/// IPC message types sent from listener to worker via stdin.
#[derive(Debug, serde::Serialize)]
#[serde(tag = "t")]
enum WorkerMessage {
    /// A job to execute.
    #[serde(rename = "job")]
    Job { body: serde_json::Value },
    /// Cancel the currently running job.
    #[serde(rename = "cancel")]
    Cancel { timeout_secs: u64 },
    /// Shut down the worker process.
    #[serde(rename = "shutdown")]
    Shutdown,
}

/// Handle to a running worker process.
pub struct RunningJob {
    /// The worker child process.
    child: Child,
    /// Stdin handle — kept open so we can write cancel messages.
    stdin: Option<tokio::process::ChildStdin>,
    /// The full job message. Retained so the listener can force-complete a
    /// job whose worker died abnormally (official `ForceFailJob`).
    pub job_message: Option<serde_json::Value>,
    /// Captured worker stdout/stderr tail (official `workerOutput`), for the
    /// crash annotation when the worker exits abnormally.
    worker_output: Arc<Mutex<WorkerOutputCapture>>,
    /// The job/request ID for matching cancellation messages.
    pub request_id: String,
    /// Agent job GUID from the job message body (`jobId`), for JobCancellation matching.
    pub job_id: Option<uuid::Uuid>,
    /// Hard-kill deadline after cancel (official: timeout − 15s).
    pub kill_at: Option<tokio::time::Instant>,
    /// Whether graceful cancellation was already delivered to the worker.
    cancel_sent: bool,
    /// Numeric agent request ID for status queries, if available.
    pub agent_request_id: Option<i64>,
}

/// Bounded capture of the worker's stdout/stderr.
///
/// Mirrors the official dispatcher's `workerOutput` list: the tail is
/// attached to the job completion as the crash detail when the worker dies
/// abnormally (`LogWorkerProcessUnhandledException`). Bounded so a chatty
/// job cannot grow the listener's memory without limit; the crash is what
/// matters, so only the tail is kept.
#[derive(Default)]
pub struct WorkerOutputCapture {
    lines: VecDeque<String>,
    bytes: usize,
}

impl WorkerOutputCapture {
    const MAX_BYTES: usize = 64 * 1024;

    fn push(&mut self, line: String) {
        self.bytes += line.len() + 1;
        self.lines.push_back(line);
        while self.bytes > Self::MAX_BYTES {
            let Some(front) = self.lines.pop_front() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(front.len() + 1);
        }
    }

    /// Tail of the worker's output, newline-joined.
    pub fn tail(&self) -> String {
        let mut out = String::with_capacity(self.bytes);
        for (i, line) in self.lines.iter().enumerate() {
            if i > 0 {
                out.push('\n');
            }
            out.push_str(line);
        }
        out
    }
}

/// Read a worker output stream, forwarding every line to the listener's own
/// stdout/stderr (as the worker's inherited descriptors did before) while
/// appending it to the crash capture.
///
/// Byte-oriented on purpose: `lines()` errors out on invalid UTF-8, which would
/// silently end forwarding at the first stray byte. `Stdio::inherit()` passed
/// raw bytes through untouched, so forwarding writes the bytes exactly as read
/// and only the crash capture goes through a lossy conversion.
fn forward_worker_stream(
    stream: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    capture: Arc<Mutex<WorkerOutputCapture>>,
    to_stderr: bool,
) {
    tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::new(stream);
        let mut buf = Vec::new();
        loop {
            buf.clear();
            match reader.read_until(b'\n', &mut buf).await {
                Ok(0) => break,
                Ok(_) => {}
                Err(e) => {
                    warn!("worker output read failed: {e}");
                    break;
                }
            }
            // Capture the logical line without its terminator; the official
            // `workerOutput` list holds lines, and `tail()` re-joins them.
            let mut line = buf.as_slice();
            if let [rest @ .., b'\n'] = line {
                line = rest;
            }
            if let [rest @ .., b'\r'] = line {
                line = rest;
            }
            capture
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(String::from_utf8_lossy(line).into_owned());
            let _ = if to_stderr {
                tokio::io::stderr().write_all(&buf).await
            } else {
                tokio::io::stdout().write_all(&buf).await
            };
        }
    });
}

/// Server-side job request status provider used to resolve a busy-runner overlap.
///
/// The official runner asks the distributed-task service whether the previous
/// request has a terminal `result` before deciding whether to accept another
/// job. Keeping this as a tiny async provider makes that decision deterministic
/// in tests without coupling worker lifecycle code to HTTP.
pub trait AgentRequestStatusProvider: Send + Sync {
    fn get_agent_request<'a>(
        &'a self,
        token: &'a str,
        pool_id: i64,
        request_id: i64,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;
}

/// Apply the official EnsureDispatchFinished overlap decision to a worker.
///
/// A completed worker needs no server query. Otherwise a terminal server
/// result identifies a zombie worker: cancel it and wait up to the official
/// 45-second grace period. A null/missing result is an active overlap and is a
/// fatal protocol error; the caller must stop rather than silently dropping the
/// new job. Status-query failures cancel and drain the worker before returning
/// the original error, matching the official cleanup guarantee.
pub async fn ensure_dispatch_finished<P: AgentRequestStatusProvider>(
    job: &mut RunningJob,
    token: &str,
    pool_id: i64,
    provider: &P,
) -> Result<()> {
    if job.try_wait()?.is_some() {
        return Ok(());
    }

    let request = match provider
        .get_agent_request(
            token,
            pool_id,
            job.agent_request_id.ok_or_else(|| {
                anyhow::anyhow!(
                    "cannot query status for job request {} without a numeric request ID",
                    job.request_id
                )
            })?,
        )
        .await
    {
        Ok(request) => request,
        Err(error) => {
            job.cancel(60).await;
            let _ = job.wait().await;
            return Err(error).context("querying previous agent request status");
        }
    };

    let terminal = request
        .get("result")
        .is_some_and(|result| !result.is_null());
    if !terminal {
        anyhow::bail!(
            "server sent a new job request while previous job request {} hasn't finished",
            job.request_id
        );
    }

    job.cancel(60).await;
    match tokio::time::timeout(Duration::from_secs(45), job.wait()).await {
        Ok(result) => {
            result?;
            Ok(())
        }
        Err(_) => anyhow::bail!(
            "job dispatch process for {} was not cancelled within 45 seconds",
            job.request_id
        ),
    }
}
impl RunningJob {
    /// Check if the worker has finished (non-blocking).
    pub fn try_wait(&mut self) -> Result<Option<bool>> {
        match self.child.try_wait() {
            Ok(Some(status)) => Ok(Some(status.success())),
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Wait for the worker to finish.
    pub async fn wait(&mut self) -> Result<bool> {
        let status = self.child.wait().await.context("waiting for worker")?;
        Ok(status.success())
    }

    /// Send graceful cancellation once. Repeated official cancellation
    /// messages only reset `kill_at`; the worker cancellation token is
    /// idempotent in `actions/runner`.
    pub async fn cancel(&mut self, timeout_secs: u64) -> bool {
        if self.cancel_sent {
            return false;
        }
        self.cancel_sent = true;
        if let Some(stdin) = &mut self.stdin {
            let msg = WorkerMessage::Cancel { timeout_secs };
            if let Ok(line) = serde_json::to_string(&msg) {
                let _ = stdin.write_all(line.as_bytes()).await;
                let _ = stdin.write_all(b"\n").await;
                let _ = stdin.flush().await;
            }
        }
        true
    }

    /// Official `JobDispatcher` shutdown path: ask the worker to cancel the
    /// running job and wrap up (`RunnerShutdown`), waiting up to `grace`;
    /// only hard-kill if the worker ignores the message.
    ///
    /// Returns `Some(success)` when the worker exited on its own (success =
    /// the worker reported a normal completion, so the job concluded
    /// cleanly), or `None` when the grace expired and the worker had to be
    /// killed.
    pub async fn shutdown_gracefully(&mut self, grace: Duration) -> Option<bool> {
        if !self.cancel_sent {
            self.cancel_sent = true;
            if let Some(stdin) = &mut self.stdin {
                let msg = WorkerMessage::Shutdown;
                if let Ok(line) = serde_json::to_string(&msg) {
                    let _ = stdin.write_all(line.as_bytes()).await;
                    let _ = stdin.write_all(b"\n").await;
                    let _ = stdin.flush().await;
                }
            }
        }
        // A cancel was already delivered, or just now — the worker cancels
        // the job and exits. Wait for it within the grace before killing.
        match tokio::time::timeout(grace, self.wait()).await {
            Ok(result) => result.ok(),
            Err(_) => {
                warn!(
                    "Worker {} did not exit within the shutdown grace — killing",
                    self.request_id
                );
                self.kill().await;
                None
            }
        }
    }

    /// Hard-kill the worker and its whole process tree (after cancel timeout
    /// expires). Steps run in their own process groups (`process.rs`
    /// `group_spawn`), so killing only the worker PID would orphan them
    /// (CR-2 F-2); the official runner's JobDispatcher kills the worker with
    /// `Process.Kill(entireProcessTree: true)`, and this mirrors that.
    pub async fn kill(&mut self) {
        // Drop stdin first to unblock the worker's reader
        self.stdin.take();
        #[cfg(unix)]
        if let Some(pid) = self.child.id() {
            kill_process_tree(pid);
        }
        if let Err(e) = self.child.kill().await {
            warn!("Failed to kill worker: {e}");
        }
    }
}

/// Best-effort SIGKILL of `root` and every descendant in its process tree.
///
/// Walks a (pid, ppid) snapshot taken once, then kills each PID with
/// `kill -9`. Errors are ignored: cleanup must never fail the listener.
#[cfg(unix)]
fn kill_process_tree(root: u32) {
    use std::collections::BTreeMap;

    let mut children: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    #[cfg(target_os = "linux")]
    {
        if let Ok(proc_dir) = std::fs::read_dir("/proc") {
            for entry in proc_dir.flatten() {
                let name = entry.file_name();
                let Ok(pid) = name.to_string_lossy().parse::<u32>() else {
                    continue;
                };
                let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
                    continue;
                };
                // /proc/<pid>/stat: pid (comm) state ppid ...
                let Some(open) = stat.find('(') else { continue };
                let Some(close) = stat.rfind(')') else {
                    continue;
                };
                let mut fields = stat[close + 1..].split_whitespace();
                fields.next(); // state
                let Some(ppid) = fields.next().and_then(|s| s.parse::<u32>().ok()) else {
                    continue;
                };
                children.entry(ppid).or_default().push(pid);
            }
        }
    }
    #[cfg(target_os = "macos")]
    {
        if let Ok(out) = std::process::Command::new("ps")
            .args(["-axo", "pid=,ppid="])
            .output()
        {
            for line in String::from_utf8_lossy(&out.stdout).lines() {
                let mut fields = line.split_whitespace();
                if let (Some(pid), Some(ppid)) = (fields.next(), fields.next()) {
                    if let (Ok(pid), Ok(ppid)) = (pid.parse::<u32>(), ppid.parse::<u32>()) {
                        children.entry(ppid).or_default().push(pid);
                    }
                }
            }
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = root;
        return;
    }

    // DFS from the worker; kill descendants first so no child outlives its
    // reaped parent.
    let mut stack = vec![root];
    let mut order = Vec::new();
    while let Some(pid) = stack.pop() {
        order.push(pid);
        if let Some(kids) = children.get(&pid) {
            stack.extend(kids.iter().copied());
        }
    }
    for pid in order {
        let _ = std::process::Command::new("kill")
            .args(["-9", &pid.to_string()])
            .status();
    }
}

/// Spawn a worker process for a job. Returns immediately without waiting.
pub async fn spawn_job(
    job_message: serde_json::Value,
    runner_root: &Path,
    via: ProtocolPath,
) -> Result<RunningJob> {
    let request_id = job_message
        .get("jobId")
        .or_else(|| job_message.get("requestId"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let job_id = job_message
        .get("jobId")
        .and_then(|v| v.as_str())
        .and_then(|s| uuid::Uuid::parse_str(s).ok());
    let agent_request_id = job_message.get("requestId").and_then(|v| v.as_i64());

    info!("Dispatching job {request_id} to worker");

    #[cfg(test)]
    let mut command = {
        static WORKER_BIN: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
        let binary = WORKER_BIN.get_or_init(|| {
            let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
            let status = std::process::Command::new("cargo")
                .args(["build", "--quiet", "--manifest-path"])
                .arg(&manifest)
                .args(["--bin", "preloop-runner"])
                .status()
                .expect("build preloop-runner test worker");
            assert!(
                status.success(),
                "building preloop-runner test worker failed"
            );
            if let Ok(target_dir_env) = std::env::var("CARGO_TARGET_DIR") {
                std::path::PathBuf::from(target_dir_env).join("debug/preloop-runner")
            } else {
                manifest
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .join("target/debug/preloop-runner")
            }
        });
        let mut command = tokio::process::Command::new(binary);
        command.arg("worker");
        command
    };
    #[cfg(not(test))]
    let mut command = {
        let raw_exe = std::env::current_exe().context("finding current executable")?;
        let current_exe = if let Ok(bin) = std::env::var("CARGO_BIN_EXE_aksh-runner") {
            let p = std::path::PathBuf::from(bin);
            if p.exists() && p.file_name().unwrap() == "aksh-runner" {
                p
            } else {
                raw_exe
            }
        } else if let Ok(bin) = std::env::var("AKSH_RUNNER_BIN") {
            std::path::PathBuf::from(bin)
        } else {
            let target_dir = raw_exe.parent().unwrap();
            let aksh_bin = if target_dir.file_name().unwrap() == "deps" {
                target_dir.parent().unwrap().join("aksh-runner")
            } else {
                target_dir.join("aksh-runner")
            };
            if aksh_bin.exists() {
                aksh_bin
            } else {
                raw_exe
            }
        };
        let mut command = tokio::process::Command::new(current_exe);
        command.arg("worker");
        command
    };
    let via_str = match via {
        ProtocolPath::Broker => "broker",
        ProtocolPath::Azdo => "azdo",
    };
    let mut child = command
        .arg("--via")
        .arg(via_str)
        .current_dir(runner_root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawning worker process")?;

    // Capture the worker's stdout/stderr: forward it to the listener's own
    // descriptors (preserving the pre-pipe behavior of `inherit`) while
    // retaining the tail for the crash annotation. Draining also prevents a
    // chatty worker from blocking on a full pipe buffer.
    let worker_output = Arc::new(Mutex::new(WorkerOutputCapture::default()));
    if let Some(stdout) = child.stdout.take() {
        forward_worker_stream(stdout, worker_output.clone(), false);
    }
    if let Some(stderr) = child.stderr.take() {
        forward_worker_stream(stderr, worker_output.clone(), true);
    }

    // Send job message via stdin — but keep stdin open for cancel messages
    let mut stdin = child.stdin.take();
    if let Some(s) = &mut stdin {
        let msg = WorkerMessage::Job {
            body: job_message.clone(),
        };
        let line = serde_json::to_string(&msg)?;
        s.write_all(line.as_bytes()).await?;
        s.write_all(b"\n").await?;
        s.flush().await?;
        // Do NOT drop stdin — worker needs it open for cancel messages
    }

    Ok(RunningJob {
        child,
        stdin,
        job_message: Some(job_message),
        worker_output,
        request_id,
        job_id,
        kill_at: None,
        cancel_sent: false,
        agent_request_id,
    })
}

impl RunningJob {
    /// Tail of the worker's captured output (empty when the worker produced
    /// nothing). Used for the crash annotation on abnormal worker exit.
    pub fn worker_output_tail(&self) -> String {
        self.worker_output
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tail()
    }
}

/// Effective cancellation timing from official `JobDispatcher.Cancel`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancellationTiming {
    /// Timeout sent to the worker after the official 60-second clamp.
    pub effective_timeout_secs: u64,
    /// Forced-kill delay: effective timeout minus 15 seconds.
    pub kill_after_secs: u64,
}

/// Clamp a cancellation timeout and derive the official forced-kill delay.
pub fn cancellation_timing(timeout_secs: u64) -> CancellationTiming {
    let effective_timeout_secs = timeout_secs.max(60);
    CancellationTiming {
        effective_timeout_secs,
        kill_after_secs: effective_timeout_secs - 15,
    }
}

/// Parse a non-negative .NET invariant TimeSpan (`hh:mm:ss` or
/// `d.hh:mm:ss[.fffffff]`) into whole seconds, rounding a non-zero fractional
/// component up so the runner never kills earlier than the requested timeout.
pub fn parse_timespan_secs(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (days, clock) = match s.split_once('.') {
        Some((days, rest)) if days.chars().all(|c| c.is_ascii_digit()) && rest.contains(':') => {
            (days.parse::<u64>().ok()?, rest)
        }
        _ => (0, s),
    };
    let (clock, fraction) = match clock.split_once('.') {
        Some((clock, fraction)) => {
            if fraction.is_empty()
                || fraction.len() > 7
                || !fraction.chars().all(|c| c.is_ascii_digit())
            {
                return None;
            }
            (clock, Some(fraction))
        }
        None => (clock, None),
    };
    let mut parts = clock.split(':');
    let hours = parts.next()?.parse::<u64>().ok()?;
    let minutes = parts.next()?.parse::<u64>().ok()?;
    let seconds = parts.next()?.parse::<u64>().ok()?;
    if parts.next().is_some() || hours >= 24 || minutes >= 60 || seconds >= 60 {
        return None;
    }
    let whole = days
        .checked_mul(86_400)?
        .checked_add(hours.checked_mul(3_600)?)?
        .checked_add(minutes.checked_mul(60)?)?
        .checked_add(seconds)?;
    let round_up = fraction.is_some_and(|value| value.bytes().any(|digit| digit != b'0'));
    whole.checked_add(u64::from(round_up))
}

#[cfg(test)]
mod timespan_tests {
    use super::{cancellation_timing, parse_timespan_secs, WorkerMessage};
    use proptest::prelude::*;

    #[test]
    fn parses_hh_mm_ss() {
        assert_eq!(parse_timespan_secs("00:05:00"), Some(300));
        assert_eq!(parse_timespan_secs("01:00:00"), Some(3600));
    }

    #[test]
    fn parses_days() {
        assert_eq!(parse_timespan_secs("1.00:00:00"), Some(86400));
    }

    #[test]
    fn garbage_returns_none() {
        assert_eq!(parse_timespan_secs("not-a-timespan"), None);
        assert_eq!(parse_timespan_secs(""), None);
    }

    proptest! {
        #[test]
        fn run_time_01_parses_valid_timespans(
            days in 0_u64..=10_000,
            hours in 0_u64..24,
            minutes in 0_u64..60,
            seconds in 0_u64..60,
            fractional_tick in any::<bool>(),
        ) {
            let fraction = if fractional_tick { "1" } else { "0" };
            let input = format!("{days}.{hours:02}:{minutes:02}:{seconds:02}.{fraction}");
            let expected = days * 86_400
                + hours * 3_600
                + minutes * 60
                + seconds
                + u64::from(fractional_tick);
            prop_assert_eq!(
                parse_timespan_secs(&input),
                Some(expected),
                "RUN-TIME-01: valid TimeSpan must preserve its duration without an early fractional truncation",
            );
        }

        #[test]
        fn run_time_01_rejects_out_of_range_clock_fields(
            hours in 24_u64..=99,
            minutes in 60_u64..=99,
            seconds in 60_u64..=99,
        ) {
            prop_assert_eq!(parse_timespan_secs(&format!("{hours:02}:00:00")), None);
            prop_assert_eq!(parse_timespan_secs(&format!("00:{minutes:02}:00")), None);
            prop_assert_eq!(parse_timespan_secs(&format!("00:00:{seconds:02}")), None);
        }

        #[test]
        fn run_time_01_never_schedules_forced_kill_before_45_seconds(
            timeout_secs in any::<u64>(),
        ) {
            let timing = cancellation_timing(timeout_secs);
            prop_assert!(timing.effective_timeout_secs >= 60);
            prop_assert!(timing.kill_after_secs >= 45);
            prop_assert_eq!(timing.kill_after_secs, timing.effective_timeout_secs - 15);
        }

        #[test]
        fn run_scope_01_cancel_ipc_excludes_server_concurrency_metadata(
            timeout_secs in any::<u64>(),
        ) {
            let encoded = serde_json::to_value(WorkerMessage::Cancel { timeout_secs }).unwrap();
            let object = encoded.as_object().unwrap();
            prop_assert_eq!(object.len(), 2);
            prop_assert_eq!(object.get("t").and_then(serde_json::Value::as_str), Some("cancel"));
            prop_assert_eq!(
                object.get("timeout_secs").and_then(serde_json::Value::as_u64),
                Some(timeout_secs),
            );
            for server_only in ["concurrency", "group", "queue", "matrix", "reusable"] {
                prop_assert!(
                    !object.contains_key(server_only),
                    "RUN-SCOPE-01: runner IPC exposed server-only field {server_only}",
                );
            }
        }
    }
}

/// Blocking dispatch — spawns and waits. Used by the AzDO message listener
/// which doesn't need concurrent polling.
pub async fn dispatch_job(
    job_message: serde_json::Value,
    runner_root: &Path,
    via: ProtocolPath,
) -> Result<()> {
    let mut job = spawn_job(job_message, runner_root, via).await?;
    let request_id = job.request_id.clone();

    let success = job.wait().await?;
    if success {
        info!("Worker completed job {request_id} successfully");
    } else {
        error!("Worker failed for job {request_id}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::BoxFuture;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Instant;
    use tempfile::TempDir;

    /// Fake status provider for testing ensure_dispatch_finished.
    struct FakeStatusProvider {
        response: serde_json::Value,
        calls: Arc<AtomicUsize>,
    }

    impl AgentRequestStatusProvider for FakeStatusProvider {
        fn get_agent_request<'a>(
            &'a self,
            _token: &'a str,
            _pool_id: i64,
            _request_id: i64,
        ) -> BoxFuture<'a, Result<serde_json::Value>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(self.response.clone()) })
        }
    }

    #[tokio::test]
    async fn test_worker_dispatch_run_new_job() {
        let dir = TempDir::new().unwrap();
        let workspace_dir = dir.path().join("work");
        let payload = serde_json::json!({
            "jobId": "job-worker-1",
            "jobDisplayName": "Worker Job",
            "steps": [
                {
                    "id": "step-1",
                    "contextName": "step1",
                    "displayName": "Step One",
                    "run": "echo step-one-executed",
                    "shell": "bash"
                }
            ],
            "fileTable": {
                "workDirectory": workspace_dir.to_str().unwrap()
            }
        });

        let mut running = spawn_job(payload, dir.path(), ProtocolPath::Broker)
            .await
            .unwrap();

        let success = running.wait().await.unwrap();
        assert!(success);
    }

    #[tokio::test]
    async fn test_worker_dispatch_cancellation() {
        let dir = TempDir::new().unwrap();
        let workspace_dir = dir.path().join("work");
        let payload = serde_json::json!({
            "jobId": "job-worker-cancel",
            "jobDisplayName": "Cancel Job",
            "steps": [
                {
                    "id": "step-1",
                    "contextName": "step1",
                    "displayName": "Step One",
                    "run": "sleep 60",
                    "shell": "bash"
                }
            ],
            "fileTable": {
                "workDirectory": workspace_dir.to_str().unwrap()
            }
        });

        let start = Instant::now();
        let mut running = spawn_job(payload, dir.path(), ProtocolPath::Broker)
            .await
            .unwrap();

        // Let it start up
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Cancel the job
        running.cancel(5).await;

        let success = running.wait().await.unwrap();
        let elapsed = start.elapsed();

        // The job should succeed/exit (with Ok status since cancellation is handled gracefully)
        assert!(success);
        // The worker intentionally gives the child process group SIGINT then
        // SIGTERM before SIGKILL; allow both test grace periods plus startup
        // scheduling overhead while still rejecting the ten-second payload.
        assert!(
            elapsed.as_secs() < 20,
            "Expected cancellation to exit within the grace budget, took {:?}",
            elapsed
        );
    }

    // --- P1 job dispatcher gap coverage ---

    #[test]
    fn worker_message_job_serialization() {
        let msg = WorkerMessage::Job {
            body: serde_json::json!({"jobId": "test-1", "steps": []}),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"t\":\"job\""));
        assert!(json.contains("\"body\""));
        assert!(json.contains("test-1"));
    }

    #[test]
    fn worker_message_cancel_serialization() {
        let msg = WorkerMessage::Cancel { timeout_secs: 300 };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"t\":\"cancel\""));
        assert!(json.contains("300"));
    }

    #[test]
    fn worker_message_shutdown_serialization() {
        let msg = WorkerMessage::Shutdown;
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"t\":\"shutdown\""));
    }

    #[test]
    fn worker_message_roundtrip_all_types() {
        // Job
        let job = WorkerMessage::Job {
            body: serde_json::json!({"k": "v"}),
        };
        let j: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&job).unwrap()).unwrap();
        assert_eq!(j["t"], "job");
        assert_eq!(j["body"]["k"], "v");

        // Cancel
        let cancel = WorkerMessage::Cancel { timeout_secs: 60 };
        let c: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&cancel).unwrap()).unwrap();
        assert_eq!(c["t"], "cancel");
        assert_eq!(c["timeout_secs"], 60);

        // Shutdown
        let shutdown = WorkerMessage::Shutdown;
        let s: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&shutdown).unwrap()).unwrap();
        assert_eq!(s["t"], "shutdown");
    }

    #[tokio::test]
    async fn spawn_job_extracts_request_id_from_job_id() {
        let dir = TempDir::new().unwrap();
        let payload = serde_json::json!({
            "jobId": "my-unique-job-42",
            "jobDisplayName": "Test",
            "steps": [],
            "fileTable": {
                "workDirectory": dir.path().join("work").to_str().unwrap()
            }
        });
        let mut running = spawn_job(payload, dir.path(), ProtocolPath::Broker)
            .await
            .unwrap();
        assert_eq!(running.request_id, "my-unique-job-42");
        running.kill().await;
    }

    #[tokio::test]
    async fn spawn_job_extracts_request_id_from_fallback() {
        let dir = TempDir::new().unwrap();
        let payload = serde_json::json!({
            "requestId": "fallback-id",
            "jobDisplayName": "Test",
            "steps": [],
            "fileTable": {
                "workDirectory": dir.path().join("work").to_str().unwrap()
            }
        });
        let mut running = spawn_job(payload, dir.path(), ProtocolPath::Broker)
            .await
            .unwrap();
        assert_eq!(running.request_id, "fallback-id");
        running.kill().await;
    }

    #[test]
    fn worker_output_capture_keeps_bounded_tail() {
        let mut capture = WorkerOutputCapture::default();
        for i in 0..10_000 {
            capture.push(format!("line {i} {}", "x".repeat(100)));
        }
        let tail = capture.tail();
        assert!(
            tail.len() <= WorkerOutputCapture::MAX_BYTES,
            "capture exceeded cap: {}",
            tail.len()
        );
        // The tail is what survives — the newest lines are present.
        assert!(tail.contains("line 9999"), "tail must keep newest output");
    }

    /// A worker that emits a non-UTF-8 byte sequence must not silence the rest
    /// of its output: `Stdio::inherit()` passed raw bytes through, and the
    /// crash capture has to keep receiving the lines that follow. Fails if the
    /// reader goes back to `lines()` with `while let Ok(Some(..))`, which ends
    /// the task at the first decode error.
    #[tokio::test]
    async fn forward_worker_stream_survives_invalid_utf8() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"before-invalid\n");
        // Lone continuation bytes: never valid UTF-8 in any position.
        bytes.extend_from_slice(&[b'b', b'a', b'd', 0xff, 0xfe, b'\n']);
        bytes.extend_from_slice(b"after-invalid-1\n");
        bytes.extend_from_slice(b"after-invalid-2\n");

        let capture = Arc::new(Mutex::new(WorkerOutputCapture::default()));
        forward_worker_stream(std::io::Cursor::new(bytes), capture.clone(), false);

        let deadline = Instant::now() + Duration::from_secs(5);
        let tail = loop {
            let tail = capture.lock().unwrap_or_else(|e| e.into_inner()).tail();
            if tail.contains("after-invalid-2") || Instant::now() >= deadline {
                break tail;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };

        assert!(tail.contains("before-invalid"), "tail: {tail:?}");
        assert!(
            tail.contains("after-invalid-1") && tail.contains("after-invalid-2"),
            "lines after invalid UTF-8 must still be captured, got: {tail:?}"
        );
        // The undecodable line is kept, lossily, rather than dropped.
        assert!(tail.contains("bad\u{fffd}"), "tail: {tail:?}");
    }

    #[tokio::test]
    async fn spawn_job_captures_worker_stdout() {
        let dir = TempDir::new().unwrap();
        let workspace_dir = dir.path().join("work");
        let payload = serde_json::json!({
            "jobId": "capture-job",
            "jobDisplayName": "Capture Job",
            "steps": [
                {
                    "id": "step-1",
                    "contextName": "step1",
                    "displayName": "Step One",
                    "run": "echo worker-capture-marker",
                    "shell": "bash"
                }
            ],
            "fileTable": {
                "workDirectory": workspace_dir.to_str().unwrap()
            }
        });

        let mut running = spawn_job(payload, dir.path(), ProtocolPath::Broker)
            .await
            .unwrap();
        assert!(running.wait().await.unwrap());
        // Give the forward tasks a moment to drain the pipes after exit.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let tail = running.worker_output_tail();
        assert!(
            tail.contains("Worker received job"),
            "worker process output must be captured, got: {tail:?}"
        );
    }

    #[tokio::test]
    async fn shutdown_gracefully_lets_worker_cancel_and_exit() {
        let dir = TempDir::new().unwrap();
        let workspace_dir = dir.path().join("work");
        let payload = serde_json::json!({
            "jobId": "shutdown-job",
            "jobDisplayName": "Shutdown Job",
            "steps": [
                {
                    "id": "step-1",
                    "contextName": "step1",
                    "displayName": "Long Step",
                    "run": "sleep 120",
                    "shell": "bash"
                }
            ],
            "fileTable": {
                "workDirectory": workspace_dir.to_str().unwrap()
            }
        });

        let mut running = spawn_job(payload, dir.path(), ProtocolPath::Broker)
            .await
            .unwrap();
        // Let the worker start the step, then shut it down.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let start = Instant::now();
        let outcome = running.shutdown_gracefully(Duration::from_secs(30)).await;
        let elapsed = start.elapsed();
        assert_eq!(
            outcome,
            Some(true),
            "worker must exit cleanly after the shutdown message"
        );
        assert!(
            elapsed < Duration::from_secs(30),
            "graceful shutdown must finish within the grace, took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn terminal_zombie_is_cancelled_before_next_dispatch() {
        let dir = TempDir::new().unwrap();
        let payload = serde_json::json!({
            "jobId": "zombie-job",
            "requestId": 41,
            "steps": [{"run": "sleep 60", "shell": "bash"}],
            "fileTable": {"workDirectory": dir.path().join("work").to_str().unwrap()}
        });
        let mut previous = spawn_job(payload, dir.path(), ProtocolPath::Azdo)
            .await
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = FakeStatusProvider {
            response: serde_json::json!({"requestId": 41, "result": "succeeded"}),
            calls: calls.clone(),
        };
        ensure_dispatch_finished(&mut previous, "token", 1, &provider)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let next = serde_json::json!({
            "jobId": "next-job",
            "steps": [{"run": "echo next", "shell": "bash"}],
            "fileTable": {"workDirectory": dir.path().join("next").to_str().unwrap()}
        });
        let mut next = spawn_job(next, dir.path(), ProtocolPath::Azdo)
            .await
            .unwrap();
        assert!(next.wait().await.unwrap());
    }

    #[tokio::test]
    async fn active_overlap_is_fatal_and_does_not_drop_job_silently() {
        let dir = TempDir::new().unwrap();
        let payload = serde_json::json!({
            "jobId": "active-job",
            "requestId": 42,
            "steps": [{"run": "sleep 60", "shell": "bash"}],
            "fileTable": {"workDirectory": dir.path().join("work").to_str().unwrap()}
        });
        let mut previous = spawn_job(payload, dir.path(), ProtocolPath::Azdo)
            .await
            .unwrap();
        let provider = FakeStatusProvider {
            response: serde_json::json!({"requestId": 42, "result": null}),
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let error = ensure_dispatch_finished(&mut previous, "token", 1, &provider)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("hasn't finished"));
        previous.kill().await;
    }

    #[tokio::test]
    async fn completed_worker_dispatches_normally_without_status_probe() {
        let dir = TempDir::new().unwrap();
        let payload = serde_json::json!({
            "jobId": "normal-job",
            "requestId": 43,
            "steps": [{"run": "echo normal", "shell": "bash"}],
            "fileTable": {"workDirectory": dir.path().join("work").to_str().unwrap()}
        });
        let mut previous = spawn_job(payload, dir.path(), ProtocolPath::Azdo)
            .await
            .unwrap();
        assert!(previous.wait().await.unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = FakeStatusProvider {
            response: serde_json::json!({"result": null}),
            calls: calls.clone(),
        };
        ensure_dispatch_finished(&mut previous, "token", 1, &provider)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
