//! Bounded waits for external dependencies.
//!
//! Every helper in this module turns a potentially unbounded wait into a
//! failure at a caller-supplied deadline. They exist because the engine calls
//! out to processes and OS services that can block forever — a hung `git`, a
//! container runtime waiting on a daemon socket, a keychain prompt nobody can
//! answer — and a single such wait must never freeze the engine.
//!
//! Rules shared by every helper:
//!
//! - The deadline covers the whole operation, including process spawn, output
//!   draining, and reaping; it is not restarted per step.
//! - Child output is drained concurrently with the wait. A child that fills a
//!   64 KiB pipe buffer blocks on `write` and looks exactly like a hang, so a
//!   naive wait would either time out spuriously or deadlock on the pipe.
//!   Capture is unbounded by default; the `_capped` variants keep at most a
//!   caller-supplied number of bytes per stream and discard the excess *while
//!   still reading to EOF*, so a cap bounds memory without reintroducing the
//!   deadlock.
//! - Expiry kills the child, then reaps it under a short grace period. The
//!   error names the caller's `what` and the deadline so an operator can tell
//!   which dependency hung. [`run_command_status_in_group`] kills the child's
//!   whole process group instead, for shells whose real work is in a
//!   grandchild; dropping any of these futures is the same cancellation as
//!   expiry, so the guard kills with it.
//! - Nothing here configures environment variables. Callers apply their own
//!   non-interactive policy (`GIT_TERMINAL_PROMPT`, `GIT_ASKPASS`, …) to the
//!   [`Command`] they pass in.
//! - These runners are ordinary futures, but they are not small ones. Awaiting
//!   one several frames deep inside handler/future chains grows the enclosing
//!   future by their state, which a debug build can push over a 2 MiB
//!   test-thread stack. Where a call site is awaited through several nested
//!   futures (proxy dispatch, snapshot handlers), box it:
//!   `Box::pin(run_command(cmd, deadline, what)).await`.
//!
//! The async helpers use [`tokio::process`]; [`run_command_blocking`] and
//! [`DeadlineGuard::run`] have no runtime dependency and are safe to use from
//! `Drop` implementations and other synchronous contexts.

use std::fmt;
use std::io;
use std::process::{ExitStatus, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};

/// How long a killed child may take to be reaped before it is abandoned.
///
/// Kept short: the point of the deadline is to return to the caller, not to
/// wait for every descendant. A child that survives its own `SIGKILL` is
/// unkillable and reaped by the OS when the engine exits.
const KILL_REAP_GRACE: Duration = Duration::from_secs(2);

/// How often the synchronous runner polls for child exit.
const POLL_INTERVAL: Duration = Duration::from_millis(25);

// ---------------------------------------------------------------------------
// Shared error values
// ---------------------------------------------------------------------------

/// One deadline expiry: what was being waited on, and for how long.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadlineExceeded {
    what: String,
    deadline: Duration,
}

impl DeadlineExceeded {
    /// Create a timeout value naming the operation and its deadline.
    pub fn new(what: impl Into<String>, deadline: Duration) -> Self {
        Self {
            what: what.into(),
            deadline,
        }
    }

    /// The caller-supplied description of the dependency operation.
    pub fn what(&self) -> &str {
        &self.what
    }

    /// The deadline that expired.
    pub fn deadline(&self) -> Duration {
        self.deadline
    }
}

impl fmt::Display for DeadlineExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} timed out after {:?}", self.what, self.deadline)
    }
}

impl std::error::Error for DeadlineExceeded {}

/// Error from an async or synchronous bounded command.
#[derive(Debug)]
pub enum CommandError {
    /// The deadline expired; the child was killed and reaped.
    TimedOut {
        /// The expired deadline.
        exceeded: DeadlineExceeded,
        /// The program that was run.
        program: String,
    },
    /// The process could not be started at all.
    Spawn {
        /// The caller-supplied description of the operation.
        what: String,
        /// The program that was run.
        program: String,
        /// The spawn failure.
        source: io::Error,
    },
    /// The process started but a wait, read, or write failed.
    Io {
        /// The caller-supplied description of the operation.
        what: String,
        /// The program that was run.
        program: String,
        /// The I/O failure.
        source: io::Error,
    },
}

impl CommandError {
    fn timeout_error(what: &str, program: &str, deadline: Duration) -> Self {
        Self::TimedOut {
            exceeded: DeadlineExceeded::new(what, deadline),
            program: program.to_owned(),
        }
    }

    fn spawn(what: &str, program: String, source: io::Error) -> Self {
        Self::Spawn {
            what: what.to_owned(),
            program,
            source,
        }
    }

    fn io(what: &str, program: &str, source: io::Error) -> Self {
        Self::Io {
            what: what.to_owned(),
            program: program.to_owned(),
            source,
        }
    }

    /// Whether this error is a deadline expiry (as opposed to a spawn or I/O
    /// failure).
    pub fn timed_out(&self) -> bool {
        matches!(self, Self::TimedOut { .. })
    }

    /// The caller-supplied description of the operation.
    pub fn what(&self) -> &str {
        match self {
            Self::TimedOut { exceeded, .. } => exceeded.what(),
            Self::Spawn { what, .. } | Self::Io { what, .. } => what,
        }
    }

    /// The program that was run.
    pub fn program(&self) -> &str {
        match self {
            Self::TimedOut { program, .. }
            | Self::Spawn { program, .. }
            | Self::Io { program, .. } => program,
        }
    }

    /// The expired deadline, when this is a timeout.
    pub fn deadline(&self) -> Option<Duration> {
        match self {
            Self::TimedOut { exceeded, .. } => Some(exceeded.deadline()),
            Self::Spawn { .. } | Self::Io { .. } => None,
        }
    }
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TimedOut { exceeded, program } => {
                write!(f, "{exceeded}: killed the hung {program} process")
            }
            Self::Spawn {
                what,
                program,
                source,
            } => write!(f, "could not start {program} for {what}: {source}"),
            Self::Io {
                what,
                program,
                source,
            } => write!(f, "{what}: failed while running {program}: {source}"),
        }
    }
}

impl std::error::Error for CommandError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Spawn { source, .. } | Self::Io { source, .. } => Some(source),
            Self::TimedOut { .. } => None,
        }
    }
}

/// Error from [`run_blocking`].
#[derive(Debug, Clone)]
pub enum BlockingError {
    /// The deadline expired. The blocking closure may still be running.
    TimedOut(DeadlineExceeded),
    /// The blocking task panicked or was cancelled before returning.
    Aborted {
        /// The caller-supplied description of the operation.
        what: String,
        /// The join failure, rendered.
        reason: String,
    },
}

impl BlockingError {
    /// Whether this error is a deadline expiry.
    pub fn timed_out(&self) -> bool {
        matches!(self, Self::TimedOut(_))
    }

    /// The caller-supplied description of the operation.
    pub fn what(&self) -> &str {
        match self {
            Self::TimedOut(exceeded) => exceeded.what(),
            Self::Aborted { what, .. } => what,
        }
    }
}

impl fmt::Display for BlockingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TimedOut(exceeded) => write!(f, "{exceeded}"),
            Self::Aborted { what, reason } => {
                write!(f, "blocking work for {what} did not return: {reason}")
            }
        }
    }
}

impl std::error::Error for BlockingError {}

// ---------------------------------------------------------------------------
// Async process runner
// ---------------------------------------------------------------------------

fn program_name(cmd: &Command) -> String {
    cmd.as_std().get_program().to_string_lossy().into_owned()
}

async fn read_pipe<R: AsyncRead + Unpin>(
    pipe: Option<R>,
    max_capture: usize,
) -> io::Result<(Vec<u8>, bool)> {
    let Some(mut pipe) = pipe else {
        return Ok((Vec::new(), false));
    };
    let mut captured = Vec::new();
    let mut truncated = false;
    let mut buffer = [0_u8; 8192];
    loop {
        let read = pipe.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let keep = (max_capture - captured.len()).min(read);
        captured.extend_from_slice(&buffer[..keep]);
        truncated |= keep < read;
    }
    Ok((captured, truncated))
}

/// Kill the child and reap it, but never block the caller past the grace
/// period. A grandchild holding the pipe open does not extend the wait.
async fn kill_and_reap(child: &mut Child) {
    // `kill_on_drop` is set by every public runner, but an explicit kill
    // returns the error to the caller instead of relying on drop timing.
    let _ = child.start_kill();
    let _ = tokio::time::timeout(KILL_REAP_GRACE, child.wait()).await;
}

async fn wait_with_capture(
    mut child: Child,
    deadline: Duration,
    started: Instant,
    what: &str,
    program: &str,
    max_capture: usize,
) -> Result<(Output, bool), CommandError> {
    let remaining = deadline.saturating_sub(started.elapsed());
    let capture = async {
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        // All four run concurrently: a child that fills one pipe while we wait
        // on it can never be mistaken for a hang. A capped drain still reads
        // to EOF, discarding the excess, so the promise holds with a cap too.
        let (stdout, stderr, status) = tokio::join!(
            read_pipe(stdout, max_capture),
            read_pipe(stderr, max_capture),
            child.wait(),
        );
        let (stdout, stdout_truncated) =
            stdout.map_err(|source| CommandError::io(what, program, source))?;
        let (stderr, stderr_truncated) =
            stderr.map_err(|source| CommandError::io(what, program, source))?;
        let status = status.map_err(|source| CommandError::io(what, program, source))?;
        Ok((
            Output {
                status,
                stdout,
                stderr,
            },
            stdout_truncated || stderr_truncated,
        ))
    };
    match tokio::time::timeout(remaining, capture).await {
        Ok(result) => result,
        Err(_) => {
            kill_and_reap(&mut child).await;
            Err(CommandError::timeout_error(what, program, deadline))
        }
    }
}

/// Run a pre-configured command to completion, bounded by `deadline`.
///
/// The helper owns the process policy that must never be optional:
/// `kill_on_drop(true)`, `stdin` from `/dev/null` (no child can wait on an
/// interactive prompt), and piped `stdout`/`stderr` drained concurrently with
/// the wait. Everything else — program, arguments, working directory, the
/// complete environment — belongs to the caller.
///
/// `deadline` covers spawn, output capture, and waiting. On expiry the child
/// is killed and reaped within a short grace period, and the returned
/// [`CommandError`] names `what`, the program, and the deadline. Captured
/// output is returned even for a non-zero exit status; success is the
/// caller's decision.
///
/// Output is unbounded. Callers that must cap what a chatty child can make the
/// engine buffer use [`run_command_capped`], which still drains the pipes to
/// EOF and discards the excess.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), preloop_observability::deadline::CommandError> {
/// use std::time::Duration;
/// use tokio::process::Command;
///
/// let mut cmd = Command::new("git");
/// cmd.args(["ls-remote", "--heads", "origin"]);
/// let output = preloop_observability::deadline::run_command(
///     cmd,
///     Duration::from_secs(60),
///     "git ls-remote origin",
/// )
/// .await?;
/// assert!(output.status.success());
/// # Ok(())
/// # }
/// ```
pub async fn run_command(
    cmd: Command,
    deadline: Duration,
    what: &str,
) -> Result<Output, CommandError> {
    run_command_with_limit(cmd, deadline, what, usize::MAX)
        .await
        .map(|(output, _)| output)
}

/// As [`run_command`], with a cap on captured output.
///
/// At most `max_capture` bytes are kept per stream; the excess is discarded
/// but **read**, so a child that out-writes the cap still reaches EOF instead
/// of blocking on a full pipe. The returned flag is true when either stream
/// was truncated, and the returned [`Output`] holds only the retained bytes.
///
/// The cap bounds engine memory, not the deadline: a child that writes forever
/// is still killed when `deadline` expires.
pub async fn run_command_capped(
    cmd: Command,
    deadline: Duration,
    what: &str,
    max_capture: usize,
) -> Result<(Output, bool), CommandError> {
    run_command_with_limit(cmd, deadline, what, max_capture).await
}

async fn run_command_with_limit(
    mut cmd: Command,
    deadline: Duration,
    what: &str,
    max_capture: usize,
) -> Result<(Output, bool), CommandError> {
    let started = Instant::now();
    let program = program_name(&cmd);
    cmd.kill_on_drop(true);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    let child = cmd
        .spawn()
        .map_err(|source| CommandError::spawn(what, program.clone(), source))?;
    wait_with_capture(child, deadline, started, what, &program, max_capture).await
}

/// Run a command with bytes written to its stdin, bounded by `deadline`.
///
/// The deadline covers spawn, writing `input`, draining both output pipes, and
/// the final wait. A child that never reads its stdin blocks the write; that
/// case is a deadline expiry like any other, and the child is killed instead
/// of leaving a writer wedged forever.
pub async fn run_command_with_stdin(
    mut cmd: Command,
    input: &[u8],
    deadline: Duration,
    what: &str,
) -> Result<Output, CommandError> {
    let started = Instant::now();
    let program = program_name(&cmd);
    cmd.kill_on_drop(true);
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    let mut child = cmd
        .spawn()
        .map_err(|source| CommandError::spawn(what, program.clone(), source))?;

    let remaining = deadline.saturating_sub(started.elapsed());
    let mut stdin = child.stdin.take();
    let exchange = async {
        let write = async {
            match stdin.take() {
                Some(mut pipe) => {
                    pipe.write_all(input).await?;
                    pipe.shutdown().await
                }
                None => Ok(()),
            }
        };
        let (written, stdout, stderr, status) = tokio::join!(
            write,
            read_pipe(child.stdout.take(), usize::MAX),
            read_pipe(child.stderr.take(), usize::MAX),
            child.wait(),
        );
        written.map_err(|source| CommandError::io(what, &program, source))?;
        let (stdout, _) = stdout.map_err(|source| CommandError::io(what, &program, source))?;
        let (stderr, _) = stderr.map_err(|source| CommandError::io(what, &program, source))?;
        let status = status.map_err(|source| CommandError::io(what, &program, source))?;
        Ok(Output {
            status,
            stdout,
            stderr,
        })
    };
    match tokio::time::timeout(remaining, exchange).await {
        Ok(result) => result,
        Err(_) => {
            kill_and_reap(&mut child).await;
            Err(CommandError::timeout_error(what, &program, deadline))
        }
    }
}

/// Run a command and return only its exit status, bounded by `deadline`.
///
/// Unlike [`run_command`], output is not captured: the caller's `stdout` and
/// `stderr` configuration is left untouched (inherited by default). This is
/// the variant for long-running installers whose output should reach the
/// engine's own streams. Do not set piped stdio here — nothing drains it, so a
/// chatty child would fill the pipe and stall; use [`run_command`] instead.
pub async fn run_command_status(
    mut cmd: Command,
    deadline: Duration,
    what: &str,
) -> Result<ExitStatus, CommandError> {
    let started = Instant::now();
    let program = program_name(&cmd);
    cmd.kill_on_drop(true);
    cmd.stdin(Stdio::null());
    let mut child = cmd
        .spawn()
        .map_err(|source| CommandError::spawn(what, program.clone(), source))?;
    let remaining = deadline.saturating_sub(started.elapsed());
    match tokio::time::timeout(remaining, child.wait()).await {
        Ok(Ok(status)) => Ok(status),
        Ok(Err(source)) => Err(CommandError::io(what, &program, source)),
        Err(_) => {
            kill_and_reap(&mut child).await;
            Err(CommandError::timeout_error(what, &program, deadline))
        }
    }
}

/// Run a command as the leader of its own process group, bounded by `deadline`.
///
/// The same contract as [`run_command_status`], with one addition: the child
/// starts a fresh process group, and expiry — or dropping the returned future,
/// which is what a caller's `select!` against a shutdown token does — SIGKILLs
/// every member of that group instead of only the direct child.
///
/// A shell wrapper is why: `sh -c '… curl … tar …'` does its real work in a
/// grandchild, so killing the shell alone leaves the transfer or the extraction
/// running behind the failure the caller already reported, and a repeated start
/// leaks another one. Use this wherever the command is a script that spawns
/// children of its own. On targets without process groups this is
/// [`run_command_status`].
pub async fn run_command_status_in_group(
    mut cmd: Command,
    deadline: Duration,
    what: &str,
) -> Result<ExitStatus, CommandError> {
    #[cfg(unix)]
    cmd.process_group(0);
    let started = Instant::now();
    let program = program_name(&cmd);
    cmd.kill_on_drop(true);
    cmd.stdin(Stdio::null());
    let mut child = cmd
        .spawn()
        .map_err(|source| CommandError::spawn(what, program.clone(), source))?;
    // Declared after `child`, so a dropped future signals the group before the
    // child's own kill-on-drop runs.
    let mut group = ProcessGroup::of(&child);
    let remaining = deadline.saturating_sub(started.elapsed());
    match tokio::time::timeout(remaining, child.wait()).await {
        Ok(Ok(status)) => {
            // Collected: the group has no live leader left to sweep, and its id
            // may already belong to somebody else.
            group.disarm();
            Ok(status)
        }
        Ok(Err(source)) => {
            // The wait failed, so the child's state is unknown; sweep the group
            // rather than leave whatever it spawned behind the error.
            group.kill();
            Err(CommandError::io(what, &program, source))
        }
        Err(_) => {
            group.kill();
            kill_and_reap(&mut child).await;
            Err(CommandError::timeout_error(what, &program, deadline))
        }
    }
}

/// The process group of a child that started one, signalled on drop.
///
/// The guard is what makes a *dropped* bounded runner as clean as an expired
/// one: without it, only the direct child dies and the group's other members
/// are reparented and outlive the wait.
struct ProcessGroup {
    #[cfg(unix)]
    id: Option<i32>,
}

impl ProcessGroup {
    /// The group of a freshly spawned leader, or an inert guard when none can
    /// be addressed.
    fn of(child: &Child) -> Self {
        #[cfg(unix)]
        {
            // `process_group(0)` makes the child the leader of a fresh group,
            // so its pid *is* the group id. That id cannot be this process's
            // own group: a live member pins its group's id — we are one — so
            // the number cannot be handed to a new process. `kill` reads an id
            // of zero or less as "this process's own group", which is why a
            // non-positive id is never used, and a child whose group attribute
            // silently did not apply leaves a pid that names no group: the
            // signal then reaches nobody, which is the safe direction.
            let id = child
                .id()
                .and_then(|id| i32::try_from(id).ok())
                .filter(|id| *id > 0);
            Self { id }
        }
        #[cfg(not(unix))]
        {
            let _ = child;
            Self {}
        }
    }

    /// Stop signalling on drop: the leader was collected, so the group is
    /// drained and its id is free for the OS to hand to somebody else.
    fn disarm(&mut self) {
        #[cfg(unix)]
        {
            self.id = None;
        }
    }

    /// SIGKILL every member now. An error means the group has already drained,
    /// which is the outcome the caller wanted anyway.
    fn kill(&mut self) {
        #[cfg(unix)]
        {
            if let Some(id) = self.id.take() {
                kill_process_group(id);
            }
        }
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        self.kill();
    }
}

/// SIGKILL a whole process group, by id.
///
/// The direct call would be `libc::killpg` (or its safe wrapper, `nix`), and
/// neither is available here: the workspace forbids `unsafe`, and a dependency
/// change may not share a pull request with source, so this crate carries no
/// signal dependency. `kill` with a negative pid is the same call
/// [`run_command_status_in_group`]'s callers need, and the same tool
/// `preloop-vm` already uses as `kill -9 <pid>` for orphaned hypervisors. It
/// runs through [`run_command_blocking`] like every other child here: bounded,
/// with the pipes drained so a chatty `kill` cannot stall the deadline path.
#[cfg(unix)]
fn kill_process_group(id: i32) {
    let mut kill = std::process::Command::new("kill");
    kill.arg("-9").arg(format!("-{id}"));
    let _ = run_command_blocking(kill, KILL_REAP_GRACE, "process-group SIGKILL");
}

// ---------------------------------------------------------------------------
// Synchronous process runner
// ---------------------------------------------------------------------------

enum PipeKind {
    Stdout,
    Stderr,
}

/// One captured stream: the retained bytes and whether the cap truncated it.
type Captured = (Vec<u8>, bool);

/// A finished capture from one reader thread, tagged with its stream.
type ReaderMessage = (PipeKind, io::Result<Captured>);

fn spawn_pipe_reader<R: io::Read + Send + 'static>(
    pipe: Option<R>,
    kind: PipeKind,
    sender: std::sync::mpsc::Sender<ReaderMessage>,
    max_capture: usize,
) -> io::Result<()> {
    let Some(mut pipe) = pipe else {
        return Ok(());
    };
    std::thread::Builder::new()
        .name(match kind {
            PipeKind::Stdout => "deadline-stdout".to_owned(),
            PipeKind::Stderr => "deadline-stderr".to_owned(),
        })
        .spawn(move || {
            let mut captured = Vec::new();
            let mut truncated = false;
            let mut buffer = [0_u8; 8192];
            let result = loop {
                match pipe.read(&mut buffer) {
                    Ok(0) => break Ok(()),
                    Ok(read) => {
                        let keep = (max_capture - captured.len()).min(read);
                        captured.extend_from_slice(&buffer[..keep]);
                        truncated |= keep < read;
                    }
                    Err(error) => break Err(error),
                }
            }
            .map(|()| (captured, truncated));
            // The receiver is gone only when the call already timed out; the
            // thread then exits at its next EOF and nothing is lost.
            let _ = sender.send((kind, result));
        })
        .map(|_| ())
}

/// Kill the child and poll for its reaping, never blocking past the grace
/// period.
fn kill_and_reap_blocking(child: &mut std::process::Child) {
    let _ = child.kill();
    let reap_deadline = Instant::now() + KILL_REAP_GRACE;
    loop {
        match child.try_wait() {
            Ok(Some(_)) | Err(_) => break,
            Ok(None) if Instant::now() < reap_deadline => std::thread::sleep(POLL_INTERVAL),
            Ok(None) => break,
        }
    }
}

/// Run a pre-configured [`std::process::Command`] to completion, bounded by
/// `deadline`, from synchronous code.
///
/// This is the synchronous twin of [`run_command`], for contexts where there
/// is no tokio runtime (`Drop` implementations, CLI setup code, threads that
/// already are blocking). It has the same process policy — `stdin` from
/// `/dev/null`, piped `stdout`/`stderr` captured into an [`Output`] — but each
/// pipe is drained by its own thread, so a child that fills a pipe buffer
/// cannot masquerade as a hang. The child is polled every 25 ms, killed at the
/// deadline, and reaped within a bounded grace period.
///
/// Output collection also stays inside the deadline: if a descendant inherits
/// the pipes and keeps them open after the direct child exits, the call fails
/// rather than waiting on that descendant forever. Reader threads are detached
/// on expiry and exit when the pipes finally close.
///
/// Output is unbounded; use [`run_command_blocking_capped`] to bound what a
/// chatty child can make the engine buffer.
pub fn run_command_blocking(
    cmd: std::process::Command,
    deadline: Duration,
    what: &str,
) -> Result<Output, CommandError> {
    run_command_blocking_capped(cmd, deadline, what, usize::MAX).map(|(output, _)| output)
}

/// As [`run_command_blocking`], with a cap on captured output.
///
/// At most `max_capture` bytes are kept per stream; the excess is discarded
/// but **read**, so a child that out-writes the cap still reaches EOF instead
/// of blocking on a full pipe. The returned flag is true when either stream
/// was truncated, and the returned [`Output`] holds only the retained bytes.
pub fn run_command_blocking_capped(
    mut cmd: std::process::Command,
    deadline: Duration,
    what: &str,
    max_capture: usize,
) -> Result<(Output, bool), CommandError> {
    let started = Instant::now();
    let program = cmd.get_program().to_string_lossy().into_owned();
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    let mut child = cmd
        .spawn()
        .map_err(|source| CommandError::spawn(what, program.clone(), source))?;

    let (sender, receiver) = std::sync::mpsc::channel();
    let readers = (|| -> io::Result<()> {
        spawn_pipe_reader(
            child.stdout.take(),
            PipeKind::Stdout,
            sender.clone(),
            max_capture,
        )?;
        spawn_pipe_reader(
            child.stderr.take(),
            PipeKind::Stderr,
            sender.clone(),
            max_capture,
        )
    })();
    // Only the reader threads hold senders now; a disconnect means they are
    // gone, not that more output is coming.
    drop(sender);
    if let Err(source) = readers {
        kill_and_reap_blocking(&mut child);
        return Err(CommandError::io(what, &program, source));
    }

    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {}
            Err(source) => {
                kill_and_reap_blocking(&mut child);
                return Err(CommandError::io(what, &program, source));
            }
        }
        let elapsed = started.elapsed();
        if elapsed >= deadline {
            kill_and_reap_blocking(&mut child);
            return Err(CommandError::timeout_error(what, &program, deadline));
        }
        std::thread::sleep(POLL_INTERVAL.min(deadline - elapsed));
    };

    // Collect both captures. The reader threads finish as soon as the child's
    // pipe ends close, which the observed exit guarantees for well-behaved
    // children. A descendant holding a pipe open cannot extend this past the
    // overall deadline.
    let mut stdout = None;
    let mut stderr = None;
    while stdout.is_none() || stderr.is_none() {
        match receiver.try_recv() {
            Ok((PipeKind::Stdout, result)) => stdout = Some(result),
            Ok((PipeKind::Stderr, result)) => stderr = Some(result),
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                return Err(CommandError::io(
                    what,
                    &program,
                    io::Error::other("output reader thread exited without reporting"),
                ));
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                let remaining = deadline.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    return Err(CommandError::timeout_error(what, &program, deadline));
                }
                match receiver.recv_timeout(remaining) {
                    Ok((PipeKind::Stdout, result)) => stdout = Some(result),
                    Ok((PipeKind::Stderr, result)) => stderr = Some(result),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        return Err(CommandError::timeout_error(what, &program, deadline));
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        return Err(CommandError::io(
                            what,
                            &program,
                            io::Error::other("output reader thread exited without reporting"),
                        ));
                    }
                }
            }
        }
    }
    let (stdout, stdout_truncated) = stdout
        .expect("stdout is collected before the loop exits")
        .map_err(|source| CommandError::io(what, &program, source))?;
    let (stderr, stderr_truncated) = stderr
        .expect("stderr is collected before the loop exits")
        .map_err(|source| CommandError::io(what, &program, source))?;
    Ok((
        Output {
            status,
            stdout,
            stderr,
        },
        stdout_truncated || stderr_truncated,
    ))
}

// ---------------------------------------------------------------------------
// Blocking closure runner
// ---------------------------------------------------------------------------

/// Run a synchronous closure off the async workers, bounded by `deadline`.
///
/// The closure runs on tokio's blocking pool, so a long call never stalls the
/// async executor. `deadline` bounds how long the caller waits; on expiry this
/// returns [`BlockingError::TimedOut`] naming `what` and the deadline.
///
/// **The closure cannot be cancelled.** A blocking call has no deadline of its
/// own, so an expired closure keeps its thread until it returns (and tokio
/// keeps that thread alive for the rest of the runtime). Put a deadline
/// *inside* the closure when the blocking body itself waits on an external
/// dependency — [`run_command_blocking`], [`DeadlineGuard::run`], or the
/// dependency's own timeout. This helper alone only protects the caller.
pub async fn run_blocking<T, F>(deadline: Duration, what: &str, work: F) -> Result<T, BlockingError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let task = tokio::task::spawn_blocking(work);
    match tokio::time::timeout(deadline, task).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(BlockingError::Aborted {
            what: what.to_owned(),
            reason: error.to_string(),
        }),
        Err(_) => Err(BlockingError::TimedOut(DeadlineExceeded::new(
            what, deadline,
        ))),
    }
}

// ---------------------------------------------------------------------------
// Fail-fast guard for blocking services
// ---------------------------------------------------------------------------

/// Error from [`DeadlineGuard::run`].
#[derive(Debug)]
pub enum GuardError<E> {
    /// The guarded operation ran and failed.
    Operation(E),
    /// The operation did not answer within the deadline, or the guard already
    /// tripped on an earlier call. Carries the caller-supplied text.
    Unresponsive(String),
    /// The worker thread panicked before answering. Does not trip the guard:
    /// a panic says nothing about the dependency's responsiveness.
    Panicked {
        /// The worker thread's name.
        thread_name: String,
    },
    /// The worker thread could not be spawned.
    Spawn {
        /// The worker thread's name.
        thread_name: String,
        /// The spawn failure.
        source: io::Error,
    },
}

impl<E: fmt::Display> fmt::Display for GuardError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operation(error) => write!(f, "{error}"),
            Self::Unresponsive(text) => f.write_str(text),
            Self::Panicked { thread_name } => {
                write!(f, "{thread_name} worker panicked before answering")
            }
            Self::Spawn {
                thread_name,
                source,
            } => write!(
                f,
                "could not spawn the {thread_name} worker thread: {source}"
            ),
        }
    }
}

impl<E: fmt::Debug + fmt::Display + std::error::Error + 'static> std::error::Error
    for GuardError<E>
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Operation(error) => Some(error),
            Self::Spawn { source, .. } => Some(source),
            Self::Unresponsive(_) | Self::Panicked { .. } => None,
        }
    }
}

/// Runs blocking operations on a dedicated thread with a hard deadline, and
/// fails fast for the rest of the process after the first expiry.
///
/// Some service APIs block in OS IPC with no timeout of their own — the macOS
/// keychain behind an unlock dialog, a Secret Service prompt, a stalled
/// Windows Credential Manager RPC — and nothing answers a prompt on an
/// unattended host. Every operation is therefore moved to a fresh thread while
/// the caller waits at most `timeout`.
///
/// On expiry the worker thread is detached and leaks until the backend call
/// returns; joining it would reinstate the hang this guard exists to prevent.
/// The first expiry marks the guard unresponsive, so later calls return
/// [`GuardError::Unresponsive`] immediately — carrying the caller-supplied
/// text — instead of stacking another blocked thread behind the same prompt.
/// A panic inside the operation does **not** trip the guard, because it says
/// nothing about the dependency.
///
/// The guard is cheap to clone: clones share the sticky unresponsive flag, so
/// every handle in the process observes the same circuit state.
#[derive(Clone)]
pub struct DeadlineGuard {
    timeout: Duration,
    thread_name: Arc<str>,
    unresponsive: Arc<AtomicBool>,
    unresponsive_text: Arc<str>,
}

impl DeadlineGuard {
    /// Create a guard.
    ///
    /// `thread_name` names the worker threads (visible in thread dumps), and
    /// `unresponsive_text` is returned verbatim after an expiry or a fail-fast
    /// call. Pass text that names the dependency, the deadline, and the
    /// recovery step the operator should take.
    pub fn new(
        timeout: Duration,
        thread_name: impl Into<String>,
        unresponsive_text: impl Into<String>,
    ) -> Self {
        Self {
            timeout,
            thread_name: Arc::from(thread_name.into()),
            unresponsive: Arc::new(AtomicBool::new(false)),
            unresponsive_text: Arc::from(unresponsive_text.into()),
        }
    }

    /// The per-operation deadline.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Whether an earlier operation tripped the guard.
    pub fn is_unresponsive(&self) -> bool {
        self.unresponsive.load(Ordering::SeqCst)
    }

    /// Run `operation` on a worker thread, waiting at most [`timeout`].
    ///
    /// [`timeout`]: DeadlineGuard::timeout
    ///
    /// Errors from the operation itself come back as
    /// [`GuardError::Operation`], preserving the caller's error type. The
    /// worker thread is detached on expiry; see the type documentation.
    pub fn run<T, E>(
        &self,
        operation: impl FnOnce() -> Result<T, E> + Send + 'static,
    ) -> Result<T, GuardError<E>>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        if self.is_unresponsive() {
            return Err(GuardError::Unresponsive(self.unresponsive_text.to_string()));
        }
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::Builder::new()
            .name(self.thread_name.to_string())
            .spawn(move || {
                let _ = sender.send(operation());
            });
        let _worker = match worker {
            Ok(worker) => worker,
            Err(source) => {
                return Err(GuardError::Spawn {
                    thread_name: self.thread_name.to_string(),
                    source,
                });
            }
        };
        match receiver.recv_timeout(self.timeout) {
            Ok(result) => result.map_err(GuardError::Operation),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                self.unresponsive.store(true, Ordering::SeqCst);
                Err(GuardError::Unresponsive(self.unresponsive_text.to_string()))
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(GuardError::Panicked {
                thread_name: self.thread_name.to_string(),
            }),
        }
    }
}

impl fmt::Debug for DeadlineGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeadlineGuard")
            .field("timeout", &self.timeout)
            .field("thread_name", &self.thread_name)
            .field("unresponsive", &self.is_unresponsive())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn async_command_is_killed_at_the_deadline() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "sleep 30"]);
        let started = Instant::now();
        let error = run_command(cmd, Duration::from_millis(250), "test sleep")
            .await
            .unwrap_err();
        let elapsed = started.elapsed();
        assert!(error.timed_out(), "expected a timeout: {error}");
        assert!(
            elapsed >= Duration::from_millis(250),
            "returned before the deadline: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(10),
            "the deadline did not bound the wait: {elapsed:?}"
        );
        let text = error.to_string();
        assert!(text.contains("test sleep"), "missing what: {text}");
        assert!(text.contains("250ms"), "missing deadline: {text}");
        assert!(text.contains("sh"), "missing program: {text}");
    }

    #[tokio::test]
    async fn async_command_captures_both_streams() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "printf stdout; printf stderr >&2; exit 3"]);
        let output = run_command(cmd, Duration::from_secs(10), "stream capture")
            .await
            .unwrap();
        assert_eq!(output.status.code(), Some(3));
        assert_eq!(output.stdout, b"stdout");
        assert_eq!(output.stderr, b"stderr");
    }

    /// A child that writes far more than a pipe buffer must not be mistaken
    /// for a hang: both pipes are drained while the waiter waits.
    #[tokio::test]
    async fn async_command_drains_a_chatty_child() {
        let mut cmd = Command::new("sh");
        cmd.args([
            "-c",
            "head -c 524288 /dev/zero; head -c 524288 /dev/zero >&2",
        ]);
        let output = run_command(cmd, Duration::from_secs(10), "chatty child")
            .await
            .unwrap();
        assert_eq!(output.stdout.len(), 524_288);
        assert_eq!(output.stderr.len(), 524_288);
    }

    /// A capped capture discards the excess but keeps reading, so the child
    /// reaches EOF and exits instead of blocking on a full pipe.
    #[tokio::test]
    async fn capped_command_discards_the_excess_and_reports_truncation() {
        let mut cmd = Command::new("sh");
        cmd.args([
            "-c",
            "head -c 524288 /dev/zero; head -c 524288 /dev/zero >&2",
        ]);
        let (output, truncated) =
            run_command_capped(cmd, Duration::from_secs(10), "capped child", 1024)
                .await
                .unwrap();
        assert!(output.status.success(), "the child must reach EOF");
        assert!(truncated);
        assert_eq!(output.stdout.len(), 1024);
        assert_eq!(output.stderr.len(), 1024);
    }

    #[tokio::test]
    async fn capped_command_keeps_small_output_and_clears_the_flag() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "printf small"]);
        let (output, truncated) =
            run_command_capped(cmd, Duration::from_secs(10), "small child", 1024)
                .await
                .unwrap();
        assert!(!truncated);
        assert_eq!(output.stdout, b"small");
    }

    #[tokio::test]
    async fn command_with_stdin_writes_input_and_captures_output() {
        let mut cmd = Command::new("cat");
        let output = run_command_with_stdin(
            std::mem::replace(&mut cmd, Command::new("cat")),
            b"hello stdin",
            Duration::from_secs(10),
            "cat with stdin",
        )
        .await
        .unwrap();
        assert_eq!(output.stdout, b"hello stdin");
    }

    #[tokio::test]
    async fn command_with_stdin_is_bounded_when_the_child_never_reads() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "sleep 30"]);
        let started = Instant::now();
        let error = run_command_with_stdin(
            cmd,
            &[b'x'; 4 * 1024 * 1024],
            Duration::from_millis(250),
            "stdin sleep",
        )
        .await
        .unwrap_err();
        assert!(error.timed_out(), "expected a timeout: {error}");
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[tokio::test]
    async fn status_command_is_bounded_and_returns_the_exit_code() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "exit 7"]);
        let status = run_command_status(cmd, Duration::from_secs(10), "status check")
            .await
            .unwrap();
        assert_eq!(status.code(), Some(7));

        let mut cmd = Command::new("sh");
        cmd.args(["-c", "sleep 30"]);
        let started = Instant::now();
        let error = run_command_status(cmd, Duration::from_millis(250), "status sleep")
            .await
            .unwrap_err();
        assert!(error.timed_out(), "expected a timeout: {error}");
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    /// A pid file for one group test: unique per test so the two of them can
    /// run in parallel.
    fn pid_file(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "preloop-deadline-{name}-{}.pid",
            std::process::id()
        ))
    }

    /// The pid the script recorded, once it has written it.
    async fn recorded_pid(path: &std::path::Path) -> String {
        for _ in 0..200 {
            if let Ok(text) = std::fs::read_to_string(path) {
                let pid = text.trim().to_owned();
                if !pid.is_empty() {
                    return pid;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("no pid was recorded at {}", path.display());
    }

    /// Whether `pid` still exists. The null signal answers it without
    /// delivering anything; a zombie is reaped by init, so it does not hold
    /// this open.
    fn process_alive(pid: &str) -> bool {
        std::process::Command::new("kill")
            .args(["-0", pid])
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }

    async fn assert_gone(pid: &str, what: &str) {
        for _ in 0..100 {
            if !process_alive(pid) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("{what} (pid {pid}) outlived its process group");
    }

    /// A shell wrapper's real work is a grandchild, and killing only the shell
    /// leaves it running: the group runner must take the grandchild with it.
    #[tokio::test]
    async fn group_runner_kills_a_hung_grandchild_at_the_deadline() {
        let path = pid_file("group-deadline");
        let mut cmd = Command::new("sh");
        cmd.args([
            "-c",
            &format!("sleep 600 & echo $! > '{}'; wait", path.display()),
        ]);
        let started = Instant::now();
        let error = run_command_status_in_group(cmd, Duration::from_millis(300), "group sleep")
            .await
            .unwrap_err();
        assert!(error.timed_out(), "expected a timeout: {error}");
        assert!(started.elapsed() < Duration::from_secs(10));
        let pid = recorded_pid(&path).await;
        assert_gone(&pid, "the grandchild of a timed-out group runner").await;
    }

    /// A caller that stops waiting drops the runner's future — a `select!`
    /// losing to a shutdown token. That is a cancellation like any other, and
    /// the group has to go with it.
    #[tokio::test]
    async fn group_runner_takes_the_group_with_a_dropped_future() {
        let path = pid_file("group-dropped");
        let mut cmd = Command::new("sh");
        cmd.args([
            "-c",
            &format!("sleep 600 & echo $! > '{}'; wait", path.display()),
        ]);
        let dropped = tokio::time::timeout(
            Duration::from_millis(300),
            run_command_status_in_group(cmd, Duration::from_secs(30), "dropped group runner"),
        )
        .await;
        assert!(
            dropped.is_err(),
            "the outer timeout must have dropped the runner"
        );
        let pid = recorded_pid(&path).await;
        assert_gone(&pid, "the grandchild of a dropped group runner").await;
    }

    #[test]
    fn blocking_command_is_killed_at_the_deadline() {
        let mut cmd = std::process::Command::new("sh");
        cmd.args(["-c", "sleep 30"]);
        let started = Instant::now();
        let error =
            run_command_blocking(cmd, Duration::from_millis(250), "sync sleep").unwrap_err();
        let elapsed = started.elapsed();
        assert!(error.timed_out(), "expected a timeout: {error}");
        assert!(
            elapsed >= Duration::from_millis(250),
            "returned before the deadline: {elapsed:?}"
        );
        assert!(elapsed < Duration::from_secs(10));
        let text = error.to_string();
        assert!(text.contains("sync sleep"), "missing what: {text}");
        assert!(text.contains("250ms"), "missing deadline: {text}");
    }

    #[test]
    fn blocking_command_drains_a_chatty_child() {
        let mut cmd = std::process::Command::new("sh");
        cmd.args([
            "-c",
            "head -c 524288 /dev/zero; head -c 524288 /dev/zero >&2; exit 4",
        ]);
        let output =
            run_command_blocking(cmd, Duration::from_secs(10), "sync chatty child").unwrap();
        assert_eq!(output.status.code(), Some(4));
        assert_eq!(output.stdout.len(), 524_288);
        assert_eq!(output.stderr.len(), 524_288);
    }

    #[test]
    fn blocking_capped_command_discards_the_excess() {
        let mut cmd = std::process::Command::new("sh");
        cmd.args([
            "-c",
            "head -c 524288 /dev/zero; head -c 524288 /dev/zero >&2",
        ]);
        let (output, truncated) =
            run_command_blocking_capped(cmd, Duration::from_secs(10), "sync capped", 1024).unwrap();
        assert!(output.status.success(), "the child must reach EOF");
        assert!(truncated);
        assert_eq!(output.stdout.len(), 1024);
        assert_eq!(output.stderr.len(), 1024);
    }

    #[tokio::test]
    async fn run_blocking_returns_at_the_deadline() {
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let started = Instant::now();
        let error = run_blocking(Duration::from_millis(200), "blocking work", move || {
            // Held until the test releases it, so the leaked thread does not
            // keep the test runtime alive after the assertion.
            let _ = release_rx.recv();
        })
        .await
        .unwrap_err();
        let elapsed = started.elapsed();
        assert!(error.timed_out(), "expected a timeout: {error}");
        assert!(
            elapsed >= Duration::from_millis(200),
            "returned before the deadline: {elapsed:?}"
        );
        assert!(elapsed < Duration::from_secs(5));
        let text = error.to_string();
        assert!(text.contains("blocking work"), "missing what: {text}");
        assert!(text.contains("200ms"), "missing deadline: {text}");
        let _ = release_tx.send(());
    }

    #[test]
    fn run_blocking_reports_a_panicking_closure() {
        let error = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run_blocking(
                Duration::from_secs(5),
                "panicking work",
                || panic!("boom"),
            ))
            .unwrap_err();
        assert!(!error.timed_out());
        assert!(error.to_string().contains("panicking work"));
    }

    /// A wedged service must cost exactly one deadline: the first call fails
    /// at the bound, and the sticky flag makes every later call fail
    /// immediately without touching the backend again.
    #[test]
    fn guard_trips_once_then_fails_fast() {
        let timeout = Duration::from_millis(500);
        let guard = DeadlineGuard::new(
            timeout,
            "test-guard",
            "the test service did not answer within 500ms and is probably hung; \
             later calls fail immediately",
        );
        let attempts = Arc::new(AtomicUsize::new(0));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        // Held for the whole test: the worker blocks like a keychain call
        // behind an unlock prompt, and only the guard's deadline ends the wait.
        let (_release_tx, release_rx) = std::sync::mpsc::channel::<()>();

        let first_attempts = attempts.clone();
        let started = Instant::now();
        let error = guard
            .run(move || -> Result<(), io::Error> {
                first_attempts.fetch_add(1, Ordering::SeqCst);
                entered_tx.send(()).unwrap();
                let _ = release_rx.recv();
                Ok(())
            })
            .unwrap_err();
        entered_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the call must reach the backend");
        let elapsed = started.elapsed();
        assert!(
            elapsed >= timeout,
            "must wait out the deadline once: {elapsed:?}"
        );
        let text = error.to_string();
        assert!(
            text.contains("did not answer within"),
            "the timeout must explain itself: {text}"
        );

        let second_attempts = attempts.clone();
        let started = Instant::now();
        let error = guard
            .run(move || -> Result<(), io::Error> {
                second_attempts.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap_err();
        let elapsed = started.elapsed();
        assert!(elapsed < timeout, "later calls must fail fast: {elapsed:?}");
        assert!(error.to_string().contains("did not answer within"));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "a tripped guard must not reach the backend again"
        );
        assert!(guard.is_unresponsive());
    }

    /// A panic in the guarded operation is not evidence that the dependency is
    /// wedged, so the circuit stays closed and the next call runs.
    #[test]
    fn guard_stays_closed_after_an_operation_panic() {
        let guard = DeadlineGuard::new(Duration::from_secs(5), "test-guard", "unresponsive");
        let error = guard
            .run(|| -> Result<(), io::Error> { panic!("boom") })
            .unwrap_err();
        assert!(matches!(error, GuardError::Panicked { .. }));
        assert!(!guard.is_unresponsive());
        assert_eq!(guard.run(|| -> Result<u8, io::Error> { Ok(1) }).unwrap(), 1);
    }
}
