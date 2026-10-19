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
//!   draining, and reaping; it is not restarted per step. Spawn itself runs on
//!   a helper thread, because `fork`/`exec` blocks in the kernel — resolving
//!   the program on an unresponsive filesystem, a caller's `pre_exec` hook —
//!   and that wait must not outrank the deadline. A spawn that outruns the
//!   deadline cannot be cancelled, so its thread lives until the kernel
//!   returns and the child it eventually produces is killed rather than
//!   orphaned (the same trade-off [`run_blocking`] documents).
//! - Child output is drained concurrently with the wait. A child that fills a
//!   64 KiB pipe buffer blocks on `write` and looks exactly like a hang, so a
//!   naive wait would either time out spuriously or deadlock on the pipe.
//!   Capture is unbounded by default; the `_capped` variants keep at most a
//!   caller-supplied number of bytes per stream and discard the excess *while
//!   still reading to EOF*, so a cap bounds memory without reintroducing the
//!   deadlock.
//! - Expiry kills the child, then reaps it under a short grace period. The
//!   error names the caller's `what` and the deadline so an operator can tell
//!   which dependency hung.
//! - On Unix the child is placed in its own process group and expiry signals
//!   the whole group: a killed `git fetch` takes its `index-pack`, transport,
//!   and credential helpers with it instead of leaving them running against
//!   the repository after the caller's lock has been released. Other platforms
//!   signal the direct child only. The group is named by the id captured at
//!   spawn, so a helper that outlives its parent is still reached when the
//!   direct child has already been reaped.
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

/// The timeout error plus the journal line an operator sees when a hung
/// dependency is killed.
///
/// The message is the same text as the returned error — `what`, the deadline,
/// and the program — so a caller that wraps or discards the error still leaves
/// the reason in the engine's log. `process_group` records whether the kill
/// reached the child's own workers.
fn timeout_error(what: &str, program: &str, deadline: Duration) -> CommandError {
    let error = CommandError::timeout_error(what, program, deadline);
    tracing::warn!(
        what = %what,
        program = %program,
        deadline_ms = deadline.as_millis() as u64,
        process_group = cfg!(unix),
        "{error}",
    );
    error
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

/// Put the child in its own process group so that expiry can signal the whole
/// tree it may spawn, not just the direct child.
#[cfg(unix)]
fn isolate_process_group(cmd: &mut Command) {
    cmd.process_group(0);
}

#[cfg(not(unix))]
fn isolate_process_group(_cmd: &mut Command) {}

/// The synchronous twin of [`isolate_process_group`].
#[cfg(unix)]
fn isolate_process_group_blocking(cmd: &mut std::process::Command) {
    use std::os::unix::process::CommandExt as _;
    cmd.process_group(0);
}

#[cfg(not(unix))]
fn isolate_process_group_blocking(_cmd: &mut std::process::Command) {}

/// SIGKILL one process group.
///
/// `pgid` is the id of a child that was spawned through
/// [`isolate_process_group`], i.e. the child's pid and its group id.
/// `-pgid` therefore names exactly the group that child leads, and no other
/// process: the engine never joins a group led by one of its children.
///
/// The id is captured at spawn, because a reaped child has no id left to read
/// while its workers may still be holding the pipes. That leaves the standard
/// pid-reuse window: if the group has emptied and the id has been handed to a
/// new process that leads its own group, this signal reaches that stranger.
/// The window is accepted — the alternative is leaving the child's workers
/// running against the repository after the caller's deadline.
#[cfg(unix)]
#[allow(unsafe_code)]
fn kill_process_group(pgid: u32) {
    // SAFETY: `kill(2)` takes two integers and reads no memory this process
    // owns. The target is a process group created for the child by
    // `process_group(0)`, so signalling it cannot reach the engine or its
    // siblings; `pgid` comes from a live `Child::id()`. SIGKILL is 9 on every
    // Unix this engine targets, and the literal keeps the module free of a
    // libc dependency.
    const SIGKILL: i32 = 9;
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    let _ = unsafe { kill(-(pgid as i32), SIGKILL) };
}

#[cfg(not(unix))]
fn kill_process_group(_pgid: u32) {}

/// Kill a child whose caller has already given up on it: its group first, so
/// the workers it spawned do not outlive the deadline either.
fn abandon_spawned_child(mut child: Child) {
    if let Some(pid) = child.id() {
        kill_process_group(pid);
    }
    let _ = child.start_kill();
    // `kill_on_drop` is set by every public runner, so the drop below reaps it
    // instead of leaving a zombie.
}

/// Kill and reap a child the synchronous runner has given up on.
fn abandon_spawned_child_blocking(mut child: std::process::Child) {
    kill_process_group(child.id());
    let _ = child.kill();
    let _ = child.wait();
}

/// Spawn `cmd` on a blocking thread, so a `fork`/`exec` that blocks in the
/// kernel — resolving the program on an unresponsive filesystem, a caller's
/// `pre_exec` hook — cannot hold the caller past `deadline`.
///
/// The kernel call itself cannot be cancelled, so a spawn that outruns the
/// deadline keeps its thread (exactly like an expired [`run_blocking`] closure)
/// and the child it eventually produces is killed, group and all, instead of
/// being orphaned behind a caller that stopped waiting.
async fn spawn_bounded(
    mut cmd: Command,
    deadline: Duration,
    started: Instant,
    what: &str,
    program: &str,
) -> Result<Child, CommandError> {
    let remaining = deadline.saturating_sub(started.elapsed());
    let (sender, receiver) = tokio::sync::oneshot::channel();
    // The handle is dropped immediately: a blocking task is never cancelled,
    // and its answer is read from the channel.
    drop(tokio::task::spawn_blocking(move || match cmd.spawn() {
        Ok(child) => {
            if let Err(Ok(child)) = sender.send(Ok(child)) {
                abandon_spawned_child(child);
            }
        }
        Err(source) => {
            let _ = sender.send(Err(source));
        }
    }));
    match tokio::time::timeout(remaining, receiver).await {
        Ok(Ok(Ok(child))) => Ok(child),
        Ok(Ok(Err(source))) => Err(CommandError::spawn(what, program.to_owned(), source)),
        Ok(Err(_cancelled)) => Err(CommandError::io(
            what,
            program,
            io::Error::other("the spawn task was cancelled"),
        )),
        Err(_) => Err(timeout_error(what, program, deadline)),
    }
}

/// What a bounded synchronous spawn produced.
enum SyncSpawn {
    /// The process started inside the deadline.
    Child(std::process::Child),
    /// The process itself could not be started.
    Failed(io::Error),
    /// The helper thread that performs the spawn could not be started.
    Unavailable(io::Error),
    /// The deadline expired while the kernel was still starting the process.
    TimedOut,
}

/// The synchronous twin of [`spawn_bounded`].
///
/// A blocking caller has no executor to park on, so the spawn moves to a
/// short-lived helper thread and the caller waits on a channel. That costs one
/// thread per call; the alternative is a `fork`/`exec` wedged in the kernel
/// outranking the deadline entirely. A child that arrives after the caller
/// gave up is killed, group and all, on the helper thread.
fn spawn_bounded_blocking(
    mut cmd: std::process::Command,
    deadline: Duration,
    started: Instant,
) -> SyncSpawn {
    let (sender, receiver) = std::sync::mpsc::channel();
    let helper = std::thread::Builder::new()
        .name("deadline-spawn".to_owned())
        .spawn(move || match cmd.spawn() {
            Ok(child) => {
                if let Err(returned) = sender.send(Ok(child))
                    && let Ok(child) = returned.0
                {
                    abandon_spawned_child_blocking(child);
                }
            }
            Err(source) => {
                let _ = sender.send(Err(source));
            }
        });
    if let Err(source) = helper {
        return SyncSpawn::Unavailable(source);
    }
    match receiver.recv_timeout(deadline.saturating_sub(started.elapsed())) {
        Ok(Ok(child)) => SyncSpawn::Child(child),
        Ok(Err(source)) => SyncSpawn::Failed(source),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => SyncSpawn::TimedOut,
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => SyncSpawn::Unavailable(
            io::Error::other("the spawn thread exited without reporting"),
        ),
    }
}

/// Kill the child and reap it, but never block the caller past the grace
/// period. A grandchild holding the pipe open does not extend the wait.
///
/// `pgid` is the id captured when the child was spawned: `Child::id()` is
/// `None` once the direct child has been reaped, and a worker that outlived it
/// still has to die, so the group is named by the value from spawn rather than
/// by a lookup that may already have gone stale.
async fn kill_and_reap(child: &mut Child, pgid: Option<u32>) {
    // The group signal takes the child's own workers down with it; the direct
    // kill below keeps non-Unix platforms and a racing exit covered.
    if let Some(pgid) = pgid {
        kill_process_group(pgid);
    }
    // `kill_on_drop` is set by every public runner, but an explicit kill
    // returns the error to the caller instead of relying on drop timing.
    let _ = child.start_kill();
    let _ = tokio::time::timeout(KILL_REAP_GRACE, child.wait()).await;
}

async fn wait_with_capture(
    mut child: Child,
    pgid: Option<u32>,
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
        // The child finished before the deadline: the ordinary result.
        Ok(Ok(finished)) if started.elapsed() < deadline => Ok(finished),
        // The child finished *after* the deadline. The deadline covers the
        // whole operation, and the synchronous runner fails an operation that
        // outran it, so this expiry is reported the same way instead of
        // pretending the bound held. Nothing is left to kill: the capture
        // reached EOF on both pipes.
        Ok(Ok(_)) => Err(timeout_error(what, program, deadline)),
        Ok(Err(error)) => Err(error),
        Err(_) => {
            kill_and_reap(&mut child, pgid).await;
            Err(timeout_error(what, program, deadline))
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
/// `deadline` covers spawn, output capture, and waiting; the spawn runs on a
/// blocking thread so a `fork`/`exec` blocked in the kernel cannot outrank the
/// deadline. On expiry the child is killed and reaped within a short grace
/// period, and the returned [`CommandError`] names `what`, the program, and the
/// deadline. Captured output is returned even for a non-zero exit status;
/// success is the caller's decision.
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
    isolate_process_group(&mut cmd);
    let child = spawn_bounded(cmd, deadline, started, what, &program).await?;
    let pgid = child.id();
    wait_with_capture(child, pgid, deadline, started, what, &program, max_capture).await
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
    isolate_process_group(&mut cmd);
    let mut child = spawn_bounded(cmd, deadline, started, what, &program).await?;
    let pgid = child.id();

    let remaining = deadline.saturating_sub(started.elapsed());
    let mut stdin = child.stdin.take();
    let exchange = async {
        let write = async {
            match stdin.take() {
                Some(mut pipe) => {
                    let result = async {
                        pipe.write_all(input).await?;
                        pipe.shutdown().await
                    }
                    .await;
                    match result {
                        // The child closed its stdin early — `head -c N`, a
                        // `git` subcommand that rejects its arguments — so
                        // there is no reader left for the rest of `input`.
                        // That is a normal end of input, not a failure: the
                        // exit status and captured output are still the
                        // caller's result.
                        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => Ok(()),
                        other => other,
                    }
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
        Ok(Ok(output)) if started.elapsed() < deadline => Ok(output),
        // Finished after the deadline; see `wait_with_capture`.
        Ok(Ok(_)) => Err(timeout_error(what, &program, deadline)),
        Ok(Err(error)) => Err(error),
        Err(_) => {
            kill_and_reap(&mut child, pgid).await;
            Err(timeout_error(what, &program, deadline))
        }
    }
}

/// Run a command and return only its exit status, bounded by `deadline`.
///
/// Unlike [`run_command`], output is not returned: streams the caller left
/// inherited reach the engine's own `stdout`/`stderr`, and streams the caller
/// piped are drained and discarded, so a chatty child cannot stall on a full
/// pipe. This is the variant for long-running installers whose output should
/// reach the engine's logs.
pub async fn run_command_status(
    mut cmd: Command,
    deadline: Duration,
    what: &str,
) -> Result<ExitStatus, CommandError> {
    let started = Instant::now();
    let program = program_name(&cmd);
    cmd.kill_on_drop(true);
    cmd.stdin(Stdio::null());
    isolate_process_group(&mut cmd);
    let mut child = spawn_bounded(cmd, deadline, started, what, &program).await?;
    let pgid = child.id();
    let remaining = deadline.saturating_sub(started.elapsed());
    let exchange = async {
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        // A piped stream is drained and thrown away (the signature returns no
        // output) — leaving it undrained would stall a healthy but chatty
        // child until the deadline. Inherited streams are `None` here and stay
        // the caller's.
        let (status, _stdout, _stderr) =
            tokio::join!(child.wait(), read_pipe(stdout, 0), read_pipe(stderr, 0),);
        status
    };
    match tokio::time::timeout(remaining, exchange).await {
        Ok(Ok(status)) if started.elapsed() < deadline => Ok(status),
        // Finished after the deadline; see `wait_with_capture`.
        Ok(Ok(_)) => Err(timeout_error(what, &program, deadline)),
        Ok(Err(source)) => Err(CommandError::io(what, &program, source)),
        Err(_) => {
            kill_and_reap(&mut child, pgid).await;
            Err(timeout_error(what, &program, deadline))
        }
    }
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
    kill_process_group(child.id());
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
/// rather than waiting on that descendant forever, and it signals the child's
/// process group on the way out so the descendant stops holding them. Reader
/// threads are detached on expiry and exit at EOF once those pipes close.
///
/// Starting the process is bounded too: the spawn runs on a short-lived helper
/// thread, so a `fork`/`exec` wedged in the kernel cannot hold the caller past
/// the deadline.
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
    isolate_process_group_blocking(&mut cmd);
    let mut child = match spawn_bounded_blocking(cmd, deadline, started) {
        SyncSpawn::Child(child) => child,
        SyncSpawn::Failed(source) => {
            return Err(CommandError::spawn(what, program.clone(), source));
        }
        SyncSpawn::Unavailable(source) => {
            return Err(CommandError::io(what, &program, source));
        }
        // The kernel was still starting the process when the deadline passed.
        SyncSpawn::TimedOut => return Err(timeout_error(what, &program, deadline)),
    };

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
            return Err(timeout_error(what, &program, deadline));
        }
        std::thread::sleep(POLL_INTERVAL.min(deadline - elapsed));
    };

    // Collect both captures. The reader threads finish as soon as the child's
    // pipe ends close, which the observed exit guarantees for well-behaved
    // children. A descendant holding a pipe open cannot extend this past the
    // overall deadline.
    let child_pid = child.id();
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
                    // The direct child is gone but something still holds its
                    // pipes: signal the group so the descendant stops holding
                    // them (and the capture threads reach EOF) instead of
                    // reporting a kill that never happened.
                    kill_process_group(child_pid);
                    return Err(timeout_error(what, &program, deadline));
                }
                match receiver.recv_timeout(remaining) {
                    Ok((PipeKind::Stdout, result)) => stdout = Some(result),
                    Ok((PipeKind::Stderr, result)) => stderr = Some(result),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        kill_process_group(child_pid);
                        return Err(timeout_error(what, &program, deadline));
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

    /// Whether `pid` no longer runs. A zombie counts as gone: it holds no
    /// memory and the OS reaps it once its parent does.
    #[cfg(unix)]
    fn process_is_gone(pid: u32) -> bool {
        let output = std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output();
        match output {
            Ok(output) if output.status.success() => {
                let stat = String::from_utf8_lossy(&output.stdout);
                let stat = stat.trim();
                stat.is_empty() || stat.starts_with('Z')
            }
            // `ps` reports nothing for a pid that no longer exists.
            _ => true,
        }
    }

    #[cfg(unix)]
    fn wait_for_pid_exit(pid: u32) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !process_is_gone(pid) {
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        true
    }

    #[cfg(unix)]
    fn read_pid(path: &std::path::Path) -> u32 {
        std::fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("reading {}: {error}", path.display()))
            .trim()
            .parse()
            .unwrap_or_else(|error| panic!("parsing {}: {error}", path.display()))
    }

    /// A script that records the child's and a background worker's pids, then
    /// waits on the worker like `git fetch` waits on its transport helpers.
    #[cfg(unix)]
    fn pidfile_script(child: &std::path::Path, grandchild: &std::path::Path) -> String {
        format!(
            "echo $$ > {}; sleep 300 & echo $! > {}; wait",
            child.display(),
            grandchild.display()
        )
    }

    /// A script that records a background worker's pid and exits at once, so
    /// the worker alone holds the inherited pipes — the shape that used to
    /// leave the worker running (and the capture threads blocked) after expiry.
    #[cfg(unix)]
    fn orphan_script(grandchild: &std::path::Path) -> String {
        format!("sleep 300 & echo $! > {}; exit 0", grandchild.display())
    }

    #[cfg(unix)]
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

    /// Expiry must take the child's own workers with it: a killed `git fetch`
    /// leaves `index-pack`/transport helpers behind otherwise, and they keep
    /// writing into the repository after the caller has released its lock.
    #[cfg(unix)]
    #[tokio::test]
    async fn async_expiry_kills_the_childs_process_group() {
        let dir = std::env::temp_dir();
        let stamp = format!("{}-{}", std::process::id(), line!());
        let child_pid = dir.join(format!("deadline-child-{stamp}"));
        let grandchild_pid = dir.join(format!("deadline-grandchild-{stamp}"));
        let mut cmd = Command::new("sh");
        cmd.args(["-c", &pidfile_script(&child_pid, &grandchild_pid)]);
        let error = run_command(cmd, Duration::from_millis(400), "group kill")
            .await
            .unwrap_err();
        assert!(error.timed_out(), "expected a timeout: {error}");
        let child = read_pid(&child_pid);
        let grandchild = read_pid(&grandchild_pid);
        assert!(
            wait_for_pid_exit(child),
            "the direct child {child} survived expiry"
        );
        assert!(
            wait_for_pid_exit(grandchild),
            "the grandchild {grandchild} survived expiry"
        );
        let _ = std::fs::remove_file(&child_pid);
        let _ = std::fs::remove_file(&grandchild_pid);
    }

    /// The direct child can exit first and leave its worker holding both
    /// pipes. Expiry must still reach the worker — through the group id
    /// captured at spawn, since the reaped child has no id left — or the
    /// worker keeps running and the capture threads stay blocked.
    #[cfg(unix)]
    #[tokio::test]
    async fn async_expiry_kills_a_descendant_that_outlives_the_child() {
        let dir = std::env::temp_dir();
        let stamp = format!("{}-{}", std::process::id(), line!());
        let grandchild_pid = dir.join(format!("deadline-orphan-{stamp}"));
        let mut cmd = Command::new("sh");
        cmd.args(["-c", &orphan_script(&grandchild_pid)]);
        let error = run_command(cmd, Duration::from_millis(400), "orphaned worker")
            .await
            .unwrap_err();
        assert!(error.timed_out(), "expected a timeout: {error}");
        let grandchild = read_pid(&grandchild_pid);
        assert!(
            wait_for_pid_exit(grandchild),
            "the descendant {grandchild} survived expiry"
        );
        let _ = std::fs::remove_file(&grandchild_pid);
    }

    /// A spawn that blocks in the kernel must still return at the deadline:
    /// the synchronous `spawn` call itself is what `tokio::time::timeout`
    /// cannot interrupt, so it happens off the caller's thread.
    #[cfg(unix)]
    #[allow(unsafe_code)]
    #[tokio::test]
    async fn async_command_with_a_blocking_spawn_returns_at_the_deadline() {
        use std::os::unix::process::CommandExt as _;
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "exit 0"]);
        // `pre_exec` runs in the forked child before `exec`, and the parent's
        // `spawn` waits for that exec to finish. Sleeping here models a hook
        // wedged in the kernel.
        unsafe {
            cmd.as_std_mut().pre_exec(|| {
                std::thread::sleep(Duration::from_secs(2));
                Ok(())
            });
        }
        let started = Instant::now();
        let error = run_command(cmd, Duration::from_millis(200), "blocked spawn")
            .await
            .unwrap_err();
        let elapsed = started.elapsed();
        assert!(error.timed_out(), "expected a timeout: {error}");
        assert!(
            elapsed < Duration::from_secs(1),
            "the blocked spawn held the caller past the deadline: {elapsed:?}"
        );
    }

    #[cfg(unix)]
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
    #[cfg(unix)]
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
    #[cfg(unix)]
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

    #[cfg(unix)]
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

    #[cfg(unix)]
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

    #[cfg(unix)]
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

    /// A child that stops reading and exits must not turn the call into an
    /// I/O error: `head -c 5` closes stdin after five bytes, and its status
    /// and captured output are still the caller's result.
    #[cfg(unix)]
    #[tokio::test]
    async fn command_with_stdin_tolerates_a_child_that_stops_reading() {
        let mut cmd = Command::new("head");
        cmd.args(["-c", "5"]);
        let output = run_command_with_stdin(
            cmd,
            &[b'x'; 1024 * 1024],
            Duration::from_secs(10),
            "early stdin close",
        )
        .await
        .unwrap();
        assert_eq!(output.status.code(), Some(0));
        assert_eq!(output.stdout, b"xxxxx");
    }

    #[cfg(unix)]
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

    #[cfg(unix)]
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

    #[cfg(unix)]
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

    #[cfg(unix)]
    #[test]
    fn blocking_expiry_kills_the_childs_process_group() {
        let dir = std::env::temp_dir();
        let stamp = format!("{}-{}", std::process::id(), line!());
        let child_pid = dir.join(format!("deadline-sync-child-{stamp}"));
        let grandchild_pid = dir.join(format!("deadline-sync-grandchild-{stamp}"));
        let mut cmd = std::process::Command::new("sh");
        cmd.args(["-c", &pidfile_script(&child_pid, &grandchild_pid)]);
        let error =
            run_command_blocking(cmd, Duration::from_millis(400), "sync group kill").unwrap_err();
        assert!(error.timed_out(), "expected a timeout: {error}");
        let child = read_pid(&child_pid);
        let grandchild = read_pid(&grandchild_pid);
        assert!(
            wait_for_pid_exit(child),
            "the direct child {child} survived expiry"
        );
        assert!(
            wait_for_pid_exit(grandchild),
            "the grandchild {grandchild} survived expiry"
        );
        let _ = std::fs::remove_file(&child_pid);
        let _ = std::fs::remove_file(&grandchild_pid);
    }

    /// The synchronous twin of `async_expiry_kills_a_descendant_that_outlives_
    /// the_child`: the capture has to signal the group it named at spawn before
    /// it reports the timeout, or the worker keeps both pipes and both reader
    /// threads stay blocked on a call that has already returned.
    #[cfg(unix)]
    #[test]
    fn blocking_expiry_kills_a_descendant_that_outlives_the_child() {
        let dir = std::env::temp_dir();
        let stamp = format!("{}-{}", std::process::id(), line!());
        let grandchild_pid = dir.join(format!("deadline-sync-orphan-{stamp}"));
        let mut cmd = std::process::Command::new("sh");
        cmd.args(["-c", &orphan_script(&grandchild_pid)]);
        let error =
            run_command_blocking(cmd, Duration::from_millis(400), "sync orphan").unwrap_err();
        assert!(error.timed_out(), "expected a timeout: {error}");
        let grandchild = read_pid(&grandchild_pid);
        assert!(
            wait_for_pid_exit(grandchild),
            "the descendant {grandchild} survived expiry"
        );
        let _ = std::fs::remove_file(&grandchild_pid);
    }

    #[cfg(unix)]
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

    /// A blocked `fork`/`exec` must not hold a synchronous caller either: the
    /// spawn happens on a helper thread, so the deadline still applies.
    #[cfg(unix)]
    #[allow(unsafe_code)]
    #[test]
    fn blocking_command_with_a_blocking_spawn_returns_at_the_deadline() {
        use std::os::unix::process::CommandExt as _;
        let mut cmd = std::process::Command::new("sh");
        cmd.args(["-c", "exit 0"]);
        unsafe {
            cmd.pre_exec(|| {
                std::thread::sleep(Duration::from_secs(2));
                Ok(())
            });
        }
        let started = Instant::now();
        let error = run_command_blocking(cmd, Duration::from_millis(200), "sync blocked spawn")
            .unwrap_err();
        let elapsed = started.elapsed();
        assert!(error.timed_out(), "expected a timeout: {error}");
        assert!(
            elapsed < Duration::from_secs(1),
            "the blocked spawn held the caller past the deadline: {elapsed:?}"
        );
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
