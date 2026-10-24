//! Reusable Git mirror and checkout-cache primitives.
//!
//! This crate holds the parts of the control plane's Git handling that are not
//! tied to a single run: bare-mirror lifecycle and presence checks, the
//! directory mutex that serializes writers per repository, bounded `git`
//! invocations, commit/tree inspection used for input fingerprinting, the
//! Git LFS content-addressed object store, and the Smart HTTP request parsing
//! needed to fetch a missing object before serving it.
//!
//! The control plane keeps its run-scoped snapshot orchestration; anything
//! reusable by another consumer (a sandbox, a CLI, a test harness) belongs
//! here instead, so it is not reachable only through an HTTP handler.

pub mod cache;
pub mod http_backend;
pub mod lfs;
pub mod lock;
pub mod trees;

/// Bound on any single Git child invoked through this crate.
///
/// A Git process that has not finished inside its deadline is treated as hung,
/// not slow: expiry kills the child, so a stalled upstream can never pin a
/// cache lock or an HTTP handler forever.
pub const GIT_CONTROL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Failure from a mirror, tree, LFS, or Git-process operation.
#[derive(Debug, thiserror::Error)]
pub enum MirrorError {
    /// A caller-supplied revision, path, object id, or repository slug was
    /// rejected before it reached a Git command.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// A revision or object the operation required is absent from the
    /// repository.
    #[error("missing object: {0}")]
    MissingObject(String),
    /// A Git child exited non-zero, could not be spawned, or exceeded its
    /// deadline.
    #[error("failed to {operation}: {message}")]
    Git {
        /// What the child was doing, for the operator reading the log.
        operation: String,
        /// Human-readable detail (stderr, spawn error, or timeout).
        message: String,
    },
    /// A decoded request body exceeded its configured limit.
    #[error("{0}")]
    TooLarge(String),
    /// A filesystem operation failed.
    #[error("io error: {0}")]
    Io(String),
}

impl MirrorError {
    /// Wrap a filesystem error with the operation that produced it.
    pub fn io(operation: &str, error: &std::io::Error) -> Self {
        Self::Io(format!("{operation}: {error}"))
    }
}

/// Whether `value` is a full Git object id the mirror accepts.
///
/// Both SHA-1 (40) and SHA-256 (64) object formats occur in repositories the
/// mirror caches, so the length bounds are inclusive and only hexadecimal
/// digits are accepted — anything else is rejected before it becomes an argv
/// entry.
pub fn is_full_object_id(value: &str) -> bool {
    (40..=64).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Whether `value` is a safe cache key: a bare object id.
///
/// Cache entry names are derived from repository identity digests, so a value
/// that is not hexadecimal could name a path outside the cache root.
pub fn validate_cache_key(value: &str) -> bool {
    is_full_object_id(value)
}

/// Arguments accepted before a `--` separator, so a revision can never be
/// mistaken for an option.
pub(crate) fn safe_revision(rev: &str) -> Result<&str, MirrorError> {
    let trimmed = rev.trim();
    if trimmed.is_empty() {
        return Err(MirrorError::InvalidInput("empty revision".to_owned()));
    }
    if trimmed.starts_with('-') {
        return Err(MirrorError::InvalidInput(format!(
            "revision `{trimmed}` looks like an option"
        )));
    }
    Ok(trimmed)
}

/// Build a `git` command with the mirror's non-interactive child policy.
///
/// `GIT_TERMINAL_PROMPT=0` plus dead askpass programs keep a credential-less
/// mirror from blocking on a terminal that cannot answer. The credential
/// helper list is reset on the command line, so an operator's global helper
/// never runs for a mirror fetch. A call site that must run a specific binary
/// (tests substitute a script) uses [`git_command_for`].
pub(crate) fn git_command() -> tokio::process::Command {
    git_command_for("git")
}

/// [`git_command`] for an explicit Git binary.
pub fn git_command_for(program: impl AsRef<std::ffi::OsStr>) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(program);
    command.arg("-c").arg("credential.helper=");
    command
        .kill_on_drop(true)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_ASKPASS", "false")
        .env("SSH_ASKPASS", "false")
        .env("GCM_INTERACTIVE", "never");
    command
}

/// Run a Git command under a deadline, returning its output whatever the exit
/// status was.
///
/// `command` must come from [`git_command`]; the policy is applied there so a
/// call site can still stage a credential afterwards.
pub(crate) async fn run_git_within(
    mut command: tokio::process::Command,
    operation: &str,
    deadline: std::time::Duration,
) -> Result<std::process::Output, MirrorError> {
    match tokio::time::timeout(deadline, command.output()).await {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(error)) => Err(MirrorError::Git {
            operation: operation.to_owned(),
            message: format!("could not run git: {error}"),
        }),
        Err(_) => Err(MirrorError::Git {
            operation: operation.to_owned(),
            message: format!("git did not finish within {}s", deadline.as_secs()),
        }),
    }
}

/// Run a Git command and require a zero exit status.
pub(crate) async fn run_git_checked(
    command: tokio::process::Command,
    operation: &str,
    deadline: std::time::Duration,
) -> Result<std::process::Output, MirrorError> {
    let output = run_git_within(command, operation, deadline).await?;
    if !output.status.success() {
        return Err(MirrorError::Git {
            operation: operation.to_owned(),
            message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    Ok(output)
}
