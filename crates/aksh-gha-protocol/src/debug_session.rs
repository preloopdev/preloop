//! Live debug-session DTOs.
//!
//! A job that fails a step with debugging enabled does not tear down. The
//! worker stays alive inside its microVM, registers a session with the control
//! plane, and blocks until a controller returns a verdict. Every actor — the
//! worker, `preloop debug`, and an agent — speaks these types.
//!
//! This is aksh's own surface (`/api/v1/debug/...`). It never crosses the
//! GitHub runner protocol, so `/_apis/...` stays byte-identical.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{JobId, RunId};

/// Where a session is in its lifecycle.
///
/// The worker only ever observes `Paused` (it is blocked) and the terminal
/// states. `Attached` and `Retrying` exist so a controller can render honest
/// status without polling the worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    /// Step failed; worker is blocked awaiting a verdict.
    Paused,
    /// A controller holds the lease.
    Attached,
    /// A verdict was issued and the worker is acting on it.
    Retrying,
    /// Worker resumed; the job is running again.
    Resumed,
    /// Session ended because the job was aborted.
    Aborted,
    /// Worker vanished without completing the session.
    Abandoned,
}

impl SessionState {
    /// Whether the session still holds the job open.
    pub fn is_open(self) -> bool {
        matches!(
            self,
            SessionState::Paused | SessionState::Attached | SessionState::Retrying
        )
    }

    /// Stable lowercase label for logs and CLI output.
    pub fn as_str(self) -> &'static str {
        match self {
            SessionState::Paused => "paused",
            SessionState::Attached => "attached",
            SessionState::Retrying => "retrying",
            SessionState::Resumed => "resumed",
            SessionState::Aborted => "aborted",
            SessionState::Abandoned => "abandoned",
        }
    }
}

/// What the controller told the worker to do.
///
/// Deliberately small. `Skip` is absent: the step already ran, so "skip" is not
/// expressible — the honest verb is [`Verdict::Continue`], which accepts the
/// failure and proceeds, mirroring runtime `continue-on-error`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Re-execute the failed step in place.
    Retry,
    /// Accept the failure and run the remaining steps.
    Continue,
    /// Fail the job now, running `post`/`always()` cleanup.
    Abort,
}

impl Verdict {
    /// Stable lowercase label.
    pub fn as_str(self) -> &'static str {
        match self {
            Verdict::Retry => "retry",
            Verdict::Continue => "continue",
            Verdict::Abort => "abort",
        }
    }
}

/// How much of the failed attempt's workspace debris to undo before retrying.
///
/// Defaults to [`RevertPolicy::None`] because Preloop does not guess: a step
/// that regenerates committed codegen is indistinguishable from one that
/// corrupted it, so the controller is shown the change set and asked.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RevertPolicy {
    /// Retry in place. The attempt's writes stay.
    #[default]
    None,
    /// Delete files the attempt created. Never touches tracked content, so it
    /// cannot discard an edit that existed before the step ran.
    Untracked,
    /// Delete created files and restore modified tracked files from the
    /// pristine snapshot.
    All,
}

impl RevertPolicy {
    /// Stable lowercase label.
    pub fn as_str(self) -> &'static str {
        match self {
            RevertPolicy::None => "none",
            RevertPolicy::Untracked => "untracked",
            RevertPolicy::All => "all",
        }
    }
}

/// A structured diagnostic lifted from the runner's problem matchers.
///
/// Preferred over a stderr excerpt for the failure banner: the real error is
/// frequently not in the last twenty lines.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    /// `error` or `warning`.
    pub level: String,
    /// Workspace-relative path, when the matcher captured one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    /// 1-based line number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<u64>,
    /// 1-based column number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<u64>,
    /// Human-readable diagnostic text.
    pub message: String,
}

/// The step that failed, as the controller needs to see it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailedStep {
    /// Zero-based index into the resolved step list.
    pub index: usize,
    /// Total steps in the job, for `4/6`-style display.
    pub total: usize,
    /// Expression-context key (`__run_2`, or the user's `id:`).
    pub context_name: String,
    /// Resolved human-readable name.
    pub display_name: String,
    /// The command as executed, when the step is a `run:` script.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// Working directory inside the guest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_directory: Option<String>,
    /// Process exit code, when the failure was a non-zero exit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Wall time of the failed attempt.
    pub elapsed_ms: u64,
    /// Structured diagnostics, matcher-derived.
    #[serde(default)]
    pub diagnostics: Vec<Diagnostic>,
    /// Trailing log excerpt, used only when `diagnostics` is empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_excerpt: Option<String>,
}

/// Compact summary of a job step, sent with the session so a controller can
/// display a numbered list and accept `--from <name>` without guessing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepSummary {
    /// Zero-based index in the resolved step list.
    pub index: usize,
    /// Expression-context key (`__run_2`, or the user's `id:`).
    pub context_name: String,
    /// Resolved human-readable name.
    pub display_name: String,
}

/// One execution of a step. Retries append rather than overwrite, so the job
/// report can show what changed between attempts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptRecord {
    /// 1-based attempt number for this step.
    pub attempt: u32,
    /// `Success`, `Failure`, or `Cancelled`.
    pub outcome: String,
    /// Process exit code, when the attempt ended in a non-zero exit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Wall time of this attempt.
    pub elapsed_ms: u64,
    /// Source revision the attempt ran against (`original`, `repair-1`, ...).
    pub source_revision: String,
}

/// Everything a controller needs to orient itself at the failure point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebugSession {
    /// Server-assigned session identifier.
    pub session_id: String,
    /// Run this session belongs to.
    pub run_id: RunId,
    /// Job within the run.
    pub job_id: JobId,
    /// Display name of the job, for multi-job runs.
    pub job_name: String,
    /// Current lifecycle state.
    pub state: SessionState,
    /// Monotonic, bumped on every mutation. Controllers use it to detect
    /// races; an agent retrying a request compares versions rather than
    /// blindly re-issuing.
    pub version: u64,
    /// The step that failed.
    pub step: FailedStep,
    /// Every execution of the failed step, oldest first.
    #[serde(default)]
    pub attempts: Vec<AttemptRecord>,
    /// What the failed attempt itself changed in the workspace.
    ///
    /// Computed as the delta between a snapshot taken before the step ran and
    /// one taken at the pause, so it excludes anything that was already dirty
    /// when the step started.
    #[serde(default)]
    pub attempt_changes: Vec<WorkspaceChange>,
    /// All steps in this job, so a controller can offer `:retry --from`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub job_steps: Vec<StepSummary>,
    /// Guest VM name, when the orchestrator supplied one. Needed to open a
    /// shell into the live machine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<String>,
    /// Absolute workspace path inside the guest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    /// Commit the immutable workspace snapshot was cut at. The pristine ref
    /// for change detection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_commit: Option<String>,
    /// Current source revision label.
    pub source_revision: String,
    /// Identifier of the controller holding the lease, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller: Option<String>,
    /// Unix millis when the step failed.
    pub created_at_ms: u64,
    /// Seconds this job has spent paused. Excluded from timeout accounting.
    pub paused_seconds: u64,
}

/// Worker → server when a step fails under debugging.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenSessionRequest {
    /// Run this session belongs to.
    pub run_id: RunId,
    /// Job within the run.
    pub job_id: JobId,
    /// AzDO job GUID for this request.
    ///
    /// The authoritative key. `job_id` is the workflow-level name (`build`),
    /// which is ambiguous across matrix legs and does not match what the
    /// worker knows itself as; the server indexes active requests by this GUID.
    pub agent_job_id: uuid::Uuid,
    /// Display name of the job.
    pub job_name: String,
    /// The step that failed.
    pub step: FailedStep,
    /// Guest VM name, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<String>,
    /// Absolute workspace path inside the guest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    /// Commit of the pristine workspace snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_commit: Option<String>,
    /// Prior attempts of this step, when reopening after a failed retry.
    #[serde(default)]
    pub attempts: Vec<AttemptRecord>,
    /// What this attempt changed in the workspace.
    #[serde(default)]
    pub attempt_changes: Vec<WorkspaceChange>,
    /// All steps in this job.
    #[serde(default)]
    pub job_steps: Vec<StepSummary>,
}

/// Server → worker on session open.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenSessionResponse {
    /// Server-assigned session identifier.
    pub session_id: String,
}

/// Worker → server: exchange the job runtime token for this job's
/// debug-worker credential.
///
/// The debug-worker token is deliberately *not* delivered in the job message.
/// The official runner projects every `isSecret` variable into the `secrets`
/// context, so anything shipped that way is readable from workflow YAML as
/// `${{ secrets['…'] }}` — which would hand untrusted steps the very
/// credential the debug privilege split exists to withhold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerTokenRequest {
    /// AzDO job GUID the worker is executing.
    ///
    /// Stated explicitly rather than inferred so the server can reject a
    /// mismatch against the job named by the presented runtime token, instead
    /// of silently issuing for whichever job the token happened to name.
    pub agent_job_id: uuid::Uuid,
}

/// Server → worker: the debug-worker credential for exactly one job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerTokenResponse {
    /// Bearer token accepted only on this job's debug-session routes.
    pub token: String,
}

/// Controller → server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerdictRequest {
    /// What the worker should do next.
    pub verdict: Verdict,
    /// How much of the failed attempt's debris to undo first. Only meaningful
    /// with [`Verdict::Retry`].
    #[serde(default)]
    pub revert: RevertPolicy,
    /// Who issued it, for the audit trail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller: Option<String>,
    /// Source revision the controller synced before deciding. Recorded on the
    /// next attempt so the journal shows what each attempt ran against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
    /// When set, re-execute from this zero-based step index instead of only
    /// the failed step. `Some(0)` means restart from the first user step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_from_step: Option<usize>,
}

/// Server → worker when the long poll resolves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerdictResponse {
    /// `None` means the poll timed out with no decision — the worker polls
    /// again. Distinguishing "no verdict yet" from "abort" matters: a dropped
    /// connection must never be read as a decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict: Option<Verdict>,
    /// Session version at the time of the response.
    pub version: u64,
    /// Revert policy the controller chose.
    #[serde(default)]
    pub revert: RevertPolicy,
    /// Source revision recorded against the next attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
    /// Step index to restart from, when the controller asked for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_from_step: Option<usize>,
}

/// Structured event consumed by an agent debugging a paused job.
///
/// Events deliberately carry the failure context and references rather than
/// an unbounded terminal transcript. The agent can request more evidence using
/// the operation surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEvent {
    /// Monotonic sequence within one debug session.
    pub event_id: u64,
    /// Stable discriminator such as `step_failed` or `retry_requested`.
    pub event: String,
    /// Session this event belongs to.
    pub session_id: String,
    /// Debug session version when the event was recorded.
    pub session_version: u64,
    /// Run this event belongs to.
    pub run_id: RunId,
    /// Job within the run.
    pub job_id: JobId,
    /// Display name of the job.
    pub job_name: String,
    /// Failed step context, present on failure events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<FailedStep>,
    /// Stable reference to the detailed attempt log, when one exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_reference: Option<String>,
    /// Human-readable event detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Capabilities available to the agent for this session.
    #[serde(default)]
    pub capabilities: Vec<String>,
}

/// Request to acquire the single mutating agent lease for a session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentLeaseRequest {
    /// Stable caller identity, shown in the audit trail.
    pub controller: String,
    /// Capabilities requested by the caller. The server grants only supported
    /// capabilities; an empty list gets the safe control-only default.
    #[serde(default)]
    pub capabilities: Vec<String>,
}

/// Lease granted to an agent controller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentLeaseResponse {
    /// Opaque lease credential required on mutating operations.
    pub lease_id: String,
    /// Controller identity recorded in the audit trail.
    pub controller: String,
    /// Capabilities granted by the server.
    pub capabilities: Vec<String>,
    /// Session version observed when the lease was acquired.
    pub session_version: u64,
}

/// Events after an optional sequence number.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEventsResponse {
    /// Events after the requested sequence number.
    pub events: Vec<AgentEvent>,
    /// Highest event id returned, for reconnecting consumers.
    pub next_event_id: u64,
}

/// Typed operation submitted by an agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum AgentOperation {
    /// Retry only the failed step.
    Retry {
        /// Workspace cleanup policy before retry.
        #[serde(default)]
        revert: RevertPolicy,
    },
    /// Retry from an earlier step. Index is zero-based on the wire.
    RetryFrom {
        /// Zero-based target step index.
        step_index: usize,
        /// Workspace cleanup policy before retry.
        #[serde(default)]
        revert: RevertPolicy,
    },
    /// Abort the job and run normal cleanup.
    Abort,
}

/// Agent operation request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentOperationRequest {
    /// Client-generated idempotency key.
    pub request_id: String,
    /// Optimistic-concurrency version. Required for new mutations.
    pub expected_version: u64,
    /// Lease credential returned by the acquire endpoint.
    pub lease_id: String,
    /// Operation to execute.
    pub operation: AgentOperation,
}

/// Result of a typed agent operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentOperationResponse {
    /// Idempotency key echoed from the request.
    pub request_id: String,
    /// Session version before the operation.
    pub prev_version: u64,
    /// Session version after the operation.
    pub new_version: u64,
    /// Stable result label.
    pub status: String,
    /// Session projection after the operation.
    pub session: DebugSession,
}

/// One audited agent mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAuditEntry {
    /// Idempotency key of the mutation.
    pub request_id: String,
    /// Agent controller identity.
    pub controller: String,
    /// Stable operation label.
    pub operation: String,
    /// Result label.
    pub status: String,
    /// Session version before the operation.
    pub prev_version: u64,
    /// Session version after the operation.
    pub new_version: u64,
    /// Unix epoch milliseconds when the operation was accepted.
    pub timestamp_ms: u64,
}

/// A workspace path changed since the job's pristine snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceChange {
    /// Workspace-relative path.
    pub path: String,
    /// Git-level change kind.
    pub status: ChangeStatus,
    /// Revert policy that applies to this path.
    pub category: ChangeCategory,
}

/// Git-level change kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeStatus {
    /// Content differs from the pristine snapshot.
    Modified,
    /// Present now, absent in the snapshot.
    Added,
    /// Absent now, present in the snapshot.
    Deleted,
}

impl ChangeStatus {
    /// Single-character sigil for terminal output.
    pub fn sigil(self) -> char {
        match self {
            ChangeStatus::Modified => 'M',
            ChangeStatus::Added => '+',
            ChangeStatus::Deleted => '-',
        }
    }
}

/// Which revert policy applies to a path.
///
/// The split is the whole design: tracked files are restorable from the
/// pristine snapshot at zero storage cost, untracked files are deletable, and
/// ignored cache must never be touched because reverting it destroys the warm
/// state that makes in-place retry worth doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeCategory {
    /// Tracked by git. Restorable from the snapshot. Requires confirmation,
    /// because a step that legitimately regenerates committed codegen is
    /// indistinguishable from one that corrupted it.
    Tracked,
    /// Untracked and not ignored. Safe to delete.
    Untracked,
    /// Gitignored build output or cache. Never reverted.
    Cache,
}

/// Result of diffing the live workspace against its pristine snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceDiff {
    /// Every detected change, in git's reporting order.
    pub changes: Vec<WorkspaceChange>,
    /// Per-category counts, for a one-line summary without walking `changes`.
    #[serde(default)]
    pub counts: BTreeMap<String, usize>,
}

impl WorkspaceDiff {
    /// Paths that can be reverted from the pristine snapshot.
    pub fn tracked(&self) -> impl Iterator<Item = &WorkspaceChange> {
        self.changes
            .iter()
            .filter(|c| c.category == ChangeCategory::Tracked)
    }

    /// Paths that can be reverted by deletion.
    pub fn untracked(&self) -> impl Iterator<Item = &WorkspaceChange> {
        self.changes
            .iter()
            .filter(|c| c.category == ChangeCategory::Untracked)
    }

    /// Whether anything is revertible. Cache-only changes are not.
    pub fn has_revertible(&self) -> bool {
        self.changes
            .iter()
            .any(|c| c.category != ChangeCategory::Cache)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_states_hold_the_job() {
        for state in [
            SessionState::Paused,
            SessionState::Attached,
            SessionState::Retrying,
        ] {
            assert!(state.is_open(), "{state:?} must hold the job open");
        }
        for state in [
            SessionState::Resumed,
            SessionState::Aborted,
            SessionState::Abandoned,
        ] {
            assert!(!state.is_open(), "{state:?} must release the job");
        }
    }

    #[test]
    fn absent_verdict_is_not_an_abort() {
        // A timed-out long poll deserializes to `None`, never a decision.
        let json = r#"{"version":3}"#;
        let parsed: VerdictResponse = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.verdict, None);
        assert_eq!(parsed.version, 3);
    }

    #[test]
    fn revert_defaults_to_doing_nothing() {
        // Absent means "retry in place". A missing field must never be read as
        // permission to discard a step's output.
        let parsed: VerdictRequest = serde_json::from_str(r#"{"verdict":"retry"}"#).unwrap();
        assert_eq!(parsed.revert, RevertPolicy::None);
        assert_eq!(RevertPolicy::default(), RevertPolicy::None);
    }

    #[test]
    fn cache_changes_are_never_revertible() {
        let diff = WorkspaceDiff {
            changes: vec![WorkspaceChange {
                path: "target/debug/foo".into(),
                status: ChangeStatus::Modified,
                category: ChangeCategory::Cache,
            }],
            counts: BTreeMap::new(),
        };
        assert!(!diff.has_revertible());
        assert_eq!(diff.tracked().count(), 0);
        assert_eq!(diff.untracked().count(), 0);
    }

    #[test]
    fn session_roundtrips_through_json() {
        let session = DebugSession {
            session_id: "dbg_abc".into(),
            run_id: RunId::new(),
            job_id: JobId("build".to_owned()),
            job_name: "build".into(),
            state: SessionState::Paused,
            version: 1,
            step: FailedStep {
                index: 3,
                total: 6,
                context_name: "__run_2".into(),
                display_name: "Run cargo test".into(),
                command: Some("cargo test --workspace".into()),
                working_directory: Some("/work".into()),
                exit_code: Some(101),
                elapsed_ms: 18_400,
                diagnostics: vec![Diagnostic {
                    level: "error".into(),
                    file: Some("src/lib.rs".into()),
                    line: Some(42),
                    column: None,
                    message: "assertion failed".into(),
                }],
                log_excerpt: None,
            },
            attempts: Vec::new(),
            attempt_changes: Vec::new(),
            job_steps: Vec::new(),
            machine: Some("preloop-runner-0-1".into()),
            workspace: Some("/work".into()),
            snapshot_commit: Some("deadbeef".into()),
            source_revision: "original".into(),
            controller: None,
            created_at_ms: 1_700_000_000_000,
            paused_seconds: 0,
        };
        let encoded = serde_json::to_string(&session).unwrap();
        let decoded: DebugSession = serde_json::from_str(&encoded).unwrap();
        assert_eq!(session, decoded);
    }

    #[test]
    fn agent_retry_operation_has_a_stable_wire_shape() {
        let request = AgentOperationRequest {
            request_id: "retry-1".into(),
            expected_version: 4,
            lease_id: "lease_1".into(),
            operation: AgentOperation::RetryFrom {
                step_index: 0,
                revert: RevertPolicy::None,
            },
        };
        let encoded = serde_json::to_value(&request).unwrap();
        assert_eq!(encoded["operation"]["operation"], "retry_from");
        assert_eq!(encoded["operation"]["step_index"], 0);
        let decoded: AgentOperationRequest = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, request);
    }
}
