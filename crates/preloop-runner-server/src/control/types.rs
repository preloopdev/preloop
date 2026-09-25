//! Domain types for the [`ControlBackend`](super::ControlBackend) contract.
//!
//! Commands take owned inputs and return owned results; no transaction
//! handle, SQL string, predicate or mutable record escapes the backend.
//! Everything here is backend-neutral: SQLite and Postgres produce identical
//! domain results from the same inputs.

use super::*;
use crate::models::{QueuedJob, RunRecord, TaskAgentJobRequestRecord};
use preloop_gha_protocol::azdo;
use preloop_gha_protocol::crypto::{AgentRsaPublicKey, SessionEncryption};

use preloop_gha_protocol::{ExecutionStatus, JobId, RegisteredRunner, RunId};

/// Default tenant boundary. Local mode runs entirely inside this namespace;
/// hosted cells route namespaces to databases but the command contract is
/// identical.
pub const DEFAULT_NAMESPACE: &str = "default";

/// Canonical label-set identity for an indexed eligibility lookup. A runner
/// matches a *subset*, not one identical key; the backend still enforces
/// capability matching before a policy can use this as a pool filter.
pub(crate) fn compute_pool_key(runs_on: &[String], runner_group: Option<&str>) -> String {
    let mut labels: Vec<String> = runs_on.iter().map(|s| s.to_ascii_lowercase()).collect();
    labels.sort_unstable();
    labels.dedup();
    let group = runner_group.unwrap_or("").to_ascii_lowercase();
    serde_json::to_string(&(group, labels)).expect("strings serialize to JSON")
}

/// Queue classification for a job row. `jobs.state` (the `ExecutionStatus`
/// in the run record and the `jobs.status` column) is canonical workflow
/// truth; `queue_kind` is the derived dispatch copy — never independently
/// mutable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueKind {
    /// In the ready queue, claimable by a matching runner.
    Ready,
    /// Waiting on `needs:` or `max_parallel` — not claimable.
    Pending,
    /// Parked behind a concurrency gate.
    Blocked,
    /// Held with its run behind a workflow-level concurrency gate.
    Held,
    /// Popped by a claim; retained for requeue-on-runner-death.
    Claimed,
    /// Awaiting deferred subtree expansion (reusable caller / dynamic
    /// matrix). Never reaches the ready queue itself.
    Expand,
    /// Not in any dispatch collection (terminal, or a caller placeholder).
    None,
}

impl QueueKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Pending => "pending",
            Self::Blocked => "blocked",
            Self::Held => "held",
            Self::Claimed => "claimed",
            Self::Expand => "expand",
            Self::None => "none",
        }
    }

    pub(crate) fn parse(s: &str) -> Option<Self> {
        match s {
            "ready" => Some(Self::Ready),
            "pending" => Some(Self::Pending),
            "blocked" => Some(Self::Blocked),
            "held" => Some(Self::Held),
            "claimed" => Some(Self::Claimed),
            "expand" => Some(Self::Expand),
            "none" => Some(Self::None),
            _ => None,
        }
    }
}

/// Everything the submit path pre-built outside the transaction: the run
/// record plus one queue entry per non-skipped job and the correlation
/// records minted for dispatchable jobs.
#[derive(Debug)]
pub(crate) struct SubmitRun {
    pub(crate) namespace: String,
    pub(crate) record: RunRecord,
    /// Jobs in workflow order with their pre-dispatch classification input.
    /// The command evaluates workflow/job/jobset gates inside the
    /// transaction and assigns each job its initial `QueueKind`.
    pub(crate) jobs: Vec<SubmitJob>,
    /// Evaluated workflow-level concurrency `(group, cancel_in_progress,
    /// queue_mode, raw)` — `None` when the workflow declares none or the
    /// group evaluated empty (the `empty_group` flag distinguishes those).
    pub(crate) workflow_concurrency: Option<WorkflowConcurrency>,
    /// Workflow declared a concurrency block whose group evaluated empty —
    /// the run is inserted already Failed.
    pub(crate) empty_concurrency_group: bool,
    /// Platforms registered runners can host, for the unhostable-platform
    /// check (evaluated inside the transaction against live runner rows).
    /// `true` = perform the check.
    pub(crate) check_hostable: bool,
}

#[derive(Debug)]
pub(crate) struct WorkflowConcurrency {
    pub(crate) group: String,
    pub(crate) cancel_in_progress: bool,
    pub(crate) queue: preloop_gha_parser::ConcurrencyQueue,
    pub(crate) raw: preloop_gha_parser::Concurrency,
}

#[derive(Debug)]
pub(crate) struct SubmitJob {
    pub(crate) queued: QueuedJob,
    /// Minted request/message correlation for dispatchable jobs. Caller
    /// placeholder nodes carry `None`.
    pub(crate) request: Option<TaskAgentJobRequestRecord>,
    /// Deferred App-token mint request for this job's request id.
    pub(crate) token_request: Option<crate::models::GitHubTokenRequest>,
    /// `id-token: write` grant resolved at build time.
    pub(crate) id_token_granted: bool,
    /// Resolved OIDC execution context.
    pub(crate) oidc_context: Option<crate::state::OidcJobContext>,
    /// Declared step manifest for the attempt (seeded at submit).
    pub(crate) step_manifest: Vec<crate::models::StepRecord>,
    /// Job was concluded `Skipped` by its `if:` at submit.
    pub(crate) initially_skipped: bool,
}

/// Outcome of `submit_run`.
#[derive(Debug)]
pub(crate) struct SubmitOutcome {
    pub(crate) run_id: RunId,
    pub(crate) run_number: u64,
    pub(crate) queued_jobs: usize,
    /// Run status after insertion (Queued / Pending / terminal).
    pub(crate) status: ExecutionStatus,
    /// Jobs concluded at submit with their reasons (unhostable platform,
    /// gate rejection, initial skips) — the handler emits events for them.
    pub(crate) concluded: Vec<(JobId, ExecutionStatus, Option<String>)>,
    /// Whether the run was parked behind a workflow-level gate.
    pub(crate) held: bool,
    /// Whether the run was rejected outright (empty group / queue overflow).
    pub(crate) rejected: Option<ExecutionStatus>,
    /// A replayed webhook delivery found an existing run — the handler
    /// returns that acceptance instead of a new run.
    pub(crate) existing: Option<Box<RunRecord>>,
    /// Ready-queue depth after the transition — the handler stores it in
    /// the node-local `queue_depth` gauge that wakes the runner supervisor.
    pub(crate) queue_depth: usize,
    /// `runs-on` labels of the next ready job, for `next_job_runs_on`.
    pub(crate) next_runs_on: Vec<String>,
}

/// What a session poll produced. The handler maps each variant onto the
/// wire response; `Empty` means long-poll or return the empty body.
#[derive(Debug)]
pub(crate) enum PollOutcome {
    /// A previously delivered message is still unacknowledged — redeliver.
    Inflight(azdo::TaskAgentMessage),
    /// A cancellation is pending for this session's active job.
    Cancel(azdo::TaskAgentMessage),
    /// The session already owns a live request — return its broker ref.
    ActiveRequest {
        request: TaskAgentJobRequestRecord,
        runner_id: i64,
    },
    /// A job was claimed for this session.
    Claimed(Box<ClaimedJob>),
    /// Nothing to deliver.
    Empty,
}

/// A claimed job: the queue entry plus the committed request record and the
/// data the acquire path needs without a second lock.
#[derive(Debug)]
pub(crate) struct ClaimedJob {
    pub(crate) queued: QueuedJob,
    pub(crate) request: TaskAgentJobRequestRecord,
    pub(crate) runner_id: i64,
    /// Queue depth after the claim (for the supervisor atomic).
    pub(crate) queue_depth: usize,
    /// `runs-on` of the new queue front, for golden selection.
    pub(crate) next_runs_on: Vec<String>,
}

/// Everything `acquirejob` needs in one read: request, message, grant and
/// the token-mint request.
#[derive(Debug)]
pub(crate) struct AcquireContext {
    pub(crate) request: TaskAgentJobRequestRecord,
    pub(crate) message: azdo::AgentJobRequestMessage,
    pub(crate) token_request: Option<crate::models::GitHubTokenRequest>,
    pub(crate) id_token_granted: bool,
    /// Run submission fields the token re-derivation path needs when the
    /// build-time request was lost: `(repository, trust_tier)`.
    pub(crate) repository: String,
    pub(crate) trust_tier: Option<String>,
}

/// Result of `complete_job`: the post-transition run plus everything the
/// handler must fan out (events, check-run effects, live-log close).
#[derive(Debug)]
pub(crate) struct CompleteOutcome {
    pub(crate) record: RunRecord,
    /// The status actually stored (terminal-locked jobs keep their first
    /// verdict).
    pub(crate) effective_status: ExecutionStatus,
    /// True when this completion first made the run terminal-success.
    pub(crate) newly_terminal_success: bool,
    /// Sibling jobs fail-fast cancelled.
    pub(crate) cancelled_siblings: Vec<JobId>,
    /// Jobs promoted/skipped/failed by the post-completion sweep.
    pub(crate) scheduling: crate::runtime_scheduling::SchedulingOutcome,
    /// Live-log key to close (attempt-scoped).
    pub(crate) live_log_key: String,
    /// Whether the ready queue or cancellation queue is non-empty.
    pub(crate) queue_nonempty: bool,
    /// Queue depth after the transition.
    pub(crate) queue_depth: usize,
    /// Whether the completion was a replay of an already-terminal job.
    pub(crate) replayed: bool,
}

/// Result of `cancel_run` / `cancel_job`.
#[derive(Debug)]
pub(crate) struct CancelOutcome {
    /// Cancellation messages queued for in-progress jobs.
    pub(crate) cancellations: usize,
    /// Run status after cancellation (for `cancel_run`).
    pub(crate) run_status: Option<ExecutionStatus>,
    /// Whether the queue is non-empty after the transition.
    pub(crate) queue_nonempty: bool,
}

/// A leased expansion node: the queue entry plus the immutable inputs the
/// build needs, claimed under `expand_generation`.
pub(crate) struct ExpansionClaim {
    pub(crate) job: QueuedJob,
    /// Generation the node was claimed under — the apply is fenced on it.
    pub(crate) generation: i64,
    pub(crate) plan: Option<crate::control::sched::ExpansionPlan>,
}

#[derive(Clone)]
pub(crate) struct RunnerRow {
    pub(crate) runner: RegisteredRunner,
    pub(crate) rsa_public_key: Option<AgentRsaPublicKey>,
    pub(crate) client_id: Option<String>,
    pub(crate) pool_proven: bool,
    pub(crate) registered_at_us: i64,
}
/// A live session row.
#[derive(Clone)]
pub(crate) struct SessionRow {
    pub(crate) session_id: String,
    pub(crate) runner_id: i64,
    pub(crate) protocol: SessionProtocol,
    pub(crate) client_id: Option<String>,
    /// Sealed session crypto material (AES key/iv state).
    pub(crate) encryption: Option<SessionEncryption>,
    pub(crate) active_request_id: Option<i64>,
    pub(crate) last_seen_at_us: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionProtocol {
    /// Modern broker session (RunnerJobRequest broker refs).
    Broker,
    /// AzDO distributedtask session (encrypted PipelineAgentJobRequest).
    Azdo,
    /// Compatibility session with no registered runner (e.g. the implicit
    /// `default` session used by unauthenticated/legacy polls). Persisted with
    /// a NULL `runner_id` so `broker_messages`/`active_request_id` foreign keys
    /// still resolve, while `runner_id_for_session` reports no runner.
    Compat,
}

impl SessionProtocol {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Broker => "broker",
            Self::Azdo => "azdo",
            Self::Compat => "compat",
        }
    }
    pub(crate) fn parse(s: &str) -> Self {
        match s {
            "azdo" => Self::Azdo,
            "compat" => Self::Compat,
            _ => Self::Broker,
        }
    }
}

/// Queue pressure snapshot for status/metrics.
#[derive(Debug, Default)]
pub(crate) struct QueueStats {
    pub(crate) ready: usize,
    pub(crate) pending: usize,
    pub(crate) blocked: usize,
    pub(crate) held: usize,
    pub(crate) claimed: usize,
    pub(crate) expanding: usize,
    /// `runs-on` labels of the ready-queue front.
    pub(crate) next_runs_on: Vec<String>,
}

/// Errors every backend maps onto the same domain vocabulary. Handlers
#[derive(Debug)]
pub(crate) enum ControlError {
    NotFound(String),
    Conflict(String),
    Forbidden(String),
    BadRequest(String),
    /// The fencing token presented (lease owner/generation) no longer owns
    /// the resource — a stale worker racing a successor.
    Stale(String),
    /// Backend failure (connection, serialization, integrity). Transient
    /// failures are retried by the caller's own policy.
    Backend(anyhow::Error),
}

impl std::fmt::Display for ControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(m) => write!(f, "not found: {m}"),
            Self::Conflict(m) => write!(f, "conflict: {m}"),
            Self::Forbidden(m) => write!(f, "forbidden: {m}"),
            Self::BadRequest(m) => write!(f, "bad request: {m}"),
            Self::Stale(m) => write!(f, "stale fence: {m}"),
            Self::Backend(e) => write!(f, "backend: {e}"),
        }
    }
}

impl std::error::Error for ControlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Backend(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

impl ControlError {
    pub(crate) fn backend(error: impl Into<anyhow::Error>) -> Self {
        Self::Backend(error.into())
    }
}

impl From<ControlError> for ApiError {
    fn from(error: ControlError) -> Self {
        match error {
            ControlError::NotFound(message) => ApiError::not_found(message),
            ControlError::Conflict(message) => ApiError::conflict(message),
            ControlError::Forbidden(message) => ApiError::forbidden(message),
            ControlError::BadRequest(message) => ApiError::bad_request(message),
            ControlError::Stale(message) => ApiError::conflict(message),
            ControlError::Backend(error) => {
                // Driver detail (SQL text, constraint names, PG DETAIL echoing
                // row values) is free schema/tenant reconnaissance for any
                // caller that can reach a control-backed handler — including
                // untrusted workflow code holding a runtime token. Log it
                // server-side; return a fixed message to the client.
                tracing::error!(?error, "control backend error");
                ApiError::internal("control backend error")
            }
        }
    }
}
