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

/// `jobs.status` / `job_requests.result` wire form (the `ExecutionStatus`
/// stored copy).
pub(crate) fn status_str(s: ExecutionStatus) -> &'static str {
    match s {
        ExecutionStatus::Queued => "queued",
        ExecutionStatus::Pending => "pending",
        ExecutionStatus::InProgress => "in_progress",
        ExecutionStatus::Success => "success",
        ExecutionStatus::Failure => "failure",
        ExecutionStatus::Skipped => "skipped",
        ExecutionStatus::Cancelled => "cancelled",
    }
}

/// Inverse of [`status_str`]; unknown strings map to `Queued`.
pub(crate) fn status_parse(s: &str) -> ExecutionStatus {
    match s {
        "pending" => ExecutionStatus::Pending,
        "in_progress" => ExecutionStatus::InProgress,
        "success" => ExecutionStatus::Success,
        "failure" => ExecutionStatus::Failure,
        "skipped" => ExecutionStatus::Skipped,
        "cancelled" => ExecutionStatus::Cancelled,
        _ => ExecutionStatus::Queued,
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
    /// `id_token_grants` row for the attempt's job: `Some` = recorded grant,
    /// `None` = no row (the caller falls back to wire markers).
    pub(crate) id_token_granted: Option<bool>,
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

/// Check-run reporting inputs for one run (see `run_dispatch_info`).
#[derive(Debug)]
pub(crate) struct RunDispatchInfo {
    pub(crate) repository: String,
    pub(crate) sha: String,
    pub(crate) started_at: Option<std::time::SystemTime>,
    pub(crate) completed_at: Option<std::time::SystemTime>,
    /// Per logical job, in `job_id` order.
    pub(crate) jobs: Vec<RunDispatchJob>,
}

/// One logical job's reporting inputs.
#[derive(Debug)]
pub(crate) struct RunDispatchJob {
    pub(crate) job_id: JobId,
    pub(crate) status: ExecutionStatus,
    /// `run_jobs.display_name` — the evaluated GitHub name, when stored.
    pub(crate) display_name: Option<String>,
    pub(crate) check_run_id: Option<u64>,
    /// Stored `jobs_list` entry (annotations, step projection) if present.
    pub(crate) detail: Option<crate::models::JobDetail>,
    /// Latest attempt's step manifest (`job_steps`), empty pre-dispatch.
    pub(crate) steps: Vec<crate::models::StepRecord>,
}

/// Submission fields the push paths read (`submission_json` holds the
/// workflow submission — decoded by the backend).
#[derive(Debug)]
pub(crate) struct SubmissionFields {
    pub(crate) repository: String,
    pub(crate) sha: String,
    pub(crate) base_ref: Option<String>,
    pub(crate) git_ref: String,
}

/// How a run participates in concurrency groups, which decides the lock scope
/// of a command on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunConcurrency {
    /// No group: every command stays under the run lock.
    None,
    /// Only a workflow-level group (`Holder::Run`). Its hold changes only
    /// when the whole run becomes terminal, so a command that leaves the run
    /// non-terminal stays under the run lock.
    WorkflowOnly,
    /// Job-level or reusable-caller gates: any job transition may acquire or
    /// release a group.
    Gated,
}

impl RunConcurrency {
    pub(crate) fn any(self) -> bool {
        self != Self::None
    }
}

impl RunConcurrency {
    /// From the two facts both backends query: any job-level/jobset gate or
    /// non-run hold (`gated`), and any concurrency at all (`any`).
    pub(crate) fn classify(gated: bool, any: bool) -> Self {
        match (gated, any) {
            (true, _) => Self::Gated,
            (false, true) => Self::WorkflowOnly,
            (false, false) => Self::None,
        }
    }
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
    /// A command ran under a narrow lock scope and found it must touch state
    /// outside it; the transaction rolled back and the caller reruns it
    /// under the wider scope. Never escapes the retrying caller.
    WidenScope,
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
            Self::WidenScope => write!(f, "command needs a wider lock scope"),
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
            ControlError::WidenScope => {
                tracing::error!("scope-widening signal escaped its retry");
                ApiError::internal("control backend error")
            }
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

/// Compare the stored cluster key fingerprint with this node's.
pub(crate) fn check_key_fingerprint(stored: &[u8], fingerprint: &str) -> Result<(), ControlError> {
    if stored == fingerprint.as_bytes() {
        return Ok(());
    }
    Err(ControlError::backend(anyhow::anyhow!(
        "this node's key (fingerprint {fingerprint}) differs from the one the control \
         database was created with ({}). Every engine node on one database must share \
         the same key: set {} to the cluster key.",
        String::from_utf8_lossy(stored),
        crate::state::HMAC_KEY_ENV,
    )))
}

/// Records kept per timeline (a runner timeline has tens of records; this
/// bounds a misbehaving client).
pub(crate) const MAX_TIMELINE_RECORDS: usize = 1024;

/// Stamp patched timeline records with the new change id and modification
/// time, returning `(record_id, json)` pairs ready to upsert.
pub(crate) fn stamp_timeline_records(
    records: &mut [preloop_gha_protocol::azdo::TimelineRecord],
    change_id: i64,
    now: std::time::SystemTime,
) -> Vec<(String, String)> {
    let modified = chrono::DateTime::<chrono::Utc>::from(now)
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    records
        .iter_mut()
        .map(|record| {
            record.change_id = Some(change_id as i32);
            record.last_modified = Some(modified.clone());
            (
                record.id.to_string(),
                serde_json::to_string(record).unwrap_or_default(),
            )
        })
        .collect()
}

/// Why a conditional lease renewal matched no row, from the attempt's
/// `(result, owner_runner_id)` (`None` = no such attempt). `Ok(false)` means
/// the attempt has no recorded owner and the caller must use the
/// transactional path.
pub(crate) fn renew_miss(
    row: Option<(Option<String>, Option<i64>)>,
    runner_id: i64,
) -> Result<bool, ControlError> {
    match row {
        None => Err(ControlError::NotFound(
            "broker renew request not found".to_owned(),
        )),
        Some((Some(_), _)) => Err(ControlError::Conflict(
            "broker request already completed".to_owned(),
        )),
        Some((None, Some(owner))) if owner != runner_id => Err(ControlError::Forbidden(
            "broker request belongs to another runner".to_owned(),
        )),
        Some((None, None)) => Ok(false),
        // Owner matches and still in flight: raced a concurrent writer; the
        // caller retries through the transactional path.
        Some((None, Some(_))) => Ok(false),
    }
}
/// Broker-acquire ownership check. Prefers the immutable owner recorded at
/// claim; otherwise falls back to the owner session's runner. An assigned
/// request whose session carries no runner still acquires, for session-less
/// replay flows (golden session ids).
pub(crate) fn ensure_request_owner(
    owner_runner_id: Option<i64>,
    session_runner: Option<i64>,
    has_session: bool,
    runner_id: i64,
) -> Result<(), ControlError> {
    if let Some(owner) = owner_runner_id {
        return if owner == runner_id {
            Ok(())
        } else {
            Err(ControlError::Forbidden(
                "broker request belongs to another runner".to_owned(),
            ))
        };
    }
    match (has_session, session_runner) {
        (_, Some(owner)) if owner == runner_id => Ok(()),
        (_, Some(_)) => Err(ControlError::Forbidden(
            "broker request belongs to another runner".to_owned(),
        )),
        (true, None) => Ok(()),
        (false, None) => Err(ControlError::NotFound(
            "broker request is not assigned to a session".to_owned(),
        )),
    }
}

/// A runner callback resolved to its execution attempt.
#[derive(Debug, Clone)]
pub(crate) struct CallbackJob {
    pub(crate) request_id: i64,
    pub(crate) run_id: RunId,
    pub(crate) job_id: JobId,
    pub(crate) agent_job_id: uuid::Uuid,
    /// The logical job's current status (`None` if its row is gone).
    pub(crate) job_status: Option<ExecutionStatus>,
}

/// One runner-reported step update: upsert by `(attempt, step id)`. A new
/// step is appended as synthetic; timestamps only ever fill in.
#[derive(Debug, Clone)]
pub(crate) struct StepPatch {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) conclusion: String,
    pub(crate) started_at_us: Option<i64>,
    pub(crate) finished_at_us: Option<i64>,
    /// Server observation time: a new step's start when the runner sent none.
    pub(crate) observed_us: i64,
}

/// An unfinished execution attempt, as the reaper sees it.
#[derive(Debug, Clone)]
pub(crate) struct ActiveRequest {
    pub(crate) request_id: i64,
    pub(crate) run_id: RunId,
    pub(crate) job_id: JobId,
    pub(crate) started_at: Option<std::time::SystemTime>,
    pub(crate) last_renewed_at: Option<std::time::SystemTime>,
    pub(crate) timeout_triggered: bool,
    /// The job's `timeout-minutes` in seconds, when its message set one.
    pub(crate) job_timeout_s: Option<i64>,
}

/// A ready-queue job, as the starvation sweep sees it.
#[derive(Debug, Clone)]
pub(crate) struct ReadyRow {
    pub(crate) run_id: RunId,
    pub(crate) job_id: JobId,
    pub(crate) runs_on: Vec<String>,
    pub(crate) enqueued_at_unix_nanos: i64,
    /// The reaper has a first-seen mark on the row (cleared once a runner
    /// can take the job).
    pub(crate) observed: bool,
}

/// Everything one reaper tick decides from, read without a lock.
#[derive(Debug, Clone, Default)]
pub(crate) struct ReapInputs {
    pub(crate) active: Vec<ActiveRequest>,
    pub(crate) ready: Vec<ReadyRow>,
    /// Label sets of every registered runner.
    pub(crate) runner_labels: Vec<Vec<String>>,
    /// Whether any assignment binding or pool-pending mark exists.
    pub(crate) has_bindings: bool,
    /// Runners whose session has not polled within the liveness timeout.
    pub(crate) stale_runners: std::collections::BTreeSet<i64>,
    /// Registrations older than the liveness timeout with no session.
    pub(crate) phantom_runners: std::collections::BTreeSet<i64>,
}

/// One reconciled step report, ready to upsert into `job_steps`.
///
/// `report_steps` resolves conclusions against the job's current status and
/// the report's wire shape (`WorkflowStepsUpdate` `status`/`conclusion`
/// numbers); backends store the result verbatim.
#[derive(Debug)]
pub(crate) struct StepReport {
    pub(crate) step_id: String,
    pub(crate) runner_number: Option<i64>,
    /// Reported display name; `None` keeps the stored name.
    pub(crate) name: Option<String>,
    pub(crate) conclusion: String,
    /// `Some` only when this report first observed the step running/finished.
    /// A terminal-only report never invents `started_at == finished_at`.
    pub(crate) started_at_us: Option<i64>,
    pub(crate) finished_at_us: Option<i64>,
}

/// Fold one `steps[]` entry of `WorkflowStepsUpdate` into a `StepReport`.
///
/// `job_cancelled` selects the cancelled conclusion for numeric 3; `observed`
/// is the server receive time. `None` means the step has no `external_id`
/// and is dropped — guessing by display name merged distinct same-named
/// steps before.
pub(crate) fn step_report(
    step: &serde_json::Value,
    job_cancelled: bool,
    observed: chrono::DateTime<chrono::Utc>,
) -> Option<StepReport> {
    let external_id = step["external_id"].as_str().unwrap_or("");
    if external_id.is_empty() {
        return None;
    }
    let conclusion_num = step["conclusion"].as_u64().unwrap_or(0);
    let status_num = step["status"].as_u64().unwrap_or(0);
    let terminal = status_num == 6;
    let conclusion = if terminal {
        match conclusion_num {
            2 => "success",
            3 if job_cancelled => "cancelled",
            3 => "failure",
            7 => "skipped",
            _ => "success",
        }
    } else {
        "in_progress"
    };
    let runner_number = step["number"].as_u64().and_then(|n| i64::try_from(n).ok());
    // The runner reports the rendered display name ("Run actions/checkout@v4"),
    // the same string GitHub's UI shows, so it wins over the message's name:
    // the server leaves that empty for steps without an explicit `name:`.
    let name = step["name"]
        .as_str()
        .filter(|name| !name.is_empty())
        .map(str::to_owned);
    let observed_us = observed.timestamp_micros();
    Some(StepReport {
        step_id: external_id.to_owned(),
        runner_number,
        name,
        conclusion: conclusion.to_owned(),
        started_at_us: (!terminal).then_some(observed_us),
        finished_at_us: terminal.then_some(observed_us),
    })
}
