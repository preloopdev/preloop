use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PushStatus {
    /// The run requested push-back and it has not been performed yet.
    Pending,
    /// The tested commit is on GitHub and the PR/check runs are in place.
    Synced,
    /// The sync could not be performed (diverged branch, tree mismatch,
    /// GitHub unreachable, …). `error` carries the reason; a later
    /// `preloop push` retry may clear it.
    Blocked,
}

/// Push-back state for a run that requested `submission.push`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushState {
    pub status: PushStatus,
    pub error: Option<String>,
    /// Pull request number, when the branch has an open PR (created or
    /// pre-existing).
    pub pr_number: Option<u64>,
    /// The commit the sync actually published (`submission.sha` for a clean
    /// submission; the materialized branch head for a dirty one). Webhook
    /// dedup matches the echo of our own push against this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_sha: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DapPortRegistration {
    pub port: u16,
    pub job_id: JobId,
}

/// How a step came to exist, which decides whether `--step N` counts it.
///
/// `Workflow` records are built from the job request message before the runner
/// starts, so their order is the workflow's declared order. `Synthetic` records
/// are runner bookkeeping discovered at execution time ("Set up job", `Pre`/
/// `Post` action hooks, container lifecycle, "Complete job"): they own real
/// logs, but must never shift the numbering a user reads off their YAML.
///
/// Verified against the official runner
/// (`.runner-watch/golden/v2.336.0/06-multi-step`): the three declared steps
/// echo the message ids back unchanged as `external_id`, while "Set up job"
/// and "Complete job" carry runner-minted ids absent from the message.
/// Membership in the manifest is therefore the classifier — never the id's
/// shape, and never the display name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    /// Declared in the workflow and present in the job request message.
    Workflow,
    /// Reported by the runner with no manifest entry. This is the default so a
    /// run record written before manifests existed answers `--step` with an
    /// explicit "no workflow manifest" error instead of a guessed blob.
    #[default]
    Synthetic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepRecord {
    /// Stable protocol identity: `TaskStep.id` in the job request message,
    /// echoed as `external_id` in `WorkflowStepsUpdate`, and the name of the
    /// durable `step-<id>.txt` blob. Empty only for a record restored from a
    /// pre-manifest run, which resolution refuses rather than guesses.
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub kind: StepKind,
    /// 0-based position among the job's declared workflow steps. `Some`
    /// exactly when `kind` is `Workflow`; this is what `--step N` indexes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_index: Option<usize>,
    /// The runner's own 1-based timeline position, which counts synthetic
    /// steps too — the golden capture reports declared step 1 as `number: 2`
    /// because "Set up job" takes 1. Presentation and protocol fidelity only;
    /// never an input to `--step`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_number: Option<u32>,
    /// Expression-context key (`compile`, `__run_2`). Unlike `id`, this is
    /// derived from the YAML and so is stable across runs of the same
    /// workflow, which is what lets one step be correlated over time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_name: Option<String>,
    pub name: String,
    pub conclusion: String,
    /// Server-side observation of when the step first appeared (started) and
    /// when it turned terminal (finished). Stamped at projection time, so
    /// durations are authoritative even when the runner omits wire
    /// timestamps (preloop-runner) or when a worker dies mid-step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl StepRecord {
    /// A manifest entry for a declared workflow step, built from the job
    /// request message before the runner has reported anything.
    pub fn workflow(
        id: String,
        workflow_index: usize,
        name: String,
        context_name: Option<String>,
    ) -> Self {
        Self {
            id,
            kind: StepKind::Workflow,
            workflow_index: Some(workflow_index),
            runner_number: None,
            context_name,
            name,
            conclusion: "pending".to_owned(),
            started_at: None,
            finished_at: None,
        }
    }

    /// Build one job attempt's manifest from its request message's steps.
    ///
    /// The message's `steps` vector *is* the declared workflow order, so the
    /// index is the order and `TaskStep.id` is the identity. Nothing here
    /// inspects the filesystem or sorts ids: a v4 UUID sorts randomly, and an
    /// upload timestamp records when a blob landed, not when a step ran.
    pub fn manifest(steps: &[azdo::TaskStep]) -> Vec<Self> {
        steps
            .iter()
            .enumerate()
            .map(|(index, step)| {
                Self::workflow(
                    step.id.to_string(),
                    index,
                    step.display_name
                        .clone()
                        .or_else(|| step.name.clone())
                        .unwrap_or_default(),
                    step.context_name.clone(),
                )
            })
            .collect()
    }

    /// Sort key placing a step in execution order.
    ///
    /// Three tiers, because the two reporting paths carry different evidence:
    ///
    /// 1. `runner_number` — the runner's own 1-based timeline position, which
    ///    counts every step it ran. The broker path reports it, and it is the
    ///    truth once present.
    /// 2. `started_at` — when the server first saw the step run. The AzDO
    ///    timeline path carries no ordinal (`TimelineRecord` has no `order`
    ///    field), so a synthetic step there has no number; without this tier
    ///    `Set up job` sorted after every declared step on that path, which is
    ///    the defect this ordering exists to prevent.
    /// 3. `workflow_index` — a declared step that has not run yet, ordered as
    ///    the workflow declares and placed after everything that has run.
    ///
    /// A step id is a v4 UUID and sorts randomly, so it is only ever the final
    /// tie-break for determinism.
    fn execution_key(&self) -> (u8, i64, usize, &str) {
        match (self.runner_number, self.started_at) {
            (Some(number), _) => (0, i64::from(number), 0, self.id.as_str()),
            (None, Some(started_at)) => (
                1,
                started_at.timestamp_micros(),
                self.workflow_index.unwrap_or(usize::MAX),
                self.id.as_str(),
            ),
            (None, None) => (
                2,
                0,
                self.workflow_index.unwrap_or(usize::MAX),
                self.id.as_str(),
            ),
        }
    }

    /// Order records as the job executed them.
    ///
    /// The in-memory manifest is seeded with declared steps and then appends
    /// synthetic ones as the runner reports them, so its raw order puts
    /// `Set up job` after the workflow steps despite it running first. A
    /// restore adds a third order again. Every surface that shows a whole
    /// step list goes through this instead.
    pub fn sort_execution_order(steps: &mut [Self]) {
        steps.sort_by(|left, right| left.execution_key().cmp(&right.execution_key()));
    }

    /// Locate a step by stable identity, and by nothing else.
    ///
    /// Display names repeat legitimately — two steps may both be named `Test`
    /// — so matching on them merged distinct steps and lost one from the run.
    pub fn find_by_id(steps: &[Self], id: &str) -> Option<usize> {
        if id.is_empty() {
            return None;
        }
        steps.iter().position(|step| step.id == id)
    }

    /// The declared workflow steps, in declared order.
    ///
    /// Synthetic steps are excluded, so `Set up job` and `Post <action>` never
    /// shift what `--step N` selects.
    pub fn workflow_steps(steps: &[Self]) -> Vec<&Self> {
        let mut workflow: Vec<&Self> = steps
            .iter()
            .filter(|step| step.kind == StepKind::Workflow)
            .collect();
        workflow.sort_by_key(|step| step.workflow_index.unwrap_or(usize::MAX));
        workflow
    }
}

/// Server-side timing for the workspace snapshot created at submission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotTiming {
    /// Wall time spent capturing the tree, including the git operations.
    pub duration_ms: u64,
    /// Objects (loose + packed) in the snapshot repository.
    pub object_count: u64,
    /// Packed size in bytes (loose objects are negligible after repacking).
    pub pack_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDetail {
    /// Stable workflow job key (`build`, `build (ubuntu-latest)`).
    ///
    /// Separate from `name` because the run projection overwrites `name` with
    /// the evaluated GitHub display name, so `name` cannot identify the job it
    /// belongs to. Empty only for a detail restored from a run written before
    /// this field existed.
    #[serde(default)]
    pub job_id: String,
    /// GitHub display name, as shown in a run's job list.
    pub name: String,
    pub conclusion: String,
    pub steps: Vec<StepRecord>,
    /// Job-level annotations reported by the runner (worker-crash detail,
    /// infrastructure failures). Kept as raw wire values.
    #[serde(default)]
    pub annotations: Vec<serde_json::Value>,
}

impl JobDetail {
    /// Locate a job's detail by its stable key.
    ///
    /// Falls back to the display name only for details restored without a
    /// `job_id`; new details always carry one.
    pub fn find<'a>(details: &'a mut [Self], job_id: &str) -> Option<&'a mut Self> {
        let index = details
            .iter()
            .position(|detail| detail.job_id == job_id)
            .or_else(|| {
                details
                    .iter()
                    .position(|detail| detail.job_id.is_empty() && detail.name == job_id)
            })?;
        details.get_mut(index)
    }
}

/// Metadata tracked per log file for results-service Twirp retrieval.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LogMetadata {
    /// Total bytes appended so far.
    pub byte_count: usize,
    /// Total lines appended so far.
    pub line_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: RunId,
    /// GitHub delivery ID that created this run, when the run came from the
    /// durable webhook queue. Persisted for at-least-once processing
    /// idempotency, but omitted from run API responses.
    #[serde(default, skip_serializing)]
    pub webhook_delivery_id: Option<String>,
    pub run_name: Option<String>,
    pub submission: Arc<WorkflowSubmission>,
    pub jobs: BTreeMap<JobId, ExecutionStatus>,
    pub status: ExecutionStatus,
    pub job_outputs: BTreeMap<JobId, BTreeMap<String, serde_json::Value>>,
    pub job_base_ids: BTreeMap<JobId, String>,
    #[serde(skip)]
    pub job_needs: BTreeMap<JobId, Vec<JobId>>,
    /// Expanded plans for deferred reusable-caller nodes, consumed by the
    /// scheduler when a caller's `if:` gate passes and its callee subtree is
    /// materialized.
    #[serde(skip)]
    pub caller_plans: BTreeMap<JobId, preloop_gha_protocol::JobPlan>,
    /// GitHub display name per job (evaluated `name:`, ` / ` caller/callee
    /// separator). The run record keys everything by job id; this maps ids to
    /// what GitHub's jobs API would show.
    #[serde(default)]
    pub job_names: BTreeMap<JobId, String>,
    /// GitHub context JSON captured at submission, reused when runtime
    /// expansion builds runner messages for a callee subtree.
    #[serde(skip)]
    pub github: serde_json::Value,
    /// Resolved head SHA / workflow ref captured at submission for runtime
    /// expansion (message context data).
    #[serde(skip)]
    pub head_sha: String,
    #[serde(skip)]
    pub workflow_ref: String,
    /// Immutable workspace snapshot created at submission, when local
    /// checkout redirection is active; runtime-expanded jobs check out the
    /// same tree.
    #[serde(skip)]
    pub workspace_snapshot: Option<crate::snapshots::WorkspaceSnapshot>,
    pub job_fail_fast: BTreeMap<String, bool>,
    #[serde(default)]
    pub job_continue_on_error: BTreeMap<String, bool>,
    #[serde(default)]
    pub job_check_run_ids: BTreeMap<JobId, u64>,
    #[serde(default)]
    pub reusable_calls: BTreeMap<String, preloop_gha_parser::ReusableCallMetadata>,
    #[serde(default)]
    pub jobs_list: Vec<JobDetail>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub run_number: u64,
    pub run_attempt: u64,
    pub workflow_path_str: String,
    pub event: String,
    pub conclusion: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push_state: Option<PushState>,
    /// Submission-time workspace snapshot cost; present only for local
    /// submissions that snapshot a workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_timing: Option<SnapshotTiming>,
}

#[derive(Debug, Clone)]
pub struct TaskAgentJobRequestRecord {
    pub request_id: i64,
    pub run_id: RunId,
    pub job_id: JobId,
    pub agent_job_id: uuid::Uuid,
    pub plan_id: String,
    pub plan_type: String,
    pub timeline_id: uuid::Uuid,
    pub result: Option<ExecutionStatus>,
    pub locked_until: String,
    /// When a runner removed this job from the ready queue.
    pub claimed_at: Option<std::time::SystemTime>,
    /// Runner identity that claimed this request. Kept after completion so
    /// late AgentRequest reads and retries remain bound to the original owner.
    pub owner_runner_id: Option<i64>,
    /// When the runner request was handed to the session.
    pub started_at: Option<std::time::SystemTime>,
    pub last_renewed_at: Option<std::time::SystemTime>,
    pub timeout_triggered: bool,
    /// Whether this job request has already spent its one debug-worker token
    /// exchange.
    ///
    /// The exchange authenticates with the job runtime token, which the runner
    /// also exports to steps as `ACTIONS_RUNTIME_TOKEN`. A worker acquires the
    /// credential during job setup, before the first step runs, so consuming
    /// the exchange closes the window in which workflow code could replay that
    /// token to mint a debug credential of its own.
    pub debug_token_issued: bool,
}

/// A job → runner pairing recorded when the pool provisions a machine for a
/// job, or when a queued job is bound to an idle registered runner.
///
/// While an assignment is fresh, only sessions bearing a verified identity of
/// `runner_id` may claim the job — this is what keeps a compromised runner
/// (or any other code running inside a pool machine) from pulling a job that
/// belongs to a different machine or tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentRecord {
    pub runner_id: Option<i64>,
    pub at: std::time::SystemTime,
    /// When this job was *first* bound to any machine. Rebinding to a
    /// replacement machine refreshes `at` but never this, so a pool that
    /// keeps provisioning and losing machines cannot hold a job away from
    /// healthy runners indefinitely.
    pub first_at: std::time::SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedJob {
    pub run_id: RunId,
    pub job_id: JobId,
    pub base_id: String,
    /// Unix nanoseconds when this job was created.
    #[serde(default)]
    pub created_at_unix_nanos: i64,
    /// Unix nanoseconds when all `needs:` dependencies became satisfied.
    #[serde(default)]
    pub dependencies_ready_at_unix_nanos: Option<i64>,
    /// Unix nanoseconds when this job first waited on a concurrency gate.
    #[serde(default)]
    pub concurrency_wait_started_at_unix_nanos: Option<i64>,
    /// Unix nanoseconds when the first applicable concurrency gate admitted
    /// this job.
    #[serde(default)]
    pub concurrency_acquired_at_unix_nanos: Option<i64>,
    /// Unix nanoseconds when the job entered the ready queue. This is the
    /// runner-wait clock, not the workflow or dependency creation time.
    /// `0` is used only before the job first becomes ready.
    #[serde(default)]
    pub enqueued_at_unix_nanos: i64,
    pub needs: Vec<JobId>,
    pub if_condition: Option<String>,
    pub condition_context: preloop_gha_expressions::Context,
    pub max_parallel: Option<u64>,
    /// Required runner labels from `runs-on`.
    pub runs_on: Vec<String>,
    /// Explicit runner group from object-valued `runs-on`.
    pub runner_group: Option<String>,
    pub message: azdo::AgentJobRequestMessage,
    /// Original `environment:` value, retained until `needs` is hydrated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<serde_json::Value>,
    /// Raw job-level concurrency (evaluated when the job becomes ready).
    pub concurrency: Option<preloop_gha_parser::Concurrency>,
    /// Matrix values for this expansion (for concurrency expression eval).
    pub matrix: BTreeMap<String, serde_json::Value>,
    /// Deferred expression for runtime dynamic matrix expansion, if any.
    pub deferred_matrix: Option<String>,
    /// Deferred reusable-workflow invocation, expanded on gate pass. This
    /// node is scheduling-only: it never reaches `inner.queue`.
    pub reusable_call: Option<preloop_gha_protocol::ReusableCallPlan>,
    /// Environment protection gate state, armed when the job first reaches
    /// scheduler admission with `[environment_rules]` configured for its
    /// `environment:`. Stamps the wait-timer deadline, the approval request
    /// time, and recorded approvals. Survives the persisted job snapshot so
    /// a restart cannot silently drop an armed gate (fail closed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment_gate: Option<EnvironmentGateState>,
}

/// Progress markers for one job's environment protection gates. All
/// timestamps are unix nanoseconds. The stamps travel in the persisted job
/// snapshot (`payload_blob`) so a restart re-arms an armed wait timer or
/// approval gate instead of dropping it — and every stamp is fail-closed
/// under snapshot loss: a lost wait deadline re-arms the wait, a lost
/// approval re-arms the approval, never the reverse.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EnvironmentGateState {
    /// When the wait timer expires. `None` once satisfied or when no wait
    /// timer is configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_until_unix_nanos: Option<i64>,
    /// When the job first entered the required-reviewer gate. `None` when
    /// no approval gate is (or was) armed for this job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_requested_at_unix_nanos: Option<i64>,
    /// Unix-nanos timestamps of recorded approvals, oldest first.
    #[serde(default)]
    pub approvals_unix_nanos: Vec<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitHubTokenRequest {
    pub repository: String,
    pub permissions: BTreeMap<String, String>,
    /// Whether the workflow wrote its own `permissions:` block.
    ///
    /// A declared set must be minted verbatim or fail visibly; the implicit
    /// default may be narrowed to what the App installation actually grants.
    pub declared: bool,
    /// Whether the job's trust tier restricts GitHub authority (fork PR or
    /// fail-closed unknown event). Such jobs carry only the read-only fork
    /// profile, and a mint failure never falls back to the broad
    /// `PRELOOP_GITHUB_TOKEN` PAT: the job keeps the local runtime token
    /// instead of receiving authority GitHub would not grant the fork.
    ///
    /// Missing persisted metadata fails closed: a request written by a
    /// pre-upgrade server has no `untrusted` field, and deserializing it as
    /// trusted would silently re-enable the PAT fallback after a restart for
    /// a job whose tier was never recorded. Newly created requests always
    /// serialize the field explicitly (`false` for trusted jobs), so only
    /// genuinely old state hits the fail-closed default.
    #[serde(default = "default_untrusted")]
    pub untrusted: bool,
}

/// Fail-closed default for persisted [`GitHubTokenRequest`]s that predate
/// the `untrusted` field: no recorded trust metadata means the request may
/// have belonged to an untrusted job, so it is treated as untrusted.
fn default_untrusted() -> bool {
    true
}

/// Runner metadata used by dispatch matching.
#[derive(Debug, Clone, Default)]
pub struct RunnerCapabilities {
    pub known: bool,
    pub labels: Vec<String>,
    pub runner_group_id: Option<i64>,
    pub runner_group_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedCancellation {
    pub run_id: RunId,
    pub job_id: JobId,
    /// Agent job GUID from the job message (`jobId`), required for official JobCancelMessage.
    pub agent_job_id: uuid::Uuid,
}

/// Lifecycle status of a durable GitHub webhook delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookDeliveryStatus {
    Received,
    Processing,
    Done,
    Failed,
}

impl WebhookDeliveryStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Processing => "processing",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "received" => Some(Self::Received),
            "processing" => Some(Self::Processing),
            "done" => Some(Self::Done),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// A durably queued webhook delivery record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookDeliveryRecord {
    pub delivery_id: String,
    pub event: String,
    pub payload: Vec<u8>,
    pub received_at_us: i64,
    pub state: WebhookDeliveryStatus,
    pub attempts: u32,
    pub lease_until_us: Option<i64>,
    /// Fencing token for the current processing lease. A stale worker cannot
    /// renew or finalize a lease that another worker has reclaimed.
    #[serde(default)]
    pub lease_token: Option<String>,
    pub last_error: Option<String>,
}

/// A webhook delivery row without its payload — the operator listing surface.
///
/// The payload is the one field that can reach 25 MiB, and no listing needs
/// it; keeping it out of this type means a list endpoint cannot accidentally
/// stream the whole queue's bodies (or their decrypted secrets) to a client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookDeliverySummary {
    pub delivery_id: String,
    pub event: String,
    pub received_at_us: i64,
    pub state: WebhookDeliveryStatus,
    pub attempts: u32,
    pub lease_until_us: Option<i64>,
    pub last_error: Option<String>,
}

/// Aggregate queue health, read by the status snapshot and the health API.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookQueueStats {
    pub received: u64,
    pub processing: u64,
    pub done: u64,
    pub failed: u64,
    /// Receipt time of the oldest row still awaiting a terminal state. A
    /// growing age here is the only signal that separates "queue is quiet"
    /// from "queue is stuck".
    pub oldest_pending_received_at_us: Option<i64>,
}

/// Persisted high-water mark for one GitHub App's delivery-history poll.
///
/// GitHub keeps delivery history for three days and never resends on its
/// own, so the watchdog's cursor is the difference between repairing a lost
/// delivery and never learning it existed. It is persisted per App id: a
/// restart must not rewind (duplicate work) or skip forward (silent gap).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookWatchdogCursor {
    /// GitHub App id the cursor belongs to.
    pub scope: String,
    /// Newest `(delivered_at, guid)` the watchdog has fully examined. Never
    /// advanced past the grace window, and never advanced on a failed poll.
    #[serde(default)]
    pub cursor_delivered_at_us: Option<i64>,
    /// GUID tie-breaker for deliveries sharing the same timestamp.
    #[serde(default)]
    pub cursor_delivered_at_guid: Option<String>,
    /// Opaque GitHub pagination cursor to resume when a bounded pass did not
    /// reach the watermark.
    #[serde(default)]
    pub scan_cursor: Option<String>,
    /// When a poll was last attempted, successful or not.
    pub last_poll_at_us: Option<i64>,
    /// When a poll last completed without error. Staleness here is an alert:
    /// a blind watchdog looks exactly like a quiet one.
    pub last_success_at_us: Option<i64>,
}

/// Why the watchdog wants a delivery replayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookRepairReason {
    /// GitHub recorded a non-2xx (or no) response: the delivery never landed.
    RemoteFailure,
    /// GitHub recorded success but no local row exists — the phantom ack.
    PhantomAck,
}

impl WebhookRepairReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RemoteFailure => "remote_failure",
            Self::PhantomAck => "phantom_ack",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "remote_failure" => Some(Self::RemoteFailure),
            "phantom_ack" => Some(Self::PhantomAck),
            _ => None,
        }
    }
}

/// One remote delivery the watchdog is repairing, and how hard it has tried.
///
/// Keyed by the GitHub delivery GUID — the same key the ingress deduplicates
/// on — so a redelivery that finally lands is recognised as the repair of
/// this row rather than as new work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookRedeliveryRecord {
    pub delivery_guid: String,
    /// Numeric delivery id: `POST /app/hook/deliveries/{id}/attempts` takes
    /// this, not the GUID.
    pub github_delivery_id: i64,
    /// App id that owns the delivery, so a multi-App deployment redelivers
    /// with the right JWT.
    pub app_id: String,
    pub reason: WebhookRepairReason,
    pub attempts: u32,
    pub first_seen_at_us: i64,
    pub last_attempt_at_us: Option<i64>,
    /// Set once the delivery is present locally; a resolved row is history,
    /// not backlog.
    pub resolved_at_us: Option<i64>,
    pub last_error: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingCache {
    pub key: String,
    pub version: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub job_backend_id: String,
    pub bytes: Vec<u8>,
    /// R1-6 — unix seconds the reservation was made. The TTL sweeper frees
    /// abandoned reservations; only `cache_commit` removed them before, so a
    /// job that never commits leaked the in-memory bytes forever.
    #[serde(default)]
    pub created_unix: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRecord {
    pub id: String,
    pub run_id: RunId,
    pub name: String,
    pub file_name: String,
    pub path: String,
    pub size: u64,
}

/// Pending cache v2 upload (Twirp CacheService).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheV2Pending {
    pub key: String,
    pub version: String,
    /// Job backend id that reserved the upload, derived from the runtime
    /// token scope. `#[serde(default)]` keeps old persisted metas restoring
    /// (an empty value means the entry predates per-job accounting and is
    /// never billed to any job).
    #[serde(default)]
    pub job_backend_id: String,
    /// Unix seconds the reservation was made; `0` for restored entries so
    /// the TTL sweeper leaves them alone.
    #[serde(default)]
    pub created_unix: i64,
}

/// Pending artifact v2 upload (Twirp ArtifactService).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactV2Pending {
    /// Registry key = "{run_backend_id}/{job_backend_id}/{name}".
    pub registry_key: String,
    /// Job backend id that reserved the upload, derived from the runtime
    /// token scope. `#[serde(default)]` keeps old persisted metas restoring.
    #[serde(default)]
    pub job_backend_id: String,
    /// Unix seconds the reservation was made; `0` for restored entries so
    /// the TTL sweeper leaves them alone.
    #[serde(default)]
    pub created_unix: i64,
}

/// Reserved diagnostic-log upload (Twirp `GetJobDiagLogsSignedBlobURL`).
///
/// The diag blob token is minted per call and handed to the runner, which
/// PUTs to `/twirp-blob/diag/{token}` bearerless (Azure SDK compat). The
/// registry binds the token to the owning job so the blob gate can reject
/// writes from any other job. In-memory only: diag uploads happen
/// immediately after minting, and the TTL sweeper bounds the map.
#[derive(Debug, Clone)]
pub struct DiagUploadToken {
    /// Job backend id that reserved the upload (job UUID string), derived
    /// from the runtime token scope; empty when minted by the system token.
    pub job_id: String,
    /// Unix seconds the reservation was made.
    pub created_unix: i64,
}

/// Finalized artifact v2 entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactV2Entry {
    pub id: u64,
    pub workflow_run_backend_id: String,
    pub workflow_job_run_backend_id: String,
    pub name: String,
    pub size: u64,
    pub created_at: String,
    pub digest: Option<String>,
    /// Upload token used to find the assembled blob on disk.
    pub blob_token: String,
}

/// Unix nanoseconds now, for queue-latency bookkeeping. `i64` keeps the
/// field serde-friendly (it travels in persisted job snapshots).
pub fn now_unix_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}
