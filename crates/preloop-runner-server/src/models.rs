use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum PushStatus {
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
pub(crate) struct PushState {
    pub(crate) status: PushStatus,
    pub(crate) error: Option<String>,
    /// Pull request number, when the branch has an open PR (created or
    /// pre-existing).
    pub(crate) pr_number: Option<u64>,
    /// The commit the sync actually published (`submission.sha` for a clean
    /// submission; the materialized branch head for a dirty one). Webhook
    /// dedup matches the echo of our own push against this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) effective_sha: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct DapPortRegistration {
    pub(crate) port: u16,
    pub(crate) job_id: JobId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StepRecord {
    pub(crate) name: String,
    pub(crate) conclusion: String,
    /// Server-side observation of when the step first appeared (started) and
    /// when it turned terminal (finished). Stamped at projection time, so
    /// durations are authoritative even when the runner omits wire
    /// timestamps (preloop-runner) or when a worker dies mid-step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) started_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) finished_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Server-side timing for the workspace snapshot created at submission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SnapshotTiming {
    /// Wall time spent capturing the tree, including the git operations.
    pub(crate) duration_ms: u64,
    /// Objects (loose + packed) in the snapshot repository.
    pub(crate) object_count: u64,
    /// Packed size in bytes (loose objects are negligible after repacking).
    pub(crate) pack_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JobDetail {
    pub(crate) name: String,
    pub(crate) conclusion: String,
    pub(crate) steps: Vec<StepRecord>,
    /// Job-level annotations reported by the runner (worker-crash detail,
    /// infrastructure failures). Kept as raw wire values.
    #[serde(default)]
    pub(crate) annotations: Vec<serde_json::Value>,
}

/// Metadata tracked per log file for results-service Twirp retrieval.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct LogMetadata {
    /// Total bytes appended so far.
    pub(crate) byte_count: usize,
    /// Total lines appended so far.
    pub(crate) line_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RunRecord {
    pub(crate) run_id: RunId,
    pub(crate) run_name: Option<String>,
    pub(crate) submission: Arc<WorkflowSubmission>,
    pub(crate) jobs: BTreeMap<JobId, ExecutionStatus>,
    pub(crate) status: ExecutionStatus,
    pub(crate) job_outputs: BTreeMap<JobId, BTreeMap<String, serde_json::Value>>,
    pub(crate) job_base_ids: BTreeMap<JobId, String>,
    #[serde(skip)]
    pub(crate) job_needs: BTreeMap<JobId, Vec<JobId>>,
    /// Expanded plans for deferred reusable-caller nodes, consumed by the
    /// scheduler when a caller's `if:` gate passes and its callee subtree is
    /// materialized.
    #[serde(skip)]
    pub(crate) caller_plans: BTreeMap<JobId, preloop_gha_protocol::JobPlan>,
    /// GitHub display name per job (evaluated `name:`, ` / ` caller/callee
    /// separator). The run record keys everything by job id; this maps ids to
    /// what GitHub's jobs API would show.
    #[serde(default)]
    pub(crate) job_names: BTreeMap<JobId, String>,
    /// GitHub context JSON captured at submission, reused when runtime
    /// expansion builds runner messages for a callee subtree.
    #[serde(skip)]
    pub(crate) github: serde_json::Value,
    /// Resolved head SHA / workflow ref captured at submission for runtime
    /// expansion (message context data).
    #[serde(skip)]
    pub(crate) head_sha: String,
    #[serde(skip)]
    pub(crate) workflow_ref: String,
    /// Immutable workspace snapshot created at submission, when local
    /// checkout redirection is active; runtime-expanded jobs check out the
    /// same tree.
    #[serde(skip)]
    pub(crate) workspace_snapshot: Option<crate::snapshots::WorkspaceSnapshot>,
    pub(crate) job_fail_fast: BTreeMap<String, bool>,
    #[serde(default)]
    pub(crate) job_continue_on_error: BTreeMap<String, bool>,
    #[serde(default)]
    pub(crate) job_check_run_ids: BTreeMap<JobId, u64>,
    #[serde(default)]
    pub(crate) reusable_calls: BTreeMap<String, preloop_gha_parser::ReusableCallMetadata>,
    #[serde(default)]
    pub(crate) jobs_list: Vec<JobDetail>,
    pub(crate) created_at: chrono::DateTime<chrono::Utc>,
    pub(crate) started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) run_number: u64,
    pub(crate) run_attempt: u64,
    pub(crate) workflow_path_str: String,
    pub(crate) event: String,
    pub(crate) conclusion: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) push_state: Option<PushState>,
    /// Submission-time workspace snapshot cost; present only for local
    /// submissions that snapshot a workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) snapshot_timing: Option<SnapshotTiming>,
}

#[derive(Debug, Clone)]
pub(crate) struct TaskAgentJobRequestRecord {
    pub(crate) request_id: i64,
    pub(crate) run_id: RunId,
    pub(crate) job_id: JobId,
    pub(crate) agent_job_id: uuid::Uuid,
    pub(crate) plan_id: String,
    pub(crate) plan_type: String,
    pub(crate) timeline_id: uuid::Uuid,
    pub(crate) result: Option<ExecutionStatus>,
    pub(crate) locked_until: String,
    pub(crate) started_at: Option<std::time::SystemTime>,
    pub(crate) last_renewed_at: Option<std::time::SystemTime>,
    pub(crate) timeout_triggered: bool,
    /// Whether this job request has already spent its one debug-worker token
    /// exchange.
    ///
    /// The exchange authenticates with the job runtime token, which the runner
    /// also exports to steps as `ACTIONS_RUNTIME_TOKEN`. A worker acquires the
    /// credential during job setup, before the first step runs, so consuming
    /// the exchange closes the window in which workflow code could replay that
    /// token to mint a debug credential of its own.
    pub(crate) debug_token_issued: bool,
}

/// A job → runner pairing recorded when the pool provisions a machine for a
/// job, or when a queued job is bound to an idle registered runner.
///
/// While an assignment is fresh, only sessions bearing a verified identity of
/// `runner_id` may claim the job — this is what keeps a compromised runner
/// (or any other code running inside a pool machine) from pulling a job that
/// belongs to a different machine or tenant.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AssignmentRecord {
    pub(crate) runner_id: i64,
    pub(crate) at: std::time::SystemTime,
    /// When this job was *first* bound to any machine. Rebinding to a
    /// replacement machine refreshes `at` but never this, so a pool that
    /// keeps provisioning and losing machines cannot hold a job away from
    /// healthy runners indefinitely.
    pub(crate) first_at: std::time::SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueuedJob {
    pub(crate) run_id: RunId,
    pub(crate) job_id: JobId,
    pub(crate) base_id: String,
    pub(crate) needs: Vec<JobId>,
    pub(crate) if_condition: Option<String>,
    pub(crate) condition_context: preloop_gha_expressions::Context,
    pub(crate) max_parallel: Option<u64>,
    /// Required runner labels from `runs-on`.
    pub(crate) runs_on: Vec<String>,
    /// Explicit runner group from object-valued `runs-on`.
    pub(crate) runner_group: Option<String>,
    pub(crate) message: azdo::AgentJobRequestMessage,
    /// Raw job-level concurrency (evaluated when the job becomes ready).
    pub(crate) concurrency: Option<preloop_gha_parser::Concurrency>,
    /// Matrix values for this expansion (for concurrency expression eval).
    pub(crate) matrix: BTreeMap<String, serde_json::Value>,
    /// Deferred expression for runtime dynamic matrix expansion, if any.
    pub(crate) deferred_matrix: Option<String>,
    /// Deferred reusable-workflow invocation, expanded on gate pass. This
    /// node is scheduling-only: it never reaches `inner.queue`.
    pub(crate) reusable_call: Option<preloop_gha_protocol::ReusableCallPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GitHubTokenRequest {
    pub(crate) repository: String,
    pub(crate) permissions: BTreeMap<String, String>,
    /// Whether the workflow wrote its own `permissions:` block.
    ///
    /// A declared set must be minted verbatim or fail visibly; the implicit
    /// default may be narrowed to what the App installation actually grants.
    pub(crate) declared: bool,
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
    pub(crate) untrusted: bool,
}

/// Fail-closed default for persisted [`GitHubTokenRequest`]s that predate
/// the `untrusted` field: no recorded trust metadata means the request may
/// have belonged to an untrusted job, so it is treated as untrusted.
fn default_untrusted() -> bool {
    true
}

/// Runner metadata used by dispatch matching.
#[derive(Debug, Clone, Default)]
pub(crate) struct RunnerCapabilities {
    pub(crate) known: bool,
    pub(crate) labels: Vec<String>,
    pub(crate) runner_group_id: Option<i64>,
    pub(crate) runner_group_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueuedCancellation {
    pub(crate) run_id: RunId,
    pub(crate) job_id: JobId,
    /// Agent job GUID from the job message (`jobId`), required for official JobCancelMessage.
    pub(crate) agent_job_id: uuid::Uuid,
}

/// Lifecycle of a GitHub webhook delivery ID used for dedup.
#[derive(Debug, Clone, Copy)]
pub(crate) enum WebhookDeliveryState {
    /// The handler is still processing this delivery; a concurrent copy of the
    /// same delivery must be skipped so one delivery yields one run.
    InFlight,
    /// Processing finished successfully at this instant; redeliveries inside
    /// the dedup window are skipped. Failed deliveries are not recorded at all
    /// so GitHub's retry is accepted.
    Completed(std::time::Instant),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PendingCache {
    pub(crate) key: String,
    pub(crate) version: String,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ArtifactRecord {
    pub(crate) id: String,
    pub(crate) run_id: RunId,
    pub(crate) name: String,
    pub(crate) file_name: String,
    pub(crate) path: String,
    pub(crate) size: u64,
}

/// Pending cache v2 upload (Twirp CacheService).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CacheV2Pending {
    pub(crate) key: String,
    pub(crate) version: String,
}

/// Pending artifact v2 upload (Twirp ArtifactService).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ArtifactV2Pending {
    /// Registry key = "{run_backend_id}/{job_backend_id}/{name}".
    pub(crate) registry_key: String,
}

/// Finalized artifact v2 entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ArtifactV2Entry {
    pub(crate) id: u64,
    pub(crate) workflow_run_backend_id: String,
    pub(crate) workflow_job_run_backend_id: String,
    pub(crate) name: String,
    pub(crate) size: u64,
    pub(crate) created_at: String,
    pub(crate) digest: Option<String>,
    /// Upload token used to find the assembled blob on disk.
    pub(crate) blob_token: String,
}
