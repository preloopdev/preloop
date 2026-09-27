//! The `ControlBackend` contract: every durable control mutation is one typed
//! command with transactional authority, idempotency and fencing.
//!
//! This is the only surface the server sees for durable control state.
//! Handlers parse requests and map domain results onto wire responses; they
//! never touch a transaction handle, a SQL string, or a mutable record. A
//! backend runs each command as one transaction: load the working set, run
//! the shared scheduling logic ([`crate::control::sched`]), write the delta
//! back, return the domain result.
//!
//! SQLite is the default backend (single writer, `BEGIN IMMEDIATE`, WAL).
//! Postgres implements the identical contract for shared-node deployments
//! (a pool of connections, `SELECT … FOR UPDATE SKIP LOCKED` for leases).
//! Both produce the same domain results from the same inputs — the shared
//! behavioral suite in `control::tests` proves it.

use super::sched::{BuiltExpansion, SchedulingOutcome};
use super::types::*;
use crate::models::{
    JobDetail, PushState, QueuedJob, RunRecord, StepRecord, TaskAgentJobRequestRecord,
    WebhookDeliveryRecord, WebhookDeliveryStatus, WebhookDeliverySummary, WebhookQueueStats,
    WebhookRedeliveryRecord, WebhookWatchdogCursor,
};
use preloop_gha_protocol::azdo;
use preloop_gha_protocol::{ExecutionStatus, JobId, NdjsonEvent, RunId};
use std::collections::{BTreeMap, BTreeSet};

/// Indexed run-list query. Backends apply filtering, ordering and limiting in
/// SQL; handlers never deserialize the complete control state to list runs.
#[derive(Debug, Clone, Default)]
pub(crate) struct RunListFilter {
    pub(crate) workflow: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) event: Option<String>,
    pub(crate) limit: usize,
}

/// Extract the optional run identity carried by a durable event.
pub(crate) fn event_run_id(event: &NdjsonEvent) -> Option<RunId> {
    match event {
        NdjsonEvent::RunAccepted { run_id, .. }
        | NdjsonEvent::JobStatus { run_id, .. }
        | NdjsonEvent::RunStatus { run_id, .. }
        | NdjsonEvent::JobCompleted { run_id, .. }
        | NdjsonEvent::CheckRunCreated { run_id } => Some(*run_id),
        _ => None,
    }
}

/// Project database rows into the public run shape without a `TxState` load.
pub(crate) fn project_run_rows(
    mut run: RunRecord,
    jobs: Vec<(JobId, ExecutionStatus, String, Option<Vec<StepRecord>>)>,
) -> RunRecord {
    let expanded_callers: std::collections::BTreeSet<String> = run
        .reusable_calls
        .iter()
        .filter(|(_, call)| !call.inner_job_ids.is_empty())
        .map(|(caller_id, _)| caller_id.clone())
        .collect();
    let existing = std::mem::take(&mut run.jobs_list);
    run.jobs.clear();
    run.jobs_list.clear();
    let mut held = false;
    for (job_id, status, queue_kind, steps) in jobs {
        if expanded_callers.contains(&job_id.0) {
            continue;
        }
        held |= queue_kind == "held";
        run.jobs.insert(job_id.clone(), status);
        let name = run
            .job_names
            .get(&job_id)
            .cloned()
            .unwrap_or_else(|| job_id.0.clone());
        let mut detail = existing
            .iter()
            .find(|detail| detail.job_id == job_id.0)
            .cloned()
            .unwrap_or(JobDetail {
                job_id: job_id.0.clone(),
                name: name.clone(),
                conclusion: crate::status_string(status),
                steps: Vec::new(),
                annotations: Vec::new(),
            });
        detail.job_id = job_id.0.clone();
        detail.name = name;
        detail.conclusion = crate::status_string(status);
        if let Some(steps) = steps {
            detail.steps = steps;
        }
        run.jobs_list.push(detail);
    }
    if run.status == ExecutionStatus::InProgress
        && !run
            .jobs
            .values()
            .any(|status| *status == ExecutionStatus::InProgress)
    {
        run.status = if held {
            ExecutionStatus::Pending
        } else {
            ExecutionStatus::Queued
        };
    }
    run
}

/// The control-plane authority. Object-safe so `AppState` holds
/// `Arc<dyn ControlBackend>` and the backend is swappable by configuration.
///
/// Every method is one transaction. `&self` (not `&mut`) because the
/// serialization lives in the database, not in the object — a Postgres
/// backend serves concurrent commands from a pool; a SQLite backend
/// serializes writers on the single connection.
#[async_trait::async_trait]
pub(crate) trait ControlBackend: Send + Sync {
    // ── Run lifecycle ─────────────────────────────────────────────────

    /// Insert a submitted run and its jobs, evaluating workflow/job/jobset
    /// concurrency gates inside the transaction. Idempotent on the run's
    /// dedup key (webhook delivery / request identity): a replay returns
    /// `SubmitOutcome::existing` instead of a second run.
    async fn submit_run(&self, submit: SubmitRun) -> Result<SubmitOutcome, ControlError>;

    /// Allocate the next run number for a workflow path from the durable
    /// counter. Called before the job messages are built (the number is
    /// embedded in `github.run_number`), so it is its own transaction — a
    /// crash between this and `submit_run` burns a number, which is
    /// acceptable (run numbers may have gaps).
    async fn allocate_run_number(&self, workflow_path: &str) -> Result<u64, ControlError>;

    /// Claim the next dispatchable job for a session. Marks the session
    /// seen, picks a ready job the runner may claim (capability + binding
    /// rules), mints its request/message correlation, and returns the
    /// claim — or a queued cancellation, or `PollOutcome::Empty`.
    async fn poll_session(&self, poll: PollRequest) -> Result<PollOutcome, ControlError>;

    /// Everything `acquirejob` needs in one read: the request record, its
    /// job message, the id-token grant and the deferred token-mint request.
    async fn acquire_context(&self, request_id: i64) -> Result<AcquireContext, ControlError>;

    /// `acquire_context` plus the broker checks: the runner must own the
    /// request (immutable owner, or the owner session's runner — an
    /// assigned-but-unowned request still acquires, for session-less replay)
    /// and the attempt must not be settled.
    async fn acquire_for_runner(
        &self,
        request_id: i64,
        runner_id: i64,
    ) -> Result<AcquireContext, ControlError>;

    /// Persist the agent-job message minted at claim for `request_id`
    /// (`None` leaves `request_blob`/`job_timeout_s` untouched), and upsert
    /// `token_request` when supplied. Runs under the run's advisory lock — a
    /// concurrent scoped write-back that snapshots the same `request_id`
    /// would otherwise clobber the row.
    async fn store_request_message(
        &self,
        run_id: RunId,
        request_id: i64,
        message: Option<&azdo::AgentJobRequestMessage>,
        token_request: Option<&crate::models::GitHubTokenRequest>,
    ) -> Result<(), ControlError>;

    /// Record a runner's job completion: flip the job (and run) status,
    /// settle the request, release concurrency, promote newly-unblocked
    /// dependents, and return the post-transition run plus the effects the
    /// handler must fan out (events, check-run updates, live-log close).
    async fn complete_job(
        &self,
        completion: JobCompletionInput,
    ) -> Result<CompleteOutcome, ControlError>;

    /// Cancel a run or one job: mark non-terminal jobs cancelled, enqueue
    /// cancellation messages for in-flight jobs, release concurrency, and
    /// settle expandable-node request records.
    async fn cancel_run(
        &self,
        run_id: RunId,
        reason: Option<String>,
    ) -> Result<CancelOutcome, ControlError>;
    async fn cancel_job(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<CancelOutcome, ControlError>;

    /// Renew a claimed request's lease. Returns the record on success;
    /// `Stale` if the runner no longer owns it, `NotFound` if unknown.
    async fn renew_request(
        &self,
        request_id: i64,
        runner_id: i64,
    ) -> Result<TaskAgentJobRequestRecord, ControlError>;

    /// Release an interrupted claim so the request can be redelivered
    /// (runner disconnect / retry). Keeps inflight + token records, drops
    /// the dead owner.
    async fn release_request(&self, request_id: i64) -> Result<(), ControlError>;

    // ── Runner and session lifecycle ──────────────────────────────────

    /// Register or refresh a runner. Dedup on `client_id`: a re-register
    /// returns the existing runner id. Pairs the runner with a pending job
    /// it can serve when pool/strict assignment is enabled.
    async fn register_runner(&self, reg: RegisterRunner) -> Result<RunnerRow, ControlError>;

    /// Open a session for a runner (broker or AzDO protocol). Returns the
    /// session id and the sealed crypto material the handler wraps for the
    /// wire.
    async fn create_session(&self, session: CreateSession) -> Result<SessionRow, ControlError>;

    /// Close a session: drop it, release its active request for retry, and
    /// requeue or fail its claimed job per the disconnect rules.
    async fn delete_session(&self, session_id: &str) -> Result<(), ControlError>;

    /// Remove a runner identity and every session/binding it owned;
    /// requeue its claimed jobs so a replacement can pick them up.
    async fn purge_runner(&self, runner_id: i64) -> Result<(), ControlError>;

    // ── Expansion ─────────────────────────────────────────────────────

    /// Lease the next pending expansion node: pop it, stamp an
    /// `expand_generation` fence, and snapshot the inputs the build needs.
    /// `None` when the expansion queue is empty.
    async fn claim_expansion(&self) -> Result<Option<ExpansionClaim>, ControlError>;

    /// Fold a built subtree back into the run under the claim's generation
    /// fence, then promote whatever the subtree unblocked. A stale
    /// generation (the node was cancelled/re-leased mid-build) discards the
    /// build.
    async fn apply_expansion(
        &self,
        claim: ExpansionApply,
    ) -> Result<SchedulingOutcome, ControlError>;

    // ── Reaper / reconcile ────────────────────────────────────────────

    /// Startup reconcile: drop concurrency holders whose runs are terminal
    /// or missing, and recover or fail claims orphaned by the restart.
    async fn reconcile_on_boot(&self) -> Result<ReconcileOutcome, ControlError>;

    // ── Queries (read-only, no transaction needed) ────────────────────

    /// The run record with its job statuses, for status APIs and event
    /// fan-out.
    async fn run_record(&self, run_id: RunId) -> Result<RunRecord, ControlError>;

    /// Runs matching `filter`, ordered and limited by the database.
    async fn list_runs(&self, filter: RunListFilter) -> Result<Vec<RunRecord>, ControlError>;

    /// Terminal logical jobs, used to seed exactly-once lifecycle metrics
    /// after restart without loading complete run records.
    async fn terminal_jobs(
        &self,
    ) -> Result<std::collections::BTreeSet<(RunId, JobId)>, ControlError>;

    /// Move a bounded batch of settled runs from active scheduling tables to
    /// immutable job/attempt history in one transaction per batch.
    async fn archive_finished_runs(&self, limit: usize) -> Result<usize, ControlError>;

    /// Append one durable control event. This never reads or rewrites run
    /// state: the command that produced the event already committed it.
    async fn append_event(&self, event: &NdjsonEvent) -> Result<(), ControlError>;

    /// A request record by id, plan id, agent job id or timeline id.
    async fn request(&self, key: RequestKey) -> Result<TaskAgentJobRequestRecord, ControlError>;
    /// Change only a run's push-sync state. The run advisory lock serializes
    /// this with scheduling transactions that may also rewrite run scalars.
    async fn set_push_state(&self, run_id: RunId, state: PushState) -> Result<(), ControlError>;
    /// Latest live attempt's run for each plan id. Unknown plan ids are
    /// omitted; callers preserve their existing fallback semantics.
    async fn artifact_scopes(
        &self,
        plan_ids: &[String],
    ) -> Result<BTreeMap<String, RunId>, ControlError>;

    /// Queue pressure snapshot for status/metrics.
    async fn queue_stats(&self) -> Result<QueueStats, ControlError>;

    // ── Live log recovery ─────────────────────────────────────────────

    /// Allocate a plan-local log id and create its empty durable row.
    async fn create_log(&self, plan_id: &str) -> Result<i64, ControlError>;

    /// Persist node-local artifact/cache/timeline metadata as one sealed value.
    async fn store_meta(&self, meta: &crate::store::MetaSnapshot) -> Result<(), ControlError>;

    /// Restore node-local metadata after restart.
    async fn load_meta(&self) -> Result<Option<crate::store::MetaSnapshot>, ControlError>;

    /// Record the cluster key fingerprint on first use; afterwards, refuse a
    /// node whose key differs (it would seal rows no other node can read).
    async fn ensure_key_fingerprint(&self, fingerprint: &str) -> Result<(), ControlError>;

    /// Record a session poll and return its protocol: one statement, no
    /// working-set load. `None` when the session does not exist.
    ///
    /// Liveness is minute-granular; a concurrent write-back restoring an
    /// older timestamp is harmless (the next poll bumps it again).
    async fn touch_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionProtocol>, ControlError>;

    /// The runner owning a session and its dispatch capabilities, read
    /// directly (the per-poll lookup). `None` for unknown or runner-less
    /// sessions.
    async fn session_owner(
        &self,
        session_id: &str,
    ) -> Result<Option<(i64, crate::models::RunnerCapabilities)>, ControlError>;

    /// Apply one timeline PATCH: bump the timeline's change counter, stamp
    /// and upsert the patched records, and return the new change id plus
    /// every stored record (ordered by record id). One transaction, shared
    /// by every node.
    async fn patch_timeline(
        &self,
        timeline_key: &str,
        records: Vec<preloop_gha_protocol::azdo::TimelineRecord>,
    ) -> Result<(i32, Vec<preloop_gha_protocol::azdo::TimelineRecord>), ControlError>;

    /// A timeline's change id and records (`skip`/`top` paging).
    async fn get_timeline(
        &self,
        timeline_key: &str,
        skip: usize,
        top: usize,
    ) -> Result<(i32, Vec<preloop_gha_protocol::azdo::TimelineRecord>), ControlError>;

    /// Drop timelines not patched since `before_us`. Returns timelines removed.
    async fn prune_timelines(&self, before_us: i64) -> Result<u64, ControlError>;

    /// One reaper tick's inputs, read directly (no working-set load, no lock).
    async fn reap_inputs(&self) -> Result<ReapInputs, ControlError>;

    /// The logical job an execution attempt belongs to.
    async fn attempt_job(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<(RunId, JobId)>, ControlError>;

    /// Upsert an attempt's runner-reported steps directly (no run lock).
    async fn patch_steps(
        &self,
        agent_job_id: uuid::Uuid,
        patches: Vec<StepPatch>,
    ) -> Result<(), ControlError>;

    /// Whether the run's `jobs_list` has no detail entry for `job_id` yet.
    async fn job_detail_missing(&self, run_id: RunId, job_id: &JobId)
        -> Result<bool, ControlError>;

    /// Create `job_id`'s `jobs_list` detail when missing (defaulting to
    /// `in_progress`) and stamp `conclusion` when it is a terminal string.
    /// Serialized on the run advisory lock; the timeline PATCH calls this
    /// instead of loading the run's working set.
    async fn ensure_job_detail(
        &self,
        run_id: RunId,
        job_id: &JobId,
        conclusion: Option<&str>,
    ) -> Result<(), ControlError>;

    /// Record the GitHub check-run id for a job that is part of the run.
    /// Writes nothing when the job has no `jobs` row (the check mapping is
    /// only meaningful for real jobs). Returns whether the mapping changed.
    /// Serialized on the run advisory lock.
    async fn set_job_check_run(
        &self,
        run_id: RunId,
        job_id: &JobId,
        check_run_id: u64,
    ) -> Result<bool, ControlError>;

    /// Clear the check-run mapping only while it still points at
    /// `expected`. Used when GitHub reports the recorded id stale.
    async fn clear_job_check_run(
        &self,
        run_id: RunId,
        job_id: &JobId,
        expected: u64,
    ) -> Result<(), ControlError>;

    /// Whether the run has a `jobs` row for `job_id` (indexed point read).
    async fn job_exists(&self, run_id: RunId, job_id: &JobId) -> Result<bool, ControlError>;

    /// The job's evaluated display name (`job_names`), if recorded.
    async fn job_display_name(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<String>, ControlError>;

    /// Whether `runner_id` is registered (indexed point read).
    async fn runner_exists(&self, runner_id: i64) -> Result<bool, ControlError>;

    /// The runner registered under OAuth `client_id`, if any.
    async fn runner_for_client(&self, client_id: &str) -> Result<Option<i64>, ControlError>;

    /// The run an execution attempt belongs to.
    async fn run_for_attempt(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<RunId>, ControlError>;

    /// Whether `run_id` takes part in any concurrency group (run- or
    /// job-level, holding or waiting). A run that doesn't can settle a job
    /// under its own run lock: nothing it releases can wake another run.
    async fn run_in_concurrency(&self, run_id: RunId) -> Result<bool, ControlError>;

    /// Resolve a runner callback (plan id, else timeline id) to its attempt:
    /// `(request_id, run_id, job_id, agent_job_id, job status)`, newest
    /// attempt first. One indexed query instead of loading every request.
    async fn callback_job(
        &self,
        plan_id: &str,
        timeline_id: Option<uuid::Uuid>,
    ) -> Result<Option<CallbackJob>, ControlError>;

    /// Renew an attempt's lease with one conditional statement. `Ok(false)`
    /// when the attempt has no recorded owner (callers fall back to the
    /// transactional path); errors distinguish unknown (`NotFound`),
    /// finished (`Conflict`) and foreign (`Forbidden`) attempts.
    async fn renew_lease(
        &self,
        agent_job_id: uuid::Uuid,
        runner_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError>;

    /// `(recorded owner, owner-session runner)` for `request_id`; `None`
    /// when no such request. Drives the AgentRequest ownership check.
    async fn request_owner(
        &self,
        request_id: i64,
    ) -> Result<Option<(Option<i64>, Option<i64>)>, ControlError>;

    /// Renew an in-flight AgentRequest's lease (`locked_until` +
    /// `last_renewed_at`) when `result IS NULL`. Returns whether a row was
    /// renewed; completed or unknown requests silently renew nothing,
    /// matching the PATCH contract.
    async fn renew_agent_request(
        &self,
        request_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError>;

    /// Settle an AgentRequest (`result`, `locked_until`) iff it is still in
    /// flight. `Ok(None)` = already completed (duplicate PATCH); `Ok(Some)`
    /// carries the attempt's `(run_id, job_id, agent_job_id)` for the
    /// completion fan-out.
    async fn settle_agent_request(
        &self,
        request_id: i64,
        result: ExecutionStatus,
        locked_until: &str,
    ) -> Result<Option<(RunId, JobId, uuid::Uuid)>, ControlError>;

    /// Drop one undelivered session message (DELETE-ack); a missing row is
    /// not an error.
    async fn delete_inflight(&self, session_id: &str, message_id: i64) -> Result<(), ControlError>;

    /// `plan_id`s of every in-flight request (live-log/replay-result
    /// pruning after a terminal run).
    async fn active_plan_ids(&self) -> Result<BTreeSet<String>, ControlError>;
    // ── Webhook inbox and repair state ────────────────────────────────

    async fn enqueue_webhook_delivery(
        &self,
        delivery: &WebhookDeliveryRecord,
    ) -> Result<bool, ControlError>;
    async fn claim_webhook_deliveries(
        &self,
        limit: usize,
        lease_duration_secs: u64,
    ) -> Result<Vec<WebhookDeliveryRecord>, ControlError>;
    async fn renew_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        lease_duration_secs: u64,
    ) -> Result<bool, ControlError>;
    async fn complete_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
    ) -> Result<bool, ControlError>;
    async fn fail_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        error: &str,
        permanent: bool,
        retry_delay: Option<std::time::Duration>,
    ) -> Result<bool, ControlError>;
    async fn get_webhook_delivery(
        &self,
        delivery_id: &str,
    ) -> Result<Option<WebhookDeliveryRecord>, ControlError>;
    async fn count_dead_letter_webhook_deliveries(&self) -> Result<u64, ControlError>;
    async fn recover_webhook_deliveries(&self) -> Result<u64, ControlError>;
    async fn prune_webhook_deliveries(
        &self,
        before_us: i64,
        limit: usize,
    ) -> Result<u64, ControlError>;
    async fn park_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        error: &str,
        retry_delay_secs: u64,
    ) -> Result<bool, ControlError>;
    async fn requeue_webhook_delivery(&self, delivery_id: &str) -> Result<bool, ControlError>;
    async fn list_webhook_deliveries(
        &self,
        state: Option<WebhookDeliveryStatus>,
        limit: usize,
    ) -> Result<Vec<WebhookDeliverySummary>, ControlError>;
    async fn webhook_deliveries_present(
        &self,
        delivery_ids: &[String],
    ) -> Result<std::collections::BTreeSet<String>, ControlError>;
    async fn webhook_queue_stats(&self) -> Result<WebhookQueueStats, ControlError>;
    async fn load_webhook_watchdog_cursor(
        &self,
        scope: &str,
    ) -> Result<Option<WebhookWatchdogCursor>, ControlError>;
    async fn store_webhook_watchdog_cursor(
        &self,
        cursor: &WebhookWatchdogCursor,
    ) -> Result<(), ControlError>;
    async fn upsert_webhook_redelivery(
        &self,
        record: &WebhookRedeliveryRecord,
    ) -> Result<(), ControlError>;
    async fn load_webhook_redelivery(
        &self,
        delivery_guid: &str,
    ) -> Result<Option<WebhookRedeliveryRecord>, ControlError>;
    async fn open_webhook_redeliveries(
        &self,
        limit: usize,
    ) -> Result<Vec<WebhookRedeliveryRecord>, ControlError>;
    async fn resolve_webhook_redelivery(
        &self,
        delivery_guid: &str,
        resolved_at_us: i64,
    ) -> Result<bool, ControlError>;

    /// Live runner → job assignments for status reporting.
    async fn live_assignments(
        &self,
    ) -> Result<Vec<preloop_observability::status::RunnerAssignment>, ControlError>;
}

// ─────────────────────────────────────────────────────────────────────────
// Command inputs (owned, backend-neutral)
// ─────────────────────────────────────────────────────────────────────────

/// What a session poll carries: who is asking and what they can run.
#[derive(Debug)]
pub(crate) struct PollRequest {
    pub(crate) session_id: String,
    /// Runner identity proven by the listen token — never the session's
    /// self-declared id.
    pub(crate) verified_runner_id: Option<i64>,
    /// Capabilities resolved for the session's runner.
    pub(crate) runner: crate::models::RunnerCapabilities,
    /// Runner self-reports busy (`status=busy`): deliver inflight/cancel/
    /// active-request outcomes but do NOT claim new work.
    pub(crate) busy: bool,
    /// Long-poll: the caller may hold the request until work appears. The
    /// backend returns `PollOutcome::Empty` immediately; the handler decides
    /// whether to wait on the notify channel.
    pub(crate) wait_ms: u64,
}

/// A runner job completion, normalized from the protocol report.
#[derive(Debug)]
pub(crate) struct JobCompletionInput {
    pub(crate) run_id: RunId,
    pub(crate) job_id: JobId,
    /// The attempt this completion belongs to, when the caller resolved one.
    pub(crate) agent_job_id: Option<uuid::Uuid>,
    pub(crate) status: ExecutionStatus,
    /// Outputs captured by the runner.
    pub(crate) outputs: BTreeMap<String, serde_json::Value>,
    /// Runner that reported (for ownership checks).
    pub(crate) runner_id: Option<i64>,
}

/// Runner registration input.
pub(crate) struct RegisterRunner {
    pub(crate) name: String,
    pub(crate) labels: Vec<String>,
    pub(crate) ephemeral: bool,
    pub(crate) public_key: Option<String>,
    /// Parsed RSA public key, when supplied.
    pub(crate) rsa_public_key: Option<preloop_gha_protocol::crypto::AgentRsaPublicKey>,
    /// Stable client identity for re-registration dedup.
    pub(crate) client_id: Option<String>,
    pub(crate) runner_group_id: Option<i64>,
    pub(crate) runner_group_name: Option<String>,
    /// Registration presented pool proof (provision token / engine bearer).
    pub(crate) pool_proven: bool,
}

/// Session creation input.
pub(crate) struct CreateSession {
    pub(crate) runner_id: i64,
    pub(crate) protocol: SessionProtocol,
    pub(crate) client_id: Option<String>,
    /// Session crypto material, sealed into the row.
    pub(crate) encryption: Option<preloop_gha_protocol::crypto::SessionEncryption>,
}

/// An expansion apply: the claim's identity plus the build result.
pub(crate) struct ExpansionApply {
    pub(crate) job: QueuedJob,
    /// Generation the node was claimed under — the apply is fenced on it.
    pub(crate) generation: i64,
    pub(crate) built: Result<BuiltExpansion, ExecutionStatus>,
}

/// A request lookup by any of its correlation keys.
#[derive(Debug)]
pub(crate) enum RequestKey {
    Id(i64),
    PlanId(String),
    AgentJobId(uuid::Uuid),
    TimelineId(uuid::Uuid),
    /// Latest request for a `(run, job)` pair (highest request_id wins).
    Job(RunId, JobId),
}

/// What boot reconcile recovered.
#[derive(Debug, Default)]
pub(crate) struct ReconcileOutcome {
    /// Claims orphaned by the restart that were recovered for retry.
    pub(crate) recovered: usize,
    /// Claims that could not be recovered and were failed.
    pub(crate) failed: usize,
    /// Concurrency holders dropped.
    pub(crate) holders_dropped: usize,
}

// ─────────────────────────────────────────────────────────────────────────
// The concrete backend
// ─────────────────────────────────────────────────────────────────────────

/// The configured control backend: SQLite (default, single node) or
/// PostgreSQL (shared nodes). `AppState` holds `Arc<Backend>` — a concrete
/// enum, not `Arc<dyn ControlBackend>`, so [`Backend::transact`] can stay
/// generic (a generic method is not object-safe over `dyn`).
pub(crate) enum Backend {
    /// Single-node default: one writer on `<state_dir>/preloop.db`.
    Sqlite(super::sqlite::SqliteBackend),
    /// Shared-node: a connection pool against the `control` schema.
    Postgres(super::postgres::PostgresBackend),
}

impl Backend {
    /// Wake-ups committed by any node sharing this database. `None` on
    /// SQLite: a single node wakes its own waiters directly.
    pub(crate) fn subscribe_wakes(
        &self,
    ) -> Option<tokio::sync::broadcast::Receiver<super::wake::Wake>> {
        match self {
            Self::Sqlite(_) => None,
            Self::Postgres(b) => Some(b.subscribe_wakes()),
        }
    }

    /// Open the backend selected by `store_url`: `postgres://…` → PostgreSQL,
    /// anything else (`sqlite://<path>`, a bare path) → SQLite. The default
    /// authoritative database is `<state_dir>/preloop.db`.
    pub(crate) async fn open(
        store_url: Option<&str>,
        state_dir: &std::path::Path,
        cipher: crate::store::Envelope,
        pool_assignments_enabled: bool,
        require_job_assignments: bool,
        runner_liveness_timeout: std::time::Duration,
    ) -> Result<Self, ControlError> {
        if let Some(url) =
            store_url.filter(|u| u.starts_with("postgres://") || u.starts_with("postgresql://"))
        {
            let backend = super::postgres::PostgresBackend::connect(
                url,
                cipher,
                pool_assignments_enabled,
                require_job_assignments,
                runner_liveness_timeout,
            )
            .await?;
            return Ok(Self::Postgres(backend));
        }
        let path = state_dir.join("preloop.db");
        let backend = super::sqlite::SqliteBackend::open(
            &path,
            cipher,
            pool_assignments_enabled,
            require_job_assignments,
            runner_liveness_timeout,
        )?;
        Ok(Self::Sqlite(backend))
    }

    /// Run `f` inside one transaction on the scheduling working set.
    /// `&self` — serialization lives in the database. The SQLite arm runs
    /// its synchronous transaction inline (a short local `BEGIN
    /// IMMEDIATE`); the Postgres arm awaits its connection.
    pub(crate) async fn transact<T>(
        &self,
        f: impl FnOnce(&mut super::txstate::TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.transact(f),
            Self::Postgres(backend) => backend.transact(f).await,
        }
    }

    /// Run `f` on a read-only pooled connection — concurrent with the
    /// writer, never queued behind it. Use for commands that only read the
    /// working set (`acquire_context`, `run_record`, `queue_stats`, …);
    /// read-modify-write commands go through [`Backend::transact`].
    pub(crate) async fn read<T>(
        &self,
        f: impl FnOnce(&super::txstate::TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.read(f),
            Self::Postgres(backend) => backend.read(f).await,
        }
    }

    /// `read` under an explicit [`TxScope`] — loads only that working set on
    /// a pooled read connection. Hot read paths (`append_log`, `console_log`)
    /// use a narrow scope instead of materializing the full working set.
    pub(crate) async fn read_scoped<T>(
        &self,
        scope: &super::txstate::TxScope,
        f: impl FnOnce(&super::txstate::TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.read_scoped(scope, f),
            Self::Postgres(backend) => backend.read_scoped(scope, f).await,
        }
    }

    /// `transact` under an explicit [`TxScope`] — loads only that working
    /// set. Used by the boot import (full scope) and hot paths that opt into
    /// a narrow scope.
    pub(crate) async fn transact_scoped<T>(
        &self,
        scope: &super::txstate::TxScope,
        f: impl FnOnce(&mut super::txstate::TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.transact_scoped(scope, f),
            Self::Postgres(backend) => backend.transact_scoped(scope, f).await,
        }
    }

    /// Find the run a webhook delivery already produced, by its durable
    /// `(delivery_id, workflow_path)` dedup key. O(1) via `runs_delivery` —
    /// the submit path calls this before `transact_scoped` so the replay
    /// check never scans the whole `runs` table.
    pub(crate) async fn find_run_by_delivery(
        &self,
        delivery_id: &str,
        workflow_path: &str,
    ) -> Result<Option<crate::models::RunRecord>, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.find_run_by_delivery(delivery_id, workflow_path),
            Self::Postgres(backend) => {
                backend
                    .find_run_by_delivery(delivery_id, workflow_path)
                    .await
            }
        }
    }

    /// Resolve a request's `(request_id, run_id)` from its `agent_job_id`.
    /// O(1) via `job_requests_agent` — the broker complete path calls this
    /// before `transact_scoped` so the working set stays narrow.
    pub(crate) async fn find_request_by_agent_job_id(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<(i64, RunId)>, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.find_request_by_agent_job_id(agent_job_id),
            Self::Postgres(backend) => backend.find_request_by_agent_job_id(agent_job_id).await,
        }
    }

    /// The session that owns the request for `agent_job_id`. `complete_job`
    /// resolves this before `transact_scoped` so `settle_request` can drop
    /// the moot cancellation from the owner session's inflight messages.
    pub(crate) async fn find_session_by_agent_job_id(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<String>, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.find_session_by_agent_job_id(agent_job_id),
            Self::Postgres(backend) => backend.find_session_by_agent_job_id(agent_job_id).await,
        }
    }

    /// Every session owning a request of `run_id`. `cancel_run`/`cancel_job`
    /// resolve these before `transact_scoped` so `settle_request` can drop
    /// moot cancellations from each owner session's inflight messages.
    pub(crate) async fn find_sessions_by_run(
        &self,
        run_id: RunId,
    ) -> Result<std::collections::BTreeSet<String>, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.find_sessions_by_run(run_id),
            Self::Postgres(backend) => backend.find_sessions_by_run(run_id).await,
        }
    }

    /// `(run_id, owner_session)` for `request_id`. `acquire_context` resolves
    /// this before `read_scoped` so the read stays narrow.
    pub(crate) async fn find_request_context(
        &self,
        request_id: i64,
    ) -> Result<Option<(RunId, Option<String>)>, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.find_request_context(request_id),
            Self::Postgres(backend) => backend.find_request_context(request_id).await,
        }
    }

    /// Update the scheduling config after open. Bootstrap applies the real
    /// server config here once known — `open` only receives the recovered
    /// defaults.
    pub(crate) fn set_config(
        &self,
        pool_assignments_enabled: bool,
        require_job_assignments: bool,
        runner_liveness_timeout: std::time::Duration,
    ) {
        match self {
            Self::Sqlite(backend) => backend.set_config(
                pool_assignments_enabled,
                require_job_assignments,
                runner_liveness_timeout,
            ),
            Self::Postgres(backend) => backend.set_config(
                pool_assignments_enabled,
                require_job_assignments,
                runner_liveness_timeout,
            ),
        }
    }

    /// Current scheduling config — the counterpart to [`Backend::set_config`]
    /// for callers that need to flip one flag without knowing the rest.
    #[cfg(test)]
    pub(crate) fn config(&self) -> (bool, bool, std::time::Duration) {
        match self {
            Self::Sqlite(backend) => backend.config(),
            Self::Postgres(backend) => backend.config(),
        }
    }
}

#[async_trait::async_trait]
impl ControlBackend for Backend {
    async fn submit_run(&self, submit: SubmitRun) -> Result<SubmitOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.submit_run(submit).await,
            Self::Postgres(b) => b.submit_run(submit).await,
        }
    }
    async fn allocate_run_number(&self, workflow_path: &str) -> Result<u64, ControlError> {
        match self {
            Self::Sqlite(b) => b.allocate_run_number(workflow_path).await,
            Self::Postgres(b) => b.allocate_run_number(workflow_path).await,
        }
    }
    async fn poll_session(&self, poll: PollRequest) -> Result<PollOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.poll_session(poll).await,
            Self::Postgres(b) => b.poll_session(poll).await,
        }
    }
    async fn acquire_context(&self, request_id: i64) -> Result<AcquireContext, ControlError> {
        match self {
            Self::Sqlite(b) => b.acquire_context(request_id).await,
            Self::Postgres(b) => b.acquire_context(request_id).await,
        }
    }
    async fn acquire_for_runner(
        &self,
        request_id: i64,
        runner_id: i64,
    ) -> Result<AcquireContext, ControlError> {
        match self {
            Self::Sqlite(b) => b.acquire_for_runner(request_id, runner_id).await,
            Self::Postgres(b) => b.acquire_for_runner(request_id, runner_id).await,
        }
    }
    async fn store_request_message(
        &self,
        run_id: RunId,
        request_id: i64,
        message: Option<&azdo::AgentJobRequestMessage>,
        token_request: Option<&crate::models::GitHubTokenRequest>,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.store_request_message(run_id, request_id, message, token_request)
                    .await
            }
            Self::Postgres(b) => {
                b.store_request_message(run_id, request_id, message, token_request)
                    .await
            }
        }
    }
    async fn complete_job(
        &self,
        completion: JobCompletionInput,
    ) -> Result<CompleteOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.complete_job(completion).await,
            Self::Postgres(b) => b.complete_job(completion).await,
        }
    }
    async fn cancel_run(
        &self,
        run_id: RunId,
        reason: Option<String>,
    ) -> Result<CancelOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.cancel_run(run_id, reason).await,
            Self::Postgres(b) => b.cancel_run(run_id, reason).await,
        }
    }
    async fn cancel_job(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<CancelOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.cancel_job(run_id, job_id).await,
            Self::Postgres(b) => b.cancel_job(run_id, job_id).await,
        }
    }
    async fn renew_request(
        &self,
        request_id: i64,
        runner_id: i64,
    ) -> Result<TaskAgentJobRequestRecord, ControlError> {
        match self {
            Self::Sqlite(b) => b.renew_request(request_id, runner_id).await,
            Self::Postgres(b) => b.renew_request(request_id, runner_id).await,
        }
    }
    async fn request(&self, key: RequestKey) -> Result<TaskAgentJobRequestRecord, ControlError> {
        match self {
            Self::Sqlite(b) => b.request(key).await,
            Self::Postgres(b) => b.request(key).await,
        }
    }
    async fn artifact_scopes(
        &self,
        plan_ids: &[String],
    ) -> Result<BTreeMap<String, RunId>, ControlError> {
        match self {
            Self::Sqlite(b) => b.artifact_scopes(plan_ids).await,
            Self::Postgres(b) => b.artifact_scopes(plan_ids).await,
        }
    }
    async fn set_push_state(&self, run_id: RunId, state: PushState) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.set_push_state(run_id, state).await,
            Self::Postgres(b) => b.set_push_state(run_id, state).await,
        }
    }
    async fn release_request(&self, request_id: i64) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.release_request(request_id).await,
            Self::Postgres(b) => b.release_request(request_id).await,
        }
    }
    async fn register_runner(&self, reg: RegisterRunner) -> Result<RunnerRow, ControlError> {
        match self {
            Self::Sqlite(b) => b.register_runner(reg).await,
            Self::Postgres(b) => b.register_runner(reg).await,
        }
    }
    async fn create_session(&self, session: CreateSession) -> Result<SessionRow, ControlError> {
        match self {
            Self::Sqlite(b) => b.create_session(session).await,
            Self::Postgres(b) => b.create_session(session).await,
        }
    }
    async fn delete_session(&self, session_id: &str) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.delete_session(session_id).await,
            Self::Postgres(b) => b.delete_session(session_id).await,
        }
    }
    async fn purge_runner(&self, runner_id: i64) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.purge_runner(runner_id).await,
            Self::Postgres(b) => b.purge_runner(runner_id).await,
        }
    }
    async fn claim_expansion(&self) -> Result<Option<ExpansionClaim>, ControlError> {
        match self {
            Self::Sqlite(b) => b.claim_expansion().await,
            Self::Postgres(b) => b.claim_expansion().await,
        }
    }
    async fn apply_expansion(
        &self,
        claim: ExpansionApply,
    ) -> Result<SchedulingOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.apply_expansion(claim).await,
            Self::Postgres(b) => b.apply_expansion(claim).await,
        }
    }
    async fn reconcile_on_boot(&self) -> Result<ReconcileOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.reconcile_on_boot().await,
            Self::Postgres(b) => b.reconcile_on_boot().await,
        }
    }
    async fn run_record(&self, run_id: RunId) -> Result<RunRecord, ControlError> {
        match self {
            Self::Sqlite(b) => b.run_record(run_id).await,
            Self::Postgres(b) => b.run_record(run_id).await,
        }
    }
    async fn list_runs(&self, filter: RunListFilter) -> Result<Vec<RunRecord>, ControlError> {
        match self {
            Self::Sqlite(b) => b.list_runs(filter).await,
            Self::Postgres(b) => b.list_runs(filter).await,
        }
    }
    async fn terminal_jobs(
        &self,
    ) -> Result<std::collections::BTreeSet<(RunId, JobId)>, ControlError> {
        match self {
            Self::Sqlite(b) => b.terminal_jobs().await,
            Self::Postgres(b) => b.terminal_jobs().await,
        }
    }
    async fn archive_finished_runs(&self, limit: usize) -> Result<usize, ControlError> {
        match self {
            Self::Sqlite(b) => b.archive_finished_runs(limit).await,
            Self::Postgres(b) => b.archive_finished_runs(limit).await,
        }
    }
    async fn append_event(&self, event: &NdjsonEvent) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.append_event(event).await,
            Self::Postgres(b) => b.append_event(event).await,
        }
    }
    async fn create_log(&self, plan_id: &str) -> Result<i64, ControlError> {
        match self {
            Self::Sqlite(b) => b.create_log(plan_id).await,
            Self::Postgres(b) => b.create_log(plan_id).await,
        }
    }
    async fn store_meta(&self, meta: &crate::store::MetaSnapshot) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.store_meta(meta).await,
            Self::Postgres(b) => b.store_meta(meta).await,
        }
    }
    async fn load_meta(&self) -> Result<Option<crate::store::MetaSnapshot>, ControlError> {
        match self {
            Self::Sqlite(b) => b.load_meta().await,
            Self::Postgres(b) => b.load_meta().await,
        }
    }
    async fn ensure_key_fingerprint(&self, fingerprint: &str) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.ensure_key_fingerprint(fingerprint).await,
            Self::Postgres(b) => b.ensure_key_fingerprint(fingerprint).await,
        }
    }
    async fn touch_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionProtocol>, ControlError> {
        match self {
            Self::Sqlite(b) => b.touch_session(session_id).await,
            Self::Postgres(b) => b.touch_session(session_id).await,
        }
    }
    async fn session_owner(
        &self,
        session_id: &str,
    ) -> Result<Option<(i64, crate::models::RunnerCapabilities)>, ControlError> {
        match self {
            Self::Sqlite(b) => b.session_owner(session_id).await,
            Self::Postgres(b) => b.session_owner(session_id).await,
        }
    }
    async fn patch_timeline(
        &self,
        timeline_key: &str,
        records: Vec<preloop_gha_protocol::azdo::TimelineRecord>,
    ) -> Result<(i32, Vec<preloop_gha_protocol::azdo::TimelineRecord>), ControlError> {
        match self {
            Self::Sqlite(b) => b.patch_timeline(timeline_key, records).await,
            Self::Postgres(b) => b.patch_timeline(timeline_key, records).await,
        }
    }
    async fn get_timeline(
        &self,
        timeline_key: &str,
        skip: usize,
        top: usize,
    ) -> Result<(i32, Vec<preloop_gha_protocol::azdo::TimelineRecord>), ControlError> {
        match self {
            Self::Sqlite(b) => b.get_timeline(timeline_key, skip, top).await,
            Self::Postgres(b) => b.get_timeline(timeline_key, skip, top).await,
        }
    }
    async fn prune_timelines(&self, before_us: i64) -> Result<u64, ControlError> {
        match self {
            Self::Sqlite(b) => b.prune_timelines(before_us).await,
            Self::Postgres(b) => b.prune_timelines(before_us).await,
        }
    }
    async fn runner_exists(&self, runner_id: i64) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => b.runner_exists(runner_id).await,
            Self::Postgres(b) => b.runner_exists(runner_id).await,
        }
    }

    async fn runner_for_client(&self, client_id: &str) -> Result<Option<i64>, ControlError> {
        match self {
            Self::Sqlite(b) => b.runner_for_client(client_id).await,
            Self::Postgres(b) => b.runner_for_client(client_id).await,
        }
    }

    async fn run_for_attempt(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<RunId>, ControlError> {
        match self {
            Self::Sqlite(b) => b.run_for_attempt(agent_job_id).await,
            Self::Postgres(b) => b.run_for_attempt(agent_job_id).await,
        }
    }

    async fn patch_steps(
        &self,
        agent_job_id: uuid::Uuid,
        patches: Vec<StepPatch>,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.patch_steps(agent_job_id, patches).await,
            Self::Postgres(b) => b.patch_steps(agent_job_id, patches).await,
        }
    }

    async fn set_job_check_run(
        &self,
        run_id: RunId,
        job_id: &JobId,
        check_run_id: u64,
    ) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => b.set_job_check_run(run_id, job_id, check_run_id).await,
            Self::Postgres(b) => b.set_job_check_run(run_id, job_id, check_run_id).await,
        }
    }

    async fn clear_job_check_run(
        &self,
        run_id: RunId,
        job_id: &JobId,
        expected: u64,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.clear_job_check_run(run_id, job_id, expected).await,
            Self::Postgres(b) => b.clear_job_check_run(run_id, job_id, expected).await,
        }
    }

    async fn job_exists(&self, run_id: RunId, job_id: &JobId) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => b.job_exists(run_id, job_id).await,
            Self::Postgres(b) => b.job_exists(run_id, job_id).await,
        }
    }

    async fn job_display_name(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<String>, ControlError> {
        match self {
            Self::Sqlite(b) => b.job_display_name(run_id, job_id).await,
            Self::Postgres(b) => b.job_display_name(run_id, job_id).await,
        }
    }

    async fn job_detail_missing(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => b.job_detail_missing(run_id, job_id).await,
            Self::Postgres(b) => b.job_detail_missing(run_id, job_id).await,
        }
    }

    async fn ensure_job_detail(
        &self,
        run_id: RunId,
        job_id: &JobId,
        conclusion: Option<&str>,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.ensure_job_detail(run_id, job_id, conclusion).await,
            Self::Postgres(b) => b.ensure_job_detail(run_id, job_id, conclusion).await,
        }
    }

    async fn attempt_job(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<(RunId, JobId)>, ControlError> {
        match self {
            Self::Sqlite(b) => b.attempt_job(agent_job_id).await,
            Self::Postgres(b) => b.attempt_job(agent_job_id).await,
        }
    }

    async fn reap_inputs(&self) -> Result<ReapInputs, ControlError> {
        match self {
            Self::Sqlite(b) => b.reap_inputs().await,
            Self::Postgres(b) => b.reap_inputs().await,
        }
    }

    async fn run_in_concurrency(&self, run_id: RunId) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => b.run_in_concurrency(run_id).await,
            Self::Postgres(b) => b.run_in_concurrency(run_id).await,
        }
    }
    async fn callback_job(
        &self,
        plan_id: &str,
        timeline_id: Option<uuid::Uuid>,
    ) -> Result<Option<CallbackJob>, ControlError> {
        match self {
            Self::Sqlite(b) => b.callback_job(plan_id, timeline_id).await,
            Self::Postgres(b) => b.callback_job(plan_id, timeline_id).await,
        }
    }
    async fn renew_lease(
        &self,
        agent_job_id: uuid::Uuid,
        runner_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => b.renew_lease(agent_job_id, runner_id, locked_until).await,
            Self::Postgres(b) => b.renew_lease(agent_job_id, runner_id, locked_until).await,
        }
    }
    async fn request_owner(
        &self,
        request_id: i64,
    ) -> Result<Option<(Option<i64>, Option<i64>)>, ControlError> {
        match self {
            Self::Sqlite(b) => b.request_owner(request_id).await,
            Self::Postgres(b) => b.request_owner(request_id).await,
        }
    }
    async fn renew_agent_request(
        &self,
        request_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => b.renew_agent_request(request_id, locked_until).await,
            Self::Postgres(b) => b.renew_agent_request(request_id, locked_until).await,
        }
    }
    async fn settle_agent_request(
        &self,
        request_id: i64,
        result: ExecutionStatus,
        locked_until: &str,
    ) -> Result<Option<(RunId, JobId, uuid::Uuid)>, ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.settle_agent_request(request_id, result, locked_until)
                    .await
            }
            Self::Postgres(b) => {
                b.settle_agent_request(request_id, result, locked_until)
                    .await
            }
        }
    }
    async fn delete_inflight(&self, session_id: &str, message_id: i64) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.delete_inflight(session_id, message_id).await,
            Self::Postgres(b) => b.delete_inflight(session_id, message_id).await,
        }
    }
    async fn active_plan_ids(&self) -> Result<BTreeSet<String>, ControlError> {
        match self {
            Self::Sqlite(b) => b.active_plan_ids().await,
            Self::Postgres(b) => b.active_plan_ids().await,
        }
    }
    async fn enqueue_webhook_delivery(
        &self,
        delivery: &WebhookDeliveryRecord,
    ) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => b.enqueue_webhook_delivery(delivery).await,
            Self::Postgres(b) => b.enqueue_webhook_delivery(delivery).await,
        }
    }
    async fn claim_webhook_deliveries(
        &self,
        limit: usize,
        lease_duration_secs: u64,
    ) -> Result<Vec<WebhookDeliveryRecord>, ControlError> {
        match self {
            Self::Sqlite(b) => b.claim_webhook_deliveries(limit, lease_duration_secs).await,
            Self::Postgres(b) => b.claim_webhook_deliveries(limit, lease_duration_secs).await,
        }
    }
    async fn renew_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        lease_duration_secs: u64,
    ) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.renew_webhook_delivery(delivery_id, lease_token, lease_duration_secs)
                    .await
            }
            Self::Postgres(b) => {
                b.renew_webhook_delivery(delivery_id, lease_token, lease_duration_secs)
                    .await
            }
        }
    }
    async fn complete_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
    ) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => b.complete_webhook_delivery(delivery_id, lease_token).await,
            Self::Postgres(b) => b.complete_webhook_delivery(delivery_id, lease_token).await,
        }
    }
    async fn fail_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        error: &str,
        permanent: bool,
        retry_delay: Option<std::time::Duration>,
    ) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.fail_webhook_delivery(delivery_id, lease_token, error, permanent, retry_delay)
                    .await
            }
            Self::Postgres(b) => {
                b.fail_webhook_delivery(delivery_id, lease_token, error, permanent, retry_delay)
                    .await
            }
        }
    }
    async fn get_webhook_delivery(
        &self,
        delivery_id: &str,
    ) -> Result<Option<WebhookDeliveryRecord>, ControlError> {
        match self {
            Self::Sqlite(b) => b.get_webhook_delivery(delivery_id).await,
            Self::Postgres(b) => b.get_webhook_delivery(delivery_id).await,
        }
    }
    async fn count_dead_letter_webhook_deliveries(&self) -> Result<u64, ControlError> {
        match self {
            Self::Sqlite(b) => b.count_dead_letter_webhook_deliveries().await,
            Self::Postgres(b) => b.count_dead_letter_webhook_deliveries().await,
        }
    }
    async fn recover_webhook_deliveries(&self) -> Result<u64, ControlError> {
        match self {
            Self::Sqlite(b) => b.recover_webhook_deliveries().await,
            Self::Postgres(b) => b.recover_webhook_deliveries().await,
        }
    }
    async fn prune_webhook_deliveries(
        &self,
        before_us: i64,
        limit: usize,
    ) -> Result<u64, ControlError> {
        match self {
            Self::Sqlite(b) => b.prune_webhook_deliveries(before_us, limit).await,
            Self::Postgres(b) => b.prune_webhook_deliveries(before_us, limit).await,
        }
    }
    async fn park_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        error: &str,
        retry_delay_secs: u64,
    ) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.park_webhook_delivery(delivery_id, lease_token, error, retry_delay_secs)
                    .await
            }
            Self::Postgres(b) => {
                b.park_webhook_delivery(delivery_id, lease_token, error, retry_delay_secs)
                    .await
            }
        }
    }
    async fn requeue_webhook_delivery(&self, delivery_id: &str) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => b.requeue_webhook_delivery(delivery_id).await,
            Self::Postgres(b) => b.requeue_webhook_delivery(delivery_id).await,
        }
    }
    async fn list_webhook_deliveries(
        &self,
        state: Option<WebhookDeliveryStatus>,
        limit: usize,
    ) -> Result<Vec<WebhookDeliverySummary>, ControlError> {
        match self {
            Self::Sqlite(b) => b.list_webhook_deliveries(state, limit).await,
            Self::Postgres(b) => b.list_webhook_deliveries(state, limit).await,
        }
    }
    async fn webhook_deliveries_present(
        &self,
        delivery_ids: &[String],
    ) -> Result<std::collections::BTreeSet<String>, ControlError> {
        match self {
            Self::Sqlite(b) => b.webhook_deliveries_present(delivery_ids).await,
            Self::Postgres(b) => b.webhook_deliveries_present(delivery_ids).await,
        }
    }
    async fn webhook_queue_stats(&self) -> Result<WebhookQueueStats, ControlError> {
        match self {
            Self::Sqlite(b) => b.webhook_queue_stats().await,
            Self::Postgres(b) => b.webhook_queue_stats().await,
        }
    }
    async fn load_webhook_watchdog_cursor(
        &self,
        scope: &str,
    ) -> Result<Option<WebhookWatchdogCursor>, ControlError> {
        match self {
            Self::Sqlite(b) => b.load_webhook_watchdog_cursor(scope).await,
            Self::Postgres(b) => b.load_webhook_watchdog_cursor(scope).await,
        }
    }
    async fn store_webhook_watchdog_cursor(
        &self,
        cursor: &WebhookWatchdogCursor,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.store_webhook_watchdog_cursor(cursor).await,
            Self::Postgres(b) => b.store_webhook_watchdog_cursor(cursor).await,
        }
    }
    async fn upsert_webhook_redelivery(
        &self,
        record: &WebhookRedeliveryRecord,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.upsert_webhook_redelivery(record).await,
            Self::Postgres(b) => b.upsert_webhook_redelivery(record).await,
        }
    }
    async fn load_webhook_redelivery(
        &self,
        delivery_guid: &str,
    ) -> Result<Option<WebhookRedeliveryRecord>, ControlError> {
        match self {
            Self::Sqlite(b) => b.load_webhook_redelivery(delivery_guid).await,
            Self::Postgres(b) => b.load_webhook_redelivery(delivery_guid).await,
        }
    }
    async fn open_webhook_redeliveries(
        &self,
        limit: usize,
    ) -> Result<Vec<WebhookRedeliveryRecord>, ControlError> {
        match self {
            Self::Sqlite(b) => b.open_webhook_redeliveries(limit).await,
            Self::Postgres(b) => b.open_webhook_redeliveries(limit).await,
        }
    }
    async fn resolve_webhook_redelivery(
        &self,
        delivery_guid: &str,
        resolved_at_us: i64,
    ) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.resolve_webhook_redelivery(delivery_guid, resolved_at_us)
                    .await
            }
            Self::Postgres(b) => {
                b.resolve_webhook_redelivery(delivery_guid, resolved_at_us)
                    .await
            }
        }
    }
    async fn queue_stats(&self) -> Result<QueueStats, ControlError> {
        match self {
            Self::Sqlite(b) => b.queue_stats().await,
            Self::Postgres(b) => b.queue_stats().await,
        }
    }
    async fn live_assignments(
        &self,
    ) -> Result<Vec<preloop_observability::status::RunnerAssignment>, ControlError> {
        match self {
            Self::Sqlite(b) => b.live_assignments().await,
            Self::Postgres(b) => b.live_assignments().await,
        }
    }
}
