//! The `ControlBackend` contract: every durable control mutation is one typed
//! command with transactional authority, idempotency and fencing.
//!
//! This is the only surface the server sees for durable control state.
//! Handlers parse requests and map domain results onto wire responses; they
//! never touch a transaction handle, a SQL string, or a mutable record. A
//! backend runs each command as one short transaction of targeted SQL
//! (conditional updates, no working-set load or write-back), calling the
//! shared decision functions in [`crate::control::logic`] where Rust
//! evaluation is needed, and returns the domain result.
//!
//! SQLite is the default backend (single writer, `BEGIN IMMEDIATE`, WAL).
//! Postgres implements the identical contract for shared-node deployments
//! (a pool of connections, `SELECT … FOR UPDATE SKIP LOCKED` for leases).
//! Both produce the same domain results from the same inputs — the shared
//! behavioral suite in `control::tests` proves it.

use super::logic::{BuiltExpansion, SchedulingOutcome};
use super::types::*;
use crate::models::{
    EnvironmentGateState, JobDetail, PushState, QueuedJob, RunRecord, StepRecord,
    TaskAgentJobRequestRecord, WebhookDeliveryRecord, WebhookDeliveryStatus,
    WebhookDeliverySummary, WebhookQueueStats, WebhookRedeliveryRecord, WebhookWatchdogCursor,
};
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

/// Outcome of a promotion pass: the jobs a hold released and the queue gauges
/// the caller stores after it (`promote_ready_jobs`).
#[derive(Debug, Clone, Default)]
pub(crate) struct PromoteOutcome {
    /// Jobs admitted to the ready queue by this pass.
    pub(crate) promoted: usize,
    /// Jobs failed closed by a denied or expired environment gate.
    pub(crate) failed: usize,
    /// The jobs the pass failed, for callers that need per-job resolution
    /// (e.g. the approval handler reporting whether *its* job concluded).
    pub(crate) failed_jobs: Vec<JobId>,
    /// Global ready-queue depth after the pass.
    pub(crate) queue_depth: usize,
    /// `runs-on` labels of the ready-queue front after the pass.
    pub(crate) next_runs_on: Vec<String>,
}

/// One job's environment-gate state as the approve-job handler reads it.
#[derive(Debug, Clone)]
pub(crate) struct EnvironmentGateRead {
    /// The job's stored `environment:` value (`job_specs.environment`).
    pub(crate) environment: Option<serde_json::Value>,
    /// The armed gate progress (`jobs.environment_gate`); `None` when the job
    /// never armed one (or it was already satisfied).
    pub(crate) gate: Option<EnvironmentGateState>,
    /// The job's current status.
    pub(crate) status: ExecutionStatus,
}

/// Extract the optional run identity carried by a durable event.
pub(crate) fn event_run_id(event: &NdjsonEvent) -> Option<RunId> {
    match event {
        NdjsonEvent::RunAccepted { run_id, .. }
        | NdjsonEvent::JobStatus { run_id, .. }
        | NdjsonEvent::RunStatus { run_id, .. }
        | NdjsonEvent::JobCompleted { run_id, .. }
        | NdjsonEvent::Annotation { run_id, .. }
        | NdjsonEvent::CheckRunCreated { run_id } => Some(*run_id),
        _ => None,
    }
}

/// Project database rows into the public run shape from a row snapshot.
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
    /// acceptable (run numbers may have gaps). Scoped by
    /// `(namespace_id, repository, workflow_path)` to match the agreed
    /// `workflow_run_numbers` primary key.
    async fn allocate_run_number(
        &self,
        namespace_id: &str,
        repository: &str,
        workflow_path: &str,
    ) -> Result<u64, ControlError>;
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

    /// Upsert the deferred GitHub-token mint request for `request_id`
    /// (`github_token_requests` row). Called at submit (the request row is
    /// written alongside the message template) and at acquire when the
    /// broker re-derives a request lost to a pre-snapshot crash.
    ///
    /// `run_id` scopes the transaction's advisory lock so the upsert cannot
    /// race a scoped write-back that would otherwise delete the row.
    ///
    /// The job message itself has no write path here by contract: stored
    /// `request_blob` is a secret-free *template*; the acquire-time fill must
    /// never be persisted.
    async fn record_token_request(
        &self,
        run_id: RunId,
        request_id: i64,
        token_request: &crate::models::GitHubTokenRequest,
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
    /// settle expandable-node request records. `cancel_run` fails with
    /// `NotFound` when the run does not exist (nothing is written). The
    /// outcome carries the post-transition run record, every job of the run
    /// now `Cancelled` (job-id order), and the global ready-queue depth and
    /// front `runs-on` labels for the pool wake gauges.
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

    /// Persist a run's fork-PR approval hold: the parking stamp written at
    /// submit, the approval release, or the expiry sweep's clear. Scoped to
    /// the `runs` row; no job or queue state is touched here (the callers
    /// own promotion/fail-closed effects).
    async fn set_fork_approval(&self, update: ForkApprovalStamp) -> Result<(), ControlError>;

    /// Persist one job's environment protection gate state (`None` clears
    /// it). The gate is job runtime state — armed at scheduler admission,
    /// updated when an approval is recorded, cleared once satisfied — and
    /// travels with the job so a restart re-arms it (fail closed).
    async fn set_environment_gate(
        &self,
        run_id: RunId,
        job_id: &JobId,
        gate: Option<EnvironmentGateState>,
    ) -> Result<(), ControlError>;

    /// Stamp whether intake reported GitHub check runs for this run (webhook,
    /// dispatch, push, or rerun path). Persisted so jobs materialized after
    /// submission — runtime-expanded matrix legs, reusable callee jobs —
    /// still mint checks after a restart (`report_check_run_queued` sets it
    /// true on every reporting path).
    async fn set_reports_check_runs(
        &self,
        run_id: RunId,
        reported: bool,
    ) -> Result<(), ControlError>;

    /// Record one operator approval for a job waiting on its environment's
    /// required-reviewer gate: fail the job closed when the approval window
    /// lapsed, otherwise append the approval stamp, persist the gate and
    /// re-run the run's promotion sweep so a satisfied gate releases the job.
    /// `NotFound` when the job row does not exist; every other decision comes
    /// back in the outcome (the handler maps them to HTTP).
    async fn record_environment_approval(
        &self,
        approval: EnvironmentApproval,
    ) -> Result<EnvironmentApprovalOutcome, ControlError>;

    /// Re-run scheduler admission for the jobs a run parked — the fork-PR
    /// approval hold and armed environment protection gates — then promote
    /// whatever the release unblocked. One transaction per run.
    ///
    /// `Some(run_id)` is the approve/release path (approve-fork, approve-job,
    /// a hold lifting). `None` sweeps every run currently parking a
    /// gate-armed job: wait timers and approval windows close on wall-clock
    /// time, so the reaper drives that sweep (after refreshing the
    /// environment-rules resolver). Environment rules come from the
    /// backend's own resolver — the same `EnvironmentResolver` `AppState`
    /// installs — so a `Pending` lookup holds the job fail-closed until the
    /// reaper's refresh fills it. A parked job whose run is still
    /// fork-approval-pending stays parked; a job whose gate fails closed
    /// (deployment-branch mismatch, approval window expired, rejection) is
    /// concluded `Failure`. Returns the promoted/failed counts, the failed
    /// job ids, and the post-pass queue gauges the caller stores.
    async fn promote_ready_jobs(&self, run: Option<RunId>) -> Result<PromoteOutcome, ControlError>;

    /// Install the environment-rules resolver the gate evaluation consults
    /// inside its transactions. AppState's resolver is the same object, so
    /// `Pending` lookups the backend triggers surface in the shared pending
    /// set the reaper drains. Tests install a TOML-only resolver.
    fn set_environment_resolver(
        &self,
        resolver: std::sync::Arc<crate::environment_resolver::EnvironmentResolver>,
    );

    /// The jobs still parked on an armed required-reviewer gate (the gate's
    /// `approval_requested_at` is set and no approval satisfies it yet),
    /// with the GitHub side-channel ids the announce/approve paths PATCH.
    /// `Some(run_id)` narrows to one run; `None` scans every run (the reaper
    /// sweep + the check-run webhook lookup).
    async fn pending_environment_approvals(
        &self,
        run_id: Option<RunId>,
    ) -> Result<Vec<PendingEnvironmentApproval>, ControlError>;

    /// One held approval-gated job by its GitHub check run id — the
    /// `check_run.requested_action` webhook's join key.
    async fn pending_environment_approval_for_check_run(
        &self,
        check_run_id: u64,
    ) -> Result<Option<PendingEnvironmentApproval>, ControlError>;

    /// Stamp `approval_announced` on the job's gate: the Approve/Reject
    /// check-run PATCH was delivered (or the job reports no check run and
    /// nothing would show). In-place update — concurrent approvals survive.
    async fn mark_environment_approval_announced(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<(), ControlError>;

    /// The job's GitHub deployment id (`jobs.deployment_id`), when the
    /// reporting path created one for a job with `environment:`.
    async fn job_deployment_id(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<u64>, ControlError>;

    /// Record the GitHub deployment id the reporting path created for the
    /// job's `environment:`.
    async fn set_job_deployment(
        &self,
        run_id: RunId,
        job_id: &JobId,
        deployment_id: u64,
    ) -> Result<(), ControlError>;

    /// One job's stored `environment:` value, its armed environment gate and
    /// its current status — the approve-job handler's read before it records
    /// an approval. `Ok(None)` when the run or job does not exist.
    async fn environment_gate(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<EnvironmentGateRead>, ControlError>;

    /// One job's GitHub deployment side-channel row: check run, deployment
    /// id, resolved environment name/url, head sha. `Ok(None)` for jobs
    /// without `environment:` or whose name no source resolves.
    async fn environment_deployment(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<EnvironmentDeploymentRow>, ControlError>;

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
    /// requeue its claimed jobs so a replacement can pick them up. Returns
    /// the runtime identities of the attempts retired for those retries.
    async fn purge_runner(&self, runner_id: i64) -> Result<Vec<uuid::Uuid>, ControlError>;

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
    /// immutable job/attempt history in one transaction per batch. Returns
    /// the archived run ids. Their run-tier secrets outlive the move: an
    /// archived run can still be re-run.
    async fn archive_finished_runs(&self, limit: usize) -> Result<Vec<RunId>, ControlError>;

    /// Retention selection: ids of terminal runs whose completion (falling
    /// back to creation) is older than `cutoff_us`, oldest first, up to
    /// `limit` (and no more than the backend's own batch cap). Archived rows
    /// count: once the archiver has moved a settled run into
    /// `run_history`, that table is where the expired run actually lives, so
    /// a retention pass that ignored it would never delete anything.
    ///
    /// Only terminal runs are ever candidates. A live run with an unfinished
    /// or session-bound attempt is skipped (the same guard the archiver
    /// uses). Read-only; the caller deletes what it selects with
    /// [`ControlBackend::delete_expired_run`].
    async fn expired_terminal_runs(
        &self,
        cutoff_us: i64,
        limit: usize,
    ) -> Result<Vec<RunId>, ControlError>;

    /// Retention delete of one terminal run: its live rows (cascading its
    /// jobs, requests, attempts, steps, logs and gates) and its history
    /// rows, plus its artifact metadata. `Conflict` when a live run row
    /// exists and is not `completed` — retention must never delete a queued
    /// or in-progress run, and the guard makes that structural rather than a
    /// property of the caller's filter. An unknown (already deleted) run is
    /// `Ok(())`, so a repeated or interrupted pass converges.
    ///
    /// Runs the deletion while holding the run's advisory lock, so it cannot
    /// interleave with a scheduling transaction for the same run.
    async fn delete_expired_run(&self, run_id: RunId) -> Result<(), ControlError>;

    /// Fail closed every fork-PR run whose approval hold expired before
    /// `expired_before_unix_nanos` (`now - FORK_APPROVAL_WINDOW_NANOS`): in
    /// one transaction per run, every non-terminal job becomes `failure`,
    /// the `fork_approval_pending` hold clears, the run finalizes as a
    /// `failure`, its scheduling rows drop and its concurrency holder is
    /// released (promoting the waiters it was wedging). Returns the failed
    /// run ids, for the caller's per-job event and check-run fan-out. Runs
    /// with no hold, a terminal status, or a missing request stamp are
    /// untouched — a bookkeeping gap must not auto-fail a run.
    async fn expire_fork_approvals(
        &self,
        expired_before_unix_nanos: i64,
    ) -> Result<Vec<RunId>, ControlError>;

    /// Append one durable control event. This never reads or rewrites run
    /// state: the command that produced the event already committed it.
    async fn append_event(&self, event: &NdjsonEvent) -> Result<(), ControlError>;

    /// A request record by id, plan id, agent job id or timeline id.
    async fn request(&self, key: RequestKey) -> Result<TaskAgentJobRequestRecord, ControlError>;
    /// Change only a run's push-sync state. The run advisory lock serializes
    /// this with scheduling transactions that may also rewrite run scalars.
    /// An unknown or archived run writes nothing and returns `Ok(())`.
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
    /// and upsert the patched records, write `events` to the transactional
    /// outbox (the PATCH projected them — annotations sit on the timeline's
    /// run), and return the new change id plus every stored record (ordered
    /// by record id). One transaction, shared by every node.
    async fn patch_timeline(
        &self,
        timeline_key: &str,
        records: Vec<preloop_gha_protocol::azdo::TimelineRecord>,
        events: &[NdjsonEvent],
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

    /// Delete up to `limit` outbox rows older than `older_than`, oldest
    /// first. Returns rows removed.
    async fn prune_outbox(
        &self,
        older_than: std::time::Duration,
        limit: usize,
    ) -> Result<u64, ControlError>;

    /// Consume one durable outbox batch for the check-run projector. The
    /// bookmark and all desired-state upserts commit atomically while the
    /// named consumer lease is held by `owner`.
    async fn consume_check_run_outbox(
        &self,
        owner: &str,
        lease_for: std::time::Duration,
        limit: usize,
    ) -> Result<usize, ControlError>;

    /// Lease due desired check-run rows for the single background sender.
    async fn lease_check_run_updates(
        &self,
        owner: &str,
        lease_for: std::time::Duration,
        limit: usize,
    ) -> Result<Vec<CheckRunUpdate>, ControlError>;

    /// Save a GitHub check-run id obtained by the sender. A newer desired
    /// version may have arrived meanwhile; the conditional update preserves
    /// that newer row while still recording the id.
    async fn set_check_run_update_id(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        version: i64,
        check_run_id: u64,
    ) -> Result<(), ControlError>;

    /// Complete a leased row only when no newer desired version arrived.
    async fn finish_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        version: i64,
    ) -> Result<(), ControlError>;

    /// Release a failed sender lease with exponential-backoff metadata.
    async fn retry_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        delay: std::time::Duration,
        permanent: bool,
    ) -> Result<(), ControlError>;

    /// Clear a stale GitHub id so the next attempt recreates/reconciles it.
    async fn clear_check_run_update_id(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        expected: u64,
    ) -> Result<(), ControlError>;

    async fn enqueue_check_run_update(
        &self,
        update: CheckRunUpdateInput,
    ) -> Result<(), ControlError>;

    /// Release a rate-limited lease without counting a delivery attempt.
    async fn defer_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        delay: std::time::Duration,
    ) -> Result<(), ControlError>;

    /// Extend this sender's lease on one row to `now + lease_for`. `false`
    /// when the row is no longer leased by `owner` (another sender took it
    /// over, or it was settled): the caller must not call GitHub for it.
    async fn renew_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        lease_for: std::time::Duration,
    ) -> Result<bool, ControlError>;

    /// Append a durable projection wake after a reporter stamped
    /// `reports_check_runs`.
    async fn append_check_run_projection(
        &self,
        run_id: RunId,
        job_id: Option<&JobId>,
    ) -> Result<(), ControlError>;

    /// Current end of the outbox stream (backend safe point).
    async fn outbox_head(&self) -> Result<Option<OutboxBookmark>, ControlError>;

    /// Up to `limit` outbox rows after `after`, in backend commit order.
    async fn outbox_read(
        &self,
        after: OutboxBookmark,
        limit: usize,
    ) -> Result<Vec<OutboxRow>, ControlError>;

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

    /// Reconcile a `WorkflowStepsUpdate` report against `job_steps` rows.
    /// Resolves the callback identity (`plan_id` then `agent_job_id`, like
    /// [`resolve_callback_job`]), merges each reported step in one
    /// transaction, and returns `false` when no request matches.
    async fn report_steps(
        &self,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
        steps: Vec<serde_json::Value>,
    ) -> Result<bool, ControlError>;

    /// Repository of the live run owning `agent_job_id` (job_requests →
    /// run_submissions, only while the run row exists). `None` when the
    /// attempt is unknown or its run is archived.
    async fn attempt_repository(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<String>, ControlError>;

    /// Whether `agent_job_id` is a request of `run_id` under `plan_id`
    /// (snapshot Git-token membership check).
    async fn attempt_in_run(
        &self,
        run_id: RunId,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
    ) -> Result<bool, ControlError>;

    /// Every job id belonging to `run_id` (jobs ∪ its requests' job ids),
    /// sorted. `NotFound` when the run row does not exist.
    async fn run_job_ids(&self, run_id: RunId) -> Result<Vec<String>, ControlError>;
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

    /// Check-run reporting inputs for one run: repository + head sha (from
    /// `run_submissions.submission_json`), and every job's id, display name,
    /// status, check-run id and `placeholder` flag (an expandable node —
    /// deferred-matrix parent / reusable caller — that never dispatches, so
    /// intake loops skip it). One indexed read — the per-job dispatch fan
    /// -out and completion reporter must not load the run's working set.
    async fn run_dispatch_info(
        &self,
        run_id: RunId,
    ) -> Result<Option<RunDispatchInfo>, ControlError>;

    /// `(repository, sha, base_ref, git_ref)` for push/push-snapshot paths;
    /// `None` when no such run.
    async fn submission_fields(
        &self,
        run_id: RunId,
    ) -> Result<Option<SubmissionFields>, ControlError>;

    /// The persisted check-run id of `job_id` (`run_jobs.check_run_id`).
    async fn job_check_run_id(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<u64>, ControlError>;

    /// A run's `jobs` map — status per logical job id.
    async fn run_job_statuses(
        &self,
        run_id: RunId,
    ) -> Result<Option<Vec<(JobId, ExecutionStatus)>>, ControlError>;

    /// Live-log key for `job_id` in `run_id`: the latest request's
    /// `agent_job_id`, else the logical key when the job belongs to the run.
    /// Also reports whether the run or that job is terminal.
    /// `Ok(None)` when the run does not exist.
    async fn live_log_key(
        &self,
        run_id: RunId,
        job_id: &str,
    ) -> Result<Option<(String, bool)>, ControlError>;

    /// All of a run's request records, sorted by request id (run-logs
    /// endpoint — cold path).
    async fn run_requests(
        &self,
        run_id: RunId,
    ) -> Result<Vec<TaskAgentJobRequestRecord>, ControlError>;

    /// A run's stored `job_steps` manifests keyed by agent job id.
    async fn run_step_manifests(
        &self,
        run_id: RunId,
    ) -> Result<BTreeMap<uuid::Uuid, Vec<crate::models::StepRecord>>, ControlError>;

    /// Issue the one-shot pause-on-failure debug credential: `NotFound`
    /// when no in-flight request owns `agent_job_id`, `Forbidden` when the
    /// run did not opt in, `Conflict` on a second issue. Returns
    /// `(run_id, plan_id)` on success — the mutation and the checks are one
    /// statement-guarded transaction.
    async fn issue_debug_token(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<(RunId, String), ControlError>;

    /// `sweep_stale_bindings` in SQL: drop assignment/pool-pending rows for
    /// jobs no longer ready (assignments on), or expired by TTL (both flags
    /// off), or release dead/expired runner bindings back to the waitlist.
    /// Returns rows changed.
    async fn sweep_stale_bindings(&self) -> Result<usize, ControlError>;

    /// Resolve a check-run rerequest to `(run_id, job_id)`: an exact
    /// `run_jobs.check_run_id` hit on a terminal run matching repository and
    /// head sha; else the check-run `name` matched against job ids and
    /// display names on terminal runs of that repository. `details_run_id`
    /// (parsed from `details_url`) is tried first, then every terminal run.
    async fn check_run_target(
        &self,
        check_run_id: u64,
        repository: &str,
        head_sha: Option<&str>,
        job_name: Option<&str>,
        details_run_id: Option<RunId>,
    ) -> Result<Option<(RunId, JobId)>, ControlError>;

    /// `submission_json` of the run that owns `agent_job_id`'s newest
    /// request — one join, no record assembly. `None` when the attempt no
    /// longer resolves to a run.
    async fn submission_json_for_attempt(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<String>, ControlError>;

    /// The push that already published `sha` for `workflow_path` on
    /// `repository` — a concluded run with a recorded `push_state`, matched
    /// on the submitted sha or the push's `effective_sha`.
    async fn published_run(
        &self,
        repository: &str,
        sha: &str,
        workflow_path: &str,
    ) -> Result<Option<RunId>, ControlError>;

    /// The committed run a webhook delivery produced (`webhook_delivery_id`
    /// + `workflow_path` uniqueness), if any. Submit checks it before
    /// allocating a run number so a replayed delivery does not burn one.
    async fn run_for_webhook_delivery(
        &self,
        delivery_id: &str,
        workflow_path: &str,
    ) -> Result<Option<RunId>, ControlError>;

    /// Whether `run_id` currently waits on a concurrency slot
    /// (`jobs.queue_kind='held'` row). Point read.
    async fn run_held(&self, run_id: RunId) -> Result<bool, ControlError>;

    /// Whether `runner_id` is registered (indexed point read).
    async fn runner_exists(&self, runner_id: i64) -> Result<bool, ControlError>;

    /// The runner registered under OAuth `client_id`, if any.
    async fn runner_for_client(&self, client_id: &str) -> Result<Option<i64>, ControlError>;

    /// The run an execution attempt belongs to.
    async fn run_for_attempt(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<RunId>, ControlError>;

    /// How `run_id` takes part in concurrency groups (holding or waiting,
    /// workflow- or job-level). A run in none settles jobs under its own run
    /// lock: nothing it releases can wake another run.
    async fn run_in_concurrency(&self, run_id: RunId) -> Result<RunConcurrency, ControlError>;

    /// Resolve a runner callback (plan id, else timeline id, else agent job
    /// id) to its attempt: `(request_id, run_id, job_id, agent_job_id, job
    /// status)`, newest attempt first. One indexed query instead of loading
    /// every request.
    async fn callback_job(
        &self,
        plan_id: &str,
        timeline_id: Option<uuid::Uuid>,
        agent_job_id: Option<uuid::Uuid>,
    ) -> Result<Option<CallbackJob>, ControlError>;

    /// The session-claimed request that is still in flight, when exactly one
    /// exists (the finish_job compatibility fallback). More than one active
    /// request returns `None`.
    async fn sole_inflight_request(
        &self,
    ) -> Result<Option<(i64, RunId, JobId, uuid::Uuid)>, ControlError>;

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

    /// `(recorded owner, owner-session runner, session-bound)` for
    /// `request_id`; `None` when no such request. `session-bound` is true
    /// when a session still claims the request — it distinguishes "assigned
    /// but unowned" (replay-compat accept) from "never assigned"
    /// (`NotFound`). Drives the AgentRequest ownership check.
    async fn request_owner(
        &self,
        request_id: i64,
    ) -> Result<Option<(Option<i64>, Option<i64>, bool)>, ControlError>;

    /// Broker-path renew: resolve `agent_job_id`, apply the
    /// recorded-owner → session-owner → session-bound ownership ladder and
    /// renew the lease in one transaction. Errors: `NotFound` (unknown or
    /// never-assigned request), `Forbidden` (foreign owner), `Conflict`
    /// (already completed).
    async fn renew_broker_request(
        &self,
        agent_job_id: uuid::Uuid,
        runner_id: i64,
        locked_until: &str,
    ) -> Result<(), ControlError>;

    /// Create a broker (`runner_sessions`) row owned by `runner_id`.
    /// `Forbidden` when the runner registration no longer exists — the
    /// liveness check and the insert run in one transaction. The session AES
    /// key is caller-derived (`AppState::session_encryption`) and never stored.
    async fn create_broker_session(
        &self,
        session_id: &str,
        runner_id: i64,
    ) -> Result<(), ControlError>;

    /// Delete a broker session owned by `runner_id`; `Forbidden` when the
    /// session belongs to another runner, `Ok(false)` when it does not
    /// exist (the broker delete contract answers 204 either way).
    async fn delete_broker_session(
        &self,
        session_id: &str,
        runner_id: i64,
    ) -> Result<bool, ControlError>;

    /// Unfinished requests whose claiming session no longer exists — either
    /// owned by a (possibly dead) runner or still bound to a dead session.
    /// Boot reconcile input: `(request_id, run_id, job_id)`.
    async fn orphaned_claims(&self) -> Result<Vec<(i64, RunId, JobId)>, ControlError>;

    /// Release a claimed request for redelivery (`release_request_for_retry`
    /// in SQL): clears the session binding, owner, start/renew stamps and
    /// timeout flag, and extends the lease. `Ok(false)` when the request is
    /// already settled or unknown.
    async fn release_claimed_request(
        &self,
        request_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError>;

    /// Settle a claimed request once (first result wins) with full
    /// `settle_request` bookkeeping in SQL: drops the deferred token
    /// request, clears the session binding and the owner session's queued
    /// job-cancellation messages, then stamps `result`/`locked_until`.
    /// Returns the attempt's `(run_id, job_id, agent_job_id)` when the
    /// request exists.
    async fn settle_request(
        &self,
        request_id: i64,
        result: ExecutionStatus,
        locked_until: &str,
    ) -> Result<Option<(RunId, JobId, uuid::Uuid)>, ControlError>;

    /// `jobs` row (`queue_kind`, `status`) for one logical job; `None` when
    /// absent. Boot reconcile classifies orphaned claims with it.
    async fn job_queue_state(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<(String, String)>, ControlError>;

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

    // ── Handler commands (one transaction each) ────
    //
    // Each method is one transaction. "Session" below means a
    // `runner_sessions` row of either protocol; "owns" means
    // `runner_sessions.runner_id`. Decisions that need Rust evaluation name
    // the shared function in `control::logic` / `control::sched` the backend
    // must call instead of re-deriving it.

    /// Pair an engine-authorized, freshly registered runner with the pending
    /// pool job it can serve. No-op unless
    /// pool assignments or strict job assignments are enabled. Otherwise, in
    /// order: mark the runner pool-proven; if the runner row is missing stop
    /// there; (pool assignments only) every assignment whose binding is
    /// stale (`at` older than the claim-binding TTL) or whose runner no
    /// longer exists loses its runner (`runner_id = NULL`), is re-marked
    /// pool-pending at `now` and bumps the released-bindings counter; then
    /// among pool-pending jobs that are in the ready queue and match the
    /// runner's capabilities, the one with the oldest pending mark (ties:
    /// earlier ready-queue position) is bound to the runner (`at = now`,
    /// `first_at` kept from an existing assignment else `now`) and its
    /// pending mark dropped.
    async fn pair_runner(&self, runner_id: i64) -> Result<(), ControlError>;

    /// Registered runners (id order) and, when `run_id` is `Some`, that
    /// run's ready-job count and the number of registered runners whose
    /// capabilities match at least one of those ready jobs. Read-only.
    async fn list_runners(&self, run_id: Option<RunId>) -> Result<RunnerListing, ControlError>;

    /// The parsed RSA public key registered for `runner_id`; `None` when the
    /// runner is unknown or registered without one.
    async fn runner_rsa_public_key(
        &self,
        runner_id: i64,
    ) -> Result<Option<preloop_gha_protocol::crypto::AgentRsaPublicKey>, ControlError>;

    /// Insert a session row for `open.runner_id` (`last_seen_at = now`).
    /// `runner_id = None` writes nothing and succeeds. When
    /// `open.require_live_runner`, `Forbidden` when `runner_id` names no
    /// registered runner — that liveness check and the insert run in one
    /// transaction, so a purge racing token validation cannot leave a
    /// session behind. When `open.verified`,
    /// fails with `Conflict` (nothing written) if the runner already owns a
    /// verified session; otherwise the new session is recorded as verified.
    /// No session key is stored: keys are derived from the cluster key and
    /// the session id by the caller.
    async fn open_runner_session(&self, open: OpenRunnerSession) -> Result<(), ControlError>;

    /// Delete a session row on the runner-facing DELETE. `caller_runner_id`
    /// is the runner behind a listen token (`None` = system/admin caller).
    /// With a runner caller: unknown session → `Ok(false)`, session owned by
    /// another runner → `Forbidden`. Otherwise the session row (and its
    /// verified mark) is deleted → `Ok(true)`. Its active request, queued
    /// messages and claimed job are deliberately left for the lease reaper
    /// (unlike `delete_session`).
    async fn close_runner_session(
        &self,
        session_id: &str,
        caller_runner_id: Option<i64>,
    ) -> Result<bool, ControlError>;

    /// Check `guard` and purge `runner_id` in one transaction (see
    /// [`PurgeGuard`]). Purging deletes the
    /// runner, its client ids, RSA key and pool-proven mark, deletes every
    /// session it owns (each like `delete_session`: active request released
    /// for retry, queued messages dropped), requeues every claimed job it
    /// owned (by an unfinished attempt's recorded owner, by a doomed
    /// session's active request, or by an assignment naming it) back to the
    /// ready queue as `queued` behind a fresh attempt, and releases every
    /// assignment still naming it (pool assignments on + job still ready →
    /// pool-pending at `now`). Returns the runtime identities of the retired
    /// attempts when a purge ran; `None` without writing when the guard
    /// refuses (`IfPhantom`: the runner is missing or owns a session).
    async fn purge_runner_guarded(
        &self,
        runner_id: i64,
        guard: PurgeGuard,
    ) -> Result<Option<Vec<uuid::Uuid>>, ControlError>;

    /// Ids of every registered ephemeral runner, ascending.
    async fn ephemeral_runner_ids(&self) -> Result<Vec<i64>, ControlError>;

    /// Ids of every registered runner named exactly `name`, ascending.
    async fn runner_ids_named(&self, name: &str) -> Result<Vec<i64>, ControlError>;

    /// The lowest-id runner named exactly `name` and its OAuth client id.
    /// When the runner has no client id, one is synthesized as
    /// `format!("{:08x}-0000-4000-8000-000000000000", runner_id as u32)` and
    /// recorded for the runner (so a later token request resolves). `None`
    /// when no runner has that name.
    async fn lookup_agent(
        &self,
        name: &str,
    ) -> Result<Option<(preloop_gha_protocol::RegisteredRunner, String)>, ControlError>;

    /// Record `client_id` as an OAuth client id of `runner_id` (upsert;
    /// a client id maps to one runner). When `pair_with_pending_job`, then
    /// pair the runner exactly like [`ControlBackend::pair_runner`], in the
    /// same transaction.
    async fn bind_runner_client(
        &self,
        runner_id: i64,
        client_id: &str,
        pair_with_pending_job: bool,
    ) -> Result<(), ControlError>;

    /// In-place runner update (`PUT …/agents/{id}`): replace the name and/or
    /// label set when supplied, keep the id. `NotFound` for an unknown
    /// runner. The returned row has `client_id: None`.
    async fn update_runner(
        &self,
        runner_id: i64,
        name: Option<String>,
        labels: Option<Vec<String>>,
    ) -> Result<RunnerRow, ControlError>;

    // ── Reaper and status ─────────────────────────────────────────────

    /// One reaper sweep, touching only rows of `sweep.runs`:
    /// 1. Starvation: first-seen marks of ready jobs of those runs that are
    ///    not in `sweep.ready` are cleared. For each `sweep.ready` job the
    ///    verdict comes from `logic::starvation_verdict` (any registered
    ///    runner's labels match its `runs-on`, first-seen mark, pool
    ///    preparing, warm window): clear the mark, set the mark (keeping an
    ///    existing one), or starve it — remove it from the ready queue, set
    ///    the job `failure`, recompute the run status and finalize the run if
    ///    every job is terminal (no dependent promotion, no concurrency
    ///    release) and report `(run, job, reason)`.
    /// 2. Per `sweep.active` attempt, timeout: started, not yet
    ///    `timeout_triggered`, and `now - started_at - paused[request]` ≥ the
    ///    job message's `timeout-minutes` (default 21600 s) → set
    ///    `timeout_triggered` and queue a job cancellation for the job's
    ///    unfinished attempt.
    /// 3. Lease: `now - last_renewed_at` ≥ `JOB_LEASE_SECONDS` → set the
    ///    attempt `result = failure`, unbind it from every session's active
    ///    request and drop its inflight marker; report it in `expired`.
    async fn reap_sweep(&self, sweep: ReapSweep) -> Result<ReapSweepOutcome, ControlError>;

    /// Status-page inputs over live control state (see [`StatusInputs`]).
    /// `stale_after` is the idle-session staleness threshold. Read-only.
    async fn status_inputs(
        &self,
        stale_after: std::time::Duration,
    ) -> Result<StatusInputs, ControlError>;

    /// Re-derive dispatch intent for every ready job after the effective
    /// scheduling config becomes known at boot: for each ready job in queue
    /// order apply the enqueue binding (config-gated and idempotent:
    /// a job with an assignment is untouched; otherwise bind it to an idle
    /// matching runner that owns a session — pool-proven only when pool
    /// assignments are on — else, with pool assignments on, mark it
    /// pool-pending).
    async fn rebuild_dispatch_intent(&self) -> Result<(), ControlError>;

    // ── Runs and jobs ─────────────────────────────────────────────────

    /// Every live (non-archived) run whose repository equals `repository`
    /// ASCII-case-insensitively, as full records, in any order.
    async fn runs_for_repository(&self, repository: &str) -> Result<Vec<RunRecord>, ControlError>;

    /// AzDO `GET …/messages` step, one transaction:
    /// 1. `verified_runner_id` set but not the session's owner → `Forbidden`.
    /// 2. Touch the session (`last_seen_at = now`).
    /// 3. Oldest queued session message → `Redeliver` (nothing else written).
    /// 4. If the session has an active request: finished or missing → unbind
    ///    it and continue; unfinished with a queued cancellation for its job
    ///    → consume that cancellation, queue a `JobCancellation` message with
    ///    body `concurrency::job_cancel_body(agent_job_id)` → `Cancel`;
    ///    otherwise → `Wait`.
    /// 5. Claim: the runner capabilities of the session's owner (unknown =
    ///    empty) choose a ready job via the claim preference
    ///    (`logic::claim_preference` over `verified_runner_id`); none →
    ///    `Wait`. Otherwise remove it from the ready queue (drop its
    ///    assignment/pending mark), set job and run `in_progress`, bind the
    ///    session's active request to the job's attempt
    ///    (`message.request_id`), stamp the attempt's owner
    ///    (`verified_runner_id`, else the session owner) and
    ///    `claimed_at`/`started_at`/`last_renewed_at = now`, and queue a
    ///    `PipelineAgentJobRequest` message carrying only that request id →
    ///    `Claimed`. The outcome also carries the post-claim ready-queue
    ///    depth and the new queue front's `runs-on` labels, so the handler
    ///    refreshes the supervisor gauges exactly like the broker claim.
    ///
    /// Message ids come from the shared session-message id sequence.
    async fn poll_azdo_session(&self, poll: AzdoPoll) -> Result<AzdoPollOutcome, ControlError>;

    /// Record a terminal job completion (the runner `completejob` path, the
    /// lease reaper and internal completions). Serializes on the run.
    ///
    /// With `settle`: the attempt is resolved by `agent_job_id` (`NotFound`
    /// if unknown), its ownership checked like `renew_broker_request`
    /// (`Forbidden`/`NotFound`); an attempt that already has a result, or
    /// is no longer inflight, returns `Unchanged(current run)` after (for the
    /// latter) recording the result; otherwise its `result = status`,
    /// `locked_until` = now + lease, and it is unbound from every session.
    ///
    /// Then: `NotFound` for an unknown run; the job must belong to the run.
    /// A job already terminal and not `cancelled` → `Unchanged(run)`.
    /// Effective status: a `failure` of a `continue-on-error` job counts as
    /// `success`; a job already `cancelled` stays `cancelled` against
    /// `success`/`failure`. Store it; upsert the job's `jobs_list` detail
    /// (conclusion; annotations, secret-masked with the run's secrets, when
    /// reported); store outputs; propagate reusable-caller outputs; recompute
    /// run status (`started_at` defaults to now; first terminal status sets
    /// `completed_at` + conclusion, and `newly_terminal_success` when it is
    /// `success`). Reported step results update the attempt's step manifest
    /// (conclusion, runner number); still-`in_progress` steps take the job
    /// status and a `finished_at`. A `failure` applies matrix fail-fast to
    /// siblings. The job leaves every queue (ready/blocked/concurrency-
    /// blocked/held). Job-level concurrency held by the job and by any
    /// finalized reusable caller is released (FIFO promotion of waiters),
    /// dependents are promoted, and every attempt of the job is settled with
    /// the effective status (token request dropped,
    /// session unbound, moot cancellation messages dropped).
    ///
    /// A run whose only concurrency is workflow-level must not need global
    /// state for a completion that leaves the run non-terminal; only the
    /// completion that finishes the run releases (and promotes) its
    /// workflow hold.
    async fn settle_job(&self, settle: SettleJob) -> Result<SettleJobOutcome, ControlError>;

    /// OIDC token inputs for the attempt behind `plan_id`: the attempt must
    /// exist and its `agent_job_id` equal `agent_job_id` (`NotFound`
    /// otherwise, also for a missing run); the job's OIDC context must exist
    /// (`Backend` error otherwise). Read-only.
    async fn oidc_grant(
        &self,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
    ) -> Result<OidcGrant, ControlError>;
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

/// Session creation input. The session AES key is NOT part of this input:
/// it is derived from the cluster key + session id and never stored.
pub(crate) struct CreateSession {
    pub(crate) runner_id: i64,
    pub(crate) protocol: SessionProtocol,
    pub(crate) client_id: Option<String>,
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
    /// The runner-protocol "plan id" — the request's `agent_job_id` in
    /// string form (derived, never a stored column). Same row as
    /// `AgentJobId`; the string key is the plan-addressed surface the
    /// runner actually sends.
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
/// PostgreSQL (shared nodes). `AppState` holds `Arc<Backend>`; the enum
/// dispatches to the two `ControlBackend` implementations under
/// `control/lite` and `control/pg`.
pub(crate) enum Backend {
    /// Single-node default: one writer on `<state_dir>/preloop.db`.
    Sqlite(super::lite::LiteBackend),
    /// Shared-node: connection pools against the `control` schema.
    Postgres(super::pg::PgBackend),
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

    /// This node process's id as stamped on its outbox rows. `None` on
    /// SQLite: one process owns the database, so there is no other node to
    /// receive events from.
    pub(crate) fn event_origin(&self) -> Option<&str> {
        match self {
            Self::Sqlite(_) => None,
            Self::Postgres(b) => Some(b.origin()),
        }
    }

    /// Notifications that another node committed events to the outbox.
    /// `None` on SQLite.
    pub(crate) fn subscribe_event_notifications(
        &self,
    ) -> Option<tokio::sync::broadcast::Receiver<String>> {
        match self {
            Self::Sqlite(_) => None,
            Self::Postgres(b) => Some(b.subscribe_event_notifications()),
        }
    }

    /// The current end of the outbox stream. SQLite returns event-id order;
    /// PostgreSQL returns its `(txid,event_id)` safe point.
    pub(crate) async fn outbox_head(&self) -> Result<Option<OutboxBookmark>, ControlError> {
        match self {
            Self::Sqlite(b) => b.outbox_head().await.map(Some),
            Self::Postgres(b) => b.outbox_head().await.map(Some),
        }
    }

    /// Up to `limit` rows after `after`, ordered by the backend safe point.
    pub(crate) async fn outbox_read(
        &self,
        after: OutboxBookmark,
        limit: usize,
    ) -> Result<Vec<OutboxRow>, ControlError> {
        match self {
            Self::Sqlite(b) => b.outbox_read(after, limit).await,
            Self::Postgres(b) => b.outbox_read(after, limit).await,
        }
    }

    /// Tell the other nodes that events were committed. A no-op on SQLite.
    pub(crate) async fn notify_events(&self) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(_) => Ok(()),
            Self::Postgres(b) => b.notify_events().await,
        }
    }

    pub(crate) async fn consume_check_run_outbox(
        &self,
        owner: &str,
        lease_for: std::time::Duration,
        limit: usize,
    ) -> Result<usize, ControlError> {
        match self {
            Self::Sqlite(b) => b.consume_check_run_outbox(owner, lease_for, limit).await,
            Self::Postgres(b) => b.consume_check_run_outbox(owner, lease_for, limit).await,
        }
    }

    pub(crate) async fn lease_check_run_updates(
        &self,
        owner: &str,
        lease_for: std::time::Duration,
        limit: usize,
    ) -> Result<Vec<CheckRunUpdate>, ControlError> {
        match self {
            Self::Sqlite(b) => b.lease_check_run_updates(owner, lease_for, limit).await,
            Self::Postgres(b) => b.lease_check_run_updates(owner, lease_for, limit).await,
        }
    }

    pub(crate) async fn set_check_run_update_id(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        version: i64,
        check_run_id: u64,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.set_check_run_update_id(owner, run_id, job_id, version, check_run_id)
                    .await
            }
            Self::Postgres(b) => {
                b.set_check_run_update_id(owner, run_id, job_id, version, check_run_id)
                    .await
            }
        }
    }

    pub(crate) async fn finish_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        version: i64,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.finish_check_run_update(owner, run_id, job_id, version)
                    .await
            }
            Self::Postgres(b) => {
                b.finish_check_run_update(owner, run_id, job_id, version)
                    .await
            }
        }
    }

    pub(crate) async fn retry_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        delay: std::time::Duration,
        permanent: bool,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.retry_check_run_update(owner, run_id, job_id, delay, permanent)
                    .await
            }
            Self::Postgres(b) => {
                b.retry_check_run_update(owner, run_id, job_id, delay, permanent)
                    .await
            }
        }
    }

    pub(crate) async fn clear_check_run_update_id(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        expected: u64,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.clear_check_run_update_id(owner, run_id, job_id, expected)
                    .await
            }
            Self::Postgres(b) => {
                b.clear_check_run_update_id(owner, run_id, job_id, expected)
                    .await
            }
        }
    }

    pub(crate) async fn defer_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        delay: std::time::Duration,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.defer_check_run_update(owner, run_id, job_id, delay).await,
            Self::Postgres(b) => b.defer_check_run_update(owner, run_id, job_id, delay).await,
        }
    }

    pub(crate) async fn renew_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        lease_for: std::time::Duration,
    ) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.renew_check_run_update(owner, run_id, job_id, lease_for)
                    .await
            }
            Self::Postgres(b) => {
                b.renew_check_run_update(owner, run_id, job_id, lease_for)
                    .await
            }
        }
    }

    /// Append a durable projection wake after a reporter stamped
    /// `reports_check_runs`; see `append_check_run_projection` on the
    /// backends.
    pub(crate) async fn append_check_run_projection(
        &self,
        run_id: RunId,
        job_id: Option<&JobId>,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.append_check_run_projection(run_id, job_id).await,
            Self::Postgres(b) => b.append_check_run_projection(run_id, job_id).await,
        }
    }
    pub(crate) async fn enqueue_check_run_update(
        &self,
        update: CheckRunUpdateInput,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.enqueue_check_run_update(update).await,
            Self::Postgres(b) => b.enqueue_check_run_update(update).await,
        }
    }

    /// `#[cfg(test)]` working-set snapshot for test assertions. Each
    /// backend rebuilds the old field names from its own tables.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) async fn test_working_set(
        &self,
    ) -> Result<super::testview::TestState, ControlError> {
        match self {
            Self::Sqlite(b) => b.test_working_set(),
            Self::Postgres(b) => b.test_working_set().await,
        }
    }
    /// Open the backend selected by `store_url`, falling back to the
    /// `PRELOOP_STORE_URL` environment variable and then to SQLite at
    /// `<state_dir>/preloop.db`. The URL goes through
    /// [`crate::store::parse_store_url`], the same parser that labels the
    /// status snapshot, so the live backend and the reported one cannot drift:
    /// `postgres://…` → PostgreSQL, `sqlite://<path>`/`sqlite:<path>`/a bare
    /// path → SQLite; an unsupported scheme is rejected.
    pub(crate) async fn open(
        store_url: Option<&str>,
        state_dir: &std::path::Path,
        pool_assignments_enabled: bool,
        require_job_assignments: bool,
        runner_liveness_timeout: std::time::Duration,
    ) -> Result<Self, ControlError> {
        let raw = match store_url {
            Some(value) if !value.trim().is_empty() => value.to_owned(),
            _ => std::env::var(crate::store::STORE_URL_ENV).unwrap_or_default(),
        };
        match crate::store::parse_store_url(&raw).map_err(ControlError::backend)? {
            crate::store::StoreUrl::Postgres(url) => {
                let backend = super::pg::PgBackend::connect(
                    &url,
                    pool_assignments_enabled,
                    require_job_assignments,
                    runner_liveness_timeout,
                )
                .await?;
                Ok(Self::Postgres(backend))
            }
            crate::store::StoreUrl::Sqlite(path) => {
                let path = if path.as_os_str().is_empty() {
                    state_dir.join("preloop.db")
                } else {
                    path
                };
                let backend = super::lite::LiteBackend::open(
                    &path,
                    pool_assignments_enabled,
                    require_job_assignments,
                    runner_liveness_timeout,
                )?;
                Ok(Self::Sqlite(backend))
            }
        }
    }

    /// Resolve a request's `(request_id, run_id)` from its `agent_job_id`.
    /// Thin alias over [`ControlBackend::request`] — the broker acquire path
    /// wants the bare `(request_id, run_id)`, not the whole record.
    pub(crate) async fn find_request_by_agent_job_id(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<(i64, RunId)>, ControlError> {
        match self.request(RequestKey::AgentJobId(agent_job_id)).await {
            Ok(record) => Ok(Some((record.request_id, record.run_id))),
            Err(ControlError::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
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

    /// Install the shared environment-rules resolver once bootstrap builds
    /// it (`AppState`'s `EnvironmentResolver`: TOML fallback + GitHub fetch).
    /// The backends consult it inside their promotion and reaper
    /// transactions, where the job rows live.
    pub(crate) fn set_environment_resolver(
        &self,
        resolver: std::sync::Arc<crate::environment_resolver::EnvironmentResolver>,
    ) {
        match self {
            Self::Sqlite(backend) => backend.set_environment_resolver(resolver),
            Self::Postgres(backend) => backend.set_environment_resolver(resolver),
        }
    }

    /// Share the co-hosted runner pool's status handle. The backend reads
    /// its advertised labels inside submit and promotion transactions to
    /// fail a `runs-on` the pool can never satisfy.
    pub(crate) fn set_pool_status(&self, status: preloop_observability::status::PoolStatus) {
        match self {
            Self::Sqlite(backend) => backend.set_pool_status(status),
            Self::Postgres(backend) => backend.set_pool_status(status),
        }
    }

    /// Current scheduling config — the counterpart to [`Backend::set_config`]
    /// for callers that need to flip one flag without knowing the rest.
    #[cfg(any(test, feature = "test-support"))]
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
    async fn allocate_run_number(
        &self,
        namespace_id: &str,
        repository: &str,
        workflow_path: &str,
    ) -> Result<u64, ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.allocate_run_number(namespace_id, repository, workflow_path)
                    .await
            }
            Self::Postgres(b) => {
                b.allocate_run_number(namespace_id, repository, workflow_path)
                    .await
            }
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
    async fn record_token_request(
        &self,
        run_id: RunId,
        request_id: i64,
        token_request: &crate::models::GitHubTokenRequest,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.record_token_request(run_id, request_id, token_request)
                    .await
            }
            Self::Postgres(b) => {
                b.record_token_request(run_id, request_id, token_request)
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
    async fn set_fork_approval(&self, update: ForkApprovalStamp) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.set_fork_approval(update).await,
            Self::Postgres(b) => b.set_fork_approval(update).await,
        }
    }
    async fn set_environment_gate(
        &self,
        run_id: RunId,
        job_id: &JobId,
        gate: Option<EnvironmentGateState>,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.set_environment_gate(run_id, job_id, gate).await,
            Self::Postgres(b) => b.set_environment_gate(run_id, job_id, gate).await,
        }
    }
    async fn record_environment_approval(
        &self,
        approval: EnvironmentApproval,
    ) -> Result<EnvironmentApprovalOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.record_environment_approval(approval).await,
            Self::Postgres(b) => b.record_environment_approval(approval).await,
        }
    }
    async fn set_reports_check_runs(
        &self,
        run_id: RunId,
        reported: bool,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.set_reports_check_runs(run_id, reported).await,
            Self::Postgres(b) => b.set_reports_check_runs(run_id, reported).await,
        }
    }
    async fn promote_ready_jobs(&self, run: Option<RunId>) -> Result<PromoteOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.promote_ready_jobs(run).await,
            Self::Postgres(b) => b.promote_ready_jobs(run).await,
        }
    }
    fn set_environment_resolver(
        &self,
        resolver: std::sync::Arc<crate::environment_resolver::EnvironmentResolver>,
    ) {
        match self {
            Self::Sqlite(b) => b.set_environment_resolver(resolver),
            Self::Postgres(b) => b.set_environment_resolver(resolver),
        }
    }
    async fn pending_environment_approvals(
        &self,
        run_id: Option<RunId>,
    ) -> Result<Vec<PendingEnvironmentApproval>, ControlError> {
        match self {
            Self::Sqlite(b) => b.pending_environment_approvals(run_id).await,
            Self::Postgres(b) => b.pending_environment_approvals(run_id).await,
        }
    }
    async fn pending_environment_approval_for_check_run(
        &self,
        check_run_id: u64,
    ) -> Result<Option<PendingEnvironmentApproval>, ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.pending_environment_approval_for_check_run(check_run_id)
                    .await
            }
            Self::Postgres(b) => {
                b.pending_environment_approval_for_check_run(check_run_id)
                    .await
            }
        }
    }
    async fn mark_environment_approval_announced(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.mark_environment_approval_announced(run_id, job_id).await,
            Self::Postgres(b) => b.mark_environment_approval_announced(run_id, job_id).await,
        }
    }
    async fn job_deployment_id(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<u64>, ControlError> {
        match self {
            Self::Sqlite(b) => b.job_deployment_id(run_id, job_id).await,
            Self::Postgres(b) => b.job_deployment_id(run_id, job_id).await,
        }
    }
    async fn set_job_deployment(
        &self,
        run_id: RunId,
        job_id: &JobId,
        deployment_id: u64,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.set_job_deployment(run_id, job_id, deployment_id).await,
            Self::Postgres(b) => b.set_job_deployment(run_id, job_id, deployment_id).await,
        }
    }
    async fn environment_gate(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<EnvironmentGateRead>, ControlError> {
        match self {
            Self::Sqlite(b) => b.environment_gate(run_id, job_id).await,
            Self::Postgres(b) => b.environment_gate(run_id, job_id).await,
        }
    }
    async fn environment_deployment(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<EnvironmentDeploymentRow>, ControlError> {
        match self {
            Self::Sqlite(b) => b.environment_deployment(run_id, job_id).await,
            Self::Postgres(b) => b.environment_deployment(run_id, job_id).await,
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
    async fn purge_runner(&self, runner_id: i64) -> Result<Vec<uuid::Uuid>, ControlError> {
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
            Self::Sqlite(b) => ControlBackend::run_record(b, run_id).await,
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
    async fn archive_finished_runs(&self, limit: usize) -> Result<Vec<RunId>, ControlError> {
        match self {
            Self::Sqlite(b) => b.archive_finished_runs(limit).await,
            Self::Postgres(b) => b.archive_finished_runs(limit).await,
        }
    }
    async fn expired_terminal_runs(
        &self,
        cutoff_us: i64,
        limit: usize,
    ) -> Result<Vec<RunId>, ControlError> {
        match self {
            Self::Sqlite(b) => b.expired_terminal_runs(cutoff_us, limit).await,
            Self::Postgres(b) => b.expired_terminal_runs(cutoff_us, limit).await,
        }
    }
    async fn delete_expired_run(&self, run_id: RunId) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.delete_expired_run(run_id).await,
            Self::Postgres(b) => b.delete_expired_run(run_id).await,
        }
    }
    async fn expire_fork_approvals(
        &self,
        expired_before_unix_nanos: i64,
    ) -> Result<Vec<RunId>, ControlError> {
        match self {
            Self::Sqlite(b) => b.expire_fork_approvals(expired_before_unix_nanos).await,
            Self::Postgres(b) => b.expire_fork_approvals(expired_before_unix_nanos).await,
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
        events: &[NdjsonEvent],
    ) -> Result<(i32, Vec<preloop_gha_protocol::azdo::TimelineRecord>), ControlError> {
        match self {
            Self::Sqlite(b) => b.patch_timeline(timeline_key, records, events).await,
            Self::Postgres(b) => b.patch_timeline(timeline_key, records, events).await,
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
    async fn prune_outbox(
        &self,
        older_than: std::time::Duration,
        limit: usize,
    ) -> Result<u64, ControlError> {
        match self {
            Self::Sqlite(b) => b.prune_outbox(older_than, limit).await,
            Self::Postgres(b) => b.prune_outbox(older_than, limit).await,
        }
    }
    async fn consume_check_run_outbox(
        &self,
        owner: &str,
        lease_for: std::time::Duration,
        limit: usize,
    ) -> Result<usize, ControlError> {
        match self {
            Self::Sqlite(b) => b.consume_check_run_outbox(owner, lease_for, limit).await,
            Self::Postgres(b) => b.consume_check_run_outbox(owner, lease_for, limit).await,
        }
    }
    async fn lease_check_run_updates(
        &self,
        owner: &str,
        lease_for: std::time::Duration,
        limit: usize,
    ) -> Result<Vec<CheckRunUpdate>, ControlError> {
        match self {
            Self::Sqlite(b) => b.lease_check_run_updates(owner, lease_for, limit).await,
            Self::Postgres(b) => b.lease_check_run_updates(owner, lease_for, limit).await,
        }
    }
    async fn set_check_run_update_id(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        version: i64,
        check_run_id: u64,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.set_check_run_update_id(owner, run_id, job_id, version, check_run_id)
                    .await
            }
            Self::Postgres(b) => {
                b.set_check_run_update_id(owner, run_id, job_id, version, check_run_id)
                    .await
            }
        }
    }
    async fn finish_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        version: i64,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.finish_check_run_update(owner, run_id, job_id, version)
                    .await
            }
            Self::Postgres(b) => {
                b.finish_check_run_update(owner, run_id, job_id, version)
                    .await
            }
        }
    }
    async fn retry_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        delay: std::time::Duration,
        permanent: bool,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.retry_check_run_update(owner, run_id, job_id, delay, permanent)
                    .await
            }
            Self::Postgres(b) => {
                b.retry_check_run_update(owner, run_id, job_id, delay, permanent)
                    .await
            }
        }
    }
    async fn clear_check_run_update_id(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        expected: u64,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.clear_check_run_update_id(owner, run_id, job_id, expected)
                    .await
            }
            Self::Postgres(b) => {
                b.clear_check_run_update_id(owner, run_id, job_id, expected)
                    .await
            }
        }
    }
    async fn enqueue_check_run_update(
        &self,
        update: CheckRunUpdateInput,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.enqueue_check_run_update(update).await,
            Self::Postgres(b) => b.enqueue_check_run_update(update).await,
        }
    }
    async fn defer_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        delay: std::time::Duration,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.defer_check_run_update(owner, run_id, job_id, delay).await,
            Self::Postgres(b) => b.defer_check_run_update(owner, run_id, job_id, delay).await,
        }
    }
    async fn renew_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        lease_for: std::time::Duration,
    ) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.renew_check_run_update(owner, run_id, job_id, lease_for)
                    .await
            }
            Self::Postgres(b) => {
                b.renew_check_run_update(owner, run_id, job_id, lease_for)
                    .await
            }
        }
    }
    async fn append_check_run_projection(
        &self,
        run_id: RunId,
        job_id: Option<&JobId>,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.append_check_run_projection(run_id, job_id).await,
            Self::Postgres(b) => b.append_check_run_projection(run_id, job_id).await,
        }
    }
    async fn outbox_head(&self) -> Result<Option<OutboxBookmark>, ControlError> {
        match self {
            Self::Sqlite(b) => b.outbox_head().await.map(Some),
            Self::Postgres(b) => b.outbox_head().await.map(Some),
        }
    }
    async fn outbox_read(
        &self,
        after: OutboxBookmark,
        limit: usize,
    ) -> Result<Vec<OutboxRow>, ControlError> {
        match self {
            Self::Sqlite(b) => b.outbox_read(after, limit).await,
            Self::Postgres(b) => b.outbox_read(after, limit).await,
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

    async fn report_steps(
        &self,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
        steps: Vec<serde_json::Value>,
    ) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => b.report_steps(plan_id, agent_job_id, steps).await,
            Self::Postgres(b) => b.report_steps(plan_id, agent_job_id, steps).await,
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

    async fn attempt_repository(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<String>, ControlError> {
        match self {
            Self::Sqlite(b) => b.attempt_repository(agent_job_id).await,
            Self::Postgres(b) => b.attempt_repository(agent_job_id).await,
        }
    }

    async fn attempt_in_run(
        &self,
        run_id: RunId,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
    ) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => b.attempt_in_run(run_id, plan_id, agent_job_id).await,
            Self::Postgres(b) => b.attempt_in_run(run_id, plan_id, agent_job_id).await,
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

    async fn run_job_ids(&self, run_id: RunId) -> Result<Vec<String>, ControlError> {
        match self {
            Self::Sqlite(b) => b.run_job_ids(run_id).await,
            Self::Postgres(b) => b.run_job_ids(run_id).await,
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
    async fn sole_inflight_request(
        &self,
    ) -> Result<Option<(i64, RunId, JobId, uuid::Uuid)>, ControlError> {
        match self {
            Self::Sqlite(b) => b.sole_inflight_request().await,
            Self::Postgres(b) => b.sole_inflight_request().await,
        }
    }

    async fn run_in_concurrency(&self, run_id: RunId) -> Result<RunConcurrency, ControlError> {
        match self {
            Self::Sqlite(b) => b.run_in_concurrency(run_id).await,
            Self::Postgres(b) => b.run_in_concurrency(run_id).await,
        }
    }
    async fn callback_job(
        &self,
        plan_id: &str,
        timeline_id: Option<uuid::Uuid>,
        agent_job_id: Option<uuid::Uuid>,
    ) -> Result<Option<CallbackJob>, ControlError> {
        match self {
            Self::Sqlite(b) => b.callback_job(plan_id, timeline_id, agent_job_id).await,
            Self::Postgres(b) => b.callback_job(plan_id, timeline_id, agent_job_id).await,
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
    ) -> Result<Option<(Option<i64>, Option<i64>, bool)>, ControlError> {
        match self {
            Self::Sqlite(b) => b.request_owner(request_id).await,
            Self::Postgres(b) => b.request_owner(request_id).await,
        }
    }
    async fn renew_broker_request(
        &self,
        agent_job_id: uuid::Uuid,
        runner_id: i64,
        locked_until: &str,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.renew_broker_request(agent_job_id, runner_id, locked_until)
                    .await
            }
            Self::Postgres(b) => {
                b.renew_broker_request(agent_job_id, runner_id, locked_until)
                    .await
            }
        }
    }
    async fn create_broker_session(
        &self,
        session_id: &str,
        runner_id: i64,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.create_broker_session(session_id, runner_id).await,
            Self::Postgres(b) => b.create_broker_session(session_id, runner_id).await,
        }
    }
    async fn run_dispatch_info(
        &self,
        run_id: RunId,
    ) -> Result<Option<RunDispatchInfo>, ControlError> {
        match self {
            Self::Sqlite(b) => b.run_dispatch_info(run_id).await,
            Self::Postgres(b) => b.run_dispatch_info(run_id).await,
        }
    }
    async fn submission_fields(
        &self,
        run_id: RunId,
    ) -> Result<Option<SubmissionFields>, ControlError> {
        match self {
            Self::Sqlite(b) => b.submission_fields(run_id).await,
            Self::Postgres(b) => b.submission_fields(run_id).await,
        }
    }
    async fn job_check_run_id(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<u64>, ControlError> {
        match self {
            Self::Sqlite(b) => b.job_check_run_id(run_id, job_id).await,
            Self::Postgres(b) => b.job_check_run_id(run_id, job_id).await,
        }
    }
    async fn run_job_statuses(
        &self,
        run_id: RunId,
    ) -> Result<Option<Vec<(JobId, ExecutionStatus)>>, ControlError> {
        match self {
            Self::Sqlite(b) => b.run_job_statuses(run_id).await,
            Self::Postgres(b) => b.run_job_statuses(run_id).await,
        }
    }
    async fn live_log_key(
        &self,
        run_id: RunId,
        job_id: &str,
    ) -> Result<Option<(String, bool)>, ControlError> {
        match self {
            Self::Sqlite(b) => b.live_log_key(run_id, job_id).await,
            Self::Postgres(b) => b.live_log_key(run_id, job_id).await,
        }
    }
    async fn run_requests(
        &self,
        run_id: RunId,
    ) -> Result<Vec<TaskAgentJobRequestRecord>, ControlError> {
        match self {
            Self::Sqlite(b) => b.run_requests(run_id).await,
            Self::Postgres(b) => b.run_requests(run_id).await,
        }
    }
    async fn run_step_manifests(
        &self,
        run_id: RunId,
    ) -> Result<BTreeMap<uuid::Uuid, Vec<crate::models::StepRecord>>, ControlError> {
        match self {
            Self::Sqlite(b) => b.run_step_manifests(run_id).await,
            Self::Postgres(b) => b.run_step_manifests(run_id).await,
        }
    }
    async fn issue_debug_token(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<(RunId, String), ControlError> {
        match self {
            Self::Sqlite(b) => b.issue_debug_token(agent_job_id).await,
            Self::Postgres(b) => b.issue_debug_token(agent_job_id).await,
        }
    }
    async fn check_run_target(
        &self,
        check_run_id: u64,
        repository: &str,
        head_sha: Option<&str>,
        job_name: Option<&str>,
        details_run_id: Option<RunId>,
    ) -> Result<Option<(RunId, JobId)>, ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.check_run_target(check_run_id, repository, head_sha, job_name, details_run_id)
                    .await
            }
            Self::Postgres(b) => {
                b.check_run_target(check_run_id, repository, head_sha, job_name, details_run_id)
                    .await
            }
        }
    }
    async fn published_run(
        &self,
        repository: &str,
        sha: &str,
        workflow_path: &str,
    ) -> Result<Option<RunId>, ControlError> {
        match self {
            Self::Sqlite(b) => b.published_run(repository, sha, workflow_path).await,
            Self::Postgres(b) => b.published_run(repository, sha, workflow_path).await,
        }
    }
    async fn run_for_webhook_delivery(
        &self,
        delivery_id: &str,
        workflow_path: &str,
    ) -> Result<Option<RunId>, ControlError> {
        match self {
            Self::Sqlite(b) => b.run_for_webhook_delivery(delivery_id, workflow_path).await,
            Self::Postgres(b) => b.run_for_webhook_delivery(delivery_id, workflow_path).await,
        }
    }
    async fn run_held(&self, run_id: RunId) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => b.run_held(run_id).await,
            Self::Postgres(b) => b.run_held(run_id).await,
        }
    }
    async fn submission_json_for_attempt(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<String>, ControlError> {
        match self {
            Self::Sqlite(b) => b.submission_json_for_attempt(agent_job_id).await,
            Self::Postgres(b) => b.submission_json_for_attempt(agent_job_id).await,
        }
    }
    async fn sweep_stale_bindings(&self) -> Result<usize, ControlError> {
        match self {
            Self::Sqlite(b) => b.sweep_stale_bindings().await,
            Self::Postgres(b) => b.sweep_stale_bindings().await,
        }
    }
    async fn settle_request(
        &self,
        request_id: i64,
        result: ExecutionStatus,
        locked_until: &str,
    ) -> Result<Option<(RunId, JobId, uuid::Uuid)>, ControlError> {
        match self {
            Self::Sqlite(b) => b.settle_request(request_id, result, locked_until).await,
            Self::Postgres(b) => b.settle_request(request_id, result, locked_until).await,
        }
    }
    async fn job_queue_state(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<(String, String)>, ControlError> {
        match self {
            Self::Sqlite(b) => b.job_queue_state(run_id, job_id).await,
            Self::Postgres(b) => b.job_queue_state(run_id, job_id).await,
        }
    }
    async fn delete_broker_session(
        &self,
        session_id: &str,
        runner_id: i64,
    ) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => b.delete_broker_session(session_id, runner_id).await,
            Self::Postgres(b) => b.delete_broker_session(session_id, runner_id).await,
        }
    }
    async fn orphaned_claims(&self) -> Result<Vec<(i64, RunId, JobId)>, ControlError> {
        match self {
            Self::Sqlite(b) => b.orphaned_claims().await,
            Self::Postgres(b) => b.orphaned_claims().await,
        }
    }
    async fn release_claimed_request(
        &self,
        request_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => b.release_claimed_request(request_id, locked_until).await,
            Self::Postgres(b) => b.release_claimed_request(request_id, locked_until).await,
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
    async fn pair_runner(&self, runner_id: i64) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.pair_runner(runner_id).await,
            Self::Postgres(b) => b.pair_runner(runner_id).await,
        }
    }
    async fn list_runners(&self, run_id: Option<RunId>) -> Result<RunnerListing, ControlError> {
        match self {
            Self::Sqlite(b) => b.list_runners(run_id).await,
            Self::Postgres(b) => b.list_runners(run_id).await,
        }
    }
    async fn runner_rsa_public_key(
        &self,
        runner_id: i64,
    ) -> Result<Option<preloop_gha_protocol::crypto::AgentRsaPublicKey>, ControlError> {
        match self {
            Self::Sqlite(b) => b.runner_rsa_public_key(runner_id).await,
            Self::Postgres(b) => b.runner_rsa_public_key(runner_id).await,
        }
    }
    async fn open_runner_session(&self, open: OpenRunnerSession) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.open_runner_session(open).await,
            Self::Postgres(b) => b.open_runner_session(open).await,
        }
    }
    async fn close_runner_session(
        &self,
        session_id: &str,
        caller_runner_id: Option<i64>,
    ) -> Result<bool, ControlError> {
        match self {
            Self::Sqlite(b) => b.close_runner_session(session_id, caller_runner_id).await,
            Self::Postgres(b) => b.close_runner_session(session_id, caller_runner_id).await,
        }
    }
    async fn purge_runner_guarded(
        &self,
        runner_id: i64,
        guard: PurgeGuard,
    ) -> Result<Option<Vec<uuid::Uuid>>, ControlError> {
        match self {
            Self::Sqlite(b) => b.purge_runner_guarded(runner_id, guard).await,
            Self::Postgres(b) => b.purge_runner_guarded(runner_id, guard).await,
        }
    }
    async fn ephemeral_runner_ids(&self) -> Result<Vec<i64>, ControlError> {
        match self {
            Self::Sqlite(b) => b.ephemeral_runner_ids().await,
            Self::Postgres(b) => b.ephemeral_runner_ids().await,
        }
    }
    async fn runner_ids_named(&self, name: &str) -> Result<Vec<i64>, ControlError> {
        match self {
            Self::Sqlite(b) => b.runner_ids_named(name).await,
            Self::Postgres(b) => b.runner_ids_named(name).await,
        }
    }
    async fn lookup_agent(
        &self,
        name: &str,
    ) -> Result<Option<(preloop_gha_protocol::RegisteredRunner, String)>, ControlError> {
        match self {
            Self::Sqlite(b) => b.lookup_agent(name).await,
            Self::Postgres(b) => b.lookup_agent(name).await,
        }
    }
    async fn bind_runner_client(
        &self,
        runner_id: i64,
        client_id: &str,
        pair_with_pending_job: bool,
    ) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => {
                b.bind_runner_client(runner_id, client_id, pair_with_pending_job)
                    .await
            }
            Self::Postgres(b) => {
                b.bind_runner_client(runner_id, client_id, pair_with_pending_job)
                    .await
            }
        }
    }
    async fn update_runner(
        &self,
        runner_id: i64,
        name: Option<String>,
        labels: Option<Vec<String>>,
    ) -> Result<RunnerRow, ControlError> {
        match self {
            Self::Sqlite(b) => b.update_runner(runner_id, name, labels).await,
            Self::Postgres(b) => b.update_runner(runner_id, name, labels).await,
        }
    }
    async fn reap_sweep(&self, sweep: ReapSweep) -> Result<ReapSweepOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.reap_sweep(sweep).await,
            Self::Postgres(b) => b.reap_sweep(sweep).await,
        }
    }
    async fn status_inputs(
        &self,
        stale_after: std::time::Duration,
    ) -> Result<StatusInputs, ControlError> {
        match self {
            Self::Sqlite(b) => b.status_inputs(stale_after).await,
            Self::Postgres(b) => b.status_inputs(stale_after).await,
        }
    }
    async fn rebuild_dispatch_intent(&self) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.rebuild_dispatch_intent().await,
            Self::Postgres(b) => b.rebuild_dispatch_intent().await,
        }
    }
    async fn runs_for_repository(&self, repository: &str) -> Result<Vec<RunRecord>, ControlError> {
        match self {
            Self::Sqlite(b) => b.runs_for_repository(repository).await,
            Self::Postgres(b) => b.runs_for_repository(repository).await,
        }
    }
    async fn poll_azdo_session(&self, poll: AzdoPoll) -> Result<AzdoPollOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.poll_azdo_session(poll).await,
            Self::Postgres(b) => b.poll_azdo_session(poll).await,
        }
    }
    async fn settle_job(&self, settle: SettleJob) -> Result<SettleJobOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.settle_job(settle).await,
            Self::Postgres(b) => b.settle_job(settle).await,
        }
    }
    async fn oidc_grant(
        &self,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
    ) -> Result<OidcGrant, ControlError> {
        match self {
            Self::Sqlite(b) => b.oidc_grant(plan_id, agent_job_id).await,
            Self::Postgres(b) => b.oidc_grant(plan_id, agent_job_id).await,
        }
    }
}
