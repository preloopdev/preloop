//! `impl ControlBackend for LiteBackend`: the trait delegates to the
//! inherent methods; the only adaptation is `run_record` (inherent returns
//! `Option` for internal callers, the trait's contract is `NotFound` on a
//! missing run).

use super::LiteBackend;
use crate::SchedulingOutcome;
use crate::control::backend::*;
use crate::control::types::*;
use crate::models::*;
use preloop_gha_protocol::*;
use std::collections::{BTreeMap, BTreeSet};

#[async_trait::async_trait]
impl ControlBackend for LiteBackend {
    async fn submit_run(&self, submit: SubmitRun) -> Result<SubmitOutcome, ControlError> {
        self.submit_run(submit).await
    }
    async fn allocate_run_number(
        &self,
        namespace_id: &str,
        repository: &str,
        workflow_path: &str,
    ) -> Result<u64, ControlError> {
        self.allocate_run_number(namespace_id, repository, workflow_path)
            .await
    }
    async fn poll_session(&self, poll: PollRequest) -> Result<PollOutcome, ControlError> {
        self.poll_session(poll).await
    }
    async fn acquire_context(&self, request_id: i64) -> Result<AcquireContext, ControlError> {
        self.acquire_context(request_id).await
    }
    async fn acquire_for_runner(
        &self,
        request_id: i64,
        runner_id: i64,
    ) -> Result<AcquireContext, ControlError> {
        self.acquire_for_runner(request_id, runner_id).await
    }
    async fn record_token_request(
        &self,
        run_id: RunId,
        request_id: i64,
        token_request: &crate::models::GitHubTokenRequest,
    ) -> Result<(), ControlError> {
        self.record_token_request(run_id, request_id, token_request)
            .await
    }
    async fn complete_job(
        &self,
        completion: JobCompletionInput,
    ) -> Result<CompleteOutcome, ControlError> {
        self.complete_job(completion).await
    }
    async fn cancel_run(
        &self,
        run_id: RunId,
        reason: Option<String>,
    ) -> Result<CancelOutcome, ControlError> {
        self.cancel_run(run_id, reason).await
    }
    async fn cancel_job(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<CancelOutcome, ControlError> {
        self.cancel_job(run_id, job_id).await
    }
    async fn set_fork_approval(&self, update: ForkApprovalStamp) -> Result<(), ControlError> {
        self.set_fork_approval(update).await
    }
    async fn set_environment_gate(
        &self,
        run_id: RunId,
        job_id: &JobId,
        gate: Option<EnvironmentGateState>,
    ) -> Result<(), ControlError> {
        self.set_environment_gate(run_id, job_id, gate).await
    }
    async fn record_environment_approval(
        &self,
        approval: EnvironmentApproval,
    ) -> Result<EnvironmentApprovalOutcome, ControlError> {
        self.record_environment_approval(approval).await
    }
    async fn set_reports_check_runs(
        &self,
        run_id: RunId,
        reported: bool,
    ) -> Result<(), ControlError> {
        self.set_reports_check_runs(run_id, reported).await
    }
    async fn promote_ready_jobs(&self, run: Option<RunId>) -> Result<PromoteOutcome, ControlError> {
        self.promote_ready_jobs(run).await
    }
    fn set_environment_resolver(
        &self,
        resolver: std::sync::Arc<crate::environment_resolver::EnvironmentResolver>,
    ) {
        LiteBackend::set_environment_resolver(self, resolver);
    }
    async fn environment_approvals(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Vec<EnvironmentApprovalAudit>, ControlError> {
        self.environment_approvals(run_id, job_id).await
    }
    async fn pending_environment_approvals(
        &self,
        run_id: Option<RunId>,
    ) -> Result<Vec<PendingEnvironmentApproval>, ControlError> {
        self.pending_environment_approvals(run_id).await
    }
    async fn mark_environment_approval_announced(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<(), ControlError> {
        self.mark_environment_approval_announced(run_id, job_id)
            .await
    }
    async fn job_deployment_id(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<u64>, ControlError> {
        self.job_deployment_id(run_id, job_id).await
    }
    async fn set_job_deployment(
        &self,
        run_id: RunId,
        job_id: &JobId,
        deployment_id: u64,
    ) -> Result<(), ControlError> {
        self.set_job_deployment(run_id, job_id, deployment_id).await
    }
    async fn pending_environment_approval_for_check_run(
        &self,
        check_run_id: u64,
    ) -> Result<Option<PendingEnvironmentApproval>, ControlError> {
        self.pending_environment_approval_for_check_run(check_run_id)
            .await
    }
    async fn environment_gate(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<EnvironmentGateRead>, ControlError> {
        self.environment_gate(run_id, job_id).await
    }
    async fn environment_deployment(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<EnvironmentDeploymentRow>, ControlError> {
        self.environment_deployment(run_id, job_id).await
    }
    async fn renew_request(
        &self,
        request_id: i64,
        runner_id: i64,
    ) -> Result<TaskAgentJobRequestRecord, ControlError> {
        self.renew_request(request_id, runner_id).await
    }
    async fn release_request(&self, request_id: i64) -> Result<(), ControlError> {
        self.release_request(request_id).await
    }
    async fn register_runner(&self, reg: RegisterRunner) -> Result<RunnerRow, ControlError> {
        self.register_runner(reg).await
    }
    async fn create_session(&self, session: CreateSession) -> Result<SessionRow, ControlError> {
        self.create_session(session).await
    }
    async fn delete_session(&self, session_id: &str) -> Result<(), ControlError> {
        self.delete_session(session_id).await
    }
    async fn purge_runner(&self, runner_id: i64) -> Result<Vec<uuid::Uuid>, ControlError> {
        self.purge_runner(runner_id).await
    }
    async fn claim_expansion(&self) -> Result<Option<ExpansionClaim>, ControlError> {
        self.claim_expansion().await
    }
    async fn apply_expansion(
        &self,
        claim: ExpansionApply,
    ) -> Result<SchedulingOutcome, ControlError> {
        self.apply_expansion(claim).await
    }
    async fn reconcile_on_boot(&self) -> Result<ReconcileOutcome, ControlError> {
        self.reconcile_on_boot().await
    }
    async fn run_record(&self, run_id: RunId) -> Result<RunRecord, ControlError> {
        self.run_record(run_id)
            .await?
            .ok_or_else(|| ControlError::NotFound(format!("run {run_id}")))
    }
    async fn list_runs(&self, filter: RunListFilter) -> Result<Vec<RunRecord>, ControlError> {
        self.list_runs(filter).await
    }
    async fn terminal_jobs(
        &self,
    ) -> Result<std::collections::BTreeSet<(RunId, JobId)>, ControlError> {
        self.terminal_jobs().await
    }
    async fn archive_finished_runs(&self, limit: usize) -> Result<Vec<RunId>, ControlError> {
        self.archive_finished_runs(limit).await
    }
    async fn expired_terminal_runs(
        &self,
        cutoff_us: i64,
        limit: usize,
    ) -> Result<Vec<RunId>, ControlError> {
        self.expired_terminal_runs(cutoff_us, limit).await
    }
    async fn delete_expired_run(&self, run_id: RunId) -> Result<(), ControlError> {
        self.delete_expired_run(run_id).await
    }
    async fn expire_fork_approvals(
        &self,
        expired_before_unix_nanos: i64,
    ) -> Result<Vec<RunId>, ControlError> {
        self.expire_fork_approvals(expired_before_unix_nanos).await
    }
    async fn append_event(&self, event: &NdjsonEvent) -> Result<(), ControlError> {
        self.append_event(event).await
    }
    async fn request(&self, key: RequestKey) -> Result<TaskAgentJobRequestRecord, ControlError> {
        self.request(key).await
    }
    async fn set_push_state(&self, run_id: RunId, state: PushState) -> Result<(), ControlError> {
        self.set_push_state(run_id, state).await
    }
    async fn artifact_scopes(
        &self,
        plan_ids: &[String],
    ) -> Result<BTreeMap<String, RunId>, ControlError> {
        self.artifact_scopes(plan_ids).await
    }
    async fn queue_stats(&self) -> Result<QueueStats, ControlError> {
        self.queue_stats().await
    }
    async fn run_event_snapshot(
        &self,
        run_id: RunId,
    ) -> Result<Vec<serde_json::Value>, ControlError> {
        self.run_event_snapshot(run_id).await
    }
    async fn artifact_catalog(
        &self,
        run_id: Option<RunId>,
    ) -> Result<Vec<ArtifactCatalogRow>, ControlError> {
        self.artifact_catalog(run_id).await
    }
    async fn artifact_by_public_id(
        &self,
        public_id: &str,
    ) -> Result<Option<ArtifactCatalogRow>, ControlError> {
        self.artifact_by_public_id(public_id).await
    }
    async fn put_artifact_catalog(&self, row: NewArtifactRow) -> Result<(), ControlError> {
        self.put_artifact_catalog(row).await
    }
    async fn create_log(&self, plan_id: &str) -> Result<i64, ControlError> {
        self.create_log(plan_id).await
    }
    async fn ensure_key_fingerprint(&self, fingerprint: &str) -> Result<(), ControlError> {
        self.ensure_key_fingerprint(fingerprint).await
    }
    async fn touch_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionProtocol>, ControlError> {
        self.touch_session(session_id).await
    }
    async fn session_owner(
        &self,
        session_id: &str,
    ) -> Result<Option<(i64, crate::models::RunnerCapabilities)>, ControlError> {
        self.session_owner(session_id).await
    }
    async fn patch_timeline(
        &self,
        timeline_key: &str,
        records: Vec<preloop_gha_protocol::azdo::TimelineRecord>,
        events: &[NdjsonEvent],
    ) -> Result<(i32, Vec<preloop_gha_protocol::azdo::TimelineRecord>), ControlError> {
        self.patch_timeline(timeline_key, records, events).await
    }
    async fn get_timeline(
        &self,
        timeline_key: &str,
        skip: usize,
        top: usize,
    ) -> Result<(i32, Vec<preloop_gha_protocol::azdo::TimelineRecord>), ControlError> {
        self.get_timeline(timeline_key, skip, top).await
    }
    async fn prune_timelines(&self, before_us: i64) -> Result<u64, ControlError> {
        self.prune_timelines(before_us).await
    }
    async fn prune_outbox(
        &self,
        older_than: std::time::Duration,
        limit: usize,
    ) -> Result<u64, ControlError> {
        self.prune_outbox(older_than, limit).await
    }
    async fn consume_check_run_outbox(
        &self,
        owner: &str,
        lease_for: std::time::Duration,
        limit: usize,
    ) -> Result<usize, ControlError> {
        self.consume_check_run_outbox(owner, lease_for, limit).await
    }
    async fn lease_check_run_updates(
        &self,
        owner: &str,
        lease_for: std::time::Duration,
        limit: usize,
    ) -> Result<Vec<CheckRunUpdate>, ControlError> {
        self.lease_check_run_updates(owner, lease_for, limit).await
    }
    async fn set_check_run_update_id(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        version: i64,
        check_run_id: u64,
    ) -> Result<(), ControlError> {
        self.set_check_run_update_id(owner, run_id, job_id, version, check_run_id)
            .await
    }
    async fn finish_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        version: i64,
    ) -> Result<(), ControlError> {
        self.finish_check_run_update(owner, run_id, job_id, version)
            .await
    }
    async fn retry_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        delay: std::time::Duration,
        permanent: bool,
    ) -> Result<(), ControlError> {
        self.retry_check_run_update(owner, run_id, job_id, delay, permanent)
            .await
    }
    async fn clear_check_run_update_id(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        expected: u64,
    ) -> Result<(), ControlError> {
        self.clear_check_run_update_id(owner, run_id, job_id, expected)
            .await
    }
    async fn enqueue_check_run_update(
        &self,
        update: CheckRunUpdateInput,
    ) -> Result<(), ControlError> {
        self.enqueue_check_run_update(update).await
    }
    async fn defer_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        delay: std::time::Duration,
    ) -> Result<(), ControlError> {
        self.defer_check_run_update(owner, run_id, job_id, delay)
            .await
    }
    async fn renew_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        lease_for: std::time::Duration,
    ) -> Result<bool, ControlError> {
        self.renew_check_run_update(owner, run_id, job_id, lease_for)
            .await
    }
    async fn append_check_run_projection(
        &self,
        run_id: RunId,
        job_id: Option<&JobId>,
    ) -> Result<(), ControlError> {
        self.append_check_run_projection(run_id, job_id).await
    }
    async fn outbox_head(&self) -> Result<Option<OutboxBookmark>, ControlError> {
        self.outbox_head().await.map(Some)
    }
    async fn outbox_read(
        &self,
        after: OutboxBookmark,
        limit: usize,
    ) -> Result<Vec<OutboxRow>, ControlError> {
        self.outbox_read(after, limit).await
    }
    async fn reap_inputs(&self) -> Result<ReapInputs, ControlError> {
        self.reap_inputs().await
    }
    async fn attempt_job(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<(RunId, JobId)>, ControlError> {
        self.attempt_job(agent_job_id).await
    }
    async fn patch_steps(
        &self,
        agent_job_id: uuid::Uuid,
        patches: Vec<StepPatch>,
    ) -> Result<(), ControlError> {
        self.patch_steps(agent_job_id, patches).await
    }
    async fn report_steps(
        &self,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
        steps: Vec<serde_json::Value>,
    ) -> Result<bool, ControlError> {
        self.report_steps(plan_id, agent_job_id, steps).await
    }
    async fn attempt_repository(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<String>, ControlError> {
        self.attempt_repository(agent_job_id).await
    }
    async fn attempt_in_run(
        &self,
        run_id: RunId,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
    ) -> Result<bool, ControlError> {
        self.attempt_in_run(run_id, plan_id, agent_job_id).await
    }
    async fn run_job_ids(&self, run_id: RunId) -> Result<Vec<String>, ControlError> {
        self.run_job_ids(run_id).await
    }
    async fn set_job_check_run(
        &self,
        run_id: RunId,
        job_id: &JobId,
        check_run_id: u64,
    ) -> Result<bool, ControlError> {
        self.set_job_check_run(run_id, job_id, check_run_id).await
    }
    async fn clear_job_check_run(
        &self,
        run_id: RunId,
        job_id: &JobId,
        expected: u64,
    ) -> Result<(), ControlError> {
        self.clear_job_check_run(run_id, job_id, expected).await
    }
    async fn job_exists(&self, run_id: RunId, job_id: &JobId) -> Result<bool, ControlError> {
        self.job_exists(run_id, job_id).await
    }
    async fn job_display_name(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<String>, ControlError> {
        self.job_display_name(run_id, job_id).await
    }
    async fn run_dispatch_info(
        &self,
        run_id: RunId,
    ) -> Result<Option<RunDispatchInfo>, ControlError> {
        self.run_dispatch_info(run_id).await
    }
    async fn submission_fields(
        &self,
        run_id: RunId,
    ) -> Result<Option<SubmissionFields>, ControlError> {
        self.submission_fields(run_id).await
    }
    async fn job_check_run_id(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<u64>, ControlError> {
        self.job_check_run_id(run_id, job_id).await
    }
    async fn run_job_statuses(
        &self,
        run_id: RunId,
    ) -> Result<Option<Vec<(JobId, ExecutionStatus)>>, ControlError> {
        self.run_job_statuses(run_id).await
    }
    async fn live_log_key(
        &self,
        run_id: RunId,
        job_id: &str,
    ) -> Result<Option<(String, bool)>, ControlError> {
        self.live_log_key(run_id, job_id).await
    }
    async fn run_requests(
        &self,
        run_id: RunId,
    ) -> Result<Vec<TaskAgentJobRequestRecord>, ControlError> {
        self.run_requests(run_id).await
    }
    async fn run_step_manifests(
        &self,
        run_id: RunId,
    ) -> Result<BTreeMap<uuid::Uuid, Vec<crate::models::StepRecord>>, ControlError> {
        self.run_step_manifests(run_id).await
    }
    async fn issue_debug_token(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<(RunId, String), ControlError> {
        self.issue_debug_token(agent_job_id).await
    }
    async fn sweep_stale_bindings(&self) -> Result<usize, ControlError> {
        self.sweep_stale_bindings().await
    }
    async fn check_run_target(
        &self,
        check_run_id: u64,
        repository: &str,
        head_sha: Option<&str>,
        job_name: Option<&str>,
        details_run_id: Option<RunId>,
    ) -> Result<Option<(RunId, JobId)>, ControlError> {
        self.check_run_target(check_run_id, repository, head_sha, job_name, details_run_id)
            .await
    }
    async fn submission_json_for_attempt(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<String>, ControlError> {
        self.submission_json_for_attempt(agent_job_id).await
    }
    async fn published_run(
        &self,
        repository: &str,
        sha: &str,
        workflow_path: &str,
    ) -> Result<Option<RunId>, ControlError> {
        self.published_run(repository, sha, workflow_path).await
    }
    async fn run_for_webhook_delivery(
        &self,
        delivery_id: &str,
        workflow_path: &str,
    ) -> Result<Option<RunId>, ControlError> {
        self.run_for_webhook_delivery(delivery_id, workflow_path)
            .await
    }
    async fn run_held(&self, run_id: RunId) -> Result<bool, ControlError> {
        self.run_held(run_id).await
    }
    async fn runner_exists(&self, runner_id: i64) -> Result<bool, ControlError> {
        self.runner_exists(runner_id).await
    }
    async fn runner_for_client(&self, client_id: &str) -> Result<Option<i64>, ControlError> {
        self.runner_for_client(client_id).await
    }
    async fn run_for_attempt(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<RunId>, ControlError> {
        self.run_for_attempt(agent_job_id).await
    }
    async fn run_in_concurrency(&self, run_id: RunId) -> Result<RunConcurrency, ControlError> {
        self.run_in_concurrency(run_id).await
    }
    async fn callback_job(
        &self,
        plan_id: &str,
        timeline_id: Option<uuid::Uuid>,
        agent_job_id: Option<uuid::Uuid>,
    ) -> Result<Option<CallbackJob>, ControlError> {
        self.callback_job(plan_id, timeline_id, agent_job_id).await
    }
    async fn sole_inflight_request(
        &self,
    ) -> Result<Option<(i64, RunId, JobId, uuid::Uuid)>, ControlError> {
        self.sole_inflight_request().await
    }
    async fn renew_lease(
        &self,
        agent_job_id: uuid::Uuid,
        runner_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError> {
        self.renew_lease(agent_job_id, runner_id, locked_until)
            .await
    }
    async fn request_owner(
        &self,
        request_id: i64,
    ) -> Result<Option<(Option<i64>, Option<i64>, bool)>, ControlError> {
        self.request_owner(request_id).await
    }
    async fn renew_broker_request(
        &self,
        agent_job_id: uuid::Uuid,
        runner_id: i64,
        locked_until: &str,
    ) -> Result<(), ControlError> {
        self.renew_broker_request(agent_job_id, runner_id, locked_until)
            .await
    }
    async fn create_broker_session(
        &self,
        session_id: &str,
        runner_id: i64,
    ) -> Result<(), ControlError> {
        self.create_broker_session(session_id, runner_id).await
    }
    async fn delete_broker_session(
        &self,
        session_id: &str,
        runner_id: i64,
    ) -> Result<bool, ControlError> {
        self.delete_broker_session(session_id, runner_id).await
    }
    async fn orphaned_claims(&self) -> Result<Vec<(i64, RunId, JobId)>, ControlError> {
        self.orphaned_claims().await
    }
    async fn release_claimed_request(
        &self,
        request_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError> {
        self.release_claimed_request(request_id, locked_until).await
    }
    async fn settle_request(
        &self,
        request_id: i64,
        result: ExecutionStatus,
        locked_until: &str,
    ) -> Result<Option<(RunId, JobId, uuid::Uuid)>, ControlError> {
        self.settle_request(request_id, result, locked_until).await
    }
    async fn job_queue_state(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<(String, String)>, ControlError> {
        self.job_queue_state(run_id, job_id).await
    }
    async fn renew_agent_request(
        &self,
        request_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError> {
        self.renew_agent_request(request_id, locked_until).await
    }
    async fn settle_agent_request(
        &self,
        request_id: i64,
        result: ExecutionStatus,
        locked_until: &str,
    ) -> Result<Option<(RunId, JobId, uuid::Uuid)>, ControlError> {
        self.settle_agent_request(request_id, result, locked_until)
            .await
    }
    async fn delete_inflight(&self, session_id: &str, message_id: i64) -> Result<(), ControlError> {
        self.delete_inflight(session_id, message_id).await
    }
    async fn active_plan_ids(&self) -> Result<BTreeSet<String>, ControlError> {
        self.active_plan_ids().await
    }
    async fn enqueue_webhook_delivery(
        &self,
        delivery: &WebhookDeliveryRecord,
    ) -> Result<bool, ControlError> {
        self.enqueue_webhook_delivery(delivery).await
    }
    async fn claim_webhook_deliveries(
        &self,
        limit: usize,
        lease_duration_secs: u64,
    ) -> Result<Vec<WebhookDeliveryRecord>, ControlError> {
        self.claim_webhook_deliveries(limit, lease_duration_secs)
            .await
    }
    async fn renew_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        lease_duration_secs: u64,
    ) -> Result<bool, ControlError> {
        self.renew_webhook_delivery(delivery_id, lease_token, lease_duration_secs)
            .await
    }
    async fn complete_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
    ) -> Result<bool, ControlError> {
        self.complete_webhook_delivery(delivery_id, lease_token)
            .await
    }
    async fn fail_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        error: &str,
        permanent: bool,
        retry_delay: Option<std::time::Duration>,
    ) -> Result<bool, ControlError> {
        self.fail_webhook_delivery(delivery_id, lease_token, error, permanent, retry_delay)
            .await
    }
    async fn get_webhook_delivery(
        &self,
        delivery_id: &str,
    ) -> Result<Option<WebhookDeliveryRecord>, ControlError> {
        self.get_webhook_delivery(delivery_id).await
    }
    async fn count_dead_letter_webhook_deliveries(&self) -> Result<u64, ControlError> {
        self.count_dead_letter_webhook_deliveries().await
    }
    async fn recover_webhook_deliveries(&self) -> Result<u64, ControlError> {
        self.recover_webhook_deliveries().await
    }
    async fn prune_webhook_deliveries(
        &self,
        before_us: i64,
        limit: usize,
    ) -> Result<u64, ControlError> {
        self.prune_webhook_deliveries(before_us, limit).await
    }
    async fn park_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        error: &str,
        retry_delay_secs: u64,
    ) -> Result<bool, ControlError> {
        self.park_webhook_delivery(delivery_id, lease_token, error, retry_delay_secs)
            .await
    }
    async fn requeue_webhook_delivery(&self, delivery_id: &str) -> Result<bool, ControlError> {
        self.requeue_webhook_delivery(delivery_id).await
    }
    async fn list_webhook_deliveries(
        &self,
        state: Option<WebhookDeliveryStatus>,
        limit: usize,
    ) -> Result<Vec<WebhookDeliverySummary>, ControlError> {
        self.list_webhook_deliveries(state, limit).await
    }
    async fn webhook_deliveries_present(
        &self,
        delivery_ids: &[String],
    ) -> Result<std::collections::BTreeSet<String>, ControlError> {
        self.webhook_deliveries_present(delivery_ids).await
    }
    async fn webhook_queue_stats(&self) -> Result<WebhookQueueStats, ControlError> {
        self.webhook_queue_stats().await
    }
    async fn load_webhook_watchdog_cursor(
        &self,
        scope: &str,
    ) -> Result<Option<WebhookWatchdogCursor>, ControlError> {
        self.load_webhook_watchdog_cursor(scope).await
    }
    async fn store_webhook_watchdog_cursor(
        &self,
        cursor: &WebhookWatchdogCursor,
    ) -> Result<(), ControlError> {
        self.store_webhook_watchdog_cursor(cursor).await
    }
    async fn upsert_webhook_redelivery(
        &self,
        record: &WebhookRedeliveryRecord,
    ) -> Result<(), ControlError> {
        self.upsert_webhook_redelivery(record).await
    }
    async fn load_webhook_redelivery(
        &self,
        delivery_guid: &str,
    ) -> Result<Option<WebhookRedeliveryRecord>, ControlError> {
        self.load_webhook_redelivery(delivery_guid).await
    }
    async fn open_webhook_redeliveries(
        &self,
        limit: usize,
    ) -> Result<Vec<WebhookRedeliveryRecord>, ControlError> {
        self.open_webhook_redeliveries(limit).await
    }
    async fn resolve_webhook_redelivery(
        &self,
        delivery_guid: &str,
        resolved_at_us: i64,
    ) -> Result<bool, ControlError> {
        self.resolve_webhook_redelivery(delivery_guid, resolved_at_us)
            .await
    }
    async fn live_assignments(
        &self,
    ) -> Result<Vec<preloop_observability::status::RunnerAssignment>, ControlError> {
        self.live_assignments().await
    }
    async fn pair_runner(&self, runner_id: i64) -> Result<(), ControlError> {
        self.pair_runner(runner_id).await
    }
    async fn list_runners(&self, run_id: Option<RunId>) -> Result<RunnerListing, ControlError> {
        self.list_runners(run_id).await
    }
    async fn runner_rsa_public_key(
        &self,
        runner_id: i64,
    ) -> Result<Option<preloop_gha_protocol::crypto::AgentRsaPublicKey>, ControlError> {
        self.runner_rsa_public_key(runner_id).await
    }
    async fn open_runner_session(&self, open: OpenRunnerSession) -> Result<(), ControlError> {
        self.open_runner_session(open).await
    }
    async fn close_runner_session(
        &self,
        session_id: &str,
        caller_runner_id: Option<i64>,
    ) -> Result<bool, ControlError> {
        self.close_runner_session(session_id, caller_runner_id)
            .await
    }
    async fn purge_runner_guarded(
        &self,
        runner_id: i64,
        guard: PurgeGuard,
    ) -> Result<Option<Vec<uuid::Uuid>>, ControlError> {
        self.purge_runner_guarded(runner_id, guard).await
    }
    async fn ephemeral_runner_ids(&self) -> Result<Vec<i64>, ControlError> {
        self.ephemeral_runner_ids().await
    }
    async fn runner_ids_named(&self, name: &str) -> Result<Vec<i64>, ControlError> {
        self.runner_ids_named(name).await
    }
    async fn lookup_agent(
        &self,
        name: &str,
    ) -> Result<Option<(preloop_gha_protocol::RegisteredRunner, String)>, ControlError> {
        self.lookup_agent(name).await
    }
    async fn bind_runner_client(
        &self,
        runner_id: i64,
        client_id: &str,
        pair_with_pending_job: bool,
    ) -> Result<(), ControlError> {
        self.bind_runner_client(runner_id, client_id, pair_with_pending_job)
            .await
    }
    async fn update_runner(
        &self,
        runner_id: i64,
        name: Option<String>,
        labels: Option<Vec<String>>,
    ) -> Result<RunnerRow, ControlError> {
        self.update_runner(runner_id, name, labels).await
    }
    async fn reap_sweep(&self, sweep: ReapSweep) -> Result<ReapSweepOutcome, ControlError> {
        self.reap_sweep(sweep).await
    }
    async fn status_inputs(
        &self,
        stale_after: std::time::Duration,
    ) -> Result<StatusInputs, ControlError> {
        self.status_inputs(stale_after).await
    }
    async fn rebuild_dispatch_intent(&self) -> Result<(), ControlError> {
        self.rebuild_dispatch_intent().await
    }
    async fn runs_for_repository(&self, repository: &str) -> Result<Vec<RunRecord>, ControlError> {
        self.runs_for_repository(repository).await
    }
    async fn poll_azdo_session(&self, poll: AzdoPoll) -> Result<AzdoPollOutcome, ControlError> {
        self.poll_azdo_session(poll).await
    }
    async fn settle_job(&self, settle: SettleJob) -> Result<SettleJobOutcome, ControlError> {
        self.settle_job(settle).await
    }
    async fn oidc_grant(
        &self,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
    ) -> Result<OidcGrant, ControlError> {
        self.oidc_grant(plan_id, agent_job_id).await
    }
}
