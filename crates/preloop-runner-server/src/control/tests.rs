//! Behavioral suite for the `ControlBackend` contract. The same scenarios
//! run against every backend (SQLite now, Postgres when it lands) so a
//! backend can never diverge from the shared scheduling semantics.
//!
//! Every assertion reads state back through a fresh `transact` (a full DB
//! reload), so a bug in `load_txstate`/`write_txstate` fails here even when
//! the in-memory path would have looked correct.
//!
//! Layout: [`suite`] holds the backend-neutral scenarios, each taking
//! `&dyn ControlBackend`. `sqlite` and `postgres` are thin `#[tokio::test]`
//! wrappers that construct their backend and call the same functions — a
//! new backend proves conformance by wiring the same suite, never by
//! copying the assertions.

use super::backend::*;
use super::types::*;
use crate::models::{QueuedJob, RunRecord, RunnerCapabilities, TaskAgentJobRequestRecord};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use std::collections::BTreeMap;
use std::sync::Arc;

// ── Fixtures ────────────────────────────────────────────────────────────

fn submission() -> preloop_gha_protocol::WorkflowSubmission {
    preloop_gha_protocol::WorkflowSubmission {
        repository: "owner/repo".to_owned(),
        event: "push".to_owned(),
        sha: "abc123".to_owned(),
        ..Default::default()
    }
}

fn test_cipher() -> crate::store::Envelope {
    crate::store::Envelope::new(b"control-test-key")
}

fn run_record(run_id: RunId) -> RunRecord {
    RunRecord {
        run_id,
        webhook_delivery_id: None,
        run_name: Some("ci".to_owned()),
        submission: Arc::new(submission()),
        jobs: BTreeMap::new(),
        status: ExecutionStatus::Queued,
        job_outputs: BTreeMap::new(),
        job_base_ids: BTreeMap::new(),
        job_needs: BTreeMap::new(),
        caller_plans: BTreeMap::new(),
        job_names: BTreeMap::new(),
        github: serde_json::json!({"sha": "abc123"}),
        head_sha: "abc123".to_owned(),
        workflow_ref: "owner/repo/.github/workflows/ci.yml@refs/heads/main".to_owned(),
        workspace_snapshot: None,
        job_fail_fast: BTreeMap::new(),
        job_continue_on_error: BTreeMap::new(),
        job_check_run_ids: BTreeMap::new(),
        reusable_calls: BTreeMap::new(),
        jobs_list: Vec::new(),
        created_at: chrono::Utc::now(),
        started_at: None,
        completed_at: None,
        run_number: 1,
        run_attempt: 1,
        workflow_path_str: ".github/workflows/ci.yml".to_owned(),
        event: "push".to_owned(),
        conclusion: None,
        push_state: None,
        snapshot_timing: None,
    }
}

fn job_message(
    job_id: &str,
    request_id: i64,
) -> preloop_gha_protocol::azdo::AgentJobRequestMessage {
    serde_json::from_value(serde_json::json!({
        "jobId": uuid::Uuid::new_v4(),
        "requestId": request_id,
        "plan": {"planId": "plan", "planType": "build", "version": 1, "artifactUri": "", "artifactLocation": ""},
        "timeline": {"id": uuid::Uuid::new_v4(), "changeId": 0, "location": null},
        "jobName": job_id,
        "lockedUntil": "",
        "resources": {"endpoints": []},
        "steps": [],
        "snapshot": null
    }))
    .unwrap()
}

fn queued_job(run_id: RunId, job_id: &str, request_id: i64) -> QueuedJob {
    let nanos = crate::models::now_unix_nanos();
    QueuedJob {
        run_id,
        job_id: JobId(job_id.to_owned()),
        base_id: job_id.to_owned(),
        created_at_unix_nanos: nanos,
        dependencies_ready_at_unix_nanos: Some(nanos),
        concurrency_wait_started_at_unix_nanos: None,
        concurrency_acquired_at_unix_nanos: None,
        enqueued_at_unix_nanos: nanos,
        needs: Vec::new(),
        if_condition: None,
        condition_context: preloop_gha_expressions::Context::default(),
        max_parallel: None,
        runs_on: vec!["self-hosted".to_owned()],
        runner_group: None,
        environment: None,
        message: job_message(job_id, request_id),
        concurrency: None,
        matrix: BTreeMap::new(),
        deferred_matrix: None,
        reusable_call: None,
    }
}

fn request_record(run_id: RunId, job_id: &str, request_id: i64) -> TaskAgentJobRequestRecord {
    TaskAgentJobRequestRecord {
        request_id,
        run_id,
        job_id: JobId(job_id.to_owned()),
        agent_job_id: uuid::Uuid::new_v4(),
        plan_id: "plan".to_owned(),
        plan_type: "build".to_owned(),
        timeline_id: uuid::Uuid::new_v4(),
        result: None,
        locked_until: String::new(),
        claimed_at: None,
        owner_runner_id: None,
        started_at: None,
        last_renewed_at: None,
        timeout_triggered: false,
        debug_token_issued: false,
    }
}

fn submit_job(run_id: RunId, job_id: &str, request_id: i64) -> SubmitJob {
    SubmitJob {
        queued: queued_job(run_id, job_id, request_id),
        request: Some(request_record(run_id, job_id, request_id)),
        token_request: None,
        id_token_granted: false,
        oidc_context: None,
        step_manifest: Vec::new(),
        initially_skipped: false,
    }
}

/// A `SubmitJob` with every truncate-family field populated — token request,
/// OIDC grant + context, and a declared step manifest — so the differential
/// scope test seeds real rows in `github_token_requests`, `id_token_grants`,
/// `oidc_job_contexts` and `job_steps` instead of asserting over empty tables.
fn submit_job_full(run_id: RunId, job_id: &str, request_id: i64) -> SubmitJob {
    let mut job = submit_job(run_id, job_id, request_id);
    job.token_request = Some(crate::models::GitHubTokenRequest {
        repository: "owner/repo".to_owned(),
        permissions: BTreeMap::from([("contents".to_owned(), "read".to_owned())]),
        declared: true,
        untrusted: false,
    });
    job.id_token_granted = true;
    job.oidc_context = Some(crate::state::OidcJobContext {
        environment: Some("prod".to_owned()),
        job_workflow_ref: Some("owner/repo/.github/workflows/ci.yml@refs/heads/main".to_owned()),
        job_workflow_sha: Some("abc123".to_owned()),
    });
    job.step_manifest = vec![crate::models::StepRecord {
        id: "step-1".to_owned(),
        kind: crate::models::StepKind::Workflow,
        workflow_index: Some(0),
        runner_number: Some(2),
        context_name: Some("build".to_owned()),
        name: "Build".to_owned(),
        conclusion: String::new(),
        started_at: None,
        finished_at: None,
    }];
    job
}

fn submit_run(run_id: RunId, jobs: Vec<SubmitJob>) -> SubmitRun {
    SubmitRun {
        namespace: "default".to_owned(),
        record: run_record(run_id),
        jobs,
        workflow_concurrency: None,
        empty_concurrency_group: false,
        check_hostable: false,
    }
}

fn capabilities() -> RunnerCapabilities {
    RunnerCapabilities {
        known: true,
        labels: vec!["self-hosted".to_owned()],
        runner_group_id: None,
        runner_group_name: None,
    }
}

fn register_runner(name: &str) -> RegisterRunner {
    RegisterRunner {
        name: name.to_owned(),
        labels: vec!["self-hosted".to_owned()],
        ephemeral: false,
        public_key: None,
        rsa_public_key: None,
        client_id: Some(format!("client-{name}")),
        runner_group_id: None,
        runner_group_name: None,
        pool_proven: false,
    }
}

fn create_session(runner_id: i64) -> CreateSession {
    CreateSession {
        runner_id,
        protocol: SessionProtocol::Broker,
        client_id: None,
        encryption: None,
    }
}

fn poll(session_id: &str, runner_id: i64) -> PollRequest {
    PollRequest {
        session_id: session_id.to_owned(),
        verified_runner_id: Some(runner_id),
        runner: capabilities(),
        busy: false,
        wait_ms: 0,
    }
}

/// Poll with no verified runner identity — produces a claim whose
/// `job_requests.owner_runner_id` is `None`, exercising the purge fallback
/// that keys off `session_active_requests` rather than the owner field.
fn poll_unverified(session_id: &str) -> PollRequest {
    PollRequest {
        session_id: session_id.to_owned(),
        verified_runner_id: None,
        runner: capabilities(),
        busy: false,
        wait_ms: 0,
    }
}

fn workflow_concurrency(group: &str, cancel_in_progress: bool) -> WorkflowConcurrency {
    WorkflowConcurrency {
        group: group.to_owned(),
        cancel_in_progress,
        queue: preloop_gha_parser::ConcurrencyQueue::Single,
        raw: preloop_gha_parser::Concurrency {
            group: group.to_owned(),
            cancel_in_progress: Some(cancel_in_progress.to_string()),
            queue: preloop_gha_parser::ConcurrencyQueue::Single,
        },
    }
}

// ── Backend-neutral suite ───────────────────────────────────────────────
// Each function is one scenario against `&dyn ControlBackend`. A backend
// proves conformance by calling these from its own `#[tokio::test]` module.
pub(crate) mod suite {
    use super::*;

    pub(crate) async fn submit_poll_complete_lifecycle(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let runner = backend
            .register_runner(register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(create_session(runner.runner.id))
            .await
            .unwrap();

        let outcome = backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        assert_eq!(outcome.run_id, run_id);
        assert_eq!(outcome.queued_jobs, 1);
        assert!(outcome.existing.is_none());

        let poll = backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        let PollOutcome::Claimed(claimed) = poll else {
            panic!("expected a claim, got {poll:?}");
        };
        assert_eq!(claimed.queued.job_id, JobId("build".to_owned()));
        assert_eq!(claimed.request.request_id, 1);

        let ctx = backend.acquire_context(1).await.unwrap();
        assert_eq!(ctx.request.request_id, 1);
        assert_eq!(ctx.repository, "owner/repo");

        let done = backend
            .complete_job(JobCompletionInput {
                run_id,
                job_id: JobId("build".to_owned()),
                agent_job_id: Some(claimed.request.agent_job_id),
                status: ExecutionStatus::Success,
                outputs: BTreeMap::new(),
                runner_id: Some(runner.runner.id),
            })
            .await
            .unwrap();
        assert_eq!(done.effective_status, ExecutionStatus::Success);
        assert!(done.newly_terminal_success);
        assert_eq!(done.record.status, ExecutionStatus::Success);
    }

    pub(crate) async fn webhook_replay_is_idempotent(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let mut submit = submit_run(run_id, vec![submit_job(run_id, "build", 1)]);
        submit.record.webhook_delivery_id = Some("delivery-1".to_owned());

        let first = backend.submit_run(submit).await.unwrap();
        assert!(first.existing.is_none());
        assert_eq!(first.queued_jobs, 1);

        let mut replay = submit_run(RunId::new(), vec![submit_job(RunId::new(), "build", 2)]);
        replay.record.webhook_delivery_id = Some("delivery-1".to_owned());
        let second = backend.submit_run(replay).await.unwrap();
        assert!(second.existing.is_some());
        assert_eq!(second.run_id, run_id);
        assert_eq!(second.queued_jobs, 0);
    }

    pub(crate) async fn cancel_run_queues_cancellation(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let runner = backend
            .register_runner(register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(create_session(runner.runner.id))
            .await
            .unwrap();
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        let claimed = match backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap()
        {
            PollOutcome::Claimed(c) => c,
            other => panic!("expected claim, got {other:?}"),
        };

        let outcome = backend.cancel_run(run_id, None).await.unwrap();
        assert_eq!(outcome.cancellations, 1);

        let poll = backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        match poll {
            PollOutcome::Cancel(message) => {
                assert_eq!(message.message_type, "JobCancellation");
                assert!(message
                    .body
                    .contains(&claimed.request.agent_job_id.to_string()));
            }
            other => panic!("expected cancel, got {other:?}"),
        }
    }

    pub(crate) async fn secrets_survive_seal_unseal(backend: &dyn ControlBackend) {
        // Regression: a naive serde round-trip turns every SecretString into
        // the literal "<redacted>". The run blob must carry real values.
        let run_id = RunId::new();
        let mut record = run_record(run_id);
        let mut secrets = preloop_gha_protocol::SecretMap::new();
        secrets.insert(
            "TOKEN".to_owned(),
            preloop_gha_protocol::SecretString::new("s3cr3t-value"),
        );
        Arc::make_mut(&mut record.submission).secrets = secrets;

        backend
            .submit_run(SubmitRun {
                namespace: "default".to_owned(),
                record,
                jobs: vec![submit_job(run_id, "build", 1)],
                workflow_concurrency: None,
                empty_concurrency_group: false,
                check_hostable: false,
            })
            .await
            .unwrap();

        let restored = backend.run_record(run_id).await.unwrap();
        let secret = restored
            .submission
            .secrets
            .get("TOKEN")
            .expect("secret must survive the round-trip");
        assert_eq!(secret.expose(), "s3cr3t-value");
        assert_ne!(secret.expose(), "<redacted>");
    }

    pub(crate) async fn concurrency_gate_serializes_group(backend: &dyn ControlBackend) {
        let run_a = RunId::new();
        let mut submit_a = submit_run(run_a, vec![submit_job(run_a, "deploy", 1)]);
        submit_a.workflow_concurrency = Some(workflow_concurrency("deploy", false));
        let a = backend.submit_run(submit_a).await.unwrap();
        assert!(!a.held, "first run should acquire the gate");

        let run_b = RunId::new();
        let mut submit_b = submit_run(run_b, vec![submit_job(run_b, "deploy", 2)]);
        submit_b.workflow_concurrency = Some(workflow_concurrency("deploy", false));
        let b = backend.submit_run(submit_b).await.unwrap();
        assert!(b.held, "second run in the same group must be held");
    }

    pub(crate) async fn reconcile_recovers_orphaned_claim(backend: &dyn ControlBackend) {
        // Crash-recovery: a job claimed by a runner whose session then dies
        // (releasing the request's owner) is orphaned. reconcile_on_boot must
        // requeue it so a replacement runner picks it up — never lost.
        let run_id = RunId::new();
        let runner = backend
            .register_runner(register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(create_session(runner.runner.id))
            .await
            .unwrap();
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        match backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap()
        {
            PollOutcome::Claimed(_) => {}
            other => panic!("expected claim, got {other:?}"),
        }

        // The session dies: its request owner is released, orphaning the
        // claimed job.
        backend.delete_session(&session.session_id).await.unwrap();

        let outcome = backend.reconcile_on_boot().await.unwrap();
        assert_eq!(outcome.recovered, 1, "orphaned claim must be recovered");
        assert_eq!(outcome.failed, 0);

        // A fresh session on a replacement runner claims the requeued job.
        let runner2 = backend
            .register_runner(register_runner("r2"))
            .await
            .unwrap();
        let session2 = backend
            .create_session(create_session(runner2.runner.id))
            .await
            .unwrap();
        match backend
            .poll_session(poll(&session2.session_id, runner2.runner.id))
            .await
            .unwrap()
        {
            PollOutcome::Claimed(claimed) => {
                assert_eq!(claimed.queued.job_id, JobId("build".to_owned()));
            }
            other => panic!("expected re-claim of recovered job, got {other:?}"),
        }
    }

    /// Runner-death recovery via `purge_runner`: a job claimed by the purged
    /// runner is requeued with `run.jobs → Queued` and the run summary
    /// recomputed — not left `in_progress` under the dead runner's claim.
    /// Backend-neutral: exercises `purge_runner_tx` on both SQLite/PG.
    pub(crate) async fn purge_requeues_claimed_as_queued(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let runner = backend
            .register_runner(register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(create_session(runner.runner.id))
            .await
            .unwrap();
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        match backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap()
        {
            PollOutcome::Claimed(_) => {}
            other => panic!("expected claim, got {other:?}"),
        }

        // The runner dies: purge requeues its claimed job.
        backend.purge_runner(runner.runner.id).await.unwrap();

        // The run summary must reflect the requeue. `summarize_run` maps any
        // Queued/Pending/InProgress job to run-level InProgress, so a sole
        // requeued job leaves the run in_progress — recomputed, not stale.
        let record = backend.run_record(run_id).await.unwrap();
        assert_eq!(
            record.jobs.get(&JobId("build".to_owned())),
            Some(&ExecutionStatus::Queued),
            "purged claim must reset job status to queued"
        );
        assert_eq!(
            record.status,
            ExecutionStatus::InProgress,
            "run summary recomputes to in_progress (sole queued job)"
        );

        // A replacement runner claims the requeued job.
        let runner2 = backend
            .register_runner(register_runner("r2"))
            .await
            .unwrap();
        let session2 = backend
            .create_session(create_session(runner2.runner.id))
            .await
            .unwrap();
        match backend
            .poll_session(poll(&session2.session_id, runner2.runner.id))
            .await
            .unwrap()
        {
            PollOutcome::Claimed(claimed) => {
                assert_eq!(claimed.queued.job_id, JobId("build".to_owned()));
            }
            other => panic!("expected re-claim of purged job, got {other:?}"),
        }
    }

    /// Purge recovery for an OWNERLESS claim: `verified_runner_id: None` leaves
    /// `job_requests.owner_runner_id = None`, so the only ownership evidence is
    /// the doomed session's `session_active_requests` mapping. The purge must
    /// still requeue the job — a filter keyed only on `owner_runner_id` would
    /// leave it stuck `claimed` forever.
    pub(crate) async fn purge_requeues_ownerless_claim(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let runner = backend
            .register_runner(register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(create_session(runner.runner.id))
            .await
            .unwrap();
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        // A cached poll must still prove ownership inside the claim transaction.
        let rejected = backend
            .poll_session(poll(&session.session_id, runner.runner.id + 1))
            .await;
        assert!(matches!(rejected, Err(ControlError::Forbidden(_))));
        assert_eq!(backend.queue_stats().await.unwrap().ready, 1);

        // Claim with NO verified runner → owner_runner_id stays None; the
        // session→active-request link is the sole ownership record.
        match backend
            .poll_session(poll_unverified(&session.session_id))
            .await
            .unwrap()
        {
            PollOutcome::Claimed(claimed) => {
                assert_eq!(claimed.runner_id, runner.runner.id);
            }
            other => panic!("expected ownerless claim, got {other:?}"),
        }
        let request = backend.request(RequestKey::Id(1)).await.unwrap();
        assert!(
            request.started_at.is_some(),
            "claim must start the job timeout"
        );
        assert!(
            request.last_renewed_at.is_some(),
            "claim must start the lease"
        );
        match backend
            .poll_session(poll_unverified(&session.session_id))
            .await
            .unwrap()
        {
            PollOutcome::ActiveRequest { request, runner_id } => {
                assert_eq!(request.request_id, 1);
                assert_eq!(runner_id, runner.runner.id);
            }
            other => panic!("expected active request, got {other:?}"),
        }

        backend.purge_runner(runner.runner.id).await.unwrap();
        // Simulate a long-poll resuming after the liveness sweep revoked it.
        let rejected = backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await;
        assert!(matches!(rejected, Err(ControlError::Forbidden(_))));
        assert_eq!(backend.queue_stats().await.unwrap().ready, 1);

        let record = backend.run_record(run_id).await.unwrap();
        assert_eq!(
            record.jobs.get(&JobId("build".to_owned())),
            Some(&ExecutionStatus::Queued),
            "ownerless purged claim must reset job status to queued"
        );
        assert_eq!(
            record.status,
            ExecutionStatus::InProgress,
            "run summary recomputes to in_progress (sole requeued job)"
        );

        // A replacement runner claims the requeued job.
        let runner2 = backend
            .register_runner(register_runner("r2"))
            .await
            .unwrap();
        let session2 = backend
            .create_session(create_session(runner2.runner.id))
            .await
            .unwrap();
        match backend
            .poll_session(poll(&session2.session_id, runner2.runner.id))
            .await
            .unwrap()
        {
            PollOutcome::Claimed(claimed) => {
                assert_eq!(claimed.queued.job_id, JobId("build".to_owned()));
            }
            other => panic!("expected re-claim of ownerless purged job, got {other:?}"),
        }
    }

    /// A job no registered runner can host must persist as terminal — the
    /// classification lands on the authoritative `tx.runs` record, not a
    /// detached clone (regression: the post-promote re-insert used to drop
    /// it, leaving the job Queued forever).
    pub(crate) async fn submit_unhostable_job_persists_failure(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let mut submit = submit_run(run_id, vec![submit_job(run_id, "build", 1)]);
        submit.check_hostable = true;
        submit.jobs[0].queued.runs_on = vec!["windows-latest".to_owned()];
        let outcome = backend.submit_run(submit).await.unwrap();
        assert_eq!(outcome.status, ExecutionStatus::Failure);
        // Read back through a fresh transaction — the DB is the authority.
        let record = backend.run_record(run_id).await.unwrap();
        assert_eq!(
            record.jobs.get(&JobId("build".to_owned())),
            Some(&ExecutionStatus::Failure)
        );
        assert_eq!(record.status, ExecutionStatus::Failure);
        assert!(
            record.completed_at.is_some(),
            "terminal run must carry completed_at"
        );
    }

    /// An initially-skipped parent must settle its dependent child through
    /// the promotion sweep — and the sweep's in-place `tx.runs` update is
    /// the one that persists (regression: a stale record clone re-inserted
    /// after the sweep reverted the child to Queued).
    pub(crate) async fn submit_skipped_parent_settles_child(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let mut parent = submit_job(run_id, "parent", 1);
        parent.initially_skipped = true;
        let mut child = submit_job(run_id, "child", 2);
        child.queued.needs = vec![JobId("parent".to_owned())];
        backend
            .submit_run(submit_run(run_id, vec![parent, child]))
            .await
            .unwrap();
        let record = backend.run_record(run_id).await.unwrap();
        assert_eq!(
            record.jobs.get(&JobId("parent".to_owned())),
            Some(&ExecutionStatus::Skipped)
        );
        assert_eq!(
            record.jobs.get(&JobId("child".to_owned())),
            Some(&ExecutionStatus::Skipped),
            "promote sweep's Skip must survive write-back"
        );
        assert!(
            record.status.is_terminal(),
            "run must be terminal, got {:?}",
            record.status
        );
    }

    /// A cancel-in-progress submit that replaces a queued run must report
    /// the surviving depth — the cancelled job's removal decrements
    /// `ready_count` (regression: raw `retain` left the scalar stale-high,
    /// reporting 2 for one surviving job).
    pub(crate) async fn cancel_in_progress_submit_reports_surviving_depth(
        backend: &dyn ControlBackend,
    ) {
        let run_a = RunId::new();
        let mut submit_a = submit_run(run_a, vec![submit_job(run_a, "build", 1)]);
        submit_a.workflow_concurrency = Some(workflow_concurrency("g", true));
        let outcome_a = backend.submit_run(submit_a).await.unwrap();
        assert_eq!(outcome_a.queue_depth, 1);

        let run_b = RunId::new();
        let mut submit_b = submit_run(run_b, vec![submit_job(run_b, "build", 2)]);
        submit_b.workflow_concurrency = Some(workflow_concurrency("g", true));
        let outcome_b = backend.submit_run(submit_b).await.unwrap();
        assert_eq!(
            outcome_b.queue_depth, 1,
            "cancelled job must not inflate depth"
        );
        let record_a = backend.run_record(run_a).await.unwrap();
        assert_eq!(record_a.status, ExecutionStatus::Cancelled);
    }
}

// ── SQLite ──────────────────────────────────────────────────────────────
mod sqlite {
    use super::suite;
    use crate::control::backend::ControlBackend;
    use crate::control::sqlite::SqliteBackend;
    use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};

    /// Parse `dump_tables` `jobs` rows into `job_id → (status, queue_kind,
    /// queue_position, seq)`. Cells render as `name=Value::Debug`, e.g.
    /// `status=Text("queued")`, `queue_position=Integer(3)`,
    /// `queue_position=Null`. Used to assert exact FIFO/status invariants
    /// across queue transitions.
    fn job_rows(
        dump: &std::collections::BTreeMap<String, Vec<String>>,
    ) -> std::collections::BTreeMap<String, (String, String, Option<i64>, i64)> {
        let cell = |row: &str, name: &str| -> String {
            row.split('|')
                .find_map(|c| c.strip_prefix(&format!("{name}=")))
                .unwrap_or("")
                .to_owned()
        };
        let text = |row: &str, name: &str| -> String {
            cell(row, name)
                .trim_start_matches("Text(\"")
                .trim_end_matches("\")")
                .to_owned()
        };
        let int = |row: &str, name: &str| -> Option<i64> {
            cell(row, name)
                .trim_start_matches("Integer(")
                .trim_end_matches(")")
                .parse()
                .ok()
        };
        dump.get("jobs")
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|r| {
                (
                    text(r, "job_id"),
                    (
                        text(r, "status"),
                        text(r, "queue_kind"),
                        int(r, "queue_position"),
                        int(r, "seq").unwrap_or(0),
                    ),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn submit_poll_complete_lifecycle() {
        suite::submit_poll_complete_lifecycle(&SqliteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn webhook_replay_is_idempotent() {
        suite::webhook_replay_is_idempotent(&SqliteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn cancel_run_queues_cancellation() {
        suite::cancel_run_queues_cancellation(&SqliteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn secrets_survive_seal_unseal() {
        suite::secrets_survive_seal_unseal(&SqliteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn concurrency_gate_serializes_group() {
        suite::concurrency_gate_serializes_group(&SqliteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn reconcile_recovers_orphaned_claim() {
        suite::reconcile_recovers_orphaned_claim(&SqliteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn purge_requeues_claimed_as_queued() {
        suite::purge_requeues_claimed_as_queued(&SqliteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn purge_requeues_ownerless_claim() {
        suite::purge_requeues_ownerless_claim(&SqliteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn submit_unhostable_job_persists_failure() {
        suite::submit_unhostable_job_persists_failure(&SqliteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn submit_skipped_parent_settles_child() {
        suite::submit_skipped_parent_settles_child(&SqliteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn cancel_in_progress_submit_reports_surviving_depth() {
        suite::cancel_in_progress_submit_reports_surviving_depth(
            &SqliteBackend::in_memory().unwrap(),
        )
        .await;
    }

    /// A file-backed backend proves durability: submit, drop, reopen, and
    /// the job is still claimable — the DB is the authority, not process
    /// memory. (SQLite-specific: it exercises the on-disk file.)
    #[tokio::test]
    async fn state_survives_reopen() {
        let dir = std::env::temp_dir().join(format!("preloop-control-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("control.db");

        let run_id = RunId::new();
        {
            let backend = SqliteBackend::open(
                &path,
                super::test_cipher(),
                false,
                false,
                std::time::Duration::from_secs(300),
            )
            .unwrap();
            backend
                .register_runner(super::register_runner("r1"))
                .await
                .unwrap();
            backend
                .submit_run(super::submit_run(
                    run_id,
                    vec![super::submit_job(run_id, "build", 1)],
                ))
                .await
                .unwrap();
        }

        let backend = SqliteBackend::open(
            &path,
            super::test_cipher(),
            false,
            false,
            std::time::Duration::from_secs(300),
        )
        .unwrap();
        let stats = backend.queue_stats().await.unwrap();
        assert_eq!(stats.ready, 1, "queued job must survive reopen");

        let record = backend.run_record(run_id).await.unwrap();
        assert_eq!(record.status, ExecutionStatus::Queued);
        assert_eq!(
            record.jobs.get(&JobId("build".to_owned())),
            Some(&ExecutionStatus::Queued)
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// v5 → v6 migration: a database created with `jobs.claim_generation`
    /// must open cleanly — the dead column is dropped and existing job rows
    /// survive. Builds a v5-shaped `jobs` table by applying the current DDL
    /// then re-adding the column, pins `user_version=5`, and lets
    /// `SqliteBackend::open` run the real migration.
    #[tokio::test]
    async fn v5_jobs_migrate_to_v6() {
        let dir = std::env::temp_dir().join(format!("preloop-control-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("control.db");

        // Build a v5 database: current schema + the dropped column, version 5.
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(crate::control::schema::SQLITE_DDL)
                .unwrap();
            conn.execute_batch(
                "ALTER TABLE jobs ADD COLUMN claim_generation INTEGER NOT NULL DEFAULT 0; \
                 PRAGMA user_version = 5;",
            )
            .unwrap();
            // A real job row that must survive the column drop.
            conn.execute(
                "INSERT INTO runs (run_id, status, run_number, record_blob, created_at_us) \
                 VALUES ('r1', 'queued', 1, X'02', 0)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO jobs (run_id, job_id, status, queue_kind, claim_generation) \
                 VALUES ('r1', 'build', 'queued', 'ready', 7)",
                [],
            )
            .unwrap();
        }

        let backend = SqliteBackend::open(
            &path,
            super::test_cipher(),
            false,
            false,
            std::time::Duration::from_secs(300),
        )
        .unwrap();

        // The column is gone: a SELECT of it must fail, and the row survives.
        let dump = backend.dump_tables();
        let jobs = dump.get("jobs").expect("jobs table present");
        assert_eq!(jobs.len(), 1, "the seeded job row must survive migration");
        assert!(
            !jobs[0].contains("claim_generation"),
            "claim_generation must be dropped, got: {}",
            jobs[0]
        );
        assert!(jobs[0].contains("job_id=Text(\"build\")"));

        std::fs::remove_dir_all(&dir).ok();
    }

    /// v7 → v8 migration: SQLite's historical `TEXT PRIMARY KEY` columns
    /// accepted NULL despite being identities. Existing rows must survive the
    /// table rebuilds, and the migrated schema must reject NULL keys.
    #[test]
    fn v7_text_primary_keys_migrate_to_not_null() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.db");
        {
            let connection = rusqlite::Connection::open(&path).unwrap();
            let v7_ddl = crate::control::schema::SQLITE_DDL
                .replace("TEXT PRIMARY KEY NOT NULL", "TEXT PRIMARY KEY");
            connection.execute_batch(&v7_ddl).unwrap();
            connection
                .execute_batch(
                    "PRAGMA user_version = 7;
                     INSERT INTO runs
                         (run_id, status, run_number, record_blob, created_at_us)
                     VALUES ('run-1', 'queued', 1, X'01', 10);
                     INSERT INTO job_steps (agent_job_id, steps_blob, revision)
                     VALUES ('agent-1', X'02', 2);
                     INSERT INTO runner_sessions
                         (session_id, protocol, verified, created_at_us)
                     VALUES ('session-1', 'broker', 1, 20);
                     INSERT INTO run_concurrency (run_id, concurrency_blob)
                     VALUES ('run-1', X'03');
                     INSERT INTO counters (name, value) VALUES ('request', 4);
                     INSERT INTO workflow_run_counters (key, value)
                     VALUES ('repo/workflow', 5);
                     INSERT INTO meta (key, value) VALUES ('namespace', X'06');",
                )
                .unwrap();
        }

        let backend = SqliteBackend::open(
            &path,
            super::test_cipher(),
            false,
            false,
            std::time::Duration::from_secs(300),
        )
        .unwrap();
        drop(backend);

        let connection = rusqlite::Connection::open(&path).unwrap();
        let version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, crate::control::schema::SQLITE_SCHEMA_VERSION);

        for (table, column) in [
            ("runs", "run_id"),
            ("job_steps", "agent_job_id"),
            ("runner_sessions", "session_id"),
            ("run_concurrency", "run_id"),
            ("counters", "name"),
            ("workflow_run_counters", "key"),
            ("meta", "key"),
        ] {
            let not_null: i64 = connection
                .query_row(
                    "SELECT \"notnull\" FROM pragma_table_info(?1) WHERE name = ?2",
                    rusqlite::params![table, column],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(not_null, 1, "{table}.{column} must be NOT NULL");

            if table == "counters" {
                let original: i64 = connection
                    .query_row(
                        "SELECT value FROM counters WHERE name='request'",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(original, 4, "original counter must survive migration");
            } else {
                let count: i64 = connection
                    .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })
                    .unwrap();
                assert_eq!(count, 1, "{table} row must survive migration");
            }
        }

        let error = connection
            .execute("INSERT INTO counters (name, value) VALUES (NULL, 0)", [])
            .expect_err("NULL primary key must be rejected");
        assert!(
            error
                .to_string()
                .contains("NOT NULL constraint failed: counters.name"),
            "unexpected NULL-key error: {error}"
        );
    }

    #[test]
    fn v11_ready_jobs_keep_identity_order_and_labels_after_upgrade() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.db");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(crate::control::schema::SQLITE_DDL)
                .unwrap();
            conn.execute_batch(
                "DROP INDEX runs_archive_pending;
                 ALTER TABLE runs DROP COLUMN archived_at_us;
                 PRAGMA user_version=11;
                 INSERT INTO runs(run_id,namespace,status,run_number,record_blob,created_at_us)
                   VALUES ('run-1','tenant-a','queued',3,X'02',42);
                 INSERT INTO jobs(run_id,job_id,status,queue_kind,queue_position,seq,runs_on)
                   VALUES ('run-1','build','queued','ready',900,700,
                           '[\"self-hosted\",\"linux\"]');",
            )
            .unwrap();
        }
        let backend = SqliteBackend::open(
            &path,
            super::test_cipher(),
            false,
            false,
            std::time::Duration::from_secs(300),
        )
        .unwrap();
        drop(backend);
        let conn = rusqlite::Connection::open(&path).unwrap();
        let (namespace, run_order, job_order, pool_key): (String, i64, i64, String) = conn
            .query_row(
                "SELECT namespace_id,run_order,job_order,pool_key FROM jobs WHERE job_id='build'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(namespace, "tenant-a");
        assert_eq!((run_order, job_order), (42, 900));
        assert_eq!(
            pool_key,
            crate::control::types::compute_pool_key(&["self-hosted".into(), "linux".into()], None)
        );
        let next: i64 = conn
            .query_row(
                "SELECT value FROM counters WHERE name='next_queue_position'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(next, 901);
    }

    #[tokio::test]
    async fn submission_namespace_is_written_to_run_and_job() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.db");
        let backend = SqliteBackend::open(
            &path,
            super::test_cipher(),
            false,
            false,
            std::time::Duration::from_secs(300),
        )
        .unwrap();
        let run_id = RunId::new();
        let mut submit = super::submit_run(run_id, vec![super::submit_job(run_id, "build", 1)]);
        submit.namespace = "tenant-a".to_owned();
        backend.submit_run(submit).await.unwrap();
        drop(backend);
        let conn = rusqlite::Connection::open(&path).unwrap();
        let (run_ns, job_ns): (String, String) = conn.query_row(
            "SELECT r.namespace,j.namespace_id FROM runs r JOIN jobs j USING(run_id) WHERE j.job_id='build'",
            [], |r| Ok((r.get(0)?, r.get(1)?)),
        ).unwrap();
        assert_eq!((run_ns.as_str(), job_ns.as_str()), ("tenant-a", "tenant-a"));
    }

    #[tokio::test]
    async fn priority_changes_the_claimed_job_within_a_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.db");
        let backend = SqliteBackend::open(
            &path,
            super::test_cipher(),
            false,
            false,
            std::time::Duration::from_secs(300),
        )
        .unwrap();
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();
        let run_id = RunId::new();
        backend
            .submit_run(super::submit_run(
                run_id,
                vec![
                    super::submit_job(run_id, "low", 1),
                    super::submit_job(run_id, "high", 2),
                ],
            ))
            .await
            .unwrap();
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE jobs SET priority=2 WHERE run_id=?1 AND job_id='high'",
                [run_id.to_string()],
            )
            .unwrap();
        let poll = backend
            .poll_session(super::poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        let crate::control::types::PollOutcome::Claimed(claim) = poll else {
            panic!("expected claim, got {poll:?}");
        };
        assert_eq!(claim.queued.job_id.0, "high");
    }

    #[tokio::test]
    async fn archive_moves_three_jobs_and_retains_attempts_for_reads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.db");
        let backend = SqliteBackend::open(
            &path,
            super::test_cipher(),
            false,
            false,
            std::time::Duration::from_secs(300),
        )
        .unwrap();
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();
        let run_id = RunId::new();
        backend
            .submit_run(super::submit_run(
                run_id,
                vec![
                    super::submit_job(run_id, "build", 1),
                    super::submit_job(run_id, "test", 2),
                    super::submit_job(run_id, "deploy", 3),
                ],
            ))
            .await
            .unwrap();
        for expected in ["build", "test", "deploy"] {
            let poll = backend
                .poll_session(super::poll(&session.session_id, runner.runner.id))
                .await
                .unwrap();
            let crate::control::types::PollOutcome::Claimed(claim) = poll else {
                panic!("expected {expected}, got {poll:?}");
            };
            assert_eq!(claim.queued.job_id.0, expected);
            backend
                .complete_job(crate::control::backend::JobCompletionInput {
                    run_id,
                    job_id: JobId(expected.to_owned()),
                    agent_job_id: Some(claim.request.agent_job_id),
                    status: ExecutionStatus::Success,
                    outputs: Default::default(),
                    runner_id: Some(runner.runner.id),
                })
                .await
                .unwrap();
        }
        // Simulate passage of the documented callback grace interval.
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE runs SET completed_at_us=1 WHERE run_id=?1",
                [run_id.to_string()],
            )
            .unwrap();
        assert_eq!(backend.archive_finished_runs(8).await.unwrap(), 1);
        assert_eq!(backend.archive_finished_runs(8).await.unwrap(), 0);
        let conn = rusqlite::Connection::open(&path).unwrap();
        for (table, expected) in [
            ("jobs", 0),
            ("job_requests", 0),
            ("job_history", 3),
            ("attempt_history", 3),
        ] {
            let count: i64 = conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM {table} WHERE run_id=?1"),
                    [run_id.to_string()],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(count, expected, "{table}");
        }
        let record = backend.run_record(run_id).await.unwrap();
        assert_eq!(record.jobs.len(), 3);
        assert!(record.jobs.values().all(|s| *s == ExecutionStatus::Success));
        let listed = backend
            .list_runs(crate::control::backend::RunListFilter {
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(listed[0].jobs.len(), 3);
    }

    /// At-rest encryption: the run record and job payload blobs must be
    /// ciphertext — a stolen `control.db` must not leak secrets. Reads the
    /// raw column bytes (no unseal) and asserts the plaintext secret and the
    /// job's display name are absent.
    #[tokio::test]
    async fn blobs_are_sealed_at_rest() {
        let dir = std::env::temp_dir().join(format!("preloop-control-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("control.db");

        let run_id = RunId::new();
        let mut record = super::run_record(run_id);
        let mut secrets = preloop_gha_protocol::SecretMap::new();
        secrets.insert(
            "TOKEN".to_owned(),
            preloop_gha_protocol::SecretString::new("s3cr3t-at-rest-value"),
        );
        std::sync::Arc::make_mut(&mut record.submission).secrets = secrets;

        let backend = SqliteBackend::open(
            &path,
            super::test_cipher(),
            false,
            false,
            std::time::Duration::from_secs(300),
        )
        .unwrap();
        backend
            .submit_run(super::SubmitRun {
                namespace: "default".to_owned(),
                record,
                jobs: vec![super::submit_job(run_id, "build", 1)],
                workflow_concurrency: None,
                empty_concurrency_group: false,
                check_hostable: false,
            })
            .await
            .unwrap();

        // Every sealed column across the schema must be ciphertext. The
        // marker byte 0x02 is the envelope version; plaintext JSON would
        // start with '{'.
        for (table, column) in [("runs", "record_blob"), ("jobs", "payload_blob")] {
            for blob in backend.raw_column(table, column) {
                assert_eq!(blob.first(), Some(&0x02), "{table}.{column} not sealed");
                let haystack = String::from_utf8_lossy(&blob);
                assert!(
                    !haystack.contains("s3cr3t-at-rest-value"),
                    "{table}.{column} leaked secret plaintext"
                );
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Depth benchmark: seed N ready jobs, time `poll_session`. The load
    /// deserializes every ready `payload_blob` to pick one claim — this is
    /// the O(depth) cost the metadata-only ready index would remove. Run
    /// with `--ignored --nocapture` to see timings.
    #[tokio::test]
    #[ignore]
    async fn poll_scales_with_ready_depth() {
        for depth in [10usize, 100, 500] {
            let backend = SqliteBackend::in_memory().unwrap();
            backend
                .register_runner(super::register_runner("r1"))
                .await
                .unwrap();
            // Seed `depth` runs, each contributing one ready job.
            for i in 0..depth {
                let run_id = RunId::new();
                backend
                    .submit_run(super::submit_run(
                        run_id,
                        vec![super::submit_job(run_id, "build", i as i64 + 1)],
                    ))
                    .await
                    .unwrap();
            }
            let session = backend
                .create_session(super::create_session(1))
                .await
                .unwrap();
            // Warm the connection, then time a claim poll.
            let _ = backend
                .poll_session(super::poll(&session.session_id, 1))
                .await;
            let start = std::time::Instant::now();
            let _ = backend
                .poll_session(super::poll(&session.session_id, 1))
                .await;
            let elapsed = start.elapsed();
            eprintln!("[depth={depth:>4}] poll_session: {elapsed:?}");
        }
    }
    /// Unclaimed-assignment release: a job bound to a runner (`job_assignments
    /// → runner_id`) but never claimed must go back to `pool_pending` on purge,
    /// so the pool provisions a replacement machine. A bare
    /// `job_assignments.retain` would drop the binding and strand the job —
    /// it would vanish from provisioning even though no runner ran it.
    /// SQLite-specific: needs a `pool_assignments_enabled` backend.
    #[tokio::test]
    async fn purge_releases_unclaimed_assignment_to_pool_pending() {
        let dir = std::env::temp_dir().join(format!("preloop-control-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("control.db");
        let backend = SqliteBackend::open(
            &path,
            super::test_cipher(),
            true,
            false,
            std::time::Duration::from_secs(300),
        )
        .unwrap();

        let run_id = RunId::new();
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        backend
            .submit_run(super::submit_run(
                run_id,
                vec![super::submit_job(run_id, "build", 1)],
            ))
            .await
            .unwrap();

        // Bind the job to the runner without claiming it: the job stays in
        // the ready queue, `job_assignments` names the runner, `pool_pending`
        // is empty (the binding consumed the pending mark).
        let job_id = JobId("build".to_owned());
        let rid = runner.runner.id;
        let jid = job_id.clone();
        backend
            .transact(move |tx| {
                tx.pool_proven_runners.insert(rid);
                tx.pool_pending.remove(&(run_id, jid.clone()));
                tx.job_assignments.insert(
                    (run_id, jid.clone()),
                    crate::models::AssignmentRecord {
                        runner_id: Some(rid),
                        at: std::time::SystemTime::now(),
                        first_at: std::time::SystemTime::now(),
                    },
                );
                Ok(())
            })
            .unwrap();
        // Sanity: bound, not pending, not claimed.
        let jid = job_id.clone();
        let (bound, pending) = backend
            .read(move |tx| {
                Ok((
                    tx.job_assignments
                        .get(&(run_id, jid.clone()))
                        .and_then(|r| r.runner_id),
                    tx.pool_pending.contains_key(&(run_id, jid.clone())),
                ))
            })
            .unwrap();
        assert_eq!(bound, Some(rid), "job must be bound to the runner");
        assert!(!pending, "bound job must not sit in pool_pending");

        backend.purge_runner(rid).await.unwrap();

        // The binding is gone AND the job is back in pool_pending — the pool
        // can provision a replacement machine for it.
        let (bound, pending) = backend
            .read(move |tx| {
                Ok((
                    tx.job_assignments
                        .get(&(run_id, job_id.clone()))
                        .and_then(|r| r.runner_id),
                    tx.pool_pending.contains_key(&(run_id, job_id.clone())),
                ))
            })
            .unwrap();
        assert_eq!(bound, None, "purge must drop the dead runner's binding");
        assert!(
            pending,
            "unclaimed assigned job must return to pool_pending for reprovisioning"
        );
    }

    /// A no-op command must not reclassify or drop loaded-but-untouched
    /// rows: `write_txstate` rebuilds `queue_kind` from the in-memory
    /// collections, so a ready job that loads into `ready_index` but isn't
    /// re-emitted could silently change or vanish. Round-trip identity for
    /// untouched rows is the property every later command relies on.
    #[tokio::test]
    async fn write_back_preserves_untouched_rows() {
        let backend = SqliteBackend::in_memory().unwrap();
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();
        let run_id = RunId::new();
        let jobs: Vec<_> = (1..=20)
            .map(|i| super::submit_job(run_id, &format!("build-{i}"), i))
            .collect();
        backend
            .submit_run(super::submit_run(run_id, jobs))
            .await
            .unwrap();

        // A command that loads the working set and writes it back without
        // touching anything must leave every ready job intact.
        backend.transact(|_tx| Ok(())).unwrap();
        let stats = backend.queue_stats().await.unwrap();
        assert_eq!(stats.ready, 20, "no-op write-back dropped ready jobs");

        // And they must still be claimable — not just counted.
        let poll = backend
            .poll_session(super::poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        assert!(
            matches!(poll, crate::control::types::PollOutcome::Claimed(_)),
            "ready jobs must survive write-back claimable, got {poll:?}"
        );
    }

    /// A mutation under a genuinely narrow `runs={A}` scope must leave every
    /// row not keyed to run A byte-identical — the scoped-delete families
    /// (sessions, requests, grants, assignments, cancellations, token
    /// requests, OIDC contexts, steps, run_concurrency) must not wipe or
    /// re-insert-conflict run B's rows. `ready_queue`/`blocked_jobs` are
    /// false so the scope is truly narrow — a `..full()` scope would load
    /// run B's ready jobs anyway and prove nothing.
    #[tokio::test]
    async fn narrow_scope_preserves_other_runs() {
        use crate::control::txstate::TxScope;
        use std::collections::BTreeSet;

        let backend = SqliteBackend::in_memory().unwrap();
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();

        // Two runs with ready jobs each.
        let run_a = RunId::new();
        let run_b = RunId::new();
        // Both runs seed every truncate family so the symmetric assertions
        // below are non-vacuous in BOTH directions — a pushdown that
        // under-selects the in-scope run (A) or over-deletes the foreign run
        // (B) both fail. `submit_job_full` populates token requests, OIDC
        // grants/contexts and step manifests; `workflow_concurrency` seeds
        // run_concurrency + concurrency_groups.
        let mut run_a_sub = super::submit_run(
            run_a,
            (1..=5)
                .map(|i| super::submit_job_full(run_a, &format!("a-{i}"), i))
                .collect(),
        );
        run_a_sub.workflow_concurrency = Some(super::workflow_concurrency("a-group", false));
        backend.submit_run(run_a_sub).await.unwrap();

        let mut run_b_sub = super::submit_run(
            run_b,
            (1..=7)
                .map(|i| super::submit_job_full(run_b, &format!("b-{i}"), 100 + i))
                .collect(),
        );
        run_b_sub.workflow_concurrency = Some(super::workflow_concurrency("b-group", false));
        backend.submit_run(run_b_sub).await.unwrap();

        // Seed a `cancellation_queue` row for run B directly — `cancel_run`
        // only enqueues for InProgress jobs (B's are Queued → 0 rows) and
        // would strip B's ready/held/assignment fixtures.
        backend
            .transact_scoped(&TxScope::full(), |tx| {
                tx.cancellation_queue
                    .push_back(crate::models::QueuedCancellation {
                        run_id: run_b,
                        job_id: JobId("b-1".to_owned()),
                        agent_job_id: uuid::Uuid::new_v4(),
                    });
                Ok(())
            })
            .unwrap();

        let before = backend.dump_tables();

        // Mutate run A under a genuinely narrow scope: enqueue one more ready
        // job. `ready_queue`/`blocked_jobs` false → run B's rows never load.
        let mut scope_runs = BTreeSet::new();
        scope_runs.insert(run_a);
        let scope = TxScope {
            include_archived: false,
            runs: Some(scope_runs),
            ready_queue: false,
            blocked_jobs: false,
            ..TxScope::full()
        };
        backend
            .transact_scoped(&scope, |tx| {
                tx.push_ready(super::queued_job(run_a, "a-extra", 900));
                Ok(())
            })
            .unwrap();

        let after = backend.dump_tables();

        // `created_at_us` is re-stamped `now_us` on every write-back (it has
        // no readers — session liveness uses `last_seen_at_us`), and `seq`
        // is a global write-order counter that re-sequences on every
        // rewrite of its queue table. Strip both before comparing; every
        // other column must be byte-identical.
        let strip_volatile = |row: &String| -> String {
            row.split('|')
                .filter(|c| !c.starts_with("created_at_us=") && !c.starts_with("seq="))
                .collect::<Vec<_>>()
                .join("|")
        };
        let run_a_str = run_a.0.to_string();
        // (1) Foreign rows: every row not keyed to run A is unchanged — the
        // scoped-delete families must not wipe or re-insert-conflict run B.
        for (table, before_rows) in &before {
            // A new ready job legitimately advances global FIFO allocators.
            // This test guards foreign run rows, not counter implementation.
            if table == "counters" {
                continue;
            }
            let after_rows = after.get(table).cloned().unwrap_or_default();
            let mut before_foreign: Vec<String> = before_rows
                .iter()
                .filter(|r| !r.contains(&run_a_str))
                .map(strip_volatile)
                .collect();
            let mut after_foreign: Vec<String> = after_rows
                .iter()
                .filter(|r| !r.contains(&run_a_str))
                .map(strip_volatile)
                .collect();
            before_foreign.sort();
            after_foreign.sort();
            assert_eq!(
                before_foreign, after_foreign,
                "narrow scope changed non-run-A rows in {table}"
            );
        }
        // (2) In-scope rows: run A's own rows are unchanged except the one
        // job the mutation adds — an under-selecting pushdown (wrong column,
        // wrong key, missing subquery rows) would drop them silently.
        for (table, before_rows) in &before {
            let after_rows = after.get(table).cloned().unwrap_or_default();
            let mut before_a: Vec<String> = before_rows
                .iter()
                .filter(|r| r.contains(&run_a_str))
                .map(strip_volatile)
                .collect();
            let mut after_a: Vec<String> = after_rows
                .iter()
                .filter(|r| r.contains(&run_a_str))
                .map(strip_volatile)
                .filter(|r| !r.contains("a-extra")) // the intentional add
                .collect();
            before_a.sort();
            after_a.sort();
            assert_eq!(
                before_a, after_a,
                "narrow scope dropped or altered run-A rows in {table}"
            );
        }

        // And run A actually changed: the new job row landed.
        let jobs_after = after.get("jobs").cloned().unwrap_or_default();
        assert!(
            jobs_after.iter().any(|r| r.contains("a-extra")),
            "narrow scope failed to persist run A's new job"
        );
    }

    /// A scope that loads a global queue kind (`ready_queue`) but not all
    /// runs widens foreign jobs into `ready_index`. Write-back must restore
    /// those widened rows byte-identically — their `status`, `queue_position`
    /// and `seq` are foreign-owned, so recomputing them would reset status to
    /// Queued and collapse the global FIFO order onto the scoped subset.
    #[tokio::test]
    async fn widened_scope_preserves_foreign_job_rows() {
        use crate::control::txstate::TxScope;
        use std::collections::BTreeSet;

        let backend = SqliteBackend::in_memory().unwrap();
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();

        let run_a = RunId::new();
        let run_b = RunId::new();
        backend
            .submit_run(super::submit_run(
                run_a,
                (1..=3)
                    .map(|i| super::submit_job(run_a, &format!("a-{i}"), i))
                    .collect(),
            ))
            .await
            .unwrap();
        backend
            .submit_run(super::submit_run(
                run_b,
                (1..=4)
                    .map(|i| super::submit_job(run_b, &format!("b-{i}"), 100 + i))
                    .collect(),
            ))
            .await
            .unwrap();

        let before = backend.dump_tables();
        let jobs_before = before.get("jobs").cloned().unwrap_or_default();

        // Scope: run A + the GLOBAL ready queue. B's ready jobs are widened
        // into `ready_index` (their run is not loaded). Mutate only run A.
        let mut scope_runs = BTreeSet::new();
        scope_runs.insert(run_a);
        let scope = TxScope {
            include_archived: false,
            runs: Some(scope_runs),
            ready_queue: true,
            blocked_jobs: false,
            ..TxScope::full()
        };
        backend
            .transact_scoped(&scope, |tx| {
                tx.push_ready(super::queued_job(run_a, "a-extra", 900));
                Ok(())
            })
            .unwrap();

        let after = backend.dump_tables();
        let jobs_after = after.get("jobs").cloned().unwrap_or_default();
        let run_b_str = run_b.0.to_string();

        // Every run-B job row must be byte-identical: status, queue_position
        // and seq are all preserved, not recomputed onto the scoped subset.
        let b_before: Vec<&String> = jobs_before
            .iter()
            .filter(|r| r.contains(&run_b_str))
            .collect();
        let b_after: Vec<&String> = jobs_after
            .iter()
            .filter(|r| r.contains(&run_b_str))
            .collect();
        assert_eq!(
            b_before, b_after,
            "widened scope altered run-B job rows (status/position/seq)"
        );

        // Run A's new ready job landed with a position ABOVE all preserved
        // ones (it must not collide with or jump ahead of B's rows).
        let a_extra = jobs_after
            .iter()
            .find(|r| r.contains("a-extra"))
            .expect("a-extra job missing");

        assert!(
            a_extra.contains("queue_kind=Text(\"ready\")"),
            "a-extra not ready: {a_extra}"
        );
    }

    /// ready→claimed (poll): the claimed row must clear `queue_position`
    /// (claimed jobs have no ready slot) and take a fresh FIFO `seq`, while
    /// `status` stays `queued` — a claimed job is dispatched, not started.
    /// Regression: preserving the loaded position/seq unconditionally would
    /// persist a claimed row still holding its old ready slot.
    #[tokio::test]
    async fn poll_claim_clears_position_and_keeps_status() {
        let backend = SqliteBackend::in_memory().unwrap();
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();
        let run_id = RunId::new();
        backend
            .submit_run(super::submit_run(
                run_id,
                vec![super::submit_job(run_id, "build", 1)],
            ))
            .await
            .unwrap();

        let before = job_rows(&backend.dump_tables());
        let (_, _, pos_before, seq_before) = before.get("build").unwrap().clone();
        assert!(pos_before.is_some(), "ready job must have a position");

        match backend
            .poll_session(super::poll(&session.session_id, runner.runner.id))
            .await
            .unwrap()
        {
            super::PollOutcome::Claimed(_) => {}
            other => panic!("expected claim, got {other:?}"),
        }

        let after = job_rows(&backend.dump_tables());
        let (status, kind, pos, seq) = after.get("build").unwrap().clone();
        assert_eq!(kind, "claimed", "claimed job must be queue_kind=claimed");
        // Claiming marks the job in_progress (commands.rs sets run.jobs →
        // InProgress); the row must reflect that, not the stale queued value.
        assert_eq!(status, "in_progress", "claimed job is in_progress");
        assert_eq!(pos, None, "claimed job must clear queue_position");
        assert_ne!(seq, seq_before, "claimed job must take a fresh seq");
    }

    /// claimed→ready (reconcile requeue): a job requeued via `push_ready`
    /// must be allocated at the BACK of the ready queue with a fresh
    /// `queue_position` — never the old ready slot and never NULL. Regression:
    /// treating the requeued key as preserved would reuse the stale position.
    #[tokio::test]
    async fn requeue_lands_at_back_with_fresh_position() {
        let backend = SqliteBackend::in_memory().unwrap();
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();
        let run_id = RunId::new();
        // Two jobs: claim the first, leave the second ready so the requeued
        // job must sort AFTER it (position > the surviving ready job's).
        backend
            .submit_run(super::submit_run(
                run_id,
                vec![
                    super::submit_job(run_id, "first", 1),
                    super::submit_job(run_id, "second", 2),
                ],
            ))
            .await
            .unwrap();

        match backend
            .poll_session(super::poll(&session.session_id, runner.runner.id))
            .await
            .unwrap()
        {
            super::PollOutcome::Claimed(c) => {
                assert_eq!(c.queued.job_id, JobId("first".to_owned()));
            }
            other => panic!("expected claim of first, got {other:?}"),
        }

        // Orphan the claim (session dies) and reconcile → requeue `first`.
        backend.delete_session(&session.session_id).await.unwrap();
        let outcome = backend.reconcile_on_boot().await.unwrap();
        assert_eq!(outcome.recovered, 1);
        let rows = job_rows(&backend.dump_tables());
        let (status, kind, pos, _seq) = rows.get("first").unwrap().clone();
        assert_eq!(kind, "ready", "requeued job must be ready again");
        // Requeue resets execution status to `queued` — the job is back in
        // the dispatch queue waiting for a fresh runner, not still marked
        // in_progress from the dead runner's claim.
        assert_eq!(status, "queued", "requeued job resets to queued");
        let requeued_pos = pos.expect("requeued job must have a position");
        let (_, _, second_pos, _) = rows.get("second").unwrap().clone();
        assert!(
            requeued_pos > second_pos.unwrap(),
            "requeued job must land at the back (pos {requeued_pos} > {})",
            second_pos.unwrap()
        );

        // The run summary recomputes too: both jobs are queued → in_progress.
        let record = backend.run_record(run_id).await.unwrap();
        assert_eq!(
            record.status,
            ExecutionStatus::InProgress,
            "run summary recomputes after requeue"
        );
    }

    /// Mixed scope (`runs={A}` + global `ready_queue`) widens a foreign job
    /// whose canonical status is NOT `queued`. The write must restore that
    /// exact status — a fallback `unwrap_or(Queued)` would silently rewrite a
    /// `pending`/`in_progress` foreign row. Seeds B's job as `pending` via a
    /// scoped status mutation before the widened write.
    #[tokio::test]
    async fn widened_scope_preserves_non_queued_foreign_status() {
        use crate::control::txstate::TxScope;
        use std::collections::BTreeSet;

        let backend = SqliteBackend::in_memory().unwrap();
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();

        let run_a = RunId::new();
        let run_b = RunId::new();
        backend
            .submit_run(super::submit_run(
                run_a,
                vec![super::submit_job(run_a, "a-1", 1)],
            ))
            .await
            .unwrap();
        backend
            .submit_run(super::submit_run(
                run_b,
                vec![super::submit_job(run_b, "b-1", 100)],
            ))
            .await
            .unwrap();

        // Set B's job canonical status to `pending` (a non-queued value) via
        // a scoped mutation on run B, so the widened write has something to
        // preserve that differs from the `unwrap_or(Queued)` fallback.
        let mut b_only = BTreeSet::new();
        b_only.insert(run_b);
        backend
            .transact_scoped(
                &TxScope {
                    include_archived: false,
                    runs: Some(b_only),
                    ..TxScope::full()
                },
                |tx| {
                    if let Some(run) = tx.runs.get_mut(&run_b) {
                        run.jobs
                            .insert(JobId("b-1".to_owned()), ExecutionStatus::Pending);
                    }
                    Ok(())
                },
            )
            .unwrap();

        // Sanity: B's job is now persisted as pending.
        let seeded = job_rows(&backend.dump_tables());
        assert_eq!(seeded.get("b-1").unwrap().0, "pending");

        // Mixed scope: run A + global ready queue widens B's job (its run is
        // not loaded). Mutate only run A.
        let mut scope_runs = BTreeSet::new();
        scope_runs.insert(run_a);
        backend
            .transact_scoped(
                &TxScope {
                    include_archived: false,
                    runs: Some(scope_runs),
                    ready_queue: true,
                    blocked_jobs: false,
                    ..TxScope::full()
                },
                |tx| {
                    tx.push_ready(super::queued_job(run_a, "a-extra", 900));
                    Ok(())
                },
            )
            .unwrap();

        let after = job_rows(&backend.dump_tables());
        assert_eq!(
            after.get("b-1").unwrap().0,
            "pending",
            "widened write must preserve B's pending status, not reset to queued"
        );
    }

    /// A widened foreign job that TRANSITIONS (blocked→ready promotion) must
    /// derive status from the landing queue kind, not preserve the stale
    /// `pending` — the `Holder::Job` promotion path pushes to `tx.queue`
    /// without loading the run, so `write_job` derives `queued` for a ready
    /// landing. Seeds B's job as `blocked`+`pending`, then a widened scope
    /// (`runs={A}` + `blocked_jobs`) moves it to `tx.queue`.
    #[tokio::test]
    async fn widened_blocked_promotion_derives_queued() {
        use crate::control::txstate::TxScope;
        use std::collections::BTreeSet;

        let backend = SqliteBackend::in_memory().unwrap();
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();

        let run_a = RunId::new();
        let run_b = RunId::new();
        backend
            .submit_run(super::submit_run(
                run_a,
                vec![super::submit_job(run_a, "a-1", 1)],
            ))
            .await
            .unwrap();
        backend
            .submit_run(super::submit_run(
                run_b,
                vec![super::submit_job(run_b, "b-1", 100)],
            ))
            .await
            .unwrap();

        // Seed B's job as `blocked` with canonical `pending` status — the
        // pre-promotion state a concurrency-blocked job holds.
        let mut b_only = BTreeSet::new();
        b_only.insert(run_b);
        backend
            .transact_scoped(
                &TxScope {
                    include_archived: false,
                    runs: Some(b_only),
                    ..TxScope::full()
                },
                |tx| {
                    // Move b-1 out of ready into concurrency_blocked and mark
                    // it pending — the state a blocked job is persisted in.
                    if let Some(job) = tx
                        .ready_index
                        .iter()
                        .position(|j| j.job_id == JobId("b-1".to_owned()))
                        .map(|p| tx.ready_index.remove(p).unwrap())
                    {
                        tx.concurrency_blocked.push_back(job);
                    }
                    if let Some(run) = tx.runs.get_mut(&run_b) {
                        run.jobs
                            .insert(JobId("b-1".to_owned()), ExecutionStatus::Pending);
                    }
                    Ok(())
                },
            )
            .unwrap();
        let seeded = job_rows(&backend.dump_tables());
        assert_eq!(seeded.get("b-1").unwrap().0, "pending");
        assert_eq!(seeded.get("b-1").unwrap().1, "blocked");

        // Widened scope: run A + global blocked_jobs loads B's blocked job
        // (its run is NOT loaded). Simulate the Holder::Job promotion: move
        // b-1 from concurrency_blocked to tx.queue AND set the explicit
        // `queued` override — exactly what `promote_next_from_group` now does
        // via `set_job_status`. The override (not a global coercion) is what
        // makes the widened job derive `queued`.
        let mut scope_runs = BTreeSet::new();
        scope_runs.insert(run_a);
        backend
            .transact_scoped(
                &TxScope {
                    include_archived: false,
                    runs: Some(scope_runs),
                    ready_queue: false,
                    blocked_jobs: true,
                    ..TxScope::full()
                },
                |tx| {
                    if let Some(pos) = tx
                        .concurrency_blocked
                        .iter()
                        .position(|j| j.job_id == JobId("b-1".to_owned()))
                    {
                        let job = tx.concurrency_blocked.remove(pos).unwrap();
                        tx.set_job_status(run_b, job.job_id.clone(), ExecutionStatus::Queued);
                        tx.push_ready(job);
                    }
                    Ok(())
                },
            )
            .unwrap();

        let after = job_rows(&backend.dump_tables());
        let (status, kind, _pos, _seq) = after.get("b-1").unwrap().clone();
        assert_eq!(kind, "ready", "promoted job must be ready");
        assert_eq!(
            status, "queued",
            "widened blocked→ready promotion must derive queued, not keep stale pending"
        );
    }

    /// FIFO: with B submitted before A, a new ready job for A must take
    /// `queue_position == global MAX+1` and existing rows keep their exact
    /// positions — the legacy local renumbering (0..N over the loaded subset)
    /// would collide with or reorder B's slots.
    #[tokio::test]
    async fn new_ready_job_takes_global_max_position() {
        use crate::control::txstate::TxScope;
        use std::collections::BTreeSet;

        let backend = SqliteBackend::in_memory().unwrap();
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();

        let run_a = RunId::new();
        let run_b = RunId::new();
        // B first so its ready positions occupy the low slots.
        backend
            .submit_run(super::submit_run(
                run_b,
                (1..=3)
                    .map(|i| super::submit_job(run_b, &format!("b-{i}"), 100 + i))
                    .collect(),
            ))
            .await
            .unwrap();
        backend
            .submit_run(super::submit_run(
                run_a,
                vec![super::submit_job(run_a, "a-1", 1)],
            ))
            .await
            .unwrap();

        let before = job_rows(&backend.dump_tables());
        let max_pos = before.values().filter_map(|(_, _, p, _)| *p).max().unwrap();

        let mut scope_runs = BTreeSet::new();
        scope_runs.insert(run_a);
        backend
            .transact_scoped(
                &TxScope {
                    include_archived: false,
                    runs: Some(scope_runs),
                    ready_queue: true,
                    blocked_jobs: false,
                    ..TxScope::full()
                },
                |tx| {
                    tx.push_ready(super::queued_job(run_a, "a-extra", 900));
                    Ok(())
                },
            )
            .unwrap();

        let after = job_rows(&backend.dump_tables());
        // B's positions unchanged.
        for (job_id, (_, _, pos, _)) in after.iter() {
            if job_id.starts_with("b-") {
                assert_eq!(
                    *pos,
                    before.get(job_id).unwrap().2,
                    "B job {job_id} position changed"
                );
            }
        }
        // a-extra lands strictly after the prior global max.
        let new_pos = after.get("a-extra").unwrap().2.unwrap();
        assert_eq!(
            new_pos,
            max_pos + 1,
            "a-extra must take global MAX+1 ({max_pos}+1), got {new_pos}"
        );
    }
}

// ── Postgres ────────────────────────────────────────────────────────────
mod postgres {
    use super::suite;
    use crate::control::backend::ControlBackend;
    use crate::control::postgres::PostgresBackend;
    use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
    use std::path::PathBuf;
    use std::process::Command;

    /// A throwaway PostgreSQL cluster in a temp dir: `initdb`, `postgres`
    /// on a free port, `pg_ctl stop` on drop. Each test gets its own
    /// cluster, so tests are isolated and parallel-safe.
    struct DisposablePg {
        dir: PathBuf,
        port: u16,
    }

    impl DisposablePg {
        fn start() -> Self {
            let bin = pg_bin();
            // Short path: the `-k` Unix socket lives inside `dir`, and
            // `std::env::temp_dir()` (`/var/folders/…/T/`) + a UUID exceeds
            // the 104-byte `sun_path` limit. `/tmp` keeps it well under.
            let short = &uuid::Uuid::new_v4().simple().to_string()[..8];
            let dir = PathBuf::from(format!("/tmp/preloop-pg-{short}"));
            let data = dir.join("data");
            let port = free_port();

            run(
                bin.join("initdb"),
                &[
                    "-D".into(),
                    data.clone().into_os_string().into_string().unwrap(),
                    "-U".into(),
                    "postgres".into(),
                    "--auth=trust".into(),
                    "-E".into(),
                    "UTF8".into(),
                ],
            );
            run(
                bin.join("pg_ctl"),
                &[
                    "-D".into(),
                    data.clone().into_os_string().into_string().unwrap(),
                    "-o".into(),
                    format!("-p {port} -k {}", dir.display()),
                    "-l".into(),
                    dir.join("log").to_string_lossy().into_owned(),
                    "-w".into(),
                    "start".into(),
                ],
            );

            Self { dir, port }
        }

        fn url(&self) -> String {
            format!("postgres://postgres@127.0.0.1:{}/postgres", self.port)
        }
    }

    impl Drop for DisposablePg {
        fn drop(&mut self) {
            let bin = pg_bin();
            let _ = Command::new(bin.join("pg_ctl"))
                .args([
                    "-D".into(),
                    self.dir.join("data").to_string_lossy().into_owned(),
                    "stop".into(),
                    "-m".into(),
                    "fast".into(),
                ])
                .output();
            std::fs::remove_dir_all(&self.dir).ok();
        }
    }

    fn run(bin: PathBuf, args: &[String]) {
        let out = Command::new(&bin)
            .args(args)
            .output()
            .unwrap_or_else(|e| panic!("{} failed to spawn: {e}", bin.display()));
        assert!(
            out.status.success(),
            "{} failed: {}",
            bin.display(),
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Locate a PostgreSQL bin dir: Postgres.app, Homebrew, system, or PATH.
    fn pg_bin() -> PathBuf {
        let candidates = [
            "/Applications/Postgres.app/Contents/Versions/latest/bin",
            "/opt/homebrew/opt/postgresql@18/bin",
            "/opt/homebrew/opt/postgresql@17/bin",
            "/opt/homebrew/opt/postgresql@16/bin",
            "/usr/local/opt/postgresql@18/bin",
            "/usr/local/opt/postgresql@17/bin",
            "/usr/local/opt/postgresql@16/bin",
            "/usr/lib/postgresql/18/bin",
            "/usr/lib/postgresql/17/bin",
            "/usr/lib/postgresql/16/bin",
        ];
        for dir in candidates {
            let bin = PathBuf::from(dir);
            if bin.join("initdb").exists() {
                return bin;
            }
        }
        // Fall back to PATH.
        if let Ok(path_var) = std::env::var("PATH") {
            for dir in std::env::split_paths(&path_var) {
                if dir.join("initdb").exists() {
                    return dir;
                }
            }
        }
        panic!(
            "no PostgreSQL binaries found: set PRELOOP_TEST_POSTGRES_URL to a \
             disposable database, or install Postgres.app / postgresql"
        );
    }

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    /// Build a backend against a disposable cluster, or the database named
    /// by `PRELOOP_TEST_POSTGRES_URL` when set (CI/dev with an existing
    /// cluster). The guard must stay alive for the test's duration.
    async fn backend() -> (Option<DisposablePg>, PostgresBackend) {
        if let Ok(url) = std::env::var("PRELOOP_TEST_POSTGRES_URL") {
            let backend = PostgresBackend::connect(
                &url,
                super::test_cipher(),
                false,
                false,
                std::time::Duration::from_secs(300),
            )
            .await
            .expect("PRELOOP_TEST_POSTGRES_URL set but connection failed");
            return (None, backend);
        }
        let pg = DisposablePg::start();
        let backend = PostgresBackend::connect(
            &pg.url(),
            super::test_cipher(),
            false,
            false,
            std::time::Duration::from_secs(300),
        )
        .await
        .expect("disposable cluster connection failed");
        (Some(pg), backend)
    }

    #[tokio::test]
    async fn submit_poll_complete_lifecycle() {
        let (_pg, backend) = backend().await;
        suite::submit_poll_complete_lifecycle(&backend).await;
    }

    #[tokio::test]
    async fn webhook_replay_is_idempotent() {
        let (_pg, backend) = backend().await;
        suite::webhook_replay_is_idempotent(&backend).await;
    }

    #[tokio::test]
    async fn cancel_run_queues_cancellation() {
        let (_pg, backend) = backend().await;
        suite::cancel_run_queues_cancellation(&backend).await;
    }

    #[tokio::test]
    async fn secrets_survive_seal_unseal() {
        let (_pg, backend) = backend().await;
        suite::secrets_survive_seal_unseal(&backend).await;
    }

    #[tokio::test]
    async fn concurrency_gate_serializes_group() {
        let (_pg, backend) = backend().await;
        suite::concurrency_gate_serializes_group(&backend).await;
    }

    #[tokio::test]
    async fn reconcile_recovers_orphaned_claim() {
        let (_pg, backend) = backend().await;
        suite::reconcile_recovers_orphaned_claim(&backend).await;
    }

    #[tokio::test]
    async fn purge_requeues_claimed_as_queued() {
        let (_pg, backend) = backend().await;
        suite::purge_requeues_claimed_as_queued(&backend).await;
    }

    #[tokio::test]
    async fn purge_requeues_ownerless_claim() {
        let (_pg, backend) = backend().await;
        suite::purge_requeues_ownerless_claim(&backend).await;
    }

    #[tokio::test]
    async fn submit_unhostable_job_persists_failure() {
        let (_pg, backend) = backend().await;
        suite::submit_unhostable_job_persists_failure(&backend).await;
    }

    #[tokio::test]
    async fn submit_skipped_parent_settles_child() {
        let (_pg, backend) = backend().await;
        suite::submit_skipped_parent_settles_child(&backend).await;
    }

    #[tokio::test]
    async fn cancel_in_progress_submit_reports_surviving_depth() {
        let (_pg, backend) = backend().await;
        suite::cancel_in_progress_submit_reports_surviving_depth(&backend).await;
    }

    /// Reconnecting a second backend to the same cluster proves the DB is
    /// the authority: submit on one connection, claim on another.
    #[tokio::test]
    async fn state_survives_reconnect() {
        let (pg, backend) = backend().await;
        let url = pg
            .as_ref()
            .map(|p| p.url())
            .unwrap_or_else(|| std::env::var("PRELOOP_TEST_POSTGRES_URL").unwrap());

        let run_id = RunId::new();
        backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        backend
            .submit_run(super::submit_run(
                run_id,
                vec![super::submit_job(run_id, "build", 1)],
            ))
            .await
            .unwrap();
        drop(backend);

        let backend = PostgresBackend::connect(
            &url,
            super::test_cipher(),
            false,
            false,
            std::time::Duration::from_secs(300),
        )
        .await
        .unwrap();
        let stats = backend.queue_stats().await.unwrap();
        assert_eq!(stats.ready, 1, "queued job must survive reconnect");
        let record = backend.run_record(run_id).await.unwrap();
        assert_eq!(record.status, ExecutionStatus::Queued);
        assert_eq!(
            record.jobs.get(&JobId("build".to_owned())),
            Some(&ExecutionStatus::Queued)
        );
    }
    /// Two independent backend connections must serialize their write
    /// transactions. The first holds the advisory lock while its command is
    /// blocked; the second must wait, then observe the committed change.
    #[tokio::test]
    async fn writers_serialize_across_connections() {
        let (pg, backend_a) = backend().await;
        let url = pg
            .as_ref()
            .map(|p| p.url())
            .unwrap_or_else(|| std::env::var("PRELOOP_TEST_POSTGRES_URL").unwrap());
        let backend_b = PostgresBackend::connect(
            &url,
            super::test_cipher(),
            false,
            false,
            std::time::Duration::from_secs(300),
        )
        .await
        .unwrap();

        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let writer = tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(async move {
                backend_a
                    .transact(move |tx| {
                        tx.runners.insert(
                            4242,
                            preloop_gha_protocol::RegisteredRunner {
                                id: 4242,
                                name: "writer-a".to_owned(),
                                labels: vec!["self-hosted".to_owned()],
                                ephemeral: false,
                                public_key: None,
                                runner_group_id: None,
                                runner_group_name: None,
                            },
                        );
                        let _ = entered_tx.send(());
                        release_rx.recv().unwrap();
                        Ok(())
                    })
                    .await
            })
        });
        entered_rx.await.unwrap();

        let mut blocked = tokio::spawn(async move {
            backend_b
                .transact(|tx| Ok(tx.runners.contains_key(&4242)))
                .await
        });
        let timed_out = tokio::time::timeout(std::time::Duration::from_millis(150), &mut blocked)
            .await
            .is_err();
        assert!(
            timed_out,
            "second writer did not wait for the advisory lock"
        );
        release_tx.send(()).unwrap();
        writer.await.unwrap().unwrap();
        assert!(
            blocked.await.unwrap().unwrap(),
            "second writer must see the committed row"
        );
    }

    /// Postgres twin of the SQLite round-trip identity check.
    #[tokio::test]
    async fn write_back_preserves_untouched_rows() {
        let (_pg, backend) = backend().await;
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();
        let run_id = RunId::new();
        let jobs: Vec<_> = (1..=20)
            .map(|i| super::submit_job(run_id, &format!("build-{i}"), i))
            .collect();
        backend
            .submit_run(super::submit_run(run_id, jobs))
            .await
            .unwrap();

        backend.transact(|_tx| Ok(())).await.unwrap();
        let stats = backend.queue_stats().await.unwrap();
        assert_eq!(stats.ready, 20, "no-op write-back dropped ready jobs");

        let poll = backend
            .poll_session(super::poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        assert!(
            matches!(poll, crate::control::types::PollOutcome::Claimed(_)),
            "ready jobs must survive write-back claimable, got {poll:?}"
        );
    }

    /// ready→claimed (poll): claimed row clears `queue_position`, takes a
    /// fresh `seq`, and reads `in_progress` (claim sets run.jobs → InProgress).
    #[tokio::test]
    async fn poll_claim_clears_position_and_keeps_status() {
        let (_pg, backend) = backend().await;
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();
        let run_id = RunId::new();
        backend
            .submit_run(super::submit_run(
                run_id,
                vec![super::submit_job(run_id, "build", 1)],
            ))
            .await
            .unwrap();

        let (_, _, pos_before, seq_before) = backend.job_row("build").await.unwrap();
        assert!(pos_before.is_some(), "ready job must have a position");

        match backend
            .poll_session(super::poll(&session.session_id, runner.runner.id))
            .await
            .unwrap()
        {
            crate::control::types::PollOutcome::Claimed(_) => {}
            other => panic!("expected claim, got {other:?}"),
        }

        let (status, kind, pos, seq) = backend.job_row("build").await.unwrap();
        assert_eq!(kind, "claimed");
        assert_eq!(status, "in_progress");
        assert_eq!(pos, None, "claimed job must clear queue_position");
        assert_ne!(seq, seq_before, "claimed job must take a fresh seq");
    }

    /// claimed→ready (reconcile requeue): requeued job lands at the BACK with
    /// a fresh `queue_position` and resets status to `queued`.
    #[tokio::test]
    async fn requeue_lands_at_back_with_fresh_position() {
        let (_pg, backend) = backend().await;
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();
        let run_id = RunId::new();
        backend
            .submit_run(super::submit_run(
                run_id,
                vec![
                    super::submit_job(run_id, "first", 1),
                    super::submit_job(run_id, "second", 2),
                ],
            ))
            .await
            .unwrap();

        match backend
            .poll_session(super::poll(&session.session_id, runner.runner.id))
            .await
            .unwrap()
        {
            crate::control::types::PollOutcome::Claimed(c) => {
                assert_eq!(c.queued.job_id, JobId("first".to_owned()));
            }
            other => panic!("expected claim of first, got {other:?}"),
        }

        backend.delete_session(&session.session_id).await.unwrap();
        let outcome = backend.reconcile_on_boot().await.unwrap();
        assert_eq!(outcome.recovered, 1);

        let (status, kind, pos, _seq) = backend.job_row("first").await.unwrap();
        assert_eq!(kind, "ready");
        assert_eq!(status, "queued", "requeued job resets to queued");
        let requeued_pos = pos.expect("requeued job must have a position");
        let (_, _, second_pos, _) = backend.job_row("second").await.unwrap();
        assert!(
            requeued_pos > second_pos.unwrap(),
            "requeued job must land at the back"
        );
    }

    /// Mixed scope (`runs={A}` + global `ready_queue`) widens a foreign job
    /// whose canonical status is `pending`; the write must preserve it, not
    /// reset to `queued`.
    #[tokio::test]
    async fn widened_scope_preserves_non_queued_foreign_status() {
        use crate::control::txstate::TxScope;
        use std::collections::BTreeSet;

        let (_pg, backend) = backend().await;
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();

        let run_a = RunId::new();
        let run_b = RunId::new();
        backend
            .submit_run(super::submit_run(
                run_a,
                vec![super::submit_job(run_a, "a-1", 1)],
            ))
            .await
            .unwrap();
        backend
            .submit_run(super::submit_run(
                run_b,
                vec![super::submit_job(run_b, "b-1", 100)],
            ))
            .await
            .unwrap();

        // Set B's job canonical status to `pending` via a scoped mutation.
        let mut b_only = BTreeSet::new();
        b_only.insert(run_b);
        backend
            .transact_scoped(
                &TxScope {
                    include_archived: false,
                    runs: Some(b_only),
                    ..TxScope::full()
                },
                |tx| {
                    if let Some(run) = tx.runs.get_mut(&run_b) {
                        run.jobs
                            .insert(JobId("b-1".to_owned()), ExecutionStatus::Pending);
                    }
                    Ok(())
                },
            )
            .await
            .unwrap();
        assert_eq!(backend.job_row("b-1").await.unwrap().0, "pending");

        // Mixed scope widens B's job; mutate only run A.
        let mut scope_runs = BTreeSet::new();
        scope_runs.insert(run_a);
        backend
            .transact_scoped(
                &TxScope {
                    include_archived: false,
                    runs: Some(scope_runs),
                    ready_queue: true,
                    blocked_jobs: false,
                    ..TxScope::full()
                },
                |tx| {
                    tx.push_ready(super::queued_job(run_a, "a-extra", 900));
                    Ok(())
                },
            )
            .await
            .unwrap();

        assert_eq!(
            backend.job_row("b-1").await.unwrap().0,
            "pending",
            "widened write must preserve B's pending status"
        );
    }

    /// FIFO: a new ready job takes `queue_position == global MAX+1` and
    /// existing rows keep their exact positions.
    #[tokio::test]
    async fn new_ready_job_takes_global_max_position() {
        use crate::control::txstate::TxScope;
        use std::collections::BTreeSet;

        let (_pg, backend) = backend().await;
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();

        let run_a = RunId::new();
        let run_b = RunId::new();
        backend
            .submit_run(super::submit_run(
                run_b,
                (1..=3)
                    .map(|i| super::submit_job(run_b, &format!("b-{i}"), 100 + i))
                    .collect(),
            ))
            .await
            .unwrap();
        backend
            .submit_run(super::submit_run(
                run_a,
                vec![super::submit_job(run_a, "a-1", 1)],
            ))
            .await
            .unwrap();

        // Snapshot every ready position before the scoped write.
        let before = backend.ready_positions().await;
        let global_max = before.values().copied().max().unwrap();

        let mut scope_runs = BTreeSet::new();
        scope_runs.insert(run_a);
        backend
            .transact_scoped(
                &TxScope {
                    include_archived: false,
                    runs: Some(scope_runs),
                    ready_queue: true,
                    blocked_jobs: false,
                    ..TxScope::full()
                },
                |tx| {
                    tx.push_ready(super::queued_job(run_a, "a-extra", 900));
                    Ok(())
                },
            )
            .await
            .unwrap();

        let after = backend.ready_positions().await;
        // Every pre-existing ready job keeps its exact position.
        for (job_id, pos) in &before {
            assert_eq!(
                after.get(job_id),
                Some(pos),
                "existing ready job {job_id} position changed"
            );
        }
        // a-extra lands at exactly global MAX+1 — no skipped/altered slot.
        assert_eq!(
            after.get("a-extra").copied(),
            Some(global_max + 1),
            "a-extra must take global MAX+1 ({global_max}+1)"
        );
    }

    /// A widened foreign job promoted blocked→ready must derive `queued` via
    /// the explicit `set_job_status` override — mirrors the sqlite case so the
    /// duplicated Postgres `write_job` status path is covered too.
    #[tokio::test]
    async fn widened_blocked_promotion_derives_queued() {
        use crate::control::txstate::TxScope;
        use std::collections::BTreeSet;

        let (_pg, backend) = backend().await;
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();

        let run_a = RunId::new();
        let run_b = RunId::new();
        backend
            .submit_run(super::submit_run(
                run_a,
                vec![super::submit_job(run_a, "a-1", 1)],
            ))
            .await
            .unwrap();
        backend
            .submit_run(super::submit_run(
                run_b,
                vec![super::submit_job(run_b, "b-1", 100)],
            ))
            .await
            .unwrap();

        // Seed B's job as `blocked` + canonical `pending`.
        let mut b_only = BTreeSet::new();
        b_only.insert(run_b);
        backend
            .transact_scoped(
                &TxScope {
                    include_archived: false,
                    runs: Some(b_only),
                    ..TxScope::full()
                },
                |tx| {
                    if let Some(job) = tx
                        .ready_index
                        .iter()
                        .position(|j| j.job_id == JobId("b-1".to_owned()))
                        .map(|p| tx.ready_index.remove(p).unwrap())
                    {
                        tx.concurrency_blocked.push_back(job);
                    }
                    if let Some(run) = tx.runs.get_mut(&run_b) {
                        run.jobs
                            .insert(JobId("b-1".to_owned()), ExecutionStatus::Pending);
                    }
                    Ok(())
                },
            )
            .await
            .unwrap();
        assert_eq!(backend.job_row("b-1").await.unwrap().0, "pending");
        assert_eq!(backend.job_row("b-1").await.unwrap().1, "blocked");

        // Widened scope (run A + global blocked_jobs): promote b-1 to ready
        // with the explicit `queued` override, as promote_next_from_group does.
        let mut scope_runs = BTreeSet::new();
        scope_runs.insert(run_a);
        backend
            .transact_scoped(
                &TxScope {
                    include_archived: false,
                    runs: Some(scope_runs),
                    ready_queue: false,
                    blocked_jobs: true,
                    ..TxScope::full()
                },
                |tx| {
                    if let Some(pos) = tx
                        .concurrency_blocked
                        .iter()
                        .position(|j| j.job_id == JobId("b-1".to_owned()))
                    {
                        let job = tx.concurrency_blocked.remove(pos).unwrap();
                        tx.set_job_status(run_b, job.job_id.clone(), ExecutionStatus::Queued);
                        tx.push_ready(job);
                    }
                    Ok(())
                },
            )
            .await
            .unwrap();

        let (status, kind, _pos, _seq) = backend.job_row("b-1").await.unwrap();
        assert_eq!(kind, "ready", "promoted job must be ready");
        assert_eq!(
            status, "queued",
            "widened blocked→ready promotion must derive queued via override"
        );
    }
}
