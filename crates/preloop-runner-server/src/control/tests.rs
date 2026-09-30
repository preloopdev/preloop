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
use crate::config::EnvironmentRulesMap;
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
// Shared submits mimic server-assigned run numbers. The PostgreSQL/lite schema
// enforces uniqueness on (namespace, repository, workflow, number, attempt).
static NEXT_FIXTURE_RUN_NUMBER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

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
        fork_approval_pending: false,
        fork_approval_requested_at_unix_nanos: None,
        fork_approved_at_unix_nanos: None,
        fork_approval_note: None,
        reports_check_runs: false,
    }
}

/// Run equality via the full persisted JSON form. The fixture below contains
/// only fields represented by the normalized run/job tables; secret values
/// never reach the database (`secret_values_never_persist`).
fn assert_same_run(actual: &RunRecord, expected: &RunRecord) {
    let mut actual = actual.clone();
    let mut expected = expected.clone();
    actual.jobs.clear();
    expected.jobs.clear();
    let actual = crate::store::run_record_value(&actual).unwrap();
    let expected = crate::store::run_record_value(&expected).unwrap();
    for (key, want) in expected.as_object().unwrap() {
        assert_eq!(
            actual.get(key),
            Some(want),
            "run record field `{key}` must round-trip through its tables unchanged"
        );
    }
    assert_eq!(actual, expected);
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
        environment_gate: None,
    }
}

/// A job exercising every decomposed payload column, `needs:` order and the
/// sealed message/context. Timestamps are whole microseconds so the
/// `enqueued_at_us` column round-trips exactly.
fn rich_job(run_id: RunId, job_id: &str) -> QueuedJob {
    let mut job = queued_job(run_id, job_id, 77);
    job.created_at_unix_nanos = 1_700_000_000_000_001_000;
    job.dependencies_ready_at_unix_nanos = Some(1_700_000_000_000_002_000);
    job.concurrency_wait_started_at_unix_nanos = Some(1_700_000_000_000_003_000);
    job.concurrency_acquired_at_unix_nanos = Some(1_700_000_000_000_004_000);
    job.enqueued_at_unix_nanos = 1_700_000_000_000_005_000;
    job.needs = vec![JobId("zeta".to_owned()), JobId("alpha".to_owned())];
    job.if_condition = Some("${{ success() && matrix.os == 'linux' }}".to_owned());
    job.max_parallel = Some(3);
    job.runs_on = vec!["self-hosted".to_owned(), "linux".to_owned()];
    job.runner_group = Some("builders".to_owned());
    job.environment = Some(serde_json::json!({"name": "prod", "url": "https://x"}));
    job.matrix = BTreeMap::from([("os".to_owned(), serde_json::json!("linux"))]);
    job.deferred_matrix = Some("${{ fromJSON(needs.plan.outputs.m) }}".to_owned());
    job
}

/// Payload equality via the serialized form (`QueuedJob` has no `PartialEq`).
fn assert_same_job(actual: &QueuedJob, expected: &QueuedJob) {
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(expected).unwrap(),
        "job payload must round-trip through rows unchanged"
    );
}

fn request_record(run_id: RunId, job_id: &str, request_id: i64) -> TaskAgentJobRequestRecord {
    request_record_with_agent(run_id, job_id, request_id, uuid::Uuid::new_v4())
}

/// `plan_id`/`plan_type` are derived from `agent_job_id` (the agreed schema
/// has no such columns), so fixtures build them via the same `plan_fields`
/// the backends use.
fn request_record_with_agent(
    run_id: RunId,
    job_id: &str,
    request_id: i64,
    agent_job_id: uuid::Uuid,
) -> TaskAgentJobRequestRecord {
    let (plan_id, plan_type) = plan_fields(agent_job_id);
    TaskAgentJobRequestRecord {
        request_id,
        run_id,
        job_id: JobId(job_id.to_owned()),
        agent_job_id,
        plan_id,
        plan_type,
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
    let mut record = run_record(run_id);
    record.run_number = NEXT_FIXTURE_RUN_NUMBER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    SubmitRun {
        namespace: "default".to_owned(),
        record,
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

    /// A registered runner without a session (a pre-provisioned successor)
    /// cannot poll, so it is not idle capacity; once a session exists and it
    /// holds no active request, it is.
    pub(crate) async fn sessionless_runner_is_not_idle_capacity(backend: &dyn ControlBackend) {
        let runner = backend
            .register_runner(register_runner("successor"))
            .await
            .unwrap();
        let stale_after = std::time::Duration::from_secs(15);
        let inputs = backend.status_inputs(stale_after).await.unwrap();
        assert_eq!(inputs.registered, 1);
        assert_eq!(
            inputs.runner_idle, 0,
            "a configured successor cannot poll until its slot starts it"
        );

        backend
            .create_session(create_session(runner.runner.id))
            .await
            .unwrap();
        let inputs = backend.status_inputs(stale_after).await.unwrap();
        assert_eq!(inputs.sessions, 1);
        assert_eq!(
            inputs.runner_idle, 1,
            "a session-backed runner with no active request is idle"
        );
        assert_eq!((inputs.runner_busy, inputs.runner_stale), (0, 0));
    }

    /// The pool's busy gauge counts only busy runners the pool proved: a
    /// runner registered outside the pool holding an active request is busy,
    /// but not a pool machine. `PoolSnapshot.busy` previously had no writer
    /// at all, so `preloop status` reported `pool busy: 0` while pool
    /// machines ran jobs.
    pub(crate) async fn pool_busy_counts_only_pool_proven_busy_runners(
        backend: &dyn ControlBackend,
    ) {
        let run_id = RunId::new();
        let mut pool_registration = register_runner("pool-1");
        pool_registration.pool_proven = true;
        let pool_runner = backend.register_runner(pool_registration).await.unwrap();
        let external = backend
            .register_runner(register_runner("external-1"))
            .await
            .unwrap();
        let pool_session = backend
            .create_session(create_session(pool_runner.runner.id))
            .await
            .unwrap();
        let external_session = backend
            .create_session(create_session(external.runner.id))
            .await
            .unwrap();
        backend
            .submit_run(submit_run(
                run_id,
                vec![submit_job(run_id, "one", 1), submit_job(run_id, "two", 2)],
            ))
            .await
            .unwrap();
        for (session_id, runner_id) in [
            (&pool_session.session_id, pool_runner.runner.id),
            (&external_session.session_id, external.runner.id),
        ] {
            let poll = backend
                .poll_session(poll(session_id, runner_id))
                .await
                .unwrap();
            assert!(
                matches!(poll, PollOutcome::Claimed(_)),
                "expected a claim, got {poll:?}"
            );
        }

        let inputs = backend
            .status_inputs(std::time::Duration::from_secs(15))
            .await
            .unwrap();
        assert_eq!(inputs.runner_busy, 2, "both runners hold an active request");
        assert_eq!(
            inputs.pool_busy, 1,
            "only the pool-proven runner counts toward pool busy"
        );
    }

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
        let stats = backend.queue_stats().await.unwrap();
        assert_eq!((stats.ready, stats.claimed), (1, 0));
        assert_eq!(stats.next_runs_on, vec!["self-hosted"]);

        let poll = backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        let PollOutcome::Claimed(claimed) = poll else {
            panic!("expected a claim, got {poll:?}");
        };
        assert_eq!(claimed.queued.job_id, JobId("build".to_owned()));
        assert_eq!(claimed.request.request_id, 1);
        let stats = backend.queue_stats().await.unwrap();
        assert_eq!((stats.ready, stats.claimed), (0, 1));
        assert!(stats.next_runs_on.is_empty());
        let assignments = backend.live_assignments().await.unwrap();
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].runner_id, runner.runner.id);
        assert_eq!(assignments[0].run_id, run_id.to_string());
        assert_eq!(assignments[0].job_id, "build");

        let ctx = backend.acquire_context(1).await.unwrap();
        assert_eq!(ctx.request.request_id, 1);
        assert_eq!(ctx.repository, "owner/repo");

        let done = backend
            .complete_job(crate::control::backend::JobCompletionInput {
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
        let stats = backend.queue_stats().await.unwrap();
        assert_eq!((stats.ready, stats.claimed), (0, 0));
        assert!(backend.live_assignments().await.unwrap().is_empty());
    }

    /// `report_steps` merges a runner's `WorkflowStepsUpdate` into the
    /// attempt's step rows: a reported display name wins, `runner_number`
    /// persists, a non-terminal report stamps `started_at`, a terminal-only
    /// report does not invent a start, and unknown identities are dropped.
    pub(crate) async fn step_reports_merge_into_manifest(backend: &dyn ControlBackend) {
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
            .submit_run(submit_run(
                run_id,
                vec![submit_job_full(run_id, "build", 1)],
            ))
            .await
            .unwrap();
        let poll = backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        let PollOutcome::Claimed(claimed) = poll else {
            panic!("expected a claim, got {poll:?}");
        };
        let agent = claimed.request.agent_job_id;
        let plan = claimed.request.plan_id.clone();

        // In-progress report: renders the name, stamps start, keeps the
        // declared workflow step's runner number when absent.
        assert!(
            backend
                .report_steps(
                    &plan,
                    agent,
                    vec![serde_json::json!({
                        "external_id": "step-1",
                        "number": 2,
                        "name": "Run build",
                        "status": 2,
                        "conclusion": 0
                    })],
                )
                .await
                .unwrap()
        );
        // Terminal report on a second, undeclared step + a name-less update.
        assert!(
            backend
                .report_steps(
                    &plan,
                    agent,
                    vec![
                        serde_json::json!({
                            "external_id": "step-1",
                            "name": "",
                            "status": 6,
                            "conclusion": 2
                        }),
                        serde_json::json!({
                            "external_id": "step-9",
                            "number": 5,
                            "name": "Post job",
                            "status": 6,
                            "conclusion": 2
                        }),
                    ],
                )
                .await
                .unwrap()
        );
        let manifests = backend.run_step_manifests(run_id).await.unwrap();
        let steps = manifests.get(&agent).expect("manifest for the attempt");
        let s1 = steps.iter().find(|s| s.id == "step-1").expect("step-1");
        assert_eq!(s1.name, "Run build");
        assert_eq!(s1.conclusion, "success");
        assert!(s1.started_at.is_some(), "in_progress report stamps start");
        assert!(s1.finished_at.is_some());
        let s9 = steps.iter().find(|s| s.id == "step-9").expect("step-9");
        assert_eq!(s9.runner_number, Some(5));
        assert!(
            s9.started_at.is_none(),
            "a terminal-only sighting must not fake started_at"
        );

        // An unresolvable identity acknowledges and drops the report.
        assert!(
            !backend
                .report_steps(
                    "no-such-plan",
                    uuid::Uuid::new_v4(),
                    vec![serde_json::json!({"external_id": "x", "status": 6})],
                )
                .await
                .unwrap()
        );
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

    /// `plan_id` is the request's `agent_job_id` string form (derived, never
    /// stored) — a plan-id lookup resolves exactly that request. The new
    /// schema gives each attempt its own unique `timeline_id`; timeline
    /// lookups therefore resolve the corresponding attempt directly.
    pub(crate) async fn request_lookup_resolves_attempt_correlations(backend: &dyn ControlBackend) {
        let first = RunId::new();
        let second = RunId::new();
        let first_timeline = uuid::Uuid::new_v4();
        let second_timeline = uuid::Uuid::new_v4();
        let mut initial = submit_job(first, "build", 1);
        initial.request.as_mut().unwrap().timeline_id = first_timeline;
        backend
            .submit_run(submit_run(first, vec![initial]))
            .await
            .unwrap();
        let first_request = backend.request(RequestKey::Id(1)).await.unwrap();
        assert_eq!(first_request.run_id, first);
        let first_plan = first_request.plan_id.clone();
        assert_eq!(
            backend
                .request(RequestKey::PlanId(first_plan.clone()))
                .await
                .unwrap()
                .request_id,
            first_request.request_id
        );
        assert_eq!(
            backend
                .request(crate::control::backend::RequestKey::AgentJobId(
                    first_request.agent_job_id
                ))
                .await
                .unwrap()
                .request_id,
            first_request.request_id
        );
        assert_eq!(
            backend
                .request(RequestKey::TimelineId(first_timeline))
                .await
                .unwrap()
                .run_id,
            first
        );

        let mut later = submit_job(second, "build", 2);
        later.request.as_mut().unwrap().timeline_id = second_timeline;
        backend
            .submit_run(submit_run(second, vec![later]))
            .await
            .unwrap();
        // The first attempt's plan and timeline remain addressable after the
        // later attempt is submitted; the later timeline resolves its own
        // request under the schema's uniqueness constraint.
        let oldest = backend
            .request(RequestKey::PlanId(first_plan))
            .await
            .unwrap();
        assert_eq!(oldest.run_id, first);
        let newest = backend
            .request(RequestKey::TimelineId(second_timeline))
            .await
            .unwrap();
        assert_eq!(newest.run_id, second);
    }

    pub(crate) async fn push_state_only_changes_its_run_column(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        backend
            .set_push_state(
                run_id,
                crate::models::PushState {
                    status: crate::models::PushStatus::Blocked,
                    error: Some("diverged".into()),
                    pr_number: None,
                    effective_sha: None,
                },
            )
            .await
            .unwrap();
        let run = backend.run_record(run_id).await.unwrap();
        let push = run.push_state.unwrap();
        assert_eq!(push.status, crate::models::PushStatus::Blocked);
        assert_eq!(push.error.as_deref(), Some("diverged"));
        assert_eq!(run.jobs[&JobId("build".into())], ExecutionStatus::Queued);
        backend
            .set_push_state(
                run_id,
                crate::models::PushState {
                    status: crate::models::PushStatus::Synced,
                    error: None,
                    pr_number: Some(42),
                    effective_sha: Some("tested-sha".into()),
                },
            )
            .await
            .unwrap();
        let run = backend.run_record(run_id).await.unwrap();
        let push = run.push_state.unwrap();
        assert_eq!(push.status, crate::models::PushStatus::Synced);
        assert_eq!(push.pr_number, Some(42));
        assert_eq!(push.effective_sha.as_deref(), Some("tested-sha"));
        assert_eq!(run.jobs[&JobId("build".into())], ExecutionStatus::Queued);
    }

    /// Artifact scope resolution: a plan id (the request's `agent_job_id`
    /// string form) maps to the run owning that attempt; unknown ids are
    /// omitted so callers keep the raw backend id.
    pub(crate) async fn artifact_scopes_map_plan_ids_to_latest_run(backend: &dyn ControlBackend) {
        let run_a = RunId::new();
        let run_b = RunId::new();
        backend
            .submit_run(submit_run(run_a, vec![submit_job(run_a, "build", 1)]))
            .await
            .unwrap();
        backend
            .submit_run(submit_run(run_b, vec![submit_job(run_b, "build", 2)]))
            .await
            .unwrap();
        let plan_b = backend.request(RequestKey::Id(2)).await.unwrap().plan_id;
        let scopes = backend
            .artifact_scopes(&[plan_b.clone(), "unknown".to_owned()])
            .await
            .unwrap();
        assert_eq!(scopes.get(&plan_b), Some(&run_b));
        assert!(!scopes.contains_key("unknown"));
        assert!(backend.artifact_scopes(&[]).await.unwrap().is_empty());
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
                assert!(
                    message
                        .body
                        .contains(&claimed.request.agent_job_id.to_string())
                );
            }
            other => panic!("expected cancel, got {other:?}"),
        }
    }

    /// Secret values are never written to the control database: a record
    /// that still carries values (a caller bug) is stored without them, so
    /// `run_record` returns an empty secrets map. Values live in the
    /// SecretProvider and are resolved when a runner acquires the job.
    pub(crate) async fn secret_values_never_persist(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let mut record = run_record(run_id);
        Arc::make_mut(&mut record.submission).secrets.insert(
            "TOKEN".to_owned(),
            preloop_gha_protocol::SecretString::new("s3cr3t-value"),
        );
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
        assert!(
            restored.submission.secrets.is_empty(),
            "secret values must never round-trip through the control database"
        );
    }

    /// The webhook inbox: enqueue deduplicates by delivery id, a claim is
    /// fenced by its lease token, the queue stats see the backlog, and
    /// completion is terminal.
    pub(crate) async fn webhook_inbox_claim_is_fenced_and_deduplicated(
        backend: &dyn ControlBackend,
    ) {
        use crate::models::{WebhookDeliveryRecord, WebhookDeliveryStatus};
        let delivery = WebhookDeliveryRecord {
            delivery_id: "d1".to_owned(),
            event: "push".to_owned(),
            payload: br#"{"installation":{"id":9},"ref":"refs/heads/main"}"#.to_vec(),
            received_at_us: 1,
            state: WebhookDeliveryStatus::Received,
            attempts: 0,
            lease_until_us: None,
            lease_token: None,
            last_error: None,
        };
        assert!(backend.enqueue_webhook_delivery(&delivery).await.unwrap());
        assert!(!backend.enqueue_webhook_delivery(&delivery).await.unwrap());
        let stats = backend.webhook_queue_stats().await.unwrap();
        assert_eq!((stats.received, stats.done), (1, 0));

        let claim = backend.claim_webhook_deliveries(1, 60).await.unwrap();
        assert_eq!(claim.len(), 1);
        let token = claim[0].lease_token.clone().unwrap();
        assert!(
            !backend
                .renew_webhook_delivery("d1", "stale", 60)
                .await
                .unwrap()
        );
        assert!(
            backend
                .complete_webhook_delivery("d1", &token)
                .await
                .unwrap()
        );
        assert_eq!(
            backend
                .get_webhook_delivery("d1")
                .await
                .unwrap()
                .unwrap()
                .state,
            WebhookDeliveryStatus::Done
        );
        let stats = backend.webhook_queue_stats().await.unwrap();
        assert_eq!((stats.received, stats.done), (0, 1));
    }

    pub(crate) async fn run_record_round_trips_through_tables(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        // The normalized schema reconstructs the run from the submitted job
        // rows; it does not restore arbitrary fields for jobs that were never
        // submitted. Keep this fixture to the fields actually represented by
        // the SubmitRun contract.
        let mut expected = super::run_record(run_id);
        let mut submission = super::submission();
        submission.vars.insert("REGION".to_owned(), "eu".to_owned());
        expected.submission = Arc::new(submission);
        expected.webhook_delivery_id = Some("delivery-1".to_owned());
        expected.created_at =
            chrono::DateTime::from_timestamp_micros(1_700_000_000_000_001).unwrap();
        expected.snapshot_timing = Some(crate::models::SnapshotTiming {
            duration_ms: 12,
            object_count: 3,
            pack_bytes: 99,
        });
        expected.job_fail_fast.insert("build".to_owned(), true);
        // Fork-PR hold + late check-run reporting must survive persist/reload
        // on both backends (real `runs` columns, not record_details).
        expected.fork_approval_requested_at_unix_nanos = Some(1_700_000_000_000_002_000);
        expected.fork_approved_at_unix_nanos = Some(1_700_000_000_000_003_000);
        expected.fork_approval_note = Some("approved by operator".to_owned());
        expected.fork_approval_pending = false;
        expected.reports_check_runs = true;
        let build = JobId("build".to_owned());
        expected
            .job_base_ids
            .insert(build.clone(), "build".to_owned());
        expected.job_names.insert(build.clone(), "build".to_owned());
        expected.jobs_list.push(crate::models::JobDetail {
            job_id: "build".to_owned(),
            name: "build".to_owned(),
            conclusion: "in_progress".to_owned(),
            steps: Vec::new(),
            annotations: Vec::new(),
        });
        backend
            .submit_run(SubmitRun {
                namespace: "default".to_owned(),
                record: expected.clone(),
                jobs: vec![submit_job(run_id, "build", 1)],
                workflow_concurrency: None,
                empty_concurrency_group: false,
                check_hostable: false,
            })
            .await
            .unwrap();
        let loaded = backend.run_record(run_id).await.unwrap();
        // The normalized database owns creation time (`DEFAULT now()`), so
        // compare the projected record against the committed timestamp.
        expected.created_at = loaded.created_at;
        super::assert_same_run(&loaded, &expected);
    }

    /// The environment protection gate blob round-trips through `jobs` on
    /// both backends: an armed gate (wait deadline, approval request stamp,
    /// recorded approvals) reloads byte-identical, and clearing it reloads
    /// as no gate. This is the fail-closed persistence contract — a lost
    /// gate would silently release a protected job.
    pub(crate) async fn environment_gate_round_trips(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let job_id = JobId("deploy".to_owned());
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "deploy", 1)]))
            .await
            .unwrap();
        let gate = crate::models::EnvironmentGateState {
            wait_until_unix_nanos: Some(1_700_000_000_000_000_000),
            approval_requested_at_unix_nanos: Some(1_700_000_000_000_001_000),
            approvals_unix_nanos: vec![1_700_000_000_000_002_000],
        };
        backend
            .set_environment_gate(run_id, &job_id, Some(gate.clone()))
            .await
            .unwrap();
        let armed = backend
            .environment_gate(run_id, &job_id)
            .await
            .unwrap()
            .expect("job exists");
        assert_eq!(armed.gate, Some(gate));
        backend
            .set_environment_gate(run_id, &job_id, None)
            .await
            .unwrap();
        let cleared = backend
            .environment_gate(run_id, &job_id)
            .await
            .unwrap()
            .expect("job exists");
        assert_eq!(cleared.gate, None);
    }

    /// The fork-PR approval hold parks a run's jobs at submit and the release
    /// admits them: while `fork_approval_pending` is set nothing reaches the
    /// ready queue, and clearing the hold plus a `promote_ready_jobs` pass
    /// puts the run's jobs on it. This is the policy's security contract — a
    /// hold that does not hold would run untrusted fork code without an
    /// operator go-ahead.
    pub(crate) async fn fork_hold_parks_until_released(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let job_id = JobId("build".to_owned());
        let requested_at = crate::models::now_unix_nanos();
        let mut submit = submit_run(run_id, vec![submit_job(run_id, "build", 1)]);
        submit.record.fork_approval_pending = true;
        submit.record.fork_approval_requested_at_unix_nanos = Some(requested_at);
        backend.submit_run(submit).await.unwrap();

        assert_eq!(
            backend.queue_stats().await.unwrap().ready,
            0,
            "a fork-held run must not queue a job"
        );
        assert_eq!(
            backend.job_queue_state(run_id, &job_id).await.unwrap(),
            Some(("blocked".to_owned(), "pending".to_owned())),
            "the job parks at admission"
        );
        let run = backend.run_record(run_id).await.unwrap();
        assert!(run.fork_approval_pending, "the hold is durable");
        assert_eq!(
            run.fork_approval_requested_at_unix_nanos,
            Some(requested_at)
        );

        // Release: the hold clears, and the run's parked jobs are admitted.
        backend
            .set_fork_approval(ForkApprovalStamp {
                run_id,
                pending: false,
                requested_at_unix_nanos: Some(requested_at),
                approved_at_unix_nanos: Some(crate::models::now_unix_nanos()),
                note: Some("lgtm".to_owned()),
            })
            .await
            .unwrap();
        let outcome = backend
            .promote_ready_jobs(Some(run_id), &EnvironmentRulesMap::new())
            .await
            .unwrap();
        assert_eq!(outcome.promoted, 1, "the released hold admits the job");
        assert_eq!(outcome.queue_depth, 1);
        assert_eq!(
            backend.job_queue_state(run_id, &job_id).await.unwrap(),
            Some(("ready".to_owned(), "queued".to_owned()))
        );
        let run = backend.run_record(run_id).await.unwrap();
        assert!(!run.fork_approval_pending);
        assert_eq!(run.fork_approval_note.as_deref(), Some("lgtm"));
    }

    /// An armed environment protection gate keeps its job parked until the
    /// gate admits it: an unexpired wait timer holds, the elapsed timer
    /// releases, and a ref outside `deployment_branches` fails the job closed.
    /// The job never dispatches on a gate that did not pass.
    pub(crate) async fn environment_gate_parks_until_satisfied(backend: &dyn ControlBackend) {
        let rules = |rule: crate::config::EnvironmentRules| {
            let mut rules = EnvironmentRulesMap::new();
            rules
                .entry("owner/repo".to_owned())
                .or_default()
                .insert("prod".to_owned(), rule);
            rules
        };
        let gate = |wait_until_unix_nanos: Option<i64>| crate::models::EnvironmentGateState {
            wait_until_unix_nanos,
            approval_requested_at_unix_nanos: None,
            approvals_unix_nanos: Vec::new(),
        };
        let now = crate::models::now_unix_nanos();
        let run_id = RunId::new();
        let job_id = JobId("deploy".to_owned());
        let mut submit = submit_run(run_id, vec![submit_job(run_id, "deploy", 1)]);
        submit.jobs[0].queued.environment = Some(serde_json::json!("prod"));
        submit.jobs[0].queued.environment_gate = Some(gate(Some(now + 60_000_000_000)));
        backend.submit_run(submit).await.unwrap();
        assert_eq!(backend.queue_stats().await.unwrap().ready, 0);

        let wait_timer = rules(crate::config::EnvironmentRules {
            deployment_branches: Vec::new(),
            wait_timer_minutes: 1,
            required_reviewers: 0,
        });
        // The wait timer has not elapsed: the job stays parked.
        let outcome = backend
            .promote_ready_jobs(Some(run_id), &wait_timer)
            .await
            .unwrap();
        assert_eq!(outcome.promoted, 0, "an unexpired wait timer holds the job");
        assert_eq!(
            backend.job_queue_state(run_id, &job_id).await.unwrap(),
            Some(("blocked".to_owned(), "pending".to_owned()))
        );
        // An elapsed deadline: the same sweep releases the job.
        backend
            .set_environment_gate(run_id, &job_id, Some(gate(Some(now - 1))))
            .await
            .unwrap();
        let outcome = backend
            .promote_ready_jobs(Some(run_id), &wait_timer)
            .await
            .unwrap();
        assert_eq!(
            outcome.promoted, 1,
            "the elapsed wait timer releases the job"
        );
        assert_eq!(
            backend.job_queue_state(run_id, &job_id).await.unwrap(),
            Some(("ready".to_owned(), "queued".to_owned()))
        );

        // A ref outside `deployment_branches` fails the job closed.
        let refused_run = RunId::new();
        let refused_job = JobId("deploy".to_owned());
        let mut submit = submit_run(refused_run, vec![submit_job(refused_run, "deploy", 2)]);
        submit.jobs[0].queued.environment = Some(serde_json::json!("prod"));
        submit.jobs[0].queued.environment_gate =
            Some(crate::models::EnvironmentGateState::default());
        backend.submit_run(submit).await.unwrap();
        let branches = rules(crate::config::EnvironmentRules {
            deployment_branches: vec!["main".to_owned()],
            wait_timer_minutes: 0,
            required_reviewers: 0,
        });
        let outcome = backend
            .promote_ready_jobs(Some(refused_run), &branches)
            .await
            .unwrap();
        assert_eq!(outcome.failed, 1, "a denied ref fails the job closed");
        assert_eq!(
            backend
                .job_queue_state(refused_run, &refused_job)
                .await
                .unwrap(),
            Some(("none".to_owned(), "failure".to_owned())),
            "a denied job never dispatches"
        );
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

    /// The job → check-run mapping: set reports whether it changed, a
    /// re-report of the same id is a no-op, clear is conditional on the id
    /// still recorded, and jobs outside the run are never mapped. Every
    /// webhook delivery goes through `set_job_check_run`.
    pub(crate) async fn check_run_mapping(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let build = JobId("build".to_owned());
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();

        assert!(backend.set_job_check_run(run_id, &build, 11).await.unwrap());
        assert!(!backend.set_job_check_run(run_id, &build, 11).await.unwrap());
        assert!(backend.set_job_check_run(run_id, &build, 12).await.unwrap());
        assert_eq!(
            backend.job_check_run_id(run_id, &build).await.unwrap(),
            Some(12)
        );

        backend
            .clear_job_check_run(run_id, &build, 11)
            .await
            .unwrap();
        assert_eq!(
            backend.job_check_run_id(run_id, &build).await.unwrap(),
            Some(12),
            "a stale clear must not drop the current mapping"
        );
        backend
            .clear_job_check_run(run_id, &build, 12)
            .await
            .unwrap();
        assert_eq!(
            backend.job_check_run_id(run_id, &build).await.unwrap(),
            None
        );

        let ghost = JobId("ghost".to_owned());
        assert!(!backend.set_job_check_run(run_id, &ghost, 13).await.unwrap());
        assert_eq!(
            backend.job_check_run_id(run_id, &ghost).await.unwrap(),
            None
        );
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
// ── Postgres harness (shared server or disposable cluster) ──────────────

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
                // Six concurrent backend pools must fit: writers+readers+2
                // each ≈ 204 connections > initdb's default 100.
                format!("-p {port} -k {} -c max_connections=300", dir.display()),
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

/// Isolation guard for one test's database: a fresh database on the
/// shared server (`PRELOOP_TEST_POSTGRES_URL`), or a disposable local
/// cluster when no server is configured.
enum PgGuard {
    Database(#[allow(dead_code)] crate::test_pg::TestDatabase),
    Cluster(#[allow(dead_code)] DisposablePg),
}

/// A URL to a database nobody else uses, plus its cleanup guard.
async fn fresh_database() -> (PgGuard, String) {
    match crate::test_pg::fresh_database().await {
        Some((db, url)) => (PgGuard::Database(db), url),
        None => {
            let pg = DisposablePg::start();
            let url = pg.url();
            (PgGuard::Cluster(pg), url)
        }
    }
}

mod pg {
    use super::*;
    use super::{PgGuard, fresh_database};
    use crate::control::pg::PgBackend;

    async fn connect(url: &str) -> PgBackend {
        PgBackend::connect(url, false, false, std::time::Duration::from_secs(300))
            .await
            .expect("test database connection failed")
    }

    /// A backend on its own fresh database. Keep the guard alive for the
    /// test's duration.
    async fn backend() -> (PgGuard, PgBackend) {
        let (guard, url) = fresh_database().await;
        let backend = connect(&url).await;
        (guard, backend)
    }

    /// Two independent backends (separate pools) on ONE database: the
    /// shape of two engine nodes sharing a cell, for race tests.
    async fn backend_pair() -> (PgGuard, PgBackend, PgBackend) {
        let (guard, url) = fresh_database().await;
        let first = connect(&url).await;
        let second = connect(&url).await;
        (guard, first, second)
    }

    #[tokio::test]
    async fn submit_poll_complete_lifecycle() {
        let (_pg, backend) = backend().await;
        suite::submit_poll_complete_lifecycle(&backend).await;
    }

    #[tokio::test]
    async fn sessionless_runner_is_not_idle_capacity() {
        let (_pg, backend) = backend().await;
        suite::sessionless_runner_is_not_idle_capacity(&backend).await;
    }

    #[tokio::test]
    async fn pool_busy_counts_only_pool_proven_busy_runners() {
        let (_pg, backend) = backend().await;
        suite::pool_busy_counts_only_pool_proven_busy_runners(&backend).await;
    }

    #[tokio::test]
    async fn step_reports_merge_into_manifest() {
        let (_pg, backend) = backend().await;
        suite::step_reports_merge_into_manifest(&backend).await;
    }

    #[tokio::test]
    async fn webhook_replay_is_idempotent() {
        let (_pg, backend) = backend().await;
        suite::webhook_replay_is_idempotent(&backend).await;
    }

    #[tokio::test]
    async fn request_lookup_resolves_attempt_correlations() {
        let (_pg, backend) = backend().await;
        suite::request_lookup_resolves_attempt_correlations(&backend).await;
    }

    #[tokio::test]
    async fn push_state_only_changes_its_run_column() {
        let (_pg, backend) = backend().await;
        suite::push_state_only_changes_its_run_column(&backend).await;
    }

    #[tokio::test]
    async fn cancel_run_queues_cancellation() {
        let (_pg, backend) = backend().await;
        suite::cancel_run_queues_cancellation(&backend).await;
    }

    #[tokio::test]
    async fn artifact_scopes_map_plan_ids_to_latest_run() {
        let (_pg, backend) = backend().await;
        suite::artifact_scopes_map_plan_ids_to_latest_run(&backend).await;
    }

    #[tokio::test]
    async fn concurrency_gate_serializes_group() {
        let (_pg, backend) = backend().await;
        suite::concurrency_gate_serializes_group(&backend).await;
    }

    #[tokio::test]
    async fn run_record_round_trips_through_tables() {
        let (_pg, backend) = backend().await;
        suite::run_record_round_trips_through_tables(&backend).await;
    }

    #[tokio::test]
    async fn environment_gate_round_trips() {
        let (_pg, backend) = backend().await;
        suite::environment_gate_round_trips(&backend).await;
    }

    #[tokio::test]
    async fn fork_hold_parks_until_released() {
        let (_pg, backend) = backend().await;
        suite::fork_hold_parks_until_released(&backend).await;
    }

    #[tokio::test]
    async fn environment_gate_parks_until_satisfied() {
        let (_pg, backend) = backend().await;
        suite::environment_gate_parks_until_satisfied(&backend).await;
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
    async fn check_run_mapping() {
        let (_pg, backend) = backend().await;
        suite::check_run_mapping(&backend).await;
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

    #[tokio::test]
    async fn secret_values_never_persist() {
        let (_pg, backend) = backend().await;
        suite::secret_values_never_persist(&backend).await;
    }

    #[tokio::test]
    async fn webhook_inbox_claim_is_fenced_and_deduplicated() {
        let (_pg, backend) = backend().await;
        suite::webhook_inbox_claim_is_fenced_and_deduplicated(&backend).await;
    }

    async fn submit_many(node: &PgBackend, count: usize) -> Vec<uuid::Uuid> {
        // Distinct `run_number` per submit: the agreed schema keys
        // `runs_number` on (namespace, repo, path, number, attempt), so
        // fixture submits that all carry number 1 collide.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1_000_000);
        let submits: Vec<_> = (0..count)
            .map(|_| {
                let run_id = RunId::new();
                let mut submit = submit_run(run_id, vec![submit_job(run_id, "build", 0)]);
                submit.record.run_number = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                submit
            })
            .collect();
        let agents = submits
            .iter()
            .map(|s| s.jobs[0].request.as_ref().unwrap().agent_job_id)
            .collect();
        let results =
            futures::future::join_all(submits.into_iter().map(|s| node.submit_run(s))).await;
        for result in results {
            result.unwrap();
        }
        agents
    }

    /// Two backend instances on one database must never mint a duplicate
    /// request id.
    #[tokio::test]
    async fn concurrent_submits_mint_distinct_request_ids() {
        let (_pg, node_a, node_b) = backend_pair().await;
        const PER_NODE: usize = 24;
        let (agents_a, agents_b) = tokio::join!(
            submit_many(&node_a, PER_NODE),
            submit_many(&node_b, PER_NODE)
        );

        let mut request_ids = std::collections::BTreeSet::new();
        for agent in agents_a.into_iter().chain(agents_b) {
            let request = node_a
                .request(RequestKey::AgentJobId(agent))
                .await
                .unwrap_or_else(|e| panic!("attempt {agent} lost its request row: {e:?}"));
            assert!(
                request_ids.insert(request.request_id),
                "request id {} was minted twice",
                request.request_id
            );
        }
        assert_eq!(request_ids.len(), PER_NODE * 2);
    }

    /// Two backend instances on one database must never hand out the
    /// same job twice.
    #[tokio::test]
    async fn concurrent_polls_across_nodes_claim_each_job_once() {
        use crate::control::types::PollOutcome;
        const JOBS: usize = 24;
        const RUNNERS: usize = 12;
        let (_pg, node_a, node_b) = backend_pair().await;
        let nodes = [std::sync::Arc::new(node_a), std::sync::Arc::new(node_b)];
        let mut sessions = Vec::new();
        for i in 0..RUNNERS {
            let node = &nodes[i % 2];
            let runner = node
                .register_runner(register_runner(&format!("r{i}")))
                .await
                .unwrap();
            let session = node
                .create_session(create_session(runner.runner.id))
                .await
                .unwrap();
            sessions.push((i % 2, session.session_id, runner.runner.id));
        }
        let run_id = RunId::new();
        let jobs = (0..JOBS)
            .map(|i| submit_job(run_id, &format!("j{i}"), i as i64 + 1))
            .collect();
        nodes[0].submit_run(submit_run(run_id, jobs)).await.unwrap();

        let mut claimed = std::collections::BTreeSet::new();
        let mut rounds = 0;
        while claimed.len() < JOBS {
            rounds += 1;
            assert!(rounds <= 6, "fan-out drained too slowly: {claimed:?}");
            let polls = sessions.iter().map(|(node, session_id, runner_id)| {
                let node = nodes[*node].clone();
                let poll = poll(session_id, *runner_id);
                tokio::spawn(async move { node.poll_session(poll).await })
            });
            let mut finished = Vec::new();
            for outcome in futures::future::join_all(polls).await {
                if let PollOutcome::Claimed(claim) = outcome.unwrap().unwrap() {
                    assert!(
                        claimed.insert(claim.queued.job_id.clone()),
                        "job {} claimed twice",
                        claim.queued.job_id.0
                    );
                    finished.push((
                        claim.queued.job_id,
                        claim.request.agent_job_id,
                        claim.runner_id,
                    ));
                }
            }
            // Finish this round's jobs so each runner is free to claim again.
            for (job_id, agent_job_id, runner_id) in finished {
                nodes[0]
                    .complete_job(JobCompletionInput {
                        run_id,
                        job_id,
                        agent_job_id: Some(agent_job_id),
                        status: ExecutionStatus::Success,
                        outputs: std::collections::BTreeMap::new(),
                        runner_id: Some(runner_id),
                    })
                    .await
                    .unwrap();
            }
        }
        assert_eq!(claimed.len(), JOBS);
    }
}
// ── New SQLite backend (`control::lite`) ────────────────────────────────
//
// The shared `suite::*` functions run against `LiteBackend` via
// `&dyn ControlBackend`.
mod lite {
    use super::suite;
    use crate::control::backend::ControlBackend;
    use crate::control::lite::LiteBackend;
    use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};

    #[tokio::test]
    async fn submit_poll_complete_lifecycle() {
        suite::submit_poll_complete_lifecycle(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn sessionless_runner_is_not_idle_capacity() {
        suite::sessionless_runner_is_not_idle_capacity(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn pool_busy_counts_only_pool_proven_busy_runners() {
        suite::pool_busy_counts_only_pool_proven_busy_runners(&LiteBackend::in_memory().unwrap())
            .await;
    }

    #[tokio::test]
    async fn step_reports_merge_into_manifest() {
        suite::step_reports_merge_into_manifest(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn webhook_replay_is_idempotent() {
        suite::webhook_replay_is_idempotent(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn request_lookup_resolves_attempt_correlations() {
        suite::request_lookup_resolves_attempt_correlations(&LiteBackend::in_memory().unwrap())
            .await;
    }

    #[tokio::test]
    async fn push_state_only_changes_its_run_column() {
        suite::push_state_only_changes_its_run_column(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn cancel_run_queues_cancellation() {
        suite::cancel_run_queues_cancellation(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn artifact_scopes_map_plan_ids_to_latest_run() {
        suite::artifact_scopes_map_plan_ids_to_latest_run(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn secret_values_never_persist() {
        suite::secret_values_never_persist(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn webhook_inbox_claim_is_fenced_and_deduplicated() {
        suite::webhook_inbox_claim_is_fenced_and_deduplicated(&LiteBackend::in_memory().unwrap())
            .await;
    }

    #[tokio::test]
    async fn run_record_round_trips_through_tables() {
        suite::run_record_round_trips_through_tables(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn environment_gate_round_trips() {
        suite::environment_gate_round_trips(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn fork_hold_parks_until_released() {
        suite::fork_hold_parks_until_released(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn environment_gate_parks_until_satisfied() {
        suite::environment_gate_parks_until_satisfied(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn concurrency_gate_serializes_group() {
        suite::concurrency_gate_serializes_group(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn reconcile_recovers_orphaned_claim() {
        suite::reconcile_recovers_orphaned_claim(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn purge_requeues_claimed_as_queued() {
        suite::purge_requeues_claimed_as_queued(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn purge_requeues_ownerless_claim() {
        suite::purge_requeues_ownerless_claim(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn check_run_mapping() {
        suite::check_run_mapping(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn submit_unhostable_job_persists_failure() {
        suite::submit_unhostable_job_persists_failure(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn submit_skipped_parent_settles_child() {
        suite::submit_skipped_parent_settles_child(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn cancel_in_progress_submit_reports_surviving_depth() {
        suite::cancel_in_progress_submit_reports_surviving_depth(
            &LiteBackend::in_memory().unwrap(),
        )
        .await;
    }

    /// A file-backed LiteBackend proves durability: submit, drop, reopen,
    /// and the job is still claimable — the DB is the authority.
    #[tokio::test]
    async fn state_survives_reopen() {
        let dir = std::env::temp_dir().join(format!("preloop-lite-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("control.db");

        let run_id = RunId::new();
        {
            let backend =
                LiteBackend::open(&path, false, false, std::time::Duration::from_secs(300))
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

        let backend =
            LiteBackend::open(&path, false, false, std::time::Duration::from_secs(300)).unwrap();
        let stats = backend.queue_stats().await.unwrap();
        assert_eq!(stats.ready, 1, "queued job must survive reopen");

        // The trait impl maps a missing run to NotFound; the inherent
        // `Option` return is what `pg`'s lookup uses internally.
        let record = ControlBackend::run_record(&backend, run_id).await.unwrap();
        assert_eq!(record.status, ExecutionStatus::Queued);
        assert_eq!(
            record.jobs.get(&JobId("build".to_owned())),
            Some(&ExecutionStatus::Queued)
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Greenfield schema_meta: a database holding foreign tables with no
    /// `schema_meta` is refused; a stamped wrong version is refused.
    #[test]
    fn opening_a_foreign_database_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.db");
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch("CREATE TABLE foreign_table (id INTEGER)")
            .unwrap();
        let error = LiteBackend::open(&path, false, false, std::time::Duration::from_secs(300))
            .err()
            .expect("a foreign database must be refused");
        assert!(error.to_string().contains("predates"), "{error}");

        // A fresh file opens and is stamped with the agreed version.
        let fresh = dir.path().join("fresh.db");
        LiteBackend::open(&fresh, false, false, std::time::Duration::from_secs(300)).unwrap();
        let version: Vec<u8> = rusqlite::Connection::open(&fresh)
            .unwrap()
            .query_row(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            String::from_utf8(version).unwrap(),
            crate::control::lite::SCHEMA_VERSION
        );
    }
}
