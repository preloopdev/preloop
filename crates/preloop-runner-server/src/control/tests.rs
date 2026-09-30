//! Behavioral suite for the `ControlBackend` contract. The same scenarios
//! run against every backend (SQLite and Postgres) so a backend can never
//! diverge from the shared scheduling semantics.
//!
//! Every assertion reads state back through a fresh `transact` (a full DB
//! reload), so a bug in the persisted write path fails here even when an
//! in-memory read would have looked correct.
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
use std::future::Future;
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

/// A minimal Azure-protocol timeline record, addressed by `id` so ordering
/// by record id is predictable.
fn timeline_record(id: u128, name: &str) -> preloop_gha_protocol::azdo::TimelineRecord {
    serde_json::from_value(serde_json::json!({
        "id": uuid::Uuid::from_u128(id),
        "name": name,
        "type": "Task",
        "state": "completed",
        "result": "succeeded",
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

/// One materialized matrix leg for an `apply_expansion` call: a `BuiltJob`
/// whose plan carries the leg's id/base and whose artifacts are the minimal
/// message/request rows `register_built_jobs` persists. The request id is
/// allocated by the backend, so the fixture's value is irrelevant.
fn built_matrix_leg(
    run_id: RunId,
    job_id: &str,
    base_id: &str,
    index: usize,
    total: usize,
) -> crate::control::logic::BuiltJob {
    let plan: preloop_gha_protocol::JobPlan = serde_json::from_value(serde_json::json!({
        "id": job_id,
        "base_id": base_id,
        "name": job_id,
        "runs_on": ["self-hosted"],
        "matrix_index": index,
        "matrix_total": total,
        "fail_fast": true,
        "continue_on_error": false,
    }))
    .expect("matrix leg plan decodes");
    crate::control::logic::BuiltJob {
        plan,
        condition_context: preloop_gha_expressions::Context::default(),
        artifacts: crate::runs::BuiltJobArtifacts {
            agent_msg: job_message(job_id, index as i64),
            job_request: request_record(run_id, job_id, index as i64),
            id_token_granted: false,
            oidc_ctx: crate::state::OidcJobContext {
                environment: None,
                job_workflow_ref: None,
                job_workflow_sha: None,
            },
            github_token_request: None,
        },
    }
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

/// `register_runner` with explicit labels (label-matching scenarios).
fn register_runner_with_labels(name: &str, labels: &[&str]) -> RegisterRunner {
    RegisterRunner {
        labels: labels.iter().map(|label| (*label).to_owned()).collect(),
        ..register_runner(name)
    }
}

/// A `submit_job` whose job carries explicit `runs-on` labels.
fn submit_job_on(run_id: RunId, job_id: &str, request_id: i64, runs_on: &[&str]) -> SubmitJob {
    let mut submit = submit_job(run_id, job_id, request_id);
    submit.queued.runs_on = runs_on.iter().map(|label| (*label).to_owned()).collect();
    submit
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

/// A `poll` reporting explicit runner labels.
fn poll_with_labels(session_id: &str, runner_id: i64, labels: &[&str]) -> PollRequest {
    let mut poll = poll(session_id, runner_id);
    poll.runner = RunnerCapabilities {
        known: true,
        labels: labels.iter().map(|label| (*label).to_owned()).collect(),
        runner_group_id: None,
        runner_group_name: None,
    };
    poll
}

/// `(runner, run, job)` keys of a pairing list (`RunnerAssignment` has no
/// equality, and the age is timing-dependent).
fn pairing_keys(
    assignments: &[preloop_observability::status::RunnerAssignment],
) -> Vec<(i64, String, String)> {
    assignments
        .iter()
        .map(|assignment| {
            (
                assignment.runner_id,
                assignment.run_id.clone(),
                assignment.job_id.clone(),
            )
        })
        .collect()
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

/// One job-level `concurrency:` gate: queue `single`, so exactly one holder.
fn job_concurrency(group: &str, cancel_in_progress: bool) -> preloop_gha_parser::Concurrency {
    preloop_gha_parser::Concurrency {
        group: group.to_owned(),
        cancel_in_progress: Some(cancel_in_progress.to_string()),
        queue: preloop_gha_parser::ConcurrencyQueue::Single,
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

/// Claim and pairing must agree on the label shapes the refactor split:
/// only the `ubuntu`/`macos`/`windows` hosted prefixes stand in for a
/// runner's OS label, exactly as the pre-refactor `take_matching_job` rule.
#[test]
fn label_matcher_keeps_the_merge_base_semantics() {
    fn labels(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }
    let runner = |values: &[&str]| crate::control::logic::RunnerMatchRow {
        labels: labels(values),
        known: true,
        group_id: None,
        group_name: None,
    };
    for (job, runner_labels, expected) in [
        (&["ubuntu"][..], &["linux"][..], true),
        (&["linux-6.1"][..], &["linux"][..], false),
        (&["osx-14"][..], &["macos"][..], false),
        (&["macos-15"][..], &["macos"][..], true),
        (&["ubuntu-24.04"][..], &["macos"][..], false),
    ] {
        assert_eq!(
            crate::runtime_scheduling::job_matches_runner(&labels(job), &labels(runner_labels)),
            expected,
            "pairing: {job:?} on {runner_labels:?}"
        );
        assert_eq!(
            crate::control::logic::runner_matches(&labels(job), None, &runner(runner_labels)),
            expected,
            "claim must agree with pairing: {job:?} on {runner_labels:?}"
        );
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

    /// A fail-fast matrix leg's failure cancels its pending siblings; the
    /// dependent that declared `needs: <matrix base>` must then compute as
    /// settled (skipped), not hang on a stale `remaining_needs` counter, and
    /// the run must finalize.
    pub(crate) async fn matrix_fail_fast_settles_dependent_and_run(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        // `fan` is a needs-driven matrix; `after` depends on the matrix base.
        let mut fan = submit_job(run_id, "fan", 2);
        fan.queued.needs = vec![JobId("gen".to_owned())];
        fan.queued.deferred_matrix = Some("${{ fromJSON(needs.gen.outputs.m) }}".to_owned());
        let mut after = submit_job(run_id, "after", 3);
        after.queued.needs = vec![JobId("fan".to_owned())];
        backend
            .submit_run(submit_run(
                run_id,
                vec![submit_job(run_id, "gen", 1), fan, after],
            ))
            .await
            .unwrap();

        // Completing `gen` queues the deferred node for expansion.
        backend
            .complete_job(JobCompletionInput {
                run_id,
                job_id: JobId("gen".to_owned()),
                agent_job_id: None,
                status: ExecutionStatus::Success,
                outputs: BTreeMap::from([("m".to_owned(), serde_json::json!({"x": [1, 2]}))]),
                runner_id: None,
            })
            .await
            .unwrap();
        let claim = backend
            .claim_expansion()
            .await
            .unwrap()
            .expect("the deferred node must be leased");
        assert_eq!(claim.job.job_id, JobId("fan".to_owned()));
        backend
            .apply_expansion(ExpansionApply {
                job: claim.job,
                generation: claim.generation,
                built: Ok(crate::control::logic::BuiltExpansion::Matrix {
                    jobs: vec![
                        built_matrix_leg(run_id, "fan-1", "fan", 1, 2),
                        built_matrix_leg(run_id, "fan-2", "fan", 2, 2),
                    ],
                }),
            })
            .await
            .unwrap();

        // One leg fails while its sibling is still pending: fail-fast cancels
        // the sibling, and the base's dependent must settle.
        backend
            .complete_job(JobCompletionInput {
                run_id,
                job_id: JobId("fan-1".to_owned()),
                agent_job_id: None,
                status: ExecutionStatus::Failure,
                outputs: BTreeMap::new(),
                runner_id: None,
            })
            .await
            .unwrap();

        let record = backend.run_record(run_id).await.unwrap();
        assert_eq!(
            record.jobs.get(&JobId("fan-1".to_owned())),
            Some(&ExecutionStatus::Failure)
        );
        assert_eq!(
            record.jobs.get(&JobId("fan-2".to_owned())),
            Some(&ExecutionStatus::Cancelled),
            "fail-fast must cancel the pending sibling"
        );
        assert_eq!(
            record.jobs.get(&JobId("after".to_owned())),
            Some(&ExecutionStatus::Skipped),
            "the dependent of a fail-fast-cancelled matrix must settle"
        );
        assert!(
            record.status.is_terminal(),
            "the run must finalize once the dependent settles, got {:?}",
            record.status
        );
    }

    /// A cancellation that lands between `claim_expansion` and
    /// `apply_expansion` invalidates the lease: the build's subtree must be
    /// discarded and the run must stay cancelled.
    pub(crate) async fn cancelled_run_discards_in_flight_expansion(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let mut fan = submit_job(run_id, "fan", 2);
        fan.queued.needs = vec![JobId("gen".to_owned())];
        fan.queued.deferred_matrix = Some("${{ fromJSON(needs.gen.outputs.m) }}".to_owned());
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "gen", 1), fan]))
            .await
            .unwrap();

        // Completing `gen` promotes the deferred node into the expansion queue.
        backend
            .complete_job(JobCompletionInput {
                run_id,
                job_id: JobId("gen".to_owned()),
                agent_job_id: None,
                status: ExecutionStatus::Success,
                outputs: BTreeMap::from([("m".to_owned(), serde_json::json!({"x": [1, 2]}))]),
                runner_id: None,
            })
            .await
            .unwrap();

        let claim = backend
            .claim_expansion()
            .await
            .unwrap()
            .expect("the deferred node must be leased");
        assert_eq!(claim.job.job_id, JobId("fan".to_owned()));
        assert_eq!(claim.generation, 1);

        backend.cancel_run(run_id, None).await.unwrap();
        assert_eq!(
            backend.run_record(run_id).await.unwrap().status,
            ExecutionStatus::Cancelled
        );

        backend
            .apply_expansion(ExpansionApply {
                job: claim.job,
                generation: claim.generation,
                built: Ok(crate::control::logic::BuiltExpansion::Matrix {
                    jobs: vec![
                        built_matrix_leg(run_id, "fan-1", "fan", 1, 2),
                        built_matrix_leg(run_id, "fan-2", "fan", 2, 2),
                    ],
                }),
            })
            .await
            .unwrap();

        let record = backend.run_record(run_id).await.unwrap();
        assert_eq!(
            record.conclusion.as_deref(),
            Some("cancelled"),
            "a build that returns after cancellation must not resurrect the run"
        );
        assert_eq!(record.status, ExecutionStatus::Cancelled);
        assert!(
            !record.jobs.contains_key(&JobId("fan-1".to_owned()))
                && !record.jobs.contains_key(&JobId("fan-2".to_owned())),
            "a stale build must not register legs into a cancelled run"
        );
    }

    /// Cancelling a run that already completed is a no-op: the terminal
    /// conclusion is preserved (pg guards the transition; lite must too).
    pub(crate) async fn cancel_of_terminal_run_preserves_conclusion(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        backend
            .complete_job(JobCompletionInput {
                run_id,
                job_id: JobId("build".to_owned()),
                agent_job_id: None,
                status: ExecutionStatus::Success,
                outputs: BTreeMap::new(),
                runner_id: None,
            })
            .await
            .unwrap();
        assert_eq!(
            backend
                .run_record(run_id)
                .await
                .unwrap()
                .conclusion
                .as_deref(),
            Some("success")
        );

        backend.cancel_run(run_id, None).await.unwrap();
        let after = backend.run_record(run_id).await.unwrap();
        assert_eq!(
            after.conclusion.as_deref(),
            Some("success"),
            "cancelling a completed run must not rewrite its conclusion"
        );
        assert_eq!(after.status, ExecutionStatus::Success);
        // A repeat cancel is still a no-op.
        backend.cancel_run(run_id, None).await.unwrap();
        assert_eq!(
            backend
                .run_record(run_id)
                .await
                .unwrap()
                .conclusion
                .as_deref(),
            Some("success")
        );
    }

    /// A starved run is finalized without releasing its workflow-level
    /// concurrency hold (the trait doc: `reap_sweep` step 1 performs no
    /// concurrency release). A run parked behind the group stays held.
    pub(crate) async fn starved_run_keeps_workflow_hold(backend: &dyn ControlBackend) {
        let run_a = RunId::new();
        let mut submit_a = submit_run(run_a, vec![submit_job(run_a, "deploy", 1)]);
        submit_a.workflow_concurrency = Some(workflow_concurrency("g", false));
        backend.submit_run(submit_a).await.unwrap();

        let run_b = RunId::new();
        let mut submit_b = submit_run(run_b, vec![submit_job(run_b, "deploy", 2)]);
        submit_b.workflow_concurrency = Some(workflow_concurrency("g", false));
        let held = backend.submit_run(submit_b).await.unwrap();
        assert!(held.held, "the second run in the group must be held");

        // No runner matches `self-hosted`, so A's only ready job starves once
        // the grace window passes.
        let ready: Vec<ReadyRow> = backend
            .reap_inputs()
            .await
            .unwrap()
            .ready
            .into_iter()
            .filter(|row| row.run_id == run_a)
            .collect();
        assert_eq!(ready.len(), 1, "the holder's job must be ready");
        backend
            .reap_sweep(ReapSweep {
                now: std::time::SystemTime::now() + std::time::Duration::from_secs(600),
                runs: std::collections::BTreeSet::from([run_a]),
                ready,
                active: Vec::new(),
                paused: BTreeMap::new(),
                pool_preparing: false,
                warm_window_open: false,
                first_seen: BTreeMap::new(),
            })
            .await
            .unwrap();

        let a = backend.run_record(run_a).await.unwrap();
        assert_eq!(
            a.jobs.get(&JobId("deploy".to_owned())),
            Some(&ExecutionStatus::Failure),
            "the starved job must be failed"
        );
        assert!(a.status.is_terminal(), "starvation must finalize the run");

        // The held run's job must not enter the ready queue: releasing A's
        // hold would promote B.
        let stats = backend.queue_stats().await.unwrap();
        assert_eq!(
            stats.ready, 0,
            "starvation must not release the run's workflow concurrency hold"
        );
        let b = backend.run_record(run_b).await.unwrap();
        assert!(!b.status.is_terminal(), "the held run must stay live");
    }

    /// A parked cancellation is redelivered with its stored protocol body
    /// (`{"jobId":..}`), never as the bare request id: `request_id` addresses
    /// only the job assignment. A runner that polls again before it reports
    /// the cancelled job must not be handed a body it can only ignore.
    pub(crate) async fn redelivered_cancellation_keeps_its_body(backend: &dyn ControlBackend) {
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
        backend.cancel_run(run_id, None).await.unwrap();

        // The protocol body: `{"jobId": .., "timeout": ..}`. Compare parsed
        // JSON — PostgreSQL stores the body as `jsonb` and normalizes its
        // text on the way out.
        let expected: serde_json::Value = serde_json::from_str(
            &crate::concurrency::job_cancel_body(claimed.request.agent_job_id),
        )
        .unwrap();
        let body_of = |message: &preloop_gha_protocol::azdo::TaskAgentMessage| {
            serde_json::from_str::<serde_json::Value>(&message.body)
                .unwrap_or_else(|error| panic!("body is not the protocol JSON: {error}"))
        };
        let first = match backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap()
        {
            PollOutcome::Cancel(message) => message,
            other => panic!("expected the cancellation, got {other:?}"),
        };
        assert_eq!(body_of(&first), expected);

        // The runner has not acknowledged the message yet: the same
        // cancellation comes back, still carrying its body.
        let again = match backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap()
        {
            PollOutcome::Inflight(message) => message,
            other => panic!("expected a redelivery of the parked message, got {other:?}"),
        };
        assert_eq!(again.message_id, first.message_id);
        assert_eq!(again.message_type, "JobCancellation");
        assert_eq!(
            body_of(&again),
            expected,
            "a redelivered cancellation carries its stored body, not the request id"
        );
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

    /// A stored template keeps the builder's baseline mask hints.
    ///
    /// The builder emits its baseline regexes first and one hint per
    /// non-empty secret value last; only that tail encodes a value, so
    /// storage must drop the tail and keep the baseline — the acquire fill
    /// re-adds value-derived hints only. A template stored with none delivers
    /// a job whose runner cannot redact credential shapes the workflow never
    /// declared.
    pub(crate) async fn baseline_mask_hints_survive_template_storage(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let plan: preloop_gha_protocol::JobPlan = serde_json::from_value(serde_json::json!({
            "id": "build",
            "base_id": "build",
            "name": "build",
            "runs_on": ["self-hosted"],
        }))
        .unwrap();
        let message = preloop_gha_parser::job_builder::build_agent_job_message(
            &plan,
            &serde_json::json!({"repository": "owner/repo", "sha": "abc123"}),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .unwrap();
        let baseline = message.mask_hints.len();
        assert!(
            baseline >= 18,
            "the builder must emit its baseline hint set with no secret values, got {baseline}"
        );
        let mut job = submit_job(run_id, "build", 1);
        job.queued.message = message;
        backend
            .submit_run(submit_run(run_id, vec![job]))
            .await
            .unwrap();

        let ctx = backend.acquire_context(1).await.unwrap();
        assert_eq!(
            ctx.message.mask_hints.len(),
            baseline,
            "the baseline mask hints must survive template storage"
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

    /// `list_runs` filters on the projected API status word, never the raw
    /// storage word (`runs.status`): a terminated run matches its conclusion
    /// and a workflow-gated run matches `pending`, never `queued`.
    pub(crate) async fn list_runs_filters_on_the_projected_status(backend: &dyn ControlBackend) {
        let cancelled = RunId::new();
        backend
            .submit_run(submit_run(
                cancelled,
                vec![submit_job(cancelled, "build", 1)],
            ))
            .await
            .unwrap();
        backend.cancel_run(cancelled, None).await.unwrap();
        assert_eq!(
            backend.run_record(cancelled).await.unwrap().status,
            ExecutionStatus::Cancelled,
            "a cancelled run's record projects `cancelled`"
        );

        let gate = RunId::new();
        let mut gate_submit = submit_run(gate, vec![submit_job(gate, "deploy", 2)]);
        gate_submit.workflow_concurrency = Some(workflow_concurrency("deploy", false));
        assert!(
            !backend.submit_run(gate_submit).await.unwrap().held,
            "the first run of a group acquires it"
        );

        let held = RunId::new();
        let mut submit = submit_run(held, vec![submit_job(held, "deploy", 3)]);
        submit.workflow_concurrency = Some(workflow_concurrency("deploy", false));
        let outcome = backend.submit_run(submit).await.unwrap();
        assert!(outcome.held, "a second run in the group waits on the gate");

        let by_status = |status: &str| {
            let status = status.to_owned();
            async move {
                backend
                    .list_runs(RunListFilter {
                        status: Some(status),
                        limit: 50,
                        ..Default::default()
                    })
                    .await
                    .unwrap()
            }
        };

        let cancelled_list = by_status("cancelled").await;
        assert!(
            cancelled_list.iter().any(|run| run.run_id == cancelled),
            "`?status=cancelled` must return the run whose record reads `cancelled`, got {:?}",
            cancelled_list
                .iter()
                .map(|run| (run.run_id, run.status))
                .collect::<Vec<_>>()
        );

        let pending_list = by_status("pending").await;
        assert!(
            pending_list.iter().any(|run| run.run_id == held),
            "`?status=pending` must return the gate-held run"
        );

        let queued_list = by_status("queued").await;
        assert!(
            queued_list.iter().any(|run| run.run_id == gate),
            "`?status=queued` must return the runnable run holding the gate"
        );
        assert!(
            !queued_list.iter().any(|run| run.run_id == held),
            "`?status=queued` must not return the gate-held run (its record reads `pending`)"
        );
    }

    /// A timeline PATCH bounds storage at `MAX_TIMELINE_RECORDS` rows per
    /// timeline and always serves the records it just wrote: filling the
    /// timeline to the cap then patching one record whose id sorts after
    /// every stored one evicts the lowest stored record and returns the new
    /// one.
    pub(crate) async fn timeline_patch_is_bounded_and_keeps_the_patch(
        backend: &dyn ControlBackend,
    ) {
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
        let PollOutcome::Claimed(claimed) = backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap()
        else {
            panic!("expected a claim");
        };
        let key = format!(
            "{}/{}",
            claimed.request.plan_id, claimed.request.timeline_id
        );

        // Exactly the cap: nothing is evicted yet.
        let fill: Vec<_> = (1..=MAX_TIMELINE_RECORDS as u128)
            .map(|id| timeline_record(id, "fill"))
            .collect();
        let (_, stored) = backend.patch_timeline(&key, fill).await.unwrap();
        assert_eq!(stored.len(), MAX_TIMELINE_RECORDS);

        // One more record, sorting after every stored one. The write-side
        // bound evicts the lowest stored record, never the fresh patch.
        let late = u128::MAX;
        let (_, stored) = backend
            .patch_timeline(&key, vec![timeline_record(late, "late")])
            .await
            .unwrap();
        let ids: Vec<String> = stored.iter().map(|record| record.id.to_string()).collect();
        assert!(
            stored
                .iter()
                .any(|record| record.name.as_deref() == Some("late")),
            "the PATCH response must contain the record it just wrote, got {ids:?}"
        );
        assert!(
            !ids.contains(&uuid::Uuid::from_u128(1).to_string()),
            "the lowest stored record is evicted past the cap, got {ids:?}"
        );
        assert_eq!(
            stored.len(),
            MAX_TIMELINE_RECORDS,
            "storage stays bounded at the cap"
        );

        let (_, fetched) = backend.get_timeline(&key, 0, usize::MAX).await.unwrap();
        assert!(
            fetched
                .iter()
                .any(|record| record.id == uuid::Uuid::from_u128(late)),
            "a later GET returns the stored patch"
        );
    }

    /// A run's workflow-level concurrency hold is released by the completion
    /// that makes the run terminal (not only by a cancellation): the group
    /// stops naming the finished run, a later arrival acquires it, and the
    /// waiter parked behind the finished run is promoted.
    pub(crate) async fn workflow_concurrency_releases_when_the_run_finishes(
        backend: &dyn ControlBackend,
    ) {
        let runner = backend
            .register_runner(register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(create_session(runner.runner.id))
            .await
            .unwrap();
        let session_id = session.session_id.clone();
        let runner_id = runner.runner.id;

        // A run finishing with no waiter must free its group outright.
        let run_a = RunId::new();
        let mut submit_a = submit_run(run_a, vec![submit_job(run_a, "deploy", 1)]);
        submit_a.workflow_concurrency = Some(workflow_concurrency("release", false));
        assert!(!backend.submit_run(submit_a).await.unwrap().held);
        let claimed = match backend
            .poll_session(poll(&session_id, runner_id))
            .await
            .unwrap()
        {
            PollOutcome::Claimed(claimed) => claimed,
            other => panic!("expected a claim for run A, got {other:?}"),
        };
        assert_eq!(claimed.queued.run_id, run_a);
        backend
            .complete_job(JobCompletionInput {
                run_id: run_a,
                job_id: JobId("deploy".to_owned()),
                agent_job_id: Some(claimed.request.agent_job_id),
                status: ExecutionStatus::Success,
                outputs: BTreeMap::new(),
                runner_id: Some(runner_id),
            })
            .await
            .unwrap();
        let record_a = backend.run_record(run_a).await.unwrap();
        assert!(
            record_a.status.is_terminal(),
            "run A must be terminal, got {:?}",
            record_a.status
        );
        let inputs = backend
            .status_inputs(std::time::Duration::from_secs(15))
            .await
            .unwrap();
        assert_eq!(
            inputs.concurrency_groups_active, 0,
            "a finished run must not keep its workflow hold"
        );

        // The group is usable again: a fresh arrival takes it.
        let run_b = RunId::new();
        let mut submit_b = submit_run(run_b, vec![submit_job(run_b, "deploy", 2)]);
        submit_b.workflow_concurrency = Some(workflow_concurrency("release", false));
        assert!(
            !backend.submit_run(submit_b).await.unwrap().held,
            "a later run must acquire the released group"
        );

        // A waiter parked behind the finished run is promoted in the same
        // command that releases the hold.
        let run_c = RunId::new();
        let mut submit_c = submit_run(run_c, vec![submit_job(run_c, "deploy", 3)]);
        submit_c.workflow_concurrency = Some(workflow_concurrency("release", false));
        assert!(backend.submit_run(submit_c).await.unwrap().held);
        let claimed = match backend
            .poll_session(poll(&session_id, runner_id))
            .await
            .unwrap()
        {
            PollOutcome::Claimed(claimed) => claimed,
            other => panic!("expected a claim for run B, got {other:?}"),
        };
        assert_eq!(claimed.queued.run_id, run_b);
        backend
            .complete_job(JobCompletionInput {
                run_id: run_b,
                job_id: JobId("deploy".to_owned()),
                agent_job_id: Some(claimed.request.agent_job_id),
                status: ExecutionStatus::Success,
                outputs: BTreeMap::new(),
                runner_id: Some(runner_id),
            })
            .await
            .unwrap();
        let state_c = backend
            .job_queue_state(run_c, &JobId("deploy".to_owned()))
            .await
            .unwrap();
        assert_eq!(
            state_c
                .as_ref()
                .map(|(queue_state, _)| queue_state.as_str()),
            Some("ready"),
            "the parked waiter must be promoted, got {state_c:?}"
        );
    }

    /// A workflow-gate release must re-evaluate each held job through the
    /// normal promotion path (needs, job-level gates, max-parallel) instead of
    /// enqueueing every parked job: a `deploy` job whose `needs: [build]` is
    /// still pending must stay blocked until `build` settles.
    pub(crate) async fn workflow_gate_release_respects_needs(backend: &dyn ControlBackend) {
        let run_a = RunId::new();
        let mut submit_a = submit_run(run_a, vec![submit_job(run_a, "hold", 1)]);
        submit_a.workflow_concurrency = Some(workflow_concurrency("g", false));
        assert!(!backend.submit_run(submit_a).await.unwrap().held);

        let run_b = RunId::new();
        let build = submit_job(run_b, "build", 2);
        // Admission order is job-id sorted, so `deploy` is evaluated after
        // `build` in the release sweep.
        let mut deploy = submit_job(run_b, "deploy", 3);
        deploy.queued.needs = vec![JobId("build".to_owned())];
        let mut submit_b = submit_run(run_b, vec![build, deploy]);
        submit_b.workflow_concurrency = Some(workflow_concurrency("g", false));
        assert!(backend.submit_run(submit_b).await.unwrap().held);

        // Freeing the gate (cancelling the holder) promotes run B.
        backend.cancel_run(run_a, None).await.unwrap();

        let build_state = backend
            .job_queue_state(run_b, &JobId("build".to_owned()))
            .await
            .unwrap();
        assert_eq!(
            build_state
                .as_ref()
                .map(|(queue_state, _)| queue_state.as_str()),
            Some("ready"),
            "the gate-free job must be promoted, got {build_state:?}"
        );
        let deploy_state = backend
            .job_queue_state(run_b, &JobId("deploy".to_owned()))
            .await
            .unwrap();
        assert_ne!(
            deploy_state
                .as_ref()
                .map(|(queue_state, _)| queue_state.as_str()),
            Some("ready"),
            "a needs-blocked job must not be enqueued by the gate release, got {deploy_state:?}"
        );
    }

    /// A reusable-caller JobSet gate is keyed by the run's namespace, like
    /// every other concurrency key: two tenants that happen to share a group
    /// name must not serialize against each other.
    pub(crate) async fn jobset_gate_is_scoped_to_run_namespace(backend: &dyn ControlBackend) {
        let caller = |run_id: RunId, request_id: i64| {
            let call = preloop_gha_protocol::ReusableCallPlan {
                uses: "owner/repo/.github/workflows/inner.yml@refs/heads/main".to_owned(),
                workflow_file: "inner.yml".to_owned(),
                workflow_sha: None,
                workflow_repository: None,
                depth: 1,
            };
            // The caller placeholder node: `record.caller_plans` supplies its
            // deferred plan and `record.reusable_calls` its gate metadata.
            let plan: preloop_gha_protocol::JobPlan = serde_json::from_value(serde_json::json!({
                "id": "caller",
                "base_id": "caller",
                "name": "caller",
                "runs_on": ["self-hosted"],
                "reusable_call": call,
            }))
            .unwrap();
            let meta = preloop_gha_parser::ReusableCallMetadata {
                caller_job_id: "caller".to_owned(),
                output_definitions: BTreeMap::new(),
                inner_job_ids: Vec::new(),
                inputs: BTreeMap::new(),
                caller_concurrency: Some(job_concurrency("tenant-gate", false)),
                embedded_concurrency: None,
                matrix: BTreeMap::new(),
                if_condition: None,
                workflow_sha: None,
                workflow_repository: None,
            };
            let mut job = submit_job(run_id, "caller", request_id);
            job.queued.reusable_call = Some(call);
            (job, plan, meta)
        };

        let run_a = RunId::new();
        let (job_a, plan_a, meta_a) = caller(run_a, 1);
        let mut submit_a = submit_run(run_a, vec![job_a]);
        submit_a.namespace = "tenant-a".to_owned();
        submit_a
            .record
            .caller_plans
            .insert(JobId("caller".to_owned()), plan_a);
        submit_a
            .record
            .reusable_calls
            .insert("caller".to_owned(), meta_a);
        backend.submit_run(submit_a).await.unwrap();

        let run_b = RunId::new();
        let (job_b, plan_b, meta_b) = caller(run_b, 2);
        let mut submit_b = submit_run(run_b, vec![job_b]);
        submit_b.namespace = "tenant-b".to_owned();
        submit_b
            .record
            .caller_plans
            .insert(JobId("caller".to_owned()), plan_b);
        submit_b
            .record
            .reusable_calls
            .insert("caller".to_owned(), meta_b);
        backend.submit_run(submit_b).await.unwrap();

        let state_b = backend
            .job_queue_state(run_b, &JobId("caller".to_owned()))
            .await
            .unwrap();
        assert_ne!(
            state_b
                .as_ref()
                .map(|(queue_state, _)| queue_state.as_str()),
            Some("held"),
            "a jobset gate in another namespace must not hold this run back, got {state_b:?}"
        );
    }

    /// Fail-fast must release the concurrency slot of the sibling it
    /// cancels. `apply_fail_fast` is the only path that cancels a sibling
    /// without completing it, so a leaked hold (or a leaked wait row) parks
    /// every later run in that group forever.
    pub(crate) async fn fail_fast_releases_the_cancelled_sibling_concurrency_slot(
        backend: &dyn ControlBackend,
    ) {
        // One fail-fast base ("build") with two gated legs on ONE group: the
        // first admitted leg takes the group's hold, the second parks behind
        // it, and failing the second cancels the holder. The ungated tail job
        // is unmatched by the test runner and never claims, so the run stays
        // non-terminal after fail-fast — a terminal run releases all of its
        // holds anyway, which would mask a leak.
        let run_id = RunId::new();
        let mut holder = submit_job(run_id, "leg-holder", 1);
        holder.queued.base_id = "build".to_owned();
        holder.queued.concurrency = Some(job_concurrency("group-a", false));
        let mut failing = submit_job(run_id, "leg-failing", 2);
        failing.queued.base_id = "build".to_owned();
        failing.queued.concurrency = Some(job_concurrency("group-a", false));
        let mut tail = submit_job(run_id, "tail", 3);
        tail.queued.runs_on = vec!["self-hosted".to_owned(), "never-provisioned".to_owned()];
        let mut submit = submit_run(run_id, vec![holder, failing, tail]);
        submit.record.job_fail_fast.insert("build".to_owned(), true);
        backend.submit_run(submit).await.unwrap();

        // leg-failing fails: fail-fast cancels its sibling leg-holder, which
        // owns the group slot.
        backend
            .complete_job(JobCompletionInput {
                run_id,
                job_id: JobId("leg-failing".to_owned()),
                agent_job_id: None,
                status: ExecutionStatus::Failure,
                outputs: BTreeMap::new(),
                runner_id: None,
            })
            .await
            .unwrap();
        let record = backend.run_record(run_id).await.unwrap();
        assert_eq!(
            record.jobs.get(&JobId("leg-holder".to_owned())),
            Some(&ExecutionStatus::Cancelled),
            "fail-fast must cancel the sibling that holds the group"
        );
        assert!(
            !record.status.is_terminal(),
            "the tail job keeps the run alive so the run-level release cannot mask the leak"
        );

        // A later run in the same group must be able to claim its job; with
        // the sibling's slot leaked, that job parks behind the dead holder.
        let runner = backend
            .register_runner(register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(create_session(runner.runner.id))
            .await
            .unwrap();
        let next_run = RunId::new();
        let mut job = submit_job(next_run, "deploy", 4);
        job.queued.concurrency = Some(job_concurrency("group-a", false));
        backend
            .submit_run(submit_run(next_run, vec![job]))
            .await
            .unwrap();
        match backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap()
        {
            PollOutcome::Claimed(claim) => {
                assert_eq!(claim.queued.job_id, JobId("deploy".to_owned()));
            }
            other => panic!("the group slot must be free after fail-fast, got {other:?}"),
        }
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

    /// Strict assignments refuse an unassigned job: the queue-time pairing
    /// never happened, so no runner may take the job (the `claim_permitted`
    /// ladder both backends must share). Requires `set_config(false, true, _)`
    /// from the wrapper.
    pub(crate) async fn strict_assignments_refuse_an_unassigned_job(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        // Submitted with no idle session: nothing pairs the job.
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        let runner = backend
            .register_runner(register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(create_session(runner.runner.id))
            .await
            .unwrap();

        let outcome = backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        assert!(
            matches!(outcome, PollOutcome::Empty),
            "strict mode: an unassigned job is never dispatched, got {outcome:?}"
        );
        let stats = backend.queue_stats().await.unwrap();
        assert_eq!(stats.ready, 1, "the job stays queued");
    }

    /// A fresh pool-pending row blocks every runner until it goes stale or the
    /// enqueue ceiling passes. Requires `set_config(true, false, _)`.
    pub(crate) async fn fresh_pool_pending_blocks_claim(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        // No session exists at enqueue: the job is marked pool-pending.
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        let runner = backend
            .register_runner(register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(create_session(runner.runner.id))
            .await
            .unwrap();

        let outcome = backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        assert!(
            matches!(outcome, PollOutcome::Empty),
            "a fresh pool-pending row blocks everyone until the ceiling, got {outcome:?}"
        );
    }

    /// A fresh assignment is exclusive to its paired runner: an equally
    /// labelled runner may not steal it. Requires `set_config(true, false, _)`.
    pub(crate) async fn fresh_assignment_is_exclusive_to_its_runner(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        // A pool-proven idle runner is paired at enqueue.
        let mut registration = register_runner("a");
        registration.pool_proven = true;
        let runner_a = backend.register_runner(registration).await.unwrap();
        backend
            .create_session(create_session(runner_a.runner.id))
            .await
            .unwrap();
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();

        // A second, equally labelled runner must not take the fresh pairing.
        let runner_b = backend.register_runner(register_runner("b")).await.unwrap();
        let session_b = backend
            .create_session(create_session(runner_b.runner.id))
            .await
            .unwrap();
        let outcome = backend
            .poll_session(poll(&session_b.session_id, runner_b.runner.id))
            .await
            .unwrap();
        assert!(
            matches!(outcome, PollOutcome::Empty),
            "a fresh assignment is exclusive to its runner, got {outcome:?}"
        );

        // The paired runner still takes it.
        let session_a = backend
            .create_session(create_session(runner_a.runner.id))
            .await
            .unwrap();
        let outcome = backend
            .poll_session(poll(&session_a.session_id, runner_a.runner.id))
            .await
            .unwrap();
        assert!(
            matches!(outcome, PollOutcome::Claimed(_)),
            "the paired runner claims its own job, got {outcome:?}"
        );
    }

    /// The claim scan must reach a matching job anywhere in the ready queue:
    /// a 64-row candidate window sorted by pool cannot hide a job a runner can
    /// serve behind unrelated pools (unbounded cross-pool starvation).
    pub(crate) async fn claim_scan_finds_a_matching_job_past_the_window(
        backend: &dyn ControlBackend,
    ) {
        let runner = backend
            .register_runner(register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(create_session(runner.runner.id))
            .await
            .unwrap();
        let zzz = RunnerCapabilities {
            known: true,
            labels: vec!["zzz".to_owned()],
            runner_group_id: None,
            runner_group_name: None,
        };
        let zzz_poll = |session_id: &str| PollRequest {
            session_id: session_id.to_owned(),
            verified_runner_id: Some(runner.runner.id),
            runner: zzz.clone(),
            busy: false,
            wait_ms: 0,
        };
        let fillers = |run_id: RunId, count: usize, base_request: i64| {
            (0..count)
                .map(|i| {
                    let mut job =
                        submit_job(run_id, &format!("filler-{i}"), base_request + i as i64);
                    job.queued.runs_on = vec!["alpha".to_owned()];
                    job
                })
                .collect::<Vec<_>>()
        };
        let target = |run_id: RunId, request_id: i64| {
            let mut job = submit_job(run_id, "target", request_id);
            job.queued.runs_on = vec!["zzz".to_owned()];
            job
        };

        // Control: the matching job sits inside the first candidate window.
        let small = RunId::new();
        let mut jobs = fillers(small, 63, 1);
        jobs.push(target(small, 100));
        backend.submit_run(submit_run(small, jobs)).await.unwrap();
        let outcome = backend
            .poll_session(zzz_poll(&session.session_id))
            .await
            .unwrap();
        let PollOutcome::Claimed(claimed) = outcome else {
            panic!("a matching job inside the window must be dispatched, got {outcome:?}");
        };
        assert_eq!(claimed.queued.job_id, JobId("target".to_owned()));
        backend
            .complete_job(crate::control::backend::JobCompletionInput {
                run_id: small,
                job_id: JobId("target".to_owned()),
                agent_job_id: Some(claimed.request.agent_job_id),
                status: ExecutionStatus::Success,
                outputs: BTreeMap::new(),
                runner_id: Some(runner.runner.id),
            })
            .await
            .unwrap();

        // The same job, now the 66th ready row: it must still be found.
        let big = RunId::new();
        let mut jobs = fillers(big, 65, 200);
        jobs.push(target(big, 300));
        backend.submit_run(submit_run(big, jobs)).await.unwrap();
        let outcome = backend
            .poll_session(zzz_poll(&session.session_id))
            .await
            .unwrap();
        let PollOutcome::Claimed(claimed) = outcome else {
            panic!("a matching job past the 64-row window must be dispatched, got {outcome:?}");
        };
        assert_eq!(claimed.queued.job_id, JobId("target".to_owned()));
        assert_eq!(claimed.queued.run_id, big);
    }

    /// A settled attempt's deferred token-mint recipe must not outlive its
    /// job: the completion drops the `github_token_requests` row, so a later
    /// `acquire_context` no longer sees it.
    pub(crate) async fn settle_drops_the_deferred_token_request(backend: &dyn ControlBackend) {
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
        let outcome = backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        let PollOutcome::Claimed(claimed) = outcome else {
            panic!("expected a claim, got {outcome:?}");
        };
        let request_id = claimed.request.request_id;
        assert!(
            backend
                .acquire_context(request_id)
                .await
                .unwrap()
                .token_request
                .is_some(),
            "the deferred recipe exists while the job may still run"
        );

        backend
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
        assert!(
            backend
                .acquire_context(request_id)
                .await
                .unwrap()
                .token_request
                .is_none(),
            "the deferred App-token recipe must not outlive the terminal job"
        );
    }

    /// An attempt no runner and no live session owns cannot be settled: the
    /// ownership ladder ends in `NotFound`, exactly like `renew_broker_request`.
    pub(crate) async fn settle_refuses_an_unowned_attempt(backend: &dyn ControlBackend) {
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
        let outcome = backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        let PollOutcome::Claimed(claimed) = outcome else {
            panic!("expected a claim, got {outcome:?}");
        };
        let agent = claimed.request.agent_job_id;
        assert!(
            backend
                .release_claimed_request(claimed.request.request_id, "")
                .await
                .unwrap(),
            "the claim is released for redelivery"
        );

        let outcome = backend
            .settle_job(SettleJob {
                completion: preloop_gha_protocol::JobCompletion {
                    run_id,
                    job_id: JobId("build".to_owned()),
                    agent_job_id: Some(agent),
                    status: ExecutionStatus::Success,
                    outputs: preloop_gha_protocol::OutputMap::new(),
                    annotations: Vec::new(),
                    step_results: Vec::new(),
                },
                settle: Some(AttemptSettle {
                    agent_job_id: agent,
                    runner_id: runner.runner.id,
                }),
            })
            .await;
        assert!(
            matches!(outcome, Err(ControlError::NotFound(_))),
            "an attempt with no owner and no live session cannot be settled, got {outcome:?}"
        );
    }

    /// `settle_job` applies the reported step's runner number to the attempt's
    /// manifest (`conclusion` + `runner number` per the trait doc).
    pub(crate) async fn completion_applies_the_reported_step_number(backend: &dyn ControlBackend) {
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
        let outcome = backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        let PollOutcome::Claimed(claimed) = outcome else {
            panic!("expected a claim, got {outcome:?}");
        };
        let agent = claimed.request.agent_job_id;

        let outcome = backend
            .settle_job(SettleJob {
                completion: preloop_gha_protocol::JobCompletion {
                    run_id,
                    job_id: JobId("build".to_owned()),
                    agent_job_id: Some(agent),
                    status: ExecutionStatus::Success,
                    outputs: preloop_gha_protocol::OutputMap::new(),
                    annotations: Vec::new(),
                    step_results: vec![preloop_gha_protocol::CompletionStepResult {
                        external_id: Some("step-1".to_owned()),
                        number: Some(4),
                        name: None,
                        status: Some(serde_json::json!("completed")),
                        conclusion: Some(serde_json::json!("succeeded")),
                    }],
                },
                settle: Some(AttemptSettle {
                    agent_job_id: agent,
                    runner_id: runner.runner.id,
                }),
            })
            .await
            .unwrap();
        assert!(
            matches!(outcome, SettleJobOutcome::Settled(_)),
            "the completion settles the job, got {outcome:?}"
        );
        let manifests = backend.run_step_manifests(run_id).await.unwrap();
        let steps = manifests.get(&agent).expect("manifest for the attempt");
        let step = steps.iter().find(|s| s.id == "step-1").expect("step-1");
        assert_eq!(
            step.runner_number,
            Some(4),
            "the reported step's runner number must persist"
        );
    }

    /// A settled attempt keeps its final lease expiry: the record reports a
    /// non-empty `lockedUntil` (`now + lease`), never a dropped lease row.
    pub(crate) async fn settle_refreshes_the_attempt_lease(backend: &dyn ControlBackend) {
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
        let outcome = backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        let PollOutcome::Claimed(claimed) = outcome else {
            panic!("expected a claim, got {outcome:?}");
        };
        let request_id = claimed.request.request_id;
        assert!(
            !backend
                .request(RequestKey::Id(request_id))
                .await
                .unwrap()
                .locked_until
                .is_empty(),
            "a claimed attempt holds a lease"
        );

        backend
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
        assert!(
            !backend
                .request(RequestKey::Id(request_id))
                .await
                .unwrap()
                .locked_until
                .is_empty(),
            "the settled attempt keeps its final lease expiry"
        );
    }

    /// A reusable caller's resolved outputs must survive a reload: the fold
    /// writes them onto the caller's job row (both backends).
    pub(crate) async fn reusable_caller_outputs_survive_a_reload(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let runner = backend
            .register_runner(register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(create_session(runner.runner.id))
            .await
            .unwrap();

        let mut submit = submit_run(run_id, vec![submit_job(run_id, "caller/inner", 1)]);
        {
            let mut caller = submit_job(run_id, "caller", 2);
            caller.queued.reusable_call = Some(preloop_gha_protocol::ReusableCallPlan {
                uses: "./.github/workflows/callee.yml".to_owned(),
                workflow_file: "callee.yml".to_owned(),
                workflow_sha: None,
                workflow_repository: None,
                depth: 1,
            });
            submit.jobs.push(caller);
            // The caller node's own plan (the real submit path keeps one for
            // every `reusable_call`) — pg keys the node's reusable spec on it.
            submit.record.caller_plans.insert(
                JobId("caller".to_owned()),
                serde_json::from_value(serde_json::json!({
                    "id": "caller",
                    "base_id": "caller",
                    "name": "caller",
                    "runs_on": ["self-hosted"],
                }))
                .expect("a minimal caller plan"),
            );
            submit.record.reusable_calls.insert(
                "caller".to_owned(),
                preloop_gha_parser::ReusableCallMetadata {
                    caller_job_id: "caller".to_owned(),
                    output_definitions: BTreeMap::from([(
                        "out".to_owned(),
                        "${{ jobs.inner.outputs.val }}".to_owned(),
                    )]),
                    inner_job_ids: vec!["caller/inner".to_owned()],
                    inputs: BTreeMap::new(),
                    caller_concurrency: None,
                    embedded_concurrency: None,
                    matrix: BTreeMap::new(),
                    if_condition: None,
                    workflow_sha: None,
                    workflow_repository: None,
                },
            );
        }
        backend.submit_run(submit).await.unwrap();

        let outcome = backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        let PollOutcome::Claimed(claimed) = outcome else {
            panic!("expected the callee job to be claimable, got {outcome:?}");
        };
        assert_eq!(claimed.queued.job_id, JobId("caller/inner".to_owned()));
        backend
            .complete_job(crate::control::backend::JobCompletionInput {
                run_id,
                job_id: JobId("caller/inner".to_owned()),
                agent_job_id: Some(claimed.request.agent_job_id),
                status: ExecutionStatus::Success,
                outputs: BTreeMap::from([(
                    "val".to_owned(),
                    serde_json::Value::String("hi".to_owned()),
                )]),
                runner_id: Some(runner.runner.id),
            })
            .await
            .unwrap();

        // A fresh reload: the caller's resolved outputs come from its job row.
        let record = backend.run_record(run_id).await.unwrap();
        assert_eq!(
            record.job_outputs.get(&JobId("caller".to_owned())),
            Some(&BTreeMap::from([(
                "out".to_owned(),
                serde_json::Value::String("hi".to_owned())
            )])),
            "the reusable caller's resolved outputs must persist"
        );
    }

    /// A workflow-held run's row reads `queued` — its `holder_kind = 'run'`
    /// wait row carries the logical `Pending` — so it counts with
    /// in-progress, and its parked jobs are not job-level concurrency
    /// blocks. The pre-refactor snapshot counts the same way.
    pub(crate) async fn workflow_held_run_is_not_queued(backend: &dyn ControlBackend) {
        let run_a = RunId::new();
        let mut submit_a = submit_run(run_a, vec![submit_job(run_a, "deploy", 1)]);
        submit_a.workflow_concurrency = Some(workflow_concurrency("held", false));
        assert!(!backend.submit_run(submit_a).await.unwrap().held);

        let run_b = RunId::new();
        let mut submit_b = submit_run(run_b, vec![submit_job(run_b, "deploy", 2)]);
        submit_b.workflow_concurrency = Some(workflow_concurrency("held", false));
        assert!(backend.submit_run(submit_b).await.unwrap().held);

        let inputs = backend
            .status_inputs(std::time::Duration::from_secs(15))
            .await
            .unwrap();
        assert_eq!(
            (
                inputs.runs_queued,
                inputs.runs_in_progress,
                inputs.runs_completed
            ),
            (1, 1, 0),
            "a held run is Pending (in-progress), never queued"
        );
        assert_eq!(inputs.queue_len, 1, "only the unheld run's job is ready");
        assert_eq!(
            inputs.concurrency_blocked, 0,
            "workflow-level holds are not job-level concurrency blocks"
        );
    }

    /// `status_inputs().runner_assignments` is the live pairing set: an
    /// in-flight attempt whose session is gone (left for the reaper) is not
    /// a live runner → job pairing.
    pub(crate) async fn status_assignments_require_a_live_session(backend: &dyn ControlBackend) {
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
            other => panic!("expected a claim, got {other:?}"),
        }
        let live = pairing_keys(&backend.live_assignments().await.unwrap());
        assert_eq!(live.len(), 1, "the claimed attempt is live");
        let inputs = backend
            .status_inputs(std::time::Duration::from_secs(15))
            .await
            .unwrap();
        assert_eq!(
            pairing_keys(&inputs.runner_assignments),
            live,
            "status inputs report the same pairings as live_assignments"
        );

        backend.delete_session(&session.session_id).await.unwrap();
        assert!(
            backend.live_assignments().await.unwrap().is_empty(),
            "a deleted session owns no live assignment"
        );
        let inputs = backend
            .status_inputs(std::time::Duration::from_secs(15))
            .await
            .unwrap();
        assert!(
            inputs.runner_assignments.is_empty(),
            "a session-less attempt is not a live pairing"
        );
    }

    /// Claim and pairing share one label matcher with the pre-refactor
    /// semantics: `runs-on: ubuntu` matches a `linux` runner, while `osx-14`
    /// never matches a `macos` runner.
    pub(crate) async fn claim_uses_the_pairing_label_matcher(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let runner = backend
            .register_runner(register_runner_with_labels("linux-1", &["linux"]))
            .await
            .unwrap();
        let session = backend
            .create_session(create_session(runner.runner.id))
            .await
            .unwrap();
        backend
            .submit_run(submit_run(
                run_id,
                vec![submit_job_on(run_id, "build", 1, &["ubuntu"])],
            ))
            .await
            .unwrap();
        match backend
            .poll_session(poll_with_labels(
                &session.session_id,
                runner.runner.id,
                &["linux"],
            ))
            .await
            .unwrap()
        {
            PollOutcome::Claimed(claimed) => {
                assert_eq!(claimed.queued.job_id, JobId("build".to_owned()));
            }
            other => panic!("`ubuntu` must match a `linux` runner, got {other:?}"),
        }

        let run_id = RunId::new();
        let runner = backend
            .register_runner(register_runner_with_labels("mac-1", &["macos"]))
            .await
            .unwrap();
        let session = backend
            .create_session(create_session(runner.runner.id))
            .await
            .unwrap();
        backend
            .submit_run(submit_run(
                run_id,
                vec![submit_job_on(run_id, "build", 2, &["osx-14"])],
            ))
            .await
            .unwrap();
        match backend
            .poll_session(poll_with_labels(
                &session.session_id,
                runner.runner.id,
                &["macos"],
            ))
            .await
            .unwrap()
        {
            PollOutcome::Empty => {}
            other => panic!("`osx-14` must not match a `macos` runner, got {other:?}"),
        }
    }

    /// Fixture identity for [`swept_binding_is_reported`]'s per-backend seed.
    pub(crate) struct SweptBinding {
        pub(crate) runner_id: i64,
        pub(crate) run_id: RunId,
        pub(crate) job_id: String,
    }

    /// A stale binding released by `sweep_stale_bindings` must be published
    /// as `StatusInputs::released_bindings`, the counter the operational
    /// snapshot reports. No command ages a binding, so the caller seeds the
    /// stale row itself (`seed`).
    pub(crate) async fn swept_binding_is_reported<F, Fut>(backend: &dyn ControlBackend, seed: F)
    where
        F: FnOnce(SweptBinding) -> Fut,
        Fut: Future<Output = ()>,
    {
        let run_id = RunId::new();
        let runner = backend
            .register_runner(register_runner("r1"))
            .await
            .unwrap();
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        seed(SweptBinding {
            runner_id: runner.runner.id,
            run_id,
            job_id: "build".to_owned(),
        })
        .await;

        let swept = backend.sweep_stale_bindings().await.unwrap();
        assert_eq!(swept, 1, "the aged binding must be swept");
        let inputs = backend
            .status_inputs(std::time::Duration::from_secs(15))
            .await
            .unwrap();
        assert_eq!(
            inputs.released_bindings, swept as u64,
            "a swept binding must be reported by the status inputs"
        );
    }

    /// `pair_runner` marks the runner pool-proven (trait doc step 1) even
    /// when no pending job matches: `pool_proven` is exactly the gate
    /// enqueue-time binding applies while pool assignments are on, and the
    /// only other stamp site is registration, which always passes
    /// `pool_proven: false`.
    pub(crate) async fn pairing_marks_runner_pool_proven(backend: &dyn ControlBackend) {
        let runner = backend
            .register_runner(register_runner("pool-proven"))
            .await
            .unwrap();
        assert!(
            !runner.pool_proven,
            "registration alone must not prove a runner"
        );
        backend.pair_runner(runner.runner.id).await.unwrap();
        let after = backend
            .update_runner(runner.runner.id, None, None)
            .await
            .unwrap();
        assert!(
            after.pool_proven,
            "pair_runner must mark the runner pool-proven"
        );
    }

    /// One reaper tick over `run_id` at `now`, fed by a fresh
    /// `reap_inputs` snapshot.
    fn sweep_at(now: std::time::SystemTime, run_id: RunId, inputs: ReapInputs) -> ReapSweep {
        ReapSweep {
            now,
            runs: [run_id].into_iter().collect(),
            ready: inputs.ready,
            active: inputs.active,
            paused: BTreeMap::new(),
            pool_preparing: false,
            warm_window_open: false,
            first_seen: BTreeMap::new(),
        }
    }

    /// The timeout arm and the lease arm of `reap_sweep` are independent
    /// (trait doc steps 2 and 3): an attempt that already hit its
    /// `timeout-minutes` and whose runner then went silent must still have
    /// its lease expired and settle as a failure on the next tick.
    pub(crate) async fn timeout_then_lease_expiry_settles_attempt(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let runner = backend
            .register_runner(register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(create_session(runner.runner.id))
            .await
            .unwrap();
        let mut job = submit_job(run_id, "build", 1);
        // 10 minutes: the timeout must fire well before the 45-minute lease.
        job.queued.message.job_timeout = Some(600);
        backend
            .submit_run(submit_run(run_id, vec![job]))
            .await
            .unwrap();
        let claimed = match backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap()
        {
            PollOutcome::Claimed(claimed) => claimed,
            other => panic!("expected a claim, got {other:?}"),
        };
        let inputs = backend.reap_inputs().await.unwrap();
        let started = inputs
            .active
            .iter()
            .find(|active| active.request_id == claimed.request.request_id)
            .and_then(|active| active.started_at)
            .expect("the claim stamps started_at");

        // Tick 1: the job timeout fires; the lease is still live.
        let inputs = backend.reap_inputs().await.unwrap();
        let outcome = backend
            .reap_sweep(sweep_at(
                started + std::time::Duration::from_secs(601),
                run_id,
                inputs,
            ))
            .await
            .unwrap();
        assert_eq!(outcome.cancellations, 1, "the job timeout must fire");

        // Tick 2: the runner never renewed; the lease expires past 45 min.
        let inputs = backend.reap_inputs().await.unwrap();
        let outcome = backend
            .reap_sweep(sweep_at(
                started + std::time::Duration::from_secs(2701),
                run_id,
                inputs,
            ))
            .await
            .unwrap();
        assert_eq!(
            outcome.expired.len(),
            1,
            "a timeout-triggered attempt must still settle on lease expiry"
        );
        assert_eq!(
            outcome.expired[0].request_id, claimed.request.request_id,
            "the expired attempt is the timed-out one"
        );
        let request = backend
            .request(RequestKey::Id(claimed.request.request_id))
            .await
            .unwrap();
        assert_eq!(
            request.result,
            Some(ExecutionStatus::Failure),
            "lease expiry must fail the attempt"
        );
    }

    /// `job_queue_state` returns the `QueueKind` vocabulary, not the storage
    /// column: a dependency-blocked job reads `pending`.
    pub(crate) async fn job_queue_state_speaks_queue_kind(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let mut child = submit_job(run_id, "child", 2);
        child.queued.needs = vec![JobId("parent".to_owned())];
        backend
            .submit_run(submit_run(
                run_id,
                vec![submit_job(run_id, "parent", 1), child],
            ))
            .await
            .unwrap();
        let (kind, _status) = backend
            .job_queue_state(run_id, &JobId("child".to_owned()))
            .await
            .unwrap()
            .expect("the blocked job has a live row");
        assert_eq!(
            kind, "pending",
            "a needs-blocked job is `pending` in the QueueKind vocabulary"
        );
    }

    /// `callback_job` resolves by plan id, then timeline id, then agent job
    /// id (trait doc): a timeline-id match outranks a newer attempt that
    /// only matches the agent job id.
    pub(crate) async fn callback_prefers_timeline_match(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let mut older = submit_job(run_id, "build", 1);
        let timeline = uuid::Uuid::new_v4();
        older.request.as_mut().unwrap().timeline_id = timeline;
        let newer = submit_job(run_id, "other", 2);
        let agent = newer.request.as_ref().unwrap().agent_job_id;
        backend
            .submit_run(submit_run(run_id, vec![older, newer]))
            .await
            .unwrap();
        let older_request = backend
            .request(RequestKey::Job(run_id, JobId("build".to_owned())))
            .await
            .unwrap();
        let newer_request = backend
            .request(RequestKey::Job(run_id, JobId("other".to_owned())))
            .await
            .unwrap();
        assert!(
            older_request.request_id < newer_request.request_id,
            "fixture: the agent-job-id match must be the newer attempt"
        );
        let found = backend
            .callback_job("not-a-uuid", Some(timeline), Some(agent))
            .await
            .unwrap()
            .expect("one attempt resolves");
        assert_eq!(
            found.request_id, older_request.request_id,
            "the timeline-id match must outrank the newer agent-job-id match"
        );
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
    /// Directory holding `initdb`/`pg_ctl` for this cluster.
    bin: PathBuf,
}

impl DisposablePg {
    fn start(bin: PathBuf) -> Self {
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

        Self { dir, port, bin }
    }

    fn url(&self) -> String {
        format!("postgres://postgres@127.0.0.1:{}/postgres", self.port)
    }
}

impl Drop for DisposablePg {
    fn drop(&mut self) {
        let _ = Command::new(self.bin.join("pg_ctl"))
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
/// `None` when no local server binaries are installed — the Postgres tests
/// then skip unless `PRELOOP_TEST_POSTGRES_URL` names a server.
fn pg_bin_opt() -> Option<PathBuf> {
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
            return Some(bin);
        }
    }
    // Fall back to PATH.
    if let Ok(path_var) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path_var) {
            if dir.join("initdb").exists() {
                return Some(dir);
            }
        }
    }
    None
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

/// A URL to a database nobody else uses, plus its cleanup guard. `None`
/// when no Postgres is reachable: `PRELOOP_TEST_POSTGRES_URL` is unset and
/// no local server binaries are installed, so tests skip instead of failing
/// (the repo-wide convention for optional Postgres coverage).
async fn fresh_database_opt() -> Option<(PgGuard, String)> {
    if let Some((db, url)) = crate::test_pg::fresh_database().await {
        return Some((PgGuard::Database(db), url));
    }
    let bin = pg_bin_opt()?;
    let pg = DisposablePg::start(bin);
    let url = pg.url();
    Some((PgGuard::Cluster(pg), url))
}

/// Printed by every Postgres test that cannot reach a server.
fn skip_no_postgres() {
    eprintln!(
        "skipping: set PRELOOP_TEST_POSTGRES_URL to a Postgres server, or install postgresql"
    );
}

mod pg {
    use super::*;
    use super::{PgGuard, fresh_database_opt, skip_no_postgres};
    use crate::control::pg::PgBackend;

    async fn connect(url: &str) -> PgBackend {
        PgBackend::connect(url, false, false, std::time::Duration::from_secs(300))
            .await
            .expect("test database connection failed")
    }

    /// A backend on its own fresh database. Keep the guard alive for the
    /// test's duration. `None` when no Postgres is reachable: the caller
    /// prints the skip notice and returns.
    async fn backend() -> Option<(PgGuard, PgBackend)> {
        let (guard, url) = fresh_database_opt().await?;
        let backend = connect(&url).await;
        Some((guard, backend))
    }

    /// Two independent backends (separate pools) on ONE database: the
    /// shape of two engine nodes sharing a cell, for race tests.
    async fn backend_pair() -> Option<(PgGuard, PgBackend, PgBackend)> {
        let (guard, url) = fresh_database_opt().await?;
        let first = connect(&url).await;
        let second = connect(&url).await;
        Some((guard, first, second))
    }

    #[tokio::test]
    async fn submit_poll_complete_lifecycle() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::submit_poll_complete_lifecycle(&backend).await;
    }

    #[tokio::test]
    async fn sessionless_runner_is_not_idle_capacity() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::sessionless_runner_is_not_idle_capacity(&backend).await;
    }

    #[tokio::test]
    async fn pool_busy_counts_only_pool_proven_busy_runners() {
        let (_pg, backend) = backend().await;
        suite::pool_busy_counts_only_pool_proven_busy_runners(&backend).await;
    }

    #[tokio::test]
    async fn step_reports_merge_into_manifest() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::step_reports_merge_into_manifest(&backend).await;
    }

    #[tokio::test]
    async fn webhook_replay_is_idempotent() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::webhook_replay_is_idempotent(&backend).await;
    }

    #[tokio::test]
    async fn request_lookup_resolves_attempt_correlations() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::request_lookup_resolves_attempt_correlations(&backend).await;
    }

    #[tokio::test]
    async fn push_state_only_changes_its_run_column() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::push_state_only_changes_its_run_column(&backend).await;
    }

    #[tokio::test]
    async fn cancel_run_queues_cancellation() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::cancel_run_queues_cancellation(&backend).await;
    }

    #[tokio::test]
    async fn matrix_fail_fast_settles_dependent_and_run() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::matrix_fail_fast_settles_dependent_and_run(&backend).await;
    }

    #[tokio::test]
    async fn cancelled_run_discards_in_flight_expansion() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::cancelled_run_discards_in_flight_expansion(&backend).await;
    }

    #[tokio::test]
    async fn cancel_of_terminal_run_preserves_conclusion() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::cancel_of_terminal_run_preserves_conclusion(&backend).await;
    }

    #[tokio::test]
    async fn starved_run_keeps_workflow_hold() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::starved_run_keeps_workflow_hold(&backend).await;
    }

    #[tokio::test]
    async fn redelivered_cancellation_keeps_its_body() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::redelivered_cancellation_keeps_its_body(&backend).await;
    }

    #[tokio::test]
    async fn artifact_scopes_map_plan_ids_to_latest_run() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::artifact_scopes_map_plan_ids_to_latest_run(&backend).await;
    }

    #[tokio::test]
    async fn concurrency_gate_serializes_group() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::concurrency_gate_serializes_group(&backend).await;
    }

    #[tokio::test]
    async fn workflow_concurrency_releases_when_the_run_finishes() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::workflow_concurrency_releases_when_the_run_finishes(&backend).await;
    }

    #[tokio::test]
    async fn workflow_gate_release_respects_needs() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::workflow_gate_release_respects_needs(&backend).await;
    }

    #[tokio::test]
    async fn jobset_gate_is_scoped_to_run_namespace() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::jobset_gate_is_scoped_to_run_namespace(&backend).await;
    }

    #[tokio::test]
    async fn fail_fast_releases_the_cancelled_sibling_concurrency_slot() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::fail_fast_releases_the_cancelled_sibling_concurrency_slot(&backend).await;
    }

    #[tokio::test]
    async fn run_record_round_trips_through_tables() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
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
    async fn list_runs_filters_on_the_projected_status() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::list_runs_filters_on_the_projected_status(&backend).await;
    }

    #[tokio::test]
    async fn timeline_patch_is_bounded_and_keeps_the_patch() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::timeline_patch_is_bounded_and_keeps_the_patch(&backend).await;
    }

    #[tokio::test]
    async fn reconcile_recovers_orphaned_claim() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::reconcile_recovers_orphaned_claim(&backend).await;
    }

    #[tokio::test]
    async fn purge_requeues_claimed_as_queued() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::purge_requeues_claimed_as_queued(&backend).await;
    }

    #[tokio::test]
    async fn purge_requeues_ownerless_claim() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::purge_requeues_ownerless_claim(&backend).await;
    }

    #[tokio::test]
    async fn check_run_mapping() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::check_run_mapping(&backend).await;
    }

    #[tokio::test]
    async fn submit_unhostable_job_persists_failure() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::submit_unhostable_job_persists_failure(&backend).await;
    }

    #[tokio::test]
    async fn submit_skipped_parent_settles_child() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::submit_skipped_parent_settles_child(&backend).await;
    }

    #[tokio::test]
    async fn cancel_in_progress_submit_reports_surviving_depth() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::cancel_in_progress_submit_reports_surviving_depth(&backend).await;
    }

    #[tokio::test]
    async fn strict_assignments_refuse_an_unassigned_job() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        backend.set_config(false, true, std::time::Duration::from_secs(300));
        suite::strict_assignments_refuse_an_unassigned_job(&backend).await;
    }

    #[tokio::test]
    async fn fresh_pool_pending_blocks_claim() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        backend.set_config(true, false, std::time::Duration::from_secs(300));
        suite::fresh_pool_pending_blocks_claim(&backend).await;
    }

    #[tokio::test]
    async fn fresh_assignment_is_exclusive_to_its_runner() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        backend.set_config(true, false, std::time::Duration::from_secs(300));
        suite::fresh_assignment_is_exclusive_to_its_runner(&backend).await;
    }

    #[tokio::test]
    async fn claim_scan_finds_a_matching_job_past_the_window() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::claim_scan_finds_a_matching_job_past_the_window(&backend).await;
    }

    #[tokio::test]
    async fn settle_drops_the_deferred_token_request() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::settle_drops_the_deferred_token_request(&backend).await;
    }

    #[tokio::test]
    async fn settle_refuses_an_unowned_attempt() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::settle_refuses_an_unowned_attempt(&backend).await;
    }

    #[tokio::test]
    async fn completion_applies_the_reported_step_number() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::completion_applies_the_reported_step_number(&backend).await;
    }

    #[tokio::test]
    async fn settle_refreshes_the_attempt_lease() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::settle_refreshes_the_attempt_lease(&backend).await;
    }

    #[tokio::test]
    async fn reusable_caller_outputs_survive_a_reload() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::reusable_caller_outputs_survive_a_reload(&backend).await;
    }

    /// Pool assignments on: the pairing path only runs in that mode.
    #[tokio::test]
    async fn pairing_marks_runner_pool_proven() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        backend.set_config(true, false, std::time::Duration::from_secs(300));
        suite::pairing_marks_runner_pool_proven(&backend).await;
    }

    #[tokio::test]
    async fn timeout_then_lease_expiry_settles_attempt() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::timeout_then_lease_expiry_settles_attempt(&backend).await;
    }

    #[tokio::test]
    async fn job_queue_state_speaks_queue_kind() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::job_queue_state_speaks_queue_kind(&backend).await;
    }

    #[tokio::test]
    async fn callback_prefers_timeline_match() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::callback_prefers_timeline_match(&backend).await;
    }

    #[tokio::test]
    async fn secret_values_never_persist() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::secret_values_never_persist(&backend).await;
    }

    #[tokio::test]
    async fn baseline_mask_hints_survive_template_storage() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::baseline_mask_hints_survive_template_storage(&backend).await;
    }

    #[tokio::test]
    async fn webhook_inbox_claim_is_fenced_and_deduplicated() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
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
        let Some((_pg, node_a, node_b)) = backend_pair().await else {
            return skip_no_postgres();
        };
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
        let Some((_pg, node_a, node_b)) = backend_pair().await else {
            return skip_no_postgres();
        };
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

    #[tokio::test]
    async fn workflow_held_run_is_not_queued() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::workflow_held_run_is_not_queued(&backend).await;
    }

    #[tokio::test]
    async fn status_assignments_require_a_live_session() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::status_assignments_require_a_live_session(&backend).await;
    }

    #[tokio::test]
    async fn claim_uses_the_pairing_label_matcher() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::claim_uses_the_pairing_label_matcher(&backend).await;
    }

    #[tokio::test]
    async fn swept_binding_is_reported() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        let db = &backend;
        suite::swept_binding_is_reported(
            &backend,
            move |binding: suite::SweptBinding| async move {
                // ASSIGNMENT_TTL (600s) is one sweep window: age past it.
                db.test_execute(&format!(
                    "INSERT INTO job_assignments (run_id, job_id, runner_id, assigned_at, \
                 first_assigned_at) VALUES ('{}', '{}', {}, now() - interval '11 minutes', \
                 now() - interval '11 minutes')",
                    binding.run_id, binding.job_id, binding.runner_id,
                ))
                .await
                .unwrap();
            },
        )
        .await;
    }
}
// ── New SQLite backend (`control::lite`) ────────────────────────────────
//
// The shared `suite::*` functions run against `LiteBackend` via
// `&dyn ControlBackend`.
mod lite {
    use super::suite;
    use super::{built_matrix_leg, create_session, poll, register_runner, submit_job, submit_run};
    use crate::control::backend::{ControlBackend, ExpansionApply, JobCompletionInput};
    use crate::control::lite::LiteBackend;
    use crate::control::types::PollOutcome;
    use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    /// Two materialized legs of the `fan` matrix.
    fn fan_legs(run_id: RunId) -> crate::control::logic::BuiltExpansion {
        crate::control::logic::BuiltExpansion::Matrix {
            jobs: vec![
                built_matrix_leg(run_id, "fan-1", "fan", 1, 2),
                built_matrix_leg(run_id, "fan-2", "fan", 2, 2),
            ],
        }
    }

    /// Queue and lease the deferred `fan` node of a fresh run (`gen` produces
    /// the matrix), returning the lease.
    async fn lease_fan(
        backend: &LiteBackend,
        run_id: RunId,
    ) -> crate::control::types::ExpansionClaim {
        let mut fan = submit_job(run_id, "fan", 2);
        fan.queued.needs = vec![JobId("gen".to_owned())];
        fan.queued.deferred_matrix = Some("${{ fromJSON(needs.gen.outputs.m) }}".to_owned());
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "gen", 1), fan]))
            .await
            .unwrap();
        backend
            .complete_job(JobCompletionInput {
                run_id,
                job_id: JobId("gen".to_owned()),
                agent_job_id: None,
                status: ExecutionStatus::Success,
                outputs: BTreeMap::from([("m".to_owned(), serde_json::json!({"x": [1, 2]}))]),
                runner_id: None,
            })
            .await
            .unwrap();
        backend.claim_expansion().await.unwrap().unwrap()
    }

    /// Cancelling an already-terminal run must not append a second
    /// `run.completed.v1` (pg emits it once, guarded).
    #[tokio::test]
    async fn terminal_cancel_does_not_re_emit_run_completed() {
        let backend = LiteBackend::in_memory().unwrap();
        let run_id = RunId::new();
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        backend
            .complete_job(JobCompletionInput {
                run_id,
                job_id: JobId("build".to_owned()),
                agent_job_id: None,
                status: ExecutionStatus::Success,
                outputs: BTreeMap::new(),
                runner_id: None,
            })
            .await
            .unwrap();
        backend.cancel_run(run_id, None).await.unwrap();
        backend.cancel_run(run_id, None).await.unwrap();
        let state = backend.test_working_set().unwrap();
        let completed = state
            .outbox_topics
            .iter()
            .filter(|(run, topic)| *run == Some(run_id) && topic == "run.completed.v1")
            .count();
        assert_eq!(
            completed, 1,
            "a run must emit run.completed.v1 exactly once"
        );
        assert_eq!(state.runs[&run_id].conclusion.as_deref(), Some("success"));
    }

    /// A run whose jobs all conclude at submit still emits its terminal event.
    #[tokio::test]
    async fn submit_concluded_run_emits_completion() {
        let backend = LiteBackend::in_memory().unwrap();
        let run_id = RunId::new();
        let mut job = submit_job(run_id, "skip", 1);
        job.initially_skipped = true;
        backend
            .submit_run(submit_run(run_id, vec![job]))
            .await
            .unwrap();
        let state = backend.test_working_set().unwrap();
        let topics: Vec<&str> = state
            .outbox_topics
            .iter()
            .filter(|(run, _)| *run == Some(run_id))
            .map(|(_, topic)| topic.as_str())
            .collect();
        assert!(topics.contains(&"run.created.v1"), "got {topics:?}");
        assert!(
            topics.contains(&"run.completed.v1"),
            "a run concluded at submit must emit run.completed.v1, got {topics:?}"
        );
    }

    /// A run cancelled on arrival emits both its creation and its completion.
    #[tokio::test]
    async fn arrival_cancelled_submit_emits_run_events() {
        let backend = LiteBackend::in_memory().unwrap();
        let newer = RunId::new();
        let mut newer_submit = submit_run(newer, vec![submit_job(newer, "deploy", 1)]);
        newer_submit.workflow_concurrency = Some(super::workflow_concurrency("g", true));
        Arc::make_mut(&mut newer_submit.record.submission).payload =
            serde_json::json!({"repository": {"pushed_at": 2000}});
        backend.submit_run(newer_submit).await.unwrap();

        let older = RunId::new();
        let mut older_submit = submit_run(older, vec![submit_job(older, "deploy", 2)]);
        older_submit.workflow_concurrency = Some(super::workflow_concurrency("g", true));
        Arc::make_mut(&mut older_submit.record.submission).payload =
            serde_json::json!({"repository": {"pushed_at": 1000}});
        let outcome = backend.submit_run(older_submit).await.unwrap();
        assert_eq!(
            outcome.rejected,
            Some(ExecutionStatus::Cancelled),
            "the older arrival must be superseded"
        );
        let state = backend.test_working_set().unwrap();
        let topics: Vec<&str> = state
            .outbox_topics
            .iter()
            .filter(|(run, _)| *run == Some(older))
            .map(|(_, topic)| topic.as_str())
            .collect();
        assert!(topics.contains(&"run.created.v1"), "got {topics:?}");
        assert!(
            topics.contains(&"run.completed.v1"),
            "an arrival-cancelled run must emit run.completed.v1, got {topics:?}"
        );
    }

    /// Fail-fast cancellations record the `fail_fast` reason (pg parity).
    #[tokio::test]
    async fn fail_fast_cancellation_reason_is_fail_fast() {
        let backend = LiteBackend::in_memory().unwrap();
        let run_id = RunId::new();
        let claim = lease_fan(&backend, run_id).await;
        backend
            .apply_expansion(ExpansionApply {
                job: claim.job,
                generation: claim.generation,
                built: Ok(fan_legs(run_id)),
            })
            .await
            .unwrap();

        // Claim one leg so it is in progress when the sibling fails.
        let runner = backend
            .register_runner(register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(create_session(runner.runner.id))
            .await
            .unwrap();
        let claimed = match backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap()
        {
            PollOutcome::Claimed(claimed) => claimed,
            other => panic!("expected a claim, got {other:?}"),
        };
        let other = if claimed.queued.job_id == JobId("fan-1".to_owned()) {
            JobId("fan-2".to_owned())
        } else {
            JobId("fan-1".to_owned())
        };
        backend
            .complete_job(JobCompletionInput {
                run_id,
                job_id: other,
                agent_job_id: None,
                status: ExecutionStatus::Failure,
                outputs: BTreeMap::new(),
                runner_id: None,
            })
            .await
            .unwrap();

        let state = backend.test_working_set().unwrap();
        assert_eq!(
            state.cancellation_reasons.len(),
            1,
            "the in-progress sibling must be cancelled"
        );
        assert_eq!(
            state.cancellation_reasons[0].2.as_deref(),
            Some("fail_fast"),
            "fail-fast cancellations must match pg's reason"
        );
    }

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
    async fn matrix_fail_fast_settles_dependent_and_run() {
        suite::matrix_fail_fast_settles_dependent_and_run(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn cancelled_run_discards_in_flight_expansion() {
        suite::cancelled_run_discards_in_flight_expansion(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn cancel_of_terminal_run_preserves_conclusion() {
        suite::cancel_of_terminal_run_preserves_conclusion(&LiteBackend::in_memory().unwrap())
            .await;
    }

    #[tokio::test]
    async fn starved_run_keeps_workflow_hold() {
        suite::starved_run_keeps_workflow_hold(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn redelivered_cancellation_keeps_its_body() {
        suite::redelivered_cancellation_keeps_its_body(&LiteBackend::in_memory().unwrap()).await;
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
    async fn baseline_mask_hints_survive_template_storage() {
        suite::baseline_mask_hints_survive_template_storage(&LiteBackend::in_memory().unwrap())
            .await;
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
    async fn list_runs_filters_on_the_projected_status() {
        suite::list_runs_filters_on_the_projected_status(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn timeline_patch_is_bounded_and_keeps_the_patch() {
        suite::timeline_patch_is_bounded_and_keeps_the_patch(&LiteBackend::in_memory().unwrap())
            .await;
    }

    #[tokio::test]
    async fn workflow_concurrency_releases_when_the_run_finishes() {
        suite::workflow_concurrency_releases_when_the_run_finishes(
            &LiteBackend::in_memory().unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn workflow_gate_release_respects_needs() {
        suite::workflow_gate_release_respects_needs(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn jobset_gate_is_scoped_to_run_namespace() {
        suite::jobset_gate_is_scoped_to_run_namespace(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn fail_fast_releases_the_cancelled_sibling_concurrency_slot() {
        suite::fail_fast_releases_the_cancelled_sibling_concurrency_slot(
            &LiteBackend::in_memory().unwrap(),
        )
        .await;
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

    /// Archiving a run drops its attempt timelines with it — pg asserts the
    /// same with `timelines.timeline_id REFERENCES job_requests(timeline_id)
    /// ON DELETE CASCADE`. Without it the rows leak forever: the archive
    /// deletes the `job_requests` rows `prune_timelines` matches on.
    #[tokio::test]
    async fn archive_drops_the_runs_timelines() {
        use crate::control::types::PollOutcome;

        let backend = LiteBackend::in_memory().unwrap();
        let run_id = RunId::new();
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();
        backend
            .submit_run(super::submit_run(
                run_id,
                vec![super::submit_job(run_id, "build", 1)],
            ))
            .await
            .unwrap();
        let claimed = match backend
            .poll_session(super::poll(&session.session_id, runner.runner.id))
            .await
            .unwrap()
        {
            PollOutcome::Claimed(claimed) => claimed,
            other => panic!("expected a claim, got {other:?}"),
        };
        let key = format!(
            "{}/{}",
            claimed.request.plan_id, claimed.request.timeline_id
        );
        backend
            .patch_timeline(&key, vec![super::timeline_record(1, "one")])
            .await
            .unwrap();
        assert_eq!(
            backend
                .get_timeline(&key, 0, usize::MAX)
                .await
                .unwrap()
                .1
                .len(),
            1
        );

        backend
            .complete_job(crate::control::backend::JobCompletionInput {
                run_id,
                job_id: JobId("build".to_owned()),
                agent_job_id: Some(claimed.request.agent_job_id),
                status: ExecutionStatus::Success,
                outputs: std::collections::BTreeMap::new(),
                runner_id: Some(runner.runner.id),
            })
            .await
            .unwrap();

        // Backdate the completion past the archive grace so the tick claims
        // the run.
        let completed = crate::models::now_unix_nanos() / 1000 - 120 * 1_000_000;
        backend
            .test_db_mutate(|db| {
                db.execute(
                    "UPDATE runs SET completed_at = ?1 WHERE run_id = ?2",
                    (completed, run_id.0.to_string()),
                )
            })
            .unwrap()
            .unwrap();
        assert!(
            backend
                .archive_finished_runs(32)
                .await
                .unwrap()
                .contains(&run_id),
            "the settled run archives"
        );

        let (change_id, records) = backend.get_timeline(&key, 0, usize::MAX).await.unwrap();
        assert_eq!(change_id, 0, "the archived run's timeline is gone");
        assert!(records.is_empty(), "its records went with it");
    }

    /// A timed-out session-bound attempt is cancelled through the marker
    /// only: the runner's next poll delivers one `JobCancellation` carrying
    /// the official body — never an eagerly parked message whose body the
    /// poll path replaces with the bare request id.
    #[tokio::test]
    async fn reaper_timeout_delivers_a_well_formed_cancellation() {
        use crate::control::types::{PollOutcome, ReapSweep};

        let backend = LiteBackend::in_memory().unwrap();
        let run_id = RunId::new();
        let runner = backend
            .register_runner(super::register_runner("r1"))
            .await
            .unwrap();
        let session = backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();
        backend
            .submit_run(super::submit_run(
                run_id,
                vec![super::submit_job(run_id, "build", 1)],
            ))
            .await
            .unwrap();
        let claimed = match backend
            .poll_session(super::poll(&session.session_id, runner.runner.id))
            .await
            .unwrap()
        {
            PollOutcome::Claimed(claimed) => claimed,
            other => panic!("expected a claim, got {other:?}"),
        };

        // Backdate the attempt past `timeout-minutes` (default 6h).
        let started = crate::models::now_unix_nanos() / 1000 - 7 * 3600 * 1_000_000;
        backend
            .test_db_mutate(|db| {
                db.execute(
                    "UPDATE job_requests SET started_at = ?1 WHERE request_id = ?2",
                    (started, claimed.request.request_id),
                )
            })
            .unwrap()
            .unwrap();

        let inputs = backend.reap_inputs().await.unwrap();
        let outcome = backend
            .reap_sweep(ReapSweep {
                now: std::time::SystemTime::now(),
                runs: [run_id].into_iter().collect(),
                ready: Vec::new(),
                active: inputs.active,
                paused: Default::default(),
                pool_preparing: false,
                warm_window_open: false,
                first_seen: Default::default(),
            })
            .await
            .unwrap();
        assert_eq!(
            outcome.cancellations, 1,
            "the timed-out attempt queues one cancellation"
        );

        let poll = backend
            .poll_session(super::poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        let PollOutcome::Cancel(message) = poll else {
            panic!("the next poll must deliver the cancellation, got {poll:?}")
        };
        assert_eq!(message.message_type, "JobCancellation");
        assert_eq!(
            message.body,
            crate::concurrency::job_cancel_body(claimed.request.agent_job_id),
            "the cancellation carries its official body, not the request id"
        );
    }

    #[tokio::test]
    async fn strict_assignments_refuse_an_unassigned_job() {
        let backend = LiteBackend::in_memory().unwrap();
        backend.set_config(false, true, std::time::Duration::from_secs(300));
        suite::strict_assignments_refuse_an_unassigned_job(&backend).await;
    }

    #[tokio::test]
    async fn fresh_pool_pending_blocks_claim() {
        let backend = LiteBackend::in_memory().unwrap();
        backend.set_config(true, false, std::time::Duration::from_secs(300));
        suite::fresh_pool_pending_blocks_claim(&backend).await;
    }

    #[tokio::test]
    async fn fresh_assignment_is_exclusive_to_its_runner() {
        let backend = LiteBackend::in_memory().unwrap();
        backend.set_config(true, false, std::time::Duration::from_secs(300));
        suite::fresh_assignment_is_exclusive_to_its_runner(&backend).await;
    }

    #[tokio::test]
    async fn claim_scan_finds_a_matching_job_past_the_window() {
        suite::claim_scan_finds_a_matching_job_past_the_window(&LiteBackend::in_memory().unwrap())
            .await;
    }

    #[tokio::test]
    async fn settle_drops_the_deferred_token_request() {
        suite::settle_drops_the_deferred_token_request(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn settle_refuses_an_unowned_attempt() {
        suite::settle_refuses_an_unowned_attempt(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn completion_applies_the_reported_step_number() {
        suite::completion_applies_the_reported_step_number(&LiteBackend::in_memory().unwrap())
            .await;
    }

    #[tokio::test]
    async fn settle_refreshes_the_attempt_lease() {
        suite::settle_refreshes_the_attempt_lease(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn reusable_caller_outputs_survive_a_reload() {
        suite::reusable_caller_outputs_survive_a_reload(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn workflow_held_run_is_not_queued() {
        suite::workflow_held_run_is_not_queued(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn status_assignments_require_a_live_session() {
        suite::status_assignments_require_a_live_session(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn claim_uses_the_pairing_label_matcher() {
        suite::claim_uses_the_pairing_label_matcher(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn swept_binding_is_reported() {
        let backend = LiteBackend::in_memory().unwrap();
        let db = &backend;
        suite::swept_binding_is_reported(
            &backend,
            move |binding: suite::SweptBinding| async move {
                // ASSIGNMENT_TTL (600s) is one sweep window: age past it.
                let stale = (std::time::SystemTime::now() - std::time::Duration::from_secs(11 * 60))
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as i64;
                db.exec_for_test(&format!(
                    "INSERT INTO job_assignments (run_id, job_id, runner_id, assigned_at, \
                 first_assigned_at) VALUES ('{}', '{}', {}, {stale}, {stale})",
                    binding.run_id, binding.job_id, binding.runner_id,
                ));
            },
        )
        .await;
    }

    /// A poll without a token-verified identity must not satisfy a fresh pool
    /// assignment: the session's self-declared runner id is not proof
    /// (`effective_claim_runner` / `claim_permitted`), so the caller gets
    /// `Empty` and the bound job stays ready for its verified runner.
    ///
    /// Lite-only: pg's claim path ignores the assignment/verified ladder
    /// (tracked separately), so it claims here either way.
    #[tokio::test]
    async fn unverified_poll_cannot_claim_a_bound_job() {
        let backend = LiteBackend::in_memory().unwrap();
        backend.set_config(true, true, std::time::Duration::from_secs(300));
        let run_id = RunId::new();
        backend
            .submit_run(super::submit_run(
                run_id,
                vec![super::submit_job(run_id, "build", 1)],
            ))
            .await
            .unwrap();
        let runner = backend
            .register_runner(super::RegisterRunner {
                pool_proven: true,
                ..super::register_runner("r1")
            })
            .await
            .unwrap();
        let session = backend
            .create_session(super::create_session(runner.runner.id))
            .await
            .unwrap();
        let binding = backend
            .test_working_set()
            .unwrap()
            .job_assignments
            .get(&(run_id, JobId("build".to_owned())))
            .cloned()
            .expect("the pool-proven registration binds the pending job");
        assert_eq!(binding.runner_id, Some(runner.runner.id));

        let outcome = backend
            .poll_session(super::poll_unverified(&session.session_id))
            .await
            .unwrap();
        assert!(
            matches!(outcome, super::PollOutcome::Empty),
            "an unverified caller cannot claim a job bound to a runner, got {outcome:?}"
        );
        assert_eq!(
            backend
                .job_queue_state(run_id, &JobId("build".to_owned()))
                .await
                .unwrap(),
            Some(("ready".to_owned(), "queued".to_owned())),
            "the bound job stays ready for its verified runner"
        );
    }

    /// Pool assignments on: the pairing path only runs in that mode.
    #[tokio::test]
    async fn pairing_marks_runner_pool_proven() {
        let backend = LiteBackend::in_memory().unwrap();
        backend.set_config(true, false, std::time::Duration::from_secs(300));
        suite::pairing_marks_runner_pool_proven(&backend).await;
    }

    #[tokio::test]
    async fn timeout_then_lease_expiry_settles_attempt() {
        suite::timeout_then_lease_expiry_settles_attempt(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn job_queue_state_speaks_queue_kind() {
        suite::job_queue_state_speaks_queue_kind(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn callback_prefers_timeline_match() {
        suite::callback_prefers_timeline_match(&LiteBackend::in_memory().unwrap()).await;
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

// ── Schema copies and legacy identity ───────────────────────────────────

/// `docs/control-schema.sql` is a hand copy of the backend's schema, and the
/// control-plane job applies *it* to prove the target schema is valid; keep
/// the two identical (comments and blank lines aside) so that check cannot
/// validate a stale DDL while the backend accepts something else.
#[test]
fn docs_control_schema_matches_backend_schema() {
    fn ddl(sql: &str) -> String {
        sql.lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n")
    }
    assert_eq!(
        ddl(include_str!("../../../../docs/control-schema.sql")),
        ddl(include_str!("pg/schema.sql")),
        "docs/control-schema.sql drifted from control/pg/schema.sql"
    );
}

/// Legacy session ids are stored under their RFC 4122 v5 (SHA-1) encoding, so
/// the derivation must not change — a stored id's rows would become
/// unreachable. Reference value from an independent implementation
/// (Python `uuid.uuid5` with the same namespace).
#[test]
fn session_uuid_keeps_the_legacy_v5_encoding() {
    use crate::control::logic::{session_uuid, SESSION_ID_NAMESPACE};

    assert_eq!(
        session_uuid("s1").to_string(),
        "b02cbb01-07e8-577e-8b5f-d710c9c53520"
    );
    assert_eq!(
        session_uuid(&SESSION_ID_NAMESPACE.to_string()).to_string(),
        SESSION_ID_NAMESPACE.to_string(),
        "a canonical UUID session id is already its own stored value"
    );
}
