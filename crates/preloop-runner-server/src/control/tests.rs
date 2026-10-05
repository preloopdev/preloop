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

pub(crate) fn queued_job(run_id: RunId, job_id: &str, request_id: i64) -> QueuedJob {
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

pub(crate) fn submit_job(run_id: RunId, job_id: &str, request_id: i64) -> SubmitJob {
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

pub(crate) fn submit_run(run_id: RunId, jobs: Vec<SubmitJob>) -> SubmitRun {
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

    /// Status events appended after the change are stamped with the version
    /// of the row they report on. Drives one job through claim and success and
    /// one run through queued and completed, appending a status event at each
    /// point, plus events that no longer match the row.
    pub(crate) async fn status_events_are_stamped_with_row_versions(backend: &dyn ControlBackend) {
        use preloop_gha_protocol::NdjsonEvent;
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
        let job = |status| NdjsonEvent::JobStatus {
            run_id,
            job_id: JobId("build".to_owned()),
            status,
            reason: None,
        };
        let run = |status| NdjsonEvent::RunStatus {
            run_id,
            status,
            reason: None,
        };
        backend
            .append_event(&job(ExecutionStatus::Queued))
            .await
            .unwrap();
        backend
            .append_event(&run(ExecutionStatus::Queued))
            .await
            .unwrap();

        let poll = backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        let PollOutcome::Claimed(claimed) = poll else {
            panic!("expected a claim, got {poll:?}");
        };
        // The same state twice: reading the row must not move its version.
        for _ in 0..2 {
            backend
                .append_event(&job(ExecutionStatus::InProgress))
                .await
                .unwrap();
        }
        // The row is in progress, not queued: a non-final mismatch is
        // published without an ordering claim.
        backend
            .append_event(&job(ExecutionStatus::Queued))
            .await
            .unwrap();

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
        // The job and the run settled on success: events for any other state
        // are stale and are not published at all.
        backend
            .append_event(&job(ExecutionStatus::Failure))
            .await
            .unwrap();
        backend
            .append_event(&run(ExecutionStatus::InProgress))
            .await
            .unwrap();
        backend
            .append_event(&job(ExecutionStatus::Success))
            .await
            .unwrap();
        backend
            .append_event(&run(ExecutionStatus::Success))
            .await
            .unwrap();
    }

    /// What [`status_events_are_stamped_with_row_versions`] must have left in
    /// the outbox, from `(topic, job_id, version)` rows in write order.
    pub(crate) fn assert_status_stamps(stamps: &[(String, Option<String>, Option<i64>)]) {
        let versions = |topic: &str| -> Vec<Option<i64>> {
            stamps
                .iter()
                .filter(|(t, _, _)| t == topic)
                .map(|(_, _, version)| *version)
                .collect()
        };
        let jobs = versions("job_status.v1");
        // queued, in progress twice, the mismatched queued, success; the
        // failure after success was dropped.
        assert_eq!(jobs.len(), 5, "job status rows: {jobs:?}");
        let (queued, claimed, claimed_again, moved_on, done) =
            (jobs[0], jobs[1], jobs[2], jobs[3], jobs[4]);
        assert!(queued.is_some() && claimed.is_some() && done.is_some());
        assert!(queued < claimed, "claiming bumps the version: {jobs:?}");
        assert_eq!(claimed, claimed_again, "appending must not bump: {jobs:?}");
        assert_eq!(moved_on, None, "a state the row left makes no claim");
        assert!(claimed < done, "completing bumps the version: {jobs:?}");
        assert!(
            stamps
                .iter()
                .filter(|(t, _, _)| t == "job_status.v1")
                .all(|(_, job, _)| job.as_deref() == Some("build")),
            "job status rows carry their job id"
        );
        let runs = versions("run_status.v1");
        // queued, then success; the in-progress event after completion was
        // dropped.
        assert_eq!(runs.len(), 2, "run status rows: {runs:?}");
        assert!(runs[0].is_some() && runs[1].is_some());
        assert!(runs[0] < runs[1], "settling bumps the run: {runs:?}");
    }

    /// Outbox retention: rows younger than the window survive, a batch never
    /// exceeds its limit, and a pass past the window empties the table.
    pub(crate) async fn prune_outbox_keeps_fresh_rows_and_bounds_a_batch(
        backend: &dyn ControlBackend,
    ) {
        let run_id = RunId::new();
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        for _ in 0..3 {
            backend
                .append_event(&preloop_gha_protocol::NdjsonEvent::CheckRunCreated { run_id })
                .await
                .unwrap();
        }
        let hour = std::time::Duration::from_secs(3600);
        assert_eq!(
            backend.prune_outbox(hour, 1000).await.unwrap(),
            0,
            "rows inside the window are kept"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let now = std::time::Duration::ZERO;
        assert_eq!(
            backend.prune_outbox(now, 1).await.unwrap(),
            1,
            "one per batch"
        );
        assert!(backend.prune_outbox(now, 1000).await.unwrap() >= 2);
        assert_eq!(backend.prune_outbox(now, 1000).await.unwrap(), 0, "empty");
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

        // An unknown run is a no-op, not an error, on every backend: callers
        // treat any `Err` as a failed write of a run they do hold.
        let unknown = RunId::new();
        backend
            .set_push_state(
                unknown,
                crate::models::PushState {
                    status: crate::models::PushStatus::Blocked,
                    error: None,
                    pr_number: None,
                    effective_sha: None,
                },
            )
            .await
            .expect("an unknown run is a no-op");
        assert!(
            backend.run_record(unknown).await.is_err(),
            "nothing was created"
        );
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

    /// A deferred build that fails before producing a subtree settles its
    /// node exactly once: `settle_node` records the failure in the returned
    /// outcome, so a duplicate would re-report the node's check run.
    pub(crate) async fn failed_deferred_expansion_settles_the_node_once(
        backend: &dyn ControlBackend,
    ) {
        let run_id = RunId::new();
        let fan_id = JobId("fan".to_owned());
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
        let claim = backend
            .claim_expansion()
            .await
            .unwrap()
            .expect("the deferred node must be leased");
        let outcome = backend
            .apply_expansion(ExpansionApply {
                job: claim.job,
                generation: claim.generation,
                built: Err(ExecutionStatus::Failure),
            })
            .await
            .unwrap();
        assert_eq!(
            outcome.failed,
            vec![(run_id, fan_id.clone())],
            "a failed build settles its node once, got {outcome:?}"
        );
        assert_eq!(
            backend.job_queue_state(run_id, &fan_id).await.unwrap(),
            Some(("none".to_owned(), "failure".to_owned())),
            "the failed build's node is terminal"
        );
    }

    /// A deferred matrix node fans out with its own scoped `inputs`: the
    /// caller's `with:` values when the node was materialized inside a
    /// reusable callee (its stored plan carries them), else the run's dispatch
    /// inputs. Fanning out with anything else hands the cells an `inputs`
    /// context no other job in the run sees (#285).
    pub(crate) async fn deferred_matrix_expansion_scopes_its_inputs(backend: &dyn ControlBackend) {
        // A top-level node has no stored plan; the fan-out falls back to the
        // dispatch inputs the submit path stamped on every plan.
        let run_id = RunId::new();
        let dispatch_inputs = BTreeMap::from([("dry_run".to_owned(), serde_json::json!(true))]);
        let mut fan = submit_job(run_id, "fan", 2);
        fan.queued.needs = vec![JobId("gen".to_owned())];
        fan.queued.deferred_matrix = Some("${{ fromJSON(needs.gen.outputs.m) }}".to_owned());
        let mut submit = submit_run(run_id, vec![submit_job(run_id, "gen", 1), fan]);
        submit.record.submission = Arc::new(preloop_gha_protocol::WorkflowSubmission {
            dispatch_inputs: dispatch_inputs.clone(),
            ..submission()
        });
        backend.submit_run(submit).await.unwrap();
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
        match claim
            .plan
            .expect("a deferred matrix node must plan an expansion")
        {
            crate::control::logic::ExpansionPlan::Matrix(inputs) => assert_eq!(
                inputs.scoped_inputs, dispatch_inputs,
                "a top-level deferred node must fan out with the dispatch inputs"
            ),
            crate::control::logic::ExpansionPlan::Reusable(_) => {
                panic!("expected a matrix expansion plan")
            }
        }

        // A node inside a reusable callee carries the caller's `with:` values
        // on its stored plan; the empty dispatch map of a push-triggered run
        // must not erase them.
        let run_id = RunId::new();
        let with_values = BTreeMap::from([("mode".to_owned(), serde_json::json!("plan"))]);
        let stored_plan: preloop_gha_protocol::JobPlan =
            serde_json::from_value(serde_json::json!({
                "id": "call/fan",
                "base_id": "call/fan",
                "name": "call / fan",
                "runs_on": ["self-hosted"],
                "inputs": {"mode": "plan"},
            }))
            .expect("the stored callee plan decodes");
        let mut callee_fan = submit_job(run_id, "call/fan", 2);
        callee_fan.queued.needs = vec![JobId("call/gen".to_owned())];
        callee_fan.queued.deferred_matrix = Some("${{ fromJSON(needs.gen.outputs.m) }}".to_owned());
        let mut submit = submit_run(run_id, vec![submit_job(run_id, "call/gen", 1), callee_fan]);
        assert!(
            submit.record.submission.dispatch_inputs.is_empty(),
            "the callee case must not be rescued by a root dispatch map"
        );
        submit
            .record
            .caller_plans
            .insert(JobId("call/fan".to_owned()), stored_plan);
        backend.submit_run(submit).await.unwrap();
        backend
            .complete_job(JobCompletionInput {
                run_id,
                job_id: JobId("call/gen".to_owned()),
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
            .expect("the deferred callee node must be leased");
        assert_eq!(claim.job.job_id, JobId("call/fan".to_owned()));
        match claim
            .plan
            .expect("a deferred matrix node must plan an expansion")
        {
            crate::control::logic::ExpansionPlan::Matrix(inputs) => assert_eq!(
                inputs.scoped_inputs, with_values,
                "a callee node must fan out with the caller's with values"
            ),
            crate::control::logic::ExpansionPlan::Reusable(_) => {
                panic!("expected a matrix expansion plan")
            }
        }
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
                pool_labels: Vec::new(),
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
        assert_eq!(
            b.status,
            ExecutionStatus::Pending,
            "the held run stays parked behind the group on every backend"
        );
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
            approvals: vec![crate::models::EnvironmentApprovalRecord {
                at_unix_nanos: 1_700_000_000_000_002_000,
                actor: Some("octocat".to_owned()),
                admin_override: false,
                note: None,
            }],
            ..Default::default()
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
        let outcome = backend.promote_ready_jobs(Some(run_id)).await.unwrap();
        assert_eq!(outcome.promoted, 1, "the released hold admits the job");
        assert_eq!(
            backend.job_queue_state(run_id, &job_id).await.unwrap(),
            Some(("ready".to_owned(), "queued".to_owned()))
        );
        let run = backend.run_record(run_id).await.unwrap();
        assert!(!run.fork_approval_pending);
        assert_eq!(run.fork_approval_note.as_deref(), Some("lgtm"));
    }

    /// A fork-approval hold nobody answered fails its run closed once the
    /// approval window has passed — and only then. A hold requested inside
    /// the window is left parked, a second sweep finds nothing new, and the
    /// expired run no longer holds. A sweep that errors (or skips the run)
    /// would leave untrusted fork work waiting for an approval forever.
    pub(crate) async fn expired_fork_hold_fails_closed(backend: &dyn ControlBackend) {
        let window_nanos: i64 = 24 * 3600 * 1_000_000_000;
        let now = crate::models::now_unix_nanos();
        let stale = RunId::new();
        let fresh = RunId::new();
        for (run_id, request_id, requested_at) in
            [(stale, 1, now - 2 * window_nanos), (fresh, 2, now)]
        {
            let mut submit = submit_run(run_id, vec![submit_job(run_id, "build", request_id)]);
            submit.record.fork_approval_pending = true;
            submit.record.fork_approval_requested_at_unix_nanos = Some(requested_at);
            backend.submit_run(submit).await.unwrap();
        }

        let expired = backend
            .expire_fork_approvals(now - window_nanos)
            .await
            .unwrap();
        assert_eq!(expired, vec![stale], "only the run past its window expires");

        let stale_run = backend.run_record(stale).await.unwrap();
        assert!(!stale_run.fork_approval_pending, "the expired hold clears");
        assert!(
            stale_run.status.is_terminal(),
            "the expired run fails closed, got {:?}",
            stale_run.status
        );
        let fresh_run = backend.run_record(fresh).await.unwrap();
        assert!(
            fresh_run.fork_approval_pending && !fresh_run.status.is_terminal(),
            "a hold inside its window stays parked"
        );
        assert!(
            backend
                .expire_fork_approvals(now - window_nanos)
                .await
                .unwrap()
                .is_empty(),
            "an expired run is not expired twice"
        );
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
            ..Default::default()
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
            wait_timer_minutes: 1,
            ..Default::default()
        });
        backend.set_environment_resolver(crate::environment_resolver::EnvironmentResolver::local(
            wait_timer,
        ));
        // The wait timer has not elapsed: the job stays parked.
        let outcome = backend.promote_ready_jobs(Some(run_id)).await.unwrap();
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
        let outcome = backend.promote_ready_jobs(Some(run_id)).await.unwrap();
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
            ..Default::default()
        });
        backend.set_environment_resolver(crate::environment_resolver::EnvironmentResolver::local(
            branches,
        ));
        let outcome = backend.promote_ready_jobs(Some(refused_run)).await.unwrap();
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

    /// A gate denied by the *promotion* sweep (a needs-satisfied job whose
    /// environment is evaluated there, not the parked-release path) settles
    /// the job exactly once: `settle_node` records the failure, so the
    /// sweep's outcome must carry one entry — a duplicate would re-report the
    /// check run and inflate `PromoteOutcome::failed`.
    pub(crate) async fn denied_environment_gate_at_promotion_fails_the_job_once(
        backend: &dyn ControlBackend,
    ) {
        let mut rules = EnvironmentRulesMap::new();
        rules.entry("owner/repo".to_owned()).or_default().insert(
            "prod".to_owned(),
            crate::config::EnvironmentRules {
                deployment_branches: vec!["main".to_owned()],
                ..Default::default()
            },
        );
        backend.set_environment_resolver(crate::environment_resolver::EnvironmentResolver::local(
            rules,
        ));
        let run_id = RunId::new();
        let deploy_id = JobId("deploy".to_owned());
        // `deploy` waits behind `gen`, so the deny lands on the sweep that
        // `complete_job(gen)` runs — the promotion pass, not submit.
        let mut deploy = submit_job(run_id, "deploy", 2);
        deploy.queued.needs = vec![JobId("gen".to_owned())];
        deploy.queued.environment = Some(serde_json::json!("prod"));
        backend
            .submit_run(submit_run(
                run_id,
                vec![submit_job(run_id, "gen", 1), deploy],
            ))
            .await
            .unwrap();
        let outcome = backend
            .complete_job(JobCompletionInput {
                run_id,
                job_id: JobId("gen".to_owned()),
                agent_job_id: None,
                status: ExecutionStatus::Success,
                outputs: BTreeMap::new(),
                runner_id: None,
            })
            .await
            .unwrap();
        assert_eq!(
            outcome.scheduling.failed,
            vec![(run_id, deploy_id.clone())],
            "a denied environment fails the job exactly once, got {:?}",
            outcome.scheduling
        );
        assert_eq!(
            backend.job_queue_state(run_id, &deploy_id).await.unwrap(),
            Some(("none".to_owned(), "failure".to_owned())),
            "a denied deployment never dispatches"
        );
    }

    /// `runs-on` that no registered runner can host concludes the job in the
    /// promotion sweep exactly once (the hydration path, distinct from the
    /// submit-time hostability check): `settle_node` records the failure, so
    /// the outcome must not carry a second copy that would re-report the
    /// check run.
    pub(crate) async fn unhostable_runs_on_at_promotion_fails_the_job_once(
        backend: &dyn ControlBackend,
    ) {
        let run_id = RunId::new();
        let build_id = JobId("build".to_owned());
        let mut build = submit_job(run_id, "build", 2);
        build.queued.needs = vec![JobId("gen".to_owned())];
        // No windows runner is registered: the resolved platform is
        // unhostable.
        build.queued.runs_on = vec!["windows-latest".to_owned()];
        backend
            .submit_run(submit_run(
                run_id,
                vec![submit_job(run_id, "gen", 1), build],
            ))
            .await
            .unwrap();
        let outcome = backend
            .complete_job(JobCompletionInput {
                run_id,
                job_id: JobId("gen".to_owned()),
                agent_job_id: None,
                status: ExecutionStatus::Success,
                outputs: BTreeMap::new(),
                runner_id: None,
            })
            .await
            .unwrap();
        assert_eq!(
            outcome.scheduling.failed,
            vec![(run_id, build_id.clone())],
            "an unhostable job fails exactly once, got {:?}",
            outcome.scheduling
        );
        assert_eq!(
            backend.job_queue_state(run_id, &build_id).await.unwrap(),
            Some(("none".to_owned(), "failure".to_owned())),
            "an unhostable job never queues"
        );
    }

    /// The TOML rules map for `owner/repo` carrying one environment's rule.
    fn rules_for(environment: &str, rule: crate::config::EnvironmentRules) -> EnvironmentRulesMap {
        let mut rules = EnvironmentRulesMap::new();
        rules
            .entry("owner/repo".to_owned())
            .or_default()
            .insert(environment.to_owned(), rule);
        rules
    }

    /// A job claiming an environment no rule set knows runs like any other
    /// job: the gate proceeds, no hold is armed, and no gate row survives.
    /// GitHub auto-creates a referenced environment unprotected, so preloop
    /// must not reject or park an unknown name (the registry is gone).
    pub(crate) async fn unknown_environment_runs_without_a_gate(backend: &dyn ControlBackend) {
        // Rules exist for a *different* environment: the unknown name must
        // still resolve to "no protection", not to the sibling's rule. The
        // submit path pre-evaluates the gate, so a parked (armed) job is the
        // realistic shape — as is the release below.
        backend.set_environment_resolver(crate::environment_resolver::EnvironmentResolver::local(
            rules_for(
                "other",
                crate::config::EnvironmentRules {
                    required_reviewers: 1,
                    ..Default::default()
                },
            ),
        ));
        let run_id = RunId::new();
        let job_id = JobId("deploy".to_owned());
        let mut submit = submit_run(run_id, vec![submit_job(run_id, "deploy", 1)]);
        submit.jobs[0].queued.environment = Some(serde_json::json!("staging"));
        submit.jobs[0].queued.environment_gate =
            Some(crate::models::EnvironmentGateState::default());
        backend.submit_run(submit).await.unwrap();

        let outcome = backend.promote_ready_jobs(Some(run_id)).await.unwrap();
        assert_eq!(
            outcome.promoted, 1,
            "an environment with no rules must not gate"
        );
        assert_eq!(outcome.failed, 0);
        assert_eq!(
            backend.job_queue_state(run_id, &job_id).await.unwrap(),
            Some(("ready".to_owned(), "queued".to_owned())),
            "the unknown environment's job reaches the ready queue"
        );
        let gate = backend
            .environment_gate(run_id, &job_id)
            .await
            .unwrap()
            .expect("job exists")
            .gate;
        assert!(
            gate.is_none(),
            "an unprotected environment must not leave gate state behind"
        );
        assert!(
            backend
                .pending_environment_approvals(Some(run_id))
                .await
                .unwrap()
                .is_empty(),
            "no review surface is announced for an unprotected environment"
        );
    }

    /// A required-reviewer rule parks the job on an armed approval gate
    /// (stamped `approval_requested_at`), and `record_environment_approval`
    /// decides it: approving releases the job to the ready queue, rejecting
    /// concludes it `failure` — GitHub's reviewer-rejection semantics. Every
    /// decision leaves its durable audit row.
    pub(crate) async fn reviewer_gate_holds_until_decision(backend: &dyn ControlBackend) {
        backend.set_environment_resolver(crate::environment_resolver::EnvironmentResolver::local(
            rules_for(
                "prod",
                crate::config::EnvironmentRules {
                    required_reviewers: 1,
                    ..Default::default()
                },
            ),
        ));
        let run_id = RunId::new();
        let job_id = JobId("deploy".to_owned());
        let mut submit = submit_run(run_id, vec![submit_job(run_id, "deploy", 1)]);
        submit.jobs[0].queued.environment = Some(serde_json::json!("prod"));
        // The submit path pre-evaluates the gate and parks the job when the
        // rules gate it; the sweep below arms the approval request.
        submit.jobs[0].queued.environment_gate =
            Some(crate::models::EnvironmentGateState::default());
        backend.submit_run(submit).await.unwrap();

        // Admission arms the gate and parks the job.
        let armed = backend.promote_ready_jobs(Some(run_id)).await.unwrap();
        assert_eq!(armed.promoted, 0, "the reviewer gate must hold the job");
        assert_eq!(armed.failed, 0, "an armed reviewer gate never denies");
        assert_eq!(
            backend.job_queue_state(run_id, &job_id).await.unwrap(),
            Some(("blocked".to_owned(), "pending".to_owned()))
        );
        let gate = backend
            .environment_gate(run_id, &job_id)
            .await
            .unwrap()
            .expect("job exists")
            .gate
            .expect("the reviewer gate is armed");
        assert!(
            gate.approval_requested_at_unix_nanos.is_some(),
            "the gate stamps when the approval was requested"
        );
        assert_eq!(gate.environment_name.as_deref(), Some("prod"));
        let pending = backend
            .pending_environment_approvals(Some(run_id))
            .await
            .unwrap();
        assert_eq!(
            pending.len(),
            1,
            "the held job is the GitHub announce loop's input"
        );

        // An approval satisfies the gate and the same command's promotion
        // pass releases the job.
        let outcome = backend
            .record_environment_approval(EnvironmentApproval {
                run_id,
                job_id: job_id.clone(),
                decision: EnvironmentDecision::Approve,
                actor: Some("octocat".to_owned()),
                admin_override: false,
                note: Some("ship it".to_owned()),
            })
            .await
            .unwrap();
        assert!(
            matches!(
                outcome.result,
                EnvironmentApprovalResult::Recorded {
                    approvals: 1,
                    satisfied: true,
                    ..
                }
            ),
            "one approval satisfies the one-reviewer gate, got {:?}",
            outcome.result
        );
        assert_eq!(outcome.promoted, 1, "the release promotes the job");
        assert_eq!(
            backend.job_queue_state(run_id, &job_id).await.unwrap(),
            Some(("ready".to_owned(), "queued".to_owned())),
            "the approved job reaches the ready queue"
        );
        let gate = backend
            .environment_gate(run_id, &job_id)
            .await
            .unwrap()
            .expect("job exists")
            .gate
            .expect("the satisfied gate keeps its record");
        assert_eq!(gate.approvals.len(), 1);
        assert_eq!(gate.approvals[0].actor.as_deref(), Some("octocat"));
        assert!(!gate.approvals[0].admin_override);
        let audit = backend
            .environment_approvals(run_id, &job_id)
            .await
            .unwrap();
        assert_eq!(audit.len(), 1, "the approval writes one audit row");
        assert_eq!(audit[0].decision, "approved");
        assert_eq!(audit[0].actor.as_deref(), Some("octocat"));
        assert_eq!(audit[0].environment, "prod");
        assert_eq!(audit[0].repository, "owner/repo");
        assert_eq!(audit[0].comment.as_deref(), Some("ship it"));
        assert!(!audit[0].admin_override);

        // A second job on the same rules: rejecting it fails the job closed.
        let rejected_run = RunId::new();
        let rejected_job = JobId("deploy".to_owned());
        let mut submit = submit_run(rejected_run, vec![submit_job(rejected_run, "deploy", 2)]);
        submit.jobs[0].queued.environment = Some(serde_json::json!("prod"));
        submit.jobs[0].queued.environment_gate =
            Some(crate::models::EnvironmentGateState::default());
        backend.submit_run(submit).await.unwrap();
        backend
            .promote_ready_jobs(Some(rejected_run))
            .await
            .unwrap();
        let outcome = backend
            .record_environment_approval(EnvironmentApproval {
                run_id: rejected_run,
                job_id: rejected_job.clone(),
                decision: EnvironmentDecision::Reject,
                actor: Some("octocat".to_owned()),
                admin_override: false,
                note: Some("not today".to_owned()),
            })
            .await
            .unwrap();
        assert!(
            matches!(outcome.result, EnvironmentApprovalResult::Rejected),
            "the rejection is recorded, got {:?}",
            outcome.result
        );
        assert_eq!(
            backend
                .job_queue_state(rejected_run, &rejected_job)
                .await
                .unwrap(),
            Some(("none".to_owned(), "failure".to_owned())),
            "a rejected deployment never dispatches"
        );
        assert!(
            backend
                .run_record(rejected_run)
                .await
                .unwrap()
                .status
                .is_terminal(),
            "the rejection concludes the run"
        );
        let audit = backend
            .environment_approvals(rejected_run, &rejected_job)
            .await
            .unwrap();
        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].decision, "rejected");
        assert_eq!(audit[0].comment.as_deref(), Some("not today"));
    }

    /// Rules that could not be fetched hold the job fail-closed — never
    /// "no rules": a GitHub-covered repository whose environment has never
    /// resolved answers `Pending`, admission parks the job `held`/`pending`,
    /// and the key queues for the reaper's fetch pass.
    pub(crate) async fn unresolved_environment_rules_hold_the_job(backend: &dyn ControlBackend) {
        let resolver = Arc::new(crate::environment_resolver::EnvironmentResolver::new(
            EnvironmentRulesMap::new(),
        ));
        resolver.set_github_configured();
        backend.set_environment_resolver(resolver.clone());
        let run_id = RunId::new();
        let job_id = JobId("deploy".to_owned());
        let mut submit = submit_run(run_id, vec![submit_job(run_id, "deploy", 1)]);
        submit.jobs[0].queued.environment = Some(serde_json::json!("prod"));
        // Parked at submit, the way the submit path parks a gated job.
        submit.jobs[0].queued.environment_gate =
            Some(crate::models::EnvironmentGateState::default());
        backend.submit_run(submit).await.unwrap();

        let outcome = backend.promote_ready_jobs(Some(run_id)).await.unwrap();
        assert_eq!(
            outcome.promoted, 0,
            "an unresolved rule set must not admit the job"
        );
        assert_eq!(outcome.failed, 0, "a fetch failure holds, it never denies");
        assert_eq!(
            backend.job_queue_state(run_id, &job_id).await.unwrap(),
            Some(("blocked".to_owned(), "pending".to_owned())),
            "the job stays held until the rules resolve"
        );
        assert_eq!(
            resolver.pending_keys(),
            vec![("owner/repo".to_owned(), "prod".to_owned())],
            "the held lookup queues its key for the reaper's fetch"
        );
        // A second sweep with the rules still unresolved must not change the
        // verdict: the hold is stable, not a one-shot pass.
        let again = backend.promote_ready_jobs(Some(run_id)).await.unwrap();
        assert_eq!((again.promoted, again.failed), (0, 0));
    }

    /// The runner evaluates `environment.url` after the job's steps ran and
    /// reports it with the completion (`environmentUrl` — the official
    /// runner's `JobRunner.CompleteJobAsync` reads it off
    /// `ActionsEnvironment.Url`). The settled job carries that evaluated
    /// value into its deployment row, overriding the pre-completion literal;
    /// before the report, a `${{ … }}` template is never surfaced as a URL.
    pub(crate) async fn reported_environment_url_lands_on_the_deployment_row(
        backend: &dyn ControlBackend,
    ) {
        backend.set_environment_resolver(crate::environment_resolver::EnvironmentResolver::local(
            EnvironmentRulesMap::new(),
        ));
        let run_id = RunId::new();
        let job_id = JobId("deploy".to_owned());
        let mut submit = submit_run(run_id, vec![submit_job(run_id, "deploy", 1)]);
        submit.jobs[0].queued.environment = Some(serde_json::json!({
            "name": "staging",
            "url": "https://${{ steps.s.outputs.host }}.example.com",
        }));
        backend.submit_run(submit).await.unwrap();
        // Pre-completion the row carries no URL: the template is unevaluated.
        // (The deployment row itself only materializes once the job's
        // environment is stored — assert the URL shape when it is there.)
        if let Some(row) = backend
            .environment_deployment(run_id, &job_id)
            .await
            .unwrap()
        {
            assert_eq!(row.environment, "staging");
            assert_eq!(
                row.environment_url, None,
                "an unevaluated `${{ }}` template is never surfaced as a URL"
            );
        }

        let (runner, session) = live_runner(backend, "env-url-runner").await;
        let claimed = match backend.poll_session(poll(&session, runner)).await.unwrap() {
            PollOutcome::Claimed(claimed) => claimed,
            other => panic!("expected a claim, got {other:?}"),
        };
        backend
            .settle_job(SettleJob {
                completion: preloop_gha_protocol::JobCompletion {
                    run_id,
                    job_id: job_id.clone(),
                    agent_job_id: Some(claimed.request.agent_job_id),
                    status: ExecutionStatus::Success,
                    outputs: preloop_gha_protocol::OutputMap::new(),
                    annotations: Vec::new(),
                    step_results: Vec::new(),
                    environment_url: Some("https://vm-42.example.com".to_owned()),
                },
                settle: Some(AttemptSettle {
                    agent_job_id: claimed.request.agent_job_id,
                    runner_id: runner,
                }),
            })
            .await
            .unwrap();
        let row = backend
            .environment_deployment(run_id, &job_id)
            .await
            .unwrap()
            .expect("the deployment row survives the completion");
        assert_eq!(
            row.environment_url.as_deref(),
            Some("https://vm-42.example.com"),
            "the runner-evaluated URL wins over the unevaluated template"
        );
    }

    /// A gate armed while the environment name was still a
    /// `${{ needs.* }}` template stamps that text; once hydration resolves
    /// the name into the stored runner message, the deployment row must
    /// report the environment GitHub actually knows — never the template.
    pub(crate) async fn deferred_environment_name_resolves_on_the_deployment_row(
        backend: &dyn ControlBackend,
    ) {
        backend.set_environment_resolver(crate::environment_resolver::EnvironmentResolver::local(
            EnvironmentRulesMap::new(),
        ));
        let run_id = RunId::new();
        let job_id = JobId("deploy".to_owned());
        let mut submit = submit_run(run_id, vec![submit_job(run_id, "deploy", 1)]);
        submit.jobs[0].queued.environment =
            Some(serde_json::json!("${{ needs.build.outputs.env }}"));
        submit.jobs[0].queued.environment_gate = Some(crate::models::EnvironmentGateState {
            environment_name: Some("${{ needs.build.outputs.env }}".to_owned()),
            ..Default::default()
        });
        // The hydrated runner message carries the name the gate evaluated
        // post-resolution — produced by `hydrate_needs_context` once the
        // needs completed.
        submit.jobs[0].queued.message.actions_environment =
            Some(preloop_gha_protocol::azdo::ActionsEnvironment {
                name: "staging".to_owned(),
                url: None,
            });
        backend.submit_run(submit).await.unwrap();

        let row = backend
            .environment_deployment(run_id, &job_id)
            .await
            .unwrap()
            .expect("an environment job has a deployment row");
        assert_eq!(
            row.environment, "staging",
            "the hydrated message name wins over the template stamp"
        );
    }

    /// A job whose `environment` name defers to `needs` outputs parks at
    /// submit and must be released by the sweep once the name resolves: the
    /// release path hydrates the stored message (which `promote_run` only
    /// does for `blocked` candidates) and re-evaluates the gate. A regression
    /// that skips hydration holds the job forever — the need completed and
    /// nothing ever dispatches.
    pub(crate) async fn deferred_environment_name_releases_after_needs(
        backend: &dyn ControlBackend,
    ) {
        backend.set_environment_resolver(crate::environment_resolver::EnvironmentResolver::local(
            EnvironmentRulesMap::new(),
        ));
        let run_id = RunId::new();
        let mut submit = submit_run(
            run_id,
            vec![
                submit_job(run_id, "build", 1),
                submit_job(run_id, "deploy", 2),
            ],
        );
        submit
            .record
            .job_needs
            .insert(JobId("deploy".to_owned()), vec![JobId("build".to_owned())]);
        let deploy = &mut submit.jobs[1].queued;
        deploy.needs = vec![JobId("build".to_owned())];
        deploy.environment = Some(serde_json::json!("${{ needs.build.outputs.env }}"));
        deploy.environment_gate = Some(crate::models::EnvironmentGateState {
            environment_name: Some("${{ needs.build.outputs.env }}".to_owned()),
            ..Default::default()
        });
        deploy.message.actions_environment = Some(preloop_gha_protocol::azdo::ActionsEnvironment {
            name: "${{ needs.build.outputs.env }}".to_owned(),
            url: None,
        });
        backend.submit_run(submit).await.unwrap();

        // `deploy` is parked; only `build` is ready.
        assert_eq!(
            backend
                .job_queue_state(run_id, &JobId("build".to_owned()))
                .await
                .unwrap(),
            Some(("ready".to_owned(), "queued".to_owned()))
        );
        assert_eq!(
            backend
                .job_queue_state(run_id, &JobId("deploy".to_owned()))
                .await
                .unwrap(),
            Some(("blocked".to_owned(), "pending".to_owned()))
        );

        let (runner, session) = live_runner(backend, "env-defer-runner").await;
        let claimed = match backend.poll_session(poll(&session, runner)).await.unwrap() {
            PollOutcome::Claimed(claimed) => claimed,
            other => panic!("expected the build job, got {other:?}"),
        };
        assert_eq!(claimed.queued.job_id, JobId("build".to_owned()));
        let mut outputs = preloop_gha_protocol::OutputMap::new();
        outputs.insert("env".to_owned(), serde_json::json!("staging"));
        backend
            .settle_job(SettleJob {
                completion: preloop_gha_protocol::JobCompletion {
                    run_id,
                    job_id: JobId("build".to_owned()),
                    agent_job_id: Some(claimed.request.agent_job_id),
                    status: ExecutionStatus::Success,
                    outputs,
                    annotations: Vec::new(),
                    step_results: Vec::new(),
                    environment_url: None,
                },
                settle: Some(AttemptSettle {
                    agent_job_id: claimed.request.agent_job_id,
                    runner_id: runner,
                }),
            })
            .await
            .unwrap();

        // The release sweep hydrates `deploy`'s message, re-evaluates the
        // gate against "staging" (no rules configured → proceed) and hands
        // the job to the ordinary promotion path.
        let outcome = backend.promote_ready_jobs(Some(run_id)).await.unwrap();
        assert_eq!(
            outcome.promoted, 1,
            "the resolved environment name releases the parked job"
        );
        let deploy_id = JobId("deploy".to_owned());
        assert_eq!(
            backend.job_queue_state(run_id, &deploy_id).await.unwrap(),
            Some(("ready".to_owned(), "queued".to_owned())),
            "a resolved name with no protection must dispatch"
        );
        let row = backend
            .environment_deployment(run_id, &deploy_id)
            .await
            .unwrap()
            .expect("the deployment row outlives the hold");
        assert_eq!(row.environment, "staging");
    }

    /// The `environment_approvals` audit row outlives the decision, the job
    /// and the run: it is written in the decision's own transaction and
    /// archival never deletes it. `backdate` moves the completed run past the
    /// archival grace so `archive_finished_runs` really claims it (no
    /// `ControlBackend` command expresses a `completed_at` edit, so each
    /// backend supplies its own SQL escape hatch).
    pub(crate) async fn environment_approval_audit_survives_archival<F, Fut>(
        backend: &dyn ControlBackend,
        backdate: F,
    ) where
        F: Fn(RunId) -> Fut,
        Fut: Future<Output = ()>,
    {
        backend.set_environment_resolver(crate::environment_resolver::EnvironmentResolver::local(
            rules_for(
                "prod",
                crate::config::EnvironmentRules {
                    required_reviewers: 1,
                    ..Default::default()
                },
            ),
        ));
        let run_id = RunId::new();
        let job_id = JobId("deploy".to_owned());
        let mut submit = submit_run(run_id, vec![submit_job(run_id, "deploy", 1)]);
        submit.jobs[0].queued.environment = Some(serde_json::json!("prod"));
        // Parked at submit (the submit path parks a gated job), so the
        // approval below has an armed gate to decide.
        submit.jobs[0].queued.environment_gate =
            Some(crate::models::EnvironmentGateState::default());
        backend.submit_run(submit).await.unwrap();
        backend.promote_ready_jobs(Some(run_id)).await.unwrap();
        backend
            .record_environment_approval(EnvironmentApproval {
                run_id,
                job_id: job_id.clone(),
                decision: EnvironmentDecision::Approve,
                actor: Some("octocat".to_owned()),
                admin_override: false,
                note: Some("ship it".to_owned()),
            })
            .await
            .unwrap();

        // Run the approved job to completion through a real claim.
        let (runner, session) = live_runner(backend, "audit-runner").await;
        let claimed = match backend.poll_session(poll(&session, runner)).await.unwrap() {
            PollOutcome::Claimed(claimed) => claimed,
            other => panic!("expected a claim, got {other:?}"),
        };
        backend
            .complete_job(JobCompletionInput {
                run_id,
                job_id: job_id.clone(),
                agent_job_id: Some(claimed.request.agent_job_id),
                status: ExecutionStatus::Success,
                outputs: BTreeMap::new(),
                runner_id: Some(runner),
            })
            .await
            .unwrap();
        assert!(
            backend
                .run_record(run_id)
                .await
                .unwrap()
                .status
                .is_terminal(),
            "the completed job settles its run"
        );
        assert_eq!(
            backend
                .environment_approvals(run_id, &job_id)
                .await
                .unwrap()
                .len(),
            1,
            "the audit row survives the job's completion"
        );

        backdate(run_id).await;
        assert!(
            backend
                .archive_finished_runs(32)
                .await
                .unwrap()
                .contains(&run_id),
            "the completed run must archive"
        );
        let audit = backend
            .environment_approvals(run_id, &job_id)
            .await
            .unwrap();
        assert_eq!(
            audit.len(),
            1,
            "archival must never delete the environment-review audit"
        );
        assert_eq!(audit[0].decision, "approved");
        assert_eq!(audit[0].actor.as_deref(), Some("octocat"));
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
        let (_, stored) = backend.patch_timeline(&key, fill, &[]).await.unwrap();
        assert_eq!(stored.len(), MAX_TIMELINE_RECORDS);

        // One more record, sorting after every stored one. The write-side
        // bound evicts the lowest stored record, never the fresh patch.
        let late = u128::MAX;
        let (_, stored) = backend
            .patch_timeline(&key, vec![timeline_record(late, "late")], &[])
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

    /// A single timeline is bounded by both record count and aggregate
    /// record size. The newest PATCH is retained even when it is the only
    /// record large enough to exceed the byte budget.
    pub(crate) async fn timeline_patch_enforces_byte_budget(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let runner = backend
            .register_runner(register_runner("timeline-bytes"))
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
        let fill: Vec<_> = (1..=MAX_TIMELINE_RECORDS as u128)
            .map(|id| timeline_record(id, "small"))
            .collect();
        backend.patch_timeline(&key, fill, &[]).await.unwrap();

        let mut huge = timeline_record(u128::MAX, "huge");
        huge.current_operation =
            Some("x".repeat(crate::memory_caps::MAX_TIMELINE_BYTES_PER_TIMELINE + 1024));
        let (_, patched) = backend.patch_timeline(&key, vec![huge], &[]).await.unwrap();
        assert!(
            patched
                .iter()
                .any(|record| record.name.as_deref() == Some("huge")),
            "the fresh record must remain in the PATCH response"
        );
        assert_eq!(
            patched.len(),
            1,
            "oversized input is reduced to the protected fresh record"
        );
        let (_, fetched) = backend.get_timeline(&key, 0, usize::MAX).await.unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].name.as_deref(), Some("huge"));
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

        // A job with no `jobs` row yet — a matrix leg minted before its
        // expansion materializes — keeps its mapping in the run record, and
        // clearing it follows the same compare-and-clear rule.
        let leg = JobId("build (linux)".to_owned());
        assert!(backend.set_job_check_run(run_id, &leg, 13).await.unwrap());
        assert_eq!(
            backend.job_check_run_id(run_id, &leg).await.unwrap(),
            Some(13)
        );
        backend.clear_job_check_run(run_id, &leg, 13).await.unwrap();
        assert_eq!(backend.job_check_run_id(run_id, &leg).await.unwrap(), None);

        // An unknown run records nothing.
        assert!(
            !backend
                .set_job_check_run(RunId::new(), &leg, 14)
                .await
                .unwrap()
        );
    }

    /// Drain the projector until a short batch, returning rows consumed.
    async fn project_all_check_run_events(backend: &dyn ControlBackend, owner: &str) -> usize {
        let mut total = 0;
        for _ in 0..64 {
            let n = backend
                .consume_check_run_outbox(owner, std::time::Duration::from_secs(30), 256)
                .await
                .unwrap();
            total += n;
            if n < 256 {
                break;
            }
        }
        total
    }

    async fn lease_check_runs(
        backend: &dyn ControlBackend,
        owner: &str,
    ) -> Vec<crate::control::types::CheckRunUpdate> {
        backend
            .lease_check_run_updates(owner, std::time::Duration::from_secs(30), 32)
            .await
            .unwrap()
    }

    async fn outbox_len(backend: &dyn ControlBackend) -> usize {
        backend
            .outbox_read(
                OutboxBookmark {
                    txid: 0,
                    event_id: 0,
                },
                10_000,
            )
            .await
            .unwrap()
            .len()
    }

    /// Consume until the projector has nothing left to read.
    ///
    /// On a shared Postgres server a concurrent transaction (another test's
    /// database, same cluster) holds `pg_snapshot_xmin` back, hiding freshly
    /// committed rows for a moment; retry instead of assuming instant
    /// visibility.
    async fn consume_until_quiescent(backend: &dyn ControlBackend, owner: &str) -> usize {
        let mut total = 0;
        for _ in 0..600 {
            let n = backend
                .consume_check_run_outbox(owner, std::time::Duration::from_secs(30), 256)
                .await
                .unwrap();
            total += n;
            if n == 0 && total > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        total
    }

    /// Read the outbox length once two consecutive reads agree, so a delayed
    /// commit cannot make an assertion count a half-visible stream.
    async fn stable_outbox_len(backend: &dyn ControlBackend) -> usize {
        let mut last = outbox_len(backend).await;
        for _ in 0..200 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            let now = outbox_len(backend).await;
            if now == last {
                return now;
            }
            last = now;
        }
        last
    }

    /// Wait until the projector has materialized `status` for the job.
    async fn wait_for_projected_status(
        backend: &dyn ControlBackend,
        run_id: RunId,
        job_id: &JobId,
        status: &str,
    ) -> crate::control::types::CheckRunUpdate {
        for _ in 0..600 {
            let _ = consume_until_quiescent(backend, "suite-projector").await;
            let rows = lease_check_runs(backend, "suite-probe").await;
            if let Some(row) = rows
                .iter()
                .find(|row| row.run_id == run_id && row.job_id == *job_id)
                && row.payload["status"] == status
            {
                return row.clone();
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("check-run row for {run_id}/{job_id} never reached {status}");
    }

    /// The desired-state row is latest-version-wins: an older projected event
    /// (or a direct insert) never overwrites a newer row, so a late `queued`
    /// cannot regress a `success` already in the queue.
    pub(crate) async fn check_run_projection_never_regresses_a_newer_row(
        backend: &dyn ControlBackend,
    ) {
        let run_id = RunId::new();
        let build = JobId("build".to_owned());
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        backend.set_reports_check_runs(run_id, true).await.unwrap();

        // A newer desired state than any event this run can produce.
        backend
            .enqueue_check_run_update(CheckRunUpdateInput {
                run_id,
                job_id: build.clone(),
                installation_id: 0,
                check_run_id: None,
                version: 9_999,
                payload: serde_json::json!({
                    "repository": "owner/repo",
                    "sha": "abc123",
                    "job_id": "build",
                    "status": "success",
                    "name": "build",
                }),
            })
            .await
            .unwrap();
        // Direct insert of an older version is a no-op too.
        backend
            .enqueue_check_run_update(CheckRunUpdateInput {
                run_id,
                job_id: build.clone(),
                installation_id: 0,
                check_run_id: None,
                version: 3,
                payload: serde_json::json!({
                    "repository": "owner/repo",
                    "sha": "abc123",
                    "job_id": "build",
                    "status": "queued",
                    "name": "build",
                }),
            })
            .await
            .unwrap();

        // A real projection wake for the same job must not regress the row.
        backend
            .append_check_run_projection(run_id, Some(&build))
            .await
            .unwrap();
        project_all_check_run_events(backend, "suite-projector").await;

        let rows = lease_check_runs(backend, "suite-probe").await;
        let row = rows
            .iter()
            .find(|row| row.run_id == run_id && row.job_id == build)
            .expect("the queue holds a row for build");
        assert_eq!(
            row.version, 9_999,
            "an older projected version must not overwrite a newer row"
        );
        assert_eq!(row.payload["status"], "success");
    }

    /// The acquire transition must reach GitHub: claiming a job writes
    /// `job.started.v1` and moves the row to in_progress, the projector
    /// coalesces that into an in_progress queue row, and the sender then
    /// PATCHes it (exercised end-to-end by the github.rs sender tests).
    pub(crate) async fn acquire_projects_in_progress_check_run(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let build = JobId("build".to_owned());
        let runner = backend
            .register_runner(register_runner("check-run-r1"))
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
        backend.set_reports_check_runs(run_id, true).await.unwrap();
        backend
            .append_check_run_projection(run_id, Some(&build))
            .await
            .unwrap();
        let queued = wait_for_projected_status(backend, run_id, &build, "queued").await;
        let queued_version = queued.version;

        let poll = backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        assert!(
            matches!(poll, PollOutcome::Claimed(_)),
            "expected a claim, got {poll:?}"
        );
        let row = wait_for_projected_status(backend, run_id, &build, "in_progress").await;
        assert!(
            row.version > queued_version,
            "the acquire transition bumps the row version ({} vs {queued_version})",
            row.version
        );
    }

    /// The consumer bookmark is durable state, not process memory: a second
    /// consumer owner (a restarted process) resumes exactly where the first
    /// stopped, so events committed while it was down are delivered once.
    pub(crate) async fn check_run_bookmark_survives_consumer_restart(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        // Postgres reads only below the cluster-wide snapshot `xmin`, so an
        // open transaction in another test's database on the shared server
        // hides these freshly committed rows for a moment. Retry like the
        // consumer loops below instead of counting once.
        let mut total = 0;
        for _ in 0..600 {
            total = outbox_len(backend).await;
            if total > 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(total > 1, "a submit emits several outbox rows");

        // First consumer reads exactly one event and commits the bookmark.
        // Its lease is left expired so the restarted consumer can take over
        // without waiting out the production lease window. Retry: on a shared
        // Postgres cluster a concurrent transaction can hide the row briefly.
        let mut first = 0;
        for _ in 0..600 {
            first = backend
                .consume_check_run_outbox(
                    "suite-consumer-1",
                    std::time::Duration::from_millis(1),
                    1,
                )
                .await
                .unwrap();
            if first == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert_eq!(first, 1, "the first consumer reads one event");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        // Events committed while it was "down" (the second submit) plus the
        // remainder are delivered after the restart. Retry until the counts
        // settle: a concurrent transaction on a shared Postgres cluster can
        // delay visibility of freshly committed rows.
        let run_id2 = RunId::new();
        backend
            .submit_run(submit_run(run_id2, vec![submit_job(run_id2, "build", 1)]))
            .await
            .unwrap();
        let mut delivered = 0;
        for _ in 0..600 {
            delivered += backend
                .consume_check_run_outbox(
                    "suite-consumer-2",
                    std::time::Duration::from_secs(30),
                    256,
                )
                .await
                .unwrap();
            if delivered > 0 && first + delivered == stable_outbox_len(backend).await {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(
            delivered > 0,
            "events committed while the consumer was down are delivered after restart"
        );
        assert_eq!(
            first + delivered,
            stable_outbox_len(backend).await,
            "a restarted consumer must resume at the persisted bookmark, not replay the stream"
        );
    }

    /// Renewal keeps only the current owner's lease: a sender whose lease
    /// expired and was re-leased by another sender is told so (and so must
    /// not POST), and its renewal attempt does not steal the row back.
    pub(crate) async fn check_run_lease_renewal_respects_takeover(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let build = JobId("build".to_owned());
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        backend
            .enqueue_check_run_update(CheckRunUpdateInput {
                run_id,
                job_id: build.clone(),
                installation_id: 0,
                check_run_id: None,
                version: 1,
                payload: serde_json::json!({
                    "repository": "owner/repo",
                    "sha": "abc123",
                    "status": "queued",
                    "name": "build",
                }),
            })
            .await
            .unwrap();
        let holds = |rows: &[crate::control::types::CheckRunUpdate]| {
            rows.iter()
                .any(|row| row.run_id == run_id && row.job_id == build)
        };

        let first = backend
            .lease_check_run_updates("sender-a", std::time::Duration::from_millis(1), 100)
            .await
            .unwrap();
        assert!(holds(&first), "sender-a leases the row");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let second = backend
            .lease_check_run_updates("sender-b", std::time::Duration::from_secs(30), 100)
            .await
            .unwrap();
        assert!(holds(&second), "sender-b re-leases the expired row");

        let lease = std::time::Duration::from_secs(60);
        assert!(
            !backend
                .renew_check_run_update("sender-a", run_id, &build, lease)
                .await
                .unwrap(),
            "the superseded sender must learn it lost the row"
        );
        assert!(
            backend
                .renew_check_run_update("sender-b", run_id, &build, lease)
                .await
                .unwrap(),
            "the current owner renews"
        );
        let third = backend
            .lease_check_run_updates("sender-c", std::time::Duration::from_secs(30), 100)
            .await
            .unwrap();
        assert!(
            !holds(&third),
            "a renewed lease is not up for grabs, and the failed renewal took nothing"
        );
    }

    /// Outbox prune never passes the durable consumer bookmark, and still
    /// bounds growth by age when no consumer has registered.
    pub(crate) async fn prune_outbox_respects_slowest_consumer(backend: &dyn ControlBackend) {
        // No consumer: age alone bounds the stream.
        let run_id = RunId::new();
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let mut before = 0;
        for _ in 0..600 {
            before = stable_outbox_len(backend).await;
            if before > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(before > 0, "committed outbox rows become visible");
        let pruned = backend
            .prune_outbox(std::time::Duration::ZERO, 10_000)
            .await
            .unwrap();
        assert!(
            pruned >= 1,
            "with no durable consumer, age prunes the stream"
        );
        // Postgres hides rows above the cluster-wide snapshot `xmin`, so a
        // row the count missed can surface after the prune; rows never
        // vanish except by pruning. Hence `>=`: prune removed no more than it
        // reported.
        assert!(
            outbox_len(backend).await >= before - pruned as usize,
            "prune removes no more rows than it counted"
        );

        // A consumer's bookmark: rows at/below it are prunable, rows past it
        // are retained however old they are.
        let run_id = RunId::new();
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        let fast = consume_until_quiescent(backend, "prune-consumer-fast").await;
        assert!(fast > 0);
        // Rows past the bookmark must be visible before the prune, or the
        // "they survive" assertion below is vacuous: wait until the second
        // submit's rows are counted, not just the first run's.
        let before_second = stable_outbox_len(backend).await;
        let run_id2 = RunId::new();
        backend
            .submit_run(submit_run(run_id2, vec![submit_job(run_id2, "build", 1)]))
            .await
            .unwrap();
        let mut total = 0;
        for _ in 0..600 {
            total = stable_outbox_len(backend).await;
            if total > before_second {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(
            total > before_second,
            "the second submit's rows become visible"
        );
        let pruned = backend
            .prune_outbox(std::time::Duration::ZERO, 10_000)
            .await
            .unwrap();
        assert!(pruned >= 1, "rows at or below the bookmark are prunable");
        let remaining = outbox_len(backend).await;
        assert!(
            remaining >= total - pruned as usize,
            "prune removes no more rows than it counted"
        );
        assert!(
            remaining >= 1,
            "rows past the consumer bookmark must survive an age prune"
        );
    }

    /// `run_dispatch_info` flags the expandable placeholder nodes — a
    /// deferred-matrix parent and a reusable caller. They never dispatch:
    /// expansion replaces them with the legs that mint their own checks, so
    /// the intake loops skip a flagged row and no `queued` check run is
    /// minted for a node whose row is purged (GitHub has no delete API). A
    /// plain job and a materialized matrix leg stay unflagged.
    pub(crate) async fn dispatch_info_flags_expandable_placeholders(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        // Both placeholders wait on `gen`: the submit-time promotion sweep
        // must not expand them, so they stay placeholders as intake sees them.
        let mut fan = submit_job(run_id, "fan", 2);
        fan.queued.needs = vec![JobId("gen".to_owned())];
        fan.queued.deferred_matrix = Some("${{ fromJSON(needs.gen.outputs.m) }}".to_owned());
        let mut call = submit_job(run_id, "call", 3);
        call.queued.needs = vec![JobId("gen".to_owned())];
        call.queued.reusable_call = Some(preloop_gha_protocol::ReusableCallPlan {
            uses: "octo-org/octo-repo/.github/workflows/callee.yml@main".to_owned(),
            workflow_file: "octo-org/octo-repo/.github/workflows/callee.yml".to_owned(),
            workflow_sha: Some("abc123".to_owned()),
            workflow_repository: Some("octo-org/octo-repo".to_owned()),
            depth: 1,
        });
        // A static matrix leg is a real dispatchable job (`kind` `matrix_leg`).
        let mut leg = submit_job(run_id, "build (linux)", 4);
        leg.queued.base_id = "build".to_owned();
        backend
            .submit_run(submit_run(
                run_id,
                vec![submit_job(run_id, "gen", 1), fan, call, leg],
            ))
            .await
            .unwrap();

        let info = backend
            .run_dispatch_info(run_id)
            .await
            .unwrap()
            .expect("the submitted run exists");
        let placeholder = |job_id: &str| {
            info.jobs
                .iter()
                .find(|job| job.job_id.0 == job_id)
                .unwrap_or_else(|| panic!("job {job_id} missing from the run"))
                .placeholder
        };
        assert!(
            !placeholder("gen"),
            "a plain dispatchable job must not be flagged"
        );
        assert!(
            !placeholder("build (linux)"),
            "a materialized matrix leg is a real job, not a placeholder"
        );
        assert!(
            placeholder("fan"),
            "a deferred-matrix parent never dispatches: intake must skip its check"
        );
        assert!(
            placeholder("call"),
            "a reusable caller never dispatches: intake must skip its check"
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
        backend.submit_run(submit_a).await.unwrap();
        let stats = backend.queue_stats().await.unwrap();
        assert_eq!((stats.ready, stats.claimed), (1, 0));

        let run_b = RunId::new();
        let mut submit_b = submit_run(run_b, vec![submit_job(run_b, "build", 2)]);
        submit_b.workflow_concurrency = Some(workflow_concurrency("g", true));
        backend.submit_run(submit_b).await.unwrap();
        let stats = backend.queue_stats().await.unwrap();
        assert_eq!(
            (stats.ready, stats.claimed),
            (1, 0),
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
                    environment_url: None,
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
                    environment_url: None,
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

    /// The job a poll claimed, or `None` when it came back empty.
    async fn claimed_job(
        backend: &dyn ControlBackend,
        session_id: &str,
        runner_id: i64,
    ) -> Option<(RunId, JobId, uuid::Uuid)> {
        match backend
            .poll_session(poll(session_id, runner_id))
            .await
            .unwrap()
        {
            PollOutcome::Claimed(claimed) => Some((
                claimed.queued.run_id,
                claimed.queued.job_id.clone(),
                claimed.request.agent_job_id,
            )),
            PollOutcome::Empty => None,
            other => panic!("expected a claim or nothing, got {other:?}"),
        }
    }

    /// A runner with a live session: `(runner_id, session_id)`.
    async fn live_runner(backend: &dyn ControlBackend, name: &str) -> (i64, String) {
        let runner = backend
            .register_runner(register_runner(name))
            .await
            .unwrap();
        let session = backend
            .create_session(create_session(runner.runner.id))
            .await
            .unwrap();
        (runner.runner.id, session.session_id)
    }

    /// `submit_run` under an explicit namespace.
    fn submit_in(namespace: &str, run_id: RunId, jobs: Vec<SubmitJob>) -> SubmitRun {
        let mut submit = submit_run(run_id, jobs);
        submit.namespace = namespace.to_owned();
        submit
    }

    /// The namespace state gates both admission points. `suspended` starts
    /// nothing and takes no runs; `draining` finishes queued work but takes
    /// no runs; `active` does both. Other namespaces are never affected.
    /// No command writes namespace state (the platform does), so `exec` runs
    /// the per-backend SQL.
    pub(crate) async fn namespace_state_gates_submit_and_claim<F, Fut>(
        backend: &dyn ControlBackend,
        exec: F,
    ) where
        F: Fn(String) -> Fut,
        Fut: Future<Output = ()>,
    {
        let tenant = "tenant-state";
        let (runner, session) = live_runner(backend, "r1").await;
        let tenant_run = RunId::new();
        backend
            .submit_run(submit_in(
                tenant,
                tenant_run,
                vec![submit_job(tenant_run, "build", 1)],
            ))
            .await
            .unwrap();
        let set_state = |state: &str| {
            exec(format!(
                "UPDATE namespaces SET state = '{state}' WHERE namespace_id = '{tenant}'"
            ))
        };

        set_state("suspended").await;
        let other_run = RunId::new();
        backend
            .submit_run(submit_run(
                other_run,
                vec![submit_job(other_run, "build", 2)],
            ))
            .await
            .unwrap();
        let claimed = claimed_job(backend, &session, runner).await;
        assert_eq!(
            claimed.map(|(run_id, _, _)| run_id),
            Some(other_run),
            "a suspended namespace starts nothing; another namespace's job runs instead"
        );
        let (runner_2, session_2) = live_runner(backend, "r2").await;
        assert!(
            claimed_job(backend, &session_2, runner_2).await.is_none(),
            "the suspended namespace's job must stay queued"
        );
        let refused = RunId::new();
        assert!(
            matches!(
                backend
                    .submit_run(submit_in(
                        tenant,
                        refused,
                        vec![submit_job(refused, "b", 3)]
                    ))
                    .await,
                Err(ControlError::Forbidden(_))
            ),
            "a suspended namespace takes no new runs"
        );

        set_state("draining").await;
        let refused = RunId::new();
        assert!(
            matches!(
                backend
                    .submit_run(submit_in(
                        tenant,
                        refused,
                        vec![submit_job(refused, "b", 4)]
                    ))
                    .await,
                Err(ControlError::Forbidden(_))
            ),
            "a draining namespace takes no new runs"
        );
        assert_eq!(
            claimed_job(backend, &session_2, runner_2)
                .await
                .map(|(run_id, _, _)| run_id),
            Some(tenant_run),
            "a draining namespace still finishes its queued work"
        );

        set_state("active").await;
        let accepted = RunId::new();
        backend
            .submit_run(submit_in(
                tenant,
                accepted,
                vec![submit_job(accepted, "b", 5)],
            ))
            .await
            .unwrap();
    }

    /// `max_running_jobs` and a per-pool cap each hold a namespace's claims
    /// at its limit; a completion frees the slot; an uncapped namespace's
    /// jobs keep flowing while the capped one waits.
    pub(crate) async fn namespace_running_caps_hold_claims_at_limit<F, Fut>(
        backend: &dyn ControlBackend,
        exec: F,
    ) where
        F: Fn(String) -> Fut,
        Fut: Future<Output = ()>,
    {
        let tenant = "tenant-capped";
        let (runner_1, session_1) = live_runner(backend, "r1").await;
        let (runner_2, session_2) = live_runner(backend, "r2").await;
        let run_id = RunId::new();
        backend
            .submit_run(submit_in(
                tenant,
                run_id,
                vec![
                    submit_job(run_id, "a", 1),
                    submit_job(run_id, "b", 2),
                    submit_job(run_id, "c", 3),
                ],
            ))
            .await
            .unwrap();
        exec(format!(
            "INSERT INTO namespace_limits (namespace_id, max_running_jobs) VALUES ('{tenant}', 1)"
        ))
        .await;

        let (_, first_job, first_agent) = claimed_job(backend, &session_1, runner_1)
            .await
            .expect("the first job fits under the cap");
        assert!(
            claimed_job(backend, &session_2, runner_2).await.is_none(),
            "a second claim would exceed max_running_jobs = 1"
        );
        let other_run = RunId::new();
        backend
            .submit_run(submit_run(
                other_run,
                vec![submit_job(other_run, "build", 4)],
            ))
            .await
            .unwrap();
        assert_eq!(
            claimed_job(backend, &session_2, runner_2)
                .await
                .map(|(run_id, _, _)| run_id),
            Some(other_run),
            "another namespace's job is not held back by this namespace's cap"
        );

        backend
            .complete_job(crate::control::backend::JobCompletionInput {
                run_id,
                job_id: first_job,
                agent_job_id: Some(first_agent),
                status: ExecutionStatus::Success,
                outputs: BTreeMap::new(),
                runner_id: Some(runner_1),
            })
            .await
            .unwrap();
        let (runner_3, session_3) = live_runner(backend, "r3").await;
        assert_eq!(
            claimed_job(backend, &session_3, runner_3)
                .await
                .map(|(run_id, _, _)| run_id),
            Some(run_id),
            "a completion frees the namespace's slot"
        );

        // Lift the namespace-wide cap; a per-pool cap of one now holds the
        // last job while the slot above is still running.
        let pool_key = crate::control::types::compute_pool_key(&["self-hosted".to_owned()], None);
        exec(format!(
            "UPDATE namespace_limits SET max_running_jobs = NULL WHERE namespace_id = '{tenant}'"
        ))
        .await;
        exec(format!(
            "INSERT INTO namespace_pool_limits (namespace_id, pool_key, max_running_jobs) \
             VALUES ('{tenant}', '{pool_key}', 1)"
        ))
        .await;
        let (runner_4, session_4) = live_runner(backend, "r4").await;
        assert!(
            claimed_job(backend, &session_4, runner_4).await.is_none(),
            "the pool cap of one is already taken"
        );
        exec(format!(
            "DELETE FROM namespace_pool_limits WHERE namespace_id = '{tenant}'"
        ))
        .await;
        assert_eq!(
            claimed_job(backend, &session_4, runner_4)
                .await
                .map(|(run_id, _, _)| run_id),
            Some(run_id),
            "an uncapped namespace claims freely"
        );
    }

    /// `max_queued_jobs` admits a run only while the namespace's queued jobs
    /// plus the run's own fit; other namespaces are unaffected.
    pub(crate) async fn namespace_queue_cap_refuses_overflowing_submit<F, Fut>(
        backend: &dyn ControlBackend,
        exec: F,
    ) where
        F: Fn(String) -> Fut,
        Fut: Future<Output = ()>,
    {
        let tenant = "tenant-queue";
        let first = RunId::new();
        backend
            .submit_run(submit_in(tenant, first, vec![submit_job(first, "a", 1)]))
            .await
            .unwrap();
        exec(format!(
            "INSERT INTO namespace_limits (namespace_id, max_queued_jobs) VALUES ('{tenant}', 2)"
        ))
        .await;
        let second = RunId::new();
        backend
            .submit_run(submit_in(tenant, second, vec![submit_job(second, "a", 2)]))
            .await
            .expect("1 queued + 1 submitted fits max_queued_jobs = 2");
        let third = RunId::new();
        assert!(
            matches!(
                backend
                    .submit_run(submit_in(tenant, third, vec![submit_job(third, "a", 3)]))
                    .await,
                Err(ControlError::QuotaExceeded(_))
            ),
            "2 queued + 1 submitted exceeds max_queued_jobs = 2"
        );
        assert!(
            backend.run_record(third).await.is_err(),
            "a refused run persists nothing"
        );
        let other = RunId::new();
        backend
            .submit_run(submit_run(other, vec![submit_job(other, "a", 4)]))
            .await
            .expect("another namespace is not limited by this one");
    }

    /// `max_jobs_per_run` refuses an oversized run outright (it can never
    /// fit), and `submit_rate_per_minute` refuses runs past the namespace's
    /// trailing-minute budget; neither persists the refused run.
    pub(crate) async fn namespace_run_size_and_rate_limits<F, Fut>(
        backend: &dyn ControlBackend,
        exec: F,
    ) where
        F: Fn(String) -> Fut,
        Fut: Future<Output = ()>,
    {
        let tenant = "tenant-submit-limits";
        let seed = RunId::new();
        backend
            .submit_run(submit_in(tenant, seed, vec![submit_job(seed, "a", 1)]))
            .await
            .unwrap();
        exec(format!(
            "INSERT INTO namespace_limits (namespace_id, max_jobs_per_run, submit_rate_per_minute) \
             VALUES ('{tenant}', 2, 3)"
        ))
        .await;

        let oversized = RunId::new();
        let jobs = (0..3)
            .map(|index| submit_job(oversized, &format!("j{index}"), 10 + index))
            .collect();
        assert!(
            matches!(
                backend.submit_run(submit_in(tenant, oversized, jobs)).await,
                Err(ControlError::BadRequest(_))
            ),
            "3 jobs exceed max_jobs_per_run = 2"
        );
        assert!(backend.run_record(oversized).await.is_err());

        // One run already counts toward the minute; two more fit, a third
        // does not.
        for request_id in [20, 21] {
            let run_id = RunId::new();
            backend
                .submit_run(submit_in(
                    tenant,
                    run_id,
                    vec![submit_job(run_id, "a", request_id)],
                ))
                .await
                .expect("within submit_rate_per_minute = 3");
        }
        let throttled = RunId::new();
        assert!(
            matches!(
                backend
                    .submit_run(submit_in(
                        tenant,
                        throttled,
                        vec![submit_job(throttled, "a", 22)]
                    ))
                    .await,
                Err(ControlError::QuotaExceeded(_))
            ),
            "a fourth run in one minute exceeds submit_rate_per_minute = 3"
        );
        assert!(backend.run_record(throttled).await.is_err());
    }

    /// A webhook-originated run is never refused by a namespace limit or a
    /// suspension (GitHub does not redeliver, so a refusal loses the push):
    /// it is recorded and waits at claim. Only a deleted namespace refuses
    /// it. The same submits through the API are refused.
    pub(crate) async fn webhook_runs_bypass_submit_quotas<F, Fut>(
        backend: &dyn ControlBackend,
        exec: F,
    ) where
        F: Fn(String) -> Fut,
        Fut: Future<Output = ()>,
    {
        let tenant = "tenant-webhooks";
        let webhook = |run_id: RunId, request_id: i64| {
            let mut submit = submit_in(tenant, run_id, vec![submit_job(run_id, "a", request_id)]);
            submit.record.webhook_delivery_id = Some(format!("delivery-{run_id}"));
            submit
        };
        let seed = RunId::new();
        backend.submit_run(webhook(seed, 1)).await.unwrap();
        exec(format!(
            "INSERT INTO namespace_limits (namespace_id, max_queued_jobs, \
             submit_rate_per_minute, max_jobs_per_run, max_running_jobs) \
             VALUES ('{tenant}', 1, 1, 1, 0)"
        ))
        .await;

        let api = RunId::new();
        assert!(
            backend
                .submit_run(submit_in(tenant, api, vec![submit_job(api, "a", 2)]))
                .await
                .is_err(),
            "an API submit over the namespace's limits is refused"
        );
        let over_quota = RunId::new();
        backend
            .submit_run(webhook(over_quota, 3))
            .await
            .expect("a webhook run over every submit-time limit is still recorded");
        assert!(backend.run_record(over_quota).await.is_ok());
        let (runner, session) = live_runner(backend, "r1").await;
        assert!(
            claimed_job(backend, &session, runner).await.is_none(),
            "it waits at claim under max_running_jobs = 0"
        );

        exec(format!(
            "UPDATE namespaces SET state = 'suspended' WHERE namespace_id = '{tenant}'"
        ))
        .await;
        let suspended = RunId::new();
        backend
            .submit_run(webhook(suspended, 4))
            .await
            .expect("a suspended namespace still records webhook runs");

        exec(format!(
            "UPDATE namespaces SET state = 'deleted' WHERE namespace_id = '{tenant}'"
        ))
        .await;
        let deleted = RunId::new();
        assert!(
            matches!(
                backend.submit_run(webhook(deleted, 5)).await,
                Err(ControlError::Forbidden(_))
            ),
            "a deleted namespace refuses webhook runs"
        );
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
            pool_labels: Vec::new(),
            first_seen: BTreeMap::new(),
        }
    }

    /// The timeout arm and the lease arm of `reap_sweep` are independent
    /// (trait doc steps 2 and 3): an attempt that already hit its
    /// `timeout-minutes` and whose runner then went silent must still have
    /// its lease expired and settle as a failure on the next tick. The
    /// session is dead throughout: a live session with a stale lease is the
    /// hung-worker case, reaped on the worker's own cadence instead.
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
        // Keep the timeout below both lease windows so the first tick can
        // trigger cancellation without also settling the attempt.
        job.queued.message.job_timeout = Some(60);
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
                started + std::time::Duration::from_secs(61),
                run_id,
                inputs,
            ))
            .await
            .unwrap();
        assert_eq!(outcome.cancellations, 1, "the job timeout must fire");

        // Tick 2: the runner never renewed; the dead-session lease expires
        // after the dedicated ten-minute server-side window.
        let mut inputs = backend.reap_inputs().await.unwrap();
        silence_sessions(&mut inputs);
        let outcome = backend
            .reap_sweep(sweep_at(
                std::time::SystemTime::now()
                    + std::time::Duration::from_secs(
                        crate::distributed_task::DEAD_SESSION_LEASE_SECONDS + 3600,
                    ),
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

    /// Model runners whose sessions stopped polling.
    fn silence_sessions(inputs: &mut ReapInputs) {
        for active in &mut inputs.active {
            active.session_live = false;
        }
    }

    /// A worker that stops renewing while its session keeps polling is hung,
    /// not disconnected: its attempt is reaped at the worker's own cadence
    /// (`HUNG_WORKER_LEASE_SECONDS`) instead of holding the slot for the full
    /// lease. A silent session keeps the full lease.
    pub(crate) async fn live_session_with_stale_lease_is_reaped_as_hung(
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
        let claimed = match backend
            .poll_session(poll(&session.session_id, runner.runner.id))
            .await
            .unwrap()
        {
            PollOutcome::Claimed(claimed) => claimed,
            other => panic!("expected a claim, got {other:?}"),
        };
        let inputs = backend.reap_inputs().await.unwrap();
        let active = inputs
            .active
            .iter()
            .find(|active| active.request_id == claimed.request.request_id)
            .expect("the claim is an active attempt");
        assert!(active.session_live, "a polling session is live");
        let renewed = active.last_renewed_at.expect("the claim stamps the lease");
        let stale = renewed
            + std::time::Duration::from_secs(crate::distributed_task::HUNG_WORKER_LEASE_SECONDS);

        let mut silent = backend.reap_inputs().await.unwrap();
        silence_sessions(&mut silent);
        let outcome = backend
            .reap_sweep(sweep_at(stale, run_id, silent))
            .await
            .unwrap();
        assert!(
            outcome.expired.is_empty(),
            "a disconnected runner keeps the full lease"
        );

        let outcome = backend
            .reap_sweep(sweep_at(
                stale,
                run_id,
                backend.reap_inputs().await.unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(
            outcome
                .expired
                .iter()
                .map(|lease| lease.request_id)
                .collect::<Vec<_>>(),
            vec![claimed.request.request_id],
            "a live session with a stale lease is a hung worker"
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

    /// Closing a runner's session releases the attempt's session binding but
    /// keeps its recorded owner: the runner that claimed the attempt may
    /// still renew and settle it, while no other runner may take it over
    /// (legacy AgentRequest ownership, pinned at the HTTP level by
    /// `legacy_agent_requests_are_bound_to_runner_identity`).
    pub(crate) async fn close_session_keeps_request_owner(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let owner = backend
            .register_runner(register_runner("session-owner"))
            .await
            .unwrap();
        let other = backend
            .register_runner(register_runner("session-other"))
            .await
            .unwrap();
        let session = backend
            .create_session(create_session(owner.runner.id))
            .await
            .unwrap();
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        let outcome = backend
            .poll_session(poll(&session.session_id, owner.runner.id))
            .await
            .unwrap();
        let PollOutcome::Claimed(claimed) = outcome else {
            panic!("expected a claim, got {outcome:?}");
        };
        let request_id = claimed.request.request_id;

        // Listener recovery deletes the session while the attempt is live.
        assert!(
            backend
                .close_runner_session(&session.session_id, Some(owner.runner.id))
                .await
                .unwrap()
        );
        assert_eq!(
            backend.request_owner(request_id).await.unwrap(),
            Some((Some(owner.runner.id), None, false)),
            "the attempt's owner survives session teardown"
        );
        // The owner keeps its attempt: renewal works and nobody else may
        // acquire it.
        backend
            .renew_request(request_id, owner.runner.id)
            .await
            .unwrap();
        assert!(matches!(
            backend
                .acquire_for_runner(request_id, other.runner.id)
                .await,
            Err(ControlError::Forbidden(_))
        ));
    }

    /// A session teardown leaves the attempt's start stamp and lease row in
    /// place: the trait doc hands its request and claimed job to the lease
    /// reaper, and the reaper reads both of them from those columns.
    pub(crate) async fn closed_session_attempt_stays_reapable(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let runner = backend
            .register_runner(register_runner("session-reapable"))
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
            backend
                .close_runner_session(&session.session_id, Some(runner.runner.id))
                .await
                .unwrap()
        );

        let inputs = backend.reap_inputs().await.unwrap();
        let active = inputs
            .active
            .iter()
            .find(|active| active.request_id == request_id)
            .expect("the orphaned attempt must stay in the reaper's input");
        assert!(
            active.started_at.is_some(),
            "session teardown must keep the attempt's start stamp"
        );
        assert!(
            active.last_renewed_at.is_some(),
            "session teardown must keep the attempt's lease"
        );

        // The lease arm of the sweep must be able to fail the attempt.
        let now = std::time::SystemTime::now()
            + std::time::Duration::from_secs(
                crate::distributed_task::DEAD_SESSION_LEASE_SECONDS + 3600,
            );
        let outcome = backend
            .reap_sweep(ReapSweep {
                now,
                runs: std::iter::once(run_id).collect(),
                ready: inputs.ready,
                active: inputs.active,
                paused: Default::default(),
                pool_preparing: false,
                warm_window_open: false,
                pool_labels: Vec::new(),
                first_seen: Default::default(),
            })
            .await
            .unwrap();
        assert!(
            outcome
                .expired
                .iter()
                .any(|lease| lease.request_id == request_id),
            "the reaper must be able to expire an attempt orphaned by session teardown"
        );
    }

    /// Lease expiry is driven by the active-request snapshot, not by the
    /// caller's due-run optimization. A missed due-run mark must not leave an
    /// otherwise expired attempt in progress forever.
    pub(crate) async fn expired_active_attempt_is_not_scoped_to_due_runs(
        backend: &dyn ControlBackend,
    ) {
        let run_id = RunId::new();
        let runner = backend
            .register_runner(register_runner("unscoped-lease"))
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
        let inputs = backend.reap_inputs().await.unwrap();
        let request_id = claimed.request.request_id;
        let outcome = backend
            .reap_sweep(ReapSweep {
                now: std::time::SystemTime::now()
                    + std::time::Duration::from_secs(
                        crate::distributed_task::DEAD_SESSION_LEASE_SECONDS + 3600,
                    ),
                runs: Default::default(),
                ready: inputs.ready,
                active: inputs.active,
                paused: Default::default(),
                pool_preparing: false,
                warm_window_open: false,
                pool_labels: Vec::new(),
                first_seen: Default::default(),
            })
            .await
            .unwrap();
        assert!(
            outcome
                .expired
                .iter()
                .any(|lease| lease.request_id == request_id),
            "an expired active attempt must be settled even when due_runs is empty"
        );
    }

    /// An assigned-but-unowned request still acquires (session-less replay):
    /// the ownership ladder keys on the claiming session row, not on a
    /// recorded runner id.
    pub(crate) async fn unowned_session_claim_still_acquires(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let runner = backend
            .register_runner(register_runner("acquire-unowned"))
            .await
            .unwrap();
        // A compat session owns no runner, so the claim records no owner at
        // all — only the session row binds the attempt.
        let session = backend
            .create_session(CreateSession {
                runner_id: runner.runner.id,
                protocol: SessionProtocol::Compat,
                client_id: None,
            })
            .await
            .unwrap();
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        let outcome = backend
            .poll_session(poll_unverified(&session.session_id))
            .await
            .unwrap();
        let PollOutcome::Claimed(claimed) = outcome else {
            panic!("expected a claim, got {outcome:?}");
        };
        let request_id = claimed.request.request_id;
        assert_eq!(
            backend.request_owner(request_id).await.unwrap(),
            Some((None, None, true))
        );
        let context = backend
            .acquire_for_runner(request_id, runner.runner.id)
            .await
            .unwrap();
        assert_eq!(context.request.request_id, request_id);
    }

    /// `lookup_agent` follows the trait doc: the lowest-id runner named
    /// `name`, synthesizing `<id:08x>-0000-4000-8000-000000000000` and
    /// recording it when the runner has no client id.
    pub(crate) async fn lookup_agent_uses_lowest_id_and_records_client(
        backend: &dyn ControlBackend,
    ) {
        let mut first = register_runner("lookup-agent");
        first.client_id = None;
        let low = backend.register_runner(first).await.unwrap();
        let mut second = register_runner("lookup-agent");
        second.client_id = None;
        let high = backend.register_runner(second).await.unwrap();
        assert!(low.runner.id < high.runner.id);

        let (runner, client_id) = backend
            .lookup_agent("lookup-agent")
            .await
            .unwrap()
            .expect("a runner is registered under that name");
        assert_eq!(runner.id, low.runner.id, "the lowest-id runner wins");
        let synthesized = format!("{:08x}-0000-4000-8000-000000000000", low.runner.id as u32);
        assert_eq!(client_id, synthesized);
        assert_eq!(
            backend.runner_for_client(&client_id).await.unwrap(),
            Some(low.runner.id),
            "the synthesized client id must be recorded"
        );
        let (again, client_again) = backend.lookup_agent("lookup-agent").await.unwrap().unwrap();
        assert_eq!((again.id, client_again), (low.runner.id, synthesized));
    }

    /// A session row cannot outlive its runner registration: the liveness
    /// check and the insert share one transaction, so a purge racing token
    /// validation cannot leave a session behind (the broker route maps the
    /// `Forbidden` to 401).
    pub(crate) async fn broker_session_requires_a_live_runner(backend: &dyn ControlBackend) {
        assert!(matches!(
            backend
                .create_broker_session("session-unregistered", 999_999)
                .await,
            Err(ControlError::Forbidden(_))
        ));
        let runner = backend
            .register_runner(register_runner("broker-session"))
            .await
            .unwrap();
        backend
            .create_broker_session("session-live", runner.runner.id)
            .await
            .unwrap();
        // A compat session (`runner_id: None`) writes nothing and succeeds.
        backend
            .open_runner_session(OpenRunnerSession {
                session_id: "session-compat".to_owned(),
                runner_id: None,
                protocol: SessionProtocol::Broker,
                verified: false,
                require_live_runner: false,
            })
            .await
            .unwrap();
    }

    /// `issue_debug_token` implements the trait doc's gates: `NotFound` when
    /// no in-flight request owns the attempt, `Forbidden` without the run's
    /// preserve-on-failure opt-in, `Conflict` on a second issue, and the
    /// attempt's plan id on success.
    pub(crate) async fn debug_token_gates_follow_the_contract(backend: &dyn ControlBackend) {
        // No opt-in: the credential does not exist for this run.
        let plain = RunId::new();
        let submit = submit_run(plain, vec![submit_job(plain, "build", 1)]);
        let agent = submit.jobs[0].request.as_ref().unwrap().agent_job_id;
        backend.submit_run(submit).await.unwrap();
        assert!(matches!(
            backend.issue_debug_token(agent).await,
            Err(ControlError::Forbidden(_))
        ));

        // Unknown attempt.
        assert!(matches!(
            backend.issue_debug_token(uuid::Uuid::new_v4()).await,
            Err(ControlError::NotFound(_))
        ));

        // Opted-in run: the first issue succeeds and returns the plan id;
        // the second one conflicts.
        let opted_in = RunId::new();
        let mut submit = submit_run(opted_in, vec![submit_job(opted_in, "build", 1)]);
        let mut submission = (*submit.record.submission).clone();
        submission.preserve_on_failure = true;
        submit.record.submission = Arc::new(submission);
        let agent = submit.jobs[0].request.as_ref().unwrap().agent_job_id;
        backend.submit_run(submit).await.unwrap();
        let (run_id, plan_id) = backend.issue_debug_token(agent).await.unwrap();
        assert_eq!(run_id, opted_in);
        assert_eq!(plan_id, agent.to_string());
        assert!(matches!(
            backend.issue_debug_token(agent).await,
            Err(ControlError::Conflict(_))
        ));
    }

    /// The implicit compat session (`sessionId=default`, any non-UUID id) is
    /// materialized on demand and served: a legacy client polls it before
    /// any session row exists and must still be handed work.
    pub(crate) async fn azdo_compat_session_is_served(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        let outcome = backend
            .poll_azdo_session(AzdoPoll {
                session_id: "default".to_owned(),
                verified_runner_id: None,
            })
            .await
            .unwrap();
        assert!(
            matches!(outcome, AzdoPollOutcome::Claimed { .. }),
            "the compat poll must be served, got {outcome:?}"
        );
    }

    /// `renew_agent_request` renews an in-flight attempt through the
    /// ownership ladder (recorded owner, else the claiming session's runner)
    /// and reports `false` when no lease holder exists at all.
    pub(crate) async fn renew_agent_request_follows_the_lease_ladder(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let runner = backend
            .register_runner(register_runner("renew-ladder"))
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
            .poll_session(poll_unverified(&session.session_id))
            .await
            .unwrap();
        let PollOutcome::Claimed(claimed) = outcome else {
            panic!("expected a claim, got {outcome:?}");
        };
        let request_id = claimed.request.request_id;
        let locked_until = crate::distributed_task::agent_request_locked_until();

        // The claim recorded the session's runner as the lease holder, so
        // the renewal lands and the reaper can see it.
        assert!(
            backend
                .renew_agent_request(request_id, &locked_until)
                .await
                .unwrap()
        );
        // A released attempt has no lease holder at all: nothing is renewed,
        // and a backend must not report a renewal it did not write.
        assert!(
            backend
                .release_claimed_request(request_id, &locked_until)
                .await
                .unwrap()
        );
        assert!(
            !backend
                .renew_agent_request(request_id, &locked_until)
                .await
                .unwrap()
        );
    }

    /// A `max-parallel: 3` cohort whose three legs become runnable in the
    /// same promotion pass admits all three at once: each admitted leg
    /// counts toward the cap exactly once.
    pub(crate) async fn max_parallel_admits_the_full_cap_in_one_pass(backend: &dyn ControlBackend) {
        let run_id = RunId::new();
        let mut jobs = vec![submit_job(run_id, "setup", 1)];
        for (index, leg) in ["build (1)", "build (2)", "build (3)"]
            .into_iter()
            .enumerate()
        {
            let mut job = submit_job(run_id, leg, 2 + index as i64);
            job.queued.base_id = "build".to_owned();
            job.queued.max_parallel = Some(3);
            job.queued.needs = vec![JobId("setup".to_owned())];
            job.queued.dependencies_ready_at_unix_nanos = None;
            jobs.push(job);
        }
        backend.submit_run(submit_run(run_id, jobs)).await.unwrap();
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
        let PollOutcome::Claimed(claimed) = outcome else {
            panic!("setup must be claimable, got {outcome:?}");
        };
        assert_eq!(claimed.queued.job_id, JobId("setup".to_owned()));
        backend
            .complete_job(crate::control::backend::JobCompletionInput {
                run_id,
                job_id: JobId("setup".to_owned()),
                agent_job_id: Some(claimed.request.agent_job_id),
                status: ExecutionStatus::Success,
                outputs: BTreeMap::new(),
                runner_id: Some(runner.runner.id),
            })
            .await
            .unwrap();
        assert_eq!(
            backend.queue_stats().await.unwrap().ready,
            3,
            "all three legs fit under max-parallel 3 and are admitted in one pass"
        );
    }

    /// Submit two runs into one cancel-in-progress group where the second
    /// arrival carries the older event, so it is cancelled on arrival.
    /// Returns the cancelled run's id.
    pub(crate) async fn submit_arrival_cancelled_run(backend: &dyn ControlBackend) -> RunId {
        let newer = RunId::new();
        let mut newer_submit = submit_run(newer, vec![submit_job(newer, "deploy", 1)]);
        newer_submit.workflow_concurrency = Some(workflow_concurrency("g", true));
        Arc::make_mut(&mut newer_submit.record.submission).payload =
            serde_json::json!({"repository": {"pushed_at": 2000}});
        backend.submit_run(newer_submit).await.unwrap();

        let older = RunId::new();
        let mut older_submit = submit_run(older, vec![submit_job(older, "deploy", 2)]);
        older_submit.workflow_concurrency = Some(workflow_concurrency("g", true));
        Arc::make_mut(&mut older_submit.record.submission).payload =
            serde_json::json!({"repository": {"pushed_at": 1000}});
        let outcome = backend.submit_run(older_submit).await.unwrap();
        assert_eq!(
            outcome.rejected,
            Some(ExecutionStatus::Cancelled),
            "the older arrival must be superseded"
        );
        older
    }

    /// The run's durable events hold exactly one `run.created.v1` and one
    /// `run.completed.v1`.
    pub(crate) fn assert_created_and_completed_once(
        topics: &[(Option<RunId>, String)],
        run: RunId,
    ) {
        let count = |topic: &str| {
            topics
                .iter()
                .filter(|(owner, t)| *owner == Some(run) && t == topic)
                .count()
        };
        assert_eq!(count("run.created.v1"), 1, "topics: {topics:?}");
        assert_eq!(count("run.completed.v1"), 1, "topics: {topics:?}");
    }

    /// Submitting the same natural run number twice reallocates the second
    /// run above both the collision and the counter; both backends expose
    /// the same number contract.
    pub(crate) async fn duplicate_run_number_is_reallocated(backend: &dyn ControlBackend) {
        let first = RunId::new();
        let mut a = submit_run(first, vec![submit_job(first, "build", 1)]);
        a.record.run_number = 900_000;
        let outcome_a = backend.submit_run(a).await.unwrap();
        assert_eq!(outcome_a.run_number, 900_000);

        let second = RunId::new();
        let mut b = submit_run(second, vec![submit_job(second, "build", 2)]);
        b.record.run_number = 900_000;
        let outcome_b = backend.submit_run(b).await.unwrap();
        assert!(
            outcome_b.run_number > outcome_a.run_number,
            "the duplicate number is reallocated: {outcome_b:?}"
        );
        assert_eq!(
            backend.run_record(second).await.unwrap().run_number,
            outcome_b.run_number
        );
    }

    /// Ids and instants have one canonical stored form on both backends: run
    /// uuids lowercase (RFC 4122 text form), instants whole microseconds
    /// since the Unix epoch. A sub-microsecond input is truncated toward the
    /// epoch (`as_micros` / `timestamp_micros`), never rounded; nanosecond
    /// `bigint` stamps keep full precision.
    ///
    /// The provenance of the run's `created_at` still differs and is recorded
    /// here instead of hidden: SQLite stores the submitted value (truncated
    /// to µs), Postgres takes the database clock (`DEFAULT now()`). Only the
    /// precision rule and the nanosecond column are asserted identical.
    pub(crate) async fn ids_and_timestamps_are_stored_canonically(backend: &dyn ControlBackend) {
        let nanos = 1_700_000_000_123_456_789_i64;
        let run_id = RunId(uuid::Uuid::parse_str("0F8FAD5B-D9CB-469F-A165-70867728950E").unwrap());
        let mut record = super::run_record(run_id);
        // Upper-case spelling, deliberately: the stored form is the canonical
        // lowercase one on both backends.
        record.created_at = chrono::DateTime::from_timestamp_nanos(nanos);
        record.fork_approval_requested_at_unix_nanos = Some(nanos);
        record
            .job_names
            .insert(JobId("build".to_owned()), "build".to_owned());
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

        let loaded = backend.run_record(run_id).await.unwrap();
        assert_eq!(
            loaded.run_id.0.to_string(),
            "0f8fad5b-d9cb-469f-a165-70867728950e",
            "a run id must round-trip in the lowercase canonical form"
        );
        assert_eq!(
            loaded.job_names.get(&JobId("build".to_owned())),
            Some(&"build".to_owned()),
            "the job id round-trips verbatim: {:?}",
            loaded.job_names
        );
        let subsec = loaded.created_at.timestamp_subsec_nanos();
        assert_eq!(
            subsec % 1000,
            0,
            "microsecond columns must not keep sub-microsecond digits ({subsec} ns)"
        );
        assert_eq!(
            loaded.created_at,
            chrono::DateTime::from_timestamp_micros(loaded.created_at.timestamp_micros()).unwrap(),
            "the stored instant must be its own microsecond truncation"
        );
        assert_eq!(
            loaded.fork_approval_requested_at_unix_nanos,
            Some(nanos),
            "nanosecond-typed stamps must keep full precision"
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
    async fn status_events_are_stamped_with_row_versions() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::status_events_are_stamped_with_row_versions(&backend).await;
        suite::assert_status_stamps(&backend.test_working_set().await.unwrap().outbox_stamps);
    }

    #[tokio::test]
    async fn prune_outbox_keeps_fresh_rows_and_bounds_a_batch() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::prune_outbox_keeps_fresh_rows_and_bounds_a_batch(&backend).await;
    }

    #[tokio::test]
    async fn check_run_projection_never_regresses_a_newer_row() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::check_run_projection_never_regresses_a_newer_row(&backend).await;
    }

    #[tokio::test]
    async fn acquire_projects_in_progress_check_run() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::acquire_projects_in_progress_check_run(&backend).await;
    }

    #[tokio::test]
    async fn check_run_bookmark_survives_consumer_restart() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::check_run_bookmark_survives_consumer_restart(&backend).await;
    }

    #[tokio::test]
    async fn check_run_lease_renewal_respects_takeover() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::check_run_lease_renewal_respects_takeover(&backend).await;
    }

    #[tokio::test]
    async fn prune_outbox_respects_slowest_consumer() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::prune_outbox_respects_slowest_consumer(&backend).await;
    }

    /// The consumer bookmark is row state in `consumer_offsets`, so a second
    /// connection (another node, or the process after a restart) resumes at
    /// it instead of replaying the stream.
    #[tokio::test]
    async fn check_run_bookmark_survives_a_second_connection() {
        let Some((_pg, first, second)) = backend_pair().await else {
            return skip_no_postgres();
        };
        let run_id = RunId::new();
        first
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        let mut consumed = 0;
        for _ in 0..600 {
            consumed = first
                .consume_check_run_outbox("pg-consumer-1", std::time::Duration::from_millis(1), 256)
                .await
                .unwrap();
            if consumed > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(consumed > 0, "the first connection reads the run's events");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let run_id2 = RunId::new();
        second
            .submit_run(submit_run(run_id2, vec![submit_job(run_id2, "build", 1)]))
            .await
            .unwrap();
        // Retry until the counts settle: a concurrent transaction on the
        // shared test cluster can hold `pg_snapshot_xmin` back.
        let mut delivered = 0;
        for _ in 0..600 {
            delivered += second
                .consume_check_run_outbox("pg-consumer-2", std::time::Duration::from_secs(30), 256)
                .await
                .unwrap();
            let total = second
                .outbox_read(
                    OutboxBookmark {
                        txid: 0,
                        event_id: 0,
                    },
                    10_000,
                )
                .await
                .unwrap()
                .len();
            if delivered > 0 && consumed + delivered == total {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(delivered > 0, "the down-window events are delivered");
        let total = second
            .outbox_read(
                OutboxBookmark {
                    txid: 0,
                    event_id: 0,
                },
                10_000,
            )
            .await
            .unwrap()
            .len();
        assert_eq!(
            consumed + delivered,
            total,
            "a second connection must resume at the persisted bookmark (no replay, no skip)"
        );
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
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
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
    async fn failed_deferred_expansion_settles_the_node_once() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::failed_deferred_expansion_settles_the_node_once(&backend).await;
    }

    #[tokio::test]
    async fn deferred_matrix_expansion_scopes_its_inputs() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::deferred_matrix_expansion_scopes_its_inputs(&backend).await;
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
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::environment_gate_round_trips(&backend).await;
    }

    #[tokio::test]
    async fn fork_hold_parks_until_released() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::fork_hold_parks_until_released(&backend).await;
    }

    #[tokio::test]
    async fn expired_fork_hold_fails_closed() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::expired_fork_hold_fails_closed(&backend).await;
    }

    #[tokio::test]
    async fn environment_gate_parks_until_satisfied() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::environment_gate_parks_until_satisfied(&backend).await;
    }

    #[tokio::test]
    async fn denied_environment_gate_at_promotion_fails_the_job_once() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::denied_environment_gate_at_promotion_fails_the_job_once(&backend).await;
    }

    #[tokio::test]
    async fn unhostable_runs_on_at_promotion_fails_the_job_once() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::unhostable_runs_on_at_promotion_fails_the_job_once(&backend).await;
    }

    #[tokio::test]
    async fn unknown_environment_runs_without_a_gate() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::unknown_environment_runs_without_a_gate(&backend).await;
    }

    #[tokio::test]
    async fn reviewer_gate_holds_until_decision() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::reviewer_gate_holds_until_decision(&backend).await;
    }

    #[tokio::test]
    async fn unresolved_environment_rules_hold_the_job() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::unresolved_environment_rules_hold_the_job(&backend).await;
    }

    #[tokio::test]
    async fn reported_environment_url_lands_on_the_deployment_row() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::reported_environment_url_lands_on_the_deployment_row(&backend).await;
    }

    #[tokio::test]
    async fn deferred_environment_name_resolves_on_the_deployment_row() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::deferred_environment_name_resolves_on_the_deployment_row(&backend).await;
    }

    #[tokio::test]
    async fn deferred_environment_name_releases_after_needs() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::deferred_environment_name_releases_after_needs(&backend).await;
    }

    #[tokio::test]
    async fn environment_approval_audit_survives_archival() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        let db = &backend;
        suite::environment_approval_audit_survives_archival(&backend, |run_id| async move {
            db.test_execute(&format!(
                "UPDATE runs SET completed_at = now() - interval '10 minutes' \
                 WHERE run_id = '{run_id}'::uuid"
            ))
            .await
            .unwrap()
        })
        .await;
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
    async fn timeline_patch_enforces_byte_budget() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::timeline_patch_enforces_byte_budget(&backend).await;
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
    async fn dispatch_info_flags_expandable_placeholders() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::dispatch_info_flags_expandable_placeholders(&backend).await;
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
    async fn live_session_with_stale_lease_is_reaped_as_hung() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::live_session_with_stale_lease_is_reaped_as_hung(&backend).await;
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

    /// `record_environment_approval` takes the run lock before it reads the
    /// gate, so it serializes with the promotion sweep (which holds the same
    /// lock across load + flush). An approval racing a sweep that already
    /// concluded the job must observe the settled row — `AlreadyTerminal`,
    /// no audit row, gate untouched — instead of committing a decision built
    /// from a stale `pending` snapshot.
    #[tokio::test]
    async fn environment_approval_serializes_with_the_promotion_sweep() {
        use crate::control::types::{
            EnvironmentApproval, EnvironmentApprovalResult, EnvironmentDecision,
        };
        let Some((_pg, url)) = fresh_database_opt().await else {
            return skip_no_postgres();
        };
        let backend = std::sync::Arc::new(connect(&url).await);
        let run_id = RunId::new();
        let job_id = JobId("deploy".to_owned());
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "deploy", 1)]))
            .await
            .unwrap();
        backend
            .set_environment_gate(
                run_id,
                &job_id,
                Some(crate::models::EnvironmentGateState {
                    environment_name: Some("prod".to_owned()),
                    approval_requested_at_unix_nanos: Some(crate::models::now_unix_nanos()),
                    approvals_required: Some(1),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        // The sweep's shape: the run row locked while the job is concluded
        // failure. The lock stays open so the approval below has to wait it
        // out rather than read the stale row. The backend's pooled
        // connections resolve unqualified names through `search_path` set at
        // connect time; this bare connection has to ask for the same.
        let (mut client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("second connection");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute("SET search_path TO control")
            .await
            .expect("control search path");
        let tx = client.transaction().await.unwrap();
        tx.query_one(
            "SELECT 1 FROM runs WHERE run_id = $1::text::uuid FOR NO KEY UPDATE",
            &[&run_id.0.to_string()],
        )
        .await
        .unwrap();
        tx.execute(
            "UPDATE jobs SET status = 'failure', queue_state = 'none' \
             WHERE run_id = $1::text::uuid AND job_id = $2",
            &[&run_id.0.to_string(), &job_id.0],
        )
        .await
        .unwrap();

        let racer = {
            let backend = std::sync::Arc::clone(&backend);
            let job_id = job_id.clone();
            tokio::spawn(async move {
                backend
                    .record_environment_approval(EnvironmentApproval {
                        run_id,
                        job_id,
                        decision: EnvironmentDecision::Approve,
                        actor: Some("octocat".to_owned()),
                        admin_override: false,
                        note: Some("ship it".to_owned()),
                    })
                    .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(
            !racer.is_finished(),
            "the approval waits for the sweep's run lock"
        );
        tx.commit().await.unwrap();
        let outcome = racer.await.unwrap().unwrap();
        assert!(
            matches!(outcome.result, EnvironmentApprovalResult::AlreadyTerminal),
            "the approval observes the sweep's settled row, got {:?}",
            outcome.result
        );
        assert!(
            backend
                .environment_approvals(run_id, &job_id)
                .await
                .unwrap()
                .is_empty(),
            "no decision is recorded against a job the sweep already failed"
        );
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
    async fn namespace_state_gates_submit_and_claim() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        let db = &backend;
        suite::namespace_state_gates_submit_and_claim(&backend, |sql| async move {
            db.test_execute(&sql).await.unwrap()
        })
        .await;
    }

    #[tokio::test]
    async fn namespace_running_caps_hold_claims_at_limit() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        let db = &backend;
        suite::namespace_running_caps_hold_claims_at_limit(&backend, |sql| async move {
            db.test_execute(&sql).await.unwrap()
        })
        .await;
    }

    #[tokio::test]
    async fn namespace_queue_cap_refuses_overflowing_submit() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        let db = &backend;
        suite::namespace_queue_cap_refuses_overflowing_submit(&backend, |sql| async move {
            db.test_execute(&sql).await.unwrap()
        })
        .await;
    }

    #[tokio::test]
    async fn namespace_run_size_and_rate_limits() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        let db = &backend;
        suite::namespace_run_size_and_rate_limits(&backend, |sql| async move {
            db.test_execute(&sql).await.unwrap()
        })
        .await;
    }

    #[tokio::test]
    async fn webhook_runs_bypass_submit_quotas() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        let db = &backend;
        suite::webhook_runs_bypass_submit_quotas(&backend, |sql| async move {
            db.test_execute(&sql).await.unwrap()
        })
        .await;
    }

    /// Two nodes polling at once must not both take the last slot under a
    /// namespace's running cap: the claim serializes on the limit row.
    #[tokio::test]
    async fn concurrent_claims_respect_namespace_running_cap() {
        let Some((_pg, node_a, node_b)) = backend_pair().await else {
            return skip_no_postgres();
        };
        let tenant = "tenant-race";
        let run_id = RunId::new();
        let jobs = (0..6)
            .map(|index| submit_job(run_id, &format!("job-{index}"), index + 1))
            .collect();
        let mut submit = submit_run(run_id, jobs);
        submit.namespace = tenant.to_owned();
        node_a.submit_run(submit).await.unwrap();
        node_a
            .test_execute(&format!(
                "INSERT INTO namespace_limits (namespace_id, max_running_jobs) \
                 VALUES ('{tenant}', 2)"
            ))
            .await
            .unwrap();
        let mut sessions = Vec::new();
        for index in 0..6 {
            let node = if index % 2 == 0 { &node_a } else { &node_b };
            let runner = node
                .register_runner(register_runner(&format!("race-{index}")))
                .await
                .unwrap();
            let session = node
                .create_session(create_session(runner.runner.id))
                .await
                .unwrap();
            sessions.push((index, runner.runner.id, session.session_id));
        }
        let polls = sessions.iter().map(|(index, runner_id, session_id)| {
            let node = if index % 2 == 0 { &node_a } else { &node_b };
            node.poll_session(poll(session_id, *runner_id))
        });
        let claimed = futures::future::join_all(polls)
            .await
            .into_iter()
            .map(Result::unwrap)
            .filter(|outcome| matches!(outcome, PollOutcome::Claimed(_)))
            .count();
        assert_eq!(claimed, 2, "max_running_jobs = 2 across both nodes");
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

    #[tokio::test]
    async fn close_session_keeps_request_owner() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::close_session_keeps_request_owner(&backend).await;
    }

    #[tokio::test]
    async fn closed_session_attempt_stays_reapable() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::closed_session_attempt_stays_reapable(&backend).await;
    }

    #[tokio::test]
    async fn expired_active_attempt_is_not_scoped_to_due_runs() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::expired_active_attempt_is_not_scoped_to_due_runs(&backend).await;
    }

    #[tokio::test]
    async fn unowned_session_claim_still_acquires() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::unowned_session_claim_still_acquires(&backend).await;
    }

    #[tokio::test]
    async fn lookup_agent_uses_lowest_id_and_records_client() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::lookup_agent_uses_lowest_id_and_records_client(&backend).await;
    }

    #[tokio::test]
    async fn broker_session_requires_a_live_runner() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::broker_session_requires_a_live_runner(&backend).await;
    }

    #[tokio::test]
    async fn debug_token_gates_follow_the_contract() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::debug_token_gates_follow_the_contract(&backend).await;
    }

    #[tokio::test]
    async fn azdo_compat_session_is_served() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::azdo_compat_session_is_served(&backend).await;
    }

    #[tokio::test]
    async fn renew_agent_request_follows_the_lease_ladder() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::renew_agent_request_follows_the_lease_ladder(&backend).await;
    }

    /// A dangling `job_requests.session_id` (no `runner_sessions` row behind
    /// it) is not a live session binding: both status reads join the session
    /// table, like the SQLite backend's.
    #[tokio::test]
    async fn dangling_session_binding_is_not_live() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        let run_id = RunId::new();
        let runner = backend
            .register_runner(register_runner("dangling"))
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
        assert!(matches!(outcome, PollOutcome::Claimed(_)));
        assert!(backend.sole_inflight_request().await.unwrap().is_some());
        assert_eq!(backend.live_assignments().await.unwrap().len(), 1);

        // `job_requests.session_id` has no foreign key, so the binding has to
        // be resolved by the join rather than by the column value.
        let client = backend.writer().await.unwrap();
        client
            .execute(
                "DELETE FROM runner_sessions WHERE session_id=$1::text::uuid",
                &[&crate::control::logic::session_uuid(&session.session_id).to_string()],
            )
            .await
            .unwrap();
        drop(client);

        assert!(backend.sole_inflight_request().await.unwrap().is_none());
        assert!(backend.live_assignments().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn max_parallel_admits_the_full_cap_in_one_pass() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::max_parallel_admits_the_full_cap_in_one_pass(&backend).await;
    }

    /// A run cancelled on arrival emits its creation and its completion,
    /// each exactly once.
    #[tokio::test]
    async fn arrival_cancelled_submit_emits_run_events() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        let older = suite::submit_arrival_cancelled_run(&backend).await;
        let topics = backend.test_working_set().await.unwrap().outbox_topics;
        suite::assert_created_and_completed_once(&topics, older);
    }

    #[tokio::test]
    async fn duplicate_run_number_is_reallocated() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::duplicate_run_number_is_reallocated(&backend).await;
    }

    #[tokio::test]
    async fn ids_and_timestamps_are_stored_canonically() {
        let Some((_pg, backend)) = backend().await else {
            return skip_no_postgres();
        };
        suite::ids_and_timestamps_are_stored_canonically(&backend).await;
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

    /// A run cancelled on arrival emits its creation and its completion,
    /// each exactly once.
    #[tokio::test]
    async fn arrival_cancelled_submit_emits_run_events() {
        let backend = LiteBackend::in_memory().unwrap();
        let older = suite::submit_arrival_cancelled_run(&backend).await;
        let topics = backend.test_working_set().unwrap().outbox_topics;
        suite::assert_created_and_completed_once(&topics, older);
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
    async fn status_events_are_stamped_with_row_versions() {
        let backend = LiteBackend::in_memory().unwrap();
        suite::status_events_are_stamped_with_row_versions(&backend).await;
        suite::assert_status_stamps(&backend.test_working_set().unwrap().outbox_stamps);
    }

    #[tokio::test]
    async fn prune_outbox_keeps_fresh_rows_and_bounds_a_batch() {
        suite::prune_outbox_keeps_fresh_rows_and_bounds_a_batch(&LiteBackend::in_memory().unwrap())
            .await;
    }

    #[tokio::test]
    async fn check_run_projection_never_regresses_a_newer_row() {
        suite::check_run_projection_never_regresses_a_newer_row(&LiteBackend::in_memory().unwrap())
            .await;
    }

    #[tokio::test]
    async fn acquire_projects_in_progress_check_run() {
        suite::acquire_projects_in_progress_check_run(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn check_run_bookmark_survives_consumer_restart() {
        suite::check_run_bookmark_survives_consumer_restart(&LiteBackend::in_memory().unwrap())
            .await;
    }

    #[tokio::test]
    async fn prune_outbox_respects_slowest_consumer() {
        suite::prune_outbox_respects_slowest_consumer(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn check_run_lease_renewal_respects_takeover() {
        suite::check_run_lease_renewal_respects_takeover(&LiteBackend::in_memory().unwrap()).await;
    }

    /// The bookmark lives in the SQLite file: reopen the database and only the
    /// events committed while the consumer was down are delivered.
    #[tokio::test]
    async fn check_run_bookmark_survives_a_file_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("preloop.db");
        let run_id = RunId::new();
        let consumed = {
            let backend =
                LiteBackend::open(&path, false, false, std::time::Duration::from_secs(300))
                    .unwrap();
            backend
                .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
                .await
                .unwrap();
            backend
                .consume_check_run_outbox(
                    "reopen-consumer",
                    std::time::Duration::from_millis(1),
                    256,
                )
                .await
                .unwrap()
        };
        assert!(consumed > 0);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        // Down window: a fresh handle appends without consuming.
        let reopened =
            LiteBackend::open(&path, false, false, std::time::Duration::from_secs(300)).unwrap();
        let run_id2 = RunId::new();
        reopened
            .submit_run(submit_run(run_id2, vec![submit_job(run_id2, "build", 1)]))
            .await
            .unwrap();
        let total = reopened
            .outbox_read(
                crate::control::types::OutboxBookmark {
                    txid: 0,
                    event_id: 0,
                },
                10_000,
            )
            .await
            .unwrap()
            .len();
        let delivered = reopened
            .consume_check_run_outbox("reopen-consumer-2", std::time::Duration::from_secs(30), 256)
            .await
            .unwrap();
        assert_eq!(
            delivered,
            total - consumed,
            "a reopened consumer must resume at the persisted bookmark"
        );
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
    async fn failed_deferred_expansion_settles_the_node_once() {
        suite::failed_deferred_expansion_settles_the_node_once(&LiteBackend::in_memory().unwrap())
            .await;
    }

    #[tokio::test]
    async fn deferred_matrix_expansion_scopes_its_inputs() {
        suite::deferred_matrix_expansion_scopes_its_inputs(&LiteBackend::in_memory().unwrap())
            .await;
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
    async fn expired_fork_hold_fails_closed() {
        suite::expired_fork_hold_fails_closed(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn environment_gate_parks_until_satisfied() {
        suite::environment_gate_parks_until_satisfied(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn denied_environment_gate_at_promotion_fails_the_job_once() {
        suite::denied_environment_gate_at_promotion_fails_the_job_once(
            &LiteBackend::in_memory().unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn unhostable_runs_on_at_promotion_fails_the_job_once() {
        suite::unhostable_runs_on_at_promotion_fails_the_job_once(
            &LiteBackend::in_memory().unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn unknown_environment_runs_without_a_gate() {
        suite::unknown_environment_runs_without_a_gate(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn reviewer_gate_holds_until_decision() {
        suite::reviewer_gate_holds_until_decision(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn unresolved_environment_rules_hold_the_job() {
        suite::unresolved_environment_rules_hold_the_job(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn reported_environment_url_lands_on_the_deployment_row() {
        suite::reported_environment_url_lands_on_the_deployment_row(
            &LiteBackend::in_memory().unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn deferred_environment_name_resolves_on_the_deployment_row() {
        suite::deferred_environment_name_resolves_on_the_deployment_row(
            &LiteBackend::in_memory().unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn deferred_environment_name_releases_after_needs() {
        suite::deferred_environment_name_releases_after_needs(&LiteBackend::in_memory().unwrap())
            .await;
    }

    #[tokio::test]
    async fn environment_approval_audit_survives_archival() {
        let backend = LiteBackend::in_memory().unwrap();
        let db = &backend;
        suite::environment_approval_audit_survives_archival(&backend, move |run_id| async move {
            let completed = crate::models::now_unix_nanos() / 1000 - 120 * 1_000_000;
            db.test_db_mutate(move |tx| {
                tx.execute(
                    "UPDATE runs SET completed_at = ?1 WHERE run_id = ?2",
                    (completed, run_id.0.to_string()),
                )
            })
            .unwrap()
            .unwrap();
        })
        .await;
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
    async fn timeline_patch_enforces_byte_budget() {
        suite::timeline_patch_enforces_byte_budget(&LiteBackend::in_memory().unwrap()).await;
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
    async fn dispatch_info_flags_expandable_placeholders() {
        suite::dispatch_info_flags_expandable_placeholders(&LiteBackend::in_memory().unwrap())
            .await;
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
            .patch_timeline(&key, vec![super::timeline_record(1, "one")], &[])
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
                pool_labels: Vec::new(),
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
    async fn namespace_state_gates_submit_and_claim() {
        let backend = LiteBackend::in_memory().unwrap();
        let db = &backend;
        suite::namespace_state_gates_submit_and_claim(&backend, |sql| async move {
            db.exec_for_test(&sql)
        })
        .await;
    }

    #[tokio::test]
    async fn namespace_running_caps_hold_claims_at_limit() {
        let backend = LiteBackend::in_memory().unwrap();
        let db = &backend;
        suite::namespace_running_caps_hold_claims_at_limit(&backend, |sql| async move {
            db.exec_for_test(&sql)
        })
        .await;
    }

    #[tokio::test]
    async fn namespace_queue_cap_refuses_overflowing_submit() {
        let backend = LiteBackend::in_memory().unwrap();
        let db = &backend;
        suite::namespace_queue_cap_refuses_overflowing_submit(&backend, |sql| async move {
            db.exec_for_test(&sql)
        })
        .await;
    }

    #[tokio::test]
    async fn namespace_run_size_and_rate_limits() {
        let backend = LiteBackend::in_memory().unwrap();
        let db = &backend;
        suite::namespace_run_size_and_rate_limits(
            &backend,
            |sql| async move { db.exec_for_test(&sql) },
        )
        .await;
    }

    #[tokio::test]
    async fn webhook_runs_bypass_submit_quotas() {
        let backend = LiteBackend::in_memory().unwrap();
        let db = &backend;
        suite::webhook_runs_bypass_submit_quotas(
            &backend,
            |sql| async move { db.exec_for_test(&sql) },
        )
        .await;
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
    async fn live_session_with_stale_lease_is_reaped_as_hung() {
        suite::live_session_with_stale_lease_is_reaped_as_hung(&LiteBackend::in_memory().unwrap())
            .await;
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

    /// The migration ledger is the boot contract: a foreign database, a
    /// legacy store and an uninitialized file each refuse with their own
    /// guidance; a test-support fresh file initializes through the real
    /// migration runner (embedded refinery) and verifies.
    #[test]
    fn opening_a_foreign_database_is_refused() {
        let dir = tempfile::tempdir().unwrap();

        // Foreign tables, no control schema.
        let foreign = dir.path().join("foreign.db");
        rusqlite::Connection::open(&foreign)
            .unwrap()
            .execute_batch("CREATE TABLE foreign_table (id INTEGER)")
            .unwrap();
        let error = LiteBackend::open(&foreign, false, false, std::time::Duration::from_secs(300))
            .err()
            .expect("a foreign database must be refused");
        assert!(error.to_string().contains("no control schema"), "{error}");

        // The released legacy store (v11, `schema_migrations`).
        let legacy = dir.path().join("legacy.db");
        rusqlite::Connection::open(&legacy)
            .unwrap()
            .execute_batch(
                "PRAGMA user_version = 11;
                 CREATE TABLE schema_migrations (version INTEGER, name TEXT);
                 CREATE TABLE runs (run_id TEXT PRIMARY KEY);",
            )
            .unwrap();
        let error = LiteBackend::open(&legacy, false, false, std::time::Duration::from_secs(300))
            .err()
            .expect("a legacy store must be refused");
        assert!(error.to_string().contains("legacy"), "{error}");
        assert!(error.to_string().contains("preloop store import-legacy"), "{error}");

        // A fresh file initializes (test-support) through the migration
        // runner and carries the build's ledger.
        let fresh = dir.path().join("fresh.db");
        LiteBackend::open(&fresh, false, false, std::time::Duration::from_secs(300)).unwrap();
        let versions: Vec<i32> = rusqlite::Connection::open(&fresh)
            .unwrap()
            .prepare("SELECT version FROM refinery_schema_history ORDER BY version")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(versions, crate::control::migrations::MIGRATIONS);
    }

    #[tokio::test]
    async fn close_session_keeps_request_owner() {
        suite::close_session_keeps_request_owner(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn closed_session_attempt_stays_reapable() {
        suite::closed_session_attempt_stays_reapable(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn expired_active_attempt_is_not_scoped_to_due_runs() {
        suite::expired_active_attempt_is_not_scoped_to_due_runs(&LiteBackend::in_memory().unwrap())
            .await;
    }

    #[tokio::test]
    async fn unowned_session_claim_still_acquires() {
        suite::unowned_session_claim_still_acquires(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn lookup_agent_uses_lowest_id_and_records_client() {
        suite::lookup_agent_uses_lowest_id_and_records_client(&LiteBackend::in_memory().unwrap())
            .await;
    }

    #[tokio::test]
    async fn broker_session_requires_a_live_runner() {
        suite::broker_session_requires_a_live_runner(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn debug_token_gates_follow_the_contract() {
        suite::debug_token_gates_follow_the_contract(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn azdo_compat_session_is_served() {
        suite::azdo_compat_session_is_served(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn renew_agent_request_follows_the_lease_ladder() {
        suite::renew_agent_request_follows_the_lease_ladder(&LiteBackend::in_memory().unwrap())
            .await;
    }

    /// The same predicate on SQLite: a dangling `job_requests.session_id`
    /// never counts, because the status reads join `runner_sessions`.
    #[tokio::test]
    async fn dangling_session_binding_is_not_live() {
        let backend = LiteBackend::in_memory().unwrap();
        let run_id = RunId::new();
        let runner = backend
            .register_runner(super::register_runner("dangling"))
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
        let outcome = backend
            .poll_session(super::poll(&session.session_id, runner.runner.id))
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            crate::control::types::PollOutcome::Claimed(_)
        ));
        assert!(backend.sole_inflight_request().await.unwrap().is_some());
        assert_eq!(backend.live_assignments().await.unwrap().len(), 1);

        let session_uuid = crate::control::logic::session_uuid(&session.session_id).to_string();
        backend
            .test_db_mutate(|tx| {
                tx.execute(
                    "DELETE FROM runner_sessions WHERE session_id = ?1",
                    [session_uuid.as_str()],
                )
            })
            .unwrap()
            .unwrap();

        assert!(backend.sole_inflight_request().await.unwrap().is_none());
        assert!(backend.live_assignments().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn max_parallel_admits_the_full_cap_in_one_pass() {
        suite::max_parallel_admits_the_full_cap_in_one_pass(&LiteBackend::in_memory().unwrap())
            .await;
    }

    #[tokio::test]
    async fn duplicate_run_number_is_reallocated() {
        suite::duplicate_run_number_is_reallocated(&LiteBackend::in_memory().unwrap()).await;
    }

    #[tokio::test]
    async fn ids_and_timestamps_are_stored_canonically() {
        suite::ids_and_timestamps_are_stored_canonically(&LiteBackend::in_memory().unwrap()).await;
    }

    /// `job_requests.timeline_id` is deliberately not UNIQUE: several attempts
    /// of one job share a timeline (pg cannot — its column is UNIQUE and
    /// `timelines` FKs to one request). The
    /// `job_requests_timeline_cascade` trigger removes the timeline row only
    /// when the last request referencing it is deleted.
    #[tokio::test]
    async fn timeline_is_shared_across_attempts_and_pruned_by_trigger() {
        fn timeline_rows(tx: &crate::control::lite::TestDb<'_>, timeline_id: &str) -> i64 {
            tx.0.query_row(
                "SELECT count(*) FROM timelines WHERE timeline_id = ?1",
                rusqlite::params![timeline_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
        }

        let backend = LiteBackend::in_memory().unwrap();
        let run_id = RunId::new();
        backend
            .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
            .await
            .unwrap();
        let run_key = run_id.0.to_string();
        let timeline_id = uuid::Uuid::new_v4().to_string();
        let retry_agent = uuid::Uuid::new_v4().to_string();
        backend
            .test_db_mutate(|tx| {
                // Submission already minted one unclaimed attempt; settle it
                // onto a shared timeline, then add the retry on the same
                // timeline. Sharing is only meaningful once the previous
                // attempt is settled — that is also what keeps the partial
                // inflight index ("one open attempt per job") satisfied.
                let first_agent: String =
                    tx.0.query_row(
                        "SELECT agent_job_id FROM job_requests \
                         WHERE run_id = ?1 AND job_id = 'build'",
                        rusqlite::params![run_key],
                        |row| row.get(0),
                    )
                    .unwrap();
                tx.execute(
                    "INSERT INTO timelines (timeline_id, change_id) VALUES (?1, 0)",
                    rusqlite::params![timeline_id],
                )
                .unwrap();
                tx.execute(
                    "UPDATE job_requests SET timeline_id = ?2, result = 'failure' \
                     WHERE agent_job_id = ?1",
                    rusqlite::params![first_agent, timeline_id],
                )
                .unwrap();
                tx.execute(
                    "INSERT INTO job_requests (run_id, job_id, namespace_id, \
                     agent_job_id, timeline_id, claimed_at) \
                     VALUES (?1, 'build', 'default', ?2, ?3, 0)",
                    rusqlite::params![run_key, retry_agent, timeline_id],
                )
                .expect("two attempts of one job may share the timeline id");
                assert_eq!(
                    timeline_rows(tx, &timeline_id),
                    1,
                    "one shared timeline row"
                );
                tx.execute(
                    "DELETE FROM job_requests WHERE agent_job_id = ?1",
                    rusqlite::params![retry_agent],
                )
                .unwrap();
                assert_eq!(
                    timeline_rows(tx, &timeline_id),
                    1,
                    "the timeline outlives the retry (the settled attempt still references it)"
                );
                tx.execute(
                    "DELETE FROM job_requests WHERE agent_job_id = ?1",
                    rusqlite::params![first_agent],
                )
                .unwrap();
                assert_eq!(
                    timeline_rows(tx, &timeline_id),
                    0,
                    "the cascade trigger prunes the orphaned timeline"
                );
            })
            .unwrap();
    }

    /// The claim query's `ORDER BY` must be served by `jobs_ready` without a
    /// temp B-tree: the key order is the whole point of the index (pg carries
    /// the same key). While `namespace_id` sat second, SQLite sorted the last
    /// three ORDER BY terms on every poll.
    #[tokio::test]
    async fn jobs_ready_serves_the_claim_order_without_a_sort() {
        let backend = LiteBackend::in_memory().unwrap();
        let plan = backend
            .test_db_mutate(|tx| {
                tx.query_plan(&format!(
                    "SELECT j.run_id FROM jobs j \
                     WHERE j.queue_state = 'ready' AND ({}) \
                     ORDER BY j.pool_key, j.priority DESC, j.run_order, j.job_order \
                     LIMIT 64 OFFSET 0",
                    crate::control::types::NAMESPACE_ADMITS_CLAIM
                ))
            })
            .unwrap();
        assert!(
            plan.iter().any(|detail| detail.contains("jobs_ready")),
            "the claim query must use the jobs_ready index: {plan:?}"
        );
        assert!(
            !plan.iter().any(|detail| detail.contains("B-TREE")),
            "the ready-queue order must come from the index, not a sort: {plan:?}"
        );
    }

    /// Latest-attempt lookups must use `job_requests_attempts`: the partial
    /// inflight index cannot serve settled attempts, and the row order must
    /// come from the index.
    #[tokio::test]
    async fn attempt_lookups_use_the_attempts_index() {
        let backend = LiteBackend::in_memory().unwrap();
        let plan = backend
            .test_db_mutate(|tx| {
                tx.query_plan(
                    "SELECT request_id FROM job_requests \
                     WHERE run_id = '00000000-0000-0000-0000-000000000000' AND job_id = 'build' \
                     ORDER BY request_id DESC LIMIT 1",
                )
            })
            .unwrap();
        assert!(
            plan.iter()
                .any(|detail| detail.contains("job_requests_attempts")),
            "latest-attempt lookups must use job_requests_attempts: {plan:?}"
        );
        assert!(
            !plan.iter().any(|detail| detail.contains("B-TREE")),
            "the attempt order must come from the index, not a sort: {plan:?}"
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
    use crate::control::logic::{SESSION_ID_NAMESPACE, session_uuid};

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
