//! Multi-node safety tests against a disposable PostgreSQL cluster.
//!
//! These run the new-backend commands through two `PgBackend` instances on
//! ONE database — the shape of two engine nodes sharing a cell — and prove
//! the conditional-`UPDATE` transitions never double-claim or double-mint.
//! The suite in `control/tests.rs` (driven via `&dyn ControlBackend`) joins
//! this module once the trait is wired.
//!
//! A `PRELOOP_TEST_POSTGRES_URL`-provided server is used when set; otherwise
//! each test starts its own `initdb`'d cluster (Postgres.app / Homebrew /
//! system bin, or `initdb` on `PATH`).

use super::PgBackend;
use crate::control::backend::{
    CreateSession, JobCompletionInput, PollRequest, RegisterRunner, RequestKey,
};
use crate::control::types::{
    ControlError, PollOutcome, SessionProtocol, StepPatch, SubmitJob, SubmitRun,
};
use crate::models::{QueuedJob, RunRecord, RunnerCapabilities, TaskAgentJobRequestRecord};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId, azdo};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

// ── disposable cluster ──────────────────────────────────────────────────

/// A throwaway PostgreSQL cluster in a temp dir: `initdb`, `postgres` on a
/// free port, `pg_ctl stop` on drop. Each test gets its own cluster, so
/// tests are isolated and parallel-safe.
struct DisposablePg {
    dir: PathBuf,
    port: u16,
    /// Retains a per-test database created on `PRELOOP_TEST_POSTGRES_URL`.
    /// Without this guard, the temporary database is dropped immediately
    /// after `fresh_database` returns and all backend connections fail.
    database: Option<crate::test_pg::TestDatabase>,
}

impl DisposablePg {
    fn start() -> Self {
        let bin = pg_bin();
        // Short path: the `-k` Unix socket lives inside `dir`, and
        // `std::env::temp_dir()` + a UUID exceeds the 104-byte `sun_path`
        // limit. `/tmp` keeps it well under.
        let short = &uuid::Uuid::new_v4().simple().to_string()[..8];
        let dir = PathBuf::from(format!("/tmp/preloop-newpg-{short}"));
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
        Self {
            dir,
            port,
            database: None,
        }
    }

    fn url(&self) -> String {
        format!("postgres://postgres@127.0.0.1:{}/postgres", self.port)
    }
}

impl Drop for DisposablePg {
    fn drop(&mut self) {
        if self.database.is_some() {
            return;
        }
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

async fn connect(url: &str) -> PgBackend {
    PgBackend::connect(url, false, false, std::time::Duration::from_secs(300))
        .await
        .expect("test database connection failed")
}

/// Two independent backends (separate writer pools) on ONE database: the
/// shape of two engine nodes sharing a cell, for race tests.
async fn backend_pair() -> (DisposablePg, PgBackend, PgBackend) {
    let (guard, url) = fresh_database().await;
    let first = connect(&url).await;
    let second = connect(&url).await;
    (guard, first, second)
}

async fn fresh_database() -> (DisposablePg, String) {
    // A shared server (PRELOOP_TEST_POSTGRES_URL) is honored by the suite
    // harness; the pg unit tests always isolate via a disposable cluster so
    // they also run without configuration.
    if let Some((database, url)) = crate::test_pg::fresh_database().await {
        return (
            DisposablePg {
                dir: PathBuf::new(),
                port: 0,
                database: Some(database),
            },
            url,
        );
    }
    let pg = DisposablePg::start();
    let url = pg.url();
    (pg, url)
}

// ── submission builders (self-contained; suite helpers are private) ─────

fn submission() -> preloop_gha_protocol::WorkflowSubmission {
    preloop_gha_protocol::WorkflowSubmission {
        repository: "owner/repo".to_owned(),
        event: "push".to_owned(),
        sha: "abc123".to_owned(),
        ..Default::default()
    }
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
        fork_approval_pending: false,
        fork_approval_requested_at_unix_nanos: None,
        fork_approved_at_unix_nanos: None,
        fork_approval_note: None,
        reports_check_runs: false,
    }
}

fn job_message(job_id: &str, request_id: i64) -> azdo::AgentJobRequestMessage {
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

// ── races ────────────────────────────────────────────────────────────────

async fn submit_many(node: &PgBackend, count: usize, base_run_number: u64) -> Vec<uuid::Uuid> {
    let submits: Vec<_> = (0..count)
        .map(|i| {
            let run_id = RunId::new();
            let mut submit = submit_run(run_id, vec![submit_job(run_id, "build", 0)]);
            // The caller owns run_number allocation (`allocate_run_number`);
            // two nodes must not collide on the unique key.
            submit.record.run_number = base_run_number + i as u64;
            submit
        })
        .collect();
    let agents = submits
        .iter()
        .map(|s| s.jobs[0].request.as_ref().unwrap().agent_job_id)
        .collect();
    let results = futures::future::join_all(submits.into_iter().map(|s| node.submit_run(s))).await;
    for result in results {
        result.unwrap();
    }
    agents
}

/// The counter is durable and scoped by `(namespace, repository, workflow)`.
/// Two nodes racing on one key receive distinct consecutive numbers; another
/// repository starts at one without colliding.
#[tokio::test]
async fn run_numbers_are_atomic_and_scoped() {
    let (_pg, node_a, node_b) = backend_pair().await;
    let key = ("default", "owner/repo", ".github/workflows/ci.yml");
    let (first, second) = tokio::join!(
        node_a.allocate_run_number(key.0, key.1, key.2),
        node_b.allocate_run_number(key.0, key.1, key.2)
    );
    let mut numbers = [first.unwrap(), second.unwrap()];
    numbers.sort_unstable();
    assert_eq!(numbers, [1, 2]);
    assert_eq!(
        node_a
            .allocate_run_number(key.0, key.1, key.2)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        node_b
            .allocate_run_number("default", "other/repo", key.2)
            .await
            .unwrap(),
        1
    );
}

/// Submits on different runs overlap across two nodes. The request id each
/// mints must still be unique — it is the cross-node correlation key.
#[tokio::test]
async fn concurrent_submits_mint_distinct_request_ids() {
    let (_pg, node_a, node_b) = backend_pair().await;
    const PER_NODE: usize = 24;
    let (agents_a, agents_b) = tokio::join!(
        submit_many(&node_a, PER_NODE, 1_000),
        submit_many(&node_b, PER_NODE, 100_000)
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

/// N sessions on N nodes poll for ONE ready job at the same instant.
/// Exactly one `PollOutcome::Claimed` may come back — the conditional claim
/// `UPDATE` is the mutual exclusion, and it must hold across nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_polls_claim_once() {
    let (_pg, node_a, node_b) = backend_pair().await;
    let runner = node_a.register_runner(register_runner("r1")).await.unwrap();
    // Two sessions on two nodes, all polling the same queue.
    let session_a = node_a
        .create_session(create_session(runner.runner.id))
        .await
        .unwrap();
    let session_b = node_b
        .create_session(create_session(runner.runner.id))
        .await
        .unwrap();
    let run_id = RunId::new();
    node_a
        .submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
        .await
        .unwrap();

    let poll_a = poll(&session_a.session_id, runner.runner.id);
    let poll_b = poll(&session_b.session_id, runner.runner.id);
    let (out_a, out_b) = tokio::join!(node_a.poll_session(poll_a), node_b.poll_session(poll_b));
    let claims = [out_a, out_b]
        .into_iter()
        .map(|r| r.unwrap())
        .filter(|o| matches!(o, PollOutcome::Claimed(_)))
        .count();
    assert_eq!(claims, 1, "one job must be claimed exactly once");
}

/// The same webhook delivery processed by two nodes at once must still produce
/// ONE run: the loser sees `SubmitOutcome::existing`, never a unique-violation
/// 500 on `runs_delivery`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_delivery_redelivery_returns_one_run() {
    let (_pg, node_a, node_b) = backend_pair().await;
    let first = RunId::new();
    let mut submit_a = submit_run(first, vec![submit_job(first, "build", 1)]);
    submit_a.record.webhook_delivery_id = Some("delivery-race".to_owned());
    submit_a.record.run_number = 101;
    let replay = RunId::new();
    let mut submit_b = submit_run(replay, vec![submit_job(replay, "build", 2)]);
    submit_b.record.webhook_delivery_id = Some("delivery-race".to_owned());
    submit_b.record.run_number = 102;

    let (a, b) = tokio::join!(node_a.submit_run(submit_a), node_b.submit_run(submit_b));
    let a = a.expect("one node must not fail on the delivery race");
    let b = b.expect("the other node must not fail on the delivery race");
    assert_eq!(a.run_id, b.run_id, "one delivery produces one run");
    assert_eq!(
        a.queued_jobs + b.queued_jobs,
        1,
        "exactly one node created the run's jobs"
    );
    assert!(
        a.existing.is_some() || b.existing.is_some(),
        "the loser replays the committed run: {a:?} / {b:?}"
    );
}

/// A transaction that makes work available wakes waiters on OTHER nodes: the
/// commit publishes the `Wake` hint on the shared LISTEN channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commits_wake_waiters_on_another_node() {
    let (_pg, node_a, node_b) = backend_pair().await;
    let mut wakes = node_b.subscribe_wakes();
    // The LISTEN connection registers asynchronously with `connect`; retry a
    // bounded number of commits so a lost race on that registration cannot
    // make the assertion flaky.
    let mut received = None;
    for attempt in 0..20u64 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let run_id = RunId::new();
        let mut submit = submit_run(run_id, vec![submit_job(run_id, "build", attempt as i64)]);
        submit.record.run_number = 5_000 + attempt;
        node_a.submit_run(submit).await.unwrap();
        match tokio::time::timeout(std::time::Duration::from_millis(500), wakes.recv()).await {
            Ok(Ok(wake)) => {
                received = Some(wake);
                break;
            }
            Ok(Err(error)) => panic!("wake channel closed: {error}"),
            Err(_) => continue,
        }
    }
    let wake = received.expect("a commit that makes a job ready must wake other nodes");
    assert!(
        wake.ready >= 1,
        "the hint must ask for at least one waiter, got {wake:?}"
    );
}

fn timeline_record(id: u128, name: &str) -> azdo::TimelineRecord {
    serde_json::from_value(serde_json::json!({
        "id": uuid::Uuid::from_u128(id),
        "name": name,
        "type": "Task",
        "state": "completed",
        "result": "succeeded",
    }))
    .unwrap()
}

/// Submit a run, claim its job, and return the request's correlation ids.
/// Timelines and steps are per attempt, so every test needs a real request
/// row behind them.
async fn submit_and_claim(
    node: &PgBackend,
    run_id: RunId,
) -> (crate::models::TaskAgentJobRequestRecord, i64) {
    let runner = node.register_runner(register_runner("r1")).await.unwrap();
    let session = node
        .create_session(create_session(runner.runner.id))
        .await
        .unwrap();
    node.submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
        .await
        .unwrap();
    let outcome = node
        .poll_session(poll(&session.session_id, runner.runner.id))
        .await
        .unwrap();
    let PollOutcome::Claimed(claimed) = outcome else {
        panic!("expected a claim, got {outcome:?}");
    };
    (claimed.request, runner.runner.id)
}

/// Far more concurrent acquires than pooled readers must all complete. An
/// acquire that held one reader while checking out a second deadlocked the
/// pool as soon as every reader was held by an acquire waiting for another —
/// taking every other command (and the webhook inbox) down with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_acquires_beyond_pool_size_complete() {
    let (_pg, node, _other) = backend_pair().await;
    let (request, runner_id) = submit_and_claim(&node, RunId::new()).await;
    let acquires = (0..64).map(|_| node.acquire_for_runner(request.request_id, runner_id));
    let results = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        futures::future::join_all(acquires),
    )
    .await
    .expect("concurrent acquires deadlocked the connection pool");
    for result in results {
        assert_eq!(result.unwrap().request.request_id, request.request_id);
    }
}

/// PATCH on node A and GET on node B share one counter and one row set;
/// concurrent PATCHes never reuse a change id. Unknown timelines are
/// `NotFound` (a PATCH must not create a timeline for a request that does
/// not exist).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timelines_are_shared_and_bounded_across_nodes() {
    let (_pg, node_a, node_b) = backend_pair().await;
    let run_id = RunId::new();
    let (request, _runner_id) = submit_and_claim(&node_a, run_id).await;
    let key = format!("{}/{}", request.plan_id, request.timeline_id);

    // No request owns this key: PATCH refuses, GET reads as empty.
    let missing = format!("{}/{}", uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    assert!(matches!(
        node_a
            .patch_timeline(&missing, vec![timeline_record(1, "x")])
            .await,
        Err(ControlError::NotFound(_))
    ));
    let (id, rows) = node_a.get_timeline(&missing, 0, 50).await.unwrap();
    assert_eq!(id, 0);
    assert!(rows.is_empty());

    let (first, _) = node_a
        .patch_timeline(&key, vec![timeline_record(1, "one")])
        .await
        .unwrap();
    let (second, stored) = node_b
        .patch_timeline(
            &key,
            vec![timeline_record(2, "two"), timeline_record(1, "uno")],
        )
        .await
        .unwrap();
    assert_eq!((first, second), (1, 2));
    assert_eq!(stored.len(), 2, "upsert by record id, not append");
    let (change_id, records) = node_a.get_timeline(&key, 0, usize::MAX).await.unwrap();
    assert_eq!(change_id, 2);
    let names: Vec<_> = records.iter().filter_map(|r| r.name.clone()).collect();
    assert_eq!(names, ["uno", "two"], "node A sees node B's upsert");

    // Interleaved PATCHes from both nodes allocate one counter sequence.
    let mut ids: Vec<i32> = Vec::new();
    let handles: Vec<_> = (0..20)
        .map(|i| {
            let node = if i % 2 == 0 { &node_a } else { &node_b };
            let key = key.clone();
            async move {
                node.patch_timeline(&key, vec![timeline_record(10 + i as u128, "x")])
                    .await
            }
        })
        .collect();
    for result in futures::future::join_all(handles).await {
        ids.push(result.unwrap().0);
    }
    ids.sort_unstable();
    assert_eq!(ids, (3..23).collect::<Vec<_>>());
}

/// `prune_timelines` removes only timelines whose request settled before
/// the cutoff; a live request's timeline survives.
#[tokio::test]
async fn prune_timelines_drops_settled_attempts() {
    let (_pg, node_a, _node_b) = backend_pair().await;
    let run_id = RunId::new();
    let (request, runner_id) = submit_and_claim(&node_a, run_id).await;
    let key = format!("{}/{}", request.plan_id, request.timeline_id);
    node_a
        .patch_timeline(&key, vec![timeline_record(1, "one")])
        .await
        .unwrap();

    // Unsettled: prune is a no-op.
    assert_eq!(
        node_a
            .prune_timelines(chrono::Utc::now().timestamp_micros())
            .await
            .unwrap(),
        0
    );
    node_a
        .complete_job(JobCompletionInput {
            run_id,
            job_id: JobId("build".to_owned()),
            agent_job_id: Some(request.agent_job_id),
            status: ExecutionStatus::Success,
            outputs: BTreeMap::new(),
            runner_id: Some(runner_id),
        })
        .await
        .unwrap();
    assert_eq!(
        node_a
            .prune_timelines(chrono::Utc::now().timestamp_micros() + 1_000_000)
            .await
            .unwrap(),
        1
    );
    assert_eq!(node_a.get_timeline(&key, 0, 50).await.unwrap().1.len(), 0);
}

/// `patch_steps` upserts runner patches keyed by step id: a new id appends
/// in position order; a repeat merges name/conclusion and only fills
/// timestamps forward. Unknown attempts write nothing.
#[tokio::test]
async fn patch_steps_upserts_synthetic_steps() {
    let (_pg, node_a, _node_b) = backend_pair().await;
    let run_id = RunId::new();
    let (request, _runner_id) = submit_and_claim(&node_a, run_id).await;
    let agent = request.agent_job_id;
    let now = chrono::Utc::now().timestamp_micros();

    node_a
        .patch_steps(
            agent,
            vec![StepPatch {
                id: "step-1".to_owned(),
                name: "Build".to_owned(),
                conclusion: "success".to_owned(),
                started_at_us: Some(now),
                finished_at_us: Some(now + 5),
                observed_us: now,
            }],
        )
        .await
        .unwrap();
    // Repeat id: merges, does not append a second row.
    node_a
        .patch_steps(
            agent,
            vec![StepPatch {
                id: "step-1".to_owned(),
                name: "Build (renamed)".to_owned(),
                conclusion: "success".to_owned(),
                started_at_us: None,
                finished_at_us: None,
                observed_us: now + 10,
            }],
        )
        .await
        .unwrap();
    // Unknown attempt: silently no-op (dropped report).
    node_a
        .patch_steps(
            uuid::Uuid::new_v4(),
            vec![StepPatch {
                id: "x".to_owned(),
                name: "x".to_owned(),
                conclusion: "success".to_owned(),
                started_at_us: None,
                finished_at_us: None,
                observed_us: now,
            }],
        )
        .await
        .unwrap();

    let manifests = node_a.run_step_manifests(run_id).await.unwrap();
    let steps = manifests.get(&agent).expect("manifest for the attempt");
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].id, "step-1");
    assert_eq!(steps[0].name, "Build (renamed)");
    assert_eq!(steps[0].started_at.map(|t| t.timestamp_micros()), Some(now));
}

/// Log ids are per plan, 1-based, and arbitrated across nodes by the
/// `(plan_id, log_id)` unique constraint — two racing appenders on different
/// nodes get distinct ids.
#[tokio::test]
async fn create_log_allocates_per_plan_across_nodes() {
    let (_pg, node_a, node_b) = backend_pair().await;
    let run_id = RunId::new();
    let (request, _runner_id) = submit_and_claim(&node_a, run_id).await;
    let plan = request.agent_job_id.to_string();

    assert!(matches!(
        node_a.create_log(&uuid::Uuid::new_v4().to_string()).await,
        Err(ControlError::NotFound(_))
    ));
    let (a, b) = tokio::join!(node_a.create_log(&plan), node_b.create_log(&plan));
    let mut ids = [a.unwrap(), b.unwrap()];
    ids.sort_unstable();
    assert_eq!(ids, [1, 2], "one log id per allocation, loser retries");
    assert_eq!(node_a.create_log(&plan).await.unwrap(), 3);
}

// ── pool pairing / purge / archival (pg-only: needs pools on, an aged
// binding, or a raw timestamp write) ─────────────────────────────────────

/// Pool assignments on. `pair_runner` runs only in that mode.
async fn pool_backend() -> (DisposablePg, PgBackend) {
    let (guard, url) = fresh_database().await;
    let backend = PgBackend::connect(&url, true, false, std::time::Duration::from_secs(300))
        .await
        .expect("test database connection failed");
    (guard, backend)
}

/// The runner id the working set shows bound to `(run, job)`.
async fn assigned_runner(node: &PgBackend, run_id: RunId, job_id: &JobId) -> Option<i64> {
    node.test_working_set()
        .await
        .unwrap()
        .job_assignments
        .get(&(run_id, job_id.clone()))
        .and_then(|assignment| assignment.runner_id)
}

/// `sweep_stale_bindings` releases a stale binding by clearing `runner_id`
/// but KEEPS the `job_assignments` row and re-creates the pending mark, so
/// the next `pair_runner` must rebind with an upsert and leave the released
/// job in the pool waitlist. A plain INSERT dies on the
/// `job_assignments_pkey` unique constraint and a bare `runner_id = NULL`
/// strands the job outside provisioning.
#[tokio::test]
async fn pair_runner_rebinds_swept_binding() {
    let (_pg, node) = pool_backend().await;
    let run_id = RunId::new();
    let job = JobId("build".to_owned());
    let first = node.register_runner(register_runner("r1")).await.unwrap();
    node.submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
        .await
        .unwrap();
    node.pair_runner(first.runner.id).await.unwrap();
    assert_eq!(
        assigned_runner(&node, run_id, &job).await,
        Some(first.runner.id)
    );

    // Age the binding past the claim-binding TTL, then sweep it: the row
    // survives with a NULL runner and the job re-enters provision_requests.
    {
        let client = node.writer().await.unwrap();
        client
            .execute(
                "UPDATE job_assignments SET assigned_at = now() - interval '10 minutes' \
                 WHERE run_id = $1::text::uuid AND job_id = $2",
                &[&run_id.0.to_string(), &job.0],
            )
            .await
            .unwrap();
    }
    assert!(
        node.sweep_stale_bindings().await.unwrap() > 0,
        "the aged binding must be swept"
    );
    assert_eq!(assigned_runner(&node, run_id, &job).await, None);

    // A second runner registers and pairs: the surviving row is upserted.
    let second = node.register_runner(register_runner("r2")).await.unwrap();
    node.pair_runner(second.runner.id)
        .await
        .expect("pair_runner must rebind the released job");
    assert_eq!(
        assigned_runner(&node, run_id, &job).await,
        Some(second.runner.id)
    );
    assert_eq!(
        node.test_working_set().await.unwrap().released_bindings_count,
        1,
        "only the sweep released a binding"
    );
}

/// `purge_runner` must re-mark a ready job it still owned as pool-pending
/// (trait doc `purge_runner_guarded`): the FK's `ON DELETE SET NULL` only
/// clears the binding, which leaves the job invisible to `pair_runner` and
/// unpairable by provisioning.
#[tokio::test]
async fn purge_returns_ready_assignment_to_pool() {
    let (_pg, node) = pool_backend().await;
    let run_id = RunId::new();
    let job = JobId("build".to_owned());
    let first = node.register_runner(register_runner("r1")).await.unwrap();
    node.submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
        .await
        .unwrap();
    node.pair_runner(first.runner.id).await.unwrap();
    assert_eq!(
        assigned_runner(&node, run_id, &job).await,
        Some(first.runner.id)
    );

    node.purge_runner(first.runner.id).await.unwrap();
    let working = node.test_working_set().await.unwrap();
    assert!(
        working.pool_pending.contains_key(&(run_id, job.clone())),
        "the purged runner's ready job must return to the pool waitlist"
    );

    // A replacement runner pairs straight from the waitlist.
    let second = node.register_runner(register_runner("r2")).await.unwrap();
    node.pair_runner(second.runner.id).await.unwrap();
    assert_eq!(
        assigned_runner(&node, run_id, &job).await,
        Some(second.runner.id)
    );
}

