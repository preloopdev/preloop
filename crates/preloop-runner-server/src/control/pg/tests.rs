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
//! system bin, or `initdb` on `PATH`). With neither available, tests print a
//! skip notice and pass.

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
    /// Directory holding `initdb`/`pg_ctl` for this cluster.
    bin: PathBuf,
    /// Retains a per-test database created on `PRELOOP_TEST_POSTGRES_URL`.
    /// Without this guard, the temporary database is dropped immediately
    /// after `fresh_database` returns and all backend connections fail.
    database: Option<crate::test_pg::TestDatabase>,
}

impl DisposablePg {
    fn start(bin: PathBuf) -> Self {
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
            bin,
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
/// `None` when no local server binaries are installed — the tests then skip
/// unless `PRELOOP_TEST_POSTGRES_URL` names a server.
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

async fn connect(url: &str) -> PgBackend {
    PgBackend::connect(url, false, false, std::time::Duration::from_secs(300))
        .await
        .expect("test database connection failed")
}

/// Two independent backends (separate writer pools) on ONE database: the
/// shape of two engine nodes sharing a cell, for race tests. `None` when no
/// Postgres is reachable: the caller prints the skip notice and returns.
async fn backend_pair() -> Option<(DisposablePg, PgBackend, PgBackend)> {
    let (guard, url) = fresh_database_opt().await?;
    let first = connect(&url).await;
    let second = connect(&url).await;
    Some((guard, first, second))
}

/// A database nobody else uses plus its cleanup guard, or `None` when no
/// Postgres is reachable (`PRELOOP_TEST_POSTGRES_URL` unset and no local
/// server binaries): tests skip instead of panicking, matching the repo's
/// convention for optional Postgres coverage.
async fn fresh_database_opt() -> Option<(DisposablePg, String)> {
    // A shared server (PRELOOP_TEST_POSTGRES_URL) is honored by the suite
    // harness; the pg unit tests always isolate via a disposable cluster so
    // they also run without configuration.
    if let Some((database, url)) = crate::test_pg::fresh_database().await {
        return Some((
            DisposablePg {
                dir: PathBuf::new(),
                port: 0,
                bin: PathBuf::new(),
                database: Some(database),
            },
            url,
        ));
    }
    let bin = pg_bin_opt()?;
    let pg = DisposablePg::start(bin);
    let url = pg.url();
    Some((pg, url))
}

/// Printed by every Postgres test that cannot reach a server.
fn skip_no_postgres() {
    eprintln!(
        "skipping: set PRELOOP_TEST_POSTGRES_URL to a Postgres server, or install postgresql"
    );
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
    let Some((_pg, node_a, node_b)) = backend_pair().await else {
        return skip_no_postgres();
    };
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
    let Some((_pg, node_a, node_b)) = backend_pair().await else {
        return skip_no_postgres();
    };
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
    let Some((_pg, node_a, node_b)) = backend_pair().await else {
        return skip_no_postgres();
    };
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
    let Some((_pg, node_a, node_b)) = backend_pair().await else {
        return skip_no_postgres();
    };
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
    let Some((_pg, node_a, node_b)) = backend_pair().await else {
        return skip_no_postgres();
    };
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

/// The session's `last_seen_at` as text (microsecond precision), read through
/// the reader pool.
async fn session_last_seen_at(node: &PgBackend, session_id: &str) -> String {
    let uuid = crate::control::logic::session_uuid(session_id).to_string();
    let reader = node.reader().await.expect("reader pool");
    reader
        .query_one(
            "SELECT last_seen_at::text FROM runner_sessions WHERE session_id = $1::text::uuid",
            &[&uuid],
        )
        .await
        .expect("last_seen_at read")
        .get(0)
}

/// Idle polls must not take a writer: with no session message, no active
/// request, no pending cancellation and no ready work, `poll_session`
/// answers from the reader pool and leaves `last_seen_at` untouched.
/// (Previously every poll opened a writer transaction and re-stamped
/// liveness — about five statements per poll, per runner, every few
/// seconds — which is what made thousands of idle long-pollers the
/// dominant writer load.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_poll_leaves_last_seen_at_untouched() {
    let Some((_guard, url)) = fresh_database_opt().await else {
        return skip_no_postgres();
    };
    let node = connect(&url).await;
    let runner = node.register_runner(register_runner("r1")).await.unwrap();
    let session = node
        .create_session(create_session(runner.runner.id))
        .await
        .unwrap();
    let seen_at = session_last_seen_at(&node, &session.session_id).await;
    // A re-stamp would land on a later microsecond; sleep so the assertion
    // below can actually observe one.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let outcome = node
        .poll_session(poll(&session.session_id, runner.runner.id))
        .await
        .unwrap();
    assert!(
        matches!(outcome, PollOutcome::Empty),
        "expected an idle poll, got {outcome:?}"
    );
    assert_eq!(
        session_last_seen_at(&node, &session.session_id).await,
        seen_at,
        "an idle poll must not rewrite last_seen_at"
    );
}

/// The probe must not swallow work: a poll right after a submit still
/// claims through the writer path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probe_poll_still_claims_after_submit() {
    let Some((_guard, url)) = fresh_database_opt().await else {
        return skip_no_postgres();
    };
    let node = connect(&url).await;
    let runner = node.register_runner(register_runner("r1")).await.unwrap();
    let session = node
        .create_session(create_session(runner.runner.id))
        .await
        .unwrap();
    // Sanity: idle first.
    let outcome = node
        .poll_session(poll(&session.session_id, runner.runner.id))
        .await
        .unwrap();
    assert!(matches!(outcome, PollOutcome::Empty));
    // Then work arrives: the probe defers to the writer and the claim lands.
    let run_id = RunId::new();
    node.submit_run(submit_run(run_id, vec![submit_job(run_id, "build", 1)]))
        .await
        .unwrap();
    let outcome = node
        .poll_session(poll(&session.session_id, runner.runner.id))
        .await
        .unwrap();
    let PollOutcome::Claimed(claimed) = outcome else {
        panic!("expected a claim after submit, got {outcome:?}");
    };
    assert_eq!(claimed.queued.job_id, JobId("build".to_owned()));
}

/// Far more concurrent acquires than pooled readers must all complete. An
/// acquire that held one reader while checking out a second deadlocked the
/// pool as soon as every reader was held by an acquire waiting for another —
/// taking every other command (and the webhook inbox) down with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_acquires_beyond_pool_size_complete() {
    let Some((_pg, node, _other)) = backend_pair().await else {
        return skip_no_postgres();
    };
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
    let Some((_pg, node_a, node_b)) = backend_pair().await else {
        return skip_no_postgres();
    };
    let run_id = RunId::new();
    let (request, _runner_id) = submit_and_claim(&node_a, run_id).await;
    let key = format!("{}/{}", request.plan_id, request.timeline_id);

    // No request owns this key: PATCH refuses, GET reads as empty.
    let missing = format!("{}/{}", uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    assert!(matches!(
        node_a
            .patch_timeline(&missing, vec![timeline_record(1, "x")], &[])
            .await,
        Err(ControlError::NotFound(_))
    ));
    let (id, rows) = node_a.get_timeline(&missing, 0, 50).await.unwrap();
    assert_eq!(id, 0);
    assert!(rows.is_empty());

    let (first, _) = node_a
        .patch_timeline(&key, vec![timeline_record(1, "one")], &[])
        .await
        .unwrap();
    let (second, stored) = node_b
        .patch_timeline(
            &key,
            vec![timeline_record(2, "two"), timeline_record(1, "uno")],
            &[],
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
                node.patch_timeline(&key, vec![timeline_record(10 + i as u128, "x")], &[])
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
    let Some((_pg, node_a, _node_b)) = backend_pair().await else {
        return skip_no_postgres();
    };
    let run_id = RunId::new();
    let (request, runner_id) = submit_and_claim(&node_a, run_id).await;
    let key = format!("{}/{}", request.plan_id, request.timeline_id);
    node_a
        .patch_timeline(&key, vec![timeline_record(1, "one")], &[])
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
    let Some((_pg, node_a, _node_b)) = backend_pair().await else {
        return skip_no_postgres();
    };
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
    let Some((_pg, node_a, node_b)) = backend_pair().await else {
        return skip_no_postgres();
    };
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
async fn pool_backend() -> Option<(DisposablePg, PgBackend)> {
    let (guard, url) = fresh_database_opt().await?;
    let backend = PgBackend::connect(&url, true, false, std::time::Duration::from_secs(300))
        .await
        .expect("test database connection failed");
    Some((guard, backend))
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
    let Some((_pg, node)) = pool_backend().await else {
        return skip_no_postgres();
    };
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
        node.test_working_set()
            .await
            .unwrap()
            .released_bindings_count,
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
    let Some((_pg, node)) = pool_backend().await else {
        return skip_no_postgres();
    };
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

/// `get_timeline` answers `(change_id, records)` as one snapshot: a record
/// can never carry a change id greater than the counter returned beside it,
/// because a PATCH bumps the counter in the same commit that stamps its
/// records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeline_reply_is_coherent_under_concurrent_patches() {
    const PATCHES: usize = 120;
    const GETS: usize = 200;
    let Some((_pg, node_a, node_b)) = backend_pair().await else {
        return skip_no_postgres();
    };
    let run_id = RunId::new();
    let (request, _runner_id) = submit_and_claim(&node_a, run_id).await;
    let key = format!("{}/{}", request.plan_id, request.timeline_id);
    node_a
        .patch_timeline(&key, vec![timeline_record(1, "seed")], &[])
        .await
        .unwrap();

    let node_a = Arc::new(node_a);
    let node_b = Arc::new(node_b);
    let patcher = {
        let node = node_a.clone();
        let key = key.clone();
        tokio::spawn(async move {
            for i in 0..PATCHES {
                node.patch_timeline(&key, vec![timeline_record(100 + i as u128, "x")], &[])
                    .await
                    .unwrap();
            }
        })
    };
    let reader = {
        let node = node_b.clone();
        let key = key.clone();
        tokio::spawn(async move {
            let mut incoherent = Vec::new();
            for _ in 0..GETS {
                let (change_id, records) = node.get_timeline(&key, 0, usize::MAX).await.unwrap();
                if records
                    .iter()
                    .any(|record| record.change_id.unwrap_or(0) > change_id)
                {
                    incoherent.push((change_id, records.len()));
                }
            }
            incoherent
        })
    };
    patcher.await.unwrap();
    let incoherent = reader.await.unwrap();
    assert!(
        incoherent.is_empty(),
        "a reply reported a change id older than its own records: {incoherent:?}"
    );
}

/// Archiving moves steps to `step_history`; the step manifest of the
/// archived run must still list them (the live `job_steps` rows are gone
/// with the run).
#[tokio::test]
async fn run_step_manifests_survive_archival() {
    let Some((_pg, node, _other)) = backend_pair().await else {
        return skip_no_postgres();
    };
    let run_id = RunId::new();
    let (request, runner_id) = submit_and_claim(&node, run_id).await;
    let agent = request.agent_job_id;
    let now = chrono::Utc::now().timestamp_micros();
    node.patch_steps(
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
    node.complete_job(JobCompletionInput {
        run_id,
        job_id: JobId("build".to_owned()),
        agent_job_id: Some(agent),
        status: ExecutionStatus::Success,
        outputs: BTreeMap::new(),
        runner_id: Some(runner_id),
    })
    .await
    .unwrap();
    {
        let client = node.writer().await.unwrap();
        client
            .execute(
                "UPDATE runs SET completed_at = now() - interval '10 minutes' \
                 WHERE run_id = $1::text::uuid",
                &[&run_id.0.to_string()],
            )
            .await
            .unwrap();
    }
    assert!(
        node.archive_finished_runs(64)
            .await
            .unwrap()
            .contains(&run_id),
        "the completed run must archive"
    );

    let manifests = node.run_step_manifests(run_id).await.unwrap();
    let steps = manifests
        .get(&agent)
        .expect("the archived attempt keeps its manifest");
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].id, "step-1");
    assert_eq!(steps[0].name, "Build");
}

/// Archiving must not lose a job's check-run id or display name: a
/// push-back retry on an archived run re-creates the check run and
/// mis-names it when the point reads only consult the live tables.
#[tokio::test]
async fn job_point_reads_fall_back_to_archived_rows() {
    let Some((_pg, node, _other)) = backend_pair().await else {
        return skip_no_postgres();
    };
    let run_id = RunId::new();
    let (request, runner_id) = submit_and_claim(&node, run_id).await;
    let job = JobId("build".to_owned());
    node.set_job_check_run(run_id, &job, 4242).await.unwrap();
    node.complete_job(JobCompletionInput {
        run_id,
        job_id: job.clone(),
        agent_job_id: Some(request.agent_job_id),
        status: ExecutionStatus::Success,
        outputs: BTreeMap::new(),
        runner_id: Some(runner_id),
    })
    .await
    .unwrap();
    {
        let client = node.writer().await.unwrap();
        client
            .execute(
                "UPDATE job_specs SET display_name = 'Build (display)' \
                 WHERE run_id = $1::text::uuid AND job_id = $2",
                &[&run_id.0.to_string(), &job.0],
            )
            .await
            .unwrap();
        client
            .execute(
                "UPDATE runs SET completed_at = now() - interval '10 minutes' \
                 WHERE run_id = $1::text::uuid",
                &[&run_id.0.to_string()],
            )
            .await
            .unwrap();
    }
    assert!(
        node.archive_finished_runs(64)
            .await
            .unwrap()
            .contains(&run_id),
        "the completed run must archive"
    );

    assert_eq!(
        node.job_check_run_id(run_id, &job).await.unwrap(),
        Some(4242),
        "the archived check-run mapping must survive"
    );
    assert_eq!(
        node.job_display_name(run_id, &job).await.unwrap(),
        Some("Build (display)".to_owned()),
        "the archived display name must survive"
    );
}

/// A corrupt out-of-range timestamptz reads as absent, matching the SQLite
/// codec (`us_to_utc`), instead of silently claiming 1970.
#[tokio::test]
async fn out_of_range_step_timestamp_reads_as_absent() {
    let Some((_pg, node, _other)) = backend_pair().await else {
        return skip_no_postgres();
    };
    let run_id = RunId::new();
    let (request, _runner_id) = submit_and_claim(&node, run_id).await;
    let agent = request.agent_job_id;
    let now = chrono::Utc::now().timestamp_micros();
    node.patch_steps(
        agent,
        vec![StepPatch {
            id: "step-1".to_owned(),
            name: "Build".to_owned(),
            conclusion: "success".to_owned(),
            started_at_us: Some(now),
            finished_at_us: None,
            observed_us: now,
        }],
    )
    .await
    .unwrap();
    {
        // ~year 280000: storable in `timestamptz`, outside chrono's range.
        let client = node.writer().await.unwrap();
        client
            .execute(
                "UPDATE job_steps SET started_at = to_timestamp(8800000000000) \
                 WHERE agent_job_id = $1::text::uuid AND step_id = $2",
                &[&agent.to_string(), &"step-1"],
            )
            .await
            .unwrap();
    }

    let manifests = node.run_step_manifests(run_id).await.unwrap();
    let steps = manifests.get(&agent).expect("manifest for the attempt");
    assert!(
        steps[0].started_at.is_none(),
        "an out-of-range stored timestamp must read as absent, not 1970"
    );
}

fn gate(group: &str) -> preloop_gha_parser::Concurrency {
    preloop_gha_parser::Concurrency {
        group: group.to_owned(),
        cancel_in_progress: Some("false".to_owned()),
        queue: preloop_gha_parser::ConcurrencyQueue::Single,
    }
}

/// A max-parallel re-park must keep the waiter's FIFO slot and drop the
/// finished holder's row: `park_waiter` re-inserted at the tail and left the
/// group naming a terminal job, so the group was wedged behind a ghost holder.
#[tokio::test]
async fn max_parallel_repark_keeps_fifo_slot_and_releases_group() {
    let Some((_pg, node, _other)) = backend_pair().await else {
        return skip_no_postgres();
    };
    let run = RunId::new();
    // Cohort `m` (max-parallel 2): `a` holds gate `g1`, `aw` waits behind it,
    // `bb` takes the free `g2`; `t1`/`t2` only drive promotion sweeps.
    let mut submit = submit_run(
        run,
        vec![
            submit_job(run, "a", 1),
            submit_job(run, "aw", 2),
            submit_job(run, "bb", 3),
            submit_job(run, "t1", 4),
            submit_job(run, "t2", 5),
        ],
    );
    for job in &mut submit.jobs {
        match job.queued.job_id.0.as_str() {
            "a" | "aw" | "bb" => {
                job.queued.base_id = "m".to_owned();
                job.queued.max_parallel = Some(2);
            }
            _ => {}
        }
    }
    submit.jobs[0].queued.concurrency = Some(gate("g1"));
    submit.jobs[1].queued.concurrency = Some(gate("g1"));
    submit.jobs[2].queued.concurrency = Some(gate("g2"));
    node.submit_run(submit).await.unwrap();

    let complete = |job: &'static str| {
        let node = &node;
        async move {
            node.complete_job(JobCompletionInput {
                run_id: run,
                job_id: JobId(job.to_owned()),
                agent_job_id: None,
                status: ExecutionStatus::Success,
                outputs: BTreeMap::new(),
                runner_id: None,
            })
            .await
            .unwrap();
        }
    };
    // Park `aw` behind `a`.
    complete("t1").await;
    let wait_id: i64 = node
        .writer()
        .await
        .unwrap()
        .query_one(
            "SELECT wait_id FROM concurrency_waits WHERE holder_job_id='aw'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    // Fill the cohort cap with `bb` so `aw`'s promotion finds it saturated.
    complete("t2").await;
    // Completing `a` releases `g1` and re-parks `aw`.
    complete("a").await;

    let hold: Option<i64> = node
        .writer()
        .await
        .unwrap()
        .query_opt(
            "SELECT 1 FROM concurrency_holds \
             WHERE repository='owner/repo' AND group_name='g1'",
            &[],
        )
        .await
        .unwrap()
        .map(|row| row.get(0));
    assert!(
        hold.is_none(),
        "a re-parked waiter must not leave the finished holder's row behind"
    );
    let reparked: Option<i64> = node
        .writer()
        .await
        .unwrap()
        .query_opt(
            "SELECT wait_id FROM concurrency_waits WHERE holder_job_id='aw'",
            &[],
        )
        .await
        .unwrap()
        .map(|row| row.get(0));
    assert_eq!(
        reparked,
        Some(wait_id),
        "the re-parked waiter must keep its FIFO position"
    );
}

/// A promotion that writes the promoted run's rows must take that run's row
/// lock before any group-row lock: the reverse order deadlocked (40P01)
/// against a command on the promoted run and clobbered its rows with a stale
/// snapshot.
#[tokio::test]
async fn promotion_takes_the_promoted_runs_row_lock_first() {
    let Some((_pg, node_a, node_b)) = backend_pair().await else {
        return skip_no_postgres();
    };
    let wf = |group: &str| crate::control::types::WorkflowConcurrency {
        group: group.to_owned(),
        cancel_in_progress: false,
        queue: preloop_gha_parser::ConcurrencyQueue::Single,
        raw: gate(group),
    };
    // R1 holds group `g`; R2 waits behind it.
    let run_1 = RunId::new();
    let mut submit_1 = submit_run(run_1, vec![submit_job(run_1, "build", 1)]);
    submit_1.workflow_concurrency = Some(wf("g"));
    node_a.submit_run(submit_1).await.unwrap();
    let run_2 = RunId::new();
    let mut submit_2 = submit_run(run_2, vec![submit_job(run_2, "build", 2)]);
    submit_2.record.run_number = 2;
    submit_2.workflow_concurrency = Some(wf("g"));
    assert!(node_a.submit_run(submit_2).await.unwrap().held);

    // A command on R2 (its run row locked) runs while R1's cancellation
    // releases `g` and promotes R2.
    let conn = node_b.writer().await.unwrap();
    conn.batch_execute("BEGIN").await.unwrap();
    conn.query_one(
        "SELECT 1 FROM runs WHERE run_id=$1::text::uuid FOR NO KEY UPDATE",
        &[&run_2.0.to_string()],
    )
    .await
    .unwrap();
    let cancel = node_a.cancel_run(run_1, None);
    let delete = async {
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        conn.execute(
            "DELETE FROM concurrency_waits WHERE holder_run_id=$1::text::uuid",
            &[&run_2.0.to_string()],
        )
        .await
        .expect(
            "the releasing transaction must not hold R2's wait row while it waits for R2's run row",
        );
        conn.batch_execute("COMMIT").await.unwrap();
    };
    let (cancel, ()) = tokio::join!(cancel, delete);
    cancel.expect("the promotion must complete once R2's row lock is released");
}

/// Cancel-in-progress displacement locks the run it will cancel before the
/// group's hold row. Otherwise it deadlocks against a command that already
/// holds that run row and next needs the group.
#[tokio::test]
async fn gate_displacement_locks_the_cancelled_run_before_the_group() {
    let Some((_pg, node_a, node_b)) = backend_pair().await else {
        return skip_no_postgres();
    };
    let wf = || crate::control::types::WorkflowConcurrency {
        group: "g".to_owned(),
        cancel_in_progress: true,
        queue: preloop_gha_parser::ConcurrencyQueue::Single,
        raw: gate("g"),
    };
    let holder = RunId::new();
    let mut first = submit_run(holder, vec![submit_job(holder, "build", 1)]);
    first.workflow_concurrency = Some(wf());
    node_a.submit_run(first).await.unwrap();

    let conn = node_b.writer().await.unwrap();
    conn.batch_execute("BEGIN").await.unwrap();
    conn.query_one(
        "SELECT 1 FROM runs WHERE run_id=$1::text::uuid FOR NO KEY UPDATE",
        &[&holder.0.to_string()],
    )
    .await
    .unwrap();

    let arrival = RunId::new();
    let mut second = submit_run(arrival, vec![submit_job(arrival, "build", 2)]);
    second.record.run_number = 2;
    second.workflow_concurrency = Some(wf());
    let submit = node_a.submit_run(second);
    let group_lock = async {
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        conn.query_one(
            "SELECT 1 FROM concurrency_holds \
             WHERE namespace_id='default' AND repository='owner/repo' \
               AND group_name='g' FOR UPDATE",
            &[],
        )
        .await
        .expect("the arrival must not hold the group row while waiting for the prior run row");
        conn.batch_execute("COMMIT").await.unwrap();
    };
    let (submitted, ()) = tokio::join!(submit, group_lock);
    let submitted = submitted.expect("displacement completes after the run-row lock is released");
    assert_eq!(submitted.run_id, arrival);
    assert_eq!(
        node_a.run_record(holder).await.unwrap().status,
        ExecutionStatus::Cancelled
    );
}

/// The outbox reader never skips a row. While a transaction that already
/// wrote an outbox row is open, rows committed after it stay behind the safe
/// point; once it commits, both arrive and the older transaction's row comes
/// first. Each row names the node that wrote it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbox_reader_does_not_pass_an_open_transaction() {
    let Some((_pg, node_a, node_b)) = backend_pair().await else {
        return skip_no_postgres();
    };
    assert_ne!(node_a.origin(), node_b.origin(), "one origin per node");
    let head = node_b.outbox_head().await.unwrap();

    let mut held = node_a.writer().await.unwrap();
    let tx = held.transaction().await.unwrap();
    super::dispatch::emit_outbox(&tx, None, "held.v1", serde_json::json!({"n": 1}))
        .await
        .unwrap();
    // Committed after the open transaction's row was written (and so after it
    // took its txid).
    node_b
        .append_event(&preloop_gha_protocol::NdjsonEvent::RunAccepted {
            run_id: RunId::new(),
            queued_jobs: 1,
        })
        .await
        .unwrap();
    let during = node_b.outbox_read(head, 100).await.unwrap();
    assert!(
        during.is_empty(),
        "rows behind an open transaction are not readable yet: {during:?}"
    );

    tx.commit().await.unwrap();
    drop(held);
    // Another database's long transaction on a shared server can hold the safe
    // point back for a moment: wait for it rather than assume it is quick.
    let mut rows = Vec::new();
    for _ in 0..100 {
        rows = node_b.outbox_read(head, 100).await.unwrap();
        if rows.len() >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let topics: Vec<&str> = rows.iter().map(|row| row.topic.as_str()).collect();
    assert_eq!(topics, ["held.v1", "run_accepted.v1"]);
    assert_eq!(rows[0].origin, node_a.origin());
    assert_eq!(rows[1].origin, node_b.origin());
    let key = |row: &crate::control::types::OutboxRow| (row.bookmark.txid, row.bookmark.event_id);
    assert!(key(&rows[0]) < key(&rows[1]));
    let after = node_b.outbox_read(rows[1].bookmark, 100).await.unwrap();
    assert!(
        after.is_empty(),
        "nothing past the last bookmark: {after:?}"
    );
}

// ── queue gauge plans ───────────────────────────────────────────────────

/// The global gauge reads (`queue_stats`, the cancel path's front gauge,
/// `rebuild_dispatch_intent`, the reaper's ready scans) must be index-fed by
/// `jobs_ready_global` — no Sort node — on a populated ready queue; the
/// claim-order reads keep using `jobs_ready`.
#[tokio::test]
async fn queue_gauge_plans_are_index_fed() {
    let Some((_pg, url)) = fresh_database_opt().await else {
        return skip_no_postgres();
    };
    let node = connect(&url).await;
    // A populated ready queue: four runs of 64 jobs.
    for batch in 0..4u64 {
        let run_id = RunId::new();
        let jobs = (0..64)
            .map(|i| {
                submit_job(
                    run_id,
                    &format!("job-{batch}-{i:03}"),
                    (batch * 64 + i) as i64,
                )
            })
            .collect();
        let mut submit = submit_run(run_id, jobs);
        submit.record.run_number = batch + 1;
        node.submit_run(submit).await.unwrap();
    }

    let client = node.reader().await.unwrap();
    client.batch_execute("ANALYZE jobs").await.unwrap();
    for sql in [
        "SELECT runs_on::text FROM jobs WHERE queue_state='ready' \
         ORDER BY priority DESC, run_order, job_order LIMIT 1",
        "SELECT runs_on::text FROM jobs WHERE queue_state='ready' \
         ORDER BY priority DESC, run_order, job_order, run_id, job_id LIMIT 1",
    ] {
        let plan: String = client
            .query(&format!("EXPLAIN {sql}"), &[])
            .await
            .unwrap()
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan.contains("Index Scan using jobs_ready_global"),
            "the gauge plan must use the global index:\n{plan}"
        );
        assert!(
            !plan.contains("Sort"),
            "the gauge plan must not sort:\n{plan}"
        );
    }
    let plan: String = client
        .query(
            "EXPLAIN SELECT job_id FROM jobs WHERE queue_state='ready' \
             ORDER BY pool_key, priority DESC, run_order, job_order LIMIT 64",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains("Index Scan using jobs_ready on jobs"),
        "the claim order must keep using jobs_ready:\n{plan}"
    );
}

/// A `runs_on` that is valid JSONB but not a string list must not abort the
/// gauges: the cancel still reports the ready set, with no front labels.
/// (The JSONB column rejects malformed JSON, so only the shape can surprise.)
#[tokio::test]
async fn non_list_runs_on_does_not_abort_gauges() {
    let Some((_pg, url)) = fresh_database_opt().await else {
        return skip_no_postgres();
    };
    let node = connect(&url).await;
    let run_id = RunId::new();
    node.submit_run(submit_run(
        run_id,
        vec![
            submit_job(run_id, "alpha", 1),
            submit_job(run_id, "beta", 2),
        ],
    ))
    .await
    .unwrap();
    // The front job's labels become a JSON object — valid JSONB, not a list.
    node.writer()
        .await
        .unwrap()
        .execute(
            "UPDATE jobs SET runs_on = '{\"label\":1}'::jsonb \
             WHERE run_id=$1::text::uuid AND job_id='alpha'",
            &[&run_id.0.to_string()],
        )
        .await
        .unwrap();

    // The gauge read tolerates it: the front labels come back empty
    // instead of failing. (The cancel command itself walks the ready set for
    // the promotion sweep, where a non-list `runs_on` is a different,
    // admission-side question — this test pins the gauge contract.)
    let front = node.ready_front_labels().await.unwrap();
    assert!(
        front.is_empty(),
        "a non-list runs_on yields no labels, not an error: {front:?}"
    );
}
