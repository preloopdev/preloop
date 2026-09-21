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
}

// ── SQLite ──────────────────────────────────────────────────────────────
mod sqlite {
    use super::suite;
    use crate::control::backend::ControlBackend;
    use crate::control::sqlite::SqliteBackend;
    use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};

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
            let backend =
                SqliteBackend::open(&path, false, false, std::time::Duration::from_secs(300))
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
            SqliteBackend::open(&path, false, false, std::time::Duration::from_secs(300)).unwrap();
        let stats = backend.queue_stats().await.unwrap();
        assert_eq!(stats.ready, 1, "queued job must survive reopen");

        let record = backend.run_record(run_id).await.unwrap();
        assert_eq!(record.status, ExecutionStatus::InProgress);
        assert_eq!(
            record.jobs.get(&JobId("build".to_owned())),
            Some(&ExecutionStatus::Queued)
        );

        std::fs::remove_dir_all(&dir).ok();
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
            let backend =
                PostgresBackend::connect(&url, false, false, std::time::Duration::from_secs(300))
                    .await
                    .expect("PRELOOP_TEST_POSTGRES_URL set but connection failed");
            return (None, backend);
        }
        let pg = DisposablePg::start();
        let backend =
            PostgresBackend::connect(&pg.url(), false, false, std::time::Duration::from_secs(300))
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

        let backend =
            PostgresBackend::connect(&url, false, false, std::time::Duration::from_secs(300))
                .await
                .unwrap();
        let stats = backend.queue_stats().await.unwrap();
        assert_eq!(stats.ready, 1, "queued job must survive reconnect");
        let record = backend.run_record(run_id).await.unwrap();
        assert_eq!(record.status, ExecutionStatus::InProgress);
        assert_eq!(
            record.jobs.get(&JobId("build".to_owned())),
            Some(&ExecutionStatus::Queued)
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
}
