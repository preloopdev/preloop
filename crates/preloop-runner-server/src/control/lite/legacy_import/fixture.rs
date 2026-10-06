//! A populated legacy v11 fixture, written from the released migration chain.
//!
//! The schema is the exact DDL of the legacy store's migrations 1..=11 (the
//! released v11 chain), not a reverse-engineering of the current schema; the
//! blobs are sealed with the same `Envelope` and JSON shapes the legacy store
//! wrote (`run_record_value`, `serde_json` of the current record types, raw
//! log chunks, raw step-name seal, sealed request snapshots). Lives under
//! `test-support` so the importer's integration tests can build a realistic
//! source without touching any operator's real `preloop.db`.

use crate::control::lite::legacy_import::legacy::LegacyRequestSnapshot;
use crate::models::{JobDetail, QueuedJob, RunRecord};
use crate::store::Envelope;
use anyhow::Context;
use preloop_gha_protocol::azdo::AgentJobRequestMessage;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId, SecretString, WorkflowSubmission};
use rusqlite::{Connection, params};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

/// Verbatim released v11 migration SQL. See the file header for provenance
/// (commit `5e7ec6ed^`, `const MIGRATIONS`); each body's sha256 is pinned in
/// [`LEGACY_MIGRATION_SHA256`] and re-checked by a unit test, so the
/// checked-in fixture cannot drift from the source it was copied from.
pub(crate) const LEGACY_MIGRATIONS_SQL: &str = include_str!("legacy_v11_migrations.sql");

/// sha256 of every migration body in [`LEGACY_MIGRATIONS_SQL`].
pub(crate) const LEGACY_MIGRATION_SHA256: &[(i64, &str, &str)] = &[
    (
        1,
        "initial-control-plane-schema",
        "b3ef13699b558193741e6202a92752b12a3272bbadb1a9d74bfca5f1b729fde3",
    ),
    (
        2,
        "drop-redundant-run-secrets",
        "bbd381e8f5b15cdbb01a2c20fb65f133ee04a1cb5e0997a3c4c904aa4f626730",
    ),
    (
        3,
        "job-request-messages-table",
        "12f1e391ba7fc09d529b291b2ba309068b75ec08fbbb0c303ddd12b77e39c5b7",
    ),
    (
        4,
        "job-steps-table",
        "ebafb32a0fd83899ecd1f7a37c9411f932b5be5f64ec6d3639c497e10f59b124",
    ),
    (
        5,
        "runtime-snapshot-revision",
        "61c785b8b807bd81a421b2a0ec9c36245fd408cb3bcf5949a6158620c42cfadf",
    ),
    (
        6,
        "webhook-deliveries-table",
        "1cf220db65a715a6d2d4105a6e4299579ceb6490672e405df11562560e410a93",
    ),
    (
        7,
        "webhook-delivery-lease-fencing",
        "e6d50ffdfd6f400b9b8ba8ffc7e0d3601d9d4602614b5e39dfc11172b1dc3b80",
    ),
    (
        8,
        "webhook-run-reservation",
        "e81ffb463b8cfaec41f0206e4fe02c33a9a6737de0e009c0aa1d54210c9eea80",
    ),
    (
        9,
        "webhook-delivery-repair-state",
        "bcddb9cf61dbc5fa9693dd7e1c8206b853e377e1d2b9ac461703ccccf6e2bce6",
    ),
    (
        10,
        "drop-source-state-reconciler",
        "a39cfc979f434c5937774e94c877b04b36fc47d6cb7668808e788c857df557bb",
    ),
    (
        11,
        "webhook-watchdog-safe-pagination",
        "1350508f628fef52026a2e3f977d9b779321daf16d591b3c7894c6c9f910f63c",
    ),
];

/// Parse the marker-delimited migration file into `(version, name, body)`.
pub(crate) fn legacy_migrations() -> Vec<(i64, String, String)> {
    let mut out = Vec::new();
    let mut lines = LEGACY_MIGRATIONS_SQL.lines().peekable();
    while let Some(line) = lines.next() {
        let Some(rest) = line.strip_prefix("--@@migration ") else {
            continue;
        };
        let Some(rest) = rest.strip_suffix("@@") else {
            continue;
        };
        let (version, name) = rest.split_once(' ').expect("migration marker shape");
        let version: i64 = version.parse().expect("migration version");
        let mut body_lines: Vec<&str> = Vec::new();
        while let Some(next) = lines.peek() {
            if next.starts_with("--@@migration ") {
                break;
            }
            body_lines.push(lines.next().expect("peeked line"));
        }
        out.push((version, name.to_owned(), body_lines.join("\n")));
    }
    out
}

/// Knobs the tests flip to exercise refusal paths.
#[derive(Debug, Clone)]
pub struct LegacyFixtureSpec {
    /// `PRAGMA user_version` to stamp. Defaults to the released v11.
    pub user_version: i64,
    /// Include the run with a claimed-but-unfinished attempt.
    pub include_active_claim: bool,
    /// The secret value submitted with the successful run.
    pub secret_value: String,
    /// Add a metadata key this importer does not know (refusal test).
    pub unknown_meta_key: bool,
    /// Add a legacy in-flight protocol frame (fail-closed refusal test).
    pub include_inflight_frame: bool,
}

impl Default for LegacyFixtureSpec {
    fn default() -> Self {
        Self {
            user_version: 11,
            include_active_claim: true,
            secret_value: "hunter2-canary-value".to_owned(),
            unknown_meta_key: false,
            include_inflight_frame: false,
        }
    }
}

/// Identifiers the tests assert against.
#[derive(Debug, Clone)]
pub struct LegacyFixture {
    pub run_ok: String,
    pub run_queued: String,
    pub run_active: String,
    pub agent_build: String,
    pub agent_active: String,
    pub request_build: i64,
    pub request_active: i64,
    pub secret_name: String,
    pub secret_value: String,
    pub log_bytes: Vec<u8>,
    pub log_key: String,
    pub check_run_id: u64,
    pub workflow_path: String,
    pub repository: String,
}

const RUN_OK: &str = "11111111-1111-4111-8111-111111111111";
const RUN_QUEUED: &str = "22222222-2222-4222-8222-222222222222";
const RUN_ACTIVE: &str = "33333333-3333-4333-8333-333333333333";
const AGENT_BUILD: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const AGENT_TEST: &str = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const AGENT_LINT: &str = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
const AGENT_DEPLOY: &str = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
const AGENT_RELEASE: &str = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
const REQUEST_BUILD: i64 = 1;
const REQUEST_TEST: i64 = 2;
const REQUEST_LINT: i64 = 3;
const REQUEST_DEPLOY: i64 = 4;
const REQUEST_RELEASE: i64 = 5;
const RUNNER_CLOSED: i64 = 6;
const RUNNER_OPEN: i64 = 7;
const SESSION_CLOSED: &str = "0f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f";
const SESSION_OPEN: &str = "1e1e1e1e-1e1e-4e1e-8e1e-1e1e1e1e1e1e";
const CHECK_RUN_ID: u64 = 4242;
const BASE_US: i64 = 1_700_000_000_000_000;
const WORKFLOW_PATH: &str = ".github/workflows/ci.yml";
const REPOSITORY: &str = "acme/widgets";

fn timeline_of(request_id: i64) -> uuid::Uuid {
    uuid::Uuid::from_u128(0x7e57_0000_0000_0000_0000_0000_0000_0000 + request_id as u128)
}

fn iso_us(us: i64) -> String {
    chrono::DateTime::from_timestamp_micros(us)
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Write the populated fixture database at `path`.
pub fn write_legacy_fixture(
    path: &Path,
    key: &[u8],
    spec: &LegacyFixtureSpec,
) -> anyhow::Result<LegacyFixture> {
    let cipher = Envelope::new(key);
    let mut conn = Connection::open(path)?;
    apply_legacy_schema(&mut conn, spec.user_version)?;
    let fixture = LegacyFixture {
        run_ok: RUN_OK.to_owned(),
        run_queued: RUN_QUEUED.to_owned(),
        run_active: RUN_ACTIVE.to_owned(),
        agent_build: AGENT_BUILD.to_owned(),
        agent_active: AGENT_RELEASE.to_owned(),
        request_build: REQUEST_BUILD,
        request_active: REQUEST_RELEASE,
        secret_name: "CANARY".to_owned(),
        secret_value: spec.secret_value.clone(),
        log_bytes: b"line one\nline two\n".to_vec(),
        log_key: format!("results:{AGENT_BUILD}:build:job:{AGENT_BUILD}"),
        check_run_id: CHECK_RUN_ID,
        workflow_path: WORKFLOW_PATH.to_owned(),
        repository: REPOSITORY.to_owned(),
    };
    let tx = conn.transaction()?;
    insert_runs(&tx, &cipher, spec, &fixture)?;
    insert_jobs(&tx, &cipher, spec)?;
    insert_attempts(&tx, &cipher, spec)?;
    insert_steps(&tx, &cipher, spec)?;
    insert_logs(&tx, &fixture)?;
    insert_runners(&tx)?;
    insert_webhooks(&tx, &cipher)?;
    insert_control_events(&tx, spec)?;
    insert_job_request_message(&tx, spec)?;
    insert_meta(&tx, &cipher, spec, &fixture)?;
    tx.commit()?;
    // The legacy node keeps the finalized artifact registry both in the
    // metadata snapshot and in a JSON sidecar next to the database; the
    // importer reads whichever is present.
    if let Some(parent) = path.parent() {
        std::fs::write(
            parent.join("artifact_v2_registry.json"),
            serde_json::json!({
                "acme/widgets|build|logs": {
                    "id": 11,
                    "workflow_run_backend_id": RUN_OK,
                    "workflow_job_run_backend_id": AGENT_BUILD,
                    "name": "logs",
                    "size": 4096,
                    "created_at": "2026-09-14T12:00:00Z",
                    "digest": "sha256:abc",
                    "blob_token": "blob-token-11"
                }
            })
            .to_string(),
        )?;
    }
    Ok(fixture)
}

fn apply_legacy_schema(conn: &mut Connection, user_version: i64) -> anyhow::Result<()> {
    let migrations = legacy_migrations();
    for (version, name, ddl) in &migrations {
        conn.execute_batch(ddl)
            .with_context(|| format!("apply legacy migration {version} ({name})"))?;
    }
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
          version INTEGER PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          applied_at_us INTEGER NOT NULL
        ) STRICT;",
    )?;
    let tx = conn.transaction()?;
    for (version, name, _) in &migrations {
        tx.execute(
            "INSERT INTO schema_migrations(version, name, applied_at_us) VALUES (?1, ?2, ?3)",
            params![version, name, BASE_US],
        )?;
    }
    tx.execute_batch(&format!("PRAGMA user_version = {user_version}"))?;
    tx.commit()?;
    Ok(())
}

fn submission(spec: &LegacyFixtureSpec) -> WorkflowSubmission {
    let mut secrets: preloop_gha_protocol::SecretMap = BTreeMap::new();
    secrets.insert(
        "CANARY".to_owned(),
        SecretString::new(spec.secret_value.clone()),
    );
    WorkflowSubmission {
        workflow_yaml:
            "name: ci\non: workflow_dispatch\njobs:\n  build:\n    runs-on: self-hosted\n"
                .to_owned(),
        event: "workflow_dispatch".to_owned(),
        payload: serde_json::json!({"ref": "refs/heads/main"}),
        repository: REPOSITORY.to_owned(),
        git_ref: "refs/heads/main".to_owned(),
        workflow_path: Some(WORKFLOW_PATH.to_owned()),
        sha: "cafebabecafebabecafebabecafebabecafebabe".to_owned(),
        actor: "octocat".to_owned(),
        secrets,
        ..Default::default()
    }
}

pub(crate) fn base_record(run_id: &str, run_number: u64, status: ExecutionStatus) -> RunRecord {
    RunRecord {
        run_id: RunId(run_id.parse().unwrap()),
        webhook_delivery_id: None,
        run_name: Some("ci".to_owned()),
        submission: Arc::new(WorkflowSubmission {
            repository: REPOSITORY.to_owned(),
            event: "workflow_dispatch".to_owned(),
            git_ref: "refs/heads/main".to_owned(),
            sha: "cafebabecafebabecafebabecafebabecafebabe".to_owned(),
            actor: "octocat".to_owned(),
            workflow_path: Some(WORKFLOW_PATH.to_owned()),
            ..Default::default()
        }),
        jobs: BTreeMap::new(),
        status,
        job_outputs: BTreeMap::new(),
        job_base_ids: BTreeMap::new(),
        job_needs: BTreeMap::new(),
        caller_plans: BTreeMap::new(),
        job_names: BTreeMap::new(),
        github: serde_json::json!({"ref": "refs/heads/main", "event_name": "workflow_dispatch"}),
        head_sha: "cafebabecafebabecafebabecafebabecafebabe".to_owned(),
        workflow_ref: format!("{REPOSITORY}/{WORKFLOW_PATH}@refs/heads/main"),
        workspace_snapshot: None,
        job_fail_fast: BTreeMap::new(),
        job_continue_on_error: BTreeMap::new(),
        job_check_run_ids: BTreeMap::new(),
        reusable_calls: BTreeMap::new(),
        jobs_list: Vec::new(),
        created_at: chrono::DateTime::from_timestamp_micros(BASE_US).unwrap(),
        started_at: None,
        completed_at: None,
        run_number,
        run_attempt: 1,
        workflow_path_str: WORKFLOW_PATH.to_owned(),
        event: "workflow_dispatch".to_owned(),
        conclusion: None,
        push_state: None,
        snapshot_timing: None,
        fork_approval_pending: false,
        fork_approval_requested_at_unix_nanos: None,
        fork_approved_at_unix_nanos: None,
        fork_approval_note: None,
        reports_check_runs: true,
    }
}

fn insert_run(
    tx: &rusqlite::Transaction<'_>,
    cipher: &Envelope,
    record: &RunRecord,
) -> anyhow::Result<()> {
    let value = crate::store::run_record_value(record)?;
    let blob = cipher.seal(&serde_json::to_vec(&value)?)?;
    tx.execute(
        "INSERT INTO runs(run_id, repository, workflow_path, status, run_number, run_attempt, \
             created_at_us, completed_at_us, record_blob, webhook_delivery_id) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            record.run_id.0.to_string(),
            record.submission.repository,
            record.workflow_path_str,
            crate::control::types::status_str(record.status),
            record.run_number as i64,
            record.run_attempt as i64,
            record.created_at.timestamp_micros(),
            record.completed_at.map(|at| at.timestamp_micros()),
            blob,
            record.webhook_delivery_id,
        ],
    )?;
    Ok(())
}

fn insert_runs(
    tx: &rusqlite::Transaction<'_>,
    cipher: &Envelope,
    spec: &LegacyFixtureSpec,
    _fixture: &LegacyFixture,
) -> anyhow::Result<()> {
    // ── Completed run: two jobs, secrets, check id, outputs, annotations.
    let mut ok = base_record(RUN_OK, 1, ExecutionStatus::Success);
    ok.submission = Arc::new(submission(spec));
    ok.jobs = BTreeMap::from([
        (JobId("build".to_owned()), ExecutionStatus::Success),
        (JobId("test".to_owned()), ExecutionStatus::Failure),
    ]);
    ok.job_names = BTreeMap::from([
        (JobId("build".to_owned()), "build".to_owned()),
        (JobId("test".to_owned()), "test".to_owned()),
    ]);
    ok.job_base_ids = BTreeMap::from([
        (JobId("build".to_owned()), "build".to_owned()),
        (JobId("test".to_owned()), "test".to_owned()),
    ]);
    ok.job_needs = BTreeMap::from([
        (JobId("build".to_owned()), Vec::new()),
        (JobId("test".to_owned()), vec![JobId("build".to_owned())]),
    ]);
    ok.job_outputs = BTreeMap::from([(
        JobId("build".to_owned()),
        BTreeMap::from([("artifact".to_owned(), serde_json::json!("ok"))]),
    )]);
    ok.job_check_run_ids = BTreeMap::from([(JobId("build".to_owned()), CHECK_RUN_ID)]);
    ok.jobs_list = vec![
        JobDetail {
            job_id: "build".to_owned(),
            name: "build".to_owned(),
            conclusion: "success".to_owned(),
            steps: Vec::new(),
            annotations: vec![serde_json::json!({"message": "kept annotation"})],
        },
        JobDetail {
            job_id: "test".to_owned(),
            name: "test".to_owned(),
            conclusion: "failure".to_owned(),
            steps: Vec::new(),
            annotations: Vec::new(),
        },
    ];
    ok.started_at = chrono::DateTime::from_timestamp_micros(BASE_US + 1_000_000);
    ok.completed_at = chrono::DateTime::from_timestamp_micros(BASE_US + 60_000_000);
    ok.conclusion = Some("success".to_owned());
    insert_run(tx, cipher, &ok)?;

    // ── Queued run: a ready job and a needs-gated job.
    let mut queued = base_record(RUN_QUEUED, 2, ExecutionStatus::Queued);
    queued.jobs = BTreeMap::from([
        (JobId("lint".to_owned()), ExecutionStatus::Queued),
        (JobId("deploy".to_owned()), ExecutionStatus::Queued),
    ]);
    queued.job_names = BTreeMap::from([
        (JobId("lint".to_owned()), "lint".to_owned()),
        (JobId("deploy".to_owned()), "deploy".to_owned()),
    ]);
    queued.job_base_ids = BTreeMap::from([
        (JobId("lint".to_owned()), "lint".to_owned()),
        (JobId("deploy".to_owned()), "deploy".to_owned()),
    ]);
    queued.job_needs = BTreeMap::from([
        (JobId("lint".to_owned()), Vec::new()),
        (JobId("deploy".to_owned()), vec![JobId("lint".to_owned())]),
    ]);
    queued.jobs_list = vec![
        JobDetail {
            job_id: "lint".to_owned(),
            name: "lint".to_owned(),
            conclusion: "pending".to_owned(),
            steps: Vec::new(),
            annotations: Vec::new(),
        },
        JobDetail {
            job_id: "deploy".to_owned(),
            name: "deploy".to_owned(),
            conclusion: "pending".to_owned(),
            steps: Vec::new(),
            annotations: Vec::new(),
        },
    ];
    insert_run(tx, cipher, &queued)?;

    // ── Run with a claimed-but-unfinished attempt.
    if spec.include_active_claim {
        let mut active = base_record(RUN_ACTIVE, 3, ExecutionStatus::InProgress);
        active.jobs = BTreeMap::from([(JobId("release".to_owned()), ExecutionStatus::InProgress)]);
        active.job_names = BTreeMap::from([(JobId("release".to_owned()), "release".to_owned())]);
        active.job_base_ids = BTreeMap::from([(JobId("release".to_owned()), "release".to_owned())]);
        active.job_needs = BTreeMap::from([(JobId("release".to_owned()), Vec::new())]);
        active.jobs_list = vec![JobDetail {
            job_id: "release".to_owned(),
            name: "release".to_owned(),
            conclusion: "in_progress".to_owned(),
            steps: Vec::new(),
            annotations: Vec::new(),
        }];
        active.started_at = chrono::DateTime::from_timestamp_micros(BASE_US + 120_000_000);
        insert_run(tx, cipher, &active)?;
    }
    Ok(())
}

fn agent_for(job: &str) -> &'static str {
    match job {
        "build" => AGENT_BUILD,
        "test" => AGENT_TEST,
        "lint" => AGENT_LINT,
        "deploy" => AGENT_DEPLOY,
        "release" => AGENT_RELEASE,
        _ => AGENT_BUILD,
    }
}

fn message_json(
    job: &str,
    agent: &str,
    request_id: i64,
    secret_value: &str,
) -> AgentJobRequestMessage {
    serde_json::from_value(serde_json::json!({
        "jobId": agent,
        "requestId": request_id,
        "plan": {"planId": agent, "planType": "actions", "version": 1,
                 "artifactUri": "", "artifactLocation": ""},
        "timeline": {"id": timeline_of(request_id), "changeId": 0, "location": null},
        "jobName": job,
        "jobDisplayName": job,
        "lockedUntil": "",
        "resources": {"endpoints": []},
        "variables": {
            "CANARY": {"value": secret_value, "isSecret": true},
            "github_token": {"value": "", "isSecret": true},
            "GITHUB_WORKSPACE": {"value": "/work"}
        },
        "maskHints": [
            {"type": "regex", "value": secret_value}
        ],
        "steps": [],
        "snapshot": null
    }))
    .expect("fixture job message shape is fixed")
}

fn queued_job(
    run_id: &str,
    job: &str,
    request_id: i64,
    needs: &[&str],
    secret_value: &str,
) -> QueuedJob {
    QueuedJob {
        run_id: RunId(run_id.parse().unwrap()),
        job_id: JobId(job.to_owned()),
        base_id: job.to_owned(),
        created_at_unix_nanos: BASE_US * 1000,
        dependencies_ready_at_unix_nanos: Some((BASE_US + 1_000_000) * 1000),
        concurrency_wait_started_at_unix_nanos: None,
        concurrency_acquired_at_unix_nanos: None,
        enqueued_at_unix_nanos: (BASE_US + 2_000_000) * 1000,
        needs: needs.iter().map(|need| JobId((*need).to_owned())).collect(),
        if_condition: None,
        condition_context: preloop_gha_expressions::Context::default(),
        max_parallel: None,
        runs_on: vec!["self-hosted".to_owned()],
        runner_group: None,
        message: message_json(job, agent_for(job), request_id, secret_value),
        environment: None,
        concurrency: None,
        matrix: BTreeMap::new(),
        deferred_matrix: None,
        reusable_call: None,
        environment_gate: None,
    }
}

fn insert_job(
    tx: &rusqlite::Transaction<'_>,
    cipher: &Envelope,
    job: &QueuedJob,
    status: ExecutionStatus,
    queue_kind: &str,
    position: i64,
) -> anyhow::Result<()> {
    let blob = cipher.seal(&serde_json::to_vec(job)?)?;
    tx.execute(
        "INSERT INTO jobs(run_id, job_id, status, queue_kind, queue_position, payload_blob) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            job.run_id.0.to_string(),
            job.job_id.0,
            crate::control::types::status_str(status),
            queue_kind,
            position,
            blob,
        ],
    )?;
    for need in &job.needs {
        tx.execute(
            "INSERT INTO job_dependencies(run_id, job_id, depends_on_job_id) VALUES (?1, ?2, ?3)",
            params![job.run_id.0.to_string(), job.job_id.0, need.0],
        )?;
    }
    Ok(())
}

fn insert_jobs(
    tx: &rusqlite::Transaction<'_>,
    cipher: &Envelope,
    spec: &LegacyFixtureSpec,
) -> anyhow::Result<()> {
    // The completed run's jobs are terminal and removed from the queue (the
    // legacy store deletes rows for jobs that left the queue).
    insert_job(
        tx,
        cipher,
        &queued_job(RUN_QUEUED, "lint", REQUEST_LINT, &[], &spec.secret_value),
        ExecutionStatus::Queued,
        "ready",
        0,
    )?;
    insert_job(
        tx,
        cipher,
        &queued_job(
            RUN_QUEUED,
            "deploy",
            REQUEST_DEPLOY,
            &["lint"],
            &spec.secret_value,
        ),
        ExecutionStatus::Queued,
        "pending",
        1,
    )?;
    if spec.include_active_claim {
        insert_job(
            tx,
            cipher,
            &queued_job(
                RUN_ACTIVE,
                "release",
                REQUEST_RELEASE,
                &[],
                &spec.secret_value,
            ),
            ExecutionStatus::InProgress,
            "ready",
            2,
        )?;
    }
    Ok(())
}

fn request_snapshot(
    run_id: &str,
    job: &str,
    agent: &str,
    request_id: i64,
    result: Option<ExecutionStatus>,
    claimed: Option<(i64, i64)>,
    locked_until: &str,
) -> LegacyRequestSnapshot {
    let agent: uuid::Uuid = agent.parse().unwrap();
    LegacyRequestSnapshot {
        request_id,
        run_id: RunId(run_id.parse().unwrap()),
        job_id: JobId(job.to_owned()),
        agent_job_id: agent,
        plan_id: agent.to_string(),
        plan_type: "actions".to_owned(),
        timeline_id: timeline_of(request_id),
        result,
        locked_until: locked_until.to_owned(),
        claimed_at_us: claimed.map(|(at, _)| at),
        owner_runner_id: claimed.map(|(_, runner)| runner),
        started_at_us: claimed.map(|(at, _)| at),
        last_renewed_at_us: claimed.map(|(at, _)| at + 100_000),
        timeout_triggered: false,
        debug_token_issued: false,
    }
}

fn insert_attempt(
    tx: &rusqlite::Transaction<'_>,
    cipher: &Envelope,
    snapshot: &LegacyRequestSnapshot,
    state: &str,
) -> anyhow::Result<()> {
    let blob = cipher.seal(&serde_json::to_vec(snapshot)?)?;
    tx.execute(
        "INSERT INTO job_requests(request_id, run_id, job_id, agent_job_id, plan_id, \
             timeline_id, state, request_blob) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            snapshot.request_id,
            snapshot.run_id.0.to_string(),
            snapshot.job_id.0,
            snapshot.agent_job_id.to_string(),
            snapshot.plan_id,
            snapshot.timeline_id.to_string(),
            state,
            blob,
        ],
    )?;
    Ok(())
}

fn insert_attempts(
    tx: &rusqlite::Transaction<'_>,
    cipher: &Envelope,
    spec: &LegacyFixtureSpec,
) -> anyhow::Result<()> {
    insert_attempt(
        tx,
        cipher,
        &request_snapshot(
            RUN_OK,
            "build",
            AGENT_BUILD,
            REQUEST_BUILD,
            Some(ExecutionStatus::Success),
            Some((BASE_US + 10_000_000, RUNNER_CLOSED)),
            &iso_us(BASE_US + 50_000_000),
        ),
        "active",
    )?;
    insert_attempt(
        tx,
        cipher,
        &request_snapshot(
            RUN_OK,
            "test",
            AGENT_TEST,
            REQUEST_TEST,
            Some(ExecutionStatus::Failure),
            Some((BASE_US + 30_000_000, RUNNER_CLOSED)),
            &iso_us(BASE_US + 55_000_000),
        ),
        "active",
    )?;
    // Placeholder attempts for the queued run (never claimed).
    insert_attempt(
        tx,
        cipher,
        &request_snapshot(RUN_QUEUED, "lint", AGENT_LINT, REQUEST_LINT, None, None, ""),
        "active",
    )?;
    insert_attempt(
        tx,
        cipher,
        &request_snapshot(
            RUN_QUEUED,
            "deploy",
            AGENT_DEPLOY,
            REQUEST_DEPLOY,
            None,
            None,
            "",
        ),
        "active",
    )?;
    if spec.include_active_claim {
        insert_attempt(
            tx,
            cipher,
            &request_snapshot(
                RUN_ACTIVE,
                "release",
                AGENT_RELEASE,
                REQUEST_RELEASE,
                None,
                Some((BASE_US + 130_000_000, RUNNER_OPEN)),
                &iso_us(BASE_US + 3_600_000_000),
            ),
            "active",
        )?;
        tx.execute(
            "INSERT INTO session_active_requests(session_id, active_request_id) VALUES (?1, ?2)",
            params![SESSION_OPEN, REQUEST_RELEASE],
        )?;
    }
    Ok(())
}

fn insert_step(
    tx: &rusqlite::Transaction<'_>,
    cipher: &Envelope,
    run_id: &str,
    agent: &str,
    step_id: &str,
    kind: &str,
    workflow_index: Option<i64>,
    runner_number: Option<i64>,
    name: &str,
    conclusion: &str,
    started: Option<i64>,
    finished: Option<i64>,
) -> anyhow::Result<()> {
    let name_blob = cipher.seal(name.as_bytes())?;
    tx.execute(
        "INSERT INTO job_steps(run_id, agent_job_id, step_id, kind, workflow_index, \
             runner_number, context_name, name_blob, conclusion, started_at_us, finished_at_us, revision) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 1)",
        params![
            run_id,
            agent,
            step_id,
            kind,
            workflow_index,
            runner_number,
            name,
            name_blob,
            conclusion,
            started,
            finished,
        ],
    )?;
    Ok(())
}

fn insert_steps(
    tx: &rusqlite::Transaction<'_>,
    cipher: &Envelope,
    spec: &LegacyFixtureSpec,
) -> anyhow::Result<()> {
    insert_step(
        tx,
        cipher,
        RUN_OK,
        AGENT_BUILD,
        "11111111-0000-0000-0000-000000000001",
        "synthetic",
        None,
        Some(1),
        "Set up job",
        "success",
        Some(BASE_US + 11_000_000),
        Some(BASE_US + 12_000_000),
    )?;
    insert_step(
        tx,
        cipher,
        RUN_OK,
        AGENT_BUILD,
        "11111111-0000-0000-0000-000000000002",
        "workflow",
        Some(0),
        Some(2),
        "Compile",
        "success",
        Some(BASE_US + 12_000_000),
        Some(BASE_US + 20_000_000),
    )?;
    insert_step(
        tx,
        cipher,
        RUN_OK,
        AGENT_TEST,
        "22222222-0000-0000-0000-000000000001",
        "workflow",
        Some(0),
        Some(2),
        "Run tests",
        "failure",
        Some(BASE_US + 31_000_000),
        Some(BASE_US + 40_000_000),
    )?;
    // `RUN_ACTIVE` exists only when the fixture claims it; its step must be
    // gated on the same flag or `job_steps.run_id` dangles.
    if spec.include_active_claim {
        insert_step(
            tx,
            cipher,
            RUN_ACTIVE,
            AGENT_RELEASE,
            "eeeeeeee-0000-0000-0000-000000000001",
            "workflow",
            Some(0),
            Some(2),
            "Deploy",
            "pending",
            Some(BASE_US + 131_000_000),
            None,
        )?;
    }
    Ok(())
}

fn insert_logs(tx: &rusqlite::Transaction<'_>, fixture: &LegacyFixture) -> anyhow::Result<()> {
    let bytes = &fixture.log_bytes;
    tx.execute(
        "INSERT INTO log_files(log_key, byte_count, line_count, updated_at_us) \
         VALUES (?1, ?2, ?3, ?4)",
        params![fixture.log_key, bytes.len() as i64, 2, BASE_US + 21_000_000],
    )?;
    for (index, chunk) in bytes.chunks(10).enumerate() {
        tx.execute(
            "INSERT INTO log_chunks(log_key, chunk_index, payload, written_at_us) \
             VALUES (?1, ?2, ?3, ?4)",
            params![
                fixture.log_key,
                index as i64,
                chunk,
                BASE_US + 21_000_000 + index as i64
            ],
        )?;
    }
    Ok(())
}

fn insert_runners(tx: &rusqlite::Transaction<'_>) -> anyhow::Result<()> {
    let keypair = preloop_gha_protocol::crypto::AgentRsaKeypair::generate()
        .map_err(|error| anyhow::anyhow!("generate fixture rsa key: {error}"))?;
    let rsa_xml = keypair.public_key().to_xml_string();
    for (runner_id, name, ephemeral, group) in [
        (RUNNER_CLOSED, "conf-gh-official-6", 1, Some("conf")),
        (RUNNER_OPEN, "runner-watch-7", 0, None),
    ] {
        tx.execute(
            "INSERT INTO runners(runner_id, name, ephemeral, runner_group_id, \
                 runner_group_name, public_key, rsa_public_key, created_at_us, updated_at_us, deleted_at_us) \
             VALUES (?1, ?2, ?3, NULL, ?4, NULL, ?5, ?6, ?7, NULL)",
            params![
                runner_id,
                name,
                ephemeral,
                group,
                rsa_xml,
                BASE_US,
                BASE_US + 100_000_000
            ],
        )?;
        for (ordinal, label) in ["self-hosted", "linux"].iter().enumerate() {
            tx.execute(
                "INSERT INTO runner_labels(runner_id, label, ordinal) VALUES (?1, ?2, ?3)",
                params![runner_id, label, ordinal as i64],
            )?;
        }
    }
    tx.execute(
        "INSERT INTO runner_sessions(session_id, runner_id, protocol, client_id, \
             session_key_blob, session_iv, session_tag, created_at_us, last_seen_at_us, closed_at_us) \
         VALUES (?1, ?2, 'broker', NULL, NULL, NULL, NULL, ?3, ?4, ?5)",
        params![
            SESSION_CLOSED,
            RUNNER_CLOSED,
            BASE_US,
            BASE_US + 50_000_000,
            BASE_US + 60_000_000
        ],
    )?;
    tx.execute(
        "INSERT INTO runner_sessions(session_id, runner_id, protocol, client_id, \
             session_key_blob, session_iv, session_tag, created_at_us, last_seen_at_us, closed_at_us) \
         VALUES (?1, ?2, 'broker', 'client-7', NULL, NULL, NULL, ?3, ?4, NULL)",
        params![
            SESSION_OPEN,
            RUNNER_OPEN,
            BASE_US,
            BASE_US + 130_000_000
        ],
    )?;
    Ok(())
}

fn insert_webhooks(tx: &rusqlite::Transaction<'_>, cipher: &Envelope) -> anyhow::Result<()> {
    let payload = br#"{"installation":{"id":9},"ref":"refs/heads/main"}"#;
    let blob = cipher.seal(payload)?;
    tx.execute(
        "INSERT INTO webhook_deliveries(delivery_id, event, payload_blob, received_at_us, \
             state, attempts, lease_until_us, lease_token, last_error) \
         VALUES ('delivery-1', 'push', ?1, ?2, 'received', 0, NULL, NULL, NULL)",
        params![blob, BASE_US + 200_000_000],
    )?;
    tx.execute(
        "INSERT INTO webhook_watchdog(scope, cursor_delivered_at_us, cursor_delivered_at_guid, \
             scan_cursor, last_poll_at_us, last_success_at_us) \
         VALUES ('app:9', ?1, 'guid-cursor', NULL, ?1, ?1)",
        params![BASE_US + 200_000_000],
    )?;
    tx.execute(
        "INSERT INTO webhook_redeliveries(delivery_guid, github_delivery_id, app_id, reason, \
             attempts, first_seen_at_us, last_attempt_at_us, resolved_at_us, last_error) \
         VALUES ('guid-cursor', 11, '9', 'remote_failure', 1, ?1, ?1, ?1, NULL)",
        params![BASE_US + 200_000_000],
    )?;
    Ok(())
}

fn insert_job_request_message(
    tx: &rusqlite::Transaction<'_>,
    spec: &LegacyFixtureSpec,
) -> anyhow::Result<()> {
    // The legacy store persists the runner-facing message per attempt; the
    // importer verifies it against the stored template.
    let message = message_json("build", AGENT_BUILD, REQUEST_BUILD, &spec.secret_value);
    tx.execute(
        "INSERT INTO job_request_messages(request_id, payload_json, written_at_us) \
         VALUES (?1, ?2, ?3)",
        params![
            REQUEST_BUILD,
            serde_json::to_string(&message)?,
            BASE_US + 21_000_000
        ],
    )?;
    Ok(())
}

fn insert_control_events(
    tx: &rusqlite::Transaction<'_>,
    spec: &LegacyFixtureSpec,
) -> anyhow::Result<()> {
    tx.execute(
        "INSERT INTO control_events(run_id, job_id, event_type, payload_json, created_at_us)          VALUES (?1, 'build', 'job_status', ?2, ?3)",
        params![
            RUN_OK,
            serde_json::json!({"run_id": RUN_OK, "job_id": "build", "status": "success"})
                .to_string(),
            BASE_US + 21_000_000
        ],
    )?;
    if spec.include_inflight_frame {
        tx.execute(
            "INSERT INTO broker_messages(session_id, message_id, payload_json, written_at_us)              VALUES (?1, 1, '{}', ?2)",
            params![SESSION_OPEN, BASE_US + 130_000_000],
        )?;
    }
    Ok(())
}

fn insert_meta(
    tx: &rusqlite::Transaction<'_>,
    cipher: &Envelope,
    spec: &LegacyFixtureSpec,
    fixture: &LegacyFixture,
) -> anyhow::Result<()> {
    let mut meta = serde_json::json!({
        "revision": 7,
        "workflow_run_counters": { ".github/workflows/ci.yml": 3 },
        "next_runner_id": 8,
        "next_cache_id": 0,
        "next_message_id": 0,
        "next_log_id": 1,
        "next_artifact_v2_id": 0,
        "azdo_sessions": [],
        "oidc_job_contexts": [[RUN_OK, "build", {"environment": null, "job_workflow_ref": null, "job_workflow_sha": null}]],
        "id_token_grants": [[RUN_OK, "build", true]],
        "concurrency_groups": [],
        "jobset_admissions": [],
        "run_concurrency": [],
        "holder_keys": [],
        "artifacts": [],
        "log_metadata": [[fixture.log_key, {"byte_count": fixture.log_bytes.len(), "line_count": 2}]],
        "timeline_events": [],
        "timeline_change_ids": [[timeline_of(REQUEST_BUILD).to_string(), 1]],
        "timeline_records": [[timeline_of(REQUEST_BUILD).to_string(), [
            {"id": "99999999-0000-4000-8000-000000000001", "changeId": 1,
             "name": "build", "type": "Job", "state": "completed", "result": "succeeded"}
        ]]],
        "cache_v2_pending": [],
        "cache_v2_dl_tokens": [],
        "artifact_v2_pending": [],
        "artifact_v2_registry": [],
        "github_token_requests": [[REQUEST_BUILD, {
            "repository": REPOSITORY,
            "permissions": {"contents": "read"},
            "declared": true,
            "untrusted": false
        }]],
        "cancellation_queue": [],
        "runner_client_ids": [["client-7", RUNNER_OPEN]],
        "pool_proven_runners": [RUNNER_OPEN],
        "job_assignments": [[RUN_QUEUED, "lint", RUNNER_OPEN, (BASE_US + 1) * 1000, (BASE_US + 1) * 1000]],
        "pool_pending": []
    });
    if spec.unknown_meta_key
        && let Some(object) = meta.as_object_mut()
    {
        object.insert("mystery_state".to_owned(), serde_json::json!([1, 2, 3]));
    }
    let blob = cipher.seal(&serde_json::to_vec(&meta)?)?;
    tx.execute(
        "INSERT INTO runtime_snapshots(snapshot_id, format_version, meta_blob, written_at_us, revision) \
         VALUES (1, 2, ?1, ?2, 7)",
        params![blob, BASE_US + 210_000_000],
    )?;
    tx.execute(
        "INSERT INTO workflow_run_counters(repository_key, workflow_path, next_run_number) \
         VALUES (?1, ?2, 4)",
        params![REPOSITORY, WORKFLOW_PATH],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    /// The checked-in fixture SQL must be byte-for-byte the released v11
    /// migration bodies (pinned sha256 per entry) and must carry exactly the
    /// migration names the importer's preflight expects.
    #[test]
    fn fixture_sql_matches_released_migrations() {
        let migrations = legacy_migrations();
        assert_eq!(migrations.len(), 11);
        for (index, (version, name, body)) in migrations.iter().enumerate() {
            let (pin_version, pin_name, pin_hash) = LEGACY_MIGRATION_SHA256[index];
            assert_eq!(*version, pin_version);
            assert_eq!(name, pin_name);
            let digest = hex::encode(Sha256::digest(body.as_bytes()));
            assert_eq!(
                digest, pin_hash,
                "fixture migration {version} ({name}) drifted from the released source"
            );
            let (expected_version, expected_name) = super::super::legacy::LEGACY_MIGRATIONS[index];
            assert_eq!(*version, expected_version);
            assert_eq!(name, expected_name);
        }
    }

    /// The fixture applies to a real file and stamps the released version.
    #[test]
    fn fixture_stamps_user_version_11() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("preloop.db");
        let spec = LegacyFixtureSpec::default();
        let fixture =
            write_legacy_fixture(&path, b"fixture-provenance-key-32-bytes!!", &spec).unwrap();
        assert_eq!(fixture.run_ok, RUN_OK);
        let conn = Connection::open(&path).unwrap();
        let version: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 11);
        let runs: i64 = conn
            .query_row("SELECT COUNT(*) FROM runs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(runs, 3);
    }
}
