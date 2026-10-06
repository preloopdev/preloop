//! Backend-level acceptance: the imported database is opened through the real
//! lite backend and exercised through its command surface — run/attempt read
//! projections, queued-job acquisition, step manifests, and the live-log
//! lookup — not just raw SQL counts.

use super::fixture::{LegacyFixtureSpec, write_legacy_fixture};
use super::{ActivePolicy, ImportOptions, run_import};
use crate::control::backend::{CreateSession, PollRequest, RequestKey};
use crate::control::lite::LiteBackend;
use crate::control::types::{PollOutcome, SessionProtocol};
use crate::models::RunnerCapabilities;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use std::time::Duration;

const KEY: &[u8] = b"legacy-import-unit-test-key-32!!";

struct Imported {
    _dir: tempfile::TempDir,
    target: std::path::PathBuf,
    state_dir: std::path::PathBuf,
    fixture: super::fixture::LegacyFixture,
    report: super::ImportReport,
}

fn import(spec: &LegacyFixtureSpec) -> Imported {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("preloop.db");
    let fixture = write_legacy_fixture(&source, KEY, spec).unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let target = state_dir.join("preloop.db");
    let report = run_import(&ImportOptions {
        source,
        target: target.clone(),
        state_dir: state_dir.clone(),
        key: KEY.to_vec(),
        active: ActivePolicy::Refuse,
    })
    .unwrap();
    Imported {
        _dir: dir,
        target,
        state_dir,
        fixture,
        report,
    }
}

#[tokio::test]
async fn imported_database_serves_runs_attempts_steps_and_queued_claims() {
    let spec = LegacyFixtureSpec {
        include_active_claim: false,
        ..Default::default()
    };
    let imported = import(&spec);
    let backend =
        LiteBackend::open(&imported.target, false, false, Duration::from_secs(300)).unwrap();
    let run_ok = RunId(imported.fixture.run_ok.parse().unwrap());
    let run_queued = RunId(imported.fixture.run_queued.parse().unwrap());

    // Run projection: status/conclusion, per-job statuses, check ids, outputs.
    let record = backend.run_record(run_ok).await.unwrap().unwrap();
    assert_eq!(record.status, ExecutionStatus::Success);
    assert_eq!(record.conclusion.as_deref(), Some("success"));
    assert_eq!(
        record.jobs.get(&JobId("build".to_owned())).copied(),
        Some(ExecutionStatus::Success)
    );
    assert_eq!(
        record
            .job_check_run_ids
            .get(&JobId("build".to_owned()))
            .copied(),
        Some(imported.fixture.check_run_id)
    );
    assert_eq!(
        record
            .job_outputs
            .get(&JobId("build".to_owned()))
            .and_then(|outputs| outputs.get("artifact"))
            .and_then(|value| value.as_str()),
        Some("ok")
    );

    // Attempts and step manifests.
    let attempt = backend
        .request(RequestKey::Id(imported.fixture.request_build))
        .await
        .unwrap();
    assert_eq!(attempt.result, Some(ExecutionStatus::Success));
    let manifests = backend.run_step_manifests(run_ok).await.unwrap();
    let agent = uuid::Uuid::parse_str(&imported.fixture.agent_build).unwrap();
    assert_eq!(manifests.get(&agent).map(Vec::len), Some(2));

    // Live-log lookup resolves the imported attempt and serves its bytes.
    let (plan, terminal) = backend
        .live_log_key(run_ok, "build")
        .await
        .unwrap()
        .expect("imported attempt owns a live-log key");
    assert_eq!(plan, imported.fixture.agent_build);
    assert!(terminal);
    let segments = crate::LiveLogSegments::new(imported.state_dir.join("live-logs"));
    assert_eq!(
        segments.read_all(&plan, "1").await.unwrap(),
        imported.fixture.log_bytes
    );

    // Queued work: the ready job is claimed through the normal broker
    // sequence — create a session, poll (which binds the placeholder request
    // to the session), then acquire. A bare acquire without a session is
    // refused by design.
    let stats = backend.queue_stats().await.unwrap();
    assert!(stats.ready >= 1, "{stats:?}");
    let session = backend
        .create_session(CreateSession {
            runner_id: 7,
            protocol: SessionProtocol::Broker,
            client_id: None,
        })
        .await
        .unwrap();
    let outcome = backend
        .poll_session(PollRequest {
            session_id: session.session_id.clone(),
            verified_runner_id: Some(7),
            runner: RunnerCapabilities {
                known: true,
                labels: vec!["self-hosted".to_owned()],
                runner_group_id: None,
                runner_group_name: None,
            },
            busy: false,
            wait_ms: 0,
        })
        .await
        .unwrap();
    let PollOutcome::Claimed(claimed) = outcome else {
        panic!("expected a claimed job, got {outcome:?}");
    };
    let context = backend
        .acquire_for_runner(claimed.request.request_id, 7)
        .await
        .expect("queued lint job acquires");
    assert_eq!(context.message.job_name, "lint");
    assert_eq!(context.repository, imported.fixture.repository);
    // `job_queue_state` returns `(queue kind, status)`. The claim moved the
    // job to `in_progress` and marked its queue entry claimed.
    let (queue_state, status) = backend
        .job_queue_state(run_queued, &JobId("lint".to_owned()))
        .await
        .unwrap()
        .expect("job row exists");
    assert_eq!(status, "in_progress");
    assert_eq!(queue_state, "claimed");
}

/// A claim the legacy store never finished while the run record already
/// settled the job is history, not live work: the default import (no
/// `--active` override) settles it with the job's terminal result, reports
/// the settlement, and writes no lease or runner/session binding.
#[test]
fn default_import_settles_stale_claims_on_finished_work_as_history() {
    let spec = LegacyFixtureSpec {
        include_active_claim: false,
        include_stale_claim: true,
        ..Default::default()
    };
    let imported = import(&spec);
    assert_eq!(imported.report.active_policy, ActivePolicy::Refuse);
    assert!(
        imported
            .report
            .notes
            .iter()
            .any(|note| note.contains("1 stale claim")),
        "{:?}",
        imported.report.notes
    );
    assert!(
        imported
            .report
            .notes
            .iter()
            .any(|note| note.contains("1 legacy session binding")),
        "{:?}",
        imported.report.notes
    );
    // The leaked gates held by the finished run and an evicted run are
    // released as history and reported, never silently dropped.
    assert!(
        imported
            .report
            .notes
            .iter()
            .any(|note| note.contains("2 stale concurrency gate")),
        "{:?}",
        imported.report.notes
    );
    let gates = imported
        .report
        .skipped
        .iter()
        .find(|skip| skip.family == "meta.concurrency_gates")
        .expect("gate family is reported");
    assert_eq!(gates.rows, 2);
    assert!(gates.reason.contains("released as history"), "{gates:?}");
    assert!(
        !imported
            .report
            .skipped
            .iter()
            .any(|skip| skip.family.contains("claimed")),
        "{:?}",
        imported.report.skipped
    );

    let conn = rusqlite::Connection::open(&imported.target).unwrap();
    let (result, finished_at, runner, session, claimed_at): (
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<String>,
        i64,
    ) = conn
        .query_row(
            "SELECT result, finished_at, runner_id, session_id, claimed_at FROM job_requests \
             WHERE request_id = ?1",
            [imported.fixture.request_stale],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(result.as_deref(), Some("cancelled"));
    assert!(finished_at.is_some(), "settled attempt carries finished_at");
    assert!(
        runner.is_none() && session.is_none(),
        "settled attempt keeps no runner/session binding"
    );
    assert_eq!(claimed_at, 0);
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM job_leases", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        0,
        "a settled stale claim never becomes a live lease"
    );
    let (status, queue_state, claimed_by): (String, String, Option<i64>) = conn
        .query_row(
            "SELECT status, queue_state, claimed_by_runner_id FROM jobs \
             WHERE run_id = ?1 AND job_id = 'archive'",
            [&imported.fixture.run_stale],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(status, "cancelled");
    assert_eq!(
        queue_state, "none",
        "settled history is never ready/claimed"
    );
    assert!(claimed_by.is_none());
}

/// The settlement is only for finished work: an unfinished claim on an
/// unfinished job still refuses by default, and the refusal names only that
/// live claim — the stale one is settled, not listed.
#[test]
fn claims_on_unfinished_jobs_still_refuse_by_default() {
    let spec = LegacyFixtureSpec {
        include_active_claim: true,
        include_stale_claim: true,
        ..Default::default()
    };
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("preloop.db");
    let fixture = write_legacy_fixture(&source, KEY, &spec).unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let error = run_import(&ImportOptions {
        source,
        target: state_dir.join("preloop.db"),
        state_dir,
        key: KEY.to_vec(),
        active: ActivePolicy::Refuse,
    })
    .unwrap_err();
    let message = format!("{error:#}");
    // Two live items: the claimed attempt on the unfinished `release` job and
    // its session binding. The stale claim on the finished `archive` job and
    // its binding are history the writer settles.
    assert!(message.contains("2 item(s)"), "{message}");
    assert!(message.contains("claimed-attempt 5:"), "{message}");
    assert!(message.contains("session-binding 5:"), "{message}");
    assert!(!message.contains("claimed-attempt 6"), "{message}");
    assert!(!message.contains("session-binding 6"), "{message}");
    assert!(message.contains("--active=requeue"), "{message}");
    assert_eq!(fixture.request_stale, 6);
}

/// A concurrency gate held by a genuinely unfinished run is live work: it
/// still refuses by default, and only an explicit policy releases it.
#[test]
fn live_concurrency_gates_still_refuse_by_default() {
    let spec = LegacyFixtureSpec {
        include_active_claim: false,
        include_live_gate: true,
        ..Default::default()
    };
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("preloop.db");
    let _fixture = write_legacy_fixture(&source, KEY, &spec).unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let target = state_dir.join("preloop.db");
    let options = |active: ActivePolicy| ImportOptions {
        source: source.clone(),
        target: target.clone(),
        state_dir: state_dir.clone(),
        key: KEY.to_vec(),
        active,
    };
    let error = run_import(&options(ActivePolicy::Refuse)).unwrap_err();
    let message = format!("{error:#}");
    assert!(message.contains("concurrency-state"), "{message}");
    assert!(message.contains("run_concurrency=1"), "{message}");
    assert!(!target.exists(), "a refused import leaves no target");

    let imported = run_import(&options(ActivePolicy::Requeue)).unwrap();
    assert!(
        imported.skipped.iter().any(|skip| {
            skip.family == "meta.concurrency_gates"
                && skip.reason.contains("explicit --active policy")
        }),
        "{:?}",
        imported.skipped
    );
}

/// Legacy unscoped log metadata (`job:<agent_job_id>`, `step:<step_id>`),
/// written before Results identifiers were canonicalized, names an attempt
/// directly; the importer attributes it instead of reporting it unmapped.
#[test]
fn unscoped_legacy_log_metadata_is_attributed_to_imported_attempts() {
    let spec = LegacyFixtureSpec {
        include_active_claim: false,
        include_unscoped_log_keys: true,
        ..Default::default()
    };
    let imported = import(&spec);
    assert_eq!(imported.report.imported.log_files, 3);
    assert!(
        !imported
            .report
            .skipped
            .iter()
            .any(|skip| skip.family.contains("log files")),
        "{:?}",
        imported.report.skipped
    );
    let conn = rusqlite::Connection::open(&imported.target).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT plan_id, COUNT(*), SUM(byte_count) FROM log_files \
             GROUP BY plan_id ORDER BY plan_id",
        )
        .unwrap();
    let rows: Vec<(String, i64, i64)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            // The scoped `results:` key: one log with its published bytes.
            (imported.fixture.agent_build.clone(), 1, 18),
            // The unscoped `job:` and `step:` keys: metadata-only (pruned).
            (imported.fixture.agent_test.clone(), 2, 0),
        ]
    );
}

#[test]
fn publication_never_replaces_an_existing_target() {
    let dir = tempfile::tempdir().unwrap();
    let staging = dir.path().join("preloop.db.importing");
    let target = dir.path().join("preloop.db");
    std::fs::write(&staging, b"imported").unwrap();
    std::fs::write(&target, b"someone-else").unwrap();
    let error =
        crate::control::lite::legacy_import::publish_no_replace(&staging, &target).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(&target).unwrap(), b"someone-else");
    std::fs::remove_file(&target).unwrap();
    crate::control::lite::legacy_import::publish_no_replace(&staging, &target).unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"imported");
}

#[tokio::test]
async fn imported_database_rejects_the_legacy_source_itself() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("preloop.db");
    let spec = LegacyFixtureSpec {
        include_active_claim: false,
        ..Default::default()
    };
    let _fixture = write_legacy_fixture(&source, KEY, &spec).unwrap();
    // The backend must refuse the legacy layout rather than half-read it.
    assert!(LiteBackend::open(&source, false, false, Duration::from_secs(300)).is_err());
}
