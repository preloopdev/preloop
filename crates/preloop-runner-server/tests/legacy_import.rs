//! Operator-visible acceptance tests for `preloop store import-legacy`.
//!
//! The source is the populated v11 fixture (`legacy_import::fixture`), built
//! from the released migration chain; the target is a fresh control database.
//! Run with `cargo test -p preloop-runner-server --features test-support`.

#![cfg(feature = "test-support")]

use preloop_runner_server::legacy_import::fixture::{LegacyFixtureSpec, write_legacy_fixture};
use preloop_runner_server::legacy_import::{
    ActivePolicy, ImportOptions, ImportReport, run_import,
};
use preloop_runner_server::store::Envelope;
use rusqlite::Connection;
use std::path::{Path, PathBuf};

const KEY: &[u8] = b"legacy-import-test-key-32-bytes!";

struct Imported {
    _dir: tempfile::TempDir,
    source: PathBuf,
    target: PathBuf,
    state_dir: PathBuf,
    report: ImportReport,
}

fn import(
    spec: &LegacyFixtureSpec,
    policy: ActivePolicy,
    key: &[u8],
) -> anyhow::Result<Imported> {
    let dir = tempfile::tempdir()?;
    let source = dir.path().join("preloop.db");
    let fixture = write_legacy_fixture(&source, KEY, spec)?;
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir)?;
    let target = state_dir.join("preloop.db");
    let report = run_import(&ImportOptions {
        source: source.clone(),
        target: target.clone(),
        state_dir: state_dir.clone(),
        key: key.to_vec(),
        active: policy,
    })?;
    let _ = fixture;
    Ok(Imported {
        _dir: dir,
        source,
        target,
        state_dir,
        report,
    })
}

fn open(target: &Path) -> Connection {
    Connection::open(target).unwrap()
}

fn scalar_i64(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |row| row.get(0)).unwrap()
}

fn scalar_text(conn: &Connection, sql: &str) -> String {
    conn.query_row(sql, [], |row| row.get(0)).unwrap()
}

#[test]
fn populated_store_imports_runs_jobs_steps_logs_and_secrets() {
    let spec = LegacyFixtureSpec {
        include_active_claim: false,
        ..Default::default()
    };
    let imported = import(&spec, ActivePolicy::Refuse, KEY).unwrap();
    assert!(imported.target.is_file());
    assert_eq!(
        imported.report.imported.runs, 2,
        "completed + queued run are persisted"
    );
    assert_eq!(imported.report.imported.job_requests, 4);
    assert_eq!(imported.report.imported.job_steps, 3);

    let conn = open(&imported.target);

    // Runs: status/conclusion split.
    assert_eq!(
        scalar_text(
            &conn,
            "SELECT status || '/' || COALESCE(conclusion, '') FROM runs \
             WHERE run_id = '11111111-1111-4111-8111-111111111111'"
        ),
        "completed/success"
    );
    assert_eq!(
        scalar_text(
            &conn,
            "SELECT status FROM runs WHERE run_id = '22222222-2222-4222-8222-222222222222'"
        ),
        "queued"
    );
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM run_submissions"),
        2
    );

    // Jobs: terminal rows are synthesized from the record (the legacy store
    // deletes queue rows for finished jobs), queued rows come from payloads.
    assert_eq!(
        scalar_text(
            &conn,
            "SELECT status || '/' || queue_state || '/' || COALESCE(check_run_id, 0) \
             FROM jobs WHERE run_id = '11111111-1111-4111-8111-111111111111' AND job_id = 'build'"
        ),
        "success/none/4242"
    );
    assert_eq!(
        scalar_text(
            &conn,
            "SELECT status || '/' || queue_state FROM jobs \
             WHERE run_id = '11111111-1111-4111-8111-111111111111' AND job_id = 'test'"
        ),
        "failure/none"
    );
    assert_eq!(
        scalar_text(
            &conn,
            "SELECT status || '/' || queue_state FROM jobs \
             WHERE run_id = '22222222-2222-4222-8222-222222222222' AND job_id = 'lint'"
        ),
        "queued/ready"
    );
    assert_eq!(
        scalar_text(
            &conn,
            "SELECT queue_state || '/' || remaining_needs FROM jobs \
             WHERE run_id = '22222222-2222-4222-8222-222222222222' AND job_id = 'deploy'"
        ),
        "blocked/1"
    );
    let outputs: String = conn
        .query_row(
            "SELECT outputs FROM jobs WHERE job_id = 'build'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(outputs.contains("\"artifact\":\"ok\""), "{outputs}");
    let details: String = conn
        .query_row(
            "SELECT record_details FROM run_submissions \
             WHERE run_id = '11111111-1111-4111-8111-111111111111'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        details.contains("\"job_check_run_ids\":{\"build\":4242}"),
        "{details}"
    );

    // Attempts + steps.
    assert_eq!(
        scalar_text(
            &conn,
            "SELECT result FROM job_requests WHERE request_id = 1"
        ),
        "success"
    );
    assert_eq!(
        scalar_text(
            &conn,
            "SELECT step_id FROM job_steps ORDER BY position LIMIT 1"
        ),
        "11111111-0000-0000-0000-000000000001"
    );

    // Logs: metadata row plus the published segment bytes.
    let bytes: i64 = conn
        .query_row(
            "SELECT byte_count FROM log_files WHERE plan_id = ?1",
            ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(bytes, 18);
    let hex = |id: &str| -> String {
        id.bytes().map(|byte| format!("{byte:02x}")).collect()
    };
    let segment = imported
        .state_dir
        .join("live-logs")
        .join(hex("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"))
        .join(hex("1"))
        .join("seg-00000000000000000001.log");
    assert_eq!(
        std::fs::read(&segment).unwrap(),
        b"line one\nline two\n".to_vec(),
        "log bytes are published in the live-log layout"
    );

    // Secrets: run tier file, not the database.
    let tier = imported
        .state_dir
        .join("run-secrets")
        .join("11111111-1111-4111-8111-111111111111");
    let sealed = std::fs::read(&tier).unwrap();
    let plain = Envelope::new(KEY).unseal(&sealed).unwrap();
    let values: std::collections::BTreeMap<String, String> =
        serde_json::from_slice(&plain).unwrap();
    assert_eq!(values.get("CANARY").map(String::as_str), Some("hunter2-canary-value"));
    let submission_json: Option<String> = conn
        .query_row(
            "SELECT submission FROM run_submissions \
             WHERE run_id = '11111111-1111-4111-8111-111111111111'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        !submission_json.unwrap().contains("hunter2-canary-value"),
        "secret values never enter the control database"
    );

    // Counters, timelines, runners, sessions, webhooks.
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT last_run_number FROM workflow_run_numbers \
             WHERE repository = 'acme/widgets' AND workflow_path = '.github/workflows/ci.yml'"
        ),
        3
    );
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM timeline_records"), 1);
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM runners"), 2);
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM runner_sessions \
             WHERE session_id = '1e1e1e1e-1e1e-4e1e-8e1e-1e1e1e1e1e1e'"
        ),
        1
    );
    assert_eq!(
        scalar_text(&conn, "SELECT state FROM webhook_deliveries"),
        "received"
    );
    // The legacy event audit log is carried into the outbox with its id,
    // order, and payload preserved.
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM outbox_events"), 1);
    assert_eq!(
        scalar_text(&conn, "SELECT topic FROM outbox_events"),
        "job_status"
    );
    assert_eq!(
        scalar_i64(&conn, "SELECT event_id FROM outbox_events"),
        7
    );
    // The per-attempt message frame is verified, and the finalized artifact
    // registry (sidecar + snapshot) is carried into `artifacts`.
    assert_eq!(imported.report.imported.job_request_messages, 1);
    assert_eq!(imported.report.imported.artifacts, 1);
    assert_eq!(
        scalar_text(
            &conn,
            "SELECT name || '/' || state || '/' || storage_key FROM artifacts"
        ),
        "logs/finalized/blob-token-11"
    );
    // No sidecar staging directories are left behind.
    let leftovers: Vec<_> = std::fs::read_dir(&imported.state_dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".import-")
        })
        .collect();
    assert!(leftovers.is_empty(), "staging dirs left: {leftovers:?}");

}

#[test]
fn source_file_bytes_are_unchanged_by_the_import() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("preloop.db");
    let spec = LegacyFixtureSpec {
        include_active_claim: false,
        ..Default::default()
    };
    let _fixture = write_legacy_fixture(&source, KEY, &spec).unwrap();
    let before = std::fs::read(&source).unwrap();
    let target = dir.path().join("state").join("preloop.db");
    std::fs::create_dir_all(target.parent().unwrap()).unwrap();
    run_import(&ImportOptions {
        source: source.clone(),
        target,
        state_dir: dir.path().join("state"),
        key: KEY.to_vec(),
        active: ActivePolicy::Refuse,
    })
    .unwrap();
    assert_eq!(before, std::fs::read(&source).unwrap());
}

#[test]
fn wrong_key_is_refused_and_leaves_no_target() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("preloop.db");
    let spec = LegacyFixtureSpec {
        include_active_claim: false,
        ..Default::default()
    };
    let _fixture = write_legacy_fixture(&source, KEY, &spec).unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let target = state_dir.join("preloop.db");
    let error = run_import(&ImportOptions {
        source,
        target: target.clone(),
        state_dir: state_dir.clone(),
        key: b"a-completely-different-cluster-key".to_vec(),
        active: ActivePolicy::Refuse,
    })
    .unwrap_err();
    let message = format!("{error:#}");
    assert!(
        message.contains("authentication failed") || message.contains("decode"),
        "{message}"
    );
    assert!(!target.exists(), "a failed import leaves no target");
    assert!(
        !state_dir.join("preloop.db.importing").exists(),
        "staging is cleaned up"
    );
}

#[test]
fn claimed_attempt_refuses_until_an_explicit_drain_policy() {
    let spec = LegacyFixtureSpec::default();
    let refused = import(&spec, ActivePolicy::Refuse, KEY).unwrap_err();
    let message = format!("{refused:#}");
    assert!(message.contains("claimed-attempt"), "{message}");
    assert!(message.contains("--active=requeue"), "{message}");

    let requeued = import(&spec, ActivePolicy::Requeue, KEY).unwrap();
    let conn = open(&requeued.target);
    assert_eq!(
        scalar_text(
            &conn,
            "SELECT queue_state || '/' || COALESCE(claimed_by_runner_id, -1) FROM jobs \
             WHERE run_id = '33333333-3333-4333-8333-333333333333' AND job_id = 'release'"
        ),
        "ready/-1"
    );
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM job_leases"), 0);

    let cancelled = import(&spec, ActivePolicy::Cancel, KEY).unwrap();
    let conn = open(&cancelled.target);
    assert_eq!(
        scalar_text(
            &conn,
            "SELECT status || '/' || queue_state FROM jobs \
             WHERE run_id = '33333333-3333-4333-8333-333333333333' AND job_id = 'release'"
        ),
        "cancelled/none"
    );
    assert_eq!(
        scalar_text(&conn, "SELECT result FROM job_requests WHERE request_id = 5"),
        "cancelled"
    );
}

#[test]
fn unrecognized_source_version_is_refused() {
    let spec = LegacyFixtureSpec {
        user_version: 5,
        ..Default::default()
    };
    let refused = import(&spec, ActivePolicy::Refuse, KEY).unwrap_err();
    let message = format!("{refused:#}");
    assert!(message.contains("user_version=5"), "{message}");
}

#[test]
fn unknown_metadata_is_refused_rather_than_dropped() {
    let spec = LegacyFixtureSpec {
        include_active_claim: false,
        unknown_meta_key: true,
        ..Default::default()
    };
    let refused = import(&spec, ActivePolicy::Refuse, KEY).unwrap_err();
    let message = format!("{refused:#}");
    assert!(message.contains("mystery_state"), "{message}");
}

#[test]
fn unrelated_durable_frames_refuse_the_import() {
    let spec = LegacyFixtureSpec {
        include_active_claim: false,
        include_inflight_frame: true,
        ..Default::default()
    };
    let refused = import(&spec, ActivePolicy::Refuse, KEY).unwrap_err();
    let message = format!("{refused:#}");
    assert!(message.contains("broker_messages=1"), "{message}");
}

#[test]
fn conflicting_sidecar_refuses_before_publishing_anything() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("preloop.db");
    let spec = LegacyFixtureSpec {
        include_active_claim: false,
        ..Default::default()
    };
    let fixture = write_legacy_fixture(&source, KEY, &spec).unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(state_dir.join("run-secrets")).unwrap();
    // A foreign secret tier for the same run: the importer must refuse
    // rather than overwrite it.
    std::fs::write(
        state_dir.join("run-secrets").join(&fixture.run_ok),
        b"not-this-importer-s-file",
    )
    .unwrap();
    let target = state_dir.join("preloop.db");
    let error = run_import(&ImportOptions {
        source,
        target: target.clone(),
        state_dir: state_dir.clone(),
        key: KEY.to_vec(),
        active: ActivePolicy::Refuse,
    })
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("refusing to overwrite existing"),
        "{error:#}"
    );
    assert!(!target.exists());
    assert!(!state_dir.join("preloop.db.importing").exists());
}

#[test]
fn retry_after_failure_and_existing_target_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("preloop.db");
    let spec = LegacyFixtureSpec {
        include_active_claim: false,
        ..Default::default()
    };
    let _fixture = write_legacy_fixture(&source, KEY, &spec).unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let target = state_dir.join("preloop.db");
    let options = |key: &[u8]| ImportOptions {
        source: source.clone(),
        target: target.clone(),
        state_dir: state_dir.clone(),
        key: key.to_vec(),
        active: ActivePolicy::Refuse,
    };
    assert!(run_import(&options(b"wrong-key-wrong-key-wrong-key!!!")).is_err());
    // Retry with the right key succeeds on the same paths.
    run_import(&options(KEY)).unwrap();
    // A second import refuses to overwrite the target it produced.
    let again = run_import(&options(KEY)).unwrap_err();
    assert!(
        format!("{again:#}").contains("already exists"),
        "{again:#}"
    );
}
