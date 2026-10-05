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
}

fn import(spec: &LegacyFixtureSpec) -> Imported {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("preloop.db");
    let fixture = write_legacy_fixture(&source, KEY, spec).unwrap();
    let state_dir = dir.path().join("state");
    std::fs::create_dir_all(&state_dir).unwrap();
    let target = state_dir.join("preloop.db");
    run_import(&ImportOptions {
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

#[test]
fn publication_never_replaces_an_existing_target() {
    let dir = tempfile::tempdir().unwrap();
    let staging = dir.path().join("preloop.db.importing");
    let target = dir.path().join("preloop.db");
    std::fs::write(&staging, b"imported").unwrap();
    std::fs::write(&target, b"someone-else").unwrap();
    let error = crate::control::lite::legacy_import::publish_no_replace(&staging, &target)
        .unwrap_err();
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
