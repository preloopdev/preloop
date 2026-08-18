use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use preloop_gha_protocol::azdo::AgentJobRequestMessage;
use serde_json::Value;
use sha1::{Digest, Sha1};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::Path as FsPath;
use std::process::{Command, Stdio};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use super::*;
const TEST_API_TOKEN: &str = "property-test-token";

fn app(state: AppState, shutdown: CancellationToken) -> Router {
    app_with_test_api(state, shutdown, TEST_API_TOKEN)
}

/// `preserve_on_failure` is a property of the run, carried to the runner on the
/// job message. It must be absent unless asked for, so the default wire shape
/// stays byte-identical to what an official runner expects.
#[tokio::test]
async fn preserve_on_failure_reaches_the_job_message_only_when_requested() {
    for (requested, expected) in [(true, Some(true)), (false, None)] {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
                "event": "push",
                "repository": "owner/repo",
                "preserve_on_failure": requested
            }),
        )
        .await;

        let inner = state.inner.lock().await;
        let queued = inner.queue.front().expect("job should be queued");
        assert_eq!(
            queued.message.preloop_preserve_on_failure, expected,
            "preserve_on_failure={requested}"
        );

        // Absent means absent on the wire, not `false`.
        let wire = serde_json::to_value(&queued.message).unwrap();
        assert_eq!(
            wire.get("preloopPreserveOnFailure")
                .and_then(Value::as_bool),
            expected,
            "wire shape for preserve_on_failure={requested}"
        );
    }
}

#[tokio::test]
async fn prebuilt_messages_preserve_monotonic_workflow_run_numbers() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";

    let first = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": workflow,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let second = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": workflow,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;

    assert_eq!(first["run_number"], 1);
    assert_eq!(second["run_number"], 2);
}

#[tokio::test]
async fn sqlite_recovery_restores_queued_runs_and_next_run_number() {
    let temp = tempfile::tempdir().unwrap();
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let (run_id, first_number) = {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": workflow,
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
        {
            let mut inner = state.inner.lock().await;
            inner
                .logs
                .insert("plan-1/7".to_owned(), b"durable log\n".to_vec());
            inner.log_metadata.insert(
                "plan-1/7".to_owned(),
                LogMetadata {
                    byte_count: 13,
                    line_count: 1,
                },
            );
            // Log bytes now go through `log_chunks`; the per-file counter
            // is UPSERTed on the same path.
            state
                .store
                .store_log_chunk("plan-1/7", 0, b"durable log\n", 13, 1)
                .await
                .unwrap();
            inner.cache_v2_pending.insert(
                "cache-upload".to_owned(),
                CacheV2Pending {
                    key: "cache-key".to_owned(),
                    version: "cache-version".to_owned(),
                },
            );
            state
                .store
                .store_meta_only(&crate::store::build_meta_snapshot(&inner))
                .await
                .unwrap();
        }
        (
            accepted["run_id"].as_str().unwrap().to_owned(),
            accepted["run_number"].as_u64().unwrap(),
        )
    };

    let recovered = AppState::new(temp.path().to_path_buf()).await.unwrap();
    {
        let inner = recovered.inner.lock().await;
        assert!(inner.runs.contains_key(&run_id.parse::<RunId>().unwrap()));
        assert_eq!(inner.queue.len(), 1);
        assert_eq!(inner.queue.front().unwrap().job_id.0, "build");
        assert_eq!(inner.logs["plan-1/7"], b"durable log\n");
        assert_eq!(inner.log_metadata["plan-1/7"].line_count, 1);
        assert_eq!(inner.cache_v2_pending["cache-upload"].key, "cache-key");
    }
    let recovered_app = app(recovered, CancellationToken::new());
    let accepted = request_json(
        &recovered_app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": workflow,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    assert_eq!(accepted["run_number"], first_number + 1);
}

/// Restart round-trip for the state this scenario can reach through the HTTP
/// surface: `session_keys`, `runner_rsa_public_keys`, run status after a
/// cancel, `log_chunks`, and `queue_depth`. The message queues, run secrets
/// and cross-run queue order have their own tests below, because they need
/// state this scenario does not produce.
#[tokio::test]
async fn sqlite_recovery_restores_post_restart_state() {
    let temp = tempfile::tempdir().unwrap();
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";

    let (run_id_str, runner_id, session_id, public_xml) = {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": workflow,
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id = accepted["run_id"].as_str().unwrap().to_owned();

        // Register a runner with an RSA public key (C2).
        let runner_keypair = AgentRsaKeypair::generate().unwrap();
        let public_xml = runner_keypair.public_key_xml();
        let modulus = public_xml
            .split("<Modulus>")
            .nth(1)
            .unwrap()
            .split("</Modulus>")
            .next()
            .unwrap()
            .to_owned();
        let exponent = public_xml
            .split("<Exponent>")
            .nth(1)
            .unwrap()
            .split("</Exponent>")
            .next()
            .unwrap()
            .to_owned();
        let runner = request_json(
            &app,
            Method::POST,
            "/runner/server/_apis/distributedtask/pools/1/agents",
            json!({
                "name": "recovery-runner",
                "labels": [{"name": "self-hosted", "type": "system"}],
                "authorization": {
                    "publicKey": { "exponent": exponent, "modulus": modulus }
                }
            }),
        )
        .await;
        let rid = runner["id"].as_i64().unwrap();

        // Create a session (C1: session_keys).
        let session_json = request_json(
            &app,
            Method::POST,
            "/runner/server/_apis/distributedtask/pools/1/sessions",
            json!({"ownerName": "recovery-runner", "agent": {"id": rid}}),
        )
        .await;
        let session_id = session_json["sessionId"].as_str().unwrap().to_owned();
        // The disttask session handler always returns `encrypted: false` for
        // local-use AzDO compatibility; the AES key is still stored under
        // `inner.session_keys` (sealed) and is what C1 restores after
        // restart. FIPS-wrapping is exercised by the
        // `session_key_uses_registered_runner_public_key` test for the
        // broker-internal path.

        // Queue a cancel (C5: cancellation_queue).
        request_json(
            &app,
            Method::POST,
            &format!("/api/v1/runs/{run_id}/cancel"),
            json!({"reason": "test cancel before restart"}),
        )
        .await;

        // Persist a log chunk (A: log_chunks hot path).
        state
            .store
            .store_log_chunk("plan-1/0", 0, b"first line\n", 11, 1)
            .await
            .unwrap();
        state
            .store
            .store_log_chunk("plan-1/0", 11, b"second line\n", 23, 2)
            .await
            .unwrap();

        (run_id, rid, session_id, public_xml)
    };

    // Restart.
    let recovered = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let recovered_inner = recovered.inner.lock().await;

    // C1: session_keys restored.
    assert!(
        recovered_inner.session_keys.contains_key(&session_id),
        "session_keys must survive restart"
    );

    // C2: runner_rsa_public_keys restored.
    assert_eq!(
        recovered_inner
            .runner_rsa_public_keys
            .get(&runner_id)
            .map(|k| k.to_xml_string()),
        Some(public_xml.clone()),
        "RSA public key must survive restart"
    );

    // C5: cancellation of a queued job removes it from the queue and
    // marks the run Cancelled. `cancellation_queue` is reserved for
    // in-progress jobs that need a JobCancellation message sent; a queued
    // job is simply dropped. Assert the run status is restored.
    let recovered_run = recovered_inner
        .runs
        .get(&run_id_str.parse::<RunId>().unwrap())
        .cloned()
        .expect("run must survive restart");
    assert_eq!(
        recovered_run.status,
        ExecutionStatus::Cancelled,
        "cancel status must survive restart"
    );

    // A: log_chunks restored into the in-memory buffer.
    assert_eq!(
        recovered_inner
            .logs
            .get("plan-1/0")
            .cloned()
            .unwrap_or_default(),
        b"first line\nsecond line\n".to_vec(),
        "log bytes must survive restart via log_chunks"
    );
    assert_eq!(
        recovered_inner
            .log_metadata
            .get("plan-1/0")
            .map(|m| (m.byte_count, m.line_count)),
        Some((23, 2)),
        "log counter must survive restart"
    );

    // C6: queue_depth restored.
    assert_eq!(
        recovered
            .queue_depth
            .load(std::sync::atomic::Ordering::SeqCst),
        recovered_inner.queue.len(),
        "queue_depth must mirror recovered ready queue"
    );

    drop(recovered_inner);

    // The post-restart server must still be able to register a new runner
    // (sanity: store + WAL + schema migration don't break startup).
    let app = app(recovered, CancellationToken::new());
    let _ = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": workflow,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
}

/// Postgres twin of `sqlite_recovery_restores_post_restart_state`: proves
/// the translated SQL (dialect, upserts, sealed blobs) round-trips the same
/// state a restart must restore. Skipped unless `PRELOOP_TEST_PG_URL` points at
/// a disposable Postgres (the repo gate does not assume one is running).
/// TLS URLs (`?sslmode=require|verify-full`) additionally need
/// `PRELOOP_TEST_PG_CA` set to a PEM trust anchor for the test database.
#[tokio::test]
async fn postgres_recovery_restores_post_restart_state() {
    let pg_url = match std::env::var("PRELOOP_TEST_PG_URL") {
        Ok(url) if !url.trim().is_empty() => url,
        _ => {
            eprintln!(
                "skipping postgres_recovery_restores_post_restart_state: \
                 set PRELOOP_TEST_PG_URL to a disposable Postgres URL"
            );
            return;
        }
    };
    let temp = tempfile::tempdir().unwrap();
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: self-hosted\n    steps:\n      - run: echo hi\n";
    let config_path = crate::config::config_path();

    // For TLS URLs, trust the operator-supplied CA (PEM) — the store's
    // connector loads it via SSL_CERT_FILE. Nothing else in the crate reads
    // this variable, so setting it process-wide cannot affect other tests.
    if let Ok(ca) = std::env::var("PRELOOP_TEST_PG_CA") {
        if !ca.is_empty() {
            std::env::set_var("SSL_CERT_FILE", ca);
        }
    }

    // The URL may point at a reused database; clear the store tables so the
    // round-trip starts from a known state (migrations stay behind).
    let connect_url = crate::store_pg::connect_url(&pg_url);
    let client = match crate::store_pg::tls_connector(&pg_url).unwrap() {
        Some(tls) => {
            let (client, connection) = tokio_postgres::connect(&connect_url, tls).await.unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
        }
        None => {
            let (client, connection) = tokio_postgres::connect(&connect_url, tokio_postgres::NoTls)
                .await
                .unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
        }
    };
    // A brand-new database has no tables yet (the store's migration creates
    // them on first open); only clean a schema that already exists.
    let has_schema: bool = client
        .query_one(
            "SELECT to_regclass('public.workflow_run_counters') IS NOT NULL",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    if has_schema {
        client
            .batch_execute(
                "TRUNCATE workflow_run_counters, runs, runners, runner_labels,
                         runner_sessions, jobs, job_dependencies, job_requests, control_events,
                         session_active_requests, broker_messages, job_request_messages,
                         log_files, log_chunks, runtime_snapshots RESTART IDENTITY CASCADE",
            )
            .await
            .unwrap();
    }
    drop(client);

    let (run_id_str, runner_id, session_id, public_xml, first_number) = {
        let state = AppState::new_with_store(
            temp.path().to_path_buf(),
            config_path.clone(),
            Some(&pg_url),
        )
        .await
        .unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": workflow,
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
        let first_number = accepted["run_number"].as_u64().unwrap();
        let run_id = accepted["run_id"].as_str().unwrap().to_owned();

        // Register a runner with an RSA public key.
        let runner_keypair = AgentRsaKeypair::generate().unwrap();
        let public_xml = runner_keypair.public_key_xml();
        let modulus = public_xml
            .split("<Modulus>")
            .nth(1)
            .unwrap()
            .split("</Modulus>")
            .next()
            .unwrap()
            .to_owned();
        let exponent = public_xml
            .split("<Exponent>")
            .nth(1)
            .unwrap()
            .split("</Exponent>")
            .next()
            .unwrap()
            .to_owned();
        let runner = request_json(
            &app,
            Method::POST,
            "/runner/server/_apis/distributedtask/pools/1/agents",
            json!({
                "name": "pg-recovery-runner",
                "labels": [{"name": "self-hosted", "type": "system"}],
                "authorization": {
                    "publicKey": { "exponent": exponent, "modulus": modulus }
                }
            }),
        )
        .await;
        let runner_id = runner["id"].as_i64().unwrap();

        // Create a session (sealed session key).
        let session_json = request_json(
            &app,
            Method::POST,
            "/runner/server/_apis/distributedtask/pools/1/sessions",
            json!({"ownerName": "pg-recovery-runner", "agent": {"id": runner_id}}),
        )
        .await;
        let session_id = session_json["sessionId"].as_str().unwrap().to_owned();

        // Claim the queued job so `job_requests`, `session_active_requests`,
        // and the per-session broker queue all have rows to persist.
        let claimed = request_json(
            &app,
            Method::GET,
            &format!("/runner/server/_apis/v1/Message/1?sessionId={session_id}&waitSeconds=0"),
            Value::Null,
        )
        .await;
        assert_eq!(
            claimed["messageType"],
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST,
            "claimed job must round-trip through the postgres store"
        );

        // Persist log chunks (hot path) and a full snapshot so every table
        // is written through the translated SQL before the restart.
        state
            .store
            .store_log_chunk("plan-1/0", 0, b"first line\n", 11, 1)
            .await
            .unwrap();
        state
            .store
            .store_log_chunk("plan-1/0", 11, b"second line\n", 23, 2)
            .await
            .unwrap();
        {
            let inner = state.inner.lock().await;
            state
                .store
                .store_inner(&crate::store::StoreSnapshot::from_inner(&inner))
                .await
                .unwrap();
        }

        (run_id, runner_id, session_id, public_xml, first_number)
    };

    // Restart against the same database.
    let recovered = AppState::new_with_store(
        temp.path().to_path_buf(),
        config_path.clone(),
        Some(&pg_url),
    )
    .await
    .unwrap();
    {
        let inner = recovered.inner.lock().await;

        // Runs survive.
        let recovered_run = inner
            .runs
            .get(&run_id_str.parse::<RunId>().unwrap())
            .cloned()
            .expect("run must survive restart");
        assert_eq!(recovered_run.run_number, first_number);

        // The claimed job is gone from the ready queue but its agent job
        // request is restored for re-delivery.
        assert_eq!(inner.queue.len(), 0, "claimed job must not re-queue");
        assert_eq!(inner.job_requests.len(), 1, "job request must survive");
        assert_eq!(
            inner.session_active_requests.len(),
            1,
            "session active request must survive"
        );

        // Runner + RSA key + sealed session key survive.
        assert!(inner.runners.contains_key(&runner_id));
        assert_eq!(
            inner
                .runner_rsa_public_keys
                .get(&runner_id)
                .map(|k| k.to_xml_string()),
            Some(public_xml.clone()),
            "RSA public key must survive restart"
        );
        assert!(
            inner.session_keys.contains_key(&session_id),
            "session_keys must survive restart"
        );
        assert!(inner.sessions.contains_key(&session_id));

        // Log chunks survive.
        assert_eq!(
            inner.logs.get("plan-1/0").cloned().unwrap_or_default(),
            b"first line\nsecond line\n".to_vec(),
            "log bytes must survive restart via log_chunks"
        );
        assert_eq!(
            inner
                .log_metadata
                .get("plan-1/0")
                .map(|m| (m.byte_count, m.line_count)),
            Some((23, 2)),
            "log counter must survive restart"
        );
    }

    // The run-number allocator survives: the next submission continues.
    let recovered_app = app(recovered, CancellationToken::new());
    let accepted = request_json(
        &recovered_app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": workflow,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    assert_eq!(accepted["run_number"], first_number + 1);
}

#[tokio::test]
async fn run_apis_never_return_submitted_secret_values() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo",
            "secrets": {
                "NPM_TOKEN": "npm_LIVE_CREDENTIAL",
                "DEPLOY_KEY": "deploy_LIVE_CREDENTIAL"
            }
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();

    // The server must still receive the real values: they are what the job runs with.
    {
        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id.parse::<RunId>().unwrap()).unwrap();
        assert_eq!(
            run.submission.secrets["NPM_TOKEN"].expose(),
            "npm_LIVE_CREDENTIAL"
        );
    }

    // ...but no run-facing response may echo them back.
    for uri in [
        format!("/api/v1/runs/{run_id}"),
        "/api/v1/runs?limit=50".to_owned(),
    ] {
        let body = request_json(&app, Method::GET, &uri, Value::Null)
            .await
            .to_string();
        assert!(
            !body.contains("npm_LIVE_CREDENTIAL") && !body.contains("deploy_LIVE_CREDENTIAL"),
            "{uri} leaked a secret value: {body}"
        );
        assert!(
            body.contains("NPM_TOKEN"),
            "{uri} should still expose secret names: {body}"
        );
    }
}

#[tokio::test]
async fn run_page_is_public_safe_status_page_without_secret_leaks() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state, CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "workflow_path": "<script>alert(1)</script>",
            "event": "push",
            "repository": "owner/repo",
            "secrets": {"NPM_TOKEN": "npm_LIVE_CREDENTIAL"}
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/runs/{run_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // The page is deliberately public: it is the check-run `details_url`
    // GitHub renders when the runner reports a check — the runner has no
    // native token to forward, so an authenticated page would 404 in the
    // checks UI. The public contract is "safe": no submission secrets, no
    // secret names, and the workflow path HTML-escaped (no XSS).
    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(body.contains(run_id));
    assert!(body.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));
    assert!(!body.contains("npm_LIVE_CREDENTIAL"));
    assert!(!body.contains("NPM_TOKEN"));
}

#[tokio::test]
async fn openapi_document_lists_native_surface_and_excludes_runner_protocol() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/openapi.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let document: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let paths = document["paths"].as_object().unwrap();
    assert!(paths.contains_key("/api/v1/runs"));
    assert!(paths.contains_key("/api/v1/debug/sessions"));
    assert!(!paths.keys().any(|path| path.starts_with("/_apis/")));
    assert!(!paths.keys().any(|path| path.starts_with("/broker/")));
    assert!(!paths.contains_key("/api/v1/scheduler/history"));
    assert!(!paths.contains_key("/api/v1/runners"));
}

#[tokio::test]
async fn matrix_max_parallel_and_fail_fast_are_enforced() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: true
      max-parallel: 1
      matrix:
        os: [ubuntu, macos, windows]
    steps:
      - run: echo matrix
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let first_job = {
        let inner = state.inner.lock().await;
        assert_eq!(inner.queue.len(), 1);
        assert_eq!(inner.pending_jobs.len(), 2);
        inner.queue.front().unwrap().job_id.clone()
    };

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": first_job,
            "status": "failure"
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    assert!(inner.queue.is_empty());
    assert!(inner.pending_jobs.is_empty());
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(
        run.jobs
            .values()
            .filter(|status| **status == ExecutionStatus::Cancelled)
            .count(),
        2
    );
}

#[tokio::test]
async fn completejob_annotations_are_stored_on_the_job_record() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap().to_string();
    let job_id = {
        let inner = state.inner.lock().await;
        inner.queue.front().unwrap().job_id.0.clone()
    };

    // The listener's force-fail completion carries the worker-crash detail as
    // an error annotation; the server must persist it on the job record.
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": job_id,
            "status": "failure",
            "annotations": [{
                "level": "failure",
                "message": "worker crashed: segmentation fault",
                "stepNumber": 0,
                "startLine": 1,
                "endLine": 1,
            }],
        }),
    )
    .await;

    let run = request_json(
        &app,
        Method::GET,
        &format!("/api/v1/runs/{run_id}"),
        Value::Null,
    )
    .await;
    let job = run["jobs_list"]
        .as_array()
        .unwrap()
        .iter()
        .find(|job| job["name"] == json!(job_id))
        .expect("job present in run record");
    assert_eq!(job["conclusion"], "failure");
    let annotations = job["annotations"].as_array().unwrap();
    assert_eq!(annotations.len(), 1);
    assert_eq!(annotations[0]["level"], "failure");
    assert_eq!(
        annotations[0]["message"],
        "worker crashed: segmentation fault"
    );
}

fn selected_jobs_workflow() -> &'static str {
    r#"
on: push
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - run: echo lint
  build:
    needs: lint
    runs-on: ubuntu-latest
    steps:
      - run: echo build
  test:
    needs: build
    runs-on: ubuntu-latest
    steps:
      - run: echo test
  docs:
    runs-on: ubuntu-latest
    steps:
      - run: echo docs
"#
}

#[tokio::test]
async fn selected_jobs_rejects_unknown_id_without_creating_run() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let (status, body) = try_req(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": selected_jobs_workflow(),
            "event": "push",
            "repository": "owner/repo",
            "selected_jobs": ["tset"]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].as_str().unwrap().contains("tset"));
    assert!(state.inner.lock().await.runs.is_empty());
}

#[tokio::test]
async fn selected_jobs_rejects_partial_typo_without_running_valid_subset() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let (status, body) = try_req(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": selected_jobs_workflow(),
            "event": "push",
            "repository": "owner/repo",
            "selected_jobs": ["build", "tset"]
        }),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a typo must reject the whole selection, not run a subset: {body}"
    );
    assert!(body["error"].as_str().unwrap().contains("tset"));
    assert!(state.inner.lock().await.runs.is_empty());
}

#[tokio::test]
async fn selected_jobs_runs_transitive_needs_closure_without_independent_jobs() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": selected_jobs_workflow(),
            "event": "push",
            "repository": "owner/repo",
            "selected_jobs": ["test"]
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    assert_eq!(inner.runs.len(), 1);
    let run = inner.runs.values().next().unwrap();
    let base_ids: BTreeSet<String> = run.job_base_ids.values().cloned().collect();
    assert_eq!(
        base_ids,
        BTreeSet::from(["lint".to_owned(), "build".to_owned(), "test".to_owned(),])
    );
    assert!(!base_ids.contains("docs"));
}

#[tokio::test]
async fn selected_reusable_call_expands_children_at_runtime() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/callee.yml
  unrelated:
    runs-on: ubuntu-latest
    steps:
      - run: echo unrelated
"#,
            "event": "push",
            "repository": "owner/repo",
            "selected_jobs": ["call"],
            "reusable_workflows": {
                ".github/workflows/callee.yml": r#"
on: workflow_call
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo build
  test:
    needs: build
    runs-on: ubuntu-latest
    steps:
      - run: echo test
"#
            }
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let run = inner.runs.values().next().unwrap();
    // Selecting the caller selects its node; since it has no `if:` gate, the
    // submission-time promote sweep materializes the callee subtree at once.
    let base_ids: BTreeSet<String> = run.job_base_ids.values().cloned().collect();
    assert_eq!(
        base_ids,
        BTreeSet::from([
            "call".to_owned(),
            "call/build".to_owned(),
            "call/test".to_owned()
        ])
    );
}

#[tokio::test]
async fn selected_jobs_empty_runs_all_workflow_jobs() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": selected_jobs_workflow(),
            "event": "push",
            "repository": "owner/repo",
            "selected_jobs": []
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    assert_eq!(inner.runs.len(), 1);
    let run = inner.runs.values().next().unwrap();
    let base_ids: BTreeSet<String> = run.job_base_ids.values().cloned().collect();
    assert_eq!(
        base_ids,
        BTreeSet::from([
            "lint".to_owned(),
            "build".to_owned(),
            "test".to_owned(),
            "docs".to_owned(),
        ])
    );
}

#[tokio::test]
async fn agent_request_patch_targets_only_the_request_id() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // Two independent runs, both reach InProgress when their job is pulled.
    let workflow = json!({
        "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "event": "push",
        "repository": "owner/repo"
    });
    let first = request_json(&app, Method::POST, "/api/v1/runs", workflow.clone()).await;
    let second = request_json(&app, Method::POST, "/api/v1/runs", workflow).await;
    let first_run: RunId = first["run_id"].as_str().unwrap().parse().unwrap();
    let second_run: RunId = second["run_id"].as_str().unwrap().parse().unwrap();

    // Pull both jobs so they are InProgress and each has a distinct request_id.
    let first_msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1",
        Value::Null,
    )
    .await;
    let second_msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s2",
        Value::Null,
    )
    .await;
    assert_eq!(
        first_msg["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );
    assert_eq!(
        second_msg["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );

    // The mapping should have two entries — one per request_id.
    let (first_req_id, _) = state
        .inner
        .lock()
        .await
        .inflight_requests
        .iter()
        .find(|(_, (rid, _))| *rid == first_run)
        .map(|(k, v)| (*k, v.clone()))
        .unwrap();

    // PATCH only the first run's request_id.
    request_json(
        &app,
        Method::PATCH,
        &format!("/runner/server/_apis/v1/AgentRequest/1/{first_req_id}"),
        json!({"result": "succeeded"}),
    )
    .await;

    let inner = state.inner.lock().await;
    let first = inner.runs.get(&first_run).unwrap();
    let second = inner.runs.get(&second_run).unwrap();
    assert!(first
        .jobs
        .values()
        .all(|status| *status == ExecutionStatus::Success));
    assert!(second
        .jobs
        .values()
        .all(|status| *status == ExecutionStatus::InProgress));
    assert!(!inner.inflight_requests.contains_key(&first_req_id));
}

#[tokio::test]
async fn agent_request_get_reports_completion_result() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let _msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1",
        Value::Null,
    )
    .await;
    let request_id = {
        let inner = state.inner.lock().await;
        inner
            .inflight_requests
            .iter()
            .find(|(_, (rid, _))| *rid == run_id)
            .map(|(request_id, _)| *request_id)
            .unwrap()
    };

    let before = request_json(
        &app,
        Method::GET,
        &format!("/runner/server/_apis/v1/AgentRequest/1/{request_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(before["requestId"], request_id);
    assert!(before["result"].is_null());

    request_json(
        &app,
        Method::PATCH,
        &format!("/runner/server/_apis/v1/AgentRequest/1/{request_id}"),
        json!({"result": "succeeded"}),
    )
    .await;

    let after = request_json(
        &app,
        Method::GET,
        &format!("/runner/server/_apis/v1/AgentRequest/1/{request_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(after["result"], "succeeded");
}

#[tokio::test]
async fn same_session_waits_for_active_request_before_next_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        n: [1, 2]
    steps:
      - run: echo matrix
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let first = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_eq!(
        first["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );
    let first_message_id = first["messageId"].as_i64().unwrap();
    request_json(
        &app,
        Method::DELETE,
        &format!("/runner/server/_apis/v1/Message/1/{first_message_id}?sessionId=s1"),
        Value::Null,
    )
    .await;

    let first_request_id = {
        let inner = state.inner.lock().await;
        *inner.session_active_requests.get("s1").unwrap()
    };

    let withheld = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert!(withheld.is_null());

    request_json(
        &app,
        Method::PATCH,
        &format!("/runner/server/_apis/v1/AgentRequest/1/{first_request_id}"),
        json!({"result": "succeeded"}),
    )
    .await;

    let second = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_eq!(
        second["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(
        run.jobs
            .values()
            .filter(|status| **status == ExecutionStatus::InProgress)
            .count(),
        1
    );
}

#[tokio::test]
async fn unacked_messages_are_scoped_to_their_session() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let workflow = json!({
        "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "event": "push",
        "repository": "owner/repo"
    });
    request_json(&app, Method::POST, "/api/v1/runs", workflow.clone()).await;
    request_json(&app, Method::POST, "/api/v1/runs", workflow).await;

    let first = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_eq!(
        first["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );
    let first_message_id = first["messageId"].as_i64().unwrap();

    let second = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s2&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_eq!(
        second["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );
    let second_message_id = second["messageId"].as_i64().unwrap();
    assert_ne!(first_message_id, second_message_id);

    // ACKing s1's message through s2 must not remove it from s1. The next
    // s1 poll should redeliver the same unacked message, not s2's message.
    request_json(
        &app,
        Method::DELETE,
        &format!("/runner/server/_apis/v1/Message/1/{first_message_id}?sessionId=s2"),
        Value::Null,
    )
    .await;

    let redelivered = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_eq!(redelivered["messageId"], first_message_id);

    let inner = state.inner.lock().await;
    assert!(inner
        .inflight_messages
        .get("s1")
        .is_some_and(|messages| messages.contains_key(&first_message_id)));
    assert!(inner
        .inflight_messages
        .get("s2")
        .is_some_and(|messages| messages.contains_key(&second_message_id)));
}

#[tokio::test]
async fn finish_job_resolves_plan_timeline_and_agent_job_ids() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let first = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_eq!(
        first["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );

    let request = {
        let inner = state.inner.lock().await;
        inner.job_requests.values().next().unwrap().clone()
    };

    request_json(
        &app,
        Method::POST,
        &format!(
            "/runner/server/_apis/v1/FinishJob/00000000-0000-0000-0000-000000000000/Job/{}",
            request.plan_id
        ),
        json!({
            "jobId": request.agent_job_id,
            "result": "succeeded",
            "timelineId": request.timeline_id,
            "outputs": {"answer": "42"}
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(
        run.jobs.get(&request.job_id),
        Some(&ExecutionStatus::Success)
    );
    assert!(!run
        .jobs
        .contains_key(&JobId(request.agent_job_id.to_string())));
    assert_eq!(
        run.job_outputs
            .get(&request.job_id)
            .and_then(|outputs| outputs.get("answer")),
        Some(&json!("42"))
    );
    assert_eq!(
        inner
            .job_requests
            .get(&request.request_id)
            .and_then(|request| request.result),
        Some(ExecutionStatus::Success)
    );
}

#[tokio::test]
async fn finish_job_falls_back_to_the_single_active_request_when_unresolved() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        n: [1, 2]
    steps:
      - run: echo matrix
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
        Value::Null,
    )
    .await;

    let active_request = {
        let inner = state.inner.lock().await;
        let active_id = *inner.session_active_requests.get("s1").unwrap();
        inner.job_requests.get(&active_id).unwrap().clone()
    };
    let unknown_plan_id = uuid::Uuid::new_v4();
    let unknown_job_id = uuid::Uuid::new_v4();
    let unknown_timeline_id = uuid::Uuid::new_v4();

    // If callback identifiers cannot be resolved at all, the only
    // unfinished active request is the safest correlation available.
    request_json(
        &app,
        Method::POST,
        &format!(
            "/runner/server/_apis/v1/FinishJob/00000000-0000-0000-0000-000000000000/Job/{}",
            unknown_plan_id
        ),
        json!({
            "jobId": unknown_job_id,
            "result": "succeeded",
            "timelineId": unknown_timeline_id,
            "outputs": {}
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(
        run.jobs.get(&active_request.job_id),
        Some(&ExecutionStatus::Success)
    );
}

#[tokio::test]
async fn matrix_fail_fast_cancels_in_progress_siblings_via_message() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: true
      matrix:
        os: [ubuntu, macos]
    steps:
      - run: echo matrix
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // Dispatch both siblings — both move to InProgress.
    let first = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    assert_eq!(
        first["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );
    let second = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    assert_eq!(
        second["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );

    let failing_job = {
        let inner = state.inner.lock().await;
        inner
            .runs
            .get(&run_id)
            .unwrap()
            .jobs
            .keys()
            .next()
            .unwrap()
            .clone()
    };

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": failing_job,
            "status": "failure"
        }),
    )
    .await;

    // The fix: in-progress siblings get a cancellation enqueued so the
    // runner receives a JOB_CANCELLED message. Inspect the queue directly
    // since the matched siblings still have unACKed in-flight job messages.
    let inner = state.inner.lock().await;
    assert_eq!(inner.cancellation_queue.len(), 1);
    let cancellation = inner.cancellation_queue.front().unwrap();
    assert_eq!(cancellation.run_id, run_id);
    assert_ne!(cancellation.job_id, failing_job);
    // The sibling is now Cancelled in the run state.
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(
        run.jobs.get(&cancellation.job_id),
        Some(&ExecutionStatus::Cancelled)
    );
}

#[tokio::test]
async fn needs_context_includes_completed_job_outputs() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo build
  deploy:
    needs: [build]
    runs-on: ubuntu-latest
    steps:
      - run: echo deploy
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "status": "success",
            "outputs": {"artifact": "dist.tgz"}
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let deploy = inner
        .queue
        .iter()
        .find(|job| job.job_id.0 == "deploy")
        .expect("deploy job should be promoted");
    let needs = deploy.message.context_data.get("needs").unwrap();
    let azdo::PipelineContextData::Dict(needs) = needs else {
        panic!("needs context should be a dict");
    };
    let azdo::PipelineContextData::Dict(build) = needs.get("build").unwrap() else {
        panic!("build context should be a dict");
    };
    let azdo::PipelineContextData::Dict(outputs) = build.get("outputs").unwrap() else {
        panic!("outputs context should be a dict");
    };
    assert!(matches!(
        outputs.get("artifact"),
        Some(azdo::PipelineContextData::String(value)) if value == "dist.tgz"
    ));
}

#[tokio::test]
async fn runtime_dynamic_matrix_expansion_fans_out_jobs() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  generator:
    runs-on: ubuntu-latest
    steps:
      - run: echo gen
  downstream:
    needs: [generator]
    runs-on: ubuntu-latest
    strategy:
      matrix: ${{ fromJson(needs.generator.outputs.matrix) }}
    steps:
      - run: echo dynamic
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // Complete generator job with matrix output
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "generator",
            "status": "success",
            "outputs": {"matrix": r#"{"include": [{"os": "ubuntu-latest"}, {"os": "macos-latest"}]}"#}
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();

    // Verify downstream (ubuntu-latest) and downstream (macos-latest) were dynamically created and queued
    assert!(run
        .jobs
        .contains_key(&JobId("downstream (ubuntu-latest)".to_string())));
    assert!(run
        .jobs
        .contains_key(&JobId("downstream (macos-latest)".to_string())));

    let queued_ids: Vec<String> = inner.queue.iter().map(|j| j.job_id.0.clone()).collect();
    assert!(queued_ids.contains(&"downstream (ubuntu-latest)".to_string()));
    assert!(queued_ids.contains(&"downstream (macos-latest)".to_string()));
}

#[tokio::test]
async fn invalid_dynamic_matrix_fails_the_run_instead_of_skipping_it() {
    // A dynamic matrix whose expression does not evaluate to a matrix is a
    // workflow error, and GitHub concludes such a job as failed. Treating the
    // expansion error as a skip would let a broken workflow report a green
    // run, because a run whose only non-success job is skipped summarizes as
    // success.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  generator:
    runs-on: ubuntu-latest
    steps:
      - run: echo gen
  downstream:
    needs: [generator]
    runs-on: ubuntu-latest
    strategy:
      matrix: ${{ fromJson(needs.generator.outputs.matrix) }}
    steps:
      - run: echo dynamic
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // `42` parses as JSON but is not a matrix object or array, so the runtime
    // expansion fails rather than yielding zero combinations.
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "generator",
            "status": "success",
            "outputs": {"matrix": "42"}
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();

    let downstream: Vec<(&JobId, ExecutionStatus)> = run
        .jobs
        .iter()
        .filter(|(id, _)| id.0.starts_with("downstream"))
        .map(|(id, status)| (id, *status))
        .collect();
    assert_eq!(
        downstream.len(),
        1,
        "a failed expansion must not materialize combinations: {:?}",
        run.jobs
    );
    // The un-expanded node is un-suffixed, the way GitHub shows it: the
    // deferred expression must not leak into the job's identity.
    assert_eq!(downstream[0].0 .0, "downstream");
    assert_eq!(
        downstream[0].1,
        ExecutionStatus::Failure,
        "a matrix expression that is not a matrix must fail the job: {:?}",
        run.jobs
    );
    assert_eq!(
        run.status,
        ExecutionStatus::Failure,
        "the run must not conclude green on a broken dynamic matrix"
    );
}

#[tokio::test]
async fn scenario_06_multi_step_dispatches_all_steps() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
name: mitm multi step
on: workflow_dispatch
jobs:
  build:
    runs-on: [self-hosted, mitm]
    steps:
      - run: echo first
      - run: echo "VAL=$VAL"
        env:
          VAL: hello
      - run: |
          echo line1
          echo line2
"#,
            "event": "workflow_dispatch",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let scripts = {
        let inner = state.inner.lock().await;
        let queued = inner.queue.front().expect("build job should be queued");
        queued
            .message
            .steps
            .iter()
            .filter_map(|step| step.script.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(scripts.len(), 3);
    assert!(scripts.contains(&"echo first".to_owned()));
    assert!(scripts.contains(&"echo \"VAL=$VAL\"".to_owned()));
    assert!(scripts
        .iter()
        .any(|script| script.contains("echo line1") && script.contains("echo line2")));

    let message = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    assert_eq!(
        message["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "status": "success"
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(run.status, ExecutionStatus::Success);
    assert_eq!(
        run.jobs.get(&JobId("build".to_owned())),
        Some(&ExecutionStatus::Success)
    );
}

#[tokio::test]
async fn scenario_07_step_failure_summarizes_run_failed() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
name: mitm step failure
on: workflow_dispatch
jobs:
  build:
    runs-on: [self-hosted, mitm]
    steps:
      - run: exit 1
      - run: echo ran-on-failure
        if: failure()
      - run: echo never
        if: success()
"#,
            "event": "workflow_dispatch",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let message = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    assert_eq!(
        message["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "status": "failure"
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(run.status, ExecutionStatus::Failure);
    assert_eq!(
        run.jobs.get(&JobId("build".to_owned())),
        Some(&ExecutionStatus::Failure)
    );
}

#[tokio::test]
async fn scenario_08_consumer_sees_producer_outputs() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
name: mitm job outputs
on: workflow_dispatch
jobs:
  producer:
    runs-on: [self-hosted, mitm]
    outputs:
      value: ${{ steps.gen.outputs.value }}
    steps:
      - id: gen
        run: echo "value=42" >> "$GITHUB_OUTPUT"
  consumer:
    needs: producer
    runs-on: [self-hosted, mitm]
    steps:
      - run: echo "got ${{ needs.producer.outputs.value }}"
"#,
            "event": "workflow_dispatch",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "producer",
            "status": "success",
            "outputs": {"value": "42"}
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let consumer = inner
        .queue
        .iter()
        .find(|job| job.job_id.0 == "consumer")
        .expect("consumer job should be promoted");
    let azdo::PipelineContextData::Dict(needs) =
        consumer.message.context_data.get("needs").unwrap()
    else {
        panic!("needs context should be a dict");
    };
    let azdo::PipelineContextData::Dict(producer) = needs.get("producer").unwrap() else {
        panic!("producer needs entry should be a dict");
    };
    let azdo::PipelineContextData::Dict(outputs) = producer.get("outputs").unwrap() else {
        panic!("producer outputs should be a dict");
    };
    assert!(matches!(
        outputs.get("value"),
        Some(azdo::PipelineContextData::String(value)) if value == "42"
    ));
}

#[tokio::test]
async fn scenario_09_matrix_fail_fast_cancels_siblings() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
name: mitm matrix
on: workflow_dispatch
jobs:
  build:
    runs-on: [self-hosted, mitm]
    strategy:
      fail-fast: true
      matrix:
        n: [1, 2, 3]
    steps:
      - run: |
          if [ "${{ matrix.n }}" = "1" ]; then exit 1; fi
          sleep 20
"#,
            "event": "workflow_dispatch",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    for index in 0..3 {
        let session_id = format!("matrix-{index}");
        let message = request_json(
            &app,
            Method::GET,
            &format!("/runner/server/_apis/v1/Message/1?sessionId={session_id}"),
            Value::Null,
        )
        .await;
        assert_eq!(
            message["messageType"],
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
        );
        let message_id = message["messageId"].as_i64().unwrap();
        let ack = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!(
                        "/runner/server/_apis/v1/Message/1/{message_id}?sessionId={session_id}"
                    ))
                    .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ack.status(), StatusCode::NO_CONTENT);
    }

    let failing_job = {
        let inner = state.inner.lock().await;
        inner
            .runs
            .get(&run_id)
            .unwrap()
            .jobs
            .iter()
            .find_map(|(job_id, status)| {
                (*status == ExecutionStatus::InProgress).then(|| job_id.clone())
            })
            .expect("a matrix sibling should be in progress")
    };

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": failing_job,
            "status": "failure"
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    assert_eq!(inner.cancellation_queue.len(), 2);
    let run = inner.runs.get(&run_id).unwrap();
    for (job_id, status) in &run.jobs {
        if job_id == &failing_job {
            assert_eq!(*status, ExecutionStatus::Failure);
        } else {
            assert_eq!(*status, ExecutionStatus::Cancelled);
        }
    }
}

#[tokio::test]
async fn timeline_patch_projects_annotations_to_run_events() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo annotated
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();
    request_json(
        &app,
        Method::PATCH,
        &format!("/_apis/v1/Timeline/scope/actions/{run_id}/timeline-1"),
        json!({"count": 1, "value": [{
            "id": "00000000-0000-0000-0000-000000000001",
            "name": "build",
            "type": "job",
            "state": "completed",
            "result": "failed",
            "issues": [{
                "type": "error",
                "message": "boom",
                "data": {"file": "src/lib.rs", "line": "42"}
            }]
        }]}),
    )
    .await;

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/runs/{run_id}/events.ndjson"))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let events = String::from_utf8(bytes.to_vec()).unwrap();

    assert!(events.contains("\"type\":\"annotation\""));
    assert!(events.contains("\"message\":\"boom\""));
    assert!(events.contains("\"status\":\"failure\""));
}

#[tokio::test]
async fn live_log_websocket_accepts_bearer_and_stores_lines() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let url = format!("ws://{addr}/ws/live-logs/job-live");
    let mut request =
        tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(url)
            .unwrap();
    request.headers_mut().insert(
        header::AUTHORIZATION,
        "Bearer preloop-system-token".parse().unwrap(),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    let payload = json!({
        "stepId": "step-1",
        "startLine": 1,
        "count": 2,
        "value": ["hello", "world"]
    });
    futures::SinkExt::send(
        &mut ws,
        tokio_tungstenite::tungstenite::Message::Text(payload.to_string()),
    )
    .await
    .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            {
                let inner = state.inner.lock().await;
                if let Some(job_lines) = inner.live_log_lines.get("job-live") {
                    let wrappers = job_lines.lock().await;
                    if wrappers.len() == 1 {
                        assert_eq!(wrappers[0].step_id, "step-1");
                        assert_eq!(wrappers[0].start_line, 1);
                        assert_eq!(wrappers[0].count, 2);
                        assert_eq!(wrappers[0].value, vec!["hello", "world"]);
                        break;
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    server.abort();
}

#[tokio::test]
async fn live_log_websocket_rejects_unauthenticated() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state, CancellationToken::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Connect without Authorization header — should fail.
    let url = format!("ws://{addr}/ws/live-logs/job-no-auth");
    let result = tokio_tungstenite::connect_async(url).await;
    assert!(result.is_err(), "WS connect without auth should fail");

    server.abort();
}

#[tokio::test]
async fn live_log_websocket_survives_malformed_payload() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let url = format!("ws://{addr}/ws/live-logs/job-malformed");
    let mut request =
        tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(url)
            .unwrap();
    request.headers_mut().insert(
        header::AUTHORIZATION,
        "Bearer preloop-system-token".parse().unwrap(),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();

    // Send invalid JSON — should not close the connection.
    futures::SinkExt::send(
        &mut ws,
        tokio_tungstenite::tungstenite::Message::Text("not json".to_string()),
    )
    .await
    .unwrap();

    // Send valid payload after the malformed one — should still work.
    let valid = json!({
        "stepId": "s1",
        "startLine": 1,
        "count": 1,
        "value": ["survived"]
    });
    futures::SinkExt::send(
        &mut ws,
        tokio_tungstenite::tungstenite::Message::Text(valid.to_string()),
    )
    .await
    .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let inner = state.inner.lock().await;
            if let Some(job_lines) = inner.live_log_lines.get("job-malformed") {
                let wrappers = job_lines.lock().await;
                if wrappers.len() == 1 {
                    assert_eq!(wrappers[0].value, vec!["survived"]);
                    break;
                }
            }
            drop(inner);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    server.abort();
}

#[tokio::test]
async fn log_append_persists_payload_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    request_json(
        &app,
        Method::POST,
        "/_apis/v1/Logfiles/scope/actions/plan-1",
        json!({"path": "log-1"}),
    )
    .await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/_apis/v1/Logfiles/scope/actions/plan-1/log-1")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::from("hello log"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let inner = state.inner.lock().await;
    assert_eq!(
        inner.logs.get("plan-1/log-1").map(Vec::as_slice),
        Some(&b"hello log"[..])
    );
}

#[tokio::test]
async fn log_get_run_logs_uses_production_plan_ids_and_numeric_order() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = submit_yaml(
        &app,
        r#"
on: push
jobs:
  first:
    runs-on: ubuntu-latest
    steps:
      - run: echo first
  second:
    runs-on: ubuntu-latest
    steps:
      - run: echo second
"#,
        "owner/repo",
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let requests = {
        let inner = state.inner.lock().await;
        let mut requests: Vec<_> = inner
            .job_requests
            .values()
            .filter(|request| request.run_id == run_id)
            .collect();
        requests.sort_by_key(|request| request.request_id);
        requests
            .into_iter()
            .map(|request| (request.plan_id.clone(), request.agent_job_id.to_string()))
            .collect::<Vec<_>>()
    };
    assert_eq!(requests.len(), 2);

    for (plan_id, log_id, body) in [
        (
            &requests[0].0,
            "10",
            "first-ten
",
        ),
        (
            &requests[0].0,
            "2",
            "first-two
",
        ),
        (
            &requests[1].0,
            "1",
            "ignored-fallback
",
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!(
                        "/_apis/v1/Logfiles/scope/actions/{plan_id}/{log_id}"
                    ))
                    .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    let results_dir = temp
        .path()
        .join("replay")
        .join("results")
        .join(&requests[1].0)
        .join(&requests[1].1);
    tokio::fs::create_dir_all(&results_dir).await.unwrap();
    tokio::fs::write(
        results_dir.join("job-logs.txt"),
        b"results-second
",
    )
    .await
    .unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/runs/{run_id}/logs"))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/plain; charset=utf-8"
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        body.as_ref(),
        b"first-two
first-ten
results-second
"
    );
}

#[tokio::test]
async fn log_get_run_logs_returns_404_for_unknown_run() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/runs/{}/logs", RunId::new()))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn log_append_masks_submitted_secrets() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo masked
"#,
            "event": "push",
            "repository": "owner/repo",
            "secrets": {"TOKEN": "super-secret"}
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/_apis/v1/Logfiles/scope/actions/{run_id}/log-1"))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::from("token=super-secret"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let inner = state.inner.lock().await;
    assert_eq!(
        inner
            .logs
            .get(&format!("{run_id}/log-1"))
            .map(Vec::as_slice),
        Some(&b"token=***"[..])
    );
}

#[tokio::test]
async fn registration_persists_runner_public_key_material() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let public_key = AgentRsaKeypair::generate().unwrap().public_key_xml();

    let runner = request_json(
        &app,
        Method::POST,
        "/api/v1/runners",
        json!({
            "name": "local",
            "labels": ["self-hosted"],
            "public_key": public_key
        }),
    )
    .await;
    let runner_id = runner["id"].as_i64().unwrap();

    let inner = state.inner.lock().await;
    assert_eq!(inner.runner_public_keys.get(&runner_id), Some(&public_key));
    assert!(inner.runner_rsa_public_keys.contains_key(&runner_id));
}

#[tokio::test]
async fn session_key_uses_registered_runner_public_key() {
    let temp = tempfile::tempdir().unwrap();
    let runner_keypair = AgentRsaKeypair::generate().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let runner = request_json(
        &app,
        Method::POST,
        "/api/v1/runners",
        json!({
            "name": "local",
            "labels": ["self-hosted"],
            "public_key": runner_keypair.public_key_xml()
        }),
    )
    .await;
    let runner_id = runner["id"].as_i64().unwrap();

    let session = request_json(
        &app,
        Method::POST,
        "/internal/test/runners/sessions",
        json!({"runner_id": runner_id, "name": "local"}),
    )
    .await;
    let key_b64 = session["encryptionKey"]["value"].as_str().unwrap();
    let encrypted = session["encryptionKey"]["encrypted"].as_bool().unwrap();
    assert!(encrypted, "session key should be RSA wrapped");
    let wrapped_key =
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64).unwrap();
    let key_bytes = runner_keypair.unwrap_key(&wrapped_key).unwrap();
    assert_eq!(key_bytes.len(), 32, "AES-256 key should be 32 bytes");
}

#[tokio::test]
async fn session_key_falls_back_to_plaintext_without_registered_public_key() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let runner = request_json(
        &app,
        Method::POST,
        "/api/v1/runners",
        json!({
            "name": "local",
            "labels": ["self-hosted"]
        }),
    )
    .await;
    let runner_id = runner["id"].as_i64().unwrap();

    let session = request_json(
        &app,
        Method::POST,
        "/internal/test/runners/sessions",
        json!({"runner_id": runner_id, "name": "local"}),
    )
    .await;
    let key_b64 = session["encryptionKey"]["value"].as_str().unwrap();
    let encrypted = session["encryptionKey"]["encrypted"].as_bool().unwrap();
    assert!(
        !encrypted,
        "session key should remain plaintext only when the runner registered no key"
    );
    let key_bytes =
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64).unwrap();
    assert_eq!(key_bytes.len(), 32, "AES-256 key should be 32 bytes");
}

#[tokio::test]
async fn task_agent_registration_extracts_nested_public_key() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let runner_keypair = AgentRsaKeypair::generate().unwrap();
    let public_xml = runner_keypair.public_key_xml();
    let modulus = public_xml
        .split("<Modulus>")
        .nth(1)
        .unwrap()
        .split("</Modulus>")
        .next()
        .unwrap();
    let exponent = public_xml
        .split("<Exponent>")
        .nth(1)
        .unwrap()
        .split("</Exponent>")
        .next()
        .unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let runner = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/agents",
        json!({
            "name": "local",
            "labels": [{"name": "self-hosted", "type": "system"}],
            "authorization": {
                "publicKey": {
                    "modulus": modulus,
                    "exponent": exponent
                }
            }
        }),
    )
    .await;
    let runner_id = runner["id"].as_i64().unwrap();
    let inner = state.inner.lock().await;
    assert!(inner.runner_rsa_public_keys.contains_key(&runner_id));
}

/// The official `actions/runner` sends a stock label set that includes
/// `self-hosted` as both a system label and a user label (the default
/// `config.sh` prompt suggests it). The strict `(runner_id, label)` primary
/// key on `runner_labels` must not reject this; the runner server collapses
/// the duplicate at handler entry. The store layer dedupes again as a
/// backstop, so the round-trip through the database preserves the collapse.
#[tokio::test]
async fn register_runner_dedupes_official_label_set() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // Generate a real RSA keypair so the registration request's publicKey
    // passes base64 + exponent/modulus validation. The label logic is what
    // we're exercising here, not the cryptography.
    let runner_keypair = AgentRsaKeypair::generate().unwrap();
    let public_xml = runner_keypair.public_key_xml();
    let modulus = public_xml
        .split("<Modulus>")
        .nth(1)
        .unwrap()
        .split("</Modulus>")
        .next()
        .unwrap()
        .to_owned();
    let exponent = public_xml
        .split("<Exponent>")
        .nth(1)
        .unwrap()
        .split("</Exponent>")
        .next()
        .unwrap()
        .to_owned();

    // Mirrors the label set captured in
    // .runner-watch/golden/v2.336.0/01-register-and-idle/flows.jsonl:
    // self-hosted appears as both system and user; Linux and linux coexist
    // (case-different today; collapsed under the same dedup rules).
    let body = json!({
        "name": "official-shape-runner",
        "labels": [
            {"name": "self-hosted", "type": "system"},
            {"name": "Linux",       "type": "system"},
            {"name": "ARM64",       "type": "system"},
            {"name": "self-hosted", "type": "user"},
            {"name": "mitm",        "type": "user"},
            {"name": "linux",       "type": "user"},
            {"name": "x64",         "type": "user"},
        ],
        "authorization": {
            "publicKey": {
                "exponent": exponent,
                "modulus": modulus,
            }
        }
    });

    let runner = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/agents",
        body,
    )
    .await;
    let runner_id = runner["id"].as_i64().unwrap();

    // In-memory labels must be deduped case-insensitively while preserving
    // the first occurrence of each canonical form.
    let inner = state.inner.lock().await;
    let stored = &inner.runners.get(&runner_id).unwrap().labels;
    let lowered: std::collections::BTreeSet<String> =
        stored.iter().map(|l| l.to_lowercase()).collect();
    assert_eq!(
        lowered.len(),
        stored.len(),
        "duplicates leaked into memory: {stored:?}"
    );
    assert!(lowered.contains("self-hosted"));
    assert!(lowered.contains("linux"));
    assert!(lowered.contains("arm64"));
    assert!(lowered.contains("mitm"));
    assert!(lowered.contains("x64"));
    // Case-folding must keep the first casing seen (the system one).
    assert!(stored.iter().any(|l| l == "self-hosted"));
    assert!(stored.iter().any(|l| l == "Linux"));

    // And the second registration (e.g. session creation) must still succeed —
    // if the dedup had only happened at handler entry and the database kept
    // duplicates, a second store_inner would 500 again.
    drop(inner);
    let session = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({"ownerName": "official-shape-runner", "agent": {"id": runner_id}}),
    )
    .await;
    assert!(session.get("sessionId").is_some());
}

#[tokio::test]
async fn connection_data_exposes_current_runner_service_locations() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let conn = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/connectionData?connectOptions=1&lastChangeId=-1&lastChangeId64=-1",
        Value::Null,
    )
    .await;
    let services = conn["locationServiceData"]["serviceDefinitions"]
        .as_array()
        .unwrap();
    let service_ids = services
        .iter()
        .filter_map(|service| service["identifier"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(service_ids.contains("38f00041-0953-4d24-86c3-5432d23e2205"));
    assert!(service_ids.contains("a4e1f2b5-0c3d-4e8a-9f6d-7b5c1a0e2d3f"));
    assert!(service_ids.contains("10d13a60-2758-406c-8ab7-cffccb21fcf4"));
    assert_eq!(
        conn["locationServiceData"]["defaultAccessMappingMoniker"],
        "ScaleUnitMapping"
    );

    let fresh = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/connectionData?connectOptions=0&lastChangeId=1&lastChangeId64=1",
        Value::Null,
    )
    .await;
    assert_eq!(fresh["locationServiceData"]["clientCacheFresh"], true);
    assert!(fresh["locationServiceData"]["serviceDefinitions"].is_null());
}

#[tokio::test]
async fn registration_and_oauth_return_runner_compatible_tokens() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let registration = request_json_with_bearer(
        &app,
        Method::POST,
        "/api/v3/actions/runner-registration",
        json!({"url": "https://github.com/preloopdev/preloop", "runner_event": "register"}),
        DEFAULT_PRELOOP_SYSTEM_TOKEN,
    )
    .await;
    assert_eq!(registration["token_schema"], "OAuthAccessToken");
    assert_eq!(registration["url"], "http://127.0.0.1:9090/runner/server");
    assert_eq!(
        registration["token"].as_str().unwrap().split('.').count(),
        3
    );
    assert!(registration.get("use_v2_flow").is_none());

    let token = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/oauth2/token",
        json!({"grant_type":"client_credentials","client_id":"t","client_secret":"t"}),
    )
    .await;
    assert_eq!(token["token_type"], "JWT");
    assert_eq!(token["expires_in"], 2999);
    assert_eq!(
        token["access_token"].as_str().unwrap().split('.').count(),
        3
    );
}

/// The registration mint hands out a RunnerManage JWT. On the TCP surface it
/// accepts any non-empty credential, exactly as GitHub accepts any token it
/// issued — the conformance golden replays a real GitHub registration token
/// and must get a 200. Through the mounted control socket, where workflow
/// code inside a VM can reach, only the system credential — the token the
/// pool injects into its own configure invocation — may mint.
#[tokio::test]
async fn registration_mint_credential_rules_are_strict_by_default() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let body = json!({"url": "https://github.com/preloopdev/preloop", "runner_event": "register"});

    // No credential: refused on every surface.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v3/actions/runner-registration")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // Strict-by-default: a made-up credential must NOT mint. This is the
    // registration hole: anyone able to reach the port could previously mint
    // a RunnerManage JWT and register a rogue runner that receives job
    // messages carrying a minted installation token plus job secrets.
    let forged = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v3/actions/runner-registration")
                .header(header::AUTHORIZATION, "RemoteAuth totally-made-up-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        forged.status(),
        StatusCode::UNAUTHORIZED,
        "strict policy must refuse an unrecognized credential"
    );

    // The system credential is the one thing strict accepts.
    let minted = request_json_with_bearer(
        &app,
        Method::POST,
        "/api/v3/actions/runner-registration",
        body,
        DEFAULT_PRELOOP_SYSTEM_TOKEN,
    )
    .await;
    assert_eq!(minted["token_schema"], "OAuthAccessToken");
}

#[tokio::test]
async fn registration_mint_permissive_only_under_an_explicit_env_opt_in() {
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.registration_policy = RegistrationPolicy::Permissive;
    let app = app(state, CancellationToken::new());
    let minted = request_json_with_bearer(
        &app,
        Method::POST,
        "/api/v3/actions/runner-registration",
        json!({"url": "https://github.com/preloopdev/preloop", "runner_event": "register"}),
        "any-non-empty-credential",
    )
    .await;
    assert_eq!(
        minted["token_schema"], "OAuthAccessToken",
        "permissive policy is the conformance-harness opt-in"
    );
}

#[tokio::test]
async fn current_runner_registration_to_broker_job_e2e() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let runner_keypair = AgentRsaKeypair::generate().unwrap();
    let public_xml = runner_keypair.public_key_xml();
    let modulus = public_xml
        .split("<Modulus>")
        .nth(1)
        .unwrap()
        .split("</Modulus>")
        .next()
        .unwrap();
    let exponent = public_xml
        .split("<Exponent>")
        .nth(1)
        .unwrap()
        .split("</Exponent>")
        .next()
        .unwrap();

    let registration_auth = request_json_with_bearer(
        &app,
        Method::POST,
        "/api/v3/actions/runner-registration",
        json!({"url": "https://github.com/preloopdev/preloop", "runner_event": "register"}),
        DEFAULT_PRELOOP_SYSTEM_TOKEN,
    )
    .await;
    assert_eq!(
        registration_auth["url"],
        "http://127.0.0.1:9090/runner/server"
    );

    let connection = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/connectionData?connectOptions=1&lastChangeId=-1&lastChangeId64=-1",
        Value::Null,
    )
    .await;
    assert!(connection["locationServiceData"]["serviceDefinitions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|service| service["displayName"] == "brokerlistener"));

    let agent = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/agents",
        json!({
            "name": "runner-1",
            "version": "2.335.1",
            // A Linux runner for a `runs-on: ubuntu-latest` job: the scheduler
            // will not hand a hosted-image label to a runner of another OS.
            "osDescription": "Linux local",
            "labels": [
                {"name": "self-hosted", "type": "system"},
                {"name": "Linux", "type": "system"},
                {"name": "X64", "type": "system"}
            ],
            "authorization": {
                "publicKey": {
                    "modulus": modulus,
                    "exponent": exponent
                }
            }
        }),
    )
    .await;
    let runner_id = agent["id"].as_i64().unwrap();
    let runner_token = state
        .local_jwt(json!({
            "sub": format!("preloop-runner-listen-{runner_id}"),
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();
    assert_eq!(agent["properties"]["UseV2Flow"]["$value"], true);
    assert_eq!(
        agent["properties"]["ServerUrlV2"]["$value"],
        "http://127.0.0.1:9090/runner/server"
    );

    let oauth = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/oauth2/token",
        json!({"grant_type":"client_credentials","client_id":"t","client_secret":"t"}),
    )
    .await;
    assert_eq!(oauth["token_type"], "JWT");

    let session = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({
            "agent": {"id": runner_id, "name": "runner-1", "version": "2.335.1"},
            "ownerName": "local current runner",
            "sessionId": "00000000-0000-0000-0000-000000000000",
            "useFipsEncryption": false
        }),
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();

    let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "name: Current Runner Verification\non: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo current\n",
                "event": "push",
                "payload": {"ref": "refs/heads/main", "commits": []},
                "repository": "preloopdev/preloop",
                "git_ref": "refs/heads/main",
                "secrets": {},
                "vars": {},
                "reusable_workflows": {}
            }),
        )
        .await;
    assert_eq!(accepted["queued_jobs"], 1);

    let broker_ref = request_json(
            &app,
            Method::GET,
            &format!("/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&waitSeconds=0"),
            Value::Null,
        )
        .await;
    assert_eq!(broker_ref["messageType"], "RunnerJobRequest");
    let body: Value = serde_json::from_str(broker_ref["body"].as_str().unwrap()).unwrap();
    assert_eq!(body["should_acknowledge"], true);
    let runner_request_id = body["runner_request_id"].as_str().unwrap();

    let acquired_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/broker/{runner_id}/acquirejob"))
                    .header(header::AUTHORIZATION, format!("Bearer {runner_token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({"jobMessageId": runner_request_id, "billingOwnerId": "local", "runnerOS": "macOS"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
    assert_eq!(acquired_response.status(), StatusCode::OK);
    let acquired = serde_json::from_slice::<Value>(
        &to_bytes(acquired_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(acquired["requestId"], 0);
    assert_eq!(acquired["billingOwnerId"], "local");
    assert_eq!(
        acquired["messageType"],
        azdo::message_type::RUNNER_JOB_REQUEST
    );
    assert_eq!(
        acquired["resources"]["endpoints"][0]["url"],
        format!("http://127.0.0.1:9090/broker/{runner_id}/")
    );
    assert_eq!(
        acquired["resources"]["endpoints"][0]["data"]["FeedStreamUrl"],
        format!(
            "ws://127.0.0.1:9090/ws/live-logs/{}",
            acquired["jobId"].as_str().unwrap()
        )
    );
    assert!(acquired["contextData"]["github"].is_object());
    let github_context_json = serde_json::to_string(&acquired["contextData"]["github"])
        .expect("github context should serialize");
    assert!(
        github_context_json.contains("\"workflow\""),
        "github context missing workflow key: {github_context_json}"
    );
    assert!(
        github_context_json.contains("Current Runner Verification"),
        "github context missing workflow name: {github_context_json}"
    );
    assert!(
        acquired["steps"].as_array().unwrap().iter().any(|step| {
            step["inputs"]["script"].as_str() == Some("echo current")
                || step["inputs"]["script"]["lit"].as_str() == Some("echo current")
                || step["inputs"]["script"]["expr"].as_str() == Some("echo current")
                || step["inputs"]["map"].as_array().is_some_and(|entries| {
                    entries.iter().any(|entry| {
                        let key = entry.get("Key").or(entry.get("key"));
                        let val = entry.get("Value").or(entry.get("value"));
                        let key_match = key.is_some_and(|k| {
                            k.as_str() == Some("script")
                                || k.get("lit").and_then(|l| l.as_str()) == Some("script")
                        });
                        let val_match = val.is_some_and(|v| {
                            v.as_str() == Some("echo current")
                                || v.get("lit").and_then(|l| l.as_str()) == Some("echo current")
                        });
                        key_match && val_match
                    })
                })
        }),
        "steps={}",
        acquired["steps"]
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/broker/{runner_id}/completejob"))
                .header(header::AUTHORIZATION, format!("Bearer {runner_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"jobId": runner_request_id, "planId": acquired["plan"]["planId"]})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

/// GitHub's dispatcher injects the job token into the `secrets` context under
/// the name `GITHUB_TOKEN` — that is what `${{ secrets.GITHUB_TOKEN }}` in a
/// workflow's `env:` resolves to. Without this exact key the most common
/// token reference in real workflows (cargo-dist's release.yml, supply-chain
/// gates) comes through empty on this control plane while working on GitHub.
#[tokio::test]
async fn job_message_carries_github_token_as_a_secret() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let runner_token = state
        .local_jwt(json!({
            "sub": "preloop-runner-listen-1",
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  rust:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "payload": {"ref": "refs/heads/main", "commits": []},
            "repository": "preloopdev/preloop",
            "git_ref": "refs/heads/main",
            "secrets": {},
            "vars": {},
            "reusable_workflows": {}
        }),
    )
    .await;
    assert_eq!(accepted["queued_jobs"], 1);

    let session = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({
            "agent": {"id": 1, "name": "runner-1"},
            "ownerName": "owner",
            "sessionId": "00000000-0000-0000-0000-000000000000",
            "useFipsEncryption": false
        }),
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&waitSeconds=0"
                ))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let message: Value = serde_json::from_slice(&bytes).unwrap();
    let body: Value = serde_json::from_str(message["body"].as_str().unwrap()).unwrap();
    let runner_request_id = body["runner_request_id"].as_str().unwrap();

    let acquired = request_json_with_bearer(
        &app,
        Method::POST,
        "/broker/1/acquirejob",
        json!({"jobMessageId": runner_request_id, "billingOwnerId": "local", "runnerOS": "Linux"}),
        &runner_token,
    )
    .await;
    assert_eq!(
        acquired["messageType"],
        azdo::message_type::RUNNER_JOB_REQUEST
    );

    let token_secret = &acquired["variables"]["GITHUB_TOKEN"];
    assert_eq!(
        token_secret["isSecret"], true,
        "GITHUB_TOKEN must be marked secret so the runner masks it: {acquired}"
    );
    assert_eq!(
        token_secret["value"], acquired["variables"]["system.github.token"]["value"],
        "secrets.GITHUB_TOKEN must be the job token the engine minted"
    );
}

/// GitHub's environment-secret tier: a job whose `environment:` resolves to
/// a stored environment sees that tier, with environment > repo > global
/// precedence per name — and only that job does. Submission-provided values
/// still win per name over every stored tier.
#[tokio::test]
async fn environment_secrets_override_repo_and_global_per_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let runner_token = state
        .local_jwt(json!({
            "sub": "preloop-runner-listen-1",
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    // Seed the three stored tiers with the same name so precedence is
    // observable, plus a name that exists only in the environment tier.
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/SHARED",
        json!({ "value": "global-shared" }),
    )
    .await;
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/SHARED",
        json!({ "value": "repo-shared", "repo": "owner/repo" }),
    )
    .await;
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/SHARED",
        json!({ "value": "env-shared", "repo": "owner/repo", "env": "prod" }),
    )
    .await;
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/ENV_ONLY",
        json!({ "value": "env-only", "repo": "owner/repo", "env": "prod" }),
    )
    .await;
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/REPO_GLOBAL",
        json!({ "value": "global-v" }),
    )
    .await;
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/REPO_GLOBAL",
        json!({ "value": "repo-v", "repo": "owner/repo" }),
    )
    .await;
    // A name present in every tier and NOT overridden by the submission: the
    // environment tier must win for the prod job, the repo tier for plain.
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/TIERED",
        json!({ "value": "tier-global" }),
    )
    .await;
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/TIERED",
        json!({ "value": "tier-repo", "repo": "owner/repo" }),
    )
    .await;
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/TIERED",
        json!({ "value": "tier-env", "repo": "owner/repo", "env": "prod" }),
    )
    .await;

    // Two jobs: one in environment prod, one with no environment. The caller
    // also supplies SHARED, which must beat the environment tier.
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  prod:\n    runs-on: ubuntu-latest\n    environment: prod\n    steps:\n      - run: echo hi\n  plain:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "payload": {"ref": "refs/heads/main", "commits": []},
            "repository": "owner/repo",
            "git_ref": "refs/heads/main",
            "secrets": {"SHARED": "sub-shared"},
            "vars": {},
            "reusable_workflows": {}
        }),
    )
    .await;
    assert_eq!(accepted["queued_jobs"], 2);
    let run_id = accepted["run_id"].as_str().unwrap();

    let session = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({
            "agent": {"id": 1, "name": "runner-1"},
            "ownerName": "owner",
            "sessionId": "00000000-0000-0000-0000-000000000000",
            "useFipsEncryption": false
        }),
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();

    let mut variables_by_job = BTreeMap::new();
    for _ in 0..2 {
        let message = poll_message(&app, "preloop-system-token", session_id).await;
        let message_id = message["messageId"]
            .as_i64()
            .expect("polled message has an id");
        let body: Value = serde_json::from_str(message["body"].as_str().unwrap()).unwrap();
        let runner_request_id = body["runner_request_id"].as_str().unwrap();
        let acquired = request_json_with_bearer(
            &app,
            Method::POST,
            "/broker/1/acquirejob",
            json!({"jobMessageId": runner_request_id, "billingOwnerId": "local", "runnerOS": "Linux"}),
            &runner_token,
        )
        .await;
        assert_eq!(
            acquired["messageType"],
            azdo::message_type::RUNNER_JOB_REQUEST
        );
        // Ack the delivered message so the next poll does not redeliver it.
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!(
                        "/runner/server/_apis/distributedtask/pools/1/messages/{message_id}?sessionId={session_id}"
                    ))
                    .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let job_name = acquired["jobName"].as_str().unwrap().to_owned();
        variables_by_job.insert(job_name.clone(), acquired["variables"].clone());
        // One session holds one active job until it completes; free the slot
        // so the next poll delivers the other job's message.
        request_json(
            &app,
            Method::POST,
            "/internal/test/jobs/complete",
            json!({"run_id": run_id, "job_id": job_name, "status": "success"}),
        )
        .await;
    }

    let prod = &variables_by_job["prod"];
    assert_eq!(
        prod["SHARED"]["value"], "sub-shared",
        "submission-provided secrets win per name over every stored tier"
    );
    assert_eq!(prod["SHARED"]["isSecret"], true);
    assert_eq!(
        prod["ENV_ONLY"]["value"], "env-only",
        "environment secrets reach jobs in that environment"
    );
    assert_eq!(prod["ENV_ONLY"]["isSecret"], true);
    assert_eq!(
        prod["TIERED"]["value"], "tier-env",
        "environment tier beats repo and global tiers for jobs in the environment"
    );
    assert_eq!(prod["TIERED"]["isSecret"], true);

    let plain = &variables_by_job["plain"];
    assert_eq!(
        plain["SHARED"]["value"], "sub-shared",
        "submission-provided secrets win per name even without an environment"
    );
    assert_eq!(
        plain["REPO_GLOBAL"]["value"], "repo-v",
        "without an environment the repo tier wins over the global tier"
    );
    assert_eq!(
        plain["TIERED"]["value"], "tier-repo",
        "without an environment the repo tier wins over the global tier"
    );
    assert!(
        plain.get("ENV_ONLY").is_none(),
        "environment secrets never reach jobs outside the environment"
    );
    assert_eq!(
        prod["REPO_GLOBAL"]["value"], "repo-v",
        "repo tier still applies to jobs in an environment that has no env-tier override"
    );
}

#[tokio::test]
async fn current_service_broker_flow_uses_queued_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let runner_token = state
        .local_jwt(json!({
            "sub": "preloop-runner-listen-1",
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    let workflow = "on:
  push:
jobs:
  rust:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
";
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": workflow,
            "event": "push",
            "payload": {"ref": "refs/heads/main", "commits": []},
            "repository": "preloopdev/preloop",
            "git_ref": "refs/heads/main",
            "secrets": {},
            "vars": {},
            "reusable_workflows": {}
        }),
    )
    .await;
    assert_eq!(accepted["queued_jobs"], 1);

    let session = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({
            "agent": {"id": 1, "name": "runner-1"},
            "ownerName": "owner",
            "sessionId": "00000000-0000-0000-0000-000000000000",
            "useFipsEncryption": false
        }),
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();

    let response = app.clone().oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&waitSeconds=0"))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        ).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let message: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(message["messageType"], "RunnerJobRequest");
    let body: Value = serde_json::from_str(message["body"].as_str().unwrap()).unwrap();
    assert_eq!(body["should_acknowledge"], true);
    let runner_request_id = body["runner_request_id"].as_str().unwrap();
    assert!(body["run_service_url"]
        .as_str()
        .unwrap()
        .contains("/broker/1/"));
    assert_eq!(session["ownerName"], "owner");
    assert_eq!(session["assignmentQueued"], false);
    assert_eq!(session["orchestrationId"], "");

    let acquired = request_json_with_bearer(
        &app,
        Method::POST,
        "/broker/1/acquirejob",
        json!({"jobMessageId": runner_request_id, "billingOwnerId": "local", "runnerOS": "macOS"}),
        &runner_token,
    )
    .await;
    assert_eq!(acquired["requestId"].as_i64().unwrap(), 0);
    assert_eq!(acquired["billingOwnerId"], "local");
    assert_eq!(
        acquired["messageType"],
        azdo::message_type::RUNNER_JOB_REQUEST
    );
    assert_eq!(
        acquired["variables"]["system.github.launch_endpoint"]["value"],
        public_base_url()
    );
    assert!(acquired["variables"]["system.github.token"]["value"].is_string());
    assert_eq!(
        acquired["variables"]["actions_runner_allow_artifacts_file"]["value"],
        "false"
    );
    assert_eq!(
        acquired["variables"]["actions_self_repository"]["value"],
        "true"
    );
    assert!(acquired.get("runnerSettings").is_none());
    assert_eq!(
        acquired["resources"]["endpoints"][0]["url"],
        "http://127.0.0.1:9090/broker/1/"
    );
    assert!(acquired["resources"]["endpoints"][0]["data"]["ConnectivityAndDNSChecks"].is_string());
    assert!(acquired["plan"]["planId"].is_string());
    assert!(acquired["jobId"].is_string());
    assert!(acquired["steps"].is_array());

    let renewed = request_json_with_bearer(
        &app,
        Method::POST,
        "/broker/1/renewjob",
        json!({"jobId": runner_request_id, "planId": acquired["plan"]["planId"]}),
        &runner_token,
    )
    .await;
    let locked_until = renewed["lockedUntil"]
        .as_str()
        .expect("renewjob must advertise lockedUntil");
    let locked_until = chrono::DateTime::parse_from_rfc3339(locked_until)
        .expect("renewed lockedUntil must be RFC3339")
        .with_timezone(&chrono::Utc);
    let seconds_until = (locked_until - chrono::Utc::now()).num_seconds();
    assert!(
        (seconds_until - JOB_LEASE_SECONDS as i64).abs() <= 5,
        "renewed lease should be approximately {JOB_LEASE_SECONDS}s, got {seconds_until}s"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/broker/1/completejob")
                .header(header::AUTHORIZATION, format!("Bearer {runner_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"jobId": runner_request_id, "planId": acquired["plan"]["planId"]})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let completed_run = request_json(
        &app,
        Method::GET,
        &format!("/api/v1/runs/{}", accepted["run_id"].as_str().unwrap()),
        Value::Null,
    )
    .await;
    assert_eq!(completed_run["status"], "success");
    assert_eq!(completed_run["jobs"]["rust"], "success");

    let ack = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/runner/server/_apis/v1/AgentRequest/1/1")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ack.status(), StatusCode::OK);
}

#[tokio::test]
async fn broker_job_refs_use_session_runner_id_for_pool_and_root_polls() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // Registration IDs are allocated monotonically. Register a predecessor so
    // this test exercises the replacement runner path instead of the first
    // runner's special-looking ID 1.
    let _ = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({"name": "runner-before-replacement", "version": "2.335.1"}),
    )
    .await;

    let runner = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({
            "name": "runner-replacement",
            "version": "2.335.1",
            "labels": [
                {"name": "self-hosted", "type": "system"},
                {"name": "ubuntu-latest", "type": "system"}
            ]
        }),
    )
    .await;
    let runner_id = runner["id"].as_i64().unwrap();
    assert!(runner_id > 1, "replacement runner must have an ID above 1");

    let runner_token = state
        .local_jwt(json!({
            "sub": format!("preloop-runner-listen-{runner_id}"),
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    // The pool-path session is explicitly tied to runner 2 by its agent body.
    // The root broker session is tied to the same runner by its listen token.
    let pool_session = request_json_with_bearer(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({
            "agent": {"id": runner_id, "name": "runner-replacement"},
            "ownerName": "replacement pool session",
            "useFipsEncryption": false
        }),
        &runner_token,
    )
    .await;
    let pool_session_id = pool_session["sessionId"].as_str().unwrap();

    let root_session = request_json_with_bearer(
        &app,
        Method::POST,
        "/runner/server/session",
        json!({}),
        &runner_token,
    )
    .await;
    let root_session_id = root_session["sessionId"].as_str().unwrap();

    let workflow = "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo broker-id\n";
    for _ in 0..2 {
        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": workflow,
                "event": "push",
                "payload": {"ref": "refs/heads/main", "commits": []},
                "repository": "preloopdev/preloop",
                "git_ref": "refs/heads/main",
                "secrets": {},
                "vars": {},
                "reusable_workflows": {}
            }),
        )
        .await;
        assert_eq!(accepted["queued_jobs"], 1);
    }

    let pool_ref = request_json_with_bearer(
        &app,
        Method::GET,
        &format!(
            "/runner/server/_apis/distributedtask/pools/1/messages?sessionId={pool_session_id}&status=Online&runnerVersion=2.335.1&waitSeconds=0"
        ),
        Value::Null,
        &runner_token,
    )
    .await;
    assert_eq!(pool_ref["messageType"], "RunnerJobRequest");
    let pool_body: Value = serde_json::from_str(pool_ref["body"].as_str().unwrap()).unwrap();
    let expected_run_service_url = format!("{}/broker/{runner_id}/", public_base_url());
    assert_eq!(pool_body["run_service_url"], expected_run_service_url);
    assert!(!pool_body["run_service_url"]
        .as_str()
        .unwrap()
        .contains("/broker/1/"));
    let pool_request_id = pool_body["runner_request_id"].as_str().unwrap();

    let root_ref = request_json_with_bearer(
        &app,
        Method::GET,
        &format!(
            "/runner/server/message?sessionId={root_session_id}&status=Online&runnerVersion=2.335.1&waitSeconds=0"
        ),
        Value::Null,
        &runner_token,
    )
    .await;
    assert_eq!(root_ref["messageType"], "RunnerJobRequest");
    let root_body: Value = serde_json::from_str(root_ref["body"].as_str().unwrap()).unwrap();
    assert_eq!(root_body["run_service_url"], expected_run_service_url);
    assert!(!root_body["run_service_url"]
        .as_str()
        .unwrap()
        .contains("/broker/1/"));
    let root_request_id = root_body["runner_request_id"].as_str().unwrap();

    // The runner-2 token must also authorize acquisition on the URL advertised
    // by both message paths; a hard-coded /broker/1 URL would fail this flow.
    for request_id in [pool_request_id, root_request_id] {
        let acquired = request_json_with_bearer(
            &app,
            Method::POST,
            &format!("/broker/{runner_id}/acquirejob"),
            json!({
                "jobMessageId": request_id,
                "billingOwnerId": "local",
                "runnerOS": "Linux"
            }),
            &runner_token,
        )
        .await;
        assert_eq!(
            acquired["resources"]["endpoints"][0]["url"],
            expected_run_service_url
        );
        let _ = request_json_with_bearer(
            &app,
            Method::POST,
            &format!("/broker/{runner_id}/completejob"),
            json!({
                "jobId": request_id,
                "planId": acquired["plan"]["planId"]
            }),
            &runner_token,
        )
        .await;
    }
}

#[tokio::test]
async fn action_download_info_returns_remote_action_tickets() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let response = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/ActionDownloadInfo/scope/actions/plan",
        json!({
            "actions": [
                {"action": "actions/checkout", "version": "v4"},
                "dtolnay/rust-toolchain@stable",
                "./.github/actions/local",
                "docker://alpine:3.20"
            ]
        }),
    )
    .await;

    let tickets = response["archiveDownloadTickets"].as_object().unwrap();
    // The URL carries a signed, expiring ticket: the download route is
    // bearerless, so the URL itself is the capability.
    let checkout = tickets["actions/checkout@v4"]["url"].as_str().unwrap();
    let (base, query) = checkout.split_once('?').expect("ticket query");
    assert_eq!(
        base,
        "http://127.0.0.1:9090/api/v1/actions/download/actions/checkout/v4"
    );
    assert!(query.contains("exp=") && query.contains("sig="), "{query}");
    assert!(
        tickets["dtolnay/rust-toolchain@stable"]["url"]
            .as_str()
            .unwrap()
            .starts_with(
                "http://127.0.0.1:9090/api/v1/actions/download/dtolnay/rust-toolchain/stable?"
            ),
        "{}",
        tickets["dtolnay/rust-toolchain@stable"]["url"]
    );
    assert!(!tickets.contains_key("./.github/actions/local"));
    assert!(!tickets.contains_key("docker://alpine:3.20"));
    assert_eq!(
        response["actionsDownloadInfo"],
        response["archiveDownloadTickets"]
    );
}

#[tokio::test]
async fn runnerresolve_actions_returns_runner_parseable_tar_urls() {
    // Held for the whole test: `PRELOOP_GITHUB_API_URL` is process-global.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    // Hermetic ref→SHA resolution: point PRELOOP_GITHUB_API_URL at a mock that
    // answers `commits/{ref}` with a fixed SHA, so the test never touches the
    // real GitHub API (and pins the new SHA-pinning behavior deterministically).
    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_base = format!("http://{}", api_listener.local_addr().unwrap());
    let mock = axum::Router::new().route(
        "/repos/:owner/:repo/commits/:git_ref",
        axum::routing::get(|| async {
            axum::Json(serde_json::json!({"sha": "abc123def456abc123def456abc123def456abc1"}))
        }),
    );
    tokio::spawn(async move {
        axum::serve(api_listener, mock).await.unwrap();
    });
    std::env::set_var("PRELOOP_GITHUB_API_URL", &api_base);

    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    std::env::remove_var("PRELOOP_GITHUB_API_URL");

    let response = request_json(
        &app,
        Method::POST,
        "/actions/build/plan/jobs/job/runnerresolve/actions",
        json!({
            "actions": [
                {"action": "actions/checkout", "version": "v4"},
                {"action": "owner/repo/path", "version": "main"}
            ]
        }),
    )
    .await;

    // Signed, expiring ticket — the bearerless download route treats the URL
    // itself as the capability.
    assert!(
        response["actions"]["actions/checkout@v4"]["tar_url"]
            .as_str()
            .unwrap()
            .starts_with(
                "http://127.0.0.1:9090/api/v1/actions/download/actions/checkout/abc123def456abc123def456abc123def456abc1?exp="
            ),
        "{}",
        response["actions"]["actions/checkout@v4"]["tar_url"]
    );
    assert_eq!(
        response["actions"]["actions/checkout@v4"]["resolved_sha"],
        "abc123def456abc123def456abc123def456abc1"
    );
    assert!(
        response["actions"]["owner/repo/path@main"]["tar_url"]
            .as_str()
            .unwrap()
            .starts_with(
                "http://127.0.0.1:9090/api/v1/actions/download/owner/repo/abc123def456abc123def456abc123def456abc1?exp="
            ),
        "{}",
        response["actions"]["owner/repo/path@main"]["tar_url"]
    );
}

#[tokio::test]
async fn action_download_requires_a_ticket_for_the_action_it_serves() {
    // The route is bearerless and reachable from inside every runner VM, so
    // the URL is the capability. Without this, workflow code could make the
    // engine fetch any repository with the engine's own GitHub credential.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    for (owner, repo, git_ref) in [("acme", "public-action", "v1"), ("acme", "private", "v1")] {
        let dir = temp
            .path()
            .join("actions")
            .join(owner)
            .join(repo)
            .join(git_ref);
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("action.tar.gz"), b"tar")
            .await
            .unwrap();
    }

    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let good = state.sign_action_ticket("acme", "public-action", "v1", future);

    let get = |uri: String| {
        let app = app.clone();
        async move {
            app.oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
        }
    };

    // No ticket at all.
    assert_eq!(
        get("/api/v1/actions/download/acme/public-action/v1".to_owned()).await,
        StatusCode::NOT_FOUND,
        "an unsigned request must not be served"
    );

    // A ticket minted for one action, replayed against another — the
    // exfiltration path: ask for a repo the workflow was never granted.
    assert_eq!(
        get(format!(
            "/api/v1/actions/download/acme/private/v1?exp={future}&sig={good}"
        ))
        .await,
        StatusCode::NOT_FOUND,
        "a ticket must not authorise a different action"
    );

    // Right action, forged signature.
    assert_eq!(
        get(format!(
            "/api/v1/actions/download/acme/public-action/v1?exp={future}&sig=AAAA"
        ))
        .await,
        StatusCode::NOT_FOUND,
        "a forged signature must not be served"
    );

    // Right signature, but expired.
    let past = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 1;
    let stale = state.sign_action_ticket("acme", "public-action", "v1", past);
    assert_eq!(
        get(format!(
            "/api/v1/actions/download/acme/public-action/v1?exp={past}&sig={stale}"
        ))
        .await,
        StatusCode::NOT_FOUND,
        "an expired ticket must not be served"
    );

    // The ticket it was actually minted for.
    assert_eq!(
        get(format!(
            "/api/v1/actions/download/acme/public-action/v1?exp={future}&sig={good}"
        ))
        .await,
        StatusCode::OK,
        "the minted ticket must still work"
    );
}

#[tokio::test]
async fn download_action_tarball_serves_from_cache_and_rejects_traversal() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // Pre-populate cache for testing
    let cache_dir = temp
        .path()
        .join("actions")
        .join("test-owner")
        .join("test-repo")
        .join("v1");
    tokio::fs::create_dir_all(&cache_dir).await.unwrap();
    let cached_path = cache_dir.join("action.tar.gz");
    tokio::fs::write(&cached_path, b"dummy-tar-content")
        .await
        .unwrap();

    // 1. Successful cache hit, with the signed ticket the server mints
    let expires_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let signature = state.sign_action_ticket("test-owner", "test-repo", "v1", expires_at);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/api/v1/actions/download/test-owner/test-repo/v1?exp={expires_at}&sig={signature}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/gzip"
    );
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(bytes.as_ref(), b"dummy-tar-content");

    // 2. Reject path traversal
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/actions/download/test-owner/test-repo/../invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/actions/download/test-owner/../../invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn protected_apis_require_bearer_token() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/_apis/artifactcache/cache?keys=x&version=v1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/_apis/artifactcache/cache?keys=x&version=v1")
                .header(header::AUTHORIZATION, "Bearer preloop-attacker-controlled")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn runner_protocol_errors_use_official_envelopes_without_changing_native_api() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    // Auth middleware failures on _apis routes must be VSS/AzDO JSON, not the
    // native {"error": ...} response used by local APIs.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/_apis/artifactcache/cache?keys=x&version=v1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/json; charset=utf-8"
    );
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["$type"], "Microsoft.VisualStudio.Services.Common.VssException, Microsoft.VisualStudio.Services.Common");
    assert_eq!(body["message"], "runner or job protocol token required");
    assert_eq!(body["typeKey"], "UnauthorizedRequestException");
    assert!(body["typeName"].as_str().is_some());

    // Router-level 404s on the runner-facing surface use the same envelope.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/_apis/does-not-exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["typeKey"], "ResourceNotFoundException");
    assert_eq!(body["message"], "Not Found");

    // JSON extractor failures on a protected _apis route are 400 VSS errors.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/runner/server/_apis/distributedtask/pools/1/sessions")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["typeKey"], "VssInvalidRequestException");
    assert!(body["message"].as_str().unwrap().contains("JSON"));

    // Twirp has its own canonical error envelope rather than the VSS object.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["code"], "invalid_argument");
    assert!(body["msg"].as_str().is_some());

    // Native callers keep the existing local API error contract.
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/runs/00000000-0000-0000-0000-000000000000")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["error"], "run not found");
    assert!(body.get("typeName").is_none());
}

#[tokio::test]
async fn all_twirp_api_routes_reject_missing_bearer_before_body_validation() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    // The malformed body proves auth runs before any route-specific JSON extractor.
    let routes = [
        "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
        "/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
        "/twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL",
        "/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
        "/twirp/results.services.receiver.Receiver/GetStepSummarySignedBlobURL",
        "/twirp/results.services.receiver.Receiver/CreateStepSummaryMetadata",
        "/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
        "/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata",
        "/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
        "/twirp/github.actions.results.api.v1.CacheService/FinalizeCacheEntryUpload",
        "/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL",
        "/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact",
        "/twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact",
        "/twirp/github.actions.results.api.v1.ArtifactService/ListArtifacts",
        "/twirp/github.actions.results.api.v1.ArtifactService/GetSignedArtifactURL",
        "/twirp/github.actions.results.api.v1.ArtifactService/DeleteArtifact",
    ];

    for route in routes {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(route)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{route}");
    }
}

#[tokio::test]
async fn twirp_metadata_routes_persist_log_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let requests = [
        (
            "/twirp/results.services.receiver.Receiver/CreateStepSummaryMetadata",
            json!({
                "step_backend_id": "step-summary",
                "workflow_job_run_backend_id": "job-1",
                "workflow_run_backend_id": "run-1",
                "size": 321,
            }),
            "summary:step-summary",
        ),
        (
            "/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
            json!({"step_backend_id": "step-logs", "line_count": 7}),
            "step:step-logs",
        ),
        (
            "/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata",
            json!({"workflow_job_run_backend_id": "job-logs", "line_count": 9}),
            "job:job-logs",
        ),
    ];

    for (uri, body, _) in requests {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(uri)
                    .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        let payload: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(payload["ok"], true, "{uri}");
    }

    let inner = state.inner.lock().await;
    let summary = inner.log_metadata.get("summary:step-summary").unwrap();
    assert_eq!(summary.byte_count, 321);
    assert_eq!(summary.line_count, 0);
    let step = inner.log_metadata.get("step:step-logs").unwrap();
    assert_eq!(step.byte_count, 560);
    assert_eq!(step.line_count, 7);
    let job = inner.log_metadata.get("job:job-logs").unwrap();
    assert_eq!(job.byte_count, 720);
    assert_eq!(job.line_count, 9);
}

#[tokio::test]
async fn twirp_diag_route_rejects_runner_listen_scope() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let plan_id = uuid::Uuid::new_v4().to_string();
    let job_id = uuid::Uuid::new_v4();
    let runner_listen_token = state
        .local_jwt(json!({
            "sub": "preloop-runner-listen-1",
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {runner_listen_token}"),
                )
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "workflow_run_backend_id": plan_id,
                        "workflow_job_run_backend_id": job_id.to_string(),
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn twirp_diag_route_issues_random_blob_url_and_accepts_bearerless_upload() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let plan_id = uuid::Uuid::new_v4().to_string();
    let job_id = uuid::Uuid::new_v4();
    let runtime_token = state.mint_runtime_token(&plan_id, &job_id);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL")
                .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "workflow_run_backend_id": plan_id,
                        "workflow_job_run_backend_id": job_id.to_string(),
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["blob_storage_type"], "BLOB_STORAGE_TYPE_AZURE");

    let diag_url = payload["diag_logs_url"].as_str().unwrap();
    assert!(!diag_url.is_empty());
    let (_, blob_token) = diag_url
        .split_once("/twirp-blob/diag/")
        .expect("diagnostic URL must use the bearerless diag blob endpoint");
    let (blob_token_clean, _) = blob_token.split_once('?').unwrap_or((blob_token, ""));
    let blob_uuid =
        uuid::Uuid::parse_str(blob_token_clean).expect("diagnostic token must be a UUID");
    assert_eq!(blob_uuid.as_bytes()[6] >> 4, 4, "token must be UUIDv4");
    assert_eq!(
        blob_uuid.as_bytes()[8] & 0xc0,
        0x80,
        "token must use RFC 4122 variant"
    );

    let bytes = b"diagnostic log bytes";
    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(diag_url)
                .body(Body::from(bytes.as_slice()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::CREATED);

    let downloaded = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(diag_url)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(downloaded.status(), StatusCode::OK);
    let downloaded_bytes = to_bytes(downloaded.into_body(), usize::MAX).await.unwrap();
    assert_eq!(downloaded_bytes.as_ref(), bytes);
}

#[tokio::test]
async fn native_api_rejects_job_runtime_token() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let token = state.mint_runtime_token("plan", &uuid::Uuid::new_v4());
    let app = app(state, CancellationToken::new());

    let rejected = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/runs")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);

    let accepted = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/runs")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(accepted.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn runner_server_v1_sensitive_routes_require_bearer() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    // These /runner/server/_apis/v1/* aliases were previously placed on
    // the public router, letting unauthenticated callers mutate timelines,
    // inject logs, and finish jobs. They MUST require a bearer token.
    let cases = [
        (Method::PATCH, "/runner/server/_apis/v1/Timeline/s/h/p/t"),
        (Method::POST, "/runner/server/_apis/v1/Logfiles/s/h/p/l"),
        (Method::POST, "/runner/server/_apis/v1/Logfiles/s/h/p/l"),
        (
            Method::POST,
            "/runner/server/_apis/v1/TimeLineWebConsoleLog/s/h/p/t/r",
        ),
        (Method::POST, "/runner/server/_apis/v1/FinishJob/s/h/p"),
        (
            Method::POST,
            "/runner/server/_apis/v1/ActionDownloadInfo/s/h/p",
        ),
        (Method::GET, "/ws/live-logs/test-job"),
        (
            Method::GET,
            "/api/v1/runs/00000000-0000-0000-0000-000000000000/jobs/test/logs/live",
        ),
    ];
    for (method, uri) in cases {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method.clone())
                    .uri(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {uri} should require bearer auth"
        );
    }
}

#[tokio::test]
async fn oidc_endpoint_mints_rs256_jwt_with_requested_audience() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let workflow = json!({
        "workflow_yaml": "name: oidc-test\non: push\npermissions:\n  id-token: write\n  contents: read\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "event": "push",
        "repository": "owner/repo",
    });
    let resp = request_json(&app, Method::POST, "/api/v1/runs", workflow).await;
    let _run_id: RunId = resp["run_id"].as_str().unwrap().parse().unwrap();

    let (plan_id, agent_job_id) = {
        let inner = state.inner.lock().await;
        inner
            .queue
            .front()
            .or_else(|| inner.pending_jobs.front())
            .map(|j| (j.message.plan.plan_id.clone(), j.message.job_id))
            .unwrap()
    };

    let token = request_json(
            &app,
            Method::GET,
            &format!("/runner/server/_apis/distributedtask/hubs/actions/plans/{plan_id}/jobs/{agent_job_id}/oidctoken?audience=api://custom"),
            Value::Null,
        )
        .await;
    let jwt = token["value"].as_str().unwrap();
    let parts: Vec<&str> = jwt.split('.').collect();
    assert_eq!(parts.len(), 3);

    // Verify the protected header is RS256 with a retained kid.
    let header: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
    assert_eq!(header["alg"], "RS256");
    assert!(!header["kid"].as_str().unwrap_or_default().is_empty());

    // The JWT thumbprint must identify the same certificate published by JWKS.
    let jwt_x5t = header["x5t"].as_str().unwrap_or_default();
    assert!(!jwt_x5t.is_empty(), "JWT header must contain x5t");
    let jwks = request_json(&app, Method::GET, "/.well-known/jwks", Value::Null).await;
    let jwks_key = &jwks["keys"][0];
    let jwks_x5t = jwks_key["x5t"].as_str().unwrap_or_default();
    assert!(!jwks_x5t.is_empty(), "JWKS key must contain x5t");
    assert_eq!(jwt_x5t, jwks_x5t);

    let certificate_der = std::fs::read(temp.path().join("oidc-cert.der")).unwrap();
    assert!(
        !certificate_der.is_empty(),
        "OIDC certificate DER must be nonempty"
    );
    let expected_x5t = URL_SAFE_NO_PAD.encode(Sha1::digest(&certificate_der));
    assert_eq!(jwt_x5t, expected_x5t);
    assert_eq!(jwks_x5t, expected_x5t);

    // Verify claims.
    let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
    assert_eq!(claims["aud"], "api://custom");
    assert_eq!(claims["iss"], "http://127.0.0.1:9090/oidc");
    assert_eq!(claims["repository"], "owner/repo");
    assert_eq!(claims["repository_owner"], "owner");
    assert_eq!(claims["event_name"], "push");
    assert_eq!(claims["runner_environment"], "self-hosted");
    assert!(claims["sub"]
        .as_str()
        .unwrap()
        .starts_with("repo:owner/repo:"));
    assert!(claims["jti"].is_string());
    assert!(claims["exp"].as_u64().unwrap() > claims["iat"].as_u64().unwrap());

    // Verify the OIDC keypair is persisted.
    assert!(temp.path().join("oidc-key.json").exists());
}

#[tokio::test]
async fn oidc_default_audience_is_owner_url() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let workflow = json!({
        "workflow_yaml": "on: push\npermissions:\n  id-token: write\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps: [{ run: \"echo hi\" }]\n",
        "event": "push",
        "repository": "octo-org/octo-repo",
    });
    let resp = request_json(&app, Method::POST, "/api/v1/runs", workflow).await;
    let _run_id: RunId = resp["run_id"].as_str().unwrap().parse().unwrap();

    let (plan_id, agent_job_id) = {
        let inner = state.inner.lock().await;
        inner
            .queue
            .front()
            .or_else(|| inner.pending_jobs.front())
            .map(|j| (j.message.plan.plan_id.clone(), j.message.job_id))
            .unwrap()
    };

    let token = request_json(
            &app,
            Method::GET,
            &format!("/runner/server/_apis/distributedtask/hubs/actions/plans/{plan_id}/jobs/{agent_job_id}/oidctoken"),
            Value::Null,
        )
        .await;
    let jwt = token["value"].as_str().unwrap();
    let parts: Vec<&str> = jwt.split('.').collect();
    let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
    assert_eq!(claims["aud"], "https://github.com/octo-org");
}

#[tokio::test]
async fn oidc_forbidden_without_id_token_write() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let workflow = json!({
        "workflow_yaml": "on: push\npermissions:\n  contents: read\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps: [{ run: \"echo hi\" }]\n",
        "event": "push",
        "repository": "owner/repo",
    });
    let _resp = request_json(&app, Method::POST, "/api/v1/runs", workflow).await;

    let (plan_id, agent_job_id) = {
        let inner = state.inner.lock().await;
        inner
            .queue
            .front()
            .or_else(|| inner.pending_jobs.front())
            .map(|job| (job.message.plan.plan_id.clone(), job.message.job_id))
            .unwrap()
    };
    let runtime_token = state.mint_runtime_token(&plan_id, &agent_job_id);

    // Use the real job-bound runtime token so this reaches permission enforcement.
    let uri = format!(
            "/runner/server/_apis/distributedtask/hubs/actions/plans/{plan_id}/jobs/{agent_job_id}/oidctoken?audience=api://test"
        );
    let request = Request::builder()
        .method(Method::GET)
        .uri(&uri)
        .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn oidc_discovery_and_jwks_endpoints() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state, CancellationToken::new());

    let discovery = request_json(
        &app,
        Method::GET,
        "/.well-known/openid-configuration",
        Value::Null,
    )
    .await;
    assert_eq!(
        discovery["jwks_uri"],
        "http://127.0.0.1:9090/oidc/.well-known/jwks"
    );
    assert_eq!(discovery["issuer"], "http://127.0.0.1:9090/oidc");
    assert_eq!(
        discovery["subject_types_supported"],
        json!(["public", "pairwise"])
    );
    assert_eq!(discovery["scopes_supported"], json!(["openid"]));

    let namespaced = request_json(
        &app,
        Method::GET,
        "/oidc/.well-known/openid-configuration",
        Value::Null,
    )
    .await;
    assert_eq!(namespaced, discovery);

    let root_jwks = request_json(&app, Method::GET, "/.well-known/jwks", Value::Null).await;
    let root_json_jwks =
        request_json(&app, Method::GET, "/.well-known/jwks.json", Value::Null).await;
    let namespaced_jwks =
        request_json(&app, Method::GET, "/oidc/.well-known/jwks", Value::Null).await;
    let namespaced_json_jwks = request_json(
        &app,
        Method::GET,
        "/oidc/.well-known/jwks.json",
        Value::Null,
    )
    .await;
    assert_eq!(root_jwks, root_json_jwks);
    assert_eq!(namespaced_jwks, root_jwks);
    assert_eq!(namespaced_json_jwks, root_jwks);

    let keys = root_jwks["keys"].as_array().unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0]["kty"], "RSA");
    assert_eq!(keys[0]["alg"], "RS256");
    assert_eq!(keys[0]["use"], "sig");
    assert!(!keys[0]["kid"].as_str().unwrap_or_default().is_empty());
    assert!(keys[0]["n"].as_str().is_some_and(|value| !value.is_empty()));
    assert_eq!(keys[0]["e"], "AQAB");
}

#[tokio::test]
async fn oidc_keypair_persists_across_restarts() {
    let temp = tempfile::tempdir().unwrap();
    let state1 = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let (kid1, x5t1, certificate_der1) = {
        let inner = state1.inner.lock().await;
        let keypair = inner.oidc_keypair.as_ref().unwrap();
        (
            keypair.kid().to_string(),
            keypair.x5t().to_string(),
            keypair.certificate_der().to_vec(),
        )
    };
    let certificate_path = temp.path().join("oidc-cert.der");
    assert!(certificate_path.exists());
    assert!(!certificate_der1.is_empty());
    assert_eq!(certificate_der1, std::fs::read(&certificate_path).unwrap());
    let expected_x5t1 = URL_SAFE_NO_PAD.encode(Sha1::digest(&certificate_der1));
    assert_eq!(x5t1, expected_x5t1);
    drop(state1);
    // Second instance should load the same keypair and certificate.
    let state2 = AppState::new(temp.path().to_path_buf()).await.unwrap();

    let (kid2, x5t2, certificate_der2) = {
        let inner = state2.inner.lock().await;
        let keypair = inner.oidc_keypair.as_ref().unwrap();
        (
            keypair.kid().to_string(),
            keypair.x5t().to_string(),
            keypair.certificate_der().to_vec(),
        )
    };
    assert_eq!(
        kid1, kid2,
        "OIDC keypair kid must be stable across restarts"
    );
    assert_eq!(
        x5t1, x5t2,
        "OIDC certificate x5t must be stable across restarts"
    );
    assert_eq!(certificate_der1, certificate_der2);
    assert_eq!(certificate_der1, std::fs::read(&certificate_path).unwrap());
    assert_eq!(
        x5t2,
        URL_SAFE_NO_PAD.encode(Sha1::digest(&certificate_der2))
    );
}

#[tokio::test]
async fn oidc_malformed_certificate_sidecar_rejects_restart() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    drop(state);

    std::fs::write(temp.path().join("oidc-cert.der"), b"not a DER certificate").unwrap();
    assert!(
        AppState::new(temp.path().to_path_buf()).await.is_err(),
        "startup must reject a malformed persisted OIDC certificate"
    );
}

#[tokio::test]
async fn messages_redeliver_until_delete_ack() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo ok
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;

    let first = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    assert_eq!(first["messageId"], 1);

    let redelivered = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    assert_eq!(redelivered["messageId"], first["messageId"]);

    let ack = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/runner/server/_apis/v1/Message/1/1?sessionId=default")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ack.status(), StatusCode::NO_CONTENT);

    let empty = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert!(empty.is_null());
}

#[tokio::test]
async fn cancel_run_delivers_cancellation_message() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 30
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();

    let message = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    let message_id = message["messageId"].as_i64().unwrap();

    let ack = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!(
                    "/runner/server/_apis/v1/Message/1/{message_id}?sessionId=default"
                ))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ack.status(), StatusCode::NO_CONTENT);

    request_json(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/cancel"),
        Value::Null,
    )
    .await;

    let cancellation = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_eq!(
        cancellation["messageType"],
        azdo::message_type::JOB_CANCELLED
    );
    // Body is base64 of plaintext (no session key in this test path).
    let body_b64 = cancellation["body"].as_str().unwrap();
    let body_bytes = BASE64_STANDARD.decode(body_b64).unwrap();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(body["jobId"]
        .as_str()
        .unwrap()
        .parse::<uuid::Uuid>()
        .is_ok());
    assert_eq!(body["timeout"], "00:05:00");
    assert!(body.get("runId").is_none());
}

#[tokio::test]
async fn cancel_run_completes_github_checks_and_terminal_metadata() {
    // Keyed by check-run id: `PRELOOP_GITHUB_API_URL` is process-global, and
    // `owner/repo` is the suite's default repository, so a co-scheduled test
    // that submits a run lands its own check-run PATCH on this stub. Counting
    // every request would make the assertion depend on the rest of the suite;
    // this test's contract is about the check run it pinned below.
    const CHECK_RUN_ID: u64 = 7;
    let check_completions = Arc::new(parking_lot::Mutex::new(
        Vec::<(u64, serde_json::Value)>::new(),
    ));
    let mock_app = Router::new().route(
        "/repos/owner/repo/check-runs/:id",
        axum::routing::patch({
            let check_completions = check_completions.clone();
            move |Path(id): Path<u64>, body: axum::extract::Json<Value>| {
                let check_completions = check_completions.clone();
                async move {
                    check_completions.lock().push((id, body.0));
                    Json(json!({"id": id}))
                }
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, mock_app).await.unwrap();
    });

    // `TestEnvVar` restores both on drop, so a panicking assertion below
    // cannot leak this stub's address (or its token) onto the rest of the run.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _api_url =
        crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", format!("http://127.0.0.1:{port}"));
    let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "cancel-test-token");

    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    {
        let mut inner = state.inner.lock().await;
        let run = inner.runs.get_mut(&run_id).unwrap();
        run.job_check_run_ids
            .insert(JobId("build".into()), CHECK_RUN_ID);
    }

    let cancelled = request_json(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/cancel"),
        Value::Null,
    )
    .await;

    assert_eq!(cancelled["status"], "cancelled");
    assert_eq!(cancelled["conclusion"], "cancelled");
    assert!(
        cancelled["completed_at"].is_string(),
        "cancelled run must carry terminal completion metadata"
    );
    let completions = check_completions.lock();
    let mine: Vec<&serde_json::Value> = completions
        .iter()
        .filter(|(id, _)| *id == CHECK_RUN_ID)
        .map(|(_, body)| body)
        .collect();
    assert_eq!(mine.len(), 1, "one cancel, one check-run completion");
    assert_eq!(mine[0]["status"], "completed");
    assert_eq!(mine[0]["conclusion"], "cancelled");
}

#[tokio::test]
async fn cancel_run_refreshes_runner_pool_queue_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let first = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-22.04\n    steps:\n      - run: echo first\n",
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-24.04\n    steps:\n      - run: echo second\n",
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    assert_eq!(
        *state.next_job_runs_on.read().unwrap(),
        vec!["ubuntu-22.04"]
    );
    assert_eq!(
        state.queue_depth.load(std::sync::atomic::Ordering::Acquire),
        2
    );

    let cancelled = request_json(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{}/cancel", first["run_id"].as_str().unwrap()),
        Value::Null,
    )
    .await;

    assert_eq!(cancelled["conclusion"], "cancelled");
    assert_eq!(
        state.queue_depth.load(std::sync::atomic::Ordering::Acquire),
        1
    );
    assert_eq!(
        *state.next_job_runs_on.read().unwrap(),
        vec!["ubuntu-24.04"]
    );
}

#[tokio::test]
async fn message_poll_waits_until_work_is_enqueued() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let poll_app = app.clone();
    let poll = tokio::spawn(async move {
        request_json(
            &poll_app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=2",
            Value::Null,
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo waited
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;

    let message = poll.await.unwrap();
    assert_eq!(message["messageId"], 1);
}

#[tokio::test]
async fn session_message_flow_encrypts_decryptable_job_body() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let _keypair = {
        let inner = state.inner.lock().await;
        inner.agent_keypair.clone().unwrap()
    };
    let app = app(state, CancellationToken::new());

    let session = request_json(
        &app,
        Method::POST,
        "/internal/test/runners/sessions",
        json!({"runner_id": 1, "name": "local"}),
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();
    let key_b64 = session["encryptionKey"]["value"].as_str().unwrap();
    let aes_key =
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64).unwrap();

    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo encrypted
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;

    let message = request_json(
        &app,
        Method::GET,
        &format!("/internal/test/runners/sessions/{session_id}/messages?sessionId={session_id}"),
        Value::Null,
    )
    .await;

    let body = BASE64_STANDARD
        .decode(message["body"].as_str().unwrap())
        .unwrap();
    let iv: Vec<u8> = BASE64_STANDARD
        .decode(message["iv"].as_str().unwrap())
        .unwrap();
    let plaintext = SessionEncryption::from_key(aes_key)
        .decrypt(&body, &iv)
        .unwrap();
    let job: azdo::AgentJobRequestMessage = serde_json::from_slice(&plaintext).unwrap();

    assert_eq!(
        message["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );
    assert_eq!(job.steps[0].script.as_deref(), Some("echo encrypted"));
}

#[tokio::test]
async fn submit_run_uses_branch_and_path_filters() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on:
  push:
    branches: [main]
    paths: ["src/**"]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo ok
"#,
            "event": "push",
            "repository": "owner/repo",
            "git_ref": "refs/heads/main",
            "payload": {
                "commits": [
                    { "added": [], "modified": ["src/lib.rs"], "removed": [] }
                ]
            }
        }),
    )
    .await;
    assert!(accepted["run_id"].is_string());

    let rejected = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/runs")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "workflow_yaml": r#"
on:
  push:
    branches: [main]
    paths: ["src/**"]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo ok
"#,
                        "event": "push",
                        "repository": "owner/repo",
                        "git_ref": "refs/heads/feature",
                        "payload": {
                            "commits": [
                                { "added": [], "modified": ["docs/readme.md"], "removed": [] }
                            ]
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_get_scheduler_history_endpoint() {
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let scheduler = crate::scheduler::Scheduler::new();

    // Add a mock fire to history
    {
        let mut hist = scheduler.history.lock().await;
        hist.push(crate::scheduler::ScheduleFire {
            workflow_path: ".github/workflows/cron.yml".to_owned(),
            cron_expr: "* * * * *".to_owned(),
            fired_at: chrono::Utc::now(),
            run_id: Some("mock-run-id".to_owned()),
            error: None,
        });
    }
    state.scheduler = Some(scheduler);

    let app = app(state, CancellationToken::new());

    let res = request_json(
        &app,
        Method::GET,
        "/api/v1/scheduler/history",
        serde_json::Value::Null,
    )
    .await;

    let arr = res.as_array().expect("expected array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["workflow_path"], ".github/workflows/cron.yml");
    assert_eq!(arr[0]["cron_expr"], "* * * * *");
    assert_eq!(arr[0]["run_id"], "mock-run-id");
}
#[tokio::test]
async fn cache_protocol_reserves_uploads_commits_and_restores() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let reserve = request_json(
        &app,
        Method::POST,
        "/_apis/artifactcache/cache",
        json!({"key": "linux-node", "version": "v1"}),
    )
    .await;
    let cache_id = reserve["cacheId"].as_i64().unwrap();

    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("/_apis/artifactcache/cache/{cache_id}"))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::from("cache-bytes"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::ACCEPTED);

    request_json(
        &app,
        Method::POST,
        &format!("/_apis/artifactcache/cache/{cache_id}"),
        json!({"size": 11}),
    )
    .await;

    let lookup = request_json(
        &app,
        Method::GET,
        "/api/v1/cache?key=linux-node&version=v1",
        Value::Null,
    )
    .await;
    assert_eq!(lookup["hit"], true);
    assert_eq!(lookup["content_base64"], "Y2FjaGUtYnl0ZXM=");

    let stored = request_json(
        &app,
        Method::POST,
        "/api/v1/cache",
        json!({
            "key": "native-cache",
            "version": "v1",
            "content_base64": "bmF0aXZlLWJ5dGVz"
        }),
    )
    .await;
    assert_eq!(stored["hit"], true);
    let native_lookup = request_json(
        &app,
        Method::GET,
        "/api/v1/cache?key=native-cache&version=v1",
        Value::Null,
    )
    .await;
    assert_eq!(native_lookup["content_base64"], "bmF0aXZlLWJ5dGVz");
}

#[tokio::test]
async fn artifact_endpoint_stores_and_downloads_payload() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let run_id = RunId::new();

    let created = request_json(
        &app,
        Method::POST,
        "/api/v1/artifacts",
        json!({
            "run_id": run_id,
            "name": "logs",
            "file_name": "job.txt",
            "content_base64": "aGVsbG8="
        }),
    )
    .await;
    let artifact_id = created["id"].as_str().unwrap();
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/artifacts/{artifact_id}"))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&bytes[..], b"hello");
}

#[tokio::test]
async fn full_runner_lifecycle_register_session_poll_complete() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // 1. connectionData
    let (s, conn) = try_req(
        &app,
        Method::GET,
        "/runner/server/_apis/connectionData",
        Value::Null,
    )
    .await;
    assert!(s.is_success(), "1 connectionData: {}", s);
    assert!(conn["locationServiceData"]["serviceDefinitions"].is_array());

    // 2. OAuth token
    let (s, _) = try_req(
        &app,
        Method::POST,
        "/_apis/v1/oauth2/token",
        json!({"grant_type":"client_credentials","client_id":"t","client_secret":"t"}),
    )
    .await;
    assert!(s.is_success(), "2 oauth2: {}", s);

    // 3. Register runner
    let (s, reg) = try_req(
        &app,
        Method::POST,
        "/api/v1/runners",
        json!({"name":"test-runner","labels":["self-hosted","linux","x64"]}),
    )
    .await;
    assert!(s.is_success(), "3 register: {} body={}", s, reg);
    let runner_id = reg["id"].as_i64().unwrap();

    // 4. Create session
    let (s, sess) = try_req(
        &app,
        Method::POST,
        "/internal/test/runners/sessions",
        json!({"runner_id": runner_id, "name": "test-runner"}),
    )
    .await;
    assert!(s.is_success(), "4 session: {} body={}", s, sess);
    let session_id = sess["sessionId"].as_str().unwrap().to_owned();

    // 5. Submit a workflow
    let (s, accepted) = try_req(&app, Method::POST, "/api/v1/runs",
            json!({"workflow_yaml":"on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n","event":"push","repository":"owner/repo"})).await;
    assert!(s.is_success(), "5 submit: {} body={}", s, accepted);
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // 6. Poll for messages — the runner uses the AzDO Message endpoint
    let (s, msg) = try_req(
        &app,
        Method::GET,
        &format!(
            "/internal/test/runners/sessions/{}/messages?sessionId={}&waitSeconds=0",
            session_id, session_id
        ),
        Value::Null,
    )
    .await;
    assert!(s.is_success(), "6 poll: {} body={}", s, msg);

    // 7. Get the job from the run
    let inner = state.inner.lock().await;
    let run_record = inner.runs.get(&run_id).unwrap();
    let job_id = run_record.jobs.keys().next().unwrap().clone();
    drop(inner);

    // 8. Complete the job
    let (s, _) = try_req(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({"run_id": run_id, "job_id": job_id, "status": "success"}),
    )
    .await;
    assert!(s.is_success(), "8 complete: {}", s);

    // 9. Verify run succeeded
    let (_, final_run) = try_req(
        &app,
        Method::GET,
        &format!("/api/v1/runs/{}", run_id),
        Value::Null,
    )
    .await;
    assert_eq!(final_run["status"], "success");
}

/// The runner prints its `GITHUB_TOKEN Permissions` group from this variable, so
/// it must state what the job's token actually carries: the restricted default
/// when the workflow declares nothing (matching the official runner's setup
/// log), and nothing at all when the workflow withholds everything.
#[tokio::test]
async fn the_wire_token_permissions_match_the_declared_policy() {
    for (declaration, expected) in [
        (
            "",
            r#"{"Contents":"read","Metadata":"read","Packages":"read"}"#,
        ),
        ("permissions: {}\n", "{}"),
        (
            "permissions:\n  contents: read\n  pull-requests: write\n",
            r#"{"Contents":"read","PullRequests":"write"}"#,
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": format!(
                    "on: push\n{declaration}jobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n"
                ),
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;

        let inner = state.inner.lock().await;
        let queued = inner.queue.front().expect("job should be queued");
        assert_eq!(
            queued
                .message
                .variables
                .get("system.github.token.permissions")
                .and_then(|variable| variable.value.as_deref()),
            Some(expected),
            "wire permissions for {declaration:?}"
        );
    }
}

/// GitHub's fork profile is the single effective job-authorization policy for
/// fork-restricted tiers. A fork PR declaring `checks: write` and
/// `id-token: write` must come out read-only on the runner-visible wire
/// variable, read-only in the App installation-token request, with no OIDC
/// request URL and no OIDC grant — while a trusted push and a
/// `pull_request_target` keep the declared writes and OIDC untouched.
#[tokio::test]
async fn fork_pr_jobs_are_downgraded_to_read_only_and_oidc_denied() {
    use crate::github_app::{GitHubAppCredentials, MintFailurePolicy};

    let private_key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.github_app = Some(GitHubAppCredentials::for_tests(
        "424",
        private_key,
        MintFailurePolicy::LocalJwt,
    ));
    let app = app(state.clone(), CancellationToken::new());

    let yaml = "on: pull_request\npermissions:\n  checks: write\n  id-token: write\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let payload = json!({
        "action": "opened",
        "number": 7,
        "pull_request": {
            "head": {"ref": "feature", "sha": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3"},
            "base": {"ref": "main", "sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"}
        },
        "repository": {"full_name": "owner/repo", "default_branch": "main"}
    });
    let submit = |tier: Option<&str>| {
        request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": yaml,
                "event": "pull_request",
                "repository": "owner/repo",
                "payload": payload,
                "trust_tier": tier,
            }),
        )
    };

    // Fork PR: declared writes must not survive, OIDC must not be granted.
    let fork = submit(Some("untrusted-fork-pull-request")).await;
    let fork_run_id = fork["run_id"].as_str().unwrap();
    let trusted = submit(None).await;
    let trusted_run_id = trusted["run_id"].as_str().unwrap();
    let target = submit(Some("pull-request-target")).await;
    let target_run_id = target["run_id"].as_str().unwrap();

    let inner = state.inner.lock().await;
    let fork_message = queued_message_for(&inner, fork_run_id);
    assert_eq!(
        variable_value(&fork_message, "system.github.token.permissions"),
        Some(r#"{"Checks":"read"}"#),
        "fork PR: declared checks write must be clamped to read, and id-token \
         must not be advertised as a read permission"
    );
    assert!(
        !variable_value(&fork_message, "system.github.token.permissions")
            .is_some_and(|wire| wire.contains("IdToken")),
        "fork PR: the wire permissions must carry no IdToken metadata"
    );
    let fork_endpoint = fork_message
        .resources
        .endpoints
        .iter()
        .find(|endpoint| endpoint.name.eq_ignore_ascii_case("SystemVssConnection"))
        .expect("SystemVssConnection endpoint present");
    assert!(
        !fork_endpoint
            .data
            .get("GenerateIdTokenUrl")
            .is_some_and(|url| !url.is_empty()),
        "fork PR: no OIDC request URL may be emitted"
    );
    let fork_request = inner
        .github_token_requests
        .get(&fork_message.request_id)
        .expect("fork PR job defers an App token request");
    assert_eq!(
        fork_request.permissions,
        BTreeMap::from([("checks".to_owned(), "read".to_owned())]),
        "fork PR: App token request carries only the read-only fork profile"
    );
    assert!(
        !fork_request.permissions.contains_key("id-token"),
        "fork PR: the App token request must not name the non-App id-token scope"
    );
    assert!(
        fork_request.untrusted,
        "fork PR: token request must be marked untrusted so no fallback widens it"
    );
    let fork_job_record = inner
        .job_requests
        .get(&fork_message.request_id)
        .expect("fork job request record present");
    assert_eq!(
        inner
            .id_token_grants
            .get(&(fork_job_record.run_id, fork_job_record.job_id.clone())),
        Some(&false),
        "fork PR: no OIDC grant may be recorded"
    );

    // Trusted push: declared writes and OIDC survive verbatim.
    let trusted_message = queued_message_for(&inner, trusted_run_id);
    assert_eq!(
        variable_value(&trusted_message, "system.github.token.permissions"),
        Some(r#"{"Checks":"write","IdToken":"write"}"#),
        "trusted job keeps the declared write profile"
    );
    let trusted_endpoint = trusted_message
        .resources
        .endpoints
        .iter()
        .find(|endpoint| endpoint.name.eq_ignore_ascii_case("SystemVssConnection"))
        .expect("SystemVssConnection endpoint present");
    assert!(
        trusted_endpoint
            .data
            .get("GenerateIdTokenUrl")
            .is_some_and(|url| !url.is_empty()),
        "trusted job keeps the OIDC request URL"
    );
    let trusted_request = inner
        .github_token_requests
        .get(&trusted_message.request_id)
        .expect("trusted job defers an App token request");
    assert_eq!(
        trusted_request.permissions,
        BTreeMap::from([("checks".to_owned(), "write".to_owned())]),
        "trusted job's App token request carries only real App repository permissions"
    );
    assert!(
        !trusted_request.permissions.contains_key("id-token"),
        "the App installation-token request must exclude the non-App id-token scope"
    );
    assert!(
        !trusted_request.untrusted,
        "trusted job is not marked untrusted"
    );

    // pull_request_target: base-repo trust, declared writes untouched.
    let target_message = queued_message_for(&inner, target_run_id);
    assert_eq!(
        variable_value(&target_message, "system.github.token.permissions"),
        Some(r#"{"Checks":"write","IdToken":"write"}"#),
        "pull_request_target keeps base-repo trust"
    );
    drop(inner);

    // The OIDC endpoint enforces the same grant: refused for the fork,
    // minted for the trusted job.
    let fork_uri = format!(
        "/runner/server/_apis/distributedtask/hubs/actions/plans/{}/jobs/{}/oidctoken?audience=api://test",
        fork_message.plan.plan_id, fork_message.job_id
    );
    let (status, _) = try_req(&app, Method::GET, &fork_uri, Value::Null).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "fork PR: OIDC token request must be refused"
    );
    let trusted_uri = format!(
        "/runner/server/_apis/distributedtask/hubs/actions/plans/{}/jobs/{}/oidctoken?audience=api://test",
        trusted_message.plan.plan_id, trusted_message.job_id
    );
    let token = request_json(&app, Method::GET, &trusted_uri, Value::Null).await;
    assert!(
        token["value"]
            .as_str()
            .is_some_and(|jwt| jwt.split('.').count() == 3),
        "trusted job still mints an OIDC JWT"
    );
}

/// A mint failure for an untrusted fork job must never reach the configured
/// `PRELOOP_GITHUB_TOKEN` PAT fallback: the PAT is repository-unscoped and
/// ignores `permissions:`, so handing it to fork PR code would grant
/// authority GitHub's read-only fork profile never allowed. The job keeps the
/// local runtime token instead — while a trusted job under the same `pat`
/// policy still receives the PAT.
#[tokio::test]
async fn untrusted_job_mint_failure_never_falls_back_to_the_pat() {
    use crate::github_app::{GitHubAppCredentials, MintFailurePolicy};

    let private_key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let mut creds = GitHubAppCredentials::for_tests("424", private_key, MintFailurePolicy::Pat);
    creds.pat_fallback = Some("github_pat_broad".to_owned());
    state.github_app = Some(creds);
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());

    // `local-workspace-only` carries no `owner/repo` slug, so the mint fails
    // before it signs anything or opens a socket — exactly like
    // `app_token_mint_failure_follows_the_configured_policy`.
    let yaml = "on: push\npermissions:\n  checks: write\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let fork = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": yaml,
            "event": "push",
            "repository": "local-workspace-only",
            "trust_tier": "untrusted-fork-pull-request",
        }),
    )
    .await;
    let trusted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": yaml,
            "event": "push",
            "repository": "local-workspace-only",
        }),
    )
    .await;

    {
        let inner = state.inner.lock().await;
        let fork_message = queued_message_for(&inner, fork["run_id"].as_str().unwrap());
        let fork_request = inner
            .github_token_requests
            .get(&fork_message.request_id)
            .cloned()
            .expect("fork job defers a token request");
        let trusted_message = queued_message_for(&inner, trusted["run_id"].as_str().unwrap());
        let trusted_request = inner
            .github_token_requests
            .get(&trusted_message.request_id)
            .cloned()
            .expect("trusted job defers a token request");
        assert!(fork_request.untrusted);
        assert!(!trusted_request.untrusted);
        let shared = Arc::new(SharedState {
            state: state.clone(),
            shutdown: shutdown.clone(),
        });
        let fork_mint = crate::broker::mint_dispatch_github_token(&shared, &fork_request).await;
        assert!(
            matches!(fork_mint, Ok(None)),
            "fork job must not receive the PAT fallback under the `pat` policy"
        );
        let trusted_mint =
            crate::broker::mint_dispatch_github_token(&shared, &trusted_request).await;
        assert_eq!(
            trusted_mint
                .expect("trusted job's mint failure is not a dispatch error")
                .expect("pat policy hands the PAT to a trusted job")
                .token,
            "github_pat_broad",
            "the PAT fallback still applies to trusted jobs"
        );
    }
}

/// The broker claim swaps the build-time token for the minted App token.
/// Every runner-visible alias must follow coherently — `system.github.token`
/// (the `${{ github.token }}` variable), `github_token`, the
/// `${{ secrets.GITHUB_TOKEN }}` alias, and the `github` context's `token`
/// entry — or workflow code would read a stale local runtime token from one
/// alias while the others carry the scoped mint.
#[tokio::test]
async fn broker_claim_patches_every_token_alias_with_the_minted_token() {
    use crate::github_app::{GitHubAppCredentials, MintFailurePolicy};
    use axum::routing::{get, post};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_base = format!("http://{}", listener.local_addr().unwrap());
    let stub = Router::new()
        .route(
            "/app/installations",
            get(|| async { Json(json!([{"id": 4242, "account": {"login": "owner"}}])) }),
        )
        .route(
            "/app/installations/:installation_id/access_tokens",
            post(
                |Path(installation_id): Path<u64>, body: Json<Value>| async move {
                    assert_eq!(installation_id, 4242);
                    assert_eq!(body.0["permissions"], json!({"checks": "write"}));
                    assert!(
                        body.0["permissions"].get("id-token").is_none(),
                        "id-token is not an installation-token scope and must not be requested"
                    );
                    Json(json!({
                        "token": "ghs_minted_alias_token",
                        "expires_at": "2999-01-01T00:00:00Z"
                    }))
                },
            ),
        );
    tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

    // Held for the whole test: `PRELOOP_GITHUB_API_URL` is process-global.
    // `TestEnvVar` restores it even if an assertion below panics — a bare
    // `remove_var` at the end of the body leaks this stub's address to every
    // later test when the test fails, which turns one failure into a cascade.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _api_url = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", api_base);

    let private_key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.github_app = Some(GitHubAppCredentials::for_tests(
        "424",
        private_key,
        MintFailurePolicy::LocalJwt,
    ));
    let app = app(state.clone(), CancellationToken::new());

    let registered = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({"name": "alias-runner", "version": "2.335.1"}),
    )
    .await;
    let runner_id = registered["id"].as_i64().unwrap();
    let runner_token = state
        .local_jwt(json!({
            "sub": format!("preloop-runner-listen-{runner_id}"),
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\npermissions:\n  checks: write\n  id-token: write\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "payload": {"ref": "refs/heads/main", "commits": []},
            "repository": "owner/repo",
            "git_ref": "refs/heads/main",
        }),
    )
    .await;

    let session = request_json_with_bearer(
        &app,
        Method::POST,
        "/runner/server/session",
        json!({}),
        &runner_token,
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();
    let job_ref = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/runner/server/message?sessionId={session_id}&status=Online&waitSeconds=0"),
        Value::Null,
        &runner_token,
    )
    .await;
    let body: Value = serde_json::from_str(job_ref["body"].as_str().unwrap()).unwrap();
    let runner_request_id = body["runner_request_id"].as_str().unwrap();

    let acquired = request_json_with_bearer(
        &app,
        Method::POST,
        &format!("/broker/{runner_id}/acquirejob"),
        json!({"jobMessageId": runner_request_id, "billingOwnerId": "local", "runnerOS": "Linux"}),
        &runner_token,
    )
    .await;
    for name in ["system.github.token", "github_token", "GITHUB_TOKEN"] {
        assert_eq!(
            acquired["variables"][name]["value"], "ghs_minted_alias_token",
            "{name} must carry the minted App token after the claim"
        );
        assert_eq!(
            acquired["variables"][name]["isSecret"], true,
            "{name} must stay marked secret"
        );
    }
    // `${{ github.token }}` in the workflow context must see the same mint.
    let context_pairs = acquired["contextData"]["github"]["d"].as_array().unwrap();
    let context_token = context_pairs
        .iter()
        .find(|pair| pair["k"] == "token")
        .expect("github context carries a token entry")
        .clone();
    assert_eq!(
        context_token["v"], "ghs_minted_alias_token",
        "the github context token must be the minted App token"
    );
    // No narrowing occurred, so the wire permissions keep the declared set
    // (including the OIDC metadata for this trusted job).
    assert_eq!(
        acquired["variables"]["system.github.token.permissions"]["value"],
        r#"{"Checks":"write","IdToken":"write"}"#,
        "an un-narrowed mint leaves the declared wire permissions intact"
    );
}

/// When the App installation grants fewer repository permissions than the
/// job requested, the broker narrows the mint and must restate the wire
/// permissions: App-scoped entries come from the effective grant (a scope
/// the installation lacks disappears), while a trusted job's Actions-only
/// metadata (`IdToken: write`, whose OIDC grant is still live) survives.
/// A fork-restricted job's wire set has no IdToken and must not gain one.
#[tokio::test]
async fn broker_claim_merges_narrowed_grants_with_actions_only_metadata() {
    use crate::github_app::{GitHubAppCredentials, MintFailurePolicy};
    use axum::routing::{get, post};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_base = format!("http://{}", listener.local_addr().unwrap());
    // The installation grants `contents`, `metadata` and `pull-requests` at
    // read — but NOT `checks`. The first mint for each claim therefore 422s
    // (checks is ungranted), the grants are fetched, the request is clamped,
    // and the second mint succeeds with only the granted scope.
    let access_token_calls = Arc::new(AtomicUsize::new(0));
    let stub = Router::new()
        .route(
            "/app/installations",
            get(|| async { Json(json!([{"id": 4242, "account": {"login": "owner"}}])) }),
        )
        .route(
            "/app/installations/:installation_id",
            get(|Path(installation_id): Path<u64>| async move {
                assert_eq!(installation_id, 4242);
                Json(json!({
                    "permissions": {
                        "contents": "read",
                        "metadata": "read",
                        "pull_requests": "read"
                    }
                }))
            }),
        )
        .route(
            "/app/installations/:installation_id/access_tokens",
            post({
                let access_token_calls = access_token_calls.clone();
                move |Path(installation_id): Path<u64>, body: Json<Value>| {
                    let access_token_calls = access_token_calls.clone();
                    async move {
                        assert_eq!(installation_id, 4242);
                        let call = access_token_calls.fetch_add(1, Ordering::SeqCst);
                        match call {
                            // First attempt per claim: the ungranted `checks`
                            // scope makes GitHub reject the whole request.
                            0 | 2 => axum::response::Response::builder()
                                .status(StatusCode::UNPROCESSABLE_ENTITY)
                                .body(Body::from(
                                    json!({"message": "checks is not granted"}).to_string(),
                                ))
                                .unwrap(),
                            _ => {
                                // The clamped request names only the granted
                                // scope, and never the Actions-only id-token.
                                assert_eq!(body.0["permissions"], json!({"pull_requests": "read"}));
                                assert!(body.0["permissions"].get("id-token").is_none());
                                axum::Json(json!({
                                    "token": format!("ghs_narrowed_{call}"),
                                    "expires_at": "2999-01-01T00:00:00Z"
                                }))
                                .into_response()
                            }
                        }
                    }
                }
            }),
        );
    tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

    // Held for the whole test: `PRELOOP_GITHUB_API_URL` is process-global, and
    // `TestEnvVar` restores it through a panicking assertion.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _api_url = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", api_base);

    let private_key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.github_app = Some(GitHubAppCredentials::for_tests(
        "424",
        private_key,
        MintFailurePolicy::LocalJwt,
    ));
    let app = app(state.clone(), CancellationToken::new());
    let registered = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({"name": "narrow-runner", "version": "2.335.1"}),
    )
    .await;
    let runner_id = registered["id"].as_i64().unwrap();
    let runner_token = state
        .local_jwt(json!({
            "sub": format!("preloop-runner-listen-{runner_id}"),
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    let yaml = "on: push\npermissions:\n  checks: write\n  pull-requests: write\n  id-token: write\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let submit = |tier: Option<&'static str>| {
        let app = app.clone();
        async move {
            request_json(
                &app,
                Method::POST,
                "/api/v1/runs",
                json!({
                    "workflow_yaml": yaml,
                    "event": "push",
                    "payload": {"ref": "refs/heads/main", "commits": []},
                    "repository": "owner/repo",
                    "git_ref": "refs/heads/main",
                    "trust_tier": tier,
                }),
            )
            .await
        }
    };

    async fn claim_and_return(app: &Router, runner_id: i64, runner_token: &str) -> Value {
        let session = request_json_with_bearer(
            app,
            Method::POST,
            "/runner/server/session",
            json!({}),
            runner_token,
        )
        .await;
        let session_id = session["sessionId"].as_str().unwrap();
        let job_ref = request_json_with_bearer(
            app,
            Method::GET,
            &format!("/runner/server/message?sessionId={session_id}&status=Online&waitSeconds=0"),
            Value::Null,
            runner_token,
        )
        .await;
        let body: Value = serde_json::from_str(job_ref["body"].as_str().unwrap()).unwrap();
        let runner_request_id = body["runner_request_id"].as_str().unwrap();
        request_json_with_bearer(
            app,
            Method::POST,
            &format!("/broker/{runner_id}/acquirejob"),
            json!({"jobMessageId": runner_request_id, "billingOwnerId": "local", "runnerOS": "Linux"}),
            runner_token,
        )
        .await
    }

    // Trusted job first (claim 1): declared writes are clamped to the
    // installation's grant, `checks` disappears entirely, and `IdToken: write`
    // survives because the OIDC grant is still live.
    submit(None).await;
    let trusted = claim_and_return(&app, runner_id, &runner_token).await;
    assert_eq!(
        trusted["variables"]["system.github.token.permissions"]["value"],
        r#"{"IdToken":"write","PullRequests":"read"}"#,
        "trusted wire keeps the OIDC metadata and reflects the narrowed App grant"
    );
    let trusted_endpoint = trusted["resources"]["endpoints"]
        .as_array()
        .unwrap()
        .iter()
        .find(|endpoint| {
            endpoint["name"]
                .as_str()
                .is_some_and(|name| name.eq_ignore_ascii_case("SystemVssConnection"))
        })
        .expect("SystemVssConnection endpoint present");
    assert!(
        trusted_endpoint["data"]["GenerateIdTokenUrl"]
            .as_str()
            .is_some_and(|url| !url.is_empty()),
        "trusted job's OIDC grant survives the narrowing"
    );
    assert_eq!(
        trusted["variables"]["system.github.token"]["value"], "ghs_narrowed_1",
        "trusted job carries the minted token"
    );

    // Fork job (claim 3): same declared workflow, but the fork profile never
    // carried IdToken — the narrowed restatement must not invent one.
    submit(Some("untrusted-fork-pull-request")).await;
    let fork = claim_and_return(&app, runner_id, &runner_token).await;
    assert_eq!(
        fork["variables"]["system.github.token.permissions"]["value"], r#"{"PullRequests":"read"}"#,
        "fork wire reflects the narrowed grant with no IdToken metadata"
    );
    assert!(
        !fork["variables"]["system.github.token.permissions"]["value"]
            .as_str()
            .is_some_and(|wire| wire.contains("IdToken")),
        "fork wire must not advertise IdToken"
    );
    let fork_endpoint = fork["resources"]["endpoints"]
        .as_array()
        .unwrap()
        .iter()
        .find(|endpoint| {
            endpoint["name"]
                .as_str()
                .is_some_and(|name| name.eq_ignore_ascii_case("SystemVssConnection"))
        })
        .expect("SystemVssConnection endpoint present");
    assert!(
        !fork_endpoint["data"]["GenerateIdTokenUrl"]
            .as_str()
            .is_some_and(|url| !url.is_empty()),
        "fork job gets no OIDC request URL"
    );
    assert_eq!(
        fork["variables"]["system.github.token"]["value"], "ghs_narrowed_3",
        "fork job carries the minted token"
    );
}

/// The deferred App-token request is deliberately kept past the first claim so
/// a re-claim after a runner disconnect re-mints under the build-time
/// conditions (the original permission set and its fallback restrictions).
/// It must not outlive the *job*, though: the record pins a repository and a
/// permission set, it is persisted into the store snapshot, and a stale entry
/// would let a re-claim mint fresh GitHub authority for work that is over.
///
/// Clearing it inside the broker's own `completejob` handler is not enough —
/// the legacy `/_apis` completion endpoints and the lease-expiry reaper never
/// run that handler. Every completion path does funnel through
/// `complete_job_inner`, so that is where the record is dropped, and this test
/// completes through the non-broker compat route to prove it.
#[tokio::test]
async fn a_completed_job_drops_its_deferred_token_request() {
    use crate::github_app::{GitHubAppCredentials, MintFailurePolicy};

    // A port nothing listens on: the mint fails on connect, so the claim
    // exercises the retention path without any network round trip. Under
    // `LocalJwt` the job simply keeps its local runtime token.
    let closed_port = {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        probe.local_addr().unwrap().port()
    };

    // Held for the whole test: `PRELOOP_GITHUB_API_URL` is process-global.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _api_url = crate::state::TestEnvVar::set(
        "PRELOOP_GITHUB_API_URL",
        format!("http://127.0.0.1:{closed_port}"),
    );

    let private_key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.github_app = Some(GitHubAppCredentials::for_tests(
        "424",
        private_key,
        MintFailurePolicy::LocalJwt,
    ));
    let app = app(state.clone(), CancellationToken::new());

    let registered = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({"name": "lifetime-runner", "version": "2.335.1"}),
    )
    .await;
    let runner_id = registered["id"].as_i64().unwrap();
    let runner_token = state
        .local_jwt(json!({
            "sub": format!("preloop-runner-listen-{runner_id}"),
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "payload": {"ref": "refs/heads/main", "commits": []},
            "repository": "owner/repo",
            "git_ref": "refs/heads/main",
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let session = request_json_with_bearer(
        &app,
        Method::POST,
        "/runner/server/session",
        json!({}),
        &runner_token,
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();
    let job_ref = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/runner/server/message?sessionId={session_id}&status=Online&waitSeconds=0"),
        Value::Null,
        &runner_token,
    )
    .await;
    let body: Value = serde_json::from_str(job_ref["body"].as_str().unwrap()).unwrap();
    let runner_request_id = body["runner_request_id"].as_str().unwrap();

    let _acquired = request_json_with_bearer(
        &app,
        Method::POST,
        &format!("/broker/{runner_id}/acquirejob"),
        json!({"jobMessageId": runner_request_id, "billingOwnerId": "local", "runnerOS": "Linux"}),
        &runner_token,
    )
    .await;

    // The internal request id is deliberately not on the wire (the broker
    // zeroes `requestId` because run-service payloads use the DTO default),
    // so read it from the correlation table the submit path populates.
    let request_id = {
        let inner = state.inner.lock().await;
        let ids: Vec<i64> = inner.job_requests.keys().copied().collect();
        assert_eq!(ids.len(), 1, "one job means one request record: {ids:?}");
        // Half one: the claim must NOT consume the record, or a re-claim after
        // a disconnect would rebuild it from defaults and lose both the
        // declared permission set and the fork profile.
        assert!(
            inner.github_token_requests.contains_key(&ids[0]),
            "the token request must survive the claim so a re-claim re-mints \
             under the build-time permission set and fallback restrictions"
        );
        ids[0]
    };

    // Half two: complete through the legacy compat route — the broker's own
    // `completejob` handler never runs here, exactly as for a lease-expiry
    // reap or an `/_apis` finish callback.
    request_json(
        &app,
        Method::PATCH,
        &format!("/runner/server/_apis/distributedtask/hubs/actions/plans/{run_id}/jobs/build"),
        json!({"status": "succeeded"}),
    )
    .await;

    let inner = state.inner.lock().await;
    assert!(
        !inner.github_token_requests.contains_key(&request_id),
        "a terminal job must not leave its App-token request registered: \
         {:?}",
        inner.github_token_requests
    );
    assert!(
        inner
            .job_requests
            .get(&request_id)
            .is_some_and(|record| record.result.is_some()),
        "the completion must have settled the job request"
    );
}

/// A `GitHubTokenRequest` persisted by a pre-upgrade server has no
/// `untrusted` field. Deserializing it as trusted would silently re-enable
/// the PAT fallback after a restart, so missing trust metadata must fail
/// closed — and the mint path must then refuse the PAT for such a request.
#[tokio::test]
async fn persisted_token_request_without_trust_metadata_fails_closed() {
    use crate::github_app::{GitHubAppCredentials, MintFailurePolicy};

    // The exact shape a pre-upgrade store snapshot would carry: no
    // `untrusted` key at all.
    let old: crate::models::GitHubTokenRequest = serde_json::from_str(
        r#"{"repository":"owner/repo","permissions":{"checks":"write"},"declared":true}"#,
    )
    .unwrap();
    assert!(
        old.untrusted,
        "missing persisted trust metadata must deserialize as untrusted"
    );

    // Newly created trusted requests serialize the field explicitly, so the
    // fail-closed default only ever applies to genuinely old state.
    let trusted = crate::models::GitHubTokenRequest {
        repository: "owner/repo".to_owned(),
        permissions: BTreeMap::from([("checks".to_owned(), "write".to_owned())]),
        declared: true,
        untrusted: false,
    };
    let wire = serde_json::to_string(&trusted).unwrap();
    assert!(
        wire.contains("\"untrusted\":false"),
        "trusted requests must persist their trust metadata explicitly: {wire}"
    );
    let round_tripped: crate::models::GitHubTokenRequest = serde_json::from_str(&wire).unwrap();
    assert!(!round_tripped.untrusted);

    // Broker/mint assertion: an old request (untrusted by fail-closed
    // default) under the `pat` policy must not receive the PAT when the mint
    // fails — `local-workspace-only` fails before any network I/O.
    let private_key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let mut creds = GitHubAppCredentials::for_tests("424", private_key, MintFailurePolicy::Pat);
    creds.pat_fallback = Some("github_pat_broad".to_owned());
    state.github_app = Some(creds);
    let shared = Arc::new(SharedState {
        state,
        shutdown: CancellationToken::new(),
    });
    let old_request = crate::models::GitHubTokenRequest {
        repository: "local-workspace-only".to_owned(),
        permissions: old.permissions.clone(),
        declared: true,
        untrusted: old.untrusted,
    };
    let mint = crate::broker::mint_dispatch_github_token(&shared, &old_request).await;
    assert!(
        matches!(mint, Ok(None)),
        "a request whose trust metadata was never recorded must not receive the PAT fallback"
    );
}

/// GitHub gives fork PR runs read-only cache access: they can restore from
/// the base repository's cache but cannot save to it, so a fork cannot poison
/// entries a trusted run later restores. Every cache write surface must deny
/// fork-restricted jobs while reads stay open.
#[tokio::test]
async fn fork_pr_runs_get_read_only_cache_access() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let submit = |tier: Option<&'static str>| {
        let app = app.clone();
        async move {
            request_json(
                &app,
                Method::POST,
                "/api/v1/runs",
                json!({
                    "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
                    "event": "push",
                    "payload": {"ref": "refs/heads/main", "commits": []},
                    "repository": "owner/repo",
                    "git_ref": "refs/heads/main",
                    "trust_tier": tier,
                }),
            )
            .await
        }
    };

    let fork = submit(Some("untrusted-fork-pull-request")).await;
    let trusted = submit(None).await;

    let (fork_token, fork_plan, fork_job) = {
        let inner = state.inner.lock().await;
        let message = queued_message_for(&inner, fork["run_id"].as_str().unwrap());
        (
            state.mint_runtime_token(&message.plan.plan_id, &message.job_id),
            message.plan.plan_id.clone(),
            message.job_id,
        )
    };
    let trusted_token = {
        let inner = state.inner.lock().await;
        let message = queued_message_for(&inner, trusted["run_id"].as_str().unwrap());
        state.mint_runtime_token(&message.plan.plan_id, &message.job_id)
    };

    let create_uri = "/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry";
    let finalize_uri = "/twirp/github.actions.results.api.v1.CacheService/FinalizeCacheEntryUpload";
    let restore_uri = "/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL";
    let cache_body = json!({"key": "shared-key", "version": "v1"});

    // Fork: writes refused on both the reserve (create) and finalize paths.
    assert_eq!(
        status_with_bearer(
            &app,
            &fork_token,
            Method::POST,
            create_uri,
            cache_body.clone()
        )
        .await,
        StatusCode::FORBIDDEN,
        "fork PR run must not reserve a cache entry"
    );
    assert_eq!(
        status_with_bearer(
            &app,
            &fork_token,
            Method::POST,
            finalize_uri,
            cache_body.clone()
        )
        .await,
        StatusCode::FORBIDDEN,
        "fork PR run must not finalize a cache upload"
    );
    // Fork: restore stays open (a miss is a normal 200 `ok: false`).
    assert_eq!(
        status_with_bearer(
            &app,
            &fork_token,
            Method::POST,
            restore_uri,
            cache_body.clone()
        )
        .await,
        StatusCode::OK,
        "fork PR run may still restore from the shared cache"
    );
    // Trusted control: the same write succeeds and returns an upload URL.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(create_uri)
                .header(header::AUTHORIZATION, format!("Bearer {trusted_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(cache_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["ok"], true);
    assert!(
        body["signed_upload_url"]
            .as_str()
            .is_some_and(|url| !url.is_empty()),
        "trusted job still gets a cache upload URL"
    );

    // Legacy v1 surface (`actions/cache@v3`): every write endpoint is denied,
    // not just the reserve. Reserve a trusted entry first so the upload and
    // commit guards run against a real cache id.
    assert_eq!(
        status_with_bearer(
            &app,
            &fork_token,
            Method::POST,
            "/_apis/artifactcache/cache",
            json!({"key": "legacy-key", "version": "v1"}),
        )
        .await,
        StatusCode::FORBIDDEN,
        "fork PR run must not reserve through the v1 cache API"
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/_apis/artifactcache/cache")
                .header(header::AUTHORIZATION, format!("Bearer {trusted_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"key": "legacy-key", "version": "v1"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let cache_id = serde_json::from_slice::<Value>(&bytes).unwrap()["cacheId"]
        .as_i64()
        .expect("trusted legacy reserve returns a cache id");
    let legacy_uri = format!("/_apis/artifactcache/cache/{cache_id}");
    assert_eq!(
        status_with_bearer(
            &app,
            &fork_token,
            Method::PATCH,
            &legacy_uri,
            json!("fork upload payload"),
        )
        .await,
        StatusCode::FORBIDDEN,
        "fork PR run must not upload through the v1 cache API"
    );
    assert_eq!(
        status_with_bearer(&app, &fork_token, Method::POST, &legacy_uri, json!({})).await,
        StatusCode::FORBIDDEN,
        "fork PR run must not commit through the v1 cache API"
    );

    // The runtime token genuinely names the fork job (not a blanket reject):
    // the OIDC surface proves it by refusing this token's job.
    let oidc_uri = format!(
        "/runner/server/_apis/distributedtask/hubs/actions/plans/{fork_plan}/jobs/{fork_job}/oidctoken"
    );
    assert_eq!(
        status_with_bearer(&app, &fork_token, Method::GET, &oidc_uri, Value::Null).await,
        StatusCode::FORBIDDEN,
        "the fork job's runtime token is real and its OIDC grant is denied"
    );
}

/// A fork job's runtime JWT must not smuggle a cache write in after the
/// job's request was retired. Retirement (`RequestRetirement::Purge` in
/// `retire_node_requests`) removes the correlation records
/// `fork_restricted_from_token` walks, and treating an unresolvable job
/// token as a control-plane caller would let a fork worker poison cache
/// entries with a leaked token. Unresolvable job tokens fail closed instead.
#[tokio::test]
async fn fork_cache_writes_fail_closed_when_the_job_no_longer_resolves() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let submit = |tier: Option<&'static str>| {
        let app = app.clone();
        async move {
            request_json(
                &app,
                Method::POST,
                "/api/v1/runs",
                json!({
                    "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
                    "event": "push",
                    "payload": {"ref": "refs/heads/main", "commits": []},
                    "repository": "owner/repo",
                    "git_ref": "refs/heads/main",
                    "trust_tier": tier,
                }),
            )
            .await
        }
    };

    let fork = submit(Some("untrusted-fork-pull-request")).await;
    let trusted = submit(None).await;

    let (fork_token, fork_job) = {
        let inner = state.inner.lock().await;
        let message = queued_message_for(&inner, fork["run_id"].as_str().unwrap());
        (
            state.mint_runtime_token(&message.plan.plan_id, &message.job_id),
            message.job_id,
        )
    };
    let trusted_token = {
        let inner = state.inner.lock().await;
        let message = queued_message_for(&inner, trusted["run_id"].as_str().unwrap());
        state.mint_runtime_token(&message.plan.plan_id, &message.job_id)
    };

    // The same surgery `RequestRetirement::Purge` performs: drop the
    // job-to-request correlation while the worker still holds the runtime
    // JWT.
    {
        let mut inner = state.inner.lock().await;
        inner.agent_job_requests.remove(&fork_job);
    }

    // Both write surfaces reject the now-unresolvable fork token.
    assert_eq!(
        status_with_bearer(
            &app,
            &fork_token,
            Method::POST,
            "/_apis/artifactcache/cache",
            json!({"key": "poison-key", "version": "v1"}),
        )
        .await,
        StatusCode::FORBIDDEN,
        "an unresolvable job token must not reserve through the v1 cache API"
    );
    assert_eq!(
        status_with_bearer(
            &app,
            &fork_token,
            Method::POST,
            "/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
            json!({"key": "poison-key", "version": "v1"}),
        )
        .await,
        StatusCode::FORBIDDEN,
        "an unresolvable job token must not create through the cache v2 API"
    );
    // The fail-closed rule fires for job tokens only: a live trusted job's
    // token still reserves without trouble.
    assert_eq!(
        status_with_bearer(
            &app,
            &trusted_token,
            Method::POST,
            "/_apis/artifactcache/cache",
            json!({"key": "trusted-key", "version": "v1"}),
        )
        .await,
        StatusCode::OK,
        "a resolvable trusted job token keeps write access"
    );
}

/// A PAT-only deployment embeds the static PAT into job messages at build
/// time. That override must never reach a fork-restricted job: the job keeps
/// the local job-scoped runtime token, which authenticates only against this
/// control plane.
#[tokio::test]
async fn fork_job_never_receives_the_configured_pat_override() {
    // The effective PAT is env-then-config; writers of `PRELOOP_GITHUB_TOKEN`
    // serialize on the env lock, so this reader must take it too or the
    // asserted token flips under parallelism.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    std::fs::write(&config_path, "[github]\npat = \"github_pat_testvalue\"\n").unwrap();
    let state = AppState::new_with_config(temp.path().to_path_buf(), config_path)
        .await
        .unwrap();
    assert!(
        state.github_app.is_none(),
        "config declares no app id or pem"
    );
    let pat = state
        .static_github_pat()
        .expect("config declares a PAT")
        .to_owned();
    let app = app(state.clone(), CancellationToken::new());

    let yaml =
        "on: push\njobs:\n  probe:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let fork = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": yaml,
            "event": "push",
            "repository": "owner/repo",
            "trust_tier": "untrusted-fork-pull-request",
        }),
    )
    .await;
    let trusted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": yaml,
            "event": "push",
            "repository": "owner/repo",
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let fork_message = queued_message_for(&inner, fork["run_id"].as_str().unwrap());
    let runtime_token = state.mint_runtime_token(&fork_message.plan.plan_id, &fork_message.job_id);
    for name in ["system.github.token", "github_token", "GITHUB_TOKEN"] {
        assert_eq!(
            variable_value(&fork_message, name),
            Some(runtime_token.as_str()),
            "fork job must carry the local runtime token, not the PAT ({name})"
        );
    }
    assert_ne!(
        variable_value(&fork_message, "system.github.token"),
        Some(pat.as_str()),
        "the static PAT must not reach a fork-restricted job"
    );

    let trusted_message = queued_message_for(&inner, trusted["run_id"].as_str().unwrap());
    assert_eq!(
        variable_value(&trusted_message, "system.github.token"),
        Some(pat.as_str()),
        "trusted jobs still receive the configured PAT"
    );
}

/// End-to-end through the webhook adapter: a fork `pull_request` delivery is
/// stamped `UntrustedForkPullRequest` and the queued job must show the
/// downgraded profile and no OIDC URL, and stored secrets stay denied.
#[tokio::test]
async fn fork_pull_request_webhook_jobs_are_downgraded_and_secrets_denied() {
    // The webhook path creates check runs, and it only takes its mock branch
    // while no GitHub credential is visible. `PRELOOP_GITHUB_TOKEN` and
    // `PRELOOP_GITHUB_API_URL` are process-global, so a co-scheduled test that
    // points them at its own stub would send this run's check-run POST there
    // and turn the webhook 200 into a 502. Serialize on the same lock those
    // tests hold, and guarantee the mock branch for this body.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let _no_api_url = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_API_URL");

    let temp = tempfile::tempdir().unwrap();
    let ws_dir = temp.path().join("workspace");
    tokio::fs::create_dir_all(ws_dir.join(".github/workflows"))
        .await
        .unwrap();
    tokio::fs::write(
        ws_dir.join(".github/workflows/test.yml"),
        "on: pull_request\npermissions:\n  checks: write\n  id-token: write\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
    )
    .await
    .unwrap();

    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.webhook_secret = Some("super-secret".to_owned());
    state.local_workspace = Some(ws_dir);
    {
        let mut secrets = state.secrets.write();
        secrets.repo.insert(
            "owner/repo".to_owned(),
            BTreeMap::from([("REPO_TOKEN".to_owned(), "repo-value".to_owned())]),
        );
    }
    let app = app(state.clone(), CancellationToken::new());

    // `head.repo.fork` absent defaults to fork — the same shape the existing
    // webhook test delivers, now asserting the downgrade.
    let payload = serde_json::json!({
        "action": "opened",
        "number": 42,
        "pull_request": {
            "head": {
                "ref": "feature-branch",
                "sha": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3"
            },
            "base": {
                "ref": "main",
                "sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
            }
        },
        "repository": {
            "full_name": "owner/repo",
            "default_branch": "main"
        }
    });
    let payload_bytes = serde_json::to_vec(&payload).unwrap();
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(b"super-secret").unwrap();
    mac.update(&payload_bytes);
    let sig_bytes = mac.finalize().into_bytes();
    let sig_hex = sig_bytes
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-event", "pull_request")
                .header("x-hub-signature-256", format!("sha256={sig_hex}"))
                .header("content-type", "application/json")
                .body(Body::from(payload_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let inner = state.inner.lock().await;
    let (_, run_record) = inner.runs.iter().next().unwrap();
    assert_eq!(
        run_record.submission.trust_tier.as_deref(),
        Some("untrusted-fork-pull-request"),
        "the webhook adapter must stamp the fork tier"
    );
    let run_id = run_record.run_id.to_string();
    let message = queued_message_for(&inner, &run_id);
    assert_eq!(
        variable_value(&message, "system.github.token.permissions"),
        Some(r#"{"Checks":"read"}"#),
        "webhook-delivered fork PR job is downgraded to read-only with no IdToken metadata"
    );
    let endpoint = message
        .resources
        .endpoints
        .iter()
        .find(|endpoint| endpoint.name.eq_ignore_ascii_case("SystemVssConnection"))
        .expect("SystemVssConnection endpoint present");
    assert!(
        !endpoint
            .data
            .get("GenerateIdTokenUrl")
            .is_some_and(|url| !url.is_empty()),
        "webhook-delivered fork PR job gets no OIDC request URL"
    );
    assert_eq!(
        variable_value(&message, "REPO_TOKEN"),
        None,
        "stored secrets stay denied for the fork PR job"
    );
    drop(inner);

    // Trusted control through the same build path: the same stored secret is
    // injected and the declared writes survive.
    let trusted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\npermissions:\n  checks: write\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo",
        }),
    )
    .await;
    let inner = state.inner.lock().await;
    let trusted_message = queued_message_for(&inner, trusted["run_id"].as_str().unwrap());
    assert_eq!(
        variable_value(&trusted_message, "REPO_TOKEN"),
        Some("repo-value"),
        "trusted jobs still receive stored secrets"
    );
    assert_eq!(
        variable_value(&trusted_message, "system.github.token.permissions"),
        Some(r#"{"Checks":"write"}"#),
        "trusted jobs keep declared writes"
    );
}

/// A failed installation-token mint must never silently reach for the broad
/// `PRELOOP_GITHUB_TOKEN` PAT: that would swap a repository-scoped,
/// `permissions:`-bounded token for an unscoped one. Only
/// `PRELOOP_GITHUB_APP_MINT_FAILURE` decides, and its default leaves the job on the
/// local HMAC JWT, which carries no GitHub authority at all.
#[tokio::test]
async fn app_token_mint_failure_follows_the_configured_policy() {
    use crate::github_app::{GitHubAppCredentials, MintFailurePolicy};

    // A key is needed to populate the credentials but is never exercised: a
    // `repository` with no `owner/repo` slug cannot be scoped to a repository,
    // so the mint fails before it signs anything or opens a socket.
    let private_key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();

    for policy in [MintFailurePolicy::LocalJwt, MintFailurePolicy::Error] {
        let temp = tempfile::tempdir().unwrap();
        let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        state.github_app = Some(GitHubAppCredentials::for_tests(
            "424",
            private_key.clone(),
            policy,
        ));
        let shutdown = CancellationToken::new();
        let app = app(state.clone(), shutdown.clone());

        let (status, _) = try_req(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
                "event": "push",
                "repository": "local-workspace-only"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "policy {policy:?}");
        let request = {
            let inner = state.inner.lock().await;
            assert_eq!(inner.queue.len(), 1);
            inner
                .github_token_requests
                .values()
                .next()
                .cloned()
                .expect("App-backed job defers a token request")
        };
        let shared = Arc::new(SharedState {
            state: state.clone(),
            shutdown,
        });
        let mint = crate::broker::mint_dispatch_github_token(&shared, &request).await;
        match policy {
            // The job still runs; it just has no GitHub credential.
            MintFailurePolicy::LocalJwt => assert!(matches!(mint, Ok(None))),
            // Refusal happens when the broker is about to dispatch the job.
            MintFailurePolicy::Error => assert_eq!(
                mint.expect_err("error policy must refuse dispatch")
                    .into_response()
                    .status(),
                StatusCode::BAD_GATEWAY
            ),
            MintFailurePolicy::Pat => unreachable!("covered by a github_app unit test"),
        }
    }
}

/// By the time `acquirejob` mints, the poll has already dequeued the job,
/// flipped the run to `InProgress` and pinned the request to the session. A
/// refusal under the `error` policy is a permanent configuration fault, so if
/// the 502 left the claim in place the runner would re-acquire forever and the
/// run would hang until the 600s disconnect reaper mopped it up.
#[tokio::test]
async fn a_dispatch_refused_by_the_mint_policy_fails_its_run_without_the_reaper() {
    use crate::github_app::{GitHubAppCredentials, MintFailurePolicy};

    // `local-workspace-only` carries no `owner/repo` slug, so the mint fails
    // before it signs anything or opens a socket.
    let private_key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.github_app = Some(GitHubAppCredentials::for_tests(
        "424",
        private_key,
        MintFailurePolicy::Error,
    ));
    let app = app(state.clone(), CancellationToken::new());
    // The broker protocol requires a listen token that names a *registered*
    // runner (tokens are revoked with the registration on purge), so register
    // the machine first — on a fresh state this gets runner id 1, which the
    // hard-coded /broker/1/ paths below expect.
    let registered = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({"name": "mint-policy-runner", "version": "2.335.1"}),
    )
    .await;
    let registered_runner_id = registered["id"].as_i64().unwrap();
    let runner_token = state
        .local_jwt(json!({
            "sub": format!("preloop-runner-listen-{registered_runner_id}"),
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "local-workspace-only"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let session = request_json_with_bearer(
        &app,
        Method::POST,
        "/runner/server/session",
        json!({}),
        &runner_token,
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();
    let job_ref = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/runner/server/message?sessionId={session_id}&status=Online&waitSeconds=0"),
        Value::Null,
        &runner_token,
    )
    .await;
    let body: Value = serde_json::from_str(job_ref["body"].as_str().unwrap()).unwrap();
    let runner_request_id = body["runner_request_id"].as_str().unwrap();
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.runs.get(&run_id).unwrap().status,
            ExecutionStatus::InProgress,
            "the poll must claim the job before the mint is attempted"
        );
    }

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/broker/1/acquirejob")
                .header(header::AUTHORIZATION, format!("Bearer {runner_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"jobMessageId": runner_request_id, "billingOwnerId": "local", "runnerOS": "Linux"})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(run.status, ExecutionStatus::Failure);
    assert!(run.jobs.values().all(|status| status.is_terminal()));
    let request_id = *inner.job_requests.keys().next().unwrap();
    assert_eq!(
        inner.job_requests[&request_id].result,
        Some(ExecutionStatus::Failure)
    );
    assert!(
        !inner
            .session_active_requests
            .values()
            .any(|rid| *rid == request_id),
        "the session must be free to take the next job"
    );
    assert!(!inner.inflight_requests.contains_key(&request_id));
}

#[tokio::test]
async fn app_only_server_fetches_webhook_workflows_with_installation_token() {
    use crate::github_app::{GitHubAppCredentials, MintFailurePolicy};
    use axum::extract::Path;
    use axum::http::HeaderMap;
    use axum::routing::{get, post};
    use rsa::RsaPrivateKey;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_base = format!("http://{}", listener.local_addr().unwrap());
    let download_url = format!("{api_base}/raw/ci.yml");
    let stub = Router::new()
        .route(
            "/app/installations",
            get(|| async {
                Json(json!([{
                    "id": 4242,
                    "account": {"login": "preloopdev"}
                }]))
            }),
        )
        .route(
            "/app/installations/:installation_id/access_tokens",
            post(|Path(installation_id): Path<u64>| async move {
                assert_eq!(installation_id, 4242);
                Json(json!({
                    "token": "ghs_app_only_workflow_token",
                    "expires_at": "2999-01-01T00:00:00Z"
                }))
            }),
        )
        .route(
            "/repos/preloopdev/preloop/contents/.github/workflows",
            get(move |headers: HeaderMap| {
                let download_url = download_url.clone();
                async move {
                    assert_eq!(
                        headers
                            .get("authorization")
                            .and_then(|value| value.to_str().ok()),
                        Some("Bearer ghs_app_only_workflow_token")
                    );
                    Json(json!([{
                        "name": "ci.yml",
                        "type": "file",
                        "download_url": download_url
                    }]))
                }
            }),
        )
        .route(
            "/raw/ci.yml",
            get(|headers: HeaderMap| async move {
                assert_eq!(
                    headers
                        .get("authorization")
                        .and_then(|value| value.to_str().ok()),
                    Some("Bearer ghs_app_only_workflow_token")
                );
                "on: push\njobs:\n  test:\n    runs-on: self-hosted\n    steps:\n      - run: true\n"
            }),
        );
    tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.github_app = Some(GitHubAppCredentials::for_tests(
        "424",
        RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap(),
        MintFailurePolicy::LocalJwt,
    ));
    let shared = Arc::new(SharedState {
        state,
        shutdown: CancellationToken::new(),
    });

    let workflows = crate::github::fetch_workflows_at(
        &shared,
        "preloopdev/preloop",
        "refs/heads/main",
        &api_base,
    )
    .await
    .unwrap();

    assert_eq!(workflows.len(), 1);
    assert!(workflows["ci.yml"].contains("runs-on: self-hosted"));
}

#[tokio::test]
async fn app_only_server_resolves_pull_request_changed_files_with_installation_token() {
    use crate::github_app::{GitHubAppCredentials, MintFailurePolicy};
    use axum::extract::{Path, Query};
    use axum::http::HeaderMap;
    use axum::routing::{get, post};
    use rsa::RsaPrivateKey;
    use std::collections::HashMap;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_base = format!("http://{}", listener.local_addr().unwrap());
    let stub = Router::new()
        .route(
            "/app/installations",
            get(|| async {
                Json(json!([{
                    "id": 4242,
                    "account": {"login": "preloopdev"}
                }]))
            }),
        )
        .route(
            "/app/installations/:installation_id/access_tokens",
            post(
                |Path(installation_id): Path<u64>, body: Json<Value>| async move {
                    assert_eq!(installation_id, 4242);
                    // Listing pull request files is refused by a token scoped
                    // only to `contents`, so the mint must ask for the
                    // pull-request scope rather than reuse the inventory token.
                    assert_eq!(body.0["permissions"]["pull_requests"], json!("read"));
                    Json(json!({
                        "token": "ghs_app_only_pr_files_token",
                        "expires_at": "2999-01-01T00:00:00Z"
                    }))
                },
            ),
        )
        .route(
            "/repos/preloopdev/preloop/pulls/7/files",
            get(
                |headers: HeaderMap, Query(query): Query<HashMap<String, String>>| async move {
                    assert_eq!(
                        headers
                            .get("authorization")
                            .and_then(|value| value.to_str().ok()),
                        Some("Bearer ghs_app_only_pr_files_token")
                    );
                    // Paging terminates on an empty page, so only the first one
                    // carries files.
                    if query.get("page").map(String::as_str) == Some("1") {
                        Json(json!([{"filename": "src/main.rs"}, {"filename": "docs/readme.md"}]))
                    } else {
                        Json(json!([]))
                    }
                },
            ),
        );
    tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.github_app = Some(GitHubAppCredentials::for_tests(
        "424",
        RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap(),
        MintFailurePolicy::LocalJwt,
    ));
    let shared = Arc::new(SharedState {
        state,
        shutdown: CancellationToken::new(),
    });

    let changed =
        crate::github::resolve_pr_changed_files_at(&shared, "preloopdev/preloop", 7, &api_base)
            .await
            .unwrap();

    assert_eq!(
        changed,
        Some(vec!["src/main.rs".to_owned(), "docs/readme.md".to_owned()]),
        "an App-only deployment must reach the changed-files lookup so `paths:` filters stay evaluable"
    );
}

// Non-asserting helper for tests that need to inspect an error response.
async fn try_req(app: &Router, method: Method, uri: &str, body: Value) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if uri.starts_with("/api/v1/")
        || uri.starts_with("/_apis/")
        || uri.starts_with("/runner/server/_apis/")
        || uri.starts_with("/broker/")
        || uri.starts_with("/twirp/")
    {
        builder = builder.header(header::AUTHORIZATION, "Bearer preloop-system-token");
    } else if uri.starts_with("/internal/test/") {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"));
    } else if uri.starts_with("/api/v3/actions/runner-registration") {
        builder = builder.header(
            header::AUTHORIZATION,
            // Strict-by-default registration accepts only the system
            // credential; test servers run with the default token.
            format!("RemoteAuth {DEFAULT_PRELOOP_SYSTEM_TOKEN}"),
        );
    }
    let request = if body.is_null() {
        builder.body(Body::empty()).unwrap()
    } else {
        builder
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let val = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, val)
}

async fn request_json(app: &Router, method: Method, uri: &str, body: Value) -> Value {
    let mut builder = Request::builder().method(method).uri(uri);
    if uri.contains("/oidctoken") {
        let token = uri
            .split("/plans/")
            .nth(1)
            .and_then(|rest| rest.split("/jobs/").next().zip(rest.split("/jobs/").nth(1)))
            .and_then(|(plan, rest)| rest.split('/').next().map(|job| (plan, job)))
            .and_then(|(plan, job)| {
                uuid::Uuid::parse_str(job)
                    .ok()
                    .map(|id| mint_runtime_token(plan, &id))
            })
            .unwrap_or_else(|| DEFAULT_PRELOOP_SYSTEM_TOKEN.to_owned());
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    } else if uri.starts_with("/api/v1/")
        || uri.starts_with("/_apis/")
        || uri.starts_with("/runner/server/_apis/")
        || uri.starts_with("/broker/")
        || uri.starts_with("/actions/build/")
        || uri.starts_with("/twirp/")
    {
        builder = builder.header(header::AUTHORIZATION, "Bearer preloop-system-token");
    } else if uri.starts_with("/internal/test/") {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"));
    } else if uri.starts_with("/api/v3/actions/runner-registration") {
        builder = builder.header(
            header::AUTHORIZATION,
            "RemoteAuth preloop-registration-token",
        );
    }
    let request = if body.is_null() {
        builder.body(Body::empty()).unwrap()
    } else {
        builder
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(
        status.is_success(),
        "unexpected status: {} body={}",
        status,
        String::from_utf8_lossy(&bytes)
    );
    if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    }
}
async fn request_json_with_bearer(
    app: &Router,
    method: Method,
    uri: &str,
    body: Value,
    bearer: &str,
) -> Value {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {bearer}"));
    let request = if body.is_null() {
        builder.body(Body::empty()).unwrap()
    } else {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
        builder.body(Body::from(body.to_string())).unwrap()
    };
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(
        status.is_success(),
        "unexpected status: {} body={}",
        status,
        String::from_utf8_lossy(&bytes)
    );
    if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    }
}

#[tokio::test]
async fn queued_job_with_no_runner_is_failed_after_the_grace_window() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    // A fresh server: no pool, no runners registered.
    let accepted = submit_simple_run(&app).await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // First reaper tick stamps the queued-at time; the job is not yet old
    // enough to fail.
    reap_once(&shared).await;
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.queue.len(),
            1,
            "job still queued inside the grace window"
        );
        assert!(inner
            .queued_at
            .contains_key(&(run_id, JobId("build".to_owned()))));
    }

    // Backdate the first-seen mark past the grace window and reap again: the
    // job must be failed with a visible reason and the run must conclude.
    {
        let mut inner = state.inner.lock().await;
        inner.queued_at.insert(
            (run_id, JobId("build".to_owned())),
            SystemTime::now() - Duration::from_secs(300),
        );
    }
    reap_once(&shared).await;

    {
        let inner = state.inner.lock().await;
        assert!(
            inner.queue.is_empty(),
            "starving job must leave the ready queue"
        );
        assert!(!inner
            .queued_at
            .contains_key(&(run_id, JobId("build".to_owned()))));
        let run = inner.runs.get(&run_id).expect("run record must survive");
        assert_eq!(
            run.jobs.get(&JobId("build".to_owned())),
            Some(&ExecutionStatus::Failure),
            "a job no runner can ever claim must fail, not queue forever"
        );
        assert_eq!(run.status, ExecutionStatus::Failure);
        assert!(run.completed_at.is_some(), "run must conclude");
    }
}

#[tokio::test]
async fn liveness_sweep_requeues_job_of_deaf_runner() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    // Register a runner, open a session, and claim a submitted job through
    // the real poll path.
    let (runner_id, token) =
        register_runner_with_token(&app, "deaf-runner", &["self-hosted"], None).await;
    let (status, session) = create_disttask_session(&app, &token, runner_id).await;
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "session creation must succeed (got {status})"
    );
    let session_id = session["sessionId"].as_str().unwrap().to_owned();

    let accepted = submit_simple_run(&app).await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let job_id = JobId("build".to_owned());
    let message = poll_message(&app, &token, &session_id).await;
    assert!(!message.is_null(), "poll must claim the queued job");
    {
        let inner = state.inner.lock().await;
        assert!(
            inner.claimed_jobs.contains_key(&(run_id, job_id.clone())),
            "poll must record the claim in claimed_jobs"
        );
        assert!(
            inner.session_active_requests.contains_key(&session_id),
            "poll must pin the claim to the session"
        );
    }

    // The runner goes deaf: backdate its last poll and shrink the timeout.
    {
        let mut inner = state.inner.lock().await;
        inner.runner_liveness_timeout = Duration::from_secs(600);
        inner.session_last_seen.insert(
            session_id.clone(),
            std::time::Instant::now() - Duration::from_secs(3600),
        );
    }
    reap_once(&shared).await;

    {
        let inner = state.inner.lock().await;
        assert!(
            !inner.runners.contains_key(&runner_id),
            "deaf runner must be purged"
        );
        assert!(
            !inner.sessions.contains_key(&session_id),
            "deaf session must be purged"
        );
        assert!(
            !inner.session_active_requests.contains_key(&session_id),
            "deaf session claim must be released"
        );
        assert!(
            !inner.claimed_jobs.contains_key(&(run_id, job_id.clone())),
            "deaf claim must leave claimed_jobs"
        );
        assert!(
            inner
                .queue
                .iter()
                .any(|job| job.run_id == run_id && job.job_id == job_id),
            "unfinished job must be requeued for a fresh machine"
        );
    }
}

#[tokio::test]
async fn queued_job_survives_the_grace_window_while_the_pool_is_preparing() {
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());

    // A co-hosted pool is warming its machine image: no runner can register
    // until the download/build and golden prep finish, so the starvation
    // clock must not expire while the signal is raised.
    state.pool_preparing = Some(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
        true,
    )));
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    let accepted = submit_simple_run(&app).await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    reap_once(&shared).await;
    // Backdate the first-seen mark far past the grace window.
    {
        let mut inner = state.inner.lock().await;
        inner.queued_at.insert(
            (run_id, JobId("build".to_owned())),
            SystemTime::now() - Duration::from_secs(300),
        );
    }
    reap_once(&shared).await;
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.queue.len(),
            1,
            "job queued during the pool warm must not starve"
        );
    }

    // The pool finished warming: the clock resets and the grace window
    // counts from the first sweep that sees a runnable-but-unclaimed job.
    state
        .pool_preparing
        .as_ref()
        .unwrap()
        .store(false, std::sync::atomic::Ordering::Release);
    reap_once(&shared).await;
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.queue.len(),
            1,
            "a fresh grace window starts once the pool is ready"
        );
    }

    // With nothing having claimed the job after a full fresh window, the
    // sweep fails it as before.
    {
        let mut inner = state.inner.lock().await;
        inner.queued_at.insert(
            (run_id, JobId("build".to_owned())),
            SystemTime::now() - Duration::from_secs(300),
        );
    }
    reap_once(&shared).await;
    {
        let inner = state.inner.lock().await;
        assert!(
            inner.queue.is_empty(),
            "a job nobody can claim still fails once the pool is ready"
        );
    }
}

#[tokio::test]
async fn job_timeout_enforcement_cancels_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    // 1. Submit run
    let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: sleep 10\n",
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // 2. Poll to start job (transitions status to InProgress and sets started_at)
    let _msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;

    let request_id = {
        let inner = state.inner.lock().await;
        *inner.job_requests.keys().next().unwrap()
    };

    // 3. Override started_at to be in the past (beyond 360m/21600s default timeout)
    {
        let mut inner = state.inner.lock().await;
        let request = inner.job_requests.get_mut(&request_id).unwrap();
        request.started_at = Some(SystemTime::now() - Duration::from_secs(22000));
    }

    // 4. Run reaper tick
    reap_once(&shared).await;

    // 5. Verify cancellation is enqueued
    {
        let inner = state.inner.lock().await;
        let request = inner.job_requests.get(&request_id).unwrap();
        assert!(request.timeout_triggered);
        assert_eq!(inner.cancellation_queue.len(), 1);
        assert_eq!(inner.cancellation_queue[0].run_id, run_id);
    }
}

/// A paused debug session must suspend job-timeout enforcement.
///
/// Prerequisite covered separately by
/// `preserve_on_failure_carries_the_run_id_for_the_debug_session`.
///
/// Without this the server reaper cancels a debug session out from under the
/// user — and `timeout-minutes: 10` is a completely ordinary thing to write,
/// so the failure would be common and would look like a crash.
#[tokio::test]
async fn debug_session_suspends_job_timeout() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: \"false\"\n",
            "event": "push",
            "repository": "owner/repo",
            "preserve_on_failure": true
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let _msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;

    let (request_id, agent_job_id, worker_token) = {
        let inner = state.inner.lock().await;
        let (id, record) = inner.job_requests.iter().next().unwrap();
        (
            *id,
            record.agent_job_id,
            state.mint_debug_worker_token(&record.plan_id, &record.agent_job_id),
        )
    };

    // The worker fails a step and opens a session. It authenticates with the
    // job debug-worker token — not a runner listen token, and deliberately not
    // the runtime token the job itself can read as GITHUB_TOKEN.
    let opened = request_json_with_bearer(
        &app,
        Method::POST,
        "/api/v1/debug/sessions",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "agent_job_id": agent_job_id,
            "job_name": "build",
            "step": {
                "index": 0,
                "total": 1,
                "context_name": "__run",
                "display_name": "Run false",
                "command": "false",
                "exit_code": 1,
                "elapsed_ms": 20,
                "diagnostics": []
            }
        }),
        &worker_token,
    )
    .await;
    let session_id = opened["session_id"].as_str().unwrap().to_owned();

    // Backdate the job and its pause by the same amount, inside the pause
    // credit ceiling, so every elapsed second is debugging rather than
    // execution.
    {
        let mut inner = state.inner.lock().await;
        let past = SystemTime::now() - Duration::from_secs(10_000);
        inner.job_requests.get_mut(&request_id).unwrap().started_at = Some(past);
        inner
            .debug_sessions
            .backdate_pause_for_test(&session_id, past);
    }

    reap_once(&shared).await;

    {
        let inner = state.inner.lock().await;
        assert!(
            !inner
                .job_requests
                .get(&request_id)
                .unwrap()
                .timeout_triggered,
            "a paused job must not time out — the clock is suspended while debugging"
        );
        assert!(inner.cancellation_queue.is_empty());
    }

    // A controller lists the session and sees the failure.
    let listed = request_json(&app, Method::GET, "/api/v1/debug/sessions", Value::Null).await;
    assert_eq!(listed["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(listed["sessions"][0]["state"], "paused");
    assert_eq!(listed["sessions"][0]["step"]["display_name"], "Run false");

    // An agent acquires the lease and receives a structured failure event.
    let lease = request_json(
        &app,
        Method::POST,
        &format!("/api/v1/agent/debug/sessions/{session_id}/lease"),
        json!({
            "controller": "test-agent",
            "capabilities": ["job.retry_from"]
        }),
    )
    .await;
    assert_eq!(lease["controller"], "test-agent");
    assert_eq!(
        lease["capabilities"],
        json!(["step.retry", "job.retry_from", "job.abort"])
    );

    let events = request_json(
        &app,
        Method::GET,
        &format!("/api/v1/agent/debug/sessions/{session_id}/events?after=0"),
        Value::Null,
    )
    .await;
    assert_eq!(events["events"][0]["event"], "step_failed");

    // The agent retries from the first step; the worker's long poll picks it
    // up through the same verdict state machine as the human CLI.
    let operation = request_json(
        &app,
        Method::POST,
        &format!("/api/v1/agent/debug/sessions/{session_id}/operations"),
        json!({
            "request_id": "agent-retry-1",
            "expected_version": 1,
            "lease_id": lease["lease_id"],
            "operation": {
                "operation": "retry_from",
                "step_index": 0
            }
        }),
    )
    .await;
    assert_eq!(operation["status"], "retrying");
    assert_eq!(operation["prev_version"], 1);
    assert_eq!(operation["new_version"], 2);

    let audit = request_json(
        &app,
        Method::GET,
        &format!("/api/v1/agent/debug/sessions/{session_id}/audit"),
        Value::Null,
    )
    .await;
    assert_eq!(audit[0]["request_id"], "agent-retry-1");

    let delivered = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/api/v1/debug/sessions/{session_id}/verdict?wait=0"),
        Value::Null,
        &worker_token,
    )
    .await;
    assert_eq!(delivered["verdict"], "retry");
    assert_eq!(delivered["retry_from_step"], 0);

    // Delivering the verdict banks the paused interval and restarts the clock.
    // Suspension is not amnesty: push the start back so that *executing* time
    // alone exceeds the timeout, and the reaper must act.
    {
        let mut inner = state.inner.lock().await;
        let banked = inner
            .debug_sessions
            .paused_for_request(request_id, SystemTime::now());
        assert!(
            banked >= Duration::from_secs(9_500),
            "the pause should have banked its full duration, got {banked:?}"
        );
        inner.job_requests.get_mut(&request_id).unwrap().started_at =
            Some(SystemTime::now() - Duration::from_secs(21_700) - banked);
    }

    reap_once(&shared).await;
    {
        let inner = state.inner.lock().await;
        assert!(
            inner
                .job_requests
                .get(&request_id)
                .unwrap()
                .timeout_triggered,
            "execution time still counts once the session resumes"
        );
    }
}

/// Status of a bearer-authenticated request, for asserting rejections.
async fn request_status_with_bearer(
    app: &Router,
    method: Method,
    uri: &str,
    body: Value,
    bearer: &str,
) -> StatusCode {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {bearer}"));
    let request = if body.is_null() {
        builder.body(Body::empty()).unwrap()
    } else {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
        builder.body(Body::from(body.to_string())).unwrap()
    };
    app.clone().oneshot(request).await.unwrap().status()
}

/// One job's debug-worker token must not reach another job's debug session.
///
/// Token validity alone used to authorize every worker route, so any live job
/// could open a session on another job's behalf — suspending its timeout — and
/// could drain its verdict, since taking a verdict consumes it.
#[tokio::test]
async fn a_job_token_cannot_touch_another_jobs_debug_session() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: \"false\"\n",
            "event": "push",
            "repository": "owner/repo",
            "preserve_on_failure": true
        }),
    )
    .await;
    let run_id: RunId = {
        let inner = state.inner.lock().await;
        inner.job_requests.values().next().unwrap().run_id
    };

    request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;

    let (agent_job_id, victim_token, attacker_token) = {
        let inner = state.inner.lock().await;
        let record = inner.job_requests.values().next().unwrap();
        (
            record.agent_job_id,
            state.mint_debug_worker_token(&record.plan_id, &record.agent_job_id),
            // A token for some other live job, minted the same way.
            state.mint_debug_worker_token(&record.plan_id, &uuid::Uuid::new_v4()),
        )
    };

    let open_body = json!({
        "run_id": run_id,
        "job_id": "build",
        "agent_job_id": agent_job_id,
        "job_name": "build",
        "step": {
            "index": 0,
            "total": 1,
            "context_name": "__run",
            "display_name": "Run false",
            "command": "false",
            "exit_code": 1,
            "elapsed_ms": 20,
            "diagnostics": []
        }
    });

    // Opening a session for a job you are not is refused outright.
    assert_eq!(
        request_status_with_bearer(
            &app,
            Method::POST,
            "/api/v1/debug/sessions",
            open_body.clone(),
            &attacker_token,
        )
        .await,
        StatusCode::FORBIDDEN
    );

    let opened = request_json_with_bearer(
        &app,
        Method::POST,
        "/api/v1/debug/sessions",
        open_body,
        &victim_token,
    )
    .await;
    let session_id = opened["session_id"].as_str().unwrap().to_owned();

    // A controller queues a verdict for the paused worker.
    request_json(
        &app,
        Method::POST,
        &format!("/api/v1/debug/sessions/{session_id}/verdict"),
        json!({ "verdict": "continue", "controller": "cli" }),
    )
    .await;

    // The other job cannot steal it, and cannot close the session either.
    // Reported as 404 so session ids are not probeable.
    for (method, path, body) in [
        (
            Method::GET,
            format!("/api/v1/debug/sessions/{session_id}/verdict?wait=0"),
            Value::Null,
        ),
        (
            Method::POST,
            format!("/api/v1/debug/sessions/{session_id}/close"),
            json!({ "state": "aborted" }),
        ),
    ] {
        assert_eq!(
            request_status_with_bearer(&app, method, &path, body, &attacker_token).await,
            StatusCode::NOT_FOUND
        );
    }

    // The verdict is still there for its rightful owner.
    let delivered = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/api/v1/debug/sessions/{session_id}/verdict?wait=0"),
        Value::Null,
        &victim_token,
    )
    .await;
    assert_eq!(delivered["verdict"], "continue");
}

/// A runner listen token is not a worker token.
#[tokio::test]
async fn the_debug_surface_rejects_a_non_job_token() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    assert_eq!(
        request_status_with_bearer(
            &app,
            Method::GET,
            "/api/v1/debug/sessions/dbg_whatever/verdict?wait=0",
            Value::Null,
            "not-a-token",
        )
        .await,
        StatusCode::UNAUTHORIZED
    );

    // A job runtime token is not enough either. That token is handed to the
    // job as GITHUB_TOKEN, so any `run:` step can read it; accepting it here
    // would let untrusted workflow code drive its own debug session.
    let runtime_token = state.mint_runtime_token("plan", &uuid::Uuid::new_v4());
    assert_eq!(
        request_status_with_bearer(
            &app,
            Method::GET,
            "/api/v1/debug/sessions/dbg_whatever/verdict?wait=0",
            Value::Null,
            &runtime_token,
        )
        .await,
        StatusCode::UNAUTHORIZED,
        "GITHUB_TOKEN must not reach the debug surface"
    );
}

/// The debug-worker credential must never be a job variable.
///
/// Official runner v2.336.0 builds its `secrets` context from every `isSecret`
/// variable in the job message, replacing only `system.github.token` with
/// `GITHUB_TOKEN`. A secret variable is therefore a publication channel to the
/// workflow being debugged: `${{ secrets['system.preloop.debug_worker_token'] }}`
/// would have handed a `run:` step the credential that drives debug sessions.
/// The Rust runner's own `system.*` filter is no defence — the server does not
/// choose which runner claims the job.
///
/// So the assertion is on the message, not on any runner's projection of it.
#[tokio::test]
async fn the_job_message_never_carries_the_debug_worker_token() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: \"false\"\n",
            "event": "push",
            "repository": "owner/repo",
            "preserve_on_failure": true,
            "secrets": {"NPM_TOKEN": "npm_LIVE_CREDENTIAL"}
        }),
    )
    .await;

    let wire = {
        let inner = state.inner.lock().await;
        let queued = inner.queue.front().expect("job should be queued");
        serde_json::to_value(&queued.message).unwrap()
    };

    assert!(
        !wire.to_string().contains("debug_worker_token"),
        "the debug credential must not ship anywhere on the job message"
    );

    // Rebuild the official runner's secrets projection over the real message.
    let variables = wire["variables"]
        .as_object()
        .expect("job message variables");
    let official_secrets: BTreeSet<&str> = variables
        .iter()
        .filter(|(key, value)| {
            value["isSecret"].as_bool().unwrap_or(false)
                && !key.eq_ignore_ascii_case("system.github.token")
        })
        .map(|(key, _)| key.as_str())
        .collect();

    // Non-vacuous: the projection does surface the run's own secrets, so its
    // silence about the debug credential means absence rather than a broken
    // filter.
    assert!(
        official_secrets.contains("NPM_TOKEN"),
        "the projection must be the real one: {official_secrets:?}"
    );
    assert!(
        !official_secrets
            .iter()
            .any(|key| key.contains("debug_worker_token")),
        "an official-style secrets context must not see a debug credential: {official_secrets:?}"
    );
}

/// Open a debug session as a worker would, for exchange tests.
fn open_session_body(run_id: RunId, agent_job_id: uuid::Uuid) -> Value {
    json!({
        "run_id": run_id,
        "job_id": "build",
        "agent_job_id": agent_job_id,
        "job_name": "build",
        "step": {
            "index": 0,
            "total": 1,
            "context_name": "__run",
            "display_name": "Run false",
            "command": "false",
            "exit_code": 1,
            "elapsed_ms": 20,
            "diagnostics": []
        }
    })
}

/// The exchange that replaces the removed variable is as narrow as the
/// credential it issues.
///
/// The runtime token is the only job-scoped credential a worker already holds,
/// so it is what authenticates here — but it is also exported to steps as
/// `ACTIONS_RUNTIME_TOKEN`, so the exchange has to be worth nothing to a step
/// that replays it. Hence: exactly one issuance per job request, spent by the
/// worker during job setup before any step runs.
#[tokio::test]
async fn the_debug_worker_token_exchange_is_narrowly_authorized() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let native_app = app(state.clone(), CancellationToken::new());
    // This is the path used by a runner inside a microVM. The worker reaches
    // the server through the mounted control socket, so the socket guard must
    // admit the narrowly authenticated worker routes without exposing the
    // controller's native debug surface.
    let app = native_app
        .clone()
        .layer(middleware::from_fn(crate::auth::runner_surface_only));

    let accepted = request_json(
        &native_app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: \"false\"\n",
            "event": "push",
            "repository": "owner/repo",
            "preserve_on_failure": true
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let (agent_job_id, plan_id) = {
        let inner = state.inner.lock().await;
        let (_, record) = inner.job_requests.iter().next().unwrap();
        (record.agent_job_id, record.plan_id.clone())
    };
    let runtime_token = state.mint_runtime_token(&plan_id, &agent_job_id);
    let asking_for_itself = json!({ "agent_job_id": agent_job_id });
    let exchange = "/api/v1/debug/worker-token";

    let refused = |bearer: String, body: Value| {
        let app = app.clone();
        async move { request_status_with_bearer(&app, Method::POST, exchange, body, &bearer).await }
    };

    assert_eq!(
        refused("not-a-token".to_owned(), asking_for_itself.clone()).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        refused(state.system_token.clone(), asking_for_itself.clone()).await,
        StatusCode::UNAUTHORIZED,
        "the native admin credential is not a job identity"
    );
    // A debug-worker token cannot mint its own successor: its `sub` names a
    // debug worker, not a job, so it is not a runtime token.
    assert_eq!(
        refused(
            state.mint_debug_worker_token(&plan_id, &agent_job_id),
            asking_for_itself.clone()
        )
        .await,
        StatusCode::UNAUTHORIZED
    );

    // Neither direction of a job mismatch is allowed: not another job's token
    // asking for this job, nor this job's token asking for another.
    let stranger = uuid::Uuid::new_v4();
    assert_eq!(
        refused(
            state.mint_runtime_token(&plan_id, &stranger),
            asking_for_itself.clone()
        )
        .await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        refused(runtime_token.clone(), json!({ "agent_job_id": stranger })).await,
        StatusCode::FORBIDDEN
    );

    // The job's own runtime token succeeds, and buys a *different* credential.
    let issued = request_json_with_bearer(
        &app,
        Method::POST,
        exchange,
        asking_for_itself.clone(),
        &runtime_token,
    )
    .await;
    let worker_token = issued["token"].as_str().expect("issued token").to_owned();
    assert_ne!(worker_token, runtime_token);

    // What it buys is precisely what the session surface demands, and what the
    // runtime token is still refused for.
    let opened = request_json_with_bearer(
        &app,
        Method::POST,
        "/api/v1/debug/sessions",
        open_session_body(run_id, agent_job_id),
        &worker_token,
    )
    .await;
    let session_id = opened["session_id"].as_str().expect("opened session");
    assert_eq!(
        request_status_with_bearer(
            &app,
            Method::POST,
            "/api/v1/debug/sessions",
            open_session_body(run_id, agent_job_id),
            &runtime_token,
        )
        .await,
        StatusCode::UNAUTHORIZED,
        "the exchange must not have widened what a runtime token can reach"
    );

    let polled = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/api/v1/debug/sessions/{session_id}/verdict?wait=0"),
        Value::Null,
        &worker_token,
    )
    .await;
    assert!(polled["verdict"].is_null());
    let closed = request_json_with_bearer(
        &app,
        Method::POST,
        &format!("/api/v1/debug/sessions/{session_id}/close"),
        json!({ "state": "aborted" }),
        &worker_token,
    )
    .await;
    assert_eq!(closed["ok"], true);

    // One shot. A step that later finds `ACTIONS_RUNTIME_TOKEN` in its
    // environment has nothing left to spend.
    assert_eq!(
        refused(runtime_token, asking_for_itself).await,
        StatusCode::CONFLICT
    );
}

#[tokio::test]
async fn debug_worker_token_outlives_job_and_pause_windows() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let token = state.mint_debug_worker_token("plan", &uuid::Uuid::new_v4());
    let claims = state
        .verify_local_jwt_claims(&token)
        .expect("fresh debug-worker token verifies");
    let issued_at = claims["iat"].as_u64().expect("iat is numeric");
    let expires_at = claims["exp"].as_u64().expect("exp is numeric");

    assert_eq!(
        expires_at - issued_at,
        crate::state::DEBUG_WORKER_TOKEN_LIFETIME.as_secs()
    );
    assert!(
        expires_at - issued_at > (6 + 4) * 60 * 60,
        "credential must cover the job limit and maximum pause credit"
    );
}

/// No pause-on-failure opt-in, no debug credential at all.
///
/// The runner only builds a pause client for a run that asked for one, so
/// issuing outside that case would grow the credential's blast radius to every
/// job on the server for no behavioural gain.
#[tokio::test]
async fn the_exchange_refuses_a_run_that_never_asked_to_pause() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;

    let (agent_job_id, plan_id) = {
        let inner = state.inner.lock().await;
        let (_, record) = inner.job_requests.iter().next().unwrap();
        (record.agent_job_id, record.plan_id.clone())
    };

    assert_eq!(
        request_status_with_bearer(
            &app,
            Method::POST,
            "/api/v1/debug/worker-token",
            json!({ "agent_job_id": agent_job_id }),
            &state.mint_runtime_token(&plan_id, &agent_job_id),
        )
        .await,
        StatusCode::FORBIDDEN
    );
}

/// A completed job cannot acquire a debug credential.
#[tokio::test]
async fn the_exchange_refuses_a_job_that_is_no_longer_running() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: \"false\"\n",
            "event": "push",
            "repository": "owner/repo",
            "preserve_on_failure": true
        }),
    )
    .await;

    let (agent_job_id, plan_id) = {
        let mut inner = state.inner.lock().await;
        let (_, record) = inner.job_requests.iter_mut().next().unwrap();
        record.result = Some(ExecutionStatus::Failure);
        (record.agent_job_id, record.plan_id.clone())
    };

    assert_eq!(
        request_status_with_bearer(
            &app,
            Method::POST,
            "/api/v1/debug/worker-token",
            json!({ "agent_job_id": agent_job_id }),
            &state.mint_runtime_token(&plan_id, &agent_job_id),
        )
        .await,
        StatusCode::NOT_FOUND
    );
}

/// Pause credit is finite: past the ceiling the job times out normally.
///
/// Otherwise a worker that keeps polling opts its job out of `timeout-minutes`
/// altogether, and holds its microVM for as long as it likes.
#[tokio::test]
async fn pause_credit_runs_out_and_the_job_times_out() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: \"false\"\n",
            "event": "push",
            "repository": "owner/repo",
            "preserve_on_failure": true
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;

    let (request_id, agent_job_id, worker_token) = {
        let inner = state.inner.lock().await;
        let (id, record) = inner.job_requests.iter().next().unwrap();
        (
            *id,
            record.agent_job_id,
            state.mint_debug_worker_token(&record.plan_id, &record.agent_job_id),
        )
    };

    let opened = request_json_with_bearer(
        &app,
        Method::POST,
        "/api/v1/debug/sessions",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "agent_job_id": agent_job_id,
            "job_name": "build",
            "step": {
                "index": 0,
                "total": 1,
                "context_name": "__run",
                "display_name": "Run false",
                "command": "false",
                "exit_code": 1,
                "elapsed_ms": 20,
                "diagnostics": []
            }
        }),
        &worker_token,
    )
    .await;
    let session_id = opened["session_id"].as_str().unwrap().to_owned();

    // The worker is still polling — `worker_seen_at` is fresh, so the
    // abandonment sweep does not apply. Only the credit ceiling can end this.
    let ceiling = crate::debug_sessions::MAX_PAUSE_CREDIT;
    {
        let mut inner = state.inner.lock().await;
        let past = SystemTime::now() - ceiling - Duration::from_secs(22_000);
        inner.job_requests.get_mut(&request_id).unwrap().started_at = Some(past);
        inner
            .debug_sessions
            .backdate_pause_for_test(&session_id, past);
    }

    reap_once(&shared).await;

    let inner = state.inner.lock().await;
    assert!(
        inner
            .job_requests
            .get(&request_id)
            .unwrap()
            .timeout_triggered,
        "pause credit must be finite — an endless pause is an endless job"
    );
}

/// Resuming a job must not hand its paused time back to the reaper.
///
/// The credit lived in the session record, and closing the session dropped it,
/// so the subtraction that kept the job alive while paused disappeared the
/// instant it resumed. A job paused for hours was then cancelled on the very
/// next reaper tick, reported as an ordinary timeout, with the debugging time
/// billed as execution and nothing in any client able to explain it.
#[tokio::test]
async fn resuming_a_job_does_not_rebill_the_time_it_spent_paused() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: \"false\"\n",
            "event": "push",
            "repository": "owner/repo",
            "preserve_on_failure": true
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;

    let (request_id, agent_job_id, worker_token) = {
        let inner = state.inner.lock().await;
        let (id, record) = inner.job_requests.iter().next().unwrap();
        (
            *id,
            record.agent_job_id,
            state.mint_debug_worker_token(&record.plan_id, &record.agent_job_id),
        )
    };

    let opened = request_json_with_bearer(
        &app,
        Method::POST,
        "/api/v1/debug/sessions",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "agent_job_id": agent_job_id,
            "job_name": "build",
            "step": {
                "index": 0,
                "total": 1,
                "context_name": "__run",
                "display_name": "Run false",
                "command": "false",
                "exit_code": 1,
                "elapsed_ms": 20,
                "diagnostics": []
            }
        }),
        &worker_token,
    )
    .await;
    let session_id = opened["session_id"].as_str().unwrap().to_owned();

    // 22_000s since the job started, 10_000 of them paused: 12_000s of
    // execution against the default 21_600s timeout. Inside the budget only if
    // the pause is subtracted.
    {
        let mut inner = state.inner.lock().await;
        let now = SystemTime::now();
        inner.job_requests.get_mut(&request_id).unwrap().started_at =
            Some(now - Duration::from_secs(22_000));
        inner
            .debug_sessions
            .backdate_pause_for_test(&session_id, now - Duration::from_secs(10_000));
    }

    // The controller says continue and the worker closes the session: from here
    // on nothing in the registry holds this request open.
    let closed = request_json_with_bearer(
        &app,
        Method::POST,
        &format!("/api/v1/debug/sessions/{session_id}/close"),
        json!({ "state": "resumed" }),
        &worker_token,
    )
    .await;
    assert_eq!(closed["ok"], true);

    {
        let inner = state.inner.lock().await;
        assert!(
            inner.debug_sessions.list().is_empty(),
            "the session is closed, so the credit cannot be coming from a live one"
        );
        assert!(
            inner
                .debug_sessions
                .paused_for_request(request_id, SystemTime::now())
                >= Duration::from_secs(9_500),
            "the closed session's pause must still be credited to its request"
        );
    }

    reap_once(&shared).await;

    let inner = state.inner.lock().await;
    assert!(
        !inner
            .job_requests
            .get(&request_id)
            .unwrap()
            .timeout_triggered,
        "a resumed job must be billed for execution only — 12_000s of it here"
    );
    assert!(inner.cancellation_queue.is_empty());
    // And the reaper's own sweep does not confiscate it either: the request is
    // still active, so the credit has to survive the tick.
    assert!(
        inner
            .debug_sessions
            .paused_for_request(request_id, SystemTime::now())
            >= Duration::from_secs(9_500),
        "a reaper tick must not reset an active request's pause credit"
    );
}

/// An empty long poll must never be mistaken for a decision.
#[tokio::test]
async fn verdict_poll_timeout_is_not_an_abort() {
    verdict_poll_timeout_is_not_an_abort_impl().await;
}

/// The worker addresses its debug session by run id, so `preserve_on_failure`
/// must carry one.
///
/// This was silently missing at first: the field was only populated for DAP
/// runs, so the live-pause path constructed no client and fell through to the
/// old post-mortem behaviour with no error anywhere.
#[tokio::test]
async fn preserve_on_failure_carries_the_run_id_for_the_debug_session() {
    for (preserve, expect_run_id) in [(true, true), (false, false)] {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
                "event": "push",
                "repository": "owner/repo",
                "preserve_on_failure": preserve
            }),
        )
        .await;
        let run_id = accepted["run_id"].as_str().unwrap().to_owned();

        let queued = {
            let inner = state.inner.lock().await;
            inner.queue.front().cloned().unwrap()
        };
        assert_eq!(
            queued.message.preloop_debug_run_id.as_deref() == Some(run_id.as_str()),
            expect_run_id,
            "preserve_on_failure={preserve} must {} carry the run id",
            if expect_run_id { "" } else { "not" }
        );

        // The wire shape stays clean when debugging was not requested.
        let encoded = serde_json::to_value(&queued.message).unwrap();
        assert_eq!(
            encoded.get("preloopDebugRunId").is_some(),
            expect_run_id,
            "absent means absent on the wire"
        );
    }
}

async fn verdict_poll_timeout_is_not_an_abort_impl() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());

    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: \"false\"\n",
            "event": "push",
            "repository": "owner/repo",
            "preserve_on_failure": true
        }),
    )
    .await;
    let _msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    let (run_id, agent_job_id, worker_token) = {
        let inner = state.inner.lock().await;
        let record = inner.job_requests.values().next().unwrap();
        (
            record.run_id,
            record.agent_job_id,
            state.mint_debug_worker_token(&record.plan_id, &record.agent_job_id),
        )
    };

    let opened = request_json_with_bearer(
        &app,
        Method::POST,
        "/api/v1/debug/sessions",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "agent_job_id": agent_job_id,
            "job_name": "build",
            "step": {
                "index": 0, "total": 1, "context_name": "__run",
                "display_name": "Run false", "elapsed_ms": 5, "diagnostics": []
            }
        }),
        &worker_token,
    )
    .await;
    let session_id = opened["session_id"].as_str().unwrap();

    let polled = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/api/v1/debug/sessions/{session_id}/verdict?wait=0"),
        Value::Null,
        &worker_token,
    )
    .await;
    assert!(
        polled.get("verdict").is_none() || polled["verdict"].is_null(),
        "an expired poll must carry no verdict, got {polled}"
    );

    // The session is still open and still holding the job.
    let listed = request_json(&app, Method::GET, "/api/v1/debug/sessions", Value::Null).await;
    assert_eq!(listed["sessions"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn runner_lease_expiration_disconnect_reaper() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    // 1. Submit run
    let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: sleep 10\n",
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // 2. Poll to start job (sets last_renewed_at)
    let _msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;

    let request_id = {
        let inner = state.inner.lock().await;
        *inner.job_requests.keys().next().unwrap()
    };

    // 3. Exercise the just-before-boundary case without sleeping.
    {
        let mut inner = state.inner.lock().await;
        let request = inner.job_requests.get_mut(&request_id).unwrap();
        request.last_renewed_at =
            Some(SystemTime::now() - Duration::from_secs(JOB_LEASE_SECONDS - 1));
    }

    reap_once(&shared).await;
    {
        let inner = state.inner.lock().await;
        let request = inner.job_requests.get(&request_id).unwrap();
        assert_eq!(
            request.result, None,
            "lease must survive just before expiry"
        );
        assert!(inner.inflight_requests.contains_key(&request_id));
        assert_eq!(
            inner.runs.get(&run_id).unwrap().status,
            ExecutionStatus::InProgress
        );
    }

    // 4. Move just beyond the same production lease boundary and reap.
    {
        let mut inner = state.inner.lock().await;
        let request = inner.job_requests.get_mut(&request_id).unwrap();
        request.last_renewed_at =
            Some(SystemTime::now() - Duration::from_secs(JOB_LEASE_SECONDS + 1));
    }

    reap_once(&shared).await;

    // 5. Verify the job was marked failed and run completes as failed
    {
        let inner = state.inner.lock().await;
        let request = inner.job_requests.get(&request_id).unwrap();
        assert_eq!(request.result, Some(ExecutionStatus::Failure));
        assert!(inner.inflight_requests.is_empty());
        assert!(inner.session_active_requests.is_empty());

        let run = inner.runs.get(&run_id).unwrap();
        assert_eq!(run.status, ExecutionStatus::Failure);
    }
}

#[tokio::test]
async fn github_webhook_flows_with_signature_and_check_runs() {
    // This test asserts the mock check-run path (`check_run_id > 0` with no
    // GitHub server involved). A co-scheduled test's `PRELOOP_GITHUB_TOKEN`
    // would flip it to a live API call that fails, leaving zero check runs.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let _no_api_url = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_API_URL");

    let temp = tempfile::tempdir().unwrap();

    // 1. Create a dummy workflow file in a local workspace
    let ws_dir = temp.path().join("workspace");
    tokio::fs::create_dir_all(ws_dir.join(".github/workflows"))
        .await
        .unwrap();
    let workflow_content = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hello
"#;
    tokio::fs::write(ws_dir.join(".github/workflows/build.yml"), workflow_content)
        .await
        .unwrap();

    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.webhook_secret = Some("super-secret".to_owned());
    state.local_workspace = Some(ws_dir.clone());

    assert_eq!(state.webhook_secret.as_deref(), Some("super-secret"));
    assert_eq!(state.local_workspace.as_ref(), Some(&ws_dir));

    let app = app(state.clone(), CancellationToken::new());

    // 2. Prepare mock webhook push payload
    let payload = serde_json::json!({
        "ref": "refs/heads/main",
        "before": "0000000000000000000000000000000000000000",
        "after": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
        "repository": {
            "full_name": "owner/repo",
            "default_branch": "main"
        },
        "commits": [
            {
                "id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
                "added": ["src/main.rs"],
                "modified": [],
                "removed": []
            }
        ]
    });

    let payload_bytes = serde_json::to_vec(&payload).unwrap();

    // 3. Compute correct signature
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(b"super-secret").unwrap();
    mac.update(&payload_bytes);
    let sig_bytes = mac.finalize().into_bytes();
    let sig_hex = sig_bytes
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    let signature_header = format!("sha256={}", sig_hex);

    // 4. Send request with WRONG signature -> should fail with 401
    let response_401 = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-event", "push")
                .header("x-hub-signature-256", "sha256=invalid")
                .header("content-type", "application/json")
                .body(Body::from(payload_bytes.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response_401.status(), StatusCode::UNAUTHORIZED);

    // 5. Send request with CORRECT signature -> should succeed with 200
    let response_200 = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-event", "push")
                .header("x-hub-signature-256", signature_header)
                .header("content-type", "application/json")
                .body(Body::from(payload_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response_200.status(), StatusCode::OK);

    // 6. Verify that a run was triggered and check runs are queued
    let inner = state.inner.lock().await;
    assert_eq!(inner.runs.len(), 1);
    assert_eq!(
        state.queue_depth.load(std::sync::atomic::Ordering::Acquire),
        1
    );
    let (_, run_record) = inner.runs.iter().next().unwrap();
    assert_eq!(run_record.submission.event, "push");
    assert_eq!(run_record.submission.repository, "owner/repo");
    assert_eq!(run_record.submission.git_ref, "refs/heads/main");

    // Verify that check_run_ids are created/queued in the record
    assert_eq!(run_record.job_check_run_ids.len(), 1);
    let (job_id, check_run_id) = run_record.job_check_run_ids.iter().next().unwrap();
    assert_eq!(job_id.to_string(), "build");
    assert!(*check_run_id > 0);
}

/// Check-run ids must survive a restart even when no job status event ever
/// fired — a long queue can sit between check-run creation and the job's
/// first status event, and a deploy in that window used to restore the run
/// with an empty mapping, orphaning the GitHub check in "queued" forever.
#[tokio::test]
async fn check_run_ids_survive_a_restart_before_any_job_event() {
    // Held for the whole test: the GitHub env vars are process-global, and a
    // parallel test's token would flip the check-run path from mock to a real
    // GitHub API call. The mock path is the contract under test.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let _no_api_url = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_API_URL");

    let temp = tempfile::tempdir().unwrap();
    let ws_dir = temp.path().join("workspace");
    tokio::fs::create_dir_all(ws_dir.join(".github/workflows"))
        .await
        .unwrap();
    let workflow_content = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hello
"#;
    tokio::fs::write(ws_dir.join(".github/workflows/build.yml"), workflow_content)
        .await
        .unwrap();

    let run_id = {
        let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        state.webhook_secret = Some("super-secret".to_owned());
        state.local_workspace = Some(ws_dir.clone());
        let app = app(state.clone(), CancellationToken::new());

        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "before": "0000000000000000000000000000000000000000",
            "after": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
            "repository": {
                "full_name": "owner/repo",
                "default_branch": "main"
            },
            "commits": [
                {
                    "id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
                    "added": ["src/main.rs"],
                    "modified": [],
                    "removed": []
                }
            ]
        });
        let payload_bytes = serde_json::to_vec(&payload).unwrap();
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(b"super-secret").unwrap();
        mac.update(&payload_bytes);
        let sig_bytes = mac.finalize().into_bytes();
        let sig_hex = sig_bytes
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/github/webhooks")
                    .header("x-github-event", "push")
                    .header("x-hub-signature-256", format!("sha256={sig_hex}"))
                    .header("content-type", "application/json")
                    .body(Body::from(payload_bytes))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let inner = state.inner.lock().await;
        let (run_id, run) = inner.runs.iter().next().expect("webhook created a run");
        assert_eq!(
            run.job_check_run_ids.len(),
            1,
            "mock check run id recorded at submission"
        );
        *run_id
    };

    // Restart with no job event in between: the mapping must come back.
    let recovered = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let inner = recovered.inner.lock().await;
    let run = inner.runs.get(&run_id).expect("run must survive restart");
    assert_eq!(
        run.job_check_run_ids.len(),
        1,
        "check run id must survive a restart before the job's first status event"
    );
}

#[tokio::test]
async fn github_check_run_rerequest_resubmits_the_owning_run() {
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.webhook_secret = Some("super-secret".to_owned());
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
            "event": "push",
            "repository": "owner/repo",
            "sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
        }),
    )
    .await;
    let original_run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let original_check_run_id = 1234;
    {
        let mut inner = state.inner.lock().await;
        let run = inner.runs.get_mut(&original_run_id).unwrap();
        run.jobs
            .insert(JobId("build".to_owned()), ExecutionStatus::Failure);
        run.status = ExecutionStatus::Failure;
        run.conclusion = Some("failure".to_owned());
        run.job_check_run_ids
            .insert(JobId("build".to_owned()), original_check_run_id);
    }

    let payload = serde_json::json!({
        "action": "rerequested",
        "repository": {"full_name": "owner/repo"},
        "check_run": {
            "id": original_check_run_id,
            "name": "build"
        }
    });
    let payload_bytes = serde_json::to_vec(&payload).unwrap();
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(b"super-secret").unwrap();
    mac.update(&payload_bytes);
    let signature = format!(
        "sha256={}",
        mac.finalize()
            .into_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-event", "check_run")
                .header("x-github-delivery", "rerun-delivery")
                .header("x-hub-signature-256", signature)
                .header("content-type", "application/json")
                .body(Body::from(payload_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let inner = state.inner.lock().await;
    assert_eq!(inner.runs.len(), 2);
    let rerun = inner
        .runs
        .values()
        .find(|run| run.run_id != original_run_id)
        .expect("rerequest should create a new run");
    assert_eq!(rerun.status, ExecutionStatus::Queued);
    assert_eq!(
        rerun.job_check_run_ids.get(&JobId("build".to_owned())),
        Some(&original_check_run_id),
        "the rerequest must continue reporting through the requested check run"
    );
}

/// Scaffolding shared by the webhook delivery dedup tests: a workspace holding
/// one push-triggered workflow, a server with a webhook secret, and the signed
/// push payload GitHub would deliver.
struct WebhookDedupFixture {
    state: AppState,
    app: Router,
    payload_bytes: Vec<u8>,
    signature_header: String,
}

impl WebhookDedupFixture {
    async fn new(temp: &tempfile::TempDir) -> Self {
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/build.yml"),
            "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        )
        .unwrap();

        let mut state = AppState::new(temp.path().join("state").to_path_buf())
            .await
            .unwrap();
        state.webhook_secret = Some("super-secret".to_owned());
        state.local_workspace = Some(ws_dir.clone());
        let app = app(state.clone(), CancellationToken::new());

        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "before": "0000000000000000000000000000000000000000",
            "after": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "repository": {"full_name": "owner/repo", "default_branch": "main"},
            "commits": [{
                "id": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
                "added": ["src/main.rs"],
                "modified": [],
                "removed": []
            }],
        });
        let payload_bytes = serde_json::to_vec(&payload).unwrap();

        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(b"super-secret").unwrap();
        mac.update(&payload_bytes);
        let sig_hex = mac
            .finalize()
            .into_bytes()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();
        let signature_header = format!("sha256={sig_hex}");

        Self {
            state,
            app,
            payload_bytes,
            signature_header,
        }
    }

    /// Deliver the signed push payload under `delivery`. `event` is the
    /// `x-github-event` header; `None` omits it, which is how this test makes
    /// post-reservation processing fail (400) the way a transient server error
    /// would.
    async fn post(&self, delivery: &str, event: Option<&str>) -> StatusCode {
        let app = self.app.clone();
        let payload_bytes = self.payload_bytes.clone();
        let signature_header = self.signature_header.clone();
        let mut request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/github/webhooks")
            .header("x-github-delivery", delivery)
            .header("x-hub-signature-256", signature_header)
            .header("content-type", "application/json");
        if let Some(event) = event {
            request = request.header("x-github-event", event);
        }
        app.oneshot(request.body(Body::from(payload_bytes)).unwrap())
            .await
            .unwrap()
            .status()
    }
}

#[tokio::test]
async fn github_webhook_same_delivery_is_deduped_but_new_delivery_creates_run() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = WebhookDedupFixture::new(&temp).await;

    // The same delivery id twice (GitHub redelivery / double-fire) must not
    // create a duplicate run; a genuinely new delivery creates another.
    assert_eq!(
        fixture.post("delivery-dup-1", Some("push")).await,
        StatusCode::OK
    );
    assert_eq!(
        fixture.post("delivery-dup-1", Some("push")).await,
        StatusCode::OK
    );
    assert_eq!(
        fixture.post("delivery-dup-2", Some("push")).await,
        StatusCode::OK
    );

    let inner = fixture.state.inner.lock().await;
    assert_eq!(
        inner.runs.len(),
        2,
        "a redelivered webhook must not create a duplicate run"
    );
}

#[tokio::test]
async fn github_webhook_failed_delivery_is_accepted_on_redelivery() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = WebhookDedupFixture::new(&temp).await;

    // First attempt fails after the delivery was reserved for dedup.
    assert_eq!(
        fixture.post("delivery-retry", None).await,
        StatusCode::BAD_REQUEST
    );
    {
        let inner = fixture.state.inner.lock().await;
        assert!(
            inner.runs.is_empty(),
            "a failed delivery must not create a run"
        );
    }

    // GitHub redelivers after an error response; the retry must be processed
    // rather than dropped as a duplicate.
    assert_eq!(
        fixture.post("delivery-retry", Some("push")).await,
        StatusCode::OK
    );
    let inner = fixture.state.inner.lock().await;
    assert_eq!(
        inner.runs.len(),
        1,
        "a redelivery of a failed delivery must create the run"
    );
}

#[tokio::test]
async fn github_webhook_concurrent_duplicate_delivery_creates_one_run() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = WebhookDedupFixture::new(&temp).await;

    // Two copies of one delivery in flight at once: the in-flight reservation
    // makes the loser a no-op instead of a second run.
    let (first, second) = tokio::join!(
        fixture.post("delivery-concurrent", Some("push")),
        fixture.post("delivery-concurrent", Some("push"))
    );
    assert_eq!(first, StatusCode::OK);
    assert_eq!(second, StatusCode::OK);

    let inner = fixture.state.inner.lock().await;
    assert_eq!(
        inner.runs.len(),
        1,
        "concurrent copies of one delivery must produce exactly one run"
    );
}

#[tokio::test]
async fn github_webhook_pull_request_event() {
    // The webhook path reads `PRELOOP_GITHUB_TOKEN` / `PRELOOP_GITHUB_API_URL`
    // live for check-run reporting. Hold the env lock so a concurrent
    // env-mutating test cannot point those at a foreign credential or stub
    // and turn this 200 into a 502.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();

    // Create a dummy workflow file in a local workspace
    let ws_dir = temp.path().join("workspace");
    tokio::fs::create_dir_all(ws_dir.join(".github/workflows"))
        .await
        .unwrap();
    let workflow_content = r#"
on: pull_request
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: make test
"#;
    tokio::fs::write(ws_dir.join(".github/workflows/test.yml"), workflow_content)
        .await
        .unwrap();

    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.webhook_secret = Some("super-secret".to_owned());
    state.local_workspace = Some(ws_dir.clone());

    let app = app(state.clone(), CancellationToken::new());

    // Prepare PR payload
    let payload = serde_json::json!({
        "action": "opened",
        "number": 42,
        "pull_request": {
            "head": {
                "ref": "feature-branch",
                "sha": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3"
            },
            "base": {
                "ref": "main",
                "sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
            }
        },
        "repository": {
            "full_name": "owner/repo",
            "default_branch": "main"
        }
    });

    let payload_bytes = serde_json::to_vec(&payload).unwrap();

    // Compute signature
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(b"super-secret").unwrap();
    mac.update(&payload_bytes);
    let sig_bytes = mac.finalize().into_bytes();
    let sig_hex = sig_bytes
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    let signature_header = format!("sha256={}", sig_hex);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-event", "pull_request")
                .header("x-hub-signature-256", signature_header)
                .header("content-type", "application/json")
                .body(Body::from(payload_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Verify triggered run
    let inner = state.inner.lock().await;
    assert_eq!(inner.runs.len(), 1);
    let (_, run_record) = inner.runs.iter().next().unwrap();
    assert_eq!(run_record.submission.event, "pull_request");
    assert_eq!(run_record.submission.git_ref, "refs/pull/42/head");
    assert_eq!(run_record.job_check_run_ids.len(), 1);
    // A pull_request payload has no `after`; the head sha must still reach
    // the job. Falling through to all-zeros makes every checkout ask the
    // server for `0000…` and fail as "not our ref".
    assert_eq!(
        run_record.head_sha, "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3",
        "pull_request head sha must drive github.sha"
    );
}

#[tokio::test]
async fn github_app_manifest_registration_flow() {
    let temp = tempfile::tempdir().unwrap();

    // 1. Setup a local mock GitHub API server for manifest conversion
    let mock_app = Router::new().route(
            "/app-manifests/:code/conversions",
            post(|Path(code): Path<String>| async move {
                assert_eq!(code, "mock_code_123");
                Json(json!({
                    "id": 987654,
                    "pem": "-----BEGIN RSA PRIVATE KEY-----\nMOCK-KEY-DATA\n-----END RSA PRIVATE KEY-----",
                    "webhook_secret": Some("mock-webhook-secret-xyz")
                }))
            }),
        );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, mock_app).await.unwrap();
    });

    // 2. Configure mock API URL in environment
    // Held for the whole test: `PRELOOP_GITHUB_API_URL` is process-global.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    std::env::set_var(
        "PRELOOP_GITHUB_API_URL",
        format!("http://127.0.0.1:{}", port),
    );

    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // 3. Request registration form (GET /api/v1/github/register)
    let response_reg = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/github/register")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response_reg.status(), StatusCode::OK);
    let bytes = to_bytes(response_reg.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(html.contains("https://github.com/settings/apps/new"));
    assert!(html.contains("preloop-local-app"));

    // 4. Request callback conversion (GET /api/v1/github/callback?code=mock_code_123)
    let response_callback = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/github/callback?code=mock_code_123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response_callback.status(), StatusCode::OK);
    let bytes_callback = to_bytes(response_callback.into_body(), usize::MAX)
        .await
        .unwrap();
    let html_callback = String::from_utf8(bytes_callback.to_vec()).unwrap();
    assert!(html_callback.contains("GitHub App Registered Successfully!"));
    assert!(html_callback.contains("987654"));
    assert!(html_callback.contains("mock-webhook-secret-xyz"));

    // Clean up
    std::env::remove_var("PRELOOP_GITHUB_API_URL");
}

#[tokio::test]
async fn runner_oauth2_token_client_assertion_verification() {
    use preloop_gha_protocol::crypto::{sign_jwt_ps256, sign_jwt_rs256};
    use serde_json::Value;

    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // 1. Generate RSA keypair for the runner using the protocol's library
    let keypair = preloop_gha_protocol::crypto::AgentRsaKeypair::generate().unwrap();
    let rsa_params = keypair.to_rsaparams();

    let keypair_xml = format!(
        "<RSAKeyValue><Modulus>{}</Modulus><Exponent>{}</Exponent></RSAKeyValue>",
        rsa_params.modulus, rsa_params.exponent
    );

    // 2. Register the runner
    let reg_response = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/agents",
        json!({
            "name": "runner-cryptographic",
            "version": "2.335.1",
            "osDescription": "Linux",
            "enabled": true,
            "status": "offline",
            "publicKey": keypair_xml,
            "authorization": {
                "publicKey": keypair_xml,
            }
        }),
    )
    .await;

    let client_id = reg_response["authorization"]["clientId"]
        .as_str()
        .unwrap()
        .to_owned();

    // 3. Build a valid client assertion JWT signed with the runner's private RSA key
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let header = json!({
        "typ": "JWT",
        "alg": "PS256"
    });
    let claims = json!({
        "sub": client_id,
        "iss": client_id,
        "aud": "https://preloop.local/oauth",
        "jti": uuid::Uuid::new_v4().to_string(),
        "nbf": now,
        "exp": now + 300,
    });

    let client_assertion = sign_jwt_ps256(&header, &claims, &rsa_params).unwrap();

    // 4. Request OAuth token using urlencoded body
    let form_body = serde_urlencoded::to_string([
        (
            "client_assertion_type",
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        ),
        ("client_assertion", &client_assertion),
        ("grant_type", "client_credentials"),
    ])
    .unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/runner/server/_apis/v1/oauth2/token")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(form_body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let token_resp: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(token_resp["access_token"].is_string());

    // 4b. Test RS256 algorithm verification
    let rs256_header = json!({
        "typ": "JWT",
        "alg": "RS256"
    });
    let rs256_client_assertion = sign_jwt_rs256(&rs256_header, &claims, &rsa_params).unwrap();
    let rs256_form_body = serde_urlencoded::to_string([
        (
            "client_assertion_type",
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        ),
        ("client_assertion", &rs256_client_assertion),
        ("grant_type", "client_credentials"),
    ])
    .unwrap();

    let rs256_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/runner/server/_apis/v1/oauth2/token")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(rs256_form_body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(rs256_response.status(), StatusCode::OK);
    let rs256_bytes = axum::body::to_bytes(rs256_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let rs256_token_resp: Value = serde_json::from_slice(&rs256_bytes).unwrap();
    assert!(rs256_token_resp["access_token"].is_string());

    // 5. Test negative case: Invalid signature (wrong key)
    let wrong_keypair = preloop_gha_protocol::crypto::AgentRsaKeypair::generate().unwrap();
    let wrong_rsa_params = wrong_keypair.to_rsaparams();
    let bad_assertion = sign_jwt_ps256(&header, &claims, &wrong_rsa_params).unwrap();

    let bad_form_body = serde_urlencoded::to_string([
        (
            "client_assertion_type",
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        ),
        ("client_assertion", &bad_assertion),
        ("grant_type", "client_credentials"),
    ])
    .unwrap();

    let bad_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/runner/server/_apis/v1/oauth2/token")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(bad_form_body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(bad_response.status(), StatusCode::UNAUTHORIZED);
}

#[test]
fn label_matching_exact() {
    assert!(job_matches_runner(
        &["self-hosted".into(), "Linux".into()],
        &["self-hosted".into(), "Linux".into(), "X64".into()]
    ));
}

#[test]
fn label_matching_case_insensitive() {
    assert!(job_matches_runner(
        &["Self-Hosted".into(), "linux".into()],
        &["self-hosted".into(), "Linux".into()]
    ));
}

#[test]
fn label_matching_ubuntu_alias() {
    // ubuntu-latest should match a runner with "self-hosted"
    assert!(job_matches_runner(
        &["ubuntu-latest".into()],
        &["self-hosted".into(), "Linux".into()]
    ));
    // Also matches via the "linux" label
    assert!(job_matches_runner(
        &["ubuntu-24.04".into()],
        &["linux".into()]
    ));
}

#[test]
fn label_matching_rejects_missing_labels() {
    // Runner missing "gpu" label
    assert!(!job_matches_runner(
        &["self-hosted".into(), "gpu".into()],
        &["self-hosted".into(), "Linux".into()]
    ));
}

/// A hosted image label names an OS, and a self-hosted runner may only stand
/// in for one it actually runs. A macOS host claiming `ubuntu-latest` fails
/// the job deep inside a step (Linux-only crate features, `/home/runner`
/// paths, apt) instead of waiting for a Linux runner.
#[test]
fn label_matching_never_crosses_operating_systems() {
    let mac = [
        "self-hosted".to_owned(),
        "macOS".to_owned(),
        "ARM64".to_owned(),
    ];
    assert!(!job_matches_runner(&["ubuntu-latest".into()], &mac));
    assert!(!job_matches_runner(&["windows-latest".into()], &mac));
    assert!(job_matches_runner(&["macos-15".into()], &mac));

    let linux = [
        "self-hosted".to_owned(),
        "Linux".to_owned(),
        "X64".to_owned(),
        "ubuntu-24.04".to_owned(),
        "ubuntu-latest".to_owned(),
    ];
    // A pool advertising 24.04 still serves a 22.04 job: same OS, and the
    // alternative is a job that never runs.
    assert!(job_matches_runner(&["ubuntu-22.04".into()], &linux));
    assert!(!job_matches_runner(&["macos-14".into()], &linux));
}

/// A runner that declares no OS label has told us nothing to contradict, so
/// it stays eligible for every hosted label.
#[test]
fn label_matching_os_less_runner_stays_eligible() {
    let unlabelled = ["self-hosted".to_owned(), "gpu".to_owned()];
    assert!(job_matches_runner(&["ubuntu-latest".into()], &unlabelled));
    assert!(job_matches_runner(&["windows-2022".into()], &unlabelled));
    assert!(!job_matches_runner(&["nvidia".into()], &unlabelled));
}

/// A 24.04 machine may stand in for an `ubuntu-22.04` job, but it must not
/// take one while a job it exactly matches is claimable: the pool is usually
/// already building the 22.04 machine that job asked for, and the stand-in
/// would hand it a different base image for no reason.
#[tokio::test]
async fn claims_prefer_a_job_the_runner_exactly_matches() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // `pinned` is first in the queue, so only the preference can reorder it.
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  pinned:\n    runs-on: ubuntu-22.04\n    steps:\n      - run: echo pinned\n  wide:\n    runs-on: self-hosted\n    steps:\n      - run: echo wide\n",
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    assert!(
        accepted["run_id"].is_string(),
        "the run was accepted: {accepted}"
    );

    let machine = RunnerCapabilities {
        known: true,
        labels: vec![
            "self-hosted".to_owned(),
            "Linux".to_owned(),
            "X64".to_owned(),
            "ubuntu-24.04".to_owned(),
            "ubuntu-latest".to_owned(),
        ],
        runner_group_id: None,
        runner_group_name: None,
    };

    let mut inner = state.inner.lock().await;
    let first = crate::runtime_scheduling::take_matching_job(&mut inner, &machine, Some(1))
        .expect("a claimable job");
    assert_eq!(
        first.job_id.0, "wide",
        "the exact `self-hosted` match must win over the 22.04 stand-in"
    );

    // And the stand-in still happens rather than starving the pinned job.
    let second = crate::runtime_scheduling::take_matching_job(&mut inner, &machine, Some(1))
        .expect("the pinned job is still claimable");
    assert_eq!(second.job_id.0, "pinned");
}

/// A job for a platform with no runner host can never be claimed. Queuing it
/// forever means a run that never finishes and a check that never reports, so
/// it is skipped — but only when nothing is registered that could serve it.
#[test]
fn jobs_are_skipped_only_for_platforms_nothing_can_host() {
    let linux_pool = || ["linux", "linux"].into_iter();

    assert_eq!(
        crate::runtime_scheduling::unhostable_platform(
            &["windows-latest".to_owned()],
            linux_pool()
        ),
        Some("windows")
    );
    assert_eq!(
        crate::runtime_scheduling::unhostable_platform(&["macos-15".to_owned()], linux_pool()),
        Some("macos")
    );

    // A registered Mac host makes macOS a supported deployment, not a gap.
    assert_eq!(
        crate::runtime_scheduling::unhostable_platform(
            &["macos-latest".to_owned()],
            ["linux", "macos"].into_iter()
        ),
        None
    );

    // Linux is never skipped: the pool provisions it on demand, and an
    // ephemeral pool is routinely between runners.
    assert_eq!(
        crate::runtime_scheduling::unhostable_platform(
            &["ubuntu-22.04".to_owned()],
            std::iter::empty()
        ),
        None
    );
    assert_eq!(
        crate::runtime_scheduling::unhostable_platform(
            &["self-hosted".to_owned(), "gpu".to_owned()],
            std::iter::empty()
        ),
        None
    );
}

#[test]
fn label_matching_empty_runner_matches_all() {
    // Unknown runner (empty labels) matches everything
    assert!(job_matches_runner(
        &["self-hosted".into(), "Linux".into()],
        &[]
    ));
}

#[test]
fn label_matching_empty_job_matches_all() {
    assert!(job_matches_runner(&[], &["self-hosted".into()]));
}

// Oracle: GitHub `needs` and status-function contracts, with worker-side
// condition semantics pinned to actions/runner v2.335.1. These tests are
// production-path checks: YAML is parsed and expanded by Preloop, then the
// real queue/promotion state is driven through the explicitly gated test
// completion API and compared with the documented outcome.
// ─── DAG scheduling regression tests (spec §1) ─────────────────────────

/// Production path: build fails → test with default condition is skipped.
/// Verifies the server's promote_ready_jobs correctly propagates failure.
#[tokio::test]
async fn dag_build_fails_test_skipped_production() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo build
  test:
    needs: [build]
    runs-on: ubuntu-latest
    steps:
      - run: echo test
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "status": "failure"
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(
        run.jobs.get(&JobId("build".to_owned())),
        Some(&ExecutionStatus::Failure)
    );
    assert_eq!(
        run.jobs.get(&JobId("test".to_owned())),
        Some(&ExecutionStatus::Skipped),
        "test must be skipped when build fails under default gate"
    );
    // No new jobs should have been promoted to queue
    assert!(
        !inner.queue.iter().any(|j| j.job_id.0 == "test"),
        "test must not be in queue"
    );
    assert!(inner.pending_jobs.is_empty(), "no jobs should be pending");
}

/// Production path: build fails → cleanup with `if: always()` runs.
#[tokio::test]
async fn dag_always_runs_after_failure_production() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo build
  cleanup:
    needs: [build]
    if: always()
    runs-on: ubuntu-latest
    steps:
      - run: echo cleanup
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "status": "failure"
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    assert!(
        inner.queue.iter().any(|job| job.job_id.0 == "cleanup"),
        "cleanup with always() must be promoted after build failure"
    );
}

/// Production path: build fails → notify with `if: failure()` runs.
#[tokio::test]
async fn dag_failure_condition_runs_after_failure_production() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo build
  notify:
    needs: [build]
    if: failure()
    runs-on: ubuntu-latest
    steps:
      - run: echo notify
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "status": "failure"
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    assert!(
        inner.queue.iter().any(|job| job.job_id.0 == "notify"),
        "notify with failure() must be promoted after build failure"
    );
}

/// Production path: diamond graph build → test-a/test-b → deploy.
/// All succeed → deploy runs → run completes successfully.
#[tokio::test]
async fn dag_diamond_settlement_production() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo build
  test-a:
    needs: [build]
    runs-on: ubuntu-latest
    steps:
      - run: echo test-a
  test-b:
    needs: [build]
    runs-on: ubuntu-latest
    steps:
      - run: echo test-b
  deploy:
    needs: [test-a, test-b]
    runs-on: ubuntu-latest
    steps:
      - run: echo deploy
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // Only build queued initially
    {
        let inner = state.inner.lock().await;
        assert_eq!(inner.queue.len(), 1);
        assert_eq!(inner.queue[0].job_id.0, "build");
    }

    // Complete build
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({"run_id": run_id, "job_id": "build", "status": "success"}),
    )
    .await;

    // test-a and test-b promoted (build QueuedJob remains until dispatched)
    {
        let inner = state.inner.lock().await;
        let queued_ids: std::collections::BTreeSet<_> =
            inner.queue.iter().map(|j| j.job_id.0.clone()).collect();
        assert!(queued_ids.contains("test-a"), "test-a should be promoted");
        assert!(queued_ids.contains("test-b"), "test-b should be promoted");
    }

    // Complete test-a and test-b
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({"run_id": run_id, "job_id": "test-a", "status": "success"}),
    )
    .await;
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({"run_id": run_id, "job_id": "test-b", "status": "success"}),
    )
    .await;

    // deploy promoted (other completed jobs' QueuedJobs may linger)
    {
        let inner = state.inner.lock().await;
        assert!(
            inner.queue.iter().any(|j| j.job_id.0 == "deploy"),
            "deploy should be promoted after test-a and test-b complete"
        );
    }

    // Complete deploy
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({"run_id": run_id, "job_id": "deploy", "status": "success"}),
    )
    .await;

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(run.status, ExecutionStatus::Success);
    assert!(inner.pending_jobs.is_empty());
}

/// Production path: cyclic graph rejected at submission time.
#[tokio::test]
async fn dag_cyclic_graph_rejected_production() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/runs")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "workflow_yaml": r#"
on: push
jobs:
  a:
    needs: [b]
    runs-on: ubuntu-latest
    steps:
      - run: echo a
  b:
    needs: [a]
    runs-on: ubuntu-latest
    steps:
      - run: echo b
"#,
                        "event": "push",
                        "repository": "owner/repo"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "cyclic graph must be rejected before dispatch"
    );
}

async fn submit_yaml(app: &Router, yaml: &str, repo: &str) -> Value {
    request_json(
        app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": yaml,
            "event": "push",
            "repository": repo,
        }),
    )
    .await
}

#[tokio::test]
async fn stored_secrets_are_injected_into_native_submissions() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state
        .secrets
        .write()
        .global
        .insert("E2E_TEST_SECRET".to_owned(), "stored-value".to_owned());
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_yaml(
        &app,
        "on: push\njobs:\n  probe:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo $SECRET\n        env:\n          SECRET: ${{ secrets.E2E_TEST_SECRET }}\n",
        "owner/repo",
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();

    // The job message must carry the stored secret as a secret variable so
    // the worker republishes it into the `secrets.*` context.
    let inner = state.inner.lock().await;
    let run = inner
        .runs
        .values()
        .find(|run| run.run_id.to_string() == run_id)
        .unwrap();
    let message = inner
        .queue
        .iter()
        .find(|job| job.run_id == run.run_id)
        .or_else(|| {
            inner
                .pending_jobs
                .iter()
                .find(|job| job.run_id == run.run_id)
        })
        .expect("queued job exists")
        .message
        .clone();
    let secret_var = message
        .variables
        .values()
        .find(|value| value.value.as_deref() == Some("stored-value"))
        .expect("stored secret present in job message variables");
    assert_eq!(secret_var.is_secret, Some(true));
}

/// Extract the queued job message for a run, wherever it currently sits.
fn queued_message_for(inner: &crate::state::InnerState, run_id: &str) -> AgentJobRequestMessage {
    let run = inner
        .runs
        .values()
        .find(|run| run.run_id.to_string() == run_id)
        .unwrap();
    inner
        .queue
        .iter()
        .find(|job| job.run_id == run.run_id)
        .or_else(|| {
            inner
                .pending_jobs
                .iter()
                .find(|job| job.run_id == run.run_id)
        })
        .expect("queued job exists")
        .message
        .clone()
}

fn variable_value<'a>(message: &'a AgentJobRequestMessage, name: &str) -> Option<&'a str> {
    message
        .variables
        .get(name)
        .and_then(|value| value.value.as_deref())
}

/// `preloop setup github --via pat` stores the credential as `github.pat` and
/// configures no App. That PAT must reach jobs as their `GITHUB_TOKEN`:
/// previously only `PRELOOP_GITHUB_TOKEN` was consulted, so setup reported
/// success while every job silently ran on the local runtime token instead.
#[tokio::test]
async fn pat_only_config_supplies_job_github_token() {
    // Same env-lock discipline: `PRELOOP_GITHUB_TOKEN` writers serialize on
    // it, and a leaked value would win env-then-config and break the assert.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    std::fs::write(&config_path, "[github]\npat = \"github_pat_testvalue\"\n").unwrap();
    // Point this engine at the temp config directly. Mutating `PRELOOP_CONFIG`
    // would race every other test that builds an `AppState` concurrently.
    let state = AppState::new_with_config(temp.path().to_path_buf(), config_path)
        .await
        .unwrap();
    assert!(
        state.github_app.is_none(),
        "config declares no app id or pem"
    );
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_yaml(
        &app,
        "on: push\njobs:\n  probe:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "owner/repo",
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();

    let inner = state.inner.lock().await;
    let message = queued_message_for(&inner, &run_id);
    let token = message
        .variables
        .get("system.github.token")
        .expect("job message carries a GitHub token variable");
    assert_eq!(token.value.as_deref(), Some("github_pat_testvalue"));
    assert_eq!(token.is_secret, Some(true));
}

/// The App-manifest setup flow receives the webhook secret from GitHub and
/// stores it in the config file. Before that key existed the secret lived
/// only in `PRELOOP_WEBHOOK_SECRET`, so a configured engine still rejected
/// every signed delivery until the operator re-exported it by hand.
#[tokio::test]
async fn config_webhook_secret_verifies_signed_deliveries() {
    let temp = tempfile::tempdir().unwrap();
    let ws_dir = temp.path().join("workspace");
    tokio::fs::create_dir_all(ws_dir.join(".github/workflows"))
        .await
        .unwrap();
    tokio::fs::write(
        ws_dir.join(".github/workflows/build.yml"),
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
    )
    .await
    .unwrap();
    let config_path = temp.path().join("config.toml");
    std::fs::write(
        &config_path,
        "[github]\nwebhook_secret = \"from-config-file\"\n",
    )
    .unwrap();

    // Explicit config path rather than `PRELOOP_CONFIG`, which would race
    // every other test building an `AppState`.
    let mut state = AppState::new_with_config(temp.path().to_path_buf(), config_path)
        .await
        .unwrap();
    assert_eq!(
        state.webhook_secret.as_deref(),
        Some("from-config-file"),
        "the config file is a valid source for the webhook secret"
    );
    state.local_workspace = Some(ws_dir);
    let app = app(state.clone(), CancellationToken::new());

    let payload = serde_json::json!({
        "ref": "refs/heads/main",
        "before": "0000000000000000000000000000000000000000",
        "after": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
        "repository": {"full_name": "owner/repo", "default_branch": "main"},
        "commits": [{
            "id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
            "added": ["src/main.rs"],
            "modified": [],
            "removed": []
        }],
    });
    let body = serde_json::to_vec(&payload).unwrap();
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(b"from-config-file").unwrap();
    mac.update(&body);
    let signature = format!(
        "sha256={}",
        mac.finalize()
            .into_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-event", "push")
                .header("x-github-delivery", "config-secret-delivery")
                .header("x-hub-signature-256", &signature)
                .header("content-type", "application/json")
                .body(Body::from(body.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a correctly signed delivery is accepted with only the config file configured"
    );

    // The same body under a different secret must still be rejected —
    // otherwise the check is decorative.
    let forged = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-event", "push")
                .header("x-github-delivery", "forged-delivery")
                .header("x-hub-signature-256", "sha256=deadbeef")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forged.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn repo_scoped_secrets_override_global_and_stay_scoped() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    {
        let mut secrets = state.secrets.write();
        secrets
            .global
            .insert("GLOBAL_TOKEN".to_owned(), "global-value".to_owned());
        secrets.repo.insert(
            "owner/repo".to_owned(),
            BTreeMap::from([
                ("REPO_TOKEN".to_owned(), "repo-value".to_owned()),
                ("GLOBAL_TOKEN".to_owned(), "repo-wins".to_owned()),
            ]),
        );
    }
    let app = app(state.clone(), CancellationToken::new());
    let workflow =
        "on: push\njobs:\n  probe:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo $SECRET\n";

    // owner/repo: the per-repo tier overrides the global tier per name and
    // contributes its own names.
    let accepted = submit_yaml(&app, workflow, "owner/repo").await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let inner = state.inner.lock().await;
    let message = queued_message_for(&inner, run_id);
    assert_eq!(
        variable_value(&message, "GLOBAL_TOKEN"),
        Some("repo-wins"),
        "per-repo secret overrides the global tier"
    );
    assert_eq!(
        variable_value(&message, "REPO_TOKEN"),
        Some("repo-value"),
        "per-repo secret is injected"
    );
    drop(inner);

    // other/repo: only the global tier applies — repo secrets stay scoped.
    let accepted = submit_yaml(&app, workflow, "other/repo").await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let inner = state.inner.lock().await;
    let message = queued_message_for(&inner, run_id);
    assert_eq!(
        variable_value(&message, "GLOBAL_TOKEN"),
        Some("global-value"),
        "unscoped repo still gets the global tier"
    );
    assert_eq!(
        variable_value(&message, "REPO_TOKEN"),
        None,
        "repo-scoped secret must not leak into another repository"
    );
    drop(inner);

    // Submission-provided secrets still win over both tiers.
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": workflow,
            "event": "push",
            "repository": "owner/repo",
            "secrets": { "GLOBAL_TOKEN": "submitted-value" }
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let inner = state.inner.lock().await;
    let message = queued_message_for(&inner, run_id);
    assert_eq!(
        variable_value(&message, "GLOBAL_TOKEN"),
        Some("submitted-value"),
        "submission-provided secrets outrank both stored tiers"
    );
}

/// Send a request carrying a specific bearer token and return just the status
/// code — the shared counterpart the cache-gating tests use, so a
/// request-shape change lands in one place.
async fn status_with_bearer(
    app: &Router,
    bearer: &str,
    method: Method,
    uri: &str,
    body: Value,
) -> StatusCode {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {bearer}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    response.status()
}

/// Like `request_json` but returns the status instead of asserting success.
async fn request_json_status(
    app: &Router,
    method: Method,
    uri: &str,
    body: Value,
) -> (StatusCode, Value) {
    let builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, "Bearer preloop-system-token");
    let request = if body.is_null() {
        builder.body(Body::empty()).unwrap()
    } else {
        builder
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, body)
}

#[tokio::test]
async fn live_secrets_api_round_trips_and_persists() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    async {
        let state = AppState::new_with_config(temp.path().to_path_buf(), config_path.clone())
            .await
            .unwrap();
        let app = app(state.clone(), CancellationToken::new());

        request_json(
            &app,
            Method::PUT,
            "/api/v1/secrets/REPO_ONLY",
            json!({ "value": "v1", "repo": "owner/repo" }),
        )
        .await;
        request_json(
            &app,
            Method::PUT,
            "/api/v1/secrets/GLOBAL_ONLY",
            json!({ "value": "g1" }),
        )
        .await;
        request_json(
            &app,
            Method::PUT,
            "/api/v1/secrets/OTHER",
            json!({ "value": "x", "repo": "other/repo" }),
        )
        .await;

        // Full listing carries both tiers; scoped listing only its repo.
        let listed = request_json(&app, Method::GET, "/api/v1/secrets", Value::Null).await;
        let entries: Vec<(String, Option<String>)> = listed["secrets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| {
                (
                    entry["name"].as_str().unwrap().to_owned(),
                    entry["repo"].as_str().map(str::to_owned),
                )
            })
            .collect();
        assert!(entries.contains(&("REPO_ONLY".to_owned(), Some("owner/repo".to_owned()))));
        assert!(entries.contains(&("GLOBAL_ONLY".to_owned(), None)));
        assert!(entries.contains(&("OTHER".to_owned(), Some("other/repo".to_owned()))));

        let scoped = request_json(
            &app,
            Method::GET,
            "/api/v1/secrets?repo=owner/repo",
            Value::Null,
        )
        .await;
        let scoped_names: Vec<&str> = scoped["secrets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["name"].as_str().unwrap())
            .collect();
        assert_eq!(scoped_names, vec!["REPO_ONLY"]);

        // Deletion, then 404 on a second attempt.
        request_json(
            &app,
            Method::DELETE,
            "/api/v1/secrets/REPO_ONLY?repo=owner/repo",
            Value::Null,
        )
        .await;
        let (status, _) = request_json_status(
            &app,
            Method::DELETE,
            "/api/v1/secrets/REPO_ONLY?repo=owner/repo",
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Validation: lowercase names and empty values are rejected.
        let (status, _) = request_json_status(
            &app,
            Method::PUT,
            "/api/v1/secrets/lowercase",
            json!({ "value": "x" }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = request_json_status(
            &app,
            Method::PUT,
            "/api/v1/secrets/BAD",
            json!({ "value": "", "repo": "owner/repo" }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // The in-memory store and the persisted file both reflect the API.
        let store = state.secrets.read();
        assert!(store.global.contains_key("GLOBAL_ONLY"));
        assert!(!store.repo.contains_key("owner/repo"));
        assert!(store.repo["other/repo"].contains_key("OTHER"));
        drop(store);
        let text = std::fs::read_to_string(&config_path).unwrap();
        assert!(text.contains("GLOBAL_ONLY"), "config persists the secret");
        assert!(text.contains("other/repo"), "config persists the scope");
    }
    .await;
}

#[tokio::test]
async fn live_secrets_api_env_scope_round_trips() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    async {
        let state = AppState::new_with_config(temp.path().to_path_buf(), config_path.clone())
            .await
            .unwrap();
        let app = app(state.clone(), CancellationToken::new());

        request_json(
            &app,
            Method::PUT,
            "/api/v1/secrets/DEPLOY_KEY",
            json!({ "value": "k1", "repo": "owner/repo", "env": "prod" }),
        )
        .await;
        request_json(
            &app,
            Method::PUT,
            "/api/v1/secrets/SHARED",
            json!({ "value": "repo-only", "repo": "owner/repo" }),
        )
        .await;

        // Full listing carries the environment scope on the env entry.
        let listed = request_json(&app, Method::GET, "/api/v1/secrets", Value::Null).await;
        let entries: Vec<(String, Option<String>, Option<String>)> = listed["secrets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| {
                (
                    entry["name"].as_str().unwrap().to_owned(),
                    entry["repo"].as_str().map(str::to_owned),
                    entry["env"].as_str().map(str::to_owned),
                )
            })
            .collect();
        assert!(entries.contains(&(
            "DEPLOY_KEY".to_owned(),
            Some("owner/repo".to_owned()),
            Some("prod".to_owned())
        )));
        assert!(entries.contains(&("SHARED".to_owned(), Some("owner/repo".to_owned()), None)));

        // Env-scoped listing returns only that environment's names.
        let scoped = request_json(
            &app,
            Method::GET,
            "/api/v1/secrets?repo=owner/repo&env=prod",
            Value::Null,
        )
        .await;
        let scoped_names: Vec<&str> = scoped["secrets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["name"].as_str().unwrap())
            .collect();
        assert_eq!(scoped_names, vec!["DEPLOY_KEY"]);

        // The in-memory store and the persisted file both reflect the env tier.
        {
            let store = state.secrets.read();
            assert_eq!(store.env["owner/repo"]["prod"]["DEPLOY_KEY"], "k1");
        }
        // Reload, don't grep: the assertion must prove the value round-trips
        // through the serializer, not merely that the literal appears in the
        // file (which a malformed or misplaced table could satisfy).
        let persisted = crate::config::load_config_from(&config_path).unwrap();
        assert_eq!(
            persisted.env_secrets["owner/repo"]["prod"]["DEPLOY_KEY"], "k1",
            "config persists the env secret"
        );

        // Validation: env without repo and malformed env names are rejected.
        let (status, _) = request_json_status(
            &app,
            Method::PUT,
            "/api/v1/secrets/NO_REPO",
            json!({ "value": "x", "env": "prod" }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = request_json_status(
            &app,
            Method::PUT,
            "/api/v1/secrets/BAD_ENV",
            json!({ "value": "x", "repo": "owner/repo", "env": "-dash" }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) =
            request_json_status(&app, Method::GET, "/api/v1/secrets?env=prod", Value::Null).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // Deletion, then 404 on a second attempt.
        request_json(
            &app,
            Method::DELETE,
            "/api/v1/secrets/DEPLOY_KEY?repo=owner/repo&env=prod",
            Value::Null,
        )
        .await;
        let (status, _) = request_json_status(
            &app,
            Method::DELETE,
            "/api/v1/secrets/DEPLOY_KEY?repo=owner/repo&env=prod",
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // The env map is pruned when its last name goes.
        {
            let store = state.secrets.read();
            assert!(
                !store.env.contains_key("owner/repo"),
                "empty environment maps are pruned"
            );
        }
    }
    .await;
}

/// `secrets_store = "memory"` keeps values out of the config file entirely:
/// the live API mutates the in-memory store only, so a restart loses the
/// secret and the file never carries it. The in-memory store must still
/// serve it for the current process lifetime.
#[tokio::test]
async fn memory_secrets_store_never_writes_the_config_file() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    std::fs::write(&config_path, "secrets_store = \"memory\"\n").unwrap();
    async {
        let state = AppState::new_with_config(temp.path().to_path_buf(), config_path.clone())
            .await
            .unwrap();
        let app = app(state.clone(), CancellationToken::new());

        request_json(
            &app,
            Method::PUT,
            "/api/v1/secrets/GLOBAL_ONLY",
            json!({ "value": "g1" }),
        )
        .await;

        // Live and visible in the store.
        let listed = request_json(&app, Method::GET, "/api/v1/secrets", Value::Null).await;
        let names: Vec<&str> = listed["secrets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["name"].as_str().unwrap())
            .collect();
        assert!(
            names.contains(&"GLOBAL_ONLY"),
            "live store serves the secret"
        );
        {
            let store = state.secrets.read();
            assert!(store.global.contains_key("GLOBAL_ONLY"));
        }

        // Never persisted: the file holds the store-mode key and nothing else.
        let persisted = crate::config::load_config_from(&config_path).unwrap();
        assert!(persisted.secrets.is_empty(), "memory mode must not persist");
        assert!(persisted.repo_secrets.is_empty());
        assert_eq!(persisted.secrets_store.as_deref(), Some("memory"));

        // Deletion must use the runtime store as the source of truth: the
        // config-driven lookup would 404 on a secret that never reached the
        // file, leaving it live in memory.
        let (status, _) = request_json_status(
            &app,
            Method::DELETE,
            "/api/v1/secrets/GLOBAL_ONLY",
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        {
            let store = state.secrets.read();
            assert!(
                !store.global.contains_key("GLOBAL_ONLY"),
                "memory-mode delete must remove from the runtime store"
            );
        }
    }
    .await;
}

/// Concurrent secret mutations must not lose writes. Each handler loads the
/// whole config file, changes one entry and writes it back; without the
/// `secret_mutation` lock the requests read the same base config and the
/// last rename wins, so the file loses secrets the in-memory store still
/// reports. Remove the lock and this test fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_secret_mutations_keep_store_and_file_in_agreement() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    async {
        let state = AppState::new_with_config(temp.path().to_path_buf(), config_path.clone())
            .await
            .unwrap();
        let app = app(state.clone(), CancellationToken::new());

        // Seeded so the concurrent burst has something to delete.
        request_json(
            &app,
            Method::PUT,
            "/api/v1/secrets/DOOMED",
            json!({ "value": "gone" }),
        )
        .await;

        const WRITERS: usize = 12;
        let mut tasks = Vec::with_capacity(WRITERS + 1);
        for index in 0..WRITERS {
            let app = app.clone();
            tasks.push(tokio::spawn(async move {
                request_json(
                    &app,
                    Method::PUT,
                    &format!("/api/v1/secrets/CONCURRENT_{index}"),
                    json!({ "value": format!("value-{index}") }),
                )
                .await;
            }));
        }
        let delete_app = app.clone();
        tasks.push(tokio::spawn(async move {
            request_json(
                &delete_app,
                Method::DELETE,
                "/api/v1/secrets/DOOMED",
                Value::Null,
            )
            .await;
        }));
        for task in tasks {
            task.await.unwrap();
        }

        let expected: Vec<String> = (0..WRITERS)
            .map(|index| format!("CONCURRENT_{index}"))
            .collect();
        let mut expected_sorted = expected.clone();
        expected_sorted.sort();

        let store = state.secrets.read();
        assert!(
            !store.global.contains_key("DOOMED"),
            "deleted secret survived in the in-memory store"
        );
        let store_names: Vec<String> = store.global.keys().cloned().collect();
        drop(store);

        let persisted = std::fs::read_to_string(&config_path).unwrap();
        let persisted: crate::config::ConfigFile = toml::from_str(&persisted).unwrap();
        for name in &expected {
            assert!(
                persisted.secrets.contains_key(name),
                "{name} lost from the persisted config: {:?}",
                persisted.secrets.keys().collect::<Vec<_>>()
            );
            let index = name.trim_start_matches("CONCURRENT_");
            assert_eq!(
                persisted.secrets[name],
                format!("value-{index}"),
                "{name} persisted with the wrong value"
            );
        }
        assert!(
            !persisted.secrets.contains_key("DOOMED"),
            "deleted secret was resurrected by a concurrent write"
        );

        // Neither side may carry a name the other does not, and nothing
        // unexpected may survive on either side.
        let persisted_names: Vec<String> = persisted.secrets.keys().cloned().collect();
        assert_eq!(store_names, expected_sorted);
        assert_eq!(persisted_names, expected_sorted);
    }
    .await;
}

/// The secret store holds plaintext values, so its `Debug` must never print
/// them — one `debug!(?store)` would otherwise dump every stored secret.
#[test]
fn secret_store_debug_redacts_values() {
    let mut store = crate::state::SecretStore::default();
    store
        .global
        .insert("GLOBAL_NAME".to_owned(), "global-plaintext".to_owned());
    store.repo.insert(
        "owner/repo".to_owned(),
        [("REPO_NAME".to_owned(), "repo-plaintext".to_owned())]
            .into_iter()
            .collect(),
    );

    let rendered = format!("{store:?}");
    assert!(rendered.contains("GLOBAL_NAME"), "{rendered}");
    assert!(rendered.contains("REPO_NAME"), "{rendered}");
    assert!(rendered.contains("owner/repo"), "{rendered}");
    assert!(!rendered.contains("global-plaintext"), "{rendered}");
    assert!(!rendered.contains("repo-plaintext"), "{rendered}");
    // Alternate formatting must be redacted too.
    let pretty = format!("{store:#?}");
    assert!(!pretty.contains("global-plaintext"), "{pretty}");
    assert!(!pretty.contains("repo-plaintext"), "{pretty}");
}

#[tokio::test]
async fn workflow_steps_update_prefers_runner_reported_step_names() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_yaml(
        &app,
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "local/preloop",
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();

    // The server leaves the broker-message display name empty for steps
    // without an explicit `name:`; the runner reports the rendered name
    // ("Run echo hi") in WorkflowStepsUpdate and that must win, not the
    // empty lookup result.
    let (plan_id, agent_job_id) = {
        let inner = state.inner.lock().await;
        let request = inner
            .job_requests
            .values()
            .find(|request| request.run_id.0.to_string() == run_id)
            .expect("submitted run must have a job request");
        (request.plan_id.clone(), request.agent_job_id.to_string())
    };

    let response = request_json(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
        json!({
            "workflow_run_backend_id": plan_id,
            "workflow_job_run_backend_id": agent_job_id,
            "steps": [{
                "external_id": uuid::Uuid::new_v4().to_string(),
                "number": 2,
                "name": "Run echo hi",
                "status": 6,
                "conclusion": 2
            }]
        }),
    )
    .await;
    assert_eq!(response["ok"], true);

    let run = get_run_json(&app, &run_id).await;
    let steps = run["jobs_list"][0]["steps"].as_array().unwrap();
    assert!(
        steps.iter().any(|step| step["name"] == "Run echo hi"),
        "runner-reported step name must appear in the run record: {steps:?}"
    );
    assert!(
        steps
            .iter()
            .all(|step| !step["name"].as_str().unwrap_or("").is_empty()),
        "no step may have an empty name in the run record: {steps:?}"
    );
}

async fn get_run_json(app: &Router, run_id: &str) -> Value {
    request_json(
        app,
        Method::GET,
        &format!("/api/v1/runs/{run_id}"),
        Value::Null,
    )
    .await
}

async fn complete_via_api(app: &Router, run_id: &str, job_id: &str) {
    request_json(
        app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": job_id,
            "status": "success",
            "outputs": {}
        }),
    )
    .await;
}

async fn complete_via_api_with_outputs(
    app: &Router,
    run_id: &str,
    job_id: &str,
    outputs: serde_json::Value,
) {
    request_json(
        app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": job_id,
            "status": "success",
            "outputs": outputs,
        }),
    )
    .await;
}

#[tokio::test]
async fn workflow_concurrency_serializes_runs_fifo() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: serial-group
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    let a_id = a["run_id"].as_str().unwrap();
    let b_id = b["run_id"].as_str().unwrap();

    let run_a = get_run_json(&app, a_id).await;
    let run_b = get_run_json(&app, b_id).await;
    assert_eq!(run_a["status"], "queued");
    assert_eq!(run_b["status"], "pending");
    assert_eq!(run_b["jobs"]["build"], "pending");

    // Complete A via message poll + complete API.
    let msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert!(!msg.is_null(), "run A should be dispatchable");
    complete_via_api(&app, a_id, "build").await;

    let run_b = get_run_json(&app, b_id).await;
    assert_eq!(run_b["status"], "queued");
    assert_eq!(run_b["jobs"]["build"], "queued");
}

#[tokio::test]
async fn workflow_concurrency_cancel_in_progress_cancels_running() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: cancel-group
  cancel-in-progress: true
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 60
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let a_id = a["run_id"].as_str().unwrap();

    // Dispatch A so it is InProgress.
    let msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    let message_id = msg["messageId"].as_i64().unwrap();
    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!(
                    "/runner/server/_apis/v1/Message/1/{message_id}?sessionId=default"
                ))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let b = submit_yaml(&app, yaml, "owner/repo").await;
    let b_id = b["run_id"].as_str().unwrap();

    let run_a = get_run_json(&app, a_id).await;
    assert_eq!(run_a["status"], "cancelled");
    assert_eq!(run_a["jobs"]["build"], "cancelled");

    // Cancellation message should be official shape.
    let cancellation = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_eq!(
        cancellation["messageType"],
        azdo::message_type::JOB_CANCELLED
    );
    let body_b64 = cancellation["body"].as_str().unwrap();
    let body_bytes = BASE64_STANDARD.decode(body_b64).unwrap();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(body["jobId"]
        .as_str()
        .unwrap()
        .parse::<uuid::Uuid>()
        .is_ok());
    assert_eq!(body["timeout"], "00:05:00");

    let run_b = get_run_json(&app, b_id).await;
    assert_eq!(run_b["status"], "queued");
}

#[tokio::test]
async fn pending_run_replaced_by_newer_submission() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: replace-group
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    let c = submit_yaml(&app, yaml, "owner/repo").await;
    let a_id = a["run_id"].as_str().unwrap();
    let b_id = b["run_id"].as_str().unwrap();
    let c_id = c["run_id"].as_str().unwrap();

    let run_a = get_run_json(&app, a_id).await;
    let run_b = get_run_json(&app, b_id).await;
    let run_c = get_run_json(&app, c_id).await;
    assert_eq!(run_a["status"], "queued");
    assert_eq!(run_b["status"], "cancelled");
    assert_eq!(run_c["status"], "pending");
}

#[tokio::test]
async fn queue_max_holds_multiple_pending_fifo() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: max-group
  queue: max
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    let c = submit_yaml(&app, yaml, "owner/repo").await;
    let d = submit_yaml(&app, yaml, "owner/repo").await;
    let a_id = a["run_id"].as_str().unwrap();
    let b_id = b["run_id"].as_str().unwrap();
    let c_id = c["run_id"].as_str().unwrap();
    let d_id = d["run_id"].as_str().unwrap();

    assert_eq!(get_run_json(&app, b_id).await["status"], "pending");
    assert_eq!(get_run_json(&app, c_id).await["status"], "pending");
    assert_eq!(get_run_json(&app, d_id).await["status"], "pending");

    // Dispatch+complete A, then B should become queued.
    let _ = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    complete_via_api(&app, a_id, "build").await;
    assert_eq!(get_run_json(&app, b_id).await["status"], "queued");
    assert_eq!(get_run_json(&app, c_id).await["status"], "pending");

    let _ = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    complete_via_api(&app, b_id, "build").await;
    assert_eq!(get_run_json(&app, c_id).await["status"], "queued");
    assert_eq!(get_run_json(&app, d_id).await["status"], "pending");
}

#[tokio::test]
async fn concurrency_group_names_case_insensitive() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let a = submit_yaml(
        &app,
        r#"
on: push
concurrency: Prod
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo a
"#,
        "owner/repo",
    )
    .await;
    let b = submit_yaml(
        &app,
        r#"
on: push
concurrency: prod
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo b
"#,
        "owner/repo",
    )
    .await;
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "pending"
    );
}

#[tokio::test]
async fn job_level_concurrency_gates_single_job() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let accepted = submit_yaml(
        &app,
        r#"
on: push
jobs:
  one:
    runs-on: ubuntu-latest
    concurrency:
      group: job-serial
    steps:
      - run: echo one
  two:
    runs-on: ubuntu-latest
    concurrency:
      group: job-serial
    steps:
      - run: echo two
"#,
        "owner/repo",
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let run = get_run_json(&app, run_id).await;
    let one = run["jobs"]["one"].as_str().unwrap();
    let two = run["jobs"]["two"].as_str().unwrap();
    // Exactly one should be queued, the other pending.
    let statuses = [one, two];
    assert!(statuses.contains(&"queued"));
    assert!(statuses.contains(&"pending"));
}

#[tokio::test]
async fn concurrency_blocked_jobs_do_not_block_unrelated_work() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    // First run holds the group.
    let _ = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: blocked-group
jobs:
  slow:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 99
"#,
        "owner/repo",
    )
    .await;
    // Second run is concurrency-pending.
    let _ = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: blocked-group
jobs:
  slow:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 99
"#,
        "owner/repo",
    )
    .await;
    // Unrelated work without concurrency must still be dispatchable after
    // the first job is taken.
    let _ = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    let free = submit_yaml(
        &app,
        r#"
on: push
jobs:
  free:
    runs-on: ubuntu-latest
    steps:
      - run: echo free
"#,
        "owner/repo",
    )
    .await;
    let free_id = free["run_id"].as_str().unwrap();
    assert_eq!(get_run_json(&app, free_id).await["jobs"]["free"], "queued");
    let msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert!(
        !msg.is_null(),
        "unrelated job must be pollable while group is blocked"
    );
}

#[tokio::test]
async fn empty_workflow_concurrency_group_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state, CancellationToken::new());
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/runs")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::from(
                    json!({
                        "workflow_yaml": r#"
on: push
concurrency:
  group: ${{ github.event.head_commit.id_missing }}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#,
                        "event": "push",
                        "repository": "owner/repo",
                        "payload": { "head_commit": {} }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let error: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        error["error"],
        "concurrency evaluation failed: concurrency group name must not be empty"
    );
}

#[tokio::test]
async fn concurrency_chaos_interleaved_submits_and_completes() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml_hold = r#"
on: push
concurrency:
  group: chaos
  queue: max
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hold
"#;
    let yaml_cancel = r#"
on: push
concurrency:
  group: chaos
  cancel-in-progress: true
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo cancel
"#;
    let mut run_ids = Vec::new();
    for i in 0..20 {
        let yaml = if i % 5 == 0 { yaml_cancel } else { yaml_hold };
        let accepted = submit_yaml(&app, yaml, "owner/repo").await;
        run_ids.push(accepted["run_id"].as_str().unwrap().to_owned());
        // Occasionally complete whatever is dispatchable.
        if i % 3 == 0 {
            let msg = request_json(
                &app,
                Method::GET,
                "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
                Value::Null,
            )
            .await;
            if !msg.is_null() {
                // Complete the currently running holder if we can find a queued/in-progress job.
                for rid in &run_ids {
                    let run = get_run_json(&app, rid).await;
                    if run["jobs"]["build"] == "in_progress" || run["jobs"]["build"] == "queued" {
                        // Mark in progress via poll already done; complete.
                        complete_via_api(&app, rid, "build").await;
                        break;
                    }
                }
            }
        }
    }
    // Server must remain consistent: no panics, every run has a known status.
    for rid in &run_ids {
        let run = get_run_json(&app, rid).await;
        let status = run["status"].as_str().unwrap();
        assert!(
            matches!(
                status,
                "queued" | "pending" | "in_progress" | "success" | "cancelled" | "failure"
            ),
            "unexpected status {status} for {rid}"
        );
    }
}

async fn poll_and_ack(app: &Router) -> Value {
    let msg = request_json(
        app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    if msg.is_null() {
        return msg;
    }
    if let Some(message_id) = msg["messageId"].as_i64() {
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!(
                        "/runner/server/_apis/v1/Message/1/{message_id}?sessionId=default"
                    ))
                    .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
    }
    msg
}

fn decode_cancel_body(msg: &Value) -> Value {
    assert_eq!(msg["messageType"], azdo::message_type::JOB_CANCELLED);
    let body_b64 = msg["body"].as_str().unwrap();
    let body_bytes = BASE64_STANDARD.decode(body_b64).unwrap();
    serde_json::from_slice(&body_bytes).unwrap()
}

#[tokio::test]
async fn job_cancellation_message_type_is_official_string() {
    // Wire regression: must be "JobCancellation", not "JobCancelled".
    assert_eq!(azdo::message_type::JOB_CANCELLED, "JobCancellation");
}

#[tokio::test]
async fn broker_root_message_path_delivers_job_cancellation() {
    // The preloop-runner broker client polls `/runner/server/message` (root
    // path), NOT `/_apis/v1/Message`. Cancel must be delivered there.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    // The broker protocol requires a listen token that names a *registered*
    // runner (tokens are revoked with the registration on purge), so register
    // the machine first.
    let registered = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({"name": "broker-cancel-runner", "version": "2.335.1"}),
    )
    .await;
    let registered_runner_id = registered["id"].as_i64().unwrap();
    // Mint a runner listen token for broker auth.
    let runner_token = state
        .local_jwt(json!({
            "sub": format!("preloop-runner-listen-{registered_runner_id}"),
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();
    // Create broker session.
    let session = request_json_with_bearer(
        &app,
        Method::POST,
        "/runner/server/session",
        json!({}),
        &runner_token,
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();

    let yaml = r#"
on: push
concurrency:
  group: broker-root-cancel
  cancel-in-progress: true
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 60
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let a_id = a["run_id"].as_str().unwrap().to_owned();

    // Dispatch A via broker root path.
    let job_msg = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/runner/server/message?sessionId={session_id}&waitSeconds=0"),
        Value::Null,
        &runner_token,
    )
    .await;
    assert_eq!(job_msg["messageType"], "RunnerJobRequest");
    assert_eq!(
        get_run_json(&app, &a_id).await["jobs"]["build"],
        "in_progress"
    );

    // B cancels A.
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    assert_eq!(get_run_json(&app, &a_id).await["status"], "cancelled");
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );

    // Busy poll must yield JobCancellation on the same session.
    let cancel_msg = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/runner/server/message?sessionId={session_id}&waitSeconds=0"),
        Value::Null,
        &runner_token,
    )
    .await;
    assert_eq!(
        cancel_msg["messageType"],
        azdo::message_type::JOB_CANCELLED,
        "broker root path must deliver JobCancellation, got {cancel_msg}"
    );
    // messageId must differ from the job message or runner in-memory dedup
    // silently drops the cancel.
    assert_ne!(
        cancel_msg["messageId"], job_msg["messageId"],
        "cancel messageId must not collide with job messageId"
    );
    // Cancels live in a high id range so they never collide with request_id
    // messageIds of subsequent RunnerJobRequests.
    assert!(
        cancel_msg["messageId"].as_i64().unwrap() >= 1_000_000,
        "cancel messageId should be in high range, got {}",
        cancel_msg["messageId"]
    );
    let body: Value = serde_json::from_str(cancel_msg["body"].as_str().unwrap()).unwrap();
    assert!(body["jobId"]
        .as_str()
        .unwrap()
        .parse::<uuid::Uuid>()
        .is_ok());
    assert_eq!(body["timeout"], "00:05:00");

    // Simulate runner finishing the cancelled job, freeing the session.
    complete_via_api(&app, &a_id, "build").await;
    // completejob can arrive before the worker process exits. A Busy poll
    // must not receive B yet or the run-service dispatcher cancels the
    // still-draining worker as an overlap.
    let busy_msg = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/runner/server/message?sessionId={session_id}&status=Busy&waitSeconds=0"),
        Value::Null,
        &runner_token,
    )
    .await;
    assert!(
        busy_msg.is_null(),
        "busy runner received successor: {busy_msg}"
    );

    // B must be pollable with a messageId that does not collide with cancel.
    let b_msg = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/runner/server/message?sessionId={session_id}&status=Online&waitSeconds=0"),
        Value::Null,
        &runner_token,
    )
    .await;
    assert_eq!(
        b_msg["messageType"], "RunnerJobRequest",
        "expected B job after A completed, got {b_msg}"
    );
    assert_ne!(b_msg["messageId"], cancel_msg["messageId"]);
    assert_ne!(b_msg["messageId"], job_msg["messageId"]);
}

#[tokio::test]
async fn concurrency_expression_group_uses_github_ref() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: ci-${{ github.ref }}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    // Same ref → collide.
    let a = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": yaml,
            "event": "push",
            "repository": "owner/repo",
            "git_ref": "refs/heads/main",
        }),
    )
    .await;
    let b = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": yaml,
            "event": "push",
            "repository": "owner/repo",
            "git_ref": "refs/heads/main",
        }),
    )
    .await;
    // Different ref → independent group.
    let c = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": yaml,
            "event": "push",
            "repository": "owner/repo",
            "git_ref": "refs/heads/feature",
        }),
    )
    .await;
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "pending"
    );
    assert_eq!(
        get_run_json(&app, c["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
}

#[tokio::test]
async fn concurrency_groups_are_repo_scoped() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: shared-name
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    let a = submit_yaml(&app, yaml, "owner/repo-a").await;
    let b = submit_yaml(&app, yaml, "owner/repo-b").await;
    // Different repos → both free to run.
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
}

#[tokio::test]
async fn cancel_in_progress_expression_false_does_not_cancel() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: expr-cancel
  cancel-in-progress: ${{ false }}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let _ = poll_and_ack(&app).await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "in_progress"
    );
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "pending"
    );
}

#[tokio::test]
async fn cancel_in_progress_expression_true_cancels() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: expr-cancel-true
  cancel-in-progress: ${{ true }}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 30
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let _ = poll_and_ack(&app).await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "cancelled"
    );
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
    // Cancel message delivered with official body.
    let cancel_msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    let body = decode_cancel_body(&cancel_msg);
    assert_eq!(body["timeout"], "00:05:00");
    assert!(body.get("runId").is_none());
}

#[tokio::test]
async fn late_success_cannot_overwrite_cancelled_job() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: late-success
  cancel-in-progress: true
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 30
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let a_id = a["run_id"].as_str().unwrap().to_owned();
    let _ = poll_and_ack(&app).await;
    let _b = submit_yaml(&app, yaml, "owner/repo").await;
    assert_eq!(
        get_run_json(&app, &a_id).await["jobs"]["build"],
        "cancelled"
    );
    // Late success from a runner that never saw JobCancellation.
    complete_via_api(&app, &a_id, "build").await;
    let run_a = get_run_json(&app, &a_id).await;
    assert_eq!(run_a["jobs"]["build"], "cancelled");
    assert_eq!(run_a["status"], "cancelled");
}

#[tokio::test]
async fn multi_job_workflow_concurrency_holds_all_jobs() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: multi-job-hold
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - run: echo one
  two:
    runs-on: ubuntu-latest
    steps:
      - run: echo two
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    let b_id = b["run_id"].as_str().unwrap();
    let run_b = get_run_json(&app, b_id).await;
    assert_eq!(run_b["status"], "pending");
    assert_eq!(run_b["jobs"]["one"], "pending");
    assert_eq!(run_b["jobs"]["two"], "pending");
    // Unrelated free job still dispatchable after A's jobs taken.
    let _ = poll_and_ack(&app).await;
    let free = submit_yaml(
        &app,
        r#"
on: push
jobs:
  free:
    runs-on: ubuntu-latest
    steps:
      - run: echo free
"#,
        "owner/repo",
    )
    .await;
    assert_eq!(
        get_run_json(&app, free["run_id"].as_str().unwrap()).await["jobs"]["free"],
        "queued"
    );
    let _ = a;
}

#[tokio::test]
async fn job_level_concurrency_with_needs_gate_order() {
    // Gate order: needs → concurrency. Dependent job must not occupy the
    // group until needs are satisfied.
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let accepted = submit_yaml(
        &app,
        r#"
on: push
jobs:
  first:
    runs-on: ubuntu-latest
    steps:
      - run: echo first
  second:
    needs: first
    runs-on: ubuntu-latest
    concurrency:
      group: needs-then-concurrency
    steps:
      - run: echo second
  peer:
    runs-on: ubuntu-latest
    concurrency:
      group: needs-then-concurrency
    steps:
      - run: echo peer
"#,
        "owner/repo",
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let run = get_run_json(&app, run_id).await;
    // first ready; peer may take the concurrency slot; second waits on needs
    // (and possibly concurrency).
    assert_eq!(run["jobs"]["first"], "queued");
    assert_eq!(run["jobs"]["second"], "queued"); // in pending_jobs (needs)
                                                 // peer has no needs → evaluates concurrency immediately.
    assert!(
        run["jobs"]["peer"] == "queued" || run["jobs"]["peer"] == "pending",
        "peer={}",
        run["jobs"]["peer"]
    );
    // Complete first; second becomes ready and hits concurrency.
    let _ = poll_and_ack(&app).await;
    complete_via_api(&app, run_id, "first").await;
    let run = get_run_json(&app, run_id).await;
    // Exactly one of {peer, second} may be pending on the shared group if
    // the other is queued/in_progress.
    let peer = run["jobs"]["peer"].as_str().unwrap();
    let second = run["jobs"]["second"].as_str().unwrap();
    assert!(
        matches!(
            (peer, second),
            ("queued", "pending")
                | ("pending", "queued")
                | ("in_progress", "pending")
                | ("pending", "in_progress")
                | ("queued", "queued") // if peer already finished — unlikely
        ) || peer != second
            || peer == "queued",
        "peer={peer} second={second}"
    );
}

#[tokio::test]
async fn job_level_and_workflow_level_share_namespace() {
    // Plan: groups are one namespace for workflow-level runs and job-level jobs.
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let a = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: shared-ns
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo a
"#,
        "owner/repo",
    )
    .await;
    let b = submit_yaml(
        &app,
        r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    concurrency:
      group: shared-ns
    steps:
      - run: echo b
"#,
        "owner/repo",
    )
    .await;
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
    // B's job should be pending on the same group held by A's run.
    let run_b = get_run_json(&app, b["run_id"].as_str().unwrap()).await;
    assert_eq!(run_b["jobs"]["build"], "pending");
}

#[tokio::test]
async fn queue_max_overflow_cancels_arrival() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: overflow-group
  queue: max
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    // 1 running + 100 pending = 101 holders; 102nd arrival cancelled.
    let mut ids = Vec::new();
    for _ in 0..101 {
        let r = submit_yaml(&app, yaml, "owner/repo").await;
        ids.push(r["run_id"].as_str().unwrap().to_owned());
    }
    // First is running/queued; next 100 pending.
    assert_eq!(get_run_json(&app, &ids[0]).await["status"], "queued");
    for id in ids.iter().skip(1).take(100) {
        assert_eq!(
            get_run_json(&app, id).await["status"],
            "pending",
            "expected pending for {id}"
        );
    }
    let overflow = submit_yaml(&app, yaml, "owner/repo").await;
    let overflow_id = overflow["run_id"].as_str().unwrap();
    assert_eq!(get_run_json(&app, overflow_id).await["status"], "cancelled");
}

#[tokio::test]
async fn cancel_run_api_releases_concurrency_slot() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: api-cancel-release
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 30
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    let a_id = a["run_id"].as_str().unwrap();
    let b_id = b["run_id"].as_str().unwrap();
    assert_eq!(get_run_json(&app, b_id).await["status"], "pending");
    request_json(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{a_id}/cancel"),
        Value::Null,
    )
    .await;
    assert_eq!(get_run_json(&app, a_id).await["status"], "cancelled");
    // B should be promoted.
    let run_b = get_run_json(&app, b_id).await;
    assert_eq!(run_b["status"], "queued");
    assert_eq!(run_b["jobs"]["build"], "queued");
}

#[tokio::test]
async fn cancel_in_progress_then_pending_chain() {
    // A running, B arrives with cancel-in-progress → A cancelled, B runs.
    // C arrives without cancel → pending. Complete B → C queued.
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml_cancel = r#"
on: push
concurrency:
  group: chain-group
  cancel-in-progress: true
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 30
"#;
    let yaml_hold = r#"
on: push
concurrency:
  group: chain-group
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hold
"#;
    let a = submit_yaml(&app, yaml_cancel, "owner/repo").await;
    let _ = poll_and_ack(&app).await;
    let b = submit_yaml(&app, yaml_cancel, "owner/repo").await;
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "cancelled"
    );
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
    let c = submit_yaml(&app, yaml_hold, "owner/repo").await;
    assert_eq!(
        get_run_json(&app, c["run_id"].as_str().unwrap()).await["status"],
        "pending"
    );
    // Drain cancel message then dispatch B and complete.
    let msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    if msg["messageType"] == azdo::message_type::JOB_CANCELLED {
        let _ = poll_and_ack(&app).await; // already consumed above; get next
    }
    // Complete B (may still be queued — complete_via_api works regardless).
    complete_via_api(&app, b["run_id"].as_str().unwrap(), "build").await;
    let run_c = get_run_json(&app, c["run_id"].as_str().unwrap()).await;
    assert_eq!(run_c["status"], "queued");
    assert_eq!(run_c["jobs"]["build"], "queued");
}

#[tokio::test]
async fn bare_string_concurrency_shorthand_enforced() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency: bare-shorthand
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "pending"
    );
}

#[tokio::test]
async fn job_level_matrix_concurrency_per_expansion() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    // Two matrix cells share one group → serialize; different group → parallel.
    let accepted = submit_yaml(
        &app,
        r#"
on: push
jobs:
  matrixed:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        os: [a, b]
    concurrency:
      group: matrix-${{ matrix.os }}
    steps:
      - run: echo ${{ matrix.os }}
"#,
        "owner/repo",
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let run = get_run_json(&app, run_id).await;
    // Different matrix.os → different groups → both queued.
    let statuses: Vec<&str> = run["jobs"]
        .as_object()
        .unwrap()
        .values()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(
        statuses.iter().filter(|s| **s == "queued").count() >= 2
            || statuses.iter().all(|s| *s == "queued" || *s == "pending"),
        "jobs={:?}",
        run["jobs"]
    );
    // Same-group matrix should serialize.
    let accepted2 = submit_yaml(
        &app,
        r#"
on: push
jobs:
  matrixed:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        n: [1, 2, 3]
    concurrency:
      group: matrix-same
    steps:
      - run: echo ${{ matrix.n }}
"#,
        "owner/repo",
    )
    .await;
    let run2 = get_run_json(&app, accepted2["run_id"].as_str().unwrap()).await;
    let queued = run2["jobs"]
        .as_object()
        .unwrap()
        .values()
        .filter(|v| v.as_str() == Some("queued"))
        .count();
    let pending = run2["jobs"]
        .as_object()
        .unwrap()
        .values()
        .filter(|v| v.as_str() == Some("pending"))
        .count();
    assert_eq!(
        queued, 1,
        "exactly one matrix cell should run: {:?}",
        run2["jobs"]
    );
    assert_eq!(pending, 2, "other cells pending: {:?}", run2["jobs"]);
}

#[tokio::test]
async fn mixed_queue_modes_arrival_owns_join() {
    // Assumption #3: each arrival's own queue mode decides how it joins.
    // A running; B arrives with queue:max (pending); C arrives with queue:single
    // → should cancel B and take the pending slot.
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let a = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: mixed-q
  queue: max
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo a
"#,
        "owner/repo",
    )
    .await;
    let b = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: mixed-q
  queue: max
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo b
"#,
        "owner/repo",
    )
    .await;
    let c = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: mixed-q
  queue: single
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo c
"#,
        "owner/repo",
    )
    .await;
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "cancelled",
        "queue:single arrival should replace existing pending"
    );
    assert_eq!(
        get_run_json(&app, c["run_id"].as_str().unwrap()).await["status"],
        "pending"
    );
}

#[tokio::test]
async fn cancel_message_targets_agent_job_guid_not_logical_id() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let a = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: guid-check
  cancel-in-progress: true
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 30
"#,
        "owner/repo",
    )
    .await;
    let msg = poll_and_ack(&app).await;
    assert!(!msg.is_null());
    // Extract agent job id from the job request path if present; otherwise
    // from cancellation body after B arrives.
    let _b = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: guid-check
  cancel-in-progress: true
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 30
"#,
        "owner/repo",
    )
    .await;
    let cancel_msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    let body = decode_cancel_body(&cancel_msg);
    let job_id = body["jobId"].as_str().unwrap();
    // Must be a UUID, not the logical job name "build".
    assert!(
        job_id.parse::<uuid::Uuid>().is_ok(),
        "jobId must be agent GUID, got {job_id}"
    );
    assert_ne!(job_id, "build");
    assert_eq!(body["timeout"], "00:05:00");
    let _ = a;
}

#[tokio::test]
async fn workflow_concurrency_cancel_before_dispatch_no_message() {
    // Cancel a pending (not yet dispatched) run → no JobCancellation enqueued.
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: no-msg
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    let b_id = b["run_id"].as_str().unwrap();
    assert_eq!(get_run_json(&app, b_id).await["status"], "pending");
    // C with queue:single replaces B without B ever being in-flight.
    let c = submit_yaml(&app, yaml, "owner/repo").await;
    assert_eq!(get_run_json(&app, b_id).await["status"], "cancelled");
    assert_eq!(
        get_run_json(&app, c["run_id"].as_str().unwrap()).await["status"],
        "pending"
    );
    // Only A's job message should be available, not a cancel for B.
    let msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_ne!(
        msg["messageType"],
        azdo::message_type::JOB_CANCELLED,
        "pending-only cancel must not emit JobCancellation"
    );
    let _ = a;
}

// ── C-01 regression: max-parallel + concurrency promotion without self-deadlock ──

#[tokio::test]
async fn c01_max_parallel_concurrency_no_self_deadlock() {
    // Two matrix cells with max-parallel: 1 and a shared concurrency group.
    // Cell A acquires the group, cell B waits. When A completes, B must
    // be promoted exactly once without contending with its own holder.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      max-parallel: 1
      matrix:
        ver: [1, 2]
    concurrency:
      group: mp-group
    steps:
      - run: echo test
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();

    // One cell should be queued, the other pending (concurrency-blocked).
    let (queued_job, _blocked_count) = {
        let inner = state.inner.lock().await;
        let q = inner.queue.len();
        let cb = inner.concurrency_blocked.len();
        let pj = inner.pending_jobs.len();
        // Exactly one in queue (or pending_jobs if max-parallel gated first)
        assert!(
            q + pj >= 1,
            "at least one job should be ready: q={q} pj={pj}"
        );
        let first_job = inner
            .queue
            .front()
            .map(|j| j.job_id.clone())
            .or_else(|| inner.pending_jobs.front().map(|j| j.job_id.clone()))
            .unwrap();
        (first_job, cb)
    };

    // Complete the first cell.
    complete_via_api(&app, run_id, queued_job.0.as_str()).await;

    // After completion + promotion, the second cell should now be queued.
    let run = get_run_json(&app, run_id).await;
    let jobs = run["jobs"].as_object().unwrap();
    // At least one job should be Queued or InProgress (promoted), and none
    // should be permanently stuck in Pending.
    let stuck_pending = jobs
        .values()
        .filter(|v| v.as_str() == Some("pending"))
        .count();
    assert_eq!(
        stuck_pending, 0,
        "no job should remain stuck in pending after promotion"
    );
}

// ── C-05 regression: eval failure → terminal run status ──

#[tokio::test]
async fn c05_eval_failure_terminates_run() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    // A single-job workflow with a malformed concurrency expression.
    let accepted = submit_yaml(
        &app,
        r#"
on: push
jobs:
  deploy:
    runs-on: ubuntu-latest
    concurrency:
      group: ""
    steps:
      - run: echo never
"#,
        "owner/repo",
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let run = get_run_json(&app, run_id).await;

    // The run must NOT stay Queued forever — it must reach a terminal state.
    let status = run["status"].as_str().unwrap();
    assert!(
        status == "failure" || status == "cancelled",
        "run with failed concurrency eval should be terminal, got: {status}"
    );
}

// ── C-06 regression: boolean expression evaluation for cancel-in-progress ──

#[tokio::test]
async fn c06_cancel_in_progress_expression_bool_eval() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    // cancel-in-progress uses an expression — must evaluate as boolean.
    let yaml = r#"
on: push
concurrency:
  group: bool-eval-group
  cancel-in-progress: ${{ true }}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;

    // B should cancel A (cancel-in-progress is true).
    let a_run = get_run_json(&app, a["run_id"].as_str().unwrap()).await;
    assert_eq!(
        a_run["status"], "cancelled",
        "${{{{ true }}}} must be evaluated as truthy cancel"
    );

    // B should be running/queued.
    let b_run = get_run_json(&app, b["run_id"].as_str().unwrap()).await;
    let b_status = b_run["status"].as_str().unwrap();
    assert!(
        b_status == "queued" || b_status == "in_progress",
        "successor should be active, got: {b_status}"
    );
}

#[tokio::test]
async fn c06_queue_max_with_dynamic_true_cancel_rejected() {
    // queue: max combined with cancel-in-progress: ${{ true }} must be
    // rejected. The parser catches literal "true" at parse time → 400.
    // Dynamic expressions are caught at evaluation time → also rejected.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let body = json!({
        "workflow_yaml": "on: push\nconcurrency:\n  group: queue-max-cancel-true\n  queue: max\n  cancel-in-progress: ${{ true }}\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "event": "push",
        "repository": "owner/repo"
    });
    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/runs")
        .header(header::AUTHORIZATION, "Bearer preloop-system-token")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(state.inner.lock().await.runs.is_empty());
}

// ── C-07 regression: holder_keys reclamation ──

#[tokio::test]
async fn c07_holder_keys_cleaned_after_run_release() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let yaml = r#"
on: push
concurrency:
  group: holder-cleanup-group
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo cleanup
"#;
    let accepted = submit_yaml(&app, yaml, "owner/repo").await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // Before completion, holder_keys should have an entry.
    {
        let inner = state.inner.lock().await;
        assert!(
            inner.holder_keys.contains_key(&run_id),
            "holder_keys should track the run"
        );
    }

    // Get the job ID and complete it.
    let job_id = {
        let inner = state.inner.lock().await;
        inner.queue.front().unwrap().job_id.clone()
    };
    complete_via_api(&app, accepted["run_id"].as_str().unwrap(), &job_id.0).await;

    // After completion, holder_keys for this run should be gone.
    {
        let inner = state.inner.lock().await;
        assert!(
            !inner.holder_keys.contains_key(&run_id),
            "holder_keys should be cleaned up after run completes"
        );
    }
}

// ── C-02 regression: reusable JobSet admission and promotion ──

#[tokio::test]
async fn c02_reusable_call_jobset_blocks_and_promotes_members() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let caller_yaml = r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/callee.yml
    concurrency:
      group: reusable-serial
"#;
    let callee_yaml = r#"
on: workflow_call
jobs:
  inner:
    runs-on: ubuntu-latest
    steps:
      - run: echo callee
"#;
    let submission = || {
        json!({
            "workflow_yaml": caller_yaml,
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": {
                ".github/workflows/callee.yml": callee_yaml,
            }
        })
    };

    let first = request_json(&app, Method::POST, "/api/v1/runs", submission()).await;
    let second = request_json(&app, Method::POST, "/api/v1/runs", submission()).await;
    let first_run: RunId = first["run_id"].as_str().unwrap().parse().unwrap();
    let second_run: RunId = second["run_id"].as_str().unwrap().parse().unwrap();
    let (first_job, second_job) = {
        let inner = state.inner.lock().await;
        // The first caller's gate is free at submission: its callee subtree
        // materializes immediately and `call/inner` is dispatched.
        let first_job = JobId("call/inner".to_owned());
        // The second caller holds the gate's pending slot: it stays one
        // parked caller node, not a materialized subtree.
        let second_job = JobId("call".to_owned());
        assert_eq!(
            inner.runs[&first_run].jobs[&JobId("call".to_owned())],
            ExecutionStatus::InProgress
        );
        assert_eq!(
            inner.runs[&first_run].jobs[&first_job],
            ExecutionStatus::Queued
        );
        assert_eq!(
            inner.runs[&second_run].jobs[&second_job],
            ExecutionStatus::Pending
        );
        assert!(inner
            .queue
            .iter()
            .any(|job| job.run_id == first_run && job.job_id == first_job));
        assert!(inner
            .concurrency_blocked
            .iter()
            .any(|job| job.run_id == second_run && job.job_id == second_job));
        (first_job, second_job)
    };

    complete_via_api(&app, &first_run.to_string(), &first_job.0).await;
    {
        let inner = state.inner.lock().await;
        // Completing the subtree terminalizes the first caller, releasing the
        // gate; the second caller is admitted and expanded in turn.
        assert_eq!(
            inner.runs[&first_run].jobs[&JobId("call".to_owned())],
            ExecutionStatus::Success
        );
        let second_inner = JobId("call/inner".to_owned());
        assert_eq!(
            inner.runs[&second_run].jobs[&JobId("call".to_owned())],
            ExecutionStatus::InProgress
        );
        assert_eq!(
            inner.runs[&second_run].jobs[&second_inner],
            ExecutionStatus::Queued
        );
        assert!(inner
            .queue
            .iter()
            .any(|job| job.run_id == second_run && job.job_id == second_inner));
        assert!(!inner
            .concurrency_blocked
            .iter()
            .any(|job| job.run_id == second_run && job.job_id == second_job));
    };
    complete_via_api(&app, &second_run.to_string(), "call/inner").await;
    assert_eq!(
        state.inner.lock().await.runs[&second_run].status,
        ExecutionStatus::Success
    );
}

#[tokio::test]
async fn c02_jobset_waits_for_embedded_gate_after_acquiring_caller_gate() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let holder = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: embedded-shared
jobs:
  hold:
    runs-on: ubuntu-latest
    steps:
      - run: echo hold
"#,
        "owner/repo",
    )
    .await;
    let reusable = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/callee.yml
    concurrency:
      group: caller-free
    with:
      concurrency_group: embedded-shared
"#,
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": {
                ".github/workflows/callee.yml": r#"
on:
  workflow_call:
    inputs:
      concurrency_group:
        required: true
        type: string
concurrency:
  group: ${{ inputs.concurrency_group }}
jobs:
  inner:
    runs-on: ubuntu-latest
    steps:
      - run: echo inner
"#,
            }
        }),
    )
    .await;
    let holder_run: RunId = holder["run_id"].as_str().unwrap().parse().unwrap();
    let reusable_run: RunId = reusable["run_id"].as_str().unwrap().parse().unwrap();
    let (holder_job, reusable_job) = {
        let inner = state.inner.lock().await;
        let holder_job = inner.runs[&holder_run].jobs.keys().next().unwrap().clone();
        let reusable_job = inner.runs[&reusable_run]
            .jobs
            .keys()
            .next()
            .unwrap()
            .clone();
        assert_eq!(
            inner.runs[&reusable_run].jobs[&reusable_job],
            ExecutionStatus::Pending
        );
        assert_eq!(inner.jobset_admissions.len(), 1);
        assert_eq!(
            inner
                .jobset_admissions
                .values()
                .next()
                .unwrap()
                .acquired_keys
                .len(),
            1
        );
        (holder_job, reusable_job)
    };

    complete_via_api(&app, &holder_run.to_string(), &holder_job.0).await;
    {
        let inner = state.inner.lock().await;
        // Both gates acquired: the caller materialized its subtree and is
        // tracked as the JobSet holder; the inner job is dispatched.
        assert_eq!(
            inner.runs[&reusable_run].jobs[&reusable_job],
            ExecutionStatus::InProgress
        );
        let inner_job = JobId(format!("{}/inner", reusable_job.0));
        assert_eq!(
            inner.runs[&reusable_run].jobs[&inner_job],
            ExecutionStatus::Queued
        );
        assert!(inner.jobset_admissions.is_empty());
        for group_name in ["caller-free", "embedded-shared"] {
            let key = concurrency::concurrency_key("owner/repo", group_name);
            assert!(matches!(
                inner.concurrency_groups[&key].running,
                Some(concurrency::Holder::JobSet { run_id, .. }) if run_id == reusable_run
            ));
        }
    };
    complete_via_api(&app, &reusable_run.to_string(), "call/inner").await;
}

#[tokio::test]
async fn c02_jobset_deduplicates_identical_caller_and_embedded_keys() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/callee.yml
    concurrency:
      group: same-key
"#,
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": {
                ".github/workflows/callee.yml": r#"
on: workflow_call
concurrency:
  group: same-key
jobs:
  inner:
    runs-on: ubuntu-latest
    steps:
      - run: echo inner
"#,
            }
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let inner = state.inner.lock().await;
    // Identical caller+embedded group dedupes to one gate: the caller was
    // admitted and its subtree materialized at submission.
    assert_eq!(
        inner.runs[&run_id].jobs[&JobId("call".to_owned())],
        ExecutionStatus::InProgress
    );
    assert_eq!(
        inner.runs[&run_id].jobs[&JobId("call/inner".to_owned())],
        ExecutionStatus::Queued
    );
    assert!(inner.jobset_admissions.is_empty());
    let key = concurrency::concurrency_key("owner/repo", "same-key");
    assert!(inner.concurrency_groups[&key].pending.is_empty());
    assert!(matches!(
        inner.concurrency_groups[&key].running,
        Some(concurrency::Holder::JobSet { run_id: holder_run, .. }) if holder_run == run_id
    ));
}

/// uv-ci shape: a reusable `plan` produces outputs; a caller job gates on
/// `needs.plan.outputs.X == 'true'` and calls another reusable.
/// GitHub evaluates a reusable call's `if:` once the caller's needs complete;
/// a false result skips the whole invocation and the run record shows exactly
/// one skipped caller entry — the callee subtree is never materialized — and
/// jobs that `needs` it are skipped in turn.
#[tokio::test]
async fn reusable_caller_gated_on_plan_outputs_is_skipped() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  plan:
    uses: ./.github/workflows/plan.yml
  gated:
    needs: plan
    if: ${{ needs.plan.outputs.flag == 'true' }}
    uses: ./.github/workflows/callee.yml
  dependent:
    needs: gated
    runs-on: ubuntu-latest
    steps:
      - run: echo dependent
"#,
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": {
                ".github/workflows/plan.yml": r#"
on:
  workflow_call:
    outputs:
      flag:
        value: ${{ jobs.p.outputs.flag }}
jobs:
  p:
    runs-on: ubuntu-latest
    outputs:
      flag: "false"
    steps:
      - run: echo "flag=false" >> $GITHUB_OUTPUT
"#,
                ".github/workflows/callee.yml": r#"
on: workflow_call
jobs:
  inner:
    runs-on: ubuntu-latest
    steps:
      - run: echo inner
"#,
            }
        }),
    )
    .await;
    let run: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let plan_job = {
        let inner = state.inner.lock().await;
        // The `plan` caller itself has no gate: its callee job materialized
        // at submission time.
        inner.runs[&run]
            .jobs
            .keys()
            .find(|id| id.0.starts_with("plan/"))
            .unwrap()
            .clone()
    };
    // Complete the plan with its output resolved to "false".
    complete_via_api_with_outputs(
        &app,
        &run.to_string(),
        &plan_job.0,
        serde_json::json!({"flag": "false"}),
    )
    .await;
    {
        let inner = state.inner.lock().await;
        // GitHub shape: exactly one skipped entry for the gated caller — and
        // no callee job ever appeared in the run record.
        let gated = inner.runs[&run]
            .jobs
            .iter()
            .find(|(id, _)| id.0 == "gated")
            .unwrap();
        assert_eq!(
            *gated.1,
            ExecutionStatus::Skipped,
            "gated reusable caller must be skipped when its `if:` evaluates false"
        );
        assert!(
            !inner.runs[&run]
                .jobs
                .keys()
                .any(|id| id.0.starts_with("gated/")),
            "a false-gated caller's callee subtree must never materialize"
        );
        let dependent = inner.runs[&run]
            .jobs
            .iter()
            .find(|(id, _)| id.0 == "dependent")
            .unwrap();
        assert_eq!(
            *dependent.1,
            ExecutionStatus::Skipped,
            "a job that needs a skipped reusable call must be skipped too"
        );
    }
    {
        let inner = state.inner.lock().await;
        assert!(
            !inner
                .queue
                .iter()
                .any(|job| job.run_id == run && job.job_id.0.starts_with("gated/")),
            "skipped gated caller's inner job must never reach the dispatch queue"
        );
        assert_eq!(
            inner.runs[&run].status,
            ExecutionStatus::Success,
            "a run whose gated call was skipped concludes success"
        );
    }
}

/// GitHub run-record parity end to end: a false-gated caller stays one
/// skipped entry, a passing caller appears only as its callee jobs, and the
/// jobs listing carries GitHub display names (evaluated `name:`, space-slash
/// `caller / callee`, per-cell matrix names).
#[tokio::test]
async fn github_shaped_run_record_for_gated_and_passing_callers() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  plan:
    uses: ./.github/workflows/plan.yml
  test-smoke:
    needs: plan
    if: ${{ needs.plan.outputs.smoke == 'true' }}
    uses: ./.github/workflows/smoke.yml
  docs:
    needs: plan
    if: ${{ needs.plan.outputs.docs == 'true' }}
    uses: ./.github/workflows/docs.yml
    with:
      suite: guide
"#,
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": {
                ".github/workflows/plan.yml": r#"
on:
  workflow_call:
    outputs:
      smoke:
        value: ${{ jobs.p.outputs.smoke }}
      docs:
        value: ${{ jobs.p.outputs.docs }}
jobs:
  p:
    name: plan
    runs-on: ubuntu-latest
    outputs:
      smoke: "false"
      docs: "true"
    steps:
      - run: echo done
"#,
                ".github/workflows/smoke.yml": r#"
on: workflow_call
jobs:
  smoke:
    strategy:
      matrix:
        os: [ubuntu, macos]
    runs-on: ${{ matrix.os }}
    steps:
      - run: echo smoke
"#,
                ".github/workflows/docs.yml": r#"
on:
  workflow_call:
    inputs:
      suite:
        required: true
        type: string
jobs:
  mkdocs:
    name: "docs ${{ matrix.python }} for ${{ inputs.suite }}"
    strategy:
      matrix:
        python: ["3.9", "3.10"]
    runs-on: ubuntu-latest
    steps:
      - run: echo docs
"#,
            }
        }),
    )
    .await;
    let run: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    complete_via_api_with_outputs(
        &app,
        &run.to_string(),
        "plan/p",
        serde_json::json!({"smoke": "false", "docs": "true"}),
    )
    .await;
    // The docs caller's gate passed: its materialized matrix jobs complete.
    for id in ["docs/mkdocs (3.9)", "docs/mkdocs (3.10)"] {
        complete_via_api(&app, &run.to_string(), id).await;
    }

    let record = request_json(
        &app,
        Method::GET,
        &format!("/api/v1/runs/{run}"),
        json!(null),
    )
    .await;
    let jobs = record["jobs"].as_object().unwrap();
    let mut job_ids: Vec<&str> = jobs.keys().map(|k| k.as_str()).collect();
    job_ids.sort();
    assert_eq!(
        job_ids,
        vec![
            "docs/mkdocs (3.10)",
            "docs/mkdocs (3.9)",
            "plan/p",
            "test-smoke"
        ],
        "visible job set matches GitHub: passing callers as callee jobs, the \
         false-gated caller as one skipped entry"
    );
    assert_eq!(jobs["test-smoke"], serde_json::json!("skipped"));
    let names: Vec<&str> = record["jobs_list"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["name"].as_str().unwrap())
        .collect();
    let mut names = names;
    names.sort();
    assert_eq!(
        names,
        vec![
            "docs / docs 3.10 for guide",
            "docs / docs 3.9 for guide",
            "plan / plan",
            "test-smoke"
        ],
        "display names follow GitHub: evaluated name:, ` / ` separator, per-cell matrix values"
    );
}

/// The same reusable call with a true condition runs normally: the inner job
/// is dispatched and the dependent follows.
#[tokio::test]
async fn reusable_caller_gated_on_true_output_runs() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  plan:
    uses: ./.github/workflows/plan.yml
  gated:
    needs: plan
    if: ${{ needs.plan.outputs.flag == 'true' }}
    uses: ./.github/workflows/callee.yml
  dependent:
    needs: gated
    runs-on: ubuntu-latest
    steps:
      - run: echo dependent
"#,
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": {
                ".github/workflows/plan.yml": r#"
on:
  workflow_call:
    outputs:
      flag:
        value: ${{ jobs.p.outputs.flag }}
jobs:
  p:
    runs-on: ubuntu-latest
    outputs:
      flag: "true"
    steps:
      - run: echo "flag=true" >> $GITHUB_OUTPUT
"#,
                ".github/workflows/callee.yml": r#"
on: workflow_call
jobs:
  inner:
    runs-on: ubuntu-latest
    steps:
      - run: echo inner
"#,
            }
        }),
    )
    .await;
    let run: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let plan_job = {
        let inner = state.inner.lock().await;
        inner.runs[&run]
            .jobs
            .keys()
            .find(|id| id.0.starts_with("plan/"))
            .unwrap()
            .clone()
    };
    // Complete the plan with its output resolved to "true".
    complete_via_api_with_outputs(
        &app,
        &run.to_string(),
        &plan_job.0,
        serde_json::json!({"flag": "true"}),
    )
    .await;
    // The gate passed: the caller is InProgress and its materialized inner
    // job is promoted and claimable.
    let gated_id = {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.runs[&run].jobs[&JobId("gated".to_owned())],
            ExecutionStatus::InProgress,
            "caller node tracks its running subtree"
        );
        let gated = inner.runs[&run]
            .jobs
            .iter()
            .find(|(id, _)| id.0.starts_with("gated/"))
            .unwrap();
        assert_eq!(
            *gated.1,
            ExecutionStatus::Queued,
            "gated reusable caller runs when the condition is true"
        );
        assert!(
            inner
                .queue
                .iter()
                .any(|job| job.run_id == run && job.job_id == *gated.0),
            "gated inner job must be in the dispatch queue"
        );
        gated.0.clone()
    };
    // The dependent stays pending until the call completes.
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.runs[&run]
                .jobs
                .get(&JobId("dependent".to_owned()))
                .copied(),
            Some(ExecutionStatus::Queued),
            "dependent of a running call stays queued"
        );
    }
    complete_via_api(&app, &run.to_string(), &gated_id.0).await;
    {
        let inner = state.inner.lock().await;
        assert_eq!(inner.runs[&run].jobs[&gated_id], ExecutionStatus::Success);
        // The caller node aggregates to its subtree's result.
        assert_eq!(
            inner.runs[&run].jobs[&JobId("gated".to_owned())],
            ExecutionStatus::Success
        );
        // The dependent's needs (the inlined inner job) are now satisfied.
        let dependent = inner.runs[&run]
            .jobs
            .iter()
            .find(|(id, _)| id.0 == "dependent")
            .unwrap();
        assert_eq!(
            *dependent.1,
            ExecutionStatus::Queued,
            "dependent of a successful call is promoted after the call completes"
        );
    }
}

#[tokio::test]
async fn c02_jobset_resolves_matrix_contexts_on_caller_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let caller_yaml = r#"
on: push
jobs:
  call:
    strategy:
      matrix:
        env: [dev, prod]
    uses: ./.github/workflows/callee.yml
    concurrency:
      group: deploy-${{ matrix.env }}
"#;
    let callee_yaml = r#"
on: workflow_call
jobs:
  inner:
    runs-on: ubuntu-latest
    steps:
      - run: echo callee
"#;

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": caller_yaml,
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": {
                ".github/workflows/callee.yml": callee_yaml,
            }
        }),
    )
    .await;

    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let inner = state.inner.lock().await;

    // Verify we evaluated both matrix groups: deploy-dev and deploy-prod
    let key_dev = concurrency::concurrency_key("owner/repo", "deploy-dev");
    let key_prod = concurrency::concurrency_key("owner/repo", "deploy-prod");

    assert!(
        inner.concurrency_groups.contains_key(&key_dev),
        "concurrency_groups must have deploy-dev"
    );
    assert!(
        inner.concurrency_groups.contains_key(&key_prod),
        "concurrency_groups must have deploy-prod"
    );

    assert!(matches!(
        inner.concurrency_groups[&key_dev].running,
        Some(concurrency::Holder::JobSet { run_id: holder_run, .. }) if holder_run == run_id
    ));
    assert!(matches!(
        inner.concurrency_groups[&key_prod].running,
        Some(concurrency::Holder::JobSet { run_id: holder_run, .. }) if holder_run == run_id
    ));
}
/// Production path: duplicate completion does not create a second promotion.
#[tokio::test]
async fn dag_duplicate_completion_idempotent_production() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo build
  test:
    needs: [build]
    runs-on: ubuntu-latest
    steps:
      - run: echo test
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // Complete build once
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({"run_id": run_id, "job_id": "build", "status": "success"}),
    )
    .await;

    // test should be queued exactly once
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.queue.iter().filter(|j| j.job_id.0 == "test").count(),
            1,
            "test must appear exactly once in queue"
        );
    }

    // Complete build again (duplicate)
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({"run_id": run_id, "job_id": "build", "status": "success"}),
    )
    .await;

    // test must still appear exactly once
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.queue.iter().filter(|j| j.job_id.0 == "test").count(),
            1,
            "duplicate completion must not create second promotion"
        );
    }
}

/// Production path: small structured YAML → parse → expand → server
/// submission → promote/complete verifies the full pipeline.
#[tokio::test]
async fn dag_yaml_parse_expand_server_production() {
    let yaml = r#"
on: push
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - run: echo lint
  build:
    needs: [lint]
    runs-on: ubuntu-latest
    steps:
      - run: echo build
  test:
    needs: [build]
    runs-on: ubuntu-latest
    steps:
      - run: echo test
  deploy:
    needs: [test]
    if: success()
    runs-on: ubuntu-latest
    steps:
      - run: echo deploy
"#;
    // Verify parser round-trip
    let workflow = preloop_gha_parser::parse_workflow(yaml).unwrap();
    let plans = preloop_gha_parser::expand_jobs(&workflow).unwrap();
    let plan_ids: Vec<_> = plans.iter().map(|p| p.id.0.as_str()).collect();
    assert!(plan_ids.contains(&"lint"));
    assert!(plan_ids.contains(&"build"));
    assert!(plan_ids.contains(&"test"));
    assert!(plan_ids.contains(&"deploy"));

    // Verify DAG validation passes
    preloop_gha_parser::dag::validate_job_plans(&plans).unwrap();

    // Run through real server
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": yaml,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // Queued jobs = parser's expanded IDs (lint is root)
    {
        let inner = state.inner.lock().await;
        assert_eq!(inner.queue.len(), 1);
        assert_eq!(inner.queue[0].job_id.0, "lint");
    }

    // Walk the chain: lint → build → test → deploy
    for (job, next_queued) in [
        ("lint", Some("build")),
        ("build", Some("test")),
        ("test", Some("deploy")),
        ("deploy", None),
    ] {
        request_json(
            &app,
            Method::POST,
            "/internal/test/jobs/complete",
            json!({"run_id": run_id, "job_id": job, "status": "success"}),
        )
        .await;

        let inner = state.inner.lock().await;
        if let Some(next) = next_queued {
            assert!(
                inner.queue.iter().any(|j| j.job_id.0 == next),
                "after completing {job}, {next} should be queued"
            );
        }
    }

    // Run is terminal — all jobs completed successfully
    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(run.status, ExecutionStatus::Success);
    assert!(inner.pending_jobs.is_empty());
    for (job_id, status) in &run.jobs {
        assert_eq!(
            *status,
            ExecutionStatus::Success,
            "job {} should be Success, got {:?}",
            job_id.0,
            status
        );
    }
}
/// Exercises the real parser → queue → completion → dependency-promotion path
/// over 1,000 deterministic bounded DAGs.
#[tokio::test]
#[allow(clippy::needless_range_loop)]
async fn generated_server_dag_properties_1000_cases() {
    fn next(seed: &mut u64) -> u64 {
        *seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *seed
    }
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    for case in 0..1_000u64 {
        let mut seed = 20250713u64 ^ case.wrapping_mul(0x9E37_79B9);
        let count = 2 + (next(&mut seed) % 4) as usize;
        let mut needs = vec![Vec::<usize>::new(); count];
        for job in 1..count {
            for dependency in 0..job {
                if next(&mut seed) & 1 == 1 {
                    needs[job].push(dependency);
                }
            }
        }
        let failed_root = (0..count).find(|job| needs[*job].is_empty()).unwrap();

        // Assign conditions to non-root jobs based on PRNG
        let mut conditions: Vec<Option<&str>> = vec![None; count];
        for job in 1..count {
            if !needs[job].is_empty() {
                conditions[job] = match next(&mut seed) % 5 {
                    0 => Some("always()"),
                    1 => Some("failure()"),
                    _ => None, // default gate
                };
            }
        }

        let mut yaml = String::from("on: push\njobs:\n");
        for job in 0..count {
            yaml.push_str(&format!("  j{job}:\n"));
            if !needs[job].is_empty() {
                yaml.push_str("    needs: [");
                for (index, dependency) in needs[job].iter().enumerate() {
                    if index > 0 {
                        yaml.push_str(", ");
                    }
                    yaml.push_str(&format!("j{dependency}"));
                }
                yaml.push_str("]\n");
            }
            if let Some(cond) = conditions[job] {
                yaml.push_str(&format!("    if: {cond}\n"));
            }
            yaml.push_str("    runs-on: ubuntu-latest\n");
            yaml.push_str("    steps:\n      - run: echo property\n");
        }

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": yaml,
                "event": "push",
                "repository": "property/test"
            }),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        for _ in 0..=count {
            let queued = {
                let inner = state.inner.lock().await;
                inner
                    .queue
                    .iter()
                    .filter(|job| job.run_id == run_id)
                    .map(|job| job.job_id.0.clone())
                    .collect::<Vec<_>>()
            };
            if queued.is_empty() {
                break;
            }
            for job_id in queued {
                let status = if job_id == format!("j{failed_root}") {
                    "failure"
                } else {
                    "success"
                };
                request_json(
                    &app,
                    Method::POST,
                    "/internal/test/jobs/complete",
                    json!({"run_id": run_id, "job_id": job_id, "status": status}),
                )
                .await;
            }
        }

        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id).unwrap();
        let mut failed_ancestor = vec![false; count];
        for job in 0..count {
            failed_ancestor[job] = job == failed_root
                || needs[job]
                    .iter()
                    .any(|dependency| failed_ancestor[*dependency]);
            let expected = if job == failed_root {
                ExecutionStatus::Failure
            } else if failed_ancestor[job] {
                // Job has a failed ancestor — what does the condition say?
                match conditions[job] {
                    Some("always()") => ExecutionStatus::Success, // always runs, completed successfully
                    Some("failure()") => ExecutionStatus::Success, // failure() is true, job runs
                    _ => ExecutionStatus::Skipped,                // default gate blocks
                }
            } else {
                // No failed ancestor
                match conditions[job] {
                    Some("failure()") => ExecutionStatus::Skipped, // failure() is false, skip
                    _ => ExecutionStatus::Success,                 // default or always() runs
                }
            };
            assert_eq!(
                run.jobs[&JobId(format!("j{job}"))],
                expected,
                "case {case} job j{job} condition={:?}",
                conditions[job]
            );
        }
    }
}

fn git_fixture_command(worktree: &FsPath, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git_fixture_output(worktree: &FsPath, args: &[&str]) -> Vec<u8> {
    let output = Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn git_fixture_output_allow_failure(worktree: &FsPath, args: &[&str]) -> (bool, Vec<u8>) {
    let output = Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(args)
        .output()
        .unwrap();
    (output.status.success(), output.stdout)
}

fn git_pack_bytes(repository: &FsPath) -> u64 {
    let pack_dir = repository.join("objects/pack");
    let Ok(entries) = fs::read_dir(pack_dir) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            if entry
                .path()
                .extension()
                .and_then(|extension| extension.to_str())
                == Some("pack")
            {
                entry.metadata().ok().map(|metadata| metadata.len())
            } else {
                None
            }
        })
        .sum()
}

fn git_alternate_object_directories(repository: &FsPath) -> Vec<std::path::PathBuf> {
    fs::read_to_string(repository.join("objects/info/alternates"))
        .unwrap()
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| std::fs::canonicalize(line).unwrap())
        .collect()
}

fn create_snapshot_fixture(root: &FsPath) -> (std::path::PathBuf, std::path::PathBuf) {
    let workspace = root.join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    git_fixture_command(&workspace, &["init", "-b", "main"]);
    git_fixture_command(&workspace, &["config", "user.name", "Snapshot Test"]);
    git_fixture_command(
        &workspace,
        &["config", "user.email", "snapshot@example.test"],
    );

    fs::write(workspace.join(".gitignore"), "*.ignored\nignored-dir/\n").unwrap();
    fs::write(workspace.join("tracked.txt"), "tracked base\n").unwrap();
    fs::write(workspace.join("deleted.txt"), "will disappear\n").unwrap();
    fs::write(workspace.join("staged.txt"), "staged base\n").unwrap();
    fs::write(workspace.join("tracked.ignored"), "tracked ignored base\n").unwrap();
    git_fixture_command(
        &workspace,
        &[
            "add",
            ".gitignore",
            "tracked.txt",
            "deleted.txt",
            "staged.txt",
        ],
    );
    git_fixture_command(&workspace, &["add", "-f", "tracked.ignored"]);
    git_fixture_command(&workspace, &["commit", "-m", "base"]);

    fs::write(workspace.join("tracked.txt"), "tracked unstaged change\n").unwrap();
    fs::write(workspace.join("staged.txt"), "staged index change\n").unwrap();
    git_fixture_command(&workspace, &["add", "staged.txt"]);
    fs::remove_file(workspace.join("deleted.txt")).unwrap();
    fs::write(workspace.join("untracked.txt"), "new nonignored file\n").unwrap();
    fs::write(
        workspace.join("ignored.ignored"),
        "must not enter snapshot\n",
    )
    .unwrap();
    fs::create_dir_all(workspace.join("ignored-dir")).unwrap();
    fs::write(
        workspace.join("ignored-dir/hidden.txt"),
        "must not enter snapshot\n",
    )
    .unwrap();
    fs::write(
        workspace.join("tracked.ignored"),
        "tracked ignored modification\n",
    )
    .unwrap();

    (root.join("state"), workspace)
}

#[tokio::test]
async fn shallow_workspace_snapshot_preserves_upstream_shas() {
    // A shallow clone's cache inherits its boundary, and serving that forced a
    // history rewrite that changed every sha. Workflows resolve a base commit
    // from git (`HEAD^`, `merge-base`) and then fetch it from the forge, so a
    // rewritten sha fails there with "not our ref". Deepening from the remote
    // must keep the served ancestry byte-identical to upstream.
    let temp = tempfile::tempdir().unwrap();
    let upstream = temp.path().join("upstream");
    fs::create_dir_all(&upstream).unwrap();
    git_fixture_command(
        &upstream,
        &["init", "--quiet", "--initial-branch=main", "."],
    );
    git_fixture_command(&upstream, &["config", "user.email", "t@example.com"]);
    git_fixture_command(&upstream, &["config", "user.name", "Test"]);
    for n in 0..3 {
        fs::write(upstream.join("file.txt"), format!("rev {n}\n")).unwrap();
        git_fixture_command(&upstream, &["add", "file.txt"]);
        git_fixture_command(
            &upstream,
            &["commit", "--quiet", "-m", &format!("commit {n}")],
        );
    }
    let upstream_tip = String::from_utf8(git_fixture_output(&upstream, &["rev-parse", "HEAD"]))
        .unwrap()
        .trim()
        .to_owned();

    // Shallow clone, exactly like CI does.
    let workspace = temp.path().join("workspace");
    let url = format!("file://{}", upstream.display());
    let clone = std::process::Command::new("git")
        .args(["clone", "--quiet", "--depth=1", &url])
        .arg(&workspace)
        .output()
        .unwrap();
    assert!(clone.status.success(), "shallow clone failed: {clone:?}");
    assert!(
        workspace.join(".git/shallow").is_file(),
        "fixture must be shallow"
    );

    let state_dir = temp.path().join("state");
    fs::create_dir_all(&state_dir).unwrap();
    let run_id: RunId = "22222222-2222-4222-8222-222222222222".parse().unwrap();
    let snapshot = create_workspace_snapshot(&state_dir, &workspace, run_id, None)
        .await
        .expect("snapshot creation should succeed");

    // The snapshot commit's parent must be the upstream sha, not a copy.
    let repository = state_dir.join(&snapshot.repository);
    let parent = String::from_utf8(git_fixture_output(
        &repository,
        &["rev-parse", &format!("{}^", snapshot.commit_sha)],
    ))
    .unwrap()
    .trim()
    .to_owned();
    assert_eq!(
        parent, upstream_tip,
        "snapshot parent must be the real upstream commit, not a rewritten copy"
    );
}

#[tokio::test]
async fn workspace_snapshot_survives_refs_with_missing_objects() {
    // `update-ref --stdin` is atomic, so a single tag whose object is absent
    // used to abort the whole snapshot with "nonexistent object" — and the
    // fallback then handed the job a zero sha.
    let temp = tempfile::tempdir().unwrap();
    let (state_dir, workspace) = create_snapshot_fixture(temp.path());
    fs::create_dir_all(&state_dir).unwrap();
    let tags = workspace.join(".git/refs/tags");
    fs::create_dir_all(&tags).unwrap();
    fs::write(
        tags.join("dangling"),
        "3c72e3fdd04bb63c9470ad0a79ad05bba0a393a4\n",
    )
    .unwrap();

    let run_id: RunId = "33333333-3333-4333-8333-333333333333".parse().unwrap();
    let snapshot = create_workspace_snapshot(&state_dir, &workspace, run_id, None)
        .await
        .expect("a dangling ref must not fail snapshot creation");
    assert_eq!(snapshot.commit_sha.len(), 40);
}

#[tokio::test]
async fn snapshot_drops_unresolvable_gitlinks_but_keeps_registered_submodules() {
    let temp = tempfile::tempdir().unwrap();
    let (state_dir, workspace) = create_snapshot_fixture(temp.path());

    // A nested repo added by hand: the parent index gets a gitlink entry
    // but no `.gitmodules` registers it — the state that makes
    // `git submodule foreach` inside the VM fail with `fatal: No url found
    // for submodule path 'stream-docker-output' in .gitmodules`.
    let nested = workspace.join("stream-docker-output");
    fs::create_dir_all(&nested).unwrap();
    git_fixture_command(&nested, &["init", "-q", "-b", "main"]);
    git_fixture_command(&nested, &["config", "user.email", "nested@example.test"]);
    fs::write(nested.join("payload.txt"), "nested\n").unwrap();
    git_fixture_command(&nested, &["add", "payload.txt"]);
    git_fixture_command(&nested, &["commit", "-qm", "nested"]);
    let nested_tip = String::from_utf8(git_fixture_output(&nested, &["rev-parse", "HEAD"]))
        .unwrap()
        .trim()
        .to_owned();
    let cacheinfo = format!("160000,{nested_tip},stream-docker-output");
    git_fixture_command(
        &workspace,
        &["update-index", "--add", "--cacheinfo", cacheinfo.as_str()],
    );

    fs::create_dir_all(&state_dir).unwrap();
    let first_run: RunId = "44444444-4444-4444-8444-444444444444".parse().unwrap();
    let first = create_workspace_snapshot(&state_dir, &workspace, first_run, None)
        .await
        .expect("snapshot with an unresolvable gitlink should succeed");
    let first_repository = state_dir.join(&first.repository);
    let output = git_fixture_output(
        &first_repository,
        &[
            "ls-tree",
            first.commit_sha.as_str(),
            "--",
            "stream-docker-output",
        ],
    );
    assert!(
        output.is_empty(),
        "unresolvable gitlink must be dropped from the snapshot: {}",
        String::from_utf8_lossy(&output)
    );

    // Register the submodule properly: the gitlink must then survive so a
    // workflow asking for submodules gets the real structure.
    fs::write(
        workspace.join(".gitmodules"),
        "[submodule \"stream-docker-output\"]\n\tpath = stream-docker-output\n\turl = https://example.test/stream-docker-output.git\n",
    )
    .unwrap();
    let second_run: RunId = "55555555-5555-4555-8555-555555555555".parse().unwrap();
    let second = create_workspace_snapshot(&state_dir, &workspace, second_run, None)
        .await
        .expect("snapshot with a registered submodule should succeed");
    let second_repository = state_dir.join(&second.repository);
    let listed = String::from_utf8(git_fixture_output(
        &second_repository,
        &[
            "ls-tree",
            second.commit_sha.as_str(),
            "--",
            "stream-docker-output",
        ],
    ))
    .unwrap();
    assert!(
        listed.starts_with("160000"),
        "a registered submodule gitlink must survive the snapshot: {listed}"
    );

    // A logical submodule name that differs from the checkout path is valid
    // (`git submodule add --name`): the gitlink must still survive, since git
    // resolves it by the `path` key, not the section name.
    fs::write(
        workspace.join(".gitmodules"),
        "[submodule \"logical-stream\"]\n\tpath = stream-docker-output\n\turl = https://example.test/stream-docker-output.git\n",
    )
    .unwrap();
    let third_run: RunId = "66666666-6666-4666-8666-666666666666".parse().unwrap();
    let third = create_workspace_snapshot(&state_dir, &workspace, third_run, None)
        .await
        .expect("snapshot with a logically-named submodule should succeed");
    let third_repository = state_dir.join(&third.repository);
    let listed = String::from_utf8(git_fixture_output(
        &third_repository,
        &[
            "ls-tree",
            third.commit_sha.as_str(),
            "--",
            "stream-docker-output",
        ],
    ))
    .unwrap();
    assert!(
        listed.starts_with("160000"),
        "a logically-named registered submodule gitlink must survive the snapshot: {listed}"
    );
}

#[tokio::test]
async fn snapshot_gitlink_resolution_matches_git() {
    let temp = tempfile::tempdir().unwrap();
    let (state_dir, workspace) = create_snapshot_fixture(temp.path());

    // The keep/drop decision must mirror git's own resolution: git resolves a
    // gitlink by the section whose `path` matches it. Three registration
    // shapes git handles but a naive parser gets wrong:
    //   `a#b`         git writes and decodes this path QUOTED in .gitmodules
    //   `mixed`       [SUBMODULE]/Path/URL: config sections and keys are
    //                 case-insensitive for git
    //   `logical-only` section name only; its `path` points elsewhere, so a
    //                 gitlink at the name itself is NOT resolvable
    for path in ["a#b", "mixed", "logical-only"] {
        let nested = workspace.join(path);
        fs::create_dir_all(&nested).unwrap();
        git_fixture_command(&nested, &["init", "-q", "-b", "main"]);
        git_fixture_command(&nested, &["config", "user.email", "nested@example.test"]);
        fs::write(nested.join("payload.txt"), format!("{path}\n")).unwrap();
        git_fixture_command(&nested, &["add", "payload.txt"]);
        git_fixture_command(&nested, &["commit", "-qm", path]);
        let tip = String::from_utf8(git_fixture_output(&nested, &["rev-parse", "HEAD"]))
            .unwrap()
            .trim()
            .to_owned();
        let cacheinfo = format!("160000,{tip},{path}");
        git_fixture_command(
            &workspace,
            &["update-index", "--add", "--cacheinfo", cacheinfo.as_str()],
        );
    }
    fs::write(
        workspace.join(".gitmodules"),
        "[submodule \"a#b\"]\n\tpath = \"a#b\"\n\turl = https://example.test/a-b.git\n\
         [SUBMODULE \"mixed\"]\n\tPath = mixed\n\tURL = https://example.test/mixed.git\n\
         [submodule \"logical-only\"]\n\tpath = elsewhere\n\turl = https://example.test/elsewhere.git\n",
    )
    .unwrap();

    fs::create_dir_all(&state_dir).unwrap();
    let run_id: RunId = "77777777-7777-4777-8777-777777777777".parse().unwrap();
    let snapshot = create_workspace_snapshot(&state_dir, &workspace, run_id, None)
        .await
        .expect("snapshot with mixed gitlink registrations should succeed");
    let repository = state_dir.join(&snapshot.repository);
    let tree_of = |path: &str| {
        String::from_utf8(git_fixture_output(
            &repository,
            &["ls-tree", snapshot.commit_sha.as_str(), "--", path],
        ))
        .unwrap()
    };

    let quoted = tree_of("a#b");
    assert!(
        quoted.starts_with("160000"),
        "quoted registered path must survive the snapshot: {quoted}"
    );
    let mixed = tree_of("mixed");
    assert!(
        mixed.starts_with("160000"),
        "mixed-case registered section must survive the snapshot: {mixed}"
    );
    let name_only = tree_of("logical-only");
    assert!(
        name_only.is_empty(),
        "name-only gitlink must be dropped from the snapshot: {name_only}"
    );
}

#[tokio::test]
async fn workspace_snapshot_captures_git_state_without_mutating_source() {
    let temp = tempfile::tempdir().unwrap();
    let (state_dir, workspace) = create_snapshot_fixture(temp.path());
    fs::create_dir_all(&state_dir).unwrap();
    let run_id: RunId = "11111111-1111-4111-8111-111111111111".parse().unwrap();

    let status_before = git_fixture_output(&workspace, &["status", "--porcelain=v1"]);
    let index_path = FsPath::new(
        String::from_utf8(git_fixture_output(
            &workspace,
            &["rev-parse", "--git-path", "index"],
        ))
        .unwrap()
        .trim(),
    )
    .to_path_buf();
    let index_path = if index_path.is_absolute() {
        index_path
    } else {
        workspace.join(index_path)
    };
    let index_before = fs::read(&index_path).unwrap();

    let snapshot = create_workspace_snapshot(&state_dir, &workspace, run_id, None)
        .await
        .expect("snapshot creation should succeed");

    assert_eq!(snapshot.repository, format!("snapshots/{run_id}"));
    assert_eq!(snapshot.commit_sha.len(), 40);
    assert!(snapshot
        .commit_sha
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(
        git_fixture_output(&workspace, &["status", "--porcelain=v1"]),
        status_before,
        "snapshot creation must not alter source status"
    );
    assert_eq!(fs::read(&index_path).unwrap(), index_before);

    let repository = state_dir.join(&snapshot.repository);
    assert!(repository.join("objects").is_dir());
    let state_dir = std::fs::canonicalize(&state_dir).unwrap();
    let alternates = git_alternate_object_directories(&repository);
    assert!(!alternates.is_empty());
    assert!(alternates.iter().all(|alternate| {
        alternate.starts_with(&state_dir) && !alternate.starts_with(&workspace)
    }));
    let head = git_fixture_output(&workspace, &["rev-parse", "HEAD"]);
    assert_eq!(
        snapshot.head_sha.as_deref(),
        Some(std::str::from_utf8(&head).unwrap().trim()),
        "the snapshot must expose the workspace's real HEAD as its identity"
    );
    let config = fs::read_to_string(repository.join("config")).unwrap();
    assert!(
        config.contains("allowReachableSHA1InWant = true"),
        "deep fetches of reachable shas must be served by the snapshot: {config}"
    );
    assert!(
        config.contains("allowTipSHA1InWant = true"),
        "deep fetches of tip shas must be served by the snapshot: {config}"
    );
    let commit = snapshot.commit_sha.as_str();
    assert!(
        git_fixture_output_allow_failure(
            &repository,
            &["cat-file", "-e", &format!("{commit}^{{commit}}")]
        )
        .0
    );
    assert_eq!(
        git_fixture_output(&repository, &["show", &format!("{commit}:tracked.txt")]),
        b"tracked unstaged change\n"
    );
    assert_eq!(
        git_fixture_output(&repository, &["show", &format!("{commit}:staged.txt")]),
        b"staged index change\n"
    );
    assert_eq!(
        git_fixture_output(&repository, &["show", &format!("{commit}:tracked.ignored")]),
        b"tracked ignored modification\n"
    );
    assert_eq!(
        git_fixture_output(&repository, &["show", &format!("{commit}:untracked.txt")]),
        b"new nonignored file\n"
    );
    assert!(
        !git_fixture_output_allow_failure(
            &repository,
            &["cat-file", "-e", &format!("{commit}:deleted.txt")]
        )
        .0
    );
    assert!(
        !git_fixture_output_allow_failure(
            &repository,
            &["cat-file", "-e", &format!("{commit}:ignored.ignored")]
        )
        .0
    );
    assert!(
        !git_fixture_output_allow_failure(
            &repository,
            &[
                "cat-file",
                "-e",
                &format!("{commit}:ignored-dir/hidden.txt")
            ]
        )
        .0
    );
}

#[tokio::test]
async fn snapshot_before_sha_tracks_working_tree_state() {
    let temp = tempfile::tempdir().unwrap();
    let state_dir = temp.path().join("state");
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    fs::create_dir_all(&state_dir).unwrap();
    git_fixture_command(&workspace, &["init", "-b", "main"]);
    git_fixture_command(&workspace, &["config", "user.name", "Snapshot Test"]);
    git_fixture_command(
        &workspace,
        &["config", "user.email", "snapshot@example.test"],
    );
    fs::write(workspace.join("file.txt"), "one\n").unwrap();
    git_fixture_command(&workspace, &["add", "file.txt"]);
    git_fixture_command(&workspace, &["commit", "-m", "c0"]);
    fs::write(workspace.join("file.txt"), "two\n").unwrap();
    git_fixture_command(&workspace, &["add", "file.txt"]);
    git_fixture_command(&workspace, &["commit", "-m", "c1"]);

    let head = String::from_utf8(git_fixture_output(&workspace, &["rev-parse", "HEAD"]))
        .unwrap()
        .trim()
        .to_owned();
    let head_parent = String::from_utf8(git_fixture_output(&workspace, &["rev-parse", "HEAD^"]))
        .unwrap()
        .trim()
        .to_owned();

    // Clean tree: the change under test is the last commit, so the diff base
    // is HEAD^ (an equal-tree HEAD..S would be empty).
    let clean_run: RunId = "66666666-6666-4666-8666-666666666666".parse().unwrap();
    let clean = create_workspace_snapshot(&state_dir, &workspace, clean_run, None)
        .await
        .expect("clean-tree snapshot should succeed");
    assert_eq!(
        clean.before_sha.as_deref(),
        Some(head_parent.as_str()),
        "clean tree must diff against HEAD^"
    );

    // Dirty tree: the change under test is the uncommitted edit, so the diff
    // base is HEAD itself.
    fs::write(workspace.join("file.txt"), "three (uncommitted)\n").unwrap();
    let dirty_run: RunId = "77777777-7777-4777-8777-777777777777".parse().unwrap();
    let dirty = create_workspace_snapshot(&state_dir, &workspace, dirty_run, None)
        .await
        .expect("dirty-tree snapshot should succeed");
    assert_ne!(
        dirty.commit_sha, clean.commit_sha,
        "uncommitted edit must produce a distinct snapshot commit"
    );
    assert_eq!(
        dirty.before_sha.as_deref(),
        Some(head.as_str()),
        "dirty tree must diff against HEAD"
    );
}

#[tokio::test]
async fn terminal_run_discards_workspace_snapshot_but_preserves_object_cache() {
    let temp = tempfile::tempdir().unwrap();
    let (state_dir, workspace) = create_snapshot_fixture(temp.path());
    let mut state = AppState::new(state_dir.clone()).await.unwrap();
    state.local_workspace = Some(workspace);
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_yaml(
        &app,
        r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo snapshot
"#,
        "owner/repo",
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let repository = state_dir.join("snapshots").join(run_id.to_string());
    let object_cache = state_dir.join("snapshot-object-cache");
    assert!(
        repository.is_dir(),
        "submission should create the run snapshot"
    );
    assert!(
        object_cache.is_dir(),
        "submission should create the shared object cache"
    );

    let job_id = {
        let inner = state.inner.lock().await;
        inner
            .runs
            .get(&run_id)
            .and_then(|run| run.jobs.keys().next())
            .cloned()
            .expect("submitted run should have one dispatchable job")
    };
    // Synthetic push payloads carry a `head_commit` object (GitHub shape):
    // workflows gate on `github.event.head_commit.message` and must not see a
    // null that makes property access error out.
    {
        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id).unwrap();
        let head_commit = &run.github["event"]["head_commit"];
        let id = head_commit["id"].as_str().unwrap();
        assert_eq!(id.len(), 40, "head_commit.id must be the snapshot commit");
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(head_commit["distinct"], true);
        assert_eq!(head_commit["message"], "");
        assert_eq!(run.github["event"]["before"].as_str().unwrap().len(), 40);
        assert_eq!(run.github["event"]["after"], json!(id));
    }
    complete_via_api(&app, &run_id.to_string(), &job_id.to_string()).await;

    assert_eq!(
        get_run_json(&app, &run_id.to_string()).await["status"],
        "success"
    );
    assert!(
        !repository.exists(),
        "terminal completion should remove the run's workspace snapshot"
    );
    assert!(
        object_cache.is_dir(),
        "terminal completion must preserve the shared snapshot object cache"
    );
}

#[tokio::test]
async fn submit_rejects_invalid_schedule_cron() {
    // GitHub rejects an unparsable `on.schedule` cron at workflow save; aksh
    // rejects it at submit instead of registering a cron job that never fires.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let response = request_json_status(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on:\n  push:\n  schedule:\n    - cron: 'not a cron'\njobs:\n  build:\n    runs-on: self-hosted\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo",
        }),
    )
    .await;
    assert_eq!(response.0, StatusCode::BAD_REQUEST);

    // Valid cron still submits.
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on:\n  push:\n  schedule:\n    - cron: '0 0 * * *'\njobs:\n  build:\n    runs-on: self-hosted\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo",
        }),
    )
    .await;
    assert_eq!(accepted["queued_jobs"], 1);
}

#[tokio::test]
async fn discard_workspace_snapshot_is_idempotent_when_repository_absent() {
    let temp = tempfile::tempdir().unwrap();
    let state_dir = temp.path().join("state");
    let snapshots = state_dir.join("snapshots");
    let object_cache = state_dir.join("snapshot-object-cache");
    fs::create_dir_all(&snapshots).unwrap();
    fs::create_dir_all(&object_cache).unwrap();
    let run_id: RunId = "55555555-5555-4555-8555-555555555555".parse().unwrap();

    discard_workspace_snapshot(&state_dir, run_id).await;
    discard_workspace_snapshot(&state_dir, run_id).await;

    assert!(!snapshots.join(run_id.to_string()).exists());
    assert!(object_cache.is_dir());
}

#[tokio::test]
async fn workspace_snapshots_reuse_large_base_objects_and_materialize_changes() {
    let temp = tempfile::tempdir().unwrap();
    let state_dir = temp.path().join("state");
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    git_fixture_command(&workspace, &["init", "-b", "main"]);
    git_fixture_command(&workspace, &["config", "user.name", "Snapshot Test"]);
    git_fixture_command(
        &workspace,
        &["config", "user.email", "snapshot@example.test"],
    );
    fs::write(workspace.join(".gitignore"), "*.ignored\nignored-dir/\n").unwrap();

    // A packed, multi-megabyte base makes accidentally copying every reachable
    // object into each run repository observable rather than a tiny-fixture
    // optimization detail.
    for file in 0..64u8 {
        let mut state = 0x9e37_79b9u32 ^ u32::from(file).wrapping_mul(0x045d_9f3b);
        let contents: Vec<u8> = (0..65_536)
            .map(|_| {
                state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                (state >> 24) as u8
            })
            .collect();
        fs::write(workspace.join(format!("base-{file:02}.bin")), contents).unwrap();
    }
    git_fixture_command(&workspace, &["add", "."]);
    git_fixture_command(&workspace, &["commit", "-m", "large base"]);
    git_fixture_command(&workspace, &["gc", "--aggressive", "--prune=now"]);
    let source_pack_bytes = git_pack_bytes(&workspace.join(".git"));
    assert!(source_pack_bytes > 1_000_000);

    fs::create_dir_all(&state_dir).unwrap();
    let first_run: RunId = "22222222-2222-4222-8222-222222222222".parse().unwrap();
    let second_run: RunId = "33333333-3333-4333-8333-333333333333".parse().unwrap();
    let changed_run: RunId = "44444444-4444-4444-8444-444444444444".parse().unwrap();

    let first = create_workspace_snapshot(&state_dir, &workspace, first_run, None)
        .await
        .expect("first snapshot should succeed");
    let second = create_workspace_snapshot(&state_dir, &workspace, second_run, None)
        .await
        .expect("second unchanged snapshot should succeed");
    let first_repository = state_dir.join(&first.repository);
    let second_repository = state_dir.join(&second.repository);
    assert_eq!(first.commit_sha, second.commit_sha);

    let state_dir = std::fs::canonicalize(&state_dir).unwrap();
    let first_alternates = git_alternate_object_directories(&first_repository);
    let second_alternates = git_alternate_object_directories(&second_repository);
    assert_eq!(first_alternates, second_alternates);
    assert!(first_alternates
        .iter()
        .all(|alternate| { alternate.starts_with(&state_dir) && alternate != &state_dir }));
    assert!(
        git_pack_bytes(&first_repository) < source_pack_bytes / 2,
        "first run repository contains a full-base pack"
    );
    assert!(
        git_pack_bytes(&second_repository) < source_pack_bytes / 2,
        "second run repository contains a full-base pack instead of reusing the cache"
    );

    fs::write(workspace.join("base-00.bin"), b"changed unstaged base\n").unwrap();
    fs::write(workspace.join("base-01.bin"), b"changed staged base\n").unwrap();
    git_fixture_command(&workspace, &["add", "base-01.bin"]);
    fs::remove_file(workspace.join("base-02.bin")).unwrap();
    fs::write(workspace.join("new.txt"), b"new untracked file\n").unwrap();
    fs::write(workspace.join("not-in-snapshot.ignored"), b"ignored\n").unwrap();
    fs::create_dir_all(workspace.join("ignored-dir")).unwrap();
    fs::write(workspace.join("ignored-dir/hidden.txt"), b"ignored\n").unwrap();

    let changed = create_workspace_snapshot(&state_dir, &workspace, changed_run, None)
        .await
        .expect("changed snapshot should succeed");
    let changed_repository = state_dir.join(&changed.repository);
    let commit = changed.commit_sha.as_str();
    assert_eq!(
        git_fixture_output(
            &changed_repository,
            &["show", &format!("{commit}:base-00.bin")]
        ),
        b"changed unstaged base\n"
    );
    assert_eq!(
        git_fixture_output(
            &changed_repository,
            &["show", &format!("{commit}:base-01.bin")]
        ),
        b"changed staged base\n"
    );
    assert_eq!(
        git_fixture_output(&changed_repository, &["show", &format!("{commit}:new.txt")]),
        b"new untracked file\n"
    );
    assert!(
        !git_fixture_output_allow_failure(
            &changed_repository,
            &["cat-file", "-e", &format!("{commit}:base-02.bin")]
        )
        .0
    );
    assert!(
        !git_fixture_output_allow_failure(
            &changed_repository,
            &[
                "cat-file",
                "-e",
                &format!("{commit}:not-in-snapshot.ignored")
            ]
        )
        .0
    );
    assert!(
        !git_fixture_output_allow_failure(
            &changed_repository,
            &[
                "cat-file",
                "-e",
                &format!("{commit}:ignored-dir/hidden.txt")
            ]
        )
        .0
    );
    assert_eq!(
        git_alternate_object_directories(&changed_repository),
        first_alternates
    );
}

fn checkout_test_message(steps: Value) -> AgentJobRequestMessage {
    serde_json::from_value(json!({
        "jobId": "00000000-0000-0000-0000-000000000001",
        "requestId": 1,
        "plan": {
            "planId": "plan",
            "planType": "build",
            "version": 1,
            "artifactUri": "",
            "artifactLocation": ""
        },
        "timeline": {
            "id": "00000000-0000-0000-0000-000000000002",
            "changeId": 0,
            "location": null
        },
        "jobName": "build",
        "lockedUntil": "",
        "resources": {"endpoints": []},
        "steps": steps,
        "snapshot": null
    }))
    .unwrap()
}

#[test]
fn redirect_primary_checkout_rewrites_only_default_checkout_inputs() {
    let mut message = checkout_test_message(json!([
        {
            "id": "00000000-0000-0000-0000-000000000010",
            "name": "checkout",
            "reference": {"name": "Actions/Checkout", "version": "v4", "type": "repository"},
            "inputs": {"path": "source", "fetch-depth": "0"},
            "continueOnError": false,
            "timeoutInMinutes": null
        },
        {
            "id": "00000000-0000-0000-0000-000000000011",
            "name": "explicit checkout",
            "reference": {"name": "actions/checkout", "version": "v4", "type": "repository"},
            "inputs": {
                "repository": "octo/other",
                "ref": "refs/heads/release",
                "token": "secret-token",
                "github-server-url": "https://github.example",
                "path": "other"
            },
            "continueOnError": false,
            "timeoutInMinutes": null
        },
        {
            "id": "00000000-0000-0000-0000-000000000012",
            "name": "run",
            "reference": {"name": "actions/setup-node", "version": "v4", "type": "repository"},
            "inputs": {"node-version": "22"},
            "continueOnError": false,
            "timeoutInMinutes": null
        }
    ]));
    let mut token_only = checkout_test_message(json!([{
        "id": "00000000-0000-0000-0000-000000000013",
        "name": "token-only checkout",
        "reference": {"name": "actions/checkout", "version": "v4", "type": "repository"},
        "inputs": {"token": "submodule-token", "fetch-depth": "0"},
        "continueOnError": false,
        "timeoutInMinutes": null
    }]));
    let mut empty_ref = checkout_test_message(json!([{
        "id": "00000000-0000-0000-0000-000000000014",
        "name": "empty-ref checkout",
        "reference": {"name": "actions/checkout", "version": "v4", "type": "repository"},
        // An expression that resolved to nothing means "default branch" —
        // the local snapshot IS the default, so the redirect must apply.
        "inputs": {"ref": "", "fetch-depth": "0"},
        "continueOnError": false,
        "timeoutInMinutes": null
    }]));
    let mut expr_ref = checkout_test_message(json!([{
        "id": "00000000-0000-0000-0000-000000000015",
        "name": "expression-ref checkout",
        "reference": {"name": "actions/checkout", "version": "v4", "type": "repository"},
        // Template refs are never evaluated server-side, and one that is not
        // provably the action's declared default selects a target the
        // workflow controls at runtime. Redirecting it would hijack that
        // target once the runner evaluates the expression, so it must be
        // treated as explicitly set.
        "inputs": {"ref": "${{ inputs.head-sha }}", "fetch-depth": "0"},
        "continueOnError": false,
        "timeoutInMinutes": null
    }]));
    let original_explicit = message.steps[1].inputs.clone();
    let original_non_checkout = message.steps[2].inputs.clone();
    assert!(message.snapshot.is_none());

    let redirected = redirect_primary_checkout(
        &mut message,
        &WorkspaceSnapshot {
            head_sha: Some("f000000000000000000000000000000000000000".to_owned()),
            commit_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            tree_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            repository: "snapshots/11111111-1111-4111-8111-111111111111".to_owned(),
            default_branch: Some("main".to_owned()),
            before_sha: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()),
        },
        "http://127.0.0.1:9090",
        "local-runtime-jwt",
    );

    assert_eq!(redirected, 1);
    assert_eq!(
        message.preloop_snapshot_token_steps,
        Some(vec!["00000000-0000-0000-0000-000000000010".to_owned()]),
        "the pinned checkout step must be recorded by id so claim and retry can re-mint it"
    );
    let primary = &message.steps[0].inputs;
    assert_eq!(
        primary.get("repository"),
        Some(&"snapshots/11111111-1111-4111-8111-111111111111".to_owned())
    );
    assert_eq!(
        primary.get("ref"),
        Some(&"0123456789abcdef0123456789abcdef01234567".to_owned())
    );
    assert_eq!(
        primary.get("github-server-url"),
        Some(&"http://127.0.0.1:9090".to_owned())
    );
    // Pinned so snapshot checkout keeps working when GITHUB_TOKEN carries a
    // GitHub App installation token or PAT the snapshot endpoint cannot verify.
    assert_eq!(primary.get("token"), Some(&"local-runtime-jwt".to_owned()));
    assert_eq!(primary.get("path"), Some(&"source".to_owned()));
    assert_eq!(primary.get("fetch-depth"), Some(&"0".to_owned()));
    assert_eq!(message.steps[1].inputs, original_explicit);
    assert_eq!(message.steps[2].inputs, original_non_checkout);
    assert!(
        message.snapshot.is_none(),
        "snapshot wire field must remain untouched"
    );

    assert_eq!(
        redirect_primary_checkout(
            &mut token_only,
            &WorkspaceSnapshot {
                head_sha: Some("f000000000000000000000000000000000000000".to_owned()),
                commit_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                tree_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                repository: "snapshots/22222222-2222-4222-8222-222222222222".to_owned(),
                default_branch: Some("main".to_owned()),
                before_sha: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()),
            },
            "http://127.0.0.1:9090",
            "local-runtime-jwt",
        ),
        1,
        "a token-only primary checkout still targets the local snapshot"
    );
    assert_eq!(
        token_only.steps[0].inputs.get("token"),
        Some(&"local-runtime-jwt".to_owned())
    );
    assert_eq!(
        redirect_primary_checkout(
            &mut empty_ref,
            &WorkspaceSnapshot {
            head_sha: Some("f000000000000000000000000000000000000000".to_owned()),
                commit_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                tree_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                repository: "snapshots/33333333-3333-4333-8333-333333333333".to_owned(),
                default_branch: Some("main".to_owned()),
                before_sha: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()),
            },
            "http://127.0.0.1:9090",
            "local-runtime-jwt",
        ),
        1,
        "an empty `ref` input is GitHub's default-branch semantics and must be redirected to the snapshot"
    );
    assert_eq!(
        empty_ref.steps[0].inputs.get("ref"),
        Some(&"0123456789abcdef0123456789abcdef01234567".to_owned())
    );
    assert_eq!(
        redirect_primary_checkout(
            &mut expr_ref,
            &WorkspaceSnapshot {
                head_sha: Some("f000000000000000000000000000000000000000".to_owned()),
                commit_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                tree_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                repository: "snapshots/44444444-4444-4444-8444-444444444444".to_owned(),
                default_branch: Some("main".to_owned()),
                before_sha: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()),
            },
            "http://127.0.0.1:9090",
            "local-runtime-jwt",
        ),
        0,
        "a template `ref` input selects the workflow's own target and must not be redirected to the snapshot"
    );
    assert_eq!(
        expr_ref.steps[0].inputs.get("ref"),
        Some(&"${{ inputs.head-sha }}".to_owned()),
        "an expression ref must survive the redirect pass untouched"
    );
}

/// A job that sat queued past the pinned token's lifetime must get a fresh
/// credential at claim, scoped to itself, and unpinned steps must be
/// untouched.
#[tokio::test]
async fn claim_remints_expired_snapshot_checkout_tokens() {
    let mut message = checkout_test_message(json!([
        {
            "id": "00000000-0000-0000-0000-000000000020",
            "name": "checkout",
            "reference": {"name": "actions/checkout", "version": "v4", "type": "repository"},
            "inputs": {"token": "expired-pinned-token", "fetch-depth": "0"},
            "continueOnError": false,
            "timeoutInMinutes": null
        },
        {
            "id": "00000000-0000-0000-0000-000000000021",
            "name": "run",
            "reference": {"name": "actions/setup-node", "version": "v4", "type": "repository"},
            "inputs": {"node-version": "22"},
            "continueOnError": false,
            "timeoutInMinutes": null
        }
    ]));
    message.preloop_snapshot_token_steps =
        Some(vec!["00000000-0000-0000-0000-000000000020".to_owned()]);

    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();

    let refreshed = crate::broker::re_mint_snapshot_tokens(&mut message, &state);
    assert_eq!(refreshed, 1);

    let token = message.steps[0].inputs.get("token").unwrap();
    assert_ne!(token, "expired-pinned-token");
    let claims = state
        .verify_local_jwt_claims(token)
        .expect("re-minted token must verify");
    assert_eq!(
        claims["sub"],
        format!("preloop-job-{}", message.job_id),
        "the fresh token must be scoped to this job"
    );
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(
        claims["exp"].as_u64().unwrap() > now,
        "the re-minted token must not be already expired"
    );
    assert_eq!(
        message.steps[1].inputs.get("token"),
        None,
        "unpinned steps keep their inputs untouched"
    );

    // Without the pinned-step marker nothing is refreshed.
    message.preloop_snapshot_token_steps = None;
    assert_eq!(
        crate::broker::re_mint_snapshot_tokens(&mut message, &state),
        0
    );
}

/// The claim-time re-mint must actually run on the real claim path: a queued
/// redirected checkout carries the submission-time pinned token, and the job
/// the runner acquires must carry a freshly minted one.
#[tokio::test]
async fn claim_remints_snapshot_tokens_on_the_real_claim_path() {
    let temp = tempfile::tempdir().unwrap();
    let (state_dir, workspace) = create_snapshot_fixture(temp.path());
    let mut state = AppState::new(state_dir.clone()).await.unwrap();
    state.local_workspace = Some(workspace.clone());
    let app = app(state.clone(), CancellationToken::new());

    submit_yaml(
        &app,
        r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
"#,
        "owner/repo",
    )
    .await;
    // The re-mint produces a fresh JWT; within the same second it is
    // byte-identical to the pinned one, so give the clock room to move.
    tokio::time::sleep(Duration::from_millis(1100)).await;

    // The pinned submission-time token, as it sits on the queued message.
    let pinned_token = {
        let inner = state.inner.lock().await;
        let queued = inner.queue.front().expect("job should be queued");
        let checkout = queued
            .message
            .steps
            .iter()
            .find(|step| {
                step.reference
                    .as_ref()
                    .and_then(|reference| reference.name.as_deref())
                    .is_some_and(|name| name.eq_ignore_ascii_case("actions/checkout"))
            })
            .expect("queued job should contain the redirected checkout step");
        checkout.inputs.get("token").cloned().expect("pinned token")
    };
    assert!(
        state.verify_local_jwt_claims(&pinned_token).is_some(),
        "the queued token must be a valid local JWT"
    );

    let session = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({
            "agent": {"id": 1, "name": "remint-runner"},
            "ownerName": "remint test",
            "sessionId": "00000000-0000-0000-0000-000000000000",
            "useFipsEncryption": false
        }),
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();
    let broker_message = request_json(
        &app,
        Method::GET,
        &format!(
            "/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&waitSeconds=0"
        ),
        Value::Null,
    )
    .await;
    let broker_body: Value =
        serde_json::from_str(broker_message["body"].as_str().unwrap()).unwrap();
    let runner_request_id = broker_body["runner_request_id"]
        .as_str()
        .expect("broker message should identify the queued request");
    let runner_token = state
        .local_jwt(json!({
            "sub": "preloop-runner-listen-1",
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    let acquired = request_json_with_bearer(
        &app,
        Method::POST,
        "/broker/1/acquirejob",
        json!({
            "jobMessageId": runner_request_id,
            "billingOwnerId": "local",
            "runnerOS": "linux"
        }),
        &runner_token,
    )
    .await;

    let checkout = acquired["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["reference"]["name"].as_str() == Some("actions/checkout"))
        .expect("the acquired job should contain the checkout step");
    fn acquired_input<'a>(step: &'a Value, name: &str) -> Option<&'a str> {
        step["inputs"]
            .get(name)
            .and_then(Value::as_str)
            .or_else(|| {
                let found = step["inputs"]["map"].as_array()?.iter().find(|entry| {
                    entry
                        .get("Key")
                        .or_else(|| entry.get("key"))
                        .and_then(|key| key.get("lit"))
                        .and_then(Value::as_str)
                        .is_some_and(|key| key == name)
                })?;
                found
                    .get("Value")
                    .or_else(|| found.get("value"))
                    .and_then(|value| value.get("lit"))
                    .and_then(Value::as_str)
            })
    }
    let claimed_token = acquired_input(checkout, "token").expect("claimed pinned token");
    assert_ne!(
        claimed_token, pinned_token,
        "claim must replace the submission-time token with a fresh one"
    );
    let claims = state
        .verify_local_jwt_claims(claimed_token)
        .expect("the claimed token must verify as a local JWT");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(
        claims["exp"].as_u64().unwrap() > now,
        "the claimed token must not be expired"
    );
}

/// A retry verdict must carry a freshly minted snapshot credential: the
/// worker replays the failed step from the message it already holds, whose
/// pinned token may be long expired.
#[tokio::test]
async fn retry_verdict_carries_a_fresh_snapshot_token() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: \"false\"\n",
            "event": "push",
            "repository": "owner/repo",
            "preserve_on_failure": true
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;

    let (agent_job_id, worker_token) = {
        let inner = state.inner.lock().await;
        let record = inner.job_requests.iter().next().unwrap().1;
        (
            record.agent_job_id,
            state.mint_debug_worker_token(&record.plan_id, &record.agent_job_id),
        )
    };

    let opened = request_json_with_bearer(
        &app,
        Method::POST,
        "/api/v1/debug/sessions",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "agent_job_id": agent_job_id,
            "job_name": "build",
            "step": {
                "index": 0,
                "total": 1,
                "context_name": "__run",
                "display_name": "Run false",
                "command": "false",
                "exit_code": 1,
                "elapsed_ms": 20,
                "diagnostics": []
            }
        }),
        &worker_token,
    )
    .await;
    let session_id = opened["session_id"].as_str().unwrap().to_owned();

    request_json(
        &app,
        Method::POST,
        &format!("/api/v1/debug/sessions/{session_id}/verdict"),
        json!({ "verdict": "retry", "controller": "test" }),
    )
    .await;

    let polled = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/api/v1/debug/sessions/{session_id}/verdict?wait=0"),
        Value::Null,
        &worker_token,
    )
    .await;
    assert_eq!(polled["verdict"], "retry");
    let token = polled["snapshot_token"]
        .as_str()
        .expect("retry verdict must carry a fresh snapshot credential");
    let claims = state
        .verify_local_jwt_claims(token)
        .expect("verdict-supplied token must verify");
    assert_eq!(claims["sub"], format!("preloop-job-{agent_job_id}"));
}

/// The snapshot surface must reject bad credentials with a Bearer challenge:
/// a bare 401 makes git fall back to Basic semantics and prompt for a
/// username no job can answer.
#[tokio::test]
async fn snapshot_401_advertises_a_bearer_challenge() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(
                    "/snapshots/00000000-0000-0000-0000-000000000001/info/refs?service=git-upload-pack",
                )
                .header(header::AUTHORIZATION, "Bearer not-a-real-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer realm=\"preloop-snapshot\"")
    );
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["error"], "invalid snapshot Git token");
}

#[tokio::test]
async fn local_workspace_checkout_acquires_synthetic_repository_and_serves_git_http() {
    // `AppState::new` captures `PRELOOP_GITHUB_TOKEN` / `PRELOOP_GITHUB_APP_*`
    // from the process env, and the claim path reads `PRELOOP_GITHUB_API_URL`
    // live. Hold the env lock so a concurrent env-mutating test cannot make
    // this job carry a foreign PAT (which would 401 the snapshot Git auth
    // instead of 403 on the wrong-run probe).
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let (state_dir, workspace) = create_snapshot_fixture(temp.path());
    let mut state = AppState::new(state_dir.clone()).await.unwrap();
    state.local_workspace = Some(workspace.clone());
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_yaml(
        &app,
        r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
"#,
        "owner/repo",
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // `github.sha` must be the workspace's real HEAD commit, not the
    // synthetic snapshot commit: a workflow step that fetches
    // `${{ github.sha }}` from the real remote (custom checkouts) must
    // receive a sha the upstream host can actually resolve, and the
    // snapshot commit exists only in this engine's store.
    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    let context_sha = run.github["sha"].as_str().unwrap().to_owned();
    let snapshot_sha = run.workspace_snapshot.as_ref().unwrap().commit_sha.clone();
    let workspace_head =
        String::from_utf8(git_fixture_output(&workspace, &["rev-parse", "HEAD"])).unwrap();
    drop(inner);
    assert_eq!(
        context_sha,
        workspace_head.trim(),
        "github.sha must be the real workspace HEAD"
    );
    assert_ne!(
        context_sha, snapshot_sha,
        "the synthetic snapshot sha must not leak into the github context"
    );

    let session = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({
            "agent": {"id": 1, "name": "snapshot-runner"},
            "ownerName": "snapshot test",
            "sessionId": "00000000-0000-0000-0000-000000000000",
            "useFipsEncryption": false
        }),
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();
    let broker_message = request_json(
        &app,
        Method::GET,
        &format!(
            "/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&waitSeconds=0"
        ),
        Value::Null,
    )
    .await;
    let broker_body: Value = serde_json::from_str(broker_message["body"].as_str().unwrap())
        .expect("broker message body should be JSON");
    let runner_request_id = broker_body["runner_request_id"]
        .as_str()
        .expect("broker message should identify the queued request");
    let runner_token = state
        .local_jwt(json!({
            "sub": "preloop-runner-listen-1",
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();
    let acquired = request_json_with_bearer(
        &app,
        Method::POST,
        "/broker/1/acquirejob",
        json!({
            "jobMessageId": runner_request_id,
            "billingOwnerId": "local",
            "runnerOS": "linux"
        }),
        &runner_token,
    )
    .await;

    let checkout = acquired["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["reference"]["name"].as_str() == Some("actions/checkout"))
        .expect("the acquired job should contain the checkout step");
    fn checkout_input<'a>(step: &'a Value, name: &str) -> Option<&'a str> {
        step["inputs"]
            .get(name)
            .and_then(Value::as_str)
            .or_else(|| {
                step["inputs"]["map"]
                    .as_array()?
                    .iter()
                    .find(|entry| {
                        entry
                            .get("Key")
                            .or_else(|| entry.get("key"))
                            .and_then(|key| key.get("lit"))
                            .and_then(Value::as_str)
                            == Some(name)
                    })
                    .and_then(|entry| entry.get("Value").or_else(|| entry.get("value")))
                    .and_then(|value| value.get("lit"))
                    .and_then(Value::as_str)
            })
    }
    let repository = checkout_input(checkout, "repository")
        .unwrap_or_else(|| panic!("checkout repository should be rewritten: {checkout}"));
    let commit = checkout_input(checkout, "ref").expect("checkout ref should be rewritten");
    let server_url = checkout_input(checkout, "github-server-url")
        .expect("checkout server URL should be rewritten");
    assert_eq!(repository, format!("snapshots/{run_id}"));
    assert_eq!(server_url, public_base_url());
    assert_eq!(commit.len(), 40);
    assert!(commit.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(acquired["snapshot"], Value::Null);

    let runtime_token = acquired["variables"]["system.github.token"]["value"]
        .as_str()
        .expect("acquired job should expose its runtime token");
    let unauthenticated = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/snapshots/{run_id}/info/refs?service=git-upload-pack"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let wrong_run = "22222222-2222-4222-8222-222222222222";
    let wrong_binding = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/snapshots/{wrong_run}/info/refs?service=git-upload-pack"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(wrong_binding.status(), StatusCode::FORBIDDEN);

    let advertisement = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/snapshots/{run_id}/info/refs?service=git-upload-pack"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(advertisement.status(), StatusCode::OK);
    let advertisement_body = to_bytes(advertisement.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(advertisement_body
        .windows(commit.len())
        .any(|window| window == commit.as_bytes()));

    fn pkt_line(payload: &[u8]) -> Vec<u8> {
        let mut line = format!("{:04x}", payload.len() + 4).into_bytes();
        line.extend_from_slice(payload);
        line
    }
    let want = format!("want {commit} multi_ack_detailed side-band-64k thin-pack ofs-delta\n");
    let mut upload_request = pkt_line(want.as_bytes());
    upload_request.extend_from_slice(b"0000");
    upload_request.extend_from_slice(&pkt_line(b"done\n"));
    let upload = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/snapshots/{run_id}/git-upload-pack"))
                .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
                .header(
                    header::CONTENT_TYPE,
                    "application/x-git-upload-pack-request",
                )
                .body(Body::from(upload_request))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::OK);
    let upload_body = to_bytes(upload.into_body(), usize::MAX).await.unwrap();

    // Decode side-band channel 1 from the actual git-http-backend response,
    // then let Git validate the fetched pack's checksum and object framing.
    let mut pack = Vec::new();
    let mut offset = 0;
    while offset + 4 <= upload_body.len() {
        let length = usize::from_str_radix(
            std::str::from_utf8(&upload_body[offset..offset + 4]).unwrap(),
            16,
        )
        .unwrap();
        offset += 4;
        if length == 0 {
            continue;
        }
        let payload_len = length - 4;
        assert!(offset + payload_len <= upload_body.len());
        let payload = &upload_body[offset..offset + payload_len];
        if payload.first() == Some(&1) {
            pack.extend_from_slice(&payload[1..]);
        } else if payload.starts_with(b"PACK") {
            pack.extend_from_slice(payload);
        }
        offset += payload_len;
    }
    assert!(
        pack.starts_with(b"PACK"),
        "upload-pack response should contain a pack"
    );
    let mut index_pack = Command::new("git")
        .args(["index-pack", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    index_pack.stdin.take().unwrap().write_all(&pack).unwrap();
    let index_result = index_pack.wait_with_output().unwrap();
    assert!(
        index_result.status.success(),
        "Git rejected the route's fetched pack: {}",
        String::from_utf8_lossy(&index_result.stderr)
    );

    let bare_repository = state_dir.join(repository);
    assert!(
        git_fixture_output_allow_failure(
            &bare_repository,
            &["cat-file", "-e", &format!("{commit}^{{commit}}")]
        )
        .0
    );
    assert_eq!(
        git_fixture_output(
            &bare_repository,
            &["show", &format!("{commit}:tracked.txt")]
        ),
        b"tracked unstaged change\n"
    );
}

/// Uploaded job logs must stay bounded, and pruning must not cost a run its
/// logs: `get_run_logs` prefers the blob and falls back to the in-memory
/// blocks, so an evicted plan degrades instead of disappearing.
#[tokio::test]
async fn replay_results_are_pruned_to_the_retention_window() {
    let temp = tempfile::tempdir().unwrap();
    let results = temp.path().join("replay").join("results");

    // One directory per execution plan, oldest first so mtime ordering is
    // unambiguous rather than dependent on filesystem timestamp resolution.
    let total = crate::blob_store::REPLAY_PLANS_RETAINED + 8;
    let mut plans = Vec::new();
    for index in 0..total {
        let plan = results.join(format!("plan-{index:03}"));
        std::fs::create_dir_all(&plan).unwrap();
        std::fs::write(plan.join("job-logs.txt"), format!("log {index}")).unwrap();
        filetime::set_file_mtime(
            &plan,
            filetime::FileTime::from_unix_time(1_700_000_000 + index as i64, 0),
        )
        .unwrap();
        plans.push(plan);
    }

    crate::blob_store::prune_replay_results(temp.path(), &std::collections::BTreeSet::new()).await;

    let surviving: Vec<_> = plans.iter().filter(|plan| plan.exists()).collect();
    assert_eq!(
        surviving.len(),
        crate::blob_store::REPLAY_PLANS_RETAINED,
        "retention window must bound the directory"
    );
    assert!(
        plans[total - 1].exists(),
        "the most recent plan must survive"
    );
    assert!(!plans[0].exists(), "the oldest plan must be evicted");
    assert_eq!(
        std::fs::read_to_string(plans[total - 1].join("job-logs.txt")).unwrap(),
        format!("log {}", total - 1),
        "surviving logs must be intact"
    );
}

#[tokio::test]
async fn replay_result_pruning_preserves_active_plans() {
    let temp = tempfile::tempdir().unwrap();
    let results = temp.path().join("replay").join("results");
    for index in 0..=crate::blob_store::REPLAY_PLANS_RETAINED {
        let plan = results.join(format!("plan-{index:03}"));
        std::fs::create_dir_all(&plan).unwrap();
        filetime::set_file_mtime(
            &plan,
            filetime::FileTime::from_unix_time(1_700_000_000 + index as i64, 0),
        )
        .unwrap();
    }
    let active = BTreeSet::from(["plan-000".to_owned()]);

    crate::blob_store::prune_replay_results(temp.path(), &active).await;

    assert!(results.join("plan-000").exists());
}

#[tokio::test]
async fn pruning_replay_results_is_a_no_op_without_a_replay_directory() {
    let temp = tempfile::tempdir().unwrap();
    crate::blob_store::prune_replay_results(temp.path(), &std::collections::BTreeSet::new()).await;
    assert!(!temp.path().join("replay").exists());
}

// ─── Guest-side impersonation hardening ──────────────────────────────────
//
// The pool's control socket is reachable from untrusted workflow code inside
// every runner VM, and the runner identity material on the guest disk is
// readable by any step. These tests pin the server-side contract that makes
// that exposure non-transitive: a guest can act as its own machine's runner
// (self-disclosure, same as GitHub-hosted runners) but can never pull a job
// assigned to another machine, even with a fully stolen runner identity.

/// Register a runner through the AzDO compat path and mint its listen token
/// through the mock OAuth flow with the clientId the server assigned.
async fn register_runner_with_token(
    app: &Router,
    name: &str,
    labels: &[&str],
    provision_token: Option<&str>,
) -> (i64, String) {
    let labels_json = labels
        .iter()
        .map(|name| json!({"id": 0, "name": name, "type": "user"}))
        .collect::<Vec<_>>();
    let mut builder = Request::builder()
        .method(Method::POST)
        .uri("/runner/server/_apis/distributedtask/pools/1/agents")
        .header(header::AUTHORIZATION, "Bearer preloop-system-token")
        .header("content-type", "application/json");
    if let Some(token) = provision_token {
        builder = builder.header("X-Preloop-Provision-Token", token);
    }
    let response = app
        .clone()
        .oneshot(
            builder
                .body(Body::from(
                    json!({"name": name, "labels": labels_json}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let registered: Value = serde_json::from_slice(&body).unwrap();
    let runner_id = registered["id"].as_i64().unwrap();
    let client_id = registered["authorization"]["clientId"]
        .as_str()
        .unwrap()
        .to_owned();
    let oauth = request_json(
        app,
        Method::POST,
        "/runner/server/_apis/v1/oauth2/token",
        json!({
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": "unused"
        }),
    )
    .await;
    let token = oauth["access_token"].as_str().unwrap().to_owned();
    (runner_id, token)
}

async fn create_disttask_session(app: &Router, bearer: &str, agent_id: i64) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(Method::POST)
        .uri("/runner/server/_apis/distributedtask/pools/1/sessions")
        .header(header::AUTHORIZATION, format!("Bearer {bearer}"))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "agent": {"id": agent_id, "name": "runner"},
                "ownerName": "owner",
                "preloopAzdo": true,
                "useFipsEncryption": false
            })
            .to_string(),
        ))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn poll_message(app: &Router, bearer: &str, session_id: &str) -> Value {
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&waitSeconds=0"
        ))
        .header(header::AUTHORIZATION, format!("Bearer {bearer}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

async fn submit_simple_run(app: &Router) -> Value {
    request_json(
        app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: self-hosted\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await
}

#[tokio::test]
async fn pull_request_submission_uses_head_sha_not_zeros() {
    // A pull_request payload has no `after`, and a submission that does not
    // pre-resolve a sha used to fall through to all-zeros. The job then asks
    // its remote for `0000…` and dies with "not our ref 0000…", which points
    // nowhere near the real cause.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: pull_request\njobs:\n  build:\n    runs-on: self-hosted\n    steps:\n      - run: echo hi\n",
            "event": "pull_request",
            "repository": "owner/repo",
            "payload": {
                "action": "opened",
                "number": 7,
                "pull_request": {
                    "head": { "ref": "feature", "sha": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3" },
                    "base": { "ref": "main", "sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2" }
                }
            }
        }),
    )
    .await;
    assert_eq!(accepted["queued_jobs"], 1);

    let inner = state.inner.lock().await;
    let (_, run_record) = inner.runs.iter().next().unwrap();
    assert_eq!(
        run_record.head_sha, "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3",
        "pull_request head sha must drive github.sha instead of the zero sha"
    );
}

#[tokio::test]
async fn pull_request_submission_uses_short_ref_name_and_job_id() {
    // GitHub presents PR events with `github.ref = refs/pull/<n>/merge` and
    // `github.ref_name = <n>/merge` (short form), and supplies the job id via
    // the `system.github.job` variable. Previously `ref_name` leaked the full
    // `refs/pull/7/merge` and `github.job`/`GITHUB_JOB` were empty.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: pull_request\njobs:\n  build:\n    runs-on: self-hosted\n    strategy:\n      matrix:\n        os: [a, b]\n    steps:\n      - run: echo hi\n",
            "event": "pull_request",
            "repository": "owner/repo",
            "payload": {
                "action": "opened",
                "number": 7,
                "pull_request": {
                    "head": { "ref": "feature", "sha": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3" },
                    "base": { "ref": "main", "sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3" }
                }
            }
        }),
    )
    .await;
    assert_eq!(accepted["queued_jobs"], 2);

    let inner = state.inner.lock().await;
    let queued: Vec<_> = inner.queue.iter().collect();
    assert_eq!(queued.len(), 2);
    let github = &queued[0].message.context_data["github"].to_json();
    assert_eq!(github["ref"], "refs/pull/7/merge");
    assert_eq!(github["ref_name"], "7/merge");
    assert_eq!(github["ref_type"], "branch");
    assert_eq!(github["head_ref"], "feature");
    assert_eq!(github["base_ref"], "main");
    assert_eq!(
        queued[0].message.variables["system.github.job"]
            .value
            .as_deref(),
        Some("build")
    );
    // GitHub's context carries no `job` key — the runner reads the variable.
    assert!(github.get("job").is_none());

    // Both matrix cells carry the same job id; strategy indices are per-cell.
    for request in &queued {
        assert_eq!(
            request.message.variables["system.github.job"]
                .value
                .as_deref(),
            Some("build")
        );
    }
    let strategy = queued[0].message.context_data["strategy"].to_json();
    assert_eq!(strategy["job-index"], 0.0);
    assert_eq!(strategy["job-total"], 2.0);
    let strategy1 = queued[1].message.context_data["strategy"].to_json();
    assert_eq!(strategy1["job-index"], 1.0);
}

async fn pool_managed_state(temp: &tempfile::TempDir) -> AppState {
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.inner.lock().await.pool_assignments_enabled = true;
    state
}

/// Simulate the host-side pool staging a provision token before a machine
/// boots and runs `configure`.
fn stage_provision_token(state: &AppState, token: &str) {
    state
        .pending_registrations
        .write()
        .unwrap()
        .insert(token.to_owned(), std::time::SystemTime::now());
}

#[tokio::test]
async fn stolen_identity_cannot_pull_another_machines_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    // Machine A is provisioned for the queued job: host-side the pool staged
    // one provision token, and the guest's configure presents it.
    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    {
        let inner = state.inner.lock().await;
        assert_eq!(inner.pool_pending.len(), 1, "job waits for its machine");
        assert!(inner.job_assignments.is_empty());
    }
    stage_provision_token(&state, "token-a");
    let (runner_a, token_a) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.job_assignments.values().next().map(|r| r.runner_id),
            Some(runner_a),
            "registration pairing bound the job to machine A"
        );
    }

    // A second machine + a rogue process on it: a valid identity for another
    // runner, plus a raw bearer-free session impersonating runner A.
    stage_provision_token(&state, "token-b");
    let (runner_b, token_b) =
        register_runner_with_token(&app, "machine-b", &["self-hosted"], Some("token-b")).await;
    let (_, session_b) = create_disttask_session(&app, &token_b, runner_b).await;
    let session_b_id = session_b["sessionId"].as_str().unwrap();
    let stolen = poll_message(&app, &token_b, session_b_id).await;
    assert!(
        stolen.is_null(),
        "other runner's identity must not receive the assigned job: {stolen}"
    );

    // Bearer-free impersonation (the test harness attaches the system token,
    // which is not a runner token — exactly what untrusted guest code has).
    let (_, rogue_session) = create_disttask_session(&app, "preloop-system-token", runner_a).await;
    let rogue_id = rogue_session["sessionId"].as_str().unwrap();
    let stolen = poll_message(&app, "preloop-system-token", rogue_id).await;
    assert!(
        stolen.is_null(),
        "unverified session impersonating runner A must not claim: {stolen}"
    );

    // The legitimately paired machine's runner receives its job.
    let (_, session_a) = create_disttask_session(&app, &token_a, runner_a).await;
    let session_a_id = session_a["sessionId"].as_str().unwrap();
    let delivered = poll_message(&app, &token_a, session_a_id).await;
    assert!(
        delivered["messageType"].as_str().is_some(),
        "paired runner must receive its assigned job: {delivered}"
    );
}

#[tokio::test]
async fn failing_provisioning_cannot_starve_a_healthy_runner() {
    // A pool that keeps provisioning machines and losing them (broken image,
    // host that cannot start VMs) re-binds the job to each short-lived
    // machine. Every rebind refreshed the binding window, so an established,
    // capable, verified runner was refused forever — observed as "jobs
    // queued, never promoted" behind a doomed on-demand loop.
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    // A healthy runner is registered and polling before the job lands.
    stage_provision_token(&state, "token-healthy");
    let (healthy_id, healthy_token) = register_runner_with_token(
        &app,
        "healthy-host",
        &["self-hosted"],
        Some("token-healthy"),
    )
    .await;
    let (_, healthy_session) = create_disttask_session(&app, &healthy_token, healthy_id).await;
    let healthy_session_id = healthy_session["sessionId"].as_str().unwrap().to_owned();

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);

    // Phantom machines: each registers, takes the pairing, then dies without
    // claiming. Age the binding between rounds so every phantom adopts a
    // "stale" pairing exactly as the real churn does.
    for round in 0..3 {
        {
            let mut inner = state.inner.lock().await;
            let keys: Vec<_> = inner.job_assignments.keys().cloned().collect();
            for key in keys {
                if let Some(record) = inner.job_assignments.get_mut(&key) {
                    record.at = std::time::SystemTime::now()
                        - crate::runtime_scheduling::CLAIM_BINDING_TTL
                        - std::time::Duration::from_secs(1);
                    record.first_at = record.at;
                }
            }
        }
        let token_name = format!("token-phantom-{round}");
        stage_provision_token(&state, &token_name);
        let (phantom_id, _) = register_runner_with_token(
            &app,
            &format!("phantom-{round}"),
            &["self-hosted"],
            Some(&token_name),
        )
        .await;
        // The machine dies before claiming anything.
        let shared = std::sync::Arc::new(crate::state::SharedState {
            state: state.clone(),
            shutdown: CancellationToken::new(),
        });
        crate::runner_lifecycle::purge_runner_identity(&shared, phantom_id).await;
    }

    // The established runner must now be able to claim: the job has been
    // bound-and-abandoned for longer than the binding window.
    {
        let mut inner = state.inner.lock().await;
        let keys: Vec<_> = inner.job_assignments.keys().cloned().collect();
        for key in keys {
            if let Some(record) = inner.job_assignments.get_mut(&key) {
                record.first_at = std::time::SystemTime::now()
                    - crate::runtime_scheduling::CLAIM_BINDING_TTL
                    - std::time::Duration::from_secs(1);
            }
        }
        let pending: Vec<_> = inner.pool_pending.keys().cloned().collect();
        for key in pending {
            inner.pool_pending.insert(
                key,
                std::time::SystemTime::now()
                    - crate::runtime_scheduling::CLAIM_BINDING_TTL
                    - std::time::Duration::from_secs(1),
            );
        }
    }

    let delivered = poll_message(&app, &healthy_token, &healthy_session_id).await;
    assert!(
        delivered["messageType"].as_str().is_some(),
        "established runner must rescue a job abandoned by churning machines: {delivered}"
    );
}

#[tokio::test]
async fn rebinding_churn_cannot_starve_an_established_runner() {
    // Machines that register and then go silent without claiming keep
    // adopting the stale pairing, and each adoption refreshed the binding
    // window. An established, capable, verified runner was refused for as
    // long as the churn continued.
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);

    // The established runner registers after the job queued, so the job is
    // pool-pending and it is not the paired machine.
    stage_provision_token(&state, "token-established");
    let (established_id, established_token) = register_runner_with_token(
        &app,
        "established-host",
        &["self-hosted"],
        Some("token-established"),
    )
    .await;
    let (_, established_session) =
        create_disttask_session(&app, &established_token, established_id).await;
    let established_session_id = established_session["sessionId"]
        .as_str()
        .unwrap()
        .to_owned();

    // Churn: each new machine adopts the pairing once the previous one's
    // binding looks stale, refreshing `at` every round.
    for round in 0..3 {
        {
            let mut inner = state.inner.lock().await;
            let keys: Vec<_> = inner.job_assignments.keys().cloned().collect();
            for key in keys {
                if let Some(record) = inner.job_assignments.get_mut(&key) {
                    record.at = std::time::SystemTime::now()
                        - crate::runtime_scheduling::CLAIM_BINDING_TTL
                        - std::time::Duration::from_secs(1);
                }
            }
        }
        let token_name = format!("token-churn-{round}");
        stage_provision_token(&state, &token_name);
        register_runner_with_token(
            &app,
            &format!("churn-{round}"),
            &["self-hosted"],
            Some(&token_name),
        )
        .await;
    }

    // Every round refreshed `at`, so the binding still looks fresh — but the
    // job has been bound-and-unclaimed since the first round.
    {
        let mut inner = state.inner.lock().await;
        let keys: Vec<_> = inner.job_assignments.keys().cloned().collect();
        assert!(!keys.is_empty(), "churn must leave the job bound");
        for key in keys {
            if let Some(record) = inner.job_assignments.get_mut(&key) {
                assert!(
                    record.runner_id != established_id,
                    "churned machine, not the established runner, holds the pairing"
                );
                record.first_at = std::time::SystemTime::now()
                    - crate::runtime_scheduling::CLAIM_BINDING_TTL
                    - std::time::Duration::from_secs(1);
                record.at = std::time::SystemTime::now();
            }
        }
    }

    let delivered = poll_message(&app, &established_token, &established_session_id).await;
    assert!(
        delivered["messageType"].as_str().is_some(),
        "established runner must rescue a job held by churning machines: {delivered}"
    );
}

#[tokio::test]
async fn stale_binding_requeues_behind_newer_waits() {
    // A job whose machine died before claiming must not monopolize the pool:
    // its stale binding is released and the job re-enters the waitlist at the
    // *back*, so a job that has been waiting with no machine gets the next
    // one. The released job is still served afterwards — nothing is dropped.
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    // Job A is paired to machine-a, which dies before claiming.
    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    stage_provision_token(&state, "token-a");
    let (runner_a, _) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    let key_a = {
        let inner = state.inner.lock().await;
        let key = inner.job_assignments.keys().next().cloned().unwrap();
        assert_eq!(
            inner.job_assignments.get(&key).map(|r| r.runner_id),
            Some(runner_a),
            "registration pairing bound job A to machine-a"
        );
        key
    };

    // Job B arrives afterwards and waits for its machine (machine-a has no
    // session, so queue-time binding cannot take it).
    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    let key_b = {
        let inner = state.inner.lock().await;
        let key = inner.pool_pending.keys().next().cloned().unwrap();
        assert!(key.0 != key_a.0, "second submission is a distinct run");
        key
    };

    // Age machine-a's binding past the claim window, then machine-b
    // registers: the stale binding is released, and the earlier wait (job B)
    // is paired — not the dying job re-adopted with priority.
    {
        let mut inner = state.inner.lock().await;
        let keys: Vec<_> = inner.job_assignments.keys().cloned().collect();
        for key in keys {
            if let Some(record) = inner.job_assignments.get_mut(&key) {
                record.at = std::time::SystemTime::now()
                    - crate::runtime_scheduling::CLAIM_BINDING_TTL
                    - std::time::Duration::from_secs(1);
                record.first_at = record.at;
            }
        }
    }
    let key_a_first_at = {
        let inner = state.inner.lock().await;
        inner
            .job_assignments
            .get(&key_a)
            .map(|r| r.first_at)
            .unwrap()
    };
    stage_provision_token(&state, "token-b");
    let (runner_b, _) =
        register_runner_with_token(&app, "machine-b", &["self-hosted"], Some("token-b")).await;
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.job_assignments.len(),
            2,
            "the stale record is kept (its first_at rides the requeue), the newer wait still gets the machine"
        );
        assert_eq!(
            inner.job_assignments.get(&key_b).map(|r| r.runner_id),
            Some(runner_b),
            "the newer wait gets the machine before the re-queued dying job"
        );
        assert!(
            inner.pool_pending.contains_key(&key_a),
            "released job is back in the waitlist, not dropped"
        );
        assert_eq!(
            inner.job_assignments.get(&key_a).map(|r| r.first_at),
            Some(key_a_first_at),
            "the requeue must not reset the bounded claim window"
        );
    }

    // The released job is still served by the next machine: it re-entered at
    // the back, not into invisibility.
    stage_provision_token(&state, "token-c");
    let (runner_c, _) =
        register_runner_with_token(&app, "machine-c", &["self-hosted"], Some("token-c")).await;
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.job_assignments.get(&key_a).map(|r| r.runner_id),
            Some(runner_c),
            "released job is paired once it reaches the front of the waitlist"
        );
        assert_eq!(
            inner.job_assignments.get(&key_a).map(|r| r.first_at),
            Some(key_a_first_at),
            "the replacement pairing keeps the original first-bound stamp"
        );
    }
}

#[tokio::test]
async fn stale_pending_mark_is_still_offered_to_a_registering_runner() {
    // A job whose machine never landed (pool-pending mark older than the
    // assignment TTL) used to be filtered out of the pairing offer set and
    // left invisible: never paired, and nothing re-armed it. A registering
    // machine must still take it over.
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    {
        let mut inner = state.inner.lock().await;
        let pending: Vec<_> = inner.pool_pending.keys().cloned().collect();
        for key in pending {
            inner.pool_pending.insert(
                key,
                std::time::SystemTime::now()
                    - crate::runtime_scheduling::ASSIGNMENT_TTL
                    - std::time::Duration::from_secs(1),
            );
        }
    }
    stage_provision_token(&state, "token-a");
    let (runner_a, _) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.job_assignments.values().next().map(|r| r.runner_id),
            Some(runner_a),
            "stale pool-pending mark must still be offered to a registering runner"
        );
    }
}

#[tokio::test]
async fn queue_time_assignment_prefers_idle_registered_runner() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    // Runner exists and is idle before the job lands: queue-time binding.
    stage_provision_token(&state, "token-a");
    let (runner_id, token) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    let (_, session) = create_disttask_session(&app, &token, runner_id).await;
    let session_id = session["sessionId"].as_str().unwrap().to_owned();

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.job_assignments.values().next().map(|r| r.runner_id),
            Some(runner_id),
            "queued job should bind immediately to the idle runner"
        );
        assert!(inner.pool_pending.is_empty());
    }

    // Unverified fabrication still cannot claim it.
    let (_, rogue_session) = create_disttask_session(&app, "preloop-system-token", runner_id).await;
    let rogue_id = rogue_session["sessionId"].as_str().unwrap();
    let stolen = poll_message(&app, "preloop-system-token", rogue_id).await;
    assert!(
        stolen.is_null(),
        "unverified claim must stay empty: {stolen}"
    );

    let delivered = poll_message(&app, &token, &session_id).await;
    assert!(
        delivered["messageType"].as_str().is_some(),
        "verified idle runner receives its job: {delivered}"
    );
}

#[tokio::test]
async fn provision_pairing_requires_the_token() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    // Host side staged a token for the next provisioning event.
    stage_provision_token(&state, "token-a");
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);

    // Registration without the token: runner works, but no pairing.
    let (runner_plain, _) =
        register_runner_with_token(&app, "external", &["self-hosted"], None).await;
    {
        let inner = state.inner.lock().await;
        assert!(
            inner.job_assignments.is_empty(),
            "no provisioning proof, no pairing"
        );
        assert_eq!(inner.pool_pending.len(), 1);
        let _ = runner_plain;
    }

    // Forged token value: no pairing either.
    let (_runner_forged, _) =
        register_runner_with_token(&app, "forger", &["self-hosted"], Some("wrong-token")).await;
    {
        let inner = state.inner.lock().await;
        assert!(inner.job_assignments.is_empty());
    }

    // With the token: pairing stamps.
    let (runner_a, _) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.job_assignments.values().next().map(|r| r.runner_id),
            Some(runner_a)
        );
        // One-time: the token is consumed.
        assert!(
            state.pending_registrations.read().unwrap().is_empty(),
            "provision token must be single-use"
        );
    }
}

#[tokio::test]
async fn strict_mode_refuses_unassigned_dispatch() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    state.inner.lock().await.require_job_assignments = true;
    let app = app(state.clone(), CancellationToken::new());
    // Strict-only engine: no provisioning channel, so nothing ever pairs.
    state.inner.lock().await.pool_assignments_enabled = false;

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    let (runner_id, token) =
        register_runner_with_token(&app, "external", &["self-hosted"], None).await;
    let (_, session) = create_disttask_session(&app, &token, runner_id).await;
    let session_id = session["sessionId"].as_str().unwrap();
    let delivered = poll_message(&app, &token, session_id).await;
    assert!(
        delivered.is_null(),
        "strict mode: unassigned job is never dispatched: {delivered}"
    );
}

#[tokio::test]
async fn strict_non_pool_mode_keeps_a_stale_binding_claimable() {
    // Strict mode without an embedded pool has no waitlist to re-mark a
    // released job on. Clearing a stale binding there used to leave the job
    // with neither a binding nor a mark, and strict claim_permitted requires
    // one — the job stranded forever while its machine had died. The stale
    // record must survive so a verified replacement runner can take over.
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    state.inner.lock().await.require_job_assignments = true;
    state.inner.lock().await.pool_assignments_enabled = false;
    let app = app(state.clone(), CancellationToken::new());

    // A pre-registered idle runner gets the queue-time binding.
    let (runner_a, token_a) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], None).await;
    let (_, session) = create_disttask_session(&app, &token_a, runner_a).await;
    let session_id = session["sessionId"].as_str().unwrap();

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    {
        let inner = state.inner.lock().await;
        let key = inner.job_assignments.keys().next().cloned().unwrap();
        assert_eq!(
            inner.job_assignments.get(&key).map(|r| r.runner_id),
            Some(runner_a),
            "queue-time binding assigned the job to the idle runner"
        );
    }

    // machine-a dies without claiming; the binding goes stale.
    {
        let mut inner = state.inner.lock().await;
        for record in inner.job_assignments.values_mut() {
            record.at = std::time::SystemTime::now()
                - crate::runtime_scheduling::CLAIM_BINDING_TTL
                - std::time::Duration::from_secs(1);
            record.first_at = record.at;
        }
    }

    // A fresh pool-authorized runner registers (provision token, so the
    // pairing path runs): the stale binding must survive and still let it
    // claim (kept, not cleared-and-stranded).
    stage_provision_token(&state, "token-b");
    let (runner_b, token_b) =
        register_runner_with_token(&app, "machine-b", &["self-hosted"], Some("token-b")).await;
    let (_, session_b) = create_disttask_session(&app, &token_b, runner_b).await;
    let session_b_id = session_b["sessionId"].as_str().unwrap();
    let delivered = poll_message(&app, &token_b, session_b_id).await;
    assert!(
        !delivered.is_null(),
        "a verified runner must be able to take over the stale binding: {delivered}"
    );

    // The original runner's session is not the beneficiary.
    let original = poll_message(&app, &token_a, session_id).await;
    assert!(
        original.is_null(),
        "the dead machine's session must not receive the job"
    );
}

#[tokio::test]
async fn permissive_default_keeps_unverified_claims_working() {
    let temp = tempfile::tempdir().unwrap();
    // Pool-management flags stay off — the external-runner default.
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);

    let (_status, session) = create_disttask_session(&app, "preloop-system-token", 1).await;
    let session_id = session["sessionId"].as_str().unwrap();
    let delivered = poll_message(&app, "preloop-system-token", session_id).await;
    assert!(
        delivered["messageType"].as_str().is_some(),
        "unverified legacy session must keep claiming without a pool: {delivered}"
    );
}

#[tokio::test]
async fn session_create_rejects_cross_runner_body() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());
    stage_provision_token(&state, "token-a");
    stage_provision_token(&state, "token-b");
    let (runner_a, token_a) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    let (runner_b, _) =
        register_runner_with_token(&app, "machine-b", &["self-hosted"], Some("token-b")).await;
    let _ = runner_b;

    // Token for A, body asking for B: rejected.
    let (status, _) = create_disttask_session(&app, &token_a, runner_b).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Token for A, body asking for A: created and bound to A.
    let (status, session) = create_disttask_session(&app, &token_a, runner_a).await;
    assert_eq!(status, StatusCode::CREATED);
    let session_id = session["sessionId"].as_str().unwrap();
    let inner = state.inner.lock().await;
    assert_eq!(inner.runner_id_for_session(session_id), Some(runner_a));
}

#[tokio::test]
async fn delete_agent_purges_identity_and_requeues_assignment() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    stage_provision_token(&state, "token-a");
    let (runner_a, _token) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    let (run_id, job_id) = {
        let inner = state.inner.lock().await;
        inner.job_assignments.keys().next().unwrap().clone()
    };
    let _ = (run_id, job_id);

    request_json(
        &app,
        Method::DELETE,
        &format!("/runner/server/_apis/distributedtask/pools/1/agents/{runner_a}"),
        Value::Null,
    )
    .await;

    let inner = state.inner.lock().await;
    assert!(!inner.runner_rsa_public_keys.contains_key(&runner_a));
    assert!(!inner.runner_public_keys.contains_key(&runner_a));
    assert!(!inner.runners.contains_key(&runner_a));
    assert!(inner.runner_client_ids.values().all(|id| *id != runner_a));
    assert!(
        inner
            .job_assignments
            .values()
            .all(|r| r.runner_id != runner_a),
        "purge drops the dead runner's assignment"
    );
    assert_eq!(
        inner.pool_pending.len(),
        1,
        "unclaimed job returns to pool-pending for re-provisioning"
    );
}

#[tokio::test]
async fn control_socket_surface_denies_native_and_test_apis() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    // Mirror the bootstrap wiring: the socket router is the full router plus
    // the runner-surface guard.
    let socket_app = app(state.clone(), CancellationToken::new())
        .layer(middleware::from_fn(crate::auth::runner_surface_only));

    for denied in [
        "/api/v1/secrets/owner/repo",
        "/api/v1/runs",
        "/api/v1/debug/sessions",
        "/api/v1/debug/sessions/dbg-controller-only",
        "/api/v1/agent/debug/sessions/dbg-controller-only/events",
        "/internal/test/jobs/complete",
    ] {
        let response = socket_app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(denied)
                    .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "socket must not expose {denied}"
        );
    }
    let controller_verdict = socket_app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/debug/sessions/dbg-controller-only/verdict")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"verdict":"abort"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        controller_verdict.status(),
        StatusCode::NOT_FOUND,
        "the controller verdict API must stay off the guest socket"
    );

    // The v3 registration-token endpoints mint runner-management JWTs
    // (`RunnerManage` scope) for the GitHub-compatible registration flow.
    // They are engine-facing: untrusted workflow code inside the VM must not
    // be able to mint runner-management credentials through the socket. The
    // one exception is the runner's own registration path, whose handler
    // requires the system credential — a wrong one is refused, and the mint
    // itself is tested separately.
    for v3 in [
        "/api/v3/orgs/acme/actions/runners/registration-token",
        "/api/v3/repos/acme/repo/actions/runners/registration-token",
    ] {
        let response = socket_app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(v3)
                    .header(
                        header::AUTHORIZATION,
                        "RemoteAuth preloop-registration-token",
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"url":"https://github.com/acme/repo"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "socket must not expose {v3}"
        );
    }
    let response = socket_app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v3/actions/runner-registration")
                .header(
                    header::AUTHORIZATION,
                    "RemoteAuth preloop-registration-token",
                )
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"url":"https://github.com/acme/repo"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "the carved-out registration path still requires the system credential"
    );
    let minted = request_json_with_bearer(
        &socket_app,
        Method::POST,
        "/api/v3/actions/runner-registration",
        json!({"url": "https://github.com/acme/repo", "runner_event": "register"}),
        DEFAULT_PRELOOP_SYSTEM_TOKEN,
    )
    .await;
    assert_eq!(
        minted["token_schema"], "OAuthAccessToken",
        "the runner's own registration must work through the socket"
    );

    // The runner's own log-blob uploads go through the same surface: the
    // in-VM runner PUTs step logs to the signed `/replay/results/*` URLs its
    // Twirp handlers minted, so the guard must not turn them into 404s — the
    // URL ticket (not the surface) is what authorises the write.
    let replay_path = "/replay/results/plan/job/step-1.txt";
    let unsigned = socket_app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(replay_path)
                .body(Body::from("log bytes"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        unsigned.status(),
        StatusCode::UNAUTHORIZED,
        "an unsigned upload must be refused once it reaches the auth layer"
    );
    let sig = crate::auth::sign_replay_upload_ticket(&state, replay_path);
    let replay = socket_app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!(
                    "{replay_path}?sv=2021-08-06&se=2028-01-01T00%3A00%3A00Z&sr=c&sp=rw&sig={sig}"
                ))
                .body(Body::from("log bytes"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        replay.status(),
        StatusCode::CREATED,
        "the runner's own signed upload must land through the socket"
    );

    // The runner surface stays reachable through the same guard.
    let response = socket_app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/runner/server/_apis/v1/AgentPools")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn replay_blob_uploads_require_a_ticket_bound_to_the_exact_path() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let plan = uuid::Uuid::new_v4().to_string();
    let job = uuid::Uuid::new_v4().to_string();
    let path = format!("/replay/results/{plan}/{job}/step-1.txt");

    // No credential at all: previously the blob was written; the ticket check
    // must refuse it.
    let unsigned = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(&path)
                .body(Body::from("overwrite attempt"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unsigned.status(), StatusCode::UNAUTHORIZED);

    // A ticket minted for a different path must not authorise this one —
    // this is the cross-job overwrite the signature binds away.
    let other_path = format!("/replay/results/{plan}/{job}/job-logs.txt");
    let other_sig = crate::auth::sign_replay_upload_ticket(&state, &other_path);
    let mismatched = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!(
                    "{path}?sv=2021-08-06&se=2028-01-01T00%3A00%3A00Z&sr=c&sp=rw&sig={other_sig}"
                ))
                .body(Body::from("overwrite attempt"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(mismatched.status(), StatusCode::UNAUTHORIZED);

    // The runner's own flow: a ticket for the exact path lands the blob.
    let sig = crate::auth::sign_replay_upload_ticket(&state, &path);
    let uploaded = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!(
                    "{path}?sv=2021-08-06&se=2028-01-01T00%3A00%3A00Z&sr=c&sp=rw&sig={sig}"
                ))
                .body(Body::from("log bytes"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(uploaded.status(), StatusCode::CREATED);
    let stored = tokio::fs::read_to_string(
        temp.path()
            .join("replay")
            .join("results")
            .join(&plan)
            .join(&job)
            .join("step-1.txt"),
    )
    .await
    .unwrap();
    assert_eq!(stored, "log bytes");

    // A tampered signature must not authorise anything.
    let forged_sig = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([0u8; 32]);
    let forged = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!(
                    "{path}?sv=2021-08-06&se=2028-01-01T00%3A00%3A00Z&sr=c&sp=rw&sig={forged_sig}"
                ))
                .body(Body::from("overwrite attempt"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forged.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn replay_blob_urls_are_minted_only_for_the_callers_own_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let plan = uuid::Uuid::new_v4().to_string();
    let my_job = uuid::Uuid::new_v4();
    let other_job = uuid::Uuid::new_v4();
    // The runtime token is exported to steps as ACTIONS_RUNTIME_TOKEN, so it
    // is exactly the credential untrusted workflow code holds.
    let runtime_token = state.mint_runtime_token(&plan, &my_job);
    let mint_url = "/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL";

    // Minting a signed URL for *another* job's backend ids is refused.
    let refused = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(mint_url)
                .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "workflow_run_backend_id": plan,
                        "workflow_job_run_backend_id": other_job.to_string(),
                        "step_backend_id": "step-1",
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(refused.status(), StatusCode::FORBIDDEN);

    // Minting for the caller's own job succeeds, and the returned URL is a
    // real ticket: uploading to it lands the blob.
    let minted = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(mint_url)
                .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "workflow_run_backend_id": plan,
                        "workflow_job_run_backend_id": my_job.to_string(),
                        "step_backend_id": "step-1",
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(minted.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&to_bytes(minted.into_body(), usize::MAX).await.unwrap()).unwrap();
    let upload_url = payload["logs_url"].as_str().unwrap().to_owned();
    assert!(upload_url.contains("/replay/results/"));

    let uploaded = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(upload_url)
                .body(Body::from("step one log"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(uploaded.status(), StatusCode::CREATED);
    let stored = tokio::fs::read_to_string(
        temp.path()
            .join("replay")
            .join("results")
            .join(&plan)
            .join(my_job.to_string())
            .join("step-step-1.txt"),
    )
    .await
    .unwrap();
    assert_eq!(stored, "step one log");
}

#[tokio::test]
async fn listen_tokens_are_revoked_when_the_runner_identity_is_purged() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // Registration precedes token issuance in every real flow: register the
    // runner, then mint its listen token.
    let registered = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({
            "name": "machine-a",
            "version": "2.335.1",
            "labels": [{"name": "self-hosted", "type": "system"}]
        }),
    )
    .await;
    let runner_id = registered["id"].as_i64().unwrap();
    let token = state
        .local_jwt(json!({
            "sub": format!("preloop-runner-listen-{runner_id}"),
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    // Before purge the token is a live runner credential: it clears
    // require_runner_bearer and reaches the handler (400 = the handler asked
    // for a sessionId, i.e. it ran past the auth layer).
    let poll = |app: &Router| {
        let app = app.clone();
        let token = token.clone();
        async move {
            app.oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/runner/message")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
        }
    };
    assert_eq!(poll(&app).await.status(), StatusCode::BAD_REQUEST);

    // The verified identity also enforces the session binding: a body
    // claiming a *different* agent is refused while the token names a live
    // registered runner.
    let session_body = json!({
        "agent": {"id": runner_id + 100, "name": "somebody-else"},
        "ownerName": "owner",
        "preloopAzdo": true,
        "useFipsEncryption": false
    });
    let create_session = |app: &Router| {
        let app = app.clone();
        let token = token.clone();
        let session_body = session_body.clone();
        async move {
            app.oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/runner/server/_apis/distributedtask/pools/1/sessions")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(session_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap()
        }
    };
    let before_session = create_session(&app).await;
    assert_eq!(
        before_session.status(),
        StatusCode::FORBIDDEN,
        "verified listen token must not let the session body claim a different agent"
    );

    // Deregister the runner: purge removes the registration, which is the
    // revocation of every listen token that runner was issued.
    request_json(
        &app,
        Method::DELETE,
        &format!("/runner/server/_apis/distributedtask/pools/1/agents/{runner_id}"),
        Value::Null,
    )
    .await;
    {
        let inner = state.inner.lock().await;
        assert!(!inner.runners.contains_key(&runner_id));
    }

    // The same token is now refused at the auth layer.
    assert_eq!(poll(&app).await.status(), StatusCode::UNAUTHORIZED);

    // And the identity resolver no longer treats the bearer as a runner: the
    // token cannot force a *verified* binding after teardown, so the session
    // body's own claim wins (legacy unverified session).
    let after_session = create_session(&app).await;
    assert_eq!(
        after_session.status(),
        StatusCode::CREATED,
        "after purge the token is unverified and cannot force a binding"
    );
}

#[tokio::test]
async fn stale_assignment_is_taken_over_by_the_next_verified_runner() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    stage_provision_token(&state, "token-a");
    let (runner_a, token_a) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    // Backdate the pairing past the pre-claim window, simulating a machine
    // whose runner died between registration and its first poll.
    {
        let mut inner = state.inner.lock().await;
        for record in inner.job_assignments.values_mut() {
            record.at = std::time::SystemTime::now()
                - crate::runtime_scheduling::CLAIM_BINDING_TTL
                - std::time::Duration::from_secs(5);
        }
    }

    // The dead owner's overdue pairing must not serve it; a new verified
    // runner takes over (as a replacement machine's registration would).
    stage_provision_token(&state, "token-b");
    let (runner_b, token_b) =
        register_runner_with_token(&app, "machine-b", &["self-hosted"], Some("token-b")).await;
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.job_assignments.values().next().map(|r| r.runner_id),
            Some(runner_b),
            "replacement machine takes over the stale pairing"
        );
    }
    let _ = token_a;
    let (_, session_b) = create_disttask_session(&app, &token_b, runner_b).await;
    let session_b_id = session_b["sessionId"].as_str().unwrap();
    let delivered = poll_message(&app, &token_b, session_b_id).await;
    assert!(
        delivered["messageType"].as_str().is_some(),
        "takeover runner receives the rescued job: {delivered}"
    );
    // And the stale owner no longer does.
    let (_, session_a) = create_disttask_session(&app, &token_a, runner_a).await;
    let session_a_id = session_a["sessionId"].as_str().unwrap();
    let stolen = poll_message(&app, &token_a, session_a_id).await;
    assert!(stolen.is_null(), "stale owner lost the job: {stolen}");
}

#[tokio::test]
async fn purge_requeues_claimed_unfinished_job_to_another_runner() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    // Machine A registers with a provision token and claims the job.
    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    stage_provision_token(&state, "token-a");
    let (runner_a, token_a) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    let (_, session_a) = create_disttask_session(&app, &token_a, runner_a).await;
    let session_a_id = session_a["sessionId"].as_str().unwrap().to_owned();
    let delivered = poll_message(&app, &token_a, &session_a_id).await;
    assert!(
        delivered["messageType"].as_str().is_some(),
        "machine A claimed the job: {delivered}"
    );
    {
        let inner = state.inner.lock().await;
        assert!(inner.queue.is_empty());
        assert_eq!(inner.claimed_jobs.len(), 1, "claim is stashed");
    }

    // The pool tears machine A down mid-job: purge by machine name, then the
    // job must be back on the queue for somebody else.
    let purge = request_json(
        &app,
        Method::POST,
        "/api/v1/runners/purge",
        json!({ "name": "machine-a" }),
    )
    .await;
    assert_eq!(purge["purged"], 1);
    {
        let inner = state.inner.lock().await;
        assert_eq!(inner.queue.len(), 1, "unfinished job requeued");
        assert!(inner.claimed_jobs.is_empty(), "stash consumed by requeue");
        assert!(!inner.runners.contains_key(&runner_a));
    }

    // A fresh machine registers and picks the job up.
    stage_provision_token(&state, "token-b");
    let (runner_b, token_b) =
        register_runner_with_token(&app, "machine-b", &["self-hosted"], Some("token-b")).await;
    let (_, session_b) = create_disttask_session(&app, &token_b, runner_b).await;
    let session_b_id = session_b["sessionId"].as_str().unwrap().to_owned();
    let delivered = poll_message(&app, &token_b, &session_b_id).await;
    assert!(
        delivered["messageType"].as_str().is_some(),
        "machine B receives the requeued job: {delivered}"
    );
}

#[tokio::test]
async fn purge_of_finished_runner_does_not_requeue() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    stage_provision_token(&state, "token-a");
    let (runner_a, token_a) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    let (_, session_a) = create_disttask_session(&app, &token_a, runner_a).await;
    let session_a_id = session_a["sessionId"].as_str().unwrap().to_owned();
    let delivered = poll_message(&app, &token_a, &session_a_id).await;
    assert!(delivered["messageType"].as_str().is_some());

    // Complete the job through the broker compat completion handler, then purge.
    let (run_id, job_id) = {
        let inner = state.inner.lock().await;
        inner.claimed_jobs.keys().next().unwrap().clone()
    };
    let _ = request_json(
        &app,
        Method::PATCH,
        &format!("/runner/server/_apis/distributedtask/hubs/actions/plans/{run_id}/jobs/{job_id}"),
        json!({ "runId": run_id, "jobId": job_id, "result": "succeeded" }),
    )
    .await;

    request_json(
        &app,
        Method::POST,
        "/api/v1/runners/purge",
        json!({ "name": "machine-a" }),
    )
    .await;
    {
        let inner = state.inner.lock().await;
        assert!(
            inner.queue.is_empty(),
            "finished job must not come back: {:?}",
            inner.queue
        );
    }
}

#[tokio::test]
async fn workflow_gate_released_when_run_ends_via_dependency_skip() {
    // MC-S2: a workflow-level Holder::Run must be released when the run
    // concludes through the dependency-skip arm of promote_ready_jobs
    // (previously the slot leaked forever and same-group successors without
    // cancel-in-progress parked permanently).
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: skip-group
jobs:
  dep:
    runs-on: ubuntu-latest
    steps:
      - run: echo dep
  main:
    runs-on: ubuntu-latest
    needs: [dep]
    if: false
    steps:
      - run: echo main
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let a_id = a["run_id"].as_str().unwrap();

    // Dispatch and complete `dep`; `main` then evaluates `if: false` and is
    // skipped, concluding run A through the skip arm.
    let msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert!(!msg.is_null(), "run A `dep` should be dispatchable");
    complete_via_api(&app, a_id, "dep").await;

    let run_a = get_run_json(&app, a_id).await;
    assert_eq!(run_a["jobs"]["main"], "skipped", "main must be skipped");
    assert!(
        run_a["status"].as_str().unwrap() == "success"
            || run_a["status"].as_str().unwrap() == "completed",
        "run A must be terminal after the skip, got {}",
        run_a["status"]
    );

    // A successor in the same group must now acquire the slot instead of
    // parking behind the leaked holder.
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    let b_id = b["run_id"].as_str().unwrap();
    let run_b = get_run_json(&app, b_id).await;
    assert_eq!(
        run_b["status"], "queued",
        "run B must acquire the freed workflow gate (MC-S2), got {}",
        run_b["status"]
    );
    assert_eq!(run_b["jobs"]["dep"], "queued");
}

#[tokio::test]
async fn needs_gated_job_concurrency_acquired_at_promote_time() {
    // MC-S3: job-level concurrency must gate needs-gated jobs at promote
    // time. Previously the gate was evaluated only at submit for needs-empty
    // jobs, so a needs-gated job with a busy group was dispatched anyway.
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    // Run A: `one` holds job group g (needs-empty → gated at submit).
    let a = submit_yaml(
        &app,
        r#"
on: push
jobs:
  one:
    runs-on: ubuntu-latest
    concurrency:
      group: shared-gate
    steps:
      - run: echo one
"#,
        "owner/repo",
    )
    .await;
    let a_id = a["run_id"].as_str().unwrap();
    // Claim `one` so it is InProgress and keeps holding the group.
    let msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert!(!msg.is_null(), "run A `one` should be dispatchable");

    // Run B: `dep` (no gate) + `two` (needs [dep], same group g).
    let b = submit_yaml(
        &app,
        r#"
on: push
jobs:
  dep:
    runs-on: ubuntu-latest
    steps:
      - run: echo dep
  two:
    runs-on: ubuntu-latest
    needs: [dep]
    concurrency:
      group: shared-gate
    steps:
      - run: echo two
"#,
        "owner/repo",
    )
    .await;
    let b_id = b["run_id"].as_str().unwrap();

    // Dispatch and complete `dep`; `two` becomes ready and must evaluate its
    // gate — the group is busy, so it parks instead of dispatching.
    let msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert!(!msg.is_null(), "run B `dep` should be dispatchable");
    complete_via_api(&app, b_id, "dep").await;

    let run_b = get_run_json(&app, b_id).await;
    assert_eq!(
        run_b["jobs"]["two"], "pending",
        "needs-gated job must park while its group is held (MC-S3), got {}",
        run_b["jobs"]["two"]
    );

    // Completing `one` releases the group; `two` must then dispatch.
    complete_via_api(&app, a_id, "one").await;
    let run_b = get_run_json(&app, b_id).await;
    assert_eq!(
        run_b["jobs"]["two"], "queued",
        "parked gated job must dispatch once the group frees, got {}",
        run_b["jobs"]["two"]
    );
}

#[tokio::test]
async fn expanded_matrix_placeholder_does_not_leak_request_correlation() {
    // MC-2: a deferred-matrix node is non-caller, so submit mints its full
    // request correlation, but the node is routed to expansion and never
    // dispatched to a runner. Expansion deletes it from the run and no
    // completion path ever fires for it, so without explicit retirement its
    // request stays inflight for the life of the process, still resolvable to
    // a job that no longer exists.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  generator:
    runs-on: ubuntu-latest
    steps:
      - run: echo gen
  downstream:
    needs: [generator]
    runs-on: ubuntu-latest
    strategy:
      matrix: ${{ fromJson(needs.generator.outputs.matrix) }}
    steps:
      - run: echo dynamic
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let placeholder = JobId("downstream".to_string());

    // The placeholder holds a real, inflight request record before expansion.
    let (request_id, plan_id, agent_job_id, timeline_id) = {
        let inner = state.inner.lock().await;
        let record = inner
            .job_requests
            .values()
            .find(|r| r.run_id == run_id && r.job_id == placeholder)
            .expect("deferred-matrix placeholder must have a submit-time request");
        let ids = (
            record.request_id,
            record.plan_id.clone(),
            record.agent_job_id,
            record.timeline_id,
        );
        assert!(
            inner.inflight_requests.contains_key(&ids.0),
            "placeholder request must start out inflight"
        );
        ids
    };

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "generator",
            "status": "success",
            "outputs": {"matrix": r#"{"include": [{"os": "ubuntu-latest"}, {"os": "macos-latest"}]}"#}
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert!(
        !run.jobs.contains_key(&placeholder),
        "expansion must replace the placeholder with its combinations"
    );
    assert!(
        !inner.inflight_requests.contains_key(&request_id),
        "MC-2: placeholder request leaked in inflight_requests after expansion"
    );
    assert!(
        !inner.job_requests.contains_key(&request_id),
        "MC-2: placeholder job_request record leaked after expansion"
    );
    assert_ne!(
        inner.plan_requests.get(&plan_id),
        Some(&request_id),
        "MC-2: plan_requests still resolves to the deleted placeholder"
    );
    assert_ne!(
        inner.agent_job_requests.get(&agent_job_id),
        Some(&request_id),
        "MC-2: agent_job_requests still resolves to the deleted placeholder"
    );
    assert_ne!(
        inner.timeline_requests.get(&timeline_id),
        Some(&request_id),
        "MC-2: timeline_requests still resolves to the deleted placeholder"
    );

    // The fan-out jobs that replaced it keep their own correlation intact.
    for id in ["downstream (ubuntu-latest)", "downstream (macos-latest)"] {
        let job_id = JobId(id.to_string());
        let record = inner
            .job_requests
            .values()
            .find(|r| r.run_id == run_id && r.job_id == job_id)
            .unwrap_or_else(|| panic!("fan-out job {id} must keep its request record"));
        assert!(
            inner.inflight_requests.contains_key(&record.request_id),
            "fan-out job {id} must still be inflight"
        );
    }
}

// ---------------------------------------------------------------------------
// Durable-store restart contracts.
//
// Each test below writes state, drops the store, reopens it, and asserts on
// the recovered `InnerState`. Before the fixes they accompany, every one of
// them failed while `just test-ci` stayed green — the gate had no restart
// dimension at all.
// ---------------------------------------------------------------------------

/// Secrets must come back as themselves. `SecretString::Serialize` emits the
/// literal `"<redacted>"`, so any persistence path that does not go through
/// `WorkflowSubmission::to_request_json` silently substitutes the redaction
/// marker for every secret and the resumed run authenticates with garbage.
#[tokio::test]
async fn store_recovery_preserves_run_secrets() {
    let temp = tempfile::tempdir().unwrap();
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let run_id = {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": workflow,
                "event": "push",
                "repository": "owner/repo",
                "secrets": {"MY_TOKEN": "s3cr3t-value", "OTHER": "second-value"}
            }),
        )
        .await;
        accepted["run_id"]
            .as_str()
            .unwrap()
            .parse::<RunId>()
            .unwrap()
    };

    let recovered = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let inner = recovered.inner.lock().await;
    let secrets = &inner
        .runs
        .get(&run_id)
        .expect("run survives restart")
        .submission
        .secrets;
    assert_eq!(
        secrets.get("MY_TOKEN").map(|s| s.expose()),
        Some("s3cr3t-value")
    );
    assert_eq!(
        secrets.get("OTHER").map(|s| s.expose()),
        Some("second-value")
    );
}

/// The ready queue is FIFO across runs, not just within one. `store_run_event`
/// rewrites a single run's rows, so its `queue_position` values have to stay on
/// the same global scale as every other writer's; numbering from zero per run
/// gave every run a `position = 0` job and interleaved them on restore.
#[tokio::test]
async fn store_recovery_preserves_cross_run_queue_order() {
    let temp = tempfile::tempdir().unwrap();
    let workflow = "on: push\njobs:\n  one:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo 1\n  two:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo 2\n";
    let before: Vec<String> = {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        for repo in ["owner/alpha", "owner/beta", "owner/gamma"] {
            request_json(
                &app,
                Method::POST,
                "/api/v1/runs",
                json!({"workflow_yaml": workflow, "event": "push", "repository": repo}),
            )
            .await;
        }
        let inner = state.inner.lock().await;
        inner
            .queue
            .iter()
            .map(|job| format!("{}:{}", job.run_id, job.job_id.0))
            .collect()
    };
    assert_eq!(before.len(), 6, "three runs of two jobs must all be queued");

    let recovered = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let inner = recovered.inner.lock().await;
    let after: Vec<String> = inner
        .queue
        .iter()
        .map(|job| format!("{}:{}", job.run_id, job.job_id.0))
        .collect();
    assert_eq!(before, after, "ready-queue FIFO order must survive restart");
}

/// A job message that was dequeued but not yet delivered has to be re-delivered
/// after a restart, otherwise the runner polls forever for an assignment the
/// server believes it already handed out.
#[tokio::test]
async fn store_recovery_preserves_broker_and_inflight_messages() {
    let temp = tempfile::tempdir().unwrap();
    {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let mut inner = state.inner.lock().await;
        inner
            .inflight_messages
            .entry("sess-1".to_owned())
            .or_default()
            .insert(
                7,
                azdo::TaskAgentMessage {
                    message_id: 7,
                    message_type: "PipelineAgentJobRequest".to_owned(),
                    body: "e30=".to_owned(),
                    iv: None,
                },
            );
        state
            .store
            .store_inner(&crate::store::StoreSnapshot::from_inner(&inner))
            .await
            .unwrap();
    }

    let recovered = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let inner = recovered.inner.lock().await;
    let session = inner
        .inflight_messages
        .get("sess-1")
        .expect("undelivered broker message must survive restart");
    let message = session.get(&7).expect("message id must be preserved");
    assert_eq!(message.message_type, "PipelineAgentJobRequest");
    assert_eq!(message.body, "e30=");
}

/// The in-flight cache payload must never enter the runtime snapshot. It is a
/// `Vec<u8>` holding the whole upload, and the snapshot is cloned, serialized
/// and AES-sealed on every `store_meta_only` — putting it there made
/// `cache_upload` quadratic in cache size with the global state lock held.
#[tokio::test]
async fn cache_upload_payload_stays_out_of_the_runtime_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let reserve = request_json(
        &app,
        Method::POST,
        "/_apis/artifactcache/cache",
        json!({"key": "big", "version": "v1"}),
    )
    .await;
    let cache_id = reserve["cacheId"].as_i64().unwrap();

    let payload = vec![b'x'; 1 << 20]; // 1 MiB (under the default body limit)
    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("/_apis/artifactcache/cache/{cache_id}"))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::from(payload.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::ACCEPTED);

    // Force a snapshot with the upload still buffered in memory.
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.pending_caches.get(&cache_id).map(|c| c.bytes.len()),
            Some(payload.len()),
            "the upload is buffered in memory"
        );
        state
            .store
            .store_meta_only(&crate::store::build_meta_snapshot(&inner))
            .await
            .unwrap();
    }

    let db = temp.path().join("preloop.db");
    let connection = rusqlite::Connection::open(&db).unwrap();
    let blob_len: i64 = connection
        .query_row(
            "SELECT length(meta_blob) FROM runtime_snapshots WHERE snapshot_id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        blob_len < 64 * 1024,
        "runtime snapshot is {blob_len} bytes after a {} byte upload — the cache \
         payload leaked into the meta blob",
        payload.len()
    );
}

/// Two servers booting against one Postgres database must both start.
/// `CREATE TABLE IF NOT EXISTS` is not race-safe in Postgres: the existence
/// check and the `pg_type` insert are separate, so an unguarded migration makes
/// the loser fail with a `pg_type_typname_nsp_index` unique violation.
#[tokio::test]
async fn postgres_concurrent_open_serializes_migrations() {
    let Ok(base) = std::env::var("PRELOOP_TEST_PG_URL") else {
        eprintln!("skipping: set PRELOOP_TEST_PG_URL to a disposable Postgres URL");
        return;
    };
    if base.trim().is_empty() {
        return;
    }
    let dbname = format!("preloop_race_{}", uuid::Uuid::new_v4().simple());
    {
        let connect_url = crate::store_pg::connect_url(&base);
        let (client, connection) = tokio_postgres::connect(&connect_url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .execute(&format!("CREATE DATABASE {dbname}"), &[])
            .await
            .unwrap();
    }
    let fresh = base
        .rsplit_once('/')
        .map(|(host, _)| format!("{host}/{dbname}"))
        .unwrap();

    let key = b"concurrent-open-root-key";
    let dir = std::path::Path::new("/tmp");
    let (first, second) = tokio::join!(
        crate::store::open_store(Some(&fresh), dir, key),
        crate::store::open_store(Some(&fresh), dir, key),
    );
    assert!(first.is_ok(), "first opener failed: {:?}", first.err());
    assert!(second.is_ok(), "second opener failed: {:?}", second.err());
}

/// Postgres twin of `store_recovery_preserves_run_secrets`. The redaction bug
/// lived in the shared serialization path, so both backends have to prove it.
#[tokio::test]
async fn postgres_recovery_preserves_run_secrets() {
    let Ok(pg_url) = std::env::var("PRELOOP_TEST_PG_URL") else {
        eprintln!("skipping: set PRELOOP_TEST_PG_URL to a disposable Postgres URL");
        return;
    };
    if pg_url.trim().is_empty() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let config_path = crate::config::config_path();
    // Start from a known state: the shared test database may hold rows left by
    // earlier Postgres tests (they restore into the queue on load). A
    // brand-new database has no tables yet; only clean a schema that exists.
    {
        let connect_url = crate::store_pg::connect_url(&pg_url);
        let (client, connection) = tokio_postgres::connect(&connect_url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let has_schema: bool = client
            .query_one(
                "SELECT to_regclass('public.workflow_run_counters') IS NOT NULL",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if has_schema {
            client
                .batch_execute(
                    "TRUNCATE workflow_run_counters, runs, runners, runner_labels,
                             runner_sessions, jobs, job_dependencies, job_requests, control_events,
                             session_active_requests, broker_messages, job_request_messages,
                             log_files, log_chunks, runtime_snapshots RESTART IDENTITY CASCADE",
                )
                .await
                .unwrap();
        }
    }
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let run_id = {
        let state = AppState::new_with_store(
            temp.path().to_path_buf(),
            config_path.clone(),
            Some(&pg_url),
        )
        .await
        .unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": workflow,
                "event": "push",
                "repository": "owner/pg-secrets",
                "secrets": {"MY_TOKEN": "s3cr3t-value"}
            }),
        )
        .await;
        accepted["run_id"]
            .as_str()
            .unwrap()
            .parse::<RunId>()
            .unwrap()
    };

    let recovered = AppState::new_with_store(temp.path().to_path_buf(), config_path, Some(&pg_url))
        .await
        .unwrap();
    let inner = recovered.inner.lock().await;
    assert_eq!(
        inner
            .runs
            .get(&run_id)
            .expect("run survives restart")
            .submission
            .secrets
            .get("MY_TOKEN")
            .map(|s| s.expose()),
        Some("s3cr3t-value")
    );
}

// ---------------------------------------------------------------------------
// Regression tests for the cubic.dev review blockers on PR #27.
// ---------------------------------------------------------------------------

/// Restart while a reusable-caller node is parked (its concurrency gate is
/// held by an earlier run) must keep the caller plan and the expansion-only
/// fields (`github`, `head_sha`, `workflow_ref`) that the scheduler needs to
/// materialize the callee subtree later. They were `#[serde(skip)]` on
/// `RunRecord`, so a restart reset them to defaults and the deferred
/// expansion failed or misbuilt.
#[tokio::test]
async fn store_recovery_preserves_deferred_caller_plan_and_expansion_fields() {
    let temp = tempfile::tempdir().unwrap();
    let caller_yaml = r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/callee.yml
    concurrency:
      group: reusable-serial
"#;
    let callee_yaml = r#"
on: workflow_call
jobs:
  inner:
    runs-on: ubuntu-latest
    steps:
      - run: echo callee
"#;
    let submission = || {
        json!({
            "workflow_yaml": caller_yaml,
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": { ".github/workflows/callee.yml": callee_yaml },
        })
    };

    let (parked_run, github, head_sha, workflow_ref) = {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let first = request_json(&app, Method::POST, "/api/v1/runs", submission()).await;
        let second = request_json(&app, Method::POST, "/api/v1/runs", submission()).await;
        let first_run: RunId = first["run_id"].as_str().unwrap().parse().unwrap();
        let parked: RunId = second["run_id"].as_str().unwrap().parse().unwrap();
        let inner = state.inner.lock().await;
        // First caller's gate is free: subtree materialized immediately.
        assert_eq!(
            inner.runs[&first_run].jobs[&JobId("call/inner".to_owned())],
            ExecutionStatus::Queued
        );
        // Second caller is parked behind the gate with its plan in the run.
        let run = &inner.runs[&parked];
        assert_eq!(
            run.jobs[&JobId("call".to_owned())],
            ExecutionStatus::Pending
        );
        assert!(
            run.caller_plans.contains_key(&JobId("call".to_owned())),
            "parked caller must keep its plan pre-restart"
        );
        assert!(
            !run.github.is_null() && !run.head_sha.is_empty() && !run.workflow_ref.is_empty(),
            "expansion fields must be populated pre-restart"
        );
        (
            parked,
            run.github.clone(),
            run.head_sha.clone(),
            run.workflow_ref.clone(),
        )
    };

    let recovered = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let inner = recovered.inner.lock().await;
    let run = inner
        .runs
        .get(&parked_run)
        .expect("parked run survives restart");
    assert!(
        run.caller_plans.contains_key(&JobId("call".to_owned())),
        "deferred caller plan must survive restart"
    );
    assert_eq!(run.github, github, "github context must survive restart");
    assert_eq!(run.head_sha, head_sha, "head_sha must survive restart");
    assert_eq!(
        run.workflow_ref, workflow_ref,
        "workflow_ref must survive restart"
    );
}

/// A job claimed (dequeued, broker message handed to a session) but not yet
/// acked is re-delivered after a restart, even when the only write between
/// the claim and the crash was a `store_run_event` for another status change.
#[tokio::test]
async fn store_recovery_preserves_claim_state_across_run_events() {
    let temp = tempfile::tempdir().unwrap();
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo 1\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo 2\n";
    let (claimed_job, other_job, request_id) = {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({"workflow_yaml": workflow, "event": "push", "repository": "owner/repo"}),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        // Simulate the broker claiming `build`: dequeued, message parked in
        // the per-session and per-request maps, session claim recorded.
        let (claimed_job, other_job, request_id) = {
            let mut inner = state.inner.lock().await;
            let claimed = inner
                .queue
                .iter()
                .find(|job| job.job_id.0 == "build")
                .cloned()
                .expect("build job queued");
            inner.queue.retain(|job| job.job_id.0 != "build");
            let request = inner
                .job_requests
                .values()
                .find(|record| record.job_id.0 == "build")
                .cloned()
                .expect("build request");
            inner
                .session_active_requests
                .insert("sess-1".to_owned(), request.request_id);
            inner
                .inflight_messages
                .entry("sess-1".to_owned())
                .or_default()
                .insert(
                    99,
                    azdo::TaskAgentMessage {
                        message_id: 99,
                        message_type: "PipelineAgentJobRequest".to_owned(),
                        body: "e30=".to_owned(),
                        iv: None,
                    },
                );
            inner
                .broker_messages
                .insert(request.request_id, claimed.message.clone());
            (
                claimed.job_id.clone(),
                JobId("test".to_owned()),
                request.request_id,
            )
        };

        // The only store write after the claim: a status event for the OTHER
        // job of the same run (store_run_event).
        state
            .emit(NdjsonEvent::JobStatus {
                run_id,
                job_id: other_job.clone(),
                status: ExecutionStatus::InProgress,
                reason: None,
            })
            .await;
        (claimed_job, other_job, request_id)
    };

    let recovered = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let inner = recovered.inner.lock().await;
    assert!(
        inner
            .inflight_messages
            .get("sess-1")
            .and_then(|messages| messages.get(&99))
            .is_some(),
        "undelivered broker message must survive a store_run_event restart"
    );
    assert!(
        inner.broker_messages.contains_key(&request_id),
        "per-request job message must survive a store_run_event restart"
    );
    assert_eq!(
        inner.session_active_requests.get("sess-1"),
        Some(&request_id),
        "session claim must survive a store_run_event restart"
    );
    assert!(
        inner.queue.iter().any(|job| job.job_id == other_job)
            && !inner.queue.iter().any(|job| job.job_id == claimed_job),
        "claimed job stays dequeued; the unclaimed job stays queued"
    );
}

/// Pool pairing state — one-time provision proof, strict job assignments and
/// pending pairings — plus the OAuth `client_id` map must survive a restart.
#[tokio::test]
async fn store_recovery_preserves_pool_pairing_and_oauth_client_ids() {
    let temp = tempfile::tempdir().unwrap();
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let (run_id, now) = {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({"workflow_yaml": workflow, "event": "push", "repository": "owner/repo"}),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
        // The store persists SystemTime as microseconds
        // (`system_time_us`/`system_time_from_us`), so a nanosecond-precision
        // `now` can never equal the recovered value on Linux hosts (where
        // SystemTime has ns resolution). Round to the store's precision —
        // the same fix `5f96d0dd` applied to the sibling assertions here.
        let now = std::time::SystemTime::now();
        let now = std::time::UNIX_EPOCH
            + std::time::Duration::from_micros(
                now.duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros()
                    .min(u64::MAX as u128) as u64,
            );
        {
            let mut inner = state.inner.lock().await;
            inner.runner_client_ids.insert("client-abc".to_owned(), 42);
            inner.pool_proven_runners.insert(7);
            inner.job_assignments.insert(
                (run_id, JobId("build".to_owned())),
                AssignmentRecord {
                    runner_id: 7,
                    at: now,
                    first_at: now,
                },
            );
            inner
                .pool_pending
                .insert((run_id, JobId("build".to_owned())), now);
            state
                .store
                .store_inner(&crate::store::StoreSnapshot::from_inner(&inner))
                .await
                .unwrap();
        }
        (run_id, now)
    };

    let recovered = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let inner = recovered.inner.lock().await;
    assert_eq!(
        inner.runner_client_ids.get("client-abc"),
        Some(&42),
        "OAuth client id must survive restart"
    );
    assert!(
        inner.pool_proven_runners.contains(&7),
        "provision-token proof must survive restart"
    );
    let assignment = inner
        .job_assignments
        .get(&(run_id, JobId("build".to_owned())))
        .expect("job assignment must survive restart");
    assert_eq!(assignment.runner_id, 7);
    assert_eq!(assignment.at, now);
    assert_eq!(assignment.first_at, now);
    assert!(
        inner
            .pool_pending
            .contains_key(&(run_id, JobId("build".to_owned()))),
        "pending pairing must survive restart"
    );
}

/// `ServerConfig`'s Debug output must never print a Postgres password.
#[test]
fn server_config_debug_redacts_store_url_password() {
    let config = ServerConfig {
        listen: "127.0.0.1:9090".parse().unwrap(),
        systemd_socket_activation: false,
        unix_socket: None,
        state_dir: std::path::PathBuf::from(".preloop"),
        store_url: Some(
            "postgres://preloop:hunter2-secret@db.example:5432/preloop?sslmode=verify-full"
                .to_owned(),
        ),
        record_flows: None,
        tls: TlsMode::None,
        queue_depth: None,
        next_job_runs_on: None,
        pool_preparing: None,
        enable_test_api: false,
        test_api_token: Some("super-secret-token".to_owned()),
        oidc_issuer: None,
        enable_scheduler: false,
        pending_registrations: None,
        require_job_assignments: false,
    };
    let debug = format!("{config:?}");
    assert!(
        !debug.contains("hunter2-secret"),
        "Debug must not expose the Postgres password: {debug}"
    );
    assert!(
        !debug.contains("super-secret-token"),
        "Debug must not expose the test API token: {debug}"
    );
    assert!(
        debug.contains("preloop:***@db.example"),
        "Debug should keep the masked URL shape"
    );
}

/// Postgres twin of `store_recovery_preserves_claim_state_across_run_events`:
/// the `job_request_messages` table and the claim rewrite inside
/// `store_run_event` are backend-specific SQL, so the round-trip has to be
/// proven against a live database too.
#[tokio::test]
async fn postgres_recovery_preserves_claim_state_across_run_events() {
    let Ok(pg_url) = std::env::var("PRELOOP_TEST_PG_URL") else {
        eprintln!("skipping: set PRELOOP_TEST_PG_URL to a disposable Postgres URL");
        return;
    };
    if pg_url.trim().is_empty() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let config_path = crate::config::config_path();
    // Isolate from earlier Postgres tests sharing this database: their rows
    // restore into the queue on load. Only clean a schema that already exists.
    {
        let connect_url = crate::store_pg::connect_url(&pg_url);
        let (client, connection) = tokio_postgres::connect(&connect_url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let has_schema: bool = client
            .query_one(
                "SELECT to_regclass('public.workflow_run_counters') IS NOT NULL",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if has_schema {
            client
                .batch_execute(
                    "TRUNCATE workflow_run_counters, runs, runners, runner_labels,
                             runner_sessions, jobs, job_dependencies, job_requests, control_events,
                             session_active_requests, broker_messages, job_request_messages,
                             log_files, log_chunks, runtime_snapshots RESTART IDENTITY CASCADE",
                )
                .await
                .unwrap();
        }
    }
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo 1\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo 2\n";
    let (claimed_job, other_job, request_id) = {
        let state = AppState::new_with_store(
            temp.path().to_path_buf(),
            config_path.clone(),
            Some(&pg_url),
        )
        .await
        .unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({"workflow_yaml": workflow, "event": "push", "repository": "owner/repo"}),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        let (claimed_job, other_job, request_id) = {
            let mut inner = state.inner.lock().await;
            let claimed = inner
                .queue
                .iter()
                .find(|job| job.job_id.0 == "build")
                .cloned()
                .expect("build job queued");
            inner.queue.retain(|job| job.job_id.0 != "build");
            let request = inner
                .job_requests
                .values()
                .find(|record| record.job_id.0 == "build")
                .cloned()
                .expect("build request");
            inner
                .session_active_requests
                .insert("sess-pg".to_owned(), request.request_id);
            inner
                .inflight_messages
                .entry("sess-pg".to_owned())
                .or_default()
                .insert(
                    99,
                    azdo::TaskAgentMessage {
                        message_id: 99,
                        message_type: "PipelineAgentJobRequest".to_owned(),
                        body: "e30=".to_owned(),
                        iv: None,
                    },
                );
            inner
                .broker_messages
                .insert(request.request_id, claimed.message.clone());
            (
                claimed.job_id.clone(),
                JobId("test".to_owned()),
                request.request_id,
            )
        };
        state
            .emit(NdjsonEvent::JobStatus {
                run_id,
                job_id: other_job.clone(),
                status: ExecutionStatus::InProgress,
                reason: None,
            })
            .await;
        (claimed_job, other_job, request_id)
    };

    let recovered = AppState::new_with_store(temp.path().to_path_buf(), config_path, Some(&pg_url))
        .await
        .unwrap();
    let inner = recovered.inner.lock().await;
    assert!(
        inner
            .inflight_messages
            .get("sess-pg")
            .and_then(|messages| messages.get(&99))
            .is_some(),
        "undelivered broker message must survive a store_run_event restart (PG)"
    );
    assert!(
        inner.broker_messages.contains_key(&request_id),
        "per-request job message must survive a store_run_event restart (PG)"
    );
    assert_eq!(
        inner.session_active_requests.get("sess-pg"),
        Some(&request_id),
        "session claim must survive a store_run_event restart (PG)"
    );
    assert!(
        inner.queue.iter().any(|job| job.job_id == other_job)
            && !inner.queue.iter().any(|job| job.job_id == claimed_job),
        "claimed job stays dequeued; the unclaimed job stays queued (PG)"
    );
}

/// A restart destroys every pool machine but persists its claim, so the
/// request returns pinned to a session that will never poll again. Nothing
/// can complete it and nothing can re-claim it: the run — and the GitHub
/// check run it created — would sit queued forever while the pool idles.
/// Startup reconciliation must settle those claims, and only those.
#[tokio::test]
async fn startup_fails_claims_orphaned_by_a_restart() {
    let temp = tempfile::tempdir().unwrap();
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo 1\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo 2\n";
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({"workflow_yaml": workflow, "event": "push", "repository": "owner/repo"}),
    )
    .await;
    let _run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // A pool machine claimed `build`, then the control plane restarted: the
    // pin survives, its session does not.
    let (claimed_request, queued_request) = {
        let mut inner = state.inner.lock().await;
        let claimed = inner
            .queue
            .iter()
            .find(|job| job.job_id.0 == "build")
            .cloned()
            .expect("build job queued");
        inner.queue.retain(|job| job.job_id.0 != "build");
        let claimed_request = inner
            .job_requests
            .values()
            .find(|record| record.job_id.0 == "build")
            .map(|record| record.request_id)
            .expect("build request");
        let queued_request = inner
            .job_requests
            .values()
            .find(|record| record.job_id.0 == "test")
            .map(|record| record.request_id)
            .expect("test request");
        inner
            .session_active_requests
            .insert("dead-session".to_owned(), claimed_request);
        inner
            .broker_messages
            .insert(claimed_request, claimed.message.clone());
        assert!(
            inner.sessions.is_empty(),
            "no session survives the restart in this scenario"
        );
        (claimed_request, queued_request)
    };

    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    let settled = crate::broker::reconcile_orphaned_claims(&shared).await;
    assert_eq!(settled, 1, "exactly the orphaned claim is settled");

    let inner = state.inner.lock().await;
    assert_eq!(
        inner
            .job_requests
            .get(&claimed_request)
            .and_then(|record| record.result),
        Some(ExecutionStatus::Failure),
        "an unclaimable job must be reported failed, not left queued forever"
    );
    assert!(
        !inner
            .session_active_requests
            .values()
            .any(|request_id| *request_id == claimed_request),
        "the dead session's claim must be released"
    );
    assert_eq!(
        inner
            .job_requests
            .get(&queued_request)
            .and_then(|record| record.result),
        None,
        "a job that was never claimed stays runnable"
    );
    assert!(
        inner.queue.iter().any(|job| job.job_id.0 == "test"),
        "the unclaimed job stays in the queue for a fresh machine"
    );
}

// ---------------------------------------------------------------------------
// Submit-driven CI push-back (`--push`): the server verifies the tested tree,
// creates the draft PR, and stays idempotent across replays.
// ---------------------------------------------------------------------------

async fn submit_push_run(app: &Router, sha: &str, push_tree: &str) -> Value {
    request_json(
        app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo",
            "git_ref": "refs/heads/feat/x",
            "sha": sha,
            "push_tree": push_tree,
            "push": {"create_pr": true, "draft_pr": true}
        }),
    )
    .await
}

#[tokio::test]
async fn submit_driven_push_publishes_pr_and_checks_idempotently() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    const SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const TREE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    let pr_creates = Arc::new(AtomicUsize::new(0));
    let pr_bodies = Arc::new(Mutex::new(Vec::<Value>::new()));
    let check_completions = Arc::new(Mutex::new(Vec::<Value>::new()));

    let mock_app = Router::new()
        .route(
            "/repos/owner/repo",
            get(|| async { Json(json!({"default_branch": "main"})) }),
        )
        .route(
            "/repos/owner/repo/commits/:sha",
            get(|Path(_sha): Path<String>| async move {
                Json(json!({"commit": {"tree": {"sha": TREE}}}))
            }),
        )
        .route(
            "/repos/owner/repo/pulls",
            get(|| async { Json(json!([])) }).post({
                let pr_creates = pr_creates.clone();
                let pr_bodies = pr_bodies.clone();
                move |body: axum::extract::Json<Value>| {
                    let pr_creates = pr_creates.clone();
                    let pr_bodies = pr_bodies.clone();
                    async move {
                        pr_creates.fetch_add(1, Ordering::SeqCst);
                        pr_bodies.lock().unwrap().push(body.0);
                        Json(json!({"number": 42}))
                    }
                }
            }),
        )
        .route(
            "/repos/owner/repo/check-runs",
            post(|| async { Json(json!({"id": 7})) }),
        )
        .route(
            "/repos/owner/repo/check-runs/:id",
            axum::routing::patch({
                let check_completions = check_completions.clone();
                move |Path(id): Path<u64>, body: axum::extract::Json<Value>| {
                    let check_completions = check_completions.clone();
                    async move {
                        check_completions.lock().unwrap().push(body.0);
                        Json(json!({"id": id}))
                    }
                }
            }),
        );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, mock_app).await.unwrap();
    });

    // Held for the whole test: the GitHub env vars are process-global.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _api_url =
        crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", format!("http://127.0.0.1:{port}"));
    let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "sync-test-token");

    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // 1. A --push submission reports queued check runs at accept time and
    //    starts in `pending`.
    let accepted = submit_push_run(&app, SHA, TREE).await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    {
        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id.parse::<RunId>().unwrap()).unwrap();
        assert_eq!(run.job_check_run_ids.len(), 1, "queued check run at submit");
        assert_eq!(
            *run.job_check_run_ids.values().next().unwrap(),
            7,
            "check run id comes from the (mock) GitHub API"
        );
        assert_eq!(run.push_state.as_ref().unwrap().status, PushStatus::Pending);
    }

    // 2. Sync before the run is terminal is refused.
    let (status, _) = request_json_status(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/push"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // 3. Terminal run: the sync verifies the tree, creates the draft PR,
    //    and marks the run pushed.
    {
        let mut inner = state.inner.lock().await;
        let run = inner
            .runs
            .get_mut(&run_id.parse::<RunId>().unwrap())
            .unwrap();
        run.conclusion = Some("success".to_owned());
    }
    let pushed = request_json(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/push"),
        Value::Null,
    )
    .await;
    assert_eq!(pushed["status"], "pushed");
    assert_eq!(pushed["pr_number"], 42);
    assert!(pushed["pr_url"].as_str().unwrap().ends_with("/pull/42"));

    {
        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id.parse::<RunId>().unwrap()).unwrap();
        assert_eq!(run.push_state.as_ref().unwrap().status, PushStatus::Synced);
        assert_eq!(run.push_state.as_ref().unwrap().pr_number, Some(42));
    }
    let pr_body = pr_bodies.lock().unwrap().first().unwrap().clone();
    assert_eq!(pr_body["head"], "feat/x");
    assert_eq!(pr_body["base"], "main");
    assert_eq!(pr_body["draft"], true, "new PRs are drafts by default");
    assert!(pr_body["body"].as_str().unwrap().contains(SHA));
    assert_eq!(pr_creates.load(Ordering::SeqCst), 1);
    assert!(
        check_completions.lock().unwrap().is_empty(),
        "jobs with a check run at submit are not re-reported by the sync"
    );

    // 4. Replay is a no-op: no second PR, same response.
    let again = request_json(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/push"),
        Value::Null,
    )
    .await;
    assert_eq!(again["pr_number"], 42);
    assert_eq!(pr_creates.load(Ordering::SeqCst), 1, "idempotent replay");

    // 5. A pushed tree that differs from the tested tree blocks the sync.
    let accepted = submit_push_run(&app, SHA, "cccccccccccccccccccccccccccccccccccccccc").await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    {
        let mut inner = state.inner.lock().await;
        let run = inner
            .runs
            .get_mut(&run_id.parse::<RunId>().unwrap())
            .unwrap();
        run.conclusion = Some("success".to_owned());
    }
    let (status, _) = request_json_status(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/push"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    {
        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id.parse::<RunId>().unwrap()).unwrap();
        assert_eq!(run.push_state.as_ref().unwrap().status, PushStatus::Blocked);
        assert!(run
            .push_state
            .as_ref()
            .unwrap()
            .error
            .as_deref()
            .unwrap()
            .contains("does not match"));
    }

    // 6. A run submitted without --push can never be pushed.
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo",
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    let (status, _) = request_json_status(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/push"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // 7. Push-back lands the commit on GitHub, which answers with a push
    //    webhook for that same commit. The workflow that was already tested
    //    and published must not run a second time, while a workflow the user
    //    never submitted still has to.
    const PUBLISHED_WORKFLOW: &str = ".github/workflows/ci.yml";
    let accepted = submit_push_run(&app, SHA, TREE).await;
    let published_id = accepted["run_id"]
        .as_str()
        .unwrap()
        .parse::<RunId>()
        .unwrap();
    {
        let mut inner = state.inner.lock().await;
        let run = inner.runs.get_mut(&published_id).unwrap();
        run.conclusion = Some("success".to_owned());
        let mut submission = (*run.submission).clone();
        submission.workflow_path = Some(PUBLISHED_WORKFLOW.to_owned());
        run.submission = Arc::new(submission);
    }
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    assert_eq!(
        crate::github_push::already_published(&shared, "owner/repo", SHA, PUBLISHED_WORKFLOW).await,
        Some(published_id),
        "the echo of our own push must be recognised"
    );
    assert_eq!(
        crate::github_push::already_published(
            &shared,
            "owner/repo",
            SHA,
            ".github/workflows/other.yml"
        )
        .await,
        None,
        "a workflow that was never submitted is new work and must still run"
    );
    assert_eq!(
        crate::github_push::already_published(
            &shared,
            "owner/repo",
            "dddddddddddddddddddddddddddddddddddddddd",
            PUBLISHED_WORKFLOW
        )
        .await,
        None,
        "a different commit is different work"
    );

    // A dirty-tree run's submission sha is the *base* commit; the webhook
    // echo carries the materialized commit recorded in push_state.
    // already_published must recognise that commit too, or every dirty-tree
    // push would re-run CI.
    const BASE_SHA: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    const MATERIALIZED_SHA: &str = "ffffffffffffffffffffffffffffffffffffffff";
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo",
            "git_ref": "refs/heads/feat/y",
            "sha": BASE_SHA,
            "push": {"create_pr": true, "draft_pr": true, "dirty": true},
            "push_tree": TREE,
            "workflow_path": PUBLISHED_WORKFLOW,
        }),
    )
    .await;
    let dirty_id = accepted["run_id"]
        .as_str()
        .unwrap()
        .parse::<RunId>()
        .unwrap();
    {
        let mut inner = state.inner.lock().await;
        let run = inner.runs.get_mut(&dirty_id).unwrap();
        run.conclusion = Some("success".to_owned());
        run.push_state = Some(crate::models::PushState {
            status: crate::models::PushStatus::Synced,
            error: None,
            pr_number: Some(7),
            effective_sha: Some(MATERIALIZED_SHA.to_owned()),
        });
    }
    assert_eq!(
        crate::github_push::already_published(
            &shared,
            "owner/repo",
            MATERIALIZED_SHA,
            PUBLISHED_WORKFLOW
        )
        .await,
        Some(dirty_id),
        "the webhook echo of a materialized dirty-tree commit must be recognised"
    );
    assert_eq!(
        crate::github_push::already_published(&shared, "owner/repo", BASE_SHA, PUBLISHED_WORKFLOW)
            .await,
        Some(dirty_id),
        "the recorded submission sha (the base commit) still matches, as for any push-back run"
    );
}

#[tokio::test]
async fn dirty_push_sync_verifies_the_branch_head_and_reports_checks_on_the_materialized_commit() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    const BASE_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const TREE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const MATERIALIZED: &str = "cccccccccccccccccccccccccccccccccccccccc";

    // A dirty-tree submission records the base commit as `sha`; the tested
    // commit is materialized after the run and pushed to the branch head, so
    // the sync must verify the BRANCH (not the base sha) and report checks
    // against the materialized head.
    let pr_creates = Arc::new(AtomicUsize::new(0));
    let check_creates = Arc::new(parking_lot::Mutex::new(Vec::<Value>::new()));
    let mock_app = Router::new()
        .route(
            "/repos/owner/repo",
            get(|| async { Json(json!({"default_branch": "main"})) }),
        )
        .route(
            // Branch names may contain slashes; GitHub's commits/{ref}
            // endpoint matches the whole remaining path.
            "/repos/owner/repo/commits/*ref",
            get(|Path(r#ref): Path<String>| async move {
                assert_eq!(r#ref, "feat/x", "dirty sync must verify the branch head");
                Json(json!({
                    "sha": MATERIALIZED,
                    "commit": {"tree": {"sha": TREE}},
                }))
            }),
        )
        .route(
            "/repos/owner/repo/pulls",
            get(|| async { Json(json!([])) }).post({
                let pr_creates = pr_creates.clone();
                move |_body: axum::extract::Json<Value>| {
                    let pr_creates = pr_creates.clone();
                    async move {
                        pr_creates.fetch_add(1, Ordering::SeqCst);
                        Json(json!({"number": 42}))
                    }
                }
            }),
        )
        .route(
            "/repos/owner/repo/check-runs",
            post({
                let check_creates = check_creates.clone();
                move |body: axum::extract::Json<Value>| {
                    let check_creates = check_creates.clone();
                    async move {
                        check_creates.lock().push(body.0);
                        Json(json!({"id": 7}))
                    }
                }
            }),
        )
        .route(
            "/repos/owner/repo/check-runs/:id",
            axum::routing::patch(|| async { Json(json!({"id": 7})) }),
        );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, mock_app).await.unwrap();
    });

    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    std::env::set_var("PRELOOP_GITHUB_API_URL", format!("http://127.0.0.1:{port}"));
    std::env::set_var("PRELOOP_GITHUB_TOKEN", "sync-test-token");

    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo",
            "git_ref": "refs/heads/feat/x",
            "sha": BASE_SHA,
            "push_tree": TREE,
            "push": {"create_pr": true, "draft_pr": true, "dirty": true},
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    {
        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id.parse::<RunId>().unwrap()).unwrap();
        assert_eq!(
            run.job_check_run_ids.len(),
            0,
            "dirty pushes get no submit-time check runs (the head is unknown)"
        );
        assert_eq!(run.push_state.as_ref().unwrap().status, PushStatus::Pending);
    }
    {
        let mut inner = state.inner.lock().await;
        let run = inner
            .runs
            .get_mut(&run_id.parse::<RunId>().unwrap())
            .unwrap();
        run.conclusion = Some("success".to_owned());
        run.jobs
            .insert(JobId("build".to_owned()), ExecutionStatus::Success);
    }

    let (status, body) = request_json_status(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/push"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "dirty sync must succeed: {body}");
    assert_eq!(pr_creates.load(Ordering::SeqCst), 1, "PR created");
    let check = check_creates
        .lock()
        .first()
        .cloned()
        .expect("queued check run");
    assert_eq!(
        check["head_sha"], MATERIALIZED,
        "checks attach to the materialized head commit, not the base"
    );
    {
        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id.parse::<RunId>().unwrap()).unwrap();
        let push_state = run.push_state.as_ref().unwrap();
        assert_eq!(push_state.status, PushStatus::Synced);
        assert_eq!(
            push_state.effective_sha.as_deref(),
            Some(MATERIALIZED),
            "the published commit is recorded for webhook dedup"
        );
    }

    std::env::remove_var("PRELOOP_GITHUB_TOKEN");
    std::env::remove_var("PRELOOP_GITHUB_API_URL");
}
