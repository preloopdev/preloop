//! preloop-runner-server integration tests — recovery group.
//! Split from the former `lib_tests.rs` unit; see `tests/common/mod.rs`.

mod common;

use common::*;

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
                    job_backend_id: String::new(),
                    created_unix: 0,
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
    assert!(paths.contains_key("/api/v1/runs/{run_id}/logs"));
    assert!(paths.contains_key("/api/v1/runs/{run_id}/logs/live"));
    assert!(paths.contains_key("/api/v1/debug/sessions"));
    assert!(!paths.keys().any(|path| path.starts_with("/_apis/")));
    assert!(!paths.keys().any(|path| path.starts_with("/broker/")));
    assert!(!paths.contains_key("/api/v1/scheduler/history"));
    // The read-only runner listing is native operator surface (the CLI uses
    // it to diagnose a dead pool); registration itself stays undocumented.
    assert!(paths.contains_key("/api/v1/runners"));
    assert_eq!(
        paths["/api/v1/runners"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        vec!["get"]
    );
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
    environment: ${{ needs.build.outputs.environment }}
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
            "outputs": {"artifact": "dist.tgz", "environment": "staging"}
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
    assert_eq!(
        deploy
            .message
            .actions_environment
            .as_ref()
            .map(|environment| environment.name.as_str()),
        Some("staging")
    );
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
        json!({"count": 2, "value": [{
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
        }, {
            // A typed step with no `parentId`. The manifest path counts this as
            // a step, so the annotation path must scope its issue to the step
            // rather than reporting it against the job.
            "id": "00000000-0000-0000-0000-000000000002",
            "name": "Run echo one",
            "type": "Task",
            "state": "completed",
            "result": "failed",
            "issues": [{
                "type": "error",
                "message": "step boom"
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

    // The step's issue is scoped to the step; the job's stays job-level. Both
    // paths deciding "is this a step" differently is what put a record in the
    // manifest while its annotation pointed at the job.
    let step_annotation = events
        .lines()
        .find(|line| line.contains("\"step boom\""))
        .expect("the step's annotation must be projected");
    assert!(
        step_annotation.contains("\"step_id\":\"00000000-0000-0000-0000-000000000002\""),
        "a typed step's issue must carry its step id: {step_annotation}"
    );
    let job_annotation = events
        .lines()
        .find(|line| line.contains("\"boom\"") && !line.contains("\"step boom\""))
        .expect("the job's annotation must be projected");
    assert!(
        !job_annotation.contains("\"step_id\""),
        "the job record's issue must stay job-level: {job_annotation}"
    );
}

/// Following a re-dispatched job streams the current attempt, not the first.
///
/// `job_requests` is keyed by monotonic request id, so selecting with `find`
/// returned the oldest attempt: after a retry, following the logical job key
/// subscribed to the dead attempt's feed, which never speaks again.

/// Following a re-dispatched job streams the current attempt, not the first.
///
/// `job_requests` is keyed by monotonic request id, so selecting with `find`
/// returned the oldest attempt: after a retry, following the logical job key
/// subscribed to the dead attempt's feed, which never speaks again.
#[tokio::test]
async fn live_log_key_follows_the_newest_attempt() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, _) = three_step_run_for_log_filters(&app, &state).await;

    // A re-dispatch: a second request for the same logical job, with a higher
    // request id and its own agent job id.
    let (first_attempt, second_attempt) = {
        let mut inner = state.inner.lock().await;
        let (first_id, first) = inner
            .job_requests
            .iter()
            .find(|(_, record)| record.run_id == run_id && record.job_id.0 == "build")
            .map(|(id, record)| (*id, record.clone()))
            .expect("the dispatched attempt");
        let mut retry = first.clone();
        retry.request_id = first_id + 1;
        retry.agent_job_id = uuid::Uuid::new_v4();
        let second = retry.agent_job_id;
        inner.job_requests.insert(retry.request_id, retry);
        (first.agent_job_id, second)
    };
    assert_ne!(first_attempt, second_attempt);

    let inner = state.inner.lock().await;
    let key = crate::live_logs::live_log_key_for_job(&inner, run_id, "build")
        .expect("a logical job key must resolve");
    assert_eq!(
        key,
        second_attempt.to_string(),
        "the logical job key must follow the current attempt, not the first"
    );

    // An explicit agent job id still addresses exactly that attempt, so an
    // older feed stays reachable when asked for by name.
    assert_eq!(
        crate::live_logs::live_log_key_for_job(&inner, run_id, &first_attempt.to_string())
            .expect("an explicit attempt id must resolve"),
        first_attempt.to_string(),
        "an explicit agent job id must not be redirected to another attempt"
    );
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
                    if wrappers.lines.len() == 1 {
                        assert_eq!(wrappers.lines[0].step_id, "step-1");
                        assert_eq!(wrappers.lines[0].start_line, 1);
                        assert_eq!(wrappers.lines[0].count, 2);
                        assert_eq!(wrappers.lines[0].value, vec!["hello", "world"]);
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

/// R1-8: the live-log ingest WebSocket must bind the target to the caller's
/// identity. The generic protocol bearer admits any job's runtime credential;
/// without an ownership check one job could stream into another job's buffer.

/// R1-8: the live-log ingest WebSocket must bind the target to the caller's
/// identity. The generic protocol bearer admits any job's runtime credential;
/// without an ownership check one job could stream into another job's buffer.
#[tokio::test]
async fn live_log_websocket_accepts_own_job_token() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (_run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    let (plan_a, agent_a) = (jobs[0].1.clone(), jobs[0].2.clone());
    let credential_a = state
        .local_jwt(json!({
            "sub": format!("preloop-job-{agent_a}"),
            "scp": format!("Actions.Results:{plan_a}:{agent_a}"),
        }))
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // The runner connects with its own job's credential against the agent job
    // id from its `FeedStreamUrl`.
    let url = format!("ws://{addr}/ws/live-logs/{agent_a}");
    let mut request =
        tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(url)
            .unwrap();
    request.headers_mut().insert(
        header::AUTHORIZATION,
        format!("Bearer {credential_a}").parse().unwrap(),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    let payload = json!({
        "stepId": "step-1",
        "startLine": 1,
        "count": 1,
        "value": ["hello"]
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
                if let Some(job_lines) = inner.live_log_lines.get(&agent_a) {
                    let wrappers = job_lines.lock().await;
                    if wrappers.lines.len() == 1 {
                        assert_eq!(wrappers.lines[0].value, vec!["hello"]);
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

/// R1-8: a job's runtime credential must not open another job's ingest feed.

/// R1-8: a job's runtime credential must not open another job's ingest feed.
#[tokio::test]
async fn live_log_websocket_rejects_other_job_token() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (_run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    let (plan_a, agent_a) = (jobs[0].1.clone(), jobs[0].2.clone());
    let agent_b = jobs[1].2.clone();
    let credential_a = state
        .local_jwt(json!({
            "sub": format!("preloop-job-{agent_a}"),
            "scp": format!("Actions.Results:{plan_a}:{agent_a}"),
        }))
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let url = format!("ws://{addr}/ws/live-logs/{agent_b}");
    let mut request =
        tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(url)
            .unwrap();
    request.headers_mut().insert(
        header::AUTHORIZATION,
        format!("Bearer {credential_a}").parse().unwrap(),
    );
    let result = tokio_tungstenite::connect_async(request).await;
    assert!(
        result.is_err(),
        "cross-job live-log ingest must be rejected"
    );

    server.abort();
}

/// R1-8: a rejected cross-job ingest attempt must not disturb the victim's
/// retained history. Reopening a closed feed clears it, so the ownership
/// check has to happen before the socket is accepted, not on first frame.

/// R1-8: a rejected cross-job ingest attempt must not disturb the victim's
/// retained history. Reopening a closed feed clears it, so the ownership
/// check has to happen before the socket is accepted, not on first frame.
#[tokio::test]
async fn live_log_websocket_cross_job_attempt_preserves_history() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (_run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    let (plan_a, agent_a) = (jobs[0].1.clone(), jobs[0].2.clone());
    let agent_b = jobs[1].2.clone();
    let credential_a = state
        .local_jwt(json!({
            "sub": format!("preloop-job-{agent_a}"),
            "scp": format!("Actions.Results:{plan_a}:{agent_a}"),
        }))
        .unwrap();
    let system_credential = state.system_token.clone();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Seed job B's retained tail through its own feed, then close the feed.
    let url = format!("ws://{addr}/ws/live-logs/{agent_b}");
    let mut request =
        tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(url)
            .unwrap();
    request.headers_mut().insert(
        header::AUTHORIZATION,
        format!("Bearer {system_credential}").parse().unwrap(),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    let payload = json!({
        "stepId": "step-1",
        "startLine": 1,
        "count": 1,
        "value": ["victim-line"]
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
                if let Some(job_lines) = inner.live_log_lines.get(&agent_b) {
                    if job_lines.lock().await.lines.len() == 1 {
                        break;
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    drop(ws);
    {
        let mut inner = state.inner.lock().await;
        crate::live_logs::close_live_log(&mut inner, &agent_b);
    }

    // Job A's credential against job B's feed: rejected at upgrade.
    let url = format!("ws://{addr}/ws/live-logs/{agent_b}");
    let mut request =
        tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(url)
            .unwrap();
    request.headers_mut().insert(
        header::AUTHORIZATION,
        format!("Bearer {credential_a}").parse().unwrap(),
    );
    let result = tokio_tungstenite::connect_async(request).await;
    assert!(
        result.is_err(),
        "cross-job live-log ingest must be rejected"
    );

    // The victim's retained tail is untouched and still marked closed.
    {
        let inner = state.inner.lock().await;
        let job_lines = inner
            .live_log_lines
            .get(&agent_b)
            .expect("victim history must survive");
        let wrappers = job_lines.lock().await;
        assert_eq!(wrappers.lines.len(), 1);
        assert_eq!(wrappers.lines[0].value, vec!["victim-line"]);
        assert!(inner.live_log_closed.contains(&agent_b));
    }

    server.abort();
}

/// M5: the protocol live-log read route must not let one job's runtime
/// credential read another job's output. A job may read its own feed.
#[tokio::test]
async fn live_log_sse_accepts_own_job_credential() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    let (logical_a, plan_a, agent_a) = (jobs[0].0.clone(), jobs[0].1.clone(), jobs[0].2.clone());
    let credential_a = state
        .local_jwt(json!({
            "sub": format!("preloop-job-{agent_a}"),
            "scp": format!("Actions.Results:{plan_a}:{agent_a}"),
        }))
        .unwrap();

    // Own logical job name.
    let response = open_protocol_live(
        &app,
        format!("/api/v1/runs/{run_id}/jobs/{logical_a}/logs/live"),
        &credential_a,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    // Own concrete agent-job UUID.
    let response = open_protocol_live(
        &app,
        format!("/api/v1/runs/{run_id}/jobs/{agent_a}/logs/live"),
        &credential_a,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

/// M5: a job's runtime credential must not read another job's live log,
/// whether addressed by logical name or by concrete agent-job UUID.

/// M5: a job's runtime credential must not read another job's live log,
/// whether addressed by logical name or by concrete agent-job UUID.
#[tokio::test]
async fn live_log_sse_rejects_other_job_credential() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    let (plan_a, agent_a) = (jobs[0].1.clone(), jobs[0].2.clone());
    let (logical_b, agent_b) = (jobs[1].0.clone(), jobs[1].2.clone());
    let credential_a = state
        .local_jwt(json!({
            "sub": format!("preloop-job-{agent_a}"),
            "scp": format!("Actions.Results:{plan_a}:{agent_a}"),
        }))
        .unwrap();

    let response = open_protocol_live(
        &app,
        format!("/api/v1/runs/{run_id}/jobs/{logical_b}/logs/live"),
        &credential_a,
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "cross-job read by logical name must be rejected"
    );

    // A UUID target is rejected without resolving it, so the mismatch never
    // reveals whether the target exists.
    let response = open_protocol_live(
        &app,
        format!("/api/v1/runs/{run_id}/jobs/{agent_b}/logs/live"),
        &credential_a,
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "cross-job read by concrete job id must be rejected"
    );
}

/// M5: the system credential keeps full read access; first-party readers are
/// unaffected by the per-job ownership check.

/// M5: the system credential keeps full read access; first-party readers are
/// unaffected by the per-job ownership check.
#[tokio::test]
async fn live_log_sse_system_credential_still_reads() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    let logical_b = jobs[1].0.clone();
    let system_credential = state.system_token.clone();

    let response = open_protocol_live(
        &app,
        format!("/api/v1/runs/{run_id}/jobs/{logical_b}/logs/live"),
        &system_credential,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
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
                if wrappers.lines.len() == 1 {
                    assert_eq!(wrappers.lines[0].value, vec!["survived"]);
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
async fn log_get_run_logs_falls_back_to_uploaded_step_logs() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = submit_yaml(
        &app,
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: exit 1\n",
        "owner/repo",
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let (plan_id, agent_job_id) = {
        let inner = state.inner.lock().await;
        let request = inner
            .job_requests
            .values()
            .find(|request| request.run_id == run_id)
            .unwrap();
        (request.plan_id.clone(), request.agent_job_id.to_string())
    };
    let results_dir = temp
        .path()
        .join("replay")
        .join("results")
        .join(plan_id)
        .join(agent_job_id);
    tokio::fs::create_dir_all(&results_dir).await.unwrap();
    tokio::fs::write(results_dir.join("step-first.txt"), b"first step\n")
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    tokio::fs::write(results_dir.join("step-second.txt"), b"failed step\n")
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
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), b"first step\nfailed step\n");
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

/// Build a two-job run and return `(run_id, [(job_id, plan_id, agent_job_id)])`
/// ordered by request id, so filter tests can address either job.
#[tokio::test]
async fn log_run_logs_job_filter_returns_only_that_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    write_merged_job_log(&temp, &jobs[0].1, &jobs[0].2, "build output\n").await;
    write_merged_job_log(&temp, &jobs[1].1, &jobs[1].2, "test output\n").await;

    // Unfiltered still merges every job, in request order.
    let (status, body) = get_logs(&app, format!("/api/v1/runs/{run_id}/logs")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"build output\ntest output\n");

    // Filtering by the workflow job key returns only that job.
    let (status, body) = get_logs(&app, format!("/api/v1/runs/{run_id}/logs?job=test")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body, b"test output\n",
        "job filter must exclude the other job"
    );

    let (status, body) = get_logs(&app, format!("/api/v1/runs/{run_id}/logs?job=build")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"build output\n");
}

#[tokio::test]
async fn log_run_logs_job_filter_accepts_agent_job_uuid() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    write_merged_job_log(&temp, &jobs[0].1, &jobs[0].2, "build output\n").await;
    write_merged_job_log(&temp, &jobs[1].1, &jobs[1].2, "test output\n").await;

    // The live-log feed accepts either identifier; so must this.
    let agent_uuid = &jobs[1].2;
    let (status, body) =
        get_logs(&app, format!("/api/v1/runs/{run_id}/logs?job={agent_uuid}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"test output\n");
}

#[tokio::test]
async fn log_run_logs_unknown_job_is_404_not_whole_run() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    write_merged_job_log(&temp, &jobs[0].1, &jobs[0].2, "build output\n").await;

    let (status, body) = get_logs(
        &app,
        format!("/api/v1/runs/{run_id}/logs?job=does-not-exist"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a bogus job must fail loudly, never fall back to the full run"
    );
    assert!(
        !String::from_utf8_lossy(&body).contains("build output"),
        "404 body must not leak the unfiltered log"
    );
}

/// A restart before the first step report keeps the declared steps.
///
/// Only a runner report writes step rows, so an attempt dispatched and then
/// interrupted has none. Its request message is persisted, and that message is
/// what the manifest was built from, so startup rebuilds it — otherwise the
/// run loses its declared steps and `--step` answers 409 for blobs that are
/// sitting on disk.

/// A restart before the first step report keeps the declared steps.
///
/// Only a runner report writes step rows, so an attempt dispatched and then
/// interrupted has none. Its request message is persisted, and that message is
/// what the manifest was built from, so startup rebuilds it — otherwise the
/// run loses its declared steps and `--step` answers 409 for blobs that are
/// sitting on disk.
#[tokio::test]
async fn dispatched_but_unreported_manifests_are_rebuilt_on_restart() {
    let temp = tempfile::tempdir().unwrap();
    let (run_id, plan_id, agent_job_id, ids) = {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let (run_id, jobs) = three_step_run_for_log_filters(&app, &state).await;
        let ids = workflow_step_ids(&state, run_id, "build").await;
        // Deliberately no forced snapshot: `store_inner` writes step rows,
        // which would persist the manifest and make the rebuild moot. The real
        // window is a submission persisted only by its run events, which carry
        // the run, the requests and the broker message but never steps.
        (run_id, jobs[0].1.clone(), jobs[0].2.clone(), ids)
    };

    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    assert_eq!(
        workflow_step_ids(&state, run_id, "build").await,
        ids,
        "declared steps must be rebuilt from the persisted request message"
    );

    write_step_job_logs(
        &temp,
        &plan_id,
        &agent_job_id,
        &[(ids[2].as_str(), "third step\n")],
    )
    .await;
    let (status, body) =
        get_logs(&app, format!("/api/v1/runs/{run_id}/logs?job=build&step=3")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"third step\n", "`--step` must resolve after restart");
}

/// The AzDO timeline path orders synthetic steps and ignores the job record.
///
/// `TimelineRecord` carries no ordinal, so a synthetic step reported this way
/// has no `runner_number` and must be ordered by when it started — otherwise
/// `Set up job` sorts after every declared step. The PATCH also carries the
/// job's own record, whose UUID never equals the workflow job key, so it was
/// reconciled in as an extra step named after the job.

/// The AzDO timeline path orders synthetic steps and ignores the job record.
///
/// `TimelineRecord` carries no ordinal, so a synthetic step reported this way
/// has no `runner_number` and must be ordered by when it started — otherwise
/// `Set up job` sorts after every declared step. The PATCH also carries the
/// job's own record, whose UUID never equals the workflow job key, so it was
/// reconciled in as an extra step named after the job.
#[tokio::test]
async fn timeline_path_orders_synthetic_steps_and_skips_the_job_record() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, _) = three_step_run_for_log_filters(&app, &state).await;
    let ids = workflow_step_ids(&state, run_id, "build").await;
    let (plan_id, timeline_id) = {
        let inner = state.inner.lock().await;
        let request = inner
            .job_requests
            .values()
            .find(|request| request.run_id == run_id)
            .expect("dispatched request");
        (request.plan_id.clone(), request.timeline_id.to_string())
    };

    // Setup started before the declared steps, and the job record is sent
    // alongside them exactly as the official runner does.
    //
    // The three step records deliberately use the three shapes that appear on
    // the wire: `Task` (official runner), `Step` (which this file's annotation
    // path already recognises), and no `type` at all (the field is optional).
    // An allow-list of one type passes a test that only sends that type while
    // silently dropping the others' conclusions.
    let setup_id = uuid::Uuid::new_v4().to_string();
    let job_record_id = uuid::Uuid::new_v4().to_string();
    let record = |id: &str, kind: Option<&str>, name: &str, start: &str| {
        let mut value = json!({
            "id": id,
            "name": name,
            "displayName": name,
            "state": "completed",
            "result": "succeeded",
            "startTime": start,
        });
        match kind {
            Some(kind) => value["type"] = json!(kind),
            // Untyped records are identified as steps by their parent.
            None => value["parentId"] = json!(job_record_id),
        }
        value
    };
    let response = request_json(
        &app,
        Method::PATCH,
        &format!("/_apis/v1/Timeline/scope/actions/{plan_id}/{timeline_id}"),
        json!({"count": 5, "value": [
            record(&job_record_id, Some("Job"), "build", "2026-01-01T00:00:00Z"),
            record(&ids[0], Some("Task"), "Run echo one", "2026-01-01T00:00:02Z"),
            record(&ids[1], Some("Step"), "Run echo two", "2026-01-01T00:00:03Z"),
            record(&ids[2], None, "Run echo three", "2026-01-01T00:00:04Z"),
            record(&setup_id, Some("Task"), "Set up job", "2026-01-01T00:00:01Z"),
        ]}),
    )
    .await;
    assert_eq!(response["count"], 5);

    let run = get_run_json(&app, &run_id.to_string()).await;
    let names: Vec<&str> = run["jobs_list"]
        .as_array()
        .unwrap()
        .iter()
        .find(|detail| detail["job_id"] == "build")
        .expect("build detail")["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|step| step["name"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(
        names,
        vec![
            "Set up job",
            "Run echo one",
            "Run echo two",
            "Run echo three"
        ],
        "synthetic setup must lead, and the job record must not become a step"
    );
}

/// Expansion does not leave the placeholder's manifest behind.
///
/// A deferred-matrix node is dispatched as a placeholder, gets a manifest
/// seeded, and is purged when expansion replaces it with real legs. Without
/// cleanup every dynamic expansion accumulates an entry no run projection can
/// reach.

/// Expansion does not leave the placeholder's manifest behind.
///
/// A deferred-matrix node is dispatched as a placeholder, gets a manifest
/// seeded, and is purged when expansion replaces it with real legs. Without
/// cleanup every dynamic expansion accumulates an entry no run projection can
/// reach.
#[tokio::test]
async fn expansion_purges_the_placeholder_step_manifest() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_yaml(
        &app,
        r#"
on: push
jobs:
  gen:
    runs-on: ubuntu-latest
    outputs:
      matrix: ${{ steps.build.outputs.matrix }}
    steps:
      - id: build
        run: echo matrix
  fan:
    needs: [gen]
    runs-on: ubuntu-latest
    strategy:
      matrix: ${{ fromJson(needs.gen.outputs.matrix) }}
    steps:
      - run: echo leg
"#,
        "owner/repo",
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let before = {
        let inner = state.inner.lock().await;
        inner.job_steps.len()
    };

    // Finish `gen` with a matrix so the deferred node expands. The result is
    // propagated: swallowing it let this test pass without ever completing the
    // job, checking orphans against a run that never expanded.
    let _completed = crate::distributed_task::complete_job_inner(
        state.shared(),
        preloop_gha_protocol::JobCompletion {
            run_id,
            job_id: preloop_gha_protocol::JobId("gen".to_owned()),
            agent_job_id: None,
            status: ExecutionStatus::Success,
            outputs: [("matrix".to_owned(), serde_json::json!("{\"leg\":[1,2]}"))]
                .into_iter()
                .collect(),
            annotations: Vec::new(),
            step_results: Vec::new(),
        },
    )
    .await
    .expect("completing gen must succeed");

    let inner = state.inner.lock().await;
    // The placeholder must actually be gone, replaced by one leg per matrix
    // value. Without this the orphan check below could pass vacuously.
    let legs: Vec<&str> = inner
        .job_requests
        .values()
        .filter(|record| record.run_id == run_id && record.job_id.0.starts_with("fan"))
        .map(|record| record.job_id.0.as_str())
        .collect();
    assert_eq!(
        legs.len(),
        2,
        "the deferred node must expand into two legs, got {legs:?}"
    );
    // Every retained manifest must belong to a request that still exists.
    let live: std::collections::BTreeSet<uuid::Uuid> = inner
        .job_requests
        .values()
        .map(|record| record.agent_job_id)
        .collect();
    let orphans: Vec<&uuid::Uuid> = inner
        .job_steps
        .keys()
        .filter(|agent_job_id| !live.contains(agent_job_id))
        .collect();
    assert!(
        orphans.is_empty(),
        "expansion left {} unreachable manifest(s) (had {before} before)",
        orphans.len()
    );
}
