//! preloop-runner-server integration tests — registration group.
//! Split from the former `lib_tests.rs` unit; see `tests/common/mod.rs`.

mod common;

use common::*;

/// When the App installation grants fewer repository permissions than the
/// job requested, the broker narrows the mint and must restate the wire
/// permissions from the effective repository-token grant. The OIDC grant
/// remains available through its dedicated endpoint metadata.
#[tokio::test]
async fn broker_claim_restates_narrowed_repository_permissions() {
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
    // Native `/api/v1/runs` clears client-supplied `trust_tier` (only the
    // webhook adapters stamp provenance); stamp it via `submit_run_inner`
    // like the webhook path does.
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    let submit = |tier: Option<&'static str>| {
        let shared = shared.clone();
        async move {
            crate::submit_run_inner(
                &shared,
                preloop_gha_protocol::WorkflowSubmission {
                    workflow_yaml: yaml.to_owned(),
                    event: "push".to_owned(),
                    payload: json!({"ref": "refs/heads/main", "commits": []}),
                    repository: "owner/repo".to_owned(),
                    git_ref: "refs/heads/main".to_owned(),
                    trust_tier: tier.map(str::to_owned),
                    ..Default::default()
                },
            )
            .await
            .expect("fork/trusted submission accepted")
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
    // installation's grant, `checks` disappears entirely, and the OIDC grant
    // remains available through the dedicated endpoint metadata.
    submit(None).await;
    let trusted = claim_and_return(&app, runner_id, &runner_token).await;
    assert_eq!(
        trusted["variables"]["system.github.token.permissions"]["value"],
        r#"{"Metadata":"read","PullRequests":"read"}"#,
        "trusted wire reflects the narrowed App grant without Actions-only scopes"
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

    // Fork job (claim 3): the narrowed restatement contains repository
    // permissions only and never invents OIDC metadata.
    submit(Some("untrusted-fork-pull-request")).await;
    let fork = claim_and_return(&app, runner_id, &runner_token).await;
    assert_eq!(
        fork["variables"]["system.github.token.permissions"]["value"],
        r#"{"Metadata":"read","PullRequests":"read"}"#,
        "fork wire reflects the narrowed grant without Actions-only metadata"
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
        let inner = state.test_tx().await;
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

    let inner = state.test_tx().await;
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

    // Native `/api/v1/runs` clears client-supplied `trust_tier` (only the
    // webhook adapters stamp provenance); stamp it via `submit_run_inner`
    // like the webhook path does.
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    let submit = |tier: Option<&'static str>| {
        let shared = shared.clone();
        async move {
            crate::submit_run_inner(
                &shared,
                preloop_gha_protocol::WorkflowSubmission {
                    workflow_yaml: "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n".to_owned(),
                    event: "push".to_owned(),
                    payload: json!({"ref": "refs/heads/main", "commits": []}),
                    repository: "owner/repo".to_owned(),
                    git_ref: "refs/heads/main".to_owned(),
                    trust_tier: tier.map(str::to_owned),
                    ..Default::default()
                },
            )
            .await
            .expect("fork/trusted submission accepted")
        }
    };

    let fork = submit(Some("untrusted-fork-pull-request")).await;
    let fork_run_id = fork.run_id.to_string();
    let trusted = submit(None).await;
    let trusted_run_id = trusted.run_id.to_string();

    let (fork_token, fork_plan, fork_job) = {
        let inner = state.test_tx().await;
        let message = queued_message_for(&inner, &fork_run_id);
        (
            state.mint_runtime_token(&message.plan.plan_id, &message.job_id),
            message.plan.plan_id.clone(),
            message.job_id,
        )
    };
    let trusted_token = {
        let inner = state.test_tx().await;
        let message = queued_message_for(&inner, &trusted_run_id);
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
        let inner = state.test_tx().await;
        let message = queued_message_for(&inner, fork["run_id"].as_str().unwrap());
        (
            state.mint_runtime_token(&message.plan.plan_id, &message.job_id),
            message.job_id,
        )
    };
    let trusted_token = {
        let inner = state.test_tx().await;
        let message = queued_message_for(&inner, trusted["run_id"].as_str().unwrap());
        state.mint_runtime_token(&message.plan.plan_id, &message.job_id)
    };

    // The same surgery `RequestRetirement::Purge` performs: drop the
    // job-to-request correlation while the worker still holds the runtime
    // JWT. `agent_job_requests` is derived from `job_requests` on load, so
    // the record itself must go.
    state
        .test_db_mutate(|tx| {
            tx.0.execute(
                "DELETE FROM job_requests WHERE agent_job_id = ?1",
                rusqlite::params![fork_job.to_string()],
            )
            .unwrap();
        })
        .await;

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

/// The cache v2 namespace (repository) must come from the caller's
/// job token, exactly like the artifact path, and never from the request
/// body: otherwise any job can read another repository's cache or plant
/// entries in it.
#[tokio::test]
async fn cache_v2_namespace_follows_the_job_token_not_the_request_body() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let submit = |repository: &'static str| {
        let state = state.clone();
        async move {
            crate::submit_run_inner(
                &state.shared(),
                preloop_gha_protocol::WorkflowSubmission {
                    workflow_yaml: "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n".to_owned(),
                    event: "push".to_owned(),
                    payload: json!({"ref": "refs/heads/main", "commits": []}),
                    repository: repository.to_owned(),
                    git_ref: "refs/heads/main".to_owned(),
                    trust_tier: None,
                    ..Default::default()
                },
            )
            .await
            .expect("trusted submission accepted")
        }
    };
    let run_a = submit("owner/a").await;
    let run_b = submit("owner/b").await;
    let token_for = |run_id: &preloop_gha_protocol::RunId| {
        let state = state.clone();
        let run_id = run_id.to_string();
        async move {
            let inner = state.test_tx().await;
            let message = queued_message_for(&inner, &run_id);
            state.mint_runtime_token(&message.plan.plan_id, &message.job_id)
        }
    };
    let token_a = token_for(&run_a.run_id).await;
    let token_b = token_for(&run_b.run_id).await;

    // Repository B's own upload lands in B's namespace, under the job's own
    // ref scope (the scope a job token may read).
    let job_scope = Some("refs/heads/main");
    let b_key = crate::results_twirp::scoped_cache_key("shared-key", job_scope, Some("owner/b"));
    state
        .cache
        .put(&b_key, "v1", b"bytes-from-b")
        .await
        .expect("seed repository B cache");

    let restore_uri = "/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL";
    let restore_body = json!({"key": "shared-key", "version": "v1", "repository": "owner/b"});

    // A's job token naming B in the body still reads A's namespace: a miss.
    let as_a = request_json_with_bearer(
        &app,
        Method::POST,
        restore_uri,
        restore_body.clone(),
        &token_a,
    )
    .await;
    assert_eq!(
        as_a["ok"], false,
        "a job token must not read another repository's cache by naming it in the body"
    );

    // B's token resolves the same body to B's own namespace: a hit.
    let as_b =
        request_json_with_bearer(&app, Method::POST, restore_uri, restore_body, &token_b).await;
    assert_eq!(
        as_b["ok"], true,
        "the owning repository's job still reads its own cache"
    );

    // A reservation is namespaced by the caller too, so A cannot plant an
    // entry in B's namespace for B to restore later.
    let create_uri = "/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry";
    let created = request_json_with_bearer(
        &app,
        Method::POST,
        create_uri,
        json!({"key": "poison-key", "version": "v1", "repository": "owner/b"}),
        &token_a,
    )
    .await;
    assert_eq!(
        created["ok"], true,
        "a trusted job may reserve a cache entry"
    );
    let inner = state.inner.lock().await;
    let keys = inner
        .cache_v2_pending
        .values()
        .map(|pending| pending.key.as_str())
        .collect::<Vec<_>>();
    assert!(
        keys.contains(
            &crate::results_twirp::scoped_cache_key("poison-key", job_scope, Some("owner/a"))
                .as_str()
        ),
        "A's reservation must be namespaced under A's repository, got {keys:?}"
    );
    assert!(
        !keys.iter().any(|key| key.starts_with("owner/b:")),
        "a body repository must not move a reservation into B's namespace, got {keys:?}"
    );
}

/// Point PAT scope introspection at a dead local address so tests that
/// submit runs with a configured PAT stay hermetic: the probe fails fast with
/// connection-refused instead of reaching api.github.com, where a fake PAT
/// would 401 and fail the run. The scopes are then `Unverifiable`, so the PAT
/// is withheld from jobs while the run itself still proceeds.

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
    // PAT scope introspection must stay hermetic — without a stub the probe
    // would reach api.github.com, where the fake PAT 401s and the run is
    // refused. A stub reporting read-only scopes lets the PAT be verified, and
    // therefore embedded, without a real credential.
    let _live_api = live_pat_scope_api("read:org, read:user").await;
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
    // Native `/api/v1/runs` clears client-supplied `trust_tier` (only the
    // webhook adapters stamp provenance); stamp it via `submit_run_inner`
    // like the webhook path does.
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });

    let yaml =
        "on: push\njobs:\n  probe:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let fork = crate::submit_run_inner(
        &shared,
        preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: yaml.to_owned(),
            event: "push".to_owned(),
            repository: "owner/repo".to_owned(),
            trust_tier: Some("untrusted-fork-pull-request".to_owned()),
            ..Default::default()
        },
    )
    .await
    .expect("fork submission accepted");
    let _ = fork.run_id;
    let trusted = crate::submit_run_inner(
        &shared,
        preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: yaml.to_owned(),
            event: "push".to_owned(),
            repository: "owner/repo".to_owned(),
            ..Default::default()
        },
    )
    .await
    .expect("trusted submission accepted");
    let _ = trusted.run_id;

    // The stored template carries no token; the broker fills it at claim.
    // A fork-restricted job receives the job-scoped runtime token — never
    // the repository-unscoped PAT.
    let fork_acquired = acquire_queued_job(&app, "fork-runner").await;
    let fork_job_id = fork_acquired["jobId"].as_str().expect("acquired job id");
    let fork_plan = fork_acquired["plan"]["planId"]
        .as_str()
        .expect("acquired plan id");
    let runtime_token =
        state.mint_runtime_token(fork_plan, &uuid::Uuid::parse_str(fork_job_id).unwrap());
    // Compare token identity (`sub`), not token strings: JWT timestamps are
    // second-granularity, so a comparison token minted across a clock tick
    // differs textually from the identical token minted at acquire.
    let expected_sub = jwt_sub(runtime_token.as_str());
    assert!(
        expected_sub.is_some(),
        "the runtime token carries a subject"
    );
    for name in ["system.github.token", "github_token"] {
        assert_eq!(
            wire_variable(&fork_acquired, name).and_then(jwt_sub),
            expected_sub.clone(),
            "fork job must carry the local runtime token, not the PAT ({name})"
        );
        assert_ne!(
            wire_variable(&fork_acquired, name),
            Some(pat.as_str()),
            "the static PAT must not reach a fork-restricted job ({name})"
        );
    }
    assert!(
        wire_variable(&fork_acquired, "GITHUB_TOKEN").is_none(),
        "uppercase GITHUB_TOKEN is not part of the official acquire schema"
    );

    let trusted_acquired = acquire_queued_job(&app, "trusted-runner").await;
    assert_eq!(
        wire_variable(&trusted_acquired, "system.github.token"),
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

    let base_sha = commit_workflow_fixture(&ws_dir, &[".github/workflows/test.yml"]);

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
                "sha": base_sha.clone()
            },
            "merge_commit_sha": base_sha
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
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    crate::github::drain_webhook_queue(&shared).await.unwrap();

    let inner = state.test_tx().await;
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
        Some(r#"{"Checks":"read","Metadata":"read"}"#),
        "webhook-delivered fork PR job is downgraded to read-only without Actions-only metadata"
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
    // Secrets denied: the stored template carries an explicit empty spec
    // (never treated as a legacy fully-formed message) and no secret
    // variable slots exist to fill.
    let spec = message
        .preloop_secret_spec
        .as_ref()
        .expect("a secrets-denied job still carries an explicit spec");
    assert!(
        spec.names.is_empty() && !spec.inherit && spec.map.is_empty(),
        "stored secrets stay denied for the fork PR job"
    );
    drop(inner);

    // Trusted control through the same build path: the same stored secret is
    // injected at acquire and the declared writes survive.
    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\npermissions:\n  checks: write\njobs:\n  build:\n    runs-on: ubuntu-latest\n    env:\n      REPO_TOKEN: ${{ secrets.REPO_TOKEN }}\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo",
        }),
    )
    .await;
    // The fork job is still queued ahead; claim it first so the next acquire
    // lands the trusted run's job.
    let _fork_job = acquire_queued_job(&app, "fork-wh-first").await;
    let acquired = acquire_queued_job(&app, "fork-wh-runner").await;
    assert_eq!(
        wire_variable(&acquired, "REPO_TOKEN"),
        Some("repo-value"),
        "trusted jobs still receive stored secrets"
    );
    assert_eq!(
        wire_variable(&acquired, "system.github.token.permissions"),
        Some(r#"{"Checks":"write","Metadata":"read"}"#),
        "trusted jobs keep declared writes and implicit metadata"
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
            let inner = state.test_tx().await;
            assert_eq!(inner.ready().count(), 1);
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
        let inner = state.test_tx().await;
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

    let inner = state.test_tx().await;
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

/// A settled attempt must not be acquirable. The broker retains
/// `owner_runner_id` after completion so late protocol reads stay bound to the
/// runner that ran the job, which means ownership alone cannot gate
/// `acquirejob`: a runner that retries the call after reporting would be
/// handed the job payload (and a freshly minted installation token) and would
/// execute the same job's side effects twice, then fail to report because
/// `renewjob` 409s and `completejob` ignores duplicates.
#[tokio::test]
async fn a_settled_attempt_cannot_be_acquired_again() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let registered = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({"name": "acquire-guard-runner", "version": "2.336.0"}),
    )
    .await;
    let runner_id = registered["id"].as_i64().unwrap();
    let runner_token = state
        .local_jwt(json!({
            "sub": format!("preloop-runner-listen-{runner_id}"),
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    submit_simple_run(&app).await;
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
    let job_message_id = body["runner_request_id"].as_str().unwrap().to_owned();
    let request_id = {
        let inner = state.test_tx().await;
        *inner.job_requests.keys().next().unwrap()
    };
    let acquire =
        json!({"jobMessageId": job_message_id, "billingOwnerId": "local", "runnerOS": "Linux"});

    // A live claim acquires normally: the guard must not break dispatch.
    assert_eq!(
        status_with_bearer(
            &app,
            &runner_token,
            Method::POST,
            &format!("/broker/{runner_id}/acquirejob"),
            acquire.clone(),
        )
        .await,
        StatusCode::OK
    );

    request_json_with_bearer(
        &app,
        Method::PATCH,
        &format!("/_apis/v1/AgentRequest/1/{request_id}"),
        json!({"result": "Succeeded"}),
        &runner_token,
    )
    .await;

    assert_eq!(
        status_with_bearer(
            &app,
            &runner_token,
            Method::POST,
            &format!("/broker/{runner_id}/acquirejob"),
            acquire,
        )
        .await,
        StatusCode::CONFLICT,
        "a reported attempt must never be handed back to a runner"
    );
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
async fn pat_only_server_fetches_webhook_workflows_with_configured_pat() {
    use axum::http::HeaderMap;
    use axum::routing::get;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_base = format!("http://{}", listener.local_addr().unwrap());
    let download_url = format!("{api_base}/raw/ci.yml");
    let stub = Router::new()
        .route(
            "/repos/preloopdev/preloop/contents/.github/workflows",
            get(move |headers: HeaderMap| {
                let download_url = download_url.clone();
                async move {
                    assert_eq!(
                        headers
                            .get("authorization")
                            .and_then(|value| value.to_str().ok()),
                        Some("Bearer ghp_config_workflow_token")
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
                    Some("Bearer ghp_config_workflow_token")
                );
                "on: push\njobs:\n  test:\n    runs-on: self-hosted\n    steps:\n      - run: true\n"
            }),
        );
    tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    std::fs::write(
        &config_path,
        "[github]\npat = \"ghp_config_workflow_token\"\n",
    )
    .unwrap();
    let mut state = AppState::new_with_config(temp.path().join("state"), config_path)
        .await
        .unwrap();
    state.local_workspace = None;
    assert!(state.github_app.is_none());
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
        let inner = state.test_tx().await;
        assert_eq!(
            inner.ready().count(),
            1,
            "job still queued inside the grace window"
        );
        assert!(
            inner
                .queued_at
                .contains_key(&(run_id, JobId("build".to_owned())))
        );
    }

    // Backdate the first-seen mark past the grace window and reap again: the
    // job must be failed with a visible reason and the run must conclude.
    // The mark is node-local reaper state, not a column.
    {
        let mut inner = state.inner.lock().await;
        let mark = inner
            .reaper_first_seen
            .get_mut(&(run_id, JobId("build".to_owned())))
            .expect("the first tick must mark the unmatched job");
        *mark -= Duration::from_secs(300);
    }
    reap_once(&shared).await;

    {
        let inner = state.test_tx().await;
        assert!(
            inner.ready().next().is_none(),
            "starving job must leave the ready queue"
        );
        assert!(
            !inner
                .queued_at
                .contains_key(&(run_id, JobId("build".to_owned())))
        );
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
async fn restored_job_without_enqueue_timestamp_is_not_granted_new_grace_window() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });
    let accepted = submit_simple_run(&app).await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    state
        .test_db_mutate(|tx| {
            tx.set_job_enqueued_us(run_id, &JobId("build".to_owned()), None)
                .unwrap();
        })
        .await;
    reap_once(&shared).await;

    let inner = state.test_tx().await;
    assert!(
        inner.ready().next().is_none(),
        "a restored job with unknown age must not receive a fresh starvation grace window"
    );
    assert_eq!(
        inner.runs.get(&run_id).unwrap().status,
        ExecutionStatus::Failure
    );
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
    let (request_id, old_agent_job_id) = {
        let inner = state.test_tx().await;
        assert!(
            inner.claimed_jobs.contains_key(&(run_id, job_id.clone())),
            "poll must record the claim in claimed_jobs"
        );
        let request_id = *inner
            .session_active_requests
            .get(&session_id)
            .expect("poll must pin the claim to the session");
        (request_id, inner.job_requests[&request_id].agent_job_id)
    };
    // The attempt already minted its debug token before its runner went deaf.
    state
        .test_db_mutate(|tx| {
            tx.execute(
                "UPDATE job_requests SET debug_token_issued = 1 WHERE request_id = ?1",
                rusqlite::params![request_id],
            )
            .unwrap();
        })
        .await;

    // The runner goes deaf: backdate its last poll and shrink the timeout.
    state.test_set_runner_liveness(Duration::from_secs(600));
    state
        .test_db_mutate(|tx| {
            let cutoff = crate::store::now_us() - 3_600_000_000;
            tx.set_session_seen(&session_id, cutoff).unwrap();
        })
        .await;
    reap_once(&shared).await;

    let retry_id = {
        let inner = state.test_tx().await;
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
        // The abandoned attempt settles: every credential minted for it —
        // runtime token, debug token, renewals — stops validating, while its
        // step manifest stays for the log blobs it already uploaded.
        let abandoned = &inner.job_requests[&request_id];
        assert_eq!(
            abandoned.agent_job_id, old_agent_job_id,
            "the abandoned attempt keeps its identity for its logs"
        );
        assert_eq!(
            abandoned.result,
            Some(ExecutionStatus::Cancelled),
            "the abandoned attempt must settle"
        );
        assert!(
            !inner.inflight_requests.contains_key(&request_id),
            "the abandoned attempt must leave the in-flight set"
        );
        assert!(
            inner.job_steps.contains_key(&old_agent_job_id),
            "abandoned attempt step manifest must remain for log-blob mapping"
        );
        let retry_id = inner
            .inflight_requests
            .keys()
            .copied()
            .find(|id| {
                let record = &inner.job_requests[id];
                record.run_id == run_id && record.job_id == job_id
            })
            .expect("the retry must be in flight");
        let retry = &inner.job_requests[&retry_id];
        assert_ne!(
            retry.agent_job_id, old_agent_job_id,
            "a retry must rotate the runtime-token identity"
        );
        assert_ne!(
            retry.timeline_id, abandoned.timeline_id,
            "a retry must own its timeline"
        );
        assert!(
            inner.job_steps.contains_key(&retry.agent_job_id),
            "retry identity must receive a fresh step manifest"
        );
        assert!(
            state
                .inner
                .lock()
                .await
                .live_log_closed
                .contains(&old_agent_job_id.to_string()),
            "abandoned attempt live-log feed must close so followers exit"
        );
        let queued = &inner
            .ready()
            .find(|job| job.run_id == run_id && job.job_id == job_id)
            .expect("unfinished job must be requeued for a fresh machine")
            .message;
        assert_eq!(
            (queued.job_id, queued.request_id),
            (retry.agent_job_id, retry_id),
            "the queued message must carry the retry's identity"
        );
        assert_eq!(queued.plan.plan_id, retry.agent_job_id.to_string());
        assert_eq!(queued.timeline.id, retry.timeline_id);
        assert_eq!(retry.result, None, "the retry must remain completable");
        assert_eq!(
            retry.owner_runner_id, None,
            "the purged runner must not own the retry"
        );
        assert_eq!(retry.started_at, None);
        assert_eq!(retry.last_renewed_at, None);
        assert!(
            !retry.debug_token_issued,
            "a retried attempt must be allowed to mint a fresh debug token"
        );
        assert!(
            crate::runtime_scheduling::live_runner_assignments(
                &inner.job_requests,
                &inner.session_active_requests,
                SystemTime::now(),
            )
            .is_empty(),
            "status must not report the purged runner as executing"
        );
        retry_id
    };
    assert!(
        !crate::auth::job_is_live(&state, old_agent_job_id)
            .await
            .unwrap(),
        "the abandoned runtime identity must be revoked"
    );

    // A replacement claims the retry under its fresh runtime identity and
    // completes the logical job.
    let (replacement_id, replacement_token) =
        register_runner_with_token(&app, "replacement-runner", &["self-hosted"], None).await;
    let (_, replacement_session) =
        create_disttask_session(&app, &replacement_token, replacement_id).await;
    let replacement_session_id = replacement_session["sessionId"].as_str().unwrap();
    let delivered = poll_message(&app, &replacement_token, replacement_session_id).await;
    assert!(!delivered.is_null(), "replacement must receive the retry");
    assert_eq!(
        state
            .test_tx()
            .await
            .session_active_requests
            .get(replacement_session_id),
        Some(&retry_id),
        "the replacement must claim the retry, not the abandoned attempt"
    );
    request_json_with_bearer(
        &app,
        Method::PATCH,
        &format!("/_apis/v1/AgentRequest/1/{retry_id}"),
        json!({}),
        &replacement_token,
    )
    .await;
    request_json_with_bearer(
        &app,
        Method::PATCH,
        &format!("/_apis/v1/AgentRequest/1/{retry_id}"),
        json!({"result": "Succeeded"}),
        &replacement_token,
    )
    .await;
    let inner = state.test_tx().await;
    assert_eq!(
        inner.runs[&run_id].jobs[&job_id],
        ExecutionStatus::Success,
        "the replacement must be able to finish the retried job"
    );
    assert!(!inner.inflight_requests.contains_key(&retry_id));
}

#[tokio::test]
async fn liveness_sweep_fails_job_without_recovery_copy() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    let (runner_id, token) =
        register_runner_with_token(&app, "deaf-runner", &["self-hosted"], None).await;
    let (_, session) = create_disttask_session(&app, &token, runner_id).await;
    let session_id = session["sessionId"].as_str().unwrap().to_owned();
    let accepted = submit_simple_run(&app).await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let job_id = JobId("build".to_owned());
    let message = poll_message(&app, &token, &session_id).await;
    assert!(!message.is_null(), "poll must claim the queued job");
    let request_id = {
        let inner = state.test_tx().await;
        *inner
            .session_active_requests
            .get(&session_id)
            .expect("poll must pin the claim to the session")
    };

    // The runner goes deaf and the control plane restarts. The claim survives
    // — the job row is still `claimed`, off the durable ready queue — but the
    // restart did not restore the owning session, so there is no recovery
    // copy to redeliver and nobody can ever complete the job. The liveness
    // purge cannot reach this state at all (it always requeues a claimed job
    // it can attribute); the claim is only ever failed by the boot reconcile.
    state
        .test_db_mutate(|tx| {
            tx.execute(
                "DELETE FROM runner_sessions WHERE runner_id = ?1",
                rusqlite::params![runner_id],
            )
            .unwrap();
        })
        .await;
    let settled = crate::broker::reconcile_orphaned_claims(&shared).await;
    assert_eq!(settled, 1, "the unrecoverable claim must be settled once");

    let inner = state.test_tx().await;
    assert_eq!(inner.runs[&run_id].jobs[&job_id], ExecutionStatus::Failure);
    assert!(
        inner.runs[&run_id].completed_at.is_some(),
        "normal completion must conclude the run"
    );
    assert_eq!(
        inner.job_requests[&request_id].result,
        Some(ExecutionStatus::Failure)
    );
    assert!(
        !inner.inflight_requests.contains_key(&request_id),
        "normal completion must release the orphaned request"
    );
}

#[tokio::test]
async fn broker_session_keeps_registered_runner_from_phantom_reaping() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    let (runner_id, _token) =
        register_runner_with_token(&app, "broker-runner", &["self-hosted"], None).await;

    state.test_set_runner_liveness(Duration::from_secs(600));
    state
        .test_db_mutate(|tx| {
            tx.set_runner_registered_at(runner_id, crate::store::now_us() - 3_600_000_000)
                .unwrap();
            // Modern broker sessions are tracked separately from the legacy
            // AzDO session map. The runner must not be treated as a phantom when
            // only that row proves its session exists.
            tx.insert_session("broker-session", runner_id, "broker", true)
                .unwrap();
        })
        .await;

    reap_once(&shared).await;

    let inner = state.test_tx().await;
    assert!(
        inner.runners.contains_key(&runner_id),
        "a broker-backed runner must not be reaped as a phantom registration"
    );
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
    // Backdate the first-seen mark far past the grace window. The mark is
    // node-local reaper state.
    state.inner.lock().await.reaper_first_seen.insert(
        (run_id, JobId("build".to_owned())),
        SystemTime::now() - Duration::from_secs(300),
    );
    reap_once(&shared).await;
    {
        let inner = state.test_tx().await;
        assert_eq!(
            inner.ready().count(),
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
        let inner = state.test_tx().await;
        assert_eq!(
            inner.ready().count(),
            1,
            "a fresh grace window starts once the pool is ready"
        );
    }

    // With nothing having claimed the job after a full fresh window, the
    // sweep fails it as before.
    state.inner.lock().await.reaper_first_seen.insert(
        (run_id, JobId("build".to_owned())),
        SystemTime::now() - Duration::from_secs(300),
    );
    reap_once(&shared).await;
    {
        let inner = state.test_tx().await;
        assert!(
            inner.ready().next().is_none(),
            "a job nobody can claim still fails once the pool is ready"
        );
    }
}

#[tokio::test]
async fn restored_old_job_survives_the_restarted_pools_warm_window() {
    let temp = tempfile::tempdir().unwrap();
    let run_id = {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let accepted = submit_simple_run(&app).await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
        state
            .test_db_mutate(|tx| {
                let cutoff = crate::store::now_us() - 700_000_000;
                tx.set_job_enqueued_us(run_id, &JobId("build".to_owned()), Some(cutoff))
                    .unwrap();
            })
            .await;
        run_id
    };

    let mut restored = AppState::new(temp.path().to_path_buf()).await.unwrap();
    restored.pool_preparing = Some(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
        true,
    )));
    let shared = Arc::new(SharedState {
        state: restored.clone(),
        shutdown: CancellationToken::new(),
    });
    reap_once(&shared).await;

    let inner = restored.test_tx().await;
    assert_eq!(
        inner.ready().count(),
        1,
        "durable queue age must not defeat the fresh process-local pool warm"
    );
    assert_eq!(
        inner.runs[&run_id].jobs[&JobId("build".to_owned())],
        ExecutionStatus::Queued
    );
}

#[tokio::test]
async fn queued_job_starves_past_the_ceiling_even_while_the_pool_is_preparing() {
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());

    // The pool signals "preparing" indefinitely -- e.g. a provision that
    // keeps failing and retrying, or continuous successor prebuilds under
    // sustained load -- so the signal never clears. A job this pool can
    // never serve must still hit the bounded starvation failure path
    // instead of being masked forever.
    state.pool_preparing = Some(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
        true,
    )));
    state.started_at = std::time::Instant::now() - Duration::from_secs(3700);
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    let accepted = submit_simple_run(&app).await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // Age the job's ready-enqueue past the absolute ceiling.
    state
        .test_db_mutate(|tx| {
            tx.set_job_enqueued_us(
                run_id,
                &JobId("build".to_owned()),
                Some(crate::store::now_us() - 3_700_000_000),
            )
            .unwrap();
        })
        .await;
    reap_once(&shared).await;
    {
        let inner = state.test_tx().await;
        assert!(
            inner.ready().next().is_none(),
            "a job past the ceiling must starve even while the pool is preparing"
        );
        let run = inner.runs.get(&run_id).expect("run record must survive");
        assert_eq!(
            run.jobs.get(&JobId("build".to_owned())),
            Some(&ExecutionStatus::Failure),
            "the unschedulable job must fail, not queue forever"
        );
        assert_eq!(run.status, ExecutionStatus::Failure);
    }
}

#[tokio::test]
async fn starvation_sweep_publishes_terminal_run_status_for_a_failed_run() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    let accepted = submit_simple_run(&app).await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // Subscribe before the sweep so the emitted events are observable.
    let mut events = state.events.subscribe();

    // Backdate the job's ready-enqueue past the grace window and reap: the
    // job starves and the run concludes.
    state
        .test_db_mutate(|tx| {
            let cutoff = crate::store::now_us() - 300_000_000;
            tx.set_job_enqueued_us(run_id, &JobId("build".to_owned()), Some(cutoff))
                .unwrap();
        })
        .await;
    reap_once(&shared).await;

    // Drain the broadcast channel. The sweep must publish the terminal
    // RunStatus for the concluded run: the event stream only closes a
    // watcher connection on a terminal RunStatus, so without it `preloop
    // run` keeps waiting on a run the engine has already failed.
    let mut saw_terminal_run_status = false;
    let mut saw_job_failure = false;
    while let Ok(event) = events.try_recv() {
        match event {
            NdjsonEvent::RunStatus {
                run_id: id, status, ..
            } if id == run_id => {
                if status.is_terminal() {
                    saw_terminal_run_status = true;
                }
            }
            NdjsonEvent::JobStatus {
                run_id: id, status, ..
            } if id == run_id && status == ExecutionStatus::Failure => {
                saw_job_failure = true;
            }
            _ => {}
        }
    }
    assert!(
        saw_job_failure,
        "the sweep must emit the per-job failure event"
    );
    assert!(
        saw_terminal_run_status,
        "the sweep must publish a terminal RunStatus so `preloop run` returns \
         once the engine has failed the run"
    );
}

#[tokio::test]
async fn starvation_sweep_does_not_close_the_stream_while_jobs_remain_queued() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    // Two independent jobs in one run.
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  one:\n    runs-on: self-hosted\n    steps:\n      - run: echo one\n  two:\n    runs-on: self-hosted\n    steps:\n      - run: echo two\n",
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let mut events = state.events.subscribe();

    // Only the first job is old enough to starve; the second must survive.
    state
        .test_db_mutate(|tx| {
            let cutoff = crate::store::now_us() - 300_000_000;
            tx.set_job_enqueued_us(run_id, &JobId("one".to_owned()), Some(cutoff))
                .unwrap();
        })
        .await;
    reap_once(&shared).await;

    {
        let inner = state.test_tx().await;
        assert_eq!(
            inner
                .runs
                .get(&run_id)
                .unwrap()
                .jobs
                .get(&JobId("one".to_owned())),
            Some(&ExecutionStatus::Failure),
            "the overdue job must fail"
        );
        assert_eq!(
            inner
                .runs
                .get(&run_id)
                .unwrap()
                .jobs
                .get(&JobId("two".to_owned())),
            Some(&ExecutionStatus::Queued),
            "the surviving job must stay queued"
        );
        assert!(
            inner.ready().any(|job| job.job_id.0 == "two"),
            "the surviving job must stay on the ready queue"
        );
    }

    // The published run status must be non-terminal: a terminal RunStatus
    // would close the watcher's event stream while the run is still live.
    let mut saw_non_terminal_run_status = false;
    let mut saw_terminal_run_status = false;
    while let Ok(event) = events.try_recv() {
        if let NdjsonEvent::RunStatus {
            run_id: id, status, ..
        } = event
        {
            if id == run_id {
                if status.is_terminal() {
                    saw_terminal_run_status = true;
                } else {
                    saw_non_terminal_run_status = true;
                }
            }
        }
    }
    assert!(
        saw_non_terminal_run_status,
        "the sweep must publish the run's updated (non-terminal) status"
    );
    assert!(
        !saw_terminal_run_status,
        "the sweep must not publish a terminal RunStatus while a job is still queued"
    );
}

#[tokio::test]
async fn starvation_sweep_publishes_final_run_status_when_every_job_starves() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    // Two independent jobs in one run; both will starve on the same sweep.
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  one:\n    runs-on: self-hosted\n    steps:\n      - run: echo one\n  two:\n    runs-on: self-hosted\n    steps:\n      - run: echo two\n",
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let mut events = state.events.subscribe();

    // Both jobs old enough to starve: the sweep fails them together and the
    // run concludes.
    state
        .test_db_mutate(|tx| {
            let cutoff = crate::store::now_us() - 300_000_000;
            for job in ["one", "two"] {
                tx.set_job_enqueued_us(run_id, &JobId(job.to_owned()), Some(cutoff))
                    .unwrap();
            }
        })
        .await;
    reap_once(&shared).await;

    {
        let inner = state.test_tx().await;
        let run = inner.runs.get(&run_id).unwrap();
        assert!(
            run.status.is_terminal(),
            "the run must conclude once every job starved"
        );
    }

    // The published RunStatus must carry the run's conclusion, not the
    // intermediate status from the first failed job: the event stream only
    // closes a watcher on the final terminal status.
    let mut saw_terminal_run_status = false;
    let mut saw_non_terminal_run_status = false;
    while let Ok(event) = events.try_recv() {
        if let NdjsonEvent::RunStatus {
            run_id: id, status, ..
        } = event
        {
            if id == run_id {
                if status.is_terminal() {
                    saw_terminal_run_status = true;
                } else {
                    saw_non_terminal_run_status = true;
                }
            }
        }
    }
    assert!(
        saw_terminal_run_status,
        "the sweep must publish the run's concluding (terminal) status when \
         every job starves, so `preloop run` returns"
    );
    assert!(
        !saw_non_terminal_run_status,
        "the sweep must not publish a stale intermediate status for a concluded run"
    );
}

#[tokio::test]
async fn queued_job_does_not_starve_while_a_runner_is_being_provisioned() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    let accepted = submit_simple_run(&app).await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // The on-demand pool publishes an in-flight provision while it builds a
    // runner; the sweep must not count that provisioning time against the
    // starvation limit.
    state.pool_status.set_provisioning(1);

    // Age the job past the 120s short grace but well under the absolute
    // backstop: with provisioning in flight it must not starve.
    state
        .test_db_mutate(|tx| {
            let cutoff = crate::store::now_us() - 300_000_000;
            tx.set_job_enqueued_us(run_id, &JobId("build".to_owned()), Some(cutoff))
                .unwrap();
        })
        .await;
    reap_once(&shared).await;

    {
        let inner = state.test_tx().await;
        assert_eq!(
            inner.ready().count(),
            1,
            "a job whose runner is being provisioned must not starve past the short grace"
        );
        assert_eq!(
            inner.runs.get(&run_id).unwrap().status,
            ExecutionStatus::Queued,
            "the run must still be live while provisioning is in flight"
        );
    }
}

#[tokio::test]
async fn starvation_clock_ignores_provisioning_time_across_a_retry_gap() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    let accepted = submit_simple_run(&app).await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // The job has been queued for 200s — past the 120s short grace — and a
    // provision is in flight for it.
    state.pool_status.set_provisioning(1);
    state
        .test_db_mutate(|tx| {
            let cutoff = crate::store::now_us() - 200_000_000;
            tx.set_job_enqueued_us(run_id, &JobId("build".to_owned()), Some(cutoff))
                .unwrap();
        })
        .await;
    // While provisioning is in flight the sweep holds the job and re-stamps
    // its observation clock instead of letting the 200s of provisioning
    // time count against the short grace.
    reap_once(&shared).await;

    // The provision fails; the pool backs off before retrying. During the
    // gap no provision is in flight, but the job must still not starve:
    // its clock was reset when provisioning was last observed, not when
    // the job was first queued.
    state.pool_status.set_provisioning(0);
    reap_once(&shared).await;

    {
        let inner = state.test_tx().await;
        assert_eq!(
            inner.ready().count(),
            1,
            "a job must not starve in a provisioning retry gap for time spent provisioning"
        );
        assert_eq!(
            inner.runs.get(&run_id).unwrap().status,
            ExecutionStatus::Queued,
            "the run must still be live while the pool retries its provision"
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

    // Submit run
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

    // Poll to start job (transitions status to InProgress and sets started_at)
    let _msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;

    let request_id = {
        let inner = state.test_tx().await;
        *inner.job_requests.keys().next().unwrap()
    };

    // Override started_at to be in the past (beyond 360m/21600s default timeout)
    state
        .test_db_mutate(|tx| {
            tx.update_request(
                request_id,
                Some(crate::store::now_us() - 22_000_000_000),
                None,
                None,
                None,
            )
            .unwrap();
        })
        .await;

    // Run reaper tick
    reap_once(&shared).await;

    // Verify cancellation is enqueued
    {
        let inner = state.test_tx().await;
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
        let inner = state.test_tx().await;
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
    state
        .test_db_mutate(|tx| {
            tx.update_request(
                request_id,
                Some(crate::store::now_us() - 10_000_000_000),
                None,
                None,
                None,
            )
            .unwrap();
        })
        .await;
    {
        let mut inner = state.inner.lock().await;
        let past = SystemTime::now() - Duration::from_secs(10_000);
        inner
            .debug_sessions
            .backdate_pause_for_test(&session_id, past);
    }

    reap_once(&shared).await;

    {
        let inner = state.test_tx().await;
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
        &format!("/api/v1/debug/sessions/{session_id}/lease"),
        json!({
            "controller": "test-agent",
            "capabilities": ["job.retry_from"]
        }),
    )
    .await;
    assert_eq!(lease["controller"], "test-agent");
    assert_eq!(
        lease["capabilities"],
        json!(["step.retry", "job.retry_from", "job.continue", "job.abort"])
    );

    let events = request_json(
        &app,
        Method::GET,
        &format!("/api/v1/debug/sessions/{session_id}/events?after=0"),
        Value::Null,
    )
    .await;
    assert_eq!(events["events"][0]["event"], "step_failed");

    // The agent retries from the first step; the worker's long poll picks it
    // up through the same verdict state machine as the human CLI.
    let operation = request_json(
        &app,
        Method::POST,
        &format!("/api/v1/debug/sessions/{session_id}/operations"),
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
        &format!("/api/v1/debug/sessions/{session_id}/audit"),
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
    let banked = {
        let inner = state.inner.lock().await;
        inner
            .debug_sessions
            .paused_for_request(request_id, SystemTime::now())
    };
    assert!(
        banked >= Duration::from_secs(9_500),
        "the pause should have banked its full duration, got {banked:?}"
    );
    state
        .test_db_mutate(|tx| {
            let us = crate::store::now_us() - (21_700_000_000 + banked.as_micros() as i64);
            tx.update_request(request_id, Some(us), None, None, None)
                .unwrap();
        })
        .await;

    reap_once(&shared).await;
    {
        let inner = state.test_tx().await;
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
        let inner = state.test_tx().await;
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
        let inner = state.test_tx().await;
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

    // A controller leases the session and queues a continue operation for the
    // paused worker.
    let lease = request_json(
        &app,
        Method::POST,
        &format!("/api/v1/debug/sessions/{session_id}/lease"),
        json!({ "controller": "cli" }),
    )
    .await;
    request_json(
        &app,
        Method::POST,
        &format!("/api/v1/debug/sessions/{session_id}/operations"),
        json!({
            "request_id": "test-continue-1",
            "expected_version": lease["session_version"],
            "lease_id": lease["lease_id"],
            "operation": { "operation": "continue" }
        }),
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

    // The stored message is a secret-free template: `NPM_TOKEN` arrives only
    // as a *name* in `preloopSecretSpec`, never as a value — so the debug
    // credential is structurally absent, not merely filtered.
    let wire = {
        let inner = state.test_tx().await;
        let queued = inner.ready().next().expect("job should be queued");
        serde_json::to_value(&queued.message).unwrap()
    };

    assert!(
        !wire.to_string().contains("debug_worker_token"),
        "the debug credential must not ship anywhere on the job message"
    );
    let spec = &wire["preloopSecretSpec"];
    let names: BTreeSet<&str> = spec["names"]
        .as_array()
        .expect("the template carries its secret-name spec")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(
        names.contains("NPM_TOKEN"),
        "the spec must carry the run's own secret names: {names:?}"
    );
    assert!(
        !names.iter().any(|name| name.contains("debug_worker_token")),
        "the debug credential must not be a declared secret name"
    );

    // The real claim path is what the runner consumes: the filled message's
    // secret variables are the official runner's secrets context source.
    let acquired = acquire_queued_job(&app, "debug-token-runner").await;
    let acquired_wire = serde_json::to_string(&acquired).unwrap();
    assert!(
        !acquired_wire.contains("debug_worker_token"),
        "the acquired message must not expose the debug credential"
    );
    assert_eq!(
        wire_variable(&acquired, "NPM_TOKEN"),
        Some("npm_LIVE_CREDENTIAL"),
        "the filled message surfaces the run's secrets at acquire"
    );
}

/// Open a debug session as a worker would, for exchange tests.

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
        let inner = state.test_tx().await;
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
        let inner = state.test_tx().await;
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

    let run_id = {
        let inner = state.test_tx().await;
        inner.runs.keys().next().copied().expect("submitted run")
    };
    let (agent_job_id, plan_id) = state
        .test_db_mutate(|tx| {
            let (request_id, agent_job_id, _) = tx
                .request_key_for(run_id, &JobId("build".to_owned()))
                .unwrap()
                .expect("dispatched request");
            tx.update_request(request_id, None, None, Some("failure"), None)
                .unwrap();
            (agent_job_id, agent_job_id.to_string())
        })
        .await;

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
        let inner = state.test_tx().await;
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
    let past = SystemTime::now() - ceiling - Duration::from_secs(22_000);
    state
        .test_db_mutate(|tx| {
            let us = crate::store::now_us()
                - (crate::debug_sessions::MAX_PAUSE_CREDIT.as_micros()
                    + std::time::Duration::from_secs(22_000).as_micros()) as i64;
            tx.update_request(request_id, Some(us), None, None, None)
                .unwrap();
        })
        .await;
    {
        let mut inner = state.inner.lock().await;
        inner
            .debug_sessions
            .backdate_pause_for_test(&session_id, past);
    }

    reap_once(&shared).await;

    let inner = state.test_tx().await;
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
        let inner = state.test_tx().await;
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
        inner
            .debug_sessions
            .backdate_pause_for_test(&session_id, SystemTime::now() - Duration::from_secs(10_000));
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

    let inner = state.test_tx().await;
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
    let inner_local = state.inner.lock().await;
    assert!(
        inner_local
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
            let inner = state.test_tx().await;
            let q = inner.ready().next().cloned().unwrap();
            q
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

    // Submit run
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

    // Poll to start job (sets last_renewed_at)
    let _msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;

    let request_id = {
        let inner = state.test_tx().await;
        *inner.job_requests.keys().next().unwrap()
    };

    // Exercise the just-before-boundary case without sleeping. The session
    // must be dead here: a *live* session with a stale lease is the hung-
    // worker case (reaped at HUNG_WORKER_LEASE_SECONDS), not the disconnect
    // boundary this test isolates. Age the session past liveness so only the
    // lease clock decides.
    let liveness = state.test_tx().await.runner_liveness_timeout;
    state
        .test_db_mutate(|tx| {
            let now = crate::store::now_us();
            tx.set_session_seen("default", now - liveness.as_micros() as i64 - 1_000_000)
                .unwrap();
            tx.set_lease_renewed(
                request_id,
                1,
                now - (JOB_LEASE_SECONDS as i64 - 1) * 1_000_000,
                now + JOB_LEASE_SECONDS as i64 * 1_000_000,
            )
            .unwrap();
        })
        .await;

    reap_once(&shared).await;
    {
        let inner = state.test_tx().await;
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

    // Move just beyond the same production lease boundary and reap.
    state
        .test_db_mutate(|tx| {
            tx.set_lease_renewed(
                request_id,
                1,
                crate::store::now_us() - (JOB_LEASE_SECONDS as i64 + 1) * 1_000_000,
                crate::store::now_us() - 1_000_000,
            )
            .unwrap();
        })
        .await;

    reap_once(&shared).await;

    // Verify the job was marked failed and run completes as failed
    {
        let inner = state.test_tx().await;
        let request = inner.job_requests.get(&request_id).unwrap();
        assert_eq!(request.result, Some(ExecutionStatus::Failure));
        assert!(inner.inflight_requests.is_empty());
        assert!(inner.session_active_requests.is_empty());

        let run = inner.runs.get(&run_id).unwrap();
        assert_eq!(run.status, ExecutionStatus::Failure);
    }
}
