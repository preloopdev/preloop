//! Behavioral regressions for the job-owned GitHub App token lifecycle.
//!
//! Every test drives the real broker/control path against a loopback GitHub
//! stub: a token is minted at claim, accepted while its attempt is live,
//! rejected as soon as the attempt is terminal (before any online validation
//! or cache), and revoked with `DELETE /installation/token` authenticated by
//! that same token. Sibling jobs, failed revocations, restart recovery and the
//! operator's `error` policy are covered separately.

mod common;

use common::*;

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

/// One acquired job plus the identifiers/credential its message carried.
struct Claimed {
    token: String,
    run_id: preloop_gha_protocol::RunId,
    job_id: preloop_gha_protocol::JobId,
    agent_job_id: uuid::Uuid,
}

async fn claimed_from_message(state: &AppState, message: Value) -> Claimed {
    let token = wire_variable(&message, "system.github.token")
        .expect("the App-backed job carries a GITHUB_TOKEN")
        .to_owned();
    let plan_id = message["plan"]["planId"]
        .as_str()
        .expect("plan id present")
        .to_owned();
    let agent_job_id: uuid::Uuid = plan_id.parse().expect("plan id is the agent job id");
    let request = {
        let inner = state.test_tx().await;
        inner
            .job_requests
            .values()
            .find(|request| request.agent_job_id == agent_job_id)
            .cloned()
            .expect("acquired attempt exists in the control database")
    };
    Claimed {
        token,
        run_id: request.run_id,
        job_id: request.job_id,
        agent_job_id,
    }
}

async fn complete(shared: &Arc<SharedState>, claimed: &Claimed) {
    let _ = crate::distributed_task::complete_job_inner(
        shared.clone(),
        preloop_gha_protocol::JobCompletion {
            run_id: claimed.run_id,
            job_id: claimed.job_id.clone(),
            agent_job_id: Some(claimed.agent_job_id),
            status: ExecutionStatus::Success,
            outputs: Default::default(),
            annotations: Vec::new(),
            step_results: Vec::new(),
            environment_url: None,
        },
    )
    .await
    .expect("job completion succeeds");
}

async fn dispatch_status(app: &Router, token: &str) -> StatusCode {
    let request = Request::builder()
        .method(Method::GET)
        .uri("/repos/owner/repo/actions/runs")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(request).await.unwrap().status()
}

/// A GitHub stub that mints a fresh installation token per call, accepts the
/// self-authenticated revoke, and reports minted tokens while the job lives.
struct GithubStub {
    base: String,
    mints: Arc<AtomicUsize>,
    revokes: Arc<Mutex<Vec<String>>>,
    revoke_failures: Arc<AtomicUsize>,
    unexpected_successes: Arc<AtomicUsize>,
}

async fn github_stub(revoke_failures: usize) -> GithubStub {
    github_stub_full(revoke_failures, 0, None).await
}

/// `expiry_secs`: mint tokens expiring that many seconds from now; `None`
/// uses a far-future expiry.
async fn github_stub_full(
    revoke_failures: usize,
    unexpected_successes: usize,
    expiry_secs: Option<u64>,
) -> GithubStub {
    let mints = Arc::new(AtomicUsize::new(0));
    let revokes = Arc::new(Mutex::new(Vec::new()));
    let failures = Arc::new(AtomicUsize::new(revoke_failures));
    let unexpected = Arc::new(AtomicUsize::new(unexpected_successes));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .route(
            "/app/installations",
            get(|| async { Json(json!([{"id": 4242, "account": {"login": "owner"}}])) }),
        )
        .route(
            "/app/installations/{installation_id}/access_tokens",
            post({
                let mints = mints.clone();
                move |Path(installation_id): Path<u64>| {
                    let mints = mints.clone();
                    async move {
                        assert_eq!(installation_id, 4242);
                        let call = mints.fetch_add(1, Ordering::SeqCst) + 1;
                        Json(json!({
                            "token": format!("ghs_job_{call}"),
                            "expires_at": expiry_secs.map_or_else(
                                || "2999-01-01T00:00:00Z".to_owned(),
                                |seconds| {
                                    (chrono::Utc::now()
                                        + chrono::Duration::seconds(seconds as i64))
                                    .to_rfc3339()
                                },
                            )
                        }))
                    }
                }
            }),
        )
        // The online path an unledgered token would fall back to. Returning a
        // valid installation proves the terminal-token gate wins before it.
        .route(
            "/installation",
            get(|| async {
                Json(json!({
                    "id": 4242,
                    "account": {"login": "owner"},
                    "app_id": 424,
                    "app_slug": "preloop-test",
                    "permissions": {"contents": "read"}
                }))
            }),
        )
        .route(
            "/installation/repositories",
            get(|| async {
                Json(json!({"total_count": 1, "repositories": [{"full_name": "owner/repo"}]}))
            }),
        )
        .route(
            "/installation/token",
            delete({
                let revokes = revokes.clone();
                let failures = failures.clone();
                let unexpected = unexpected.clone();
                move |headers: HeaderMap| {
                    let revokes = revokes.clone();
                    let failures = failures.clone();
                    async move {
                        let authorization = headers
                            .get(header::AUTHORIZATION)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_owned();
                        revokes.lock().unwrap().push(authorization);
                        if failures.load(Ordering::SeqCst) > 0 {
                            failures.fetch_sub(1, Ordering::SeqCst);
                            return StatusCode::SERVICE_UNAVAILABLE.into_response();
                        }
                        if unexpected.load(Ordering::SeqCst) > 0 {
                            unexpected.fetch_sub(1, Ordering::SeqCst);
                            return StatusCode::OK.into_response();
                        }
                        StatusCode::NO_CONTENT.into_response()
                    }
                }
            }),
        );
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    GithubStub {
        base,
        mints,
        revokes,
        revoke_failures: failures,
        unexpected_successes: unexpected,
    }
}

fn app_credentials(
    policy: crate::github_app::MintFailurePolicy,
) -> crate::github_app::GitHubAppCredentials {
    let key = rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).unwrap();
    crate::github_app::GitHubAppCredentials::for_tests("424", key, policy)
}

/// A job token is accepted while its attempt is live; after completion it is
/// rejected immediately, revoked per job, and a sibling job's token is not
/// revoked. Revocation is triggered by the lifecycle reconciler, the same
/// entry point the 10s reaper uses.
#[tokio::test]
async fn job_tokens_are_revoked_per_terminal_attempt_and_siblings_survive() {
    let stub = github_stub(0).await;
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &stub.base);
    let _pat = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let _policy = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_APP_MINT_FAILURE");

    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.github_app = Some(app_credentials(
        crate::github_app::MintFailurePolicy::LocalJwt,
    ));
    let app = app(state.clone(), CancellationToken::new());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });

    let yaml = "on: push\njobs:\n  first:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo first\n  sibling:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo sibling\n";
    submit_yaml(&app, yaml, "owner/repo").await;

    let first = claimed_from_message(&state, acquire_queued_job(&app, "revoke-first").await).await;
    let sibling =
        claimed_from_message(&state, acquire_queued_job(&app, "revoke-sibling").await).await;
    assert_eq!(stub.mints.load(Ordering::SeqCst), 2);

    assert_eq!(
        dispatch_status(&app, &first.token).await,
        StatusCode::OK,
        "a live attempt's token authenticates a dispatch"
    );
    assert_eq!(
        dispatch_status(&app, &sibling.token).await,
        StatusCode::OK,
        "the sibling token is independently live"
    );

    complete(&shared, &first).await;
    assert_eq!(
        crate::github_token_revocation::token_dispatchable_for_tests(&shared, &first.token).await,
        Some(false),
        "terminal attempt is rejected immediately, before the reconciler runs"
    );

    let report = crate::github_token_revocation::reconcile_at_for_tests(
        &shared,
        std::time::SystemTime::now(),
        &stub.base,
    )
    .await;
    assert_eq!(report.revoked, 1, "one terminal token revoked: {report:?}");
    assert_eq!(
        dispatch_status(&app, &first.token).await,
        StatusCode::UNAUTHORIZED,
        "the revoked token stays rejected even though the online stub would accept it"
    );
    assert_eq!(
        dispatch_status(&app, &sibling.token).await,
        StatusCode::OK,
        "a live sibling's token must not be revoked"
    );

    let revoked = stub.revokes.lock().unwrap().clone();
    assert_eq!(revoked, vec![format!("token {}", first.token)]);
    assert_eq!(stub.revoke_failures.load(Ordering::SeqCst), 0);

    complete(&shared, &sibling).await;
    let report = crate::github_token_revocation::reconcile_at_for_tests(
        &shared,
        std::time::SystemTime::now(),
        &stub.base,
    )
    .await;
    assert_eq!(report.revoked, 1);
    let revoked = stub.revokes.lock().unwrap().clone();
    assert_eq!(revoked.len(), 2);
    assert_eq!(revoked[1], format!("token {}", sibling.token));
}

/// A failed revoke is retained, the token stays rejected because its attempt
/// is terminal, and a later pass retries the same credential. This is the
/// outage window: the engine never claims the credential was revoked.
#[tokio::test]
async fn failed_revocation_is_retained_and_retried() {
    let stub = github_stub(1).await;
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &stub.base);
    let _pat = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let _policy = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_APP_MINT_FAILURE");

    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.github_app = Some(app_credentials(
        crate::github_app::MintFailurePolicy::LocalJwt,
    ));
    let app = app(state.clone(), CancellationToken::new());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });

    submit_yaml(
        &app,
        "on: push\njobs:\n  only:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "owner/repo",
    )
    .await;
    let claimed =
        claimed_from_message(&state, acquire_queued_job(&app, "retry-runner").await).await;
    complete(&shared, &claimed).await;

    let now = std::time::SystemTime::now();
    let report =
        crate::github_token_revocation::reconcile_at_for_tests(&shared, now, &stub.base).await;
    assert_eq!(report.retried, 1, "the 503 keeps the row: {report:?}");
    assert_eq!(report.revoked, 0);
    assert_eq!(stub.revokes.lock().unwrap().len(), 1);
    assert_eq!(
        crate::github_token_revocation::token_dispatchable_for_tests(&shared, &claimed.token).await,
        Some(false),
        "remote failure does not re-enable a terminal job token"
    );

    // The first failure scheduled a retry in the future; advance the clock.
    let later = now + std::time::Duration::from_secs(11);
    let report =
        crate::github_token_revocation::reconcile_at_for_tests(&shared, later, &stub.base).await;
    assert_eq!(
        report.revoked, 1,
        "the retained row was retried: {report:?}"
    );
    let revoked = stub.revokes.lock().unwrap().clone();
    assert_eq!(revoked.len(), 2);
    assert_eq!(revoked[0], revoked[1], "the same token is retried");
}

/// Pending revocations survive a process restart: a new AppState over the same
/// state directory decrypts the sealed token with the cluster key and revokes
/// it on the next pass, without relying on any in-memory ledger.
#[tokio::test]
async fn pending_revocation_survives_restart() {
    let stub = github_stub(1).await;
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &stub.base);
    let _pat = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let _policy = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_APP_MINT_FAILURE");

    let temp = tempfile::tempdir().unwrap();
    let token = {
        let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        state.github_app = Some(app_credentials(
            crate::github_app::MintFailurePolicy::LocalJwt,
        ));
        let app = app(state.clone(), CancellationToken::new());
        let shared = Arc::new(SharedState {
            state: state.clone(),
            shutdown: CancellationToken::new(),
        });
        submit_yaml(
            &app,
            "on: push\njobs:\n  only:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "owner/repo",
        )
        .await;
        let claimed =
            claimed_from_message(&state, acquire_queued_job(&app, "restart-runner").await).await;
        complete(&shared, &claimed).await;
        let now = std::time::SystemTime::now();
        let report =
            crate::github_token_revocation::reconcile_at_for_tests(&shared, now, &stub.base).await;
        assert_eq!(report.retried, 1, "first pass fails and keeps the row");
        claimed.token
        // `app`, `shared` and `state` drop here; the process would restart.
    };

    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    assert_eq!(
        crate::github_token_revocation::token_dispatchable_for_tests(&shared, &token).await,
        Some(false),
        "an offline engine denies a terminal tracked token after restart, before any sweep"
    );
    let later = std::time::SystemTime::now() + std::time::Duration::from_secs(20);
    let report =
        crate::github_token_revocation::reconcile_at_for_tests(&shared, later, &stub.base).await;
    assert_eq!(
        report.revoked, 1,
        "the pending row was recovered and revoked after restart: {report:?}"
    );
    let revoked = stub.revokes.lock().unwrap().clone();
    assert_eq!(revoked.len(), 2);
    assert_eq!(revoked[1], format!("token {token}"));
    assert_eq!(
        crate::github_token_revocation::token_dispatchable_for_tests(&shared, &token).await,
        Some(false)
    );
}

/// `200 OK` is not GitHub's revoke contract (`204 No Content`). Treat it as
/// unproven: the row is retained, the token stays denied, and a later 204
/// completes the revoke.
#[tokio::test]
async fn unexpected_2xx_revocation_is_not_treated_as_revoked() {
    let stub = github_stub_full(0, 1, None).await;
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &stub.base);
    let _pat = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let _policy = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_APP_MINT_FAILURE");

    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.github_app = Some(app_credentials(
        crate::github_app::MintFailurePolicy::LocalJwt,
    ));
    let app = app(state.clone(), CancellationToken::new());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    submit_yaml(
        &app,
        "on: push\njobs:\n  only:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "owner/repo",
    )
    .await;
    let claimed = claimed_from_message(
        &state,
        acquire_queued_job(&app, "unexpected-ok-runner").await,
    )
    .await;
    complete(&shared, &claimed).await;

    let now = std::time::SystemTime::now();
    let report =
        crate::github_token_revocation::reconcile_at_for_tests(&shared, now, &stub.base).await;
    assert_eq!(
        report.revoked, 0,
        "a 200 is not proof of revocation: {report:?}"
    );
    assert_eq!(report.retried, 1);
    assert_eq!(stub.unexpected_successes.load(Ordering::SeqCst), 0);

    let later = now + std::time::Duration::from_secs(11);
    let report =
        crate::github_token_revocation::reconcile_at_for_tests(&shared, later, &stub.base).await;
    assert_eq!(
        report.revoked, 1,
        "the retained row retried and got 204: {report:?}"
    );
    assert_eq!(
        crate::github_token_revocation::token_dispatchable_for_tests(&shared, &claimed.token).await,
        Some(false)
    );
}

/// A tracked token whose own expiry has passed is denied immediately, even
/// while its attempt/lease is live and an online validation cache might still
/// hold it.
#[tokio::test]
async fn expired_tracked_token_is_denied_while_job_is_live() {
    let stub = github_stub_full(0, 0, Some(30)).await;
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &stub.base);
    let _pat = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let _policy = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_APP_MINT_FAILURE");

    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.github_app = Some(app_credentials(
        crate::github_app::MintFailurePolicy::LocalJwt,
    ));
    let app = app(state.clone(), CancellationToken::new());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    submit_yaml(
        &app,
        "on: push\njobs:\n  only:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "owner/repo",
    )
    .await;
    let claimed = claimed_from_message(
        &state,
        acquire_queued_job(&app, "expired-token-runner").await,
    )
    .await;
    assert_eq!(
        crate::github_token_revocation::token_dispatchable_for_tests(&shared, &claimed.token).await,
        Some(true),
        "the live, unexpired token is dispatchable"
    );
    assert_eq!(
        crate::github_token_revocation::token_dispatchable_at_for_tests(
            &shared,
            &claimed.token,
            std::time::SystemTime::now() + std::time::Duration::from_secs(31),
        )
        .await,
        Some(false),
        "past its own expires_at the token is denied while the lease is still valid"
    );
}

/// An invalid mint-failure value cannot be ignored just because no App is
/// configured: startup fails, so a typo never degrades into a silent fallback.
#[tokio::test]
async fn invalid_mint_failure_policy_is_a_startup_error_without_app() {
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _bad = crate::state::TestEnvVar::set("PRELOOP_GITHUB_APP_MINT_FAILURE", "fallback");
    let temp = tempfile::tempdir().unwrap();
    let error = AppState::new(temp.path().to_path_buf())
        .await
        .err()
        .expect("invalid policy must refuse startup");
    assert!(
        format!("{error:#}").contains("PRELOOP_GITHUB_APP_MINT_FAILURE"),
        "startup error must name the setting: {error:#}"
    );
}

/// `mint_failure=error` with a static PAT configured but no App at all must
/// refuse the job. The common broker fallback must not inject the PAT.
#[tokio::test]
async fn error_policy_without_app_refuses_and_never_injects_static_pat() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new().route(
        "/",
        get(|| async { ([("X-OAuth-Scopes", "read:org")], Json(json!({}))) }),
    );
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &base);
    let _pat = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "ghp_static_pat");
    let _policy = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_APP_MINT_FAILURE");

    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    assert!(state.github_app.is_none(), "test must run without an App");
    state.github_mint_failure = crate::github_app::MintFailurePolicy::Error;
    let app = app(state.clone(), CancellationToken::new());

    submit_yaml(
        &app,
        "on: push\njobs:\n  only:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "owner/repo",
    )
    .await;

    let (status, body) = try_acquire_queued_job(&app, "strict-no-app").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(
        body.to_string()
            .contains("PRELOOP_GITHUB_APP_MINT_FAILURE=error"),
        "the refusal must name the policy: {body}"
    );
    // The claim was failed, not left queued for a retry that loops forever.
    let inner = state.test_tx().await;
    assert_eq!(inner.ready().count(), 0, "the refused job is not retried");
}

/// A configured App whose mint fails under `error` must also fail the claimed
/// job — even with a static PAT available.
#[tokio::test]
async fn error_policy_with_failing_app_refuses_dispatch() {
    let mints = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .route(
            "/app/installations",
            get(|| async { Json(json!([{"id": 4242, "account": {"login": "owner"}}])) }),
        )
        .route(
            "/app/installations/{installation_id}/access_tokens",
            post({
                let mints = mints.clone();
                move || {
                    let mints = mints.clone();
                    async move {
                        mints.fetch_add(1, Ordering::SeqCst);
                        (StatusCode::INTERNAL_SERVER_ERROR, "upstream broke").into_response()
                    }
                }
            }),
        );
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &base);
    let _pat = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "ghp_static_pat");
    let _policy = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_APP_MINT_FAILURE");

    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.github_app = Some(app_credentials(crate::github_app::MintFailurePolicy::Error));
    let app = app(state.clone(), CancellationToken::new());

    submit_yaml(
        &app,
        "on: push\njobs:\n  only:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "owner/repo",
    )
    .await;

    let (status, body) = try_acquire_queued_job(&app, "strict-app").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "body={body}");
    assert_eq!(mints.load(Ordering::SeqCst), 1);
}

/// Normal local/PAT mode is unchanged: no App and the default policy still
/// embeds a verified static PAT in the job message.
#[tokio::test]
async fn local_pat_mode_still_delivers_the_verified_pat() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new().route(
        "/",
        get(|| async { ([("X-OAuth-Scopes", "read:org")], Json(json!({}))) }),
    );
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &base);
    let _pat = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "ghp_keep_pat");
    let _policy = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_APP_MINT_FAILURE");

    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    submit_yaml(
        &app,
        "on: push\njobs:\n  only:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "owner/repo",
    )
    .await;
    let message = acquire_queued_job(&app, "local-pat-runner").await;
    assert_eq!(
        wire_variable(&message, "system.github.token"),
        Some("ghp_keep_pat"),
        "PAT support remains available in the default local mode"
    );
}

/// Same as `acquire_queued_job`, but returns the failure instead of panicking
/// so a refused claim's status/body can be asserted.
async fn try_acquire_queued_job(app: &Router, runner_name: &str) -> (StatusCode, Value) {
    let (runner_id, runner_token) =
        register_runner_with_token(app, runner_name, &["self-hosted"], None).await;
    let session = request_json_with_bearer(
        app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({
            "agent": {"id": runner_id, "name": runner_name},
            "ownerName": "acquire test",
            "sessionId": "00000000-0000-0000-0000-000000000000",
            "useFipsEncryption": false
        }),
        &runner_token,
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();
    let broker_message = request_json_with_bearer(
        app,
        Method::GET,
        &format!(
            "/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&waitSeconds=0"
        ),
        Value::Null,
        &runner_token,
    )
    .await;
    let body: Value = serde_json::from_str(broker_message["body"].as_str().unwrap()).unwrap();
    let request_id = body["runner_request_id"].as_str().unwrap();
    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("/broker/{runner_id}/acquirejob"))
        .header(header::AUTHORIZATION, format!("Bearer {runner_token}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "jobMessageId": request_id,
                "billingOwnerId": "local",
                "runnerOS": "linux"
            })
            .to_string(),
        ))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}
