//! HTTP-level tests for the pause-on-failure client.
//!
//! These run a real axum server on a loopback port and drive
//! [`DebugPauseClient`] against it. A hand-rolled fake would not exercise serde
//! round-trips, bearer auth, status handling, or the long-poll timeout path —
//! and every bug this feature has hit so far lived in exactly those seams.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use aksh_gha_protocol::debug_session::{FailedStep, Verdict};
use aksh_gha_protocol::{JobId, RunId};
use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

use super::debug_pause::DebugPauseClient;

/// What the fake control plane observed and will answer.
#[derive(Default)]
struct Fake {
    /// Verdict handed out once `polls` reaches `verdict_after`.
    verdict: Option<Verdict>,
    /// Empty polls to serve before answering, simulating a human thinking.
    verdict_after: u32,
    polls: AtomicU32,
    opens: AtomicU32,
    closes: AtomicU32,
    last_open: parking_lot::Mutex<Option<Value>>,
    last_close_state: parking_lot::Mutex<Option<String>>,
    /// The client's pause flag, sampled from inside a verdict poll — that is,
    /// at a moment the worker is provably blocked.
    pause_probe: parking_lot::Mutex<Option<Arc<AtomicBool>>>,
    probe_saw_paused: AtomicBool,
}

async fn spawn_fake(fake: Arc<Fake>) -> String {
    let app = Router::new()
        .route(
            "/api/v1/debug/sessions",
            post(
                |State(fake): State<Arc<Fake>>, Json(body): Json<Value>| async move {
                    fake.opens.fetch_add(1, Ordering::SeqCst);
                    *fake.last_open.lock() = Some(body);
                    Json(json!({ "session_id": "dbg_test" }))
                },
            ),
        )
        .route(
            "/api/v1/debug/sessions/:id/verdict",
            get(|State(fake): State<Arc<Fake>>| async move {
                if let Some(probe) = fake.pause_probe.lock().as_ref() {
                    if probe.load(Ordering::SeqCst) {
                        fake.probe_saw_paused.store(true, Ordering::SeqCst);
                    }
                }
                let seen = fake.polls.fetch_add(1, Ordering::SeqCst);
                let verdict = if seen >= fake.verdict_after {
                    fake.verdict
                } else {
                    None
                };
                Json(json!({ "verdict": verdict, "version": seen + 1 }))
            }),
        )
        .route(
            "/api/v1/debug/sessions/:id/close",
            post(
                |State(fake): State<Arc<Fake>>, Json(body): Json<Value>| async move {
                    fake.closes.fetch_add(1, Ordering::SeqCst);
                    *fake.last_close_state.lock() = body
                        .get("state")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned);
                    Json(json!({ "ok": true }))
                },
            ),
        )
        .with_state(fake);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

fn client(base_url: &str) -> DebugPauseClient {
    DebugPauseClient::with_http(
        Arc::new(crate::client::http::HttpClient::with_control(None, None).unwrap()),
        base_url,
        "debug-worker-token".to_owned(),
        RunId::new(),
        JobId("build".to_owned()),
        uuid::Uuid::new_v4(),
        "build".to_owned(),
    )
    .unwrap()
    .with_workspace(Some("/work".to_owned()), Some("deadbeef".to_owned()))
}

/// What the fake control plane recorded about the credential exchange.
#[derive(Default)]
struct Exchange {
    /// Bearer and body of the token exchange.
    request: parking_lot::Mutex<Option<(Option<String>, Value)>>,
    /// Bearers seen on each session route, in call order.
    session_bearers: parking_lot::Mutex<Vec<Option<String>>>,
    /// Status the exchange answers with.
    status: axum::http::StatusCode,
}

fn bearer(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::to_owned)
}

async fn spawn_exchange(state: Arc<Exchange>) -> String {
    let record = |state: &Arc<Exchange>, headers: &axum::http::HeaderMap| {
        state.session_bearers.lock().push(bearer(headers));
    };
    let app = Router::new()
        .route(
            "/api/v1/debug/worker-token",
            post(
                |State(state): State<Arc<Exchange>>,
                 headers: axum::http::HeaderMap,
                 Json(body): Json<Value>| async move {
                    *state.request.lock() = Some((bearer(&headers), body));
                    if state.status.is_success() {
                        (
                            state.status,
                            Json(json!({ "token": "minted-debug-worker-token" })),
                        )
                    } else {
                        (state.status, Json(json!({ "error": "denied" })))
                    }
                },
            ),
        )
        .route(
            "/api/v1/debug/sessions",
            post(
                move |State(state): State<Arc<Exchange>>,
                      headers: axum::http::HeaderMap,
                      Json(_): Json<Value>| async move {
                    record(&state, &headers);
                    Json(json!({ "session_id": "dbg_test" }))
                },
            ),
        )
        .route(
            "/api/v1/debug/sessions/:id/verdict",
            get(
                move |State(state): State<Arc<Exchange>>, headers: axum::http::HeaderMap| async move {
                    record(&state, &headers);
                    Json(json!({ "verdict": "continue", "version": 1 }))
                },
            ),
        )
        .route(
            "/api/v1/debug/sessions/:id/close",
            post(
                move |State(state): State<Arc<Exchange>>,
                      headers: axum::http::HeaderMap,
                      Json(_): Json<Value>| async move {
                    record(&state, &headers);
                    Json(json!({ "ok": true }))
                },
            ),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

/// The worker acquires its debug credential over an authenticated exchange and
/// then speaks with *that* credential, never the runtime token.
///
/// The credential cannot travel on the job message: the official runner copies
/// every secret variable into the `secrets` context, so a workflow being
/// debugged could read it out of its own YAML. The runtime token is the only
/// job-scoped credential the worker already holds, so it buys the exchange —
/// and must buy nothing else, which is why the session calls are asserted to
/// carry the issued token instead.
#[tokio::test]
async fn the_worker_trades_its_runtime_token_for_the_debug_credential() {
    let state = Arc::new(Exchange {
        status: axum::http::StatusCode::OK,
        ..Default::default()
    });
    let base = spawn_exchange(state.clone()).await;
    let agent_job_id = uuid::Uuid::new_v4();

    let client = DebugPauseClient::acquire_with_http(
        Arc::new(crate::client::http::HttpClient::with_control(None, None).unwrap()),
        &format!("{base}/broker/4"),
        "job-runtime-token",
        RunId::new(),
        JobId("build".to_owned()),
        agent_job_id,
        "build".to_owned(),
    )
    .await
    .expect("the exchange must yield a usable client");

    let (exchange_bearer, exchange_body) = state.request.lock().clone().expect("exchange happened");
    assert_eq!(
        exchange_bearer.as_deref(),
        Some("job-runtime-token"),
        "the exchange is what the runtime token is for"
    );
    assert_eq!(
        exchange_body["agent_job_id"],
        json!(agent_job_id),
        "the server rejects a mismatch, so the job must name itself"
    );

    let decision = client
        .pause(failed_step(), Vec::new(), Vec::new(), Vec::new())
        .await;
    assert_eq!(decision.map(|d| d.verdict), Some(Verdict::Continue));

    let seen = state.session_bearers.lock().clone();
    assert_eq!(seen.len(), 3, "open, verdict poll, and close");
    for bearer in seen {
        assert_eq!(
            bearer.as_deref(),
            Some("minted-debug-worker-token"),
            "session routes must be driven by the issued credential"
        );
    }
}

/// A refused exchange leaves pause-on-failure unavailable, not half-armed.
///
/// The server declines when the run never asked for pause-on-failure, or when
/// the one-shot exchange is already spent. Either way the worker must surface
/// the fault and fail the step normally rather than build a client whose every
/// call will 401.
#[tokio::test]
async fn a_refused_exchange_yields_no_client() {
    let state = Arc::new(Exchange {
        status: axum::http::StatusCode::FORBIDDEN,
        ..Default::default()
    });
    let base = spawn_exchange(state.clone()).await;

    let result = DebugPauseClient::acquire_with_http(
        Arc::new(crate::client::http::HttpClient::with_control(None, None).unwrap()),
        &base,
        "job-runtime-token",
        RunId::new(),
        JobId("build".to_owned()),
        uuid::Uuid::new_v4(),
        "build".to_owned(),
    )
    .await;

    assert!(
        result.is_err(),
        "a denied exchange must not produce a client that cannot talk"
    );
    assert!(
        state.session_bearers.lock().is_empty(),
        "no session traffic may be attempted without a credential"
    );
}

fn failed_step() -> FailedStep {
    FailedStep {
        index: 1,
        total: 3,
        context_name: "__run_2".to_owned(),
        display_name: "Run cargo test".to_owned(),
        command: Some("cargo test --workspace".to_owned()),
        working_directory: Some("/work".to_owned()),
        exit_code: Some(101),
        elapsed_ms: 18_400,
        diagnostics: Vec::new(),
        log_excerpt: None,
    }
}

/// The runner's own job-timeout timer must stop counting while a step is
/// paused.
///
/// The server suspends its copy of the clock independently. If the runner's
/// kept running it would cancel a job the server is deliberately holding open,
/// and the user would see a bare timeout in the middle of a debug session.
///
/// Sampled from inside the fake's verdict handler rather than from a racing
/// watcher task: that is the one instant the worker is provably blocked.
#[tokio::test]
async fn the_job_clock_is_suspended_for_exactly_the_wait() {
    let paused = Arc::new(AtomicBool::new(false));
    let fake = Arc::new(Fake {
        verdict: Some(Verdict::Continue),
        pause_probe: parking_lot::Mutex::new(Some(paused.clone())),
        ..Default::default()
    });
    let base = spawn_fake(fake.clone()).await;

    assert!(
        !paused.load(Ordering::SeqCst),
        "the clock runs normally until a step fails"
    );

    let decision = client(&base)
        .with_pause_flag(paused.clone())
        .pause(failed_step(), Vec::new(), Vec::new(), Vec::new())
        .await;

    assert_eq!(decision.map(|d| d.verdict), Some(Verdict::Continue));
    assert!(
        fake.probe_saw_paused.load(Ordering::SeqCst),
        "the timer must see the pause while the worker is waiting on a verdict"
    );
    assert!(
        !paused.load(Ordering::SeqCst),
        "the clock must resume once the verdict lands"
    );
}

#[tokio::test]
async fn pause_reports_the_failure_and_returns_the_verdict() {
    let fake = Arc::new(Fake {
        verdict: Some(Verdict::Retry),
        ..Default::default()
    });
    let base = spawn_fake(fake.clone()).await;

    let verdict = client(&base)
        .pause(failed_step(), Vec::new(), Vec::new(), Vec::new())
        .await;

    assert_eq!(verdict.map(|d| d.verdict), Some(Verdict::Retry));
    assert_eq!(fake.opens.load(Ordering::SeqCst), 1);
    assert_eq!(fake.closes.load(Ordering::SeqCst), 1);

    // The controller must receive enough to orient without another round trip.
    let opened = fake.last_open.lock().clone().unwrap();
    assert_eq!(opened["job_name"], "build");
    assert_eq!(opened["step"]["display_name"], "Run cargo test");
    assert_eq!(opened["step"]["exit_code"], 101);
    assert_eq!(opened["workspace"], "/work");
    assert_eq!(
        opened["snapshot_commit"], "deadbeef",
        "the pristine ref must travel with the session or change detection has nothing to diff against"
    );
}

#[tokio::test]
async fn empty_polls_are_not_decisions() {
    // Three empty polls, then retry. The client must keep waiting rather than
    // treating a quiet channel as an answer.
    let fake = Arc::new(Fake {
        verdict: Some(Verdict::Retry),
        verdict_after: 3,
        ..Default::default()
    });
    let base = spawn_fake(fake.clone()).await;

    let verdict = client(&base)
        .pause(failed_step(), Vec::new(), Vec::new(), Vec::new())
        .await;

    assert_eq!(verdict.map(|d| d.verdict), Some(Verdict::Retry));
    assert_eq!(
        fake.polls.load(Ordering::SeqCst),
        4,
        "must poll until an actual verdict arrives"
    );
}

#[tokio::test]
async fn abort_is_reported_as_aborted_on_close() {
    let fake = Arc::new(Fake {
        verdict: Some(Verdict::Abort),
        ..Default::default()
    });
    let base = spawn_fake(fake.clone()).await;

    let verdict = client(&base)
        .pause(failed_step(), Vec::new(), Vec::new(), Vec::new())
        .await;

    assert_eq!(verdict.map(|d| d.verdict), Some(Verdict::Abort));
    assert_eq!(fake.last_close_state.lock().as_deref(), Some("aborted"));
}

#[tokio::test]
async fn a_vanished_session_resumes_instead_of_hanging() {
    // Server returns 404: the session was swept. That is not a verdict, but it
    // must not block the worker forever either.
    let app = Router::new().route(
        "/api/v1/debug/sessions",
        post(|| async { Json(json!({ "session_id": "dbg_gone" })) }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let verdict = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        client(&format!("http://{addr}")).pause(failed_step(), Vec::new(), Vec::new(), Vec::new()),
    )
    .await
    .expect("a 404 on the verdict poll must not hang the worker");

    assert!(verdict.is_none(), "a missing session is not a decision");
}

#[tokio::test]
async fn an_unreachable_control_plane_fails_the_step_normally() {
    // Nothing listening. Debugging is unavailable; the step must fail as it
    // would have without the feature rather than blocking the job.
    let verdict = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        client("http://127.0.0.1:1").pause(failed_step(), Vec::new(), Vec::new(), Vec::new()),
    )
    .await
    .expect("an unreachable control plane must not hang the worker");

    assert!(verdict.is_none());
}

#[tokio::test]
async fn retry_advances_the_source_revision_label() {
    let fake = Arc::new(Fake {
        verdict: Some(Verdict::Retry),
        ..Default::default()
    });
    let base = spawn_fake(fake).await;
    let client = client(&base);

    assert_eq!(client.current_revision(), "original");
    client
        .pause(failed_step(), Vec::new(), Vec::new(), Vec::new())
        .await;
    assert_eq!(
        client.current_revision(),
        "repair-1",
        "each retry must be attributable to the tree it ran against"
    );
}
