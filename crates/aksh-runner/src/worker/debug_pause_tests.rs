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
        "job-runtime-token".to_owned(),
        RunId::new(),
        JobId("build".to_owned()),
        uuid::Uuid::new_v4(),
        "build".to_owned(),
    )
    .unwrap()
    .with_workspace(Some("/work".to_owned()), Some("deadbeef".to_owned()))
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
