//! Shared prelude and helpers for the split integration-test crates.

//!

//! These tests were one `#[cfg(test)] mod tests` unit; splitting them into

//! separate `tests/*.rs` crates bounds the rustc unit that OOMed the 4 GiB

//! runner guest. `use common::*` restores the `use super::*` namespace.

#![allow(unused_imports, dead_code)]

pub use preloop_runner_server::*;

pub use preloop_runner_server::actions::*;

pub use preloop_runner_server::artifact_twirp::*;

pub use preloop_runner_server::auth::*;

pub use preloop_runner_server::blob_store::*;

pub use preloop_runner_server::bootstrap::*;

pub use preloop_runner_server::broker::*;

pub use preloop_runner_server::cache_artifacts::*;

pub use preloop_runner_server::compat_ghes::*;

pub use preloop_runner_server::concurrency::*;

pub use preloop_runner_server::config::*;

pub use preloop_runner_server::connection::*;

pub use preloop_runner_server::credential_store::*;

pub use preloop_runner_server::debug::*;

pub use preloop_runner_server::debug_sessions::*;

pub use preloop_runner_server::dispatch::*;

pub use preloop_runner_server::dispatch_auth::*;

pub use preloop_runner_server::distributed_task::*;

pub use preloop_runner_server::errors::*;

pub use preloop_runner_server::events::*;

pub use preloop_runner_server::github::*;

pub use preloop_runner_server::github_app::*;

pub use preloop_runner_server::github_breaker::*;

pub use preloop_runner_server::github_pr::*;

pub use preloop_runner_server::github_push::*;

pub use preloop_runner_server::live_logs::*;

pub use preloop_runner_server::memory_caps::*;

pub use preloop_runner_server::models::*;

pub use preloop_runner_server::oauth::*;

pub use preloop_runner_server::oidc::*;

pub use preloop_runner_server::oidc_handlers::*;

pub use preloop_runner_server::openapi::*;

pub use preloop_runner_server::recording::*;

pub use preloop_runner_server::remote_workflows::*;

pub use preloop_runner_server::results_twirp::*;

pub use preloop_runner_server::reusable_workflows::*;

pub use preloop_runner_server::routes::*;

pub use preloop_runner_server::runner_lifecycle::*;

pub use preloop_runner_server::runs::*;

pub use preloop_runner_server::runtime_scheduling::*;

pub use preloop_runner_server::scheduler::*;

pub use preloop_runner_server::scheduling::*;

pub use preloop_runner_server::secrets_api::*;

pub use preloop_runner_server::shared_http::*;

pub use preloop_runner_server::snapshots::*;

pub use preloop_runner_server::state::*;

pub use preloop_runner_server::store::*;

pub use preloop_runner_server::store_pg::*;

pub use preloop_runner_server::timeline_logs::*;

pub use preloop_runner_server::webhook_api::*;

pub use preloop_runner_server::webhook_health::*;

pub use preloop_runner_server::webhook_status::*;

pub use preloop_runner_server::webhook_watchdog::*;

pub use axum::body::{to_bytes, Body};

pub use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};

pub use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};

pub use axum::http::{header, HeaderMap, Method, StatusCode};

pub use axum::middleware::{self, Next};

pub use axum::response::sse::{Event, KeepAlive, Sse};

pub use axum::response::{IntoResponse, Response};

pub use axum::routing::{delete, get, patch, post, put};

pub use axum::{Json, Router};

pub use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};

pub use base64::Engine;

pub use bytes::Bytes;

pub use futures::{stream, StreamExt};

pub use hmac::{Hmac, Mac};

pub use preloop_artifacts::{validate_artifact_name, ArtifactStore};

pub use preloop_cache::CacheStore;

pub use preloop_gha_parser::eval::build_context;

pub use preloop_gha_parser::parse_workflow;

pub use preloop_gha_protocol::{
    azdo,
    azdo::AgentJobRequestMessage,
    crypto::{AgentRsaKeypair, AgentRsaPublicKey, SessionEncryption},
    event_to_ndjson, AnnotationLevel, ExecutionStatus, JobCompletion, JobId,
    LiveLogFeedLinesWrapper, NdjsonEvent, RegisteredRunner, RunAccepted, RunId,
    RunnerRegistrationRequest, RunnerSession, RunnerSessionRequest, SessionId, WorkflowSubmission,
    PROTOCOL_VERSION,
};

pub use std::sync::Arc;

pub use serde::{Deserialize, Serialize};

pub use serde_json::{json, Value};

pub use sha1::{Digest, Sha1};

pub use sha2::Sha256;

pub use std::collections::{BTreeMap, BTreeSet};

pub use std::fs;

pub use std::io::Write;

pub use std::path::Path as FsPath;

pub use std::process::{Command, Stdio};

pub use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use tokio::net::TcpListener;

pub use tokio::sync::{broadcast, Mutex, Notify};

pub use tokio_util::sync::CancellationToken;

pub use tower::ServiceExt;

pub use tracing::{debug, error, info, warn};

pub use axum_server::{tls_rustls::RustlsConfig, Handle};

pub use rcgen::generate_simple_self_signed;

pub const TEST_API_TOKEN: &str = "property-test-token";

pub fn app(state: AppState, shutdown: CancellationToken) -> Router {
    app_with_test_api(state, shutdown, TEST_API_TOKEN)
}

/// `preserve_on_failure` is a property of the run, carried to the runner on the
/// job message. It must be absent unless asked for, so the default wire shape
/// stays byte-identical to what an official runner expects.

pub fn selected_jobs_workflow() -> &'static str {
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

pub async fn open_protocol_live(app: &axum::Router, uri: String, bearer: &str) -> Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {bearer}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

/// M5: the protocol live-log read route must not let one job's runtime
/// credential read another job's output. A job may read its own feed.

/// Build a two-job run and return `(run_id, [(job_id, plan_id, agent_job_id)])`
/// ordered by request id, so filter tests can address either job.
pub async fn two_job_run_for_log_filters(
    app: &axum::Router,
    state: &AppState,
) -> (RunId, Vec<(String, String, String)>) {
    let accepted = submit_yaml(
        app,
        r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo build
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo test
"#,
        "owner/repo",
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let jobs = {
        let inner = state.inner.lock().await;
        let mut requests: Vec<_> = inner
            .job_requests
            .values()
            .filter(|request| request.run_id == run_id)
            .collect();
        requests.sort_by_key(|request| request.request_id);
        requests
            .into_iter()
            .map(|request| {
                (
                    request.job_id.0.clone(),
                    request.plan_id.clone(),
                    request.agent_job_id.to_string(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(jobs.len(), 2, "fixture must produce two jobs");
    (run_id, jobs)
}

/// Build a run whose `build` job declares three steps.
///
/// Step-filter tests need more than one declared step to prove `--step N`
/// selects the right one.

/// Build a run whose `build` job declares three steps.
///
/// Step-filter tests need more than one declared step to prove `--step N`
/// selects the right one.
pub async fn three_step_run_for_log_filters(
    app: &axum::Router,
    state: &AppState,
) -> (RunId, Vec<(String, String, String)>) {
    let accepted = submit_yaml(
        app,
        r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo one
      - run: echo two
      - run: echo three
"#,
        "owner/repo",
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let jobs = {
        let inner = state.inner.lock().await;
        inner
            .job_requests
            .values()
            .filter(|request| request.run_id == run_id)
            .map(|request| {
                (
                    request.job_id.0.clone(),
                    request.plan_id.clone(),
                    request.agent_job_id.to_string(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(jobs.len(), 1, "fixture must produce one job");
    (run_id, jobs)
}

/// The declared workflow step ids of a job's latest attempt, in workflow order.
///
/// Tests must name their `step-<id>.txt` blobs with these ids and report them
/// as `external_id`: that is what the official runner does (verified in
/// `.runner-watch/golden/v2.336.0/06-multi-step`), and the server resolves
/// `?step=` through this manifest rather than through anything on disk.

/// The declared workflow step ids of a job's latest attempt, in workflow order.
///
/// Tests must name their `step-<id>.txt` blobs with these ids and report them
/// as `external_id`: that is what the official runner does (verified in
/// `.runner-watch/golden/v2.336.0/06-multi-step`), and the server resolves
/// `?step=` through this manifest rather than through anything on disk.
pub async fn workflow_step_ids(state: &AppState, run_id: RunId, job: &str) -> Vec<String> {
    let inner = state.inner.lock().await;
    let agent_job_id = inner
        .job_requests
        .values()
        .filter(|request| request.run_id == run_id && request.job_id.0 == job)
        .max_by_key(|request| request.request_id)
        .map(|request| request.agent_job_id)
        .expect("job must have been dispatched");
    inner
        .job_steps
        .get(&agent_job_id)
        .map(|records| {
            crate::models::StepRecord::workflow_steps(records)
                .into_iter()
                .map(|step| step.id.clone())
                .collect()
        })
        .unwrap_or_default()
}

pub async fn get_logs(app: &axum::Router, uri: String) -> (StatusCode, Vec<u8>) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(uri)
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, body.to_vec())
}

/// Write `job-logs.txt` for one job.

/// Write `job-logs.txt` for one job.
pub async fn write_merged_job_log(temp: &tempfile::TempDir, plan: &str, agent: &str, body: &str) {
    let dir = temp
        .path()
        .join("replay")
        .join("results")
        .join(plan)
        .join(agent);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(dir.join("job-logs.txt"), body.as_bytes())
        .await
        .unwrap();
}

/// Write ordered per-step logs for one job. Mtimes are spaced so the handler's
/// (mtime, name) ordering is deterministic.

/// Write ordered per-step logs for one job. Mtimes are spaced so the handler's
/// (mtime, name) ordering is deterministic.
pub async fn write_step_job_logs(
    temp: &tempfile::TempDir,
    plan: &str,
    agent: &str,
    steps: &[(&str, &str)],
) {
    let dir = temp
        .path()
        .join("replay")
        .join("results")
        .join(plan)
        .join(agent);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    for (name, body) in steps {
        tokio::fs::write(dir.join(format!("step-{name}.txt")), body.as_bytes())
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// Drive one job of a run to a terminal status through the same path every
/// completion funnels through (`complete_job_inner`).
pub async fn complete_job(state: &AppState, run_id: RunId, job_id: &str, status: ExecutionStatus) {
    let _ = crate::distributed_task::complete_job_inner(
        state.shared(),
        preloop_gha_protocol::JobCompletion {
            run_id,
            job_id: preloop_gha_protocol::JobId(job_id.to_owned()),
            agent_job_id: None,
            status,
            outputs: Default::default(),
            annotations: Vec::new(),
            step_results: Vec::new(),
        },
    )
    .await
    .expect("completion should succeed");
}

/// Read an SSE response body to its end, failing (rather than hanging the test
/// run) if the stream does not close within a short window. A live-log stream
/// that never ends is exactly the bug these tests guard against.

/// Read an SSE response body to its end, failing (rather than hanging the test
/// run) if the stream does not close within a short window. A live-log stream
/// that never ends is exactly the bug these tests guard against.
pub async fn read_sse_to_end(response: Response) -> String {
    let body = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("live-log stream must close, not hang")
    .expect("body readable");
    String::from_utf8_lossy(&body).into_owned()
}

pub async fn open_live(app: &axum::Router, uri: String) -> Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(uri)
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

/// Mint a blob JWT the way the signed-URL handlers do: `sub: preloop-blob`,
/// `kind`, `job` ("" for system), `jti` = staging dir name. Returns
/// `(jwt, jti)` — the pending-reservation maps key on `jti`.
pub fn mint_blob_jwt(state: &AppState, kind: &str, job: &str) -> (String, String) {
    let jti = uuid::Uuid::new_v4().to_string();
    let jwt = state
        .local_jwt_with_lifetime(
            json!({
                "sub": "preloop-blob",
                "kind": kind,
                "job": job,
                "jti": jti,
            }),
            crate::memory_caps::PENDING_UPLOAD_TTL,
        )
        .unwrap();
    (jwt, jti)
}

/// Point PAT scope introspection (H3) at a dead local address so tests that
/// submit runs with a configured PAT stay hermetic: the probe fails fast with
/// connection-refused instead of reaching api.github.com, where a fake PAT
/// would 401 and fail the run. The scopes are then `Unverifiable`, so the PAT
/// is withheld from jobs while the run itself still proceeds.
pub fn dead_pat_scope_api() -> crate::state::TestEnvVar {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind dead API port");
    let port = listener.local_addr().expect("dead API local addr").port();
    drop(listener);
    crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", format!("http://127.0.0.1:{port}"))
}

/// Point PAT scope introspection (H3) at a local stub that reports `scopes` as
/// the PAT's classic OAuth scopes, so the credential can be verified and is
/// therefore embedded.

/// Point PAT scope introspection (H3) at a local stub that reports `scopes` as
/// the PAT's classic OAuth scopes, so the credential can be verified and is
/// therefore embedded.
pub async fn live_pat_scope_api(scopes: &'static str) -> crate::state::TestEnvVar {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind API stub port");
    let addr = listener.local_addr().expect("API stub local addr");
    let stub = axum::Router::new().route(
        "/",
        axum::routing::get(move || async move {
            (
                [("X-OAuth-Scopes", scopes)],
                axum::Json(serde_json::json!({})),
            )
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, stub).await.unwrap();
    });
    crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", format!("http://{addr}"))
}

/// A PAT-only deployment embeds the static PAT into job messages at build
/// time. That override must never reach a fork-restricted job: the job keeps
/// the local job-scoped runtime token, which authenticates only against this
/// control plane.

// Non-asserting helper for tests that need to inspect an error response.
pub async fn try_req(app: &Router, method: Method, uri: &str, body: Value) -> (StatusCode, Value) {
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

pub async fn request_json(app: &Router, method: Method, uri: &str, body: Value) -> Value {
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

pub async fn request_json_with_bearer(
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

/// Status of a bearer-authenticated request, for asserting rejections.
pub async fn request_status_with_bearer(
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

pub async fn request_status_without_bearer(
    app: &Router,
    method: Method,
    uri: &str,
    body: Value,
) -> StatusCode {
    let mut builder = Request::builder().method(method).uri(uri);
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

/// Open a debug session as a worker would, for exchange tests.
pub fn open_session_body(run_id: RunId, agent_job_id: uuid::Uuid) -> Value {
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

pub async fn verdict_poll_timeout_is_not_an_abort_impl() {
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

/// Scaffolding shared by the webhook delivery dedup tests: a workspace holding
/// one push-triggered workflow, a server with a webhook secret, and the signed
/// push payload GitHub would deliver.
pub struct WebhookDedupFixture {
    pub state: AppState,
    pub app: Router,
    pub payload_bytes: Vec<u8>,
    pub signature_header: String,
}

impl WebhookDedupFixture {
    pub async fn new(temp: &tempfile::TempDir) -> Self {
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/build.yml"),
            "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        )
        .unwrap();

        let event_sha = commit_workflow_fixture(&ws_dir, &[".github/workflows/build.yml"]);

        let mut state = AppState::new(temp.path().join("state").to_path_buf())
            .await
            .unwrap();
        state.webhook_secret = Some("super-secret".to_owned());
        state.local_workspace = Some(ws_dir.clone());
        let app = app(state.clone(), CancellationToken::new());

        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "before": "0000000000000000000000000000000000000000",
            "after": event_sha.clone(),
            "repository": {"full_name": "owner/repo", "default_branch": "main"},
            "commits": [{
                "id": event_sha,
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
    /// `x-github-event` header; `None` omits it, so the handler rejects the
    /// request before a durable delivery row is created.
    pub async fn post(&self, delivery: &str, event: Option<&str>) -> StatusCode {
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
    pub async fn drain(&self) -> usize {
        let shared = Arc::new(SharedState {
            state: self.state.clone(),
            shutdown: CancellationToken::new(),
        });
        crate::github::drain_webhook_queue(&shared).await.unwrap()
    }
}

pub fn r1_9_test_uri() -> axum::http::Uri {
    "/runner/server/_apis/v1/oauth2/token".parse().unwrap()
}

pub fn r1_9_claims(now: i64, aud: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "sub": "test-client",
        "iss": "test-client",
        "aud": aud,
        "nbf": now,
        "exp": now + 300,
    })
}

pub async fn submit_yaml(app: &Router, yaml: &str, repo: &str) -> Value {
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

/// Extract the queued job message for a run, wherever it currently sits.
pub fn queued_message_for(
    inner: &crate::state::InnerState,
    run_id: &str,
) -> AgentJobRequestMessage {
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

pub fn variable_value<'a>(message: &'a AgentJobRequestMessage, name: &str) -> Option<&'a str> {
    message
        .variables
        .get(name)
        .and_then(|value| value.value.as_deref())
}
/// `sub` claim of a runtime JWT, for comparisons that must ignore
/// second-granularity timestamps (`iat`/`exp`). Returns `None` for
/// non-JWT values (e.g. a leaked PAT) so mismatches fail the assertion
/// instead of panicking in the helper.

/// `sub` claim of a runtime JWT, for comparisons that must ignore
/// second-granularity timestamps (`iat`/`exp`). Returns `None` for
/// non-JWT values (e.g. a leaked PAT) so mismatches fail the assertion
/// instead of panicking in the helper.
pub fn jwt_sub(token: &str) -> Option<String> {
    use base64::Engine as _;
    let payload = token.split('.').nth(1)?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let value: serde_json::Value = serde_json::from_slice(&decoded).ok()?;
    value.get("sub")?.as_str().map(str::to_owned)
}

/// `preloop setup github --via pat` stores the credential as `github.pat` and
/// configures no App. That PAT must reach jobs as their `GITHUB_TOKEN`:
/// previously only `PRELOOP_GITHUB_TOKEN` was consulted, so setup reported
/// success while every job silently ran on the local runtime token instead.

/// Send a request carrying a specific bearer token and return just the status
/// code — the shared counterpart the cache-gating tests use, so a
/// request-shape change lands in one place.
pub async fn status_with_bearer(
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

/// Like `request_json` but returns the status instead of asserting success.
pub async fn request_json_status(
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

pub async fn terminal_job_completion_terminalizes_an_active_step() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = submit_yaml(
        &app,
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: exit 1\n",
        "local/preloop",
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    let (plan_id, agent_job_id) = {
        let inner = state.inner.lock().await;
        let request = inner
            .job_requests
            .values()
            .find(|request| request.run_id.0.to_string() == run_id)
            .unwrap();
        (request.plan_id.clone(), request.agent_job_id.to_string())
    };
    request_json(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
        json!({
            "workflow_run_backend_id": plan_id,
            "workflow_job_run_backend_id": agent_job_id,
            "steps": [{
                "external_id": uuid::Uuid::new_v4().to_string(),
                "number": 2,
                "name": "Test",
                "status": 3,
                "conclusion": 0
            }]
        }),
    )
    .await;
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "status": "failure",
            "outputs": {}
        }),
    )
    .await;

    let run = get_run_json(&app, &run_id).await;
    assert_eq!(run["status"], "failure");
    assert_eq!(run["jobs_list"][0]["conclusion"], "failure");
    assert_eq!(run["jobs_list"][0]["steps"][0]["name"], "Test");
    assert_eq!(
        run["jobs_list"][0]["steps"][0]["conclusion"], "failure",
        "a terminal job must not retain an in-progress step"
    );
}

pub async fn get_run_json(app: &Router, run_id: &str) -> Value {
    request_json(
        app,
        Method::GET,
        &format!("/api/v1/runs/{run_id}"),
        Value::Null,
    )
    .await
}

pub async fn complete_via_api(app: &Router, run_id: &str, job_id: &str) {
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

pub async fn complete_via_api_with_outputs(
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

pub async fn poll_and_ack(app: &Router) -> Value {
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

pub fn decode_cancel_body(msg: &Value) -> Value {
    assert_eq!(msg["messageType"], azdo::message_type::JOB_CANCELLED);
    let body_b64 = msg["body"].as_str().unwrap();
    let body_bytes = BASE64_STANDARD.decode(body_b64).unwrap();
    serde_json::from_slice(&body_bytes).unwrap()
}

pub async fn generated_server_dag_properties_1000_cases() {
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

pub fn commit_workflow_fixture(worktree: &FsPath, paths: &[&str]) -> String {
    git_fixture_command(worktree, &["init", "-b", "main"]);
    git_fixture_command(
        worktree,
        &["config", "user.email", "preloop-tests@example.invalid"],
    );
    git_fixture_command(worktree, &["config", "user.name", "Preloop Tests"]);
    let mut add = vec!["add"];
    add.extend_from_slice(paths);
    git_fixture_command(worktree, &add);
    git_fixture_command(worktree, &["commit", "-m", "test workflow"]);
    String::from_utf8(git_fixture_output(worktree, &["rev-parse", "HEAD"]))
        .unwrap()
        .trim()
        .to_owned()
}

pub fn git_fixture_command(worktree: &FsPath, args: &[&str]) {
    // Fixture commits must not depend on the machine's global git identity:
    // clean CI runners have none, so `commit` fails with Author unknown.
    let output = Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(args)
        .env("GIT_AUTHOR_NAME", "preloop")
        .env("GIT_AUTHOR_EMAIL", "preloop@example.com")
        .env("GIT_COMMITTER_NAME", "preloop")
        .env("GIT_COMMITTER_EMAIL", "preloop@example.com")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
}

pub fn git_fixture_output(worktree: &FsPath, args: &[&str]) -> Vec<u8> {
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

pub fn git_fixture_output_allow_failure(worktree: &FsPath, args: &[&str]) -> (bool, Vec<u8>) {
    let output = Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(args)
        .output()
        .unwrap();
    (output.status.success(), output.stdout)
}

pub fn git_pack_bytes(repository: &FsPath) -> u64 {
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

pub fn git_alternate_object_directories(repository: &FsPath) -> Vec<std::path::PathBuf> {
    fs::read_to_string(repository.join("objects/info/alternates"))
        .unwrap()
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| std::fs::canonicalize(line).unwrap())
        .collect()
}

pub fn create_snapshot_fixture(root: &FsPath) -> (std::path::PathBuf, std::path::PathBuf) {
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

pub fn checkout_test_message(steps: Value) -> AgentJobRequestMessage {
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

/// Register a runner through the AzDO compat path and mint its listen token
/// through the mock OAuth flow with the clientId the server assigned.
pub async fn register_runner_with_token(
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

pub async fn create_disttask_session(
    app: &Router,
    bearer: &str,
    agent_id: i64,
) -> (StatusCode, Value) {
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

pub async fn poll_message(app: &Router, bearer: &str, session_id: &str) -> Value {
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

pub async fn submit_simple_run(app: &Router) -> Value {
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

pub async fn pool_managed_state(temp: &tempfile::TempDir) -> AppState {
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.inner.lock().await.pool_assignments_enabled = true;
    state
}

/// Simulate the host-side pool staging a provision token before a machine
/// boots and runs `configure`.

/// Simulate the host-side pool staging a provision token before a machine
/// boots and runs `configure`.
pub fn stage_provision_token(state: &AppState, token: &str) {
    state
        .pending_registrations
        .write()
        .unwrap()
        .insert(token.to_owned(), std::time::SystemTime::now());
}

pub async fn submit_push_run(app: &Router, sha: &str, push_tree: &str) -> Value {
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

// Helper: register a minimal live job record so R1-10 liveness checks pass.
pub async fn r1_10_register_live_job(state: &AppState, job_uuid: uuid::Uuid, plan_id: &str) {
    use preloop_gha_protocol::{JobId, RunId};
    let mut inner = state.inner.lock().await;
    let request_id = inner.job_requests.keys().copied().max().unwrap_or(0) + 1;
    let record = crate::models::TaskAgentJobRequestRecord {
        request_id,
        run_id: RunId(uuid::Uuid::new_v4()),
        job_id: JobId("test-job".to_owned()),
        agent_job_id: job_uuid,
        plan_id: plan_id.to_owned(),
        plan_type: "test".to_owned(),
        timeline_id: uuid::Uuid::new_v4(),
        result: None,
        locked_until: String::new(),
        owner_runner_id: None,
        started_at: None,
        last_renewed_at: None,
        timeout_triggered: false,
        claimed_at: None,
        debug_token_issued: false,
    };
    inner.agent_job_requests.insert(job_uuid, request_id);
    inner.job_requests.insert(request_id, record);
}

// Helper: mark a registered job's request record terminally complete, so the
// R1-10 liveness check treats its token as stale.

// Helper: mark a registered job's request record terminally complete, so the
// R1-10 liveness check treats its token as stale.
pub async fn r1_10_complete_job(state: &AppState, job_uuid: uuid::Uuid) {
    let mut inner = state.inner.lock().await;
    let request_id = inner.agent_job_requests.get(&job_uuid).copied().unwrap();
    if let Some(record) = inner.job_requests.get_mut(&request_id) {
        record.result = Some(preloop_gha_protocol::ExecutionStatus::Success);
    }
}

// Helper: register a live job plus the minimal run record the legacy cache
// path needs (`job_repository_from_headers` resolves job → request → run →
// submission.repository).

// Helper: register a live job plus the minimal run record the legacy cache
// path needs (`job_repository_from_headers` resolves job → request → run →
// submission.repository).
pub async fn r1_10_register_live_job_with_run(
    state: &AppState,
    job_uuid: uuid::Uuid,
    plan_id: &str,
) {
    r1_10_register_live_job(state, job_uuid, plan_id).await;
    let run_id = {
        let inner = state.inner.lock().await;
        let request_id = inner.agent_job_requests.get(&job_uuid).copied().unwrap();
        inner.job_requests.get(&request_id).unwrap().run_id
    };
    let submission = std::sync::Arc::new(preloop_gha_protocol::WorkflowSubmission {
        repository: "test-org/test-repo".to_owned(),
        ..Default::default()
    });
    let mut inner = state.inner.lock().await;
    inner.runs.insert(
        run_id,
        crate::models::RunRecord {
            run_id,
            webhook_delivery_id: None,
            run_name: None,
            submission,
            jobs: Default::default(),
            status: preloop_gha_protocol::ExecutionStatus::InProgress,
            job_outputs: Default::default(),
            job_base_ids: Default::default(),
            job_needs: Default::default(),
            caller_plans: Default::default(),
            job_names: Default::default(),
            github: serde_json::Value::Null,
            head_sha: String::new(),
            workflow_ref: String::new(),
            workspace_snapshot: None,
            job_fail_fast: Default::default(),
            job_continue_on_error: Default::default(),
            job_check_run_ids: Default::default(),
            reusable_calls: Default::default(),
            jobs_list: Vec::new(),
            created_at: chrono::Utc::now(),
            started_at: None,
            completed_at: None,
            run_number: 1,
            run_attempt: 1,
            workflow_path_str: String::new(),
            event: "push".to_owned(),
            conclusion: None,
            push_state: None,
            snapshot_timing: None,
            fork_approval_pending: false,
            fork_approval_requested_at_unix_nanos: None,
            fork_approved_at_unix_nanos: None,
            fork_approval_note: None,
        },
    );
}
