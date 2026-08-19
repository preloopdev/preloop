//! Router-level tests for the GitHub-compatible dispatch API (M2/M3).
//!
//! Exercises the real axum router (`app_with_test_api`) against a local git
//! workspace, the same way `concurrency_http_properties.rs` drives the real
//! router. Auth coverage follows the D2 chain: system bearer, PAT, own-App
//! JWT, own-minted installation token (offline ledger), third-party token
//! (stubbed github.com round-trip), and fail-closed on network errors.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use base64::Engine;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use super::*;

const TEST_API_TOKEN: &str = "dispatch-test-token";

/// Workflow with declared workflow_dispatch inputs (string required, number
/// with default, choice with options).
const DISPATCH_WORKFLOW: &str = r#"
name: dispatchable
on:
  workflow_dispatch:
    inputs:
      greeting:
        type: string
        required: true
      count:
        type: number
        default: 3
      flavor:
        type: choice
        options: [vanilla, chocolate]
        default: vanilla
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo "${{ github.event.inputs.greeting }}"
"#;

// ─── Fixtures ───────────────────────────────────────────────────────────────

/// App with a local git workspace holding `workflows` under
/// `.github/workflows/`. The workspace is a real git repo with `main`, so the
/// dispatch ref/SHA resolution path works.
async fn dispatch_fixture(workflows: &[(&str, &str)]) -> (AppState, Router, tempfile::TempDir) {
    let temp = tempfile::tempdir().unwrap();
    let ws = temp.path().join("ws");
    std::fs::create_dir_all(ws.join(".github/workflows")).unwrap();
    for (name, content) in workflows {
        std::fs::write(ws.join(".github/workflows").join(name), content).unwrap();
    }
    init_git_repo(&ws);
    let mut state = AppState::new(temp.path().join("state").to_path_buf())
        .await
        .unwrap();
    state.local_workspace = Some(ws);
    let app = app_with_test_api(state.clone(), CancellationToken::new(), TEST_API_TOKEN);
    (state, app, temp)
}

/// App with one App configured (for own-App-JWT and ledger tests). The App
/// is set *before* the router is built — `AppState.github_app` is a plain
/// field, not an `Arc`, so a later mutation would not reach the router.
async fn dispatch_fixture_with_app(
    workflows: &[(&str, &str)],
) -> (AppState, Router, rsa::RsaPrivateKey, tempfile::TempDir) {
    let temp = tempfile::tempdir().unwrap();
    let ws = temp.path().join("ws");
    std::fs::create_dir_all(ws.join(".github/workflows")).unwrap();
    for (name, content) in workflows {
        std::fs::write(ws.join(".github/workflows").join(name), content).unwrap();
    }
    init_git_repo(&ws);
    let mut state = AppState::new(temp.path().join("state").to_path_buf())
        .await
        .unwrap();
    state.local_workspace = Some(ws);
    let key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    state.github_app = Some(crate::github_app::GitHubAppCredentials::for_tests(
        "424",
        key.clone(),
        crate::github_app::MintFailurePolicy::LocalJwt,
    ));
    let app = app_with_test_api(state.clone(), CancellationToken::new(), TEST_API_TOKEN);
    (state, app, key, temp)
}

fn init_git_repo(ws: &std::path::Path) {
    for args in [
        &["init", "-q", "-b", "main"][..],
        &["config", "user.email", "dispatch@test.local"][..],
        &["config", "user.name", "dispatch tests"][..],
    ] {
        git_ok(ws, args);
    }
    std::fs::write(ws.join("README.md"), "dispatch fixture\n").unwrap();
    git_ok(ws, &["add", "-A"]);
    git_ok(ws, &["commit", "-qm", "initial"]);
}

fn git_ok(ws: &std::path::Path, args: &[&str]) {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(ws)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// POST a raw JSON body to `uri` with `bearer` (defaults to the system
/// token). Returns (status, parsed body).
async fn post_json(
    app: &Router,
    uri: &str,
    body: &str,
    bearer: Option<&str>,
) -> (StatusCode, Value) {
    let token = bearer.unwrap_or(DEFAULT_PRELOOP_SYSTEM_TOKEN);
    let request = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_owned()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

/// GET with `bearer`.
async fn get_json(app: &Router, uri: &str, bearer: Option<&str>) -> (StatusCode, Value) {
    let token = bearer.unwrap_or(DEFAULT_PRELOOP_SYSTEM_TOKEN);
    let request = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

/// The runs currently recorded, newest last.
async fn recorded_runs(state: &AppState) -> Vec<(String, crate::models::RunRecord)> {
    let inner = state.inner.lock().await;
    inner
        .runs
        .iter()
        .map(|(id, run)| (id.to_string(), run.clone()))
        .collect()
}

/// Record a minted token in the App's mint ledger, as `mint_for_repository`
/// would.
fn record_ledger_token(
    state: &AppState,
    token: &str,
    repository: &str,
    permissions: &[(&str, &str)],
) {
    let app = state.github_app.as_ref().expect("an App is configured");
    app.mint_ledger.record(
        token,
        crate::github_app::MintLedgerEntry {
            installation_id: 7,
            repository: repository.to_owned(),
            permissions: permissions
                .iter()
                .map(|(name, level)| ((*name).to_owned(), (*level).to_owned()))
                .collect(),
            expires_at: SystemTime::now() + Duration::from_secs(600),
            app_id: "424".to_owned(),
            account_login: "octocat-org".to_owned(),
        },
    );
}

/// Start a stub github.com API on a loopback port; returns the base URL.
async fn spawn_github_stub(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (base, handle)
}

/// Pin `PRELOOP_GITHUB_API_URL` for a stub test, holding both the process-wide
/// env lock and the restore-on-drop variable guard.
async fn pin_api_base(
    base: &str,
) -> (
    crate::state::TestEnvVar,
    tokio::sync::MutexGuard<'static, ()>,
) {
    let guard = crate::state::GITHUB_ENV_LOCK.lock().await;
    let var = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", base);
    (var, guard)
}

// ─── Endpoints: workflow_dispatch ──────────────────────────────────────────

#[tokio::test]
async fn workflow_dispatch_creates_run_with_github_context_fidelity() {
    let (state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;

    let (status, body) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch.yml/dispatches",
        r#"{"ref": "main", "inputs": {"greeting": "hello"}}"#,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "body={body}");

    let runs = recorded_runs(&state).await;
    assert_eq!(runs.len(), 1);
    let (_, run) = &runs[0];
    assert_eq!(run.submission.event, "workflow_dispatch");
    assert_eq!(run.submission.repository, "octocat/repo");
    assert_eq!(run.submission.git_ref, "refs/heads/main");
    // `github.event` is the synthesized payload: ref/ref_type/repository/
    // sender shape matches a github.com-delivered webhook.
    assert_eq!(run.github["event_name"], "workflow_dispatch");
    assert_eq!(run.github["actor"], "preloop-system");
    assert_eq!(run.github["event"]["ref"], "refs/heads/main");
    assert_eq!(run.github["event"]["ref_type"], "branch");
    assert_eq!(
        run.github["event"]["repository"]["full_name"],
        "octocat/repo"
    );
    assert_eq!(run.github["event"]["sender"]["login"], "preloop-system");
    // github.event.inputs are stringified, exactly like github.com.
    assert_eq!(run.github["event"]["inputs"]["greeting"], "hello");
    assert_eq!(run.github["event"]["inputs"]["count"], "3");
    assert_eq!(run.github["event"]["inputs"]["flavor"], "vanilla");
    // The typed inputs are what job-level `${{ inputs.count }}` sees.
    assert_eq!(run.submission.dispatch_inputs["count"], json!(3));
    assert_eq!(run.head_sha.len(), 40);
}

#[tokio::test]
async fn workflow_dispatch_defaults_ref_to_default_branch_and_accepts_filename_stem() {
    let (state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;

    // No `ref`, and the workflow named by its stem without the extension.
    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch/dispatches",
        r#"{"inputs": {"greeting": "hi"}}"#,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let runs = recorded_runs(&state).await;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].1.submission.git_ref, "refs/heads/main");
}

#[tokio::test]
async fn workflow_dispatch_missing_required_input_is_422() {
    let (state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;

    let (status, body) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch.yml/dispatches",
        r#"{"ref": "main", "inputs": {}}"#,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("greeting"),
        "the 422 must name the offending input: {body}"
    );
    assert!(
        recorded_runs(&state).await.is_empty(),
        "no run may be created before validation passes"
    );
}

#[tokio::test]
async fn workflow_dispatch_type_mismatch_is_422() {
    let (state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;

    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch.yml/dispatches",
        r#"{"ref": "main", "inputs": {"greeting": "hi", "count": "not-a-number"}}"#,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(recorded_runs(&state).await.is_empty());
}

#[tokio::test]
async fn workflow_dispatch_choice_outside_options_is_422() {
    let (state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;

    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch.yml/dispatches",
        r#"{"ref": "main", "inputs": {"greeting": "hi", "flavor": "strawberry"}}"#,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(recorded_runs(&state).await.is_empty());
}

#[tokio::test]
async fn workflow_dispatch_unknown_workflow_is_404() {
    let (state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;

    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/nope.yml/dispatches",
        r#"{"ref": "main", "inputs": {"greeting": "hi"}}"#,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(recorded_runs(&state).await.is_empty());
}

#[tokio::test]
async fn workflow_dispatch_unknown_ref_is_404() {
    let (state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;

    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch.yml/dispatches",
        r#"{"ref": "no-such-branch", "inputs": {"greeting": "hi"}}"#,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(recorded_runs(&state).await.is_empty());
}

#[tokio::test]
async fn workflow_dispatch_without_dispatch_trigger_is_409() {
    let (state, app, _temp) = dispatch_fixture(&[(
        "push-only.yml",
        "on: push\njobs:\n  b:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
    )])
    .await;

    let (status, body) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/push-only.yml/dispatches",
        r#"{"ref": "main"}"#,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "body={body}");
    assert!(recorded_runs(&state).await.is_empty());
}

#[tokio::test]
async fn workflow_dispatch_malformed_json_is_400() {
    let (state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;

    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch.yml/dispatches",
        "not json",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(recorded_runs(&state).await.is_empty());
}

// ─── Endpoints: repository_dispatch (broadcast) ────────────────────────────

const RD_DEPLOY: &str = r#"
name: deploy
on:
  repository_dispatch:
    types: [deploy]
jobs:
  b:
    runs-on: ubuntu-latest
    steps:
      - run: echo deploy
"#;
const RD_OTHER: &str = r#"
name: other
on:
  repository_dispatch:
    types: [other]
jobs:
  b:
    runs-on: ubuntu-latest
    steps:
      - run: echo other
"#;
const RD_ANY: &str = r#"
name: any
on: repository_dispatch
jobs:
  b:
    runs-on: ubuntu-latest
    steps:
      - run: echo any
"#;
// `broken.yml` fails workflow *submission* (not trigger matching):
// `submit_run_inner` validates `on.schedule` crons via
// `validate_schedule_crons` on every submitted workflow, so the invalid cron
// is a hard per-workflow submission failure. The broadcast test relies on
// that: the broken workflow is rejected at submission while its
// `repository_dispatch` types would otherwise match.
const RD_BROKEN: &str = r#"
name: broken
on:
  repository_dispatch:
    types: [deploy]
  schedule:
    - cron: "not a cron"
jobs:
  b:
    runs-on: ubuntu-latest
    steps:
      - run: echo broken
"#;

#[tokio::test]
async fn repository_dispatch_broadcasts_to_matching_and_untyped_workflows() {
    let (state, app, _temp) = dispatch_fixture(&[
        ("deploy.yml", RD_DEPLOY),
        ("other.yml", RD_OTHER),
        ("any.yml", RD_ANY),
    ])
    .await;

    let (status, body) = post_json(
        &app,
        "/repos/octocat/repo/dispatches",
        r#"{"event_type": "deploy", "client_payload": {"sha": "abc123"}}"#,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "body={body}");

    let runs = recorded_runs(&state).await;
    let mut files: Vec<&str> = runs
        .iter()
        .map(|(_, run)| run.submission.workflow_file.as_deref().unwrap_or_default())
        .collect();
    files.sort();
    assert_eq!(
        files,
        vec!["any.yml", "deploy.yml"],
        "matching and untyped workflows run; non-matching `types` does not"
    );
    for (_, run) in &runs {
        assert_eq!(run.submission.event, "repository_dispatch");
        assert_eq!(run.github["event"]["action"], "deploy");
        assert_eq!(run.github["event"]["client_payload"]["sha"], "abc123");
        assert_eq!(run.github["actor"], "preloop-system");
    }
}

#[tokio::test]
async fn repository_dispatch_with_no_matching_workflow_is_still_204() {
    let (state, app, _temp) = dispatch_fixture(&[("other.yml", RD_OTHER)]).await;

    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/dispatches",
        r#"{"event_type": "deploy"}"#,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(recorded_runs(&state).await.is_empty());
}

#[tokio::test]
async fn repository_dispatch_keeps_broadcasting_after_workflow_validation_failure() {
    let (state, app, _temp) =
        dispatch_fixture(&[("deploy.yml", RD_DEPLOY), ("broken.yml", RD_BROKEN)]).await;

    let (status, body) = post_json(
        &app,
        "/repos/octocat/repo/dispatches",
        r#"{"event_type": "deploy"}"#,
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "a per-workflow validation failure must not stop unrelated broadcasts: {body}"
    );

    let runs = recorded_runs(&state).await;
    assert_eq!(
        runs.len(),
        1,
        "the valid matching workflow runs; the invalid workflow is rejected"
    );
    assert_eq!(
        runs[0].1.submission.workflow_file.as_deref(),
        Some("deploy.yml")
    );
}

#[tokio::test]
async fn repository_dispatch_validates_event_type_and_client_payload() {
    let (state, app, _temp) = dispatch_fixture(&[("any.yml", RD_ANY)]).await;

    // Missing event_type → 422.
    let (status, _) = post_json(&app, "/repos/octocat/repo/dispatches", r#"{}"#, None).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    // Empty event_type → 422.
    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/dispatches",
        r#"{"event_type": "  "}"#,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    // Over 100 chars → 422.
    let long: String = "x".repeat(101);
    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/dispatches",
        &json!({ "event_type": long }).to_string(),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    // Non-object client_payload → 422.
    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/dispatches",
        r#"{"event_type": "deploy", "client_payload": [1, 2]}"#,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    assert!(recorded_runs(&state).await.is_empty());
}

// ─── Read endpoints ────────────────────────────────────────────────────────

#[tokio::test]
async fn list_workflows_returns_github_shape() {
    let (_state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;

    let (status, body) = get_json(&app, "/repos/octocat/repo/actions/workflows", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_count"], 1);
    let workflow = &body["workflows"][0];
    assert!(workflow["id"].is_u64(), "synthetic numeric id: {workflow}");
    assert_eq!(workflow["path"], ".github/workflows/dispatch.yml");
    assert_eq!(workflow["name"], "dispatchable");
    assert_eq!(workflow["state"], "active");
}

#[tokio::test]
async fn list_actions_runs_reports_recent_runs_for_the_repo() {
    let (_state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;
    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch.yml/dispatches",
        r#"{"inputs": {"greeting": "hi"}}"#,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, body) = get_json(&app, "/repos/octocat/repo/actions/runs", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_count"], 1);
    let run = &body["workflow_runs"][0];
    assert_eq!(run["event"], "workflow_dispatch");
    assert_eq!(run["status"], "queued");
    assert!(run["id"].is_u64());
    assert_eq!(run["workflow_path"], ".github/workflows/dispatch.yml");
}

// ─── Auth: the D2 chain ────────────────────────────────────────────────────

#[tokio::test]
async fn dispatch_without_token_is_401() {
    let (_state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;
    let request = Request::builder()
        .method(Method::POST)
        .uri("/repos/octocat/repo/dispatches")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"event_type": "deploy"}"#))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn dispatch_with_garbage_token_is_401() {
    use axum::routing::get;

    let (state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;
    // github.com's honest answer to a nonsense token is 401, which preloop
    // must relay rather than read as a transport failure.
    let stub = Router::new().route(
        "/installation",
        get(|| async { (StatusCode::UNAUTHORIZED, "bad credentials") }),
    );
    let (base, handle) = spawn_github_stub(stub).await;
    let _env = pin_api_base(&base).await;

    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/dispatches",
        r#"{"event_type": "deploy"}"#,
        Some("definitely-not-a-real-token"),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(recorded_runs(&state).await.is_empty());
    handle.abort();
}

#[tokio::test]
async fn dispatch_with_pat_authenticates() {
    // Pin the API base to an unreachable loopback port: the PAT actor
    // fallback depends on github.com being unreachable, and pinning makes
    // that deterministic regardless of any ambient PRELOOP_GITHUB_API_URL.
    let _env = pin_api_base("http://127.0.0.1:1").await;
    let (state, app, _temp) = {
        let temp = tempfile::tempdir().unwrap();
        let ws = temp.path().join("ws");
        std::fs::create_dir_all(ws.join(".github/workflows")).unwrap();
        std::fs::write(ws.join(".github/workflows/dispatch.yml"), DISPATCH_WORKFLOW).unwrap();
        init_git_repo(&ws);
        let mut state = AppState::new(temp.path().join("state").to_path_buf())
            .await
            .unwrap();
        state.local_workspace = Some(ws);
        state.github_pat = Some(preloop_gha_protocol::SecretString::new("ghp_dispatch_test"));
        let app = app_with_test_api(state.clone(), CancellationToken::new(), TEST_API_TOKEN);
        (state, app, temp)
    };

    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch.yml/dispatches",
        r#"{"inputs": {"greeting": "via pat"}}"#,
        Some("ghp_dispatch_test"),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let runs = recorded_runs(&state).await;
    assert_eq!(runs.len(), 1);
    // github.com is unreachable in this test, so the PAT's actor falls back
    // to `preloop-pat` (not the system-bearer identity) instead of failing.
    assert_eq!(runs[0].1.github["actor"], "preloop-pat");
}

#[tokio::test]
async fn dispatch_with_pat_actor_resolved_from_github_user_endpoint() {
    use axum::routing::get;

    let (state, app, _temp) = {
        let temp = tempfile::tempdir().unwrap();
        let ws = temp.path().join("ws");
        std::fs::create_dir_all(ws.join(".github/workflows")).unwrap();
        std::fs::write(ws.join(".github/workflows/dispatch.yml"), DISPATCH_WORKFLOW).unwrap();
        init_git_repo(&ws);
        let mut state = AppState::new(temp.path().join("state").to_path_buf())
            .await
            .unwrap();
        state.local_workspace = Some(ws);
        state.github_pat = Some(preloop_gha_protocol::SecretString::new("ghp_dispatch_test"));
        let app = app_with_test_api(state.clone(), CancellationToken::new(), TEST_API_TOKEN);
        (state, app, temp)
    };

    let stub = Router::new().route(
        "/user",
        get(|| async { Json(json!({ "login": "octocat-pat" })) }),
    );
    let (base, handle) = spawn_github_stub(stub).await;
    let _env = pin_api_base(&base).await;

    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch.yml/dispatches",
        r#"{"inputs": {"greeting": "via pat"}}"#,
        Some("ghp_dispatch_test"),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let runs = recorded_runs(&state).await;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].1.github["actor"], "octocat-pat");
    handle.abort();
}

#[tokio::test]
async fn dispatch_with_own_app_jwt_authenticates_offline() {
    let (state, app, key, _temp) =
        dispatch_fixture_with_app(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;

    let jwt = crate::github_app::sign_app_jwt("424", &key).unwrap();
    // Pre-populate the actor cache so the offline test needs no network.
    state
        .dispatch_actor_cache
        .put("app:424".to_owned(), "preloop-local-app[bot]".to_owned());

    let (status, body) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch.yml/dispatches",
        r#"{"inputs": {"greeting": "via app jwt"}}"#,
        Some(&jwt),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "body={body}");
    let runs = recorded_runs(&state).await;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].1.github["actor"], "preloop-local-app[bot]");
}

#[tokio::test]
async fn dispatch_with_foreign_app_jwt_is_401() {
    let (state, app, _key, _temp) =
        dispatch_fixture_with_app(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;

    // A JWT from an unrelated App's keypair.
    let other_key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let jwt = crate::github_app::sign_app_jwt("999", &other_key).unwrap();

    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/dispatches",
        r#"{"event_type": "deploy"}"#,
        Some(&jwt),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(recorded_runs(&state).await.is_empty());
}

#[tokio::test]
async fn dispatch_with_expired_app_jwt_is_401() {
    let (state, app, key, _temp) =
        dispatch_fixture_with_app(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let expired = sign_test_jwt("424", &key, now - 300, now - 60);

    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/dispatches",
        r#"{"event_type": "deploy"}"#,
        Some(&expired),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(recorded_runs(&state).await.is_empty());
}

#[tokio::test]
async fn dispatch_with_own_minted_token_authenticates_offline_via_ledger() {
    // The offline ledger path resolves the bot actor from the App (`GET /app`
    // when reachable); pin the API base to a closed port so the fallback
    // `{app_id}[bot]` is deterministic instead of depending on the ambient
    // network.
    let (_env, _env_lock) = pin_api_base("http://127.0.0.1:1").await;
    let (state, app, _key, _temp) =
        dispatch_fixture_with_app(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;
    record_ledger_token(
        &state,
        "ghs_own_minted_token",
        "octocat/repo",
        &[("actions", "write"), ("contents", "read")],
    );

    let (status, body) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch.yml/dispatches",
        r#"{"inputs": {"greeting": "via ledger"}}"#,
        Some("ghs_own_minted_token"),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "body={body}");
    let runs = recorded_runs(&state).await;
    assert_eq!(runs.len(), 1);
    // The offline ledger path derives the bot actor from the App id when
    // github.com cannot resolve the slug.
    assert_eq!(runs[0].1.github["actor"], "424[bot]");
    // Installation-token dispatches carry the AppDispatch tier (secrets
    // allowed), distinct from AdminManual.
    assert_eq!(
        runs[0].1.submission.trust_tier.as_deref(),
        Some("app-dispatch")
    );
}

#[tokio::test]
async fn ledger_token_without_actions_write_is_403() {
    let (state, app, _key, _temp) =
        dispatch_fixture_with_app(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;
    record_ledger_token(
        &state,
        "ghs_read_only_token",
        "octocat/repo",
        &[("contents", "read")],
    );

    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch.yml/dispatches",
        r#"{"inputs": {"greeting": "hi"}}"#,
        Some("ghs_read_only_token"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(recorded_runs(&state).await.is_empty());
}

#[tokio::test]
async fn ledger_token_scoped_to_another_repo_is_403() {
    let (state, app, _key, _temp) =
        dispatch_fixture_with_app(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;
    record_ledger_token(
        &state,
        "ghs_other_repo_token",
        "other/repo",
        &[("actions", "write")],
    );

    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch.yml/dispatches",
        r#"{"inputs": {"greeting": "hi"}}"#,
        Some("ghs_other_repo_token"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(recorded_runs(&state).await.is_empty());
}

#[tokio::test]
async fn unknown_token_fails_closed_when_github_is_unreachable() {
    let (state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;

    // Point the API base at a closed port: connection refused is a transport
    // failure, which must NOT be read as "anonymous" — the dispatch fails.
    let _env = pin_api_base("http://127.0.0.1:1").await;
    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/dispatches",
        r#"{"event_type": "deploy"}"#,
        Some("ghs_third_party_token"),
    )
    .await;
    assert!(
        status == StatusCode::BAD_GATEWAY || status == StatusCode::UNAUTHORIZED,
        "fail closed: {status}"
    );
    assert!(recorded_runs(&state).await.is_empty());
}

// ─── M3: third-party installation tokens (stubbed github.com) ──────────────

// ─── M4: multi-App registry ────────────────────────────────────────────────

/// Build a two-App registry: the default App (secret `legacy-secret`) plus a
/// second App with its own secret and key.
fn two_app_registry() -> (crate::github_app::GitHubApps, rsa::RsaPrivateKey) {
    let default_key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let second_key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let mut second = crate::github_app::GitHubAppCredentials::for_tests(
        "525",
        second_key.clone(),
        crate::github_app::MintFailurePolicy::LocalJwt,
    );
    second.webhook_secret = Some("second-secret".to_owned());
    let registry = crate::github_app::GitHubApps {
        apps: vec![
            crate::github_app::GitHubAppCredentials::for_tests(
                "424",
                default_key,
                crate::github_app::MintFailurePolicy::LocalJwt,
            ),
            second,
        ],
        default_index: 0,
    };
    (registry, second_key)
}

#[tokio::test]
async fn webhook_receiver_accepts_any_registered_app_secret() {
    let temp = tempfile::tempdir().unwrap();
    let ws = temp.path().join("ws");
    std::fs::create_dir_all(ws.join(".github/workflows")).unwrap();
    std::fs::write(
        ws.join(".github/workflows/build.yml"),
        "on: push\njobs:\n  b:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
    )
    .unwrap();
    init_git_repo(&ws);
    let mut state = AppState::new(temp.path().join("state").to_path_buf())
        .await
        .unwrap();
    state.local_workspace = Some(ws);
    state.webhook_secret = Some("legacy-secret".to_owned());
    let (registry, _) = two_app_registry();
    state.github_apps = Some(registry);
    let app = app_with_test_api(state.clone(), CancellationToken::new(), TEST_API_TOKEN);

    let payload = json!({
        "ref": "refs/heads/main",
        "after": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        "repository": {"full_name": "octocat/repo", "default_branch": "main"},
        "commits": [],
    });
    // Signed by the second registered App's secret → accepted.
    assert_eq!(
        deliver_webhook(&app, &payload, "second-secret").await,
        StatusCode::OK
    );
    // Signed by the legacy App's secret → accepted.
    assert_eq!(
        deliver_webhook(&app, &payload, "legacy-secret").await,
        StatusCode::OK
    );
    // Signed by nothing registered → 401.
    assert_eq!(
        deliver_webhook(&app, &payload, "wrong-secret").await,
        StatusCode::UNAUTHORIZED
    );
}

/// Deliver a signed push webhook and return the status.
async fn deliver_webhook(app: &Router, payload: &Value, secret: &str) -> StatusCode {
    use hmac::Mac;
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(payload.to_string().as_bytes());
    let sig = mac.finalize().into_bytes();
    let signature = format!(
        "sha256={}",
        sig.iter().map(|b| format!("{b:02x}")).collect::<String>()
    );
    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/github/webhooks")
        .header("x-github-event", "push")
        .header("x-hub-signature-256", signature)
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap();
    app.clone().oneshot(request).await.unwrap().status()
}

#[tokio::test]
async fn select_app_for_repo_picks_the_app_installed_on_the_owner() {
    use axum::routing::get;

    // Installation discovery, routed by the App JWT's `iss`.
    let stub = Router::new().route(
        "/app/installations",
        get(|headers: axum::http::HeaderMap| async move {
            let auth = headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default();
            let iss = auth
                .split('.')
                .nth(1)
                .and_then(|claims| {
                    base64::engine::general_purpose::URL_SAFE_NO_PAD
                        .decode(claims)
                        .ok()
                })
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .and_then(|claims| claims["iss"].as_str().map(str::to_owned))
                .unwrap_or_default();
            let account = if iss == "525" { "org-b" } else { "org-a" };
            Json(json!([{
                "id": 1,
                "account": { "login": account },
            }]))
        }),
    );
    let (base, handle) = spawn_github_stub(stub).await;
    let _env = pin_api_base(&base).await;

    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().join("state").to_path_buf())
        .await
        .unwrap();
    let (registry, _second_key) = two_app_registry();
    state.github_apps = Some(registry);
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });

    let for_org_b = crate::github_app::select_app_for_repo(&shared, "org-b/repo")
        .await
        .expect("an App is installed on org-b");
    assert_eq!(for_org_b.app_id, "525");

    let for_org_a = crate::github_app::select_app_for_repo(&shared, "org-a/repo")
        .await
        .expect("an App is installed on org-a");
    assert_eq!(for_org_a.app_id, "424");

    // In multi-App setups, a repo neither App covers returns None (no guessing).
    let unmapped = crate::github_app::select_app_for_repo(&shared, "nowhere/repo").await;
    assert!(
        unmapped.is_none(),
        "a repo neither App covers must return None, not guess"
    );
    handle.abort();
}

fn installation_stub(
    calls: Arc<parking_lot::Mutex<usize>>,
    permissions: serde_json::Map<String, Value>,
    repository_selection: &'static str,
    accessible: &'static [&'static str],
) -> Router {
    use axum::routing::get;
    let calls_for_installation = Arc::clone(&calls);
    let repositories: Vec<Value> = accessible
        .iter()
        .map(|full_name| json!({ "full_name": full_name }))
        .collect();
    Router::new()
        .route(
            "/installation",
            get(move || {
                let calls = Arc::clone(&calls_for_installation);
                async move {
                    *calls.lock() += 1;
                    Json(json!({
                        "id": 42,
                        "account": { "login": "third-party-org" },
                        "app_id": 12345,
                        "app_slug": "third-party-app",
                        "repository_selection": repository_selection,
                        "permissions": permissions,
                    }))
                }
            }),
        )
        .route(
            "/installation/repositories",
            get(move || async move {
                Json(json!({
                    "total_count": repositories.len(),
                    "repositories": repositories,
                }))
            }),
        )
        .route(
            "/repos/octocat/repo",
            get(|| async { Json(json!({ "default_branch": "main" })) }),
        )
        .route(
            "/repos/octocat/repo/commits/main",
            get(|| async { Json(json!({ "sha": "0123456789012345678901234567890123456789" })) }),
        )
        .route(
            "/user",
            get(|| async { Json(json!({ "login": "someone" })) }),
        )
        .route(
            "/app",
            get(|| async { Json(json!({ "slug": "preloop-local-app" })) }),
        )
}

#[tokio::test]
async fn third_party_installation_token_dispatches_when_it_holds_actions_write() {
    let (state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;
    let calls = Arc::new(parking_lot::Mutex::new(0));
    let mut permissions = serde_json::Map::new();
    permissions.insert("actions".to_owned(), json!("write"));
    permissions.insert("contents".to_owned(), json!("read"));
    let stub = installation_stub(
        Arc::clone(&calls),
        permissions,
        "selected",
        &["octocat/repo"],
    );
    let (base, handle) = spawn_github_stub(stub).await;
    let _env = pin_api_base(&base).await;

    let (status, body) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch.yml/dispatches",
        r#"{"inputs": {"greeting": "third party"}}"#,
        Some("ghs_third_party_token"),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "body={body}");

    let runs = recorded_runs(&state).await;
    assert_eq!(runs.len(), 1);
    // Actor is the third-party App's bot login ({app_slug}[bot]).
    assert_eq!(runs[0].1.github["actor"], "third-party-app[bot]");
    assert_eq!(
        runs[0].1.submission.trust_tier.as_deref(),
        Some("app-dispatch")
    );

    // The second dispatch with the same token must hit the cache, not
    // github.com again.
    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/dispatches",
        r#"{"event_type": "deploy"}"#,
        Some("ghs_third_party_token"),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(
        *calls.lock(),
        1,
        "the installation round-trip must be cached"
    );
    handle.abort();
}

#[tokio::test]
async fn third_party_token_without_actions_write_is_403() {
    let (state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;
    let calls = Arc::new(parking_lot::Mutex::new(0));
    let mut permissions = serde_json::Map::new();
    permissions.insert("contents".to_owned(), json!("read"));
    let stub = installation_stub(
        Arc::clone(&calls),
        permissions,
        "selected",
        &["octocat/repo"],
    );
    let (base, handle) = spawn_github_stub(stub).await;
    let _env = pin_api_base(&base).await;

    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch.yml/dispatches",
        r#"{"inputs": {"greeting": "hi"}}"#,
        Some("ghs_read_only_third_party"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(recorded_runs(&state).await.is_empty());
    handle.abort();
}

#[tokio::test]
async fn third_party_token_without_repo_access_is_403() {
    let (state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;
    let calls = Arc::new(parking_lot::Mutex::new(0));
    let mut permissions = serde_json::Map::new();
    permissions.insert("actions".to_owned(), json!("write"));
    // The stub's repository list names only "octocat/other", so octocat/repo
    // is out of reach.
    let stub = installation_stub(
        Arc::clone(&calls),
        permissions,
        "selected",
        &["octocat/other"],
    );
    let (base, handle) = spawn_github_stub(stub).await;
    let _env = pin_api_base(&base).await;

    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch.yml/dispatches",
        r#"{"inputs": {"greeting": "hi"}}"#,
        Some("ghs_no_repo_access"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(recorded_runs(&state).await.is_empty());
    handle.abort();
}

#[tokio::test]
async fn github_refusing_the_token_is_401_not_502() {
    use axum::routing::get;

    let (state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;
    let stub = Router::new().route(
        "/installation",
        get(|| async { (StatusCode::UNAUTHORIZED, "bad credentials") }),
    );
    let (base, handle) = spawn_github_stub(stub).await;
    let _env = pin_api_base(&base).await;

    let (status, _) = post_json(
        &app,
        "/repos/octocat/repo/dispatches",
        r#"{"event_type": "deploy"}"#,
        Some("ghs_revoked_token"),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(recorded_runs(&state).await.is_empty());
    handle.abort();
}

#[tokio::test]
async fn all_repositories_installation_needs_no_repository_list() {
    let (state, app, _temp) = dispatch_fixture(&[("dispatch.yml", DISPATCH_WORKFLOW)]).await;
    let calls = Arc::new(parking_lot::Mutex::new(0));
    let mut permissions = serde_json::Map::new();
    permissions.insert("actions".to_owned(), json!("admin"));
    let stub = installation_stub(Arc::clone(&calls), permissions, "all", &["octocat/repo"]);
    let (base, handle) = spawn_github_stub(stub).await;
    let _env = pin_api_base(&base).await;

    let (status, body) = post_json(
        &app,
        "/repos/octocat/repo/actions/workflows/dispatch.yml/dispatches",
        r#"{"inputs": {"greeting": "all repos"}}"#,
        Some("ghs_all_repos_token"),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "body={body}");
    assert_eq!(recorded_runs(&state).await.len(), 1);
    handle.abort();
}

/// Sign an RS256 JWT with explicit iat/exp (for the expired-token test).
fn sign_test_jwt(app_id: &str, key: &rsa::RsaPrivateKey, iat: u64, exp: u64) -> String {
    use rsa::pkcs1v15::SigningKey;
    use rsa::signature::{RandomizedSigner, SignatureEncoding};
    use sha2::Sha256;
    let header = json!({ "alg": "RS256", "typ": "JWT" });
    let claims = json!({ "iss": app_id, "iat": iat, "exp": exp });
    let encode = |value: &Value| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(value).unwrap())
    };
    let signing_input = format!("{}.{}", encode(&header), encode(&claims));
    let signature = SigningKey::<Sha256>::new(key.clone())
        .sign_with_rng(&mut rand::thread_rng(), signing_input.as_bytes());
    format!(
        "{signing_input}.{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signature.to_bytes())
    )
}
