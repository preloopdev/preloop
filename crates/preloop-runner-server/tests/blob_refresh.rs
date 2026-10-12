//! Blob upload-URL refresh regression tests (round-3 follow-up to finding 3).
//!
//! Signed blob upload URLs expire 20 minutes after mint. An upload that
//! outlasts its URL (large blob, slow link) must be able to get a fresh
//! credential for the same staging directory — without weakening the bound:
//! refresh requires a live owning job, so the session dies with the job.

mod common;

use common::*;
use std::time::Duration;

const REFRESH_URI: &str = "/twirp/github.actions.results.api.v1.BlobService/RefreshUploadURL";
const CREATE_URI: &str = "/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact";
const WORKFLOW: &str =
    "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";

/// Submit a one-job run and return (run_id string, plan_id, job_id, job token).
async fn live_job(app: &axum::Router, state: &AppState) -> (String, String, String, String) {
    let accepted = submit_yaml(app, WORKFLOW, "owner/repo").await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    let (plan_id, job_id, token) = {
        let inner = state.test_tx().await;
        let message = queued_message_for(&inner, &run_id);
        (
            message.plan.plan_id.clone(),
            message.job_id.to_string(),
            state.mint_runtime_token(&message.plan.plan_id, &message.job_id),
        )
    };
    (run_id, plan_id, job_id, token)
}

/// Create an artifact upload reservation; returns the signed upload URL.
async fn create_upload(
    app: &axum::Router,
    plan_id: &str,
    job_id: &str,
    token: &str,
    name: &str,
) -> String {
    let create = request_json_with_bearer(
        app,
        Method::POST,
        CREATE_URI,
        json!({
            "workflow_run_backend_id": plan_id,
            "workflow_job_run_backend_id": job_id,
            "name": name,
        }),
        token,
    )
    .await;
    assert_eq!(create["ok"], true, "create must succeed: {create:?}");
    create["signed_upload_url"].as_str().unwrap().to_owned()
}

/// The `jti` claim of a blob JWT (decodes the payload without verifying).
fn jwt_jti(token: &str) -> String {
    let payload = token.split('.').nth(1).unwrap();
    let bytes = {
        let mut s = payload.replace('-', "+").replace('_', "/");
        while s.len() % 4 != 0 {
            s.push('=');
        }
        base64::engine::general_purpose::STANDARD
            .decode(&s)
            .unwrap()
    };
    let claims: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    claims["jti"].as_str().unwrap().to_owned()
}

/// POST the refresh endpoint with a bearer; returns (status, body).
async fn refresh(app: &axum::Router, token: &str, bearer: &str) -> (StatusCode, serde_json::Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(REFRESH_URI)
                .header(header::AUTHORIZATION, format!("Bearer {bearer}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({ "token": token }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, body)
}

/// Bearerless PUT of bytes to a URL; returns the status.
async fn put_url(app: &axum::Router, url: &str, bytes: Vec<u8>) -> StatusCode {
    app.clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(url)
                .body(Body::from(bytes))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn refresh_mints_fresh_url_for_same_staging_jti() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (_run_id, plan_id, job_id, token) = live_job(&app, &state).await;

    let upload_url = create_upload(&app, &plan_id, &job_id, &token, "refresh-me").await;
    let old_token = upload_url.split("/twirp-blob/artifact/").nth(1).unwrap();
    let old_jti = jwt_jti(old_token);

    let (status, body) = refresh(&app, old_token, &token).await;
    assert_eq!(status, StatusCode::OK, "refresh must succeed: {body:?}");
    assert_eq!(body["ok"], true);
    let new_url = body["signed_upload_url"].as_str().unwrap();
    let new_token = new_url.split("/twirp-blob/artifact/").nth(1).unwrap();
    // Note: a refresh in the same second as the create mints a byte-identical
    // token (second-granularity iat/exp) — that is fine; the invariant is the
    // preserved staging jti and a working upload URL.
    assert_eq!(
        jwt_jti(new_token),
        old_jti,
        "refresh must preserve the staging jti"
    );

    // The fresh URL uploads into the same session.
    assert_eq!(
        put_url(&app, new_url, b"via refreshed url".to_vec()).await,
        StatusCode::CREATED
    );
}

#[tokio::test]
async fn refresh_expired_url_succeeds() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (_run_id, plan_id, job_id, token) = live_job(&app, &state).await;

    let upload_url = create_upload(&app, &plan_id, &job_id, &token, "expired-refresh").await;
    let live_token = upload_url.split("/twirp-blob/artifact/").nth(1).unwrap();
    let jti = jwt_jti(live_token);

    // Forge the exact expired-URL scenario: same claims, 1s TTL, then wait it out.
    let expired = state
        .local_jwt_with_lifetime(
            blob_jwt_claims("artifact", blob_store::BLOB_OP_WRITE, &job_id, &jti),
            Duration::from_secs(1),
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(
        state.verify_local_jwt_claims(&expired).is_none(),
        "test token must actually be expired"
    );
    // Sanity: the expired URL is rejected for upload...
    assert_eq!(
        put_url(
            &app,
            &format!("/twirp-blob/artifact/{expired}"),
            b"x".to_vec()
        )
        .await,
        StatusCode::NOT_FOUND,
        "expired token must not authorize the PUT"
    );

    // ...but refreshes into a working URL for the same staging dir.
    let (status, body) = refresh(&app, &expired, &token).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "expired refresh must succeed: {body:?}"
    );
    let new_url = body["signed_upload_url"].as_str().unwrap().to_owned();
    assert_eq!(
        jwt_jti(new_url.split("/twirp-blob/artifact/").nth(1).unwrap()),
        jti
    );
    assert_eq!(
        put_url(&app, &new_url, b"after refresh".to_vec()).await,
        StatusCode::CREATED
    );
}

#[tokio::test]
async fn refresh_by_settled_job_is_forbidden() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, plan_id, job_id, token) = live_job(&app, &state).await;

    let upload_url = create_upload(&app, &plan_id, &job_id, &token, "settled").await;
    let old_token = upload_url.split("/twirp-blob/artifact/").nth(1).unwrap();

    // Settle the job through the real completion path.
    let run_id_parsed: RunId = run_id.parse().unwrap();
    complete_job(&state, run_id_parsed, "build", ExecutionStatus::Success).await;

    let (status, _body) = refresh(&app, old_token, &token).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a settled job must not extend its upload session"
    );
}

#[tokio::test]
async fn refresh_unknown_pending_is_not_found() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (_run_id, _plan_id, job_id, token) = live_job(&app, &state).await;

    // A well-formed, server-signed upload token for a jti with no pending
    // reservation (never created, or already swept).
    let orphan = state
        .local_jwt_with_lifetime(
            blob_jwt_claims(
                "artifact",
                blob_store::BLOB_OP_WRITE,
                &job_id,
                &uuid::Uuid::new_v4().to_string(),
            ),
            Duration::from_secs(600),
        )
        .unwrap();

    let (status, _body) = refresh(&app, &orphan, &token).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn refresh_other_jobs_pending_is_forbidden() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (_run_a, plan_a, job_a, token_a) = live_job(&app, &state).await;
    let (_run_b, _plan_b, _job_b, token_b) = live_job(&app, &state).await;

    let upload_url = create_upload(&app, &plan_a, &job_a, &token_a, "not-yours").await;
    let old_token = upload_url.split("/twirp-blob/artifact/").nth(1).unwrap();

    let (status, _body) = refresh(&app, old_token, &token_b).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a job must not refresh another job's upload session"
    );
}

#[tokio::test]
async fn refresh_rejects_tampered_token() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (_run_id, plan_id, job_id, token) = live_job(&app, &state).await;

    let upload_url = create_upload(&app, &plan_id, &job_id, &token, "tampered").await;
    let mut tampered = upload_url
        .split("/twirp-blob/artifact/")
        .nth(1)
        .unwrap()
        .to_owned();
    // Flip a character in the payload segment (signature now invalid).
    let mid = tampered.len() / 2;
    tampered.replace_range(mid..mid + 1, "A");

    let (status, _body) = refresh(&app, &tampered, &token).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn refresh_rejects_download_url() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (_run_id, plan_id, job_id, token) = live_job(&app, &state).await;

    let upload_url = create_upload(&app, &plan_id, &job_id, &token, "dl-refresh").await;
    assert_eq!(
        put_url(&app, &upload_url, b"final bytes".to_vec()).await,
        StatusCode::CREATED
    );
    let finalized = request_json_with_bearer(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact",
        json!({
            "workflow_run_backend_id": plan_id,
            "workflow_job_run_backend_id": job_id,
            "name": "dl-refresh",
            "size": "11",
        }),
        &token,
    )
    .await;
    assert_eq!(finalized["ok"], true);

    let signed = request_json_with_bearer(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.ArtifactService/GetSignedArtifactURL",
        json!({
            "workflow_run_backend_id": plan_id,
            "workflow_job_run_backend_id": job_id,
            "name": "dl-refresh",
        }),
        &token,
    )
    .await;
    let dl_url = signed["signed_url"].as_str().unwrap();
    // Artifact download URLs append `.zip` for toolkit content-type
    // detection; strip it before presenting the raw JWT.
    let dl_token = dl_url
        .split("/twirp-blob/artifact/")
        .nth(1)
        .unwrap()
        .strip_suffix(".zip")
        .unwrap();

    // A read-op download URL has no pending upload reservation: 404, and the
    // op gate would reject it even if it did.
    let (status, _body) = refresh(&app, dl_token, &token).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
