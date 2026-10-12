//! Blob signed-URL op/job binding regression tests (round-3 finding 3).
//!
//! Signed `/twirp-blob` URLs used to carry `"job": ""` with no operation
//! claim, so a *download* URL accepted a bearerless PUT that overwrote the
//! finalized blob. URLs now carry `op` (`read`|`write`) and a non-empty
//! `job` owner, and serve-time enforcement rejects cross-operation use with
//! 403.

mod common;

use common::*;

/// The round-3 repro, end to end: CreateArtifact -> bearerless PUT of the
/// original bytes -> FinalizeArtifact -> GetSignedArtifactURL -> bearerless
/// PUT to the *download* URL with different bytes. Before the fix the last
/// PUT overwrote the finalized artifact; now it is 403 and the download
/// returns the original bytes.
#[tokio::test]
async fn artifact_download_url_rejects_put_and_preserves_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let original = b"original artifact bytes".to_vec();
    let evil = b"OVERWRITTEN BY ATTACKER".to_vec();

    let owner_run = submit_yaml(&app, workflow, "owner/repo").await;
    let owner_run_id = owner_run["run_id"].as_str().unwrap().to_owned();
    let (plan_id, job_id, token) = {
        let inner = state.test_tx().await;
        let message = queued_message_for(&inner, &owner_run_id);
        (
            message.plan.plan_id.clone(),
            message.job_id.to_string(),
            state.mint_runtime_token(&message.plan.plan_id, &message.job_id),
        )
    };

    let create = request_json_with_bearer(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact",
        json!({
            "workflow_run_backend_id": plan_id.clone(),
            "workflow_job_run_backend_id": job_id.clone(),
            "name": "bound-artifact",
        }),
        &token,
    )
    .await;
    assert_eq!(create["ok"], true);
    let upload_url = create["signed_upload_url"].as_str().unwrap().to_owned();

    // An upload URL must not serve GETs (op binding, the other direction).
    let get_upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(&upload_url)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_upload.status(), StatusCode::FORBIDDEN);

    // Bearerless PUT of the original bytes (the Azure-SDK-style flow).
    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(&upload_url)
                .body(Body::from(original.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::CREATED);

    let finalized = request_json_with_bearer(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact",
        json!({
            "workflow_run_backend_id": plan_id,
            "workflow_job_run_backend_id": job_id,
            "name": "bound-artifact",
            "size": original.len().to_string(),
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
            "name": "bound-artifact",
        }),
        &token,
    )
    .await;
    let signed_url = signed["signed_url"].as_str().unwrap().to_owned();

    // The attack: bearerless PUT to the *download* URL with different bytes.
    let overwrite = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(&signed_url)
                .body(Body::from(evil))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(overwrite.status(), StatusCode::FORBIDDEN);

    // The legitimate flow still works: the download returns the original
    // bytes, not the attacker's.
    let downloaded = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(&signed_url)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(downloaded.status(), StatusCode::OK);
    let body = to_bytes(downloaded.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), original.as_slice());
}

/// A cache `read` token must reject PUTs too: the binding is per-claim, not
/// per-kind.
#[tokio::test]
async fn cache_download_url_rejects_put() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let job_id = uuid::Uuid::new_v4();
    let (read_jwt, _jti) = mint_blob_jwt(&state, "cache", &job_id.to_string(), "read");

    let put = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("/twirp-blob/cache/{read_jwt}"))
                .body(Body::from(vec![b'x'; 16]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::FORBIDDEN);
}

/// The pre-fix claim shape (valid signature, `op: read`, empty `job`) is
/// rejected on both operations: empty owners no longer exist.
#[tokio::test]
async fn blob_token_with_empty_job_claim_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let jti = uuid::Uuid::new_v4().to_string();
    let token = state
        .local_jwt_with_lifetime(
            json!({
                "sub": "preloop-blob",
                "kind": "artifact",
                "op": "read",
                "job": "",
                "jti": jti,
            }),
            memory_caps::SIGNED_BLOB_URL_TTL,
        )
        .unwrap();

    for method in [Method::GET, Method::PUT] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method.clone())
                    .uri(format!("/twirp-blob/artifact/{token}"))
                    .body(Body::from(vec![b'x'; 16]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{method}");
    }
}

/// Legacy tokens minted before op binding (no `op` claim at all) are
/// rejected on both operations.
#[tokio::test]
async fn blob_token_without_op_claim_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let job_id = uuid::Uuid::new_v4().to_string();
    let jti = uuid::Uuid::new_v4().to_string();
    let token = state
        .local_jwt_with_lifetime(
            json!({
                "sub": "preloop-blob",
                "kind": "artifact",
                "job": job_id,
                "jti": jti,
            }),
            memory_caps::SIGNED_BLOB_URL_TTL,
        )
        .unwrap();

    for method in [Method::GET, Method::PUT] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method.clone())
                    .uri(format!("/twirp-blob/artifact/{token}"))
                    .body(Body::from(vec![b'x'; 16]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{method}");
    }
}

/// Minted blob URLs carry a short TTL (20 minutes), not the old hour: the
/// `exp` window on a freshly minted token bounds replay.
#[tokio::test]
async fn signed_blob_url_ttl_is_short() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let job_id = uuid::Uuid::new_v4().to_string();
    let (jwt, _jti) = mint_blob_jwt(&state, "artifact", &job_id, "write");
    let claims = state
        .verify_local_jwt_claims(&jwt)
        .expect("minted blob JWT must verify");
    let iat = claims["iat"].as_u64().unwrap();
    let exp = claims["exp"].as_u64().unwrap();
    assert_eq!(
        exp - iat,
        memory_caps::SIGNED_BLOB_URL_TTL.as_secs(),
        "blob URL TTL must be the short signed-URL TTL"
    );
    assert!(
        memory_caps::SIGNED_BLOB_URL_TTL <= std::time::Duration::from_secs(30 * 60),
        "blob URL TTL must stay at ~15-30 minutes"
    );
}

/// Control-plane-minted upload URLs carry the `system` owner (never empty)
/// and still accept bearerless PUTs: the `system` owner skips the
/// job-liveness gate.
#[tokio::test]
async fn system_minted_upload_url_accepts_bearerless_put() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";

    let owner_run = submit_yaml(&app, workflow, "owner/repo").await;
    let owner_run_id = owner_run["run_id"].as_str().unwrap().to_owned();
    let plan_id = {
        let inner = state.test_tx().await;
        queued_message_for(&inner, &owner_run_id)
            .plan
            .plan_id
            .clone()
    };

    let create = request_json_with_bearer(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact",
        json!({
            "workflow_run_backend_id": plan_id.clone(),
            "workflow_job_run_backend_id": "",
            "name": "system-artifact",
        }),
        &state.system_token,
    )
    .await;
    assert_eq!(create["ok"], true);
    let upload_url = create["signed_upload_url"].as_str().unwrap().to_owned();

    // The minted token names the `system` owner, never "".
    let token = upload_url
        .rsplit_once("/twirp-blob/artifact/")
        .map(|(_, t)| t)
        .unwrap();
    let claims = state
        .verify_local_jwt_claims(token)
        .expect("upload URL token must verify");
    assert_eq!(claims["op"].as_str(), Some("write"));
    assert_eq!(claims["job"].as_str(), Some("system"));

    let bytes = b"control-plane artifact bytes".to_vec();
    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(&upload_url)
                .body(Body::from(bytes.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::CREATED);

    let finalized = request_json_with_bearer(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact",
        json!({
            "workflow_run_backend_id": plan_id,
            "workflow_job_run_backend_id": "",
            "name": "system-artifact",
            "size": bytes.len().to_string(),
        }),
        &state.system_token,
    )
    .await;
    assert_eq!(finalized["ok"], true);
}
