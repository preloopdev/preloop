//! Secondary-credential mint liveness regression tests (round-3 finding 5
//! + the secondary-credential cascade).
//!
//! Every endpoint that mints a credential outliving the caller's runtime
//! token must refuse a settled job's still-valid token: OIDC id-tokens,
//! artifact download URLs, cache download URLs, replay upload tickets, and
//! action download tickets. Each test drives the mint with a live job's
//! token (must succeed) and again after the job completes (must be 403).

mod common;

use common::*;

struct LiveJob {
    _temp: tempfile::TempDir,
    app: Router,
    run_id: String,
    plan_id: String,
    job_id: String,
    token: String,
}

/// Submit a one-job workflow and mint its runtime token, like the broker
/// would for a claimed job.
async fn submit_live_job(workflow: &str) -> LiveJob {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = submit_yaml(&app, workflow, "owner/repo").await;
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
    LiveJob {
        _temp: temp,
        app,
        run_id,
        plan_id,
        job_id,
        token,
    }
}

const SIMPLE_WORKFLOW: &str =
    "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";

fn oidc_uri(job: &LiveJob) -> String {
    format!(
        "/runner/server/_apis/distributedtask/hubs/actions/plans/{}/jobs/{}/oidctoken?audience=api://preloop-test",
        job.plan_id, job.job_id
    )
}

#[tokio::test]
async fn oidc_mint_refused_for_settled_job() {
    let workflow = "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    permissions:\n      id-token: write\n    steps:\n      - run: echo hi\n";
    let job = submit_live_job(workflow).await;

    // Live job mints fine.
    let (status, body) = {
        let response = job
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(oidc_uri(&job))
                    .header(header::AUTHORIZATION, format!("Bearer {}", job.token))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (
            status,
            serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null),
        )
    };
    assert_eq!(status, StatusCode::OK, "live job must mint an OIDC token");
    let value = body["value"].as_str().unwrap_or("");
    assert_eq!(
        value.split('.').count(),
        3,
        "minted OIDC token must be a JWT"
    );

    // Settle the job; the same still-unexpired runtime token must no
    // longer mint.
    complete_via_api(&job.app, &job.run_id, "build").await;
    let status = status_with_bearer(
        &job.app,
        &job.token,
        Method::GET,
        &oidc_uri(&job),
        Value::Null,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "settled job must not mint OIDC tokens"
    );
}

#[tokio::test]
async fn artifact_download_url_mint_refused_for_settled_job() {
    let job = submit_live_job(SIMPLE_WORKFLOW).await;
    let base = json!({
        "workflow_run_backend_id": job.plan_id,
        "workflow_job_run_backend_id": job.job_id,
        "name": "mint-liveness-artifact",
    });

    // Live: full upload round trip, then mint the download URL.
    let create = request_json_with_bearer(
        &job.app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact",
        base.clone(),
        &job.token,
    )
    .await;
    assert_eq!(create["ok"], true);
    let upload_url = create["signed_upload_url"].as_str().unwrap().to_owned();
    let upload = job
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(&upload_url)
                .body(Body::from(b"artifact bytes".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::CREATED);
    let finalized = request_json_with_bearer(
        &job.app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact",
        json!({
            "workflow_run_backend_id": job.plan_id,
            "workflow_job_run_backend_id": job.job_id,
            "name": "mint-liveness-artifact",
            "size": "14",
        }),
        &job.token,
    )
    .await;
    assert_eq!(finalized["ok"], true);
    let signed = request_json_with_bearer(
        &job.app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.ArtifactService/GetSignedArtifactURL",
        base.clone(),
        &job.token,
    )
    .await;
    assert!(
        signed["signed_url"].as_str().is_some_and(|u| !u.is_empty()),
        "live job must mint an artifact download URL"
    );

    // Settled: mint refused.
    complete_via_api(&job.app, &job.run_id, "build").await;
    let status = status_with_bearer(
        &job.app,
        &job.token,
        Method::POST,
        "/twirp/github.actions.results.api.v1.ArtifactService/GetSignedArtifactURL",
        base,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "settled job must not mint artifact download URLs"
    );
}

#[tokio::test]
async fn cache_download_url_mint_refused_for_settled_job() {
    let job = submit_live_job(SIMPLE_WORKFLOW).await;
    let cache_body = json!({"key": "mint-liveness-key", "version": "v1"});
    let create_uri = "/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry";
    let restore_uri = "/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL";

    // Live: store an entry, then mint its download URL.
    let response = job
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(create_uri)
                .header(header::AUTHORIZATION, format!("Bearer {}", job.token))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(cache_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["ok"], true);
    let upload_url = body["signed_upload_url"].as_str().unwrap().to_owned();
    let upload = job
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(&upload_url)
                .body(Body::from(b"cache bytes".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::CREATED);
    let finalized = request_json_with_bearer(
        &job.app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.CacheService/FinalizeCacheEntryUpload",
        cache_body.clone(),
        &job.token,
    )
    .await;
    assert_eq!(finalized["ok"], true);

    let minted = request_json_with_bearer(
        &job.app,
        Method::POST,
        restore_uri,
        cache_body.clone(),
        &job.token,
    )
    .await;
    assert_eq!(
        minted["ok"], true,
        "cache restore must hit the stored entry"
    );
    assert!(
        minted["signed_download_url"]
            .as_str()
            .is_some_and(|u| !u.is_empty()),
        "live job must mint a cache download URL"
    );

    // Settled: mint refused.
    complete_via_api(&job.app, &job.run_id, "build").await;
    let status =
        status_with_bearer(&job.app, &job.token, Method::POST, restore_uri, cache_body).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "settled job must not mint cache download URLs"
    );
}

#[tokio::test]
async fn replay_upload_ticket_mint_refused_for_settled_job() {
    let job = submit_live_job(SIMPLE_WORKFLOW).await;
    let uri = "/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL";
    let body = json!({
        "workflow_run_backend_id": job.plan_id,
        "workflow_job_run_backend_id": job.job_id,
        "step_backend_id": "1",
    });

    // Live job mints fine (the write-path gate already covered this; this
    // locks the behavior in).
    let minted =
        request_json_with_bearer(&job.app, Method::POST, uri, body.clone(), &job.token).await;
    assert!(
        minted["logs_url"].as_str().is_some_and(|u| !u.is_empty()),
        "live job must mint a replay upload ticket"
    );

    // Settled: mint refused.
    complete_via_api(&job.app, &job.run_id, "build").await;
    let status = status_with_bearer(&job.app, &job.token, Method::POST, uri, body).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "settled job must not mint replay upload tickets"
    );
}

#[tokio::test]
async fn action_download_info_mint_refused_for_settled_job() {
    let job = submit_live_job(SIMPLE_WORKFLOW).await;
    // `./`-relative refs short-circuit before any network fetch, so the
    // test needs no GitHub access: a 200 proves the liveness gate passed.
    let uri = format!("/_apis/v1/ActionDownloadInfo/owner/repo/{}", job.plan_id);
    let body = json!({"actions": [{"nameWithOwner": "./local-action", "ref": ""}]});

    let status = status_with_bearer(&job.app, &job.token, Method::POST, &uri, body.clone()).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "live job must reach action ticket minting"
    );

    let runnerresolve_uri = format!(
        "/actions/build/orchestration/jobs/{}/runnerresolve/actions",
        job.job_id
    );
    let status = status_with_bearer(
        &job.app,
        &job.token,
        Method::POST,
        &runnerresolve_uri,
        json!({"action": "./local-action"}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "live job must reach runnerresolve ticket minting"
    );

    // Settled: both mints refused.
    complete_via_api(&job.app, &job.run_id, "build").await;
    let status = status_with_bearer(&job.app, &job.token, Method::POST, &uri, body).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "settled job must not mint action download tickets"
    );
    let status = status_with_bearer(
        &job.app,
        &job.token,
        Method::POST,
        &runnerresolve_uri,
        json!({"action": "./local-action"}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "settled job must not mint runnerresolve tickets"
    );
}
