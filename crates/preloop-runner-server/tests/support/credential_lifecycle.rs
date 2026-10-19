//! Job-credential revocation regressions.
//!
//! A job runtime token is exported to workflow code as
//! `ACTIONS_RUNTIME_TOKEN`, and every signed blob/replay/action URL is a
//! bearerless capability minted from it. All of them must stop authorizing
//! once the attempt is no longer live — settled, cancelled, timed out, lease
//! expired, or replaced by a retry — and a live job must not spend another
//! job's signed credential.

use super::common::*;

async fn request(
    app: &axum::Router,
    method: Method,
    uri: &str,
    body: Vec<u8>,
    bearer: Option<&str>,
) -> StatusCode {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(axum::http::header::CONTENT_TYPE, "application/json");
    if let Some(bearer) = bearer {
        builder = builder.header(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {bearer}"),
        );
    }
    app.clone()
        .oneshot(builder.body(Body::from(body)).unwrap())
        .await
        .unwrap()
        .status()
}

/// A live attempt plus the run key its logical job belongs to.
async fn live_job(state: &AppState) -> (uuid::Uuid, String, String) {
    let job = uuid::Uuid::new_v4();
    let run_key = register_live_job_in(state, job, "plan-cred-lifecycle", "owner/repo").await;
    let plan_id = state
        .test_tx()
        .await
        .job_requests
        .values()
        .find(|request| request.agent_job_id == job)
        .expect("registered attempt")
        .plan_id
        .clone();
    (job, run_key, plan_id)
}

/// Mark the attempt's logical job terminal without settling its request row —
/// the cancellation window that used to stay fully authorized until the
/// runner reported.
async fn cancel_logical_job(state: &AppState, run_key: &str) {
    let updated = state
        .test_db_mutate(|tx| {
            tx.execute(
                "UPDATE jobs SET status = 'cancelled' WHERE run_id = ?1 AND job_id = 'test-job'",
                (run_key,),
            )
        })
        .await
        .expect("cancel logical job");
    assert_eq!(updated, 1);
}

async fn expire_lease(state: &AppState, job: uuid::Uuid) {
    let stale = crate::store::now_us()
        - (crate::distributed_task::HUNG_WORKER_LEASE_SECONDS as i64 + 1) * 1_000_000;
    let updated = state
        .test_db_mutate(|tx| {
            tx.execute(
                "INSERT INTO job_leases (request_id, runner_id, expires_at, renewed_at) \
                 SELECT request_id, 1, ?2, ?2 FROM job_requests WHERE agent_job_id = ?1 \
                 ON CONFLICT (request_id) DO UPDATE SET renewed_at = excluded.renewed_at",
                (job.to_string(), stale),
            )
        })
        .await
        .expect("age the lease");
    assert_eq!(updated, 1);
}

async fn trigger_timeout(state: &AppState, job: uuid::Uuid) {
    let updated = state
        .test_db_mutate(|tx| {
            tx.execute(
                "UPDATE job_requests SET timeout_triggered = 1 WHERE agent_job_id = ?1",
                (job.to_string(),),
            )
        })
        .await
        .expect("trigger timeout");
    assert_eq!(updated, 1);
}

/// Signed blob URLs are derived credentials. They must work while the owner
/// is live, reject a different job's bearer, and die on completion — for
/// downloads as well as uploads.
#[tokio::test]
async fn signed_blob_urls_die_with_their_job_and_reject_cross_owner_bearers() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (owner, _run, _plan) = live_job(&state).await;
    let (other, _other_run, other_plan) = live_job(&state).await;

    let (read_jwt, jti) = mint_blob_jwt(&state, "artifact", &owner.to_string(), "read");
    let data_dir = state.state_dir.join("blobs").join("artifact").join(&jti);
    tokio::fs::create_dir_all(&data_dir).await.unwrap();
    tokio::fs::write(data_dir.join("data"), b"artifact bytes")
        .await
        .unwrap();
    let read_uri = format!("/twirp-blob/artifact/{read_jwt}");
    assert_eq!(
        request(&app, Method::GET, &read_uri, Vec::new(), None).await,
        StatusCode::OK,
        "a live owner's download URL must serve"
    );

    let (write_jwt, _jti) = mint_blob_jwt(&state, "artifact", &owner.to_string(), "write");
    let write_uri = format!("/twirp-blob/artifact/{write_jwt}");
    assert_eq!(
        request(&app, Method::PUT, &write_uri, vec![b'x'; 16], None).await,
        StatusCode::CREATED,
        "a live owner's upload URL must accept data"
    );

    // A live job may not present its own bearer against another owner's URL.
    let other_token = state.mint_runtime_token(&other_plan, &other);
    assert_eq!(
        request(&app, Method::GET, &read_uri, Vec::new(), Some(&other_token)).await,
        StatusCode::FORBIDDEN,
        "cross-owner blob reads must fail even when both jobs are live"
    );

    complete_live_job(&state, owner).await;
    assert_eq!(
        request(&app, Method::GET, &read_uri, Vec::new(), None).await,
        StatusCode::FORBIDDEN,
        "a completed job's download URL must be rejected"
    );
    assert_eq!(
        request(&app, Method::PUT, &write_uri, vec![b'y'; 16], None).await,
        StatusCode::FORBIDDEN,
        "a completed job's upload URL must be rejected"
    );
}

/// A terminal producer must not take its finalized bytes with it: a live job's
/// own download ticket still serves them. The liveness rule belongs to the
/// credential that was minted (the requester), never to whoever produced the
/// blob. A completed producer's copied ticket, by contrast, dies.
#[tokio::test]
async fn live_job_downloads_terminal_producers_blob_with_its_own_ticket() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (producer, _producer_run, _producer_plan) = live_job(&state).await;
    let (consumer, _consumer_run, _consumer_plan) = live_job(&state).await;

    // The producer finalized its blob. Completing the producer ends its own
    // credentials; the bytes stay in the artifact staging directory.
    let (producer_jwt, jti) = mint_blob_jwt(&state, "artifact", &producer.to_string(), "read");
    let data_dir = state.state_dir.join("blobs").join("artifact").join(&jti);
    tokio::fs::create_dir_all(&data_dir).await.unwrap();
    tokio::fs::write(data_dir.join("data"), b"produced bytes")
        .await
        .unwrap();
    let producer_uri = format!("/twirp-blob/artifact/{producer_jwt}");
    assert_eq!(
        request(&app, Method::GET, &producer_uri, Vec::new(), None).await,
        StatusCode::OK
    );
    complete_live_job(&state, producer).await;
    assert_eq!(
        request(&app, Method::GET, &producer_uri, Vec::new(), None).await,
        StatusCode::FORBIDDEN,
        "the completed producer's own ticket is dead"
    );

    // A live consumer gets its own read capability for the same finalized
    // store, exactly as `GetSignedArtifactURL`/cache lookup mint one.
    let consumer_jwt =
        mint_blob_jwt_for_jti(&state, "artifact", &consumer.to_string(), "read", &jti);
    let consumer_uri = format!("/twirp-blob/artifact/{consumer_jwt}");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(&consumer_uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a live job's own ticket must still serve a terminal producer's blob"
    );
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(bytes.as_ref(), b"produced bytes");

    complete_live_job(&state, consumer).await;
    assert_eq!(
        request(&app, Method::GET, &consumer_uri, Vec::new(), None).await,
        StatusCode::FORBIDDEN,
        "the consumer's ticket dies with the consumer, not with the producer"
    );
}

/// Replay log upload tickets are bearerless and signed only for a path. The
/// path names the owning job, and the serve-time gate re-checks its liveness.
#[tokio::test]
async fn replay_upload_tickets_die_with_their_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (job, _run, plan) = live_job(&state).await;
    let (other, _other_run, _other_plan) = live_job(&state).await;

    let expires_at = crate::auth::replay_ticket_expiry();
    let path = format!("/replay/results/{plan}/{job}/step-1.txt");
    let sig = crate::auth::sign_replay_upload_ticket(&state, &path, expires_at);
    let uri = format!("{path}?sv=2021-08-06&se={expires_at}&sr=c&sp=rw&sig={sig}");

    assert!(
        request(&app, Method::PUT, &uri, b"live log".to_vec(), None)
            .await
            .is_success(),
        "a live job's signed upload URL must work"
    );

    // The same signature does not authorize another job's path.
    let other_path = format!("/replay/results/{plan}/{other}/step-1.txt");
    assert_eq!(
        request(
            &app,
            Method::PUT,
            &format!("{other_path}?se={expires_at}&sig={sig}"),
            b"forged".to_vec(),
            None,
        )
        .await,
        StatusCode::UNAUTHORIZED,
        "a ticket signed for one job must not authorize another path"
    );

    complete_live_job(&state, job).await;
    assert_eq!(
        request(&app, Method::PUT, &uri, b"after death".to_vec(), None).await,
        StatusCode::FORBIDDEN,
        "a completed job's signed upload URL must be rejected"
    );
}

/// Results Twirp reads are job-scoped too. A settled token must not list
/// artifacts or mint fresh download URLs.
#[tokio::test]
async fn twirp_reads_die_with_their_job_and_reject_cross_run_reads() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (job, _run_key, plan) = live_job(&state).await;
    let (other, _other_run, other_plan) = live_job(&state).await;

    let token = state.mint_runtime_token(&plan, &job);
    let body = json!({
        "workflow_run_backend_id": plan.clone(),
        "workflow_job_run_backend_id": job.to_string(),
    })
    .to_string();
    let list_uri = "/twirp/github.actions.results.api.v1.ArtifactService/ListArtifacts";
    assert_eq!(
        request(
            &app,
            Method::POST,
            list_uri,
            body.into_bytes(),
            Some(&token)
        )
        .await,
        StatusCode::OK,
        "a live job may read its own run's artifact metadata"
    );

    // A different live job's token cannot read this run.
    let other_token = state.mint_runtime_token(&other_plan, &other);
    let cross_body = json!({
        "workflow_run_backend_id": plan.clone(),
        "workflow_job_run_backend_id": other.to_string(),
    })
    .to_string();
    assert_eq!(
        request(
            &app,
            Method::POST,
            list_uri,
            cross_body.into_bytes(),
            Some(&other_token)
        )
        .await,
        StatusCode::FORBIDDEN,
        "a live job must not read another job's run scope"
    );
    // Ensure the other run key is otherwise usable by its own token.
    let own_body = json!({
        "workflow_run_backend_id": other_plan.clone(),
        "workflow_job_run_backend_id": other.to_string(),
    })
    .to_string();
    assert_eq!(
        request(
            &app,
            Method::POST,
            list_uri,
            own_body.into_bytes(),
            Some(&other_token)
        )
        .await,
        StatusCode::OK
    );

    complete_live_job(&state, job).await;
    let stale_body = json!({
        "workflow_run_backend_id": plan,
        "workflow_job_run_backend_id": job.to_string(),
    })
    .to_string();
    assert_eq!(
        request(
            &app,
            Method::POST,
            list_uri,
            stale_body.into_bytes(),
            Some(&token)
        )
        .await,
        StatusCode::FORBIDDEN,
        "a settled job's runtime token must not keep reading Results"
    );
}

/// Deadline, cancellation-before-report, and an expired lease each revoke a
/// token without waiting for the reaper's next settlement tick.
#[tokio::test]
async fn timeout_cancel_and_expired_lease_revoke_job_credentials() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (job, run_key, _plan) = live_job(&state).await;
    let (read_jwt, jti) = mint_blob_jwt(&state, "artifact", &job.to_string(), "read");
    let data_dir = state.state_dir.join("blobs").join("artifact").join(&jti);
    tokio::fs::create_dir_all(&data_dir).await.unwrap();
    tokio::fs::write(data_dir.join("data"), b"x").await.unwrap();
    let uri = format!("/twirp-blob/artifact/{read_jwt}");

    assert_eq!(
        request(&app, Method::GET, &uri, Vec::new(), None).await,
        StatusCode::OK
    );

    trigger_timeout(&state, job).await;
    assert_eq!(
        request(&app, Method::GET, &uri, Vec::new(), None).await,
        StatusCode::FORBIDDEN,
        "the reaper's deadline flag must revoke immediately"
    );
    state
        .test_db_mutate(|tx| {
            tx.execute(
                "UPDATE job_requests SET timeout_triggered = 0 WHERE agent_job_id = ?1",
                (job.to_string(),),
            )
        })
        .await
        .unwrap();

    cancel_logical_job(&state, &run_key).await;
    assert_eq!(
        request(&app, Method::GET, &uri, Vec::new(), None).await,
        StatusCode::FORBIDDEN,
        "cancellation must revoke before the runner reports"
    );
    state
        .test_db_mutate(|tx| {
            tx.execute(
                "UPDATE jobs SET status = 'queued' WHERE run_id = ?1 AND job_id = 'test-job'",
                (&run_key,),
            )
        })
        .await
        .unwrap();

    expire_lease(&state, job).await;
    assert_eq!(
        request(&app, Method::GET, &uri, Vec::new(), None).await,
        StatusCode::FORBIDDEN,
        "a lease older than the reaper window must revoke before settlement"
    );
}

/// Debug credentials are job-scoped: both the one-shot runtime exchange and
/// a previously issued debug-worker token stop working after settlement.
#[tokio::test]
async fn debug_credentials_die_with_their_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (job, _run, plan) = live_job(&state).await;
    let runtime = state.mint_runtime_token(&plan, &job);
    let worker = state.mint_debug_worker_token(&plan, &job);

    complete_live_job(&state, job).await;

    assert_eq!(
        request(
            &app,
            Method::POST,
            "/api/v1/debug/worker-token",
            json!({ "agent_job_id": job }).to_string().into_bytes(),
            Some(&runtime),
        )
        .await,
        StatusCode::FORBIDDEN,
        "a settled job's runtime token must not mint a debug credential"
    );
    assert_eq!(
        request(
            &app,
            Method::POST,
            "/api/v1/debug/sessions",
            json!({
                "run_id": RunId::new(),
                "job_id": "test-job",
                "agent_job_id": job,
                "job_name": "test-job",
                "step": {"index": 0, "total": 1, "context_name": "__run", "display_name": "step"}
            })
            .to_string()
            .into_bytes(),
            Some(&worker),
        )
        .await,
        StatusCode::FORBIDDEN,
        "a settled job's debug-worker token must not open a session"
    );
}

/// Signed action-archive URLs are derived capabilities too: they carry the
/// owning job in the HMAC and stop serving once that job settles.
#[tokio::test]
async fn action_download_tickets_die_with_their_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (job, _run, _plan) = live_job(&state).await;

    let cache_dir = state
        .state_dir
        .join("actions")
        .join("acme")
        .join("public-action")
        .join("v1");
    tokio::fs::create_dir_all(&cache_dir).await.unwrap();
    tokio::fs::write(cache_dir.join("action.tar.gz"), b"tar")
        .await
        .unwrap();

    let expires_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let signature =
        state.sign_action_ticket(&job.to_string(), "acme", "public-action", "v1", expires_at);
    let uri = format!(
        "/api/v1/actions/download/acme/public-action/v1?exp={expires_at}&job={job}&sig={signature}"
    );
    assert_eq!(
        request(&app, Method::GET, &uri, Vec::new(), None).await,
        StatusCode::OK,
        "a live job's archive ticket must serve"
    );

    complete_live_job(&state, job).await;
    assert_eq!(
        request(&app, Method::GET, &uri, Vec::new(), None).await,
        StatusCode::NOT_FOUND,
        "a settled job's archive ticket must stop serving"
    );
}
