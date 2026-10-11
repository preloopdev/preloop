//! preloop-runner-server integration tests — diag upload token invalidation
//! and diagnostic staging reaping (round-3 finding §10).
//!
//! A live job could mint diag upload tokens without bound: the 32-cap
//! `diag_upload_tokens` map evicts the oldest token, but the blob gate only
//! checked the JWT signature + owner-job liveness — never map membership —
//! so an evicted token's PUT still returned 201. And `blobs/diag/` staging
//! directories were never reaped by any sweeper. These tests pin both
//! halves of the fix: eviction (or non-registration) invalidates the token,
//! and the reaper removes expired/aged staging directories.
//!
//! The config test mutates process env and serializes via GITHUB_ENV_LOCK,
//! the same discipline as the other test crates.

mod common;

use common::*;
use std::time::{Duration, SystemTime};

/// Mint one diag upload URL through the real Twirp endpoint, returning the
/// full URL the runner would PUT to.
async fn mint_diag_url(
    app: &axum::Router,
    runtime_token: &str,
    plan_id: &str,
    job_id: &str,
) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL")
                .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "workflow_run_backend_id": plan_id,
                        "workflow_job_run_backend_id": job_id,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    payload["diag_logs_url"].as_str().unwrap().to_owned()
}

/// The JWT embedded in a diag upload URL.
fn token_of_url(url: &str) -> &str {
    let (_, after) = url
        .split_once("/twirp-blob/diag/")
        .expect("diag URL must use the bearerless diag blob endpoint");
    after.split_once('?').map(|(t, _)| t).unwrap_or(after)
}

/// Round-3 §10 repro, fixed: 33 mints against the 32-cap per-job map evict
/// the oldest token, and the evicted token's PUT must no longer upload.
#[tokio::test]
async fn diag_evicted_upload_token_put_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let plan_id = uuid::Uuid::new_v4().to_string();
    let job_id = uuid::Uuid::new_v4();
    let runtime_token = state.mint_runtime_token(&plan_id, &job_id);
    // URL-minting writes and bearerless PUTs both require a live job record.
    register_live_job(&state, job_id, &plan_id).await;

    let mut urls = Vec::new();
    for _ in 0..33 {
        urls.push(mint_diag_url(&app, &runtime_token, &plan_id, &job_id.to_string()).await);
    }

    // The map holds exactly the 32 live tokens; the evicted one is whichever
    // minted URL's `jti` is no longer registered.
    let live_jtis: std::collections::HashSet<String> = {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.diag_upload_tokens.len(),
            32,
            "per-job cap must hold 32 live diag tokens after 33 mints"
        );
        inner.diag_upload_tokens.keys().cloned().collect()
    };
    let mut evicted = Vec::new();
    let mut live = Vec::new();
    for url in &urls {
        let token = token_of_url(url);
        let claims = verify_blob_token(&state, "diag", token).expect("minted token verifies");
        let jti = blob_token_jti(&claims).expect("minted token carries a jti");
        if live_jtis.contains(&jti) {
            live.push(url.clone());
        } else {
            evicted.push(url.clone());
        }
    }
    assert_eq!(evicted.len(), 1, "exactly the oldest token is evicted");
    assert_eq!(live.len(), 32);

    // The evicted token's PUT is rejected (not 201) — signature alone no
    // longer authorizes a diag upload.
    let rejected = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(evicted[0].clone())
                .body(Body::from(b"evicted token must not upload".as_slice()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::NOT_FOUND);

    // A token that was never registered at all is rejected the same way.
    let (unknown_jwt, _) = mint_blob_jwt(&state, "diag", &job_id.to_string());
    let unknown = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("/twirp-blob/diag/{unknown_jwt}"))
                .body(Body::from(b"unknown token must not upload".as_slice()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);

    // A live token's PUT still works — the fix must not break the runner's
    // legitimate bearerless diag upload.
    let accepted = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(live[0].clone())
                .body(Body::from(b"diagnostic log bytes".as_slice()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::CREATED);
}

/// The reaper deletes the staging directory of a diag token whose
/// reservation expired, and keeps the directory of a live token.
#[tokio::test]
async fn diag_reaper_removes_expired_token_staging_dirs() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    let stale_jti = uuid::Uuid::new_v4().to_string();
    let fresh_jti = uuid::Uuid::new_v4().to_string();
    let now = now_unix();
    {
        let mut inner = state.inner.lock().await;
        inner.diag_upload_tokens.insert(
            stale_jti.clone(),
            DiagUploadToken {
                job_id: "job-stale".to_owned(),
                created_unix: now - PENDING_UPLOAD_TTL.as_secs() as i64 - 100,
            },
        );
        inner.diag_upload_tokens.insert(
            fresh_jti.clone(),
            DiagUploadToken {
                job_id: "job-fresh".to_owned(),
                created_unix: now,
            },
        );
    }
    for jti in [&stale_jti, &fresh_jti] {
        let dir = temp.path().join("blobs").join("diag").join(jti);
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("data"), b"staged diag bytes")
            .await
            .unwrap();
    }

    reap_once(&shared).await;

    {
        let inner = state.inner.lock().await;
        assert!(
            !inner.diag_upload_tokens.contains_key(&stale_jti),
            "expired diag token leaves the map"
        );
        assert!(
            inner.diag_upload_tokens.contains_key(&fresh_jti),
            "live diag token stays in the map"
        );
    }
    assert!(
        !temp
            .path()
            .join("blobs")
            .join("diag")
            .join(&stale_jti)
            .exists(),
        "expired token's staging dir is reaped"
    );
    assert!(
        temp.path()
            .join("blobs")
            .join("diag")
            .join(&fresh_jti)
            .exists(),
        "live token's staging dir is kept"
    );
}

/// The age-based disk sweep deletes only staging directories older than the
/// configured max age. The clock is injected so the test never sleeps
/// through real ages.
#[tokio::test]
async fn diag_disk_sweep_removes_only_aged_staging_dirs() {
    let temp = tempfile::tempdir().unwrap();
    let state_dir = temp.path();
    let diag_root = state_dir.join("blobs").join("diag");

    // Missing root: nothing to sweep, no error.
    assert_eq!(
        sweep_diag_staging(state_dir, Duration::from_secs(1), SystemTime::now()).await,
        0
    );

    // Aged dir: older than the 24h default → removed.
    let old_dir = diag_root.join("old-staging");
    tokio::fs::create_dir_all(&old_dir).await.unwrap();
    tokio::fs::write(old_dir.join("data"), b"orphaned")
        .await
        .unwrap();
    // A stray file (not a directory) must never be swept.
    tokio::fs::write(diag_root.join("stray-file"), b"not a dir")
        .await
        .unwrap();
    let old_mtime = tokio::fs::metadata(&old_dir)
        .await
        .unwrap()
        .modified()
        .unwrap();
    let removed = sweep_diag_staging(
        state_dir,
        Duration::from_secs(24 * 3600),
        old_mtime + Duration::from_secs(25 * 3600),
    )
    .await;
    assert_eq!(removed, 1);
    assert!(!old_dir.exists(), "aged staging dir is swept");
    assert!(
        diag_root.join("stray-file").exists(),
        "non-directory entries are left alone"
    );

    // Fresh dir: younger than the max age → kept.
    let new_dir = diag_root.join("new-staging");
    tokio::fs::create_dir_all(&new_dir).await.unwrap();
    tokio::fs::write(new_dir.join("data"), b"in flight")
        .await
        .unwrap();
    let new_mtime = tokio::fs::metadata(&new_dir)
        .await
        .unwrap()
        .modified()
        .unwrap();
    let removed = sweep_diag_staging(
        state_dir,
        Duration::from_secs(24 * 3600),
        new_mtime + Duration::from_secs(3600),
    )
    .await;
    assert_eq!(removed, 0);
    assert!(new_dir.exists(), "fresh staging dir is kept");
}

/// The new config key parses with a 24h default and honors the env override.
#[tokio::test]
async fn diag_staging_max_age_config_defaults_and_env_override() {
    let _env = GITHUB_ENV_LOCK.lock().await;
    let config = ConfigFile::default();
    assert_eq!(config.diag_staging_max_age_hours, 24);
    assert_eq!(diag_staging_max_age_hours(&config).unwrap(), 24);

    let _var = TestEnvVar::set(DIAG_STAGING_MAX_AGE_HOURS_ENV, "48");
    assert_eq!(diag_staging_max_age_hours(&config).unwrap(), 48);
}
