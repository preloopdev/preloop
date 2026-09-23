//! preloop-runner-server integration tests — logs group.
//! Split from the former `lib_tests.rs` unit; see `tests/common/mod.rs`.

mod common;

use common::*;

#[tokio::test]
async fn all_twirp_api_routes_reject_missing_bearer_before_body_validation() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    // The malformed body proves auth runs before any route-specific JSON extractor.
    let routes = [
        "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
        "/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
        "/twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL",
        "/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
        "/twirp/results.services.receiver.Receiver/GetStepSummarySignedBlobURL",
        "/twirp/results.services.receiver.Receiver/CreateStepSummaryMetadata",
        "/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
        "/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata",
        "/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
        "/twirp/github.actions.results.api.v1.CacheService/FinalizeCacheEntryUpload",
        "/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL",
        "/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact",
        "/twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact",
        "/twirp/github.actions.results.api.v1.ArtifactService/ListArtifacts",
        "/twirp/github.actions.results.api.v1.ArtifactService/GetSignedArtifactURL",
        "/twirp/github.actions.results.api.v1.ArtifactService/DeleteArtifact",
    ];

    for route in routes {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(route)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{route}");
    }
}

#[tokio::test]
async fn results_cache_and_artifact_routes_reject_inconsistent_bearers() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let subject_job = uuid::Uuid::new_v4();
    let other_job = uuid::Uuid::new_v4();

    let mismatched = state
        .local_jwt(json!({
            "sub": format!("preloop-job-{subject_job}"),
            "scp": format!("Actions.Results:plan-{other_job}:{other_job}"),
        }))
        .unwrap();
    let malformed = state
        .local_jwt(json!({
            "sub": format!("preloop-job-{subject_job}"),
            "scp": "Actions.Results:plan",
        }))
        .unwrap();
    let routes = [
        (
            "/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
            json!({"key": "rejected-cache", "version": "v1"}),
        ),
        (
            "/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact",
            json!({
                "workflow_run_backend_id": "rejected-plan",
                "workflow_job_run_backend_id": subject_job.to_string(),
                "name": "rejected-artifact",
            }),
        ),
    ];

    for (token, reason) in [
        (&mismatched, "mismatched subject/scope"),
        (&malformed, "malformed scope"),
    ] {
        for (uri, body) in &routes {
            assert_eq!(
                status_with_bearer(&app, token, Method::POST, uri, body.clone()).await,
                StatusCode::UNAUTHORIZED,
                "{reason} must be rejected by the Results bearer gate on {uri}"
            );
        }
    }

    // A regular runner token for a registered trusted run remains usable on
    // the cache Results route and carries the same job identity the quota
    // helper records. (Artifact creation additionally requires run scoping,
    // and unregistered jobs fail closed on cache writes, so the cache route
    // with a real job is the right place to prove gate-plus-quota.)
    let trusted = crate::submit_run_inner(
        &state.shared(),
        preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n"
                .to_owned(),
            event: "push".to_owned(),
            payload: json!({"ref": "refs/heads/main", "commits": []}),
            repository: "owner/repo".to_owned(),
            git_ref: "refs/heads/main".to_owned(),
            trust_tier: None,
            ..Default::default()
        },
    )
    .await
    .expect("trusted submission accepted");
    let (matching, subject_job) = {
        let inner = state.inner.lock().await;
        let message = queued_message_for(&inner, &trusted.run_id.to_string());
        (
            state.mint_runtime_token(&message.plan.plan_id, &message.job_id),
            message.job_id,
        )
    };
    assert_eq!(
        status_with_bearer(
            &app,
            &matching,
            Method::POST,
            "/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
            json!({"key": "runner-cache", "version": "v1"}),
        )
        .await,
        StatusCode::OK
    );

    // The administrator credential intentionally remains a cross-job Results
    // credential and is not assigned to a per-job quota bucket.
    assert_eq!(
        status_with_bearer(
            &app,
            &state.system_token,
            Method::POST,
            "/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
            json!({"key": "system-cache", "version": "v1"}),
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(
        status_with_bearer(
            &app,
            &state.system_token,
            Method::POST,
            "/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact",
            json!({
                "workflow_run_backend_id": "system-plan",
                "workflow_job_run_backend_id": "system-job",
                "name": "system-artifact",
            }),
        )
        .await,
        StatusCode::OK
    );

    let subject_job = subject_job.to_string();
    let inner = state.inner.lock().await;
    assert_eq!(inner.cache_v2_pending.len(), 2);
    let cache_job_ids = inner
        .cache_v2_pending
        .values()
        .map(|pending| pending.job_backend_id.as_str())
        .collect::<Vec<_>>();
    assert!(
        cache_job_ids.contains(&subject_job.as_str()),
        "the runner token's reservation must carry its job quota identity"
    );
    assert!(
        cache_job_ids.contains(&""),
        "system Results credentials must not enter a job quota bucket"
    );
    let artifact_job_ids = inner
        .artifact_v2_pending
        .values()
        .map(|pending| pending.job_backend_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        artifact_job_ids,
        vec![""],
        "only the unscoped system reservation reaches the artifact registry here"
    );
}

#[tokio::test]
async fn results_metadata_noop_requests_require_strict_identity() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    // The Results route guard validates typed identity before body extraction,
    // so malformed identity tokens cannot take the metadata no-op path.
    let loose_results_token = state
        .local_jwt(json!({
            "sub": "preloop-job-not-a-uuid",
            "scp": "Actions.Results:plan:not-a-uuid",
        }))
        .unwrap();

    for route in [
        "/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
        "/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(route)
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {loose_results_token}"),
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{route}");
    }
}

#[tokio::test]
async fn twirp_metadata_routes_persist_log_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let requests = [
        (
            "/twirp/results.services.receiver.Receiver/CreateStepSummaryMetadata",
            json!({
                "step_backend_id": "step-summary",
                "workflow_job_run_backend_id": "job-1",
                "workflow_run_backend_id": "run-1",
                "size": 321,
            }),
            "results:run-1:job-1:summary:step-summary",
        ),
        (
            "/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
            json!({"step_backend_id": "step-logs", "line_count": 7}),
            "step:step-logs",
        ),
        (
            "/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata",
            json!({"workflow_job_run_backend_id": "job-logs", "line_count": 9}),
            "job:job-logs",
        ),
    ];

    for (uri, body, _) in requests {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(uri)
                    .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        let payload: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(payload["ok"], true, "{uri}");
    }

    let inner = state.inner.lock().await;
    let summary = inner
        .log_metadata
        .get("results:run-1:job-1:summary:step-summary")
        .unwrap();
    assert_eq!(summary.byte_count, 321);
    assert_eq!(summary.line_count, 0);
    // Requests without plan/job identifiers succeed without writing: there is
    // nothing to key or authorize against.
    assert!(!inner.log_metadata.contains_key("step:step-logs"));
    assert!(!inner.log_metadata.contains_key("job:job-logs"));
}

#[tokio::test]
async fn job_results_metadata_cannot_overwrite_another_job_namespace() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let plan_a = uuid::Uuid::new_v4().to_string();
    let plan_b = uuid::Uuid::new_v4().to_string();
    let job_a = uuid::Uuid::new_v4();
    let job_b = uuid::Uuid::new_v4();
    let token_a = state.mint_runtime_token(&plan_a, &job_a);
    let shared_step = "shared-step";
    let keys = [
        format!("summary:{shared_step}"),
        format!("step:{shared_step}"),
        format!("job:{job_b}"),
    ];
    {
        let mut inner = state.inner.lock().await;
        for (index, key) in keys.iter().enumerate() {
            inner.log_metadata.insert(
                key.clone(),
                LogMetadata {
                    byte_count: index + 1,
                    line_count: index + 10,
                },
            );
        }
    }

    let requests = [
        (
            "/twirp/results.services.receiver.Receiver/CreateStepSummaryMetadata",
            json!({
                "step_backend_id": shared_step,
                "workflow_job_run_backend_id": job_b,
                "workflow_run_backend_id": plan_b,
                "size": 999
            }),
        ),
        (
            "/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
            json!({
                "step_backend_id": shared_step,
                "workflow_job_run_backend_id": job_b,
                "workflow_run_backend_id": plan_b,
                "line_count": 999
            }),
        ),
        (
            "/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata",
            json!({
                "workflow_job_run_backend_id": job_b,
                "workflow_run_backend_id": plan_b,
                "line_count": 999
            }),
        ),
    ];
    for (uri, body) in requests {
        assert_eq!(
            status_with_bearer(&app, &token_a, Method::POST, uri, body).await,
            StatusCode::FORBIDDEN,
            "{uri}"
        );
    }

    let inner = state.inner.lock().await;
    assert_eq!(
        keys.iter()
            .map(|key| {
                inner
                    .log_metadata
                    .get(key)
                    .map(|meta| (meta.byte_count, meta.line_count))
            })
            .collect::<Vec<_>>(),
        vec![Some((1, 10)), Some((2, 11)), Some((3, 12))]
    );
    assert_eq!(inner.log_metadata.len(), keys.len());
}

#[tokio::test]
async fn job_results_token_cannot_update_another_jobs_steps() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (_, jobs) = two_job_run_for_log_filters(&app, &state).await;
    let (_, plan_id, own_agent_job_id) = jobs[0].clone();
    let (_, _, other_agent_job_id) = jobs[1].clone();
    let own_job_id = uuid::Uuid::parse_str(&own_agent_job_id).unwrap();
    let token = state.mint_runtime_token(&plan_id, &own_job_id);
    let uri = "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate";
    let update = |job_id: &str, external_id: &str| {
        json!({
            "workflow_run_backend_id": plan_id,
            "workflow_job_run_backend_id": job_id,
            "steps": [{
                "external_id": external_id,
                "number": 1,
                "name": "Run echo",
                "status": 6,
                "conclusion": 2
            }]
        })
    };

    assert_eq!(
        status_with_bearer(
            &app,
            &token,
            Method::POST,
            uri,
            update(&own_agent_job_id, "own-step")
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(
        status_with_bearer(
            &app,
            &token,
            Method::POST,
            uri,
            update(&other_agent_job_id, "other-step")
        )
        .await,
        StatusCode::FORBIDDEN
    );

    let inner = state.inner.lock().await;
    assert!(inner
        .job_steps
        .get(&own_job_id)
        .is_some_and(|steps| steps.iter().any(|step| step.id == "own-step")));
    assert!(!inner
        .job_steps
        .get(&own_job_id)
        .is_some_and(|steps| steps.iter().any(|step| step.id == "other-step")));
    let other_job_id = uuid::Uuid::parse_str(&other_agent_job_id).unwrap();
    assert!(!inner
        .job_steps
        .get(&other_job_id)
        .is_some_and(|steps| steps.iter().any(|step| step.id == "other-step")));
}

#[tokio::test]
async fn twirp_step_metadata_rejects_missing_step_backend_id() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let requests = [
        (
            "/twirp/results.services.receiver.Receiver/CreateStepSummaryMetadata",
            json!({
                "workflow_job_run_backend_id": "job-1",
                "workflow_run_backend_id": "run-1",
                "size": 321,
            }),
        ),
        (
            "/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
            json!({
                "workflow_job_run_backend_id": "job-1",
                "workflow_run_backend_id": "run-1",
                "line_count": 7,
            }),
        ),
    ];

    for (uri, body) in requests {
        let (status, payload) = request_json_status(&app, Method::POST, uri, body).await;
        assert!(status.is_client_error(), "{uri} returned {status}");
        assert!(
            !matches!(payload.get("ok"), Some(Value::Bool(true))),
            "{uri} must not report success"
        );
    }

    assert!(
        state.inner.lock().await.log_metadata.is_empty(),
        "missing step ids must not create metadata"
    );
}

#[tokio::test]
async fn twirp_diag_route_rejects_runner_listen_scope() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let plan_id = uuid::Uuid::new_v4().to_string();
    let job_id = uuid::Uuid::new_v4();
    let runner_listen_token = state
        .local_jwt(json!({
            "sub": "preloop-runner-listen-1",
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {runner_listen_token}"),
                )
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "workflow_run_backend_id": plan_id,
                        "workflow_job_run_backend_id": job_id.to_string(),
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn twirp_diag_route_issues_random_blob_url_and_accepts_bearerless_upload() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let plan_id = uuid::Uuid::new_v4().to_string();
    let job_id = uuid::Uuid::new_v4();
    let runtime_token = state.mint_runtime_token(&plan_id, &job_id);
    // R1-10: URL-minting writes require a live job record.
    r1_10_register_live_job(&state, job_id, &plan_id).await;

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
                        "workflow_job_run_backend_id": job_id.to_string(),
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
    assert_eq!(payload["blob_storage_type"], "BLOB_STORAGE_TYPE_AZURE");

    let diag_url = payload["diag_logs_url"].as_str().unwrap();
    assert!(!diag_url.is_empty());
    let (_, blob_token) = diag_url
        .split_once("/twirp-blob/diag/")
        .expect("diagnostic URL must use the bearerless diag blob endpoint");
    let (blob_token_clean, _) = blob_token.split_once('?').unwrap_or((blob_token, ""));
    // The blob token is a server-signed JWT: three base64url segments.
    assert_eq!(
        blob_token_clean.split('.').count(),
        3,
        "diagnostic token must be a signed JWT"
    );

    let bytes = b"diagnostic log bytes";
    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(diag_url)
                .body(Body::from(bytes.as_slice()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::CREATED);

    let downloaded = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(diag_url)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(downloaded.status(), StatusCode::OK);
    let downloaded_bytes = to_bytes(downloaded.into_body(), usize::MAX).await.unwrap();
    assert_eq!(downloaded_bytes.as_ref(), bytes);
}

/// Mint a blob JWT the way the signed-URL handlers do: `sub: preloop-blob`,
/// `kind`, `job` ("" for system), `jti` = staging dir name. Returns
/// `(jwt, jti)` — the pending-reservation maps key on `jti`.
#[tokio::test]
async fn blob_single_shot_streams_to_disk_and_roundtrips() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let job_id = uuid::Uuid::new_v4();
    let token = state.mint_runtime_token("plan-blob", &job_id);

    // R1-2: the blob gate requires a server-signed blob JWT whose `job`
    // claim matches the bearer's job on writes.
    let (blob_jwt, jti) = mint_blob_jwt(&state, "artifact", &job_id.to_string());
    {
        let mut inner = state.inner.lock().await;
        inner.artifact_v2_pending.insert(
            jti,
            crate::models::ArtifactV2Pending {
                registry_key: format!("run/{job_id}/single-shot"),
                job_backend_id: job_id.to_string(),
                created_unix: 0,
            },
        );
    }
    // R1-10: blob PUTs with a job bearer require a live job record.
    r1_10_register_live_job(&state, job_id, "plan-blob").await;

    // A 3 MiB single-shot upload is streamed to a temp file, never buffered
    // whole in memory, and must round-trip byte-for-byte.
    let payload = vec![b'z'; 3 * 1024 * 1024];
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("/twirp-blob/artifact/{blob_jwt}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(payload.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::CREATED);

    let get = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/twirp-blob/artifact/{blob_jwt}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let body = to_bytes(get.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.len(), payload.len());
    assert_eq!(body.as_ref(), payload.as_slice());
}

#[tokio::test]
async fn blob_blocklist_commits_are_serialized_and_survive_concurrency() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let job_id = uuid::Uuid::new_v4();
    let token = state.mint_runtime_token("plan-blob", &job_id);
    // R1-10: blob PUTs with a job bearer require a live job record.
    r1_10_register_live_job(&state, job_id, "plan-blob").await;
    let bearer = format!("Bearer {token}");

    // R1-2: the blob gate requires a server-signed blob JWT whose `job`
    // claim matches the bearer's job on writes.
    let (blob_jwt, jti) = mint_blob_jwt(&state, "artifact", &job_id.to_string());
    {
        let mut inner = state.inner.lock().await;
        inner.artifact_v2_pending.insert(
            jti,
            crate::models::ArtifactV2Pending {
                registry_key: format!("run/{job_id}/concurrent"),
                job_backend_id: job_id.to_string(),
                created_unix: 0,
            },
        );
    }
    let put_uri = format!("/twirp-blob/artifact/{blob_jwt}");

    // Stage two 1 MiB blocks (ids are base64-safe, so they survive
    // blockid_to_filename unchanged and match the commit XML verbatim).
    for (bid, byte) in [("YmxvY2sx", b'a'), ("YmxvY2sy", b'b')] {
        let chunk = vec![byte; 1024 * 1024];
        let staged = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri(format!("{put_uri}?comp=block&blockid={bid}"))
                    .header(header::AUTHORIZATION, bearer.clone())
                    .body(Body::from(chunk))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(staged.status(), StatusCode::CREATED);
    }

    let commit_xml =
        "<BlockList><Latest>YmxvY2sx</Latest><Latest>YmxvY2sy</Latest></BlockList>".to_string();
    let commit = |app: Router, bearer: String, xml: String, uri: String| {
        tokio::spawn(async move {
            app.oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri(format!("{uri}?comp=blocklist"))
                    .header(header::AUTHORIZATION, bearer)
                    .body(Body::from(xml))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
        })
    };

    // Two identical commits race. The per-(kind,token) lock serializes them,
    // and assembly goes through a temp file + atomic rename, so a losing/late
    // commit can never truncate or delete the blob the winner committed.
    let (s1, s2) = tokio::join!(
        commit(
            app.clone(),
            bearer.clone(),
            commit_xml.clone(),
            put_uri.clone()
        ),
        commit(
            app.clone(),
            bearer.clone(),
            commit_xml.clone(),
            put_uri.clone()
        ),
    );
    let (s1, s2) = (s1.unwrap(), s2.unwrap());
    assert!(
        s1 == StatusCode::CREATED || s2 == StatusCode::CREATED,
        "at least one concurrent commit must succeed (got {s1}, {s2})"
    );

    // The committed blob is exactly the two blocks concatenated — never a
    // truncated or missing `data` file from a clobbering concurrent commit.
    let get = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(put_uri)
                .header(header::AUTHORIZATION, bearer)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let body = to_bytes(get.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        body.len(),
        2 * 1024 * 1024,
        "assembled blob must contain both blocks, never truncated"
    );
    assert!(body[..1024 * 1024].iter().all(|&b| b == b'a'));
    assert!(body[1024 * 1024..].iter().all(|&b| b == b'b'));
}

#[tokio::test]
async fn blob_cache_block_upload_accepts_sdk_sized_blocks() {
    // Issue #292: @actions/cache v6 uploads archives larger than its
    // 128 MiB maxSingleShotSize as staged block blobs with 64 MiB blocks
    // (uploadChunkSize, concurrency 8). The server capped staged blocks at
    // 8 MiB -- the artifact client's chunk size -- so 256 MB+ cache uploads
    // died with 413 on the first block while 16/64 MB single-shot uploads
    // succeeded. SDK-sized (64 MiB) blocks must stage, commit, and
    // round-trip byte-for-byte.
    //
    // Two blocks (128 MiB total) rather than the SDK's typical four: the
    // commit assembles staged + output side by side, and the test tempdir
    // lives on /tmp (a 512M tmpfs here), so 256 MiB staged + 256 MiB
    // assembled would exhaust it for reasons unrelated to the fix.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let job_id = uuid::Uuid::new_v4();
    let token = state.mint_runtime_token("plan-blob", &job_id);
    // R1-10: blob PUTs with a job token require a live job record.
    r1_10_register_live_job(&state, job_id, "plan-blob").await;
    let auth_header = format!("Bearer {token}");
    let (blob_jwt, _jti) = mint_blob_jwt(&state, "cache", &job_id.to_string());
    let put_uri = format!("/twirp-blob/cache/{blob_jwt}");

    // 128 MiB in 2 x 64 MiB blocks, the block size the Azure SDK uses for
    // @actions/cache v6 (BlockBlobClient.uploadFile with uploadChunkSize).
    const BLOCK: usize = 64 * 1024 * 1024;
    let mut expected = Vec::with_capacity(2 * BLOCK);
    let mut xml = String::from("<BlockList>");
    for i in 0..2u8 {
        let bid = format!("YmxvY2st{i}"); // base64url-safe block id
        let chunk = vec![i; BLOCK];
        expected.extend_from_slice(&chunk);
        let staged = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri(format!("{put_uri}?comp=block&blockid={bid}"))
                    .header(header::AUTHORIZATION, auth_header.clone())
                    .header(header::CONTENT_LENGTH, BLOCK)
                    .body(Body::from(chunk))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            staged.status(),
            StatusCode::CREATED,
            "64 MiB staged block {i} must be accepted (issue #292)"
        );
        xml.push_str(&format!("<Latest>{bid}</Latest>"));
    }
    xml.push_str("</BlockList>");

    let commit = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("{put_uri}?comp=blocklist"))
                .header(header::AUTHORIZATION, auth_header.clone())
                .body(Body::from(xml))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(commit.status(), StatusCode::CREATED);

    let get = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(put_uri)
                .header(header::AUTHORIZATION, auth_header)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let body = to_bytes(get.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), expected.as_slice());
}

#[tokio::test]
async fn blob_block_upload_still_rejects_blocks_over_cap() {
    // F5 is preserved: blocks larger than the per-block cap are rejected
    // with 413 via the early Content-Length check.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let job_id = uuid::Uuid::new_v4();
    let token = state.mint_runtime_token("plan-blob", &job_id);
    r1_10_register_live_job(&state, job_id, "plan-blob").await;
    let bearer = format!("Bearer {token}");
    let (blob_jwt, _jti) = mint_blob_jwt(&state, "cache", &job_id.to_string());

    // One byte over the per-block cap: rejected before reading the body.
    let over_cap = memory_caps::MAX_BLOCK_BYTES + 1;
    let rejected = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!(
                    "/twirp-blob/cache/{blob_jwt}?comp=block&blockid=b3ZlcnNpemVk"
                ))
                .header(header::AUTHORIZATION, bearer)
                .header(header::CONTENT_LENGTH, over_cap)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn native_api_rejects_job_runtime_token() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let token = state.mint_runtime_token("plan", &uuid::Uuid::new_v4());
    let app = app(state, CancellationToken::new());

    let rejected = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/runs")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);

    let accepted = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/runs")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(accepted.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn runner_server_v1_sensitive_routes_require_bearer() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    // These /runner/server/_apis/v1/* aliases were previously placed on
    // the public router, letting unauthenticated callers mutate timelines,
    // inject logs, and finish jobs. They MUST require a bearer token.
    let cases = [
        (Method::PATCH, "/runner/server/_apis/v1/Timeline/s/h/p/t"),
        (Method::POST, "/runner/server/_apis/v1/Logfiles/s/h/p/l"),
        (Method::POST, "/runner/server/_apis/v1/Logfiles/s/h/p/l"),
        (
            Method::POST,
            "/runner/server/_apis/v1/TimeLineWebConsoleLog/s/h/p/t/r",
        ),
        (Method::POST, "/runner/server/_apis/v1/FinishJob/s/h/p"),
        (
            Method::POST,
            "/runner/server/_apis/v1/ActionDownloadInfo/s/h/p",
        ),
        (Method::GET, "/ws/live-logs/test-job"),
        (
            Method::GET,
            "/api/v1/runs/00000000-0000-0000-0000-000000000000/jobs/test/logs/live",
        ),
    ];
    for (method, uri) in cases {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method.clone())
                    .uri(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {uri} should require bearer auth"
        );
    }
}
/// SEC-01. Runner/session/agent administration must reject a job's
/// `ACTIONS_RUNTIME_TOKEN` (arbitrary workflow code holds it) while still
/// serving the credential the runner itself owns: the official runner deletes
/// its own session on shutdown and deregisters its own agent on clean exit
/// through these very routes, so the guard cannot be system-token-only.

/// SEC-01. Runner/session/agent administration must reject a job's
/// `ACTIONS_RUNTIME_TOKEN` (arbitrary workflow code holds it) while still
/// serving the credential the runner itself owns: the official runner deletes
/// its own session on shutdown and deregisters its own agent on clean exit
/// through these very routes, so the guard cannot be system-token-only.
#[tokio::test]
async fn admin_deletes_reject_job_tokens_and_confine_runners_to_themselves() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let runtime_token = state.mint_runtime_token("plan-delete-auth", &uuid::Uuid::new_v4());
    let app = app(state, CancellationToken::new());

    for uri in [
        "/runner/server/_apis/distributedtask/pools/1/agents/101",
        "/_apis/distributedtask/pools/1/agents/101",
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        "/_apis/distributedtask/pools/1/sessions",
        "/_apis/v1/AgentSession/1/session-1",
        "/runner/server/_apis/v1/AgentSession/1/session-1",
        "/acme/_apis/v1/AgentSession/1/session-1",
    ] {
        let status =
            status_with_bearer(&app, &runtime_token, Method::DELETE, uri, Value::Null).await;
        assert!(
            matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN),
            "{uri} should reject a runtime token, got {status}"
        );
    }

    let (status, body) = try_req(
        &app,
        Method::DELETE,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(body, Value::Null);

    let (runner_a, token_a) =
        register_runner_with_token(&app, "runner-a", &["self-hosted"], None).await;
    let (runner_b, token_b) =
        register_runner_with_token(&app, "runner-b", &["self-hosted"], None).await;
    let (status, session_a) = create_disttask_session(&app, &token_a, runner_a).await;
    assert_eq!(status, StatusCode::CREATED);
    let session_a = session_a["sessionId"].as_str().unwrap().to_owned();

    // Runner B must not end runner A's session: that strands A's in-flight
    // job until the lease reaper notices.
    let uri = format!("/runner/server/_apis/distributedtask/pools/1/sessions/{session_a}");
    assert_eq!(
        status_with_bearer(&app, &token_b, Method::DELETE, &uri, Value::Null).await,
        StatusCode::FORBIDDEN,
        "a runner must not delete another runner's session"
    );
    // Its owner may, and that is the runner's normal shutdown path.
    assert_eq!(
        status_with_bearer(&app, &token_a, Method::DELETE, &uri, Value::Null).await,
        StatusCode::NO_CONTENT,
        "a runner must be able to end its own session"
    );

    // Deregistration is likewise self-only: purging another runner revokes
    // its listen tokens and requeues its work.
    let foreign = format!("/runner/server/_apis/distributedtask/pools/1/agents/{runner_b}");
    assert_eq!(
        status_with_bearer(&app, &token_a, Method::DELETE, &foreign, Value::Null).await,
        StatusCode::FORBIDDEN,
        "a runner must not deregister another runner"
    );
    let own = format!("/runner/server/_apis/distributedtask/pools/1/agents/{runner_a}");
    assert_eq!(
        status_with_bearer(&app, &token_a, Method::DELETE, &own, Value::Null).await,
        StatusCode::NO_CONTENT,
        "a runner must be able to deregister itself on clean exit"
    );
    // Purging the identity revokes the listen token it was good for.
    assert_eq!(
        status_with_bearer(&app, &token_a, Method::DELETE, &own, Value::Null).await,
        StatusCode::UNAUTHORIZED,
        "a listen token must not outlive its runner registration"
    );
}

#[tokio::test]
async fn artifact_v2_ownership_is_enforced_by_runtime_token_scope() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let artifact_name = "owner-artifact";
    let artifact_bytes = b"artifact bytes from owner".to_vec();
    let artifact_size = artifact_bytes.len().to_string();

    let owner_run = submit_yaml(&app, workflow, "owner/repo").await;
    let owner_run_id = owner_run["run_id"].as_str().unwrap().to_owned();
    let (owner_plan_id, owner_job_id, owner_token) = {
        let inner = state.inner.lock().await;
        let message = queued_message_for(&inner, &owner_run_id);
        (
            message.plan.plan_id.clone(),
            message.job_id.to_string(),
            state.mint_runtime_token(&message.plan.plan_id, &message.job_id),
        )
    };

    let foreign_run = submit_yaml(&app, workflow, "owner/repo").await;
    let foreign_run_id = foreign_run["run_id"].as_str().unwrap().to_owned();
    let foreign_token = {
        let inner = state.inner.lock().await;
        let message = queued_message_for(&inner, &foreign_run_id);
        state.mint_runtime_token(&message.plan.plan_id, &message.job_id)
    };

    let create_request = json!({
        "workflow_run_backend_id": owner_plan_id.clone(),
        "workflow_job_run_backend_id": owner_job_id.clone(),
        "name": artifact_name,
    });
    let created = request_json_with_bearer(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact",
        create_request,
        &owner_token,
    )
    .await;
    assert_eq!(created["ok"], true);
    let upload_url = created["signed_upload_url"].as_str().unwrap().to_owned();
    assert!(!upload_url.is_empty());

    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(upload_url)
                .body(Body::from(artifact_bytes.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::CREATED);

    let finalize_request = json!({
        "workflow_run_backend_id": owner_plan_id.clone(),
        "workflow_job_run_backend_id": owner_job_id.clone(),
        "name": artifact_name,
        "size": artifact_size.clone(),
    });
    let denied_finalize = status_with_bearer(
        &app,
        &foreign_token,
        Method::POST,
        "/twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact",
        finalize_request.clone(),
    )
    .await;
    assert_eq!(denied_finalize, StatusCode::FORBIDDEN);
    let artifact_key = artifact_v2_registry_key(&owner_run_id, artifact_name);
    {
        let inner = state.inner.lock().await;
        assert!(inner
            .artifact_v2_pending
            .values()
            .any(|pending| pending.registry_key == artifact_key));
        assert!(!inner.artifact_v2_registry.contains_key(&artifact_key));
    }

    let finalized = request_json_with_bearer(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact",
        finalize_request.clone(),
        &owner_token,
    )
    .await;
    let artifact_id = finalized["artifact_id"].as_str().unwrap().to_owned();
    assert_eq!(finalized["ok"], true);
    assert_eq!(artifact_id, "1");
    {
        let inner = state.inner.lock().await;
        let entry = inner
            .artifact_v2_registry
            .get(&artifact_key)
            .expect("owner artifact must be registered");
        assert_eq!(entry.id, 1);
        assert_eq!(entry.workflow_run_backend_id, owner_plan_id);
        assert_eq!(entry.workflow_job_run_backend_id, owner_job_id);
        assert_eq!(entry.name, artifact_name);
        assert_eq!(entry.size, artifact_bytes.len() as u64);
        assert_eq!(entry.digest, None);
        assert!(inner.artifact_v2_pending.is_empty());
    }

    let list_request = json!({
        "workflow_run_backend_id": owner_plan_id.clone(),
        "workflow_job_run_backend_id": owner_job_id.clone(),
    });
    let listed = request_json_with_bearer(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.ArtifactService/ListArtifacts",
        list_request.clone(),
        &owner_token,
    )
    .await;
    let artifacts = listed["artifacts"].as_array().unwrap();
    assert_eq!(artifacts.len(), 1);
    let artifact = &artifacts[0];
    assert_eq!(artifact["database_id"].as_str(), Some(artifact_id.as_str()));
    assert_eq!(
        artifact["workflow_run_backend_id"].as_str(),
        Some(owner_plan_id.as_str())
    );
    assert_eq!(
        artifact["workflow_job_run_backend_id"].as_str(),
        Some(owner_job_id.as_str())
    );
    assert_eq!(artifact["name"].as_str(), Some(artifact_name));
    assert_eq!(artifact["size"].as_str(), Some(artifact_size.as_str()));

    let signed_request = json!({
        "workflow_run_backend_id": owner_plan_id.clone(),
        "workflow_job_run_backend_id": owner_job_id.clone(),
        "name": artifact_name,
    });
    let signed = request_json_with_bearer(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.ArtifactService/GetSignedArtifactURL",
        signed_request.clone(),
        &owner_token,
    )
    .await;
    let signed_url = signed["signed_url"].as_str().unwrap().to_owned();
    assert!(!signed_url.is_empty());

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
    let downloaded_bytes = to_bytes(downloaded.into_body(), usize::MAX).await.unwrap();
    assert_eq!(downloaded_bytes.as_ref(), artifact_bytes.as_slice());

    for (uri, body) in [
        (
            "/twirp/github.actions.results.api.v1.ArtifactService/ListArtifacts",
            list_request.clone(),
        ),
        (
            "/twirp/github.actions.results.api.v1.ArtifactService/GetSignedArtifactURL",
            signed_request.clone(),
        ),
        (
            "/twirp/github.actions.results.api.v1.ArtifactService/DeleteArtifact",
            signed_request.clone(),
        ),
    ] {
        let status = status_with_bearer(&app, &foreign_token, Method::POST, uri, body).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri}");
    }
    {
        let inner = state.inner.lock().await;
        assert!(inner.artifact_v2_registry.contains_key(&artifact_key));
    }

    let deleted = request_json_with_bearer(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.ArtifactService/DeleteArtifact",
        signed_request,
        &owner_token,
    )
    .await;
    assert_eq!(deleted["ok"], true);
    assert_eq!(deleted["artifact_id"].as_str(), Some(artifact_id.as_str()));
    {
        let inner = state.inner.lock().await;
        assert!(!inner.artifact_v2_registry.contains_key(&artifact_key));
    }
}

/// The system-token split must not swallow job-facing runner traffic. The
/// distributedtask message DELETE is paired with the GET on the same prefix;
/// dropping it (as an earlier cut of the split did) 404s every client that
/// polls messages there, since no other route serves that path.

/// The system-token split must not swallow job-facing runner traffic. The
/// distributedtask message DELETE is paired with the GET on the same prefix;
/// dropping it (as an earlier cut of the split did) 404s every client that
/// polls messages there, since no other route serves that path.
#[tokio::test]
async fn disttask_message_delete_stays_reachable_for_protocol_tokens() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (runner_id, listen_token) =
        register_runner_with_token(&app, "message-ack-runner", &["self-hosted"], None).await;
    let (_, session) = create_disttask_session(&app, &listen_token, runner_id).await;
    assert!(session.get("sessionId").and_then(Value::as_str).is_some());
    let session_id = session["sessionId"].as_str().unwrap();
    let status = status_with_bearer(
        &app,
        &listen_token,
        Method::DELETE,
        &format!("/runner/server/_apis/distributedtask/pools/1/messages/7?sessionId={session_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "message ack must stay on the distributedtask prefix under the runner protocol guard"
    );
}

#[tokio::test]
async fn listener_token_lifecycle_calls_require_runtime_token() {
    use std::sync::atomic::Ordering::Relaxed;

    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (runner_id, listen_token) =
        register_runner_with_token(&app, "runner-1", &["self-hosted"], None).await;

    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let run = submit_yaml(&app, workflow, "owner/repo").await;
    let run_id = run["run_id"].as_str().unwrap().to_owned();

    let session = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({
            "agent": {"id": runner_id, "name": "runner-1"},
            "ownerName": "owner",
            "sessionId": "00000000-0000-0000-0000-000000000000",
            "useFipsEncryption": false
        }),
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap().to_owned();

    let (plan_id, agent_job_id) = {
        let inner = state.inner.lock().await;
        let message = queued_message_for(&inner, &run_id);
        (message.plan.plan_id.clone(), message.job_id)
    };
    let runtime_token = state.mint_runtime_token(&plan_id, &agent_job_id);

    // Claim the job so renew/complete resolve an owned request.
    let claimed = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&waitSeconds=0"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {listen_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(claimed.status(), StatusCode::OK);

    let acquire = json!({
        "jobMessageId": agent_job_id.to_string(),
        "billingOwnerId": "local",
        "runnerOS": "macOS",
    });
    assert_eq!(
        status_with_bearer(
            &app,
            &runtime_token,
            Method::POST,
            &format!("/broker/{runner_id}/acquirejob"),
            acquire.clone(),
        )
        .await,
        StatusCode::FORBIDDEN,
        "a job runtime token must not claim through acquirejob"
    );
    let acquired = request_json_with_bearer(
        &app,
        Method::POST,
        &format!("/broker/{runner_id}/acquirejob"),
        acquire,
        &listen_token,
    )
    .await;
    assert!(acquired["jobId"].is_string());

    // acquirejob is the Listener's own call: baseline, never the gate.
    assert_eq!(state.listener_token_acquire_calls.load(Relaxed), 1);
    assert_eq!(
        state.listener_token_lifecycle_calls.load(Relaxed),
        0,
        "claims and acquirejob must not register on the fencing gate"
    );

    let renew = json!({"jobId": agent_job_id.to_string(), "planId": plan_id});

    // The job runtime token is what Plan 004's fencing carrier rides.
    let renewed = request_json_with_bearer(
        &app,
        Method::POST,
        &format!("/broker/{runner_id}/renewjob"),
        renew.clone(),
        &runtime_token,
    )
    .await;
    assert!(renewed["lockedUntil"].is_string());
    assert_eq!(
        state.listener_token_lifecycle_calls.load(Relaxed),
        0,
        "a renew on the job runtime token is the official runner path"
    );

    // The bare listen token reaches the route but is fenced before any job
    // state is read or mutated. Keep the probe counter so dogfood can expose
    // a protocol regression without granting the credential lifecycle power.
    let rejected_renew = status_with_bearer(
        &app,
        &listen_token,
        Method::POST,
        &format!("/broker/{runner_id}/renewjob"),
        renew.clone(),
    )
    .await;
    assert_eq!(rejected_renew, StatusCode::FORBIDDEN);

    let rejected_complete = status_with_bearer(
        &app,
        &listen_token,
        Method::POST,
        &format!("/broker/{runner_id}/completejob"),
        json!({"jobId": agent_job_id.to_string(), "planId": plan_id, "conclusion": "succeeded"}),
    )
    .await;
    assert_eq!(rejected_complete, StatusCode::FORBIDDEN);
    assert_eq!(state.listener_token_lifecycle_calls.load(Relaxed), 2);

    // Both rejected calls leave the attempt usable with its job runtime token.
    let renewed = request_json_with_bearer(
        &app,
        Method::POST,
        &format!("/broker/{runner_id}/renewjob"),
        renew,
        &runtime_token,
    )
    .await;
    assert!(renewed["lockedUntil"].is_string());
    let completed = status_with_bearer(
        &app,
        &runtime_token,
        Method::POST,
        &format!("/broker/{runner_id}/completejob"),
        json!({"jobId": agent_job_id.to_string(), "planId": plan_id, "conclusion": "succeeded"}),
    )
    .await;
    assert_eq!(completed, StatusCode::NO_CONTENT);
    assert_eq!(
        state.listener_token_acquire_calls.load(Relaxed),
        1,
        "the lifecycle counter must count only rejected listener-token calls"
    );
}

#[tokio::test]
async fn legacy_runner_aliases_require_registration_and_bound_runner_credentials() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let socket_app = app
        .clone()
        .layer(middleware::from_fn(crate::auth::runner_surface_only));
    let prefixes = ["/runner/server/_apis/v1", "/_apis/v1", "/contoso/_apis/v1"];

    // The TCP router and the guest-mounted socket must reject the complete
    // legacy lifecycle surface before it can create or address an identity.
    for surface in [&app, &socket_app] {
        for prefix in prefixes {
            for (method, suffix, body) in [
                (Method::GET, "/Agent/1/0", Value::Null),
                (
                    Method::POST,
                    "/Agent/1/0",
                    json!({"name": "unauthorized", "labels": []}),
                ),
                (
                    Method::POST,
                    "/AgentSession/1/session",
                    json!({"agent": {"id": 1, "name": "unauthorized"}}),
                ),
                (
                    Method::GET,
                    "/Message/1?sessionId=session&waitSeconds=0",
                    Value::Null,
                ),
                (Method::GET, "/AgentRequest/1/1", Value::Null),
                (Method::PATCH, "/Timeline/s/h/p/t", json!({})),
                (Method::POST, "/Logfiles/s/h/p", json!({})),
                (Method::POST, "/Logfiles/s/h/p/l", json!({})),
                (Method::POST, "/TimeLineWebConsoleLog/s/h/p/t/r", json!({})),
                (Method::POST, "/FinishJob/s/h/p", json!({})),
                (Method::POST, "/ActionDownloadInfo/s/h/p", json!({})),
            ] {
                let uri = format!("{prefix}{suffix}");
                assert_eq!(
                    request_status_without_bearer(surface, method.clone(), &uri, body).await,
                    StatusCode::UNAUTHORIZED,
                    "{method} {uri} must reject an unauthenticated caller"
                );
            }
        }
    }
    // The configure client also uses the distributedtask registration alias.
    // It must enforce the same boundary on TCP and the guest socket.
    for surface in [&app, &socket_app] {
        assert_eq!(
            request_status_without_bearer(
                surface,
                Method::POST,
                "/runner/server/_apis/distributedtask/pools/1/agents",
                json!({"name": "unauthorized", "labels": []}),
            )
            .await,
            StatusCode::UNAUTHORIZED,
            "distributedtask registration must reject an unauthenticated caller"
        );
    }

    // The GitHub-compatible endpoint returns a narrowly scoped local
    // RunnerManage credential. That credential is valid for all registration
    // aliases, unlike an arbitrary locally signed JWT.
    let registration = request_json_with_bearer(
        &app,
        Method::POST,
        "/api/v3/actions/runner-registration",
        json!({"url": "https://github.com/acme/repo"}),
        DEFAULT_PRELOOP_SYSTEM_TOKEN,
    )
    .await;
    let manage = registration["token"].as_str().unwrap();
    let registration_paths = [
        (
            "/runner/server/_apis/distributedtask/pools/1/agents",
            "distributedtask",
        ),
        ("/runner/server/_apis/v1/Agent/1/0", "runner-server"),
        ("/_apis/v1/Agent/1/0", "root"),
        ("/contoso/_apis/v1/Agent/1/0", "org"),
    ];
    let mut registered = Vec::new();
    for (path, name) in registration_paths {
        let body = json!({"name": name, "labels": ["self-hosted"]});
        let response = request_json_with_bearer(&app, Method::POST, path, body, manage).await;
        registered.push((
            response["id"].as_i64().unwrap(),
            response["authorization"]["clientId"]
                .as_str()
                .unwrap()
                .to_owned(),
        ));
    }

    // A pool-issued one-time credential is the alternate host-side
    // registration path. It is consumed by the handler, not reusable.
    stage_provision_token(&state, "one-time-provision");
    let provisioned = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/runner/server/_apis/distributedtask/pools/1/agents")
                .header("x-preloop-provision-token", "one-time-provision")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"name": "provisioned", "labels": []}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(provisioned.status(), StatusCode::OK);
    assert!(
        state
            .pending_registrations
            .read()
            .map(|pending| pending.is_empty())
            .unwrap_or(false),
        "the provision credential must be single-use"
    );

    // Knowing a client id is not an OAuth credential. The JSON compatibility
    // flow is intentionally limited to the trusted local control-plane token;
    // production runners use the signed client_assertion form.
    let oauth_body = json!({
        "grant_type": "client_credentials",
        "client_id": registered[0].1.clone(),
        "client_secret": "unused"
    });
    assert_eq!(
        request_status_without_bearer(
            &app,
            Method::POST,
            "/runner/server/_apis/v1/oauth2/token",
            oauth_body.clone(),
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    let oauth_a = request_json_with_bearer(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/oauth2/token",
        oauth_body,
        DEFAULT_PRELOOP_SYSTEM_TOKEN,
    )
    .await;
    let token_a = oauth_a["access_token"].as_str().unwrap().to_owned();

    assert_eq!(
        request_status_with_bearer(
            &app,
            Method::POST,
            "/_apis/v1/Agent/1/0",
            json!({"name": "listen-token-registration", "labels": []}),
            &token_a,
        )
        .await,
        StatusCode::UNAUTHORIZED,
        "a RunnerListen token must not authorize a new registration"
    );
    assert_eq!(
        request_status_with_bearer(
            &app,
            Method::POST,
            "/runner/server/_apis/distributedtask/pools/1/agents",
            json!({"name": "listen-token-registration", "labels": []}),
            &token_a,
        )
        .await,
        StatusCode::UNAUTHORIZED,
        "a RunnerListen token must not authorize distributedtask registration"
    );
    let oauth_b = request_json_with_bearer(
        &app,
        Method::POST,
        "/_apis/v1/oauth2/token",
        json!({
            "grant_type": "client_credentials",
            "client_id": registered[1].1.clone(),
            "client_secret": "unused"
        }),
        DEFAULT_PRELOOP_SYSTEM_TOKEN,
    )
    .await;
    let token_b = oauth_b["access_token"].as_str().unwrap().to_owned();

    assert_eq!(
        request_status_with_bearer(
            &app,
            Method::DELETE,
            &format!(
                "/runner/server/_apis/distributedtask/pools/1/agents/{}",
                registered[0].0
            ),
            Value::Null,
            &token_b,
        )
        .await,
        StatusCode::FORBIDDEN,
        "runner B must not delete runner A"
    );

    // Session creation binds the stored owner to the verified listen token,
    // not to a caller-controlled agent.id field.
    assert_eq!(
        request_status_with_bearer(
            &app,
            Method::POST,
            "/runner/server/_apis/v1/AgentSession/1/session-cross",
            json!({"agent": {"id": registered[1].0, "name": "wrong"}}),
            &token_a,
        )
        .await,
        StatusCode::FORBIDDEN
    );
    let session_a = request_json_with_bearer(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/AgentSession/1/session-a",
        json!({"agent": {"id": registered[0].0, "name": "runner-a"}}),
        &token_a,
    )
    .await;
    let session_a_id = session_a["sessionId"].as_str().unwrap().to_owned();
    let _session_b = request_json_with_bearer(
        &app,
        Method::POST,
        "/contoso/_apis/v1/AgentSession/1/session-b",
        json!({"agent": {"id": registered[1].0, "name": "runner-b"}}),
        &token_b,
    )
    .await;

    // A valid runner credential still cannot poll another runner's session,
    // including when that session already has an inflight message.
    assert_eq!(
        request_status_with_bearer(
            &app,
            Method::GET,
            &format!("/_apis/v1/Message/1?sessionId={session_a_id}&waitSeconds=0"),
            Value::Null,
            &token_b,
        )
        .await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request_status_with_bearer(
            &app,
            Method::GET,
            &format!("/runner/server/_apis/v1/Message/1?sessionId={session_a_id}&waitSeconds=0"),
            Value::Null,
            &token_a,
        )
        .await,
        StatusCode::OK
    );
}

#[tokio::test]
async fn legacy_agent_requests_are_bound_to_runner_identity() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let (runner_a, token_a) =
        register_runner_with_token(&app, "agent-request-a", &["self-hosted"], None).await;
    let (runner_b, token_b) =
        register_runner_with_token(&app, "agent-request-b", &["self-hosted"], None).await;
    let (session_status, session) = create_disttask_session(&app, &token_a, runner_a).await;
    assert_eq!(session_status, StatusCode::CREATED);
    let session_id = session["sessionId"].as_str().unwrap().to_owned();

    let _accepted = submit_simple_run(&app).await;
    let message = poll_message(&app, &token_a, &session_id).await;
    assert_eq!(
        message["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );
    let request_id = {
        let inner = state.inner.lock().await;
        *inner.session_active_requests.get(&session_id).unwrap()
    };

    // Deleting a session can happen during listener recovery. The request
    // owner must survive that lifecycle event so the original runner can
    // still finish its in-flight job.
    assert_eq!(
        request_status_with_bearer(
            &app,
            Method::DELETE,
            &format!("/runner/server/_apis/v1/AgentSession/1/{session_id}"),
            Value::Null,
            &token_a,
        )
        .await,
        StatusCode::NO_CONTENT
    );

    for (method, body) in [
        (Method::GET, Value::Null),
        (Method::POST, Value::Null),
        (Method::PATCH, json!({"result": "failed"})),
    ] {
        assert_eq!(
            request_status_with_bearer(
                &app,
                method.clone(),
                &format!("/runner/server/_apis/v1/AgentRequest/1/{request_id}"),
                body,
                &token_b,
            )
            .await,
            StatusCode::FORBIDDEN,
            "runner B must not address runner A's agent request with {method}"
        );
    }

    assert_eq!(
        request_status_with_bearer(
            &app,
            Method::PATCH,
            &format!("/runner/server/_apis/v1/AgentRequest/1/{request_id}"),
            json!({"result": "succeeded"}),
            &token_a,
        )
        .await,
        StatusCode::OK
    );

    // Completion does not make the request readable or writable by another
    // registered runner; the owner is retained on the request record.
    for (method, body) in [
        (Method::GET, Value::Null),
        (Method::PATCH, json!({"result": "failed"})),
    ] {
        assert_eq!(
            request_status_with_bearer(
                &app,
                method.clone(),
                &format!("/runner/server/_apis/v1/AgentRequest/1/{request_id}"),
                body,
                &token_b,
            )
            .await,
            StatusCode::FORBIDDEN,
            "runner B must not address completed runner A request with {method}"
        );
    }
    let inner = state.inner.lock().await;
    assert_eq!(
        inner.job_requests.get(&request_id).unwrap().result,
        Some(ExecutionStatus::Success)
    );
    let _ = runner_b;
}

#[tokio::test]
async fn legacy_provision_token_is_consumed_atomically() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    stage_provision_token(&state, "concurrent-provision");

    let make_request = || {
        Request::builder()
            .method(Method::POST)
            .uri("/_apis/v1/Agent/1/0")
            .header("x-preloop-provision-token", "concurrent-provision")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({"name": "concurrent", "labels": []}).to_string(),
            ))
            .unwrap()
    };
    let (first, second) = tokio::join!(
        app.clone().oneshot(make_request()),
        app.clone().oneshot(make_request())
    );
    let first = first.unwrap().status();
    let second = second.unwrap().status();

    assert!(
        [first, second].contains(&StatusCode::OK),
        "one concurrent registration must consume the token: {first}, {second}"
    );
    assert!(
        [first, second].contains(&StatusCode::UNAUTHORIZED),
        "the consumed token must reject the competing registration: {first}, {second}"
    );
    assert!(state
        .pending_registrations
        .read()
        .map(|pending| pending.is_empty())
        .unwrap_or(false));
}

#[tokio::test]
async fn oidc_endpoint_mints_rs256_jwt_with_requested_audience() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let workflow = json!({
        "workflow_yaml": "name: oidc-test\non: push\npermissions:\n  id-token: write\n  contents: read\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "event": "push",
        "repository": "owner/repo",
    });
    let resp = request_json(&app, Method::POST, "/api/v1/runs", workflow).await;
    let _run_id: RunId = resp["run_id"].as_str().unwrap().parse().unwrap();

    let (plan_id, agent_job_id) = {
        let inner = state.inner.lock().await;
        inner
            .queue
            .front()
            .or_else(|| inner.pending_jobs.front())
            .map(|j| (j.message.plan.plan_id.clone(), j.message.job_id))
            .unwrap()
    };

    let token = request_json(
            &app,
            Method::GET,
            &format!("/runner/server/_apis/distributedtask/hubs/actions/plans/{plan_id}/jobs/{agent_job_id}/oidctoken?audience=api://custom"),
            Value::Null,
        )
        .await;
    let jwt = token["value"].as_str().unwrap();
    let parts: Vec<&str> = jwt.split('.').collect();
    assert_eq!(parts.len(), 3);

    // Verify the protected header is RS256 with a retained kid.
    let header: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
    assert_eq!(header["alg"], "RS256");
    assert!(!header["kid"].as_str().unwrap_or_default().is_empty());

    // The JWT thumbprint must identify the same certificate published by JWKS.
    let jwt_x5t = header["x5t"].as_str().unwrap_or_default();
    assert!(!jwt_x5t.is_empty(), "JWT header must contain x5t");
    let jwks = request_json(&app, Method::GET, "/.well-known/jwks", Value::Null).await;
    let jwks_key = &jwks["keys"][0];
    let jwks_x5t = jwks_key["x5t"].as_str().unwrap_or_default();
    assert!(!jwks_x5t.is_empty(), "JWKS key must contain x5t");
    assert_eq!(jwt_x5t, jwks_x5t);

    let certificate_der = std::fs::read(temp.path().join("oidc-cert.der")).unwrap();
    assert!(
        !certificate_der.is_empty(),
        "OIDC certificate DER must be nonempty"
    );
    let expected_x5t = URL_SAFE_NO_PAD.encode(Sha1::digest(&certificate_der));
    assert_eq!(jwt_x5t, expected_x5t);
    assert_eq!(jwks_x5t, expected_x5t);

    // Verify claims.
    let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
    assert_eq!(claims["aud"], "api://custom");
    assert_eq!(claims["iss"], "http://127.0.0.1:9090/oidc");
    assert_eq!(claims["repository"], "owner/repo");
    assert_eq!(claims["repository_owner"], "owner");
    assert_eq!(claims["event_name"], "push");
    assert_eq!(claims["runner_environment"], "self-hosted");
    assert!(claims["sub"]
        .as_str()
        .unwrap()
        .starts_with("repo:owner/repo:"));
    assert!(claims["jti"].is_string());
    assert!(claims["exp"].as_u64().unwrap() > claims["iat"].as_u64().unwrap());

    // Verify the OIDC keypair is persisted.
    assert!(temp.path().join("oidc-key.json").exists());
}

#[tokio::test]
async fn results_surfaces_agree_on_alternate_uuid_scope_spelling() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\npermissions:\n  id-token: write\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps: [{ run: \"echo hi\" }]\n",
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;

    let (plan_id, agent_job_id) = {
        let inner = state.inner.lock().await;
        inner
            .queue
            .front()
            .map(|job| (job.message.plan.plan_id.clone(), job.message.job_id))
            .unwrap()
    };
    let alternate_scope_token = state
        .local_jwt(json!({
            "sub": format!("preloop-job-{agent_job_id}"),
            "scp": format!(
                "Actions.Results:{plan_id}:{}",
                agent_job_id.to_string().to_uppercase()
            ),
        }))
        .unwrap();

    // The trust-tier surface already accepts UUID spelling variants through
    // the canonical Results payload parser.
    assert_eq!(
        crate::events::trust_tier::fork_restricted_from_token(&state, &alternate_scope_token).await,
        Some(false)
    );

    let malformed_job_token = state
        .local_jwt(json!({
            "sub": "preloop-job-not-a-uuid",
            "scp": format!("Actions.Results:{plan_id}:{agent_job_id}"),
        }))
        .unwrap();
    assert_eq!(
        crate::events::trust_tier::fork_restricted_from_token(&state, &malformed_job_token).await,
        Some(true),
        "job-shaped malformed claims must keep cache writes fail-closed"
    );
    // Before centralization, the OIDC surface compared the raw scope string,
    // so this valid Results identity was rejected: the scope spells the job
    // UUID uppercase while the path uses the canonical lowercase spelling.
    // (Using the same spelling in both would compare equal pre-centralization
    // and prove nothing.)
    let oidc_status = status_with_bearer(
        &app,
        &alternate_scope_token,
        Method::GET,
        &format!(
            "/runner/server/_apis/distributedtask/hubs/actions/plans/{plan_id}/jobs/{agent_job_id}/oidctoken"
        ),
        Value::Null,
    )
    .await;
    assert_eq!(
        oidc_status,
        StatusCode::OK,
        "OIDC must accept the same typed identity as trust-tier checks"
    );

    let invalid_path_status = status_with_bearer(
        &app,
        &alternate_scope_token,
        Method::GET,
        &format!(
            "/runner/server/_apis/distributedtask/hubs/actions/plans/{plan_id}/jobs/not-a-uuid/oidctoken"
        ),
        Value::Null,
    )
    .await;
    assert_eq!(
        invalid_path_status,
        StatusCode::FORBIDDEN,
        "an invalid job path remains an authorization failure for a job token"
    );
}

#[tokio::test]
async fn oidc_default_audience_is_owner_url() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let workflow = json!({
        "workflow_yaml": "on: push\npermissions:\n  id-token: write\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps: [{ run: \"echo hi\" }]\n",
        "event": "push",
        "repository": "octo-org/octo-repo",
    });
    let resp = request_json(&app, Method::POST, "/api/v1/runs", workflow).await;
    let _run_id: RunId = resp["run_id"].as_str().unwrap().parse().unwrap();

    let (plan_id, agent_job_id) = {
        let inner = state.inner.lock().await;
        inner
            .queue
            .front()
            .or_else(|| inner.pending_jobs.front())
            .map(|j| (j.message.plan.plan_id.clone(), j.message.job_id))
            .unwrap()
    };

    let token = request_json(
            &app,
            Method::GET,
            &format!("/runner/server/_apis/distributedtask/hubs/actions/plans/{plan_id}/jobs/{agent_job_id}/oidctoken"),
            Value::Null,
        )
        .await;
    let jwt = token["value"].as_str().unwrap();
    let parts: Vec<&str> = jwt.split('.').collect();
    let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
    assert_eq!(claims["aud"], "https://github.com/octo-org");
}

#[tokio::test]
async fn oidc_forbidden_without_id_token_write() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let workflow = json!({
        "workflow_yaml": "on: push\npermissions:\n  contents: read\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps: [{ run: \"echo hi\" }]\n",
        "event": "push",
        "repository": "owner/repo",
    });
    let _resp = request_json(&app, Method::POST, "/api/v1/runs", workflow).await;

    let (plan_id, agent_job_id) = {
        let inner = state.inner.lock().await;
        inner
            .queue
            .front()
            .or_else(|| inner.pending_jobs.front())
            .map(|job| (job.message.plan.plan_id.clone(), job.message.job_id))
            .unwrap()
    };
    let runtime_token = state.mint_runtime_token(&plan_id, &agent_job_id);

    // Use the real job-bound runtime token so this reaches permission enforcement.
    let uri = format!(
            "/runner/server/_apis/distributedtask/hubs/actions/plans/{plan_id}/jobs/{agent_job_id}/oidctoken?audience=api://test"
        );
    let request = Request::builder()
        .method(Method::GET)
        .uri(&uri)
        .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn oidc_discovery_and_jwks_endpoints() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state, CancellationToken::new());

    let discovery = request_json(
        &app,
        Method::GET,
        "/.well-known/openid-configuration",
        Value::Null,
    )
    .await;
    assert_eq!(
        discovery["jwks_uri"],
        "http://127.0.0.1:9090/oidc/.well-known/jwks"
    );
    assert_eq!(discovery["issuer"], "http://127.0.0.1:9090/oidc");
    assert_eq!(
        discovery["subject_types_supported"],
        json!(["public", "pairwise"])
    );
    assert_eq!(discovery["scopes_supported"], json!(["openid"]));

    let namespaced = request_json(
        &app,
        Method::GET,
        "/oidc/.well-known/openid-configuration",
        Value::Null,
    )
    .await;
    assert_eq!(namespaced, discovery);

    let root_jwks = request_json(&app, Method::GET, "/.well-known/jwks", Value::Null).await;
    let root_json_jwks =
        request_json(&app, Method::GET, "/.well-known/jwks.json", Value::Null).await;
    let namespaced_jwks =
        request_json(&app, Method::GET, "/oidc/.well-known/jwks", Value::Null).await;
    let namespaced_json_jwks = request_json(
        &app,
        Method::GET,
        "/oidc/.well-known/jwks.json",
        Value::Null,
    )
    .await;
    assert_eq!(root_jwks, root_json_jwks);
    assert_eq!(namespaced_jwks, root_jwks);
    assert_eq!(namespaced_json_jwks, root_jwks);

    let keys = root_jwks["keys"].as_array().unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0]["kty"], "RSA");
    assert_eq!(keys[0]["alg"], "RS256");
    assert_eq!(keys[0]["use"], "sig");
    assert!(!keys[0]["kid"].as_str().unwrap_or_default().is_empty());
    assert!(keys[0]["n"].as_str().is_some_and(|value| !value.is_empty()));
    assert_eq!(keys[0]["e"], "AQAB");
}

#[tokio::test]
async fn oidc_keypair_persists_across_restarts() {
    let temp = tempfile::tempdir().unwrap();
    let state1 = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let (kid1, x5t1, certificate_der1) = {
        let inner = state1.inner.lock().await;
        let keypair = inner.oidc_keypair.as_ref().unwrap();
        (
            keypair.kid().to_string(),
            keypair.x5t().to_string(),
            keypair.certificate_der().to_vec(),
        )
    };
    let certificate_path = temp.path().join("oidc-cert.der");
    assert!(certificate_path.exists());
    assert!(!certificate_der1.is_empty());
    assert_eq!(certificate_der1, std::fs::read(&certificate_path).unwrap());
    let expected_x5t1 = URL_SAFE_NO_PAD.encode(Sha1::digest(&certificate_der1));
    assert_eq!(x5t1, expected_x5t1);
    drop(state1);
    // Second instance should load the same keypair and certificate.
    let state2 = AppState::new(temp.path().to_path_buf()).await.unwrap();

    let (kid2, x5t2, certificate_der2) = {
        let inner = state2.inner.lock().await;
        let keypair = inner.oidc_keypair.as_ref().unwrap();
        (
            keypair.kid().to_string(),
            keypair.x5t().to_string(),
            keypair.certificate_der().to_vec(),
        )
    };
    assert_eq!(
        kid1, kid2,
        "OIDC keypair kid must be stable across restarts"
    );
    assert_eq!(
        x5t1, x5t2,
        "OIDC certificate x5t must be stable across restarts"
    );
    assert_eq!(certificate_der1, certificate_der2);
    assert_eq!(certificate_der1, std::fs::read(&certificate_path).unwrap());
    assert_eq!(
        x5t2,
        URL_SAFE_NO_PAD.encode(Sha1::digest(&certificate_der2))
    );
}

#[tokio::test]
async fn oidc_malformed_certificate_sidecar_rejects_restart() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    drop(state);

    std::fs::write(temp.path().join("oidc-cert.der"), b"not a DER certificate").unwrap();
    assert!(
        AppState::new(temp.path().to_path_buf()).await.is_err(),
        "startup must reject a malformed persisted OIDC certificate"
    );
}

#[tokio::test]
async fn messages_redeliver_until_delete_ack() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo ok
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;

    let first = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    assert_eq!(first["messageId"], 1);

    let redelivered = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    assert_eq!(redelivered["messageId"], first["messageId"]);

    let ack = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/runner/server/_apis/v1/Message/1/1?sessionId=default")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ack.status(), StatusCode::NO_CONTENT);

    let empty = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert!(empty.is_null());
}

#[tokio::test]
async fn cancel_run_delivers_cancellation_message() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 30
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();

    let message = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    let message_id = message["messageId"].as_i64().unwrap();

    let ack = app
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
    assert_eq!(ack.status(), StatusCode::NO_CONTENT);

    request_json(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/cancel"),
        Value::Null,
    )
    .await;

    let cancellation = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_eq!(
        cancellation["messageType"],
        azdo::message_type::JOB_CANCELLED
    );
    // Body is base64 of plaintext (no session key in this test path).
    let body_b64 = cancellation["body"].as_str().unwrap();
    let body_bytes = BASE64_STANDARD.decode(body_b64).unwrap();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(body["jobId"]
        .as_str()
        .unwrap()
        .parse::<uuid::Uuid>()
        .is_ok());
    assert_eq!(body["timeout"], "00:05:00");
    assert!(body.get("runId").is_none());
}

#[tokio::test]
async fn cancel_run_completes_github_checks_and_terminal_metadata() {
    // Keyed by check-run id: `PRELOOP_GITHUB_API_URL` is process-global, and
    // `owner/repo` is the suite's default repository, so a co-scheduled test
    // that submits a run lands its own check-run PATCH on this stub. Counting
    // every request would make the assertion depend on the rest of the suite;
    // this test's contract is about the check run it pinned below.
    const CHECK_RUN_ID: u64 = 7;
    let check_completions = Arc::new(parking_lot::Mutex::new(
        Vec::<(u64, serde_json::Value)>::new(),
    ));
    let mock_app = Router::new().route(
        "/repos/owner/repo/check-runs/:id",
        axum::routing::patch({
            let check_completions = check_completions.clone();
            move |Path(id): Path<u64>, body: axum::extract::Json<Value>| {
                let check_completions = check_completions.clone();
                async move {
                    check_completions.lock().push((id, body.0));
                    Json(json!({"id": id}))
                }
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, mock_app).await.unwrap();
    });

    // `TestEnvVar` restores both on drop, so a panicking assertion below
    // cannot leak this stub's address (or its token) onto the rest of the run.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _api_url =
        crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", format!("http://127.0.0.1:{port}"));
    let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "cancel-test-token");

    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    {
        let mut inner = state.inner.lock().await;
        let run = inner.runs.get_mut(&run_id).unwrap();
        run.job_check_run_ids
            .insert(JobId("build".into()), CHECK_RUN_ID);
    }

    let cancelled = request_json(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/cancel"),
        Value::Null,
    )
    .await;

    assert_eq!(cancelled["status"], "cancelled");
    assert_eq!(cancelled["conclusion"], "cancelled");
    assert!(
        cancelled["completed_at"].is_string(),
        "cancelled run must carry terminal completion metadata"
    );
    let completions = check_completions.lock();
    let mine: Vec<&serde_json::Value> = completions
        .iter()
        .filter(|(id, _)| *id == CHECK_RUN_ID)
        .map(|(_, body)| body)
        .collect();
    assert_eq!(mine.len(), 1, "one cancel, one check-run completion");
    assert_eq!(mine[0]["status"], "completed");
    assert_eq!(mine[0]["conclusion"], "cancelled");
}

#[tokio::test]
async fn completed_check_uploads_every_annotation_in_batches_of_fifty() {
    const CHECK_RUN_ID: u64 = 7;
    let patches = Arc::new(parking_lot::Mutex::new(Vec::<Value>::new()));
    let mock_app = Router::new()
        .route(
            "/repos/owner/repo/check-runs",
            post(|| async { Json(json!({"id": CHECK_RUN_ID})) }),
        )
        .route(
            "/repos/owner/repo/check-runs/:id",
            axum::routing::patch({
                let patches = patches.clone();
                move |Path(_id): Path<u64>, body: axum::extract::Json<Value>| {
                    let patches = patches.clone();
                    async move {
                        patches.lock().push(body.0);
                        Json(json!({"id": CHECK_RUN_ID}))
                    }
                }
            }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, mock_app).await.unwrap();
    });

    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _api_url =
        crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", format!("http://127.0.0.1:{port}"));
    let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "annotation-test-token");
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = submit_simple_run(&app).await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let job_id = JobId("build".to_owned());
    {
        let mut inner = state.inner.lock().await;
        inner
            .runs
            .get_mut(&run_id)
            .unwrap()
            .job_check_run_ids
            .insert(job_id.clone(), CHECK_RUN_ID);
        let events = inner.timeline_events.entry(run_id).or_default();
        for line in 1..=120 {
            events.push(NdjsonEvent::Annotation {
                run_id,
                job_id: job_id.clone(),
                level: AnnotationLevel::Warning,
                message: format!("warning {line}"),
                file: Some("src/lib.rs".to_owned()),
                line: Some(line),
                end_line: None,
                col: None,
                end_column: None,
                title: None,
                step_id: None,
            });
        }
    }

    request_json(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/cancel"),
        Value::Null,
    )
    .await;

    let patches = patches.lock();
    assert_eq!(patches.len(), 3);
    let batch_sizes: Vec<usize> = patches
        .iter()
        .map(|body| body["output"]["annotations"].as_array().unwrap().len())
        .collect();
    assert_eq!(batch_sizes, vec![50, 50, 20]);
    assert!(patches[0].get("status").is_none());
    assert_eq!(patches[2]["status"], "completed");
    assert_eq!(patches[2]["conclusion"], "cancelled");
}

#[tokio::test]
async fn cancel_run_refreshes_runner_pool_queue_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let first = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-22.04\n    steps:\n      - run: echo first\n",
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-24.04\n    steps:\n      - run: echo second\n",
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    assert_eq!(
        *state.next_job_runs_on.read().unwrap(),
        vec!["ubuntu-22.04"]
    );
    assert_eq!(
        state.queue_depth.load(std::sync::atomic::Ordering::Acquire),
        2
    );

    let cancelled = request_json(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{}/cancel", first["run_id"].as_str().unwrap()),
        Value::Null,
    )
    .await;

    assert_eq!(cancelled["conclusion"], "cancelled");
    assert_eq!(
        state.queue_depth.load(std::sync::atomic::Ordering::Acquire),
        1
    );
    assert_eq!(
        *state.next_job_runs_on.read().unwrap(),
        vec!["ubuntu-24.04"]
    );
}

#[tokio::test]
async fn message_poll_waits_until_work_is_enqueued() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let poll_app = app.clone();
    let poll = tokio::spawn(async move {
        request_json(
            &poll_app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=2",
            Value::Null,
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo waited
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;

    let message = poll.await.unwrap();
    assert_eq!(message["messageId"], 1);
}

#[tokio::test]
async fn session_message_flow_encrypts_decryptable_job_body() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let _keypair = {
        let inner = state.inner.lock().await;
        inner.agent_keypair.clone().unwrap()
    };
    let app = app(state, CancellationToken::new());

    let session = request_json(
        &app,
        Method::POST,
        "/internal/test/runners/sessions",
        json!({"runner_id": 1, "name": "local"}),
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();
    let key_b64 = session["encryptionKey"]["value"].as_str().unwrap();
    let aes_key =
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64).unwrap();

    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo encrypted
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;

    let message = request_json(
        &app,
        Method::GET,
        &format!("/internal/test/runners/sessions/{session_id}/messages?sessionId={session_id}"),
        Value::Null,
    )
    .await;

    let body = BASE64_STANDARD
        .decode(message["body"].as_str().unwrap())
        .unwrap();
    let iv: Vec<u8> = BASE64_STANDARD
        .decode(message["iv"].as_str().unwrap())
        .unwrap();
    let plaintext = SessionEncryption::from_key(aes_key)
        .decrypt(&body, &iv)
        .unwrap();
    let job: azdo::AgentJobRequestMessage = serde_json::from_slice(&plaintext).unwrap();

    assert_eq!(
        message["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );
    assert_eq!(job.steps[0].script.as_deref(), Some("echo encrypted"));
}

#[tokio::test]
async fn submit_run_uses_branch_and_path_filters() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on:
  push:
    branches: [main]
    paths: ["src/**"]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo ok
"#,
            "event": "push",
            "repository": "owner/repo",
            "git_ref": "refs/heads/main",
            "payload": {
                "commits": [
                    { "added": [], "modified": ["src/lib.rs"], "removed": [] }
                ]
            }
        }),
    )
    .await;
    assert!(accepted["run_id"].is_string());

    let rejected = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/runs")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "workflow_yaml": r#"
on:
  push:
    branches: [main]
    paths: ["src/**"]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo ok
"#,
                        "event": "push",
                        "repository": "owner/repo",
                        "git_ref": "refs/heads/feature",
                        "payload": {
                            "commits": [
                                { "added": [], "modified": ["docs/readme.md"], "removed": [] }
                            ]
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_get_scheduler_history_endpoint() {
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let scheduler = crate::scheduler::Scheduler::new();

    // Add a mock fire to history
    {
        let mut hist = scheduler.history.lock().await;
        hist.push(crate::scheduler::ScheduleFire {
            workflow_path: ".github/workflows/cron.yml".to_owned(),
            cron_expr: "* * * * *".to_owned(),
            fired_at: chrono::Utc::now(),
            run_id: Some("mock-run-id".to_owned()),
            error: None,
        });
    }
    state.scheduler = Some(scheduler);

    let app = app(state, CancellationToken::new());

    let res = request_json(
        &app,
        Method::GET,
        "/api/v1/scheduler/history",
        serde_json::Value::Null,
    )
    .await;

    let arr = res.as_array().expect("expected array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["workflow_path"], ".github/workflows/cron.yml");
    assert_eq!(arr[0]["cron_expr"], "* * * * *");
    assert_eq!(arr[0]["run_id"], "mock-run-id");
}

#[tokio::test]
async fn cache_protocol_reserves_uploads_commits_and_restores() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let reserve = request_json(
        &app,
        Method::POST,
        "/_apis/artifactcache/cache",
        json!({"key": "linux-node", "version": "v1"}),
    )
    .await;
    let cache_id = reserve["cacheId"].as_i64().unwrap();

    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("/_apis/artifactcache/cache/{cache_id}"))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::from("cache-bytes"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::ACCEPTED);

    request_json(
        &app,
        Method::POST,
        &format!("/_apis/artifactcache/cache/{cache_id}"),
        json!({"size": 11}),
    )
    .await;

    let lookup = request_json(
        &app,
        Method::GET,
        "/api/v1/cache?key=linux-node&version=v1",
        Value::Null,
    )
    .await;
    assert_eq!(lookup["hit"], true);
    assert_eq!(lookup["content_base64"], "Y2FjaGUtYnl0ZXM=");

    let stored = request_json(
        &app,
        Method::POST,
        "/api/v1/cache",
        json!({
            "key": "native-cache",
            "version": "v1",
            "content_base64": "bmF0aXZlLWJ5dGVz"
        }),
    )
    .await;
    assert_eq!(stored["hit"], true);
    let native_lookup = request_json(
        &app,
        Method::GET,
        "/api/v1/cache?key=native-cache&version=v1",
        Value::Null,
    )
    .await;
    assert_eq!(native_lookup["content_base64"], "bmF0aXZlLWJ5dGVz");
}

#[tokio::test]
async fn artifact_endpoint_stores_and_downloads_payload() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let run_id = RunId::new();

    let created = request_json(
        &app,
        Method::POST,
        "/api/v1/artifacts",
        json!({
            "run_id": run_id,
            "name": "logs",
            "file_name": "job.txt",
            "content_base64": "aGVsbG8="
        }),
    )
    .await;
    let artifact_id = created["id"].as_str().unwrap();
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/artifacts/{artifact_id}"))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&bytes[..], b"hello");
}

#[tokio::test]
async fn full_runner_lifecycle_register_session_poll_complete() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // 1. connectionData
    let (s, conn) = try_req(
        &app,
        Method::GET,
        "/runner/server/_apis/connectionData",
        Value::Null,
    )
    .await;
    assert!(s.is_success(), "1 connectionData: {}", s);
    assert!(conn["locationServiceData"]["serviceDefinitions"].is_array());

    // 2. OAuth token
    let (s, _) = try_req(
        &app,
        Method::POST,
        "/_apis/v1/oauth2/token",
        json!({"grant_type":"client_credentials","client_id":"t","client_secret":"t"}),
    )
    .await;
    assert!(s.is_success(), "2 oauth2: {}", s);

    // 3. Register runner
    let (s, reg) = try_req(
        &app,
        Method::POST,
        "/api/v1/runners",
        json!({"name":"test-runner","labels":["self-hosted","linux","x64"]}),
    )
    .await;
    assert!(s.is_success(), "3 register: {} body={}", s, reg);
    let runner_id = reg["id"].as_i64().unwrap();

    // 4. Create session
    let (s, sess) = try_req(
        &app,
        Method::POST,
        "/internal/test/runners/sessions",
        json!({"runner_id": runner_id, "name": "test-runner"}),
    )
    .await;
    assert!(s.is_success(), "4 session: {} body={}", s, sess);
    let session_id = sess["sessionId"].as_str().unwrap().to_owned();

    // 5. Submit a workflow
    let (s, accepted) = try_req(&app, Method::POST, "/api/v1/runs",
            json!({"workflow_yaml":"on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n","event":"push","repository":"owner/repo"})).await;
    assert!(s.is_success(), "5 submit: {} body={}", s, accepted);
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // 6. Poll for messages — the runner uses the AzDO Message endpoint
    let (s, msg) = try_req(
        &app,
        Method::GET,
        &format!(
            "/internal/test/runners/sessions/{}/messages?sessionId={}&waitSeconds=0",
            session_id, session_id
        ),
        Value::Null,
    )
    .await;
    assert!(s.is_success(), "6 poll: {} body={}", s, msg);

    // 7. Get the job from the run
    let inner = state.inner.lock().await;
    let run_record = inner.runs.get(&run_id).unwrap();
    let job_id = run_record.jobs.keys().next().unwrap().clone();
    drop(inner);

    // 8. Complete the job
    let (s, _) = try_req(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({"run_id": run_id, "job_id": job_id, "status": "success"}),
    )
    .await;
    assert!(s.is_success(), "8 complete: {}", s);

    // 9. Verify run succeeded
    let (_, final_run) = try_req(
        &app,
        Method::GET,
        &format!("/api/v1/runs/{}", run_id),
        Value::Null,
    )
    .await;
    assert_eq!(final_run["status"], "success");
}

/// The runner prints its `GITHUB_TOKEN Permissions` group from this variable, so
/// it must state what the job's token actually carries: the restricted default
/// when the workflow declares nothing (matching the official runner's setup
/// log), and nothing at all when the workflow withholds everything.

/// The runner prints its `GITHUB_TOKEN Permissions` group from this variable, so
/// it must state what the job's token actually carries: the restricted default
/// when the workflow declares nothing (matching the official runner's setup
/// log), and nothing at all when the workflow withholds everything.
#[tokio::test]
async fn the_wire_token_permissions_match_the_declared_policy() {
    for (declaration, expected) in [
        (
            "",
            r#"{"Contents":"read","Metadata":"read","Packages":"read"}"#,
        ),
        ("permissions: {}\n", "{}"),
        (
            "permissions:\n  contents: read\n  pull-requests: write\n",
            r#"{"Contents":"read","Metadata":"read","PullRequests":"write"}"#,
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": format!(
                    "on: push\n{declaration}jobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n"
                ),
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;

        let inner = state.inner.lock().await;
        let queued = inner.queue.front().expect("job should be queued");
        assert_eq!(
            queued
                .message
                .variables
                .get("system.github.token.permissions")
                .and_then(|variable| variable.value.as_deref()),
            Some(expected),
            "wire permissions for {declaration:?}"
        );
    }
}

/// GitHub's fork profile is the single effective job-authorization policy for
/// fork-restricted tiers. A fork PR declaring `checks: write` and
/// `id-token: write` must come out read-only on the runner-visible wire
/// variable, read-only in the App installation-token request, with no OIDC
/// request URL and no OIDC grant — while a trusted push and a
/// `pull_request_target` keep the declared writes and OIDC untouched.

/// GitHub's fork profile is the single effective job-authorization policy for
/// fork-restricted tiers. A fork PR declaring `checks: write` and
/// `id-token: write` must come out read-only on the runner-visible wire
/// variable, read-only in the App installation-token request, with no OIDC
/// request URL and no OIDC grant — while a trusted push and a
/// `pull_request_target` keep the declared writes and OIDC untouched.
#[tokio::test]
async fn fork_pr_jobs_are_downgraded_to_read_only_and_oidc_denied() {
    use crate::github_app::{GitHubAppCredentials, MintFailurePolicy};

    let private_key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.github_app = Some(GitHubAppCredentials::for_tests(
        "424",
        private_key,
        MintFailurePolicy::LocalJwt,
    ));
    let app = app(state.clone(), CancellationToken::new());
    // Native `/api/v1/runs` clears client-supplied `trust_tier` (only the
    // webhook adapters stamp provenance). Drive the fork downgrades through
    // the same entry point the webhook path uses: `submit_run_inner` with an
    // already-stamped submission.
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });

    let yaml = "on: pull_request\npermissions:\n  checks: write\n  id-token: write\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let payload = json!({
        "action": "opened",
        "number": 7,
        "pull_request": {
            "head": {"ref": "feature", "sha": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3"},
            "base": {"ref": "main", "sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"}
        },
        "repository": {"full_name": "owner/repo", "default_branch": "main"}
    });
    let submit = |tier: Option<&'static str>| {
        let shared = shared.clone();
        let payload = payload.clone();
        async move {
            crate::submit_run_inner(
                &shared,
                preloop_gha_protocol::WorkflowSubmission {
                    workflow_yaml: yaml.to_owned(),
                    event: "pull_request".to_owned(),
                    repository: "owner/repo".to_owned(),
                    payload,
                    trust_tier: tier.map(str::to_owned),
                    ..Default::default()
                },
            )
            .await
            .expect("fork/trusted/target submission accepted")
        }
    };

    // Fork PR: declared writes must not survive, OIDC must not be granted.
    let fork = submit(Some("untrusted-fork-pull-request")).await;
    let fork_run_id = fork.run_id.to_string();
    let trusted = submit(None).await;
    let trusted_run_id = trusted.run_id.to_string();
    let target = submit(Some("pull-request-target")).await;
    let target_run_id = target.run_id.to_string();

    let inner = state.inner.lock().await;
    let fork_message = queued_message_for(&inner, &fork_run_id);
    assert_eq!(
        variable_value(&fork_message, "system.github.token.permissions"),
        Some(r#"{"Checks":"read","Metadata":"read"}"#),
        "fork PR: declared checks write must be clamped to read, and id-token \
         must not be advertised as a read permission"
    );
    assert!(
        !variable_value(&fork_message, "system.github.token.permissions")
            .is_some_and(|wire| wire.contains("IdToken")),
        "fork PR: the wire permissions must carry no IdToken metadata"
    );
    let fork_endpoint = fork_message
        .resources
        .endpoints
        .iter()
        .find(|endpoint| endpoint.name.eq_ignore_ascii_case("SystemVssConnection"))
        .expect("SystemVssConnection endpoint present");
    assert!(
        !fork_endpoint
            .data
            .get("GenerateIdTokenUrl")
            .is_some_and(|url| !url.is_empty()),
        "fork PR: no OIDC request URL may be emitted"
    );
    let fork_request = inner
        .github_token_requests
        .get(&fork_message.request_id)
        .expect("fork PR job defers an App token request");
    assert_eq!(
        fork_request.permissions,
        BTreeMap::from([("checks".to_owned(), "read".to_owned())]),
        "fork PR: App token request carries only the read-only fork profile"
    );
    assert!(
        !fork_request.permissions.contains_key("id-token"),
        "fork PR: the App token request must not name the non-App id-token scope"
    );
    assert!(
        fork_request.untrusted,
        "fork PR: token request must be marked untrusted so no fallback widens it"
    );
    let fork_job_record = inner
        .job_requests
        .get(&fork_message.request_id)
        .expect("fork job request record present");
    assert_eq!(
        inner
            .id_token_grants
            .get(&(fork_job_record.run_id, fork_job_record.job_id.clone())),
        Some(&false),
        "fork PR: no OIDC grant may be recorded"
    );

    // Trusted push: declared writes and OIDC survive verbatim.
    let trusted_message = queued_message_for(&inner, &trusted_run_id);
    assert_eq!(
        variable_value(&trusted_message, "system.github.token.permissions"),
        Some(r#"{"Checks":"write","Metadata":"read"}"#),
        "trusted job keeps the declared write profile"
    );
    let trusted_endpoint = trusted_message
        .resources
        .endpoints
        .iter()
        .find(|endpoint| endpoint.name.eq_ignore_ascii_case("SystemVssConnection"))
        .expect("SystemVssConnection endpoint present");
    assert!(
        trusted_endpoint
            .data
            .get("GenerateIdTokenUrl")
            .is_some_and(|url| !url.is_empty()),
        "trusted job keeps the OIDC request URL"
    );
    let trusted_request = inner
        .github_token_requests
        .get(&trusted_message.request_id)
        .expect("trusted job defers an App token request");
    assert_eq!(
        trusted_request.permissions,
        BTreeMap::from([("checks".to_owned(), "write".to_owned())]),
        "trusted job's App token request carries only real App repository permissions"
    );
    assert!(
        !trusted_request.permissions.contains_key("id-token"),
        "the App installation-token request must exclude the non-App id-token scope"
    );
    assert!(
        !trusted_request.untrusted,
        "trusted job is not marked untrusted"
    );

    // pull_request_target: base-repo trust, declared writes untouched.
    let target_message = queued_message_for(&inner, &target_run_id);
    assert_eq!(
        variable_value(&target_message, "system.github.token.permissions"),
        Some(r#"{"Checks":"write","Metadata":"read"}"#),
        "pull_request_target keeps base-repo trust"
    );
    drop(inner);

    // The OIDC endpoint enforces the same grant: refused for the fork,
    // minted for the trusted job.
    let fork_uri = format!(
        "/runner/server/_apis/distributedtask/hubs/actions/plans/{}/jobs/{}/oidctoken?audience=api://test",
        fork_message.plan.plan_id, fork_message.job_id
    );
    let (status, _) = try_req(&app, Method::GET, &fork_uri, Value::Null).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "fork PR: OIDC token request must be refused"
    );
    let trusted_uri = format!(
        "/runner/server/_apis/distributedtask/hubs/actions/plans/{}/jobs/{}/oidctoken?audience=api://test",
        trusted_message.plan.plan_id, trusted_message.job_id
    );
    let token = request_json(&app, Method::GET, &trusted_uri, Value::Null).await;
    assert!(
        token["value"]
            .as_str()
            .is_some_and(|jwt| jwt.split('.').count() == 3),
        "trusted job still mints an OIDC JWT"
    );
}

/// A mint failure for an untrusted fork job must never reach the configured
/// `PRELOOP_GITHUB_TOKEN` PAT fallback: the PAT is repository-unscoped and
/// ignores `permissions:`, so handing it to fork PR code would grant
/// authority GitHub's read-only fork profile never allowed. The job keeps the
/// local runtime token instead — while a trusted job under the same `pat`
/// policy still receives the PAT.

/// A mint failure for an untrusted fork job must never reach the configured
/// `PRELOOP_GITHUB_TOKEN` PAT fallback: the PAT is repository-unscoped and
/// ignores `permissions:`, so handing it to fork PR code would grant
/// authority GitHub's read-only fork profile never allowed. The job keeps the
/// local runtime token instead — while a trusted job under the same `pat`
/// policy still receives the PAT.
#[tokio::test]
async fn untrusted_job_mint_failure_never_falls_back_to_the_pat() {
    use crate::github_app::{GitHubAppCredentials, MintFailurePolicy};

    let private_key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let mut creds = GitHubAppCredentials::for_tests("424", private_key, MintFailurePolicy::Pat);
    creds.pat_fallback = Some("github_pat_broad".to_owned());
    state.github_app = Some(creds);
    let shutdown = CancellationToken::new();
    let _app = app(state.clone(), shutdown.clone());
    // Native `/api/v1/runs` clears client-supplied `trust_tier` (only the
    // webhook adapters stamp provenance); stamp it via `submit_run_inner`
    // like the webhook path does.
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: shutdown.clone(),
    });

    // `local-workspace-only` carries no `owner/repo` slug, so the mint fails
    // before it signs anything or opens a socket — exactly like
    // `app_token_mint_failure_follows_the_configured_policy`.
    let yaml = "on: push\npermissions:\n  checks: write\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let fork = crate::submit_run_inner(
        &shared,
        preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: yaml.to_owned(),
            event: "push".to_owned(),
            repository: "local-workspace-only".to_owned(),
            trust_tier: Some("untrusted-fork-pull-request".to_owned()),
            ..Default::default()
        },
    )
    .await
    .expect("fork submission accepted");
    let fork_run_id = fork.run_id.to_string();
    let trusted = crate::submit_run_inner(
        &shared,
        preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: yaml.to_owned(),
            event: "push".to_owned(),
            repository: "local-workspace-only".to_owned(),
            ..Default::default()
        },
    )
    .await
    .expect("trusted submission accepted");
    let trusted_run_id = trusted.run_id.to_string();

    {
        let inner = state.inner.lock().await;
        let fork_message = queued_message_for(&inner, &fork_run_id);
        let fork_request = inner
            .github_token_requests
            .get(&fork_message.request_id)
            .cloned()
            .expect("fork job defers a token request");
        let trusted_message = queued_message_for(&inner, &trusted_run_id);
        let trusted_request = inner
            .github_token_requests
            .get(&trusted_message.request_id)
            .cloned()
            .expect("trusted job defers a token request");
        assert!(fork_request.untrusted);
        assert!(!trusted_request.untrusted);
        let fork_mint = crate::broker::mint_dispatch_github_token(&shared, &fork_request).await;
        assert!(
            matches!(fork_mint, Ok(None)),
            "fork job must not receive the PAT fallback under the `pat` policy"
        );
        let trusted_mint =
            crate::broker::mint_dispatch_github_token(&shared, &trusted_request).await;
        assert_eq!(
            trusted_mint
                .expect("trusted job's mint failure is not a dispatch error")
                .expect("pat policy hands the PAT to a trusted job")
                .token,
            "github_pat_broad",
            "the PAT fallback still applies to trusted jobs"
        );
    }
}

/// The broker claim swaps the build-time token for the minted App token.
/// Every official runner-visible wire alias must follow coherently —
/// `system.github.token` (the `${{ github.token }}` variable), `github_token`,
/// and the `github` context's `token` entry. The runner maps `github_token` to
/// `${{ secrets.GITHUB_TOKEN }}` locally; the uppercase name is not wire data.

/// The broker claim swaps the build-time token for the minted App token.
/// Every official runner-visible wire alias must follow coherently —
/// `system.github.token` (the `${{ github.token }}` variable), `github_token`,
/// and the `github` context's `token` entry. The runner maps `github_token` to
/// `${{ secrets.GITHUB_TOKEN }}` locally; the uppercase name is not wire data.
#[tokio::test]
async fn broker_claim_patches_every_official_token_alias_with_the_minted_token() {
    use crate::github_app::{GitHubAppCredentials, MintFailurePolicy};
    use axum::routing::{get, post};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_base = format!("http://{}", listener.local_addr().unwrap());
    let stub = Router::new()
        .route(
            "/app/installations",
            get(|| async { Json(json!([{"id": 4242, "account": {"login": "owner"}}])) }),
        )
        .route(
            "/app/installations/:installation_id/access_tokens",
            post(
                |Path(installation_id): Path<u64>, body: Json<Value>| async move {
                    assert_eq!(installation_id, 4242);
                    assert_eq!(body.0["permissions"], json!({"checks": "write"}));
                    assert!(
                        body.0["permissions"].get("id-token").is_none(),
                        "id-token is not an installation-token scope and must not be requested"
                    );
                    Json(json!({
                        "token": "ghs_minted_alias_token",
                        "expires_at": "2999-01-01T00:00:00Z"
                    }))
                },
            ),
        );
    tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

    // Held for the whole test: `PRELOOP_GITHUB_API_URL` is process-global.
    // `TestEnvVar` restores it even if an assertion below panics — a bare
    // `remove_var` at the end of the body leaks this stub's address to every
    // later test when the test fails, which turns one failure into a cascade.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _api_url = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", api_base);

    let private_key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.github_app = Some(GitHubAppCredentials::for_tests(
        "424",
        private_key,
        MintFailurePolicy::LocalJwt,
    ));
    let app = app(state.clone(), CancellationToken::new());

    let registered = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({"name": "alias-runner", "version": "2.335.1"}),
    )
    .await;
    let runner_id = registered["id"].as_i64().unwrap();
    let runner_token = state
        .local_jwt(json!({
            "sub": format!("preloop-runner-listen-{runner_id}"),
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\npermissions:\n  checks: write\n  id-token: write\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "payload": {"ref": "refs/heads/main", "commits": []},
            "repository": "owner/repo",
            "git_ref": "refs/heads/main",
        }),
    )
    .await;

    let session = request_json_with_bearer(
        &app,
        Method::POST,
        "/runner/server/session",
        json!({}),
        &runner_token,
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();
    let job_ref = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/runner/server/message?sessionId={session_id}&status=Online&waitSeconds=0"),
        Value::Null,
        &runner_token,
    )
    .await;
    let body: Value = serde_json::from_str(job_ref["body"].as_str().unwrap()).unwrap();
    let runner_request_id = body["runner_request_id"].as_str().unwrap();

    let acquired = request_json_with_bearer(
        &app,
        Method::POST,
        &format!("/broker/{runner_id}/acquirejob"),
        json!({"jobMessageId": runner_request_id, "billingOwnerId": "local", "runnerOS": "Linux"}),
        &runner_token,
    )
    .await;
    for name in ["system.github.token", "github_token"] {
        assert_eq!(
            acquired["variables"][name]["value"], "ghs_minted_alias_token",
            "{name} must carry the minted App token after the claim"
        );
        assert_eq!(
            acquired["variables"][name]["isSecret"], true,
            "{name} must stay marked secret"
        );
    }
    assert!(
        acquired["variables"].get("GITHUB_TOKEN").is_none(),
        "uppercase GITHUB_TOKEN is not part of the official acquire schema"
    );
    // `${{ github.token }}` in the workflow context must see the same mint.
    let context_pairs = acquired["contextData"]["github"]["d"].as_array().unwrap();
    let context_token = context_pairs
        .iter()
        .find(|pair| pair["k"] == "token")
        .expect("github context carries a token entry")
        .clone();
    assert_eq!(
        context_token["v"], "ghs_minted_alias_token",
        "the github context token must be the minted App token"
    );
    // No narrowing occurred, so the wire permissions keep the declared
    // repository set; the OIDC grant is carried by the endpoint metadata.
    assert_eq!(
        acquired["variables"]["system.github.token.permissions"]["value"],
        r#"{"Checks":"write","Metadata":"read"}"#,
        "an un-narrowed mint leaves the declared wire permissions intact"
    );
}
