use super::*;

#[derive(Debug, Deserialize)]
pub(crate) struct JobLogsSignedBlobUrlRequest {
    pub(crate) workflow_job_run_backend_id: String,
    pub(crate) workflow_run_backend_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct StepLogsSignedBlobUrlRequest {
    pub(crate) step_backend_id: String,
    pub(crate) workflow_job_run_backend_id: String,
    pub(crate) workflow_run_backend_id: String,
}

pub(crate) async fn twirp_workflow_steps_update(
    State(shared): State<Arc<SharedState>>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mut inner = shared.state.inner.lock().await;

    let plan_id = payload["workflow_run_backend_id"].as_str().unwrap_or("");
    let agent_job_id_str = payload["workflow_job_run_backend_id"]
        .as_str()
        .unwrap_or("");

    if let (Some(plan_uuid), Some(job_uuid)) = (
        uuid::Uuid::parse_str(plan_id).ok(),
        uuid::Uuid::parse_str(agent_job_id_str).ok(),
    ) {
        if let Some((_, run_id, job_id)) =
            resolve_callback_job(&inner, &plan_uuid.to_string(), None, Some(job_uuid))
        {
            if let Some(run) = inner.runs.get_mut(&run_id) {
                let job_name = job_id.0.clone();
                let job_detail =
                    if let Some(pos) = run.jobs_list.iter().position(|j| j.name == job_name) {
                        &mut run.jobs_list[pos]
                    } else {
                        run.jobs_list.push(JobDetail {
                            name: job_name,
                            conclusion: "success".to_owned(),
                            steps: Vec::new(),
                        });
                        run.jobs_list.last_mut().unwrap()
                    };

                if let Some(status) = run.jobs.get(&job_id) {
                    job_detail.conclusion = format!("{:?}", status).to_lowercase();
                }
                if let Some(steps) = payload["steps"].as_array() {
                    for step in steps {
                        let name = step["name"].as_str().unwrap_or("").to_owned();
                        let conclusion_num = step["conclusion"].as_u64().unwrap_or(0);
                        let status_num = step["status"].as_u64().unwrap_or(0);

                        let job_status = run.jobs.get(&job_id).copied();
                        let conclusion_str = if status_num == 6 {
                            match conclusion_num {
                                2 => "success",
                                3 => {
                                    if job_status == Some(ExecutionStatus::Cancelled) {
                                        "cancelled"
                                    } else {
                                        "failure"
                                    }
                                }
                                7 => "skipped",
                                _ => "success",
                            }
                        } else {
                            "in_progress"
                        };

                        if let Some(pos) = job_detail.steps.iter().position(|s| s.name == name) {
                            job_detail.steps[pos].conclusion = conclusion_str.to_owned();
                        } else {
                            job_detail.steps.push(StepRecord {
                                name,
                                conclusion: conclusion_str.to_owned(),
                            });
                        }
                    }
                }
            }
        }
    }

    Ok(Json(json!({"ok": true})))
}

pub(crate) async fn twirp_get_job_logs_signed_blob_url(
    Json(request): Json<JobLogsSignedBlobUrlRequest>,
) -> Json<serde_json::Value> {
    Json(json!({
        "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
        "logs_url": format!(
            "{}/replay/results/{}/{}/job-logs.txt",
            public_base_url(), request.workflow_run_backend_id, request.workflow_job_run_backend_id
        )
    }))
}

pub(crate) async fn twirp_get_job_diag_logs_signed_blob_url(
    Json(_request): Json<JobLogsSignedBlobUrlRequest>,
) -> Json<serde_json::Value> {
    let token = uuid::Uuid::new_v4();
    Json(json!({
        "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
        "diag_logs_url": format!("{}/twirp-blob/diag/{token}", public_base_url()),
    }))
}

pub(crate) async fn twirp_get_step_logs_signed_blob_url(
    Json(request): Json<StepLogsSignedBlobUrlRequest>,
) -> Json<serde_json::Value> {
    Json(json!({
        "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
        "logs_url": format!(
            "{}/replay/results/{}/{}/step-{}.txt",
            public_base_url(), request.workflow_run_backend_id, request.workflow_job_run_backend_id, request.step_backend_id
        ),
        "soft_size_limit": "1048576"
    }))
}

#[derive(Debug, Deserialize)]
pub(crate) struct StepSummarySignedBlobUrlRequest {
    pub(crate) step_backend_id: String,
    pub(crate) workflow_job_run_backend_id: String,
    pub(crate) workflow_run_backend_id: String,
}

pub(crate) async fn twirp_get_step_summary_signed_blob_url(
    Json(request): Json<StepSummarySignedBlobUrlRequest>,
) -> Json<serde_json::Value> {
    Json(json!({
        "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
        "summary_url": format!(
            "{}/replay/results/{}/{}/step-{}-summary.md",
            public_base_url(), request.workflow_run_backend_id, request.workflow_job_run_backend_id, request.step_backend_id
        ),
        "soft_size_limit": "1048576"
    }))
}

#[derive(Debug, Deserialize)]
pub(crate) struct StepSummaryMetadataRequest {
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) step_backend_id: String,
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) workflow_job_run_backend_id: String,
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) workflow_run_backend_id: String,
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) size: Option<u64>,
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) uploaded_at: Option<String>,
}

pub(crate) async fn twirp_create_step_summary_metadata(
    Json(_request): Json<StepSummaryMetadataRequest>,
) -> Json<serde_json::Value> {
    Json(json!({"ok": true}))
}

#[derive(Debug, Deserialize)]
pub(crate) struct StepLogsMetadataRequest {
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) step_backend_id: Option<String>,
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) workflow_job_run_backend_id: Option<String>,
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) workflow_run_backend_id: Option<String>,
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) upload_url: Option<String>,
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) line_count: Option<u64>,
}

/// POST CreateStepLogsMetadata — runner calls this after uploading step logs.
pub(crate) async fn twirp_create_step_logs_metadata(
    Json(_request): Json<StepLogsMetadataRequest>,
) -> Json<serde_json::Value> {
    Json(json!({"ok": true}))
}

#[derive(Debug, Deserialize)]
pub(crate) struct JobLogsMetadataRequest {
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) workflow_job_run_backend_id: Option<String>,
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) workflow_run_backend_id: Option<String>,
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) upload_url: Option<String>,
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) line_count: Option<u64>,
}

/// POST CreateJobLogsMetadata — runner calls this after uploading job logs.
pub(crate) async fn twirp_create_job_logs_metadata(
    Json(_request): Json<JobLogsMetadataRequest>,
) -> Json<serde_json::Value> {
    Json(json!({"ok": true}))
}

// ─── Cache v2 Twirp (github.actions.results.api.v1.CacheService) ─────────────

pub(crate) fn scoped_cache_key(key: &str, scope: Option<&str>, repository: Option<&str>) -> String {
    format!(
        "{}:{}\0{key}",
        repository.unwrap_or("default"),
        scope.unwrap_or("default")
    )
}

#[derive(Debug, Deserialize)]
pub(crate) struct CacheV2CreateRequest {
    pub(crate) key: String,
    pub(crate) version: String,
    #[serde(default)]
    pub(crate) scope: Option<String>,
    #[serde(default)]
    pub(crate) repository: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CacheV2FinalizeRequest {
    pub(crate) key: String,
    pub(crate) version: String,
    #[serde(default)]
    pub(crate) scope: Option<String>,
    #[serde(default)]
    pub(crate) repository: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CacheV2GetDlUrlRequest {
    pub(crate) key: String,
    pub(crate) version: String,
    #[serde(default)]
    pub(crate) restore_keys: Vec<String>,
    #[serde(default)]
    pub(crate) scope: Option<String>,
    #[serde(default)]
    pub(crate) repository: Option<String>,
}

pub(crate) async fn twirp_cache_v2_create(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<CacheV2CreateRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let storage_key = scoped_cache_key(
        &request.key,
        request.scope.as_deref(),
        request.repository.as_deref(),
    );
    if shared
        .state
        .cache
        .get(&storage_key, &request.version, &[])
        .await
        .map_err(|error| ApiError::internal(format!("cache lookup error: {error}")))?
        .is_some()
    {
        return Ok(Json(json!({
            "ok": false,
            "signed_upload_url": "",
            "message": "cache already exists"
        })));
    }
    let token = uuid::Uuid::new_v4().to_string();
    let stage_dir = shared
        .state
        .state_dir
        .join("blobs")
        .join("cache")
        .join(&token);
    tokio::fs::create_dir_all(&stage_dir)
        .await
        .map_err(|e| ApiError::internal(format!("failed to create cache stage dir: {e}")))?;
    let already_reserved = {
        let mut inner = shared.state.inner.lock().await;
        if inner
            .cache_v2_pending
            .values()
            .any(|pending| pending.key == storage_key && pending.version == request.version)
        {
            true
        } else {
            inner.cache_v2_pending.insert(
                token.clone(),
                CacheV2Pending {
                    key: storage_key,
                    version: request.version,
                },
            );
            false
        }
    };
    if already_reserved {
        let _ = tokio::fs::remove_dir_all(&stage_dir).await;
        return Ok(Json(json!({
            "ok": false,
            "signed_upload_url": "",
            "message": "cache upload already reserved"
        })));
    }
    let upload_url = format!("{}/twirp-blob/cache/{token}", public_base_url());
    info!(token, "cache v2 create entry");
    Ok(Json(
        json!({ "ok": true, "signed_upload_url": upload_url, "message": "" }),
    ))
}

pub(crate) async fn twirp_cache_v2_finalize(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<CacheV2FinalizeRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let storage_key = scoped_cache_key(
        &request.key,
        request.scope.as_deref(),
        request.repository.as_deref(),
    );
    // Find the pending upload token matching key+version.
    let token = {
        let inner = shared.state.inner.lock().await;
        inner
            .cache_v2_pending
            .iter()
            .find(|(_, p)| p.key == storage_key && p.version == request.version)
            .map(|(k, _)| k.clone())
    }
    .ok_or_else(|| ApiError::not_found("no pending cache upload for key+version"))?;

    let blob_path = shared
        .state
        .state_dir
        .join("blobs")
        .join("cache")
        .join(&token)
        .join("data");
    let bytes = tokio::fs::read(&blob_path).await.map_err(|e| {
        ApiError::not_found(format!("cache blob not found (not yet uploaded?): {e}"))
    })?;

    let (key, version) = {
        let inner = shared.state.inner.lock().await;
        let pending = inner
            .cache_v2_pending
            .get(&token)
            .ok_or_else(|| ApiError::internal("pending entry vanished"))?;
        (pending.key.clone(), pending.version.clone())
    };

    shared
        .state
        .cache
        .put(&key, &version, &bytes)
        .await
        .map_err(|e| ApiError::internal(format!("cache store error: {e}")))?;

    {
        let mut inner = shared.state.inner.lock().await;
        inner.cache_v2_pending.remove(&token);
    }

    // Clean up staging directory.
    let _ = tokio::fs::remove_dir_all(
        shared
            .state
            .state_dir
            .join("blobs")
            .join("cache")
            .join(&token),
    )
    .await;

    info!(key, version, size = bytes.len(), "cache v2 finalized");
    Ok(Json(json!({ "ok": true, "entry_id": "1", "message": "" })))
}

pub(crate) async fn twirp_cache_v2_get_dl_url(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<CacheV2GetDlUrlRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let storage_key = scoped_cache_key(
        &request.key,
        request.scope.as_deref(),
        request.repository.as_deref(),
    );
    let storage_restore_keys = request
        .restore_keys
        .iter()
        .map(|key| scoped_cache_key(key, request.scope.as_deref(), request.repository.as_deref()))
        .collect::<Vec<_>>();
    let result = shared
        .state
        .cache
        .get(&storage_key, &request.version, &storage_restore_keys)
        .await
        .map_err(|e| ApiError::internal(format!("cache lookup error: {e}")))?;

    let (entry, _bytes) = match result {
        Some(r) => r,
        None => {
            return Ok(Json(
                json!({ "ok": false, "signed_download_url": "", "matched_key": "" }),
            ))
        }
    };

    let dl_token = uuid::Uuid::new_v4().to_string();
    {
        let mut inner = shared.state.inner.lock().await;
        inner
            .cache_v2_dl_tokens
            .insert(dl_token.clone(), (entry.key.clone(), entry.version.clone()));
    }
    let download_url = format!("{}/twirp-blob/cache/{dl_token}", public_base_url());
    let matched_key = entry
        .key
        .split_once('\0')
        .map(|(_, key)| key.to_owned())
        .unwrap_or_else(|| entry.key.clone());
    info!(key = %matched_key, "cache v2 download URL issued");
    Ok(Json(json!({
        "ok": true,
        "signed_download_url": download_url,
        "matched_key": matched_key
    })))
}
