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
            // Find the step names by external_id and clone them to release the borrow on inner
            let request_id = inner.agent_job_requests.get(&job_uuid).copied();
            let step_names: std::collections::HashMap<uuid::Uuid, String> = request_id
                .and_then(|id| inner.broker_messages.get(&id))
                .map(|msg| {
                    msg.steps
                        .iter()
                        .map(|s| {
                            (
                                s.id,
                                s.display_name
                                    .clone()
                                    .or_else(|| s.name.clone())
                                    .unwrap_or_default(),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();

            if let Some(run) = inner.runs.get_mut(&run_id) {
                let job_name = job_id.0.clone();
                let job_detail =
                    if let Some(pos) = run.jobs_list.iter().position(|j| j.name == job_name) {
                        &mut run.jobs_list[pos]
                    } else {
                        run.jobs_list.push(JobDetail {
                            name: job_name,
                            // A step update means the job started; the run
                            // record's final conclusion comes from the job
                            // status map (projected in the runs GET). Default
                            // to the truthful in-flight state, never "success".
                            conclusion: "in_progress".to_owned(),
                            steps: Vec::new(),
                            annotations: Vec::new(),
                        });
                        run.jobs_list.last_mut().unwrap()
                    };

                if let Some(status) = run.jobs.get(&job_id) {
                    job_detail.conclusion = format!("{:?}", status).to_lowercase();
                }
                if let Some(steps) = payload["steps"].as_array() {
                    for step in steps {
                        let external_id_str = step["external_id"].as_str().unwrap_or("");
                        let step_uuid = uuid::Uuid::parse_str(external_id_str).ok();

                        // The runner reports the rendered display name in the
                        // update payload ("Run actions/checkout@v4", "Set up
                        // job") — the same string GitHub's UI shows. Prefer it
                        // over the broker-message name, which the server
                        // leaves empty for steps without an explicit `name:`
                        // (an empty lookup result previously won and steps
                        // showed as `''` in run records).
                        let name = step["name"]
                            .as_str()
                            .filter(|name| !name.is_empty())
                            .map(str::to_owned)
                            .or_else(|| step_uuid.and_then(|suuid| step_names.get(&suuid).cloned()))
                            .unwrap_or_default();

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
                        let terminal = status_num == 6;
                        let observed = chrono::Utc::now();

                        if let Some(pos) = job_detail.steps.iter().position(|s| s.name == name) {
                            job_detail.steps[pos].conclusion = conclusion_str.to_owned();
                            // First non-terminal sighting is the start signal.
                            if !terminal && job_detail.steps[pos].started_at.is_none() {
                                job_detail.steps[pos].started_at = Some(observed);
                            }
                            if terminal && job_detail.steps[pos].finished_at.is_none() {
                                job_detail.steps[pos].finished_at = Some(observed);
                            }
                        } else {
                            // First time we hear about this step:
                            // - in_progress → record started_at only
                            // - already terminal → record finished_at only
                            //   (do not invent started_at == finished_at, which
                            //   forces duration 0 for fast steps that complete
                            //   before any in-progress update is processed)
                            job_detail.steps.push(StepRecord {
                                name,
                                conclusion: conclusion_str.to_owned(),
                                started_at: (!terminal).then_some(observed),
                                finished_at: terminal.then_some(observed),
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
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<JobLogsSignedBlobUrlRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // The signed URL is the upload credential for `/replay/results/*` — a
    // bearerless route reachable from inside every runner VM. Only mint for
    // the plan/job the caller's token actually names, or workflow code could
    // ask for another job's URL and overwrite its logs.
    if !crate::auth::results_token_binds_job(
        &shared.state,
        crate::auth::bearer_from_headers(&headers),
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
    ) {
        return Err(ApiError::forbidden(
            "replay blob URL minting requires a token for that job",
        ));
    }
    let path = format!(
        "/replay/results/{}/{}/job-logs.txt",
        request.workflow_run_backend_id, request.workflow_job_run_backend_id
    );
    let sig = crate::auth::sign_replay_upload_ticket(&shared.state, &path);
    Ok(Json(json!({
        "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
        "logs_url": format!(
            "{}{}?sv=2021-08-06&se=2028-01-01T00%3A00%3A00Z&sr=c&sp=rw&sig={sig}",
            runner_base_url(), path
        )
    })))
}

pub(crate) async fn twirp_get_job_diag_logs_signed_blob_url(
    Json(_request): Json<JobLogsSignedBlobUrlRequest>,
) -> Json<serde_json::Value> {
    let token = uuid::Uuid::new_v4();
    Json(json!({
        "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
        "diag_logs_url": format!("{}/twirp-blob/diag/{token}?sv=2021-08-06&se=2028-01-01T00%3A00%3A00Z&sr=c&sp=rw&sig=dummy", runner_base_url()),
    }))
}

pub(crate) async fn twirp_get_step_logs_signed_blob_url(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<StepLogsSignedBlobUrlRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if !crate::auth::results_token_binds_job(
        &shared.state,
        crate::auth::bearer_from_headers(&headers),
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
    ) {
        return Err(ApiError::forbidden(
            "replay blob URL minting requires a token for that job",
        ));
    }
    let path = format!(
        "/replay/results/{}/{}/step-{}.txt",
        request.workflow_run_backend_id,
        request.workflow_job_run_backend_id,
        request.step_backend_id
    );
    let sig = crate::auth::sign_replay_upload_ticket(&shared.state, &path);
    Ok(Json(json!({
        "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
        "logs_url": format!(
            "{}{}?sv=2021-08-06&se=2028-01-01T00%3A00%3A00Z&sr=c&sp=rw&sig={sig}",
            runner_base_url(), path
        ),
        "soft_size_limit": "1048576"
    })))
}

#[derive(Debug, Deserialize)]
pub(crate) struct StepSummarySignedBlobUrlRequest {
    pub(crate) step_backend_id: String,
    pub(crate) workflow_job_run_backend_id: String,
    pub(crate) workflow_run_backend_id: String,
}

pub(crate) async fn twirp_get_step_summary_signed_blob_url(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<StepSummarySignedBlobUrlRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if !crate::auth::results_token_binds_job(
        &shared.state,
        crate::auth::bearer_from_headers(&headers),
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
    ) {
        return Err(ApiError::forbidden(
            "replay blob URL minting requires a token for that job",
        ));
    }
    let path = format!(
        "/replay/results/{}/{}/step-{}-summary.md",
        request.workflow_run_backend_id,
        request.workflow_job_run_backend_id,
        request.step_backend_id
    );
    let sig = crate::auth::sign_replay_upload_ticket(&shared.state, &path);
    Ok(Json(json!({
        "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
        "summary_url": format!(
            "{}{}?sv=2021-08-06&se=2028-01-01T00%3A00%3A00Z&sr=c&sp=rw&sig={sig}",
            runner_base_url(), path
        ),
        "soft_size_limit": "1048576"
    })))
}

#[derive(Debug, Deserialize)]
pub(crate) struct StepSummaryMetadataRequest {
    // serde: metadata is accepted for protocol compatibility; this identifies the summary.
    pub(crate) step_backend_id: String,
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) workflow_job_run_backend_id: String,
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) workflow_run_backend_id: String,
    // serde: metadata is accepted for protocol compatibility; this records the summary size.
    pub(crate) size: Option<u64>,
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) uploaded_at: Option<String>,
}

pub(crate) async fn twirp_create_step_summary_metadata(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<StepSummaryMetadataRequest>,
) -> Json<serde_json::Value> {
    let byte_count = request.size.unwrap_or_default().min(usize::MAX as u64) as usize;
    let mut inner = shared.state.inner.lock().await;
    inner.log_metadata.insert(
        format!("summary:{}", request.step_backend_id),
        LogMetadata {
            byte_count,
            line_count: 0,
        },
    );
    let meta = crate::store::build_meta_snapshot(&inner);
    if let Err(error) = shared.state.store.store_meta_only(&meta).await {
        tracing::warn!(?error, "failed to persist step summary metadata");
    }

    Json(json!({"ok": true}))
}

#[derive(Debug, Deserialize)]
pub(crate) struct StepLogsMetadataRequest {
    // serde: metadata is accepted for protocol compatibility; this identifies the step.
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
    // serde: metadata is accepted for protocol compatibility; this records the line count.
    pub(crate) line_count: Option<u64>,
}

/// POST CreateStepLogsMetadata — runner calls this after uploading step logs.
pub(crate) async fn twirp_create_step_logs_metadata(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<StepLogsMetadataRequest>,
) -> Json<serde_json::Value> {
    if let Some(step_backend_id) = request.step_backend_id {
        let line_count = request.line_count.unwrap_or_default();
        let line_count_usize = line_count.min(usize::MAX as u64) as usize;
        let byte_count = line_count.saturating_mul(80).min(usize::MAX as u64) as usize;
        let mut inner = shared.state.inner.lock().await;
        inner.log_metadata.insert(
            format!("step:{step_backend_id}"),
            LogMetadata {
                byte_count,
                line_count: line_count_usize,
            },
        );
        let meta = crate::store::build_meta_snapshot(&inner);
        if let Err(error) = shared.state.store.store_meta_only(&meta).await {
            tracing::warn!(?error, "failed to persist step log metadata");
        }
    }

    Json(json!({"ok": true}))
}

#[derive(Debug, Deserialize)]
pub(crate) struct JobLogsMetadataRequest {
    // serde: metadata is accepted for protocol compatibility; this identifies the job.
    pub(crate) workflow_job_run_backend_id: Option<String>,
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) workflow_run_backend_id: Option<String>,
    // serde: metadata is accepted for protocol compatibility; field is not inspected.
    #[allow(dead_code)]
    pub(crate) upload_url: Option<String>,
    // serde: metadata is accepted for protocol compatibility; this records the line count.
    pub(crate) line_count: Option<u64>,
}

/// POST CreateJobLogsMetadata — runner calls this after uploading job logs.
pub(crate) async fn twirp_create_job_logs_metadata(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<JobLogsMetadataRequest>,
) -> Json<serde_json::Value> {
    if let Some(workflow_job_run_backend_id) = request.workflow_job_run_backend_id {
        let line_count = request.line_count.unwrap_or_default();
        let line_count_usize = line_count.min(usize::MAX as u64) as usize;
        let byte_count = line_count.saturating_mul(80).min(usize::MAX as u64) as usize;
        let mut inner = shared.state.inner.lock().await;
        inner.log_metadata.insert(
            format!("job:{workflow_job_run_backend_id}"),
            LogMetadata {
                byte_count,
                line_count: line_count_usize,
            },
        );
        let meta = crate::store::build_meta_snapshot(&inner);
        if let Err(error) = shared.state.store.store_meta_only(&meta).await {
            tracing::warn!(?error, "failed to persist job log metadata");
        }
    }

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

// ---------------------------------------------------------------------------
// Minimal protobuf (twirp) support for the cache routes.
//
// actions/cache@v4 speaks JSON, but sccache's GHA storage backend sends the
// twirp protobuf encoding (content-type `application/protobuf`) and rejects
// anything else with a 415.
//
// Official ghac / @actions/cache CacheService field numbers (do not "fix"
// the decoder to a flat key=1 layout — that breaks the wire format):
//   CreateCacheEntryRequest:         metadata=1 key=2 version=3
//   FinalizeCacheEntryUploadRequest: metadata=1 key=2 size_bytes=3 version=4
//   GetCacheEntryDownloadURLRequest: metadata=1 key=2 restore_keys=3 version=4
//   GetCacheEntryDownloadURLResponse: ok=1 signed_download_url=2 matched_key=3
//   CreateCacheEntryResponse:        ok=1 signed_upload_url=2
//   FinalizeCacheEntryUploadResponse: ok=1 entry_id=2
// Scope / repository live inside CacheMetadata (field 1), not as top-level
// key=1 style fields. See `pb_cache_request` below.

fn pb_varint(buf: &[u8], pos: usize) -> Option<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift = 0;
    let mut i = pos;
    while i < buf.len() && shift < 64 {
        let byte = buf[i];
        value |= u64::from(byte & 0x7f) << shift;
        i += 1;
        if byte & 0x80 == 0 {
            return Some((value, i));
        }
        shift += 7;
    }
    None
}

fn pb_varint_bytes(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
    out
}

/// Extract every length-delimited string value for one field number.
/// `None` on malformed wire data.
fn pb_strings(buf: &[u8], want: u64) -> Option<Vec<String>> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < buf.len() {
        let (tag, ni) = pb_varint(buf, i)?;
        let field = tag >> 3;
        match tag & 7 {
            0 => {
                let (_, ni2) = pb_varint(buf, ni)?;
                i = ni2;
            }
            2 => {
                let (len, ni2) = pb_varint(buf, ni)?;
                let start = ni2;
                let end = start.checked_add(len as usize)?;
                if end > buf.len() {
                    return None;
                }
                if field == want {
                    out.push(String::from_utf8_lossy(&buf[start..end]).into_owned());
                }
                i = end;
            }
            _ => return None,
        }
    }
    Some(out)
}

fn pb_string_field(field: u64, value: &str) -> Vec<u8> {
    let mut out = pb_varint_bytes((field << 3) | 2);
    out.extend(pb_varint_bytes(value.len() as u64));
    out.extend(value.as_bytes());
    out
}

type CachePbFields = (String, String, Vec<String>, Option<String>, Option<String>);

fn pb_cache_request(body: &[u8]) -> Result<CachePbFields, ApiError> {
    // ghac 0.2.0 field numbers: metadata=1 (message), key=2,
    // restore_keys=3, version=4 (GetCacheEntryDownloadURL /
    // FinalizeCacheEntryUpload) or 3 (CreateCacheEntry).
    let one = |v: Option<Vec<String>>| {
        v.and_then(|mut s| {
            if s.is_empty() {
                None
            } else {
                Some(s.remove(0))
            }
        })
    };
    let key = one(pb_strings(body, 2)).unwrap_or_default();
    let version = one(pb_strings(body, 4))
        .or_else(|| one(pb_strings(body, 3)))
        .unwrap_or_default();
    let restore_keys = pb_strings(body, 3).ok_or_else(|| ApiError::bad_request("bad protobuf"))?;
    // Scope lives inside the CacheMetadata message: metadata(1) ->
    // CacheMetadata.scope(2) -> CacheScope.scope(1).
    let scope = pb_strings(body, 1)
        .and_then(|mut v| {
            if v.is_empty() {
                None
            } else {
                Some(v.remove(0))
            }
        })
        .and_then(|meta| {
            pb_strings(meta.as_bytes(), 2).and_then(|mut v| {
                if v.is_empty() {
                    None
                } else {
                    Some(v.remove(0))
                }
            })
        })
        .and_then(|entry| {
            pb_strings(entry.as_bytes(), 1).and_then(|mut v| {
                if v.is_empty() {
                    None
                } else {
                    Some(v.remove(0))
                }
            })
        });
    let repository = None;
    Ok((key, version, restore_keys, scope, repository))
}

/// Protobuf varint field (wire type 0), for the `ok` / `entry_id` fields.
fn pb_uint_field(field: u64, value: u64) -> Vec<u8> {
    let mut out = pb_varint_bytes(field << 3);
    out.extend(pb_varint_bytes(value));
    out
}

fn pb_or_json(
    headers: &axum::http::HeaderMap,
    fields: Vec<Vec<u8>>,
    json: serde_json::Value,
) -> axum::response::Response {
    if headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.contains("protobuf"))
    {
        let body: Vec<u8> = fields.into_iter().flatten().collect();
        axum::response::Response::builder()
            .header(axum::http::header::CONTENT_TYPE, "application/protobuf")
            .body(axum::body::Body::from(body))
            .unwrap()
    } else {
        axum::Json(json).into_response()
    }
}

/// The twirp cache routes accept JSON (actions/cache@v4) and protobuf
/// (sccache's GHA storage backend). Returns `(key, version, restore_keys,
/// scope, repository)`.
fn cache_request_fields(
    headers: &axum::http::HeaderMap,
    body: &[u8],
) -> Result<CachePbFields, ApiError> {
    if headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.contains("protobuf"))
    {
        pb_cache_request(body)
    } else {
        let request: CacheV2GetDlUrlRequest = serde_json::from_slice(body)
            .map_err(|e| ApiError::bad_request(format!("invalid JSON request: {e}")))?;
        Ok((
            request.key,
            request.version,
            request.restore_keys,
            request.scope,
            request.repository,
        ))
    }
}

pub(crate) async fn twirp_cache_v2_create(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<axum::response::Response, ApiError> {
    crate::events::trust_tier::ensure_cache_write_allowed(&shared.state, &headers).await?;
    let (key, version, _restore, scope, repository) = cache_request_fields(&headers, &body)?;
    let storage_key = scoped_cache_key(key.as_str(), scope.as_deref(), repository.as_deref());
    if shared
        .state
        .cache
        .get(&storage_key, &version, &[])
        .await
        .map_err(|error| ApiError::internal(format!("cache lookup error: {error}")))?
        .is_some()
    {
        return Ok(pb_or_json(
            &headers,
            vec![pb_uint_field(1, 0), pb_string_field(2, "")],
            json!({
                "ok": false,
                "signed_upload_url": "",
                "message": "cache already exists"
            }),
        ));
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
            .any(|pending| pending.key == storage_key && pending.version == version)
        {
            true
        } else {
            inner.cache_v2_pending.insert(
                token.clone(),
                CacheV2Pending {
                    key: storage_key,
                    version,
                },
            );
            let meta = crate::store::build_meta_snapshot(&inner);
            if let Err(error) = shared.state.store.store_meta_only(&meta).await {
                tracing::warn!(?error, "failed to persist cache v2 reservation");
            }
            false
        }
    };
    if already_reserved {
        let _ = tokio::fs::remove_dir_all(&stage_dir).await;
        return Ok(pb_or_json(
            &headers,
            vec![pb_uint_field(1, 0), pb_string_field(2, "")],
            json!({
                "ok": false,
                "signed_upload_url": "",
                "message": "cache upload already reserved"
            }),
        ));
    }
    let upload_url = format!("{}/twirp-blob/cache/{token}", runner_base_url());
    info!(token, "cache v2 create entry");
    Ok(pb_or_json(
        &headers,
        vec![pb_uint_field(1, 1), pb_string_field(2, &upload_url)],
        json!({ "ok": true, "signed_upload_url": upload_url, "message": "" }),
    ))
}

pub(crate) async fn twirp_cache_v2_finalize(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<axum::response::Response, ApiError> {
    crate::events::trust_tier::ensure_cache_write_allowed(&shared.state, &headers).await?;
    let t0 = std::time::Instant::now();
    let (key, version, _restore, scope, repository) = cache_request_fields(&headers, &body)?;
    let storage_key = scoped_cache_key(key.as_str(), scope.as_deref(), repository.as_deref());
    // Find the pending upload token matching key+version.
    let token = {
        let inner = shared.state.inner.lock().await;
        inner
            .cache_v2_pending
            .iter()
            .find(|(_, p)| p.key == storage_key && p.version == version)
            .map(|(k, _)| k.clone())
    };
    let Some(token) = token else {
        // If no pending upload exists, check if the cache entry already exists.
        // This happens when CreateCacheEntry returned "cache already exists".
        if shared
            .state
            .cache
            .get(&storage_key, &version, &[])
            .await
            .map_err(|error| ApiError::internal(format!("cache lookup error: {error}")))?
            .is_some()
        {
            return Ok(pb_or_json(
                &headers,
                vec![pb_uint_field(1, 1), pb_uint_field(2, 1)],
                json!({ "ok": true, "entry_id": "1", "message": "" }),
            ));
        }
        return Err(ApiError::not_found(
            "no pending cache upload for key+version",
        ));
    };

    let blob_path = shared
        .state
        .state_dir
        .join("blobs")
        .join("cache")
        .join(&token)
        .join("data");
    let t_read = std::time::Instant::now();
    let bytes = tokio::fs::read(&blob_path).await.map_err(|e| {
        ApiError::not_found(format!("cache blob not found (not yet uploaded?): {e}"))
    })?;
    let read_ms = t_read.elapsed().as_millis();

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
        let meta = crate::store::build_meta_snapshot(&inner);
        if let Err(error) = shared.state.store.store_meta_only(&meta).await {
            tracing::warn!(?error, "failed to persist cache v2 finalization");
        }
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

    let total_ms = t0.elapsed().as_millis();
    tracing::info!(
        key,
        version,
        size = bytes.len(),
        read_ms,
        total_ms,
        "cache v2 finalized"
    );
    Ok(pb_or_json(
        &headers,
        vec![pb_uint_field(1, 1), pb_uint_field(2, 1)],
        json!({ "ok": true, "entry_id": "1", "message": "" }),
    ))
}

pub(crate) async fn twirp_cache_v2_get_dl_url(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<axum::response::Response, ApiError> {
    let t0 = std::time::Instant::now();
    let (key, version, restore_keys, scope, repository) = cache_request_fields(&headers, &body)?;
    let storage_key = scoped_cache_key(&key, scope.as_deref(), repository.as_deref());
    let storage_restore_keys = restore_keys
        .iter()
        .map(|key| scoped_cache_key(key, scope.as_deref(), repository.as_deref()))
        .collect::<Vec<_>>();
    let t_lookup = std::time::Instant::now();
    let result = shared
        .state
        .cache
        .get(&storage_key, &version, &storage_restore_keys)
        .await
        .map_err(|e| ApiError::internal(format!("cache lookup error: {e}")))?;
    let lookup_ms = t_lookup.elapsed().as_millis();

    let (entry, _bytes) = match result {
        Some(r) => r,
        None => {
            tracing::info!(
                key = %key,
                version = %version,
                lookup_ms,
                outcome = "miss",
                "cache restore"
            );
            return Ok(pb_or_json(
                &headers,
                vec![
                    pb_uint_field(1, 0),
                    pb_string_field(2, ""),
                    pb_string_field(3, ""),
                ],
                json!({ "ok": false, "signed_download_url": "", "matched_key": "" }),
            ));
        }
    };

    let dl_token = uuid::Uuid::new_v4().to_string();
    {
        let mut inner = shared.state.inner.lock().await;
        inner
            .cache_v2_dl_tokens
            .insert(dl_token.clone(), (entry.key.clone(), entry.version.clone()));
    }
    let download_url = format!("{}/twirp-blob/cache/{dl_token}", runner_base_url());
    let matched_key = entry
        .key
        .split_once('\0')
        .map(|(_, key)| key.to_owned())
        .unwrap_or_else(|| entry.key.clone());
    let total_ms = t0.elapsed().as_millis();
    tracing::info!(
        key = %matched_key,
        version = %version,
        size = entry.size,
        lookup_ms,
        total_ms,
        outcome = "hit",
        "cache restore"
    );
    Ok(pb_or_json(
        &headers,
        vec![
            pb_uint_field(1, 1),
            pb_string_field(2, &download_url),
            pb_string_field(3, &matched_key),
        ],
        json!({
            "ok": true,
            "signed_download_url": download_url,
            "matched_key": matched_key
        }),
    ))
}

#[cfg(test)]
mod cache_pb_tests {
    use super::*;

    #[test]
    fn pb_roundtrip_decodes_sccache_style_request() {
        // ghac GetCacheEntryDownloadURLRequest: metadata=1 key=2
        // restore_keys=3 version=4; scope nested in metadata.
        let mut body = pb_string_field(2, ".sccache_check");
        body.extend(pb_string_field(3, "restore-a"));
        body.extend(pb_string_field(4, "abc123"));
        // metadata(1) -> CacheMetadata.scope(2) -> CacheScope.scope(1)
        let scope_entry = pb_string_field(1, "refs/heads/main");
        let meta = pb_string_field(2, &String::from_utf8(scope_entry).unwrap());
        body.extend(pb_string_field(1, &String::from_utf8(meta).unwrap()));
        let (key, version, restore, scope, repository) = pb_cache_request(&body).unwrap();
        assert_eq!(key, ".sccache_check");
        assert_eq!(version, "abc123");
        assert_eq!(restore, vec!["restore-a"]);
        assert_eq!(scope.as_deref(), Some("refs/heads/main"));
        assert_eq!(repository, None);
    }

    #[test]
    fn pb_response_encoding_roundtrips_through_decoder() {
        // The sccache client decodes what we encode; make sure our encoder
        // produces fields our decoder reads back identically.
        let wire: Vec<u8> = pb_string_field(1, "https://dl.example/x")
            .into_iter()
            .chain(pb_string_field(2, "hit-key"))
            .collect();
        let (url, matched) = (pb_strings(&wire, 1).unwrap(), pb_strings(&wire, 2).unwrap());
        assert_eq!(url, vec!["https://dl.example/x"]);
        assert_eq!(matched, vec!["hit-key"]);
    }

    #[test]
    fn pb_or_json_returns_protobuf_for_protobuf_clients() {
        let headers = axum::http::HeaderMap::from_iter([(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/protobuf"),
        )]);
        let out = pb_or_json(
            &headers,
            vec![pb_string_field(1, "k")],
            serde_json::json!({}),
        );
        assert_eq!(
            out.headers().get(axum::http::header::CONTENT_TYPE).unwrap(),
            "application/protobuf"
        );
    }
}
