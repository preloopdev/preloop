use super::*;
use prost::Message;

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
                        let duplicate_name = !name.is_empty()
                            && steps
                                .iter()
                                .filter(|candidate| candidate["name"].as_str() == Some(&name))
                                .count()
                                > 1;

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

                        if let Some(pos) = StepRecord::find_matching_index(
                            &job_detail.steps,
                            external_id_str,
                            &name,
                            !duplicate_name,
                        ) {
                            if !external_id_str.is_empty() {
                                job_detail.steps[pos].id = Some(external_id_str.to_owned());
                            }
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
                                id: (!external_id_str.is_empty())
                                    .then(|| external_id_str.to_owned()),
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

/// SHA-256 digest of a scoped cache key + version. The cache key is
/// workflow-controlled content; log the digest (plus `version_len`) instead
/// of the raw key/version so entries correlate across the create/finalize
/// log records while leaking nothing.
fn cache_id_digest(key: &str, version: &str) -> String {
    use sha2::Digest;
    format!(
        "{:x}",
        sha2::Sha256::digest(format!("{key}\u{0}{version}").as_bytes())
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
// Protobuf (Twirp) support for the cache routes.
//
// actions/cache@v4 speaks JSON, but sccache's GHA storage backend sends the
// twirp protobuf encoding (content-type `application/protobuf`) and rejects
// anything else with a 415.
//
// Official ghac / @actions/cache CacheService field numbers:
//   CreateCacheEntryRequest:         metadata=1 key=2 version=3
//   FinalizeCacheEntryUploadRequest: metadata=1 key=2 size_bytes=3 version=4
//   GetCacheEntryDownloadURLRequest: metadata=1 key=2 restore_keys=3 version=4
//   GetCacheEntryDownloadURLResponse: ok=1 signed_download_url=2 matched_key=3
//   CreateCacheEntryResponse:        ok=1 signed_upload_url=2
//   FinalizeCacheEntryUploadResponse: ok=1 entry_id=2
// Scope / repository live inside CacheMetadata (field 1), not as top-level
// fields. Prost ignores unknown fields, as required by protobuf forward
// compatibility, while still rejecting malformed wire data.

#[derive(Clone, PartialEq, Message)]
struct PbCacheScope {
    #[prost(string, tag = "1")]
    scope: String,
    #[prost(int64, tag = "2")]
    permission: i64,
}

#[derive(Clone, PartialEq, Message)]
struct PbCacheMetadata {
    #[prost(int64, tag = "1")]
    repository_id: i64,
    #[prost(message, repeated, tag = "2")]
    scope: Vec<PbCacheScope>,
}

#[derive(Clone, PartialEq, Message)]
struct PbCreateCacheEntryRequest {
    #[prost(message, optional, tag = "1")]
    metadata: Option<PbCacheMetadata>,
    #[prost(string, tag = "2")]
    key: String,
    #[prost(string, tag = "3")]
    version: String,
}

#[derive(Clone, PartialEq, Message)]
struct PbFinalizeCacheEntryUploadRequest {
    #[prost(message, optional, tag = "1")]
    metadata: Option<PbCacheMetadata>,
    #[prost(string, tag = "2")]
    key: String,
    #[prost(int64, tag = "3")]
    size_bytes: i64,
    #[prost(string, tag = "4")]
    version: String,
}

#[derive(Clone, PartialEq, Message)]
struct PbGetCacheEntryDownloadUrlRequest {
    #[prost(message, optional, tag = "1")]
    metadata: Option<PbCacheMetadata>,
    #[prost(string, tag = "2")]
    key: String,
    #[prost(string, repeated, tag = "3")]
    restore_keys: Vec<String>,
    #[prost(string, tag = "4")]
    version: String,
}

#[derive(Clone, PartialEq, Message)]
struct PbCreateCacheEntryResponse {
    #[prost(bool, tag = "1")]
    ok: bool,
    #[prost(string, tag = "2")]
    signed_upload_url: String,
}

#[derive(Clone, PartialEq, Message)]
struct PbFinalizeCacheEntryUploadResponse {
    #[prost(bool, tag = "1")]
    ok: bool,
    #[prost(int64, tag = "2")]
    entry_id: i64,
}

#[derive(Clone, PartialEq, Message)]
struct PbGetCacheEntryDownloadUrlResponse {
    #[prost(bool, tag = "1")]
    ok: bool,
    #[prost(string, tag = "2")]
    signed_download_url: String,
    #[prost(string, tag = "3")]
    matched_key: String,
}

type CachePbFields = (String, String, Vec<String>, Vec<String>, Option<String>);

#[derive(Clone, Copy)]
enum CacheRequestKind {
    Create,
    Finalize,
    GetDownloadUrl,
}

fn metadata_fields(metadata: Option<PbCacheMetadata>) -> (Vec<String>, Option<String>) {
    let Some(metadata) = metadata else {
        return (Vec::new(), None);
    };
    let repository = (metadata.repository_id > 0).then(|| metadata.repository_id.to_string());
    let scopes = metadata
        .scope
        .into_iter()
        .filter_map(|scope| (!scope.scope.is_empty()).then_some(scope.scope))
        .collect();
    (scopes, repository)
}

fn validate_cache_identity(key: &str, version: &str) -> Result<(), ApiError> {
    if key.is_empty() || version.is_empty() {
        return Err(ApiError::bad_request("cache key and version are required"));
    }
    Ok(())
}

fn pb_cache_request(body: &[u8], kind: CacheRequestKind) -> Result<CachePbFields, ApiError> {
    match kind {
        CacheRequestKind::Create => {
            let request = PbCreateCacheEntryRequest::decode(body).map_err(|error| {
                ApiError::bad_request(format!("invalid protobuf request: {error}"))
            })?;
            validate_cache_identity(&request.key, &request.version)?;
            let (scopes, repository) = metadata_fields(request.metadata);
            Ok((request.key, request.version, Vec::new(), scopes, repository))
        }
        CacheRequestKind::Finalize => {
            let request = PbFinalizeCacheEntryUploadRequest::decode(body).map_err(|error| {
                ApiError::bad_request(format!("invalid protobuf request: {error}"))
            })?;
            validate_cache_identity(&request.key, &request.version)?;
            let (scopes, repository) = metadata_fields(request.metadata);
            Ok((request.key, request.version, Vec::new(), scopes, repository))
        }
        CacheRequestKind::GetDownloadUrl => {
            let request = PbGetCacheEntryDownloadUrlRequest::decode(body).map_err(|error| {
                ApiError::bad_request(format!("invalid protobuf request: {error}"))
            })?;
            validate_cache_identity(&request.key, &request.version)?;
            let (scopes, repository) = metadata_fields(request.metadata);
            Ok((
                request.key,
                request.version,
                request.restore_keys,
                scopes,
                repository,
            ))
        }
    }
}

fn pb_or_json<M: Message>(
    headers: &axum::http::HeaderMap,
    protobuf: M,
    json: serde_json::Value,
) -> axum::response::Response {
    if headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.contains("protobuf"))
    {
        let body = protobuf.encode_to_vec();
        axum::response::Response::builder()
            .header(axum::http::header::CONTENT_TYPE, "application/protobuf")
            .body(axum::body::Body::from(body))
            .unwrap()
    } else {
        axum::Json(json).into_response()
    }
}

/// The twirp cache routes accept JSON (actions/cache@v4) and protobuf
/// (sccache's GHA storage backend). Returns `(key, version, restore_keys, scopes, repository)`.
fn cache_request_fields(
    headers: &axum::http::HeaderMap,
    body: &[u8],
    kind: CacheRequestKind,
) -> Result<CachePbFields, ApiError> {
    if headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.contains("protobuf"))
    {
        pb_cache_request(body, kind)
    } else {
        let request: CacheV2GetDlUrlRequest = serde_json::from_slice(body)
            .map_err(|e| ApiError::bad_request(format!("invalid JSON request: {e}")))?;
        validate_cache_identity(&request.key, &request.version)?;
        Ok((
            request.key,
            request.version,
            request.restore_keys,
            request.scope.into_iter().collect::<Vec<_>>(),
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
    let (key, version, _restore, scopes, repository) =
        cache_request_fields(&headers, &body, CacheRequestKind::Create)?;
    let scope = scopes.first().map(String::as_str);
    let storage_key = scoped_cache_key(key.as_str(), scope, repository.as_deref());
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
            PbCreateCacheEntryResponse {
                ok: false,
                signed_upload_url: String::new(),
            },
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
        let job_backend_id = job_backend_id_from_bearer(&shared.state, &headers);
        if inner
            .cache_v2_pending
            .values()
            .any(|pending| pending.key == storage_key && pending.version == version)
        {
            true
        } else {
            // F7: a runner is capped at MAX_PENDING_PER_JOB in-flight cache
            // uploads. The job comes from the signed token scope, not the
            // request body. A refusal is a plain `ok: false`, the same
            // non-fatal shape actions/cache already handles for a miss.
            if let Some(job_id) = &job_backend_id {
                let pending = inner
                    .cache_v2_pending
                    .values()
                    .filter(|pending| &pending.job_backend_id == job_id)
                    .count();
                if pending >= MAX_PENDING_PER_JOB {
                    // Drop the stage dir we just created — the pending map never
                    // learns this token, so the sweeper can't find it.
                    let dir = stage_dir.clone();
                    drop(inner);
                    let _ = tokio::fs::remove_dir_all(&dir).await;
                    return Ok(pb_or_json(
                        &headers,
                        PbCreateCacheEntryResponse {
                            ok: false,
                            signed_upload_url: String::new(),
                        },
                        json!({
                            "ok": false,
                            "signed_upload_url": "",
                            "message": format!(
                                "job has {pending} pending cache uploads (cap {MAX_PENDING_PER_JOB})"
                            )
                        }),
                    ));
                }
            }
            inner.cache_v2_pending.insert(
                token.clone(),
                CacheV2Pending {
                    key: storage_key.clone(),
                    version: version.clone(),
                    job_backend_id: job_backend_id.unwrap_or_default(),
                    created_unix: now_unix(),
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
            PbCreateCacheEntryResponse {
                ok: false,
                signed_upload_url: String::new(),
            },
            json!({
                "ok": false,
                "signed_upload_url": "",
                "message": "cache upload already reserved"
            }),
        ));
    }
    let upload_url = format!("{}/twirp-blob/cache/{token}", runner_base_url());
    // The cache key is workflow-controlled content; never log it or the
    // version verbatim. A SHA-256 digest identifies the entry well enough to
    // correlate with the finalize/restore logs while leaking nothing.
    let cache_id = cache_id_digest(&storage_key, &version);
    info!(
        cache_id = %cache_id,
        version_len = version.len(),
        "cache v2 create entry"
    );
    Ok(pb_or_json(
        &headers,
        PbCreateCacheEntryResponse {
            ok: true,
            signed_upload_url: upload_url.clone(),
        },
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
    let (key, version, _restore, scopes, repository) =
        cache_request_fields(&headers, &body, CacheRequestKind::Finalize)?;
    let scope = scopes.first().map(String::as_str);
    let storage_key = scoped_cache_key(key.as_str(), scope, repository.as_deref());
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
                PbFinalizeCacheEntryUploadResponse {
                    ok: true,
                    entry_id: 1,
                },
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
    // Match the create record: log the digest + length, never the raw
    // workflow-controlled key/version.
    let cache_id = cache_id_digest(&key, &version);
    tracing::info!(
        cache_id = %cache_id,
        version_len = version.len(),
        size = bytes.len(),
        read_ms,
        total_ms,
        "cache v2 finalized"
    );
    Ok(pb_or_json(
        &headers,
        PbFinalizeCacheEntryUploadResponse {
            ok: true,
            entry_id: 1,
        },
        json!({ "ok": true, "entry_id": "1", "message": "" }),
    ))
}

pub(crate) async fn twirp_cache_v2_get_dl_url(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Result<axum::response::Response, ApiError> {
    let t0 = std::time::Instant::now();
    let (key, version, restore_keys, scopes, repository) =
        cache_request_fields(&headers, &body, CacheRequestKind::GetDownloadUrl)?;
    // Try each scope in wire order until a cache hit. This preserves
    // authorization through any later scope: a download authorized via
    // `refs/heads/feature` must not fail because `refs/heads/main` was first
    // and misses. If no scopes were supplied, try the unscoped default once.
    let primary_scopes: Vec<Option<String>> = if scopes.is_empty() {
        vec![None]
    } else {
        scopes.into_iter().map(Some).collect()
    };
    let mut hit: Option<(preloop_cache::CacheEntry, Vec<u8>)> = None;
    let mut lookup_ms: u128 = 0;
    for primary in &primary_scopes {
        let storage_key = scoped_cache_key(&key, primary.as_deref(), repository.as_deref());
        let storage_restore_keys = restore_keys
            .iter()
            .map(|rk| scoped_cache_key(rk, primary.as_deref(), repository.as_deref()))
            .collect::<Vec<_>>();
        let t_lookup = std::time::Instant::now();
        let result = shared
            .state
            .cache
            .get(&storage_key, &version, &storage_restore_keys)
            .await
            .map_err(|e| ApiError::internal(format!("cache lookup error: {e}")))?;
        lookup_ms = t_lookup.elapsed().as_millis();
        if result.is_some() {
            hit = result;
            break;
        }
    }
    let Some((entry, _bytes)) = hit else {
        tracing::info!(
            key = %key,
            version = %version,
            lookup_ms,
            outcome = "miss",
            "cache restore"
        );
        return Ok(pb_or_json(
            &headers,
            PbGetCacheEntryDownloadUrlResponse {
                ok: false,
                signed_download_url: String::new(),
                matched_key: String::new(),
            },
            json!({ "ok": false, "signed_download_url": "", "matched_key": "" }),
        ));
    };

    let dl_token = uuid::Uuid::new_v4().to_string();
    {
        let mut inner = shared.state.inner.lock().await;
        inner
            .cache_v2_dl_tokens
            .insert(dl_token.clone(), (entry.key.clone(), entry.version.clone()));
        // F7: bound the minted-token map; the oldest tokens are evicted
        // first. A token that a runner has not yet fetched still works, so a
        // real workflow's few concurrent downloads are never affected.
        inner.cache_v2_dl_tokens_order.push_back(dl_token.clone());
        inner
            .cache_v2_dl_tokens_created
            .insert(dl_token.clone(), now_unix());
        trim_cache_dl_tokens(&mut inner);
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
        PbGetCacheEntryDownloadUrlResponse {
            ok: true,
            signed_download_url: download_url.clone(),
            matched_key: matched_key.clone(),
        },
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

    const SCCACHE_CREATE_FIXTURE: &[u8] =
        include_bytes!("../../../fixtures/wire/cache-create-sccache.pb");
    const SCCACHE_GET_FIXTURE: &[u8] =
        include_bytes!("../../../fixtures/wire/cache-get-sccache.pb");

    #[test]
    fn pb_roundtrip_decodes_sccache_style_request() {
        let request = PbGetCacheEntryDownloadUrlRequest {
            metadata: Some(PbCacheMetadata {
                repository_id: 42,
                scope: vec![PbCacheScope {
                    scope: "refs/heads/main".to_string(),
                    permission: 1,
                }],
            }),
            key: ".sccache_check".to_string(),
            restore_keys: vec!["restore-a".to_string()],
            version: "abc123".to_string(),
        };
        let mut body = request.encode_to_vec();
        // Unknown fields must be ignored for protobuf forward compatibility.
        body.extend([0x28, 0x01]); // field 5, varint 1
        let (key, version, restore, scopes, repository) =
            pb_cache_request(&body, CacheRequestKind::GetDownloadUrl).unwrap();
        assert_eq!(key, ".sccache_check");
        assert_eq!(version, "abc123");
        assert_eq!(restore, vec!["restore-a"]);
        assert_eq!(scopes, vec!["refs/heads/main"]);
        assert_eq!(repository.as_deref(), Some("42"));
    }

    #[test]
    fn pb_golden_sccache_create_fixture_decodes() {
        // Correct fixture: bytes generated with prost using official field
        // numbers (metadata=1, key=2, version=3). This is the failing pre-fix
        // exchange: hand-rolled flat key=1 decoder cannot parse it, prost does.
        let (key, version, restore, scopes, repository) =
            pb_cache_request(SCCACHE_CREATE_FIXTURE, CacheRequestKind::Create).unwrap();
        assert_eq!(key, ".sccache_check");
        assert_eq!(version, "abc123");
        assert!(restore.is_empty());
        assert_eq!(scopes, vec!["refs/heads/main"]);
        assert_eq!(repository.as_deref(), Some("42"));
    }

    #[test]
    fn pb_golden_sccache_get_fixture_decodes_with_unknown_field() {
        // Includes trailing unknown field 5 (0x28 0x01) — must be ignored.
        let (key, version, restore, scopes, repository) =
            pb_cache_request(SCCACHE_GET_FIXTURE, CacheRequestKind::GetDownloadUrl).unwrap();
        assert_eq!(key, ".sccache_check");
        assert_eq!(version, "abc123");
        assert_eq!(restore, vec!["restore-a"]);
        assert_eq!(scopes, vec!["refs/heads/main"]);
        assert_eq!(repository.as_deref(), Some("42"));
    }

    #[test]
    fn pb_multi_scope_preserves_all_scopes_in_wire_order() {
        let request = PbGetCacheEntryDownloadUrlRequest {
            metadata: Some(PbCacheMetadata {
                repository_id: 42,
                scope: vec![
                    PbCacheScope {
                        scope: "refs/heads/main".to_string(),
                        permission: 1,
                    },
                    PbCacheScope {
                        scope: "refs/heads/feature".to_string(),
                        permission: 2,
                    },
                ],
            }),
            key: "k".to_string(),
            restore_keys: vec![],
            version: "v".to_string(),
        };
        let (key, version, restore, scopes, repository) =
            pb_cache_request(&request.encode_to_vec(), CacheRequestKind::GetDownloadUrl).unwrap();
        assert_eq!(key, "k");
        assert_eq!(version, "v");
        assert!(restore.is_empty());
        assert_eq!(scopes, vec!["refs/heads/main", "refs/heads/feature"]);
        assert_eq!(repository.as_deref(), Some("42"));
        // Also verify the golden multi-scope fixture decodes identically
        let fixture = include_bytes!("../../../fixtures/wire/cache-multi-scope.pb");
        let (_, _, _, fixture_scopes, _) =
            pb_cache_request(fixture, CacheRequestKind::GetDownloadUrl).unwrap();
        assert_eq!(
            fixture_scopes,
            vec!["refs/heads/main", "refs/heads/feature"]
        );
    }

    #[test]
    fn pb_request_schemas_use_their_distinct_version_fields() {
        let create = PbCreateCacheEntryRequest {
            metadata: None,
            key: "k".to_string(),
            version: "create-v".to_string(),
        };
        let finalize = PbFinalizeCacheEntryUploadRequest {
            metadata: None,
            key: "k".to_string(),
            size_bytes: 123,
            version: "finalize-v".to_string(),
        };
        let create_fields =
            pb_cache_request(&create.encode_to_vec(), CacheRequestKind::Create).unwrap();
        let finalize_fields =
            pb_cache_request(&finalize.encode_to_vec(), CacheRequestKind::Finalize).unwrap();
        assert_eq!(create_fields.1, "create-v");
        assert_eq!(finalize_fields.1, "finalize-v");
        assert!(finalize_fields.2.is_empty());
    }

    #[test]
    fn pb_request_rejects_missing_identity() {
        let request = PbCreateCacheEntryRequest {
            metadata: None,
            key: String::new(),
            version: "v".to_string(),
        };
        assert!(pb_cache_request(&request.encode_to_vec(), CacheRequestKind::Create).is_err());
    }

    #[test]
    fn pb_or_json_returns_protobuf_for_protobuf_clients() {
        let headers = axum::http::HeaderMap::from_iter([(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/protobuf"),
        )]);
        let out = pb_or_json(
            &headers,
            PbGetCacheEntryDownloadUrlResponse {
                ok: true,
                signed_download_url: "https://dl.example/x".to_string(),
                matched_key: "k".to_string(),
            },
            serde_json::json!({}),
        );
        assert_eq!(
            out.headers().get(axum::http::header::CONTENT_TYPE).unwrap(),
            "application/protobuf"
        );
    }
}
