use super::*;

#[derive(Debug, Deserialize)]
pub(crate) struct CachePutRequest {
    key: String,
    version: String,
    #[serde(default)]
    content_base64: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CacheQuery {
    key: Option<String>,
    keys: Option<String>,
    version: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct CacheLookupResponse {
    hit: bool,
    key: Option<String>,
    version: Option<String>,
    size: Option<u64>,
    content_base64: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CacheReserveRequest {
    key: String,
    version: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CacheReserveResponse {
    cache_id: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CacheCommitRequest {
    #[serde(default)]
    size: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ArtifactPutRequest {
    run_id: RunId,
    name: String,
    file_name: String,
    #[serde(default)]
    content_base64: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ArtifactCreateRequest {
    name: String,
    #[serde(default = "default_artifact_file_name")]
    file_name: String,
}

fn default_artifact_file_name() -> String {
    "artifact.bin".to_owned()
}

/// R1-5: include the job's git ref in the legacy cache namespace so one
/// branch cannot poison another branch's entries (first write wins on an
/// exact key+version). The system token keeps the historical
/// repository-only namespace.
pub(crate) fn ref_scoped_namespace(repository: Option<String>, git_ref: Option<String>) -> String {
    match (repository, git_ref) {
        (Some(repository), Some(git_ref)) => format!("{repository}\0{git_ref}"),
        (Some(repository), None) => repository,
        (None, _) => String::new(),
    }
}

/// R1-6: refuse a chunk that would push an in-flight upload past
/// `max_bytes`. Split from `cache_upload` so tests can exercise the
/// boundary with a small limit instead of allocating the production
/// 512 MiB cap.
pub(crate) fn ensure_cache_chunk_fits(
    current_bytes: u64,
    chunk_bytes: u64,
    max_bytes: u64,
) -> Result<(), ApiError> {
    if current_bytes + chunk_bytes > max_bytes {
        return Err(ApiError::payload_too_large(format!(
            "cache upload exceeds the {} MiB per-upload cap",
            max_bytes / (1024 * 1024)
        )));
    }
    Ok(())
}

/// R1-6: refuse a chunk that would push a job's *aggregate* pending bytes
/// past `max_bytes`. The per-upload cap alone still lets a job hold
/// `MAX_PENDING_PER_JOB` × 512 MiB; this bounds the job's total. Split out
/// so tests can exercise the boundary with a small limit instead of
/// allocating the production 1 GiB budget.
pub(crate) fn ensure_pending_cache_bytes_fit(
    inner: &InnerState,
    job_backend_id: &str,
    chunk_bytes: u64,
    max_bytes: u64,
) -> Result<(), ApiError> {
    let job_bytes: u64 = inner
        .pending_caches
        .values()
        .filter(|pending| pending.job_backend_id == job_backend_id)
        .map(|pending| pending.bytes.len() as u64)
        .sum();
    if job_bytes + chunk_bytes > max_bytes {
        return Err(ApiError::payload_too_large(format!(
            "job exceeds the {} MiB pending-cache byte budget",
            max_bytes / (1024 * 1024)
        )));
    }
    Ok(())
}

pub(crate) async fn cache_put(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<CachePutRequest>,
) -> Result<Json<CacheLookupResponse>, ApiError> {
    let bytes = decode_base64(&request.content_base64)?;
    let entry = shared
        .state
        .cache
        .put(&request.key, &request.version, &bytes)
        .await?;
    Ok(Json(CacheLookupResponse {
        hit: true,
        key: Some(entry.key),
        version: Some(entry.version),
        size: Some(entry.size),
        content_base64: None,
    }))
}

pub(crate) async fn cache_get(
    State(shared): State<Arc<SharedState>>,
    Query(query): Query<CacheQuery>,
) -> Result<Json<CacheLookupResponse>, ApiError> {
    let key = query.key.unwrap_or_default();
    let restore_keys = parse_restore_keys(query.keys.as_deref());
    let Some((entry, bytes)) = shared
        .state
        .cache
        .get(&key, &query.version, &restore_keys)
        .await?
    else {
        return Ok(Json(CacheLookupResponse {
            hit: false,
            key: None,
            version: None,
            size: None,
            content_base64: None,
        }));
    };
    Ok(Json(CacheLookupResponse {
        hit: true,
        key: Some(entry.key),
        version: Some(entry.version),
        size: Some(entry.size),
        content_base64: Some(BASE64_STANDARD.encode(bytes)),
    }))
}

pub(crate) async fn cache_reserve(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<CacheReserveRequest>,
) -> Result<Json<CacheReserveResponse>, ApiError> {
    crate::events::trust_tier::ensure_cache_write_allowed(&shared.state, &headers).await?;
    let repository = auth::job_repository_from_headers(&shared.state, &headers).await?;
    let git_ref = auth::job_git_ref_from_headers(&shared.state, &headers).await?;
    let claims = auth::job_runtime_claims_from_headers(&shared.state, &headers);
    // R1-10: a stale job token must not reserve new uploads after its job
    // completes. The system bearer manages the lifecycle itself and bypasses;
    // every other caller must present a resolvable job identity — a request
    // without claims cannot be attributed to a live job.
    if !auth::system_bearer_authorized(&shared.state, &headers) {
        let Some(claims) = claims.as_ref() else {
            return Err(ApiError::unauthorized(
                "job token required for cache writes",
            ));
        };
        auth::require_live_job(&shared.state, claims.job_id).await?;
    }
    let job_uuid = claims.map(|claims| claims.job_id);
    let job_backend_id = job_uuid.map(|uuid| uuid.to_string()).unwrap_or_default();
    let mut inner = shared.state.inner.lock().await;
    // In-lock re-check: the job may have settled between the gate above and
    // this lock — a settled job must not mint a fresh reservation.
    if let Some(job_uuid) = job_uuid {
        if !auth::job_is_live_locked(&inner, job_uuid) {
            return Err(ApiError::forbidden(
                "job is not live; writes are rejected for completed or unknown jobs",
            ));
        }
    }
    // R1-6: bound in-flight legacy reservations per job, mirroring the v2
    // path's MAX_PENDING_PER_JOB. Without it a job could accumulate
    // unbounded reservation state in server RAM.
    if !job_backend_id.is_empty() {
        let in_flight = inner
            .pending_caches
            .values()
            .filter(|pending| pending.job_backend_id == job_backend_id)
            .count();
        if in_flight >= MAX_PENDING_PER_JOB {
            return Err(ApiError::bad_request(format!(
                "job has {in_flight} pending cache uploads (cap {MAX_PENDING_PER_JOB})"
            )));
        }
    }
    inner.next_cache_id += 1;
    let cache_id = inner.next_cache_id;
    inner.pending_caches.insert(
        cache_id,
        PendingCache {
            key: request.key,
            // R1-5: the reservation is bound to the job's git ref, not just
            // the repository, so a branch run cannot squat another branch's
            // key namespace.
            namespace: ref_scoped_namespace(repository, git_ref),
            version: request.version,
            bytes: Vec::new(),
            job_backend_id,
            // R1-6: stamp the reservation so the TTL sweeper can free it if
            // the job never commits (previously abandoned reservations held
            // their bytes forever).
            created_unix: crate::memory_caps::now_unix(),
        },
    );
    let meta = crate::store::build_meta_snapshot(&inner);
    if let Err(error) = shared.state.store.store_meta_only(&meta).await {
        tracing::warn!(?error, "failed to persist cache reservation");
    }
    Ok(Json(CacheReserveResponse { cache_id }))
}

pub(crate) async fn cache_upload(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Path(cache_id): Path<i64>,
    bytes: Bytes,
) -> Result<StatusCode, ApiError> {
    crate::events::trust_tier::ensure_cache_write_allowed(&shared.state, &headers).await?;
    let claims = auth::job_runtime_claims_from_headers(&shared.state, &headers);
    let caller_job_id = claims.as_ref().map(|claims| claims.job_id.to_string());
    let system = auth::system_bearer_authorized(&shared.state, &headers);
    // R1-10: a stale job token must not keep uploading after its job
    // completes. The system bearer manages the lifecycle itself and bypasses.
    if !system {
        let Some(claims) = claims else {
            return Err(ApiError::unauthorized(
                "job token required for cache writes",
            ));
        };
        auth::require_live_job(&shared.state, claims.job_id).await?;
    }
    let mut inner = shared.state.inner.lock().await;
    {
        let pending = inner
            .pending_caches
            .get(&cache_id)
            .ok_or_else(|| ApiError::not_found("cache reservation not found"))?;
        if !system && pending.job_backend_id != caller_job_id.unwrap_or_default() {
            return Err(ApiError::forbidden(
                "cache reservation belongs to another job",
            ));
        }
        // R1-6: cap each in-flight upload's running total. Without this
        // check a job could grow server RAM without bound by PATCHing
        // chunks forever (~500 requests/GiB at the 2 MiB default body
        // limit). The check runs before the vector grows so the refusal
        // itself allocates nothing.
        ensure_cache_chunk_fits(
            pending.bytes.len() as u64,
            bytes.len() as u64,
            MAX_CACHE_UPLOAD_BYTES,
        )?;
        // R1-6: cap the job's *aggregate* pending bytes, not just each
        // upload — MAX_PENDING_PER_JOB × MAX_CACHE_UPLOAD_BYTES would
        // otherwise let one job hold ~16 GiB in reservations. The sum runs
        // under the same lock as the append, so concurrent chunks cannot
        // race past the budget.
        if !pending.job_backend_id.is_empty() {
            ensure_pending_cache_bytes_fit(
                &inner,
                &pending.job_backend_id,
                bytes.len() as u64,
                MAX_PENDING_CACHE_BYTES_PER_JOB,
            )?;
        }
    }
    let pending = inner
        .pending_caches
        .get_mut(&cache_id)
        .ok_or_else(|| ApiError::not_found("cache reservation not found"))?;
    pending.bytes.extend_from_slice(&bytes);
    // Refresh the activity stamp: the TTL sweeper frees reservations idle
    // past PENDING_UPLOAD_TTL, and an active chunked upload must not be
    // reaped between PATCHes.
    pending.created_unix = crate::memory_caps::now_unix();
    // No write-through here on purpose: the in-flight payload is not durable
    // state (see `MetaSnapshot`), and snapshotting per chunk was quadratic in
    // cache size. The committed cache is persisted by `CacheStore` in
    // `cache_commit`.
    Ok(StatusCode::ACCEPTED)
}

pub(crate) async fn cache_commit(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Path(cache_id): Path<i64>,
    Json(request): Json<CacheCommitRequest>,
) -> Result<Json<CacheLookupResponse>, ApiError> {
    crate::events::trust_tier::ensure_cache_write_allowed(&shared.state, &headers).await?;
    let claims = auth::job_runtime_claims_from_headers(&shared.state, &headers);
    let caller_job_id = claims.as_ref().map(|claims| claims.job_id.to_string());
    let system = auth::system_bearer_authorized(&shared.state, &headers);
    // R1-10: a stale job token must not commit uploads after its job
    if !system {
        let Some(claims) = claims else {
            return Err(ApiError::unauthorized(
                "job token required for cache writes",
            ));
        };
        auth::require_live_job(&shared.state, claims.job_id).await?;
    }
    let pending = {
        let mut inner = shared.state.inner.lock().await;
        let pending = inner
            .pending_caches
            .get(&cache_id)
            .ok_or_else(|| ApiError::not_found("cache reservation not found"))?;
        if !system && pending.job_backend_id != caller_job_id.unwrap_or_default() {
            return Err(ApiError::forbidden(
                "cache reservation belongs to another job",
            ));
        }
        inner
            .pending_caches
            .remove(&cache_id)
            .ok_or_else(|| ApiError::not_found("cache reservation not found"))?
    };
    let meta = {
        let inner = shared.state.inner.lock().await;
        crate::store::build_meta_snapshot(&inner)
    };
    if let Err(error) = shared.state.store.store_meta_only(&meta).await {
        tracing::warn!(?error, "failed to persist cache commit");
    }
    if let Some(size) = request.size {
        let actual = pending.bytes.len() as u64;
        if size != actual {
            return Err(ApiError::bad_request(format!(
                "cache size mismatch: expected {size}, got {actual}"
            )));
        }
    }
    let entry = shared
        .state
        .cache
        .put_scoped(
            &pending.namespace,
            &pending.key,
            &pending.version,
            &pending.bytes,
        )
        .await?;
    Ok(Json(CacheLookupResponse {
        hit: true,
        key: Some(entry.key),
        version: Some(entry.version),
        size: Some(entry.size),
        content_base64: None,
    }))
}

pub(crate) async fn cache_lookup(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Query(query): Query<CacheQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let key = query.key.unwrap_or_default();
    let context = auth::job_cache_context_from_headers(&shared.state, &headers).await?;
    let restore_keys = parse_restore_keys(query.keys.as_deref());
    // R1-5: a job reads its own ref's namespace, then the PR base branch
    // (pull_request runs), then the repository's real default branch —
    // resolved from the event payload, not assumed to be `main`. Caches on
    // unrelated branches stay invisible. The system token keeps the
    // historical repository-only namespace.
    let namespaces: Vec<String> = match context {
        Some(context) => {
            let mut namespaces = vec![format!("{}\0{}", context.repository, context.git_ref)];
            for candidate in [context.base_ref, Some(context.default_branch_ref)]
                .into_iter()
                .flatten()
            {
                if candidate != context.git_ref
                    && !namespaces
                        .iter()
                        .any(|ns| *ns == format!("{}\0{}", context.repository, candidate))
                {
                    namespaces.push(format!("{}\0{}", context.repository, candidate));
                }
            }
            namespaces
        }
        None => vec![String::new()],
    };
    let mut response = None;
    for namespace in &namespaces {
        response = shared
            .state
            .cache
            .get_scoped(namespace, &key, &query.version, &restore_keys)
            .await?;
        if response.is_some() {
            break;
        }
    }
    if let Some((entry, _bytes)) = response {
        Ok(Json(json!({
            "cacheKey": entry.key,
            "scope": "preloop",
            "archiveLocation": format!("/api/v1/cache?key={}&version={}", key, query.version),
        })))
    } else {
        Ok(Json(json!({})))
    }
}

pub(crate) async fn artifact_put(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<ArtifactPutRequest>,
) -> Result<Json<ArtifactRecord>, ApiError> {
    let bytes = decode_base64(&request.content_base64)?;
    put_artifact(
        shared,
        request.run_id,
        request.name,
        request.file_name,
        bytes,
    )
    .await
}

pub(crate) async fn artifact_create(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Path(run_id): Path<RunId>,
    Json(request): Json<ArtifactCreateRequest>,
) -> Result<Json<ArtifactRecord>, ApiError> {
    // R1-10: a stale job token must not create artifacts after its job
    // completes. The system bearer manages the lifecycle itself and bypasses;
    // the official runner uploads during the run, while its job is live.
    if !auth::system_bearer_authorized(&shared.state, &headers) {
        let Some(claims) = auth::job_runtime_claims_from_headers(&shared.state, &headers) else {
            return Err(ApiError::unauthorized(
                "job token required for artifact writes",
            ));
        };
        auth::require_live_job(&shared.state, claims.job_id).await?;
    }
    put_artifact(shared, run_id, request.name, request.file_name, Vec::new()).await
}

pub(crate) async fn put_artifact(
    shared: Arc<SharedState>,
    run_id: RunId,
    name: String,
    file_name: String,
    bytes: Vec<u8>,
) -> Result<Json<ArtifactRecord>, ApiError> {
    let artifact = shared
        .state
        .artifacts
        .put(run_id, &name, &file_name, &bytes)
        .await?;
    let record = ArtifactRecord {
        id: artifact.id.to_string(),
        run_id,
        name,
        file_name,
        path: artifact.path.to_string_lossy().into_owned(),
        size: artifact.size,
    };
    let mut inner = shared.state.inner.lock().await;
    inner.artifacts.insert(record.id.clone(), record.clone());
    let meta = crate::store::build_meta_snapshot(&inner);
    if let Err(error) = shared.state.store.store_meta_only(&meta).await {
        tracing::warn!(?error, "failed to persist artifact metadata");
    }
    Ok(Json(record))
}

pub(crate) async fn artifact_get(
    State(shared): State<Arc<SharedState>>,
    Path(artifact_id): Path<String>,
) -> Result<Response, ApiError> {
    read_artifact(shared, artifact_id).await
}

pub(crate) async fn artifact_get_compat(
    State(shared): State<Arc<SharedState>>,
    Path((_run_id, artifact_id)): Path<(RunId, String)>,
) -> Result<Response, ApiError> {
    read_artifact(shared, artifact_id).await
}

pub(crate) async fn read_artifact(
    shared: Arc<SharedState>,
    artifact_id: String,
) -> Result<Response, ApiError> {
    let record = {
        let inner = shared.state.inner.lock().await;
        inner
            .artifacts
            .get(&artifact_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("artifact not found"))?
    };
    let bytes = tokio::fs::read(&record.path).await?;
    Ok(Response::builder()
        .header("content-type", "application/octet-stream")
        .body(Body::from(bytes))
        .expect("static response builder"))
}

pub(crate) async fn artifact_list(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Json<serde_json::Value> {
    let inner = shared.state.inner.lock().await;
    let value = inner
        .artifacts
        .values()
        .filter(|artifact| artifact.run_id == run_id)
        .collect::<Vec<_>>();
    Json(json!({
        "count": value.len(),
        "value": value,
    }))
}

pub(crate) fn parse_restore_keys(keys: Option<&str>) -> Vec<String> {
    keys.unwrap_or_default()
        .split(',')
        .filter(|key| !key.is_empty())
        .map(str::to_owned)
        .collect()
}

pub(crate) fn decode_base64(value: &str) -> Result<Vec<u8>, ApiError> {
    BASE64_STANDARD
        .decode(value)
        .map_err(|error| ApiError::bad_request(format!("invalid base64 content: {error}")))
}
