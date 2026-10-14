use super::*;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Deserialize)]
pub struct CachePutRequest {
    key: String,
    version: String,
    #[serde(default)]
    content_base64: String,
}

#[derive(Debug, Deserialize)]
pub struct CacheQuery {
    key: Option<String>,
    keys: Option<String>,
    version: String,
}

#[derive(Debug, Serialize)]
pub struct CacheLookupResponse {
    hit: bool,
    key: Option<String>,
    version: Option<String>,
    size: Option<u64>,
    content_base64: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheReserveRequest {
    key: String,
    version: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheReserveResponse {
    cache_id: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheCommitRequest {
    #[serde(default)]
    size: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ArtifactPutRequest {
    run_id: RunId,
    name: String,
    file_name: String,
    #[serde(default)]
    content_base64: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactCreateRequest {
    name: String,
    #[serde(default = "default_artifact_file_name")]
    file_name: String,
}

fn default_artifact_file_name() -> String {
    "artifact.bin".to_owned()
}

/// Include the job's git ref in the legacy cache namespace so one
/// branch cannot poison another branch's entries (first write wins on an
/// exact key+version). The system token keeps the historical
/// repository-only namespace.
pub fn ref_scoped_namespace(repository: Option<String>, git_ref: Option<String>) -> String {
    match (repository, git_ref) {
        (Some(repository), Some(git_ref)) => format!("{repository}\0{git_ref}"),
        (Some(repository), None) => repository,
        (None, _) => String::new(),
    }
}

pub async fn cache_put(
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

pub async fn cache_get(
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

pub async fn cache_reserve(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<CacheReserveRequest>,
) -> Result<Json<CacheReserveResponse>, ApiError> {
    crate::events::trust_tier::ensure_cache_write_allowed(&shared.state, &headers).await?;
    let repository = auth::job_repository_from_headers(&shared.state, &headers).await?;
    let git_ref = auth::job_git_ref_from_headers(&shared.state, &headers).await?;
    let claims = auth::job_runtime_claims_from_headers(&shared.state, &headers);
    // A stale job token must not reserve new uploads after its job
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
    // Re-check liveness against the backend: a settled job must not mint a
    // fresh reservation.
    if let Some(job_uuid) = job_uuid
        && !auth::job_is_live(&shared.state, job_uuid).await?
    {
        return Err(ApiError::forbidden(
            "job is not live; writes are rejected for completed or unknown jobs",
        ));
    }
    // Stage upload chunks to disk as they arrive instead of buffering the
    // whole blob in memory: a 150 MB upload used to sit in server RAM for
    // the reservation's lifetime. The staging file is
    // consumed by `cache_commit` (moved into the entry directory) or
    // deleted by the TTL sweeper when the reservation is abandoned.
    // Created before the state lock: the parent directory is shared by all
    // reservations, so no per-reservation I/O happens under the lock.
    let staging_dir = shared.state.state_dir.join("cache-staging");
    tokio::fs::create_dir_all(&staging_dir)
        .await
        .map_err(|error| ApiError::internal(format!("cache staging dir: {error}")))?;
    let mut inner = shared.state.inner.lock().await;
    // Bound in-flight legacy reservations per job, mirroring the v2
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
    let job_quota = if job_backend_id.is_empty() {
        None
    } else {
        Some(
            inner
                .pending_caches
                .values()
                .find(|pending| pending.job_backend_id == job_backend_id)
                .and_then(|pending| pending.job_quota.clone())
                .unwrap_or_else(|| {
                    Arc::new(tokio::sync::Semaphore::new(
                        MAX_PENDING_CACHE_BYTES_PER_JOB as usize,
                    ))
                }),
        )
    };
    let upload_quota = Arc::new(tokio::sync::Semaphore::new(MAX_CACHE_UPLOAD_BYTES as usize));
    let invalidated = Arc::new(std::sync::atomic::AtomicBool::new(false));
    inner.next_cache_id += 1;
    let cache_id = inner.next_cache_id;
    let staging_path = staging_dir.join(format!("{cache_id}.part"));
    inner.pending_caches.insert(
        cache_id,
        PendingCache {
            key: request.key,
            // The reservation is bound to the job's git ref, not just
            // the repository, so a branch run cannot squat another branch's
            // key namespace.
            namespace: ref_scoped_namespace(repository, git_ref),
            version: request.version,
            staging_path,
            staging_len: 0,
            upload_lock: Arc::new(tokio::sync::Mutex::new(())),
            upload_quota,
            job_quota,
            invalidated,
            job_backend_id,
            // Stamp the reservation so the TTL sweeper can free it if
            // the job never commits (previously abandoned reservations held
            // their bytes forever).
            created_unix: crate::memory_caps::now_unix(),
        },
    );
    drop(inner);
    Ok(Json(CacheReserveResponse { cache_id }))
}
struct CacheUploadGuard {
    cache_id: i64,
    staging_path: std::path::PathBuf,
    start_len: u64,
    invalidated: Arc<std::sync::atomic::AtomicBool>,
    staged: tokio::fs::File,
    upload_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    job_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    finished: bool,
}

impl CacheUploadGuard {
    fn reserve_chunk(
        &mut self,
        upload_quota: Arc<tokio::sync::Semaphore>,
        job_quota: Option<Arc<tokio::sync::Semaphore>>,
        bytes: usize,
    ) -> Result<(), ApiError> {
        let permits = u32::try_from(bytes)
            .map_err(|_| ApiError::payload_too_large("cache upload frame is too large"))?;
        let upload = match upload_quota.try_acquire_many_owned(permits) {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                return Err(ApiError::payload_too_large(
                    "cache upload exceeds the per-upload cap",
                ));
            }
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Err(ApiError::internal("cache upload quota is closed"));
            }
        };
        let Some(job_quota) = job_quota else {
            if let Some(existing) = &mut self.upload_permit {
                existing.merge(upload);
            } else {
                self.upload_permit = Some(upload);
            }
            return Ok(());
        };
        let job = match job_quota.try_acquire_many_owned(permits) {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                return Err(ApiError::payload_too_large(
                    "job exceeds the pending-cache byte budget",
                ));
            }
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Err(ApiError::internal("pending-cache quota is closed"));
            }
        };
        if let Some(existing) = &mut self.upload_permit {
            existing.merge(upload);
        } else {
            self.upload_permit = Some(upload);
        }
        if let Some(existing) = &mut self.job_permit {
            existing.merge(job);
        } else {
            self.job_permit = Some(job);
        }
        Ok(())
    }

    async fn rollback(&mut self) -> bool {
        let result = match self.staged.set_len(self.start_len).await {
            Ok(()) => self.staged.flush().await,
            Err(error) => Err(error),
        };
        if result.is_ok() {
            self.finished = true;
            true
        } else {
            self.invalidated
                .store(true, std::sync::atomic::Ordering::SeqCst);
            false
        }
    }

    fn finish(&mut self) {
        if let Some(permit) = self.upload_permit.take() {
            permit.forget();
        }
        if let Some(permit) = self.job_permit.take() {
            permit.forget();
        }
        self.finished = true;
    }
}

impl Drop for CacheUploadGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.invalidated
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let path = self.staging_path.clone();
        let start_len = self.start_len;
        let cache_id = self.cache_id;
        tokio::spawn(async move {
            let result = async {
                let mut file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .await?;
                file.set_len(start_len).await?;
                file.flush().await
            }
            .await;
            if let Err(error) = result {
                warn!(
                    cache_id,
                    path = %path.display(),
                    ?error,
                    "cancelled cache upload rollback failed"
                );
            }
        });
    }
}

async fn cache_upload_rollback_error(guard: &mut CacheUploadGuard, error: ApiError) -> ApiError {
    if guard.rollback().await {
        error
    } else {
        ApiError::internal("cache upload rollback failed; reservation invalidated")
    }
}

pub async fn cache_upload(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Path(cache_id): Path<i64>,
    body: Body,
) -> Result<StatusCode, ApiError> {
    crate::events::trust_tier::ensure_cache_write_allowed(&shared.state, &headers).await?;
    let claims = auth::job_runtime_claims_from_headers(&shared.state, &headers);
    let caller_job_id = claims.as_ref().map(|claims| claims.job_id.to_string());
    let system = auth::system_bearer_authorized(&shared.state, &headers);
    // A stale job token must not keep uploading after its job
    // completes. The system bearer manages the lifecycle itself and bypasses.
    if !system {
        let Some(claims) = claims else {
            return Err(ApiError::unauthorized(
                "job token required for cache writes",
            ));
        };
        auth::require_live_job(&shared.state, claims.job_id).await?;
    }

    // Look up the reservation lock without waiting while `InnerState` is
    // held. The second lookup after acquiring it closes the commit/reaper
    // race where the reservation disappears while this request waits.
    let upload_lock = {
        let inner = shared.state.inner.lock().await;
        let pending = inner
            .pending_caches
            .get(&cache_id)
            .ok_or_else(|| ApiError::not_found("cache reservation not found"))?;
        if !system && pending.job_backend_id != caller_job_id.as_deref().unwrap_or_default() {
            return Err(ApiError::forbidden(
                "cache reservation belongs to another job",
            ));
        }
        pending.upload_lock.clone()
    };
    let _upload_guard = upload_lock.lock().await;
    let (staging_path, start_len, invalidated) = {
        let inner = shared.state.inner.lock().await;
        let pending = inner
            .pending_caches
            .get(&cache_id)
            .ok_or_else(|| ApiError::not_found("cache reservation not found"))?;
        if !system && pending.job_backend_id != caller_job_id.as_deref().unwrap_or_default() {
            return Err(ApiError::forbidden(
                "cache reservation belongs to another job",
            ));
        }
        if pending
            .invalidated
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ApiError::bad_request(
                "cache upload reservation is invalidated",
            ));
        }
        (
            pending.staging_path.clone(),
            pending.staging_len,
            pending.invalidated.clone(),
        )
    };
    let staged = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(&staging_path)
        .await
        .map_err(|error| ApiError::internal(format!("cache staging write: {error}")))?;
    let actual_len = staged
        .metadata()
        .await
        .map_err(|error| ApiError::internal(format!("cache staging stat: {error}")))?
        .len();
    if actual_len != start_len {
        invalidated.store(true, std::sync::atomic::Ordering::SeqCst);
        return Err(ApiError::internal(
            "cache staging length disagrees with reservation",
        ));
    }
    let mut guard = CacheUploadGuard {
        cache_id,
        staging_path,
        start_len,
        invalidated,
        staged,
        upload_permit: None,
        job_permit: None,
        finished: false,
    };
    let mut stream = body.into_data_stream();
    let mut total = 0u64;
    while let Some(next) = stream.next().await {
        let chunk = next.map_err(|error| ApiError::internal(format!("cache upload body: {error}")));
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                return Err(cache_upload_rollback_error(&mut guard, error).await);
            }
        };
        if chunk.is_empty() {
            continue;
        }
        let quotas = {
            let inner = shared.state.inner.lock().await;
            let pending = inner
                .pending_caches
                .get(&cache_id)
                .ok_or_else(|| ApiError::not_found("cache reservation not found"));
            match pending {
                Ok(pending) => {
                    if pending
                        .invalidated
                        .load(std::sync::atomic::Ordering::SeqCst)
                    {
                        Err(ApiError::bad_request(
                            "cache upload reservation is invalidated",
                        ))
                    } else {
                        Ok((pending.upload_quota.clone(), pending.job_quota.clone()))
                    }
                }
                Err(error) => Err(error),
            }
        };
        let (upload_quota, job_quota) = match quotas {
            Ok(quotas) => quotas,
            Err(error) => {
                return Err(cache_upload_rollback_error(&mut guard, error).await);
            }
        };
        if let Err(error) = guard.reserve_chunk(upload_quota, job_quota, chunk.len()) {
            return Err(cache_upload_rollback_error(&mut guard, error).await);
        }
        if let Err(error) = guard.staged.write_all(&chunk).await {
            let error = ApiError::internal(format!("cache staging write: {error}"));
            return Err(cache_upload_rollback_error(&mut guard, error).await);
        }
        total = total.saturating_add(chunk.len() as u64);
    }
    if let Err(error) = guard.staged.flush().await {
        let error = ApiError::internal(format!("cache staging flush: {error}"));
        return Err(cache_upload_rollback_error(&mut guard, error).await);
    }

    let promoted = {
        let mut inner = shared.state.inner.lock().await;
        if let Some(pending) = inner.pending_caches.get_mut(&cache_id) {
            if pending
                .invalidated
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                Err(ApiError::bad_request(
                    "cache upload reservation is invalidated",
                ))
            } else {
                pending.staging_len = pending.staging_len.saturating_add(total);
                pending.created_unix = crate::memory_caps::now_unix();
                Ok(())
            }
        } else {
            Err(ApiError::not_found("cache reservation not found"))
        }
    };
    if let Err(error) = promoted {
        return Err(cache_upload_rollback_error(&mut guard, error).await);
    }
    guard.finish();
    Ok(StatusCode::ACCEPTED)
}

pub async fn cache_commit(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Path(cache_id): Path<i64>,
    Json(request): Json<CacheCommitRequest>,
) -> Result<Json<CacheLookupResponse>, ApiError> {
    crate::events::trust_tier::ensure_cache_write_allowed(&shared.state, &headers).await?;
    let claims = auth::job_runtime_claims_from_headers(&shared.state, &headers);
    let caller_job_id = claims.as_ref().map(|claims| claims.job_id.to_string());
    let system = auth::system_bearer_authorized(&shared.state, &headers);
    // A stale job token must not commit uploads after its job
    // completes. The system bearer manages the lifecycle itself and bypasses.
    if !system {
        let Some(claims) = claims else {
            return Err(ApiError::unauthorized(
                "job token required for cache writes",
            ));
        };
        auth::require_live_job(&shared.state, claims.job_id).await?;
    }

    let upload_lock = {
        let inner = shared.state.inner.lock().await;
        let pending = inner
            .pending_caches
            .get(&cache_id)
            .ok_or_else(|| ApiError::not_found("cache reservation not found"))?;
        if !system && pending.job_backend_id != caller_job_id.as_deref().unwrap_or_default() {
            return Err(ApiError::forbidden(
                "cache reservation belongs to another job",
            ));
        }
        pending.upload_lock.clone()
    };
    let _upload_guard = upload_lock.lock().await;
    let pending = {
        let mut inner = shared.state.inner.lock().await;
        let pending = inner
            .pending_caches
            .get(&cache_id)
            .ok_or_else(|| ApiError::not_found("cache reservation not found"))?;
        if !system && pending.job_backend_id != caller_job_id.as_deref().unwrap_or_default() {
            return Err(ApiError::forbidden(
                "cache reservation belongs to another job",
            ));
        }
        inner
            .pending_caches
            .remove(&cache_id)
            .ok_or_else(|| ApiError::not_found("cache reservation not found"))?
    };
    if let Some(job_quota) = &pending.job_quota {
        job_quota.add_permits(pending.staging_len as usize);
    }
    if pending
        .invalidated
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        let _ = tokio::fs::remove_file(&pending.staging_path).await;
        return Err(ApiError::bad_request(
            "cache upload reservation was interrupted",
        ));
    }
    if let Some(size) = request.size {
        let actual = pending.staging_len;
        if size != actual {
            let _ = tokio::fs::remove_file(&pending.staging_path).await;
            return Err(ApiError::bad_request(format!(
                "cache size mismatch: expected {size}, got {actual}"
            )));
        }
    }
    // Commit the staged file into the cache store without ever loading it
    // into RAM: the store moves the file and enforces the LRU quota.
    let entry = match shared
        .state
        .cache
        .commit_file_scoped(
            &pending.namespace,
            &pending.key,
            &pending.version,
            &pending.staging_path,
        )
        .await
    {
        Ok(entry) => entry,
        Err(error) => {
            // The store consumed nothing on failure (duplicate key, I/O):
            // the staging file is ours to clean up.
            let _ = tokio::fs::remove_file(&pending.staging_path).await;
            return Err(error.into());
        }
    };
    Ok(Json(CacheLookupResponse {
        hit: true,
        key: Some(entry.key),
        version: Some(entry.version),
        size: Some(entry.size),
        content_base64: None,
    }))
}

pub async fn cache_lookup(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Query(query): Query<CacheQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let key = query.key.unwrap_or_default();
    let context = auth::job_cache_context_from_headers(&shared.state, &headers).await?;
    let restore_keys = parse_restore_keys(query.keys.as_deref());
    // A job reads its own ref's namespace, then the PR base branch
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

pub async fn artifact_put(
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

pub async fn artifact_create(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Path(run_id): Path<RunId>,
    Json(request): Json<ArtifactCreateRequest>,
) -> Result<Json<ArtifactRecord>, ApiError> {
    // A stale job token must not create artifacts after its job
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
    // A job token may only name its own run: without the binding, run A's
    // live token could squat run B's artifact namespace through `:run_id`.
    require_legacy_artifact_run_binding(&shared, &headers, run_id).await?;
    put_artifact(shared, run_id, request.name, request.file_name, Vec::new()).await
}

/// Bind the legacy `/_apis/pipelines/workflows/:run_id/artifacts` handlers
/// to the caller's workflow run. The protocol-bearer gate accepts any valid
/// job token, and the path's `:run_id` was never checked — so run A's live
/// job token could create artifacts catalogued under run B (a cross-run
/// squat), or list/read them. A job token may only touch the run its own job
/// belongs to, resolved through its job request record. The system bearer
/// manages the lifecycle itself and bypasses, mirroring the other legacy
/// artifact checks; a request with no resolvable job claims cannot be
/// attributed to a run and is rejected.
async fn require_legacy_artifact_run_binding(
    shared: &Arc<SharedState>,
    headers: &axum::http::HeaderMap,
    run_id: RunId,
) -> Result<(), ApiError> {
    if auth::system_bearer_authorized(&shared.state, headers) {
        return Ok(());
    }
    let forbidden =
        || ApiError::forbidden("artifact access requires a token for that workflow run");
    let Some(claims) = auth::job_runtime_claims_from_headers(&shared.state, headers) else {
        return Err(ApiError::unauthorized(
            "job token required for artifact access",
        ));
    };
    let record = shared
        .state
        .backend
        .request(crate::control::backend::RequestKey::AgentJobId(
            claims.job_id,
        ))
        .await
        .map_err(|_| forbidden())?;
    if record.run_id != run_id {
        return Err(forbidden());
    }
    Ok(())
}

pub async fn put_artifact(
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
    drop(inner);
    // Durable catalog row: a restart must not lose an uploaded artifact
    // (the GET/list paths read the catalog before the in-memory map).
    if let Err(error) = shared
        .state
        .backend
        .put_artifact_catalog(crate::control::types::NewArtifactRow {
            public_id: record.id.clone(),
            run_id,
            name: record.name.clone(),
            storage_key: record.path.clone(),
            size_bytes: record.size.min(i64::MAX as u64) as i64,
        })
        .await
    {
        tracing::warn!(?error, "failed to persist artifact catalog row");
    }
    Ok(Json(record))
}

pub async fn artifact_get(
    State(shared): State<Arc<SharedState>>,
    Path(artifact_id): Path<String>,
) -> Result<Response, ApiError> {
    read_artifact(shared, artifact_id).await
}

pub async fn artifact_get_compat(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Path((run_id, artifact_id)): Path<(RunId, String)>,
) -> Result<Response, ApiError> {
    require_legacy_artifact_run_binding(&shared, &headers, run_id).await?;
    read_artifact(shared, artifact_id).await
}

pub async fn read_artifact(
    shared: Arc<SharedState>,
    artifact_id: String,
) -> Result<Response, ApiError> {
    // The control catalog is the durable home: imported artifacts and
    // uploads from a previous process live there. The in-memory map serves
    // only rows this process has not catalogued (test hooks, a backend
    // hiccup). One artifact id is one indexed row — never a catalog scan.
    let catalog_row = shared
        .state
        .backend
        .artifact_by_public_id(&artifact_id)
        .await
        .ok()
        .flatten();
    let bytes = if let Some(row) = catalog_row {
        tokio::fs::read(&row.storage_key).await?
    } else {
        let record = {
            let inner = shared.state.inner.lock().await;
            inner
                .artifacts
                .get(&artifact_id)
                .cloned()
                .ok_or_else(|| ApiError::not_found("artifact not found"))?
        };
        tokio::fs::read(&record.path).await?
    };
    Ok(Response::builder()
        .header("content-type", "application/octet-stream")
        .body(Body::from(bytes))
        .expect("static response builder"))
}

pub async fn artifact_list(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Path(run_id): Path<RunId>,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_legacy_artifact_run_binding(&shared, &headers, run_id).await?;
    // Catalog first (durable, includes imported artifacts), then in-memory
    // records this process has not catalogued; ids de-duplicate.
    let mut seen = std::collections::HashSet::new();
    let mut value = Vec::new();
    if let Ok(catalog) = shared.state.backend.artifact_catalog(Some(run_id)).await {
        for row in catalog {
            seen.insert(row.public_id.clone());
            // The server-side storage path is deliberately omitted: it is a
            // filesystem location clients never need and must not learn.
            value.push(json!({
                "id": row.public_id,
                "run_id": row.run_id,
                "name": row.name,
                "size": row.size_bytes,
            }));
        }
    }
    {
        let inner = shared.state.inner.lock().await;
        for artifact in inner.artifacts.values().filter(|a| a.run_id == run_id) {
            if !seen.insert(artifact.id.clone()) {
                continue;
            }
            value.push(json!({
                "id": artifact.id,
                "run_id": artifact.run_id,
                "name": artifact.name,
                "file_name": artifact.file_name,
                "size": artifact.size,
            }));
        }
    }
    Ok(Json(json!({
        "count": value.len(),
        "value": value,
    })))
}

pub fn parse_restore_keys(keys: Option<&str>) -> Vec<String> {
    keys.unwrap_or_default()
        .split(',')
        .filter(|key| !key.is_empty())
        .map(str::to_owned)
        .collect()
}

pub fn decode_base64(value: &str) -> Result<Vec<u8>, ApiError> {
    BASE64_STANDARD
        .decode(value)
        .map_err(|error| ApiError::bad_request(format!("invalid base64 content: {error}")))
}
