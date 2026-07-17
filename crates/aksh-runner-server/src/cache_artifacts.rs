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
    Json(request): Json<CacheReserveRequest>,
) -> Json<CacheReserveResponse> {
    let mut inner = shared.state.inner.lock().await;
    inner.next_cache_id += 1;
    let cache_id = inner.next_cache_id;
    inner.pending_caches.insert(
        cache_id,
        PendingCache {
            key: request.key,
            version: request.version,
            bytes: Vec::new(),
        },
    );
    Json(CacheReserveResponse { cache_id })
}

pub(crate) async fn cache_upload(
    State(shared): State<Arc<SharedState>>,
    Path(cache_id): Path<i64>,
    bytes: Bytes,
) -> Result<StatusCode, ApiError> {
    let mut inner = shared.state.inner.lock().await;
    let pending = inner
        .pending_caches
        .get_mut(&cache_id)
        .ok_or_else(|| ApiError::not_found("cache reservation not found"))?;
    pending.bytes.extend_from_slice(&bytes);
    Ok(StatusCode::ACCEPTED)
}

pub(crate) async fn cache_commit(
    State(shared): State<Arc<SharedState>>,
    Path(cache_id): Path<i64>,
    Json(request): Json<CacheCommitRequest>,
) -> Result<Json<CacheLookupResponse>, ApiError> {
    let pending = {
        let mut inner = shared.state.inner.lock().await;
        inner
            .pending_caches
            .remove(&cache_id)
            .ok_or_else(|| ApiError::not_found("cache reservation not found"))?
    };
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
        .put(&pending.key, &pending.version, &pending.bytes)
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
    Query(query): Query<CacheQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let key = query.key.unwrap_or_default();
    let restore_keys = parse_restore_keys(query.keys.as_deref());
    let response = shared
        .state
        .cache
        .get(&key, &query.version, &restore_keys)
        .await?;
    if let Some((entry, _bytes)) = response {
        Ok(Json(json!({
            "cacheKey": entry.key,
            "scope": "aksh",
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
    Path(run_id): Path<RunId>,
    Json(request): Json<ArtifactCreateRequest>,
) -> Result<Json<ArtifactRecord>, ApiError> {
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
