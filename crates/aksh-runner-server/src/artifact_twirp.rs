use super::*;

// ─── Artifact v2 Twirp (github.actions.results.api.v1.ArtifactService) ────────

#[derive(Debug, Deserialize)]
pub(crate) struct ArtifactV2CreateRequest {
    pub(crate) workflow_run_backend_id: String,
    pub(crate) workflow_job_run_backend_id: String,
    pub(crate) name: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ArtifactV2FinalizeRequest {
    pub(crate) workflow_run_backend_id: String,
    pub(crate) workflow_job_run_backend_id: String,
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) size: serde_json::Value, // proto3 JSON: int64 as string
    #[serde(default)]
    pub(crate) hash: Option<serde_json::Value>, // StringValue: plain string or wrapped object
}

#[derive(Debug, Deserialize)]
pub(crate) struct ArtifactV2ListRequest {
    pub(crate) workflow_run_backend_id: String,
    pub(crate) workflow_job_run_backend_id: String,
    #[serde(default)]
    pub(crate) name_filter: Option<serde_json::Value>, // StringValue: plain string in proto3 JSON
    #[serde(default)]
    pub(crate) id_filter: Option<serde_json::Value>, // Int64Value: string in proto3 JSON
}

#[derive(Debug, Deserialize)]
pub(crate) struct ArtifactV2GetSignedUrlRequest {
    pub(crate) workflow_run_backend_id: String,
    pub(crate) workflow_job_run_backend_id: String,
    pub(crate) name: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ArtifactV2DeleteRequest {
    pub(crate) workflow_run_backend_id: String,
    pub(crate) workflow_job_run_backend_id: String,
    pub(crate) name: String,
}

pub(crate) fn artifact_v2_registry_key(run_id: &str, job_id: &str, name: &str) -> String {
    format!("{run_id}/{job_id}/{name}")
}

pub(crate) async fn save_artifact_v2_registry(
    shared: &Arc<SharedState>,
) -> Result<(), std::io::Error> {
    let registry_path = shared.state.state_dir.join("artifact_v2_registry.json");
    let serialized = {
        let inner = shared.state.inner.lock().await;
        serde_json::to_string(&inner.artifact_v2_registry)?
    };
    tokio::fs::write(&registry_path, serialized.as_bytes()).await?;
    Ok(())
}
pub(crate) async fn twirp_artifact_v2_create(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<ArtifactV2CreateRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    validate_artifact_name(&request.name)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let token = uuid::Uuid::new_v4().to_string();
    let registry_key = artifact_v2_registry_key(
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
        &request.name,
    );
    let stage_dir = shared
        .state
        .state_dir
        .join("blobs")
        .join("artifact")
        .join(&token);
    tokio::fs::create_dir_all(&stage_dir)
        .await
        .map_err(|e| ApiError::internal(format!("failed to create artifact stage dir: {e}")))?;
    {
        let mut inner = shared.state.inner.lock().await;
        inner
            .artifact_v2_pending
            .insert(token.clone(), ArtifactV2Pending { registry_key });
    }
    let upload_url = format!("{}/twirp-blob/artifact/{token}", public_base_url());
    info!(token, name = request.name, "artifact v2 create");
    Ok(Json(json!({ "ok": true, "signed_upload_url": upload_url })))
}

pub(crate) async fn twirp_artifact_v2_finalize(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<ArtifactV2FinalizeRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let registry_key = artifact_v2_registry_key(
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
        &request.name,
    );
    let token = {
        let inner = shared.state.inner.lock().await;
        inner
            .artifact_v2_pending
            .iter()
            .find(|(_, p)| p.registry_key == registry_key)
            .map(|(k, _)| k.clone())
    }
    .ok_or_else(|| ApiError::not_found("no pending artifact upload for this name/run/job"))?;

    // Measure actual blob size.
    let blob_path = shared
        .state
        .state_dir
        .join("blobs")
        .join("artifact")
        .join(&token)
        .join("data");
    let size = tokio::fs::metadata(&blob_path)
        .await
        .map(|m| m.len())
        .unwrap_or_else(|_| match &request.size {
            serde_json::Value::String(s) => s.parse::<u64>().unwrap_or(0),
            serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
            _ => 0,
        });

    let artifact_id;
    {
        let mut inner = shared.state.inner.lock().await;
        inner.artifact_v2_pending.remove(&token);
        inner.next_artifact_v2_id += 1;
        artifact_id = inner.next_artifact_v2_id;
        let digest = request.hash.and_then(|v| match v {
            serde_json::Value::String(s) => Some(s),
            serde_json::Value::Object(ref obj) => obj
                .get("value")
                .and_then(|val| val.as_str().map(|s| s.to_owned())),
            _ => None,
        });
        inner.artifact_v2_registry.insert(
            registry_key,
            ArtifactV2Entry {
                id: artifact_id,
                workflow_run_backend_id: request.workflow_run_backend_id,
                workflow_job_run_backend_id: request.workflow_job_run_backend_id,
                name: request.name.clone(),
                size,
                created_at: server_iso_now(),
                digest,
                blob_token: token,
            },
        );
    }
    let _ = save_artifact_v2_registry(&shared).await;
    info!(
        artifact_id,
        name = request.name,
        size,
        "artifact v2 finalized"
    );
    Ok(Json(
        json!({ "ok": true, "artifact_id": artifact_id.to_string() }),
    ))
}

pub(crate) async fn twirp_artifact_v2_list(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<ArtifactV2ListRequest>,
) -> Json<serde_json::Value> {
    let inner = shared.state.inner.lock().await;

    let name_filter: Option<String> = request.name_filter.and_then(|v| match v {
        serde_json::Value::String(s) => Some(s),
        serde_json::Value::Object(ref obj) => obj
            .get("value")
            .and_then(|val| val.as_str().map(|s| s.to_owned())),
        _ => None,
    });
    let id_filter: Option<u64> = request.id_filter.and_then(|v| match v {
        serde_json::Value::String(s) => s.parse::<u64>().ok(),
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::Object(ref obj) => obj.get("value").and_then(|val| match val {
            serde_json::Value::String(s) => s.parse::<u64>().ok(),
            serde_json::Value::Number(n) => n.as_u64(),
            _ => None,
        }),
        _ => None,
    });

    let artifacts: Vec<serde_json::Value> = inner
        .artifact_v2_registry
        .values()
        .filter(|e| {
            e.workflow_run_backend_id == request.workflow_run_backend_id
                && e.workflow_job_run_backend_id == request.workflow_job_run_backend_id
        })
        .filter(|e| name_filter.as_deref().map(|f| e.name == f).unwrap_or(true))
        .filter(|e| id_filter.map(|id| e.id == id).unwrap_or(true))
        .map(|e| {
            json!({
                "workflow_run_backend_id": e.workflow_run_backend_id,
                "workflow_job_run_backend_id": e.workflow_job_run_backend_id,
                "database_id": e.id.to_string(),
                "name": e.name,
                "size": e.size.to_string(),
                "created_at": e.created_at,
                "digest": e.digest.as_deref().unwrap_or("")
            })
        })
        .collect();
    Json(json!({ "artifacts": artifacts }))
}

pub(crate) async fn twirp_artifact_v2_get_signed_url(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<ArtifactV2GetSignedUrlRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let registry_key = artifact_v2_registry_key(
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
        &request.name,
    );
    let blob_token = {
        let inner = shared.state.inner.lock().await;
        inner
            .artifact_v2_registry
            .get(&registry_key)
            .map(|e| e.blob_token.clone())
    }
    .ok_or_else(|| ApiError::not_found("artifact not found"))?;

    // URL must end in .zip so the toolkit's streamExtract detects it as a zip.
    let signed_url = format!("{}/twirp-blob/artifact/{blob_token}.zip", public_base_url());
    Ok(Json(json!({ "signed_url": signed_url })))
}

pub(crate) async fn twirp_artifact_v2_delete(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<ArtifactV2DeleteRequest>,
) -> Json<serde_json::Value> {
    let registry_key = artifact_v2_registry_key(
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
        &request.name,
    );
    let removed = {
        let mut inner = shared.state.inner.lock().await;
        inner.artifact_v2_registry.remove(&registry_key)
    };
    if let Some(e) = removed {
        let _ = save_artifact_v2_registry(&shared).await;
        let blob_dir = shared
            .state
            .state_dir
            .join("blobs")
            .join("artifact")
            .join(&e.blob_token);
        let _ = tokio::fs::remove_dir_all(blob_dir).await;
        Json(json!({ "ok": true, "artifact_id": e.id.to_string() }))
    } else {
        Json(json!({ "ok": false, "artifact_id": "0" }))
    }
}
