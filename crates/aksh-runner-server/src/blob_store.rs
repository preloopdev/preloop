use super::*;

// ─── Azure Block Blob compat blob store ───────────────────────────────────────
//
// Both actions/cache@v4 and actions/upload-artifact@v4 upload via the Azure SDK
// (BlockBlobClient).  The protocol is:
//   • Single-shot: PUT /twirp-blob/{kind}/{token}                  → 201
//   • Stage block: PUT /twirp-blob/{kind}/{token}?comp=block&blockid={b64} → 201
//   • Commit list: PUT /twirp-blob/{kind}/{token}?comp=blocklist   → 201
// Downloads (cache + artifact) use a plain GET.

#[derive(Debug, Deserialize)]
pub(crate) struct BlobPutQuery {
    comp: Option<String>,
    blockid: Option<String>,
}

/// Convert a base64 block ID to a filesystem-safe name.
pub(crate) fn blockid_to_filename(blockid: &str) -> String {
    blockid.replace('+', "-").replace('/', "_").replace('=', "")
}

/// Parse an Azure Block Blob blocklist XML body and return block IDs in order.
pub(crate) fn parse_blocklist_xml(body: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let mut pos = 0;
    while let Some(start_off) = body[pos..].find("<Latest>") {
        let content_start = pos + start_off + 8; // len("<Latest>") == 8
        if let Some(end_off) = body[content_start..].find("</Latest>") {
            let id = body[content_start..content_start + end_off]
                .trim()
                .to_owned();
            if !id.is_empty() {
                ids.push(id);
            }
            pos = content_start + end_off + 9; // len("</Latest>") == 9
        } else {
            break;
        }
    }
    ids
}

pub(crate) async fn blob_put(
    State(shared): State<Arc<SharedState>>,
    Path((kind, token)): Path<(String, String)>,
    Query(query): Query<BlobPutQuery>,
    body: axum::body::Bytes,
) -> StatusCode {
    let blob_root = shared
        .state
        .state_dir
        .join("blobs")
        .join(&kind)
        .join(&token);

    match query.comp.as_deref() {
        Some("block") => {
            let block_id = query.blockid.unwrap_or_default();
            let safe_id = blockid_to_filename(&block_id);
            let blocks_dir = blob_root.join("blocks");
            if let Err(e) = tokio::fs::create_dir_all(&blocks_dir).await {
                warn!(kind, token, "failed to create blocks dir: {e}");
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            match tokio::fs::write(blocks_dir.join(&safe_id), &body).await {
                Ok(()) => {
                    debug!(
                        kind,
                        token,
                        block = safe_id,
                        bytes = body.len(),
                        "blob block staged"
                    );
                    StatusCode::CREATED
                }
                Err(e) => {
                    warn!(kind, token, "failed to write block {safe_id}: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
        }
        Some("blocklist") => {
            let body_str = String::from_utf8_lossy(&body);
            let block_ids = parse_blocklist_xml(&body_str);
            let blocks_dir = blob_root.join("blocks");
            let data_path = blob_root.join("data");

            let mut assembled: Vec<u8> = Vec::new();
            for bid in &block_ids {
                let safe_id = blockid_to_filename(bid);
                match tokio::fs::read(blocks_dir.join(&safe_id)).await {
                    Ok(bytes) => assembled.extend_from_slice(&bytes),
                    Err(e) => {
                        warn!(kind, token, "failed to read block {safe_id}: {e}");
                        return StatusCode::INTERNAL_SERVER_ERROR;
                    }
                }
            }
            match tokio::fs::write(&data_path, &assembled).await {
                Ok(()) => {
                    let _ = tokio::fs::remove_dir_all(&blocks_dir).await;
                    info!(
                        kind,
                        token,
                        size = assembled.len(),
                        blocks = block_ids.len(),
                        "blob assembled from blocks"
                    );
                    StatusCode::CREATED
                }
                Err(e) => {
                    warn!(kind, token, "failed to write assembled blob: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
        }
        _ => {
            // Single-shot upload.
            let data_path = blob_root.join("data");
            match tokio::fs::write(&data_path, &body).await {
                Ok(()) => {
                    info!(kind, token, size = body.len(), "blob single-shot upload");
                    StatusCode::CREATED
                }
                Err(e) => {
                    warn!(kind, token, "failed to write single-shot blob: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
        }
    }
}

pub(crate) async fn blob_get(
    State(shared): State<Arc<SharedState>>,
    Path((kind, mut token)): Path<(String, String)>,
) -> Response {
    // Artifact download URLs end in .zip for toolkit zip-detection.
    if kind == "artifact" && token.ends_with(".zip") {
        token.truncate(token.len() - 4);
    }

    if kind == "cache" {
        // Token is a download token → look up (key, version) in state.
        let kv = {
            let inner = shared.state.inner.lock().await;
            inner.cache_v2_dl_tokens.get(&token).cloned()
        };
        if let Some((key, version)) = kv {
            let empty: Vec<String> = Vec::new();
            return match shared.state.cache.get(&key, &version, &empty).await {
                Ok(Some((_entry, bytes))) => (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "application/octet-stream")],
                    bytes,
                )
                    .into_response(),
                Ok(None) => StatusCode::NOT_FOUND.into_response(),
                Err(e) => {
                    warn!(key, version, "cache read error: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            };
        }
    }

    // Artifact (or cache fallback): serve from blob staging dir.
    let data_path = shared
        .state
        .state_dir
        .join("blobs")
        .join(&kind)
        .join(&token)
        .join("data");
    match tokio::fs::read(&data_path).await {
        Ok(bytes) => {
            if kind == "artifact" {
                let name = {
                    let inner = shared.state.inner.lock().await;
                    inner
                        .artifact_v2_registry
                        .values()
                        .find(|e| e.blob_token == token)
                        .map(|e| e.name.clone())
                };
                let filename = name.unwrap_or_else(|| "artifact".to_owned());
                let content_disposition = format!("attachment; filename=\"{filename}.zip\"");
                (
                    StatusCode::OK,
                    [
                        (header::CONTENT_TYPE, "application/zip"),
                        (header::CONTENT_DISPOSITION, &content_disposition),
                    ],
                    bytes,
                )
                    .into_response()
            } else {
                (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "application/octet-stream")],
                    bytes,
                )
                    .into_response()
            }
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Accept blob uploads (logs, summaries) at signed-URL paths.
/// Stores them in a local replay directory for conformance inspection.
pub(crate) async fn replay_results_put(
    State(shared): State<Arc<SharedState>>,
    axum::extract::Path(path): axum::extract::Path<String>,
    body: axum::body::Bytes,
) -> StatusCode {
    // Reject path traversal attempts
    if path.contains("..")
        || std::path::Path::new(&path)
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
    {
        tracing::warn!("Rejected path traversal attempt: {path}");
        return StatusCode::BAD_REQUEST;
    }

    let dest = shared
        .state
        .state_dir
        .join("replay")
        .join("results")
        .join(&path);
    if let Some(parent) = dest.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    match std::fs::write(&dest, &body) {
        Ok(()) => {
            tracing::info!("Stored {} bytes at replay/results/{path}", body.len());
            StatusCode::CREATED
        }
        Err(e) => {
            tracing::warn!("Failed to store replay/results/{path}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
