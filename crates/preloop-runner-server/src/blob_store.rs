use super::*;

use axum::body::Body;
use axum::http::{header::CONTENT_LENGTH, HeaderMap, StatusCode};
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};
use tokio::sync::Mutex;

static BLOB_LOCKS: LazyLock<std::sync::Mutex<BTreeMap<String, Arc<Mutex<()>>>>> =
    LazyLock::new(|| std::sync::Mutex::new(BTreeMap::new()));

fn blob_lock_for(kind: &str, token: &str) -> Arc<Mutex<()>> {
    let key = format!("{kind}/{token}");
    let mut map = BLOB_LOCKS.lock().expect("blob lock poisoned");
    map.entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

/// Drop a per-`(kind,token)` commit lock from `BLOB_LOCKS` once no in-flight
/// commit is using it, keeping the map bounded (a runner minting unbounded
/// tokens would otherwise leak an `Arc<Mutex>` per token forever). Holding the
/// map mutex serializes against `blob_lock_for`, so a strong count of 2 (the
/// map's ref plus the caller's `arc`) proves no other commit holds a clone.
fn release_blob_lock(kind: &str, token: &str, arc: Arc<Mutex<()>>) {
    let key = format!("{kind}/{token}");
    let mut map = BLOB_LOCKS.lock().expect("blob lock poisoned");
    if let Some(existing) = map.get(&key) {
        if Arc::strong_count(existing) <= 2 {
            map.remove(&key);
        }
    }
    drop(arc);
}

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
    headers: HeaderMap,
    body: Body,
) -> StatusCode {
    // Early Content-Length check before buffering — avoids allocating 512 MiB
    // for a block that will be rejected at 4 MiB.
    if let Some(cl) = headers
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<usize>().ok())
    {
        let cap = match query.comp.as_deref() {
            Some("block") => MAX_BLOCK_BYTES,
            Some("blocklist") => 1024 * 1024, // XML is tiny; 1 MiB is generous
            _ => MAX_ASSEMBLED_BYTES,
        };
        if cl > cap {
            warn!(kind, token, cl, cap, "blob content-length exceeds cap");
            return StatusCode::PAYLOAD_TOO_LARGE;
        }
    }
    // Stream the body with a mode-specific limit — never buffer more than the
    // per-mode cap. Single-shot may be up to 512 MiB, which is the route's
    // DefaultBodyLimit already.
    let limit = match query.comp.as_deref() {
        Some("block") => MAX_BLOCK_BYTES,
        Some("blocklist") => 1024 * 1024,
        _ => MAX_ASSEMBLED_BYTES,
    };
    let body_bytes = match axum::body::to_bytes(body, limit).await {
        Ok(b) => b,
        Err(_) => {
            warn!(
                kind,
                token, limit, "blob body exceeds limit while streaming"
            );
            return StatusCode::PAYLOAD_TOO_LARGE;
        }
    };
    let body = body_bytes;
    let blob_root = shared
        .state
        .state_dir
        .join("blobs")
        .join(&kind)
        .join(&token);

    match query.comp.as_deref() {
        Some("block") => {
            // F5: a block over the per-block cap is rejected up front. The
            // official runner stages 4 MiB blocks, so nothing legitimate is
            // lost; a large single PUT is an attacker, not an uploader.
            if body.len() > MAX_BLOCK_BYTES {
                warn!(
                    kind,
                    token,
                    bytes = body.len(),
                    "blob block exceeds the per-block cap"
                );
                return StatusCode::PAYLOAD_TOO_LARGE;
            }
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
            if block_ids.len() > MAX_BLOCKLIST_BLOCKS {
                warn!(
                    kind,
                    token,
                    blocks = block_ids.len(),
                    "blocklist exceeds the block-id budget"
                );
                return StatusCode::PAYLOAD_TOO_LARGE;
            }
            let blocks_dir = blob_root.join("blocks");
            let data_path = blob_root.join("data");

            // F5: fail fast on the total assembly budget before reading any
            // block, so a blocklist referencing > 512 MiB never starts
            // assembling (or materializes the destination).
            let mut total: u64 = 0;
            for bid in &block_ids {
                let safe_id = blockid_to_filename(bid);
                match tokio::fs::metadata(blocks_dir.join(&safe_id)).await {
                    Ok(md) => {
                        total = total.saturating_add(md.len());
                        if total > MAX_ASSEMBLED_BYTES as u64 {
                            warn!(
                                kind,
                                token, total, "blocklist exceeds the total assembly budget"
                            );
                            return StatusCode::PAYLOAD_TOO_LARGE;
                        }
                    }
                    Err(e) => {
                        warn!(kind, token, "failed to stat block {safe_id}: {e}");
                        return StatusCode::INTERNAL_SERVER_ERROR;
                    }
                }
            }

            // Serialize blocklist commits per (kind,token) so concurrent
            // commits don't clobber each other's `data` file. Assemble into
            // a temp file and atomically rename on success — a failed commit
            // never truncates or deletes the previously committed blob.
            let lock = blob_lock_for(&kind, &token);
            let guard = lock.lock().await;
            let tmp_path = data_path.with_extension(format!("tmp.{}", uuid::Uuid::new_v4()));
            let status = match assemble_streaming(&blocks_dir, &tmp_path, &block_ids).await {
                Ok(size) => match tokio::fs::rename(&tmp_path, &data_path).await {
                    Ok(()) => {
                        let _ = tokio::fs::remove_dir_all(&blocks_dir).await;
                        info!(
                            kind,
                            token,
                            size,
                            blocks = block_ids.len(),
                            "blob assembled from blocks"
                        );
                        StatusCode::CREATED
                    }
                    Err(e) => {
                        let _ = tokio::fs::remove_file(&tmp_path).await;
                        warn!(kind, token, "failed to commit assembled blob: {e}");
                        StatusCode::INTERNAL_SERVER_ERROR
                    }
                },
                Err(AssemblyError::Budget) => {
                    let _ = tokio::fs::remove_file(&tmp_path).await;
                    warn!(kind, token, "assembly budget exceeded during copy");
                    StatusCode::PAYLOAD_TOO_LARGE
                }
                Err(AssemblyError::Io(e)) => {
                    let _ = tokio::fs::remove_file(&tmp_path).await;
                    warn!(kind, token, "failed to write assembled blob: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            };
            drop(guard);
            release_blob_lock(&kind, &token, lock);
            status
        }
        _ => {
            if body.len() > MAX_ASSEMBLED_BYTES {
                warn!(
                    kind,
                    token,
                    bytes = body.len(),
                    "single-shot blob exceeds the assembly budget"
                );
                return StatusCode::PAYLOAD_TOO_LARGE;
            }
            // Single-shot upload.
            if let Err(e) = tokio::fs::create_dir_all(&blob_root).await {
                warn!(kind, token, "failed to create blob dir: {e}");
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
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

/// Failure mode for streaming blob assembly.
enum AssemblyError {
    Io(std::io::Error),
    Budget,
}

/// Stream blocks (in `block_ids` order) into `data_path`, never holding more
/// than one block in memory. Fails with [`AssemblyError::Budget`] if the
/// running total exceeds [`MAX_ASSEMBLED_BYTES`].
async fn assemble_streaming(
    blocks_dir: &std::path::Path,
    data_path: &std::path::Path,
    block_ids: &[String],
) -> Result<u64, AssemblyError> {
    let mut dest = tokio::fs::File::create(data_path)
        .await
        .map_err(AssemblyError::Io)?;
    let mut total: u64 = 0;
    for bid in block_ids {
        let safe_id = blockid_to_filename(bid);
        let mut src = tokio::fs::File::open(blocks_dir.join(&safe_id))
            .await
            .map_err(AssemblyError::Io)?;
        let copied = tokio::io::copy(&mut src, &mut dest)
            .await
            .map_err(AssemblyError::Io)?;
        total = total.saturating_add(copied);
        if total > MAX_ASSEMBLED_BYTES as u64 {
            return Err(AssemblyError::Budget);
        }
    }
    Ok(total)
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

/// Execution plans whose uploaded logs are kept on disk.
///
/// `preloop logs` prefers these blobs and falls back to the in-memory log
/// blocks when they are gone, so a pruned run still reports its logs for as
/// long as the engine lives.
pub(crate) const REPLAY_PLANS_RETAINED: usize = 64;

/// Bound the replay directory to the most recently written plans.
///
/// Every job uploads its step and job logs here and nothing removed them, so
/// the directory grew for the lifetime of the state directory. It is the same
/// failure mode that filled the disk during benchmarking, just slower: the
/// snapshot fix bounded the large per-run repositories, this bounds the small
/// per-job blobs.
///
/// Retention is by modification time rather than by run, because blobs are
/// keyed by execution plan and a run's plan ids are not recoverable once its
/// records are gone.
pub(crate) async fn prune_replay_results(
    state_dir: &std::path::Path,
    active_plans: &std::collections::BTreeSet<String>,
) {
    let root = state_dir.join("replay").join("results");
    let plans = match collect_plan_directories(&root).await {
        Ok(plans) => plans,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            tracing::warn!(path = %root.display(), %error, "failed to scan replay results");
            return;
        }
    };
    if plans.len() <= REPLAY_PLANS_RETAINED {
        return;
    }

    let mut plans: Vec<_> = plans
        .into_iter()
        .filter(|(path, _)| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_none_or(|plan| !active_plans.contains(plan))
        })
        .collect();
    // Newest first, so everything past the retention window is the tail.
    plans.sort_by_key(|(_, modified)| std::cmp::Reverse(*modified));
    for (path, _) in plans.into_iter().skip(REPLAY_PLANS_RETAINED) {
        if let Err(error) = tokio::fs::remove_dir_all(&path).await {
            if error.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %path.display(), %error, "failed to prune replay results");
            }
        }
    }
}

async fn collect_plan_directories(
    root: &std::path::Path,
) -> std::io::Result<Vec<(std::path::PathBuf, std::time::SystemTime)>> {
    let mut entries = tokio::fs::read_dir(root).await?;
    let mut plans = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let metadata = match entry.metadata().await {
            Ok(metadata) if metadata.is_dir() => metadata,
            _ => continue,
        };
        let modified = metadata.modified().unwrap_or(std::time::UNIX_EPOCH);
        plans.push((entry.path(), modified));
    }
    Ok(plans)
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
