use super::*;
use futures::StreamExt;
use preloop_gha_protocol::azdo::{
    ActionDownloadInfo, ActionDownloadInfoCollection, ActionReferenceList,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// POST action download info — resolve action references to download URLs.
pub async fn action_download_info(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let collection = collect_action_download_infos(&shared.state, &request).await;
    Json(serde_json::to_value(collection).unwrap_or_else(|_| json!({ "actions": {} })))
}

pub async fn runnerresolve_actions(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let mut actions = serde_json::Map::new();
    collect_runnerresolve_refs(&shared.state, &request, &mut actions).await;

    Json(json!({ "actions": actions }))
}

/// Maximum archive-checksum pins held in memory. Pins are minted only by
/// the engine when it fetches an action tarball (never by job VMs), so the
/// table grows with the engine's own action cache, not with attacker
/// input. When full, new fetches simply stay unpinned — fail open on the
/// cap, never on verification; pins already held keep enforcing.
const MAX_DIGEST_PINS: usize = 50_000;

/// SHA-256 hex digest of action tarball bytes, lowercase. Kept in sync
/// with the runner's `archive_sha256_hex`: the pin the engine computes
/// must compare equal to the digest the runner computes over the same
/// bytes.
fn archive_sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Response header attesting the engine-observed SHA-256 of the action
/// tarball being served. Lets the runner verify downloaded bytes even
/// when the resolve-time pin was Unpinned (P2): the digest is computed
/// by the engine over the exact bytes it serves, never by a job VM.
pub const ACTION_ARCHIVE_SHA256_HEADER: &str = "x-preloop-action-archive-sha256";

/// Pin-table key for an action archive checksum: `(owner, repo, sha)`,
/// lowercased. Returns `None` when the ref is not a resolved commit SHA —
/// mutable refs (branches, tags like `v4`) are never pinned, because the
/// bytes they name can change.
fn archive_pin_key(owner: &str, repo: &str, git_ref: &str) -> Option<(String, String, String)> {
    if !preloop_gha_protocol::git_ref::is_commit_sha_not_zero(git_ref) {
        return None;
    }
    Some((
        owner.to_lowercase(),
        repo.to_lowercase(),
        git_ref.to_lowercase(),
    ))
}

/// Sidecar recording the SHA-256 of the engine's cached action tarball:
/// `<cache_dir>/action.tar.gz.sha256`. Lets the in-memory pin table be
/// rebuilt after an engine restart without re-downloading.
fn action_archive_digest_sidecar(cache_dir: &std::path::Path) -> std::path::PathBuf {
    cache_dir.join("action.tar.gz.sha256")
}

/// Ensure the in-memory archive-checksum pin for a cached action tarball,
/// returning the digest that was ensured (or `None` when no pin applies).
///
/// `observed` is the digest computed while streaming the fetch
/// (cache-miss path); on the cache-hit path it is `None` and the pin is
/// backfilled from the sidecar. Only commit SHAs are pinned. A missing or
/// malformed sidecar simply leaves the action unpinned — it never fails
/// the download.
fn ensure_action_archive_pin(
    state: &AppState,
    cache_dir: &std::path::Path,
    owner: &str,
    repo: &str,
    git_ref: &str,
    observed: Option<&str>,
) -> Option<String> {
    let key = archive_pin_key(owner, repo, git_ref)?;
    let digest: String = match observed {
        Some(digest) => digest.to_owned(),
        None => {
            let raw = std::fs::read_to_string(action_archive_digest_sidecar(cache_dir)).ok()?;
            let digest = raw.trim().to_ascii_lowercase();
            if digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()) {
                digest
            } else {
                return None;
            }
        }
    };
    let mut pins = state.action_archive_sha256_pins.lock().ok()?;
    if let Some(existing) = pins.get(&key) {
        return Some(existing.clone());
    }
    if pins.len() >= MAX_DIGEST_PINS {
        tracing::warn!(
            "action archive digest pin table full; skipping pin for {owner}/{repo}@{git_ref}"
        );
        // The digest is still authoritative for this response even when the
        // pin table is full: attest it in the download header so the
        // runner can verify these bytes.
        return Some(digest);
    }
    pins.insert(key, digest.clone());
    Some(digest)
}

/// Build the tarball download response, attesting the engine-observed
/// archive digest in [`ACTION_ARCHIVE_SHA256_HEADER`] whenever one is
/// known. The runner verifies the bytes it receives against this
/// attestation even when its resolve-time pin was Unpinned (P2): a
/// poisoned local cache is evicted and the fresh download is checked
/// before extraction. A missing digest (old pre-feature cache entry)
/// simply omits the header — it never fails the download.
fn action_tarball_response(
    repo: &str,
    git_ref: &str,
    body: Body,
    digest: Option<&str>,
) -> Result<Response, ApiError> {
    let mut builder = Response::builder()
        .header(header::CONTENT_TYPE, "application/gzip")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{repo}-{git_ref}.tar.gz\""),
        );
    if let Some(digest) = digest {
        builder = builder.header(ACTION_ARCHIVE_SHA256_HEADER, digest);
    }
    builder
        .body(body)
        .map_err(|e| ApiError::internal(format!("failed to build response: {e}")))
}

/// How long a minted archive ticket stays valid. Actions are fetched during
/// job setup, so this only has to outlive a queue wait, not a whole run.
pub const ACTION_TICKET_TTL_SECS: u64 = 6 * 60 * 60;
/// How long a resolved action ref→SHA binding is trusted before the ref is
/// re-resolved. Matches the freshness GitHub gives a `@main`-style reference:
/// a new push to the ref is picked up after at most one TTL window.
pub const ACTION_SHA_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(300);
/// How long a *failed* ref resolution is remembered before it is retried.
///
/// Shorter than the success TTL so a transient outage heals quickly, but long
/// enough that an offline server does not pay the client's 10s connect timeout
/// once per `uses:` on every single job dispatch. Without this the lookup is
/// retried forever and lands directly on the cold-start path.
pub const ACTION_SHA_NEGATIVE_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// Whether a cached entry recorded at `at` is still fresh, given that a
/// negative entry expires sooner than a positive one.
fn sha_entry_fresh(sha: &Option<String>, at: std::time::Instant) -> bool {
    let ttl = if sha.is_some() {
        ACTION_SHA_CACHE_TTL
    } else {
        ACTION_SHA_NEGATIVE_TTL
    };
    at.elapsed() < ttl
}

/// Resolve an action ref (branch, tag, or short SHA) to the commit SHA GitHub
/// would pin for the job. Cached briefly in memory so a matrix fan-out
/// resolves each `uses:` once per window. Returns `None` on any failure
/// (offline, rate-limited, private repo without a PAT) so callers fall back
/// to the ref itself — the historical behavior.
async fn resolve_ref_to_sha(
    state: &AppState,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> Option<String> {
    // Already a full SHA: no lookup needed. The all-zero sentinel is not a
    // real commit, so it must not short-circuit as "resolved".
    if preloop_gha_protocol::git_ref::is_commit_sha_not_zero(git_ref) {
        return Some(git_ref.to_owned());
    }
    let cache_key = (owner.to_owned(), repo.to_owned(), git_ref.to_owned());
    if let Ok(cache) = state.action_sha_cache.lock() {
        if let Some((sha, at)) = cache.get(&cache_key) {
            if sha_entry_fresh(sha, *at) {
                return sha.clone();
            }
        }
    }

    let enc_owner = percent_encode_path_segment(owner);
    let enc_repo = percent_encode_path_segment(repo);
    let enc_git_ref = percent_encode_path_segment(git_ref);
    let api_base = state.github_urls.api_url.trim_end_matches('/').to_owned();
    let url = format!("{api_base}/repos/{enc_owner}/{enc_repo}/commits/{enc_git_ref}");
    let mut request = crate::shared_http::CLIENT.get(&url);
    if let Some(pat) = state.static_github_pat() {
        if url.starts_with("https://") {
            request = request.bearer_auth(pat);
        }
    }
    let response = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%owner, %repo, %git_ref, %error, "action ref resolution request failed; falling back to ref");
            return None;
        }
    };
    let sha = if response.status().is_success() {
        response
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|body| {
                body.get("sha")
                    .and_then(|value| value.as_str())
                    .filter(|sha| preloop_gha_protocol::git_ref::is_commit_sha_not_zero(sha))
                    .map(str::to_owned)
            })
    } else {
        let status = response.status();
        tracing::warn!(%owner, %repo, %git_ref, %status, "action ref resolution rejected; falling back to ref");
        None
    };
    if let Ok(mut cache) = state.action_sha_cache.lock() {
        // Drop anything that has aged out, so a long-lived server's cache stays
        // bounded by the set of refs currently in flight rather than by every
        // ref ever seen.
        cache.retain(|_, (cached, at)| sha_entry_fresh(cached, *at));
        cache.insert(cache_key, (sha.clone(), std::time::Instant::now()));
    }
    sha
}

/// Percent-encode a path component for RFC 3986 URL safety.
pub fn percent_encode_path_segment(input: &str) -> String {
    let mut encoded = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => {
                encoded.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    encoded
}

#[derive(serde::Deserialize)]
pub struct ActionTicketQuery {
    #[serde(default)]
    exp: Option<u64>,
    #[serde(default)]
    sig: Option<String>,
}

pub async fn download_action_tarball(
    State(shared): State<Arc<SharedState>>,
    Path((owner, repo, git_ref)): Path<(String, String, String)>,
    Query(ticket): Query<ActionTicketQuery>,
) -> Result<Response, ApiError> {
    // 1. Sanitize parameters to avoid directory traversal
    if owner.is_empty()
        || repo.is_empty()
        || git_ref.is_empty()
        || owner == "."
        || owner == ".."
        || repo == "."
        || repo == ".."
        || owner.contains('/')
        || owner.contains('\\')
        || owner.contains('\0')
        || repo.contains('/')
        || repo.contains('\\')
        || repo.contains('\0')
        || git_ref.starts_with('/')
        || git_ref.starts_with('\\')
        || std::path::Path::new(&git_ref).is_absolute()
        || git_ref.contains('\\')
        || git_ref.contains('\0')
        || git_ref.split('/').any(|seg| seg == "..")
    {
        return Err(ApiError::bad_request("invalid owner, repo, or git_ref"));
    }

    // 2. The URL is the capability. This route is bearerless and reachable
    // from inside every runner VM, so without a signature any workflow could
    // make the engine fetch an arbitrary repository with the engine's own
    // GitHub credential. Answer 404 rather than 403: an unauthorised caller
    // learns nothing about which actions exist.
    let authorised = match (ticket.exp, ticket.sig.as_deref()) {
        (Some(expires_at), Some(signature)) => shared
            .state
            .verify_action_ticket(&owner, &repo, &git_ref, expires_at, signature),
        _ => false,
    };
    if !authorised {
        warn!("rejected action download with missing or invalid ticket: {owner}/{repo}@{git_ref}");
        return Err(ApiError::not_found("action archive not found"));
    }

    let cache_dir = shared
        .state
        .state_dir
        .join("actions")
        .join(&owner)
        .join(&repo)
        .join(&git_ref);
    let cached_path = cache_dir.join("action.tar.gz");

    if cached_path.exists() {
        // Restart resilience: the pin table is in-memory, so rebuild it
        // from the sidecar the fetch wrote. A missing or malformed sidecar
        // leaves the action unpinned without failing the download.
        let digest =
            ensure_action_archive_pin(&shared.state, &cache_dir, &owner, &repo, &git_ref, None);
        let file = tokio::fs::File::open(&cached_path)
            .await
            .map_err(|e| ApiError::internal(format!("failed to open cached action: {e}")))?;
        let stream = tokio_util::io::ReaderStream::new(file);
        let body = Body::from_stream(stream);

        let res = action_tarball_response(&repo, &git_ref, body, digest.as_deref())?;
        return Ok(res);
    }

    // Cache Miss: Download from GitHub
    tokio::fs::create_dir_all(&cache_dir)
        .await
        .map_err(|e| ApiError::internal(format!("failed to create action cache dir: {e}")))?;

    // Unique per-request temp file: two jobs preparing the same uncached
    // action concurrently must not share a path — interleaved truncates
    // corrupt the stream (one worker gets a 500, the other a garbage
    // archive). The rename is the atomic publish; the loser serves the
    // winner's file.
    let temp_path = cache_dir.join(format!("action.tar.gz.{}.tmp", uuid::Uuid::new_v4()));
    let enc_owner = percent_encode_path_segment(&owner);
    let enc_repo = percent_encode_path_segment(&repo);
    let enc_git_ref = percent_encode_path_segment(&git_ref);
    let api_base = shared.state.github_urls.api_url.trim_end_matches('/');
    let github_url = format!("{api_base}/repos/{enc_owner}/{enc_repo}/tarball/{enc_git_ref}");

    info!(
        owner,
        repo, git_ref, github_url, "Downloading action to server cache"
    );

    let client = reqwest::Client::builder()
        .user_agent("preloop-runner-server")
        .build()
        .map_err(|e| ApiError::internal(format!("failed to build reqwest client: {e}")))?;

    // Authenticated where possible: the anonymous GitHub API is capped at 60
    // requests/hour per IP, and a campaign or busy engine burns that in
    // minutes of action downloads — after which every uncached tarball fetch
    // comes back rate-limited and every job fails at "Set up job". The
    // engine's static PAT (env or config) raises the budget to 5000/hour and
    // is the only credential that works for arbitrary third-party action
    // repos (a GitHub App installation token is scoped to the App's repos).
    let mut request = client.get(&github_url);
    if let Some(pat) = shared.state.static_github_pat() {
        if github_url.starts_with("https://") {
            request = request.bearer_auth(pat);
        }
    }
    let response = request.send().await.map_err(|e| {
        ApiError::internal(format!("failed to send download request to GitHub: {e}"))
    })?;

    if !response.status().is_success() {
        return Err(ApiError::not_found(format!(
            "GitHub returned status {} for {}",
            response.status(),
            github_url
        )));
    }

    let mut temp_file = tokio::fs::File::create(&temp_path)
        .await
        .map_err(|e| ApiError::internal(format!("failed to create temporary action file: {e}")))?;

    let mut hasher = Sha256::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| {
            ApiError::internal(format!("failed to read chunk from GitHub response: {e}"))
        })?;
        // Hash the archive bytes as they arrive, so the digest the engine
        // pins is computed over exactly the bytes written to the cache —
        // no second read, no TOCTOU between write and hash.
        hasher.update(&chunk);
        tokio::io::copy(&mut &chunk[..], &mut temp_file)
            .await
            .map_err(|e| {
                ApiError::internal(format!("failed to write chunk to temporary file: {e}"))
            })?;
    }
    let digest = format!("{:x}", hasher.finalize());

    // Atomically rename to final target path. A concurrent request may have
    // published the same action first — then the cached file is the winner's
    // (byte-identical) download and ours is discarded.
    let published_by_us = tokio::fs::rename(&temp_path, &cached_path).await.is_ok();
    if published_by_us {
        info!(cached_path = ?cached_path, "Action cached successfully on server");
        // Record the authoritative archive checksum next to the published
        // file: the engine fetched these bytes over TLS itself, so this
        // digest — not any job-VM report — is what later downloads are
        // verified against.
        let sidecar = action_archive_digest_sidecar(&cache_dir);
        if let Err(error) = tokio::fs::write(&sidecar, digest.as_bytes()).await {
            warn!(
                sidecar = %sidecar.display(), %error,
                "failed to write action archive digest sidecar"
            );
        }
    } else {
        let _ = tokio::fs::remove_file(&temp_path).await;
        if !cached_path.exists() {
            return Err(ApiError::internal(
                "failed to rename cached action file".to_string(),
            ));
        }
        info!(cached_path = ?cached_path, "Action cache published by a concurrent request");
    }
    // Populate the in-memory pin from our streaming digest (or, on the
    // concurrent-loser path, from the winner's sidecar once it lands —
    // later requests backfill it). The returned digest is attested in the
    // download response header so the runner can verify these bytes even
    // when its resolve-time pin was Unpinned.
    let digest = ensure_action_archive_pin(
        &shared.state,
        &cache_dir,
        &owner,
        &repo,
        &git_ref,
        Some(&digest),
    );

    let file = tokio::fs::File::open(&cached_path)
        .await
        .map_err(|e| ApiError::internal(format!("failed to open newly cached action: {e}")))?;
    let stream = tokio_util::io::ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let res = action_tarball_response(&repo, &git_ref, body, digest.as_deref())?;
    Ok(res)
}

pub fn action_download_ticket(
    state: &AppState,
    action: &str,
    version_override: Option<&str>,
) -> Option<(String, serde_json::Value)> {
    if action.starts_with("./") || action.starts_with("../") || action.starts_with("docker://") {
        return None;
    }

    let (repo_part, git_ref) = if let Some(version) = version_override {
        (action, version)
    } else {
        action.split_once('@')?
    };
    if git_ref.is_empty() {
        return None;
    }
    if git_ref.starts_with('/')
        || git_ref.starts_with('\\')
        || std::path::Path::new(git_ref).is_absolute()
    {
        return None;
    }

    let mut parts = repo_part.split('/');
    let owner = parts.next()?;
    let repo = parts.next()?;
    if owner.is_empty()
        || repo.is_empty()
        || owner == "."
        || owner == ".."
        || repo == "."
        || repo == ".."
    {
        return None;
    }

    let key = format!("{repo_part}@{git_ref}");
    let runner_url = runner_base_url();
    // The download route is bearerless and reachable from inside runner VMs,
    // so the URL itself has to be the capability: signed, scoped to this one
    // action, and short-lived. Actions are fetched during job setup, so a few
    // hours covers even a long queue wait.
    let expires_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_secs())
        .unwrap_or_default()
        + ACTION_TICKET_TTL_SECS;
    let signature = state.sign_action_ticket(owner, repo, git_ref, expires_at);
    let enc_owner = percent_encode_path_segment(owner);
    let enc_repo = percent_encode_path_segment(repo);
    let enc_ref = percent_encode_path_segment(git_ref);
    let url = format!(
        "{runner_url}/api/v1/actions/download/{enc_owner}/{enc_repo}/{enc_ref}\
?exp={expires_at}&sig={signature}"
    );
    Some((
        key,
        json!({
            "type": "Archive",
            "url": url,
            "authentication": null,
            "auth": null,
        }),
    ))
}

/// Maximum number of actions accepted in a single resolution batch to bound memory.
pub const MAX_ACTION_BATCH_SIZE: usize = 256;
/// Maximum concurrent outbound GitHub ref resolution requests.
pub const MAX_ACTION_CONCURRENCY: usize = 16;

pub async fn collect_runnerresolve_refs(
    state: &AppState,
    value: &serde_json::Value,
    actions: &mut serde_json::Map<String, serde_json::Value>,
) {
    let mut requests: Vec<(String, Option<String>)> = Vec::new();
    collect_runnerresolve_requests(value, &mut requests);
    let mut seen = std::collections::HashSet::new();
    requests.retain(|request| seen.insert(request.clone()));
    requests.truncate(MAX_ACTION_BATCH_SIZE);

    let stream = futures::stream::iter(requests.into_iter().map(|(action, version)| async move {
        runnerresolve_action(state, &action, version.as_deref()).await
    }))
    .buffer_unordered(MAX_ACTION_CONCURRENCY);

    let resolved: Vec<Option<(String, serde_json::Value)>> = stream.collect().await;
    for (key, value) in resolved.into_iter().flatten() {
        actions.entry(key).or_insert(value);
    }
}

/// Batch collector for the official JobServer `ActionDownloadInfo` endpoint.
pub async fn collect_action_download_infos(
    state: &AppState,
    value: &serde_json::Value,
) -> ActionDownloadInfoCollection {
    let mut requests: Vec<(String, Option<String>)> = Vec::new();
    if let Ok(list) = serde_json::from_value::<ActionReferenceList>(value.clone()) {
        for item in list.actions.into_iter() {
            if !item.name_with_owner.is_empty() {
                let ref_opt = if item.r#ref.is_empty() {
                    None
                } else {
                    Some(item.r#ref)
                };
                requests.push((item.name_with_owner, ref_opt));
            }
        }
    } else {
        collect_runnerresolve_requests(value, &mut requests);
    }

    let mut seen = std::collections::HashSet::new();
    requests.retain(|request| seen.insert(request.clone()));
    requests.truncate(MAX_ACTION_BATCH_SIZE);

    let stream = futures::stream::iter(requests.into_iter().map(|(action, version)| async move {
        action_download_info_entry(state, &action, version.as_deref()).await
    }))
    .buffer_unordered(MAX_ACTION_CONCURRENCY);

    let resolved: Vec<Option<(String, ActionDownloadInfo)>> = stream.collect().await;
    let mut actions = BTreeMap::new();
    for (key, value) in resolved.into_iter().flatten() {
        actions.insert(key, value);
    }

    ActionDownloadInfoCollection { actions }
}

fn collect_runnerresolve_requests(
    value: &serde_json::Value,
    requests: &mut Vec<(String, Option<String>)>,
) {
    match value {
        serde_json::Value::String(raw) => {
            requests.push((raw.to_owned(), None));
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_runnerresolve_requests(item, requests);
            }
        }
        serde_json::Value::Object(map) => {
            let action = map
                .get("action")
                .or_else(|| map.get("name"))
                .or_else(|| map.get("nameWithOwner"))
                .or_else(|| map.get("repository"))
                .and_then(|v| v.as_str());
            let version = map
                .get("version")
                .or_else(|| map.get("ref"))
                .or_else(|| map.get("reference"))
                .and_then(|v| v.as_str());
            if let Some(action) = action {
                requests.push((action.to_owned(), version.map(str::to_owned)));
            }

            for nested in map.values() {
                collect_runnerresolve_requests(nested, requests);
            }
        }
        _ => {}
    }
}

/// Resolve one action reference to its pinned download parts, shared by both
/// the JobServer (`ActionDownloadInfo`) and Launch (`runnerresolve`) wire
/// shapes. Returns `(lookup_key, name_with_owner, ref, resolved_sha,
/// tar_url)`.
///
/// The first `action_download_ticket` call is the validation gate as well as
/// the key source: it rejects `./`, `../` and `docker://` references, so those
/// never reach the network lookup below. Its ticket is discarded because the
/// final URL has to carry the resolved SHA, which is not known until after
/// resolution.
///
/// Pin the ref to the SHA GitHub would resolve at job time. The ticket URL
/// then carries the SHA, so both the server-side tarball cache and the
/// runner's `_actions/{owner}/{repo}/{sha}` extraction dir are keyed by
/// content identity: when the ref moves, the next job gets a fresh SHA, a
/// fresh download, and the stale archive is never served again. When the
/// lookup is unavailable the ref itself is used, preserving the historical
/// ref-keyed behavior.
async fn resolve_action_download(
    state: &AppState,
    action: &str,
    version_override: Option<&str>,
) -> Option<(String, String, String, Option<String>, String)> {
    let (key, _) = action_download_ticket(state, action, version_override)?;
    let (name, git_ref) = key.split_once('@')?;
    let name = name.to_string();
    let git_ref = git_ref.to_string();
    let repo_part = if version_override.is_some() {
        action.to_owned()
    } else {
        action.split_once('@')?.0.to_owned()
    };
    let mut parts = repo_part.split('/');
    let owner = parts.next()?.to_owned();
    let repo = parts.next()?.to_owned();

    let pinned = resolve_ref_to_sha(state, &owner, &repo, &git_ref).await;
    let effective_ref = pinned.as_deref().unwrap_or(&git_ref).to_owned();
    let (_, ticket) = action_download_ticket(state, action, Some(&effective_ref))?;
    let tar_url = ticket.get("url")?.as_str()?.to_string();
    Some((key, name, git_ref, pinned, tar_url))
}

/// Look up the engine-authoritative archive checksum pin for a resolved
/// action. Returns `Some(digest)` when the engine has fetched this exact
/// (owner, repo, sha) tarball itself and pinned its SHA-256, `None` when
/// the server supports checksums but has no pin for this (owner, repo,
/// sha) yet. The caller serializes `None` as an explicit JSON null so
/// runners can distinguish "supported but unpinned" from a pre-checksum
/// server that omits the key.
pub fn archive_sha256_pin_for(
    state: &AppState,
    name: &str,
    resolved_sha_opt: Option<&str>,
) -> Option<String> {
    let sha = resolved_sha_opt?;
    let (owner, repo) = name.split_once('/')?;
    let key = (
        owner.to_lowercase(),
        repo.to_lowercase(),
        sha.to_lowercase(),
    );
    state
        .action_archive_sha256_pins
        .lock()
        .ok()?
        .get(&key)
        .cloned()
}

pub async fn runnerresolve_action(
    state: &AppState,
    action: &str,
    version_override: Option<&str>,
) -> Option<(String, serde_json::Value)> {
    let (key, name, git_ref, resolved_sha_opt, tar_url) =
        resolve_action_download(state, action, version_override).await?;
    // M2: never echo the mutable ref back as `resolved_sha`. A caller that trusts the
    // field would treat `v4` as a pinned commit. Omitting it makes the unresolved case
    // explicit on the wire, and the runner refuses the download instead of fetching
    // the mutable ref.
    //
    // `archive_sha256` carries the engine-authoritative pin for this exact
    // (owner, repo, sha): the SHA-256 the engine computed while fetching
    // the tarball itself. A present null means the server supports
    // checksums but has not fetched this version yet — the engine pins it
    // during the download this resolve triggers, so later resolves hand
    // the runner a pin to verify against. The key is always present so
    // runners can distinguish "supported" from a pre-checksum server.
    let archive_sha256_pin = archive_sha256_pin_for(state, &name, resolved_sha_opt.as_deref());
    let mut entry = json!({
        "name": name,
        "version": git_ref,
        "tar_url": tar_url,
        "authentication": null,
        "archive_sha256": archive_sha256_pin,
    });
    if let Some(resolved_sha) = resolved_sha_opt {
        entry["resolved_sha"] = json!(resolved_sha);
    }
    Some((key, entry))
}

/// One `ActionDownloadInfo` entry in the official `ActionDownloadInfoCollection` wire shape.
pub async fn action_download_info_entry(
    state: &AppState,
    action: &str,
    version_override: Option<&str>,
) -> Option<(String, ActionDownloadInfo)> {
    let (key, name, git_ref, resolved_sha, tar_url) =
        resolve_action_download(state, action, version_override).await?;
    // Preloop download capability URLs are HMAC-signed and bearerless.
    // Operator PAT is kept server-side to avoid leaking broad credentials to untrusted workflows.
    let authentication = None;

    Some((
        key,
        ActionDownloadInfo {
            name_with_owner: Some(name.clone()),
            resolved_name_with_owner: Some(name),
            resolved_sha,
            r#ref: Some(git_ref),
            tarball_url: Some(tar_url),
            zipball_url: None,
            authentication,
            package_details: None,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";
    const DIGEST_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const DIGEST_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    async fn test_state() -> AppState {
        let temp = tempfile::tempdir().unwrap();
        AppState::new(temp.path().to_path_buf()).await.unwrap()
    }

    /// `archive_sha256_hex` is the lowercase hex SHA-256 of the bytes —
    /// the same value the runner computes over the downloaded archive.
    #[test]
    fn archive_sha256_hex_matches_known_vector() {
        // SHA-256("abc").
        assert_eq!(
            archive_sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(archive_sha256_hex(b"").len(), 64);
    }

    /// `archive_pin_key` only keys commit SHAs, lowercased. Mutable refs,
    /// the all-zero sentinel, and empty refs are never pinned.
    #[test]
    fn archive_pin_key_only_for_commit_shas() {
        assert_eq!(
            archive_pin_key("Actions", "Checkout", &SHA.to_uppercase()),
            Some((
                "actions".to_string(),
                "checkout".to_string(),
                SHA.to_string()
            ))
        );
        assert_eq!(archive_pin_key("actions", "checkout", "v4"), None);
        assert_eq!(archive_pin_key("actions", "checkout", "main"), None);
        assert_eq!(
            archive_pin_key("actions", "checkout", &"0".repeat(40)),
            None
        );
        assert_eq!(archive_pin_key("actions", "checkout", ""), None);
    }

    /// The engine pins the digest it computed while fetching: after
    /// `ensure_action_archive_pin` with an observed digest,
    /// `archive_sha256_pin_for` returns it, so runnerresolve hands the
    /// runner an authoritative pin.
    #[tokio::test]
    async fn engine_fetch_establishes_pin() {
        let state = test_state().await;
        let cache_dir = state
            .state_dir
            .join("actions")
            .join("o")
            .join("r")
            .join(SHA);
        std::fs::create_dir_all(&cache_dir).unwrap();

        ensure_action_archive_pin(&state, &cache_dir, "o", "r", SHA, Some(DIGEST_A));

        assert_eq!(
            archive_sha256_pin_for(&state, "o/r", Some(SHA)),
            Some(DIGEST_A.to_string())
        );
        // Unknown actions stay unpinned.
        assert_eq!(archive_sha256_pin_for(&state, "o/other", Some(SHA)), None);
        // No pin without a resolved SHA, and mutable refs never pin.
        assert_eq!(archive_sha256_pin_for(&state, "o/r", None), None);
        ensure_action_archive_pin(&state, &cache_dir, "o", "r", "v4", Some(DIGEST_A));
        assert_eq!(archive_sha256_pin_for(&state, "o/r", Some("v4")), None);
    }

    /// Restart resilience: the in-memory pin table is rebuilt from the
    /// on-disk digest sidecar on the next cache hit, without re-downloading.
    #[tokio::test]
    async fn pin_backfills_from_sidecar_on_cache_hit() {
        let state = test_state().await;
        let cache_dir = state
            .state_dir
            .join("actions")
            .join("o")
            .join("r")
            .join(SHA);
        std::fs::create_dir_all(&cache_dir).unwrap();
        std::fs::write(action_archive_digest_sidecar(&cache_dir), DIGEST_A).unwrap();

        // Fresh state: no pin yet (as after an engine restart).
        assert_eq!(archive_sha256_pin_for(&state, "o/r", Some(SHA)), None);

        ensure_action_archive_pin(&state, &cache_dir, "o", "r", SHA, None);

        assert_eq!(
            archive_sha256_pin_for(&state, "o/r", Some(SHA)),
            Some(DIGEST_A.to_string())
        );
    }

    /// A missing or malformed sidecar leaves the action unpinned instead of
    /// failing — the download still works, just unverified.
    #[tokio::test]
    async fn missing_or_malformed_sidecar_leaves_unpinned() {
        let state = test_state().await;
        let cache_dir = state
            .state_dir
            .join("actions")
            .join("o")
            .join("r")
            .join(SHA);
        std::fs::create_dir_all(&cache_dir).unwrap();

        ensure_action_archive_pin(&state, &cache_dir, "o", "r", SHA, None);
        assert_eq!(archive_sha256_pin_for(&state, "o/r", Some(SHA)), None);

        std::fs::write(action_archive_digest_sidecar(&cache_dir), "not-a-digest").unwrap();
        ensure_action_archive_pin(&state, &cache_dir, "o", "r", SHA, None);
        assert_eq!(archive_sha256_pin_for(&state, "o/r", Some(SHA)), None);
    }

    /// The pin table is bounded: when full, new pins are skipped (fail open
    /// on the cap) while existing pins keep enforcing.
    #[tokio::test]
    async fn pin_table_cap_skips_new_pins_when_full() {
        let state = test_state().await;
        {
            let mut pins = state.action_archive_sha256_pins.lock().unwrap();
            for i in 0..MAX_DIGEST_PINS {
                let sha = format!("{i:040x}");
                pins.insert(
                    ("o".to_string(), "r".to_string(), sha),
                    DIGEST_A.to_string(),
                );
            }
        }
        let cache_dir = state
            .state_dir
            .join("actions")
            .join("o")
            .join("r")
            .join(SHA);
        std::fs::create_dir_all(&cache_dir).unwrap();

        ensure_action_archive_pin(&state, &cache_dir, "o", "r", SHA, Some(DIGEST_B));
        assert_eq!(
            archive_sha256_pin_for(&state, "o/r", Some(SHA)),
            None,
            "a full pin table must not accept new pins"
        );
        // Existing pins still enforce.
        let first_sha = format!("{:040x}", 0);
        assert_eq!(
            archive_sha256_pin_for(&state, "o/r", Some(&first_sha)),
            Some(DIGEST_A.to_string())
        );
    }

    /// `action_tarball_response` attests the engine-observed digest in the
    /// download header when known, and omits the header otherwise — a
    /// missing digest never fails the download.
    #[tokio::test]
    async fn tarball_response_attests_digest_header() {
        let res =
            action_tarball_response("repo", SHA, Body::from("bytes"), Some(DIGEST_A)).unwrap();
        assert_eq!(
            res.headers()
                .get(ACTION_ARCHIVE_SHA256_HEADER)
                .and_then(|v| v.to_str().ok()),
            Some(DIGEST_A)
        );

        let res = action_tarball_response("repo", SHA, Body::from("bytes"), None).unwrap();
        assert!(
            res.headers().get(ACTION_ARCHIVE_SHA256_HEADER).is_none(),
            "no digest must mean no header, not a failed download"
        );
    }
}
