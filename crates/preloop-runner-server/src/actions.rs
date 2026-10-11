use super::*;
use futures::StreamExt;
use preloop_gha_protocol::azdo::{
    ActionDownloadInfo, ActionDownloadInfoCollection, ActionReferenceList,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// POST action download info — resolve action references to download URLs.
///
/// Tickets are minted only for actions declared in the run's workflow
/// (`uses:`, transitively through reusable callees and composite nests);
/// any other requested action fails the whole batch with 403 — a job
/// runtime token must not turn the engine into a fetch oracle for
/// arbitrary repositories on the operator's API quota. The run is
/// identified by the `plan_id` path segment; an unknown plan fails closed.
pub async fn action_download_info(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id)): Path<(String, String, String)>,
    Json(request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    action_download_info_for_plan(&shared, &plan_id, &request).await
}

pub(crate) async fn action_download_info_for_plan(
    shared: &Arc<SharedState>,
    plan_id: &str,
    request: &serde_json::Value,
) -> Result<Json<serde_json::Value>, ApiError> {
    let collection = collect_action_download_infos(&shared.state, plan_id, request).await?;
    Ok(Json(
        serde_json::to_value(collection).unwrap_or_else(|_| json!({ "actions": {} })),
    ))
}

pub async fn runnerresolve_actions(
    State(shared): State<Arc<SharedState>>,
    Path((orchestration_id, job_id)): Path<(String, String)>,
    Json(request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mut actions = serde_json::Map::new();
    collect_runnerresolve_refs(
        &shared.state,
        &orchestration_id,
        &job_id,
        &request,
        &mut actions,
    )
    .await?;

    Ok(Json(json!({ "actions": actions })))
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
/// when the resolve-time pin was Unpinned: the digest is computed
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
/// attestation even when its resolve-time pin was Unpinned: a
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

/// How long a minted archive ticket stays valid: 20 minutes. Actions are
/// fetched during job setup, so the ticket only has to outlive a queue wait
/// plus the setup waves (a composite action's nested refs resolve in later
/// waves, minutes after the first), not a whole run. Short enough that a
/// leaked bearerless ticket URL is a briefly-open window, not a 6-hour one.
pub const ACTION_TICKET_TTL_SECS: u64 = 20 * 60;
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

/// Whether the static PAT may be attached to a request for `url`: the URL
/// must target a configured GitHub host — any of the `github_urls`
/// endpoints, compared by host and effective port — over a transport that
/// keeps the PAT off the wire.
///
/// HTTPS always qualifies. Plain HTTP qualifies only for a loopback host:
/// the PAT has to follow the engine onto a local GitHub emulator
/// (gh-simulate local mode, `http://127.0.0.1:…`), but a configured
/// *remote* `http://` origin would carry the PAT across the network in
/// cleartext. An unrelated origin never qualifies, whatever the scheme.
fn url_targets_configured_github(url: &str, urls: &GitHubUrls) -> bool {
    let Ok(target) = reqwest::Url::parse(url) else {
        return false;
    };
    let Some(target_host) = target.host_str() else {
        return false;
    };
    let transport_ok = match target.scheme() {
        "https" => true,
        "http" => is_loopback_host(target_host),
        _ => false,
    };
    transport_ok
        && [&urls.api_url, &urls.server_url, &urls.graphql_url]
            .iter()
            .filter_map(|configured| reqwest::Url::parse(configured).ok())
            .any(|configured| {
                configured.host_str() == Some(target_host)
                    && configured.port_or_known_default() == target.port_or_known_default()
            })
}

/// `localhost`, or a literal IPv4/IPv6 loopback address (`127.0.0.0/8`,
/// `::1`). `host_str` keeps IPv6 literals bracketed.
fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
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
    if let Ok(cache) = state.action_sha_cache.lock()
        && let Some((sha, at)) = cache.get(&cache_key)
        && sha_entry_fresh(sha, *at)
    {
        return sha.clone();
    }

    let enc_owner = percent_encode_path_segment(owner);
    let enc_repo = percent_encode_path_segment(repo);
    let enc_git_ref = percent_encode_path_segment(git_ref);
    let api_base = state.github_urls.api_url.trim_end_matches('/').to_owned();
    let url = format!("{api_base}/repos/{enc_owner}/{enc_repo}/commits/{enc_git_ref}");
    let mut request = crate::shared_http::CLIENT.get(&url);
    if let Some(pat) = state.static_github_pat()
        && url_targets_configured_github(&url, &state.github_urls)
    {
        request = request.bearer_auth(pat);
    }
    let response = match crate::github_breaker::send_observed_labeled(
        &state.github_breaker,
        &state.github_consumption,
        crate::github_breaker::GithubSubsystem::RefResolve,
        request,
    )
    .await
    {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%owner, %repo, %git_ref, %error, "action ref resolution request failed; falling back to ref");
            return None;
        }
    };
    state.github_consumption.record_advertised_bytes(
        crate::github_breaker::GithubSubsystem::RefResolve,
        &response,
    );
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

    let client = crate::shared_http::github_client_builder()
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
    if let Some(pat) = shared.state.static_github_pat()
        && url_targets_configured_github(&github_url, &shared.state.github_urls)
    {
        request = request.bearer_auth(pat);
    }
    let response = match crate::github_breaker::send_observed_labeled(
        &shared.state.github_breaker,
        &shared.state.github_consumption,
        crate::github_breaker::GithubSubsystem::Actions,
        request,
    )
    .await
    {
        Ok(response) => response,
        // A tripped breaker fails fast with 503 instead of queueing behind
        // the outage; runners retry the download on the next attempt.
        Err(error) if shared.state.github_breaker.is_open() => {
            return Err(ApiError::service_unavailable(format!(
                "GitHub is temporarily unavailable: {error:#}"
            )));
        }
        Err(error) => {
            return Err(ApiError::internal(format!(
                "failed to send download request to GitHub: {error:#}"
            )));
        }
    };

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
    let mut streamed_bytes: u64 = 0;
    let mut stream = response.bytes_stream();
    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| {
            ApiError::internal(format!("failed to read chunk from GitHub response: {e}"))
        })?;
        // Hash the archive bytes as they arrive, so the digest the engine
        // pins is computed over exactly the bytes written to the cache —
        // no second read, no TOCTOU between write and hash.
        hasher.update(&chunk);
        streamed_bytes = streamed_bytes.saturating_add(chunk.len() as u64);
        tokio::io::copy(&mut &chunk[..], &mut temp_file)
            .await
            .map_err(|e| {
                ApiError::internal(format!("failed to write chunk to temporary file: {e}"))
            })?;
    }
    let digest = format!("{:x}", hasher.finalize());
    // Exact byte count, measured off the stream — the one true number for
    // tarball bandwidth accounting.
    shared.state.github_consumption.record_bytes(
        crate::github_breaker::GithubSubsystem::Actions,
        streamed_bytes,
    );

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
    // action, and short-lived (20 minutes — minted at job setup and used
    // within minutes, so a leaked URL is a briefly-open window).
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
    orchestration_id: &str,
    job_id: &str,
    value: &serde_json::Value,
    actions: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<(), ApiError> {
    let mut requests: Vec<(String, Option<String>)> = Vec::new();
    collect_runnerresolve_requests(value, &mut requests);
    let mut seen = std::collections::HashSet::new();
    requests.retain(|request| seen.insert(request.clone()));
    requests.truncate(MAX_ACTION_BATCH_SIZE);

    let (run_id, declared) =
        declared_actions_for_runnerresolve(state, orchestration_id, job_id).await?;
    enforce_declared_actions(state, run_id, &declared, &requests).await?;

    let stream = futures::stream::iter(requests.into_iter().map(|(action, version)| async move {
        runnerresolve_action(state, &action, version.as_deref()).await
    }))
    .buffer_unordered(MAX_ACTION_CONCURRENCY);

    let resolved: Vec<Option<(String, serde_json::Value)>> = stream.collect().await;
    for (key, value) in resolved.into_iter().flatten() {
        actions.entry(key).or_insert(value);
    }
    Ok(())
}

/// Batch collector for the official JobServer `ActionDownloadInfo` endpoint.
///
/// Every requested action must be declared in the run's workflow (see
/// [`enforce_declared_actions`]); an undeclared action fails the whole
/// batch with 403 instead of minting a ticket.
pub async fn collect_action_download_infos(
    state: &AppState,
    plan_id: &str,
    value: &serde_json::Value,
) -> Result<ActionDownloadInfoCollection, ApiError> {
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

    let (run_id, declared) = declared_actions_for_plan(state, plan_id).await?;
    enforce_declared_actions(state, run_id, &declared, &requests).await?;

    let stream = futures::stream::iter(requests.into_iter().map(|(action, version)| async move {
        action_download_info_entry(state, &action, version.as_deref()).await
    }))
    .buffer_unordered(MAX_ACTION_CONCURRENCY);

    let resolved: Vec<Option<(String, ActionDownloadInfo)>> = stream.collect().await;
    let mut actions = BTreeMap::new();
    for (key, value) in resolved.into_iter().flatten() {
        actions.insert(key, value);
    }

    Ok(ActionDownloadInfoCollection { actions })
}

/// Normalize a `uses:` reference to its `owner/repo` download scope
/// (lowercased), or `None` when the reference names no fixed remote
/// repository: local (`./`, `../`), `docker://`, self-repo (`$/`),
/// expression-driven owners (`${{ }}` in the repo part), and malformed
/// references. A subpath (`owner/repo/sub/dir@ref`) normalizes to
/// `owner/repo`: the tarball — and the quota it burns — is per repository.
pub fn normalize_action_owner_repo(uses: &str) -> Option<String> {
    let uses = uses.trim();
    if uses.is_empty() {
        return None;
    }
    for prefix in ["./", "../", "docker://", "$/"] {
        if uses.starts_with(prefix) {
            return None;
        }
    }
    // The ref may be dynamic (`@${{ matrix.ref }}`); only the repo part has
    // to be static — enforcement is owner/repo-scoped, never ref-pinned.
    let repo_part = uses.split('@').next().unwrap_or(uses);
    if repo_part.contains("${{") {
        return None;
    }
    let mut segments = repo_part.split('/');
    let owner = segments.next()?;
    let repo = segments.next()?;
    for segment in [owner, repo] {
        if segment.is_empty()
            || segment == "."
            || segment == ".."
            || segment.contains('\\')
            || segment.contains('\0')
        {
            return None;
        }
    }
    Some(format!("{}/{}", owner.to_lowercase(), repo.to_lowercase()))
}

/// Normalize a bare `owner/repo` slug (e.g. the run's repository) to its
/// lowercased form, or `None` when it is not exactly two clean segments.
fn normalize_repo_slug(slug: &str) -> Option<String> {
    let slug = slug.trim().trim_matches('/');
    let mut segments = slug.split('/');
    let owner = segments.next()?;
    let repo = segments.next()?;
    if segments.next().is_some() {
        return None;
    }
    for segment in [owner, repo] {
        if segment.is_empty()
            || segment == "."
            || segment == ".."
            || segment.contains('\\')
            || segment.contains('\0')
        {
            return None;
        }
    }
    Some(format!("{}/{}", owner.to_lowercase(), repo.to_lowercase()))
}

/// Maximum nesting depth when following reusable-workflow callees while
/// collecting a run's declared actions. Mirrors the submit-time
/// `MAX_REUSABLE_WORKFLOW_DEPTH` so the allowlist sees the same tree the
/// expander did.
const DECLARED_ACTION_REUSABLE_DEPTH: usize = 4;

/// The actions a run may download, named directly by its workflow: every
/// `owner/repo` in a step `uses:` or a job-level reusable `uses:`
/// (transitively through local and already-fetched remote callee YAML),
/// plus the workflow's own repository — `$/path` self-references resolve
/// to it through the forge and the runner asks the server to resolve them.
///
/// Derived from the run record's stored submission, so it needs no extra
/// per-run state and survives engine restarts. A workflow that cannot be
/// (re)parsed yields an empty set: fail closed, never fail open.
fn direct_declared_actions(
    workflow_yaml: &str,
    reusable_workflows: &BTreeMap<String, String>,
    repository: &str,
) -> BTreeSet<String> {
    let mut declared = BTreeSet::new();
    let workflow = match preloop_gha_parser::parse_workflow(workflow_yaml) {
        Ok(workflow) => workflow,
        Err(error) => {
            warn!("cannot parse stored workflow for declared-action allowlist: {error:#}");
            return declared;
        }
    };
    // `$/path` self-references resolve to the workflow's own repository.
    if let Some(slug) = normalize_repo_slug(repository) {
        declared.insert(slug);
    }
    let mut visited = BTreeSet::new();
    collect_workflow_declared(
        &workflow,
        reusable_workflows,
        &mut declared,
        &mut visited,
        0,
    );
    declared
}

fn collect_workflow_declared(
    workflow: &preloop_gha_parser::Workflow,
    reusable_workflows: &BTreeMap<String, String>,
    declared: &mut BTreeSet<String>,
    visited: &mut BTreeSet<String>,
    depth: usize,
) {
    if depth > DECLARED_ACTION_REUSABLE_DEPTH {
        return;
    }
    for job in workflow.jobs.values() {
        // A job-level `uses:` is a reusable-workflow call: the callee repo
        // is declared, and the callee's own steps are too (recurse into the
        // callee YAML stashed on the submission at submit time).
        if let Some(uses) = job.uses.as_deref() {
            if let Some(owner_repo) = normalize_action_owner_repo(uses) {
                declared.insert(owner_repo);
            }
            if visited.insert(uses.to_owned())
                && let Some(yaml) = reusable_callee_yaml(uses, reusable_workflows)
                && let Ok(callee) = preloop_gha_parser::parse_workflow(yaml)
            {
                collect_workflow_declared(
                    &callee,
                    reusable_workflows,
                    declared,
                    visited,
                    depth + 1,
                );
            }
        }
        for step in &job.steps {
            if let Some(uses) = step.uses.as_deref()
                && let Some(owner_repo) = normalize_action_owner_repo(uses)
            {
                declared.insert(owner_repo);
            }
        }
    }
}

/// Callee YAML for a job-level reusable `uses:`: remote callees are keyed
/// by the full reference string (stashed by submit-time fetching); local
/// callees (`./`, `$/`) are keyed by repository-relative path.
fn reusable_callee_yaml<'a>(
    uses: &str,
    reusable_workflows: &'a BTreeMap<String, String>,
) -> Option<&'a String> {
    if let Some(yaml) = reusable_workflows.get(uses) {
        return Some(yaml);
    }
    let without_ref = uses.split('@').next().unwrap_or(uses);
    let path = without_ref
        .strip_prefix("./")
        .or_else(|| without_ref.strip_prefix("$/"))?;
    // Lexically normalize the relative path the way the expander does, so
    // `./a/../b/c.yml` finds the `b/c.yml` key.
    let normalized = std::path::Path::new(path)
        .components()
        .collect::<std::path::PathBuf>()
        .to_string_lossy()
        .into_owned();
    reusable_workflows
        .get(&normalized)
        .or_else(|| reusable_workflows.get(path))
}

/// Resolve the run behind a JobServer `plan_id` path segment and return its
/// declared action allowlist. An unknown plan fails closed: without a run
/// there is no declared set to mint against.
async fn declared_actions_for_plan(
    state: &AppState,
    plan_id: &str,
) -> Result<(RunId, BTreeSet<String>), ApiError> {
    use crate::control::backend::RequestKey;
    let run_id = match state
        .backend
        .request(RequestKey::PlanId(plan_id.to_owned()))
        .await
    {
        Ok(request) => request.run_id,
        Err(crate::control::ControlError::NotFound(_)) => {
            return Err(ApiError::forbidden(format!(
                "action downloads are not permitted for unknown plan `{plan_id}`"
            )));
        }
        Err(error) => return Err(ApiError::from(error)),
    };
    let declared = declared_actions_for_run_id(state, run_id).await?;
    Ok((run_id, declared))
}

/// Resolve the run behind a `runnerresolve` (`orchestration_id`, `job_id`)
/// path pair and return its declared action allowlist. Prefers the job id
/// (exact), falling back to the orchestration plan id.
async fn declared_actions_for_runnerresolve(
    state: &AppState,
    orchestration_id: &str,
    job_id: &str,
) -> Result<(RunId, BTreeSet<String>), ApiError> {
    use crate::control::backend::RequestKey;
    let mut run_id = None;
    if let Ok(agent_job_id) = job_id.parse::<uuid::Uuid>()
        && let Ok(request) = state
            .backend
            .request(RequestKey::AgentJobId(agent_job_id))
            .await
    {
        run_id = Some(request.run_id);
    }
    let run_id = match run_id {
        Some(run_id) => run_id,
        None => match state
            .backend
            .request(RequestKey::PlanId(orchestration_id.to_owned()))
            .await
        {
            Ok(request) => request.run_id,
            Err(crate::control::ControlError::NotFound(_)) => {
                return Err(ApiError::forbidden(format!(
                    "action downloads are not permitted for unknown job `{job_id}`"
                )));
            }
            Err(error) => return Err(ApiError::from(error)),
        },
    };
    let declared = declared_actions_for_run_id(state, run_id).await?;
    Ok((run_id, declared))
}

async fn declared_actions_for_run_id(
    state: &AppState,
    run_id: RunId,
) -> Result<BTreeSet<String>, ApiError> {
    let run = state
        .backend
        .run_record(run_id)
        .await
        .map_err(ApiError::from)?;
    Ok(direct_declared_actions(
        &run.submission.workflow_yaml,
        &run.submission.reusable_workflows,
        &run.submission.repository,
    ))
}

/// Cap on a fetched action manifest's decoded bytes. Manifests are small
/// YAML files; anything larger is not a manifest we should parse.
const ACTION_MANIFEST_MAX_BYTES: usize = 256 * 1024;

/// Nested `uses:` scopes from a composite action's manifest, memoized per
/// (`owner`, `repo`, `ref`). Only definitive answers are cached (see
/// [`ActionManifestNestedUses`]); a transient failure returns an empty set
/// uncached so the next adjudication retries.
async fn nested_action_uses(
    state: &AppState,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> BTreeSet<String> {
    let key = (
        owner.to_lowercase(),
        repo.to_lowercase(),
        git_ref.to_owned(),
    );
    if let Ok(cache) = state.action_manifest_nested_uses.lock()
        && let Some(cached) = cache.get(&key)
    {
        return cached.clone();
    }
    // A transient failure yields `None`: fail closed for this adjudication
    // without poisoning the cache — the next wave retries the fetch.
    // Definitive answers (even empty: a non-composite action, or a repo
    // with no manifest, never grows nested uses at this ref) are cached.
    let Some(nested) = fetch_action_manifest_nested_uses(state, owner, repo, git_ref).await else {
        return BTreeSet::new();
    };
    if let Ok(mut cache) = state.action_manifest_nested_uses.lock() {
        cache.insert(key, nested.clone());
    }
    nested
}

/// Fetch `action.yml`/`action.yaml` for (`owner`, `repo`, `ref`) through
/// the forge contents API and extract nested `uses:` scopes when the action
/// is composite. Returns `None` on transient failures (not cached);
/// `Some` (possibly empty) for definitive answers: a manifest was read, or
/// no manifest exists (404), or the forge definitively refused.
async fn fetch_action_manifest_nested_uses(
    state: &AppState,
    owner: &str,
    repo: &str,
    git_ref: &str,
) -> Option<BTreeSet<String>> {
    // The (owner, repo) pair reaching here was already resolved for this run
    // (it is in the run's declared set or was admitted through it), so
    // fetching its manifest spends quota the run's own execution would
    // spend anyway — never on an attacker's arbitrary repository.
    let api_base = state.github_urls.api_url.trim_end_matches('/').to_owned();
    let enc_owner = percent_encode_path_segment(owner);
    let enc_repo = percent_encode_path_segment(repo);
    for manifest in ["action.yml", "action.yaml"] {
        let url = format!(
            "{api_base}/repos/{enc_owner}/{enc_repo}/contents/{manifest}?ref={}",
            percent_encode_path_segment(git_ref)
        );
        let mut request = crate::shared_http::CLIENT.get(&url);
        if let Some(pat) = state.static_github_pat()
            && url_targets_configured_github(&url, &state.github_urls)
        {
            request = request.bearer_auth(pat);
        }
        let response = match crate::github_breaker::send_observed_labeled(
            &state.github_breaker,
            &state.github_consumption,
            crate::github_breaker::GithubSubsystem::Actions,
            request,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                tracing::debug!(%owner, %repo, %git_ref, %error, "action manifest fetch failed; denying nested uses for this wave");
                return None;
            }
        };
        state
            .github_consumption
            .record_advertised_bytes(crate::github_breaker::GithubSubsystem::Actions, &response);
        let status = response.status();
        if status.is_success() {
            let body = match response.json::<serde_json::Value>().await {
                Ok(body) => body,
                Err(_) => return Some(BTreeSet::new()),
            };
            return Some(manifest_nested_uses(&body));
        }
        if status == reqwest::StatusCode::NOT_FOUND {
            continue; // Try the other manifest filename.
        }
        if status.is_server_error()
            || status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
        {
            return None; // Transient: do not cache, retry next time.
        }
        // Definitive refusal (401/403/422 on a private repo without a PAT,
        // …): no readable manifest, hence no nested uses.
        return Some(BTreeSet::new());
    }
    // Neither manifest exists: not a composite action (or no manifest at
    // all) — definitively no nested uses.
    Some(BTreeSet::new())
}

/// Look up `key` in a YAML mapping.
fn yaml_get<'a>(value: &'a serde_yaml::Value, key: &str) -> Option<&'a serde_yaml::Value> {
    value
        .as_mapping()?
        .get(serde_yaml::Value::String(key.to_owned()))
}

/// Extract nested `uses:` scopes from a forge contents-API response for an
/// action manifest. Only composite actions (`runs.using: composite`) can
/// nest remote `uses:`; anything else — Node/Docker actions, unparseable
/// bodies — yields an empty set.
fn manifest_nested_uses(body: &serde_json::Value) -> BTreeSet<String> {
    let mut nested = BTreeSet::new();
    let content = body.get("content").and_then(|value| value.as_str());
    let Some(encoded) = content else {
        return nested;
    };
    // The contents API wraps base64 at 60 columns; strip whitespace before
    // decoding and cap the decoded size — this is a small YAML file.
    let cleaned: String = encoded.chars().filter(|ch| !ch.is_whitespace()).collect();
    let decoded = match base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        cleaned.as_bytes(),
    ) {
        Ok(decoded) if decoded.len() <= ACTION_MANIFEST_MAX_BYTES => decoded,
        _ => return nested,
    };
    let manifest: serde_yaml::Value = match serde_yaml::from_slice(&decoded) {
        Ok(manifest) => manifest,
        Err(_) => return nested,
    };
    let runs = yaml_get(&manifest, "runs");
    let using_is_composite = runs
        .and_then(|runs| yaml_get(runs, "using"))
        .and_then(|using| using.as_str())
        .is_some_and(|using| using == "composite");
    if !using_is_composite {
        return nested;
    }
    let steps = runs.and_then(|runs| yaml_get(runs, "steps"));
    let Some(steps) = steps.and_then(|steps| steps.as_sequence()) else {
        return nested;
    };
    for step in steps {
        if let Some(uses) = yaml_get(step, "uses").and_then(|uses| uses.as_str())
            && let Some(owner_repo) = normalize_action_owner_repo(uses)
        {
            nested.insert(owner_repo);
        }
    }
    nested
}

/// Enforce the run's declared-action allowlist on one resolution batch.
///
/// Every requested remote action must be in `declared`, or be nested inside
/// the manifest of an action this run already resolved (the transitive
/// composite closure: a composite action's nested `uses:` arrive in later
/// waves, after the parent's own download, so they cannot be in the
/// workflow's direct set). Anything else fails the whole batch with 403 —
/// no ticket is minted for an undeclared action, closing the download
/// oracle.
///
/// Requests that are not remote actions (local `./`, `docker://`,
/// self-repo `$/`, expression-driven) are left to the resolver, which drops
/// them exactly as before: they can never mint a ticket.
///
/// On success, this batch's admitted (`owner/repo`, `ref`) triples are
/// recorded on the run's resolution ledger for later waves.
async fn enforce_declared_actions(
    state: &AppState,
    run_id: RunId,
    declared: &BTreeSet<String>,
    requests: &[(String, Option<String>)],
) -> Result<(), ApiError> {
    // (batch index, owner/repo, ref) for requests naming a remote action.
    let mut remote: Vec<(usize, String, String)> = Vec::new();
    for (index, (action, version)) in requests.iter().enumerate() {
        if let Some(owner_repo) = normalize_action_owner_repo(action) {
            remote.push((index, owner_repo, version.clone().unwrap_or_default()));
        }
    }

    let mut admitted: BTreeSet<usize> = BTreeSet::new();
    // (`owner/repo`, `ref`) triples whose manifests may authorize nested
    // uses: this batch's direct hits plus earlier batches' ledger.
    let mut manifest_sources: Vec<(String, String)> = Vec::new();
    for (index, owner_repo, git_ref) in &remote {
        if declared.contains(owner_repo) {
            admitted.insert(*index);
            manifest_sources.push((owner_repo.clone(), git_ref.clone()));
        }
    }

    // Fast path: everything directly declared — no manifest fetch needed.
    // The transitive closure below only runs when some request is not
    // directly declared, so ordinary job setups never pay for it.
    let mut unadmitted: Vec<(usize, String, String)> = remote
        .iter()
        .filter(|(index, _, _)| !admitted.contains(index))
        .cloned()
        .collect();
    if !unadmitted.is_empty() {
        // Seed cross-batch sources: nested refs arrive in later waves, after
        // the parent's own download, so earlier batches' ledger entries must
        // be consultable here.
        if let Ok(ledger) = state.action_resolved_uses.lock()
            && let Some(prior) = ledger.get(&run_id)
        {
            manifest_sources.extend(prior.iter().cloned());
        }
        // Transitive composite closure, to a fixpoint within the batch: each
        // newly admitted action's manifest may admit further nested actions.
        let mut checked: BTreeSet<(String, String)> = BTreeSet::new();
        loop {
            let mut nested_union: BTreeSet<String> = BTreeSet::new();
            let mut newly_checked = false;
            for (owner_repo, git_ref) in &manifest_sources {
                if !checked.insert((owner_repo.clone(), git_ref.clone())) {
                    continue;
                }
                newly_checked = true;
                // `normalize_action_owner_repo` always yields `owner/repo`.
                let (owner, repo) = owner_repo
                    .split_once('/')
                    .unwrap_or((owner_repo.as_str(), ""));
                nested_union.extend(nested_action_uses(state, owner, repo, git_ref).await);
            }
            if !newly_checked {
                break;
            }
            let mut progressed = false;
            unadmitted.retain(|(index, owner_repo, git_ref)| {
                if nested_union.contains(owner_repo) {
                    admitted.insert(*index);
                    manifest_sources.push((owner_repo.clone(), git_ref.clone()));
                    progressed = true;
                    false
                } else {
                    true
                }
            });
            if !progressed {
                break;
            }
        }
    }

    let denied: Vec<String> = remote
        .iter()
        .filter(|(index, _, _)| !admitted.contains(index))
        .map(|(_, owner_repo, git_ref)| {
            if git_ref.is_empty() {
                owner_repo.clone()
            } else {
                format!("{owner_repo}@{git_ref}")
            }
        })
        .collect();
    if !denied.is_empty() {
        warn!(
            "action download denied for run {}: {} not declared in the run's workflow `uses:`",
            run_id.0,
            denied.join(", ")
        );
        return Err(ApiError::forbidden(format!(
            "action download not permitted: {} not declared in this run's workflow `uses:`",
            denied.join(", ")
        )));
    }

    // Ledger this batch's admitted triples for later waves' closures.
    if let Ok(mut ledger) = state.action_resolved_uses.lock() {
        let entry = ledger.entry(run_id).or_default();
        for (index, owner_repo, git_ref) in &remote {
            if admitted.contains(index) && !entry.contains(&(owner_repo.clone(), git_ref.clone())) {
                entry.push((owner_repo.clone(), git_ref.clone()));
            }
        }
        if entry.len() > crate::state::MAX_RESOLVED_USES_PER_RUN {
            let excess = entry.len() - crate::state::MAX_RESOLVED_USES_PER_RUN;
            entry.drain(..excess);
        }
    }
    Ok(())
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
    // Never echo the mutable ref back as `resolved_sha`. A caller that trusts the
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

    /// The static PAT follows the engine onto a configured plain-http
    /// GitHub emulator on loopback, but never to an unrelated host (or a
    /// lookalike), and never over plain http to a remote host.
    #[test]
    fn pat_targets_configured_github_over_a_safe_transport() {
        let sim = GitHubUrls {
            server_url: "http://127.0.0.1:8888".to_string(),
            api_url: "http://127.0.0.1:8888".to_string(),
            graphql_url: "http://127.0.0.1:8888".to_string(),
        };
        assert!(url_targets_configured_github(
            "http://127.0.0.1:8888/repos/o/r/tarball/main",
            &sim
        ));
        assert!(!url_targets_configured_github(
            "http://127.0.0.1:9999/repos/o/r/tarball/main",
            &sim
        ));
        assert!(!url_targets_configured_github(
            "https://evil.example.com/repos/o/r/tarball/main",
            &sim
        ));

        // Real github.com: still attaches over https, and a different host
        // (even a lookalike) does not.
        let real = GitHubUrls {
            server_url: "https://github.com".to_string(),
            api_url: "https://api.github.com".to_string(),
            graphql_url: "https://api.github.com".to_string(),
        };
        assert!(url_targets_configured_github(
            "https://api.github.com/repos/o/r/commits/main",
            &real
        ));
        assert!(!url_targets_configured_github(
            "https://github.com.evil.example/repos/o/r/commits/main",
            &real
        ));
        assert!(!url_targets_configured_github("not a url", &real));

        // A configured *remote* emulator over plain http would carry the PAT
        // in cleartext: refused. The same origin over https qualifies.
        let remote = GitHubUrls {
            server_url: "http://ghsim.internal:8888".to_string(),
            api_url: "http://ghsim.internal:8888".to_string(),
            graphql_url: "http://ghsim.internal:8888".to_string(),
        };
        assert!(!url_targets_configured_github(
            "http://ghsim.internal:8888/repos/o/r/tarball/main",
            &remote
        ));
        let remote_tls = GitHubUrls {
            server_url: "https://ghes.internal".to_string(),
            api_url: "https://ghes.internal/api/v3".to_string(),
            graphql_url: "https://ghes.internal/api/graphql".to_string(),
        };
        assert!(url_targets_configured_github(
            "https://ghes.internal/api/v3/repos/o/r/commits/main",
            &remote_tls
        ));
        // A plain-http request to a configured https host is a downgrade.
        assert!(!url_targets_configured_github(
            "http://api.github.com/repos/o/r/commits/main",
            &real
        ));

        // Every loopback spelling qualifies over plain http.
        for base in [
            "http://localhost:8888",
            "http://[::1]:8888",
            "http://127.0.0.2:8888",
        ] {
            let local = GitHubUrls {
                server_url: base.to_string(),
                api_url: base.to_string(),
                graphql_url: base.to_string(),
            };
            assert!(
                url_targets_configured_github(&format!("{base}/repos/o/r/tarball/main"), &local),
                "{base} is loopback"
            );
        }
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

    /// Stub "GitHub" for the tarball download path: serves one scripted
    /// response on the tarball route and counts hits, so tests prove the
    /// breaker engages without touching the real github.com.
    struct TarballStub {
        base: String,
        hits: Arc<std::sync::atomic::AtomicU64>,
    }

    impl TarballStub {
        async fn serve(status: StatusCode, headers: Vec<(String, String)>, body: Vec<u8>) -> Self {
            let hits = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let hits_route = hits.clone();
            let app = axum::Router::new().route(
                "/repos/o/r/tarball/main",
                axum::routing::any(move || {
                    let hits_route = hits_route.clone();
                    async move {
                        hits_route.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let mut builder = axum::http::Response::builder().status(status);
                        for (name, value) in &headers {
                            builder = builder.header(name, value);
                        }
                        builder.body(axum::body::Body::from(body)).unwrap()
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            Self { base, hits }
        }

        fn hits(&self) -> u64 {
            self.hits.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// Shared state pointed at the stub instead of the real GitHub. The
    /// download path reads `github_urls` live, so setting the struct field
    /// needs no env vars and cannot leak into other tests.
    async fn tarball_shared(stub_base: &str) -> (Arc<SharedState>, tempfile::TempDir) {
        let temp = tempfile::tempdir().unwrap();
        let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        state.github_urls = crate::state::GitHubUrls {
            server_url: stub_base.to_owned(),
            api_url: stub_base.to_owned(),
            graphql_url: stub_base.to_owned(),
        };
        (
            Arc::new(SharedState {
                state,
                shutdown: tokio_util::sync::CancellationToken::new(),
            }),
            temp,
        )
    }

    /// Extractor tuple for one `download_action_tarball` call.
    type DownloadCall = (
        State<Arc<SharedState>>,
        Path<(String, String, String)>,
        Query<ActionTicketQuery>,
    );

    fn download_call(shared: &Arc<SharedState>) -> DownloadCall {
        let expires_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let sig = shared
            .state
            .sign_action_ticket("o", "r", "main", expires_at);
        (
            State(shared.clone()),
            Path(("o".to_owned(), "r".to_owned(), "main".to_owned())),
            Query(ActionTicketQuery {
                exp: Some(expires_at),
                sig: Some(sig),
            }),
        )
    }

    fn actions_usage(shared: &SharedState) -> crate::github_breaker::GithubSubsystemUsage {
        shared
            .state
            .github_consumption
            .snapshot()
            .into_iter()
            .find(|usage| usage.subsystem == "actions")
            .expect("actions subsystem must be in the snapshot")
    }

    /// A 429 with `x-ratelimit-reset` on the tarball route trips the shared
    /// breaker immediately; the next download fails fast without a second
    /// GitHub hit, and the accounting attributes everything to `actions`.
    /// Pre-fix this path had no breaker at all and hammered the 429.
    #[tokio::test]
    async fn tarball_download_429_engages_breaker() {
        let reset = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        let stub = TarballStub::serve(
            StatusCode::TOO_MANY_REQUESTS,
            vec![("x-ratelimit-reset".to_owned(), reset.to_string())],
            b"rate limited".to_vec(),
        )
        .await;
        let (shared, _temp) = tarball_shared(&stub.base).await;

        let (state, path, query) = download_call(&shared);
        let err = download_action_tarball(state, path, query)
            .await
            .expect_err("a 429 tarball fetch must fail the download");
        // The 429 response itself still maps the way it did pre-fix; the
        // behavior change is the breaker tripping, not the status code.
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert!(
            shared.state.github_breaker.is_open(),
            "the 429 must trip the breaker at once, not after a threshold"
        );
        assert!(shared.state.github_breaker.snapshot().rate_limited);

        // The breaker is open now: the next download fails fast with 503
        // instead of spending another request on the guaranteed 429.
        let (state, path, query) = download_call(&shared);
        let err = download_action_tarball(state, path, query)
            .await
            .expect_err("breaker-open must fail fast");
        assert_eq!(err.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            err.message().contains("circuit breaker is open"),
            "unexpected error: {}",
            err.message()
        );
        assert_eq!(
            stub.hits(),
            1,
            "only the first exchange may reach GitHub; the second is breaker-blocked"
        );

        let usage = actions_usage(&shared);
        assert_eq!(usage.requests, 1);
        assert_eq!(usage.rate_limited, 1);
        assert_eq!(usage.breaker_blocked, 1);
        // Other subsystems are not charged for the actions path's 429.
        for other in shared
            .state
            .github_consumption
            .snapshot()
            .into_iter()
            .filter(|usage| usage.subsystem != "actions")
        {
            assert_eq!(other.requests, 0, "{} must not be charged", other.subsystem);
        }
    }

    /// A successful tarball download accounts the exact streamed byte count
    /// to `actions`, and the second download serves from the disk cache
    /// without touching GitHub again.
    #[tokio::test]
    async fn tarball_download_success_accounts_bytes_and_caches() {
        let body = b"fake-tarball-bytes-for-accounting".to_vec();
        let stub = TarballStub::serve(StatusCode::OK, vec![], body.clone()).await;
        let (shared, _temp) = tarball_shared(&stub.base).await;

        let (state, path, query) = download_call(&shared);
        let first = download_action_tarball(state, path, query).await;
        assert!(first.is_ok(), "download must succeed: {first:?}");
        drop(first);

        let usage = actions_usage(&shared);
        assert_eq!(usage.requests, 1);
        assert_eq!(
            usage.bytes,
            body.len() as u64,
            "accounting must see the exact streamed bytes"
        );
        assert_eq!(usage.rate_limited, 0);

        // Cache hit: served from disk, no second GitHub exchange.
        let (state, path, query) = download_call(&shared);
        let second = download_action_tarball(state, path, query).await;
        assert!(second.is_ok(), "cached download must succeed: {second:?}");
        drop(second);
        assert_eq!(stub.hits(), 1, "the cache hit must not touch GitHub");
        assert_eq!(actions_usage(&shared).requests, 1);
    }

    /// `normalize_action_owner_repo` maps `uses:` to the lowercased
    /// `owner/repo` download scope, and rejects everything that names no
    /// fixed remote repository.
    #[test]
    fn normalize_action_owner_repo_cases() {
        let some = |s: &str| Some(s.to_string());
        // Standard refs, case-insensitive, subpaths collapse to the repo.
        assert_eq!(
            normalize_action_owner_repo("actions/checkout@v4"),
            some("actions/checkout")
        );
        assert_eq!(
            normalize_action_owner_repo("Actions/Checkout@V4"),
            some("actions/checkout")
        );
        assert_eq!(
            normalize_action_owner_repo("owner/repo/sub/dir@main"),
            some("owner/repo")
        );
        assert_eq!(
            normalize_action_owner_repo("actions/setup-node.js@v4"),
            some("actions/setup-node.js")
        );
        // A dynamic ref is fine: enforcement is owner/repo-scoped.
        assert_eq!(
            normalize_action_owner_repo("actions/checkout@${{ matrix.ref }}"),
            some("actions/checkout")
        );
        // The `docker/` org is a real org; only the `docker://` scheme is local.
        assert_eq!(
            normalize_action_owner_repo("docker/build-push-action@v5"),
            some("docker/build-push-action")
        );
        // No fixed remote repository: local, docker scheme, self-repo,
        // expression-driven owners, malformed.
        assert_eq!(normalize_action_owner_repo("./.github/actions/local"), None);
        assert_eq!(normalize_action_owner_repo("../shared/action@v1"), None);
        assert_eq!(normalize_action_owner_repo("docker://alpine:3.20"), None);
        assert_eq!(normalize_action_owner_repo("$/actions/local@v1"), None);
        assert_eq!(
            normalize_action_owner_repo("${{ matrix.owner }}/repo@v1"),
            None
        );
        assert_eq!(normalize_action_owner_repo("just-a-name"), None);
        assert_eq!(normalize_action_owner_repo("owner/@v1"), None);
        assert_eq!(normalize_action_owner_repo("/repo@v1"), None);
        assert_eq!(normalize_action_owner_repo(""), None);
        assert_eq!(normalize_action_owner_repo("   "), None);
    }

    /// `direct_declared_actions` collects step `uses:`, job-level reusable
    /// calls, and the workflow's own repository (for `$/` self-references).
    #[test]
    fn direct_declared_actions_covers_steps_reusables_and_self_repo() {
        let yaml = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: Actions/Setup-Node@v4
      - uses: owner/repo/sub/dir@main
      - uses: ./local/action
      - uses: docker://alpine:3.20
      - run: echo hi
  call:
    uses: octo-org/octo-repo/.github/workflows/ci.yml@v1
"#;
        let declared = direct_declared_actions(yaml, &BTreeMap::new(), "MyOrg/MyRepo");
        assert!(declared.contains("actions/checkout"), "{declared:?}");
        assert!(declared.contains("actions/setup-node"), "{declared:?}");
        assert!(declared.contains("owner/repo"), "{declared:?}");
        assert!(declared.contains("octo-org/octo-repo"), "{declared:?}");
        // The workflow's own repo: `$/path` self-references resolve to it.
        assert!(declared.contains("myorg/myrepo"), "{declared:?}");
        assert_eq!(declared.len(), 5, "{declared:?}");
    }

    /// A corrupt stored workflow fails closed: nothing is declared, so no
    /// ticket is minted.
    #[test]
    fn direct_declared_actions_fails_closed_on_unparseable_yaml() {
        let declared = direct_declared_actions("not: [valid", &BTreeMap::new(), "o/r");
        assert!(declared.is_empty(), "{declared:?}");
    }

    /// Reusable callees contribute their own steps' actions, transitively.
    #[test]
    fn direct_declared_actions_follows_reusable_callees() {
        let callee = r#"
on: workflow_call
jobs:
  inner:
    runs-on: ubuntu-latest
    steps:
      - uses: docker/build-push-action@v5
"#;
        let root = r#"
on: push
jobs:
  call:
    uses: ./reusable/ci.yml
"#;
        let mut reusables = BTreeMap::new();
        reusables.insert("reusable/ci.yml".to_string(), callee.to_string());
        let declared = direct_declared_actions(root, &reusables, "o/r");
        assert!(
            declared.contains("docker/build-push-action"),
            "{declared:?}"
        );
        assert!(declared.contains("o/r"), "{declared:?}");
        assert_eq!(declared.len(), 2, "{declared:?}");
    }

    /// `manifest_nested_uses` extracts nested `uses:` only from composite
    /// action manifests; Node/Docker actions and junk yield nothing.
    #[test]
    fn manifest_nested_uses_only_for_composite() {
        fn contents_body(yaml: &str) -> serde_json::Value {
            let encoded = base64::engine::general_purpose::STANDARD.encode(yaml);
            serde_json::json!({ "content": encoded, "encoding": "base64" })
        }
        let composite = contents_body(
            "name: composite\nruns:\n  using: composite\n  steps:\n    - uses: actions/checkout@v4\n    - run: echo hi\n",
        );
        let nested = manifest_nested_uses(&composite);
        assert_eq!(nested, BTreeSet::from(["actions/checkout".to_string()]));

        let node = contents_body("name: node\nruns:\n  using: node20\n  main: dist/index.js\n");
        assert!(manifest_nested_uses(&node).is_empty());

        let docker = contents_body("name: docker\nruns:\n  using: docker\n  image: Dockerfile\n");
        assert!(manifest_nested_uses(&docker).is_empty());

        assert!(manifest_nested_uses(&serde_json::json!({})).is_empty());
        assert!(
            manifest_nested_uses(&serde_json::json!({"content": "!!!not-base64!!!"})).is_empty()
        );
    }

    /// Ticket TTL is ~20 minutes, not 6 hours: a leaked bearerless ticket URL
    /// is a briefly-open window.
    #[tokio::test]
    async fn action_ticket_ttl_is_twenty_minutes_not_six_hours() {
        let state = test_state().await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let (_, ticket) =
            action_download_ticket(&state, "actions/checkout@v4", None).expect("ticket must mint");
        let url = ticket.get("url").and_then(|u| u.as_str()).unwrap();
        let exp: u64 = url
            .split('?')
            .nth(1)
            .unwrap()
            .split('&')
            .find_map(|pair| pair.strip_prefix("exp=").and_then(|v| v.parse().ok()))
            .expect("ticket URL must carry exp");
        let ttl = exp.saturating_sub(now);
        assert_eq!(ACTION_TICKET_TTL_SECS, 20 * 60);
        assert!(
            (15 * 60..=30 * 60).contains(&ttl),
            "ticket TTL {ttl}s is outside the 15–30 minute window"
        );
    }
}
