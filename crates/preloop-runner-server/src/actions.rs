use super::*;
use futures::StreamExt;
use preloop_gha_protocol::azdo::{
    ActionDownloadInfo, ActionDownloadInfoCollection, ActionReferenceList,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::process::Stdio;
use tokio::io::AsyncReadExt;

fn require_action_plan(
    identity: &crate::auth::ResultsIdentity,
    plan_id: &str,
) -> Result<(), ApiError> {
    match identity {
        crate::auth::ResultsIdentity::System => Ok(()),
        crate::auth::ResultsIdentity::Job(job) if job.plan_id == plan_id => Ok(()),
        crate::auth::ResultsIdentity::Job(_) => {
            warn!(%plan_id, "action download refused: the job token is not bound to this plan");
            Err(ApiError::forbidden(
                "results-service token is not bound to that plan",
            ))
        }
    }
}

/// POST action download info — resolve action references to download URLs.
///
/// Tickets are minted only for actions declared in the run's workflow
/// (`uses:`, transitively through reusable callees and composite nests);
/// any other requested action fails the whole batch with 403 — a job
/// runtime token must not turn the engine into a fetch oracle for arbitrary
/// repositories on the operator's API quota. The caller is also bound to the
/// plan in the URL before resolution starts.
pub async fn action_download_info(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id)): Path<(String, String, String)>,
    axum::extract::Extension(identity): axum::extract::Extension<crate::auth::ResultsIdentity>,
    Json(request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    action_download_info_for_plan(&shared, &identity, &plan_id, &request).await
}

pub(crate) async fn action_download_info_for_plan(
    shared: &Arc<SharedState>,
    identity: &crate::auth::ResultsIdentity,
    plan_id: &str,
    request: &serde_json::Value,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_action_plan(identity, plan_id)?;
    crate::auth::require_live_results_job(&shared.state, identity).await?;
    let collection = collect_action_download_infos(shared, identity, plan_id, request).await?;
    Ok(Json(
        serde_json::to_value(collection).unwrap_or_else(|_| json!({ "actions": {} })),
    ))
}
pub async fn runnerresolve_actions(
    State(shared): State<Arc<SharedState>>,
    Path((orchestration_id, job_id)): Path<(String, String)>,
    axum::extract::Extension(identity): axum::extract::Extension<crate::auth::ResultsIdentity>,
    Json(request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if let Err(error) = crate::auth::require_results_job(&identity, &orchestration_id, &job_id) {
        warn!(
            %orchestration_id,
            %job_id,
            reason = error.message(),
            "runnerresolve/actions refused: the job token does not name this plan and job"
        );
        return Err(error);
    }
    if let Err(error) = crate::auth::require_live_results_job(&shared.state, &identity).await {
        warn!(
            %orchestration_id,
            %job_id,
            reason = error.message(),
            "runnerresolve/actions refused: the job is no longer live"
        );
        return Err(error);
    }
    let mut actions = serde_json::Map::new();
    collect_runnerresolve_refs(
        &shared,
        &identity,
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
    // The PAT budget and the anonymous budget are different rate-limit
    // identities: the breaker bucket must match the credential actually
    // attached, or a throttled PAT would park anonymous traffic and vice
    // versa.
    let bucket = if let Some(pat) = state.static_github_pat()
        && url_targets_configured_github(&url, &state.github_urls)
    {
        request = request.bearer_auth(pat);
        crate::github_breaker::GithubCredentialBucket::Pat
    } else {
        crate::github_breaker::GithubCredentialBucket::Anonymous
    };
    let response = match crate::github_breaker::send_observed_labeled(
        &state.github_breakers,
        bucket,
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
    let bucket = if let Some(pat) = shared.state.static_github_pat()
        && url_targets_configured_github(&github_url, &shared.state.github_urls)
    {
        request = request.bearer_auth(pat);
        crate::github_breaker::GithubCredentialBucket::Pat
    } else {
        crate::github_breaker::GithubCredentialBucket::Anonymous
    };
    let response = match crate::github_breaker::send_observed_labeled(
        &shared.state.github_breakers,
        bucket,
        &shared.state.github_consumption,
        crate::github_breaker::GithubSubsystem::Actions,
        request,
    )
    .await
    {
        Ok(response) => response,
        // A tripped breaker fails fast with 503 instead of queueing behind
        // the outage; runners retry the download on the next attempt.
        Err(error)
            if error
                .downcast_ref::<crate::github_breaker::BreakerOpen>()
                .is_some() =>
        {
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
    let mut stream = response.bytes_stream();
    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| {
            ApiError::internal(format!("failed to read chunk from GitHub response: {e}"))
        })?;
        // Account each chunk as it arrives, so a stream that dies mid-download
        // still charges the bytes GitHub already sent.
        shared.state.github_consumption.record_bytes(
            crate::github_breaker::GithubSubsystem::Actions,
            chunk.len() as u64,
        );
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

type ResolvedActionParts = (String, String, String, Option<String>, String);

pub async fn collect_runnerresolve_refs(
    shared: &SharedState,
    identity: &crate::auth::ResultsIdentity,
    orchestration_id: &str,
    job_id: &str,
    value: &serde_json::Value,
    actions: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<(), ApiError> {
    let state = &shared.state;
    let mut requests: Vec<(String, Option<String>)> = Vec::new();
    collect_runnerresolve_requests(value, &mut requests);
    let mut seen = std::collections::HashSet::new();
    requests.retain(|request| seen.insert(request.clone()));
    requests.truncate(MAX_ACTION_BATCH_SIZE);

    let (run_id, mut declared) =
        declared_actions_for_runnerresolve(shared, orchestration_id, job_id).await?;
    extend_with_local_composites(shared, run_id, &mut declared, &requests).await?;
    enforce_declared_actions(state, run_id, &declared, &requests).await?;

    let stream = futures::stream::iter(requests.into_iter().map(|(action, version)| async move {
        resolve_action_download_parts(state, &action, version.as_deref()).await
    }))
    .buffer_unordered(MAX_ACTION_CONCURRENCY);

    let resolved: Vec<Option<ResolvedActionParts>> = stream.collect().await;
    if let Err(error) = crate::auth::require_live_results_job(state, identity).await {
        warn!(
            run_id = %run_id.0,
            %orchestration_id,
            %job_id,
            reason = error.message(),
            "runnerresolve/actions refused after resolution: the job settled"
        );
        return Err(error);
    }
    for parts in resolved.into_iter().flatten() {
        if let Some((key, value)) = runnerresolve_action_from_parts(state, parts) {
            actions.entry(key).or_insert(value);
        }
    }
    Ok(())
}

/// Batch collector for the official JobServer `ActionDownloadInfo` endpoint.
///
/// Every requested action must be declared in the run's workflow (see
/// [`enforce_declared_actions`]); an undeclared action fails the whole
/// batch with 403 instead of minting a ticket.
pub async fn collect_action_download_infos(
    shared: &SharedState,
    identity: &crate::auth::ResultsIdentity,
    plan_id: &str,
    value: &serde_json::Value,
) -> Result<ActionDownloadInfoCollection, ApiError> {
    let state = &shared.state;
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

    let (run_id, mut declared) = declared_actions_for_plan(shared, plan_id).await?;
    extend_with_local_composites(shared, run_id, &mut declared, &requests).await?;
    enforce_declared_actions(state, run_id, &declared, &requests).await?;

    let stream = futures::stream::iter(requests.into_iter().map(|(action, version)| async move {
        resolve_action_download_parts(state, &action, version.as_deref()).await
    }))
    .buffer_unordered(MAX_ACTION_CONCURRENCY);

    let resolved: Vec<Option<ResolvedActionParts>> = stream.collect().await;
    if let Err(error) = crate::auth::require_live_results_job(state, identity).await {
        warn!(
            run_id = %run_id.0,
            %plan_id,
            reason = error.message(),
            "action download refused after resolution: the job settled"
        );
        return Err(error);
    }
    let mut actions = BTreeMap::new();
    for parts in resolved.into_iter().flatten() {
        if let Some((key, value)) = action_download_info_from_parts(state, parts) {
            actions.insert(key, value);
        }
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
/// collecting a run's declared actions and local composite steps: the parser's
/// own limit, so the allowlist sees the same tree the expander did.
const DECLARED_ACTION_REUSABLE_DEPTH: usize = preloop_gha_parser::MAX_REUSABLE_WORKFLOW_DEPTH;

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
    let mut visited = BTreeMap::new();
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
    visited: &mut BTreeMap<String, usize>,
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
            if walk_callee_at(visited, uses, depth)
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
    shared: &SharedState,
    plan_id: &str,
) -> Result<(RunId, BTreeSet<String>), ApiError> {
    let state = &shared.state;
    use crate::control::backend::RequestKey;
    let run_id = match state
        .backend
        .request(RequestKey::PlanId(plan_id.to_owned()))
        .await
    {
        Ok(request) => request.run_id,
        Err(crate::control::ControlError::NotFound(_)) => {
            warn!(%plan_id, "action download refused: no run has this plan");
            return Err(ApiError::forbidden(format!(
                "action downloads are not permitted for unknown plan `{plan_id}`"
            )));
        }
        Err(error) => return Err(ApiError::from(error)),
    };
    let declared = declared_actions_for_run_id(shared, run_id).await?;
    Ok((run_id, declared))
}

/// Resolve the run behind a `runnerresolve` (`orchestration_id`, `job_id`)
/// path pair and return its declared action allowlist. Prefers the job id
/// (exact), falling back to the orchestration plan id.
async fn declared_actions_for_runnerresolve(
    shared: &SharedState,
    orchestration_id: &str,
    job_id: &str,
) -> Result<(RunId, BTreeSet<String>), ApiError> {
    let state = &shared.state;
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
                warn!(
                    %orchestration_id,
                    %job_id,
                    "runnerresolve/actions refused: no request or plan matches this job"
                );
                return Err(ApiError::forbidden(format!(
                    "action downloads are not permitted for unknown job `{job_id}`"
                )));
            }
            Err(error) => return Err(ApiError::from(error)),
        },
    };
    let declared = declared_actions_for_run_id(shared, run_id).await?;
    Ok((run_id, declared))
}

async fn declared_actions_for_run_id(
    shared: &SharedState,
    run_id: RunId,
) -> Result<BTreeSet<String>, ApiError> {
    let state = &shared.state;
    let run = state
        .backend
        .run_record(run_id)
        .await
        .map_err(ApiError::from)?;
    // Direct references only. Remote actions reached through local composites
    // are added on demand by `extend_with_local_composites`.
    Ok(direct_declared_actions(
        &run.submission.workflow_yaml,
        &run.submission.reusable_workflows,
        &run.submission.repository,
    ))
}

/// Add the remote actions a run's local composites reach to `declared`, but only
/// when a request names a remote action the workflow does not declare directly.
/// Ordinary jobs never pay for the manifest traversal.
async fn extend_with_local_composites(
    shared: &SharedState,
    run_id: RunId,
    declared: &mut BTreeSet<String>,
    requests: &[(String, Option<String>)],
) -> Result<(), ApiError> {
    let undeclared = requests.iter().any(|(action, _)| {
        normalize_action_owner_repo(action)
            .is_some_and(|owner_repo| !declared.contains(&owner_repo))
    });
    if undeclared {
        let local = local_composite_declared_for_run(shared, run_id).await?;
        declared.extend(local.iter().cloned());
    }
    Ok(())
}

/// The remote actions a run's local composites reach, computed once per run.
/// A traversal that reached a definitive answer is remembered; one that hit a
/// transient GitHub failure is used for this request only.
async fn local_composite_declared_for_run(
    shared: &SharedState,
    run_id: RunId,
) -> Result<Arc<BTreeSet<String>>, ApiError> {
    let state = &shared.state;
    if let Ok(memo) = state.local_composite_declared.lock()
        && let Some(cached) = memo.get(&run_id)
    {
        return Ok(Arc::clone(cached));
    }
    let run = state
        .backend
        .run_record(run_id)
        .await
        .map_err(ApiError::from)?;
    // A GitHub-sourced run reads its manifests from its own repository at its
    // head commit. A local submission without a snapshot has no such commit, so
    // nothing can be read: that answer is incomplete and is never remembered.
    let source = match run.workspace_snapshot.as_ref() {
        Some(snapshot) => Some(LocalManifests::Snapshot(snapshot)),
        None if run.submission.local_workspace.is_none() => Some(LocalManifests::Forge {
            repository: &run.submission.repository,
            commit: &run.head_sha,
        }),
        None => None,
    };
    let found = match source {
        Some(source) => {
            local_composite_declared_actions(
                shared,
                source,
                &run.submission.workflow_yaml,
                &run.submission.reusable_workflows,
            )
            .await
        }
        None => {
            warn!(
                %run_id,
                "run has a local workspace but no snapshot on its record; its local composite manifests cannot be read, so their nested actions stay denied"
            );
            LocalDeclared {
                actions: BTreeSet::new(),
                complete: false,
            }
        }
    };
    let actions = Arc::new(found.actions);
    if found.complete
        && let Ok(mut memo) = state.local_composite_declared.lock()
    {
        if memo.len() >= crate::state::MAX_LOCAL_COMPOSITE_DECLARED_RUNS {
            memo.clear();
        }
        memo.insert(run_id, Arc::clone(&actions));
    }
    Ok(actions)
}

/// Cap on a fetched action manifest's decoded bytes. Manifests are small
/// YAML files; anything larger is not a manifest we should parse.
const ACTION_MANIFEST_MAX_BYTES: usize = 256 * 1024;

/// Most composite manifests one traversal reads. Real composites nest a few
/// levels; the cap bounds the reads a hostile tree can force.
const MAX_LOCAL_ACTION_MANIFESTS: usize = 256;
/// Deepest composite nesting a traversal follows from a workflow's local `uses:`.
const MAX_LOCAL_ACTION_DEPTH: usize = 10;
/// Most `uses:` references one traversal reads from composite manifests. A
/// manifest that would exceed what is left is not read, and the traversal
/// stops, so whatever it has not reached stays denied.
const MAX_LOCAL_TRAVERSAL_REFERENCES: usize = 4096;
/// Most tree lookups (git reads or contents-API requests) one traversal makes.
/// Every path component and every symlink hop costs one. When the budget runs
/// out the traversal stops, so whatever it has not reached stays denied.
const MAX_LOCAL_TRAVERSAL_LOOKUPS: usize = 512;
/// Most components in a local action path. Real action paths are a few
/// components deep, and each component costs a lookup.
const MAX_LOCAL_ACTION_PATH_COMPONENTS: usize = 32;
/// Symlinks followed while resolving one path. Past it the path is missing, as
/// a link loop is on disk.
const MAX_LOCAL_SYMLINK_HOPS: usize = 40;

/// Where a run's local composite manifests are read from.
#[derive(Clone, Copy)]
enum LocalManifests<'a> {
    /// The run's immutable workspace snapshot, a git object store on disk.
    Snapshot(&'a crate::snapshots::WorkspaceSnapshot),
    /// A run without a workspace snapshot (webhook and other GitHub-sourced
    /// runs): the run's own repository at its head commit, read through the
    /// contents API.
    Forge {
        repository: &'a str,
        commit: &'a str,
    },
}

/// The nested `uses:` of one composite manifest: remote `owner/repo` scopes,
/// then the local `./`, `../`, and `$/` references to resolve in the tree.
type NestedUses = (BTreeSet<String>, Vec<String>);

/// The remote actions a run's local composites reach. `complete` is false when
/// GitHub could not answer for some path: that answer serves this request only
/// and is not remembered for the run.
#[derive(Default)]
struct LocalDeclared {
    actions: BTreeSet<String>,
    complete: bool,
}

/// What a repository-relative path names in the run's commit.
#[derive(Clone)]
enum TreeNode {
    Directory,
    /// A regular file. `None` when its content is over
    /// [`ACTION_MANIFEST_MAX_BYTES`] or was not available.
    File(Option<Vec<u8>>),
    /// A symbolic link and its target text.
    Symlink(Vec<u8>),
    /// A submodule or another entry the runner does not load as a manifest.
    Other,
}

/// One answer from the tree about a single path.
#[derive(Clone)]
enum Probe {
    Node(TreeNode),
    Missing,
    /// GitHub could not answer. The answer is not remembered.
    Transient,
    /// The traversal's lookup budget is spent.
    Exhausted,
}

/// How a repository-relative path resolves once symlinks are followed the way
/// a checkout follows them.
enum Resolution {
    /// The path with every symlink replaced by its target, and what it names.
    Resolved(Vec<String>, TreeNode),
    /// Nothing there, or a link that dangles or loops.
    Missing,
    /// The path leaves the tree, descends below a file, or is too deep.
    Refused,
    Exhausted,
    Transient,
}

/// The outcome of reading one composite's manifest.
enum ManifestRead {
    Found(NestedUses),
    /// No manifest the runner would load, so nothing nested is admitted.
    Absent,
    Exhausted,
    Transient,
}

/// Where one traversal reads tree entries from: the run's snapshot, or its
/// repository at its head commit through the contents API.
enum TreeSource<'a> {
    Snapshot {
        repository: std::path::PathBuf,
        commit: String,
    },
    Forge(ForgeManifests<'a>),
}

/// Reads composite manifests for one traversal under that traversal's budgets.
/// Lookups are remembered for the traversal, so a directory that many
/// composites share is read once.
struct LocalTree<'a> {
    shared: &'a SharedState,
    source: TreeSource<'a>,
    /// Non-file answers by path; `None` is a path with nothing at it. Files are
    /// not remembered, since their contents can be large.
    probes: BTreeMap<String, Option<TreeNode>>,
    lookups_left: usize,
    /// Cleared once GitHub could not answer for some path.
    complete: bool,
}

impl<'a> LocalTree<'a> {
    /// The reader for `source`, or `None` when the source names nothing that
    /// can be read: an invalid commit or repository.
    fn open(shared: &'a SharedState, source: LocalManifests<'_>) -> Option<Self> {
        let source = match source {
            LocalManifests::Snapshot(snapshot) => {
                let repository = snapshot_repository_path(&shared.state, snapshot)?;
                if !valid_snapshot_commit(&snapshot.commit_sha) {
                    return None;
                }
                TreeSource::Snapshot {
                    repository,
                    commit: snapshot.commit_sha.clone(),
                }
            }
            LocalManifests::Forge { repository, commit } => {
                let Some((owner, repo)) = forge_repository_slug(repository) else {
                    warn!(
                        repository,
                        "run repository is not a plain owner/repo slug; its local composite manifests cannot be read"
                    );
                    return None;
                };
                if !preloop_gha_protocol::git_ref::is_commit_sha_not_zero(commit) {
                    return None;
                }
                TreeSource::Forge(ForgeManifests {
                    shared,
                    owner: owner.to_owned(),
                    repo: repo.to_owned(),
                    commit: commit.to_owned(),
                    credential: None,
                })
            }
        };
        Some(Self {
            shared,
            source,
            probes: BTreeMap::new(),
            lookups_left: MAX_LOCAL_TRAVERSAL_LOOKUPS,
            complete: true,
        })
    }

    /// Resolve `relative`, a repository path (`""` is the root), the way a
    /// checkout resolves it: each component is looked up, a symlink is replaced
    /// by its target relative to the directory that holds the link, and `..`
    /// steps up a resolved directory.
    async fn resolve(&mut self, relative: &str) -> Resolution {
        let mut pending: VecDeque<String> = relative
            .split('/')
            .filter(|part| !part.is_empty() && *part != ".")
            .map(str::to_owned)
            .collect();
        if pending.len() > MAX_LOCAL_ACTION_PATH_COMPONENTS {
            return Resolution::Refused;
        }
        let mut resolved: Vec<String> = Vec::new();
        let mut current = TreeNode::Directory;
        let mut hops = 0;
        while let Some(part) = pending.pop_front() {
            if !matches!(current, TreeNode::Directory) {
                return Resolution::Refused;
            }
            if part == ".." {
                if resolved.pop().is_none() {
                    return Resolution::Refused;
                }
                continue;
            }
            resolved.push(part);
            match self.probe(&resolved.join("/")).await {
                Probe::Node(TreeNode::Symlink(target)) => {
                    hops += 1;
                    if hops > MAX_LOCAL_SYMLINK_HOPS {
                        return Resolution::Missing;
                    }
                    let Ok(target) = String::from_utf8(target) else {
                        return Resolution::Refused;
                    };
                    if target.is_empty() {
                        return Resolution::Missing;
                    }
                    if target.starts_with('/') {
                        return Resolution::Refused;
                    }
                    // The link is replaced by its target, which is relative to
                    // the directory that holds the link.
                    resolved.pop();
                    for part in target
                        .split('/')
                        .rev()
                        .filter(|part| !part.is_empty() && *part != ".")
                    {
                        pending.push_front(part.to_owned());
                    }
                    current = TreeNode::Directory;
                }
                Probe::Node(node) => current = node,
                Probe::Missing => return Resolution::Missing,
                Probe::Transient => return Resolution::Transient,
                Probe::Exhausted => return Resolution::Exhausted,
            }
        }
        Resolution::Resolved(resolved, current)
    }

    /// What `path`, a symlink-free repository path, names. Answers are kept for
    /// the traversal; each fresh lookup costs one unit of the lookup budget.
    async fn probe(&mut self, path: &str) -> Probe {
        if let Some(known) = self.probes.get(path) {
            return match known {
                Some(node) => Probe::Node(node.clone()),
                None => Probe::Missing,
            };
        }
        if self.lookups_left == 0 {
            return Probe::Exhausted;
        }
        self.lookups_left -= 1;
        let answer = match &mut self.source {
            TreeSource::Snapshot { repository, commit } => {
                snapshot_probe(repository.as_path(), commit.as_str(), path).await
            }
            TreeSource::Forge(forge) => forge.probe(path).await,
        };
        match &answer {
            Probe::Node(TreeNode::File(_)) | Probe::Exhausted => {}
            Probe::Node(node) => {
                self.probes.insert(path.to_owned(), Some(node.clone()));
            }
            Probe::Missing => {
                self.probes.insert(path.to_owned(), None);
            }
            Probe::Transient => self.complete = false,
        }
        answer
    }

    /// The nested `uses:` of the composite at `action_path`, read the way the
    /// runner loads it. A composite read from GitHub is memoised per commit:
    /// definitive answers only.
    async fn manifest(&mut self, action_path: &std::path::Path) -> ManifestRead {
        let relative = action_path.to_string_lossy().into_owned();
        let memo_key = match &self.source {
            TreeSource::Forge(forge) => Some((
                format!("{}/{}", forge.owner, forge.repo).to_lowercase(),
                forge.commit.clone(),
                relative.clone(),
            )),
            TreeSource::Snapshot { .. } => None,
        };
        if let Some(key) = &memo_key
            && let Ok(memo) = self.shared.state.local_composite_nested_uses.lock()
            && let Some(cached) = memo.get(key)
        {
            return match cached {
                Some(nested) => ManifestRead::Found(nested.clone()),
                None => ManifestRead::Absent,
            };
        }
        let read = self.read_manifest(&relative).await;
        let remembered = match &read {
            ManifestRead::Found(nested) => Some(Some(nested.clone())),
            ManifestRead::Absent => Some(None),
            ManifestRead::Exhausted | ManifestRead::Transient => None,
        };
        if let (Some(key), Some(value)) = (memo_key, remembered)
            && let Ok(mut memo) = self.shared.state.local_composite_nested_uses.lock()
        {
            if memo.len() >= crate::state::MAX_LOCAL_COMPOSITE_MEMO {
                memo.clear();
            }
            memo.insert(key, value);
        }
        read
    }

    /// Read the manifest of the action directory `relative`, trying
    /// `action.yml` and then `action.yaml` as the runner does. The directory and
    /// the manifest are resolved through symlinks, and a manifest that resolves
    /// outside its action directory is refused.
    async fn read_manifest(&mut self, relative: &str) -> ManifestRead {
        let directory = match self.resolve(relative).await {
            Resolution::Resolved(path, TreeNode::Directory) => path,
            Resolution::Exhausted => return ManifestRead::Exhausted,
            Resolution::Transient => return ManifestRead::Transient,
            Resolution::Resolved(..) | Resolution::Missing | Resolution::Refused => {
                return ManifestRead::Absent;
            }
        };
        for name in ["action.yml", "action.yaml"] {
            let path = if directory.is_empty() {
                name.to_owned()
            } else {
                format!("{}/{name}", directory.join("/"))
            };
            match self.resolve(&path).await {
                Resolution::Resolved(resolved, TreeNode::File(Some(bytes))) => {
                    if !resolved.starts_with(&directory) {
                        return ManifestRead::Absent;
                    }
                    return ManifestRead::Found(manifest_nested_uses_from_yaml(&bytes));
                }
                // A missing name, or a link that dangles or loops, falls through
                // to the next name, as the runner's existence check does.
                Resolution::Missing => continue,
                Resolution::Exhausted => return ManifestRead::Exhausted,
                Resolution::Transient => return ManifestRead::Transient,
                // Over the size cap, a directory named like a manifest, or refused:
                // the runner would not load it, so nothing nested is admitted.
                Resolution::Resolved(..) | Resolution::Refused => return ManifestRead::Absent,
            }
        }
        ManifestRead::Absent
    }
}

/// What `path` names in a snapshot's commit, read with git. A missing entry is
/// absent. A git failure is not: the entry may well exist, so the traversal is
/// left incomplete (and is not remembered) rather than reading "nothing".
async fn snapshot_probe(repository: &std::path::Path, commit: &str, path: &str) -> Probe {
    let kind = match snapshot_entry_kind(repository, commit, path).await {
        Ok(Some(kind)) => kind,
        Ok(None) => return Probe::Missing,
        Err(error) => {
            warn!(path, %error, "could not read the run's snapshot tree; its local composite manifests stay denied for this request");
            return Probe::Transient;
        }
    };
    let file = std::path::Path::new(path);
    let node = match kind {
        SnapshotKind::Directory => TreeNode::Directory,
        SnapshotKind::Symlink => match read_snapshot_file(repository, commit, file).await {
            Ok(Some(target)) => TreeNode::Symlink(target),
            Ok(None) => return Probe::Missing,
            Err(error) => {
                warn!(path, %error, "could not read a symlink in the run's snapshot; its local composite manifests stay denied for this request");
                return Probe::Transient;
            }
        },
        SnapshotKind::File => match read_snapshot_file(repository, commit, file).await {
            Ok(bytes) => TreeNode::File(bytes),
            Err(error) => {
                warn!(path, %error, "could not read a file in the run's snapshot; its local composite manifests stay denied for this request");
                return Probe::Transient;
            }
        },
        SnapshotKind::Other => TreeNode::Other,
    };
    Probe::Node(node)
}

/// The kind of tree entry a snapshot path names.
enum SnapshotKind {
    Directory,
    Symlink,
    File,
    Other,
}

/// The kind of the entry `path` names in `commit`'s tree, read without following
/// links. `Ok(None)` when there is no such entry; `Err` when git could not answer.
async fn snapshot_entry_kind(
    repository: &std::path::Path,
    commit: &str,
    path: &str,
) -> anyhow::Result<Option<SnapshotKind>> {
    // Literal pathspecs: the path names one entry, so a glob or magic prefix in
    // it must not match other entries.
    let output = tokio::process::Command::new("git")
        .arg("--literal-pathspecs")
        .arg("--git-dir")
        .arg(repository)
        .args(["ls-tree", "-z", "--full-tree", commit, "--", path])
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|error| {
            anyhow::anyhow!("starting git ls-tree in {}: {error}", repository.display())
        })?;
    if !output.status.success() {
        anyhow::bail!(
            "git ls-tree {path} at {commit} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    // Records are `<mode> SP <type> SP <object> TAB <path> NUL`.
    Ok(output.stdout.split(|byte| *byte == 0).find_map(|record| {
        let tab = record.iter().position(|byte| *byte == b'\t')?;
        if &record[tab + 1..] != path.as_bytes() {
            return None;
        }
        let mode = std::str::from_utf8(&record[..tab])
            .ok()?
            .split(' ')
            .next()?;
        Some(match mode {
            "040000" => SnapshotKind::Directory,
            "120000" => SnapshotKind::Symlink,
            mode if mode.starts_with("100") => SnapshotKind::File,
            _ => SnapshotKind::Other,
        })
    }))
}

/// Add remote actions nested in the local composite manifests that a run's
/// workflow reaches, read from the run's own commit: its snapshot when it has
/// one, otherwise its GitHub repository at its head commit. Nothing a runner
/// sends selects the repository or ref, and a manifest that cannot be read adds
/// nothing, so its nested actions stay denied.
async fn local_composite_declared_actions(
    shared: &SharedState,
    source: LocalManifests<'_>,
    workflow_yaml: &str,
    reusable_workflows: &BTreeMap<String, String>,
) -> LocalDeclared {
    let Some(mut tree) = LocalTree::open(shared, source) else {
        warn!(
            "the run's source is not a readable snapshot or repository; its local composite manifests cannot be read, so their nested actions stay denied"
        );
        return LocalDeclared {
            actions: BTreeSet::new(),
            complete: false,
        };
    };
    let Ok(workflow) = preloop_gha_parser::parse_workflow(workflow_yaml) else {
        warn!(
            "the run's stored workflow does not parse; its local composite manifests cannot be followed, so their nested actions stay denied"
        );
        return LocalDeclared {
            actions: BTreeSet::new(),
            complete: false,
        };
    };

    let mut pending = VecDeque::new();
    let mut visited_workflows = BTreeMap::new();
    collect_workflow_local_action_paths(
        &workflow,
        reusable_workflows,
        &mut pending,
        &mut visited_workflows,
        0,
    );

    let mut declared = BTreeSet::new();
    let mut visited_actions = BTreeSet::new();
    let mut references_left = MAX_LOCAL_TRAVERSAL_REFERENCES;
    // A traversal that stopped on a limit has not read every manifest the
    // workflow could reach, so what it found is not the run's whole answer.
    let mut truncated = false;
    while let Some((action_path, depth)) = pending.pop_front() {
        if depth > MAX_LOCAL_ACTION_DEPTH || visited_actions.len() >= MAX_LOCAL_ACTION_MANIFESTS {
            truncated = true;
            continue;
        }
        if !visited_actions.insert(action_path.clone()) {
            continue;
        }
        let (remote, nested_local) = match tree.manifest(&action_path).await {
            ManifestRead::Found(nested) => nested,
            ManifestRead::Absent | ManifestRead::Transient => continue,
            // Out of lookups: nothing further is read, so the rest stays denied.
            ManifestRead::Exhausted => {
                truncated = true;
                break;
            }
        };
        let cost = remote.len() + nested_local.len();
        if cost > references_left {
            warn!(
                run_references = cost,
                "local composite traversal reached its reference budget; the rest of the tree stays denied"
            );
            truncated = true;
            break;
        }
        references_left -= cost;
        declared.extend(remote);
        for uses in nested_local {
            // The runner runs a nested `./` beside its composite when that
            // directory is itself a composite, and from the workspace root
            // otherwise (GitHub resolves it from the root). Admit both, so the
            // allowlist covers whichever the run uses.
            for base in [action_path.as_path(), std::path::Path::new("")] {
                if let Some(nested_path) = resolve_local_action_path(base, &uses) {
                    pending.push_back((nested_path, depth + 1));
                }
            }
        }
    }
    LocalDeclared {
        actions: declared,
        complete: tree.complete && !truncated,
    }
}

/// A run's own GitHub repository at its head commit. Tree entries are read
/// through the contents API with the repository's installation token.
struct ForgeManifests<'a> {
    shared: &'a SharedState,
    owner: String,
    repo: String,
    commit: String,
    /// The `contents: read` credential, looked up on first use and kept for the
    /// rest of the traversal. `None` records a failed lookup, so it is not retried
    /// within the traversal.
    credential: Option<Option<(String, crate::github_breaker::GithubCredentialBucket)>>,
}

impl ForgeManifests<'_> {
    /// What `path` names at this commit. The API serves a link to a file as
    /// the file it names, and reports a link to a directory as a `symlink`
    /// object carrying its target.
    async fn probe(&mut self, path: &str) -> Probe {
        let Some((token, bucket)) = self.credential().await else {
            // No App covers the repository, or the lookup failed: the path cannot
            // be read. That is not a definitive absence, so the traversal stays
            // incomplete and is not remembered.
            return Probe::Transient;
        };
        let api_base = self.shared.state.github_urls.api_url.trim_end_matches('/');
        let segments: Vec<String> = path.split('/').map(percent_encode_path_segment).collect();
        let url = format!(
            "{api_base}/repos/{}/{}/contents/{}?ref={}",
            percent_encode_path_segment(&self.owner),
            percent_encode_path_segment(&self.repo),
            segments.join("/"),
            percent_encode_path_segment(&self.commit),
        );
        let request = crate::shared_http::CLIENT
            .get(&url)
            .bearer_auth(&token)
            .header(reqwest::header::ACCEPT, "application/vnd.github+json");
        let response = match crate::github_breaker::send_observed_labeled(
            &self.shared.state.github_breakers,
            bucket,
            &self.shared.state.github_consumption,
            crate::github_breaker::GithubSubsystem::Actions,
            request,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                tracing::debug!(%error, path, "local composite path lookup failed; its nested uses stay denied");
                return Probe::Transient;
            }
        };
        self.shared
            .state
            .github_consumption
            .record_advertised_bytes(crate::github_breaker::GithubSubsystem::Actions, &response);
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Probe::Missing;
        }
        if !status.is_success() {
            warn!(path, %status, "local composite path lookup was refused; its nested uses stay denied");
            return Probe::Transient;
        }
        match response.json::<serde_json::Value>().await {
            Ok(body) => Probe::Node(forge_node(&body)),
            Err(_) => Probe::Transient,
        }
    }

    /// The run repository's contents-read credential, looked up on first use and
    /// kept for the rest of the traversal.
    async fn credential(
        &mut self,
    ) -> Option<(String, crate::github_breaker::GithubCredentialBucket)> {
        if self.credential.is_none() {
            let repository = format!("{}/{}", self.owner, self.repo);
            self.credential = Some(contents_read_credential(self.shared, &repository).await);
        }
        self.credential.clone().flatten()
    }
}

/// What a contents-API body describes. A listing (an array) is a directory;
/// an object's `type` names the rest. A file the API withholds (over 1 MB)
/// carries no content.
fn forge_node(body: &serde_json::Value) -> TreeNode {
    if body.is_array() {
        return TreeNode::Directory;
    }
    match body.get("type").and_then(serde_json::Value::as_str) {
        Some("file") => TreeNode::File(contents_file_bytes(body)),
        Some("dir") => TreeNode::Directory,
        Some("symlink") => TreeNode::Symlink(
            body.get("target")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .as_bytes()
                .to_vec(),
        ),
        _ => TreeNode::Other,
    }
}

/// The credential that reads `repository`'s contents: a `contents: read`
/// installation token scoped to that repository, minted from the GitHub App
/// installed on its owner, with the installation as its rate-limit bucket.
/// `None` when no App covers the repository or the lookup or mint fails; the
/// caller then reads nothing and the traversal stays incomplete. Unlike action
/// downloads there is no static-PAT fallback.
async fn contents_read_credential(
    shared: &SharedState,
    repository: &str,
) -> Option<(String, crate::github_breaker::GithubCredentialBucket)> {
    let app = match crate::github_app::candidate_apps_for_repo_inner(shared, repository).await {
        Ok(mut candidates) if !candidates.is_empty() => candidates.swap_remove(0),
        Ok(_) | Err(crate::github_app::AppLookupError::NotInstalled) => {
            warn!(
                repository,
                "no GitHub App installation covers this repository; its local composite manifests cannot be read, so their nested actions stay denied"
            );
            return None;
        }
        Err(crate::github_app::AppLookupError::Transient(error)) => {
            warn!(
                repository,
                %error,
                "could not tell whether a GitHub App covers this repository; its local composite manifests cannot be read, so their nested actions stay denied"
            );
            return None;
        }
    };
    let api_base = shared.state.github_urls.api_url.trim_end_matches('/');
    let permissions = BTreeMap::from([("contents".to_owned(), "read".to_owned())]);
    let token = match crate::github_app::get_or_mint_token_at(
        api_base,
        &app,
        repository,
        &permissions,
    )
    .await
    {
        Ok(token) => token,
        Err(error) => {
            warn!(
                repository,
                %error,
                "could not mint a contents-read installation token; its local composite manifests cannot be read, so their nested actions stay denied"
            );
            return None;
        }
    };
    // The installation is the rate-limit identity, so never invent one.
    match crate::github_app::installation_for_repo(&app, repository).await {
        Ok(installation) if installation != 0 => Some((
            token,
            crate::github_breaker::GithubCredentialBucket::Installation(installation),
        )),
        _ => {
            warn!(
                repository,
                "could not resolve the installation for this repository; its local composite manifests cannot be read, so their nested actions stay denied"
            );
            None
        }
    }
}

/// The owner and repository of a plain `owner/repo` slug. Anything the
/// contents-API path could carry unsafely (empty, `.`/`..`, separators,
/// whitespace) is rejected.
fn forge_repository_slug(repository: &str) -> Option<(&str, &str)> {
    let (owner, repo) = repository.split_once('/')?;
    let plain = |name: &str| {
        !name.is_empty()
            && name != "."
            && name != ".."
            && !name.contains(['/', '\\', '\0'])
            && !name.chars().any(char::is_whitespace)
    };
    (plain(owner) && plain(repo)).then_some((owner, repo))
}

/// Whether reusable workflow `uses` must be walked at `depth`: it has not been
/// walked there or at a shallower depth. A shallower walk reaches callees that
/// a deeper walk had to leave out.
fn walk_callee_at(visited: &mut BTreeMap<String, usize>, uses: &str, depth: usize) -> bool {
    match visited.get(uses) {
        Some(&seen) if seen <= depth => false,
        _ => {
            visited.insert(uses.to_owned(), depth);
            true
        }
    }
}

/// Local action paths named by the steps of `workflow` and of the reusable
/// workflows it calls, to the parser's nesting limit. Each starts at depth 0.
fn collect_workflow_local_action_paths(
    workflow: &preloop_gha_parser::Workflow,
    reusable_workflows: &BTreeMap<String, String>,
    pending: &mut VecDeque<(std::path::PathBuf, usize)>,
    visited: &mut BTreeMap<String, usize>,
    depth: usize,
) {
    if depth > DECLARED_ACTION_REUSABLE_DEPTH {
        return;
    }
    for job in workflow.jobs.values() {
        if let Some(uses) = job.uses.as_deref()
            && walk_callee_at(visited, uses, depth)
            && let Some(yaml) = reusable_callee_yaml(uses, reusable_workflows)
            && let Ok(callee) = preloop_gha_parser::parse_workflow(yaml)
        {
            collect_workflow_local_action_paths(
                &callee,
                reusable_workflows,
                pending,
                visited,
                depth + 1,
            );
        }
        for step in &job.steps {
            if let Some(uses) = step.uses.as_deref()
                && let Some(path) = resolve_local_action_path(std::path::Path::new(""), uses)
            {
                pending.push_back((path, 0));
            }
        }
    }
}

/// Normalize a local `uses:` (`./`, `../`, or the self-repository `$/`) to a
/// repository-relative action directory; the empty path is the repository
/// root. `None` for remote references and for paths that escape the root.
fn resolve_local_action_path(base: &std::path::Path, uses: &str) -> Option<std::path::PathBuf> {
    let path = if let Some(relative) = uses.strip_prefix("$/") {
        std::path::PathBuf::from(relative)
    } else if uses.starts_with("./") || uses.starts_with("../") {
        base.join(uses)
    } else {
        return None;
    };
    let mut normalized = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(value) => normalized.push(value),
            std::path::Component::CurDir => {}
            // A nested local action may walk to another path in the same
            // snapshot, but it may never escape the repository root.
            std::path::Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
            std::path::Component::RootDir | std::path::Component::Prefix(_) => return None,
        }
    }
    Some(normalized)
}

fn snapshot_repository_path(
    state: &AppState,
    snapshot: &crate::snapshots::WorkspaceSnapshot,
) -> Option<std::path::PathBuf> {
    let relative = snapshot
        .storage_repository
        .as_deref()
        .unwrap_or(&snapshot.repository);
    let path = std::path::Path::new(relative);
    if path.is_absolute() {
        return None;
    }
    let mut normalized = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(value) => normalized.push(value),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir
            | std::path::Component::RootDir
            | std::path::Component::Prefix(_) => return None,
        }
    }
    Some(state.state_dir.join(normalized))
}

fn valid_snapshot_commit(commit: &str) -> bool {
    (40..=64).contains(&commit.len()) && commit.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// The bytes of `path` in `commit`, read with git. `Ok(None)` when the blob is
/// over the manifest size cap, which the traversal does not load. `Err` when git
/// could not produce it.
async fn read_snapshot_file(
    repository: &std::path::Path,
    commit: &str,
    path: &std::path::Path,
) -> anyhow::Result<Option<Vec<u8>>> {
    let object = format!("{}:{}", commit, path.to_string_lossy());
    let mut child = tokio::process::Command::new("git")
        .arg("--git-dir")
        .arg(repository)
        .arg("show")
        .arg("--format=")
        .arg("--no-ext-diff")
        .arg(&object)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| anyhow::anyhow!("starting git show {object}: {error}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("git show {object}: no stdout pipe"))?;
    let mut bytes = Vec::with_capacity(ACTION_MANIFEST_MAX_BYTES + 1);
    let read_result = stdout
        .take((ACTION_MANIFEST_MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await;
    if let Err(error) = read_result {
        let _ = child.kill().await;
        let _ = child.wait().await;
        anyhow::bail!("reading git show {object}: {error}");
    }
    if bytes.len() > ACTION_MANIFEST_MAX_BYTES {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return Ok(None);
    }
    let status = child
        .wait()
        .await
        .map_err(|error| anyhow::anyhow!("waiting for git show {object}: {error}"))?;
    if !status.success() {
        anyhow::bail!("git show {object} exited with {status}");
    }
    Ok(Some(bytes))
}

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
        let mut bucket = crate::github_breaker::GithubCredentialBucket::Anonymous;
        if let Some(pat) = state.static_github_pat()
            && url_targets_configured_github(&url, &state.github_urls)
        {
            request = request.bearer_auth(pat);
            bucket = crate::github_breaker::GithubCredentialBucket::Pat;
        }
        let response = match crate::github_breaker::send_observed_labeled(
            &state.github_breakers,
            bucket,
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
    contents_manifest_bytes(body).map_or_else(BTreeSet::new, |decoded| {
        manifest_nested_uses_from_yaml(&decoded).0
    })
}

/// Decoded bytes of a contents-API body's `content`, capped at
/// [`ACTION_MANIFEST_MAX_BYTES`]; `None` when absent, undecodable, or oversized.
fn contents_manifest_bytes(body: &serde_json::Value) -> Option<Vec<u8>> {
    let encoded = body.get("content").and_then(|value| value.as_str())?;
    // The contents API wraps base64 at 60 columns; strip whitespace before
    // decoding and cap the decoded size — this is a small YAML file.
    let cleaned: String = encoded.chars().filter(|ch| !ch.is_whitespace()).collect();
    match base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        cleaned.as_bytes(),
    ) {
        Ok(decoded) if decoded.len() <= ACTION_MANIFEST_MAX_BYTES => Some(decoded),
        _ => None,
    }
}

/// Decoded bytes of a contents-API body for a file the forge serves inline.
/// Directory listings are arrays, and the API withholds content for files over
/// 1 MB (`encoding: "none"`); neither is a manifest we can read.
fn contents_file_bytes(body: &serde_json::Value) -> Option<Vec<u8>> {
    let inline_file = body.get("type").and_then(serde_json::Value::as_str) == Some("file")
        && body.get("encoding").and_then(serde_json::Value::as_str) == Some("base64");
    if !inline_file {
        return None;
    }
    contents_manifest_bytes(body)
}

/// Extract remote and local nested `uses:` references from a composite action
/// manifest. Local references are returned separately so callers can resolve
/// them against a checked-out tree rather than treating them as repositories.
fn manifest_nested_uses_from_yaml(decoded: &[u8]) -> (BTreeSet<String>, Vec<String>) {
    let mut nested = BTreeSet::new();
    let mut local = Vec::new();
    let manifest: serde_yaml::Value = match serde_yaml::from_slice(decoded) {
        Ok(manifest) => manifest,
        Err(_) => return (nested, local),
    };
    let runs = yaml_get(&manifest, "runs");
    let using_is_composite = runs
        .and_then(|runs| yaml_get(runs, "using"))
        .and_then(|using| using.as_str())
        .is_some_and(|using| using == "composite");
    if !using_is_composite {
        return (nested, local);
    }
    let steps = runs.and_then(|runs| yaml_get(runs, "steps"));
    let Some(steps) = steps.and_then(|steps| steps.as_sequence()) else {
        return (nested, local);
    };
    for step in steps {
        let Some(uses) = yaml_get(step, "uses").and_then(|uses| uses.as_str()) else {
            continue;
        };
        if let Some(owner_repo) = normalize_action_owner_repo(uses) {
            nested.insert(owner_repo);
        } else if uses.starts_with("./") || uses.starts_with("../") || uses.starts_with("$/") {
            local.push(uses.to_owned());
        }
    }
    (nested, local)
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
async fn resolve_action_download_parts(
    state: &AppState,
    action: &str,
    version_override: Option<&str>,
) -> Option<ResolvedActionParts> {
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
    Some((key, name, git_ref, pinned, effective_ref))
}

fn runnerresolve_action_from_parts(
    state: &AppState,
    (key, name, git_ref, resolved_sha_opt, effective_ref): ResolvedActionParts,
) -> Option<(String, serde_json::Value)> {
    let (_, ticket) = action_download_ticket(state, &name, Some(&effective_ref))?;
    let tar_url = ticket.get("url")?.as_str()?.to_string();
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

fn action_download_info_from_parts(
    state: &AppState,
    (key, name, git_ref, resolved_sha, effective_ref): ResolvedActionParts,
) -> Option<(String, ActionDownloadInfo)> {
    let (_, ticket) = action_download_ticket(state, &name, Some(&effective_ref))?;
    let tar_url = ticket.get("url")?.as_str()?.to_string();
    Some((
        key,
        ActionDownloadInfo {
            name_with_owner: Some(name.clone()),
            resolved_name_with_owner: Some(name),
            resolved_sha,
            r#ref: Some(git_ref),
            tarball_url: Some(tar_url),
            zipball_url: None,
            authentication: None,
            package_details: None,
        },
    ))
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
    let parts = resolve_action_download_parts(state, action, version_override).await?;
    runnerresolve_action_from_parts(state, parts)
}

/// One `ActionDownloadInfo` entry in the official `ActionDownloadInfoCollection` wire shape.
pub async fn action_download_info_entry(
    state: &AppState,
    action: &str,
    version_override: Option<&str>,
) -> Option<(String, ActionDownloadInfo)> {
    let parts = resolve_action_download_parts(state, action, version_override).await?;
    action_download_info_from_parts(state, parts)
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
        // Held for the whole test, with the PAT cleared before the state is
        // built: `AppState::new` reads `PRELOOP_GITHUB_TOKEN`, and a concurrent
        // test that sets it would move this download into the Pat bucket.
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _pat = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
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
            shared
                .state
                .github_breakers
                .breaker_for(crate::github_breaker::GithubCredentialBucket::Anonymous)
                .is_open(),
            "the 429 must trip the breaker at once, not after a threshold"
        );
        assert!(
            shared
                .state
                .github_breakers
                .breaker_for(crate::github_breaker::GithubCredentialBucket::Anonymous)
                .snapshot()
                .rate_limited
        );

        // The breaker is open now: the next download fails fast with 503
        // instead of spending another request on the guaranteed 429.
        let (state, path, query) = download_call(&shared);
        let err = download_action_tarball(state, path, query)
            .await
            .expect_err("breaker-open must fail fast");
        assert_eq!(err.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            err.message()
                .starts_with("GitHub is temporarily unavailable"),
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

    /// Only a breaker refusal is a 503. A transport failure that never reached
    /// GitHub stays an internal error, even though it also counts against the
    /// breaker.
    #[tokio::test]
    async fn tarball_transport_failure_is_internal_not_unavailable() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _pat = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
        // Port 1 on loopback is closed: connection refused, no HTTP exchange.
        let (shared, _temp) = tarball_shared("http://127.0.0.1:1").await;
        let (state, path, query) = download_call(&shared);
        let err = download_action_tarball(state, path, query)
            .await
            .expect_err("a refused connection must fail the download");
        assert_eq!(err.status(), StatusCode::INTERNAL_SERVER_ERROR);
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

    /// A caller `top` and five reusable workflows `l1` to `l5` below it, each
    /// calling the next. `l5`'s job runs `leaf_steps`. Its callee sits at depth
    /// five: past the old limit of four, within the parser's nine.
    fn reusable_chain(leaf_steps: &str) -> (String, BTreeMap<String, String>) {
        let caller = |uses: &str| format!("on: push\njobs:\n  call:\n    uses: {uses}\n");
        let mut reusable = BTreeMap::new();
        for level in 1..=5 {
            let body = if level == 5 {
                format!(
                    "on: workflow_call\njobs:\n  work:\n    runs-on: ubuntu-latest\n    steps:\n{leaf_steps}"
                )
            } else {
                caller(&format!("./.github/workflows/l{}.yml", level + 1))
            };
            reusable.insert(format!("./.github/workflows/l{level}.yml"), body);
        }
        (caller("./.github/workflows/l1.yml"), reusable)
    }

    /// A remote action named at the parser's full reusable depth is declared.
    #[test]
    fn direct_declared_actions_follow_the_parser_depth() {
        let (top, reusable) = reusable_chain("      - uses: deep/action@v1\n");
        let declared = direct_declared_actions(&top, &reusable, "o/r");
        assert!(declared.contains("deep/action"), "{declared:?}");
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

    /// Commit `files` as one snapshot and mirror it into a fresh engine state,
    /// the way a run's workspace snapshot is stored. The returned temp dir
    /// keeps that state directory alive for the test.
    async fn local_snapshot_fixture(
        files: &[(&str, &str)],
    ) -> (
        tempfile::TempDir,
        AppState,
        crate::snapshots::WorkspaceSnapshot,
    ) {
        local_snapshot_fixture_with_links(files, &[]).await
    }

    /// [`local_snapshot_fixture`] plus symbolic links, each `(path, target)`.
    async fn local_snapshot_fixture_with_links(
        files: &[(&str, &str)],
        links: &[(&str, &str)],
    ) -> (
        tempfile::TempDir,
        AppState,
        crate::snapshots::WorkspaceSnapshot,
    ) {
        fn git(dir: &std::path::Path, args: &[&str]) -> String {
            let output = std::process::Command::new("git")
                .arg("-C")
                .arg(dir)
                .args(args)
                .output()
                .unwrap_or_else(|error| panic!("git {args:?}: {error}"));
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout).unwrap().trim().to_owned()
        }

        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let source = temp.path().join("source");
        for (path, body) in files {
            let file = source.join(path);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, body).unwrap();
        }
        for (path, target) in links {
            let link = source.join(path);
            std::fs::create_dir_all(link.parent().unwrap()).unwrap();
            std::os::unix::fs::symlink(target, link).unwrap();
        }
        git(&source, &["init", "-q"]);
        git(&source, &["config", "user.name", "test"]);
        git(&source, &["config", "user.email", "test@example.invalid"]);
        git(&source, &["add", "."]);
        git(&source, &["commit", "-qm", "snapshot"]);
        let commit = git(&source, &["rev-parse", "HEAD"]);

        let repository = state.state_dir.join("snapshots/local-action-test");
        std::fs::create_dir_all(repository.parent().unwrap()).unwrap();
        let clone = std::process::Command::new("git")
            .args(["clone", "--bare"])
            .arg(&source)
            .arg(&repository)
            .output()
            .unwrap();
        assert!(
            clone.status.success(),
            "git clone --bare: {}",
            String::from_utf8_lossy(&clone.stderr)
        );

        let snapshot = crate::snapshots::WorkspaceSnapshot {
            commit_sha: commit,
            repository: "snapshots/local-action-test".to_owned(),
            ..Default::default()
        };
        (temp, state, snapshot)
    }

    /// A composite action manifest whose steps are the given `uses:` values.
    fn composite_manifest(uses: &[&str]) -> String {
        let steps: String = uses
            .iter()
            .map(|step| format!("    - uses: {step}\n"))
            .collect();
        format!("runs:\n  using: composite\n  steps:\n{steps}")
    }

    /// A local composite's remote dependency is read from the immutable
    /// checked-out tree and admitted, while an unrelated action remains
    /// forbidden. This exercises the same allowlist used by runnerresolve.
    #[tokio::test]
    async fn local_composite_dependency_is_allowed_but_unlisted_action_denied() {
        let setup = composite_manifest(&["dtolnay/rust-toolchain@stable"]);
        let (_temp, state, snapshot) =
            local_snapshot_fixture(&[(".github/actions/setup/action.yml", setup.as_str())]).await;
        let workflow = r#"
name: ci
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: ./.github/actions/setup
"#;
        let declared = local_composite_declared_actions(
            &state.shared(),
            LocalManifests::Snapshot(&snapshot),
            workflow,
            &BTreeMap::new(),
        )
        .await
        .actions;
        assert!(
            declared.contains("dtolnay/rust-toolchain"),
            "local composite dependency must enter the declared set: {declared:?}"
        );

        let allowed = vec![(
            "dtolnay/rust-toolchain".to_owned(),
            Some("stable".to_owned()),
        )];
        enforce_declared_actions(&state, RunId(uuid::Uuid::new_v4()), &declared, &allowed)
            .await
            .expect("local composite dependency should be authorized");

        let denied = vec![("attacker/unlisted".to_owned(), Some("main".to_owned()))];
        let error =
            enforce_declared_actions(&state, RunId(uuid::Uuid::new_v4()), &declared, &denied)
                .await
                .expect_err("an action absent from workflow and manifests must be denied");
        assert_eq!(error.status(), StatusCode::FORBIDDEN);
    }

    /// A `./` step nested in a local composite resolves as the runner does: the
    /// composite beside the referencing one wins when it exists, otherwise the
    /// path is taken from the workspace root. A root-level `uses: ./` action is
    /// read too.
    #[tokio::test]
    async fn local_nested_composites_resolve_beside_then_from_the_root() {
        let outer = composite_manifest(&["./.github/actions/inner", "./beside"]);
        let inner = composite_manifest(&["root/inner@v1"]);
        let beside = composite_manifest(&["beside/dep@v1"]);
        let root = composite_manifest(&["root/action-dep@v1"]);
        let (_temp, state, snapshot) = local_snapshot_fixture(&[
            (".github/actions/outer/action.yml", outer.as_str()),
            (".github/actions/inner/action.yml", inner.as_str()),
            (".github/actions/outer/beside/action.yml", beside.as_str()),
            ("action.yml", root.as_str()),
        ])
        .await;
        let workflow = r#"
name: ci
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: ./.github/actions/outer
      - uses: ./
"#;
        let declared = local_composite_declared_actions(
            &state.shared(),
            LocalManifests::Snapshot(&snapshot),
            workflow,
            &BTreeMap::new(),
        )
        .await
        .actions;
        for expected in ["root/inner", "beside/dep", "root/action-dep"] {
            assert!(
                declared.contains(expected),
                "{expected} must enter the declared set: {declared:?}"
            );
        }
    }

    /// An action's depth is the shallowest path that reaches it. Here `shared`
    /// is a direct step (depth 0) and also the end of a nine-composite chain
    /// (depth 9); its dependency two levels below it must still be admitted.
    #[tokio::test]
    async fn local_composite_depth_is_taken_from_the_shallowest_path() {
        let mut manifests: Vec<(String, String)> = Vec::new();
        for index in 0..9 {
            let next = if index == 8 {
                "./.github/actions/shared".to_owned()
            } else {
                format!("./.github/actions/chain{}", index + 1)
            };
            manifests.push((
                format!(".github/actions/chain{index}/action.yml"),
                composite_manifest(&[next.as_str()]),
            ));
        }
        manifests.push((
            ".github/actions/shared/action.yml".to_owned(),
            composite_manifest(&["./.github/actions/s1"]),
        ));
        manifests.push((
            ".github/actions/s1/action.yml".to_owned(),
            composite_manifest(&["./.github/actions/s2"]),
        ));
        manifests.push((
            ".github/actions/s2/action.yml".to_owned(),
            composite_manifest(&["deep/dep@v1"]),
        ));
        let files: Vec<(&str, &str)> = manifests
            .iter()
            .map(|(path, body)| (path.as_str(), body.as_str()))
            .collect();
        let (_temp, state, snapshot) = local_snapshot_fixture(&files).await;
        let workflow = r#"
name: ci
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: ./.github/actions/shared
      - uses: ./.github/actions/chain0
"#;
        let declared = local_composite_declared_actions(
            &state.shared(),
            LocalManifests::Snapshot(&snapshot),
            workflow,
            &BTreeMap::new(),
        )
        .await
        .actions;
        assert!(
            declared.contains("deep/dep"),
            "deep/dep must enter the declared set: {declared:?}"
        );
    }

    /// The workflow whose only step is the local composite `./.github/actions/setup`.
    const SETUP_WORKFLOW: &str = r#"
name: ci
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: ./.github/actions/setup
"#;

    /// The declared set a snapshot run of `workflow` reaches.
    async fn snapshot_declared(
        state: &AppState,
        snapshot: &crate::snapshots::WorkspaceSnapshot,
        workflow: &str,
        reusable: &BTreeMap<String, String>,
    ) -> BTreeSet<String> {
        local_composite_declared_actions(
            &state.shared(),
            LocalManifests::Snapshot(snapshot),
            workflow,
            reusable,
        )
        .await
        .actions
    }

    /// A composite whose `action.yml` is a symlink to `action.yaml` is read
    /// through the link, as the runner reads it. The link's own text is not YAML.
    #[tokio::test]
    async fn local_composite_manifest_symlink_is_followed() {
        let setup = composite_manifest(&["dtolnay/rust-toolchain@stable"]);
        let (_temp, state, snapshot) = local_snapshot_fixture_with_links(
            &[(".github/actions/setup/action.yaml", setup.as_str())],
            &[(".github/actions/setup/action.yml", "action.yaml")],
        )
        .await;
        let declared = snapshot_declared(&state, &snapshot, SETUP_WORKFLOW, &BTreeMap::new()).await;
        assert!(declared.contains("dtolnay/rust-toolchain"), "{declared:?}");
    }

    /// An action directory that is a symlink to another directory is read where
    /// it points, as the runner loads it.
    #[tokio::test]
    async fn local_composite_action_directory_symlink_is_followed() {
        let setup = composite_manifest(&["dtolnay/rust-toolchain@stable"]);
        let (_temp, state, snapshot) = local_snapshot_fixture_with_links(
            &[(".github/shared/setup/action.yml", setup.as_str())],
            &[(".github/actions/setup", "../shared/setup")],
        )
        .await;
        let declared = snapshot_declared(&state, &snapshot, SETUP_WORKFLOW, &BTreeMap::new()).await;
        assert!(declared.contains("dtolnay/rust-toolchain"), "{declared:?}");
    }

    /// A manifest link that resolves outside its action directory is one the
    /// runner refuses to load, so nothing nested in it, or in `action.yaml`, is
    /// admitted.
    #[tokio::test]
    async fn local_composite_link_out_of_its_action_directory_is_refused() {
        let other = composite_manifest(&["evil/dep@v1"]);
        let setup = composite_manifest(&["dtolnay/rust-toolchain@stable"]);
        let (_temp, state, snapshot) = local_snapshot_fixture_with_links(
            &[
                (".github/actions/other/action.yml", other.as_str()),
                (".github/actions/setup/action.yaml", setup.as_str()),
            ],
            &[(".github/actions/setup/action.yml", "../other/action.yml")],
        )
        .await;
        let declared = snapshot_declared(&state, &snapshot, SETUP_WORKFLOW, &BTreeMap::new()).await;
        assert!(declared.is_empty(), "{declared:?}");
    }

    /// Links that point at each other never resolve: the path is missing, the
    /// traversal terminates, and nothing is admitted.
    #[tokio::test]
    async fn local_composite_link_loop_admits_nothing() {
        let (_temp, state, snapshot) = local_snapshot_fixture_with_links(
            &[(".github/actions/setup/README.md", "docs")],
            &[
                (".github/actions/setup/action.yml", "action.yaml"),
                (".github/actions/setup/action.yaml", "action.yml"),
            ],
        )
        .await;
        let declared = snapshot_declared(&state, &snapshot, SETUP_WORKFLOW, &BTreeMap::new()).await;
        assert!(declared.is_empty(), "{declared:?}");
    }

    /// A dangling `action.yml` link is skipped, as the runner's existence check
    /// skips it: `action.yaml` is the manifest that loads.
    #[tokio::test]
    async fn local_composite_dangling_manifest_link_falls_back_to_action_yaml() {
        let setup = composite_manifest(&["dtolnay/rust-toolchain@stable"]);
        let (_temp, state, snapshot) = local_snapshot_fixture_with_links(
            &[(".github/actions/setup/action.yaml", setup.as_str())],
            &[(".github/actions/setup/action.yml", "missing.yml")],
        )
        .await;
        let declared = snapshot_declared(&state, &snapshot, SETUP_WORKFLOW, &BTreeMap::new()).await;
        assert!(declared.contains("dtolnay/rust-toolchain"), "{declared:?}");
    }

    /// `uses:` paths are literal: a glob names no entry, so nothing is read.
    #[tokio::test]
    async fn local_composite_glob_in_a_uses_path_names_nothing() {
        let setup = composite_manifest(&["dtolnay/rust-toolchain@stable"]);
        let (_temp, state, snapshot) =
            local_snapshot_fixture(&[(".github/actions/setup/action.yml", setup.as_str())]).await;
        let workflow = r#"
name: ci
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: ./.github/actions/*
"#;
        let declared = snapshot_declared(&state, &snapshot, workflow, &BTreeMap::new()).await;
        assert!(declared.is_empty(), "{declared:?}");
    }

    /// A manifest with more references than the traversal budget is not read:
    /// nothing it names is admitted, and the traversal stops.
    #[tokio::test]
    async fn local_composite_reference_budget_stops_the_traversal() {
        let names: Vec<String> = (0..=MAX_LOCAL_TRAVERSAL_REFERENCES)
            .map(|index| format!("vendor/dep{index}@v1"))
            .collect();
        let uses: Vec<&str> = names.iter().map(String::as_str).collect();
        let big = composite_manifest(&uses);
        let (_temp, state, snapshot) =
            local_snapshot_fixture(&[(".github/actions/setup/action.yml", big.as_str())]).await;
        let declared = snapshot_declared(&state, &snapshot, SETUP_WORKFLOW, &BTreeMap::new()).await;
        assert!(declared.is_empty(), "{} admitted", declared.len());
    }

    /// A composite named by the leaf of a reusable chain at the parser's full
    /// depth is read.
    #[tokio::test]
    async fn local_composite_under_the_deepest_reusable_workflow_is_read() {
        let setup = composite_manifest(&["deep/action@v1"]);
        let (_temp, state, snapshot) =
            local_snapshot_fixture(&[(".github/actions/deep/action.yml", setup.as_str())]).await;
        let (top, reusable) = reusable_chain("      - uses: ./.github/actions/deep\n");
        let declared = snapshot_declared(&state, &snapshot, &top, &reusable).await;
        assert!(declared.contains("deep/action"), "{declared:?}");
    }

    /// A snapshot made by the production `create_workspace_snapshot` (the one a
    /// `preloop run` submission stores on its run record) must admit the remote
    /// actions its local composites call. The fixtures above use a plain bare
    /// clone; this runs the real layout (alternates, synthetic commit, staged
    /// then published) with the template's shape: two jobs that call one local
    /// composite naming four remote actions, one of them undeclared.
    #[tokio::test]
    async fn real_workspace_snapshot_admits_nested_remote_actions() {
        fn git(dir: &std::path::Path, args: &[&str]) {
            let output = std::process::Command::new("git")
                .arg("-C")
                .arg(dir)
                .args(args)
                .output()
                .unwrap_or_else(|error| panic!("git {args:?}: {error}"));
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let workflow = r#"
name: Build & Publish
on: push
jobs:
  app:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - uses: ./.github/actions/common-setup
  web:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - uses: ./.github/actions/common-setup
"#;
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let composite = workspace.join(".github/actions/common-setup/action.yml");
        std::fs::create_dir_all(composite.parent().unwrap()).unwrap();
        std::fs::write(
            &composite,
            composite_manifest(&[
                "jdx/mise-action@v4",
                "1password/load-secrets-action@v4",
                "iloveitaly/github-action-direnv-load-and-mask@master",
                "actions/cache@v6",
            ]),
        )
        .unwrap();
        let workflow_file = workspace.join(".github/workflows/build_and_publish.yml");
        std::fs::create_dir_all(workflow_file.parent().unwrap()).unwrap();
        std::fs::write(&workflow_file, workflow).unwrap();
        git(&workspace, &["init", "-q"]);
        git(&workspace, &["config", "user.name", "test"]);
        git(
            &workspace,
            &["config", "user.email", "test@example.invalid"],
        );
        git(&workspace, &["add", "."]);
        git(&workspace, &["commit", "-qm", "workspace"]);

        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let snapshot = crate::snapshots::create_workspace_snapshot(
            &state.state_dir,
            &workspace,
            RunId(uuid::Uuid::new_v4()),
            None,
            None,
            None,
        )
        .await
        .unwrap_or_else(|error| panic!("workspace snapshot: {}", error.message()));
        let declared = local_composite_declared_actions(
            &state.shared(),
            LocalManifests::Snapshot(&snapshot),
            workflow,
            &BTreeMap::new(),
        )
        .await;
        for expected in [
            "jdx/mise-action",
            "1password/load-secrets-action",
            "iloveitaly/github-action-direnv-load-and-mask",
            "actions/cache",
        ] {
            assert!(
                declared.actions.contains(expected),
                "real snapshot {} must admit {expected}: {:?} complete={}",
                snapshot.repository,
                declared.actions,
                declared.complete,
            );
        }
        assert!(
            declared.complete,
            "a readable snapshot is a definitive answer"
        );
    }

    /// A traversal that could not read its source is incomplete, so its empty
    /// answer is never remembered for the run: a snapshot whose commit is not a
    /// commit id, a snapshot repository that is not on disk, and a workflow that
    /// does not parse. Each used to come back as a definitive, memoised "nothing".
    #[tokio::test]
    async fn local_composite_missing_source_is_incomplete() {
        let setup = composite_manifest(&["dtolnay/rust-toolchain@stable"]);
        let (_temp, state, snapshot) =
            local_snapshot_fixture(&[(".github/actions/setup/action.yml", setup.as_str())]).await;
        let shared = state.shared();
        let invalid_commit = crate::snapshots::WorkspaceSnapshot {
            commit_sha: "not-a-commit".to_owned(),
            ..snapshot.clone()
        };
        let missing_repository = crate::snapshots::WorkspaceSnapshot {
            repository: "snapshots/no-such-run".to_owned(),
            ..snapshot.clone()
        };
        for (label, source, workflow) in [
            (
                "invalid commit",
                LocalManifests::Snapshot(&invalid_commit),
                SETUP_WORKFLOW,
            ),
            (
                "missing repository",
                LocalManifests::Snapshot(&missing_repository),
                SETUP_WORKFLOW,
            ),
            (
                "unparseable workflow",
                LocalManifests::Snapshot(&snapshot),
                "jobs: [}",
            ),
        ] {
            let declared =
                local_composite_declared_actions(&shared, source, workflow, &BTreeMap::new()).await;
            assert!(
                declared.actions.is_empty(),
                "{label}: {:?}",
                declared.actions
            );
            assert!(
                !declared.complete,
                "{label}: an unreadable source must not be remembered as a definitive answer"
            );
        }
    }

    /// A traversal that stops on one of its budgets has not read every
    /// manifest the workflow reaches, so what it found is a partial answer and
    /// is never remembered for the run.
    #[tokio::test]
    async fn local_composite_limit_hit_is_incomplete() {
        // One composite names more distinct remote actions than the whole
        // traversal may spend references on, so the traversal stops at it.
        let actions: Vec<String> = (0..=MAX_LOCAL_TRAVERSAL_REFERENCES)
            .map(|index| format!("owner{index}/repo@v1"))
            .collect();
        let uses: Vec<&str> = actions.iter().map(String::as_str).collect();
        let setup = composite_manifest(&uses);
        let (_temp, state, snapshot) =
            local_snapshot_fixture(&[(".github/actions/setup/action.yml", setup.as_str())]).await;
        let declared = local_composite_declared_actions(
            &state.shared(),
            LocalManifests::Snapshot(&snapshot),
            SETUP_WORKFLOW,
            &BTreeMap::new(),
        )
        .await;
        assert!(
            !declared.complete,
            "a traversal that stopped on its reference budget is not a definitive answer"
        );
        assert!(
            declared.actions.is_empty(),
            "the manifest past the budget is not read: {} actions",
            declared.actions.len()
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

    use axum::response::IntoResponse;

    /// One App key for every forge test: a fresh 2048-bit key per test is slow.
    static FORGE_TEST_KEY: std::sync::LazyLock<rsa::RsaPrivateKey> =
        std::sync::LazyLock::new(|| {
            rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).unwrap()
        });

    /// Installation id the forge stub reports for owner `o`.
    const STUB_INSTALLATION: u64 = 4242;

    /// The workflow the forge tests run: one local composite step.
    const FORGE_WORKFLOW: &str = r#"
name: ci
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: ./.github/actions/setup
"#;

    /// GitHub stand-in for a webhook run's repository `o/r`: App installation
    /// discovery, the installation-token mint, and the contents API serving
    /// `files` at any ref. Records every contents request and mint body.
    #[derive(Default)]
    struct ForgeStub {
        /// Whether the App is installed on `o`.
        installed: bool,
        /// Repository-relative path to file text.
        files: BTreeMap<String, String>,
        /// Answer every contents request with HTTP 500.
        fail_contents: bool,
        /// `(path, ref, authorization)` for each contents request.
        contents: parking_lot::Mutex<Vec<(String, String, String)>>,
        /// Symbolic links, path to target, as the API reports them.
        symlinks: BTreeMap<String, String>,
        /// Body of each installation-token mint.
        mints: parking_lot::Mutex<Vec<serde_json::Value>>,
    }

    async fn forge_installations(
        axum::extract::State(stub): axum::extract::State<Arc<ForgeStub>>,
    ) -> axum::Json<serde_json::Value> {
        if !stub.installed {
            return axum::Json(serde_json::json!([]));
        }
        axum::Json(serde_json::json!([
            { "id": STUB_INSTALLATION, "account": { "login": "o" } }
        ]))
    }

    async fn forge_mint(
        axum::extract::State(stub): axum::extract::State<Arc<ForgeStub>>,
        axum::extract::Path(installation): axum::extract::Path<u64>,
        axum::Json(body): axum::Json<serde_json::Value>,
    ) -> axum::response::Response {
        stub.mints.lock().push(body);
        if !stub.installed || installation != STUB_INSTALLATION {
            return StatusCode::NOT_FOUND.into_response();
        }
        axum::Json(serde_json::json!({
            "token": "ghs_forge_stub",
            "expires_at": "2999-01-01T00:00:00Z",
        }))
        .into_response()
    }

    async fn forge_contents(
        axum::extract::State(stub): axum::extract::State<Arc<ForgeStub>>,
        axum::extract::Path((_owner, _repo, path)): axum::extract::Path<(String, String, String)>,
        axum::extract::Query(query): axum::extract::Query<BTreeMap<String, String>>,
        headers: axum::http::HeaderMap,
    ) -> axum::response::Response {
        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let git_ref = query.get("ref").cloned().unwrap_or_default();
        stub.contents
            .lock()
            .push((path.clone(), git_ref, authorization));
        if stub.fail_contents {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        if let Some(target) = stub.symlinks.get(&path) {
            return axum::Json(serde_json::json!({
                "type": "symlink",
                "target": target,
                "path": path,
            }))
            .into_response();
        }
        if let Some(text) = stub.files.get(&path) {
            return axum::Json(serde_json::json!({
                "type": "file",
                "encoding": "base64",
                "size": text.len(),
                "content": base64::engine::general_purpose::STANDARD.encode(text),
            }))
            .into_response();
        }
        // Any proper prefix of a file or link is a directory, served as a listing.
        let is_directory = stub.files.keys().chain(stub.symlinks.keys()).any(|entry| {
            entry
                .strip_prefix(path.as_str())
                .is_some_and(|rest| rest.starts_with('/'))
        });
        if is_directory {
            return axum::Json(serde_json::json!([])).into_response();
        }
        StatusCode::NOT_FOUND.into_response()
    }

    /// Serve `stub` on a loopback port; returns the stub for assertions and
    /// the API base to point the engine at.
    async fn spawn_forge_stub(stub: ForgeStub) -> (Arc<ForgeStub>, String) {
        let stub = Arc::new(stub);
        let app = axum::Router::new()
            .route(
                "/app/installations",
                axum::routing::get(forge_installations),
            )
            .route(
                "/app/installations/{installation}/access_tokens",
                axum::routing::post(forge_mint),
            )
            .route(
                "/repos/{owner}/{repo}/contents/{*path}",
                axum::routing::get(forge_contents),
            )
            .with_state(Arc::clone(&stub));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (stub, base)
    }

    /// A webhook-style run's engine: no workspace snapshot, the stub App
    /// configured when `with_app`, and the GitHub API pointed at the stub. The
    /// guards hold the process-wide env lock and restore `PRELOOP_GITHUB_API_URL`
    /// when dropped, so one fixture at a time per test.
    struct ForgeFixture {
        shared: Arc<SharedState>,
        stub: Arc<ForgeStub>,
        _temp: tempfile::TempDir,
        _env: (
            crate::state::TestEnvVar,
            tokio::sync::MutexGuard<'static, ()>,
        ),
    }

    async fn forge_fixture(stub: ForgeStub, with_app: bool) -> ForgeFixture {
        let (stub, base) = spawn_forge_stub(stub).await;
        let lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let var = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &base);
        let temp = tempfile::tempdir().unwrap();
        let mut state = AppState::new(temp.path().join("state")).await.unwrap();
        state.github_urls = crate::state::GitHubUrls {
            server_url: base.clone(),
            api_url: base.clone(),
            graphql_url: base,
        };
        if with_app {
            state.github_app = Some(crate::github_app::GitHubAppCredentials::for_tests(
                "424",
                FORGE_TEST_KEY.clone(),
                crate::github_app::MintFailurePolicy::LocalJwt,
            ));
        }
        ForgeFixture {
            shared: state.shared(),
            stub,
            _temp: temp,
            _env: (var, lock),
        }
    }

    fn forge_files(files: &[(&str, &str)]) -> BTreeMap<String, String> {
        files
            .iter()
            .map(|(path, text)| ((*path).to_owned(), (*text).to_owned()))
            .collect()
    }

    /// The declared set a forge run of `FORGE_WORKFLOW` reaches at `SHA`.
    async fn forge_declared(shared: &SharedState) -> BTreeSet<String> {
        local_composite_declared_actions(
            shared,
            LocalManifests::Forge {
                repository: "o/r",
                commit: SHA,
            },
            FORGE_WORKFLOW,
            &BTreeMap::new(),
        )
        .await
        .actions
    }

    /// A webhook run has no snapshot, so its local composite manifests come
    /// from its own repository at its head commit, through the contents API.
    /// The remote action that composite calls is admitted, an unlisted action
    /// stays forbidden, and every read is pinned to that commit and sent with
    /// the run's installation token.
    #[tokio::test]
    async fn forge_local_composite_dependency_is_allowed_but_unlisted_action_denied() {
        let setup = composite_manifest(&["dtolnay/rust-toolchain@stable"]);
        let fixture = forge_fixture(
            ForgeStub {
                installed: true,
                files: forge_files(&[(".github/actions/setup/action.yml", setup.as_str())]),
                ..Default::default()
            },
            true,
        )
        .await;
        let shared = &fixture.shared;
        let declared = forge_declared(shared).await;
        assert!(
            declared.contains("dtolnay/rust-toolchain"),
            "local composite dependency must enter the declared set: {declared:?}"
        );

        let allowed = vec![(
            "dtolnay/rust-toolchain".to_owned(),
            Some("stable".to_owned()),
        )];
        enforce_declared_actions(
            &shared.state,
            RunId(uuid::Uuid::new_v4()),
            &declared,
            &allowed,
        )
        .await
        .expect("local composite dependency should be authorized");

        let denied = vec![("attacker/unlisted".to_owned(), Some("main".to_owned()))];
        let error = enforce_declared_actions(
            &shared.state,
            RunId(uuid::Uuid::new_v4()),
            &declared,
            &denied,
        )
        .await
        .expect_err("an action absent from workflow and manifests must be denied");
        assert_eq!(error.status(), StatusCode::FORBIDDEN);

        // The manifest's directory is resolved one component at a time, then
        // the manifest itself is read, all at the run's commit and with its token.
        let contents = fixture.stub.contents.lock().clone();
        assert_eq!(
            contents
                .iter()
                .map(|(path, _, _)| path.as_str())
                .collect::<Vec<_>>(),
            [
                ".github",
                ".github/actions",
                ".github/actions/setup",
                ".github/actions/setup/action.yml",
            ],
        );
        for (_, git_ref, authorization) in &contents {
            assert_eq!(git_ref, SHA);
            assert_eq!(authorization, "Bearer ghs_forge_stub");
        }
        let mints = fixture.stub.mints.lock();
        assert_eq!(mints.len(), 1);
        assert_eq!(mints[0]["repositories"], serde_json::json!(["r"]));
        assert_eq!(
            mints[0]["permissions"],
            serde_json::json!({ "contents": "read" })
        );
    }

    /// The forge reader resolves nested `./` references as the snapshot reader
    /// does: a webhook run and the same commit submitted with a snapshot get
    /// the same declared set, and every read stays inside the repository at
    /// that commit.
    #[tokio::test]
    async fn forge_nested_composites_resolve_like_the_snapshot() {
        let outer = composite_manifest(&["./.github/actions/inner", "./beside"]);
        let inner = composite_manifest(&["root/inner@v1"]);
        let beside = composite_manifest(&["beside/dep@v1"]);
        let root = composite_manifest(&["root/action-dep@v1"]);
        let files = [
            (".github/actions/outer/action.yml", outer.as_str()),
            (".github/actions/inner/action.yml", inner.as_str()),
            (".github/actions/outer/beside/action.yml", beside.as_str()),
            ("action.yml", root.as_str()),
        ];
        let workflow = r#"
name: ci
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: ./.github/actions/outer
      - uses: ./
"#;
        let (_temp, snapshot_state, snapshot) = local_snapshot_fixture(&files).await;
        let from_snapshot = local_composite_declared_actions(
            &snapshot_state.shared(),
            LocalManifests::Snapshot(&snapshot),
            workflow,
            &BTreeMap::new(),
        )
        .await
        .actions;

        let fixture = forge_fixture(
            ForgeStub {
                installed: true,
                files: forge_files(&files),
                ..Default::default()
            },
            true,
        )
        .await;
        let from_forge = local_composite_declared_actions(
            &fixture.shared,
            LocalManifests::Forge {
                repository: "o/r",
                commit: &snapshot.commit_sha,
            },
            workflow,
            &BTreeMap::new(),
        )
        .await
        .actions;

        assert_eq!(from_forge, from_snapshot);
        for expected in ["root/inner", "beside/dep", "root/action-dep"] {
            assert!(
                from_forge.contains(expected),
                "{expected} must enter the declared set: {from_forge:?}"
            );
        }
        let contents = fixture.stub.contents.lock();
        assert!(!contents.is_empty());
        for (path, git_ref, _) in contents.iter() {
            assert!(!path.contains(".."), "read escaped the tree: {path}");
            assert_eq!(git_ref, &snapshot.commit_sha);
        }
    }

    /// A failed contents read adds nothing, so the nested action stays denied.
    /// The failure is not memoised: the next adjudication reads GitHub again.
    #[tokio::test]
    async fn forge_contents_failure_leaves_nested_action_denied() {
        let setup = composite_manifest(&["dtolnay/rust-toolchain@stable"]);
        let fixture = forge_fixture(
            ForgeStub {
                installed: true,
                fail_contents: true,
                files: forge_files(&[(".github/actions/setup/action.yml", setup.as_str())]),
                ..Default::default()
            },
            true,
        )
        .await;
        let declared = forge_declared(&fixture.shared).await;
        assert!(!declared.contains("dtolnay/rust-toolchain"), "{declared:?}");

        let requested = vec![(
            "dtolnay/rust-toolchain".to_owned(),
            Some("stable".to_owned()),
        )];
        let error = enforce_declared_actions(
            &fixture.shared.state,
            RunId(uuid::Uuid::new_v4()),
            &declared,
            &requested,
        )
        .await
        .expect_err("a nested action whose manifest could not be read must stay denied");
        assert_eq!(error.status(), StatusCode::FORBIDDEN);

        let _ = forge_declared(&fixture.shared).await;
        assert_eq!(
            fixture.stub.contents.lock().len(),
            2,
            "a failed read must not be memoised"
        );
    }

    /// A repository no App installation covers gets no contents read at all,
    /// so the nested action stays denied, with or without an App configured.
    #[tokio::test]
    async fn forge_without_an_installation_token_reads_nothing() {
        let setup = composite_manifest(&["dtolnay/rust-toolchain@stable"]);
        let files = forge_files(&[(".github/actions/setup/action.yml", setup.as_str())]);
        {
            let uninstalled = forge_fixture(
                ForgeStub {
                    installed: false,
                    files: files.clone(),
                    ..Default::default()
                },
                true,
            )
            .await;
            assert!(forge_declared(&uninstalled.shared).await.is_empty());
            assert!(uninstalled.stub.contents.lock().is_empty());
        }
        let no_app = forge_fixture(
            ForgeStub {
                installed: true,
                files,
                ..Default::default()
            },
            false,
        )
        .await;
        assert!(forge_declared(&no_app.shared).await.is_empty());
        assert!(no_app.stub.contents.lock().is_empty());
    }

    /// A manifest is read once per commit: a second adjudication of the same
    /// run is served from the memo, with no GitHub read and no token mint.
    #[tokio::test]
    async fn forge_manifests_are_memoised_per_commit() {
        let setup = composite_manifest(&["dtolnay/rust-toolchain@stable"]);
        let fixture = forge_fixture(
            ForgeStub {
                installed: true,
                files: forge_files(&[(".github/actions/setup/action.yml", setup.as_str())]),
                ..Default::default()
            },
            true,
        )
        .await;
        assert!(
            forge_declared(&fixture.shared)
                .await
                .contains("dtolnay/rust-toolchain")
        );
        let first_read = fixture.stub.contents.lock().len();
        assert!(
            forge_declared(&fixture.shared)
                .await
                .contains("dtolnay/rust-toolchain")
        );
        assert_eq!(
            fixture.stub.contents.lock().len(),
            first_read,
            "a second adjudication is served from the memo"
        );
        assert_eq!(fixture.stub.mints.lock().len(), 1);
    }

    /// A webhook run reads a composite whose action directory is a symlink the
    /// way the runner does: the link is followed and the manifest it names is
    /// read. The contents API does not traverse links in a path, so the walk
    /// resolves them itself.
    #[tokio::test]
    async fn forge_directory_symlink_is_followed() {
        let setup = composite_manifest(&["dtolnay/rust-toolchain@stable"]);
        let fixture = forge_fixture(
            ForgeStub {
                installed: true,
                files: forge_files(&[(".github/shared/setup/action.yml", setup.as_str())]),
                symlinks: BTreeMap::from([(
                    ".github/actions/setup".to_owned(),
                    "../shared/setup".to_owned(),
                )]),
                ..Default::default()
            },
            true,
        )
        .await;
        let declared = forge_declared(&fixture.shared).await;
        assert!(declared.contains("dtolnay/rust-toolchain"), "{declared:?}");
    }

    /// A head commit that is not a real commit, or a repository that is not a
    /// plain `owner/repo`, reads nothing.
    #[tokio::test]
    async fn forge_requires_a_real_commit_and_a_plain_repository() {
        let setup = composite_manifest(&["dtolnay/rust-toolchain@stable"]);
        let fixture = forge_fixture(
            ForgeStub {
                installed: true,
                files: forge_files(&[(".github/actions/setup/action.yml", setup.as_str())]),
                ..Default::default()
            },
            true,
        )
        .await;
        let zeros = "0".repeat(40);
        for (repository, commit) in [
            ("o/r", zeros.as_str()),
            ("o/r", "not-a-commit"),
            ("solo", SHA),
            ("o/../r", SHA),
        ] {
            let declared = local_composite_declared_actions(
                &fixture.shared,
                LocalManifests::Forge { repository, commit },
                FORGE_WORKFLOW,
                &BTreeMap::new(),
            )
            .await
            .actions;
            assert!(
                declared.is_empty(),
                "{repository} at {commit}: {declared:?}"
            );
        }
        assert!(fixture.stub.contents.lock().is_empty());
    }

    /// Only plain `owner/repo` slugs reach the contents API path.
    #[test]
    fn forge_repository_slug_accepts_only_plain_owner_and_repo() {
        assert_eq!(
            forge_repository_slug("preloopdev/AgentENV-private"),
            Some(("preloopdev", "AgentENV-private"))
        );
        for bad in [
            "", "solo", "/repo", "owner/", "o/r/x", "o/..", "./r", "o/r s", "o\\x/r",
        ] {
            assert_eq!(
                forge_repository_slug(bad),
                None,
                "{bad:?} must not name a repository"
            );
        }
    }
}
