use super::*;

/// POST action download info — resolve action references to download URLs.
pub(crate) async fn action_download_info(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let mut tickets = serde_json::Map::new();
    collect_action_download_refs(&shared.state, &request, &mut tickets);

    Json(json!({
        "archiveDownloadTickets": tickets.clone(),
        // Some runner/protocol paths call the same payload an actionsDownloadInfo
        // map. Return both names so legacy and batch clients can consume the same
        // local fallback without a second resolution path.
        "actionsDownloadInfo": tickets,
    }))
}

pub(crate) async fn runnerresolve_actions(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let mut actions = serde_json::Map::new();
    collect_runnerresolve_refs(&shared.state, &request, &mut actions).await;

    Json(json!({ "actions": actions }))
}

/// How long a minted archive ticket stays valid. Actions are fetched during
/// job setup, so this only has to outlive a queue wait, not a whole run.
pub(crate) const ACTION_TICKET_TTL_SECS: u64 = 6 * 60 * 60;
/// How long a resolved action ref→SHA binding is trusted before the ref is
/// re-resolved. Matches the freshness GitHub gives a `@main`-style reference:
/// a new push to the ref is picked up after at most one TTL window.
pub(crate) const ACTION_SHA_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(300);
/// How long a *failed* ref resolution is remembered before it is retried.
///
/// Shorter than the success TTL so a transient outage heals quickly, but long
/// enough that an offline server does not pay the client's 10s connect timeout
/// once per `uses:` on every single job dispatch. Without this the lookup is
/// retried forever and lands directly on the cold-start path.
pub(crate) const ACTION_SHA_NEGATIVE_TTL: std::time::Duration = std::time::Duration::from_secs(60);

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
    // Already a full SHA — no lookup needed.
    if git_ref.len() == 40
        && git_ref
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
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

    let api_base = state.github_urls.api_url.trim_end_matches('/').to_owned();
    let url = format!("{api_base}/repos/{owner}/{repo}/commits/{git_ref}");
    let mut request = crate::shared_http::CLIENT.get(&url);
    if let Some(pat) = state.static_github_pat() {
        request = request.bearer_auth(pat);
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

#[derive(serde::Deserialize)]
pub(crate) struct ActionTicketQuery {
    #[serde(default)]
    exp: Option<u64>,
    #[serde(default)]
    sig: Option<String>,
}

pub(crate) async fn download_action_tarball(
    State(shared): State<Arc<SharedState>>,
    Path((owner, repo, git_ref)): Path<(String, String, String)>,
    Query(ticket): Query<ActionTicketQuery>,
) -> Result<Response, ApiError> {
    // 1. Sanitize parameters to avoid directory traversal
    if owner.contains('.')
        || owner.contains('/')
        || owner.contains('\\')
        || repo.contains('.')
        || repo.contains('/')
        || repo.contains('\\')
        || git_ref.contains("..")
        || git_ref.contains('\\')
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
        let file = tokio::fs::File::open(&cached_path)
            .await
            .map_err(|e| ApiError::internal(format!("failed to open cached action: {e}")))?;
        let stream = tokio_util::io::ReaderStream::new(file);
        let body = Body::from_stream(stream);

        let res = Response::builder()
            .header(header::CONTENT_TYPE, "application/gzip")
            .header(
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{repo}-{git_ref}.tar.gz\""),
            )
            .body(body)
            .map_err(|e| ApiError::internal(format!("failed to build response: {e}")))?;
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
    let github_url = format!("https://api.github.com/repos/{owner}/{repo}/tarball/{git_ref}");

    info!(
        owner,
        repo, git_ref, github_url, "Downloading action to server cache"
    );

    let client = reqwest::Client::builder()
        .user_agent("aksh-runner-server")
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
        request = request.bearer_auth(pat);
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

    let mut stream = response.bytes_stream();
    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| {
            ApiError::internal(format!("failed to read chunk from GitHub response: {e}"))
        })?;
        tokio::io::copy(&mut &chunk[..], &mut temp_file)
            .await
            .map_err(|e| {
                ApiError::internal(format!("failed to write chunk to temporary file: {e}"))
            })?;
    }

    // Atomically rename to final target path. A concurrent request may have
    // published the same action first — then the cached file is the winner's
    // (byte-identical) download and ours is discarded.
    if let Err(error) = tokio::fs::rename(&temp_path, &cached_path).await {
        let _ = tokio::fs::remove_file(&temp_path).await;
        if !cached_path.exists() {
            return Err(ApiError::internal(format!(
                "failed to rename cached action file: {error}"
            )));
        }
        info!(cached_path = ?cached_path, "Action cache published by a concurrent request");
    } else {
        info!(cached_path = ?cached_path, "Action cached successfully on server");
    }

    let file = tokio::fs::File::open(&cached_path)
        .await
        .map_err(|e| ApiError::internal(format!("failed to open newly cached action: {e}")))?;
    let stream = tokio_util::io::ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let res = Response::builder()
        .header(header::CONTENT_TYPE, "application/gzip")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{repo}-{git_ref}.tar.gz\""),
        )
        .body(body)
        .map_err(|e| ApiError::internal(format!("failed to build response: {e}")))?;
    Ok(res)
}

pub(crate) fn collect_action_download_refs(
    state: &AppState,
    value: &serde_json::Value,
    tickets: &mut serde_json::Map<String, serde_json::Value>,
) {
    match value {
        serde_json::Value::String(raw) => {
            if let Some((key, ticket)) = action_download_ticket(state, raw, None) {
                tickets.entry(key).or_insert(ticket);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_action_download_refs(state, item, tickets);
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
                if let Some((key, ticket)) = action_download_ticket(state, action, version) {
                    tickets.entry(key).or_insert(ticket);
                }
            }

            for nested in map.values() {
                collect_action_download_refs(state, nested, tickets);
            }
        }
        _ => {}
    }
}

pub(crate) fn action_download_ticket(
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

    let mut parts = repo_part.split('/');
    let owner = parts.next()?;
    let repo = parts.next()?;
    if owner.is_empty() || repo.is_empty() {
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
    let url = format!(
        "{runner_url}/api/v1/actions/download/{owner}/{repo}/{git_ref}\
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

pub(crate) async fn collect_runnerresolve_refs(
    state: &AppState,
    value: &serde_json::Value,
    actions: &mut serde_json::Map<String, serde_json::Value>,
) {
    // Collect the (action, version) pairs first (sync recursion), then
    // resolve them — async recursion would need boxing for no benefit.
    let mut requests: Vec<(String, Option<String>)> = Vec::new();
    collect_runnerresolve_requests(value, &mut requests);
    // A payload repeats the same `uses:` once per step and once per matrix
    // cell. Resolving a duplicate costs a full round trip before the cache is
    // populated, so collapse them first; the retained order is what decides
    // `or_insert` precedence below.
    let mut seen = std::collections::HashSet::new();
    requests.retain(|request| seen.insert(request.clone()));
    // Concurrently: each entry is an independent GitHub round trip, and the
    // whole set sits on the job-setup critical path. Sequential awaits made a
    // job with N actions pay N × the client timeout on a slow or unreachable
    // API.
    let resolved = futures::future::join_all(
        requests
            .iter()
            .map(|(action, version)| runnerresolve_action(state, action, version.as_deref())),
    )
    .await;
    for (key, value) in resolved.into_iter().flatten() {
        actions.entry(key).or_insert(value);
    }
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

pub(crate) async fn runnerresolve_action(
    state: &AppState,
    action: &str,
    version_override: Option<&str>,
) -> Option<(String, serde_json::Value)> {
    // First call is the validation gate as well as the key source: it rejects
    // `./`, `../` and `docker://` references, so those never reach the network
    // lookup below. The ticket it mints is discarded because the final URL has
    // to carry the resolved SHA, which is not known until after resolution.
    let (key, _) = action_download_ticket(state, action, version_override)?;
    let (name, version) = key.split_once('@')?;
    let name = name.to_string();
    let version = version.to_string();
    let (repo_part, git_ref) = if let Some(version) = version_override {
        (action.to_owned(), version.to_owned())
    } else {
        action
            .split_once('@')
            .map(|(n, v)| (n.to_owned(), v.to_owned()))?
    };
    let mut parts = repo_part.split('/');
    let owner = parts.next()?.to_owned();
    let repo = parts.next()?.to_owned();

    // Pin the ref to the SHA GitHub would resolve at job time. The ticket URL
    // then carries the SHA, so both the server-side tarball cache and the
    // runner's `_actions/{owner}/{repo}/{sha}` extraction dir are keyed by
    // content identity: when the ref moves, the next job gets a fresh SHA, a
    // fresh download, and the stale archive is never served again. When the
    // lookup is unavailable the ref itself is used, preserving the historical
    // ref-keyed behavior.
    let pinned = resolve_ref_to_sha(state, &owner, &repo, &git_ref).await;
    let effective_ref = pinned.as_deref().unwrap_or(&git_ref);
    let (_, ticket) = action_download_ticket(state, action, Some(effective_ref))?;
    let tar_url = ticket.get("url")?.as_str()?.to_string();
    Some((
        key,
        json!({
            "name": name,
            "version": version,
            "resolved_sha": effective_ref,
            "tar_url": tar_url,
            "authentication": null,
        }),
    ))
}
