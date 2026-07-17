use super::*;

/// POST action download info — resolve action references to download URLs.
pub(crate) async fn action_download_info(
    State(_shared): State<Arc<SharedState>>,
    Json(request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let mut tickets = serde_json::Map::new();
    collect_action_download_refs(&request, &mut tickets);

    Json(json!({
        "archiveDownloadTickets": tickets.clone(),
        // Some runner/protocol paths call the same payload an actionsDownloadInfo
        // map. Return both names so legacy and batch clients can consume the same
        // local fallback without a second resolution path.
        "actionsDownloadInfo": tickets,
    }))
}

pub(crate) async fn runnerresolve_actions(
    State(_shared): State<Arc<SharedState>>,
    Json(request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let mut actions = serde_json::Map::new();
    collect_runnerresolve_refs(&request, &mut actions);

    Json(json!({ "actions": actions }))
}

pub(crate) async fn download_action_tarball(
    State(shared): State<Arc<SharedState>>,
    Path((owner, repo, git_ref)): Path<(String, String, String)>,
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

    let temp_path = cache_dir.join("action.tar.gz.tmp");
    let github_url = format!("https://api.github.com/repos/{owner}/{repo}/tarball/{git_ref}");

    info!(
        owner,
        repo, git_ref, github_url, "Downloading action to server cache"
    );

    let client = reqwest::Client::builder()
        .user_agent("aksh-runner-server")
        .build()
        .map_err(|e| ApiError::internal(format!("failed to build reqwest client: {e}")))?;

    let response = client.get(&github_url).send().await.map_err(|e| {
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

    // Atomically rename to final target path
    tokio::fs::rename(&temp_path, &cached_path)
        .await
        .map_err(|e| ApiError::internal(format!("failed to rename cached action file: {e}")))?;

    info!(cached_path = ?cached_path, "Action cached successfully on server");

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
    value: &serde_json::Value,
    tickets: &mut serde_json::Map<String, serde_json::Value>,
) {
    match value {
        serde_json::Value::String(raw) => {
            if let Some((key, ticket)) = action_download_ticket(raw, None) {
                tickets.entry(key).or_insert(ticket);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_action_download_refs(item, tickets);
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
                if let Some((key, ticket)) = action_download_ticket(action, version) {
                    tickets.entry(key).or_insert(ticket);
                }
            }

            for nested in map.values() {
                collect_action_download_refs(nested, tickets);
            }
        }
        _ => {}
    }
}

pub(crate) fn action_download_ticket(
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
    let public_url = public_base_url();
    let url = format!("{public_url}/api/v1/actions/download/{owner}/{repo}/{git_ref}");
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

pub(crate) fn collect_runnerresolve_refs(
    value: &serde_json::Value,
    actions: &mut serde_json::Map<String, serde_json::Value>,
) {
    match value {
        serde_json::Value::String(raw) => {
            if let Some((key, action)) = runnerresolve_action(raw, None) {
                actions.entry(key).or_insert(action);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_runnerresolve_refs(item, actions);
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
                if let Some((key, value)) = runnerresolve_action(action, version) {
                    actions.entry(key).or_insert(value);
                }
            }

            for nested in map.values() {
                collect_runnerresolve_refs(nested, actions);
            }
        }
        _ => {}
    }
}

pub(crate) fn runnerresolve_action(
    action: &str,
    version_override: Option<&str>,
) -> Option<(String, serde_json::Value)> {
    let (key, ticket) = action_download_ticket(action, version_override)?;
    let (name, version) = key.split_once('@')?;
    let name = name.to_string();
    let version = version.to_string();
    let tar_url = ticket.get("url")?.as_str()?.to_string();
    Some((
        key,
        json!({
            "name": name,
            "version": version,
            // Local aksh does not pin refs yet; use the requested ref as the
            // extraction directory until a GitHub API lookup is added.
            "resolved_sha": version,
            "tar_url": tar_url,
            "authentication": null,
        }),
    ))
}
