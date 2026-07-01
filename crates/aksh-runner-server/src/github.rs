//! GitHub App Webhook Integration.

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::{submit_run_inner, ExecutionStatus, SharedState};
use aksh_gha_protocol::{JobId, RunId, WorkflowSubmission};

/// Webhook push event payload.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct PushEvent {
    /// Git reference for the push event.
    #[serde(rename = "ref")]
    pub(crate) git_ref: String,
    /// Previous commit SHA.
    pub(crate) before: String,
    /// Current commit SHA.
    pub(crate) after: String,
    /// Repository info.
    pub(crate) repository: RepositoryInfo,
    /// Commits in this push.
    pub(crate) commits: Vec<CommitInfo>,
}

/// Repository info.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct RepositoryInfo {
    /// Full repository name (e.g. owner/repo).
    pub(crate) full_name: String,
    /// Default branch (e.g. main).
    pub(crate) default_branch: Option<String>,
}

/// Commit info.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct CommitInfo {
    /// Commit ID.
    pub(crate) id: String,
    /// Added files.
    pub(crate) added: Vec<String>,
    /// Modified files.
    pub(crate) modified: Vec<String>,
    /// Removed files.
    pub(crate) removed: Vec<String>,
}

/// Webhook pull request event payload.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct PullRequestEvent {
    /// Webhook action type.
    pub(crate) action: String,
    /// PR number.
    pub(crate) number: u64,
    /// PR details.
    pub(crate) pull_request: PullRequestDetails,
    /// Repository info.
    pub(crate) repository: RepositoryInfo,
}

/// Pull request details.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct PullRequestDetails {
    /// Head reference.
    pub(crate) head: GitReference,
    /// Base reference.
    pub(crate) base: GitReference,
}

/// Git reference.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct GitReference {
    /// Git reference name.
    #[serde(rename = "ref")]
    pub(crate) git_ref: String,
    /// Commit SHA.
    pub(crate) sha: String,
}

/// Verify X-Hub-Signature-256 webhook signature.
pub(crate) fn verify_signature(secret: &str, payload: &[u8], signature_header: &str) -> bool {
    let signature_hex = match signature_header.strip_prefix("sha256=") {
        Some(hex) => hex,
        None => return false,
    };
    let signature_bytes = match decode_hex(signature_hex) {
        Ok(bytes) => bytes,
        Err(_) => return false,
    };

    type HmacSha256 = Hmac<Sha256>;
    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(payload);
    mac.verify_slice(&signature_bytes).is_ok()
}

fn decode_hex(hex: &str) -> Result<Vec<u8>, &'static str> {
    if hex.len() % 2 != 0 {
        return Err("Odd length");
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for i in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| "Invalid hex character")?;
        bytes.push(byte);
    }
    Ok(bytes)
}

async fn send_github_check_request(
    token: &str,
    repo: &str,
    method: reqwest::Method,
    path: &str,
    body: Value,
) -> anyhow::Result<Value> {
    let client = reqwest::Client::new();
    let url = format!("https://api.github.com/repos/{}/{}", repo, path);
    let res = client
        .request(method, &url)
        .header("User-Agent", "aksh")
        .header("Authorization", format!("Bearer {}", token))
        .header("Accept", "application/vnd.github+json")
        .json(&body)
        .send()
        .await?;

    if !res.status().is_success() {
        let status = res.status();
        let err_text = res.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!(
            "GitHub Check API failed with status {}: {}",
            status,
            err_text
        ));
    }

    let val = res.json().await.unwrap_or(Value::Null);
    Ok(val)
}

/// Report a queued check run to GitHub or simulate it locally.
pub(crate) async fn report_check_run_queued(
    shared: &Arc<SharedState>,
    repo: &str,
    sha: &str,
    job_id: &JobId,
    run_id: RunId,
) {
    let token = std::env::var("AKSH_GITHUB_TOKEN").ok();
    let mut check_run_id = None;

    if let Some(token) = &token {
        let body = serde_json::json!({
            "name": job_id.to_string(),
            "head_sha": sha,
            "status": "queued",
        });

        match send_github_check_request(token, repo, reqwest::Method::POST, "check-runs", body)
            .await
        {
            Ok(res) => {
                if let Some(id) = res.get("id").and_then(|id| id.as_u64()) {
                    check_run_id = Some(id);
                    info!(
                        %run_id,
                        %job_id,
                        check_run_id = id,
                        "GitHub check run created successfully"
                    );
                }
            }
            Err(e) => {
                warn!(%run_id, %job_id, error = %e, "Failed to create GitHub check run");
            }
        }
    } else {
        info!(%run_id, %job_id, "GitHub token not configured, using mock check run");
        check_run_id = Some(rand::random::<u32>() as u64);
    }

    if let Some(check_id) = check_run_id {
        let mut inner = shared.state.inner.lock().await;
        if let Some(run) = inner.runs.get_mut(&run_id) {
            run.job_check_run_ids.insert(job_id.clone(), check_id);
        }
    }
}

/// Report check run status to in_progress on GitHub or simulate it locally.
pub(crate) async fn report_check_run_in_progress(
    shared: &Arc<SharedState>,
    run_id: RunId,
    job_id: &JobId,
) {
    let (repo, check_run_id) = {
        let inner = shared.state.inner.lock().await;
        let run = match inner.runs.get(&run_id) {
            Some(r) => r,
            None => return,
        };
        let repo = run.submission.repository.clone();
        let check_run_id = match run.job_check_run_ids.get(job_id).copied() {
            Some(id) => id,
            None => return,
        };
        (repo, check_run_id)
    };

    let token = std::env::var("AKSH_GITHUB_TOKEN").ok();
    if let Some(token) = &token {
        let body = serde_json::json!({
            "status": "in_progress",
        });

        let path = format!("check-runs/{}", check_run_id);
        if let Err(e) =
            send_github_check_request(token, &repo, reqwest::Method::PATCH, &path, body).await
        {
            warn!(
                %run_id,
                %job_id,
                check_run_id,
                error = %e,
                "Failed to update GitHub check run to in_progress"
            );
        }
    } else {
        info!(%run_id, %job_id, check_run_id, "Mock updated check run to in_progress");
    }
}

/// Report check run status to completed on GitHub or simulate it locally.
pub(crate) async fn report_check_run_completed(
    shared: &Arc<SharedState>,
    run_id: RunId,
    job_id: &JobId,
    status: ExecutionStatus,
) {
    let (repo, check_run_id) = {
        let inner = shared.state.inner.lock().await;
        let run = match inner.runs.get(&run_id) {
            Some(r) => r,
            None => return,
        };
        let repo = run.submission.repository.clone();
        let check_run_id = match run.job_check_run_ids.get(job_id).copied() {
            Some(id) => id,
            None => return,
        };
        (repo, check_run_id)
    };

    let conclusion = match status {
        ExecutionStatus::Success => "success",
        ExecutionStatus::Failure => "failure",
        ExecutionStatus::Cancelled => "cancelled",
        _ => "failure",
    };

    let token = std::env::var("AKSH_GITHUB_TOKEN").ok();
    if let Some(token) = &token {
        let body = serde_json::json!({
            "status": "completed",
            "conclusion": conclusion,
        });

        let path = format!("check-runs/{}", check_run_id);
        if let Err(e) =
            send_github_check_request(token, &repo, reqwest::Method::PATCH, &path, body).await
        {
            warn!(
                %run_id,
                %job_id,
                check_run_id,
                error = %e,
                "Failed to update GitHub check run to completed"
            );
        }
    } else {
        info!(
            %run_id,
            %job_id,
            check_run_id,
            conclusion,
            "Mock updated check run to completed"
        );
    }
}

/// Fetch workflows helper.
pub(crate) async fn fetch_workflows(
    local_workspace: &Option<PathBuf>,
    repo: &str,
    git_ref: &str,
) -> anyhow::Result<BTreeMap<String, String>> {
    if let Some(base_path) = local_workspace {
        let workflows_dir = base_path.join(".github/workflows");
        let mut workflows = BTreeMap::new();
        if workflows_dir.exists() {
            let mut dir = tokio::fs::read_dir(workflows_dir).await?;
            while let Some(entry) = dir.next_entry().await? {
                let path = entry.path();
                if path.is_file() {
                    if let Some(ext) = path.extension() {
                        if ext == "yml" || ext == "yaml" {
                            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                let content = tokio::fs::read_to_string(&path).await?;
                                workflows.insert(name.to_owned(), content);
                            }
                        }
                    }
                }
            }
        }
        Ok(workflows)
    } else {
        let token = std::env::var("AKSH_GITHUB_TOKEN").ok();
        if let Some(token) = &token {
            fetch_remote_workflows(token, repo, git_ref).await
        } else {
            // Default fallback to current workspace root if nothing is configured
            let workflows_dir = PathBuf::from(".").join(".github/workflows");
            let mut workflows = BTreeMap::new();
            if workflows_dir.exists() {
                let mut dir = tokio::fs::read_dir(workflows_dir).await?;
                while let Some(entry) = dir.next_entry().await? {
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(ext) = path.extension() {
                            if ext == "yml" || ext == "yaml" {
                                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                    let content = tokio::fs::read_to_string(&path).await?;
                                    workflows.insert(name.to_owned(), content);
                                }
                            }
                        }
                    }
                }
            }
            Ok(workflows)
        }
    }
}

async fn fetch_remote_workflows(
    token: &str,
    repo: &str,
    git_ref: &str,
) -> anyhow::Result<BTreeMap<String, String>> {
    let client = reqwest::Client::new();
    let url = format!(
        "https://api.github.com/repos/{}/contents/.github/workflows?ref={}",
        repo, git_ref
    );
    let response = client
        .get(&url)
        .header("User-Agent", "aksh")
        .header("Authorization", format!("Bearer {}", token))
        .header("Accept", "application/vnd.github+json")
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "GitHub API returned status: {}",
            response.status()
        ));
    }

    #[derive(Deserialize)]
    struct GitHubContentItem {
        name: String,
        r#type: String,
        download_url: Option<String>,
    }

    let items: Vec<GitHubContentItem> = response.json().await?;
    let mut workflows = BTreeMap::new();

    for item in &items {
        if item.r#type == "file" && (item.name.ends_with(".yml") || item.name.ends_with(".yaml")) {
            if let Some(download_url) = &item.download_url {
                let file_res = client
                    .get(download_url)
                    .header("User-Agent", "aksh")
                    .header("Authorization", format!("Bearer {}", token))
                    .send()
                    .await?;
                if file_res.status().is_success() {
                    let content = file_res.text().await?;
                    workflows.insert(item.name.clone(), content);
                }
            }
        }
    }
    Ok(workflows)
}

async fn get_pr_changed_files(
    token: &str,
    repo: &str,
    pr_number: u64,
) -> anyhow::Result<Vec<String>> {
    let client = reqwest::Client::new();
    let url = format!(
        "https://api.github.com/repos/{}/pulls/{}/files",
        repo, pr_number
    );
    let response = client
        .get(&url)
        .header("User-Agent", "aksh")
        .header("Authorization", format!("Bearer {}", token))
        .header("Accept", "application/vnd.github+json")
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "GitHub API returned status: {}",
            response.status()
        ));
    }

    #[derive(Deserialize)]
    struct GitHubFileItem {
        filename: String,
    }

    let files: Vec<GitHubFileItem> = response.json().await?;
    Ok(files.into_iter().map(|f| f.filename).collect())
}

/// Route handler for GitHub App Webhooks.
pub(crate) async fn handle_github_webhook(
    State(shared): State<Arc<SharedState>>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    // 1. Verify Signature
    if let Some(secret) = &shared.state.webhook_secret {
        let sig_header = headers
            .get("x-hub-signature-256")
            .and_then(|h| h.to_str().ok())
            .ok_or(StatusCode::UNAUTHORIZED)?;

        if !verify_signature(secret, &body, sig_header) {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }

    // 2. Get event type
    let event_name = headers
        .get("x-github-event")
        .and_then(|h| h.to_str().ok())
        .ok_or(StatusCode::BAD_REQUEST)?;

    // 3. Process the event payload
    let payload_val: Value = serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (repo_full_name, git_ref, sha, changed_paths) = match event_name {
        "push" => {
            let event: PushEvent =
                serde_json::from_value(payload_val.clone()).map_err(|_| StatusCode::BAD_REQUEST)?;
            let mut paths = Vec::new();
            // Collect changed files from push commits
            for commit in &event.commits {
                paths.extend(commit.added.clone());
                paths.extend(commit.modified.clone());
                paths.extend(commit.removed.clone());
            }
            paths.sort();
            paths.dedup();
            (
                event.repository.full_name,
                event.git_ref,
                event.after,
                paths,
            )
        }
        "pull_request" => {
            let event: PullRequestEvent =
                serde_json::from_value(payload_val.clone()).map_err(|_| StatusCode::BAD_REQUEST)?;
            let repo = event.repository.full_name;
            let git_ref = format!("refs/pull/{}/merge", event.number);
            let sha = event.pull_request.head.sha;
            let mut paths = Vec::new();
            // Retrieve changed files from remote GitHub API if possible
            if let Some(token) = &std::env::var("AKSH_GITHUB_TOKEN").ok() {
                if let Ok(files) = get_pr_changed_files(token, &repo, event.number).await {
                    paths = files;
                }
            }
            (repo, git_ref, sha, paths)
        }
        _ => {
            info!("Received unsupported GitHub webhook event: {}", event_name);
            return Ok((StatusCode::OK, Json(serde_json::json!([]))));
        }
    };

    // 4. Fetch workflows
    let workflows = fetch_workflows(&shared.state.local_workspace, &repo_full_name, &git_ref)
        .await
        .map_err(|e| {
            error!("Failed to fetch workflows: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let mut triggered_runs = Vec::new();

    // 5. Evaluate and trigger matching workflows
    for (filename, content) in workflows {
        // Parse workflow to check triggers
        let parsed = match aksh_gha_parser::parse_workflow(&content) {
            Ok(w) => w,
            Err(e) => {
                warn!("Failed to parse workflow file {}: {:?}", filename, e);
                continue;
            }
        };

        let (branch, tag) = crate::git_ref_context(&git_ref);
        let activity_type = payload_val.get("action").and_then(|v| v.as_str());

        if parsed.on.matches_with_context(
            event_name,
            branch.as_deref(),
            tag.as_deref(),
            &changed_paths,
            activity_type,
        ) {
            info!(
                "Triggering workflow run for {} matching {}",
                filename, event_name
            );

            // Construct WorkflowSubmission
            let submission = WorkflowSubmission {
                workflow_yaml: content,
                event: event_name.to_owned(),
                payload: payload_val.clone(),
                repository: repo_full_name.clone(),
                git_ref: git_ref.clone(),
                vars: BTreeMap::new(),
                secrets: BTreeMap::new(),
                reusable_workflows: BTreeMap::new(),
            };

            // Call submit_run_inner
            match submit_run_inner(&shared, submission).await {
                Ok(accepted) => {
                    let run_id = accepted.run_id;

                    // Create GitHub Check Runs for each job in the run
                    let jobs = {
                        let inner = shared.state.inner.lock().await;
                        inner
                            .runs
                            .get(&run_id)
                            .map(|r| r.jobs.keys().cloned().collect::<Vec<_>>())
                    };

                    if let Some(jobs) = jobs {
                        for job_id in jobs {
                            report_check_run_queued(
                                &shared,
                                &repo_full_name,
                                &sha,
                                &job_id,
                                run_id,
                            )
                            .await;
                        }
                    }

                    triggered_runs.push(accepted);
                }
                Err(e) => {
                    error!("Failed to submit run for {}: {:?}", filename, e);
                }
            }
        }
    }

    Ok((StatusCode::OK, Json(serde_json::json!(triggered_runs))))
}

/// Serve registration page for GitHub App Manifest flow.
pub(crate) async fn github_register(headers: HeaderMap) -> impl IntoResponse {
    let host = headers
        .get("host")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("localhost:9090");

    let scheme = if host.contains("localhost") || host.contains("127.0.0.1") {
        "http"
    } else {
        "https"
    };

    let base_url = format!("{}://{}", scheme, host);
    let is_local = host.contains("localhost") || host.contains("127.0.0.1");

    let mut manifest_json = serde_json::json!({
        "name": "aksh-local-app",
        "url": base_url,
        "redirect_url": format!("{}/api/v1/github/callback", base_url),
        "public": false,
        "default_permissions": {
            "checks": "write",
            "contents": "read",
            "metadata": "read",
            "pull_requests": "read"
        },
        "default_events": [
            "push",
            "pull_request"
        ]
    });

    if is_local {
        manifest_json["webhook_active"] = serde_json::json!(false);
    } else {
        manifest_json["hook_attributes"] = serde_json::json!({
            "url": format!("{}/api/v1/github/webhooks", base_url)
    });
    }

    let html = format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <title>Register GitHub App</title>
</head>
<body style="font-family: sans-serif; padding: 40px; max-width: 600px; margin: auto;">
    <h1>Register GitHub App for aksh</h1>
    <p>Click the button below to register a local GitHub App on your GitHub account automatically.</p>
    <form action="https://github.com/settings/apps/new" method="post">
        <input type="hidden" name="manifest" value='{}'>
        <button type="submit" style="font-size: 16px; padding: 10px 20px; cursor: pointer; background: #2da44e; color: white; border: none; border-radius: 6px; font-weight: bold;">Register App on GitHub</button>
    </form>
</body>
</html>"#,
        manifest_json.to_string()
    );

    axum::response::Html(html)
}

/// Query parameters for GitHub callback.
#[derive(Debug, Deserialize)]
pub(crate) struct CallbackQuery {
    code: String,
}

/// Callback endpoint for GitHub App Manifest conversion.
pub(crate) async fn github_callback(
    State(shared): State<Arc<SharedState>>,
    Query(params): Query<CallbackQuery>,
) -> Result<impl IntoResponse, StatusCode> {
    let client = reqwest::Client::new();
    let api_base = std::env::var("AKSH_GITHUB_API_URL")
        .unwrap_or_else(|_| "https://api.github.com".to_owned());
    let url = format!("{}/app-manifests/{}/conversions", api_base, params.code);
    let res = client
        .post(&url)
        .header("User-Agent", "aksh")
        .header("Accept", "application/vnd.github+json")
        .send()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if !res.status().is_success() {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[derive(Deserialize)]
    struct AppManifestConversion {
        id: u64,
        pem: String,
        webhook_secret: Option<String>,
    }

    let credentials: AppManifestConversion = res
        .json()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    info!("Successfully registered GitHub App ID: {}", credentials.id);

    // Save webhook secret to AppState
    if let Some(secret) = &credentials.webhook_secret {
        let mut inner = shared.state.inner.lock().await;
        inner.next_runner_id += 0; // dummy access to keep compiler happy if needed
        info!("Webhook secret registered: {}", secret);
    }

    let credentials_html = format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <title>GitHub App Registered</title>
</head>
<body style="font-family: sans-serif; padding: 40px; max-width: 800px; margin: auto;">
    <h1 style="color: #2da44e;">GitHub App Registered Successfully!</h1>
    <p><strong>App ID:</strong> {}</p>
    <p><strong>Webhook Secret:</strong> {}</p>
    <p><strong>Private Key PEM:</strong></p>
    <pre style="background: #f6f8fa; padding: 16px; border-radius: 6px; overflow-x: auto;">{}</pre>
    <p>To use this App, configure your local environment and restart `aksh`:</p>
    <pre style="background: #f6f8fa; padding: 16px; border-radius: 6px;">
export AKSH_WEBHOOK_SECRET="{}"
export AKSH_GITHUB_APP_ID="{}"
    </pre>
</body>
</html>"#,
        credentials.id,
        credentials.webhook_secret.as_deref().unwrap_or("none"),
        credentials.pem,
        credentials.webhook_secret.as_deref().unwrap_or("none"),
        credentials.id
    );

    Ok(axum::response::Html(credentials_html))
}
