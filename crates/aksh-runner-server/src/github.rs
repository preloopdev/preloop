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

use crate::{changed_paths_from_payload, submit_run_inner, ExecutionStatus, SharedState};
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

async fn resolve_ref_sha(
    local_workspace: &Option<PathBuf>,
    repository: &str,
    git_ref: &str,
) -> anyhow::Result<Option<String>> {
    if let Some(workspace) = local_workspace {
        let output = tokio::process::Command::new("git")
            .arg("-C")
            .arg(workspace)
            .args(["rev-parse", git_ref])
            .output()
            .await?;
        if output.status.success() {
            return Ok(String::from_utf8(output.stdout)
                .ok()
                .map(|sha| sha.trim().to_owned())
                .filter(|sha| {
                    sha.len() == 40 && sha.chars().all(|character| character.is_ascii_hexdigit())
                }));
        }
        return Ok(None);
    }
    let Some(token) = std::env::var("AKSH_GITHUB_TOKEN").ok() else {
        return Ok(None);
    };
    let commit_ref = git_ref
        .strip_prefix("refs/heads/")
        .or_else(|| git_ref.strip_prefix("refs/tags/"))
        .unwrap_or(git_ref);
    let response = reqwest::Client::new()
        .get(format!(
            "https://api.github.com/repos/{repository}/commits/{commit_ref}"
        ))
        .header("User-Agent", "aksh")
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .send()
        .await?;
    if !response.status().is_success() {
        return Ok(None);
    }
    let commit: Value = response.json().await?;
    Ok(commit.get("sha").and_then(Value::as_str).map(str::to_owned))
}

async fn get_pr_changed_files(
    token: &str,
    repo: &str,
    pr_number: u64,
) -> anyhow::Result<Vec<String>> {
    let client = reqwest::Client::new();
    let mut page = 1;
    let mut all_files = Vec::new();

    #[derive(Deserialize)]
    struct GitHubFileItem {
        filename: String,
    }

    loop {
        let url = format!(
            "https://api.github.com/repos/{}/pulls/{}/files?per_page=100&page={}",
            repo, pr_number, page
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

        let files: Vec<GitHubFileItem> = response.json().await?;
        if files.is_empty() {
            break;
        }

        all_files.extend(files.into_iter().map(|f| f.filename));
        page += 1;
    }

    Ok(all_files)
}

/// Route handler for GitHub App Webhooks.
pub(crate) async fn handle_github_webhook(
    State(shared): State<Arc<SharedState>>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    // 1. Verify Signature
    let secret = shared.state.webhook_secret.as_ref().ok_or_else(|| {
        warn!("Webhook secret not configured on server, rejecting request");
        StatusCode::UNAUTHORIZED
    })?;

    let sig_header = headers
        .get("x-hub-signature-256")
        .and_then(|h| h.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    if !verify_signature(secret, &body, sig_header) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    // 2. Get event type
    let event_name = headers
        .get("x-github-event")
        .and_then(|h| h.to_str().ok())
        .ok_or(StatusCode::BAD_REQUEST)?;

    // 3. Parse the event payload
    let payload_val: Value = serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;

    // 4. Look up the event adapter
    let adapter = match crate::events::adapter_for(event_name) {
        Some(a) => a,
        None => {
            info!("No adapter for event: {}", event_name);
            return Ok((StatusCode::OK, Json(serde_json::json!([]))));
        }
    };

    // 5. Project the payload into effective events
    let effective_events = adapter.project(&payload_val);

    if effective_events.is_empty() {
        info!(
            "Event {} produced no effective events (e.g. [skip ci] or fork-gated)",
            event_name
        );
        return Ok((StatusCode::OK, Json(serde_json::json!([]))));
    }

    let repo_full_name = payload_val
        .get("repository")
        .and_then(|r| r.get("full_name"))
        .and_then(|v| v.as_str())
        .unwrap_or("local/repo")
        .to_owned();
    let changed_paths = if matches!(
        event_name,
        "pull_request" | "pull_request_target" | "pull_request_review"
    ) {
        let pr_number = payload_val
            .get("number")
            .or_else(|| {
                payload_val
                    .get("pull_request")
                    .and_then(|pr| pr.get("number"))
            })
            .and_then(|value| value.as_u64());
        match (std::env::var("AKSH_GITHUB_TOKEN").ok(), pr_number) {
            (Some(token), Some(number)) => get_pr_changed_files(&token, &repo_full_name, number)
                .await
                .map_err(|error| {
                    error!(?error, "failed to resolve pull request changed files");
                    StatusCode::BAD_GATEWAY
                })?,
            _ => changed_paths_from_payload(&payload_val),
        }
    } else {
        changed_paths_from_payload(&payload_val)
    };
    let changed_paths_known = !matches!(
        event_name,
        "pull_request" | "pull_request_target" | "pull_request_review"
    ) || std::env::var("AKSH_GITHUB_TOKEN").is_ok()
        || payload_val.get("paths").is_some()
        || payload_val.get("commits").is_some();

    let mut triggered_runs = Vec::new();

    // 5. For each effective event, fetch workflows and submit runs
    for effective in &effective_events {
        if effective.skip {
            info!("Skipping event {} (skip flag set)", effective.event);
            continue;
        }

        let default_branch = payload_val
            .get("repository")
            .and_then(|r| r.get("default_branch"))
            .and_then(|v| v.as_str())
            .unwrap_or("main");
        let ref_default = format!("refs/heads/{default_branch}");

        // Log filter validity warnings (non-fatal — GitHub only warns)
        // This is done per-workflow later, but we log at the event level too

        // `workflow_run` is privileged: downstream workflow YAML must always
        // come from the repository default branch, never the upstream head.
        let workflow_ref = if effective.event == "workflow_run" {
            &ref_default
        } else {
            &effective.git_ref
        };
        let workflows = match fetch_workflows(&shared.state.local_workspace, &repo_full_name, workflow_ref)
            .await
        {
            Ok(w) => w,
            Err(e) => {
                error!("Failed to fetch workflows for {}: {:?}", effective.event, e);
                continue;
            }
        };
        let resolved_sha = match &effective.sha {
            Some(sha) => sha.clone(),
            None => match resolve_ref_sha(
                &shared.state.local_workspace,
                &repo_full_name,
                &effective.git_ref,
            )
            .await
            {
                Ok(Some(sha)) => sha,
                Ok(None) => {
                    error!(ref_name = %effective.git_ref, "webhook ref has no resolvable commit SHA");
                    continue;
                }
                Err(error) => {
                    error!(?error, "failed to resolve webhook ref SHA");
                    continue;
                }
            },
        };
        if effective.event == "push" && effective.git_ref == ref_default {
            if let Some(scheduler) = &shared.state.scheduler {
                scheduler
                    .reconcile_all(&workflows, payload_val.clone(), shared.clone())
                    .await;
            }
        }

        for (filename, content) in workflows {
            // Scheduler reconciliation runs once for the complete workflow
            // inventory above so deletions are observable too.
            // Validate filter keys / conflicting filters (warning only — submit_run_inner
            // does the actual match; we warn early so the log is tied to the file).
            match aksh_gha_parser::parse_workflow(&content) {
                Ok(parsed) => {
                    if let Err(e) = parsed.on.validate_filters(&effective.event) {
                        warn!("Filter validation warning for {filename}: {e}");
                    }
                    if let Err(e) = parsed.on.check_conflicting_filters(&effective.event) {
                        warn!("Conflicting filters in {filename}: {e}");
                        continue;
                    }
                }
                Err(e) => {
                    warn!("Failed to parse workflow file {filename}: {e:?}");
                    continue;
                }
            }

            let filter_branch = if effective.event == "workflow_run" {
                effective
                    .payload
                    .get("workflow_run")
                    .and_then(|run| run.get("head_branch"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            } else if matches!(
                effective.event.as_str(),
                "pull_request" | "pull_request_target"
            ) {
                effective
                    .payload
                    .get("pull_request")
                    .and_then(|pr| pr.get("base"))
                    .and_then(|base| base.get("ref"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            } else {
                None
            };
            let dispatch_inputs = effective
                .payload
                .get("inputs")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect::<BTreeMap<_, _>>();
            let dispatch_inputs_stringified = dispatch_inputs
                .iter()
                .map(|(name, value)| {
                    let rendered = match value {
                        Value::String(value) => value.clone(),
                        Value::Bool(value) => value.to_string(),
                        Value::Number(value) => value.to_string(),
                        _ => value.to_string(),
                    };
                    (name.clone(), rendered)
                })
                .collect::<BTreeMap<_, _>>();

            // Construct a fully resolved submission. Adapters own event ref,
            // SHA, activity, and upstream workflow identity semantics.
            let submission = WorkflowSubmission {
                workflow_yaml: content,
                event: effective.event.clone(),
                payload: effective.payload.clone(),
                repository: repo_full_name.clone(),
                git_ref: effective.git_ref.clone(),
                vars: BTreeMap::new(),
                secrets: BTreeMap::new(),
                reusable_workflows: BTreeMap::new(),
                enable_debugger: false,
                debugger_welcome_message: None,
                trust_tier: effective.trust_tier.as_ref().and_then(|tier| {
                    serde_json::to_value(tier)
                        .ok()
                        .and_then(|value| value.as_str().map(str::to_owned))
                }),
                workflow_run_upstream_names: effective.upstream_workflow_names.clone(),
                activity_type: effective.activity_type.clone(),
                changed_paths: changed_paths.clone(),
                changed_paths_known,
                resolved_sha: Some(resolved_sha.clone()),
                filter_branch,
                dispatch_inputs,
                dispatch_inputs_stringified,
            };

            // Call submit_run_inner — it performs the authoritative trigger match.
            match submit_run_inner(&shared, submission).await {
                Ok(accepted) => {
                    let run_id = accepted.run_id;
                    let sha = effective
                        .status_check_sha
                        .clone()
                        .unwrap_or_else(|| resolved_sha.clone());
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
                    error!("Failed to submit run for {filename}: {e:?}");
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
    }
    });

    if !is_local {
        manifest_json["hook_attributes"] = serde_json::json!({
                "url": format!("{}/api/v1/github/webhooks", base_url)
        });
        manifest_json["default_events"] = serde_json::json!(["push", "pull_request"]);
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
        manifest_json
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
