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
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::{
    changed_paths_from_payload, submit_run_inner, ExecutionStatus, SharedState,
    WebhookDeliveryState,
};
use preloop_gha_protocol::{AnnotationLevel, JobId, NdjsonEvent, RunId, WorkflowSubmission};

/// Comma-separated workflow filenames or `.github/workflows/...` paths that
/// GitHub, rather than Preloop, owns. This keeps release and artifact-publish
/// workflows out of the local webhook dispatcher while leaving the default
/// generic forges-only behavior unchanged.
pub(crate) const GITHUB_OWNED_WORKFLOWS_ENV: &str = "PRELOOP_GITHUB_SKIP_WORKFLOWS";

pub(crate) fn configured_github_owned_workflows() -> BTreeSet<String> {
    std::env::var(GITHUB_OWNED_WORKFLOWS_ENV)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(|entry| entry.trim_start_matches("./").to_owned())
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn is_github_owned_workflow(filename: &str, configured: &BTreeSet<String>) -> bool {
    let path = format!(".github/workflows/{filename}");
    configured
        .iter()
        .any(|entry| entry == filename || entry == &path)
}

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
    if !hex.len().is_multiple_of(2) {
        return Err("Odd length");
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for i in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| "Invalid hex character")?;
        bytes.push(byte);
    }
    Ok(bytes)
}

/// GitHub REST root, overridable for GHES and for tests that point the server
/// at a stub API.
pub(crate) fn github_api_base() -> String {
    std::env::var("PRELOOP_GITHUB_API_URL")
        .ok()
        .map(|base| base.trim_end_matches('/').to_owned())
        .filter(|base| !base.is_empty())
        .unwrap_or_else(|| "https://api.github.com".to_owned())
}

async fn resolve_check_run_token(shared: &Arc<SharedState>, repo: &str) -> Option<String> {
    if let Some(app_creds) = &shared.state.github_app {
        let mut permissions = std::collections::BTreeMap::new();
        permissions.insert("checks".to_owned(), "write".to_owned());
        // The App mint intermittently 422s while the installation grants are
        // being read; a single retry keeps a transient rejection from
        // stranding the check run in `queued` (the fallback JWT cannot
        // PATCH check runs and GitHub keeps showing them pending).
        for attempt in 0..2 {
            match crate::github_app::get_or_mint_token(app_creds, repo, &permissions).await {
                Ok(token) => return Some(token),
                Err(error) if attempt == 0 => {
                    tracing::warn!(
                        %repo,
                        %error,
                        "check run token mint failed; retrying once"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(_) => break,
            }
        }
    }
    std::env::var("PRELOOP_GITHUB_TOKEN").ok()
}

async fn send_github_check_request(
    token: &str,
    repo: &str,
    method: reqwest::Method,
    path: &str,
    body: Value,
) -> anyhow::Result<Value> {
    let client = crate::shared_http::CLIENT.clone();
    let url = format!("{}/repos/{}/{}", github_api_base(), repo, path);
    let res = client
        .request(method, &url)
        .header("User-Agent", "preloop")
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

pub(crate) fn run_details_url(run_id: RunId) -> Option<String> {
    std::env::var("PRELOOP_PUBLIC_URL")
        .ok()
        .map(|base| format!("{}/runs/{run_id}", base.trim_end_matches('/')))
}

/// Report a queued check run to GitHub or simulate it locally.
pub(crate) async fn report_check_run_queued(
    shared: &Arc<SharedState>,
    repo: &str,
    sha: &str,
    job_id: &JobId,
    run_id: RunId,
) {
    let token = resolve_check_run_token(shared, repo).await;
    let mut check_run_id = None;

    if let Some(token) = &token {
        let details_url = run_details_url(run_id);

        let mut body = serde_json::json!({
            "name": job_id.to_string(),
            "head_sha": sha,
            "status": "queued",
        });
        if let Some(url) = details_url {
            body["details_url"] = serde_json::json!(url);
        }

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
        let mapping_changed = {
            let mut inner = shared.state.inner.lock().await;
            inner.runs.get_mut(&run_id).and_then(|run| {
                Some(
                    run.job_check_run_ids
                        .insert(job_id.clone(), check_id)
                        .is_none_or(|previous| previous != check_id),
                )
            })
        };
        if mapping_changed == Some(true) {
            // Persist the record now. The mapping is only meaningful while
            // the run lives, and the next status event may be hours away (a
            // long queue); a restart in that window used to restore the run
            // with an empty mapping, silently orphaning the GitHub check in
            // "queued" forever even though the job ran and completed.
            shared
                .state
                .emit(preloop_gha_protocol::NdjsonEvent::CheckRunCreated { run_id })
                .await;
        }
    }
}

/// Move an existing GitHub check run back to the queue after a rerequest.
pub(crate) async fn report_existing_check_run_queued(
    shared: &Arc<SharedState>,
    repo: &str,
    job_id: &JobId,
    run_id: RunId,
    check_run_id: u64,
) {
    let token = resolve_check_run_token(shared, repo).await;
    if let Some(token) = &token {
        let mut body = serde_json::json!({
            "status": "queued",
        });
        if let Some(url) = run_details_url(run_id) {
            body["details_url"] = serde_json::json!(url);
        }
        let path = format!("check-runs/{check_run_id}");
        if let Err(error) =
            send_github_check_request(token, repo, reqwest::Method::PATCH, &path, body).await
        {
            warn!(
                %run_id,
                %job_id,
                check_run_id,
                %error,
                "Failed to requeue GitHub check run"
            );
        }
    } else {
        info!(
            %run_id,
            %job_id,
            check_run_id,
            "Mock requeued GitHub check run"
        );
    }
}

/// Publish queued/completed checks for a native rerun.
pub(crate) async fn report_check_runs_for_run(
    shared: &Arc<SharedState>,
    run_id: RunId,
    reused_check_run: Option<(JobId, u64)>,
) {
    let (repository, sha, jobs) = {
        let inner = shared.state.inner.lock().await;
        let Some(run) = inner.runs.get(&run_id) else {
            return;
        };
        (
            run.submission.repository.clone(),
            run.submission.sha.clone(),
            run.jobs.keys().cloned().collect::<Vec<_>>(),
        )
    };

    for job_id in jobs {
        if let Some((reused_job_id, check_run_id)) = &reused_check_run {
            if reused_job_id == &job_id {
                report_existing_check_run_queued(
                    shared,
                    &repository,
                    &job_id,
                    run_id,
                    *check_run_id,
                )
                .await;
            } else {
                report_check_run_queued(shared, &repository, &sha, &job_id, run_id).await;
            }
        } else {
            report_check_run_queued(shared, &repository, &sha, &job_id, run_id).await;
        }

        let status = {
            let inner = shared.state.inner.lock().await;
            inner
                .runs
                .get(&run_id)
                .and_then(|run| run.jobs.get(&job_id).copied())
        };
        if let Some(status) = status.filter(|status| status.is_terminal()) {
            report_check_run_completed(shared, run_id, &job_id, status).await;
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

    let token = resolve_check_run_token(shared, &repo).await;
    if let Some(token) = &token {
        let details_url = run_details_url(run_id);

        let mut body = serde_json::json!({
            "status": "in_progress",
        });
        if let Some(url) = details_url {
            body["details_url"] = serde_json::json!(url);
        }

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
    let (repo, check_run_id, annotations, global_issues) = {
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

        let mut annotations = Vec::new();
        let mut global_issues = Vec::new();

        if let Some(events) = inner.timeline_events.get(&run_id) {
            for event in events {
                if let NdjsonEvent::Annotation {
                    job_id: event_job_id,
                    level,
                    message,
                    file,
                    line,
                    ..
                } = event
                {
                    if event_job_id == job_id {
                        let level_str = match level {
                            AnnotationLevel::Notice => "notice",
                            AnnotationLevel::Warning => "warning",
                            AnnotationLevel::Error => "failure",
                        };
                        if let Some(file_path) = file {
                            let line_num = line.unwrap_or(1);
                            annotations.push(serde_json::json!({
                                "path": file_path,
                                "start_line": line_num,
                                "end_line": line_num,
                                "annotation_level": level_str,
                                "message": message,
                            }));
                        } else {
                            global_issues.push(format!(
                                "**{}**: {}",
                                level_str.to_uppercase(),
                                message
                            ));
                        }
                    }
                }
            }
        }

        if annotations.len() > 50 {
            annotations.truncate(50);
        }

        (repo, check_run_id, annotations, global_issues)
    };

    let conclusion = match status {
        ExecutionStatus::Success => "success",
        ExecutionStatus::Failure => "failure",
        ExecutionStatus::Cancelled => "cancelled",
        ExecutionStatus::Skipped => "skipped",
        _ => "failure",
    };

    let token = resolve_check_run_token(shared, &repo).await;
    if let Some(token) = &token {
        let details_url = run_details_url(run_id);

        let summary = if global_issues.is_empty() {
            format!("Job completed with status: {}", conclusion)
        } else {
            format!(
                "Job completed with status: {}\n\n### Global/Job-Level Issues:\n{}",
                conclusion,
                global_issues.join("\n")
            )
        };

        let mut body = serde_json::json!({
            "status": "completed",
            "conclusion": conclusion,
        });
        if let Some(url) = details_url {
            body["details_url"] = serde_json::json!(url);
        }

        if !annotations.is_empty() || !global_issues.is_empty() {
            body["output"] = serde_json::json!({
                "title": format!("Job: {}", job_id.0),
                "summary": summary,
                "annotations": annotations,
            });
        }

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
            annotations_count = annotations.len(),
            global_issues_count = global_issues.len(),
            "Mock updated check run to completed"
        );
    }
}

/// Fetch workflows helper.
pub(crate) async fn fetch_workflows(
    shared: &Arc<SharedState>,
    repo: &str,
    git_ref: &str,
) -> anyhow::Result<BTreeMap<String, String>> {
    let api_base = github_api_base();
    fetch_workflows_at(shared, repo, git_ref, &api_base).await
}

pub(crate) async fn fetch_workflows_at(
    shared: &Arc<SharedState>,
    repo: &str,
    git_ref: &str,
    api_base: &str,
) -> anyhow::Result<BTreeMap<String, String>> {
    if let Some(base_path) = &shared.state.local_workspace {
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
        let token = if let Some(app) = &shared.state.github_app {
            let permissions = BTreeMap::from([("contents".to_owned(), "read".to_owned())]);
            Some(crate::github_app::get_or_mint_token_at(api_base, app, repo, &permissions).await?)
        } else {
            std::env::var("PRELOOP_GITHUB_TOKEN").ok()
        };
        if let Some(token) = &token {
            fetch_remote_workflows(token, repo, git_ref, api_base).await
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
    api_base: &str,
) -> anyhow::Result<BTreeMap<String, String>> {
    let client = crate::shared_http::CLIENT.clone();
    let url = format!(
        "{}/repos/{}/contents/.github/workflows?ref={}",
        api_base.trim_end_matches('/'),
        repo,
        git_ref
    );
    let response = client
        .get(&url)
        .header("User-Agent", "preloop")
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
                    .header("User-Agent", "preloop")
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
    let Some(token) = std::env::var("PRELOOP_GITHUB_TOKEN").ok() else {
        return Ok(None);
    };
    let commit_ref = git_ref
        .strip_prefix("refs/heads/")
        .or_else(|| git_ref.strip_prefix("refs/tags/"))
        .unwrap_or(git_ref);
    let response = crate::shared_http::CLIENT
        .clone()
        .get(format!(
            "https://api.github.com/repos/{repository}/commits/{commit_ref}"
        ))
        .header("User-Agent", "preloop")
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
    api_base: &str,
) -> anyhow::Result<Vec<String>> {
    let client = crate::shared_http::CLIENT.clone();
    let mut page = 1;
    let mut all_files = Vec::new();

    #[derive(Deserialize)]
    struct GitHubFileItem {
        filename: String,
    }

    loop {
        let url = format!(
            "{}/repos/{}/pulls/{}/files?per_page=100&page={}",
            api_base.trim_end_matches('/'),
            repo,
            pr_number,
            page
        );
        let response = client
            .get(&url)
            .header("User-Agent", "preloop")
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

/// Changed files for a pull request, or `None` when nothing can authenticate
/// the lookup.
///
/// A webhook payload never carries the full file list, so `paths:` and
/// `paths-ignore:` can only be evaluated against this call. Consulting
/// `PRELOOP_GITHUB_TOKEN` alone would leave an App-only deployment — the
/// documented way to run this server — permanently unable to answer, and every
/// path-filtered workflow would be rejected as unevaluable rather than queued.
/// So the App is tried first, exactly as the workflow inventory does.
///
/// The workflow-inventory token is not reused: it is scoped to
/// `contents: read`, and listing pull request files needs `pull_requests`.
pub(crate) async fn resolve_pr_changed_files_at(
    shared: &Arc<SharedState>,
    repo: &str,
    pr_number: u64,
    api_base: &str,
) -> anyhow::Result<Option<Vec<String>>> {
    let token = if let Some(app) = &shared.state.github_app {
        let permissions = BTreeMap::from([("pull_requests".to_owned(), "read".to_owned())]);
        Some(crate::github_app::get_or_mint_token_at(api_base, app, repo, &permissions).await?)
    } else {
        std::env::var("PRELOOP_GITHUB_TOKEN").ok()
    };
    let Some(token) = token else {
        return Ok(None);
    };
    get_pr_changed_files(&token, repo, pr_number, api_base)
        .await
        .map(Some)
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

    // Dedup redelivered webhooks. GitHub redelivers on 5xx and occasionally
    // double-fires; processing the same delivery twice created duplicate runs
    // per workflow (observed: two runs per push, every job dispatched twice
    // and the pool saturated). The delivery ID is the authoritative key —
    // one delivery must produce exactly one processing pass.
    //
    // The reservation is only permanent once processing succeeded. A delivery
    // that failed releases its reservation below so GitHub's redelivery of it
    // is processed instead of being silently dropped.
    const WEBHOOK_DEDUP_WINDOW: std::time::Duration = std::time::Duration::from_secs(300);
    let delivery_id = headers
        .get("x-github-delivery")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if let Some(delivery_id) = &delivery_id {
        let now = std::time::Instant::now();
        let mut inner = shared.state.inner.lock().await;
        inner.webhook_deliveries.retain(|(_, state)| match state {
            WebhookDeliveryState::InFlight => true,
            WebhookDeliveryState::Completed(at) => now.duration_since(*at) < WEBHOOK_DEDUP_WINDOW,
        });
        if let Some((_, state)) = inner
            .webhook_deliveries
            .iter()
            .find(|(seen, _)| seen == delivery_id)
        {
            let reason = match state {
                WebhookDeliveryState::InFlight => "already in flight",
                WebhookDeliveryState::Completed(_) => "already processed",
            };
            info!(delivery = %delivery_id, reason, "Duplicate GitHub webhook delivery — skipping");
            return Ok((StatusCode::OK, Json(serde_json::json!([]))));
        }
        inner
            .webhook_deliveries
            .push_back((delivery_id.clone(), WebhookDeliveryState::InFlight));
    }

    // A delivery that dies mid-processing — the future is cancelled (client
    // disconnect, server shutdown) or the processing panics — never reaches
    // the Completed/release code below, so its InFlight reservation would
    // stick forever and make GitHub's later redelivery look like a duplicate
    // that gets skipped: a silently lost run. The guard releases the
    // reservation on every exit path, including cancellation and panic.
    let mut reservation_guard = delivery_id
        .as_ref()
        .map(|id| InFlightReservationGuard::arm(shared.clone(), id.clone()));

    // Everything past the reservation runs in a helper so that no `?` can
    // escape while still holding the in-flight marker.
    let result = process_github_webhook(&shared, &headers, &body).await;

    if let Some(delivery_id) = &delivery_id {
        let mut inner = shared.state.inner.lock().await;
        match &result {
            Ok(_) => {
                let completed_at = std::time::Instant::now();
                if let Some((_, state)) = inner
                    .webhook_deliveries
                    .iter_mut()
                    .find(|(seen, _)| seen == delivery_id)
                {
                    *state = WebhookDeliveryState::Completed(completed_at);
                }
                // The Completed entry is now the durable reservation; without
                // disarming, the guard's drop would erase it and the dedup
                // window would admit a duplicate run.
                if let Some(guard) = &mut reservation_guard {
                    guard.disarm();
                }
            }
            // Release the reservation: GitHub retries this delivery and the
            // retry must be allowed to do the work this attempt did not.
            Err(_) => inner
                .webhook_deliveries
                .retain(|(seen, _)| seen != delivery_id),
        }
    }

    result
}

/// RAII release of a webhook delivery's in-flight reservation.
///
/// The reservation is normally released explicitly — `Completed` once
/// processing succeeded, removed once it failed. If processing is cancelled or
/// panics instead, neither path runs; this guard releases the reservation on
/// drop so a redelivery is processed rather than skipped as a duplicate.
struct InFlightReservationGuard {
    shared: Arc<SharedState>,
    delivery_id: String,
    armed: bool,
}

impl InFlightReservationGuard {
    fn arm(shared: Arc<SharedState>, delivery_id: String) -> Self {
        Self {
            shared,
            delivery_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InFlightReservationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let shared = self.shared.clone();
        let delivery_id = self.delivery_id.clone();
        // Synchronous release when the state lock is free (the common case:
        // the guard outlives the processing, so no one else is holding it).
        if let Ok(mut inner) = shared.state.inner.try_lock() {
            inner
                .webhook_deliveries
                .retain(|(seen, _)| seen != &delivery_id);
            return;
        }
        // The state lock is contended; defer the release to the runtime so the
        // reservation is still cleared promptly instead of leaking forever.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let mut inner = shared.state.inner.lock().await;
                inner
                    .webhook_deliveries
                    .retain(|(seen, _)| seen != &delivery_id);
            });
        }
    }
}

/// Handle GitHub's Checks API rerequest action by resubmitting the native run
/// that owns the requested check. GitHub sends this as a `check_run` webhook,
/// not as a workflow trigger event.
async fn process_check_run_rerequest(
    shared: &Arc<SharedState>,
    payload: &Value,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    if payload.get("action").and_then(Value::as_str) != Some("rerequested") {
        return Ok((StatusCode::OK, Json(serde_json::json!([]))));
    }

    let Some(check_run) = payload.get("check_run") else {
        warn!("check_run rerequest is missing check_run payload");
        return Ok((StatusCode::OK, Json(serde_json::json!([]))));
    };
    let Some(check_run_id) = check_run.get("id").and_then(Value::as_u64) else {
        warn!("check_run rerequest is missing check_run.id");
        return Ok((StatusCode::OK, Json(serde_json::json!([]))));
    };
    let repository = payload
        .get("repository")
        .and_then(|repository| repository.get("full_name"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let head_sha = check_run
        .get("head_sha")
        .and_then(Value::as_str)
        .filter(|sha| !sha.is_empty());
    let job_name = check_run.get("name").and_then(Value::as_str);
    let details_run_id = check_run
        .get("details_url")
        .and_then(Value::as_str)
        .and_then(|url| {
            url.trim_end_matches('/')
                .rsplit('/')
                .next()
                .and_then(|value| value.parse::<RunId>().ok())
        });

    let target = {
        let inner = shared.state.inner.lock().await;
        let mut candidates = Vec::new();
        if let Some(run_id) = details_run_id {
            candidates.push(run_id);
        }
        candidates.extend(
            inner
                .runs
                .keys()
                .filter(|run_id| Some(**run_id) != details_run_id),
        );

        candidates.into_iter().find_map(|run_id| {
            let run = inner.runs.get(&run_id)?;
            if run.submission.repository != repository
                || head_sha.is_some_and(|sha| run.head_sha != sha)
                || !run.status.is_terminal()
            {
                return None;
            }

            let job_id = run
                .job_check_run_ids
                .iter()
                .find_map(|(job_id, id)| (*id == check_run_id).then(|| job_id.clone()))
                .or_else(|| {
                    job_name
                        .map(|name| JobId(name.to_owned()))
                        .filter(|job_id| run.jobs.contains_key(job_id))
                })?;
            Some((run_id, job_id))
        })
    };

    let Some((run_id, job_id)) = target else {
        warn!(
            repository,
            check_run_id, "check_run rerequest does not match a known terminal run"
        );
        return Ok((StatusCode::OK, Json(serde_json::json!([]))));
    };

    let accepted = crate::rerun_run_inner(shared, run_id, Some((job_id.clone(), check_run_id)))
        .await
        .map_err(|error| {
            error!(
                %run_id,
                %job_id,
                check_run_id,
                ?error,
                "failed to resubmit check_run rerequest"
            );
            error.into_response().status()
        })?;
    info!(
        %run_id,
        rerun_run_id = %accepted.run_id,
        %job_id,
        check_run_id,
        "resubmitted check_run rerequest"
    );
    Ok((StatusCode::OK, Json(serde_json::json!([accepted]))))
}

/// Processing half of [`handle_github_webhook`], after signature verification
/// and delivery reservation.
async fn process_github_webhook(
    shared: &Arc<SharedState>,
    headers: &HeaderMap,
    body: &bytes::Bytes,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    // 2. Get event type
    let event_name = headers
        .get("x-github-event")
        .and_then(|h| h.to_str().ok())
        .ok_or(StatusCode::BAD_REQUEST)?;

    // 3. Parse the event payload
    let payload_val: Value = serde_json::from_slice(body).map_err(|_| StatusCode::BAD_REQUEST)?;

    if event_name == "check_run" {
        return process_check_run_rerequest(shared, &payload_val).await;
    }

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
    let (changed_paths, changed_paths_known) = if matches!(
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
        let api_base = std::env::var("PRELOOP_GITHUB_API_URL")
            .unwrap_or_else(|_| "https://api.github.com".to_owned());
        let fetched = match pr_number {
            Some(number) => resolve_pr_changed_files_at(shared, &repo_full_name, number, &api_base)
                .await
                .map_err(|error| {
                    error!(?error, "failed to resolve pull request changed files");
                    StatusCode::BAD_GATEWAY
                })?,
            None => None,
        };
        match fetched {
            // The list came from the API, so a `paths:` filter that matches
            // nothing is a real "no match" rather than a missing answer.
            Some(files) => (files, true),
            None => (
                changed_paths_from_payload(&payload_val),
                payload_val.get("paths").is_some() || payload_val.get("commits").is_some(),
            ),
        }
    } else {
        (changed_paths_from_payload(&payload_val), true)
    };

    let mut triggered_runs = Vec::new();
    let github_owned_workflows = configured_github_owned_workflows();

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
        let workflows = fetch_workflows(shared, &repo_full_name, workflow_ref)
            .await
            .map_err(|error| {
                error!(
                    event = %effective.event,
                    ?error,
                    "Failed to fetch workflows — delivery failed, will be redelivered"
                );
                StatusCode::BAD_GATEWAY
            })?;
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
                    error!(
                        ref_name = %effective.git_ref,
                        "webhook ref has no resolvable commit SHA — delivery failed, will be redelivered"
                    );
                    return Err(StatusCode::BAD_GATEWAY);
                }
                Err(error) => {
                    error!(
                        ?error,
                        "failed to resolve webhook ref SHA — delivery failed, will be redelivered"
                    );
                    return Err(StatusCode::BAD_GATEWAY);
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
            if is_github_owned_workflow(&filename, &github_owned_workflows) {
                info!(
                    workflow = %filename,
                    event = %effective.event,
                    "Skipping workflow owned by GitHub"
                );
                continue;
            }
            // Scheduler reconciliation runs once for the complete workflow
            // inventory above so deletions are observable too.
            // Validate filter keys / conflicting filters (warning only — submit_run_inner
            // does the actual match; we warn early so the log is tied to the file).
            match preloop_gha_parser::parse_workflow(&content) {
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
                workflow_path: Some(format!(".github/workflows/{filename}")),
                local_workspace: None,
                vars: BTreeMap::new(),
                secrets: BTreeMap::new(),
                submission_names: BTreeSet::new(),
                reusable_workflows: BTreeMap::new(),
                reusable_workflow_shas: BTreeMap::new(),
                enable_debugger: false,
                debugger_welcome_message: None,
                sha: resolved_sha.clone(),
                actor: payload_val
                    .get("sender")
                    .and_then(|s| s.get("login"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("preloop-system")
                    .to_owned(),
                environment: None,
                workflow_file: Some(filename.clone()),
                inputs: BTreeMap::new(),
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
                selected_jobs: vec![],
                base_ref: None,
                preserve_on_failure: false,
                push: None,
                push_tree: None,
            };

            // Push-back already ran this exact workflow against this exact
            // commit and published its checks; the delivery we are handling
            // is the echo of that push. Re-running would duplicate the work
            // and overwrite good results with a second set.
            if let Some(tested_by) = crate::github_push::already_published(
                shared,
                &repo_full_name,
                &submission.sha,
                submission.workflow_path.as_deref().unwrap_or_default(),
            )
            .await
            {
                info!(
                    workflow = %filename,
                    sha = %submission.sha,
                    run_id = %tested_by,
                    "skipping webhook run: this commit was already tested and published by push-back"
                );
                continue;
            }

            // Call submit_run_inner — it performs the authoritative trigger match.
            match submit_run_inner(shared, submission).await {
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
                            report_check_run_queued(shared, &repo_full_name, &sha, &job_id, run_id)
                                .await;
                            // If the job was already resolved at submission
                            // time (e.g. skipped due to unsatisfiable
                            // dependencies), report completion immediately so
                            // the GitHub check does not stay queued forever.
                            let status = {
                                let inner = shared.state.inner.lock().await;
                                inner
                                    .runs
                                    .get(&run_id)
                                    .and_then(|r| r.jobs.get(&job_id).copied())
                            };
                            if let Some(status) = status.filter(|s| s.is_terminal()) {
                                report_check_run_completed(shared, run_id, &job_id, status).await;
                            }
                        }
                    }
                    triggered_runs.push(accepted);
                }
                Err(e) => {
                    // A 4xx submission outcome means the workflow legitimately
                    // does not run for this event — no trigger match, or a
                    // permanent workflow problem a redelivery cannot fix — so
                    // the delivery itself is complete. A 5xx means the
                    // delivery's work did not finish: surface it as a failure
                    // so the delivery is not marked Completed and GitHub's
                    // redelivery gets a real chance to do the work.
                    let detail = format!("{e:?}");
                    let status = e.into_response().status();
                    if status.is_client_error() {
                        info!(
                            "Workflow {filename} was not triggered by this event ({detail}) — not a delivery failure"
                        );
                        continue;
                    }
                    error!(
                        "Failed to submit run for {filename}: {detail} — delivery failed, will be redelivered"
                    );
                    return Err(status);
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
        "name": "preloop-local-app",
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
    <h1>Register GitHub App for preloop</h1>
    <p>Click the button below to register a local GitHub App on your GitHub account automatically.</p>
    <form action="https://github.com/settings/apps/new" method="post">
        <input type="hidden" name="manifest" value='{}'>
        <button type="submit" style="font-size: 16px; padding: 10px 20px; cursor: pointer; background: #2da44e; color: white; border: none; border-radius: 6px; font-weight: bold;">Register App on GitHub</button>
    </form>
    <p style="color: #57606a; font-size: 14px;">
        If you want to run CI before creating a pull request
        (<code>preloop run --push --create-pr</code>), grant the App
        <code>pull_requests: write</code>: GitHub App settings &rarr;
        Permissions &rarr; Pull requests &rarr; Read and write. Check-run
        reporting works with just <code>checks: write</code>.
    </p>
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
    // The App credentials are handed to the operator through the one-time HTML
    // response below; `AppState` is immutable behind an `Arc`, so nothing here
    // can persist them into the running server.
    State(_shared): State<Arc<SharedState>>,
    Query(params): Query<CallbackQuery>,
) -> Result<impl IntoResponse, StatusCode> {
    let client = crate::shared_http::CLIENT.clone();
    let api_base = std::env::var("PRELOOP_GITHUB_API_URL")
        .unwrap_or_else(|_| "https://api.github.com".to_owned());
    let url = format!("{}/app-manifests/{}/conversions", api_base, params.code);
    let res = client
        .post(&url)
        .header("User-Agent", "preloop")
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
        #[serde(default)]
        slug: Option<String>,
        pem: String,
        webhook_secret: Option<String>,
    }

    let credentials: AppManifestConversion = res
        .json()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    info!("Successfully registered GitHub App ID: {}", credentials.id);

    // The webhook secret is deliberately never logged: it is the only thing
    // standing between this server and a forged `push` event. It reaches the
    // operator through the one-time HTML handoff below, the same way the
    // private key does, and is configured from there.
    info!(
        has_webhook_secret = credentials.webhook_secret.is_some(),
        "GitHub App credentials received"
    );

    let install = credentials
        .slug
        .as_ref()
        .map(|slug| format!("https://github.com/apps/{slug}/installations/new"));
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
    <p>Save the key to a file, then hand both to the engine and restart it:</p>
    <pre style="background: #f6f8fa; padding: 16px; border-radius: 6px;">
preloop setup github --via app --app-id {} --pem-file ./app.pem
export PRELOOP_WEBHOOK_SECRET="{}"
    </pre>
    <p>{}</p>
    <p>On the machine running the engine, <code>preloop setup github --via app</code>
       does all of this without copying anything out of a browser.</p>
</body>
</html>"#,
        credentials.id,
        credentials.webhook_secret.as_deref().unwrap_or("none"),
        credentials.pem,
        credentials.id,
        credentials.webhook_secret.as_deref().unwrap_or("none"),
        install
            .as_ref()
            .map(|url| format!(
                r#"Then install it on your repositories: <a href="{url}">{url}</a>"#
            ))
            .unwrap_or_else(|| {
                "Then install it on your repositories from the App's settings page.".to_owned()
            }),
    );

    Ok(axum::response::Html(credentials_html))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppState;
    use axum::body::Body;
    use axum::http::{HeaderValue, Method, Request};
    use std::future::Future;
    use tokio_util::sync::CancellationToken;
    use tower::ServiceExt;

    /// Sign `payload` the way GitHub does: HMAC-SHA256 over the raw body with
    /// the webhook secret, hex-encoded and prefixed with `sha256=`.
    fn sign(payload: &[u8], secret: &str) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(payload);
        let sig = mac.finalize().into_bytes();
        let hex = sig.iter().map(|b| format!("{b:02x}")).collect::<String>();
        format!("sha256={hex}")
    }

    #[test]
    fn github_owned_workflow_filter_matches_filename_or_path() {
        let configured =
            "release.yml, .github/workflows/release-golden.yml, ./release-linux-runner.yml"
                .split(',')
                .map(str::trim)
                .map(|entry| entry.trim_start_matches("./").to_owned())
                .collect();

        assert!(is_github_owned_workflow("release.yml", &configured));
        assert!(is_github_owned_workflow("release-golden.yml", &configured));
        assert!(is_github_owned_workflow(
            "release-linux-runner.yml",
            &configured
        ));
        assert!(!is_github_owned_workflow("ci.yml", &configured));
    }

    /// The signed push payload GitHub would deliver for `owner/repo`.
    fn signed_push_payload() -> (Vec<u8>, String) {
        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "before": "0000000000000000000000000000000000000000",
            "after": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "repository": {"full_name": "owner/repo", "default_branch": "main"},
            "commits": [{
                "id": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
                "added": ["src/main.rs"],
                "modified": [],
                "removed": []
            }],
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let signature = sign(&bytes, "super-secret");
        (bytes, signature)
    }

    /// In-process server fixture mirroring the crate's `WebhookDedupFixture`:
    /// a local workspace (with one push-triggered workflow by default), a
    /// webhook secret, and the signed push payload.
    struct WebhookFixture {
        state: AppState,
        app: axum::Router,
        payload_bytes: Vec<u8>,
        signature_header: String,
    }

    impl WebhookFixture {
        /// Standard fixture: the workspace holds one push-triggered workflow.
        async fn new(temp: &tempfile::TempDir) -> Self {
            let ws_dir = temp.path().join("ws");
            std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
            std::fs::write(
                ws_dir.join(".github/workflows/build.yml"),
                "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
            )
            .unwrap();
            Self::with_workspace(temp, ws_dir).await
        }

        async fn with_workspace(temp: &tempfile::TempDir, ws_dir: std::path::PathBuf) -> Self {
            let mut state = AppState::new(temp.path().join("state").to_path_buf())
                .await
                .unwrap();
            state.webhook_secret = Some("super-secret".to_owned());
            state.local_workspace = Some(ws_dir);
            let app =
                crate::app_with_test_api(state.clone(), CancellationToken::new(), "test-token");
            let (payload_bytes, signature_header) = signed_push_payload();
            Self {
                state,
                app,
                payload_bytes,
                signature_header,
            }
        }

        /// Deliver the signed standard payload under `delivery`.
        async fn post(&self, delivery: &str, event: Option<&str>) -> StatusCode {
            self.post_body(delivery, event, &self.payload_bytes).await
        }

        /// Deliver an arbitrary signed payload under `delivery`.
        async fn post_body(
            &self,
            delivery: &str,
            event: Option<&str>,
            payload: &[u8],
        ) -> StatusCode {
            let app = self.app.clone();
            let signature = sign(payload, "super-secret");
            let mut request = Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-delivery", delivery)
                .header("x-hub-signature-256", signature)
                .header("content-type", "application/json");
            if let Some(event) = event {
                request = request.header("x-github-event", event);
            }
            app.oneshot(request.body(Body::from(payload.to_vec())).unwrap())
                .await
                .unwrap()
                .status()
        }
    }

    /// Issue 1: a delivery whose workflow inventory cannot be fetched must not
    /// be acknowledged as processed. GitHub only redelivers after an error
    /// response, and a Completed (or lingering InFlight) marker would make the
    /// retry look like a duplicate and skip it — a silently lost run.
    #[tokio::test]
    async fn webhook_workflow_fetch_failure_is_redelivered() {
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(&ws_dir).unwrap();
        std::fs::create_dir_all(ws_dir.join(".github")).unwrap();
        // `.github/workflows` exists but is a regular file: the inventory read
        // fails deterministically (read_dir on a non-directory errors), so the
        // delivery cannot be processed.
        std::fs::write(ws_dir.join(".github/workflows"), "not a directory").unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir.clone()).await;

        let status = fixture.post("delivery-fetch-fail", Some("push")).await;
        assert_eq!(
            status,
            StatusCode::BAD_GATEWAY,
            "a delivery whose processing failed must not be acknowledged as processed"
        );
        {
            let inner = fixture.state.inner.lock().await;
            assert!(
                !inner
                    .webhook_deliveries
                    .iter()
                    .any(|(id, _)| id == "delivery-fetch-fail"),
                "a failed delivery must not keep a Completed/InFlight marker"
            );
            assert!(
                inner.runs.is_empty(),
                "a failed delivery must not create a run"
            );
        }

        // Repair the workspace: the redelivery GitHub sends after the error
        // response must be processed instead of dropped as a duplicate.
        std::fs::remove_file(ws_dir.join(".github/workflows")).unwrap();
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/build.yml"),
            "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        )
        .unwrap();

        assert_eq!(
            fixture.post("delivery-fetch-fail", Some("push")).await,
            StatusCode::OK
        );
        let inner = fixture.state.inner.lock().await;
        assert_eq!(
            inner.runs.len(),
            1,
            "the redelivery of a failed delivery must create the run"
        );
    }

    /// Issue 1: a delivery whose ref SHA cannot be resolved must not be
    /// acknowledged as processed either — the run cannot be created and would
    /// be silently lost if the delivery were marked Completed.
    #[tokio::test]
    async fn webhook_unresolvable_sha_is_redelivered() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;

        // A push without `after` leaves the effective SHA unresolved, and the
        // local workspace is not a Git repository, so no SHA can be resolved:
        // the delivery's work cannot be done.
        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "before": "0000000000000000000000000000000000000000",
            "repository": {"full_name": "owner/repo", "default_branch": "main"},
            "commits": [{
                "id": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
                "added": ["src/main.rs"],
                "modified": [],
                "removed": []
            }],
        });
        let bytes = serde_json::to_vec(&payload).unwrap();

        let status = fixture
            .post_body("delivery-no-sha", Some("push"), &bytes)
            .await;
        assert_eq!(
            status,
            StatusCode::BAD_GATEWAY,
            "a delivery whose ref SHA cannot be resolved must not be acknowledged as processed"
        );
        let inner = fixture.state.inner.lock().await;
        assert!(
            !inner
                .webhook_deliveries
                .iter()
                .any(|(id, _)| id == "delivery-no-sha"),
            "a failed delivery must not keep a Completed/InFlight marker"
        );
        assert!(
            inner.runs.is_empty(),
            "a failed delivery must not create a run"
        );
    }

    /// Regression guard: a workflow that simply is not triggered by the event
    /// is a *completed* delivery, not a failure. Pushing to a repository whose
    /// workflows all gate on `pull_request` must stay a 200 or GitHub would
    /// redeliver forever.
    #[tokio::test]
    async fn webhook_untriggered_workflow_still_completes_delivery() {
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/build.yml"),
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        )
        .unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;

        assert_eq!(
            fixture.post("delivery-no-match", Some("push")).await,
            StatusCode::OK
        );
        let inner = fixture.state.inner.lock().await;
        assert!(
            inner
                .webhook_deliveries
                .iter()
                .any(|(id, s)| id == "delivery-no-match"
                    && matches!(s, WebhookDeliveryState::Completed(_))),
            "a delivery that triggered no workflow is still processed"
        );
        assert!(inner.runs.is_empty());
    }

    /// A deployment can leave release and artifact workflows to GitHub while
    /// Preloop owns the ordinary CI workflows in the same repository.
    #[tokio::test]
    async fn webhook_skips_github_owned_workflows() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        std::env::set_var(GITHUB_OWNED_WORKFLOWS_ENV, "release.yml");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/release.yml"),
            "on: push\njobs:\n  release:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo release\n",
        )
        .unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;

        assert_eq!(
            fixture.post("delivery-github-owned", Some("push")).await,
            StatusCode::OK
        );
        let inner = fixture.state.inner.lock().await;
        assert!(inner.runs.is_empty());
        assert!(inner.webhook_deliveries.iter().any(|(id, state)| {
            id == "delivery-github-owned" && matches!(state, WebhookDeliveryState::Completed(_))
        }));

        std::env::remove_var(GITHUB_OWNED_WORKFLOWS_ENV);
    }

    /// Issue 2: a delivery whose processing future is cancelled (client
    /// disconnect, server shutdown) or panics must not keep its InFlight
    /// reservation. A stale reservation makes GitHub's redelivery look like an
    /// in-flight duplicate, gets skipped with a 200, and the run is lost.
    #[tokio::test]
    async fn webhook_cancelled_processing_releases_reservation() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;

        let shared = Arc::new(SharedState {
            state: fixture.state.clone(),
            shutdown: CancellationToken::new(),
        });

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-github-delivery",
            HeaderValue::from_static("delivery-cancel"),
        );
        headers.insert("x-github-event", HeaderValue::from_static("push"));
        headers.insert(
            "x-hub-signature-256",
            HeaderValue::from_str(&fixture.signature_header).unwrap(),
        );
        headers.insert("content-type", HeaderValue::from_static("application/json"));

        let future = handle_github_webhook(
            State(shared.clone()),
            headers,
            bytes::Bytes::from(fixture.payload_bytes.clone()),
        );
        let mut future = Box::pin(future);

        // Poll once with a waker that never fires. The handler reserves the
        // delivery and parks inside processing (the workspace snapshot runs
        // real git subprocesses it cannot pass within one poll), so the future
        // cannot complete while parked — exactly a cancelled request.
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        assert!(
            future.as_mut().poll(&mut context).is_pending(),
            "processing must still be in flight after the first poll"
        );
        {
            let inner = fixture.state.inner.lock().await;
            assert!(
                inner.webhook_deliveries.iter().any(|(id, s)| {
                    id == "delivery-cancel" && matches!(s, WebhookDeliveryState::InFlight)
                }),
                "the delivery must be reserved InFlight while processing"
            );
        }

        // Drop the future mid-processing: the reservation must not survive the
        // processing that owned it.
        drop(future);
        {
            let inner = fixture.state.inner.lock().await;
            assert!(
                !inner
                    .webhook_deliveries
                    .iter()
                    .any(|(id, _)| id == "delivery-cancel"),
                "a cancelled delivery must not keep an InFlight reservation"
            );
        }

        // GitHub redelivers after the failed delivery; the retry must be
        // processed, not skipped as an in-flight duplicate.
        assert_eq!(
            fixture.post("delivery-cancel", Some("push")).await,
            StatusCode::OK
        );
        let inner = fixture.state.inner.lock().await;
        assert_eq!(
            inner.runs.len(),
            1,
            "a redelivery after cancellation must be processed"
        );
    }
}
