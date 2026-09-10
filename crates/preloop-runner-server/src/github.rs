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
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use crate::models::{WebhookDeliveryRecord, WebhookDeliveryStatus};
use crate::{
    changed_paths_from_payload, submit_run_inner_with_webhook_delivery, ExecutionStatus,
    SharedState,
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
    if let Some(app_creds) = crate::github_app::select_app_for_repo(shared, repo).await {
        let mut permissions = std::collections::BTreeMap::new();
        permissions.insert("checks".to_owned(), "write".to_owned());
        // The App mint intermittently 422s while the installation grants are
        // being read; a single retry keeps a transient rejection from
        // stranding the check run in `queued` (the fallback JWT cannot
        // PATCH check runs and GitHub keeps showing them pending).
        for attempt in 0..2 {
            match crate::github_app::get_or_mint_token(&app_creds, repo, &permissions).await {
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
    breaker: &crate::github_breaker::GithubBreaker,
    token: &str,
    repo: &str,
    method: reqwest::Method,
    path: &str,
    body: Value,
) -> anyhow::Result<Value> {
    let client = crate::shared_http::CLIENT.clone();
    let url = format!("{}/repos/{}/{}", github_api_base(), repo, path);
    let res = crate::github_breaker::send_observed(
        breaker,
        client
            .request(method, &url)
            .header("User-Agent", "preloop")
            .header("Authorization", format!("Bearer {}", token))
            .header("Accept", "application/vnd.github+json")
            .json(&body),
    )
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
    // Webhook delivery is at-least-once. A replay can find a run whose
    // queued check was already persisted before the worker crashed; PATCH
    // that check instead of POSTing a second one.
    let existing_check_run_id = {
        let inner = shared.state.inner.lock().await;
        inner
            .runs
            .get(&run_id)
            .and_then(|run| run.job_check_run_ids.get(job_id).copied())
    };
    if let Some(check_run_id) = existing_check_run_id {
        report_existing_check_run_queued(shared, repo, job_id, run_id, check_run_id).await;
        return;
    }

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

        match send_github_check_request(
            &shared.state.github_breaker,
            token,
            repo,
            reqwest::Method::POST,
            "check-runs",
            body,
        )
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
            inner.runs.get_mut(&run_id).map(|run| {
                run.job_check_run_ids
                    .insert(job_id.clone(), check_id)
                    .is_none_or(|previous| previous != check_id)
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
        if let Err(error) = send_github_check_request(
            &shared.state.github_breaker,
            token,
            repo,
            reqwest::Method::PATCH,
            &path,
            body,
        )
        .await
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
/// Report a permanent failure check run to GitHub (e.g. invalid workflow YAML, expression failure).
pub(crate) async fn report_check_run_permanent_failure(
    shared: &Arc<SharedState>,
    repo: &str,
    sha: &str,
    name: &str,
    summary: &str,
) {
    let token = resolve_check_run_token(shared, repo).await;
    if let Some(token) = &token {
        let body = serde_json::json!({
            "name": name,
            "head_sha": sha,
            "status": "completed",
            "conclusion": "failure",
            "completed_at": chrono::Utc::now().to_rfc3339(),
            "output": {
                "title": "Workflow evaluation failed",
                "summary": summary,
            }
        });
        if let Err(e) = send_github_check_request(
            &shared.state.github_breaker,
            token,
            repo,
            reqwest::Method::POST,
            "check-runs",
            body,
        )
        .await
        {
            warn!(%repo, %name, error = %e, "Failed to create failed GitHub check run");
        }
    } else {
        info!(%repo, %name, "GitHub token not configured, mock failure check run recorded");
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
        if let Err(e) = send_github_check_request(
            &shared.state.github_breaker,
            token,
            &repo,
            reqwest::Method::PATCH,
            &path,
            body,
        )
        .await
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
        if let Err(e) = send_github_check_request(
            &shared.state.github_breaker,
            token,
            &repo,
            reqwest::Method::PATCH,
            &path,
            body,
        )
        .await
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
        let mut workflows = BTreeMap::new();
        // A dispatch for a branch other than the checked-out tree must read
        // the workflow definitions from that ref, not from the working tree.
        if !git_ref.is_empty() {
            if git_ref.starts_with('-') {
                anyhow::bail!("workflow revision {git_ref:?} is not a valid Git ref");
            }
            let commitish = format!("{git_ref}^{{commit}}");
            let resolved = tokio::process::Command::new("git")
                .arg("-C")
                .arg(base_path)
                .args(["rev-parse", "--verify", &commitish])
                .output()
                .await?;
            if !resolved.status.success() {
                anyhow::bail!(
                    "workflow revision {git_ref:?} is unavailable in local workspace {}: {}",
                    base_path.display(),
                    String::from_utf8_lossy(&resolved.stderr),
                );
            }
            let listing = tokio::process::Command::new("git")
                .arg("-C")
                .arg(base_path)
                .args([
                    "ls-tree",
                    "-r",
                    "--name-only",
                    git_ref,
                    "--",
                    ".github/workflows",
                ])
                .output()
                .await?;
            if !listing.status.success() {
                anyhow::bail!(
                    "git ls-tree for ref {git_ref:?} in {} failed: {}",
                    base_path.display(),
                    String::from_utf8_lossy(&listing.stderr),
                );
            }
            for line in String::from_utf8_lossy(&listing.stdout).lines() {
                let path = line.trim();
                if path.is_empty() {
                    continue;
                }
                let name = path.rsplit('/').next().unwrap_or(path);
                if name.ends_with(".yml") || name.ends_with(".yaml") {
                    let path_ref = format!("{git_ref}:{path}");
                    let content = tokio::process::Command::new("git")
                        .arg("-C")
                        .arg(base_path)
                        .args(["show", &path_ref])
                        .output()
                        .await?;
                    if !content.status.success() {
                        anyhow::bail!(
                            "git show {path_ref:?} in {} failed: {}",
                            base_path.display(),
                            String::from_utf8_lossy(&content.stderr),
                        );
                    }
                    workflows.insert(
                        name.to_owned(),
                        String::from_utf8_lossy(&content.stdout).into_owned(),
                    );
                }
            }
            return Ok(workflows);
        }
        let workflows_dir = base_path.join(".github/workflows");
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
        let token = if let Some(app) = crate::github_app::select_app_for_repo(shared, repo).await {
            let permissions = BTreeMap::from([("contents".to_owned(), "read".to_owned())]);
            Some(crate::github_app::get_or_mint_token_at(api_base, &app, repo, &permissions).await?)
        } else {
            shared.state.static_github_pat()
        };
        if let Some(token) = &token {
            fetch_remote_workflows(&shared.state.github_breaker, token, repo, git_ref, api_base)
                .await
        } else if !git_ref.is_empty() {
            anyhow::bail!(
                "cannot fetch workflow revision {git_ref:?} without a local workspace or GitHub credentials"
            );
        } else {
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
    breaker: &crate::github_breaker::GithubBreaker,
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
    let response = crate::github_breaker::send_observed(
        breaker,
        client
            .get(&url)
            .header("User-Agent", "preloop")
            .header("Authorization", format!("Bearer {}", token))
            .header("Accept", "application/vnd.github+json"),
    )
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
                let file_res = crate::github_breaker::send_observed(
                    breaker,
                    client
                        .get(download_url)
                        .header("User-Agent", "preloop")
                        .header("Authorization", format!("Bearer {}", token)),
                )
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

/// Resolve `git_ref` to a commit SHA, using the same credential ladder as
/// [`fetch_workflows`]: a local workspace is answered by `git rev-parse`,
/// otherwise the PAT or an App-minted `contents: read` installation token
/// queries the GitHub commits API. Returns `Ok(None)` when the ref does not
/// resolve (unknown ref, offline without a workspace) — the caller decides
/// what that means.
pub(crate) async fn resolve_ref_sha(
    shared: &Arc<SharedState>,
    repository: &str,
    git_ref: &str,
) -> anyhow::Result<Option<String>> {
    if let Some(workspace) = &shared.state.local_workspace {
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
    let api_base = github_api_base();
    let token = if let Some(app) = crate::github_app::select_app_for_repo(shared, repository).await
    {
        let permissions = BTreeMap::from([("contents".to_owned(), "read".to_owned())]);
        Some(
            crate::github_app::get_or_mint_token_at(&api_base, &app, repository, &permissions)
                .await?,
        )
    } else {
        std::env::var("PRELOOP_GITHUB_TOKEN").ok()
    };
    let Some(token) = token else {
        return Ok(None);
    };
    let commit_ref = git_ref
        .strip_prefix("refs/heads/")
        .or_else(|| git_ref.strip_prefix("refs/tags/"))
        .unwrap_or(git_ref);
    let response = crate::github_breaker::send_observed(
        &shared.state.github_breaker,
        crate::shared_http::CLIENT
            .clone()
            .get(format!(
                "{api_base}/repos/{repository}/commits/{commit_ref}"
            ))
            .header("User-Agent", "preloop")
            .header("Authorization", format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json"),
    )
    .await?;
    if !response.status().is_success() {
        return Ok(None);
    }
    let commit: Value = response.json().await?;
    Ok(commit.get("sha").and_then(Value::as_str).map(str::to_owned))
}

async fn get_pr_changed_files(
    breaker: &crate::github_breaker::GithubBreaker,
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
        let response = crate::github_breaker::send_observed(
            breaker,
            client
                .get(&url)
                .header("User-Agent", "preloop")
                .header("Authorization", format!("Bearer {}", token))
                .header("Accept", "application/vnd.github+json"),
        )
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
    let token = if let Some(app) = crate::github_app::select_app_for_repo(shared, repo).await {
        let permissions = BTreeMap::from([("pull_requests".to_owned(), "read".to_owned())]);
        Some(crate::github_app::get_or_mint_token_at(api_base, &app, repo, &permissions).await?)
    } else {
        std::env::var("PRELOOP_GITHUB_TOKEN").ok()
    };
    let Some(token) = token else {
        return Ok(None);
    };
    get_pr_changed_files(
        &shared.state.github_breaker,
        &token,
        repo,
        pr_number,
        api_base,
    )
    .await
    .map(Some)
}

const WEBHOOK_ACK_BUDGET: Duration = Duration::from_secs(8);
const WEBHOOK_ENQUEUE_ATTEMPTS: usize = 2;
const WEBHOOK_LEASE_DURATION_SECS: u64 = 60;
const WEBHOOK_LEASE_RENEW_INTERVAL_SECS: u64 = 20;
const WEBHOOK_DELIVERY_RETENTION_SECS: i64 = 30 * 24 * 60 * 60;
const WEBHOOK_MAX_ATTEMPTS: u32 = 6;
const WEBHOOK_PRUNE_INTERVAL: Duration = Duration::from_secs(5 * 60);
const WEBHOOK_PRUNE_BATCH_SIZE: usize = 256;
/// How stale the published queue counters may get while the worker has nothing
/// to do. The worker refreshes them whenever it moves or prunes a row; this
/// only covers changes it did not make — another engine on a shared store, or
/// an operator replay through the native API.
const WEBHOOK_STATS_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Backoff ladder for transient webhook delivery failures, indexed by the
/// delivery's attempt count (the final entry repeats).
///
/// The two cheap leading retries cover what this path actually sees: a
/// workflow file momentarily unreadable, or a snapshot not yet visible. The
/// rest stretches so a broken dependency is not hammered, and the attempt cap
/// dead-letters a delivery long before the ladder could become a hot loop.
pub(crate) const WEBHOOK_RETRY_BACKOFF: &[Duration] = &[
    Duration::from_secs(1),
    Duration::from_secs(1),
    Duration::from_secs(5),
    Duration::from_secs(15),
    Duration::from_secs(30),
];

/// Delay before the next attempt at a delivery that has now failed `attempts`
/// times.
///
/// Takes the ladder as an argument rather than reading the constant so the
/// retry path is driven by state: tests shorten it instead of sleeping through
/// real tiers. An empty ladder falls back to the first tier, never to zero —
/// "retry immediately" is the one answer this must not invent.
fn webhook_retry_backoff(ladder: &[Duration], attempts: u32) -> Duration {
    ladder
        .get(attempts as usize)
        .or_else(|| ladder.last())
        .copied()
        .unwrap_or_else(|| WEBHOOK_RETRY_BACKOFF[0])
}

/// Publish the queue counters the operational snapshot reads.
///
/// The sampler used to query the store on every tick to report these. The
/// queue worker both mutates the queue and already talks to the store, so it
/// hands over what it read and the snapshot reads memory instead of taking the
/// store's single connection for a query that usually reports "unchanged".
pub(crate) async fn refresh_webhook_queue_stats(state: &crate::state::AppState) -> bool {
    match state.store.webhook_queue_stats().await {
        Ok(stats) => {
            state.webhook_status.set_queue_stats(stats);
            true
        }
        Err(error) => {
            debug!(?error, "failed to refresh webhook queue counters");
            false
        }
    }
}

/// Commit the delivery row within the webhook acknowledgement budget.
///
/// The 202 response is the durable boundary. GitHub does not automatically
/// redeliver a delivery after a non-2xx response, so retry quick store errors
/// here, but never wait indefinitely behind a contended store connection.
async fn enqueue_webhook_delivery_with_budget(
    shared: &Arc<SharedState>,
    delivery: &WebhookDeliveryRecord,
) -> anyhow::Result<bool> {
    let deadline = Instant::now() + WEBHOOK_ACK_BUDGET;
    let mut last_error = None;

    for attempt in 0..WEBHOOK_ENQUEUE_ATTEMPTS {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let attempt_budget = remaining.min(Duration::from_secs(4));
        match tokio::time::timeout(
            attempt_budget,
            shared.state.store.enqueue_webhook_delivery(delivery),
        )
        .await
        {
            Ok(Ok(inserted)) => return Ok(inserted),
            Ok(Err(error)) => {
                last_error = Some(error.to_string());
                if attempt + 1 < WEBHOOK_ENQUEUE_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "timed out persisting webhook delivery after {}s",
                    WEBHOOK_ACK_BUDGET.as_secs()
                ));
            }
        }
    }

    Err(anyhow::anyhow!(
        "failed to persist webhook delivery after {WEBHOOK_ENQUEUE_ATTEMPTS} attempts: {}",
        last_error.unwrap_or_else(|| "acknowledgement budget exhausted".to_owned())
    ))
}

/// Route handler for GitHub App Webhooks.
///
/// Verifies the signature, atomically enqueues the delivery to the durable
/// store, and acknowledges with HTTP 202 Accepted. Background workers drain
/// the queue asynchronously.
pub(crate) async fn handle_github_webhook(
    State(shared): State<Arc<SharedState>>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    // 1. Verify Signature
    let sig_header = headers
        .get("x-hub-signature-256")
        .and_then(|h| h.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    // Every registered App's secret is a candidate (D6): a payload signed by
    // any App preloop fronts is accepted, one signed by none is rejected.
    let secrets: Vec<String> = match &shared.state.github_apps {
        Some(apps) => apps.webhook_secrets(shared.state.webhook_secret.as_deref()),
        None => shared.state.webhook_secret.clone().into_iter().collect(),
    };
    if secrets.is_empty() {
        warn!("No webhook secret is configured on the server, rejecting request");
        return Err(StatusCode::UNAUTHORIZED);
    }
    if !secrets
        .iter()
        .any(|secret| verify_signature(secret, &body, sig_header))
    {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let event_name = headers
        .get("x-github-event")
        .and_then(|h| h.to_str().ok())
        .ok_or(StatusCode::BAD_REQUEST)?;

    let delivery_id = headers
        .get("x-github-delivery")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    let record = WebhookDeliveryRecord {
        delivery_id: delivery_id.clone(),
        event: event_name.to_owned(),
        payload: body.to_vec(),
        received_at_us: crate::store::now_us(),
        state: WebhookDeliveryStatus::Received,
        attempts: 0,
        lease_until_us: None,
        lease_token: None,
        last_error: None,
    };

    // A failed durable commit MUST NOT be acknowledged. GitHub does not
    // automatically redeliver non-2xx responses, so the delivery remains
    // available for an operator/API redelivery after the store recovers.
    match enqueue_webhook_delivery_with_budget(&shared, &record).await {
        Ok(true) => {
            info!(delivery = %delivery_id, event = %event_name, "GitHub webhook delivery queued");
            // Keep one permit when the worker is between drains; `notify_waiters`
            // can lose this wakeup and defer processing to the polling tick.
            shared.state.webhook_queue_notify.notify_one();
            Ok((
                StatusCode::ACCEPTED,
                Json(serde_json::json!({ "delivery_id": delivery_id, "status": "accepted" })),
            ))
        }
        Ok(false) => {
            info!(
                delivery = %delivery_id,
                event = %event_name,
                "Duplicate GitHub webhook delivery — already enqueued"
            );
            Ok((
                StatusCode::ACCEPTED,
                Json(serde_json::json!({ "delivery_id": delivery_id, "status": "duplicate" })),
            ))
        }
        Err(error) => {
            error!(
                delivery = %delivery_id,
                ?error,
                "Failed to commit webhook delivery row — returning 500; redelivery is manual"
            );
            Err(StatusCode::INTERNAL_SERVER_ERROR)
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

/// Outcome of processing a webhook delivery payload.
#[derive(Debug)]
enum WebhookOutcome {
    Success,
    TransientError(String),
    PermanentErrors {
        repo: String,
        failures: Vec<WebhookFailure>,
    },
    Unreportable(String),
    /// GitHub itself is unreachable. Distinct from `TransientError` because
    /// the delivery did nothing wrong: it is returned to the queue with its
    /// attempt refunded and retried after the breaker's window, instead of
    /// spending one of six attempts on an outage it cannot influence.
    DependencyUnavailable {
        error: String,
        retry_after_secs: u64,
    },
}
#[derive(Debug)]
struct WebhookFailure {
    sha: Option<String>,
    check_name: String,
    error: String,
}

/// Renew a claimed delivery while its workflow evaluation is in flight.
///
/// A worker may spend longer than the initial lease fetching workflow files or
/// creating checks. Without renewal, another worker can reclaim the same row
/// and submit duplicate runs before the first worker finishes. The fencing
/// token prevents a stale worker from renewing the replacement lease.
async fn run_webhook_lease_heartbeat(
    shared: Arc<SharedState>,
    delivery_id: String,
    lease_token: String,
    lease_until_us: i64,
    lease_lost: tokio_util::sync::CancellationToken,
) {
    let mut interval =
        tokio::time::interval(Duration::from_secs(WEBHOOK_LEASE_RENEW_INTERVAL_SECS));
    let remaining_us = lease_until_us.saturating_sub(crate::store::now_us()).max(0) as u64;
    let mut lease_deadline = Instant::now() + Duration::from_micros(remaining_us);
    let mut deadline = Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
        lease_deadline,
    )));
    interval.tick().await;
    loop {
        tokio::select! {
            _ = shared.shutdown.cancelled() => {
                lease_lost.cancel();
                return;
            }
            _ = &mut deadline => {
                warn!(
                    delivery = %delivery_id,
                    "webhook delivery lease expired while renewal was unavailable"
                );
                lease_lost.cancel();
                return;
            }
            _ = interval.tick() => {
                let renewal_timeout = lease_deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_secs(WEBHOOK_LEASE_RENEW_INTERVAL_SECS));
                if renewal_timeout.is_zero() {
                    lease_lost.cancel();
                    return;
                }
                let renewal = tokio::time::timeout(
                    renewal_timeout,
                    shared
                        .state
                        .store
                        .renew_webhook_delivery(
                            &delivery_id,
                            &lease_token,
                            WEBHOOK_LEASE_DURATION_SECS,
                        ),
                )
                .await;
                match renewal {
                    Ok(Ok(true)) => {
                        debug!(delivery = %delivery_id, "renewed webhook delivery lease");
                        lease_deadline =
                            Instant::now() + Duration::from_secs(WEBHOOK_LEASE_DURATION_SECS);
                        deadline
                            .as_mut()
                            .reset(tokio::time::Instant::from_std(lease_deadline));
                    }
                    Ok(Ok(false)) => {
                        warn!(
                            delivery = %delivery_id,
                            "webhook delivery lease was fenced while processing"
                        );
                        lease_lost.cancel();
                        return;
                    }
                    Ok(Err(error)) => {
                        warn!(
                            delivery = %delivery_id,
                            ?error,
                            "failed to renew webhook delivery lease"
                        );
                        if Instant::now() >= lease_deadline {
                            lease_lost.cancel();
                            return;
                        }
                    }
                    Err(_) => {
                        warn!(
                            delivery = %delivery_id,
                            "timed out renewing webhook delivery lease"
                        );
                        if Instant::now() >= lease_deadline {
                            lease_lost.cancel();
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Background task that continuously drains the durable webhook queue.
pub(crate) async fn run_webhook_queue_worker(
    shared: Arc<SharedState>,
    heartbeat: preloop_observability::HeartbeatHandle,
) {
    // Registering happens before the task is spawned so a panic during
    // startup remains visible to readiness checks. Beat before recovery and
    // again on every loop; a stuck queue worker must not look healthy.
    heartbeat.beat();
    // Crash recovery: on boot, reset processing rows whose lease has expired back to received.
    if let Err(error) = shared.state.store.recover_webhook_deliveries().await {
        warn!(
            ?error,
            "failed to recover stale webhook deliveries on startup"
        );
    }
    let mut last_prune = Instant::now();
    let mut last_stats_refresh = Instant::now();
    // Publish once before the first drain: the boot snapshot is built while
    // this task is still starting, and every later snapshot reads the cache.
    refresh_webhook_queue_stats(&shared.state).await;

    loop {
        heartbeat.beat();
        if shared.shutdown.is_cancelled() {
            break;
        }
        let processed = match drain_webhook_queue_with_heartbeat(&shared, &heartbeat).await {
            Ok(processed) => processed,
            Err(error) => {
                if !shared.shutdown.is_cancelled() {
                    warn!(?error, "error draining webhook delivery queue");
                }
                0
            }
        };
        let mut pruned = 0u64;
        if last_prune.elapsed() >= WEBHOOK_PRUNE_INTERVAL {
            last_prune = Instant::now();
            let cutoff = crate::store::now_us()
                .saturating_sub(WEBHOOK_DELIVERY_RETENTION_SECS.saturating_mul(1_000_000));
            match shared
                .state
                .store
                .prune_webhook_deliveries(cutoff, WEBHOOK_PRUNE_BATCH_SIZE)
                .await
            {
                Ok(count) => {
                    if count > 0 {
                        info!(pruned = count, "pruned terminal webhook deliveries");
                    }
                    pruned = count;
                }
                Err(error) => warn!(?error, "failed to prune terminal webhook deliveries"),
            }
        }
        // Every row this worker moved or pruned changed the counters, so the
        // cache is refreshed then. The interval is the fallback for changes
        // made elsewhere (a second engine on a shared store, an operator
        // replay), and keeps a stuck queue visible instead of frozen at the
        // last busy moment.
        let counters_changed = processed > 0 || pruned > 0;
        let refresh_due = last_stats_refresh.elapsed() >= WEBHOOK_STATS_REFRESH_INTERVAL;
        if (counters_changed || refresh_due) && refresh_webhook_queue_stats(&shared.state).await {
            last_stats_refresh = Instant::now();
        }
        tokio::select! {
            _ = shared.shutdown.cancelled() => break,
            _ = shared.state.webhook_queue_notify.notified() => {},
            _ = tokio::time::sleep(Duration::from_secs(5)) => {},
        }
    }
}

/// Drain pending webhook deliveries in FIFO order by `received_at_us`.
pub(crate) async fn drain_webhook_queue(shared: &Arc<SharedState>) -> anyhow::Result<usize> {
    // Claim one row at a time so every processing lease is heartbeated; a
    // claimed batch could let later rows expire while earlier ones run.
    const BATCH_SIZE: usize = 1;

    let mut total_processed = 0;
    loop {
        // A known GitHub outage means every claim here would charge an
        // attempt and then fail on the same dependency. Not claiming is the
        // difference between riding out an incident and dead-lettering every
        // push that arrived during it.
        if let Some(retry_after) = shared.state.github_breaker.retry_after() {
            debug!(
                retry_in_secs = retry_after.as_secs(),
                "GitHub breaker open; deferring webhook queue drain"
            );
            break;
        }
        let deliveries = shared
            .state
            .store
            .claim_webhook_deliveries(BATCH_SIZE, WEBHOOK_LEASE_DURATION_SECS)
            .await?;
        if deliveries.is_empty() {
            break;
        }
        for delivery in &deliveries {
            process_one_delivery(shared, delivery).await;
            total_processed += 1;
        }
        if deliveries.len() < BATCH_SIZE {
            break;
        }
    }
    Ok(total_processed)
}

async fn drain_webhook_queue_with_heartbeat(
    shared: &Arc<SharedState>,
    heartbeat: &preloop_observability::HeartbeatHandle,
) -> anyhow::Result<usize> {
    let mut drain = Box::pin(drain_webhook_queue(shared));
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            result = &mut drain => return result,
            _ = ticker.tick() => heartbeat.beat(),
            _ = shared.shutdown.cancelled() => return Ok(0),
        }
    }
}

/// Process one claimed delivery and update its state according to the failure taxonomy.
pub(crate) async fn process_one_delivery(
    shared: &Arc<SharedState>,
    delivery: &WebhookDeliveryRecord,
) {
    let Some(lease_token) = delivery.lease_token.as_deref() else {
        error!(
            delivery_id = %delivery.delivery_id,
            "claimed webhook delivery has no lease token"
        );
        return;
    };
    let Some(lease_until_us) = delivery.lease_until_us else {
        error!(
            delivery_id = %delivery.delivery_id,
            "claimed webhook delivery has no lease expiry"
        );
        return;
    };
    let lease_lost = tokio_util::sync::CancellationToken::new();
    let heartbeat = tokio::spawn(run_webhook_lease_heartbeat(
        shared.clone(),
        delivery.delivery_id.clone(),
        lease_token.to_owned(),
        lease_until_us,
        lease_lost.clone(),
    ));
    // Isolate payload processing so a panic cannot strand the heartbeat task
    // or leave the queue row in `processing` forever. A fenced or expired
    // lease cancels this task before it can report external side effects.
    let processing_shared = shared.clone();
    let processing_delivery = delivery.clone();
    let processing_lease_lost = lease_lost.clone();
    let mut processing = tokio::spawn(async move {
        process_delivery_payload_with_lease(
            &processing_shared,
            &processing_delivery,
            &processing_lease_lost,
        )
        .await
    });
    let outcome = tokio::select! {
        _ = lease_lost.cancelled() => {
            processing.abort();
            let _ = processing.await;
            None
        }
        result = &mut processing => Some(match result {
            Ok(outcome) => outcome,
            Err(error) => {
                WebhookOutcome::Unreportable(format!("webhook processing task failed: {error}"))
            }
        }),
    };
    heartbeat.abort();
    let _ = heartbeat.await;
    let Some(outcome) = outcome else {
        warn!(
            delivery_id = %delivery.delivery_id,
            "abandoned webhook processing after losing its lease"
        );
        return;
    };
    // The payload may have completed at the same instant the heartbeat
    // observed fencing. Do not report checks or mutate the queue row after
    // that point; the replacement worker owns the retry.
    if lease_lost.is_cancelled() {
        warn!(
            delivery_id = %delivery.delivery_id,
            "discarding webhook outcome after losing its lease"
        );
        return;
    }
    // Outage reclassification happens BEFORE the attempt cap: a delivery
    // must never be dead-lettered for a dependency that was down. The
    // breaker, not the delivery, is the authority on whether GitHub is the
    // reason a step failed.
    let outcome = match outcome {
        WebhookOutcome::TransientError(error) => {
            match shared.state.github_breaker.retry_after() {
                Some(retry_after) => WebhookOutcome::DependencyUnavailable {
                    error,
                    // Bounded: a long breaker window still gets re-examined
                    // periodically, and a short one does not become a spin.
                    retry_after_secs: retry_after.as_secs().clamp(5, 300),
                },
                None if delivery.attempts >= WEBHOOK_MAX_ATTEMPTS => {
                    WebhookOutcome::Unreportable(format!(
                        "transient webhook failure exceeded {WEBHOOK_MAX_ATTEMPTS} attempts: {error}"
                    ))
                }
                None => WebhookOutcome::TransientError(error),
            }
        }
        outcome => outcome,
    };
    match outcome {
        WebhookOutcome::Success => {
            match shared
                .state
                .store
                .complete_webhook_delivery(&delivery.delivery_id, lease_token)
                .await
            {
                Ok(true) => {}
                Ok(false) => warn!(
                    delivery_id = %delivery.delivery_id,
                    "lost webhook delivery lease before marking completed"
                ),
                Err(error) => warn!(
                    delivery_id = %delivery.delivery_id,
                    ?error,
                    "failed to mark webhook delivery completed"
                ),
            }
        }
        WebhookOutcome::DependencyUnavailable {
            error,
            retry_after_secs,
        } => {
            warn!(
                delivery_id = %delivery.delivery_id,
                attempts = delivery.attempts,
                retry_after_secs,
                error = %error,
                "GitHub is unavailable; parking webhook delivery without spending an attempt"
            );
            match shared
                .state
                .store
                .park_webhook_delivery(&delivery.delivery_id, lease_token, &error, retry_after_secs)
                .await
            {
                Ok(true) => {}
                Ok(false) => warn!(
                    delivery_id = %delivery.delivery_id,
                    "lost webhook delivery lease before parking it"
                ),
                Err(error) => warn!(
                    delivery_id = %delivery.delivery_id,
                    ?error,
                    "failed to park webhook delivery for a GitHub outage"
                ),
            }
        }
        WebhookOutcome::TransientError(err) => {
            let backoff =
                webhook_retry_backoff(&shared.state.webhook_retry_backoff, delivery.attempts);
            warn!(
                delivery_id = %delivery.delivery_id,
                attempts = delivery.attempts,
                backoff_ms = backoff.as_millis() as u64,
                error = %err,
                "transient error processing webhook delivery; retrying internally"
            );
            match shared
                .state
                .store
                .fail_webhook_delivery(
                    &delivery.delivery_id,
                    lease_token,
                    &err,
                    false,
                    Some(backoff),
                )
                .await
            {
                Ok(true) => {}
                Ok(false) => warn!(
                    delivery_id = %delivery.delivery_id,
                    "lost webhook delivery lease before scheduling retry"
                ),
                Err(error) => warn!(
                    delivery_id = %delivery.delivery_id,
                    ?error,
                    "failed to update webhook delivery for retry"
                ),
            }
        }
        WebhookOutcome::PermanentErrors { repo, failures } => {
            let error = failures
                .iter()
                .map(|failure| format!("{}: {}", failure.check_name, failure.error))
                .collect::<Vec<_>>()
                .join("; ");
            error!(
                delivery_id = %delivery.delivery_id,
                %repo,
                error = %error,
                failures = failures.len(),
                "permanent errors processing webhook delivery; reporting failure check runs"
            );
            for failure in &failures {
                if lease_lost.is_cancelled() {
                    warn!(
                        delivery_id = %delivery.delivery_id,
                        "stopped reporting webhook failures after losing lease"
                    );
                    return;
                }
                if let Some(sha) = &failure.sha {
                    tokio::select! {
                        _ = lease_lost.cancelled() => return,
                        _ = report_check_run_permanent_failure(
                            shared,
                            &repo,
                            sha,
                            &failure.check_name,
                            &failure.error,
                        ) => {}
                    }
                }
            }
            if lease_lost.is_cancelled() {
                return;
            }
            match shared
                .state
                .store
                .fail_webhook_delivery(&delivery.delivery_id, lease_token, &error, true, None)
                .await
            {
                Ok(true) => {}
                Ok(false) => warn!(
                    delivery_id = %delivery.delivery_id,
                    "lost webhook delivery lease before marking permanently failed"
                ),
                Err(error) => warn!(
                    delivery_id = %delivery.delivery_id,
                    ?error,
                    "failed to mark webhook delivery permanently failed"
                ),
            }
        }
        WebhookOutcome::Unreportable(err) => {
            error!(
                delivery_id = %delivery.delivery_id,
                error = %err,
                "unreportable error processing webhook delivery; dead-lettering row"
            );
            match shared
                .state
                .store
                .fail_webhook_delivery(&delivery.delivery_id, lease_token, &err, true, None)
                .await
            {
                Ok(true) => {}
                Ok(false) => warn!(
                    delivery_id = %delivery.delivery_id,
                    "lost webhook delivery lease before dead-lettering row"
                ),
                Err(error) => warn!(
                    delivery_id = %delivery.delivery_id,
                    ?error,
                    "failed to dead-letter webhook delivery row"
                ),
            }
        }
    }
}

async fn process_delivery_payload(
    shared: &Arc<SharedState>,
    delivery: &WebhookDeliveryRecord,
) -> WebhookOutcome {
    let lease_lost = tokio_util::sync::CancellationToken::new();
    process_delivery_payload_with_lease(shared, delivery, &lease_lost).await
}

async fn process_delivery_payload_with_lease(
    shared: &Arc<SharedState>,
    delivery: &WebhookDeliveryRecord,
    lease_lost: &tokio_util::sync::CancellationToken,
) -> WebhookOutcome {
    let payload_val: Value = match serde_json::from_slice(&delivery.payload) {
        Ok(v) => v,
        Err(e) => {
            return WebhookOutcome::Unreportable(format!("malformed JSON payload: {e}"));
        }
    };

    if lease_lost.is_cancelled() {
        return WebhookOutcome::Success;
    }

    if delivery.event == "check_run" {
        let rerequest = tokio::select! {
            _ = lease_lost.cancelled() => return WebhookOutcome::Success,
            result = process_check_run_rerequest(shared, &payload_val) => result,
        };
        match rerequest {
            Ok(_) => return WebhookOutcome::Success,
            Err(status) if status.is_server_error() => {
                return WebhookOutcome::TransientError(format!(
                    "check run rerequest failed with status {status}"
                ));
            }
            Err(status) => {
                info!(%status, "check run rerequest ignored");
                return WebhookOutcome::Success;
            }
        }
    }

    let adapter = match crate::events::adapter_for(&delivery.event) {
        Some(a) => a,
        None => {
            info!("No adapter for event: {}", delivery.event);
            return WebhookOutcome::Success;
        }
    };

    let effective_events = adapter.project(&payload_val);
    if effective_events.is_empty() {
        info!(
            "Event {} produced no effective events (e.g. [skip ci] or fork-gated)",
            delivery.event
        );
        return WebhookOutcome::Success;
    }

    let repo_full_name = match payload_val
        .get("repository")
        .and_then(|r| r.get("full_name"))
        .and_then(|v| v.as_str())
    {
        Some(name) => name.to_owned(),
        None => {
            return WebhookOutcome::Unreportable(
                "webhook payload missing repository.full_name".to_owned(),
            );
        }
    };

    let (changed_paths, changed_paths_known) = if matches!(
        delivery.event.as_str(),
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
            Some(number) => {
                match resolve_pr_changed_files_at(shared, &repo_full_name, number, &api_base).await
                {
                    Ok(files) => files,
                    Err(error) => {
                        error!(?error, "failed to resolve pull request changed files");
                        return WebhookOutcome::TransientError(format!(
                            "failed to resolve pull request changed files: {error:?}"
                        ));
                    }
                }
            }
            None => None,
        };
        match fetched {
            Some(files) => (files, true),
            None => (
                changed_paths_from_payload(&payload_val),
                payload_val.get("paths").is_some() || payload_val.get("commits").is_some(),
            ),
        }
    } else {
        (changed_paths_from_payload(&payload_val), true)
    };

    let github_owned_workflows = configured_github_owned_workflows();
    let mut unmatched_workflows: Vec<String> = Vec::new();
    let mut triggered_count = 0usize;
    let mut permanent_failures: Vec<WebhookFailure> = Vec::new();
    let mut permanent_failure_keys: BTreeSet<(Option<String>, String)> = BTreeSet::new();

    for effective in &effective_events {
        if lease_lost.is_cancelled() {
            return WebhookOutcome::Success;
        }
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

        let workflow_ref = if effective.event == "workflow_run" {
            &ref_default
        } else {
            &effective.git_ref
        };

        let resolved_sha = match &effective.sha {
            Some(sha) => sha.clone(),
            None if effective.event == "pull_request_target" => {
                error!(
                    event = %effective.event,
                    ref_name = %workflow_ref,
                    "pull_request_target has no base commit SHA"
                );
                return WebhookOutcome::TransientError(
                    "pull_request_target has no base commit SHA".to_owned(),
                );
            }
            None => match resolve_ref_sha(shared, &repo_full_name, workflow_ref).await {
                Ok(Some(sha)) => sha,
                Ok(None) => {
                    error!(
                        ref_name = %workflow_ref,
                        "webhook workflow ref has no resolvable commit SHA"
                    );
                    return WebhookOutcome::TransientError(
                        "webhook workflow ref has no resolvable commit SHA".to_owned(),
                    );
                }
                Err(error) => {
                    error!(
                        ?error,
                        ref_name = %workflow_ref,
                        "failed to resolve webhook workflow ref SHA"
                    );
                    return WebhookOutcome::TransientError(format!(
                        "failed to resolve webhook workflow ref SHA: {error:?}"
                    ));
                }
            },
        };

        let workflows = match fetch_workflows(shared, &repo_full_name, &resolved_sha).await {
            Ok(w) => w,
            Err(error) => {
                error!(
                    event = %effective.event,
                    sha = %resolved_sha,
                    source_ref = %workflow_ref,
                    ?error,
                    "Failed to fetch workflows at the event commit"
                );
                return WebhookOutcome::TransientError(format!(
                    "Failed to fetch workflows at event commit: {error:?}"
                ));
            }
        };

        if effective.event == "push" && effective.git_ref == ref_default {
            if let Some(scheduler) = &shared.state.scheduler {
                let scheduler_source = match resolve_ref_sha(shared, &repo_full_name, &ref_default)
                    .await
                {
                    Ok(Some(scheduler_sha)) => {
                        let scheduler_workflows = if scheduler_sha == resolved_sha {
                            Some(workflows.clone())
                        } else {
                            match fetch_workflows(shared, &repo_full_name, &scheduler_sha).await {
                                Ok(workflows) => Some(workflows),
                                Err(error) => {
                                    warn!(
                                        sha = %scheduler_sha,
                                        ?error,
                                        "failed to fetch current default-branch workflows — skipping cron reconciliation"
                                    );
                                    None
                                }
                            }
                        };
                        scheduler_workflows.map(|workflows| (scheduler_sha, workflows))
                    }
                    Ok(None) => {
                        warn!(
                            ref_name = %ref_default,
                            "current default branch has no resolvable commit SHA — skipping cron reconciliation"
                        );
                        None
                    }
                    Err(error) => {
                        warn!(
                            ?error,
                            ref_name = %ref_default,
                            "failed to resolve current default branch SHA — skipping cron reconciliation"
                        );
                        None
                    }
                };
                if let Some((scheduler_sha, scheduler_workflows)) = scheduler_source {
                    let mut scheduler_payload = payload_val.clone();
                    if let Some(object) = scheduler_payload.as_object_mut() {
                        object.insert("after".to_owned(), Value::String(scheduler_sha));
                    }
                    scheduler
                        .reconcile_all(&scheduler_workflows, scheduler_payload, shared.clone())
                        .await;
                }
            }
        }

        for (filename, content) in workflows {
            if lease_lost.is_cancelled() {
                return WebhookOutcome::Success;
            }
            if is_github_owned_workflow(&filename, &github_owned_workflows) {
                info!(
                    workflow = %filename,
                    event = %effective.event,
                    "Skipping workflow owned by GitHub"
                );
                continue;
            }

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
                    let error_msg = format!("Failed to parse workflow file {filename}: {e:?}");
                    warn!("{error_msg}");
                    let sha = effective
                        .status_check_sha
                        .clone()
                        .or_else(|| Some(resolved_sha.clone()));
                    if permanent_failure_keys.insert((sha.clone(), filename.clone())) {
                        permanent_failures.push(WebhookFailure {
                            sha,
                            check_name: filename.clone(),
                            error: error_msg,
                        });
                    }
                    // A malformed file must not prevent other workflows at
                    // the same commit from being evaluated and queued.
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

            let submission_result = tokio::select! {
                _ = lease_lost.cancelled() => return WebhookOutcome::Success,
                result = submit_run_inner_with_webhook_delivery(
                    shared,
                    submission,
                    Some(&delivery.delivery_id),
                ) => result,
            };
            match submission_result {
                Ok(accepted) => {
                    if lease_lost.is_cancelled() {
                        return WebhookOutcome::Success;
                    }
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
                            tokio::select! {
                                _ = lease_lost.cancelled() => return WebhookOutcome::Success,
                                _ = report_check_run_queued(
                                    shared,
                                    &repo_full_name,
                                    &sha,
                                    &job_id,
                                    run_id,
                                ) => {}
                            }
                            let status = tokio::select! {
                                _ = lease_lost.cancelled() => return WebhookOutcome::Success,
                                status = async {
                                    let inner = shared.state.inner.lock().await;
                                    inner
                                        .runs
                                        .get(&run_id)
                                        .and_then(|r| r.jobs.get(&job_id).copied())
                                } => status,
                            };
                            if let Some(status) = status.filter(|s| s.is_terminal()) {
                                tokio::select! {
                                    _ = lease_lost.cancelled() => return WebhookOutcome::Success,
                                    _ = report_check_run_completed(shared, run_id, &job_id, status) => {}
                                }
                            }
                        }
                    }
                    triggered_count += 1;
                }
                Err(e) => {
                    let detail = format!("{e:?}");
                    let status = e.into_response().status();
                    if status.is_client_error() {
                        debug!(
                            workflow = %filename,
                            event = %effective.event,
                            "workflow not triggered by this event ({detail}) — not a delivery failure"
                        );
                        unmatched_workflows.push(filename.clone());
                        continue;
                    }
                    error!("Failed to submit run for {filename}: {detail}");
                    return WebhookOutcome::TransientError(format!(
                        "Failed to submit run for {filename}: {detail}"
                    ));
                }
            }
        }
    }
    if !unmatched_workflows.is_empty() {
        info!(
            event = %delivery.event,
            unmatched = unmatched_workflows.len(),
            triggered = triggered_count,
            workflows = %unmatched_workflows.join(", "),
            "workflows evaluated but not triggered by this event"
        );
    }
    if !permanent_failures.is_empty() {
        return WebhookOutcome::PermanentErrors {
            repo: repo_full_name,
            failures: permanent_failures,
        };
    }

    WebhookOutcome::Success
}

/// Webhook events the App-manifest flow asks GitHub to subscribe a new App to.
///
/// Defaults to the minimal CI event set (`push`, `pull_request`). GitHub
/// cannot change an App's event subscriptions through its API after creation,
/// so operators who need additional triggers must add them manually in the
/// App settings UI. Operators who want a different creation-time set can
/// override it with `PRELOOP_GITHUB_APP_DEFAULT_EVENTS` (comma-separated).
pub fn manifest_default_events() -> Vec<String> {
    if let Some(raw) = std::env::var("PRELOOP_GITHUB_APP_DEFAULT_EVENTS")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        let events: Vec<String> = raw
            .split(',')
            .map(str::trim)
            .filter(|event| !event.is_empty())
            .map(str::to_owned)
            .collect();
        if !events.is_empty() {
            return events;
        }
    }
    vec!["push".to_owned(), "pull_request".to_owned()]
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
        manifest_json["default_events"] = serde_json::json!(manifest_default_events());
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
    use axum::http::{Method, Request};
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

    fn git_output(workspace: &std::path::Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(workspace)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    fn commit_workspace(workspace: &std::path::Path) -> String {
        for args in [
            &["init", "-q", "-b", "main"][..],
            &["config", "user.email", "github-tests@example.invalid"][..],
            &["config", "user.name", "GitHub Tests"][..],
        ] {
            git_output(workspace, args);
        }
        git_output(workspace, &["add", "-A"]);
        git_output(workspace, &["commit", "-qm", "test workflow"]);
        git_output(workspace, &["rev-parse", "HEAD"])
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

    #[tokio::test]
    async fn manifest_default_events_use_minimal_ci_defaults() {
        // Held for the whole test: `PRELOOP_GITHUB_APP_DEFAULT_EVENTS` is
        // process-global and other tests build apps concurrently; the
        // fallback must be asserted with the override absent.
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        std::env::remove_var("PRELOOP_GITHUB_APP_DEFAULT_EVENTS");
        let defaults = manifest_default_events();
        assert_eq!(defaults, vec!["push", "pull_request"]);
    }

    #[tokio::test]
    async fn manifest_default_events_override_is_used_when_set() {
        // Held for the whole test: `PRELOOP_GITHUB_APP_DEFAULT_EVENTS` is
        // process-global and other tests build apps concurrently.
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        std::env::set_var("PRELOOP_GITHUB_APP_DEFAULT_EVENTS", "push, pull_request");
        let events = manifest_default_events();
        assert_eq!(events, vec!["push".to_owned(), "pull_request".to_owned()]);
        std::env::remove_var("PRELOOP_GITHUB_APP_DEFAULT_EVENTS");

        // A blank override falls back to the minimal default.
        std::env::set_var("PRELOOP_GITHUB_APP_DEFAULT_EVENTS", "  ");
        assert_eq!(manifest_default_events(), vec!["push", "pull_request"]);
        std::env::remove_var("PRELOOP_GITHUB_APP_DEFAULT_EVENTS");
    }

    /// The signed push payload GitHub would deliver for `owner/repo`.
    fn signed_push_payload(after: &str) -> (Vec<u8>, String) {
        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "before": "0000000000000000000000000000000000000000",
            "after": after,
            "repository": {"full_name": "owner/repo", "default_branch": "main"},
            "commits": [{
                "id": after,
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
        workspace: std::path::PathBuf,
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
            let event_sha = if ws_dir.join(".github/workflows").is_dir() {
                commit_workspace(&ws_dir)
            } else {
                "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_owned()
            };
            let mut state = AppState::new(temp.path().join("state").to_path_buf())
                .await
                .unwrap();
            state.webhook_secret = Some("super-secret".to_owned());
            state.local_workspace = Some(ws_dir.clone());
            let app =
                crate::app_with_test_api(state.clone(), CancellationToken::new(), "test-token");
            let (payload_bytes, signature_header) = signed_push_payload(&event_sha);
            Self {
                state,
                app,
                workspace: ws_dir,
                payload_bytes,
                signature_header,
            }
        }

        fn refresh_payload_from_head(&mut self) {
            let event_sha = git_output(&self.workspace, &["rev-parse", "HEAD"]);
            let (payload_bytes, signature_header) = signed_push_payload(&event_sha);
            self.payload_bytes = payload_bytes;
            self.signature_header = signature_header;
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
        async fn drain(&self) -> usize {
            let shared = Arc::new(SharedState {
                state: self.state.clone(),
                shutdown: CancellationToken::new(),
            });
            drain_webhook_queue(&shared).await.unwrap()
        }

        /// Replace the retry ladder for this fixture.
        ///
        /// The real ladder opens at one second, so a test that exercises a
        /// retry would otherwise have to sleep through it. The delay still has
        /// to be non-zero: a zero-delay retry is immediately claimable, and
        /// one drain pass would burn the whole attempt budget in a hot loop.
        fn with_retry_backoff(mut self, ladder: Vec<Duration>) -> Self {
            self.state.webhook_retry_backoff = ladder;
            self
        }
    }

    /// A delivery whose workflow inventory cannot be fetched fails internally
    /// and is retried with backoff rather than dropped.
    #[tokio::test]
    async fn webhook_workflow_fetch_failure_is_redelivered() {
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/build.yml"),
            "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        )
        .unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir.clone())
            .await
            // One 40ms tier: the retry is still a real scheduled retry (the row
            // is not claimable while its backoff is pending), but the test does
            // not sleep through the production ladder.
            .with_retry_backoff(vec![Duration::from_millis(40)]);

        // Hide workspace temporarily so workflow fetching fails transiently.
        let hidden_ws = temp.path().join("hidden_ws");
        std::fs::rename(&ws_dir, &hidden_ws).unwrap();

        let status = fixture.post("delivery-fetch-fail", Some("push")).await;
        assert_eq!(
            status,
            StatusCode::ACCEPTED,
            "durable enqueue must be acknowledged with 202"
        );
        fixture.drain().await;
        {
            let record = fixture
                .state
                .store
                .get_webhook_delivery("delivery-fetch-fail")
                .await
                .unwrap()
                .expect("delivery row must exist");
            assert_eq!(
                record.state,
                WebhookDeliveryStatus::Received,
                "transient fetch failure must keep the row claimable for internal retry"
            );
            assert_eq!(record.attempts, 1);
            assert!(record.last_error.is_some());
            let inner = fixture.state.inner.lock().await;
            assert!(
                inner.runs.is_empty(),
                "a failed delivery must not create a run"
            );
        }

        // Restore the workspace: internal retry drains the queue and creates the run.
        std::fs::rename(&hidden_ws, &ws_dir).unwrap();
        // Past the injected tier, so the retry is due rather than early.
        tokio::time::sleep(Duration::from_millis(80)).await;
        fixture.drain().await;
        let record = fixture
            .state
            .store
            .get_webhook_delivery("delivery-fetch-fail")
            .await
            .unwrap()
            .expect("delivery row must exist");
        assert_eq!(record.state, WebhookDeliveryStatus::Done);
        let inner = fixture.state.inner.lock().await;
        assert_eq!(
            inner.runs.len(),
            1,
            "internal retry must successfully create the run"
        );
    }

    /// Trip the shared breaker the way three consecutive 5xx responses would.
    fn open_breaker(state: &AppState) {
        for _ in 0..3 {
            state.github_breaker.record_failure(
                &crate::github_breaker::GithubFailureKind::Unavailable,
                "GitHub responded 503",
            );
        }
        assert!(state.github_breaker.is_open(), "breaker must be open");
    }

    /// A known GitHub outage must stop the queue from claiming: every claim
    /// charges an attempt and would fail on the same dependency.
    #[tokio::test]
    async fn webhook_queue_stops_claiming_while_github_is_unavailable() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;
        assert_eq!(
            fixture.post("delivery-outage-gate", Some("push")).await,
            StatusCode::ACCEPTED
        );
        open_breaker(&fixture.state);

        assert_eq!(
            fixture.drain().await,
            0,
            "no delivery may be claimed while GitHub is circuit-broken"
        );

        let record = fixture
            .state
            .store
            .get_webhook_delivery("delivery-outage-gate")
            .await
            .unwrap()
            .expect("delivery row must exist");
        assert_eq!(record.state, WebhookDeliveryStatus::Received);
        assert_eq!(
            record.attempts, 0,
            "an outage must not spend the delivery's retry budget"
        );
    }

    /// A delivery that fails because GitHub is down is parked with its
    /// attempt refunded — never dead-lettered, even past the attempt cap.
    #[tokio::test]
    async fn github_outage_parks_a_delivery_instead_of_dead_lettering_it() {
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/build.yml"),
            "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        )
        .unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir.clone()).await;
        // Make workflow evaluation fail the way an unreachable dependency
        // would, and start from an attempt count already past the cap.
        let hidden_ws = temp.path().join("hidden_ws");
        std::fs::rename(&ws_dir, &hidden_ws).unwrap();
        fixture
            .state
            .store
            .enqueue_webhook_delivery(&WebhookDeliveryRecord {
                delivery_id: "delivery-outage-park".to_owned(),
                event: "push".to_owned(),
                payload: fixture.payload_bytes.clone(),
                received_at_us: crate::store::now_us(),
                state: WebhookDeliveryStatus::Received,
                attempts: WEBHOOK_MAX_ATTEMPTS,
                lease_until_us: None,
                lease_token: None,
                last_error: None,
            })
            .await
            .unwrap();
        let shared = Arc::new(SharedState {
            state: fixture.state.clone(),
            shutdown: CancellationToken::new(),
        });
        let claimed = shared
            .state
            .store
            .claim_webhook_deliveries(1, WEBHOOK_LEASE_DURATION_SECS)
            .await
            .unwrap();
        let delivery = claimed.into_iter().next().expect("claimed the delivery");
        assert_eq!(delivery.attempts, WEBHOOK_MAX_ATTEMPTS + 1);

        open_breaker(&fixture.state);
        process_one_delivery(&shared, &delivery).await;

        let record = fixture
            .state
            .store
            .get_webhook_delivery("delivery-outage-park")
            .await
            .unwrap()
            .expect("delivery row must exist");
        assert_eq!(
            record.state,
            WebhookDeliveryStatus::Received,
            "a dependency outage must never dead-letter a delivery"
        );
        assert_eq!(
            record.attempts, WEBHOOK_MAX_ATTEMPTS,
            "parking refunds the attempt the claim charged"
        );
        assert!(record
            .lease_until_us
            .is_some_and(|lease_until| lease_until > crate::store::now_us()));
    }

    /// A delivery whose ref SHA cannot be resolved is retried internally rather than dropped.
    #[tokio::test]
    async fn webhook_unresolvable_sha_is_redelivered() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;

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

        let status = fixture
            .post_body("delivery-no-sha", Some("push"), &bytes)
            .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        fixture.drain().await;

        let record = fixture
            .state
            .store
            .get_webhook_delivery("delivery-no-sha")
            .await
            .unwrap()
            .expect("delivery row must exist");
        assert_eq!(
            record.state,
            WebhookDeliveryStatus::Received,
            "transient unresolvable SHA must keep the row claimable for retry"
        );
        assert_eq!(record.attempts, 1);
        assert!(record.last_error.is_some());
        let inner = fixture.state.inner.lock().await;
        assert!(
            inner.runs.is_empty(),
            "a failed delivery must not create a run"
        );
    }

    /// A malformed workflow is reported permanently without preventing valid
    /// workflows from the same webhook commit from being submitted.
    #[tokio::test]
    async fn malformed_workflow_does_not_abort_other_workflows() {
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/a-malformed.yml"),
            "on: [push\njobs:\n",
        )
        .unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/z-valid.yml"),
            "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo valid\n",
        )
        .unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;

        assert_eq!(
            fixture.post("delivery-malformed", Some("push")).await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let record = fixture
            .state
            .store
            .get_webhook_delivery("delivery-malformed")
            .await
            .unwrap()
            .expect("delivery row exists");
        assert_eq!(record.state, WebhookDeliveryStatus::Failed);
        assert!(
            record
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("a-malformed.yml")),
            "permanent delivery error must identify malformed workflow"
        );
        let inner = fixture.state.inner.lock().await;
        assert_eq!(
            inner.runs.len(),
            1,
            "valid workflow must still be submitted after malformed workflow"
        );
        assert_eq!(
            inner.runs.values().next().unwrap().workflow_path_str,
            ".github/workflows/z-valid.yml"
        );
    }

    #[tokio::test]
    async fn malformed_pull_request_workflow_reports_head_sha() {
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/a-malformed.yml"),
            "on: [push\njobs:\n",
        )
        .unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;
        let base_sha = git_output(&fixture.workspace, &["rev-parse", "HEAD"]);
        let payload = serde_json::json!({
            "action": "opened",
            "repository": {
                "full_name": "owner/repo",
                "default_branch": "main"
            },
            "pull_request": {
                "base": { "ref": "main", "sha": base_sha },
                "head": { "ref": "feature", "sha": "head-sha-456" }
            }
        });
        let payload = serde_json::to_vec(&payload).unwrap();
        assert_eq!(
            fixture
                .post_body(
                    "delivery-pr-failure-sha",
                    Some("pull_request_target"),
                    &payload
                )
                .await,
            StatusCode::ACCEPTED
        );

        let shared = Arc::new(SharedState {
            state: fixture.state.clone(),
            shutdown: CancellationToken::new(),
        });
        let claimed = fixture
            .state
            .store
            .claim_webhook_deliveries(1, 60)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        let outcome = process_delivery_payload(&shared, &claimed[0]).await;
        match outcome {
            WebhookOutcome::PermanentErrors { failures, .. } => {
                assert_eq!(failures.len(), 1);
                assert_eq!(failures[0].sha.as_deref(), Some("head-sha-456"));
            }
            other => panic!("malformed pull request workflow must be permanent: {other:?}"),
        }
    }

    #[tokio::test]
    async fn malformed_pull_request_workflow_failure_is_deduplicated() {
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/a-malformed.yml"),
            "on: [push\njobs:\n",
        )
        .unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;
        let commit_sha = git_output(&fixture.workspace, &["rev-parse", "HEAD"]);
        let payload = serde_json::json!({
            "action": "opened",
            "number": 7,
            "repository": {
                "full_name": "owner/repo",
                "default_branch": "main"
            },
            "pull_request": {
                "number": 7,
                "base": { "ref": "main", "sha": commit_sha },
                "head": {
                    "ref": "feature",
                    "sha": "head-sha-456",
                    "repo": { "fork": false }
                },
                "merge_commit_sha": commit_sha
            }
        });
        let shared = Arc::new(SharedState {
            state: fixture.state.clone(),
            shutdown: CancellationToken::new(),
        });
        let delivery = WebhookDeliveryRecord {
            delivery_id: "delivery-pr-duplicate-failure".to_owned(),
            event: "pull_request".to_owned(),
            payload: serde_json::to_vec(&payload).unwrap(),
            received_at_us: crate::store::now_us(),
            state: WebhookDeliveryStatus::Processing,
            attempts: 1,
            lease_until_us: Some(crate::store::now_us() + 60_000_000),
            lease_token: Some("test-lease".to_owned()),
            last_error: None,
        };

        match process_delivery_payload(&shared, &delivery).await {
            WebhookOutcome::PermanentErrors { failures, .. } => {
                assert_eq!(
                    failures.len(),
                    1,
                    "pull_request target and pull_request projections share one failure"
                );
                assert_eq!(failures[0].sha.as_deref(), Some("head-sha-456"));
            }
            other => panic!("malformed pull request workflow must be permanent: {other:?}"),
        }
    }

    /// Regression guard: a workflow that simply is not triggered by the event
    /// is a *completed* delivery, not a failure.
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
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let record = fixture
            .state
            .store
            .get_webhook_delivery("delivery-no-match")
            .await
            .unwrap()
            .expect("delivery row must exist");
        assert_eq!(
            record.state,
            WebhookDeliveryStatus::Done,
            "a delivery that triggered no workflow is still completed successfully"
        );
        let inner = fixture.state.inner.lock().await;
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
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let record = fixture
            .state
            .store
            .get_webhook_delivery("delivery-github-owned")
            .await
            .unwrap()
            .expect("delivery row must exist");
        assert_eq!(
            record.state,
            WebhookDeliveryStatus::Done,
            "skipped github-owned workflow completes delivery"
        );
        let inner = fixture.state.inner.lock().await;
        assert!(inner.runs.is_empty());

        std::env::remove_var(GITHUB_OWNED_WORKFLOWS_ENV);
    }

    /// Crash recovery: processing that died mid-flight has its expired lease
    /// recovered on boot and drains to completion.
    #[tokio::test]
    async fn webhook_cancelled_processing_releases_reservation() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;

        let record = WebhookDeliveryRecord {
            delivery_id: "delivery-cancel".to_owned(),
            event: "push".to_owned(),
            payload: fixture.payload_bytes.clone(),
            received_at_us: crate::store::now_us() - 100_000_000,
            state: WebhookDeliveryStatus::Processing,
            attempts: 1,
            lease_until_us: Some(crate::store::now_us() - 1_000_000), // expired lease
            lease_token: Some("expired-token".to_owned()),
            last_error: None,
        };
        fixture
            .state
            .store
            .enqueue_webhook_delivery(&record)
            .await
            .unwrap();

        let recovered = fixture
            .state
            .store
            .recover_webhook_deliveries()
            .await
            .unwrap();
        assert_eq!(recovered, 1, "expired processing lease must be recovered");

        let rec = fixture
            .state
            .store
            .get_webhook_delivery("delivery-cancel")
            .await
            .unwrap()
            .expect("delivery row exists");
        assert_eq!(rec.state, WebhookDeliveryStatus::Received);

        fixture.drain().await;
        let inner = fixture.state.inner.lock().await;
        assert_eq!(
            inner.runs.len(),
            1,
            "recovered delivery must be processed to create run"
        );
    }
    #[tokio::test]
    async fn webhook_replay_reuses_existing_run() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;

        assert_eq!(
            fixture.post("delivery-replay", Some("push")).await,
            StatusCode::ACCEPTED
        );
        let shared = Arc::new(SharedState {
            state: fixture.state.clone(),
            shutdown: CancellationToken::new(),
        });
        let claimed = fixture
            .state
            .store
            .claim_webhook_deliveries(1, 60)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        let outcome = process_delivery_payload(&shared, &claimed[0]).await;
        assert!(matches!(outcome, WebhookOutcome::Success));
        let (original_run_id, original_check_run_ids) = {
            let inner = fixture.state.inner.lock().await;
            assert_eq!(inner.runs.len(), 1);
            let run = inner.runs.values().next().unwrap();
            assert_eq!(run.webhook_delivery_id.as_deref(), Some("delivery-replay"));
            (run.run_id, run.job_check_run_ids.clone())
        };

        // Simulate a worker crash after run creation but before marking the
        // delivery done. The replay must reuse that persisted run.
        let lease_token = claimed[0].lease_token.as_deref().unwrap();
        assert!(fixture
            .state
            .store
            .fail_webhook_delivery(
                "delivery-replay",
                lease_token,
                "simulated crash",
                false,
                Some(Duration::ZERO),
            )
            .await
            .unwrap());
        fixture.drain().await;

        let inner = fixture.state.inner.lock().await;
        assert_eq!(
            inner.runs.len(),
            1,
            "replaying one delivery must not create a second run"
        );
        let run = inner
            .runs
            .get(&original_run_id)
            .expect("original run survives replay");
        assert_eq!(
            run.job_check_run_ids, original_check_run_ids,
            "replaying a persisted run must reuse its GitHub check-run mapping"
        );
    }
    #[tokio::test]
    async fn failed_webhook_delivery_can_be_reopened_for_redelivery() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;
        let delivery = WebhookDeliveryRecord {
            delivery_id: "delivery-redelivery".to_owned(),
            event: "push".to_owned(),
            payload: fixture.payload_bytes.clone(),
            received_at_us: crate::store::now_us(),
            state: WebhookDeliveryStatus::Received,
            attempts: 0,
            lease_until_us: None,
            lease_token: None,
            last_error: None,
        };
        assert!(fixture
            .state
            .store
            .enqueue_webhook_delivery(&delivery)
            .await
            .unwrap());
        let claimed = fixture
            .state
            .store
            .claim_webhook_deliveries(1, 60)
            .await
            .unwrap();
        let lease_token = claimed[0].lease_token.as_deref().unwrap();
        assert!(fixture
            .state
            .store
            .fail_webhook_delivery(
                &delivery.delivery_id,
                lease_token,
                "permanent test failure",
                true,
                None,
            )
            .await
            .unwrap());

        let failed = fixture
            .state
            .store
            .get_webhook_delivery(&delivery.delivery_id)
            .await
            .unwrap()
            .expect("failed delivery row exists");
        assert_eq!(failed.state, WebhookDeliveryStatus::Failed);

        let mut redelivery = delivery.clone();
        redelivery.received_at_us = crate::store::now_us();
        assert!(
            fixture
                .state
                .store
                .enqueue_webhook_delivery(&redelivery)
                .await
                .unwrap(),
            "GitHub redelivery must reopen a retained failed row"
        );
        let reopened = fixture
            .state
            .store
            .get_webhook_delivery(&delivery.delivery_id)
            .await
            .unwrap()
            .expect("reopened delivery row exists");
        assert_eq!(reopened.state, WebhookDeliveryStatus::Received);
        assert_eq!(reopened.attempts, 0);
        assert!(reopened.lease_token.is_none());
        assert!(reopened.last_error.is_none());
        assert!(
            !fixture
                .state
                .store
                .enqueue_webhook_delivery(&redelivery)
                .await
                .unwrap(),
            "an active redelivery remains deduplicated"
        );
    }

    #[tokio::test]
    async fn corrupt_webhook_payload_is_dead_lettered_without_wedging_queue() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;
        let corrupt = WebhookDeliveryRecord {
            delivery_id: "delivery-corrupt".to_owned(),
            event: "push".to_owned(),
            payload: fixture.payload_bytes.clone(),
            received_at_us: crate::store::now_us() - 1,
            state: WebhookDeliveryStatus::Received,
            attempts: 0,
            lease_until_us: None,
            lease_token: None,
            last_error: None,
        };
        let valid = WebhookDeliveryRecord {
            delivery_id: "delivery-after-corrupt".to_owned(),
            received_at_us: crate::store::now_us(),
            ..corrupt.clone()
        };
        assert!(fixture
            .state
            .store
            .enqueue_webhook_delivery(&corrupt)
            .await
            .unwrap());
        assert!(fixture
            .state
            .store
            .enqueue_webhook_delivery(&valid)
            .await
            .unwrap());

        let db_path = temp.path().join("state").join("preloop.db");
        let connection = rusqlite::Connection::open(db_path).unwrap();
        connection
            .execute(
                "UPDATE webhook_deliveries SET payload_blob = ?1 WHERE delivery_id = ?2",
                rusqlite::params![vec![0_u8, 1, 2], corrupt.delivery_id],
            )
            .unwrap();
        drop(connection);

        let claimed = fixture
            .state
            .store
            .claim_webhook_deliveries(1, 60)
            .await
            .unwrap();
        assert!(
            claimed.is_empty(),
            "corrupt payload is dead-lettered instead of returned to the worker"
        );
        assert_eq!(
            fixture
                .state
                .store
                .count_dead_letter_webhook_deliveries()
                .await
                .unwrap(),
            1
        );

        let claimed = fixture
            .state
            .store
            .claim_webhook_deliveries(1, 60)
            .await
            .unwrap();
        assert_eq!(
            claimed.len(),
            1,
            "a corrupt FIFO row must not wedge later valid deliveries"
        );
        let lease_token = claimed[0].lease_token.as_deref().unwrap();
        assert!(fixture
            .state
            .store
            .complete_webhook_delivery(&valid.delivery_id, lease_token)
            .await
            .unwrap());
    }
    #[tokio::test]
    async fn webhook_processing_lease_can_be_renewed() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;
        let delivery = WebhookDeliveryRecord {
            delivery_id: "delivery-lease".to_owned(),
            event: "push".to_owned(),
            payload: fixture.payload_bytes.clone(),
            received_at_us: crate::store::now_us(),
            state: WebhookDeliveryStatus::Received,
            attempts: 0,
            lease_until_us: None,
            lease_token: None,
            last_error: None,
        };
        fixture
            .state
            .store
            .enqueue_webhook_delivery(&delivery)
            .await
            .unwrap();
        let claimed = fixture
            .state
            .store
            .claim_webhook_deliveries(1, 1)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);

        let lease_token = claimed[0].lease_token.as_deref().unwrap();
        assert!(
            !fixture
                .state
                .store
                .renew_webhook_delivery("delivery-lease", "stale-token", 60)
                .await
                .unwrap(),
            "a stale worker must not renew a reclaimed lease"
        );
        assert!(fixture
            .state
            .store
            .renew_webhook_delivery("delivery-lease", lease_token, 60)
            .await
            .unwrap());
        assert!(
            !fixture
                .state
                .store
                .complete_webhook_delivery("delivery-lease", "stale-token")
                .await
                .unwrap(),
            "a stale worker must not finalize a reclaimed lease"
        );
        let renewed = fixture
            .state
            .store
            .get_webhook_delivery("delivery-lease")
            .await
            .unwrap()
            .unwrap();
        assert!(
            renewed.lease_until_us.unwrap() > crate::store::now_us() + 50_000_000,
            "renewal must move the lease beyond the original one-second claim"
        );
    }
    #[tokio::test]
    async fn expired_webhook_lease_rejects_all_mutations() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;
        let delivery = WebhookDeliveryRecord {
            delivery_id: "delivery-expired-lease".to_owned(),
            event: "push".to_owned(),
            payload: fixture.payload_bytes.clone(),
            received_at_us: crate::store::now_us(),
            state: WebhookDeliveryStatus::Received,
            attempts: 0,
            lease_until_us: None,
            lease_token: None,
            last_error: None,
        };
        fixture
            .state
            .store
            .enqueue_webhook_delivery(&delivery)
            .await
            .unwrap();
        let claimed = fixture
            .state
            .store
            .claim_webhook_deliveries(1, 0)
            .await
            .unwrap();
        let lease_token = claimed[0].lease_token.as_deref().unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;

        assert!(
            !fixture
                .state
                .store
                .renew_webhook_delivery(&delivery.delivery_id, lease_token, 60)
                .await
                .unwrap(),
            "an expired lease must not be renewed by its old owner"
        );
        assert!(
            !fixture
                .state
                .store
                .complete_webhook_delivery(&delivery.delivery_id, lease_token)
                .await
                .unwrap(),
            "an expired lease must not be completed by its old owner"
        );
        assert!(
            !fixture
                .state
                .store
                .fail_webhook_delivery(
                    &delivery.delivery_id,
                    lease_token,
                    "stale failure",
                    false,
                    Some(Duration::ZERO),
                )
                .await
                .unwrap(),
            "an expired lease must not be failed by its old owner"
        );
        let retained = fixture
            .state
            .store
            .get_webhook_delivery(&delivery.delivery_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retained.state, WebhookDeliveryStatus::Processing);
        assert_eq!(retained.lease_token.as_deref(), Some(lease_token));
    }

    #[tokio::test]
    async fn webhook_terminal_rows_are_pruned_after_retention() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;
        let now = crate::store::now_us();
        for (delivery_id, received_at_us) in
            [("delivery-old", now - 2_000_000), ("delivery-fresh", now)]
        {
            fixture
                .state
                .store
                .enqueue_webhook_delivery(&WebhookDeliveryRecord {
                    delivery_id: delivery_id.to_owned(),
                    event: "push".to_owned(),
                    payload: fixture.payload_bytes.clone(),
                    received_at_us,
                    state: WebhookDeliveryStatus::Done,
                    attempts: 1,
                    lease_until_us: None,
                    lease_token: None,
                    last_error: None,
                })
                .await
                .unwrap();
        }

        let pruned = fixture
            .state
            .store
            .prune_webhook_deliveries(now - 1_000_000, 10)
            .await
            .unwrap();
        assert_eq!(pruned, 1);
        assert!(fixture
            .state
            .store
            .get_webhook_delivery("delivery-old")
            .await
            .unwrap()
            .is_none());
        assert!(fixture
            .state
            .store
            .get_webhook_delivery("delivery-fresh")
            .await
            .unwrap()
            .is_some());
    }
}
