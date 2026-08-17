//! GitHub-compatible dispatch REST endpoints (surface 2 of the App contract).
//!
//! `POST /repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches`
//! and `POST /repos/{owner}/{repo}/dispatches` trigger runs exactly like
//! github.com's Actions API: authenticated through the D2 chain
//! ([`crate::dispatch_auth`]), validated against the workflow's declared
//! triggers and inputs, and submitted through the *same* event adapters the
//! webhook path uses (`events::workflow_dispatch`,
//! `events::repository_dispatch`) into `submit_run_inner` — no parallel
//! submit path.
//!
//! Error semantics follow github.com: 401 (bad token), 403 (token valid but
//! no `actions: write` / repo not accessible), 404 (unknown repo, workflow,
//! or ref — existence is never leaked), 409 (workflow exists but is not
//! `workflow_dispatch`-triggered), 422 (input validation, missing
//! `event_type`).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::dispatch_auth::DispatchIdentity;
use crate::events::trust_tier::TrustTier;
use crate::events::EventAdapter;
use crate::state::SharedState;
use crate::{ApiError, RunAccepted, WorkflowSubmission};

/// POST /repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches
///
/// Body: `{ "ref": string?, "inputs": {k: v}? }` — `ref` defaults to the
/// default branch. Success is `204 No Content`.
pub(crate) async fn workflow_dispatch(
    State(shared): State<Arc<SharedState>>,
    Path((owner, repo, workflow_id)): Path<(String, String, String)>,
    Extension(identity): Extension<DispatchIdentity>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    authorize_dispatch(&identity, &owner, &repo)?;
    let repository = format!("{owner}/{repo}");
    let object = parse_object(&body)?;
    let selected_ref = object
        .get("ref")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let inputs = object.get("inputs").cloned().unwrap_or_else(|| json!({}));
    if !inputs.is_object() {
        return Err(ApiError::unprocessable("`inputs` must be a JSON object"));
    }

    let default_branch = resolve_default_branch(&shared, &repository).await?;
    let (git_ref, ref_type) =
        resolve_dispatch_ref(&shared, &repository, selected_ref, &default_branch).await?;
    let workflows = fetch_workflows(&shared, &repository, &git_ref)
        .await
        .map_err(|error| {
            ApiError::bad_gateway(format!("failed to fetch workflows at {git_ref}: {error}"))
        })?;
    let filename = find_workflow(&workflows, &workflow_id).ok_or_else(|| {
        ApiError::not_found(format!(
            "workflow {workflow_id} not found in {repository} at {git_ref}"
        ))
    })?;
    let content = &workflows[&filename];
    let parsed = preloop_gha_parser::parse_workflow(content)
        .map_err(|_| ApiError::not_found(format!("workflow {workflow_id} could not be parsed")))?;
    if !workflow_has_trigger(&parsed, "workflow_dispatch") {
        return Err(ApiError::conflict(format!(
            "workflow {workflow_id} does not have a `workflow_dispatch` trigger"
        )));
    }

    // Synthesize the webhook-shaped payload the adapter projects. `github.event`
    // is this payload verbatim, so `ref`/`ref_type`/`repository`/`sender` keep
    // the same shape a github.com-delivered workflow_dispatch webhook has.
    let mut payload = json!({
        "ref": git_ref,
        "ref_type": ref_type,
        "inputs": inputs,
        "repository": {
            "full_name": repository,
            "default_branch": default_branch,
        },
        "sender": { "login": identity.actor },
    });
    // D4: validate inputs against `on.workflow_dispatch.inputs` *before* any
    // run is created — missing required, type mismatch, and out-of-options
    // choices all surface as 422. Defaults are applied here so the run and
    // `github.event.inputs` carry them.
    parsed
        .apply_workflow_dispatch_inputs(&mut payload)
        .map_err(|error| ApiError::unprocessable(error.to_string()))?;

    let sha = resolve_ref_sha(&shared, &repository, &git_ref)
        .await
        .map_err(|error| ApiError::bad_gateway(format!("failed to resolve {git_ref}: {error}")))?
        .ok_or_else(|| ApiError::not_found(format!("ref {git_ref} not found in {repository}")))?;

    let effective = crate::events::workflow_dispatch::Adapter
        .project(&payload)
        .into_iter()
        .next()
        .expect("the workflow_dispatch adapter always projects one event");
    let submission = submission_from_effective(
        content.clone(),
        "workflow_dispatch",
        payload,
        &repository,
        effective,
        &filename,
        &sha,
        &identity,
    );
    submit_and_report(&shared, submission, &repository, &sha).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// POST /repos/{owner}/{repo}/dispatches
///
/// Body: `{ "event_type": string (required, <= 100 chars), "client_payload":
/// object? }`. A broadcast: every workflow whose `on.repository_dispatch.types`
/// matches `event_type` runs (an absent `types` matches every event_type).
/// Success is `204 No Content` even when no workflow matches.
pub(crate) async fn repository_dispatch(
    State(shared): State<Arc<SharedState>>,
    Path((owner, repo)): Path<(String, String)>,
    Extension(identity): Extension<DispatchIdentity>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    authorize_dispatch(&identity, &owner, &repo)?;
    let repository = format!("{owner}/{repo}");
    let object = parse_object(&body)?;
    let event_type = object
        .get("event_type")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::unprocessable("`event_type` is required"))?;
    if event_type.chars().count() > 100 {
        return Err(ApiError::unprocessable(
            "`event_type` must be 100 characters or fewer",
        ));
    }
    let client_payload = match object.get("client_payload") {
        None | Some(Value::Null) => json!({}),
        Some(payload) if payload.is_object() => payload.clone(),
        Some(_) => {
            return Err(ApiError::unprocessable(
                "`client_payload` must be a JSON object",
            ))
        }
    };

    let default_branch = resolve_default_branch(&shared, &repository).await?;
    let git_ref = format!("refs/heads/{default_branch}");
    let sha = resolve_ref_sha(&shared, &repository, &git_ref)
        .await
        .map_err(|error| ApiError::bad_gateway(format!("failed to resolve {git_ref}: {error}")))?
        .ok_or_else(|| ApiError::not_found(format!("ref {git_ref} not found in {repository}")))?;
    let workflows = fetch_workflows(&shared, &repository, &git_ref)
        .await
        .map_err(|error| {
            ApiError::bad_gateway(format!("failed to fetch workflows at {git_ref}: {error}"))
        })?;

    for (filename, content) in workflows {
        let payload = json!({
            "action": event_type,
            "client_payload": client_payload,
            "repository": {
                "full_name": repository,
                "default_branch": default_branch,
            },
            "sender": { "login": identity.actor },
        });
        let Some(effective) = crate::events::repository_dispatch::Adapter
            .project(&payload)
            .into_iter()
            .next()
        else {
            continue;
        };
        let submission = submission_from_effective(
            content,
            "repository_dispatch",
            payload,
            &repository,
            effective,
            &filename,
            &sha,
            &identity,
        );
        match submit_and_report(&shared, submission, &repository, &sha).await {
            Ok(_) => {}
            // A trigger mismatch is expected in a broadcast: the adapter
            // submits every workflow, and submit_run_inner is authoritative
            // about the workflow's `on.repository_dispatch` filter.
            Err(error) if is_trigger_mismatch(&error) => {
                info!(
                    workflow = %filename,
                    event_type,
                    detail = %error.message(),
                    "workflow not triggered by repository_dispatch"
                );
            }
            // Other client errors are real per-workflow failures (for
            // example, invalid workflow configuration). Keep broadcasting so
            // unrelated workflows can still run, but make the partial
            // failure visible instead of mislabeling it as a non-match.
            Err(error) if error.status().is_client_error() => {
                warn!(
                    workflow = %filename,
                    event_type,
                    status = %error.status(),
                    detail = %error.message(),
                    "repository_dispatch workflow submission failed; other workflows will continue"
                );
            }
            Err(error) => return Err(error),
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

/// GET /repos/{owner}/{repo}/actions/workflows
///
/// Convenience list for Apps that poll instead of relying on check runs.
/// `id` is a deterministic hash of the workflow path — preloop does not track
/// github.com's numeric workflow ids (see the plan's open question 2), so the
/// dispatch `workflow_id` accepts filenames, not numbers.
pub(crate) async fn list_workflows(
    State(shared): State<Arc<SharedState>>,
    Path((owner, repo)): Path<(String, String)>,
    Extension(identity): Extension<DispatchIdentity>,
) -> Result<Json<Value>, ApiError> {
    authorize_read(&identity, &owner, &repo)?;
    let repository = format!("{owner}/{repo}");
    let default_branch = resolve_default_branch(&shared, &repository).await?;
    let git_ref = format!("refs/heads/{default_branch}");
    let workflows = fetch_workflows(&shared, &repository, &git_ref)
        .await
        .map_err(|error| {
            ApiError::bad_gateway(format!("failed to fetch workflows at {git_ref}: {error}"))
        })?;
    let mut items = Vec::with_capacity(workflows.len());
    for (filename, content) in workflows {
        let path = format!(".github/workflows/{filename}");
        let name = preloop_gha_parser::parse_workflow(&content)
            .ok()
            .and_then(|workflow| workflow.name)
            .unwrap_or_else(|| filename.clone());
        items.push(json!({
            "id": stable_id(&path),
            "name": name,
            "path": path,
            "state": "active",
        }));
    }
    Ok(Json(
        json!({ "total_count": items.len(), "workflows": items }),
    ))
}

/// GET /repos/{owner}/{repo}/actions/runs
///
/// Convenience list of recent runs for the repository. `id` is a
/// deterministic hash of the preloop run UUID; the native `run_id` field is
/// included for preloop-native consumers.
pub(crate) async fn list_actions_runs(
    State(shared): State<Arc<SharedState>>,
    Path((owner, repo)): Path<(String, String)>,
    Extension(identity): Extension<DispatchIdentity>,
) -> Result<Json<Value>, ApiError> {
    authorize_read(&identity, &owner, &repo)?;
    let repository = format!("{owner}/{repo}");
    let inner = shared.state.inner.lock().await;
    let mut runs: Vec<&crate::models::RunRecord> = inner
        .runs
        .values()
        .filter(|run| run.submission.repository.eq_ignore_ascii_case(&repository))
        .collect();
    // github.com lists the newest runs first.
    runs.sort_by_key(|right| std::cmp::Reverse(right.created_at));
    let workflow_runs: Vec<Value> = runs
        .iter()
        .map(|run| {
            json!({
                "id": stable_id(&run.run_id.to_string()),
                "run_id": run.run_id.to_string(),
                "name": run.run_name,
                "event": run.event,
                "status": github_run_status(run.status),
                "conclusion": run.conclusion,
                "head_sha": run.head_sha,
                "created_at": run.created_at.to_rfc3339(),
                "run_number": run.run_number,
                "run_attempt": run.run_attempt,
                "workflow_path": run.workflow_path_str,
                "html_url": crate::github::run_details_url(run.run_id),
            })
        })
        .collect();
    Ok(Json(
        json!({ "total_count": workflow_runs.len(), "workflow_runs": workflow_runs }),
    ))
}

/// Shared authorization for the dispatch POSTs: `actions: write` on the repo
/// plus repository reachability.
fn authorize_dispatch(
    identity: &DispatchIdentity,
    owner: &str,
    repo: &str,
) -> Result<(), ApiError> {
    if !identity.covers_repository(owner, repo) {
        return Err(ApiError::forbidden(format!(
            "the installation token cannot access {owner}/{repo}"
        )));
    }
    if !identity.has_actions_write() {
        return Err(ApiError::forbidden(
            "the installation token does not grant `actions: write` on this repository",
        ));
    }
    Ok(())
}

/// Shared authorization for the read endpoints: repository reachability only.
fn authorize_read(identity: &DispatchIdentity, owner: &str, repo: &str) -> Result<(), ApiError> {
    if !identity.covers_repository(owner, repo) {
        return Err(ApiError::forbidden(format!(
            "the installation token cannot access {owner}/{repo}"
        )));
    }
    Ok(())
}

/// Parse a request body as a JSON object. Non-JSON is 400; a non-object JSON
/// body is 422 (github.com's dispatch bodies are objects).
fn parse_object(body: &Bytes) -> Result<serde_json::Map<String, Value>, ApiError> {
    let value: Value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(body)
            .map_err(|_| ApiError::bad_request("the request body is not valid JSON"))?
    };
    value
        .as_object()
        .cloned()
        .ok_or_else(|| ApiError::unprocessable("the request body must be a JSON object"))
}

/// Resolve the dispatch ref to a full `refs/...` reference plus its type.
///
/// An absent `ref` uses the default branch. A `refs/heads/...` / `refs/tags/...`
/// prefix is honored directly; a bare name is tried as a branch first, then a
/// tag, matching github.com's resolution.
async fn resolve_dispatch_ref(
    shared: &Arc<SharedState>,
    repository: &str,
    selected_ref: Option<&str>,
    default_branch: &str,
) -> Result<(String, &'static str), ApiError> {
    let Some(raw) = selected_ref else {
        return Ok((format!("refs/heads/{default_branch}"), "branch"));
    };
    if let Some(branch) = raw.strip_prefix("refs/heads/") {
        return Ok((format!("refs/heads/{branch}"), "branch"));
    }
    if let Some(tag) = raw.strip_prefix("refs/tags/") {
        return Ok((format!("refs/tags/{tag}"), "tag"));
    }
    let branch_ref = format!("refs/heads/{raw}");
    if resolve_ref_sha(shared, repository, &branch_ref)
        .await
        .map_err(|error| ApiError::bad_gateway(format!("failed to resolve {branch_ref}: {error}")))?
        .is_some()
    {
        return Ok((branch_ref, "branch"));
    }
    let tag_ref = format!("refs/tags/{raw}");
    if resolve_ref_sha(shared, repository, &tag_ref)
        .await
        .map_err(|error| ApiError::bad_gateway(format!("failed to resolve {tag_ref}: {error}")))?
        .is_some()
    {
        return Ok((tag_ref, "tag"));
    }
    Err(ApiError::not_found(format!(
        "ref {raw} not found in {repository}"
    )))
}

/// Find the workflow file `workflow_id` names.
///
/// Accepts `ci.yml`, `ci`, and `.github/workflows/ci.yml` (github.com accepts
/// both the numeric id and the path; preloop does not track numeric workflow
/// ids, so filenames only — see the plan's open question 2).
fn find_workflow(workflows: &BTreeMap<String, String>, workflow_id: &str) -> Option<String> {
    let normalized = workflow_id
        .trim()
        .trim_start_matches("./")
        .trim_start_matches(".github/workflows/");
    if workflows.contains_key(normalized) {
        return Some(normalized.to_owned());
    }
    let stem = normalized
        .strip_suffix(".yml")
        .or_else(|| normalized.strip_suffix(".yaml"))
        .unwrap_or(normalized);
    workflows
        .keys()
        .find(|filename| {
            filename
                .strip_suffix(".yml")
                .or_else(|| filename.strip_suffix(".yaml"))
                .is_some_and(|candidate| candidate == stem)
        })
        .cloned()
}

/// Whether the workflow's `on:` includes `event`.
fn workflow_has_trigger(workflow: &preloop_gha_parser::Workflow, event: &str) -> bool {
    use preloop_gha_parser::Trigger;
    match &workflow.on {
        Trigger::Single(name) => name == event,
        Trigger::Many(names) => names.iter().any(|name| name == event),
        Trigger::Map(triggers) => triggers.contains_key(event),
    }
}

/// `submit_run_inner` uses this exact error for a workflow whose trigger
/// filters do not match the broadcast event. Other 4xx errors must remain
/// visible as per-workflow failures.
fn is_trigger_mismatch(error: &ApiError) -> bool {
    error.status() == StatusCode::BAD_REQUEST
        && error
            .message()
            .starts_with("workflow does not match event `")
}

/// Build the `WorkflowSubmission` for a dispatched effective event, carrying
/// the identity's trust tier (not the adapter's — the caller proved its
/// authority at the API boundary) and actor.
#[allow(clippy::too_many_arguments)]
fn submission_from_effective(
    workflow_yaml: String,
    event: &str,
    payload: Value,
    repository: &str,
    effective: crate::events::EffectiveEvent,
    filename: &str,
    sha: &str,
    identity: &DispatchIdentity,
) -> WorkflowSubmission {
    WorkflowSubmission {
        workflow_yaml,
        event: event.to_owned(),
        payload,
        repository: repository.to_owned(),
        git_ref: effective.git_ref,
        workflow_path: Some(format!(".github/workflows/{filename}")),
        local_workspace: None,
        vars: BTreeMap::new(),
        secrets: BTreeMap::new(),
        submission_names: BTreeSet::new(),
        reusable_workflows: BTreeMap::new(),
        reusable_workflow_shas: BTreeMap::new(),
        enable_debugger: false,
        debugger_welcome_message: None,
        sha: sha.to_owned(),
        actor: identity.actor.clone(),
        environment: None,
        workflow_file: Some(filename.to_owned()),
        inputs: BTreeMap::new(),
        trust_tier: Some(tier_string(identity.tier)),
        workflow_run_upstream_names: vec![],
        activity_type: effective.activity_type,
        changed_paths: vec![],
        changed_paths_known: false,
        resolved_sha: Some(sha.to_owned()),
        filter_branch: None,
        dispatch_inputs: BTreeMap::new(),
        dispatch_inputs_stringified: BTreeMap::new(),
        selected_jobs: vec![],
        base_ref: None,
        preserve_on_failure: false,
        push: None,
        push_tree: None,
    }
}

fn tier_string(tier: TrustTier) -> String {
    serde_json::to_value(tier)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "admin-manual".to_owned())
}

/// Submit a dispatched run and report queued/completed check runs exactly
/// like the webhook path does.
async fn submit_and_report(
    shared: &Arc<SharedState>,
    submission: WorkflowSubmission,
    repository: &str,
    sha: &str,
) -> Result<RunAccepted, ApiError> {
    let accepted = crate::submit_run_inner(shared, submission).await?;
    let run_id = accepted.run_id;
    let jobs = {
        let inner = shared.state.inner.lock().await;
        inner
            .runs
            .get(&run_id)
            .map(|run| run.jobs.keys().cloned().collect::<Vec<_>>())
    };
    if let Some(jobs) = jobs {
        for job_id in jobs {
            crate::github::report_check_run_queued(shared, repository, sha, &job_id, run_id).await;
            let status = {
                let inner = shared.state.inner.lock().await;
                inner
                    .runs
                    .get(&run_id)
                    .and_then(|run| run.jobs.get(&job_id).copied())
            };
            if let Some(status) = status.filter(|status| status.is_terminal()) {
                crate::github::report_check_run_completed(shared, run_id, &job_id, status).await;
            }
        }
    }
    Ok(accepted)
}

/// Resolve the repository's default branch, mirroring the scheduler: a local
/// workspace answers from git, otherwise the GitHub API with the same
/// credential ladder as [`crate::github::fetch_workflows`].
async fn resolve_default_branch(
    shared: &Arc<SharedState>,
    repository: &str,
) -> Result<String, ApiError> {
    if let Some(workspace) = &shared.state.local_workspace {
        let output = tokio::process::Command::new("git")
            .arg("-C")
            .arg(workspace)
            .args(["symbolic-ref", "--short", "refs/remotes/origin/HEAD"])
            .output()
            .await;
        if let Ok(output) = output {
            if output.status.success() {
                if let Ok(branch) = String::from_utf8(output.stdout) {
                    let branch = branch.trim();
                    if !branch.is_empty() && !branch.contains(' ') {
                        return Ok(branch.to_owned());
                    }
                }
            }
        }
        return Ok("main".to_owned());
    }
    let api_base = crate::github::github_api_base();
    let token = if let Some(app) = crate::github_app::select_app_for_repo(shared, repository).await
    {
        let permissions = BTreeMap::from([("contents".to_owned(), "read".to_owned())]);
        crate::github_app::get_or_mint_token_at(&api_base, &app, repository, &permissions)
            .await
            .map_err(|error| {
                ApiError::not_found(format!(
                    "{repository} is not accessible to preloop's GitHub credential ({error})"
                ))
            })?
    } else {
        std::env::var("PRELOOP_GITHUB_TOKEN").ok().ok_or_else(|| {
            ApiError::not_found(format!(
                "{repository} is not accessible: no GitHub credential is configured"
            ))
        })?
    };
    let response = crate::shared_http::CLIENT
        .get(format!("{api_base}/repos/{repository}"))
        .header("User-Agent", "preloop")
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json")
        .send()
        .await
        .map_err(|error| ApiError::bad_gateway(format!("failed to fetch {repository}: {error}")))?;
    if !response.status().is_success() {
        return Err(ApiError::not_found(format!(
            "repository {repository} not found or not accessible"
        )));
    }
    let metadata: Value = response.json().await.map_err(|error| {
        ApiError::bad_gateway(format!("{repository} metadata was not JSON: {error}"))
    })?;
    Ok(metadata
        .get("default_branch")
        .and_then(Value::as_str)
        .unwrap_or("main")
        .to_owned())
}

/// Fetch workflows for the dispatch target, like the webhook path.
async fn fetch_workflows(
    shared: &Arc<SharedState>,
    repository: &str,
    git_ref: &str,
) -> anyhow::Result<BTreeMap<String, String>> {
    crate::github::fetch_workflows(shared, repository, git_ref).await
}

/// Resolve a ref to a commit SHA, like the webhook path.
async fn resolve_ref_sha(
    shared: &Arc<SharedState>,
    repository: &str,
    git_ref: &str,
) -> anyhow::Result<Option<String>> {
    crate::github::resolve_ref_sha(shared, repository, git_ref).await
}

/// Deterministic 64-bit hash of `value` — the synthetic numeric id for
/// workflows and runs (preloop tracks neither github.com workflow ids nor
/// numeric run ids). `DefaultHasher::new()` uses fixed keys, so ids are
/// stable across restarts.
fn stable_id(value: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// github.com run status from the internal execution status.
fn github_run_status(status: preloop_gha_protocol::ExecutionStatus) -> &'static str {
    use preloop_gha_protocol::ExecutionStatus as Status;
    match status {
        Status::Queued | Status::Pending => "queued",
        Status::InProgress => "in_progress",
        Status::Success | Status::Failure | Status::Skipped | Status::Cancelled => "completed",
    }
}
