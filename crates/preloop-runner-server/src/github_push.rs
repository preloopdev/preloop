//! Push-back for submit-driven CI (`preloop run --push`).
//!
//! After a run that requested `submission.push` reaches a terminal state,
//! the submitting client pushes the tested commit to GitHub itself; this
//! module then (1) verifies the pushed commit's tree matches the tree the
//! run actually tested, (2) creates or reuses the branch's pull request, and
//! (3) reports check runs for any job that lacks one. Every step is
//! idempotent so `preloop push <run_id>` can replay a failed or interrupted
//! sync freely.
//!
//! The server never pushes: it holds no `contents: write` power. The client
//! owns the git operations and their credentials.

use super::*;

/// Result of a completed sync, returned to the client.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct SyncResponse {
    pub(crate) status: &'static str,
    pub(crate) pr_number: Option<u64>,
    pub(crate) pr_url: Option<String>,
}

/// `POST /api/v1/runs/:run_id/push` — publish a terminal run's result to
/// GitHub. Idempotent: re-running after success is a no-op, and every
/// external effect is guarded by a check-before-create.
pub(crate) async fn push_run(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Json<SyncResponse>, ApiError> {
    Ok(Json(push_run_to_github(&shared, run_id).await?))
}

pub(crate) async fn push_run_to_github(
    shared: &Arc<SharedState>,
    run_id: RunId,
) -> Result<SyncResponse, ApiError> {
    // Snapshot everything the sync needs under one lock, then work outside
    // it: the GitHub calls are slow and must not hold the state mutex.
    let (repository, git_ref, sha, push_tree, create_pr, draft_pr, actor, conclusion, jobs) = {
        let inner = shared.state.inner.lock().await;
        let run = inner
            .runs
            .get(&run_id)
            .ok_or_else(|| ApiError::not_found("run not found"))?;

        if let Some(state) = &run.push_state {
            if state.status == PushStatus::Synced {
                // Already published; replay is a no-op.
                return Ok(SyncResponse {
                    status: "pushed",
                    pr_number: state.pr_number,
                    pr_url: state.pr_number.map(|number| {
                        pr_web_url(
                            &crate::github::github_api_base(),
                            &run.submission.repository,
                            number,
                        )
                    }),
                });
            }
        }

        let Some(push) = &run.submission.push else {
            return Err(ApiError::bad_request(
                "run was not submitted with --push; submit with `preloop run --push`",
            ));
        };
        let Some(conclusion) = &run.conclusion else {
            return Err(ApiError::bad_request(
                "run is not terminal yet; push runs after completion",
            ));
        };
        let Some(tree) = &run.submission.push_tree else {
            return Err(ApiError::bad_request(
                "run has no recorded tested tree; it was not submitted with --push",
            ));
        };

        let repository = run.submission.repository.clone();
        validate_push_target(
            &repository,
            &run.submission.sha,
            &run.submission.git_ref,
            tree,
        )?;

        (
            repository,
            run.submission.git_ref.clone(),
            run.submission.sha.clone(),
            tree.clone(),
            push.create_pr,
            push.draft_pr,
            run.submission.actor.clone(),
            conclusion.clone(),
            run.jobs.clone(),
        )
    };

    async fn mark_blocked(shared: &Arc<SharedState>, run_id: RunId, error: String) {
        let mut inner = shared.state.inner.lock().await;
        if let Some(run) = inner.runs.get_mut(&run_id) {
            run.push_state = Some(PushState {
                status: PushStatus::Blocked,
                error: Some(error),
                pr_number: None,
            });
        }
    }

    let (owner, _) = repository
        .split_once('/')
        .expect("validated above to contain one slash");
    let branch = git_ref
        .strip_prefix("refs/heads/")
        .expect("validated above to be a branch ref");

    let token = match push_token(shared, &repository).await {
        Some(token) => token,
        None => {
            let message =
                "no GitHub credentials configured (GitHub App or PRELOOP_GITHUB_TOKEN)".to_owned();
            mark_blocked(shared, run_id, message.clone()).await;
            return Err(ApiError::bad_request(message));
        }
    };

    // 1. The tested tree must be the pushed tree. A 404 here means the
    //    client never pushed the commit (or pushed something else).
    let commit =
        match github_json(&token, &repository, "GET", &format!("commits/{sha}"), None).await {
            Ok(commit) => commit,
            Err(error) => {
                let message = format!("commit {sha} not found on GitHub: {error}");
                mark_blocked(shared, run_id, message.clone()).await;
                return Err(classify(&message));
            }
        };
    let pushed_tree = commit
        .get("commit")
        .and_then(|commit| commit.get("tree"))
        .and_then(|tree| tree.get("sha"))
        .and_then(|sha| sha.as_str())
        .unwrap_or_default();
    if pushed_tree != push_tree {
        let message = format!(
            "tested tree {push_tree} does not match pushed commit tree {pushed_tree}; \
             the branch was not pushed from the tested commit — re-submit after pushing"
        );
        mark_blocked(shared, run_id, message.clone()).await;
        return Err(classify(&message));
    }

    // 2. Default base branch for PR creation: explicit base_ref wins,
    //    otherwise the repository's default branch.
    let base = match &{
        let inner = shared.state.inner.lock().await;
        inner
            .runs
            .get(&run_id)
            .and_then(|run| run.submission.base_ref.clone())
    } {
        Some(base) => base
            .strip_prefix("refs/heads/")
            .map(str::to_owned)
            .unwrap_or_else(|| base.clone()),
        None => {
            let repo_info = match github_json(&token, &repository, "GET", "", None).await {
                Ok(info) => info,
                Err(error) => {
                    let message = format!("could not read repository metadata: {error}");
                    mark_blocked(shared, run_id, message.clone()).await;
                    return Err(classify(&message));
                }
            };
            match repo_info.get("default_branch").and_then(|b| b.as_str()) {
                Some(base) => base.to_owned(),
                None => {
                    let message = "repository metadata has no default_branch".to_owned();
                    mark_blocked(shared, run_id, message.clone()).await;
                    return Err(classify(&message));
                }
            }
        }
    };

    // Authoritative backstop: push-back is for feature branches. The CLI
    // refuses the default branch before pushing; this catches direct API
    // callers and repos whose default differs from what the client knew.
    if branch == base {
        let message = format!(
            "branch {branch} is the repository's default branch; push-back is for \
             feature branches (main stays webhook-driven)"
        );
        mark_blocked(shared, run_id, message.clone()).await;
        return Err(classify(&message));
    }

    // 3. Reuse an open PR for the branch; create one only when asked.
    let head = format!("{owner}:{branch}");
    let pr_number = match github_json(
        &token,
        &repository,
        "GET",
        &format!("pulls?head={head}&state=open"),
        None,
    )
    .await
    {
        Ok(pulls) => pulls
            .as_array()
            .and_then(|list| list.first())
            .and_then(|pr| pr.get("number"))
            .and_then(|number| number.as_u64()),
        Err(error) => {
            let message = format!("could not look up pull requests: {error}");
            mark_blocked(shared, run_id, message.clone()).await;
            return Err(classify(&message));
        }
    };
    let pr_number = if let Some(number) = pr_number {
        Some(number)
    } else if create_pr {
        let body = format!(
            "CI run `{run_id}` completed with `{conclusion}`.\n\n\
             - Head: `{sha}`\n\
             - Tested tree: `{push_tree}`\n\
             - Actor: `{actor}`\n\
             - Details: {}",
            crate::github::run_details_url(run_id)
                .unwrap_or_else(|| "local server (no public URL configured)".to_owned())
        );
        match github_json(
            &token,
            &repository,
            "POST",
            "pulls",
            Some(serde_json::json!({
                "title": branch,
                "head": branch,
                "base": base,
                "body": body,
                "draft": draft_pr,
            })),
        )
        .await
        {
            Ok(pr) => pr.get("number").and_then(|number| number.as_u64()),
            Err(error) => {
                let mut message = format!("could not create pull request: {error}");
                if format!("{error}").contains("status 403") {
                    message.push_str(
                        ". To run CI before creating a pull request, grant the App \
                         `pull_requests: write` (App settings → Permissions → \
                         Pull requests → Read and write)",
                    );
                }
                mark_blocked(shared, run_id, message.clone()).await;
                return Err(classify(&message));
            }
        }
    } else {
        None
    };

    // 4. Report check runs for jobs that never got one (the submit-time
    //    loop may have been skipped or failed). Jobs with an existing check
    //    run were already updated through the normal lifecycle.
    for job_id in jobs.keys() {
        let has_check_run = {
            let inner = shared.state.inner.lock().await;
            inner
                .runs
                .get(&run_id)
                .is_some_and(|run| run.job_check_run_ids.contains_key(job_id))
        };
        if !has_check_run {
            crate::github::report_check_run_queued(shared, &repository, &sha, job_id, run_id).await;
            if jobs.get(job_id).is_some_and(|status| status.is_terminal()) {
                crate::github::report_check_run_completed(shared, run_id, job_id, jobs[job_id])
                    .await;
            }
        }
    }

    let mut inner = shared.state.inner.lock().await;
    if let Some(run) = inner.runs.get_mut(&run_id) {
        run.push_state = Some(PushState {
            status: PushStatus::Synced,
            error: None,
            pr_number,
        });
    }
    drop(inner);

    Ok(SyncResponse {
        status: "pushed",
        pr_number,
        pr_url: pr_number
            .map(|number| pr_web_url(&crate::github::github_api_base(), &repository, number)),
    })
}

/// A sync target must be a real GitHub branch at a real commit: a local-only
/// repository or an unpushed SHA can never produce a PR or honest checks.
pub(crate) fn validate_push_target(
    repository: &str,
    sha: &str,
    git_ref: &str,
    push_tree: &str,
) -> Result<(), ApiError> {
    let (owner, repo) = repository.split_once('/').ok_or_else(|| {
        ApiError::bad_request(format!(
            "--push requires a GitHub repository in git origin (got `{repository}`)"
        ))
    })?;
    if owner.is_empty() || repo.is_empty() || repo.contains('/') {
        return Err(ApiError::bad_request(format!(
            "--push requires a GitHub repository in git origin (got `{repository}`)"
        )));
    }
    if !git_ref.starts_with("refs/heads/") {
        return Err(ApiError::bad_request(format!(
            "--push supports branch refs only (got `{git_ref}`)"
        )));
    }
    if !(sha.len() == 40 && sha.chars().all(|c| c.is_ascii_hexdigit()) && sha != ZERO_SHA) {
        return Err(ApiError::bad_request(
            "--push requires a committed HEAD (submit from a git checkout)",
        ));
    }
    if push_tree.len() != 40 || !push_tree.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ApiError::bad_request("invalid tested tree recorded"));
    }
    Ok(())
}

const ZERO_SHA: &str = "0000000000000000000000000000000000000000";

/// Installation token covering everything the sync touches, or the ambient
/// `PRELOOP_GITHUB_TOKEN` when no App is configured.
async fn push_token(shared: &Arc<SharedState>, repository: &str) -> Option<String> {
    if let Some(app_creds) = &shared.state.github_app {
        let permissions = std::collections::BTreeMap::from([
            ("checks".to_owned(), "write".to_owned()),
            ("pull_requests".to_owned(), "write".to_owned()),
            ("contents".to_owned(), "read".to_owned()),
        ]);
        match crate::github_app::get_or_mint_token(app_creds, repository, &permissions).await {
            Ok(token) => return Some(token),
            Err(error) => tracing::warn!(%repository, %error, "push token mint failed"),
        }
    }
    std::env::var("PRELOOP_GITHUB_TOKEN").ok()
}

/// One GitHub REST call returning the parsed JSON body. Errors carry the
/// HTTP status so [`classify`] can tell user mistakes from outages.
async fn github_json(
    token: &str,
    repository: &str,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
) -> anyhow::Result<serde_json::Value> {
    let client = crate::shared_http::CLIENT.clone();
    let url = format!(
        "{}/repos/{}{}",
        crate::github::github_api_base(),
        repository,
        if path.is_empty() {
            String::new()
        } else {
            format!("/{path}")
        }
    );
    let request = client
        .request(
            reqwest::Method::from_bytes(method.as_bytes()).expect("fixed methods"),
            &url,
        )
        .header("User-Agent", "preloop")
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/vnd.github+json");
    let request = match body {
        Some(body) => request.json(&body),
        None => request,
    };
    let response = request.send().await?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow::anyhow!(
            "GitHub API {} {path} failed with status {}: {}",
            method,
            status.as_u16(),
            text
        ));
    }
    serde_json::from_str(&text)
        .map_err(|error| anyhow::anyhow!("unparsable GitHub response: {error}"))
}

/// Map an upstream failure to the right HTTP answer: GitHub being down
/// (5xx, transport) is a gateway failure; everything else — a missing
/// commit, a divergent branch, a tree mismatch — is the client's problem
/// and reads better as a 4xx.
fn classify(message: &str) -> ApiError {
    let text = message.to_lowercase();
    let transient = [
        "status 5",
        "connection",
        "timeout",
        "temporary failure",
        "dns",
    ]
    .iter()
    .any(|needle| text.contains(needle));
    if transient {
        ApiError::bad_gateway(message.to_owned())
    } else {
        ApiError::bad_request(message.to_owned())
    }
}

fn pr_web_url(api_base: &str, repository: &str, number: u64) -> String {
    let host = api_base
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/');
    let host = host.strip_prefix("api.").unwrap_or(host);
    format!("https://{host}/{repository}/pull/{number}")
}
