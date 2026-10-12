use super::*;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::Deserialize;

const MAX_REUSABLE_WORKFLOW_DEPTH: usize = 4;

#[derive(Debug, Deserialize)]
struct GithubContentResponse {
    content: String,
    encoding: String,
}

#[derive(Debug, Deserialize)]
struct GithubCommitResponse {
    sha: String,
}

/// Populate the submission's reusable-workflow table with remote references.
///
/// GitHub resolves these references before handing the expanded workflow to
/// the runner. The server performs the equivalent resolution for references
/// not already supplied by a caller (local workflows and test fixtures).
///
/// `github_token` is the engine's static credential (env `PRELOOP_GITHUB_TOKEN`
/// or the config file's `github.pat` — the caller resolves the precedence),
/// attached as a bearer header so private `uses: owner/repo/...` references
/// resolve without a separately exported token. Both fetches go through the
/// labeled GitHub sender under the PAT bucket (or the anonymous bucket when
/// no token is configured), so breaker and consumption accounting see them.
pub async fn resolve_remote_workflows(
    shared: &Arc<SharedState>,
    submission: &mut WorkflowSubmission,
    root_workflow: &preloop_gha_parser::Workflow,
    github_token: Option<&str>,
) -> Result<(), ApiError> {
    if !root_workflow.jobs.values().any(|job| job.uses.is_some()) {
        return Ok(());
    }

    let mut client = None;
    let token = github_token;
    // Mirrors `github::fetch_workflows_at`: tests point this at a local mock,
    // and GHES installs can point it at their instance.
    let api_base = std::env::var("PRELOOP_GITHUB_API_URL")
        .unwrap_or_else(|_| "https://api.github.com".to_owned());
    let mut queue = vec![(root_workflow.clone(), 0usize)];
    let mut visited = std::collections::BTreeSet::new();
    while let Some((workflow, depth)) = queue.pop() {
        if depth >= MAX_REUSABLE_WORKFLOW_DEPTH {
            return Err(ApiError::bad_request(
                "nested reusable workflow depth exceeded",
            ));
        }
        for job in workflow.jobs.values() {
            let Some(reference) = job.uses.as_deref() else {
                continue;
            };
            if reference.starts_with("./") {
                continue;
            };
            if let Some(contents) = submission.reusable_workflows.get(reference).cloned() {
                if visited.insert(reference.to_owned()) {
                    queue.push((preloop_gha_parser::parse_workflow(&contents)?, depth + 1));
                }
                continue;
            }
            let Some((owner, repo, path, git_ref)) = parse_remote_reference(reference) else {
                return Err(ApiError::bad_request(format!(
                    "unsupported reusable workflow reference `{reference}`"
                )));
            };
            if !visited.insert(reference.to_owned()) {
                continue;
            }
            let client = client.get_or_insert(
                crate::shared_http::github_client_builder()
                    .build()
                    .map_err(|error| ApiError::internal(format!("build GitHub client: {error}")))?,
            );
            // Resolve the mutable ref to a commit SHA first, then fetch the
            // workflow content pinned to that exact SHA. Fetching content with
            // the mutable ref and resolving the SHA afterwards would let a
            // retargeted branch/tag store content from commit A under the
            // identity of commit B (TOCTOU); `?ref=<sha>` binds both values
            // to the same revision.
            let mut commit_request =
                client.get(format!("{api_base}/repos/{owner}/{repo}/commits/{git_ref}"));
            if let Some(token) = token {
                commit_request = commit_request.bearer_auth(token);
            }
            let bucket = if token.is_some() {
                crate::github_breaker::GithubCredentialBucket::Pat
            } else {
                crate::github_breaker::GithubCredentialBucket::Anonymous
            };
            let commit_response = crate::github_breaker::send_observed_labeled(
                &shared.state.github_breakers,
                bucket,
                &shared.state.github_consumption,
                crate::github_breaker::GithubSubsystem::Workflows,
                commit_request,
            )
            .await
            .map_err(|error| {
                ApiError::bad_gateway(format!(
                    "resolve reusable workflow `{reference}` SHA: {error}"
                ))
            })?;
            let commit_status = commit_response.status();
            if !commit_status.is_success() {
                return Err(ApiError::bad_gateway(format!(
                    "GitHub returned {commit_status} while resolving reusable workflow `{reference}` SHA"
                )));
            }
            let commit: GithubCommitResponse = commit_response.json().await.map_err(|error| {
                ApiError::bad_gateway(format!("read reusable workflow `{reference}` SHA: {error}"))
            })?;
            // The recorded SHA is what later stages trust as this reusable
            // workflow's pinned identity, so a malformed lookup response must
            // not be stored as if it were a commit — and no content is
            // fetched until the identity is validated.
            if !preloop_gha_protocol::git_ref::is_commit_sha_not_zero(&commit.sha) {
                return Err(ApiError::bad_gateway(format!(
                    "reusable workflow `{reference}` resolved to a non-commit SHA: {:?}",
                    commit.sha
                )));
            }
            let mut request = client
                .get(format!(
                    "{api_base}/repos/{owner}/{repo}/contents/{path}?ref={}",
                    commit.sha
                ))
                .header(reqwest::header::ACCEPT, "application/vnd.github+json");
            if let Some(token) = token {
                request = request.bearer_auth(token);
            }
            let response = crate::github_breaker::send_observed_labeled(
                &shared.state.github_breakers,
                bucket,
                &shared.state.github_consumption,
                crate::github_breaker::GithubSubsystem::Workflows,
                request,
            )
            .await
            .map_err(|error| {
                ApiError::bad_gateway(format!("fetch reusable workflow `{reference}`: {error}"))
            })?;
            let status = response.status();
            if !status.is_success() {
                return Err(ApiError::bad_gateway(format!(
                    "GitHub returned {status} for reusable workflow `{reference}`"
                )));
            }
            let content_response: GithubContentResponse =
                response.json().await.map_err(|error| {
                    ApiError::bad_gateway(format!("read reusable workflow `{reference}`: {error}"))
                })?;
            let contents = decode_github_contents(&content_response).map_err(|error| {
                ApiError::bad_gateway(format!("decode reusable workflow `{reference}`: {error}"))
            })?;
            submission
                .reusable_workflows
                .insert(reference.to_owned(), contents.clone());
            submission
                .reusable_workflow_shas
                .insert(reference.to_owned(), commit.sha);
            queue.push((preloop_gha_parser::parse_workflow(&contents)?, depth + 1));
        }
    }
    Ok(())
}

fn decode_github_contents(response: &GithubContentResponse) -> Result<String, String> {
    if response.encoding != "base64" {
        return Err(format!(
            "unsupported GitHub content encoding `{}`",
            response.encoding
        ));
    }
    let encoded = response.content.lines().collect::<String>();
    let bytes = BASE64_STANDARD
        .decode(encoded)
        .map_err(|error| format!("invalid base64 content: {error}"))?;
    String::from_utf8(bytes).map_err(|error| format!("workflow is not UTF-8: {error}"))
}

fn parse_remote_reference(reference: &str) -> Option<(&str, &str, &str, &str)> {
    let (repository_path, git_ref) = reference.rsplit_once('@')?;
    let mut parts = repository_path.splitn(3, '/');
    let owner = parts.next()?;
    let repo = parts.next()?;
    let path = parts.next()?;
    if owner.is_empty() || repo.is_empty() || !path.starts_with(".github/workflows/") {
        return None;
    }
    Some((owner, repo, path, git_ref))
}

#[cfg(test)]
mod tests {
    use super::{GithubContentResponse, decode_github_contents, parse_remote_reference};

    #[test]
    fn parses_remote_workflow_reference() {
        assert_eq!(
            parse_remote_reference("octo/demo/.github/workflows/build.yml@v1"),
            Some(("octo", "demo", ".github/workflows/build.yml", "v1"))
        );
    }

    #[test]
    fn rejects_local_and_non_workflow_references() {
        assert!(parse_remote_reference("./.github/workflows/build.yml").is_none());
        assert!(parse_remote_reference("octo/demo/action.yml@v1").is_none());
        assert!(parse_remote_reference("octo/demo/.github/workflows/build.yml").is_none());
    }

    #[test]
    fn decodes_github_contents_response() {
        let response = GithubContentResponse {
            content: "bmFtZTogQ0kK\n".to_owned(),
            encoding: "base64".to_owned(),
        };
        assert_eq!(decode_github_contents(&response).unwrap(), "name: CI\n");
    }
}

#[cfg(test)]
mod observed_sender_tests {
    use super::*;
    use crate::github_breaker::{GithubCredentialBucket, GithubSubsystem};
    use std::sync::Arc;

    /// The two fetches `resolve_remote_workflows` makes must go through the
    /// labeled sender: the breaker sees them and the `workflows` subsystem
    /// accounts for them. Pre-fix these were direct `.send()` calls with the
    /// static PAT, invisible to both.
    #[tokio::test]
    async fn remote_workflow_fetches_use_the_labeled_sender() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let sha = "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4";
        let reusable_yaml = "on: push\njobs:\n  inner:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
        let content_b64 =
            base64::engine::general_purpose::STANDARD.encode(reusable_yaml.as_bytes());
        let hits = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let hits_route = hits.clone();
        let sha_owned = sha.to_owned();
        let app = axum::Router::new().route(
            "/*path",
            axum::routing::get(move |request: axum::extract::Request| {
                let hits_route = hits_route.clone();
                let sha_owned = sha_owned.clone();
                let content_b64 = content_b64.clone();
                async move {
                    hits_route.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let path = request.uri().path().to_owned();
                    let body = if path.contains("/commits/") {
                        serde_json::json!({ "sha": sha_owned }).to_string()
                    } else {
                        serde_json::json!({
                            "content": content_b64,
                            "encoding": "base64",
                        })
                        .to_string()
                    };
                    axum::http::Response::builder()
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(body))
                        .unwrap()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().join("state").to_path_buf())
            .await
            .unwrap();
        let shared = Arc::new(SharedState {
            state,
            shutdown: tokio_util::sync::CancellationToken::new(),
        });

        let yaml = "on: push\njobs:\n  call:\n    uses: o/r/.github/workflows/reusable.yml@main\n";
        let workflow = preloop_gha_parser::parse_workflow(yaml).unwrap();
        let mut submission = preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: yaml.to_owned(),
            ..Default::default()
        };
        resolve_remote_workflows(&shared, &mut submission, &workflow, None)
            .await
            .expect("resolution must succeed against the stub");
        assert_eq!(
            submission
                .reusable_workflows
                .get("o/r/.github/workflows/reusable.yml@main")
                .map(String::as_str),
            Some(reusable_yaml),
            "the fetched workflow must be stored decoded"
        );
        assert_eq!(
            submission
                .reusable_workflow_shas
                .get("o/r/.github/workflows/reusable.yml@main")
                .map(String::as_str),
            Some(sha),
            "the resolved SHA must be pinned"
        );

        // Both fetches went through the labeled sender: the stub saw them,
        // the anonymous bucket's breaker observed them, and the workflows
        // subsystem accounts for them.
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "commit SHA + contents fetches"
        );
        let usage = shared
            .state
            .github_consumption
            .snapshot()
            .into_iter()
            .find(|usage| usage.subsystem == GithubSubsystem::Workflows.as_str())
            .expect("workflows subsystem must be in the snapshot");
        assert_eq!(usage.requests, 2);
        assert!(
            shared
                .state
                .github_breakers
                .breaker_for(GithubCredentialBucket::Anonymous)
                .acquire()
                .is_ok(),
            "successful fetches must leave the bucket closed"
        );
    }

    /// A throttled PAT bucket fails the submission-time fetch fast instead of
    /// spending the (already exhausted) budget: the error surfaces as a
    /// gateway error, not a hang.
    #[tokio::test]
    async fn throttled_pat_bucket_fails_remote_workflow_fetch_fast() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().join("state").to_path_buf())
            .await
            .unwrap();
        let shared = Arc::new(SharedState {
            state,
            shutdown: tokio_util::sync::CancellationToken::new(),
        });
        // Park the PAT bucket the way a 429 would.
        shared
            .state
            .github_breakers
            .breaker_for(GithubCredentialBucket::Pat)
            .record_failure(
                &crate::github_breaker::GithubFailureKind::RateLimited {
                    retry_after: Some(std::time::Duration::from_secs(60)),
                },
                "429",
            );

        let yaml = "on: push\njobs:\n  call:\n    uses: o/r/.github/workflows/reusable.yml@main\n";
        let workflow = preloop_gha_parser::parse_workflow(yaml).unwrap();
        let mut submission = preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: yaml.to_owned(),
            ..Default::default()
        };
        // A token is configured so the PAT bucket governs (anonymous would
        // be a different, open bucket).
        let error = resolve_remote_workflows(&shared, &mut submission, &workflow, Some("pat-x"))
            .await
            .expect_err("a parked PAT bucket must fail the fetch");
        assert!(
            error.message().contains("circuit breaker is open"),
            "must fail fast on the open breaker, got: {}",
            error.message()
        );
    }
}
