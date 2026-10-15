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

/// The remote reusable workflow a queued workflow was fetched from: its
/// `owner/repo/path@ref` key and the commit it is pinned to. `None` marks the
/// submitted repository.
type RemoteOrigin = Option<(String, String)>;

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
    // Each queued workflow carries the remote reference it was fetched from and
    // the commit that reference is pinned to. `None` marks the submitted
    // repository (the root and its same-repository callees), which is pinned to
    // the submitted commit.
    let mut queue: Vec<(preloop_gha_parser::Workflow, usize, RemoteOrigin)> =
        vec![(root_workflow.clone(), 0, None)];
    let mut visited = std::collections::BTreeSet::new();
    while let Some((workflow, depth, origin)) = queue.pop() {
        if depth >= MAX_REUSABLE_WORKFLOW_DEPTH {
            return Err(ApiError::bad_request(
                "nested reusable workflow depth exceeded",
            ));
        }
        for job in workflow.jobs.values() {
            let Some(declared) = job.uses.as_deref() else {
                continue;
            };
            // A same-repository reference inside a fetched workflow names that
            // workflow's repository at its pinned commit, not the submission's.
            let reference = match &origin {
                Some((key, pin)) if preloop_gha_parser::is_local_workflow_ref(declared) => {
                    preloop_gha_parser::scope_same_repository_ref(declared, key, pin).ok_or_else(
                        || {
                            ApiError::bad_request(format!(
                                "unsupported reusable workflow reference `{declared}`"
                            ))
                        },
                    )?
                }
                _ => declared.to_owned(),
            };
            let same_repository = preloop_gha_parser::is_local_workflow_ref(&reference);
            // A same-repository reference is validated before anything is looked
            // up or requested: its path must be a plain reusable workflow path.
            let same_repository_path = if same_repository {
                Some(
                    preloop_gha_parser::same_repository_path(&reference).ok_or_else(|| {
                        ApiError::bad_request(format!(
                            "reusable workflow reference `{reference}` must name a plain path under `.github/workflows/`"
                        ))
                    })?,
                )
            } else {
                None
            };
            let supplied = match same_repository_path {
                Some(path) => submission
                    .reusable_workflows
                    .get(&reference)
                    .or_else(|| submission.reusable_workflows.get(path)),
                None => submission.reusable_workflows.get(&reference),
            };
            if let Some(contents) = supplied.cloned() {
                if visited.insert(reference.clone()) {
                    let next = (!same_repository).then(|| {
                        (
                            reference.clone(),
                            pinned_commit(&reference, &submission.reusable_workflow_shas),
                        )
                    });
                    queue.push((
                        preloop_gha_parser::parse_workflow(&contents)?,
                        depth + 1,
                        next,
                    ));
                }
                continue;
            }
            // A same-repository reference names the submitted repository at the
            // submitted commit; any other reference names its own repository.
            let (owner, repo, path, git_ref) = if let Some(path) = same_repository_path {
                let (owner, repo) = submission
                    .repository
                    .split_once('/')
                    .filter(|(owner, repo)| {
                        preloop_gha_parser::is_plain_segment(owner)
                            && preloop_gha_parser::is_plain_segment(repo)
                    })
                    .ok_or_else(|| {
                        ApiError::bad_request(format!(
                            "reusable workflow `{reference}` is in the submission repository, but `{}` is not a plain `owner/repo`",
                            submission.repository
                        ))
                    })?;
                let git_ref = submitted_git_ref(submission);
                if !preloop_gha_parser::is_plain_path(&git_ref) {
                    return Err(ApiError::bad_request(format!(
                        "reusable workflow `{reference}` cannot be fetched at the submitted ref `{git_ref}`"
                    )));
                }
                (owner.to_owned(), repo.to_owned(), path.to_owned(), git_ref)
            } else {
                let Some((owner, repo, path, git_ref)) = parse_remote_reference(&reference) else {
                    return Err(ApiError::bad_request(format!(
                        "unsupported reusable workflow reference `{reference}`"
                    )));
                };
                (
                    owner.to_owned(),
                    repo.to_owned(),
                    path.to_owned(),
                    git_ref.to_owned(),
                )
            };
            if !visited.insert(reference.clone()) {
                continue;
            }
            let client = client.get_or_insert(
                crate::shared_http::github_client_builder()
                    .build()
                    .map_err(|error| ApiError::internal(format!("build GitHub client: {error}")))?,
            );
            let bucket = if token.is_some() {
                crate::github_breaker::GithubCredentialBucket::Pat
            } else {
                crate::github_breaker::GithubCredentialBucket::Anonymous
            };
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
            let sha = commit.sha;
            submission
                .reusable_workflows
                .insert(reference.clone(), contents.clone());
            submission
                .reusable_workflow_shas
                .insert(reference.clone(), sha.clone());
            let next = (!same_repository).then_some((reference, sha));
            queue.push((
                preloop_gha_parser::parse_workflow(&contents)?,
                depth + 1,
                next,
            ));
        }
    }
    Ok(())
}

/// The ref a submission's same-repository workflows are fetched at: the
/// submitted commit when it is a real SHA. Scheduled runs carry no commit, so
/// the submitted branch or tag is used instead; the commit lookup pins it to a
/// SHA before any content is fetched.
fn submitted_git_ref(submission: &WorkflowSubmission) -> String {
    if preloop_gha_protocol::git_ref::is_commit_sha_not_zero(&submission.sha) {
        return submission.sha.clone();
    }
    submission
        .git_ref
        .trim_start_matches("refs/heads/")
        .trim_start_matches("refs/tags/")
        .to_owned()
}

/// The commit a reusable workflow already resolved to, or the ref it was
/// written with when no commit was recorded for it.
fn pinned_commit(reference: &str, shas: &BTreeMap<String, String>) -> String {
    shas.get(reference).cloned().unwrap_or_else(|| {
        reference
            .rsplit_once('@')
            .map_or_else(String::new, |(_, git_ref)| git_ref.to_owned())
    })
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
    if !preloop_gha_parser::is_plain_segment(owner)
        || !preloop_gha_parser::is_plain_segment(repo)
        || !preloop_gha_parser::is_reusable_workflow_path(path)
        || !preloop_gha_parser::is_plain_path(git_ref)
    {
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
            "/{*path}",
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

#[cfg(test)]
mod same_repository_tests {
    use super::*;
    use base64::Engine;
    use std::sync::Arc;

    /// A GitHub stand-in that answers each `(path and query, body)` route,
    /// returns 404 for anything else, and records every request target it gets.
    async fn serve_github_recording(
        routes: Vec<(String, String)>,
    ) -> (String, Arc<parking_lot::Mutex<Vec<String>>>) {
        let routes = Arc::new(routes);
        let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let recorded = seen.clone();
        let app = axum::Router::new().fallback(move |request: axum::extract::Request| {
            let routes = routes.clone();
            let recorded = recorded.clone();
            async move {
                let target = request
                    .uri()
                    .path_and_query()
                    .map_or_else(String::new, |path| path.as_str().to_owned());
                recorded.lock().push(target.clone());
                let (status, body) = match routes.iter().find(|(route, _)| *route == target) {
                    Some((_, body)) => (axum::http::StatusCode::OK, body.clone()),
                    None => (axum::http::StatusCode::NOT_FOUND, String::new()),
                };
                axum::http::Response::builder()
                    .status(status)
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body))
                    .unwrap()
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (api, seen)
    }

    /// A GitHub stand-in that answers each `(path and query, body)` route and
    /// returns 404 for anything else, so an unexpected fetch fails resolution.
    async fn serve_github(routes: Vec<(String, String)>) -> String {
        serve_github_recording(routes).await.0
    }

    fn contents_json(text: &str) -> String {
        serde_json::json!({
            "content": BASE64_STANDARD.encode(text.as_bytes()),
            "encoding": "base64",
        })
        .to_string()
    }

    fn commit_json(sha: &str) -> String {
        serde_json::json!({ "sha": sha }).to_string()
    }

    async fn test_shared() -> (tempfile::TempDir, Arc<SharedState>) {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().join("state").to_path_buf())
            .await
            .unwrap();
        let shared = Arc::new(SharedState {
            state,
            shutdown: tokio_util::sync::CancellationToken::new(),
        });
        (temp, shared)
    }

    /// Webhook and scheduled submissions arrive with an empty reusable-workflow
    /// map. A `$/` callee is therefore fetched from the submitted repository at
    /// the submitted commit, so expansion finds it.
    #[tokio::test]
    async fn same_repository_callee_is_fetched_at_the_submitted_commit() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let sha = "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4";
        let callee = "on: workflow_call\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo build\n";
        let api = serve_github(vec![
            (format!("/repos/o/r/commits/{sha}"), commit_json(sha)),
            (
                format!("/repos/o/r/contents/.github/workflows/build.yml?ref={sha}"),
                contents_json(callee),
            ),
        ])
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);
        let (_temp, shared) = test_shared().await;

        let yaml = "on: push\njobs:\n  call:\n    uses: $/.github/workflows/build.yml\n";
        let workflow = preloop_gha_parser::parse_workflow(yaml).unwrap();
        let mut submission = preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: yaml.to_owned(),
            repository: "o/r".to_owned(),
            git_ref: "refs/heads/main".to_owned(),
            sha: sha.to_owned(),
            ..Default::default()
        };
        resolve_remote_workflows(&shared, &mut submission, &workflow, None)
            .await
            .expect("the submitted repository's callee resolves");

        assert_eq!(
            submission.reusable_workflows["$/.github/workflows/build.yml"],
            callee
        );
        assert_eq!(
            submission.reusable_workflow_shas["$/.github/workflows/build.yml"],
            sha
        );
        preloop_gha_parser::expand_jobs_with_reusables_and_shas(
            &workflow,
            &submission.reusable_workflows,
            &submission.reusable_workflow_shas,
        )
        .expect("the fetched callee expands");
    }

    /// A same-repository reference inside a remote reusable workflow names that
    /// workflow's repository at the commit it was fetched from. The nested file
    /// is fetched there and stored under that identity, never under the
    /// submitted tree's path.
    #[tokio::test]
    async fn nested_same_repository_call_resolves_in_the_callee_repository() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let pinned = "b2c3d4e5b2c3d4e5b2c3d4e5b2c3d4e5b2c3d4e5";
        let outer =
            "on: workflow_call\njobs:\n  nested:\n    uses: $/.github/workflows/inner.yml\n";
        let inner = "on: workflow_call\njobs:\n  leaf:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo inner\n";
        let api = serve_github(vec![
            (
                "/repos/octo/shared/commits/v1".to_owned(),
                commit_json(pinned),
            ),
            (
                format!("/repos/octo/shared/contents/.github/workflows/outer.yml?ref={pinned}"),
                contents_json(outer),
            ),
            (
                format!("/repos/octo/shared/commits/{pinned}"),
                commit_json(pinned),
            ),
            (
                format!("/repos/octo/shared/contents/.github/workflows/inner.yml?ref={pinned}"),
                contents_json(inner),
            ),
        ])
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);
        let (_temp, shared) = test_shared().await;

        let yaml =
            "on: push\njobs:\n  call:\n    uses: octo/shared/.github/workflows/outer.yml@v1\n";
        let workflow = preloop_gha_parser::parse_workflow(yaml).unwrap();
        let mut submission = preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: yaml.to_owned(),
            repository: "o/r".to_owned(),
            git_ref: "refs/heads/main".to_owned(),
            sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            ..Default::default()
        };
        resolve_remote_workflows(&shared, &mut submission, &workflow, None)
            .await
            .expect("the nested same-repository call resolves");

        let nested_key = format!("octo/shared/.github/workflows/inner.yml@{pinned}");
        assert_eq!(submission.reusable_workflows[&nested_key], inner);
        assert_eq!(submission.reusable_workflow_shas[&nested_key], pinned);
        assert!(
            !submission
                .reusable_workflows
                .contains_key("$/.github/workflows/inner.yml")
        );
        // Expansion walks the same identity, so it finds the nested callee.
        preloop_gha_parser::expand_jobs_with_reusables_and_shas(
            &workflow,
            &submission.reusable_workflows,
            &submission.reusable_workflow_shas,
        )
        .expect("the nested callee is found under its callee-repository key");
    }

    /// A same-repository callee the submitter supplied under its normalized
    /// path is used as is: no forge fetch, so the submission resolves even when
    /// the forge is unreachable.
    #[tokio::test]
    async fn supplied_same_repository_callee_is_used_without_a_fetch() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        // A closed port: any fetch would fail the resolution.
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", "http://127.0.0.1:1");
        let (_temp, shared) = test_shared().await;
        let callee = "on: workflow_call\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo build\n";
        let yaml = "on: push\njobs:\n  call:\n    uses: ./.github/workflows/build.yml\n";
        let workflow = preloop_gha_parser::parse_workflow(yaml).unwrap();
        let mut submission = preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: yaml.to_owned(),
            repository: "o/r".to_owned(),
            reusable_workflows: std::collections::BTreeMap::from([(
                ".github/workflows/build.yml".to_owned(),
                callee.to_owned(),
            )]),
            ..Default::default()
        };
        resolve_remote_workflows(&shared, &mut submission, &workflow, None)
            .await
            .expect("a supplied callee needs no fetch");
        assert_eq!(submission.reusable_workflows.len(), 1);
        assert!(submission.reusable_workflow_shas.is_empty());
    }

    /// Scheduled runs carry no commit, so their same-repository callees are
    /// fetched at the submitted branch and pinned by the commit lookup.
    #[test]
    fn same_repository_refs_fall_back_to_the_branch_without_a_commit() {
        let scheduled = preloop_gha_protocol::WorkflowSubmission {
            git_ref: "refs/heads/main".to_owned(),
            sha: "0".repeat(40),
            ..Default::default()
        };
        assert_eq!(submitted_git_ref(&scheduled), "main");
        let pushed = preloop_gha_protocol::WorkflowSubmission {
            git_ref: "refs/heads/main".to_owned(),
            sha: "a".repeat(40),
            ..Default::default()
        };
        assert_eq!(submitted_git_ref(&pushed), "a".repeat(40));
    }

    /// The engine token is sent only to the submitted repository's own
    /// endpoints. A top-level `$/` reference that climbs out of the repository
    /// with `..` must be refused before any request is built. The stand-in also
    /// serves the endpoint the traversal reaches, so an unchecked path succeeds.
    #[tokio::test]
    async fn top_level_same_repository_path_cannot_leave_the_repository() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let sha = "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4";
        let other = "on: workflow_call\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo other\n";
        let (api, seen) = serve_github_recording(vec![
            (format!("/repos/o/r/commits/{sha}"), commit_json(sha)),
            // Where the traversal lands once the URL parser removes the dot segments.
            (
                format!("/repos/other/private/contents/.github/workflows/x.yml?ref={sha}"),
                contents_json(other),
            ),
        ])
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);
        let (_temp, shared) = test_shared().await;

        let yaml = "on: push\njobs:\n  call:\n    uses: $/../../../../repos/other/private/contents/.github/workflows/x.yml\n";
        let workflow = preloop_gha_parser::parse_workflow(yaml).unwrap();
        let mut submission = preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: yaml.to_owned(),
            repository: "o/r".to_owned(),
            git_ref: "refs/heads/main".to_owned(),
            sha: sha.to_owned(),
            ..Default::default()
        };
        let outcome = resolve_remote_workflows(&shared, &mut submission, &workflow, None).await;
        let requests = seen.lock().to_vec();
        assert!(
            requests.is_empty(),
            "a refused path must not build a request, but the server sent {requests:?}"
        );
        let error = outcome.expect_err("a path that leaves the submitted repository is refused");
        assert!(
            error.message().contains("plain path"),
            "the refusal must say why, got: {}",
            error.message()
        );
    }

    /// Every same-repository spelling a URL parser could reinterpret is refused
    /// before a request is built: `.` and `..` segments, empty segments, absolute
    /// paths, backslashes, percent-escapes, query and fragment delimiters,
    /// whitespace, and paths outside `.github/workflows/`.
    #[tokio::test]
    async fn top_level_same_repository_path_must_be_plain() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let sha = "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4";
        let (api, seen) = serve_github_recording(vec![(
            format!("/repos/o/r/commits/{sha}"),
            commit_json(sha),
        )])
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);
        let (_temp, shared) = test_shared().await;
        for uses in [
            "$/.github/workflows/../../x.yml",
            "$/../.github/workflows/x.yml",
            "$/.github/workflows/..\\..\\x.yml",
            "$/.github\\workflows\\x.yml",
            "$/%2e%2e/%2e%2e/x.yml",
            "$/.github/workflows/%2e%2e/x.yml",
            "$//.github/workflows/x.yml",
            "$/.github/workflows//x.yml",
            "$/.github/workflows/x.yml/",
            "$/.github/workflows/./x.yml",
            "$/.github/workflows/x.yml?ref=main",
            "$/.github/workflows/x.yml#frag",
            "$/.github/workflows/a b.yml",
            "$/x.yml",
            "./../x.yml",
        ] {
            seen.lock().clear();
            let yaml = format!("on: push\njobs:\n  call:\n    uses: {uses}\n");
            let workflow = preloop_gha_parser::parse_workflow(&yaml).expect("the workflow parses");
            let mut submission = preloop_gha_protocol::WorkflowSubmission {
                workflow_yaml: yaml.clone(),
                repository: "o/r".to_owned(),
                git_ref: "refs/heads/main".to_owned(),
                sha: sha.to_owned(),
                ..Default::default()
            };
            let outcome = resolve_remote_workflows(&shared, &mut submission, &workflow, None).await;
            let requests = seen.lock().to_vec();
            assert!(requests.is_empty(), "`{uses}` built requests: {requests:?}");
            assert!(outcome.is_err(), "`{uses}` must be refused");
        }
    }

    /// Remote references build the same contents URLs, so the owner, repository,
    /// path and ref must each be plain: a `..` in any of them would escape the
    /// named repository.
    #[test]
    fn remote_reference_components_must_be_plain() {
        assert!(parse_remote_reference("octo/demo/.github/workflows/build.yml@v1").is_some());
        for reference in [
            "octo/demo/.github/workflows/../../../../repos/other/private/contents/x.yml@v1",
            "../demo/.github/workflows/build.yml@v1",
            "octo/../.github/workflows/build.yml@v1",
            "octo/demo/.github/workflows/build.yml@../../repos/other/commits/main",
            "octo/demo/.github/workflows/build.yml@",
        ] {
            assert!(
                parse_remote_reference(reference).is_none(),
                "`{reference}` must be refused"
            );
        }
    }

    /// A top-level `$/` or `./` reference resolves only when it names a file under
    /// `.github/workflows/`, and that file is fetched from the submitted repository
    /// at the submitted commit. Every other spelling is refused before a request.
    #[tokio::test]
    async fn top_level_same_repository_refs_must_name_a_workflow_file() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let sha = "a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4";
        let ci = "on: workflow_call\njobs:\n  ci:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ci\n";
        let (api, seen) = serve_github_recording(vec![
            (format!("/repos/o/r/commits/{sha}"), commit_json(sha)),
            (
                format!("/repos/o/r/contents/.github/workflows/ci.yml?ref={sha}"),
                contents_json(ci),
            ),
        ])
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);
        let (_temp, shared) = test_shared().await;

        let yaml = "on: push\njobs:\n  call:\n    uses: $/.github/workflows/ci.yml\n";
        let workflow = preloop_gha_parser::parse_workflow(yaml).unwrap();
        let mut submission = preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: yaml.to_owned(),
            repository: "o/r".to_owned(),
            git_ref: "refs/heads/main".to_owned(),
            sha: sha.to_owned(),
            ..Default::default()
        };
        resolve_remote_workflows(&shared, &mut submission, &workflow, None)
            .await
            .expect("a workflow file under .github/workflows/ resolves");
        assert_eq!(
            submission.reusable_workflows["$/.github/workflows/ci.yml"],
            ci
        );

        for uses in [
            "$/src/x.yml",
            "./../x.yml",
            "$/../../../repos/other/private/contents/.github/workflows/x.yml",
        ] {
            seen.lock().clear();
            let yaml = format!("on: push\njobs:\n  call:\n    uses: {uses}\n");
            let workflow = preloop_gha_parser::parse_workflow(&yaml).expect("the workflow parses");
            let mut submission = preloop_gha_protocol::WorkflowSubmission {
                workflow_yaml: yaml.clone(),
                repository: "o/r".to_owned(),
                git_ref: "refs/heads/main".to_owned(),
                sha: sha.to_owned(),
                ..Default::default()
            };
            let outcome = resolve_remote_workflows(&shared, &mut submission, &workflow, None).await;
            let requests = seen.lock().to_vec();
            assert!(requests.is_empty(), "`{uses}` built requests: {requests:?}");
            assert!(outcome.is_err(), "`{uses}` must be refused");
        }
    }
}
