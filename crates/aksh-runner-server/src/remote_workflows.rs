use super::*;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
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
pub(crate) async fn resolve_remote_workflows(
    submission: &mut WorkflowSubmission,
    root_workflow: &aksh_gha_parser::Workflow,
) -> Result<(), ApiError> {
    if !root_workflow.jobs.values().any(|job| job.uses.is_some()) {
        return Ok(());
    }

    let mut client = None;
    let token = std::env::var("AKSH_GITHUB_TOKEN").ok();
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
                    queue.push((aksh_gha_parser::parse_workflow(&contents)?, depth + 1));
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
                reqwest::Client::builder()
                    .user_agent("aksh-runner-server")
                    .build()
                    .map_err(|error| ApiError::internal(format!("build GitHub client: {error}")))?,
            );
            let mut request = client
                .get(format!(
                    "https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={git_ref}"
                ))
                .header(reqwest::header::ACCEPT, "application/vnd.github+json");
            if let Some(token) = token.as_deref() {
                request = request.bearer_auth(token);
            }
            let response = request.send().await.map_err(|error| {
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

            let mut commit_request = client.get(format!(
                "https://api.github.com/repos/{owner}/{repo}/commits/{git_ref}"
            ));
            if let Some(token) = token.as_deref() {
                commit_request = commit_request.bearer_auth(token);
            }
            let commit_response = commit_request.send().await.map_err(|error| {
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
            submission
                .reusable_workflows
                .insert(reference.to_owned(), contents.clone());
            submission
                .reusable_workflow_shas
                .insert(reference.to_owned(), commit.sha);
            queue.push((aksh_gha_parser::parse_workflow(&contents)?, depth + 1));
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
    use super::{decode_github_contents, parse_remote_reference, GithubContentResponse};

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
