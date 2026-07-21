use super::*;

const MAX_REUSABLE_WORKFLOW_DEPTH: usize = 4;

/// Populate the submission's reusable-workflow table with remote references.
///
/// GitHub resolves these references before handing the expanded workflow to
/// the runner. The server performs the equivalent resolution for references
/// not already supplied by a caller (local workflows and test fixtures).
pub(crate) async fn resolve_remote_workflows(
    submission: &mut WorkflowSubmission,
) -> Result<(), ApiError> {
    let client = reqwest::Client::builder()
        .user_agent("aksh-runner-server")
        .build()
        .map_err(|error| ApiError::internal(format!("build GitHub client: {error}")))?;
    let token = std::env::var("AKSH_GITHUB_TOKEN").ok();
    let mut queue = vec![(submission.workflow_yaml.clone(), 0usize)];
    let mut visited = std::collections::BTreeSet::new();
    while let Some((workflow_yaml, depth)) = queue.pop() {
        if depth > MAX_REUSABLE_WORKFLOW_DEPTH {
            return Err(ApiError::bad_request(
                "nested reusable workflow depth exceeded",
            ));
        }
        let workflow = aksh_gha_parser::parse_workflow(&workflow_yaml)?;
        for job in workflow.jobs.values() {
            let Some(reference) = job.uses.as_deref() else {
                continue;
            };
            if reference.starts_with("./") || submission.reusable_workflows.contains_key(reference)
            {
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
            let mut request = client
                .get(format!(
                    "https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={git_ref}"
                ))
                .header(reqwest::header::ACCEPT, "application/vnd.github.raw+json");
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
            let contents = response.text().await.map_err(|error| {
                ApiError::bad_gateway(format!("read reusable workflow `{reference}`: {error}"))
            })?;
            submission
                .reusable_workflows
                .insert(reference.to_owned(), contents.clone());
            queue.push((contents, depth + 1));
        }
    }
    Ok(())
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
    use super::parse_remote_reference;

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
}
