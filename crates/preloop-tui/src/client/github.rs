//! GitHub access through the `gh` CLI, for the stack view.
//!
//! Using `gh api graphql` keeps credentials in the user's existing `gh` login.
//! The TUI never sees a GitHub token. Fetching is performed by the runtime;
//! renderers only consume the parsed snapshot.

use anyhow::{Context as _, bail};
use serde_json::Value;

use crate::api::{CheckSummary, StackPullRequest, StackRepository, StackSnapshot};
use crate::config::StackConfig;

const STACK_QUERY: &str = r#"query($owner:String!,$name:String!) {
  repository(owner:$owner,name:$name) {
    nameWithOwner
    defaultBranchRef { name }
    pullRequests(first:100, states:[OPEN,CLOSED,MERGED], orderBy:{field:UPDATED_AT,direction:DESC}) {
      nodes {
        number title url isDraft state mergeable
        author { login }
        baseRefName headRefName baseRefOid headRefOid
        commits(last:1) { nodes { commit { statusCheckRollup {
          contexts(first:100) { nodes {
            __typename
            ... on CheckRun { name status conclusion }
            ... on StatusContext { context state }
          }}
        }}}}
      }
    }
  }
}"#;

/// Run a GraphQL query with `gh api graphql` and return the parsed response.
///
/// `variables` are sent as string fields (`-f name=value`).
pub async fn graphql(query: &str, variables: &[(&str, &str)]) -> anyhow::Result<Value> {
    let mut command = tokio::process::Command::new("gh");
    command
        .arg("api")
        .arg("graphql")
        .arg("-f")
        .arg(format!("query={query}"));
    for (name, value) in variables {
        command.arg("-f").arg(format!("{name}={value}"));
    }
    let output = command
        .output()
        .await
        .context("running `gh api graphql` (is the GitHub CLI installed?)")?;
    if !output.status.success() {
        bail!(
            "`gh api graphql` failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    serde_json::from_slice(&output.stdout).context("parsing `gh api graphql` output")
}

/// Fetch all configured repositories. Individual failures stay attached to
/// their repository so one unavailable repo does not hide the others.
pub async fn stack(config: &StackConfig) -> anyhow::Result<StackSnapshot> {
    let mut snapshot = StackSnapshot::default();
    for configured in &config.repos {
        let mut repository = StackRepository {
            name: configured.clone(),
            ..Default::default()
        };
        let Some((owner, name)) = configured.split_once('/') else {
            repository.error = Some("repository must be owner/name".to_owned());
            snapshot.repositories.push(repository);
            continue;
        };
        let result = graphql(STACK_QUERY, &[("owner", owner), ("name", name)]).await;
        match result.and_then(|value| parse_repository(&value, configured)) {
            Ok(mut parsed) => {
                if let Some(author) = config.author.as_deref() {
                    parsed.pull_requests.retain(|pull_request| {
                        pull_request
                            .author
                            .as_deref()
                            .is_some_and(|value| value.eq_ignore_ascii_case(author))
                    });
                }
                snapshot.repositories.push(parsed);
            }
            Err(error) => {
                repository.error = Some(format!("{error:#}"));
                snapshot.repositories.push(repository);
            }
        }
    }
    Ok(snapshot)
}

/// Parse one GraphQL response. This is public for fixture-driven tests and for
/// callers that provide a cached response.
pub fn parse_repository(value: &Value, configured_name: &str) -> anyhow::Result<StackRepository> {
    if let Some(errors) = value.get("errors") {
        bail!("GitHub GraphQL error: {errors}");
    }
    let repository = value
        .get("data")
        .and_then(|data| data.get("repository"))
        .filter(|value| !value.is_null())
        .context("repository not found")?;
    let name = repository
        .get("nameWithOwner")
        .and_then(Value::as_str)
        .unwrap_or(configured_name)
        .to_owned();
    let default_branch = repository
        .get("defaultBranchRef")
        .and_then(|value| value.get("name"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let mut pull_requests = Vec::new();
    for node in repository
        .get("pullRequests")
        .and_then(|value| value.get("nodes"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(number) = node.get("number").and_then(Value::as_u64) else {
            continue;
        };
        let checks = node
            .get("commits")
            .and_then(|value| value.get("nodes"))
            .and_then(Value::as_array)
            .and_then(|nodes| nodes.first())
            .and_then(|value| value.get("commit"))
            .and_then(|value| value.get("statusCheckRollup"))
            .map(parse_checks)
            .unwrap_or_default();
        pull_requests.push(StackPullRequest {
            repository: name.clone(),
            number,
            title: string(node, "title"),
            url: string(node, "url"),
            author: node
                .get("author")
                .and_then(|value| value.get("login"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            base: string(node, "baseRefName"),
            head: string(node, "headRefName"),
            base_sha: optional_string(node, "baseRefOid"),
            head_sha: optional_string(node, "headRefOid"),
            draft: node
                .get("isDraft")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            state: string(node, "state"),
            mergeable: optional_string(node, "mergeable"),
            checks,
        });
    }
    Ok(StackRepository {
        name,
        default_branch,
        pull_requests,
        error: None,
    })
}

fn parse_checks(rollup: &Value) -> CheckSummary {
    let mut summary = CheckSummary::default();
    for node in rollup
        .get("contexts")
        .and_then(|value| value.get("nodes"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        summary.total += 1;
        let status = node.get("conclusion").or_else(|| node.get("state"));
        match status
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_ascii_uppercase()
            .as_str()
        {
            "SUCCESS" => summary.passing += 1,
            "FAILURE" | "ERROR" | "CANCELLED" | "TIMED_OUT" | "ACTION_REQUIRED" => {
                summary.failing += 1
            }
            _ => summary.pending += 1,
        }
    }
    summary
}

fn string(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("not loaded")
        .to_owned()
}

fn optional_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fixture_and_check_summary() {
        let value: Value =
            serde_json::from_str(include_str!("../../tests/fixtures/stack_github.json")).unwrap();
        let repo = parse_repository(&value, "acme/widgets").unwrap();
        assert_eq!(repo.name, "acme/widgets");
        assert_eq!(repo.pull_requests.len(), 2);
        assert_eq!(repo.pull_requests[0].number, 42);
        assert_eq!(repo.pull_requests[0].checks.passing, 1);
        assert_eq!(repo.pull_requests[0].checks.pending, 1);
        assert!(repo.pull_requests[1].draft);
    }
}
