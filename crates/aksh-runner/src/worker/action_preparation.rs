//! Remote action download and reference resolution.

use anyhow::Result;
use tracing::warn;

use super::helpers::extract_service_endpoint;
use super::steps_runner::{Step, StepType};
use crate::client::http::HttpClient;

pub(crate) async fn prepare_remote_actions(
    job_message: &serde_json::Value,
    workspace: &str,
    steps: &[Step],
    plan_id: &str,
) -> Result<std::collections::HashMap<String, String>> {
    let mut refs = Vec::new();
    for step in steps {
        let StepType::Action { uses, .. } = &step.step_type else {
            continue;
        };
        if uses.starts_with("./") || uses.starts_with("../") || uses.starts_with("docker://") {
            continue;
        }
        if let Some(parsed) = parse_remote_uses(uses) {
            refs.push((uses.clone(), parsed));
        } else {
            warn!("Cannot parse remote action ref (missing @version?): {uses:?}");
        }
    }

    if refs.is_empty() {
        return Ok(std::collections::HashMap::new());
    }

    let job_id = job_message
        .get("jobId")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let access_token = extract_service_endpoint(job_message)
        .map(|(_, token)| token)
        .unwrap_or_default();
    let launch_url =
        message_variable(job_message, "system.github.launch_endpoint").map(str::to_string);

    let http = HttpClient::new(None)?;
    let resolver = crate::client::actions_download::ActionsResolveClient::new(http, launch_url);
    let action_pairs: Vec<(String, String)> = refs
        .iter()
        .map(|(_, parsed)| (parsed.action_name.clone(), parsed.git_ref.clone()))
        .collect();
    let action_pair_refs: Vec<(&str, &str)> = action_pairs
        .iter()
        .map(|(action, version)| (action.as_str(), version.as_str()))
        .collect();
    let resolved = if !access_token.is_empty() {
        resolver
            .resolve_batch(&access_token, plan_id, job_id, &action_pair_refs)
            .await?
    } else {
        std::collections::HashMap::new()
    };

    let actions_dir = std::path::Path::new(workspace)
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .join("_actions");
    let mut action_paths = std::collections::HashMap::new();

    for (uses, parsed) in refs {
        let key = format!("{}@{}", parsed.action_name, parsed.git_ref);
        let meta = resolved.get(&key);
        let dir_ref = meta
            .map(|m| m.resolved_sha.as_str())
            .filter(|sha| !sha.is_empty())
            .unwrap_or(parsed.git_ref.as_str());
        let download_url = meta
            .map(|m| m.tar_url.as_str())
            .filter(|url| !url.is_empty());
        let auth_token = meta.and_then(|m| m.auth_token.as_deref());

        let action_root = super::actions::manager::download_action(
            &parsed.owner,
            &parsed.repo,
            dir_ref,
            &actions_dir,
            download_url,
            auth_token,
        )
        .await?;

        let action_dir = if parsed.subpath.is_empty() {
            action_root
        } else {
            action_root.join(&parsed.subpath)
        };
        action_paths.insert(uses, action_dir.to_string_lossy().to_string());
    }

    Ok(action_paths)
}

pub(crate) struct ParsedUses {
    pub(crate) owner: String,
    pub(crate) repo: String,
    pub(crate) subpath: String,
    pub(crate) git_ref: String,
    pub(crate) action_name: String,
}

pub(crate) fn parse_remote_uses(uses: &str) -> Option<ParsedUses> {
    let (repo_part, git_ref) = uses.split_once('@')?;
    let mut parts = repo_part.split('/');
    let owner = parts.next()?.to_string();
    let repo = parts.next()?.to_string();
    let rest: Vec<&str> = parts.collect();
    let subpath = rest.join("/");
    Some(ParsedUses {
        owner: owner.clone(),
        repo: repo.clone(),
        subpath,
        git_ref: git_ref.to_string(),
        action_name: format!("{owner}/{repo}"),
    })
}

pub(crate) fn message_variable<'a>(
    job_message: &'a serde_json::Value,
    key: &str,
) -> Option<&'a str> {
    job_message
        .get("variables")
        .and_then(|v| v.get(key))
        .and_then(|v| v.get("value"))
        .and_then(|v| v.as_str())
}
