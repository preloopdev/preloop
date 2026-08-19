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
        // v2.336.0 ActionManager.ResolveSelfRepositoryReferences — gated by
        // `actions_self_repository` (Constants.Runner.Features.SelfRepository).
        if uses.starts_with("$/") {
            if !self_repository_enabled(job_message) {
                warn!(
                    "Self-repository reference '{uses}' requires actions_self_repository; leaving unresolved"
                );
                continue;
            }
            let subpath = uses
                .strip_prefix("$/")
                .unwrap_or("")
                .trim_start_matches('/');
            if subpath.is_empty() {
                warn!("Bare $/ without subpath is not valid: {uses:?}");
                continue;
            }
            let workflow_repo =
                message_variable(job_message, "system.github.repository").or_else(|| {
                    job_message
                        .get("contextData")
                        .and_then(|cd| cd.get("github"))
                        .and_then(|g| g.get("repository"))
                        .and_then(|v| v.as_str())
                });
            let workflow_sha = message_variable(job_message, "system.github.sha").or_else(|| {
                job_message
                    .get("contextData")
                    .and_then(|cd| cd.get("github"))
                    .and_then(|g| g.get("sha"))
                    .and_then(|v| v.as_str())
            });
            if let (Some(repo), Some(sha)) = (workflow_repo, workflow_sha) {
                let parts: Vec<&str> = repo.splitn(2, '/').collect();
                if parts.len() == 2 {
                    refs.push((
                        uses.clone(),
                        ParsedUses {
                            owner: parts[0].to_string(),
                            repo: parts[1].to_string(),
                            subpath: subpath.to_string(),
                            git_ref: sha.to_string(),
                            action_name: repo.to_string(),
                        },
                    ));
                } else {
                    warn!("Cannot parse workflow repository for $/ resolution: {repo}");
                }
            } else {
                warn!("Cannot resolve $/ ref: workflow repo/sha not in job message");
            }
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
    use tracing::info;

    let resolved = if !access_token.is_empty() {
        // v2.336.0 (#4536): Log action resolution telemetry
        let start = std::time::Instant::now();
        let result = resolver
            .resolve_batch(&access_token, plan_id, job_id, &action_pair_refs)
            .await?;
        let elapsed = start.elapsed();
        info!(
            "Action resolution: {} actions resolved in {elapsed:?}",
            result.len()
        );
        result
    } else {
        std::collections::HashMap::new()
    };

    let actions_dir = std::path::Path::new(workspace)
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .join("_actions");
    let mut action_paths = std::collections::HashMap::new();

    // The official ActionManager.PrepareActionsRecursiveAsync resolves and
    // downloads nested actions recursively at prepare time (depth-limited,
    // batched per level). A composite manifest is only readable after its own
    // download, so walk the manifest tree level by level: each wave resolves
    // the nested remote refs discovered in the previous wave's manifests.
    let mut pending: Vec<(String, ParsedUses)> = refs;
    let mut wave_resolved = resolved;
    // Exclusive upper bound: at most `composite_max_depth()` executable
    // nesting levels (official Constants.CompositeActionsMaxDepth = 10).
    // An inclusive range pre-downloads one extra level the runner will reject.
    for _depth in 0..composite_max_depth() {
        if pending.is_empty() {
            break;
        }
        let mut nested: Vec<(String, ParsedUses)> = Vec::new();
        for (uses, parsed) in pending {
            if action_paths.contains_key(&uses) {
                continue;
            }
            let key = format!("{}@{}", parsed.action_name, parsed.git_ref);
            let meta = wave_resolved.get(&key);
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

            // A composite's nested `uses:` refs are collected for the next
            // wave, mirroring PrepareActionsRecursiveAsync.
            let Ok(manifest) = super::handlers::factory::load_action_manifest(&action_dir) else {
                continue;
            };
            if manifest.runs_using != "composite" {
                continue;
            }
            if let Some(steps) = &manifest.runs_steps {
                for step in steps {
                    let Some(nested_uses) = step.get("uses").and_then(|v| v.as_str()) else {
                        continue;
                    };
                    if nested_uses.starts_with("./")
                        || nested_uses.starts_with("../")
                        || nested_uses.starts_with("docker://")
                        || nested_uses.starts_with("$/")
                    {
                        continue;
                    }
                    if action_paths.contains_key(nested_uses) {
                        continue;
                    }
                    if let Some(parsed) = parse_remote_uses(nested_uses) {
                        if !nested.iter().any(|(u, _)| u == nested_uses) {
                            nested.push((nested_uses.to_owned(), parsed));
                        }
                    }
                }
            }
        }
        if nested.is_empty() {
            break;
        }
        // Resolve the next wave's refs against the server (batched, like the
        // official manager), tolerating a failed resolve so preparation still
        // downloads what it can; execution-time staging covers the rest.
        let action_pairs: Vec<(&str, &str)> = nested
            .iter()
            .map(|(_, parsed)| (parsed.action_name.as_str(), parsed.git_ref.as_str()))
            .collect();
        wave_resolved = resolver
            .resolve_batch(&access_token, plan_id, job_id, &action_pairs)
            .await
            .unwrap_or_default();
        pending = nested;
    }

    Ok(action_paths)
}

/// Official `Constants.CompositeActionsMaxDepth` (10).
fn composite_max_depth() -> usize {
    10
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

/// Official `Constants.Runner.Features.SelfRepository` = `actions_self_repository`.
fn self_repository_enabled(job_message: &serde_json::Value) -> bool {
    message_variable(job_message, "actions_self_repository").is_some_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "t" | "y" | "yes" | "on"
        )
    })
}
