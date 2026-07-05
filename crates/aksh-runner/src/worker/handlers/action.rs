//! Action step handler — dispatches `uses:` steps to the appropriate handler.

use anyhow::{Context, Result};
use tracing::info;

use crate::worker::execution_context::StepContext;

/// Run an action step (`uses:` reference).
///
/// This function is recursive (composite actions can reference other actions),
/// so it returns a boxed future to avoid infinite-size futures.
pub fn run_action<'a>(
    uses: &'a str,
    with: &'a serde_json::Value,
    workspace: &'a str,
    ctx: &'a mut StepContext<'_>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        info!("Running action: {uses}");

        // Set github.action_repository, github.action_ref, and github.action
        // The official runner leaves these set after the action completes so
        // subsequent script steps can read them.
        set_action_repository_context(ctx, uses);
        ctx.job.set_github_context_value(
            "action",
            Some(serde_json::Value::String(ctx.step_name.clone())),
        );

        if uses.starts_with("docker://") {
            super::container::run_docker_action(uses, with, workspace, ctx).await
        } else if uses.starts_with("./") || uses.starts_with("../") {
            let action_dir = std::path::Path::new(workspace).join(uses);
            run_action_from_dir(&action_dir, with, workspace, ctx, cancel_rx).await
        } else {
            let action_dir = resolve_remote_action(uses, workspace, ctx)?;
            run_action_from_dir(&action_dir, with, workspace, ctx, cancel_rx).await
        }
    })
}

use std::future::Future;

/// Run an action from a resolved directory.
async fn run_action_from_dir(
    action_dir: &std::path::Path,
    with: &serde_json::Value,
    workspace: &str,
    ctx: &mut StepContext<'_>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let manifest = super::factory::load_action_manifest(action_dir)?;

    match manifest.runs_using.as_str() {
        "node12" | "node16" | "node20" | "node24" => {
            if manifest.runs_using == "node12" || manifest.runs_using == "node16" {
                ctx.log(&format!(
                    "##[warning]Node.js {} actions are deprecated. Please update to node20 or later.",
                    manifest.runs_using
                ));
            }
            super::node::run_node_action(&manifest, action_dir, with, workspace, ctx, cancel_rx)
                .await
        }
        "composite" => {
            super::composite::run_composite_action(
                &manifest, action_dir, with, workspace, ctx, cancel_rx,
            )
            .await
        }
        "docker" => {
            super::container::run_docker_action_from_manifest(
                &manifest, action_dir, with, workspace, ctx,
            )
            .await
        }
        other => {
            anyhow::bail!("Unsupported action type: {other}")
        }
    }
}

/// Resolve a remote action reference to a local directory.
fn resolve_remote_action(
    uses: &str,
    workspace: &str,
    ctx: &StepContext<'_>,
) -> Result<std::path::PathBuf> {
    if let Some(path) = ctx.job.action_paths.get(uses) {
        return Ok(std::path::PathBuf::from(path));
    }
    let (repo_part, git_ref) = uses
        .split_once('@')
        .context("action reference must contain @ref")?;

    let parts: Vec<&str> = repo_part.splitn(3, '/').collect();
    if parts.len() < 2 {
        anyhow::bail!("invalid action reference: {uses}");
    }

    let owner = parts[0];
    let repo = parts[1];
    let subpath = if parts.len() > 2 { parts[2] } else { "" };

    let base = std::path::Path::new(workspace)
        .parent()
        .unwrap_or(std::path::Path::new("."));
    let action_dir = base.join("_actions").join(owner).join(repo).join(git_ref);

    if !subpath.is_empty() {
        Ok(action_dir.join(subpath))
    } else {
        Ok(action_dir)
    }
}

fn set_action_repository_context(ctx: &mut StepContext<'_>, uses: &str) {
    // Set github.action to the step's context name (matches official runner behavior)
    ctx.job.set_github_context_value(
        "action",
        Some(serde_json::Value::String(ctx.step_id.clone())),
    );

    if let Some((repository, git_ref)) = action_repository_context(uses) {
        ctx.job.set_github_context_value(
            "action_repository",
            Some(serde_json::Value::String(repository)),
        );
        ctx.job
            .set_github_context_value("action_ref", Some(serde_json::Value::String(git_ref)));
    } else {
        ctx.job
            .set_github_context_value("action_repository", Some(serde_json::Value::Null));
        ctx.job
            .set_github_context_value("action_ref", Some(serde_json::Value::Null));
    }
}

fn action_repository_context(uses: &str) -> Option<(String, String)> {
    if uses.starts_with("docker://") || uses.starts_with("./") || uses.starts_with("../") {
        return None;
    }

    let (repo_part, git_ref) = uses.split_once('@')?;
    let mut parts = repo_part.split('/');
    let owner = parts.next()?;
    let repo = parts.next()?;
    if owner.is_empty() || repo.is_empty() || git_ref.is_empty() {
        return None;
    }

    Some((format!("{owner}/{repo}"), git_ref.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_repository_context_extracts_repository_and_ref() {
        assert_eq!(
            action_repository_context("actions/checkout/path@v4"),
            Some(("actions/checkout".to_string(), "v4".to_string()))
        );
    }

    #[test]
    fn action_repository_context_is_empty_for_local_and_docker_actions() {
        assert_eq!(action_repository_context("./.github/actions/local"), None);
        assert_eq!(action_repository_context("docker://alpine:3.20"), None);
    }
}
