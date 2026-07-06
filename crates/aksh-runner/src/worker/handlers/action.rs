//! Action step handler — dispatches `uses:` steps to the appropriate handler.

use std::future::Future;

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

    // --- P0 action resolution gap coverage ---

    #[test]
    fn resolve_remote_action_constructs_path() {
        let dir = tempfile::TempDir::new().unwrap();
        let workspace = dir.path().join("work").join("repo");
        std::fs::create_dir_all(&workspace).unwrap();
        let mut job = crate::worker::contexts::JobContext::new(
            "j1".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let ctx = crate::worker::execution_context::StepContext::new(
            &mut job,
            "step1".into(),
            "Step".into(),
        );

        let result =
            resolve_remote_action("actions/checkout@v4", workspace.to_str().unwrap(), &ctx)
                .unwrap();
        let expected = dir
            .path()
            .join("work")
            .join("_actions")
            .join("actions")
            .join("checkout")
            .join("v4");
        assert_eq!(result, expected);
    }

    #[test]
    fn resolve_remote_action_with_subpath() {
        let dir = tempfile::TempDir::new().unwrap();
        let workspace = dir.path().join("work").join("repo");
        std::fs::create_dir_all(&workspace).unwrap();
        let mut job = crate::worker::contexts::JobContext::new(
            "j1".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let ctx = crate::worker::execution_context::StepContext::new(
            &mut job,
            "step1".into(),
            "Step".into(),
        );

        let result = resolve_remote_action(
            "actions/checkout/subdir@v4",
            workspace.to_str().unwrap(),
            &ctx,
        )
        .unwrap();
        let expected = dir
            .path()
            .join("work")
            .join("_actions")
            .join("actions")
            .join("checkout")
            .join("v4")
            .join("subdir");
        assert_eq!(result, expected);
    }

    #[test]
    fn resolve_remote_action_missing_ref_errors() {
        let dir = tempfile::TempDir::new().unwrap();
        let workspace = dir.path().join("work").join("repo");
        std::fs::create_dir_all(&workspace).unwrap();
        let mut job = crate::worker::contexts::JobContext::new(
            "j1".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let ctx = crate::worker::execution_context::StepContext::new(
            &mut job,
            "step1".into(),
            "Step".into(),
        );

        let err = resolve_remote_action("actions/checkout", workspace.to_str().unwrap(), &ctx)
            .unwrap_err();
        assert!(err.to_string().contains("@ref"));
    }

    #[test]
    fn resolve_remote_action_invalid_format_errors() {
        let dir = tempfile::TempDir::new().unwrap();
        let workspace = dir.path().join("work").join("repo");
        std::fs::create_dir_all(&workspace).unwrap();
        let mut job = crate::worker::contexts::JobContext::new(
            "j1".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let ctx = crate::worker::execution_context::StepContext::new(
            &mut job,
            "step1".into(),
            "Step".into(),
        );

        let err =
            resolve_remote_action("checkout@v4", workspace.to_str().unwrap(), &ctx).unwrap_err();
        assert!(err.to_string().contains("invalid action reference"));
    }

    #[test]
    fn resolve_remote_action_uses_cached_path() {
        let dir = tempfile::TempDir::new().unwrap();
        let workspace = dir.path().join("work").join("repo");
        std::fs::create_dir_all(&workspace).unwrap();
        let mut job = crate::worker::contexts::JobContext::new(
            "j1".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        job.action_paths.insert(
            "actions/checkout@v4".to_string(),
            "/cached/actions/checkout/v4".to_string(),
        );
        let ctx = crate::worker::execution_context::StepContext::new(
            &mut job,
            "step1".into(),
            "Step".into(),
        );

        let result =
            resolve_remote_action("actions/checkout@v4", workspace.to_str().unwrap(), &ctx)
                .unwrap();
        assert_eq!(
            result,
            std::path::PathBuf::from("/cached/actions/checkout/v4")
        );
    }

    #[test]
    fn set_action_repository_context_sets_fields() {
        let mut job = crate::worker::contexts::JobContext::new(
            "j1".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({"github": {"repository": "owner/repo"}}),
        );
        let mut ctx = crate::worker::execution_context::StepContext::new(
            &mut job,
            "step1".into(),
            "Step".into(),
        );

        set_action_repository_context(&mut ctx, "actions/checkout@v4");
        assert_eq!(
            ctx.job
                .github_context_value("action_repository")
                .and_then(|v| v.as_str().map(String::from)),
            Some("actions/checkout".to_string())
        );
        assert_eq!(
            ctx.job
                .github_context_value("action_ref")
                .and_then(|v| v.as_str().map(String::from)),
            Some("v4".to_string())
        );
    }

    #[test]
    fn set_action_repository_context_clears_for_local() {
        let mut job = crate::worker::contexts::JobContext::new(
            "j1".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({"github": {"repository": "owner/repo"}}),
        );
        let mut ctx = crate::worker::execution_context::StepContext::new(
            &mut job,
            "step1".into(),
            "Step".into(),
        );

        set_action_repository_context(&mut ctx, "./.github/actions/local");
        // For local actions, action_repository is set to null
        let val = ctx.job.github_context_value("action_repository");
        assert!(val.is_none() || val == Some(serde_json::Value::Null));
    }
    #[test]
    fn set_action_repository_context_sets_action_to_step_id_not_display_name() {
        // RHAND-01 regression: github.action must equal the step ID, not the display name.
        // Pre-fix: run_action() overrode set_action_repository_context() with step_name.
        // Post-fix: only set_action_repository_context() runs, which correctly uses step_id.
        let mut job = crate::worker::contexts::JobContext::new(
            "j1".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let mut ctx = crate::worker::execution_context::StepContext::new(
            &mut job,
            "my_step_id".into(),          // step ID (context_name)
            "My Display Name".into(),     // step display name — must NOT appear in github.action
        );

        // Call set_action_repository_context as run_action does
        set_action_repository_context(&mut ctx, "actions/checkout@v4");

        // github.action must be the step ID, not the display name
        let action_val = ctx
            .job
            .github_context_value("action")
            .and_then(|v| v.as_str().map(String::from));
        assert_eq!(
            action_val,
            Some("my_step_id".to_string()),
            "github.action should be the step id, not the display name"
        );
        assert_ne!(
            ctx.job
                .github_context_value("action")
                .and_then(|v| v.as_str().map(String::from)),
            Some("My Display Name".to_string()),
            "github.action must not be the display name"
        );
    }

}
