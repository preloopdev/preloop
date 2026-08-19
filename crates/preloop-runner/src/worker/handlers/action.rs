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
            run_action_from_dir(&action_dir, with, workspace, ctx, cancel_rx, None).await
        } else {
            let action_dir = resolve_remote_action(uses, workspace, ctx)?;
            run_action_from_dir(&action_dir, with, workspace, ctx, cancel_rx, Some(uses)).await
        }
    })
}

/// Run an action from a resolved directory.
pub(crate) async fn run_action_from_dir(
    action_dir: &std::path::Path,
    with: &serde_json::Value,
    workspace: &str,
    ctx: &mut StepContext<'_>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    action_name: Option<&str>,
) -> Result<()> {
    let manifest = super::factory::load_action_manifest(action_dir)?;

    match manifest.runs_using.as_str() {
        "node12" | "node16" | "node20" | "node22" | "node24" => {
            if manifest.runs_using == "node12" || manifest.runs_using == "node16" {
                ctx.log(&format!(
                    "##[warning]Node.js {} actions are deprecated. Please update to node20 or later.",
                    manifest.runs_using
                ));
            }
            super::node::run_node_action(
                &manifest,
                action_dir,
                with,
                workspace,
                ctx,
                cancel_rx,
                action_name,
            )
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
    validate_remote_action_reference(owner, repo, git_ref, subpath)?;

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

/// Stage a remote action on demand and return its resolved directory.
///
/// Job-start preparation only stages the actions named by the message's own
/// steps, so a `uses:` nested inside a composite action (e.g.
/// `ruby/setup-ruby@v1` in a local `.github/actions/*`) is never downloaded
/// there. GitHub downloads nested actions when the composite is first
/// invoked; this replicates that. Cached under `_actions/` like the
/// prepared ones, and remembered in `action_paths` for later steps.
pub(crate) async fn ensure_remote_action_staged(
    uses: &str,
    workspace: &str,
    ctx: &mut StepContext<'_>,
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
    let (owner, repo) = (parts[0], parts[1]);
    let subpath = if parts.len() > 2 { parts[2] } else { "" };
    validate_remote_action_reference(owner, repo, git_ref, subpath)?;

    let base = std::path::Path::new(workspace)
        .parent()
        .unwrap_or(std::path::Path::new("."));
    let actions_dir = base.join("_actions");
    // Resolve ref → SHA through the server's runnerresolve endpoint first,
    // like job-start preparation does (and like the official ActionManager):
    // the download is then SHA-pinned via the server-minted URL. When the
    // launch endpoint is unavailable (None), fall back to the api.github.com
    // tarball. Auth/protocol failures from a configured endpoint must
    // propagate — never launder them into an unauthenticated download.
    let launch_url = ctx
        .job
        .get_variable("system.github.launch_endpoint")
        .map(str::to_owned);
    let mut resolved = None;
    if let Some(launch_url) = launch_url {
        let http = crate::client::http::HttpClient::new(None)?;
        let resolver =
            crate::client::actions_download::ActionsResolveClient::new(http, Some(launch_url));
        let action_name = format!("{owner}/{repo}");
        let key = format!("{action_name}@{git_ref}");
        // Job-start preparation authenticates with the SystemVssConnection
        // token and the plan/job identity. Nested on-demand staging must do
        // the same; empty credentials make private/GHES actions fail and
        // skip the server's SHA-pinned download path.
        let access_token = ctx
            .job
            .env
            .get("ACTIONS_RUNTIME_TOKEN")
            .cloned()
            .unwrap_or_default();
        let plan_id = ctx
            .job
            .get_variable("system.orchestrationId")
            .or_else(|| ctx.job.get_variable("system.planId"))
            .unwrap_or("");
        let job_id = ctx.job.job_id.as_str();
        let batch = resolver
            .resolve_batch(
                &access_token,
                plan_id,
                job_id,
                &[(action_name.as_str(), git_ref)],
            )
            .await
            .with_context(|| format!("runnerresolve nested action {uses}"))?;
        resolved = batch.get(&key).cloned();
    }
    let dir_ref = resolved
        .as_ref()
        .map(|meta| meta.resolved_sha.as_str())
        .filter(|sha| !sha.is_empty())
        .unwrap_or(git_ref);
    let download_url = resolved
        .as_ref()
        .map(|meta| meta.tar_url.as_str())
        .filter(|url| !url.is_empty());
    let auth_token = resolved
        .as_ref()
        .and_then(|meta| meta.auth_token.as_deref());
    let action_root = crate::worker::actions::manager::download_action(
        owner,
        repo,
        dir_ref,
        &actions_dir,
        download_url,
        auth_token,
    )
    .await?;
    let action_dir = if subpath.is_empty() {
        action_root
    } else {
        action_root.join(subpath)
    };
    // Containment: resolved path must stay under `_actions/`.
    ensure_under_actions_dir(&actions_dir, &action_dir)?;
    ctx.job
        .action_paths
        .insert(uses.to_owned(), action_dir.to_string_lossy().into_owned());
    Ok(action_dir)
}

fn validate_remote_action_reference(
    owner: &str,
    repo: &str,
    git_ref: &str,
    subpath: &str,
) -> Result<()> {
    if owner.is_empty() || repo.is_empty() || git_ref.is_empty() {
        anyhow::bail!("invalid action reference components");
    }
    // Reject absolute paths and `.` / `..` in every reference segment before
    // any filesystem join. Owner/repo are single path components; ref and
    // subpath may contain `/` but never traversal.
    for (label, value) in [("owner", owner), ("repo", repo)] {
        if !is_safe_single_component(value) {
            anyhow::bail!("action reference {label} contains an unsafe path component");
        }
    }
    if !is_safe_relative_path(git_ref) || (!subpath.is_empty() && !is_safe_relative_path(subpath)) {
        anyhow::bail!("action reference contains an unsafe path component");
    }
    Ok(())
}

fn is_safe_single_component(value: &str) -> bool {
    !value.is_empty()
        && !value.contains('/')
        && !value.contains('\\')
        && value != "."
        && value != ".."
}

fn is_safe_relative_path(value: &str) -> bool {
    let path = std::path::Path::new(value);
    !value.is_empty()
        && !path.is_absolute()
        && !value.contains('\\')
        && path
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
}

fn ensure_under_actions_dir(
    actions_dir: &std::path::Path,
    action_dir: &std::path::Path,
) -> Result<()> {
    let actions_canon =
        std::fs::canonicalize(actions_dir).unwrap_or_else(|_| actions_dir.to_path_buf());
    // action_dir may not exist yet for path-only resolution; walk parents.
    let mut probe = action_dir.to_path_buf();
    let action_canon = loop {
        if let Ok(canon) = std::fs::canonicalize(&probe) {
            break canon;
        }
        if !probe.pop() {
            break action_dir.to_path_buf();
        }
    };
    if action_canon == actions_canon || action_canon.starts_with(&actions_canon) {
        return Ok(());
    }
    anyhow::bail!(
        "action path escapes _actions directory: {}",
        action_dir.display()
    );
}

pub(crate) fn set_action_repository_context(ctx: &mut StepContext<'_>, uses: &str) {
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

    #[tokio::test]
    async fn ensure_remote_action_staged_returns_prepared_path_without_download() {
        // A nested action already staged (by a previous composite invocation
        // or by job preparation) must resolve from `action_paths` without
        // touching the network.
        let workspace = tempfile::TempDir::new().unwrap();
        let mut job = crate::worker::contexts::JobContext::new(
            "j1".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({"github": {"workspace": workspace.path()}}),
        );
        job.workspace = Some(workspace.path().to_string_lossy().to_string());
        let staged = workspace.path().join("_actions/ruby/setup-ruby/sha");
        std::fs::create_dir_all(&staged).unwrap();
        job.action_paths.insert(
            "ruby/setup-ruby@6e5d382445ae5590b7449d8b3bc8cb1c2c27f617".to_owned(),
            staged.to_string_lossy().into_owned(),
        );
        let mut ctx = StepContext::new(&mut job, "composite".into(), "Composite".into());
        let resolved = crate::worker::handlers::action::ensure_remote_action_staged(
            "ruby/setup-ruby@6e5d382445ae5590b7449d8b3bc8cb1c2c27f617",
            workspace.path().to_str().unwrap(),
            &mut ctx,
        )
        .await
        .unwrap();
        assert_eq!(resolved, staged);
    }

    #[test]
    fn validate_remote_action_reference_rejects_traversal() {
        assert!(validate_remote_action_reference("actions", "checkout", "v4", "").is_ok());
        assert!(
            validate_remote_action_reference("..", "checkout", "v4", "").is_err(),
            "owner must not be .."
        );
        assert!(
            validate_remote_action_reference("actions", ".", "v4", "").is_err(),
            "repo must not be ."
        );
        assert!(
            validate_remote_action_reference("actions", "checkout", "../v4", "").is_err(),
            "ref must not traverse"
        );
        assert!(
            validate_remote_action_reference("actions", "checkout", "v4", "../../etc").is_err(),
            "subpath must not traverse"
        );
        assert!(
            validate_remote_action_reference("/abs", "checkout", "v4", "").is_err(),
            "owner must not be absolute"
        );
        assert!(
            validate_remote_action_reference("actions", "checkout", "v4", "/abs").is_err(),
            "subpath must not be absolute"
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
            "my_step_id".into(),      // step ID (context_name)
            "My Display Name".into(), // step display name — must NOT appear in github.action
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
