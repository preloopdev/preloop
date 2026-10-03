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
    let mut snapshot_refs: Vec<(String, String, SnapshotOrigin)> = Vec::new();
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
            match resolve_self_repository(job_message, uses) {
                Some(SelfRepositorySource::Forge(parsed)) => refs.push((uses.clone(), parsed)),
                Some(SelfRepositorySource::Snapshot { subpath, origin }) => {
                    snapshot_refs.push((uses.clone(), subpath, origin));
                }
                None => {}
            }
            continue;
        }
        if let Some(parsed) = parse_remote_uses(uses) {
            refs.push((uses.clone(), parsed));
        } else {
            warn!("Cannot parse remote action ref (missing @version?): {uses:?}");
        }
    }

    let actions_dir = std::path::Path::new(workspace)
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .join("_actions");
    let mut action_paths = std::collections::HashMap::new();

    for (uses, subpath, origin) in &snapshot_refs {
        let root = stage_snapshot_tree(origin, &actions_dir).await?;
        action_paths.insert(
            uses.clone(),
            root.join(subpath).to_string_lossy().into_owned(),
        );
    }

    if refs.is_empty() {
        return Ok(action_paths);
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
            let archive_pin = meta.map(|m| m.archive_sha256.clone()).unwrap_or_default();

            let (action_root, _) = super::actions::manager::download_action(
                &parsed.owner,
                &parsed.repo,
                dir_ref,
                &actions_dir,
                download_url,
                auth_token,
                archive_pin,
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
                    if let Some(parsed) = parse_remote_uses(nested_uses)
                        && !nested.iter().any(|(u, _)| u == nested_uses)
                    {
                        nested.push((nested_uses.to_owned(), parsed));
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

/// The repository and commit a `$/` reference names: the official
/// `job.workflow_repository` / `job.workflow_sha` (v2.336.0
/// `PrepareActionsRecursiveAsync`). These point at the repository holding the
/// workflow file, so a reusable workflow resolves against the callee, not the
/// caller. `context_data` is the message's typed `contextData`.
pub(crate) fn self_repository_identity(
    context_data: &serde_json::Value,
) -> Option<(String, String)> {
    let job = super::job_extension::decode_typed_value(context_data.get("job")?);
    let field = |key: &str| {
        job.get(key)
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    };
    Some((field("workflow_repository")?, field("workflow_sha")?))
}

/// Where a `$/` reference's tree comes from.
enum SelfRepositorySource {
    /// `workflow_repository@workflow_sha` from the forge — the official path.
    Forge(ParsedUses),
    /// The run's local-workspace snapshot, which holds the exact tree under
    /// test (uncommitted edits included) that the forge has never seen.
    Snapshot {
        subpath: String,
        origin: SnapshotOrigin,
    },
}

/// The engine-served snapshot a local-workspace run's primary checkout reads.
struct SnapshotOrigin {
    owner: String,
    repo: String,
    commit: String,
    url: String,
    auth_header: String,
}

fn resolve_self_repository(
    job_message: &serde_json::Value,
    uses: &str,
) -> Option<SelfRepositorySource> {
    if !self_repository_enabled(job_message) {
        warn!(
            "Self-repository reference '{uses}' requires actions_self_repository; leaving unresolved"
        );
        return None;
    }
    let subpath = uses.strip_prefix("$/")?.trim_start_matches('/');
    if subpath.is_empty() {
        warn!("Bare $/ without subpath is not valid: {uses:?}");
        return None;
    }
    let Some((repository, sha)) = job_message
        .get("contextData")
        .and_then(self_repository_identity)
    else {
        warn!("Cannot resolve {uses}: job.workflow_repository/workflow_sha not in job message");
        return None;
    };
    let Some((owner, repo)) = repository.split_once('/') else {
        warn!("Cannot parse workflow repository for $/ resolution: {repository}");
        return None;
    };
    if let Err(error) =
        super::handlers::action::validate_remote_action_reference(owner, repo, &sha, subpath)
    {
        warn!("Cannot resolve {uses}: {error}");
        return None;
    }
    if let Some(origin) = local_snapshot_origin(job_message, owner, repo) {
        return Some(SelfRepositorySource::Snapshot {
            subpath: subpath.to_owned(),
            origin,
        });
    }
    Some(SelfRepositorySource::Forge(ParsedUses {
        owner: owner.to_owned(),
        repo: repo.to_owned(),
        subpath: subpath.to_owned(),
        git_ref: sha,
        action_name: repository.clone(),
    }))
}

/// The snapshot to read a `$/` tree from, when the run tests a local
/// workspace and the workflow lives in the run's own repository.
///
/// The engine sends `preloopSnapshotOriginRewrite` only for local-workspace
/// snapshots: their `workflow_sha` may be unpushed, and even a pushed one
/// lacks the uncommitted edits the run tests. `preloopSnapshotCommit` is the
/// immutable commit the primary checkout was redirected to. A workflow from
/// another repository (remote reusable workflow) is not in the snapshot and
/// takes the forge path.
fn local_snapshot_origin(
    job_message: &serde_json::Value,
    owner: &str,
    repo: &str,
) -> Option<SnapshotOrigin> {
    let rewrite = job_message.get("preloopSnapshotOriginRewrite")?;
    let url = rewrite.get("snapshotUrl")?.as_str()?;
    let auth_header = rewrite.get("authHeader")?.as_str()?;
    let commit = job_message
        .get("preloopSnapshotCommit")?
        .as_str()
        .filter(|sha| preloop_gha_protocol::git_ref::is_commit_sha_not_zero(sha))?;
    let github =
        super::job_extension::decode_typed_value(job_message.get("contextData")?.get("github")?);
    let run_repository = github.get("repository")?.as_str()?;
    if !run_repository.eq_ignore_ascii_case(&format!("{owner}/{repo}")) {
        return None;
    }
    Some(SnapshotOrigin {
        owner: owner.to_owned(),
        repo: repo.to_owned(),
        commit: commit.to_owned(),
        url: url.to_owned(),
        auth_header: auth_header.to_owned(),
    })
}

/// Fetch the snapshot commit into `_actions/{owner}/{repo}/{commit}` and
/// return that root. The commit is immutable, so an existing directory is
/// reused; a partial fetch never lands there because it is staged in a
/// sibling and renamed into place.
async fn stage_snapshot_tree(
    origin: &SnapshotOrigin,
    actions_dir: &std::path::Path,
) -> Result<std::path::PathBuf> {
    let repo_dir = actions_dir.join(&origin.owner).join(&origin.repo);
    let root = repo_dir.join(&origin.commit);
    if root.is_dir() {
        return Ok(root);
    }
    tokio::fs::create_dir_all(&repo_dir).await?;
    let staging = repo_dir.join(format!(
        "{}.staging-{}",
        origin.commit,
        uuid::Uuid::new_v4()
    ));
    tokio::fs::create_dir(&staging).await?;
    let staged = async {
        snapshot_git(&staging, origin, &["init", "-q"]).await?;
        snapshot_git(
            &staging,
            origin,
            &[
                "fetch",
                "-q",
                "--depth=1",
                "--no-tags",
                &origin.url,
                &origin.commit,
            ],
        )
        .await?;
        snapshot_git(
            &staging,
            origin,
            &["checkout", "-q", "--detach", "FETCH_HEAD"],
        )
        .await
    }
    .await;
    if let Err(error) = staged {
        let _ = tokio::fs::remove_dir_all(&staging).await;
        return Err(error);
    }
    if let Err(error) = tokio::fs::rename(&staging, &root).await {
        let _ = tokio::fs::remove_dir_all(&staging).await;
        // Another job may have staged this same immutable commit first.
        if root.is_dir() {
            return Ok(root);
        }
        return Err(error.into());
    }
    tracing::info!(
        "Staged self-repository snapshot {} for {}/{}",
        origin.commit,
        origin.owner,
        origin.repo
    );
    Ok(root)
}

/// Run git in `dir`. The snapshot credential travels in the process
/// environment (`GIT_CONFIG_*`), never on argv.
async fn snapshot_git(dir: &std::path::Path, origin: &SnapshotOrigin, args: &[&str]) -> Result<()> {
    let output = tokio::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_COUNT", "1")
        .env(
            "GIT_CONFIG_KEY_0",
            format!("http.{}.extraheader", origin.url),
        )
        .env("GIT_CONFIG_VALUE_0", &origin.auth_header)
        .output()
        .await
        .map_err(|error| anyhow::anyhow!("running git {}: {error}", args[0]))?;
    if !output.status.success() {
        anyhow::bail!(
            "staging self-repository snapshot {}: git {} failed: {}",
            origin.commit,
            args[0],
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const WORKFLOW_SHA: &str = "6b2de6eb160186cd946fb2f0929e2b265624cd61";

    /// `contextData` on the wire is typed `PipelineContextData`
    /// (`{"t":2,"d":[{"k":…,"v":…}]}`), not plain JSON.
    fn typed_dict(entries: &[(&str, &str)]) -> serde_json::Value {
        serde_json::json!({
            "t": 2,
            "d": entries
                .iter()
                .map(|(key, value)| serde_json::json!({"k": key, "v": value}))
                .collect::<Vec<_>>(),
        })
    }

    fn message(github_repository: &str, workflow_repository: &str) -> serde_json::Value {
        serde_json::json!({
            "variables": {"actions_self_repository": {"value": "true"}},
            "contextData": {
                "github": typed_dict(&[
                    ("repository", github_repository),
                    ("sha", "1111111111111111111111111111111111111111"),
                ]),
                "job": typed_dict(&[
                    ("workflow_repository", workflow_repository),
                    ("workflow_sha", WORKFLOW_SHA),
                ]),
            },
        })
    }

    /// The campaign failure: `$/` must resolve from the typed `job` context's
    /// `workflow_repository`/`workflow_sha` — not `github.sha` — or pytest's
    /// `$/.github/actions/setup-tox` is never staged and dies at execution.
    #[test]
    fn self_repository_resolves_from_typed_job_workflow_identity() {
        let msg = message("pytest-dev/pytest", "pytest-dev/pytest");
        let Some(SelfRepositorySource::Forge(parsed)) =
            resolve_self_repository(&msg, "$/.github/actions/setup-tox")
        else {
            panic!("a webhook run resolves $/ through the forge");
        };
        assert_eq!(parsed.action_name, "pytest-dev/pytest");
        assert_eq!(parsed.git_ref, WORKFLOW_SHA);
        assert_eq!(parsed.subpath, ".github/actions/setup-tox");
    }

    /// A remote reusable workflow's `$/` names the callee repository.
    #[test]
    fn self_repository_follows_the_workflow_repository_not_the_run() {
        let mut msg = message("octo-org/app", "octo-org/shared-workflows");
        msg["preloopSnapshotCommit"] = serde_json::json!(WORKFLOW_SHA);
        msg["preloopSnapshotOriginRewrite"] = serde_json::json!({
            "snapshotUrl": "http://engine/snapshots/octo-org/app",
            "forgeUrl": "https://github.com/octo-org/app",
            "authHeader": "AUTHORIZATION: basic x",
        });
        let Some(SelfRepositorySource::Forge(parsed)) =
            resolve_self_repository(&msg, "$/actions/build")
        else {
            panic!("a callee outside the snapshot resolves through the forge");
        };
        assert_eq!(parsed.action_name, "octo-org/shared-workflows");
    }

    #[test]
    fn self_repository_rejects_traversal_and_closed_gate() {
        let msg = message("o/r", "o/r");
        assert!(resolve_self_repository(&msg, "$/../../etc").is_none());
        assert!(resolve_self_repository(&msg, "$/").is_none());
        let mut closed = msg.clone();
        closed["variables"] = serde_json::json!({});
        assert!(resolve_self_repository(&closed, "$/a").is_none());
    }

    /// A local-workspace run stages `$/` from the snapshot commit — the tree
    /// under test — even when `workflow_sha` names a commit the forge lacks.
    #[tokio::test]
    async fn local_workspace_run_stages_self_repository_from_the_snapshot() {
        let temp = tempfile::TempDir::new().unwrap();
        let source = temp.path().join("source");
        std::fs::create_dir_all(source.join(".github/actions/setup-tox")).unwrap();
        std::fs::write(
            source.join(".github/actions/setup-tox/action.yml"),
            "name: setup-tox\nruns:\n  using: composite\n  steps: []\n",
        )
        .unwrap();
        let git = |args: &[&str]| {
            let status = std::process::Command::new("git")
                .arg("-C")
                .arg(&source)
                .args(args)
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .status()
                .unwrap();
            assert!(status.success(), "git {args:?}");
        };
        git(&["init", "-q"]);
        git(&["config", "uploadpack.allowAnySHA1InWant", "true"]);
        git(&["add", "."]);
        git(&[
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "commit",
            "-qm",
            "snapshot",
        ]);
        let commit = String::from_utf8(
            std::process::Command::new("git")
                .arg("-C")
                .arg(&source)
                .args(["rev-parse", "HEAD"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_owned();

        let mut msg = message("pytest-dev/pytest", "pytest-dev/pytest");
        msg["preloopSnapshotCommit"] = serde_json::json!(commit);
        msg["preloopSnapshotOriginRewrite"] = serde_json::json!({
            "snapshotUrl": format!("file://{}", source.display()),
            "forgeUrl": "https://github.com/pytest-dev/pytest",
            "authHeader": "AUTHORIZATION: basic x",
        });
        let workspace = temp.path().join("_work/pytest/pytest");
        std::fs::create_dir_all(&workspace).unwrap();
        let steps = vec![Step {
            id: "tox".into(),
            context_name: "tox".into(),
            display_name: "tox".into(),
            step_type: StepType::Action {
                uses: "$/.github/actions/setup-tox".into(),
                with: serde_json::json!({}),
            },
            condition: None,
            continue_on_error: false,
            timeout_minutes: None,
            env: Default::default(),
            raw: serde_json::json!({}),
            is_background: false,
        }];

        let paths = prepare_remote_actions(&msg, workspace.to_str().unwrap(), &steps, "plan")
            .await
            .expect("snapshot staging succeeds");
        let staged = std::path::PathBuf::from(&paths["$/.github/actions/setup-tox"]);
        assert_eq!(
            staged,
            temp.path()
                .join("_work/pytest/_actions/pytest-dev/pytest")
                .join(&commit)
                .join(".github/actions/setup-tox")
        );
        assert!(staged.join("action.yml").is_file());
    }
}
