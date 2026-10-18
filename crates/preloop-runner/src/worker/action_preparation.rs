//! Remote action download and reference resolution.

use anyhow::Result;
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};

use super::helpers::extract_service_endpoint;
use super::steps_runner::{Step, StepType};
use crate::client::actions_download::{ActionsResolveClient, ResolvedAction};
use crate::client::http::HttpClient;

/// The Set Up job announcement the official runner prints for a job with lockfile
/// pins (`JobExtension.cs`: after the proxy lines, before `Prepare workflow directory`).
const LOCKED_ACTIONS_ANNOUNCEMENT: &str =
    "Using locked action versions from the workflow's lockfile";

/// The lockfile pins a job message carries: the official wire key `dependencies`
/// (`AgentJobRequestMessage.ActionsDependencies`, `EmitDefaultValue = false`).
/// Missing or empty means the workflow has no lockfile entry. Other keys, such as
/// the camelCase `actionsDependencies`, are not the wire name and are ignored.
pub(crate) fn actions_dependencies(job_message: &serde_json::Value) -> Vec<String> {
    job_message
        .get("dependencies")
        .and_then(|value| value.as_array())
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry.as_str().map(str::to_owned))
                .collect::<Vec<String>>()
        })
        .unwrap_or_default()
}

/// The Set Up job announcement for a job with lockfile pins, or `None` without pins.
pub(crate) fn lockfile_announcement(dependencies: &[String]) -> Option<&'static str> {
    (!dependencies.is_empty()).then_some(LOCKED_ACTIONS_ANNOUNCEMENT)
}

/// A reference the preparation phase stages.
enum StagedRef {
    /// `owner/repo[/subpath]@ref`, fetched from the forge through Launch.
    Remote { uses: String, parsed: ParsedUses },
    /// A `$/` reference staged from the run's local-workspace snapshot.
    Snapshot {
        uses: String,
        subpath: String,
        origin: SnapshotOrigin,
    },
}

/// Stage the remote actions a job's steps use, and append the Set Up job lines the
/// official runner prints for them to `setup_lines`. Lines are unstamped; the caller
/// stamps them with the Set Up job timestamp.
///
/// Follows `ActionManager.PrepareActionsRecursiveLegacyAsync`, the path the official
/// runner takes unless `actions_batch_action_resolution` is set. The v2.336.0 job
/// messages carry that variable as `false`, and Preloop never sets it. A call of
/// references logs `Getting action download info` before its Launch batch when it has
/// refs to resolve, downloads its refs in order, then prepares each composite's nested
/// refs depth-first before the next sibling. Two deliberate differences: a reference
/// this job already downloaded is not expanded or downloaded again, a reference already
/// resolved is not requested again, and a nested batch that fails falls back to the refs
/// as written, so preparation still downloads what it can. The job's own batch fails
/// closed.
pub(crate) async fn prepare_remote_actions(
    job_message: &serde_json::Value,
    workspace: &str,
    steps: &[Step],
    plan_id: &str,
    setup_lines: &mut Vec<String>,
) -> Result<HashMap<String, String>> {
    let mut direct = Vec::new();
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
                Some(SelfRepositorySource::Forge(parsed)) => direct.push(StagedRef::Remote {
                    uses: uses.clone(),
                    parsed,
                }),
                Some(SelfRepositorySource::Snapshot { subpath, origin }) => {
                    direct.push(StagedRef::Snapshot {
                        uses: uses.clone(),
                        subpath,
                        origin,
                    });
                }
                None => {}
            }
            continue;
        }
        if let Some(parsed) = parse_remote_uses(uses) {
            direct.push(StagedRef::Remote {
                uses: uses.clone(),
                parsed,
            });
        } else {
            warn!("Cannot parse remote action ref (missing @version?): {uses:?}");
        }
    }

    let mut action_paths = HashMap::new();
    if direct.is_empty() {
        return Ok(action_paths);
    }

    let actions_dir = std::path::Path::new(workspace)
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .join("_actions");
    let job_id = job_message
        .get("jobId")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let access_token = extract_service_endpoint(job_message)
        .map(|(_, token)| token)
        .unwrap_or_default();
    let launch_url =
        message_variable(job_message, "system.github.launch_endpoint").map(str::to_string);
    // Without credentials and a Launch endpoint nothing can be resolved; the downloads
    // then fail closed on the missing SHA-pinned URL, as they always have.
    let can_resolve = !access_token.is_empty() && launch_url.is_some();
    let dependencies = actions_dependencies(job_message);
    let resolver = ActionsResolveClient::new(HttpClient::new(None)?, launch_url);

    let mut resolved: HashMap<String, ResolvedAction> = HashMap::new();
    let mut announced: HashSet<String> = HashSet::new();
    // A frame is one call of references at one composite depth. Frames pop LIFO and
    // each call's nested frames are pushed in reverse, so a composite's whole subtree
    // is prepared before its next sibling.
    let mut frames: Vec<(usize, Vec<StagedRef>)> = vec![(0, direct)];
    while let Some((depth, frame)) = frames.pop() {
        let pending = unresolved_refs(&frame, &resolved, &action_paths);
        if !pending.is_empty() {
            setup_lines.push("Getting action download info".to_owned());
            if can_resolve {
                let pairs: Vec<(&str, &str)> = pending
                    .iter()
                    .map(|(action, git_ref)| (action.as_str(), git_ref.as_str()))
                    .collect();
                let started = std::time::Instant::now();
                match resolver
                    .resolve_batch(&access_token, plan_id, job_id, &pairs, &dependencies)
                    .await
                {
                    Ok(batch) => {
                        info!(
                            "Action resolution: {} actions resolved in {:?}",
                            batch.len(),
                            started.elapsed()
                        );
                        resolved.extend(batch);
                    }
                    Err(error) if depth > 0 => {
                        warn!(
                            "Nested action resolution failed; downloading refs as \
                             written: {error:#}"
                        );
                    }
                    Err(error) => return Err(error),
                }
            }
        }

        let mut fetched: Vec<std::path::PathBuf> = Vec::new();
        for staged in &frame {
            match staged {
                StagedRef::Snapshot {
                    uses,
                    subpath,
                    origin,
                } => {
                    if action_paths.contains_key(uses) {
                        continue;
                    }
                    announce_download(
                        &mut announced,
                        setup_lines,
                        &format!("{}/{}", origin.owner, origin.repo),
                        &origin.commit,
                        &origin.commit,
                    );
                    let root = stage_snapshot_tree(origin, &actions_dir).await?;
                    action_paths.insert(
                        uses.clone(),
                        root.join(subpath).to_string_lossy().into_owned(),
                    );
                }
                StagedRef::Remote { uses, parsed } => {
                    if action_paths.contains_key(uses) {
                        continue;
                    }
                    let meta = resolved.get(&format!("{}@{}", parsed.action_name, parsed.git_ref));
                    let dir_ref = meta
                        .map(|meta| meta.resolved_sha.as_str())
                        .filter(|sha| !sha.is_empty())
                        .unwrap_or(parsed.git_ref.as_str());
                    let name = meta
                        .map(|meta| meta.name.as_str())
                        .filter(|name| !name.is_empty())
                        .unwrap_or(parsed.action_name.as_str());
                    let version = meta
                        .map(|meta| meta.version.as_str())
                        .filter(|version| !version.is_empty())
                        .unwrap_or(parsed.git_ref.as_str());
                    // An unresolved ref is refused by `download_action`, and naming it as a
                    // SHA would mislead the reader, so only a resolved commit is announced.
                    if preloop_gha_protocol::git_ref::is_commit_sha_not_zero(dir_ref) {
                        announce_download(&mut announced, setup_lines, name, version, dir_ref);
                    }
                    let download_url = meta
                        .map(|meta| meta.tar_url.as_str())
                        .filter(|url| !url.is_empty());
                    let auth_token = meta.and_then(|meta| meta.auth_token.as_deref());
                    let archive_pin = meta
                        .map(|meta| meta.archive_sha256.clone())
                        .unwrap_or_default();
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
                    action_paths.insert(uses.clone(), action_dir.to_string_lossy().into_owned());
                    fetched.push(action_dir);
                }
            }
        }

        let mut nested_frames: Vec<Vec<StagedRef>> = Vec::new();
        for action_dir in &fetched {
            // A composite's nested `uses:` refs form the next call, as the steps of
            // ActionManager's PrepareRepositoryActionAsync do.
            let Ok(manifest) = super::handlers::factory::load_action_manifest(action_dir) else {
                continue;
            };
            if manifest.runs_using != "composite" {
                continue;
            }
            let Some(runs_steps) = &manifest.runs_steps else {
                continue;
            };
            let mut nested: Vec<StagedRef> = Vec::new();
            for step in runs_steps {
                let Some(nested_uses) = step.get("uses").and_then(|v| v.as_str()) else {
                    continue;
                };
                if nested_uses.starts_with("./")
                    || nested_uses.starts_with("../")
                    || nested_uses.starts_with("docker://")
                    || nested_uses.starts_with("$/")
                    || action_paths.contains_key(nested_uses)
                {
                    continue;
                }
                let repeated = nested.iter().any(|staged| match staged {
                    StagedRef::Remote { uses, .. } => uses.as_str() == nested_uses,
                    StagedRef::Snapshot { .. } => false,
                });
                if repeated {
                    continue;
                }
                if let Some(parsed) = parse_remote_uses(nested_uses) {
                    nested.push(StagedRef::Remote {
                        uses: nested_uses.to_owned(),
                        parsed,
                    });
                }
            }
            if !nested.is_empty() {
                nested_frames.push(nested);
            }
        }
        if depth + 1 < composite_max_depth() {
            for nested in nested_frames.into_iter().rev() {
                frames.push((depth + 1, nested));
            }
        }
    }

    Ok(action_paths)
}

/// The `(action, git_ref)` pairs of one call that still need a Launch resolution: not
/// yet downloaded and not yet resolved in this job, each pair once.
fn unresolved_refs(
    frame: &[StagedRef],
    resolved: &HashMap<String, ResolvedAction>,
    action_paths: &HashMap<String, String>,
) -> Vec<(String, String)> {
    let mut seen = HashSet::new();
    let mut pending = Vec::new();
    for staged in frame {
        let StagedRef::Remote { uses, parsed } = staged else {
            continue;
        };
        if action_paths.contains_key(uses) {
            continue;
        }
        let key = format!("{}@{}", parsed.action_name, parsed.git_ref);
        if !resolved.contains_key(&key) && seen.insert(key) {
            pending.push((parsed.action_name.clone(), parsed.git_ref.clone()));
        }
    }
    pending
}

/// Append the `Download action repository` line for `name@git_ref`, once per job.
///
/// The official runner prints this line only when it downloads; a cache hit is a silent
/// Debug line there. This runner announces cache hits too, so the log lists every action
/// the job uses. Repeats within one job are announced once.
fn announce_download(
    announced: &mut HashSet<String>,
    setup_lines: &mut Vec<String>,
    name: &str,
    git_ref: &str,
    resolved_sha: &str,
) {
    if announced.insert(format!("{name}@{git_ref}")) {
        setup_lines.push(format!(
            "Download action repository '{name}@{git_ref}' (SHA:{resolved_sha})"
        ));
    }
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
/// The engine sends `preloopSnapshotOriginRewrite` for local workspace and
/// self-built merge snapshots, whose commits may not exist on the forge.
/// Remote reusable workflows are outside the snapshot and use the forge.
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
    use crate::client::actions_download::ArchiveDigestPin;

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

        let mut setup_lines = Vec::new();
        let paths = prepare_remote_actions(
            &msg,
            workspace.to_str().unwrap(),
            &steps,
            "plan",
            &mut setup_lines,
        )
        .await
        .expect("snapshot staging succeeds");
        assert_eq!(
            setup_lines,
            [format!(
                "Download action repository 'pytest-dev/pytest@{commit}' (SHA:{commit})"
            )]
        );
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

    const DEPENDENCY: &str = "actions/checkout@v4:sha256-abc123";
    const PARENT_SHA: &str = "1111111111111111111111111111111111111111";
    const OTHER_SHA: &str = "2222222222222222222222222222222222222222";
    const CHILD_SHA: &str = "3333333333333333333333333333333333333333";
    const PARENT_ACTION_YML: &str =
        "name: parent\nruns:\n  using: composite\n  steps:\n    - uses: owner/child@v1\n";
    const LEAF_ACTION_YML: &str = "name: leaf\nruns:\n  using: node20\n  main: index.js\n";

    fn action_step(uses: &str) -> Step {
        Step {
            id: uses.to_owned(),
            context_name: uses.to_owned(),
            display_name: uses.to_owned(),
            step_type: StepType::Action {
                uses: uses.to_owned(),
                with: serde_json::json!({}),
            },
            condition: None,
            continue_on_error: false,
            timeout_minutes: None,
            env: Default::default(),
            raw: serde_json::json!({}),
            is_background: false,
        }
    }

    /// A codeload-shaped gzip tarball: one top-level directory, which extraction strips,
    /// holding `action.yml`.
    fn action_tarball(action_yml: &str) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        {
            let mut archive = tar::Builder::new(&mut encoder);
            let mut header = tar::Header::new_gnu();
            header.set_size(action_yml.len() as u64);
            header.set_mode(0o644);
            archive
                .append_data(&mut header, "root/action.yml", action_yml.as_bytes())
                .unwrap();
            archive.finish().unwrap();
        }
        encoder.finish().unwrap()
    }

    /// Loopback stand-in for Launch's `runnerresolve/actions` and codeload. The resolve
    /// route answers each requested `action@version` it knows and records the request.
    async fn serve_launch_and_codeload(
        requests: std::sync::Arc<parking_lot::Mutex<Vec<serde_json::Value>>>,
    ) -> String {
        use axum::routing::{get, post};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let resolve_base = base.clone();
        let parent_tar = action_tarball(PARENT_ACTION_YML);
        let other_tar = action_tarball(LEAF_ACTION_YML);
        let child_tar = action_tarball(LEAF_ACTION_YML);
        let app = axum::Router::new()
            .route(
                "/actions/build/plan-1/jobs/job-1/runnerresolve/actions",
                post(move |body: bytes::Bytes| {
                    let base = resolve_base.clone();
                    let requests = requests.clone();
                    async move {
                        let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        let mut resolved = serde_json::Map::new();
                        for entry in request["actions"].as_array().into_iter().flatten() {
                            let action = entry["action"].as_str().unwrap_or_default();
                            let version = entry["version"].as_str().unwrap_or_default();
                            let (sha, slug) = match format!("{action}@{version}").as_str() {
                                "owner/parent@v1" => (PARENT_SHA, "parent"),
                                "owner/other@v1" => (OTHER_SHA, "other"),
                                "owner/child@v1" => (CHILD_SHA, "child"),
                                _ => continue,
                            };
                            resolved.insert(
                                format!("{action}@{version}"),
                                serde_json::json!({
                                    "name": action,
                                    "version": version,
                                    "resolved_sha": sha,
                                    "tar_url": format!("{base}/tar/{slug}"),
                                }),
                            );
                        }
                        requests.lock().push(request);
                        serde_json::json!({ "actions": resolved }).to_string()
                    }
                }),
            )
            .route("/tar/parent", get(|| async move { parent_tar }))
            .route("/tar/other", get(|| async move { other_tar }))
            .route("/tar/child", get(|| async move { child_tar }));
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        base
    }

    /// The job's own references are batched and downloaded in step order, then each
    /// composite's nested reference is resolved and downloaded in its own call. Every
    /// resolve request carries the job's lockfile pins.
    #[tokio::test]
    async fn prepare_logs_direct_and_nested_downloads_in_official_order() {
        let requests = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
        let base = serve_launch_and_codeload(requests.clone()).await;
        let temp = tempfile::TempDir::new().unwrap();
        let workspace = temp.path().join("_work").join("repo");
        std::fs::create_dir_all(&workspace).unwrap();
        let msg = serde_json::json!({
            "jobId": "job-1",
            "resources": {"endpoints": [{
                "name": "SystemVssConnection",
                "url": base.clone(),
                "authorization": {"parameters": {"AccessToken": "token"}},
            }]},
            "variables": {"system.github.launch_endpoint": {"value": base}},
            "dependencies": [DEPENDENCY],
        });
        let steps = [
            action_step("owner/parent@v1"),
            action_step("owner/other@v1"),
        ];
        let mut setup_lines = Vec::new();
        let paths = prepare_remote_actions(
            &msg,
            workspace.to_str().unwrap(),
            &steps,
            "plan-1",
            &mut setup_lines,
        )
        .await
        .expect("preparation succeeds against the stand-in");

        assert_eq!(
            setup_lines,
            [
                "Getting action download info".to_owned(),
                format!("Download action repository 'owner/parent@v1' (SHA:{PARENT_SHA})"),
                format!("Download action repository 'owner/other@v1' (SHA:{OTHER_SHA})"),
                "Getting action download info".to_owned(),
                format!("Download action repository 'owner/child@v1' (SHA:{CHILD_SHA})"),
            ]
        );
        let actions_dir = temp.path().join("_work").join("_actions");
        assert_eq!(
            std::path::PathBuf::from(&paths["owner/child@v1"]),
            actions_dir.join("owner").join("child").join(CHILD_SHA)
        );
        let requests = requests.lock();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0]["actions"],
            serde_json::json!([
                {"action": "owner/parent", "version": "v1"},
                {"action": "owner/other", "version": "v1"},
            ])
        );
        assert_eq!(
            requests[1]["actions"],
            serde_json::json!([{"action": "owner/child", "version": "v1"}])
        );
        for request in requests.iter() {
            assert_eq!(
                request["actions_dependencies"],
                serde_json::json!([DEPENDENCY])
            );
        }
    }

    #[test]
    fn dependencies_come_from_the_official_wire_key_only() {
        let pinned = serde_json::json!({ "dependencies": [DEPENDENCY] });
        assert_eq!(actions_dependencies(&pinned), vec![DEPENDENCY.to_owned()]);
        assert_eq!(
            lockfile_announcement(&actions_dependencies(&pinned)),
            Some("Using locked action versions from the workflow's lockfile")
        );
        // The camelCase key is not the wire name, so it never announces.
        let legacy = serde_json::json!({ "actionsDependencies": [DEPENDENCY] });
        assert!(actions_dependencies(&legacy).is_empty());
        assert_eq!(lockfile_announcement(&actions_dependencies(&legacy)), None);
        // Omitted and empty both mean the workflow has no lockfile entry.
        let omitted = serde_json::json!({});
        assert_eq!(lockfile_announcement(&actions_dependencies(&omitted)), None);
        let empty = serde_json::json!({ "dependencies": [] });
        assert_eq!(lockfile_announcement(&actions_dependencies(&empty)), None);
    }

    #[test]
    fn unresolved_refs_asks_once_per_pair_and_skips_known_refs() {
        let remote = |uses: &str| StagedRef::Remote {
            uses: uses.to_owned(),
            parsed: parse_remote_uses(uses).unwrap(),
        };
        let frame = vec![
            remote("a/x@v1"),
            remote("a/x@v1"),
            remote("b/y@v2"),
            remote("c/z@v3"),
            StagedRef::Snapshot {
                uses: "$/s".to_owned(),
                subpath: "s".to_owned(),
                origin: SnapshotOrigin {
                    owner: "o".to_owned(),
                    repo: "r".to_owned(),
                    commit: "abc".to_owned(),
                    url: "https://example.invalid".to_owned(),
                    auth_header: "AUTHORIZATION: basic x".to_owned(),
                },
            },
        ];
        let resolved = HashMap::from([(
            "b/y@v2".to_owned(),
            ResolvedAction {
                name: "b/y".to_owned(),
                version: "v2".to_owned(),
                resolved_sha: "4444444444444444444444444444444444444444".to_owned(),
                tar_url: "https://example.invalid/y.tar.gz".to_owned(),
                auth_token: None,
                archive_sha256: ArchiveDigestPin::Unsupported,
            },
        )]);
        let action_paths = HashMap::from([("c/z@v3".to_owned(), "/cached".to_owned())]);
        assert_eq!(
            unresolved_refs(&frame, &resolved, &action_paths),
            vec![("a/x".to_owned(), "v1".to_owned())]
        );
    }

    #[test]
    fn announce_download_names_each_action_once_in_the_official_format() {
        let mut announced = HashSet::new();
        let mut lines = Vec::new();
        announce_download(
            &mut announced,
            &mut lines,
            "actions/checkout",
            "v4",
            PARENT_SHA,
        );
        announce_download(
            &mut announced,
            &mut lines,
            "actions/checkout",
            "v4",
            PARENT_SHA,
        );
        announce_download(
            &mut announced,
            &mut lines,
            "actions/checkout",
            "v5",
            OTHER_SHA,
        );
        assert_eq!(
            lines,
            [
                format!("Download action repository 'actions/checkout@v4' (SHA:{PARENT_SHA})"),
                format!("Download action repository 'actions/checkout@v5' (SHA:{OTHER_SHA})"),
            ]
        );
    }

    /// Runs the Set up job section for `job_message` with no steps and returns its log
    /// text. Pins come from the message as `run_job` reads them; `setup_lines` stand in for
    /// action preparation output.
    async fn set_up_job_log(job_message: &serde_json::Value, setup_lines: Vec<String>) -> String {
        let workspace = tempfile::TempDir::new().unwrap();
        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        job.actions_dependencies = actions_dependencies(job_message);
        job.setup_action_lines = setup_lines;
        let queue = std::sync::Arc::new(tokio::sync::Mutex::new(
            crate::worker::server_queue::ServerQueue::new("job".into(), "plan".into()),
        ));
        let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        crate::worker::steps_runner::run_steps(
            &[],
            &mut job,
            workspace.path().to_str().unwrap(),
            cancel_rx,
            queue.clone(),
            None,
            None,
            &[],
            None,
            None,
        )
        .await
        .unwrap();
        let mut queue = queue.lock().await;
        queue.all_step_log_content().unwrap()
    }

    /// The announcement reaches the Set up job log once, after the proxy lines and before
    /// `Prepare workflow directory`, where the official runner prints it.
    #[tokio::test]
    async fn set_up_job_announces_pinned_jobs_once_at_the_official_position() {
        let pinned = serde_json::json!({ "dependencies": [DEPENDENCY] });
        let log = set_up_job_log(&pinned, Vec::new()).await;
        assert_eq!(log.matches(LOCKED_ACTIONS_ANNOUNCEMENT).count(), 1, "{log}");
        let announced = log.find(LOCKED_ACTIONS_ANNOUNCEMENT).unwrap();
        let prepared = log.find("Prepare workflow directory").unwrap();
        assert!(announced < prepared, "{log}");
    }

    /// Jobs without pins, and jobs carrying only the camelCase key, print no announcement.
    #[tokio::test]
    async fn set_up_job_omits_the_announcement_without_wire_pins() {
        for message in [
            serde_json::json!({}),
            serde_json::json!({ "dependencies": [] }),
            serde_json::json!({ "actionsDependencies": [DEPENDENCY] }),
        ] {
            let log = set_up_job_log(&message, Vec::new()).await;
            assert!(
                !log.contains(LOCKED_ACTIONS_ANNOUNCEMENT),
                "{message}: {log}"
            );
        }
    }

    /// Action preparation lines print between `Prepare all required actions` and
    /// `Complete job name`, as in the official Set up job log.
    #[tokio::test]
    async fn set_up_job_prints_action_preparation_lines_in_order() {
        let download = format!("Download action repository 'owner/parent@v1' (SHA:{PARENT_SHA})");
        let log = set_up_job_log(
            &serde_json::json!({}),
            vec!["Getting action download info".to_owned(), download.clone()],
        )
        .await;
        let prepare = log.find("Prepare all required actions").unwrap();
        let getting = log.find("Getting action download info").unwrap();
        let downloaded = log.find(&download).unwrap();
        let complete = log.find("Complete job name:").unwrap();
        assert!(
            prepare < getting && getting < downloaded && downloaded < complete,
            "{log}"
        );
    }

    /// A ref that did not resolve to a commit is refused by the download. Naming it as a
    /// SHA would mislead the log reader, so the announcement is withheld.
    #[tokio::test]
    async fn unresolved_ref_is_not_announced_as_a_sha() {
        let temp = tempfile::TempDir::new().unwrap();
        let workspace = temp.path().join("_work").join("repo");
        std::fs::create_dir_all(&workspace).unwrap();
        let mut setup_lines = Vec::new();
        let result = prepare_remote_actions(
            &serde_json::json!({ "jobId": "job-1" }),
            workspace.to_str().unwrap(),
            &[action_step("owner/unresolved@v1")],
            "plan-1",
            &mut setup_lines,
        )
        .await;
        assert!(result.is_err());
        assert_eq!(setup_lines, ["Getting action download info".to_owned()]);
    }
}
