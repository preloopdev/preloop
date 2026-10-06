//! Node.js action handler.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tracing::info;

use super::factory::ActionManifest;
use crate::process;
use crate::worker::execution_context::StepContext;

/// Pre-`/home/runner` goldens baked node externals at this absolute path.
/// The upward walk from a new-layout workspace can never reach it, so it
/// serves as a last-resort search root (probed, never assumed).
const LEGACY_BAKED_EXTERNALS_ROOT: &str = "/var/lib/preloop-runner";

/// Find the runner root whose `externals/` holds the bundled node runtimes:
/// walk up from the workspace, falling back to the legacy baked location
/// (pre-root-move goldens), then to the workspace grandparent.
fn runner_root_for_externals(workspace: &Path, legacy_root: &Path) -> std::path::PathBuf {
    let mut runner_root = workspace.to_path_buf();
    while !runner_root.join("externals").exists() {
        if let Some(parent) = runner_root.parent() {
            runner_root = parent.to_path_buf();
        } else {
            break;
        }
    }
    if !runner_root.join("externals").exists() && legacy_root.join("externals").exists() {
        return legacy_root.to_path_buf();
    }
    if runner_root.join("externals").exists() {
        return runner_root;
    }
    workspace
        .parent()
        .and_then(|p| p.parent())
        .unwrap_or(Path::new("."))
        .to_path_buf()
}

fn system_node_major(node_path: &str) -> Result<u64> {
    let output = std::process::Command::new(node_path)
        .arg("--version")
        .output()
        .with_context(|| format!("checking Node.js runtime at {node_path}"))?;
    if !output.status.success() {
        anyhow::bail!(
            "Node.js runtime at {node_path} failed --version with {}",
            output.status
        );
    }
    parse_node_major(&String::from_utf8_lossy(&output.stdout))
}

fn parse_node_major(version: &str) -> Result<u64> {
    version
        .trim()
        .strip_prefix('v')
        .and_then(|value| value.split('.').next())
        .and_then(|value| value.parse::<u64>().ok())
        .with_context(|| format!("could not parse Node.js version {version:?}"))
}

/// Extract the required major version from a `node_version` string like
/// "node20", "node24", etc. Used to validate system-Node fallback.
fn required_major_from_version(node_version: &str) -> Option<u64> {
    node_version
        .strip_prefix("node")
        .and_then(|v| v.parse::<u64>().ok())
}

#[test]
fn parses_system_node_major_version() {
    assert_eq!(parse_node_major("v20.19.0\n").unwrap(), 20);
    assert_eq!(parse_node_major("v24.3.0\n").unwrap(), 24);
    assert!(parse_node_major("node-unknown\n").is_err());
}

#[test]
fn required_major_from_version_extracts_correctly() {
    assert_eq!(required_major_from_version("node20"), Some(20));
    assert_eq!(required_major_from_version("node24"), Some(24));
    assert_eq!(required_major_from_version("node16"), Some(16));
    assert_eq!(required_major_from_version(""), None);
    assert_eq!(required_major_from_version("python3"), None);
}

/// Resolve the Node.js runtime for an action from its `runs.using` value.
///
/// GitHub removed Node 12, 16, and 20 for JavaScript actions on
/// September 23, 2026, and also deleted the
/// `ACTIONS_ALLOW_USE_UNSECURE_NODE_VERSION` opt-out. Node 22 was never a
/// supported action runtime. Steps using one of those values fail here with
/// an actionable error instead of silently running on a wrong runtime.
/// Anything unrecognized (including an empty value) resolves to Node 24.
fn resolve_node_version(
    runs_using: &str,
    target_os: &str,
    target_arch: &str,
) -> Result<&'static str> {
    match runs_using {
        "node12" | "node16" | "node20" => {
            let major = &runs_using[4..];
            anyhow::bail!(
                "Node.js {major} actions are no longer available: GitHub removed Node.js {major} \
                 for JavaScript actions on September 23, 2026. Update the action's action.yml to \
                 `runs.using: node24`."
            );
        }
        "node22" => {
            anyhow::bail!(
                "Node.js 22 is not a supported runtime for JavaScript actions: GitHub never \
                 shipped a Node 22 runtime, and Node.js 12/16/20 were removed on \
                 September 23, 2026. Update the action's action.yml to `runs.using: node24`."
            );
        }
        _ => {
            if target_os == "linux" && target_arch == "arm" {
                anyhow::bail!(
                    "Node.js 24 is not available on Linux ARM32, and Node.js 20 (the previous \
                     fallback) was removed on September 23, 2026. This runner cannot execute \
                     JavaScript actions on this platform."
                );
            }
            Ok("node24")
        }
    }
}

/// Resolve a Node action's `runs.main` entry point under `action_dir` and
/// contain the canonical result in the action's repository root.
///
/// Real actions keep entry points outside their own subdir: actions/cache's
/// `restore` sub-action runs `../dist/restore-only/index.js`, gradle's
/// setup-gradle runs `../dist/...`, codeql's init runs `../lib/...` (all
/// resolve inside the repo; see issue #291). The official runner performs no
/// such check; bounding by repo root instead of the action subdir keeps the
/// sandbox while matching legitimate layouts. A result outside the repo root
/// is rejected.
fn resolve_contained_entry_point(action_dir: &Path, main: &str) -> Result<PathBuf> {
    let entry_point = action_dir.join(main);
    if !entry_point.exists() {
        anyhow::bail!("action entry point not found: {}", entry_point.display());
    }

    let containment_root = super::composite::actions_tarball_root(action_dir)
        .unwrap_or_else(|| action_dir.to_path_buf());
    if let (Ok(canonical_dir), Ok(canonical_entry)) =
        (containment_root.canonicalize(), entry_point.canonicalize())
    {
        if !canonical_entry.starts_with(&canonical_dir) {
            anyhow::bail!(
                "action entry point {} escapes action directory {}",
                entry_point.display(),
                containment_root.display()
            );
        }
        return Ok(canonical_entry);
    }
    Ok(entry_point)
}

/// Run a Node.js action.
pub async fn run_node_action(
    manifest: &ActionManifest,
    action_dir: &Path,
    with: &serde_json::Value,
    workspace: &str,
    ctx: &mut StepContext<'_>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let main = with
        .get("__preloop_entry")
        .and_then(|v| v.as_str())
        .or(manifest.runs_main.as_deref())
        .context("node action missing runs.main")?;

    let entry_point = resolve_contained_entry_point(action_dir, main)?;

    // Resolve the Node.js runtime. Removed majors (12/16/20) and the
    // never-supported node22 fail the step here with an actionable error.
    let runs_using = manifest.runs_using.as_str();
    let node_version =
        resolve_node_version(runs_using, std::env::consts::OS, std::env::consts::ARCH)?;

    // Build environment with INPUT_* variables, evaluating any ${{ }} expressions.
    let mut env = ctx.build_env();
    // INPUT_* scope is per action step: strip anything inherited from an
    // enclosing composite so a nested action never sees the parent's
    // inputs. (junit's run-gradle composite exports INPUT_ARGUMENTS for
    // its own run steps; without scoping, the nested setup-gradle node
    // action inherits it and fails on its removed `arguments` parameter.)
    // The step's own `with:` inputs and manifest defaults are added back
    // below; composite RUN steps are unaffected (script path).
    env.retain(|key, _| !key.starts_with("INPUT_"));

    let expr_ctx_for_inputs = ctx.build_expression_context();
    if let Some(inputs) = with.as_object() {
        for (key, value) in inputs {
            if key.starts_with("__preloop_") {
                continue;
            }
            let env_key = format!("INPUT_{}", key.to_uppercase().replace(' ', "_"));
            let raw = if let Some(val_str) = value.as_str() {
                val_str.to_string()
            } else {
                value.to_string()
            };
            let evaluated = crate::worker::template::evaluate_template(&raw, &expr_ctx_for_inputs)
                .unwrap_or(raw);
            env.insert(env_key, evaluated);
        }
    }

    // Apply defaults from manifest inputs, evaluating any ${{ }} expressions.
    if let Some(manifest_inputs) = &manifest.inputs {
        let expr_ctx = ctx.build_expression_context();
        for (key, input_def) in manifest_inputs {
            let env_key = format!("INPUT_{}", key.to_uppercase().replace(' ', "_"));
            let default = input_def
                .get("default")
                .and_then(super::factory::input_default_string);
            if let Some(default) = default {
                env.entry(env_key).or_insert_with(|| {
                    crate::worker::template::evaluate_template(&default, &expr_ctx)
                        .unwrap_or_else(|_| default.to_string())
                });
            }
        }
    }

    // Emit deprecation warnings for inputs with deprecationMessage
    if let Some(manifest_inputs) = &manifest.inputs {
        for (key, input_def) in manifest_inputs {
            if let Some(msg) = input_def.get("deprecationMessage").and_then(|v| v.as_str())
                && !msg.is_empty()
            {
                let env_key = format!("INPUT_{}", key.to_uppercase().replace(' ', "_"));
                if env.contains_key(&env_key) {
                    tracing::warn!("Input '{key}' has been deprecated: {msg}");
                    ctx.log(&format!(
                        "::warning::Input '{key}' has been deprecated with message: {msg}"
                    ));
                }
            }
        }
    }

    // The host-side bundled-Node probe and its system-Node fallback live
    // *after* the container branch below: a container job runs Node inside
    // the container, and a probe against the runner's own filesystem must not
    // decide whether that path can run.

    // Set GITHUB_ACTION_PATH
    env.insert(
        "GITHUB_ACTION_PATH".to_string(),
        action_dir.to_string_lossy().to_string(),
    );

    // A job container runs every step inside it — the official runner execs
    // node actions through `docker exec` too, using the externals mounted at
    // `/__e`. Running the host binary against container paths fails two ways:
    // the action cannot see its inputs at their advertised locations, and the
    // host-side externals probe reports `bundled nodeXX is missing` whenever
    // the runner process cannot read the mount source, killing the job before
    // the container ever gets a chance. The container path runs first.
    let job_container_id = ctx
        .job
        .container_state
        .as_ref()
        .and_then(|state| state.job_container_id.clone());
    if let Some(container_id) = job_container_id {
        let host_work = Path::new(workspace)
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();
        // `/__e` is the read-only externals mount, sourced from
        // `{runner_work}/../externals` (steps_runner.rs) — and `host_work` is
        // that same runner work root, so this is exactly the path docker
        // mounts. The probe is *advisory*: `is_file()` cannot distinguish an
        // EACCES'd directory from an absent one, and the container (which the
        // daemon mounts as root) may see the runtime regardless. Log and let
        // docker exec produce the real error instead of failing here.
        let mounted_node = Path::new(&host_work)
            .join("..")
            .join("externals")
            .join(node_version)
            .join("bin")
            .join("node");
        if !mounted_node.is_file() {
            tracing::warn!(
                path = %mounted_node.display(),
                "container externals probe cannot see the bundled Node; trying docker exec anyway"
            );
        }
        let container_node = format!("/__e/{node_version}/bin/node");
        for key in [
            "GITHUB_WORKSPACE",
            "GITHUB_ENV",
            "GITHUB_PATH",
            "GITHUB_OUTPUT",
            "GITHUB_STATE",
            "GITHUB_STEP_SUMMARY",
            "GITHUB_ARTIFACTS",
            "GITHUB_ARTIFACTS_LIST",
            "GITHUB_ACTION_PATH",
            "RUNNER_TEMP",
            "RUNNER_TOOL_CACHE",
        ] {
            if let Some(val) = env.get(key).cloned() {
                env.insert(
                    key.to_string(),
                    crate::worker::container_ops::translate_to_container_path(&val, &host_work),
                );
            }
        }
        env.insert("HOME".to_string(), "/github/home".to_string());
        let container_workdir =
            crate::worker::container_ops::translate_to_container_path(workspace, &host_work);
        let container_entry = crate::worker::container_ops::translate_to_container_path(
            &entry_point.to_string_lossy(),
            &host_work,
        );
        info!(
            "Running node action in container {container_id}: {container_node} {container_entry}"
        );
        ctx.debug(&format!(
            "Command line: docker exec -i {container_id} {container_node} [{container_entry}]"
        ));
        let ctx_ref = &mut *ctx;
        let on_chunk = Box::new(move |chunk: &[u8]| {
            ctx_ref.write_chunk(chunk);
        });
        let result = crate::worker::container_ops::docker_exec(
            &container_id,
            &container_node,
            &[container_entry.as_str()],
            &container_workdir,
            &env,
            Some(cancel_rx),
            Some(on_chunk),
        )
        .await?;
        if result.exit_code != 0 {
            anyhow::bail!("node action exited with code {}", result.exit_code);
        }
        return Ok(());
    }

    // Not a container job: resolve the bundled Node on the runner's own
    // filesystem. `runner_root_for_externals` walks up from the workspace to
    // the runner root that carries `externals/`.
    let runner_root =
        runner_root_for_externals(Path::new(workspace), Path::new(LEGACY_BAKED_EXTERNALS_ROOT));
    let bundled_node = if cfg!(target_os = "windows") {
        runner_root
            .join("externals")
            .join(node_version)
            .join("node.exe")
    } else {
        runner_root
            .join("externals")
            .join(node_version)
            .join("bin")
            .join("node")
    };
    // The official runner invokes the bundled Node by absolute path and never
    // prepends its directory to PATH. Child processes inherit the job's PATH
    // unchanged — this is what lets setup-node's toolcache entry win.
    let node_path = if bundled_node.is_file() {
        bundled_node.to_string_lossy().to_string()
    } else {
        // Bundled externals are missing (--no-externals). Fall back to system
        // Node, but enforce that its major version matches what the action
        // declared. Without this, a node24 action silently runs on Node 20.
        let required = required_major_from_version(node_version);
        let path = "node";
        let major = system_node_major(path).with_context(|| {
            format!(
                "bundled {node_version} is missing at {} and system node is unusable; \
                 run `configure` without --no-externals to download it",
                bundled_node.display()
            )
        })?;
        if let Some(req) = required
            && major != req
        {
            anyhow::bail!(
                "bundled {node_version} is missing at {}; system Node is v{major} \
                     but the action requires Node {req}",
                bundled_node.display()
            );
        }
        info!("Bundled {node_version} not found, using system Node v{major} (--no-externals)");
        path.to_owned()
    };

    info!("Running node action: {node_path} {}", entry_point.display());
    let ctx_ref = &mut *ctx;
    let on_chunk = Box::new(move |chunk: &[u8]| {
        ctx_ref.write_chunk(chunk);
    });

    let result = process::invoke(
        &node_path,
        &[entry_point.to_str().unwrap_or("")],
        Path::new(workspace),
        &env,
        Some(on_chunk),
        Some(cancel_rx),
        false,
    )
    .await?;

    if result.exit_code != 0 {
        anyhow::bail!("node action exited with code {}", result.exit_code);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::handlers::factory::ActionManifest;

    fn node_manifest(main: &str) -> ActionManifest {
        ActionManifest {
            name: "node".into(),
            description: String::new(),
            runs_using: "node20".into(),
            runs_main: Some(main.into()),
            runs_pre: None,
            runs_pre_if: None,
            runs_post: None,
            runs_post_if: None,
            runs_steps: None,
            runs_image: None,
            runs_entrypoint: None,
            runs_args: None,
            runs_env: None,
            inputs: None,
            outputs: None,
        }
    }

    fn resolve(runs_using: &str, target_os: &str, target_arch: &str) -> Result<&'static str> {
        resolve_node_version(runs_using, target_os, target_arch)
    }

    #[test]
    fn removed_node_majors_fail_with_actionable_error() {
        for runs_using in ["node12", "node16", "node20"] {
            let err = resolve(runs_using, "linux", "x64").unwrap_err();
            let major = &runs_using[4..];
            let message = err.to_string();
            assert!(
                message.contains(&format!("Node.js {major} actions are no longer available")),
                "{runs_using}: unexpected error: {message}"
            );
            assert!(
                message.contains("September 23, 2026"),
                "{runs_using}: unexpected error: {message}"
            );
            assert!(
                message.contains("runs.using: node24"),
                "{runs_using}: unexpected error: {message}"
            );
        }
    }

    #[test]
    fn node22_fails_because_it_was_never_supported() {
        let err = resolve("node22", "linux", "x64").unwrap_err();
        let message = err.to_string();
        assert!(message.contains("never"), "unexpected error: {message}");
        assert!(
            message.contains("September 23, 2026"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("runs.using: node24"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn node24_resolves_on_supported_platforms() {
        for (target_os, target_arch) in [
            ("linux", "x64"),
            ("linux", "aarch64"),
            ("darwin", "arm"),
            ("darwin", "x64"),
            ("windows", "x64"),
        ] {
            let version = resolve("node24", target_os, target_arch).unwrap();
            assert_eq!(version, "node24", "{target_os}/{target_arch}");
        }
    }

    #[test]
    fn unknown_using_values_fall_back_to_node24() {
        for runs_using in ["", "node18", "node25", "bun"] {
            let version = resolve(runs_using, "linux", "x64").unwrap();
            assert_eq!(version, "node24", "{runs_using:?}");
        }
    }

    #[test]
    fn node24_on_linux_arm32_fails_with_clear_message() {
        let err = resolve("node24", "linux", "arm").unwrap_err();
        let message = err.to_string();
        assert!(message.contains("ARM32"), "unexpected error: {message}");
        assert!(
            message.contains("September 23, 2026"),
            "unexpected error: {message}"
        );
    }

    /// A node20 action fails the step outright — it is neither silently run
    /// on Node 20 nor upgraded. The error tells the author exactly what to do.
    #[tokio::test]
    async fn node20_action_fails_step_with_removal_error() {
        let tmp = tempfile::tempdir().unwrap();
        let action_dir = tmp.path().join("action");
        std::fs::create_dir_all(&action_dir).unwrap();
        std::fs::write(action_dir.join("index.js"), "console.log('hi')").unwrap();

        let manifest = node_manifest("index.js");
        assert_eq!(manifest.runs_using, "node20");

        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let mut ctx = StepContext::new(&mut job, "step1".into(), "Step".into());
        let (_tx, cancel_rx) = tokio::sync::watch::channel(false);

        let err = run_node_action(
            &manifest,
            &action_dir,
            &serde_json::json!({}),
            action_dir.to_str().unwrap(),
            &mut ctx,
            cancel_rx,
        )
        .await
        .unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("no longer available"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("September 23, 2026"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("runs.using: node24"),
            "unexpected error: {message}"
        );
    }

    #[tokio::test]
    async fn missing_entry_point_errors() {
        let dir = tempfile::TempDir::new().unwrap();
        let manifest = node_manifest("does_not_exist.js");
        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let mut ctx = StepContext::new(&mut job, "step1".into(), "Step".into());
        let (_tx, cancel_rx) = tokio::sync::watch::channel(false);

        let err = run_node_action(
            &manifest,
            dir.path(),
            &serde_json::json!({}),
            dir.path().to_str().unwrap(),
            &mut ctx,
            cancel_rx,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("entry point not found"));
    }

    #[tokio::test]
    async fn missing_runs_main_errors() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut manifest = node_manifest("index.js");
        manifest.runs_main = None;
        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let mut ctx = StepContext::new(&mut job, "step1".into(), "Step".into());
        let (_tx, cancel_rx) = tokio::sync::watch::channel(false);

        let err = run_node_action(
            &manifest,
            dir.path(),
            &serde_json::json!({}),
            dir.path().to_str().unwrap(),
            &mut ctx,
            cancel_rx,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("missing runs.main"));
    }

    /// Issue #291: actions/cache v6 ships split sub-actions (`restore`, `save`)
    /// whose `runs.main` is `../dist/<name>/index.js` — above their own
    /// subdir but inside the repository. The entry point must be accepted
    /// when it resolves inside the repo root.
    #[test]
    fn subaction_shared_dist_entry_point_accepted() {
        let temp = tempfile::TempDir::new().unwrap();
        // Mirror the on-disk layout: _actions/actions/cache/<sha>/restore
        let repo_root = temp
            .path()
            .join("_actions")
            .join("actions")
            .join("cache")
            .join("55cc8345863c7cc4c66a329aec7e433d2d1c52a9");
        let subaction_dir = repo_root.join("restore");
        let dist_file = repo_root.join("dist").join("restore-only").join("index.js");
        std::fs::create_dir_all(&subaction_dir).unwrap();
        std::fs::create_dir_all(dist_file.parent().unwrap()).unwrap();
        std::fs::write(&dist_file, "console.log('restore');").unwrap();

        let resolved =
            resolve_contained_entry_point(&subaction_dir, "../dist/restore-only/index.js").unwrap();
        assert_eq!(resolved, dist_file.canonicalize().unwrap());
    }

    /// The repo-root containment still rejects true escapes: an entry point
    /// resolving outside the repository root must fail closed.
    #[test]
    fn entry_point_escaping_repo_root_rejected() {
        let temp = tempfile::TempDir::new().unwrap();
        let repo_root = temp
            .path()
            .join("_actions")
            .join("actions")
            .join("cache")
            .join("55cc8345863c7cc4c66a329aec7e433d2d1c52a9");
        let subaction_dir = repo_root.join("restore");
        std::fs::create_dir_all(&subaction_dir).unwrap();
        // Payload lives outside the repo root entirely: five levels up from
        // `restore/` reaches the temp dir itself.
        let evil = temp.path().join("evil.js");
        std::fs::write(&evil, "console.log('evil');").unwrap();

        let err =
            resolve_contained_entry_point(&subaction_dir, "../../../../../evil.js").unwrap_err();
        assert!(
            err.to_string().contains("escapes action directory"),
            "unexpected error: {err:#}"
        );
    }

    /// Without an `_actions/` marker in the path (e.g. local actions), the
    /// containment root falls back to the action directory itself, and a
    /// plain in-directory entry point is accepted.
    #[test]
    fn entry_point_without_actions_marker_contained_in_action_dir() {
        let temp = tempfile::TempDir::new().unwrap();
        let action_dir = temp.path().join("my-action");
        std::fs::create_dir_all(&action_dir).unwrap();
        let index = action_dir.join("index.js");
        std::fs::write(&index, "console.log('hi');").unwrap();

        let resolved = resolve_contained_entry_point(&action_dir, "index.js").unwrap();
        assert_eq!(resolved, index.canonicalize().unwrap());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn escaping_entry_point_symlink_errors() {
        let temp = tempfile::TempDir::new().unwrap();
        let action_dir = temp.path().join("action");
        std::fs::create_dir_all(&action_dir).unwrap();

        let outside_dir = temp.path().join("outside");
        std::fs::create_dir_all(&outside_dir).unwrap();
        let outside_script = outside_dir.join("payload.js");
        std::fs::write(&outside_script, "console.log('evil');").unwrap();

        std::os::unix::fs::symlink(&outside_script, action_dir.join("index.js")).unwrap();

        let manifest = node_manifest("index.js");
        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let mut ctx = StepContext::new(&mut job, "step1".into(), "Step".into());
        let (_tx, cancel_rx) = tokio::sync::watch::channel(false);

        let err = run_node_action(
            &manifest,
            &action_dir,
            &serde_json::json!({}),
            action_dir.to_str().unwrap(),
            &mut ctx,
            cancel_rx,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("escapes action directory"));
    }

    #[test]
    fn externals_search_prefers_walk_falls_back_to_legacy() {
        // New-layout workspace (`<root>/_work/<repo>/<repo>`) whose ancestors
        // carry no externals, with a legacy baked tree elsewhere: the legacy
        // root wins over the workspace-grandparent fallback.
        let tmp = tempfile::TempDir::new().unwrap();
        let workspace = tmp
            .path()
            .join("home")
            .join("runner")
            .join("_work")
            .join("repo")
            .join("repo");
        std::fs::create_dir_all(&workspace).unwrap();
        let legacy = tmp.path().join("legacyroot");
        std::fs::create_dir_all(legacy.join("externals")).unwrap();
        assert_eq!(runner_root_for_externals(&workspace, &legacy), legacy,);
        // Walk hit still wins when present.
        let rooted = tmp.path().join("rooted");
        let ws2 = rooted.join("_work").join("r").join("r");
        std::fs::create_dir_all(rooted.join("externals")).unwrap();
        std::fs::create_dir_all(&ws2).unwrap();
        assert_eq!(runner_root_for_externals(&ws2, &legacy), rooted);
        // Neither: workspace grandparent, as before.
        let bare = tmp.path().join("bare").join("_work").join("r").join("r");
        std::fs::create_dir_all(&bare).unwrap();
        assert_eq!(
            runner_root_for_externals(&bare, &tmp.path().join("nolegacy")),
            tmp.path().join("bare").join("_work"),
        );
    }
}
