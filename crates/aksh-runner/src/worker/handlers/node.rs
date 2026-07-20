//! Node.js action handler.

use anyhow::{Context, Result};
use std::path::Path;
use tracing::info;

use super::factory::ActionManifest;
use crate::process;
use crate::worker::execution_context::StepContext;

const FORCE_NODE24: &str = "FORCE_JAVASCRIPT_ACTIONS_TO_NODE24";
const ALLOW_UNSECURE_NODE: &str = "ACTIONS_ALLOW_USE_UNSECURE_NODE_VERSION";

#[derive(Debug, PartialEq, Eq)]
struct NodeSelection {
    version: &'static str,
    warnings: Vec<String>,
}

#[derive(Clone, Copy)]
struct MigrationFlag {
    is_true: bool,
    from_workflow: bool,
    from_system: bool,
}

fn node_bool(value: &str) -> bool {
    value == "1" || value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("$true")
}

fn migration_flag(
    name: &str,
    workflow_env: &std::collections::HashMap<String, String>,
    system_value: Option<&str>,
) -> MigrationFlag {
    let workflow_value = workflow_env.get(name);
    MigrationFlag {
        is_true: workflow_value
            .map(|value| node_bool(value))
            .unwrap_or_else(|| system_value.is_some_and(node_bool)),
        from_workflow: workflow_value.is_some(),
        from_system: system_value.is_some_and(|value| !value.is_empty()),
    }
}

#[allow(clippy::too_many_arguments)]
fn resolve_node_version(
    runs_using: &str,
    workflow_env: &std::collections::HashMap<String, String>,
    system_force_node24: Option<&str>,
    system_allow_unsecure_node: Option<&str>,
    use_node24_by_default: bool,
    require_node24: bool,
    target_os: &str,
    target_arch: &str,
) -> NodeSelection {
    let mut warnings = Vec::new();
    let mut version = match runs_using {
        "node20" if require_node24 => "node24",
        "node20" => {
            let force_node24 = migration_flag(FORCE_NODE24, workflow_env, system_force_node24);
            let allow_unsecure = migration_flag(
                ALLOW_UNSECURE_NODE,
                workflow_env,
                system_allow_unsecure_node,
            );
            let both_from_workflow = force_node24.is_true
                && allow_unsecure.is_true
                && force_node24.from_workflow
                && allow_unsecure.from_workflow;
            let both_from_system = force_node24.is_true
                && allow_unsecure.is_true
                && force_node24.from_system
                && allow_unsecure.from_system;
            if both_from_workflow || both_from_system {
                let source = if both_from_workflow {
                    "workflow"
                } else {
                    "system"
                };
                let default_version = if use_node24_by_default {
                    "node24"
                } else {
                    "node20"
                };
                warnings.push(format!(
                    "Both {FORCE_NODE24} and {ALLOW_UNSECURE_NODE} environment variables are set to true in the {source} environment. This is likely a configuration error. Using the default Node version: {default_version}."
                ));
                default_version
            } else if use_node24_by_default {
                if allow_unsecure.is_true {
                    "node20"
                } else {
                    "node24"
                }
            } else if force_node24.is_true {
                "node24"
            } else {
                "node20"
            }
        }
        "node24" => "node24",
        "node22" => "node22",
        _ => "node20",
    };

    if version == "node24" && target_os == "linux" && target_arch == "arm" {
        version = "node20";
        warnings.push(
            "Node 24 is not supported on Linux ARM32 platforms. Falling back to Node 20."
                .to_owned(),
        );
    }

    NodeSelection { version, warnings }
}

/// Run a Node.js action.
pub async fn run_node_action(
    manifest: &ActionManifest,
    action_dir: &Path,
    with: &serde_json::Value,
    workspace: &str,
    ctx: &mut StepContext<'_>,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    action_name: Option<&str>,
) -> Result<()> {
    let main = with
        .get("__aksh_entry")
        .and_then(|v| v.as_str())
        .or(manifest.runs_main.as_deref())
        .context("node action missing runs.main")?;

    let entry_point = action_dir.join(main);
    if !entry_point.exists() {
        anyhow::bail!("action entry point not found: {}", entry_point.display());
    }

    // Resolve node binary and apply the runner's Node 20 migration policy.
    let runs_using = manifest.runs_using.as_str();
    if runs_using == "node12" || runs_using == "node16" {
        tracing::warn!(
            "Node.js {} actions are deprecated. Action authors should update to use node20 or later.",
            &runs_using[4..]
        );
    }

    // Build environment with INPUT_* variables, evaluating any ${{ }} expressions.
    let mut env = ctx.build_env();
    let use_node24_by_default = ctx
        .job
        .get_variable_bool("actions.runner.usenode24bydefault");
    let require_node24 = ctx.job.get_variable_bool("actions.runner.requirenode24");
    let system_force_node24 = std::env::var(FORCE_NODE24).ok();
    let system_allow_unsecure_node = std::env::var(ALLOW_UNSECURE_NODE).ok();
    let selection = resolve_node_version(
        runs_using,
        &env,
        system_force_node24.as_deref(),
        system_allow_unsecure_node.as_deref(),
        use_node24_by_default,
        require_node24,
        std::env::consts::OS,
        std::env::consts::ARCH,
    );
    for warning in &selection.warnings {
        tracing::warn!("{warning}");
        ctx.log(&format!("::warning::{warning}"));
    }
    let node_version = selection.version;
    if runs_using == "node20" {
        if let Some(name) = action_name {
            if node_version == "node24" {
                ctx.job.record_upgraded_node24_action(name);
            } else if ctx.job.get_variable_bool("actions.runner.warnonnode20") {
                ctx.job.record_deprecated_node20_action(name);
            }
        }
    }

    let expr_ctx_for_inputs = ctx.job.build_expression_context();
    if let Some(inputs) = with.as_object() {
        for (key, value) in inputs {
            if key.starts_with("__aksh_") {
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

    // Apply defaults from manifest inputs, evaluating any ${{ }} expressions
    if let Some(manifest_inputs) = &manifest.inputs {
        let expr_ctx = ctx.job.build_expression_context();
        for (key, input_def) in manifest_inputs {
            let env_key = format!("INPUT_{}", key.to_uppercase().replace(' ', "_"));
            if let Some(default) = input_def.get("default").and_then(|v| v.as_str()) {
                env.entry(env_key).or_insert_with(|| {
                    crate::worker::template::evaluate_template(default, &expr_ctx)
                        .unwrap_or_else(|_| default.to_string())
                });
            }
        }
    }

    // P1.14: Emit deprecation warnings for inputs with deprecationMessage
    if let Some(manifest_inputs) = &manifest.inputs {
        for (key, input_def) in manifest_inputs {
            if let Some(msg) = input_def.get("deprecationMessage").and_then(|v| v.as_str()) {
                if !msg.is_empty() {
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
    }

    let mut runner_root = Path::new(workspace).to_path_buf();
    while !runner_root.join("externals").exists() {
        if let Some(parent) = runner_root.parent() {
            runner_root = parent.to_path_buf();
        } else {
            runner_root = Path::new(workspace)
                .parent()
                .and_then(|p| p.parent())
                .unwrap_or(Path::new("."))
                .to_path_buf();
            break;
        }
    }
    let node_bin = runner_root
        .join("externals")
        .join(node_version)
        .join("bin")
        .join("node");
    let node_path = if node_bin.exists() {
        node_bin.to_string_lossy().to_string()
    } else {
        "node".to_string()
    };

    // Set GITHUB_ACTION_PATH
    env.insert(
        "GITHUB_ACTION_PATH".to_string(),
        action_dir.to_string_lossy().to_string(),
    );

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
    #[allow(clippy::too_many_arguments)]
    fn resolve(
        runs_using: &str,
        workflow: &[(&str, &str)],
        system_force_node24: Option<&str>,
        system_allow_unsecure_node: Option<&str>,
        use_node24_by_default: bool,
        require_node24: bool,
        target_os: &str,
        target_arch: &str,
    ) -> NodeSelection {
        let workflow_env = workflow
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        resolve_node_version(
            runs_using,
            &workflow_env,
            system_force_node24,
            system_allow_unsecure_node,
            use_node24_by_default,
            require_node24,
            target_os,
            target_arch,
        )
    }

    #[test]
    fn system_only_force_node24_selects_node24() {
        let selection = resolve(
            "node20",
            &[],
            Some("true"),
            None,
            false,
            false,
            "linux",
            "x64",
        );

        assert_eq!(selection.version, "node24");
        assert!(selection.warnings.is_empty());
    }

    #[test]
    fn workflow_false_overrides_system_true() {
        let selection = resolve(
            "node20",
            &[(FORCE_NODE24, "false")],
            Some("true"),
            None,
            false,
            false,
            "linux",
            "x64",
        );

        assert_eq!(selection.version, "node20");
        assert!(selection.warnings.is_empty());
    }

    #[test]
    fn workflow_true_overrides_system_false() {
        let selection = resolve(
            "node20",
            &[(FORCE_NODE24, "true")],
            Some("false"),
            None,
            false,
            false,
            "linux",
            "x64",
        );

        assert_eq!(selection.version, "node24");
        assert!(selection.warnings.is_empty());
    }

    #[test]
    fn both_workflow_flags_true_use_configured_default_and_warning() {
        for (use_node24_by_default, expected_version) in [(false, "node20"), (true, "node24")] {
            let selection = resolve(
                "node20",
                &[(FORCE_NODE24, "true"), (ALLOW_UNSECURE_NODE, "true")],
                None,
                None,
                use_node24_by_default,
                false,
                "linux",
                "x64",
            );

            assert_eq!(selection.version, expected_version);
            assert_eq!(
                selection.warnings,
                vec![format!(
                    "Both {FORCE_NODE24} and {ALLOW_UNSECURE_NODE} environment variables are set to true in the workflow environment. This is likely a configuration error. Using the default Node version: {expected_version}."
                )]
            );
        }
    }

    #[test]
    fn both_system_flags_true_use_configured_default_and_warning() {
        for (use_node24_by_default, expected_version) in [(false, "node20"), (true, "node24")] {
            let selection = resolve(
                "node20",
                &[],
                Some("true"),
                Some("true"),
                use_node24_by_default,
                false,
                "linux",
                "x64",
            );

            assert_eq!(selection.version, expected_version);
            assert_eq!(
                selection.warnings,
                vec![format!(
                    "Both {FORCE_NODE24} and {ALLOW_UNSECURE_NODE} environment variables are set to true in the system environment. This is likely a configuration error. Using the default Node version: {expected_version}."
                )]
            );
        }
    }

    #[test]
    fn flags_from_different_sources_do_not_trigger_conflict_warning() {
        let selection = resolve(
            "node20",
            &[(FORCE_NODE24, "true")],
            None,
            Some("true"),
            false,
            false,
            "linux",
            "x64",
        );

        assert_eq!(selection.version, "node24");
        assert!(selection.warnings.is_empty());
    }

    #[test]
    fn require_node24_overrides_conflicting_flags_without_warning() {
        let selection = resolve(
            "node20",
            &[(FORCE_NODE24, "true"), (ALLOW_UNSECURE_NODE, "true")],
            Some("true"),
            Some("true"),
            false,
            true,
            "linux",
            "x64",
        );

        assert_eq!(selection.version, "node24");
        assert!(selection.warnings.is_empty());
    }

    #[test]
    fn dollar_true_is_accepted_for_force_node24() {
        let selection = resolve(
            "node20",
            &[(FORCE_NODE24, "$true")],
            None,
            None,
            false,
            false,
            "linux",
            "x64",
        );

        assert_eq!(selection.version, "node24");
        assert!(selection.warnings.is_empty());
    }

    #[test]
    fn linux_arm32_downgrades_selected_and_direct_node24() {
        let cases = [
            ("selected node24", "node20", &[(FORCE_NODE24, "true")][..]),
            ("direct node24", "node24", &[][..]),
        ];
        for (case, runs_using, workflow) in cases {
            let selection = resolve(
                runs_using, workflow, None, None, false, false, "linux", "arm",
            );

            assert_eq!(selection.version, "node20", "{case}");
            assert_eq!(
                selection.warnings,
                vec![
                    "Node 24 is not supported on Linux ARM32 platforms. Falling back to Node 20."
                        .to_owned()
                ],
                "{case}"
            );
        }
    }

    #[test]
    fn node24_is_preserved_on_linux_aarch64_and_non_linux_arm() {
        for (target_os, target_arch) in [("linux", "aarch64"), ("darwin", "arm")] {
            let selection = resolve(
                "node24",
                &[],
                None,
                None,
                false,
                false,
                target_os,
                target_arch,
            );

            assert_eq!(selection.version, "node24");
            assert!(selection.warnings.is_empty());
        }
    }

    #[test]
    fn legacy_node_versions_migrate_to_node20_and_node22_is_preserved() {
        for runs_using in ["node12", "node16"] {
            let selection = resolve(runs_using, &[], None, None, false, false, "linux", "x64");

            assert_eq!(selection.version, "node20", "{runs_using}");
            assert!(selection.warnings.is_empty(), "{runs_using}");
        }

        let selection = resolve("node22", &[], None, None, false, false, "linux", "x64");
        assert_eq!(selection.version, "node22");
        assert!(selection.warnings.is_empty());
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
            None,
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
            None,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("missing runs.main"));
    }
}
