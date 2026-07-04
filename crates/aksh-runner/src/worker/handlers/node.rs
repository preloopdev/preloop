//! Node.js action handler.

use anyhow::{Context, Result};
use std::path::Path;
use tracing::info;

use super::factory::ActionManifest;
use crate::process;
use crate::worker::execution_context::StepContext;

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
        .get("__aksh_entry")
        .and_then(|v| v.as_str())
        .or(manifest.runs_main.as_deref())
        .context("node action missing runs.main")?;

    let entry_point = action_dir.join(main);
    if !entry_point.exists() {
        anyhow::bail!("action entry point not found: {}", entry_point.display());
    }

    // Resolve node binary; warn on deprecated node versions
    let runs_using = manifest.runs_using.as_str();
    if runs_using == "node12" || runs_using == "node16" {
        tracing::warn!(
            "Node.js {} actions are deprecated. Action authors should update to use node20 or later.",
            &runs_using[4..]
        );
    }
    let node_version = match runs_using {
        "node24" => "node24",
        _ => "node20", // node12/16 mapped to node20
    };

    let mut runner_root = Path::new(workspace).to_path_buf();
    while !runner_root.join("externals").exists() {
        if let Some(parent) = runner_root.parent() {
            runner_root = parent.to_path_buf();
        } else {
            // Fallback if we hit root without finding it
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
        // Fallback to PATH
        "node".to_string()
    };

    // Build environment with INPUT_* variables, evaluating any ${{ }} expressions
    let mut env = ctx.build_env();
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

    // Set GITHUB_ACTION_PATH
    env.insert(
        "GITHUB_ACTION_PATH".to_string(),
        action_dir.to_string_lossy().to_string(),
    );

    info!("Running node action: {node_path} {}", entry_point.display());

    let result = process::invoke(
        &node_path,
        &[entry_point.to_str().unwrap_or("")],
        Path::new(workspace),
        &env,
        None,
        Some(cancel_rx),
    )
    .await?;

    for line in &result.lines {
        ctx.log(line);
    }

    if result.exit_code != 0 {
        anyhow::bail!("node action exited with code {}", result.exit_code);
    }

    Ok(())
}
