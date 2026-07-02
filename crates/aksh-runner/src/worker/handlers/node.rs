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
) -> Result<()> {
    let main = manifest
        .runs_main
        .as_deref()
        .context("node action missing runs.main")?;

    let entry_point = action_dir.join(main);
    if !entry_point.exists() {
        anyhow::bail!("action entry point not found: {}", entry_point.display());
    }

    // Resolve node binary
    let node_version = match manifest.runs_using.as_str() {
        "node24" => "node24",
        _ => "node20", // node12/16 mapped to node20
    };

    let runner_root = Path::new(workspace)
        .parent()
        .and_then(|p| p.parent())
        .unwrap_or(Path::new("."));
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

    // Build environment with INPUT_* variables
    let mut env = ctx.build_env();
    if let Some(inputs) = with.as_object() {
        for (key, value) in inputs {
            let env_key = format!("INPUT_{}", key.to_uppercase().replace(' ', "_"));
            if let Some(val_str) = value.as_str() {
                env.insert(env_key, val_str.to_string());
            } else {
                env.insert(env_key, value.to_string());
            }
        }
    }

    // Apply defaults from manifest inputs
    if let Some(manifest_inputs) = &manifest.inputs {
        for (key, input_def) in manifest_inputs {
            let env_key = format!("INPUT_{}", key.to_uppercase().replace(' ', "_"));
            if !env.contains_key(&env_key) {
                if let Some(default) = input_def.get("default").and_then(|v| v.as_str()) {
                    env.insert(env_key, default.to_string());
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
        None,
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
