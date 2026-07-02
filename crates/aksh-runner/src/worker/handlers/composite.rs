//! Composite action handler.

use anyhow::{Context, Result};
use std::path::Path;
use tracing::info;

use super::factory::ActionManifest;
use crate::worker::execution_context::StepContext;

/// Run a composite action.
pub async fn run_composite_action(
    manifest: &ActionManifest,
    action_dir: &Path,
    with: &serde_json::Value,
    workspace: &str,
    ctx: &mut StepContext<'_>,
) -> Result<()> {
    info!("Running composite action from {}", action_dir.display());

    let steps = manifest
        .runs_steps
        .as_ref()
        .context("composite action missing runs.steps")?;

    // Set up inputs context for composite steps
    let mut input_env = std::collections::HashMap::new();
    if let Some(inputs) = with.as_object() {
        for (key, value) in inputs {
            let env_key = format!("INPUT_{}", key.to_uppercase().replace(' ', "_"));
            if let Some(val_str) = value.as_str() {
                input_env.insert(env_key, val_str.to_string());
            }
        }
    }

    // Apply defaults from manifest
    if let Some(manifest_inputs) = &manifest.inputs {
        for (key, input_def) in manifest_inputs {
            let env_key = format!("INPUT_{}", key.to_uppercase().replace(' ', "_"));
            if !input_env.contains_key(&env_key) {
                if let Some(default) = input_def.get("default").and_then(|v| v.as_str()) {
                    input_env.insert(env_key, default.to_string());
                }
            }
        }
    }

    // GITHUB_ACTION_PATH for composite steps
    ctx.env.insert(
        "GITHUB_ACTION_PATH".to_string(),
        action_dir.to_string_lossy().to_string(),
    );

    for (k, v) in &input_env {
        ctx.env.insert(k.clone(), v.clone());
    }

    // Execute each composite step
    for step in steps {
        let step_run = step.get("run").and_then(|v| v.as_str());
        let step_uses = step.get("uses").and_then(|v| v.as_str());
        let step_shell = step.get("shell").and_then(|v| v.as_str());
        let step_name = step
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("composite step");

        info!("  Composite step: {step_name}");

        if let Some(script) = step_run {
            super::script::run_script(script, step_shell, workspace, ctx, None).await?;
        } else if let Some(uses) = step_uses {
            let with = step
                .get("with")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            super::action::run_action(uses, &with, workspace, ctx).await?;
        }
    }

    Ok(())
}
