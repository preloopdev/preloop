//! Composite action handler.
//!
//! F024: After running nested steps, evaluates `outputs.<name>.value` expressions
//! against the nested steps context to produce composite action outputs.
//! Nesting depth is capped at 10 (official limit).

use anyhow::{Context, Result};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use tracing::{info, warn};

use super::factory::ActionManifest;
use crate::worker::execution_context::StepContext;

/// Maximum nesting depth for composite actions.
const MAX_COMPOSITE_DEPTH: u32 = 10;

/// Run a composite action.
pub async fn run_composite_action(
    manifest: &ActionManifest,
    action_dir: &Path,
    with: &serde_json::Value,
    workspace: &str,
    ctx: &mut StepContext<'_>,
) -> Result<()> {
    run_composite_action_inner(manifest, action_dir, with, workspace, ctx, 0).await
}

fn run_composite_action_inner<'a>(
    manifest: &'a ActionManifest,
    action_dir: &'a Path,
    with: &'a serde_json::Value,
    workspace: &'a str,
    ctx: &'a mut StepContext<'_>,
    depth: u32,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        if depth >= MAX_COMPOSITE_DEPTH {
            anyhow::bail!(
                "Composite action nesting depth exceeded (max {MAX_COMPOSITE_DEPTH}): {}",
                action_dir.display()
            );
        }

        info!("Running composite action from {}", action_dir.display());

        let steps = manifest
            .runs_steps
            .as_ref()
            .context("composite action missing runs.steps")?;

        // Set up INPUT_* env from `with` inputs
        let mut input_env = std::collections::HashMap::new();
        let expr_ctx_for_inputs = ctx.job.build_expression_context();
        if let Some(inputs) = with.as_object() {
            for (key, value) in inputs {
                let env_key = format!("INPUT_{}", key.to_uppercase().replace(' ', "_"));
                let raw = if let Some(val_str) = value.as_str() {
                    val_str.to_string()
                } else {
                    value.to_string()
                };
                let evaluated =
                    crate::worker::template::evaluate_template(&raw, &expr_ctx_for_inputs)
                        .unwrap_or(raw);
                input_env.insert(env_key, evaluated);
            }
        }

        // Apply defaults from manifest for missing inputs, evaluating ${{ }} expressions
        if let Some(manifest_inputs) = &manifest.inputs {
            let expr_ctx = ctx.job.build_expression_context();
            for (key, input_def) in manifest_inputs {
                let env_key = format!("INPUT_{}", key.to_uppercase().replace(' ', "_"));
                if let Some(default) = input_def.get("default").and_then(|v| v.as_str()) {
                    input_env.entry(env_key).or_insert_with(|| {
                        crate::worker::template::evaluate_template(default, &expr_ctx)
                            .unwrap_or_else(|_| default.to_string())
                    });
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

        // Track nested step results for output evaluation (F024)
        let mut nested_step_results: indexmap::IndexMap<
            String,
            crate::worker::contexts::StepResult,
        > = indexmap::IndexMap::new();

        // Execute each composite step
        for (i, step) in steps.iter().enumerate() {
            let step_run = step.get("run").and_then(|v| v.as_str());
            let step_uses = step.get("uses").and_then(|v| v.as_str());
            let step_shell = step.get("shell").and_then(|v| v.as_str());
            let step_id = step
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or(&format!("composite_step_{i}"))
                .to_string();
            let step_name = step
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("composite step");

            info!("  Composite step: {step_name}");

            let temp_dir = Path::new(workspace)
                .parent()
                .unwrap_or(Path::new("."))
                .join("_temp");
            let file_commands = crate::worker::file_commands::create_file_commands(&temp_dir)?;
            let file_env = crate::worker::file_commands::file_command_env(&file_commands);
            let saved_file_env: Vec<(String, Option<String>)> = file_env
                .keys()
                .map(|key| (key.clone(), ctx.env.get(key).cloned()))
                .collect();
            for (k, v) in file_env {
                ctx.env.insert(k, v);
            }

            let outcome = if let Some(script) = step_run {
                // Evaluate ${{ }} expressions in composite step scripts.
                // Build an expression context that includes `inputs.*` from the
                // INPUT_* env vars set above (stripping the INPUT_ prefix and lowercasing).
                let mut expr_ctx = ctx.job.build_expression_context();
                let mut inputs_map = serde_json::Map::new();
                for (k, v) in &input_env {
                    if let Some(name) = k.strip_prefix("INPUT_") {
                        inputs_map.insert(name.to_lowercase(), serde_json::json!(v));
                    }
                }
                expr_ctx.insert("inputs", serde_json::Value::Object(inputs_map));
                let evaluated = crate::worker::template::evaluate_template(script, &expr_ctx)
                    .unwrap_or_else(|_| script.to_string());
                super::script::run_script(&evaluated, step_shell, workspace, ctx, None)
                    .await
                    .map(|_| "Success".to_string())
            } else if let Some(uses) = step_uses {
                let inner_with = step
                    .get("with")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({}));

                // Recursively run nested composite actions with depth tracking
                if uses.starts_with("./") || uses.starts_with("../") {
                    let inner_action_dir = action_dir.join(uses);
                    match super::factory::load_action_manifest(&inner_action_dir) {
                        Ok(inner_manifest) if inner_manifest.runs_using == "composite" => {
                            run_composite_action_inner(
                                &inner_manifest,
                                &inner_action_dir,
                                &inner_with,
                                workspace,
                                ctx,
                                depth + 1,
                            )
                            .await
                            .map(|_| "Success".to_string())
                        }
                        _ => super::action::run_action(uses, &inner_with, workspace, ctx)
                            .await
                            .map(|_| "Success".to_string()),
                    }
                } else {
                    super::action::run_action(uses, &inner_with, workspace, ctx)
                        .await
                        .map(|_| "Success".to_string())
                }
            } else {
                Ok("Skipped".to_string())
            };

            let step_outputs =
                crate::worker::file_commands::parse_kv_file(&file_commands.output_file)
                    .unwrap_or_default();
            crate::worker::file_commands::cleanup_file_commands(&file_commands);
            for (key, value) in saved_file_env {
                if let Some(value) = value {
                    ctx.env.insert(key, value);
                } else {
                    ctx.env.remove(&key);
                }
            }

            let conclusion = match &outcome {
                Ok(s) => s.clone(),
                Err(e) => {
                    warn!("Composite step '{step_name}' failed: {e:#}");
                    "Failure".to_string()
                }
            };

            nested_step_results.insert(
                step_id.clone(),
                crate::worker::contexts::StepResult {
                    outcome: conclusion.clone(),
                    conclusion,
                    outputs: step_outputs,
                },
            );

            if outcome.is_err() {
                // Non-continue-on-error composite step failure stops the composite
                break;
            }
        }

        // F024: Evaluate composite outputs after all steps complete
        if let Some(ref outputs) = manifest.outputs {
            let steps_ctx: serde_json::Value = nested_step_results
                .iter()
                .map(|(id, result)| {
                    let mut step_val = serde_json::Map::new();
                    step_val.insert("outcome".to_string(), serde_json::json!(result.outcome));
                    step_val.insert(
                        "conclusion".to_string(),
                        serde_json::json!(result.conclusion),
                    );
                    let outputs_map: serde_json::Map<String, serde_json::Value> = result
                        .outputs
                        .iter()
                        .map(|(k, v)| (k.clone(), serde_json::json!(v)))
                        .collect();
                    step_val.insert(
                        "outputs".to_string(),
                        serde_json::Value::Object(outputs_map),
                    );
                    (id.clone(), serde_json::Value::Object(step_val))
                })
                .collect();

            let mut eval_ctx = aksh_gha_expressions::Context::new();
            eval_ctx.insert("steps", steps_ctx);
            // Also include env from parent context
            let env_map: serde_json::Value = ctx
                .job
                .env
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::json!(v)))
                .collect();
            eval_ctx.insert("env", env_map);

            for (output_name, output_def) in outputs {
                if let Some(value_expr) = output_def.get("value").and_then(|v| v.as_str()) {
                    let trimmed = aksh_gha_expressions::trim_expression_markers(value_expr);
                    match aksh_gha_expressions::eval_expression(trimmed, &eval_ctx) {
                        Ok(val) => {
                            let val_str = match val {
                                serde_json::Value::String(s) => s,
                                other => serde_json::to_string(&other).unwrap_or_default(),
                            };
                            if let Some(output_path) = ctx.env.get("GITHUB_OUTPUT") {
                                use std::io::Write as _;
                                let mut file = std::fs::OpenOptions::new()
                                    .create(true)
                                    .append(true)
                                    .open(output_path)
                                    .with_context(|| {
                                        format!("opening GITHUB_OUTPUT {output_path}")
                                    })?;
                                writeln!(file, "{output_name}={val_str}")?;
                            } else {
                                let env_key = format!(
                                    "OUTPUT_{}",
                                    output_name.to_uppercase().replace('-', "_")
                                );
                                ctx.env.insert(env_key, val_str);
                            }
                        }
                        Err(e) => {
                            warn!(
                            "Composite output '{output_name}' expression '{value_expr}' failed: {e}"
                        );
                        }
                    }
                }
            }
        }

        Ok(())
    }) // end Box::pin
}
