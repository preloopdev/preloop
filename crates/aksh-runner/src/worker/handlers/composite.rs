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
    cancel_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    run_composite_action_inner(manifest, action_dir, with, workspace, ctx, 0, cancel_rx).await
}

fn run_composite_action_inner<'a>(
    manifest: &'a ActionManifest,
    action_dir: &'a Path,
    with: &'a serde_json::Value,
    workspace: &'a str,
    ctx: &'a mut StepContext<'_>,
    depth: u32,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
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

        let saved_env = ctx.env.clone();
        let result = async {
            let previous_action_status = ctx.job.github_context_value("action_status");

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
            let action_status = if *cancel_rx.borrow() {
                "cancelled"
            } else {
                "success"
            };
            ctx.job
                .set_github_context_value("action_status", Some(serde_json::json!(action_status)));
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

            // Evaluate `if` condition — skip step if condition is false
            let step_if = step.get("if").and_then(|v| v.as_str());
            if let Some(condition) = step_if {
                let mut if_ctx = ctx.job.build_expression_context();
                // Include composite inputs and nested step results in if-condition context
                let mut inputs_map = serde_json::Map::new();
                for (k, v) in &input_env {
                    if let Some(name) = k.strip_prefix("INPUT_") {
                        inputs_map.insert(name.to_lowercase(), serde_json::json!(v));
                    }
                }
                if_ctx.insert("inputs", serde_json::Value::Object(inputs_map));
                let mut nested_steps_map = serde_json::Map::new();
                for (sid, sresult) in &nested_step_results {
                    let mut step_val = serde_json::Map::new();
                    step_val.insert("outcome".into(), serde_json::json!(sresult.outcome));
                    step_val.insert("conclusion".into(), serde_json::json!(sresult.conclusion));
                    let mut out_map = serde_json::Map::new();
                    for (ok, ov) in &sresult.outputs {
                        out_map.insert(ok.clone(), serde_json::json!(ov));
                    }
                    step_val.insert("outputs".into(), serde_json::Value::Object(out_map));
                    nested_steps_map.insert(sid.clone(), serde_json::Value::Object(step_val));
                }
                if_ctx.insert("steps", serde_json::Value::Object(nested_steps_map));

                match crate::worker::template::evaluate_condition(condition, &if_ctx) {
                    Ok(true) => {} // condition met, continue
                    Ok(false) => {
                        info!("  Skipping composite step '{step_name}' (if: {condition} → false)");
                        nested_step_results.insert(
                            step_id.clone(),
                            crate::worker::contexts::StepResult {
                                outcome: "Skipped".to_string(),
                                conclusion: "Skipped".to_string(),
                                outputs: Default::default(),
                            },
                        );
                        continue;
                    }
                    Err(e) => {
                        warn!("  Failed to evaluate if condition for '{step_name}': {e:#}. Running step.");
                    }
                }
            }

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
                // Build expression context with inputs + nested step results
                let mut expr_ctx = ctx.job.build_expression_context();
                let mut inputs_map = serde_json::Map::new();
                for (k, v) in &input_env {
                    if let Some(name) = k.strip_prefix("INPUT_") {
                        inputs_map.insert(name.to_lowercase(), serde_json::json!(v));
                    }
                }
                expr_ctx.insert("inputs", serde_json::Value::Object(inputs_map));

                let mut nested_steps_map = serde_json::Map::new();
                for (sid, sresult) in &nested_step_results {
                    let mut step_val = serde_json::Map::new();
                    step_val.insert("outcome".into(), serde_json::json!(sresult.outcome));
                    step_val.insert("conclusion".into(), serde_json::json!(sresult.conclusion));
                    let mut out_map = serde_json::Map::new();
                    for (k, v) in &sresult.outputs {
                        out_map.insert(k.clone(), serde_json::json!(v));
                    }
                    step_val.insert("outputs".into(), serde_json::Value::Object(out_map));
                    nested_steps_map.insert(sid.clone(), serde_json::Value::Object(step_val));
                }
                expr_ctx.insert("steps", serde_json::Value::Object(nested_steps_map));

                // Evaluate ${{ }} in step-level env: block and inject into ctx.env
                let mut step_env_overrides = Vec::new();
                if let Some(env_obj) = step.get("env").and_then(|v| v.as_object()) {
                    for (ek, ev) in env_obj {
                        let raw_val = ev.as_str().unwrap_or(&ev.to_string()).to_string();
                        let evaluated_val =
                            crate::worker::template::evaluate_template(&raw_val, &expr_ctx)
                                .unwrap_or(raw_val);
                        let prev = ctx.env.insert(ek.clone(), evaluated_val);
                        step_env_overrides.push((ek.clone(), prev));
                    }
                }

                let evaluated = crate::worker::template::evaluate_template(script, &expr_ctx)
                    .unwrap_or_else(|_| script.to_string());
                let result = super::script::run_script(
                    &evaluated,
                    step_shell,
                    workspace,
                    ctx,
                    Some(cancel_rx.clone()),
                )
                .await;

                // Restore env overrides (step-level env is scoped to the step)
                for (ek, prev) in step_env_overrides {
                    if let Some(prev_val) = prev {
                        ctx.env.insert(ek, prev_val);
                    } else {
                        ctx.env.remove(&ek);
                    }
                }

                result.map(|_| "Success".to_string())
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
                                cancel_rx.clone(),
                            )
                            .await
                            .map(|_| "Success".to_string())
                        }
                        _ => super::action::run_action(
                            uses,
                            &inner_with,
                            workspace,
                            ctx,
                            cancel_rx.clone(),
                        )
                        .await
                        .map(|_| "Success".to_string()),
                    }
                } else {
                    super::action::run_action(uses, &inner_with, workspace, ctx, cancel_rx.clone())
                        .await
                        .map(|_| "Success".to_string())
                }
            } else {
                Ok("Skipped".to_string())
            };

            // Apply GITHUB_ENV and GITHUB_PATH from this composite step
            // so subsequent steps see the env changes (e.g. dtolnay/rust-toolchain
            // sets CARGO_HOME via GITHUB_ENV and adds to PATH via GITHUB_PATH)
            crate::worker::file_commands::apply_file_commands_to_job(&file_commands, ctx.job);

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

            let continue_on_error = step
                .get("continue-on-error")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let conclusion = match &outcome {
                Ok(s) => s.clone(),
                Err(e) => {
                    if continue_on_error {
                        info!("Composite step '{step_name}' failed but continue-on-error is set: {e:#}");
                        "Success".to_string()
                    } else {
                        warn!("Composite step '{step_name}' failed: {e:#}");
                        "Failure".to_string()
                    }
                }
            };

            nested_step_results.insert(
                step_id.clone(),
                crate::worker::contexts::StepResult {
                    outcome: conclusion.clone(),
                    conclusion: conclusion.clone(),
                    outputs: step_outputs,
                },
            );

            if outcome.is_err() && !continue_on_error {
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
                                serde_json::Value::Null => String::new(),
                                serde_json::Value::Bool(b) => b.to_string(),
                                serde_json::Value::Number(n) => n.to_string(),
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
            ctx.job
                .set_github_context_value("action_status", previous_action_status);
            Ok(())
        }
        .await;

        ctx.env = saved_env;
        result
    }) // end Box::pin
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::contexts::JobContext;
    use crate::worker::execution_context::StepContext;

    fn composite_manifest(steps: Vec<serde_json::Value>) -> ActionManifest {
        ActionManifest {
            name: "composite".into(),
            description: String::new(),
            runs_using: "composite".into(),
            runs_main: None,
            runs_pre: None,
            runs_pre_if: None,
            runs_post: None,
            runs_post_if: None,
            runs_steps: Some(steps),
            runs_image: None,
            runs_entrypoint: None,
            runs_args: None,
            runs_env: None,
            inputs: None,
            outputs: None,
        }
    }

    #[tokio::test]
    async fn composite_steps_receive_action_status_context() {
        let workspace = tempfile::TempDir::new().unwrap();
        let manifest = composite_manifest(vec![serde_json::json!({
            "run": "echo status=${{ github.action_status }}",
            "shell": "bash"
        })]);
        let mut job = JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({"github": {"workspace": workspace.path()}}),
        );
        job.workspace = Some(workspace.path().to_string_lossy().to_string());
        let mut ctx = StepContext::new(&mut job, "composite".into(), "Composite".into());
        let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        run_composite_action(
            &manifest,
            workspace.path(),
            &serde_json::json!({}),
            workspace.path().to_str().unwrap(),
            &mut ctx,
            cancel_rx,
        )
        .await
        .unwrap();

        assert!(ctx.log_content().contains("status=success"));
        assert_eq!(ctx.job.github_context_value("action_status"), None);
        assert!(!ctx.job.env.contains_key("GITHUB_ACTION_STATUS"));
    }

    #[tokio::test]
    async fn composite_maps_with_inputs_and_manifest_defaults_to_input_env() {
        let workspace = tempfile::TempDir::new().unwrap();
        let mut manifest = composite_manifest(vec![serde_json::json!({
            "run": "echo first=$INPUT_FIRST second=$INPUT_SECOND",
            "shell": "bash"
        })]);
        manifest.inputs = Some(serde_json::Map::from_iter([
            (
                "first".to_string(),
                serde_json::json!({"default": "default-first"}),
            ),
            (
                "second".to_string(),
                serde_json::json!({"default": "default-second"}),
            ),
        ]));
        let mut job = JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({"github": {"workspace": workspace.path()}}),
        );
        job.workspace = Some(workspace.path().to_string_lossy().to_string());
        let mut ctx = StepContext::new(&mut job, "composite".into(), "Composite".into());
        let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        run_composite_action(
            &manifest,
            workspace.path(),
            &serde_json::json!({"first": "provided"}),
            workspace.path().to_str().unwrap(),
            &mut ctx,
            cancel_rx,
        )
        .await
        .unwrap();

        assert!(ctx
            .log_content()
            .contains("first=provided second=default-second"));
    }

    #[tokio::test]
    async fn composite_evaluates_outputs_from_nested_step_outputs() {
        let workspace = tempfile::TempDir::new().unwrap();
        let parent_output = workspace.path().join("parent_output");
        let mut manifest = composite_manifest(vec![serde_json::json!({
            "id": "produce",
            "run": "echo value=from-nested >> \"$GITHUB_OUTPUT\"",
            "shell": "bash"
        })]);
        manifest.outputs = Some(serde_json::Map::from_iter([(
            "result".to_string(),
            serde_json::json!({"value": "${{ steps.produce.outputs.value }}"}),
        )]));
        let mut job = JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({"github": {"workspace": workspace.path()}}),
        );
        job.workspace = Some(workspace.path().to_string_lossy().to_string());
        let mut ctx = StepContext::new(&mut job, "composite".into(), "Composite".into());
        ctx.env.insert(
            "GITHUB_OUTPUT".to_string(),
            parent_output.to_string_lossy().to_string(),
        );
        let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        run_composite_action(
            &manifest,
            workspace.path(),
            &serde_json::json!({}),
            workspace.path().to_str().unwrap(),
            &mut ctx,
            cancel_rx,
        )
        .await
        .unwrap();

        let output = std::fs::read_to_string(parent_output).unwrap();
        assert!(output.contains("result=from-nested"));
    }

    #[tokio::test]
    async fn composite_stops_after_nested_step_failure() {
        let workspace = tempfile::TempDir::new().unwrap();
        let manifest = composite_manifest(vec![
            serde_json::json!({
                "id": "fail",
                "run": "exit 1",
                "shell": "bash"
            }),
            serde_json::json!({
                "id": "after",
                "run": "echo should-not-run",
                "shell": "bash"
            }),
        ]);
        let mut job = JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({"github": {"workspace": workspace.path()}}),
        );
        job.workspace = Some(workspace.path().to_string_lossy().to_string());
        let mut ctx = StepContext::new(&mut job, "composite".into(), "Composite".into());
        let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        run_composite_action(
            &manifest,
            workspace.path(),
            &serde_json::json!({}),
            workspace.path().to_str().unwrap(),
            &mut ctx,
            cancel_rx,
        )
        .await
        .unwrap();

        assert!(!ctx.log_content().contains("should-not-run"));
    }

    #[tokio::test]
    async fn composite_enforces_nesting_depth_limit() {
        let workspace = tempfile::TempDir::new().unwrap();
        let manifest = composite_manifest(vec![]);
        let mut job = JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({"github": {"workspace": workspace.path()}}),
        );
        let mut ctx = StepContext::new(&mut job, "composite".into(), "Composite".into());
        let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        let err = run_composite_action_inner(
            &manifest,
            workspace.path(),
            &serde_json::json!({}),
            workspace.path().to_str().unwrap(),
            &mut ctx,
            MAX_COMPOSITE_DEPTH,
            cancel_rx,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("nesting depth exceeded"));
    }

    // --- P0 composite gap coverage ---

    #[tokio::test]
    async fn composite_nested_uses_dispatches_inner_action() {
        // Create a composite whose step has `uses: ./inner` pointing to another
        // composite action.
        let workspace = tempfile::TempDir::new().unwrap();
        let inner_dir = workspace.path().join("inner");
        std::fs::create_dir_all(&inner_dir).unwrap();
        std::fs::write(
            inner_dir.join("action.yml"),
            r#"
name: Inner
runs:
  using: composite
  steps:
    - run: echo inner-executed
      shell: bash
"#,
        )
        .unwrap();

        let manifest = composite_manifest(vec![serde_json::json!({
            "uses": "./inner"
        })]);
        let mut job = JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({"github": {"workspace": workspace.path()}}),
        );
        job.workspace = Some(workspace.path().to_string_lossy().to_string());
        let mut ctx = StepContext::new(&mut job, "composite".into(), "Composite".into());
        let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        run_composite_action(
            &manifest,
            workspace.path(),
            &serde_json::json!({}),
            workspace.path().to_str().unwrap(),
            &mut ctx,
            cancel_rx,
        )
        .await
        .unwrap();

        assert!(ctx.log_content().contains("inner-executed"));
    }

    #[tokio::test]
    async fn composite_output_captures_from_script_step() {
        // Composite action with both inputs and outputs exercised together.
        let workspace = tempfile::TempDir::new().unwrap();
        let parent_output = workspace.path().join("parent_output");
        let mut manifest = composite_manifest(vec![serde_json::json!({
            "id": "greet",
            "run": "echo greeting=hello-$INPUT_NAME >> \"$GITHUB_OUTPUT\"",
            "shell": "bash"
        })]);
        manifest.inputs = Some(serde_json::Map::from_iter([(
            "name".to_string(),
            serde_json::json!({"default": "world"}),
        )]));
        manifest.outputs = Some(serde_json::Map::from_iter([(
            "greeting".to_string(),
            serde_json::json!({"value": "${{ steps.greet.outputs.greeting }}"}),
        )]));

        let mut job = JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({"github": {"workspace": workspace.path()}}),
        );
        job.workspace = Some(workspace.path().to_string_lossy().to_string());
        let mut ctx = StepContext::new(&mut job, "composite".into(), "Composite".into());
        ctx.env.insert(
            "GITHUB_OUTPUT".to_string(),
            parent_output.to_string_lossy().to_string(),
        );
        let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        run_composite_action(
            &manifest,
            workspace.path(),
            &serde_json::json!({"name": "rust"}),
            workspace.path().to_str().unwrap(),
            &mut ctx,
            cancel_rx,
        )
        .await
        .unwrap();

        let output = std::fs::read_to_string(parent_output).unwrap();
        assert!(output.contains("greeting=hello-rust"));
    }

    #[tokio::test]
    async fn composite_isolates_env_and_inputs() {
        let workspace = tempfile::TempDir::new().unwrap();
        let manifest = composite_manifest(vec![serde_json::json!({
            "run": "echo inner-run",
            "shell": "bash"
        })]);
        let mut job = JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({"github": {"workspace": workspace.path()}}),
        );
        job.workspace = Some(workspace.path().to_string_lossy().to_string());
        let mut ctx = StepContext::new(&mut job, "composite".into(), "Composite".into());
        ctx.env
            .insert("PRE_EXISTING".to_string(), "original".to_string());
        let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        run_composite_action(
            &manifest,
            workspace.path(),
            &serde_json::json!({"input_one": "val"}),
            workspace.path().to_str().unwrap(),
            &mut ctx,
            cancel_rx,
        )
        .await
        .unwrap();

        // Verify that pre-existing variables are preserved
        assert_eq!(
            ctx.env.get("PRE_EXISTING").map(String::as_str),
            Some("original")
        );
        // Verify that composite action inputs do not leak to the outer context
        assert!(!ctx.env.contains_key("INPUT_INPUT_ONE"));
        assert!(!ctx.env.contains_key("GITHUB_ACTION_PATH"));
    }

    #[tokio::test]
    async fn composite_handles_missing_outputs_gracefully() {
        let workspace = tempfile::TempDir::new().unwrap();
        let parent_output = workspace.path().join("parent_output");

        let mut manifest = composite_manifest(vec![serde_json::json!({
            "run": "echo first",
            "shell": "bash"
        })]);
        manifest.outputs = Some(serde_json::Map::from_iter([(
            "greeting".to_string(),
            serde_json::json!({
                "value": "${{ steps.nonexistent.outputs.val }}"
            }),
        )]));

        let mut job = JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({"github": {"workspace": workspace.path()}}),
        );
        job.workspace = Some(workspace.path().to_string_lossy().to_string());
        let mut ctx = StepContext::new(&mut job, "composite".into(), "Composite".into());
        ctx.env.insert(
            "GITHUB_OUTPUT".to_string(),
            parent_output.to_string_lossy().to_string(),
        );
        let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

        run_composite_action(
            &manifest,
            workspace.path(),
            &serde_json::json!({}),
            workspace.path().to_str().unwrap(),
            &mut ctx,
            cancel_rx,
        )
        .await
        .unwrap();

        let output = std::fs::read_to_string(parent_output).unwrap();
        assert!(output.contains("greeting=\n") || output.contains("greeting=\r\n"));
    }
}
