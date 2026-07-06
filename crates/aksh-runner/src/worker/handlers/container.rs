//! Docker/container action handler.

use anyhow::{Context, Result};
use std::path::Path;
use tracing::info;

use super::factory::ActionManifest;
use crate::process;
use crate::worker::execution_context::StepContext;

/// Run a `docker://image` action.
pub async fn run_docker_action(
    uses: &str,
    with: &serde_json::Value,
    workspace: &str,
    ctx: &mut StepContext<'_>,
) -> Result<()> {
    ctx.translate_container_path = true;
    let image = uses
        .strip_prefix("docker://")
        .context("invalid docker action reference")?;

    info!("Running docker action: {image}");

    let inputs = evaluated_inputs(None, with, ctx)?;
    let env = container_action_env(ctx, &inputs, None, None)?;
    run_docker_image(image, workspace, ctx, env, None, Vec::new()).await
}

/// Run a docker action from a manifest (Dockerfile or image).
pub async fn run_docker_action_from_manifest(
    manifest: &ActionManifest,
    action_dir: &Path,
    with: &serde_json::Value,
    workspace: &str,
    ctx: &mut StepContext<'_>,
) -> Result<()> {
    ctx.translate_container_path = true;
    let image = manifest
        .runs_image
        .as_deref()
        .context("docker action missing runs.image")?;

    let image = if image.starts_with("Dockerfile") || image.starts_with("./") {
        // Build from Dockerfile
        let dockerfile = action_dir.join(image);
        let tag = format!("action-{}", uuid::Uuid::new_v4());

        info!("Building docker action from {}", dockerfile.display());

        let build_result = process::invoke(
            "docker",
            &[
                "build",
                "-t",
                &tag,
                "-f",
                &dockerfile.to_string_lossy(),
                &action_dir.to_string_lossy(),
            ],
            Path::new(workspace),
            &std::collections::HashMap::new(),
            None,
            None,
        )
        .await?;

        for line in &build_result.lines {
            ctx.log(line);
        }

        if build_result.exit_code != 0 {
            anyhow::bail!(
                "docker build failed with exit code {}",
                build_result.exit_code
            );
        }

        tag
    } else {
        image.to_string()
    };

    let inputs = evaluated_inputs(Some(manifest), with, ctx)?;
    let mut expr_ctx = ctx.job.build_expression_context();
    expr_ctx.insert("inputs", inputs_to_json(&inputs));
    let manifest_env = evaluate_manifest_env(manifest.runs_env.as_ref(), &expr_ctx)?;
    let env = container_action_env(ctx, &inputs, Some(manifest_env), Some(&expr_ctx))?;
    let entrypoint = lifecycle_entry(with)
        .or(manifest.runs_entrypoint.as_deref())
        .map(|entry| evaluate_template_value(entry, &expr_ctx))
        .transpose()?;
    let args = evaluate_manifest_args(manifest.runs_args.as_ref(), &expr_ctx)?;

    run_docker_image(&image, workspace, ctx, env, entrypoint, args).await
}

async fn run_docker_image(
    image: &str,
    workspace: &str,
    ctx: &mut StepContext<'_>,
    env: std::collections::HashMap<String, String>,
    entrypoint: Option<String>,
    args: Vec<String>,
) -> Result<()> {
    let docker_args =
        build_docker_run_args(workspace, ctx, &env, image, entrypoint.as_deref(), &args);
    let args_ref: Vec<&str> = docker_args.iter().map(|s| s.as_str()).collect();
    let result =
        process::invoke("docker", &args_ref, Path::new(workspace), &env, None, None).await?;

    for line in &result.lines {
        ctx.log(line);
    }

    if result.exit_code != 0 {
        anyhow::bail!("docker action exited with code {}", result.exit_code);
    }

    Ok(())
}

fn build_docker_run_args(
    workspace: &str,
    ctx: &StepContext<'_>,
    env: &std::collections::HashMap<String, String>,
    image: &str,
    entrypoint: Option<&str>,
    entrypoint_args: &[String],
) -> Vec<String> {
    let mut docker_args = vec![
        "run".to_string(),
        "--rm".to_string(),
        "-v".to_string(),
        format!("{workspace}:/github/workspace"),
        "--workdir".to_string(),
        "/github/workspace".to_string(),
    ];

    // Phase 2: Attach to job network if container state exists
    if let Some(state) = &ctx.job.container_state {
        docker_args.push("--network".to_string());
        docker_args.push(state.network.clone());
        docker_args.push("--label".to_string());
        docker_args.push(state.label.clone());
    }

    mount_file_command_dirs(&mut docker_args, env);

    if let Some(entrypoint) = entrypoint {
        docker_args.push("--entrypoint".to_string());
        docker_args.push(entrypoint.to_string());
    }

    push_inherited_env_args(&mut docker_args, env);

    // F049: Inject proxy env vars from host into container
    super::super::container_ops::inject_proxy_env_for_docker(&mut docker_args, env);

    docker_args.push(image.to_string());
    docker_args.extend(entrypoint_args.iter().cloned());
    docker_args
}

fn evaluated_inputs(
    manifest: Option<&ActionManifest>,
    with: &serde_json::Value,
    ctx: &StepContext<'_>,
) -> Result<std::collections::HashMap<String, String>> {
    let mut inputs = std::collections::HashMap::new();
    let expr_ctx = ctx.job.build_expression_context();
    if let Some(obj) = with.as_object() {
        for (key, value) in obj {
            if key.starts_with("__aksh_") {
                continue;
            }
            inputs.insert(
                key.clone(),
                evaluate_template_value(&value_to_string(value), &expr_ctx)?,
            );
        }
    }

    if let Some(manifest) = manifest {
        if let Some(manifest_inputs) = &manifest.inputs {
            let expr_ctx = ctx.job.build_expression_context();
            for (key, input_def) in manifest_inputs {
                if inputs.contains_key(key) {
                    continue;
                }
                if let Some(default) = input_def.get("default").and_then(|v| v.as_str()) {
                    let evaluated = crate::worker::template::evaluate_template(default, &expr_ctx)
                        .unwrap_or_else(|_| default.to_string());
                    inputs.insert(key.clone(), evaluated);
                }
            }
        }
    }

    Ok(inputs)
}

fn container_action_env(
    ctx: &StepContext<'_>,
    inputs: &std::collections::HashMap<String, String>,
    manifest_env: Option<std::collections::HashMap<String, String>>,
    _expr_ctx: Option<&aksh_gha_expressions::Context>,
) -> Result<std::collections::HashMap<String, String>> {
    let mut env = ctx.build_env();

    for (key, value) in inputs {
        let env_key = format!("INPUT_{}", key.to_uppercase().replace(' ', "_"));
        env.insert(env_key, value.clone());
    }

    if let Some(manifest_env) = manifest_env {
        for (key, value) in manifest_env {
            env.entry(key).or_insert(value);
        }
    }

    Ok(env)
}

fn evaluate_manifest_env(
    env: Option<&serde_json::Map<String, serde_json::Value>>,
    expr_ctx: &aksh_gha_expressions::Context,
) -> Result<std::collections::HashMap<String, String>> {
    let mut evaluated = std::collections::HashMap::new();
    if let Some(env) = env {
        for (key, value) in env {
            evaluated.insert(
                key.clone(),
                evaluate_template_value(&value_to_string(value), expr_ctx)?,
            );
        }
    }
    Ok(evaluated)
}

fn evaluate_manifest_args(
    args: Option<&Vec<String>>,
    expr_ctx: &aksh_gha_expressions::Context,
) -> Result<Vec<String>> {
    args.map(|args| {
        args.iter()
            .map(|arg| evaluate_template_value(arg, expr_ctx))
            .collect()
    })
    .unwrap_or_else(|| Ok(Vec::new()))
}

fn evaluate_template_value(
    value: &str,
    expr_ctx: &aksh_gha_expressions::Context,
) -> Result<String> {
    crate::worker::template::evaluate_template(value, expr_ctx)
        .with_context(|| format!("evaluating container manifest value {value:?}"))
}

fn inputs_to_json(inputs: &std::collections::HashMap<String, String>) -> serde_json::Value {
    serde_json::Value::Object(
        inputs
            .iter()
            .map(|(key, value)| (key.clone(), serde_json::json!(value)))
            .collect(),
    )
}

fn lifecycle_entry(with: &serde_json::Value) -> Option<&str> {
    with.get("__aksh_entry").and_then(|v| v.as_str())
}

fn value_to_string(value: &serde_json::Value) -> String {
    value
        .as_str()
        .map(String::from)
        .unwrap_or_else(|| value.to_string())
}

fn mount_file_command_dirs(
    docker_args: &mut Vec<String>,
    env: &std::collections::HashMap<String, String>,
) {
    let mut dirs = std::collections::BTreeSet::new();
    for key in [
        "GITHUB_ENV",
        "GITHUB_PATH",
        "GITHUB_OUTPUT",
        "GITHUB_STATE",
        "GITHUB_STEP_SUMMARY",
    ] {
        let Some(path) = env.get(key) else {
            continue;
        };
        let Some(parent) = Path::new(path).parent() else {
            continue;
        };
        dirs.insert(parent.to_string_lossy().to_string());
    }

    for dir in dirs {
        docker_args.push("-v".to_string());
        docker_args.push(format!("{dir}:{dir}"));
    }
}

fn push_inherited_env_args(
    docker_args: &mut Vec<String>,
    env: &std::collections::HashMap<String, String>,
) {
    for key in env.keys() {
        docker_args.push("-e".to_string());
        docker_args.push(key.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn inherited_env_args_do_not_include_secret_values() {
        let mut args = Vec::new();
        let env = HashMap::from([("MY_SECRET".to_string(), "s3cr3t".to_string())]);

        push_inherited_env_args(&mut args, &env);

        assert_eq!(args, vec!["-e".to_string(), "MY_SECRET".to_string()]);
        assert!(!args.iter().any(|arg| arg.contains("s3cr3t")));
    }

    #[test]
    fn docker_run_args_mount_file_command_directories() {
        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let ctx = test_step_context(&mut job);
        let env = HashMap::from([
            (
                "GITHUB_OUTPUT".to_string(),
                "/tmp/runner/_work/repo/_temp/_runner_file_commands/out".to_string(),
            ),
            (
                "GITHUB_STATE".to_string(),
                "/tmp/runner/_work/repo/_temp/_runner_file_commands/state".to_string(),
            ),
        ]);

        let args = build_docker_run_args("/tmp/work", &ctx, &env, "alpine:3.20", None, &[]);

        assert_eq!(
            args.iter()
                .filter(|arg| *arg == "/tmp/runner/_work/repo/_temp/_runner_file_commands:/tmp/runner/_work/repo/_temp/_runner_file_commands")
                .count(),
            1
        );
    }

    fn test_manifest() -> ActionManifest {
        ActionManifest {
            name: "docker".into(),
            description: String::new(),
            runs_using: "docker".into(),
            runs_main: None,
            runs_pre: None,
            runs_pre_if: None,
            runs_post: None,
            runs_post_if: None,
            runs_steps: None,
            runs_image: Some("alpine:3.20".into()),
            runs_entrypoint: Some("${{ inputs.entrypoint }}".into()),
            runs_args: Some(vec!["${{ inputs.message }}".into(), "literal".into()]),
            runs_env: Some(serde_json::Map::from_iter([(
                "MANIFEST_ENV".to_string(),
                serde_json::json!("hello-${{ inputs.message }}"),
            )])),
            inputs: Some(serde_json::Map::from_iter([
                (
                    "entrypoint".to_string(),
                    serde_json::json!({"default": "default-entry"}),
                ),
                (
                    "message".to_string(),
                    serde_json::json!({"default": "world"}),
                ),
            ])),
            outputs: None,
        }
    }

    fn test_step_context(job: &mut crate::worker::contexts::JobContext) -> StepContext<'_> {
        StepContext::new(job, "container".into(), "Container".into())
    }

    #[test]
    fn manifest_env_entrypoint_and_args_evaluate_against_inputs() {
        let manifest = test_manifest();
        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let ctx = test_step_context(&mut job);
        let inputs = evaluated_inputs(
            Some(&manifest),
            &serde_json::json!({"message": "from-input", "entrypoint": "run.sh"}),
            &ctx,
        )
        .unwrap();
        let mut expr_ctx = ctx.job.build_expression_context();
        expr_ctx.insert("inputs", inputs_to_json(&inputs));

        assert_eq!(
            evaluate_template_value(manifest.runs_entrypoint.as_deref().unwrap(), &expr_ctx)
                .unwrap(),
            "run.sh"
        );
        assert_eq!(
            evaluate_manifest_args(manifest.runs_args.as_ref(), &expr_ctx).unwrap(),
            vec!["from-input".to_string(), "literal".to_string()]
        );
        assert_eq!(
            evaluate_manifest_env(manifest.runs_env.as_ref(), &expr_ctx)
                .unwrap()
                .get("MANIFEST_ENV")
                .map(String::as_str),
            Some("hello-from-input")
        );
    }

    #[test]
    fn docker_run_args_apply_entrypoint_args_and_hide_env_values() {
        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let ctx = test_step_context(&mut job);
        let env = HashMap::from([("MY_SECRET".to_string(), "s3cr3t".to_string())]);
        let entrypoint_args = vec!["arg1".to_string(), "arg2".to_string()];

        let args = build_docker_run_args(
            "/tmp/work",
            &ctx,
            &env,
            "alpine:3.20",
            Some("run.sh"),
            &entrypoint_args,
        );

        assert!(args
            .windows(2)
            .any(|pair| pair == ["--entrypoint", "run.sh"]));
        assert!(args.windows(2).any(|pair| pair == ["-e", "MY_SECRET"]));
        assert!(!args.iter().any(|arg| arg.contains("s3cr3t")));
        let image_index = args.iter().position(|arg| arg == "alpine:3.20").unwrap();
        assert_eq!(&args[image_index + 1..], ["arg1", "arg2"]);
    }

    // --- P0 container action gap coverage ---

    #[test]
    fn docker_image_reference_builds_run_args() {
        // Simulates `docker://alpine:3.20` action — verifies the run args
        // include the image, workspace mount, and workdir.
        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let ctx = test_step_context(&mut job);
        let env = HashMap::new();
        let args = build_docker_run_args("/tmp/work", &ctx, &env, "alpine:3.20", None, &[]);

        assert!(args.contains(&"alpine:3.20".to_string()));
        assert!(args.contains(&"--rm".to_string()));
        assert!(args.contains(&"/tmp/work:/github/workspace".to_string()));
        assert!(args.contains(&"/github/workspace".to_string()));
    }

    #[test]
    fn manifest_without_entrypoint_or_args() {
        // DockerHub image with no entrypoint/args — uses image's default CMD
        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let ctx = test_step_context(&mut job);
        let env = HashMap::new();
        let args = build_docker_run_args("/tmp/work", &ctx, &env, "python:3.12", None, &[]);

        // No --entrypoint flag
        assert!(!args.windows(2).any(|pair| pair[0] == "--entrypoint"));
        // Image is last arg (no entrypoint args after it)
        assert_eq!(args.last().map(String::as_str), Some("python:3.12"));
    }

    #[test]
    fn evaluated_inputs_applies_defaults_from_manifest() {
        let manifest = test_manifest();
        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let ctx = test_step_context(&mut job);

        // Only provide message, entrypoint should get default
        let inputs =
            evaluated_inputs(Some(&manifest), &serde_json::json!({"message": "hi"}), &ctx).unwrap();
        assert_eq!(inputs.get("message").map(String::as_str), Some("hi"));
        assert_eq!(
            inputs.get("entrypoint").map(String::as_str),
            Some("default-entry")
        );
    }

    #[test]
    fn evaluated_inputs_skips_aksh_internal_keys() {
        let mut job = crate::worker::contexts::JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let ctx = test_step_context(&mut job);

        let inputs = evaluated_inputs(
            None,
            &serde_json::json!({"__aksh_entry": "pre.js", "real": "val"}),
            &ctx,
        )
        .unwrap();
        assert!(!inputs.contains_key("__aksh_entry"));
        assert_eq!(inputs.get("real").map(String::as_str), Some("val"));
    }
}
