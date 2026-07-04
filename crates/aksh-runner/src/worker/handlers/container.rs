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
    let image = uses
        .strip_prefix("docker://")
        .context("invalid docker action reference")?;

    info!("Running docker action: {image}");

    let mut env = ctx.build_env();
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

    // Add action inputs as INPUT_* env vars to the docker client process
    // environment. Docker receives only `-e KEY`, matching the official runner
    // and keeping secret values out of command-line args.
    if let Some(inputs) = with.as_object() {
        for (key, value) in inputs {
            let env_key = format!("INPUT_{}", key.to_uppercase().replace(' ', "_"));
            let val = value.as_str().unwrap_or(&value.to_string()).to_string();
            env.insert(env_key, val);
        }
    }

    push_inherited_env_args(&mut docker_args, &env);

    docker_args.push(image.to_string());

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

/// Run a docker action from a manifest (Dockerfile or image).
pub async fn run_docker_action_from_manifest(
    manifest: &ActionManifest,
    action_dir: &Path,
    with: &serde_json::Value,
    workspace: &str,
    ctx: &mut StepContext<'_>,
) -> Result<()> {
    let image = manifest
        .runs_image
        .as_deref()
        .context("docker action missing runs.image")?;

    if image.starts_with("Dockerfile") || image.starts_with("./") {
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

        if build_result.exit_code != 0 {
            anyhow::bail!(
                "docker build failed with exit code {}",
                build_result.exit_code
            );
        }

        run_docker_action(&format!("docker://{tag}"), with, workspace, ctx).await
    } else {
        // Direct image reference
        run_docker_action(&format!("docker://{image}"), with, workspace, ctx).await
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
}
