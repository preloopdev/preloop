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

    let env = ctx.build_env();
    let mut docker_args = vec![
        "run".to_string(),
        "--rm".to_string(),
        "-v".to_string(),
        format!("{workspace}:/github/workspace"),
        "--workdir".to_string(),
        "/github/workspace".to_string(),
    ];

    // Add environment variables
    for (k, v) in &env {
        docker_args.push("-e".to_string());
        docker_args.push(format!("{k}={v}"));
    }

    // Add action inputs as INPUT_* env vars
    if let Some(inputs) = with.as_object() {
        for (key, value) in inputs {
            let env_key = format!("INPUT_{}", key.to_uppercase().replace(' ', "_"));
            let val = value.as_str().unwrap_or(&value.to_string()).to_string();
            docker_args.push("-e".to_string());
            docker_args.push(format!("{env_key}={val}"));
        }
    }

    docker_args.push(image.to_string());

    let args_ref: Vec<&str> = docker_args.iter().map(|s| s.as_str()).collect();
    let result = process::invoke(
        "docker",
        &args_ref,
        Path::new(workspace),
        &std::collections::HashMap::new(),
        None,
        None,
    )
    .await?;

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
