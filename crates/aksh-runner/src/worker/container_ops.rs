//! Container operations — job/service containers via docker CLI.

use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use tracing::info;

use crate::process;

/// Check if Docker is available.
pub async fn check_docker() -> Result<bool> {
    let result = process::invoke(
        "docker",
        &["version", "--format", "{{.Server.Version}}"],
        Path::new("."),
        &HashMap::new(),
        None,
        None,
    )
    .await;

    match result {
        Ok(output) => Ok(output.exit_code == 0),
        Err(_) => Ok(false),
    }
}

/// Create a Docker network for the job.
pub async fn create_network(network_name: &str) -> Result<()> {
    let result = process::invoke(
        "docker",
        &["network", "create", network_name],
        Path::new("."),
        &HashMap::new(),
        None,
        None,
    )
    .await?;

    if result.exit_code != 0 {
        anyhow::bail!("Failed to create Docker network: {network_name}");
    }
    Ok(())
}

/// Remove a Docker network.
pub async fn remove_network(network_name: &str) -> Result<()> {
    let _ = process::invoke(
        "docker",
        &["network", "rm", network_name],
        Path::new("."),
        &HashMap::new(),
        None,
        None,
    )
    .await;
    Ok(())
}

/// Pull a Docker image.
pub async fn pull_image(image: &str) -> Result<()> {
    info!("Pulling Docker image: {image}");
    let result = process::invoke(
        "docker",
        &["pull", image],
        Path::new("."),
        &HashMap::new(),
        None,
        None,
    )
    .await?;

    if result.exit_code != 0 {
        anyhow::bail!("Failed to pull image: {image}");
    }
    Ok(())
}

/// Start a container and return its ID.
pub async fn start_container(
    image: &str,
    name: &str,
    network: &str,
    volumes: &[(&str, &str)],
    env: &HashMap<String, String>,
    workdir: &str,
) -> Result<String> {
    let mut args = vec![
        "create".to_string(),
        "--name".to_string(),
        name.to_string(),
        "--network".to_string(),
        network.to_string(),
        "--workdir".to_string(),
        workdir.to_string(),
    ];

    for (host, container) in volumes {
        args.push("-v".to_string());
        args.push(format!("{host}:{container}"));
    }

    for (k, v) in env {
        args.push("-e".to_string());
        args.push(format!("{k}={v}"));
    }

    args.push(image.to_string());

    let args_ref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    let result = process::invoke(
        "docker",
        &args_ref,
        Path::new("."),
        &HashMap::new(),
        None,
        None,
    )
    .await?;

    if result.exit_code != 0 {
        anyhow::bail!("Failed to create container from {image}");
    }

    let container_id = result
        .lines
        .first()
        .cloned()
        .unwrap_or_default()
        .trim()
        .to_string();

    // Start the container
    process::invoke(
        "docker",
        &["start", &container_id],
        Path::new("."),
        &HashMap::new(),
        None,
        None,
    )
    .await?;

    Ok(container_id)
}

/// Remove a container forcefully.
pub async fn remove_container(container_id: &str) -> Result<()> {
    let _ = process::invoke(
        "docker",
        &["rm", "-f", container_id],
        Path::new("."),
        &HashMap::new(),
        None,
        None,
    )
    .await;
    Ok(())
}

/// Translate a host path to container path.
pub fn translate_to_container_path(host_path: &str, host_work: &str) -> String {
    if let Some(relative) = host_path.strip_prefix(host_work) {
        format!("/__w{relative}")
    } else {
        host_path.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_translation() {
        assert_eq!(
            translate_to_container_path("/home/runner/_work/repo/repo", "/home/runner/_work"),
            "/__w/repo/repo"
        );
        assert_eq!(
            translate_to_container_path("/other/path", "/home/runner/_work"),
            "/other/path"
        );
    }
}
