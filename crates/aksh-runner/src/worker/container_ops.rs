//! Container operations — job/service containers via Docker CLI.
//!
//! Matches official runner v2.335.1 Docker command sequences observed in golden traces:
//! - Instance labels for cleanup (`--label <6-hex>`)
//! - Container naming: `<32-hex-uuid>_<sanitized-image>_<6-hex>`
//! - Network naming: `github_network_<uuid-no-dashes>`
//! - Docker socket auto-mount into job containers
//! - Health check polling with 2s/3s/interval backoff
//! - Cleanup order: job container → service logs → service containers → network

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::Path;
use tracing::{debug, info, warn};

use crate::process;

/// Parsed container spec from the job message.
#[derive(Debug, Clone)]
pub struct ContainerSpec {
    pub image: String,
    pub env: HashMap<String, String>,
    pub ports: Vec<String>,
    pub volumes: Vec<String>,
    pub options: String,
}

/// Parsed service container spec.
#[derive(Debug, Clone)]
pub struct ServiceSpec {
    pub alias: String,
    pub image: String,
    pub env: HashMap<String, String>,
    pub ports: Vec<String>,
    pub volumes: Vec<String>,
    pub options: String,
}

/// Runtime state for a running container job.
#[derive(Debug, Clone)]
pub struct ContainerState {
    /// 6-hex instance label for all containers/networks in this job.
    pub label: String,
    /// Docker network name: `github_network_<uuid-no-dashes>`.
    pub network: String,
    /// Job container ID (full 64-char), if `container:` is set.
    pub job_container_id: Option<String>,
    /// Job container name.
    pub job_container_name: Option<String>,
    /// Service container IDs keyed by alias.
    pub service_containers: Vec<(String, String, String)>, // (alias, container_id, container_name)
}

// ── TemplateToken decoding ──────────────────────────────────────────

/// Decode a GitHub TemplateToken JSON value into plain JSON.
///
/// GitHub's control plane sends container/service specs as TemplateTokens:
/// - type 0: string literal → `"lit"` field
/// - type 1: sequence → `"seq"` array of tokens
/// - type 2: mapping → `"map"` array of `{"Key": token, "Value": token}`
///
/// If the value is already plain JSON (e.g. from aksh-native payloads), return as-is.
fn decode_template_token(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) if map.contains_key("type") => {
            let tt = map.get("type").and_then(|v| v.as_u64()).unwrap_or(99);
            match tt {
                0 => {
                    // String literal
                    let lit = map.get("lit").and_then(|v| v.as_str()).unwrap_or("");
                    serde_json::Value::String(lit.to_string())
                }
                1 => {
                    // Sequence
                    let seq = map
                        .get("seq")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    serde_json::Value::Array(seq.iter().map(decode_template_token).collect())
                }
                2 => {
                    // Mapping
                    let entries = map
                        .get("map")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    let mut result = serde_json::Map::new();
                    for entry in &entries {
                        let key = entry
                            .get("Key")
                            .or_else(|| entry.get("key"))
                            .map(decode_template_token);
                        let val = entry
                            .get("Value")
                            .or_else(|| entry.get("value"))
                            .map(decode_template_token);
                        if let (Some(serde_json::Value::String(k)), Some(v)) = (key, val) {
                            result.insert(k, v);
                        }
                    }
                    serde_json::Value::Object(result)
                }
                _ => value.clone(),
            }
        }
        // Already plain JSON — pass through
        _ => value.clone(),
    }
}

// ── Parsing ──────────────────────────────────────────────────────────

/// Parse a `jobContainer` value (string, mapping, or TemplateToken) into a ContainerSpec.
pub fn parse_container_spec(value: &serde_json::Value) -> Option<ContainerSpec> {
    // Decode TemplateToken if present
    let decoded = decode_template_token(value);
    parse_container_spec_plain(&decoded)
}

/// Parse a plain (non-TemplateToken) container spec value.
fn parse_container_spec_plain(value: &serde_json::Value) -> Option<ContainerSpec> {
    match value {
        serde_json::Value::String(image) if !image.is_empty() => Some(ContainerSpec {
            image: image.clone(),
            env: HashMap::new(),
            ports: Vec::new(),
            volumes: Vec::new(),
            options: String::new(),
        }),
        serde_json::Value::Object(map) => {
            let image = map
                .get("image")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if image.is_empty() {
                return None;
            }
            Some(ContainerSpec {
                image,
                env: parse_env_map(map.get("env")),
                ports: parse_string_array(map.get("ports")),
                volumes: parse_string_array(map.get("volumes")),
                options: map
                    .get("options")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
            })
        }
        _ => None,
    }
}

/// Parse `jobServiceContainers` value into a list of ServiceSpecs.
///
/// Handles both TemplateToken format (from GitHub) and plain JSON (from aksh).
pub fn parse_service_specs(value: &serde_json::Value) -> Vec<ServiceSpec> {
    // Decode TemplateToken if present
    let decoded = decode_template_token(value);

    let mut services = Vec::new();
    if let Some(map) = decoded.as_object() {
        for (alias, spec) in map {
            if let Some(container) = parse_container_spec_plain(spec) {
                services.push(ServiceSpec {
                    alias: alias.clone(),
                    image: container.image,
                    env: container.env,
                    ports: container.ports,
                    volumes: container.volumes,
                    options: container.options,
                });
            }
        }
    }
    services
}

fn parse_env_map(value: Option<&serde_json::Value>) -> HashMap<String, String> {
    let mut env = HashMap::new();
    if let Some(serde_json::Value::Object(map)) = value {
        for (k, v) in map {
            env.insert(k.clone(), v.as_str().unwrap_or("").to_string());
        }
    }
    env
}

fn parse_string_array(value: Option<&serde_json::Value>) -> Vec<String> {
    match value {
        Some(serde_json::Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect(),
        _ => Vec::new(),
    }
}

// ── Docker CLI operations ────────────────────────────────────────────

/// Check if Docker is available and return the server API version.
pub async fn check_docker(log: &mut Vec<String>) -> Result<bool> {
    log.push("##[group]Checking docker version".to_string());

    let server = docker_cmd(&["version", "--format", "{{.Server.APIVersion}}"], log).await;
    let client = docker_cmd(&["version", "--format", "{{.Client.APIVersion}}"], log).await;

    log.push("##[endgroup]".to_string());

    Ok(server.is_ok() && client.is_ok())
}

/// Clean up stale containers/networks from previous jobs with this label.
pub async fn cleanup_stale(label: &str, log: &mut Vec<String>) -> Result<()> {
    log.push("##[group]Clean up resources from previous jobs".to_string());

    // Find stale containers
    let result = docker_cmd(
        &[
            "ps",
            "--all",
            "--quiet",
            "--no-trunc",
            &format!("--filter=label={label}"),
        ],
        log,
    )
    .await?;

    // Remove any found containers
    for line in &result {
        if !line.is_empty() {
            let _ = docker_cmd(&["rm", "--force", line], log).await;
        }
    }

    // Prune networks
    docker_cmd(
        &[
            "network",
            "prune",
            "--force",
            &format!("--filter=label={label}"),
        ],
        log,
    )
    .await?;

    log.push("##[endgroup]".to_string());
    Ok(())
}

/// Create the job Docker network.
pub async fn create_network(network: &str, label: &str, log: &mut Vec<String>) -> Result<()> {
    log.push("##[group]Create local container network".to_string());
    docker_cmd(&["network", "create", "--label", label, network], log).await?;
    log.push("##[endgroup]".to_string());
    Ok(())
}

/// Pull a Docker image.
pub async fn pull_image(image: &str, log: &mut Vec<String>) -> Result<()> {
    docker_cmd(&["pull", image], log).await?;
    Ok(())
}

/// Start the job container (long-running with `tail -f /dev/null` entrypoint).
///
/// Returns the full container ID.
pub async fn start_job_container(
    spec: &ContainerSpec,
    container_name: &str,
    label: &str,
    network: &str,
    work_dir: &str,
    runner_work: &str,
    runner_temp: &str,
    runner_externals: &str,
    runner_actions: &str,
    toolcache: &str,
    log: &mut Vec<String>,
) -> Result<String> {
    log.push("##[group]Starting job container".to_string());

    // Pull image
    pull_image(&spec.image, log).await?;

    let container_workdir = translate_to_container_path(work_dir, runner_work);

    let mut args: Vec<String> = vec![
        "create".into(),
        "--name".into(),
        container_name.into(),
        "--label".into(),
        label.into(),
        "--workdir".into(),
        container_workdir,
        "--network".into(),
        network.into(),
    ];

    // User options (e.g. --cpus 1)
    if !spec.options.is_empty() {
        for opt in split_options(&spec.options) {
            args.push(opt);
        }
    }

    // User env vars first (matching golden ordering)
    for (k, v) in &spec.env {
        args.push("-e".into());
        args.push(format!("{k}={v}"));
    }

    // Auto-injected env vars
    args.push("-e".into());
    args.push("HOME=/github/home".into());
    args.push("-e".into());
    args.push("GITHUB_ACTIONS=true".into());
    args.push("-e".into());
    args.push("CI=true".into());

    // Docker socket auto-mount (enables DinD)
    args.push("-v".into());
    args.push("/var/run/docker.sock:/var/run/docker.sock".into());

    // Standard mount table (matching golden traces)
    args.push("-v".into());
    args.push(format!("{runner_work}:/__w"));
    args.push("-v".into());
    args.push(format!("{runner_externals}:/__e:ro"));
    args.push("-v".into());
    args.push(format!("{runner_temp}:/__w/_temp"));
    args.push("-v".into());
    args.push(format!("{runner_actions}:/__w/_actions"));
    args.push("-v".into());
    args.push(format!("{toolcache}:/__t"));

    // GitHub home and workflow dirs
    let github_home = format!("{runner_temp}/_github_home");
    let github_workflow = format!("{runner_temp}/_github_workflow");
    std::fs::create_dir_all(&github_home).ok();
    std::fs::create_dir_all(&github_workflow).ok();
    args.push("-v".into());
    args.push(format!("{github_home}:/github/home"));
    args.push("-v".into());
    args.push(format!("{github_workflow}:/github/workflow"));

    // User volumes
    for vol in &spec.volumes {
        args.push("-v".into());
        args.push(vol.clone());
    }

    // Entrypoint override: keep container running
    args.push("--entrypoint".into());
    args.push("tail".into());
    args.push(spec.image.clone());
    args.push("-f".into());
    args.push("/dev/null".into());

    let result = docker_cmd(&args.iter().map(|s| s.as_str()).collect::<Vec<_>>(), log).await?;

    let container_id = result
        .first()
        .cloned()
        .unwrap_or_default()
        .trim()
        .to_string();

    if container_id.is_empty() {
        anyhow::bail!("Failed to create job container — no ID returned");
    }

    // Start the container
    docker_cmd(&["start", &container_id], log).await?;

    // Verify it's running
    docker_cmd(
        &[
            "ps",
            "--all",
            &format!("--filter=id={container_id}"),
            "--filter=status=running",
            "--no-trunc",
            "--format",
            "{{.ID}} {{.Status}}",
        ],
        log,
    )
    .await?;

    // Inspect env (for PATH extraction)
    docker_cmd(
        &[
            "inspect",
            "--format",
            "{{range .Config.Env}}{{println .}}{{end}}",
            &container_id,
        ],
        log,
    )
    .await?;

    log.push("##[endgroup]".to_string());
    Ok(container_id)
}

/// Start a service container.
///
/// Returns the full container ID.
pub async fn start_service_container(
    service: &ServiceSpec,
    container_name: &str,
    label: &str,
    network: &str,
    log: &mut Vec<String>,
) -> Result<String> {
    log.push(format!(
        "##[group]Starting {} service container",
        service.alias
    ));

    // Pull image
    pull_image(&service.image, log).await?;

    let mut args: Vec<String> = vec![
        "create".into(),
        "--name".into(),
        container_name.into(),
        "--label".into(),
        label.into(),
        "--network".into(),
        network.into(),
        "--network-alias".into(),
        service.alias.clone(),
    ];

    // Health check options from the `options` field
    if !service.options.is_empty() {
        for opt in split_options(&service.options) {
            args.push(opt);
        }
    }

    // Service env vars
    for (k, v) in &service.env {
        args.push("-e".into());
        args.push(format!("{k}={v}"));
    }

    // Auto-injected env
    args.push("-e".into());
    args.push("GITHUB_ACTIONS=true".into());
    args.push("-e".into());
    args.push("CI=true".into());

    // Port mappings
    for port in &service.ports {
        args.push("-p".into());
        args.push(port.clone());
    }

    // User volumes
    for vol in &service.volumes {
        args.push("-v".into());
        args.push(vol.clone());
    }

    args.push(service.image.clone());

    let result = docker_cmd(&args.iter().map(|s| s.as_str()).collect::<Vec<_>>(), log).await?;

    let container_id = result
        .first()
        .cloned()
        .unwrap_or_default()
        .trim()
        .to_string();

    if container_id.is_empty() {
        anyhow::bail!(
            "Failed to create service container '{}' — no ID returned",
            service.alias
        );
    }

    // Start the container
    docker_cmd(&["start", &container_id], log).await?;

    // Verify running
    docker_cmd(
        &[
            "ps",
            "--all",
            &format!("--filter=id={container_id}"),
            "--filter=status=running",
            "--no-trunc",
            "--format",
            "{{.ID}} {{.Status}}",
        ],
        log,
    )
    .await?;

    // Get port mappings
    docker_cmd(&["port", &container_id], log).await.ok();

    log.push("##[endgroup]".to_string());
    Ok(container_id)
}

/// Wait for all service containers to be healthy.
///
/// Golden trace shows 2s/3s backoff, then continues at health-interval.
pub async fn wait_for_services_healthy(
    services: &[(String, String, String)],
    log: &mut Vec<String>,
) -> Result<()> {
    log.push("##[group]Waiting for all services to be ready".to_string());

    let delays = [2u64, 3, 5, 5, 5, 10, 10, 10, 10, 10];
    for (alias, container_id, _) in services {
        let mut healthy = false;
        for (attempt, &delay) in delays.iter().enumerate() {
            let result = docker_cmd(
                &[
                    "inspect",
                    "--format={{if .Config.Healthcheck}}{{print .State.Health.Status}}{{end}}",
                    container_id,
                ],
                log,
            )
            .await?;

            let status = result.first().cloned().unwrap_or_default();
            let status = status.trim();

            if status.is_empty() || status == "none" {
                // No health check configured — consider healthy immediately
                info!("{alias} service has no health check — ready");
                healthy = true;
                break;
            } else if status == "healthy" {
                log.push(format!("{alias} service is healthy."));
                info!("{alias} service is healthy");
                healthy = true;
                break;
            } else if status == "unhealthy" {
                anyhow::bail!("{alias} service is unhealthy");
            } else {
                // starting
                log.push(format!(
                    "{alias} service is starting, waiting {delay} seconds before checking again."
                ));
                info!("{alias} service is starting (attempt {attempt}), waiting {delay}s");
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
            }
        }
        if !healthy {
            anyhow::bail!(
                "{alias} service did not become healthy after {} attempts",
                delays.len()
            );
        }
    }

    log.push("##[endgroup]".to_string());
    Ok(())
}

/// Execute a command inside the job container via `docker exec`.
pub async fn docker_exec(
    container_id: &str,
    program: &str,
    args: &[&str],
    workdir: &str,
    env: &HashMap<String, String>,
    cancel_rx: Option<tokio::sync::watch::Receiver<bool>>,
) -> Result<process::ProcessOutput> {
    let mut exec_args: Vec<String> = vec!["exec".into(), "-w".into(), workdir.into()];

    for (k, v) in env {
        exec_args.push("-e".into());
        exec_args.push(format!("{k}={v}"));
    }

    exec_args.push(container_id.into());
    exec_args.push(program.into());
    for arg in args {
        exec_args.push((*arg).into());
    }

    let args_ref: Vec<&str> = exec_args.iter().map(|s| s.as_str()).collect();
    process::invoke(
        "docker",
        &args_ref,
        Path::new("."),
        &HashMap::new(),
        None,
        cancel_rx,
    )
    .await
}

/// Get port mappings for a service container.
///
/// Returns port mappings as `(container_port, host_port)` pairs.
pub async fn get_port_mappings(container_id: &str) -> Vec<(String, String)> {
    let result = process::invoke(
        "docker",
        &["port", container_id],
        Path::new("."),
        &HashMap::new(),
        None,
        None,
    )
    .await;

    let mut mappings = Vec::new();
    if let Ok(output) = result {
        for line in &output.lines {
            // Format: "5432/tcp -> 0.0.0.0:32768"
            if let Some((container_part, host_part)) = line.split_once(" -> ") {
                let container_port = container_part.split('/').next().unwrap_or("").to_string();
                let host_port = host_part.rsplit(':').next().unwrap_or("").to_string();
                if !container_port.is_empty() && !host_port.is_empty() {
                    mappings.push((container_port, host_port));
                }
            }
        }
    }
    mappings
}

/// Stop and clean up all containers and the network.
///
/// Golden cleanup order: job container → per-service (logs then rm) → network.
pub async fn cleanup_containers(state: &ContainerState, log: &mut Vec<String>) -> Result<()> {
    // 1. Stop and remove job container
    if let Some(name) = &state.job_container_name {
        if let Some(id) = &state.job_container_id {
            log.push(format!("Stop and remove container: {name}"));
            let _ = docker_cmd(&["rm", "--force", id], log).await;
        }
    }

    // 2. Per-service: print logs, then remove
    for (alias, container_id, container_name) in &state.service_containers {
        log.push(format!("Print service container logs: {container_name}"));
        let _ = docker_cmd(&["logs", "--details", container_id], log).await;

        log.push(format!("Stop and remove container: {container_name}"));
        let _ = docker_cmd(&["rm", "--force", container_id], log).await;
        debug!("Removed service container {alias} ({container_id})");
    }

    // 3. Remove network
    log.push(format!("Remove container network: {}", state.network));
    let _ = docker_cmd(&["network", "rm", &state.network], log).await;

    Ok(())
}

// ── Naming helpers ───────────────────────────────────────────────────

/// Generate a 6-hex instance label.
pub fn generate_label() -> String {
    let bytes: [u8; 3] = rand_bytes();
    format!("{:02x}{:02x}{:02x}", bytes[0], bytes[1], bytes[2])
}

/// Generate a Docker network name: `github_network_<uuid-no-dashes>`.
pub fn generate_network_name() -> String {
    let id = uuid::Uuid::new_v4().to_string().replace('-', "");
    format!("github_network_{id}")
}

/// Generate a container name: `<32-hex-uuid>_<sanitized-image>_<6-hex>`.
pub fn container_name(image: &str, label: &str) -> String {
    let uuid = uuid::Uuid::new_v4().to_string().replace('-', "");
    let sanitized = sanitize_image_name(image);
    format!("{uuid}_{sanitized}_{label}")
}

/// Generate a shorter container name for docker:// actions: `<sanitized-image>_<6-hex>`.
pub fn action_container_name(image: &str, label: &str) -> String {
    let sanitized = sanitize_image_name(image);
    format!("{sanitized}_{label}")
}

/// Sanitize image name: remove colons, dots, dashes, and slashes.
fn sanitize_image_name(image: &str) -> String {
    image
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect()
}

/// Translate a host path to container path.
pub fn translate_to_container_path(host_path: &str, host_work: &str) -> String {
    if let Some(relative) = host_path.strip_prefix(host_work) {
        format!("/__w{relative}")
    } else {
        host_path.to_string()
    }
}

// ── Internal helpers ─────────────────────────────────────────────────

/// Run a docker command, logging the command line, and return stdout lines.
async fn docker_cmd(args: &[&str], log: &mut Vec<String>) -> Result<Vec<String>> {
    let cmd_line = format!("/usr/bin/docker {}", args.join(" "));
    log.push(format!("##[command]{cmd_line}"));
    debug!("docker {}", args.join(" "));

    let result = process::invoke("docker", args, Path::new("."), &HashMap::new(), None, None)
        .await
        .with_context(|| format!("docker {}", args.first().unwrap_or(&"")))?;

    // Log output lines
    for line in &result.lines {
        log.push(line.clone());
    }

    if result.exit_code != 0 {
        anyhow::bail!(
            "docker {} exited with code {}",
            args.first().unwrap_or(&""),
            result.exit_code
        );
    }

    Ok(result.lines)
}

/// Split Docker options string into individual arguments.
/// Handles simple quoting for health-check commands.
fn split_options(options: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut in_quote = None;
    for ch in options.chars() {
        match (ch, in_quote) {
            ('"', None) => in_quote = Some('"'),
            ('"', Some('"')) => in_quote = None,
            ('\'', None) => in_quote = Some('\''),
            ('\'', Some('\'')) => in_quote = None,
            (' ', None) | ('\t', None) => {
                if !current.is_empty() {
                    result.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }
    if !current.is_empty() {
        result.push(current);
    }
    result
}

/// Generate 3 random bytes using the rand crate.
fn rand_bytes() -> [u8; 3] {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    [rng.gen(), rng.gen(), rng.gen()]
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

    #[test]
    fn sanitize_image() {
        assert_eq!(sanitize_image_name("node:20-bookworm"), "node20bookworm");
        assert_eq!(sanitize_image_name("postgres:16"), "postgres16");
        assert_eq!(sanitize_image_name("redis:7-alpine"), "redis7alpine");
        assert_eq!(sanitize_image_name("nginx:1.27-alpine"), "nginx127alpine");
    }

    #[test]
    fn container_naming() {
        let label = "abc123";
        let name = container_name("node:20-bookworm", label);
        assert!(name.ends_with("_node20bookworm_abc123"));
        assert_eq!(name.len(), 32 + 1 + "node20bookworm".len() + 1 + 6);
    }

    #[test]
    fn action_container_naming() {
        let name = action_container_name("alpine:3.20", "abc123");
        assert_eq!(name, "alpine320_abc123");
    }

    #[test]
    fn parse_container_string() {
        let v = serde_json::json!("node:20");
        let spec = parse_container_spec(&v).unwrap();
        assert_eq!(spec.image, "node:20");
        assert!(spec.env.is_empty());
    }

    #[test]
    fn parse_container_mapping() {
        let v = serde_json::json!({
            "image": "alpine:3.20",
            "env": {"FOO": "bar"},
            "options": "--cpus 1",
            "ports": ["8080:80"],
            "volumes": ["data:/data"]
        });
        let spec = parse_container_spec(&v).unwrap();
        assert_eq!(spec.image, "alpine:3.20");
        assert_eq!(spec.env.get("FOO").unwrap(), "bar");
        assert_eq!(spec.options, "--cpus 1");
        assert_eq!(spec.ports, vec!["8080:80"]);
        assert_eq!(spec.volumes, vec!["data:/data"]);
    }

    #[test]
    fn parse_services() {
        let v = serde_json::json!({
            "postgres": {
                "image": "postgres:16",
                "env": {"POSTGRES_PASSWORD": "ci"},
                "options": "--health-cmd pg_isready"
            },
            "redis": {
                "image": "redis:7"
            }
        });
        let services = parse_service_specs(&v);
        assert_eq!(services.len(), 2);
    }

    #[test]
    fn label_is_6_hex() {
        let label = generate_label();
        assert_eq!(label.len(), 6);
        assert!(label.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn network_name_format() {
        let name = generate_network_name();
        assert!(name.starts_with("github_network_"));
        assert_eq!(name.len(), "github_network_".len() + 32);
    }

    #[test]
    fn non_empty_services_omits_empty() {
        let v = serde_json::json!({});
        let specs = parse_service_specs(&v);
        assert!(specs.is_empty());
    }
}
