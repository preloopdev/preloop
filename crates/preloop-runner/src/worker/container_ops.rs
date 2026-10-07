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
use std::net::SocketAddr;
use std::path::Path;
use tracing::{debug, info, warn};

use crate::process;

/// Registry credentials for pulling images from private registries.
///
/// Parsed from the `credentials` field in the container spec.
/// Matches `ContainerRegistryCredentials` in the official runner (JobContainer.cs:89-108).
#[derive(Debug, Clone)]
pub struct RegistryCredentials {
    pub username: String,
    pub password: String,
}

/// Parsed container spec from the job message.
#[derive(Debug, Clone)]
pub struct ContainerSpec {
    pub image: String,
    pub env: HashMap<String, String>,
    pub ports: Vec<String>,
    pub volumes: Vec<String>,
    pub options: String,
    /// Optional registry credentials for pulling from private registries.
    pub credentials: Option<RegistryCredentials>,
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
    /// Optional registry credentials for pulling from private registries.
    pub credentials: Option<RegistryCredentials>,
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

/// Evaluate TemplateToken expression nodes (`type: 3`) against the job
/// expression context, recursively.
///
/// The server keeps `container:`/`services:` raw and encodes a `${{ }}` value
/// as an expression token, so a matrix-driven
/// `container: ${{ matrix.build.container }}` arrives unevaluated. Without
/// this, [`parse_container_spec`] sees an object with no `image` key, returns
/// `None`, and the job silently runs on the VM instead of the container —
/// which is how `apk add` ended up executing on an Ubuntu host for curl's
/// Alpine matrix legs.
///
/// Returns a copy with expression tokens replaced by their evaluated values;
/// other objects and arrays are traversed, and scalar values are preserved.
/// Returns an error containing the expression and its evaluation error if any
/// token fails to evaluate.
pub(crate) fn evaluate_expression_tokens(
    value: &serde_json::Value,
    ctx: &preloop_gha_expressions::Context,
) -> Result<serde_json::Value, String> {
    match value {
        serde_json::Value::Object(map) => {
            if map.get("type").and_then(|t| t.as_u64()) == Some(3)
                && let Some(expr) = map.get("expr").and_then(|e| e.as_str())
            {
                return preloop_gha_expressions::eval_expression(expr, ctx).map_err(|error| {
                    format!("container/service expression `{expr}` could not be evaluated: {error}")
                });
            }
            let mut evaluated = serde_json::Map::new();
            for (key, item) in map {
                evaluated.insert(key.clone(), evaluate_expression_tokens(item, ctx)?);
            }
            Ok(serde_json::Value::Object(evaluated))
        }
        serde_json::Value::Array(items) => {
            let mut evaluated = Vec::with_capacity(items.len());
            for item in items {
                evaluated.push(evaluate_expression_tokens(item, ctx)?);
            }
            Ok(serde_json::Value::Array(evaluated))
        }
        other => Ok(other.clone()),
    }
}

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
            credentials: None,
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
            let credentials =
                map.get("credentials")
                    .and_then(|v| v.as_object())
                    .and_then(|creds| {
                        let username = creds
                            .get("username")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let password = creds
                            .get("password")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        if username.is_empty() && password.is_empty() {
                            None
                        } else {
                            Some(RegistryCredentials { username, password })
                        }
                    });
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
                credentials,
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
                    credentials: container.credentials,
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

/// Pull a Docker image, optionally authenticating to a private registry first.
///
/// When `credentials` is `Some`, logs into the registry using a temporary
/// Docker config directory (so the user's global `~/.docker/config.json` is
/// never touched), pulls with that config, then removes the credentials on
/// drop. Matches official runner `ContainerOperationProvider.cs:193-221`.
pub async fn pull_image(
    image: &str,
    credentials: Option<&RegistryCredentials>,
    log: &mut Vec<String>,
) -> Result<()> {
    if let Some(creds) = credentials {
        let config_dir = docker_registry_login(image, creds, log).await?;
        let config_path = config_dir.path().to_string_lossy().to_string();
        let result = docker_cmd(&["--config", &config_path, "pull", image], log).await;
        drop(config_dir); // deletes temp credentials dir
        result?;
    } else {
        docker_cmd(&["pull", image], log).await?;
    }
    Ok(())
}

/// Start the job container (long-running with `tail -f /dev/null` entrypoint).
///
/// Returns the full container ID.
#[allow(clippy::too_many_arguments)]
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
    engine: Option<&ContainerEngineAccess>,
    log: &mut Vec<String>,
) -> Result<String> {
    log.push("##[group]Starting job container".to_string());

    // Pull image
    pull_image(&spec.image, spec.credentials.as_ref(), log).await?;

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

    // User env vars first (matching golden ordering). Docker create uses
    // KEY=VALUE for non-empty values, but `-e KEY` for empty values to match
    // the official runner's inherit-from-host behavior.
    append_container_env(&mut args, &spec.env, engine);

    // Auto-injected env vars
    args.push("-e".into());
    args.push("HOME=/github/home".into());
    args.push("-e".into());
    args.push("GITHUB_ACTIONS=true".into());
    args.push("-e".into());
    args.push("CI=true".into());

    // Inject proxy env vars from host into container
    inject_proxy_env(&mut args, &spec.env, engine);

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

    // The official runner takes the first stdout line as the container ID
    // (`outputStrings.FirstOrDefault()` where the list is stdout-only). Parse
    // stdout separately — docker's platform warning lands on stderr and the
    // merged stream can surface it first.
    let result =
        docker_cmd_stdout(&args.iter().map(|s| s.as_str()).collect::<Vec<_>>(), log).await?;
    let container_id = result
        .first()
        .cloned()
        .unwrap_or_default()
        .trim()
        .to_owned();

    if container_id.is_empty() {
        anyhow::bail!(
            "Failed to create job container — no ID returned (output: {})",
            result.join(" | ")
        );
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
    engine: Option<&ContainerEngineAccess>,
    log: &mut Vec<String>,
) -> Result<String> {
    log.push(format!(
        "##[group]Starting {} service container",
        service.alias
    ));

    // Pull image
    pull_image(&service.image, service.credentials.as_ref(), log).await?;

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
    append_container_env(&mut args, &service.env, engine);

    // Auto-injected env
    args.push("-e".into());
    args.push("GITHUB_ACTIONS=true".into());
    args.push("-e".into());
    args.push("CI=true".into());

    // Inject proxy env vars from host into container
    inject_proxy_env(&mut args, &service.env, engine);

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

    let result =
        docker_cmd_stdout(&args.iter().map(|s| s.as_str()).collect::<Vec<_>>(), log).await?;
    let container_id = result
        .first()
        .cloned()
        .unwrap_or_default()
        .trim()
        .to_owned();

    if container_id.is_empty() {
        anyhow::bail!(
            "Failed to create service container '{}' — no ID returned (output: {})",
            service.alias,
            result.join(" | ")
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
/// Polls each container until its health status is `healthy`, `none` (no
/// health-check configured), or definitively `unhealthy`. Backoff follows
/// the official runner's `GetExponentialBackoff(attempt, min=2s, max=32s,
/// delta=2s)`: delay = min(2 + (2^attempt - 1) * 2, 32) seconds.
/// There is no hard retry cap — the outer job-level timeout or cancel channel
/// terminates a stuck wait.
pub async fn wait_for_services_healthy(
    services: &[(String, String, String)],
    log: &mut Vec<String>,
) -> Result<()> {
    log.push("##[group]Waiting for all services to be ready".to_string());

    for (alias, container_id, _) in services {
        let mut attempt: u32 = 0;
        loop {
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
                // No HEALTHCHECK directive — container is considered ready
                info!("{alias} service has no health check — ready");
                break;
            } else if status == "healthy" {
                log.push(format!("{alias} service is healthy."));
                info!("{alias} service is healthy");
                break;
            } else if status == "unhealthy" {
                anyhow::bail!("{alias} service is unhealthy");
            } else if status == "starting" {
                // Exponential backoff: min=2s, max=32s, delta=2s
                // delay = min(2 + (2^attempt - 1) * 2, 32)
                let additional = (1u64 << attempt).saturating_sub(1).saturating_mul(2);
                let delay = std::cmp::min(2u64 + additional, 32);
                log.push(format!(
                    "{alias} service is starting, waiting {delay} seconds before checking again."
                ));
                info!("{alias} service is starting (attempt {attempt}), waiting {delay}s");
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                attempt = attempt.saturating_add(1);
            } else {
                anyhow::bail!("{alias} service has unexpected health status: {status}");
            }
        }
    }

    log.push("##[endgroup]".to_string());
    Ok(())
}

/// Execute a command inside the job container via `docker exec`.
pub async fn docker_exec<'a>(
    container_id: &str,
    program: &str,
    args: &[&str],
    workdir: &str,
    env: &HashMap<String, String>,
    cancel_rx: Option<tokio::sync::watch::Receiver<bool>>,
    on_chunk: Option<crate::process::ChunkCallback<'a>>,
) -> Result<process::ProcessOutput> {
    let exec_args = build_docker_exec_args(container_id, program, args, workdir, env);
    let args_ref: Vec<&str> = exec_args.iter().map(|s| s.as_str()).collect();

    let keep_lines = on_chunk.is_none();
    process::invoke(
        "docker",
        &args_ref,
        Path::new("."),
        env,
        on_chunk,
        cancel_rx,
        keep_lines,
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
        true,
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
    if let Some(name) = &state.job_container_name
        && let Some(id) = &state.job_container_id
    {
        log.push(format!("Stop and remove container: {name}"));
        let _ = docker_cmd(&["rm", "--force", id], log).await;
    }

    // Per-service: print logs, then remove
    for (alias, container_id, container_name) in &state.service_containers {
        log.push(format!("Print service container logs: {container_name}"));
        // Bound the logs pulled into memory at teardown; a chatty
        // service would otherwise have its whole log buffered by the runner.
        let _ = docker_cmd(&["logs", "--details", "--tail", "5000", container_id], log).await;

        log.push(format!("Stop and remove container: {container_name}"));
        let _ = docker_cmd(&["rm", "--force", container_id], log).await;
        debug!("Removed service container {alias} ({container_id})");
    }

    // Remove network
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

// ── Container → engine reachability ─────────────────────────────────

/// Engine reachability for a job's containers.
///
/// The job message carries the engine's origin as the runner's *loopback*
/// bridge address (see `crate::control_bridge`): inside the guest, the runner
/// and host steps reach the engine there. A container has its own network
/// namespace, where that origin resolves to the container itself, so the
/// runner binds a second bridge on the address the container's network can
/// reach — its gateway — and rewrites the origin in every environment a
/// container process receives.
///
/// This mirrors the official runner's `TranslateToContainerPath`, which
/// likewise rewrites every environment value for `docker exec`/`docker run`
/// containers; the official runner hands containers GitHub's public URLs
/// unchanged, while preloop's engine origin needs the container-visible form.
///
/// One access per job: [`ensure_container_engine`] creates it, the container
/// step handlers call [`Self::translate_env`], and dropping it (with the job)
/// releases the listener.
pub struct ContainerEngineAccess {
    /// The origin the job message advertises (the runner's loopback bridge).
    host_origin: String,
    /// The origin containers must use instead (their network's gateway).
    container_origin: String,
    /// Listener serving `container_origin`. `None` only in tests (see
    /// [`Self::for_tests`]); a real access always owns the listener that makes
    /// its origin reachable.
    _bridge: Option<crate::control_bridge::ControlBridge>,
}

impl ContainerEngineAccess {
    /// The origin the job message advertises.
    pub fn host_origin(&self) -> &str {
        &self.host_origin
    }

    /// The origin the job's containers can reach the engine at.
    pub fn container_origin(&self) -> &str {
        &self.container_origin
    }

    /// The `host:port` a proxy bypass list must carry so engine requests from
    /// a container skip the proxy.
    pub fn no_proxy_authority(&self) -> &str {
        strip_scheme(&self.container_origin)
    }

    /// Rewrite every embedded reference to the engine's advertised origin in a
    /// container-bound environment. Returns how many values changed.
    pub fn translate_env(&self, env: &mut HashMap<String, String>) -> usize {
        translate_engine_origin(env, &self.host_origin, &self.container_origin)
    }

    #[cfg(test)]
    pub(crate) fn for_tests(host_origin: &str, container_origin: &str) -> Self {
        Self {
            host_origin: host_origin.to_owned(),
            container_origin: container_origin.to_owned(),
            _bridge: None,
        }
    }
}

/// Rewrite the engine's advertised origin inside container-bound environment
/// values.
///
/// Covered by the substring rewrite: `ACTIONS_RUNTIME_URL`,
/// `ACTIONS_RESULTS_URL`, `ACTIONS_CACHE_URL`, `ACTIONS_ID_TOKEN_REQUEST_URL`,
/// the `INPUT_GITHUB-SERVER-URL` a snapshot-redirected `actions/checkout`
/// reads, and `GIT_CONFIG_*` snapshot origin rewrites — anything holding the
/// advertised origin, wherever it sits in the value.
pub fn translate_engine_origin(
    env: &mut HashMap<String, String>,
    host_origin: &str,
    container_origin: &str,
) -> usize {
    if host_origin.is_empty() || host_origin == container_origin {
        return 0;
    }
    let mut rewritten = 0;
    for value in env.values_mut() {
        if value.contains(host_origin) {
            *value = value.replace(host_origin, container_origin);
            rewritten += 1;
        }
    }
    // A proxied container must reach the container-facing origin directly. A
    // bypass list written for the advertised origin names the loopback
    // address (`127.0.0.1`), which no longer matches after the rewrite, and a
    // proxied engine request fails even while the bridge is listening.
    let authority = strip_scheme(container_origin);
    for key in ["NO_PROXY", "no_proxy"] {
        if let Some(value) = env.get_mut(key) {
            *value = extend_no_proxy(value, authority);
        }
    }
    rewritten
}

/// The `host:port` of an origin URL.
fn strip_scheme(origin: &str) -> &str {
    origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))
        .unwrap_or(origin)
        .trim_end_matches('/')
}

/// Append `authority` to a proxy bypass list, preserving existing entries and
/// never duplicating it.
fn extend_no_proxy(value: &str, authority: &str) -> String {
    if value
        .split(',')
        .any(|entry| entry.trim().eq_ignore_ascii_case(authority))
    {
        return value.to_owned();
    }
    if value.trim().is_empty() {
        return authority.to_owned();
    }
    format!("{value},{authority}")
}

/// Prepare container → engine reachability for a job.
///
/// Local mode only: this runs when the orchestrator set the control
/// environment (`PRELOOP_CONTROL_ORIGIN` with `PRELOOP_CONTROL_SOCKET` or
/// `PRELOOP_CONTROL_UPSTREAM`) — the deployment where the guest reaches the
/// engine through the control bridge at a loopback origin. A hosted runner's
/// engine URL is routable and none of those variables is set, so the first
/// lookup returns `None` and container URLs are never rewritten.
///
/// `network` is the Docker network the job's containers attach to — the job
/// network created by [`create_network`], or `None` for the default bridge a
/// host job's `docker://` action container runs on. Returns `None` when the
/// advertised origin is not a loopback bridge address (a runner whose origin
/// is already container-reachable, or one with no control bridge at all) or
/// when the network's gateway cannot be resolved.
pub async fn container_engine_access(network: Option<&str>) -> Option<ContainerEngineAccess> {
    let host_origin = std::env::var(crate::control_bridge::CONTROL_ORIGIN_ENV).ok()?;
    let host_origin = host_origin.trim_end_matches('/').to_owned();
    // Only the bridged configuration needs translation: loopback is what a
    // container would resolve to itself.
    crate::control_bridge::loopback_address(&host_origin)?;
    let gateway = network_gateway(network.unwrap_or("bridge")).await?;
    // Any free port works: the rewrite replaces the whole origin. Reusing the
    // engine's port would collide with whatever the workflow itself listens
    // on there (a published service port, a step's server).
    let bridge = match crate::control_bridge::spawn_container_reachable(SocketAddr::new(gateway, 0))
        .await
    {
        Ok(Some(bridge)) => bridge,
        Ok(None) => return None,
        Err(error) => {
            // No free port on the gateway, or the gateway is not a guest
            // address. Container steps then cannot reach the engine at all.
            warn!(
                %host_origin,
                %gateway,
                %error,
                "container-facing control bridge unavailable; container steps cannot reach the engine"
            );
            return None;
        }
    };
    let container_address = bridge.address();
    // Keep the advertised scheme: the splice is transparent, so a TLS origin
    // stays TLS and the container's trust store decides.
    let scheme = if host_origin.starts_with("https://") {
        "https"
    } else {
        "http"
    };
    let container_origin = format!("{scheme}://{container_address}");
    info!(
        %host_origin,
        %container_origin,
        network = network.unwrap_or("bridge"),
        "container steps reach the engine through the Docker network gateway"
    );
    Some(ContainerEngineAccess {
        host_origin,
        container_origin,
        _bridge: Some(bridge),
    })
}

/// Set up (once) and return the job's container engine access.
///
/// Container jobs set this up while initializing their network; a host job
/// that runs a `docker://` action is the other entry point — its action
/// container joins the default bridge (or the job network of a
/// services-only job) and needs the same reachability.
pub async fn ensure_container_engine(
    job: &mut super::contexts::JobContext,
) -> Option<std::sync::Arc<ContainerEngineAccess>> {
    if job.container_engine.is_none() {
        let network = job
            .container_state
            .as_ref()
            .map(|state| state.network.clone());
        job.container_engine = container_engine_access(network.as_deref())
            .await
            .map(std::sync::Arc::new);
    }
    job.container_engine.clone()
}

/// The gateway address of a Docker network, as seen from its containers.
///
/// The gateway is the host end of a container's default route: the one
/// address that always reaches the guest itself from inside a container's
/// network namespace. Not written to the job log — it is runner plumbing, not
/// a step command.
async fn network_gateway(network: &str) -> Option<std::net::IpAddr> {
    let result = process::invoke(
        "docker",
        &[
            "network",
            "inspect",
            "--format",
            "{{json .IPAM.Config}}",
            network,
        ],
        Path::new("."),
        &HashMap::new(),
        None,
        None,
        true,
    )
    .await
    .ok()?;
    if result.exit_code != 0 {
        return None;
    }
    parse_network_gateway(&result.lines.join("\n"))
}

/// Parse the `Gateway` fields of `docker network inspect --format
/// '{{json .IPAM.Config}}'` output, preferring IPv4.
fn parse_network_gateway(output: &str) -> Option<std::net::IpAddr> {
    let configs: Vec<serde_json::Value> = serde_json::from_str(output.trim()).ok()?;
    let mut first: Option<std::net::IpAddr> = None;
    for config in configs {
        let Some(gateway) = config.get("Gateway").and_then(|value| value.as_str()) else {
            continue;
        };
        let Ok(address) = gateway.parse::<std::net::IpAddr>() else {
            continue;
        };
        if address.is_ipv4() {
            return Some(address);
        }
        first.get_or_insert(address);
    }
    first
}

/// Extract the registry hostname from a Docker image reference.
///
/// Matches `DockerUtil.ParseRegistryHostnameFromImageName` (DockerUtil.cs:53-67):
/// - `ghcr.io/owner/image` → `"ghcr.io"` (first component has `.`)
/// - `registry:5000/image` → `"registry:5000"` (first component has `:`)
/// - `owner/image` → `""` (DockerHub shorthand)
/// - `ubuntu` → `""` (DockerHub image)
fn parse_registry_from_image(image: &str) -> &str {
    let parts: Vec<&str> = image.splitn(3, '/').collect();
    if parts.len() >= 2 && (parts[0].contains('.') || parts[0].contains(':')) {
        parts[0]
    } else {
        ""
    }
}

/// Authenticate to the registry that hosts `image`, placing credentials in a
/// fresh temporary directory so the user's global `~/.docker/config.json` is
/// never touched. Returns the `TempDir`; dropping it deletes the config.
///
/// Retries up to 3 times with 5 s / 10 s exponential backoff, matching
/// official runner `ContainerOperationProvider.cs:470-499`.
async fn docker_registry_login(
    image: &str,
    creds: &RegistryCredentials,
    log: &mut Vec<String>,
) -> Result<tempfile::TempDir> {
    use tokio::io::AsyncWriteExt;

    let registry = parse_registry_from_image(image);
    let config_dir =
        tempfile::TempDir::new().context("creating docker config directory for registry auth")?;

    let registry_display = if registry.is_empty() {
        "docker.io".to_string()
    } else {
        registry.to_string()
    };
    log.push(format!(
        "##[command]docker --config <tmpdir> login{} -u {} --password-stdin",
        if registry.is_empty() {
            String::new()
        } else {
            format!(" {registry}")
        },
        creds.username
    ));

    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0u32..3 {
        if attempt > 0 {
            let secs = 5u64 * (1 << (attempt - 1));
            log.push(format!(
                "##[warning]docker login for '{registry_display}' failed (attempt {attempt}), \
                 retrying in {secs}s"
            ));
            tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
        }

        let mut cmd = tokio::process::Command::new("docker");
        cmd.arg("--config").arg(config_dir.path());
        cmd.arg("login");
        if !registry.is_empty() {
            cmd.arg(registry);
        }
        cmd.arg("-u")
            .arg(&creds.username)
            .arg("--password-stdin")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn().context("spawning docker login")?;
        if let Some(mut stdin) = child.stdin.take() {
            let _ = stdin.write_all(creds.password.as_bytes()).await;
        }
        let output = child.wait_with_output().await.context("docker login")?;

        for line in String::from_utf8_lossy(&output.stdout).lines() {
            log.push(line.to_string());
        }
        for line in String::from_utf8_lossy(&output.stderr).lines() {
            log.push(line.to_string());
        }

        if output.status.success() {
            return Ok(config_dir);
        }
        last_err = Some(anyhow::anyhow!(
            "docker login for '{}' failed with exit code {}",
            registry_display,
            output.status.code().unwrap_or(-1)
        ));
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("docker login failed")))
}

// ── Internal helpers ─────────────────────────────────────────────────

/// Wait for the container daemon that provisioning boots in the background.
///
/// The orchestrator no longer gates runner readiness on the 5-15 s dockerd
/// cold boot, so a container job paired immediately after provisioning can
/// reach setup before the daemon answers. Poll `docker info` (the same
/// readiness definition the start script uses) until it succeeds or the
/// timeout elapses; only container setup calls this, so plain jobs and
/// ad-hoc `docker` steps never wait here.
pub(crate) async fn wait_for_daemon_ready(log: &mut Vec<String>) -> Result<()> {
    const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);
    const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
    log.push(
        "Waiting for the container daemon (booted in background at provision time)".to_string(),
    );
    let start = std::time::Instant::now();
    loop {
        let ready = process::invoke(
            "docker",
            &["info"],
            Path::new("."),
            &HashMap::new(),
            None,
            None,
            false,
        )
        .await
        .map(|output| output.exit_code == 0)
        .unwrap_or(false);
        if ready {
            log.push("Container daemon is ready".to_string());
            return Ok(());
        }
        if start.elapsed() >= TIMEOUT {
            anyhow::bail!("container daemon did not become ready within 90s");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Run a docker command, logging the command line, and return stdout lines.
async fn docker_cmd(args: &[&str], log: &mut Vec<String>) -> Result<Vec<String>> {
    let cmd_line = format!("/usr/bin/docker {}", args.join(" "));
    log.push(format!("##[command]{cmd_line}"));
    debug!("docker {}", args.join(" "));

    let result = process::invoke(
        "docker",
        args,
        Path::new("."),
        &HashMap::new(),
        None,
        None,
        true,
    )
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

/// Run a docker command and return the stdout-only lines.
///
/// The official runner collects stdout and stderr separately
/// (`OutputDataReceived` vs `ErrorDataReceived`): everything is echoed to the
/// log, but only stdout feeds the parse. `docker create` prints its container
/// ID as the single stdout line, and docker's platform warning lands on
/// stderr — the merged stream can surface the warning first, so parsing the
/// merged stream is how the ID gets lost.
async fn docker_cmd_stdout(args: &[&str], log: &mut Vec<String>) -> Result<Vec<String>> {
    let cmd_line = format!("/usr/bin/docker {}", args.join(" "));
    log.push(format!("##[command]{cmd_line}"));
    debug!("docker {}", args.join(" "));

    let result = process::invoke(
        "docker",
        args,
        Path::new("."),
        &HashMap::new(),
        None,
        None,
        true,
    )
    .await
    .with_context(|| format!("docker {}", args.first().unwrap_or(&"")))?;

    // Log every line (both streams), matching the official runner echoing
    // stdout and stderr to the console.
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

    Ok(result.stdout_lines)
}

fn push_docker_create_env(args: &mut Vec<String>, key: &str, value: &str) {
    args.push("-e".into());
    if value.is_empty() {
        args.push(key.to_string());
    } else {
        args.push(format!("{key}={value}"));
    }
}

/// Append a container's workflow-declared environment to `docker create`,
/// rewriting engine URLs to the container-reachable origin.
///
/// The job container and every service container share the job network, so
/// the engine's advertised loopback origin is as unreachable to them as it is
/// to a `docker://` action container; any value a workflow plants there (a
/// job-level `ENGINE_URL` handed to a service, say) must name their gateway
/// instead. Values are inlined (`-e KEY=VALUE`) rather than inherited from the
/// CLI environment, so the rewrite has to happen here.
fn append_container_env(
    args: &mut Vec<String>,
    env: &HashMap<String, String>,
    engine: Option<&ContainerEngineAccess>,
) {
    let mut translated = env.clone();
    if let Some(engine) = engine {
        engine.translate_env(&mut translated);
    }
    for (key, value) in &translated {
        push_docker_create_env(args, key, value);
    }
}

fn push_docker_inherited_env(args: &mut Vec<String>, key: &str) {
    args.push("-e".into());
    args.push(key.to_string());
}

/// Inject web proxy env vars into container if configured on the host.
///
/// Matches official runner `ContainerInfo.UpdateWebProxyEnv()`:
/// - `HTTP_PROXY`/`http_proxy` from `$HTTP_PROXY` or `$http_proxy`
/// - `HTTPS_PROXY`/`https_proxy` from `$HTTPS_PROXY` or `$https_proxy`
/// - `NO_PROXY`/`no_proxy` from `$NO_PROXY` or `$no_proxy`
///
/// Uses TryAdd semantics: only injects if not already set by the container's own env.
///
/// The one divergence the engine rewrite needs: a `NO_PROXY`/`no_proxy` the
/// host *does* declare is injected already extended with the container-facing
/// engine authority, because the rewrite moves engine requests off the
/// loopback address the list was written for. A host that declares no bypass
/// list gets none injected — that is the official runner's shape, and
/// inventing one would change the container environment beyond what the
/// engine-origin rewrite justifies.
fn inject_proxy_env(
    args: &mut Vec<String>,
    user_env: &HashMap<String, String>,
    engine: Option<&ContainerEngineAccess>,
) {
    inject_proxy_env_lookup(args, user_env, engine, |name| std::env::var(name).ok());
}

/// [`inject_proxy_env`] with the host environment behind a `lookup` closure,
/// so tests can supply proxy settings without writing the process environment
/// (`set_var` is unsafe under edition 2024 and this crate forbids `unsafe`).
fn inject_proxy_env_lookup(
    args: &mut Vec<String>,
    user_env: &HashMap<String, String>,
    engine: Option<&ContainerEngineAccess>,
    lookup: impl Fn(&str) -> Option<String>,
) {
    let proxy_vars = [
        ("HTTP_PROXY", "http_proxy"),
        ("HTTPS_PROXY", "https_proxy"),
        ("NO_PROXY", "no_proxy"),
    ];

    for (upper, lower) in &proxy_vars {
        // Read from host environment (check both cases)
        let value = lookup(upper).or_else(|| lookup(lower)).unwrap_or_default();
        if value.is_empty() {
            continue;
        }
        // The container-facing engine origin must bypass the proxy: a bypass
        // list inherited for the advertised loopback origin does not cover
        // the gateway the container actually reaches the engine at.
        let value = if *upper == "NO_PROXY" {
            engine.map_or_else(
                || value.clone(),
                |engine| extend_no_proxy(&value, engine.no_proxy_authority()),
            )
        } else {
            value.clone()
        };
        // TryAdd: only inject if user hasn't already set this key
        if !user_env.contains_key(*upper) {
            push_docker_create_env(args, upper, &value);
        }
        if !user_env.contains_key(*lower) {
            push_docker_create_env(args, lower, &value);
        }
    }
}

/// Public wrapper for docker action containers.
/// Same as `inject_proxy_env` but callable from handler modules.
pub fn inject_proxy_env_for_docker(
    args: &mut Vec<String>,
    env: &HashMap<String, String>,
    engine: Option<&ContainerEngineAccess>,
) {
    inject_proxy_env(args, env, engine);
}

fn build_docker_exec_args(
    container_id: &str,
    program: &str,
    args: &[&str],
    workdir: &str,
    env: &HashMap<String, String>,
) -> Vec<String> {
    let mut exec_args: Vec<String> = vec!["exec".into(), "-i".into(), "-w".into(), workdir.into()];

    for key in env.keys() {
        // Match official StepHost: pass only the key on the Docker CLI and put
        // the value in the docker client process environment, avoiding secret
        // values in argv and docker inspect output.
        push_docker_inherited_env(&mut exec_args, key);
    }

    exec_args.push(container_id.into());
    exec_args.push(program.into());
    for arg in args {
        exec_args.push((*arg).into());
    }

    exec_args
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
    [rng.r#gen(), rng.r#gen(), rng.r#gen()]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A matrix-driven `container: ${{ matrix.build.container }}` arrives as a
    /// type-3 expression token; evaluating it is what turns the job into a
    /// container job (curl's Alpine legs otherwise ran `apk` on the host).
    #[test]
    fn expression_container_token_evaluates_against_the_matrix() {
        let mut ctx = preloop_gha_expressions::Context::new();
        ctx.insert(
            "matrix",
            serde_json::json!({"build": {"container": "alpine:3.22"}}),
        );
        let token = serde_json::json!({
            "type": 3,
            "file": 1,
            "line": 1,
            "col": 1,
            "expr": "matrix.build.container"
        });

        let evaluated = evaluate_expression_tokens(&token, &ctx).expect("expression evaluates");
        assert_eq!(evaluated, serde_json::json!("alpine:3.22"));
        let spec = parse_container_spec(&evaluated).expect("container spec parses");
        assert_eq!(spec.image, "alpine:3.22");

        // Mapping form: the expression sits on the `image` entry.
        let mapping = serde_json::json!({
            "type": 2,
            "map": [
                {"Key": {"type": 0, "lit": "image"}, "Value": token.clone()},
                {"Key": {"type": 0, "lit": "options"}, "Value": {"type": 0, "lit": "--cpus 2"}}
            ]
        });
        let evaluated = evaluate_expression_tokens(&mapping, &ctx).expect("mapping evaluates");
        let spec = parse_container_spec(&evaluated).expect("mapping container spec parses");
        assert_eq!(spec.image, "alpine:3.22");
        assert_eq!(spec.options, "--cpus 2");

        // A failing expression must be reported, not silently downgraded to a
        // host job (`parse_container_spec` would return None and run_job would
        // continue without the declared container).
        let broken = serde_json::json!({
            "type": 3,
            "file": 1,
            "line": 1,
            "col": 1,
            "expr": "fromJSON('not json')"
        });
        let error = evaluate_expression_tokens(&broken, &ctx).expect_err("bad JSON must fail");
        assert!(
            error.contains("could not be evaluated"),
            "unexpected error: {error}"
        );
    }

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

    #[test]
    fn docker_create_env_uses_inherit_form_for_empty_values() {
        let mut args = Vec::new();

        push_docker_create_env(&mut args, "EMPTY_VAR", "");
        push_docker_create_env(&mut args, "SET_VAR", "visible");

        assert_eq!(
            args,
            vec![
                "-e".to_string(),
                "EMPTY_VAR".to_string(),
                "-e".to_string(),
                "SET_VAR=visible".to_string(),
            ]
        );
    }

    #[test]
    fn docker_exec_env_args_do_not_include_secret_values() {
        let env = HashMap::from([("MY_SECRET".to_string(), "s3cr3t".to_string())]);

        let args = build_docker_exec_args("container-id", "sh", &["-c", "true"], "/__w/repo", &env);

        assert!(args.windows(2).any(|pair| pair == ["-e", "MY_SECRET"]));
        assert!(!args.iter().any(|arg| arg.contains("s3cr3t")));
    }

    // --- container/step host gap coverage ---

    #[test]
    fn split_options_handles_quotes() {
        assert_eq!(
            split_options("--cpus 1 --health-cmd 'pg_isready -U postgres'"),
            vec!["--cpus", "1", "--health-cmd", "pg_isready -U postgres"]
        );
        assert_eq!(
            split_options("--health-cmd \"pg_isready -U ci\" --memory 512m"),
            vec!["--health-cmd", "pg_isready -U ci", "--memory", "512m"]
        );
        assert_eq!(split_options(""), Vec::<String>::new());
    }

    #[test]
    fn translate_to_container_path_various() {
        // Nested path
        assert_eq!(
            translate_to_container_path(
                "/home/runner/_work/repo/repo/subdir",
                "/home/runner/_work"
            ),
            "/__w/repo/repo/subdir"
        );
        // Exact match of work dir
        assert_eq!(
            translate_to_container_path("/home/runner/_work", "/home/runner/_work"),
            "/__w"
        );
        // Non-matching path stays unchanged
        assert_eq!(
            translate_to_container_path("/tmp/something", "/home/runner/_work"),
            "/tmp/something"
        );
    }

    #[test]
    fn parse_container_spec_with_template_token() {
        // GitHub sends container specs as TemplateTokens
        let v = serde_json::json!({
            "type": 2,
            "map": [
                {
                    "Key": {"type": 0, "lit": "image"},
                    "Value": {"type": 0, "lit": "node:20"}
                },
                {
                    "Key": {"type": 0, "lit": "options"},
                    "Value": {"type": 0, "lit": "--cpus 1"}
                }
            ]
        });
        let spec = parse_container_spec(&v).unwrap();
        assert_eq!(spec.image, "node:20");
        assert_eq!(spec.options, "--cpus 1");
    }

    #[test]
    fn parse_service_specs_with_template_tokens() {
        let v = serde_json::json!({
            "type": 2,
            "map": [
                {
                    "Key": {"type": 0, "lit": "db"},
                    "Value": {
                        "type": 2,
                        "map": [
                            {
                                "Key": {"type": 0, "lit": "image"},
                                "Value": {"type": 0, "lit": "postgres:16"}
                            }
                        ]
                    }
                }
            ]
        });
        let specs = parse_service_specs(&v);
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].alias, "db");
        assert_eq!(specs[0].image, "postgres:16");
    }

    #[test]
    fn docker_exec_args_include_workdir_and_env() {
        let env = HashMap::from([
            ("FOO".to_string(), "bar".to_string()),
            ("BAZ".to_string(), "qux".to_string()),
        ]);
        let args = build_docker_exec_args("cid123", "sh", &["-c", "echo hi"], "/__w/repo", &env);

        // Must start with exec -i -w <workdir>
        assert_eq!(args[0], "exec");
        assert_eq!(args[1], "-i");
        assert_eq!(args[2], "-w");
        assert_eq!(args[3], "/__w/repo");

        // Must contain -e for each env key (not values!)
        let env_keys: Vec<_> = args
            .windows(2)
            .filter(|w| w[0] == "-e")
            .map(|w| w[1].clone())
            .collect();
        assert!(env_keys.contains(&"FOO".to_string()));
        assert!(env_keys.contains(&"BAZ".to_string()));
        // Values must NOT appear
        assert!(
            !args
                .iter()
                .any(|a| a == "bar" || a == "qux" || a.contains("FOO=bar"))
        );

        // Must end with container_id, program, args
        assert!(args.contains(&"cid123".to_string()));
        assert!(args.contains(&"sh".to_string()));
        assert!(args.contains(&"-c".to_string()));
        assert!(args.contains(&"echo hi".to_string()));
    }

    #[test]
    fn parse_container_spec_string_with_tag() {
        let v = serde_json::json!("ubuntu:22.04");
        let spec = parse_container_spec(&v).unwrap();
        assert_eq!(spec.image, "ubuntu:22.04");
        assert!(spec.env.is_empty());
        assert!(spec.options.is_empty());
        assert!(spec.ports.is_empty());
        assert!(spec.volumes.is_empty());
    }

    #[test]
    fn parse_container_spec_full_mapping() {
        let v = serde_json::json!({
            "image": "node:20",
            "env": {"NODE_ENV": "test", "CI": "true"},
            "options": "--memory 1g --cpus 2",
            "ports": ["3000:3000", "5432:5432"],
            "volumes": ["/data:/data", "cache:/cache"]
        });
        let spec = parse_container_spec(&v).unwrap();
        assert_eq!(spec.image, "node:20");
        assert_eq!(spec.env.len(), 2);
        assert_eq!(spec.env.get("NODE_ENV").unwrap(), "test");
        assert_eq!(spec.options, "--memory 1g --cpus 2");
        assert_eq!(spec.ports, vec!["3000:3000", "5432:5432"]);
        assert_eq!(spec.volumes, vec!["/data:/data", "cache:/cache"]);
    }

    #[test]
    fn push_docker_create_env_non_empty_value() {
        let mut args = Vec::new();
        push_docker_create_env(&mut args, "HTTP_PROXY", "http://proxy.test:8080");
        assert_eq!(args, vec!["-e", "HTTP_PROXY=http://proxy.test:8080"]);
    }

    #[test]
    fn push_docker_create_env_empty_value_inherits() {
        let mut args = Vec::new();
        push_docker_create_env(&mut args, "HTTP_PROXY", "");
        // Empty value → just key (inherit from host)
        assert_eq!(args, vec!["-e", "HTTP_PROXY"]);
    }

    #[test]
    fn push_docker_inherited_env_key_only() {
        let mut args = Vec::new();
        push_docker_inherited_env(&mut args, "SOME_VAR");
        assert_eq!(args, vec!["-e", "SOME_VAR"]);
    }

    /// The engine advertises its loopback bridge origin in every URL a step
    /// receives; a container must see its own network's address instead. The
    /// rewrite is value-based because the origin appears both as whole URLs
    /// (ACTIONS_* endpoints, checkout inputs) and embedded in larger values
    /// (git config snapshot rewrites).
    #[test]
    fn translate_engine_origin_rewrites_embedded_engine_urls() {
        let mut env = HashMap::from([
            (
                "ACTIONS_RUNTIME_URL".to_string(),
                "http://127.0.0.1:9198/broker/7/".to_string(),
            ),
            (
                "ACTIONS_RESULTS_URL".to_string(),
                "http://127.0.0.1:9198/".to_string(),
            ),
            (
                "ACTIONS_CACHE_URL".to_string(),
                "http://127.0.0.1:9198/".to_string(),
            ),
            (
                "ACTIONS_ID_TOKEN_REQUEST_URL".to_string(),
                "http://127.0.0.1:9198/runner/server/_apis/distributedtask/hubs/actions/plans/1/jobs/2/oidctoken?api-version=2.0".to_string(),
            ),
            (
                "INPUT_GITHUB-SERVER-URL".to_string(),
                "http://127.0.0.1:9198".to_string(),
            ),
            (
                "GIT_CONFIG_VALUE_0".to_string(),
                "url.http://127.0.0.1:9198/snapshots/run-1.insteadOf=https://github.com/".to_string(),
            ),
            ("GITHUB_SERVER_URL".to_string(), "https://github.com".to_string()),
            (
                // A bypass list written for the advertised origin names the
                // loopback address; the proxy would otherwise swallow engine
                // requests to the container-facing origin.
                "NO_PROXY".to_string(),
                "127.0.0.1,localhost".to_string(),
            ),
            ("PATH".to_string(), "/usr/bin:/bin".to_string()),
        ]);

        let changed =
            translate_engine_origin(&mut env, "http://127.0.0.1:9198", "http://172.18.0.1:9198");

        assert_eq!(changed, 6);
        assert_eq!(
            env["ACTIONS_RUNTIME_URL"],
            "http://172.18.0.1:9198/broker/7/"
        );
        assert_eq!(env["ACTIONS_RESULTS_URL"], "http://172.18.0.1:9198/");
        assert_eq!(env["ACTIONS_CACHE_URL"], "http://172.18.0.1:9198/");
        assert_eq!(
            env["ACTIONS_ID_TOKEN_REQUEST_URL"],
            "http://172.18.0.1:9198/runner/server/_apis/distributedtask/hubs/actions/plans/1/jobs/2/oidctoken?api-version=2.0"
        );
        assert_eq!(env["INPUT_GITHUB-SERVER-URL"], "http://172.18.0.1:9198");
        assert_eq!(
            env["GIT_CONFIG_VALUE_0"],
            "url.http://172.18.0.1:9198/snapshots/run-1.insteadOf=https://github.com/"
        );
        // Values without the engine origin are untouched.
        assert_eq!(env["GITHUB_SERVER_URL"], "https://github.com");
        assert_eq!(env["PATH"], "/usr/bin:/bin");
        // The bypass list gains the container-facing authority.
        assert_eq!(env["NO_PROXY"], "127.0.0.1,localhost,172.18.0.1:9198");

        // Rewriting is idempotent: a second pass (e.g. a nested action
        // rebuilding its env) must not duplicate the bypass entry.
        let changed =
            translate_engine_origin(&mut env, "http://127.0.0.1:9198", "http://172.18.0.1:9198");
        assert_eq!(changed, 0);
        assert_eq!(env["NO_PROXY"], "127.0.0.1,localhost,172.18.0.1:9198");
    }

    #[test]
    fn extend_no_proxy_preserves_entries() {
        assert_eq!(extend_no_proxy("", "172.18.0.1:9198"), "172.18.0.1:9198");
        assert_eq!(
            extend_no_proxy("localhost", "172.18.0.1:9198"),
            "localhost,172.18.0.1:9198"
        );
        assert_eq!(
            extend_no_proxy("localhost,172.18.0.1:9198", "172.18.0.1:9198"),
            "localhost,172.18.0.1:9198"
        );
        assert_eq!(
            extend_no_proxy(" 172.18.0.1:9198 ", "172.18.0.1:9198"),
            " 172.18.0.1:9198 "
        );
    }

    #[test]
    fn strip_scheme_yields_proxy_bypass_list_authorities() {
        assert_eq!(strip_scheme("http://172.18.0.1:9198"), "172.18.0.1:9198");
        assert_eq!(strip_scheme("https://[fd00::1]:9198/"), "[fd00::1]:9198");
    }

    /// Values of `-e KEY=VALUE` arguments for one key, in argv order.
    fn env_arg_values<'a>(args: &'a [String], key: &str) -> Vec<&'a str> {
        let prefix = format!("{key}=");
        args.iter()
            .filter_map(|arg| arg.strip_prefix(prefix.as_str()))
            .collect()
    }

    /// Proxy injection must not replace a bypass list: the host's list is
    /// injected exactly once, already carrying the container-facing
    /// authority, so the host's own entries (an internal registry, say)
    /// survive. Docker applies the last `-e` value per key, so a second push
    /// would drop them — the ordering hazard the review caught.
    #[test]
    fn inject_proxy_env_extends_the_host_bypass_list_once() {
        let access =
            ContainerEngineAccess::for_tests("http://127.0.0.1:9198", "http://172.18.0.1:9198");
        let host = HashMap::from([
            (
                "HTTP_PROXY".to_string(),
                "http://proxy.example:3128".to_string(),
            ),
            ("NO_PROXY".to_string(), "localhost,127.0.0.1".to_string()),
        ]);
        let mut args = Vec::new();
        inject_proxy_env_lookup(&mut args, &HashMap::new(), Some(&access), |name| {
            host.get(name).cloned()
        });

        assert_eq!(
            env_arg_values(&args, "NO_PROXY"),
            ["localhost,127.0.0.1,172.18.0.1:9198"]
        );
        assert_eq!(
            env_arg_values(&args, "no_proxy"),
            ["localhost,127.0.0.1,172.18.0.1:9198"]
        );
        assert_eq!(
            env_arg_values(&args, "HTTP_PROXY"),
            ["http://proxy.example:3128"]
        );
        assert_eq!(env_arg_values(&args, "http_proxy").len(), 1);
    }

    /// A proxy without a bypass list of its own gets none injected: the
    /// official runner's `UpdateWebProxyEnv` shape. (A container that needs
    /// the engine to bypass a proxy must declare a list itself — the runner
    /// does not invent one.)
    #[test]
    fn inject_proxy_env_does_not_invent_a_bypass_list() {
        let access =
            ContainerEngineAccess::for_tests("http://127.0.0.1:9198", "http://172.18.0.1:9198");
        let host = HashMap::from([(
            "HTTPS_PROXY".to_string(),
            "http://proxy.example:3128".to_string(),
        )]);
        let mut args = Vec::new();
        inject_proxy_env_lookup(&mut args, &HashMap::new(), Some(&access), |name| {
            host.get(name).cloned()
        });

        assert_eq!(
            env_arg_values(&args, "HTTPS_PROXY"),
            ["http://proxy.example:3128"]
        );
        assert_eq!(env_arg_values(&args, "https_proxy").len(), 1);
        assert!(env_arg_values(&args, "NO_PROXY").is_empty());
        assert!(env_arg_values(&args, "no_proxy").is_empty());
    }

    /// A bypass list the container declares itself (extended in place by the
    /// caller's translation) is the container's configuration; injecting a
    /// second, narrower list would be the one Docker applies.
    #[test]
    fn inject_proxy_env_leaves_a_container_bypass_list_alone() {
        let access =
            ContainerEngineAccess::for_tests("http://127.0.0.1:9198", "http://172.18.0.1:9198");
        let host = HashMap::from([(
            "HTTP_PROXY".to_string(),
            "http://proxy.example:3128".to_string(),
        )]);
        let user = HashMap::from([("NO_PROXY".to_string(), "corp.example".to_string())]);
        let mut args = Vec::new();
        inject_proxy_env_lookup(&mut args, &user, Some(&access), |name| {
            host.get(name).cloned()
        });

        assert!(env_arg_values(&args, "NO_PROXY").is_empty());
        assert!(env_arg_values(&args, "no_proxy").is_empty());
        assert_eq!(
            env_arg_values(&args, "HTTP_PROXY"),
            ["http://proxy.example:3128"]
        );
    }

    #[test]
    fn translate_engine_origin_is_a_noop_without_a_distinct_origin() {
        let mut env = HashMap::from([(
            "ACTIONS_CACHE_URL".to_string(),
            "http://127.0.0.1:9198/".to_string(),
        )]);

        assert_eq!(
            translate_engine_origin(&mut env, "http://127.0.0.1:9198", "http://127.0.0.1:9198"),
            0
        );
        assert_eq!(
            translate_engine_origin(&mut env, "", "http://172.18.0.1:9198"),
            0
        );
        assert_eq!(env["ACTIONS_CACHE_URL"], "http://127.0.0.1:9198/");
    }

    #[test]
    fn container_engine_access_translates_through_its_own_origins() {
        let mut env = HashMap::from([(
            "ACTIONS_RUNTIME_URL".to_string(),
            "http://127.0.0.1:9198/broker/7/".to_string(),
        )]);
        let access =
            ContainerEngineAccess::for_tests("http://127.0.0.1:9198", "http://172.18.0.1:9198");

        assert_eq!(access.host_origin(), "http://127.0.0.1:9198");
        assert_eq!(access.container_origin(), "http://172.18.0.1:9198");
        assert_eq!(access.translate_env(&mut env), 1);
        assert_eq!(
            env["ACTIONS_RUNTIME_URL"],
            "http://172.18.0.1:9198/broker/7/"
        );
    }

    /// `docker network inspect --format '{{json .IPAM.Config}}'` output: the
    /// first IPv4 gateway wins, an IPv6-only network still resolves (the URL
    /// then carries the bracketed literal), and anything else is unavailable.
    #[test]
    fn parse_network_gateway_prefers_ipv4() {
        let ipv4 = Some(std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 18, 0, 1)));
        assert_eq!(
            parse_network_gateway(r#"[{"Subnet":"172.18.0.0/16","Gateway":"172.18.0.1"}]"#),
            ipv4
        );
        assert_eq!(
            parse_network_gateway(
                r#"[{"Subnet":"fd00::/64","Gateway":"fd00::1"},{"Subnet":"172.19.0.0/16","Gateway":"172.19.0.1"}]"#
            ),
            Some(std::net::IpAddr::V4(std::net::Ipv4Addr::new(172, 19, 0, 1)))
        );
        assert_eq!(
            parse_network_gateway(r#"[{"Subnet":"fd00::/64","Gateway":"fd00::1"}]"#),
            Some("fd00::1".parse().unwrap())
        );
        assert_eq!(parse_network_gateway("[]"), None);
        assert_eq!(
            parse_network_gateway(r#"[{"Subnet":"172.18.0.0/16"}]"#),
            None
        );
        assert_eq!(parse_network_gateway(""), None);
        assert_eq!(parse_network_gateway("not json"), None);
    }

    /// Job and service containers share the job network, so the workflow env
    /// they are created with (`container.env`, `services.<id>.env`) must name
    /// the container-reachable origin too — a workflow that hands a service
    /// the engine URL would otherwise create the same connection-refused the
    /// step environment had. Empty values keep the official inherit form.
    #[test]
    fn append_container_env_rewrites_engine_urls_for_the_container() {
        let env = HashMap::from([
            (
                "ENGINE_URL".to_string(),
                "http://127.0.0.1:9198".to_string(),
            ),
            ("PLAIN".to_string(), "value".to_string()),
            ("INHERIT".to_string(), String::new()),
        ]);
        let access =
            ContainerEngineAccess::for_tests("http://127.0.0.1:9198", "http://172.18.0.1:9198");
        let mut args = Vec::new();
        append_container_env(&mut args, &env, Some(&access));

        assert!(args.contains(&"ENGINE_URL=http://172.18.0.1:9198".to_string()));
        assert!(args.contains(&"PLAIN=value".to_string()));
        assert!(args.contains(&"INHERIT".to_string()));
        assert!(!args.iter().any(|arg| arg.contains("127.0.0.1")));

        // Without container engine access nothing is rewritten.
        let mut args = Vec::new();
        append_container_env(&mut args, &env, None);
        assert!(args.contains(&"ENGINE_URL=http://127.0.0.1:9198".to_string()));
    }
}
