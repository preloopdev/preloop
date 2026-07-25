//! Preloop CI command-line interface.

use aksh_gha_protocol::{ExecutionStatus, NdjsonEvent, RunAccepted, WorkflowSubmission};
use anyhow::Context;
use base64::Engine as _;
use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use preloop_orchestrator::{RunnerPool, RunnerPoolConfig};
use preloop_vm::SmolVmProvider;
use rand::RngCore;
use std::collections::HashSet;
use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;

fn server_url() -> String {
    std::env::var("AKSH_URL").unwrap_or_else(|_| "http://127.0.0.1:9090".to_owned())
}

fn api_token() -> Option<String> {
    std::env::var("AKSH_TOKEN").ok().or_else(|| {
        std::fs::read_to_string(preloop_home().join("engine.token"))
            .ok()
            .map(|token| token.trim().to_owned())
            .filter(|token| !token.is_empty())
    })
}

fn build_client() -> reqwest::Client {
    let builder = reqwest::Client::builder();
    #[cfg(unix)]
    let builder = if std::env::var("AKSH_URL").is_err() {
        builder.unix_socket(preloop_home().join("preloop.sock"))
    } else {
        builder
    };
    builder.build().expect("valid HTTP client configuration")
}

fn preloop_home() -> PathBuf {
    std::env::var_os("PRELOOP_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".preloop")))
        .unwrap_or_else(|| PathBuf::from(".preloop"))
}

#[derive(Debug, Parser)]
#[command(name = "preloop", about = "Local CI with hardware isolation")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Run(RunArgs),

    /// Show the expanded job DAG without executing.
    Plan(PlanArgs),

    /// Show active and recent runs.
    Status,

    Logs(LogsArgs),

    /// Cancel the current run.
    Cancel(CancelArgs),

    /// Manage secrets.
    Secret(SecretArgs),

    /// Open a shell in a preserved VM.
    Shell(ShellArgs),

    /// Internal persistent control plane and local runner pool.
    #[command(hide = true)]
    Engine,
}

#[derive(Debug, Parser)]
struct RunArgs {
    /// Workflow file path. Bare filenames resolve inside .github/workflows/.
    #[arg(short = 'f', long = "file")]
    file: Option<PathBuf>,

    /// Run a single job by its YAML key. Includes the needs: dependency closure.
    #[arg(long)]
    job: Option<String>,

    /// Simulate a trigger event (push, pull_request, merge_group, …).
    #[arg(long)]
    event: Option<String>,

    /// Base ref for pull_request or merge_group events.
    #[arg(long)]
    base: Option<String>,

    /// Keep the failed job VM alive for `preloop shell`.
    #[arg(long)]
    preserve_on_failure: bool,

    /// Inline secret as NAME=VALUE. Repeatable.
    #[arg(long = "secret", value_name = "NAME=VALUE")]
    secrets: Vec<String>,

    /// Submit and return immediately without streaming events.
    #[arg(short = 'd', long)]
    detach: bool,
}

#[derive(Debug, Parser)]
struct PlanArgs {
    /// Workflow file path.
    #[arg(short = 'f', long = "file")]
    file: Option<PathBuf>,

    #[arg(long)]
    json: bool,
}

#[derive(Debug, Parser)]
struct LogsArgs {
    /// Run ID. Defaults to the most recent run.
    run_id: Option<String>,

    /// Filter by job ID.
    #[arg(long)]
    job: Option<String>,

    /// Filter by step number.
    #[arg(long)]
    step: Option<u32>,
}

#[derive(Debug, Parser)]
struct CancelArgs {
    /// Run ID. Defaults to the most recent active run.
    run_id: Option<String>,
}

#[derive(Debug, Parser)]
struct SecretArgs {
    #[command(subcommand)]
    command: SecretCommand,
}

#[derive(Debug, Subcommand)]
enum SecretCommand {
    /// Set a secret (prompts for value).
    Set {
        /// Secret name.
        name: String,
    },

    /// List secret names.
    List,
}

#[derive(Debug, Parser)]
struct ShellArgs {
    /// Run reference (e.g. "last-failed"). Defaults to last failed run.
    run_ref: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    if matches!(cli.command, Command::Engine) {
        return cmd_engine().await;
    }
    ensure_engine_running().await?;

    match cli.command {
        Command::Run(args) => cmd_run(args).await,
        Command::Plan(args) => cmd_plan(args).await,
        Command::Status => cmd_status().await,
        Command::Logs(args) => cmd_logs(args).await,
        Command::Cancel(args) => cmd_cancel(args).await,
        Command::Secret(args) => cmd_secret(args).await,
        Command::Shell(args) => cmd_shell(args).await,
        Command::Engine => unreachable!("engine handled before client startup"),
    }
}

async fn ensure_engine_running() -> anyhow::Result<()> {
    if std::env::var("AKSH_URL").is_ok() {
        return Ok(());
    }

    let client = build_client();
    let url = server_url();

    let mut health_req = client
        .get(format!("{url}/healthz"))
        .timeout(Duration::from_millis(500));
    if let Some(token) = api_token() {
        health_req = health_req.bearer_auth(token);
    }
    if health_req.send().await.is_ok() {
        return Ok(());
    }

    eprintln!("[preloop] Starting local background engine...");

    let preloop_dir = preloop_home();

    let state_dir = preloop_dir.join("state");
    let pid_path = preloop_dir.join("preloop.pid");
    let token_path = preloop_dir.join("engine.token");

    std::fs::create_dir_all(&state_dir)?;
    set_private_directory_permissions(&preloop_dir)?;

    let token = if let Ok(existing) = std::fs::read_to_string(&token_path) {
        existing.trim().to_owned()
    } else {
        let mut bytes = [0_u8; 32];
        rand::thread_rng().fill_bytes(&mut bytes);
        let token = bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        std::fs::write(&token_path, &token)?;
        set_private_file_permissions(&token_path)?;
        token
    };

    let engine_bin = std::env::current_exe().context("resolve preloop executable")?;

    let mut cmd = std::process::Command::new(&engine_bin);
    cmd.arg("engine");
    cmd.env("AKSH_SYSTEM_TOKEN", token);
    cmd.env("PRELOOP_HOME", &preloop_dir);
    if std::env::var_os("RUST_LOG").is_none() {
        cmd.env("RUST_LOG", "info,preloop=debug,aksh=debug");
    }

    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(preloop_dir.join("engine.log"))?;

    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(log_file.try_clone()?);
    cmd.stderr(log_file);

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to spawn engine at {}", engine_bin.display()))?;

    let _ = std::fs::write(&pid_path, child.id().to_string());

    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(status) = child.try_wait()? {
            anyhow::bail!("engine exited before becoming ready: {status}");
        }
        if client
            .get(format!("{url}/healthz"))
            .timeout(Duration::from_millis(300))
            .send()
            .await
            .is_ok()
        {
            eprintln!("[preloop] Engine ready.");
            return Ok(());
        }
    }

    anyhow::bail!("engine auto-boot timed out after 30 seconds");
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &std::path::Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &std::path::Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(path: &std::path::Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &std::path::Path) -> anyhow::Result<()> {
    Ok(())
}

async fn cmd_engine() -> anyhow::Result<()> {
    let home = preloop_home();
    let state_dir = home.join("state");
    let socket = home.join("preloop.sock");
    let listen: std::net::SocketAddr = std::env::var("PRELOOP_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:9090".to_owned())
        .parse()
        .context("PRELOOP_LISTEN must be a socket address")?;
    let public_url = std::env::var("PRELOOP_PUBLIC_URL")
        .unwrap_or_else(|_| format!("http://127.0.0.1:{}", listen.port()));
    std::env::set_var("AKSH_PUBLIC_URL", &public_url);

    // Shared with the runner pool so it can size provisioning to the work
    // actually waiting, not just to whether it has an idle runner left.
    let queue_depth = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut server = tokio::spawn(aksh_runner_server::serve(
        aksh_runner_server::ServerConfig {
            listen,
            unix_socket: Some(socket.clone()),
            queue_depth: Some(queue_depth.clone()),
            state_dir,
            record_flows: None,
            tls: aksh_runner_server::TlsMode::None,
            enable_test_api: false,
            test_api_token: None,
            oidc_issuer: None,
            enable_scheduler: false,
        },
    ));
    // Keep the server in the race so a bind failure surfaces its own error
    // instead of a generic socket-wait timeout.
    tokio::select! {
        result = &mut server => return result?,
        result = wait_for_engine_socket(&socket) => result?,
    }

    let shutdown = tokio_util::sync::CancellationToken::new();
    let mut pool = match local_runner_pool_config(&home, public_url, queue_depth) {
        Ok(config) => {
            let pool_shutdown = shutdown.clone();
            Some(tokio::spawn(async move {
                RunnerPool::new(std::sync::Arc::new(SmolVmProvider::default()), config)?
                    .run(pool_shutdown)
                    .await
            }))
        }
        Err(error) => {
            tracing::warn!(%error, "local runner pool unavailable; control plane remains available");
            None
        }
    };

    if let Some(pool_task) = pool.as_mut() {
        tokio::select! {
            result = &mut server => { result??; return Ok(()); },
            result = pool_task => { result??; return Ok(()); },
            _ = engine_shutdown_signal() => {},
        }
    } else {
        tokio::select! {
            result = &mut server => { result??; return Ok(()); },
            _ = engine_shutdown_signal() => {},
        }
    }
    shutdown.cancel();
    if let Some(pool_task) = pool.as_mut() {
        if tokio::time::timeout(Duration::from_secs(30), &mut *pool_task)
            .await
            .is_err()
        {
            pool_task.abort();
        }
    }
    server.abort();
    let _ = std::fs::remove_file(socket);
    Ok(())
}

async fn engine_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn wait_for_engine_socket(socket: &std::path::Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    let client = reqwest::Client::builder().unix_socket(socket).build()?;
    #[cfg(not(unix))]
    let client = reqwest::Client::new();
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if client
            .get("http://localhost/healthz")
            .timeout(Duration::from_millis(500))
            .send()
            .await
            .is_ok()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("local control plane did not become ready within 30 seconds")
}

fn local_runner_pool_config(
    home: &std::path::Path,
    server_url: String,
    queue_depth: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) -> anyhow::Result<RunnerPoolConfig> {
    let control_bridge = home.join("control-bridge");
    std::fs::create_dir_all(&control_bridge)?;
    set_private_directory_permissions(&control_bridge)?;
    let runner_bundle = std::env::var_os("PRELOOP_RUNNER_BUNDLE")
        .map(PathBuf::from)
        .filter(|path| linux_runner_bundle(path))
        .or_else(|| {
            let exe_dir = std::env::current_exe().ok()?.parent()?.to_path_buf();
            let target_dir = exe_dir.parent()?;
            // Prefer Linux guest binaries when the CLI itself is a macOS
            // development build. The runner executes inside Linux VMs.
            let candidates = [
                target_dir.join("aarch64-unknown-linux-gnu/debug"),
                target_dir.join("aarch64-unknown-linux-musl/debug"),
                target_dir.join("aarch64-unknown-linux-gnu/release"),
                target_dir.join("aarch64-unknown-linux-musl/release"),
                exe_dir.join("preloop-runner"),
                exe_dir.to_path_buf(),
            ];
            candidates
                .into_iter()
                .find(|directory| directory.join("preloop-runner").is_file())
        })
        .filter(|path| linux_runner_bundle(path))
        .context("Linux runner bundle unavailable; run `just build-preloop` to build target/aarch64-unknown-linux-gnu/debug/preloop-runner")?;
    Ok(RunnerPoolConfig {
        size: std::env::var("PRELOOP_RUNNER_POOL_SIZE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| host_runner_pool_size(RUNNER_CPUS)),
        use_fork: std::env::var("PRELOOP_USE_FORK")
            .ok()
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(true),
        name_prefix: "preloop-runner".into(),
        base_image: std::env::var("PRELOOP_RUNNER_BASE_IMAGE")
            .unwrap_or_else(|_| "ubuntu:24.04".into()),
        workspace: None,
        artifact_stem: home.join("vms/preloop-runner-base"),
        runner_bundle,
        runner_binary_name: "preloop-runner".into(),
        server_url,
        control_socket: Some(home.join("preloop.sock")),
        registration_token_env: "AKSH_SYSTEM_TOKEN".into(),
        labels: vec![
            "self-hosted".into(),
            "Linux".into(),
            std::env::consts::ARCH.into(),
        ],
        cpus: RUNNER_CPUS,
        memory_mib: RUNNER_MEMORY_MIB,
        storage_gib: 20,
        debug_dir: Some(home.join("state").join("debug")),
        runner_key_dir: Some(home.join("runner-keys")),
        pending_jobs: Some(queue_depth),
    })
}

/// vCPUs given to each runner VM.
const RUNNER_CPUS: u16 = 4;
/// Memory given to each runner VM, in MiB. SmolVM balloons this, so an idle
/// runner commits far less than its ceiling.
const RUNNER_MEMORY_MIB: u32 = 4096;

/// Resident memory an idle runner VM actually holds, in MiB.
///
/// Measured at ~390 MiB for a forked runner sitting on its long poll, against a
/// 4096 MiB ceiling: SmolVM balloons the guest, so the ceiling says nothing
/// about the cost of keeping one warm.
const IDLE_RUNNER_MIB: u64 = 400;

/// Share of host memory this is willing to hold in idle runners.
const IDLE_MEMORY_SHARE: u64 = 8;

/// Most runners to keep warm, however large the host.
const WARM_POOL_CAP: usize = 8;

/// How many runners to keep warm.
///
/// Two different resources set the bounds, and conflating them under-sized the
/// pool. Running jobs are CPU-bound, so `parallelism / cpus_per_runner` is what
/// the host can execute at once. Warm runners are *idle*, consuming memory and
/// almost no CPU, and their job is to absorb a fan-out without anyone waiting
/// on a VM build — which costs ~500 ms under load and shows up as a cliff the
/// moment a matrix is one job wider than the pool.
///
/// So the warm set is allowed past the CPU budget, bounded by the memory we are
/// willing to leave parked and never below what the host can actually run.
/// Capped at `WARM_POOL_CAP` so a very large machine does not sit on dozens of
/// idle VMs. Set
/// `PRELOOP_RUNNER_POOL_SIZE` to override.
fn host_runner_pool_size(cpus_per_runner: u16) -> usize {
    let parallelism = std::thread::available_parallelism().map_or(2, |value| value.get());
    let by_cpu = (parallelism / usize::from(cpus_per_runner.max(1))).max(1);
    let by_memory = host_memory_mib()
        .map(|total| (total / IDLE_MEMORY_SHARE / IDLE_RUNNER_MIB) as usize)
        .unwrap_or(by_cpu);
    // Not `clamp`: on a host with more CPU budget than the cap its lower bound
    // would exceed its upper bound and panic.
    let target = by_cpu.saturating_mul(2).min(by_memory).min(WARM_POOL_CAP);
    target.max(by_cpu.min(WARM_POOL_CAP)).max(1)
}

/// Total physical memory in MiB, or `None` when it cannot be determined.
#[cfg(target_os = "macos")]
fn host_memory_mib() -> Option<u64> {
    let output = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    let bytes: u64 = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .ok()?;
    Some(bytes / (1024 * 1024))
}

/// Total physical memory in MiB, or `None` when it cannot be determined.
#[cfg(target_os = "linux")]
fn host_memory_mib() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let kib: u64 = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    Some(kib / 1024)
}

/// Total physical memory in MiB, or `None` when it cannot be determined.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn host_memory_mib() -> Option<u64> {
    None
}

fn linux_runner_bundle(path: &std::path::Path) -> bool {
    let binary = path.join("preloop-runner");
    let Ok(mut file) = std::fs::File::open(binary) else {
        return false;
    };
    let mut magic = [0_u8; 4];
    file.read_exact(&mut magic).is_ok() && magic == *b"\x7fELF"
}

async fn cmd_run(args: RunArgs) -> anyhow::Result<()> {
    let workflow_path = resolve_workflow_path(args.file.as_deref())?;
    let workflow_yaml = std::fs::read_to_string(&workflow_path)
        .with_context(|| format!("failed to read workflow: {}", workflow_path.display()))?;
    let event = args.event.as_deref().unwrap_or("push");

    let mut secrets = aksh_gha_protocol::SecretMap::default();
    for secret in &args.secrets {
        if let Some((name, value)) = secret.split_once('=') {
            secrets.insert(
                name.to_owned(),
                aksh_gha_protocol::SecretString::new(value.to_owned()),
            );
        } else {
            anyhow::bail!("invalid --secret format `{secret}`: expected NAME=VALUE");
        }
    }

    let submission = WorkflowSubmission {
        workflow_yaml,
        event: event.to_owned(),
        repository: detect_repository(),
        git_ref: detect_git_ref(),
        workflow_path: Some(workflow_path.display().to_string()),
        local_workspace: None,
        secrets,
        selected_jobs: args.job.into_iter().collect(),
        base_ref: args.base,
        preserve_on_failure: args.preserve_on_failure,
        ..Default::default()
    };

    let client = build_client();
    let url = server_url();
    // Secrets redact on plain serialization; sending them is opt-in.
    let mut request = client
        .post(format!("{url}/api/v1/runs"))
        .json(&submission.to_request_json()?);
    if std::env::var("AKSH_URL").is_err() {
        let workspace = std::fs::canonicalize(".")?;
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(workspace.as_os_str().to_string_lossy().as_bytes());
        request = request.header("x-preloop-local-workspace", encoded);
    }
    if let Some(token) = api_token() {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status}: {body}");
    }

    let accepted: RunAccepted = response.json().await?;
    println!(
        "Run {} created ({} jobs queued)",
        accepted.run_id, accepted.queued_jobs
    );

    let mut final_status = None;
    if args.detach {
        println!(
            "Run {} submitted in detached mode. Use `preloop status` to check",
            accepted.run_id
        );
        return Ok(());
    }
    let mut seen_events = HashSet::new();
    loop {
        let mut events_request = client.get(format!(
            "{url}/api/v1/runs/{}/events.ndjson",
            accepted.run_id
        ));
        if let Some(token) = api_token() {
            events_request = events_request.bearer_auth(token);
        }
        let events_response = events_request.send().await?;
        if !events_response.status().is_success() {
            let status = events_response.status();
            let body = events_response.text().await.unwrap_or_default();
            anyhow::bail!("server returned {status}: {body}");
        }

        let mut stream = events_response.bytes_stream();
        let mut pending = String::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            pending.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(newline) = pending.find('\n') {
                let line = pending[..newline].trim().to_owned();
                pending.drain(..=newline);
                if seen_events.insert(line.clone()) {
                    update_run_status(&mut final_status, render_event(&line));
                }
            }
            if final_status.is_some_and(ExecutionStatus::is_terminal) {
                break;
            }
        }
        if !final_status.is_some_and(ExecutionStatus::is_terminal)
            && !pending.trim().is_empty()
            && seen_events.insert(pending.trim().to_owned())
        {
            update_run_status(&mut final_status, render_event(pending.trim()));
        }

        if final_status.is_some_and(ExecutionStatus::is_terminal) {
            break;
        }
        // The server holds `events.ndjson` open until the run is terminal, so
        // reaching here means the stream dropped early. Retry promptly rather
        // than adding a fixed poll interval to every run's wall clock.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    if let Some(status) = final_status {
        let symbol = if status == ExecutionStatus::Success {
            "✓"
        } else {
            "✗"
        };
        println!("{symbol} Run: {status:?}");
        if status != ExecutionStatus::Success {
            anyhow::bail!("run completed with status {status:?}");
        }
    }

    Ok(())
}

fn update_run_status(current: &mut Option<ExecutionStatus>, next: Option<ExecutionStatus>) {
    if let Some(status) = next {
        if status.is_terminal() || current.is_none() {
            *current = Some(status);
        }
    }
}

fn render_event(line: &str) -> Option<ExecutionStatus> {
    if line.is_empty() {
        return None;
    }
    match serde_json::from_str::<NdjsonEvent>(line) {
        Ok(NdjsonEvent::JobStatus { job_id, status, .. }) => {
            let symbol = match status {
                ExecutionStatus::Success => "✓",
                ExecutionStatus::Failure => "✗",
                ExecutionStatus::Cancelled | ExecutionStatus::Skipped => "⊘",
                ExecutionStatus::InProgress => "⠋",
                ExecutionStatus::Queued | ExecutionStatus::Pending => "○",
            };
            println!("  {symbol} {job_id} ({status:?})");
            None
        }
        Ok(NdjsonEvent::RunStatus { status, .. }) => Some(status),
        Ok(_) | Err(_) => None,
    }
}

fn resolve_workflow_path(file: Option<&std::path::Path>) -> anyhow::Result<PathBuf> {
    match file {
        Some(path) => {
            if path.components().count() == 1 {
                let workflow = PathBuf::from(".github/workflows").join(path);
                if workflow.exists() {
                    return Ok(workflow);
                }
            }
            if path.exists() {
                Ok(path.to_owned())
            } else {
                anyhow::bail!("workflow file not found: {}", path.display())
            }
        }
        None => {
            let dir = PathBuf::from(".github/workflows");
            if !dir.is_dir() {
                anyhow::bail!("no .github/workflows directory found")
            }
            let mut workflows: Vec<PathBuf> = std::fs::read_dir(&dir)?
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| {
                    path.extension()
                        .is_some_and(|extension| extension == "yml" || extension == "yaml")
                })
                .collect();
            workflows.sort();
            match workflows.len() {
                0 => anyhow::bail!("no workflow files found in .github/workflows/"),
                1 => Ok(workflows.into_iter().next().expect("one workflow")),
                _ => {
                    println!("Multiple workflows found:");
                    for (index, workflow) in workflows.iter().enumerate() {
                        println!("  {}: {}", index + 1, workflow.display());
                    }
                    anyhow::bail!("specify a workflow with -f")
                }
            }
        }
    }
}

fn detect_repository() -> String {
    std::process::Command::new("git")
        .args(["remote", "get-url", "origin"])
        .output()
        .ok()
        .and_then(|output| {
            if !output.status.success() {
                return None;
            }
            let remote = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            let remote = remote.strip_suffix(".git").unwrap_or(&remote);
            let remote = remote.rsplit_once(':').map_or(remote, |(_, path)| path);
            let path = remote.rsplit('/').take(2).collect::<Vec<_>>();
            if path.len() != 2 {
                return None;
            }
            Some(path.into_iter().rev().collect::<Vec<_>>().join("/"))
        })
        .unwrap_or_else(|| {
            let dir_name = std::env::current_dir()
                .ok()
                .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()))
                .filter(|n| !n.is_empty())
                .unwrap_or_else(|| "repo".to_owned());
            format!("local/{dir_name}")
        })
}

fn detect_git_ref() -> String {
    if let Ok(output) = std::process::Command::new("git")
        .args(["symbolic-ref", "HEAD"])
        .output()
    {
        if output.status.success() {
            let ref_str = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if !ref_str.is_empty() {
                return ref_str;
            }
        }
    }

    if let Ok(output) = std::process::Command::new("git")
        .args(["describe", "--tags", "--exact-match"])
        .output()
    {
        if output.status.success() {
            let tag = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if !tag.is_empty() {
                return format!("refs/tags/{tag}");
            }
        }
    }

    "refs/heads/main".to_owned()
}

async fn cmd_plan(args: PlanArgs) -> anyhow::Result<()> {
    let workflow = args
        .file
        .as_ref()
        .map_or("all workflows".into(), |p| p.display().to_string());

    println!("preloop plan: {workflow}");
    if args.json {
        println!("  format: json");
    }

    anyhow::bail!("")
}

async fn cmd_status() -> anyhow::Result<()> {
    let client = build_client();
    let url = server_url();
    let mut request = client.get(format!("{url}/api/v1/runs?limit=20"));
    if let Some(token) = api_token() {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status}: {body}");
    }
    let runs: Vec<serde_json::Value> = response.json().await?;
    if runs.is_empty() {
        println!("No runs found.");
        return Ok(());
    }
    println!(
        "{:<38}  {:<6}  {:<12}  {:<12}  WORKFLOW",
        "RUN ID", "#", "STATUS", "EVENT"
    );
    println!("{}", "-".repeat(90));
    for run in &runs {
        let run_id = run["run_id"].as_str().unwrap_or("?");
        let run_number = run
            .get("run_number")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let status = run["status"].as_str().unwrap_or("?");
        let event = run
            .get("event")
            .and_then(serde_json::Value::as_str)
            .or_else(|| {
                run.get("submission")
                    .and_then(|s| s.get("event"))
                    .and_then(serde_json::Value::as_str)
            })
            .unwrap_or("?");
        let workflow = run
            .get("workflow_path")
            .and_then(serde_json::Value::as_str)
            .or_else(|| {
                run.get("workflow_path_str")
                    .and_then(serde_json::Value::as_str)
            })
            .or_else(|| {
                run.get("submission")
                    .and_then(|s| s.get("workflow_path"))
                    .and_then(serde_json::Value::as_str)
            })
            .unwrap_or("?");
        println!(
            "{:<38}  {:<6}  {:<12}  {:<12}  {}",
            run_id, run_number, status, event, workflow
        );
    }
    Ok(())
}

async fn cmd_logs(args: LogsArgs) -> anyhow::Result<()> {
    let client = build_client();
    let url = server_url();
    let run_id = match args.run_id {
        Some(id) => id,
        None => latest_run_id(&client, &url, None)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no runs found"))?,
    };
    let mut request = client.get(format!("{url}/api/v1/runs/{run_id}/logs"));
    if let Some(token) = api_token() {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status}: {body}");
    }
    let body = response.text().await?;
    print!("{body}");
    Ok(())
}

async fn cmd_cancel(args: CancelArgs) -> anyhow::Result<()> {
    let client = build_client();
    let url = server_url();
    let run_id = match args.run_id {
        Some(id) => id,
        None => latest_run_id(&client, &url, Some("in_progress"))
            .await?
            .ok_or_else(|| anyhow::anyhow!("no active runs found"))?,
    };
    let mut request = client.post(format!("{url}/api/v1/runs/{run_id}/cancel"));
    if let Some(token) = api_token() {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status}: {body}");
    }
    println!("Run {run_id} cancelled.");
    Ok(())
}

async fn latest_run_id(
    client: &reqwest::Client,
    url: &str,
    status: Option<&str>,
) -> anyhow::Result<Option<String>> {
    let query = status.map_or_else(
        || "limit=1".to_owned(),
        |status| format!("status={status}&limit=1"),
    );
    let mut request = client.get(format!("{url}/api/v1/runs?{query}"));
    if let Some(token) = api_token() {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status}: {body}");
    }
    let runs: Vec<serde_json::Value> = response.json().await?;
    Ok(runs
        .first()
        .and_then(|run| run["run_id"].as_str())
        .map(str::to_owned))
}

async fn cmd_secret(args: SecretArgs) -> anyhow::Result<()> {
    match args.command {
        SecretCommand::Set { name } => {
            println!("preloop secret set: {name}");
            anyhow::bail!("")
        }
        SecretCommand::List => {
            println!("preloop secret list");
            anyhow::bail!("")
        }
    }
}

async fn cmd_shell(args: ShellArgs) -> anyhow::Result<()> {
    let debug_dir = preloop_home().join("state").join("debug");

    // Find the preserved VM. If a run_ref is given, treat it as a machine
    // name prefix; otherwise pick the first (usually only) marker.
    let machine_name = if let Some(run_ref) = &args.run_ref {
        let path = debug_dir.join(run_ref);
        if path.is_file() {
            run_ref.clone()
        } else {
            // Try matching as a prefix (e.g. "0" → "preloop-runner-0")
            find_debug_machine(&debug_dir, Some(run_ref))?
        }
    } else {
        find_debug_machine(&debug_dir, None)?
    };

    let marker = debug_dir.join(&machine_name);
    eprintln!("[preloop] Connecting to preserved VM: {machine_name}");
    eprintln!("[preloop] Exit the shell to release the VM.");

    // Claim the session before starting so the orchestrator stops counting down.
    let _ = std::fs::write(&marker, preloop_orchestrator::DEBUG_MARKER_ACTIVE);

    // Spawn a background task to touch the marker every 15 seconds,
    // keeping the orchestrator's 10-minute timeout alive.
    let heartbeat_marker = marker.clone();
    let heartbeat = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(15)).await;
            if std::fs::write(&heartbeat_marker, preloop_orchestrator::DEBUG_MARKER_ACTIVE).is_err()
            {
                break;
            }
        }
    });

    // Run smolvm machine shell interactively.
    let status = std::process::Command::new("smolvm")
        .args(["machine", "shell", "--name", &machine_name])
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .context("failed to run smolvm machine shell")?;

    heartbeat.abort();

    // Remove the marker so the orchestrator cleans up the VM.
    let _ = std::fs::remove_file(&marker);
    eprintln!("[preloop] Shell exited — VM will be cleaned up.");

    if !status.success() {
        anyhow::bail!("shell exited with {status}");
    }
    Ok(())
}

fn find_debug_machine(
    debug_dir: &std::path::Path,
    prefix: Option<&String>,
) -> anyhow::Result<String> {
    let entries: Vec<String> = std::fs::read_dir(debug_dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(prefix) = prefix {
                if name.contains(prefix) {
                    Some(name)
                } else {
                    None
                }
            } else {
                Some(name)
            }
        })
        .collect();

    match entries.len() {
        0 => anyhow::bail!(
            "no preserved VMs found. Run with --preserve-on-failure to keep failed job VMs alive"
        ),
        1 => Ok(entries.into_iter().next().unwrap()),
        _ => anyhow::bail!(
            "multiple preserved VMs found: {}. Specify one with: preloop shell <name>",
            entries.join(", ")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::error::ErrorKind;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("preloop").chain(args.iter().copied()))
    }

    #[test]
    fn warm_pool_tracks_host_capacity_within_bounds() {
        let parallelism = std::thread::available_parallelism().map_or(2, |value| value.get());

        // `cpus_per_runner = 0` would divide by zero; it is read as one core.
        for cpus in [0_u16, 1, 2, 4, 64, u16::MAX] {
            let by_cpu = (parallelism / usize::from(cpus.max(1))).max(1);
            let size = host_runner_pool_size(cpus);

            assert!(size >= 1, "a host must always keep a runner warm");
            assert!(size <= WARM_POOL_CAP, "{size} exceeds the warm pool cap");
            assert!(
                size >= by_cpu.min(WARM_POOL_CAP),
                "warm pool {size} starves the {by_cpu} jobs this host can run at once"
            );
            assert!(
                size <= by_cpu.saturating_mul(2).max(1),
                "warm pool {size} parks more than twice the CPU budget of {by_cpu}"
            );
        }
    }

    #[test]
    fn run_no_args() {
        let cli = parse(&["run"]).unwrap();
        let Command::Run(args) = cli.command else {
            panic!("expected Run");
        };
        assert!(args.file.is_none());
        assert!(args.job.is_none());
        assert!(args.event.is_none());
        assert!(args.base.is_none());
        assert!(!args.preserve_on_failure);
        assert!(args.secrets.is_empty());
    }

    #[test]
    fn run_all_flags() {
        let cli = parse(&[
            "run",
            "-f",
            "ci.yml",
            "--job",
            "test",
            "--event",
            "pull_request",
            "--base",
            "main",
            "--preserve-on-failure",
            "--secret",
            "TOKEN=abc",
            "--secret",
            "KEY=xyz",
        ])
        .unwrap();
        let Command::Run(args) = cli.command else {
            panic!("expected Run");
        };
        assert_eq!(args.file.unwrap(), PathBuf::from("ci.yml"));
        assert_eq!(args.job.unwrap(), "test");
        assert_eq!(args.event.unwrap(), "pull_request");
        assert_eq!(args.base.unwrap(), "main");
        assert!(args.preserve_on_failure);
        assert_eq!(args.secrets, vec!["TOKEN=abc", "KEY=xyz"]);
    }

    #[test]
    fn run_detach_aliases_enable_detached_submission() {
        for args in [["run", "-d"], ["run", "--detach"]] {
            let cli = parse(&args).unwrap();
            let Command::Run(args) = cli.command else {
                panic!("expected Run");
            };
            assert!(args.detach, "detach alias should enable detached mode");
        }
    }

    #[test]
    fn run_file_full_path() {
        let cli = parse(&["run", "-f", ".github/workflows/deploy.yml"]).unwrap();
        let Command::Run(args) = cli.command else {
            panic!("expected Run");
        };
        assert_eq!(
            args.file.unwrap(),
            PathBuf::from(".github/workflows/deploy.yml")
        );
    }

    #[test]
    fn run_rejects_unknown_flag() {
        let err = parse(&["run", "--unknown"]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnknownArgument);
    }

    #[test]
    fn run_job_requires_value() {
        let err = parse(&["run", "--job"]).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::InvalidValue | ErrorKind::MissingRequiredArgument
        ));
    }

    // -- plan --

    #[test]
    fn plan_no_args() {
        let cli = parse(&["plan"]).unwrap();
        let Command::Plan(args) = cli.command else {
            panic!("expected Plan");
        };
        assert!(args.file.is_none());
        assert!(!args.json);
    }

    #[test]
    fn plan_json_flag() {
        let cli = parse(&["plan", "-f", "ci.yml", "--json"]).unwrap();
        let Command::Plan(args) = cli.command else {
            panic!("expected Plan");
        };
        assert_eq!(args.file.unwrap(), PathBuf::from("ci.yml"));
        assert!(args.json);
    }

    // -- logs --

    #[test]
    fn logs_filters() {
        let cli = parse(&["logs", "--job", "build", "--step", "3"]).unwrap();
        let Command::Logs(args) = cli.command else {
            panic!("expected Logs");
        };
        assert_eq!(args.job.unwrap(), "build");
        assert_eq!(args.step.unwrap(), 3);
    }

    #[test]
    fn logs_step_rejects_non_numeric() {
        let err = parse(&["logs", "--step", "abc"]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ValueValidation);
    }

    // -- secret --

    #[test]
    fn secret_set() {
        let cli = parse(&["secret", "set", "MY_TOKEN"]).unwrap();
        let Command::Secret(args) = cli.command else {
            panic!("expected Secret");
        };
        let SecretCommand::Set { name } = args.command else {
            panic!("expected Set");
        };
        assert_eq!(name, "MY_TOKEN");
    }

    #[test]
    fn secret_list() {
        let cli = parse(&["secret", "list"]).unwrap();
        let Command::Secret(args) = cli.command else {
            panic!("expected Secret");
        };
        assert!(matches!(args.command, SecretCommand::List));
    }

    #[test]
    fn secret_requires_subcommand() {
        let err = parse(&["secret"]).unwrap_err();
        assert_eq!(
            err.kind(),
            ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        );
    }

    // -- shell --

    #[test]
    fn shell_default_ref() {
        let cli = parse(&["shell"]).unwrap();
        let Command::Shell(args) = cli.command else {
            panic!("expected Shell");
        };
        assert!(args.run_ref.is_none());
    }

    #[test]
    fn shell_explicit_ref() {
        let cli = parse(&["shell", "run-42"]).unwrap();
        let Command::Shell(args) = cli.command else {
            panic!("expected Shell");
        };
        assert_eq!(args.run_ref.unwrap(), "run-42");
    }

    // -- top-level --

    #[test]
    fn no_subcommand_errors() {
        let err = parse(&[]).unwrap_err();
        assert_eq!(
            err.kind(),
            ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        );
    }

    #[test]
    fn status_parses() {
        let cli = parse(&["status"]).unwrap();
        assert!(matches!(cli.command, Command::Status));
    }

    #[test]
    fn cancel_parses() {
        let cli = parse(&["cancel"]).unwrap();
        let Command::Cancel(args) = cli.command else {
            panic!("expected Cancel");
        };
        assert!(args.run_id.is_none());
    }

    #[test]
    fn cancel_accepts_run_id() {
        let cli = parse(&["cancel", "run-42"]).unwrap();
        let Command::Cancel(args) = cli.command else {
            panic!("expected Cancel");
        };
        assert_eq!(args.run_id.as_deref(), Some("run-42"));
    }
}
