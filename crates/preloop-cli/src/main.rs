//! Preloop CI command-line interface.

use aksh_gha_protocol::{ExecutionStatus, NdjsonEvent, RunAccepted, WorkflowSubmission};
use anyhow::Context;
use base64::Engine as _;
use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use preloop_orchestrator::environment::DEFAULT_BASE_IMAGE;
use preloop_orchestrator::{RunnerPool, RunnerPoolConfig};
use preloop_vm::SmolVmProvider;
use rand::RngCore;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;

mod debug_session;
mod github_auth;
mod github_setup;
mod update;

pub(crate) fn server_url() -> String {
    std::env::var("AKSH_URL").unwrap_or_else(|_| "http://127.0.0.1:9090".to_owned())
}

fn should_send_local_workspace_header(url: &str, uses_default_transport: bool) -> bool {
    if uses_default_transport {
        return true;
    }

    // An explicit loopback URL is still a local engine invocation. Do not
    // trust a host path for an arbitrary remote server.
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    let Some(host) = parsed.host_str() else {
        return false;
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn mounted_control_origin(public_url: &str) -> Option<String> {
    let parsed = reqwest::Url::parse(public_url).ok()?;
    let host = parsed.host_str()?;
    let loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    loopback.then(|| public_url.trim_end_matches('/').to_owned())
}

pub(crate) fn api_token() -> Option<String> {
    std::env::var("AKSH_TOKEN")
        .or_else(|_| std::env::var("AKSH_SYSTEM_TOKEN"))
        .ok()
        .or_else(|| {
            std::fs::read_to_string(preloop_home().join("engine.token"))
                .ok()
                .map(|token| token.trim().to_owned())
                .filter(|token| !token.is_empty())
        })
}

pub(crate) fn build_client() -> reqwest::Client {
    let builder = reqwest::Client::builder();
    #[cfg(unix)]
    let builder = if std::env::var("AKSH_URL").is_err() {
        builder.unix_socket(preloop_home().join("preloop-native.sock"))
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
    Status(StatusArgs),

    Logs(LogsArgs),

    /// Cancel the current run.
    Cancel(CancelArgs),

    /// Manage the local secret store.
    Secret(github_setup::SecretArgs),

    /// Configure GitHub credentials (App or fine-grained PAT).
    Setup(github_setup::SetupArgs),

    /// Verify the GitHub credential configuration.
    Doctor(github_setup::DoctorArgs),

    /// Open a shell in a preserved VM.
    Shell(ShellArgs),

    /// Attach to a job paused at a failed step: inspect, fix, retry.
    Debug(debug_session::DebugArgs),

    /// Poll GitHub Releases and atomically install the matching binary.
    Update(update::UpdateArgs),

    /// Run the control plane and microVM runner pool in the foreground.
    ///
    /// This is the self-hosting entry point: it serves the GitHub webhook and
    /// Checks endpoints and provisions a microVM per queued job. Point a
    /// tunnel or reverse proxy at `--listen` to receive events from GitHub.
    Serve(ServeArgs),

    /// Former name for `serve`. Retained because `ensure_engine_running`
    /// spawns it by name, and to keep existing supervisor units working.
    #[command(hide = true)]
    Engine,

    /// Build a fresh packed microVM artifact for release automation.
    #[command(hide = true)]
    BuildGolden(BuildGoldenArgs),
}

#[derive(Debug, Parser)]
struct BuildGoldenArgs {
    /// Directory containing the Linux preloop-runner binary.
    #[arg(long)]
    runner_bundle: PathBuf,

    /// Workspace to detect toolchains from (rust-toolchain.toml, .nvmrc, …).
    /// Defaults to the current directory, so the release-golden workflow —
    /// which runs from the repo checkout — bakes the project's toolchains
    /// into the artifact. Without them every fork of the packed golden
    /// reinstalls rust per job.
    #[arg(long)]
    workspace: Option<PathBuf>,

    /// Destination path for the packed artifact.
    #[arg(long)]
    output: PathBuf,

    /// OCI base image to pack.
    #[arg(long, default_value = DEFAULT_BASE_IMAGE)]
    base_image: String,
}

#[derive(Debug, Default, clap::Args)]
struct ServeArgs {
    /// Address to bind. Overrides PRELOOP_LISTEN.
    #[arg(long, value_name = "ADDR")]
    listen: Option<String>,

    /// Externally reachable base URL. Overrides PRELOOP_PUBLIC_URL.
    ///
    /// Must be the address GitHub and any remote runners can reach — a
    /// loopback URL here is only correct when everything is on this host.
    #[arg(long, value_name = "URL")]
    public_url: Option<String>,

    /// GitHub App id.
    #[arg(long, value_name = "ID")]
    github_app_id: Option<String>,

    /// Path to the GitHub App private key PEM.
    #[arg(long, value_name = "PATH")]
    github_app_key: Option<PathBuf>,

    /// Installation id. Skips installation discovery when supplied.
    #[arg(long, value_name = "ID")]
    github_app_installation_id: Option<u64>,

    /// Shared secret for verifying `X-Hub-Signature-256`.
    #[arg(long, value_name = "SECRET")]
    webhook_secret: Option<String>,

    /// Persist the supplied GitHub credentials so later runs reuse them.
    #[arg(long)]
    save: bool,
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

    /// Event payload JSON file (webhook body) for the simulated trigger.
    #[arg(long, value_name = "PATH")]
    payload: Option<PathBuf>,

    /// Base ref for pull_request or merge_group events.
    #[arg(long)]
    base: Option<String>,

    /// Tear down on failure instead of pausing for debugging.
    ///
    /// Pausing is the default in a terminal: a failed step holds its microVM
    /// open so you can fix and retry from that step. Non-interactive runs
    /// (`--detach`, pipes, CI) never pause, so nothing hangs.
    #[arg(long)]
    no_debug: bool,

    /// Keep the failed job VM alive even when nothing can attach interactively.
    ///
    /// Pausing already implies this for an interactive run. Pass it to hold a
    /// VM open for a later `preloop shell` from a detached or piped run, which
    /// otherwise tears down because there is nobody to answer the pause.
    #[arg(long)]
    preserve_on_failure: bool,

    /// Inline secret as NAME=VALUE. Repeatable.
    #[arg(long = "secret", value_name = "NAME=VALUE")]
    secrets: Vec<String>,

    /// Submit and return immediately without streaming events.
    #[arg(short = 'd', long)]
    detach: bool,

    /// Print the accepted run as JSON (`run_id`, `queued_jobs`) and exit.
    ///
    /// With `--detach` this is the whole output; without it the run still
    /// executes and the summary line is JSON. Machines want this. And a
    /// cancelled run keeps its final summary, so polling for the run to leave
    /// `queued` works regardless of how the human interrupted it.
    #[arg(long)]
    json: bool,
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

    /// Print the cancelled run as JSON (`run_id`, `status`) and exit.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Parser)]
struct StatusArgs {
    /// Print the run list as JSON (the same records `status` renders as a
    /// table) and exit. For agents and scripts.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Parser)]
struct ShellArgs {
    /// Run reference (e.g. "last-failed"). Defaults to last failed run.
    run_ref: Option<String>,
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    // `fmt::init()` alone filters to ERROR when `RUST_LOG` is unset, which hid
    // a runner pool that failed to provision 77 times in a row: every
    // provisioning fault logs at `warn` or `info`, so the operator saw a server
    // that accepted webhooks and silently never ran anything. Default to `info`
    // and let `RUST_LOG` override as usual.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {error:#}");
            // Distinct failure classes so scripts and agents can branch on
            // why a command failed: 3 = the control plane was unreachable or
            // rejected the request, 4 = local file/argument validation,
            // 1 = everything else (including a run that completed failing).
            if error.downcast_ref::<UpstreamError>().is_some() {
                std::process::ExitCode::from(3)
            } else if error.downcast_ref::<LocalError>().is_some() {
                std::process::ExitCode::from(4)
            } else {
                std::process::ExitCode::from(1)
            }
        }
    }
}

/// Marker for failures talking to the control plane (connection, 4xx/5xx,
/// malformed response). Maps to exit code 3.
///
/// The marker stores the formatted chain rather than the error itself: a
/// wrapper that delegates `Display` to its inner error would print the same
/// message twice in the top-level `{error:#}` chain.
#[derive(Debug)]
pub(crate) struct UpstreamError {
    message: String,
}

impl UpstreamError {
    fn wrap(error: anyhow::Error) -> anyhow::Error {
        anyhow::Error::new(Self {
            message: format!("{error:#}"),
        })
    }
}

impl std::fmt::Display for UpstreamError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for UpstreamError {}

/// Marker for local failures (missing workflow file, unparsable payload,
/// invalid flags). Maps to exit code 4.
#[derive(Debug)]
pub(crate) struct LocalError {
    message: String,
}

impl LocalError {
    fn wrap(error: anyhow::Error) -> anyhow::Error {
        anyhow::Error::new(Self {
            message: format!("{error:#}"),
        })
    }
}

impl std::fmt::Display for LocalError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for LocalError {}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // One config path for the whole process. `setup`/`doctor`/`secret` return
    // before `cmd_engine` runs, so pinning this only inside the engine let a
    // custom PRELOOP_HOME split the CLI's writes ($HOME/.preloop/config.toml)
    // from the engine's reads ($PRELOOP_HOME/config.toml) — setup silently had
    // no effect. An explicit operator override still wins.
    if std::env::var_os(aksh_runner_server::config::CONFIG_PATH_ENV).is_none() {
        std::env::set_var(
            aksh_runner_server::config::CONFIG_PATH_ENV,
            github_setup::config_path_for_home(),
        );
    }
    // Both run the daemon in this process, so neither may bootstrap another
    // one underneath itself.
    match cli.command {
        Command::Serve(args) => return cmd_engine(args).await,
        Command::Engine => return cmd_engine(ServeArgs::default()).await,
        Command::BuildGolden(args) => return cmd_build_golden(args).await,
        Command::Update(args) => return update::run(args).await,
        // Local configuration commands must not spawn the engine.
        Command::Setup(args) => return github_setup::cmd_setup(args).await,
        Command::Doctor(args) => return github_setup::cmd_doctor(args).await,
        Command::Secret(args) => return github_setup::cmd_secret(args).await,
        _ => {}
    }
    ensure_engine_running().await?;

    match cli.command {
        Command::Run(args) => cmd_run(args).await,
        Command::Plan(args) => cmd_plan(args).await,
        Command::Status(args) => cmd_status(args).await,
        Command::Logs(args) => cmd_logs(args).await,
        Command::Cancel(args) => cmd_cancel(args).await,
        Command::Shell(args) => cmd_shell(args).await,
        Command::Debug(args) => {
            debug_session::run(args, build_client(), server_url(), api_token()).await
        }
        Command::Update(_)
        | Command::Serve(_)
        | Command::Engine
        | Command::BuildGolden(_)
        | Command::Setup(_)
        | Command::Doctor(_)
        | Command::Secret(_) => {
            unreachable!("daemon commands handled before client startup")
        }
    }
}

fn systemd_socket_activation_requested() -> bool {
    cfg!(target_os = "linux") && std::env::var_os("LISTEN_FDS").is_some()
}

async fn cmd_build_golden(args: BuildGoldenArgs) -> anyhow::Result<()> {
    const TOKEN_ENV: &str = "PRELOOP_GOLDEN_BUILD_TOKEN";
    std::env::set_var(TOKEN_ENV, "artifact-build-only");
    let runner_bundle = std::fs::canonicalize(&args.runner_bundle).with_context(|| {
        format!(
            "runner bundle does not exist: {}",
            args.runner_bundle.display()
        )
    })?;
    let output = if args.output.is_absolute() {
        args.output
    } else {
        std::env::current_dir()?.join(args.output)
    };
    let config = RunnerPoolConfig {
        size: 1,
        use_fork: false,
        use_packed_artifact: false,
        name_prefix: "preloop-release-golden".into(),
        base_image: args.base_image,
        workspace: args.workspace.or_else(|| std::env::current_dir().ok()),
        artifact_stem: output.clone(),
        runner_bundle,
        externals_dir: std::env::var_os("PRELOOP_RUNNER_EXTERNALS")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("preloop-externals")),
        runner_binary_name: "preloop-runner".into(),
        server_url: "http://127.0.0.1:1".into(),
        control_origin: None,
        control_socket: None,
        control_upstream: None,
        dns: std::env::var("PRELOOP_RUNNER_DNS").ok(),
        registration_token_env: TOKEN_ENV.into(),
        labels: vec![
            "self-hosted".into(),
            "Linux".into(),
            std::env::consts::ARCH.into(),
        ],
        cpus: RUNNER_CPUS,
        memory_mib: RUNNER_MEMORY_MIB,
        storage_gib: 20,
        overlay_gib: std::env::var("PRELOOP_RUNNER_OVERLAY_GB")
            .ok()
            .and_then(|v| v.parse().ok()),
        debug_dir: None,
        runner_key_dir: None,
        pending_jobs: None,
        preload_images: Vec::new(),
        next_job_runs_on: None,
        pending_registrations: None,
    };
    RunnerPool::new(std::sync::Arc::new(SmolVmProvider::default()), config)?
        .rebuild_artifact()
        .await?;
    anyhow::ensure!(
        output.is_file(),
        "golden build did not create {}",
        output.display()
    );
    println!("{}", output.display());
    Ok(())
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
        write_private_file(&token_path, token.as_bytes())?;
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

fn write_private_file(path: &std::path::Path, contents: &[u8]) -> anyhow::Result<()> {
    use std::io::Write as _;

    let mut options = std::fs::OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("create private file {}", path.display()))?;
    set_private_file_permissions(path)?;
    file.write_all(contents)
        .with_context(|| format!("write private file {}", path.display()))?;
    Ok(())
}

fn prepare_engine_token(
    home: &std::path::Path,
    configured: Option<String>,
) -> anyhow::Result<String> {
    std::fs::create_dir_all(home)
        .with_context(|| format!("create PRELOOP_HOME {}", home.display()))?;
    set_private_directory_permissions(home)?;

    let token_path = home.join("engine.token");
    if let Some(token) = configured {
        anyhow::ensure!(!token.trim().is_empty(), "AKSH_SYSTEM_TOKEN is empty");
        write_private_file(&token_path, token.as_bytes())?;
        return Ok(token);
    }
    if let Ok(existing) = std::fs::read_to_string(&token_path) {
        let token = existing.trim().to_owned();
        anyhow::ensure!(!token.is_empty(), "{} is empty", token_path.display());
        set_private_file_permissions(&token_path)?;
        return Ok(token);
    }

    let mut bytes = [0_u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    let token = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    write_private_file(&token_path, token.as_bytes())?;
    Ok(token)
}

/// Merge stored and command-line GitHub credentials into the environment the
/// server reads, optionally persisting them, and report the effective state.
///
/// Precedence, widest to narrowest: existing environment, then `--flags`, then
/// the stored file. The environment stays authoritative so a container that
/// injects secrets is never overridden by a file left behind by `--save`.
fn resolve_github_auth(args: &ServeArgs, state_dir: &std::path::Path) -> anyhow::Result<()> {
    let mut auth = github_auth::StoredAuth::load(state_dir)?;

    let private_key_pem = args
        .github_app_key
        .as_ref()
        .map(|path| {
            std::fs::read_to_string(path)
                .with_context(|| format!("reading GitHub App key {}", path.display()))
        })
        .transpose()?;

    let from_flags = github_auth::StoredAuth {
        app_id: args.github_app_id.clone(),
        installation_id: args.github_app_installation_id,
        private_key_pem,
        webhook_secret: args.webhook_secret.clone(),
    };
    let supplied = from_flags != github_auth::StoredAuth::default();
    auth.overlay(from_flags);

    if args.save {
        if !supplied {
            anyhow::bail!("--save needs at least one GitHub credential flag to save");
        }
        let path = auth.save(state_dir)?;
        eprintln!("[preloop] saved GitHub credentials to {}", path.display());
    }

    auth.apply();
    eprintln!("[preloop] {}", github_auth::StoredAuth::report());
    Ok(())
}

async fn cmd_engine(args: ServeArgs) -> anyhow::Result<()> {
    let home = preloop_home();
    let state_dir = home.join("state");
    let socket = home.join("preloop.sock");

    // Ensure AKSH_SYSTEM_TOKEN and engine.token stay synchronized.
    let token = prepare_engine_token(&home, std::env::var("AKSH_SYSTEM_TOKEN").ok())?;
    std::env::set_var("AKSH_SYSTEM_TOKEN", &token);
    let listen: std::net::SocketAddr = args
        .listen
        .clone()
        .or_else(|| std::env::var("PRELOOP_LISTEN").ok())
        .unwrap_or_else(|| "0.0.0.0:9090".to_owned())
        .parse()
        .context("--listen / PRELOOP_LISTEN must be a socket address")?;
    let public_url = args
        .public_url
        .clone()
        .or_else(|| std::env::var("PRELOOP_PUBLIC_URL").ok())
        .unwrap_or_else(|| format!("http://127.0.0.1:{}", listen.port()));
    std::env::set_var("AKSH_PUBLIC_URL", &public_url);

    // Runner-facing origin is always the loopback listen address: in-VM
    // runners reach it over the mounted control socket, and their job-side
    // programs through the in-guest loopback bridge — never over the public
    // network. The public URL stays strictly GitHub-facing (check-run
    // details links). Runners on other machines need AKSH_RUNNER_URL to
    // point at a host-reachable address instead.
    // When the control upstream is set, the VM reaches the server over
    // virtio-net TCP at that address. `AKSH_RUNNER_URL` stays loopback so
    // the server advertises loopback URLs in job messages — the in-guest
    // bridge makes them work.
    let control_upstream = std::env::var("PRELOOP_CONTROL_UPSTREAM").ok();
    if std::env::var("AKSH_RUNNER_URL").is_err() {
        std::env::set_var(
            "AKSH_RUNNER_URL",
            format!("http://127.0.0.1:{}", listen.port()),
        );
    }
    let runner_url = std::env::var("AKSH_RUNNER_URL").unwrap();
    let control_origin = mounted_control_origin(&runner_url);

    // Resolve GitHub credentials before `AppState::new` reads the environment.
    // Both `github_app::load_from_env` and the webhook-secret lookup happen
    // inside `serve`, so anything published after that call is ignored.
    resolve_github_auth(&args, &state_dir)?;

    // Shared with the runner pool so it can size provisioning to the work
    // actually waiting, not just to whether it has an idle runner left.
    let queue_depth = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let next_job_runs_on = std::sync::Arc::new(std::sync::RwLock::new(Vec::new()));
    // Shared one-time provision-token map. The pool writes a token for every
    // machine it provisions and forwards it into the guest's `configure`;
    // the control plane trusts only registrations presenting a match, which
    // is what authorizes job → runner assignment binding. Present whenever
    // the pool can provision.
    let pending_registrations =
        std::sync::Arc::new(std::sync::RwLock::new(std::collections::BTreeMap::<
            String,
            std::time::SystemTime,
        >::new()));
    let pool_enabled = env_flag("PRELOOP_RUNNER_POOL_ENABLED", false);
    let pool_config = local_runner_pool_config(
        &home,
        runner_url.clone(),
        control_origin.clone(),
        control_upstream.clone(),
        queue_depth.clone(),
        next_job_runs_on.clone(),
        pool_enabled,
        pending_registrations.clone(),
    );
    let pool_available = match &pool_config {
        Ok(_) => true,
        Err(error) => {
            tracing::warn!(%error, "local runner provisioning unavailable; control plane remains available");
            false
        }
    };
    let mut server = tokio::spawn(aksh_runner_server::serve(
        aksh_runner_server::ServerConfig {
            listen,
            systemd_socket_activation: systemd_socket_activation_requested(),
            unix_socket: Some(socket.clone()),
            // The CLI's default transport (AKSH_URL unset) talks to the engine
            // over this socket. It must carry the full native surface: the
            // runner socket is VM-mounted and guarded to the runner protocol,
            // so a CLI pointed at it would see nothing but 404s.
            host_socket: Some(home.join("preloop-native.sock")),
            queue_depth: Some(queue_depth.clone()),
            next_job_runs_on: Some(next_job_runs_on.clone()),
            pending_registrations: pool_available.then_some(pending_registrations),
            require_job_assignments: env_flag("PRELOOP_REQUIRE_JOB_ASSIGNMENTS", false),
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
    let mut pool = match pool_config {
        Ok(config) => {
            if !pool_enabled {
                tracing::info!("local runner pool disabled; provisioning one VM per queued job");
            }
            let pool_shutdown = shutdown.clone();
            Some(tokio::spawn(async move {
                RunnerPool::new(std::sync::Arc::new(SmolVmProvider::default()), config)?
                    .run(pool_shutdown)
                    .await
            }))
        }
        Err(_) => None,
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

// Configuration assembly, not a public API: the parameter list mirrors the
// inputs the pool genuinely needs.
#[allow(clippy::too_many_arguments)]
fn local_runner_pool_config(
    home: &std::path::Path,
    server_url: String,
    control_origin: Option<String>,
    control_upstream: Option<String>,
    queue_depth: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    next_job_runs_on: std::sync::Arc<std::sync::RwLock<Vec<String>>>,
    pool_enabled: bool,
    pending_registrations: std::sync::Arc<
        std::sync::RwLock<std::collections::BTreeMap<String, std::time::SystemTime>>,
    >,
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
    let use_packed_artifact = env_flag("PRELOOP_USE_PACKED_GOLDEN", false);
    // The workspace is scanned for toolchain version files (rust-toolchain.toml,
    // .nvmrc, etc.) so the golden can be built with the project's toolchains
    // pre-installed instead of installing them per job. PRELOOP_WORKSPACE
    // overrides the current directory for daemon-style deployments.
    let workspace = std::env::var_os("PRELOOP_WORKSPACE")
        .map(PathBuf::from)
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."));
    // The mounted control socket only makes sense when the runner URL is
    // loopback (the VM reaches the host through the socket relay). With
    // `AKSH_RUNNER_URL` pointing at a host-reachable LAN address — the
    // "runners on other machines" mode — there is no relay, and the runner
    // talks to the control plane over plain TCP instead. The orchestrator
    // gates both the socket mount and the guest env on this field, so leaving
    // it `None` is what switches the transport.
    let control_socket = control_origin.as_ref().map(|_| home.join("preloop.sock"));
    // When a TCP upstream is configured, skip the socket mount — the
    // guest control bridge will forward via TCP instead of vsock.
    let control_socket = if control_upstream.is_some() {
        None
    } else {
        control_socket
    };
    let base_image = std::env::var("PRELOOP_RUNNER_BASE_IMAGE")
        .unwrap_or_else(|_| preloop_orchestrator::environment::DEFAULT_BASE_IMAGE.into());
    // A custom base image (`.smolmachine` artifact or any non-stock OCI
    // reference) serves every queued job itself, so environment-based runner
    // replacement has nothing to switch to: the job's implied stock base
    // (`ubuntu:24.04` from `ubuntu-latest` labels) will always differ from
    // the configured image and idle runners would be replaced forever.
    // Compare on the plain `repository:tag`, so the digest-pinned defaults
    // (ubuntu:24.04@sha256:…) still count as stock Ubuntu images.
    let custom_base = !matches!(
        preloop_orchestrator::environment::base_name(&base_image),
        "ubuntu:24.04" | "ubuntu:22.04"
    );
    Ok(RunnerPoolConfig {
        // Size zero is the deliberate low-memory mode: keep the local
        // supervisor alive, but build a runner only when a job is queued.
        size: if pool_enabled {
            std::env::var("PRELOOP_RUNNER_POOL_SIZE")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or_else(|| host_runner_pool_size(RUNNER_CPUS))
        } else {
            0
        },
        // Forking is safe only when the warm pool has a prepared packed
        // artifact. A live OCI golden does not carry post-create filesystem
        // mutations into SmolVM forks.
        use_packed_artifact,
        use_fork: pool_enabled && use_packed_artifact && env_flag("PRELOOP_USE_FORK", true),
        // Multiple engines on one host must not share a namespace: smolvm
        // keys machines and persistent overlays by name, and cross-engine
        // reuse boots a runner whose persisted state points at the other
        // engine's control plane.
        name_prefix: std::env::var("PRELOOP_RUNNER_NAME_PREFIX")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "preloop-runner".into()),
        base_image: base_image.clone(),
        workspace: Some(workspace),
        // The packed artifact cache key includes the resolved base image
        // (tag AND digest): the digest-pinned defaults are a golden's
        // provenance. When the pin moves, a stale packed golden baked from
        // the old digest must not be reused. Custom `.smolmachine` bases are
        // filesystem paths, so separators are normalized out of the key.
        artifact_stem: home.join("vms").join(format!(
            "preloop-{}-{}",
            base_image.replace(['/', ':', '@'], "-"),
            std::env::consts::ARCH
        )),
        runner_bundle,
        // Node externals shared with every VM via a read-only mount. The
        // operator may point this anywhere; the default lives next to the
        // control-bridge state.
        externals_dir: std::env::var_os("PRELOOP_RUNNER_EXTERNALS")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join("externals")),
        runner_binary_name: "preloop-runner".to_owned(),
        server_url,
        control_origin,
        control_socket,
        control_upstream,
        dns: std::env::var("PRELOOP_RUNNER_DNS").ok(),
        registration_token_env: "AKSH_SYSTEM_TOKEN".into(),
        labels: runner_pool_labels(),
        cpus: RUNNER_CPUS,
        memory_mib: RUNNER_MEMORY_MIB,
        storage_gib: 20,
        overlay_gib: std::env::var("PRELOOP_RUNNER_OVERLAY_GB")
            .ok()
            .and_then(|v| v.parse().ok()),
        debug_dir: Some(home.join("state").join("debug")),
        runner_key_dir: Some(home.join("runner-keys")),
        // Warm the golden with the images this project's workflows declare,
        // so `container:`/`services:` jobs do not re-pull on every run.
        preload_images: preloop_orchestrator::environment::scan_workflow_images(
            &std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        ),
        pending_jobs: Some(queue_depth),
        next_job_runs_on: (!custom_base).then_some(next_job_runs_on),
        pending_registrations: Some(pending_registrations),
    })
}

/// Base runner labels plus any extras from `PRELOOP_RUNNER_LABELS`.
///
/// The scheduler matches `runs-on` labels against what a runner declares, so
/// a non-x86 host can still volunteer for X64-pinned workflows by declaring
/// the extra label explicitly. Comma-separated; empty entries are ignored.
fn runner_pool_labels() -> Vec<String> {
    let mut labels = vec![
        "self-hosted".to_owned(),
        "Linux".to_owned(),
        std::env::consts::ARCH.to_owned(),
    ];
    if let Ok(extra) = std::env::var("PRELOOP_RUNNER_LABELS") {
        for label in extra.split(',').map(str::trim).filter(|l| !l.is_empty()) {
            if !labels
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(label))
            {
                labels.push(label.to_owned());
            }
        }
    }
    labels
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

fn env_flag(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|value| parse_flag(&value))
        .unwrap_or(default)
}

fn parse_flag(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// What the operator is most likely to have wrong when the control plane is
/// unreachable, appended to connection errors. The engine's unix socket is the
/// default transport; a TCP target needs a token.
fn upstream_hint(url: &str) -> String {
    let mut hint = String::from("start the engine with `preloop serve`");
    if !url.starts_with("http://127.0.0.1") && !url.starts_with("http://localhost") {
        hint.push_str(", and point AKSH_URL at it");
    }
    if api_token().is_none() {
        hint.push_str(", and set AKSH_SYSTEM_TOKEN to the engine's token");
    }
    hint
}

/// Map an HTTP status into an actionable error: a 401 is almost always a
/// missing or mismatched token, a 404 on a known endpoint means the server is
/// not running the same binary as this CLI (its native API is served from the
/// unix socket, not the runner-facing TCP surface).
fn upstream_error_with_hint(status: reqwest::StatusCode, message: String) -> anyhow::Error {
    let mut detail = message;
    if status == reqwest::StatusCode::UNAUTHORIZED {
        detail.push_str(
            "\ncheck AKSH_SYSTEM_TOKEN (or `preloop secret`/config.toml): the engine \
             rejected the token this CLI presented",
        );
    } else if status == reqwest::StatusCode::NOT_FOUND {
        detail.push_str(
            "\nthis endpoint is not served by that listener — the engine serves the \
             native API on its unix socket (AKSH_URL unset), and the TCP listener \
             only serves the runner protocol",
        );
    }
    UpstreamError::wrap(anyhow::anyhow!("{detail}"))
}

/// Send a request to the control plane and turn connection and HTTP failures
/// into `UpstreamError`s with a hint about the likely cause.
async fn send_api(
    request: reqwest::RequestBuilder,
    endpoint: &str,
) -> anyhow::Result<reqwest::Response> {
    let response = request.send().await.map_err(|error| {
        UpstreamError::wrap(anyhow::anyhow!(
            "failed to reach the preloop control plane ({endpoint}): {error}\n{}",
            upstream_hint(endpoint)
        ))
    })?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let message = if body.trim().is_empty() {
            format!("server returned {status} for {endpoint}")
        } else {
            format!("server returned {status} for {endpoint}: {body}")
        };
        return Err(upstream_error_with_hint(status, message));
    }
    Ok(response)
}

async fn cmd_run(args: RunArgs) -> anyhow::Result<()> {
    let workflow_path = resolve_workflow_path(args.file.as_deref()).map_err(LocalError::wrap)?;
    let workflow_yaml = std::fs::read_to_string(&workflow_path).map_err(|error| {
        LocalError::wrap(anyhow::Error::new(error)).context(format!(
            "failed to read workflow: {}",
            workflow_path.display()
        ))
    })?;
    let event = args.event.as_deref().unwrap_or("push");

    // Hand local reusable workflows (`uses: ./.github/workflows/…`) to the
    // server the same way the native client does: everything under the
    // workspace `.github/workflows/`, keyed repository-relative, minus the
    // submitted workflow itself.
    let mut reusable_workflows = BTreeMap::new();
    if let Ok(current_dir) = std::env::current_dir() {
        let workflows_dir = current_dir.join(".github").join("workflows");
        if let Ok(entries) = std::fs::read_dir(&workflows_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if !matches!(
                    path.extension().and_then(|ext| ext.to_str()),
                    Some("yml" | "yaml")
                ) || same_file_path(&path, &workflow_path)
                {
                    continue;
                }
                let Some(relative) = path.strip_prefix(&current_dir).ok() else {
                    continue;
                };
                let yaml = std::fs::read_to_string(&path)
                    .with_context(|| format!("read reusable workflow {}", path.display()))?;
                reusable_workflows.insert(relative.to_string_lossy().into_owned(), yaml);
            }
        }
    }

    let payload = match args.payload.as_deref() {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("failed to read payload: {}", path.display()))?;
            serde_json::from_str(&text).map_err(|error| {
                LocalError::wrap(anyhow::anyhow!(
                    "parse payload {}: not valid JSON ({error})",
                    path.display()
                ))
            })?
        }
        None => serde_json::json!({}),
    };

    let mut secrets = aksh_gha_protocol::SecretMap::default();
    for secret in &args.secrets {
        if let Some((name, value)) = secret.split_once('=') {
            secrets.insert(
                name.to_owned(),
                aksh_gha_protocol::SecretString::new(value.to_owned()),
            );
        } else {
            return Err(LocalError::wrap(anyhow::anyhow!(
                "invalid --secret format `{secret}`: expected NAME=VALUE"
            )));
        }
    }

    let submission = WorkflowSubmission {
        workflow_yaml,
        event: event.to_owned(),
        payload,
        repository: detect_repository(),
        git_ref: detect_git_ref(),
        workflow_path: Some(workflow_path.display().to_string()),
        local_workspace: None,
        secrets,
        reusable_workflows,
        selected_jobs: args.job.into_iter().collect(),
        base_ref: args.base,
        // On by default where it can be acted on: a paused job blocks until a
        // controller answers, so pausing a piped or detached run would hang
        // something with no way to respond. `--preserve-on-failure` is the
        // escape hatch for exactly that case — hold the VM for a later
        // `preloop shell` without anyone attached now.
        preserve_on_failure: !args.no_debug
            && (args.preserve_on_failure
                || (!args.detach && std::io::IsTerminal::is_terminal(&std::io::stdin()))),
        ..Default::default()
    };

    let client = build_client();
    let url = server_url();
    // Secrets redact on plain serialization; sending them is opt-in.
    let mut request = client
        .post(format!("{url}/api/v1/runs"))
        .json(&submission.to_request_json()?);
    if should_send_local_workspace_header(&url, std::env::var("AKSH_URL").is_err()) {
        let workspace = std::fs::canonicalize(".")?;
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(workspace.as_os_str().to_string_lossy().as_bytes());
        request = request.header("x-preloop-local-workspace", encoded);
    }
    if let Some(token) = api_token() {
        request = request.bearer_auth(token);
    }
    let response = send_api(request, "/api/v1/runs").await?;
    let accepted: RunAccepted = response.json().await.map_err(|error| {
        UpstreamError::wrap(anyhow::Error::new(error))
            .context("control plane returned an unparsable run-acceptance response")
    })?;

    if args.detach {
        if args.json {
            println!(
                "{}",
                serde_json::json!({
                    "run_id": accepted.run_id,
                    "queued_jobs": accepted.queued_jobs,
                    "status": "queued",
                })
            );
        } else {
            println!(
                "Run {} submitted in detached mode. Use `preloop status` to check",
                accepted.run_id
            );
        }
        return Ok(());
    }
    if !args.json {
        println!(
            "Run {} created ({} jobs queued)",
            accepted.run_id, accepted.queued_jobs
        );
    }

    let mut final_status = None;
    let mut interrupted = false;
    // A run that stays `queued` is often a provisioning failure the CLI could
    // be silently sitting on (the engine logs it, the CLI sees nothing). Say
    // something after a quiet spell instead of hanging forever. The hint is
    // suppressed once a job actually starts (an InProgress event) or the run
    // goes terminal — either way "no runner picked it up" stops being true.
    let mut job_started = false;
    let mut stall_since: Option<std::time::Instant> = None;
    let mut stalled_printed = false;
    let mut seen_events = HashSet::new();
    loop {
        let mut events_request = client.get(format!(
            "{url}/api/v1/runs/{}/events.ndjson",
            accepted.run_id
        ));
        if let Some(token) = api_token() {
            events_request = events_request.bearer_auth(token);
        }
        let events_response = send_api(
            events_request,
            &format!("/api/v1/runs/{}/events.ndjson", accepted.run_id),
        )
        .await?;

        let mut stream = events_response.bytes_stream();
        let mut pending = String::new();
        let mut paused: Option<aksh_gha_protocol::debug_session::DebugSession> = None;
        loop {
            // The server holds this stream open until the run is terminal, and
            // a job paused at a failed step never gets there. Watching only the
            // stream would sit silently forever with the answer one poll away,
            // so race the next chunk against a check for a paused session.
            let chunk = tokio::select! {
                biased;
                chunk = stream.next() => match chunk {
                    Some(chunk) => chunk?,
                    None => break,
                },
                () = tokio::time::sleep(Duration::from_millis(750)) => {
                    paused = debug_session::paused_for_run(
                        &client,
                        &url,
                        api_token(),
                        accepted.run_id,
                    )
                    .await;
                    if paused.is_some() {
                        break;
                    }
                    // Nothing arrived and nothing is paused: the run is still
                    // queued. Eventually say so.
                    stall_since.get_or_insert_with(std::time::Instant::now);
                    if !stalled_printed
                        && !job_started
                        && !final_status.is_some_and(ExecutionStatus::is_terminal)
                        && stall_since.is_some_and(|since| {
                            since.elapsed() > Duration::from_secs(20)
                        })
                    {
                        stalled_printed = true;
                        eprintln!(
                            "run {} is still queued — no runner has picked up a job. \
                             `preloop status` shows the queue; check the `preloop serve` \
                             logs for provisioning failures, or register a runner \
                             (`preloop-runner configure` then `preloop-runner run`).",
                            accepted.run_id
                        );
                    }
                    continue;
                }
                _ = tokio::signal::ctrl_c() => {
                    interrupted = true;
                    break;
                }
            };
            pending.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(newline) = pending.find('\n') {
                let line = pending[..newline].trim().to_owned();
                pending.drain(..=newline);
                if seen_events.insert(line.clone()) {
                    if matches!(
                        serde_json::from_str::<NdjsonEvent>(&line),
                        Ok(NdjsonEvent::JobStatus {
                            status: ExecutionStatus::InProgress,
                            ..
                        })
                    ) {
                        job_started = true;
                    }
                    update_run_status(&mut final_status, render_event(&line));
                }
            }
            if final_status.is_some_and(ExecutionStatus::is_terminal) {
                break;
            }
        }

        if interrupted {
            break;
        }

        if let Some(session) = paused {
            match debug_session::prompt_at_failure(&client, &url, api_token(), session).await {
                // Resumed: reconnect and keep reporting the run.
                Ok(true) => continue,
                Ok(false) => break,
                Err(error) => {
                    eprintln!("debug session error: {error:#}");
                    break;
                }
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

    if interrupted {
        // The run is still executing on the server; the CLI is the only thing
        // that died. Say so in a way a terminal user actually reads, and in a
        // way that does not look like a crash to a script.
        if !args.json {
            eprintln!(
                "interrupted: run {} continues in the background. \
                 `preloop status` to follow, `preloop cancel {}` to stop it.",
                accepted.run_id, accepted.run_id
            );
        }
        std::process::exit(130);
    }

    if let Some(status) = final_status {
        if args.json {
            println!(
                "{}",
                serde_json::json!({
                    "run_id": accepted.run_id,
                    "status": serde_json::to_value(status)?,
                })
            );
        } else {
            let symbol = if status == ExecutionStatus::Success {
                "✓"
            } else {
                "✗"
            };
            println!("{symbol} Run: {status:?}");
        }
        if status != ExecutionStatus::Success {
            return Err(anyhow::anyhow!("run completed with status {status:?}"));
        }
    }

    Ok(())
}

fn same_file_path(left: &std::path::Path, right: &std::path::Path) -> bool {
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
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
    let workflow_path = resolve_workflow_path(args.file.as_deref()).map_err(LocalError::wrap)?;
    let workflow_yaml = std::fs::read_to_string(&workflow_path).map_err(|error| {
        LocalError::wrap(anyhow::Error::new(error)).context(format!(
            "failed to read workflow: {}",
            workflow_path.display()
        ))
    })?;

    // Hand local reusable workflows (`uses: ./.github/workflows/…`) to the
    // expander the same way `run` hands them to the server. Remote reusables
    // (`owner/repo/…`) are left for the server to fetch at expansion time and
    // reported as such.
    let mut reusable_workflows = BTreeMap::new();
    if let Ok(current_dir) = std::env::current_dir() {
        let workflows_dir = current_dir.join(".github").join("workflows");
        if let Ok(entries) = std::fs::read_dir(&workflows_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if !matches!(
                    path.extension().and_then(|ext| ext.to_str()),
                    Some("yml" | "yaml")
                ) || same_file_path(&path, &workflow_path)
                {
                    continue;
                }
                let Some(relative) = path.strip_prefix(&current_dir).ok() else {
                    continue;
                };
                let yaml = std::fs::read_to_string(&path)
                    .with_context(|| format!("read reusable workflow {}", path.display()))?;
                reusable_workflows.insert(relative.to_string_lossy().into_owned(), yaml);
            }
        }
    }

    let parsed = aksh_gha_parser::parse_workflow(&workflow_yaml).map_err(|error| {
        LocalError::wrap(anyhow::anyhow!(
            "parse workflow {}: {error}",
            workflow_path.display()
        ))
    })?;
    let expanded = aksh_gha_parser::expand_jobs_with_reusables(&parsed, &reusable_workflows)
        .map_err(|error| {
            LocalError::wrap(anyhow::anyhow!(
                "expand workflow {}: {error}",
                workflow_path.display()
            ))
        })?;

    let jobs = &expanded.jobs;
    let step_count: usize = jobs.iter().map(|job| job.steps.len()).sum();
    if args.json {
        let summary = serde_json::json!({
            "workflow": workflow_path.display().to_string(),
            "jobs": jobs,
            "total_steps": step_count,
        });
        println!("{}", serde_json::to_string_pretty(&summary)?);
        return Ok(());
    }

    println!(
        "Plan for {}: {} job(s), {} step(s)",
        workflow_path.display(),
        jobs.len(),
        step_count
    );
    for job in jobs {
        let needs = if job.needs.is_empty() {
            String::new()
        } else {
            format!(
                "  (needs {})",
                job.needs
                    .iter()
                    .map(|need| need.0.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        println!(
            "  - {}  runs-on: {}{needs}",
            job.name,
            job.runs_on.join(", ")
        );
        for (index, step) in job.steps.iter().enumerate() {
            let label = step
                .name
                .as_deref()
                .or(step.uses.as_deref())
                .or(step.run.as_deref())
                .unwrap_or("(anonymous step)");
            println!("      {}. {label}", index + 1);
        }
    }
    Ok(())
}

async fn cmd_status(args: StatusArgs) -> anyhow::Result<()> {
    let client = build_client();
    let url = server_url();
    let mut request = client.get(format!("{url}/api/v1/runs?limit=20"));
    if let Some(token) = api_token() {
        request = request.bearer_auth(token);
    }
    let response = send_api(request, "/api/v1/runs?limit=20").await?;
    let runs: Vec<serde_json::Value> = response.json().await.map_err(|error| {
        UpstreamError::wrap(anyhow::Error::new(error))
            .context("control plane returned an unparsable run list")
    })?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&runs)?);
        return Ok(());
    }
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
            .ok_or_else(|| LocalError::wrap(anyhow::anyhow!("no runs found")))?,
    };
    let mut request = client.get(format!("{url}/api/v1/runs/{run_id}/logs"));
    if let Some(token) = api_token() {
        request = request.bearer_auth(token);
    }
    let response = send_api(request, &format!("/api/v1/runs/{run_id}/logs")).await?;
    let body = response.text().await?;
    print!("{body}");
    Ok(())
}

async fn cmd_cancel(args: CancelArgs) -> anyhow::Result<()> {
    let client = build_client();
    let url = server_url();
    let run_id = match args.run_id {
        Some(id) => id,
        None => {
            // Prefer an in-progress run, then a queued one: cancelling a queued
            // run is exactly what a stuck pipeline needs, and it is valid.
            match latest_run_id(&client, &url, Some("in_progress")).await? {
                Some(id) => id,
                None => latest_run_id(&client, &url, Some("queued"))
                    .await?
                    .ok_or_else(|| LocalError::wrap(anyhow::anyhow!("no active runs found")))?,
            }
        }
    };
    let mut request = client.post(format!("{url}/api/v1/runs/{run_id}/cancel"));
    if let Some(token) = api_token() {
        request = request.bearer_auth(token);
    }
    let response = send_api(request, &format!("/api/v1/runs/{run_id}/cancel")).await?;
    let _ = response;
    if args.json {
        println!(
            "{}",
            serde_json::json!({ "run_id": run_id, "status": "cancelled" })
        );
    } else {
        println!("Run {run_id} cancelled.");
    }
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
    let response = send_api(request, &format!("/api/v1/runs?{query}")).await?;
    let runs: Vec<serde_json::Value> = response.json().await.map_err(|error| {
        UpstreamError::wrap(anyhow::Error::new(error))
            .context("control plane returned an unparsable run list")
    })?;
    Ok(runs
        .first()
        .and_then(|run| run["run_id"].as_str())
        .map(str::to_owned))
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

    /// Serializes tests that mutate process-global env vars read by
    /// `local_runner_pool_config` (`PRELOOP_RUNNER_BUNDLE`,
    /// `PRELOOP_RUNNER_BASE_IMAGE`): parallel test threads would otherwise
    /// race each other's set_var/remove_var pairs.
    static TEST_ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn first_serve_token_creates_private_home_and_file() {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("missing").join(".preloop");
        assert!(!home.exists());

        let token = prepare_engine_token(&home, None).unwrap();

        assert_eq!(token.len(), 64);
        assert_eq!(
            std::fs::read_to_string(home.join("engine.token")).unwrap(),
            token
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(&home).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                std::fs::metadata(home.join("engine.token"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

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
    fn env_flag_accepts_common_boolean_values() {
        for value in ["1", "true", "yes", "on", " TRUE "] {
            assert_eq!(parse_flag(value), Some(true));
        }
        for value in ["0", "false", "no", "off", " OFF "] {
            assert_eq!(parse_flag(value), Some(false));
        }
        assert_eq!(parse_flag("maybe"), None);
        assert!(env_flag("__PRELOOP_TEST_FLAG_UNSET__", true));
        assert!(!env_flag("__PRELOOP_TEST_FLAG_UNSET__", false));
    }

    #[test]
    fn runner_pool_labels_includes_base_and_extra_labels() {
        unsafe {
            std::env::set_var("PRELOOP_RUNNER_LABELS", "X64,linux, custom ");
        }
        let labels = runner_pool_labels();
        unsafe {
            std::env::remove_var("PRELOOP_RUNNER_LABELS");
        }
        assert!(labels.iter().any(|l| l == "self-hosted"));
        assert!(labels.iter().any(|l| l == "Linux"));
        assert!(labels.iter().any(|l| l == std::env::consts::ARCH));
        // X64 appended; `linux` dropped (case-insensitive duplicate of the
        // base label); `custom` trimmed and appended.
        assert!(labels.iter().any(|l| l == "X64"));
        assert!(!labels.iter().any(|l| l == "linux"));
        assert!(labels.iter().any(|l| l == "custom"));
        assert_eq!(labels.len(), 5);
        // Empty extras add nothing.
        unsafe {
            std::env::set_var("PRELOOP_RUNNER_LABELS", ", ,");
        }
        let labels = runner_pool_labels();
        unsafe {
            std::env::remove_var("PRELOOP_RUNNER_LABELS");
        }
        assert_eq!(labels.len(), 3);
    }

    #[test]
    fn custom_base_image_disables_environment_replacement() {
        let _env_guard = TEST_ENV_MUTEX.lock().unwrap();
        let home = tempfile::tempdir().unwrap();
        // The config resolves a Linux guest runner bundle; fabricate one so
        // the construction reaches the base-image decision.
        let bundle = tempfile::tempdir().unwrap();
        std::fs::write(
            bundle.path().join("preloop-runner"),
            [0x7f, b'E', b'L', b'F'],
        )
        .unwrap();
        unsafe {
            std::env::set_var("PRELOOP_RUNNER_BUNDLE", bundle.path());
        }
        let queue_depth = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let next_job_runs_on =
            std::sync::Arc::new(std::sync::RwLock::new(vec!["ubuntu-latest".to_owned()]));
        let config = |base: &str| {
            unsafe {
                std::env::set_var("PRELOOP_RUNNER_BASE_IMAGE", base);
            }
            let config = local_runner_pool_config(
                home.path(),
                "http://127.0.0.1:9090".to_owned(),
                None,
                None,
                queue_depth.clone(),
                next_job_runs_on.clone(),
                false,
                std::sync::Arc::new(std::sync::RwLock::new(std::collections::BTreeMap::new())),
            )
            .unwrap();
            unsafe {
                std::env::remove_var("PRELOOP_RUNNER_BASE_IMAGE");
            }
            config
        };
        // Stock base: environment-based replacement stays enabled.
        assert!(config("ubuntu:24.04").next_job_runs_on.is_some());
        // Custom base (artifact): disabled, so idle runners are never
        // replaced by the job's implied stock base.
        assert!(config("/tmp/custom.smolmachine").next_job_runs_on.is_none());
        unsafe {
            std::env::remove_var("PRELOOP_RUNNER_BUNDLE");
        }
    }

    #[test]
    fn packed_artifact_cache_key_tracks_base_image_digest() {
        let _env_guard = TEST_ENV_MUTEX.lock().unwrap();
        let home = tempfile::tempdir().unwrap();
        let bundle = tempfile::tempdir().unwrap();
        std::fs::write(
            bundle.path().join("preloop-runner"),
            [0x7f, b'E', b'L', b'F'],
        )
        .unwrap();
        unsafe {
            std::env::set_var("PRELOOP_RUNNER_BUNDLE", bundle.path());
        }
        let queue_depth = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let next_job_runs_on =
            std::sync::Arc::new(std::sync::RwLock::new(vec!["ubuntu-latest".to_owned()]));
        let stem = |digest: &str| {
            unsafe {
                std::env::set_var(
                    "PRELOOP_RUNNER_BASE_IMAGE",
                    format!("ubuntu:24.04@sha256:{digest}"),
                );
            }
            let config = local_runner_pool_config(
                home.path(),
                "http://127.0.0.1:9090".to_owned(),
                None,
                None,
                queue_depth.clone(),
                next_job_runs_on.clone(),
                false,
                std::sync::Arc::new(std::sync::RwLock::new(std::collections::BTreeMap::new())),
            )
            .unwrap();
            unsafe {
                std::env::remove_var("PRELOOP_RUNNER_BASE_IMAGE");
            }
            config.artifact_stem
        };
        let first = stem("aaaa");
        let second = stem("bbbb");
        unsafe {
            std::env::remove_var("PRELOOP_RUNNER_BUNDLE");
        }
        assert_ne!(
            first, second,
            "a digest bump must invalidate the packed golden cache key (stale golden reuse)"
        );
        assert!(
            first.to_string_lossy().contains("ubuntu"),
            "the cache key should still name the base image: {first:?}"
        );
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
        assert!(!args.no_debug, "pausing on failure is the default");
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
            "--no-debug",
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
        assert!(args.no_debug);
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
        let github_setup::SecretCommand::Set { name, .. } = args.command else {
            panic!("expected Set");
        };
        assert_eq!(name, "MY_TOKEN");
    }

    #[test]
    fn secret_set_with_repo_scope() {
        let cli = parse(&[
            "secret",
            "set",
            "MY_TOKEN",
            "--repo",
            "owner/repo",
            "--value",
            "x",
        ])
        .unwrap();
        let Command::Secret(args) = cli.command else {
            panic!("expected Secret");
        };
        let github_setup::SecretCommand::Set { name, repo, .. } = args.command else {
            panic!("expected Set");
        };
        assert_eq!(name, "MY_TOKEN");
        assert_eq!(repo.as_deref(), Some("owner/repo"));
    }

    #[test]
    fn secret_list() {
        let cli = parse(&["secret", "list"]).unwrap();
        let Command::Secret(args) = cli.command else {
            panic!("expected Secret");
        };
        let github_setup::SecretCommand::List { repo } = args.command else {
            panic!("expected List");
        };
        assert!(repo.is_none());
    }

    #[test]
    fn secret_list_with_repo_scope() {
        let cli = parse(&["secret", "list", "--repo", "owner/repo"]).unwrap();
        let Command::Secret(args) = cli.command else {
            panic!("expected Secret");
        };
        let github_setup::SecretCommand::List { repo } = args.command else {
            panic!("expected List");
        };
        assert_eq!(repo.as_deref(), Some("owner/repo"));
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
        assert!(matches!(cli.command, Command::Status(_)));
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

    #[test]
    fn local_engine_url_keeps_workspace_checkout_offline() {
        assert!(
            should_send_local_workspace_header("http://127.0.0.1:19090", false),
            "an explicit loopback engine URL still targets the local engine"
        );
    }

    #[test]
    fn remote_engine_url_does_not_send_host_workspace_path() {
        assert!(!should_send_local_workspace_header(
            "https://ci.example.test",
            false
        ));
    }

    #[test]
    fn default_engine_transport_sends_workspace_path() {
        assert!(should_send_local_workspace_header(
            "http://127.0.0.1:9090",
            true
        ));
    }

    #[test]
    fn mounted_socket_routes_only_loopback_advertised_origins() {
        assert_eq!(
            mounted_control_origin("http://127.0.0.1:9090/").as_deref(),
            Some("http://127.0.0.1:9090")
        );
        assert_eq!(
            mounted_control_origin("http://localhost:9090").as_deref(),
            Some("http://localhost:9090")
        );
        assert_eq!(mounted_control_origin("https://aksh.preloop.dev"), None);
    }
}
