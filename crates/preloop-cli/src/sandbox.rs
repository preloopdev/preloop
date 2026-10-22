//! `preloop sandbox …` and the engine's embedded Sandbox Service.
//!
//! The engine runs a Sandbox Service in-process with this host's SmolVM
//! driver attached through an in-process host agent; the CI runner pool
//! gets its machines from it. The same service is reachable over HTTPS
//! (Connect) so other hosts can enroll as providers (`preloop sandbox agent`)
//! and other callers can acquire sandboxes (`preloop sandbox acquire`).
//! `preloop sandbox stats` prints each provider's host sample, capacity and
//! sandboxes with their runtime facts.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use clap::{Args, Subcommand};
use futures::StreamExt as _;
use preloop_orchestrator::RunnerPoolConfig;
use preloop_orchestrator::fleet::SandboxMachines;
use preloop_sandbox::agent::{Agent, AgentConfig, HostAccess, ProviderLink, RemoteLink};
use preloop_sandbox::api::*;
use preloop_sandbox::drivers::smolvm::SmolvmDriver;
use preloop_sandbox::drivers::smolvm::cli::SmolVmProvider;
use preloop_sandbox::drivers::smolvm::pack::ArtifactCache;
use preloop_sandbox::server::{Auth, serve as serve_api};
use preloop_sandbox::{
    Driver, LeastLoaded, Placement, SandboxApi, SandboxClient, ServerTls, Service, ServiceConfig,
    store, tls,
};
use tokio_util::sync::CancellationToken;

/// Default address of the engine's sandbox API.
pub(crate) const DEFAULT_SANDBOX_URL: &str = "https://127.0.0.1:9191";

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.trim().is_empty())
}

fn smolvm_slots() -> u32 {
    env_nonempty("PRELOOP_SANDBOX_SMOLVM_SLOTS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Placement strategy for this process's service:
/// `PRELOOP_SANDBOX_PLACEMENT` (default `least-loaded`).
fn placement_from_env() -> anyhow::Result<Arc<dyn Placement>> {
    let name =
        env_nonempty("PRELOOP_SANDBOX_PLACEMENT").unwrap_or_else(|| LeastLoaded::NAME.to_owned());
    preloop_sandbox::placement::by_name(&name).ok_or_else(|| {
        anyhow::anyhow!(
            "unknown sandbox placement `{name}` in PRELOOP_SANDBOX_PLACEMENT (valid: {})",
            preloop_sandbox::placement::NAMES.join(", ")
        )
    })
}

/// Service tuning: defaults overridden by the documented environment.
fn service_config(state: &Path) -> anyhow::Result<ServiceConfig> {
    let mut config = ServiceConfig::new(state.join("sandbox-blobs"));
    if let Some(raw) = env_nonempty("PRELOOP_SANDBOX_BLOB_MAX_BYTES") {
        config.blob_max_bytes = raw.parse().with_context(|| {
            format!("PRELOOP_SANDBOX_BLOB_MAX_BYTES must be a byte count, got `{raw}`")
        })?;
    }
    if let Some(value) = env_nonempty("PRELOOP_SANDBOX_POLL_BATCH")
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
    {
        config.poll_batch = value;
    }
    if let Some(secs) =
        env_nonempty("PRELOOP_SANDBOX_DELIVERY_TIMEOUT_SECS").and_then(|v| v.parse::<u64>().ok())
    {
        config.delivery_timeout = Duration::from_secs(secs);
    }
    if let Some(secs) =
        env_nonempty("PRELOOP_SANDBOX_EXEC_TIMEOUT_SECS").and_then(|v| v.parse::<u64>().ok())
    {
        config.exec_timeout = Duration::from_secs(secs);
    }
    Ok(config)
}

/// Agent tuning: defaults overridden by the documented environment.
fn sandbox_agent_config(name: impl Into<String>, scratch: PathBuf) -> AgentConfig {
    let mut config = AgentConfig::new(name, scratch);
    if let Some(value) = env_nonempty("PRELOOP_SANDBOX_AGENT_CONCURRENCY")
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
    {
        config.concurrency = value;
    }
    config
}

/// Drivers attached to this host: `PRELOOP_SANDBOX_DRIVERS`, comma-separated
/// (default `smolvm`). `named_prefix` lets the SmolVM inventory adopt the CI
/// pool's caller-named machines; `cache` holds the packed artifacts drivers
/// materialize.
pub(crate) fn host_drivers(
    named_prefix: Option<&str>,
    slots: u32,
    cache: &ArtifactCache,
) -> anyhow::Result<Vec<Arc<dyn Driver>>> {
    let names = env_nonempty("PRELOOP_SANDBOX_DRIVERS").unwrap_or_else(|| "smolvm".into());
    let mut drivers: Vec<Arc<dyn Driver>> = Vec::new();
    for name in names
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        drivers.push(match name {
            "smolvm" => {
                let mut driver =
                    SmolvmDriver::new(SmolVmProvider::default(), slots).with_cache(cache.clone());
                if let Some(prefix) = named_prefix {
                    driver = driver.with_named_prefix(prefix);
                }
                Arc::new(driver)
            }
            // Downstream builds register additional backends here.
            other => anyhow::bail!("unknown sandbox driver `{other}` in PRELOOP_SANDBOX_DRIVERS"),
        });
    }
    Ok(drivers)
}

/// The engine's in-process Sandbox Service plus its local agent.
pub(crate) struct LocalSandboxes {
    service: Arc<Service>,
    local: Arc<SmolVmProvider>,
    /// Provider id this process's agent registered under.
    provider_id: String,
    pool_driver: String,
    pool_capabilities: Option<Capabilities>,
    shutdown: CancellationToken,
}

impl LocalSandboxes {
    /// Open the service (state under `<home>/state`), attach this host's
    /// SmolVM driver, and serve the API when `PRELOOP_SANDBOX_LISTEN`
    /// (default `127.0.0.1:9191`) can be bound.
    ///
    /// `pool` is the CI pool this engine runs: its host mounts (runner
    /// bundle, Node externals, control bridge) and control socket are the
    /// embedded agent's allowlist, so the pool keeps working while every
    /// other host path is refused.
    ///
    /// `observability` supplies the in-process meter the service and its local
    /// agent record `preloop.sandbox.*` instruments on.
    pub(crate) async fn start(
        home: &Path,
        pool: Option<&RunnerPoolConfig>,
        observability: preloop_observability::Observability,
    ) -> anyhow::Result<Self> {
        let state = home.join("state");
        let store_url = env_nonempty("PRELOOP_SANDBOX_STORE_URL")
            .unwrap_or_else(|| state.join("sandbox.db").display().to_string());
        let mut config = service_config(&state)?;
        config.placement = placement_from_env()?;
        let store = store::open(&store_url)
            .await
            .with_context(|| format!("opening sandbox store {store_url}"))?;
        let tls = ServerTls::load_or_create(
            &state,
            tls::sans_from_env(),
            env_path("PRELOOP_SANDBOX_TLS_CERT").as_deref(),
            env_path("PRELOOP_SANDBOX_TLS_KEY").as_deref(),
        )
        .context("configuring the sandbox API TLS")?;
        config.ca = Some(tls.ca.clone());
        let metrics = preloop_sandbox::metrics::SandboxMetrics::from_meter(&observability.meter());
        config.metrics = metrics.clone();
        let service = Service::open(store, config).await?;
        let shutdown = CancellationToken::new();
        tokio::spawn(service.clone().run_maintenance(shutdown.clone()));

        let local = Arc::new(SmolVmProvider::default());
        let prefix =
            env_nonempty("PRELOOP_RUNNER_NAME_PREFIX").unwrap_or_else(|| "preloop-runner".into());
        let drivers = host_drivers(
            Some(&prefix),
            smolvm_slots(),
            &ArtifactCache::from_env(state.join("sandbox-cache")),
        )?;
        let pool_driver = env_nonempty("PRELOOP_SANDBOX_POOL_DRIVER").unwrap_or_else(|| {
            drivers
                .first()
                .map(|driver| driver.name().to_owned())
                .unwrap_or_else(|| "smolvm".into())
        });
        let pool_capabilities = drivers
            .iter()
            .find(|driver| driver.name() == pool_driver)
            .map(|driver| driver.capabilities());
        let mut access = HostAccess::from_env();
        if let Some(pool) = pool {
            access.allow_mounts(pool.sandbox_mount_prefixes());
            access.allow_sockets(pool.sandbox_socket_prefixes());
        }
        let mut agent_config =
            sandbox_agent_config(local_provider_name(), state.join("sandbox-agent"))
                .with_access(access);
        agent_config.metrics = metrics;
        let agent = Agent::new(
            Arc::new(service.clone()) as Arc<dyn ProviderLink>,
            drivers,
            agent_config,
        );
        let session = agent.register().await.ok();
        let provider_id = session
            .as_ref()
            .map(|session| session.provider_id.clone())
            .unwrap_or_default();
        tokio::spawn(agent.run(shutdown.clone()));

        let listen =
            env_nonempty("PRELOOP_SANDBOX_LISTEN").unwrap_or_else(|| "127.0.0.1:9191".into());
        if listen != "off" {
            let addr: SocketAddr = listen
                .parse()
                .context("PRELOOP_SANDBOX_LISTEN must be a socket address or `off`")?;
            let auth = Auth::new(resolve_client_token(&state)?);
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    tracing::info!(%addr, "sandbox API listening (https)");
                    let stop = shutdown.clone();
                    let service = service.clone();
                    tokio::spawn(async move {
                        if let Err(error) = serve_api(listener, service, auth, tls, stop).await {
                            tracing::warn!(%error, "sandbox API server stopped");
                        }
                    });
                }
                Err(error) => {
                    tracing::warn!(%addr, %error, "sandbox API not served (address in use?); the pool still works in-process");
                }
            }
        }
        // Wait for the local agent so the pool's first acquire can place.
        // An enrollment record (no drivers yet) does not count.
        for _ in 0..200 {
            if service
                .list_providers()
                .iter()
                .any(|provider| !provider.drivers.is_empty())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok(Self {
            service,
            local,
            provider_id,
            pool_driver,
            pool_capabilities,
            shutdown,
        })
    }

    /// The service API plus the provider id this process's agent registered
    /// under — the pool provider whose host sample describes this engine's
    /// host. Used to publish live sandbox telemetry to the runner server.
    pub(crate) fn api_and_provider_id(&self) -> (Arc<dyn SandboxApi>, String) {
        (
            Arc::new(preloop_sandbox::InProcess(self.service.clone())),
            self.provider_id.clone(),
        )
    }

    /// The runner pool's machine provider, pinned to the pool driver
    /// (`PRELOOP_SANDBOX_POOL_DRIVER`, default: the first host driver).
    pub(crate) fn machines(&self) -> Arc<SandboxMachines> {
        let smolvm = self.pool_driver == preloop_sandbox::drivers::smolvm::NAME;
        let caps = self.pool_capabilities.clone().unwrap_or_default();
        let capabilities = preloop_sandbox::drivers::smolvm::cli::ProviderCapabilities {
            live_host_volumes: caps.host_mounts,
            socket_mounts: caps.socket_mounts,
            // Packs are host files only on this host's SmolVM.
            file_packs: smolvm,
            // A sandbox golden is a live machine whose overlay is branched
            // (SmolVM restarts it forkable; snapshot backends capture it), so
            // a clone inherits everything the bake installed and the pool
            // must not reinstall the baseline per fork.
            fork_inherits_guest_writes: true,
            preserves_runtime_state_on_suspend: !smolvm && caps.pause,
            // SmolVM can branch a fork base that holds a read-only host
            // volume; the archive-style backends cannot.
            fork_bases_may_mount_read_only_volumes: true,
            // The bundle is mounted live, so the guest links externals into
            // the runner root rather than finding them inside the bundle.
            runner_bundle_carries_externals: false,
        };
        SandboxMachines::new(
            Arc::new(preloop_sandbox::InProcess(self.service.clone())) as Arc<dyn SandboxApi>,
            smolvm.then(|| self.local.clone()),
            smolvm.then(|| self.provider_id.clone()),
            Requirements {
                driver: self.pool_driver.clone(),
                // The pool provisions a runner over many execs.
                capabilities: vec!["multi_exec".into()],
                ..Requirements::default()
            },
            capabilities,
        )
    }
}

impl Drop for LocalSandboxes {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

fn local_provider_name() -> String {
    format!(
        "local-{}",
        hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "host".into())
    )
}

/// A path from a non-empty environment variable.
fn env_path(key: &str) -> Option<PathBuf> {
    env_nonempty(key).map(PathBuf::from)
}

/// The client token: `PRELOOP_SANDBOX_TOKEN`, else one generated on first
/// start into `<state>/sandbox-client-token` (mode 0600).
fn resolve_client_token(state: &Path) -> anyhow::Result<String> {
    if let Some(token) = env_nonempty("PRELOOP_SANDBOX_TOKEN") {
        return Ok(token);
    }
    let path = state.join("sandbox-client-token");
    if let Ok(token) = std::fs::read_to_string(&path) {
        let token = token.trim();
        if !token.is_empty() {
            return Ok(token.to_owned());
        }
    }
    let token = tls::random_hex(32);
    tls::write_private(&path, token.as_bytes())
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(token)
}

/// The token a local deployment wrote, when there is one.
fn local_client_token(home: &Path) -> Option<String> {
    std::fs::read_to_string(home.join("state/sandbox-client-token"))
        .ok()
        .map(|token| token.trim().to_owned())
        .filter(|token| !token.is_empty())
}

/// The CA a local deployment wrote, when there is one.
fn local_ca(home: &Path) -> Option<PathBuf> {
    let path = home.join("state").join(tls::CA_DIR).join(tls::CA_CERT_FILE);
    path.is_file().then_some(path)
}

/// Build the caller client, resolving the token and CA from flags, the
/// environment, then the local state directory.
fn sandbox_client(
    service: &str,
    token: Option<String>,
    ca: Option<PathBuf>,
) -> anyhow::Result<SandboxClient> {
    let home = crate::preloop_home();
    let token = token.or_else(|| local_client_token(&home)).context(
        "set --token (or PRELOOP_SANDBOX_TOKEN), or run against a local service that wrote \
         state/sandbox-client-token",
    )?;
    match ca.or_else(|| local_ca(&home)) {
        Some(path) => {
            let pem = std::fs::read_to_string(&path)
                .with_context(|| format!("reading sandbox CA {}", path.display()))?;
            Ok(SandboxClient::with_tls(service, Some(token), &pem, None)?)
        }
        None => Ok(SandboxClient::new(service, Some(token))),
    }
}

/// `preloop sandbox`.
#[derive(Debug, Args)]
pub(crate) struct SandboxArgs {
    /// Sandbox API base URL.
    #[arg(long, env = "PRELOOP_SANDBOX_URL", default_value = DEFAULT_SANDBOX_URL, global = true)]
    service: String,
    /// Bearer token for the Sandboxes API. For `agent`'s first run this is the
    /// join token (`plj1.…`).
    #[arg(
        long,
        env = "PRELOOP_SANDBOX_TOKEN",
        global = true,
        hide_env_values = true
    )]
    token: Option<String>,
    /// Lease id from `acquire`; the capability for every sandbox-scoped command.
    #[arg(long = "lease", env = "PRELOOP_SANDBOX_LEASE", global = true)]
    lease_id: Option<String>,
    /// CA certificate for the Sandbox Service (PEM).
    #[arg(long, env = "PRELOOP_SANDBOX_CA", global = true)]
    ca: Option<PathBuf>,
    #[command(subcommand)]
    command: SandboxCommand,
}

#[derive(Debug, Subcommand)]
enum SandboxCommand {
    /// Run a standalone Sandbox Service (with this host's SmolVM unless --no-local).
    Serve(ServeSandboxArgs),
    /// Join a Sandbox Service as a provider with this host's drivers.
    Agent(AgentArgs),
    /// Templates.
    #[command(subcommand)]
    Template(TemplateCommand),
    /// Pools.
    #[command(subcommand)]
    Pool(PoolCommand),
    /// Acquire a sandbox; prints the lease and sandbox as JSON.
    Acquire(AcquireArgs),
    /// Run a command in a sandbox, streaming output; exits with its status.
    Exec(ExecArgs),
    /// Copy into or out of a sandbox: `sbx-…:/path` marks the sandbox side.
    Cp(CpArgs),
    /// Power a sandbox off, keeping it (and its disk).
    Stop {
        /// Sandbox id.
        sandbox: String,
    },
    /// Start a stopped sandbox (used by golden bakes).
    Start {
        /// Sandbox id.
        sandbox: String,
        /// Start as a branch base.
        #[arg(long)]
        forkable: bool,
    },
    /// Release a lease.
    Release(ReleaseArgs),
    /// Capture a leased sandbox as a new template.
    Snapshot(SnapshotArgs),
    /// List sandboxes.
    Ls,
    /// Join tokens for provider enrollment.
    #[command(subcommand)]
    Token(TokenCommand),
    /// Providers (capacity owners).
    #[command(subcommand)]
    Provider(ProviderCommand),
    /// Per-provider host sample, capacity and sandboxes with their runtime facts.
    Stats,
}

#[derive(Debug, Subcommand)]
enum TokenCommand {
    /// Mint a single-use join token for `sandbox agent`.
    Create {
        /// Lifetime (`30s`, `10m`, `1h`, `2d`).
        #[arg(long, default_value = "1h")]
        ttl: String,
    },
}

#[derive(Debug, Subcommand)]
enum ProviderCommand {
    /// List providers and their capabilities.
    Ls,
    /// Stop placing new sandboxes on a provider.
    Cordon {
        /// Provider name.
        name: String,
    },
    /// Allow placements on a provider again.
    Uncordon {
        /// Provider name.
        name: String,
    },
    /// Cordon a provider, release its idle pool members, report leased sandboxes.
    Drain {
        /// Provider name.
        name: String,
    },
    /// Remove a provider and revoke its certificates.
    Rm {
        /// Provider name.
        name: String,
    },
}

#[derive(Debug, Args)]
struct ServeSandboxArgs {
    /// Listen address.
    #[arg(long, default_value = "127.0.0.1:9191")]
    listen: SocketAddr,
    /// Store URL (`sqlite://…`, path, or `postgres://…`).
    #[arg(long, env = "PRELOOP_SANDBOX_STORE_URL")]
    store: Option<String>,
    /// Do not attach this host's SmolVM.
    #[arg(long)]
    no_local: bool,
}

#[derive(Debug, Args)]
struct AgentArgs {
    /// Service URL to enroll with (`https://host:9191`). Required on the
    /// first run; the URL is stored with the identity.
    #[arg(long)]
    join: Option<String>,
    /// Stable provider name. Stored with the identity on first run.
    #[arg(long)]
    name: Option<String>,
    /// Join token (`plj1.…`) for the first run. Defaults to the global
    /// `--token`; later runs reuse the stored identity.
    #[arg(
        long = "join-token",
        env = "PRELOOP_SANDBOX_JOIN_TOKEN",
        hide_env_values = true
    )]
    join_token: Option<String>,
    /// Identity directory (default `<home>/state/sandbox-identity`).
    #[arg(long)]
    identity: Option<PathBuf>,
    /// Concurrent sandbox cap for the SmolVM driver (0 = none).
    #[arg(long, default_value_t = 0)]
    slots: u32,
}

#[derive(Debug, Subcommand)]
enum TemplateCommand {
    /// Register a template from one or more artifacts, or a driver recipe.
    Put {
        /// Name.
        #[arg(long)]
        name: String,
        /// Guest OS.
        #[arg(long, default_value = "linux")]
        os: String,
        /// Artifact as comma-separated `key=value` pairs:
        /// `format=oci-image|smolmachine,arch=…,ref=…,digest=sha256:…[,size=…]`.
        /// Repeat for several formats or architectures.
        #[arg(
            long = "artifact",
            value_name = "KEY=VALUE,…",
            conflicts_with = "recipe"
        )]
        artifact: Vec<String>,
        /// Driver-specific recipe interpreted by the selected driver.
        #[arg(long)]
        recipe: Option<String>,
        /// Driver that understands the recipe.
        #[arg(long)]
        driver: Option<String>,
    },
    /// List templates.
    Ls,
    /// Delete a template.
    Rm {
        /// Name.
        name: String,
    },
}

#[derive(Debug, Subcommand)]
enum PoolCommand {
    /// Create or update a pool.
    Put {
        /// Name.
        #[arg(long)]
        name: String,
        /// Template.
        #[arg(long)]
        template: String,
        /// Warm sandboxes to keep.
        #[arg(long, default_value_t = 0)]
        warm: u32,
        /// Maximum live sandboxes (0 = unbounded).
        #[arg(long, default_value_t = 0)]
        max: u32,
        /// vCPUs.
        #[arg(long, default_value_t = 2)]
        cpus: u32,
        /// Memory MiB.
        #[arg(long, default_value_t = 2048)]
        memory: u32,
        /// Disk GiB.
        #[arg(long, default_value_t = 10)]
        disk: u32,
    },
    /// List pools.
    Ls,
    /// Delete a pool.
    Rm {
        /// Name.
        name: String,
    },
}

#[derive(Debug, Args)]
struct AcquireArgs {
    /// Pool.
    #[arg(long, conflicts_with = "template")]
    pool: Option<String>,
    /// Template.
    #[arg(long)]
    template: Option<String>,
    /// Idempotency key (default: random).
    #[arg(long)]
    key: Option<String>,
    /// Lease TTL seconds.
    #[arg(long, default_value_t = 3600)]
    ttl: u64,
    /// `KEY=VALUE` merged into every exec on the lease.
    #[arg(long = "env", value_parser = parse_kv)]
    env: Vec<(String, String)>,
    /// Required guest OS.
    #[arg(long)]
    os: Option<String>,
    /// Build (snapshot-capable) sandbox.
    #[arg(long)]
    build: bool,
    /// vCPUs (template acquires).
    #[arg(long, default_value_t = 0)]
    cpus: u32,
    /// Memory MiB (template acquires).
    #[arg(long, default_value_t = 0)]
    memory: u32,
}

#[derive(Debug, Args)]
struct ExecArgs {
    /// Sandbox id.
    sandbox: String,
    /// `KEY=VALUE`.
    #[arg(long = "env", value_parser = parse_kv)]
    env: Vec<(String, String)>,
    /// Working directory.
    #[arg(long)]
    workdir: Option<String>,
    /// Command.
    #[arg(last = true, required = true)]
    argv: Vec<String>,
}

#[derive(Debug, Args)]
struct CpArgs {
    /// Source (`sbx-…:/path` for the sandbox side).
    source: String,
    /// Destination.
    destination: String,
}

#[derive(Debug, Args)]
struct ReleaseArgs {
    /// Lease id.
    lease: String,
    /// Keep the sandbox for debugging for this many seconds.
    #[arg(long)]
    preserve: Option<u64>,
}

#[derive(Debug, Args)]
struct SnapshotArgs {
    /// Sandbox id.
    sandbox: String,
    /// New template name.
    #[arg(long)]
    name: String,
}

fn parse_kv(value: &str) -> Result<(String, String), String> {
    value
        .split_once('=')
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .ok_or_else(|| format!("expected KEY=VALUE, got `{value}`"))
}

/// Parse one `--artifact` value:
/// `format=…,arch=…,ref=…,digest=sha256:…[,size=…]`.
fn parse_artifact(raw: &str) -> anyhow::Result<Artifact> {
    let mut artifact = Artifact::default();
    for pair in raw.split(',') {
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("expected key=value in `{raw}`, got `{pair}`"))?;
        match key.trim() {
            "format" => artifact.format = value.trim().to_owned(),
            "arch" => artifact.arch = value.trim().to_owned(),
            "ref" => artifact.r#ref = value.trim().to_owned(),
            "digest" => artifact.digest = value.trim().to_owned(),
            "size" => {
                artifact.size_bytes = value
                    .trim()
                    .parse()
                    .map_err(|_| anyhow::anyhow!("invalid artifact size `{value}`"))?;
            }
            other => anyhow::bail!("unknown artifact field `{other}`"),
        }
    }
    artifact
        .validate()
        .map_err(|error| anyhow::anyhow!("artifact `{raw}`: {error}"))?;
    Ok(artifact)
}

/// Parse `30s`, `10m`, `1h` or `2d`.
fn parse_ttl(value: &str) -> anyhow::Result<Duration> {
    let value = value.trim();
    let (number, unit) = value.split_at(value.len().saturating_sub(1));
    let number: u64 = number
        .parse()
        .with_context(|| format!("expected a duration like 30m, 1h or 2d, got `{value}`"))?;
    let seconds = match unit {
        "s" => number,
        "m" => number.saturating_mul(60),
        "h" => number.saturating_mul(3600),
        "d" => number.saturating_mul(86_400),
        _ => anyhow::bail!("expected a duration like 30m, 1h or 2d, got `{value}`"),
    };
    Ok(Duration::from_secs(seconds))
}

fn print_json<T: serde::Serialize>(value: &T) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

/// Entry point for `preloop sandbox`.
pub(crate) async fn run(
    args: SandboxArgs,
    observability: preloop_observability::Observability,
) -> anyhow::Result<()> {
    let SandboxArgs {
        service,
        token,
        ca,
        lease_id,
        command,
    } = args;
    match command {
        SandboxCommand::Serve(serve) => serve_standalone(serve, observability).await,
        SandboxCommand::Agent(agent) => run_agent(&service, token, agent, observability).await,
        command => {
            let client = sandbox_client(&service, token, ca)?;
            run_client(client, command, lease_id.unwrap_or_default()).await
        }
    }
}

async fn run_client(
    client: SandboxClient,
    command: SandboxCommand,
    lease: String,
) -> anyhow::Result<()> {
    let lease_id = || -> anyhow::Result<String> {
        if lease.is_empty() {
            anyhow::bail!(
                "this command needs the sandbox's lease: pass --lease <lse-…> (or set \
                 PRELOOP_SANDBOX_LEASE) with the lease `preloop sandbox acquire` returned"
            );
        }
        Ok(lease.clone())
    };
    match command {
        SandboxCommand::Serve(_) | SandboxCommand::Agent(_) => {
            unreachable!("serve and agent run without a caller client")
        }
        SandboxCommand::Template(TemplateCommand::Put {
            name,
            os,
            artifact,
            recipe,
            driver,
        }) => print_json(
            &client
                .put_template(TemplateSpec {
                    name,
                    os,
                    artifacts: artifact
                        .iter()
                        .map(|raw| parse_artifact(raw))
                        .collect::<anyhow::Result<Vec<_>>>()?,
                    source: recipe.map(template_spec::Source::Recipe),
                    requires: Requirements {
                        driver: driver.unwrap_or_default(),
                        ..Requirements::default()
                    }
                    .into(),
                    ..TemplateSpec::default()
                })
                .await?,
        ),
        SandboxCommand::Template(TemplateCommand::Ls) => {
            print_json(&client.list_templates().await?)
        }
        SandboxCommand::Template(TemplateCommand::Rm { name }) => {
            Ok(client.delete_template(&name).await?)
        }
        SandboxCommand::Pool(PoolCommand::Put {
            name,
            template,
            warm,
            max,
            cpus,
            memory,
            disk,
        }) => print_json(
            &client
                .put_pool(PoolSpec {
                    name,
                    template,
                    warm,
                    max,
                    shape: Shape {
                        cpus,
                        memory_mib: memory,
                        disk_gib: disk,
                        ..Shape::default()
                    }
                    .into(),
                    network: NetworkPolicy {
                        mode: NetworkMode::UNRESTRICTED.into(),
                        ..NetworkPolicy::default()
                    }
                    .into(),
                    ..PoolSpec::default()
                })
                .await?,
        ),
        SandboxCommand::Pool(PoolCommand::Ls) => print_json(&client.list_pools().await?),
        SandboxCommand::Pool(PoolCommand::Rm { name }) => Ok(client.delete_pool(&name).await?),
        SandboxCommand::Acquire(acquire) => {
            let mut request = AcquireRequest {
                idempotency_key: acquire
                    .key
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
                pool: acquire.pool.unwrap_or_default(),
                template: acquire.template.unwrap_or_default(),
                ttl_secs: acquire.ttl,
                purpose: if acquire.build {
                    Purpose::BUILD
                } else {
                    Purpose::EXEC
                }
                .into(),
                shape: Shape {
                    cpus: acquire.cpus,
                    memory_mib: acquire.memory,
                    disk_gib: 0,
                    ..Shape::default()
                }
                .into(),
                requires: Requirements {
                    os: acquire.os.unwrap_or_default(),
                    ..Requirements::default()
                }
                .into(),
                network: NetworkPolicy {
                    mode: NetworkMode::UNRESTRICTED.into(),
                    ..NetworkPolicy::default()
                }
                .into(),
                ..AcquireRequest::default()
            };
            request
                .inject
                .modify(|inject| inject.env.extend(acquire.env));
            print_json(&client.acquire(request).await?)
        }
        SandboxCommand::Exec(exec) => {
            let mut interrupt = Interrupt::install()?;
            let request = ExecRequest {
                sandbox_id: exec.sandbox,
                lease_id: lease_id()?,
                argv: exec.argv,
                env: exec.env.into_iter().collect(),
                workdir: exec.workdir.unwrap_or_default(),
                ..ExecRequest::default()
            };
            let stream = tokio::select! {
                result = client.exec(request) => result?,
                _ = interrupt.wait() => std::process::exit(INTERRUPTED),
            };
            match pump_exec(stream, interrupt.wait()).await? {
                Some(code) => std::process::exit(code),
                None => std::process::exit(INTERRUPTED),
            }
        }
        SandboxCommand::Cp(cp) => copy(&client, &cp.source, &cp.destination, &lease_id()?).await,
        SandboxCommand::Stop { sandbox } => Ok(client.stop(&sandbox, &lease_id()?).await?),
        SandboxCommand::Start { sandbox, forkable } => {
            Ok(client.start(&sandbox, &lease_id()?, forkable).await?)
        }
        SandboxCommand::Release(release) => Ok(client
            .release(ReleaseRequest {
                lease_id: release.lease,
                mode: if release.preserve.is_some() {
                    ReleaseMode::PRESERVE
                } else {
                    ReleaseMode::DESTROY
                }
                .into(),
                preserve_ttl_secs: release.preserve.unwrap_or_default(),
                ..ReleaseRequest::default()
            })
            .await?),
        SandboxCommand::Snapshot(snapshot) => print_json(
            &client
                .snapshot(SnapshotRequest {
                    sandbox_id: snapshot.sandbox,
                    lease_id: lease_id()?,
                    name: snapshot.name,
                    ..SnapshotRequest::default()
                })
                .await?,
        ),
        SandboxCommand::Ls => print_json(&client.list().await?),
        SandboxCommand::Token(TokenCommand::Create { ttl }) => {
            let ttl = parse_ttl(&ttl)?;
            let response = client.create_join_token(ttl.as_secs()).await?;
            println!("{}", response.token);
            Ok(())
        }
        SandboxCommand::Provider(provider) => match provider {
            ProviderCommand::Ls => print_json(&client.list_providers().await?),
            ProviderCommand::Rm { name } => Ok(client.delete_provider(&name).await?),
            ProviderCommand::Cordon { name } => print_json(&client.cordon_provider(&name).await?),
            ProviderCommand::Uncordon { name } => {
                print_json(&client.uncordon_provider(&name).await?)
            }
            ProviderCommand::Drain { name } => print_json(&client.drain_provider(&name).await?),
        },
        SandboxCommand::Stats => print_json(&client.stats().await?),
    }
}

/// `sbx-…:/unix/path` or `sbx-…:C:/windows/path`.
fn split_sandbox(path: &str) -> Option<(&str, &str)> {
    let (sandbox, guest) = path.split_once(':')?;
    let windows =
        guest.len() > 2 && guest.as_bytes()[0].is_ascii_alphabetic() && guest.as_bytes()[1] == b':';
    (sandbox.starts_with("sbx-") && (guest.starts_with('/') || windows)).then_some((sandbox, guest))
}

/// Exit status for an exec abandoned with Ctrl-C, following the shell's
/// `128 + SIGINT`.
const INTERRUPTED: i32 = 130;

/// Ctrl-C listener for an exec, installed before the request is sent.
///
/// The process may have inherited `SIG_IGN` for SIGINT (a background job
/// started by a non-interactive shell does), so relying on the default
/// disposition leaves `preloop sandbox exec` alive after Ctrl-C. Registering
/// a handler replaces that disposition; dropping the returned stream then
/// closes the response body, which is what lets the service cancel the guest
/// command.
struct Interrupt {
    #[cfg(unix)]
    signal: tokio::signal::unix::Signal,
}

impl Interrupt {
    fn install() -> anyhow::Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Self {
                signal: signal(SignalKind::interrupt()).context("listening for SIGINT")?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    async fn wait(&mut self) {
        #[cfg(unix)]
        {
            // A closed signal stream cannot happen while this process lives;
            // never resolve rather than treating it as Ctrl-C.
            if self.signal.recv().await.is_none() {
                std::future::pending::<()>().await;
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

/// Forward exec output until the exit event or `interrupt`.
///
/// Returns the command's exit code, or `None` when interrupted. The stream is
/// dropped on every path, so an interrupted exec closes the response body and
/// the service queues `CancelExec` for the guest command.
async fn pump_exec<S, I>(mut stream: S, interrupt: I) -> anyhow::Result<Option<i32>>
where
    S: futures::Stream<Item = preloop_sandbox::Result<ExecEvent>> + Unpin,
    I: Future<Output = ()>,
{
    use std::io::Write as _;
    let mut interrupt = std::pin::pin!(interrupt);
    loop {
        let next = tokio::select! {
            next = stream.next() => next,
            _ = &mut interrupt => return Ok(None),
        };
        let Some(event) = next else {
            anyhow::bail!("exec stream ended without an exit status");
        };
        let event = event?;
        match event.event {
            Some(exec_event::Event::Stdout(bytes)) => {
                std::io::stdout().write_all(&bytes)?;
                std::io::stdout().flush()?;
            }
            Some(exec_event::Event::Stderr(bytes)) => {
                std::io::stderr().write_all(&bytes)?;
            }
            Some(exec_event::Event::ExitCode(code)) => return Ok(Some(code)),
            None => {}
        }
    }
}

async fn copy(
    client: &SandboxClient,
    source: &str,
    destination: &str,
    lease_id: &str,
) -> anyhow::Result<()> {
    let scratch = tempfile::tempdir()?;
    let tar_path = scratch.path().join("copy.tar");
    match (split_sandbox(source), split_sandbox(destination)) {
        (None, Some((sandbox, guest_dir))) => {
            let source = PathBuf::from(source);
            let name = source
                .file_name()
                .context("source needs a file name")?
                .to_string_lossy()
                .into_owned();
            {
                let mut builder = tar::Builder::new(std::fs::File::create(&tar_path)?);
                if source.is_dir() {
                    builder.append_dir_all(&name, &source)?;
                } else {
                    builder.append_path_with_name(&source, &name)?;
                }
                builder.finish()?;
            }
            client
                .upload(sandbox, lease_id, guest_dir, &tar_path)
                .await?;
            Ok(())
        }
        (Some((sandbox, guest)), None) => {
            client.download(sandbox, lease_id, guest, &tar_path).await?;
            std::fs::create_dir_all(destination)?;
            tar::Archive::new(std::fs::File::open(&tar_path)?).unpack(destination)?;
            Ok(())
        }
        _ => anyhow::bail!("exactly one side must be `sbx-…:/absolute/path`"),
    }
}

async fn serve_standalone(
    args: ServeSandboxArgs,
    observability: preloop_observability::Observability,
) -> anyhow::Result<()> {
    let home = crate::preloop_home();
    let state = home.join("state");
    let store_url = args
        .store
        .unwrap_or_else(|| state.join("sandbox.db").display().to_string());
    let mut config = service_config(&state)?;
    config.placement = placement_from_env()?;
    let store = store::open(&store_url).await?;
    let tls = ServerTls::load_or_create(
        &state,
        tls::sans_from_env(),
        env_path("PRELOOP_SANDBOX_TLS_CERT").as_deref(),
        env_path("PRELOOP_SANDBOX_TLS_KEY").as_deref(),
    )?;
    config.ca = Some(tls.ca.clone());
    let metrics = preloop_sandbox::metrics::SandboxMetrics::from_meter(&observability.meter());
    config.metrics = metrics.clone();
    let service = Service::open(store, config).await?;
    let shutdown = CancellationToken::new();
    tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
    if !args.no_local {
        let mut agent_config =
            sandbox_agent_config(local_provider_name(), state.join("sandbox-agent"));
        agent_config.metrics = metrics;
        let agent = Agent::new(
            Arc::new(service.clone()) as Arc<dyn ProviderLink>,
            host_drivers(
                None,
                smolvm_slots(),
                &ArtifactCache::from_env(state.join("sandbox-cache")),
            )?,
            agent_config.with_access(HostAccess::from_env()),
        );
        tokio::spawn(agent.run(shutdown.clone()));
    }
    let auth = Auth::new(resolve_client_token(&state)?);
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    eprintln!("[preloop] sandbox service on https://{}", args.listen);
    let stop = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        stop.cancel();
    });
    serve_api(listener, service, auth, tls, shutdown.clone()).await?;
    shutdown.cancel();
    Ok(())
}

async fn run_agent(
    service: &str,
    client_token: Option<String>,
    args: AgentArgs,
    observability: preloop_observability::Observability,
) -> anyhow::Result<()> {
    let home = crate::preloop_home();
    let state = home.join("state");
    let identity_dir = args
        .identity
        .unwrap_or_else(|| state.join("sandbox-identity"));
    let initial_name = args.name.unwrap_or_else(local_provider_name);
    let base = args.join.as_deref().or(Some(service));
    let join_token = args.join_token.or(client_token);
    let link =
        RemoteLink::connect(&identity_dir, base, join_token.as_deref(), &initial_name).await?;
    // An existing identity owns the name it was enrolled under.
    let name = link.name().unwrap_or(initial_name);
    let base = link.base().to_owned();
    let mut agent_config =
        sandbox_agent_config(name, state.join("sandbox-agent")).with_access(HostAccess::from_env());
    agent_config.metrics =
        preloop_sandbox::metrics::SandboxMetrics::from_meter(&observability.meter());
    let agent = Agent::new(
        Arc::new(link),
        host_drivers(
            None,
            args.slots,
            &ArtifactCache::from_env(state.join("sandbox-cache")),
        )?,
        agent_config,
    );
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        stop.cancel();
    });
    eprintln!("[preloop] sandbox agent joining {base}");
    agent.run(shutdown).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pump_exec_forwards_output_and_returns_the_exit_code() {
        let stream = futures::stream::iter(vec![
            Ok(ExecEvent::stdout(b"hello".to_vec())),
            Ok(ExecEvent::stderr(b"warn".to_vec())),
            Ok(ExecEvent::exit(7)),
        ]);
        let outcome = pump_exec(stream, std::future::pending::<()>())
            .await
            .unwrap();
        assert_eq!(outcome, Some(7));
    }

    #[tokio::test]
    async fn pump_exec_stops_without_an_exit_when_interrupted_while_idle() {
        let stream = futures::stream::pending::<preloop_sandbox::Result<ExecEvent>>();
        let outcome = pump_exec(stream, std::future::ready(())).await.unwrap();
        assert_eq!(outcome, None);
    }
}
