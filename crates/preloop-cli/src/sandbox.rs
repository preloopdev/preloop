//! `preloop sandbox …` and the engine's embedded Sandbox Service.
//!
//! The engine runs a Sandbox Service in-process with this host's SmolVM
//! driver attached through an in-process host agent; the CI runner pool
//! gets its machines from it. The same service is reachable over HTTP
//! (Connect) so other hosts can join as providers (`preloop sandbox agent`)
//! and other callers can acquire sandboxes (`preloop sandbox acquire`).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use clap::{Args, Subcommand};
use futures::StreamExt as _;
use preloop_orchestrator::fleet::SandboxMachines;
use preloop_sandbox::agent::{Agent, AgentConfig, ProviderLink, RemoteLink};
use preloop_sandbox::api::*;
use preloop_sandbox::drivers::smolvm::SmolvmDriver;
use preloop_sandbox::drivers::smolvm::cli::SmolVmProvider;
use preloop_sandbox::server::{Auth, router};
use preloop_sandbox::{
    Driver, LeastLoaded, Placement, SandboxApi, SandboxClient, Service, ServiceConfig, store,
};
use tokio_util::sync::CancellationToken;

/// Default loopback address of the engine's sandbox API.
pub(crate) const DEFAULT_SANDBOX_URL: &str = "http://127.0.0.1:9191";

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

/// Drivers attached to this host: `PRELOOP_SANDBOX_DRIVERS`, comma-separated
/// (default `smolvm`). `named_prefix` lets the SmolVM inventory adopt the CI
/// pool's caller-named machines. `local` is the provider an adapter will use
/// for host-side operations; passing it here (as a clone of the same handle)
/// keeps the driver and adapter on one lifecycle lock.
pub(crate) fn host_drivers(
    named_prefix: Option<&str>,
    slots: u32,
    local: Option<SmolVmProvider>,
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
                let driver = smolvm_driver(named_prefix, slots, local.clone().unwrap_or_default());
                Arc::new(driver) as Arc<dyn Driver>
            }
            // Downstream builds register additional backends here.
            other => anyhow::bail!("unknown sandbox driver `{other}` in PRELOOP_SANDBOX_DRIVERS"),
        });
    }
    Ok(drivers)
}

fn smolvm_driver(named_prefix: Option<&str>, slots: u32, provider: SmolVmProvider) -> SmolvmDriver {
    let mut driver = SmolvmDriver::new(provider, slots);
    if let Some(prefix) = named_prefix {
        driver = driver.with_named_prefix(prefix);
    }
    driver
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
    pub(crate) async fn start(home: &Path) -> anyhow::Result<Self> {
        let state = home.join("state");
        let store_url = env_nonempty("PRELOOP_SANDBOX_STORE_URL")
            .unwrap_or_else(|| state.join("sandbox.db").display().to_string());
        let mut config = ServiceConfig::new(state.join("sandbox-blobs"));
        config.placement = placement_from_env()?;
        let store = store::open(&store_url)
            .await
            .with_context(|| format!("opening sandbox store {store_url}"))?;
        let service = Service::open(store, config).await?;
        let shutdown = CancellationToken::new();
        tokio::spawn(service.clone().run_maintenance(shutdown.clone()));

        let local = Arc::new(SmolVmProvider::default());
        let prefix =
            env_nonempty("PRELOOP_RUNNER_NAME_PREFIX").unwrap_or_else(|| "preloop-runner".into());
        let drivers = host_drivers(Some(&prefix), smolvm_slots(), Some((*local).clone()))?;
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
        let agent = Agent::new(
            Arc::new(service.clone()) as Arc<dyn ProviderLink>,
            drivers,
            AgentConfig::new(local_provider_name(), state.join("sandbox-agent")),
        );
        // Register once here so the pool can place immediately; the agent
        // reuses this session instead of registering a second time (which
        // would fence its own in-flight work).
        let session = agent
            .register()
            .await
            .context("registering the local sandbox provider")?;
        let provider_id = session.provider_id.clone();
        tokio::spawn(agent.run(Some(session), shutdown.clone()));

        let listen =
            env_nonempty("PRELOOP_SANDBOX_LISTEN").unwrap_or_else(|| "127.0.0.1:9191".into());
        if listen != "off" {
            let addr: SocketAddr = listen
                .parse()
                .context("PRELOOP_SANDBOX_LISTEN must be a socket address or `off`")?;
            let auth = api_auth(&state, &addr)?;
            match bind_endpoint(&state, addr).await {
                Ok(listener) => {
                    let bound = listener.local_addr()?;
                    tracing::info!(addr = %bound, "sandbox API listening");
                    let app = router(service.clone(), auth);
                    let stop = shutdown.clone();
                    let endpoint_state = state.clone();
                    tokio::spawn(async move {
                        let _ = axum::serve(listener, app)
                            .with_graceful_shutdown(async move { stop.cancelled().await })
                            .await;
                        clear_endpoint(&endpoint_state, bound);
                    });
                }
                Err(error) => {
                    tracing::warn!(%addr, %error, "sandbox API not served (address in use?); the pool still works in-process");
                }
            }
        }
        // Wait for the local agent so the pool's first acquire can place.
        for _ in 0..200 {
            if !service.list_providers().is_empty() {
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

/// The API endpoint file: the address the last successful bind published.
pub(crate) const ENDPOINT_FILE: &str = "sandbox-endpoint";

fn endpoint_path(state: &Path) -> PathBuf {
    state.join(ENDPOINT_FILE)
}

fn read_token_file(path: &Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|token| token.trim().to_owned())
        .filter(|token| !token.is_empty())
}

/// A bearer token: the configured value, else the one this state directory
/// already holds, else a freshly generated one written with mode 0600.
pub(crate) fn resolve_token(
    configured: Option<String>,
    state: &Path,
    file_name: &str,
) -> anyhow::Result<String> {
    if let Some(token) = configured.filter(|token| !token.trim().is_empty()) {
        return Ok(token);
    }
    let path = state.join(file_name);
    if let Some(token) = read_token_file(&path) {
        // A token file left with a wider mode by an old build or a manual
        // copy must not stay readable by other local users.
        crate::set_private_file_permissions(&path)
            .with_context(|| format!("securing {}", path.display()))?;
        return Ok(token);
    }
    // 32 random bytes, hex: stable across restarts, unguessable to a local
    // process that cannot read the 0600 file.
    let token = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    crate::write_private_file(&path, token.as_bytes())
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(token)
}

/// Both bearer tokens are required, whatever the listener address. Missing
/// ones are generated into the state directory on first start.
fn api_auth(state: &Path, addr: &SocketAddr) -> anyhow::Result<Auth> {
    Ok(Auth {
        client_token: Some(resolve_token(
            env_nonempty("PRELOOP_SANDBOX_TOKEN"),
            state,
            "sandbox-client-token",
        )?),
        provider_token: Some(resolve_token(
            env_nonempty("PRELOOP_SANDBOX_PROVIDER_TOKEN"),
            state,
            "sandbox-provider-token",
        )?),
        loopback_only: addr.ip().is_loopback(),
    })
}

/// Publish the address a listener actually bound. Written owner-only so a
/// local caller can trust it as the service it should send a token to.
pub(crate) fn write_endpoint(state: &Path, addr: SocketAddr) -> anyhow::Result<()> {
    let path = endpoint_path(state);
    crate::write_private_file(&path, format!("http://{addr}\n").as_bytes())
        .with_context(|| format!("writing {}", path.display()))
}

fn read_endpoint(state: &Path) -> Option<String> {
    std::fs::read_to_string(endpoint_path(state))
        .ok()
        .map(|base| base.trim().to_owned())
        .filter(|base| !base.is_empty())
}

/// Remove the endpoint file only while it still names `addr`: a newer
/// process may have published its own address.
pub(crate) fn clear_endpoint(state: &Path, addr: SocketAddr) {
    let path = endpoint_path(state);
    if read_endpoint(state).as_deref() == Some(format!("http://{addr}").as_str()) {
        let _ = std::fs::remove_file(path);
    }
}

/// Bind and publish the endpoint, or clear a stale entry naming the address
/// that could not be bound (a squatter must not inherit the CLI's trust).
pub(crate) async fn bind_endpoint(
    state: &Path,
    addr: SocketAddr,
) -> std::io::Result<tokio::net::TcpListener> {
    match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => {
            let bound = listener.local_addr()?;
            write_endpoint(state, bound).map_err(std::io::Error::other)?;
            Ok(listener)
        }
        Err(error) => {
            clear_endpoint(state, addr);
            Err(error)
        }
    }
}

/// Resolve the address a CLI call targets: an explicit `--service` (or
/// `PRELOOP_SANDBOX_URL`) wins; otherwise the endpoint file written by the
/// local service. Sending the token to an address the file does not name
/// requires the caller to ask for it explicitly.
pub(crate) fn resolve_client(
    state: &Path,
    service: Option<String>,
    token: Option<String>,
) -> anyhow::Result<(String, String)> {
    let service = match service.filter(|service| !service.trim().is_empty()) {
        Some(service) => service,
        None => read_endpoint(state).with_context(|| {
            format!(
                "no sandbox endpoint file ({}); pass --service (default {DEFAULT_SANDBOX_URL})",
                endpoint_path(state).display()
            )
        })?,
    };
    let token = match token.filter(|token| !token.trim().is_empty()) {
        Some(token) => token,
        None => read_token_file(&state.join("sandbox-client-token")).context(
            "set --token (or PRELOOP_SANDBOX_TOKEN), or run against a local service that \
             wrote state/sandbox-client-token",
        )?,
    };
    Ok((service, token))
}

fn resolve_service(state: &Path, service: Option<String>) -> anyhow::Result<String> {
    match service.filter(|service| !service.trim().is_empty()) {
        Some(service) => Ok(service),
        None => read_endpoint(state).with_context(|| {
            format!(
                "no sandbox endpoint file ({}); pass --service",
                endpoint_path(state).display()
            )
        }),
    }
}

/// The embedded service is only needed when a pool will place VMs on it.
pub(crate) async fn start_if_pool(
    pool_configured: bool,
    home: &Path,
) -> anyhow::Result<Option<LocalSandboxes>> {
    if !pool_configured {
        return Ok(None);
    }
    Ok(Some(LocalSandboxes::start(home).await?))
}

/// `preloop sandbox`.
#[derive(Debug, Args)]
pub(crate) struct SandboxArgs {
    /// Sandbox API base URL (default: the address in `<home>/state/sandbox-endpoint`).
    #[arg(long, env = "PRELOOP_SANDBOX_URL", global = true)]
    service: Option<String>,
    /// Bearer token for the Sandboxes API.
    #[arg(
        long,
        env = "PRELOOP_SANDBOX_TOKEN",
        global = true,
        hide_env_values = true
    )]
    token: Option<String>,
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
    /// List providers and their capabilities.
    Providers,
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
    /// Stable provider name.
    #[arg(long)]
    name: Option<String>,
    /// Provider token.
    #[arg(long, env = "PRELOOP_SANDBOX_PROVIDER_TOKEN", hide_env_values = true)]
    provider_token: Option<String>,
    /// Concurrent sandbox cap for the SmolVM driver (0 = none).
    #[arg(long, default_value_t = 0)]
    slots: u32,
}

#[derive(Debug, Subcommand)]
enum TemplateCommand {
    /// Register a template from an OCI image or a driver recipe.
    Put {
        /// Name.
        #[arg(long)]
        name: String,
        /// Guest OS.
        #[arg(long, default_value = "linux")]
        os: String,
        /// OCI image.
        #[arg(long, conflicts_with = "recipe")]
        image: Option<String>,
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

fn print_json<T: serde::Serialize>(value: &T) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

/// Entry point for `preloop sandbox`.
pub(crate) async fn run(args: SandboxArgs) -> anyhow::Result<()> {
    let SandboxArgs {
        service,
        token,
        command,
    } = args;
    match command {
        SandboxCommand::Serve(serve) => serve_standalone(serve).await,
        SandboxCommand::Agent(agent) => run_agent(service, agent).await,
        command => {
            let state = crate::preloop_home().join("state");
            let (base, token) = resolve_client(&state, service, token)?;
            let client = SandboxClient::new(base, Some(token));
            match command {
                SandboxCommand::Serve(_) | SandboxCommand::Agent(_) => {
                    unreachable!("handled above")
                }
                SandboxCommand::Template(TemplateCommand::Put {
                    name,
                    os,
                    image,
                    recipe,
                    driver,
                }) => print_json(
                    &client
                        .put_template(TemplateSpec {
                            name,
                            os,
                            image,
                            recipe,
                            requires: Requirements {
                                driver: driver.unwrap_or_default(),
                                ..Requirements::default()
                            },
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
                            },
                            network: NetworkPolicy {
                                mode: NetworkMode::Unrestricted,
                                ..NetworkPolicy::default()
                            },
                            ..PoolSpec::default()
                        })
                        .await?,
                ),
                SandboxCommand::Pool(PoolCommand::Ls) => print_json(&client.list_pools().await?),
                SandboxCommand::Pool(PoolCommand::Rm { name }) => {
                    Ok(client.delete_pool(&name).await?)
                }
                SandboxCommand::Acquire(acquire) => {
                    let pool = acquire.pool.unwrap_or_default();
                    let template = acquire.template.unwrap_or_default();
                    // A pool acquire takes its placement from the pool; the service
                    // rejects template-only fields, so leave them unset.
                    let pooled = !pool.is_empty();
                    let mut request = AcquireRequest {
                        idempotency_key: acquire
                            .key
                            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
                        pool,
                        template,
                        ttl_secs: acquire.ttl,
                        purpose: if acquire.build {
                            Purpose::Build
                        } else {
                            Purpose::Exec
                        },
                        shape: Shape {
                            cpus: acquire.cpus,
                            memory_mib: acquire.memory,
                            disk_gib: 0,
                        },
                        requires: Requirements {
                            os: acquire.os.unwrap_or_default(),
                            ..Requirements::default()
                        },
                        network: if pooled {
                            NetworkPolicy::default()
                        } else {
                            NetworkPolicy {
                                mode: NetworkMode::Unrestricted,
                                ..NetworkPolicy::default()
                            }
                        },
                        ..AcquireRequest::default()
                    };
                    request.inject.env.extend(acquire.env);
                    print_json(&client.acquire(request).await?)
                }
                SandboxCommand::Exec(exec) => {
                    let mut stream = client
                        .exec(ExecRequest {
                            sandbox_id: exec.sandbox,
                            argv: exec.argv,
                            env: exec.env.into_iter().collect(),
                            workdir: exec.workdir.unwrap_or_default(),
                            ..ExecRequest::default()
                        })
                        .await?;
                    use std::io::Write as _;
                    while let Some(event) = stream.next().await {
                        let event = event?;
                        if let Some(bytes) = event.stdout {
                            std::io::stdout().write_all(&bytes)?;
                            std::io::stdout().flush()?;
                        }
                        if let Some(bytes) = event.stderr {
                            std::io::stderr().write_all(&bytes)?;
                        }
                        if let Some(code) = event.exit_code {
                            std::process::exit(code);
                        }
                    }
                    anyhow::bail!("exec stream ended without an exit status")
                }
                SandboxCommand::Cp(cp) => copy(&client, &cp.source, &cp.destination).await,
                SandboxCommand::Stop { sandbox } => Ok(client.stop(&sandbox).await?),
                SandboxCommand::Start { sandbox, forkable } => {
                    Ok(client.start(&sandbox, forkable).await?)
                }
                SandboxCommand::Release(release) => Ok(client
                    .release(ReleaseRequest {
                        lease_id: release.lease,
                        mode: if release.preserve.is_some() {
                            ReleaseMode::Preserve
                        } else {
                            ReleaseMode::Destroy
                        },
                        preserve_ttl_secs: release.preserve.unwrap_or_default(),
                    })
                    .await?),
                SandboxCommand::Snapshot(snapshot) => print_json(
                    &client
                        .snapshot(SnapshotRequest {
                            sandbox_id: snapshot.sandbox,
                            name: snapshot.name,
                        })
                        .await?,
                ),
                SandboxCommand::Ls => print_json(&client.list().await?),
                SandboxCommand::Providers => print_json(&client.list_providers().await?),
            }
        }
    }
}

/// `sbx-…:/unix/path` or `sbx-…:C:/windows/path`.
fn split_sandbox(path: &str) -> Option<(&str, &str)> {
    let (sandbox, guest) = path.split_once(':')?;
    let windows =
        guest.len() > 2 && guest.as_bytes()[0].is_ascii_alphabetic() && guest.as_bytes()[1] == b':';
    (sandbox.starts_with("sbx-") && (guest.starts_with('/') || windows)).then_some((sandbox, guest))
}

async fn copy(client: &SandboxClient, source: &str, destination: &str) -> anyhow::Result<()> {
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
            client.upload(sandbox, guest_dir, &tar_path).await?;
            Ok(())
        }
        (Some((sandbox, guest)), None) => {
            client.download(sandbox, guest, &tar_path).await?;
            std::fs::create_dir_all(destination)?;
            tar::Archive::new(std::fs::File::open(&tar_path)?).unpack(destination)?;
            Ok(())
        }
        _ => anyhow::bail!("exactly one side must be `sbx-…:/absolute/path`"),
    }
}

async fn serve_standalone(args: ServeSandboxArgs) -> anyhow::Result<()> {
    let home = crate::preloop_home();
    let state = home.join("state");
    let store_url = args
        .store
        .unwrap_or_else(|| state.join("sandbox.db").display().to_string());
    let mut config = ServiceConfig::new(state.join("sandbox-blobs"));
    config.placement = placement_from_env()?;
    let store = store::open(&store_url).await?;
    let service = Service::open(store, config).await?;
    let shutdown = CancellationToken::new();
    tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
    if !args.no_local {
        let agent = Agent::new(
            Arc::new(service.clone()) as Arc<dyn ProviderLink>,
            host_drivers(None, smolvm_slots(), None)?,
            AgentConfig::new(local_provider_name(), state.join("sandbox-agent")),
        );
        tokio::spawn(agent.run(None, shutdown.clone()));
    }
    let auth = api_auth(&state, &args.listen)?;
    let listener = bind_endpoint(&state, args.listen)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    let bound = listener.local_addr()?;
    eprintln!("[preloop] sandbox service on http://{bound}");
    axum::serve(listener, router(service, auth))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    clear_endpoint(&state, bound);
    shutdown.cancel();
    Ok(())
}

async fn run_agent(service: Option<String>, args: AgentArgs) -> anyhow::Result<()> {
    let name = args.name.unwrap_or_else(local_provider_name);
    let state = crate::preloop_home().join("state");
    let service = resolve_service(&state, service)?;
    let provider_token = resolve_token(args.provider_token, &state, "sandbox-provider-token")?;
    let scratch = state.join("sandbox-agent");
    let agent = Agent::new(
        Arc::new(RemoteLink::new(&service, Some(provider_token))),
        host_drivers(None, args.slots, None)?,
        AgentConfig::new(name, scratch),
    );
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        stop.cancel();
    });
    eprintln!("[preloop] sandbox agent joining {service}");
    agent.run(None, shutdown).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn tokens_are_generated_once_and_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path();
        let first = resolve_token(None, state, "sandbox-client-token").unwrap();
        assert_eq!(first.len(), 64, "32 random bytes as hex");
        let path = state.join("sandbox-client-token");
        assert_eq!(mode(&path), 0o600);
        assert_eq!(
            resolve_token(None, state, "sandbox-client-token").unwrap(),
            first,
            "later starts reuse the generated token"
        );
        // A stale wider file is tightened, not trusted as-is.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(
            resolve_token(None, state, "sandbox-client-token").unwrap(),
            first
        );
        assert_eq!(mode(&path), 0o600);
        // A configured token wins and is not written.
        assert_eq!(
            resolve_token(Some("given".into()), state, "other-token").unwrap(),
            "given"
        );
        assert!(!state.join("other-token").exists());
    }

    #[tokio::test]
    async fn endpoint_is_published_on_bind_and_cleared_on_failure() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path();
        let listener = bind_endpoint(state, "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        assert_eq!(read_endpoint(state), Some(format!("http://{addr}")));
        assert_eq!(mode(&endpoint_path(state)), 0o600);

        // The port is still held: the second bind fails and must not leave
        // an endpoint naming the address behind for a caller to trust.
        let error = bind_endpoint(state, addr).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
        assert_eq!(read_endpoint(state), None);

        drop(listener);
        clear_endpoint(state, addr);
    }

    #[test]
    fn client_resolution_never_guesses_an_endpoint() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path();
        // No endpoint file and no explicit --service: refuse rather than
        // send the bearer token to the built-in default.
        assert!(resolve_client(state, None, Some("token".into())).is_err());

        write_endpoint(state, "127.0.0.1:9191".parse().unwrap()).unwrap();
        resolve_token(None, state, "sandbox-client-token").unwrap();
        let (base, token) = resolve_client(state, None, None).unwrap();
        assert_eq!(base, "http://127.0.0.1:9191");
        assert_eq!(token.len(), 64);

        // An explicit --service is trusted even though the file names
        // another address.
        let (base, _) = resolve_client(state, Some("http://other:1234".into()), None).unwrap();
        assert_eq!(base, "http://other:1234");

        // An endpoint without a token is still an error.
        let other = tempfile::tempdir().unwrap();
        write_endpoint(other.path(), "127.0.0.1:9191".parse().unwrap()).unwrap();
        assert!(resolve_client(other.path(), None, None).is_err());
    }

    #[tokio::test]
    async fn the_service_starts_only_for_a_pool() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        assert!(start_if_pool(false, &home).await.unwrap().is_none());
        assert!(
            !home.exists(),
            "no pool must not open the store or touch the state directory"
        );
    }

    #[test]
    fn adapter_and_driver_share_one_provider() {
        let provider = Arc::new(SmolVmProvider::default());
        let driver = smolvm_driver(None, 0, (*provider).clone());
        assert!(
            provider.shares_lifecycle_with(driver.provider()),
            "the adapter's sweep must take the same lifecycle lock as creates"
        );
        assert!(!SmolVmProvider::default().shares_lifecycle_with(driver.provider()));
    }
}
