//! `preloop sandbox …` and the engine's embedded Sandbox Service.
//!
//! The engine runs a Sandbox Service in-process with this host's SmolVM
//! driver attached through an in-process host agent; the CI runner pool
//! gets its machines from it. The same service is reachable over HTTP
//! (Connect) so other hosts can join as providers (`preloop sandbox agent`)
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

/// Service config with the blob cap from `PRELOOP_SANDBOX_BLOB_MAX_BYTES`.
fn service_config(state: &Path) -> anyhow::Result<ServiceConfig> {
    let mut config = ServiceConfig::new(state.join("sandbox-blobs"));
    if let Some(raw) = env_nonempty("PRELOOP_SANDBOX_BLOB_MAX_BYTES") {
        config.blob_max_bytes = raw.parse().with_context(|| {
            format!("PRELOOP_SANDBOX_BLOB_MAX_BYTES must be a byte count, got `{raw}`")
        })?;
    }
    Ok(config)
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
        let mut agent_config = AgentConfig::new(local_provider_name(), state.join("sandbox-agent"))
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
            let auth = api_auth(&addr)?;
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    tracing::info!(%addr, "sandbox API listening");
                    let app = router(service.clone(), auth);
                    let stop = shutdown.clone();
                    tokio::spawn(async move {
                        let _ = axum::serve(listener, app)
                            .with_graceful_shutdown(async move { stop.cancelled().await })
                            .await;
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

/// Tokens are optional only on loopback.
fn api_auth(addr: &SocketAddr) -> anyhow::Result<Auth> {
    let auth = Auth {
        client_token: env_nonempty("PRELOOP_SANDBOX_TOKEN"),
        provider_token: env_nonempty("PRELOOP_SANDBOX_PROVIDER_TOKEN"),
    };
    if !addr.ip().is_loopback() && (auth.client_token.is_none() || auth.provider_token.is_none()) {
        anyhow::bail!(
            "the sandbox API on {addr} is not loopback: set PRELOOP_SANDBOX_TOKEN and \
             PRELOOP_SANDBOX_PROVIDER_TOKEN"
        );
    }
    Ok(auth)
}

/// `preloop sandbox`.
#[derive(Debug, Args)]
pub(crate) struct SandboxArgs {
    /// Sandbox API base URL.
    #[arg(long, env = "PRELOOP_SANDBOX_URL", default_value = DEFAULT_SANDBOX_URL, global = true)]
    service: String,
    /// Bearer token for the Sandboxes API.
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
    /// Cordon, uncordon or drain a provider.
    #[command(subcommand)]
    Provider(ProviderCommand),
    /// Per-provider host sample, capacity and sandboxes with their runtime facts.
    Stats,
}

#[derive(Debug, Subcommand)]
enum ProviderCommand {
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
            "ref" => artifact.reference = value.trim().to_owned(),
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
        lease_id,
        command,
    } = args;
    let client = SandboxClient::new(service.clone(), token);
    let lease = lease_id.unwrap_or_default();
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
        SandboxCommand::Serve(serve) => serve_standalone(serve, observability).await,
        SandboxCommand::Agent(agent) => run_agent(&service, agent, observability).await,
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
                network: NetworkPolicy {
                    mode: NetworkMode::Unrestricted,
                    ..NetworkPolicy::default()
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
                    lease_id: lease_id()?,
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
        SandboxCommand::Cp(cp) => copy(&client, &cp.source, &cp.destination, &lease_id()?).await,
        SandboxCommand::Stop { sandbox } => Ok(client.stop(&sandbox, &lease_id()?).await?),
        SandboxCommand::Start { sandbox, forkable } => {
            Ok(client.start(&sandbox, &lease_id()?, forkable).await?)
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
                    lease_id: lease_id()?,
                    name: snapshot.name,
                })
                .await?,
        ),
        SandboxCommand::Ls => print_json(&client.list().await?),
        SandboxCommand::Providers => print_json(&client.list_providers().await?),
        SandboxCommand::Provider(provider) => match provider {
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
    let metrics = preloop_sandbox::metrics::SandboxMetrics::from_meter(&observability.meter());
    config.metrics = metrics.clone();
    let service = Service::open(store, config).await?;
    let shutdown = CancellationToken::new();
    tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
    if !args.no_local {
        let mut agent_config = AgentConfig::new(local_provider_name(), state.join("sandbox-agent"));
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
    let auth = api_auth(&args.listen)?;
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    eprintln!("[preloop] sandbox service on http://{}", args.listen);
    axum::serve(listener, router(service, auth))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    shutdown.cancel();
    Ok(())
}

async fn run_agent(
    service: &str,
    args: AgentArgs,
    observability: preloop_observability::Observability,
) -> anyhow::Result<()> {
    let name = args.name.unwrap_or_else(local_provider_name);
    let state = crate::preloop_home().join("state");
    let scratch = state.join("sandbox-agent");
    let mut agent_config = AgentConfig::new(name, scratch).with_access(HostAccess::from_env());
    agent_config.metrics =
        preloop_sandbox::metrics::SandboxMetrics::from_meter(&observability.meter());
    let agent = Agent::new(
        Arc::new(RemoteLink::new(service, args.provider_token)),
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
    eprintln!("[preloop] sandbox agent joining {service}");
    agent.run(shutdown).await;
    Ok(())
}
