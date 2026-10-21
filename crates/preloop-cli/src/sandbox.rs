//! `preloop sandbox …` and the engine's embedded Sandbox Service.
//!
//! The engine runs a Sandbox Service in-process with this host's SmolVM
//! driver attached through an in-process host agent; the CI runner pool
//! gets its machines from it. The same service is reachable over HTTPS
//! (Connect) so other hosts can enroll as providers (`preloop sandbox agent`)
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
use preloop_sandbox::server::{Auth, serve as serve_api};
use preloop_sandbox::{
    Driver, SandboxApi, SandboxClient, ServerTls, Service, ServiceConfig, store, tls,
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

/// Drivers attached to this host: `PRELOOP_SANDBOX_DRIVERS`, comma-separated
/// (default `smolvm`). `named_prefix` lets the SmolVM inventory adopt the CI
/// pool's caller-named machines.
pub(crate) fn host_drivers(
    named_prefix: Option<&str>,
    slots: u32,
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
                let mut driver = SmolvmDriver::new(SmolVmProvider::default(), slots);
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
    pub(crate) async fn start(home: &Path) -> anyhow::Result<Self> {
        let state = home.join("state");
        let store_url = env_nonempty("PRELOOP_SANDBOX_STORE_URL")
            .unwrap_or_else(|| state.join("sandbox.db").display().to_string());
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
        let mut config = ServiceConfig::new(state.join("sandbox-blobs"));
        config.ca = Some(tls.ca.clone());
        let service = Service::open(store, config).await?;
        let shutdown = CancellationToken::new();
        tokio::spawn(service.clone().run_maintenance(shutdown.clone()));

        let local = Arc::new(SmolVmProvider::default());
        let prefix =
            env_nonempty("PRELOOP_RUNNER_NAME_PREFIX").unwrap_or_else(|| "preloop-runner".into());
        let drivers = host_drivers(Some(&prefix), smolvm_slots())?;
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
pub(crate) async fn run(args: SandboxArgs) -> anyhow::Result<()> {
    match args.command {
        SandboxCommand::Serve(serve) => serve_standalone(serve).await,
        SandboxCommand::Agent(agent) => run_agent(&args.service, args.token.clone(), agent).await,
        command => {
            let client = sandbox_client(&args.service, args.token.clone(), args.ca.clone())?;
            run_client(client, command).await
        }
    }
}

async fn run_client(client: SandboxClient, command: SandboxCommand) -> anyhow::Result<()> {
    match command {
        SandboxCommand::Serve(_) | SandboxCommand::Agent(_) => {
            unreachable!("serve and agent run without a caller client")
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
        SandboxCommand::Start { sandbox, forkable } => Ok(client.start(&sandbox, forkable).await?),
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
        SandboxCommand::Token(TokenCommand::Create { ttl }) => {
            let ttl = parse_ttl(&ttl)?;
            let response = client.create_join_token(ttl.as_secs()).await?;
            println!("{}", response.token);
            Ok(())
        }
        SandboxCommand::Provider(ProviderCommand::Ls) => {
            print_json(&client.list_providers().await?)
        }
        SandboxCommand::Provider(ProviderCommand::Rm { name }) => {
            Ok(client.delete_provider(&name).await?)
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
    let store = store::open(&store_url).await?;
    let tls = ServerTls::load_or_create(
        &state,
        tls::sans_from_env(),
        env_path("PRELOOP_SANDBOX_TLS_CERT").as_deref(),
        env_path("PRELOOP_SANDBOX_TLS_KEY").as_deref(),
    )?;
    let mut config = ServiceConfig::new(state.join("sandbox-blobs"));
    config.ca = Some(tls.ca.clone());
    let service = Service::open(store, config).await?;
    let shutdown = CancellationToken::new();
    tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
    if !args.no_local {
        let agent = Agent::new(
            Arc::new(service.clone()) as Arc<dyn ProviderLink>,
            host_drivers(None, smolvm_slots())?,
            AgentConfig::new(local_provider_name(), state.join("sandbox-agent")),
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
    let agent = Agent::new(
        Arc::new(link),
        host_drivers(None, args.slots)?,
        AgentConfig::new(name, state.join("sandbox-agent")),
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
