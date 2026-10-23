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

use anyhow::{Context as _, bail};
use clap::{Args, Subcommand};
use futures::StreamExt as _;
use preloop_orchestrator::RunnerPoolConfig;
use preloop_orchestrator::fleet::SandboxMachines;
use preloop_sandbox::agent::{Agent, AgentConfig, HostAccess, ProviderLink, RemoteLink};
use preloop_sandbox::api::*;
use preloop_sandbox::backend::{BackendConfig, SandboxBackend};
use preloop_sandbox::driver_conformance::ConformanceOptions;
use preloop_sandbox::drivers::external::{ExternalDriver, ExternalSpec};
use preloop_sandbox::drivers::fake::FakeDriver;
use preloop_sandbox::drivers::smolvm::SmolvmDriver;
use preloop_sandbox::drivers::smolvm::cli::SmolVmProvider;
use preloop_sandbox::drivers::smolvm::pack::ArtifactCache;
use preloop_sandbox::server::{Auth, HostAllowlist, ListenerSurface, serve_with_options};
use preloop_sandbox::{
    Driver, LeastLoaded, Placement, SandboxApi, SandboxClient, ServerTls, Service, ServiceConfig,
    backend, tls,
};
use tokio_util::sync::CancellationToken;

/// Default address of the engine's sandbox API.
pub(crate) const DEFAULT_SANDBOX_URL: &str = "https://127.0.0.1:9191";

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.trim().is_empty())
}

/// Prefix owned by this host's callers; the driver reports it in inventory.
fn named_prefix() -> String {
    env_nonempty("PRELOOP_RUNNER_NAME_PREFIX").unwrap_or_else(|| "preloop-runner".into())
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
fn service_config(_state: &Path) -> anyhow::Result<ServiceConfig> {
    let mut config = ServiceConfig::new();
    config.peers = ServiceConfig::peers_from_env()?;
    if let Some(value) = env_nonempty("PRELOOP_SANDBOX_NODE_ID") {
        config.node_id = value;
    }
    if let Some(value) = env_nonempty("PRELOOP_SANDBOX_NODE_CREDENTIAL") {
        config.node_credential = value;
    }
    if let Some(value) = env_nonempty("PRELOOP_SANDBOX_PEER_URL") {
        config.peer_url = value;
    }
    if let Some(secs) = env_nonempty("PRELOOP_SANDBOX_NODE_LOST_AFTER_SECS")
        .and_then(|value| value.parse::<u64>().ok())
    {
        config.node_lost_after = Duration::from_secs(secs);
    }
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
    if config.node_session.is_empty() {
        config.node_session = uuid::Uuid::new_v4().simple().to_string();
    }
    Ok(config)
}

/// Open the durable backend for this replica.
///
/// The seal key and CA live outside the database: replicas share them by
/// configuration, so a database reader cannot decrypt sealed environment or
/// transfer material and a node whose key differs refuses at open.
async fn open_backend(
    url: &str,
    state: &Path,
    config: &ServiceConfig,
) -> anyhow::Result<Arc<dyn SandboxBackend>> {
    let pool_size = env_nonempty("PRELOOP_SANDBOX_PG_POOL")
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(8);
    // `None` lets the backend load `PRELOOP_SANDBOX_SEAL_KEY` or a 0600 key
    // file in the state directory (shared by replicas through configuration).
    let seal_key = None;
    let backend = backend::open_with(BackendConfig {
        url: url.to_owned(),
        state_dir: state.to_path_buf(),
        seal_key,
        pool_size,
        node_id: config.node_id.clone(),
        session_id: config.node_session.clone(),
        node_credential_sha256: backend::credential_hash(&config.node_credential),
        peer_url: config.peer_url.clone(),
        node_lost_after: config.node_lost_after,
    })
    .await
    .with_context(|| format!("opening sandbox store {url}"))?;
    Ok(backend)
}

/// Refuse to start when the selected authorization policy is unknown.
///
/// A policy is configuration, not availability: unlike a listener that
/// cannot bind, an unsupported `PRELOOP_SANDBOX_AUTHORIZER` must stop the
/// process instead of leaving it running without the intended policy.
fn ensure_authorizer_config() -> anyhow::Result<()> {
    preloop_sandbox::authz::from_config()
        .map(|_| ())
        .map_err(anyhow::Error::msg)
}

/// Agent tuning: defaults overridden by the documented environment.
///
/// The scratch directory lives under the state directory, not the system
/// temp dir: it holds the durable started-exec fence, which only works if
/// a restart reopens the same directory.
fn sandbox_agent_config(state: &Path, name: impl Into<String>) -> AgentConfig {
    let mut config = AgentConfig::new(name).with_scratch(state.join("sandbox-agent"));
    if let Some(value) = env_nonempty("PRELOOP_SANDBOX_AGENT_CONCURRENCY")
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
    {
        config.concurrency = value;
    }
    config
}
/// Drivers attached to this host: `PRELOOP_SANDBOX_DRIVERS`, comma-separated
/// (default `smolvm`). An `external:<spec>` entry starts a driver plugin (see
/// [`ExternalSpec`]); `runtime_dir` holds their sockets. `named_prefix` lets
/// the SmolVM inventory adopt the CI pool's caller-named machines; `cache`
/// holds the packed artifacts drivers materialize. `local` is the provider an
/// adapter will use for host-side operations; passing it here (as a clone of
/// the same handle) keeps the driver and adapter on one lifecycle lock.
pub(crate) async fn host_drivers(
    named_prefix: Option<&str>,
    slots: u32,
    cache: &ArtifactCache,
    local: Option<SmolVmProvider>,
    runtime_dir: &Path,
) -> anyhow::Result<Vec<Arc<dyn Driver>>> {
    let names = env_nonempty("PRELOOP_SANDBOX_DRIVERS").unwrap_or_else(|| "smolvm".into());
    let entries: Vec<&str> = names
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .collect();
    configured_driver_names(&entries)?;
    let mut drivers: Vec<Arc<dyn Driver>> = Vec::new();
    for entry in entries {
        if let Some(spec) = entry.strip_prefix("external:") {
            let spec = ExternalSpec::parse(spec)
                .with_context(|| format!("PRELOOP_SANDBOX_DRIVERS entry `{entry}`"))?;
            drivers.push(
                ExternalDriver::start(spec, runtime_dir)
                    .await
                    .with_context(|| format!("starting external driver from `{entry}`"))?,
            );
            continue;
        }
        drivers.push(match entry {
            "smolvm" => {
                let driver = smolvm_driver(
                    named_prefix,
                    slots,
                    local.clone().unwrap_or_default(),
                    cache,
                );
                Arc::new(driver) as Arc<dyn Driver>
            }
            // Downstream builds register additional backends here.
            other => anyhow::bail!("unknown sandbox driver `{other}` in PRELOOP_SANDBOX_DRIVERS"),
        });
    }
    Ok(drivers)
}

/// Registration names of the configured entries, validated before any plugin
/// is started.
///
/// Drivers are addressed by name, so two entries that register the same one
/// would leave a driver unreachable. Name both raw entries — that is what the
/// operator has to edit — rather than only the resulting driver.
fn configured_driver_names(entries: &[&str]) -> anyhow::Result<Vec<String>> {
    let mut names = Vec::with_capacity(entries.len());
    let mut seen: std::collections::HashMap<String, &str> = std::collections::HashMap::new();
    for entry in entries {
        let driver_name = match entry.strip_prefix("external:") {
            Some(spec) => {
                ExternalSpec::parse(spec)
                    .with_context(|| format!("PRELOOP_SANDBOX_DRIVERS entry `{entry}`"))?
                    .name
            }
            None => (*entry).to_owned(),
        };
        if let Some(previous) = seen.insert(driver_name.clone(), entry) {
            anyhow::bail!(
                "duplicate sandbox driver name `{driver_name}` in PRELOOP_SANDBOX_DRIVERS: \
                 `{previous}` and `{entry}` both register it; give each entry a unique name"
            );
        }
        names.push(driver_name);
    }
    Ok(names)
}

fn smolvm_driver(
    named_prefix: Option<&str>,
    slots: u32,
    provider: SmolVmProvider,
    cache: &ArtifactCache,
) -> SmolvmDriver {
    let mut driver = SmolvmDriver::new(provider, slots).with_cache(cache.clone());
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
    /// Held for this process's lifetime; releases the state directory.
    _lock: ServiceLock,
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
        ensure_authorizer_config()?;
        // One destructive authority per state directory: a bake or a second
        // engine must not load and reap a running service's leases.
        let lock = ServiceLock::acquire(&state)?;
        let store_url = env_nonempty("PRELOOP_SANDBOX_STORE_URL")
            .unwrap_or_else(|| state.join("sandbox.db").display().to_string());
        let mut config = service_config(&state)?;
        config.placement = placement_from_env()?;
        let store = open_backend(&store_url, &state, &config)
            .await
            .with_context(|| format!("opening sandbox store {store_url}"))?;
        let listen =
            env_nonempty("PRELOOP_SANDBOX_LISTEN").unwrap_or_else(|| "127.0.0.1:9191".into());
        let plan = ListenerPlan::from_values(
            &listen,
            env_nonempty("PRELOOP_SANDBOX_AGENT_LISTEN").as_deref(),
            env_nonempty("PRELOOP_SANDBOX_LISTEN_ALL_INTERFACES").as_deref() == Some("1"),
        )?;
        let listener_tls = listener_tls(&state, &plan).await?;
        config.ca = Some(listener_tls.ca.clone());
        let metrics = preloop_sandbox::metrics::SandboxMetrics::from_meter(&observability.meter());
        config.metrics = metrics.clone();
        let service = Service::open(store, config).await?;
        let shutdown = CancellationToken::new();
        tokio::spawn(service.clone().run_maintenance(shutdown.clone()));

        let local = Arc::new(SmolVmProvider::default());
        let prefix = named_prefix();
        let drivers = host_drivers(
            Some(&prefix),
            smolvm_slots(),
            &ArtifactCache::from_env(state.join("sandbox-cache")),
            Some((*local).clone()),
            &state.join("driver-sockets"),
        )
        .await?;
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
            sandbox_agent_config(&state, local_provider_name()).with_access(access);
        agent_config.metrics = metrics;
        let agent = Agent::try_new(
            Arc::new(service.clone()) as Arc<dyn ProviderLink>,
            drivers,
            agent_config,
        )
        .context("configuring this host's sandbox drivers")?;
        // Register once here so the pool can place immediately; the agent
        // reuses this session instead of registering a second time (which
        // would fence its own in-flight work).
        let session = agent
            .register()
            .await
            .context("registering the local sandbox provider")?;
        let provider_id = session.provider_id.clone();
        tokio::spawn(agent.run(Some(session), shutdown.clone()));

        let token = resolve_client_token(&state)?;
        if let Err(error) = start_listeners(
            &state,
            service.clone(),
            token,
            &plan,
            shutdown.clone(),
            false,
            listener_tls,
        )
        .await
        {
            tracing::warn!(%error, "sandbox API not served; the pool still works in-process");
        }
        // Wait for the local agent so the pool's first acquire can place.
        // An enrollment record (no drivers yet) does not count.
        for _ in 0..200 {
            let registered = service
                .list_providers()
                .await
                .map(|providers| {
                    providers
                        .iter()
                        .any(|provider| !provider.drivers.is_empty())
                })
                .unwrap_or(false);
            if registered {
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
            _lock: lock,
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

/// SIGINT and SIGTERM handlers, installed up front.
///
/// The service manager that stops this process (launchd, systemd, a
/// container runtime) sends SIGTERM; Ctrl-C sends SIGINT. Both must run the
/// same graceful shutdown, or a SIGTERM kills the process before spawned
/// driver plugins are terminated and leaves them orphaned.
#[cfg(unix)]
struct ShutdownSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl ShutdownSignals {
    fn install() -> Self {
        use tokio::signal::unix::{SignalKind, signal};
        Self {
            interrupt: signal(SignalKind::interrupt()).expect("install SIGINT handler"),
            terminate: signal(SignalKind::terminate()).expect("install SIGTERM handler"),
        }
    }

    async fn wait(&mut self) {
        tokio::select! {
            _ = self.interrupt.recv() => {}
            _ = self.terminate.recv() => {}
        }
    }
}

/// Wait until the process is asked to stop (SIGINT or SIGTERM).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        ShutdownSignals::install().wait().await;
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
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
            // A token file left with a wider mode by an old build or a manual
            // copy must not stay readable by other local users.
            crate::set_private_file_permissions(&path)
                .with_context(|| format!("securing {}", path.display()))?;
            return Ok(token.to_owned());
        }
    }
    let token = tls::random_hex(32);
    tls::write_private(&path, token.as_bytes())
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(token)
}

/// The client API endpoint file: the address the last successful bind
/// published.
pub(crate) const ENDPOINT_FILE: &str = "sandbox-endpoint";
/// The agent API endpoint file, written only in split-listener mode.
pub(crate) const AGENT_ENDPOINT_FILE: &str = "sandbox-agent-endpoint";

fn endpoint_path_named(state: &Path, name: &str) -> PathBuf {
    state.join(name)
}

fn endpoint_path(state: &Path) -> PathBuf {
    endpoint_path_named(state, ENDPOINT_FILE)
}

fn read_token_file(path: &Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|text| text.lines().next().map(str::trim).map(str::to_owned))
        .filter(|base| !base.is_empty())
}

/// The token a local deployment wrote, when there is one.
fn local_client_token(home: &Path) -> Option<String> {
    read_token_file(&home.join("state/sandbox-client-token"))
}

/// The CA a local deployment wrote, when there is one.
fn local_ca(home: &Path) -> Option<PathBuf> {
    let path = home.join("state").join(tls::CA_DIR).join(tls::CA_CERT_FILE);
    path.is_file().then_some(path)
}

fn endpoint_url_named(addr: SocketAddr, scheme: &str) -> String {
    format!("{scheme}://{addr}")
}

/// Publish the address a listener actually bound. Written owner-only so a
/// local caller can trust it as the service it should send a token to; the
/// second line names the owning process so a failed bind can tell a live
/// listener from a stale file.
fn write_endpoint_named(
    state: &Path,
    addr: SocketAddr,
    name: &str,
    scheme: &str,
) -> anyhow::Result<()> {
    let path = endpoint_path_named(state, name);
    tls::write_private(
        &path,
        format!(
            "{}\n{}\n",
            endpoint_url_named(addr, scheme),
            std::process::id()
        )
        .as_bytes(),
    )
    .with_context(|| format!("writing {}", path.display()))
}

#[cfg(test)]
pub(crate) fn write_endpoint(state: &Path, addr: SocketAddr) -> anyhow::Result<()> {
    write_endpoint_named(state, addr, ENDPOINT_FILE, "https")
}

fn read_endpoint_named(state: &Path, name: &str) -> Option<String> {
    std::fs::read_to_string(endpoint_path_named(state, name))
        .ok()
        .and_then(|text| text.lines().next().map(str::trim).map(str::to_owned))
        .filter(|base| !base.is_empty())
}

fn read_endpoint(state: &Path) -> Option<String> {
    read_endpoint_named(state, ENDPOINT_FILE)
}

fn endpoint_owner_pid_named(state: &Path, name: &str) -> Option<i32> {
    std::fs::read_to_string(endpoint_path_named(state, name))
        .ok()?
        .lines()
        .nth(1)?
        .trim()
        .parse()
        .ok()
}

fn endpoint_owner_alive_named(state: &Path, name: &str) -> bool {
    let Some(pid) = endpoint_owner_pid_named(state, name) else {
        return false;
    };
    // SAFETY: `kill` with signal 0 only probes existence.
    #[allow(unsafe_code)]
    let alive = unsafe { libc::kill(pid, 0) } == 0
        || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM);
    alive
}

/// Remove the endpoint file only while it still names `addr` and no live
/// owner has republished it.
fn clear_endpoint_named(state: &Path, addr: SocketAddr, name: &str, scheme: &str) {
    let path = endpoint_path_named(state, name);
    let matches = read_endpoint_named(state, name).as_deref()
        == Some(endpoint_url_named(addr, scheme).as_str());
    let current_owner =
        endpoint_owner_pid_named(state, name) == i32::try_from(std::process::id()).ok();
    if matches && (current_owner || !endpoint_owner_alive_named(state, name)) {
        let _ = std::fs::remove_file(path);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientTlsMode {
    ServiceCa,
    File,
    PlainLoopback,
}

impl ClientTlsMode {
    fn from_env() -> anyhow::Result<Self> {
        match env_nonempty("PRELOOP_SANDBOX_CLIENT_TLS")
            .as_deref()
            .unwrap_or("service-ca")
        {
            "service-ca" => Ok(Self::ServiceCa),
            "file" => Ok(Self::File),
            "plain-loopback" => Ok(Self::PlainLoopback),
            value => bail!(
                "PRELOOP_SANDBOX_CLIENT_TLS must be service-ca, file, or plain-loopback (got `{value}`)"
            ),
        }
    }
}

/// How this process exposes the service: one compatibility listener, or a
/// caller listener plus a separate agent listener.
#[derive(Debug, Clone)]
struct ListenerPlan {
    client: Option<SocketAddr>,
    agent: Option<SocketAddr>,
    mode: ClientTlsMode,
    all_interfaces: bool,
    allowed_hosts: Option<String>,
}

impl ListenerPlan {
    fn from_values(
        client_raw: &str,
        agent_raw: Option<&str>,
        all_interfaces: bool,
    ) -> anyhow::Result<Self> {
        let client = if client_raw.trim().eq_ignore_ascii_case("off") {
            None
        } else {
            Some(parse_listen_addr(client_raw, "PRELOOP_SANDBOX_LISTEN")?)
        };
        let agent = match agent_raw {
            Some(raw) if raw.trim().eq_ignore_ascii_case("off") => None,
            Some(raw) => Some(parse_listen_addr(raw, "PRELOOP_SANDBOX_AGENT_LISTEN")?),
            None => None,
        };
        if client.is_some() && agent.is_some() && client == agent {
            bail!(
                "PRELOOP_SANDBOX_LISTEN and PRELOOP_SANDBOX_AGENT_LISTEN must use different addresses"
            );
        }
        if client.is_none() && agent.is_some() {
            bail!("PRELOOP_SANDBOX_AGENT_LISTEN requires PRELOOP_SANDBOX_LISTEN");
        }
        let mode = ClientTlsMode::from_env()?;
        if mode == ClientTlsMode::PlainLoopback
            && client.is_some_and(|addr| !addr.ip().is_loopback())
        {
            bail!(
                "PRELOOP_SANDBOX_CLIENT_TLS=plain-loopback requires PRELOOP_SANDBOX_LISTEN \
                 to bind to a loopback address"
            );
        }
        if let Some(addr) = client {
            validate_bind_address(addr, all_interfaces)?;
        }
        if let Some(addr) = agent {
            validate_bind_address(addr, all_interfaces)?;
        }
        Ok(Self {
            client,
            agent,
            mode,
            all_interfaces,
            allowed_hosts: std::env::var("PRELOOP_SANDBOX_ALLOWED_HOSTS").ok(),
        })
    }

    fn split(&self) -> bool {
        self.agent.is_some()
    }
}

fn parse_listen_addr(raw: &str, key: &str) -> anyhow::Result<SocketAddr> {
    raw.parse()
        .with_context(|| format!("{key} must be a socket address or `off`"))
}

fn validate_bind_address(addr: SocketAddr, allow_all: bool) -> anyhow::Result<()> {
    if !addr.ip().is_unspecified() {
        return Ok(());
    }
    if !allow_all {
        bail!(
            "refusing to bind sandbox API to all interfaces at {addr}; set \
             PRELOOP_SANDBOX_LISTEN_ALL_INTERFACES=1 or use --listen-all-interfaces"
        );
    }
    tracing::warn!(%addr, "sandbox API is listening on all interfaces");
    Ok(())
}

fn exact_allowed_sans(raw: Option<&str>) -> Vec<String> {
    raw.into_iter()
        .flat_map(|raw| raw.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.starts_with("*."))
        .map(|value| {
            let value = if let Some(rest) = value.strip_prefix('[') {
                rest.split_once(']').map_or(value, |(host, _)| host)
            } else if let Some((host, port)) = value.rsplit_once(':') {
                if !host.contains(':') && port.parse::<u16>().is_ok() {
                    host
                } else {
                    value
                }
            } else {
                value
            };
            value.trim_end_matches('.').to_ascii_lowercase()
        })
        .filter(|value| !value.contains('/') && !value.contains('\\'))
        .fold(Vec::new(), |mut result, value| {
            if !result.contains(&value) {
                result.push(value);
            }
            result
        })
}

async fn detected_magic_dns() -> Option<String> {
    let output = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::process::Command::new("tailscale")
            .args(["status", "--json"])
            .kill_on_drop(true)
            .output(),
    )
    .await
    .ok()?
    .ok()?;
    if !output.status.success() {
        return None;
    }
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    let name = value.get("Self")?.get("DNSName")?.as_str()?;
    let name = name.trim().trim_end_matches('.');
    (!name.is_empty()).then(|| name.to_ascii_lowercase())
}

async fn server_sans(allowed_hosts: Option<&str>, magic_dns: Option<&str>) -> Vec<String> {
    let mut sans = tls::sans_from_env();
    for value in exact_allowed_sans(allowed_hosts) {
        if !sans.iter().any(|san| san.eq_ignore_ascii_case(&value)) {
            sans.push(value);
        }
    }
    if let Some(name) = magic_dns
        && !sans.iter().any(|san| san.eq_ignore_ascii_case(name))
    {
        sans.push(name.to_owned());
    }
    sans
}

struct ListenerTls {
    client: Option<Arc<ServerTls>>,
    agent: Option<Arc<ServerTls>>,
    ca: Arc<preloop_sandbox::ServiceCa>,
}

async fn listener_tls(state: &Path, plan: &ListenerPlan) -> anyhow::Result<ListenerTls> {
    // Replicas of one service share the CA directory by configuration; each
    // keeps its own local home, lock and seal key unless told otherwise.
    let shared_state = env_path("PRELOOP_SANDBOX_CA_DIR");
    let state = shared_state.as_deref().unwrap_or(state);
    let magic_dns = detected_magic_dns().await;
    let sans = server_sans(plan.allowed_hosts.as_deref(), magic_dns.as_deref()).await;
    let agent_tls =
        ServerTls::load_or_create_with_client_auth(state, sans.clone(), None, None, true)
            .context("configuring the sandbox agent TLS")?;
    let ca = agent_tls.ca.clone();
    let client = if plan.mode == ClientTlsMode::PlainLoopback {
        None
    } else if plan.split() {
        match plan.mode {
            ClientTlsMode::ServiceCa => Some(
                ServerTls::load_or_create_with_client_auth(state, sans, None, None, false)
                    .context("configuring the sandbox client TLS")?,
            ),
            ClientTlsMode::File => {
                let cert = env_path("PRELOOP_SANDBOX_CLIENT_TLS_CERT").context(
                    "PRELOOP_SANDBOX_CLIENT_TLS_CERT is required when \
                     PRELOOP_SANDBOX_CLIENT_TLS=file",
                )?;
                let key = env_path("PRELOOP_SANDBOX_CLIENT_TLS_KEY").context(
                    "PRELOOP_SANDBOX_CLIENT_TLS_KEY is required when \
                     PRELOOP_SANDBOX_CLIENT_TLS=file",
                )?;
                Some(
                    ServerTls::load_or_create_with_client_auth(
                        state,
                        sans,
                        Some(&cert),
                        Some(&key),
                        false,
                    )
                    .context("configuring the sandbox client TLS certificate")?,
                )
            }
            ClientTlsMode::PlainLoopback => None,
        }
    } else {
        match plan.mode {
            ClientTlsMode::ServiceCa => Some(agent_tls.clone()),
            ClientTlsMode::File => {
                let cert = env_path("PRELOOP_SANDBOX_CLIENT_TLS_CERT").context(
                    "PRELOOP_SANDBOX_CLIENT_TLS_CERT is required when \
                     PRELOOP_SANDBOX_CLIENT_TLS=file",
                )?;
                let key = env_path("PRELOOP_SANDBOX_CLIENT_TLS_KEY").context(
                    "PRELOOP_SANDBOX_CLIENT_TLS_KEY is required when \
                     PRELOOP_SANDBOX_CLIENT_TLS=file",
                )?;
                Some(
                    ServerTls::load_or_create_with_client_auth(
                        state,
                        sans,
                        Some(&cert),
                        Some(&key),
                        true,
                    )
                    .context("configuring the sandbox client TLS certificate")?,
                )
            }
            ClientTlsMode::PlainLoopback => None,
        }
    };
    Ok(ListenerTls {
        client,
        agent: plan.split().then_some(agent_tls),
        ca,
    })
}

/// Bind the planned listeners, publish their endpoint files and serve the
/// matching surfaces until `shutdown`.
async fn start_listeners(
    state: &Path,
    service: Arc<Service>,
    token: String,
    plan: &ListenerPlan,
    shutdown: CancellationToken,
    strict_bind: bool,
    listener_tls: ListenerTls,
) -> anyhow::Result<()> {
    let magic_dns = detected_magic_dns().await;
    if let Some(addr) = plan.client {
        let scheme = listener_tls.client.as_ref().map_or("http", |_| "https");
        let listener = match bind_endpoint_named(
            state,
            addr,
            ENDPOINT_FILE,
            scheme,
            plan.all_interfaces,
        )
        .await
        {
            Ok(listener) => Some(listener),
            Err(error) if !strict_bind => {
                tracing::warn!(%addr, %error, "sandbox client API not served");
                None
            }
            Err(error) => return Err(error).with_context(|| format!("binding {addr}")),
        };
        if let Some(listener) = listener {
            let bound = listener.local_addr()?;
            let hosts =
                HostAllowlist::from_config(bound, plan.allowed_hosts.as_deref(), magic_dns.clone())
                    .map_err(anyhow::Error::msg)?;
            let auth = Auth::for_listener(token.clone(), bound);
            let service = service.clone();
            let stop = shutdown.clone();
            let tls = listener_tls.client.clone();
            let split = plan.split();
            let endpoint_state = state.to_owned();
            tokio::spawn(async move {
                if let Err(error) = serve_with_options(
                    listener,
                    service,
                    auth,
                    tls,
                    if split {
                        ListenerSurface::Client
                    } else {
                        ListenerSurface::Combined
                    },
                    hosts,
                    stop,
                )
                .await
                {
                    tracing::warn!(%error, "sandbox client API server stopped");
                }
                clear_endpoint_named(&endpoint_state, bound, ENDPOINT_FILE, scheme);
            });
        }
    }
    if let Some(addr) = plan.agent {
        let listener = match bind_endpoint_named(
            state,
            addr,
            AGENT_ENDPOINT_FILE,
            "https",
            plan.all_interfaces,
        )
        .await
        {
            Ok(listener) => Some(listener),
            Err(error) if !strict_bind => {
                tracing::warn!(%addr, %error, "sandbox agent API not served");
                None
            }
            Err(error) => return Err(error).with_context(|| format!("binding {addr}")),
        };
        if let Some(listener) = listener {
            let bound = listener.local_addr()?;
            let service = service.clone();
            let stop = shutdown.clone();
            let tls = listener_tls.agent.clone().expect("split listener TLS");
            let endpoint_state = state.to_owned();
            tokio::spawn(async move {
                if let Err(error) = serve_with_options(
                    listener,
                    service,
                    Auth::new(String::new()),
                    Some(tls),
                    ListenerSurface::Agent,
                    HostAllowlist::disabled(),
                    stop,
                )
                .await
                {
                    tracing::warn!(%error, "sandbox agent API server stopped");
                }
                clear_endpoint_named(&endpoint_state, bound, AGENT_ENDPOINT_FILE, "https");
            });
        }
    }
    Ok(())
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
        None => Ok(SandboxClient::new(service, Some(token))?),
    }
}

/// Exclusive claim on a state directory, held by the one process that may
/// open the sandbox store and run maintenance against it.
#[derive(Debug)]
pub(crate) struct ServiceLock {
    file: std::fs::File,
}

impl ServiceLock {
    pub(crate) fn acquire(state: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(state).with_context(|| format!("creating {}", state.display()))?;
        let path = state.join("sandbox-service.lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("opening {}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd as _;
            // SAFETY: the descriptor is valid for the lifetime of `file`.
            #[allow(unsafe_code)]
            let locked = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if locked != 0 {
                anyhow::bail!(
                    "another preloop process is already serving sandbox state in {}; stop it \
                     (or use a different PRELOOP_HOME) before starting another service or bake",
                    state.display()
                );
            }
        }
        Ok(Self { file })
    }
}

impl Drop for ServiceLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd as _;
            // SAFETY: closing would release the lock as well; unlocking first
            // keeps the intent explicit.
            #[allow(unsafe_code)]
            unsafe {
                libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
            }
        }
    }
}

/// Bind and publish the endpoint, or clear a stale entry naming the address
/// that could not be bound (a squatter must not inherit the CLI's trust).
#[cfg(test)]
pub(crate) async fn bind_endpoint(
    state: &Path,
    addr: SocketAddr,
) -> std::io::Result<tokio::net::TcpListener> {
    bind_endpoint_named(state, addr, ENDPOINT_FILE, "https", false).await
}

/// Bind `addr`, publish it under `name`, or clear a stale entry naming the
/// address that could not be bound (a squatter must not inherit the CLI's
/// trust).
async fn bind_endpoint_named(
    state: &Path,
    addr: SocketAddr,
    name: &str,
    scheme: &str,
    allow_all_interfaces: bool,
) -> std::io::Result<tokio::net::TcpListener> {
    if addr.ip().is_unspecified() && !allow_all_interfaces {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "refusing to bind sandbox API to all interfaces at {addr}; set \
                 PRELOOP_SANDBOX_LISTEN_ALL_INTERFACES=1 or use --listen-all-interfaces"
            ),
        ));
    }
    if addr.ip().is_unspecified() {
        tracing::warn!(%addr, "sandbox API is listening on all interfaces");
    }
    match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => {
            let bound = listener.local_addr()?;
            write_endpoint_named(state, bound, name, scheme).map_err(std::io::Error::other)?;
            Ok(listener)
        }
        Err(error) => {
            // A live owner may legitimately hold the address (the embedded
            // engine, or another `serve`); keep its endpoint file. Only a
            // dead owner's stale file is removed, so a squatter cannot
            // inherit the CLI's trust.
            if !endpoint_owner_alive_named(state, name) {
                clear_endpoint_named(state, addr, name, scheme);
            }
            Err(error)
        }
    }
}

fn resolve_service(state: &Path, service: Option<String>) -> anyhow::Result<String> {
    match service.filter(|service| !service.trim().is_empty()) {
        Some(service) => Ok(service),
        None => read_endpoint(state).with_context(|| {
            format!(
                "no sandbox endpoint file ({}); pass --service (for example {DEFAULT_SANDBOX_URL})",
                endpoint_path(state).display()
            )
        }),
    }
}

/// The embedded service is only needed when a pool will place VMs on it.
pub(crate) async fn start_if_pool(
    pool_configured: bool,
    home: &Path,
    pool: Option<&RunnerPoolConfig>,
    observability: preloop_observability::Observability,
) -> anyhow::Result<Option<LocalSandboxes>> {
    if !pool_configured {
        return Ok(None);
    }
    Ok(Some(
        LocalSandboxes::start(home, pool, observability).await?,
    ))
}

/// `preloop sandbox`.
#[derive(Debug, Args)]
pub(crate) struct SandboxArgs {
    /// Sandbox API base URL (default: the address in `<home>/state/sandbox-endpoint`).
    #[arg(long, env = "PRELOOP_SANDBOX_URL", global = true)]
    service: Option<String>,
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
    /// Driver plugins: serve one over a Unix socket, or run conformance.
    #[command(subcommand)]
    Driver(DriverCommand),
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
        /// Lifetime (`30s`, `10m`, `1h`); default 1h, maximum 24h.
        #[arg(long, default_value = "1h")]
        ttl: String,
        /// Bind the token to this provider name. A bound token enrolls
        /// only that provider and is the only kind that may re-enroll an
        /// existing provider (replacing its certificates). Without
        /// `--name` the token may only enroll a provider name that does
        /// not exist yet.
        #[arg(long)]
        name: Option<String>,
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

#[derive(Debug, Subcommand)]
enum DriverCommand {
    /// Serve an in-process driver as a plugin on a Unix socket.
    Serve(DriverServeArgs),
    /// Run the driver conformance suite against a driver.
    Conformance(DriverConformanceArgs),
}

#[derive(Debug, Args)]
struct DriverServeArgs {
    /// In-process driver to serve: `smolvm` or `fake`.
    #[arg(long, default_value = "smolvm")]
    driver: String,
    /// Unix socket to serve on.
    #[arg(long)]
    socket: PathBuf,
}

#[derive(Debug, Args)]
struct DriverConformanceArgs {
    /// Driver to test: `smolvm`, `fake`, or an external spec
    /// (`external:name=/path/to/plugin [args…]` or
    /// `external:name=unix:///path.sock`). The `external:` prefix may be
    /// omitted.
    #[arg(long)]
    driver: String,
    /// Artifact as comma-separated `key=value` pairs:
    /// `format=oci-image|smolmachine,arch=…,ref=…,digest=sha256:…[,size=…]`.
    /// Repeat for several formats or architectures. Without it the driver
    /// applies its own default template.
    #[arg(long = "artifact", value_name = "KEY=VALUE,…")]
    artifact: Vec<String>,
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
    /// Allow binding the sandbox API to all interfaces.
    #[arg(long, env = "PRELOOP_SANDBOX_LISTEN_ALL_INTERFACES")]
    listen_all_interfaces: bool,
}

#[derive(Debug, Args)]
struct AgentArgs {
    /// Service URL to enroll with (`https://host:9191`). Required on the
    /// first run; the URL is stored with the identity. On later runs this
    /// overrides the stored URL; without it the agent dials the stored one.
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
        SandboxCommand::Agent(agent) => run_agent(token, agent, observability).await,
        SandboxCommand::Driver(driver) => run_driver(driver).await,
        command => {
            let state = crate::preloop_home().join("state");
            let base = resolve_service(&state, service)?;
            let client = sandbox_client(&base, token, ca)?;
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
        SandboxCommand::Serve(_) | SandboxCommand::Agent(_) | SandboxCommand::Driver(_) => {
            unreachable!("serve, agent and driver run without a caller client")
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
                network: if pooled {
                    NetworkPolicy::default().into()
                } else {
                    NetworkPolicy {
                        mode: NetworkMode::UNRESTRICTED.into(),
                        ..NetworkPolicy::default()
                    }
                    .into()
                },
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
        SandboxCommand::Token(TokenCommand::Create { ttl, name }) => {
            let ttl = parse_ttl(&ttl)?;
            let response = client
                .create_join_token(ttl.as_secs(), name.as_deref().unwrap_or_default())
                .await?;
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
    ensure_authorizer_config()?;
    let _lock = ServiceLock::acquire(&state)?;
    let store_url = args
        .store
        .unwrap_or_else(|| state.join("sandbox.db").display().to_string());
    let mut config = service_config(&state)?;
    config.placement = placement_from_env()?;
    let store = open_backend(&store_url, &state, &config).await?;
    let listen = args.listen.to_string();
    let plan = ListenerPlan::from_values(
        &listen,
        env_nonempty("PRELOOP_SANDBOX_AGENT_LISTEN").as_deref(),
        args.listen_all_interfaces,
    )?;
    let listener_tls = listener_tls(&state, &plan).await?;
    config.ca = Some(listener_tls.ca.clone());
    let metrics = preloop_sandbox::metrics::SandboxMetrics::from_meter(&observability.meter());
    config.metrics = metrics.clone();
    let service = Service::open(store, config).await?;
    let shutdown = CancellationToken::new();
    tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
    if !args.no_local {
        let mut agent_config = sandbox_agent_config(&state, local_provider_name());
        agent_config.metrics = metrics;
        let agent = Agent::try_new(
            Arc::new(service.clone()) as Arc<dyn ProviderLink>,
            host_drivers(
                Some(&named_prefix()),
                smolvm_slots(),
                &ArtifactCache::from_env(state.join("sandbox-cache")),
                None,
                &state.join("driver-sockets"),
            )
            .await?,
            agent_config.with_access(HostAccess::from_env()),
        )
        .context("configuring this host's sandbox drivers")?;
        tokio::spawn(agent.run(None, shutdown.clone()));
    }
    if let Some(addr) = plan.client {
        let scheme = listener_tls.client.as_ref().map_or("http", |_| "https");
        eprintln!("[preloop] sandbox service on {scheme}://{addr}");
    }
    let token = resolve_client_token(&state)?;
    let stop = shutdown.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        stop.cancel();
    });
    start_listeners(
        &state,
        service,
        token,
        &plan,
        shutdown.clone(),
        true,
        listener_tls,
    )
    .await?;
    // The listeners run in their own tasks; keep this process alive until a
    // signal cancels them, or `serve` would return as soon as it has bound.
    shutdown.cancelled().await;
    Ok(())
}

/// Resolve the link `sandbox agent` runs on.
///
/// `--join` is the only override. Without it the service URL recorded with
/// the identity wins: the global `--service` default is for caller commands
/// and must not silently point a restarted agent at `127.0.0.1:9191`.
async fn agent_link(
    identity_dir: &Path,
    join: Option<&str>,
    join_token: Option<&str>,
    name: &str,
) -> anyhow::Result<RemoteLink> {
    Ok(RemoteLink::connect(identity_dir, join, join_token, name).await?)
}

async fn run_agent(
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
    let join_token = args.join_token.or(client_token);
    let link = agent_link(
        &identity_dir,
        args.join.as_deref(),
        join_token.as_deref(),
        &initial_name,
    )
    .await?;
    // An existing identity owns the name it was enrolled under.
    let name = link.name().unwrap_or(initial_name);
    let base = link.base().to_owned();
    let mut agent_config = sandbox_agent_config(&state, name).with_access(HostAccess::from_env());
    agent_config.metrics =
        preloop_sandbox::metrics::SandboxMetrics::from_meter(&observability.meter());
    let agent = Agent::try_new(
        Arc::new(link),
        host_drivers(
            None,
            args.slots,
            &ArtifactCache::from_env(state.join("sandbox-cache")),
            None,
            &state.join("driver-sockets"),
        )
        .await?,
        agent_config,
    )
    .context("configuring this host's sandbox drivers")?;
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        stop.cancel();
    });
    eprintln!("[preloop] sandbox agent joining {base}");
    agent.run(None, shutdown).await;
    Ok(())
}

/// An in-process backend for the driver commands.
fn in_process_driver(name: &str, state: &Path) -> anyhow::Result<Arc<dyn Driver>> {
    match name {
        "smolvm" => Ok(Arc::new(
            SmolvmDriver::new(SmolVmProvider::default(), smolvm_slots())
                .with_cache(ArtifactCache::from_env(state.join("sandbox-cache"))),
        )),
        "fake" => {
            let root = state.join("driver-fake");
            std::fs::create_dir_all(&root)
                .with_context(|| format!("creating {}", root.display()))?;
            Ok(Arc::new(FakeDriver::new(
                "fake",
                root,
                FakeDriver::full_capabilities(),
            )))
        }
        other => anyhow::bail!("`{other}` is not an in-process driver; use `smolvm` or `fake`"),
    }
}

/// Conformance options for the `driver conformance` command.
///
/// Without `--artifact`, the driver applies its own default template. With
/// artifacts, they form a `conformance` template so a real VM driver boots a
/// digest-pinned image instead of whatever happens to be the default.
fn conformance_options(artifact: &[String]) -> anyhow::Result<ConformanceOptions> {
    if artifact.is_empty() {
        return Ok(ConformanceOptions::default());
    }
    let artifacts = artifact
        .iter()
        .map(|raw| parse_artifact(raw))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(ConformanceOptions::new(Template {
        spec: TemplateSpec {
            name: "conformance".to_owned(),
            os: "linux".to_owned(),
            artifacts,
            ..TemplateSpec::default()
        }
        .into(),
        ..Template::default()
    }))
}

/// `preloop sandbox driver …`.
async fn run_driver(command: DriverCommand) -> anyhow::Result<()> {
    let state = crate::preloop_home().join("state");
    match command {
        DriverCommand::Serve(args) => {
            let driver = in_process_driver(&args.driver, &state)?;
            let shutdown = CancellationToken::new();
            let stop = shutdown.clone();
            tokio::spawn(async move {
                shutdown_signal().await;
                stop.cancel();
            });
            eprintln!(
                "[preloop] serving driver `{}` on unix://{}",
                driver.name(),
                args.socket.display()
            );
            preloop_sandbox::driver_plugin::serve(driver, &args.socket, shutdown).await?;
            Ok(())
        }
        DriverCommand::Conformance(args) => {
            let driver = match args.driver.as_str() {
                "smolvm" | "fake" => in_process_driver(&args.driver, &state)?,
                raw => {
                    let spec = ExternalSpec::parse(raw).with_context(|| {
                        format!(
                            "`{raw}` is not an in-process driver (`smolvm`, `fake`) and not a \
                             valid external spec"
                        )
                    })?;
                    ExternalDriver::start(spec, &state.join("driver-sockets")).await?
                }
            };
            eprintln!("[preloop] driver conformance: {}", driver.name());
            let options = conformance_options(&args.artifact)?;
            let results = preloop_sandbox::driver_conformance::run(driver, &options).await;
            let mut failed: Vec<&str> = Vec::new();
            let mut skipped = 0usize;
            println!("{:<28} {:<6} detail", "case", "result");
            for case in &results {
                if case.skipped {
                    skipped += 1;
                    println!("{:<28} {:<6}", case.name, "skip");
                    continue;
                }
                match &case.outcome {
                    Ok(()) => println!("{:<28} {:<6}", case.name, "pass"),
                    Err(error) => {
                        println!(
                            "{:<28} {:<6} {}",
                            case.name,
                            "fail",
                            error.replace('\n', " ")
                        );
                        failed.push(case.name);
                    }
                }
            }
            println!(
                "\n{} passed, {} failed, {} skipped",
                results.len() - failed.len() - skipped,
                failed.len(),
                skipped
            );
            if !failed.is_empty() {
                anyhow::bail!("driver conformance failed: {}", failed.join(", "));
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_scratch_is_pinned_under_the_state_directory() {
        let state = Path::new("/var/lib/preloop/state");
        let config = sandbox_agent_config(state, "host-a");
        assert_eq!(config.scratch, state.join("sandbox-agent"));
    }

    #[test]
    fn driver_conformance_parses_repeated_artifacts() {
        use clap::Parser as _;
        let digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
        let oci = format!(
            "format=oci-image,arch=aarch64,ref=ghcr.io/acme/image@{digest},digest={digest}"
        );
        let pack = format!("format=smolmachine,arch=aarch64,ref=file:///tmp/pack,digest={digest}");
        let cli = crate::Cli::try_parse_from([
            "preloop",
            "sandbox",
            "driver",
            "conformance",
            "--driver",
            "smolvm",
            "--artifact",
            &oci,
            "--artifact",
            &pack,
        ])
        .unwrap();
        let crate::Command::Sandbox(args) = cli.command else {
            panic!("expected the sandbox command");
        };
        let SandboxCommand::Driver(DriverCommand::Conformance(args)) = args.command else {
            panic!("expected driver conformance");
        };
        assert_eq!(args.driver, "smolvm");
        assert_eq!(args.artifact, vec![oci, pack]);

        let options = conformance_options(&args.artifact).unwrap();
        let spec = options
            .template
            .spec
            .into_option()
            .expect("artifacts build a template");
        assert_eq!(spec.name, "conformance");
        assert_eq!(spec.artifacts.len(), 2);
        assert_eq!(spec.artifacts[0].format, Artifact::OCI_IMAGE);
        assert_eq!(spec.artifacts[1].format, Artifact::SMOLMACHINE);

        let default = conformance_options(&[]).unwrap();
        assert!(default.template.spec.into_option().is_none());
    }

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

    use preloop_sandbox::tls::{AgentIdentity, IdentityMetadata, ServiceCa, generate_csr};

    fn stored_identity(root: &Path, base: &str) -> AgentIdentity {
        let ca = ServiceCa::load_or_create(&root.join("ca")).unwrap();
        let (key_pem, csr) = generate_csr("host-a").unwrap();
        let signed = ca
            .sign_client(&csr, "host-a", tls::CLIENT_CERT_LIFETIME)
            .unwrap();
        let identity = AgentIdentity {
            dir: root.join("identity"),
            metadata: IdentityMetadata {
                name: "host-a".into(),
                base: base.into(),
                ca_sha256: ca.sha256_hex().to_owned(),
                issued_at: 1,
                expires_at: 2,
            },
            ca_pem: ca.pem().to_owned(),
            cert_pem: signed.cert_pem,
            key_pem,
        };
        identity.save().unwrap();
        identity
    }

    #[tokio::test]
    async fn restart_without_join_reuses_the_stored_service_url() {
        let dir = tempfile::tempdir().unwrap();
        let identity = stored_identity(dir.path(), "https://stored.example:9191");
        let link = agent_link(&identity.dir, None, None, "host-a")
            .await
            .unwrap();
        assert_eq!(link.base(), "https://stored.example:9191");
    }

    #[tokio::test]
    async fn join_overrides_the_stored_service_url() {
        let dir = tempfile::tempdir().unwrap();
        let identity = stored_identity(dir.path(), "https://stored.example:9191");
        let link = agent_link(
            &identity.dir,
            Some("https://other.example:9191"),
            None,
            "host-a",
        )
        .await
        .unwrap();
        assert_eq!(link.base(), "https://other.example:9191");
        let reloaded = AgentIdentity::load(&identity.dir).unwrap().unwrap();
        assert_eq!(reloaded.metadata.base, "https://other.example:9191");
    }

    #[tokio::test]
    async fn a_first_run_without_join_asks_for_one() {
        let dir = tempfile::tempdir().unwrap();
        let error = agent_link(dir.path(), None, None, "host-a")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("--join"), "{error}");
    }

    /// Set for the re-executed test binary that waits on [`ShutdownSignals`].
    const SIGTERM_CHILD_ENV: &str = "PRELOOP_TEST_SANDBOX_SIGTERM_CHILD";

    #[test]
    fn duplicate_driver_entries_name_both_entries() {
        let names = configured_driver_names(&["smolvm"]).unwrap();
        assert_eq!(names, ["smolvm"]);

        let names =
            configured_driver_names(&["smolvm", "external:smolvm-ext=/usr/bin/plugin"]).unwrap();
        assert_eq!(names, ["smolvm", "smolvm-ext"]);

        let error =
            configured_driver_names(&["smolvm", "external:smolvm=/usr/local/bin/other-smolvm"])
                .unwrap_err()
                .to_string();
        assert!(
            error.contains("duplicate sandbox driver name `smolvm`"),
            "{error}"
        );
        assert!(error.contains("`smolvm` and `external:smolvm="), "{error}");
    }

    /// The child half of [`sigterm_resolves_the_shutdown_future`]: install the
    /// handlers, report readiness, then wait. A SIGTERM with the default
    /// disposition would kill this process instead of letting it return.
    #[cfg(unix)]
    #[test]
    fn sigterm_child_waits_for_shutdown() {
        if std::env::var_os(SIGTERM_CHILD_ENV).is_none() {
            return;
        }
        use std::io::Write as _;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            // Signal streams need the runtime's reactor.
            let mut signals = ShutdownSignals::install();
            println!("ready");
            std::io::stdout().flush().unwrap();
            tokio::time::timeout(Duration::from_secs(30), signals.wait())
                .await
                .expect("SIGTERM must resolve the shutdown future");
        });
    }

    #[cfg(unix)]
    fn mode(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[cfg(unix)]
    #[test]
    fn sigterm_resolves_the_shutdown_future() {
        use std::io::{BufRead as _, BufReader};
        let exe = std::env::current_exe().unwrap();
        let mut child = std::process::Command::new(exe)
            .args([
                "--exact",
                "sandbox::tests::sigterm_child_waits_for_shutdown",
                "--nocapture",
            ])
            .env(SIGTERM_CHILD_ENV, "1")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();

        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let stdout = child.stdout.take().unwrap();
        std::thread::spawn(move || {
            // Keep draining after readiness: closing the read end would make
            // the child's own test harness exit on EPIPE instead of reporting
            // the result we assert on.
            let mut announced = false;
            for line in BufReader::new(stdout).lines() {
                if !announced && line.unwrap_or_default().contains("ready") {
                    announced = true;
                    let _ = ready_tx.send(());
                }
            }
        });
        ready_rx
            .recv_timeout(Duration::from_secs(15))
            .expect("the child installs its handlers and reports readiness");

        // SAFETY: the child is alive and waiting; SIGTERM is the signal a
        // service manager sends on stop.
        let killed = unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
        assert_eq!(killed, 0, "SIGTERM delivery");
        let status = child.wait().unwrap();
        let mut stderr = String::new();
        if let Some(mut pipe) = child.stderr.take() {
            use std::io::Read as _;
            let _ = pipe.read_to_string(&mut stderr);
        }
        assert!(
            status.success(),
            "the child must exit cleanly on SIGTERM, not die from it: {status}\n{stderr}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn client_token_is_generated_once_and_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path();
        let first = resolve_client_token(state).unwrap();
        assert_eq!(first.len(), 64, "32 random bytes as hex");
        let path = state.join("sandbox-client-token");
        assert_eq!(mode(&path), 0o600);
        assert_eq!(
            resolve_client_token(state).unwrap(),
            first,
            "later starts reuse the generated token"
        );
        // A stale wider file is tightened, not trusted as-is.
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(resolve_client_token(state).unwrap(), first);
        assert_eq!(mode(&path), 0o600);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn endpoint_is_published_over_https_and_keeps_a_live_owners_file() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path();
        let listener = bind_endpoint(state, "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        assert_eq!(read_endpoint(state), Some(format!("https://{addr}")));
        assert_eq!(mode(&endpoint_path(state)), 0o600);

        // The port is still held by a live process: a second bind fails but
        // must not delete the endpoint other CLI calls depend on.
        let error = bind_endpoint(state, addr).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
        assert_eq!(read_endpoint(state), Some(format!("https://{addr}")));

        // A stale file owned by a dead process is removed on failed bind.
        tls::write_private(
            &endpoint_path(state),
            format!("https://{addr}\n{}\n", u32::MAX).as_bytes(),
        )
        .unwrap();
        let error = bind_endpoint(state, addr).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
        assert_eq!(read_endpoint(state), None);

        drop(listener);
    }

    #[test]
    fn the_state_directory_admits_one_service() {
        let dir = tempfile::tempdir().unwrap();
        let first = ServiceLock::acquire(dir.path()).unwrap();
        let error = ServiceLock::acquire(dir.path()).unwrap_err();
        assert!(error.to_string().contains("already serving"), "{error}");
        drop(first);
        ServiceLock::acquire(dir.path()).unwrap();
    }

    #[tokio::test]
    async fn the_service_starts_only_for_a_pool() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        assert!(
            start_if_pool(
                false,
                &home,
                None,
                preloop_observability::Observability::noop()
            )
            .await
            .unwrap()
            .is_none()
        );
        assert!(
            !home.exists(),
            "no pool must not open the store or touch the state directory"
        );
    }

    #[test]
    fn client_resolution_never_guesses_an_endpoint() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path();
        // No endpoint file and no explicit --service: refuse rather than
        // send the bearer token to the built-in default.
        assert!(resolve_service(state, None).is_err());

        write_endpoint(state, "127.0.0.1:9191".parse().unwrap()).unwrap();
        assert_eq!(
            resolve_service(state, None).unwrap(),
            "https://127.0.0.1:9191"
        );

        // An explicit --service is trusted even though the file names
        // another address.
        assert_eq!(
            resolve_service(state, Some("https://other:1234".into())).unwrap(),
            "https://other:1234"
        );
    }

    #[test]
    fn adapter_and_driver_share_one_provider() {
        let provider = Arc::new(SmolVmProvider::default());
        let driver = smolvm_driver(
            None,
            0,
            (*provider).clone(),
            &ArtifactCache::from_env(std::env::temp_dir().join("sandbox-cache-test")),
        );
        assert!(
            provider.shares_lifecycle_with(driver.provider()),
            "the adapter's sweep must take the same lifecycle lock as creates"
        );
        assert!(!SmolVmProvider::default().shares_lifecycle_with(driver.provider()));
    }
}
