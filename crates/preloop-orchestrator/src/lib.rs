//! SmolVM-backed ephemeral runner pool for Preloop CI.

pub mod environment;

use crate::environment::{EnvironmentResolver, EnvironmentSpec};
use aksh_gha_protocol::RUNNER_BUSY_SENTINEL;

/// Line an ephemeral runner prints when it accepts a job. Re-exported so a
/// `VmProvider` implementation can model the handshake this pool relies on.
pub use aksh_gha_protocol::RUNNER_BUSY_SENTINEL as RUNNER_BUSY_LINE;

use preloop_vm::{
    MachineName, MachineSpec, MachineState, NetworkPolicy, OutputChunk, SmolVmProvider,
    SocketMount, VmError, VmProvider, VolumeMount,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const GUEST_CONTROL_DIR: &str = "/run/preloop-control";
const GUEST_CONTROL_SOCKET: &str = "/run/preloop-control/engine.sock";
const GUEST_FAILURE_MARKER: &str = "/var/lib/preloop-runner/.preloop-job-failed";

/// How long a preserved VM survives with nobody attached.
const DEBUG_IDLE_TIMEOUT: Duration = Duration::from_secs(600);
/// How often the preserved VM re-checks the debug marker.
const DEBUG_POLL_INTERVAL: Duration = Duration::from_secs(10);
/// Marker mtime newer than this counts as an active `preloop shell` session.
/// Must exceed the CLI heartbeat interval.
const DEBUG_HEARTBEAT_WINDOW: Duration = Duration::from_secs(30);

/// Debug marker contents written by the orchestrator when it parks a failed VM.
pub const DEBUG_MARKER_IDLE: &str = "preserved";
/// Debug marker contents written by `preloop shell` while a session is live.
///
/// The orchestrator only extends the idle deadline for a marker in this state,
/// so its own initial write cannot masquerade as a heartbeat.
pub const DEBUG_MARKER_ACTIVE: &str = "active";

fn control_bridge_dir(config: &RunnerPoolConfig) -> Option<PathBuf> {
    config
        .control_socket
        .as_deref()
        .and_then(Path::parent)
        .map(|parent| parent.join("control-bridge"))
}

fn runner_volumes(config: &RunnerPoolConfig) -> Vec<VolumeMount> {
    let mut volumes = vec![VolumeMount {
        host: config.runner_bundle.clone(),
        guest: PathBuf::from("/opt/preloop/bin"),
        read_only: true,
    }];
    if let Some(host) = control_bridge_dir(config) {
        volumes.push(VolumeMount {
            host,
            guest: PathBuf::from(GUEST_CONTROL_DIR),
            read_only: false,
        });
    }
    volumes
}

const BASE_PACKAGES: &str = "git curl ca-certificates nodejs";

fn base_install_commands() -> Vec<Vec<String>> {
    // One shell round trip instead of several: every `exec` is a host process
    // spawn plus a vsock round trip, and this runs on the engine's start-up
    // critical path.
    [vec![
        "sh".to_owned(),
        "-c".to_owned(),
        format!(
            "apt-get update -qq && \
             apt-get install -y -qq --no-install-recommends {BASE_PACKAGES}"
        ),
    ]]
    .into_iter()
    .collect()
}

/// How long to wait for a freshly started guest to accept commands.
const GUEST_READY_TIMEOUT: Duration = Duration::from_secs(30);
/// Gap between guest readiness probes.
const GUEST_READY_POLL: Duration = Duration::from_millis(25);

/// Block until the guest agent executes a trivial command.
///
/// `machine start` returns once the agent marker appears, but the guest can
/// still refuse the first `exec`. Polling costs one round trip when the guest
/// is already up, where a fixed sleep charged every boot for the worst case.
async fn await_guest_ready<P: VmProvider>(
    provider: &P,
    name: &MachineName,
) -> Result<(), OrchestratorError> {
    let deadline = tokio::time::Instant::now() + GUEST_READY_TIMEOUT;
    let probe = ["true".to_owned()];
    loop {
        match provider.exec(name, &probe).await {
            Ok(_) => return Ok(()),
            Err(error) if tokio::time::Instant::now() >= deadline => {
                return Err(OrchestratorError::from(error))
            }
            Err(_) => tokio::time::sleep(GUEST_READY_POLL).await,
        }
    }
}

async fn install_base_dependencies<P: VmProvider>(
    provider: &P,
    name: &MachineName,
) -> Result<(), OrchestratorError> {
    for command in base_install_commands() {
        provider.exec(name, &command).await?;
    }
    Ok(())
}

/// `env` prefix for guest runner invocations, empty when nothing needs setting.
///
/// Control-socket routing and failure-marker debugging are independent
/// features: a pool can debug failed jobs without a mounted control socket and
/// vice versa, so neither may gate the other.
fn guest_env_prefix(config: &RunnerPoolConfig) -> Vec<String> {
    let mut env = Vec::new();
    if config.control_socket.is_some() {
        env.push(format!(
            "PRELOOP_CONTROL_ORIGIN={}",
            config.server_url.trim_end_matches('/')
        ));
        env.push(format!("PRELOOP_CONTROL_SOCKET={GUEST_CONTROL_SOCKET}"));
    }
    if config.debug_dir.is_some() {
        env.push(format!("PRELOOP_FAILURE_MARKER={GUEST_FAILURE_MARKER}"));
    }
    if !env.is_empty() {
        env.insert(0, "/usr/bin/env".to_owned());
    }
    env
}

/// Local ephemeral-runner pool configuration.
#[derive(Debug, Clone)]
pub struct RunnerPoolConfig {
    /// Number of runners polling concurrently.
    pub size: usize,
    /// Use a forkable golden VM as a fork base for instant runner creation.
    /// When enabled, a single "golden" VM boots once and each runner slot
    /// clones from it with CoW memory and disks.
    pub use_fork: bool,
    /// Prefix used for owned SmolVM names.
    pub name_prefix: String,
    /// Base OCI image used for one-time tool installation.
    pub base_image: String,
    /// Optional workspace path for environment detection from version files.
    pub workspace: Option<PathBuf>,
    /// Host path stem for the reusable packed VM artifact.
    pub artifact_stem: PathBuf,
    /// Host directory containing the Linux `preloop-runner` executable.
    pub runner_bundle: PathBuf,
    /// Runner executable filename within `runner_bundle`.
    pub runner_binary_name: String,
    /// Guest-visible control-plane URL.
    pub server_url: String,
    /// Host Unix socket used for runner control-plane traffic.
    pub control_socket: Option<PathBuf>,
    /// Host environment variable containing the registration credential.
    pub registration_token_env: String,
    /// Runner labels advertised to the scheduler.
    pub labels: Vec<String>,
    /// vCPUs per runner.
    pub cpus: u16,
    /// Memory per runner in MiB.
    pub memory_mib: u32,
    /// Storage per runner in GiB.
    pub storage_gib: u32,
    /// Directory for debug session markers (e.g. `~/.preloop/state/debug`).
    ///
    /// When set, a runner whose job requested `preserve_on_failure` and then
    /// failed is held open for interactive debugging. Whether any individual
    /// job opts in is decided per run by the control plane, not here.
    pub debug_dir: Option<PathBuf>,
}

/// Cache of environment-specific golden VMs.
pub(crate) struct GoldenRegistry {
    goldens: RwLock<HashMap<String, MachineName>>,
    name_prefix: String,
}

impl GoldenRegistry {
    pub fn new(name_prefix: String) -> Self {
        Self {
            goldens: RwLock::new(HashMap::new()),
            name_prefix,
        }
    }

    /// Get existing golden or return None if not yet prepared.
    pub async fn get(&self, fingerprint: &str) -> Option<MachineName> {
        self.goldens.read().await.get(fingerprint).cloned()
    }

    /// Register a prepared golden VM for a fingerprint.
    pub async fn insert(&self, fingerprint: String, name: MachineName) {
        self.goldens.write().await.insert(fingerprint, name);
    }

    /// Remove and return a golden VM entry.
    #[allow(dead_code)]
    pub async fn remove(&self, fingerprint: &str) -> Option<MachineName> {
        self.goldens.write().await.remove(fingerprint)
    }

    /// Return all registered golden machine names.
    pub async fn all_names(&self) -> Vec<MachineName> {
        self.goldens.read().await.values().cloned().collect()
    }
}

impl RunnerPoolConfig {
    /// Validate configuration before changing machine state.
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        if self.size == 0 || self.size > 64 {
            return Err(OrchestratorError::Config(
                "runner pool size must be between 1 and 64".into(),
            ));
        }
        MachineName::new(format!("{}-0", self.name_prefix))?;
        if self.base_image.trim().is_empty() || self.server_url.trim().is_empty() {
            return Err(OrchestratorError::Config(
                "base image and server URL are required".into(),
            ));
        }
        if !self.runner_bundle.is_absolute() || !self.runner_bundle.is_dir() {
            return Err(OrchestratorError::Config(format!(
                "runner bundle does not exist: {}",
                self.runner_bundle.display()
            )));
        }
        if self.runner_binary_name.contains('/') || self.runner_binary_name.is_empty() {
            return Err(OrchestratorError::Config(
                "runner binary name must be a filename".into(),
            ));
        }
        if let Some(socket) = &self.control_socket {
            if !socket.is_absolute() || !socket.exists() {
                return Err(OrchestratorError::Config(format!(
                    "control socket does not exist: {}",
                    socket.display()
                )));
            }
            let bridge = control_bridge_dir(self).expect("control socket has a parent");
            if !bridge.is_dir() {
                return Err(OrchestratorError::Config(format!(
                    "control bridge directory does not exist: {}",
                    bridge.display()
                )));
            }
        }
        if std::env::var_os(&self.registration_token_env).is_none() {
            return Err(OrchestratorError::Config(format!(
                "registration token environment variable `{}` is not set",
                self.registration_token_env
            )));
        }
        Ok(())
    }

    fn artifact_payload(&self) -> PathBuf {
        let mut value = self.artifact_stem.as_os_str().to_owned();
        value.push(".smolmachine");
        PathBuf::from(value)
    }
}

/// Runner-pool lifecycle error.
#[derive(Debug, Error)]
pub enum OrchestratorError {
    /// Invalid pool configuration.
    #[error("invalid runner pool configuration: {0}")]
    Config(String),
    /// VM provider failure.
    #[error(transparent)]
    Vm(#[from] VmError),
    /// Host filesystem failure.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// One or more runner slots exited unexpectedly.
    #[error("runner pool stopped unexpectedly: {0}")]
    Pool(String),
}

/// Supervises disposable one-job runners backed by a reusable packed VM image.
pub struct RunnerPool<P: VmProvider = SmolVmProvider> {
    provider: Arc<P>,
    config: RunnerPoolConfig,
}

impl<P: VmProvider + 'static> RunnerPool<P> {
    /// Construct a runner pool.
    pub fn new(provider: Arc<P>, config: RunnerPoolConfig) -> Result<Self, OrchestratorError> {
        config.validate()?;
        Ok(Self { provider, config })
    }

    /// Prepare the immutable runner image once, then supervise all slots until cancellation.
    pub async fn run(&self, shutdown: CancellationToken) -> Result<(), OrchestratorError> {
        // SmolVM currently drops socket mounts from `machine create --from`.
        // Local control-plane sockets therefore use an image-backed golden VM;
        // remote TCP pools can retain the packed-artifact path.
        if self.config.control_socket.is_none() {
            self.prepare_artifact().await?;
        }
        self.remove_stale_machines().await?;

        let resolver = Arc::new(EnvironmentResolver::new(self.config.base_image.clone()));
        let golden_registry = Arc::new(GoldenRegistry::new(self.config.name_prefix.clone()));

        // If fork mode is enabled, prepare a golden fork base VM for the
        // default (base-image-only) environment. Its fingerprint is the
        // canonical hash of the base image with an empty toolchain list.
        if self.config.use_fork {
            let default_environment =
                EnvironmentSpec::new(self.config.base_image.clone(), Vec::new());
            let golden = MachineName::new(format!("{}-golden", golden_registry.name_prefix))?;
            if let Err(error) = self
                .prepare_golden_for_env(&golden, &default_environment)
                .await
            {
                warn!(%error, "golden fork base unavailable; falling back to create-per-runner");
            } else {
                golden_registry
                    .insert(default_environment.fingerprint, golden)
                    .await;
            }
        }

        let mut slots = JoinSet::new();
        // Runners currently registered and waiting for work. Slots consult it
        // to decide whether a replacement is worth booting mid-job.
        let idle = Arc::new(AtomicUsize::new(0));
        for slot in 0..self.config.size {
            let provider = self.provider.clone();
            let config = self.config.clone();
            let slot_shutdown = shutdown.child_token();
            let slot_registry = golden_registry.clone();
            let slot_resolver = resolver.clone();
            let slot_idle = idle.clone();
            slots.spawn(async move {
                run_slot(
                    provider,
                    config,
                    slot,
                    slot_shutdown,
                    slot_registry,
                    slot_resolver,
                    slot_idle,
                )
                .await
            });
        }

        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {}
            result = slots.join_next() => {
                shutdown.cancel();
                match result {
                    Some(Ok(Err(error))) => return Err(error),
                    Some(Err(error)) => return Err(OrchestratorError::Pool(error.to_string())),
                    Some(Ok(Ok(()))) => return Err(OrchestratorError::Pool("runner slot exited".into())),
                    None => return Err(OrchestratorError::Pool("runner pool had no slots".into())),
                }
            }
        }

        while slots.join_next().await.is_some() {}
        // Clean up every environment-specific golden fork base.
        for golden in golden_registry.all_names().await {
            let _ = self.provider.delete(&golden).await;
        }
        self.remove_stale_machines().await?;
        Ok(())
    }

    async fn prepare_artifact(&self) -> Result<(), OrchestratorError> {
        let payload = self.config.artifact_payload();
        if payload.is_file() {
            return Ok(());
        }
        if let Some(parent) = self.config.artifact_stem.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let name = MachineName::new(format!("{}-builder", self.config.name_prefix))?;
        if self.provider.status(&name).await? != MachineState::Missing {
            self.provider.delete(&name).await?;
        }
        let spec = MachineSpec {
            name: name.clone(),
            image: self.config.base_image.clone(),
            cpus: self.config.cpus,
            memory_mib: self.config.memory_mib,
            storage_gib: self.config.storage_gib,
            network: NetworkPolicy::PublicOnly,
            volumes: Vec::new(),
            sockets: Vec::new(),
        };
        self.provider.create(&spec).await?;
        self.provider.start(&name).await?;
        if let Err(error) = install_base_dependencies(self.provider.as_ref(), &name).await {
            let _ = self.provider.delete(&name).await;
            return Err(error);
        }
        self.provider.stop(&name).await?;
        self.provider
            .pack(&name, &self.config.artifact_stem)
            .await?;
        self.provider.delete(&name).await?;
        if !payload.is_file() {
            return Err(OrchestratorError::Config(format!(
                "smolvm did not create expected artifact {}",
                payload.display()
            )));
        }
        Ok(())
    }

    /// Prepare a running forkable golden VM from the packed artifact and
    /// install the requested environment toolchains.
    /// The golden VM is booted but NOT registered as a runner — each fork
    /// gets its own unique identity via configure.
    async fn prepare_golden_for_env(
        &self,
        golden: &MachineName,
        env_spec: &EnvironmentSpec,
    ) -> Result<(), OrchestratorError> {
        if self.provider.status(golden).await? != MachineState::Missing {
            self.provider.delete(golden).await?;
        }
        let spec = MachineSpec {
            name: golden.clone(),
            image: self.config.base_image.clone(),
            cpus: self.config.cpus,
            memory_mib: self.config.memory_mib,
            storage_gib: self.config.storage_gib,
            network: NetworkPolicy::PublicOnly,
            volumes: runner_volumes(&self.config),
            sockets: self
                .config
                .control_socket
                .iter()
                .map(|host| SocketMount {
                    host: host.clone(),
                    guest: PathBuf::from(GUEST_CONTROL_SOCKET),
                })
                .collect(),
        };
        self.provider.create(&spec).await?;
        self.provider.start_forkable(golden).await?;
        if let Err(error) = await_guest_ready(self.provider.as_ref(), golden).await {
            let _ = self.provider.delete(golden).await;
            return Err(error);
        }
        if let Err(error) = install_base_dependencies(self.provider.as_ref(), golden).await {
            let _ = self.provider.delete(golden).await;
            return Err(error);
        }
        for layer in &env_spec.toolchains {
            for command in layer.install_commands() {
                if let Err(error) = self.provider.exec(golden, &command).await {
                    let _ = self.provider.delete(golden).await;
                    return Err(error.into());
                }
            }
        }
        info!(machine = golden.as_str(), "golden fork base ready");
        Ok(())
    }

    async fn remove_stale_machines(&self) -> Result<(), OrchestratorError> {
        for name in self.provider.list().await? {
            if name
                .as_str()
                .starts_with(&format!("{}-", self.config.name_prefix))
            {
                if let Err(error) = self.provider.delete(&name).await {
                    warn!(machine = name.as_str(), %error, "failed to delete stale Preloop runner");
                }
            }
        }
        Ok(())
    }
}

/// A provisioned, registered runner waiting to be handed a job.
#[derive(Debug)]
struct ReadyRunner {
    name: MachineName,
    run: Vec<String>,
}

/// What a slot needs in order to build its next runner.
struct SlotPlan<'a> {
    /// Pool slot index, used to name machines.
    slot: usize,
    /// Generation for the replacement machine name.
    generation: u64,
    /// Fork base, when the pool has one.
    golden: Option<&'a MachineName>,
    /// Runners across the whole pool that are registered and unclaimed.
    idle: &'a AtomicUsize,
}
async fn run_slot<P: VmProvider + 'static>(
    provider: Arc<P>,
    config: RunnerPoolConfig,
    slot: usize,
    shutdown: CancellationToken,
    golden_registry: Arc<GoldenRegistry>,
    _environment_resolver: Arc<EnvironmentResolver>,
    idle: Arc<AtomicUsize>,
) -> Result<(), OrchestratorError> {
    let default_fingerprint =
        EnvironmentSpec::new(config.base_image.clone(), Vec::new()).fingerprint;
    let mut generation: u64 = 0;
    let mut spare: Option<ReadyRunner> = None;

    while !shutdown.is_cancelled() {
        // Until jobs are connected to environment selection, all slots use
        // the default base-only golden. An absent entry preserves the
        // create-per-runner fallback used by non-fork mode and tests.
        let golden = golden_registry.get(&default_fingerprint).await;
        let runner = match spare.take() {
            Some(runner) => runner,
            None => {
                generation += 1;
                match provision_slot(&provider, &config, slot, generation, golden.as_ref()).await {
                    Ok(runner) => runner,
                    Err(error) => {
                        warn!(slot, %error, "provisioning runner failed; retrying");
                        tokio::select! {
                            _ = shutdown.cancelled() => break,
                            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                        }
                        continue;
                    }
                }
            }
        };

        generation += 1;
        let successor = run_one_runner(
            provider.clone(),
            &config,
            runner,
            shutdown.clone(),
            SlotPlan {
                slot,
                generation,
                golden: golden.as_ref(),
                idle: &idle,
            },
        )
        .await;
        spare = match successor {
            Ok(spare) => spare,
            Err(error) => {
                warn!(slot, %error, "ephemeral runner failed; replenishing slot");
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                }
                None
            }
        };
    }

    if let Some(spare) = spare {
        let _ = provider.delete(&spare.name).await;
    }
    Ok(())
}

/// Provision one ephemeral runner for a slot under a fresh machine name.
///
/// Names carry a generation so a replacement can boot while its predecessor is
/// still being torn down; reusing one name per slot forced those to serialize.
async fn provision_slot<P: VmProvider + 'static>(
    provider: &Arc<P>,
    config: &RunnerPoolConfig,
    slot: usize,
    generation: u64,
    golden: Option<&MachineName>,
) -> Result<ReadyRunner, OrchestratorError> {
    let name = MachineName::new(format!("{}-{slot}-{generation}", config.name_prefix))?;
    match provision_runner(provider, config, &name, golden).await {
        Ok(run) => Ok(ReadyRunner { name, run }),
        Err(error) => {
            if let Err(cleanup) = provider.delete(&name).await {
                warn!(
                    machine = name.as_str(),
                    %cleanup,
                    "failed to delete machine after provisioning error"
                );
            }
            Err(error)
        }
    }
}

/// Run one job on a provisioned runner, building its replacement in parallel.
///
/// The runner is single-use, so the moment it announces that it has taken a
/// job its successor can start booting. That moves fork + configure — the bulk
/// of a slot's turnaround — off the path of whatever job arrives next, which is
/// what a matrix workflow deeper than the pool spends its time waiting on.
///
/// Returns the replacement when one was built, so the caller can use it
/// immediately instead of provisioning again.
async fn run_one_runner<P: VmProvider + 'static>(
    provider: Arc<P>,
    config: &RunnerPoolConfig,
    runner: ReadyRunner,
    shutdown: CancellationToken,
    plan: SlotPlan<'_>,
) -> Result<Option<ReadyRunner>, OrchestratorError> {
    let ReadyRunner { name, run } = runner;
    let name = &name;
    let SlotPlan {
        slot,
        generation: next_generation,
        golden,
        idle,
    } = plan;

    let (busy_tx, busy_rx) = tokio::sync::oneshot::channel();
    let run_provider = provider.clone();
    let run_name = name.clone();
    idle.fetch_add(1, Ordering::AcqRel);
    let mut run_task =
        tokio::spawn(async move { run_until_exit(&run_provider, &run_name, &run, busy_tx).await });

    // Resolves once the runner reports a job and its replacement is ready. A
    // runner that exits without taking a job (shutdown, transient failure)
    // drops the sender, and this yields `None` without provisioning anything.
    let build_successor = async {
        if busy_rx.await.is_err() {
            idle.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        // Booting a VM costs real CPU, and it would be spent alongside the job
        // that just started. Only pay that while the job runs when the pool has
        // nothing left to hand out — otherwise an idle peer already covers the
        // next arrival and the replacement can wait for teardown.
        if idle.fetch_sub(1, Ordering::AcqRel) > 1 {
            return None;
        }
        match provision_slot(&provider, config, slot, next_generation, golden).await {
            Ok(successor) => Some(successor),
            Err(error) => {
                warn!(slot, %error, "pre-provisioning the replacement runner failed");
                None
            }
        }
    };

    let (result, successor) = tokio::select! {
        _ = shutdown.cancelled() => {
            // Killing the host-side `smolvm machine exec` process does not
            // terminate the guest command. Abort the wrapper first, then stop
            // the VM so deletion cannot wait indefinitely on a live listener.
            run_task.abort();
            let _ = run_task.await;
            (provider.stop(name).await.map_err(OrchestratorError::from), None)
        },
        pair = async {
            // Concurrent on purpose: the successor is built while the job is
            // still running, which is the whole point of the busy signal.
            tokio::join!(&mut run_task, build_successor)
        } => {
            let result = match pair.0 {
                Ok(result) => result.map_err(OrchestratorError::from),
                Err(error) => Err(OrchestratorError::Pool(error.to_string())),
            };
            (result, pair.1)
        },
    };

    // The runner writes this marker only when the job it ran opted in via
    // `preserve_on_failure` and then genuinely failed, so preservation is
    // decided per run rather than by engine-wide configuration.
    let preserved = match &config.debug_dir {
        Some(debug_dir)
            if provider
                .exec(
                    name,
                    &["test".into(), "-f".into(), GUEST_FAILURE_MARKER.into()],
                )
                .await
                .is_ok() =>
        {
            Some(debug_dir.clone())
        }
        _ => None,
    };

    if let Some(debug_dir) = preserved {
        hold_for_debugging(name, &debug_dir, &shutdown).await;
        if let Err(error) = provider.delete(name).await {
            warn!(machine = name.as_str(), %error, "failed to delete preserved machine");
        }
        return finish(&provider, result, successor).await;
    }

    // Report the runner's own failure in preference to a teardown failure.
    let delete_result = provider.delete(name).await.map_err(OrchestratorError::from);
    finish(&provider, result.and(delete_result), successor).await
}

/// Hand the replacement back, or discard it if this runner is failing.
///
/// A pre-provisioned successor owns a live VM. Returning early on the runner's
/// error would drop the handle and strand that machine until the pool next
/// swept stale names, so failure paths delete it explicitly.
async fn finish<P: VmProvider + 'static>(
    provider: &Arc<P>,
    result: Result<(), OrchestratorError>,
    successor: Option<ReadyRunner>,
) -> Result<Option<ReadyRunner>, OrchestratorError> {
    match result {
        Ok(()) => Ok(successor),
        Err(error) => {
            if let Some(successor) = successor {
                if let Err(cleanup) = provider.delete(&successor.name).await {
                    warn!(
                        machine = successor.name.as_str(),
                        %cleanup,
                        "failed to delete the replacement runner of a failed slot"
                    );
                }
            }
            Err(error)
        }
    }
}

/// Run the guest runner to completion, signalling the first job it accepts.
///
/// Streaming rather than buffering the guest's output is what makes the busy
/// signal observable while the job is still running; it also drops SmolVM's
/// 30-second buffered-exec read timeout.
async fn run_until_exit<P: VmProvider + 'static>(
    provider: &Arc<P>,
    name: &MachineName,
    run: &[String],
    busy: tokio::sync::oneshot::Sender<()>,
) -> Result<(), VmError> {
    let (chunks, mut receiver) = mpsc::channel(64);
    let watcher = tokio::spawn(async move {
        let mut busy = Some(busy);
        let mut pending = String::new();
        while let Some(chunk) = receiver.recv().await {
            let OutputChunk::Stdout(bytes) = chunk else {
                continue;
            };
            if busy.is_none() {
                continue;
            }
            pending.push_str(&String::from_utf8_lossy(&bytes));
            if pending.contains(RUNNER_BUSY_SENTINEL) {
                if let Some(busy) = busy.take() {
                    let _ = busy.send(());
                }
                pending.clear();
            } else if pending.len() > 2 * RUNNER_BUSY_SENTINEL.len() {
                // Keep only enough tail to rejoin a sentinel split across reads.
                let keep = pending.len() - RUNNER_BUSY_SENTINEL.len();
                pending.drain(..keep);
            }
        }
    });

    let code = provider.exec_stream(name, run, chunks).await?;
    let _ = watcher.await;
    if code == 0 {
        Ok(())
    } else {
        Err(VmError::Command {
            operation: "run",
            exit_code: code,
            message: format!("guest runner exited with code {code}"),
        })
    }
}

/// Create, boot, and register one ephemeral runner; return its `run` argv.
///
/// The caller owns cleanup: on any error the machine may already exist.
async fn provision_runner<P: VmProvider + 'static>(
    provider: &Arc<P>,
    config: &RunnerPoolConfig,
    name: &MachineName,
    golden: Option<&MachineName>,
) -> Result<Vec<String>, OrchestratorError> {
    if let Some(golden) = golden {
        // Fork from the already-booted golden VM — instant CoW clone.
        provider.fork(golden, name).await?;
    } else {
        // Socket mounts are currently ignored by SmolVM's packed-artifact
        // create path, so local pools fall back to the base image directly.
        let uses_control_socket = config.control_socket.is_some();
        let spec = MachineSpec {
            name: name.clone(),
            image: if uses_control_socket {
                config.base_image.clone()
            } else {
                config.artifact_payload().display().to_string()
            },
            cpus: config.cpus,
            memory_mib: config.memory_mib,
            storage_gib: config.storage_gib,
            network: NetworkPolicy::PublicOnly,
            volumes: runner_volumes(config),
            sockets: config
                .control_socket
                .iter()
                .map(|host| SocketMount {
                    host: host.clone(),
                    guest: PathBuf::from(GUEST_CONTROL_SOCKET),
                })
                .collect(),
        };
        provider.create(&spec).await?;
        provider.start(name).await?;
        if uses_control_socket {
            install_base_dependencies(provider.as_ref(), name).await?;
        }
    }

    let runner = format!("/opt/preloop/bin/{}", config.runner_binary_name);
    let labels = config.labels.join(",");
    let mut configure = guest_env_prefix(config);
    configure.extend([
        runner.clone(),
        "configure".into(),
        "--url".into(),
        config.server_url.clone(),
        "--name".into(),
        name.as_str().into(),
        "--labels".into(),
        labels,
        "--runner-root".into(),
        "/var/lib/preloop-runner".into(),
        "--unattended".into(),
        "--replace".into(),
        "--ephemeral".into(),
        "--no-externals".into(),
    ]);
    provider
        .exec_with_secret_env(
            name,
            &configure,
            &[(
                "PRELOOP_RUNNER_TOKEN".into(),
                config.registration_token_env.clone(),
            )],
        )
        .await?;

    info!(machine = name.as_str(), "ephemeral runner ready");
    let mut run = guest_env_prefix(config);
    run.extend([
        runner,
        "run".into(),
        "--once".into(),
        "--runner-root".into(),
        "/var/lib/preloop-runner".into(),
    ]);
    Ok(run)
}

/// Hold a failed runner's VM open so `preloop shell` can attach.
///
/// The marker file is the session handle: `preloop shell` refreshes its mtime
/// while attached and removes it on exit, which releases the slot immediately
/// instead of stranding it until the idle deadline.
async fn hold_for_debugging(name: &MachineName, debug_dir: &Path, shutdown: &CancellationToken) {
    let marker = debug_dir.join(name.as_str());
    if let Err(error) =
        std::fs::create_dir_all(debug_dir).and_then(|()| std::fs::write(&marker, DEBUG_MARKER_IDLE))
    {
        warn!(
            machine = name.as_str(),
            path = %marker.display(),
            %error,
            "cannot record debug marker — deleting VM instead of preserving it"
        );
        return;
    }

    warn!(
        machine = name.as_str(),
        timeout_secs = DEBUG_IDLE_TIMEOUT.as_secs(),
        "job failed — VM preserved for debugging; attach with `preloop shell`"
    );

    let mut deadline = tokio::time::Instant::now() + DEBUG_IDLE_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            info!(
                machine = name.as_str(),
                "debug idle timeout expired — deleting preserved VM"
            );
            break;
        }
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(remaining.min(DEBUG_POLL_INTERVAL)) => {}
        }
        let Ok(state) = std::fs::read_to_string(&marker) else {
            // `preloop shell` removed the marker: the session is over.
            info!(
                machine = name.as_str(),
                "debug session ended — deleting preserved VM"
            );
            break;
        };
        // Only a live `preloop shell` heartbeat extends the window. Matching on
        // mtime alone would let this function's own initial write renew it.
        if state.trim() == DEBUG_MARKER_ACTIVE
            && std::fs::metadata(&marker)
                .and_then(|meta| meta.modified())
                .ok()
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age < DEBUG_HEARTBEAT_WINDOW)
        {
            deadline = tokio::time::Instant::now() + DEBUG_IDLE_TIMEOUT;
        }
    }
    let _ = std::fs::remove_file(&marker);
}

/// Return the runner artifact payload generated for an output stem.
pub fn artifact_payload(stem: &Path) -> PathBuf {
    let mut value = stem.as_os_str().to_owned();
    value.push(".smolmachine");
    PathBuf::from(value)
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use async_trait::async_trait;
    use preloop_vm::{ExecOutput, OutputChunk};
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    #[derive(Debug)]
    struct TestProvider {
        machines: Mutex<HashMap<String, MachineState>>,
        events: Mutex<Vec<String>>,
        fail_start: bool,
        fail_install: bool,
        fail_configure: bool,
        fail_run: bool,
        fail_delete: bool,
    }

    impl TestProvider {
        fn new(
            fail_start: bool,
            fail_install: bool,
            fail_configure: bool,
            fail_run: bool,
            fail_delete: bool,
        ) -> Self {
            Self {
                machines: Mutex::new(HashMap::new()),
                events: Mutex::new(Vec::new()),
                fail_start,
                fail_install,
                fail_configure,
                fail_run,
                fail_delete,
            }
        }

        async fn has_machine(&self, name: &MachineName) -> bool {
            self.machines.lock().await.contains_key(name.as_str())
        }

        async fn events(&self) -> Vec<String> {
            self.events.lock().await.clone()
        }
    }

    fn test_error(message: &'static str) -> VmError {
        VmError::Command {
            operation: "lifecycle-test",
            exit_code: 1,
            message: message.to_owned(),
        }
    }

    fn test_output() -> ExecOutput {
        ExecOutput {
            exit_code: 0,
            stdout: Vec::new(),
            stderr: Vec::new(),
            truncated: false,
        }
    }

    fn test_config(control_socket: bool) -> RunnerPoolConfig {
        RunnerPoolConfig {
            size: 1,
            use_fork: false,
            name_prefix: "lifecycle-test".to_owned(),
            base_image: "base-image".to_owned(),
            workspace: None,
            artifact_stem: PathBuf::from("/tmp/lifecycle-artifact"),
            runner_bundle: PathBuf::from("/tmp"),
            runner_binary_name: "runner".to_owned(),
            server_url: "https://runner.test".to_owned(),
            control_socket: control_socket.then(|| PathBuf::from("/tmp/engine.sock")),
            registration_token_env: "LIFECYCLE_TEST_TOKEN".to_owned(),
            labels: vec!["test".to_owned()],
            cpus: 1,
            memory_mib: 128,
            storage_gib: 1,
            debug_dir: None,
        }
    }

    #[async_trait]
    impl VmProvider for TestProvider {
        async fn create(&self, spec: &MachineSpec) -> Result<(), VmError> {
            self.machines
                .lock()
                .await
                .insert(spec.name.as_str().to_owned(), MachineState::Stopped);
            self.events
                .lock()
                .await
                .push(format!("create:{}", spec.name.as_str()));
            Ok(())
        }

        async fn start(&self, name: &MachineName) -> Result<(), VmError> {
            self.events
                .lock()
                .await
                .push(format!("start:{}", name.as_str()));
            if self.fail_start {
                return Err(test_error("start-failure"));
            }
            self.machines
                .lock()
                .await
                .insert(name.as_str().to_owned(), MachineState::Running);
            Ok(())
        }

        async fn start_forkable(&self, name: &MachineName) -> Result<(), VmError> {
            self.start(name).await
        }

        async fn fork(&self, _golden: &MachineName, clone: &MachineName) -> Result<(), VmError> {
            self.events
                .lock()
                .await
                .push(format!("create:{}", clone.as_str()));
            self.machines
                .lock()
                .await
                .insert(clone.as_str().to_owned(), MachineState::Running);
            Ok(())
        }

        async fn stop(&self, name: &MachineName) -> Result<(), VmError> {
            self.machines
                .lock()
                .await
                .insert(name.as_str().to_owned(), MachineState::Stopped);
            Ok(())
        }

        async fn delete(&self, name: &MachineName) -> Result<(), VmError> {
            self.events
                .lock()
                .await
                .push(format!("delete:{}", name.as_str()));
            if self.fail_delete {
                return Err(test_error("delete-failure"));
            }
            self.machines.lock().await.remove(name.as_str());
            Ok(())
        }

        async fn status(&self, name: &MachineName) -> Result<MachineState, VmError> {
            Ok(self
                .machines
                .lock()
                .await
                .get(name.as_str())
                .copied()
                .unwrap_or(MachineState::Missing))
        }

        async fn list(&self) -> Result<Vec<MachineName>, VmError> {
            Ok(Vec::new())
        }

        async fn exec(&self, name: &MachineName, argv: &[String]) -> Result<ExecOutput, VmError> {
            self.events
                .lock()
                .await
                .push(format!("exec:{}:{:?}", name.as_str(), argv));
            if self.fail_install && argv.iter().any(|arg| arg.contains("apt-get")) {
                return Err(test_error("install-failure"));
            }
            if self.fail_run && argv.iter().any(|arg| arg == "run") {
                return Err(test_error("run-failure"));
            }
            Ok(test_output())
        }

        async fn exec_with_secret_env(
            &self,
            name: &MachineName,
            _argv: &[String],
            _secrets: &[(String, String)],
        ) -> Result<ExecOutput, VmError> {
            self.events
                .lock()
                .await
                .push(format!("configure:{}", name.as_str()));
            if self.fail_configure {
                return Err(test_error("configure-failure"));
            }
            Ok(test_output())
        }

        async fn exec_stream(
            &self,
            name: &MachineName,
            argv: &[String],
            _output: tokio::sync::mpsc::Sender<OutputChunk>,
        ) -> Result<i32, VmError> {
            self.events
                .lock()
                .await
                .push(format!("run:{}", name.as_str()));
            if self.fail_run && argv.iter().any(|arg| arg == "run") {
                return Err(test_error("run-failure"));
            }
            Ok(0)
        }

        async fn copy(&self, _source: &str, _destination: &str) -> Result<(), VmError> {
            Ok(())
        }

        async fn pack(&self, _name: &MachineName, _output: &Path) -> Result<(), VmError> {
            Ok(())
        }
    }

    async fn provisioning_failure(
        provider: Arc<TestProvider>,
        config: &RunnerPoolConfig,
        golden: Option<&MachineName>,
        expected: &str,
    ) {
        let error = provision_slot(&provider, config, 0, 1, golden)
            .await
            .expect_err("provisioning failure must propagate");
        let name = MachineName::new(format!("{}-0-1", config.name_prefix)).unwrap();
        assert!(error.to_string().contains(expected));
        assert!(!provider.has_machine(&name).await);
        let events = provider.events().await;
        let create = events
            .iter()
            .position(|event| event == &format!("create:{}", name.as_str()))
            .expect("machine creation event");
        assert!(events[create + 1..]
            .iter()
            .any(|event| event == &format!("delete:{}", name.as_str())));
    }

    #[tokio::test]
    async fn provisioning_failures_delete_created_runner() {
        let cases = [
            (
                TestProvider::new(true, false, false, false, false),
                false,
                "start-failure",
            ),
            (
                TestProvider::new(false, true, false, false, false),
                true,
                "install-failure",
            ),
            (
                TestProvider::new(false, false, true, false, false),
                false,
                "configure-failure",
            ),
        ];
        for (provider, control_socket, expected) in cases {
            provisioning_failure(
                Arc::new(provider),
                &test_config(control_socket),
                None,
                expected,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn fork_provisioning_failure_deletes_cloned_runner() {
        let provider = Arc::new(TestProvider::new(false, false, true, false, false));
        let config = test_config(false);
        let golden = MachineName::new("lifecycle-test-golden").unwrap();
        provisioning_failure(provider, &config, Some(&golden), "configure-failure").await;
    }

    #[tokio::test]
    async fn runner_error_wins_when_delete_also_fails() {
        let provider = Arc::new(TestProvider::new(false, false, false, true, true));
        let config = test_config(false);
        let runner = provision_slot(&provider, &config, 0, 1, None)
            .await
            .expect("provisioning succeeds");
        let idle = AtomicUsize::new(0);
        let error = run_one_runner(
            provider,
            &config,
            runner,
            CancellationToken::new(),
            SlotPlan {
                slot: 0,
                generation: 2,
                golden: None,
                idle: &idle,
            },
        )
        .await
        .expect_err("runner failure must propagate");
        assert!(error.to_string().contains("run-failure"));
        assert!(!error.to_string().contains("delete-failure"));
    }
}
