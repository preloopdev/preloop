//! SmolVM driver, over the `smolvm` CLI.
//!
//! Golden-fork model: a build sandbox starts `--branchable`; `Snapshot`
//! keeps that machine running as the fork base and returns its name, and
//! every sandbox created from the snapshot template is a CoW branch of it.
//! Pools are the service's generic warm pool (smolvm's native pools live
//! only in `smolvm serve`, whose clones lose `--mount-socket` relays).

pub mod cli;
pub mod pack;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use self::cli::{
    ForkLaunch, MachineName, MachineRole, MachineSpec, MachineUpdate, NetworkPolicy as VmNetwork,
    OutputChunk, SmolVmProvider, SocketMount as VmSocket, VmError, VmProvider, VolumeMount,
    mark_sandbox_owned,
};
use self::pack::{ArtifactCache, ArtifactPin, LocalImage, PackFormat, select_artifact};
use crate::api::*;
use crate::driver::{
    Driver, EXEC_CANCELLED, EXEC_TIMED_OUT, ExecCancels, NATIVE_PREFIX, native_name,
    parse_native_name, shell_quote,
};
use crate::error::{Code, Error, Result};

/// Driver name.
pub const NAME: &str = "smolvm";

/// Template recipe prefix for a packed `.smolmachine` artifact on this host.
pub const SMOLMACHINE_RECIPE: &str = "smolmachine:";

/// Template recipe prefix for a host path the CLI boots with `--image`: a
/// Docker-save `.tar` archive or a bare rootfs directory.
pub const IMAGE_RECIPE: &str = "image:";

/// SmolVM's vCPU default for a machine created without a shape.
const DEFAULT_CPUS: u32 = 2;
/// SmolVM's memory default for a machine created without a shape, in MiB.
const DEFAULT_MEMORY_MIB: u32 = 4096;
/// SmolVM's persistent-storage default for a machine created without a shape.
const DEFAULT_STORAGE_GIB: u32 = 20;

/// The size a create request actually gets: its own fields, with SmolVM's
/// defaults filling the zeros.
fn effective_shape(requested: &Shape) -> Shape {
    Shape {
        cpus: if requested.cpus == 0 {
            DEFAULT_CPUS
        } else {
            requested.cpus
        },
        memory_mib: if requested.memory_mib == 0 {
            DEFAULT_MEMORY_MIB
        } else {
            requested.memory_mib
        },
        disk_gib: if requested.disk_gib == 0 {
            DEFAULT_STORAGE_GIB
        } else {
            requested.disk_gib
        },
        ..Shape::default()
    }
}

/// SmolVM driver.
pub struct SmolvmDriver {
    provider: SmolVmProvider,
    slots: u32,
    named_prefix: Option<String>,
    /// Formats this host's SmolVM can materialize.
    formats: Vec<Arc<dyn PackFormat>>,
    /// Content-addressed cache of packed artifacts.
    cache: ArtifactCache,
    /// Artifact each live machine keeps resident, by native name.
    pins: Mutex<HashMap<String, ArtifactPin>>,
    cancels: ExecCancels,
}

impl SmolvmDriver {
    /// Driver over `provider`; `slots` caps concurrent sandboxes (0 = none).
    pub fn new(provider: SmolVmProvider, slots: u32) -> Self {
        Self {
            provider,
            slots,
            named_prefix: None,
            formats: pack::default_formats(),
            cache: ArtifactCache::from_env(std::env::temp_dir().join("preloop-sandbox-artifacts")),
            pins: Mutex::new(HashMap::new()),
            cancels: ExecCancels::default(),
        }
    }

    /// Use `cache` for packed artifacts instead of the default one.
    pub fn with_cache(mut self, cache: ArtifactCache) -> Self {
        self.cache = cache;
        self
    }

    /// Also report machines with this name prefix (caller-named sandboxes,
    /// e.g. the CI pool's `preloop-…` runners) in the inventory.
    pub fn with_named_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.named_prefix = Some(prefix.into());
        self
    }

    fn adopts(&self, name: &str) -> bool {
        self.named_prefix
            .as_deref()
            .is_some_and(|prefix| name.starts_with(prefix))
    }

    /// Mark a machine the CI pool does not own as a Sandbox Service sandbox.
    ///
    /// The engine's start/teardown sweep (`OrphanPurge::All`) kills every
    /// hypervisor under this home it cannot attribute, which used to include
    /// sandboxes leased to other callers. The sweep sees only a machine's
    /// data directory, so that is where the ownership marker goes. Pool
    /// machines (`named_prefix`) stay unmarked: the pool deletes them itself
    /// on teardown and the sweep is its crash backstop.
    ///
    /// A marker that cannot be written fails the create: an unmarked sandbox
    /// could be killed under a live lease at the next engine start or stop.
    async fn mark_owned(&self, name: &MachineName) -> Result<()> {
        if self.adopts(name.as_str()) {
            return Ok(());
        }
        let data_dir = self
            .provider
            .machine_data_dir(name)
            .await
            .map_err(vm_error)?;
        mark_sandbox_owned(&data_dir, name.as_str()).map_err(vm_error)?;
        Ok(())
    }

    /// [`SmolvmDriver::mark_owned`] for a machine already created: a machine
    /// that cannot be marked must not be handed out, so it is destroyed.
    async fn mark_owned_or_destroy(&self, name: &MachineName) -> Result<()> {
        match self.mark_owned(name).await {
            Ok(()) => Ok(()),
            Err(error) => {
                let _ = self.provider.delete(name).await;
                Err(error)
            }
        }
    }

    /// The underlying CLI provider.
    pub fn provider(&self) -> &SmolVmProvider {
        &self.provider
    }

    /// The artifact cache this driver materializes packs into.
    pub fn cache(&self) -> &ArtifactCache {
        &self.cache
    }

    /// Evict least-recently-used cached artifacts, sparing those in use.
    pub fn collect_garbage(&self) -> Result<u64> {
        self.cache.collect_garbage()
    }

    /// Materialize the first artifact this host can boot for `template`.
    ///
    /// Returns the image string for SmolVM and, when the bytes live in the
    /// cache, the pin that keeps them resident for the machine's lifetime.
    async fn image_for(&self, template: &Template) -> Result<(String, Option<ArtifactPin>)> {
        if template.spec.artifacts.is_empty() {
            if let Some(recipe) = template.spec.recipe() {
                let path = recipe
                    .strip_prefix(SMOLMACHINE_RECIPE)
                    .or_else(|| recipe.strip_prefix(IMAGE_RECIPE))
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        Error::invalid_argument(format!("smolvm: unknown recipe `{recipe}`"))
                    })?;
                return Ok((path, None));
            }
            return Err(Error::invalid_argument(format!(
                "template `{}` has no artifacts",
                template.spec.name
            )));
        }
        let (artifact, format) =
            select_artifact(&template.spec.artifacts, &self.formats, guest_arch())?;
        // Pin before fetching: a concurrent GC must not delete bytes that are
        // about to be handed to a machine.
        let pin = self.cache.pin(&artifact.digest);
        let materialized = format.materialize(artifact, &self.cache).await?;
        // The backend pulls a registry image itself; only cached bytes need
        // to stay resident.
        let pin = match &materialized {
            LocalImage::File(_) => Some(pin),
            LocalImage::Reference(_) => None,
        };
        Ok((materialized.into_image(), pin))
    }

    /// The command half of [`Driver::exec`], split out so the cancel registry
    /// is cleaned up on every path.
    async fn exec_inner(
        &self,
        name: &MachineName,
        exec_id: &str,
        request: &ExecRequest,
        argv: &[String],
        output: mpsc::Sender<ExecEvent>,
        cancel: CancellationToken,
    ) -> Result<i32> {
        if cancel.is_cancelled() {
            return Ok(EXEC_CANCELLED);
        }
        let (tx, mut rx) = mpsc::channel::<OutputChunk>(64);
        let forward = tokio::spawn(async move {
            while let Some(chunk) = rx.recv().await {
                let event = match chunk {
                    OutputChunk::Stdout(bytes) => ExecEvent::stdout(bytes),
                    OutputChunk::Stderr(bytes) => ExecEvent::stderr(bytes),
                };
                if output.send(event).await.is_err() {
                    return;
                }
            }
        });
        let pidfile = guest_pid_file(exec_id);
        let wrapped = guest_wrapped_argv(argv, &pidfile);
        let run = self.provider.exec_stream(name, &wrapped, tx);
        let end = {
            let wait = async {
                if request.timeout_secs == 0 {
                    return run.await.map_err(vm_error).map(End::Status);
                }
                match tokio::time::timeout(
                    std::time::Duration::from_secs(request.timeout_secs),
                    run,
                )
                .await
                {
                    Ok(result) => result.map_err(vm_error).map(End::Status),
                    // Dropping the stream killed the CLI child (`kill_on_drop`).
                    Err(_) => Ok(End::TimedOut),
                }
            };
            tokio::select! {
                result = wait => result?,
                _ = cancel.cancelled() => End::Cancelled,
            }
        };
        // Killing the host-side `machine exec` child does not stop the
        // command in the guest: SmolVM's guest agent does not reap it when
        // the client goes away. Terminate it in the guest as well.
        let code = match end {
            End::Status(code) => code,
            End::TimedOut => {
                self.kill_guest(name, exec_id).await;
                EXEC_TIMED_OUT
            }
            End::Cancelled => {
                self.kill_guest(name, exec_id).await;
                EXEC_CANCELLED
            }
        };
        let _ = forward.await;
        Ok(code)
    }

    /// Terminate a command the guest agent left behind, with a second
    /// `machine exec` (see [`guest_kill_script`]).
    ///
    /// Best effort: the caller already has its `124`/`130`, and a guest that
    /// cannot answer must not hold the agent's exec slot. The pid file makes
    /// the call idempotent, so a deadline followed by a cancel is harmless.
    async fn kill_guest(&self, name: &MachineName, exec_id: &str) {
        let argv = [
            "sh".to_owned(),
            "-c".to_owned(),
            guest_kill_script(&guest_pid_file(exec_id)),
        ];
        match tokio::time::timeout(GUEST_KILL_TIMEOUT, self.provider.exec(name, &argv)).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                warn!(%error, machine = %name.as_str(), exec = exec_id, "smolvm: guest command kill failed")
            }
            Err(_) => {
                warn!(machine = %name.as_str(), exec = exec_id, "smolvm: guest command kill timed out")
            }
        }
    }
}

/// How long the guest-side kill may take, including the wait for a command
/// that is still starting up.
const GUEST_KILL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Guest file recording the PID of the command started for `exec_id`.
///
/// The exec id is driver-internal, but sanitise it anyway: it ends up inside
/// a guest shell script and a path, and must never be able to break out of
/// either.
fn guest_pid_file(exec_id: &str) -> String {
    let token: String = exec_id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect();
    let token = if token.is_empty() { "anon" } else { &token };
    format!("/var/tmp/.psbx-exec-{token}.pid")
}

/// Wrap `argv` so the guest records the command's PID and process group.
///
/// SmolVM's `machine exec` client can be killed (deadline, cancel, a dropped
/// connection) while the guest agent keeps running the command: it does not
/// reap the process it started. The wrapper starts the command in its own
/// session when the guest has `setsid` (Alpine's busybox does), records the
/// PID and the PID's `/proc` start time — so a recycled PID is never
/// signalled — and removes the record when the command exits. A later
/// [`guest_kill_script`] can then signal the whole process group.
///
/// Without `setsid`, the command is still recorded and killed by PID; only
/// its children survive.
fn guest_wrapped_argv(argv: &[String], pidfile: &str) -> Vec<String> {
    let pidfile = shell_quote(pidfile);
    let script = format!(
        "pidfile={pidfile}\n\
         if command -v setsid >/dev/null 2>&1; then\n\
         \x20 setsid \"$@\" &\n\
         else\n\
         \x20 \"$@\" &\n\
         fi\n\
         child=$!\n\
         start=\n\
         if [ -r \"/proc/$child/stat\" ]; then\n\
         \x20 stat=$(cat \"/proc/$child/stat\")\n\
         \x20 rest=${{stat##*) }}\n\
         \x20 set -- $rest\n\
         \x20 start=${{20-}}\n\
         fi\n\
         printf '%s %s\\n' \"$child\" \"$start\" > \"$pidfile\"\n\
         wait \"$child\"\n\
         status=$?\n\
         rm -f \"$pidfile\"\n\
         exit $status\n"
    );
    let mut wrapped = vec![
        "sh".to_owned(),
        "-c".to_owned(),
        script,
        "psbx-exec".to_owned(),
    ];
    wrapped.extend_from_slice(argv);
    wrapped
}

/// Guest-side script that terminates the command recorded by
/// [`guest_wrapped_argv`].
///
/// Waits briefly for the PID file (a cancel can beat the wrapper), refuses a
/// PID whose start time no longer matches, then sends `SIGTERM` to the
/// command's process group — falling back to the PID — and escalates to
/// `SIGKILL` after two seconds.
fn guest_kill_script(pidfile: &str) -> String {
    let pidfile = shell_quote(pidfile);
    format!(
        "pidfile={pidfile}\n\
         i=0\n\
         while [ ! -s \"$pidfile\" ] && [ \"$i\" -lt 20 ]; do sleep 0.1; i=$((i+1)); done\n\
         [ -s \"$pidfile\" ] || exit 0\n\
         read pid start < \"$pidfile\"\n\
         case \"$pid\" in ''|*[!0-9]*) exit 0;; esac\n\
         if [ -n \"$start\" ]; then\n\
         \x20 [ -r \"/proc/$pid/stat\" ] || exit 0\n\
         \x20 stat=$(cat \"/proc/$pid/stat\")\n\
         \x20 rest=${{stat##*) }}\n\
         \x20 set -- $rest\n\
         \x20 [ \"${{20-}}\" = \"$start\" ] || exit 0\n\
         fi\n\
         rm -f \"$pidfile\"\n\
         kill -TERM \"-$pid\" 2>/dev/null || kill -TERM \"$pid\" 2>/dev/null || exit 0\n\
         i=0\n\
         while kill -0 \"$pid\" 2>/dev/null && [ \"$i\" -lt 20 ]; do sleep 0.1; i=$((i+1)); done\n\
         kill -KILL \"-$pid\" 2>/dev/null || kill -KILL \"$pid\" 2>/dev/null || true\n\
         exit 0\n"
    )
}

/// The end of a streaming exec.
enum End {
    /// The host-side `machine exec` returned this exit code.
    Status(i32),
    /// The deadline dropped the host-side client.
    TimedOut,
    /// [`Driver::cancel_exec`] dropped the host-side client.
    Cancelled,
}

fn guest_arch() -> &'static str {
    crate::api::host_arch()
}

fn vm_error(error: VmError) -> Error {
    let code = match &error {
        VmError::InvalidMachineName(_) | VmError::InvalidSpec(_) => Code::InvalidArgument,
        VmError::UnsupportedSocketMount { .. }
        | VmError::InvalidSandboxEnv { .. }
        | VmError::UnsupportedForkOptions { .. } => Code::FailedPrecondition,
        VmError::ForkBaseBusy { .. } => Code::ResourceExhausted,
        _ => Code::Unavailable,
    };
    Error::new(code, error.to_string())
}

fn machine(native_ref: &str) -> Result<MachineName> {
    MachineName::new(native_ref).map_err(vm_error)
}

/// Decode a JSON driver option the CI pool encoded from its `MachineSpec`.
fn json_option<T: serde::de::DeserializeOwned>(raw: Option<&str>, key: &str) -> Result<Option<T>> {
    raw.map(|raw| {
        serde_json::from_str(raw).map_err(|error| {
            Error::invalid_argument(format!("smolvm: invalid `{key}` option: {error}"))
        })
    })
    .transpose()
}

fn network(policy: &NetworkPolicy) -> VmNetwork {
    match policy.mode.as_known().unwrap_or_default() {
        NetworkMode::Disabled => VmNetwork::Disabled,
        NetworkMode::PublicOnly => VmNetwork::PublicOnly,
        NetworkMode::Restricted => VmNetwork::Restricted {
            hosts: policy.hosts.clone(),
            cidrs: policy.cidrs.clone(),
        },
        NetworkMode::Unrestricted | NetworkMode::NetworkModeUnspecified => VmNetwork::Unrestricted,
    }
}

/// Fork egress policy: `None` keeps the fork base's policy, unlike a cold
/// boot, where an unset mode means unrestricted.
fn fork_network(policy: &NetworkPolicy) -> Option<VmNetwork> {
    match policy.mode.as_known().unwrap_or_default() {
        NetworkMode::NetworkModeUnspecified => None,
        NetworkMode::Disabled => Some(VmNetwork::Disabled),
        NetworkMode::PublicOnly => Some(VmNetwork::PublicOnly),
        NetworkMode::Restricted => Some(VmNetwork::Restricted {
            hosts: policy.hosts.clone(),
            cidrs: policy.cidrs.clone(),
        }),
        NetworkMode::Unrestricted => Some(VmNetwork::Unrestricted),
    }
}

/// Split a fork request into what `machine branch` can apply while the clone
/// boots and what `machine update` must rewrite afterwards.
///
/// A clone starts as a copy of its fork base: it inherits the base's sockets,
/// resolver, role, guest volumes and workload, none of which SmolVM can change
/// on an existing machine. Restating one of those is therefore a request the
/// fork cannot honor, and refusing it is the only alternative to silently
/// returning a machine that does not match. Everything else is applied:
/// network, mounts, shape, overlay size and Rosetta.
fn fork_request(work: &CreateWork) -> Result<(ForkLaunch, MachineUpdate)> {
    let option = |key: &str| work.options.get(key).map(String::as_str);
    let mut unsupported = Vec::new();
    if !work.sockets.is_empty() {
        unsupported.push("sockets".to_owned());
    }
    if option("dns").is_some() {
        unsupported.push("dns".to_owned());
    }
    if let Some(raw) = option("role") {
        // `image` is the create-time default (no pack launch settings); any
        // other role selected guest volumes or a workload the base already
        // carries and a fork cannot replace.
        if !matches!(
            serde_json::from_str::<MachineRole>(raw),
            Ok(MachineRole::Image)
        ) {
            unsupported.push("role".to_owned());
        }
    }
    if option("guest_volumes").is_some() {
        unsupported.push("guest_volumes".to_owned());
    }
    if option("workload_init").is_some() {
        unsupported.push("workload_init".to_owned());
    }
    for key in work.options.keys() {
        if !matches!(
            key.as_str(),
            "dns" | "role" | "guest_volumes" | "workload_init" | "rosetta" | "overlay_gib"
        ) {
            unsupported.push(format!("option `{key}`"));
        }
    }
    if !unsupported.is_empty() {
        return Err(Error::failed_precondition(format!(
            "smolvm: forking cannot apply {}: a clone inherits {} from its fork base",
            unsupported.join(", "),
            if unsupported.len() == 1 { "it" } else { "them" },
        )));
    }

    let mut update = MachineUpdate::default();
    if work.shape.cpus > 0 {
        update.cpus = Some(
            u16::try_from(work.shape.cpus)
                .map_err(|_| Error::invalid_argument("smolvm: shape.cpus is too large"))?,
        );
    }
    if work.shape.memory_mib > 0 {
        update.memory_mib = Some(work.shape.memory_mib);
    }
    if work.shape.disk_gib > 0 {
        update.storage_gib = Some(work.shape.disk_gib);
    }
    if let Some(raw) = option("overlay_gib") {
        update.overlay_gib = Some(
            raw.parse()
                .map_err(|_| Error::invalid_argument("smolvm: invalid `overlay_gib` option"))?,
        );
    }
    update.network = fork_network(&work.network);
    update.volumes = work
        .mounts
        .iter()
        .map(|mount| VolumeMount {
            host: PathBuf::from(&mount.host),
            guest: PathBuf::from(&mount.guest),
            read_only: mount.readonly,
        })
        .collect();
    update.rosetta = option("rosetta") == Some("true");
    let launch = ForkLaunch {
        branchable: work.snapshot_capable,
        network: update.network.clone(),
    };
    Ok((launch, update))
}

/// This host's per-sandbox ceiling: usable vCPUs and total memory.
///
/// SmolVM cannot give one machine more than the host has, so placement needs
/// that ceiling to reject a shape the host would fail to boot. Cached: the
/// host does not change size under a running agent, and `capabilities()` is on
/// the registration hot path.
static HOST_CAPACITY: std::sync::LazyLock<(u32, u32)> = std::sync::LazyLock::new(|| {
    let cpus = std::thread::available_parallelism()
        .map(|count| count.get() as u32)
        .unwrap_or(0);
    (cpus, host_memory_mib().unwrap_or(0))
});

fn host_capacity() -> (u32, u32) {
    *HOST_CAPACITY
}

/// Total host memory in MiB, when the platform exposes it cheaply.
#[cfg(target_os = "linux")]
fn host_memory_mib() -> Option<u32> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let kib: u64 = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    u32::try_from(kib / 1024).ok()
}

#[cfg(target_os = "macos")]
fn host_memory_mib() -> Option<u32> {
    let output = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    let bytes: u64 = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .ok()?;
    u32::try_from(bytes / (1024 * 1024)).ok()
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn host_memory_mib() -> Option<u32> {
    None
}

#[async_trait]
impl Driver for SmolvmDriver {
    fn name(&self) -> &str {
        NAME
    }

    fn capabilities(&self) -> Capabilities {
        let mut features = Vec::new();
        if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
            features.push("rosetta".into());
        }
        let (max_vcpus, max_memory_mib) = host_capacity();
        Capabilities {
            os: "linux".into(),
            arch: vec![guest_arch().into()],
            features,
            pause: true,
            snapshot: true,
            fork: true,
            socket_mounts: true,
            host_mounts: true,
            network_modes: vec![
                NetworkMode::Disabled.into(),
                NetworkMode::Unrestricted.into(),
                NetworkMode::PublicOnly.into(),
                NetworkMode::Restricted.into(),
            ],
            slots: self.slots,
            native_pool: false,
            one_shot: false,
            max_vcpus,
            max_memory_mib,
            default_shape: buffa::MessageField::some(effective_shape(&Shape::default())),
            ..Capabilities::default()
        }
    }

    async fn create(&self, work: &CreateWork) -> Result<String> {
        // A caller-chosen name doubles as the native name, so host tools
        // (`preloop shell`, `smolvm machine exec`) address the same machine.
        let name = if work.name.is_empty() {
            native_name(&work.sandbox_id)
        } else {
            work.name.clone()
        };
        let clone = machine(&name)?;
        // One native name is one machine: a pin from an earlier incarnation
        // must not outlive it.
        self.pins.lock().remove(&name);
        if !work.template.native_ref.is_empty() {
            let golden = machine(&work.template.native_ref)?;
            let (launch, update) = fork_request(work)?;
            let forked = match self.provider.fork_with(&golden, &clone, &launch).await {
                Ok(()) => Ok(()),
                Err(error) => {
                    // Older SmolVM fork bases serve one live clone; re-arm
                    // once when nothing depends on the base any more.
                    warn!(%error, golden = %golden.as_str(), "fork failed; trying to re-arm the base");
                    if self
                        .provider
                        .rearm_fork_base(&golden, Some(&clone))
                        .await
                        .map_err(vm_error)?
                    {
                        self.provider
                            .fork_with(&golden, &clone, &launch)
                            .await
                            .map_err(vm_error)
                    } else {
                        Err(vm_error(error))
                    }
                }
            };
            forked?;
            // The clone boots from the base's state; settings `machine branch`
            // cannot express are applied now, while the clone is ours. A
            // failure leaves no half-configured machine behind.
            if let Err(error) = self
                .provider
                .apply_fork_update(&clone, &update, launch.branchable)
                .await
            {
                let _ = self.provider.delete(&clone).await;
                return Err(vm_error(error));
            }
            self.mark_owned_or_destroy(&clone).await?;
            return Ok(name);
        }
        let (image, pin) = self.image_for(&work.template).await?;
        let option = |key: &str| work.options.get(key).map(String::as_str);
        let shape = effective_shape(&work.shape);
        let spec = MachineSpec {
            name: clone.clone(),
            image,
            cpus: shape.cpus as u16,
            memory_mib: shape.memory_mib,
            storage_gib: shape.disk_gib,
            overlay_gib: option("overlay_gib").and_then(|v| v.parse().ok()),
            network: network(&work.network),
            // The host agent admitted these paths against its allowlist
            // before this work item reached the driver; never resolve or
            // widen them here.
            volumes: work
                .mounts
                .iter()
                .map(|m| VolumeMount {
                    host: PathBuf::from(&m.host),
                    guest: PathBuf::from(&m.guest),
                    read_only: m.readonly,
                })
                .collect(),
            sockets: work
                .sockets
                .iter()
                .map(|s| VmSocket {
                    host: PathBuf::from(&s.host),
                    guest: PathBuf::from(&s.guest),
                })
                .collect(),
            dns: option("dns").map(str::to_owned),
            rosetta: option("rosetta") == Some("true"),
            role: json_option(option("role"), "role")?.unwrap_or_default(),
            guest_volumes: json_option(option("guest_volumes"), "guest_volumes")?,
            workload_init: json_option(option("workload_init"), "workload_init")?,
        };
        self.provider.create(&spec).await.map_err(vm_error)?;
        // Mark before boot: the engine's sweep can only kill a running
        // hypervisor, so doing it here leaves no window in which a live
        // sandbox is unmarked.
        self.mark_owned_or_destroy(&clone).await?;
        let started = if work.snapshot_capable {
            self.provider.start_forkable(&clone).await
        } else {
            self.provider.start(&clone).await
        };
        if let Err(error) = started {
            let _ = self.provider.delete(&clone).await;
            return Err(vm_error(error));
        }
        if let Some(pin) = pin {
            self.pins.lock().insert(name.clone(), pin);
        }
        Ok(name)
    }

    async fn destroy(&self, native_ref: &str) -> Result<()> {
        let name = machine(native_ref)?;
        let destroyed = match self.provider.delete(&name).await {
            Ok(()) => Ok(()),
            Err(error) if error.to_string().contains("not found") => Ok(()),
            Err(error) => Err(vm_error(error)),
        };
        // The pack is still needed if the machine survived a failed delete.
        if destroyed.is_ok() {
            self.pins.lock().remove(native_ref);
        }
        destroyed
    }

    async fn exec(
        &self,
        native_ref: &str,
        exec_id: &str,
        request: &ExecRequest,
        output: mpsc::Sender<ExecEvent>,
    ) -> Result<i32> {
        if request.argv.is_empty() {
            return Err(Error::invalid_argument("argv is required"));
        }
        let name = machine(native_ref)?;
        let argv = crate::driver::posix_exec_argv(request);
        let cancel = self.cancels.start(exec_id);
        let result = self
            .exec_inner(&name, exec_id, request, &argv, output, cancel)
            .await;
        self.cancels.finish(exec_id);
        result
    }

    async fn cancel_exec(&self, _native_ref: &str, exec_id: &str) -> Result<()> {
        self.cancels.cancel(exec_id);
        Ok(())
    }

    async fn copy_in(&self, native_ref: &str, tar: &Path, guest_dir: &str) -> Result<()> {
        let name = machine(native_ref)?;
        let staged = format!("/var/tmp/.psbx-in-{}.tar", uuid::Uuid::new_v4().simple());
        self.provider
            .copy(
                &tar.display().to_string(),
                &format!("{native_ref}:{staged}"),
            )
            .await
            .map_err(vm_error)?;
        let script = format!(
            "mkdir -p {dir} && tar -xf {staged} -C {dir}; rc=$?; rm -f {staged}; exit $rc",
            dir = shell_quote(guest_dir),
            staged = shell_quote(&staged)
        );
        let output = self
            .provider
            .exec(&name, &["sh".into(), "-c".into(), script])
            .await
            .map_err(vm_error)?;
        if output.exit_code != 0 {
            return Err(Error::internal(format!(
                "smolvm: extract into {guest_dir} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(())
    }

    async fn copy_out(&self, native_ref: &str, guest_path: &str, tar: &Path) -> Result<()> {
        let name = machine(native_ref)?;
        let staged = format!("/var/tmp/.psbx-out-{}.tar", uuid::Uuid::new_v4().simple());
        let path = Path::new(guest_path);
        let parent = path
            .parent()
            .map_or("/".into(), |p| p.display().to_string());
        let base = path
            .file_name()
            .map_or(".".into(), |b| b.to_string_lossy().into_owned());
        let script = format!(
            "tar -cf {staged} -C {parent} {base}",
            staged = shell_quote(&staged),
            parent = shell_quote(if parent.is_empty() { "/" } else { &parent }),
            base = shell_quote(&base)
        );
        let output = self
            .provider
            .exec(&name, &["sh".into(), "-c".into(), script])
            .await
            .map_err(vm_error)?;
        if output.exit_code != 0 {
            return Err(Error::internal(format!(
                "smolvm: archive {guest_path} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        let copied = self
            .provider
            .copy(
                &format!("{native_ref}:{staged}"),
                &tar.display().to_string(),
            )
            .await
            .map_err(vm_error);
        let _ = self
            .provider
            .exec(&name, &["rm".into(), "-f".into(), staged])
            .await;
        copied
    }

    async fn snapshot(&self, native_ref: &str, _template: &str) -> Result<String> {
        // The build machine becomes the fork base. It is restarted
        // `--branchable` first so clones branch from a clean boot of the
        // provisioned disk rather than inheriting the bake's live processes
        // (a daemon or agent state mid-flight at snapshot time).
        let name = machine(native_ref)?;
        match self.provider.status(&name).await.map_err(vm_error)? {
            cli::MachineState::Running | cli::MachineState::Stopped => {}
            state => {
                return Err(Error::failed_precondition(format!(
                    "smolvm: snapshot source {native_ref} is {state:?}"
                )));
            }
        }
        self.provider.stop(&name).await.map_err(vm_error)?;
        self.provider
            .start_forkable(&name)
            .await
            .map_err(vm_error)?;
        Ok(native_ref.to_owned())
    }

    async fn drop_snapshot(&self, native_ref: &str) -> Result<()> {
        self.destroy(native_ref).await
    }

    async fn stop(&self, native_ref: &str) -> Result<()> {
        self.provider
            .stop(&machine(native_ref)?)
            .await
            .map_err(vm_error)
    }

    async fn start(&self, native_ref: &str, forkable: bool) -> Result<()> {
        let name = machine(native_ref)?;
        if forkable {
            self.provider.start_forkable(&name).await.map_err(vm_error)
        } else {
            self.provider.start(&name).await.map_err(vm_error)
        }
    }

    async fn pause(&self, native_ref: &str) -> Result<()> {
        self.provider
            .pause_machine(&machine(native_ref)?)
            .await
            .map_err(vm_error)
    }

    async fn resume(&self, native_ref: &str) -> Result<()> {
        self.provider
            .resume_machine(&machine(native_ref)?)
            .await
            .map_err(vm_error)
    }

    async fn inventory(&self) -> Result<Vec<Observed>> {
        let names = self.provider.list().await.map_err(vm_error)?;
        Ok(names
            .into_iter()
            .filter(|name| name.as_str().starts_with(NATIVE_PREFIX) || self.adopts(name.as_str()))
            .map(|name| {
                let sandbox_id = parse_native_name(name.as_str()).unwrap_or_default();
                Observed {
                    driver: NAME.into(),
                    native_ref: name.as_str().to_owned(),
                    sandbox_id,
                    ..Observed::default()
                }
            })
            .collect())
    }

    async fn host_report(&self) -> HostReport {
        let mut report = self.provider.host_report().await;
        // Only this driver's own sandboxes: `list()` sees every machine
        // smolvm knows about (goldens the pool manages separately,
        // adopted or foreign machines), matching `inventory`'s ownership
        // filter.
        report
            .sandboxes
            .retain(|s| s.native_ref.starts_with(NATIVE_PREFIX) || self.adopts(&s.native_ref));
        report
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::api::{
        Mount, NetworkMode, NetworkPolicy, Shape, SocketMount, Template, TemplateSpec,
    };
    use std::fs;
    use std::path::Path;

    /// Write `contents` as an executable without exposing a descriptor this
    /// process holds: a concurrently forked child would otherwise inherit it
    /// and fail the exec with ETXTBSY.
    fn write_executable(path: &Path, contents: &str) {
        use std::io::Write;

        let staged = path.with_extension("staged");
        let mut child = std::process::Command::new("sh")
            .args(["-c", r#"cat > "$1" && chmod 755 "$1""#, "sh"])
            .arg(&staged)
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .expect("piped stdin")
            .write_all(contents.as_bytes())
            .unwrap();
        assert!(child.wait().unwrap().success());
        fs::rename(staged, path).unwrap();
    }

    /// A smolvm that records every invocation (argv and the egress floor) and
    /// succeeds, so the driver's command sequence can be asserted.
    fn fake_smolvm() -> (tempfile::TempDir, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("smolvm");
        write_executable(
            &executable,
            r##"#!/bin/sh
set -eu
printf 'CALL\n' >> "$0.log"
printf 'ENV:%s\n' "${SMOLVM_EGRESS_FLOOR-}" >> "$0.log"
for arg in "$@"; do
  printf 'ARG:%s\n' "$arg" >> "$0.log"
done
if [ "${1-}:${2-}" = "machine:data-dir" ]; then
  mkdir -p "$0.data"
  printf '%s\n' "$0.data"
fi
exit 0
"##,
        );
        (directory, executable)
    }

    /// Recorded calls as `(egress floor, argv)`.
    fn calls(executable: &Path) -> Vec<(String, Vec<String>)> {
        let text = fs::read_to_string(executable.with_extension("log")).unwrap_or_default();
        let mut recorded: Vec<(String, Vec<String>)> = Vec::new();
        for line in text.lines() {
            if line == "CALL" {
                recorded.push((String::new(), Vec::new()));
            } else if let Some(floor) = line.strip_prefix("ENV:") {
                if let Some(last) = recorded.last_mut() {
                    last.0 = floor.to_owned();
                }
            } else if let Some(arg) = line.strip_prefix("ARG:")
                && let Some(last) = recorded.last_mut()
            {
                last.1.push(arg.to_owned());
            }
        }
        recorded
    }

    fn index_of(calls: &[(String, Vec<String>)], command: &str) -> Option<usize> {
        calls.iter().position(|(_, argv)| {
            argv.first().is_some_and(|first| first == "machine")
                && argv.get(1).is_some_and(|second| second == command)
        })
    }

    fn fork_work() -> CreateWork {
        CreateWork {
            sandbox_id: "sbx-test1".into(),
            driver: NAME.into(),
            name: "runtime-1".into(),
            template: Template {
                spec: TemplateSpec {
                    name: "golden".into(),
                    os: "linux".into(),
                    ..TemplateSpec::default()
                }
                .into(),
                native_ref: "golden".into(),
                ..Template::default()
            }
            .into(),
            ..CreateWork::default()
        }
    }

    fn driver(directory: &tempfile::TempDir) -> SmolvmDriver {
        SmolvmDriver::new(SmolVmProvider::new(directory.path().join("smolvm")), 0)
    }

    #[tokio::test]
    async fn a_cold_create_without_a_shape_uses_the_advertised_defaults() {
        let (directory, _executable) = fake_smolvm();
        let driver = driver(&directory);
        let defaults = driver.capabilities().default_shape;
        assert_eq!(
            *defaults,
            Shape {
                cpus: 2,
                memory_mib: 4096,
                disk_gib: 20,
                ..Shape::default()
            }
        );
        let work = CreateWork {
            sandbox_id: "sbx-default".into(),
            driver: NAME.into(),
            template: Template {
                spec: TemplateSpec {
                    name: "alpine".into(),
                    os: "linux".into(),
                    artifacts: vec![crate::api::Artifact {
                        format: crate::api::Artifact::OCI_IMAGE.into(),
                        arch: crate::api::host_arch().into(),
                        r#ref: format!("alpine@sha256:{}", "0".repeat(64)),
                        digest: format!("sha256:{}", "0".repeat(64)),
                        size_bytes: 0,
                        ..crate::api::Artifact::default()
                    }],
                    ..TemplateSpec::default()
                }
                .into(),
                ..Template::default()
            }
            .into(),
            ..CreateWork::default()
        };

        driver.create(&work).await.unwrap();

        let calls = calls(&directory.path().join("smolvm"));
        let create = index_of(&calls, "create").expect("machine create");
        let args = &calls[create].1;
        for (flag, value) in [
            ("--cpus", defaults.cpus.to_string()),
            ("--mem", defaults.memory_mib.to_string()),
            ("--storage", defaults.disk_gib.to_string()),
        ] {
            assert!(
                args.windows(2)
                    .any(|pair| pair[0] == flag && pair[1] == value),
                "create must pass {flag} {value}: {args:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_fork_applies_network_and_mounts_to_the_clone() {
        let (directory, _executable) = fake_smolvm();
        let host = directory.path().join("workspace");
        fs::create_dir_all(&host).unwrap();
        let driver = driver(&directory);
        let mut work = fork_work();
        work.network = NetworkPolicy {
            mode: NetworkMode::Disabled.into(),
            ..NetworkPolicy::default()
        }
        .into();
        work.mounts = vec![Mount {
            host: host.display().to_string(),
            guest: "/workspace".into(),
            readonly: true,
            ..Mount::default()
        }];
        work.shape = Shape {
            cpus: 4,
            memory_mib: 2048,
            disk_gib: 0,
            ..Shape::default()
        }
        .into();

        driver.create(&work).await.unwrap();

        let calls = calls(&directory.path().join("smolvm"));
        let fork = index_of(&calls, "fork").expect("machine fork");
        let stop = index_of(&calls, "stop").expect("machine stop");
        let update = index_of(&calls, "update").expect("machine update");
        let start = index_of(&calls, "start").expect("machine start");
        assert!(fork < stop && stop < update && update < start);
        let update_args = &calls[update].1;
        assert!(update_args.contains(&"--no-net".to_owned()));
        assert!(
            update_args
                .windows(2)
                .any(|pair| pair[0] == "--cpus" && pair[1] == "4")
        );
        assert!(
            update_args
                .windows(2)
                .any(|pair| pair[0] == "--mem" && pair[1] == "2048")
        );
        assert!(update_args.windows(2).any(|pair| {
            pair[0] == "--volume" && pair[1] == format!("{}:/workspace:ro", host.display())
        }));
    }

    #[tokio::test]
    async fn a_public_only_fork_launches_under_the_strict_egress_floor() {
        let (directory, _executable) = fake_smolvm();
        let driver = driver(&directory);
        let mut work = fork_work();
        work.network = NetworkPolicy {
            mode: NetworkMode::PublicOnly.into(),
            ..NetworkPolicy::default()
        }
        .into();

        driver.create(&work).await.unwrap();

        let calls = calls(&directory.path().join("smolvm"));
        let fork = index_of(&calls, "fork").expect("machine fork");
        assert_eq!(calls[fork].0, "strict");
        let update = index_of(&calls, "update").expect("machine update");
        assert!(calls[update].1.contains(&"--net".to_owned()));
    }

    #[tokio::test]
    async fn a_fork_refuses_settings_it_cannot_change() {
        for (field, mutate) in [
            (
                "sockets",
                Box::new(|work: &mut CreateWork| {
                    work.sockets = vec![SocketMount {
                        host: "/tmp/engine.sock".into(),
                        guest: "/run/engine.sock".into(),
                        ..SocketMount::default()
                    }];
                }) as Box<dyn Fn(&mut CreateWork)>,
            ),
            (
                "dns",
                Box::new(|work: &mut CreateWork| {
                    work.options.insert("dns".into(), "1.1.1.1".into());
                }) as Box<dyn Fn(&mut CreateWork)>,
            ),
            (
                "role",
                Box::new(|work: &mut CreateWork| {
                    work.options
                        .insert("role".into(), "\"official-pack\"".into());
                }) as Box<dyn Fn(&mut CreateWork)>,
            ),
        ] {
            let (directory, _executable) = fake_smolvm();
            let driver = driver(&directory);
            let mut work = fork_work();
            mutate(&mut work);
            let error = driver.create(&work).await.unwrap_err();
            assert_eq!(error.code, Code::FailedPrecondition, "{field}");
            assert!(
                crate::error::message(&error).contains(field),
                "{field}: {}",
                crate::error::message(&error)
            );
            assert!(
                calls(&directory.path().join("smolvm")).is_empty(),
                "{field}: nothing may reach smolvm"
            );
        }
    }

    #[tokio::test]
    async fn a_fork_that_matches_the_base_needs_no_reconfiguration() {
        let (directory, _executable) = fake_smolvm();
        let driver = driver(&directory);

        driver.create(&fork_work()).await.unwrap();

        let calls = calls(&directory.path().join("smolvm"));
        let fork = index_of(&calls, "fork").expect("machine fork");
        assert!(calls[fork].1.contains(&"--freeze-source".to_owned()));
        assert!(!calls[fork].1.contains(&"--branchable".to_owned()));
        assert_eq!(index_of(&calls, "update"), None);
        assert_eq!(index_of(&calls, "stop"), None);
        assert_eq!(index_of(&calls, "start"), None);
    }

    #[tokio::test]
    async fn a_snapshot_capable_fork_is_itself_branchable() {
        let (directory, _executable) = fake_smolvm();
        let driver = driver(&directory);
        let mut work = fork_work();
        work.snapshot_capable = true;

        driver.create(&work).await.unwrap();

        let calls = calls(&directory.path().join("smolvm"));
        let fork = index_of(&calls, "fork").expect("machine fork");
        assert!(calls[fork].1.contains(&"--branchable".to_owned()));
        assert_eq!(index_of(&calls, "update"), None);
    }

    use std::time::Duration;

    /// The wrapper records a PID that `guest_kill_script` can terminate; this
    /// exercises the exact scripts the guest runs, not just their framing.
    #[tokio::test]
    async fn guest_scripts_record_and_kill_the_wrapped_command() {
        let dir = tempfile::tempdir().unwrap();
        let pidfile = dir.path().join("exec.pid");
        let pidfile = pidfile.display().to_string();
        let argv = guest_wrapped_argv(&["sleep".to_owned(), "30".to_owned()], &pidfile);
        let mut wrapper = std::process::Command::new(&argv[0])
            .args(&argv[1..])
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();

        let mut pid = None;
        for _ in 0..200 {
            pid = std::fs::read_to_string(&pidfile)
                .ok()
                .and_then(|text| text.split_whitespace().next()?.parse::<i32>().ok());
            if pid.is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let pid = pid.expect("the wrapper records its command's pid");

        let script = guest_kill_script(&pidfile);
        let status = std::process::Command::new("sh")
            .args(["-c", &script])
            .status()
            .unwrap();
        assert!(status.success(), "kill script failed");

        let status = wrapper.wait().unwrap();
        assert!(!status.success(), "the killed command is not a success");
        let alive = std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(!alive.success(), "pid {pid} survived the guest kill");
        assert!(
            !std::path::Path::new(&pidfile).exists(),
            "pid file left behind"
        );
    }
}
