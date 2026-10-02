//! Typed SmolVM lifecycle primitives for Preloop CI.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing::{info, warn};

pub mod agentenv;
pub mod telemetry;

const DEFAULT_CAPTURE_LIMIT: usize = 1024 * 1024;

/// A validated persistent SmolVM machine name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MachineName(String);

impl MachineName {
    /// Validate and construct a machine name.
    pub fn new(value: impl Into<String>) -> Result<Self, VmError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 63
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            && value
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            && value
                .as_bytes()
                .last()
                .is_some_and(u8::is_ascii_alphanumeric);
        if !valid {
            return Err(VmError::InvalidMachineName(value));
        }
        Ok(Self(value))
    }

    /// Return the name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for MachineName {
    type Error = VmError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<MachineName> for String {
    fn from(value: MachineName) -> Self {
        value.0
    }
}

/// A host directory exposed to a guest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeMount {
    /// Canonical host path.
    pub host: PathBuf,
    /// Absolute guest path.
    pub guest: PathBuf,
    /// Deny guest writes to the host directory.
    pub read_only: bool,
}

/// Host Unix socket exposed at a fixed guest path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SocketMount {
    /// Host socket path.
    pub host: PathBuf,
    /// Absolute guest socket path.
    pub guest: PathBuf,
}

/// Explicit VM egress policy. Networking is disabled unless selected here.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum NetworkPolicy {
    /// No guest networking.
    #[default]
    Disabled,
    /// Unrestricted outbound networking.
    Unrestricted,
    /// Full outbound networking with the egress hard-floor enabled: loopback,
    /// RFC 1918, link-local / cloud metadata, CGNAT, and IPv6 private ranges
    /// are denied. Enforced by the chosen backend with
    /// `SMOLVM_EGRESS_FLOOR=strict` in the provider; see
    /// [`public_only_net_backend`] for which backend that is per platform.
    PublicOnly,
    /// Restrict outbound traffic to these host names and CIDRs.
    Restricted {
        /// DNS host names allowed for egress.
        hosts: Vec<String>,
        /// IP address ranges allowed for egress.
        cidrs: Vec<String>,
    },
}

/// The smolvm network backend used for the egress-only policy.
///
/// virtio-net carries the host-side egress floor, which TSI cannot provide
/// (TSI has no host network stack), so PublicOnly needs it. The official
/// SmolVM release pinned by `preloop update` bundles a NET=1 libkrun that
/// exports `krun_add_net_unixstream`. `PRELOOP_SMOLVM_NET_BACKEND=tsi|virtio-net`
/// overrides the choice for setups that need a different backend.
fn public_only_net_backend(lookup: impl Fn(&str) -> Option<String>) -> &'static str {
    match lookup("PRELOOP_SMOLVM_NET_BACKEND").as_deref() {
        Some("tsi") => "tsi",
        _ => "virtio-net",
    }
}

/// Where a guest environment value is resolved from, at launch time.
///
/// SmolVM never persists the value itself, only this reference, so a secret
/// stays out of the machine record and out of any packed artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecretSource {
    /// Read from a host environment variable of this name.
    HostEnv(String),
    /// Read from this absolute host file.
    HostFile(PathBuf),
}

/// Persistent VM configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineSpec {
    /// Machine identifier.
    pub name: MachineName,
    /// Immutable OCI image or `.smolmachine` artifact.
    pub image: String,
    /// Virtual CPU count.
    pub cpus: u16,
    /// Guest memory in MiB.
    pub memory_mib: u32,
    /// Persistent storage in GiB.
    pub storage_gib: u32,
    /// Root overlay size in GiB. `None` keeps the provider default.
    pub overlay_gib: Option<u32>,
    /// Guest network policy.
    pub network: NetworkPolicy,
    /// Narrowly scoped host mounts.
    pub volumes: Vec<VolumeMount>,
    /// Narrowly scoped host Unix sockets.
    pub sockets: Vec<SocketMount>,
    /// Guest DNS resolver, passed through as smolvm's `--dns`.
    ///
    /// smolvm's registry client resolves through the guest, and defaults to
    /// the public resolvers (8.8.8.8/1.1.1.1) — unreachable on networks that
    /// filter them (many LANs). Override from the host when the guest must
    /// pull images on such networks.
    #[serde(default)]
    pub dns: Option<String>,
    /// Enable Rosetta 2 x86_64 translation on Apple Silicon.
    #[serde(default)]
    pub rosetta: bool,
}

/// Observable VM state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MachineState {
    /// Machine exists and is running.
    Running,
    /// Machine exists and is stopped.
    Stopped,
    /// No machine with this name exists.
    Missing,
    /// SmolVM returned an unrecognized state.
    Unknown,
}

/// Captured process result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecOutput {
    /// Guest command exit status.
    pub exit_code: i32,
    /// Bounded standard output.
    pub stdout: Vec<u8>,
    /// Bounded standard error.
    pub stderr: Vec<u8>,
    /// Whether either captured stream exceeded the configured bound.
    pub truncated: bool,
}

/// One streaming guest output fragment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputChunk {
    /// Standard output bytes.
    Stdout(Vec<u8>),
    /// Standard error bytes.
    Stderr(Vec<u8>),
}

/// VM lifecycle failure.
#[derive(Debug, Error)]
pub enum VmError {
    /// Invalid machine name.
    #[error("invalid machine name `{0}`")]
    InvalidMachineName(String),
    /// Invalid VM configuration.
    #[error("invalid VM configuration: {0}")]
    InvalidSpec(String),
    /// Failed to launch SmolVM.
    #[error("failed to launch `{program}`: {source}")]
    Launch {
        /// Program path.
        program: String,
        /// Operating-system error.
        source: std::io::Error,
    },
    /// Failed to prepare the SmolVM runtime directories.
    #[error("failed to prepare SmolVM runtime directories: {source}")]
    RuntimeIo {
        /// Operating-system error.
        source: std::io::Error,
    },
    /// SmolVM rejected an operation.
    #[error("smolvm {operation} failed with exit code {exit_code}: {message}")]
    Command {
        /// Logical operation.
        operation: &'static str,
        /// Process exit code.
        exit_code: i32,
        /// Bounded diagnostic.
        message: String,
    },
    /// A plain-fork golden already has a live copy-on-write clone.
    ///
    /// SmolVM 1.7.4's `machine fork` does not retain a checkpoint for reuse.
    /// Invoking it again against the paused base can invalidate storage still
    /// used by the first clone, so callers must use an independent image.
    #[error("fork base `{golden}` already has live clone `{clone}`")]
    ForkBaseBusy {
        /// Forkable base that cannot safely serve another clone yet.
        golden: String,
        /// Existing clone whose storage still depends on the base.
        clone: String,
    },
    /// Invalid SmolVM JSON output.
    #[error("invalid smolvm response: {0}")]
    Protocol(String),
    /// The guest agent stopped answering while a command was streaming.
    ///
    /// A wedged guest never closes its exec stream, so without this the
    /// caller would wait on it forever.
    #[error("guest `{machine}` stopped answering after {probes} consecutive liveness probes")]
    GuestUnresponsive {
        /// Machine whose agent stopped answering.
        machine: String,
        /// Consecutive probes that failed or timed out.
        probes: u32,
    },
    /// The resolved SmolVM predates generic socket forwarding.
    ///
    /// `--mount-socket` (added upstream in the 2026-07 socket-forwarding
    /// work) generalizes the old docker-only bridge into a host→guest
    /// mount, which is what preloop needs to hand the control socket to
    /// the guest. Older binaries only accept `--docker-socket` (guest→host,
    /// docker specific), which cannot carry that mount. `smolvm --version`
    /// reports the wrapper's version, not the binary's, so the capability
    /// probe (help text) is the reliable check.
    #[error(
        "the resolved smolvm (`{binary}`) does not support `machine create --mount-socket`, \
         which preloop needs to mount the control socket into the guest; check which smolvm \
         the engine resolves (PATH) and update it from \
         https://github.com/smol-machines/smolvm/releases"
    )]
    UnsupportedSocketMount {
        /// Program path.
        binary: String,
    },
    /// An operator-set SmolVM sandbox override has a value upstream would
    /// silently treat as "control off". Preloop fails closed instead of
    /// forwarding an unrecognized mode that would boot VMs unconfined.
    #[error(
        "invalid {variable} value `{value}` for the VM sandbox (expected {expected}); \
         refusing to run VMs with an unvalidated override — remove the variable or set a \
         supported mode"
    )]
    InvalidSandboxEnv {
        /// The environment variable carrying the override.
        variable: &'static str,
        /// The offending value.
        value: String,
        /// Modes upstream accepts.
        expected: &'static str,
    },
}

/// What a backend can express, so the orchestrator can pick a strategy
/// instead of assuming SmolVM's.
///
/// Every field defaults to what SmolVM does, so an existing provider needs no
/// change and a new backend only declares what it *cannot* do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderCapabilities {
    /// Host directories can be mounted into a guest and stay live.
    ///
    /// `false` means [`MachineSpec::volumes`] is satisfied by copying at
    /// start time: correct for read-mostly mounts like the runner bundle,
    /// wrong for anything the host mutates while the guest runs.
    pub live_host_volumes: bool,
    /// Host Unix sockets can be forwarded into a guest.
    ///
    /// `false` forces the guest control plane onto TCP.
    pub socket_mounts: bool,
    /// [`VmProvider::pack`] produces a host file usable as a base image.
    ///
    /// `false` means packs live inside the backend (AgentENV snapshots), so
    /// the pool must not try to build, download, or relocate artifact files.
    pub file_packs: bool,
    /// A clone inherits filesystem writes the golden made *after* it booted.
    ///
    /// SmolVM's forkable snapshot does not: the pool reinstalls the baseline
    /// into each fork. AgentENV snapshots the golden's live filesystem.
    pub fork_inherits_guest_writes: bool,
    /// A suspended guest retains its live runner, processes, and filesystem
    /// state when resumed. Backends without this must not auto-suspend a live
    /// debug session.
    pub preserves_runtime_state_on_suspend: bool,
}

impl Default for ProviderCapabilities {
    fn default() -> Self {
        Self {
            live_host_volumes: true,
            socket_mounts: true,
            file_packs: true,
            fork_inherits_guest_writes: false,
            preserves_runtime_state_on_suspend: false,
        }
    }
}

/// Provider contract consumed by the Preloop orchestrator.
#[async_trait]
pub trait VmProvider: Send + Sync {
    /// What this backend can express. Defaults to SmolVM's behaviour.
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities::default()
    }
    /// Create a persistent machine.
    async fn create(&self, spec: &MachineSpec) -> Result<(), VmError>;
    /// Start an existing machine.
    async fn start(&self, name: &MachineName) -> Result<(), VmError>;
    /// Start an existing machine as a forkable base (CoW memory + disks).
    async fn start_forkable(&self, name: &MachineName) -> Result<(), VmError>;
    /// Fork a running forkable machine into a new clone with CoW memory and disks.
    async fn fork(&self, golden: &MachineName, clone: &MachineName) -> Result<(), VmError>;
    /// Remove `pack/` build intermediates from a machine's data directory once
    /// its finished disk exists (issue #295).
    ///
    /// smolvm's pack export leaves ~29 GB of intermediates (`storage.ext4`,
    /// `layers/*.tar`) beside the finished `storage.raw`, and a fork copies
    /// the golden's whole data directory — so every runner inherits a copy.
    /// Providers without pack intermediates keep the default no-op.
    async fn prune_pack_intermediates(&self, name: &MachineName) -> Result<bool, VmError> {
        let _ = name;
        Ok(false)
    }
    /// Remove machine data directories the provider's registry no longer
    /// knows about.
    ///
    /// smolvm's delete can drop the registry row while the data dir survives
    /// (the "directory not empty" / "database is locked" races `delete`
    /// retries bound but cannot eliminate), and a crashed engine leaves dirs
    /// no `list()` can reach. Such orphans are invisible to every other
    /// cleanup path and leak whole VM disks. Providers without
    /// host-visible data dirs keep the default no-op.
    async fn sweep_orphaned_data_dirs(&self) -> Result<usize, VmError> {
        Ok(0)
    }
    /// Stop an existing machine.
    async fn stop(&self, name: &MachineName) -> Result<(), VmError>;
    /// Delete a machine and its mutable overlay.
    async fn delete(&self, name: &MachineName) -> Result<(), VmError>;
    /// Read current machine state.
    async fn status(&self, name: &MachineName) -> Result<MachineState, VmError>;
    /// List persistent machines.
    async fn list(&self) -> Result<Vec<MachineName>, VmError>;
    /// Execute a guest command and return bounded output.
    async fn exec(&self, name: &MachineName, argv: &[String]) -> Result<ExecOutput, VmError>;
    /// Execute with guest environment values resolved from the host at launch.
    async fn exec_with_secret_env(
        &self,
        name: &MachineName,
        argv: &[String],
        secrets: &[(String, SecretSource)],
    ) -> Result<ExecOutput, VmError>;
    /// Re-arm a spent fork base so it can serve forks again.
    ///
    /// Atomic with forking: takes the same per-golden exclusion `fork` uses,
    /// so a concurrent fork can neither observe the golden mid-re-arm nor
    /// create a clone between the live-clone check and the golden being
    /// stopped. Removes a partial clone left by the failed fork (when
    /// `partial` is given) — only if that cleanup succeeds, because an
    /// untracked clone would otherwise share the resumed base's disks — then
    /// verifies no live clone still depends on the golden, and only then
    /// stops and restarts it forkable.
    ///
    /// Returns `Ok(true)` when the golden can serve forks again, `Ok(false)`
    /// when it cannot (a live clone exists, or the partial clone could not be
    /// removed), and `Err` on operational failure. The default implementation
    /// never re-arms, so providers without the machinery conservatively fall
    /// back to direct creation.
    async fn rearm_fork_base(
        &self,
        _golden: &MachineName,
        _partial: Option<&MachineName>,
    ) -> Result<bool, VmError> {
        Ok(false)
    }
    /// Execute while forwarding output fragments to the caller.
    async fn exec_stream(
        &self,
        name: &MachineName,
        argv: &[String],
        output: mpsc::Sender<OutputChunk>,
    ) -> Result<i32, VmError>;
    /// Copy a file or directory using SmolVM's host/guest path syntax.
    async fn copy(&self, source: &str, destination: &str) -> Result<(), VmError>;
    /// Pack a configured machine into a reusable immutable artifact.
    async fn pack(&self, name: &MachineName, output: &Path) -> Result<(), VmError>;
}

/// Delegate through a box so a backend can be chosen at runtime.
///
/// The pool is generic over its provider for zero-cost static dispatch, but
/// the CLI picks between SmolVM and AgentENV from the environment. One
/// `Box<dyn VmProvider>` keeps that choice out of the pool's type parameter
/// without an enum that has to be extended for every future backend. The
/// virtual call is irrelevant next to booting a VM.
#[async_trait]
impl<T: VmProvider + ?Sized> VmProvider for Box<T> {
    fn capabilities(&self) -> ProviderCapabilities {
        (**self).capabilities()
    }
    async fn create(&self, spec: &MachineSpec) -> Result<(), VmError> {
        (**self).create(spec).await
    }
    async fn start(&self, name: &MachineName) -> Result<(), VmError> {
        (**self).start(name).await
    }
    async fn start_forkable(&self, name: &MachineName) -> Result<(), VmError> {
        (**self).start_forkable(name).await
    }
    async fn fork(&self, golden: &MachineName, clone: &MachineName) -> Result<(), VmError> {
        (**self).fork(golden, clone).await
    }
    async fn stop(&self, name: &MachineName) -> Result<(), VmError> {
        (**self).stop(name).await
    }
    async fn delete(&self, name: &MachineName) -> Result<(), VmError> {
        (**self).delete(name).await
    }
    async fn status(&self, name: &MachineName) -> Result<MachineState, VmError> {
        (**self).status(name).await
    }
    async fn list(&self) -> Result<Vec<MachineName>, VmError> {
        (**self).list().await
    }
    async fn sweep_orphaned_data_dirs(&self) -> Result<usize, VmError> {
        (**self).sweep_orphaned_data_dirs().await
    }
    async fn exec(&self, name: &MachineName, argv: &[String]) -> Result<ExecOutput, VmError> {
        (**self).exec(name, argv).await
    }
    async fn exec_with_secret_env(
        &self,
        name: &MachineName,
        argv: &[String],
        secrets: &[(String, SecretSource)],
    ) -> Result<ExecOutput, VmError> {
        (**self).exec_with_secret_env(name, argv, secrets).await
    }
    async fn rearm_fork_base(
        &self,
        golden: &MachineName,
        partial: Option<&MachineName>,
    ) -> Result<bool, VmError> {
        (**self).rearm_fork_base(golden, partial).await
    }
    async fn exec_stream(
        &self,
        name: &MachineName,
        argv: &[String],
        output: mpsc::Sender<OutputChunk>,
    ) -> Result<i32, VmError> {
        (**self).exec_stream(name, argv, output).await
    }
    async fn copy(&self, source: &str, destination: &str) -> Result<(), VmError> {
        (**self).copy(source, destination).await
    }
    async fn pack(&self, name: &MachineName, output: &Path) -> Result<(), VmError> {
        (**self).pack(name, output).await
    }
}

/// CLI-backed SmolVM provider.
///
/// SmolVM's `machine exec` (non-streaming) drops the connection after a
/// short client-side read timeout unless an explicit `--timeout` extends
/// it, and it buffers all output until the command exits. Provisioning
/// steps (toolchain installs, `configure`) routinely run
/// minutes with little output, so a generous timeout is mandatory there;
/// streaming execs (the runner itself) are unaffected.
const EXEC_TIMEOUT: &str = "30m";

#[derive(Debug, Clone)]
pub struct SmolVmProvider {
    binary: PathBuf,
    capture_limit: usize,
    /// Proxy forwarded to smolvm's separate export VM during `pack create`.
    pack_proxy: Option<String>,
    /// Hosts and CIDRs that the pack export VM should bypass the proxy for.
    pack_no_proxy: Option<String>,
    /// Serializes operations that build or replace a machine's base against
    /// everything else. See [`SmolVmProvider::exclusive`].
    lifecycle_lock: Arc<tokio::sync::RwLock<()>>,
    /// One in-flight `machine fork` per golden. See [`SmolVmProvider::fork`].
    fork_locks: Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    /// Machines forked from a golden, by clone name, while they still exist.
    ///
    /// A golden is a copy-on-write base: once paused it must outlive every
    /// clone, and starting it again would corrupt them. This census is how the
    /// orchestrator knows a spent fork base may be safely re-armed.
    forked_machines: Arc<tokio::sync::Mutex<HashMap<String, String>>>,
    /// Whether the resolved binary's `machine create` accepts
    /// `--mount-socket`, probed once per provider.
    socket_mount_supported: Arc<tokio::sync::OnceCell<bool>>,
    /// Where this provider reads host environment overrides from (the sandbox
    /// modes and the network backend). Production is always the process
    /// environment; unit tests substitute a pure lookup so the policy can be
    /// exercised without mutating global state.
    env_lookup: EnvLookup,
}

impl Default for SmolVmProvider {
    fn default() -> Self {
        Self::from_environment("smolvm")
    }
}

impl SmolVmProvider {
    /// Construct a provider and resolve pack-export proxy settings.
    ///
    /// Preloop-specific variables take precedence over the conventional proxy
    /// variables inherited by the process.
    pub fn from_environment(binary: impl Into<PathBuf>) -> Self {
        Self::new(binary).with_pack_network(
            first_nonempty_env(&[
                "PRELOOP_RUNNER_PACK_PROXY",
                "HTTPS_PROXY",
                "https_proxy",
                "HTTP_PROXY",
                "http_proxy",
            ]),
            first_nonempty_env(&["PRELOOP_RUNNER_PACK_NO_PROXY", "NO_PROXY", "no_proxy"]),
        )
    }
}

fn first_nonempty_env(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    })
}

impl SmolVmProvider {
    /// Construct a provider using an explicit SmolVM executable.
    pub fn new(binary: impl Into<PathBuf>) -> Self {
        Self {
            binary: binary.into(),
            capture_limit: DEFAULT_CAPTURE_LIMIT,
            pack_proxy: None,
            pack_no_proxy: None,
            lifecycle_lock: Arc::new(tokio::sync::RwLock::new(())),
            fork_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            forked_machines: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            socket_mount_supported: Arc::new(tokio::sync::OnceCell::new()),
            env_lookup: process_env,
        }
    }

    /// Override the maximum bytes retained from each process stream.
    pub fn with_capture_limit(mut self, bytes: usize) -> Self {
        self.capture_limit = bytes.max(1024);
        self
    }

    /// Configure networking for smolvm's separate registry export VM.
    ///
    /// The export VM re-pulls the base image while flattening a machine, so
    /// its network path can differ from the VM Preloop already provisioned.
    pub fn with_pack_network(mut self, proxy: Option<String>, no_proxy: Option<String>) -> Self {
        self.pack_proxy = proxy.filter(|value| !value.trim().is_empty());
        self.pack_no_proxy = no_proxy.filter(|value| !value.trim().is_empty());
        self
    }

    /// Substitute the host-environment lookup this provider reads.
    ///
    /// Test seam for the Linux override tests: integration tests compile the
    /// library without `cfg(test)`, so this is unconditionally available on
    /// Linux. In production nothing constructs a provider with anything but
    /// [`process_env`], so the seam cannot redirect a real deployment.
    #[cfg(target_os = "linux")]
    pub fn with_env_lookup(mut self, lookup: EnvLookup) -> Self {
        self.env_lookup = lookup;
        self
    }

    fn command(&self) -> Command {
        Command::new(&self.binary)
    }

    /// A command ready to run against the resolved SmolVM binary with the
    /// sandbox environment applied. Every operation goes through here, so
    /// the sandbox reaches any command that can spawn or restart SmolVM's
    /// `_boot-vm` subprocess — create, start, start_forkable, fork, exec,
    /// pack — not just machine creation.
    fn sandboxed_command(&self) -> Result<Command, VmError> {
        let mut command = self.command();
        apply_smolvm_runtime_env_async(&mut command, Some(&self.binary))?;
        apply_sandbox_env(&mut command, self.env_lookup)?;
        Ok(command)
    }

    /// A command for an operation that can never boot or restart `_boot-vm`
    /// (status, list, stop, delete): the validated sandbox policy when it
    /// resolves, otherwise the sandbox variables stripped.
    ///
    /// Fail-closed validation must not extend here. A single operator typo
    /// in `SMOLVM_SECCOMP`/`SMOLVM_LANDLOCK` is recoverable *because* the
    /// pool can still be enumerated and torn down; hard-failing `status`,
    /// `list`, `stop`, and `delete` on the same typo would make the very
    /// operations needed to fix it unusable. The variables are still never
    /// forwarded unvalidated: on an invalid override they are removed, so a
    /// recovery command runs without sandbox variables (no VMM is spawned
    /// here, so nothing boots unconfined) rather than with garbage.
    fn recovery_command(&self) -> Result<Command, VmError> {
        let mut command = self.command();
        apply_smolvm_runtime_env_async(&mut command, Some(&self.binary))?;
        match sandbox_env_with(self.env_lookup) {
            Ok(sandbox) => {
                for key in sandbox.remove {
                    command.env_remove(key);
                }
                for (key, value) in sandbox.set {
                    command.env(key, value);
                }
            }
            Err(_) => {
                command.env_remove("SMOLVM_SECCOMP");
                command.env_remove("SMOLVM_LANDLOCK");
                command.env_remove("SMOLVM_CGROUP_ROOT");
            }
        }
        Ok(command)
    }

    /// Run an operation that can never boot or restart a VMM, using
    /// [`Self::recovery_command`] so a bad sandbox override cannot wedge the
    /// recovery paths. Mirrors [`Self::checked`]'s plumbing.
    async fn recovery(
        &self,
        operation: &'static str,
        args: &[String],
    ) -> Result<ExecOutput, VmError> {
        let mut command = self.recovery_command()?;
        command
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|source| VmError::Launch {
            program: self.binary.display().to_string(),
            source,
        })?;
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let (stdout, stderr, status) = tokio::join!(
            read_bounded(stdout, self.capture_limit),
            read_bounded(stderr, self.capture_limit),
            child.wait(),
        );
        let (stdout, stdout_truncated) = stdout.map_err(|source| VmError::Launch {
            program: self.binary.display().to_string(),
            source,
        })?;
        let (stderr, stderr_truncated) = stderr.map_err(|source| VmError::Launch {
            program: self.binary.display().to_string(),
            source,
        })?;
        let status = status.map_err(|source| VmError::Launch {
            program: self.binary.display().to_string(),
            source,
        })?;
        let result = ExecOutput {
            exit_code: status.code().unwrap_or(-1),
            stdout,
            stderr,
            truncated: stdout_truncated || stderr_truncated,
        };
        if !status.success() {
            let message = String::from_utf8_lossy(&result.stderr).trim().to_owned();
            return Err(VmError::Command {
                operation,
                exit_code: result.exit_code,
                message,
            });
        }
        Ok(result)
    }

    /// Whether `machine create` accepts `--mount-socket`, probed from the
    /// binary's own help text and cached. SmolVM's wrapper scripts can
    /// report a recent `--version` while resolving to an old binary, so the
    /// flag's presence is the reliable capability check.
    ///
    /// An invalid sandbox override is reported as such instead of being
    /// folded into a `false` answer: the caller caches the probe result for
    /// the provider's lifetime, and a rejected environment says nothing
    /// about the binary's capabilities.
    async fn supports_mount_socket(&self) -> Result<bool, VmError> {
        let mut command = self.sandboxed_command()?;
        command
            .args(["machine", "create", "--help"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let Ok(mut child) = command.spawn() else {
            return Ok(false);
        };
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let (stdout, stderr, status) = tokio::join!(
            read_bounded(stdout, self.capture_limit),
            read_bounded(stderr, self.capture_limit),
            child.wait(),
        );
        let (Ok((stdout, _)), Ok(_), Ok(status)) = (stdout, stderr, status) else {
            return Ok(false);
        };
        Ok(status.success() && String::from_utf8_lossy(&stdout).contains("--mount-socket"))
    }

    async fn checked(
        &self,
        operation: &'static str,
        args: &[String],
    ) -> Result<ExecOutput, VmError> {
        self.checked_with_network(operation, args, None, None).await
    }

    async fn checked_with_network(
        &self,
        operation: &'static str,
        args: &[String],
        network: Option<&NetworkPolicy>,
        staging_dir: Option<&Path>,
    ) -> Result<ExecOutput, VmError> {
        let mut command = self.sandboxed_command()?;
        match network {
            Some(NetworkPolicy::PublicOnly) => {
                command.env("SMOLVM_EGRESS_FLOOR", "strict");
            }
            Some(_) => {
                command.env_remove("SMOLVM_EGRESS_FLOOR");
            }
            None => {}
        }
        if let Some(staging_dir) = staging_dir {
            command.env("SMOLVM_PACK_STAGING", staging_dir);
            // smolvm-pack uses tempfile::tempdir() while assembling the
            // archive. Keep that scratch space beside the output instead of
            // falling back to a small host /tmp tmpfs.
            command.env("TMPDIR", staging_dir);
            // Pack export streams multi-GiB flattened layers; the general 4 GiB
            // file-transfer cap would abort a large golden pack.
            command.env("SMOLVM_FILE_TRANSFER_MAX_BYTES", "64GiB");
        }
        command
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|source| VmError::Launch {
            program: self.binary.display().to_string(),
            source,
        })?;
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let (stdout, stderr, status) = tokio::join!(
            read_bounded(stdout, self.capture_limit),
            read_bounded(stderr, self.capture_limit),
            child.wait(),
        );
        let (stdout, stdout_truncated) = stdout.map_err(|source| VmError::Launch {
            program: self.binary.display().to_string(),
            source,
        })?;
        let (stderr, stderr_truncated) = stderr.map_err(|source| VmError::Launch {
            program: self.binary.display().to_string(),
            source,
        })?;
        let status = status.map_err(|source| VmError::Launch {
            program: self.binary.display().to_string(),
            source,
        })?;
        let result = ExecOutput {
            exit_code: status.code().unwrap_or(-1),
            stdout,
            stderr,
            truncated: stdout_truncated || stderr_truncated,
        };
        if !status.success() {
            let message = String::from_utf8_lossy(&result.stderr).trim().to_owned();
            return Err(VmError::Command {
                operation,
                exit_code: result.exit_code,
                message,
            });
        }
        Ok(result)
    }

    /// Run an operation that constructs or replaces a machine's base image,
    /// excluding every other lifecycle operation for its duration.
    async fn exclusive(
        &self,
        operation: &'static str,
        args: &[String],
    ) -> Result<ExecOutput, VmError> {
        let _guard = self.lifecycle_lock.write().await;
        self.checked(operation, args).await
    }

    async fn exclusive_with_staging(
        &self,
        operation: &'static str,
        args: &[String],
        staging_dir: &Path,
    ) -> Result<ExecOutput, VmError> {
        let _guard = self.lifecycle_lock.write().await;
        self.checked_with_network(operation, args, None, Some(staging_dir))
            .await
    }

    async fn exclusive_with_network(
        &self,
        operation: &'static str,
        args: &[String],
        network: &NetworkPolicy,
    ) -> Result<ExecOutput, VmError> {
        let _guard = self.lifecycle_lock.write().await;
        self.checked_with_network(operation, args, Some(network), None)
            .await
    }

    /// Run an operation that only touches one already-defined machine.
    ///
    /// These run concurrently with each other: a pool replenishing several
    /// slots at once issues a delete and a fork per slot, and serializing them
    /// made the whole refill wait one VM operation at a time. They stay
    /// excluded from base construction, so a golden cannot be replaced
    /// underneath a fork.
    ///
    /// Forks additionally serialize per golden: see [`SmolVmProvider::fork`]
    /// for the checkpoint invariant that requires it.
    async fn concurrent(
        &self,
        operation: &'static str,
        args: &[String],
    ) -> Result<ExecOutput, VmError> {
        let _guard = self.lifecycle_lock.read().await;
        self.checked(operation, args).await
    }

    /// The mutex guarding forks from one golden, created on first use.
    ///
    /// Keyed by name rather than held on the golden record because a provider
    /// outlives any single golden and forks arrive from independent slot tasks.
    async fn fork_lock(&self, golden: &MachineName) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.fork_locks.lock().await;
        Arc::clone(
            locks
                .entry(golden.as_str().to_owned())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    /// Resolve a machine's on-disk data directory via `smolvm machine data-dir`.
    ///
    /// The directory is hash-derived, not name-derived, so it must be asked
    /// of smolvm rather than reconstructed from the platform cache layout.
    async fn machine_data_dir(&self, name: &MachineName) -> Result<PathBuf, VmError> {
        let output = self
            .concurrent(
                "data-dir",
                &[
                    "machine".into(),
                    "data-dir".into(),
                    "--name".into(),
                    name.as_str().into(),
                ],
            )
            .await?;
        let dir = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if dir.is_empty() {
            return Err(VmError::Protocol(format!(
                "smolvm machine data-dir for `{}` returned empty output",
                name.as_str()
            )));
        }
        Ok(PathBuf::from(dir))
    }
}

/// Best-effort recursive byte sum of a directory tree (metadata only).
fn dir_size_bytes(dir: &Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&current) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            // Do not follow symlinks: count the link itself, never its target.
            let Ok(meta) = std::fs::symlink_metadata(&path) else {
                continue;
            };
            if meta.is_dir() {
                stack.push(path);
            } else {
                total = total.saturating_add(meta.len());
            }
        }
    }
    total
}

/// Remove `<data_dir>/pack` build intermediates left by smolvm's pack export
/// (issue #295: `storage.ext4` + `layers/*.tar`, ~29 GB beside the finished
/// `storage.raw`, copied into every fork).
///
/// Only removes when the finished disk (`storage.raw`) exists beside it, and
/// only when `pack` is a real directory strictly inside `data_dir` — a
/// symlink (even one pointing inside) is never followed. Returns true when a
/// pack directory was removed.
fn prune_pack_dir(data_dir: &Path) -> Result<bool, VmError> {
    if !data_dir.join("storage.raw").is_file() {
        return Ok(false);
    }
    let pack = data_dir.join("pack");
    // Refuse symlinks outright: the removal must never follow a link.
    if std::fs::symlink_metadata(&pack)
        .map(|meta| meta.file_type().is_symlink())
        .unwrap_or(false)
    {
        return Ok(false);
    }
    let canonical_dir = data_dir
        .canonicalize()
        .map_err(|source| VmError::RuntimeIo { source })?;
    let canonical_pack = match pack.canonicalize() {
        Ok(path) => path,
        Err(_) => return Ok(false),
    };
    if !canonical_pack.is_dir()
        || canonical_pack == canonical_dir
        || !canonical_pack.starts_with(&canonical_dir)
    {
        return Ok(false);
    }
    let bytes = dir_size_bytes(&canonical_pack);
    std::fs::remove_dir_all(&canonical_pack).map_err(|source| VmError::RuntimeIo { source })?;
    info!(
        machine_dir = %canonical_dir.display(),
        reclaimed_bytes = bytes,
        "removed pack/ build intermediates beside finished disk"
    );
    Ok(true)
}

#[async_trait]
impl VmProvider for SmolVmProvider {
    async fn create(&self, spec: &MachineSpec) -> Result<(), VmError> {
        validate_spec(spec)?;
        let mut args = vec![
            "machine".into(),
            "create".into(),
            "--name".into(),
            spec.name.as_str().into(),
        ];
        // `.smolmachine` packs and other local files go through `--from`.
        // Docker-save OCI archives (`.tar`) are image inputs, not packs:
        // smolvm's `--image` accepts them and sets up virtiofs mounts the
        // same way it does for registry images, while `--from` machines do
        // not (bare rootfs directories lose mounts entirely).
        let is_pack = spec.image.ends_with(".smolmachine")
            || (Path::new(&spec.image).is_file() && !spec.image.ends_with(".tar"));
        if is_pack {
            args.extend(["--from".into(), spec.image.clone()]);
        } else {
            args.extend(["--image".into(), spec.image.clone()]);
        }
        args.extend([
            "--cpus".into(),
            spec.cpus.to_string(),
            "--mem".into(),
            spec.memory_mib.to_string(),
            "--storage".into(),
            spec.storage_gib.to_string(),
        ]);
        if let Some(overlay_gib) = spec.overlay_gib {
            args.extend(["--overlay".into(), overlay_gib.to_string()]);
        }
        match &spec.network {
            NetworkPolicy::Disabled => {}
            NetworkPolicy::Unrestricted => args.push("--net".into()),
            NetworkPolicy::PublicOnly => {
                args.extend([
                    "--net".into(),
                    "--net-backend".into(),
                    public_only_net_backend(self.env_lookup).into(),
                ]);
            }
            NetworkPolicy::Restricted { hosts, cidrs } => {
                for host in hosts {
                    args.extend(["--allow-host".into(), host.clone()]);
                }
                for cidr in cidrs {
                    args.extend(["--allow-cidr".into(), cidr.clone()]);
                }
            }
        }
        if let Some(dns) = &spec.dns {
            args.extend(["--dns".into(), dns.clone()]);
        }
        for mount in &spec.volumes {
            let mut value = format!("{}:{}", mount.host.display(), mount.guest.display());
            if mount.read_only {
                value.push_str(":ro");
            }
            args.extend(["--volume".into(), value]);
        }
        if !spec.sockets.is_empty()
            && !*self
                .socket_mount_supported
                .get_or_try_init(|| self.supports_mount_socket())
                .await?
        {
            return Err(VmError::UnsupportedSocketMount {
                binary: self.binary.display().to_string(),
            });
        }
        for mount in &spec.sockets {
            args.extend([
                "--mount-socket".into(),
                format!("{}:{}", mount.host.display(), mount.guest.display()),
            ]);
        }
        if !is_pack {
            // Everything this provider does runs through `machine exec`, so
            // pin a harmless keep-alive workload when the image declares no
            // usable startup command. Without it, machines whose record
            // carries no entrypoint/cmd (bare archives, the official runner
            // image's `bash` CMD) fail to start with libkrun's EINVAL once
            // the memory ceiling is raised. Images that declare a real
            // CMD/entrypoint keep their own workload — overriding it changes
            // runtime behavior. The workload must be the LAST positional
            // block: `machine create` treats everything after `--` as the
            // workload, so any flags appended later would be swallowed.
            if !image_has_startup_command(&spec.image) {
                args.extend([
                    "--".into(),
                    "/bin/sh".into(),
                    "-c".into(),
                    "sleep infinity".into(),
                ]);
            }
        }
        self.exclusive_with_network("create", &args, &spec.network)
            .await?;
        if spec.rosetta {
            let update_args = vec![
                "machine".into(),
                "update".into(),
                "--name".into(),
                spec.name.as_str().into(),
                "--rosetta".into(),
            ];
            if let Err(error) = self.exclusive("update", &update_args).await {
                let _ = self
                    .concurrent(
                        "delete",
                        &[
                            "machine".into(),
                            "delete".into(),
                            "--name".into(),
                            spec.name.as_str().into(),
                            "-f".into(),
                        ],
                    )
                    .await;
                return Err(error);
            }
        }
        Ok(())
    }

    /// Starting an already-defined machine constructs no base image, so it
    /// runs alongside forks. Holding the write lock here stalled the whole
    /// pool: a direct-created runner's cold boot (minutes on macOS while the
    /// guest unpacks its layers) blocked every fork until it finished.
    async fn start(&self, name: &MachineName) -> Result<(), VmError> {
        self.concurrent(
            "start",
            &[
                "machine".into(),
                "start".into(),
                "--name".into(),
                name.as_str().into(),
            ],
        )
        .await
        .map(|_| ())
    }

    async fn start_forkable(&self, name: &MachineName) -> Result<(), VmError> {
        self.exclusive(
            "start_forkable",
            &[
                "machine".into(),
                "start".into(),
                "--name".into(),
                name.as_str().into(),
                "--forkable".into(),
            ],
        )
        .await
        .map(|_| ())
    }

    /// Fork one clone from a golden, one fork per golden at a time.
    ///
    /// `--freeze-source` (SmolVM ≥1.18.1, upstream PR #1376) keeps the golden
    /// frozen as a reusable branch base: the first fork freezes the base and
    /// publishes a retained checkpoint, and later forks restore from that
    /// checkpoint instead of re-freezing or resuming the source onto new CoW
    /// layers. Live forking — the upstream default on Linux and macOS —
    /// resumes the source per fork, accumulating qcow2 backing layers until
    /// MAX_FORK_LINEAGE_DEPTH (32) wedges the pool, and on macOS
    /// re-checkpoints the source on every fork. Two forks racing the same
    /// golden break the invariant — the loser issues a second FORK against an
    /// already-paused VM, and that failure's rollback resumes the base and
    /// deletes the retained checkpoint. Every later fork then fails with
    /// `golden '<name>' is already paused; a valid retained checkpoint is
    /// required`, so the pool cannot produce another runner until the golden is
    /// rebuilt from scratch: queued jobs stall indefinitely, in exchange for
    /// the few hundred milliseconds a concurrent refill saves. SmolVM's own
    /// fork-pool controller serializes on the golden for the same reason.
    ///
    /// Different goldens still fork concurrently, and forks remain excluded
    /// from base construction, so a golden cannot be replaced underneath one.
    async fn fork(&self, golden: &MachineName, clone: &MachineName) -> Result<(), VmError> {
        let fork_lock = self.fork_lock(golden).await;
        let _fork_guard = fork_lock.lock().await;
        let result = self
            .concurrent(
                "fork",
                &[
                    "machine".into(),
                    "fork".into(),
                    "--golden".into(),
                    golden.as_str().into(),
                    "--name".into(),
                    clone.as_str().into(),
                    "--freeze-source".into(),
                ],
            )
            .await;
        if result.is_ok() {
            self.forked_machines
                .lock()
                .await
                .insert(clone.as_str().to_owned(), golden.as_str().to_owned());
            // Issue #295: smolvm's fork copies the golden's whole data dir,
            // pack/ build intermediates included. Prune the clone's copy;
            // never fail provisioning over best-effort disk hygiene.
            if let Err(error) = self.prune_pack_intermediates(clone).await {
                warn!(
                    clone = clone.as_str(),
                    %error,
                    "failed to prune pack/ intermediates from forked machine"
                );
            }
        }
        result.map(|_| ())
    }

    async fn prune_pack_intermediates(&self, name: &MachineName) -> Result<bool, VmError> {
        let data_dir = self.machine_data_dir(name).await?;
        // Removing ~29 GB of intermediates is metadata-heavy; keep it off the
        // async runtime.
        tokio::task::spawn_blocking(move || prune_pack_dir(&data_dir))
            .await
            .map_err(|error| VmError::Protocol(format!("pack prune task failed: {error}")))?
    }

    async fn sweep_orphaned_data_dirs(&self) -> Result<usize, VmError> {
        let registered = self.list().await?;
        let mut known_dirs = std::collections::BTreeSet::new();
        let mut roots = std::collections::BTreeSet::new();
        for name in &registered {
            let dir = self.machine_data_dir(name).await?;
            if let Some(parent) = dir.parent() {
                roots.insert(parent.to_path_buf());
            }
            known_dirs.insert(dir);
        }
        // Conventional roots cover the empty-registry case: with no machines
        // left, registered data dirs cannot reveal where orphans live.
        // Only scan the root belonging to the active registry configuration.
        if let Some(root) = machine_data_root() {
            roots.insert(root);
        }
        let roots: Vec<PathBuf> = roots.into_iter().collect();
        tokio::task::spawn_blocking(move || {
            sweep_orphaned_dirs(&roots, &known_dirs, ORPHAN_DIR_GRACE)
        })
        .await
        .map_err(|error| VmError::Protocol(format!("orphan sweep task failed: {error}")))?
    }

    async fn stop(&self, name: &MachineName) -> Result<(), VmError> {
        self.recovery(
            "stop",
            &[
                "machine".into(),
                "stop".into(),
                "--name".into(),
                name.as_str().into(),
            ],
        )
        .await
        .map(|_| ())
    }

    async fn delete(&self, name: &MachineName) -> Result<(), VmError> {
        let args = [
            "machine".into(),
            "delete".into(),
            "--name".into(),
            name.as_str().into(),
            "-f".into(),
        ];
        let mut attempts = 0;
        let result = loop {
            attempts += 1;
            match self.recovery("delete", &args).await {
                Ok(_) => break Ok(()),
                // Delete is idempotent. SmolVM may remove its registry entry
                // before a retry observes the partially cleaned directory.
                Err(VmError::Command { message, .. })
                    if message.to_ascii_lowercase().contains("not found") =>
                {
                    break Ok(());
                }

                // SmolVM 1.7.7 can race its final agent/log writes with the
                // recursive data-directory removal. The same force-delete
                // succeeds once those writers exit.
                Err(VmError::Command { ref message, .. })
                    if attempts < 3
                        && message.to_ascii_lowercase().contains("directory not empty") =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                // The machine registry is a single SQLite database; concurrent
                // create/fork/delete from the pool contends on it. A delete
                // that hits the lock leaves the machine leaked — and a leaked
                // clone is what blocks the golden's re-arm. Retry the same
                // bounded way; the lock clears once the competing operation
                // commits.
                Err(VmError::Command { ref message, .. })
                    if attempts < 3
                        && message.to_ascii_lowercase().contains("database is locked") =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(error) => break Err(error),
            }
        };
        if result.is_ok() {
            let mut forked = self.forked_machines.lock().await;
            forked.remove(name.as_str());
            // Deleting a golden must evict every clone forked from it. Without
            // this, a rebaked or recreated golden name would report live
            // clones forever, silently disabling the re-arm recovery.
            forked.retain(|_, owner| owner != name.as_str());
        }
        result
    }

    async fn status(&self, name: &MachineName) -> Result<MachineState, VmError> {
        let args = [
            "machine".into(),
            "status".into(),
            "--name".into(),
            name.as_str().into(),
        ];
        match self.recovery("status", &args).await {
            Ok(output) => {
                let text = String::from_utf8_lossy(&output.stdout).to_ascii_lowercase();
                Ok(if text.contains("running") {
                    MachineState::Running
                } else if text.contains("stopped") {
                    MachineState::Stopped
                } else {
                    MachineState::Unknown
                })
            }
            Err(VmError::Command { message, .. })
                if message.to_ascii_lowercase().contains("not found") =>
            {
                Ok(MachineState::Missing)
            }
            Err(error) => Err(error),
        }
    }

    async fn list(&self) -> Result<Vec<MachineName>, VmError> {
        let output = self
            .recovery("list", &["machine".into(), "ls".into(), "--json".into()])
            .await?;
        let values: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|error| VmError::Protocol(error.to_string()))?;
        values
            .as_array()
            .ok_or_else(|| VmError::Protocol("machine list was not an array".into()))?
            .iter()
            .filter_map(|value| value.get("name").and_then(serde_json::Value::as_str))
            .map(|name| MachineName::new(name.to_owned()))
            .collect()
    }

    async fn exec(&self, name: &MachineName, argv: &[String]) -> Result<ExecOutput, VmError> {
        if argv.is_empty() {
            return Err(VmError::InvalidSpec("guest command is empty".into()));
        }
        let mut args = vec![
            "machine".into(),
            "exec".into(),
            "--name".into(),
            name.as_str().into(),
            "--timeout".into(),
            EXEC_TIMEOUT.into(),
            "--".into(),
        ];
        args.extend_from_slice(argv);
        self.checked("exec", &args).await
    }

    async fn exec_with_secret_env(
        &self,
        name: &MachineName,
        argv: &[String],
        secrets: &[(String, SecretSource)],
    ) -> Result<ExecOutput, VmError> {
        if argv.is_empty() {
            return Err(VmError::InvalidSpec("guest command is empty".into()));
        }
        let mut args = vec![
            "machine".into(),
            "exec".into(),
            "--name".into(),
            name.as_str().into(),
            "--timeout".into(),
            EXEC_TIMEOUT.into(),
        ];
        for (guest, source) in secrets {
            if !is_env_identifier(guest) {
                return Err(VmError::InvalidSpec(
                    "secret environment names must be non-empty ASCII identifiers".into(),
                ));
            }
            match source {
                SecretSource::HostEnv(host) => {
                    if !is_env_identifier(host) {
                        return Err(VmError::InvalidSpec(
                            "secret environment names must be non-empty ASCII identifiers".into(),
                        ));
                    }
                    args.extend(["--secret-env".into(), format!("{guest}={host}")]);
                }
                SecretSource::HostFile(path) => {
                    // SmolVM resolves the path itself; a relative one would be
                    // read against its working directory, not the caller's.
                    if !path.is_absolute() {
                        return Err(VmError::InvalidSpec(
                            "secret file paths must be absolute".into(),
                        ));
                    }
                    args.extend([
                        "--secret-file".into(),
                        format!("{guest}={}", path.display()),
                    ]);
                }
            }
        }
        args.push("--".into());
        args.extend_from_slice(argv);
        self.checked("exec", &args).await
    }

    async fn exec_stream(
        &self,
        name: &MachineName,
        argv: &[String],
        output: mpsc::Sender<OutputChunk>,
    ) -> Result<i32, VmError> {
        if argv.is_empty() {
            return Err(VmError::InvalidSpec("guest command is empty".into()));
        }
        let mut command = self.sandboxed_command()?;
        command
            .args(["machine", "exec", "--stream", "--name", name.as_str(), "--"])
            .args(argv)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|source| VmError::Launch {
            program: self.binary.display().to_string(),
            source,
        })?;
        stream_output(&mut child, output)
            .await
            .map_err(|source| VmError::Launch {
                program: self.binary.display().to_string(),
                source,
            })
    }

    async fn copy(&self, source: &str, destination: &str) -> Result<(), VmError> {
        if source.is_empty() || destination.is_empty() {
            return Err(VmError::InvalidSpec("copy paths cannot be empty".into()));
        }
        self.checked(
            "copy",
            &[
                "machine".into(),
                "cp".into(),
                source.into(),
                destination.into(),
            ],
        )
        .await
        .map(|_| ())
    }

    async fn pack(&self, name: &MachineName, output: &Path) -> Result<(), VmError> {
        if !output.is_absolute() {
            return Err(VmError::InvalidSpec(
                "pack output path must be absolute".into(),
            ));
        }
        let staging_dir = output.parent().expect("absolute output has a parent");
        // smolvm 1.7.2 rejects `-o <name>.smolmachine` and writes the packed
        // VM data as `<output>.smolmachine` alongside an ELF launcher stub at
        // `<output>`. Strip the extension so the output path names the stub
        // and the caller picks up the `<output>.smolmachine` sidecar.
        let output = if output.extension().is_some_and(|ext| ext == "smolmachine") {
            output.with_extension("")
        } else {
            output.to_path_buf()
        };
        let mut args = vec![
            "pack".into(),
            "create".into(),
            "--from-vm".into(),
            name.as_str().into(),
        ];
        if let Some(proxy) = &self.pack_proxy {
            args.extend(["--proxy".into(), proxy.clone()]);
        }
        if let Some(no_proxy) = &self.pack_no_proxy {
            args.extend(["--no-proxy".into(), no_proxy.clone()]);
        }
        // Stage beside the output, not in smolvm's cache dir.
        //
        // `pack create` re-pulls the base inside its own export VM and
        // flushes the extracted layers, the merged layer, the agent rootfs
        // and an ext4 template to the staging dir: tens of GiB for a
        // runner-scale image. The default is the smolvm cache dir, which on
        // CI hosts sits on the small root filesystem — the golden bake died
        // there with `flushing extracted layers to disk failed (out of
        // space?)` while the output directory had room to spare. TMPDIR
        // below does not cover it; smolvm honours `--staging-dir` only.
        args.extend(["--staging-dir".into(), staging_dir.display().to_string()]);
        args.extend(["-o".into(), output.display().to_string()]);
        self.exclusive_with_staging("pack", &args, staging_dir)
            .await
            .map(|_| ())
    }

    async fn rearm_fork_base(
        &self,
        golden: &MachineName,
        partial: Option<&MachineName>,
    ) -> Result<bool, VmError> {
        let fork_lock = self.fork_lock(golden).await;
        let _fork_guard = fork_lock.lock().await;
        if let Some(partial) = partial {
            match self.delete(partial).await {
                Ok(()) => {}
                // A failed fork that never registered its clone leaves
                // nothing to remove; "not found" positively establishes the
                // clone is gone, which is all the cleanup contract needs.
                Err(VmError::Command { message, .. })
                    if message.to_ascii_lowercase().contains("not found") => {}
                Err(error) => return Err(error),
            }
        }
        {
            let forked = self.forked_machines.lock().await;
            if forked.values().any(|owner| owner == golden.as_str()) {
                return Ok(false);
            }
        }
        match self
            .concurrent(
                "stop",
                &[
                    "machine".into(),
                    "stop".into(),
                    "--name".into(),
                    golden.as_str().into(),
                ],
            )
            .await
        {
            Ok(_) => {}
            Err(VmError::Command { message, .. })
                if message.contains("stop or delete the clones first") =>
            {
                // SmolVM's registry knows live clones this process's
                // in-memory map does not (the map is rebuilt per engine
                // start, so clones leaked by an earlier session are
                // invisible here). Delete them so the golden can re-freeze;
                // `delete` retries transient registry-lock contention.
                warn!(
                    golden = golden.as_str(),
                    clones = extract_dependent_clones(&message).join(", "),
                    "re-arm blocked by clones unknown to this session; removing them"
                );
                for clone in extract_dependent_clones(&message) {
                    if let Ok(name) = MachineName::new(clone) {
                        let _ = self.delete(&name).await;
                    }
                }
                self.concurrent(
                    "stop",
                    &[
                        "machine".into(),
                        "stop".into(),
                        "--name".into(),
                        golden.as_str().into(),
                    ],
                )
                .await?;
            }
            Err(error) => return Err(error),
        }
        self.concurrent(
            "start",
            &[
                "machine".into(),
                "start".into(),
                "--name".into(),
                golden.as_str().into(),
                "--forkable".into(),
            ],
        )
        .await?;
        Ok(true)
    }
}

/// Grace window covering the create-before-register gap: smolvm writes the
/// data dir before the registry row commits, so a brand-new dir is not yet
/// provably orphaned.
const ORPHAN_DIR_GRACE: std::time::Duration = std::time::Duration::from_secs(120);

/// Delete children of `roots` that are not in `known_dirs` and are older than
/// `grace`. Synchronous: the sweep runs inside `spawn_blocking` because a
/// leaked VM dir can be gigabytes of metadata.
fn sweep_orphaned_dirs(
    roots: &[PathBuf],
    known_dirs: &std::collections::BTreeSet<PathBuf>,
    grace: std::time::Duration,
) -> Result<usize, VmError> {
    let mut swept = 0usize;
    for root in roots {
        let Ok(entries) = std::fs::read_dir(root) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if known_dirs.contains(&path) || !path.is_dir() {
                continue;
            }
            let old_enough = std::fs::metadata(&path)
                .and_then(|meta| meta.modified())
                .ok()
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age >= grace);
            if !old_enough {
                continue;
            }
            match std::fs::remove_dir_all(&path) {
                Ok(()) => {
                    swept += 1;
                    info!(path = %path.display(), "removed orphaned machine data dir");
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => warn!(
                    path = %path.display(),
                    %error,
                    "failed to remove orphaned machine data dir"
                ),
            }
        }
    }
    Ok(swept)
}

/// Clone machine names SmolVM names in its "stop or delete the clones first"
/// refusal, e.g. `is the fork base for 2 live clone(s) (clone-a, clone-b)`.
///
/// The names are the only authoritative list of what still depends on the
/// frozen base — the pool's own map can be empty after a restart even though
/// SmolVM's registry kept the clones.
fn extract_dependent_clones(message: &str) -> Vec<String> {
    let Some(open) = message.find("live clone(s) (") else {
        return Vec::new();
    };
    let rest = &message[open + "live clone(s) (".len()..];
    let Some(close) = rest.find(')') else {
        return Vec::new();
    };
    rest[..close]
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Whether a name is usable as a shell environment variable identifier.
fn is_env_identifier(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

// ---------------------------------------------------------------------------
// VM sandbox environment (seccomp / Landlock / per-VM cgroup)
// ---------------------------------------------------------------------------
//
// SmolVM's `_boot-vm` always hardens the process (no_new_privs, non-dumpable,
// core limit zero), but its seccomp syscall allowlist and Landlock filesystem
// confinement are opt-in knobs: `smolvm serve` defaults `SMOLVM_SECCOMP` and
// `SMOLVM_LANDLOCK` to `enforce`, while the standalone `machine ...` commands
// Preloop drives do not. Preloop therefore applies the same defaults itself,
// on Linux, for every invocation that can boot or restart a VM.
//
// The upstream convention is preserved: a value the operator already set in
// the environment wins over the default — exactly what `serve` documents
// ("a pre-set SMOLVM_SECCOMP env var takes precedence"). Unlike upstream,
// Preloop validates the pre-set value instead of forwarding garbage: smolvm
// treats any unrecognized mode as "off", so forwarding a typo would silently
// boot VMs unconfined. An unrecognized override is a hard error.
//
// On non-Linux hosts both controls are no-ops upstream (Landlock is a Linux
// LSM; the seccomp filter installs only on Linux), so nothing is injected and
// macOS behavior is unchanged.
//
// The per-VM cgroup root is separate, because claiming one *writes* to the
// cgroup hierarchy. Only a supervisor may do that, and only once, through
// [`init_vm_cgroup_delegation`]; every other process (the CLI's `shell`,
// `machine exec`, `machine cp`) resolves the root read-only and simply goes
// without caps when there is no usable delegation already in place.

/// How the sandbox policy resolves an operator override. Production passes
/// [`process_env`]; unit tests pass a pure map so the policy is exercised
/// without mutating process-global state.
type EnvLookup = fn(&str) -> Option<String>;

/// The process environment — the production [`EnvLookup`].
fn process_env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

/// The cgroup v2 root passed to `_boot-vm` as `SMOLVM_CGROUP_ROOT`, resolved
/// at most once per process. Whichever path runs first wins, so a supervisor
/// that called [`init_vm_cgroup_delegation`] keeps the delegated root it
/// established, and every other process observes only what already exists.
///
/// `OnceLock`, not `LazyLock`: the initializer is precisely what differs
/// between the two paths, so it cannot be fixed at the declaration.
#[cfg(target_os = "linux")]
static CGROUP_ROOT: std::sync::OnceLock<Option<PathBuf>> = std::sync::OnceLock::new();

/// Claim the cgroup v2 delegation the per-VM resource caps need.
///
/// Call this exactly once, from the supervisor (`preloop serve`) at startup,
/// before the async runtime spawns the VM pool — it mirrors what `smolvm
/// serve` does through its own `setup_cgroup_delegation_root`. It is the only
/// entry point allowed to mutate the cgroup hierarchy: it may create a
/// `preloop-supervisor` leaf, move this process into it, and enable
/// `cpu`/`memory`/`pids` distribution on the parent.
///
/// That write is what makes the caps exist. systemd's `Delegate=cpu memory
/// pids` chowns the unit's subtree to the service user and lists the
/// controllers in `cgroup.controllers`, but leaves `cgroup.subtree_control`
/// **empty**; a `vm-<pid>` leaf created under it then has no `cpu.max`,
/// `memory.max` or `pids.max` at all. Enabling the controllers ourselves is
/// what turns a delegated subtree into one that can actually cap VMs.
///
/// Best-effort: any failure leaves the process without a cgroup root, so VMs
/// boot uncapped rather than not at all. No-op off Linux.
pub fn init_vm_cgroup_delegation() {
    #[cfg(target_os = "linux")]
    {
        CGROUP_ROOT.get_or_init(delegated_cgroup_root_mutating);
    }
}

/// The Linux VM-sandbox environment for a pending SmolVM invocation.
///
/// This is the single source of the sandbox policy: the provider applies it
/// to every command it builds, and the CLI applies it to its direct
/// `machine exec/cp/shell` calls, which can implicitly boot or restart a
/// stopped machine. Returns `(variable, value)` pairs plus removals;
/// non-Linux hosts get an empty list (both controls are no-ops upstream
/// there).
///
/// - Seccomp: default `enforce`; a pre-set operator value
///   (`enforce`/`audit`/`off`) wins — upstream precedence — but only modes
///   SmolVM honors pass; anything else is an error rather than the silent
///   "off" upstream would treat it as.
/// - Landlock: default `enforce`; pre-set `enforce`/`off` wins, validated.
/// - Per-VM cgroup: `SMOLVM_CGROUP_ROOT` names whichever root this process
///   resolved first. In a supervisor that called
///   [`init_vm_cgroup_delegation`] that is the root it established, writes
///   included. In every other process it is read-only: the variable is set
///   only when this process already sits in a cgroup v2 subtree that has
///   `cpu`/`memory`/`pids` enabled in its `cgroup.subtree_control` and can
///   host child leaves, and is otherwise **removed from the child
///   environment** — Preloop is authoritative for all three variables, so an
///   inherited value (from the operator's environment or a parent service)
///   never reaches `_boot-vm` with a root we did not validate.
pub fn smolvm_sandbox_env() -> Result<SandboxEnv, VmError> {
    sandbox_env_with(process_env)
}

/// [`smolvm_sandbox_env`] over an explicit environment lookup.
fn sandbox_env_with(lookup: EnvLookup) -> Result<SandboxEnv, VmError> {
    #[cfg(target_os = "linux")]
    {
        let mut env = sandbox_env_from(lookup)?;
        // Authoritative: if this process resolved no usable cgroup root, an
        // inherited SMOLVM_CGROUP_ROOT must be stripped, not merely left unset
        // (the child would inherit it and _boot-vm would place itself in a
        // cgroup we never validated).
        let mut remove = Vec::new();
        if let Some(root) = CGROUP_ROOT.get_or_init(read_only_cgroup_root) {
            env.push(("SMOLVM_CGROUP_ROOT".to_owned(), root.display().to_string()));
        } else {
            remove.push("SMOLVM_CGROUP_ROOT");
        }
        Ok(SandboxEnv { set: env, remove })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = lookup;
        Ok(SandboxEnv::default())
    }
}

/// The sandbox policy as command-environment operations.
#[derive(Default)]
pub struct SandboxEnv {
    /// Variables to set on the child command.
    pub set: Vec<(String, String)>,
    /// Variables to strip from the child command's inherited environment.
    pub remove: Vec<&'static str>,
}

/// The seccomp/Landlock half of the sandbox policy, over an environment
/// lookup: defaults to `enforce`, honors a validated operator override, and
/// rejects any value upstream would silently read as "control off".
#[cfg(any(target_os = "linux", test))]
fn sandbox_env_from(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Vec<(String, String)>, VmError> {
    fn invalid(variable: &'static str, value: String, expected: &'static str) -> VmError {
        VmError::InvalidSandboxEnv {
            variable,
            value,
            expected,
        }
    }

    let mut env = Vec::with_capacity(3);
    match lookup("SMOLVM_SECCOMP") {
        Some(value) if matches!(value.as_str(), "enforce" | "audit" | "off") => {
            env.push(("SMOLVM_SECCOMP".to_owned(), value));
        }
        Some(value) => {
            return Err(invalid("SMOLVM_SECCOMP", value, "enforce, audit, or off"));
        }
        None => env.push(("SMOLVM_SECCOMP".to_owned(), "enforce".to_owned())),
    }
    match lookup("SMOLVM_LANDLOCK") {
        Some(value) if matches!(value.as_str(), "enforce" | "off") => {
            env.push(("SMOLVM_LANDLOCK".to_owned(), value));
        }
        Some(value) => {
            return Err(invalid("SMOLVM_LANDLOCK", value, "enforce or off"));
        }
        None => env.push(("SMOLVM_LANDLOCK".to_owned(), "enforce".to_owned())),
    }
    Ok(env)
}

/// Apply [`smolvm_sandbox_env`] to a synchronous command (the CLI's direct
/// `smolvm machine exec/cp/shell` spawns).
pub fn apply_smolvm_sandbox_env(command: &mut std::process::Command) -> Result<(), VmError> {
    let sandbox = smolvm_sandbox_env()?;
    for key in sandbox.remove {
        command.env_remove(key);
    }
    for (key, value) in sandbox.set {
        command.env(key, value);
    }
    Ok(())
}

/// Preloop home the helper isolates SmolVM into: the explicit `PRELOOP_HOME`,
/// else `<HOME>/.preloop` (the CLI's `preloop_home()` default), else the
/// bare `.preloop` relative directory.
fn effective_preloop_home() -> Option<PathBuf> {
    std::env::var_os("PRELOOP_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".preloop")))
}

/// Directory SmolVM keeps per-machine data dirs under for the active registry
/// configuration: `$SMOLVM_DATA_DIR/vms`, else the isolated Preloop home's
/// platform cache layout. This is where golden and job-VM disks land, so it is
/// the filesystem disk-space checks must measure.
pub fn machine_data_root() -> Option<PathBuf> {
    if let Some(data_dir) = std::env::var_os("SMOLVM_DATA_DIR").map(PathBuf::from) {
        return Some(data_dir.join("vms"));
    }
    let home = effective_preloop_home()?;
    #[cfg(target_os = "macos")]
    return Some(home.join("smolvm-home/Library/Caches/smolvm/vms"));
    #[cfg(not(target_os = "macos"))]
    return Some(home.join("smolvm/vms"));
}

/// Bytes available to an unprivileged writer on the filesystem holding
/// `path`, measured at its nearest existing ancestor (the target directory may
/// not exist yet). Uses POSIX `df -Pk`, which every supported host ships, so
/// no FFI dependency is needed for a `statvfs` call.
pub fn filesystem_available_bytes(path: &Path) -> Result<u64, VmError> {
    let mut probe = path;
    while !probe.exists() {
        probe = probe.parent().ok_or_else(|| {
            VmError::Protocol(format!("no existing ancestor for {}", path.display()))
        })?;
    }
    let output = std::process::Command::new("df")
        .arg("-Pk")
        .arg(probe)
        .output()
        .map_err(|error| VmError::Protocol(format!("df failed to start: {error}")))?;
    if !output.status.success() {
        return Err(VmError::Protocol(format!(
            "df -Pk {} exited with {}",
            probe.display(),
            output.status
        )));
    }
    parse_df_available_kib(&String::from_utf8_lossy(&output.stdout))
        .map(|kib| kib.saturating_mul(1024))
        .ok_or_else(|| VmError::Protocol(format!("unparseable df output for {}", probe.display())))
}

/// `Available` column (KiB) from `df -Pk` output. Located as the field just
/// before the `Capacity` percentage rather than by fixed index, so a
/// filesystem or mount name containing spaces cannot shift it.
fn parse_df_available_kib(output: &str) -> Option<u64> {
    let line = output.lines().nth(1)?;
    let fields: Vec<&str> = line.split_whitespace().collect();
    let capacity = fields.iter().position(|field| {
        field
            .strip_suffix('%')
            .is_some_and(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
    })?;
    fields.get(capacity.checked_sub(1)?)?.parse().ok()
}

/// Candidate locations for the SmolVM guest agent rootfs, in probe order.
///
/// SmolVM keeps its data directory — and the agent rootfs inside it — at the
/// platform default (`~/Library/Application Support/smolvm` on macOS,
/// `~/.local/share/smolvm` on Linux) or wherever `SMOLVM_DATA_DIR` points.
/// Preloop derives its own registry dir from the effective Preloop home and
/// may isolate the child's `HOME` on macOS, so the REAL host locations must
/// be probed explicitly or a standard install's agent rootfs is never found
/// (the isolated HOME sends smolvm to an empty directory).
fn agent_rootfs_candidates(host_home: Option<&Path>, data_dir: Option<&Path>) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if let Some(data_dir) = data_dir {
        candidates.push(data_dir.join("agent-rootfs"));
    }
    if let Some(home) = host_home {
        #[cfg(target_os = "macos")]
        {
            candidates.push(home.join("Library/Application Support/smolvm/agent-rootfs"));
            candidates.push(home.join(".smolvm/agent-rootfs"));
        }
        #[cfg(not(target_os = "macos"))]
        {
            candidates.push(home.join(".local/share/smolvm/agent-rootfs"));
            candidates.push(home.join(".smolvm/agent-rootfs"));
        }
    }
    candidates
}

/// Executable name libkrun's hypervisor shows up as in `ps`.
const BOOT_VM_PROCESS: &str = "_boot-vm";
/// Boot config `_boot-vm`'s argv names inside its machine data directory.
///
/// smolvm consumes the file while booting — a running machine's directory no
/// longer contains it — but the path stays in argv for the life of the
/// process and names the machine's data directory, which is the durable
/// evidence of whether that machine still exists.
const BOOT_CONFIG_FILE: &str = "boot-config.json";

/// What a hypervisor's `ps` argv says about its machine data directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MachineDataState {
    /// The data directory the argv names still exists, so the machine is
    /// live: registered, running a job, a golden fork base, or still being
    /// created.
    Present,
    /// The data directory is gone: it was removed from under the hypervisor,
    /// which still holds that machine's storage descriptors open.
    Removed,
    /// No token names a boot config, so nothing can be concluded. Treated as
    /// live: the mid-flight purge only kills what it can positively identify
    /// as orphaned, because the other answer is to kill a running machine.
    Unknown,
}

/// Classify a `ps` line by the machine state its argv names.
///
/// The boot config is handed to `_boot-vm` as a path, either bare or behind a
/// flag (`--boot-config=<path>`), so the path starts at the data-root marker
/// inside whichever token carries it. The marker may be the shorter `smolvm`
/// prefix of the macOS `smolvm-home` root, so the earliest occurrence bounds
/// the path. Only a token that names `boot-config.json` decides anything; an
/// argv shape this cannot parse yields [`MachineDataState::Unknown`].
fn machine_data_state(line: &str, markers: &[String]) -> MachineDataState {
    let mut state = MachineDataState::Unknown;
    for token in line.split_whitespace() {
        let Some(start) = markers
            .iter()
            .filter_map(|marker| token.find(marker.as_str()))
            .min()
        else {
            continue;
        };
        let path = token[start..].trim_matches(['"', '\'']);
        // The boot config itself is gone once the machine booted, so the
        // directory is what proves the machine exists.
        if !path.starts_with('/') || !path.ends_with(BOOT_CONFIG_FILE) {
            continue;
        }
        let Some(dir) = Path::new(path).parent() else {
            continue;
        };
        if dir.exists() {
            return MachineDataState::Present;
        }
        state = MachineDataState::Removed;
    }
    state
}

/// Which `_boot-vm` hypervisors a [`purge_orphaned_vms`] call may kill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanPurge {
    /// Every hypervisor whose argv names this Preloop home's data root.
    ///
    /// Only safe where nothing of ours can be running: pool startup, after
    /// the stale machines have been deleted, and shutdown.
    All,
    /// Only hypervisors whose machine data directory is already gone — the
    /// directory the `boot-config.json` in their argv names no longer exists.
    ///
    /// Safe while the engine is serving. A registered machine, a running job
    /// VM, a golden fork base, and a create in flight all have their data
    /// directory on disk (smolvm writes the directory before it launches the
    /// hypervisor that reads its config out of it), so none of them can
    /// match. What does match is a hypervisor holding storage descriptors for
    /// files nothing can reach again, which is exactly the leak a failed
    /// `machine delete` leaves behind.
    RemovedDataDir,
}

/// Kill any lingering SmolVM `_boot-vm` hypervisor processes whose machine
/// state lives under this Preloop home.
///
/// A server death (crash, OOM, SIGKILL) orphans the detached `_boot-vm`
/// processes: the CLI flow detaches them (the parent-death watchdog is
/// off by design), and only the in-VM agent or an explicit `machine stop`
/// can shut them down. If the machine's data directory is then removed out
/// from under the live VM (a home cleanup), the smolvm database no longer
/// knows the machine, so `machine delete` cannot reach it — the `_boot-vm`
/// keeps the storage file descriptors open and the unlinked blocks leak
/// until the process exits. The `_boot-vm` argv names its
/// `boot-config.json` under the data dir, so orphaned processes are
/// identifiable by path.
///
/// `scope` decides which of those processes may be killed; see
/// [`OrphanPurge`]. Called at pool startup (crash recovery, `All`) and on an
/// interval while the pool serves (`RemovedDataDir`). Returns the number of
/// processes killed.
pub fn purge_orphaned_vms(scope: OrphanPurge) -> Result<usize, VmError> {
    let Some(preloop_home) = effective_preloop_home() else {
        return Ok(0);
    };
    let markers: Vec<String> = [
        preloop_home.join("smolvm"),
        preloop_home.join("smolvm-home"),
    ]
    .iter()
    .filter_map(|path| path.to_str().map(str::to_owned))
    .collect();
    if markers.is_empty() {
        return Ok(0);
    }
    let output = std::process::Command::new("ps")
        .args(["ax", "-o", "pid=,command="])
        .output()
        .map_err(|source| VmError::Launch {
            program: "ps".to_owned(),
            source,
        })?;
    let text = String::from_utf8_lossy(&output.stdout);
    let mut killed = 0usize;
    for line in text.lines() {
        if !line.contains(BOOT_VM_PROCESS) {
            continue;
        }
        if !markers.iter().any(|marker| line.contains(marker.as_str())) {
            continue;
        }
        // Anything whose machine data directory still exists may be live:
        // spare it rather than kill a machine that is registered, running a
        // job, or still being created. See `OrphanPurge::RemovedDataDir`.
        if scope == OrphanPurge::RemovedDataDir
            && machine_data_state(line, &markers) != MachineDataState::Removed
        {
            continue;
        }
        let Some(pid) = line
            .split_whitespace()
            .next()
            .and_then(|token| token.parse::<i32>().ok())
        else {
            continue;
        };
        if pid <= 0 {
            continue;
        }
        // SIGKILL: the machine is unreachable through smolvm (no record, or
        // an unresponsive agent) — the hypervisor process must die to
        // release the storage fds.
        let status = std::process::Command::new("kill")
            .args(["-9", &pid.to_string()])
            .status();
        if matches!(status, Ok(status) if status.success()) {
            killed += 1;
        }
    }
    Ok(killed)
}

fn smolvm_runtime_env(binary: Option<&Path>) -> Vec<(String, std::ffi::OsString)> {
    let host_home = std::env::var_os("HOME").map(PathBuf::from);
    let mut env = Vec::new();
    let explicit_data_dir = std::env::var_os("SMOLVM_DATA_DIR").map(PathBuf::from);
    let data_dir = explicit_data_dir
        .clone()
        .or_else(|| effective_preloop_home().map(|home| home.join("smolvm")));
    if explicit_data_dir.is_none()
        && let Some(data_dir) = &data_dir
    {
        // SmolVM 1.8.x ignores SMOLVM_DATA_DIR on macOS and derives its
        // registry from HOME. Keep each Preloop home isolated while still
        // leaving an explicit operator registry untouched.
        #[cfg(target_os = "macos")]
        if let Some(preloop_home) = effective_preloop_home() {
            env.push((
                "HOME".to_owned(),
                preloop_home.join("smolvm-home").into_os_string(),
            ));
        }
        env.push((
            "SMOLVM_DATA_DIR".to_owned(),
            data_dir.clone().into_os_string(),
        ));
    }

    // HOME may be isolated above, so preserve the host installation assets
    // explicitly for the child SmolVM process: the agent rootfs lives in the
    // REAL host's platform data dir, which the isolated HOME would miss.
    if std::env::var_os("SMOLVM_AGENT_ROOTFS").is_none()
        && let Some(path) = agent_rootfs_candidates(host_home.as_deref(), data_dir.as_deref())
            .into_iter()
            .find(|path| path.is_dir())
    {
        env.push(("SMOLVM_AGENT_ROOTFS".to_owned(), path.into_os_string()));
    }

    #[cfg(target_os = "macos")]
    {
        let mut candidates = Vec::new();
        if let Some(path) = std::env::var_os("SMOLVM_LIB_DIR") {
            candidates.push(PathBuf::from(path));
        }
        if let Some(binary) = binary {
            let resolved = binary
                .canonicalize()
                .ok()
                .or_else(|| binary.is_absolute().then(|| binary.to_path_buf()));
            if let Some(parent) = resolved.and_then(|path| path.parent().map(Path::to_path_buf)) {
                candidates.push(parent.join("lib"));
            }
        }
        if let Some(home) = host_home {
            candidates.push(home.join(".smolvm/lib"));
        }
        let Some(lib_dir) = candidates
            .into_iter()
            .find(|path| path.join("libkrunfw.5.dylib").is_file())
        else {
            return env;
        };
        let mut paths = vec![lib_dir];
        if let Some(existing) = std::env::var_os("DYLD_LIBRARY_PATH") {
            paths.extend(std::env::split_paths(&existing));
        }
        if let Ok(loader_path) = std::env::join_paths(paths) {
            env.push(("DYLD_LIBRARY_PATH".to_owned(), loader_path));
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = binary;
    }
    env
}

/// Add the bundled SmolVM libraries to boot-capable child commands on macOS.
///
/// The macOS release binary dynamically loads `libkrunfw.5.dylib` from the
/// adjacent `.smolvm/lib` directory, but the binary has no rpath and the
/// bootstrap process does not inherit a loader path from the installer.
/// Without this, `machine create` succeeds and every subsequent VM start
/// fails at the Hypervisor.framework boundary. An explicit `SMOLVM_LIB_DIR`
/// wins; otherwise use the resolved binary's sibling directory or the
/// standard per-user install location.
/// Directories derived from the effective Preloop home that must exist before
/// SmolVM starts (it does not create a missing data dir itself).
fn runtime_dirs_to_create() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if std::env::var_os("SMOLVM_DATA_DIR").is_none()
        && let Some(home) = effective_preloop_home()
    {
        dirs.push(home.join("smolvm"));
        #[cfg(target_os = "macos")]
        dirs.push(home.join("smolvm-home"));
    }
    dirs
}

/// Add the bundled SmolVM libraries to boot-capable child commands on macOS.
///
/// The macOS release binary dynamically loads `libkrunfw.5.dylib` from the
/// adjacent `.smolvm/lib` directory, but the binary has no rpath and the
/// bootstrap process does not inherit a loader path from the installer.
/// Without this, `machine create` succeeds and every subsequent VM start
/// fails at the Hypervisor.framework boundary. An explicit `SMOLVM_LIB_DIR`
/// wins; otherwise use the resolved binary's sibling directory or the
/// standard per-user install location.
pub fn apply_smolvm_runtime_env(
    command: &mut std::process::Command,
    binary: Option<&Path>,
) -> Result<(), VmError> {
    for dir in runtime_dirs_to_create() {
        std::fs::create_dir_all(&dir).map_err(|source| VmError::RuntimeIo { source })?;
    }
    for (key, value) in smolvm_runtime_env(binary) {
        command.env(key, value);
    }
    Ok(())
}

fn apply_smolvm_runtime_env_async(
    command: &mut Command,
    binary: Option<&Path>,
) -> Result<(), VmError> {
    for dir in runtime_dirs_to_create() {
        std::fs::create_dir_all(&dir).map_err(|source| VmError::RuntimeIo { source })?;
    }
    for (key, value) in smolvm_runtime_env(binary) {
        command.env(key, value);
    }
    Ok(())
}

/// Apply the sandbox policy to a provider command.
#[cfg(target_os = "linux")]
fn apply_sandbox_env(command: &mut Command, lookup: EnvLookup) -> Result<(), VmError> {
    let sandbox = sandbox_env_with(lookup)?;
    for key in sandbox.remove {
        command.env_remove(key);
    }
    for (key, value) in sandbox.set {
        command.env(key, value);
    }
    Ok(())
}

/// Non-Linux: seccomp and Landlock are no-ops upstream; nothing to set.
#[cfg(not(target_os = "linux"))]
fn apply_sandbox_env(_command: &mut Command, _lookup: EnvLookup) -> Result<(), VmError> {
    Ok(())
}

/// This process's own cgroup v2 directory, or `None` on a cgroup v1 / hybrid
/// host (no unified `0::` line). Mirrors upstream's `cgroup_v2_self_dir`.
#[cfg(target_os = "linux")]
fn process_cgroup_dir() -> Option<PathBuf> {
    // Not cgroup v2 (no unified hierarchy).
    if !Path::new("/sys/fs/cgroup/cgroup.controllers").is_file() {
        return None;
    }
    let rel = std::fs::read_to_string("/proc/self/cgroup")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("0::"))?
        .trim()
        .to_owned();
    if rel.is_empty() {
        return None;
    }
    Some(cgroup_root_dir(&rel))
}

/// Resolve a cgroup v2 root without changing anything in the hierarchy: the
/// answer for every process that is not the supervisor. `None` means no
/// usable delegation, so VMs run without host resource caps (the seccomp and
/// Landlock variables are unaffected).
#[cfg(target_os = "linux")]
fn read_only_cgroup_root() -> Option<PathBuf> {
    process_cgroup_dir().filter(|root| is_usable_delegation_read_only(root))
}

/// Resolve a cgroup v2 root, establishing the delegation when one is missing
/// and this process is privileged enough to create it. Supervisor-only — see
/// [`init_vm_cgroup_delegation`], the sole caller.
#[cfg(target_os = "linux")]
fn delegated_cgroup_root_mutating() -> Option<PathBuf> {
    let root = process_cgroup_dir()?;
    // Already delegated (e.g. a systemd unit with `Delegate=cpu memory pids`
    // that also enabled the controllers in `cgroup.subtree_control`):
    // VMM leaves can be created and capped immediately. The cgroup may hold
    // this process (controllers were enabled by systemd while it was empty),
    // which is fine — the no-internal-process rule only constrains enabling,
    // not hosting children — but the root must actually accept child leaves:
    // an unprivileged process in an undelegated cgroup must not be handed a
    // root whose `vm-<pid>` leaf creation would silently fail.
    if is_usable_delegation_read_only(&root) {
        return Some(root);
    }
    // The controllers are not distributed yet, but this process may be able
    // to enable them itself (a root supervisor, or a systemd unit with
    // `Delegate=` — which chowns the subtree to the service user yet leaves
    // `cgroup.subtree_control` empty, so without this write the per-VM
    // leaves get no `cpu.max`/`memory.max`/`pids.max` at all). The kernel
    // only accepts a `cgroup.subtree_control` write on a cgroup with no
    // member processes, so move into a leaf first — upstream's
    // `setup_cgroup_delegation_root` does exactly this.
    // Any failure means no delegation: caps are skipped, never fatal.
    let leaf = root.join("preloop-supervisor");
    let pid = std::process::id().to_string();
    // A leaf left behind by an earlier run is fine — an empty cgroup still
    // accepts new members, and re-enabling already-enabled controllers is a
    // no-op, so only the two writes matter.
    let _ = std::fs::create_dir(&leaf);
    let ok = fs_write(&leaf.join("cgroup.procs"), &pid)
        && fs_write(&root.join("cgroup.subtree_control"), "+cpu +memory +pids")
        && can_host_child_leaf(&root);
    ok.then_some(root)
}

/// Whether `root` is a delegation Preloop can actually cap VMs with, using
/// only reads: the controllers are distributed to children and the current
/// user can create a child leaf.
///
/// Deliberately non-mutating. This predicate runs on the read-only path used
/// by `preloop shell` and the debug session, which must never write to the
/// cgroup hierarchy; the mutating probe below is confined to the
/// supervisor's delegation setup where a write is already expected.
#[cfg(target_os = "linux")]
fn is_usable_delegation_read_only(root: &Path) -> bool {
    cgroup_controllers_enabled(root) && can_create_child_leaf(root)
}

/// Whether the current user can create a child cgroup directory under `root`
/// without creating anything. A directory may be writable/executable while
/// the cgroup filesystem still refuses `mkdir` — under systemd delegation
/// the subtree is chowned to the service user, and `CGROUP_DELEGATE` file
/// ownership is what actually authorizes child creation — so this checks
/// write+execute permission on the directory (the closest read-only proxy)
/// rather than assuming.
#[cfg(target_os = "linux")]
fn can_create_child_leaf(root: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(root) else {
        return false;
    };
    use std::os::unix::fs::PermissionsExt;
    let mode = metadata.permissions().mode();
    // Owner w+x for the owner, group w+x for the group, other w+x for the
    // rest — exactly the permissions `mkdir` requires of the parent.
    let owner_wx = mode & 0o300 == 0o300;
    let group_wx = mode & 0o030 == 0o030;
    let other_wx = mode & 0o003 == 0o003;
    owner_wx || group_wx || other_wx
}

/// The cgroup can host `_boot-vm`'s `vm-<pid>` child leaves, proven by
/// actually creating and removing a probe leaf. Supervisor-only: this is a
/// write, so it must never run on the read-only CLI path.
///
/// The probe leaf is removed again; a leaf that cannot be removed is not a
/// usable root either, and reporting `false` keeps `.preloop-probe` from
/// being left behind on a path that claims success.
#[cfg(target_os = "linux")]
fn can_host_child_leaf(root: &Path) -> bool {
    let probe = root.join(".preloop-probe");
    std::fs::create_dir(&probe)
        .and_then(|()| std::fs::remove_dir(&probe))
        .is_ok()
}

#[cfg(target_os = "linux")]
fn fs_write(path: &Path, value: &str) -> bool {
    use std::io::Write;
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .and_then(|mut file| file.write_all(value.as_bytes()))
        .is_ok()
}

/// Whether `dir/cgroup.subtree_control` enables the controllers Preloop caps
/// on (`cpu`, `memory`, `pids`) — the kernel's record of a delegation.
///
/// Accepts both readback spellings seen in the wild: `+cpu +memory +pids`
/// (sign-prefixed) and `cpu memory pids` (bare). A `-`-prefixed controller
/// is disabled either way.
#[cfg(any(target_os = "linux", test))]
fn cgroup_controllers_enabled(dir: &Path) -> bool {
    let Ok(control) = std::fs::read_to_string(dir.join("cgroup.subtree_control")) else {
        return false;
    };
    let enabled: std::collections::HashSet<&str> = control
        .split_whitespace()
        .filter_map(|token| match token.strip_prefix('-') {
            Some(_) => None,
            None => Some(token.strip_prefix('+').unwrap_or(token)),
        })
        .collect();
    enabled.contains("cpu") && enabled.contains("memory") && enabled.contains("pids")
}

/// Map a `/proc/self/cgroup` v2 entry to the on-disk cgroup directory.
///
/// The file prints `0::/system.slice/preloop.service` — an absolute-looking
/// path whose leading slash `Path::join` would treat as a fresh root and
/// discard `/sys/fs/cgroup` — so the slash is trimmed before joining. That
/// is the whole mapping, exactly as upstream's `cgroup_v2_self_dir` does it:
/// the on-disk directory name is the systemd unit name verbatim, `\xHH`
/// escapes included (`mnt-my\x2ddir.mount` is a real directory; the
/// unescaped `mnt-my-dir.mount` is not).
#[cfg(any(target_os = "linux", test))]
fn cgroup_root_dir(rel: &str) -> PathBuf {
    Path::new("/sys/fs/cgroup").join(rel.trim_start_matches('/'))
}

/// Whether an image declares a usable startup command (entrypoint or cmd).
///
/// Registry references and packs carry their own entrypoint/cmd in the image
/// or pack metadata, so the provider assumes they have one and never overrides
/// it with the keep-alive workload. A local OCI archive (docker-save `.tar`)
/// is inspected from its `manifest.json` + config: if the config names neither
/// an entrypoint nor a cmd, the machine would boot with an empty record and
/// can fail to start (libkrun EINVAL) once the memory ceiling is raised.
fn image_has_startup_command(image: &str) -> bool {
    let path = Path::new(image);
    if !path.is_file() || image.ends_with(".smolmachine") {
        return true;
    }
    // Docker-save archive: read manifest.json, then the config file it names.
    let manifest = read_tar_json_member(path, "manifest.json").and_then(|value| {
        value
            .get(0)
            .and_then(|entry| entry.get("Config"))
            .and_then(|config| config.as_str())
            .map(str::to_owned)
    });
    let Some(config_name) = manifest else {
        return true;
    };
    let Some(config) = read_tar_json_member(path, &config_name) else {
        return true;
    };
    // docker-save nests the image config under `config`; the OCI layout keeps
    // it at the top level.
    let config = config.get("config").unwrap_or(&config);
    let has_entrypoint = config
        .get("Entrypoint")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|v| !v.is_empty());
    let has_cmd = config
        .get("Cmd")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|v| !v.is_empty());
    has_entrypoint || has_cmd
}

/// Read and parse one member of a docker-save tar by name (prefix-stripped).
fn read_tar_json_member(tar_path: &Path, member: &str) -> Option<serde_json::Value> {
    let Ok(file) = File::open(tar_path) else {
        return None;
    };
    let mut archive = tar::Archive::new(file);
    let Ok(entries) = archive.entries() else {
        return None;
    };
    let member = member.trim_start_matches("./");
    for mut entry in entries.flatten() {
        let Ok(entry_path) = entry.path() else {
            continue;
        };
        if entry_path.to_string_lossy().trim_start_matches("./") == member {
            let mut raw = Vec::new();
            if entry.read_to_end(&mut raw).is_err() {
                return None;
            }
            return serde_json::from_slice(&raw).ok();
        }
    }
    None
}

fn validate_spec(spec: &MachineSpec) -> Result<(), VmError> {
    if spec.image.trim().is_empty()
        || spec.cpus == 0
        || spec.memory_mib < 128
        || spec.storage_gib == 0
    {
        return Err(VmError::InvalidSpec(
            "image, CPU, memory, and storage must be non-zero".into(),
        ));
    }
    for mount in &spec.volumes {
        if !mount.host.is_absolute() || !mount.guest.is_absolute() {
            return Err(VmError::InvalidSpec("volume paths must be absolute".into()));
        }
        if !Path::new(&mount.host).exists() {
            return Err(VmError::InvalidSpec(format!(
                "volume source does not exist: {}",
                mount.host.display()
            )));
        }
    }
    for mount in &spec.sockets {
        if !mount.host.is_absolute() || !mount.guest.is_absolute() {
            return Err(VmError::InvalidSpec("socket paths must be absolute".into()));
        }
        validate_socket_source(&mount.host)?;
    }
    Ok(())
}

/// A socket mount punches a hole in the guest boundary, so the host path must
/// be exactly what the caller named: a real socket, reached without traversing
/// a symlink that could be repointed at another endpoint.
#[cfg(unix)]
fn validate_socket_source(host: &Path) -> Result<(), VmError> {
    use std::os::unix::fs::FileTypeExt;

    let symlink_meta = std::fs::symlink_metadata(host).map_err(|error| {
        VmError::InvalidSpec(format!(
            "socket source does not exist: {} ({error})",
            host.display()
        ))
    })?;
    if symlink_meta.file_type().is_symlink() {
        return Err(VmError::InvalidSpec(format!(
            "socket source must not be a symlink: {}",
            host.display()
        )));
    }
    if !symlink_meta.file_type().is_socket() {
        return Err(VmError::InvalidSpec(format!(
            "socket source is not a Unix socket: {}",
            host.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_socket_source(host: &Path) -> Result<(), VmError> {
    Err(VmError::InvalidSpec(format!(
        "socket mounts require Unix: {}",
        host.display()
    )))
}

async fn read_bounded(
    mut reader: impl AsyncRead + Unpin,
    limit: usize,
) -> std::io::Result<(Vec<u8>, bool)> {
    let mut retained = Vec::with_capacity(limit.min(64 * 1024));
    let mut chunk = [0_u8; 16 * 1024];
    let mut truncated = false;
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        let available = limit.saturating_sub(retained.len());
        retained.extend_from_slice(&chunk[..read.min(available)]);
        truncated |= read > available;
    }
    Ok((retained, truncated))
}

async fn forward(
    mut reader: impl AsyncRead + Unpin,
    output: mpsc::Sender<OutputChunk>,
    stdout: bool,
) -> std::io::Result<()> {
    let mut chunk = vec![0_u8; 16 * 1024];
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            return Ok(());
        }
        let value = if stdout {
            OutputChunk::Stdout(chunk[..read].to_vec())
        } else {
            OutputChunk::Stderr(chunk[..read].to_vec())
        };
        if output.send(value).await.is_err() {
            return Ok(());
        }
    }
}

/// Forward a child's piped stdout and stderr to `output` and return its exit
/// code.
///
/// Shared by every provider's streaming exec: both readers are drained
/// concurrently with the wait, so a child that fills one pipe while the other
/// stays idle cannot deadlock, and a dropped receiver ends forwarding without
/// killing the child.
async fn stream_output(
    child: &mut tokio::process::Child,
    output: mpsc::Sender<OutputChunk>,
) -> std::io::Result<i32> {
    let stdout = forward(
        child.stdout.take().expect("piped stdout"),
        output.clone(),
        true,
    );
    let stderr = forward(child.stderr.take().expect("piped stderr"), output, false);
    let (stdout, stderr, status) = tokio::join!(stdout, stderr, child.wait());
    stdout?;
    stderr?;
    Ok(status?.code().unwrap_or(-1))
}

#[cfg(test)]
mod tests {

    /// Serializes the tests that rewrite `PRELOOP_HOME`: `purge_orphaned_vms`
    /// resolves the Preloop home from the process environment, so two of them
    /// running concurrently would purge each other's scratch home.
    static PRELOOP_HOME_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    /// Sets `PRELOOP_HOME` for the test's scope and restores it on drop.
    struct PreloopHomeGuard {
        previous: Option<std::ffi::OsString>,
    }

    impl PreloopHomeGuard {
        fn set(home: &std::path::Path) -> Self {
            let previous = std::env::var_os("PRELOOP_HOME");
            // SAFETY: single-threaded test section under `PRELOOP_HOME_LOCK`.
            unsafe { std::env::set_var("PRELOOP_HOME", home) };
            Self { previous }
        }
    }

    impl Drop for PreloopHomeGuard {
        fn drop(&mut self) {
            match self.previous.take() {
                // SAFETY: single-threaded test section under `PRELOOP_HOME_LOCK`.
                Some(value) => unsafe { std::env::set_var("PRELOOP_HOME", value) },
                None => unsafe { std::env::remove_var("PRELOOP_HOME") },
            }
        }
    }

    /// Spawn a stand-in for a detached `_boot-vm` whose argv carries `args`.
    ///
    /// `sh -c '… & wait'` keeps the shell alive with the marker visible in its
    /// argv (a bare `sh -c cmd` execs cmd, dropping the args).
    fn spawn_fake_boot_vm(args: &[&str]) -> std::process::Child {
        let argv = ["-c", "sleep 60 & wait", "_boot-vm"]
            .into_iter()
            .chain(args.iter().copied());
        std::process::Command::new("sh")
            .args(argv)
            .spawn()
            .expect("spawn dummy boot-vm")
    }

    fn process_alive(pid: i32) -> bool {
        // A dead pid makes `kill` print "No such process"; the exit status is
        // the answer, so keep its stderr out of the test output.
        std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stderr(std::process::Stdio::null())
            .status()
            .expect("probe process")
            .success()
    }

    /// Scratch machine data dir for the pack-prune tests: `<tmp>/pack-prune-<uuid>/`.
    fn pack_prune_scratch() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("preloop-pack-prune-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        dir
    }

    #[test]
    fn prune_pack_dir_removes_pack_beside_finished_disk() {
        let data_dir = pack_prune_scratch();
        std::fs::write(data_dir.join("storage.raw"), b"disk").unwrap();
        let pack = data_dir.join("pack");
        std::fs::create_dir_all(pack.join("layers")).unwrap();
        std::fs::write(pack.join("storage.ext4"), b"intermediate").unwrap();
        std::fs::write(pack.join("layers").join("abc.tar"), b"layer").unwrap();

        assert!(super::prune_pack_dir(&data_dir).expect("prune"));
        assert!(!pack.exists(), "pack/ must be gone");
        assert!(data_dir.join("storage.raw").is_file(), "disk must survive");
        std::fs::remove_dir_all(&data_dir).ok();
    }

    #[test]
    fn prune_pack_dir_skips_when_disk_missing() {
        let data_dir = pack_prune_scratch();
        let pack = data_dir.join("pack");
        std::fs::create_dir_all(&pack).unwrap();
        std::fs::write(pack.join("storage.ext4"), b"intermediate").unwrap();

        assert!(!super::prune_pack_dir(&data_dir).expect("prune"));
        assert!(
            pack.is_dir(),
            "pack/ must be left alone without storage.raw"
        );
        std::fs::remove_dir_all(&data_dir).ok();
    }

    /// A data dir with no registry entry is unreachable garbage once the
    /// create-before-register grace window passes; the sweep must take it
    /// while sparing registered dirs and anything too young to be proven
    /// orphaned.
    #[test]
    fn sweep_orphaned_dirs_removes_unregistered_only() {
        let root = std::env::temp_dir().join(format!("preloop-orphan-{}", uuid::Uuid::new_v4()));
        let known = root.join("registered");
        let orphan = root.join("orphaned");
        std::fs::create_dir_all(&known).unwrap();
        std::fs::create_dir_all(&orphan).unwrap();
        let known_dirs = std::collections::BTreeSet::from([known.clone()]);

        // Zero grace: every unregistered dir is provably old enough.
        let swept = super::sweep_orphaned_dirs(
            std::slice::from_ref(&root),
            &known_dirs,
            std::time::Duration::ZERO,
        )
        .expect("sweep");

        assert_eq!(swept, 1);
        assert!(known.is_dir(), "registered dir survives");
        assert!(!orphan.exists(), "orphaned dir is removed");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn df_available_column_survives_spaces_in_names() {
        // Linux: plain layout.
        let linux = "Filesystem     1024-blocks      Used Available Capacity Mounted on\n\
                     /dev/nvme0n1p2   960379920 512000000 399560000      57% /\n";
        assert_eq!(parse_df_available_kib(linux), Some(399_560_000));
        // macOS automounter: the filesystem name itself contains a space, so a
        // fixed column index would read `Used` as `Available`.
        let spaced_fs = "Filesystem    1024-blocks Used Available Capacity  Mounted on\n\
                         map auto_home           0    0         0   100%    /System/Volumes/Data/home\n";
        assert_eq!(parse_df_available_kib(spaced_fs), Some(0));
        // A mount point containing spaces sits after Capacity and must not
        // matter either.
        let spaced_mount = "Filesystem 1024-blocks Used Available Capacity Mounted on\n\
                            /dev/disk3s5 971350180 698000000 235000000 75% /Volumes/My Disk\n";
        assert_eq!(parse_df_available_kib(spaced_mount), Some(235_000_000));
    }

    #[test]
    fn df_output_without_a_data_row_is_rejected() {
        assert_eq!(
            parse_df_available_kib("Filesystem 1024-blocks Used Available Capacity Mounted on\n"),
            None
        );
        assert_eq!(parse_df_available_kib(""), None);
    }

    #[test]
    fn available_bytes_measures_nearest_existing_ancestor() {
        let missing = std::env::temp_dir().join(format!("preloop-df-{}/a/b", uuid::Uuid::new_v4()));
        let bytes = filesystem_available_bytes(&missing).expect("ancestor temp dir is measurable");
        assert!(bytes > 0, "temp filesystem reports no free space");
    }

    /// The grace window keeps a dir too young to be proven orphaned: smolvm
    /// writes the data dir before the registry row commits, so a fresh dir
    /// may belong to a create still in flight.
    #[test]
    fn sweep_orphaned_dirs_spares_young_dirs() {
        let root = std::env::temp_dir().join(format!("preloop-orphan-{}", uuid::Uuid::new_v4()));
        let young = root.join("young");
        std::fs::create_dir_all(&young).unwrap();

        let swept = super::sweep_orphaned_dirs(
            std::slice::from_ref(&root),
            &std::collections::BTreeSet::new(),
            super::ORPHAN_DIR_GRACE,
        )
        .expect("sweep");

        assert_eq!(swept, 0);
        assert!(young.is_dir(), "young dir survives the grace window");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn prune_pack_dir_skips_when_no_pack() {
        let data_dir = pack_prune_scratch();
        std::fs::write(data_dir.join("storage.raw"), b"disk").unwrap();

        assert!(!super::prune_pack_dir(&data_dir).expect("prune"));
        std::fs::remove_dir_all(&data_dir).ok();
    }

    #[test]
    fn prune_pack_dir_refuses_symlink_pack() {
        let data_dir = pack_prune_scratch();
        std::fs::write(data_dir.join("storage.raw"), b"disk").unwrap();
        let target = data_dir.join("real-target");
        std::fs::create_dir_all(&target).unwrap();
        std::fs::write(target.join("keep"), b"data").unwrap();
        std::os::unix::fs::symlink(&target, data_dir.join("pack")).unwrap();

        assert!(!super::prune_pack_dir(&data_dir).expect("prune"));
        assert!(target.join("keep").is_file(), "symlink target must survive");
        std::fs::remove_dir_all(&data_dir).ok();
    }

    #[test]
    fn prune_pack_dir_counts_bytes_without_following_links() {
        let data_dir = pack_prune_scratch();
        std::fs::write(data_dir.join("a"), vec![7u8; 100]).unwrap();
        let sub = data_dir.join("sub");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(sub.join("b"), vec![8u8; 50]).unwrap();
        std::os::unix::fs::symlink(data_dir.join("a"), sub.join("link")).unwrap();
        // 100 (a) + 50 (b) + the link's own bytes; the link target's 100
        // bytes are not counted twice, i.e. the link is not followed.
        let link_len = std::fs::symlink_metadata(sub.join("link")).unwrap().len();
        assert_eq!(super::dir_size_bytes(&data_dir), 150 + link_len);
        std::fs::remove_dir_all(&data_dir).ok();
    }

    #[test]
    fn purge_orphaned_vms_kills_matching_boot_vm_processes() {
        let _guard = PRELOOP_HOME_LOCK.lock();
        let home = std::env::temp_dir().join(format!("preloop-purge-{}", uuid::Uuid::new_v4()));
        let marker = home.join("smolvm-home/Library/Caches/smolvm/vms/deadbeef/boot-config.json");
        // A process that looks like an orphaned _boot-vm for this home: the
        // argv carries the marker path, so `ps` shows it in the command line.
        let mut child = spawn_fake_boot_vm(&[marker.to_str().unwrap()]);
        let pid = child.id() as i32;
        let _home = PreloopHomeGuard::set(&home);
        let killed = purge_orphaned_vms(OrphanPurge::All).expect("purge");
        assert!(killed >= 1, "purge should have killed the matching process");
        // Reap the SIGKILLed child so it is not a zombie (kill -0 on a
        // zombie still succeeds until it is reaped).
        let _ = child.wait();
        assert!(!process_alive(pid), "matching _boot-vm must be dead");
    }

    /// The mid-flight scope must spare every hypervisor whose machine data
    /// directory still exists — a registered machine, a running job VM, a
    /// golden, or a create in flight all keep theirs — spare an argv shape it
    /// cannot classify, and still reclaim one whose data directory was
    /// removed from under it. `All` kills every survivor, which is what makes
    /// the scope, not the process match, the protection.
    #[test]
    fn removed_data_dir_purge_spares_live_and_unknown_data_dirs() {
        let _guard = PRELOOP_HOME_LOCK.lock();
        let home =
            std::env::temp_dir().join(format!("preloop-purge-live-{}", uuid::Uuid::new_v4()));
        let vms = home.join("smolvm-home/Library/Caches/smolvm/vms");
        // A live machine's directory, in the shape a running machine leaves on
        // disk: smolvm consumes `boot-config.json` at boot, so only the
        // directory — not that file — is durable evidence of the machine.
        let live_dir = vms.join("live");
        std::fs::create_dir_all(&live_dir).expect("create live machine dir");
        std::fs::write(live_dir.join("vm.lock"), b"").unwrap();
        let live = live_dir.join("boot-config.json");
        // Same home, no data directory: the failed-delete leak.
        let gone = vms.join("gone/boot-config.json");
        // The data root itself, in an argv shape that names no boot config:
        // nothing can be concluded about that process, so it must be spared.
        let unclassified = home.join("smolvm-home");
        let mut live_child = spawn_fake_boot_vm(&[live.to_str().unwrap()]);
        let mut gone_child = spawn_fake_boot_vm(&[gone.to_str().unwrap()]);
        let mut unknown_child = spawn_fake_boot_vm(&["--data-dir", unclassified.to_str().unwrap()]);
        let live_pid = live_child.id() as i32;
        let gone_pid = gone_child.id() as i32;
        let unknown_pid = unknown_child.id() as i32;
        let _home = PreloopHomeGuard::set(&home);

        assert_eq!(
            purge_orphaned_vms(OrphanPurge::RemovedDataDir).expect("purge"),
            1,
            "only the hypervisor whose data directory is gone may be killed"
        );
        assert!(
            process_alive(live_pid),
            "a machine whose data directory exists must survive the mid-flight purge"
        );
        assert!(
            process_alive(unknown_pid),
            "an unclassifiable argv must survive the mid-flight purge"
        );
        let _ = gone_child.wait();
        assert!(
            !process_alive(gone_pid),
            "the orphaned hypervisor must be dead"
        );

        assert_eq!(
            purge_orphaned_vms(OrphanPurge::All).expect("purge"),
            2,
            "the startup scope kills the hypervisors the mid-flight scope spared"
        );
        let _ = live_child.wait();
        let _ = unknown_child.wait();
        assert!(!process_alive(live_pid), "only the scope spared it");
        assert!(!process_alive(unknown_pid), "only the scope spared it");
        std::fs::remove_dir_all(&home).ok();
    }

    #[test]
    fn agent_rootfs_candidates_probe_platform_then_legacy() {
        let dir = tempfile::tempdir().expect("temp dir");
        let home = dir.path();
        #[cfg(target_os = "macos")]
        let platform = home.join("Library/Application Support/smolvm/agent-rootfs");
        #[cfg(not(target_os = "macos"))]
        let platform = home.join(".local/share/smolvm/agent-rootfs");
        let legacy = home.join(".smolvm/agent-rootfs");
        std::fs::create_dir_all(&platform).expect("platform agent rootfs");
        std::fs::create_dir_all(&legacy).expect("legacy agent rootfs");
        let found = agent_rootfs_candidates(Some(home), None)
            .into_iter()
            .find(|path| path.is_dir())
            .expect("a candidate exists");
        assert_eq!(
            found, platform,
            "the platform default must be probed before the legacy location"
        );

        // An explicit SMOLVM_DATA_DIR wins over the platform defaults.
        let explicit = home.join("custom/agent-rootfs");
        std::fs::create_dir_all(&explicit).expect("explicit agent rootfs");
        let found = agent_rootfs_candidates(Some(home), Some(home.join("custom").as_path()))
            .into_iter()
            .find(|path| path.is_dir())
            .expect("explicit candidate exists");
        assert_eq!(found, explicit);
    }

    use super::*;

    #[test]
    fn cgroup_root_dir_trims_the_leading_slash_of_proc_self_cgroup_paths() {
        // `/proc/self/cgroup` prints `0::/system.slice/preloop.service` with
        // an absolute-looking path; Path::join would discard the /sys/fs/cgroup
        // base entirely.
        assert_eq!(
            cgroup_root_dir("/system.slice/preloop.service"),
            PathBuf::from("/sys/fs/cgroup/system.slice/preloop.service")
        );
        assert_eq!(
            cgroup_root_dir("user.slice/user-1000.slice/user@1000.service/app.slice"),
            PathBuf::from("/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice")
        );
        // The root cgroup prints `/`.
        assert_eq!(cgroup_root_dir("/"), PathBuf::from("/sys/fs/cgroup"));
    }

    /// systemd's `\xHH` escaping is part of the directory name on disk:
    /// `/sys/fs/cgroup/system.slice/mnt-my\x2ddir.mount` exists while the
    /// unescaped spelling does not, and `/proc/self/cgroup` reports the
    /// escaped form. Decoding it would name a directory that is not there.
    #[test]
    fn cgroup_root_dir_preserves_systemd_escapes_verbatim() {
        assert_eq!(
            cgroup_root_dir("/system.slice/mnt-my\\x2ddir.mount"),
            PathBuf::from("/sys/fs/cgroup/system.slice/mnt-my\\x2ddir.mount")
        );
    }

    /// The exact refusal SmolVM emits when a re-arm tries to stop a golden
    /// that still has live clones — names parsed from the message are the
    /// authoritative dependency list when the pool's in-memory clone map is
    /// empty (fresh engine start after a session that leaked machines).
    #[test]
    fn extract_dependent_clones_parses_smolvm_refusal() {
        assert_eq!(
            extract_dependent_clones(
                "Error: agent operation failed: stop: machine 'preloop-runner-golden' is the \
                 fork base for 2 live clone(s) (preloop-runner-535-1, preloop-runner-642-1); \
                 stop or delete the clones first"
            ),
            vec!["preloop-runner-535-1", "preloop-runner-642-1"]
        );
        assert_eq!(
            extract_dependent_clones("is the fork base for 1 live clone(s) (preloop-runner-9-1)"),
            vec!["preloop-runner-9-1"]
        );
    }

    #[test]
    fn extract_dependent_clones_ignores_unrelated_errors() {
        assert!(extract_dependent_clones("vm not found: preloop-runner-golden").is_empty());
        assert!(extract_dependent_clones("").is_empty());
        assert!(extract_dependent_clones("database is locked").is_empty());
    }

    #[test]
    fn cgroup_controllers_enabled_requires_all_three_controllers() {
        let directory = tempfile::tempdir().unwrap();
        let control = directory.path().join("cgroup.subtree_control");
        // Kernel readback with sign prefixes.
        std::fs::write(&control, "+cpu +memory +pids\n").unwrap();
        assert!(cgroup_controllers_enabled(directory.path()));
        // Readback without sign prefixes.
        std::fs::write(&control, "cpu memory pids\n").unwrap();
        assert!(cgroup_controllers_enabled(directory.path()));
        std::fs::write(&control, "+cpu +memory +pids +io\n").unwrap();
        assert!(cgroup_controllers_enabled(directory.path()));
        std::fs::write(&control, "+cpu +memory\n").unwrap();
        assert!(!cgroup_controllers_enabled(directory.path()));
        std::fs::write(&control, "cpu memory\n").unwrap();
        assert!(!cgroup_controllers_enabled(directory.path()));
        std::fs::write(&control, "-cpu +memory +pids\n").unwrap();
        assert!(!cgroup_controllers_enabled(directory.path()));
        std::fs::write(&control, "+cpu -memory +pids\n").unwrap();
        assert!(!cgroup_controllers_enabled(directory.path()));
        std::fs::remove_file(&control).unwrap();
        assert!(!cgroup_controllers_enabled(directory.path()));
    }

    /// The exported policy is the exact observable contract: on Linux every
    /// invocation defaults to seccomp/Landlock `enforce` and honors only
    /// validated overrides; everywhere else nothing is injected.
    #[test]
    fn sandbox_env_defaults_to_enforce_and_validates_overrides() {
        assert_eq!(
            sandbox_env_from(fake_env(&[])).unwrap(),
            vec![
                ("SMOLVM_SECCOMP".to_owned(), "enforce".to_owned()),
                ("SMOLVM_LANDLOCK".to_owned(), "enforce".to_owned()),
            ]
        );

        // A pre-set value upstream honors wins over the default.
        for (seccomp, landlock) in [("enforce", "enforce"), ("audit", "off"), ("off", "enforce")] {
            assert_eq!(
                sandbox_env_from(fake_env(&[
                    ("SMOLVM_SECCOMP", seccomp),
                    ("SMOLVM_LANDLOCK", landlock),
                ]))
                .unwrap(),
                vec![
                    ("SMOLVM_SECCOMP".to_owned(), seccomp.to_owned()),
                    ("SMOLVM_LANDLOCK".to_owned(), landlock.to_owned()),
                ]
            );
        }

        // Anything else is a hard error: upstream would read it as "off".
        for (variable, value) in [
            ("SMOLVM_SECCOMP", "banana"),
            ("SMOLVM_SECCOMP", ""),
            ("SMOLVM_SECCOMP", "Enforce"),
            ("SMOLVM_LANDLOCK", "banana"),
            ("SMOLVM_LANDLOCK", "audit"),
            ("SMOLVM_LANDLOCK", ""),
        ] {
            assert!(
                matches!(
                    sandbox_env_from(fake_env(&[(variable, value)])),
                    Err(VmError::InvalidSandboxEnv { variable: v, value: ref got, .. })
                        if v == variable && got == value
                ),
                "{variable}={value} must fail closed"
            );
        }
    }

    /// An [`EnvLookup`]-shaped view over a fixed set of pairs: the sandbox
    /// policy is exercised without touching the process environment, so
    /// these tests carry no ordering dependence.
    fn fake_env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| {
            pairs
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| (*value).to_owned())
        }
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn sandbox_env_is_empty_off_linux() {
        let sandbox = smolvm_sandbox_env().unwrap();
        assert!(sandbox.set.is_empty());
        assert!(sandbox.remove.is_empty());
    }

    /// Preloop is authoritative for `SMOLVM_CGROUP_ROOT`: when no usable
    /// delegation exists, an inherited value must be removed from the child
    /// environment, never forwarded. The test environment never calls
    /// `init_vm_cgroup_delegation`, so this process takes the read-only path;
    /// on a host with a delegation the variable is set instead, and the
    /// assertion below reflects that.
    #[cfg(target_os = "linux")]
    #[test]
    fn sandbox_policy_strips_an_inherited_cgroup_root_without_delegation() {
        let sandbox = smolvm_sandbox_env().unwrap();
        let resolved_root = sandbox
            .set
            .iter()
            .any(|(key, _)| key == "SMOLVM_CGROUP_ROOT");
        if resolved_root {
            // Delegated host: the variable is set to our own validated root,
            // and must not ALSO be scheduled for removal.
            assert!(!sandbox.remove.contains(&"SMOLVM_CGROUP_ROOT"));
        } else {
            assert!(sandbox.remove.contains(&"SMOLVM_CGROUP_ROOT"));
        }
        // Regardless of the host, the seccomp/Landlock halves are enforced.
        assert!(
            sandbox
                .set
                .iter()
                .any(|(key, value)| key == "SMOLVM_SECCOMP" && value == "enforce")
        );
        assert!(
            sandbox
                .set
                .iter()
                .any(|(key, value)| key == "SMOLVM_LANDLOCK" && value == "enforce")
        );
    }

    /// An invalid override must not wedge the recovery operations: a typo in
    /// `SMOLVM_SECCOMP` must make `create` fail closed while `list`, `status`,
    /// `stop`, and `delete` still work — they are the only way to fix the
    /// typo. The recovery command strips the invalid variables rather than
    /// forwarding them.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn invalid_override_keeps_recovery_operations_usable() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let binary = directory.path().join("smolvm");
        std::fs::write(
            &binary,
            "#!/bin/sh\ncase \"$*\" in\n  *ls*) echo '[]';;\n  *status*) echo running;;\nesac\n",
        )
        .unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();

        fn rejected(key: &str) -> Option<String> {
            (key == "SMOLVM_SECCOMP").then(|| "Enforce".to_owned())
        }
        let provider = SmolVmProvider::new(&binary).with_env_lookup(rejected);

        // Boot-capable path: fails closed.
        let spec = MachineSpec {
            name: MachineName::new("runner").unwrap(),
            image: "ghcr.io/acme/runner:latest".to_owned(),
            cpus: 2,
            memory_mib: 256,
            storage_gib: 10,
            overlay_gib: None,
            network: NetworkPolicy::Disabled,
            volumes: Vec::new(),
            sockets: Vec::new(),
            dns: None,
            rosetta: false,
        };
        let create_error = provider.create(&spec).await.unwrap_err();
        assert!(
            matches!(create_error, VmError::InvalidSandboxEnv { .. }),
            "create must fail closed, got {create_error}"
        );

        // Recovery paths: still usable, with the invalid variables stripped.
        assert_eq!(
            provider
                .list()
                .await
                .unwrap_or_else(|error| panic!("list must stay usable: {error}")),
            Vec::<MachineName>::new()
        );
        assert_eq!(
            provider
                .status(&MachineName::new("runner").unwrap())
                .await
                .unwrap_or_else(|error| panic!("status must stay usable: {error}")),
            MachineState::Running
        );
        provider
            .stop(&MachineName::new("runner").unwrap())
            .await
            .unwrap();
        provider
            .delete(&MachineName::new("runner").unwrap())
            .await
            .unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delete_retries_transient_nonempty_directory_and_is_idempotent() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let binary = directory.path().join("smolvm");
        let marker = directory.path().join("delete-attempted");
        std::fs::write(
            &binary,
            format!(
                "#!/bin/sh\nif [ ! -f '{}' ]; then\n  touch '{}'\n  echo 'failed to remove machine data: Directory not empty (os error 66)' >&2\n  exit 1\nfi\n",
                marker.display(),
                marker.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();

        let provider = SmolVmProvider::new(&binary);
        provider
            .delete(&MachineName::new("runner").unwrap())
            .await
            .expect("transient nonempty-directory cleanup must be retried");
        assert!(marker.exists());

        // A missing machine is already deleted and must satisfy the same
        // idempotent provider contract.
        std::fs::write(&binary, "#!/bin/sh\necho 'machine not found' >&2\nexit 1\n").unwrap();
        provider
            .delete(&MachineName::new("runner").unwrap())
            .await
            .expect("deleting an absent machine must succeed");
    }

    /// A rejected sandbox override must surface as
    /// [`VmError::InvalidSandboxEnv`] — never as the misleading
    /// `UnsupportedSocketMount`, which would blame the binary — and must not
    /// be remembered as a capability verdict: once the operator fixes the
    /// environment the very next call probes the binary for real, on the
    /// same provider whose probe cell the failure could have poisoned.
    /// Linux-only: the sandbox policy (and so any override) applies there.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn invalid_sandbox_override_never_poisons_the_socket_mount_probe() {
        use std::os::unix::fs::PermissionsExt;

        fn rejected(key: &str) -> Option<String> {
            (key == "SMOLVM_SECCOMP").then(|| "banana".to_owned())
        }
        fn unset(_key: &str) -> Option<String> {
            None
        }

        let directory = tempfile::tempdir().unwrap();
        // Advertises `--mount-socket` to the capability probe and accepts
        // every other invocation.
        let binary = directory.path().join("smolvm");
        std::fs::write(&binary, "#!/bin/sh\nprintf -- '--mount-socket\\n'\n").unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
        let host_socket = directory.path().join("engine.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&host_socket).unwrap();
        let spec = MachineSpec {
            name: MachineName::new("probe").unwrap(),
            image: "ghcr.io/acme/runner:latest".to_owned(),
            cpus: 2,
            memory_mib: 256,
            storage_gib: 10,
            overlay_gib: None,
            network: NetworkPolicy::Disabled,
            volumes: Vec::new(),
            sockets: vec![SocketMount {
                host: host_socket,
                guest: PathBuf::from("/run/preloop-engine.sock"),
            }],
            dns: None,
            rosetta: false,
        };

        let provider = SmolVmProvider::new(&binary).with_env_lookup(rejected);
        let error = provider.create(&spec).await.unwrap_err();
        assert!(
            matches!(
                error,
                VmError::InvalidSandboxEnv {
                    variable: "SMOLVM_SECCOMP",
                    ..
                }
            ),
            "expected InvalidSandboxEnv, got {error}"
        );

        // Same provider, same (still unset) probe cell, corrected environment.
        provider
            .clone()
            .with_env_lookup(unset)
            .create(&spec)
            .await
            .unwrap();
    }

    /// The egress-floor policy needs a backend that carries it, so
    /// virtio-net is the default and the only recognized escape hatch is an
    /// exact `tsi`; anything else (including a typo) keeps virtio-net rather
    /// than silently dropping the floor.
    #[test]
    fn public_only_net_backend_defaults_to_virtio_net_and_honors_only_exact_overrides() {
        assert_eq!(public_only_net_backend(fake_env(&[])), "virtio-net");
        assert_eq!(
            public_only_net_backend(fake_env(&[("PRELOOP_SMOLVM_NET_BACKEND", "tsi")])),
            "tsi"
        );
        assert_eq!(
            public_only_net_backend(fake_env(&[("PRELOOP_SMOLVM_NET_BACKEND", "virtio-net")])),
            "virtio-net"
        );
        for bogus in ["", "TSI", "gvproxy"] {
            assert_eq!(
                public_only_net_backend(fake_env(&[("PRELOOP_SMOLVM_NET_BACKEND", bogus)])),
                "virtio-net",
                "`{bogus}` must not select a backend that cannot carry the egress floor"
            );
        }
    }
}
