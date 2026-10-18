//! AgentENV (`aenv`) backed [`VmProvider`] for KVM/Firecracker hosts.
//!
//! # Why a second provider
//!
//! SmolVM is a libkrun/Hypervisor.framework runtime: one process per VM, a
//! local machine registry, host directory and Unix-socket passthrough. AgentENV
//! ([`kvcache-ai/AgentENV`](https://github.com/kvcache-ai/AgentENV)) is a
//! Firecracker control plane: a server owns the sandboxes, the `aenv` CLI is a
//! thin HTTP/gRPC client, and every guest is a KVM microVM with snapshot-backed
//! boot. The two disagree on almost every primitive, so this provider is a
//! translation layer, not a rename. The mapping, and every place AgentENV
//! cannot express what [`VmProvider`] asks for, is documented per method below.
//!
//! # Mapping
//!
//! | [`VmProvider`] | AgentENV |
//! |---|---|
//! | `create` | nothing (records the spec; AgentENV has no defined-but-unstarted sandbox) |
//! | `start` | `aenv start [--cold] <image-or-snapshot> [--volume <guest>=<ro volume>]… -d`, or `aenv resume` for a paused sandbox |
//! | `start_forkable` | `start` + mark the record as a fork base (the snapshot is taken lazily by the first `fork`) |
//! | `fork` | `aenv snapshot create <golden> --name <snap>` once, then `aenv start <snap> -d` per clone |
//! | `stop` | `aenv pause` (the sandbox keeps its id, so `start` resumes it) |
//! | `delete` | `aenv delete` |
//! | `status` | `aenv ls --output json`, matched on the recorded sandbox id |
//! | `list` | the local registry (AgentENV has no notion of preloop machine names) |
//! | `exec` / `exec_stream` | `aenv exec <id> -- argv` |
//! | `exec_with_secret_env` | secret file uploaded 0600; a root-or-sudo step reads and unlinks it, then the payload runs as the exec user |
//! | `copy` | `aenv upload` / `aenv download` |
//! | `pack` | `aenv snapshot create --name`, plus a JSON descriptor at the output path |
//! | `rearm_fork_base` | always available: AgentENV snapshots are persistent and re-forkable |
//!
//! # Capability gaps (deliberate, not stubs)
//!
//! * **No host bind mounts, so read-only trees become volumes.** AgentENV has
//!   no virtiofs/9p host-directory passthrough: volumes are block devices
//!   (`aenv volume create`, attached with `--volume /guest/path=<volume>`). A
//!   read-only [`MachineSpec::volumes`] entry, such as the runner bundle or the
//!   Node externals, is archived once into a content-addressed read-only volume
//!   named `preloop-runner-<digest>`, and every sandbox that needs the tree mounts
//!   that volume at start (see [`volume`]). Nothing is copied per sandbox, and a
//!   changed tree is a new volume rather than a rebuilt golden. Writable host
//!   mounts are refused: a block volume cannot be shared live with the host.
//! * **No host Unix-socket passthrough.** [`MachineSpec::sockets`] is rejected
//!   with an explanatory [`VmError::InvalidSpec`]. The engine must run the
//!   guest control plane over TCP (`RunnerPoolConfig::control_socket = None`),
//!   which the runner already supports.
//! * **Server-assigned identity.** Sandboxes have no caller-supplied name, so
//!   [`MachineName`] → sandbox id lives in a local registry persisted next to
//!   the rest of preloop's state. It is a *restart source*, exactly like the
//!   store: the live server is authoritative, and [`AgentEnvProvider::status`]
//!   reconciles a vanished sandbox (expired TTL) back to
//!   [`MachineState::Stopped`].
//! * **Sandbox TTL.** Every sandbox has a deadline (`DEFAULT_TIMEOUT_SECS` is
//!   300 s upstream). A CI job outliving it would be killed mid-run, so each
//!   running sandbox gets a keepalive task that re-arms the TTL at a third of
//!   its length.
//! * **Exec user is the image's.** `aenv exec` has no user flag, so commands run as
//!   the image's `USER` (uid 1001 on the official runner image). The provider's own
//!   root-only steps (the secret directory, the populate step's unpack, secret-file reads
//!   and removal) run through [`crate::guest_privilege`], which uses `sudo -n` when the
//!   exec user is not root. Payloads keep the exec user, as a plain `exec` does.

pub mod gc;
pub mod volume;

use crate::guest_privilege::{run_as_root_or_sudo, run_as_root_or_sudo_strict};
use crate::{
    DEFAULT_CAPTURE_LIMIT, ExecOutput, MachineName, MachineSpec, MachineState, NetworkPolicy,
    OutputChunk, SecretSource, VmError, VmProvider, VolumeMount, read_bounded, stream_output,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::{Mutex, mpsc};
use tracing::{debug, info, warn};

/// Sandbox lifetime requested from AgentENV, in seconds.
///
/// Upstream's default is 300 s (`DEFAULT_TIMEOUT_SECS`), far below a CI job.
/// The keepalive task re-arms this window while the sandbox is running, so the
/// value only bounds how long an *abandoned* sandbox survives a crashed engine.
const DEFAULT_TTL_SECS: u32 = 3600;

/// How long to wait for a freshly started guest to answer an exec.
const READY_TIMEOUT: Duration = Duration::from_secs(120);
/// Delay between guest readiness probes.
const READY_INTERVAL: Duration = Duration::from_millis(50);

/// Guest directory holding uploaded secret env files (tmpfs-backed `/run`).
const GUEST_SECRET_DIR: &str = "/run/preloop-vm-secrets";
/// Where a populate sandbox mounts the exclusive volume it writes a runner tree into.
const POPULATE_MOUNT: &str = "/mnt/preloop-volume";
/// Guest directory a populate archive is uploaded to before it is unpacked.
const GUEST_UPLOAD_DIR: &str = "/tmp";

/// The environment variable that overrides the sandbox TTL.
const TTL_ENV: &str = "PRELOOP_AENV_TTL_SECS";
/// The environment variable that overrides the `aenv` executable.
const BINARY_ENV: &str = "PRELOOP_AENV_BINARY";

/// One preloop machine, as this provider models it on top of AgentENV.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MachineRecord {
    /// The spec [`VmProvider::create`] was called with.
    spec: MachineSpec,
    /// Server-assigned sandbox id, once the machine has been started.
    sandbox: Option<String>,
    /// Persistent snapshot name this machine's clones are started from.
    ///
    /// Created lazily by the first [`VmProvider::fork`] and reused for every
    /// later clone: AgentENV snapshots are immutable and re-forkable, so
    /// unlike SmolVM there is no single-checkpoint invariant to protect.
    fork_snapshot: Option<String>,
    /// Set on a clone: the fork base it was started from.
    golden: Option<String>,
}

/// The persisted name → sandbox mapping.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Registry {
    machines: BTreeMap<String, MachineRecord>,
}

/// A sandbox row from `aenv ls --output json`.
#[derive(Debug, Deserialize)]
struct ListedSandbox {
    #[serde(rename = "sandboxID")]
    sandbox_id: String,
    #[serde(default)]
    state: Option<String>,
}

/// A volume as `aenv volume inspect` and `aenv volume list` report it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEnvVolume {
    /// AgentENV's volume id.
    #[serde(rename = "volumeID", default)]
    pub volume_id: String,
    /// The name: for a runner volume, its content address.
    #[serde(default)]
    pub name: String,
    /// `ro` or `exclusive`, fixed at creation.
    #[serde(default)]
    pub mode: String,
    /// Size in MiB.
    #[serde(rename = "sizeMB", default)]
    pub size_mb: u64,
    /// `ready` once the volume can be mounted; `uploading` or `failed` otherwise.
    #[serde(default)]
    pub status: String,
}

/// What [`AgentEnvProvider::prune_volumes`] did, volume by volume.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PruneReport {
    /// Deleted by this call.
    pub deleted: Vec<String>,
    /// Named in `keep`, so left in place.
    pub kept: Vec<String>,
    /// Still mounted by a sandbox, so the server refused the deletion.
    pub in_use: Vec<String>,
    /// A dry run's candidates: the volumes a real run would attempt to delete.
    ///
    /// A dry run cannot say which of them a sandbox holds. AgentENV reveals a
    /// reservation only when a delete is attempted (`HTTP 409: volume is
    /// reserved by sandbox …`) and no read-only API reports it, so the report
    /// names them as candidates instead of claiming they would be deleted.
    pub candidates: Vec<String>,
    /// Refused for another reason, with the reason.
    pub failed: Vec<(String, String)>,
}

/// The read-only volume a host tree resolved to, and the fingerprint of the tree
/// it was archived from.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PublishedVolume {
    /// The tree's fingerprint at the time it was archived.
    fingerprint: String,
    /// The content-addressed volume name.
    name: String,
}

/// What one populated volume holds, as the local index records it.
///
/// The manifest is also stamped inside the volume itself, but AgentENV has no
/// API that reads inside a volume without mounting it, so `volumes list`
/// answers from here. `created` lives only here: a timestamp inside the tree
/// would change the digest of an unchanged bundle on every archive.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexedVolume {
    /// The version pins the tree was archived with.
    #[serde(default)]
    pub manifest: volume::BundleManifest,
    /// Regular files in the archived tree.
    #[serde(default)]
    pub bundle_files: u64,
    /// Summed size of those files, in bytes.
    #[serde(default)]
    pub bundle_bytes: u64,
    /// The host tree the volume was archived from.
    #[serde(default)]
    pub host: String,
    /// When the volume was populated, in Unix seconds.
    #[serde(default)]
    pub created: u64,
}

/// What this host's populated volumes hold, keyed by volume name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeIndex {
    /// Volume name → what it holds.
    #[serde(default)]
    pub volumes: BTreeMap<String, IndexedVolume>,
}

impl VolumeIndex {
    /// What `name` holds, when this host populated it.
    pub fn get(&self, name: &str) -> Option<&IndexedVolume> {
        self.volumes.get(name)
    }

    /// Whether the index knows nothing about `name`, which is what a volume
    /// populated before this index existed, or by another host, looks like.
    pub fn is_unknown(&self, name: &str) -> bool {
        !self.volumes.contains_key(name)
    }
}

/// The descriptor [`VmProvider::pack`] writes in place of a `.smolmachine`.
///
/// AgentENV keeps packed state server-side, so a pack is a *reference* to a
/// persistent snapshot rather than a file. The descriptor is what
/// [`VmProvider::create`] recognizes to boot from that snapshot again.
#[derive(Debug, Serialize, Deserialize)]
pub struct PackDescriptor {
    /// Marker identifying this file to [`AgentEnvProvider`].
    pub provider: String,
    /// Persistent snapshot name to start clones from.
    pub snapshot: String,
    /// Image the packed machine was originally built from.
    pub image: String,
}

/// Marker value for [`PackDescriptor::provider`].
const PACK_PROVIDER: &str = "agentenv";

/// The AgentENV sandbox id recorded for `machine`, read from the persisted
/// registry.
///
/// `preloop shell` and `preloop debug` run in a *different process* from the
/// engine that created the machine, and AgentENV addresses sandboxes by
/// server-assigned id, so the only way for those commands to reach a paused
/// job's guest is the registry the engine persisted. Returns `None` when there
/// is no registry, no such machine, or the machine was never started.
pub fn recorded_sandbox(machine: &str) -> Option<String> {
    let path = crate::effective_preloop_home()?.join("agentenv-machines.json");
    let registry: Registry = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
    registry.machines.get(machine)?.sandbox.clone()
}

/// The `aenv` executable this crate invokes, honouring `PRELOOP_AENV_BINARY`.
pub fn binary() -> PathBuf {
    std::env::var_os(BINARY_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("aenv"))
}

/// CLI-backed AgentENV provider.
///
/// Cloneable and cheap to clone: every mutable field is behind an [`Arc`], so a
/// pool sharing one provider across slots shares one registry and one keepalive
/// set.
#[derive(Debug, Clone)]
pub struct AgentEnvProvider {
    /// The `aenv` executable.
    binary: PathBuf,
    /// Maximum bytes retained per captured stream.
    capture_limit: usize,
    /// Requested sandbox TTL, in seconds.
    ttl_secs: u32,
    /// Name → sandbox mapping, authoritative for *naming* only.
    registry: Arc<Mutex<Registry>>,
    /// Where the registry is persisted, when persistence is configured.
    registry_path: Option<PathBuf>,
    /// TTL keepalive tasks, keyed by machine name.
    keepalives: Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
    /// One in-flight snapshot creation per fork base.
    fork_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    /// The read-only volume each archived host tree resolved to, with the
    /// fingerprint of the tree it was archived from. A start re-derives the
    /// fingerprint and re-checks that the volume still exists, so an in-place
    /// bundle update and a `prune` both invalidate the entry.
    published_volumes: Arc<Mutex<HashMap<PathBuf, PublishedVolume>>>,
    /// One populate lock per host tree, so different trees archive and populate
    /// concurrently while repeated starts of one tree wait on the first instead
    /// of archiving it twice.
    volume_locks: Arc<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>>,
    /// The version pins stamped into every volume this provider populates.
    bundle_manifest: volume::BundleManifest,
    /// What the populated volumes hold, keyed by volume name.
    volume_index: Arc<Mutex<VolumeIndex>>,
    /// Where the index is persisted, when persistence is configured.
    volume_index_path: Option<PathBuf>,
    /// Automatic collection of the volumes nothing needs any more: the knobs,
    /// the staging volumes a populate in this process is writing, and the
    /// scheduler's own bookkeeping. Nothing is deleted without it.
    volume_gc: Arc<gc::GcState>,
}

impl Default for AgentEnvProvider {
    fn default() -> Self {
        Self::from_environment()
    }
}

impl AgentEnvProvider {
    /// Construct a provider using an explicit `aenv` executable.
    pub fn new(binary: impl Into<PathBuf>) -> Self {
        Self {
            binary: binary.into(),
            capture_limit: DEFAULT_CAPTURE_LIMIT,
            ttl_secs: DEFAULT_TTL_SECS,
            registry: Arc::new(Mutex::new(Registry::default())),
            registry_path: None,
            keepalives: Arc::new(Mutex::new(HashMap::new())),
            fork_locks: Arc::new(Mutex::new(HashMap::new())),
            published_volumes: Arc::new(Mutex::new(HashMap::new())),
            volume_locks: Arc::new(Mutex::new(HashMap::new())),
            bundle_manifest: volume::BundleManifest::default(),
            volume_index: Arc::new(Mutex::new(VolumeIndex::default())),
            volume_index_path: None,
            volume_gc: Arc::new(gc::GcState::disabled()),
        }
    }

    /// Construct a provider from the process environment.
    ///
    /// `PRELOOP_AENV_BINARY` overrides the executable and
    /// `PRELOOP_AENV_TTL_SECS` the sandbox TTL; the registry is persisted under
    /// the effective preloop home so a restarted engine can still enumerate and
    /// tear down the machines it created, and the volume index beside it so
    /// `volumes list` can say what a volume holds without mounting it.
    pub fn from_environment() -> Self {
        let mut provider = Self::new(binary());
        if let Some(ttl) = std::env::var(TTL_ENV)
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|ttl| *ttl >= 60)
        {
            provider.ttl_secs = ttl;
        }
        if let Some(home) = crate::effective_preloop_home() {
            provider = provider
                .with_registry_path(home.join("agentenv-machines.json"))
                .with_volume_index_path(home.join("agentenv-runner-volumes.json"));
        }
        provider.with_volume_gc(gc::GcState::from_environment())
    }

    /// Stamp `manifest` into every volume this provider populates.
    ///
    /// The pins come from the caller because `versions.toml` is compiled into
    /// the CLI and the orchestrator, not into this crate. A provider that keeps
    /// the default records `unknown` for every version instead of guessing.
    pub fn with_bundle_manifest(mut self, manifest: volume::BundleManifest) -> Self {
        self.bundle_manifest = manifest;
        self
    }

    /// Collect the volumes nothing needs with `state`'s policy and snapshot of
    /// what is being written.
    ///
    /// A provider built with [`AgentEnvProvider::new`] collects nothing until it
    /// is given a state: [`AgentEnvProvider::from_environment`] is the
    /// constructor that turns it on from `PRELOOP_VOLUMES_GC`.
    pub fn with_volume_gc(mut self, state: gc::GcState) -> Self {
        self.volume_gc = Arc::new(state);
        self
    }

    /// Persist the volume index at `path`, loading it if it exists.
    ///
    /// A missing or malformed file is not fatal: `volumes list` then
    /// reports the manifest as unknown for every volume. Nothing else depends on
    /// the index — it exists so an operator can see what a volume holds without
    /// mounting it, since AgentENV has no API that reads inside a volume.
    pub fn with_volume_index_path(mut self, path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        match std::fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<VolumeIndex>(&bytes) {
                Ok(index) => {
                    self.volume_index = Arc::new(Mutex::new(index));
                }
                Err(error) => warn!(
                    path = %path.display(), %error,
                    "AgentENV volume index is unreadable; starting empty"
                ),
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => warn!(
                path = %path.display(), %error,
                "AgentENV volume index could not be read; starting empty"
            ),
        }
        self.volume_index_path = Some(path);
        self
    }

    /// What the volumes this host populated hold, keyed by volume name.
    ///
    /// A volume with no entry was populated by an engine that did not keep an
    /// index (or by another host): `volumes list` says so rather than
    /// claiming a version.
    pub async fn volume_index(&self) -> VolumeIndex {
        self.volume_index.lock().await.clone()
    }

    /// Persist the name → sandbox registry at `path`, loading it if it exists.
    ///
    /// A missing or malformed file is not fatal: the registry starts empty and
    /// the provider behaves as a fresh install. Losing it leaks sandboxes to
    /// their TTL rather than corrupting anything, which is why it is a restart
    /// source and not a source of truth.
    pub fn with_registry_path(mut self, path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        match std::fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<Registry>(&bytes) {
                Ok(registry) => {
                    self.registry = Arc::new(Mutex::new(registry));
                }
                Err(error) => warn!(
                    path = %path.display(), %error,
                    "AgentENV machine registry is unreadable; starting empty"
                ),
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => warn!(
                path = %path.display(), %error,
                "AgentENV machine registry could not be read; starting empty"
            ),
        }
        self.registry_path = Some(path);
        self
    }

    /// Override the maximum bytes retained from each process stream.
    pub fn with_capture_limit(mut self, bytes: usize) -> Self {
        self.capture_limit = bytes.max(1024);
        self
    }

    /// Override the requested sandbox TTL, in seconds.
    pub fn with_ttl_secs(mut self, seconds: u32) -> Self {
        self.ttl_secs = seconds.max(60);
        self
    }

    // -- process plumbing ---------------------------------------------------

    /// Run `aenv` with `args`, capturing bounded output, and fail on nonzero.
    async fn checked(
        &self,
        operation: &'static str,
        args: &[String],
    ) -> Result<ExecOutput, VmError> {
        let output = self.capture(args).await?;
        if output.exit_code != 0 {
            return Err(self.command_error(operation, &output));
        }
        Ok(output)
    }

    /// Run `aenv` with `args`, capturing bounded output, without judging it.
    async fn capture(&self, args: &[String]) -> Result<ExecOutput, VmError> {
        let launch = |source| VmError::Launch {
            program: self.binary.display().to_string(),
            source,
        };
        let mut child = Command::new(&self.binary)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(launch)?;
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let (stdout, stderr, status) = tokio::join!(
            read_bounded(stdout, self.capture_limit),
            read_bounded(stderr, self.capture_limit),
            child.wait(),
        );
        let (stdout, stdout_truncated) = stdout.map_err(launch)?;
        let (stderr, stderr_truncated) = stderr.map_err(launch)?;
        let status = status.map_err(launch)?;
        Ok(ExecOutput {
            exit_code: status.code().unwrap_or(-1),
            stdout,
            stderr,
            truncated: stdout_truncated || stderr_truncated,
        })
    }

    /// The error for a failed `aenv` invocation.
    ///
    /// `aenv` reports API failures on stderr but a few paths (build waits) put
    /// the reason on stdout, so both are considered before settling for the
    /// exit code alone.
    fn command_error(&self, operation: &'static str, output: &ExecOutput) -> VmError {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        let message = if stderr.is_empty() {
            String::from_utf8_lossy(&output.stdout).trim().to_owned()
        } else {
            stderr
        };
        VmError::Command {
            operation,
            exit_code: output.exit_code,
            message,
        }
    }

    // -- registry -----------------------------------------------------------

    /// Persist the registry, atomically, best-effort.
    ///
    /// Called with the registry lock held so the file can never interleave two
    /// writers. A failure is logged and ignored: the in-memory map still drives
    /// this process, and refusing an otherwise successful VM operation because
    /// a bookkeeping file could not be written would be strictly worse.
    fn persist(&self, registry: &Registry) {
        let Some(path) = &self.registry_path else {
            return;
        };
        let Some(parent) = path.parent() else {
            return;
        };
        let write = || -> std::io::Result<()> {
            std::fs::create_dir_all(parent)?;
            let temporary = parent.join(format!(".agentenv-machines.{}.tmp", std::process::id()));
            let bytes = serde_json::to_vec_pretty(registry)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            std::fs::write(&temporary, &bytes)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
            }
            std::fs::rename(&temporary, path)
        };
        if let Err(error) = write() {
            warn!(path = %path.display(), %error, "AgentENV machine registry write failed");
        }
    }

    /// The record for `name`, or [`VmError::Command`]-shaped "not found".
    ///
    /// The message deliberately contains `not found`: the orchestrator and
    /// this provider's own delete path both treat that substring as "already
    /// gone", which is what an unknown machine is.
    async fn record(&self, name: &MachineName) -> Result<MachineRecord, VmError> {
        self.registry
            .lock()
            .await
            .machines
            .get(name.as_str())
            .cloned()
            .ok_or_else(|| VmError::Command {
                operation: "lookup",
                exit_code: 1,
                message: format!("machine `{}` not found", name.as_str()),
            })
    }

    /// Apply `mutate` to the record for `name` and persist the result.
    async fn update(
        &self,
        name: &MachineName,
        mutate: impl FnOnce(&mut MachineRecord),
    ) -> Result<(), VmError> {
        let mut registry = self.registry.lock().await;
        let record = registry
            .machines
            .get_mut(name.as_str())
            .ok_or_else(|| VmError::Command {
                operation: "lookup",
                exit_code: 1,
                message: format!("machine `{}` not found", name.as_str()),
            })?;
        mutate(record);
        self.persist(&registry);
        Ok(())
    }

    /// The sandbox id backing `name`, or an error naming the missing start.
    async fn sandbox(&self, name: &MachineName) -> Result<String, VmError> {
        self.record(name)
            .await?
            .sandbox
            .ok_or_else(|| VmError::Command {
                operation: "sandbox",
                exit_code: 1,
                message: format!("machine `{}` has no running sandbox", name.as_str()),
            })
    }

    /// The mutex guarding snapshot creation for one fork base.
    async fn fork_lock(&self, golden: &MachineName) -> Arc<Mutex<()>> {
        let mut locks = self.fork_locks.lock().await;
        Arc::clone(
            locks
                .entry(golden.as_str().to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    }

    // -- sandbox lifecycle --------------------------------------------------

    /// Live sandbox states, keyed by sandbox id.
    async fn live_sandboxes(&self) -> Result<HashMap<String, String>, VmError> {
        let output = self
            .checked("list", &["ls".into(), "--output".into(), "json".into()])
            .await?;
        let rows: Vec<ListedSandbox> = serde_json::from_slice(&output.stdout)
            .map_err(|error| VmError::Protocol(format!("aenv ls: {error}")))?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let state = row.state.unwrap_or_default().to_ascii_lowercase();
                (row.sandbox_id, state)
            })
            .collect())
    }

    /// Start a sandbox from `target`, returning its server-assigned id.
    ///
    /// `cold` selects `--cold`, which is the only path that honours the spec's
    /// CPU, memory, and disk: template and snapshot starts inherit the
    /// resources baked into the artifact they boot from.
    ///
    /// `mounts` are the `<guest>=<volume>` pairs of read-only volumes to attach.
    /// They apply to a fresh start only: a resumed sandbox keeps the mounts it was
    /// created with.
    ///
    /// The requested disk size is a *floor*, not a contract. AgentENV refuses
    /// to shrink a guest below its overlaybd base virtual size (Ubuntu 24.04
    /// resolves to 64 GiB), and preloop's specs are written against SmolVM,
    /// whose `--storage` is a sparse ceiling. Rather than under-provision or
    /// hard-fail on a spec that is merely smaller than the base, the request is
    /// retried once without the flag, which inherits the (larger) base size.
    async fn start_sandbox(
        &self,
        target: &str,
        cold: Option<&MachineSpec>,
        mounts: &[String],
        operation: &'static str,
    ) -> Result<String, VmError> {
        let build = |with_disk: bool| {
            let mut args: Vec<String> = vec!["start".into()];
            if let Some(spec) = cold {
                args.push("--cold".into());
                args.push(target.to_owned());
                args.extend(["--cpu".into(), spec.cpus.to_string()]);
                args.extend(["--memory".into(), spec.memory_mib.to_string()]);
                if with_disk {
                    // AgentENV requires whole-GiB steps. Zero is rejected by
                    // `validate_spec` at create time, so no substitution here.
                    args.extend([
                        "--disk-size-mb".into(),
                        (spec.storage_gib * 1024).to_string(),
                    ]);
                }
            } else {
                args.push(target.to_owned());
            }
            for mount in mounts {
                args.extend(["--volume".into(), mount.clone()]);
            }
            args.extend(["--timeout".into(), self.ttl_secs.to_string()]);
            // Detached: the attaching form of `aenv start` would take over
            // this process's stdio and only return when the shell exits.
            args.push("-d".into());
            args
        };
        let output = match self.checked(operation, &build(true)).await {
            Ok(output) => output,
            Err(VmError::Command { message, .. }) if is_disk_too_small(&message) => {
                debug!(
                    target,
                    message, "AgentENV base image is larger than the requested disk; inheriting it"
                );
                self.checked(operation, &build(false)).await?
            }
            Err(error) => return Err(error),
        };
        let sandbox = String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(str::trim)
            .rfind(|line| !line.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| VmError::Protocol("aenv start printed no sandbox id".to_owned()))?;
        if sandbox.contains(char::is_whitespace) {
            return Err(VmError::Protocol(format!(
                "aenv start printed an unexpected sandbox id: {sandbox}"
            )));
        }
        Ok(sandbox)
    }

    /// Run [`Self::start_sandbox`] to completion even if the caller is
    /// cancelled, and make the server-assigned sandbox id survive it.
    ///
    /// The pool cancels a slot's provisioning (and a golden's preparation)
    /// when a shutdown arrives, and dropping this future with it would kill
    /// the local `aenv` client before it can read the id — the server-side
    /// sandbox would then run on, recorded nowhere, until its TTL. The
    /// command therefore runs in a detached task that outlives the caller:
    /// on success it hands the id back when the caller is still there, and
    /// [`Self::finish_start`] records it when it is not.
    ///
    /// The read-only volumes are resolved inside the same task: resolving one can
    /// populate it, which takes minutes and must not be abandoned half way.
    async fn start_sandbox_supervised(
        &self,
        name: &MachineName,
        target: &str,
        cold: Option<&MachineSpec>,
        volumes: &[VolumeMount],
        operation: &'static str,
    ) -> Result<String, VmError> {
        let provider = self.clone();
        let machine = name.clone();
        let target = target.to_owned();
        let cold = cold.cloned();
        let volumes = volumes.to_vec();
        let handle = tokio::spawn(async move {
            let result = match provider
                .read_only_mounts(&volumes, &target, cold.as_ref())
                .await
            {
                Ok(mounts) => {
                    provider
                        .start_sandbox(&target, cold.as_ref(), &mounts, operation)
                        .await
                }
                Err(error) => Err(error),
            };
            if let Ok(sandbox) = &result {
                provider.finish_start(&machine, sandbox).await;
            }
            result
        });
        match handle.await {
            Ok(result) => result,
            Err(error) => Err(VmError::Protocol(format!(
                "AgentENV {operation} task failed: {error}"
            ))),
        }
    }

    /// Keep the sandbox a completed start created deletable.
    ///
    /// The caller normally records the id itself; when it was cancelled
    /// mid-start it never will, so the record is written here. A machine
    /// whose record is already gone was torn down while the start was in
    /// flight — nothing will ever delete the sandbox it created, so delete
    /// it outright. The record is persisted, so even an engine that exits
    /// before its teardown reaches this machine leaves the next engine the
    /// id it needs.
    async fn finish_start(&self, name: &MachineName, sandbox: &str) {
        if self
            .update(name, |record| record.sandbox = Some(sandbox.to_owned()))
            .await
            .is_ok()
        {
            return;
        }
        warn!(
            machine = name.as_str(),
            sandbox, "machine was torn down while its start ran; deleting the unrecorded sandbox"
        );
        if let Err(error) = self
            .checked("delete", &["delete".into(), sandbox.to_owned()])
            .await
            && !matches!(&error, VmError::Command { message, .. } if is_absent(message))
        {
            warn!(
                machine = name.as_str(),
                sandbox,
                %error,
                "failed to delete the stranded AgentENV sandbox; it will expire with its TTL"
            );
        }
    }

    /// Block until the guest agent answers an exec, or fail with the reason.
    ///
    /// A freshly created sandbox is reachable before `envd` is ready; upstream's
    /// attaching `aenv start` polls for the same condition before handing over a
    /// shell. Probing with a real exec (rather than sleeping) is what makes a
    /// snapshot restore return in milliseconds instead of a fixed delay.
    async fn await_guest(&self, sandbox: &str) -> Result<(), VmError> {
        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        let args = vec![
            "exec".into(),
            sandbox.to_owned(),
            "--".into(),
            "/bin/true".into(),
        ];
        let mut last: Option<ExecOutput> = None;
        while tokio::time::Instant::now() < deadline {
            let output = self.capture(&args).await?;
            if output.exit_code == 0 {
                return Ok(());
            }
            last = Some(output);
            tokio::time::sleep(READY_INTERVAL).await;
        }
        Err(match last {
            Some(output) => self.command_error("await_guest", &output),
            None => VmError::Command {
                operation: "await_guest",
                exit_code: -1,
                message: format!("sandbox {sandbox} never became reachable"),
            },
        })
    }

    /// Resume `sandbox` if AgentENV has it paused, and report whether it did.
    ///
    /// AgentENV snapshots only running sandboxes, but `stop` pauses rather than
    /// stopping, and the orchestrator packs a golden right after stopping it. A
    /// paused sandbox is therefore resumed before it is snapshotted, so the
    /// snapshot captures the machine the same way it would have while running.
    async fn resume_if_paused(&self, sandbox: &str) -> Result<bool, VmError> {
        let states = self.live_sandboxes().await?;
        let paused = states
            .get(sandbox)
            .is_some_and(|state| state.as_str() != "running");
        if !paused {
            return Ok(false);
        }
        self.checked(
            "resume",
            &[
                "resume".into(),
                sandbox.to_owned(),
                "--timeout".into(),
                self.ttl_secs.to_string(),
            ],
        )
        .await?;
        Ok(true)
    }

    /// Take a snapshot of `sandbox` with `args`. AgentENV takes snapshots only
    /// of running sandboxes, so a paused one is resumed first.
    ///
    /// A sandbox resumed for the snapshot is paused again afterwards, on success
    /// and on error alike: the orchestrator recorded it as stopped, and a
    /// running sandbox with no keepalive would be deleted by its TTL. A failed
    /// pause is logged and does not replace the snapshot's own outcome.
    async fn snapshot_running(
        &self,
        sandbox: &str,
        operation: &'static str,
        args: &[String],
    ) -> Result<(), VmError> {
        let resumed = self.resume_if_paused(sandbox).await?;
        let outcome: Result<(), VmError> = async {
            if resumed {
                self.await_guest(sandbox).await?;
            }
            self.checked(operation, args).await.map(|_| ())
        }
        .await;
        if resumed
            && let Err(error) = self
                .checked("stop", &["pause".into(), sandbox.to_owned()])
                .await
        {
            warn!(
                sandbox,
                %error,
                "could not pause the sandbox again after its snapshot; it stays running until its TTL expires"
            );
        }
        outcome
    }

    /// Re-arm the sandbox TTL for as long as the machine is running.
    ///
    /// Without this a job outliving the TTL would have its VM deleted
    /// underneath it. The task is aborted by `stop`, `delete`, and by a
    /// replacement start for the same name, so it can never outlive its
    /// sandbox.
    async fn spawn_keepalive(&self, name: &MachineName, sandbox: &str) {
        let interval = Duration::from_secs(u64::from(self.ttl_secs) / 3 + 1);
        let binary = self.binary.clone();
        let sandbox = sandbox.to_owned();
        let ttl = self.ttl_secs.to_string();
        let handle = tokio::spawn(async move {
            // Consecutive renewal failures. A missing sandbox fails every
            // probe, so it still exits quickly; a transient daemon hiccup
            // gets two more chances before the sandbox is left to its TTL.
            let mut consecutive_failures = 0u32;
            loop {
                tokio::time::sleep(interval).await;
                let result = Command::new(&binary)
                    .args(["timeout", &sandbox, &ttl])
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .kill_on_drop(true)
                    .status()
                    .await;
                match &result {
                    Ok(status) if status.success() => {
                        consecutive_failures = 0;
                    }
                    outcome => {
                        consecutive_failures += 1;
                        if consecutive_failures >= 3 {
                            warn!(
                                sandbox,
                                failures = consecutive_failures,
                                ?outcome,
                                "AgentENV TTL keepalive giving up; sandbox left to its TTL"
                            );
                            break;
                        }
                    }
                }
            }
        });
        if let Some(previous) = self
            .keepalives
            .lock()
            .await
            .insert(name.as_str().to_owned(), handle)
        {
            previous.abort();
        }
    }

    /// Stop the keepalive task for `name`, if any.
    async fn cancel_keepalive(&self, name: &MachineName) {
        if let Some(handle) = self.keepalives.lock().await.remove(name.as_str()) {
            handle.abort();
        }
    }

    // -- read-only volumes --------------------------------------------------

    /// Resolve the read-only `volumes` to the `<guest>=<volume>` pairs that
    /// `aenv start --volume` takes.
    ///
    /// [`validate_spec`] refuses writable mounts at create time. A writable entry
    /// reaching this point is refused again rather than copied, because a copy
    /// would silently diverge from the host.
    async fn read_only_mounts(
        &self,
        volumes: &[VolumeMount],
        target: &str,
        cold: Option<&MachineSpec>,
    ) -> Result<Vec<String>, VmError> {
        let mut mounts = Vec::with_capacity(volumes.len());
        for volume in volumes {
            if !volume.read_only {
                return Err(VmError::InvalidSpec(format!(
                    "AgentENV cannot mount the writable host directory {}",
                    volume.host.display()
                )));
            }
            let name = self
                .ensure_read_only_volume(&volume.host, target, cold)
                .await?;
            mounts.push(format!("{}={name}", volume.guest.display()));
        }
        Ok(mounts)
    }

    /// The read-only volume holding `host`'s tree, created on first use.
    ///
    /// The volume name derives from the digest of the archived tree (see
    /// [`volume`]), so a changed tree resolves to a new volume and an unchanged
    /// one to the volume already published. A start re-derives the tree's
    /// fingerprint and re-checks the volume before reusing it, so an in-place
    /// bundle update and a `prune` that removed the volume are both noticed:
    /// the tree is archived again and, when the volume is gone, populated again.
    ///
    /// The lock is per tree: two starts of the same tree wait for one archive and
    /// one populate, while starts of different trees proceed in parallel.
    async fn ensure_read_only_volume(
        &self,
        host: &Path,
        target: &str,
        cold: Option<&MachineSpec>,
    ) -> Result<String, VmError> {
        // An engine that needs a volume is an engine that can collect the ones
        // nothing needs, on a timer from here on.
        gc::ensure_scheduled(self);
        let lock = self.volume_lock(host).await;
        let _populate = lock.lock().await;
        let fingerprint = volume::fingerprint_tree(host, &self.bundle_manifest).await?;
        if let Some(name) = self.published_volume(host, &fingerprint).await? {
            return Ok(name);
        }
        let tree = volume::archive_tree(host, &self.bundle_manifest).await?;
        let name = volume::volume_name(&tree.digest);
        if !self.volume_is_ready(&name).await? {
            let size_mb = volume::volume_size_mb(tree.bytes)?;
            self.populate_volume(host, &name, &tree, size_mb, target, cold)
                .await?;
            info!(
                volume = %name,
                bytes = tree.bytes,
                files = tree.files,
                host = %host.display(),
                "populated read-only runner volume"
            );
            // Publishing a volume is what supersedes one, so this is where a
            // pass is worth asking for. It runs in the background.
            gc::request_pass(self);
        }
        self.published_volumes.lock().await.insert(
            host.to_path_buf(),
            PublishedVolume {
                fingerprint,
                name: name.clone(),
            },
        );
        Ok(name)
    }

    /// The volume the memo holds for `host`, when it still matches `fingerprint`
    /// and the server still has the volume.
    async fn published_volume(
        &self,
        host: &Path,
        fingerprint: &str,
    ) -> Result<Option<String>, VmError> {
        let remembered = {
            let published = self.published_volumes.lock().await;
            match published.get(host) {
                Some(entry) if entry.fingerprint == fingerprint => Some(entry.name.clone()),
                _ => None,
            }
        };
        let Some(name) = remembered else {
            return Ok(None);
        };
        // A remembered name is only reusable while the volume exists: `prune`
        // can delete an unmounted volume while this engine keeps running.
        if self.volume_is_ready(&name).await? {
            Ok(Some(name))
        } else {
            Ok(None)
        }
    }

    /// The populate lock for `host`: one per tree, so different trees archive and
    /// populate concurrently instead of waiting on each other.
    async fn volume_lock(&self, host: &Path) -> Arc<Mutex<()>> {
        let mut locks = self.volume_locks.lock().await;
        Arc::clone(locks.entry(host.to_path_buf()).or_default())
    }

    /// Record a freshly populated volume in the local index.
    ///
    /// Best-effort: the volume is already published and the engine can start
    /// runners from it, so a failed write is logged and ignored. The index is
    /// what lets `volumes list` say what a volume holds.
    async fn remember_volume(&self, host: &Path, tree: &volume::ArchivedTree) {
        let entry = IndexedVolume {
            manifest: self.bundle_manifest.clone(),
            bundle_files: tree.files,
            bundle_bytes: tree.bytes,
            host: host.display().to_string(),
            created: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|since| since.as_secs())
                .unwrap_or_default(),
        };
        let mut index = self.volume_index.lock().await;
        index
            .volumes
            .insert(volume::volume_name(&tree.digest), entry);
        self.persist_volume_index(&index);
    }

    /// Drop volumes `names` from the index, after `prune` deleted them.
    ///
    /// Best-effort, like every other index write. Names are content-addressed,
    /// so a volume re-populated from the same tree records the same entry again.
    async fn forget_volumes(&self, names: &[String]) {
        if names.is_empty() {
            return;
        }
        let mut index = self.volume_index.lock().await;
        let before = index.volumes.len();
        index.volumes.retain(|name, _| !names.contains(name));
        if index.volumes.len() != before {
            self.persist_volume_index(&index);
        }
    }

    /// Persist the volume index, atomically, best-effort.
    fn persist_volume_index(&self, index: &VolumeIndex) {
        let Some(path) = &self.volume_index_path else {
            return;
        };
        let Some(parent) = path.parent() else {
            return;
        };
        let write = || -> std::io::Result<()> {
            std::fs::create_dir_all(parent)?;
            let temporary = parent.join(format!(
                ".agentenv-runner-volumes.{}.tmp",
                std::process::id()
            ));
            let bytes = serde_json::to_vec_pretty(index)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            std::fs::write(&temporary, &bytes)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
            }
            std::fs::rename(&temporary, path)
        };
        if let Err(error) = write() {
            warn!(path = %path.display(), %error, "AgentENV volume index write failed");
        }
    }

    /// Write `tree` into a new read-only volume named `name`.
    ///
    /// Population goes through an exclusive staging volume, so the final name only
    /// ever refers to a complete tree: a throwaway sandbox writes the archive into
    /// the staging volume, deleting that sandbox commits the staging volume, and a
    /// read-only fork of the committed volume takes the final name. Any failure
    /// deletes the staging volume, and nothing falls back to copying the tree into
    /// a sandbox.
    async fn populate_volume(
        &self,
        host: &Path,
        name: &str,
        tree: &volume::ArchivedTree,
        size_mb: u64,
        target: &str,
        cold: Option<&MachineSpec>,
    ) -> Result<(), VmError> {
        let attempt = &uuid::Uuid::new_v4().simple().to_string()[..8];
        let staging = volume::staging_name(&tree.digest, attempt);
        // Record the staging volume before it exists: a pass that saw the volume
        // and not the record would treat a populate in flight as a leftover.
        let _staging = self.volume_gc.begin_staging(&staging, &tree.digest, host);
        let size = size_mb.to_string();
        self.checked(
            "runner_volume",
            &[
                "volume".into(),
                "create".into(),
                staging.clone(),
                "--mode".into(),
                "exclusive".into(),
                "--size-mb".into(),
                size.clone(),
            ],
        )
        .await?;
        if let Err(error) = self.write_staging(&staging, tree, target, cold).await {
            self.discard_volume(&staging).await;
            return Err(error);
        }
        let frozen = self
            .checked(
                "runner_volume",
                &[
                    "volume".into(),
                    "create".into(),
                    name.to_owned(),
                    "--mode".into(),
                    "ro".into(),
                    "--from-volume".into(),
                    staging.clone(),
                    "--size-mb".into(),
                    size,
                ],
            )
            .await;
        match frozen {
            Ok(_) => {}
            // Another engine froze this digest first. Its volume holds the same
            // archive, so it is kept and the staging copy is discarded.
            Err(VmError::Command { message, .. }) if is_already_exists(&message) => {}
            Err(error) => {
                self.discard_volume(&staging).await;
                return Err(error);
            }
        }
        self.discard_volume(&staging).await;
        // Record the final volume before asking whether it can be mounted: a
        // volume the server reports as `failed` or `uploading` still carries the
        // creation time collection needs, or it would look ageless and be kept
        // forever — and every later start would fail on it, since a volume this
        // engine remembers is refused unless it is ready.
        self.remember_volume(host, tree).await;
        if self.volume_is_ready(name).await? {
            Ok(())
        } else {
            Err(VmError::Command {
                operation: "runner_volume",
                exit_code: 1,
                message: format!("read-only volume {name} is not ready after populate"),
            })
        }
    }

    /// Populate the exclusive `staging` volume with `tree`, then commit it.
    async fn write_staging(
        &self,
        staging: &str,
        tree: &volume::ArchivedTree,
        target: &str,
        cold: Option<&MachineSpec>,
    ) -> Result<(), VmError> {
        let mount = format!("{POPULATE_MOUNT}={staging}");
        let sandbox = self
            .start_sandbox(target, cold, &[mount], "populate")
            .await?;
        let unpacked = match self.await_guest(&sandbox).await {
            Ok(()) => self.unpack_archive(&sandbox, tree).await,
            Err(error) => Err(error),
        };
        // Deleting the sandbox is what commits its writes to the volume, so it runs
        // even after a failed unpack, and its own failure fails the populate.
        let deleted = self
            .checked("delete", &["delete".into(), sandbox.clone()])
            .await
            .map(|_| ());
        unpacked.and(deleted)
    }

    /// Upload `tree`'s archive into `sandbox` and unpack it into the populate mount.
    ///
    /// The archive is checked against its digest first, so a corrupted upload
    /// fails here instead of freezing a bad volume. Unpacking runs as root, since
    /// the mount is root-owned, through the root-or-sudo wrapper.
    async fn unpack_archive(
        &self,
        sandbox: &str,
        tree: &volume::ArchivedTree,
    ) -> Result<(), VmError> {
        let guest_archive = format!(
            "{GUEST_UPLOAD_DIR}/preloop-volume-{}.tar",
            uuid::Uuid::new_v4().simple()
        );
        self.checked(
            "upload",
            &[
                "upload".into(),
                sandbox.to_owned(),
                tree.archive.path().display().to_string(),
                guest_archive.clone(),
            ],
        )
        .await?;
        let script = format!(
            "set -eu; printf '%s  %s\\n' {digest} {archive} | sha256sum -c - >/dev/null; \
             mkdir -p {mount}; tar --no-same-owner -xf {archive} -C {mount}; sync; \
             rm -f {archive}",
            digest = shell_quote(&tree.digest),
            archive = shell_quote(&guest_archive),
            mount = shell_quote(POPULATE_MOUNT),
        );
        let output = self.capture(&guest_root_argv(sandbox, &script)).await?;
        if output.exit_code != 0 {
            return Err(guest_privilege_error(
                self.command_error("populate_volume", &output),
            ));
        }
        Ok(())
    }

    /// Whether the volume `name` exists and is mountable.
    ///
    /// A volume in any other state is an error, not a reason to populate it
    /// again: its owner may still be writing it.
    async fn volume_is_ready(&self, name: &str) -> Result<bool, VmError> {
        let output = self
            .capture(&["volume".into(), "inspect".into(), name.to_owned()])
            .await?;
        if output.exit_code != 0 {
            return match self.command_error("runner_volume", &output) {
                VmError::Command { message, .. } if is_absent(&message) => Ok(false),
                error => Err(error),
            };
        }
        let volume: AgentEnvVolume = serde_json::from_slice(&output.stdout)
            .map_err(|error| VmError::Protocol(format!("aenv volume inspect: {error}")))?;
        match volume.status.as_str() {
            "ready" => Ok(true),
            "" => Err(VmError::Protocol(format!(
                "aenv volume inspect {name} reported no status"
            ))),
            status => Err(VmError::Command {
                operation: "runner_volume",
                exit_code: 1,
                message: format!(
                    "read-only volume {name} is `{status}` and cannot be mounted; \
                     delete it with `aenv volume delete {name}` and start again"
                ),
            }),
        }
    }

    /// The read-only volume name that `host`'s tree maps to.
    ///
    /// Reads the tree but creates nothing, so an operator can name the volumes
    /// `prune` must keep.
    pub async fn runner_volume_name(&self, host: &Path) -> Result<String, VmError> {
        let tree = volume::archive_tree(host, &self.bundle_manifest).await?;
        Ok(volume::volume_name(&tree.digest))
    }

    /// Every runner volume AgentENV lists: published volumes and populate
    /// leftovers alike.
    pub async fn list_volumes(&self) -> Result<Vec<AgentEnvVolume>, VmError> {
        let output = self
            .checked("volume_list", &["volume".into(), "list".into()])
            .await?;
        let volumes: Vec<AgentEnvVolume> = serde_json::from_slice(&output.stdout)
            .map_err(|error| VmError::Protocol(format!("aenv volume list: {error}")))?;
        Ok(volumes
            .into_iter()
            .filter(|volume| volume::is_runner_volume_name(&volume.name))
            .collect())
    }

    /// Delete exactly `names` and classify each outcome.
    ///
    /// AgentENV refuses a volume a sandbox mounts (`HTTP 409: volume is reserved
    /// by sandbox …`), which is an outcome rather than a failure, and a volume
    /// that is already gone counts as deleted. Index entries for what was
    /// deleted are dropped.
    ///
    /// Deleting a set decided by the caller is what an automatic collection pass
    /// needs: it plans from one listing and then deletes exactly the volumes it
    /// classified, so a volume another engine publishes in between is never
    /// deleted unclassified.
    pub async fn delete_volumes(&self, names: &[String]) -> Result<PruneReport, VmError> {
        let mut report = PruneReport::default();
        for name in names {
            let deletion = self
                .checked(
                    "volume_delete",
                    &["volume".into(), "delete".into(), name.clone()],
                )
                .await;
            match deletion {
                Ok(_) => report.deleted.push(name.clone()),
                Err(VmError::Command { message, .. }) if is_absent(&message) => {
                    report.deleted.push(name.clone());
                }
                Err(VmError::Command { message, .. }) if is_mounted(&message) => {
                    report.in_use.push(name.clone());
                }
                Err(error) => report.failed.push((name.clone(), error.to_string())),
            }
        }
        self.forget_volumes(&report.deleted).await;
        Ok(report)
    }

    /// Delete every runner volume that is not in `keep` and that no sandbox
    /// mounts. AgentENV refuses to delete a mounted volume, which is reported as
    /// in use rather than as a failure. `dry_run` deletes nothing and reports
    /// candidates instead: the server only reveals a reservation when a delete
    /// is attempted, so a dry run does not claim those volumes would be deleted.
    ///
    /// Safe while an engine is running: a provider revalidates a remembered
    /// volume before reusing it and repopulates a volume that is gone. It is not
    /// safe while a volume is being populated — a volume deleted between its
    /// populate's create and its first mount fails that populate.
    pub async fn prune_volumes(
        &self,
        keep: &[String],
        dry_run: bool,
    ) -> Result<PruneReport, VmError> {
        let mut report = PruneReport::default();
        let mut doomed = Vec::new();
        for volume in self.list_volumes().await? {
            if keep.contains(&volume.name) {
                report.kept.push(volume.name);
            } else if dry_run {
                report.candidates.push(volume.name);
            } else {
                doomed.push(volume.name);
            }
        }
        if dry_run {
            return Ok(report);
        }
        let deleted = self.delete_volumes(&doomed).await?;
        report.deleted = deleted.deleted;
        report.in_use = deleted.in_use;
        report.failed = deleted.failed;
        Ok(report)
    }

    /// Delete a volume this provider created and no longer needs.
    ///
    /// Best-effort: the populate has already succeeded or failed, and a leftover
    /// staging volume is reclaimed by `preloop runner-volumes prune`.
    async fn discard_volume(&self, name: &str) {
        if let Err(error) = self
            .checked(
                "runner_volume",
                &["volume".into(), "delete".into(), name.to_owned()],
            )
            .await
        {
            warn!(
                volume = name,
                %error,
                "could not delete a runner volume; `preloop runner-volumes prune` will reclaim it"
            );
        }
    }

    /// Create the guest directory that secret env files are uploaded into.
    ///
    /// `/run` is root-owned, so this runs through the root-or-sudo wrapper.
    async fn prepare_guest_dirs(&self, sandbox: &str) -> Result<(), VmError> {
        let script = format!("mkdir -p {GUEST_SECRET_DIR} && chmod 700 {GUEST_SECRET_DIR}");
        self.checked("prepare_guest_dirs", &guest_root_argv(sandbox, &script))
            .await
            .map_err(guest_privilege_error)
            .map(|_| ())
    }
}

/// Quote `value` for POSIX `sh`.
fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// `aenv exec` argv for a provider-internal step that needs root.
///
/// The script goes through [`run_as_root_or_sudo_strict`]: a root exec runs it
/// directly, and the image user runs it under `sudo -n`.
fn guest_root_argv(sandbox: &str, script: &str) -> Vec<String> {
    vec![
        "exec".into(),
        sandbox.to_owned(),
        "--".into(),
        "/bin/sh".into(),
        "-c".into(),
        run_as_root_or_sudo_strict(script),
    ]
}

/// Name the prerequisite when a root step fails because sudo refused.
///
/// Without this, a missing passwordless sudo surfaces as a bare sudo message
/// from deep inside a provisioning step.
fn guest_privilege_error(error: VmError) -> VmError {
    match error {
        VmError::Command {
            operation,
            exit_code,
            message,
        } if message.contains("sudo") => VmError::Command {
            operation,
            exit_code,
            message: format!(
                "{message} (provider steps that need root run through `sudo -n`: \
                 the guest's exec user must have passwordless sudo)"
            ),
        },
        other => other,
    }
}

/// The pack descriptor at `path`, when the path holds one.
fn read_pack_descriptor(path: &Path) -> Option<PackDescriptor> {
    let bytes = std::fs::read(path).ok()?;
    let descriptor: PackDescriptor = serde_json::from_slice(&bytes).ok()?;
    (descriptor.provider == PACK_PROVIDER).then_some(descriptor)
}

/// What a machine should be started from.
enum StartTarget {
    /// A persistent snapshot or template name; resources come from it.
    Artifact(String),
    /// An OCI image reference started cold with the spec's resources.
    Image(String),
}

/// Resolve a [`MachineSpec::image`] to a start target.
///
/// Three forms are accepted, in order: a pack descriptor written by
/// [`VmProvider::pack`], the explicit `agentenv-snapshot:<name>` form, and
/// anything else, which is treated as an OCI image reference.
fn start_target(image: &str) -> StartTarget {
    if let Some(snapshot) = image.strip_prefix("agentenv-snapshot:") {
        return StartTarget::Artifact(snapshot.to_owned());
    }
    let path = Path::new(image);
    if path.is_file()
        && let Some(descriptor) = read_pack_descriptor(path)
    {
        return StartTarget::Artifact(descriptor.snapshot);
    }
    StartTarget::Image(image.to_owned())
}

/// Validate a spec against what AgentENV can actually express.
fn validate_spec(spec: &MachineSpec) -> Result<(), VmError> {
    if spec.image.trim().is_empty() || spec.cpus == 0 || spec.memory_mib < 128 {
        return Err(VmError::InvalidSpec(
            "image, CPU, and memory must be non-zero".into(),
        ));
    }
    if !spec.sockets.is_empty() {
        return Err(VmError::InvalidSpec(
            "AgentENV cannot forward host Unix sockets into a guest: run the guest control \
             plane over TCP (leave `control_socket` unset) when using the agentenv backend"
                .into(),
        ));
    }
    for (index, mount) in spec.volumes.iter().enumerate() {
        if !mount.host.is_absolute() || !mount.guest.is_absolute() {
            return Err(VmError::InvalidSpec("volume paths must be absolute".into()));
        }
        if mount.guest.as_os_str() == "/" {
            return Err(VmError::InvalidSpec(
                "AgentENV cannot mount a volume over the guest root".into(),
            ));
        }
        if !mount.read_only {
            return Err(VmError::InvalidSpec(format!(
                "AgentENV cannot share the writable host directory {} with a guest: a volume \
                 is a block device that cannot follow host writes, so only read-only trees \
                 are supported",
                mount.host.display()
            )));
        }
        if !mount.host.is_dir() {
            return Err(VmError::InvalidSpec(format!(
                "volume source is not a directory: {}",
                mount.host.display()
            )));
        }
        for other in &spec.volumes[..index] {
            if mount.guest.starts_with(&other.guest) || other.guest.starts_with(&mount.guest) {
                return Err(VmError::InvalidSpec(format!(
                    "volume mounts overlap: {} and {}",
                    other.guest.display(),
                    mount.guest.display()
                )));
            }
        }
    }
    if matches!(
        spec.network,
        NetworkPolicy::Disabled | NetworkPolicy::Restricted { .. }
    ) {
        return Err(VmError::InvalidSpec(
            "AgentENV sandboxes are always networked with the node deny-list; \
             NetworkPolicy::Disabled and NetworkPolicy::Restricted cannot be honoured"
                .into(),
        ));
    }
    if spec.dns.is_some() {
        return Err(VmError::InvalidSpec(
            "AgentENV exposes no per-sandbox resolver setting; `dns` cannot be honoured".into(),
        ));
    }
    if spec.storage_gib == 0 {
        return Err(VmError::InvalidSpec(
            "image, CPU, memory, and storage must be non-zero".into(),
        ));
    }
    Ok(())
}

#[async_trait]
impl VmProvider for AgentEnvProvider {
    /// AgentENV has block volumes rather than host passthrough, keeps packs
    /// server-side as snapshots, and — unlike SmolVM — snapshots the golden's
    /// live filesystem, so clones start already baked.
    fn capabilities(&self) -> crate::ProviderCapabilities {
        crate::ProviderCapabilities {
            live_host_volumes: false,
            socket_mounts: false,
            file_packs: false,
            fork_bases_may_mount_read_only_volumes: false,
            runner_bundle_carries_externals: true,
            fork_inherits_guest_writes: true,
            preserves_runtime_state_on_suspend: true,
        }
    }

    /// Start the periodic collection pass with the engine, not with the first
    /// volume it needs.
    ///
    /// Collection is otherwise armed where a volume is resolved (a cold start or
    /// a bundle change), so an engine whose runners are all resumed — or which
    /// sits idle with nothing queued — would never run a periodic pass and the
    /// volumes nothing needs would stay on the server indefinitely.
    fn start_maintenance(&self) {
        gc::ensure_scheduled(self);
    }

    /// Record the machine. AgentENV has no unstarted sandbox to create.
    ///
    /// Deliberately cheap: booting here and immediately pausing would double
    /// the cost of the pool's create-then-start sequence, and preloop treats a
    /// created-but-unstarted machine as [`MachineState::Stopped`] anyway.
    async fn create(&self, spec: &MachineSpec) -> Result<(), VmError> {
        validate_spec(spec)?;
        let mut registry = self.registry.lock().await;
        if let Some(existing) = registry.machines.get(spec.name.as_str())
            && existing.sandbox.is_some()
        {
            return Err(VmError::Command {
                operation: "create",
                exit_code: 1,
                message: format!("machine `{}` already exists", spec.name.as_str()),
            });
        }
        registry.machines.insert(
            spec.name.as_str().to_owned(),
            MachineRecord {
                spec: spec.clone(),
                sandbox: None,
                fork_snapshot: None,
                golden: None,
            },
        );
        self.persist(&registry);
        Ok(())
    }

    async fn start(&self, name: &MachineName) -> Result<(), VmError> {
        let record = self.record(name).await?;
        // A paused sandbox resumes in place: that is AgentENV's fast path and
        // the reason `stop` pauses instead of deleting.
        if let Some(sandbox) = &record.sandbox {
            let states = self.live_sandboxes().await?;
            match states.get(sandbox).map(String::as_str) {
                Some("running") => {
                    self.spawn_keepalive(name, sandbox).await;
                    return Ok(());
                }
                Some(_) => {
                    self.checked(
                        "resume",
                        &[
                            "resume".into(),
                            sandbox.clone(),
                            "--timeout".into(),
                            self.ttl_secs.to_string(),
                        ],
                    )
                    .await?;
                    self.await_guest(sandbox).await?;
                    self.spawn_keepalive(name, sandbox).await;
                    return Ok(());
                }
                // Known to us, unknown to the server: the TTL expired or an
                // operator deleted it. Fall through and start a new sandbox.
                None => {
                    warn!(
                        machine = name.as_str(),
                        sandbox, "AgentENV sandbox vanished; starting a replacement"
                    );
                }
            }
        }
        let sandbox = match start_target(&record.spec.image) {
            StartTarget::Artifact(artifact) => {
                self.start_sandbox_supervised(name, &artifact, None, &record.spec.volumes, "start")
                    .await?
            }
            StartTarget::Image(image) => {
                self.start_sandbox_supervised(
                    name,
                    &image,
                    Some(&record.spec),
                    &record.spec.volumes,
                    "start",
                )
                .await?
            }
        };
        // Record the id before anything else can fail: readiness and dir prep
        // below all run against a live sandbox, and the orchestrator's
        // provisioning-error cleanup (`delete`) can only tear down a sandbox it
        // knows about. An unrecorded id leaks the VM.
        let sandbox_for_record = sandbox.clone();
        self.update(name, move |record| {
            record.sandbox = Some(sandbox_for_record);
        })
        .await?;
        self.await_guest(&sandbox).await?;
        self.prepare_guest_dirs(&sandbox).await?;
        self.spawn_keepalive(name, &sandbox).await;
        Ok(())
    }

    /// Start a fork base.
    ///
    /// AgentENV needs no special mode for this: any running sandbox can be
    /// snapshotted, and the snapshot is what clones boot from. The flag is
    /// recorded so [`VmProvider::fork`] knows the snapshot may be created.
    async fn start_forkable(&self, name: &MachineName) -> Result<(), VmError> {
        self.start(name).await
    }

    /// Fork a clone from a running golden.
    ///
    /// The first fork creates one persistent snapshot from the golden; every
    /// later clone starts from that same snapshot. Unlike SmolVM's single
    /// retained RAM checkpoint, an AgentENV snapshot is immutable and
    /// re-forkable, so concurrent forks are safe — the per-golden lock exists
    /// only to keep two first-forks from building two snapshots.
    async fn fork(&self, golden: &MachineName, clone: &MachineName) -> Result<(), VmError> {
        self.fork_with_volumes(golden, clone, &[]).await
    }

    /// Fork a clone, attaching the caller's read-only volumes at clone start.
    async fn fork_with_volumes(
        &self,
        golden: &MachineName,
        clone: &MachineName,
        volumes: &[VolumeMount],
    ) -> Result<(), VmError> {
        let golden_record = self.record(golden).await?;
        let golden_sandbox = golden_record
            .sandbox
            .clone()
            .ok_or_else(|| VmError::Command {
                operation: "fork",
                exit_code: 1,
                message: format!("fork base `{}` is not running", golden.as_str()),
            })?;
        let snapshot = match golden_record.fork_snapshot.clone() {
            Some(snapshot) => snapshot,
            None => {
                let lock = self.fork_lock(golden).await;
                let _guard = lock.lock().await;
                // Re-read under the lock: a racing fork may have created it.
                match self.record(golden).await?.fork_snapshot {
                    Some(snapshot) => snapshot,
                    None => {
                        let snapshot = format!(
                            "{}-snap-{}",
                            golden.as_str(),
                            &uuid::Uuid::new_v4().simple().to_string()[..8]
                        );
                        self.snapshot_running(
                            &golden_sandbox,
                            "snapshot",
                            &[
                                "snapshot".into(),
                                "create".into(),
                                golden_sandbox.clone(),
                                "--name".into(),
                                snapshot.clone(),
                            ],
                        )
                        .await?;
                        let recorded = snapshot.clone();
                        self.update(golden, move |record| {
                            record.fork_snapshot = Some(recorded);
                        })
                        .await?;
                        snapshot
                    }
                }
            }
        };
        // Insert the clone's record before starting it: the start runs in a
        // detached task that hands the server-assigned sandbox id back even
        // when the caller is cancelled (see `start_sandbox_supervised`), and
        // that task can only tell "torn down while starting" apart from
        // "not inserted yet" when the record exists up front. The row also
        // makes the sandbox deletable by name the moment it exists.
        {
            let mut registry = self.registry.lock().await;
            if registry.machines.contains_key(clone.as_str()) {
                return Err(VmError::Command {
                    operation: "fork",
                    exit_code: 1,
                    message: format!("machine `{}` already exists", clone.as_str()),
                });
            }
            registry.machines.insert(
                clone.as_str().to_owned(),
                MachineRecord {
                    spec: MachineSpec {
                        name: clone.clone(),
                        // Clones boot from the snapshot, never the image.
                        image: format!("agentenv-snapshot:{snapshot}"),
                        volumes: volumes.to_vec(),
                        ..golden_record.spec.clone()
                    },
                    sandbox: None,
                    fork_snapshot: None,
                    golden: Some(golden.as_str().to_owned()),
                },
            );
            self.persist(&registry);
        }
        let sandbox = match self
            .start_sandbox_supervised(clone, &snapshot, None, volumes, "fork")
            .await
        {
            Ok(sandbox) => sandbox,
            Err(error) => {
                // Nothing was started: drop the placeholder so a retry can
                // fork the same name.
                let _ = self.delete(clone).await;
                return Err(error);
            }
        };
        self.update(clone, |record| record.sandbox = Some(sandbox.clone()))
            .await?;
        self.await_guest(&sandbox).await?;
        self.spawn_keepalive(clone, &sandbox).await;
        Ok(())
    }

    /// Pause the sandbox, keeping its identity for a later resume.
    ///
    /// The keepalive is cancelled only once the sandbox is actually parked:
    /// a failed pause leaves a live, unrenewed sandbox behind, and with a
    /// short TTL AgentENV would delete it mid-session.
    async fn stop(&self, name: &MachineName) -> Result<(), VmError> {
        let record = self.record(name).await?;
        let Some(sandbox) = record.sandbox else {
            self.cancel_keepalive(name).await;
            return Ok(());
        };
        match self
            .checked("stop", &["pause".into(), sandbox.clone()])
            .await
        {
            Ok(_) => {
                self.cancel_keepalive(name).await;
                Ok(())
            }
            // Already gone or already paused: `stop` is idempotent.
            Err(VmError::Command { message, .. })
                if is_absent(&message) || message.to_ascii_lowercase().contains("paused") =>
            {
                self.cancel_keepalive(name).await;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn delete(&self, name: &MachineName) -> Result<(), VmError> {
        self.cancel_keepalive(name).await;
        let record = match self.record(name).await {
            Ok(record) => record,
            // Unknown machine: nothing to delete.
            Err(_) => return Ok(()),
        };
        if let Some(sandbox) = &record.sandbox {
            match self
                .checked("delete", &["delete".into(), sandbox.clone()])
                .await
            {
                Ok(_) => {}
                Err(VmError::Command { message, .. }) if is_absent(&message) => {}
                Err(error) => return Err(error),
            }
        }
        let mut registry = self.registry.lock().await;
        // A detached start still running when the record above was read may
        // have recorded its sandbox since: it is live and about to lose its
        // only reference, so delete it with the row. (A start that records
        // *after* this removal observes the missing row and deletes its own
        // sandbox — see `finish_start` — so no interleaving leaks.)
        let late = registry
            .machines
            .get(name.as_str())
            .and_then(|record| record.sandbox.clone())
            .filter(|sandbox| Some(sandbox) != record.sandbox.as_ref());
        registry.machines.remove(name.as_str());
        // Deleting a golden orphans its clones' start artifact, but the clones
        // themselves keep running until their own delete; only the parent link
        // is dropped so a recreated golden cannot inherit stale children.
        for machine in registry.machines.values_mut() {
            if machine.golden.as_deref() == Some(name.as_str()) {
                machine.golden = None;
            }
        }
        self.persist(&registry);
        drop(registry);
        if let Some(sandbox) = late
            && let Err(error) = self
                .checked("delete", &["delete".into(), sandbox.clone()])
                .await
            && !matches!(&error, VmError::Command { message, .. } if is_absent(message))
        {
            warn!(
                machine = name.as_str(),
                sandbox,
                %error,
                "failed to delete a sandbox recorded while the machine was being deleted"
            );
        }
        Ok(())
    }

    async fn status(&self, name: &MachineName) -> Result<MachineState, VmError> {
        let Some(record) = self
            .registry
            .lock()
            .await
            .machines
            .get(name.as_str())
            .cloned()
        else {
            return Ok(MachineState::Missing);
        };
        let Some(sandbox) = record.sandbox else {
            // Created but never started: no sandbox exists yet, which is
            // exactly what the pool treats as stopped.
            return Ok(MachineState::Stopped);
        };
        let states = self.live_sandboxes().await?;
        Ok(match states.get(&sandbox).map(String::as_str) {
            Some("running") => MachineState::Running,
            Some("paused") | Some("pausing") | Some("stopped") => MachineState::Stopped,
            Some(_) => MachineState::Unknown,
            // The server forgot it: an expired TTL or an out-of-band delete.
            // Reconcile so a later `start` provisions a replacement.
            None => {
                let _ = self
                    .update(name, |record| {
                        record.sandbox = None;
                    })
                    .await;
                MachineState::Stopped
            }
        })
    }

    /// The machines this provider knows about.
    ///
    /// Sourced from the registry, not from `aenv ls`: AgentENV has no concept
    /// of a preloop machine name, and the pool's stale-machine sweep must see
    /// names it can pass back to `delete`.
    async fn list(&self) -> Result<Vec<MachineName>, VmError> {
        self.registry
            .lock()
            .await
            .machines
            .keys()
            .map(|name| MachineName::new(name.clone()))
            .collect()
    }

    async fn exec(&self, name: &MachineName, argv: &[String]) -> Result<ExecOutput, VmError> {
        if argv.is_empty() {
            return Err(VmError::InvalidSpec("guest command is empty".into()));
        }
        let sandbox = self.sandbox(name).await?;
        let mut args = vec!["exec".into(), sandbox, "--".into()];
        args.extend_from_slice(argv);
        // Match SmolVM: a guest command's non-zero status is a VM error;
        // callers that intentionally need the status can use a shell command
        // that converts it to output (for example, `cmd; printf '%s' "$?"`).
        self.checked("exec", &args).await
    }

    /// Execute with guest environment values resolved from the host.
    ///
    /// `aenv exec` exposes no environment flags, and putting a secret in argv
    /// would publish it to every process table in the guest. Instead the
    /// resolved values are written to a 0600 file, uploaded, sourced by a
    /// wrapper shell, and unlinked before the payload runs — so the secret is
    /// never an argument, never in the sandbox record, and never in a snapshot.
    async fn exec_with_secret_env(
        &self,
        name: &MachineName,
        argv: &[String],
        secrets: &[(String, SecretSource)],
    ) -> Result<ExecOutput, VmError> {
        if argv.is_empty() {
            return Err(VmError::InvalidSpec("guest command is empty".into()));
        }
        if secrets.is_empty() {
            return self.exec(name, argv).await;
        }
        let sandbox = self.sandbox(name).await?;
        let mut script = String::new();
        for (guest, source) in secrets {
            if !crate::is_env_identifier(guest) {
                return Err(VmError::InvalidSpec(
                    "secret environment names must be non-empty ASCII identifiers".into(),
                ));
            }
            let value = match source {
                SecretSource::HostEnv(host) => {
                    if !crate::is_env_identifier(host) {
                        return Err(VmError::InvalidSpec(
                            "secret environment names must be non-empty ASCII identifiers".into(),
                        ));
                    }
                    std::env::var(host).map_err(|_| {
                        VmError::InvalidSpec(format!("host environment variable {host} is unset"))
                    })?
                }
                SecretSource::HostFile(path) => {
                    if !path.is_absolute() {
                        return Err(VmError::InvalidSpec(
                            "secret file paths must be absolute".into(),
                        ));
                    }
                    std::fs::read_to_string(path)
                        .map_err(|source| VmError::RuntimeIo { source })?
                        .trim_end_matches(['\n', '\r'])
                        .to_owned()
                }
            };
            script.push_str(&format!("export {guest}={}\n", shell_quote(&value)));
        }
        let host_file = tempfile::Builder::new()
            .prefix("preloop-secret-")
            .tempfile()
            .map_err(|source| VmError::RuntimeIo { source })?;
        std::fs::write(host_file.path(), script.as_bytes())
            .map_err(|source| VmError::RuntimeIo { source })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(host_file.path(), std::fs::Permissions::from_mode(0o600))
                .map_err(|source| VmError::RuntimeIo { source })?;
        }
        let guest_file = format!("{GUEST_SECRET_DIR}/{}.env", uuid::Uuid::new_v4().simple());
        self.prepare_guest_dirs(&sandbox).await?;
        self.checked(
            "upload_secret",
            &[
                "upload".into(),
                sandbox.clone(),
                host_file.path().display().to_string(),
                guest_file.clone(),
            ],
        )
        .await?;
        let payload = argv
            .iter()
            .map(|argument| shell_quote(argument))
            .collect::<Vec<_>>()
            .join(" ");
        // The secret directory is root-only, so reading and unlinking the file
        // run through the root-or-sudo wrapper. The payload keeps the exec
        // user's identity, as a plain `exec` does.
        let read = run_as_root_or_sudo_strict(&format!("cat {}", shell_quote(&guest_file)));
        let unlink = run_as_root_or_sudo_strict(&format!("rm -f {}", shell_quote(&guest_file)));
        let wrapper =
            format!("set -eu; SECRETS=$({read}); {unlink}; eval \"$SECRETS\"; exec {payload}");
        let wrapper_args = [
            "exec".into(),
            sandbox.clone(),
            "--".into(),
            "/bin/sh".into(),
            "-c".into(),
            wrapper,
        ];
        // `checked`, not `capture`: callers treat `Err` as "the payload did
        // not run to success" — a failed `configure` must fail the
        // provisioning, exactly as it does through SmolVmProvider, whose
        // exec path is `checked` as well.
        // Sudo for the privileged read and unlink is verified by the
        // `prepare_guest_dirs` call above, which names the prerequisite; the
        // payload's own errors are not rewritten here.
        let result = self.checked("exec", &wrapper_args).await;
        // The wrapper unlinks the file itself, but a failure before its removal
        // ran would leave a live secret behind, so clean up on every failure
        // shape. Best-effort: the removal may be refused and must not mask the
        // payload's own error.
        if result.is_err() {
            let cleanup = run_as_root_or_sudo(&format!("rm -f {}", shell_quote(&guest_file)));
            let _ = self
                .capture(&[
                    "exec".into(),
                    sandbox,
                    "--".into(),
                    "/bin/sh".into(),
                    "-c".into(),
                    cleanup,
                ])
                .await;
        }
        result
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
        let sandbox = self.sandbox(name).await?;
        let launch = |source| VmError::Launch {
            program: self.binary.display().to_string(),
            source,
        };
        let mut child = Command::new(&self.binary)
            .args(["exec", &sandbox, "--"])
            .args(argv)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(launch)?;
        stream_output(&mut child, output).await.map_err(launch)
    }

    /// Copy between host and guest using SmolVM's `machine:path` syntax.
    ///
    /// The syntax is preloop's, not SmolVM's alone: exactly one side carries a
    /// `<machine>:` prefix, and that decides upload versus download.
    async fn copy(&self, source: &str, destination: &str) -> Result<(), VmError> {
        if source.is_empty() || destination.is_empty() {
            return Err(VmError::InvalidSpec("copy paths cannot be empty".into()));
        }
        match (split_machine_path(source), split_machine_path(destination)) {
            (None, Some((machine, guest))) => {
                let sandbox = self.sandbox(&MachineName::new(machine)?).await?;
                self.checked(
                    "copy",
                    &[
                        "upload".into(),
                        sandbox,
                        source.to_owned(),
                        guest.to_owned(),
                    ],
                )
                .await
                .map(|_| ())
            }
            (Some((machine, guest)), None) => {
                let sandbox = self.sandbox(&MachineName::new(machine)?).await?;
                self.checked(
                    "copy",
                    &[
                        "download".into(),
                        sandbox,
                        guest.to_owned(),
                        destination.to_owned(),
                        "--force".into(),
                    ],
                )
                .await
                .map(|_| ())
            }
            _ => Err(VmError::InvalidSpec(
                "exactly one copy path must name a machine as `<machine>:<path>`".into(),
            )),
        }
    }

    /// Pack a machine into a reusable artifact.
    ///
    /// AgentENV keeps the bytes server-side, so the "artifact" is a persistent
    /// snapshot plus a small JSON descriptor at `output` naming it. The
    /// descriptor is also written to the `<output>.smolmachine` sidecar because
    /// the orchestrator's artifact plumbing expects the packed payload there.
    async fn pack(&self, name: &MachineName, output: &Path) -> Result<(), VmError> {
        if !output.is_absolute() {
            return Err(VmError::InvalidSpec(
                "pack output path must be absolute".into(),
            ));
        }
        let record = self.record(name).await?;
        let sandbox = record.sandbox.clone().ok_or_else(|| VmError::Command {
            operation: "pack",
            exit_code: 1,
            message: format!("machine `{}` is not running", name.as_str()),
        })?;
        let snapshot = format!(
            "{}-pack-{}",
            name.as_str(),
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        );
        self.snapshot_running(
            &sandbox,
            "pack",
            &[
                "snapshot".into(),
                "create".into(),
                sandbox.clone(),
                "--name".into(),
                snapshot.clone(),
            ],
        )
        .await?;
        let descriptor = serde_json::to_vec_pretty(&PackDescriptor {
            provider: PACK_PROVIDER.to_owned(),
            snapshot,
            image: record.spec.image.clone(),
        })
        .map_err(|error| VmError::Protocol(error.to_string()))?;
        if let Some(parent) = output.parent() {
            std::fs::create_dir_all(parent).map_err(|source| VmError::RuntimeIo { source })?;
        }
        std::fs::write(output, &descriptor).map_err(|source| VmError::RuntimeIo { source })?;
        std::fs::write(
            PathBuf::from(format!("{}.smolmachine", output.display())),
            &descriptor,
        )
        .map_err(|source| VmError::RuntimeIo { source })?;
        Ok(())
    }

    /// Re-arm a fork base.
    ///
    /// Always possible: the snapshot clones boot from is immutable, so a spent
    /// base is a concept AgentENV does not have. The golden is only ensured to
    /// be running again, and a partial clone from a failed fork is removed.
    async fn rearm_fork_base(
        &self,
        golden: &MachineName,
        partial: Option<&MachineName>,
    ) -> Result<bool, VmError> {
        if let Some(partial) = partial {
            self.delete(partial).await?;
        }
        match self.status(golden).await? {
            MachineState::Running => Ok(true),
            MachineState::Stopped => {
                self.start(golden).await?;
                Ok(true)
            }
            MachineState::Missing | MachineState::Unknown => Ok(false),
        }
    }
}

/// Whether an `aenv` failure means "the thing is not there".
fn is_absent(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("not found") || message.contains("404")
}

/// Whether an `aenv volume create` failure means the name is already taken.
fn is_already_exists(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("already exists") || message.contains("409")
}

/// Whether an `aenv volume delete` failure is the server refusing a volume that a
/// sandbox still holds. AgentENV answers `HTTP 409: volume is reserved by sandbox
/// <id>` for an exclusive or read-only volume that a sandbox mounts.
fn is_mounted(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("reserved by sandbox") || message.contains("mounted")
}

/// Whether an `aenv start --cold` failure is only the disk-shrink refusal.
///
/// AgentENV materializes the guest root from an overlaybd base and refuses to
/// present it smaller than the base's virtual size:
/// `requested overlaybd runtime virtual size 8589934592 is smaller than base
/// virtual size 68719476736; shrinking is disabled`.
fn is_disk_too_small(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("shrinking is disabled")
        || (message.contains("virtual size") && message.contains("is smaller than base"))
}

/// Split a `<machine>:<path>` copy operand.
///
/// A Windows-style drive letter cannot appear here (paths are POSIX), but a
/// bare absolute path must not be mistaken for a machine reference, so a
/// leading `/` disqualifies the operand.
fn split_machine_path(operand: &str) -> Option<(&str, &str)> {
    if operand.starts_with('/') {
        return None;
    }
    let (machine, path) = operand.split_once(':')?;
    (!machine.is_empty() && path.starts_with('/')).then_some((machine, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn machine_path_split_only_accepts_a_machine_and_absolute_guest_path() {
        assert_eq!(
            split_machine_path("runner-1:/opt/preloop/bin"),
            Some(("runner-1", "/opt/preloop/bin"))
        );
        assert_eq!(split_machine_path("/host/path"), None);
        assert_eq!(split_machine_path("/host/weird:name"), None);
        assert_eq!(split_machine_path("runner-1:relative"), None);
        assert_eq!(split_machine_path("plain-name"), None);
    }

    /// The refusals and absences `aenv volume` reports, copied from an AgentENV
    /// 0.2.1 server.
    #[test]
    fn volume_errors_are_classified_by_what_the_server_said() {
        assert!(is_already_exists(
            "Error: HTTP 409: volume name already exists: preloop-runner-0123456789ab"
        ));
        assert!(!is_already_exists("Error: HTTP 404: volume not found: x"));
        assert!(is_absent(
            "Error: HTTP 404: volume not found: preloop-runner-0123456789ab"
        ));
        assert!(is_mounted(
            "Error: HTTP 409: volume is reserved by sandbox 01a1419e-058a-7733-baea-3e37a236fe4d"
        ));
        assert!(!is_mounted("Error: HTTP 404: volume not found: x"));
    }

    #[test]
    fn shell_quoting_survives_embedded_quotes() {
        assert_eq!(shell_quote("plain"), "'plain'");
        assert_eq!(shell_quote("it's"), "'it'\\''s'");
    }

    #[test]
    fn socket_mounts_are_rejected_with_the_tcp_remedy() {
        let spec = MachineSpec {
            name: MachineName::new("runner").unwrap(),
            image: "ubuntu:24.04".into(),
            cpus: 2,
            memory_mib: 2048,
            storage_gib: 8,
            overlay_gib: None,
            network: NetworkPolicy::Unrestricted,
            volumes: Vec::new(),
            sockets: vec![crate::SocketMount {
                host: PathBuf::from("/run/engine.sock"),
                guest: PathBuf::from("/run/engine.sock"),
            }],
            dns: None,
            rosetta: false,
        };
        let error = validate_spec(&spec).unwrap_err();
        assert!(
            matches!(&error, VmError::InvalidSpec(message) if message.contains("control_socket")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn unenforceable_network_dns_and_storage_are_rejected() {
        let base = || MachineSpec {
            name: MachineName::new("runner").unwrap(),
            image: "ubuntu:24.04".into(),
            cpus: 2,
            memory_mib: 2048,
            storage_gib: 8,
            overlay_gib: None,
            network: NetworkPolicy::Unrestricted,
            volumes: Vec::new(),
            sockets: Vec::new(),
            dns: None,
            rosetta: false,
        };
        assert!(validate_spec(&base()).is_ok());
        let mut restricted = base();
        restricted.network = NetworkPolicy::Restricted {
            hosts: Vec::new(),
            cidrs: vec!["10.0.0.0/8".to_owned()],
        };
        assert!(matches!(
            validate_spec(&restricted),
            Err(VmError::InvalidSpec(_))
        ));
        let mut dns = base();
        dns.dns = Some("192.168.1.1".to_owned());
        assert!(matches!(validate_spec(&dns), Err(VmError::InvalidSpec(_))));
        let mut zero_storage = base();
        zero_storage.storage_gib = 0;
        assert!(matches!(
            validate_spec(&zero_storage),
            Err(VmError::InvalidSpec(_))
        ));
    }

    #[test]
    fn pack_descriptors_round_trip_and_reject_foreign_files() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("golden");
        std::fs::write(
            &path,
            serde_json::to_vec(&PackDescriptor {
                provider: PACK_PROVIDER.to_owned(),
                snapshot: "golden-pack-abc".to_owned(),
                image: "ubuntu:24.04".to_owned(),
            })
            .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            start_target(&path.display().to_string()),
            StartTarget::Artifact(snapshot) if snapshot == "golden-pack-abc"
        ));

        let foreign = directory.path().join("smol");
        std::fs::write(&foreign, b"SMOLPACK").unwrap();
        assert!(matches!(
            start_target(&foreign.display().to_string()),
            StartTarget::Image(_)
        ));
    }

    #[test]
    fn explicit_snapshot_references_start_from_the_artifact() {
        assert!(matches!(
            start_target("agentenv-snapshot:golden-snap-1"),
            StartTarget::Artifact(snapshot) if snapshot == "golden-snap-1"
        ));
        assert!(matches!(
            start_target("ubuntu:24.04"),
            StartTarget::Image(image) if image == "ubuntu:24.04"
        ));
    }

    /// The exact refusal observed from AgentENV 0.2.0 on a 64 GiB Ubuntu
    /// 24.04 overlaybd base when preloop asked for its usual 8 GiB.
    #[test]
    fn disk_shrink_refusal_is_recognized_and_other_failures_are_not() {
        assert!(is_disk_too_small(
            "materialize overlaybd runtime in /var/lib/aenv/firecracker-work/agentenv-fc-j5JbMg/\
             overlaybd: requested overlaybd runtime virtual size 8589934592 is smaller than base \
             virtual size 68719476736; shrinking is disabled"
        ));
        assert!(!is_disk_too_small("HTTP 404: route not found"));
        assert!(!is_disk_too_small(
            "failed to start sandbox: no capacity for a new microVM"
        ));
    }

    /// Write an executable fake `aenv` where `start` blocks until `release`
    /// exists (a server-side start in flight) and then prints `sandbox`, and
    /// every `delete <id>` is appended to `log`.
    fn fake_aenv(
        dir: &std::path::Path,
        sandbox: &str,
        started: &std::path::Path,
        release: &std::path::Path,
        log: &std::path::Path,
    ) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let binary = dir.join("aenv");
        std::fs::write(
            &binary,
            format!(
                "#!/bin/sh\n\
                 case \"$1\" in\n\
                 start)\n\
                   : > '{started}'\n\
                   while [ ! -f '{release}' ]; do sleep 0.02; done\n\
                   echo '{sandbox}'\n\
                   ;;\n\
                 delete)\n\
                   printf 'delete %s\\n' \"$2\" >> '{log}'\n\
                   ;;\n\
                 ls) echo '[]' ;;\n\
                 esac\n\
                 exit 0\n",
                started = started.display(),
                release = release.display(),
                log = log.display(),
            ),
        )
        .expect("write fake aenv");
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755))
            .expect("chmod fake aenv");
        binary
    }

    fn cancellation_spec(name: &MachineName) -> MachineSpec {
        MachineSpec {
            name: name.clone(),
            image: "ubuntu:24.04".into(),
            cpus: 2,
            memory_mib: 2048,
            storage_gib: 8,
            overlay_gib: None,
            network: NetworkPolicy::Unrestricted,
            volumes: Vec::new(),
            sockets: Vec::new(),
            dns: None,
            rosetta: false,
        }
    }

    /// Wait (bounded) for `path` to exist.
    async fn wait_for_path(path: &std::path::Path) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !path.exists() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "{} never appeared",
                path.display()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Wait (bounded) for `needle` to appear in `path`.
    async fn wait_for_log(path: &std::path::Path, needle: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if std::fs::read_to_string(path).is_ok_and(|text| text.contains(needle)) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "`{needle}` never appeared in {}",
                path.display()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// A start cancelled mid-flight must not strand the server-side sandbox:
    /// the id is only known from the command's output, and dropping the
    /// caller's future kills the local client that would have read it. The
    /// detached start task records the id regardless, so the pool's teardown
    /// (which deletes machines from what the registry says) can reach it.
    #[tokio::test]
    async fn cancelled_start_still_records_the_server_assigned_sandbox() {
        let dir = tempfile::tempdir().unwrap();
        let started = dir.path().join("started");
        let release = dir.path().join("release");
        let log = dir.path().join("delete.log");
        let binary = fake_aenv(dir.path(), "s1", &started, &release, &log);
        let provider = AgentEnvProvider::new(binary)
            .with_registry_path(dir.path().join("agentenv-machines.json"));
        let name = MachineName::new("runner-0").unwrap();
        provider.create(&cancellation_spec(&name)).await.unwrap();

        let start = {
            let provider = provider.clone();
            let name = name.clone();
            tokio::spawn(async move { provider.start(&name).await })
        };
        wait_for_path(&started).await;
        // The pool cancels provisioning on shutdown; the caller's future is
        // dropped while `aenv start` is still running.
        start.abort();
        let _ = start.await;

        // The server-side start now completes. Its id must be recorded even
        // though nobody waited for it.
        std::fs::write(&release, b"").unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(record) = provider.record(&name).await
                && record.sandbox.as_deref() == Some("s1")
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the cancelled start's sandbox was never recorded"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // And teardown deletes it like any recorded machine.
        provider.delete(&name).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(&log).unwrap().trim(),
            "delete s1",
            "teardown must reach the sandbox the cancelled start created"
        );
    }

    /// A start that completes after its machine was torn down must not leave
    /// the sandbox orphaned: the row it would have been recorded into is
    /// gone, so the detached task deletes the sandbox outright.
    #[tokio::test]
    async fn start_completing_after_teardown_deletes_the_unrecorded_sandbox() {
        let dir = tempfile::tempdir().unwrap();
        let started = dir.path().join("started");
        let release = dir.path().join("release");
        let log = dir.path().join("delete.log");
        let binary = fake_aenv(dir.path(), "s1", &started, &release, &log);
        let provider = AgentEnvProvider::new(binary)
            .with_registry_path(dir.path().join("agentenv-machines.json"));
        let name = MachineName::new("runner-0").unwrap();
        provider.create(&cancellation_spec(&name)).await.unwrap();

        let start = {
            let provider = provider.clone();
            let name = name.clone();
            tokio::spawn(async move { provider.start(&name).await })
        };
        wait_for_path(&started).await;
        // Teardown: the machine record is deleted while its start is in
        // flight, when there is no sandbox id to delete yet.
        provider.delete(&name).await.unwrap();
        std::fs::write(&release, b"").unwrap();

        wait_for_log(&log, "delete s1").await;
        // The caller is still waiting on the start; it now fails because its
        // machine is gone, instead of adopting a deleted sandbox.
        assert!(
            start.await.unwrap().is_err(),
            "a start whose machine was torn down must report the failure"
        );
    }
}
