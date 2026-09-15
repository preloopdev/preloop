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
//! | `start` | `aenv start --cold <image> --cpu --memory --disk-size-mb -d`, or `aenv resume` for a paused sandbox |
//! | `start_forkable` | `start` + mark the record as a fork base (the snapshot is taken lazily by the first `fork`) |
//! | `fork` | `aenv snapshot create <golden> --name <snap>` once, then `aenv start <snap> -d` per clone |
//! | `stop` | `aenv pause` (the sandbox keeps its id, so `start` resumes it) |
//! | `delete` | `aenv delete` |
//! | `status` | `aenv ls --output json`, matched on the recorded sandbox id |
//! | `list` | the local registry (AgentENV has no notion of preloop machine names) |
//! | `exec` / `exec_stream` | `aenv exec <id> -- argv` |
//! | `exec_with_secret_env` | secret file uploaded 0600, sourced by a wrapper shell, then unlinked |
//! | `copy` | `aenv upload` / `aenv download` |
//! | `pack` | `aenv snapshot create --name`, plus a JSON descriptor at the output path |
//! | `rearm_fork_base` | always available: AgentENV snapshots are persistent and re-forkable |
//!
//! # Capability gaps (deliberate, not stubs)
//!
//! * **No host bind mounts.** AgentENV volumes are block devices
//!   (`aenv volume create`, attached as `--volume /guest/path=<volume>`); there
//!   is no virtiofs/9p host-directory passthrough anywhere in the CLI or HTTP
//!   API. [`MachineSpec::volumes`] is therefore *materialized*: the host
//!   directory is streamed into the guest as a tar archive at start time (see
//!   [`AgentEnvProvider::materialize_volumes`]). Reads inside the guest are
//!   then local-disk reads rather than virtiofs round trips — faster per
//!   access, paid for once per machine. Fork clones inherit an already
//!   materialized filesystem from the snapshot, so the cost lands on the
//!   golden only.
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

use crate::{
    read_bounded, stream_output, ExecOutput, MachineName, MachineSpec, MachineState, NetworkPolicy,
    OutputChunk, SecretSource, VmError, VmProvider, VolumeMount, DEFAULT_CAPTURE_LIMIT,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, warn};

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
/// Guest staging path for materialized volume archives.
const GUEST_STAGING_DIR: &str = "/run/preloop-vm-staging";

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
    /// Whether the volumes in `spec` have been streamed into the guest.
    volumes_materialized: bool,
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
        }
    }

    /// Construct a provider from the process environment.
    ///
    /// `PRELOOP_AENV_BINARY` overrides the executable and
    /// `PRELOOP_AENV_TTL_SECS` the sandbox TTL; the registry is persisted under
    /// the effective preloop home so a restarted engine can still enumerate and
    /// tear down the machines it created.
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
            provider = provider.with_registry_path(home.join("agentenv-machines.json"));
        }
        provider
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
                    // AgentENV requires whole-GiB steps.
                    args.extend([
                        "--disk-size-mb".into(),
                        (spec.storage_gib.max(1) * 1024).to_string(),
                    ]);
                }
            } else {
                args.push(target.to_owned());
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
                match result {
                    Ok(status) if status.success() => {}
                    // The sandbox is gone (deleted, or its TTL already
                    // expired): nothing left to keep alive.
                    Ok(_) => break,
                    Err(error) => {
                        warn!(%error, sandbox, "AgentENV TTL keepalive could not run");
                        break;
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

    // -- volume materialization ---------------------------------------------

    /// Stream every [`MachineSpec::volumes`] entry into the guest.
    ///
    /// AgentENV has no host-directory passthrough, so a host mount becomes a
    /// one-time copy. The copy goes through a single tar stream rather than
    /// per-file uploads because `aenv upload` refuses symlinks and does not
    /// preserve modes — both of which the runner bundle and the Node externals
    /// rely on — and because one transfer beats thousands of gRPC round trips.
    ///
    /// A read-only mount is approximated by stripping write bits in the guest.
    /// That is weaker than virtiofs `:ro` (root inside the guest can restore
    /// them) and is documented as such: the guest boundary, not the mount flag,
    /// is what isolates the host here — and unlike SmolVM there is no host
    /// directory behind it to protect in the first place.
    async fn materialize_volumes(
        &self,
        sandbox: &str,
        volumes: &[VolumeMount],
    ) -> Result<(), VmError> {
        for volume in volumes {
            let archive = tar_directory(&volume.host).await?;
            let guest_archive =
                format!("{GUEST_STAGING_DIR}/{}.tar", uuid::Uuid::new_v4().simple());
            let guest_dir = volume.guest.display().to_string();
            self.checked(
                "upload",
                &[
                    "upload".into(),
                    sandbox.to_owned(),
                    archive.path().display().to_string(),
                    guest_archive.clone(),
                ],
            )
            .await
            .map_err(|error| match error {
                // The staging directory does not exist on a first upload;
                // `aenv upload` creates parents only for directory transfers.
                VmError::Command { .. } => error,
                other => other,
            })?;
            let script = format!(
                "set -eu; mkdir -p {dir}; tar -xf {archive} -C {dir}; rm -f {archive}{ro}",
                dir = shell_quote(&guest_dir),
                archive = shell_quote(&guest_archive),
                ro = if volume.read_only {
                    format!(" ; chmod -R a-w {}", shell_quote(&guest_dir))
                } else {
                    String::new()
                },
            );
            let output = self
                .capture(&[
                    "exec".into(),
                    sandbox.to_owned(),
                    "--".into(),
                    "/bin/sh".into(),
                    "-c".into(),
                    script,
                ])
                .await?;
            if output.exit_code != 0 {
                return Err(self.command_error("materialize_volume", &output));
            }
            debug!(sandbox, guest = %guest_dir, "materialized host volume into guest");
        }
        Ok(())
    }

    /// Create the guest scratch directories the provider uploads into.
    async fn prepare_guest_dirs(&self, sandbox: &str) -> Result<(), VmError> {
        self.checked(
            "prepare_guest_dirs",
            &[
                "exec".into(),
                sandbox.to_owned(),
                "--".into(),
                "/bin/sh".into(),
                "-c".into(),
                format!(
                    "mkdir -p {GUEST_STAGING_DIR} {GUEST_SECRET_DIR} && chmod 700 {GUEST_SECRET_DIR}"
                ),
            ],
        )
        .await
        .map(|_| ())
    }
}

/// Quote `value` for POSIX `sh`.
fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// Tar `directory` into a temporary host file, preserving modes and symlinks.
///
/// Runs on the blocking pool: `tar::Builder` is synchronous and a runner bundle
/// is large enough that walking it on a reactor thread would stall other slots.
async fn tar_directory(directory: &Path) -> Result<tempfile::NamedTempFile, VmError> {
    let directory = directory.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let file = tempfile::Builder::new()
            .prefix("preloop-volume-")
            .suffix(".tar")
            .tempfile()
            .map_err(|source| VmError::RuntimeIo { source })?;
        let mut builder = tar::Builder::new(
            file.reopen()
                .map_err(|source| VmError::RuntimeIo { source })?,
        );
        builder.follow_symlinks(false);
        builder
            .append_dir_all(".", &directory)
            .map_err(|source| VmError::RuntimeIo { source })?;
        builder
            .into_inner()
            .and_then(|mut file| {
                use std::io::Write as _;
                file.flush()
            })
            .map_err(|source| VmError::RuntimeIo { source })?;
        Ok(file)
    })
    .await
    .map_err(|error| VmError::Protocol(format!("volume archive task failed: {error}")))?
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
    if path.is_file() {
        if let Some(descriptor) = read_pack_descriptor(path) {
            return StartTarget::Artifact(descriptor.snapshot);
        }
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
    for mount in &spec.volumes {
        if !mount.host.is_absolute() || !mount.guest.is_absolute() {
            return Err(VmError::InvalidSpec("volume paths must be absolute".into()));
        }
        if !mount.host.is_dir() {
            return Err(VmError::InvalidSpec(format!(
                "volume source is not a directory: {}",
                mount.host.display()
            )));
        }
    }
    if matches!(spec.network, NetworkPolicy::Disabled) {
        return Err(VmError::InvalidSpec(
            "AgentENV sandboxes are always networked; NetworkPolicy::Disabled cannot be honoured"
                .into(),
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
            fork_inherits_guest_writes: true,
            preserves_runtime_state_on_suspend: true,
        }
    }

    /// Record the machine. AgentENV has no unstarted sandbox to create.
    ///
    /// Deliberately cheap: booting here and immediately pausing would double
    /// the cost of the pool's create-then-start sequence, and preloop treats a
    /// created-but-unstarted machine as [`MachineState::Stopped`] anyway.
    async fn create(&self, spec: &MachineSpec) -> Result<(), VmError> {
        validate_spec(spec)?;
        let mut registry = self.registry.lock().await;
        if let Some(existing) = registry.machines.get(spec.name.as_str()) {
            if existing.sandbox.is_some() {
                return Err(VmError::Command {
                    operation: "create",
                    exit_code: 1,
                    message: format!("machine `{}` already exists", spec.name.as_str()),
                });
            }
        }
        registry.machines.insert(
            spec.name.as_str().to_owned(),
            MachineRecord {
                spec: spec.clone(),
                sandbox: None,
                fork_snapshot: None,
                golden: None,
                volumes_materialized: false,
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
            StartTarget::Artifact(artifact) => self.start_sandbox(&artifact, None, "start").await?,
            StartTarget::Image(image) => {
                self.start_sandbox(&image, Some(&record.spec), "start")
                    .await?
            }
        };
        self.await_guest(&sandbox).await?;
        self.prepare_guest_dirs(&sandbox).await?;
        if !record.spec.volumes.is_empty() && !record.volumes_materialized {
            self.materialize_volumes(&sandbox, &record.spec.volumes)
                .await?;
        }
        let sandbox_for_record = sandbox.clone();
        self.update(name, move |record| {
            record.sandbox = Some(sandbox_for_record);
            record.volumes_materialized = true;
        })
        .await?;
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
                        self.checked(
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
        let sandbox = self.start_sandbox(&snapshot, None, "fork").await?;
        {
            let mut registry = self.registry.lock().await;
            registry.machines.insert(
                clone.as_str().to_owned(),
                MachineRecord {
                    spec: MachineSpec {
                        name: clone.clone(),
                        // Clones boot from the snapshot, never the image.
                        image: format!("agentenv-snapshot:{snapshot}"),
                        ..golden_record.spec.clone()
                    },
                    sandbox: Some(sandbox.clone()),
                    fork_snapshot: None,
                    golden: Some(golden.as_str().to_owned()),
                    // Inherited from the golden's filesystem via the snapshot.
                    volumes_materialized: true,
                },
            );
            self.persist(&registry);
        }
        self.await_guest(&sandbox).await?;
        self.spawn_keepalive(clone, &sandbox).await;
        Ok(())
    }

    /// Pause the sandbox, keeping its identity for a later resume.
    async fn stop(&self, name: &MachineName) -> Result<(), VmError> {
        self.cancel_keepalive(name).await;
        let record = self.record(name).await?;
        let Some(sandbox) = record.sandbox else {
            return Ok(());
        };
        match self
            .checked("stop", &["pause".into(), sandbox.clone()])
            .await
        {
            Ok(_) => Ok(()),
            // Already gone or already paused: `stop` is idempotent.
            Err(VmError::Command { message, .. })
                if is_absent(&message) || message.to_ascii_lowercase().contains("paused") =>
            {
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
        registry.machines.remove(name.as_str());
        // Deleting a golden orphans its clones' start artifact, but the clones
        // themselves keep running until their own delete; only the parent link
        // is dropped so a recreated golden cannot inherit stale children.
        for record in registry.machines.values_mut() {
            if record.golden.as_deref() == Some(name.as_str()) {
                record.golden = None;
            }
        }
        self.persist(&registry);
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
                        record.volumes_materialized = false;
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
        let wrapper = format!(
            "set -eu; . {file}; rm -f {file}; exec {payload}",
            file = shell_quote(&guest_file),
        );
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
        let result = self.checked("exec", &wrapper_args).await;
        // The wrapper unlinks the file itself, but a failure before the
        // wrapper's `rm -f` ran would leave it behind with a live secret in
        // it, so clean up on every failure shape.
        if result.is_err() {
            let _ = self
                .capture(&[
                    "exec".into(),
                    sandbox,
                    "--".into(),
                    "/bin/sh".into(),
                    "-c".into(),
                    format!("rm -f {}", shell_quote(&guest_file)),
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
        self.checked(
            "pack",
            &[
                "snapshot".into(),
                "create".into(),
                sandbox,
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
}
