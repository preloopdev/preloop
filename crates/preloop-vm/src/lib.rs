//! Typed SmolVM lifecycle primitives for Preloop CI.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::sync::mpsc;

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
    /// are denied. Enforced by the virtio-net backend with
    /// `SMOLVM_EGRESS_FLOOR=strict` in the provider.
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
/// (TSI has no host network stack), so PublicOnly needs it. The macOS
/// release artifacts 1.7.2-1.7.4 bundle a NET=1 libkrun that exports
/// `krun_add_net_unixstream`; the 1.7.5 artifact shipped a libkrun without
/// it ("libkrun does not expose krun_add_net_unixstream"), which is why
/// `preloop update` pins the smolvm install to the last known-good release.
/// `PRELOOP_SMOLVM_NET_BACKEND=tsi|virtio-net` overrides the choice for
/// setups stuck on a broken artifact.
fn public_only_net_backend() -> &'static str {
    match std::env::var("PRELOOP_SMOLVM_NET_BACKEND").as_deref() {
        Ok("tsi") => return "tsi",
        Ok("virtio-net") => return "virtio-net",
        _ => {}
    }
    "virtio-net"
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
    /// Invalid SmolVM JSON output.
    #[error("invalid smolvm response: {0}")]
    Protocol(String),
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
         the engine resolves (PATH) and update it, e.g. https://smolmachines.com/install.sh"
    )]
    UnsupportedSocketMount {
        /// Program path.
        binary: String,
    },
}

/// Provider contract consumed by the Preloop orchestrator.
#[async_trait]
pub trait VmProvider: Send + Sync {
    /// Create a persistent machine.
    async fn create(&self, spec: &MachineSpec) -> Result<(), VmError>;
    /// Start an existing machine.
    async fn start(&self, name: &MachineName) -> Result<(), VmError>;
    /// Start an existing machine as a forkable base (CoW memory + disks).
    async fn start_forkable(&self, name: &MachineName) -> Result<(), VmError>;
    /// Fork a running forkable machine into a new clone with CoW memory and disks.
    async fn fork(&self, golden: &MachineName, clone: &MachineName) -> Result<(), VmError>;
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

/// CLI-backed SmolVM provider.
#[derive(Debug, Clone)]
pub struct SmolVmProvider {
    binary: PathBuf,
    capture_limit: usize,
    /// Serializes operations that build or replace a machine's base against
    /// everything else. See [`SmolVmProvider::exclusive`].
    lifecycle_lock: Arc<tokio::sync::RwLock<()>>,
    /// Whether the resolved binary's `machine create` accepts
    /// `--mount-socket`, probed once per provider.
    socket_mount_supported: Arc<tokio::sync::OnceCell<bool>>,
}

impl Default for SmolVmProvider {
    fn default() -> Self {
        Self::new("smolvm")
    }
}

impl SmolVmProvider {
    /// Construct a provider using an explicit SmolVM executable.
    pub fn new(binary: impl Into<PathBuf>) -> Self {
        Self {
            binary: binary.into(),
            capture_limit: DEFAULT_CAPTURE_LIMIT,
            lifecycle_lock: Arc::new(tokio::sync::RwLock::new(())),
            socket_mount_supported: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    /// Override the maximum bytes retained from each process stream.
    pub fn with_capture_limit(mut self, bytes: usize) -> Self {
        self.capture_limit = bytes.max(1024);
        self
    }

    fn command(&self) -> Command {
        Command::new(&self.binary)
    }

    /// Whether `machine create` accepts `--mount-socket`, probed from the
    /// binary's own help text and cached. SmolVM's wrapper scripts can
    /// report a recent `--version` while resolving to an old binary, so the
    /// flag's presence is the reliable capability check.
    async fn supports_mount_socket(&self) -> bool {
        let mut command = self.command();
        command
            .args(["machine", "create", "--help"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let Ok(mut child) = command.spawn() else {
            return false;
        };
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let (stdout, stderr, status) = tokio::join!(
            read_bounded(stdout, self.capture_limit),
            read_bounded(stderr, self.capture_limit),
            child.wait(),
        );
        let (Ok((stdout, _)), Ok(_), Ok(status)) = (stdout, stderr, status) else {
            return false;
        };
        status.success() && String::from_utf8_lossy(&stdout).contains("--mount-socket")
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
        let mut command = self.command();
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
    /// made the whole refill wait one VM operation at a time. Forking four
    /// clones from the same golden — including the first forks, which trigger
    /// the base freeze — measured 101-119 ms concurrently against 271-283 ms
    /// serially, with every clone usable and no failures across three trials.
    /// They stay excluded from base construction, so a golden cannot be
    /// replaced underneath a fork.
    async fn concurrent(
        &self,
        operation: &'static str,
        args: &[String],
    ) -> Result<ExecOutput, VmError> {
        let _guard = self.lifecycle_lock.read().await;
        self.checked(operation, args).await
    }
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
            // A bare rootfs directory carries no OCI metadata, so the image
            // defines no entrypoint/CMD and `machine start` refuses to run a
            // detached workload. Everything this provider does runs through
            // `machine exec` anyway, so pin a harmless keep-alive workload
            // for directory images. Registry images keep their own CMD.
            if Path::new(&spec.image).is_dir() {
                args.extend([
                    "--".into(),
                    "/bin/sh".into(),
                    "-c".into(),
                    "sleep infinity".into(),
                ]);
            }
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
                args.extend(["--net".into(), "--net-backend".into(), "virtio-net".into()]);
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
                .get_or_init(|| self.supports_mount_socket())
                .await
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

    async fn start(&self, name: &MachineName) -> Result<(), VmError> {
        self.exclusive(
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

    async fn fork(&self, golden: &MachineName, clone: &MachineName) -> Result<(), VmError> {
        self.concurrent(
            "fork",
            &[
                "machine".into(),
                "fork".into(),
                "--golden".into(),
                golden.as_str().into(),
                "--name".into(),
                clone.as_str().into(),
            ],
        )
        .await
        .map(|_| ())
    }

    async fn stop(&self, name: &MachineName) -> Result<(), VmError> {
        self.concurrent(
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
        self.concurrent(
            "delete",
            &[
                "machine".into(),
                "delete".into(),
                "--name".into(),
                name.as_str().into(),
                "-f".into(),
            ],
        )
        .await
        .map(|_| ())
    }

    async fn status(&self, name: &MachineName) -> Result<MachineState, VmError> {
        let args = [
            "machine".into(),
            "status".into(),
            "--name".into(),
            name.as_str().into(),
        ];
        match self.concurrent("status", &args).await {
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
            .concurrent("list", &["machine".into(), "ls".into(), "--json".into()])
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
        let mut command = self.command();
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
        let stdout_task = forward(
            child.stdout.take().expect("piped stdout"),
            output.clone(),
            true,
        );
        let stderr_task = forward(child.stderr.take().expect("piped stderr"), output, false);
        let (stdout_result, stderr_result, status) =
            tokio::join!(stdout_task, stderr_task, child.wait());
        stdout_result.map_err(|source| VmError::Launch {
            program: self.binary.display().to_string(),
            source,
        })?;
        stderr_result.map_err(|source| VmError::Launch {
            program: self.binary.display().to_string(),
            source,
        })?;
        let status = status.map_err(|source| VmError::Launch {
            program: self.binary.display().to_string(),
            source,
        })?;
        Ok(status.code().unwrap_or(-1))
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
        self.exclusive_with_staging(
            "pack",
            &[
                "pack".into(),
                "create".into(),
                "--from-vm".into(),
                name.as_str().into(),
                "-o".into(),
                output.display().to_string(),
            ],
            staging_dir,
        )
        .await
        .map(|_| ())
    }
}

/// Whether a name is usable as a shell environment variable identifier.
fn is_env_identifier(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
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
