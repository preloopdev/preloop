//! Typed SmolVM lifecycle primitives for Preloop CI.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::Stdio;
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

/// Explicit VM egress policy. Networking is disabled unless selected here.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum NetworkPolicy {
    /// No guest networking.
    #[default]
    Disabled,
    /// Unrestricted outbound networking.
    Unrestricted,
    /// Restrict outbound traffic to these host names and CIDRs.
    Restricted {
        /// DNS host names allowed for egress.
        hosts: Vec<String>,
        /// IP address ranges allowed for egress.
        cidrs: Vec<String>,
    },
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
    /// Guest network policy.
    pub network: NetworkPolicy,
    /// Narrowly scoped host mounts.
    pub volumes: Vec<VolumeMount>,
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
}

/// Provider contract consumed by the Preloop orchestrator.
#[async_trait]
pub trait VmProvider: Send + Sync {
    /// Create a persistent machine.
    async fn create(&self, spec: &MachineSpec) -> Result<(), VmError>;
    /// Start an existing machine.
    async fn start(&self, name: &MachineName) -> Result<(), VmError>;
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
    /// Execute with guest environment values resolved from named host variables.
    async fn exec_with_secret_env(
        &self,
        name: &MachineName,
        argv: &[String],
        secrets: &[(String, String)],
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

    async fn checked(
        &self,
        operation: &'static str,
        args: &[String],
    ) -> Result<ExecOutput, VmError> {
        let mut command = self.command();
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
        if spec.image.ends_with(".smolmachine") {
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
        match &spec.network {
            NetworkPolicy::Disabled => {}
            NetworkPolicy::Unrestricted => args.push("--net".into()),
            NetworkPolicy::Restricted { hosts, cidrs } => {
                for host in hosts {
                    args.extend(["--allow-host".into(), host.clone()]);
                }
                for cidr in cidrs {
                    args.extend(["--allow-cidr".into(), cidr.clone()]);
                }
            }
        }
        for mount in &spec.volumes {
            let mut value = format!("{}:{}", mount.host.display(), mount.guest.display());
            if mount.read_only {
                value.push_str(":ro");
            }
            args.extend(["--volume".into(), value]);
        }
        self.checked("create", &args).await.map(|_| ())
    }

    async fn start(&self, name: &MachineName) -> Result<(), VmError> {
        self.checked(
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

    async fn stop(&self, name: &MachineName) -> Result<(), VmError> {
        self.checked(
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
        self.checked(
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
        match self.checked("status", &args).await {
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
            .checked("list", &["machine".into(), "ls".into(), "--json".into()])
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
        secrets: &[(String, String)],
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
        for (guest, host) in secrets {
            if guest.is_empty()
                || host.is_empty()
                || !guest
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                || !host
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            {
                return Err(VmError::InvalidSpec(
                    "secret environment names must be non-empty ASCII identifiers".into(),
                ));
            }
            args.extend(["--secret-env".into(), format!("{guest}={host}")]);
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
        self.checked(
            "pack",
            &[
                "pack".into(),
                "create".into(),
                "--from-vm".into(),
                name.as_str().into(),
                "-o".into(),
                output.display().to_string(),
            ],
        )
        .await
        .map(|_| ())
    }
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
    Ok(())
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
