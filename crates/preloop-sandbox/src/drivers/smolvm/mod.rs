//! SmolVM driver, over the `smolvm` CLI.
//!
//! Golden-fork model: a build sandbox starts `--branchable`; `Snapshot`
//! keeps that machine running as the fork base and returns its name, and
//! every sandbox created from the snapshot template is a CoW branch of it.
//! Pools are the service's generic warm pool (smolvm's native pools live
//! only in `smolvm serve`, whose clones lose `--mount-socket` relays).

pub mod cli;

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use self::cli::{
    MachineName, MachineSpec, NetworkPolicy as VmNetwork, OutputChunk, SmolVmProvider,
    SocketMount as VmSocket, VmError, VmProvider, VolumeMount,
};
use crate::api::*;
use crate::connect::{Code, Error, Result};
use crate::driver::{
    Driver, EXEC_CANCELLED, EXEC_TIMED_OUT, ExecCancels, NATIVE_PREFIX, native_name,
    parse_native_name, shell_quote,
};

/// Driver name.
pub const NAME: &str = "smolvm";

/// Template recipe prefix for a packed `.smolmachine` artifact on this host.
pub const SMOLMACHINE_RECIPE: &str = "smolmachine:";

/// SmolVM driver.
pub struct SmolvmDriver {
    provider: SmolVmProvider,
    slots: u32,
    named_prefix: Option<String>,
    cancels: ExecCancels,
}

impl SmolvmDriver {
    /// Driver over `provider`; `slots` caps concurrent sandboxes (0 = none).
    pub fn new(provider: SmolVmProvider, slots: u32) -> Self {
        Self {
            provider,
            slots,
            named_prefix: None,
            cancels: ExecCancels::default(),
        }
    }

    /// Also report machines with this name prefix (caller-named sandboxes,
    /// e.g. the CI pool's `preloop-…` runners) in the inventory.
    pub fn with_named_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.named_prefix = Some(prefix.into());
        self
    }

    /// The typed lifecycle primitive behind this driver. Callers that also
    /// need host-side operations must clone this provider rather than build a
    /// second one, so both share the lifecycle lock and fork census.
    pub fn provider(&self) -> &SmolVmProvider {
        &self.provider
    }

    fn adopts(&self, name: &str) -> bool {
        self.named_prefix
            .as_deref()
            .is_some_and(|prefix| name.starts_with(prefix))
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
    match std::env::consts::ARCH {
        "aarch64" | "arm64" => "aarch64",
        other => other,
    }
}

fn vm_error(error: VmError) -> Error {
    let code = match &error {
        VmError::InvalidMachineName(_) | VmError::InvalidSpec(_) => Code::InvalidArgument,
        VmError::UnsupportedSocketMount { .. } | VmError::InvalidSandboxEnv { .. } => {
            Code::FailedPrecondition
        }
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
        serde_json::from_str(raw)
            .map_err(|error| Error::invalid(format!("smolvm: invalid `{key}` option: {error}")))
    })
    .transpose()
}

fn network(policy: &NetworkPolicy) -> VmNetwork {
    match policy.mode {
        NetworkMode::Disabled => VmNetwork::Disabled,
        NetworkMode::PublicOnly => VmNetwork::PublicOnly,
        NetworkMode::Restricted => VmNetwork::Restricted {
            hosts: policy.hosts.clone(),
            cidrs: policy.cidrs.clone(),
        },
        NetworkMode::Unrestricted | NetworkMode::NetworkModeUnspecified => VmNetwork::Unrestricted,
    }
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
                NetworkMode::Disabled,
                NetworkMode::Unrestricted,
                NetworkMode::PublicOnly,
                NetworkMode::Restricted,
            ],
            slots: self.slots,
            native_pool: false,
            one_shot: false,
        }
    }

    async fn create(&self, work: &CreateWork) -> Result<String> {
        // A caller-chosen name doubles as the native name, so host tools
        // (`preloop shell`, `smolvm machine exec`) address the same machine.
        let name = if work.name.is_empty() {
            native_name(&work.sandbox_id, work.incarnation)
        } else {
            work.name.clone()
        };
        let clone = machine(&name)?;
        if !work.template.native_ref.is_empty() {
            let golden = machine(&work.template.native_ref)?;
            match self.provider.fork(&golden, &clone).await {
                Ok(()) => return Ok(name),
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
                            .fork(&golden, &clone)
                            .await
                            .map_err(vm_error)?;
                        return Ok(name);
                    }
                    return Err(vm_error(error));
                }
            }
        }
        let image = match (
            &work.template.spec.recipe,
            work.template.spec.image_for(guest_arch()),
        ) {
            (Some(recipe), _) => recipe
                .strip_prefix(SMOLMACHINE_RECIPE)
                .map(str::to_owned)
                .ok_or_else(|| Error::invalid(format!("smolvm: unknown recipe `{recipe}`")))?,
            (None, Some(image)) => image.to_owned(),
            (None, None) => {
                return Err(Error::invalid(format!(
                    "template `{}` has no image for {}",
                    work.template.spec.name,
                    guest_arch()
                )));
            }
        };
        let option = |key: &str| work.options.get(key).map(String::as_str);
        let spec = MachineSpec {
            name: clone.clone(),
            image,
            cpus: if work.shape.cpus == 0 {
                2
            } else {
                work.shape.cpus as u16
            },
            memory_mib: if work.shape.memory_mib == 0 {
                4096
            } else {
                work.shape.memory_mib
            },
            storage_gib: if work.shape.disk_gib == 0 {
                20
            } else {
                work.shape.disk_gib
            },
            overlay_gib: option("overlay_gib").and_then(|v| v.parse().ok()),
            network: network(&work.network),
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
        let started = if work.snapshot_capable {
            self.provider.start_forkable(&clone).await
        } else {
            self.provider.start(&clone).await
        };
        if let Err(error) = started {
            let _ = self.provider.delete(&clone).await;
            return Err(vm_error(error));
        }
        Ok(name)
    }

    async fn destroy(&self, native_ref: &str) -> Result<()> {
        let name = machine(native_ref)?;
        match self.provider.delete(&name).await {
            Ok(()) => Ok(()),
            Err(error) if error.to_string().contains("not found") => Ok(()),
            Err(error) => Err(vm_error(error)),
        }
    }

    async fn exec(
        &self,
        native_ref: &str,
        exec_id: &str,
        request: &ExecRequest,
        output: mpsc::Sender<ExecEvent>,
    ) -> Result<i32> {
        if request.argv.is_empty() {
            return Err(Error::invalid("argv is required"));
        }
        let name = machine(native_ref)?;
        let argv = crate::driver::posix_exec_argv(request)?;
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
                return Err(Error::precondition(format!(
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
                let (sandbox_id, incarnation) =
                    parse_native_name(name.as_str()).unwrap_or_default();
                Observed {
                    driver: NAME.into(),
                    native_ref: name.as_str().to_owned(),
                    sandbox_id,
                    incarnation,
                }
            })
            .collect())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
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
