//! A process-backed driver for tests and conformance: each sandbox is a
//! host directory; commands run as host processes inside it. No isolation.
//! Guest absolute paths map under the sandbox root (`/x` → `<root>/x`), and
//! commands receive the root as `SANDBOX_ROOT`.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::io::AsyncReadExt as _;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::api::*;
use crate::driver::{
    Driver, EXEC_CANCELLED, EXEC_TIMED_OUT, ExecCancels, native_name, parse_native_name,
};
use crate::error::{Error, Result};

/// Fake driver.
pub struct FakeDriver {
    name: String,
    root: PathBuf,
    capabilities: Capabilities,
    /// Fail the next N creates (tests).
    pub fail_creates: AtomicU32,
    /// How many creates actually ran (redelivery dedupe tests).
    pub create_calls: AtomicU32,
    /// How many `cancel_exec` calls the agent delivered (disconnect tests).
    pub cancel_calls: AtomicU32,
    cancels: ExecCancels,
    /// Exec ids currently running, per sandbox native ref, so `destroy`
    /// can stop a sandbox's commands instead of orphaning them.
    running: Mutex<HashMap<String, HashSet<String>>>,
}

impl FakeDriver {
    /// A driver keeping sandboxes under `root`.
    pub fn new(name: impl Into<String>, root: PathBuf, capabilities: Capabilities) -> Self {
        std::fs::create_dir_all(root.join("sandboxes")).ok();
        std::fs::create_dir_all(root.join("snapshots")).ok();
        Self {
            name: name.into(),
            root,
            capabilities,
            fail_creates: AtomicU32::new(0),
            create_calls: AtomicU32::new(0),
            cancel_calls: AtomicU32::new(0),
            cancels: ExecCancels::default(),
            running: Mutex::new(HashMap::new()),
        }
    }

    /// Linux-like capabilities with every optional operation.
    pub fn full_capabilities() -> Capabilities {
        Capabilities {
            os: "linux".into(),
            arch: vec![std::env::consts::ARCH.into()],
            features: vec!["docker".into()],
            pause: true,
            snapshot: true,
            fork: false,
            socket_mounts: true,
            host_mounts: true,
            network_modes: vec![
                NetworkMode::Disabled.into(),
                NetworkMode::Unrestricted.into(),
                NetworkMode::PublicOnly.into(),
            ],
            slots: 0,
            native_pool: false,
            one_shot: false,
            max_vcpus: 0,
            max_memory_mib: 0,
            default_shape: buffa::MessageField::some(Shape {
                cpus: 2,
                memory_mib: 1024,
                disk_gib: 10,
                ..Shape::default()
            }),
            ..Capabilities::default()
        }
    }

    fn dir(&self, native_ref: &str) -> Result<PathBuf> {
        if native_ref.contains('/') || native_ref.contains("..") || native_ref.is_empty() {
            return Err(Error::invalid_argument(format!(
                "bad native ref `{native_ref}`"
            )));
        }
        Ok(self.root.join("sandboxes").join(native_ref))
    }

    fn guest(root: &Path, guest: &str) -> PathBuf {
        root.join(guest.trim_start_matches('/'))
    }

    /// The command half of [`Driver::exec`], split out so the cancel registry
    /// is cleaned up on every path.
    async fn exec_inner(
        &self,
        native_ref: &str,
        request: &ExecRequest,
        output: mpsc::Sender<ExecEvent>,
        cancel: CancellationToken,
    ) -> Result<i32> {
        if cancel.is_cancelled() {
            return Ok(EXEC_CANCELLED);
        }
        let root = self.dir(native_ref)?;
        if !root.is_dir() {
            return Err(Error::not_found("sandbox is gone"));
        }
        if root.join(".paused").exists() {
            return Err(Error::failed_precondition("sandbox is paused"));
        }
        if root.join(".stopped").exists() {
            return Err(Error::failed_precondition("sandbox is stopped"));
        }
        let mut command = tokio::process::Command::new(&request.argv[0]);
        command
            .args(&request.argv[1..])
            .envs(&request.env)
            .env("SANDBOX_ROOT", &root)
            .current_dir(if request.workdir.is_empty() {
                root.clone()
            } else {
                Self::guest(&root, &request.workdir)
            })
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        // The command leads its own process group, so a cancel, a timeout
        // or a destroy can take down the whole tree: a shell's children
        // hold the output pipes and outlive the shell otherwise.
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command.spawn().map_err(io("spawn"))?;
        if let Some(mut stdin) = child.stdin.take() {
            use tokio::io::AsyncWriteExt as _;
            let data = request.stdin.clone();
            tokio::spawn(async move {
                let _ = stdin.write_all(&data).await;
            });
        }
        let pump = |mut reader: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
                    out: mpsc::Sender<ExecEvent>,
                    stderr: bool| async move {
            let mut buffer = vec![0u8; 16 * 1024];
            loop {
                match reader.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => {
                        let chunk = buffer[..n].to_vec();
                        let event = if stderr {
                            ExecEvent::stderr(chunk)
                        } else {
                            ExecEvent::stdout(chunk)
                        };
                        if out.send(event).await.is_err() {
                            return;
                        }
                    }
                }
            }
        };
        let stdout = tokio::spawn(pump(
            Box::new(child.stdout.take().expect("piped")),
            output.clone(),
            false,
        ));
        let stderr = tokio::spawn(pump(
            Box::new(child.stderr.take().expect("piped")),
            output,
            true,
        ));
        let end = {
            let wait = async {
                if request.timeout_secs == 0 {
                    return child.wait().await.map(WaitEnd::Status).map_err(io("wait"));
                }
                match tokio::time::timeout(
                    std::time::Duration::from_secs(request.timeout_secs),
                    child.wait(),
                )
                .await
                {
                    Ok(status) => status.map(WaitEnd::Status).map_err(io("wait")),
                    Err(_) => Ok(WaitEnd::TimedOut),
                }
            };
            tokio::select! {
                end = wait => end?,
                _ = cancel.cancelled() => WaitEnd::Cancelled,
            }
        };
        // Dropping the wait on cancel (or on the deadline) leaves the child
        // running; kill it before returning so nothing outlives the exec.
        // The whole group goes, not just the shell: its children inherited
        // the output pipes and would hold this exec (and, after a destroy,
        // the host) open until they exit on their own.
        if !matches!(end, WaitEnd::Status(_)) {
            #[cfg(unix)]
            if let Some(pid) = child.id() {
                kill_process_group(pid);
            }
            let _ = child.kill().await;
        }
        let _ = stdout.await;
        let _ = stderr.await;
        Ok(match end {
            WaitEnd::Status(status) => status.code().unwrap_or(-1),
            WaitEnd::TimedOut => EXEC_TIMED_OUT,
            WaitEnd::Cancelled => EXEC_CANCELLED,
        })
    }
}

enum WaitEnd {
    Status(std::process::ExitStatus),
    TimedOut,
    Cancelled,
}

/// SIGKILLs the process group led by `pid`.
///
/// SAFETY: the caller passes the pid of a child this driver just spawned
/// and made a group leader (see `exec_inner`), so the group id is the pid
/// and every member of the group is part of that exec's tree.
#[cfg(unix)]
#[allow(unsafe_code)]
fn kill_process_group(pid: u32) {
    unsafe {
        libc::killpg(pid as libc::pid_t, libc::SIGKILL);
    }
}

/// How long `destroy` waits for a sandbox's cancelled execs to die before
/// removing the directory anyway.
const DESTROY_KILL_WAIT: Duration = Duration::from_secs(10);

fn io(context: &str) -> impl Fn(std::io::Error) -> Error + '_ {
    move |error| Error::internal(format!("fake {context}: {error}"))
}

#[async_trait]
impl Driver for FakeDriver {
    fn name(&self) -> &str {
        &self.name
    }

    fn capabilities(&self) -> Capabilities {
        self.capabilities.clone()
    }

    async fn create(&self, work: &CreateWork) -> Result<String> {
        if self
            .fail_creates
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(Error::unavailable("injected create failure"));
        }
        self.create_calls.fetch_add(1, Ordering::AcqRel);
        let name = native_name(&work.sandbox_id);
        let dir = self.dir(&name)?;
        std::fs::create_dir_all(&dir).map_err(io("create"))?;
        if !work.template.native_ref.is_empty() {
            let source = self.root.join("snapshots").join(&work.template.native_ref);
            copy_tree(&source, &dir).map_err(io("restore"))?;
        }
        Ok(name)
    }

    async fn destroy(&self, native_ref: &str) -> Result<()> {
        // Stop the sandbox's commands before removing anything: destroy
        // used to only delete the directory, leaving the exec processes
        // running on the host, orphaned, with their working directory
        // deleted under them. Cancellation is delivered to the exec task,
        // which kills the command's process group; wait for it to land so
        // "destroyed" means the processes are gone — bounded, so one stuck
        // exec cannot wedge the destroy (and the pool slot) forever.
        let running: Vec<String> = self
            .running
            .lock()
            .get(native_ref)
            .map(|ids| ids.iter().cloned().collect())
            .unwrap_or_default();
        for exec_id in &running {
            self.cancels.cancel(exec_id);
        }
        let deadline = std::time::Instant::now() + DESTROY_KILL_WAIT;
        while std::time::Instant::now() < deadline
            && self
                .running
                .lock()
                .get(native_ref)
                .is_some_and(|ids| !ids.is_empty())
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let dir = self.dir(native_ref)?;
        match std::fs::remove_dir_all(dir) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(io("destroy")(error)),
            _ => Ok(()),
        }
    }

    async fn exec(
        &self,
        native_ref: &str,
        exec_id: &str,
        request: &ExecRequest,
        output: mpsc::Sender<ExecEvent>,
    ) -> Result<i32> {
        let cancel = self.cancels.start(exec_id);
        self.running
            .lock()
            .entry(native_ref.to_owned())
            .or_default()
            .insert(exec_id.to_owned());
        let result = self.exec_inner(native_ref, request, output, cancel).await;
        self.cancels.finish(exec_id);
        {
            let mut running = self.running.lock();
            if let Some(ids) = running.get_mut(native_ref) {
                ids.remove(exec_id);
                if ids.is_empty() {
                    running.remove(native_ref);
                }
            }
        }
        result
    }

    async fn cancel_exec(&self, _native_ref: &str, exec_id: &str) -> Result<()> {
        self.cancel_calls.fetch_add(1, Ordering::AcqRel);
        self.cancels.cancel(exec_id);
        Ok(())
    }

    async fn copy_in(&self, native_ref: &str, tar: &Path, guest_dir: &str) -> Result<()> {
        let root = self.dir(native_ref)?;
        let dest = Self::guest(&root, guest_dir);
        std::fs::create_dir_all(&dest).map_err(io("copy_in"))?;
        let file = std::fs::File::open(tar).map_err(io("copy_in"))?;
        tar::Archive::new(file).unpack(&dest).map_err(io("copy_in"))
    }

    async fn copy_out(&self, native_ref: &str, guest_path: &str, tar: &Path) -> Result<()> {
        let root = self.dir(native_ref)?;
        let source = Self::guest(&root, guest_path);
        let file = std::fs::File::create(tar).map_err(io("copy_out"))?;
        let mut builder = tar::Builder::new(file);
        let name = source
            .file_name()
            .map(|n| n.to_owned())
            .unwrap_or_else(|| ".".into());
        if source.is_dir() {
            builder
                .append_dir_all(&name, &source)
                .map_err(io("copy_out"))?;
        } else {
            builder
                .append_path_with_name(&source, &name)
                .map_err(io("copy_out"))?;
        }
        builder.finish().map_err(io("copy_out"))
    }

    async fn snapshot(&self, native_ref: &str, template: &str) -> Result<String> {
        let source = self.dir(native_ref)?;
        let snapshot = format!(
            "snap-{}-{}",
            template.replace(['/', ':'], "_"),
            uuid::Uuid::new_v4().simple()
        );
        let dest = self.root.join("snapshots").join(&snapshot);
        std::fs::rename(&source, &dest).map_err(io("snapshot"))?;
        Ok(snapshot)
    }

    async fn drop_snapshot(&self, native_ref: &str) -> Result<()> {
        let _ = std::fs::remove_dir_all(self.root.join("snapshots").join(native_ref));
        Ok(())
    }

    async fn stop(&self, native_ref: &str) -> Result<()> {
        std::fs::write(self.dir(native_ref)?.join(".stopped"), b"").map_err(io("stop"))
    }

    async fn start(&self, native_ref: &str, _forkable: bool) -> Result<()> {
        let _ = std::fs::remove_file(self.dir(native_ref)?.join(".stopped"));
        Ok(())
    }

    async fn pause(&self, native_ref: &str) -> Result<()> {
        std::fs::write(self.dir(native_ref)?.join(".paused"), b"").map_err(io("pause"))
    }

    async fn resume(&self, native_ref: &str) -> Result<()> {
        let _ = std::fs::remove_file(self.dir(native_ref)?.join(".paused"));
        Ok(())
    }

    async fn inventory(&self) -> Result<Vec<Observed>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(self.root.join("sandboxes")).map_err(io("inventory"))? {
            let name = entry
                .map_err(io("inventory"))?
                .file_name()
                .to_string_lossy()
                .into_owned();
            let sandbox_id = parse_native_name(&name).unwrap_or_default();
            out.push(Observed {
                driver: self.name.clone(),
                native_ref: name,
                sandbox_id,
                ..Observed::default()
            });
        }
        Ok(out)
    }

    async fn host_report(&self) -> HostReport {
        let host = preloop_observability::vm_telemetry::sample_host().into();
        let mut sandboxes = Vec::new();
        let Ok(entries) = std::fs::read_dir(self.root.join("sandboxes")) else {
            return HostReport {
                host: buffa::MessageField::some(host),
                sandboxes,
                ..HostReport::default()
            };
        };
        for entry in entries.flatten() {
            let native_ref = entry.file_name().to_string_lossy().into_owned();
            let start_time = entry
                .metadata()
                .ok()
                .and_then(|meta| meta.created().ok())
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs() as i64);
            sandboxes.push(SandboxRuntimeStats {
                native_ref,
                memory_bytes: None,
                start_time,
                ..SandboxRuntimeStats::default()
            });
        }
        HostReport {
            host: buffa::MessageField::some(host),
            sandboxes,
            ..HostReport::default()
        }
    }
}

fn copy_tree(from: &Path, to: &Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            std::fs::create_dir_all(&target)?;
            copy_tree(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// True while `pid` names a live (not zombie) process.
    #[cfg(unix)]
    fn running(pid: u32) -> bool {
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            return false;
        };
        // The state letter follows the parenthesised command name.
        stat.rsplit(')')
            .next()
            .and_then(|rest| rest.trim_start().chars().next())
            .is_some_and(|state| state != 'Z' && state != 'X')
    }

    async fn wait_until(mut condition: impl FnMut() -> bool, what: &str) {
        for _ in 0..200 {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("timed out waiting for {what}");
    }

    /// Pentest W4-F7: `destroy` used to only delete the directory,
    /// leaving the sandbox's exec processes running on the host under a
    /// native ref the service had already forgotten. Destroy now
    /// cancels the sandbox's execs and waits for their process groups —
    /// children included — to die before removing the directory.
    #[cfg(unix)]
    #[tokio::test]
    async fn destroy_kills_the_sandboxes_running_processes() {
        let dir = tempfile::tempdir().unwrap();
        let driver = Arc::new(FakeDriver::new(
            "fake",
            dir.path().join("host"),
            FakeDriver::full_capabilities(),
        ));
        let native = driver
            .create(&CreateWork {
                sandbox_id: "sbx-destroy".into(),
                driver: "fake".into(),
                ..CreateWork::default()
            })
            .await
            .unwrap();
        // The pid files live outside the sandbox root, which destroy
        // deletes: the shell records itself and a background child.
        let child_pid_file = dir.path().join("child.pid");
        let shell_pid_file = dir.path().join("shell.pid");
        let script = format!(
            "sleep 60 & echo $! > {}; echo $$ > {}; wait",
            child_pid_file.display(),
            shell_pid_file.display()
        );
        let (tx, _rx) = mpsc::channel(64);
        let exec_driver = driver.clone();
        let exec_native = native.clone();
        let exec = tokio::spawn(async move {
            exec_driver
                .exec(
                    &exec_native,
                    "exe-destroy",
                    &ExecRequest {
                        argv: vec!["sh".into(), "-c".into(), script],
                        ..ExecRequest::default()
                    },
                    tx,
                )
                .await
        });
        let read_pid = |path: &Path| {
            std::fs::read_to_string(path)
                .ok()
                .and_then(|text| text.trim().parse::<u32>().ok())
        };
        wait_until(
            || read_pid(&child_pid_file).is_some() && read_pid(&shell_pid_file).is_some(),
            "the exec to start",
        )
        .await;
        let child_pid = read_pid(&child_pid_file).unwrap();
        let shell_pid = read_pid(&shell_pid_file).unwrap();
        assert!(running(child_pid) && running(shell_pid));

        driver.destroy(&native).await.unwrap();

        assert!(!running(shell_pid), "the shell survived destroy");
        assert!(!running(child_pid), "the shell's child survived destroy");
        tokio::time::timeout(Duration::from_secs(5), exec)
            .await
            .expect("the exec task outlived destroy")
            .unwrap()
            .unwrap();
        assert!(!driver.dir(&native).unwrap().exists());
    }
}
