//! A process-backed driver for tests and conformance: each sandbox is a
//! host directory; commands run as host processes inside it. No isolation.
//! Guest absolute paths map under the sandbox root (`/x` → `<root>/x`), and
//! commands receive the root as `SANDBOX_ROOT`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use tokio::io::AsyncReadExt as _;
use tokio::sync::mpsc;

use crate::api::*;
use crate::connect::{Error, Result};
use crate::driver::{Driver, native_name, parse_native_name};

/// Fake driver.
pub struct FakeDriver {
    name: String,
    root: PathBuf,
    capabilities: Capabilities,
    /// Fail the next N creates (tests).
    pub fail_creates: AtomicU32,
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
                NetworkMode::Disabled,
                NetworkMode::Unrestricted,
                NetworkMode::PublicOnly,
            ],
            slots: 0,
            native_pool: false,
            one_shot: false,
            max_vcpus: 0,
            max_memory_mib: 0,
        }
    }

    fn dir(&self, native_ref: &str) -> Result<PathBuf> {
        if native_ref.contains('/') || native_ref.contains("..") || native_ref.is_empty() {
            return Err(Error::invalid(format!("bad native ref `{native_ref}`")));
        }
        Ok(self.root.join("sandboxes").join(native_ref))
    }

    fn guest(root: &Path, guest: &str) -> PathBuf {
        root.join(guest.trim_start_matches('/'))
    }
}

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
        let name = native_name(&work.sandbox_id, work.incarnation);
        let dir = self.dir(&name)?;
        std::fs::create_dir_all(&dir).map_err(io("create"))?;
        if !work.template.native_ref.is_empty() {
            let source = self.root.join("snapshots").join(&work.template.native_ref);
            copy_tree(&source, &dir).map_err(io("restore"))?;
        }
        Ok(name)
    }

    async fn destroy(&self, native_ref: &str) -> Result<()> {
        let dir = self.dir(native_ref)?;
        match std::fs::remove_dir_all(dir) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(io("destroy")(error)),
            _ => Ok(()),
        }
    }

    async fn exec(
        &self,
        native_ref: &str,
        request: &ExecRequest,
        output: mpsc::Sender<ExecEvent>,
    ) -> Result<i32> {
        let root = self.dir(native_ref)?;
        if !root.is_dir() {
            return Err(Error::not_found("sandbox is gone"));
        }
        if root.join(".paused").exists() {
            return Err(Error::precondition("sandbox is paused"));
        }
        if root.join(".stopped").exists() {
            return Err(Error::precondition("sandbox is stopped"));
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
        let status = if request.timeout_secs > 0 {
            match tokio::time::timeout(
                std::time::Duration::from_secs(request.timeout_secs),
                child.wait(),
            )
            .await
            {
                Ok(status) => status.map_err(io("wait"))?,
                Err(_) => {
                    let _ = child.kill().await;
                    return Err(Error::new(
                        crate::connect::Code::DeadlineExceeded,
                        "exec timed out",
                    ));
                }
            }
        } else {
            child.wait().await.map_err(io("wait"))?
        };
        let _ = stdout.await;
        let _ = stderr.await;
        Ok(status.code().unwrap_or(-1))
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
            let (sandbox_id, incarnation) = parse_native_name(&name).unwrap_or_default();
            out.push(Observed {
                driver: self.name.clone(),
                native_ref: name,
                sandbox_id,
                incarnation,
            });
        }
        Ok(out)
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
