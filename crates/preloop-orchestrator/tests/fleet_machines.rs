//! The runner pool's sandbox-backed machine provider against a real service
//! and the process-backed fake driver.

use std::io::{Cursor, Read};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use preloop_sandbox::api::{Capabilities, CreateWork, ExecEvent, ExecRequest, Observed};
use preloop_sandbox::driver::Driver as SandboxDriver;
use preloop_sandbox::drivers::fake::FakeDriver;
use preloop_sandbox::drivers::smolvm::cli::{
    MachineName, MachineSpec, MachineState, NetworkPolicy, ProviderCapabilities, SecretSource,
    VmError, VmProvider,
};

use preloop_orchestrator::fleet::SandboxMachines;
use preloop_sandbox::agent::{Agent, AgentConfig, ProviderLink};
use preloop_sandbox::api::Requirements;
use preloop_sandbox::{Driver, InProcess, SandboxApi, Service, ServiceConfig, store};
use tokio_util::sync::CancellationToken;

/// A provider plus the service behind it, so tests can build a second
/// provider (an engine restart) over the same service.
struct Fixture {
    machines: Arc<SandboxMachines>,
    service: Arc<Service>,
    shutdown: CancellationToken,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

async fn fixture(dir: &tempfile::TempDir) -> Fixture {
    let fake = Arc::new(FakeDriver::new(
        "fake",
        dir.path().join("host"),
        FakeDriver::full_capabilities(),
    ));
    fixture_with_driver(dir, fake as Arc<dyn Driver>).await
}

async fn fixture_with_driver(dir: &tempfile::TempDir, driver: Arc<dyn Driver>) -> Fixture {
    let service = Service::open(
        store::open("memory:").await.unwrap(),
        ServiceConfig::new(dir.path().join("blobs")),
    )
    .await
    .unwrap();
    let shutdown = CancellationToken::new();
    let agent = Agent::new(
        Arc::new(service.clone()) as Arc<dyn ProviderLink>,
        vec![driver],
        AgentConfig::new("host", dir.path().join("scratch")),
    );
    tokio::spawn(agent.run(shutdown.clone()));
    tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
    while service.list_providers().is_empty() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let machines = SandboxMachines::new(
        Arc::new(InProcess(service.clone())) as Arc<dyn SandboxApi>,
        None,
        None,
        Requirements::default(),
        ProviderCapabilities::default(),
    );
    Fixture {
        machines,
        service,
        shutdown,
    }
}

struct RecordingDriver {
    inner: Arc<FakeDriver>,
    uploaded: Arc<tokio::sync::Mutex<Option<Vec<u8>>>>,
}

#[async_trait]
impl SandboxDriver for RecordingDriver {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn create(&self, work: &CreateWork) -> preloop_sandbox::Result<String> {
        self.inner.create(work).await
    }

    async fn destroy(&self, native_ref: &str) -> preloop_sandbox::Result<()> {
        self.inner.destroy(native_ref).await
    }

    async fn exec(
        &self,
        native_ref: &str,
        request: &ExecRequest,
        output: tokio::sync::mpsc::Sender<ExecEvent>,
    ) -> preloop_sandbox::Result<i32> {
        self.inner.exec(native_ref, request, output).await
    }

    async fn copy_in(
        &self,
        native_ref: &str,
        tar: &std::path::Path,
        guest_dir: &str,
    ) -> preloop_sandbox::Result<()> {
        *self.uploaded.lock().await = Some(std::fs::read(tar).unwrap());
        self.inner.copy_in(native_ref, tar, guest_dir).await
    }

    async fn copy_out(
        &self,
        native_ref: &str,
        guest_path: &str,
        tar: &std::path::Path,
    ) -> preloop_sandbox::Result<()> {
        self.inner.copy_out(native_ref, guest_path, tar).await
    }

    async fn snapshot(&self, native_ref: &str, template: &str) -> preloop_sandbox::Result<String> {
        self.inner.snapshot(native_ref, template).await
    }

    async fn drop_snapshot(&self, native_ref: &str) -> preloop_sandbox::Result<()> {
        self.inner.drop_snapshot(native_ref).await
    }

    async fn stop(&self, native_ref: &str) -> preloop_sandbox::Result<()> {
        self.inner.stop(native_ref).await
    }

    async fn start(&self, native_ref: &str, forkable: bool) -> preloop_sandbox::Result<()> {
        self.inner.start(native_ref, forkable).await
    }

    async fn pause(&self, native_ref: &str) -> preloop_sandbox::Result<()> {
        self.inner.pause(native_ref).await
    }

    async fn resume(&self, native_ref: &str) -> preloop_sandbox::Result<()> {
        self.inner.resume(native_ref).await
    }

    async fn inventory(&self) -> preloop_sandbox::Result<Vec<Observed>> {
        self.inner.inventory().await
    }
}

/// A provider that never created anything: what a restarted engine has.
fn stranger(service: &Arc<Service>) -> Arc<SandboxMachines> {
    SandboxMachines::new(
        Arc::new(InProcess(service.clone())) as Arc<dyn SandboxApi>,
        None,
        None,
        Requirements::default(),
        ProviderCapabilities::default(),
    )
}

fn spec(name: &str) -> MachineSpec {
    MachineSpec {
        name: MachineName::new(name).unwrap(),
        image: "alpine:3.20".into(),
        cpus: 1,
        memory_mib: 512,
        storage_gib: 1,
        overlay_gib: None,
        network: NetworkPolicy::Unrestricted,
        volumes: Vec::new(),
        sockets: Vec::new(),
        dns: None,
        rosetta: false,
        role: Default::default(),
        guest_volumes: None,
        workload_init: None,
    }
}

#[tokio::test]
async fn secret_env_upload_archives_secret_script_content() {
    let dir = tempfile::tempdir().unwrap();
    let uploaded = Arc::new(tokio::sync::Mutex::new(None));
    let inner = Arc::new(FakeDriver::new(
        "fake",
        dir.path().join("host"),
        FakeDriver::full_capabilities(),
    ));
    let recorder = Arc::new(RecordingDriver {
        inner,
        uploaded: uploaded.clone(),
    });
    let f = fixture_with_driver(&dir, recorder as Arc<dyn Driver>).await;
    let name = MachineName::new("pool-secret-0-1").unwrap();
    f.machines.create(&spec(name.as_str())).await.unwrap();
    f.machines.start(&name).await.unwrap();

    let secret_file = dir.path().join("secret");
    std::fs::write(&secret_file, "archive-secret\n").unwrap();
    let _ = f
        .machines
        .exec_with_secret_env(
            &name,
            &["true".into()],
            &[("TOKEN".into(), SecretSource::HostFile(secret_file))],
        )
        .await;

    let tar_bytes = uploaded
        .lock()
        .await
        .take()
        .expect("secret archive uploaded");
    let mut archive = tar::Archive::new(Cursor::new(tar_bytes));
    let mut entries = archive.entries().unwrap();
    let mut entry = entries.next().unwrap().unwrap();
    assert_eq!(entry.header().mode().unwrap() & 0o777, 0o600);
    let mut content = Vec::new();
    entry.read_to_end(&mut content).unwrap();
    assert!(
        content
            .windows(b"archive-secret".len())
            .any(|window| { window == b"archive-secret" })
    );
    assert!(
        content
            .windows(b"TOKEN=".len())
            .any(|window| window == b"TOKEN=")
    );
    drop(entry);
    assert!(entries.next().is_none());
}

#[tokio::test]
async fn non_zero_exit_is_an_error_like_the_cli_provider() {
    let dir = tempfile::tempdir().unwrap();
    let f = fixture(&dir).await;
    let name = MachineName::new("pool-runner-0-1").unwrap();
    f.machines.create(&spec(name.as_str())).await.unwrap();
    f.machines.start(&name).await.unwrap();
    let ok = f.machines.exec(&name, &["true".into()]).await.unwrap();
    assert_eq!(ok.exit_code, 0);
    // The pool's `test -f <marker>` probe must read "absent" as an error,
    // or every finished runner is held for debugging.
    match f
        .machines
        .exec(
            &name,
            &["sh".into(), "-c".into(), "echo nope >&2; exit 1".into()],
        )
        .await
    {
        Err(VmError::Command {
            exit_code, message, ..
        }) => {
            assert_eq!(exit_code, 1);
            assert_eq!(message, "nope");
        }
        other => panic!("expected a command error, got {other:?}"),
    }
    f.machines.delete(&name).await.unwrap();
    assert_eq!(
        f.machines.status(&name).await.unwrap(),
        MachineState::Missing
    );
}

#[tokio::test]
async fn concurrent_first_forks_capture_the_golden_once() {
    let dir = tempfile::tempdir().unwrap();
    let f = fixture(&dir).await;
    let golden = MachineName::new("pool-golden").unwrap();
    f.machines.create(&spec(golden.as_str())).await.unwrap();
    f.machines.start(&golden).await.unwrap();
    f.machines
        .exec(
            &golden,
            &[
                "sh".into(),
                "-c".into(),
                "echo baked > \"$SANDBOX_ROOT/marker\"".into(),
            ],
        )
        .await
        .unwrap();
    f.machines.start_forkable(&golden).await.unwrap();
    let clones: Vec<MachineName> = (0..4)
        .map(|i| MachineName::new(format!("pool-runner-{i}-1")).unwrap())
        .collect();
    let results =
        futures::future::join_all(clones.iter().map(|clone| f.machines.fork(&golden, clone))).await;
    for result in results {
        result.unwrap();
    }
    for clone in &clones {
        let out = f
            .machines
            .exec(clone, &["cat".into(), "marker".into()])
            .await
            .unwrap();
        assert_eq!(out.stdout, b"baked\n");
    }
    assert_eq!(
        f.machines.status(&golden).await.unwrap(),
        MachineState::Running
    );
}

#[tokio::test]
async fn a_restarted_adapter_adopts_a_snapshot_golden_and_forks() {
    let dir = tempfile::tempdir().unwrap();
    let f = fixture(&dir).await;
    let golden = MachineName::new("pool-restart-golden").unwrap();
    let first = MachineName::new("pool-restart-first").unwrap();
    let second = MachineName::new("pool-restart-second").unwrap();

    f.machines.create(&spec(golden.as_str())).await.unwrap();
    f.machines.start(&golden).await.unwrap();
    f.machines
        .exec(
            &golden,
            &[
                "sh".into(),
                "-c".into(),
                "echo baked > \"$SANDBOX_ROOT/marker\"".into(),
            ],
        )
        .await
        .unwrap();
    f.machines.start_forkable(&golden).await.unwrap();
    // The first fork consumes the live source sandbox and persists the
    // snapshot template that a replacement adapter must recover.
    f.machines.fork(&golden, &first).await.unwrap();

    let restarted = stranger(&f.service);
    assert_eq!(
        restarted.status(&golden).await.unwrap(),
        MachineState::Running
    );
    restarted.fork(&golden, &second).await.unwrap();

    let output = restarted
        .exec(&second, &["cat".into(), "marker".into()])
        .await
        .unwrap();
    assert_eq!(output.stdout, b"baked\n");
}

#[tokio::test]
async fn a_restart_adopts_machines_this_process_never_created() {
    let dir = tempfile::tempdir().unwrap();
    let f = fixture(&dir).await;
    let name = MachineName::new("pool-runner-9-1").unwrap();
    f.machines.create(&spec(name.as_str())).await.unwrap();
    f.machines.start(&name).await.unwrap();
    // The service is authoritative for machines this process never created.
    let restarted = stranger(&f.service);
    assert_eq!(
        restarted.status(&name).await.unwrap(),
        MachineState::Running
    );
    assert_eq!(
        restarted
            .exec(&name, &["true".into()])
            .await
            .unwrap()
            .exit_code,
        0
    );
    assert!(restarted.list().await.unwrap().contains(&name));
}
