#![cfg(unix)]
//! End-to-end tests for the out-of-process driver protocol.
//!
//! The plugin side is `driver_plugin::serve`, an in-process [`Driver`] exposed
//! over a Unix socket; the agent side is [`ExternalDriver`]. The tests cover
//! socket mode (conformance plus a real service/agent acquire → exec →
//! release), a driver that declares no optional operations (which must refuse
//! them with `failed_precondition`), and spawn mode.
//!
//! Spawn mode uses this test binary as the plugin: the wrapper script sets
//! `PRELOOP_TEST_DRIVER_PLUGIN=1` and re-executes the binary with
//! `--exact plugin_child_process --`, so the child test serves a
//! [`FakeDriver`] on the `--socket` path the agent appends. `CARGO_BIN_EXE_*`
//! does not cover examples, and this avoids a nested `cargo build` from a
//! test.

mod common;

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use preloop_sandbox::Result as DriverResult;
use preloop_sandbox::agent::{Agent, AgentConfig};
use preloop_sandbox::api::*;
use preloop_sandbox::driver_conformance::{self, CaseResult, ConformanceOptions};
use preloop_sandbox::driver_plugin;
use preloop_sandbox::drivers::external::{ExternalDriver, ExternalSpec};
use preloop_sandbox::drivers::fake::FakeDriver;
use preloop_sandbox::{Driver, SandboxApi, Service};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Set for the re-executed test binary that serves the spawn-mode plugin.
const PLUGIN_CHILD_ENV: &str = "PRELOOP_TEST_DRIVER_PLUGIN";
/// The socket path the wrapper receives from the agent's `--socket` flag.
const PLUGIN_SOCKET_ENV: &str = "PRELOOP_TEST_DRIVER_SOCKET";

/// Every case the suite must report.
const CONFORMANCE_CASES: [&str; 12] = [
    "capabilities",
    "create_inventory",
    "create_idempotent",
    "destroy_idempotent",
    "exec_streams",
    "exec_timeout",
    "cancel_exec",
    "copy_round_trip",
    "snapshot",
    "stop_start",
    "pause_resume",
    "host_report",
];

/// A plugin served on a temporary socket.
struct Served {
    socket: PathBuf,
    dir: tempfile::TempDir,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Served {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}

/// Serve `make`'s driver on `<dir>/<name>.sock` until the test ends.
async fn serve(name: &str, make: impl FnOnce(&Path) -> Arc<dyn Driver>) -> Served {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join(format!("{name}.sock"));
    let driver = make(dir.path());
    let shutdown = CancellationToken::new();
    let token = shutdown.clone();
    let path = socket.clone();
    let task = tokio::spawn(async move {
        if let Err(error) = driver_plugin::serve(driver, &path, token).await {
            eprintln!("driver plugin stopped: {error:#}");
        }
    });
    Served {
        socket,
        dir,
        shutdown,
        task,
    }
}

/// Dial a served plugin through the agent's driver shim.
async fn connect(served: &Served, name: &str) -> Arc<dyn Driver> {
    let spec = ExternalSpec::parse(&format!("{name}=unix://{}", served.socket.display())).unwrap();
    ExternalDriver::start(spec, &served.dir.path().join("runtime"))
        .await
        .unwrap()
}

fn assert_conformance(results: &[CaseResult]) {
    let failures: Vec<String> = results
        .iter()
        .filter(|result| !result.passed() && !result.skipped)
        .map(|result| {
            format!(
                "{}: {}",
                result.name,
                result
                    .outcome
                    .as_ref()
                    .err()
                    .map(String::as_str)
                    .unwrap_or("failed")
            )
        })
        .collect();
    assert!(
        failures.is_empty(),
        "driver conformance failed:\n{}",
        failures.join("\n")
    );
    for name in CONFORMANCE_CASES {
        assert!(
            results.iter().any(|result| result.name == name),
            "conformance did not report `{name}`"
        );
    }
    assert!(
        results.iter().all(|result| !result.skipped),
        "no conformance case should be skipped here: {:?}",
        results
            .iter()
            .filter(|result| result.skipped)
            .map(|result| result.name)
            .collect::<Vec<_>>()
    );
}

async fn wait_for(mut condition: impl FnMut() -> bool) {
    for _ in 0..400 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("condition not reached");
}

fn conformance_options() -> ConformanceOptions {
    ConformanceOptions {
        // Keep the deadline case quick while still proving the driver kills
        // the command and reports 124.
        exec_timeout: Duration::from_secs(1),
        ..ConformanceOptions::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn socket_driver_passes_conformance() {
    let served = serve("fake", |dir| {
        Arc::new(FakeDriver::new(
            "fake",
            dir.join("host"),
            FakeDriver::full_capabilities(),
        ))
    })
    .await;
    let driver = connect(&served, "fake").await;

    let results = driver_conformance::run(Arc::clone(&driver), &conformance_options()).await;
    assert_conformance(&results);

    // Every case cleans up its own sandbox; nothing may be left for the
    // agent's reconcile to destroy.
    assert!(
        driver.inventory().await.unwrap().is_empty(),
        "conformance left sandboxes behind"
    );
}

/// A driver that declares only the mandatory operations. The trait's default
/// `snapshot`/`stop`/`start`/`pause`/`resume` return `failed_precondition`,
/// which the protocol must carry back unchanged.
struct LimitedDriver(FakeDriver);

#[async_trait]
impl Driver for LimitedDriver {
    fn name(&self) -> &str {
        "limited"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            os: "linux".into(),
            arch: vec![preloop_sandbox::api::host_arch().into()],
            ..Capabilities::default()
        }
    }

    async fn create(&self, work: &CreateWork) -> DriverResult<String> {
        self.0.create(work).await
    }

    async fn destroy(&self, native_ref: &str) -> DriverResult<()> {
        self.0.destroy(native_ref).await
    }

    async fn exec(
        &self,
        native_ref: &str,
        exec_id: &str,
        request: &ExecRequest,
        output: mpsc::Sender<ExecEvent>,
    ) -> DriverResult<i32> {
        self.0.exec(native_ref, exec_id, request, output).await
    }

    async fn cancel_exec(&self, native_ref: &str, exec_id: &str) -> DriverResult<()> {
        self.0.cancel_exec(native_ref, exec_id).await
    }

    async fn copy_in(&self, native_ref: &str, tar: &Path, guest_dir: &str) -> DriverResult<()> {
        self.0.copy_in(native_ref, tar, guest_dir).await
    }

    async fn copy_out(&self, native_ref: &str, guest_path: &str, tar: &Path) -> DriverResult<()> {
        self.0.copy_out(native_ref, guest_path, tar).await
    }

    async fn inventory(&self) -> DriverResult<Vec<Observed>> {
        self.0.inventory().await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn undeclared_optional_operations_are_refused_over_the_wire() {
    let served = serve("limited", |dir| {
        Arc::new(LimitedDriver(FakeDriver::new(
            "limited",
            dir.join("host"),
            Capabilities::default(),
        )))
    })
    .await;
    let driver = connect(&served, "limited").await;

    let results = driver_conformance::run(Arc::clone(&driver), &conformance_options()).await;
    assert_conformance(&results);
    for name in ["snapshot", "stop_start", "pause_resume"] {
        let result = results
            .iter()
            .find(|result| result.name == name)
            .expect("case present");
        assert!(result.passed(), "{name}: {:?}", result.outcome);
    }
    assert!(driver.inventory().await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn external_driver_carries_the_agent_flow() {
    let served = serve("external", |dir| {
        Arc::new(FakeDriver::new(
            "external",
            dir.join("host"),
            FakeDriver::full_capabilities(),
        ))
    })
    .await;
    let driver = connect(&served, "external").await;

    let service = common::TestService::start().await;
    let link = service.enroll("external-host").await;
    let mut config = AgentConfig::new("external-host", service.dir.path().join("scratch"));
    config.heartbeat = Duration::from_millis(50);
    let agent = Agent::new(Arc::new(link), vec![Arc::clone(&driver)], config);
    tokio::spawn(agent.run(service.shutdown_token()));

    wait_for(|| !Service::list_providers(&service.service).is_empty()).await;

    let client = service.client();
    client
        .put_template(common::template("alpine"))
        .await
        .unwrap();
    let lease = client
        .acquire(AcquireRequest {
            idempotency_key: "external-driver-flow".into(),
            template: "alpine".into(),
            purpose: Purpose::Runner.into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();

    let (status, stdout, stderr) = client
        .exec_collect(ExecRequest {
            sandbox_id: lease.sandbox.id.clone(),
            lease_id: lease.lease.id.clone(),
            argv: vec!["/bin/sh".into(), "-c".into(), "printf external-ok".into()],
            timeout_secs: 30,
            ..ExecRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(status, 0, "stderr: {}", String::from_utf8_lossy(&stderr));
    assert_eq!(stdout, b"external-ok");

    client
        .release(ReleaseRequest {
            lease_id: lease.lease.id.clone(),
            mode: ReleaseMode::Destroy.into(),
            ..ReleaseRequest::default()
        })
        .await
        .unwrap();
    wait_for(|| Service::list(&service.service).is_empty()).await;

    // Release destroyed the machine through the plugin, not just in the
    // service's bookkeeping.
    assert!(
        driver.inventory().await.unwrap().is_empty(),
        "the plugin still reports the released sandbox"
    );
}

/// A wrapper the agent can spawn as a plugin: it records its pid (which
/// `exec` preserves) and re-executes this test binary as the plugin child.
///
/// The agent appends `--socket <path>`. libtest does not accept unknown
/// flags, so the wrapper reads them and passes the path through the
/// environment, then re-executes this binary filtered to the one child test.
fn plugin_wrapper(dir: &Path) -> PathBuf {
    let wrapper = dir.join("plugin.sh");
    let pidfile = dir.join("plugin.pid");
    let exe = std::env::current_exe().unwrap();
    let quoted = preloop_sandbox::driver::shell_quote(&exe.to_string_lossy());
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nset -eu\necho $$ > {pidfile}\n[ \"$1\" = \"--socket\" ]\nexport {PLUGIN_CHILD_ENV}=1\nexport {PLUGIN_SOCKET_ENV}=\"$2\"\nexec {quoted} --exact plugin_child_process\n",
            pidfile = preloop_sandbox::driver::shell_quote(&pidfile.to_string_lossy()),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    wrapper
}

/// Whether `pid` is a live process (a zombie counts: the kill only delivered).
fn process_alive(pid: i32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn recorded_pid(dir: &Path) -> i32 {
    std::fs::read_to_string(dir.join("plugin.pid"))
        .expect("the wrapper records its pid")
        .trim()
        .parse()
        .expect("the pid is a number")
}

#[tokio::test(flavor = "multi_thread")]
async fn spawn_mode_starts_the_plugin() {
    let dir = tempfile::tempdir().unwrap();
    let wrapper = plugin_wrapper(dir.path());

    let spec = ExternalSpec::parse(&format!("spawn-test={}", wrapper.display())).unwrap();
    let driver = ExternalDriver::start(spec, &dir.path().join("runtime"))
        .await
        .unwrap();

    // A focused smoke test: the agent started the process, passed the socket
    // path and can create, exec and destroy through it.
    let sandbox_id = format!("sbx-spawn-{}", uuid::Uuid::new_v4().simple());
    let work = CreateWork {
        sandbox_id: sandbox_id.clone(),
        driver: driver.name().to_owned(),
        ..CreateWork::default()
    };
    let native_ref = driver.create(&work).await.unwrap();
    assert!(!native_ref.is_empty());

    let (events_tx, mut events_rx) = mpsc::channel(16);
    let status = driver
        .exec(
            &native_ref,
            "exe-spawn",
            &ExecRequest {
                sandbox_id,
                argv: vec!["/bin/sh".into(), "-c".into(), "printf spawn-ok".into()],
                timeout_secs: 30,
                ..ExecRequest::default()
            },
            events_tx,
        )
        .await
        .unwrap();
    let mut stdout = Vec::new();
    while let Some(event) = events_rx.recv().await {
        if let Some(exec_event::Event::Stdout(bytes)) = event.event {
            stdout.extend(bytes);
        }
    }
    assert_eq!(status, 0);
    assert_eq!(stdout, b"spawn-ok");

    driver.destroy(&native_ref).await.unwrap();
    // Dropping the driver stops the supervised child process.
    let pid = recorded_pid(dir.path());
    drop(driver);
    wait_for(|| !process_alive(pid)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn spawn_mode_refuses_a_socket_a_live_process_serves() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = dir.path().join("runtime");
    std::fs::create_dir_all(&runtime).unwrap();
    let socket = runtime.join("live.sock");
    // Stand in for a plugin an earlier agent left behind: a live listener on
    // the path the new spawn would use.
    let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    let wrapper = plugin_wrapper(dir.path());
    let spec = ExternalSpec::parse(&format!("live={}", wrapper.display())).unwrap();

    let error = ExternalDriver::start(spec, &runtime)
        .await
        .err()
        .expect("starting a second plugin on a served socket must fail");
    assert_eq!(error.code, preloop_sandbox::error::Code::FailedPrecondition);
    let message = preloop_sandbox::error::message(&error);
    assert!(
        message.contains("already served by a live process"),
        "{message}"
    );
    drop(listener);
}

#[tokio::test(flavor = "multi_thread")]
async fn spawn_mode_replaces_a_stale_socket() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = dir.path().join("runtime");
    std::fs::create_dir_all(&runtime).unwrap();
    let socket = runtime.join("stale.sock");
    // A socket file whose listener died: connect refuses, so it is stale.
    drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());
    assert!(socket.exists(), "the dead listener left its socket behind");
    let wrapper = plugin_wrapper(dir.path());
    let spec = ExternalSpec::parse(&format!("stale={}", wrapper.display())).unwrap();

    let driver = ExternalDriver::start(spec, &runtime)
        .await
        .expect("a stale socket must be replaced, not block the start");
    let pid = recorded_pid(dir.path());
    drop(driver);
    wait_for(|| !process_alive(pid)).await;
}

/// The spawn-mode child: when re-executed with the marker environment, serve
/// the fake driver on the `--socket` path the agent appended. In a normal test
/// run the marker is absent, so this returns immediately.
#[test]
fn plugin_child_process() {
    if std::env::var_os(PLUGIN_CHILD_ENV).is_none() {
        return;
    }
    let socket =
        PathBuf::from(std::env::var_os(PLUGIN_SOCKET_ENV).expect("the wrapper passes the socket"));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        let root = std::env::temp_dir().join(format!("preloop-spawn-child-{}", std::process::id()));
        let driver = Arc::new(FakeDriver::new(
            "child",
            root,
            FakeDriver::full_capabilities(),
        ));
        driver_plugin::serve(driver, &socket, CancellationToken::new())
            .await
            .expect("serving the spawn-mode plugin");
    });
}
