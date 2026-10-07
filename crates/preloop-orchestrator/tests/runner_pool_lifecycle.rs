use async_trait::async_trait;
use preloop_orchestrator::{
    DEBUG_MARKER_IDLE, JobVmDiskReserve, RUNNER_BUSY_LINE, RunnerPool, RunnerPoolConfig,
    artifact_payload, node_externals,
};
use preloop_vm::{
    ExecOutput, MachineName, MachineSpec, MachineState, NetworkPolicy, OutputChunk,
    ProviderCapabilities, SecretSource, SocketMount, VmError, VmProvider, VolumeMount,
};
use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);
static TEST_ENV_LOCK: std::sync::LazyLock<std::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(()));

#[derive(Debug, Clone, Copy)]
enum RunAction {
    Complete,
    Wait,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Create(String),
    Start(String),
    Stop(String),
    Delete(String),
    Pack(String),
    Exec(String, Vec<String>),
    Configure(String, Vec<String>, Vec<(String, SecretSource)>),
}

#[derive(Debug, Default)]
struct ProviderState {
    machines: HashMap<String, MachineState>,
    created_specs: Vec<MachineSpec>,
    events: Vec<Event>,
    run_calls: usize,
    pack_calls: usize,
}

#[derive(Debug)]
struct RecordingVmProvider {
    state: Mutex<ProviderState>,
    run_actions: Mutex<Vec<RunAction>>,
    changed: Notify,
    /// Whether packs are host files (SmolVM) or live server-side (AgentENV).
    /// A backend without host-side packs has no artifact to unpack, so its
    /// golden is baked in the guest instead.
    file_packs: bool,
}

impl RecordingVmProvider {
    fn with_machines(names: &[&str], run_actions: Vec<RunAction>) -> Self {
        let machines = names
            .iter()
            .map(|name| ((*name).to_owned(), MachineState::Stopped))
            .collect();
        Self {
            state: Mutex::new(ProviderState {
                machines,
                ..ProviderState::default()
            }),
            run_actions: Mutex::new(run_actions),
            changed: Notify::new(),
            file_packs: true,
        }
    }

    /// Model a backend whose packs are not host files (AgentENV): there is no
    /// artifact to unpack, so the pool prepares its golden in the guest.
    fn no_packs(mut self) -> Self {
        self.file_packs = false;
        self
    }

    async fn wait_until<F>(&self, predicate: F)
    where
        F: Fn(&ProviderState) -> bool,
    {
        loop {
            let notification = self.changed.notified();
            let matched = {
                let state = self.state.lock().await;
                predicate(&state)
            };
            if matched {
                return;
            }
            notification.await;
        }
    }

    async fn snapshot(&self) -> ProviderStateSnapshot {
        let state = self.state.lock().await;
        ProviderStateSnapshot {
            machines: state.machines.clone(),
            created_specs: state.created_specs.clone(),
            events: state.events.clone(),
            pack_calls: state.pack_calls,
        }
    }

    fn notify_changed(&self) {
        self.changed.notify_waiters();
    }
}

#[derive(Debug, Clone)]
struct ProviderStateSnapshot {
    machines: HashMap<String, MachineState>,
    created_specs: Vec<MachineSpec>,
    events: Vec<Event>,
    pack_calls: usize,
}

fn output() -> ExecOutput {
    ExecOutput {
        exit_code: 0,
        stdout: Vec::new(),
        stderr: Vec::new(),
        truncated: false,
    }
}

fn provider_error(message: &'static str) -> VmError {
    VmError::Command {
        operation: "recording-provider",
        exit_code: 1,
        message: message.to_owned(),
    }
}

#[async_trait]
impl VmProvider for RecordingVmProvider {
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            file_packs: self.file_packs,
            ..ProviderCapabilities::default()
        }
    }

    async fn create(&self, spec: &MachineSpec) -> Result<(), VmError> {
        let mut state = self.state.lock().await;
        state
            .machines
            .insert(spec.name.as_str().to_owned(), MachineState::Stopped);
        state.created_specs.push(spec.clone());
        state
            .events
            .push(Event::Create(spec.name.as_str().to_owned()));
        drop(state);
        self.notify_changed();
        Ok(())
    }

    async fn start(&self, name: &MachineName) -> Result<(), VmError> {
        let mut state = self.state.lock().await;
        state
            .machines
            .insert(name.as_str().to_owned(), MachineState::Running);
        state.events.push(Event::Start(name.as_str().to_owned()));
        drop(state);
        self.notify_changed();
        Ok(())
    }

    async fn start_forkable(&self, name: &MachineName) -> Result<(), VmError> {
        self.start(name).await
    }

    async fn fork(&self, _golden: &MachineName, clone: &MachineName) -> Result<(), VmError> {
        let mut state = self.state.lock().await;
        state
            .machines
            .insert(clone.as_str().to_owned(), MachineState::Running);
        state.events.push(Event::Create(clone.as_str().to_owned()));
        state.events.push(Event::Start(clone.as_str().to_owned()));
        drop(state);
        self.notify_changed();
        Ok(())
    }

    async fn stop(&self, name: &MachineName) -> Result<(), VmError> {
        let mut state = self.state.lock().await;
        state
            .machines
            .insert(name.as_str().to_owned(), MachineState::Stopped);
        state.events.push(Event::Stop(name.as_str().to_owned()));
        drop(state);
        self.notify_changed();
        Ok(())
    }

    async fn delete(&self, name: &MachineName) -> Result<(), VmError> {
        let mut state = self.state.lock().await;
        state.machines.remove(name.as_str());
        state.events.push(Event::Delete(name.as_str().to_owned()));
        drop(state);
        self.notify_changed();
        Ok(())
    }

    async fn status(&self, name: &MachineName) -> Result<MachineState, VmError> {
        Ok(self
            .state
            .lock()
            .await
            .machines
            .get(name.as_str())
            .copied()
            .unwrap_or(MachineState::Missing))
    }

    async fn list(&self) -> Result<Vec<MachineName>, VmError> {
        self.state
            .lock()
            .await
            .machines
            .keys()
            .map(|name| MachineName::new(name.clone()).map_err(|_| provider_error("invalid name")))
            .collect()
    }

    async fn exec(&self, name: &MachineName, argv: &[String]) -> Result<ExecOutput, VmError> {
        let mut state = self.state.lock().await;
        state
            .events
            .push(Event::Exec(name.as_str().to_owned(), argv.to_vec()));
        drop(state);
        self.notify_changed();
        Ok(output())
    }

    async fn exec_with_secret_env(
        &self,
        name: &MachineName,
        argv: &[String],
        secrets: &[(String, SecretSource)],
    ) -> Result<ExecOutput, VmError> {
        let mut state = self.state.lock().await;
        state.events.push(Event::Configure(
            name.as_str().to_owned(),
            argv.to_vec(),
            secrets.to_vec(),
        ));
        drop(state);
        self.notify_changed();
        Ok(output())
    }

    async fn exec_stream(
        &self,
        name: &MachineName,
        argv: &[String],
        output: tokio::sync::mpsc::Sender<OutputChunk>,
    ) -> Result<i32, VmError> {
        // The pool runs the guest runner here, so this is where a job is
        // modelled: announce that the runner is busy, then behave as the
        // scripted action says.
        let action = {
            let mut state = self.state.lock().await;
            state
                .events
                .push(Event::Exec(name.as_str().to_owned(), argv.to_vec()));
            state.run_calls += 1;
            self.run_actions
                .lock()
                .await
                .get(state.run_calls - 1)
                .copied()
                .unwrap_or(RunAction::Wait)
        };
        self.notify_changed();
        let _ = output
            .send(OutputChunk::Stdout(
                format!("{RUNNER_BUSY_LINE}\n").into_bytes(),
            ))
            .await;
        match action {
            RunAction::Complete => Ok(0),
            RunAction::Wait => std::future::pending().await,
        }
    }

    async fn copy(&self, _source: &str, _destination: &str) -> Result<(), VmError> {
        Ok(())
    }

    async fn pack(&self, name: &MachineName, output: &Path) -> Result<(), VmError> {
        // Mirror the smolvm 1.7.2 pack contract: `<output>` is an ELF
        // launcher stub and `<output>.smolmachine` carries the packed VM
        // data. The orchestrator consumes the sidecar; the stub is discarded.
        let stub = output.to_path_buf();
        let sidecar = PathBuf::from(format!("{}.smolmachine", output.display()));
        fs::write(&stub, b"elf-launcher-stub").map_err(|_| provider_error("pack"))?;
        fs::write(&sidecar, b"immutable-runner-artifact").map_err(|_| provider_error("pack"))?;
        let mut state = self.state.lock().await;
        state.pack_calls += 1;
        state.events.push(Event::Pack(name.as_str().to_owned()));
        drop(state);
        self.notify_changed();
        Ok(())
    }
}

struct Fixture {
    _env_guard: std::sync::MutexGuard<'static, ()>,
    _hermetic_env: HermeticEnvGuard,
    root: PathBuf,
    config: RunnerPoolConfig,
    token_env: String,
    token: String,
}

/// Pins the host-sensitive environment for the fixture's lifetime and
/// restores the previous values on drop. The pool reads all of these from the
/// process environment, so without the pins the tests depend on the host:
///
/// - `PRELOOP_GOLDEN_URL` -> unreachable. Configured-image fixtures bake
///   their golden locally and never download; only the official sentinel does
///   (`prepare_artifact` -> `download_prebaked_golden`), so the pin keeps
///   that download off the network and failing fast in
///   `official_golden_download_failure_is_a_startup_error` instead of
///   fetching a real pack from GHCR or GitHub on a host with egress.
/// - `PRELOOP_SKIP_DISK_PREFLIGHT` -> on. The golden build refuses when the
///   host volume lacks builder disk + pack staging (60 GiB for this fixture);
///   the provider here is a recording mock that writes nothing, so a host
///   with less free space would refuse the build and hang the same wait.
/// - `PRELOOP_HOME` -> the fixture root. Startup reconciliation kills real
///   `_boot-vm` processes and sweeps data dirs under the Preloop home; left
///   unset it resolves to the developer's `~/.preloop`, where a running
///   engine's VMs live.
struct HermeticEnvGuard {
    previous: Vec<(&'static str, Option<std::ffi::OsString>)>,
}

impl HermeticEnvGuard {
    fn new(root: &Path) -> Self {
        let pins: [(&'static str, std::ffi::OsString); 3] = [
            (
                "PRELOOP_GOLDEN_URL",
                "http://127.0.0.1:1/preloop-golden-unreachable".into(),
            ),
            ("PRELOOP_SKIP_DISK_PREFLIGHT", "1".into()),
            ("PRELOOP_HOME", root.join("preloop-home").into_os_string()),
        ];
        let previous = pins
            .iter()
            .map(|(name, _)| (*name, std::env::var_os(name)))
            .collect();
        for (name, value) in &pins {
            unsafe { std::env::set_var(name, value) };
        }
        Self { previous }
    }
}

impl Drop for HermeticEnvGuard {
    fn drop(&mut self) {
        for (name, value) in &self.previous {
            match value {
                Some(value) => unsafe { std::env::set_var(name, value) },
                None => unsafe { std::env::remove_var(name) },
            }
        }
    }
}

impl Fixture {
    fn new(label: &str, payload_exists: bool) -> Self {
        // A configured custom image, not the official sentinel: the pool bakes
        // it locally (as-is plus the runner contract), so no fixture here
        // depends on the released official golden.
        const BASE_IMAGE: &str = "ghcr.io/preloop/base:latest";
        let env_guard = TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "preloop-orchestrator-{label}-{}-{id}",
            std::process::id()
        ));
        fs::create_dir(&root).expect("unique test fixture directory");
        let bundle = root.join("runner-bundle");
        fs::create_dir(&bundle).unwrap();
        let externals_dir = root.join("host-externals");
        stage_fake_node_externals(&externals_dir.join("externals"));
        let artifact_stem = root.join("runner-image");
        if payload_exists {
            fs::write(
                artifact_payload(&artifact_stem, BASE_IMAGE),
                b"existing-artifact",
            )
            .unwrap();
        }
        let control_socket = root.join("engine.sock");
        fs::write(&control_socket, b"test-control-socket").unwrap();
        fs::create_dir(root.join("control-bridge")).unwrap();

        let token_env = format!("PRELOOP_TEST_TOKEN_{label}_{id}");
        let token = format!("sentinel-registration-token-{id}");
        unsafe { std::env::set_var(&token_env, &token) };
        let config = RunnerPoolConfig {
            size: 1,
            use_fork: false,
            name_prefix: format!("pool-{label}-{id}"),
            pool_status: None,
            base_image: BASE_IMAGE.to_owned(),
            artifact_stem,
            release_version: "9.9.9".to_owned(),
            runner_bundle: bundle,
            externals_dir,
            runner_binary_name: "preloop-runner".to_owned(),
            server_url: "https://preloop.example".to_owned(),
            control_origin: None,
            control_socket: None,
            control_upstream: None,
            dns: None,
            registration_token_env: token_env.clone(),
            labels: vec!["self-hosted".to_owned(), "linux".to_owned()],
            cpus: 2,
            memory_mib: 256,
            storage_gib: 10,
            overlay_gib: None,
            // The hermetic provider writes no disks: keep the reserve off so
            // these tests never depend on the host's free space.
            job_vm_disk: JobVmDiskReserve::new(0, |_| Ok(0)),
            debug_dir: None,
            runner_key_dir: None,
            pending_jobs: None,
            preload_images: Vec::new(),
            runner_user: None,
            runner_uid: None,
            next_job_runs_on: None,
            pending_registrations: None,
            preparing_signal: None,
            observability: None,
        };
        Self {
            _env_guard: env_guard,
            _hermetic_env: HermeticEnvGuard::new(&root),
            root,
            config,
            token_env,
            token,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        unsafe { std::env::remove_var(&self.token_env) };
        let _ = fs::remove_dir_all(&self.root);
    }
}

/// Stand in for the host's Node externals with a tiny, already-valid tree.
///
/// The pool validates `externals/<runtime>` by manifest plus `bin/node
/// --version` and downloads ~360 MB from nodejs.org when that fails, then
/// copies it into every fixture's bundle. A shared real path made each test
/// download on a fresh CI machine, and parallel test processes raced on it; a
/// loser's pool errored out and its test hung waiting for a runner.
fn stage_fake_node_externals(externals: &Path) {
    use std::os::unix::fs::PermissionsExt;
    for (runtime, version) in node_externals::expected_runtimes() {
        let plain = version.trim_start_matches('v');
        let dir = externals.join(runtime);
        fs::create_dir_all(dir.join("bin")).unwrap();
        let node = dir.join("bin/node");
        fs::write(&node, format!("#!/bin/sh\necho v{plain}\n")).unwrap();
        fs::set_permissions(&node, fs::Permissions::from_mode(0o755)).unwrap();
        let manifest = node_externals::NodeManifest::new(
            runtime,
            plain,
            &node_externals::current_platform(),
            "fixture",
            "fixture",
        );
        node_externals::write_manifest(&dir, &manifest).unwrap();
    }
}

async fn run_until_cancelled(
    pool: RunnerPool<RecordingVmProvider>,
    provider: &RecordingVmProvider,
    shutdown: CancellationToken,
    run_calls: usize,
) {
    let task_shutdown = shutdown.clone();
    let mut task = tokio::spawn(async move { pool.run(task_shutdown).await });
    // A pool that exits before reaching the runner must fail the test, not
    // leave it waiting for a run call that can no longer happen.
    tokio::select! {
        () = provider.wait_until(|state| state.run_calls >= run_calls) => {}
        result = &mut task => panic!("pool exited before {run_calls} runner call(s): {result:?}"),
    }
    shutdown.cancel();
    task.await.unwrap().unwrap();
}

async fn wait_for_debug_marker(provider: &RecordingVmProvider, marker: &Path) {
    provider
        .wait_until(|state| {
            state.events.iter().any(|event| {
                matches!(event, Event::Exec(_, argv) if argv.iter().any(|argument| argument == "-f"))
            })
        })
        .await;
    while !marker.is_file() {
        tokio::task::yield_now().await;
    }
}

async fn yield_to_pool() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

/// Advance paused time in small steps until `predicate` holds, bounded so a
/// regression fails loudly instead of spinning the suite.
///
/// The fixed `advance(n)` + yield-budget dance this replaces was flaky under
/// CPU contention: the pool's poll is a multi-await chain (timer → fs →
/// provider), and when other tasks on the runtime starved it of scheduling
/// turns the assertion ran before the chain completed. Advancing 100 ms at a
/// time and yielding after every step lets the pool's chain interleave at
/// every boundary; the step is far below the pool's poll interval, so no
/// observable transition can be skipped over.
async fn advance_until<F, Fut>(mut predicate: F, what: &str)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if predicate().await {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for: {what}");
        }
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
    }
}

/// Name of the first machine slot 0 created.
///
/// Slot machines carry a generation suffix so consecutive runners get distinct
/// names across the fork/delete cycle, so tests resolve the name instead of
/// assuming one machine per slot.
fn first_slot_machine(events: &[Event], name_prefix: &str) -> String {
    let slot_prefix = format!("{name_prefix}-0-");
    events
        .iter()
        .find_map(|event| match event {
            Event::Create(name) if name.starts_with(&slot_prefix) => Some(name.clone()),
            _ => None,
        })
        .expect("slot 0 created a machine")
}

/// Block until slot 0 has created its first machine, then return the name.
async fn await_first_slot_machine(provider: &RecordingVmProvider, name_prefix: &str) -> String {
    let slot_prefix = format!("{name_prefix}-0-");
    provider
        .wait_until(|state| {
            state
                .events
                .iter()
                .any(|event| matches!(event, Event::Create(name) if name.starts_with(&slot_prefix)))
        })
        .await;
    first_slot_machine(&provider.snapshot().await.events, name_prefix)
}

#[tokio::test]
async fn artifact_preparation_runs_once_and_reuses_payload_on_next_run() {
    let fixture = Fixture::new("artifact", false);
    let provider = Arc::new(RecordingVmProvider::with_machines(
        &[],
        vec![RunAction::Wait, RunAction::Wait],
    ));
    let pool = RunnerPool::new(provider.clone(), fixture.config.clone()).unwrap();
    // The builder machine's bake steps (guest readiness, the runner
    // contract, the externals symlink) count as `exec` calls, so a
    // `run_calls` wait can cancel the pool between the builder's start and
    // its pack on a loaded host — the pack never runs and the assertion
    // below fails with `pack_calls == 0`. Wait on the pack itself, which
    // only happens after the artifact is fully built, then cancel.
    let task_shutdown = CancellationToken::new();
    let task = tokio::spawn({
        let task_shutdown = task_shutdown.clone();
        async move { pool.run(task_shutdown).await }
    });
    provider
        .wait_until(|state| {
            state
                .events
                .iter()
                .any(|event| matches!(event, Event::Pack(_)))
        })
        .await;
    task_shutdown.cancel();
    task.await.unwrap().unwrap();

    assert!(artifact_payload(&fixture.config.artifact_stem, &fixture.config.base_image).is_file());
    let first = provider.snapshot().await;
    assert_eq!(first.pack_calls, 1);
    assert_eq!(
        first
            .events
            .iter()
            .filter(|event| matches!(event, Event::Create(name) if name.ends_with("-builder")))
            .count(),
        1
    );

    let pool = RunnerPool::new(provider.clone(), fixture.config.clone()).unwrap();
    run_until_cancelled(pool, &provider, CancellationToken::new(), 2).await;
    let second = provider.snapshot().await;
    assert_eq!(second.pack_calls, 1);
    assert_eq!(
        second
            .events
            .iter()
            .filter(|event| matches!(event, Event::Create(name) if name.ends_with("-builder")))
            .count(),
        1
    );
}

/// The official golden is downloaded packed and is never baked locally.
///
/// The deleted stock-Ubuntu bake used to stand in when the official golden
/// could not be fetched, which silently ran jobs on a different image than the
/// one `runs-on: ubuntu-latest` names. There is no substitute now: an
/// unreachable pack fails the pool at startup, and the error names the
/// official golden so an operator knows to point
/// `PRELOOP_GOLDEN_OCI_REF`/`PRELOOP_GOLDEN_URL` at a reachable one.
#[tokio::test]
async fn official_golden_download_failure_is_a_startup_error() {
    let fixture = Fixture::new("official-fatal", false);
    let mut config = fixture.config.clone();
    config.base_image = preloop_orchestrator::environment::OFFICIAL_GOLDEN.to_owned();
    let provider = Arc::new(RecordingVmProvider::with_machines(&[], vec![]));
    let pool = RunnerPool::new(provider.clone(), config).unwrap();

    let error = pool
        .run(CancellationToken::new())
        .await
        .expect_err("an unreachable official golden must fail the pool, not bake a substitute");
    let message = error.to_string();
    assert!(
        message.contains("official golden"),
        "the error must name the official golden: {message}"
    );
    assert!(
        provider.snapshot().await.events.is_empty(),
        "no substitute golden may be created for the official sentinel"
    );
}

/// A backend without host-side packs has no artifact to unpack: its golden is
/// booted from the configured image, the runner contract is applied in the
/// guest, and the result is frozen with `start_forkable`. The contract is
/// applied once, on the golden — forks inherit it — so no runner pays for it.
#[tokio::test]
async fn fork_golden_is_marked_forkable_after_guest_provisioning() {
    let fixture = Fixture::new("fork-golden", false);
    let mut config = fixture.config.clone();
    config.use_fork = true;
    config.control_socket = Some(fixture.root.join("engine.sock"));
    let provider =
        Arc::new(RecordingVmProvider::with_machines(&[], vec![RunAction::Wait]).no_packs());
    let pool = RunnerPool::new(provider.clone(), config).unwrap();
    run_until_cancelled(pool, &provider, CancellationToken::new(), 1).await;

    let snapshot = provider.snapshot().await;
    let golden = format!("{}-golden", fixture.config.name_prefix);
    // The golden boots the configured image itself: there is no packed
    // artifact on this backend to unpack, and none may be built.
    let spec = snapshot
        .created_specs
        .iter()
        .find(|spec| spec.name.as_str() == golden)
        .expect("golden machine specification");
    assert_eq!(spec.image, fixture.config.base_image);

    let events = &snapshot.events;
    let golden_starts = events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            matches!(event, Event::Start(name) if name == &golden).then_some(index)
        })
        .collect::<Vec<_>>();
    assert_eq!(golden_starts.len(), 2);
    let first_start = golden_starts[0];
    let stop = events
        .iter()
        .position(|event| matches!(event, Event::Stop(name) if name == &golden))
        .expect("golden stopped before forkable restart");
    assert!(first_start < stop);
    assert!(
        events[first_start + 1..stop]
            .iter()
            .any(|event| matches!(event, Event::Exec(name, _) if name == &golden)),
        "the guest contract must be applied to the golden before it freezes: {events:?}"
    );
    assert!(stop < golden_starts[1]);
}

#[tokio::test]
async fn configure_passes_secret_environment_mapping_without_token_value() {
    let fixture = Fixture::new("secret", true);
    let provider = Arc::new(RecordingVmProvider::with_machines(
        &[],
        vec![RunAction::Wait],
    ));
    let pool = RunnerPool::new(provider.clone(), fixture.config.clone()).unwrap();
    run_until_cancelled(pool, &provider, CancellationToken::new(), 1).await;

    let snapshot = provider.snapshot().await;
    let configure = snapshot
        .events
        .iter()
        .find_map(|event| match event {
            Event::Configure(_, argv, secrets) => Some((argv, secrets)),
            _ => None,
        })
        .expect("runner configure command");
    assert_eq!(
        configure.1,
        &vec![(
            "PRELOOP_RUNNER_TOKEN".to_owned(),
            SecretSource::HostEnv(fixture.token_env.clone())
        )]
    );
    assert!(
        configure
            .0
            .iter()
            .all(|argument| argument != &fixture.token)
    );
    // Only a reference to the credential is ever handed to SmolVM.
    assert!(configure.1.iter().all(|(key, source)| {
        key != &fixture.token
            && match source {
                SecretSource::HostEnv(name) => name != &fixture.token,
                SecretSource::HostFile(path) => path.as_os_str() != fixture.token.as_str(),
            }
    }));
}

#[tokio::test]
async fn runner_keeps_public_only_egress_and_wires_control_socket_and_environment() {
    let fixture = Fixture::new("control", true);
    let mut config = fixture.config.clone();
    config.control_socket = Some(fixture.root.join("engine.sock"));
    let provider = Arc::new(RecordingVmProvider::with_machines(
        &[],
        vec![RunAction::Wait],
    ));
    let pool = RunnerPool::new(provider.clone(), config.clone()).unwrap();
    run_until_cancelled(pool, &provider, CancellationToken::new(), 1).await;

    let snapshot = provider.snapshot().await;
    let runner = first_slot_machine(&snapshot.events, &fixture.config.name_prefix);
    let spec = snapshot
        .created_specs
        .iter()
        .find(|spec| spec.name.as_str() == runner)
        .expect("runner machine specification");
    assert_eq!(spec.network, NetworkPolicy::PublicOnly);
    // A packed machine reaches node through the artifact's baked symlink
    // (`<root>/externals -> /opt/preloop/bin/externals`): the externals ride
    // the bundle mount instead of a third virtiofs mount, which the packed
    // launcher has no IRQ budget for. Only a machine booted from a raw image
    // carries the separate externals mount.
    assert_eq!(
        &spec.volumes,
        &vec![
            VolumeMount {
                host: fixture.root.join("runner-bundle"),
                guest: PathBuf::from("/opt/preloop/bin"),
                read_only: true,
            },
            VolumeMount {
                // Per-machine control-bridge directory: the guest agent's
                // socket node lands INSIDE this host directory, so a shared
                // one would leak dead machines' listeners into new machines.
                host: fixture.root.join("control-bridge").join(&runner),
                guest: PathBuf::from("/run/preloop-control"),
                read_only: false,
            },
        ]
    );
    assert_eq!(
        &spec.sockets,
        &vec![SocketMount {
            host: config.control_socket.clone().unwrap(),
            guest: PathBuf::from("/run/preloop-control/engine.sock"),
        }]
    );

    // The guest is always told its own machine name: a debug session needs it
    // to tell a controller which VM to open a shell into.
    let expected_prefix = vec![
        "/usr/bin/env".to_owned(),
        format!("PATH={}", preloop_orchestrator::guest_runner_path(&config)),
        format!("PRELOOP_MACHINE_NAME={runner}"),
        "PRELOOP_CONTROL_ORIGIN=https://preloop.example".to_owned(),
        "PRELOOP_CONTROL_SOCKET=/run/preloop-control/engine.sock".to_owned(),
    ];
    let configure = snapshot
        .events
        .iter()
        .find_map(|event| match event {
            Event::Configure(name, argv, _) if name == &runner => Some(argv),
            _ => None,
        })
        .expect("runner configure command");
    // `runner_user: None`, so the launch is the pass-through wrapper
    // (`sh -c '<stack raise>; exec "$@"' sh`): the env prefix follows the
    // wrapper, untouched.
    assert_eq!(
        &configure[4..4 + expected_prefix.len()],
        expected_prefix.as_slice()
    );

    let run = snapshot
        .events
        .iter()
        .find_map(|event| match event {
            Event::Exec(name, argv) if name == &runner && argv.iter().any(|arg| arg == "run") => {
                Some(argv)
            }
            _ => None,
        })
        .expect("runner run command");
    // This pool runs `runner_user: None`, so the launch is the pass-through
    // wrapper: `sh -c '<stack raise>; exec "$@"' sh <argv…>`. The original
    // argv follows the four wrapper elements untouched — that is the point of
    // the `$@` form, so nothing here is re-quoted.
    assert_eq!(run[0], "sh");
    assert_eq!(run[1], "-c");
    assert_eq!(
        run[2],
        "ulimit -Hs unlimited; ulimit -Ss 16384; \
          ulimit -Sn 65536 2>/dev/null || true; \
          ulimit -Hn 65536 2>/dev/null || \
          echo preloop: RLIMIT_NOFILE hard limit stays $(ulimit -Hn) - raising it needs root and this launch keeps the exec channel identity >&2; \
          exec \"$@\""
    );
    assert_eq!(run[3], "sh");
    assert_eq!(
        &run[4..4 + expected_prefix.len()],
        expected_prefix.as_slice()
    );
}

/// Control-socket routing and failure-marker debugging are independent knobs.
///
/// The marker used to be gated behind `control_socket.is_some()`, so a pool
/// configured for debugging but without a mounted socket silently never told
/// the runner where to write the marker — preservation could never trigger.
#[tokio::test]
async fn guest_environment_tracks_control_socket_and_debug_dir_independently() {
    const ORIGIN: &str = "PRELOOP_CONTROL_ORIGIN=https://preloop.example";
    const SOCKET: &str = "PRELOOP_CONTROL_SOCKET=/run/preloop-control/engine.sock";
    const MARKER: &str = "PRELOOP_FAILURE_MARKER=/home/runner/.preloop-job-failed";
    const PAUSE_MARKER: &str = "PRELOOP_PAUSE_MARKER=/home/runner/.preloop-job-paused";

    // `PRELOOP_MACHINE_NAME` is unconditional and slot-dependent, so each case
    // lists only the knob-driven tail.
    let cases: [(bool, bool, Vec<&str>); 4] = [
        (false, false, vec![]),
        (true, false, vec![ORIGIN, SOCKET]),
        (false, true, vec![MARKER, PAUSE_MARKER]),
        (true, true, vec![ORIGIN, SOCKET, MARKER, PAUSE_MARKER]),
    ];

    for (with_socket, with_debug_dir, expected) in cases {
        let fixture = Fixture::new("guestenv", true);
        let mut config = fixture.config.clone();
        if with_socket {
            config.control_socket = Some(fixture.root.join("engine.sock"));
        }
        if with_debug_dir {
            config.debug_dir = Some(fixture.root.join("debug"));
        }

        let provider = Arc::new(RecordingVmProvider::with_machines(
            &[],
            vec![RunAction::Wait],
        ));
        let pool = RunnerPool::new(provider.clone(), config.clone()).unwrap();
        run_until_cancelled(pool, &provider, CancellationToken::new(), 1).await;

        let snapshot = provider.snapshot().await;
        let runner = first_slot_machine(&snapshot.events, &config.name_prefix);
        let configure = snapshot
            .events
            .iter()
            .find_map(|event| match event {
                Event::Configure(name, argv, _) if name == &runner => Some(argv),
                _ => None,
            })
            .expect("runner configure command")
            .clone();

        // The prefix is everything before the runner executable itself.
        // `runner_user: None`, so the launch is the pass-through wrapper
        // (`sh -c '<stack raise>; exec "$@"' sh`) and the original argv — env
        // entries included — follows it untouched.
        assert_eq!(
            &configure[..4],
            [
                "sh",
                "-c",
                "ulimit -Hs unlimited; ulimit -Ss 16384; \
                  ulimit -Sn 65536 2>/dev/null || true; \
                  ulimit -Hn 65536 2>/dev/null || \
                  echo preloop: RLIMIT_NOFILE hard limit stays $(ulimit -Hn) - raising it needs root and this launch keeps the exec channel identity >&2; \
                  exec \"$@\"",
                "sh"
            ]
        );
        let prefix: Vec<&str> = configure[4..]
            .iter()
            .take_while(|arg| !arg.ends_with(&config.runner_binary_name))
            .map(String::as_str)
            .collect();
        let machine_name = format!("PRELOOP_MACHINE_NAME={runner}");
        // The runner's PATH is exactly the system directories a hosted step
        // shell sees: the deleted Rust/Go language installs (and their
        // `/usr/local/cargo/bin` and `/usr/local/go/bin` entries) are gone.
        let want_base = vec![
            "/usr/bin/env",
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            machine_name.as_str(),
        ];
        let mut want = want_base;
        want.extend(expected);
        assert_eq!(prefix, want);
        // Nor may anything export the deleted Rust/Go install homes: a runner
        // told RUSTUP_HOME/CARGO_HOME would point rustup at paths the golden
        // no longer carries.
        assert!(
            configure
                .iter()
                .all(|argument| !argument.starts_with("RUSTUP_HOME=")
                    && !argument.starts_with("CARGO_HOME=")),
            "the deleted Rust/Go install homes must not be exported: {configure:?}"
        );
    }
}

/// A slot must hold at most one VM: the next runner is forked only after the
/// finished job's machine was deleted.
///
/// Fork-on-completion trades the successor's head start for half the VMs per
/// slot; if a fork ever outlived the previous VM, the pool would neither get
/// that saving nor the speedup.
#[tokio::test]
async fn slot_never_holds_two_vms_at_once() {
    let fixture = Fixture::new("replenish", true);
    let provider = Arc::new(RecordingVmProvider::with_machines(
        &[],
        vec![RunAction::Complete, RunAction::Complete, RunAction::Wait],
    ));
    let pool = RunnerPool::new(provider.clone(), fixture.config.clone()).unwrap();
    run_until_cancelled(pool, &provider, CancellationToken::new(), 2).await;

    let events = provider.snapshot().await.events;
    let slot_prefix = fixture.config.name_prefix.clone() + "-0-";
    let mut live = std::collections::HashSet::new();
    let mut created = 0usize;
    for event in &events {
        match event {
            Event::Create(name) if name.starts_with(&slot_prefix) => {
                created += 1;
                assert!(
                    live.insert(name.clone()),
                    "{name} was forked while {live:?} was still alive: {events:?}"
                );
            }
            Event::Delete(name) => {
                live.remove(name);
            }
            _ => {}
        }
    }
    assert!(
        created >= 2,
        "the slot must have replaced its VM after the first job: {events:?}"
    );
}

#[tokio::test]
async fn stale_owned_machines_are_removed_without_touching_unrelated_machines() {
    let fixture = Fixture::new("stale", true);
    let stale = fixture.config.name_prefix.clone() + "-old";
    let unrelated = "unrelated-machine";
    let provider = Arc::new(RecordingVmProvider::with_machines(
        &[&stale, unrelated],
        vec![RunAction::Wait],
    ));
    let pool = RunnerPool::new(provider.clone(), fixture.config.clone()).unwrap();
    run_until_cancelled(pool, &provider, CancellationToken::new(), 1).await;

    let snapshot = provider.snapshot().await;
    assert!(!snapshot.machines.contains_key(&stale));
    assert!(snapshot.machines.contains_key(unrelated));
    assert!(
        snapshot
            .events
            .iter()
            .any(|event| matches!(event, Event::Delete(name) if name == &stale))
    );
    assert!(
        !snapshot
            .events
            .iter()
            .any(|event| matches!(event, Event::Delete(name) if name == unrelated))
    );
}

#[tokio::test]
async fn cancellation_deletes_owned_active_machine() {
    let fixture = Fixture::new("cancel", true);
    let provider = Arc::new(RecordingVmProvider::with_machines(
        &[],
        vec![RunAction::Wait],
    ));
    let pool = RunnerPool::new(provider.clone(), fixture.config.clone()).unwrap();
    run_until_cancelled(pool, &provider, CancellationToken::new(), 1).await;

    let snapshot = provider.snapshot().await;
    let runner = first_slot_machine(&snapshot.events, &fixture.config.name_prefix);
    assert_eq!(snapshot.machines.get(&runner), None);
    assert!(
        snapshot
            .events
            .iter()
            .any(|event| matches!(event, Event::Delete(name) if name == &runner))
    );
}

#[tokio::test]
async fn preserved_runner_expires_at_idle_timeout_without_heartbeat() {
    tokio::time::pause();
    let fixture = Fixture::new("debug-idle", true);
    let mut config = fixture.config.clone();
    let debug_dir = fixture.root.join("debug");
    config.debug_dir = Some(debug_dir.clone());
    let provider = Arc::new(RecordingVmProvider::with_machines(
        &[],
        vec![RunAction::Complete],
    ));
    let name_prefix = config.name_prefix.clone();
    let shutdown = CancellationToken::new();
    let task_provider = provider.clone();
    let task_shutdown = shutdown.clone();
    let pool = RunnerPool::new(provider.clone(), config).unwrap();
    let task = tokio::spawn(async move { pool.run(task_shutdown).await });

    let runner = await_first_slot_machine(&provider, &name_prefix).await;
    let marker = debug_dir.join(&runner);
    wait_for_debug_marker(&provider, &marker).await;
    assert_eq!(fs::read_to_string(&marker).unwrap(), DEBUG_MARKER_IDLE);

    tokio::time::advance(std::time::Duration::from_secs(599)).await;
    yield_to_pool().await;
    assert!(
        task_provider
            .snapshot()
            .await
            .machines
            .contains_key(&runner)
    );

    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    yield_to_pool().await;
    provider
        .wait_until(|state| {
            state
                .events
                .iter()
                .any(|event| matches!(event, Event::Delete(name) if name == &runner))
        })
        .await;
    shutdown.cancel();
    task.await.unwrap().unwrap();
    assert!(!provider.snapshot().await.machines.contains_key(&runner));
}

#[tokio::test]
async fn removing_debug_marker_releases_preserved_runner_on_next_poll() {
    tokio::time::pause();
    let fixture = Fixture::new("debug-remove", true);
    let mut config = fixture.config.clone();
    let debug_dir = fixture.root.join("debug");
    config.debug_dir = Some(debug_dir.clone());
    let provider = Arc::new(RecordingVmProvider::with_machines(
        &[],
        vec![RunAction::Complete],
    ));
    let name_prefix = config.name_prefix.clone();
    let shutdown = CancellationToken::new();
    let task_provider = provider.clone();
    let task_shutdown = shutdown.clone();
    let pool = RunnerPool::new(provider.clone(), config).unwrap();
    let task = tokio::spawn(async move { pool.run(task_shutdown).await });

    let runner = await_first_slot_machine(&provider, &name_prefix).await;
    let marker = debug_dir.join(&runner);
    wait_for_debug_marker(&provider, &marker).await;
    advance_until(
        || async {
            task_provider
                .snapshot()
                .await
                .machines
                .contains_key(&runner)
        },
        "preserved runner to exist while its debug marker is present",
    )
    .await;

    fs::remove_file(&marker).unwrap();

    advance_until(
        || async {
            task_provider
                .snapshot()
                .await
                .events
                .iter()
                .any(|event| matches!(event, Event::Delete(name) if name == &runner))
        },
        "preserved runner to be released after its debug marker was removed",
    )
    .await;
    shutdown.cancel();
    task.await.unwrap().unwrap();
    assert!(!provider.snapshot().await.machines.contains_key(&runner));
}
