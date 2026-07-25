use async_trait::async_trait;
use preloop_orchestrator::{
    artifact_payload, RunnerPool, RunnerPoolConfig, DEBUG_MARKER_IDLE, RUNNER_BUSY_LINE,
};
use preloop_vm::{
    ExecOutput, MachineName, MachineSpec, MachineState, NetworkPolicy, OutputChunk, SecretSource,
    SocketMount, VmError, VmProvider, VolumeMount,
};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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
        }
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
        let mut payload = output.as_os_str().to_owned();
        payload.push(".smolmachine");
        fs::write(&payload, b"immutable-runner-artifact").map_err(|_| provider_error("pack"))?;
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
    root: PathBuf,
    config: RunnerPoolConfig,
    token_env: String,
    token: String,
}

impl Fixture {
    fn new(label: &str, payload_exists: bool) -> Self {
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
        let artifact_stem = root.join("runner-image");
        if payload_exists {
            fs::write(artifact_payload(&artifact_stem), b"existing-artifact").unwrap();
        }
        let control_socket = root.join("engine.sock");
        fs::write(&control_socket, b"test-control-socket").unwrap();
        fs::create_dir(root.join("control-bridge")).unwrap();

        let token_env = format!("PRELOOP_TEST_TOKEN_{label}_{id}");
        let token = format!("sentinel-registration-token-{id}");
        std::env::set_var(&token_env, &token);
        let config = RunnerPoolConfig {
            size: 1,
            use_fork: false,
            name_prefix: format!("pool-{label}-{id}"),
            base_image: "ghcr.io/preloop/base:latest".to_owned(),
            workspace: None,
            artifact_stem,
            runner_bundle: bundle,
            runner_binary_name: "preloop-runner".to_owned(),
            server_url: "https://preloop.example".to_owned(),
            control_socket: None,
            registration_token_env: token_env.clone(),
            labels: vec!["self-hosted".to_owned(), "linux".to_owned()],
            cpus: 2,
            memory_mib: 256,
            storage_gib: 10,
            debug_dir: None,
            runner_key_dir: None,
            pending_jobs: None,
        };
        Self {
            _env_guard: env_guard,
            root,
            config,
            token_env,
            token,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::env::remove_var(&self.token_env);
        let _ = fs::remove_dir_all(&self.root);
    }
}

async fn run_until_cancelled(
    pool: RunnerPool<RecordingVmProvider>,
    provider: &RecordingVmProvider,
    shutdown: CancellationToken,
    run_calls: usize,
) {
    let task_shutdown = shutdown.clone();
    let task = tokio::spawn(async move { pool.run(task_shutdown).await });
    provider
        .wait_until(|state| state.run_calls >= run_calls)
        .await;
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

/// Name of the first machine slot 0 created.
///
/// Slot machines carry a generation suffix so a replacement can be built while
/// its predecessor is still alive, so tests resolve the name instead of
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
    run_until_cancelled(pool, &provider, CancellationToken::new(), 1).await;

    assert!(artifact_payload(&fixture.config.artifact_stem).is_file());
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
    assert!(configure
        .0
        .iter()
        .all(|argument| argument != &fixture.token));
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
    assert_eq!(
        &spec.volumes,
        &vec![
            VolumeMount {
                host: fixture.root.join("runner-bundle"),
                guest: PathBuf::from("/opt/preloop/bin"),
                read_only: true,
            },
            VolumeMount {
                host: fixture.root.join("control-bridge"),
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

    let expected_prefix = vec![
        "/usr/bin/env".to_owned(),
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
    assert_eq!(
        &configure[..expected_prefix.len()],
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
    assert_eq!(&run[..expected_prefix.len()], expected_prefix.as_slice());
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
    const MARKER: &str = "PRELOOP_FAILURE_MARKER=/var/lib/preloop-runner/.preloop-job-failed";

    let cases: [(bool, bool, Vec<&str>); 4] = [
        (false, false, vec![]),
        (true, false, vec!["/usr/bin/env", ORIGIN, SOCKET]),
        (false, true, vec!["/usr/bin/env", MARKER]),
        (true, true, vec!["/usr/bin/env", ORIGIN, SOCKET, MARKER]),
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
        let prefix: Vec<&str> = configure
            .iter()
            .take_while(|arg| !arg.ends_with(&config.runner_binary_name))
            .map(String::as_str)
            .collect();
        assert_eq!(
            prefix, expected,
            "socket={with_socket} debug_dir={with_debug_dir}"
        );
    }
}

/// A slot must build its replacement while the current job is still running,
/// and must still tear the finished runner down.
///
/// Waiting for the job to end before provisioning put a fork plus a full
/// runner registration in front of every job that arrives while the pool is
/// saturated — the cost a matrix workflow pays on every shard past the pool
/// size.
#[tokio::test]
async fn slot_builds_its_replacement_while_the_job_runs() {
    let fixture = Fixture::new("replenish", true);
    let provider = Arc::new(RecordingVmProvider::with_machines(
        &[],
        vec![RunAction::Complete, RunAction::Wait],
    ));
    let pool = RunnerPool::new(provider.clone(), fixture.config.clone()).unwrap();
    run_until_cancelled(pool, &provider, CancellationToken::new(), 2).await;

    let events = provider.snapshot().await.events;
    let slot_prefix = fixture.config.name_prefix.clone() + "-0-";
    let (first_run, first_runner) = events
        .iter()
        .enumerate()
        .find_map(|(index, event)| match event {
            Event::Exec(name, argv)
                if name.starts_with(&slot_prefix) && argv.iter().any(|arg| arg == "run") =>
            {
                Some((index, name.clone()))
            }
            _ => None,
        })
        .expect("the slot ran a runner");

    let replacement_created = events
        .iter()
        .enumerate()
        .position(|(index, event)| {
            index > first_run
                && matches!(event, Event::Create(name) if name.starts_with(&slot_prefix) && name != &first_runner)
        })
        .expect("the slot created a replacement runner");
    let first_deleted = events
        .iter()
        .position(|event| matches!(event, Event::Delete(name) if name == &first_runner))
        .expect("the finished runner was deleted");

    assert!(
        replacement_created < first_deleted,
        "replacement must be built before the finished runner is torn down, got {events:?}"
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
    assert!(snapshot
        .events
        .iter()
        .any(|event| matches!(event, Event::Delete(name) if name == &stale)));
    assert!(!snapshot
        .events
        .iter()
        .any(|event| matches!(event, Event::Delete(name) if name == unrelated)));
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
    assert!(snapshot
        .events
        .iter()
        .any(|event| matches!(event, Event::Delete(name) if name == &runner)));
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
    assert!(task_provider
        .snapshot()
        .await
        .machines
        .contains_key(&runner));

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
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    fs::remove_file(&marker).unwrap();

    tokio::time::advance(std::time::Duration::from_secs(9)).await;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert!(task_provider
        .snapshot()
        .await
        .machines
        .contains_key(&runner));

    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    println!(
        "events at removal poll: {:?}",
        task_provider.snapshot().await.events
    );
    assert!(task_provider
        .snapshot()
        .await
        .events
        .iter()
        .any(|event| matches!(event, Event::Delete(name) if name == &runner)));
    shutdown.cancel();
    task.await.unwrap().unwrap();
    assert!(!provider.snapshot().await.machines.contains_key(&runner));
}
