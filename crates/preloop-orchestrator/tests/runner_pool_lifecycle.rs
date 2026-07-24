use async_trait::async_trait;
use preloop_orchestrator::{artifact_payload, RunnerPool, RunnerPoolConfig};
use preloop_vm::{
    ExecOutput, MachineName, MachineSpec, MachineState, OutputChunk, VmError, VmProvider,
};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

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
    Configure(String, Vec<String>, Vec<(String, String)>),
}

#[derive(Debug, Default)]
struct ProviderState {
    machines: HashMap<String, MachineState>,
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
        let is_run = argv.get(1).is_some_and(|argument| argument == "run");
        let action = {
            let mut state = self.state.lock().await;
            state
                .events
                .push(Event::Exec(name.as_str().to_owned(), argv.to_vec()));
            if is_run {
                state.run_calls += 1;
                self.run_actions
                    .lock()
                    .await
                    .get(state.run_calls - 1)
                    .copied()
                    .unwrap_or(RunAction::Wait)
            } else {
                RunAction::Complete
            }
        };
        self.notify_changed();
        match action {
            RunAction::Complete => Ok(output()),
            RunAction::Wait => std::future::pending().await,
        }
    }

    async fn exec_with_secret_env(
        &self,
        name: &MachineName,
        argv: &[String],
        secrets: &[(String, String)],
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
        _name: &MachineName,
        _argv: &[String],
        _output: tokio::sync::mpsc::Sender<OutputChunk>,
    ) -> Result<i32, VmError> {
        Ok(0)
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
    root: PathBuf,
    config: RunnerPoolConfig,
    token_env: String,
    token: String,
}

impl Fixture {
    fn new(label: &str, payload_exists: bool) -> Self {
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
            runner_binary_name: "aksh-runner".to_owned(),
            server_url: "https://preloop.example".to_owned(),
            registration_token_env: token_env.clone(),
            labels: vec!["self-hosted".to_owned(), "linux".to_owned()],
            cpus: 2,
            memory_mib: 256,
            storage_gib: 10,
        };
        Self {
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
        &vec![("PRELOOP_RUNNER_TOKEN".to_owned(), fixture.token_env.clone())]
    );
    assert!(configure
        .0
        .iter()
        .all(|argument| argument != &fixture.token));
    assert!(configure
        .1
        .iter()
        .all(|(key, value)| key != &fixture.token && value != &fixture.token));
}

#[tokio::test]
async fn completed_runner_is_deleted_before_slot_is_replenished() {
    let fixture = Fixture::new("replenish", true);
    let provider = Arc::new(RecordingVmProvider::with_machines(
        &[],
        vec![RunAction::Complete, RunAction::Wait],
    ));
    let pool = RunnerPool::new(provider.clone(), fixture.config.clone()).unwrap();
    run_until_cancelled(pool, &provider, CancellationToken::new(), 2).await;

    let events = provider.snapshot().await.events;
    let runner = fixture.config.name_prefix.clone() + "-0";
    let first_run = events
        .iter()
        .position(|event| matches!(event, Event::Exec(name, argv) if name == &runner && argv.get(1).is_some_and(|arg| arg == "run")))
        .unwrap();
    let second_create = events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            (index > first_run && matches!(event, Event::Create(name) if name == &runner))
                .then_some(index)
        })
        .next()
        .unwrap();
    assert!(events[first_run + 1..second_create]
        .iter()
        .any(|event| matches!(event, Event::Delete(name) if name == &runner)));
    assert!(
        events
            .iter()
            .filter(|event| matches!(event, Event::Create(name) if name == &runner))
            .count()
            >= 2
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

    let runner = fixture.config.name_prefix.clone() + "-0";
    let snapshot = provider.snapshot().await;
    assert_eq!(snapshot.machines.get(&runner), None);
    assert!(snapshot
        .events
        .iter()
        .any(|event| matches!(event, Event::Delete(name) if name == &runner)));
}
