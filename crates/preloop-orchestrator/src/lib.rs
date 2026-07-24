//! SmolVM-backed ephemeral runner pool for Preloop CI.

use preloop_vm::{
    MachineName, MachineSpec, MachineState, NetworkPolicy, SmolVmProvider, VmError, VmProvider,
    VolumeMount,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Local ephemeral-runner pool configuration.
#[derive(Debug, Clone)]
pub struct RunnerPoolConfig {
    /// Number of runners polling concurrently.
    pub size: usize,
    /// Prefix used for owned SmolVM names.
    pub name_prefix: String,
    /// Base OCI image used for one-time tool installation.
    pub base_image: String,
    /// Host path stem for the reusable packed VM artifact.
    pub artifact_stem: PathBuf,
    /// Host directory containing the Linux `aksh-runner` executable.
    pub runner_bundle: PathBuf,
    /// Runner executable filename within `runner_bundle`.
    pub runner_binary_name: String,
    /// Guest-visible control-plane URL.
    pub server_url: String,
    /// Host environment variable containing the registration credential.
    pub registration_token_env: String,
    /// Runner labels advertised to the scheduler.
    pub labels: Vec<String>,
    /// vCPUs per runner.
    pub cpus: u16,
    /// Memory per runner in MiB.
    pub memory_mib: u32,
    /// Storage per runner in GiB.
    pub storage_gib: u32,
}

impl RunnerPoolConfig {
    /// Validate configuration before changing machine state.
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        if self.size == 0 || self.size > 64 {
            return Err(OrchestratorError::Config(
                "runner pool size must be between 1 and 64".into(),
            ));
        }
        MachineName::new(format!("{}-0", self.name_prefix))?;
        if self.base_image.trim().is_empty() || self.server_url.trim().is_empty() {
            return Err(OrchestratorError::Config(
                "base image and server URL are required".into(),
            ));
        }
        if !self.runner_bundle.is_absolute() || !self.runner_bundle.is_dir() {
            return Err(OrchestratorError::Config(format!(
                "runner bundle does not exist: {}",
                self.runner_bundle.display()
            )));
        }
        if self.runner_binary_name.contains('/') || self.runner_binary_name.is_empty() {
            return Err(OrchestratorError::Config(
                "runner binary name must be a filename".into(),
            ));
        }
        if std::env::var_os(&self.registration_token_env).is_none() {
            return Err(OrchestratorError::Config(format!(
                "registration token environment variable `{}` is not set",
                self.registration_token_env
            )));
        }
        Ok(())
    }

    fn artifact_payload(&self) -> PathBuf {
        let mut value = self.artifact_stem.as_os_str().to_owned();
        value.push(".smolmachine");
        PathBuf::from(value)
    }
}

/// Runner-pool lifecycle error.
#[derive(Debug, Error)]
pub enum OrchestratorError {
    /// Invalid pool configuration.
    #[error("invalid runner pool configuration: {0}")]
    Config(String),
    /// VM provider failure.
    #[error(transparent)]
    Vm(#[from] VmError),
    /// Host filesystem failure.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// One or more runner slots exited unexpectedly.
    #[error("runner pool stopped unexpectedly: {0}")]
    Pool(String),
}

/// Supervises disposable one-job runners backed by a reusable packed VM image.
pub struct RunnerPool<P: VmProvider = SmolVmProvider> {
    provider: Arc<P>,
    config: RunnerPoolConfig,
}

impl<P: VmProvider + 'static> RunnerPool<P> {
    /// Construct a runner pool.
    pub fn new(provider: Arc<P>, config: RunnerPoolConfig) -> Result<Self, OrchestratorError> {
        config.validate()?;
        Ok(Self { provider, config })
    }

    /// Prepare the immutable runner image once, then supervise all slots until cancellation.
    pub async fn run(&self, shutdown: CancellationToken) -> Result<(), OrchestratorError> {
        self.prepare_artifact().await?;
        self.remove_stale_machines().await?;

        let mut slots = JoinSet::new();
        for slot in 0..self.config.size {
            let provider = self.provider.clone();
            let config = self.config.clone();
            let slot_shutdown = shutdown.child_token();
            slots.spawn(async move { run_slot(provider, config, slot, slot_shutdown).await });
        }

        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {}
            result = slots.join_next() => {
                shutdown.cancel();
                match result {
                    Some(Ok(Err(error))) => return Err(error),
                    Some(Err(error)) => return Err(OrchestratorError::Pool(error.to_string())),
                    Some(Ok(Ok(()))) => return Err(OrchestratorError::Pool("runner slot exited".into())),
                    None => return Err(OrchestratorError::Pool("runner pool had no slots".into())),
                }
            }
        }

        while slots.join_next().await.is_some() {}
        self.remove_stale_machines().await?;
        Ok(())
    }

    async fn prepare_artifact(&self) -> Result<(), OrchestratorError> {
        let payload = self.config.artifact_payload();
        if payload.is_file() {
            return Ok(());
        }
        if let Some(parent) = self.config.artifact_stem.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let name = MachineName::new(format!("{}-builder", self.config.name_prefix))?;
        if self.provider.status(&name).await? != MachineState::Missing {
            self.provider.delete(&name).await?;
        }
        let spec = MachineSpec {
            name: name.clone(),
            image: self.config.base_image.clone(),
            cpus: self.config.cpus,
            memory_mib: self.config.memory_mib,
            storage_gib: self.config.storage_gib,
            network: NetworkPolicy::Unrestricted,
            volumes: Vec::new(),
        };
        self.provider.create(&spec).await?;
        self.provider.start(&name).await?;
        for command in [
            vec!["apt-get", "update", "-qq"],
            vec![
                "apt-get",
                "install",
                "-y",
                "-qq",
                "--no-install-recommends",
                "git",
                "curl",
                "ca-certificates",
                "nodejs",
            ],
        ] {
            let argv = command.into_iter().map(str::to_owned).collect::<Vec<_>>();
            if let Err(error) = self.provider.exec(&name, &argv).await {
                let _ = self.provider.delete(&name).await;
                return Err(error.into());
            }
        }
        self.provider.stop(&name).await?;
        self.provider
            .pack(&name, &self.config.artifact_stem)
            .await?;
        self.provider.delete(&name).await?;
        if !payload.is_file() {
            return Err(OrchestratorError::Config(format!(
                "smolvm did not create expected artifact {}",
                payload.display()
            )));
        }
        Ok(())
    }

    async fn remove_stale_machines(&self) -> Result<(), OrchestratorError> {
        for name in self.provider.list().await? {
            if name
                .as_str()
                .starts_with(&format!("{}-", self.config.name_prefix))
            {
                if let Err(error) = self.provider.delete(&name).await {
                    warn!(machine = name.as_str(), %error, "failed to delete stale Preloop runner");
                }
            }
        }
        Ok(())
    }
}

async fn run_slot<P: VmProvider + 'static>(
    provider: Arc<P>,
    config: RunnerPoolConfig,
    slot: usize,
    shutdown: CancellationToken,
) -> Result<(), OrchestratorError> {
    let name = MachineName::new(format!("{}-{slot}", config.name_prefix))?;
    while !shutdown.is_cancelled() {
        if let Err(error) = run_one_runner(provider.clone(), &config, &name, shutdown.clone()).await
        {
            warn!(machine = name.as_str(), %error, "ephemeral runner failed; replenishing slot");
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
        }
    }
    let _ = provider.delete(&name).await;
    Ok(())
}

async fn run_one_runner<P: VmProvider + 'static>(
    provider: Arc<P>,
    config: &RunnerPoolConfig,
    name: &MachineName,
    shutdown: CancellationToken,
) -> Result<(), OrchestratorError> {
    if provider.status(name).await? != MachineState::Missing {
        provider.delete(name).await?;
    }
    let spec = MachineSpec {
        name: name.clone(),
        image: config.artifact_payload().display().to_string(),
        cpus: config.cpus,
        memory_mib: config.memory_mib,
        storage_gib: config.storage_gib,
        network: NetworkPolicy::Unrestricted,
        volumes: vec![VolumeMount {
            host: config.runner_bundle.clone(),
            guest: PathBuf::from("/opt/preloop/bin"),
            read_only: true,
        }],
    };
    provider.create(&spec).await?;
    provider.start(name).await?;

    let runner = format!("/opt/preloop/bin/{}", config.runner_binary_name);
    let labels = config.labels.join(",");
    let configure = vec![
        runner.clone(),
        "configure".into(),
        "--url".into(),
        config.server_url.clone(),
        "--name".into(),
        name.as_str().into(),
        "--labels".into(),
        labels,
        "--runner-root".into(),
        "/var/lib/preloop-runner".into(),
        "--unattended".into(),
        "--replace".into(),
        "--ephemeral".into(),
        "--no-externals".into(),
    ];
    provider
        .exec_with_secret_env(
            name,
            &configure,
            &[(
                "PRELOOP_RUNNER_TOKEN".into(),
                config.registration_token_env.clone(),
            )],
        )
        .await?;

    info!(machine = name.as_str(), "ephemeral runner ready");
    let run = vec![
        runner,
        "run".into(),
        "--once".into(),
        "--runner-root".into(),
        "/var/lib/preloop-runner".into(),
    ];
    let run_provider = provider.clone();
    let run_name = name.clone();
    let mut run_task = tokio::spawn(async move { run_provider.exec(&run_name, &run).await });
    let result = tokio::select! {
        _ = shutdown.cancelled() => {
            // Killing the host-side `smolvm machine exec` process does not
            // terminate the guest command. Abort the wrapper first, then stop
            // the VM so deletion cannot wait indefinitely on a live listener.
            run_task.abort();
            let _ = run_task.await;
            provider.stop(name).await.map_err(OrchestratorError::from)
        },
        result = &mut run_task => match result {
            Ok(result) => result.map(|_| ()).map_err(OrchestratorError::from),
            Err(error) => Err(OrchestratorError::Pool(error.to_string())),
        },
    };
    let delete_result = provider.delete(name).await;
    result?;
    delete_result?;
    Ok(())
}

/// Return the runner artifact payload generated for an output stem.
pub fn artifact_payload(stem: &Path) -> PathBuf {
    let mut value = stem.as_os_str().to_owned();
    value.push(".smolmachine");
    PathBuf::from(value)
}
