//! Secret delivery through the pool adapter against a real SmolVM machine.
//!
//! Runs only with `PRELOOP_SANDBOX_E2E_SMOLVM=1`.

use std::sync::Arc;
use std::time::Duration;

use preloop_orchestrator::fleet::SandboxMachines;
use preloop_sandbox::agent::{Agent, AgentConfig, ProviderLink};
use preloop_sandbox::api::Requirements;
use preloop_sandbox::drivers::smolvm::SmolvmDriver;
use preloop_sandbox::drivers::smolvm::cli::SmolVmProvider;
use preloop_sandbox::drivers::smolvm::cli::{
    MachineName, MachineSpec, NetworkPolicy, ProviderCapabilities, SecretSource, VmProvider,
};
use preloop_sandbox::store;
use preloop_sandbox::{Driver, InProcess, SandboxApi, Service, ServiceConfig};
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn secrets_reach_the_guest_without_entering_argv() {
    if std::env::var("PRELOOP_SANDBOX_E2E_SMOLVM").as_deref() != Ok("1") {
        eprintln!("skipped: set PRELOOP_SANDBOX_E2E_SMOLVM=1");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let service = Service::open(
        store::open("memory:").await.unwrap(),
        ServiceConfig::new(dir.path().join("blobs")),
    )
    .await
    .unwrap();
    let shutdown = CancellationToken::new();
    let driver = Arc::new(SmolvmDriver::new(Default::default(), 0));
    let agent = Agent::new(
        Arc::new(service.clone()) as Arc<dyn ProviderLink>,
        vec![driver as Arc<dyn Driver>],
        AgentConfig::new("e2e-secrets", dir.path().join("scratch")),
    );
    tokio::spawn(agent.run(None, shutdown.clone()));
    tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
    while service.list_providers().is_empty() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let provider_id = service
        .list_providers()
        .first()
        .map(|provider| provider.id.clone())
        .expect("agent registered");
    let machines = SandboxMachines::new(
        Arc::new(InProcess(service)) as Arc<dyn SandboxApi>,
        None,
        Some(provider_id),
        Requirements::default(),
        ProviderCapabilities::default(),
    );
    // Unique per run: a failed run leaves its machine behind for triage.
    let name = MachineName::new(format!("e2e-secret-{}", std::process::id())).unwrap();
    machines
        .create(&MachineSpec {
            name: name.clone(),
            image: "alpine:3.20".into(),
            cpus: 1,
            memory_mib: 512,
            storage_gib: 1,
            overlay_gib: None,
            network: NetworkPolicy::Unrestricted,
            volumes: Vec::new(),
            sockets: Vec::new(),
            dns: Some("8.8.8.8".into()),
            rosetta: false,
            role: Default::default(),
            guest_volumes: None,
            workload_init: None,
        })
        .await
        .unwrap();
    machines.start(&name).await.unwrap();

    // A host file keeps this test parallel-safe: no process env mutation.
    let secret_file = dir.path().join("e2e-secret");
    std::fs::write(&secret_file, "e2e-s3cr3t-value").unwrap();
    let output = machines
        .exec_with_secret_env(
            &name,
            &["sh".into(), "-c".into(), "printf %s \"$TOKEN\"".into()],
            &[("TOKEN".into(), SecretSource::HostFile(secret_file))],
        )
        .await
        .expect("secret exec");
    assert_eq!(output.stdout, b"e2e-s3cr3t-value");
    // The value must not survive anywhere the guest can read: no process
    // command line carries it, and the carrier file is gone.
    let probe = machines
        .exec(
            &name,
            &[
                "sh".into(),
                "-c".into(),
                // The bracket keeps this probe's own command line from
                // matching the pattern it searches for. The carrier is
                // staged in /var/tmp, not /tmp.
                "printf '%s %s\\n' \"$(ps -eo args= | grep -c 'e2e-s3cr3t-valu[e]')\" \
                 \"$(ls -a /var/tmp/.preloop-secret-env-* 2>/dev/null | wc -l)\""
                    .into(),
            ],
        )
        .await
        .expect("probe");
    assert_eq!(
        String::from_utf8_lossy(&probe.stdout).trim(),
        "0 0",
        "the secret leaked into the guest"
    );
    machines.delete(&name).await.unwrap();
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_packed_artifact_becomes_a_runnable_machine() {
    if std::env::var("PRELOOP_SANDBOX_E2E_SMOLVM").as_deref() != Ok("1") {
        eprintln!("skipped: set PRELOOP_SANDBOX_E2E_SMOLVM=1");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let service = Service::open(
        store::open("memory:").await.unwrap(),
        ServiceConfig::new(dir.path().join("blobs")),
    )
    .await
    .unwrap();
    let shutdown = CancellationToken::new();
    let provider = Arc::new(SmolVmProvider::default());
    let driver = Arc::new(SmolvmDriver::new((*provider).clone(), 0));
    let agent = Agent::new(
        Arc::new(service.clone()) as Arc<dyn ProviderLink>,
        vec![driver as Arc<dyn Driver>],
        AgentConfig::new("e2e-pack", dir.path().join("scratch")),
    );
    tokio::spawn(agent.run(None, shutdown.clone()));
    tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
    while service.list_providers().is_empty() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let provider_id = service
        .list_providers()
        .first()
        .map(|provider| provider.id.clone())
        .expect("agent registered");
    let machines = SandboxMachines::new(
        Arc::new(InProcess(service)) as Arc<dyn SandboxApi>,
        Some(provider.clone()),
        Some(provider_id),
        Requirements::default(),
        ProviderCapabilities::default(),
    );
    // Bake a tiny base into a packed artifact, exactly as the CI golden does.
    let base = MachineName::new(format!("e2e-pack-base-{}", std::process::id())).unwrap();
    machines
        .create(&MachineSpec {
            name: base.clone(),
            image: "alpine:3.20".into(),
            cpus: 1,
            memory_mib: 512,
            storage_gib: 1,
            overlay_gib: None,
            network: NetworkPolicy::Unrestricted,
            volumes: Vec::new(),
            sockets: Vec::new(),
            dns: Some("8.8.8.8".into()),
            rosetta: false,
            role: Default::default(),
            guest_volumes: None,
            workload_init: None,
        })
        .await
        .unwrap();
    machines.start(&base).await.unwrap();
    machines
        .exec(
            &base,
            &["sh".into(), "-c".into(), "echo baked > /etc/marker".into()],
        )
        .await
        .unwrap();
    // Packing needs the machine powered off, exactly as the pool stops it
    // before `pack create --from-vm`.
    machines.stop(&base).await.expect("stop before pack");
    let pack = dir.path().join("tiny.smolmachine");
    machines.pack(&base, &pack).await.unwrap();
    machines.delete(&base).await.unwrap();
    assert!(pack.is_file(), "pack artifact missing");

    // The CI golden path: create from the pack, start forkable, exec.
    let golden = MachineName::new(format!("e2e-pack-golden-{}", std::process::id())).unwrap();
    machines
        .create(&MachineSpec {
            name: golden.clone(),
            image: pack.display().to_string(),
            cpus: 1,
            memory_mib: 512,
            storage_gib: 1,
            overlay_gib: None,
            network: NetworkPolicy::Unrestricted,
            volumes: Vec::new(),
            sockets: Vec::new(),
            dns: Some("8.8.8.8".into()),
            rosetta: false,
            role: Default::default(),
            guest_volumes: None,
            workload_init: None,
        })
        .await
        .expect("create from packed artifact");
    machines
        .start(&golden)
        .await
        .expect("start the packed golden");
    let out = machines
        .exec(&golden, &["cat".into(), "/etc/marker".into()])
        .await
        .expect("exec in the packed golden");
    assert_eq!(out.stdout, b"baked\n");
    machines.delete(&golden).await.unwrap();
    shutdown.cancel();
}
