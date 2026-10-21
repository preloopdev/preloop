//! Service/agent conformance over the process-backed fake driver, driven
//! both in-process and across a real HTTP server with a remote agent.

use std::sync::Arc;
use std::time::Duration;

use preloop_sandbox::agent::{Agent, AgentConfig, HostAccess, ProviderLink, RemoteLink};
use preloop_sandbox::api::*;
use preloop_sandbox::drivers::fake::FakeDriver;
use preloop_sandbox::server::{Auth, router};
use preloop_sandbox::store::{self, Kind, Store};
use preloop_sandbox::{Code, Driver, SandboxClient, Service, ServiceConfig};
use tokio_util::sync::CancellationToken;

struct Harness {
    service: Arc<Service>,
    fake: Arc<FakeDriver>,
    shutdown: CancellationToken,
    _dir: tempfile::TempDir,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// A syntactically valid digest-pinned artifact for the fake driver, which
/// boots nothing: these tests exercise the service, not a registry.
const TEST_DIGEST: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

fn oci_artifact() -> Artifact {
    Artifact {
        format: Artifact::OCI_IMAGE.into(),
        arch: host_arch().into(),
        reference: format!("ghcr.io/preloop/alpine@{TEST_DIGEST}"),
        digest: TEST_DIGEST.into(),
        size_bytes: 0,
    }
}

fn template(name: &str) -> TemplateSpec {
    TemplateSpec {
        name: name.into(),
        os: "linux".into(),
        artifacts: vec![oci_artifact()],
        ..TemplateSpec::default()
    }
}

async fn harness_with(
    store: Arc<dyn Store>,
    dir: tempfile::TempDir,
    caps: Capabilities,
) -> Harness {
    let host = dir.path().join("host");
    harness_with_host(store, dir, host, caps).await
}

/// `host` is the fake driver's root: a restart test must reuse the machine
/// directory the previous process created, or reconcile sees it as lost.
async fn harness_with_host(
    store: Arc<dyn Store>,
    dir: tempfile::TempDir,
    host: std::path::PathBuf,
    caps: Capabilities,
) -> Harness {
    harness_with_host_access(store, dir, host, caps, HostAccess::default()).await
}

async fn harness_with_access(
    store: Arc<dyn Store>,
    dir: tempfile::TempDir,
    caps: Capabilities,
    access: HostAccess,
) -> Harness {
    let host = dir.path().join("host");
    harness_with_host_access(store, dir, host, caps, access).await
}

async fn harness_with_host_access(
    store: Arc<dyn Store>,
    dir: tempfile::TempDir,
    host: std::path::PathBuf,
    caps: Capabilities,
    access: HostAccess,
) -> Harness {
    let mut config = ServiceConfig::new(dir.path().join("blobs"));
    config.acquire_wait = Duration::from_secs(10);
    let service = Service::open(store, config).await.unwrap();
    let fake = Arc::new(FakeDriver::new("fake", host, caps));
    let shutdown = CancellationToken::new();
    let mut agent_config = AgentConfig::new("host-a", dir.path().join("scratch"));
    agent_config.reconcile = Duration::from_millis(300);
    let agent = Agent::new(
        Arc::new(service.clone()) as Arc<dyn ProviderLink>,
        vec![fake.clone() as Arc<dyn Driver>],
        agent_config.with_access(access),
    );
    tokio::spawn(agent.run(shutdown.clone()));
    tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
    // A restored provider row is listed before the new agent registers, so
    // wait for the registration itself (generation bump), not for presence.
    let generation = service
        .list_providers()
        .iter()
        .map(|provider| provider.generation)
        .max()
        .unwrap_or(0);
    wait_for(|| {
        service
            .list_providers()
            .iter()
            .any(|provider| provider.generation > generation)
    })
    .await;
    Harness {
        service,
        fake,
        shutdown,
        _dir: dir,
    }
}

async fn harness() -> Harness {
    let dir = tempfile::tempdir().unwrap();
    harness_with(
        store::open("memory:").await.unwrap(),
        dir,
        FakeDriver::full_capabilities(),
    )
    .await
}

async fn wait_for(mut condition: impl FnMut() -> bool) {
    for _ in 0..200 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("condition not reached");
}

fn acquire(key: &str, pool: &str) -> AcquireRequest {
    AcquireRequest {
        idempotency_key: key.into(),
        pool: pool.into(),
        purpose: Purpose::Runner,
        ..AcquireRequest::default()
    }
}

#[tokio::test]
async fn acquire_is_idempotent_and_injects_identity() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    h.service
        .put_pool(PoolSpec {
            name: "linux".into(),
            template: "alpine".into(),
            ..PoolSpec::default()
        })
        .await
        .unwrap();
    let mut request = acquire("run-1/job-a/1", "linux");
    request
        .inject
        .env
        .insert("RUNNER_TOKEN".into(), "tok-123".into());
    request.inject.files.push(FileSpec {
        path: "/run/preloop/bootstrap".into(),
        data: b"hello".to_vec(),
        mode: 0o600,
    });
    let first = h.service.acquire(request.clone()).await.unwrap();
    let second = h.service.acquire(request).await.unwrap();
    assert_eq!(first.lease.id, second.lease.id);
    assert_eq!(first.sandbox.id, second.sandbox.id);
    assert_eq!(first.sandbox.state, SandboxState::Leased);
    // Lease env is merged into exec; injected file is present.
    let (code, stdout, _) = h
        .service
        .exec_collect(ExecRequest {
            sandbox_id: first.sandbox.id.clone(),
            lease_id: first.lease.id.clone(),
            argv: vec![
                "sh".into(),
                "-c".into(),
                "printf %s \"$RUNNER_TOKEN\"; cat \"$SANDBOX_ROOT/run/preloop/bootstrap\"; exit 3"
                    .into(),
            ],
            ..ExecRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(code, 3);
    assert_eq!(stdout, b"tok-123hello");
    // Release destroys the machine; the same key then starts over.
    h.service
        .release(ReleaseRequest {
            lease_id: first.lease.id.clone(),
            mode: ReleaseMode::Destroy,
            ..ReleaseRequest::default()
        })
        .await
        .unwrap();
    wait_for(|| h.service.list().is_empty()).await;
    assert!(h.fake.inventory().await.unwrap().is_empty());
}

#[tokio::test]
async fn warm_pool_refills_and_serves_warm_members() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    h.service
        .put_pool(PoolSpec {
            name: "warm".into(),
            template: "alpine".into(),
            warm: 2,
            ..PoolSpec::default()
        })
        .await
        .unwrap();
    let ready = |service: &Service| {
        service
            .list()
            .iter()
            .filter(|s| s.state == SandboxState::Ready)
            .count()
    };
    wait_for(|| ready(&h.service) == 2).await;
    let warm: Vec<String> = h.service.list().into_iter().map(|s| s.id).collect();
    let lease = h.service.acquire(acquire("k1", "warm")).await.unwrap();
    assert!(warm.contains(&lease.sandbox.id), "served a warm member");
    // The pool replaces the claimed member.
    wait_for(|| ready(&h.service) == 2).await;
    assert_eq!(h.service.list().len(), 3);
}

#[tokio::test]
async fn placement_rejects_unsatisfiable_requirements_by_name() {
    let h = harness().await;
    let mut spec = template("mac");
    spec.os = "macos".into();
    h.service.put_template(spec).await.unwrap();
    let error = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "k".into(),
            template: "mac".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::FailedPrecondition);
    assert!(error.message.contains("os=macos"), "{}", error.message);
    let error = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "k2".into(),
            template: "alpine-missing".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::NotFound);
}

#[tokio::test]
async fn placement_rejects_a_shape_the_provider_cannot_host() {
    let dir = tempfile::tempdir().unwrap();
    let mut caps = FakeDriver::full_capabilities();
    caps.max_vcpus = 2;
    caps.max_memory_mib = 2048;
    let h = harness_with(store::open("memory:").await.unwrap(), dir, caps).await;
    h.service.put_template(template("alpine")).await.unwrap();

    let error = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "too-many-cpus".into(),
            template: "alpine".into(),
            shape: Shape {
                cpus: 4,
                ..Shape::default()
            },
            ..AcquireRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::FailedPrecondition);
    assert!(
        error.message.contains("shape=4cpus/0mib"),
        "{}",
        error.message
    );

    let error = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "too-much-memory".into(),
            template: "alpine".into(),
            shape: Shape {
                memory_mib: 4096,
                ..Shape::default()
            },
            ..AcquireRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::FailedPrecondition);
    assert!(
        error.message.contains("shape=0cpus/4096mib"),
        "{}",
        error.message
    );

    // A shape inside both ceilings still places.
    let lease = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "fits".into(),
            template: "alpine".into(),
            shape: Shape {
                cpus: 2,
                memory_mib: 2048,
                ..Shape::default()
            },
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(lease.sandbox.template, "alpine");
}

#[tokio::test]
async fn unsupported_operations_fail_instead_of_being_emulated() {
    let dir = tempfile::tempdir().unwrap();
    let mut caps = FakeDriver::full_capabilities();
    caps.pause = false;
    caps.snapshot = false;
    let h = harness_with(store::open("memory:").await.unwrap(), dir, caps).await;
    h.service.put_template(template("alpine")).await.unwrap();
    let lease = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "k".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let error = h
        .service
        .pause(&lease.sandbox.id, &lease.lease.id)
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::FailedPrecondition);
    let error = h
        .service
        .snapshot(SnapshotRequest {
            sandbox_id: lease.sandbox.id.clone(),
            lease_id: lease.lease.id.clone(),
            name: "x".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::FailedPrecondition);
    // A build (snapshot-capable) acquire cannot be placed here at all.
    let error = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "b".into(),
            template: "alpine".into(),
            purpose: Purpose::Build,
            ..AcquireRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::FailedPrecondition);
}

#[tokio::test]
async fn snapshot_becomes_a_template_that_starts_with_the_captured_state() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let build = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "bake".into(),
            template: "alpine".into(),
            purpose: Purpose::Build,
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let (code, _, _) = h
        .service
        .exec_collect(ExecRequest {
            sandbox_id: build.sandbox.id.clone(),
            lease_id: build.lease.id.clone(),
            argv: vec![
                "sh".into(),
                "-c".into(),
                "echo baked > \"$SANDBOX_ROOT/marker\"".into(),
            ],
            ..ExecRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(code, 0);
    let golden = h
        .service
        .snapshot(SnapshotRequest {
            sandbox_id: build.sandbox.id.clone(),
            lease_id: build.lease.id.clone(),
            name: "golden".into(),
        })
        .await
        .unwrap();
    assert!(!golden.native_ref.is_empty());
    assert_eq!(
        golden.spec.snapshot_of.as_deref(),
        Some(build.sandbox.id.as_str())
    );
    // The source is consumed.
    assert!(h.service.get(&build.sandbox.id, &build.lease.id).is_err());
    h.service
        .put_pool(PoolSpec {
            name: "from-golden".into(),
            template: "golden".into(),
            ..PoolSpec::default()
        })
        .await
        .unwrap();
    for key in ["a", "b"] {
        let lease = h
            .service
            .acquire(acquire(key, "from-golden"))
            .await
            .unwrap();
        let (_, stdout, _) = h
            .service
            .exec_collect(ExecRequest {
                sandbox_id: lease.sandbox.id.clone(),
                lease_id: lease.lease.id.clone(),
                argv: vec!["cat".into(), "marker".into()],
                ..ExecRequest::default()
            })
            .await
            .unwrap();
        assert_eq!(stdout, b"baked\n");
    }
    // Reconcile never treats the snapshot as an orphan.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let lease = h
        .service
        .acquire(acquire("c", "from-golden"))
        .await
        .unwrap();
    assert_eq!(lease.sandbox.state, SandboxState::Leased);
}

#[tokio::test]
async fn create_failure_surfaces_and_same_key_retries_cleanly() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    h.service
        .put_pool(PoolSpec {
            name: "p".into(),
            template: "alpine".into(),
            ..PoolSpec::default()
        })
        .await
        .unwrap();
    h.fake
        .fail_creates
        .store(1, std::sync::atomic::Ordering::Release);
    let error = h.service.acquire(acquire("k", "p")).await.unwrap_err();
    assert_eq!(error.code, Code::Unavailable);
    assert!(error.message.contains("injected create failure"));
    let lease = h.service.acquire(acquire("k", "p")).await.unwrap();
    assert_eq!(lease.sandbox.state, SandboxState::Leased);
}

#[tokio::test]
async fn expired_lease_destroys_and_preserve_holds_until_ttl() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let mut request = AcquireRequest {
        idempotency_key: "short".into(),
        template: "alpine".into(),
        ttl_secs: 1,
        ..AcquireRequest::default()
    };
    h.service.acquire(request.clone()).await.unwrap();
    wait_for(|| h.service.list().is_empty()).await;

    request.idempotency_key = "keep".into();
    request.ttl_secs = 60;
    let lease = h.service.acquire(request).await.unwrap();
    h.service
        .release(ReleaseRequest {
            lease_id: lease.lease.id.clone(),
            mode: ReleaseMode::Preserve,
            preserve_ttl_secs: 1,
        })
        .await
        .unwrap();
    let preserved = h.service.get(&lease.sandbox.id, &lease.lease.id).unwrap();
    assert_eq!(preserved.state, SandboxState::Preserved);
    // Still usable for debugging while preserved.
    let (code, _, _) = h
        .service
        .exec_collect(ExecRequest {
            sandbox_id: lease.sandbox.id.clone(),
            lease_id: lease.lease.id.clone(),
            argv: vec!["true".into()],
            ..ExecRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(code, 0);
    wait_for(|| h.service.get(&lease.sandbox.id, &lease.lease.id).is_err()).await;
}

/// Stats counts follow the lease: an expired lease destroys the machine and
/// the provider's sandbox list drops back to empty.
#[tokio::test]
async fn stats_follow_lease_expiry() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let lease = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "stats-ttl".into(),
            template: "alpine".into(),
            ttl_secs: 1,
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let stats = h.service.stats();
    let provider = stats
        .providers
        .iter()
        .find(|provider| provider.provider_id == lease.sandbox.provider_id)
        .expect("the acquiring provider is listed");
    assert_eq!(provider.sandboxes.len(), 1);
    assert_eq!(provider.sandboxes[0].sandbox.id, lease.sandbox.id);
    assert_eq!(provider.sandboxes[0].sandbox.state, SandboxState::Leased);
    wait_for(|| {
        h.service
            .stats()
            .providers
            .iter()
            .all(|provider| provider.sandboxes.is_empty())
    })
    .await;
    assert!(h.service.get(&lease.sandbox.id, &lease.lease.id).is_err());
}

#[tokio::test]
async fn reconcile_destroys_orphans_and_fails_lost_sandboxes() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let lease = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "k".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    // An orphan the service never created appears on the host.
    let orphan = h
        .fake
        .create(&CreateWork {
            sandbox_id: "sbx-orphan".into(),
            driver: "fake".into(),
            ..CreateWork::default()
        })
        .await
        .unwrap();
    // The leased machine disappears behind the service's back.
    h.fake.destroy(&lease.sandbox.native_ref).await.unwrap();
    wait_for(|| {
        h.service
            .get(&lease.sandbox.id, &lease.lease.id)
            .is_ok_and(|s| s.state == SandboxState::Failed)
    })
    .await;
    for _ in 0..100 {
        if h.fake
            .inventory()
            .await
            .unwrap()
            .iter()
            .all(|o| o.native_ref != orphan)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("orphan was not destroyed");
}

/// Reconcile results are visible through Stats: a machine that disappeared
/// behind the service's back becomes `Failed` instead of staying `Leased`.
#[tokio::test]
async fn stats_follow_reconcile_state_changes() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let lease = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "stats-lost".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let state_of = |service: &Service, id: &str| {
        service
            .stats()
            .providers
            .into_iter()
            .flat_map(|provider| provider.sandboxes)
            .find(|sandbox| sandbox.sandbox.id == id)
            .map(|sandbox| sandbox.sandbox.state)
    };
    assert_eq!(
        state_of(&h.service, &lease.sandbox.id),
        Some(SandboxState::Leased)
    );
    // The leased machine disappears behind the service's back.
    h.fake.destroy(&lease.sandbox.native_ref).await.unwrap();
    wait_for(|| state_of(&h.service, &lease.sandbox.id) == Some(SandboxState::Failed)).await;
    // The count follows the state: the sandbox is still tracked, once, as
    // failed.
    let sandboxes: Vec<_> = h
        .service
        .stats()
        .providers
        .into_iter()
        .flat_map(|provider| provider.sandboxes)
        .filter(|sandbox| sandbox.sandbox.id == lease.sandbox.id)
        .collect();
    assert_eq!(sandboxes.len(), 1);
    assert_eq!(sandboxes[0].sandbox.state, SandboxState::Failed);
}

#[tokio::test]
async fn stale_generation_is_fenced() {
    let h = harness().await;
    let first = h
        .service
        .register(RegisterRequest {
            name: "host-b".into(),
            drivers: vec![],
        })
        .await
        .unwrap();
    let second = h
        .service
        .register(RegisterRequest {
            name: "host-b".into(),
            drivers: vec![],
        })
        .await
        .unwrap();
    assert_eq!(first.provider_id, second.provider_id);
    assert_eq!(second.generation, first.generation + 1);
    let error = h
        .service
        .poll(PollRequest {
            provider_id: first.provider_id.clone(),
            generation: first.generation,
            wait_secs: 0,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::Aborted);
    let error = h
        .service
        .reconcile(ReconcileRequest {
            provider_id: first.provider_id,
            generation: first.generation,
            sandboxes: vec![],
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::Aborted);
}

#[tokio::test]
async fn state_survives_restart_from_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sandbox.db");
    let pool_name;
    {
        let store = store::open(db.to_str().unwrap()).await.unwrap();
        let h = harness_with(
            store,
            tempfile::tempdir().unwrap(),
            FakeDriver::full_capabilities(),
        )
        .await;
        h.service.put_template(template("alpine")).await.unwrap();
        pool_name = h
            .service
            .put_pool(PoolSpec {
                name: "p".into(),
                template: "alpine".into(),
                ..PoolSpec::default()
            })
            .await
            .unwrap()
            .name;
        h.service.acquire(acquire("k", &pool_name)).await.unwrap();
    }
    let store = store::open(db.to_str().unwrap()).await.unwrap();
    assert_eq!(store.load(Kind::Leases).await.unwrap().len(), 1);
    let restored = Service::open(store, ServiceConfig::new(dir.path().join("blobs2")))
        .await
        .unwrap();
    assert_eq!(restored.list_pools().len(), 1);
    assert_eq!(restored.list_templates().len(), 1);
    assert_eq!(restored.list().len(), 1);
}

/// An old agent that only sends `provider_id`/`generation` is still
/// accepted: absent fields never fabricate a host sample, and a report with
/// no runtime facts replaces the previous report wholesale (never merges)
/// while a host sample it did not carry survives.
#[tokio::test]
async fn old_agent_heartbeat_without_runtime_fields_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sandbox.db");
    let sandbox;
    {
        let h = harness_with(
            store::open(db.to_str().unwrap()).await.unwrap(),
            tempfile::tempdir().unwrap(),
            FakeDriver::full_capabilities(),
        )
        .await;
        h.service.put_template(template("alpine")).await.unwrap();
        sandbox = h
            .service
            .acquire(AcquireRequest {
                idempotency_key: "old-agent".into(),
                template: "alpine".into(),
                ..AcquireRequest::default()
            })
            .await
            .unwrap()
            .sandbox;
    }
    // Restart without an agent: the only heartbeats below are the ones this
    // test sends, so their effect is deterministic.
    let service = Service::open(
        store::open(db.to_str().unwrap()).await.unwrap(),
        ServiceConfig::new(dir.path().join("blobs2")),
    )
    .await
    .unwrap();
    let provider = service
        .list_providers()
        .into_iter()
        .find(|provider| provider.id == sandbox.provider_id)
        .expect("the provider row is restored");
    let old_shape = || HeartbeatRequest {
        provider_id: provider.id.clone(),
        generation: provider.generation,
        ..HeartbeatRequest::default()
    };
    let provider_stats = || {
        service
            .stats()
            .providers
            .into_iter()
            .find(|entry| entry.provider_id == provider.id)
            .expect("provider is listed")
    };

    // Never reported anything: succeeds and invents nothing.
    Service::heartbeat(&service, old_shape()).unwrap();
    let first = provider_stats();
    assert_eq!(first.host, HostSample::default());
    assert_eq!(first.host_sample_age_secs, 0);
    assert_eq!(first.sandboxes.len(), 1);
    assert_eq!(first.sandboxes[0].sandbox.id, sandbox.id);
    assert_eq!(first.sandboxes[0].runtime, SandboxRuntimeStats::default());

    // A new agent reports host + runtime facts...
    let reported = SandboxRuntimeStats {
        native_ref: sandbox.native_ref.clone(),
        memory_bytes: Some(64 << 20),
        ..SandboxRuntimeStats::default()
    };
    Service::heartbeat(
        &service,
        HeartbeatRequest {
            provider_id: provider.id.clone(),
            generation: provider.generation,
            host: Some(HostSample {
                mem_total_bytes: Some(8 << 30),
                ..HostSample::default()
            }),
            sandboxes: vec![reported.clone()],
        },
    )
    .unwrap();
    let second = provider_stats();
    assert_eq!(second.host.mem_total_bytes, Some(8 << 30));
    assert_eq!(second.sandboxes[0].runtime, reported);

    // ...and a later old-shape heartbeat clears the runtime report (it is
    // replaced wholesale) but cannot clear a host sample it did not send.
    Service::heartbeat(&service, old_shape()).unwrap();
    let third = provider_stats();
    assert_eq!(third.host.mem_total_bytes, Some(8 << 30));
    assert_eq!(third.sandboxes[0].runtime, SandboxRuntimeStats::default());
}

/// The same flows across a real HTTP server with a remote agent.
#[tokio::test]
async fn remote_agent_and_client_over_connect() {
    use preloop_sandbox::SandboxApi as _;
    let dir = tempfile::tempdir().unwrap();
    let service = Service::open(
        store::open("memory:").await.unwrap(),
        ServiceConfig::new(dir.path().join("blobs")),
    )
    .await
    .unwrap();
    let auth = Auth {
        client_token: Some("client-secret".into()),
        provider_token: Some("provider-secret".into()),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = router(service.clone(), auth);
    tokio::spawn(async move { axum::serve(listener, app).await });
    let shutdown = CancellationToken::new();
    tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
    let fake = Arc::new(FakeDriver::new(
        "fake",
        dir.path().join("host"),
        FakeDriver::full_capabilities(),
    ));
    let agent = Agent::new(
        Arc::new(RemoteLink::new(&base, Some("provider-secret".into()))),
        vec![fake as Arc<dyn Driver>],
        AgentConfig::new("remote-host", dir.path().join("scratch")),
    );
    tokio::spawn(agent.run(shutdown.clone()));

    // Wrong token is rejected.
    let bad = SandboxClient::new(&base, Some("nope".into()));
    assert_eq!(bad.list().await.unwrap_err().code, Code::Unauthenticated);

    let client = SandboxClient::new(&base, Some("client-secret".into()));
    wait_for(|| !Service::list_providers(&service).is_empty()).await;
    client.put_template(template("alpine")).await.unwrap();
    let lease = client
        .acquire(AcquireRequest {
            idempotency_key: "remote".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    // The capability crosses the wire: `List` redacts, a foreign lease is
    // refused, and the holder proceeds.
    assert!(
        client
            .list()
            .await
            .unwrap()
            .iter()
            .all(|sandbox| sandbox.lease_id.is_empty())
    );
    let denied = client
        .get(&lease.sandbox.id, "lse-someone-else")
        .await
        .unwrap_err();
    assert_eq!(denied.code, Code::PermissionDenied);
    assert_eq!(
        client
            .get(&lease.sandbox.id, &lease.lease.id)
            .await
            .unwrap()
            .id,
        lease.sandbox.id
    );
    // Streaming exec with large output crosses batching.
    let (code, stdout, stderr) = client
        .exec_collect(ExecRequest {
            sandbox_id: lease.sandbox.id.clone(),
            lease_id: lease.lease.id.clone(),
            argv: vec![
                "sh".into(),
                "-c".into(),
                "head -c 300000 /dev/zero | tr '\\0' x; echo err >&2".into(),
            ],
            ..ExecRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(code, 0);
    assert_eq!(stdout.len(), 300_000);
    assert_eq!(stderr, b"err\n");

    // Upload a tar through a ticket, then download it back.
    let src = dir.path().join("in.tar");
    {
        let mut builder = tar::Builder::new(std::fs::File::create(&src).unwrap());
        let data = b"payload";
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, "dir/file.txt", &data[..])
            .unwrap();
        builder.finish().unwrap();
    }
    client
        .upload(&lease.sandbox.id, &lease.lease.id, "/work", &src)
        .await
        .unwrap();
    let out = dir.path().join("out.tar");
    client
        .download(&lease.sandbox.id, &lease.lease.id, "/work/dir", &out)
        .await
        .unwrap();
    let mut archive = tar::Archive::new(std::fs::File::open(&out).unwrap());
    let names: Vec<String> = archive
        .entries()
        .unwrap()
        .map(|e| e.unwrap().path().unwrap().to_string_lossy().into_owned())
        .collect();
    assert!(
        names.iter().any(|n| n.ends_with("dir/file.txt")),
        "{names:?}"
    );

    // Exec errors travel as Connect end-of-stream errors.
    let error = client
        .exec_collect(ExecRequest {
            sandbox_id: "sbx-missing".into(),
            argv: vec!["true".into()],
            ..ExecRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::NotFound);
    client
        .release(ReleaseRequest {
            lease_id: lease.lease.id,
            mode: ReleaseMode::Destroy,
            ..ReleaseRequest::default()
        })
        .await
        .unwrap();
    wait_for(|| Service::list(&service).is_empty()).await;
    shutdown.cancel();
}

/// Heartbeat stats travel from a remote agent to the Stats API: the host
/// sample (where the platform measures one) and each sandbox's runtime
/// facts, keyed by the driver-native ref the sandbox record carries.
#[tokio::test]
async fn remote_agent_heartbeat_feeds_the_stats_api() {
    use preloop_sandbox::SandboxApi as _;
    let dir = tempfile::tempdir().unwrap();
    let service = Service::open(
        store::open("memory:").await.unwrap(),
        ServiceConfig::new(dir.path().join("blobs")),
    )
    .await
    .unwrap();
    let auth = Auth {
        client_token: Some("client-secret".into()),
        provider_token: Some("provider-secret".into()),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = router(service.clone(), auth);
    tokio::spawn(async move { axum::serve(listener, app).await });
    let shutdown = CancellationToken::new();
    tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
    let fake = Arc::new(FakeDriver::new(
        "fake",
        dir.path().join("host"),
        FakeDriver::full_capabilities(),
    ));
    let mut agent_config = AgentConfig::new("remote-host", dir.path().join("scratch"));
    agent_config.heartbeat = Duration::from_millis(50);
    let agent = Agent::new(
        Arc::new(RemoteLink::new(&base, Some("provider-secret".into()))),
        vec![fake as Arc<dyn Driver>],
        agent_config,
    );
    tokio::spawn(agent.run(shutdown.clone()));

    let client = SandboxClient::new(&base, Some("client-secret".into()));
    wait_for(|| !Service::list_providers(&service).is_empty()).await;
    let provider_id = Service::list_providers(&service)[0].id.clone();
    client.put_template(template("alpine")).await.unwrap();
    let lease = client
        .acquire(AcquireRequest {
            idempotency_key: "stats".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();

    // Read Stats over the client transport, waiting for a heartbeat that
    // has seen the acquired machine on disk.
    let mut provider = None;
    for _ in 0..200 {
        let stats = client.stats().await.unwrap();
        provider = stats
            .providers
            .into_iter()
            .find(|entry| entry.provider_id == provider_id);
        if provider.as_ref().is_some_and(|entry| {
            entry
                .sandboxes
                .iter()
                .any(|sandbox| sandbox.runtime.native_ref == lease.sandbox.native_ref)
        }) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let provider = provider.expect("stats lists the registered provider");
    assert_eq!(provider.sandboxes.len(), 1);
    let reported = &provider.sandboxes[0];
    assert_eq!(reported.sandbox.id, lease.sandbox.id);
    assert_eq!(reported.sandbox.state, SandboxState::Leased);
    assert_eq!(reported.runtime.native_ref, lease.sandbox.native_ref);
    assert!(provider.host_sample_age_secs < 10);
    // `sample_host` reads `/proc`; other platforms report "unmeasured"
    // rather than a fabricated sample, and the service stores nothing.
    if cfg!(target_os = "linux") {
        assert_ne!(provider.host, HostSample::default());
    }
    // The wire carries aggregates only: no host PIDs, process names or
    // data-directory paths, which the provider measured for itself.
    let wire = serde_json::to_value(client.stats().await.unwrap())
        .unwrap()
        .to_string();
    assert!(!wire.contains("\"pid\""), "Stats leaked a host PID: {wire}");
    assert!(
        !wire.contains("\"dataDir\""),
        "Stats leaked a host path: {wire}"
    );
    assert!(
        !wire.contains("\"topProcesses\""),
        "Stats leaked host process names: {wire}"
    );

    client
        .release(ReleaseRequest {
            lease_id: lease.lease.id,
            mode: ReleaseMode::Destroy,
            ..ReleaseRequest::default()
        })
        .await
        .unwrap();
    shutdown.cancel();
}

#[tokio::test]
async fn destroying_a_sandbox_closes_open_exec_streams() {
    use futures::StreamExt as _;
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let lease = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "k".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let mut stream = h
        .service
        .exec(ExecRequest {
            sandbox_id: lease.sandbox.id.clone(),
            lease_id: lease.lease.id.clone(),
            argv: vec!["sh".into(), "-c".into(), "sleep 300".into()],
            ..ExecRequest::default()
        })
        .unwrap();
    // Give the exec work a moment to reach the agent, then destroy the
    // sandbox out from under the caller.
    tokio::time::sleep(Duration::from_millis(200)).await;
    h.service
        .release(ReleaseRequest {
            lease_id: lease.lease.id.clone(),
            mode: ReleaseMode::Destroy,
            ..ReleaseRequest::default()
        })
        .await
        .unwrap();
    let error = loop {
        match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
            Ok(Some(Err(error))) => break error,
            Ok(Some(Ok(_))) => continue,
            Ok(None) => panic!("stream ended without an error"),
            Err(_) => panic!("exec stream hung after the sandbox was destroyed"),
        }
    };
    assert_eq!(error.code, Code::Unavailable);
    assert!(error.message.contains("destroyed"), "{}", error.message);
}

#[tokio::test]
async fn invalid_names_are_rejected() {
    let h = harness().await;
    for name in ["../escape", "a/b", "bad\nname", ""] {
        let error = h
            .service
            .put_template(TemplateSpec {
                name: name.into(),
                os: "linux".into(),
                artifacts: vec![oci_artifact()],
                ..TemplateSpec::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.code, Code::InvalidArgument, "{name:?}");
    }
    let error = h
        .service
        .put_pool(PoolSpec {
            name: "../pool".into(),
            template: "alpine".into(),
            ..PoolSpec::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::InvalidArgument);
}

#[tokio::test]
async fn templates_require_digest_pinned_artifacts() {
    let h = harness().await;
    let base = template("alpine");

    let mut tag = base.clone();
    tag.artifacts[0].reference = "ghcr.io/preloop/alpine:3.20".into();
    let error = h.service.put_template(tag).await.unwrap_err();
    assert_eq!(error.code, Code::InvalidArgument);
    assert!(error.message.contains("digest-pinned"), "{}", error.message);

    let mut undigested = base.clone();
    undigested.artifacts[0].digest = "sha256:not-a-digest".into();
    let error = h.service.put_template(undigested).await.unwrap_err();
    assert_eq!(error.code, Code::InvalidArgument);
    assert!(error.message.contains("sha256"), "{}", error.message);

    let mut mismatched = base.clone();
    mismatched.artifacts[0].digest = format!("sha256:{}", "b".repeat(64));
    let error = h.service.put_template(mismatched).await.unwrap_err();
    assert_eq!(error.code, Code::InvalidArgument);
    assert!(
        error.message.contains("does not match"),
        "{}",
        error.message
    );

    let mut unknown_format = base.clone();
    unknown_format.artifacts[0].format = "raw-disk".into();
    let error = h.service.put_template(unknown_format).await.unwrap_err();
    assert_eq!(error.code, Code::InvalidArgument);
    assert!(
        error.message.contains("unsupported artifact format"),
        "{}",
        error.message
    );

    let mut both_sources = base.clone();
    both_sources.recipe = Some("smolmachine:/tmp/runner.smolmachine".into());
    let error = h.service.put_template(both_sources).await.unwrap_err();
    assert_eq!(error.code, Code::InvalidArgument);

    let mut duplicate = base.clone();
    duplicate.artifacts.push(duplicate.artifacts[0].clone());
    let error = h.service.put_template(duplicate).await.unwrap_err();
    assert_eq!(error.code, Code::InvalidArgument);
    assert!(error.message.contains("twice"), "{}", error.message);

    // A local pack needs a digest, not a registry pin.
    h.service
        .put_template(TemplateSpec {
            name: "pack".into(),
            os: "linux".into(),
            artifacts: vec![Artifact {
                format: Artifact::SMOLMACHINE.into(),
                arch: host_arch().into(),
                reference: "file:///tmp/runner.smolmachine".into(),
                digest: format!("sha256:{}", "c".repeat(64)),
                size_bytes: 4096,
            }],
            ..TemplateSpec::default()
        })
        .await
        .unwrap();

    // An OCI-pack ref without a digest pin is still mutable.
    let error = h
        .service
        .put_template(TemplateSpec {
            name: "oci-pack".into(),
            os: "linux".into(),
            artifacts: vec![Artifact {
                format: Artifact::SMOLMACHINE.into(),
                arch: host_arch().into(),
                reference: "ghcr.io/preloop/runner:latest".into(),
                digest: format!("sha256:{}", "d".repeat(64)),
                size_bytes: 0,
            }],
            ..TemplateSpec::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::InvalidArgument);
    assert!(error.message.contains("digest-pinned"), "{}", error.message);
}

#[tokio::test]
async fn restored_leases_re_inject_their_files() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sandbox.db");
    let home = tempfile::tempdir().unwrap();
    let host = dir.path().join("host");
    let first = {
        let h = harness_with_host(
            store::open(db.to_str().unwrap()).await.unwrap(),
            home,
            host.clone(),
            FakeDriver::full_capabilities(),
        )
        .await;
        h.service.put_template(template("alpine")).await.unwrap();
        let mut request = AcquireRequest {
            idempotency_key: "key".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        };
        request.inject.files.push(FileSpec {
            path: "/run/first".into(),
            data: b"one".to_vec(),
            mode: 0o600,
        });
        let lease = h.service.acquire(request).await.unwrap();
        let (_, stdout, _) = h
            .service
            .exec_collect(ExecRequest {
                sandbox_id: lease.sandbox.id.clone(),
                lease_id: lease.lease.id.clone(),
                argv: vec![
                    "sh".into(),
                    "-c".into(),
                    "cat \"$SANDBOX_ROOT/run/first\"".into(),
                ],
                ..ExecRequest::default()
            })
            .await
            .unwrap();
        assert_eq!(stdout, b"one");
        lease
    };
    // Restart on the same file: the lease comes back, and a retry with the
    // same key must deliver the files again rather than trusting that the
    // pre-crash process finished injecting them.
    let h = harness_with_host(
        store::open(db.to_str().unwrap()).await.unwrap(),
        tempfile::tempdir().unwrap(),
        host,
        FakeDriver::full_capabilities(),
    )
    .await;
    let mut request = AcquireRequest {
        idempotency_key: "key".into(),
        template: "alpine".into(),
        ..AcquireRequest::default()
    };
    request.inject.files.push(FileSpec {
        path: "/run/second".into(),
        data: b"two".to_vec(),
        mode: 0o600,
    });
    let lease = h.service.acquire(request).await.unwrap();
    assert_eq!(lease.lease.id, first.lease.id, "same key, same lease");
    let (_, stdout, _) = h
        .service
        .exec_collect(ExecRequest {
            sandbox_id: lease.sandbox.id.clone(),
            lease_id: lease.lease.id.clone(),
            argv: vec![
                "sh".into(),
                "-c".into(),
                "cat \"$SANDBOX_ROOT/run/second\"".into(),
            ],
            ..ExecRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(stdout, b"two");
}

#[tokio::test]
async fn a_lost_provider_fails_its_sandboxes_instead_of_hanging_leases() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = ServiceConfig::new(dir.path().join("blobs"));
    config.provider_timeout = Duration::from_millis(80);
    config.provider_lost_factor = 1;
    config.acquire_wait = Duration::from_millis(200);
    let store = store::open("memory:").await.unwrap();
    let service = Service::open(store.clone(), config).await.unwrap();
    // A provider that registers and then dies without reporting: its create
    // never completes.
    service
        .register(RegisterRequest {
            name: "ghost".into(),
            drivers: vec![DriverInfo {
                driver: "ghost".into(),
                capabilities: FakeDriver::full_capabilities(),
            }],
        })
        .await
        .unwrap();
    service
        .put_template(TemplateSpec {
            name: "alpine".into(),
            os: "linux".into(),
            artifacts: vec![oci_artifact()],
            requires: Requirements {
                driver: "ghost".into(),
                ..Requirements::default()
            },
            ..TemplateSpec::default()
        })
        .await
        .unwrap();
    let error = service
        .acquire(AcquireRequest {
            idempotency_key: "k".into(),
            template: "alpine".into(),
            wait_secs: Some(1),
            ..AcquireRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::DeadlineExceeded);
    tokio::time::sleep(Duration::from_millis(120)).await;
    service.maintain().await;
    let sandboxes = service.list();
    assert!(
        sandboxes
            .iter()
            .all(|s| s.state != SandboxState::Provisioning && s.state != SandboxState::Leased),
        "{sandboxes:?}"
    );
    assert!(
        store.load(Kind::Leases).await.unwrap().is_empty(),
        "a lost provider's lease is released, not left to expire"
    );
}

#[tokio::test]
async fn stop_and_start_round_trip_a_sandbox() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let lease = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "k".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let id = lease.sandbox.id.clone();
    let lease_id = lease.lease.id.clone();
    let exec = |argv: Vec<String>| {
        let service = h.service.clone();
        let id = id.clone();
        let lease_id = lease_id.clone();
        async move {
            service
                .exec_collect(ExecRequest {
                    sandbox_id: id,
                    lease_id,
                    argv,
                    ..ExecRequest::default()
                })
                .await
        }
    };
    assert_eq!(exec(vec!["true".into()]).await.unwrap().0, 0);
    h.service.stop(&id, &lease_id).await.unwrap();
    let error = exec(vec!["true".into()]).await.unwrap_err();
    assert_eq!(error.code, Code::FailedPrecondition);
    assert!(error.message.contains("stopped"), "{}", error.message);
    assert_eq!(
        h.service.get(&id, &lease_id).unwrap().state,
        SandboxState::Stopped
    );
    // A stopped sandbox keeps its lease, so a golden can be packed and
    // restarted forkable under the same lease.
    h.service.start(&id, &lease_id, true).await.unwrap();
    assert_eq!(exec(vec!["true".into()]).await.unwrap().0, 0);
    assert_eq!(
        h.service.get(&id, &lease_id).unwrap().state,
        SandboxState::Leased
    );
    assert_eq!(h.service.list().len(), 1);
}

#[tokio::test]
async fn reconcile_never_destroys_another_providers_machine() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let lease = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "k".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    // A second agent on the same host sees the first provider's machine in
    // its inventory (both use the same naming prefix) and reports it.
    let other = h
        .service
        .register(RegisterRequest {
            name: "host-b".into(),
            drivers: vec![DriverInfo {
                driver: "fake".into(),
                capabilities: FakeDriver::full_capabilities(),
            }],
        })
        .await
        .unwrap();
    let observed = h.fake.inventory().await.unwrap();
    let sandbox_id = lease.sandbox.id.clone();
    let response = h
        .service
        .reconcile(ReconcileRequest {
            provider_id: other.provider_id.clone(),
            generation: other.generation,
            sandboxes: observed,
        })
        .await
        .unwrap();
    assert!(
        response.destroy.is_empty(),
        "cross-provider purge: {:?}",
        response.destroy
    );
    assert_eq!(
        h.service.get(&sandbox_id, &lease.lease.id).unwrap().state,
        SandboxState::Leased
    );

    // An unowned orphan from nobody is still reaped.
    let orphan = h
        .fake
        .create(&CreateWork {
            sandbox_id: "sbx-orphan".into(),
            driver: "fake".into(),
            ..CreateWork::default()
        })
        .await
        .unwrap();
    let observed = h.fake.inventory().await.unwrap();
    assert!(observed.iter().any(|o| o.native_ref == orphan));
    let response = h
        .service
        .reconcile(ReconcileRequest {
            provider_id: other.provider_id,
            generation: other.generation,
            sandboxes: observed,
        })
        .await
        .unwrap();
    assert!(response.destroy.iter().any(|w| w.native_ref == orphan));
}

#[tokio::test]
async fn a_restored_sandbox_without_its_lease_is_failed_not_zombied() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sandbox.db");
    let host = dir.path().join("host");
    let sandbox_id = {
        let h = harness_with_host(
            store::open(db.to_str().unwrap()).await.unwrap(),
            tempfile::tempdir().unwrap(),
            host.clone(),
            FakeDriver::full_capabilities(),
        )
        .await;
        h.service.put_template(template("alpine")).await.unwrap();
        let lease = h
            .service
            .acquire(AcquireRequest {
                idempotency_key: "k".into(),
                template: "alpine".into(),
                ..AcquireRequest::default()
            })
            .await
            .unwrap();
        lease.sandbox.id
    };
    // Simulate the crash window: the sandbox row survives, its lease does not.
    let raw = store::open(db.to_str().unwrap()).await.unwrap();
    for lease in raw
        .load(preloop_sandbox::store::Kind::Leases)
        .await
        .unwrap()
    {
        let id = lease["id"].as_str().unwrap().to_owned();
        raw.delete(preloop_sandbox::store::Kind::Leases, &id)
            .await
            .unwrap();
    }
    let restored = Service::open(
        store::open(db.to_str().unwrap()).await.unwrap(),
        ServiceConfig::new(dir.path().join("blobs2")),
    )
    .await
    .unwrap();
    // Its lease row is gone, so `Get` cannot be authorized for it; the
    // unscoped list projection is the only view left.
    let sandbox = restored
        .list()
        .into_iter()
        .find(|sandbox| sandbox.id == sandbox_id)
        .expect("restored sandbox is listed");
    assert_eq!(sandbox.state, SandboxState::Failed);
    assert!(
        sandbox.error.contains("without a lease"),
        "{}",
        sandbox.error
    );
    // The name is free again, so a retry with the same key can start over.
    let h = harness_with_host(
        store::open(db.to_str().unwrap()).await.unwrap(),
        tempfile::tempdir().unwrap(),
        host,
        FakeDriver::full_capabilities(),
    )
    .await;
    let lease = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "k".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .expect("a failed sandbox must not block its name");
    assert_ne!(lease.sandbox.id, sandbox_id);
}

/// The lease from `Acquire` is the capability: every sandbox-scoped call
/// needs it, a foreign one is denied, and an unknown sandbox is not found.
#[tokio::test]
async fn sandbox_scoped_calls_require_the_holding_lease() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let a = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "a".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let b = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "b".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let sandbox_id = a.sandbox.id.clone();
    for lease_id in ["", b.lease.id.as_str()] {
        let get = h.service.get(&sandbox_id, lease_id).unwrap_err();
        assert_eq!(get.code, Code::PermissionDenied, "{get}");
        let exec = h
            .service
            .exec(ExecRequest {
                sandbox_id: sandbox_id.clone(),
                lease_id: lease_id.to_owned(),
                argv: vec!["true".into()],
                ..ExecRequest::default()
            })
            .err()
            .expect("exec must be denied");
        assert_eq!(exec.code, Code::PermissionDenied, "{exec}");
        let stream = h
            .service
            .open_stream(OpenStreamRequest {
                sandbox_id: sandbox_id.clone(),
                lease_id: lease_id.to_owned(),
                kind: StreamKind::Download,
                path: "/etc".into(),
            })
            .unwrap_err();
        assert_eq!(stream.code, Code::PermissionDenied, "{stream}");
        let stop = h.service.stop(&sandbox_id, lease_id).await.unwrap_err();
        assert_eq!(stop.code, Code::PermissionDenied, "{stop}");
        let start = h
            .service
            .start(&sandbox_id, lease_id, false)
            .await
            .unwrap_err();
        assert_eq!(start.code, Code::PermissionDenied, "{start}");
        let pause = h.service.pause(&sandbox_id, lease_id).await.unwrap_err();
        assert_eq!(pause.code, Code::PermissionDenied, "{pause}");
        let resume = h.service.resume(&sandbox_id, lease_id).await.unwrap_err();
        assert_eq!(resume.code, Code::PermissionDenied, "{resume}");
        let snapshot = h
            .service
            .snapshot(SnapshotRequest {
                sandbox_id: sandbox_id.clone(),
                lease_id: lease_id.to_owned(),
                name: "stolen".into(),
            })
            .await
            .unwrap_err();
        assert_eq!(snapshot.code, Code::PermissionDenied, "{snapshot}");
    }
    // The holder still works, and an unknown sandbox is not found (never a
    // permission hint about another tenant).
    assert_eq!(
        h.service.get(&sandbox_id, &a.lease.id).unwrap().id,
        sandbox_id
    );
    assert_eq!(
        h.service
            .exec_collect(ExecRequest {
                sandbox_id: sandbox_id.clone(),
                lease_id: a.lease.id.clone(),
                argv: vec!["true".into()],
                ..ExecRequest::default()
            })
            .await
            .unwrap()
            .0,
        0
    );
    let missing = h.service.get("sbx-missing", &a.lease.id).unwrap_err();
    assert_eq!(missing.code, Code::NotFound, "{missing}");
    h.service
        .release(ReleaseRequest {
            lease_id: a.lease.id,
            mode: ReleaseMode::Destroy,
            ..ReleaseRequest::default()
        })
        .await
        .unwrap();
    h.service
        .release(ReleaseRequest {
            lease_id: b.lease.id,
            mode: ReleaseMode::Destroy,
            ..ReleaseRequest::default()
        })
        .await
        .unwrap();
}

/// `List` is unscoped, so it never carries a lease id; `Get` only shows the
/// lease the caller already presented.
#[tokio::test]
async fn list_redacts_lease_ids() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let first = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "first".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let second = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "second".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let listed = h.service.list();
    assert_eq!(listed.len(), 2);
    assert!(
        listed.iter().all(|sandbox| sandbox.lease_id.is_empty()),
        "{listed:?}"
    );
    // The holder sees its own lease; another lease does not unlock it.
    assert_eq!(
        h.service
            .get(&first.sandbox.id, &first.lease.id)
            .unwrap()
            .lease_id,
        first.lease.id
    );
    assert_eq!(
        h.service
            .get(&first.sandbox.id, &second.lease.id)
            .unwrap_err()
            .code,
        Code::PermissionDenied
    );
}

fn mounted(key: &str, host: &std::path::Path) -> AcquireRequest {
    AcquireRequest {
        idempotency_key: key.into(),
        template: "alpine".into(),
        mounts: vec![Mount {
            host: host.display().to_string(),
            guest: "/mnt".into(),
            readonly: true,
        }],
        ..AcquireRequest::default()
    }
}

fn socketed(key: &str, host: &std::path::Path) -> AcquireRequest {
    AcquireRequest {
        idempotency_key: key.into(),
        template: "alpine".into(),
        sockets: vec![SocketMount {
            host: host.display().to_string(),
            guest: "/run/host.sock".into(),
        }],
        ..AcquireRequest::default()
    }
}

/// The agent admits only host paths under its configured prefixes, resolving
/// `..` and symlinks before the check, and denies everything by default.
#[tokio::test]
async fn agent_enforces_the_host_mount_and_socket_allowlist() {
    let dir = tempfile::tempdir().unwrap();
    let allowed = dir.path().join("allowed");
    let child = allowed.join("child");
    let outside = dir.path().join("outside");
    std::fs::create_dir_all(&child).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    let access = HostAccess {
        mounts: vec![allowed.clone()],
        sockets: vec![allowed.clone()],
    };
    let h = harness_with_access(
        store::open("memory:").await.unwrap(),
        tempfile::tempdir().unwrap(),
        FakeDriver::full_capabilities(),
        access,
    )
    .await;
    h.service.put_template(template("alpine")).await.unwrap();

    // Allowed prefix, its descendant, and a path that stays inside after
    // resolving `..` all succeed.
    for (key, host) in [
        ("allow-prefix", allowed.clone()),
        ("allow-child", child.clone()),
        ("allow-dotdot-inside", child.join("..").join("child")),
    ] {
        let response = h.service.acquire(mounted(key, &host)).await.unwrap();
        h.service
            .release(ReleaseRequest {
                lease_id: response.lease.id,
                mode: ReleaseMode::Destroy,
                ..ReleaseRequest::default()
            })
            .await
            .unwrap();
    }
    // A sibling tree is refused before the driver runs.
    let error = h
        .service
        .acquire(mounted("deny-outside", &outside))
        .await
        .unwrap_err();
    assert!(
        error.message.contains("outside this agent's allowlist"),
        "{}",
        error.message
    );
    // `..` cannot climb out of the allowed prefix.
    let escape = allowed.join("..").join("outside");
    let error = h
        .service
        .acquire(mounted("deny-escape", &escape))
        .await
        .unwrap_err();
    assert!(
        error.message.contains("outside this agent's allowlist"),
        "{}",
        error.message
    );
    // Sockets use their own prefix list.
    let response = h
        .service
        .acquire(socketed("socket-allow", &allowed))
        .await
        .unwrap();
    h.service
        .release(ReleaseRequest {
            lease_id: response.lease.id,
            mode: ReleaseMode::Destroy,
            ..ReleaseRequest::default()
        })
        .await
        .unwrap();
    let error = h
        .service
        .acquire(socketed("socket-deny", &outside))
        .await
        .unwrap_err();
    assert!(
        error.message.contains("outside this agent's allowlist"),
        "{}",
        error.message
    );
}

/// With no allowlist configured (the default), every host mount is refused.
#[tokio::test]
async fn agent_denies_all_host_mounts_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let error = h
        .service
        .acquire(mounted("no-allow", dir.path()))
        .await
        .unwrap_err();
    assert!(
        error.message.contains("exports no mount paths"),
        "{}",
        error.message
    );
    let error = h
        .service
        .acquire(socketed("no-socket-allow", dir.path()))
        .await
        .unwrap_err();
    assert!(
        error.message.contains("exports no socket paths"),
        "{}",
        error.message
    );
}

/// Bind the service on a loopback port and return its base URL.
async fn serve(service: Arc<Service>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = router(service, Auth::default());
    tokio::spawn(async move { axum::serve(listener, app).await });
    base
}

#[tokio::test]
async fn cordoned_provider_receives_no_placements() {
    let store = store::open("memory:").await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let h = harness_with(store.clone(), dir, FakeDriver::full_capabilities()).await;
    h.service.put_template(template("alpine")).await.unwrap();
    let name = h.service.list_providers()[0].name.clone();

    let provider = h.service.set_provider_cordon(&name, true).await.unwrap();
    assert!(provider.cordoned);
    // The cordon is part of the persisted provider record.
    let records = store.load(Kind::Providers).await.unwrap();
    assert!(records[0]["cordoned"].as_bool().unwrap_or(false));

    let error = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "cordoned".into(),
            template: "alpine".into(),
            wait_secs: Some(0),
            ..AcquireRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::Unavailable, "{}", error.message);
    assert!(h.fake.inventory().await.unwrap().is_empty());

    let provider = h.service.set_provider_cordon(&name, false).await.unwrap();
    assert!(!provider.cordoned);
    let lease = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "open".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(lease.sandbox.state, SandboxState::Leased);
}

#[tokio::test]
async fn drain_releases_idle_pool_members_and_keeps_leases() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    // `max` equals `warm`, so claiming a member leaves nothing to refill:
    // the drain sees exactly the one idle replacement and one lease.
    h.service
        .put_pool(PoolSpec {
            name: "linux".into(),
            template: "alpine".into(),
            warm: 2,
            max: 2,
            ..PoolSpec::default()
        })
        .await
        .unwrap();
    wait_for(|| {
        h.service
            .list()
            .iter()
            .filter(|s| s.state == SandboxState::Ready)
            .count()
            == 2
    })
    .await;
    let lease = h.service.acquire(acquire("drain", "linux")).await.unwrap();
    let name = h.service.list_providers()[0].name.clone();

    let result = h.service.drain_provider(&name).await.unwrap();
    assert_eq!(result.released_pool_members, 1);
    assert_eq!(result.leased_remaining, 1);
    assert!(h.service.list_providers()[0].cordoned);

    // The idle member is destroyed; the leased one keeps running.
    wait_for(|| {
        let sandboxes = h.service.list();
        sandboxes.len() == 1 && sandboxes[0].id == lease.sandbox.id
    })
    .await;
    assert_eq!(h.fake.inventory().await.unwrap().len(), 1);

    // No refill while cordoned.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(h.service.list().len(), 1);

    h.service.set_provider_cordon(&name, false).await.unwrap();
    wait_for(|| h.service.list().len() == 2).await;
}

#[tokio::test]
async fn connect_unary_bodies_are_capped_at_four_mib() {
    use preloop_sandbox::server::CONNECT_BODY_LIMIT;
    use tower::ServiceExt as _;
    let dir = tempfile::tempdir().unwrap();
    let service = Service::open(
        store::open("memory:").await.unwrap(),
        ServiceConfig::new(dir.path().join("blobs")),
    )
    .await
    .unwrap();
    let response = router(service, Auth::default())
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/preloop.sandbox.v1.Sandboxes/PutTemplate")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(vec![b' '; CONNECT_BODY_LIMIT + 1]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn blob_upload_cap_rejects_and_removes_the_partial_file() {
    let dir = tempfile::tempdir().unwrap();
    let blobs = dir.path().join("blobs");
    let mut config = ServiceConfig::new(blobs.clone());
    config.blob_max_bytes = 1024;
    let service = Service::open(store::open("memory:").await.unwrap(), config)
        .await
        .unwrap();
    let fake = Arc::new(FakeDriver::new(
        "fake",
        dir.path().join("host"),
        FakeDriver::full_capabilities(),
    ));
    let shutdown = CancellationToken::new();
    let agent = Agent::new(
        Arc::new(service.clone()) as Arc<dyn ProviderLink>,
        vec![fake as Arc<dyn Driver>],
        AgentConfig::new("host-a", dir.path().join("scratch")),
    );
    tokio::spawn(agent.run(shutdown.clone()));
    tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
    wait_for(|| !service.list_providers().is_empty()).await;
    service.put_template(template("alpine")).await.unwrap();
    let lease = service
        .acquire(AcquireRequest {
            idempotency_key: "blob".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let ticket = service
        .open_stream(OpenStreamRequest {
            sandbox_id: lease.sandbox.id,
            lease_id: lease.lease.id,
            kind: StreamKind::Upload,
            path: "/work".into(),
        })
        .unwrap();
    let ticket_id = ticket.url.rsplit('/').next().unwrap().to_owned();
    let base = serve(service.clone()).await;

    // A chunked body crosses the cap while streaming, so the handler has
    // already created the file when it rejects.
    let chunked = reqwest::Body::wrap_stream(futures::stream::iter(vec![Ok::<_, std::io::Error>(
        bytes::Bytes::from(vec![0u8; 2048]),
    )]));
    let response = reqwest::Client::new()
        .put(format!("{base}{}", ticket.url))
        .bearer_auth(&ticket.token)
        .body(chunked)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    assert!(!blobs.join(format!("{ticket_id}.tar")).exists());
    shutdown.cancel();
}

#[tokio::test]
async fn list_pagination_round_trips_across_250_items() {
    use preloop_sandbox::SandboxApi as _;
    let dir = tempfile::tempdir().unwrap();
    let service = Service::open(
        store::open("memory:").await.unwrap(),
        ServiceConfig::new(dir.path().join("blobs")),
    )
    .await
    .unwrap();
    for index in 0..250 {
        service
            .put_template(TemplateSpec {
                name: format!("t{index:03}"),
                os: "linux".into(),
                artifacts: vec![oci_artifact()],
                ..TemplateSpec::default()
            })
            .await
            .unwrap();
    }

    // Explicit pages, ordered by id (the template name).
    let mut request = ListRequest {
        page_size: 100,
        page_token: String::new(),
    };
    let mut names = Vec::new();
    loop {
        let page = service.list_templates_page(&request).unwrap();
        assert!(page.templates.len() <= 100);
        names.extend(page.templates.into_iter().map(|t| t.spec.name));
        if page.next_page_token.is_empty() {
            break;
        }
        request.page_token = page.next_page_token;
    }
    assert_eq!(names.len(), 250);
    assert_eq!(names[0], "t000");
    assert_eq!(names[249], "t249");
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted);

    // The network client follows the same tokens.
    let base = serve(service).await;
    let client = SandboxClient::new(&base, None);
    assert_eq!(client.list_templates().await.unwrap().len(), 250);
}

#[tokio::test]
async fn boot_sweeps_orphaned_blob_files() {
    let dir = tempfile::tempdir().unwrap();
    let blobs = dir.path().join("blobs");
    std::fs::create_dir_all(blobs.join("keep-dir")).unwrap();
    std::fs::write(blobs.join("tkt-stale.tar"), b"partial upload").unwrap();
    std::fs::write(blobs.join("inject-stale.tar"), b"staged files").unwrap();
    let _service = Service::open(
        store::open("memory:").await.unwrap(),
        ServiceConfig::new(blobs.clone()),
    )
    .await
    .unwrap();
    assert!(!blobs.join("tkt-stale.tar").exists());
    assert!(!blobs.join("inject-stale.tar").exists());
    assert!(blobs.join("keep-dir").exists());
}

/// A restart fences every agent session that survived the previous process:
/// the generation was bumped and persisted, the old session is `aborted` on
/// its next call, and the agent re-registering gets a fresh generation this
/// service places work on.
#[tokio::test]
async fn restart_fences_the_agent_and_it_re_registers() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sandbox.db");
    let host = dir.path().join("host");
    // A registered provider stands in for an agent whose process died with
    // the service: its row (and generation) survives in the store.
    let (provider_id, generation) = {
        let service = Service::open(
            store::open(db.to_str().unwrap()).await.unwrap(),
            ServiceConfig::new(dir.path().join("blobs")),
        )
        .await
        .unwrap();
        let response = service
            .register(RegisterRequest {
                name: "host-a".into(),
                drivers: vec![DriverInfo {
                    driver: "fake".into(),
                    capabilities: FakeDriver::full_capabilities(),
                }],
            })
            .await
            .unwrap();
        (response.provider_id, response.generation)
    };
    assert_eq!(generation, 1);
    // Reopening the same store bumps the generation before serving: the
    // session above can no longer poll, report or heartbeat.
    let restored = Service::open(
        store::open(db.to_str().unwrap()).await.unwrap(),
        ServiceConfig::new(dir.path().join("blobs")),
    )
    .await
    .unwrap();
    let error = restored
        .poll(PollRequest {
            provider_id: provider_id.clone(),
            generation,
            wait_secs: 0,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::Aborted, "{}", error.message);
    let bumped = restored
        .list_providers()
        .into_iter()
        .find(|provider| provider.id == provider_id)
        .expect("the provider row survives the restart");
    assert_eq!(bumped.generation, generation + 1);
    // The agent's answer to `aborted` is to register again, which is what the
    // real agent does; the restarted service places work on that session.
    let h = harness_with_host(
        store::open(db.to_str().unwrap()).await.unwrap(),
        tempfile::tempdir().unwrap(),
        host,
        FakeDriver::full_capabilities(),
    )
    .await;
    let re_registered = h
        .service
        .list_providers()
        .into_iter()
        .find(|provider| provider.id == provider_id)
        .expect("the re-registered agent keeps its provider id");
    assert!(
        re_registered.generation > bumped.generation,
        "re-registration must be a new session: {} vs {}",
        re_registered.generation,
        bumped.generation
    );
    h.service.put_template(template("alpine")).await.unwrap();
    let lease = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "after-restart".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .expect("the re-registered agent serves work");
    assert_eq!(lease.sandbox.state, SandboxState::Leased);
}

/// A create in flight when the process died cannot be re-driven (its request
/// is not on the record) and nothing will ever report it: the record is
/// failed with a reason instead of waiting for the create timeout.
#[tokio::test]
async fn a_creating_record_is_failed_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sandbox.db");
    let sandbox_id = {
        let service = Service::open(
            store::open(db.to_str().unwrap()).await.unwrap(),
            ServiceConfig::new(dir.path().join("blobs")),
        )
        .await
        .unwrap();
        // A provider that registers and never polls: its create never lands.
        service
            .register(RegisterRequest {
                name: "ghost".into(),
                drivers: vec![DriverInfo {
                    driver: "ghost".into(),
                    capabilities: FakeDriver::full_capabilities(),
                }],
            })
            .await
            .unwrap();
        service
            .put_template(TemplateSpec {
                name: "alpine".into(),
                os: "linux".into(),
                artifacts: vec![oci_artifact()],
                requires: Requirements {
                    driver: "ghost".into(),
                    ..Requirements::default()
                },
                ..TemplateSpec::default()
            })
            .await
            .unwrap();
        let error = service
            .acquire(AcquireRequest {
                idempotency_key: "k".into(),
                template: "alpine".into(),
                wait_secs: Some(0),
                ..AcquireRequest::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.code, Code::DeadlineExceeded);
        let sandboxes = service.list();
        assert_eq!(sandboxes.len(), 1);
        assert_eq!(sandboxes[0].state, SandboxState::Provisioning);
        sandboxes[0].id.clone()
    };
    let restored = Service::open(
        store::open(db.to_str().unwrap()).await.unwrap(),
        ServiceConfig::new(dir.path().join("blobs2")),
    )
    .await
    .unwrap();
    let sandbox = restored
        .list()
        .into_iter()
        .find(|sandbox| sandbox.id == sandbox_id)
        .expect("the creating record survives the restart");
    assert_eq!(sandbox.state, SandboxState::Failed);
    assert!(
        sandbox.error.contains("restarted while creating"),
        "{}",
        sandbox.error
    );
}

/// A record left `Terminating` by a crashed process whose machine is still in
/// the provider's inventory gets its destroy re-queued until the machine is
/// gone, then stops holding a slot.
#[tokio::test]
async fn a_terminating_record_with_a_live_machine_is_reaped() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sandbox.db");
    let host = dir.path().join("host");
    let (sandbox_id, native_ref) = {
        let h = harness_with_host(
            store::open(db.to_str().unwrap()).await.unwrap(),
            tempfile::tempdir().unwrap(),
            host.clone(),
            FakeDriver::full_capabilities(),
        )
        .await;
        h.service.put_template(template("alpine")).await.unwrap();
        let lease = h
            .service
            .acquire(AcquireRequest {
                idempotency_key: "k".into(),
                template: "alpine".into(),
                ..AcquireRequest::default()
            })
            .await
            .unwrap();
        (lease.sandbox.id, lease.sandbox.native_ref)
    };
    // Acquire returns once the in-memory record is leased, which a reconcile
    // notification can deliver before the create report's store write lands.
    // The crash is only simulated from a state that was durable, so wait for
    // the reported native reference to be persisted.
    let raw = store::open(db.to_str().unwrap()).await.unwrap();
    let mut durable = false;
    for _ in 0..200 {
        let sandboxes = raw.load(Kind::Sandboxes).await.unwrap();
        let leases = raw.load(Kind::Leases).await.unwrap();
        durable = !leases.is_empty()
            && sandboxes.iter().any(|row| {
                row["nativeRef"]
                    .as_str()
                    .is_some_and(|native| !native.is_empty())
            });
        if durable {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(durable, "the create report was never persisted");
    // Simulate the crash window: the destroy was decided and persisted, but
    // the machine outlived the process that asked for it.
    for value in raw.load(Kind::Sandboxes).await.unwrap() {
        let mut sandbox: Sandbox = serde_json::from_value(value).unwrap();
        sandbox.state = SandboxState::Terminating;
        raw.put(
            Kind::Sandboxes,
            &sandbox.id,
            &serde_json::to_value(&sandbox).unwrap(),
        )
        .await
        .unwrap();
    }
    let h = harness_with_host(
        store::open(db.to_str().unwrap()).await.unwrap(),
        tempfile::tempdir().unwrap(),
        host,
        FakeDriver::full_capabilities(),
    )
    .await;
    wait_for(|| h.service.list().iter().all(|sandbox| sandbox.id != sandbox_id)).await;
    assert!(
        h.fake
            .inventory()
            .await
            .unwrap()
            .iter()
            .all(|observed| observed.native_ref != native_ref),
        "the machine behind the terminating record must be destroyed"
    );
}

/// A lease on a host that stopped answering is released, not held to its TTL:
/// the caller learns it is gone, and the machine is left for reconcile.
#[tokio::test]
async fn a_lost_provider_releases_its_leases() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sandbox.db");
    let store = store::open(db.to_str().unwrap()).await.unwrap();
    let mut config = ServiceConfig::new(dir.path().join("blobs"));
    config.provider_timeout = Duration::from_millis(80);
    config.provider_lost_factor = 1;
    config.acquire_wait = Duration::from_secs(10);
    let service = Service::open(store.clone(), config).await.unwrap();
    let fake = Arc::new(FakeDriver::new(
        "fake",
        dir.path().join("host"),
        FakeDriver::full_capabilities(),
    ));
    let shutdown = CancellationToken::new();
    let agent = Agent::new(
        Arc::new(service.clone()) as Arc<dyn ProviderLink>,
        vec![fake as Arc<dyn Driver>],
        AgentConfig::new("host-a", dir.path().join("scratch")),
    );
    tokio::spawn(agent.run(shutdown.clone()));
    wait_for(|| !service.list_providers().is_empty()).await;
    service.put_template(template("alpine")).await.unwrap();
    let lease = service
        .acquire(AcquireRequest {
            idempotency_key: "k".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    // Silencing the agent is what a host losing its network looks like: no
    // heartbeat, no poll, no report.
    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(120)).await;
    service.maintain().await;
    let error = service
        .renew(RenewRequest {
            lease_id: lease.lease.id.clone(),
            ttl_secs: 60,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::NotFound, "{}", error.message);
    assert!(
        store.load(Kind::Leases).await.unwrap().is_empty(),
        "the lease row must be released, not left to expire"
    );
    let sandbox = service
        .list()
        .into_iter()
        .find(|sandbox| sandbox.id == lease.sandbox.id)
        .expect("the sandbox remains listed while it terminates");
    assert!(sandbox.lease_id.is_empty(), "the lease id is cleared");
    assert_eq!(sandbox.state, SandboxState::Terminating);
    assert!(sandbox.error.contains("provider lost"), "{}", sandbox.error);
}

/// A lease that outlives its sandbox's failure cannot be renewed: extending
/// it would promise a machine that is gone.
#[tokio::test]
async fn renew_on_a_failed_sandbox_is_rejected() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let lease = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "k".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    // The machine disappears behind the service's back; the reconcile fails
    // the record but the caller still holds its lease.
    h.fake.destroy(&lease.sandbox.native_ref).await.unwrap();
    wait_for(|| {
        h.service
            .get(&lease.sandbox.id, &lease.lease.id)
            .is_ok_and(|sandbox| sandbox.state == SandboxState::Failed)
    })
    .await;
    let error = h
        .service
        .renew(RenewRequest {
            lease_id: lease.lease.id,
            ttl_secs: 60,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::FailedPrecondition, "{}", error.message);
}

/// An idempotency key names one request: reusing it for a different payload
/// is a caller bug, not a retry, and must not silently hand back the first
/// sandbox.
#[tokio::test]
async fn a_reused_idempotency_key_with_a_different_payload_is_rejected() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let other = template("debian");
    h.service.put_template(other).await.unwrap();
    let first = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "k".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let error = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "k".into(),
            template: "debian".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::InvalidArgument, "{}", error.message);
    // The lease is untouched and still answers the original request.
    let same = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "k".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(same.lease.id, first.lease.id);
}

/// A store whose writes always fail.
struct FailingStore;

#[async_trait::async_trait]
impl Store for FailingStore {
    async fn put(
        &self,
        _kind: Kind,
        _id: &str,
        _value: &serde_json::Value,
    ) -> Result<(), preloop_sandbox::Error> {
        Err(preloop_sandbox::Error::internal("disk full"))
    }

    async fn delete(&self, _kind: Kind, _id: &str) -> Result<(), preloop_sandbox::Error> {
        Err(preloop_sandbox::Error::internal("disk full"))
    }

    async fn load(&self, _kind: Kind) -> Result<Vec<serde_json::Value>, preloop_sandbox::Error> {
        Ok(Vec::new())
    }
}

/// A failed store write is logged with the record kind and the RPC still
/// succeeds: the in-memory working set is authoritative, the store is the
/// restart source that will be behind.
#[tokio::test]
async fn store_write_failures_keep_the_rpc_successful() {
    let dir = tempfile::tempdir().unwrap();
    let service = Service::open(
        Arc::new(FailingStore),
        ServiceConfig::new(dir.path().join("blobs")),
    )
    .await
    .unwrap();
    let created = service
        .put_template(template("alpine"))
        .await
        .expect("a store failure must not fail the caller");
    assert_eq!(created.spec.name, "alpine");
    assert_eq!(service.list_templates().len(), 1);
}
