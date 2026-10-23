//! Service/agent conformance over the process-backed fake driver, driven
//! both in-process and across a real HTTP server with a remote agent.

use std::sync::Arc;
use std::time::Duration;

use preloop_sandbox::agent::{Agent, AgentConfig, ProviderLink, RemoteLink};
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

fn template(name: &str) -> TemplateSpec {
    TemplateSpec {
        name: name.into(),
        os: "linux".into(),
        image: Some("alpine:3.20".into()),
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

async fn harness_with_host(
    store: Arc<dyn Store>,
    dir: tempfile::TempDir,
    host: std::path::PathBuf,
    caps: Capabilities,
) -> Harness {
    let mut config = ServiceConfig::new(dir.path().join("blobs"));
    config.acquire_wait = Duration::from_secs(10);
    harness_with_host_config(store, dir, host, caps, config).await
}

/// `host` is the fake driver's root: a restart test must reuse the machine
/// directory the previous process created, or reconcile sees it as lost.
async fn harness_with_host_config(
    store: Arc<dyn Store>,
    dir: tempfile::TempDir,
    host: std::path::PathBuf,
    caps: Capabilities,
    config: ServiceConfig,
) -> Harness {
    let service = Service::open(store, config).await.unwrap();
    let fake = Arc::new(FakeDriver::new("fake", host, caps));
    let mut agent_config = AgentConfig::new("host-a", dir.path().join("scratch"));
    agent_config.reconcile = Duration::from_millis(300);
    // Placement reads a provider silent past `provider_timeout` as offline.
    // The default heartbeat (10 s) leaves little room on a loaded runner;
    // one second keeps the harness provider online through a stall.
    agent_config.heartbeat = Duration::from_secs(1);
    let shutdown = CancellationToken::new();
    let agent = Agent::new(
        Arc::new(service.clone()) as Arc<dyn ProviderLink>,
        vec![fake.clone() as Arc<dyn Driver>],
        agent_config,
    );
    tokio::spawn(agent.run(None, shutdown.clone()));
    tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
    wait_for_registered(&service).await;
    Harness {
        service,
        fake,
        shutdown,
        _dir: dir,
    }
}

/// A restored provider row is listed before the new agent registers, so wait
/// for the registration itself (generation bump), not for presence.
async fn wait_for_registered(service: &Service) {
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
    let error = h.service.pause(&lease.sandbox.id).await.unwrap_err();
    assert_eq!(error.code, Code::FailedPrecondition);
    let error = h
        .service
        .snapshot(SnapshotRequest {
            sandbox_id: lease.sandbox.id.clone(),
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
    assert!(h.service.get(&build.sandbox.id).is_err());
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

    // A preserved sandbox stays usable for its whole window, so the debugging
    // exec below must not race the window: this one is held far longer than
    // an exec can take.
    request.idempotency_key = "keep".into();
    request.ttl_secs = 60;
    let lease = h.service.acquire(request.clone()).await.unwrap();
    h.service
        .release(ReleaseRequest {
            lease_id: lease.lease.id.clone(),
            mode: ReleaseMode::Preserve,
            preserve_ttl_secs: 60,
        })
        .await
        .unwrap();
    let preserved = h.service.get(&lease.sandbox.id).unwrap();
    assert_eq!(preserved.state, SandboxState::Preserved);
    // Still usable for debugging while preserved.
    let (code, _, _) = h
        .service
        .exec_collect(ExecRequest {
            sandbox_id: lease.sandbox.id.clone(),
            argv: vec!["true".into()],
            ..ExecRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(code, 0);

    // A short window ends at its TTL. Only the window elapses here, so the
    // destroy is caused by the TTL itself.
    request.idempotency_key = "short-preserve".into();
    let short = h.service.acquire(request).await.unwrap();
    h.service
        .release(ReleaseRequest {
            lease_id: short.lease.id.clone(),
            mode: ReleaseMode::Preserve,
            preserve_ttl_secs: 1,
        })
        .await
        .unwrap();
    assert_eq!(
        h.service.get(&short.sandbox.id).unwrap().state,
        SandboxState::Preserved
    );
    wait_for(|| h.service.get(&short.sandbox.id).is_err()).await;
    // The long window outlived it: the short sandbox ended by its TTL, not
    // by reconcile.
    assert_eq!(
        h.service.get(&lease.sandbox.id).unwrap().state,
        SandboxState::Preserved
    );
}

#[tokio::test]
async fn reconcile_spares_foreign_orphans_and_fails_lost_sandboxes() {
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
            incarnation: 1,
            driver: "fake".into(),
            ..CreateWork::default()
        })
        .await
        .unwrap();
    // The leased machine disappears behind the service's back.
    h.fake.destroy(&lease.sandbox.native_ref).await.unwrap();
    wait_for(|| {
        h.service
            .get(&lease.sandbox.id)
            .is_ok_and(|s| s.state == SandboxState::Failed)
    })
    .await;
    // The orphan is not ours (another service sharing this host registry may
    // have created it), so reconcile must leave it alone.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        h.fake
            .inventory()
            .await
            .unwrap()
            .iter()
            .any(|o| o.native_ref == orphan),
        "a foreign machine was reaped"
    );
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
        loopback_only: true,
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
    tokio::spawn(agent.run(None, shutdown.clone()));

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
    // Streaming exec with large output crosses batching.
    let (code, stdout, stderr) = client
        .exec_collect(ExecRequest {
            sandbox_id: lease.sandbox.id.clone(),
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
        .upload(&lease.sandbox.id, "/work", &src)
        .await
        .unwrap();
    let out = dir.path().join("out.tar");
    client
        .download(&lease.sandbox.id, "/work/dir", &out)
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

#[tokio::test]
async fn loopback_auth_requires_tokens_and_a_loopback_host() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    async fn raw_status(
        addr: std::net::SocketAddr,
        host: &str,
        path: &str,
        bearer: Option<&str>,
        body: &str,
    ) -> u16 {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let auth = bearer
            .map(|token| format!("Authorization: Bearer {token}\r\n"))
            .unwrap_or_default();
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {host}\r\n{auth}Content-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let head = String::from_utf8_lossy(&response);
        head.split_whitespace()
            .nth(1)
            .and_then(|code| code.parse().ok())
            .unwrap_or_else(|| panic!("no status line in {head:?}"))
    }

    async fn serve(service: Arc<Service>, auth: Auth) -> (std::net::SocketAddr, CancellationToken) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        tokio::spawn(async move {
            axum::serve(listener, router(service, auth))
                .with_graceful_shutdown(async move { stop.cancelled().await })
                .await
                .unwrap();
        });
        (addr, shutdown)
    }

    let dir = tempfile::tempdir().unwrap();
    let service = Service::open(
        store::open("memory:").await.unwrap(),
        ServiceConfig::new(dir.path().join("blobs")),
    )
    .await
    .unwrap();
    let (addr, shutdown) = serve(
        service.clone(),
        Auth {
            client_token: Some("client-secret".into()),
            provider_token: Some("provider-secret".into()),
            loopback_only: true,
        },
    )
    .await;
    let list = "/preloop.sandbox.v1.Sandboxes/List";
    let providers = "/preloop.sandbox.v1.Providers/Poll";

    // No bearer on either API: rejected.
    assert_eq!(raw_status(addr, "127.0.0.1", list, None, "{}").await, 401);
    assert_eq!(
        raw_status(addr, "127.0.0.1", providers, None, "{}").await,
        401
    );
    // A token for the other role is not accepted either.
    assert_eq!(
        raw_status(addr, "127.0.0.1", providers, Some("client-secret"), "{}").await,
        401
    );
    // DNS rebinding: the bearer is valid, the Host is not.
    assert_eq!(
        raw_status(addr, "evil.example", list, Some("client-secret"), "{}").await,
        403
    );
    // Loopback names and ports are fine.
    for host in [
        "127.0.0.1",
        "127.0.0.1:9191",
        "localhost:9191",
        "[::1]:9191",
    ] {
        assert_eq!(
            raw_status(addr, host, list, Some("client-secret"), "{}").await,
            200,
            "Host {host} must be accepted"
        );
    }
    shutdown.cancel();

    // A listener built without tokens fails closed instead of authorizing.
    let (addr, shutdown) = serve(service, Auth::default()).await;
    assert_eq!(
        raw_status(addr, "127.0.0.1", list, Some("anything"), "{}").await,
        401
    );
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
async fn exec_rejects_a_shell_metacharacter_env_key() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let lease = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "env-key".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let marker = h._dir.path().join("pwned");
    let key = format!("A; touch {}", marker.display());
    let error = h
        .service
        .exec_collect(ExecRequest {
            sandbox_id: lease.sandbox.id.clone(),
            argv: vec!["true".into()],
            env: [(key, "1".into())].into_iter().collect(),
            ..ExecRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::InvalidArgument);
    assert!(!marker.exists(), "the env key injected a command");
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
                image: Some("alpine:3.20".into()),
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
    // `Acquire` only places on a provider the service still considers
    // online. A ghost that never reports would be read as already gone if
    // the acquire ran more than `provider_timeout` after it registered, so
    // the test refreshes its liveness itself and stops the heartbeats to
    // make the host lose it.
    let provider_timeout = Duration::from_secs(1);
    let mut config = ServiceConfig::new(dir.path().join("blobs"));
    config.provider_timeout = provider_timeout;
    config.provider_lost_factor = 1;
    config.acquire_wait = Duration::from_millis(200);
    let service = Service::open(store::open("memory:").await.unwrap(), config)
        .await
        .unwrap();
    // A provider that registers and then dies without reporting: its create
    // never completes.
    let ghost = service
        .register(RegisterRequest {
            name: "ghost".into(),
            drivers: vec![DriverInfo {
                driver: "ghost".into(),
                capabilities: FakeDriver::full_capabilities(),
            }],
        })
        .await
        .unwrap();
    let alive = CancellationToken::new();
    {
        let service = service.clone();
        let alive = alive.clone();
        let heartbeat = provider_timeout / 20;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = alive.cancelled() => return,
                    _ = tokio::time::sleep(heartbeat) => {
                        // A fenced or failed heartbeat is irrelevant: the
                        // next tick retries, exactly like an agent.
                        drop(service.heartbeat(HeartbeatRequest {
                            provider_id: ghost.provider_id.clone(),
                            generation: ghost.generation,
                        }));
                    }
                }
            }
        });
    }
    service
        .put_template(TemplateSpec {
            name: "alpine".into(),
            os: "linux".into(),
            image: Some("alpine:3.20".into()),
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
    // The host stops answering: wait past the timeout the service declares
    // it lost by, not past the last heartbeat.
    alive.cancel();
    tokio::time::sleep(provider_timeout * 2).await;
    service.maintain().await;
    let sandboxes = service.list();
    assert!(
        !sandboxes.is_empty() && sandboxes.iter().all(|s| s.state == SandboxState::Failed),
        "{sandboxes:?}"
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
    let exec = |argv: Vec<String>| {
        let service = h.service.clone();
        let id = id.clone();
        async move {
            service
                .exec_collect(ExecRequest {
                    sandbox_id: id,
                    argv,
                    ..ExecRequest::default()
                })
                .await
        }
    };
    assert_eq!(exec(vec!["true".into()]).await.unwrap().0, 0);
    h.service.stop(&id).await.unwrap();
    let error = exec(vec!["true".into()]).await.unwrap_err();
    assert_eq!(error.code, Code::FailedPrecondition);
    assert!(error.message.contains("stopped"), "{}", error.message);
    assert_eq!(h.service.get(&id).unwrap().state, SandboxState::Stopped);
    // A stopped sandbox keeps its lease, so a golden can be packed and
    // restarted forkable under the same lease.
    h.service.start(&id, true).await.unwrap();
    assert_eq!(exec(vec!["true".into()]).await.unwrap().0, 0);
    assert_eq!(h.service.get(&id).unwrap().state, SandboxState::Leased);
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
        h.service.get(&sandbox_id).unwrap().state,
        SandboxState::Leased
    );

    // A machine this service never created is foreign: another service may
    // share the host registry, so it must not be reaped.
    let orphan = h
        .fake
        .create(&CreateWork {
            sandbox_id: "sbx-orphan".into(),
            incarnation: 1,
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
            provider_id: other.provider_id.clone(),
            generation: other.generation,
            sandboxes: observed,
        })
        .await
        .unwrap();
    assert!(
        !response.destroy.iter().any(|w| w.native_ref == orphan),
        "an unknown machine was reaped: {:?}",
        response.destroy
    );

    // A stale incarnation of a sandbox this service did create is ours.
    let stale = h
        .fake
        .create(&CreateWork {
            sandbox_id: sandbox_id.clone(),
            incarnation: 7,
            driver: "fake".into(),
            ..CreateWork::default()
        })
        .await
        .unwrap();
    let response = h
        .service
        .reconcile(ReconcileRequest {
            provider_id: other.provider_id,
            generation: other.generation,
            sandboxes: vec![Observed {
                driver: "fake".into(),
                native_ref: stale.clone(),
                sandbox_id: sandbox_id.clone(),
                incarnation: 7,
            }],
        })
        .await
        .unwrap();
    assert!(
        response.destroy.iter().any(|w| w.native_ref == stale),
        "a stale incarnation of our own sandbox must be reaped"
    );
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
    let sandbox = restored.get(&sandbox_id).unwrap();
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

// ─── At-least-once delivery, exec deadlines and disconnect ───────────

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use preloop_sandbox::agent::Session as ProviderSession;
use preloop_sandbox::service::collect_exec;

/// Causes the agent's reports to be lost, and counts deliveries that repeat.
#[derive(Default)]
struct Lossy {
    /// Drop the next `Report` (the response never reaches the agent).
    drop_report_once: AtomicBool,
    /// Drop this many `ReportExec` calls.
    drop_execs: AtomicUsize,
    /// How many work ids the service delivered more than once.
    redeliveries: AtomicUsize,
    seen: std::sync::Mutex<std::collections::HashSet<String>>,
}

struct LossyLink {
    inner: Arc<Service>,
    lossy: Arc<Lossy>,
}

#[async_trait::async_trait]
impl ProviderLink for LossyLink {
    async fn register(
        &self,
        request: RegisterRequest,
    ) -> preloop_sandbox::Result<RegisterResponse> {
        Service::register(&self.inner, request).await
    }
    async fn heartbeat(&self, request: HeartbeatRequest) -> preloop_sandbox::Result<()> {
        Service::heartbeat(&self.inner, request)
    }
    async fn poll(&self, request: PollRequest) -> preloop_sandbox::Result<PollResponse> {
        let response = Service::poll(&self.inner, request).await?;
        let mut seen = self.lossy.seen.lock().unwrap();
        for item in &response.work {
            if !seen.insert(item.id.clone()) {
                self.lossy.redeliveries.fetch_add(1, Ordering::AcqRel);
            }
        }
        Ok(response)
    }
    async fn report(&self, request: ReportRequest) -> preloop_sandbox::Result<()> {
        if self.lossy.drop_report_once.swap(false, Ordering::AcqRel) {
            return Err(preloop_sandbox::Error::unavailable("injected lost report"));
        }
        Service::report(&self.inner, request).await
    }
    async fn report_exec(&self, request: ReportExecRequest) -> preloop_sandbox::Result<()> {
        if self
            .lossy
            .drop_execs
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(preloop_sandbox::Error::unavailable(
                "injected lost ReportExec",
            ));
        }
        Service::report_exec(&self.inner, request).await
    }
    async fn reconcile(
        &self,
        request: ReconcileRequest,
    ) -> preloop_sandbox::Result<ReconcileResponse> {
        Service::reconcile(&self.inner, request).await
    }
    async fn fetch_blob(
        &self,
        id: &ProviderSession,
        ticket: &str,
        dest: &std::path::Path,
    ) -> preloop_sandbox::Result<()> {
        <Arc<Service> as ProviderLink>::fetch_blob(&self.inner, id, ticket, dest).await
    }
    async fn put_blob(
        &self,
        id: &ProviderSession,
        ticket: &str,
        src: &std::path::Path,
    ) -> preloop_sandbox::Result<()> {
        <Arc<Service> as ProviderLink>::put_blob(&self.inner, id, ticket, src).await
    }
}

/// One service, one agent whose reports can be dropped, no warm pool.
async fn lossy_harness(
    configure: impl FnOnce(&mut ServiceConfig),
    retry_backoff: Duration,
) -> (Harness, Arc<Lossy>) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = ServiceConfig::new(dir.path().join("blobs"));
    config.acquire_wait = Duration::from_secs(10);
    configure(&mut config);
    let service = Service::open(store::open("memory:").await.unwrap(), config)
        .await
        .unwrap();
    let fake = Arc::new(FakeDriver::new(
        "fake",
        dir.path().join("host"),
        FakeDriver::full_capabilities(),
    ));
    let lossy = Arc::new(Lossy::default());
    let link: Arc<dyn ProviderLink> = Arc::new(LossyLink {
        inner: service.clone(),
        lossy: lossy.clone(),
    });
    let mut agent_config = AgentConfig::new("host-a", dir.path().join("scratch"));
    agent_config.reconcile = Duration::from_millis(300);
    agent_config.retry_backoff = retry_backoff;
    let shutdown = CancellationToken::new();
    let agent = Agent::new(link, vec![fake.clone() as Arc<dyn Driver>], agent_config);
    tokio::spawn(agent.run(None, shutdown.clone()));
    tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
    wait_for_registered(&service).await;
    (
        Harness {
            service,
            fake,
            shutdown,
            _dir: dir,
        },
        lossy,
    )
}

/// A service with one registered provider and no agent: work stays queued
/// until the test polls and reports it by hand.
async fn manual_service(
    configure: impl FnOnce(&mut ServiceConfig),
) -> (Arc<Service>, tempfile::TempDir, RegisterResponse) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = ServiceConfig::new(dir.path().join("blobs"));
    configure(&mut config);
    let service = Service::open(store::open("memory:").await.unwrap(), config)
        .await
        .unwrap();
    service.put_template(template("alpine")).await.unwrap();
    let provider = service
        .register(RegisterRequest {
            name: "manual".into(),
            drivers: vec![DriverInfo {
                driver: "fake".into(),
                capabilities: FakeDriver::full_capabilities(),
            }],
        })
        .await
        .unwrap();
    (service, dir, provider)
}

async fn poll_now(service: &Arc<Service>, provider: &RegisterResponse) -> Vec<WorkItem> {
    service
        .poll(PollRequest {
            provider_id: provider.provider_id.clone(),
            generation: provider.generation,
            wait_secs: 0,
        })
        .await
        .unwrap()
        .work
}

/// Acquire without blocking, leaving the create work queued for the test.
async fn acquire_unreported(service: &Arc<Service>, key: &str) {
    let error = service
        .acquire(AcquireRequest {
            idempotency_key: key.into(),
            template: "alpine".into(),
            wait_secs: Some(0),
            ..AcquireRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::DeadlineExceeded);
}

#[tokio::test]
async fn poll_returns_at_most_the_poll_batch() {
    let (service, _dir, provider) = manual_service(|config| config.poll_batch = 2).await;
    for key in ["a", "b", "c", "d", "e"] {
        acquire_unreported(&service, key).await;
    }
    assert_eq!(poll_now(&service, &provider).await.len(), 2);
    assert_eq!(poll_now(&service, &provider).await.len(), 2);
    assert_eq!(poll_now(&service, &provider).await.len(), 1);
    assert!(poll_now(&service, &provider).await.is_empty());
}

#[tokio::test]
async fn unacknowledged_work_is_redelivered_and_re_registering_releases_it() {
    let (service, _dir, provider) =
        manual_service(|config| config.delivery_timeout = Duration::from_millis(100)).await;
    acquire_unreported(&service, "first").await;
    let delivered = poll_now(&service, &provider).await;
    assert_eq!(delivered.len(), 1);
    let work_id = delivered[0].id.clone();
    // Still in flight: not delivered again early.
    assert!(poll_now(&service, &provider).await.is_empty());
    tokio::time::sleep(Duration::from_millis(150)).await;
    let redelivered = poll_now(&service, &provider).await;
    assert_eq!(redelivered.len(), 1);
    assert_eq!(redelivered[0].id, work_id, "same work id, redelivered");
    // Acknowledging it completes the work; a duplicate report is not found.
    service
        .report(ReportRequest {
            provider_id: provider.provider_id.clone(),
            generation: provider.generation,
            work_id: work_id.clone(),
            ok: true,
            native_ref: "psbx-manual-i1".into(),
            error: String::new(),
        })
        .await
        .unwrap();
    let duplicate = service
        .report(ReportRequest {
            provider_id: provider.provider_id.clone(),
            generation: provider.generation,
            work_id,
            ok: true,
            native_ref: "psbx-manual-i1".into(),
            error: String::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(duplicate.code, Code::NotFound);

    // Work the next generation has not acknowledged is redelivered to it.
    acquire_unreported(&service, "second").await;
    let pending = poll_now(&service, &provider).await;
    assert_eq!(pending.len(), 1);
    let pending_id = pending[0].id.clone();
    let again = service
        .register(RegisterRequest {
            name: "manual".into(),
            drivers: vec![DriverInfo {
                driver: "fake".into(),
                capabilities: FakeDriver::full_capabilities(),
            }],
        })
        .await
        .unwrap();
    assert!(again.generation > provider.generation);
    let released = poll_now(&service, &again).await;
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].id, pending_id);
}

#[tokio::test]
async fn report_exec_orders_by_sequence_and_closes_on_exit() {
    let (service, _dir, provider) = manual_service(|_| {}).await;
    acquire_unreported(&service, "k").await;
    let create = poll_now(&service, &provider).await;
    assert_eq!(create.len(), 1);
    service
        .report(ReportRequest {
            provider_id: provider.provider_id.clone(),
            generation: provider.generation,
            work_id: create[0].id.clone(),
            ok: true,
            native_ref: "psbx-manual-i1".into(),
            error: String::new(),
        })
        .await
        .unwrap();
    let sandbox_id = service.list()[0].id.clone();
    let stream = service
        .exec(ExecRequest {
            sandbox_id,
            argv: vec!["true".into()],
            ..ExecRequest::default()
        })
        .unwrap();
    let work = poll_now(&service, &provider).await;
    assert_eq!(work.len(), 1);
    let exec_id = work[0].exec.as_ref().unwrap().exec_id.clone();
    let report = |events: Vec<ExecEvent>, sequence: u64| ReportExecRequest {
        provider_id: provider.provider_id.clone(),
        generation: provider.generation,
        exec_id: exec_id.clone(),
        events,
        sequence,
    };
    service
        .report_exec(report(vec![ExecEvent::stdout(b"a".to_vec())], 1))
        .await
        .unwrap();
    // A retried batch is dropped instead of duplicated.
    service
        .report_exec(report(vec![ExecEvent::stdout(b"b".to_vec())], 1))
        .await
        .unwrap();
    // The exit closes the stream; events after it in the batch are ignored.
    service
        .report_exec(report(
            vec![
                ExecEvent::stdout(b"c".to_vec()),
                ExecEvent::exit(0),
                ExecEvent::stdout(b"d".to_vec()),
            ],
            2,
        ))
        .await
        .unwrap();
    let (code, stdout, stderr) = collect_exec(stream).await.unwrap();
    assert_eq!(code, 0);
    assert_eq!(stdout, b"ac");
    assert!(stderr.is_empty());
    let late = service
        .report_exec(report(vec![ExecEvent::stdout(b"late".to_vec())], 3))
        .await
        .unwrap_err();
    assert_eq!(late.code, Code::NotFound);
    service
        .report(ReportRequest {
            provider_id: provider.provider_id.clone(),
            generation: provider.generation,
            work_id: work[0].id.clone(),
            ok: true,
            native_ref: String::new(),
            error: String::new(),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn exec_without_a_timeout_gets_the_service_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let host = dir.path().join("host");
    let mut config = ServiceConfig::new(dir.path().join("blobs"));
    config.exec_timeout = Duration::from_secs(1);
    let h = harness_with_host_config(
        store::open("memory:").await.unwrap(),
        dir,
        host,
        FakeDriver::full_capabilities(),
        config,
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
    let started = std::time::Instant::now();
    let (code, _, _) = h
        .service
        .exec_collect(ExecRequest {
            sandbox_id: lease.sandbox.id.clone(),
            argv: vec!["sh".into(), "-c".into(), "sleep 30".into()],
            ..ExecRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(code, 124, "the service deadline killed the command");
    assert!(started.elapsed() < Duration::from_secs(10));
}

#[tokio::test]
async fn a_swept_exec_reports_the_timeout_exit_not_an_error() {
    // A provider that takes the work but never reports (a lost agent, or one
    // stalled long enough that the backstop wins): the caller must still see
    // the driver's timeout exit, not a scheduler-dependent error.
    let (service, _dir, provider) =
        manual_service(|config| config.exec_timeout = Duration::from_secs(1)).await;
    acquire_unreported(&service, "k").await;
    let create = poll_now(&service, &provider).await;
    assert_eq!(create.len(), 1);
    service
        .report(ReportRequest {
            provider_id: provider.provider_id.clone(),
            generation: provider.generation,
            work_id: create[0].id.clone(),
            ok: true,
            native_ref: "psbx-manual-i1".into(),
            error: String::new(),
        })
        .await
        .unwrap();
    let stream = service
        .exec(ExecRequest {
            sandbox_id: service.list()[0].id.clone(),
            argv: vec!["sleep".into(), "30".into()],
            ..ExecRequest::default()
        })
        .unwrap();
    // Delivered but never run: no exit report will ever arrive.
    let work = poll_now(&service, &provider).await;
    assert_eq!(work.len(), 1);
    assert!(work[0].exec.is_some());
    // The sweep fires once the deadline and its grace have passed.
    tokio::time::sleep(Duration::from_millis(6_100)).await;
    service.maintain().await;
    let (code, stdout, stderr) = collect_exec(stream).await.unwrap();
    assert_eq!(code, 124);
    assert!(stdout.is_empty() && stderr.is_empty());
    // The backstop also asks the provider to kill the guest command.
    let cancel = poll_now(&service, &provider).await;
    assert_eq!(cancel.len(), 1);
    assert!(cancel[0].cancel_exec.is_some());
}

#[tokio::test]
async fn dropping_an_exec_stream_cancels_the_command() {
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
    let stream = h
        .service
        .exec(ExecRequest {
            sandbox_id: lease.sandbox.id.clone(),
            argv: vec!["sh".into(), "-c".into(), "sleep 30".into()],
            ..ExecRequest::default()
        })
        .unwrap();
    // Let the work reach the agent, then walk away.
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(stream);
    wait_for(|| h.fake.cancel_calls.load(Ordering::Acquire) > 0).await;
    // The sandbox is still usable: the dropped exec left no state behind.
    let (code, _, _) = h
        .service
        .exec_collect(ExecRequest {
            sandbox_id: lease.sandbox.id.clone(),
            argv: vec!["true".into()],
            ..ExecRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(code, 0);
}

#[tokio::test]
async fn a_lost_report_exec_is_retried_until_the_caller_sees_the_exit() {
    let (h, lossy) = lossy_harness(|_| {}, Duration::from_millis(20)).await;
    lossy.drop_execs.store(1, Ordering::Release);
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
    let (code, stdout, _) = h
        .service
        .exec_collect(ExecRequest {
            sandbox_id: lease.sandbox.id.clone(),
            argv: vec!["sh".into(), "-c".into(), "printf hi".into()],
            ..ExecRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(lossy.drop_execs.load(Ordering::Acquire), 0, "retried");
    assert_eq!(code, 0);
    assert_eq!(stdout, b"hi");
}

#[tokio::test]
async fn a_redelivered_create_is_not_executed_twice() {
    let (h, lossy) = lossy_harness(
        |config| config.delivery_timeout = Duration::from_millis(100),
        // Slower than the visibility timeout, so a redelivery happens first.
        Duration::from_millis(400),
    )
    .await;
    lossy.drop_report_once.store(true, Ordering::Release);
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
    assert_eq!(lease.sandbox.state, SandboxState::Leased);
    assert!(
        lossy.redeliveries.load(Ordering::Acquire) > 0,
        "the unacknowledged create was redelivered"
    );
    assert_eq!(
        h.fake.create_calls.load(Ordering::Acquire),
        1,
        "a redelivered work item must not run again"
    );
}

#[tokio::test]
async fn a_snapshot_wait_is_bounded_by_the_work_timeout() {
    let (service, _dir, provider) =
        manual_service(|config| config.work_timeout = Duration::from_millis(100)).await;
    acquire_unreported(&service, "k").await;
    let create = poll_now(&service, &provider).await;
    service
        .report(ReportRequest {
            provider_id: provider.provider_id.clone(),
            generation: provider.generation,
            work_id: create[0].id.clone(),
            ok: true,
            native_ref: "psbx-manual-i1".into(),
            error: String::new(),
        })
        .await
        .unwrap();
    let sandbox_id = service.list()[0].id.clone();
    let error = service
        .snapshot(SnapshotRequest {
            sandbox_id,
            name: "golden".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::Unavailable);
    assert!(error.message.contains("snapshot"), "{}", error.message);
}

/// The caller is a network client, so its disconnect is observed through the
/// response body, not through a shared channel. Dropping the stream must
/// still queue `CancelExec` for the provider while the service stays alive.
#[tokio::test]
async fn dropping_a_remote_exec_stream_cancels_the_command() {
    use preloop_sandbox::SandboxApi as _;
    let dir = tempfile::tempdir().unwrap();
    let service = Service::open(
        store::open("memory:").await.unwrap(),
        ServiceConfig::new(dir.path().join("blobs")),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let auth = Auth {
        client_token: Some("client-secret".into()),
        provider_token: Some("provider-secret".into()),
        loopback_only: true,
    };
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
        Arc::new(RemoteLink::new(&base, Some("provider-secret".into()))) as Arc<dyn ProviderLink>,
        vec![fake.clone() as Arc<dyn Driver>],
        AgentConfig::new("remote-host", dir.path().join("scratch")),
    );
    tokio::spawn(agent.run(None, shutdown.clone()));
    let client = SandboxClient::new(&base, Some("client-secret".into()));
    wait_for(|| !service.list_providers().is_empty()).await;
    client.put_template(template("alpine")).await.unwrap();
    let lease = client
        .acquire(AcquireRequest {
            idempotency_key: "k".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let stream = client
        .exec(ExecRequest {
            sandbox_id: lease.sandbox.id.clone(),
            argv: vec!["sh".into(), "-c".into(), "sleep 30".into()],
            ..ExecRequest::default()
        })
        .await
        .unwrap();
    // Let the work reach the agent, then walk away while the service lives.
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(stream);
    wait_for(|| fake.cancel_calls.load(Ordering::Acquire) > 0).await;
    // The sandbox is still usable: the dropped exec left no state behind.
    let (code, _, _) = client
        .exec_collect(ExecRequest {
            sandbox_id: lease.sandbox.id,
            argv: vec!["true".into()],
            ..ExecRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(code, 0);
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// A service with a registered provider that never polls: acquires create
/// sandboxes and leases, but nothing ever finishes provisioning them.
async fn unclaimed_service(dir: &tempfile::TempDir, unclaimed: Duration) -> Arc<Service> {
    let mut config = ServiceConfig::new(dir.path().join("blobs"));
    config.unclaimed_lease_ttl = unclaimed;
    let service = Service::open(store::open("memory:").await.unwrap(), config)
        .await
        .unwrap();
    service
        .register(RegisterRequest {
            name: "host-a".into(),
            drivers: vec![DriverInfo {
                driver: "fake".into(),
                capabilities: FakeDriver::full_capabilities(),
            }],
        })
        .await
        .unwrap();
    service.put_template(template("alpine")).await.unwrap();
    service
        .put_pool(PoolSpec {
            name: "p".into(),
            template: "alpine".into(),
            ..PoolSpec::default()
        })
        .await
        .unwrap();
    service
}

#[tokio::test]
async fn pool_acquire_rejects_template_placement_fields() {
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
    let base = || AcquireRequest {
        idempotency_key: "k".into(),
        pool: "p".into(),
        ..AcquireRequest::default()
    };
    let cases: Vec<(&str, AcquireRequest)> = vec![
        (
            "shape",
            AcquireRequest {
                shape: Shape {
                    cpus: 2,
                    ..Shape::default()
                },
                ..base()
            },
        ),
        (
            "network",
            AcquireRequest {
                network: NetworkPolicy {
                    mode: NetworkMode::Unrestricted,
                    ..NetworkPolicy::default()
                },
                ..base()
            },
        ),
        (
            "mounts",
            AcquireRequest {
                mounts: vec![Mount {
                    host: "/tmp".into(),
                    guest: "/mnt".into(),
                    readonly: true,
                }],
                ..base()
            },
        ),
        (
            "sockets",
            AcquireRequest {
                sockets: vec![SocketMount {
                    host: "/tmp/sock".into(),
                    guest: "/run/sock".into(),
                }],
                ..base()
            },
        ),
        (
            "options",
            AcquireRequest {
                options: [("dns".to_owned(), "8.8.8.8".to_owned())].into(),
                ..base()
            },
        ),
    ];
    for (field, request) in cases {
        let error = h.service.acquire(request).await.unwrap_err();
        assert_eq!(error.code, Code::InvalidArgument);
        assert!(error.message.contains(field), "{}", error.message);
        assert!(
            error
                .message
                .contains("set these on the pool, or acquire from the template"),
            "{}",
            error.message
        );
    }
    assert!(h.service.list().is_empty(), "nothing was created");
}

#[tokio::test]
async fn abandoned_unclaimed_lease_is_released_after_the_unclaimed_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let service = unclaimed_service(&dir, Duration::ZERO).await;
    let error = service
        .acquire(AcquireRequest {
            idempotency_key: "abandoned".into(),
            pool: "p".into(),
            wait_secs: Some(0),
            ..AcquireRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::DeadlineExceeded);
    assert_eq!(service.list().len(), 1, "the lease is provisioning");
    service.maintain().await;
    assert!(
        service.list().is_empty(),
        "the abandoned lease was released"
    );
}

#[tokio::test]
async fn waiting_acquire_keeps_its_lease_past_the_unclaimed_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let service = unclaimed_service(&dir, Duration::ZERO).await;
    let handle = tokio::spawn({
        let service = service.clone();
        async move {
            service
                .acquire(AcquireRequest {
                    idempotency_key: "waiting".into(),
                    pool: "p".into(),
                    ..AcquireRequest::default()
                })
                .await
        }
    });
    wait_for(|| service.list().len() == 1).await;
    service.maintain().await;
    assert_eq!(service.list().len(), 1, "a waiting caller keeps its lease");
    handle.abort();
    let _ = handle.await;
    service.maintain().await;
    assert!(
        service.list().is_empty(),
        "once no caller waits the lease expires"
    );
}

#[tokio::test]
async fn delivered_lease_gets_the_full_ttl() {
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
    let response = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "delivered".into(),
            pool: "p".into(),
            ttl_secs: 3600,
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(response.lease.requested_ttl_secs, 3600);
    let remaining = response.lease.expires_at - unix_now();
    assert!(
        remaining > 3000,
        "delivery restores the full TTL, got {remaining}s"
    );
}

#[tokio::test]
async fn injected_files_default_to_owner_only_mode() {
    let dir = tempfile::tempdir().unwrap();
    let host = dir.path().join("host");
    let h = harness_with_host(
        store::open("memory:").await.unwrap(),
        tempfile::tempdir().unwrap(),
        host.clone(),
        FakeDriver::full_capabilities(),
    )
    .await;
    h.service.put_template(template("alpine")).await.unwrap();
    let mut request = AcquireRequest {
        idempotency_key: "mode".into(),
        template: "alpine".into(),
        ..AcquireRequest::default()
    };
    request.inject.files.push(FileSpec {
        path: "/run/secret".into(),
        data: b"token".to_vec(),
        mode: 0,
    });
    let lease = h.service.acquire(request).await.unwrap();
    let path = host
        .join("sandboxes")
        .join(&lease.sandbox.native_ref)
        .join("run/secret");
    use std::os::unix::fs::PermissionsExt as _;
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "injected files are owner-only by default");
}

#[tokio::test]
async fn restart_keeps_the_requested_lease_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sandbox.db");
    {
        let mut config = ServiceConfig::new(dir.path().join("blobs"));
        config.unclaimed_lease_ttl = Duration::from_secs(5);
        let service = Service::open(store::open(db.to_str().unwrap()).await.unwrap(), config)
            .await
            .unwrap();
        service
            .register(RegisterRequest {
                name: "host-a".into(),
                drivers: vec![DriverInfo {
                    driver: "fake".into(),
                    capabilities: FakeDriver::full_capabilities(),
                }],
            })
            .await
            .unwrap();
        service.put_template(template("alpine")).await.unwrap();
        service
            .put_pool(PoolSpec {
                name: "p".into(),
                template: "alpine".into(),
                ..PoolSpec::default()
            })
            .await
            .unwrap();
        let error = service
            .acquire(AcquireRequest {
                idempotency_key: "kept".into(),
                pool: "p".into(),
                ttl_secs: 3600,
                wait_secs: Some(0),
                ..AcquireRequest::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.code, Code::DeadlineExceeded);
    }
    let store = store::open(db.to_str().unwrap()).await.unwrap();
    let leases: Vec<Lease> = store
        .load(Kind::Leases)
        .await
        .unwrap()
        .into_iter()
        .map(|value| serde_json::from_value(value).unwrap())
        .collect();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].requested_ttl_secs, 3600);
    let remaining = leases[0].expires_at - unix_now();
    assert!(
        remaining <= 6,
        "an unclaimed lease expires in the short window, got {remaining}s"
    );
    let restored = Service::open(store, ServiceConfig::new(dir.path().join("blobs2")))
        .await
        .unwrap();
    assert_eq!(restored.list().len(), 1);
}

#[tokio::test]
async fn leases_persisted_before_requested_ttls_are_treated_as_delivered() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sandbox.db");
    let expires = unix_now() + 100;
    {
        let store = store::open(db.to_str().unwrap()).await.unwrap();
        store
            .put(
                Kind::Leases,
                "lse-old",
                &serde_json::json!({
                    "id": "lse-old",
                    "sandboxId": "sbx-old",
                    "idempotencyKey": "legacy",
                    "expiresAt": expires,
                }),
            )
            .await
            .unwrap();
        store
            .put(
                Kind::Sandboxes,
                "sbx-old",
                &serde_json::json!({
                    "id": "sbx-old",
                    "template": "alpine",
                    "pool": "p",
                    "providerId": "prv-x",
                    "driver": "fake",
                    "state": "LEASED",
                    "leaseId": "lse-old",
                    "createdAt": unix_now(),
                }),
            )
            .await
            .unwrap();
    }
    let service = Service::open(
        store::open(db.to_str().unwrap()).await.unwrap(),
        ServiceConfig::new(dir.path().join("blobs")),
    )
    .await
    .unwrap();
    let response = service
        .acquire(AcquireRequest {
            idempotency_key: "legacy".into(),
            template: "gone".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(response.lease.id, "lse-old");
    assert_eq!(response.lease.requested_ttl_secs, 0);
    assert_eq!(
        response.lease.expires_at, expires,
        "a pre-change lease keeps its expiry"
    );
}

#[tokio::test]
async fn replayed_acquire_of_a_failed_sandbox_persists_its_release() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sandbox.db");
    {
        let h = harness_with(
            store::open(db.to_str().unwrap()).await.unwrap(),
            tempfile::tempdir().unwrap(),
            FakeDriver::full_capabilities(),
        )
        .await;
        h.service.put_template(template("alpine")).await.unwrap();
        h.fake
            .fail_creates
            .store(1, std::sync::atomic::Ordering::Release);
        let error = h
            .service
            .acquire(AcquireRequest {
                idempotency_key: "k".into(),
                template: "alpine".into(),
                ..AcquireRequest::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.code, Code::Unavailable);
    }
    // The replay's failure path removed the lease in memory. That removal
    // must reach the store, or a restart resurrects a dead key.
    let store = store::open(db.to_str().unwrap()).await.unwrap();
    assert!(
        store.load(Kind::Leases).await.unwrap().is_empty(),
        "the failed replay's lease release was not persisted"
    );
    assert!(
        store.load(Kind::Sandboxes).await.unwrap().is_empty(),
        "the failed replay's sandbox delete was not persisted"
    );
}

#[tokio::test]
async fn a_preserved_pool_member_never_returns_to_the_warm_set() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    h.service
        .put_pool(PoolSpec {
            name: "p".into(),
            template: "alpine".into(),
            warm: 1,
            ..PoolSpec::default()
        })
        .await
        .unwrap();
    wait_for(|| {
        h.service
            .list()
            .iter()
            .any(|s| s.pool == "p" && s.state == SandboxState::Ready)
    })
    .await;
    let idle = h.service.list()[0].clone();
    let error = h.service.pause(&idle.id).await.unwrap_err();
    assert_eq!(error.code, Code::FailedPrecondition);

    let lease = h.service.acquire(acquire("k", "p")).await.unwrap();
    let id = lease.sandbox.id.clone();
    h.service.pause(&id).await.unwrap();
    h.service
        .release(ReleaseRequest {
            lease_id: lease.lease.id,
            mode: ReleaseMode::Preserve,
            preserve_ttl_secs: 60,
        })
        .await
        .unwrap();
    let paused = h.service.get(&id).unwrap();
    assert_eq!(
        paused.state,
        SandboxState::Paused,
        "PRESERVE kept power state"
    );
    assert!(paused.preserve_until > unix_now());
    h.service.resume(&id).await.unwrap();
    let resumed = h.service.get(&id).unwrap();
    assert_eq!(
        resumed.state,
        SandboxState::Preserved,
        "a machine that held a lease must never rejoin the warm set"
    );
    assert!(resumed.preserve_until > unix_now());
    h.service.maintain().await;
    assert_eq!(
        h.service.get(&id).unwrap().state,
        SandboxState::Preserved,
        "the preserved member must not be swept before its deadline"
    );
    // The next acquire gets a clean machine, not the used one.
    let fresh = h.service.acquire(acquire("k2", "p")).await.unwrap();
    assert_ne!(fresh.sandbox.id, id);
}

#[tokio::test]
async fn paused_and_stopped_debug_sandboxes_expire_from_the_preserve_window() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let mut request = AcquireRequest {
        idempotency_key: "pause".into(),
        template: "alpine".into(),
        ..AcquireRequest::default()
    };
    let paused = h.service.acquire(request.clone()).await.unwrap();
    h.service.pause(&paused.sandbox.id).await.unwrap();
    h.service
        .release(ReleaseRequest {
            lease_id: paused.lease.id,
            mode: ReleaseMode::Preserve,
            preserve_ttl_secs: 1,
        })
        .await
        .unwrap();
    assert_eq!(
        h.service.get(&paused.sandbox.id).unwrap().state,
        SandboxState::Paused
    );

    request.idempotency_key = "stop".into();
    let stopped = h.service.acquire(request).await.unwrap();
    h.service.stop(&stopped.sandbox.id).await.unwrap();
    h.service
        .release(ReleaseRequest {
            lease_id: stopped.lease.id,
            mode: ReleaseMode::Preserve,
            preserve_ttl_secs: 1,
        })
        .await
        .unwrap();
    assert_eq!(
        h.service.get(&stopped.sandbox.id).unwrap().state,
        SandboxState::Stopped
    );

    tokio::time::sleep(Duration::from_millis(1200)).await;
    h.service.maintain().await;
    wait_for(|| h.service.get(&paused.sandbox.id).is_err()).await;
    wait_for(|| h.service.get(&stopped.sandbox.id).is_err()).await;
}

#[tokio::test]
async fn renew_keeps_the_acquired_ttl() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let lease = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "k".into(),
            template: "alpine".into(),
            ttl_secs: 120,
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let renewed = h
        .service
        .renew(RenewRequest {
            lease_id: lease.lease.id,
            ttl_secs: 0,
        })
        .await
        .unwrap();
    let now = unix_now();
    assert_eq!(renewed.requested_ttl_secs, 120);
    assert!(
        renewed.expires_at >= now + 115 && renewed.expires_at <= now + 125,
        "renew without a TTL used the service default: {}",
        renewed.expires_at - now
    );

    // A pool lease keeps the pool's TTL.
    h.service
        .put_pool(PoolSpec {
            name: "p".into(),
            template: "alpine".into(),
            lease_ttl_secs: 90,
            ..PoolSpec::default()
        })
        .await
        .unwrap();
    let pooled = h.service.acquire(acquire("pooled", "p")).await.unwrap();
    let renewed = h
        .service
        .renew(RenewRequest {
            lease_id: pooled.lease.id,
            ttl_secs: 0,
        })
        .await
        .unwrap();
    let now = unix_now();
    assert!(
        renewed.expires_at >= now + 85 && renewed.expires_at <= now + 95,
        "renew lost the pool TTL: {}",
        renewed.expires_at - now
    );
}

#[tokio::test]
async fn delete_template_refuses_while_a_clone_is_live() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let source = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "source".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    h.service
        .snapshot(SnapshotRequest {
            sandbox_id: source.sandbox.id.clone(),
            name: "golden".into(),
        })
        .await
        .unwrap();
    let clone = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "clone".into(),
            template: "golden".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let error = h.service.delete_template("golden").await.unwrap_err();
    assert_eq!(error.code, Code::FailedPrecondition);
    assert!(
        error.message.contains(&clone.sandbox.id),
        "{}",
        error.message
    );
    h.service
        .release(ReleaseRequest {
            lease_id: clone.lease.id,
            mode: ReleaseMode::Destroy,
            ..ReleaseRequest::default()
        })
        .await
        .unwrap();
    wait_for(|| Service::list(&h.service).is_empty()).await;
    h.service.delete_template("golden").await.unwrap();
}

#[tokio::test]
async fn a_snapshot_in_flight_reserves_its_source() {
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
    h.fake
        .snapshot_delay_ms
        .store(300, std::sync::atomic::Ordering::Release);
    let snapshot = tokio::spawn({
        let service = h.service.clone();
        let id = id.clone();
        async move {
            service
                .snapshot(SnapshotRequest {
                    sandbox_id: id,
                    name: "golden".into(),
                })
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    // A second snapshot and a release must fail while the first is in flight.
    let second = h
        .service
        .snapshot(SnapshotRequest {
            sandbox_id: id.clone(),
            name: "other".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(second.code, Code::FailedPrecondition);
    let release = h
        .service
        .release(ReleaseRequest {
            lease_id: lease.lease.id,
            mode: ReleaseMode::Destroy,
            ..ReleaseRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(release.code, Code::FailedPrecondition);

    // A lifecycle change (pause) during the capture invalidates the result.
    h.service.pause(&id).await.unwrap();
    let error = snapshot.await.unwrap().unwrap_err();
    assert_eq!(error.code, Code::FailedPrecondition, "{}", error.message);
    assert!(
        h.service
            .list_templates()
            .iter()
            .all(|t| t.spec.name != "golden")
    );
}

#[tokio::test]
async fn a_reconfigured_pool_never_serves_an_old_warm_member() {
    let h = harness().await;
    h.service
        .put_template(TemplateSpec {
            name: "one".into(),
            os: "linux".into(),
            image: Some("alpine:3.20".into()),
            ..TemplateSpec::default()
        })
        .await
        .unwrap();
    h.service
        .put_template(TemplateSpec {
            name: "two".into(),
            os: "linux".into(),
            image: Some("alpine:3.21".into()),
            ..TemplateSpec::default()
        })
        .await
        .unwrap();
    h.service
        .put_pool(PoolSpec {
            name: "p".into(),
            template: "one".into(),
            warm: 1,
            ..PoolSpec::default()
        })
        .await
        .unwrap();
    wait_for(|| {
        h.service
            .list()
            .iter()
            .any(|s| s.state == SandboxState::Ready && s.template == "one")
    })
    .await;
    let old = h.service.list()[0].id.clone();

    // Scaling the pool does not change how members are built.
    h.service
        .put_pool(PoolSpec {
            name: "p".into(),
            template: "one".into(),
            warm: 2,
            ..PoolSpec::default()
        })
        .await
        .unwrap();
    h.service.maintain().await;
    assert!(
        h.service.get(&old).is_ok(),
        "a warm-count change retired the warm member"
    );

    h.service
        .put_pool(PoolSpec {
            name: "p".into(),
            template: "two".into(),
            warm: 1,
            ..PoolSpec::default()
        })
        .await
        .unwrap();
    h.service.maintain().await;
    wait_for(|| {
        h.service
            .list()
            .iter()
            .any(|s| s.state == SandboxState::Ready && s.template == "two")
    })
    .await;
    assert!(
        h.service.get(&old).is_err(),
        "the old-revision warm member was not retired"
    );
    let lease = h.service.acquire(acquire("k", "p")).await.unwrap();
    assert_eq!(lease.sandbox.template, "two");
}

#[tokio::test]
async fn replayed_acquire_of_a_stopped_sandbox_returns_it() {
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
    h.service.stop(&lease.sandbox.id).await.unwrap();
    let replay = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "k".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(replay.sandbox.id, lease.sandbox.id);
    assert_eq!(replay.sandbox.state, SandboxState::Stopped);
}

#[tokio::test]
async fn a_ticket_authorises_exactly_one_transfer() {
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
    let ticket = h
        .service
        .open_stream(OpenStreamRequest {
            sandbox_id: lease.sandbox.id,
            kind: StreamKind::Download,
            path: "/work".into(),
        })
        .unwrap();
    let id = ticket.url.rsplit('/').next().unwrap();
    h.service.claim_ticket(id, &ticket.token).unwrap();
    let error = h.service.claim_ticket(id, &ticket.token).unwrap_err();
    assert_eq!(error.code, Code::NotFound);
}

#[tokio::test]
async fn pool_acquire_rejects_a_build_purpose() {
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
    let error = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "k".into(),
            pool: "p".into(),
            purpose: Purpose::Build,
            ..AcquireRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::InvalidArgument);
    assert!(error.message.contains("build"), "{}", error.message);
}

#[tokio::test]
async fn reconcile_protects_snapshot_bases_from_any_provider() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let source = h
        .service
        .acquire(AcquireRequest {
            idempotency_key: "source".into(),
            template: "alpine".into(),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let golden = h
        .service
        .snapshot(SnapshotRequest {
            sandbox_id: source.sandbox.id,
            name: "golden".into(),
        })
        .await
        .unwrap();
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
    let response = h
        .service
        .reconcile(ReconcileRequest {
            provider_id: other.provider_id,
            generation: other.generation,
            sandboxes: vec![Observed {
                driver: "fake".into(),
                native_ref: golden.native_ref.clone(),
                sandbox_id: String::new(),
                incarnation: 0,
            }],
        })
        .await
        .unwrap();
    assert!(
        response.destroy.is_empty(),
        "another provider reaped the snapshot base: {:?}",
        response.destroy
    );
}

#[tokio::test]
async fn concurrent_same_key_acquires_inject_once() {
    let h = harness().await;
    h.service.put_template(template("alpine")).await.unwrap();
    let request = AcquireRequest {
        idempotency_key: "same".into(),
        template: "alpine".into(),
        inject: Inject {
            files: vec![FileSpec {
                path: "/work/value".into(),
                data: b"injected".to_vec(),
                mode: 0,
            }],
            ..Inject::default()
        },
        ..AcquireRequest::default()
    };
    let (first, second) = tokio::join!(
        h.service.acquire(request.clone()),
        h.service.acquire(request)
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.sandbox.id, second.sandbox.id);
    let (code, stdout, _) = h
        .service
        .exec_collect(ExecRequest {
            sandbox_id: first.sandbox.id,
            argv: vec!["cat".into(), "work/value".into()],
            ..ExecRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(code, 0);
    assert_eq!(stdout, b"injected");
}
