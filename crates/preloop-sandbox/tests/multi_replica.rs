//! Two service replicas sharing one database: provider work is claimed through
//! one node, live exec output and transfer tickets are routed to the node that
//! owns the socket. Real TLS listeners, real generated RPC clients.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt as _;
use preloop_sandbox::agent::{Agent, AgentConfig, ProviderLink, RemoteLink, Session};
use preloop_sandbox::api::*;
use preloop_sandbox::backend::{self, BackendConfig, PeerConfig};
use preloop_sandbox::driver::{TransferChunk, native_name};
use preloop_sandbox::drivers::fake::FakeDriver;
use preloop_sandbox::server::{Auth, serve};
use preloop_sandbox::{Driver, SandboxApi, SandboxClient, ServerTls, Service, ServiceConfig};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const TOKEN: &str = "multi-replica-token";

struct Node {
    service: Arc<Service>,
    shutdown: CancellationToken,
    _dir: tempfile::TempDir,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

fn config(
    state: &std::path::Path,
    seeds: &std::path::Path,
    node_id: &str,
    session_id: &str,
    credential: &str,
    peer_url: &str,
    peers: Vec<PeerConfig>,
) -> ServiceConfig {
    let mut config = ServiceConfig::new();
    config.ca = Some(Arc::new(
        preloop_sandbox::tls::ServiceCa::load_or_create(&state.join("sandbox-ca")).unwrap(),
    ));
    let _ = seeds;
    config.node_id = node_id.into();
    config.node_session = session_id.into();
    config.node_credential = credential.into();
    config.peer_url = peer_url.into();
    config.peers = peers;
    config.node_lost_after = Duration::from_secs(60);
    config.acquire_wait = Duration::from_secs(10);
    config.provider_timeout = Duration::from_secs(10);
    config.create_timeout = Duration::from_secs(30);
    config.work_timeout = Duration::from_secs(20);
    config
}

/// Register through A, then poll/report/transfer through B.
struct SplitLink {
    a: Arc<RemoteLink>,
    b: Arc<RemoteLink>,
}

#[async_trait]
impl ProviderLink for SplitLink {
    async fn register(
        &self,
        request: RegisterRequest,
    ) -> preloop_sandbox::Result<RegisterResponse> {
        self.a.register(request).await
    }
    async fn heartbeat(&self, request: HeartbeatRequest) -> preloop_sandbox::Result<()> {
        self.b.heartbeat(request).await
    }
    async fn poll(&self, request: PollRequest) -> preloop_sandbox::Result<PollResponse> {
        self.b.poll(request).await
    }
    async fn report(&self, request: ReportRequest) -> preloop_sandbox::Result<()> {
        self.b.report(request).await
    }
    async fn report_exec(&self, request: ReportExecRequest) -> preloop_sandbox::Result<()> {
        self.b.report_exec(request).await
    }
    async fn reconcile(
        &self,
        request: ReconcileRequest,
    ) -> preloop_sandbox::Result<ReconcileResponse> {
        self.b.reconcile(request).await
    }
    async fn fetch_blob(
        &self,
        id: &Session,
        ticket: &str,
    ) -> preloop_sandbox::Result<mpsc::Receiver<TransferChunk>> {
        self.b.fetch_blob(id, ticket).await
    }
    async fn put_blob(
        &self,
        id: &Session,
        ticket: &str,
        tar: mpsc::Receiver<TransferChunk>,
    ) -> preloop_sandbox::Result<()> {
        self.b.put_blob(id, ticket, tar).await
    }
}

fn template(name: &str) -> TemplateSpec {
    TemplateSpec {
        name: name.into(),
        os: "linux".into(),
        source: Some(template_spec::Source::Recipe("test".into())),
        requires: Requirements {
            driver: "fake".into(),
            arch: host_arch().into(),
            ..Requirements::default()
        }
        .into(),
        ..TemplateSpec::default()
    }
}

async fn wait_for<F, Fut>(mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..400 {
        if condition().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("condition not reached");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn provider_reports_exec_and_transfers_through_the_other_replica() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    std::fs::create_dir_all(&state).unwrap();
    let store_url = format!("sqlite://{}", dir.path().join("shared.db").display());
    let tls = ServerTls::load_or_create(
        &state,
        vec!["127.0.0.1".into(), "localhost".into()],
        None,
        None,
    )
    .unwrap();
    // Bind both listeners before building the peer lists.
    let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_a = format!("https://{}", listener_a.local_addr().unwrap());
    let base_b = format!("https://{}", listener_b.local_addr().unwrap());
    let node_a = start_listener(
        listener_a,
        &store_url,
        &state,
        tls.clone(),
        "node-a",
        "session-a",
        "credential-a",
        vec![PeerConfig {
            node_id: "node-b".into(),
            url: base_b.clone(),
        }],
        &base_a,
    )
    .await;
    let _node_b = start_listener(
        listener_b,
        &store_url,
        &state,
        tls.clone(),
        "node-b",
        "session-b",
        "credential-b",
        vec![PeerConfig {
            node_id: "node-a".into(),
            url: base_a.clone(),
        }],
        &base_b,
    )
    .await;

    let client =
        SandboxClient::with_tls(&base_a, Some(TOKEN.to_owned()), tls.ca.pem(), None).unwrap();
    client.put_template(template("base")).await.unwrap();

    // Enroll through A, then present the same certificate to B.
    let join = node_a
        .service
        .create_join_token(Duration::from_secs(60), "")
        .await
        .unwrap();
    let identity_dir = dir.path().join("identity");
    let a = Arc::new(
        RemoteLink::enroll(&base_a, &join.token, &identity_dir, "host-a")
            .await
            .unwrap(),
    );
    let b = Arc::new(
        RemoteLink::connect(&identity_dir, Some(&base_b), None, "host-a")
            .await
            .unwrap(),
    );
    let fake = Arc::new(FakeDriver::new(
        "fake",
        dir.path().join("host"),
        FakeDriver::full_capabilities(),
    ));
    let agent = Agent::new(
        Arc::new(SplitLink { a, b }) as Arc<dyn ProviderLink>,
        vec![fake as Arc<dyn Driver>],
        AgentConfig::new("host-a"),
    );
    let agent_shutdown = CancellationToken::new();
    tokio::spawn(agent.run(None, agent_shutdown.clone()));

    wait_for(|| async {
        node_a
            .service
            .list_providers()
            .await
            .map(|providers| {
                providers
                    .iter()
                    .any(|provider| !provider.drivers.is_empty())
            })
            .unwrap_or(false)
    })
    .await;

    let acquired = tokio::time::timeout(
        Duration::from_secs(30),
        client.acquire(AcquireRequest {
            idempotency_key: "multi-1".into(),
            template: "base".into(),
            wait_secs: Some(20),
            ..AcquireRequest::default()
        }),
    )
    .await
    .unwrap()
    .unwrap();

    // Client stream on A, provider ReportExec on B.
    let mut stream = client
        .exec(ExecRequest {
            sandbox_id: acquired.sandbox.id.clone(),
            lease_id: acquired.lease.id.clone(),
            argv: vec![
                "/bin/sh".into(),
                "-c".into(),
                "printf out; printf err >&2; exit 7".into(),
            ],
            ..ExecRequest::default()
        })
        .await
        .unwrap();
    let (mut stdout, mut stderr, mut exit) = (Vec::new(), Vec::new(), None);
    while let Some(event) = stream.next().await {
        match event.unwrap().event {
            Some(exec_event::Event::Stdout(bytes)) => stdout.extend(bytes),
            Some(exec_event::Event::Stderr(bytes)) => stderr.extend(bytes),
            Some(exec_event::Event::ExitCode(code)) => {
                exit = Some(code);
                break;
            }
            None => {}
        }
    }
    assert_eq!(exit, Some(7), "exit delivered once");
    assert_eq!(stdout, b"out");
    assert_eq!(stderr, b"err");

    // Upload through A, agent consumes via B.
    let source_dir = dir.path().join("upload");
    std::fs::create_dir_all(source_dir.join("dir")).unwrap();
    std::fs::write(source_dir.join("dir/file.txt"), b"hello replica").unwrap();
    let tar_path = dir.path().join("upload.tar");
    {
        let file = std::fs::File::create(&tar_path).unwrap();
        let mut builder = tar::Builder::new(file);
        builder.append_dir_all(".", &source_dir).unwrap();
        builder.finish().unwrap();
    }
    client
        .upload(&acquired.sandbox.id, &acquired.lease.id, "/", &tar_path)
        .await
        .unwrap();
    let copied = std::fs::read(
        dir.path()
            .join("host/sandboxes")
            .join(fake_native(&acquired.sandbox.id))
            .join("dir/file.txt"),
    )
    .unwrap();
    assert_eq!(copied, b"hello replica");

    // Download through A, agent produces via B.
    let dest = dir.path().join("download.tar");
    client
        .download(&acquired.sandbox.id, &acquired.lease.id, "/", &dest)
        .await
        .unwrap();
    let mut archive = tar::Archive::new(std::fs::File::open(&dest).unwrap());
    let mut found = false;
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        if entry.path().unwrap().ends_with("dir/file.txt") {
            let mut contents = String::new();
            use std::io::Read as _;
            entry.read_to_string(&mut contents).unwrap();
            assert_eq!(contents, "hello replica");
            found = true;
        }
    }
    assert!(found, "download contains the uploaded file");

    // B keeps serving durable lease operations while A's live channels die.
    node_a.shutdown.cancel();
    node_a
        .service
        .backend()
        .node_close("node-a", "session-a")
        .await
        .unwrap();
    let renewed = client
        .renew(RenewRequest {
            lease_id: acquired.lease.id.clone(),
            ttl_secs: 300,
            ..RenewRequest::default()
        })
        .await;
    // The caller still has A's URL; use B directly for the surviving replica.
    let client_b =
        SandboxClient::with_tls(&base_b, Some(TOKEN.to_owned()), tls.ca.pem(), None).unwrap();
    let renewed = if renewed.is_err() {
        client_b
            .renew(RenewRequest {
                lease_id: acquired.lease.id.clone(),
                ttl_secs: 300,
                ..RenewRequest::default()
            })
            .await
            .unwrap()
    } else {
        renewed.unwrap()
    };
    assert_eq!(renewed.id, acquired.lease.id);
    client_b
        .release(ReleaseRequest {
            lease_id: acquired.lease.id.clone(),
            mode: ReleaseMode::Destroy.into(),
            ..ReleaseRequest::default()
        })
        .await
        .unwrap();
    agent_shutdown.cancel();
}

fn fake_native(sandbox_id: &str) -> String {
    native_name(sandbox_id)
}

#[allow(clippy::too_many_arguments)]
async fn start_listener(
    listener: tokio::net::TcpListener,
    store_url: &str,
    state: &std::path::Path,
    tls: Arc<ServerTls>,
    node_id: &str,
    session_id: &str,
    credential: &str,
    peers: Vec<PeerConfig>,
    base: &str,
) -> Node {
    let mut config = config(state, state, node_id, session_id, credential, base, peers);
    config.ca = Some(tls.ca.clone());
    let backend = backend::open_with(BackendConfig {
        url: store_url.to_owned(),
        state_dir: state.to_path_buf(),
        seal_key: Some([42u8; 32]),
        pool_size: 4,
        node_id: node_id.into(),
        session_id: session_id.into(),
        node_credential_sha256: backend::credential_hash(credential),
        peer_url: base.to_owned(),
        node_lost_after: config.node_lost_after,
    })
    .await
    .unwrap();
    let service = Service::open(backend, config).await.unwrap();
    let shutdown = CancellationToken::new();
    tokio::spawn(serve(
        listener,
        service.clone(),
        Auth::new(TOKEN),
        tls,
        shutdown.clone(),
    ));
    tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
    Node {
        service,
        shutdown,
        _dir: tempfile::tempdir().unwrap(),
    }
}
