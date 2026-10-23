//! Postgres multi-replica matrix for the database-authoritative Sandbox Service.
//!
//! Two real `preloop sandbox serve` processes share one Postgres database. The
//! providers are real agents that speak generated RPCs over TLS and drive the
//! fake driver, and the callers use the real client API. Every test creates its
//! own database on the server named by `PRELOOP_TEST_POSTGRES_URL` (an admin
//! URL such as `postgres://user@127.0.0.1:5432/postgres`) and drops it on exit.
//! `psql` must be on `PATH`. Without the variable each test prints a skip
//! notice and returns, the same convention as the backend suite.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::future::Future;
use std::io::{Read as _, Write as _};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt as _;
use preloop_sandbox::agent::{Agent, AgentConfig, ProviderLink, RemoteLink, Session};
use preloop_sandbox::api::*;
use preloop_sandbox::driver::TransferChunk;
use preloop_sandbox::drivers::fake::FakeDriver;
use preloop_sandbox::{Code, Driver, SandboxApi, SandboxClient, tls};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const DIGEST: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const SEAL_KEY_HEX: &str = "4242424242424242424242424242424242424242424242424242424242424242";

/// Skip (with a visible notice) when no Postgres server is configured. When
/// `PRELOOP_TEST_REQUIRE_POSTGRES` is set the missing server is a failure: a
/// CI leg that asks for the matrix must not silently pass without it.
macro_rules! require_postgres {
    () => {
        if std::env::var("PRELOOP_TEST_POSTGRES_URL").map_or(true, |url| url.trim().is_empty()) {
            assert!(
                std::env::var("PRELOOP_TEST_REQUIRE_POSTGRES").is_err(),
                "PRELOOP_TEST_POSTGRES_URL must name a Postgres server \
                 (PRELOOP_TEST_REQUIRE_POSTGRES is set)"
            );
            eprintln!("skipped: set PRELOOP_TEST_POSTGRES_URL to run the Postgres matrix");
            return;
        }
    };
}

// ── Database and SQL inspection ───────────────────────────────────

/// A fresh database on the configured server, dropped when it goes out of scope.
struct Database {
    admin: String,
    name: String,
    url: String,
}

impl Database {
    fn create() -> Self {
        let admin = std::env::var("PRELOOP_TEST_POSTGRES_URL").expect("checked by the caller");
        let (base, _) = admin
            .rsplit_once('/')
            .expect("PRELOOP_TEST_POSTGRES_URL names a database");
        let name = format!("sbx_matrix_{}", uuid::Uuid::new_v4().simple());
        psql(&admin, &format!("CREATE DATABASE \"{name}\""));
        Self {
            url: format!("{base}/{name}"),
            admin,
            name,
        }
    }

    /// One scalar from the sandbox schema.
    fn scalar(&self, sql: &str) -> String {
        psql(&self.url, sql)
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let _ = Command::new("psql")
            .args(["-X", "-q", "-d", &self.admin, "-c"])
            .arg(format!(
                "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
                self.name
            ))
            .output();
    }
}

fn psql(url: &str, sql: &str) -> String {
    let output = Command::new("psql")
        .args([
            "-X",
            "-A",
            "-t",
            "-v",
            "ON_ERROR_STOP=1",
            "-d",
            url,
            "-c",
            sql,
        ])
        .output()
        .expect("psql on PATH (the Postgres matrix inspects rows with it)");
    assert!(
        output.status.success(),
        "psql failed on `{sql}`: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

// ── Replica processes ─────────────────────────────────────────────

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("a free loopback port")
        .local_addr()
        .expect("local address")
        .port()
}

struct Replica {
    name: &'static str,
    home: PathBuf,
    client_port: u16,
    agent_port: u16,
    child: Option<Child>,
}

impl Replica {
    fn client_base(&self) -> String {
        format!("https://127.0.0.1:{}", self.client_port)
    }

    fn agent_base(&self) -> String {
        format!("https://127.0.0.1:{}", self.agent_port)
    }
}

/// Settings every replica of one cluster shares.
struct Shared {
    store: String,
    ca_dir: PathBuf,
    token: String,
    ca_pem: String,
}

/// Two replicas on one store, with their own homes and one shared CA.
struct Cluster {
    root: tempfile::TempDir,
    shared: Shared,
    replicas: Vec<Replica>,
    /// Owns the database; dropped after the processes are gone.
    _db: Option<Database>,
}

impl Cluster {
    fn start(store: String, db: Option<Database>) -> Self {
        let root = tempfile::tempdir().expect("temp root");
        let ca_dir = root.path().join("ca");
        // Created before any replica starts so the replicas only load it.
        let ca = tls::ServiceCa::load_or_create(&ca_dir.join(tls::CA_DIR)).expect("shared CA");
        let shared = Shared {
            store,
            ca_dir,
            token: uuid::Uuid::new_v4().simple().to_string(),
            ca_pem: ca.pem().to_owned(),
        };
        let mut replicas = Vec::new();
        for name in ["node-a", "node-b"] {
            replicas.push(Replica {
                name,
                home: root.path().join(format!("home-{name}")),
                client_port: free_port(),
                agent_port: free_port(),
                child: None,
            });
        }
        let mut cluster = Self {
            root,
            shared,
            replicas,
            _db: db,
        };
        for index in 0..cluster.replicas.len() {
            cluster.spawn(index);
        }
        cluster
    }

    /// The other replicas, as `node_id=url` peer configuration.
    fn peers_for(&self, index: usize) -> String {
        self.replicas
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .map(|(_, replica)| format!("{}={}", replica.name, replica.agent_base()))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Start (or restart) replica `index` as a new process with a new session.
    fn spawn(&mut self, index: usize) {
        let peers = self.peers_for(index);
        let replica = &self.replicas[index];
        std::fs::create_dir_all(&replica.home).expect("replica home");
        let log = std::fs::File::create(replica.home.join("serve.log")).expect("replica log");
        let child = Command::new(env!("CARGO_BIN_EXE_preloop"))
            .args([
                "sandbox",
                "serve",
                "--listen",
                &format!("127.0.0.1:{}", replica.client_port),
                "--no-local",
            ])
            .env("PRELOOP_HOME", &replica.home)
            .env("PRELOOP_SANDBOX_STORE_URL", &self.shared.store)
            .env("PRELOOP_SANDBOX_CA_DIR", &self.shared.ca_dir)
            .env("PRELOOP_SANDBOX_TOKEN", &self.shared.token)
            .env("PRELOOP_SANDBOX_SEAL_KEY", SEAL_KEY_HEX)
            .env("PRELOOP_SANDBOX_NODE_ID", replica.name)
            .env("PRELOOP_SANDBOX_PEER_URL", replica.agent_base())
            .env("PRELOOP_SANDBOX_PEERS", peers)
            .env(
                "PRELOOP_SANDBOX_AGENT_LISTEN",
                format!("127.0.0.1:{}", replica.agent_port),
            )
            .env("PRELOOP_SANDBOX_NODE_LOST_AFTER_SECS", "12")
            .stdin(Stdio::null())
            .stdout(log.try_clone().expect("log clone"))
            .stderr(log)
            .spawn()
            .expect("spawning `preloop sandbox serve`");
        self.replicas[index].child = Some(child);
    }

    /// SIGKILL replica `index` and reap it.
    fn kill(&mut self, index: usize) {
        if let Some(mut child) = self.replicas[index].child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Wait until replica `index` answers RPCs on its client listener.
    async fn ready(&self, index: usize) {
        let client = self.client(index);
        let started = Instant::now();
        loop {
            if client.list_providers().await.is_ok() {
                return;
            }
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "replica {} never became ready; see {}",
                self.replicas[index].name,
                self.replicas[index].home.join("serve.log").display()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn start_ready(store: String, db: Option<Database>) -> Self {
        let cluster = Self::start(store, db);
        cluster.ready(0).await;
        cluster.ready(1).await;
        cluster
    }

    fn client(&self, index: usize) -> SandboxClient {
        SandboxClient::with_tls(
            self.replicas[index].client_base(),
            Some(self.shared.token.clone()),
            &self.shared.ca_pem,
            None,
        )
        .expect("caller client")
    }

    fn agent_base(&self, index: usize) -> String {
        self.replicas[index].agent_base()
    }

    fn home(&self, index: usize) -> &Path {
        &self.replicas[index].home
    }

    fn root(&self) -> &Path {
        self.root.path()
    }

    /// Start a scripted provider: enrolled through replica A, with polls and
    /// reports on replica B.
    async fn provider(&self, name: &str, caps: Capabilities) -> Provider {
        let identity = self.root().join("identity").join(name);
        if !identity.exists() {
            let token = self
                .client(0)
                .create_join_token(600, name)
                .await
                .expect("join token")
                .token;
            RemoteLink::enroll(self.agent_base(0), &token, &identity, name)
                .await
                .expect("enrollment through replica A");
        }
        let a = self.link(0, name).await;
        let b = self.link(1, name).await;
        let host = self.root().join(format!("host-{name}"));
        let fake = Arc::new(FakeDriver::new("fake", host.clone(), caps));
        let mut config =
            AgentConfig::new(name).with_scratch(self.root().join("agent-scratch").join(name));
        config.reconcile = Duration::from_millis(300);
        config.heartbeat = Duration::from_secs(1);
        let shutdown = CancellationToken::new();
        let agent = Agent::new(
            Arc::new(SplitLink {
                a: Arc::new(a),
                b: Arc::new(b),
            }) as Arc<dyn ProviderLink>,
            vec![fake.clone() as Arc<dyn Driver>],
            config,
        );
        let task = tokio::spawn(agent.run(None, shutdown.clone()));
        let provider = Provider {
            name: name.to_owned(),
            host,
            shutdown,
            _task: task,
        };
        provider.wait_registered(self.client(0)).await;
        provider
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for index in 0..self.replicas.len() {
            self.kill(index);
        }
        if std::thread::panicking() {
            for replica in &self.replicas {
                let log =
                    std::fs::read_to_string(replica.home.join("serve.log")).unwrap_or_default();
                let tail: Vec<&str> = log.lines().rev().take(40).collect();
                eprintln!(
                    "--- {} serve.log (last lines) ---\n{}",
                    replica.name,
                    tail.into_iter().rev().collect::<Vec<_>>().join("\n")
                );
            }
        }
    }
}

/// Scripted provider: a real agent with its own enrollment, host and fake driver.
struct Provider {
    name: String,
    host: PathBuf,
    shutdown: CancellationToken,
    _task: tokio::task::JoinHandle<()>,
}

impl Provider {
    fn stop(&self) {
        self.shutdown.cancel();
    }

    async fn wait_registered(&self, client: SandboxClient) {
        let started = Instant::now();
        loop {
            let registered = client
                .list_providers()
                .await
                .map(|providers| {
                    providers
                        .iter()
                        .any(|p| p.name == self.name && !p.drivers.is_empty())
                })
                .unwrap_or(false);
            if registered {
                return;
            }
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "provider {} never registered",
                self.name
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The machine directory name of a sandbox on this host.
    fn native(&self, sandbox_id: &str) -> PathBuf {
        self.host
            .join("sandboxes")
            .join(preloop_sandbox::driver::native_name(sandbox_id))
    }
}

impl Drop for Provider {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Polls and reports through replica B, registers through replica A.
struct SplitLink {
    a: Arc<RemoteLink>,
    b: Arc<RemoteLink>,
}

impl ProviderLink for SplitLink {
    fn register<'life0, 'async_trait>(
        &'life0 self,
        request: RegisterRequest,
    ) -> Pin<
        Box<dyn Future<Output = preloop_sandbox::Result<RegisterResponse>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.a.register(request).await })
    }
    fn heartbeat<'life0, 'async_trait>(
        &'life0 self,
        request: HeartbeatRequest,
    ) -> Pin<Box<dyn Future<Output = preloop_sandbox::Result<()>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.a.heartbeat(request).await })
    }
    fn poll<'life0, 'async_trait>(
        &'life0 self,
        request: PollRequest,
    ) -> Pin<Box<dyn Future<Output = preloop_sandbox::Result<PollResponse>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.b.poll(request).await })
    }
    fn report<'life0, 'async_trait>(
        &'life0 self,
        request: ReportRequest,
    ) -> Pin<Box<dyn Future<Output = preloop_sandbox::Result<()>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.b.report(request).await })
    }
    fn report_exec<'life0, 'async_trait>(
        &'life0 self,
        request: ReportExecRequest,
    ) -> Pin<Box<dyn Future<Output = preloop_sandbox::Result<()>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.b.report_exec(request).await })
    }
    fn reconcile<'life0, 'async_trait>(
        &'life0 self,
        request: ReconcileRequest,
    ) -> Pin<
        Box<dyn Future<Output = preloop_sandbox::Result<ReconcileResponse>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.b.reconcile(request).await })
    }
    fn fetch_blob<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        id: &'life1 Session,
        ticket: &'life2 str,
    ) -> Pin<
        Box<
            dyn Future<Output = preloop_sandbox::Result<mpsc::Receiver<TransferChunk>>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.b.fetch_blob(id, ticket).await })
    }
    fn put_blob<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        id: &'life1 Session,
        ticket: &'life2 str,
        tar: mpsc::Receiver<TransferChunk>,
    ) -> Pin<Box<dyn Future<Output = preloop_sandbox::Result<()>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.b.put_blob(id, ticket, tar).await })
    }
}

// ── Fixtures ──────────────────────────────────────────────────────

fn template(name: &str) -> TemplateSpec {
    TemplateSpec {
        name: name.into(),
        os: "linux".into(),
        artifacts: vec![Artifact {
            format: Artifact::OCI_IMAGE.into(),
            arch: host_arch().into(),
            r#ref: format!("ghcr.io/preloop/alpine@{DIGEST}"),
            digest: DIGEST.into(),
            size_bytes: 0,
            ..Artifact::default()
        }],
        ..TemplateSpec::default()
    }
}

fn acquire(key: &str) -> AcquireRequest {
    AcquireRequest {
        idempotency_key: key.into(),
        template: "base".into(),
        wait_secs: Some(60),
        ..AcquireRequest::default()
    }
}

fn pool_acquire(key: &str, pool: &str) -> AcquireRequest {
    AcquireRequest {
        idempotency_key: key.into(),
        pool: pool.into(),
        wait_secs: Some(60),
        ..AcquireRequest::default()
    }
}

fn caps(slots: u32) -> Capabilities {
    let mut caps = FakeDriver::full_capabilities();
    caps.slots = slots;
    caps
}

/// Live (not gone, failed or terminating) sandboxes, as the listing sees them.
fn live(sandboxes: &[Sandbox]) -> usize {
    sandboxes
        .iter()
        .filter(|s| {
            !matches!(
                s.state.known(),
                SandboxState::Gone | SandboxState::Failed | SandboxState::Terminating
            )
        })
        .count()
}

/// Sample the live count of `pool` (or all sandboxes when empty) until stopped.
fn sample_live(client: Arc<SandboxClient>, pool: &str) -> (Arc<AtomicBool>, Arc<Mutex<usize>>) {
    let stop = Arc::new(AtomicBool::new(false));
    let max = Arc::new(Mutex::new(0usize));
    let (flag, peak, pool) = (stop.clone(), max.clone(), pool.to_owned());
    tokio::spawn(async move {
        while !flag.load(Ordering::Acquire) {
            if let Ok(listed) = client.list().await {
                let count = listed
                    .iter()
                    .filter(|s| pool.is_empty() || s.pool == pool)
                    .filter(|s| {
                        !matches!(
                            s.state.known(),
                            SandboxState::Gone | SandboxState::Failed | SandboxState::Terminating
                        )
                    })
                    .count();
                let mut peak = peak.lock().unwrap();
                *peak = (*peak).max(count);
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    });
    (stop, max)
}

async fn wait_until<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let started = Instant::now();
    loop {
        if condition().await {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ── Scenarios ─────────────────────────────────────────────────────

/// Acknowledged writes survive a SIGKILL of the replica that took them; the
/// queued destroy is still queued after the restart; and a warm member that
/// was never leased is not handed out twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn acknowledged_writes_survive_sigkill_with_their_queued_destroy() {
    require_postgres!();
    let db = Database::create();
    let url = db.url.clone();
    let mut cluster = Cluster::start_ready(url, Some(db)).await;
    let a = cluster.client(0);
    let b = cluster.client(1);
    let provider = cluster.provider("host-a", caps(0)).await;

    a.put_template(template("base")).await.unwrap();
    a.put_pool(PoolSpec {
        name: "warm".into(),
        template: "base".into(),
        warm: 2,
        ..PoolSpec::default()
    })
    .await
    .unwrap();
    wait_until("two warm members", || async {
        live_pool(&a, "warm").await == 2 && ready_pool(&a, "warm").await == 2
    })
    .await;

    let kept = a.acquire(acquire("keep")).await.unwrap();
    let doomed = a.acquire(acquire("doomed")).await.unwrap();
    // Stop delivering work, so the destroy below stays acknowledged but queued.
    provider.stop();
    tokio::time::sleep(Duration::from_millis(300)).await;
    a.release(ReleaseRequest {
        lease_id: doomed.lease.id.clone(),
        mode: ReleaseMode::Destroy.into(),
        ..ReleaseRequest::default()
    })
    .await
    .unwrap();

    // The process that took the writes dies without warning.
    cluster.kill(0);
    cluster.spawn(0);
    cluster.ready(0).await;

    let queued = cluster.db_scalar(&format!(
        "SELECT count(*) FROM sandbox.work_items WHERE dedup_key = 'destroy:{}' AND status <> 'done'",
        doomed.sandbox.id
    ));
    assert_eq!(queued, "1", "the acknowledged destroy is still queued");
    let listed = b.list().await.unwrap();
    let state_of = |id: &str| listed.iter().find(|s| s.id == id).map(|s| s.state.known());
    assert_eq!(state_of(&kept.sandbox.id), Some(SandboxState::Leased));
    assert_eq!(
        state_of(&doomed.sandbox.id),
        Some(SandboxState::Terminating)
    );
    let templates: Vec<String> = b
        .list_templates()
        .await
        .unwrap()
        .into_iter()
        .map(|t| {
            t.spec
                .as_option()
                .map(|s| s.name.clone())
                .unwrap_or_default()
        })
        .collect();
    assert_eq!(templates, vec!["base".to_owned()]);

    // The acknowledged lease replays on the other replica with the same identity.
    let replay = b.acquire(acquire("keep")).await.unwrap();
    assert_eq!(replay.lease.id, kept.lease.id);
    assert_eq!(replay.sandbox.id, kept.sandbox.id);

    // The restarted provider drains the queued destroy exactly once.
    let provider = cluster.provider("host-a", caps(0)).await;
    wait_until("the queued destroy completes", || async {
        b.list()
            .await
            .unwrap()
            .iter()
            .all(|s| s.id != doomed.sandbox.id)
    })
    .await;
    assert!(!provider.native(&doomed.sandbox.id).exists());

    // Two concurrent pool acquires over both replicas get distinct members.
    let (first, second) = tokio::join!(
        a.acquire(pool_acquire("warm-1", "warm")),
        b.acquire(pool_acquire("warm-2", "warm"))
    );
    let (first, second) = (first.unwrap(), second.unwrap());
    assert_ne!(
        first.sandbox.id, second.sandbox.id,
        "a warm member was leased twice"
    );
    assert_ne!(first.sandbox.id, kept.sandbox.id);
    assert_ne!(second.sandbox.id, kept.sandbox.id);
}

/// 200 concurrent Acquire calls across both replicas and 50 idempotency keys
/// produce exactly 50 leases on 50 distinct sandboxes with distinct native refs.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_hundred_acquires_over_two_replicas_produce_fifty_leases() {
    require_postgres!();
    let db = Database::create();
    let cluster = Cluster::start_ready(db.url.clone(), Some(db)).await;
    let _provider = cluster.provider("host-a", caps(0)).await;
    cluster
        .client(0)
        .put_template(template("base"))
        .await
        .unwrap();
    let a = Arc::new(cluster.client(0));
    let b = Arc::new(cluster.client(1));

    let mut calls = Vec::new();
    for index in 0..200usize {
        let client = if index % 2 == 0 { a.clone() } else { b.clone() };
        let key = format!("burst-{}", index % 50);
        calls.push(tokio::spawn(async move {
            let result = client.acquire(acquire(&key)).await;
            (key, result)
        }));
    }
    let mut lease_by_key: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut sandboxes = HashSet::new();
    let mut native_refs = HashSet::new();
    for call in calls {
        let (key, result) = call.await.unwrap();
        let response = result.unwrap_or_else(|error| panic!("{key}: {error}"));
        lease_by_key
            .entry(key)
            .or_default()
            .insert(response.lease.id.clone());
        sandboxes.insert(response.sandbox.id.clone());
        native_refs.insert(response.sandbox.native_ref.clone());
    }
    assert_eq!(lease_by_key.len(), 50);
    assert!(
        lease_by_key.values().all(|ids| ids.len() == 1),
        "a key produced more than one lease: {lease_by_key:?}"
    );
    let leases: HashSet<&String> = lease_by_key.values().flatten().collect();
    assert_eq!(leases.len(), 50, "exactly fifty leases");
    assert_eq!(sandboxes.len(), 50, "one sandbox per lease");
    assert_eq!(native_refs.len(), 50, "distinct native references");
    let listed = a.list().await.unwrap();
    // Name what is not live: a failure here is a state the burst should never
    // reach (a failed or terminating sandbox), so report its reason rather
    // than only a count.
    let stalled: Vec<(&str, SandboxState, &str)> = listed
        .iter()
        .filter(|sandbox| {
            matches!(
                sandbox.state.known(),
                SandboxState::Gone | SandboxState::Failed | SandboxState::Terminating
            )
        })
        .map(|sandbox| {
            (
                sandbox.id.as_str(),
                sandbox.state.known(),
                sandbox.error.as_str(),
            )
        })
        .collect();
    assert_eq!(
        live(&listed),
        50,
        "the listing agrees; not live: {stalled:?}"
    );
    let rows = cluster
        .db_scalar("SELECT count(*) FROM sandbox.leases WHERE idempotency_key LIKE 'burst-%'");
    assert_eq!(rows, "50", "one durable lease row per key");
}

/// Two requests for one live name over both replicas: one winner, one
/// AlreadyExists.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn duplicate_live_name_has_exactly_one_winner() {
    require_postgres!();
    let db = Database::create();
    let cluster = Cluster::start_ready(db.url.clone(), Some(db)).await;
    let _provider = cluster.provider("host-a", caps(0)).await;
    let a = cluster.client(0);
    let b = cluster.client(1);
    a.put_template(template("base")).await.unwrap();
    let named = |key: &str| AcquireRequest {
        name: "dup-name".into(),
        ..acquire(key)
    };
    for round in 0..3 {
        let (first, second) = tokio::join!(
            a.acquire(named(&format!("n-{round}-a"))),
            b.acquire(named(&format!("n-{round}-b")))
        );
        let outcomes = [first, second];
        let winners = outcomes.iter().filter(|r| r.is_ok()).count();
        let losers: Vec<Code> = outcomes
            .iter()
            .filter_map(|r| r.as_ref().err().map(|e| e.code))
            .collect();
        assert_eq!(winners, 1, "round {round}: exactly one winner");
        assert_eq!(losers, vec![Code::AlreadyExists], "round {round}");
        // Free the live name before the next round.
        let winner = outcomes.into_iter().find_map(Result::ok).unwrap();
        a.release(ReleaseRequest {
            lease_id: winner.lease.id.clone(),
            mode: ReleaseMode::Destroy.into(),
            ..ReleaseRequest::default()
        })
        .await
        .unwrap();
        // A destroy in flight still holds the live name: wait for the record
        // to be gone, not merely terminating, or the next round races it.
        wait_until("the name is free", || async {
            b.list().await.unwrap().iter().all(|s| s.name != "dup-name")
        })
        .await;
    }
}

/// Driver slots bound the live sandboxes of one provider, and a pool's max
/// bounds its members, under concurrent acquires over both replicas.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn driver_slots_and_pool_max_never_overshoot() {
    require_postgres!();
    let db = Database::create();
    let cluster = Cluster::start_ready(db.url.clone(), Some(db)).await;
    let _provider = cluster.provider("host-a", caps(5)).await;
    let a = Arc::new(cluster.client(0));
    let b = Arc::new(cluster.client(1));
    a.put_template(template("base")).await.unwrap();
    a.put_pool(PoolSpec {
        name: "bounded".into(),
        template: "base".into(),
        warm: 0,
        max: 3,
        ..PoolSpec::default()
    })
    .await
    .unwrap();

    // Slots: twelve acquires against five slots.
    let (stop, peak) = sample_live(a.clone(), "");
    let mut calls = Vec::new();
    for index in 0..12usize {
        let client = if index % 2 == 0 { a.clone() } else { b.clone() };
        calls.push(tokio::spawn(async move {
            client.acquire(acquire(&format!("slot-{index}"))).await
        }));
    }
    let outcomes: Vec<_> = futures::future::join_all(calls).await;
    stop.store(true, Ordering::Release);
    let mut winners = Vec::new();
    for outcome in outcomes {
        match outcome.unwrap() {
            Ok(response) => winners.push(response),
            Err(error) => assert_eq!(
                error.code,
                Code::ResourceExhausted,
                "a full provider fails only with capacity: {error}"
            ),
        }
    }
    assert_eq!(winners.len(), 5, "exactly the five slots are leased");
    assert!(
        *peak.lock().unwrap() <= 5,
        "slots overshot: {}",
        peak.lock().unwrap()
    );

    // Free the slots so the pool phase below exercises pool.max, not the
    // provider's five slots.
    for winner in &winners {
        a.release(ReleaseRequest {
            lease_id: winner.lease.id.clone(),
            mode: ReleaseMode::Destroy.into(),
            ..ReleaseRequest::default()
        })
        .await
        .unwrap();
    }
    wait_until("the slot leases drain", || async {
        live(&a.list().await.unwrap()) == 0
    })
    .await;

    // Pool max: eight pool acquires against a max of three.
    let (stop, peak) = sample_live(a.clone(), "bounded");
    let mut calls = Vec::new();
    for index in 0..8usize {
        let client = if index % 2 == 0 { a.clone() } else { b.clone() };
        calls.push(tokio::spawn(async move {
            client
                .acquire(pool_acquire(&format!("bounded-{index}"), "bounded"))
                .await
        }));
    }
    let outcomes: Vec<_> = futures::future::join_all(calls).await;
    stop.store(true, Ordering::Release);
    let ok = outcomes
        .iter()
        .filter(|r| r.as_ref().unwrap().is_ok())
        .count();
    assert_eq!(ok, 3, "exactly the pool max is leased");
    assert!(
        *peak.lock().unwrap() <= 3,
        "pool max overshot: {}",
        peak.lock().unwrap()
    );
}

/// Two replicas reap one expired lease concurrently; the destroy is queued once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_reapers_queue_one_destroy() {
    require_postgres!();
    let db = Database::create();
    let cluster = Cluster::start_ready(db.url.clone(), Some(db)).await;
    let _provider = cluster.provider("host-a", caps(0)).await;
    let a = cluster.client(0);
    let b = cluster.client(1);
    a.put_template(template("base")).await.unwrap();
    let lease = a
        .acquire(AcquireRequest {
            ttl_secs: 1,
            ..acquire("short")
        })
        .await
        .unwrap();
    let sandbox = lease.sandbox.id.clone();
    wait_until("the lease expires and is reaped", || async {
        b.list().await.unwrap().iter().all(|s| s.id != sandbox)
    })
    .await;
    let queued = cluster.db_scalar(&format!(
        "SELECT count(*) FROM sandbox.work_items WHERE dedup_key = 'destroy:{sandbox}'"
    ));
    assert_eq!(queued, "1", "one destroy was queued for the reaped lease");
}

/// Register through A, poll and report through B: one generation throughout.
/// A stale generation mutates nothing, and the current one completes the work.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn provider_registers_on_one_replica_and_reports_through_another() {
    require_postgres!();
    let db = Database::create();
    let cluster = Cluster::start_ready(db.url.clone(), Some(db)).await;
    let provider = cluster.provider("host-a", caps(0)).await;
    let a = cluster.client(0);
    let b = cluster.client(1);
    a.put_template(template("base")).await.unwrap();
    let first = a.acquire(acquire("via-b")).await.unwrap();
    assert_eq!(first.sandbox.state.known(), SandboxState::Leased);
    let registered = a.list_providers().await.unwrap();
    let record = registered.iter().find(|p| p.name == "host-a").unwrap();
    let generation = record.generation;
    let provider_id = record.id.clone();

    // Stop the agent, queue a create, and fence the old session by
    // re-registering through A.
    provider.stop();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let pending = {
        let client = Arc::new(cluster.client(0));
        tokio::spawn(async move { client.acquire(acquire("fenced")).await })
    };
    wait_until("the create is queued", || async {
        cluster.db_scalar(&format!(
            "SELECT count(*) FROM sandbox.work_items WHERE provider_id = '{provider_id}' \
             AND kind = 'create' AND status = 'queued'"
        )) == "1"
    })
    .await;
    let fresh = cluster.link(0, "host-a").await;
    let registration = fresh
        .register(RegisterRequest {
            name: "host-a".into(),
            drivers: vec![DriverInfo {
                driver: "fake".into(),
                capabilities: FakeDriver::full_capabilities().into(),
                ..DriverInfo::default()
            }],
            protocol_version: preloop_sandbox::PROTOCOL_VERSION,
            ..RegisterRequest::default()
        })
        .await
        .unwrap();
    assert!(registration.generation > generation);
    let stale = cluster.link(1, "host-a").await;
    let stale_poll = stale
        .poll(PollRequest {
            provider_id: provider_id.clone(),
            generation,
            wait_secs: 0,
            ..PollRequest::default()
        })
        .await;
    assert_eq!(
        stale_poll.unwrap_err().code,
        Code::Aborted,
        "a stale poll is fenced"
    );
    let work = stale
        .poll(PollRequest {
            provider_id: provider_id.clone(),
            generation: registration.generation,
            wait_secs: 0,
            ..PollRequest::default()
        })
        .await
        .unwrap()
        .work;
    assert_eq!(
        work.len(),
        1,
        "the current generation receives the queued create"
    );
    let item = &work[0];
    let stale_report = stale
        .report(ReportRequest {
            provider_id: provider_id.clone(),
            generation,
            work_id: item.id.clone(),
            ok: true,
            native_ref: "psbx-stale".into(),
            attempt: item.attempt,
            ..ReportRequest::default()
        })
        .await;
    assert!(stale_report.is_err(), "a stale report is refused");
    let listed = b.list().await.unwrap();
    assert!(
        listed
            .iter()
            .any(|s| s.state.known() == SandboxState::Provisioning && s.native_ref.is_empty()),
        "the stale report changed nothing"
    );
    stale
        .report(ReportRequest {
            provider_id: provider_id.clone(),
            generation: registration.generation,
            work_id: item.id.clone(),
            ok: true,
            native_ref: "psbx-current".into(),
            attempt: item.attempt,
            ..ReportRequest::default()
        })
        .await
        .unwrap();
    let completed = pending.await.unwrap().unwrap();
    assert_eq!(completed.sandbox.native_ref, "psbx-current");
    assert_eq!(completed.sandbox.state.known(), SandboxState::Leased);
}

/// Exec: the client stream opens on A, the provider reports on B, and A owns
/// the stream. Stdout, stderr and the exit arrive exactly once. A re-registration
/// while output is still arriving neither drops nor mixes lines.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn exec_streams_through_one_replica_while_the_provider_reports_through_another() {
    require_postgres!();
    let db = Database::create();
    let cluster = Cluster::start_ready(db.url.clone(), Some(db)).await;
    let _provider = cluster.provider("host-a", caps(0)).await;
    let a = cluster.client(0);
    a.put_template(template("base")).await.unwrap();
    let lease = a.acquire(acquire("exec")).await.unwrap();
    let request = |argv: Vec<String>| ExecRequest {
        sandbox_id: lease.sandbox.id.clone(),
        lease_id: lease.lease.id.clone(),
        argv,
        ..ExecRequest::default()
    };

    let mut stream = a
        .exec(request(vec![
            "/bin/sh".into(),
            "-c".into(),
            "printf out; printf err >&2; exit 7".into(),
        ]))
        .await
        .unwrap();
    let (mut stdout, mut stderr, mut exit) = (Vec::new(), Vec::new(), None);
    while let Some(event) = stream.next().await {
        match event.unwrap().event {
            Some(preloop_sandbox::api::exec_event::Event::Stdout(bytes)) => stdout.extend(bytes),
            Some(preloop_sandbox::api::exec_event::Event::Stderr(bytes)) => stderr.extend(bytes),
            Some(preloop_sandbox::api::exec_event::Event::ExitCode(code)) => {
                exit = Some(code);
                break;
            }
            None => {}
        }
    }
    assert_eq!(exit, Some(7));
    assert_eq!(stdout, b"out");
    assert_eq!(stderr, b"err");
    let owner = cluster.db_scalar("SELECT DISTINCT owner_node FROM sandbox.execs");
    assert_eq!(owner, "node-a", "replica A owns the stream");

    // Re-register the provider while a multi-line command is still running.
    let mut stream = a
        .exec(request(vec![
            "/bin/sh".into(),
            "-c".into(),
            "i=0; while [ $i -lt 8 ]; do echo line$i; i=$((i+1)); sleep 0.25; done; exit 3".into(),
        ]))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    let fresh = cluster.link(0, "host-a").await;
    fresh
        .register(RegisterRequest {
            name: "host-a".into(),
            drivers: vec![DriverInfo {
                driver: "fake".into(),
                capabilities: FakeDriver::full_capabilities().into(),
                ..DriverInfo::default()
            }],
            protocol_version: preloop_sandbox::PROTOCOL_VERSION,
            ..RegisterRequest::default()
        })
        .await
        .unwrap();
    let mut stdout = Vec::new();
    let mut exit = None;
    while let Some(event) = stream.next().await {
        let event = match event {
            Ok(event) => event,
            Err(error) => {
                let row = cluster.db_scalar(
                    "SELECT e.status || ' / ' || e.error || ' / work ' || w.status || \
                     ' attempt ' || w.attempt || ' visible_in_ms ' || \
                     (w.visible_at - (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint) \
                     FROM sandbox.execs e JOIN sandbox.work_items w ON w.id = e.work_id \
                     ORDER BY w.created_at DESC LIMIT 1",
                );
                panic!(
                    "exec stream failed: {error} (db: {row}; stdout so far: {:?})",
                    String::from_utf8_lossy(&stdout)
                );
            }
        };
        match event.event {
            Some(preloop_sandbox::api::exec_event::Event::Stdout(bytes)) => stdout.extend(bytes),
            Some(preloop_sandbox::api::exec_event::Event::ExitCode(code)) => {
                exit = Some(code);
                break;
            }
            _ => {}
        }
    }
    let expected: String = (0..8).map(|i| format!("line{i}\n")).collect();
    assert_eq!(
        String::from_utf8_lossy(&stdout),
        expected,
        "no line lost or repeated"
    );
    assert_eq!(exit, Some(3));
}

/// A one-shot upload and download whose producer and consumer sit on different
/// replicas are byte-identical, and nothing is staged on disk.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn transfers_across_replicas_are_byte_identical_without_staging() {
    require_postgres!();
    let db = Database::create();
    let cluster = Cluster::start_ready(db.url.clone(), Some(db)).await;
    let provider = cluster.provider("host-a", caps(0)).await;
    let a = cluster.client(0);
    a.put_template(template("base")).await.unwrap();
    let lease = a.acquire(acquire("xfer")).await.unwrap();

    let payload: Vec<u8> = (0..8 * 1024 * 1024usize)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    let source = cluster.root().join("payload.bin");
    std::fs::write(&source, &payload).unwrap();
    let tar_path = cluster.root().join("upload.tar");
    {
        let file = std::fs::File::create(&tar_path).unwrap();
        let mut builder = tar::Builder::new(file);
        builder
            .append_path_with_name(&source, "payload.bin")
            .unwrap();
        builder.finish().unwrap();
    }
    // Producer on A (the caller), consumer on B (the provider's replica).
    a.upload(&lease.sandbox.id, &lease.lease.id, "/", &tar_path)
        .await
        .unwrap();
    let copied = std::fs::read(provider.native(&lease.sandbox.id).join("payload.bin")).unwrap();
    assert!(copied == payload, "upload is byte-identical");

    // Consumer on A (the caller), producer on B (the provider's replica).
    let dest = cluster.root().join("download.tar");
    a.download(&lease.sandbox.id, &lease.lease.id, "/payload.bin", &dest)
        .await
        .unwrap();
    let mut archive = tar::Archive::new(std::fs::File::open(&dest).unwrap());
    let mut downloaded = Vec::new();
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        if entry.path().unwrap().ends_with("payload.bin") {
            entry.read_to_end(&mut downloaded).unwrap();
        }
    }
    assert!(downloaded == payload, "download is byte-identical");

    let staged: Vec<PathBuf> = [cluster.home(0), cluster.home(1)]
        .iter()
        .flat_map(|home| walk_tars(home))
        .collect();
    assert!(staged.is_empty(), "transfers staged files: {staged:?}");
}

fn walk_tars(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "tar") {
                found.push(path);
            }
        }
    }
    found
}

/// Killing the replica that owns a live exec fails that stream explicitly,
/// and an unrelated lease on the survivor stays listable, renewable and releasable.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn killing_the_owner_fails_its_stream_and_keeps_unrelated_leases() {
    require_postgres!();
    let db = Database::create();
    let mut cluster = Cluster::start_ready(db.url.clone(), Some(db)).await;
    let _provider = cluster.provider("host-a", caps(0)).await;
    let a = cluster.client(0);
    let b = cluster.client(1);
    a.put_template(template("base")).await.unwrap();
    let unrelated = b.acquire(acquire("unrelated")).await.unwrap();
    let streaming = a.acquire(acquire("streaming")).await.unwrap();
    let mut stream = a
        .exec(ExecRequest {
            sandbox_id: streaming.sandbox.id.clone(),
            lease_id: streaming.lease.id.clone(),
            argv: vec!["/bin/sh".into(), "-c".into(), "sleep 60; echo late".into()],
            ..ExecRequest::default()
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;

    cluster.kill(0);
    let outcome = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match stream.next().await {
                Some(Ok(event)) => {
                    if matches!(
                        event.event,
                        Some(preloop_sandbox::api::exec_event::Event::ExitCode(_))
                    ) {
                        return Err("the stream delivered an exit after its owner died".to_owned());
                    }
                }
                Some(Err(error)) => return Ok(error),
                None => return Err("the stream ended silently instead of failing".to_owned()),
            }
        }
    })
    .await
    .expect("the stream must end once its owner is killed");
    let error = outcome.unwrap_or_else(|reason| panic!("{reason}"));
    eprintln!("the stream failed explicitly: {error}");

    let listed = b.list().await.unwrap();
    assert!(
        listed
            .iter()
            .any(|s| s.id == unrelated.sandbox.id && s.state.known() == SandboxState::Leased),
        "the unrelated lease is still listed"
    );
    b.renew(RenewRequest {
        lease_id: unrelated.lease.id.clone(),
        ttl_secs: 600,
        ..RenewRequest::default()
    })
    .await
    .expect("the unrelated lease renews on the surviving replica");
    b.release(ReleaseRequest {
        lease_id: unrelated.lease.id.clone(),
        mode: ReleaseMode::Destroy.into(),
        ..ReleaseRequest::default()
    })
    .await
    .expect("the unrelated lease releases on the surviving replica");
}

/// With the store unreachable, writes fail with a typed error and nothing is
/// visible in List; once the store is back, only the acknowledged rows exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn store_outage_fails_writes_and_lists_nothing_new() {
    require_postgres!();
    let db = Database::create();
    let proxy_port = free_port();
    let server = db
        .admin
        .rsplit_once('/')
        .expect("an admin URL names a database")
        .0;
    let (authority, port) = server.rsplit_once(':').expect("the admin URL names a port");
    let target_port: u16 = port.parse().expect("a numeric port");
    let mut proxy = Proxy::open(proxy_port, target_port);
    let through_proxy = format!("{authority}:{proxy_port}/{}", db.name);
    let cluster = Cluster::start_ready(through_proxy, Some(db)).await;
    let _provider = cluster.provider("host-a", caps(0)).await;
    let a = cluster.client(0);
    let b = cluster.client(1);
    a.put_template(template("before")).await.unwrap();

    proxy.stop();
    let write = a.put_template(template("during")).await.unwrap_err();
    assert!(
        matches!(
            write.code,
            Code::Unavailable | Code::Internal | Code::DeadlineExceeded
        ),
        "a write during the outage fails with a typed error, got {:?}: {}",
        write.code,
        write.message.clone().unwrap_or_default()
    );
    let read = b.list().await.expect_err("a read during the outage fails");
    assert!(
        matches!(
            read.code,
            Code::Unavailable | Code::Internal | Code::DeadlineExceeded
        ),
        "a read during the outage fails with a typed error, got {:?}: {}",
        read.code,
        read.message.clone().unwrap_or_default()
    );

    proxy.start();
    wait_until("the store is reachable again", || async {
        a.list_templates().await.is_ok()
    })
    .await;
    let names: Vec<String> = a
        .list_templates()
        .await
        .unwrap()
        .into_iter()
        .map(|t| {
            t.spec
                .as_option()
                .map(|s| s.name.clone())
                .unwrap_or_default()
        })
        .collect();
    assert_eq!(
        names,
        vec!["before".to_owned()],
        "nothing from the outage is visible"
    );
    b.put_template(template("after")).await.unwrap();
}

/// A TCP forwarder that can be cut and restored on the same port, standing in
/// for a database that becomes unreachable and comes back.
struct Proxy {
    port: u16,
    target: u16,
    running: Arc<AtomicBool>,
    live: Arc<Mutex<Vec<TcpStream>>>,
    accept: Option<std::thread::JoinHandle<()>>,
}

impl Proxy {
    fn open(port: u16, target: u16) -> Self {
        let mut proxy = Self {
            port,
            target,
            running: Arc::new(AtomicBool::new(false)),
            live: Arc::new(Mutex::new(Vec::new())),
            accept: None,
        };
        proxy.start();
        proxy
    }

    fn start(&mut self) {
        let listener = TcpListener::bind(("127.0.0.1", self.port)).expect("proxy port");
        listener.set_nonblocking(true).expect("nonblocking proxy");
        self.running.store(true, Ordering::Release);
        let (running, live, target) = (self.running.clone(), self.live.clone(), self.target);
        self.accept = Some(std::thread::spawn(move || {
            while running.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((client, _)) => {
                        // Darwin lets the accepted socket inherit the
                        // listener's O_NONBLOCK; the relay must block or the
                        // first read fails and the connection is torn down.
                        let _ = client.set_nonblocking(false);
                        let Ok(server) = TcpStream::connect(("127.0.0.1", target)) else {
                            continue;
                        };
                        for stream in [&client, &server] {
                            if let Ok(clone) = stream.try_clone() {
                                live.lock().unwrap().push(clone);
                            }
                        }
                        pump(client.try_clone().unwrap(), server.try_clone().unwrap());
                        pump(server, client);
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(5)),
                }
            }
        }));
    }

    fn stop(&mut self) {
        self.running.store(false, Ordering::Release);
        if let Some(accept) = self.accept.take() {
            let _ = accept.join();
        }
        for stream in self.live.lock().unwrap().drain(..) {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.stop();
    }
}

fn pump(mut from: TcpStream, mut to: TcpStream) {
    std::thread::spawn(move || {
        let mut buffer = [0u8; 16 * 1024];
        loop {
            match from.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if to.write_all(&buffer[..n]).is_err() {
                        break;
                    }
                }
            }
        }
        let _ = to.shutdown(Shutdown::Both);
    });
}

async fn live_pool(client: &SandboxClient, pool: &str) -> usize {
    client
        .list()
        .await
        .unwrap()
        .iter()
        .filter(|s| s.pool == pool)
        .filter(|s| {
            !matches!(
                s.state.known(),
                SandboxState::Gone | SandboxState::Failed | SandboxState::Terminating
            )
        })
        .count()
}

async fn ready_pool(client: &SandboxClient, pool: &str) -> usize {
    client
        .list()
        .await
        .unwrap()
        .iter()
        .filter(|s| s.pool == pool && s.state.known() == SandboxState::Ready)
        .count()
}

impl Cluster {
    /// A link for provider `name` whose calls go to replica `index`.
    async fn link(&self, index: usize, name: &str) -> RemoteLink {
        let identity = self.root().join("identity").join(name);
        let base = self.agent_base(index);
        RemoteLink::connect(identity, Some(base.as_str()), None, name)
            .await
            .expect("a link to the replica")
    }
}

impl Cluster {
    /// One scalar query against the cluster's database.
    fn db_scalar(&self, sql: &str) -> String {
        self._db
            .as_ref()
            .expect("the cluster owns its database")
            .scalar(sql)
    }
}
