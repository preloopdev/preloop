//! The Sandbox Service core: templates, pools, leases, placement, provider
//! work and the live channel runtime.
//!
//! Durable state lives in [`crate::backend`]; this module owns only bounded
//! live channels (exec output, transfer rendezvous), the placement policy and
//! the instants a caller is currently waiting on. Every decision that grants
//! capacity, a lease, a provider generation, a work claim or a live-channel
//! owner is asked of the database in one short transaction.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use futures::StreamExt as _;
use futures::stream::BoxStream;
use parking_lot::Mutex;
use sha2::{Digest as _, Sha256};
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::api::*;
use crate::backend::{
    self, AcquireCommand, AcquireStep, BackendConfig, ExecStatus, MaintenanceConfig, NewSandbox,
    Owner, OwnerKind, PeerConfig, PlacementSnapshot, PowerOp, RefillOutcome, SandboxBackend,
    SnapshotCommand, SnapshotStart, TicketAuthority, TicketSide, WorkResult,
};
use crate::driver::{EXEC_TIMED_OUT, TRANSFER_QUEUE, TransferChunk};
use crate::error::{Code, Error, Result};
use crate::placement::{Candidate, CandidateId, LeastLoaded, Placement, PlacementRequest};
use crate::tls;

/// Tunables and node identity.
#[derive(Clone)]
pub struct ServiceConfig {
    /// Default lease TTL.
    pub lease_ttl: Duration,
    /// Default time `Acquire` waits for a ready sandbox.
    pub acquire_wait: Duration,
    /// How long an acquired-but-never-returned lease (one whose `Acquire` is
    /// still provisioning, or timed out) may live before maintenance releases
    /// it. A waiting `Acquire` keeps its lease past this window.
    pub unclaimed_lease_ttl: Duration,
    /// How long an `Acquire` may wait *for capacity* — the pool is at
    /// `max`, or placement reports `resource_exhausted` — before the wait
    /// fails with that error. Provisioning waits stay bounded by
    /// `acquire_wait`: a cold create legitimately takes minutes, but a full
    /// pool does not free up faster for waiting longer.
    pub acquire_capacity_wait: Duration,
    /// A provider silent this long is not placed on.
    pub provider_timeout: Duration,
    /// A create not reported within this window fails.
    pub create_timeout: Duration,
    /// Default preservation window.
    pub preserve_ttl: Duration,
    /// Maximum bytes accepted for one transfer (data plane).
    pub blob_max_bytes: u64,
    /// How long a data-plane ticket stays valid before both sides claim.
    pub ticket_ttl: Duration,
    /// How long a caller waits for a provider report.
    pub work_timeout: Duration,
    /// A provider silent this many times `provider_timeout` is lost.
    pub provider_lost_factor: u32,
    /// Strategy that ranks placement candidates.
    pub placement: Arc<dyn Placement>,
    /// `preloop.sandbox.*` instruments.
    pub metrics: crate::metrics::SandboxMetrics,
    /// Most work items one `Poll` returns.
    pub poll_batch: usize,
    /// How long delivered work stays in flight before redelivery.
    pub delivery_timeout: Duration,
    /// Default exec deadline.
    pub exec_timeout: Duration,
    /// CA that issues provider client certificates. Shared by every replica.
    pub ca: Option<Arc<crate::tls::ServiceCa>>,
    /// This replica's node id. Must be unique per serving process.
    pub node_id: String,
    /// This replica's node session; generated when empty.
    pub node_session: String,
    /// This replica's inter-node credential. Generated for single-node use.
    pub node_credential: String,
    /// This replica's advertised base URL for peer routing (optional).
    pub peer_url: String,
    /// Peer replicas, from configuration only.
    pub peers: Vec<PeerConfig>,
    /// A node unheard from this long is reaped and its live channels fail.
    pub node_lost_after: Duration,
    /// An acquire waiter's claim lifetime (renewed in the wait loop).
    pub waiter_timeout: Duration,
    /// How long an injection claim is held before another retry may claim it.
    pub injection_timeout: Duration,
}

impl std::fmt::Debug for ServiceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceConfig")
            .field("lease_ttl", &self.lease_ttl)
            .field("node_id", &self.node_id)
            .field("peers", &self.peers.len())
            .finish_non_exhaustive()
    }
}

impl ServiceConfig {
    /// Defaults.
    pub fn new() -> Self {
        Self {
            lease_ttl: Duration::from_secs(3600),
            acquire_wait: Duration::from_secs(900),
            unclaimed_lease_ttl: Duration::from_secs(600),
            acquire_capacity_wait: Duration::from_secs(30),
            provider_timeout: Duration::from_secs(45),
            create_timeout: Duration::from_secs(1800),
            preserve_ttl: Duration::from_secs(600),
            blob_max_bytes: DEFAULT_BLOB_MAX_BYTES,
            ticket_ttl: Duration::from_secs(300),
            work_timeout: Duration::from_secs(900),
            provider_lost_factor: 3,
            placement: Arc::new(LeastLoaded),
            metrics: crate::metrics::SandboxMetrics::default(),
            poll_batch: 64,
            delivery_timeout: Duration::from_secs(120),
            exec_timeout: Duration::from_secs(3600),
            ca: None,
            node_id: std::env::var("PRELOOP_SANDBOX_NODE_ID").unwrap_or_else(|_| {
                hostname::get()
                    .ok()
                    .and_then(|name| name.into_string().ok())
                    .unwrap_or_else(|| "sandbox-node".to_owned())
            }),
            node_session: String::new(),
            node_credential: std::env::var("PRELOOP_SANDBOX_NODE_CREDENTIAL").unwrap_or_default(),
            peer_url: std::env::var("PRELOOP_SANDBOX_PEER_URL").unwrap_or_default(),
            peers: Vec::new(),
            node_lost_after: Duration::from_secs(30),
            waiter_timeout: Duration::from_secs(30),
            injection_timeout: Duration::from_secs(60),
        }
    }

    /// Parse `PRELOOP_SANDBOX_PEERS` (`node_id=https://host:port,...`).
    pub fn peers_from_env() -> Result<Vec<PeerConfig>> {
        let Ok(value) = std::env::var("PRELOOP_SANDBOX_PEERS") else {
            return Ok(Vec::new());
        };
        let mut peers = Vec::new();
        for entry in value.split(',').filter(|entry| !entry.trim().is_empty()) {
            let (node_id, url) = entry.trim().split_once('=').ok_or_else(|| {
                Error::invalid_argument(
                    "PRELOOP_SANDBOX_PEERS entries must be node_id=https://host:port",
                )
            })?;
            peers.push(PeerConfig {
                node_id: node_id.trim().to_owned(),
                url: url.trim().to_owned(),
            });
        }
        Ok(peers)
    }
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Pause between warm-pool refills after a creation failure.
const POOL_FAILURE_BACKOFF: Duration = Duration::from_secs(10);

/// Default `ListRequest.page_size`.
pub const DEFAULT_PAGE_SIZE: u32 = 100;

/// Largest accepted `ListRequest.page_size`.
pub const MAX_PAGE_SIZE: u32 = 1000;

/// Default lifetime of a join token when `Enroll` is asked for none.
pub const DEFAULT_JOIN_TOKEN_TTL: Duration = Duration::from_secs(60 * 60);

/// Longest accepted join-token lifetime. A token is a bearer credential for
/// one enrollment; a day is long enough for an operator to hand it to a host
/// and short enough that a leaked token stops working.
pub const MAX_JOIN_TOKEN_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// How many simultaneously valid certificates one provider may hold: the
/// current certificate plus two predecessors. Renewal keeps a short overlap
/// so a client that fails to store its new key keeps working, but the set is
/// bounded — after this many rotations the oldest certificate is pruned even
/// if unexpired, so rotating can actually evict a compromised certificate.
pub const MAX_PROVIDER_CERTIFICATES: usize = 3;

/// Default cap for one blob upload: 8 GiB.
pub const DEFAULT_BLOB_MAX_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// The provider a client certificate authenticates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderIdentity {
    /// Provider id.
    pub provider_id: String,
    /// Provider name; `Register` must use exactly this name.
    pub name: String,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

fn new_id(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

/// SHA-256 of the canonical JSON of a template spec.
pub fn template_digest(spec: &TemplateSpec) -> String {
    let canonical = serde_json::to_vec(spec).unwrap_or_default();
    format!("sha256:{}", hex::encode(Sha256::digest(&canonical)))
}

/// Per-sandbox placement inputs.
#[derive(Clone)]
struct PlacementSpec {
    template: Template,
    shape: Shape,
    network: NetworkPolicy,
    mounts: Vec<Mount>,
    sockets: Vec<SocketMount>,
    options: StringMap,
    requires: Requirements,
    snapshot_capable: bool,
}

impl PlacementSpec {
    fn request(&self) -> PlacementRequest {
        let template = &self.template;
        let requires = &self.requires;
        let os = if requires.os.is_empty() {
            template
                .spec
                .as_option()
                .map(|spec| spec.os.clone())
                .unwrap_or_default()
        } else {
            requires.os.clone()
        };
        let driver = if requires.driver.is_empty() {
            template.driver.clone()
        } else {
            requires.driver.clone()
        };
        PlacementRequest {
            template: template
                .spec
                .as_option()
                .map(|spec| spec.name.clone())
                .unwrap_or_default(),
            driver,
            os,
            arch: requires.arch.clone(),
            features: requires.features.clone(),
            capabilities: requires.capabilities.clone(),
            snapshot_capable: self.snapshot_capable,
            network: self.network.mode.as_known().unwrap_or_default(),
            host_mounts: !self.mounts.is_empty(),
            socket_mounts: !self.sockets.is_empty(),
        }
    }
}

/// One running exec's local output channel.
struct Exec {
    tx: mpsc::Sender<Result<ExecEvent>>,
    finished: CancellationToken,
}

/// Data-plane ticket: a single-use rendezvous for one tar stream.
struct Blob {
    sandbox_id: String,
    kind: StreamKind,
    guest_path: String,
    expires_at: i64,
    producer_claimed: bool,
    consumer_claimed: bool,
    status: watch::Sender<TransferStatus>,
    tx: Option<mpsc::Sender<TransferChunk>>,
    rx: Option<mpsc::Receiver<TransferChunk>>,
    control: Option<mpsc::Sender<TransferChunk>>,
}

/// Which end of a transfer a caller is claiming.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferSide {
    /// Supplies the bytes.
    Producer,
    /// Consumes the bytes.
    Consumer,
}

/// State two peers rendezvous on before bytes flow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferStatus {
    /// Only one side has claimed the ticket.
    Open,
    /// Both sides are present; bytes may flow.
    Ready,
    /// The transfer ended before it completed.
    Aborted(String),
}

/// One side's claim on a data-plane ticket.
#[derive(Debug)]
pub struct StreamClaim {
    /// Sandbox the copy targets.
    pub sandbox_id: String,
    /// Direction of the ticket.
    pub kind: StreamKind,
    /// Guest directory for uploads, guest path for downloads.
    pub guest_path: String,
    /// Ticket id, for cleanup helpers.
    pub ticket: String,
    status: watch::Receiver<TransferStatus>,
    producer: Option<mpsc::Sender<TransferChunk>>,
    consumer: Option<mpsc::Receiver<TransferChunk>>,
}

impl StreamClaim {
    /// Take the producer's sending end.
    pub fn take_producer(&mut self) -> Option<mpsc::Sender<TransferChunk>> {
        self.producer.take()
    }

    /// Take the consumer's receiving end.
    pub fn take_consumer(&mut self) -> Option<mpsc::Receiver<TransferChunk>> {
        self.consumer.take()
    }
}

/// The caller's exec response stream plus its disconnect guard.
struct ExecStream {
    inner: BoxStream<'static, Result<ExecEvent>>,
    _disconnect: oneshot::Sender<()>,
}

impl futures::Stream for ExecStream {
    type Item = Result<ExecEvent>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        futures::Stream::poll_next(self.inner.as_mut(), cx)
    }
}

/// Local live-channel state. Never durable inventory.
#[derive(Default)]
struct Runtime {
    execs: Mutex<HashMap<String, Exec>>,
    tickets: Mutex<HashMap<String, Blob>>,
}

/// Peer replicas, from configuration only.
struct PeerDirectory {
    clients: BTreeMap<String, reqwest::Client>,
    node_id: String,
    session_id: String,
    credential: String,
}

impl PeerDirectory {
    fn build(config: &ServiceConfig) -> Result<Self> {
        let mut clients = BTreeMap::new();
        if !config.peers.is_empty() && config.ca.is_none() {
            return Err(Error::failed_precondition(
                "multi-replica routing requires the shared service CA",
            ));
        }
        for peer in &config.peers {
            if peer.node_id == config.node_id {
                continue;
            }
            let url = peer.url.trim();
            let parsed = reqwest::Url::parse(url).map_err(|error| {
                Error::invalid_argument(format!("invalid peer URL for {}: {error}", peer.node_id))
            })?;
            if parsed.scheme() != "https" || parsed.username() != "" || parsed.password().is_some()
            {
                return Err(Error::invalid_argument(format!(
                    "peer URL for {} must be an https:// base URL without credentials",
                    peer.node_id
                )));
            }
            let ca = config
                .ca
                .as_ref()
                .ok_or_else(|| Error::failed_precondition("peer routing needs a CA"))?;
            let client = tls::http_client(ca.pem(), None)?;
            clients.insert(peer.node_id.clone(), client);
        }
        Ok(Self {
            clients,
            node_id: config.node_id.clone(),
            session_id: config.node_session.clone(),
            credential: config.node_credential.clone(),
        })
    }

    fn is_local(&self, owner: &Owner) -> bool {
        owner.node_id == self.node_id && owner.session_id == self.session_id
    }

    fn client(&self, node_id: &str) -> Result<&reqwest::Client> {
        self.clients
            .get(node_id)
            .ok_or_else(|| Error::unavailable(format!("peer node `{node_id}` is not configured")))
    }

    fn auth_headers(&self) -> [(&'static str, String); 2] {
        [
            ("x-preloop-node-id", self.node_id.clone()),
            ("x-preloop-node-session", self.session_id.clone()),
        ]
    }
}

/// The service.
pub struct Service {
    backend: Arc<dyn SandboxBackend>,
    config: ServiceConfig,
    changed: Notify,
    runtime: Runtime,
    node: Owner,
    peers: PeerDirectory,
    pool_backoff: Mutex<HashMap<String, Instant>>,
    /// Idempotency keys whose local `Acquire` future was dropped. The
    /// authoritative waiter row is cleared by the next maintenance pass; the
    /// hint makes an aborted caller release its lease without waiting for the
    /// claim's expiry.
    abandoned_waiters: Mutex<HashSet<String>>,
}

/// Clears an `Acquire`'s durable waiter claim when the future is dropped.
///
/// `Drop` cannot await, so an abandoned key becomes a local hint that
/// maintenance drains into the database. The DB row still exists and expires
/// on its own, so another replica is never blocked by a lost hint.
struct AcquireWaiterGuard {
    service: Arc<Service>,
    key: String,
    armed: bool,
}

impl AcquireWaiterGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for AcquireWaiterGuard {
    fn drop(&mut self) {
        if self.armed {
            self.service
                .abandoned_waiters
                .lock()
                .insert(self.key.clone());
        }
    }
}

impl Service {
    /// Construct the service over a durable backend. No state is loaded into
    /// memory: every read and mutation is authoritative SQL.
    pub async fn open(
        backend: Arc<dyn SandboxBackend>,
        mut config: ServiceConfig,
    ) -> Result<Arc<Self>> {
        if config.node_session.is_empty() {
            config.node_session = uuid::Uuid::new_v4().simple().to_string();
        }
        if config.node_credential.is_empty() {
            config.node_credential = tls::random_hex(32);
        }
        if let Some(ca) = &config.ca {
            backend.set_trust_fingerprint(ca.sha256_hex()).await?;
        }
        let node = Owner {
            node_id: config.node_id.clone(),
            session_id: config.node_session.clone(),
        };
        let peer_dir = PeerDirectory::build(&config)?;
        let backend_config = BackendConfig {
            url: String::new(),
            state_dir: std::env::temp_dir().join("preloop-sandbox"),
            seal_key: None,
            pool_size: 8,
            node_id: config.node_id.clone(),
            session_id: config.node_session.clone(),
            node_credential_sha256: backend::logic::token_hash(&config.node_credential),
            peer_url: config.peer_url.clone(),
            node_lost_after: config.node_lost_after,
        };
        backend.node_register(&backend_config).await?;
        let service = Arc::new(Self {
            backend,
            config,
            changed: Notify::new(),
            runtime: Runtime::default(),
            node,
            peers: peer_dir,
            pool_backoff: Mutex::new(HashMap::new()),
            abandoned_waiters: Mutex::new(HashSet::new()),
        });
        // One authoritative pass at open repairs crash-left rows (a sandbox
        // whose lease row vanished) and resumes warm pools. It is a normal
        // database transaction, not a boot-time state reconstruction.
        service.maintain().await;
        Ok(service)
    }

    /// Service configuration.
    pub fn config(&self) -> &ServiceConfig {
        &self.config
    }

    /// The durable backend.
    pub fn backend(&self) -> &Arc<dyn SandboxBackend> {
        &self.backend
    }

    /// This node's identity.
    pub fn node(&self) -> &Owner {
        &self.node
    }

    /// Whether this node owns a live channel.
    pub fn is_local_owner(&self, owner: &Owner) -> bool {
        self.peers.is_local(owner)
    }

    /// The HTTP client for a peer, from configuration.
    pub fn peer_client(&self, node_id: &str) -> Result<&reqwest::Client> {
        self.peers.client(node_id)
    }

    /// Headers proving this node's registered session and credential.
    pub fn node_auth_headers(&self) -> Vec<(&'static str, String)> {
        let mut headers = self.peers.auth_headers().to_vec();
        headers.push(("x-preloop-node-credential", self.peers.credential.clone()));
        headers
    }

    /// Authenticate an internal request from another replica.
    pub async fn authenticate_node(
        &self,
        node_id: &str,
        session_id: &str,
        credential: &str,
    ) -> Result<()> {
        let hash = backend::logic::token_hash(credential);
        if self
            .backend
            .node_authenticate(node_id, session_id, &hash)
            .await?
        {
            Ok(())
        } else {
            Err(Error::unauthenticated(
                "unknown or stale service node credential",
            ))
        }
    }

    /// The node owning a live exec.
    pub async fn exec_owner(&self, exec_id: &str) -> Result<Option<Owner>> {
        self.backend.owner_of(OwnerKind::Exec, exec_id).await
    }

    /// The node owning a live ticket.
    pub async fn ticket_owner(&self, ticket: &str) -> Result<Option<Owner>> {
        self.backend.owner_of(OwnerKind::Ticket, ticket).await
    }

    /// Run maintenance (reapers, warm pools, node heartbeat) until shutdown.
    pub async fn run_maintenance(self: Arc<Self>, shutdown: CancellationToken) {
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        let mut heartbeat = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    if let Err(error) = self.backend.node_close(&self.node.node_id, &self.node.session_id).await {
                        warn!(%error, "closing the service node session failed");
                    }
                    self.runtime.execs.lock().clear();
                    self.runtime.tickets.lock().clear();
                    return;
                }
                _ = heartbeat.tick() => {
                    if let Err(error) = self.backend.node_heartbeat(&self.node.node_id, &self.node.session_id).await {
                        warn!(%error, "sandbox node heartbeat failed");
                    }
                }
                _ = tick.tick() => self.maintain().await,
            }
        }
    }

    /// One maintenance pass (exposed for tests).
    pub async fn maintain(&self) {
        // Turn locally abandoned waiters into released DB claims before the
        // reaper decides whether their leases are still waited on. The
        // durable row remains the authority and expires on its own.
        let abandoned: Vec<String> = self.abandoned_waiters.lock().drain().collect();
        for key in abandoned {
            let _ = self.backend.clear_waiter(&key, &self.node).await;
        }
        let config = MaintenanceConfig {
            create_timeout: self.config.create_timeout,
            provider_timeout: self.config.provider_timeout,
            provider_lost_factor: self.config.provider_lost_factor,
            exec_sweep_grace: Duration::from_secs(5),
            max_destroy_attempts: 3,
            node_lost_after: self.config.node_lost_after,
            injection_timeout: self.config.injection_timeout,
            waiter_timeout: self.config.waiter_timeout,
        };
        let outcome = match self.backend.maintain(&config).await {
            Ok(outcome) => outcome,
            Err(error) => {
                warn!(%error, "sandbox maintenance pass failed");
                return;
            }
        };
        for (exec_id, reason) in &outcome.failed_execs {
            self.close_exec(exec_id, reason).await;
        }
        for ticket in &outcome.aborted_tickets {
            self.abort_local_ticket(ticket, "transfer ticket expired")
                .await;
        }
        // Exec streams live only on this node, but a destroy or snapshot on
        // any replica finalizes their durable rows. Close the local streams
        // from that authoritative status instead of waiting for their
        // deadline.
        let local_execs: Vec<String> = self.runtime.execs.lock().keys().cloned().collect();
        for exec_id in local_execs {
            match self.backend.exec_status(&exec_id).await {
                Ok(Some(ExecStatus::Failed { reason })) => {
                    self.close_exec(&exec_id, &reason).await;
                }
                Ok(None) => {
                    self.close_exec(&exec_id, "the sandbox was destroyed")
                        .await;
                }
                _ => {}
            }
        }
        for deficit in &outcome.pool_deficits {
            self.fill_pool(deficit).await;
        }
        if outcome.changed {
            self.changed.notify_waiters();
        }
    }

    async fn close_exec(&self, exec_id: &str, reason: &str) {
        let exec = self.runtime.execs.lock().remove(exec_id);
        let Some(exec) = exec else {
            return;
        };
        exec.finished.cancel();
        let event = if reason == "timeout" {
            Ok(ExecEvent::exit(EXEC_TIMED_OUT))
        } else {
            Err(Error::unavailable(reason.to_owned()))
        };
        let _ = tokio::time::timeout(Duration::from_secs(1), exec.tx.send(event)).await;
    }

    // ── Templates and pools ───────────────────────────────────────

    /// `PutTemplate`.
    pub async fn put_template(&self, spec: TemplateSpec) -> Result<Template> {
        self.backend.put_template(spec).await
    }

    /// `DeleteTemplate`.
    pub async fn delete_template(&self, name: &str) -> Result<()> {
        self.backend.delete_template(name).await
    }

    /// `ListTemplates`.
    pub async fn list_templates(&self) -> Result<Vec<Template>> {
        self.backend.list_templates_all().await
    }

    /// `ListTemplates` page.
    pub async fn list_templates_page(
        &self,
        request: &ListRequest,
    ) -> Result<ListTemplatesResponse> {
        self.backend.list_templates(request).await
    }

    /// `PutPool`.
    pub async fn put_pool(&self, pool: PoolSpec) -> Result<PoolSpec> {
        self.pool_placement_check(&pool).await?;
        let pool = self.backend.put_pool(pool).await?;
        self.changed.notify_waiters();
        Ok(pool)
    }

    /// `ListPools`.
    pub async fn list_pools(&self) -> Result<Vec<PoolSpec>> {
        self.backend.list_pools_all().await
    }

    /// `ListPools` page.
    pub async fn list_pools_page(&self, request: &ListRequest) -> Result<ListPoolsResponse> {
        self.backend.list_pools(request).await
    }

    /// `DeletePool`.
    pub async fn delete_pool(&self, name: &str) -> Result<()> {
        self.backend.delete_pool(name).await
    }

    // ── Placement ─────────────────────────────────────────────────

    async fn pool_placement(
        &self,
        pool: &PoolSpec,
        extra: &Requirements,
        snapshot: &PlacementSnapshot,
    ) -> Result<(PlacementSpec, String, String)> {
        let template = self
            .backend
            .template_get(&pool.template)
            .await?
            .ok_or_else(|| Error::not_found(format!("template `{}` not found", pool.template)))?
            .value;
        let placement = PlacementSpec {
            requires: template
                .spec
                .as_option()
                .map(|spec| spec.requires.as_option().cloned().unwrap_or_default())
                .unwrap_or_default()
                .merged(&pool.requires.as_option().cloned().unwrap_or_default())
                .merged(extra),
            template,
            shape: pool.shape.as_option().cloned().unwrap_or_default(),
            network: pool.network.as_option().cloned().unwrap_or_default(),
            mounts: pool.mounts.clone(),
            sockets: pool.sockets.clone(),
            options: pool.options.clone(),
            snapshot_capable: false,
        };
        let (provider, driver) = self.place(snapshot, &placement, true)?;
        Ok((placement, provider, driver))
    }

    async fn pool_placement_check(&self, pool: &PoolSpec) -> Result<()> {
        let snapshot = self.backend.placement_snapshot().await?;
        match self
            .pool_placement(pool, &Requirements::default(), &snapshot)
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if error.code == Code::FailedPrecondition => {
                if snapshot.providers.is_empty() {
                    return Ok(());
                }
                let template = self
                    .backend
                    .template_get(&pool.template)
                    .await?
                    .ok_or_else(|| {
                        Error::not_found(format!("template `{}` not found", pool.template))
                    })?
                    .value;
                let placement = PlacementSpec {
                    requires: template
                        .spec
                        .as_option()
                        .map(|spec| spec.requires.as_option().cloned().unwrap_or_default())
                        .unwrap_or_default()
                        .merged(&pool.requires.as_option().cloned().unwrap_or_default()),
                    template,
                    shape: pool.shape.as_option().cloned().unwrap_or_default(),
                    network: pool.network.as_option().cloned().unwrap_or_default(),
                    mounts: pool.mounts.clone(),
                    sockets: pool.sockets.clone(),
                    options: pool.options.clone(),
                    snapshot_capable: false,
                };
                self.place_with(&snapshot, &placement, false, false)
                    .map(|_| ())
            }
            Err(_) => Ok(()),
        }
    }

    fn place(
        &self,
        snapshot: &PlacementSnapshot,
        spec: &PlacementSpec,
        require_online: bool,
    ) -> Result<(String, String)> {
        self.place_with(snapshot, spec, require_online, true)
    }

    fn place_with(
        &self,
        snapshot: &PlacementSnapshot,
        spec: &PlacementSpec,
        require_online: bool,
        respect_cordon: bool,
    ) -> Result<(String, String)> {
        let template = &spec.template;
        let template_spec = template.spec.as_option().cloned().unwrap_or_default();
        let requires = &spec.requires;
        let os = if requires.os.is_empty() {
            template_spec.os.clone()
        } else {
            requires.os.clone()
        };
        let mut unmet = Vec::new();
        let mut full = false;
        let mut candidates: Vec<Candidate> = Vec::new();
        let mut cordoned: Vec<&str> = Vec::new();
        let now = now();
        for provider in &snapshot.providers {
            if require_online
                && now.saturating_sub(provider.last_seen)
                    > self.config.provider_timeout.as_secs() as i64
            {
                continue;
            }
            if !template.native_ref.is_empty() && template.provider_id != provider.id {
                continue;
            }
            let is_cordoned = respect_cordon && provider.cordoned;
            let mut capable = false;
            for driver in &provider.drivers {
                let caps = &driver.capabilities;
                let mut reasons = Vec::new();
                if !template.driver.is_empty() && template.driver != driver.driver {
                    reasons.push(format!("driver={}", template.driver));
                }
                if !requires.driver.is_empty() && requires.driver != driver.driver {
                    reasons.push(format!("driver={}", requires.driver));
                }
                if !os.is_empty() && caps.os != os {
                    reasons.push(format!("os={os}"));
                }
                if !requires.arch.is_empty() && !caps.arch.contains(&requires.arch) {
                    reasons.push(format!("arch={}", requires.arch));
                }
                for feature in &requires.features {
                    if !caps.features.contains(feature) {
                        reasons.push(format!("feature={feature}"));
                    }
                }
                for capability in &requires.capabilities {
                    if !caps.has(capability) {
                        reasons.push(format!("capability={capability}"));
                    }
                }
                if spec.snapshot_capable && !caps.snapshot {
                    reasons.push("capability=snapshot".into());
                }
                if !spec.sockets.is_empty() && !caps.socket_mounts {
                    reasons.push("capability=socket_mounts".into());
                }
                if spec.mounts.iter().any(|_| !caps.host_mounts) {
                    reasons.push("capability=host_mounts".into());
                }
                let mode = spec.network.mode;
                if mode != NetworkMode::NetworkModeUnspecified
                    && !caps.network_modes.contains(&mode)
                {
                    reasons.push(format!("network={mode:?}"));
                }
                if template_spec.recipe().is_some()
                    && template.native_ref.is_empty()
                    && requires.driver.is_empty()
                    && template_spec.requires.driver.is_empty()
                {
                    reasons.push("recipe templates must set requires.driver".into());
                }
                if !reasons.is_empty() {
                    if !is_cordoned {
                        unmet.push(format!(
                            "{}/{}: {}",
                            provider.name,
                            driver.driver,
                            reasons.join(", ")
                        ));
                    }
                    continue;
                }
                capable = true;
                if is_cordoned {
                    continue;
                }
                if !backend::logic::fits_shape(caps, &spec.shape) {
                    unmet.push(format!(
                        "{}/{}: shape={}cpus/{}mib (max {}cpus/{}mib)",
                        provider.name,
                        driver.driver,
                        spec.shape.cpus,
                        spec.shape.memory_mib,
                        caps.max_vcpus,
                        caps.max_memory_mib
                    ));
                    continue;
                }
                let live = snapshot
                    .live
                    .get(&(provider.id.clone(), driver.driver.clone()))
                    .copied()
                    .unwrap_or(0) as usize;
                if caps.slots > 0 && live >= caps.slots as usize {
                    full = true;
                    continue;
                }
                candidates.push(Candidate {
                    id: CandidateId(candidates.len()),
                    provider_id: provider.id.clone(),
                    provider_name: provider.name.clone(),
                    driver: driver.driver.clone(),
                    live,
                    slots: caps.slots,
                });
            }
            if is_cordoned && capable {
                cordoned.push(provider.name.as_str());
            }
        }
        if candidates.is_empty() {
            if full {
                return Err(Error::new(
                    Code::ResourceExhausted,
                    "every matching provider is at capacity",
                ));
            }
            if !cordoned.is_empty() {
                return Err(Error::failed_precondition(match cordoned.as_slice() {
                    [name] => format!("provider {name} is cordoned"),
                    names => format!("providers {} are cordoned", names.join(", ")),
                }));
            }
            if unmet.is_empty() {
                return Err(Error::unavailable(
                    "no provider is online for this template",
                ));
            }
            return Err(Error::failed_precondition(format!(
                "no provider satisfies template `{}`: {}",
                template_spec.name,
                unmet.join("; ")
            )));
        }
        let request = spec.request();
        let ranked = self.config.placement.rank(&request, &candidates);
        ranked
            .iter()
            .find_map(|id| candidates.get(id.0))
            .map(|candidate| (candidate.provider_id.clone(), candidate.driver.clone()))
            .ok_or_else(|| {
                Error::internal(format!(
                    "placement `{}` ranked no candidate out of {}",
                    self.config.placement.name(),
                    candidates.len()
                ))
            })
    }

    async fn placement_for_acquire(
        &self,
        request: &AcquireRequest,
        snapshot: &PlacementSnapshot,
    ) -> Result<NewSandbox> {
        if !request.pool.is_empty() {
            let pool = self
                .backend
                .pool_get(&request.pool)
                .await?
                .ok_or_else(|| Error::not_found(format!("pool `{}` not found", request.pool)))?
                .value;
            let revision = self.backend.pool_revision(&pool.name).await?.unwrap_or(0);
            let (spec, provider, driver) = self
                .pool_placement(&pool, &request.requires, snapshot)
                .await?;
            Ok(NewSandbox {
                template: spec.template,
                pool: pool.name.clone(),
                pool_revision: revision,
                purpose: request
                    .purpose
                    .as_known()
                    .unwrap_or(Purpose::PurposeUnspecified),
                name: String::new(),
                provider_id: provider,
                driver,
                shape: spec.shape,
                network: spec.network,
                mounts: spec.mounts,
                sockets: spec.sockets,
                options: spec.options,
                requires: spec.requires,
                snapshot_capable: spec.snapshot_capable,
            })
        } else {
            let template = self
                .backend
                .template_get(&request.template)
                .await?
                .ok_or_else(|| {
                    Error::not_found(format!("template `{}` not found", request.template))
                })?
                .value;
            let template_spec = template.spec.as_option().cloned().unwrap_or_default();
            let spec = PlacementSpec {
                requires: template_spec
                    .requires
                    .as_option()
                    .cloned()
                    .unwrap_or_default()
                    .merged(&request.requires),
                template,
                shape: request.shape.as_option().cloned().unwrap_or_default(),
                network: request.network.as_option().cloned().unwrap_or_default(),
                mounts: request.mounts.clone(),
                sockets: request.sockets.clone(),
                options: request.options.clone(),
                snapshot_capable: request.purpose.as_known() == Some(Purpose::Build),
            };
            let (provider, driver) = self.place(snapshot, &spec, true)?;
            Ok(NewSandbox {
                template: spec.template,
                pool: String::new(),
                pool_revision: 0,
                purpose: request
                    .purpose
                    .as_known()
                    .unwrap_or(Purpose::PurposeUnspecified),
                name: request.name.clone(),
                provider_id: provider,
                driver,
                shape: spec.shape,
                network: spec.network,
                mounts: spec.mounts,
                sockets: spec.sockets,
                options: spec.options,
                requires: spec.requires,
                snapshot_capable: spec.snapshot_capable,
            })
        }
    }

    async fn fill_pool(&self, deficit: &backend::PoolDeficit) {
        {
            let backoff = self.pool_backoff.lock();
            if backoff
                .get(&deficit.pool)
                .is_some_and(|at| at.elapsed() < POOL_FAILURE_BACKOFF)
            {
                return;
            }
        }
        let snapshot = match self.backend.placement_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                warn!(%error, pool = %deficit.pool, "warm pool snapshot failed");
                return;
            }
        };
        let pool = match self.backend.pool_get(&deficit.pool).await {
            Ok(Some(pool)) => pool.value,
            _ => return,
        };
        if self
            .backend
            .pool_revision(&deficit.pool)
            .await
            .ok()
            .flatten()
            != Some(deficit.revision)
        {
            return;
        }
        for _ in 0..deficit.missing {
            let placement = match self
                .pool_placement(&pool, &Requirements::default(), &snapshot)
                .await
            {
                Ok(placement) => placement,
                Err(_) => {
                    self.config
                        .metrics
                        .record_pool_refill_failure(&deficit.pool);
                    self.pool_backoff
                        .lock()
                        .insert(deficit.pool.clone(), Instant::now());
                    return;
                }
            };
            let (spec, provider, driver) = placement;
            let member = NewSandbox {
                template: spec.template,
                pool: pool.name.clone(),
                pool_revision: deficit.revision,
                purpose: Purpose::Runner,
                name: String::new(),
                provider_id: provider,
                driver,
                shape: spec.shape,
                network: spec.network,
                mounts: spec.mounts,
                sockets: spec.sockets,
                options: spec.options,
                requires: spec.requires,
                snapshot_capable: spec.snapshot_capable,
            };
            match self
                .backend
                .create_pool_member(member, self.config.create_timeout)
                .await
            {
                Ok(RefillOutcome::Created(_)) => {}
                Ok(_) => return,
                Err(error) => {
                    warn!(%error, pool = %pool.name, "warm pool refill failed");
                    self.config.metrics.record_pool_refill_failure(&pool.name);
                    self.pool_backoff
                        .lock()
                        .insert(pool.name.clone(), Instant::now());
                    return;
                }
            }
        }
    }

    // ── Leases ────────────────────────────────────────────────────

    /// `Acquire`.
    pub async fn acquire(self: &Arc<Self>, request: AcquireRequest) -> Result<AcquireResponse> {
        use tracing::Instrument as _;
        let span = tracing::info_span!(
            "sandbox.acquire",
            pool = %request.pool,
            template = %request.template,
        );
        let started = Instant::now();
        let this = self.clone();
        let result = async move { this.acquire_inner(request).await }
            .instrument(span)
            .await;
        let reason = result.as_ref().err().map(|error| error.code.as_str());
        self.config
            .metrics
            .record_acquire(started.elapsed(), reason);
        result
    }

    async fn acquire_inner(self: &Arc<Self>, request: AcquireRequest) -> Result<AcquireResponse> {
        if request.idempotency_key.is_empty() {
            return Err(Error::invalid_argument("idempotencyKey is required"));
        }
        if request.pool.is_empty() == request.template.is_empty() {
            return Err(Error::invalid_argument(
                "exactly one of pool or template is required",
            ));
        }
        if !request.pool.is_empty() {
            // The pool path uses the pool's placement; silently dropping
            // these would hand back a sandbox that ignores what the caller
            // asked for.
            let mut ignored = Vec::new();
            if *request.shape != Shape::default() {
                ignored.push("shape");
            }
            if *request.network != NetworkPolicy::default() {
                ignored.push("network");
            }
            if !request.mounts.is_empty() {
                ignored.push("mounts");
            }
            if !request.sockets.is_empty() {
                ignored.push("sockets");
            }
            if !request.options.is_empty() {
                ignored.push("options");
            }
            if !ignored.is_empty() {
                return Err(Error::invalid_argument(format!(
                    "pool acquires cannot set {}; set these on the pool, or acquire from the template",
                    ignored.join(", ")
                )));
            }
            // A build sandbox must be started snapshot-capable, and a pool
            // placement is fixed: take build sandboxes from the template.
            if request.purpose == Purpose::Build {
                return Err(Error::invalid_argument(
                    "pool acquires cannot be build sandboxes; acquire from the template",
                ));
            }
        }
        let wait = match request.wait_secs {
            None => self.config.acquire_wait,
            Some(0) => Duration::ZERO,
            Some(secs) => Duration::from_secs(secs),
        };
        let deadline = Instant::now() + wait;
        let fingerprint = backend::logic::acquire_fingerprint(&request);
        let ttl = if request.ttl_secs > 0 {
            request.ttl_secs
        } else if !request.pool.is_empty() {
            self.backend
                .pool_get(&request.pool)
                .await?
                .map(|pool| pool.value.lease_ttl_secs)
                .filter(|ttl| *ttl > 0)
                .unwrap_or(self.config.lease_ttl.as_secs())
        } else {
            self.config.lease_ttl.as_secs()
        };
        let mut placement: Option<NewSandbox> = None;
        let mut attempted_with_placement = false;
        let mut waiter_for: Option<String> = None;
        let mut waiter_guard: Option<AcquireWaiterGuard> = None;
        // Starts at the first capacity-blocked step and is never reset: a
        // caller that cannot be told about overload fails over instead of
        // parking for the whole acquire wait.
        let mut capacity_deadline: Option<Instant> = None;
        let result = loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let command = AcquireCommand {
                request: request.clone(),
                fingerprint: fingerprint.clone(),
                ttl_secs: ttl,
                unclaimed_ttl_secs: self.config.unclaimed_lease_ttl.as_secs(),
                create_timeout: self.config.create_timeout,
                placement: placement.clone(),
                owner: self.node.clone(),
            };
            if placement.is_some() {
                attempted_with_placement = true;
            }
            let step = self.backend.acquire(command).await?;
            match step {
                AcquireStep::Ready { lease, sandbox } => {
                    self.clear_waiter_if_any(&request.idempotency_key, waiter_for.take())
                        .await;
                    if let Some(guard) = waiter_guard.as_mut() {
                        guard.disarm();
                    }
                    return Ok(AcquireResponse {
                        lease: lease.into(),
                        sandbox: sandbox.into(),
                        ..AcquireResponse::default()
                    });
                }
                AcquireStep::Inject { lease, sandbox } => {
                    let injected = self.inject(&lease, &sandbox, &request.inject).await;
                    match injected {
                        Ok(()) => {
                            self.backend
                                .finish_injection(&lease.id, &self.node, true)
                                .await?;
                        }
                        Err(error) => {
                            let _ = self
                                .backend
                                .finish_injection(&lease.id, &self.node, false)
                                .await;
                            self.clear_waiter_if_any(&request.idempotency_key, waiter_for.take())
                                .await;
                            if let Some(guard) = waiter_guard.as_mut() {
                                guard.disarm();
                            }
                            return Err(error);
                        }
                    }
                }
                AcquireStep::Wait {
                    last,
                    retry_after,
                    sandbox_id,
                    lease_id,
                } => {
                    if last
                        .as_ref()
                        .is_some_and(|error| error.code == Code::ResourceExhausted)
                    {
                        let capacity_at = *capacity_deadline
                            .get_or_insert(Instant::now() + self.config.acquire_capacity_wait);
                        if Instant::now() >= capacity_at {
                            break Err(last.unwrap_or_else(|| {
                                Error::new(Code::ResourceExhausted, "no capacity")
                            }));
                        }
                    }
                    if let Some(sandbox_id) = sandbox_id {
                        if waiter_guard.is_none() {
                            waiter_guard = Some(AcquireWaiterGuard {
                                service: self.clone(),
                                key: request.idempotency_key.clone(),
                                armed: true,
                            });
                        }
                        let waiter = backend::Waiter {
                            key: request.idempotency_key.clone(),
                            sandbox_id,
                            lease_id: lease_id.unwrap_or_default(),
                            owner: self.node.clone(),
                            expires_at_ms: now_ms() + 30_000,
                        };
                        let _ = self.backend.register_waiter(waiter).await;
                        waiter_for = Some(request.idempotency_key.clone());
                    }
                    let wait_until =
                        deadline.min(Instant::now() + retry_after.max(Duration::from_millis(25)));
                    if tokio::time::timeout_at(wait_until.into(), changed)
                        .await
                        .is_err()
                        && Instant::now() >= deadline
                    {
                        break Err(last.unwrap_or_else(|| {
                            Error::new(Code::DeadlineExceeded, "sandbox not ready in time")
                        }));
                    }
                }
                AcquireStep::PlacementNeeded => {
                    let snapshot = self.backend.placement_snapshot().await?;
                    match self.placement_for_acquire(&request, &snapshot).await {
                        Ok(member) => {
                            placement = Some(member);
                            // Even a zero-wait Acquire gets one placement
                            // attempt; the caller's deadline may expire while
                            // the new sandbox provisions, but the sandbox and
                            // its create work must exist.
                            continue;
                        }
                        Err(error) if error.code == Code::FailedPrecondition => break Err(error),
                        Err(error) => {
                            if Instant::now() >= deadline {
                                break Err(error);
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
                AcquireStep::Failed(error) => {
                    break Err(error);
                }
            }
            if Instant::now() >= deadline && !(placement.is_some() && !attempted_with_placement) {
                break Err(Error::new(
                    Code::DeadlineExceeded,
                    "sandbox not ready in time",
                ));
            }
        };
        self.clear_waiter_if_any(&request.idempotency_key, waiter_for)
            .await;
        if let Some(guard) = waiter_guard.as_mut() {
            guard.disarm();
        }
        result
    }

    async fn clear_waiter_if_any(&self, key: &str, waiter: Option<String>) {
        if waiter.is_some() {
            let _ = self.backend.clear_waiter(key, &self.node).await;
        }
    }

    async fn inject(
        self: &Arc<Self>,
        lease: &Lease,
        sandbox: &Sandbox,
        inject: &Inject,
    ) -> Result<()> {
        if inject.files.is_empty() {
            return Ok(());
        }
        let mut builder = tar::Builder::new(Vec::new());
        for spec in &inject.files {
            let path = spec.path.trim_start_matches('/');
            if path.is_empty() || path.split('/').any(|part| part == "..") {
                return Err(Error::invalid_argument(format!(
                    "bad inject path `{}`",
                    spec.path
                )));
            }
            let mut header = tar::Header::new_gnu();
            header.set_size(spec.data.len() as u64);
            header.set_mode(if spec.mode == 0 { 0o600 } else { spec.mode });
            header.set_mtime(now() as u64);
            header.set_cksum();
            builder
                .append_data(&mut header, path, spec.data.as_slice())
                .map_err(|error| Error::internal(format!("inject tar: {error}")))?;
        }
        let bytes = builder
            .into_inner()
            .map_err(|error| Error::internal(format!("inject tar: {error}")))?;
        self.upload_bytes(&sandbox.id, &lease.id, "/", bytes).await
    }

    /// `Renew`.
    pub async fn renew(&self, request: RenewRequest) -> Result<Lease> {
        let lease = self
            .backend
            .renew(
                &request.lease_id,
                request.ttl_secs,
                self.config.lease_ttl.as_secs(),
            )
            .await?;
        self.config.metrics.record_lease_transition("renewed");
        Ok(lease)
    }

    /// `Release`.
    pub async fn release(&self, request: ReleaseRequest) -> Result<()> {
        self.backend
            .release(
                &request.lease_id,
                request.mode.as_known().unwrap_or_default(),
                request.preserve_ttl_secs,
                self.config.preserve_ttl.as_secs(),
            )
            .await?;
        self.changed.notify_waiters();
        Ok(())
    }

    /// `Get`.
    pub async fn get(&self, id: &str, lease_id: &str) -> Result<Sandbox> {
        self.backend.get(id, lease_id).await
    }

    /// `List`, with lease ids redacted.
    pub async fn list(&self) -> Result<Vec<Sandbox>> {
        self.backend.list_all().await
    }

    /// `List` page.
    pub async fn list_page(&self, request: &ListRequest) -> Result<ListSandboxesResponse> {
        self.backend.list(request).await
    }

    /// `ListProviders`.
    pub async fn list_providers(&self) -> Result<Vec<Provider>> {
        self.backend.list_providers_all().await
    }

    /// `ListProviders` page.
    pub async fn list_providers_page(
        &self,
        request: &ListRequest,
    ) -> Result<ListProvidersResponse> {
        self.backend.list_providers(request).await
    }

    /// `Stats`.
    pub async fn stats(&self) -> Result<StatsResponse> {
        self.backend.stats().await
    }

    /// PEM of the service CA.
    pub fn ca_pem(&self) -> Option<String> {
        self.config.ca.as_ref().map(|ca| ca.pem().to_owned())
    }

    // ── Exec ──────────────────────────────────────────────────────

    /// `Exec`: stream output then the exit status.
    pub async fn exec(
        self: &Arc<Self>,
        request: ExecRequest,
    ) -> Result<BoxStream<'static, Result<ExecEvent>>> {
        if request.argv.is_empty() {
            return Err(Error::invalid_argument("argv is required"));
        }
        let exec_id = new_id("exe");
        let (tx, rx) = mpsc::channel(256);
        let finished = CancellationToken::new();
        let (disconnected, dropped) = oneshot::channel::<()>();
        let timeout = if request.timeout_secs == 0 {
            self.config.exec_timeout.as_secs().max(1)
        } else {
            request.timeout_secs
        };
        // Install the local channel before the durable handle exists, so a
        // fast provider report cannot arrive with no stream to deliver to.
        self.runtime.execs.lock().insert(
            exec_id.clone(),
            Exec {
                tx,
                finished: finished.clone(),
            },
        );
        if let Err(error) = self
            .backend
            .create_exec(&exec_id, request, timeout, &self.node)
            .await
        {
            self.runtime.execs.lock().remove(&exec_id);
            finished.cancel();
            return Err(error);
        }
        let service = Arc::clone(self);
        let watched = exec_id.clone();
        tokio::spawn(async move {
            tokio::pin!(dropped);
            let mut tick = tokio::time::interval(Duration::from_millis(200));
            loop {
                tokio::select! {
                    _ = &mut dropped => {
                        let _ = service.backend.cancel_exec(&watched, &service.node).await;
                        break;
                    }

                    _ = tick.tick() => {
                        match service.backend.exec_status(&watched).await {
                            Ok(Some(ExecStatus::Done { code })) => {
                                let tx = service
                                    .runtime
                                    .execs
                                    .lock()
                                    .get(&watched)
                                    .map(|exec| exec.tx.clone());
                                if let Some(tx) = tx {
                                    let _ = tx.send(Ok(ExecEvent::exit(code))).await;
                                }
                                break;
                            }
                            Ok(Some(ExecStatus::Failed { reason })) => {
                                let tx = service
                                    .runtime
                                    .execs
                                    .lock()
                                    .get(&watched)
                                    .map(|exec| exec.tx.clone());
                                if let Some(tx) = tx {
                                    let _ = tx.send(Err(crate::error::from_reported(reason))).await;
                                }
                                break;
                            }
                            // The durable handle is gone: the channel can no
                            // longer be delivered to, so end the caller's
                            // stream explicitly instead of pretending it
                            // completed.
                            Ok(None) => {
                                let tx = service
                                    .runtime
                                    .execs
                                    .lock()
                                    .get(&watched)
                                    .map(|exec| exec.tx.clone());
                                if let Some(tx) = tx {
                                    let _ = tx
                                        .send(Err(Error::unavailable("the sandbox was destroyed")))
                                        .await;
                                }
                                break;
                            }
                            Ok(Some(ExecStatus::Running)) => {}
                            Err(error) => {
                                tracing::debug!(%error, exec_id = %watched, "exec status check failed");
                            }
                        }
                    }
                }
            }
            let _ = service.runtime.execs.lock().remove(&watched);
            finished.cancel();
        });
        Ok(Box::pin(ExecStream {
            inner: record_exec_metrics(
                self.config.metrics.clone(),
                Instant::now(),
                tokio_stream_from(rx),
            ),
            _disconnect: disconnected,
        }))
    }

    /// `ReportExec` delivered to the owning node.
    pub async fn report_exec_local(&self, request: ReportExecRequest) -> Result<()> {
        match self.backend.report_exec(request).await? {
            backend::ReportExecOutcome::Events {
                exec_id,
                events,
                terminal,
            } => {
                let exec = self
                    .runtime
                    .execs
                    .lock()
                    .get(&exec_id)
                    .map(|exec| (exec.tx.clone(), exec.finished.clone()));
                let Some((tx, finished)) = exec else {
                    if terminal {
                        return Ok(());
                    }
                    return Err(Error::not_found("exec not found"));
                };
                for event in events {
                    if tx.send(Ok(event)).await.is_err() {
                        return Ok(());
                    }
                }
                if terminal {
                    self.runtime.execs.lock().remove(&exec_id);
                    finished.cancel();
                }
                Ok(())
            }
            backend::ReportExecOutcome::Duplicate => Ok(()),
            backend::ReportExecOutcome::AlreadyStarted { exec_id, reason } => {
                let exec = self.runtime.execs.lock().remove(&exec_id);
                if let Some(exec) = exec {
                    exec.finished.cancel();
                    let _ = tokio::time::timeout(
                        Duration::from_secs(1),
                        exec.tx.send(Err(Error::aborted(reason))),
                    )
                    .await;
                }
                Ok(())
            }
            backend::ReportExecOutcome::Gone => Err(Error::not_found("exec not found")),
        }
    }

    /// Run a command to completion, collecting output.
    pub async fn exec_collect(
        self: &Arc<Self>,
        request: ExecRequest,
    ) -> Result<(i32, Vec<u8>, Vec<u8>)> {
        collect_exec(self.exec(request).await?).await
    }

    // ── Data plane ────────────────────────────────────────────────

    /// `OpenStream`: issue a ticket.
    pub async fn open_stream(&self, request: OpenStreamRequest) -> Result<Ticket> {
        if request.kind.as_known().unwrap_or_default() == StreamKind::StreamKindUnspecified {
            return Err(Error::invalid_argument("kind is required"));
        }
        let windows_absolute = request.path.len() > 2
            && request.path.as_bytes()[0].is_ascii_alphabetic()
            && request.path.as_bytes()[1] == b':';
        if !request.path.starts_with('/') && !windows_absolute {
            return Err(Error::invalid_argument("path must be absolute"));
        }
        let sandbox = self.get(&request.sandbox_id, &request.lease_id).await?;
        backend::logic::usable(&sandbox)?;
        let token = uuid::Uuid::new_v4().simple().to_string();
        let ticket_id = new_id("tkt");
        let kind = request.kind.as_known().unwrap_or_default();
        let expires_at = now() + self.config.ticket_ttl.as_secs() as i64;
        self.backend
            .create_ticket(
                &ticket_id,
                &request.sandbox_id,
                &request.lease_id,
                kind,
                &request.path,
                &backend::logic::token_hash(&token),
                expires_at,
                &self.node,
            )
            .await?;
        self.insert_ticket(
            &ticket_id,
            request.sandbox_id,
            kind,
            request.path,
            expires_at,
        );
        Ok(Ticket {
            url: format!("/sandbox/v1/streams/{ticket_id}"),
            token,
            expires_at,
            ..Ticket::default()
        })
    }

    fn insert_ticket(
        &self,
        ticket: &str,
        sandbox_id: String,
        kind: StreamKind,
        guest_path: String,
        expires_at: i64,
    ) {
        let (tx, rx) = mpsc::channel(TRANSFER_QUEUE);
        let control = tx.clone();
        let (status, _) = watch::channel(TransferStatus::Open);
        self.runtime.tickets.lock().insert(
            ticket.to_owned(),
            Blob {
                sandbox_id,
                kind,
                guest_path,
                expires_at,
                producer_claimed: false,
                consumer_claimed: false,
                status,
                tx: Some(tx),
                rx: Some(rx),
                control: Some(control),
            },
        );
    }

    /// A losing transfer-claim is an HTTP precondition failure. The Connect
    /// code alone maps to 400, which a caller cannot distinguish from a bad
    /// request; the duplicate side is a "this was already claimed" state.
    fn claim_failure(error: Error) -> Error {
        if error.code == Code::FailedPrecondition {
            error.with_http_status(StatusCode::PRECONDITION_FAILED)
        } else {
            error
        }
    }

    /// Claim the caller's side of a ticket with its bearer token.
    pub async fn claim_caller(
        &self,
        ticket: &str,
        token: &str,
        side: TransferSide,
    ) -> Result<StreamClaim> {
        let expected = match side {
            TransferSide::Producer => StreamKind::Upload,
            TransferSide::Consumer => StreamKind::Download,
        };
        self.backend
            .claim_ticket(
                ticket,
                ticket_side(side),
                expected,
                self.config.work_timeout.as_secs() as i64,
                TicketAuthority::Caller { token },
            )
            .await
            .map_err(Self::claim_failure)?;
        self.claim_local(ticket, side).map_err(Self::claim_failure)
    }

    /// Claim the agent's side (the provider must own the sandbox).
    pub async fn claim_agent(
        &self,
        provider_id: &str,
        generation: u64,
        ticket: &str,
        side: TransferSide,
    ) -> Result<StreamClaim> {
        self.backend
            .claim_ticket(
                ticket,
                ticket_side(side),
                self.local_ticket_kind(ticket)?,
                self.config.work_timeout.as_secs() as i64,
                TicketAuthority::Provider {
                    provider_id,
                    generation,
                },
            )
            .await
            .map_err(Self::claim_failure)?;
        self.claim_local(ticket, side).map_err(Self::claim_failure)
    }

    fn local_ticket_kind(&self, ticket: &str) -> Result<StreamKind> {
        self.runtime
            .tickets
            .lock()
            .get(ticket)
            .map(|blob| blob.kind)
            .ok_or_else(|| Error::not_found("ticket not found or expired"))
    }

    /// Claim the service's own side of an internally issued ticket.
    async fn claim_internal(&self, ticket: &str, side: TransferSide) -> Result<StreamClaim> {
        self.backend
            .claim_ticket(
                ticket,
                ticket_side(side),
                self.local_ticket_kind(ticket)?,
                self.config.work_timeout.as_secs() as i64,
                TicketAuthority::Internal,
            )
            .await
            .map_err(Self::claim_failure)?;
        self.claim_local(ticket, side).map_err(Self::claim_failure)
    }

    fn claim_local(&self, ticket: &str, side: TransferSide) -> Result<StreamClaim> {
        let mut tickets = self.runtime.tickets.lock();
        let blob = tickets
            .get_mut(ticket)
            .ok_or_else(|| Error::not_found("ticket not found or expired"))?;
        let already = match side {
            TransferSide::Producer => std::mem::replace(&mut blob.producer_claimed, true),
            TransferSide::Consumer => std::mem::replace(&mut blob.consumer_claimed, true),
        };
        if already {
            return Err(Error::failed_precondition(
                "that side of the transfer was already claimed",
            ));
        }
        if blob.producer_claimed && blob.consumer_claimed {
            blob.status.send_replace(TransferStatus::Ready);
            blob.expires_at = now() + self.config.work_timeout.as_secs() as i64;
        }
        let (producer, consumer) = match side {
            TransferSide::Producer => (blob.tx.take(), None),
            TransferSide::Consumer => (None, blob.rx.take()),
        };
        Ok(StreamClaim {
            sandbox_id: blob.sandbox_id.clone(),
            kind: blob.kind,
            guest_path: blob.guest_path.clone(),
            ticket: ticket.to_owned(),
            status: blob.status.subscribe(),
            producer,
            consumer,
        })
    }

    /// Wait until both sides have claimed the ticket.
    pub async fn wait_peer(&self, claim: &mut StreamClaim) -> Result<()> {
        let wait = async {
            loop {
                let current = { claim.status.borrow_and_update().clone() };
                match current {
                    TransferStatus::Ready => return Ok(()),
                    TransferStatus::Aborted(reason) => return Err(Error::unavailable(reason)),
                    TransferStatus::Open => {
                        if claim.status.changed().await.is_err() {
                            return Err(Error::unavailable("transfer ticket closed"));
                        }
                    }
                }
            }
        };
        tokio::time::timeout(self.config.work_timeout, wait)
            .await
            .map_err(|_| Error::unavailable("the peer did not claim the transfer"))?
    }

    /// Close the producer side cleanly.
    pub async fn finish_ticket(&self, ticket: &str) {
        if let Some(blob) = self.runtime.tickets.lock().get_mut(ticket) {
            blob.control.take();
        }
        let _ = self.backend.finish_ticket(ticket, "done", "").await;
    }

    /// Abort a transfer.
    pub async fn abort_ticket(&self, ticket: &str, reason: &str) {
        self.abort_local_ticket(ticket, reason).await;
        let _ = self.backend.finish_ticket(ticket, "aborted", reason).await;
    }

    async fn abort_local_ticket(&self, ticket: &str, reason: &str) {
        let control = {
            let mut tickets = self.runtime.tickets.lock();
            let Some(blob) = tickets.get_mut(ticket) else {
                return;
            };
            blob.status
                .send_replace(TransferStatus::Aborted(reason.to_owned()));
            blob.control.take()
        };
        if let Some(tx) = control {
            let _ = tokio::time::timeout(
                Duration::from_secs(5),
                tx.send(TransferChunk::Failed(reason.to_owned())),
            )
            .await;
        }
        self.runtime.tickets.lock().remove(ticket);
    }

    /// Retire a ticket: the local rendezvous and its durable row.
    pub async fn remove_ticket(&self, ticket: &str) {
        self.runtime.tickets.lock().remove(ticket);
        let _ = self.backend.finish_ticket(ticket, "done", "").await;
    }

    /// Upload a local tar into the guest.
    pub async fn upload_path(
        self: &Arc<Self>,
        sandbox_id: &str,
        lease_id: &str,
        guest_dir: &str,
        tar: &std::path::Path,
    ) -> Result<()> {
        self.get(sandbox_id, lease_id).await?;
        let file = tokio::fs::File::open(tar)
            .await
            .map_err(|error| Error::internal(format!("upload: {error}")))?;
        let source = tokio_util::io::ReaderStream::new(file).map(|chunk| match chunk {
            Ok(bytes) => TransferChunk::Data(bytes.to_vec()),
            Err(error) => TransferChunk::Failed(format!("upload: {error}")),
        });
        self.upload_stream(sandbox_id, lease_id, guest_dir, source)
            .await
    }

    async fn upload_bytes(
        self: &Arc<Self>,
        sandbox_id: &str,
        lease_id: &str,
        guest_dir: &str,
        bytes: Vec<u8>,
    ) -> Result<()> {
        let chunks: Vec<TransferChunk> = bytes
            .chunks(64 * 1024)
            .map(|chunk| TransferChunk::Data(chunk.to_vec()))
            .collect();
        self.upload_stream(
            sandbox_id,
            lease_id,
            guest_dir,
            futures::stream::iter(chunks),
        )
        .await
    }

    async fn upload_stream<S>(
        self: &Arc<Self>,
        sandbox_id: &str,
        lease_id: &str,
        guest_dir: &str,
        mut source: S,
    ) -> Result<()>
    where
        S: futures::Stream<Item = TransferChunk> + Send + Unpin,
    {
        let mut claim = self
            .open_internal(sandbox_id, lease_id, StreamKind::Upload, guest_dir)
            .await?;
        let ticket = claim.ticket.clone();
        let work_id = self
            .backend
            .start_copy(
                sandbox_id,
                &ticket,
                guest_dir,
                StreamKind::Upload,
                &self.node,
            )
            .await?;
        if let Err(error) = self.wait_peer(&mut claim).await {
            self.abort_ticket(&ticket, crate::error::message(&error))
                .await;
            return Err(error);
        }
        let Some(tx) = claim.take_producer() else {
            return Err(Error::internal("upload ticket has no producer side"));
        };
        let max_bytes = self.config.blob_max_bytes;
        let feed = async move {
            let mut written = 0u64;
            while let Some(chunk) = source.next().await {
                if let TransferChunk::Data(bytes) = &chunk {
                    written = written.saturating_add(bytes.len() as u64);
                    if written > max_bytes {
                        let _ = tx
                            .send(TransferChunk::Failed("blob exceeds the size limit".into()))
                            .await;
                        return Err(Error::out_of_range(format!(
                            "blob exceeds the {max_bytes} byte limit"
                        )));
                    }
                }
                let failed = matches!(chunk, TransferChunk::Failed(_));
                if tx.send(chunk).await.is_err() {
                    return Err(Error::unavailable("transfer consumer went away"));
                }
                if failed {
                    return Err(Error::unavailable("transfer source failed"));
                }
            }
            drop(tx);
            Ok(())
        }
        .await;
        self.finish_ticket(&ticket).await;
        let result = match feed {
            Ok(()) => self.work_result(&work_id, self.config.work_timeout).await,
            Err(error) => {
                self.abort_ticket(&ticket, crate::error::message(&error))
                    .await;
                Err(error)
            }
        };
        self.remove_ticket(&ticket).await;
        result
    }

    /// Download a guest path into a local tar.
    pub async fn download_path(
        self: &Arc<Self>,
        sandbox_id: &str,
        lease_id: &str,
        guest_path: &str,
        dest: &std::path::Path,
    ) -> Result<()> {
        use tokio::io::AsyncWriteExt as _;
        self.get(sandbox_id, lease_id).await?;
        let mut claim = self
            .open_internal(sandbox_id, lease_id, StreamKind::Download, guest_path)
            .await?;
        let ticket = claim.ticket.clone();
        let work_id = self
            .backend
            .start_copy(
                sandbox_id,
                &ticket,
                guest_path,
                StreamKind::Download,
                &self.node,
            )
            .await?;
        if let Err(error) = self.wait_peer(&mut claim).await {
            self.abort_ticket(&ticket, crate::error::message(&error))
                .await;
            return Err(error);
        }
        let Some(mut rx) = claim.take_consumer() else {
            return Err(Error::internal("download ticket has no consumer side"));
        };
        let mut file = tokio::fs::File::create(dest)
            .await
            .map_err(|error| Error::internal(format!("download: {error}")))?;
        let result = async {
            while let Some(chunk) = rx.recv().await {
                match chunk {
                    TransferChunk::Data(bytes) => file
                        .write_all(&bytes)
                        .await
                        .map_err(|error| Error::internal(format!("download: {error}")))?,
                    TransferChunk::Failed(reason) => {
                        return Err(Error::unavailable(reason));
                    }
                }
            }
            file.flush()
                .await
                .map_err(|error| Error::internal(format!("download: {error}")))
        }
        .await;
        drop(rx);
        let result = match result {
            Ok(()) => self.work_result(&work_id, self.config.work_timeout).await,
            Err(error) => Err(error),
        };
        if result.is_err() {
            let _ = tokio::fs::remove_file(dest).await;
        }
        self.remove_ticket(&ticket).await;
        result
    }

    async fn open_internal(
        self: &Arc<Self>,
        sandbox_id: &str,
        lease_id: &str,
        kind: StreamKind,
        guest_path: &str,
    ) -> Result<StreamClaim> {
        let ticket = new_id("tkt");
        let expires_at = now() + self.config.ticket_ttl.as_secs() as i64;
        self.backend
            .create_ticket(
                &ticket, sandbox_id, lease_id, kind, guest_path, "", expires_at, &self.node,
            )
            .await?;
        self.insert_ticket(
            &ticket,
            sandbox_id.to_owned(),
            kind,
            guest_path.to_owned(),
            expires_at,
        );
        let side = match kind {
            StreamKind::Upload => TransferSide::Producer,
            StreamKind::Download => TransferSide::Consumer,
            StreamKind::StreamKindUnspecified => {
                return Err(Error::invalid_argument("transfer kind is unset"));
            }
        };
        self.claim_internal(&ticket, side).await
    }

    /// Queue the provider copy for an already-claimed ticket.
    pub async fn start_copy(
        &self,
        sandbox_id: &str,
        ticket: &str,
        guest_path: &str,
        kind: StreamKind,
    ) -> Result<String> {
        self.backend
            .start_copy(sandbox_id, ticket, guest_path, kind, &self.node)
            .await
    }

    /// Wait for one queued work item's durable result.
    pub async fn await_work(&self, work_id: &str, timeout: Duration) -> Result<()> {
        self.work_result(work_id, timeout).await
    }

    async fn work_result(&self, work_id: &str, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(result) = self.backend.work_result(work_id).await? {
                result.into_result()?;
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Error::unavailable("provider did not report the request"));
            }
            let changed = self.changed.notified();
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                _ = changed => {}
            }
        }
    }

    async fn work_value(&self, work_id: &str, timeout: Duration) -> Result<WorkResult> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(result) = self.backend.work_result(work_id).await? {
                return Ok(result);
            }
            if Instant::now() >= deadline {
                return Err(Error::unavailable("provider did not report the request"));
            }
            let changed = self.changed.notified();
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                _ = changed => {}
            }
        }
    }

    // ── Snapshot / power ──────────────────────────────────────────

    /// `Snapshot`.
    pub async fn snapshot(&self, request: SnapshotRequest) -> Result<Template> {
        let start = self
            .backend
            .snapshot_start(SnapshotCommand {
                sandbox_id: request.sandbox_id.clone(),
                name: request.name.clone(),
                lease_id: request.lease_id.clone(),
                idempotency_key: request.idempotency_key.clone(),
                owner: self.node.clone(),
                timeout: self.config.work_timeout,
            })
            .await?;
        match start {
            SnapshotStart::Replay(template) => Ok(template),
            SnapshotStart::Queued { work_id } => {
                let result = self
                    .work_value(&work_id, self.config.work_timeout)
                    .await
                    .map_err(|error| {
                        if error.code == Code::Unavailable {
                            Error::unavailable("provider did not report the snapshot")
                        } else {
                            error
                        }
                    })?;
                if !result.ok {
                    return Err(crate::error::from_reported(result.error));
                }
                result
                    .template
                    .ok_or_else(|| Error::internal("snapshot completed without a template receipt"))
            }
        }
    }

    /// `Stop`.
    pub async fn stop(&self, id: &str, lease_id: &str) -> Result<()> {
        self.power(id, lease_id, PowerOp::Stop).await
    }

    /// `Start`.
    pub async fn start(&self, id: &str, lease_id: &str, forkable: bool) -> Result<()> {
        self.power(id, lease_id, PowerOp::Start { forkable }).await
    }

    /// `Pause`.
    pub async fn pause(&self, id: &str, lease_id: &str) -> Result<()> {
        self.power(id, lease_id, PowerOp::Pause).await
    }

    /// `Resume`.
    pub async fn resume(&self, id: &str, lease_id: &str) -> Result<()> {
        self.power(id, lease_id, PowerOp::Resume).await
    }

    async fn power(&self, id: &str, lease_id: &str, op: PowerOp) -> Result<()> {
        let work_id = self
            .backend
            .power(id, lease_id, op, self.config.work_timeout)
            .await?;
        self.work_result(&work_id, self.config.work_timeout).await
    }

    // ── Providers ─────────────────────────────────────────────────

    /// `Register`.
    pub async fn register(&self, request: RegisterRequest) -> Result<RegisterResponse> {
        self.backend.register(request, None, &self.node).await
    }

    /// `Register` for a certificate-authenticated provider.
    pub async fn register_enrolled(
        &self,
        request: RegisterRequest,
        provider_id: &str,
    ) -> Result<RegisterResponse> {
        self.backend
            .register(request, Some(provider_id), &self.node)
            .await
    }

    /// `Heartbeat`.
    pub async fn heartbeat(&self, request: HeartbeatRequest) -> Result<()> {
        self.backend.heartbeat(request).await
    }

    /// `Poll`: long-poll for work.
    pub async fn poll(&self, request: PollRequest) -> Result<PollResponse> {
        let deadline = Instant::now() + Duration::from_secs(request.wait_secs.min(55));
        loop {
            let work = self
                .backend
                .poll_work(
                    &request.provider_id,
                    request.generation,
                    self.config.poll_batch,
                    self.config.delivery_timeout,
                )
                .await?;
            if !work.is_empty() {
                return Ok(PollResponse {
                    work,
                    ..PollResponse::default()
                });
            }
            if Instant::now() >= deadline {
                return Ok(PollResponse::default());
            }
            let changed = self.changed.notified();
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                _ = changed => {}
            }
        }
    }

    /// `Report`.
    pub async fn report(&self, request: ReportRequest) -> Result<()> {
        self.backend.report(request).await?;
        self.changed.notify_waiters();
        Ok(())
    }

    /// `ReportExec` from any node; routes to the owning node.
    pub async fn report_exec(&self, request: ReportExecRequest) -> Result<()> {
        let owner = self.exec_owner(&request.exec_id).await?;
        match owner {
            Some(owner) if !self.is_local_owner(&owner) => {
                self.forward_report_exec(&owner, &request).await
            }
            Some(_) => self.report_exec_local(request).await,
            None => Err(Error::not_found("exec not found")),
        }
    }

    async fn forward_report_exec(&self, owner: &Owner, request: &ReportExecRequest) -> Result<()> {
        let client = self.peers.client(&owner.node_id)?;
        let url = self
            .config
            .peers
            .iter()
            .find(|peer| peer.node_id == owner.node_id)
            .map(|peer| {
                format!(
                    "{}/sandbox/v1/internal/report-exec",
                    peer.url.trim_end_matches('/')
                )
            })
            .ok_or_else(|| Error::unavailable("owning peer is not configured"))?;
        let mut builder = client.post(url).json(request);
        for (name, value) in self.node_auth_headers() {
            builder = builder.header(name, value);
        }
        let response = builder.send().await.map_err(|error| {
            Error::unavailable(format!(
                "forwarding ReportExec to {}: {error}",
                owner.node_id
            ))
        })?;
        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            Err(Error::unavailable(format!(
                "owning node rejected ReportExec ({status}): {body}"
            )))
        }
    }

    /// `Reconcile`.
    pub async fn reconcile(&self, request: ReconcileRequest) -> Result<ReconcileResponse> {
        let response = self.backend.reconcile(request).await?;
        self.changed.notify_waiters();
        Ok(response)
    }

    /// `CordonProvider` / `UncordonProvider`.
    pub async fn set_provider_cordon(&self, reference: &str, cordoned: bool) -> Result<Provider> {
        let provider = self.backend.set_cordon(reference, cordoned).await?;
        self.changed.notify_waiters();
        Ok(provider)
    }

    /// `DrainProvider`.
    pub async fn drain_provider(&self, reference: &str) -> Result<DrainProviderResult> {
        let result = self.backend.drain(reference).await?;
        self.changed.notify_waiters();
        Ok(result)
    }

    /// `DeleteProvider`.
    pub async fn delete_provider(&self, name: &str) -> Result<()> {
        self.backend.delete_provider(name).await?;
        self.changed.notify_waiters();
        Ok(())
    }

    /// Mint a single-use join token, optionally bound to one provider name.
    ///
    /// A bound token enrolls only that name and is the only kind that may
    /// re-enroll an existing provider (replacing its certificates). An
    /// unbound token may only enroll a name that does not exist yet.
    pub async fn create_join_token(
        &self,
        ttl: Duration,
        provider_name: &str,
    ) -> Result<CreateJoinTokenResponse> {
        let ca = self.config.ca.as_ref().ok_or_else(|| {
            Error::failed_precondition("enrollment is unavailable: the service has no TLS CA")
        })?;
        if !provider_name.is_empty() {
            backend::logic::validate_name("provider", provider_name)?;
        }
        let ttl = if ttl.is_zero() {
            DEFAULT_JOIN_TOKEN_TTL
        } else {
            ttl
        };
        if ttl > MAX_JOIN_TOKEN_TTL {
            return Err(Error::invalid_argument(format!(
                "join token ttl must be at most {}h",
                MAX_JOIN_TOKEN_TTL.as_secs() / 3600
            )));
        }
        let id = new_id("jt");
        let secret = tls::random_hex(32);
        let seconds = i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX);
        let expires_at = now().saturating_add(seconds);
        self.backend
            .create_join_token(
                &id,
                &tls::sha256_hex(secret.as_bytes()),
                expires_at,
                provider_name,
            )
            .await?;
        Ok(CreateJoinTokenResponse {
            token: tls::join_token(&id, &secret, ca.sha256_hex()),
            expires_at,
            ..CreateJoinTokenResponse::default()
        })
    }

    /// The provider a client certificate authenticates.
    pub async fn provider_identity(&self, fingerprint_sha256: &str) -> Result<ProviderIdentity> {
        self.backend.provider_identity(fingerprint_sha256).await
    }

    /// `Enroll`: exchange a single-use join token for a client certificate.
    ///
    /// Every token-validation failure returns the same `Unauthenticated`
    /// error, so the endpoint cannot be used as an oracle for which part of
    /// a token is wrong. A retry with the same token, secret, CSR and name
    /// returns the certificate the original enrollment issued; a different
    /// CSR or name is refused as "join token already used". Name-binding
    /// failures are authorization errors about an otherwise valid token and
    /// keep their own codes.
    pub async fn enroll(&self, request: EnrollRequest) -> Result<EnrollResponse> {
        let ca = self.config.ca.clone().ok_or_else(|| {
            Error::failed_precondition("enrollment is unavailable: the service has no TLS CA")
        })?;
        let invalid = |reason: &str| {
            warn!(reason, provider = %request.name, "enroll rejected");
            Error::new(Code::Unauthenticated, "invalid join token")
        };
        let parts =
            tls::parse_join_token(&request.token).map_err(|_| invalid("malformed join token"))?;
        if !backend::logic::constant_time_eq(parts.ca_sha256.as_bytes(), ca.sha256_hex().as_bytes())
        {
            return Err(invalid("join token was issued for a different CA"));
        }
        backend::logic::validate_name("provider", &request.name)?;
        // Signing happens before the token is consumed, so a malformed CSR
        // never burns a valid token. A replay discards this fresh signature
        // and returns the stored one.
        let signed = ca.sign_client(&request.csr_pem, &request.name, tls::CLIENT_CERT_LIFETIME)?;
        let outcome = self
            .backend
            .enroll(
                parts.id,
                &tls::sha256_hex(parts.secret.as_bytes()),
                &request.name,
                &tls::sha256_hex(request.csr_pem.as_bytes()),
                &signed.sha256,
                &signed.cert_pem,
                signed.expires_at,
            )
            .await?;
        let (cert_pem, expires_at) = match outcome {
            backend::EnrollOutcome::Issued => (signed.cert_pem, signed.expires_at),
            backend::EnrollOutcome::Replay {
                cert_pem,
                expires_at,
            } => (cert_pem, expires_at),
        };
        tracing::info!(provider = %request.name, "provider enrolled");
        Ok(EnrollResponse {
            cert_pem,
            ca_pem: ca.pem().to_owned(),
            expires_at,
            ..EnrollResponse::default()
        })
    }

    /// `RenewCertificate`.
    pub async fn renew_certificate(
        &self,
        request: RenewCertificateRequest,
        fingerprint_sha256: &str,
    ) -> Result<RenewCertificateResponse> {
        let ca = self.config.ca.clone().ok_or_else(|| {
            Error::failed_precondition("enrollment is unavailable: the service has no TLS CA")
        })?;
        let signed = ca.sign_client(
            &request.csr_pem,
            &self.provider_identity(fingerprint_sha256).await?.name,
            tls::CLIENT_CERT_LIFETIME,
        )?;
        self.backend
            .renew_certificate(fingerprint_sha256, &signed.sha256, signed.expires_at)
            .await?;
        Ok(RenewCertificateResponse {
            cert_pem: signed.cert_pem,
            expires_at: signed.expires_at,
            ..RenewCertificateResponse::default()
        })
    }

    // ── Routing helpers for the HTTP surface ─────────────────────

    /// The owner URL and client for a peer, from configuration only.
    pub fn peer_endpoint(&self, owner: &Owner) -> Result<(String, &reqwest::Client)> {
        let configured = self
            .config
            .peers
            .iter()
            .find(|peer| peer.node_id == owner.node_id)
            .ok_or_else(|| {
                Error::unavailable(format!("peer node `{}` is not configured", owner.node_id))
            })?;
        let base = configured.url.trim_end_matches('/').to_owned();
        let client = self.peers.client(&owner.node_id)?;
        Ok((base, client))
    }
}

fn ticket_side(side: TransferSide) -> TicketSide {
    match side {
        TransferSide::Producer => TicketSide::Producer,
        TransferSide::Consumer => TicketSide::Consumer,
    }
}

fn tokio_stream_from(
    mut rx: mpsc::Receiver<Result<ExecEvent>>,
) -> BoxStream<'static, Result<ExecEvent>> {
    futures::stream::poll_fn(move |cx| rx.poll_recv(cx)).boxed()
}

/// Record an exec's duration and exit class when its terminal event passes.
fn record_exec_metrics(
    metrics: crate::metrics::SandboxMetrics,
    started: Instant,
    stream: BoxStream<'static, Result<ExecEvent>>,
) -> BoxStream<'static, Result<ExecEvent>> {
    futures::stream::unfold((stream, false), move |(mut stream, done)| {
        let metrics = metrics.clone();
        async move {
            if done {
                return None;
            }
            match stream.next().await {
                Some(Ok(event)) => {
                    if let Some(code) = match event.event {
                        Some(exec_event::Event::ExitCode(code)) => Some(code),
                        _ => None,
                    } {
                        let class = if code == 0 { "success" } else { "failure" };
                        metrics.record_exec(started.elapsed(), class);
                        Some((Ok(event), (stream, true)))
                    } else {
                        Some((Ok(event), (stream, false)))
                    }
                }
                Some(Err(error)) => {
                    metrics.record_exec(started.elapsed(), "error");
                    Some((Err(error), (stream, true)))
                }
                None => {
                    metrics.record_exec(started.elapsed(), "unknown");
                    None
                }
            }
        }
    })
    .boxed()
}

/// Drain an exec stream into `(exit, stdout, stderr)`.
pub async fn collect_exec(
    mut stream: BoxStream<'static, Result<ExecEvent>>,
) -> Result<(i32, Vec<u8>, Vec<u8>)> {
    let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
    while let Some(event) = stream.next().await {
        let event = event?;
        match event.event {
            Some(exec_event::Event::Stdout(bytes)) => stdout.extend(bytes),
            Some(exec_event::Event::Stderr(bytes)) => stderr.extend(bytes),
            Some(exec_event::Event::ExitCode(code)) => return Ok((code, stdout, stderr)),
            None => {}
        }
    }
    Err(Error::unavailable(
        "exec stream ended without an exit status",
    ))
}
