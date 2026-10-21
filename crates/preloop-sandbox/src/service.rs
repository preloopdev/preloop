//! The Sandbox Service core: templates, pools, leases, placement, provider
//! work queues, fencing, reconciliation and the generic warm pool.
//!
//! State lives in memory behind one mutex and is written through to the
//! [`Store`]; the store is the restart source. Providers (in-process or
//! remote host agents, connectors) receive work by long-polling and report
//! results fenced by their registration generation.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt as _;
use futures::stream::BoxStream;
use parking_lot::Mutex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest as _, Sha256};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::api::*;
use crate::connect::{Code, Error, Result};
use crate::placement::{Candidate, CandidateId, LeastLoaded, Placement, PlacementRequest};
use crate::store::{Kind, Store};

/// Tunables.
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    /// Default lease TTL.
    pub lease_ttl: Duration,
    /// Default time `Acquire` waits for a ready sandbox.
    pub acquire_wait: Duration,
    /// A provider silent this long is not placed on.
    pub provider_timeout: Duration,
    /// A create not reported within this window fails.
    pub create_timeout: Duration,
    /// Default preservation window.
    pub preserve_ttl: Duration,
    /// Directory for data-plane blobs.
    pub blob_dir: PathBuf,
    /// Maximum bytes accepted for one blob upload (data plane).
    pub blob_max_bytes: u64,
    /// How long a data-plane ticket stays valid.
    pub ticket_ttl: Duration,
    /// How long a caller waits for a provider to report a snapshot, copy,
    /// pause or resume before the request is failed.
    pub work_timeout: Duration,
    /// A provider silent this many times `provider_timeout` is treated as
    /// lost: its sandboxes fail, which frees leases and closes exec streams.
    pub provider_lost_factor: u32,
    /// Strategy that ranks the candidates the capability filter admits.
    /// `PRELOOP_SANDBOX_PLACEMENT` selects a built-in one in the CLI and the
    /// engine; the default is [`LeastLoaded`].
    pub placement: Arc<dyn Placement>,
    /// `preloop.sandbox.*` instruments. Defaults to a no-op registry;
    /// callers wanting real export build one from
    /// `Observability::meter()` and overwrite this field.
    pub metrics: crate::metrics::SandboxMetrics,
    /// Most work items one `Poll` returns (`PRELOOP_SANDBOX_POLL_BATCH`).
    pub poll_batch: usize,
    /// How long delivered work stays in flight before it is redelivered
    /// (`PRELOOP_SANDBOX_DELIVERY_TIMEOUT_SECS`).
    pub delivery_timeout: Duration,
    /// An exec with no explicit deadline gets this one
    /// (`PRELOOP_SANDBOX_EXEC_TIMEOUT_SECS`).
    pub exec_timeout: Duration,
}

impl ServiceConfig {
    /// Defaults with blobs under `blob_dir`.
    pub fn new(blob_dir: PathBuf) -> Self {
        Self {
            lease_ttl: Duration::from_secs(3600),
            acquire_wait: Duration::from_secs(900),
            provider_timeout: Duration::from_secs(45),
            create_timeout: Duration::from_secs(1800),
            preserve_ttl: Duration::from_secs(600),
            blob_dir,
            blob_max_bytes: DEFAULT_BLOB_MAX_BYTES,
            ticket_ttl: Duration::from_secs(300),
            work_timeout: Duration::from_secs(900),
            provider_lost_factor: 3,
            placement: Arc::new(LeastLoaded),
            metrics: crate::metrics::SandboxMetrics::default(),
            poll_batch: 64,
            delivery_timeout: Duration::from_secs(120),
            exec_timeout: Duration::from_secs(3600),
        }
    }
}

/// Pause between warm-pool refills after a creation failure.
const POOL_FAILURE_BACKOFF: Duration = Duration::from_secs(10);

/// Default `ListRequest.page_size`.
pub const DEFAULT_PAGE_SIZE: u32 = 100;

/// Largest accepted `ListRequest.page_size`.
pub const MAX_PAGE_SIZE: u32 = 1000;

/// Default cap for one blob upload: 8 GiB.
pub const DEFAULT_BLOB_MAX_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// How often `Reconcile` re-queues a destroy for a `Terminating` record whose
/// machine is still in the provider's inventory. A provider that keeps
/// reporting the machine alive is not making progress: after this many
/// attempts the record is dropped and the machine becomes an orphan the same
/// reconcile destroys.
const MAX_DESTROY_ATTEMPTS: u32 = 3;

/// How long after its deadline an exec is swept. The driver enforces the
/// deadline itself (exit 124) and its clock starts after delivery, so the
/// sweep waits out the delivery and report latency before ending the caller's
/// stream as a backstop.
const EXEC_SWEEP_GRACE: Duration = Duration::from_secs(5);

/// Names are identifiers: they reach driver-native names, snapshot paths and
/// pool configuration, so path separators and traversal are rejected.
fn validate_name(kind: &str, name: &str) -> Result<()> {
    let bad = name.is_empty()
        || name.len() > 128
        || name.contains('/')
        || name.contains('\\')
        || name.contains("..")
        || name.chars().any(|c| c.is_control());
    if bad {
        return Err(Error::invalid(format!("invalid {kind} name `{name}`")));
    }
    Ok(())
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default()
}

/// Remove every file a previous process left in the blob directory.
fn sweep_blob_dir(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file() {
            match std::fs::remove_file(&path) {
                Ok(()) => info!(path = %path.display(), "removed orphaned sandbox blob"),
                Err(error) => warn!(
                    path = %path.display(),
                    %error,
                    "could not remove orphaned sandbox blob"
                ),
            }
        }
    }
}

fn new_id(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

/// sha256 of the canonical JSON of a template spec.
pub fn template_digest(spec: &TemplateSpec) -> String {
    let canonical = serde_json::to_vec(spec).unwrap_or_default();
    format!("sha256:{}", hex::encode(Sha256::digest(&canonical)))
}

/// Page `items` (already ordered by id) as the request asks.
///
/// The token is the base64url encoding of the last returned id: callers pass
/// it back untouched, and an id that has since disappeared still resumes at
/// the right position.
fn paginate<T>(
    items: impl IntoIterator<Item = (String, T)>,
    request: &ListRequest,
) -> Result<(Vec<T>, String)> {
    use base64::Engine as _;
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let limit = match request.page_size {
        0 => DEFAULT_PAGE_SIZE,
        size => size.min(MAX_PAGE_SIZE),
    } as usize;
    let after = if request.page_token.is_empty() {
        None
    } else {
        let decoded = engine
            .decode(request.page_token.as_bytes())
            .map_err(|_| Error::invalid("invalid page token"))?;
        Some(String::from_utf8(decoded).map_err(|_| Error::invalid("invalid page token"))?)
    };
    let items: Vec<(String, T)> = items.into_iter().collect();
    let start = after
        .as_ref()
        .map_or(0, |after| items.partition_point(|(id, _)| id <= after));
    let mut page = Vec::with_capacity(limit.min(items.len()));
    let mut last = None;
    for (id, item) in items.into_iter().skip(start) {
        if page.len() == limit {
            let token = last.expect("a full page has a last id");
            return Ok((page, engine.encode(token)));
        }
        last = Some(id);
        page.push(item);
    }
    Ok((page, String::new()))
}

/// Fingerprint of the fields that decide *which* sandbox an idempotency key
/// acquired: pool or template, shape, requirements, purpose, egress, mounts,
/// sockets, options and name.
///
/// Call tactics (`ttl_secs`, `wait_secs`) and the lease's injected payload
/// are deliberately excluded: a retry may change them without becoming a
/// different request, and re-injection is idempotent by design.
fn acquire_fingerprint(request: &AcquireRequest) -> String {
    let identity = (
        &request.pool,
        &request.template,
        request.shape,
        &request.requires,
        request.purpose,
        &request.network,
        &request.mounts,
        &request.sockets,
        &request.options,
        &request.name,
    );
    let canonical = serde_json::to_vec(&identity).unwrap_or_default();
    format!("sha256:{}", hex::encode(Sha256::digest(&canonical)))
}

#[derive(Debug)]
enum Pending {
    Create {
        sandbox_id: String,
        driver: String,
    },
    Destroy {
        sandbox_id: String,
    },
    Exec {
        exec_id: String,
    },
    Copy {
        reply: oneshot::Sender<Result<String>>,
    },
    Snapshot {
        reply: oneshot::Sender<Result<String>>,
    },
    Pause {
        sandbox_id: String,
        reply: oneshot::Sender<Result<String>>,
    },
    Stop {
        sandbox_id: String,
        reply: oneshot::Sender<Result<String>>,
    },
    Start {
        sandbox_id: String,
        forkable: bool,
        reply: oneshot::Sender<Result<String>>,
    },
    Resume {
        sandbox_id: String,
        reply: oneshot::Sender<Result<String>>,
    },
}

struct PendingWork {
    provider_id: String,
    kind: Pending,
    /// When the service stops waiting (and forgets the work).
    deadline: Instant,
}

/// Work delivered to a provider and not yet acknowledged.
struct InFlight {
    work: WorkItem,
    /// When the item becomes visible for redelivery.
    visible_at: Instant,
}

struct ProviderRec {
    provider: Provider,
    seen: Instant,
    queue: VecDeque<WorkItem>,
    /// Delivered work, keyed by work id, until `Report` acknowledges it.
    in_flight: HashMap<String, InFlight>,
    wake: Arc<Notify>,
    /// Latest reported host sample, with the instant it was received for
    /// age reporting. Memory-only: a `Heartbeat` never persists (fenced
    /// liveness data is a restart source, not durable state).
    host: Option<HostSample>,
    host_sampled_at: Option<Instant>,
    /// Latest per-sandbox runtime facts, keyed by native ref. Replaced
    /// wholesale on every `Heartbeat` that reports stats (never merged),
    /// so a sandbox the driver stops reporting falls out on its own.
    sandbox_stats: HashMap<String, SandboxRuntimeStats>,
}

/// One running exec, as seen by the service.
struct Exec {
    sandbox_id: String,
    /// The `Report`-acknowledged work item that started it.
    work_id: String,
    tx: mpsc::Sender<Result<ExecEvent>>,
    /// Last `ReportExec` batch sequence applied.
    sequence: u64,
    /// Absolute deadline for the whole command.
    deadline: Instant,
    /// Fires when the service stops forwarding this exec, so the watcher
    /// that detects a dropped caller stream releases its sender.
    finished: CancellationToken,
}

/// Data-plane blob for one ticket.
struct Blob {
    sandbox_id: String,
    kind: StreamKind,
    guest_path: String,
    token: String,
    expires_at: i64,
    path: PathBuf,
}

/// Per-sandbox creation parameters kept for (re)provisioning and audit.
#[derive(Clone)]
struct PlacementSpec {
    template: Template,
    shape: Shape,
    network: NetworkPolicy,
    mounts: Vec<Mount>,
    sockets: Vec<SocketMount>,
    options: BTreeMap<String, String>,
    requires: Requirements,
    snapshot_capable: bool,
}

impl PlacementSpec {
    /// What the configured [`Placement`] ranks on: the request as resolved
    /// against the template, in capability terms.
    fn request(&self) -> PlacementRequest {
        let template = &self.template;
        let requires = &self.requires;
        let os = if requires.os.is_empty() {
            template.spec.os.clone()
        } else {
            requires.os.clone()
        };
        let driver = if requires.driver.is_empty() {
            template.driver.clone()
        } else {
            requires.driver.clone()
        };
        PlacementRequest {
            template: template.spec.name.clone(),
            driver,
            os,
            arch: requires.arch.clone(),
            features: requires.features.clone(),
            capabilities: requires.capabilities.clone(),
            snapshot_capable: self.snapshot_capable,
            network: self.network.mode,
            host_mounts: !self.mounts.is_empty(),
            socket_mounts: !self.sockets.is_empty(),
        }
    }
}

/// Check that `lease_id` is the capability for `sandbox`: it must be the
/// sandbox's active lease, and the lease must still exist. Every
/// sandbox-scoped RPC goes through this; `List` is the only read that does
/// not name a sandbox and therefore redacts lease ids instead.
fn authorize(state: &State, sandbox: &Sandbox, lease_id: &str) -> Result<()> {
    if lease_id.is_empty() {
        return Err(Error::new(
            Code::PermissionDenied,
            format!(
                "leaseId is required to use sandbox `{}`; pass the lease returned by Acquire",
                sandbox.id
            ),
        ));
    }
    let held = sandbox.lease_id == lease_id
        && state
            .leases
            .get(lease_id)
            .is_some_and(|lease| lease.sandbox_id == sandbox.id);
    if held {
        Ok(())
    } else {
        Err(Error::new(
            Code::PermissionDenied,
            format!("lease `{lease_id}` does not hold sandbox `{}`", sandbox.id),
        ))
    }
}

/// Close every open exec stream of a sandbox, so a destroyed or failed
/// sandbox cannot leave a caller streaming forever.
fn close_execs(state: &mut State, sandbox_id: &str, reason: &str) {
    let stale: Vec<String> = state
        .execs
        .iter()
        .filter(|(_, exec)| exec.sandbox_id == sandbox_id)
        .map(|(exec_id, _)| exec_id.clone())
        .collect();
    for exec_id in stale {
        if let Some(exec) = state.execs.remove(&exec_id) {
            exec.finished.cancel();
            let _ = exec.tx.try_send(Err(Error::unavailable(reason.to_owned())));
        }
    }
}

#[derive(Default)]
struct State {
    templates: BTreeMap<String, Template>,
    pools: BTreeMap<String, PoolSpec>,
    sandboxes: BTreeMap<String, Sandbox>,
    created_at: HashMap<String, Instant>,
    leases: BTreeMap<String, Lease>,
    lease_by_key: HashMap<String, String>,
    injected: HashSet<String>,
    providers: BTreeMap<String, ProviderRec>,
    pending: HashMap<String, PendingWork>,
    execs: HashMap<String, Exec>,
    blobs: HashMap<String, Blob>,
    /// Last creation failure per pool, to back off refills.
    pool_backoff: HashMap<String, Instant>,
    /// Destroy re-queued for a `Terminating` record, per sandbox.
    destroy_attempts: HashMap<String, u32>,
}

/// The service.
pub struct Service {
    state: Mutex<State>,
    changed: Notify,
    store: Arc<dyn Store>,
    config: ServiceConfig,
    /// Monotonic write order, bumped while the state lock is held.
    seq: std::sync::atomic::AtomicU64,
    /// Highest sequence already written, per record.
    persisted: tokio::sync::Mutex<HashMap<(Kind, String), u64>>,
}

/// One store write, stamped with the order it was decided in.
///
/// Ops are built while the state lock is held but written after it is
/// released, so two writers can reach the store out of order. The stamp lets
/// [`Service::persist`] drop a snapshot that a newer one already replaced.
struct Op {
    seq: u64,
    kind: Kind,
    id: String,
    value: Option<serde_json::Value>,
}

impl Service {
    /// Construct and restore from the store.
    pub async fn open(store: Arc<dyn Store>, config: ServiceConfig) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&config.blob_dir)
            .map_err(|e| Error::internal(format!("blob dir: {e}")))?;
        // Tickets are memory-only, so every file left in the blob directory
        // belongs to a previous process: an upload interrupted by shutdown or
        // a staging tar whose call never finished. Nothing can claim them.
        sweep_blob_dir(&config.blob_dir);
        let mut state = State::default();
        for template in load::<Template>(&*store, Kind::Templates).await? {
            state.templates.insert(template.spec.name.clone(), template);
        }
        for pool in load::<PoolSpec>(&*store, Kind::Pools).await? {
            state.pools.insert(pool.name.clone(), pool);
        }
        for sandbox in load::<Sandbox>(&*store, Kind::Sandboxes).await? {
            state.created_at.insert(sandbox.id.clone(), Instant::now());
            state.sandboxes.insert(sandbox.id.clone(), sandbox);
        }
        for lease in load::<Lease>(&*store, Kind::Leases).await? {
            // A preserved lease outlives its idempotency key, which release
            // cleared; only live keys map back to leases.
            if !lease.idempotency_key.is_empty() {
                state
                    .lease_by_key
                    .insert(lease.idempotency_key.clone(), lease.id.clone());
            }
            // Deliberately not marked injected: a crash between persisting the
            // lease and uploading its files would otherwise leave the caller
            // with a lease whose files never arrive. The next `Acquire` with
            // this key re-runs injection, which is idempotent.
            state.leases.insert(lease.id.clone(), lease);
        }
        for mut provider in load::<Provider>(&*store, Kind::Providers).await? {
            // Fence every session that survived the previous process: the
            // generation it holds is now stale, so its next call is Aborted
            // and the agent re-registers (with a fresh generation) instead of
            // consuming work against state this process no longer has.
            provider.generation += 1;
            state.providers.insert(
                provider.id.clone(),
                ProviderRec {
                    provider,
                    // Not placed on until it re-registers: `seen` is older
                    // than the online window, and the bumped generation
                    // fences everything it still believes.
                    seen: Instant::now() - config.provider_timeout * 2,
                    queue: VecDeque::new(),
                    in_flight: HashMap::new(),
                    wake: Arc::new(Notify::new()),
                    host: None,
                    host_sampled_at: None,
                    sandbox_stats: HashMap::new(),
                },
            );
        }
        let service = Arc::new(Self {
            state: Mutex::new(state),
            changed: Notify::new(),
            store,
            config,
            seq: std::sync::atomic::AtomicU64::new(0),
            persisted: tokio::sync::Mutex::new(HashMap::new()),
        });
        service.persist_provider_generations().await;
        service.sweep_lease_orphans().await;
        service.recover_lost_work().await;
        Ok(service)
    }

    /// Persist the generation bump every restored provider took at boot.
    ///
    /// In-memory state is authoritative (best-effort store): a failed write
    /// means the *next* restart bumps from the older number, which still
    /// fences the previous process's sessions.
    async fn persist_provider_generations(&self) {
        let ops = {
            let state = self.state.lock();
            state
                .providers
                .values()
                .map(|rec| self.put(Kind::Providers, &rec.provider.id, &rec.provider))
                .collect()
        };
        self.persist(ops).await;
    }

    /// Decide the records whose in-flight work died with the previous
    /// process, before any provider is placed on or any caller is served.
    ///
    /// Provider queues, pending work, exec streams, tickets and pool backoff
    /// are process memory, so a record caught mid-transition has nothing left
    /// to report to it:
    ///
    /// * `Provisioning` (creating) becomes `Failed`. It cannot be re-driven:
    ///   the create request — shape, network, mounts, sockets, options — is
    ///   not on the record, so a re-create would be a different sandbox. If
    ///   the machine did land, no record names it and the next reconcile
    ///   destroys it as an orphan.
    /// * `Terminating` is left `Terminating`. Its destroy is re-queued from
    ///   `Reconcile` once the provider reports the machine still exists —
    ///   that inventory is the only proof there is something to destroy.
    ///   `Reconcile` also removes the record when the machine is gone.
    ///
    /// Every other state is a settled machine: leases, pools and the
    /// reconcile inventory carry it forward.
    async fn recover_lost_work(&self) {
        let (lost, ops) = {
            let mut state = self.state.lock();
            let lost: Vec<String> = state
                .sandboxes
                .values()
                .filter(|sandbox| sandbox.state == SandboxState::Provisioning)
                .map(|sandbox| sandbox.id.clone())
                .collect();
            let mut ops = Vec::new();
            for id in &lost {
                self.fail_locked(
                    &mut state,
                    id,
                    "restarted while creating: the create was lost with the previous process",
                    &mut ops,
                );
            }
            (lost, ops)
        };
        for id in &lost {
            warn!(sandbox = %id, "restored creating sandbox failed; it cannot be re-driven");
        }
        self.persist(ops).await;
    }

    /// Fail restored sandboxes whose lease row is missing.
    ///
    /// A crash between persisting a sandbox and its lease — or a lease
    /// deleted under a live sandbox — would otherwise leave a record holding
    /// a machine forever, with no lease to expire. The dangling lease
    /// reference is cleared with the failure, so the failed sandbox is
    /// destroyed like any other and names the machine it still holds.
    async fn sweep_lease_orphans(&self) {
        let (orphans, ops) = {
            let mut state = self.state.lock();
            let orphans: Vec<String> = state
                .sandboxes
                .values()
                .filter(|sandbox| {
                    !sandbox.lease_id.is_empty() && !state.leases.contains_key(&sandbox.lease_id)
                })
                .map(|sandbox| sandbox.id.clone())
                .collect();
            let mut ops = Vec::new();
            for id in &orphans {
                state.created_at.insert(id.clone(), Instant::now());
                // The lease row is gone, so the reference must go too: a
                // stale lease id would keep the failed-unleased sweep from
                // freeing the machine the record still names.
                if let Some(sandbox) = state.sandboxes.get_mut(id) {
                    sandbox.lease_id.clear();
                }
            }
            for id in &orphans {
                self.fail_locked(&mut state, id, "restored without a lease", &mut ops);
            }
            (orphans, ops)
        };
        for id in &orphans {
            warn!(sandbox = %id, "restored sandbox has no lease; failing it for cleanup");
        }
        self.persist(ops).await;
    }

    /// Service configuration.
    pub fn config(&self) -> &ServiceConfig {
        &self.config
    }

    /// Stage a record write, stamped with the current order.
    fn put<T: Serialize>(&self, kind: Kind, id: &str, value: &T) -> Op {
        Op {
            seq: self.seq.fetch_add(1, std::sync::atomic::Ordering::AcqRel),
            kind,
            id: id.to_owned(),
            value: Some(serde_json::to_value(value).unwrap_or_default()),
        }
    }

    /// Stage a record delete, stamped with the current order.
    fn remove(&self, kind: Kind, id: &str) -> Op {
        Op {
            seq: self.seq.fetch_add(1, std::sync::atomic::Ordering::AcqRel),
            kind,
            id: id.to_owned(),
            value: None,
        }
    }

    async fn persist(&self, ops: Vec<Op>) {
        if ops.is_empty() {
            return;
        }
        // Serialized so a record's writes land in decision order; a stale
        // snapshot is dropped rather than overwriting newer truth.
        let mut persisted = self.persisted.lock().await;
        for op in ops {
            let key = (op.kind, op.id.clone());
            if persisted.get(&key).is_some_and(|seq| *seq > op.seq) {
                continue;
            }
            let result = match &op.value {
                Some(value) => self.store.put(op.kind, &op.id, value).await,
                None => self.store.delete(op.kind, &op.id).await,
            };
            match result {
                Ok(()) => {
                    persisted.insert(key, op.seq);
                }
                // Best-effort store: in-memory state stays authoritative, so
                // the RPC succeeds. The lost write is visible here, with the
                // record kind, for the restart it will cost.
                Err(cause) => error!(
                    kind = ?op.kind,
                    id = %op.id,
                    error = %cause,
                    "sandbox store write failed; the record is not durable"
                ),
            }
        }
    }

    /// Run the maintenance loop (warm pools, lease/preserve expiry,
    /// create timeouts) until `shutdown`.
    pub async fn run_maintenance(self: Arc<Self>, shutdown: tokio_util::sync::CancellationToken) {
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tick.tick() => self.maintain().await,
            }
        }
    }

    /// One maintenance pass (exposed for tests).
    pub async fn maintain(&self) {
        let ops = {
            let mut state = self.state.lock();
            let mut ops = Vec::new();
            let t = now();
            // Expired leases and preservation windows.
            let expired: Vec<String> = state
                .leases
                .values()
                .filter(|lease| lease.expires_at <= t)
                .map(|lease| lease.id.clone())
                .collect();
            for lease_id in expired {
                info!(%lease_id, "lease expired; destroying sandbox");
                self.release_locked(&mut state, &lease_id, ReleaseMode::Destroy, 0, &mut ops);
                self.config.metrics.record_lease_transition("expired");
            }
            // A stopped sandbox holds no host capacity, but it does hold
            // disk: the create sweep below still applies to its record.
            let preserved: Vec<String> = state
                .sandboxes
                .values()
                .filter(|s| s.state == SandboxState::Preserved && s.preserve_until <= t)
                .map(|s| s.id.clone())
                .collect();
            for id in preserved {
                self.destroy_locked(&mut state, &id, &mut ops);
            }
            // Creates that never reported.
            let timed_out: Vec<String> = state
                .sandboxes
                .values()
                .filter(|s| {
                    s.state == SandboxState::Provisioning
                        && state
                            .created_at
                            .get(&s.id)
                            .is_some_and(|at| at.elapsed() > self.config.create_timeout)
                })
                .map(|s| s.id.clone())
                .collect();
            for id in timed_out {
                self.fail_locked(&mut state, &id, "create timed out", &mut ops);
                self.config.metrics.record_create_failure("timeout");
            }
            // Execs past their deadline: the driver should have killed the
            // command already; this is the backstop for a lost provider. The
            // caller gets an error and the guest command is cancelled.
            let over_deadline: Vec<String> = state
                .execs
                .iter()
                .filter(|(_, exec)| exec.deadline + EXEC_SWEEP_GRACE <= Instant::now())
                .map(|(exec_id, _)| exec_id.clone())
                .collect();
            for exec_id in over_deadline {
                self.fail_exec_locked(
                    &mut state,
                    &exec_id,
                    Error::new(Code::DeadlineExceeded, "exec timed out"),
                );
            }
            // Work nobody will ever acknowledge: fail whatever still waits
            // on it so nothing leaks when a provider loses a report.
            let stale_work: Vec<String> = state
                .pending
                .iter()
                .filter(|(_, work)| work.deadline <= Instant::now())
                .map(|(work_id, _)| work_id.clone())
                .collect();
            for work_id in stale_work {
                self.expire_pending_locked(&mut state, &work_id, &mut ops);
            }
            // Expired tickets.
            let stale: Vec<String> = state
                .blobs
                .iter()
                .filter(|(_, blob)| blob.expires_at <= t)
                .map(|(ticket, _)| ticket.clone())
                .collect();
            for ticket in stale {
                if let Some(blob) = state.blobs.remove(&ticket) {
                    let _ = std::fs::remove_file(&blob.path);
                }
            }
            // Sandboxes on a provider that has gone silent: nothing will ever
            // report for them, so fail them rather than leave leases and exec
            // streams open forever.
            let silent: Vec<String> = state
                .providers
                .iter()
                .filter(|(_, rec)| {
                    rec.seen.elapsed()
                        > self.config.provider_timeout * self.config.provider_lost_factor
                })
                .map(|(provider_id, _)| provider_id.clone())
                .collect();
            let stranded_sandboxes: Vec<String> = state
                .sandboxes
                .values()
                .filter(|s| silent.contains(&s.provider_id))
                .map(|s| s.id.clone())
                .collect();
            // A lease on a host that stopped answering can never be honoured
            // again — the machine is out of reach, not idle. Release it (the
            // caller's next `Renew` reports not_found and its next `Acquire`
            // with the same key starts over) and then fail the sandbox.
            for id in &stranded_sandboxes {
                self.detach_lease_locked(&mut state, id, &mut ops);
            }
            let orphaned: Vec<String> = stranded_sandboxes
                .into_iter()
                .filter(|id| {
                    state
                        .sandboxes
                        .get(id)
                        .is_some_and(|s| !s.state.is_terminal())
                })
                .collect();
            for id in orphaned {
                self.fail_locked(&mut state, &id, "provider lost (no heartbeat)", &mut ops);
            }
            // Its in-flight work will never report: release the waiters
            // instead of leaking them until the process exits.
            let stranded: Vec<String> = state
                .pending
                .iter()
                .filter(|(_, work)| silent.contains(&work.provider_id))
                .map(|(work_id, _)| work_id.clone())
                .collect();
            for work_id in stranded {
                self.complete_locked(
                    &mut state,
                    &work_id,
                    Err(Error::unavailable("provider lost (no heartbeat)")),
                    &mut ops,
                );
            }
            // Failed, unleased sandboxes: free whatever the host still holds.
            let failed: Vec<String> = state
                .sandboxes
                .values()
                .filter(|s| s.state == SandboxState::Failed && s.lease_id.is_empty())
                .map(|s| s.id.clone())
                .collect();
            for id in failed {
                self.destroy_locked(&mut state, &id, &mut ops);
            }
            // Warm pools.
            let pools: Vec<PoolSpec> = state.pools.values().cloned().collect();
            for pool in pools {
                self.fill_pool_locked(&mut state, &pool, &mut ops);
            }
            // Live sandboxes by state/provider/template: every combination
            // present this tick, and a zero for every combination that was
            // live last tick but is gone now, so a drained pool falls to 0
            // instead of keeping its last value.
            let mut counts: HashMap<(&'static str, String, String), u64> = HashMap::new();
            for sandbox in state.sandboxes.values() {
                *counts
                    .entry((
                        sandbox.state.as_metric_label(),
                        sandbox.provider_id.clone(),
                        sandbox.template.clone(),
                    ))
                    .or_insert(0) += 1;
            }
            self.config
                .metrics
                .record_sandbox_counts(counts.into_iter().map(
                    |((sandbox_state, provider, template), count)| {
                        (sandbox_state.to_owned(), provider, template, count)
                    },
                ));
            ops
        };
        if !ops.is_empty() {
            self.changed.notify_waiters();
        }
        self.persist(ops).await;
    }

    fn fill_pool_locked(&self, state: &mut State, pool: &PoolSpec, ops: &mut Vec<Op>) {
        if state
            .pool_backoff
            .get(&pool.name)
            .is_some_and(|at| at.elapsed() < POOL_FAILURE_BACKOFF)
        {
            return;
        }
        let members: Vec<&Sandbox> = state
            .sandboxes
            .values()
            .filter(|s| s.pool == pool.name && !s.state.is_terminal())
            .collect();
        let idle = members
            .iter()
            .filter(|s| {
                s.lease_id.is_empty()
                    && matches!(s.state, SandboxState::Ready | SandboxState::Provisioning)
            })
            .count() as u32;
        self.config
            .metrics
            .record_pool_warm_depth(&pool.name, idle as u64);
        let live = members
            .iter()
            .filter(|s| s.state != SandboxState::Terminating)
            .count() as u32;
        let mut missing = pool.warm.saturating_sub(idle);
        if pool.max > 0 {
            missing = missing.min(pool.max.saturating_sub(live));
        }
        for _ in 0..missing {
            let Ok(placement) = self.pool_placement(state, pool, &Requirements::default()) else {
                self.config.metrics.record_pool_refill_failure(&pool.name);
                return;
            };
            match self.create_locked(state, placement, &pool.name, Purpose::Runner, "", ops) {
                Ok(_) => {}
                // No capacity right now: try again next pass.
                Err(_) => {
                    self.config.metrics.record_pool_refill_failure(&pool.name);
                    return;
                }
            }
        }
    }

    // ─── Templates and pools ───────────────────────────────────────

    /// `PutTemplate`.
    pub async fn put_template(&self, spec: TemplateSpec) -> Result<Template> {
        validate_name("template", &spec.name)?;
        let sources = [
            !spec.artifacts.is_empty(),
            spec.recipe.is_some(),
            spec.snapshot_of.is_some(),
        ];
        if sources.iter().filter(|set| **set).count() != 1 {
            return Err(Error::invalid(
                "exactly one template source is required: artifacts, recipe, or snapshotOf",
            ));
        }
        if spec.snapshot_of.is_some() {
            return Err(Error::invalid("snapshot templates are created by Snapshot"));
        }
        let mut seen = std::collections::BTreeSet::new();
        for artifact in &spec.artifacts {
            artifact.validate().map_err(Error::invalid)?;
            if !seen.insert((artifact.format.as_str(), artifact.arch.as_str())) {
                return Err(Error::invalid(format!(
                    "artifact for format `{}` and arch `{}` is declared twice",
                    artifact.format, artifact.arch
                )));
            }
        }
        let template = Template {
            digest: template_digest(&spec),
            created_at: now(),
            spec,
            ..Template::default()
        };
        let ops = {
            let mut state = self.state.lock();
            if let Some(existing) = state.templates.get(&template.spec.name)
                && existing.digest == template.digest
            {
                return Ok(existing.clone());
            }
            state
                .templates
                .insert(template.spec.name.clone(), template.clone());
            vec![self.put(Kind::Templates, &template.spec.name, &template)]
        };
        self.persist(ops).await;
        Ok(template)
    }

    /// `DeleteTemplate`: snapshot templates also release their native
    /// snapshot. Refused while a pool still references the template.
    pub async fn delete_template(&self, name: &str) -> Result<()> {
        let ops = {
            let mut state = self.state.lock();
            if let Some(pool) = state.pools.values().find(|p| p.template == name) {
                return Err(Error::precondition(format!(
                    "template `{name}` is used by pool `{}`",
                    pool.name
                )));
            }
            let template = state
                .templates
                .remove(name)
                .ok_or_else(|| Error::not_found(format!("template `{name}` not found")))?;
            if !template.native_ref.is_empty() {
                self.drop_snapshot_locked(&mut state, &template);
            }
            vec![self.remove(Kind::Templates, name)]
        };
        self.persist(ops).await;
        Ok(())
    }

    /// `ListTemplates`.
    pub fn list_templates(&self) -> Vec<Template> {
        self.state.lock().templates.values().cloned().collect()
    }

    /// `ListTemplates` page, ordered by name.
    pub fn list_templates_page(&self, request: &ListRequest) -> Result<ListTemplatesResponse> {
        let state = self.state.lock();
        let (templates, next_page_token) = paginate(
            state
                .templates
                .iter()
                .map(|(name, template)| (name.clone(), template.clone())),
            request,
        )?;
        Ok(ListTemplatesResponse {
            templates,
            next_page_token,
        })
    }

    /// `PutPool`.
    pub async fn put_pool(&self, pool: PoolSpec) -> Result<PoolSpec> {
        validate_name("pool", &pool.name)?;
        let ops = {
            let mut state = self.state.lock();
            if !state.templates.contains_key(&pool.template) {
                return Err(Error::not_found(format!(
                    "template `{}` not found",
                    pool.template
                )));
            }
            // Surface impossible pools at configuration time.
            self.pool_placement_check(&state, &pool)?;
            state.pools.insert(pool.name.clone(), pool.clone());
            vec![self.put(Kind::Pools, &pool.name, &pool)]
        };
        self.persist(ops).await;
        self.changed.notify_waiters();
        Ok(pool)
    }

    /// `ListPools`.
    pub fn list_pools(&self) -> Vec<PoolSpec> {
        self.state.lock().pools.values().cloned().collect()
    }

    /// `ListPools` page, ordered by name.
    pub fn list_pools_page(&self, request: &ListRequest) -> Result<ListPoolsResponse> {
        let state = self.state.lock();
        let (pools, next_page_token) = paginate(
            state
                .pools
                .iter()
                .map(|(name, pool)| (name.clone(), pool.clone())),
            request,
        )?;
        Ok(ListPoolsResponse {
            pools,
            next_page_token,
        })
    }

    /// `DeletePool`: idle members are destroyed; leased ones finish.
    pub async fn delete_pool(&self, name: &str) -> Result<()> {
        let ops = {
            let mut state = self.state.lock();
            if state.pools.remove(name).is_none() {
                return Err(Error::not_found(format!("pool `{name}` not found")));
            }
            let mut ops = vec![self.remove(Kind::Pools, name)];
            let idle: Vec<String> = state
                .sandboxes
                .values()
                .filter(|s| s.pool == name && s.lease_id.is_empty() && !s.state.is_terminal())
                .map(|s| s.id.clone())
                .collect();
            for id in idle {
                self.destroy_locked(&mut state, &id, &mut ops);
            }
            ops
        };
        self.persist(ops).await;
        Ok(())
    }

    // ─── Placement ─────────────────────────────────────────────────

    fn pool_placement(
        &self,
        state: &State,
        pool: &PoolSpec,
        extra: &Requirements,
    ) -> Result<(PlacementSpec, String, String)> {
        let template = state
            .templates
            .get(&pool.template)
            .cloned()
            .ok_or_else(|| Error::not_found(format!("template `{}` not found", pool.template)))?;
        let placement = PlacementSpec {
            requires: template.spec.requires.merged(&pool.requires).merged(extra),
            template,
            shape: pool.shape,
            network: pool.network.clone(),
            mounts: pool.mounts.clone(),
            sockets: pool.sockets.clone(),
            options: pool.options.clone(),
            snapshot_capable: false,
        };
        let (provider, driver) = self.place(state, &placement, true)?;
        Ok((placement, provider, driver))
    }

    fn pool_placement_check(&self, state: &State, pool: &PoolSpec) -> Result<()> {
        match self.pool_placement(state, pool, &Requirements::default()) {
            Ok(_) => Ok(()),
            // Capacity or offline providers are transient; only a pool no
            // registered provider could ever serve is rejected.
            Err(error) if error.code == Code::FailedPrecondition => {
                let template = &state.templates[&pool.template];
                let placement = PlacementSpec {
                    requires: template.spec.requires.merged(&pool.requires),
                    template: template.clone(),
                    shape: pool.shape,
                    network: pool.network.clone(),
                    mounts: pool.mounts.clone(),
                    sockets: pool.sockets.clone(),
                    options: pool.options.clone(),
                    snapshot_capable: false,
                };
                if state.providers.is_empty() {
                    return Ok(());
                }
                self.place(state, &placement, false).map(|_| ())
            }
            Err(_) => Ok(()),
        }
    }

    /// Whether a provider's declared per-sandbox capacity can host `shape`.
    ///
    /// Zero on either side means "not reported" (or "driver default"), never
    /// zero capacity: a driver that publishes no ceiling still accepts the
    /// request rather than being rejected by placement.
    fn fits_shape(caps: &Capabilities, shape: &Shape) -> bool {
        let cpus_fit = caps.max_vcpus == 0 || shape.cpus == 0 || shape.cpus <= caps.max_vcpus;
        let memory_fit = caps.max_memory_mib == 0
            || shape.memory_mib == 0
            || shape.memory_mib <= caps.max_memory_mib;
        cpus_fit && memory_fit
    }

    /// Pick `(provider_id, driver)`: filter on capabilities and capacity,
    /// then let the configured [`Placement`] rank the survivors.
    ///
    /// The filter is the service's: offline providers (and, for snapshot
    /// templates, providers that do not hold the snapshot), providers whose
    /// drivers miss a required capability, and pairs at their slot cap never
    /// reach the placement.
    fn place(
        &self,
        state: &State,
        spec: &PlacementSpec,
        require_online: bool,
    ) -> Result<(String, String)> {
        let template = &spec.template;
        let requires = &spec.requires;
        let os = if requires.os.is_empty() {
            template.spec.os.as_str()
        } else {
            requires.os.as_str()
        };
        let mut unmet = Vec::new();
        let mut full = false;
        let mut candidates: Vec<Candidate> = Vec::new();
        for (provider_id, rec) in &state.providers {
            if rec.provider.cordoned {
                continue;
            }
            if require_online && rec.seen.elapsed() > self.config.provider_timeout {
                continue;
            }
            // Snapshot templates start only where the snapshot lives.
            if !template.native_ref.is_empty() && template.provider_id != *provider_id {
                continue;
            }
            for driver in &rec.provider.drivers {
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
                if template.spec.recipe.is_some() && template.native_ref.is_empty() {
                    // Recipes are driver-specific; the template names its driver
                    // through `requires.driver`.
                    if requires.driver.is_empty() && template.spec.requires.driver.is_empty() {
                        reasons.push("recipe templates must set requires.driver".into());
                    }
                }
                if !reasons.is_empty() {
                    unmet.push(format!(
                        "{}/{}: {}",
                        rec.provider.name,
                        driver.driver,
                        reasons.join(", ")
                    ));
                    continue;
                }
                if !Self::fits_shape(caps, &spec.shape) {
                    unmet.push(format!(
                        "{}/{}: shape={}cpus/{}mib (max {}cpus/{}mib)",
                        rec.provider.name,
                        driver.driver,
                        spec.shape.cpus,
                        spec.shape.memory_mib,
                        caps.max_vcpus,
                        caps.max_memory_mib
                    ));
                    continue;
                }
                let live = state
                    .sandboxes
                    .values()
                    .filter(|s| {
                        s.provider_id == *provider_id
                            && s.driver == driver.driver
                            && !s.state.is_terminal()
                    })
                    .count();
                if caps.slots > 0 && live >= caps.slots as usize {
                    full = true;
                    continue;
                }
                candidates.push(Candidate {
                    id: CandidateId(candidates.len()),
                    provider_id: provider_id.clone(),
                    provider_name: rec.provider.name.clone(),
                    driver: driver.driver.clone(),
                    live,
                    slots: caps.slots,
                });
            }
        }
        if candidates.is_empty() {
            if full {
                return Err(Error::new(
                    Code::ResourceExhausted,
                    "every matching provider is at capacity",
                ));
            }
            if unmet.is_empty() {
                return Err(Error::unavailable(
                    "no provider is online for this template",
                ));
            }
            return Err(Error::precondition(format!(
                "no provider satisfies template `{}`: {}",
                template.spec.name,
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

    fn create_locked(
        &self,
        state: &mut State,
        (placement, provider_id, driver): (PlacementSpec, String, String),
        pool: &str,
        purpose: Purpose,
        name: &str,
        ops: &mut Vec<Op>,
    ) -> Result<String> {
        let id = new_id("sbx");
        let sandbox = Sandbox {
            id: id.clone(),
            name: name.to_owned(),
            template: placement.template.spec.name.clone(),
            pool: pool.to_owned(),
            provider_id: provider_id.clone(),
            driver: driver.clone(),
            state: SandboxState::Provisioning,
            purpose,
            created_at: now(),
            ..Sandbox::default()
        };
        let sandbox_driver = driver.clone();
        let work = WorkItem {
            id: new_id("wrk"),
            create: Some(CreateWork {
                sandbox_id: id.clone(),
                name: name.to_owned(),
                driver,
                template: placement.template,
                shape: placement.shape,
                network: placement.network,
                mounts: placement.mounts,
                sockets: placement.sockets,
                options: placement.options,
                snapshot_capable: placement.snapshot_capable,
            }),
            ..WorkItem::default()
        };
        self.enqueue(
            state,
            &provider_id,
            work,
            Pending::Create {
                sandbox_id: id.clone(),
                driver: sandbox_driver,
            },
        )?;
        ops.push(self.put(Kind::Sandboxes, &id, &sandbox));
        state.created_at.insert(id.clone(), Instant::now());
        state.sandboxes.insert(id.clone(), sandbox);
        Ok(id)
    }

    fn enqueue(
        &self,
        state: &mut State,
        provider_id: &str,
        work: WorkItem,
        kind: Pending,
    ) -> Result<()> {
        let deadline = self.pending_deadline(state, &kind);
        let work_id = work.id.clone();
        state.pending.insert(
            work_id.clone(),
            PendingWork {
                provider_id: provider_id.to_owned(),
                kind,
                deadline,
            },
        );
        if let Err(error) = self.enqueue_work(state, provider_id, work) {
            // The queue never got it; do not leave a waiter behind.
            state.pending.remove(&work_id);
            return Err(error);
        }
        Ok(())
    }

    /// Queue work without tracking a report (cancellations are fire-and-forget).
    fn enqueue_work(&self, state: &mut State, provider_id: &str, work: WorkItem) -> Result<()> {
        let rec = state
            .providers
            .get_mut(provider_id)
            .ok_or_else(|| Error::unavailable(format!("provider {provider_id} is gone")))?;
        rec.queue.push_back(work);
        rec.wake.notify_one();
        Ok(())
    }

    /// When the service stops waiting for a work item.
    fn pending_deadline(&self, state: &State, kind: &Pending) -> Instant {
        match kind {
            Pending::Create { .. } => Instant::now() + self.config.create_timeout,
            Pending::Exec { exec_id } => state.execs.get(exec_id).map_or_else(
                || Instant::now() + self.config.exec_timeout + EXEC_SWEEP_GRACE,
                |exec| exec.deadline + EXEC_SWEEP_GRACE,
            ),
            _ => Instant::now() + self.config.work_timeout,
        }
    }

    /// Fail a work item whose deadline passed, naming the operation.
    fn expire_pending_locked(&self, state: &mut State, work_id: &str, ops: &mut Vec<Op>) {
        let message = match state.pending.get(work_id).map(|work| &work.kind) {
            Some(Pending::Create { .. }) => "create timed out",
            Some(Pending::Exec { .. }) => "exec timed out",
            _ => "work timed out",
        };
        let error = Error::new(Code::DeadlineExceeded, message);
        self.complete_locked(state, work_id, Err(error), ops);
    }

    /// Drop an exec's work from the queue and its in-flight set: nothing will
    /// start (or restart) the command.
    fn forget_work_locked(state: &mut State, provider_id: &str, work_id: &str) {
        state.pending.remove(work_id);
        if let Some(rec) = state.providers.get_mut(provider_id) {
            rec.in_flight.remove(work_id);
            rec.queue.retain(|item| item.id != work_id);
        }
    }

    /// Stop forwarding an exec: end the caller's stream with `error`, forget
    /// its work, and ask the provider to kill the guest command.
    fn fail_exec_locked(&self, state: &mut State, exec_id: &str, error: Error) {
        let Some(exec) = state.execs.remove(exec_id) else {
            return;
        };
        exec.finished.cancel();
        let _ = exec.tx.try_send(Err(error));
        let Some(sandbox) = state.sandboxes.get(&exec.sandbox_id).cloned() else {
            if let Some(pending) = state.pending.get(&exec.work_id) {
                let provider_id = pending.provider_id.clone();
                Self::forget_work_locked(state, &provider_id, &exec.work_id);
            }
            return;
        };
        Self::forget_work_locked(state, &sandbox.provider_id, &exec.work_id);
        self.request_cancel_locked(state, &sandbox, exec_id);
    }

    /// Queue a `CancelExec` for a sandbox that still holds the command.
    fn request_cancel_locked(&self, state: &mut State, sandbox: &Sandbox, exec_id: &str) {
        if sandbox.state.is_terminal() || sandbox.native_ref.is_empty() {
            return;
        }
        let work = WorkItem {
            id: new_id("wrk"),
            cancel_exec: Some(CancelExecWork {
                exec_id: exec_id.to_owned(),
                target: Self::target(sandbox),
            }),
            ..WorkItem::default()
        };
        let _ = self.enqueue_work(state, &sandbox.provider_id, work);
    }

    fn target(sandbox: &Sandbox) -> SandboxWork {
        SandboxWork {
            sandbox_id: sandbox.id.clone(),
            driver: sandbox.driver.clone(),
            native_ref: sandbox.native_ref.clone(),
        }
    }

    fn destroy_locked(&self, state: &mut State, id: &str, ops: &mut Vec<Op>) {
        match state.sandboxes.get(id).map(|sandbox| sandbox.state) {
            None => return,
            Some(SandboxState::Gone | SandboxState::Terminating) => return,
            Some(_) => {}
        }
        close_execs(state, id, "sandbox was destroyed");
        let sandbox = state.sandboxes.get_mut(id).expect("state checked above");
        if sandbox.native_ref.is_empty() {
            // Never created (or still provisioning): drop the record; a late
            // create report finds no record and the reconcile removes the
            // orphan.
            sandbox.state = SandboxState::Gone;
            let id = id.to_owned();
            state.sandboxes.remove(&id);
            state.created_at.remove(&id);
            state.destroy_attempts.remove(&id);
            ops.push(self.remove(Kind::Sandboxes, &id));
            return;
        }
        sandbox.state = SandboxState::Terminating;
        let snapshot = sandbox.clone();
        ops.push(self.put(Kind::Sandboxes, id, &snapshot));
        let work = WorkItem {
            id: new_id("wrk"),
            destroy: Some(Self::target(&snapshot)),
            ..WorkItem::default()
        };
        if self
            .enqueue(
                state,
                &snapshot.provider_id,
                work,
                Pending::Destroy {
                    sandbox_id: id.to_owned(),
                },
            )
            .is_err()
        {
            // Provider gone: reconcile on re-registration destroys it.
            state.sandboxes.remove(id);
            state.created_at.remove(id);
            state.destroy_attempts.remove(id);
            ops.push(self.remove(Kind::Sandboxes, id));
        }
    }

    /// Ask the provider to destroy a `Terminating` record's machine again,
    /// bounding how many times a reconcile can do so. Returns `false` when the
    /// retries are spent: the caller drops the record so the machine becomes
    /// an orphan the same reconcile destroys.
    fn requeue_destroy_locked(&self, state: &mut State, id: &str) -> bool {
        let attempts = state.destroy_attempts.get(id).copied().unwrap_or(0);
        if attempts >= MAX_DESTROY_ATTEMPTS {
            return false;
        }
        let Some(sandbox) = state.sandboxes.get(id).cloned() else {
            return false;
        };
        let work = WorkItem {
            id: new_id("wrk"),
            destroy: Some(Self::target(&sandbox)),
            ..WorkItem::default()
        };
        let queued = self
            .enqueue(
                state,
                &sandbox.provider_id,
                work,
                Pending::Destroy {
                    sandbox_id: id.to_owned(),
                },
            )
            .is_ok();
        if queued {
            state.destroy_attempts.insert(id.to_owned(), attempts + 1);
            warn!(
                sandbox = id,
                attempt = attempts + 1,
                "terminating sandbox is still on the provider; destroy re-queued"
            );
        }
        queued
    }

    fn fail_locked(&self, state: &mut State, id: &str, reason: &str, ops: &mut Vec<Op>) {
        close_execs(state, id, reason);
        if let Some(sandbox) = state.sandboxes.get_mut(id) {
            warn!(sandbox = id, reason, "sandbox failed");
            sandbox.state = SandboxState::Failed;
            sandbox.error = reason.to_owned();
            ops.push(self.put(Kind::Sandboxes, id, &*sandbox));
            if !sandbox.pool.is_empty() {
                state
                    .pool_backoff
                    .insert(sandbox.pool.clone(), Instant::now());
            }
        }
        state.created_at.remove(id);
    }

    /// Release a sandbox's lease without touching the machine.
    ///
    /// Used when the lease can no longer be honoured (its provider is lost)
    /// but the machine may still exist for the next reconcile: the lease row
    /// goes, the sandbox keeps its state and native reference.
    fn detach_lease_locked(&self, state: &mut State, sandbox_id: &str, ops: &mut Vec<Op>) {
        let lease_id = match state.sandboxes.get(sandbox_id) {
            Some(sandbox) if !sandbox.lease_id.is_empty() => sandbox.lease_id.clone(),
            _ => return,
        };
        if let Some(lease) = state.leases.remove(&lease_id) {
            state.lease_by_key.remove(&lease.idempotency_key);
            state.injected.remove(&lease_id);
            ops.push(self.remove(Kind::Leases, &lease_id));
        }
        if let Some(sandbox) = state.sandboxes.get_mut(sandbox_id) {
            sandbox.lease_id.clear();
            ops.push(self.put(Kind::Sandboxes, sandbox_id, &*sandbox));
        }
    }

    fn release_locked(
        &self,
        state: &mut State,
        lease_id: &str,
        mode: ReleaseMode,
        preserve_ttl: u64,
        ops: &mut Vec<Op>,
    ) {
        let Some(lease) = state.leases.get(lease_id).cloned() else {
            return;
        };
        let preserve = mode == ReleaseMode::Preserve;
        let failed = state
            .sandboxes
            .get(&lease.sandbox_id)
            .is_some_and(|s| s.state == SandboxState::Failed);
        // Preservation is the one way a lease outlives `Release`: the id keeps
        // working as the debugging capability until the window closes. A
        // second preserve extends that window instead of destroying.
        if preserve
            && !failed
            && state
                .sandboxes
                .get(&lease.sandbox_id)
                .is_some_and(|s| matches!(s.state, SandboxState::Leased | SandboxState::Preserved))
        {
            let ttl = if preserve_ttl == 0 {
                self.config.preserve_ttl.as_secs()
            } else {
                preserve_ttl
            };
            let until = now() + ttl as i64;
            if let Some(sandbox) = state.sandboxes.get_mut(&lease.sandbox_id) {
                sandbox.state = SandboxState::Preserved;
                sandbox.preserve_until = until;
                ops.push(self.put(Kind::Sandboxes, &lease.sandbox_id, &*sandbox));
            }
            // The idempotency key is free again (also after a restart): a
            // retry with it starts a new sandbox, as it did before
            // preservation kept the lease alive.
            state.lease_by_key.remove(&lease.idempotency_key);
            if let Some(lease) = state.leases.get_mut(lease_id) {
                lease.expires_at = until;
                lease.idempotency_key.clear();
                ops.push(self.put(Kind::Leases, lease_id, &*lease));
            }
            self.config.metrics.record_lease_transition("preserved");
            return;
        }
        state.leases.remove(lease_id);
        state.lease_by_key.remove(&lease.idempotency_key);
        state.injected.remove(lease_id);
        ops.push(self.remove(Kind::Leases, lease_id));
        if let Some(sandbox) = state.sandboxes.get_mut(&lease.sandbox_id) {
            sandbox.lease_id.clear();

        }
        // Failed sandboxes may still hold a native machine: destroy_locked
        // handles both cases.
        self.destroy_locked(state, &lease.sandbox_id, ops);
    }

    // ─── Leases ────────────────────────────────────────────────────

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
            return Err(Error::invalid("idempotencyKey is required"));
        }
        if request.pool.is_empty() == request.template.is_empty() {
            return Err(Error::invalid(
                "exactly one of pool or template is required",
            ));
        }
        let wait = match request.wait_secs {
            None => self.config.acquire_wait,
            // An explicit zero means "report the current state, do not block".
            Some(0) => Duration::ZERO,
            Some(secs) => Duration::from_secs(secs),
        };
        let deadline = tokio::time::Instant::now() + wait;
        // Hashed once: the request cannot change across a wait loop.
        let fingerprint = acquire_fingerprint(&request);
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.try_acquire(&request, &fingerprint).await? {
                Step::Ready(ready) => {
                    let (lease, sandbox) = *ready;
                    self.inject(&lease, &sandbox, &request.inject).await?;
                    return Ok(AcquireResponse { lease, sandbox });
                }
                Step::Wait(last) => {
                    if tokio::time::timeout_at(deadline, changed).await.is_err() {
                        return Err(last.unwrap_or_else(|| {
                            Error::new(Code::DeadlineExceeded, "sandbox not ready in time")
                        }));
                    }
                }
            }
        }
    }

    async fn try_acquire(&self, request: &AcquireRequest, fingerprint: &str) -> Result<Step> {
        let mut ops = Vec::new();
        let step = {
            let mut state = self.state.lock();
            self.try_acquire_locked(&mut state, request, fingerprint, &mut ops)?
        };
        if !ops.is_empty() {
            self.persist(ops).await;
            self.changed.notify_waiters();
        }
        Ok(step)
    }

    fn try_acquire_locked(
        &self,
        state: &mut State,
        request: &AcquireRequest,
        fingerprint: &str,
        ops: &mut Vec<Op>,
    ) -> Result<Step> {
        if let Some(lease_id) = state.lease_by_key.get(&request.idempotency_key).cloned() {
            let stored = state.leases[&lease_id].request_hash.as_str();
            if !stored.is_empty() && stored != fingerprint {
                return Err(Error::invalid(format!(
                    "idempotency key `{}` was used for a different acquire request",
                    request.idempotency_key
                )));
            }
            if stored.is_empty() {
                // Leases persisted before fingerprints existed: record the
                // first payload seen, so a later mismatch is still caught.
                if let Some(lease) = state.leases.get_mut(&lease_id) {
                    lease.request_hash = fingerprint.to_owned();
                    ops.push(self.put(Kind::Leases, &lease_id, &*lease));
                }
            }
            let lease = state.leases[&lease_id].clone();
            let sandbox = state
                .sandboxes
                .get(&lease.sandbox_id)
                .cloned()
                .ok_or_else(|| Error::not_found("leased sandbox is gone"))?;
            return match sandbox.state {
                SandboxState::Leased | SandboxState::Paused | SandboxState::Preserved => {
                    Ok(Step::Ready(Box::new((lease, sandbox))))
                }
                SandboxState::Provisioning => Ok(Step::Wait(None)),
                _ => {
                    // Failed/gone: release so a retry with the same key can
                    // start over, and report the failure now.
                    let error = if sandbox.error.is_empty() {
                        format!("sandbox {} is {:?}", sandbox.id, sandbox.state)
                    } else {
                        sandbox.error.clone()
                    };
                    self.release_locked(state, &lease_id, ReleaseMode::Destroy, 0, ops);
                    Err(Error::unavailable(error))
                }
            };
        }
        let ttl = if request.ttl_secs > 0 {
            request.ttl_secs
        } else {
            request
                .pool
                .is_empty()
                .then_some(0)
                .or_else(|| state.pools.get(&request.pool).map(|p| p.lease_ttl_secs))
                .filter(|ttl| *ttl > 0)
                .unwrap_or(self.config.lease_ttl.as_secs())
        };
        if !request.name.is_empty()
            && state
                .sandboxes
                .values()
                .any(|s| s.name == request.name && !s.state.is_terminal())
        {
            return Err(Error::new(
                Code::AlreadyExists,
                format!("a live sandbox is already named `{}`", request.name),
            ));
        }
        // Resolve the sandbox: a warm pool member, or a new one.
        let sandbox_id = if !request.pool.is_empty() {
            let pool =
                state.pools.get(&request.pool).cloned().ok_or_else(|| {
                    Error::not_found(format!("pool `{}` not found", request.pool))
                })?;
            let warm = state
                .sandboxes
                .values()
                .filter(|s| {
                    s.pool == pool.name && s.lease_id.is_empty() && s.state == SandboxState::Ready
                })
                .filter(|s| sandbox_meets(state, s, &request.requires))
                .map(|s| s.id.clone())
                .next();
            match warm {
                Some(id) => id,
                None => {
                    let live = state
                        .sandboxes
                        .values()
                        .filter(|s| s.pool == pool.name && !s.state.is_terminal())
                        .count() as u32;
                    // An idle provisioning member will become ready; do not
                    // over-create while it boots.
                    let booting = state.sandboxes.values().any(|s| {
                        s.pool == pool.name
                            && s.lease_id.is_empty()
                            && s.state == SandboxState::Provisioning
                            && sandbox_meets(state, s, &request.requires)
                    });
                    if booting {
                        return Ok(Step::Wait(None));
                    }
                    if pool.max > 0 && live >= pool.max {
                        return Ok(Step::Wait(Some(Error::new(
                            Code::ResourceExhausted,
                            format!("pool `{}` is at max ({})", pool.name, pool.max),
                        ))));
                    }
                    match self.pool_placement(state, &pool, &request.requires) {
                        Ok(placement) => self.create_locked(
                            state,
                            placement,
                            &pool.name,
                            request.purpose,
                            "",
                            ops,
                        )?,
                        Err(error) if error.code == Code::FailedPrecondition => return Err(error),
                        Err(error) => return Ok(Step::Wait(Some(error))),
                    }
                }
            }
        } else {
            let template = state
                .templates
                .get(&request.template)
                .cloned()
                .ok_or_else(|| {
                    Error::not_found(format!("template `{}` not found", request.template))
                })?;
            let placement = PlacementSpec {
                requires: template.spec.requires.merged(&request.requires),
                template,
                shape: request.shape,
                network: request.network.clone(),
                mounts: request.mounts.clone(),
                sockets: request.sockets.clone(),
                options: request.options.clone(),
                snapshot_capable: request.purpose == Purpose::Build,
            };
            match self.place(state, &placement, true) {
                Ok((provider, driver)) => self.create_locked(
                    state,
                    (placement, provider, driver),
                    "",
                    request.purpose,
                    &request.name,
                    ops,
                )?,
                Err(error) if error.code == Code::FailedPrecondition => return Err(error),
                Err(error) => return Ok(Step::Wait(Some(error))),
            }
        };
        let lease = Lease {
            id: new_id("lse"),
            sandbox_id: sandbox_id.clone(),
            pool: request.pool.clone(),
            idempotency_key: request.idempotency_key.clone(),
            expires_at: now() + ttl as i64,
            env: request.inject.env.clone(),
            request_hash: fingerprint.to_owned(),
        };
        let sandbox = state
            .sandboxes
            .get_mut(&sandbox_id)
            .ok_or_else(|| Error::internal("sandbox vanished"))?;
        sandbox.lease_id = lease.id.clone();
        sandbox.purpose = request.purpose;
        sandbox.name = request.name.clone();
        if sandbox.state == SandboxState::Ready {
            sandbox.state = SandboxState::Leased;
        }
        let ready = sandbox.state == SandboxState::Leased;
        let snapshot = sandbox.clone();
        ops.push(self.put(Kind::Sandboxes, &sandbox_id, &snapshot));
        ops.push(self.put(Kind::Leases, &lease.id, &lease));
        state
            .lease_by_key
            .insert(lease.idempotency_key.clone(), lease.id.clone());
        state.leases.insert(lease.id.clone(), lease.clone());
        Ok(if ready {
            Step::Ready(Box::new((lease, snapshot)))
        } else {
            Step::Wait(None)
        })
    }

    async fn inject(
        self: &Arc<Self>,
        lease: &Lease,
        sandbox: &Sandbox,
        inject: &Inject,
    ) -> Result<()> {
        if inject.files.is_empty() || self.state.lock().injected.contains(&lease.id) {
            return Ok(());
        }
        // Group files by directory into one tar per call: extract at `/`.
        let tar_path = self
            .config
            .blob_dir
            .join(format!("inject-{}.tar", lease.id));
        {
            let file = std::fs::File::create(&tar_path)
                .map_err(|e| Error::internal(format!("inject tar: {e}")))?;
            let mut builder = tar::Builder::new(file);
            for spec in &inject.files {
                let path = spec.path.trim_start_matches('/');
                if path.is_empty() || path.split('/').any(|p| p == "..") {
                    return Err(Error::invalid(format!("bad inject path `{}`", spec.path)));
                }
                let mut header = tar::Header::new_gnu();
                header.set_size(spec.data.len() as u64);
                header.set_mode(if spec.mode == 0 { 0o644 } else { spec.mode });
                header.set_mtime(now() as u64);
                header.set_cksum();
                builder
                    .append_data(&mut header, path, spec.data.as_slice())
                    .map_err(|e| Error::internal(format!("inject tar: {e}")))?;
            }
            builder
                .finish()
                .map_err(|e| Error::internal(format!("inject tar: {e}")))?;
        }
        let result = self
            .upload_path(&sandbox.id, &lease.id, "/", &tar_path)
            .await;
        let _ = std::fs::remove_file(&tar_path);
        result?;
        self.state.lock().injected.insert(lease.id.clone());
        Ok(())
    }

    /// `Renew`.
    pub async fn renew(&self, request: RenewRequest) -> Result<Lease> {
        let (lease, ops) = {
            let mut state = self.state.lock();
            let sandbox_id = state
                .leases
                .get(&request.lease_id)
                .map(|lease| lease.sandbox_id.clone())
                .ok_or_else(|| Error::not_found("lease not found"))?;
            // A lease outlives neither its machine's failure nor its destroy:
            // extending it would promise capacity that cannot be used.
            let sandbox_state = state
                .sandboxes
                .get(&sandbox_id)
                .map(|sandbox| sandbox.state);
            match sandbox_state {
                Some(SandboxState::Failed) => {
                    let reason = &state.sandboxes[&sandbox_id].error;
                    let detail = if reason.is_empty() {
                        String::new()
                    } else {
                        format!(": {reason}")
                    };
                    return Err(Error::precondition(format!(
                        "sandbox {sandbox_id} failed{detail}"
                    )));
                }
                Some(SandboxState::Terminating | SandboxState::Gone) => {
                    return Err(Error::precondition(format!(
                        "sandbox {sandbox_id} is being destroyed"
                    )));
                }
                Some(_) => {}
                None => return Err(Error::not_found("leased sandbox is gone")),
            }
            let lease = state
                .leases
                .get_mut(&request.lease_id)
                .ok_or_else(|| Error::not_found("lease not found"))?;
            let ttl = if request.ttl_secs == 0 {
                self.config.lease_ttl.as_secs()
            } else {
                request.ttl_secs
            };
            lease.expires_at = now() + ttl as i64;
            let lease = lease.clone();
            let ops = vec![self.put(Kind::Leases, &lease.id, &lease)];
            (lease, ops)
        };
        self.persist(ops).await;
        self.config.metrics.record_lease_transition("renewed");
        Ok(lease)
    }

    /// `Release`.
    #[tracing::instrument(skip(self), fields(lease_id = %request.lease_id, mode = ?request.mode))]
    pub async fn release(&self, request: ReleaseRequest) -> Result<()> {
        let ops = {
            let mut state = self.state.lock();
            if !state.leases.contains_key(&request.lease_id) {
                return Err(Error::not_found("lease not found"));
            }
            let mut ops = Vec::new();
            self.release_locked(
                &mut state,
                &request.lease_id,
                request.mode,
                request.preserve_ttl_secs,
                &mut ops,
            );
            ops
        };
        self.persist(ops).await;
        self.changed.notify_waiters();
        Ok(())
    }

    /// `Get`: one sandbox, for the caller that holds its lease.
    pub fn get(&self, id: &str, lease_id: &str) -> Result<Sandbox> {
        let state = self.state.lock();
        let sandbox = state
            .sandboxes
            .get(id)
            .ok_or_else(|| Error::not_found(format!("sandbox `{id}` not found")))?;
        authorize(&state, sandbox, lease_id)?;
        Ok(sandbox.clone())
    }

    /// `List`: every sandbox, with lease ids redacted. Callers learn which
    /// sandboxes exist, never which capability drives an arbitrary one.
    pub fn list(&self) -> Vec<Sandbox> {
        self.state
            .lock()
            .sandboxes
            .values()
            .map(|sandbox| {
                let mut sandbox = sandbox.clone();
                sandbox.lease_id.clear();
                sandbox
            })
            .collect()
    }

    /// Look up a sandbox and require the caller's lease for it.
    fn check_lease(&self, sandbox_id: &str, lease_id: &str) -> Result<()> {
        let state = self.state.lock();
        let sandbox = state
            .sandboxes
            .get(sandbox_id)
            .ok_or_else(|| Error::not_found("sandbox not found"))?;
        authorize(&state, sandbox, lease_id)
    }

    /// `List` page, ordered by id; lease ids are redacted like `list`.
    pub fn list_page(&self, request: &ListRequest) -> Result<ListSandboxesResponse> {
        let state = self.state.lock();
        let (sandboxes, next_page_token) = paginate(
            state.sandboxes.iter().map(|(id, sandbox)| {
                let mut sandbox = sandbox.clone();
                sandbox.lease_id.clear();
                (id.clone(), sandbox)
            }),
            request,
        )?;
        Ok(ListSandboxesResponse {
            sandboxes,
            next_page_token,
        })
    }

    /// `ListProviders`.
    pub fn list_providers(&self) -> Vec<Provider> {
        self.state
            .lock()
            .providers
            .values()
            .map(|rec| rec.provider.clone())
            .collect()
    }

    /// `ListProviders` page, ordered by provider id.
    pub fn list_providers_page(&self, request: &ListRequest) -> Result<ListProvidersResponse> {
        let state = self.state.lock();
        let (providers, next_page_token) = paginate(
            state
                .providers
                .iter()
                .map(|(id, rec)| (id.clone(), rec.provider.clone())),
            request,
        )?;
        Ok(ListProvidersResponse {
            providers,
            next_page_token,
        })
    }

    /// `Stats`: per-provider host sample + capacity and sandboxes by state
    /// with their latest reported runtime facts. Aggregates only — the
    /// stored report never carries host PIDs, process names or data-dir
    /// paths, because every client-token holder can read this.
    pub fn stats(&self) -> StatsResponse {
        let state = self.state.lock();
        let providers = state
            .providers
            .values()
            .map(|rec| {
                let sandboxes = state
                    .sandboxes
                    .values()
                    .filter(|sandbox| sandbox.provider_id == rec.provider.id)
                    .map(|sandbox| SandboxStats {
                        sandbox: sandbox.clone(),
                        runtime: rec
                            .sandbox_stats
                            .get(&sandbox.native_ref)
                            .cloned()
                            .unwrap_or_default(),
                    })
                    .collect();
                ProviderStats {
                    provider_id: rec.provider.id.clone(),
                    name: rec.provider.name.clone(),
                    drivers: rec.provider.drivers.clone(),
                    host: rec.host.clone().unwrap_or_default(),
                    host_sample_age_secs: rec
                        .host_sampled_at
                        .map(|at| at.elapsed().as_secs() as i64)
                        .unwrap_or(0),
                    sandboxes,
                }
            })
            .collect();
        StatsResponse { providers }
    }

    fn usable(sandbox: &Sandbox) -> Result<()> {
        match sandbox.state {
            SandboxState::Leased | SandboxState::Preserved | SandboxState::Ready => Ok(()),
            SandboxState::Stopped => Err(Error::precondition(format!(
                "sandbox {} is stopped; start it first",
                sandbox.id
            ))),
            state => Err(Error::precondition(format!(
                "sandbox {} is {state:?}",
                sandbox.id
            ))),
        }
    }

    /// `Exec`: stream output then the exit status.
    ///
    /// A zero `timeout_secs` gets the service deadline
    /// ([`ServiceConfig::exec_timeout`]). Dropping the returned stream queues
    /// a `CancelExec` for the provider and drops the exec state.
    pub fn exec(
        self: &Arc<Self>,
        request: ExecRequest,
    ) -> Result<BoxStream<'static, Result<ExecEvent>>> {
        let _span = tracing::info_span!("sandbox.exec", sandbox_id = %request.sandbox_id).entered();
        if request.argv.is_empty() {
            return Err(Error::invalid("argv is required"));
        }
        let mut state = self.state.lock();
        let sandbox = state
            .sandboxes
            .get(&request.sandbox_id)
            .cloned()
            .ok_or_else(|| Error::not_found("sandbox not found"))?;
        authorize(&state, &sandbox, &request.lease_id)?;
        Self::usable(&sandbox)?;
        let mut merged = request.clone();
        if let Some(lease) = state.leases.get(&request.lease_id) {
            let mut env = lease.env.clone();
            env.extend(request.env.clone());
            merged.env = env;
        }
        // The lease is the caller's capability: a provider never sees it.
        merged.lease_id.clear();
        if merged.timeout_secs == 0 {
            merged.timeout_secs = self.config.exec_timeout.as_secs().max(1);
        }
        let deadline = Instant::now() + Duration::from_secs(merged.timeout_secs);
        let exec_id = new_id("exe");
        let work_id = new_id("wrk");
        let (tx, rx) = mpsc::channel(256);
        let finished = CancellationToken::new();
        state.execs.insert(
            exec_id.clone(),
            Exec {
                sandbox_id: sandbox.id.clone(),
                work_id: work_id.clone(),
                tx: tx.clone(),
                sequence: 0,
                deadline,
                finished: finished.clone(),
            },
        );
        let work = WorkItem {
            id: work_id,
            exec: Some(ExecWork {
                exec_id: exec_id.clone(),
                target: Self::target(&sandbox),
                request: merged,
            }),
            ..WorkItem::default()
        };
        if let Err(error) = self.enqueue(
            &mut state,
            &sandbox.provider_id,
            work,
            Pending::Exec {
                exec_id: exec_id.clone(),
            },
        ) {
            if let Some(exec) = state.execs.remove(&exec_id) {
                exec.finished.cancel();
            }
            return Err(error);
        }
        drop(state);
        // The sender held here keeps the channel open until either the exec
        // finishes (`finished`) or the caller drops the stream (`closed`).
        let service = Arc::clone(self);
        tokio::spawn(async move {
            let held = tx;
            tokio::select! {
                _ = held.closed() => service.cancel_disconnected(&exec_id).await,
                _ = finished.cancelled() => {}
            }
        });
        let metrics = self.config.metrics.clone();
        Ok(record_exec_metrics(
            metrics,
            Instant::now(),
            tokio_stream_from(rx),
        ))
    }

    /// The caller's exec stream went away: drop the exec and tell the
    /// provider to kill the guest command.
    async fn cancel_disconnected(&self, exec_id: &str) {
        let mut state = self.state.lock();
        let Some(exec) = state.execs.remove(exec_id) else {
            // Completed in the meantime; there is nothing to cancel.
            return;
        };
        let Some(sandbox) = state.sandboxes.get(&exec.sandbox_id).cloned() else {
            return;
        };
        Self::forget_work_locked(&mut state, &sandbox.provider_id, &exec.work_id);
        self.request_cancel_locked(&mut state, &sandbox, exec_id);
    }

    /// Run a command to completion, collecting output (convenience).
    pub async fn exec_collect(
        self: &Arc<Self>,
        request: ExecRequest,
    ) -> Result<(i32, Vec<u8>, Vec<u8>)> {
        collect_exec(self.exec(request)?).await
    }

    // ─── Data plane ────────────────────────────────────────────────

    /// `OpenStream`: issue a ticket.
    pub fn open_stream(&self, request: OpenStreamRequest) -> Result<Ticket> {
        if request.kind == StreamKind::StreamKindUnspecified {
            return Err(Error::invalid("kind is required"));
        }
        let windows_absolute = request.path.len() > 2
            && request.path.as_bytes()[0].is_ascii_alphabetic()
            && request.path.as_bytes()[1] == b':';
        if !request.path.starts_with('/') && !windows_absolute {
            return Err(Error::invalid("path must be absolute"));
        }
        let mut state = self.state.lock();
        let sandbox = state
            .sandboxes
            .get(&request.sandbox_id)
            .ok_or_else(|| Error::not_found("sandbox not found"))?;
        authorize(&state, sandbox, &request.lease_id)?;
        Self::usable(sandbox)?;
        let ticket = new_id("tkt");
        let token = uuid::Uuid::new_v4().simple().to_string();
        let expires_at = now() + self.config.ticket_ttl.as_secs() as i64;
        state.blobs.insert(
            ticket.clone(),
            Blob {
                sandbox_id: request.sandbox_id,
                kind: request.kind,
                guest_path: request.path,
                token: token.clone(),
                expires_at,
                path: self.config.blob_dir.join(format!("{ticket}.tar")),
            },
        );
        Ok(Ticket {
            url: format!("/sandbox/v1/streams/{ticket}"),
            token,
            expires_at,
        })
    }

    /// Validate a client ticket; returns `(kind, blob path)`.
    pub fn claim_ticket(&self, ticket: &str, token: &str) -> Result<(StreamKind, PathBuf)> {
        let state = self.state.lock();
        let blob = state
            .blobs
            .get(ticket)
            .filter(|blob| blob.expires_at > now())
            .ok_or_else(|| Error::not_found("ticket not found or expired"))?;
        if !constant_time_eq(blob.token.as_bytes(), token.as_bytes()) {
            return Err(Error::new(Code::Unauthenticated, "bad ticket token"));
        }
        Ok((blob.kind, blob.path.clone()))
    }

    /// Complete an upload ticket whose tar landed at its blob path: extract
    /// it in the guest. Consumes the ticket.
    pub async fn finish_upload(&self, ticket: &str) -> Result<()> {
        let (sandbox_id, guest_path, path) = {
            let mut state = self.state.lock();
            let blob = state
                .blobs
                .remove(ticket)
                .ok_or_else(|| Error::not_found("ticket not found"))?;
            (blob.sandbox_id, blob.guest_path, blob.path)
        };
        let result = self.stage_upload(&sandbox_id, &guest_path, &path).await;
        let _ = std::fs::remove_file(&path);
        result
    }

    /// Fill a download ticket's blob with a tar of the guest path.
    /// The caller streams the file and then calls [`Service::drop_ticket`].
    pub async fn prepare_download(&self, ticket: &str) -> Result<PathBuf> {
        let (sandbox_id, guest_path, path) = {
            let state = self.state.lock();
            let blob = state
                .blobs
                .get(ticket)
                .ok_or_else(|| Error::not_found("ticket not found"))?;
            (
                blob.sandbox_id.clone(),
                blob.guest_path.clone(),
                blob.path.clone(),
            )
        };
        self.copy_work(&sandbox_id, ticket, &guest_path, StreamKind::Download)
            .await?;
        Ok(path)
    }

    /// Forget a ticket and its blob.
    pub fn drop_ticket(&self, ticket: &str) {
        if let Some(blob) = self.state.lock().blobs.remove(ticket) {
            let _ = std::fs::remove_file(blob.path);
        }
    }

    /// Extract a local tar into the guest (in-process callers). The caller
    /// must hold the sandbox's lease; data-plane tickets are their own
    /// capability and use `stage_upload` directly.
    pub async fn upload_path(
        &self,
        sandbox_id: &str,
        lease_id: &str,
        guest_dir: &str,
        tar: &Path,
    ) -> Result<()> {
        self.check_lease(sandbox_id, lease_id)?;
        self.stage_upload(sandbox_id, guest_dir, tar).await
    }

    /// Copy a staged tar to a fresh agent ticket and extract it in the guest.
    async fn stage_upload(&self, sandbox_id: &str, guest_dir: &str, tar: &Path) -> Result<()> {
        let ticket = new_id("tkt");
        let path = self.config.blob_dir.join(format!("{ticket}.tar"));
        std::fs::copy(tar, &path).map_err(|e| Error::internal(format!("stage upload: {e}")))?;
        self.state.lock().blobs.insert(
            ticket.clone(),
            Blob {
                sandbox_id: sandbox_id.to_owned(),
                kind: StreamKind::Upload,
                guest_path: guest_dir.to_owned(),
                token: String::new(),
                expires_at: now() + self.config.ticket_ttl.as_secs() as i64,
                path: path.clone(),
            },
        );
        let result = self
            .copy_work(sandbox_id, &ticket, guest_dir, StreamKind::Upload)
            .await;
        self.drop_ticket(&ticket);
        result
    }

    /// Tar a guest path into a local file (in-process callers).
    pub async fn download_path(
        &self,
        sandbox_id: &str,
        lease_id: &str,
        guest_path: &str,
        dest: &Path,
    ) -> Result<()> {
        self.check_lease(sandbox_id, lease_id)?;
        let ticket = new_id("tkt");
        let path = self.config.blob_dir.join(format!("{ticket}.tar"));
        self.state.lock().blobs.insert(
            ticket.clone(),
            Blob {
                sandbox_id: sandbox_id.to_owned(),
                kind: StreamKind::Download,
                guest_path: guest_path.to_owned(),
                token: String::new(),
                expires_at: now() + self.config.ticket_ttl.as_secs() as i64,
                path: path.clone(),
            },
        );
        let result = self
            .copy_work(sandbox_id, &ticket, guest_path, StreamKind::Download)
            .await
            .and_then(|_| {
                std::fs::copy(&path, dest)
                    .map(|_| ())
                    .map_err(|e| Error::internal(format!("download: {e}")))
            });
        self.drop_ticket(&ticket);
        result
    }

    async fn copy_work(
        &self,
        sandbox_id: &str,
        ticket: &str,
        guest_path: &str,
        kind: StreamKind,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.state.lock();
            let sandbox = state
                .sandboxes
                .get(sandbox_id)
                .cloned()
                .ok_or_else(|| Error::not_found("sandbox not found"))?;
            let work = WorkItem {
                id: new_id("wrk"),
                copy: Some(CopyWork {
                    target: Self::target(&sandbox),
                    ticket: ticket.to_owned(),
                    path: guest_path.to_owned(),
                    kind,
                }),
                ..WorkItem::default()
            };
            self.enqueue(
                &mut state,
                &sandbox.provider_id,
                work,
                Pending::Copy { reply: tx },
            )?;
        }
        tokio::time::timeout(self.config.work_timeout, rx)
            .await
            .map_err(|_| Error::unavailable("provider did not report the copy"))?
            .map_err(|_| Error::unavailable("provider dropped the copy"))?
            .map(|_| ())
    }

    /// Blob path for an agent (provider must own the ticket's sandbox).
    pub fn agent_blob(&self, provider_id: &str, generation: u64, ticket: &str) -> Result<PathBuf> {
        let state = self.state.lock();
        self.fence(&state, provider_id, generation)?;
        let blob = state
            .blobs
            .get(ticket)
            .ok_or_else(|| Error::not_found("ticket not found"))?;
        let owner = state
            .sandboxes
            .get(&blob.sandbox_id)
            .map(|s| s.provider_id.as_str());
        if owner != Some(provider_id) {
            return Err(Error::new(
                Code::PermissionDenied,
                "ticket belongs to another provider",
            ));
        }
        Ok(blob.path.clone())
    }

    // ─── Snapshot / pause / resume ─────────────────────────────────

    /// `Snapshot`: capture a leased, snapshot-capable sandbox as a template.
    /// The source sandbox is consumed (its lease ends).
    pub async fn snapshot(&self, request: SnapshotRequest) -> Result<Template> {
        validate_name("template", &request.name)?;
        let (tx, rx) = oneshot::channel();
        let (sandbox, source_template) = {
            let mut state = self.state.lock();
            let sandbox = state
                .sandboxes
                .get(&request.sandbox_id)
                .cloned()
                .ok_or_else(|| Error::not_found("sandbox not found"))?;
            authorize(&state, &sandbox, &request.lease_id)?;
            if sandbox.state != SandboxState::Leased {
                return Err(Error::precondition(
                    "only a leased sandbox can be snapshotted",
                ));
            }
            let caps = self.driver_caps(&state, &sandbox)?;
            if !caps.snapshot {
                return Err(Error::precondition(format!(
                    "{}: snapshot unsupported",
                    sandbox.driver
                )));
            }
            let source_template = state
                .templates
                .get(&sandbox.template)
                .cloned()
                .unwrap_or_default();
            let work = WorkItem {
                id: new_id("wrk"),
                snapshot: Some(SnapshotWork {
                    target: Self::target(&sandbox),
                    template: request.name.clone(),
                }),
                ..WorkItem::default()
            };
            self.enqueue(
                &mut state,
                &sandbox.provider_id,
                work,
                Pending::Snapshot { reply: tx },
            )?;
            (sandbox, source_template)
        };
        let native_ref = tokio::time::timeout(self.config.work_timeout, rx)
            .await
            .map_err(|_| Error::unavailable("provider did not report the snapshot"))?
            .map_err(|_| Error::unavailable("provider dropped the snapshot"))??;
        let spec = TemplateSpec {
            name: request.name.clone(),
            os: source_template.spec.os.clone(),
            snapshot_of: Some(sandbox.id.clone()),
            requires: source_template.spec.requires.clone(),
            labels: source_template.spec.labels.clone(),
            ..TemplateSpec::default()
        };
        let template = Template {
            digest: template_digest(&spec),
            spec,
            provider_id: sandbox.provider_id.clone(),
            driver: sandbox.driver.clone(),
            native_ref,
            created_at: now(),
        };
        let ops = {
            let mut state = self.state.lock();
            let mut ops = vec![self.put(Kind::Templates, &template.spec.name, &template)];
            let replaced = state
                .templates
                .insert(template.spec.name.clone(), template.clone());
            if let Some(old) = replaced.filter(|old| {
                !old.native_ref.is_empty()
                    && (old.native_ref != template.native_ref
                        || old.provider_id != template.provider_id)
            }) {
                // Retire the previous snapshot once nothing still starts from it.
                self.drop_snapshot_locked(&mut state, &old);
            }
            // The source sandbox now backs the snapshot (or was freed by the
            // driver): end its lease without destroying it.
            if let Some(source) = state.sandboxes.remove(&sandbox.id) {
                if let Some(lease) = state.leases.remove(&source.lease_id) {
                    state.lease_by_key.remove(&lease.idempotency_key);
                    ops.push(self.remove(Kind::Leases, &lease.id));
                }
                ops.push(self.remove(Kind::Sandboxes, &source.id));
            }
            ops
        };
        self.persist(ops).await;
        Ok(template)
    }

    fn drop_snapshot_locked(&self, state: &mut State, template: &Template) {
        let work = WorkItem {
            id: new_id("wrk"),
            destroy: Some(SandboxWork {
                sandbox_id: String::new(),
                driver: template.driver.clone(),
                native_ref: format!("snapshot:{}", template.native_ref),
            }),
            ..WorkItem::default()
        };
        let _ = self.enqueue(
            state,
            &template.provider_id,
            work,
            Pending::Destroy {
                sandbox_id: String::new(),
            },
        );
    }

    fn driver_caps(&self, state: &State, sandbox: &Sandbox) -> Result<Capabilities> {
        state
            .providers
            .get(&sandbox.provider_id)
            .and_then(|rec| {
                rec.provider
                    .drivers
                    .iter()
                    .find(|d| d.driver == sandbox.driver)
            })
            .map(|d| d.capabilities.clone())
            .ok_or_else(|| Error::unavailable("sandbox provider is gone"))
    }

    /// `Stop`: power off, keeping the machine and its disk.
    pub async fn stop(&self, id: &str, lease_id: &str) -> Result<()> {
        self.power(id, lease_id, PowerOp::Stop).await
    }

    /// `Start`: boot a stopped machine, optionally as a branch base.
    pub async fn start(&self, id: &str, lease_id: &str, forkable: bool) -> Result<()> {
        self.power(id, lease_id, PowerOp::Start { forkable }).await
    }

    async fn power(&self, id: &str, lease_id: &str, op: PowerOp) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.state.lock();
            let sandbox = state
                .sandboxes
                .get(id)
                .cloned()
                .ok_or_else(|| Error::not_found("sandbox not found"))?;
            authorize(&state, &sandbox, lease_id)?;
            let caps = self.driver_caps(&state, &sandbox)?;
            if !caps.pause {
                return Err(Error::precondition(format!(
                    "{}: stop/start unsupported",
                    sandbox.driver
                )));
            }
            let target = Self::target(&sandbox);
            let (work, kind) = match op {
                PowerOp::Stop => (
                    WorkItem {
                        id: new_id("wrk"),
                        stop: Some(target),
                        ..WorkItem::default()
                    },
                    Pending::Stop {
                        sandbox_id: id.to_owned(),
                        reply: tx,
                    },
                ),
                PowerOp::Start { forkable } => (
                    WorkItem {
                        id: new_id("wrk"),
                        start: Some(StartWork { target, forkable }),
                        ..WorkItem::default()
                    },
                    Pending::Start {
                        sandbox_id: id.to_owned(),
                        forkable,
                        reply: tx,
                    },
                ),
            };
            self.enqueue(&mut state, &sandbox.provider_id, work, kind)?;
        }
        tokio::time::timeout(self.config.work_timeout, rx)
            .await
            .map_err(|_| Error::unavailable("provider did not report the request"))?
            .map_err(|_| Error::unavailable("provider dropped the request"))?
            .map(|_| ())
    }

    /// `Pause`.
    pub async fn pause(&self, id: &str, lease_id: &str) -> Result<()> {
        self.pause_resume(id, lease_id, true).await
    }

    /// `Resume`.
    pub async fn resume(&self, id: &str, lease_id: &str) -> Result<()> {
        self.pause_resume(id, lease_id, false).await
    }

    async fn pause_resume(&self, id: &str, lease_id: &str, pause: bool) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.state.lock();
            let sandbox = state
                .sandboxes
                .get(id)
                .cloned()
                .ok_or_else(|| Error::not_found("sandbox not found"))?;
            authorize(&state, &sandbox, lease_id)?;
            let caps = self.driver_caps(&state, &sandbox)?;
            if !caps.pause {
                return Err(Error::precondition(format!(
                    "{}: pause unsupported",
                    sandbox.driver
                )));
            }
            let target = Self::target(&sandbox);
            let (work, kind) = if pause {
                (
                    WorkItem {
                        id: new_id("wrk"),
                        pause: Some(target),
                        ..WorkItem::default()
                    },
                    Pending::Pause {
                        sandbox_id: id.to_owned(),
                        reply: tx,
                    },
                )
            } else {
                (
                    WorkItem {
                        id: new_id("wrk"),
                        resume: Some(target),
                        ..WorkItem::default()
                    },
                    Pending::Resume {
                        sandbox_id: id.to_owned(),
                        reply: tx,
                    },
                )
            };
            self.enqueue(&mut state, &sandbox.provider_id, work, kind)?;
        }
        tokio::time::timeout(self.config.work_timeout, rx)
            .await
            .map_err(|_| Error::unavailable("provider did not report the request"))?
            .map_err(|_| Error::unavailable("provider dropped the request"))?
            .map(|_| ())
    }

    // ─── Providers ─────────────────────────────────────────────────

    fn fence(&self, state: &State, provider_id: &str, generation: u64) -> Result<()> {
        let rec = state
            .providers
            .get(provider_id)
            .ok_or_else(|| Error::not_found("provider not registered"))?;
        if rec.provider.generation != generation {
            return Err(Error::aborted(format!(
                "stale generation {generation} (current {})",
                rec.provider.generation
            )));
        }
        Ok(())
    }

    /// Find a provider by its registered name, falling back to its id.
    fn provider_id_for(state: &State, reference: &str) -> Result<String> {
        if reference.is_empty() {
            return Err(Error::invalid("provider name is required"));
        }
        for (id, rec) in &state.providers {
            if id == reference || rec.provider.name == reference {
                return Ok(id.clone());
            }
        }
        Err(Error::not_found(format!(
            "provider `{reference}` not found"
        )))
    }

    /// `CordonProvider` / `UncordonProvider`: stop or resume placements.
    /// Existing leases and warm members are untouched.
    pub async fn set_provider_cordon(&self, reference: &str, cordoned: bool) -> Result<Provider> {
        let (provider, ops) = {
            let mut state = self.state.lock();
            let id = Self::provider_id_for(&state, reference)?;
            let rec = state.providers.get_mut(&id).expect("provider looked up");
            rec.provider.cordoned = cordoned;
            let provider = rec.provider.clone();
            let ops = vec![self.put(Kind::Providers, &id, &provider)];
            (provider, ops)
        };
        info!(provider = %provider.id, cordoned, "provider cordon changed");
        self.persist(ops).await;
        self.changed.notify_waiters();
        Ok(provider)
    }

    /// `DrainProvider`: cordon, release idle pool members, and report the
    /// leases still running. Leased sandboxes are not killed.
    pub async fn drain_provider(&self, reference: &str) -> Result<DrainProviderResult> {
        let (result, ops) = {
            let mut state = self.state.lock();
            let id = Self::provider_id_for(&state, reference)?;
            let rec = state.providers.get_mut(&id).expect("provider looked up");
            rec.provider.cordoned = true;
            let provider = rec.provider.clone();
            let mut ops = vec![self.put(Kind::Providers, &id, &provider)];
            let idle: Vec<String> = state
                .sandboxes
                .values()
                .filter(|sandbox| {
                    sandbox.provider_id == id
                        && !sandbox.pool.is_empty()
                        && sandbox.lease_id.is_empty()
                        && sandbox.state != SandboxState::Terminating
                        && !sandbox.state.is_terminal()
                })
                .map(|sandbox| sandbox.id.clone())
                .collect();
            let released_pool_members = idle.len() as u32;
            for sandbox_id in idle {
                self.destroy_locked(&mut state, &sandbox_id, &mut ops);
            }
            let leased_remaining = state
                .sandboxes
                .values()
                .filter(|sandbox| {
                    sandbox.provider_id == id
                        && !sandbox.lease_id.is_empty()
                        && !sandbox.state.is_terminal()
                })
                .count() as u32;
            (
                DrainProviderResult {
                    released_pool_members,
                    leased_remaining,
                },
                ops,
            )
        };
        info!(
            provider = %reference,
            released = result.released_pool_members,
            leased = result.leased_remaining,
            "provider drained"
        );
        self.persist(ops).await;
        self.changed.notify_waiters();
        Ok(result)
    }

    /// `Register`: same name → same id, next generation. Work the previous
    /// generation holds in flight is redelivered to the new one; its
    /// sandboxes wait for `Reconcile`.
    pub async fn register(&self, request: RegisterRequest) -> Result<RegisterResponse> {
        if request.name.is_empty() {
            return Err(Error::invalid("name is required"));
        }
        let (response, ops) = {
            let mut state = self.state.lock();
            let existing = state
                .providers
                .values()
                .find(|rec| rec.provider.name == request.name)
                .map(|rec| rec.provider.id.clone());
            let id = existing.unwrap_or_else(|| new_id("prv"));
            let generation = state
                .providers
                .get(&id)
                .map_or(1, |rec| rec.provider.generation + 1);
            // Cordon is an operator decision about the provider record: a
            // re-registration must not clear it.
            let cordoned = state
                .providers
                .get(&id)
                .is_some_and(|rec| rec.provider.cordoned);
            let provider = Provider {
                id: id.clone(),
                name: request.name,
                generation,
                drivers: request.drivers,
                last_seen: now(),
                cordoned,
            };
            // Everything the old generation had queued or in flight is
            // unacknowledged: hand it to the new one.
            let (queue, wake) = match state.providers.remove(&id) {
                Some(rec) => {
                    let mut queue = rec.queue;
                    let mut redeliver: Vec<InFlight> = rec.in_flight.into_values().collect();
                    redeliver.sort_by_key(|item| item.visible_at);
                    for item in redeliver.into_iter().rev() {
                        queue.push_front(item.work);
                    }
                    (queue, rec.wake)
                }
                None => (VecDeque::new(), Arc::new(Notify::new())),
            };
            state.providers.insert(
                id.clone(),
                ProviderRec {
                    provider: provider.clone(),
                    seen: Instant::now(),
                    queue,
                    in_flight: HashMap::new(),
                    wake: wake.clone(),
                    host: None,
                    host_sampled_at: None,
                    sandbox_stats: HashMap::new(),
                },
            );
            wake.notify_waiters();
            let ops = vec![self.put(Kind::Providers, &id, &provider)];
            (
                RegisterResponse {
                    provider_id: id,
                    generation,
                },
                ops,
            )
        };
        info!(provider = %response.provider_id, generation = response.generation, "provider registered");
        self.persist(ops).await;
        self.changed.notify_waiters();
        Ok(response)
    }

    /// `Heartbeat`.
    pub fn heartbeat(&self, request: HeartbeatRequest) -> Result<()> {
        let mut state = self.state.lock();
        self.fence(&state, &request.provider_id, request.generation)?;
        if let Some(rec) = state.providers.get_mut(&request.provider_id) {
            rec.seen = Instant::now();
            rec.provider.last_seen = now();
            if let Some(host) = request.host {
                rec.host = Some(host);
                rec.host_sampled_at = Some(Instant::now());
            }
            if !request.sandboxes.is_empty() || !rec.sandbox_stats.is_empty() {
                rec.sandbox_stats = request
                    .sandboxes
                    .into_iter()
                    .filter(|stats| !stats.native_ref.is_empty())
                    .map(|stats| (stats.native_ref.clone(), stats))
                    .collect();
            }
        }
        Ok(())
    }

    /// `Poll`: long-poll for work.
    ///
    /// At most [`ServiceConfig::poll_batch`] items per call. Delivered items
    /// stay in flight until `Report` acknowledges them; in-flight items whose
    /// visibility timeout expired become visible again and are redelivered,
    /// and a re-registration releases everything the previous generation held
    /// (see [`Service::register`]).
    pub async fn poll(&self, request: PollRequest) -> Result<PollResponse> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(request.wait_secs.min(55));
        loop {
            let (wake, next_visible) = {
                let mut state = self.state.lock();
                self.fence(&state, &request.provider_id, request.generation)?;
                let rec = state
                    .providers
                    .get_mut(&request.provider_id)
                    .ok_or_else(|| Error::not_found("provider not registered"))?;
                rec.seen = Instant::now();
                rec.provider.last_seen = now();
                let at = Instant::now();
                // Visibility timeout: unacknowledged work is delivered again.
                let expired: Vec<String> = rec
                    .in_flight
                    .iter()
                    .filter(|(_, item)| item.visible_at <= at)
                    .map(|(work_id, _)| work_id.clone())
                    .collect();
                for work_id in expired {
                    if let Some(item) = rec.in_flight.remove(&work_id) {
                        rec.queue.push_back(item.work);
                    }
                }
                let batch = self.config.poll_batch.max(1);
                let mut work = Vec::with_capacity(batch.min(rec.queue.len()));
                while work.len() < batch {
                    let Some(item) = rec.queue.pop_front() else {
                        break;
                    };
                    rec.in_flight.insert(
                        item.id.clone(),
                        InFlight {
                            work: item.clone(),
                            visible_at: at + self.config.delivery_timeout,
                        },
                    );
                    work.push(item);
                }
                if !work.is_empty() {
                    return Ok(PollResponse { work });
                }
                let next = rec.in_flight.values().map(|item| item.visible_at).min();
                (rec.wake.clone(), next)
            };
            let wake = wake.notified();
            tokio::pin!(wake);
            let until = next_visible.map_or(deadline, tokio::time::Instant::from_std);
            tokio::select! {
                _ = tokio::time::sleep_until(until.min(deadline)) => {}
                _ = &mut wake => {}
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(PollResponse::default());
            }
        }
    }

    /// `Report`.
    pub async fn report(&self, request: ReportRequest) -> Result<()> {
        let ops = {
            let mut state = self.state.lock();
            self.fence(&state, &request.provider_id, request.generation)?;
            let Some(owner) = state
                .pending
                .get(&request.work_id)
                .map(|work| work.provider_id.clone())
            else {
                // Completed work, or a fire-and-forget cancellation: accept
                // the acknowledgement so the delivery is not repeated.
                if let Some(rec) = state.providers.get_mut(&request.provider_id) {
                    rec.in_flight.remove(&request.work_id);
                    rec.queue.retain(|item| item.id != request.work_id);
                }
                return Err(Error::not_found("unknown or completed work"));
            };
            if owner != request.provider_id {
                return Err(Error::new(
                    Code::PermissionDenied,
                    "work belongs to another provider",
                ));
            }
            let result = if request.ok {
                Ok(request.native_ref)
            } else {
                Err(Error::unavailable(request.error))
            };
            let mut ops = Vec::new();
            self.complete_locked(&mut state, &request.work_id, result, &mut ops);
            ops
        };
        self.persist(ops).await;
        self.changed.notify_waiters();
        Ok(())
    }

    fn complete_locked(
        &self,
        state: &mut State,
        work_id: &str,
        result: Result<String>,
        ops: &mut Vec<Op>,
    ) {
        let Some(work) = state.pending.remove(work_id) else {
            return;
        };
        let provider_id = work.provider_id;
        if let Some(rec) = state.providers.get_mut(&provider_id) {
            // The work may have been redelivered (queued) between the
            // provider reading it and this report.
            rec.in_flight.remove(work_id);
            rec.queue.retain(|item| item.id != work_id);
        }
        match work.kind {
            Pending::Create { sandbox_id, driver } => match result {
                Ok(native_ref) => {
                    let Some(sandbox) = state.sandboxes.get_mut(&sandbox_id) else {
                        // Released while provisioning: destroy the late machine.
                        let work = WorkItem {
                            id: new_id("wrk"),
                            destroy: Some(SandboxWork {
                                sandbox_id: sandbox_id.clone(),
                                driver,
                                native_ref,
                            }),
                            ..WorkItem::default()
                        };
                        let _ = self.enqueue(
                            state,
                            &provider_id,
                            work,
                            Pending::Destroy { sandbox_id },
                        );
                        return;
                    };
                    sandbox.native_ref = native_ref;
                    sandbox.state = if sandbox.lease_id.is_empty() {
                        SandboxState::Ready
                    } else {
                        SandboxState::Leased
                    };
                    ops.push(self.put(Kind::Sandboxes, &sandbox_id, &*sandbox));
                    state.created_at.remove(&sandbox_id);
                }
                Err(error) => {
                    self.config.metrics.record_create_failure("reported");
                    self.fail_locked(state, &sandbox_id, &error.message, ops);
                }
            },
            Pending::Destroy { sandbox_id } => {
                // A failed destroy still drops the record: the machine is
                // then an orphan the next reconcile removes.
                if let Err(error) = &result {
                    warn!(%sandbox_id, %error, "destroy failed; reconcile will retry");
                }
                state.destroy_attempts.remove(&sandbox_id);
                if state.sandboxes.remove(&sandbox_id).is_some() {
                    ops.push(self.remove(Kind::Sandboxes, &sandbox_id));
                }
            }
            Pending::Exec { exec_id } => {
                // Success is signalled by the exit event (`ReportExec`); a
                // failed exec work ends the stream with the error.
                if let Err(error) = result
                    && let Some(exec) = state.execs.remove(&exec_id)
                {
                    exec.finished.cancel();
                    let _ = exec.tx.try_send(Err(error));
                }
            }
            Pending::Copy { reply } | Pending::Snapshot { reply } => {
                let _ = reply.send(result);
            }
            Pending::Pause { sandbox_id, reply } => {
                if result.is_ok()
                    && let Some(sandbox) = state.sandboxes.get_mut(&sandbox_id)
                {
                    sandbox.state = SandboxState::Paused;
                    ops.push(self.put(Kind::Sandboxes, &sandbox_id, &*sandbox));
                }
                let _ = reply.send(result);
            }
            Pending::Stop { sandbox_id, reply } => {
                if result.is_ok()
                    && let Some(sandbox) = state.sandboxes.get_mut(&sandbox_id)
                {
                    sandbox.state = SandboxState::Stopped;
                    ops.push(self.put(Kind::Sandboxes, &sandbox_id, &*sandbox));
                }
                let _ = reply.send(result);
            }
            Pending::Start {
                sandbox_id,
                forkable,
                reply,
            } => {
                if result.is_ok()
                    && let Some(sandbox) = state.sandboxes.get_mut(&sandbox_id)
                {
                    sandbox.state = if sandbox.lease_id.is_empty() {
                        SandboxState::Ready
                    } else {
                        SandboxState::Leased
                    };
                    if forkable {
                        sandbox.error.clear();
                    }
                    ops.push(self.put(Kind::Sandboxes, &sandbox_id, &*sandbox));
                }
                let _ = reply.send(result);
            }
            Pending::Resume { sandbox_id, reply } => {
                if result.is_ok()
                    && let Some(sandbox) = state.sandboxes.get_mut(&sandbox_id)
                {
                    sandbox.state = if sandbox.lease_id.is_empty() {
                        SandboxState::Preserved
                    } else {
                        SandboxState::Leased
                    };
                    ops.push(self.put(Kind::Sandboxes, &sandbox_id, &*sandbox));
                }
                let _ = reply.send(result);
            }
        }
    }

    /// `ReportExec`: forward output; an exit event ends the stream.
    ///
    /// Batches carry a per-exec `sequence`; a batch the service already
    /// applied (a retried `ReportExec`) is acknowledged and dropped. The
    /// caller's stream closes on the exit event even if later batches arrive.
    pub async fn report_exec(&self, request: ReportExecRequest) -> Result<()> {
        let tx = {
            let mut state = self.state.lock();
            self.fence(&state, &request.provider_id, request.generation)?;
            let sandbox_id = state
                .execs
                .get(&request.exec_id)
                .map(|exec| exec.sandbox_id.clone())
                .ok_or_else(|| {
                    // Caller went away (or the exec finished); stop the agent.
                    Error::not_found("exec not found")
                })?;
            // Only the provider that owns the sandbox may report output for
            // its exec: a leaked exec id must not inject output into another
            // provider's stream.
            let mine = state
                .sandboxes
                .get(&sandbox_id)
                .is_some_and(|sandbox| sandbox.provider_id == request.provider_id);
            if !mine {
                return Err(Error::new(
                    Code::PermissionDenied,
                    "exec belongs to another provider",
                ));
            }
            let exec = state
                .execs
                .get_mut(&request.exec_id)
                .ok_or_else(|| Error::not_found("exec not found"))?;
            if request.sequence != 0 {
                if request.sequence <= exec.sequence {
                    // A retry of a batch already delivered.
                    return Ok(());
                }
                exec.sequence = request.sequence;
            }
            exec.tx.clone()
        };
        let mut exited = false;
        for event in request.events {
            let exit = event.exit_code.is_some();
            if tx.send(Ok(event)).await.is_err() {
                // The caller dropped its stream: leave the state for the
                // disconnect watcher, which cancels the guest command.
                return Ok(());
            }
            if exit {
                exited = true;
                break;
            }
        }
        if exited && let Some(exec) = self.state.lock().execs.remove(&request.exec_id) {
            exec.finished.cancel();
        }
        Ok(())
    }

    /// `Reconcile`: diff a provider's inventory against the records.
    #[tracing::instrument(skip(self, request), fields(provider_id = %request.provider_id))]
    pub async fn reconcile(&self, request: ReconcileRequest) -> Result<ReconcileResponse> {
        let (response, ops) = {
            let mut state = self.state.lock();
            self.fence(&state, &request.provider_id, request.generation)?;
            let mut ops = Vec::new();
            let observed: HashSet<&str> = request
                .sandboxes
                .iter()
                .map(|o| o.native_ref.as_str())
                .collect();
            // Records the host no longer has.
            let lost: Vec<String> = state
                .sandboxes
                .values()
                .filter(|s| {
                    s.provider_id == request.provider_id
                        && !s.native_ref.is_empty()
                        && !s.state.is_terminal()
                        && !observed.contains(s.native_ref.as_str())
                })
                .map(|s| s.id.clone())
                .collect();
            self.config.metrics.record_reconcile_lost(lost.len() as u64);
            for id in lost {
                warn!(sandbox = %id, "sandbox missing from provider inventory");
                if state
                    .sandboxes
                    .get(&id)
                    .is_some_and(|s| s.state == SandboxState::Terminating)
                {
                    // The destroy landed (or the machine went away): the
                    // record stops holding capacity now.
                    state.sandboxes.remove(&id);
                    state.created_at.remove(&id);
                    state.destroy_attempts.remove(&id);
                    ops.push(self.remove(Kind::Sandboxes, &id));
                } else {
                    self.fail_locked(
                        &mut state,
                        &id,
                        "lost: missing from provider inventory",
                        &mut ops,
                    );
                    if let Some(sandbox) = state.sandboxes.get_mut(&id) {
                        sandbox.native_ref.clear();
                    }
                }
            }
            // Terminating records whose machine is still on the host: the
            // destroy that was in flight died with the previous process (or
            // its report was lost), so ask again. Bounded: a provider that
            // keeps the machine while reporting it alive is not making
            // progress, and the record must stop holding a slot.
            let stuck: Vec<String> = state
                .sandboxes
                .values()
                .filter(|s| {
                    s.provider_id == request.provider_id
                        && s.state == SandboxState::Terminating
                        && !s.native_ref.is_empty()
                        && observed.contains(s.native_ref.as_str())
                })
                .map(|s| s.id.clone())
                .collect();
            for id in stuck {
                if !self.requeue_destroy_locked(&mut state, &id) {
                    warn!(
                        sandbox = %id,
                        attempts = MAX_DESTROY_ATTEMPTS,
                        "terminating sandbox out of destroy retries; dropping the record so the machine is reaped as an orphan"
                    );
                    state.sandboxes.remove(&id);
                    state.created_at.remove(&id);
                    state.destroy_attempts.remove(&id);
                    ops.push(self.remove(Kind::Sandboxes, &id));
                }
            }
            // Machines no record owns (orphans).
            let snapshots: HashSet<&str> = state
                .templates
                .values()
                .filter(|t| t.provider_id == request.provider_id)
                .map(|t| t.native_ref.as_str())
                .collect();
            let mut destroy = Vec::new();
            for item in &request.sandboxes {
                if snapshots.contains(item.native_ref.as_str()) {
                    continue;
                }
                let owned = state.sandboxes.values().any(|s| {
                    s.provider_id == request.provider_id && s.native_ref == item.native_ref
                });
                let provisioning = (!item.sandbox_id.is_empty()
                    && state
                        .sandboxes
                        .get(&item.sandbox_id)
                        .is_some_and(|s| s.state == SandboxState::Provisioning))
                    || state.sandboxes.values().any(|s| {
                        s.provider_id == request.provider_id
                            && s.state == SandboxState::Provisioning
                            && !s.name.is_empty()
                            && s.name == item.native_ref
                    });
                // Another provider's machine (two agents on one host, or a
                // hand-created VM registered elsewhere) is never ours to
                // reap: only orphans this provider could have created.
                let foreign = state.sandboxes.values().any(|s| {
                    s.provider_id != request.provider_id
                        && !s.state.is_terminal()
                        && (s.native_ref == item.native_ref
                            || (!s.id.is_empty()
                                && !item.sandbox_id.is_empty()
                                && s.id == item.sandbox_id))
                });
                if !owned && !provisioning && !foreign {
                    destroy.push(SandboxWork {
                        sandbox_id: item.sandbox_id.clone(),
                        driver: item.driver.clone(),
                        native_ref: item.native_ref.clone(),
                    });
                }
            }
            self.config
                .metrics
                .record_reconcile_orphans(destroy.len() as u64);
            (ReconcileResponse { destroy }, ops)
        };
        self.persist(ops).await;
        self.changed.notify_waiters();
        Ok(response)
    }
}

enum PowerOp {
    Stop,
    Start { forkable: bool },
}

enum Step {
    Ready(Box<(Lease, Sandbox)>),
    Wait(Option<Error>),
}

fn sandbox_meets(state: &State, sandbox: &Sandbox, requires: &Requirements) -> bool {
    if requires == &Requirements::default() {
        return true;
    }
    state
        .providers
        .get(&sandbox.provider_id)
        .and_then(|rec| {
            rec.provider
                .drivers
                .iter()
                .find(|d| d.driver == sandbox.driver)
        })
        .is_some_and(|driver| {
            let caps = &driver.capabilities;
            (requires.os.is_empty() || caps.os == requires.os)
                && (requires.arch.is_empty() || caps.arch.contains(&requires.arch))
                && requires.features.iter().all(|f| caps.features.contains(f))
                && requires.capabilities.iter().all(|c| caps.has(c))
                && (requires.driver.is_empty() || requires.driver == driver.driver)
        })
}

async fn load<T: DeserializeOwned>(store: &dyn Store, kind: Kind) -> Result<Vec<T>> {
    store
        .load(kind)
        .await?
        .into_iter()
        .map(|value| {
            serde_json::from_value(value)
                .map_err(|e| Error::internal(format!("sandbox store decode: {e}")))
        })
        .collect()
}

fn tokio_stream_from(
    mut rx: mpsc::Receiver<Result<ExecEvent>>,
) -> BoxStream<'static, Result<ExecEvent>> {
    futures::stream::poll_fn(move |cx| rx.poll_recv(cx)).boxed()
}

/// Wrap an exec stream so its duration and exit class are recorded once its
/// terminal event (an exit code, or a stream error) passes through.
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
                    if let Some(code) = event.exit_code {
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
                // Dropped without a terminal event (e.g. the pending work
                // was stranded): still a real duration worth recording.
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
        if let Some(bytes) = event.stdout {
            stdout.extend(bytes);
        }
        if let Some(bytes) = event.stderr {
            stderr.extend(bytes);
        }
        if let Some(code) = event.exit_code {
            return Ok((code, stdout, stderr));
        }
    }
    Err(Error::unavailable(
        "exec stream ended without an exit status",
    ))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn items(count: usize) -> Vec<(String, usize)> {
        (0..count)
            .map(|index| (format!("id-{index:04}"), index))
            .collect()
    }

    #[test]
    fn pages_are_ordered_and_carry_a_token_until_exhausted() {
        let mut request = ListRequest::default();
        let (page, token) = paginate(items(250), &request).unwrap();
        assert_eq!(page, (0..100usize).collect::<Vec<_>>());
        assert!(!token.is_empty());
        request.page_token = token;
        let (page, token) = paginate(items(250), &request).unwrap();
        assert_eq!(page, (100..200usize).collect::<Vec<_>>());
        assert!(!token.is_empty());
        request.page_token = token;
        let (page, token) = paginate(items(250), &request).unwrap();
        assert_eq!(page, (200..250usize).collect::<Vec<_>>());
        assert!(token.is_empty());
    }

    #[test]
    fn page_size_is_capped_and_tokens_are_validated() {
        let request = ListRequest {
            page_size: 5000,
            page_token: String::new(),
        };
        let (page, _) = paginate(items(1500), &request).unwrap();
        assert_eq!(page.len(), MAX_PAGE_SIZE as usize);

        let request = ListRequest {
            page_token: "not base64!".into(),
            ..ListRequest::default()
        };
        let error = paginate(items(3), &request).unwrap_err();
        assert_eq!(error.code, Code::InvalidArgument);
    }
}
