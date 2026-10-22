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
use crate::driver::EXEC_TIMED_OUT;
use crate::error::{Code, Error, Result};
use crate::placement::{Candidate, CandidateId, LeastLoaded, Placement, PlacementRequest};
use crate::store::{Kind, Store};
use crate::tls;

/// Tunables.
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    /// Default lease TTL.
    pub lease_ttl: Duration,
    /// Default time `Acquire` waits for a ready sandbox.
    pub acquire_wait: Duration,
    /// How long an `Acquire` may wait *for capacity* — the pool is at
    /// `max`, or placement reports `resource_exhausted` — before the wait
    /// fails with that `resource_exhausted` error. Provisioning waits are
    /// bounded by `acquire_wait` instead: a cold create legitimately takes
    /// minutes, but a full pool does not free up faster for waiting longer,
    /// and a caller that cannot hear about overload within this window
    /// cannot fail over to another pool or provider either. Without the
    /// bound, an over-capacity `Acquire` parked for the whole
    /// `acquire_wait` (observed ≥600 s against a 900 s default), pinning a
    /// handler and a connection per parked caller.
    pub acquire_capacity_wait: Duration,
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
    /// CA that issues provider client certificates. `None` disables
    /// `Enroll` and `RenewCertificate` (an in-process-only service).
    pub ca: Option<Arc<crate::tls::ServiceCa>>,
}

impl ServiceConfig {
    /// Defaults with blobs under `blob_dir`.
    pub fn new(blob_dir: PathBuf) -> Self {
        Self {
            lease_ttl: Duration::from_secs(3600),
            acquire_wait: Duration::from_secs(900),
            acquire_capacity_wait: Duration::from_secs(30),
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
            ca: None,
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
/// stream as a backstop. The sweep reports the same timeout exit as the
/// driver: whichever path gets there first, the caller sees `124`, never an
/// error that depends on scheduling.
const EXEC_SWEEP_GRACE: Duration = Duration::from_secs(5);

/// The provider a client certificate authenticates, resolved from the leaf's
/// SHA-256 fingerprint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderIdentity {
    /// Provider id.
    pub provider_id: String,
    /// Provider name; `Register` must use exactly this name.
    pub name: String,
}

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
        return Err(Error::invalid_argument(format!(
            "invalid {kind} name `{name}`"
        )));
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
/// The token is the base64url encoding of `v1:<kind>:<last returned id>`:
/// callers pass it back untouched, an id that has since disappeared still
/// resumes at the right position, and anything else (garbage, or a token
/// minted for another listing) is rejected rather than silently rewinding to
/// the first page.
fn paginate<T>(
    kind: &str,
    items: impl IntoIterator<Item = (String, T)>,
    request: &ListRequest,
) -> Result<(Vec<T>, String)> {
    use base64::Engine as _;
    const PREFIX: &str = "v1";
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
            .map_err(|_| Error::invalid_argument("invalid page token"))?;
        let decoded = String::from_utf8(decoded)
            .map_err(|_| Error::invalid_argument("invalid page token"))?;
        let (token_kind, id) = decoded
            .strip_prefix(PREFIX)
            .and_then(|rest| rest.strip_prefix(':'))
            .and_then(|rest| rest.split_once(':'))
            .ok_or_else(|| Error::invalid_argument("invalid page token"))?;
        if token_kind != kind || id.is_empty() {
            return Err(Error::invalid_argument("invalid page token"));
        }
        Some(id.to_owned())
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
            return Ok((page, engine.encode(format!("{PREFIX}:{kind}:{token}"))));
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
        &request.shape,
        &request.requires,
        &request.purpose,
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
    /// How many times the exec's work item has been delivered. Delivery is
    /// at-least-once; a count above one means the previous delivery's
    /// report never arrived (the agent crashed, or the visibility timeout
    /// lapsed mid-run). The agent fences re-execution from its durable
    /// started-set; this counter is the service-side record of the attempt
    /// dimension, kept with the exec for its whole life.
    attempts: u32,
    tx: mpsc::Sender<Result<ExecEvent>>,
    /// Last `ReportExec` batch sequence applied.
    sequence: u64,
    /// Absolute deadline for the whole command.
    deadline: Instant,
    /// Fires when the service stops forwarding this exec, so the watcher
    /// that detects a dropped caller stream releases its sender.
    finished: CancellationToken,
}

/// A single-use join token; only the secret's hash is stored.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct JoinTokenRecord {
    id: String,
    secret_sha256: String,
    expires_at: i64,
}

/// The caller's exec response: the event stream plus the end of a oneshot
/// that tells the disconnect watcher the caller is gone.
///
/// The service keeps a sender for this exec in [`State::execs`] so
/// `ReportExec` can forward output; watching the channel's senders for
/// closure therefore cannot detect a caller that walked away. This guard is
/// dropped with the response stream itself and is unaffected by that sender.
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
    options: StringMap,
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
            network: self.network.mode.as_known().unwrap_or_default(),
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
    join_tokens: BTreeMap<String, JoinTokenRecord>,
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
        for token in load::<JoinTokenRecord>(&*store, Kind::JoinTokens).await? {
            state.join_tokens.insert(token.id.clone(), token);
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
                // `Destroy` releases never fail.
                let _ =
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
            // command already; this is the backstop for a lost provider or a
            // delayed report. The caller gets the same timeout exit the
            // driver reports, so a slow report cannot turn its `124` into an
            // error.
            let over_deadline: Vec<String> = state
                .execs
                .iter()
                .filter(|(_, exec)| exec.deadline + EXEC_SWEEP_GRACE <= Instant::now())
                .map(|(exec_id, _)| exec_id.clone())
                .collect();
            for exec_id in over_deadline {
                warn!(exec_id, "exec passed its deadline; reporting the timeout");
                self.expire_exec_locked(&mut state, &exec_id);
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
            // Expired join tokens and client certificates: neither can
            // authenticate anything anymore, so forget them rather than
            // grow the store forever.
            let stale_tokens: Vec<String> = state
                .join_tokens
                .iter()
                .filter(|(_, token)| token.expires_at <= t)
                .map(|(id, _)| id.clone())
                .collect();
            for id in stale_tokens {
                state.join_tokens.remove(&id);
                ops.push(self.remove(Kind::JoinTokens, &id));
            }
            let mut pruned = Vec::new();
            for rec in state.providers.values_mut() {
                let before = rec.provider.certificates.len();
                rec.provider
                    .certificates
                    .retain(|certificate| certificate.expires_at > t);
                if rec.provider.certificates.len() != before {
                    pruned.push(rec.provider.clone());
                }
            }
            for provider in pruned {
                ops.push(self.put(Kind::Providers, &provider.id, &provider));
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
                    && matches!(
                        s.state.known(),
                        SandboxState::Ready | SandboxState::Provisioning
                    )
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
            spec.recipe().is_some(),
            spec.snapshot_of().is_some(),
        ];
        if sources.iter().filter(|set| **set).count() != 1 {
            return Err(Error::invalid_argument(
                "exactly one template source is required: artifacts, recipe, or snapshotOf",
            ));
        }
        if spec.snapshot_of().is_some() {
            return Err(Error::invalid_argument(
                "snapshot templates are created by Snapshot",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for artifact in &spec.artifacts {
            artifact.validate().map_err(Error::invalid_argument)?;
            if !seen.insert((artifact.format.as_str(), artifact.arch.as_str())) {
                return Err(Error::invalid_argument(format!(
                    "artifact for format `{}` and arch `{}` is declared twice",
                    artifact.format, artifact.arch
                )));
            }
        }
        let template = Template {
            digest: template_digest(&spec),
            created_at: now(),
            spec: spec.into(),
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
                return Err(Error::failed_precondition(format!(
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
            "template",
            state
                .templates
                .iter()
                .map(|(name, template)| (name.clone(), template.clone())),
            request,
        )?;
        Ok(ListTemplatesResponse {
            templates,
            next_page_token,
            ..ListTemplatesResponse::default()
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
            "pool",
            state
                .pools
                .iter()
                .map(|(name, pool)| (name.clone(), pool.clone())),
            request,
        )?;
        Ok(ListPoolsResponse {
            pools,
            next_page_token,
            ..ListPoolsResponse::default()
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
            shape: (*pool.shape).clone(),
            network: (*pool.network).clone(),
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
                    shape: (*pool.shape).clone(),
                    network: (*pool.network).clone(),
                    mounts: pool.mounts.clone(),
                    sockets: pool.sockets.clone(),
                    options: pool.options.clone(),
                    snapshot_capable: false,
                };
                if state.providers.is_empty() {
                    return Ok(());
                }
                self.place_with(state, &placement, false, false).map(|_| ())
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
        self.place_with(state, spec, require_online, true)
    }

    /// `place`, optionally honouring cordons.
    ///
    /// `PutPool` validates with cordons ignored: a cordon is transient, so a
    /// pool whose only capable provider is cordoned is still valid
    /// configuration once that provider is uncordoned.
    fn place_with(
        &self,
        state: &State,
        spec: &PlacementSpec,
        require_online: bool,
        respect_cordon: bool,
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
        let mut cordoned: Vec<&str> = Vec::new();
        for (provider_id, rec) in &state.providers {
            if require_online && rec.seen.elapsed() > self.config.provider_timeout {
                continue;
            }
            // Snapshot templates start only where the snapshot lives.
            if !template.native_ref.is_empty() && template.provider_id != *provider_id {
                continue;
            }
            let is_cordoned = respect_cordon && rec.provider.cordoned;
            let mut capable = false;
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
                if template.spec.recipe().is_some() && template.native_ref.is_empty() {
                    // Recipes are driver-specific; the template names its driver
                    // through `requires.driver`.
                    if requires.driver.is_empty() && template.spec.requires.driver.is_empty() {
                        reasons.push("recipe templates must set requires.driver".into());
                    }
                }
                if !reasons.is_empty() {
                    // A cordoned provider's gaps did not decide the
                    // placement, so keep them out of the unmet list.
                    if !is_cordoned {
                        unmet.push(format!(
                            "{}/{}: {}",
                            rec.provider.name,
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
            if is_cordoned && capable {
                cordoned.push(rec.provider.name.as_str());
            }
        }
        if candidates.is_empty() {
            if full {
                return Err(Error::new(
                    Code::ResourceExhausted,
                    "every matching provider is at capacity",
                ));
            }
            // Every provider that could have taken this placement is
            // cordoned: waiting cannot help, and "not online" is wrong.
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
        let shape = effective_shape(state, &provider_id, &driver, &placement.shape);
        let sandbox = Sandbox {
            id: id.clone(),
            name: name.to_owned(),
            template: placement.template.spec.name.clone(),
            pool: pool.to_owned(),
            provider_id: provider_id.clone(),
            driver: driver.clone(),
            state: SandboxState::Provisioning.into(),
            purpose: purpose.into(),
            created_at: now(),
            shape: shape.into(),
            ..Sandbox::default()
        };
        let sandbox_driver = driver.clone();
        let work = WorkItem::new(
            new_id("wrk"),
            work_item::Work::Create(
                CreateWork {
                    sandbox_id: id.clone(),
                    name: name.to_owned(),
                    driver,
                    template: placement.template.into(),
                    shape: placement.shape.into(),
                    network: placement.network.into(),
                    mounts: placement.mounts,
                    sockets: placement.sockets,
                    options: placement.options,
                    snapshot_capable: placement.snapshot_capable,
                    ..CreateWork::default()
                }
                .into(),
            ),
        );
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

    /// End an exec whose deadline passed without a report: close the caller's
    /// stream with the timeout exit ([`EXEC_TIMED_OUT`]), forget its work,
    /// and ask the provider to kill the guest command. This is the backstop,
    /// so it must land on the same result the driver would have reported.
    fn expire_exec_locked(&self, state: &mut State, exec_id: &str) {
        let Some(exec) = state.execs.remove(exec_id) else {
            return;
        };
        exec.finished.cancel();
        let tx = exec.tx;
        let exit = ExecEvent::exit(EXEC_TIMED_OUT);
        // Queue the exit after any output already buffered. A full channel
        // must not cost the caller its exit status (`try_send` would), so
        // this waits its turn off the state lock.
        tokio::spawn(async move {
            let _ = tx.send(Ok(exit)).await;
        });
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
        let work = WorkItem::new(
            new_id("wrk"),
            work_item::Work::CancelExec(
                CancelExecWork {
                    exec_id: exec_id.to_owned(),
                    target: Self::target(sandbox).into(),
                    ..CancelExecWork::default()
                }
                .into(),
            ),
        );
        let _ = self.enqueue_work(state, &sandbox.provider_id, work);
    }

    fn target(sandbox: &Sandbox) -> SandboxWork {
        SandboxWork {
            sandbox_id: sandbox.id.clone(),
            driver: sandbox.driver.clone(),
            native_ref: sandbox.native_ref.clone(),
            ..SandboxWork::default()
        }
    }

    fn destroy_locked(&self, state: &mut State, id: &str, ops: &mut Vec<Op>) {
        match state.sandboxes.get(id).map(|sandbox| sandbox.state.known()) {
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
            sandbox.state = SandboxState::Gone.into();
            let id = id.to_owned();
            state.sandboxes.remove(&id);
            state.created_at.remove(&id);
            state.destroy_attempts.remove(&id);
            ops.push(self.remove(Kind::Sandboxes, &id));
            return;
        }
        sandbox.state = SandboxState::Terminating.into();
        let snapshot = sandbox.clone();
        ops.push(self.put(Kind::Sandboxes, id, &snapshot));
        let work = WorkItem::new(
            new_id("wrk"),
            work_item::Work::Destroy(Self::target(&snapshot).into()),
        );
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
        let work = WorkItem::new(
            new_id("wrk"),
            work_item::Work::Destroy(Self::target(&sandbox).into()),
        );
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
            sandbox.state = SandboxState::Failed.into();
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
    ) -> Result<()> {
        let Some(lease) = state.leases.get(lease_id).cloned() else {
            return Ok(());
        };
        let preserve = mode == ReleaseMode::Preserve;
        let failed = state
            .sandboxes
            .get(&lease.sandbox_id)
            .is_some_and(|s| s.state == SandboxState::Failed);
        // Preservation is the one way a lease outlives `Release`: the id keeps
        // working as the debugging capability until the window closes. A
        // second preserve extends that window instead of destroying.
        //
        // A stopped or paused sandbox preserves too: `Stop` exists to keep a
        // machine and its disk around, and the debug flow is exactly "stop
        // it, preserve-release it, inspect it later". The record moves to
        // `Preserved`; the machine's power state is the driver's and is not
        // touched (a stopped sandbox stays stopped and must be `Start`ed
        // before it executes again). Anything that cannot be preserved —
        // a failed sandbox, one still provisioning — fails the call instead
        // of falling through to destroy under a success response, which is
        // what this gate used to do.
        if preserve {
            let sandbox_state = state
                .sandboxes
                .get(&lease.sandbox_id)
                .map(|s| s.state.known());
            let preservable = !failed
                && matches!(
                    sandbox_state,
                    Some(
                        SandboxState::Leased
                            | SandboxState::Preserved
                            | SandboxState::Paused
                            | SandboxState::Stopped
                    )
                );
            if !preservable {
                let state_name = sandbox_state
                    .map(|state| format!("{state:?}"))
                    .unwrap_or_else(|| "gone".to_owned());
                return Err(Error::failed_precondition(format!(
                    "sandbox {} cannot be preserved from state {state_name}; \
                     release with DESTROY to give it up",
                    lease.sandbox_id
                )));
            }
            let ttl = if preserve_ttl == 0 {
                self.config.preserve_ttl.as_secs()
            } else {
                preserve_ttl
            };
            let until = now() + ttl as i64;
            if let Some(sandbox) = state.sandboxes.get_mut(&lease.sandbox_id) {
                sandbox.state = SandboxState::Preserved.into();
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
            return Ok(());
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
        Ok(())
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
            return Err(Error::invalid_argument("idempotencyKey is required"));
        }
        if request.pool.is_empty() == request.template.is_empty() {
            return Err(Error::invalid_argument(
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
        // The capacity clock starts at the first capacity-blocked step and
        // is never reset: it bounds the total time this acquire spends
        // unable to place for lack of room, however the steps interleave.
        let mut capacity_deadline: Option<tokio::time::Instant> = None;
        let mut capacity_error: Option<Error> = None;
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
                    return Ok(AcquireResponse {
                        lease: lease.into(),
                        sandbox: sandbox.into(),
                        ..AcquireResponse::default()
                    });
                }
                Step::Wait(last) => {
                    let mut wake_deadline = deadline;
                    if let Some(error) = &last
                        && error.code == Code::ResourceExhausted
                    {
                        let started = *capacity_deadline.get_or_insert(
                            tokio::time::Instant::now() + self.config.acquire_capacity_wait,
                        );
                        capacity_error = Some(Error::new(
                            error.code,
                            crate::error::message(error).to_owned(),
                        ));
                        wake_deadline = wake_deadline.min(started);
                    }
                    if tokio::time::timeout_at(wake_deadline, changed)
                        .await
                        .is_err()
                    {
                        // Whichever bound ran out, report the failure that
                        // bound is about: an overload the caller waited out
                        // in full is `resource_exhausted`, not a generic
                        // deadline — the caller fails over on it.
                        if let Some(error) = capacity_error.take_if(|_| {
                            capacity_deadline.is_some_and(|at| tokio::time::Instant::now() >= at)
                        }) {
                            return Err(error);
                        }
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
                return Err(Error::invalid_argument(format!(
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
            return match sandbox.state.known() {
                // A stopped sandbox is a live sandbox the caller powered
                // off on purpose: hand the lease back so the retry can
                // `Start` it. Destroying it here made an idempotent retry —
                // the operation whose whole point is being safe to repeat —
                // permanently delete a machine and its disk, and the next
                // retry then silently provisioned a different sandbox.
                SandboxState::Leased
                | SandboxState::Paused
                | SandboxState::Preserved
                | SandboxState::Stopped => Ok(Step::Ready(Box::new((lease, sandbox)))),
                SandboxState::Provisioning => Ok(Step::Wait(None)),
                _ => {
                    // Failed/gone: release so a retry with the same key can
                    // start over, and report the failure now.
                    let error = if sandbox.error.is_empty() {
                        format!("sandbox {} is {:?}", sandbox.id, sandbox.state)
                    } else {
                        sandbox.error.clone()
                    };
                    let _ = self.release_locked(state, &lease_id, ReleaseMode::Destroy, 0, ops);
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
                            request.purpose.as_known().unwrap_or_default(),
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
                shape: (*request.shape).clone(),
                network: (*request.network).clone(),
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
                    request.purpose.as_known().unwrap_or_default(),
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
            ..Lease::default()
        };
        let sandbox = state
            .sandboxes
            .get_mut(&sandbox_id)
            .ok_or_else(|| Error::internal("sandbox vanished"))?;
        sandbox.lease_id = lease.id.clone();
        sandbox.purpose = request.purpose;
        sandbox.name = request.name.clone();
        if sandbox.state == SandboxState::Ready {
            sandbox.state = SandboxState::Leased.into();
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
                    return Err(Error::invalid_argument(format!(
                        "bad inject path `{}`",
                        spec.path
                    )));
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
                .map(|sandbox| sandbox.state.known());
            match sandbox_state {
                Some(SandboxState::Failed) => {
                    let reason = &state.sandboxes[&sandbox_id].error;
                    let detail = if reason.is_empty() {
                        String::new()
                    } else {
                        format!(": {reason}")
                    };
                    return Err(Error::failed_precondition(format!(
                        "sandbox {sandbox_id} failed{detail}"
                    )));
                }
                Some(SandboxState::Terminating | SandboxState::Gone) => {
                    return Err(Error::failed_precondition(format!(
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
                request.mode.as_known().unwrap_or_default(),
                request.preserve_ttl_secs,
                &mut ops,
            )?;
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
            "sandbox",
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
            ..ListSandboxesResponse::default()
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
            "provider",
            state
                .providers
                .iter()
                .map(|(id, rec)| (id.clone(), rec.provider.clone())),
            request,
        )?;
        Ok(ListProvidersResponse {
            providers,
            next_page_token,
            ..ListProvidersResponse::default()
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
                        sandbox: sandbox.clone().into(),
                        runtime: rec
                            .sandbox_stats
                            .get(&sandbox.native_ref)
                            .cloned()
                            .unwrap_or_default()
                            .into(),
                        ..SandboxStats::default()
                    })
                    .collect();
                ProviderStats {
                    provider_id: rec.provider.id.clone(),
                    name: rec.provider.name.clone(),
                    drivers: rec.provider.drivers.clone(),
                    host: rec.host.clone().unwrap_or_default().into(),
                    host_sample_age_secs: rec
                        .host_sampled_at
                        .map(|at| at.elapsed().as_secs() as i64)
                        .unwrap_or(0),
                    sandboxes,
                    ..ProviderStats::default()
                }
            })
            .collect();
        StatsResponse {
            providers,
            ..StatsResponse::default()
        }
    }

    /// PEM of the service CA, when TLS is configured.
    pub fn ca_pem(&self) -> Option<String> {
        self.config.ca.as_ref().map(|ca| ca.pem().to_owned())
    }

    fn usable(sandbox: &Sandbox) -> Result<()> {
        match sandbox.state.known() {
            SandboxState::Leased | SandboxState::Preserved | SandboxState::Ready => Ok(()),
            SandboxState::Stopped => Err(Error::failed_precondition(format!(
                "sandbox {} is stopped; start it first",
                sandbox.id
            ))),
            state => Err(Error::failed_precondition(format!(
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
            return Err(Error::invalid_argument("argv is required"));
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
                attempts: 0,
                tx,
                sequence: 0,
                deadline,
                finished: finished.clone(),
            },
        );
        let work = WorkItem::new(
            work_id,
            work_item::Work::Exec(
                ExecWork {
                    exec_id: exec_id.clone(),
                    target: Self::target(&sandbox).into(),
                    request: merged.into(),
                    ..ExecWork::default()
                }
                .into(),
            ),
        );
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
        // The response stream owns the only end of this channel that observes
        // the caller: its drop guard fires when the caller drops the stream,
        // independently of the sender the service keeps in `state.execs` for
        // `ReportExec`.
        let (disconnected, dropped) = oneshot::channel::<()>();
        let service = Arc::clone(self);
        tokio::spawn(async move {
            tokio::select! {
                _ = dropped => service.cancel_disconnected(&exec_id).await,
                _ = finished.cancelled() => {}
            }
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
        if request.kind.as_known().unwrap_or_default() == StreamKind::StreamKindUnspecified {
            return Err(Error::invalid_argument("kind is required"));
        }
        let windows_absolute = request.path.len() > 2
            && request.path.as_bytes()[0].is_ascii_alphabetic()
            && request.path.as_bytes()[1] == b':';
        if !request.path.starts_with('/') && !windows_absolute {
            return Err(Error::invalid_argument("path must be absolute"));
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
                kind: request.kind.as_known().unwrap_or_default(),
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
            ..Ticket::default()
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
            let work = WorkItem::new(
                new_id("wrk"),
                work_item::Work::Copy(
                    CopyWork {
                        target: Self::target(&sandbox).into(),
                        ticket: ticket.to_owned(),
                        path: guest_path.to_owned(),
                        kind: kind.into(),
                        ..CopyWork::default()
                    }
                    .into(),
                ),
            );
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
                return Err(Error::failed_precondition(
                    "only a leased sandbox can be snapshotted",
                ));
            }
            let caps = self.driver_caps(&state, &sandbox)?;
            if !caps.snapshot {
                return Err(Error::failed_precondition(format!(
                    "{}: snapshot unsupported",
                    sandbox.driver
                )));
            }
            let source_template = state
                .templates
                .get(&sandbox.template)
                .cloned()
                .unwrap_or_default();
            let work = WorkItem::new(
                new_id("wrk"),
                work_item::Work::Snapshot(
                    SnapshotWork {
                        target: Self::target(&sandbox).into(),
                        template: request.name.clone(),
                        ..SnapshotWork::default()
                    }
                    .into(),
                ),
            );
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
            source: Some(template_spec::Source::SnapshotOf(sandbox.id.clone())),
            requires: source_template.spec.requires.clone(),
            labels: source_template.spec.labels.clone(),
            ..TemplateSpec::default()
        };
        let template = Template {
            digest: template_digest(&spec),
            spec: spec.into(),
            provider_id: sandbox.provider_id.clone(),
            driver: sandbox.driver.clone(),
            native_ref,
            created_at: now(),
            ..Template::default()
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
                // End its execs the way destroy does: close the callers'
                // streams (a stream left open would hang until the exec
                // deadline, up to an hour) and tell the provider to stop
                // the commands, which otherwise keep running against a
                // sandbox the driver just consumed.
                let open: Vec<(String, String)> = state
                    .execs
                    .iter()
                    .filter(|(_, exec)| exec.sandbox_id == source.id)
                    .map(|(exec_id, exec)| (exec_id.clone(), exec.work_id.clone()))
                    .collect();
                if !open.is_empty() {
                    close_execs(&mut state, &source.id, "sandbox was snapshotted");
                    for (exec_id, work_id) in open {
                        Self::forget_work_locked(&mut state, &source.provider_id, &work_id);
                        self.request_cancel_locked(&mut state, &source, &exec_id);
                    }
                }
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
        let work = WorkItem::new(
            new_id("wrk"),
            work_item::Work::Destroy(
                SandboxWork {
                    sandbox_id: String::new(),
                    driver: template.driver.clone(),
                    native_ref: format!("snapshot:{}", template.native_ref),
                    ..SandboxWork::default()
                }
                .into(),
            ),
        );
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
            .map(|d| (*d.capabilities).clone())
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
                return Err(Error::failed_precondition(format!(
                    "{}: stop/start unsupported",
                    sandbox.driver
                )));
            }
            let target = Self::target(&sandbox);
            let (work, kind) = match op {
                PowerOp::Stop => (
                    WorkItem::new(new_id("wrk"), work_item::Work::Stop(target.into())),
                    Pending::Stop {
                        sandbox_id: id.to_owned(),
                        reply: tx,
                    },
                ),
                PowerOp::Start { forkable } => (
                    WorkItem::new(
                        new_id("wrk"),
                        work_item::Work::Start(
                            StartWork {
                                target: target.into(),
                                forkable,
                                ..StartWork::default()
                            }
                            .into(),
                        ),
                    ),
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
                return Err(Error::failed_precondition(format!(
                    "{}: pause unsupported",
                    sandbox.driver
                )));
            }
            let target = Self::target(&sandbox);
            let (work, kind) = if pause {
                (
                    WorkItem::new(new_id("wrk"), work_item::Work::Pause(target.into())),
                    Pending::Pause {
                        sandbox_id: id.to_owned(),
                        reply: tx,
                    },
                )
            } else {
                (
                    WorkItem::new(new_id("wrk"), work_item::Work::Resume(target.into())),
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
            return Err(Error::invalid_argument("provider name is required"));
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
                    ..DrainProviderResult::default()
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
        self.register_inner(request, None).await
    }

    /// `Register` for a provider authenticated by a client certificate.
    ///
    /// The provider id came from the certificate fingerprint. Re-checking it
    /// under the same lock as the registration closes the window where a
    /// concurrently removed provider could otherwise be re-created by name.
    pub async fn register_enrolled(
        &self,
        request: RegisterRequest,
        provider_id: &str,
    ) -> Result<RegisterResponse> {
        self.register_inner(request, Some(provider_id)).await
    }

    async fn register_inner(
        &self,
        request: RegisterRequest,
        enrolled: Option<&str>,
    ) -> Result<RegisterResponse> {
        if request.name.is_empty() {
            return Err(Error::invalid_argument("name is required"));
        }
        if request.protocol_version != crate::PROTOCOL_VERSION {
            return Err(Error::failed_precondition(format!(
                "agent speaks protocol version {}, this service speaks protocol version {}",
                request.protocol_version,
                crate::PROTOCOL_VERSION
            )));
        }
        let (response, ops) = {
            let mut state = self.state.lock();
            if let Some(provider_id) = enrolled {
                let rec = state.providers.get(provider_id).ok_or_else(|| {
                    Error::new(Code::PermissionDenied, "client certificate is not enrolled")
                })?;
                if rec.provider.name != request.name {
                    return Err(Error::new(
                        Code::PermissionDenied,
                        "provider name does not match the client certificate",
                    ));
                }
            }
            let id = match enrolled {
                Some(provider_id) => provider_id.to_owned(),
                None => state
                    .providers
                    .values()
                    .find(|rec| rec.provider.name == request.name)
                    .map(|rec| rec.provider.id.clone())
                    .unwrap_or_else(|| new_id("prv")),
            };
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
            // Enrollment owns the identity: re-registering keeps the
            // certificates that authenticate this provider.
            let certificates = state
                .providers
                .get(&id)
                .map(|rec| rec.provider.certificates.clone())
                .unwrap_or_default();
            let provider = Provider {
                id: id.clone(),
                name: request.name,
                generation,
                drivers: request.drivers,
                last_seen: now(),
                cordoned,
                certificates,
                ..Provider::default()
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
                    ..RegisterResponse::default()
                },
                ops,
            )
        };
        info!(provider = %response.provider_id, generation = response.generation, "provider registered");
        self.persist(ops).await;
        self.changed.notify_waiters();
        Ok(response)
    }

    // ─── Enrollment and identity ───────────────────────────────────

    /// Mint a single-use join token for provider enrollment.
    pub async fn create_join_token(&self, ttl: Duration) -> Result<CreateJoinTokenResponse> {
        let ca = self.config.ca.as_ref().ok_or_else(|| {
            Error::failed_precondition("enrollment is unavailable: the service has no TLS CA")
        })?;
        let ttl = if ttl.is_zero() {
            Duration::from_secs(3600)
        } else {
            ttl
        };
        let id = new_id("jt");
        let secret = tls::random_hex(32);
        let seconds = i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX);
        let expires_at = now().saturating_add(seconds);
        let record = JoinTokenRecord {
            id: id.clone(),
            secret_sha256: tls::sha256_hex(secret.as_bytes()),
            expires_at,
        };
        self.state
            .lock()
            .join_tokens
            .insert(id.clone(), record.clone());
        self.persist(vec![self.put(Kind::JoinTokens, &id, &record)])
            .await;
        Ok(CreateJoinTokenResponse {
            token: tls::join_token(&id, &secret, ca.sha256_hex()),
            expires_at,
            ..CreateJoinTokenResponse::default()
        })
    }

    /// The provider a client certificate authenticates, by leaf fingerprint.
    ///
    /// Removed providers and revoked certificates (pruned at expiry) are not
    /// found, which is what denies them at the RPC boundary.
    pub fn provider_identity(&self, fingerprint_sha256: &str) -> Result<ProviderIdentity> {
        let t = now();
        let state = self.state.lock();
        state
            .providers
            .values()
            .find(|rec| {
                rec.provider.certificates.iter().any(|certificate| {
                    certificate.sha256 == fingerprint_sha256 && certificate.expires_at > t
                })
            })
            .map(|rec| ProviderIdentity {
                provider_id: rec.provider.id.clone(),
                name: rec.provider.name.clone(),
            })
            .ok_or_else(|| Error::new(Code::PermissionDenied, "client certificate is not enrolled"))
    }

    /// `Enroll`: exchange a join token for a client certificate.
    ///
    /// The certificate is signed before the token is consumed, so a malformed
    /// CSR does not burn a valid token; a token that reaches the store lookup
    /// is removed whether or not binding the provider then succeeds.
    pub async fn enroll(&self, request: EnrollRequest) -> Result<EnrollResponse> {
        let ca = self.config.ca.clone().ok_or_else(|| {
            Error::failed_precondition("enrollment is unavailable: the service has no TLS CA")
        })?;
        let parts = tls::parse_join_token(&request.token)?;
        if !constant_time_eq(parts.ca_sha256.as_bytes(), ca.sha256_hex().as_bytes()) {
            return Err(Error::new(
                Code::Unauthenticated,
                "join token was issued for a different CA",
            ));
        }
        validate_name("provider", &request.name)?;
        let signed = ca.sign_client(&request.csr_pem, &request.name, tls::CLIENT_CERT_LIFETIME)?;
        let ops = {
            let mut state = self.state.lock();
            let t = now();
            let record = state
                .join_tokens
                .get(parts.id)
                .ok_or_else(|| Error::new(Code::Unauthenticated, "unknown or used join token"))?;
            if record.expires_at <= t {
                return Err(Error::new(Code::Unauthenticated, "join token expired"));
            }
            if !constant_time_eq(
                record.secret_sha256.as_bytes(),
                tls::sha256_hex(parts.secret.as_bytes()).as_bytes(),
            ) {
                return Err(Error::new(Code::Unauthenticated, "bad join token"));
            }
            state.join_tokens.remove(parts.id);
            let mut ops = vec![self.remove(Kind::JoinTokens, parts.id)];
            let existing = state
                .providers
                .values()
                .find(|rec| rec.provider.name == request.name)
                .map(|rec| rec.provider.id.clone());
            let id = existing.unwrap_or_else(|| new_id("prv"));
            let (seen, queue, wake) = state
                .providers
                .get(&id)
                .map(|rec| (rec.seen, rec.queue.clone(), rec.wake.clone()))
                .unwrap_or_else(|| {
                    (
                        // Enrolled but never registered: not placeable yet.
                        Instant::now() - self.config.provider_timeout * 2,
                        VecDeque::new(),
                        Arc::new(Notify::new()),
                    )
                });
            let mut provider = state
                .providers
                .get(&id)
                .map(|rec| rec.provider.clone())
                .unwrap_or_else(|| Provider {
                    id: id.clone(),
                    name: request.name.clone(),
                    ..Provider::default()
                });
            provider.name = request.name.clone();
            // Re-enrolling replaces earlier certificates: issuing a new token
            // means these credentials, not accumulated old ones.
            provider.certificates = vec![ProviderCertificate {
                sha256: signed.sha256.clone(),
                expires_at: signed.expires_at,
                ..ProviderCertificate::default()
            }];
            state.providers.insert(
                id.clone(),
                ProviderRec {
                    provider: provider.clone(),
                    seen,
                    queue,
                    in_flight: HashMap::new(),
                    wake,
                    host: None,
                    host_sampled_at: None,
                    sandbox_stats: HashMap::new(),
                },
            );
            ops.push(self.put(Kind::Providers, &id, &provider));
            ops
        };
        self.persist(ops).await;
        self.changed.notify_waiters();
        info!(provider = %request.name, "provider enrolled");
        Ok(EnrollResponse {
            cert_pem: signed.cert_pem,
            ca_pem: ca.pem().to_owned(),
            expires_at: signed.expires_at,
            ..EnrollResponse::default()
        })
    }

    /// `RenewCertificate`: issue a new certificate for an enrolled provider.
    ///
    /// The previous fingerprint stays valid until it expires, so a client
    /// that fails to store the new key keeps working.
    pub async fn renew_certificate(
        &self,
        request: RenewCertificateRequest,
        fingerprint_sha256: &str,
    ) -> Result<RenewCertificateResponse> {
        let ca = self.config.ca.clone().ok_or_else(|| {
            Error::failed_precondition("enrollment is unavailable: the service has no TLS CA")
        })?;
        let (signed, ops) = {
            let mut state = self.state.lock();
            let t = now();
            let id = state
                .providers
                .values()
                .find(|rec| {
                    rec.provider.certificates.iter().any(|certificate| {
                        certificate.sha256 == fingerprint_sha256 && certificate.expires_at > t
                    })
                })
                .map(|rec| rec.provider.id.clone())
                .ok_or_else(|| {
                    Error::new(Code::PermissionDenied, "client certificate is not enrolled")
                })?;
            let rec = state.providers.get_mut(&id).expect("resolved above");
            let signed = ca.sign_client(
                &request.csr_pem,
                &rec.provider.name,
                tls::CLIENT_CERT_LIFETIME,
            )?;
            rec.provider
                .certificates
                .retain(|certificate| certificate.expires_at > t);
            rec.provider.certificates.push(ProviderCertificate {
                sha256: signed.sha256.clone(),
                expires_at: signed.expires_at,
                ..ProviderCertificate::default()
            });
            rec.seen = Instant::now();
            rec.provider.last_seen = t;
            let provider = rec.provider.clone();
            (signed, vec![self.put(Kind::Providers, &id, &provider)])
        };
        self.persist(ops).await;
        Ok(RenewCertificateResponse {
            cert_pem: signed.cert_pem,
            expires_at: signed.expires_at,
            ..RenewCertificateResponse::default()
        })
    }

    /// Remove a provider, its certificates and its machine records.
    ///
    /// The host's machines are unreachable once it can no longer authenticate,
    /// so their records and leases are dropped instead of pinning names and
    /// capacity until a reconcile that can never come.
    pub async fn delete_provider(&self, name: &str) -> Result<()> {
        let ops = {
            let mut state = self.state.lock();
            let Some(id) = state
                .providers
                .values()
                .find(|rec| rec.provider.name == name)
                .map(|rec| rec.provider.id.clone())
            else {
                return Err(Error::not_found(format!(
                    "provider `{name}` is not registered"
                )));
            };
            state.providers.remove(&id);
            let mut ops = vec![self.remove(Kind::Providers, &id)];
            let stale: Vec<String> = state
                .pending
                .iter()
                .filter(|(_, work)| work.provider_id == id)
                .map(|(work_id, _)| work_id.clone())
                .collect();
            for work_id in stale {
                self.complete_locked(
                    &mut state,
                    &work_id,
                    Err(Error::unavailable("provider was removed")),
                    &mut ops,
                );
            }
            let sandboxes: Vec<String> = state
                .sandboxes
                .values()
                .filter(|sandbox| sandbox.provider_id == id)
                .map(|sandbox| sandbox.id.clone())
                .collect();
            let leases: Vec<String> = state
                .leases
                .values()
                .filter(|lease| sandboxes.contains(&lease.sandbox_id))
                .map(|lease| lease.id.clone())
                .collect();
            for lease_id in leases {
                if let Some(lease) = state.leases.remove(&lease_id) {
                    state.lease_by_key.remove(&lease.idempotency_key);
                    state.injected.remove(&lease_id);
                    ops.push(self.remove(Kind::Leases, &lease_id));
                }
            }
            for sandbox_id in sandboxes {
                self.destroy_locked(&mut state, &sandbox_id, &mut ops);
            }
            ops
        };
        self.persist(ops).await;
        self.changed.notify_waiters();
        info!(provider = name, "provider removed");
        Ok(())
    }

    /// `Heartbeat`.
    pub fn heartbeat(&self, request: HeartbeatRequest) -> Result<()> {
        let mut state = self.state.lock();
        self.fence(&state, &request.provider_id, request.generation)?;
        if let Some(rec) = state.providers.get_mut(&request.provider_id) {
            rec.seen = Instant::now();
            rec.provider.last_seen = now();
            if let Some(host) = request.host.into_option() {
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
                let mut delivered: Vec<WorkItem> = Vec::new();
                let (wake, next) = {
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
                    while delivered.len() < batch {
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
                        delivered.push(item);
                    }
                    let next = rec.in_flight.values().map(|item| item.visible_at).min();
                    (rec.wake.clone(), next)
                };
                if !delivered.is_empty() {
                    // Stamp the attempt on every exec this delivery carries:
                    // the record of how many times the work went out, kept
                    // with the exec. Re-execution itself is fenced by the
                    // agent's durable started-set (see `agent.rs`).
                    let exec_ids: Vec<String> = delivered
                        .iter()
                        .filter_map(|item| match state.pending.get(&item.id) {
                            Some(pending) => match &pending.kind {
                                Pending::Exec { exec_id } => Some(exec_id.clone()),
                                _ => None,
                            },
                            None => None,
                        })
                        .collect();
                    for exec_id in &exec_ids {
                        if let Some(exec) = state.execs.get_mut(exec_id) {
                            exec.attempts += 1;
                            if exec.attempts > 1 {
                                warn!(
                                    exec_id,
                                    attempts = exec.attempts,
                                    "exec work redelivered; the agent fences re-execution"
                                );
                            }
                        }
                    }
                    return Ok(PollResponse {
                        work: delivered,
                        ..PollResponse::default()
                    });
                }
                (wake, next)
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
                        let work = WorkItem::new(
                            new_id("wrk"),
                            work_item::Work::Destroy(
                                SandboxWork {
                                    sandbox_id: sandbox_id.clone(),
                                    driver,
                                    native_ref,
                                    ..SandboxWork::default()
                                }
                                .into(),
                            ),
                        );
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
                    }
                    .into();
                    ops.push(self.put(Kind::Sandboxes, &sandbox_id, &*sandbox));
                    state.created_at.remove(&sandbox_id);
                }
                Err(error) => {
                    self.config.metrics.record_create_failure("reported");
                    self.fail_locked(state, &sandbox_id, crate::error::message(&error), ops);
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
                    sandbox.state = SandboxState::Paused.into();
                    ops.push(self.put(Kind::Sandboxes, &sandbox_id, &*sandbox));
                }
                let _ = reply.send(result);
            }
            Pending::Stop { sandbox_id, reply } => {
                if result.is_ok()
                    && let Some(sandbox) = state.sandboxes.get_mut(&sandbox_id)
                {
                    sandbox.state = SandboxState::Stopped.into();
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
                    }
                    .into();
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
                    }
                    .into();
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
            let exit = matches!(event.event, Some(exec_event::Event::ExitCode(_)));
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
            //
            // A template's `native_ref` is its fork base (SmolVM's golden):
            // a live machine with no sandbox record. It is never an orphan,
            // whichever provider reports it. Two agents can share one host's
            // engine home and each lists every machine there, and the base
            // belongs to the template rather than to the agent that happened
            // to create it. Reaping it wedges every later fork ("cannot be
            // re-launched while … live clone(s)"); the owning provider
            // destroys it when the template is deleted, and nowhere else.
            //
            // Narrowing the driver's inventory to machines carrying this
            // agent's ownership marker would not do instead: the marker names
            // the machine, not the agent, and a provider must still see the
            // other agent's genuinely orphaned machines to reap them.
            let goldens: HashSet<&str> = state
                .templates
                .values()
                .map(|t| t.native_ref.as_str())
                .filter(|native_ref| !native_ref.is_empty())
                .collect();
            let mut destroy = Vec::new();
            for item in &request.sandboxes {
                if goldens.contains(item.native_ref.as_str()) {
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
                        ..SandboxWork::default()
                    });
                }
            }
            self.config
                .metrics
                .record_reconcile_orphans(destroy.len() as u64);
            (
                ReconcileResponse {
                    destroy,
                    ..ReconcileResponse::default()
                },
                ops,
            )
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

/// A new sandbox's effective size: the request's fields, with the driver's
/// advertised defaults filling the zeros.
fn effective_shape(state: &State, provider_id: &str, driver: &str, requested: &Shape) -> Shape {
    let defaults = state
        .providers
        .get(provider_id)
        .and_then(|rec| {
            rec.provider
                .drivers
                .iter()
                .find(|info| info.driver == driver)
        })
        .map(|info| info.capabilities.default_shape.clone())
        .unwrap_or_default();
    let pick = |requested: u32, default: u32| {
        if requested == 0 { default } else { requested }
    };
    Shape {
        cpus: pick(requested.cpus, defaults.cpus),
        memory_mib: pick(requested.memory_mib, defaults.memory_mib),
        disk_gib: pick(requested.disk_gib, defaults.disk_gib),
        ..Shape::default()
    }
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

    fn encode_token(token: &str) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(token)
    }

    #[test]
    fn pages_are_ordered_and_carry_a_token_until_exhausted() {
        let mut request = ListRequest::default();
        let (page, token) = paginate("widget", items(250), &request).unwrap();
        assert_eq!(page, (0..100usize).collect::<Vec<_>>());
        assert!(!token.is_empty());
        request.page_token = token;
        let (page, token) = paginate("widget", items(250), &request).unwrap();
        assert_eq!(page, (100..200usize).collect::<Vec<_>>());
        assert!(!token.is_empty());
        request.page_token = token;
        let (page, token) = paginate("widget", items(250), &request).unwrap();
        assert_eq!(page, (200..250usize).collect::<Vec<_>>());
        assert!(token.is_empty());
    }

    #[test]
    fn page_size_is_capped_and_tokens_are_validated() {
        let request = ListRequest {
            page_size: 5000,
            page_token: String::new(),
            ..ListRequest::default()
        };
        let (page, _) = paginate("widget", items(1500), &request).unwrap();
        assert_eq!(page.len(), MAX_PAGE_SIZE as usize);

        let request = ListRequest {
            page_token: "not base64!".into(),
            ..ListRequest::default()
        };
        let error = paginate("widget", items(3), &request).unwrap_err();
        assert_eq!(error.code, Code::InvalidArgument);
    }

    #[test]
    fn a_bogus_page_token_is_rejected_instead_of_restarting_at_page_one() {
        // `base64url("not-a-sandbox")`, the token the end-to-end run used.
        let request = ListRequest {
            page_size: 2,
            page_token: "bm90LWEtc2FuZGJveA".into(),
            ..ListRequest::default()
        };
        let error = paginate("sandbox", items(4), &request).unwrap_err();
        assert_eq!(
            error.code,
            Code::InvalidArgument,
            "{}",
            crate::error::message(&error)
        );

        // A cursor minted for another listing is not a cursor for this one.
        let first = ListRequest {
            page_size: 2,
            page_token: String::new(),
            ..ListRequest::default()
        };
        let (_, token) = paginate("template", items(4), &first).unwrap();
        let request = ListRequest {
            page_size: 2,
            page_token: token,
            ..ListRequest::default()
        };
        let error = paginate("sandbox", items(4), &request).unwrap_err();
        assert_eq!(
            error.code,
            Code::InvalidArgument,
            "{}",
            crate::error::message(&error)
        );
    }

    #[test]
    fn a_token_whose_id_disappeared_still_resumes_after_it() {
        let request = ListRequest {
            page_size: 2,
            page_token: String::new(),
            ..ListRequest::default()
        };
        let (_, token) = paginate("widget", items(4), &request).unwrap();
        // The token names `id-0001`, which is gone by the second page.
        let remaining: Vec<_> = items(4)
            .into_iter()
            .filter(|(id, _)| id != "id-0001")
            .collect();
        let request = ListRequest {
            page_size: 2,
            page_token: token,
            ..ListRequest::default()
        };
        let (page, _) = paginate("widget", remaining, &request).unwrap();
        assert_eq!(page, vec![2, 3]);
    }

    #[test]
    fn a_page_token_with_a_missing_value_is_rejected() {
        let request = ListRequest {
            page_token: encode_token("v1:sandbox:"),
            ..ListRequest::default()
        };
        let error = paginate("sandbox", items(3), &request).unwrap_err();
        assert_eq!(error.code, Code::InvalidArgument);
    }

    /// Pentest W4-F6 / W5-F2 (service half): every delivery of an exec's
    /// work item is counted on the exec record, so a redelivery — the
    /// attempt that must never become a second execution — is recorded
    /// where the exec lives. (Re-execution itself is fenced by the
    /// agent's durable started-set; see `agent.rs`.)
    #[tokio::test]
    async fn exec_deliveries_are_counted_on_the_exec_record() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = ServiceConfig::new(dir.path().join("blobs"));
        config.delivery_timeout = Duration::from_millis(50);
        let service = Service::open(crate::store::open("memory:").await.unwrap(), config)
            .await
            .unwrap();
        let digest = format!("sha256:{}", "0".repeat(64));
        service
            .put_template(TemplateSpec {
                name: "alpine".into(),
                os: "linux".into(),
                artifacts: vec![Artifact {
                    format: Artifact::OCI_IMAGE.into(),
                    arch: std::env::consts::ARCH.into(),
                    r#ref: format!("ghcr.io/preloop/alpine@{digest}"),
                    digest,
                    ..Artifact::default()
                }],
                ..TemplateSpec::default()
            })
            .await
            .unwrap();
        let provider = service
            .register(RegisterRequest {
                name: "manual".into(),
                drivers: vec![DriverInfo {
                    driver: "fake".into(),
                    capabilities: crate::drivers::fake::FakeDriver::full_capabilities().into(),
                    ..DriverInfo::default()
                }],
                protocol_version: crate::PROTOCOL_VERSION,
                ..RegisterRequest::default()
            })
            .await
            .unwrap();
        async fn poll_work(service: &Service, provider: &RegisterResponse) -> Vec<WorkItem> {
            service
                .poll(PollRequest {
                    provider_id: provider.provider_id.clone(),
                    generation: provider.generation,
                    wait_secs: 0,
                    ..PollRequest::default()
                })
                .await
                .unwrap()
                .work
        }
        // Provision one sandbox by hand (no agent): queue the create,
        // report it, then pick the lease up with the same key.
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
        let create = poll_work(&service, &provider).await;
        assert_eq!(create.len(), 1);
        service
            .report(ReportRequest {
                provider_id: provider.provider_id.clone(),
                generation: provider.generation,
                work_id: create[0].id.clone(),
                ok: true,
                native_ref: "psbx-unit-1".into(),
                error: String::new(),
                ..ReportRequest::default()
            })
            .await
            .unwrap();
        let lease = service
            .acquire(AcquireRequest {
                idempotency_key: "k".into(),
                template: "alpine".into(),
                ..AcquireRequest::default()
            })
            .await
            .unwrap();
        let _stream = service
            .exec(ExecRequest {
                sandbox_id: lease.sandbox.id.clone(),
                lease_id: lease.lease.id.clone(),
                argv: vec!["true".into()],
                ..ExecRequest::default()
            })
            .unwrap();
        let delivered = poll_work(&service, &provider).await;
        assert_eq!(delivered.len(), 1);
        let exec_id = match delivered[0].work.as_ref().expect("exec work") {
            work_item::Work::Exec(exec) => exec.exec_id.clone(),
            other => panic!("expected exec work, got {other:?}"),
        };
        let attempts = |service: &Service| service.state.lock().execs[&exec_id].attempts;
        assert_eq!(attempts(&service), 1, "the first delivery is attempt 1");
        // The report never arrives; past the visibility timeout the work
        // is delivered again and the record shows the second attempt.
        tokio::time::sleep(Duration::from_millis(80)).await;
        let redelivered = poll_work(&service, &provider).await;
        assert_eq!(redelivered.len(), 1);
        assert_eq!(redelivered[0].id, delivered[0].id, "same work, redelivered");
        assert_eq!(attempts(&service), 2, "the redelivery is attempt 2");
    }
}
