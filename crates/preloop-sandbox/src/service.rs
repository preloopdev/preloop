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
use tracing::{info, warn};

use crate::api::*;
use crate::connect::{Code, Error, Result};
use crate::store::{Kind, Store};

/// Tunables.
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    /// Default lease TTL.
    pub lease_ttl: Duration,
    /// How long an acquired-but-never-returned lease (one whose `Acquire` is
    /// still provisioning, or timed out) may live before maintenance releases
    /// it. A waiting `Acquire` keeps its lease past this window.
    pub unclaimed_lease_ttl: Duration,
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
    /// How long a data-plane ticket stays valid.
    pub ticket_ttl: Duration,
    /// How long a caller waits for a provider to report a snapshot, copy,
    /// pause or resume before the request is failed.
    pub work_timeout: Duration,
    /// A provider silent this many times `provider_timeout` is treated as
    /// lost: its sandboxes fail, which frees leases and closes exec streams.
    pub provider_lost_factor: u32,
}

impl ServiceConfig {
    /// Defaults with blobs under `blob_dir`.
    pub fn new(blob_dir: PathBuf) -> Self {
        Self {
            lease_ttl: Duration::from_secs(3600),
            unclaimed_lease_ttl: Duration::from_secs(600),
            acquire_wait: Duration::from_secs(900),
            provider_timeout: Duration::from_secs(45),
            create_timeout: Duration::from_secs(1800),
            preserve_ttl: Duration::from_secs(600),
            blob_dir,
            ticket_ttl: Duration::from_secs(300),
            work_timeout: Duration::from_secs(900),
            provider_lost_factor: 3,
        }
    }
}

/// Pause between warm-pool refills after a creation failure.
const POOL_FAILURE_BACKOFF: Duration = Duration::from_secs(10);

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

/// Exec env keys become shell assignments in argv-only drivers; reject
/// anything that is not a POSIX identifier before it reaches a driver.
fn validate_env(env: &BTreeMap<String, String>) -> Result<()> {
    for key in env.keys() {
        if !crate::driver::env_key_is_identifier(key) {
            return Err(Error::invalid(format!(
                "invalid environment variable name `{key}`"
            )));
        }
    }
    Ok(())
}

fn new_id(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

/// Copy a blob without blocking a Tokio worker for the length of the I/O.
async fn copy_off_runtime(from: PathBuf, to: PathBuf, what: &str) -> Result<()> {
    tokio::task::spawn_blocking(move || std::fs::copy(&from, &to))
        .await
        .map_err(|error| Error::internal(format!("{what}: {error}")))?
        .map(|_| ())
        .map_err(|error| Error::internal(format!("{what}: {error}")))
}

/// Build the inject tar readable only by its owner: its payload can carry
/// secrets, and the guest owner is the only intended reader.
fn build_inject_tar(tar_path: &Path, files: &[FileSpec]) -> Result<()> {
    use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};

    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(tar_path)
        .map_err(|e| Error::internal(format!("inject tar: {e}")))?;
    // `mode` only applies to a newly created file; a stale tar left by an
    // earlier attempt must not keep its wider mode.
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .map_err(|e| Error::internal(format!("inject tar: {e}")))?;
    let mut builder = tar::Builder::new(file);
    for spec in files {
        let path = spec.path.trim_start_matches('/');
        if path.is_empty() || path.split('/').any(|p| p == "..") {
            return Err(Error::invalid(format!("bad inject path `{}`", spec.path)));
        }
        let mut header = tar::Header::new_gnu();
        header.set_size(spec.data.len() as u64);
        header.set_mode(if spec.mode == 0 { 0o600 } else { spec.mode });
        header.set_mtime(now() as u64);
        header.set_cksum();
        builder
            .append_data(&mut header, path, spec.data.as_slice())
            .map_err(|e| Error::internal(format!("inject tar: {e}")))?;
    }
    builder
        .finish()
        .map_err(|e| Error::internal(format!("inject tar: {e}")))?;
    Ok(())
}

/// sha256 of the canonical JSON of a template spec.
pub fn template_digest(spec: &TemplateSpec) -> String {
    let canonical = serde_json::to_vec(spec).unwrap_or_default();
    format!("sha256:{}", hex::encode(Sha256::digest(&canonical)))
}

/// Revision of a pool's full placement: its configuration plus the template
/// digest it references. A warm member built for another revision must not
/// serve a new acquire.
fn pool_revision(state: &State, pool: &PoolSpec) -> String {
    let template = state
        .templates
        .get(&pool.template)
        .map(|template| template.digest.clone())
        .unwrap_or_default();
    let canonical = serde_json::to_vec(&(pool, template)).unwrap_or_default();
    format!("sha256:{}", hex::encode(Sha256::digest(&canonical)))
}

/// State a sandbox returns to when its Pause/Stop is undone with no lease.
fn restored_state(sandbox: &Sandbox) -> SandboxState {
    match sandbox.prior_state {
        // A preserved debug sandbox stays preserved (its deadline is kept).
        SandboxState::Preserved => SandboxState::Preserved,
        // A lease that was released while powered off: a pool member goes
        // back to the warm set; any other sandbox stays a debug hold.
        SandboxState::Leased if sandbox.pool.is_empty() => SandboxState::Preserved,
        _ => SandboxState::Ready,
    }
}

#[derive(Debug)]
enum Pending {
    Create {
        sandbox_id: String,
        incarnation: u64,
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
        lease_id: String,
        expected: SandboxState,
        reply: oneshot::Sender<Result<String>>,
    },
    Stop {
        sandbox_id: String,
        lease_id: String,
        expected: SandboxState,
        reply: oneshot::Sender<Result<String>>,
    },
    Start {
        sandbox_id: String,
        lease_id: String,
        expected: SandboxState,
        forkable: bool,
        reply: oneshot::Sender<Result<String>>,
    },
    Resume {
        sandbox_id: String,
        lease_id: String,
        expected: SandboxState,
        reply: oneshot::Sender<Result<String>>,
    },
}

struct PendingWork {
    provider_id: String,
    kind: Pending,
}

struct ProviderRec {
    provider: Provider,
    seen: Instant,
    queue: VecDeque<WorkItem>,
    wake: Arc<Notify>,
}

/// Data-plane blob for one ticket.
struct Blob {
    sandbox_id: String,
    kind: StreamKind,
    guest_path: String,
    token: String,
    expires_at: i64,
    path: PathBuf,
    /// Set by the first `claim_ticket`: a ticket is a single-use capability.
    claimed: bool,
}

/// Per-sandbox creation parameters kept for (re)provisioning and audit.
#[derive(Clone)]
struct Placement {
    template: Template,
    shape: Shape,
    network: NetworkPolicy,
    mounts: Vec<Mount>,
    sockets: Vec<SocketMount>,
    options: BTreeMap<String, String>,
    requires: Requirements,
    snapshot_capable: bool,
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
/// sandbox cannot leave a caller streaming forever. Returns the persist ops.
fn close_execs(state: &mut State, sandbox_id: &str, reason: &str) {
    let stale: Vec<String> = state
        .execs
        .iter()
        .filter(|(_, (owner, _))| owner == sandbox_id)
        .map(|(exec_id, _)| exec_id.clone())
        .collect();
    for exec_id in stale {
        if let Some((_, tx)) = state.execs.remove(&exec_id) {
            let _ = tx.try_send(Err(Error::unavailable(reason.to_owned())));
        }
    }
}

/// A lifecycle completion is applied only to the same sandbox incarnation
/// that started the operation, under the same lease and in the state the
/// operation expected to leave behind.
fn still_in_state(state: &State, sandbox_id: &str, lease_id: &str, expected: SandboxState) -> bool {
    state
        .sandboxes
        .get(sandbox_id)
        .is_some_and(|sandbox| sandbox.lease_id == lease_id && sandbox.state == expected)
}

#[derive(Default)]
struct State {
    templates: BTreeMap<String, Template>,
    pools: BTreeMap<String, PoolSpec>,
    sandboxes: BTreeMap<String, Sandbox>,
    created_at: HashMap<String, Instant>,
    leases: BTreeMap<String, Lease>,
    lease_by_key: HashMap<String, String>,
    /// In-process `Acquire` calls waiting for a lease, by idempotency key.
    /// Maintenance must not expire a lease while its caller is still waiting.
    waiting: HashMap<String, usize>,
    injected: HashSet<String>,
    providers: BTreeMap<String, ProviderRec>,
    pending: HashMap<String, PendingWork>,
    execs: HashMap<String, (String, mpsc::Sender<Result<ExecEvent>>)>,
    blobs: HashMap<String, Blob>,
    /// Last creation failure per pool, to back off refills.
    pool_backoff: HashMap<String, Instant>,
    /// Sandboxes with a snapshot in flight: release must not destroy their
    /// base, and a second snapshot is rejected.
    busy: HashSet<String>,
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
    /// Serializes file injection per lease: concurrent same-key acquires
    /// would otherwise stage and unlink one shared tar.
    inject_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

/// An `Acquire` call waiting in this process, keyed by idempotency key.
///
/// Maintenance skips an expired lease while its waiter is alive: a boot or a
/// golden unpack can outlast the unclaimed lease window.
struct AcquireWaiter {
    service: Arc<Service>,
    key: String,
}

impl AcquireWaiter {
    fn register(service: &Arc<Service>, key: &str) -> Self {
        *service
            .state
            .lock()
            .waiting
            .entry(key.to_owned())
            .or_default() += 1;
        Self {
            service: service.clone(),
            key: key.to_owned(),
        }
    }
}

impl Drop for AcquireWaiter {
    fn drop(&mut self) {
        let mut state = self.service.state.lock();
        if let Some(count) = state.waiting.get_mut(&self.key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.waiting.remove(&self.key);
            }
        }
    }
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

/// Holds a sandbox in the service's busy set while a snapshot is in flight,
/// and releases it however the call exits.
struct SnapshotBusy<'a> {
    service: &'a Service,
    sandbox_id: String,
}

impl Drop for SnapshotBusy<'_> {
    fn drop(&mut self) {
        self.service.state.lock().busy.remove(&self.sandbox_id);
    }
}

impl Service {
    /// Construct and restore from the store.
    pub async fn open(store: Arc<dyn Store>, config: ServiceConfig) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&config.blob_dir)
            .map_err(|e| Error::internal(format!("blob dir: {e}")))?;
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
        for provider in load::<Provider>(&*store, Kind::Providers).await? {
            state.providers.insert(
                provider.id.clone(),
                ProviderRec {
                    provider,
                    // Restored providers must re-register (bumping their
                    // generation) before they are placed on.
                    seen: Instant::now() - config.provider_timeout * 2,
                    queue: VecDeque::new(),
                    wake: Arc::new(Notify::new()),
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
            inject_locks: Mutex::new(HashMap::new()),
        });
        service.sweep_lease_orphans().await;
        Ok(service)
    }

    /// Fail restored sandboxes whose lease row is missing.
    ///
    /// A crash between persisting a sandbox and its lease — or a lease
    /// deleted under a live sandbox — would otherwise leave a record holding
    /// a machine forever, with no lease to expire.
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
                // Best-effort store: in-memory state stays authoritative.
                Err(error) => warn!(%error, "sandbox store write failed"),
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
            // Expired leases and preservation windows. A lease whose caller is
            // still waiting in this process is not expired: a real boot or a
            // golden unpack can outlast the unclaimed window.
            let waiting = &state.waiting;
            let expired: Vec<String> = state
                .leases
                .values()
                .filter(|lease| {
                    lease.expires_at <= t && !waiting.contains_key(&lease.idempotency_key)
                })
                // A snapshot in flight still owns the base: do not destroy it
                // from under the provider.
                .filter(|lease| !state.busy.contains(&lease.sandbox_id))
                .map(|lease| lease.id.clone())
                .collect();
            for lease_id in expired {
                info!(%lease_id, "lease expired; destroying sandbox");
                self.release_locked(&mut state, &lease_id, ReleaseMode::Destroy, 0, &mut ops);
            }
            // Stop deliberately keeps the lease; a stopped sandbox holds no
            // host capacity, but it does hold disk: the create sweep below
            // still applies to its record. Paused/stopped sandboxes whose
            // lease was preserved (or that were preserved before pausing)
            // expire with the same window.
            let preserved: Vec<String> = state
                .sandboxes
                .values()
                .filter(|s| {
                    s.lease_id.is_empty()
                        && matches!(
                            s.state,
                            SandboxState::Preserved | SandboxState::Paused | SandboxState::Stopped
                        )
                        && s.preserve_until > 0
                        && s.preserve_until <= t
                })
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
            let orphaned: Vec<String> = state
                .sandboxes
                .values()
                .filter(|s| silent.contains(&s.provider_id) && !s.state.is_terminal())
                .map(|s| s.id.clone())
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
        // Retire idle members built for an older pool/template revision: they
        // no longer satisfy the configuration a new acquire expects.
        let revision = pool_revision(state, pool);
        let stale: Vec<String> = state
            .sandboxes
            .values()
            .filter(|s| {
                s.pool == pool.name
                    && s.lease_id.is_empty()
                    && s.state != SandboxState::Terminating
                    && !s.state.is_terminal()
                    && s.pool_revision != revision
            })
            .map(|s| s.id.clone())
            .collect();
        for id in stale {
            info!(sandbox = %id, pool = %pool.name, "retiring a warm member built for an older pool revision");
            self.destroy_locked(state, &id, ops);
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
                    && s.pool_revision == revision
                    && matches!(s.state, SandboxState::Ready | SandboxState::Provisioning)
            })
            .count() as u32;
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
                return;
            };
            match self.create_locked(state, placement, &pool.name, Purpose::Runner, "", ops) {
                Ok(_) => {}
                // No capacity right now: try again next pass.
                Err(_) => return,
            }
        }
    }

    // ─── Templates and pools ───────────────────────────────────────

    /// `PutTemplate`.
    pub async fn put_template(&self, spec: TemplateSpec) -> Result<Template> {
        validate_name("template", &spec.name)?;
        let sources = [
            spec.image.is_some() || !spec.images_by_arch.is_empty(),
            spec.recipe.is_some(),
            spec.snapshot_of.is_some(),
        ];
        if sources.iter().filter(|set| **set).count() != 1 {
            return Err(Error::invalid(
                "exactly one template source is required: image/imagesByArch, recipe, or snapshotOf",
            ));
        }
        if spec.snapshot_of.is_some() {
            return Err(Error::invalid("snapshot templates are created by Snapshot"));
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
            // A fork base must outlive every clone: deleting it while a
            // sandbox still starts from it invalidates that machine.
            if let Some(sandbox) = state
                .sandboxes
                .values()
                .find(|sandbox| sandbox.template == name && !sandbox.state.is_terminal())
            {
                return Err(Error::precondition(format!(
                    "template `{name}` is still used by sandbox `{}`",
                    sandbox.id
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
    ) -> Result<(Placement, String, String)> {
        let template = state
            .templates
            .get(&pool.template)
            .cloned()
            .ok_or_else(|| Error::not_found(format!("template `{}` not found", pool.template)))?;
        let placement = Placement {
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
                let placement = Placement {
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

    /// Pick `(provider_id, driver)`: filter on capabilities and capacity,
    /// then prefer the least-loaded owner.
    fn place(
        &self,
        state: &State,
        placement: &Placement,
        require_online: bool,
    ) -> Result<(String, String)> {
        let template = &placement.template;
        let requires = &placement.requires;
        let os = if requires.os.is_empty() {
            template.spec.os.as_str()
        } else {
            requires.os.as_str()
        };
        let mut unmet = Vec::new();
        let mut full = false;
        let mut best: Option<(usize, String, String)> = None;
        for (provider_id, rec) in &state.providers {
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
                if placement.snapshot_capable && !caps.snapshot {
                    reasons.push("capability=snapshot".into());
                }
                if !placement.sockets.is_empty() && !caps.socket_mounts {
                    reasons.push("capability=socket_mounts".into());
                }
                if placement.mounts.iter().any(|_| !caps.host_mounts) {
                    reasons.push("capability=host_mounts".into());
                }
                let mode = placement.network.mode;
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
                if best.as_ref().is_none_or(|(load, _, _)| live < *load) {
                    best = Some((live, provider_id.clone(), driver.driver.clone()));
                }
            }
        }
        match best {
            Some((_, provider, driver)) => Ok((provider, driver)),
            None if full => Err(Error::new(
                Code::ResourceExhausted,
                "every matching provider is at capacity",
            )),
            None if unmet.is_empty() => Err(Error::unavailable(
                "no provider is online for this template",
            )),
            None => Err(Error::precondition(format!(
                "no provider satisfies template `{}`: {}",
                template.spec.name,
                unmet.join("; ")
            ))),
        }
    }

    fn create_locked(
        &self,
        state: &mut State,
        (placement, provider_id, driver): (Placement, String, String),
        pool: &str,
        purpose: Purpose,
        name: &str,
        ops: &mut Vec<Op>,
    ) -> Result<String> {
        let id = new_id("sbx");
        let revision = if pool.is_empty() {
            String::new()
        } else {
            state
                .pools
                .get(pool)
                .map(|spec| pool_revision(state, spec))
                .unwrap_or_default()
        };
        let sandbox = Sandbox {
            id: id.clone(),
            name: name.to_owned(),
            template: placement.template.spec.name.clone(),
            pool: pool.to_owned(),
            provider_id: provider_id.clone(),
            driver: driver.clone(),
            incarnation: 1,
            state: SandboxState::Provisioning,
            purpose,
            created_at: now(),
            pool_revision: revision,
            ..Sandbox::default()
        };
        let sandbox_driver = driver.clone();
        let work = WorkItem {
            id: new_id("wrk"),
            create: Some(CreateWork {
                sandbox_id: id.clone(),
                name: name.to_owned(),
                incarnation: 1,
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
                incarnation: 1,
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
        let rec = state
            .providers
            .get_mut(provider_id)
            .ok_or_else(|| Error::unavailable(format!("provider {provider_id} is gone")))?;
        state.pending.insert(
            work.id.clone(),
            PendingWork {
                provider_id: provider_id.to_owned(),
                kind,
            },
        );
        rec.queue.push_back(work);
        rec.wake.notify_one();
        Ok(())
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
            ops.push(self.remove(Kind::Sandboxes, id));
        }
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
        // second preserve extends that window instead of destroying. A paused
        // or stopped debugging machine keeps its power state (and must not be
        // woken or destroyed just because it is no longer `Leased`).
        if preserve
            && !failed
            && state.sandboxes.get(&lease.sandbox_id).is_some_and(|s| {
                matches!(
                    s.state,
                    SandboxState::Leased
                        | SandboxState::Paused
                        | SandboxState::Stopped
                        | SandboxState::Preserved
                )
            })
        {
            let ttl = if preserve_ttl == 0 {
                self.config.preserve_ttl.as_secs()
            } else {
                preserve_ttl
            };
            let until = now() + ttl as i64;
            if let Some(sandbox) = state.sandboxes.get_mut(&lease.sandbox_id) {
                if sandbox.state == SandboxState::Leased {
                    sandbox.state = SandboxState::Preserved;
                }
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
        if request.idempotency_key.is_empty() {
            return Err(Error::invalid("idempotencyKey is required"));
        }
        if request.pool.is_empty() == request.template.is_empty() {
            return Err(Error::invalid(
                "exactly one of pool or template is required",
            ));
        }
        if !request.pool.is_empty() {
            // The pool path uses the pool's placement; silently dropping these
            // would hand back a sandbox that ignores what the caller asked for.
            let mut ignored = Vec::new();
            if request.shape != Shape::default() {
                ignored.push("shape");
            }
            if request.network != NetworkPolicy::default() {
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
                return Err(Error::invalid(format!(
                    "pool acquires cannot set {}; set these on the pool, or acquire from the template",
                    ignored.join(", ")
                )));
            }
            // A build sandbox must be started snapshot-capable, and a pool
            // placement is fixed: take build sandboxes from the template.
            if request.purpose == Purpose::Build {
                return Err(Error::invalid(
                    "pool acquires cannot be build sandboxes; acquire from the template",
                ));
            }
        }
        // From here until the response is returned this call counts as a
        // waiter, so maintenance will not expire the lease it is provisioning.
        let _waiter = AcquireWaiter::register(self, &request.idempotency_key);
        let wait = match request.wait_secs {
            None => self.config.acquire_wait,
            // An explicit zero means "report the current state, do not block".
            Some(0) => Duration::ZERO,
            Some(secs) => Duration::from_secs(secs),
        };
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.try_acquire(&request).await? {
                Step::Ready(ready) => {
                    let (lease, sandbox) = *ready;
                    self.inject(&lease, &sandbox, &request.inject).await?;
                    let lease = self.claim(&lease).await?;
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

    async fn try_acquire(&self, request: &AcquireRequest) -> Result<Step> {
        let mut ops = Vec::new();
        let step = {
            let mut state = self.state.lock();
            self.try_acquire_locked(&mut state, request, &mut ops)
        };
        if !ops.is_empty() {
            self.persist(ops).await;
            self.changed.notify_waiters();
        }
        step
    }

    fn try_acquire_locked(
        &self,
        state: &mut State,
        request: &AcquireRequest,
        ops: &mut Vec<Op>,
    ) -> Result<Step> {
        if let Some(lease_id) = state.lease_by_key.get(&request.idempotency_key).cloned() {
            let lease = state.leases[&lease_id].clone();
            let sandbox = state
                .sandboxes
                .get(&lease.sandbox_id)
                .cloned()
                .ok_or_else(|| Error::not_found("leased sandbox is gone"))?;
            return match sandbox.state {
                SandboxState::Leased
                | SandboxState::Paused
                | SandboxState::Stopped
                | SandboxState::Preserved => Ok(Step::Ready(Box::new((lease, sandbox)))),
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
            let revision = pool_revision(state, &pool);
            let warm = state
                .sandboxes
                .values()
                .filter(|s| {
                    s.pool == pool.name
                        && s.lease_id.is_empty()
                        && s.pool_revision == revision
                        && s.state == SandboxState::Ready
                })
                .filter(|s| sandbox_meets(state, s, &request.requires))
                // Oldest first, so warm members rotate and none goes stale.
                .min_by_key(|s| (s.created_at, s.id.as_str()))
                .map(|s| s.id.clone());
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
                            && s.pool_revision == revision
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
            let placement = Placement {
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
            // Until the lease reaches a caller it may only live for the
            // unclaimed window; delivery restores the requested TTL.
            expires_at: now() + ttl.min(self.config.unclaimed_lease_ttl.as_secs()) as i64,
            requested_ttl_secs: ttl,
            env: request.inject.env.clone(),
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

    /// Mark a lease delivered when `Acquire` returns it: its expiry becomes
    /// the caller's full TTL. A lease persisted before `requested_ttl_secs`
    /// existed has 0 and is already delivered, so its expiry is left alone.
    async fn claim(&self, lease: &Lease) -> Result<Lease> {
        let (claimed, ops) = {
            let mut state = self.state.lock();
            let Some(current) = state.leases.get_mut(&lease.id) else {
                return Ok(lease.clone());
            };
            if current.requested_ttl_secs == 0 {
                return Ok(current.clone());
            }
            current.expires_at = now() + current.requested_ttl_secs as i64;
            let current = current.clone();
            let op = self.put(Kind::Leases, &current.id, &current);
            (current, vec![op])
        };
        self.persist(ops).await;
        Ok(claimed)
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
        // Concurrent same-key acquires share one lease: serialize staging so
        // they cannot truncate or unlink each other's tar.
        let lock = self
            .inject_locks
            .lock()
            .entry(lease.id.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;
        if self.state.lock().injected.contains(&lease.id) {
            self.inject_locks.lock().remove(&lease.id);
            return Ok(());
        }
        let result = async {
            // Group files by directory into one tar per call: extract at `/`.
            // A unique staging name keeps a retry of an earlier attempt from
            // colliding with this one.
            let tar_path = self.config.blob_dir.join(format!(
                "inject-{}-{}.tar",
                lease.id,
                uuid::Uuid::new_v4().simple()
            ));
            let files = inject.files.clone();
            let build_path = tar_path.clone();
            let built = tokio::task::spawn_blocking(move || build_inject_tar(&build_path, &files))
                .await
                .map_err(|e| Error::internal(format!("inject tar: {e}")))?;
            let result = match built {
                Ok(()) => {
                    self.upload_path(&sandbox.id, &lease.id, "/", &tar_path)
                        .await
                }
                Err(error) => Err(error),
            };
            let _ = std::fs::remove_file(&tar_path);
            result?;
            self.state.lock().injected.insert(lease.id.clone());
            Ok(())
        }
        .await;
        self.inject_locks.lock().remove(&lease.id);
        result
    }

    /// `Renew`.
    pub async fn renew(&self, request: RenewRequest) -> Result<Lease> {
        let (lease, ops) = {
            let mut state = self.state.lock();
            let lease = state
                .leases
                .get_mut(&request.lease_id)
                .ok_or_else(|| Error::not_found("lease not found"))?;
            let ttl = if request.ttl_secs > 0 {
                request.ttl_secs
            } else if lease.requested_ttl_secs > 0 {
                // A zero TTL means "keep the lease alive at the TTL it was
                // acquired with", not the service default.
                lease.requested_ttl_secs
            } else {
                // A lease persisted before `requested_ttl_secs` existed.
                self.config.lease_ttl.as_secs()
            };
            lease.requested_ttl_secs = ttl;
            lease.expires_at = now() + ttl as i64;
            let lease = lease.clone();
            let ops = vec![self.put(Kind::Leases, &lease.id, &lease)];
            (lease, ops)
        };
        self.persist(ops).await;
        Ok(lease)
    }

    /// `Release`.
    pub async fn release(&self, request: ReleaseRequest) -> Result<()> {
        let ops = {
            let mut state = self.state.lock();
            let Some(lease) = state.leases.get(&request.lease_id) else {
                return Err(Error::not_found("lease not found"));
            };
            if state.busy.contains(&lease.sandbox_id) {
                return Err(Error::precondition(
                    "a snapshot is in progress; release after it completes",
                ));
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

    /// `ListProviders`.
    pub fn list_providers(&self) -> Vec<Provider> {
        self.state
            .lock()
            .providers
            .values()
            .map(|rec| rec.provider.clone())
            .collect()
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
    pub fn exec(&self, request: ExecRequest) -> Result<BoxStream<'static, Result<ExecEvent>>> {
        if request.argv.is_empty() {
            return Err(Error::invalid("argv is required"));
        }
        validate_env(&request.env)?;
        let mut state = self.state.lock();
        let sandbox = state
            .sandboxes
            .get(&request.sandbox_id)
            .cloned()
            .ok_or_else(|| Error::not_found("sandbox not found"))?;
        authorize(&state, &sandbox, &request.lease_id)?;
        Self::usable(&sandbox)?;
        let mut merged = request.clone();
        if let Some(lease) = state.leases.get(&sandbox.lease_id) {
            validate_env(&lease.env)?;
            let mut env = lease.env.clone();
            env.extend(request.env.clone());
            merged.env = env;
        }
        // The lease is the caller's capability: a provider never sees it.
        merged.lease_id.clear();
        let exec_id = new_id("exe");
        let (tx, rx) = mpsc::channel(256);
        state
            .execs
            .insert(exec_id.clone(), (sandbox.id.clone(), tx));
        let work = WorkItem {
            id: new_id("wrk"),
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
            state.execs.remove(&exec_id);
            return Err(error);
        }
        Ok(tokio_stream_from(rx))
    }

    /// Run a command to completion, collecting output (convenience).
    pub async fn exec_collect(&self, request: ExecRequest) -> Result<(i32, Vec<u8>, Vec<u8>)> {
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
                claimed: false,
            },
        );
        Ok(Ticket {
            url: format!("/sandbox/v1/streams/{ticket}"),
            token,
            expires_at,
        })
    }

    /// Validate and consume a client ticket; returns `(kind, blob path)`.
    /// The claim is atomic: two concurrent transfers cannot share (and
    /// truncate) one destination.
    pub fn claim_ticket(&self, ticket: &str, token: &str) -> Result<(StreamKind, PathBuf)> {
        let mut state = self.state.lock();
        let blob = state
            .blobs
            .get(ticket)
            .filter(|blob| blob.expires_at > now())
            .ok_or_else(|| Error::not_found("ticket not found or expired"))?;
        if !constant_time_eq(blob.token.as_bytes(), token.as_bytes()) {
            return Err(Error::new(Code::Unauthenticated, "bad ticket token"));
        }
        let blob = state.blobs.get_mut(ticket).expect("ticket looked up above");
        if blob.claimed {
            return Err(Error::not_found("ticket already claimed"));
        }
        blob.claimed = true;
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
        copy_off_runtime(tar.to_owned(), path.clone(), "stage upload").await?;
        self.state.lock().blobs.insert(
            ticket.clone(),
            Blob {
                sandbox_id: sandbox_id.to_owned(),
                kind: StreamKind::Upload,
                guest_path: guest_dir.to_owned(),
                token: String::new(),
                expires_at: now() + self.config.ticket_ttl.as_secs() as i64,
                path: path.clone(),
                claimed: false,
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
                claimed: false,
            },
        );
        let result = self
            .copy_work(sandbox_id, &ticket, guest_path, StreamKind::Download)
            .await;
        let result = match result {
            Ok(()) => copy_off_runtime(path.clone(), dest.to_owned(), "download").await,
            Err(error) => Err(error),
        };
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
            if !state.busy.insert(sandbox.id.clone()) {
                return Err(Error::precondition(
                    "a snapshot of this sandbox is already in progress",
                ));
            }
            if state.templates.contains_key(&request.name) {
                state.busy.remove(&sandbox.id);
                return Err(Error::new(
                    Code::AlreadyExists,
                    format!("template `{}` already exists", request.name),
                ));
            }
            let caps = match self.driver_caps(&state, &sandbox) {
                Ok(caps) => caps,
                Err(error) => {
                    state.busy.remove(&sandbox.id);
                    return Err(error);
                }
            };
            if !caps.snapshot {
                state.busy.remove(&sandbox.id);
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
            if let Err(error) = self.enqueue(
                &mut state,
                &sandbox.provider_id,
                work,
                Pending::Snapshot { reply: tx },
            ) {
                state.busy.remove(&sandbox.id);
                return Err(error);
            }
            (sandbox, source_template)
        };
        // The set is guarded from here on, including every error path. It is
        // created after the state lock is released so its drop never
        // re-enters the mutex.
        let _busy = SnapshotBusy {
            service: self,
            sandbox_id: sandbox.id.clone(),
        };
        let native_ref = rx
            .await
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
            // A release, failure or another lifecycle transition while the
            // provider was working invalidates the capture: installing the
            // template would point at a machine that is being destroyed.
            let still_valid = state.sandboxes.get(&sandbox.id).is_some_and(|source| {
                source.state == SandboxState::Leased
                    && source.lease_id == sandbox.lease_id
                    && source.incarnation == sandbox.incarnation
            });
            if !still_valid {
                warn!(
                    sandbox = %sandbox.id,
                    "snapshot finished after its source changed; discarding the base"
                );
                self.drop_snapshot_locked(&mut state, &template);
                return Err(Error::precondition(
                    "the sandbox changed while its snapshot was in flight; result discarded",
                ));
            }
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
            if matches!(op, PowerOp::Stop)
                && !sandbox.pool.is_empty()
                && sandbox.lease_id.is_empty()
                && sandbox.state == SandboxState::Ready
            {
                return Err(Error::precondition(
                    "an idle pool member cannot be stopped; acquire it first",
                ));
            }
            let target = Self::target(&sandbox);
            let expected = sandbox.state;
            let lease_id = sandbox.lease_id.clone();
            let (work, kind) = match op {
                PowerOp::Stop => (
                    WorkItem {
                        id: new_id("wrk"),
                        stop: Some(target),
                        ..WorkItem::default()
                    },
                    Pending::Stop {
                        sandbox_id: id.to_owned(),
                        lease_id,
                        expected,
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
                        lease_id,
                        expected,
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
            if pause
                && !sandbox.pool.is_empty()
                && sandbox.lease_id.is_empty()
                && sandbox.state == SandboxState::Ready
            {
                return Err(Error::precondition(
                    "an idle pool member cannot be paused; acquire it first",
                ));
            }
            let target = Self::target(&sandbox);
            let expected = sandbox.state;
            let lease_id = sandbox.lease_id.clone();
            let (work, kind) = if pause {
                (
                    WorkItem {
                        id: new_id("wrk"),
                        pause: Some(target),
                        ..WorkItem::default()
                    },
                    Pending::Pause {
                        sandbox_id: id.to_owned(),
                        lease_id,
                        expected,
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
                        lease_id,
                        expected,
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

    /// `Register`: same name → same id, next generation. In-flight work of
    /// the previous generation fails; its sandboxes wait for `Reconcile`.
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
            let provider = Provider {
                id: id.clone(),
                name: request.name,
                generation,
                drivers: request.drivers,
                last_seen: now(),
            };
            let wake = state
                .providers
                .get(&id)
                .map(|rec| rec.wake.clone())
                .unwrap_or_else(|| Arc::new(Notify::new()));
            state.providers.insert(
                id.clone(),
                ProviderRec {
                    provider: provider.clone(),
                    seen: Instant::now(),
                    queue: VecDeque::new(),
                    wake,
                },
            );
            let mut ops = vec![self.put(Kind::Providers, &id, &provider)];
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
                    Err(Error::unavailable("provider re-registered")),
                    &mut ops,
                );
            }
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
        }
        Ok(())
    }

    /// `Poll`: long-poll for work.
    pub async fn poll(&self, request: PollRequest) -> Result<PollResponse> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(request.wait_secs.min(55));
        loop {
            let wake = {
                let mut state = self.state.lock();
                self.fence(&state, &request.provider_id, request.generation)?;
                let rec = state
                    .providers
                    .get_mut(&request.provider_id)
                    .ok_or_else(|| Error::not_found("provider not registered"))?;
                rec.seen = Instant::now();
                rec.provider.last_seen = now();
                if !rec.queue.is_empty() {
                    return Ok(PollResponse {
                        work: rec.queue.drain(..).collect(),
                    });
                }
                rec.wake.clone()
            };
            if tokio::time::timeout_at(deadline, wake.notified())
                .await
                .is_err()
            {
                return Ok(PollResponse::default());
            }
        }
    }

    /// `Report`.
    pub async fn report(&self, request: ReportRequest) -> Result<()> {
        let ops = {
            let mut state = self.state.lock();
            self.fence(&state, &request.provider_id, request.generation)?;
            let owner = state
                .pending
                .get(&request.work_id)
                .map(|work| work.provider_id.clone())
                .ok_or_else(|| Error::not_found("unknown or completed work"))?;
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
        match work.kind {
            Pending::Create {
                sandbox_id,
                incarnation,
                driver,
            } => match result {
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
                    if sandbox.incarnation != incarnation {
                        return;
                    }
                    sandbox.native_ref = native_ref;
                    sandbox.state = if sandbox.lease_id.is_empty() {
                        SandboxState::Ready
                    } else {
                        SandboxState::Leased
                    };
                    ops.push(self.put(Kind::Sandboxes, &sandbox_id, &*sandbox));
                    state.created_at.remove(&sandbox_id);
                }
                Err(error) => self.fail_locked(state, &sandbox_id, &error.message, ops),
            },
            Pending::Destroy { sandbox_id } => {
                // A failed destroy still drops the record: the machine is
                // then an orphan the next reconcile removes.
                if let Err(error) = &result {
                    warn!(%sandbox_id, %error, "destroy failed; reconcile will retry");
                }
                if state.sandboxes.remove(&sandbox_id).is_some() {
                    ops.push(self.remove(Kind::Sandboxes, &sandbox_id));
                }
            }
            Pending::Exec { exec_id } => {
                // Success is signalled by the exit event (`ReportExec`); a
                // failed exec work ends the stream with the error.
                if let Err(error) = result
                    && let Some((_, tx)) = state.execs.remove(&exec_id)
                {
                    let _ = tx.try_send(Err(error));
                }
            }
            Pending::Copy { reply } => {
                let _ = reply.send(result);
            }
            Pending::Snapshot { reply, .. } => {
                // Validity is checked when the template is installed (the
                // snapshot call still owns the source at this point).
                let _ = reply.send(result);
            }
            Pending::Pause {
                sandbox_id,
                lease_id,
                expected,
                reply,
            } => {
                let result = match result {
                    Ok(_) if still_in_state(state, &sandbox_id, &lease_id, expected) => {
                        let sandbox = state
                            .sandboxes
                            .get_mut(&sandbox_id)
                            .expect("state checked above");
                        sandbox.prior_state = sandbox.state;
                        sandbox.state = SandboxState::Paused;
                        ops.push(self.put(Kind::Sandboxes, &sandbox_id, &*sandbox));
                        Ok(String::new())
                    }
                    Ok(_) => Err(Error::precondition(
                        "the sandbox changed while the pause was in flight",
                    )),
                    Err(error) => Err(error),
                };
                let _ = reply.send(result);
            }
            Pending::Stop {
                sandbox_id,
                lease_id,
                expected,
                reply,
            } => {
                let result = match result {
                    Ok(_) if still_in_state(state, &sandbox_id, &lease_id, expected) => {
                        let sandbox = state
                            .sandboxes
                            .get_mut(&sandbox_id)
                            .expect("state checked above");
                        sandbox.prior_state = sandbox.state;
                        sandbox.state = SandboxState::Stopped;
                        ops.push(self.put(Kind::Sandboxes, &sandbox_id, &*sandbox));
                        Ok(String::new())
                    }
                    Ok(_) => Err(Error::precondition(
                        "the sandbox changed while the stop was in flight",
                    )),
                    Err(error) => Err(error),
                };
                let _ = reply.send(result);
            }
            Pending::Start {
                sandbox_id,
                lease_id,
                expected,
                forkable,
                reply,
            } => {
                let result = match result {
                    Ok(_) if still_in_state(state, &sandbox_id, &lease_id, expected) => {
                        let sandbox = state
                            .sandboxes
                            .get_mut(&sandbox_id)
                            .expect("state checked above");
                        let next = if sandbox.lease_id.is_empty() {
                            restored_state(sandbox)
                        } else {
                            SandboxState::Leased
                        };
                        self.restore_locked(sandbox, next);
                        if forkable {
                            sandbox.error.clear();
                        }
                        ops.push(self.put(Kind::Sandboxes, &sandbox_id, &*sandbox));
                        Ok(String::new())
                    }
                    Ok(_) => Err(Error::precondition(
                        "the sandbox changed while the start was in flight",
                    )),
                    Err(error) => Err(error),
                };
                let _ = reply.send(result);
            }
            Pending::Resume {
                sandbox_id,
                lease_id,
                expected,
                reply,
            } => {
                let result = match result {
                    Ok(_) if still_in_state(state, &sandbox_id, &lease_id, expected) => {
                        let sandbox = state
                            .sandboxes
                            .get_mut(&sandbox_id)
                            .expect("state checked above");
                        let next = if sandbox.lease_id.is_empty() {
                            restored_state(sandbox)
                        } else {
                            SandboxState::Leased
                        };
                        self.restore_locked(sandbox, next);
                        ops.push(self.put(Kind::Sandboxes, &sandbox_id, &*sandbox));
                        Ok(String::new())
                    }
                    Ok(_) => Err(Error::precondition(
                        "the sandbox changed while the resume was in flight",
                    )),
                    Err(error) => Err(error),
                };
                let _ = reply.send(result);
            }
        }
    }

    /// Apply a restored power state and its preservation bookkeeping.
    fn restore_locked(&self, sandbox: &mut Sandbox, next: SandboxState) {
        sandbox.state = next;
        sandbox.prior_state = SandboxState::SandboxStateUnspecified;
        match next {
            SandboxState::Preserved => {
                if sandbox.preserve_until == 0 {
                    sandbox.preserve_until = now() + self.config.preserve_ttl.as_secs() as i64;
                }
            }
            // Resuming or starting a preserved sandbox ends its debugging
            // window; only an explicitly preserved sandbox holds one open.
            _ => sandbox.preserve_until = 0,
        }
    }

    /// `ReportExec`: forward output; an exit event ends the stream.
    pub async fn report_exec(&self, request: ReportExecRequest) -> Result<()> {
        let tx = {
            let state = self.state.lock();
            self.fence(&state, &request.provider_id, request.generation)?;
            let owner = state
                .execs
                .get(&request.exec_id)
                .map(|(sandbox_id, _)| sandbox_id.clone());
            if let Some(sandbox_id) = owner {
                // Only the provider that owns the sandbox may report output
                // for its exec: a leaked exec id must not inject output into
                // another provider's stream.
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
            }
            state.execs.get(&request.exec_id).map(|(_, tx)| tx.clone())
        };
        let Some(tx) = tx else {
            // Caller went away; tell the agent to stop sending.
            return Err(Error::not_found("exec not found"));
        };
        let mut done = false;
        for event in request.events {
            done |= event.exit_code.is_some();
            if tx.send(Ok(event)).await.is_err() {
                done = true;
                break;
            }
        }
        if done {
            self.state.lock().execs.remove(&request.exec_id);
        }
        Ok(())
    }

    /// `Reconcile`: diff a provider's inventory against the records.
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
            for id in lost {
                warn!(sandbox = %id, "sandbox missing from provider inventory");
                if state
                    .sandboxes
                    .get(&id)
                    .is_some_and(|s| s.state == SandboxState::Terminating)
                {
                    state.sandboxes.remove(&id);
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
            // Machines no record owns (orphans, stale incarnations). Snapshot
            // bases are protected service-wide: any provider may report the
            // inventory, but a base belongs to the template record.
            let snapshots: HashSet<&str> = state
                .templates
                .values()
                .filter(|t| !t.native_ref.is_empty())
                .map(|t| t.native_ref.as_str())
                .collect();
            let mut destroy = Vec::new();
            for item in &request.sandboxes {
                if snapshots.contains(item.native_ref.as_str()) {
                    continue;
                }
                let owned = state.sandboxes.values().any(|s| {
                    s.provider_id == request.provider_id
                        && s.native_ref == item.native_ref
                        && (item.incarnation == 0 || s.incarnation == item.incarnation)
                });
                // Proof this service created the machine: its id is one of
                // ours (the driver encodes it in the native name), or a
                // caller-named create we dispatched. Machines from another
                // service sharing this host registry carry neither.
                let ours = (!item.sandbox_id.is_empty()
                    && state.sandboxes.contains_key(&item.sandbox_id))
                    || state.sandboxes.values().any(|s| {
                        s.provider_id == request.provider_id
                            && s.state == SandboxState::Provisioning
                            && !s.name.is_empty()
                            && s.name == item.native_ref
                    });
                // Another provider's machine (two agents on one host, or a
                // hand-created VM registered elsewhere) is never ours to
                // reap: only orphans this provider could have created.
                let foreign = !item.native_ref.is_empty()
                    && state.sandboxes.values().any(|s| {
                        s.provider_id != request.provider_id
                            && !s.state.is_terminal()
                            && s.native_ref == item.native_ref
                    });
                if !owned && ours && !foreign {
                    destroy.push(SandboxWork {
                        sandbox_id: item.sandbox_id.clone(),
                        driver: item.driver.clone(),
                        native_ref: item.native_ref.clone(),
                    });
                } else if !foreign {
                    warn!(
                        native_ref = %item.native_ref,
                        driver = %item.driver,
                        "inventory entry was not created by this service; leaving it alone"
                    );
                }
            }
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
    use crate::store;

    fn ready_member(id: &str, created_at: i64) -> Sandbox {
        Sandbox {
            id: id.into(),
            template: "alpine".into(),
            pool: "p".into(),
            state: SandboxState::Ready,
            created_at,
            ..Sandbox::default()
        }
    }

    fn with_current_pool_revision(state: &State, sandbox: &mut Sandbox) {
        if let Some(pool) = state.pools.get(&sandbox.pool) {
            sandbox.pool_revision = pool_revision(state, pool);
        }
    }

    #[tokio::test]
    async fn warm_members_rotate_oldest_first() {
        let dir = tempfile::tempdir().unwrap();
        let service = Service::open(
            store::open("memory:").await.unwrap(),
            ServiceConfig::new(dir.path().join("blobs")),
        )
        .await
        .unwrap();
        {
            let mut state = service.state.lock();
            state.templates.insert(
                "alpine".into(),
                Template {
                    spec: TemplateSpec {
                        name: "alpine".into(),
                        ..TemplateSpec::default()
                    },
                    ..Template::default()
                },
            );
            state.pools.insert(
                "p".into(),
                PoolSpec {
                    name: "p".into(),
                    template: "alpine".into(),
                    ..PoolSpec::default()
                },
            );
            // Insert the newer member first: map order must not decide.
            let mut newer = ready_member("sbx-newer", 200);
            with_current_pool_revision(&state, &mut newer);
            state.sandboxes.insert("sbx-newer".into(), newer);
            let mut older = ready_member("sbx-older", 100);
            with_current_pool_revision(&state, &mut older);
            state.sandboxes.insert("sbx-older".into(), older);
        }
        let first = service
            .acquire(AcquireRequest {
                idempotency_key: "k1".into(),
                pool: "p".into(),
                ..AcquireRequest::default()
            })
            .await
            .unwrap();
        assert_eq!(first.sandbox.id, "sbx-older");
        let second = service
            .acquire(AcquireRequest {
                idempotency_key: "k2".into(),
                pool: "p".into(),
                ..AcquireRequest::default()
            })
            .await
            .unwrap();
        assert_eq!(second.sandbox.id, "sbx-newer");
        // Equal ages break the tie by id.
        {
            let mut state = service.state.lock();
            let mut y = ready_member("sbx-y", 300);
            with_current_pool_revision(&state, &mut y);
            state.sandboxes.insert("sbx-y".into(), y);
            let mut x = ready_member("sbx-x", 300);
            with_current_pool_revision(&state, &mut x);
            state.sandboxes.insert("sbx-x".into(), x);
        }
        let third = service
            .acquire(AcquireRequest {
                idempotency_key: "k3".into(),
                pool: "p".into(),
                ..AcquireRequest::default()
            })
            .await
            .unwrap();
        assert_eq!(third.sandbox.id, "sbx-x");
    }
}
