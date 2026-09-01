//! Durable state backends for the control plane.
//!
//! The HTTP layer still uses the in-memory structures for fast protocol
//! handling, but every correctness-bearing transition is written through to
//! the selected backend before the transition is announced to observers. The
//! backend is also the restart source for runs, dispatch queues, runners,
//! sessions, and broker request identifiers.
//!

//!
//! Backends are selected at startup via [`open_store`] (env `PRELOOP_STORE_URL`:
//! `sqlite://<path>` or `postgres://…`; the default is SQLite at
//! `<state_dir>/preloop.db`). The [`Store`] trait is the only surface the rest
//! of the server sees, so a new database plugs in without touching callers.

use super::*;
use async_trait::async_trait;
use preloop_gha_protocol::SessionId;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use sha2::Digest;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Instant;

const DATABASE_FILE: &str = "preloop.db";
pub(crate) const SNAPSHOT_FORMAT: u8 = 2;
const MIGRATION_DOMAIN: &[u8] = b"preloop-store-v2";
const KEY_INFO_ENCRYPT: &[u8] = b"aks-store-aead/v1";
const KEY_INFO_MAC: &[u8] = b"aks-store-mac/v1";

type HmacSha256 = Hmac<Sha256>;

/// Durable-state backend. See the module docs for the contract.
#[async_trait]
pub(crate) trait Store: Send + Sync {
    /// Restore the persisted state into `inner` (startup path).
    async fn load_into(&self, inner: &mut InnerState) -> anyhow::Result<()>;
    /// Full snapshot: rewrite every table from a captured [`StoreSnapshot`]
    /// in one transaction.
    async fn store_inner(&self, snapshot: &StoreSnapshot) -> anyhow::Result<()>;
    /// Persist only the runtime metadata snapshot (hot path).
    async fn store_meta_only(&self, meta: &MetaSnapshot) -> anyhow::Result<()>;
    /// Persist one run's mutable projection plus a control event.
    async fn store_run_event(&self, projection: RunProjection) -> anyhow::Result<()>;
    /// Persist one attempt's step records.
    ///
    /// Separate from [`Store::store_run_event`] on purpose. Steps change far
    /// more often than anything else in a run, and a run-scoped projection
    /// would rewrite every attempt's rows on each transition — quadratic in
    /// the width of a matrix, against a single-writer backend. This writes the
    /// one attempt that changed.
    async fn store_job_steps(
        &self,
        run_id: RunId,
        agent_job_id: uuid::Uuid,
        records: &[crate::models::StepRecord],
        revision: u64,
    ) -> anyhow::Result<()>;
    /// Persist the run-number allocator for one workflow path.
    async fn store_workflow_run_counter(
        &self,
        workflow_path: &str,
        next_run_number: u64,
    ) -> anyhow::Result<()>;
    /// Append one log chunk (and upsert the parent log file aggregate).
    async fn store_log_chunk(
        &self,
        key: &str,
        chunk_index: i64,
        payload: &[u8],
        byte_count: i64,
        line_count: i64,
    ) -> anyhow::Result<()>;
    /// Delete a log entirely (parent `log_files` row + all `log_chunks` via
    /// cascade). Called when the in-memory retention caps evict a log key so
    /// the durable store cannot outgrow memory (D2).
    async fn delete_log(&self, key: &str) -> anyhow::Result<()>;
    /// Append a control event (`run_accepted` / `run_status` / `job_status`).
    async fn append_event(&self, event: &NdjsonEvent) -> anyhow::Result<()>;
}

/// Decorator that records `preloop.store.operation.duration` for every
/// `Store` method. One wrapper, not per-backend duplication.
pub(crate) struct InstrumentedStore {
    inner: Arc<dyn Store>,
    observability: preloop_observability::Observability,
    backend: String,
}

impl InstrumentedStore {
    fn new(
        inner: Arc<dyn Store>,
        observability: preloop_observability::Observability,
        backend: &str,
    ) -> Self {
        Self {
            inner,
            observability,
            backend: backend.to_string(),
        }
    }

    pub(crate) fn wrap(
        inner: Arc<dyn Store>,
        observability: preloop_observability::Observability,
        backend: &str,
    ) -> Arc<dyn Store> {
        Arc::new(Self::new(inner, observability, backend))
    }

    /// Time one delegated call, record duration and outcome, return the
    /// original result unchanged. Centralizing this means a newly added
    /// `Store` method cannot silently skip instrumentation.
    fn record<T>(
        &self,
        operation: &'static str,
        start: Instant,
        result: anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        let outcome = if result.is_ok() { "ok" } else { "error" };
        self.observability.metrics().store.observe(
            &self.backend,
            operation,
            outcome,
            start.elapsed(),
        );
        result
    }
}

#[async_trait]
impl Store for InstrumentedStore {
    async fn load_into(&self, inner: &mut InnerState) -> anyhow::Result<()> {
        let start = Instant::now();
        self.record("load_into", start, self.inner.load_into(inner).await)
    }

    async fn store_inner(&self, snapshot: &StoreSnapshot) -> anyhow::Result<()> {
        let start = Instant::now();
        self.record("store_inner", start, self.inner.store_inner(snapshot).await)
    }

    async fn store_meta_only(&self, meta: &MetaSnapshot) -> anyhow::Result<()> {
        let start = Instant::now();
        self.record(
            "store_meta_only",
            start,
            self.inner.store_meta_only(meta).await,
        )
    }

    async fn store_run_event(&self, projection: RunProjection) -> anyhow::Result<()> {
        let start = Instant::now();
        self.record(
            "store_run_event",
            start,
            self.inner.store_run_event(projection).await,
        )
    }

    async fn store_job_steps(
        &self,
        run_id: RunId,
        agent_job_id: uuid::Uuid,
        records: &[crate::models::StepRecord],
        revision: u64,
    ) -> anyhow::Result<()> {
        let start = Instant::now();
        self.record(
            "store_job_steps",
            start,
            self.inner
                .store_job_steps(run_id, agent_job_id, records, revision)
                .await,
        )
    }

    async fn store_workflow_run_counter(
        &self,
        workflow_path: &str,
        next_run_number: u64,
    ) -> anyhow::Result<()> {
        let start = Instant::now();
        self.record(
            "store_workflow_run_counter",
            start,
            self.inner
                .store_workflow_run_counter(workflow_path, next_run_number)
                .await,
        )
    }

    async fn store_log_chunk(
        &self,
        key: &str,
        chunk_index: i64,
        payload: &[u8],
        byte_count: i64,
        line_count: i64,
    ) -> anyhow::Result<()> {
        let start = Instant::now();
        self.record(
            "store_log_chunk",
            start,
            self.inner
                .store_log_chunk(key, chunk_index, payload, byte_count, line_count)
                .await,
        )
    }

    async fn append_event(&self, event: &NdjsonEvent) -> anyhow::Result<()> {
        let start = Instant::now();
        self.record("append_event", start, self.inner.append_event(event).await)
    }

    async fn delete_log(&self, key: &str) -> anyhow::Result<()> {
        let start = Instant::now();
        self.record("delete_log", start, self.inner.delete_log(key).await)
    }
}

/// Owned projection of the in-memory state that a full snapshot persists.
/// Captured under the state lock; the database write happens after the lock
/// is released, so a slow backend never stalls the control plane.
#[derive(Clone)]
pub(crate) struct StoreSnapshot {
    pub(crate) runs: Vec<RunRecord>,
    /// (queue_kind, job, global queue position within the kind).
    pub(crate) jobs: Vec<(&'static str, QueuedJob, i64)>,
    pub(crate) runners: Vec<RegisteredRunner>,
    pub(crate) rsa_public_keys: Vec<(i64, AgentRsaPublicKey)>,
    pub(crate) sessions: Vec<RunnerSession>,
    pub(crate) session_keys: Vec<(String, SessionEncryption)>,
    pub(crate) requests: Vec<TaskAgentJobRequestRecord>,
    /// (session_id, message_id, undelivered message).
    pub(crate) inflight: Vec<(String, i64, azdo::TaskAgentMessage)>,
    /// session_id → currently claimed request id.
    pub(crate) session_active_requests: Vec<(String, i64)>,
    /// request_id → undelivered job message.
    pub(crate) broker_request_messages: Vec<(i64, azdo::AgentJobRequestMessage)>,
    /// (agent_job_id, that attempt's step records, its revision).
    pub(crate) job_steps: Vec<(uuid::Uuid, Vec<crate::models::StepRecord>, u64)>,
    pub(crate) meta: MetaSnapshot,
}

impl StoreSnapshot {
    pub(crate) fn from_inner(inner: &InnerState) -> Self {
        StoreSnapshot {
            runs: inner.runs.values().cloned().collect(),
            jobs: queue_rows(inner)
                .into_iter()
                .map(|(kind, job, position)| (kind, job.clone(), position))
                .collect(),
            runners: inner.runners.values().cloned().collect(),
            rsa_public_keys: inner
                .runner_rsa_public_keys
                .iter()
                .map(|(id, key)| (*id, key.clone()))
                .collect(),
            sessions: inner.sessions.values().cloned().collect(),
            session_keys: inner
                .session_keys
                .iter()
                .map(|(id, enc)| (id.clone(), enc.clone()))
                .collect(),
            requests: inner.job_requests.values().cloned().collect(),
            inflight: inner
                .inflight_messages
                .iter()
                .flat_map(|(session, messages)| {
                    messages
                        .iter()
                        .map(move |(id, message)| (session.clone(), *id, message.clone()))
                })
                .collect(),
            session_active_requests: inner
                .session_active_requests
                .iter()
                .map(|(session, request)| (session.clone(), *request))
                .collect(),
            broker_request_messages: inner
                .broker_messages
                .iter()
                .map(|(id, message)| (*id, message.clone()))
                .collect(),
            job_steps: inner
                .job_steps
                .iter()
                .map(|(agent_job_id, records)| {
                    let revision = inner
                        .job_steps_revision
                        .get(agent_job_id)
                        .copied()
                        .unwrap_or(0);
                    (*agent_job_id, records.clone(), revision)
                })
                .collect(),
            meta: build_meta_snapshot(inner),
        }
    }
}

/// The per-run projection persisted on control events. Captured under the
/// state lock, written after it is released. Includes the claim/message state
/// so a job that was claimed (dequeued, message handed to a session) but not
/// yet acked survives a restart in the same transaction that rewrites its
/// run's queue rows.
#[derive(Clone)]
pub(crate) struct RunProjection {
    pub(crate) run: RunRecord,
    /// (queue_kind, job, global queue position within the kind).
    pub(crate) jobs: Vec<(&'static str, QueuedJob, i64)>,
    pub(crate) requests: Vec<TaskAgentJobRequestRecord>,
    pub(crate) session_active_requests: Vec<(String, i64)>,
    pub(crate) inflight: Vec<(String, i64, azdo::TaskAgentMessage)>,
    pub(crate) broker_request_messages: Vec<(i64, azdo::AgentJobRequestMessage)>,
    pub(crate) event: NdjsonEvent,
}

impl RunProjection {
    /// Returns `None` when the run is no longer present; the caller then skips
    /// persistence but still broadcasts.
    pub(crate) fn from_inner(
        inner: &InnerState,
        run_id: RunId,
        event: NdjsonEvent,
    ) -> Option<Self> {
        let run = inner.runs.get(&run_id)?.clone();
        Some(RunProjection {
            run,
            jobs: queue_rows_for_run(inner, run_id)
                .into_iter()
                .map(|(kind, job, position)| (kind, job.clone(), position))
                .collect(),
            requests: inner
                .job_requests
                .values()
                .filter(|record| record.run_id == run_id)
                .cloned()
                .collect(),
            session_active_requests: inner
                .session_active_requests
                .iter()
                .map(|(session, request)| (session.clone(), *request))
                .collect(),
            inflight: inner
                .inflight_messages
                .iter()
                .flat_map(|(session, messages)| {
                    messages
                        .iter()
                        .map(move |(id, message)| (session.clone(), *id, message.clone()))
                })
                .collect(),
            broker_request_messages: inner
                .broker_messages
                .iter()
                .map(|(id, message)| (*id, message.clone()))
                .collect(),
            event,
        })
    }
}

/// SQLite backend: `<state_dir>/preloop.db`, one connection behind a mutex.
#[derive(Clone)]
pub(crate) struct SqliteStore {
    connection: Arc<StdMutex<Connection>>,
    cipher: Envelope,
    /// Commits since the last forced WAL truncation. A full
    /// `wal_checkpoint(TRUNCATE)` on every commit fsyncs the DB every runner
    /// event (~35x slower in a WAL micro-benchmark and head-of-line blocking
    /// on the single connection); instead SQLite's background
    /// `wal_autocheckpoint` (PASSIVE) bounds routine growth and we force a
    /// TRUNCATE only every [`WAL_CHECKPOINT_INTERVAL`] commits to reclaim the
    /// file. Amortized cost is one blocking checkpoint per N events.
    checkpoint_counter: Arc<AtomicU64>,
}

/// Force a WAL truncation every N commits. Between forced truncations the
/// default `wal_autocheckpoint` (1000 pages ≈ 4 MB, PASSIVE, non-blocking)
/// keeps the WAL bounded; the periodic TRUNCATE guarantees the file is
/// reclaimed even when a reader kept PASSIVE from advancing.
const WAL_CHECKPOINT_INTERVAL: u64 = 128;

/// Where the server should look for durable state. Parsed from `PRELOOP_STORE_URL`
/// (or an explicit override); see [`parse_store_url`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoreUrl {
    /// `sqlite://<path>`, `sqlite:<path>`, or a bare filesystem path.
    /// An empty path means the default `<state_dir>/preloop.db`.
    Sqlite(std::path::PathBuf),
    /// `postgres://<user>:<pass>@<host>:<port>/<db>`.
    Postgres(String),
}

/// Environment variable selecting the store backend when no explicit URL is
/// given. Values: `sqlite://<path>`, `postgres://…`, or a bare path.
pub(crate) const STORE_URL_ENV: &str = "PRELOOP_STORE_URL";

/// Parse a store URL. Bare paths and `sqlite:` forms map to the SQLite
/// backend; `postgres://` / `postgresql://` map to the Postgres backend.
pub(crate) fn parse_store_url(value: &str) -> anyhow::Result<StoreUrl> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(StoreUrl::Sqlite(std::path::PathBuf::new()));
    }
    if let Some(rest) = value
        .strip_prefix("sqlite://")
        .or_else(|| value.strip_prefix("sqlite:"))
    {
        return Ok(StoreUrl::Sqlite(std::path::PathBuf::from(rest)));
    }
    if value.starts_with("postgres://") || value.starts_with("postgresql://") {
        return Ok(StoreUrl::Postgres(value.to_owned()));
    }
    if !value.contains("://") {
        // Bare path: keep the SQLite default behaviour.
        return Ok(StoreUrl::Sqlite(std::path::PathBuf::from(value)));
    }
    anyhow::bail!(
        "unsupported store URL {value:?}: expected sqlite://<path>, \
         postgres://<user>:<pass>@<host>/<db>, or a bare sqlite path"
    )
}

/// Open the store selected by `url` (falling back to `PRELOOP_STORE_URL`, then
/// to SQLite at `<state_dir>/preloop.db`). The AEAD envelope key is derived from
/// the JWT HMAC key with domain separation, independent of the backend.
pub(crate) async fn open_store(
    url: Option<&str>,
    state_dir: &std::path::Path,
    key: &[u8],
) -> anyhow::Result<Arc<dyn Store>> {
    let raw = match url {
        Some(value) if !value.trim().is_empty() => value.to_owned(),
        _ => std::env::var(STORE_URL_ENV).unwrap_or_default(),
    };
    let cipher = Envelope::new(key);
    let store: Arc<dyn Store> = match parse_store_url(&raw)? {
        StoreUrl::Sqlite(path) => {
            let path = if path.as_os_str().is_empty() {
                state_dir.join(DATABASE_FILE)
            } else {
                path
            };
            Arc::new(SqliteStore::open(&path, cipher)?)
        }
        StoreUrl::Postgres(url) => Arc::new(crate::store_pg::PgStore::open(&url, cipher).await?),
    };
    Ok(store)
}

/// AEAD envelope used to seal persisted blobs (runs, requests, session keys,
/// metadata snapshot). Backend-independent: SQLite and Postgres both store
/// the sealed bytes as opaque blobs.
#[derive(Clone)]
pub(crate) struct Envelope {
    aead: [u8; 32],
    mac: [u8; 32],
}

impl Envelope {
    /// Derive the AEAD + MAC sub-keys from the root HMAC key (HKDF-SHA256,
    /// domain-separated per purpose).
    pub(crate) fn new(root: &[u8]) -> Self {
        let keys = derive_keys(root);
        Self {
            aead: keys.aead,
            mac: keys.mac,
        }
    }

    /// AES-256-CBC + HMAC-SHA256 over the migration domain, IV, and
    /// ciphertext. Returns `(ciphertext, iv, tag)`.
    pub(crate) fn encrypt_sealed(&self, plaintext: &[u8]) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let cipher = SessionEncryption::from_key(self.aead.to_vec());
        let (ciphertext, iv) = cipher
            .encrypt(plaintext)
            .expect("AES-256-CBC encrypt is infallible for in-spec inputs");
        let mut mac = HmacSha256::new_from_slice(&self.mac).expect("HMAC accepts any key length");
        mac.update(MIGRATION_DOMAIN);
        mac.update(&iv);
        mac.update(&ciphertext);
        (ciphertext, iv, mac.finalize().into_bytes().to_vec())
    }

    /// Verify the MAC and decrypt. Fails on tampering or a wrong key.
    pub(crate) fn decrypt_sealed(
        &self,
        ciphertext: &[u8],
        iv: &[u8],
        tag: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        let mut mac = HmacSha256::new_from_slice(&self.mac).expect("HMAC accepts any key length");
        mac.update(MIGRATION_DOMAIN);
        mac.update(iv);
        mac.update(ciphertext);
        mac.verify_slice(tag)
            .map_err(|_| anyhow::anyhow!("store session-key envelope authentication failed"))?;
        SessionEncryption::from_key(self.aead.to_vec())
            .decrypt(ciphertext, iv)
            .map_err(|error| anyhow::anyhow!("store session-key decryption failed: {error}"))
    }

    /// Seal a plaintext blob: `version || iv || ciphertext || tag`.
    pub(crate) fn seal(&self, plaintext: &[u8]) -> anyhow::Result<Vec<u8>> {
        let (ciphertext, iv, tag) = self.encrypt_sealed(plaintext);
        let mut sealed = Vec::with_capacity(1 + iv.len() + ciphertext.len() + tag.len());
        sealed.push(SNAPSHOT_FORMAT);
        sealed.extend_from_slice(&iv);
        sealed.extend_from_slice(&ciphertext);
        sealed.extend_from_slice(&tag);
        Ok(sealed)
    }

    /// Unseal a blob written by [`Envelope::seal`]. Rejects foreign envelope
    /// versions (v1 used a different, unauthenticated scheme).
    pub(crate) fn unseal(&self, sealed: &[u8]) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(sealed.len() >= 1 + 16 + 32, "invalid store envelope");
        let version = sealed[0];
        if version != SNAPSHOT_FORMAT {
            // Old envelopes (v1) used a different key derivation (raw key or
            // SHA-256 of env input) and an unauthenticated AES-CBC layer.
            // We don't try to decrypt them — the user must drop the state
            // directory to start fresh. This is a hard cut-over; see the
            // migration notes in the architecture doc.
            anyhow::bail!(
                "unsupported store envelope version {version}; current version is {SNAPSHOT_FORMAT}"
            );
        }
        let iv = &sealed[1..17];
        let tag_start = sealed.len() - 32;
        let ciphertext = &sealed[17..tag_start];
        let tag = &sealed[tag_start..];
        self.decrypt_sealed(ciphertext, iv, tag)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MetaSnapshot {
    workflow_run_counters: BTreeMap<String, u64>,
    next_runner_id: i64,
    next_cache_id: i64,
    next_message_id: i64,
    next_log_id: usize,
    next_artifact_v2_id: u64,
    azdo_sessions: std::collections::HashSet<String>,
    oidc_job_contexts: Vec<(RunId, JobId, OidcJobContext)>,
    id_token_grants: Vec<(RunId, JobId, bool)>,
    concurrency_groups: Vec<((String, String), concurrency::ConcurrencyGroup)>,
    jobset_admissions: Vec<(JobSetId, JobSetAdmission)>,
    run_concurrency: Vec<(RunId, preloop_gha_parser::Concurrency)>,
    holder_keys: Vec<(RunId, Vec<(String, String)>)>,
    // `pending_caches` is deliberately absent. `PendingCache` owns the whole
    // in-flight upload payload (`bytes: Vec<u8>`), and every field in this
    // struct is cloned, JSON-serialized and AES-sealed on *every*
    // `store_meta_only` call. Persisting it made `cache_upload` quadratic in
    // total cache size — measured 75 ms per accumulated MiB per chunk, with
    // the global state lock held throughout. An interrupted upload now 404s
    // on commit and the runner re-uploads next run, which is what
    // `actions/cache` already expects.
    #[serde(default)]
    artifacts: Vec<(String, ArtifactRecord)>,
    #[serde(default)]
    log_metadata: Vec<(String, LogMetadata)>,
    #[serde(default)]
    timeline_events: Vec<(RunId, Vec<NdjsonEvent>)>,
    #[serde(default)]
    timeline_change_ids: Vec<(String, i32)>,
    #[serde(default)]
    timeline_records: Vec<(String, Vec<(uuid::Uuid, azdo::TimelineRecord)>)>,
    #[serde(default)]
    cache_v2_pending: Vec<(String, CacheV2Pending)>,
    #[serde(default)]
    cache_v2_dl_tokens: Vec<(String, (String, String))>,
    #[serde(default)]
    artifact_v2_pending: Vec<(String, ArtifactV2Pending)>,
    #[serde(default)]
    artifact_v2_registry: Vec<(String, ArtifactV2Entry)>,
    /// `github_token_requests` keyed by `(run_id, job_id)` so post-restart
    /// dispatch can mint the same token under the same conditions the original
    /// run recorded. Sealed on disk; the in-memory map drives the dispatcher.
    #[serde(default)]
    github_token_requests: Vec<(i64, GitHubTokenRequest)>,
    /// `cancellation_queue` queued cancels that did not yet propagate to the
    /// runner. Persisted so a restart mid-cancel still delivers the cancel.
    #[serde(default)]
    cancellation_queue: std::collections::VecDeque<QueuedCancellation>,
    /// Per-request job messages: request_id → job message, the counterpart of
    /// `inflight_messages` (which is per-session and lives in the
    /// `broker_messages` table). Moved to the `job_request_messages` table by
    /// `store_inner` / `store_run_event` so a claimed-but-unacked job survives
    /// a restart; kept out of this blob so a full snapshot is not needed to
    /// persist one claim.
    #[serde(default)]
    runner_client_ids: Vec<(String, i64)>,
    /// Runners that presented a valid one-time provision token. Persisted so a
    /// restart between registration and job claim keeps the pairing proof.
    #[serde(default)]
    pool_proven_runners: Vec<i64>,
    /// Job → runner assignments (strict-assignment mode), as
    /// (run_id, job_id, runner_id, at_us, first_at_us).
    #[serde(default)]
    job_assignments: Vec<(String, String, i64, u64, u64)>,
    /// Jobs waiting for a provisioned runner: (run_id, job_id, marked_at_us).
    #[serde(default)]
    pool_pending: Vec<(String, String, i64)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RequestSnapshot {
    request_id: i64,
    run_id: RunId,
    job_id: JobId,
    agent_job_id: uuid::Uuid,
    plan_id: String,
    plan_type: String,
    timeline_id: uuid::Uuid,
    result: Option<ExecutionStatus>,
    locked_until: String,
    started_at_us: Option<i64>,
    last_renewed_at_us: Option<i64>,
    timeout_triggered: bool,
    debug_token_issued: bool,
}

#[derive(Debug)]
pub(crate) struct RowJob {
    pub(crate) run_id: RunId,
    pub(crate) job_id: JobId,
    pub(crate) queue_kind: String,
    pub(crate) queue_position: i64,
    pub(crate) payload: Vec<u8>,
}

struct DerivedKeys {
    aead: [u8; 32],
    mac: [u8; 32],
}

/// Derive independent sub-keys from the loaded HMAC key via HKDF-SHA256.
///
/// The same root key is used for both the JWT HMAC and the store AEAD
/// (so callers can keep one `<state_dir>/hmac-key.bin`); HKDF gives each
/// purpose a domain-separated 32-byte key. This costs nothing when the keys
/// are derived at startup once.
fn derive_keys(root: &[u8]) -> DerivedKeys {
    // Salt = the root key itself, so two aksh installs that happen to load
    // the same weak env var don't end up with the same sub-keys.
    let mut aead_out = [0u8; 32];
    let mut mac_out = [0u8; 32];
    let salt = Sha256::digest(root);
    hkdf::Hkdf::<Sha256>::new(Some(salt.as_slice()), root)
        .expand(KEY_INFO_ENCRYPT, &mut aead_out)
        .expect("HKDF expand to 32 bytes is infallible");
    hkdf::Hkdf::<Sha256>::new(Some(salt.as_slice()), root)
        .expand(KEY_INFO_MAC, &mut mac_out)
        .expect("HKDF expand to 32 bytes is infallible");
    DerivedKeys {
        aead: aead_out,
        mac: mac_out,
    }
}

/// Serialize a run record for storage. The fields `#[serde(skip)]`-ped off
/// the wire shape are injected as JSON so the persisted blob is
/// self-contained: `submission` (through the sanctioned expose boundary),
/// `job_needs`, and the expansion-only fields (`caller_plans`, `github`,
/// `head_sha`, `workflow_ref`, `workspace_snapshot`) that the scheduler needs
/// to materialize a deferred reusable-caller or matrix subtree after a
/// restart.
pub(crate) fn run_record_value(run: &RunRecord) -> anyhow::Result<serde_json::Value> {
    let mut value = serde_json::to_value(run)?;
    if let Some(object) = value.as_object_mut() {
        object.insert("submission".to_owned(), run.submission.to_request_json()?);
        object.insert(
            "job_needs".to_owned(),
            serde_json::to_value(&run.job_needs)?,
        );
        object.insert(
            "caller_plans".to_owned(),
            serde_json::to_value(&run.caller_plans)?,
        );
        object.insert("github".to_owned(), run.github.clone());
        object.insert("head_sha".to_owned(), serde_json::to_value(&run.head_sha)?);
        object.insert(
            "workflow_ref".to_owned(),
            serde_json::to_value(&run.workflow_ref)?,
        );
        object.insert(
            "workspace_snapshot".to_owned(),
            serde_json::to_value(&run.workspace_snapshot)?,
        );
    }
    Ok(value)
}

/// Unseal + parse a run blob written by [`run_record_value`].
pub(crate) fn restore_run_record(cipher: &Envelope, blob: &[u8]) -> anyhow::Result<RunRecord> {
    let value: serde_json::Value = serde_json::from_slice(&cipher.unseal(blob)?)?;
    let mut run: RunRecord = serde_json::from_value(value.clone())?;
    if let Some(object) = value.as_object() {
        run.job_needs = object
            .get("job_needs")
            .cloned()
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_default();
        run.caller_plans = object
            .get("caller_plans")
            .cloned()
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_default();
        run.github = object
            .get("github")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        run.head_sha = object
            .get("head_sha")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        run.workflow_ref = object
            .get("workflow_ref")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        // `run_record_value` always writes the key (null when absent), so a
        // JSON null must restore as `None` rather than fail to parse. A
        // snapshot whose shape this binary no longer understands is dropped
        // with a warning: the store is best-effort and one stale record must
        // not brick startup (see `load_into`).
        run.workspace_snapshot = match object.get("workspace_snapshot") {
            Some(value) if !value.is_null() => match serde_json::from_value(value.clone()) {
                Ok(snapshot) => Some(snapshot),
                Err(error) => {
                    tracing::warn!(run_id = %run.run_id, %error, "dropping undecodable workspace snapshot on load");
                    None
                }
            },
            _ => None,
        };
    }
    Ok(run)
}

/// Project a job-request record into its persisted snapshot shape.
pub(crate) fn request_snapshot(record: &TaskAgentJobRequestRecord) -> RequestSnapshot {
    RequestSnapshot {
        request_id: record.request_id,
        run_id: record.run_id,
        job_id: record.job_id.clone(),
        agent_job_id: record.agent_job_id,
        plan_id: record.plan_id.clone(),
        plan_type: record.plan_type.clone(),
        timeline_id: record.timeline_id,
        result: record.result,
        locked_until: record.locked_until.clone(),
        started_at_us: record.started_at.map(system_time_us),
        last_renewed_at_us: record.last_renewed_at.map(system_time_us),
        timeout_triggered: record.timeout_triggered,
        debug_token_issued: record.debug_token_issued,
    }
}

/// Unseal + parse a request blob written by [`request_snapshot`].
pub(crate) fn restore_request_snapshot(
    cipher: &Envelope,
    blob: &[u8],
) -> anyhow::Result<TaskAgentJobRequestRecord> {
    let snapshot: RequestSnapshot = serde_json::from_slice(&cipher.unseal(blob)?)?;
    Ok(TaskAgentJobRequestRecord {
        request_id: snapshot.request_id,
        run_id: snapshot.run_id,
        job_id: snapshot.job_id.clone(),
        agent_job_id: snapshot.agent_job_id,
        plan_id: snapshot.plan_id.clone(),
        plan_type: snapshot.plan_type,
        timeline_id: snapshot.timeline_id,
        result: snapshot.result,
        locked_until: snapshot.locked_until,
        started_at: snapshot.started_at_us.map(system_time_from_us),
        last_renewed_at: snapshot.last_renewed_at_us.map(system_time_from_us),
        timeout_triggered: snapshot.timeout_triggered,
        debug_token_issued: snapshot.debug_token_issued,
    })
}

/// Serialize the runtime metadata snapshot from in-memory state. Shared by
/// every backend so one code path defines what survives a restart.
pub(crate) fn build_meta_snapshot(inner: &InnerState) -> MetaSnapshot {
    MetaSnapshot {
        workflow_run_counters: inner.workflow_run_counters.clone(),
        next_runner_id: inner.next_runner_id,
        next_cache_id: inner.next_cache_id,
        next_message_id: inner.next_message_id,
        next_log_id: inner.next_log_id,
        next_artifact_v2_id: inner.next_artifact_v2_id,
        azdo_sessions: inner.azdo_sessions.clone(),
        oidc_job_contexts: inner
            .oidc_job_contexts
            .iter()
            .map(|((run_id, job_id), context)| (*run_id, job_id.clone(), context.clone()))
            .collect(),
        id_token_grants: inner
            .id_token_grants
            .iter()
            .map(|((run_id, job_id), granted)| (*run_id, job_id.clone(), *granted))
            .collect(),
        concurrency_groups: inner
            .concurrency_groups
            .iter()
            .map(|(key, group)| (key.clone(), group.clone()))
            .collect(),
        jobset_admissions: inner
            .jobset_admissions
            .iter()
            .map(|(key, admission)| (key.clone(), admission.clone()))
            .collect(),
        run_concurrency: inner
            .run_concurrency
            .iter()
            .map(|(run_id, config)| (*run_id, config.clone()))
            .collect(),
        holder_keys: inner
            .holder_keys
            .iter()
            .map(|(run_id, keys)| (*run_id, keys.clone()))
            .collect(),
        artifacts: inner
            .artifacts
            .iter()
            .map(|(id, artifact)| (id.clone(), artifact.clone()))
            .collect(),
        log_metadata: inner
            .log_metadata
            .iter()
            .map(|(key, metadata)| (key.clone(), metadata.clone()))
            .collect(),
        timeline_events: inner
            .timeline_events
            .iter()
            .map(|(run_id, events)| (*run_id, events.clone()))
            .collect(),
        timeline_change_ids: inner
            .timeline_change_ids
            .iter()
            .map(|(key, change_id)| (key.clone(), *change_id))
            .collect(),
        timeline_records: inner
            .timeline_records
            .iter()
            .map(|(key, records)| {
                (
                    key.clone(),
                    records
                        .iter()
                        .map(|(id, record)| (*id, record.clone()))
                        .collect(),
                )
            })
            .collect(),
        cache_v2_pending: inner
            .cache_v2_pending
            .iter()
            .map(|(token, pending)| (token.clone(), pending.clone()))
            .collect(),
        cache_v2_dl_tokens: inner
            .cache_v2_dl_tokens
            .iter()
            .map(|(token, value)| (token.clone(), value.clone()))
            .collect(),
        artifact_v2_pending: inner
            .artifact_v2_pending
            .iter()
            .map(|(token, pending)| (token.clone(), pending.clone()))
            .collect(),
        artifact_v2_registry: inner
            .artifact_v2_registry
            .iter()
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect(),
        github_token_requests: inner
            .github_token_requests
            .iter()
            .map(|(request_id, req)| (*request_id, req.clone()))
            .collect(),
        cancellation_queue: inner.cancellation_queue.clone(),
        runner_client_ids: inner
            .runner_client_ids
            .iter()
            .map(|(client_id, runner_id)| (client_id.clone(), *runner_id))
            .collect(),
        pool_proven_runners: inner.pool_proven_runners.iter().copied().collect(),
        job_assignments: inner
            .job_assignments
            .iter()
            .map(|((run_id, job_id), record)| {
                (
                    run_id.to_string(),
                    job_id.0.clone(),
                    record.runner_id,
                    system_time_us(record.at) as u64,
                    system_time_us(record.first_at) as u64,
                )
            })
            .collect(),
        pool_pending: inner
            .pool_pending
            .iter()
            .map(|((run_id, job_id), at)| {
                (run_id.to_string(), job_id.0.clone(), system_time_us(*at))
            })
            .collect(),
    }
}

/// Apply a restored metadata snapshot onto in-memory state.
pub(crate) fn apply_meta_snapshot(inner: &mut InnerState, meta: MetaSnapshot) {
    inner.workflow_run_counters = meta.workflow_run_counters;
    inner.next_runner_id = meta.next_runner_id;
    inner.next_cache_id = meta.next_cache_id;
    inner.next_message_id = meta.next_message_id;
    inner.next_log_id = meta.next_log_id;
    inner.next_artifact_v2_id = meta.next_artifact_v2_id;
    inner.azdo_sessions = meta.azdo_sessions;
    inner.oidc_job_contexts = meta
        .oidc_job_contexts
        .into_iter()
        .map(|(run_id, job_id, context)| ((run_id, job_id), context))
        .collect();
    inner.id_token_grants = meta
        .id_token_grants
        .into_iter()
        .map(|(run_id, job_id, granted)| ((run_id, job_id), granted))
        .collect();
    inner.concurrency_groups = meta.concurrency_groups.into_iter().collect();
    // A restored group may name a holder whose run is already terminal (the
    // snapshot predates the completion) or missing entirely; leaving it in
    // place parks every later submission in that group forever. Reconcile
    // before anything dispatches, and re-promote whatever the freed slots
    // unblock.
    crate::runtime_scheduling::reconcile_concurrency_groups(inner);
    crate::runtime_scheduling::promote_ready_jobs(inner);
    inner.jobset_admissions = meta.jobset_admissions.into_iter().collect();
    inner.run_concurrency = meta.run_concurrency.into_iter().collect();
    inner.holder_keys = meta.holder_keys.into_iter().collect();
    inner.artifacts = meta.artifacts.into_iter().collect();
    inner.log_metadata = meta.log_metadata.into_iter().collect();
    inner.timeline_events = meta.timeline_events.into_iter().collect();
    inner.timeline_change_ids = meta.timeline_change_ids.into_iter().collect();
    inner.timeline_records = meta
        .timeline_records
        .into_iter()
        .map(|(key, records)| (key, records.into_iter().collect()))
        .collect();
    inner.cache_v2_pending = meta.cache_v2_pending.into_iter().collect();
    inner.cache_v2_dl_tokens = meta.cache_v2_dl_tokens.into_iter().collect();
    inner.artifact_v2_pending = meta.artifact_v2_pending.into_iter().collect();
    let mut migrated_registry = std::collections::BTreeMap::new();
    for (k, v) in meta.artifact_v2_registry {
        let parts: Vec<&str> = k.split('/').collect();
        if parts.len() >= 3 {
            migrated_registry.insert(format!("{}/{}", parts[0], parts[2..].join("/")), v);
        } else {
            migrated_registry.insert(k, v);
        }
    }
    inner.artifact_v2_registry = migrated_registry;
    // Rebuild in-memory order queues for FIFO eviction and apply caps so a
    // restart doesn't reload unbounded history that was pending before the
    // caps shipped.
    {
        // Trim per-key overlong logs that were persisted before the 16 MiB
        // cap FIRST, then seed `log_bytes_total` from the trimmed sizes so
        // `trim_plan_logs` sees the correct total (its fast path keys on it).
        for buf in inner.logs.values_mut() {
            let excess = buf
                .len()
                .saturating_sub(crate::memory_caps::MAX_LOG_BYTES_PER_KEY);
            if excess > 0 {
                buf.drain(0..excess);
            }
        }
        inner.log_bytes_total = inner.logs.values().map(Vec::len).sum();
        inner.log_order.clear();
        for key in inner.logs.keys() {
            inner.log_order.push_back(key.clone());
        }
        let plans: std::collections::BTreeSet<String> = inner
            .logs
            .keys()
            .filter_map(|k| k.split('/').next().map(|s| s.to_owned()))
            .collect();
        for plan in plans {
            crate::memory_caps::trim_plan_logs(inner, &plan);
        }
    }
    inner.timeline_records_order.clear();
    for key in inner.timeline_records.keys() {
        inner.timeline_records_order.push_back(key.clone());
    }
    // Enforce global caps after restore.
    {
        let keys: Vec<String> = inner.timeline_records.keys().cloned().collect();
        for key in keys {
            crate::memory_caps::trim_timeline_after_patch(inner, &key, &[]);
        }
    }
    inner.timeline_events_order.clear();
    for run in inner.timeline_events.keys().copied() {
        inner.timeline_events_order.push_back(run);
    }
    {
        let runs: Vec<RunId> = inner.timeline_events.keys().copied().collect();
        for run in runs {
            crate::memory_caps::trim_timeline_events(inner, run);
        }
    }
    inner.artifact_registry_order.clear();
    for key in inner.artifact_v2_registry.keys() {
        inner.artifact_registry_order.push_back(key.clone());
    }
    crate::memory_caps::trim_artifact_registry(inner);
    crate::memory_caps::trim_cache_dl_tokens(inner);
    // Cache dl tokens order deque was not persisted — nothing to rebuild, but
    // ensure restored map doesn't already exceed the cap.
    inner.github_token_requests = meta.github_token_requests.into_iter().collect();
    inner.cancellation_queue = meta.cancellation_queue;
    inner.runner_client_ids = meta.runner_client_ids.into_iter().collect();
    inner.pool_proven_runners = meta.pool_proven_runners.into_iter().collect();
    inner.job_assignments = meta
        .job_assignments
        .into_iter()
        .filter_map(|(run_id, job_id, runner_id, at_us, first_at_us)| {
            run_id.parse().ok().map(|run_id| {
                (
                    (run_id, JobId(job_id)),
                    AssignmentRecord {
                        runner_id,
                        at: system_time_from_us(at_us as i64),
                        first_at: system_time_from_us(first_at_us as i64),
                    },
                )
            })
        })
        .collect();
    inner.pool_pending = meta
        .pool_pending
        .into_iter()
        .filter_map(|(run_id, job_id, at_us)| {
            run_id
                .parse()
                .ok()
                .map(|run_id| ((run_id, JobId(job_id)), system_time_from_us(at_us)))
        })
        .collect();
}

/// Seal a session AES key for storage. Returns `(ciphertext, iv, tag)`.
pub(crate) fn seal_session_key(
    cipher: &Envelope,
    enc: &SessionEncryption,
) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let payload = serde_json::to_vec(&SessionKeyPayload(enc.key.clone()))
        .expect("SessionKeyPayload is always serializable");
    cipher.encrypt_sealed(&payload)
}

/// Unseal + parse a session AES key blob written by [`seal_session_key`].
pub(crate) fn restore_session_key(
    cipher: &Envelope,
    key_blob: &[u8],
    iv: &[u8],
    tag: &[u8],
) -> anyhow::Result<SessionEncryption> {
    let plaintext = cipher.decrypt_sealed(key_blob, iv, tag)?;
    let payload: SessionKeyPayload = serde_json::from_slice(&plaintext)?;
    Ok(SessionEncryption::from_key(payload.0))
}

/// Bound the WAL after a commit, and fail loudly when the checkpoint could not
/// complete.
///
/// `PRAGMA wal_checkpoint(TRUNCATE)` returns a result row of
/// `(busy, log_frames, checkpointed_frames)`; a blocked checkpoint reports
/// `busy = 1` but does not raise a SQL error, so discarding the row would
/// treat "WAL still full" as success. Backing off here also keeps one write
/// from starving the next: the retry gives competing readers time to finish
/// before we TRUNCATE again. Every post-commit write path funnels through
/// this helper.
fn checkpoint_wal(connection: &Connection) -> anyhow::Result<()> {
    for _ in 0..10 {
        let (busy, log, checkpointed) =
            connection.query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?;
        if busy == 0 {
            return Ok(());
        }
        tracing::warn!(
            log_frames = log,
            checkpointed_frames = checkpointed,
            "WAL checkpoint blocked; retrying"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    anyhow::bail!("WAL checkpoint stayed blocked after retries")
}

impl SqliteStore {
    pub(crate) fn open(path: &std::path::Path, cipher: Envelope) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path)?;
        connection.execute_batch(
            r#"
            PRAGMA application_id = 0x414B5348;
            PRAGMA journal_mode = WAL;
            -- WAL + NORMAL preserves committed transactions across process
            -- crashes while avoiding an fsync for every runner event.
            PRAGMA synchronous = NORMAL;
            PRAGMA foreign_keys = ON;
            PRAGMA busy_timeout = 5000;
            "#,
        )?;
        Self::migrate(&connection)?;
        Ok(Self {
            connection: Arc::new(StdMutex::new(connection)),
            cipher,
            checkpoint_counter: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Post-commit WAL maintenance. Forces a truncating checkpoint only every
    /// [`WAL_CHECKPOINT_INTERVAL`] commits (the first commit truncates so a
    /// fresh DB starts clean); routine growth between truncations is bounded
    /// by SQLite's background `wal_autocheckpoint`. This replaces the previous
    /// truncate-on-every-commit policy, which fsynced the DB per runner event.
    fn maybe_checkpoint_wal(&self, connection: &Connection) -> anyhow::Result<()> {
        if self
            .checkpoint_counter
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(WAL_CHECKPOINT_INTERVAL)
        {
            checkpoint_wal(connection)?;
        }
        Ok(())
    }

    /// Apply pending migrations. Each step runs in its own transaction inside
    /// `PRAGMA user_version`; the `schema_migrations` table is a human audit
    /// trail. Steps are append-only and idempotent.
    fn migrate(connection: &Connection) -> anyhow::Result<()> {
        // Always-present bookkeeping table for the migration audit trail.
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS schema_migrations (
              version INTEGER PRIMARY KEY,
              name TEXT NOT NULL UNIQUE,
              applied_at_us INTEGER NOT NULL
            ) STRICT;
            "#,
        )?;

        let current_version: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
            .unwrap_or(0);
        for (version, name, sql) in MIGRATIONS {
            if *version as i64 <= current_version {
                continue;
            }
            let tx = connection.unchecked_transaction()?;
            tx.execute_batch(sql)?;
            tx.execute(
                "INSERT INTO schema_migrations(version, name, applied_at_us) VALUES (?1, ?2, ?3)
                 ON CONFLICT(version) DO NOTHING",
                params![*version as i64, *name, now_us()],
            )?;
            tx.execute_batch(&format!("PRAGMA user_version = {}", *version))?;
            tx.commit()?;
        }
        Ok(())
    }

    pub(crate) fn load_into(&self, inner: &mut InnerState) -> anyhow::Result<()> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let mut run_stmt =
            connection.prepare("SELECT record_blob FROM runs ORDER BY created_at_us")?;
        let runs = run_stmt
            .query_map([], |row| row.get::<_, Vec<u8>>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for blob in runs {
            let run = restore_run_record(&self.cipher, &blob)?;
            let run_id = run.run_id;
            inner.runs.insert(run_id, run);
        }

        let mut job_stmt = connection.prepare(
            "SELECT run_id, job_id, queue_kind, queue_position, payload_blob
             FROM jobs ORDER BY queue_kind, queue_position",
        )?;
        let jobs = job_stmt
            .query_map([], |row| {
                Ok(RowJob {
                    run_id: row.get::<_, String>(0)?.parse().map_err(|_| {
                        rusqlite::Error::InvalidColumnType(
                            0,
                            "run_id".to_owned(),
                            rusqlite::types::Type::Text,
                        )
                    })?,
                    job_id: JobId(row.get(1)?),
                    queue_kind: row.get(2)?,
                    queue_position: row.get(3)?,
                    payload: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for row in jobs {
            let job: QueuedJob = serde_json::from_slice(&self.cipher.unseal(&row.payload)?)?;
            match row.queue_kind.as_str() {
                "ready" => inner.queue.push_back(job),
                "pending" => inner.pending_jobs.push_back(job),
                "blocked" => inner.concurrency_blocked.push_back(job),
                "held" => inner.held_runs.entry(row.run_id).or_default().push(job),
                _ => unreachable!("schema constrains queue_kind"),
            }
        }

        let mut runner_stmt = connection.prepare(
            "SELECT runner_id, name, ephemeral, runner_group_id, runner_group_name,
                    public_key, rsa_public_key
             FROM runners WHERE deleted_at_us IS NULL",
        )?;
        for runner in runner_stmt.query_map([], |row| {
            Ok(RegisteredRunner {
                id: row.get(0)?,
                name: row.get(1)?,
                ephemeral: row.get(2)?,
                runner_group_id: row.get(3)?,
                runner_group_name: row.get(4)?,
                public_key: row.get(5)?,
                labels: Vec::new(),
            })
        })? {
            let runner = runner?;
            inner.runner_public_keys.extend(
                runner
                    .public_key
                    .as_ref()
                    .map(|key| (runner.id, key.clone())),
            );
            inner.runners.insert(runner.id, runner);
        }
        // Restore typed RSA public keys so post-restart sessions can be
        // FIPS-encrypted. Without this, every session is created
        // `encrypted:false` regardless of `RequireFipsCryptography`.
        let mut rsa_stmt = connection.prepare(
            "SELECT runner_id, rsa_public_key FROM runners
             WHERE deleted_at_us IS NULL AND rsa_public_key IS NOT NULL",
        )?;
        for row in rsa_stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })? {
            let (runner_id, rsa_xml) = row?;
            if let Ok(parsed) = AgentRsaPublicKey::parse(&rsa_xml) {
                inner.runner_rsa_public_keys.insert(runner_id, parsed);
            }
        }
        let mut label_stmt =
            connection.prepare("SELECT runner_id, label FROM runner_labels ORDER BY ordinal")?;
        for label in label_stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })? {
            let (runner_id, label) = label?;
            if let Some(runner) = inner.runners.get_mut(&runner_id) {
                runner.labels.push(label);
            }
        }
        // Restore session encryption keys (sealed). Without these, every
        // post-restart session is `encrypted:false` regardless of the runner's
        // RSA key — the AES session key is sent in plaintext to the runner.
        let mut session_keys_stmt = connection.prepare(
            "SELECT session_id, session_key_blob, session_iv, session_tag
             FROM runner_sessions
             WHERE closed_at_us IS NULL AND session_key_blob IS NOT NULL",
        )?;
        for row in session_keys_stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, Vec<u8>>(3)?,
            ))
        })? {
            let (session_id, key_blob, iv, tag) = row?;
            match restore_session_key(&self.cipher, &key_blob, &iv, &tag) {
                Ok(enc) => {
                    inner.session_keys.insert(session_id.clone(), enc);
                }
                Err(error) => {
                    tracing::warn!(%session_id, %error, "failed to restore session_key on load");
                }
            }
        }
        let mut session_stmt = connection.prepare(
            "SELECT session_id, runner_id FROM runner_sessions WHERE closed_at_us IS NULL",
        )?;
        for session in session_stmt.query_map([], |row| {
            Ok(RunnerSession {
                session_id: SessionId(row.get::<_, String>(0)?.parse().map_err(|_| {
                    rusqlite::Error::InvalidColumnType(
                        0,
                        "session_id".to_owned(),
                        rusqlite::types::Type::Text,
                    )
                })?),
                runner_id: row.get(1)?,
            })
        })? {
            let session = session?;
            inner
                .broker_session_runners
                .insert(session.session_id.0.to_string(), session.runner_id);
            inner
                .sessions
                .insert(session.session_id.0.to_string(), session);
        }
        // Restore `session_active_requests` so a restarted broker session
        // knows which request it had claimed but not acked.
        let mut sar_stmt = connection
            .prepare("SELECT session_id, active_request_id FROM session_active_requests")?;
        for row in sar_stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })? {
            let (session_id, request_id) = row?;
            inner.session_active_requests.insert(session_id, request_id);
        }
        // Restore per-session broker message queues (dequeued but not yet
        // delivered to the runner) from the `broker_messages` table that
        // `store_inner` writes. `inner.broker_messages` (keyed by request_id)
        // is a separate map and comes back with the meta snapshot below.
        let mut inflight_stmt = connection.prepare(
            "SELECT session_id, message_id, payload_json FROM broker_messages
             ORDER BY session_id, message_id",
        )?;
        for row in inflight_stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })? {
            let (session_id, message_id, payload_json) = row?;
            match serde_json::from_str::<azdo::TaskAgentMessage>(&payload_json) {
                Ok(message) => {
                    inner
                        .inflight_messages
                        .entry(session_id)
                        .or_default()
                        .insert(message_id, message);
                }
                Err(error) => {
                    tracing::warn!(%session_id, message_id, %error, "dropping undecodable broker message");
                }
            }
        }
        // Restore per-request job messages (request_id → message) from their
        // own table; `inner.broker_messages` is keyed by request_id and the
        // broker re-delivers from it after a restart.
        let mut jrm_stmt = connection.prepare(
            "SELECT request_id, payload_json FROM job_request_messages ORDER BY request_id",
        )?;
        for row in jrm_stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })? {
            let (request_id, payload_json) = row?;
            match serde_json::from_str::<azdo::AgentJobRequestMessage>(&payload_json) {
                Ok(message) => {
                    inner.broker_messages.insert(request_id, message);
                }
                Err(error) => {
                    tracing::warn!(request_id, %error, "dropping undecodable job request message");
                }
            }
        }
        let mut request_stmt =
            connection.prepare("SELECT request_id, request_blob FROM job_requests")?;
        for row in request_stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
        })? {
            let (request_id, blob) = row?;
            let record = restore_request_snapshot(&self.cipher, &blob)?;
            inner
                .inflight_requests
                .insert(request_id, (record.run_id, record.job_id.clone()));
            inner
                .plan_requests
                .insert(record.plan_id.clone(), request_id);
            inner
                .agent_job_requests
                .insert(record.agent_job_id, request_id);
            inner
                .timeline_requests
                .insert(record.timeline_id, request_id);
            inner.job_requests.insert(request_id, record);
        }

        if let Some(blob) = connection
            .query_row(
                "SELECT meta_blob FROM runtime_snapshots WHERE snapshot_id = 1",
                [],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?
        {
            let meta: MetaSnapshot = serde_json::from_slice(&self.cipher.unseal(&blob)?)?;
            apply_meta_snapshot(inner, meta);
        }
        let mut counters = connection.prepare(
            "SELECT workflow_path, next_run_number
             FROM workflow_run_counters
             ORDER BY workflow_path",
        )?;
        let rows = counters.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })?;
        for row in rows {
            let (workflow_path, next_run_number) = row?;
            inner
                .workflow_run_counters
                .insert(workflow_path, next_run_number.saturating_sub(1));
        }
        // Step manifests, ordered so a workflow step's `--step` position is
        // rebuilt exactly as it was recorded. Without this a restart leaves
        // `--step` unable to identify anything and it refuses rather than
        // guessing, so the run's step history has to come back here.
        let mut step_stmt = connection.prepare(
            "SELECT agent_job_id, step_id, kind, workflow_index, runner_number,
                    context_name, name_blob, conclusion, started_at_us, finished_at_us
             FROM job_steps
             ORDER BY agent_job_id, COALESCE(runner_number, 2147483647),
                      COALESCE(workflow_index, 2147483647), step_id",
        )?;
        for row in step_stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Vec<u8>>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, Option<i64>>(8)?,
                row.get::<_, Option<i64>>(9)?,
            ))
        })? {
            let (
                agent_job_id,
                step_id,
                kind,
                workflow_index,
                runner_number,
                context_name,
                name_blob,
                conclusion,
                started_at_us,
                finished_at_us,
            ) = row?;
            let Ok(agent_job_id) = agent_job_id.parse::<uuid::Uuid>() else {
                tracing::warn!(%agent_job_id, "dropping step row with an unparseable attempt id");
                continue;
            };
            let name = match self.cipher.unseal(&name_blob) {
                Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                Err(error) => {
                    tracing::warn!(%error, %step_id, "dropping step row with an unreadable name");
                    continue;
                }
            };
            inner
                .job_steps
                .entry(agent_job_id)
                .or_default()
                .push(crate::models::StepRecord {
                    id: step_id,
                    kind: match kind.as_str() {
                        "workflow" => crate::models::StepKind::Workflow,
                        _ => crate::models::StepKind::Synthetic,
                    },
                    workflow_index: workflow_index.map(|index| index as usize),
                    runner_number: runner_number.map(|number| number as u32),
                    context_name,
                    name,
                    conclusion,
                    started_at: started_at_us.and_then(chrono::DateTime::from_timestamp_micros),
                    finished_at: finished_at_us.and_then(chrono::DateTime::from_timestamp_micros),
                });
        }
        // Log bytes live in their own table; rebuild the in-memory buffers
        // from ordered chunks. Failing to do this leaves the post-restart
        // server unable to serve GET /logs/<id> for in-flight jobs.
        let mut log_chunk_stmt = connection.prepare(
            "SELECT log_key, chunk_index, payload FROM log_chunks
             ORDER BY log_key, chunk_index",
        )?;
        for row in log_chunk_stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })? {
            let (key, _index, payload) = row?;
            inner
                .logs
                .entry(key)
                .or_default()
                .extend_from_slice(&payload);
        }
        // `log_metadata` (byte_count, line_count) lives in `log_files`, the
        // aggregate table written by `store_log_chunk`. Reconstruct the
        // in-memory counter map from it so the post-restart counts match
        // what was last persisted.
        let mut log_files_stmt =
            connection.prepare("SELECT log_key, byte_count, line_count FROM log_files")?;
        for row in log_files_stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })? {
            let (key, byte_count, line_count) = row?;
            inner.log_metadata.insert(
                key,
                crate::models::LogMetadata {
                    byte_count: byte_count.max(0) as usize,
                    line_count: line_count.max(0) as usize,
                },
            );
        }
        Ok(())
    }

    /// Persist a single log chunk. Replaces the old behaviour of rewriting the
    /// entire `meta_blob` on every append.
    pub(crate) fn store_log_chunk(
        &self,
        key: &str,
        chunk_index: i64,
        payload: &[u8],
        byte_count: i64,
        line_count: i64,
    ) -> anyhow::Result<()> {
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let tx = connection.transaction()?;
        // UPSERT the parent row first so the FK from `log_chunks` is
        // satisfied even on the very first append.
        tx.execute(
            "INSERT INTO log_files(log_key, byte_count, line_count, updated_at_us)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(log_key) DO UPDATE SET
               byte_count = excluded.byte_count,
               line_count = excluded.line_count,
               updated_at_us = excluded.updated_at_us",
            params![key, byte_count, line_count, now_us()],
        )?;
        tx.execute(
            "INSERT INTO log_chunks(log_key, chunk_index, payload, written_at_us)
             VALUES (?1, ?2, ?3, ?4)",
            params![key, chunk_index, payload, now_us()],
        )?;
        // D2: bound durable bytes per log key to the in-memory retention.
        // `chunk_index` is the cumulative byte count after this append, so
        // chunks whose index is at or below `byte_count - MAX_LOG_BYTES_PER_KEY`
        // fall entirely outside the retained tail and can be dropped. Restart
        // reload trims to the same budget, so nothing recoverable is lost.
        let cutoff = byte_count - crate::memory_caps::MAX_LOG_BYTES_PER_KEY as i64;
        if cutoff > 0 {
            tx.execute(
                "DELETE FROM log_chunks WHERE log_key = ?1 AND chunk_index <= ?2",
                params![key, cutoff],
            )?;
        }
        tx.commit()
            .map_err(|error| anyhow::anyhow!("committing log chunk: {error}"))?;
        self.maybe_checkpoint_wal(&connection)?;
        Ok(())
    }

    /// Delete a log's parent row; `log_chunks` cascade away with it.
    pub(crate) fn delete_log(&self, key: &str) -> anyhow::Result<()> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        connection.execute("DELETE FROM log_files WHERE log_key = ?1", params![key])?;
        Ok(())
    }

    pub(crate) fn store_inner(&self, snapshot: &StoreSnapshot) -> anyhow::Result<()> {
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let tx = connection.transaction()?;
        for table in [
            "broker_messages",
            "job_request_messages",
            "session_active_requests",
            "job_requests",
            "job_dependencies",
            "jobs",
            "runner_sessions",
            "runner_labels",
            "runners",
            "runs",
        ] {
            tx.execute(&format!("DELETE FROM {table}"), [])
                .map_err(|error| anyhow::anyhow!("deleting {table}: {error}"))?;
        }
        for run in &snapshot.runs {
            let sealed_run = self
                .cipher
                .seal(&serde_json::to_vec(&run_record_value(run)?)?)?;
            tx.execute(
                "INSERT INTO runs(run_id, repository, workflow_path, status, run_number,
                                  run_attempt, created_at_us, completed_at_us, record_blob)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    run.run_id.to_string(),
                    run.submission.repository.clone(),
                    run.workflow_path_str.clone(),
                    status_string(run.status),
                    run.run_number as i64,
                    run.run_attempt as i64,
                    unix_us(run.created_at),
                    run.completed_at.map(unix_us),
                    sealed_run,
                ],
            )?;
        }

        for (kind, job, position) in &snapshot.jobs {
            self.insert_job(&tx, job, kind, *position)?;
        }

        let rsa_keys: std::collections::BTreeMap<i64, &AgentRsaPublicKey> = snapshot
            .rsa_public_keys
            .iter()
            .map(|(id, key)| (*id, key))
            .collect();
        let session_keys: std::collections::BTreeMap<String, &SessionEncryption> = snapshot
            .session_keys
            .iter()
            .map(|(id, key)| (id.clone(), key))
            .collect();
        for runner in &snapshot.runners {
            // `OR REPLACE` so re-registration (same `runner_id`, new name)
            // overwrites the row in place. Without it, `UNIQUE(name)` would
            // reject the insert whenever the in-memory map had two runners
            // that share a name — which the official `actions/runner` does
            // on every re-register / replace flow.
            // Typed RSA public key is stored alongside the XML form so a
            // post-restart session can be FIPS-encrypted without re-parsing
            // and without an extra table.
            let rsa_xml = rsa_keys.get(&runner.id).map(|key| key.to_xml_string());
            tx.execute(
                "INSERT OR REPLACE INTO runners(runner_id, name, ephemeral,
                                                runner_group_id, runner_group_name,
                                                public_key, rsa_public_key,
                                                created_at_us, updated_at_us)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
                params![
                    runner.id,
                    runner.name.clone(),
                    runner.ephemeral as i64,
                    runner.runner_group_id,
                    runner.runner_group_name.clone(),
                    runner.public_key.clone(),
                    rsa_xml,
                    now_us(),
                ],
            )?;
            // Defense-in-depth: handler-level `dedupe_labels_ci` should have
            // already collapsed duplicates, but if a future code path mutates
            // `inner.runners[id].labels` directly we still need a `(runner_id,
            // label)` insert that cannot violate the primary key. `OR IGNORE`
            // is idempotent: a re-persist of the same label set is a no-op.
            for (ordinal, label) in dedupe_labels_ci(&runner.labels).iter().enumerate() {
                tx.execute(
                    "INSERT OR IGNORE INTO runner_labels(runner_id, label, ordinal) VALUES (?1, ?2, ?3)",
                    params![runner.id, label, ordinal as i64],
                )?;
            }
        }
        for session in &snapshot.sessions {
            // `session_id` is the natural primary key; a re-persist
            // overwrites the same row.
            let key_blob = session_keys
                .get(&session.session_id.0.to_string())
                .map(|enc| seal_session_key(&self.cipher, enc));
            match key_blob {
                Some((ct, iv, tag)) => {
                    tx.execute(
                    "INSERT OR REPLACE INTO runner_sessions(session_id, runner_id, protocol,
                                                            session_key_blob, session_iv, session_tag,
                                                            created_at_us, last_seen_at_us)
                     VALUES (?1, ?2, 'broker', ?3, ?4, ?5, ?6, ?6)",
                    params![
                        session.session_id.0.to_string(),
                        session.runner_id,
                        ct,
                        iv,
                        tag,
                        now_us()
                    ],
                    )?;
                }
                None => {
                    tx.execute(
                        "INSERT OR REPLACE INTO runner_sessions(session_id, runner_id, protocol,
                                                            created_at_us, last_seen_at_us)
                     VALUES (?1, ?2, 'broker', ?3, ?3)",
                        params![
                            session.session_id.0.to_string(),
                            session.runner_id,
                            now_us()
                        ],
                    )?;
                }
            }
        }
        for record in &snapshot.requests {
            self.insert_request_tx(&tx, record)?;
        }
        // Steps are keyed by attempt, and the request records are what tie an
        // attempt to its run.
        for record in &snapshot.requests {
            if let Some((_, steps, revision)) = snapshot
                .job_steps
                .iter()
                .find(|(agent_job_id, _, _)| *agent_job_id == record.agent_job_id)
            {
                self.write_job_steps_tx(&tx, record.run_id, record.agent_job_id, steps, *revision)?;
            }
        }
        self.write_claim_state_tx(
            &tx,
            &snapshot.session_active_requests,
            &snapshot.inflight,
            &snapshot.broker_request_messages,
        )?;
        self.write_meta_tx(&tx, &snapshot.meta)?;
        tx.commit()
            .map_err(|error| anyhow::anyhow!("committing store snapshot: {error}"))?;
        // The WAL grows with every runner event; an unbounded WAL (hundreds of
        // MB) makes a later checkpoint sync stall the server for minutes.
        // Force a truncation periodically (autocheckpoint bounds the rest) so
        // the file is reclaimed without an fsync on every commit.
        self.maybe_checkpoint_wal(&connection)?;
        Ok(())
    }

    /// Persist only one run's mutable projection. This is the hot path used
    /// after runner events; rebuilding every run on every status transition
    /// turns a burst of independent submissions into quadratic work.
    pub(crate) fn store_run_event(&self, projection: &RunProjection) -> anyhow::Result<()> {
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let tx = connection.transaction()?;
        let run_id = projection.run.run_id;
        self.store_run_tx(&tx, &projection.run)?;
        tx.execute("DELETE FROM jobs WHERE run_id = ?1", [run_id.to_string()])?;
        for (kind, job, position) in &projection.jobs {
            self.insert_job(&tx, job, kind, *position)?;
        }
        tx.execute(
            "DELETE FROM job_requests WHERE run_id = ?1",
            [run_id.to_string()],
        )?;
        for record in &projection.requests {
            self.insert_request_tx(&tx, record)?;
        }
        // Steps are not part of this projection: they change far more often
        // than the rest of a run, and rewriting every attempt's rows on each
        // transition is quadratic in matrix width against a single writer.
        // `store_job_steps` persists the one attempt that changed instead.
        //
        // The claim state must land in the same transaction as the queue
        // rewrite above: a job that was claimed (dequeued, message handed to a
        // session) but not yet acked would otherwise have neither its queue
        // row nor its claim after a restart.
        self.write_claim_state_tx(
            &tx,
            &projection.session_active_requests,
            &projection.inflight,
            &projection.broker_request_messages,
        )?;
        self.insert_event_tx(&tx, &projection.event)?;
        tx.commit()
            .map_err(|error| anyhow::anyhow!("committing run event: {error}"))?;
        // Same rationale as the full-snapshot path: bound the WAL so a
        // runner-event burst cannot stall a later commit behind a giant
        // checkpoint sync — periodically, not on every event.
        self.maybe_checkpoint_wal(&connection)?;
        Ok(())
    }

    /// Persist one attempt's step rows in their own transaction.
    pub(crate) fn store_job_steps(
        &self,
        run_id: RunId,
        agent_job_id: uuid::Uuid,
        records: &[crate::models::StepRecord],
        revision: u64,
    ) -> anyhow::Result<()> {
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let tx = connection.transaction()?;
        self.write_job_steps_tx(&tx, run_id, agent_job_id, records, revision)?;
        tx.commit()
            .map_err(|error| anyhow::anyhow!("committing job steps: {error}"))?;
        self.maybe_checkpoint_wal(&connection)?;
        Ok(())
    }

    /// Persist the run-number allocator without rewriting the encrypted
    /// runtime snapshot. Run submission is a hot path, and the full snapshot
    /// grows with the number of workflow paths.
    pub(crate) fn store_workflow_run_counter(
        &self,
        workflow_path: &str,
        next_run_number: u64,
    ) -> anyhow::Result<()> {
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let tx = connection.transaction()?;
        tx.execute(
            "INSERT INTO workflow_run_counters(repository_key, workflow_path, next_run_number)
             VALUES ('', ?1, ?2)
             ON CONFLICT(repository_key, workflow_path) DO UPDATE SET
               next_run_number = excluded.next_run_number",
            params![workflow_path, next_run_number as i64],
        )?;
        tx.commit()
            .map_err(|error| anyhow::anyhow!("committing workflow run counter: {error}"))?;
        self.maybe_checkpoint_wal(&connection)?;
        Ok(())
    }

    pub(crate) fn store_meta_only(&self, meta: &MetaSnapshot) -> anyhow::Result<()> {
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let tx = connection.transaction()?;
        self.write_meta_tx(&tx, meta)?;
        tx.commit()
            .map_err(|error| anyhow::anyhow!("committing metadata: {error}"))?;
        self.maybe_checkpoint_wal(&connection)?;
        Ok(())
    }

    fn store_run_tx(&self, tx: &Transaction<'_>, run: &RunRecord) -> anyhow::Result<()> {
        let value = run_record_value(run)?;
        tx.execute(
            "INSERT INTO runs(run_id, repository, workflow_path, status, run_number,
                              run_attempt, created_at_us, completed_at_us, record_blob)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(run_id) DO UPDATE SET
               repository = excluded.repository,
               workflow_path = excluded.workflow_path,
               status = excluded.status,
               run_number = excluded.run_number,
               run_attempt = excluded.run_attempt,
               created_at_us = excluded.created_at_us,
               completed_at_us = excluded.completed_at_us,
               record_blob = excluded.record_blob",
            params![
                run.run_id.to_string(),
                run.submission.repository.clone(),
                run.workflow_path_str.clone(),
                status_string(run.status),
                run.run_number as i64,
                run.run_attempt as i64,
                unix_us(run.created_at),
                run.completed_at.map(unix_us),
                self.cipher.seal(&serde_json::to_vec(&value)?)?,
            ],
        )?;
        Ok(())
    }

    fn insert_request_tx(
        &self,
        tx: &Transaction<'_>,
        record: &TaskAgentJobRequestRecord,
    ) -> anyhow::Result<()> {
        let snapshot = request_snapshot(record);
        tx.execute(
            "INSERT INTO job_requests(request_id, run_id, job_id, agent_job_id,
                                      plan_id, timeline_id, state, request_blob)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'active', ?7)",
            params![
                record.request_id,
                record.run_id.to_string(),
                record.job_id.0,
                record.agent_job_id.to_string(),
                record.plan_id,
                record.timeline_id.to_string(),
                self.cipher.seal(&serde_json::to_vec(&snapshot)?)?,
            ],
        )?;
        Ok(())
    }

    /// The stored spelling of a step's kind.
    ///
    /// Plaintext and constrained by a `CHECK`, because `--step` filters on it.
    fn step_kind_str(kind: crate::models::StepKind) -> &'static str {
        match kind {
            crate::models::StepKind::Workflow => "workflow",
            crate::models::StepKind::Synthetic => "synthetic",
        }
    }

    /// Upsert one run's attempt step rows.
    ///
    /// Deliberately no `DELETE ... WHERE run_id` first: manifests only grow or
    /// have fields updated, so a keyed upsert writes just the changed rows.
    /// Deleting and re-inserting per run event would reintroduce the very
    /// write amplification this table exists to avoid. Rows go away with their
    /// run, through the `runs` foreign key.
    fn write_job_steps_tx(
        &self,
        tx: &Transaction<'_>,
        run_id: RunId,
        agent_job_id: uuid::Uuid,
        records: &[crate::models::StepRecord],
        revision: u64,
    ) -> anyhow::Result<()> {
        {
            for step in records {
                if step.id.is_empty() {
                    continue;
                }
                tx.execute(
                    "INSERT INTO job_steps(run_id, agent_job_id, step_id, kind, workflow_index,
                                           runner_number, context_name, name_blob, conclusion,
                                           started_at_us, finished_at_us, revision)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                     ON CONFLICT(agent_job_id, step_id) DO UPDATE SET
                       kind = excluded.kind,
                       workflow_index = excluded.workflow_index,
                       runner_number = excluded.runner_number,
                       context_name = excluded.context_name,
                       name_blob = excluded.name_blob,
                       conclusion = excluded.conclusion,
                       started_at_us = excluded.started_at_us,
                       finished_at_us = excluded.finished_at_us,
                       revision = excluded.revision
                     WHERE excluded.revision >= job_steps.revision",
                    params![
                        run_id.to_string(),
                        agent_job_id.to_string(),
                        step.id,
                        Self::step_kind_str(step.kind),
                        step.workflow_index.map(|index| index as i64),
                        step.runner_number.map(|number| number as i64),
                        step.context_name,
                        self.cipher.seal(step.name.as_bytes())?,
                        step.conclusion,
                        step.started_at.map(unix_us),
                        step.finished_at.map(unix_us),
                        revision as i64,
                    ],
                )?;
            }
        }
        Ok(())
    }

    fn write_meta_tx(&self, tx: &Transaction<'_>, meta: &MetaSnapshot) -> anyhow::Result<()> {
        tx.execute(
            "INSERT INTO runtime_snapshots(snapshot_id, format_version, meta_blob, written_at_us)
             VALUES (1, ?1, ?2, ?3)
             ON CONFLICT(snapshot_id) DO UPDATE SET
               format_version = excluded.format_version,
               meta_blob = excluded.meta_blob,
               written_at_us = excluded.written_at_us",
            params![
                SNAPSHOT_FORMAT as i64,
                self.cipher.seal(&serde_json::to_vec(&meta)?)?,
                now_us()
            ],
        )?;
        Ok(())
    }

    /// Persist the claim/message state: per-session active requests, per-session
    /// undelivered broker messages, and per-request job messages. Rewrites all
    /// rows from the captured projections; the maps are bounded by the number
    /// of live claims.
    fn write_claim_state_tx(
        &self,
        tx: &Transaction<'_>,
        session_active_requests: &[(String, i64)],
        inflight: &[(String, i64, azdo::TaskAgentMessage)],
        broker_request_messages: &[(i64, azdo::AgentJobRequestMessage)],
    ) -> anyhow::Result<()> {
        tx.execute("DELETE FROM session_active_requests", [])?;
        for (session_id, request_id) in session_active_requests {
            tx.execute(
                "INSERT INTO session_active_requests(session_id, active_request_id) VALUES (?1, ?2)",
                params![session_id, *request_id],
            )?;
        }
        tx.execute("DELETE FROM broker_messages", [])?;
        for (session_id, message_id, payload) in inflight {
            let payload_json = serde_json::to_string(payload)?;
            tx.execute(
                "INSERT INTO broker_messages(session_id, message_id, payload_json, written_at_us) VALUES (?1, ?2, ?3, ?4)",
                params![session_id, *message_id, payload_json, now_us()],
            )?;
        }
        tx.execute("DELETE FROM job_request_messages", [])?;
        for (request_id, payload) in broker_request_messages {
            let payload_json = serde_json::to_string(payload)?;
            tx.execute(
                "INSERT INTO job_request_messages(request_id, payload_json, written_at_us) VALUES (?1, ?2, ?3)",
                params![*request_id, payload_json, now_us()],
            )?;
        }
        Ok(())
    }

    fn insert_job(
        &self,
        tx: &Transaction<'_>,
        job: &QueuedJob,
        queue_kind: &str,
        position: i64,
    ) -> anyhow::Result<()> {
        let payload = self.cipher.seal(&serde_json::to_vec(job)?)?;
        tx.execute(
            "INSERT INTO jobs(run_id, job_id, status, queue_kind, queue_position, payload_blob)
             VALUES (?1, ?2, 'queued', ?3, ?4, ?5)",
            params![
                job.run_id.to_string(),
                job.job_id.0,
                queue_kind,
                position,
                payload,
            ],
        )?;
        for (dependency, _) in job.needs.iter().enumerate() {
            let depends_on = &job.needs[dependency];
            tx.execute(
                "INSERT INTO job_dependencies(run_id, job_id, depends_on_job_id)
                 VALUES (?1, ?2, ?3)",
                params![job.run_id.to_string(), job.job_id.0, depends_on.0],
            )?;
        }
        Ok(())
    }

    pub(crate) fn append_event(&self, event: &NdjsonEvent) -> anyhow::Result<()> {
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let tx = connection.transaction()?;
        self.insert_event_tx(&tx, event)?;
        tx.commit()
            .map_err(|error| anyhow::anyhow!("committing control event: {error}"))?;
        self.maybe_checkpoint_wal(&connection)?;
        Ok(())
    }

    fn insert_event_tx(&self, tx: &Transaction<'_>, event: &NdjsonEvent) -> anyhow::Result<()> {
        let (run_id, job_id, event_type) = match event {
            NdjsonEvent::RunAccepted { run_id, .. } => (*run_id, None, "run_accepted"),
            NdjsonEvent::RunStatus { run_id, .. } => (*run_id, None, "run_status"),
            NdjsonEvent::JobStatus { run_id, job_id, .. } => {
                (*run_id, Some(job_id.clone()), "job_status")
            }
            _ => return Ok(()),
        };
        let payload = serde_json::to_string(event)?;
        tx.execute(
            "INSERT INTO control_events(run_id, job_id, event_type, payload_json, created_at_us)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                run_id.to_string(),
                job_id.map(|id| id.0),
                event_type,
                payload,
                now_us()
            ],
        )?;
        Ok(())
    }
}

#[async_trait]
impl Store for SqliteStore {
    async fn load_into(&self, inner: &mut InnerState) -> anyhow::Result<()> {
        SqliteStore::load_into(self, inner)
    }

    async fn store_inner(&self, snapshot: &StoreSnapshot) -> anyhow::Result<()> {
        let store = self.clone();
        let snapshot = snapshot.clone();
        tokio::task::spawn_blocking(move || store.store_inner(&snapshot))
            .await
            .map_err(|error| anyhow::anyhow!("store snapshot task panicked: {error}"))?
    }

    async fn store_meta_only(&self, meta: &MetaSnapshot) -> anyhow::Result<()> {
        let store = self.clone();
        let meta = meta.clone();
        tokio::task::spawn_blocking(move || store.store_meta_only(&meta))
            .await
            .map_err(|error| anyhow::anyhow!("store metadata task panicked: {error}"))?
    }

    async fn store_run_event(&self, projection: RunProjection) -> anyhow::Result<()> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.store_run_event(&projection))
            .await
            .map_err(|error| anyhow::anyhow!("store run-event task panicked: {error}"))?
    }

    async fn store_job_steps(
        &self,
        run_id: RunId,
        agent_job_id: uuid::Uuid,
        records: &[crate::models::StepRecord],
        revision: u64,
    ) -> anyhow::Result<()> {
        let store = self.clone();
        let records = records.to_vec();
        tokio::task::spawn_blocking(move || {
            store.store_job_steps(run_id, agent_job_id, &records, revision)
        })
        .await
        .map_err(|error| anyhow::anyhow!("store job-steps task panicked: {error}"))?
    }

    async fn store_workflow_run_counter(
        &self,
        workflow_path: &str,
        next_run_number: u64,
    ) -> anyhow::Result<()> {
        let store = self.clone();
        let workflow_path = workflow_path.to_owned();
        tokio::task::spawn_blocking(move || {
            store.store_workflow_run_counter(&workflow_path, next_run_number)
        })
        .await
        .map_err(|error| anyhow::anyhow!("store counter task panicked: {error}"))?
    }

    async fn store_log_chunk(
        &self,
        key: &str,
        chunk_index: i64,
        payload: &[u8],
        byte_count: i64,
        line_count: i64,
    ) -> anyhow::Result<()> {
        let store = self.clone();
        let key = key.to_owned();
        let payload = payload.to_vec();
        tokio::task::spawn_blocking(move || {
            store.store_log_chunk(&key, chunk_index, &payload, byte_count, line_count)
        })
        .await
        .map_err(|error| anyhow::anyhow!("store log-chunk task panicked: {error}"))?
    }

    async fn delete_log(&self, key: &str) -> anyhow::Result<()> {
        let store = self.clone();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || store.delete_log(&key))
            .await
            .map_err(|error| anyhow::anyhow!("delete log task panicked: {error}"))?
    }

    async fn append_event(&self, event: &NdjsonEvent) -> anyhow::Result<()> {
        let store = self.clone();
        let event = event.clone();
        tokio::task::spawn_blocking(move || store.append_event(&event))
            .await
            .map_err(|error| anyhow::anyhow!("store event task panicked: {error}"))?
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SessionKeyPayload(Vec<u8>);

const MIGRATIONS: &[(u32, &str, &str)] = &[
    (
        1,
        "initial-control-plane-schema",
        r#"
        CREATE TABLE IF NOT EXISTS workflow_run_counters (
          repository_key TEXT NOT NULL,
          workflow_path TEXT NOT NULL,
          next_run_number INTEGER NOT NULL CHECK (next_run_number >= 1),
          PRIMARY KEY (repository_key, workflow_path)
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS runs (
          run_id TEXT PRIMARY KEY,
          repository TEXT NOT NULL,
          workflow_path TEXT NOT NULL,
          status TEXT NOT NULL,
          run_number INTEGER NOT NULL,
          run_attempt INTEGER NOT NULL,
          created_at_us INTEGER NOT NULL,
          completed_at_us INTEGER,
          record_blob BLOB NOT NULL
        ) STRICT;

        CREATE TABLE IF NOT EXISTS run_secrets (
          run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
          crypto_version INTEGER NOT NULL DEFAULT 1,
          secret_blob BLOB NOT NULL
        ) STRICT;

        CREATE TABLE IF NOT EXISTS runners (
          runner_id INTEGER PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          ephemeral INTEGER NOT NULL CHECK (ephemeral IN (0,1)),
          runner_group_id INTEGER,
          runner_group_name TEXT,
          public_key TEXT,
          rsa_public_key TEXT,
          created_at_us INTEGER NOT NULL,
          updated_at_us INTEGER NOT NULL,
          deleted_at_us INTEGER
        ) STRICT;

        CREATE TABLE IF NOT EXISTS runner_labels (
          runner_id INTEGER NOT NULL REFERENCES runners(runner_id) ON DELETE CASCADE,
          label TEXT NOT NULL,
          ordinal INTEGER NOT NULL,
          PRIMARY KEY (runner_id, label)
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS runner_sessions (
          session_id TEXT PRIMARY KEY,
          runner_id INTEGER NOT NULL,
          protocol TEXT NOT NULL,
          client_id TEXT,
          session_key_blob BLOB,
          session_iv BLOB,
          session_tag BLOB,
          created_at_us INTEGER NOT NULL,
          last_seen_at_us INTEGER NOT NULL,
          closed_at_us INTEGER
        ) STRICT;

        CREATE TABLE IF NOT EXISTS jobs (
          run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
          job_id TEXT NOT NULL,
          status TEXT NOT NULL,
          queue_kind TEXT NOT NULL CHECK (
            queue_kind IN ('ready','pending','blocked','held')
          ),
          queue_position INTEGER NOT NULL,
          payload_blob BLOB NOT NULL,
          PRIMARY KEY (run_id, job_id)
        ) STRICT, WITHOUT ROWID;

        CREATE INDEX IF NOT EXISTS jobs_claim_idx
          ON jobs (queue_kind, queue_position)
          WHERE status IN ('queued','pending');

        CREATE TABLE IF NOT EXISTS job_dependencies (
          run_id TEXT NOT NULL,
          job_id TEXT NOT NULL,
          depends_on_job_id TEXT NOT NULL,
          PRIMARY KEY (run_id, job_id, depends_on_job_id),
          FOREIGN KEY (run_id, job_id)
            REFERENCES jobs(run_id, job_id) ON DELETE CASCADE
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS job_requests (
          request_id INTEGER PRIMARY KEY,
          run_id TEXT NOT NULL,
          job_id TEXT NOT NULL,
          agent_job_id TEXT NOT NULL UNIQUE,
          plan_id TEXT NOT NULL UNIQUE,
          timeline_id TEXT NOT NULL UNIQUE,
          state TEXT NOT NULL,
          request_blob BLOB NOT NULL,
          FOREIGN KEY (run_id)
            REFERENCES runs(run_id) ON DELETE CASCADE
        ) STRICT;

        CREATE TABLE IF NOT EXISTS runner_commands (
          message_id INTEGER PRIMARY KEY,
          command_type TEXT NOT NULL,
          run_id TEXT NOT NULL,
          job_id TEXT NOT NULL,
          payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
          state TEXT NOT NULL,
          created_at_us INTEGER NOT NULL
        ) STRICT;

        CREATE TABLE IF NOT EXISTS control_events (
          event_id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id TEXT NOT NULL,
          job_id TEXT,
          event_type TEXT NOT NULL,
          payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
          created_at_us INTEGER NOT NULL
        ) STRICT;

        CREATE INDEX IF NOT EXISTS control_events_cursor_idx
          ON control_events (run_id, event_id);

        CREATE TABLE IF NOT EXISTS session_active_requests (
          session_id TEXT PRIMARY KEY,
          active_request_id INTEGER NOT NULL
            REFERENCES job_requests(request_id) ON DELETE CASCADE
        ) STRICT;

        CREATE TABLE IF NOT EXISTS broker_messages (
          session_id TEXT NOT NULL,
          message_id INTEGER NOT NULL,
          payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
          written_at_us INTEGER NOT NULL,
          PRIMARY KEY (session_id, message_id)
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS log_files (
          log_key TEXT PRIMARY KEY,
          byte_count INTEGER NOT NULL DEFAULT 0 CHECK (byte_count >= 0),
          line_count INTEGER NOT NULL DEFAULT 0 CHECK (line_count >= 0),
          updated_at_us INTEGER NOT NULL
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS log_chunks (
          log_key TEXT NOT NULL REFERENCES log_files(log_key) ON DELETE CASCADE,
          chunk_index INTEGER NOT NULL,
          payload BLOB NOT NULL,
          written_at_us INTEGER NOT NULL,
          PRIMARY KEY (log_key, chunk_index)
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS runtime_snapshots (
          snapshot_id INTEGER PRIMARY KEY CHECK (snapshot_id = 1),
          format_version INTEGER NOT NULL,
          meta_blob BLOB NOT NULL,
          written_at_us INTEGER NOT NULL
        ) STRICT;
        "#,
    ),
    (
        2,
        "drop-redundant-run-secrets",
        // `runs.record_blob` already carries the submission through
        // `WorkflowSubmission::to_request_json`, which is the sanctioned
        // expose boundary. This table held a second copy written with the
        // plain `Serialize` impl, i.e. the literal `"<redacted>"`, and
        // `load_into` clobbered the good copy with it.
        r#"
        DROP TABLE IF EXISTS run_secrets;
        "#,
    ),
    (
        3,
        "job-request-messages-table",
        // `inner.broker_messages` (request_id → job message) used to live only
        // in the runtime meta blob, which `store_run_event` never rewrites —
        // a job claimed right before a restart had neither its queue row nor
        // its broker payload. Its own table lets the hot path persist the
        // claim in the same transaction as the queue rewrite.
        r#"
        CREATE TABLE IF NOT EXISTS job_request_messages (
          request_id INTEGER PRIMARY KEY,
          payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
          written_at_us INTEGER NOT NULL
        ) STRICT;
        "#,
    ),
    (
        4,
        "job-steps-table",
        // Step records get their own table rather than riding in
        // `runs.record_blob`: that blob carries the workflow YAML, the event
        // payload and the github context, and `store_run_event` reseals all of
        // it on every run event. Rows here are upserted individually, so a
        // step transition writes one small row instead of re-encrypting the
        // whole run. `MetaSnapshot` is not an option either — every field in
        // it is cloned and sealed on each `store_meta_only` call.
        //
        // Keyed by `agent_job_id` (the attempt), because a re-dispatch mints
        // fresh step ids and must not overwrite the mapping the previous
        // attempt's `step-<id>.txt` blobs are named after.
        //
        // `kind` and `workflow_index` stay plaintext so `--step` can resolve
        // through an indexed query; only the display name is sealed, since it
        // comes from user YAML.
        r#"
        CREATE TABLE IF NOT EXISTS job_steps (
          run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
          agent_job_id TEXT NOT NULL,
          step_id TEXT NOT NULL,
          kind TEXT NOT NULL CHECK (kind IN ('workflow','synthetic')),
          workflow_index INTEGER,
          runner_number INTEGER,
          context_name TEXT,
          name_blob BLOB NOT NULL,
          conclusion TEXT NOT NULL,
          started_at_us INTEGER,
          finished_at_us INTEGER,
          -- Monotonic per attempt, bumped on every in-memory mutation. Two
          -- reports for one attempt snapshot under the lock and then commit
          -- outside it, so they can commit out of order; the upsert refuses to
          -- move a row backwards rather than letting the older snapshot
          -- overwrite the newer conclusions.
          revision INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY (agent_job_id, step_id)
        ) STRICT, WITHOUT ROWID;

        CREATE INDEX IF NOT EXISTS job_steps_order_idx
          ON job_steps (agent_job_id, kind, workflow_index);
        "#,
    ),
];

pub(crate) fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(i64::MAX as u128) as i64
}

pub(crate) fn unix_us(value: chrono::DateTime<chrono::Utc>) -> i64 {
    value.timestamp_micros()
}

fn system_time_us(value: SystemTime) -> i64 {
    value
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(i64::MAX as u128) as i64
}

fn system_time_from_us(value: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(value.max(0) as u64)
}

/// Every queued job paired with its `queue_kind` and a **globally** assigned
/// `queue_position` within that kind.
///
/// Restore reads `ORDER BY queue_kind, queue_position` and pushes into the
/// matching container, so positions only have to be consistent *within* a
/// kind — but they must be consistent across writers. `store_inner` rewrites
/// every row while `store_run_event` rewrites one run's rows, so both derive
/// the position from the same global index instead of numbering from zero
/// per run. Numbering per run made every run's first job `position = 0`,
/// which collapsed cross-run FIFO order on the next restart.
///
/// The queues are FIFO (push-back / pop-front), so a stale absolute index
/// left behind by another run's write still orders correctly relative to
/// later writes: popping the front shifts everyone down uniformly and
/// pushing to the back always yields a larger index than anything present.
pub(crate) fn queue_rows(inner: &InnerState) -> Vec<(&'static str, &QueuedJob, i64)> {
    let mut rows = Vec::new();
    for (kind, jobs) in [
        ("ready", &inner.queue),
        ("pending", &inner.pending_jobs),
        ("blocked", &inner.concurrency_blocked),
    ] {
        rows.extend(
            jobs.iter()
                .enumerate()
                .map(|(index, job)| (kind, job, index as i64)),
        );
    }
    rows.extend(
        inner
            .held_runs
            .values()
            .flatten()
            .enumerate()
            .map(|(index, job)| ("held", job, index as i64)),
    );
    rows
}

/// [`queue_rows`] restricted to one run, keeping the global positions.
pub(crate) fn queue_rows_for_run(
    inner: &InnerState,
    run_id: RunId,
) -> Vec<(&'static str, &QueuedJob, i64)> {
    let mut rows = queue_rows(inner);
    rows.retain(|(_, job, _)| job.run_id == run_id);
    rows
}

/// Deduplicate label strings case-insensitively, preserving first occurrence.
///
/// The handler layer already runs this when a runner registers; this is the
/// database-side backstop so a direct mutation of `inner.runners[id].labels`
/// from another code path still produces a valid `(runner_id, label)` insert.
/// Matches the case-insensitive semantics of
/// `runtime_scheduling::job_matches_runner`.
pub(crate) fn dedupe_labels_ci(labels: &[String]) -> Vec<String> {
    let mut seen: std::collections::HashSet<String> =
        std::collections::HashSet::with_capacity(labels.len());
    let mut out: Vec<String> = Vec::with_capacity(labels.len());
    for label in labels {
        if seen.insert(label.to_lowercase()) {
            out.push(label.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_url_parsing() {
        // Empty → default sqlite in the state dir.
        assert_eq!(
            parse_store_url("").unwrap(),
            StoreUrl::Sqlite(std::path::PathBuf::new())
        );
        assert_eq!(
            parse_store_url("   ").unwrap(),
            StoreUrl::Sqlite(std::path::PathBuf::new())
        );
        // sqlite forms.
        assert_eq!(
            parse_store_url("sqlite:///tmp/preloop.db").unwrap(),
            StoreUrl::Sqlite(std::path::PathBuf::from("/tmp/preloop.db"))
        );
        assert_eq!(
            parse_store_url("sqlite:relative.db").unwrap(),
            StoreUrl::Sqlite(std::path::PathBuf::from("relative.db"))
        );
        // Bare path keeps the old behaviour.
        assert_eq!(
            parse_store_url("/var/lib/preloop.db").unwrap(),
            StoreUrl::Sqlite(std::path::PathBuf::from("/var/lib/preloop.db"))
        );
        // Postgres forms.
        assert_eq!(
            parse_store_url("postgres://preloop:secret@localhost:5432/preloop").unwrap(),
            StoreUrl::Postgres("postgres://preloop:secret@localhost:5432/preloop".to_owned())
        );
        assert_eq!(
            parse_store_url("postgresql://preloop@db.example/preloop").unwrap(),
            StoreUrl::Postgres("postgresql://preloop@db.example/preloop".to_owned())
        );
        // Unsupported schemes are rejected loudly.
        assert!(parse_store_url("mysql://localhost/preloop").is_err());
        assert!(parse_store_url("redis://localhost/0").is_err());
        assert!(parse_store_url("mongodb://localhost/preloop").is_err());
    }

    #[test]
    fn envelope_roundtrip_and_tamper_detection() {
        let envelope = Envelope::new(b"test-root-key");
        let sealed = envelope.seal(b"secret payload").unwrap();
        assert_eq!(envelope.unseal(&sealed).unwrap(), b"secret payload");

        // Any flipped byte (version, IV, ciphertext, tag) must fail auth.
        for index in [0usize, 1, 7, sealed.len() / 2, sealed.len() - 1] {
            let mut tampered = sealed.clone();
            tampered[index] ^= 0x01;
            assert!(
                envelope.unseal(&tampered).is_err(),
                "tampered envelope at byte {index} must not unseal"
            );
        }
    }
}
