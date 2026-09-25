//! The SQLite `ControlBackend`: the default, single-node authority.
//!
//! One connection behind a mutex (single writer), WAL for concurrent
//! readers, `BEGIN IMMEDIATE` so a writer never upgrades mid-transaction.
//! Every command is one transaction: [`load_txstate`] builds the working
//! set, the shared [`crate::control::sched`] logic runs on it, and
//! [`write_txstate`] persists the delta — upserting present rows and
//! deleting rows that were loaded but removed — before commit.
//!
//! The database is the serialization and fencing boundary: two commands
//! never interleave inside a transaction, and a crash mid-command rolls
//! back cleanly. Node-local state (live logs, DAP ports, debug sessions)
//! never enters the working set — it is applied from [`TxSideEffects`]
//! after commit.
//!
//! The control store lives in its own file, `<state_dir>/control.db` —
//! deliberately separate from the legacy `preloop.db`, which `store.rs`
//! still owns (its `runs`/`jobs` tables and `PRAGMA user_version` are
//! incompatible with this schema). The cutover imports `preloop.db` state
//! into `control.db` and retires the legacy file; until then the two never
//! share a file, a table, or a version counter.

use super::backend::*;
use super::commands;
use super::sched;
use super::schema::{SQLITE_DDL, SQLITE_SCHEMA_VERSION};
use super::txstate::{TxScope, TxState};
use super::types::*;
use crate::concurrency;
use crate::models::{
    QueuedJob, RunRecord, TaskAgentJobRequestRecord, WebhookDeliveryRecord, WebhookDeliveryStatus,
    WebhookDeliverySummary, WebhookQueueStats, WebhookRedeliveryRecord, WebhookWatchdogCursor,
};
use crate::state::JobSetId;
use crate::store;
use parking_lot::Mutex;
use preloop_gha_protocol::crypto::{AgentRsaPublicKey, SessionEncryption};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId, SessionId};
use rusqlite::{params, Connection, OptionalExtension};

use std::collections::{BTreeSet, VecDeque};

/// The SQLite control backend. `conn` is the single writer (`BEGIN
/// IMMEDIATE`); `readers` is a pool of `query_only` connections that serve
/// read-only commands concurrently under WAL, so a read never queues
/// behind a write. The database file is the cross-process fence.
///
/// A single `Mutex<Connection>` would serialize reads behind writes with
/// fsync in the critical section — strictly worse than the in-memory lock
/// this replaces on the hot read path (`acquirejob` context, status,
/// queue stats). The reader pool is what makes the cutover a serialization
/// win rather than a regression.
pub(crate) struct SqliteBackend {
    conn: Mutex<Connection>,
    /// Read-only connections handed out by [`SqliteBackend::read`]. Empty
    /// for an in-memory database (a second connection can't see it), where
    /// `read` falls back to the writer.
    readers: Mutex<Vec<Connection>>,
    readers_idle: parking_lot::Condvar,
    /// AEAD envelope sealing every `*_blob` column — the same envelope the
    /// legacy store applies to `preloop.db`, so a stolen `control.db` (or a
    /// read-only Postgres replica) yields ciphertext, not workflow secrets.
    cipher: store::Envelope,
    /// Auxiliary SQL operations that have not yet been folded into this
    /// module. They use a second connection to the same authoritative file;
    /// there is no second database.
    aux: Option<crate::store::SqliteStore>,
    pool_assignments_enabled: std::sync::atomic::AtomicBool,
    require_job_assignments: std::sync::atomic::AtomicBool,
    runner_liveness_timeout: std::sync::atomic::AtomicU64,
}

/// Run `f` without stalling the async executor. On a multi-thread runtime
/// `block_in_place` hands the blocking section a spare worker so a long
/// write transaction cannot starve concurrent reads; on a `current_thread`
/// runtime (unit tests) there is no spare worker to give, so `f` runs
/// inline — `block_in_place` would panic there.
fn run_blocking<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

impl SqliteBackend {
    /// Open (or create) the control database and migrate it to the current
    /// schema version.
    pub(crate) fn open(
        path: &std::path::Path,
        cipher: store::Envelope,
        pool_assignments_enabled: bool,
        require_job_assignments: bool,
        runner_liveness_timeout: std::time::Duration,
    ) -> Result<Self, ControlError> {
        // The control database holds sealed workflow secrets and session
        // keys; create it owner-only before SQLite materializes the file,
        // matching the 0600 convention every other state artifact follows.
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let _ = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                // Never truncate — this open exists only to set 0600 on a
                // newly created file; `Connection::open` does the real open.
                .truncate(false)
                .mode(0o600)
                .open(path)
                .map_err(ControlError::backend)?;
        }
        let conn = Connection::open(path).map_err(ControlError::backend)?;
        // Apply pending migrations (see `schema::SQLITE_MIGRATIONS`) before
        // the idempotent DDL. Foreign-key enforcement is disabled for the
        // whole pass: table rebuilds need it, and FK-off is correct for any
        // schema rewrite. Each migration and its version stamp commit
        // atomically so a failed rebuild can be retried without inheriting a
        // half-replaced table. Foreign keys are re-enabled after the pass (a
        // no-op inside a transaction, hence set before the batches).
        let existing_version: i64 = conn
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .map_err(ControlError::backend)?;
        if (1..SQLITE_SCHEMA_VERSION).contains(&existing_version) {
            conn.pragma_update(None, "foreign_keys", false)
                .map_err(ControlError::backend)?;
            for (version, sql) in super::schema::SQLITE_MIGRATIONS {
                if *version <= existing_version {
                    continue;
                }
                let transaction = conn
                    .unchecked_transaction()
                    .map_err(ControlError::backend)?;
                transaction
                    .execute_batch(sql)
                    .map_err(ControlError::backend)?;
                transaction
                    .pragma_update(None, "user_version", *version)
                    .map_err(ControlError::backend)?;
                transaction.commit().map_err(ControlError::backend)?;
            }
            conn.pragma_update(None, "foreign_keys", true)
                .map_err(ControlError::backend)?;
        }
        conn.execute_batch(SQLITE_DDL)
            .map_err(ControlError::backend)?;
        conn.pragma_update(None, "user_version", SQLITE_SCHEMA_VERSION)
            .map_err(ControlError::backend)?;
        // Foreign keys default OFF per connection; enable so the schema's
        // ON DELETE CASCADE clauses (broker_messages→sessions, holder_keys→runs,
        // jobset_admissions→runs) actually fire under targeted deletes.
        conn.pragma_update(None, "foreign_keys", true)
            .map_err(ControlError::backend)?;
        // A small pool of read-only connections. WAL lets readers run
        // concurrently with the single writer; `query_only` makes a write
        // through a reader a hard error rather than silent corruption.
        // Four is enough for the read-heavy paths (acquire context, status,
        // queue stats) without fd pressure.
        let mut readers = Vec::with_capacity(4);
        for _ in 0..4 {
            let reader = Connection::open(path).map_err(ControlError::backend)?;
            reader
                .pragma_update(None, "query_only", true)
                .map_err(ControlError::backend)?;
            reader
                .pragma_update(None, "busy_timeout", 5000_i64)
                .map_err(ControlError::backend)?;
            readers.push(reader);
        }
        // WAL/SHM siblings are created lazily by SQLite; tighten them too.
        #[cfg(unix)]
        for suffix in ["-wal", "-shm"] {
            let sibling = std::path::PathBuf::from(format!("{}{}", path.display(), suffix));
            if sibling.exists() {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(&sibling, std::fs::Permissions::from_mode(0o600));
            }
        }
        let aux = crate::store::SqliteStore::open_existing(path, cipher.clone())
            .map_err(ControlError::backend)?;
        Ok(Self {
            conn: Mutex::new(conn),
            readers: Mutex::new(readers),
            readers_idle: parking_lot::Condvar::new(),
            cipher,
            aux: Some(aux),
            pool_assignments_enabled: std::sync::atomic::AtomicBool::new(pool_assignments_enabled),
            require_job_assignments: std::sync::atomic::AtomicBool::new(require_job_assignments),
            runner_liveness_timeout: std::sync::atomic::AtomicU64::new(
                runner_liveness_timeout.as_nanos() as u64,
            ),
        })
    }

    /// An in-memory backend for the behavioral suite.
    #[cfg(test)]
    pub(crate) fn in_memory() -> Result<Self, ControlError> {
        Self::in_memory_with_cipher(store::Envelope::new(b"control-test-key"))
    }

    /// An in-memory backend with an explicit envelope (seal round-trip tests).
    #[cfg(test)]
    pub(crate) fn in_memory_with_cipher(cipher: store::Envelope) -> Result<Self, ControlError> {
        let conn = Connection::open_in_memory().map_err(ControlError::backend)?;
        conn.execute_batch(SQLITE_DDL)
            .map_err(ControlError::backend)?;
        conn.pragma_update(None, "foreign_keys", true)
            .map_err(ControlError::backend)?;
        Ok(Self {
            conn: Mutex::new(conn),
            // A second connection can't see an in-memory database, so the
            // pool is empty and `read` falls back to the writer.
            readers: Mutex::new(Vec::new()),
            readers_idle: parking_lot::Condvar::new(),
            cipher,
            aux: None,
            pool_assignments_enabled: std::sync::atomic::AtomicBool::new(false),
            require_job_assignments: std::sync::atomic::AtomicBool::new(false),
            runner_liveness_timeout: std::sync::atomic::AtomicU64::new(
                std::time::Duration::from_secs(300).as_nanos() as u64,
            ),
        })
    }

    /// The live scheduling config as a `with_config` argument tuple.
    pub(crate) fn config(&self) -> (bool, bool, std::time::Duration) {
        (
            self.pool_assignments_enabled
                .load(std::sync::atomic::Ordering::Acquire),
            self.require_job_assignments
                .load(std::sync::atomic::Ordering::Acquire),
            std::time::Duration::from_nanos(
                self.runner_liveness_timeout
                    .load(std::sync::atomic::Ordering::Acquire),
            ),
        )
    }

    /// Update the scheduling config after open. Bootstrap applies the real
    /// server config here once it is known — the values passed to `open` are
    /// only the recovered defaults.
    pub(crate) fn set_config(
        &self,
        pool_assignments_enabled: bool,
        require_job_assignments: bool,
        runner_liveness_timeout: std::time::Duration,
    ) {
        self.pool_assignments_enabled.store(
            pool_assignments_enabled,
            std::sync::atomic::Ordering::Release,
        );
        self.require_job_assignments.store(
            require_job_assignments,
            std::sync::atomic::Ordering::Release,
        );
        self.runner_liveness_timeout.store(
            runner_liveness_timeout.as_nanos() as u64,
            std::sync::atomic::Ordering::Release,
        );
    }

    /// Dump every table's rows for the differential scope test: table name →
    /// sorted row renderings. Enumerated from `sqlite_master` so a family
    /// added later cannot silently escape the assertion.
    #[cfg(test)]
    pub(crate) fn dump_tables(&self) -> std::collections::BTreeMap<String, Vec<String>> {
        let conn = self.conn.lock();
        let mut out = std::collections::BTreeMap::new();
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap();
        let tables: Vec<String> = stmt
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        for table in tables {
            if table == "sqlite_sequence" {
                continue;
            }
            let mut q = conn.prepare(&format!("SELECT * FROM {table}")).unwrap();
            let names: Vec<String> = q.column_names().iter().map(|s| s.to_string()).collect();
            let cols = names.len();
            let rows = q
                .query_map([], |r| {
                    let mut cells = Vec::with_capacity(cols);
                    for (i, name) in names.iter().enumerate() {
                        let v: rusqlite::types::Value = r.get(i)?;
                        // Sealed blobs are non-deterministic (random IV), so a
                        // byte compare can't tell "unchanged" from "re-sealed".
                        // Render the unsealed plaintext so the assertion is
                        // about logical content, not ciphertext bytes. Applies
                        // to any Blob cell — sealed columns aren't uniformly
                        // named (`running_holder`, `pending_holders`, …).
                        let rendered = if let rusqlite::types::Value::Blob(b) = &v {
                            match self.cipher.unseal(b) {
                                Ok(plain) => format!("{name}=sealed({plain:?})"),
                                Err(_) => format!("{name}={v:?}"),
                            }
                        } else {
                            format!("{name}={v:?}")
                        };
                        cells.push(rendered);
                    }
                    Ok(cells.join("|"))
                })
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            out.insert(table, rows);
        }
        out
    }

    /// Read one raw column value without unsealing — for the at-rest
    /// encryption test, which must see ciphertext bytes, not plaintext.
    #[cfg(test)]
    pub(crate) fn raw_column(&self, table: &str, column: &str) -> Vec<Vec<u8>> {
        let conn = self.conn.lock();
        let mut q = conn
            .prepare(&format!("SELECT {column} FROM {table}"))
            .unwrap();
        q.query_map([], |r| r.get::<_, Vec<u8>>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    /// Run one command as a transaction: `BEGIN IMMEDIATE`, load the
    /// working set, run `f`, write the delta back, `COMMIT`. `f` is the
    /// shared scheduling logic — pure Rust over `TxState`. A failure or
    /// crash anywhere before commit rolls the whole command back, so the
    /// database never holds a half-applied mutation.
    ///
    /// `pub(crate)` so [`crate::control::Backend`] can offer the same
    /// escape hatch over the enum.
    pub(crate) fn transact<T>(
        &self,
        f: impl FnOnce(&mut TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        self.transact_scoped(&TxScope::full(), f)
    }

    /// Run `f` inside one transaction, loading only `scope`. Write-back
    /// touches only the loaded rows, so a narrow scope is also a narrow
    /// write. See [`TxScope`] for the always-global concurrency invariant.
    pub(crate) fn transact_scoped<T>(
        &self,
        scope: &TxScope,
        f: impl FnOnce(&mut TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        // The transaction is synchronous (rusqlite has no async driver), so
        // it would otherwise run on the executor thread and stall every
        // concurrent request for the duration of load→f→write-back→commit.
        run_blocking(|| {
            let mut conn = self.conn.lock();
            let txn = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(ControlError::backend)?;
            let (tx, effective_scope) = load_txstate(&txn, scope, &self.cipher)?;
            let mut tx = tx.with_config(self.config());
            let result = f(&mut tx)?;
            write_txstate(&txn, &tx, &effective_scope, &self.cipher)?;
            txn.commit().map_err(ControlError::backend)?;
            Ok(result)
        })
    }

    /// Run a read-only command on a `query_only` reader connection under
    /// WAL — concurrent with the writer, never queued behind it. `f` sees
    /// one consistent snapshot (a `BEGIN DEFERRED` read transaction). Falls
    /// back to the writer when the pool is empty (in-memory databases).
    ///
    /// `pub(crate)` so [`crate::control::Backend`] can route read-only
    /// commands here instead of serializing them on the writer.
    pub(crate) fn read<T>(
        &self,
        f: impl FnOnce(&TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        self.read_scoped(&TxScope::full(), f)
    }

    /// `read` under a scope — loads only `scope` on the reader snapshot.
    pub(crate) fn read_scoped<T>(
        &self,
        scope: &TxScope,
        f: impl FnOnce(&TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        run_blocking(|| {
            // An empty pool (in-memory databases) can't lend a reader — fall
            // back to the writer, which still gives a consistent snapshot.
            if self.readers.lock().is_empty() {
                let mut conn = self.conn.lock();
                let txn = conn.transaction().map_err(ControlError::backend)?;
                let (tx, _effective_scope) = load_txstate(&txn, scope, &self.cipher)?;
                let tx = tx.with_config(self.config());
                let result = f(&tx)?;
                txn.rollback().map_err(ControlError::backend)?;
                return Ok(result);
            }
            // Check out a reader; block until one is free. A `query_only`
            // connection can never write, so a reader is always safe to lend.
            let mut conn = {
                let mut pool = self.readers.lock();
                loop {
                    if let Some(conn) = pool.pop() {
                        break conn;
                    }
                    self.readers_idle.wait(&mut pool);
                }
            };
            // Return the reader to the pool on every exit path.
            let result = (|| {
                let txn = conn.transaction().map_err(ControlError::backend)?;
                let (tx, _effective_scope) = load_txstate(&txn, scope, &self.cipher)?;
                let tx = tx.with_config(self.config());
                let result = f(&tx)?;
                txn.rollback().map_err(ControlError::backend)?;
                Ok(result)
            })();
            self.readers.lock().push(conn);
            self.readers_idle.notify_one();
            result
        })
    }

    /// Borrow a pooled reader (or the writer for an in-memory database) and
    /// run `f` on it. Read-only point lookups go through here so they never
    /// queue behind the single writer.
    fn with_reader<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        run_blocking(|| {
            if self.readers.lock().is_empty() {
                let conn = self.conn.lock();
                return f(&conn);
            }
            let conn = {
                let mut pool = self.readers.lock();
                loop {
                    if let Some(conn) = pool.pop() {
                        break conn;
                    }
                    self.readers_idle.wait(&mut pool);
                }
            };
            let result = f(&conn);
            self.readers.lock().push(conn);
            self.readers_idle.notify_one();
            result
        })
    }

    /// Find the run a webhook delivery already produced, by its durable
    /// `(delivery_id, workflow_path)` dedup key. O(1) via `runs_delivery` —
    /// the submit path calls this before `transact_scoped` so the replay
    /// check never scans the whole `runs` table.
    pub(crate) fn find_run_by_delivery(
        &self,
        delivery_id: &str,
        workflow_path: &str,
    ) -> Result<Option<RunRecord>, ControlError> {
        self.with_reader(|conn| {
            let row = conn
                .query_row(
                    "SELECT run_id, record_blob FROM runs \
                     WHERE webhook_delivery_id = ?1 AND workflow_path = ?2",
                    params![delivery_id, workflow_path],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?)),
                )
                .optional()
                .map_err(ControlError::backend)?;
            let Some((run_id_s, record)) = row else {
                return Ok(None);
            };
            let mut run: RunRecord = blob(&self.cipher, &record)?;
            run.jobs.clear();
            let mut stmt = conn
                .prepare("SELECT job_id, status FROM jobs WHERE run_id = ?1")
                .map_err(ControlError::backend)?;
            let rows = stmt
                .query_map(params![run_id_s], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })
                .map_err(ControlError::backend)?;
            for row in rows {
                let (job_id_s, status_s) = row.map_err(ControlError::backend)?;
                run.jobs.insert(JobId(job_id_s), status_parse(&status_s));
            }
            Ok(Some(run))
        })
    }

    /// Resolve a request's `(request_id, run_id)` from its `agent_job_id`.
    /// O(1) via `job_requests_agent` — the broker complete path calls this
    /// before `transact_scoped` so the working set stays narrow.
    pub(crate) fn find_request_by_agent_job_id(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<(i64, RunId)>, ControlError> {
        self.with_reader(|conn| {
            conn.query_row(
                "SELECT request_id, run_id FROM job_requests WHERE agent_job_id = ?1",
                params![agent_job_id.to_string()],
                |r| Ok((r.get::<_, i64>(0)?, parse_run_id(&r.get::<_, String>(1)?))),
            )
            .optional()
            .map_err(ControlError::backend)
        })
    }

    /// The session that owns the request for `agent_job_id`. `complete_job`
    /// resolves this before `transact_scoped` so `settle_request` can drop
    /// the moot cancellation from the owner session's inflight messages.
    pub(crate) fn find_session_by_agent_job_id(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<String>, ControlError> {
        self.with_reader(|conn| {
            conn.query_row(
                "SELECT rs.session_id FROM runner_sessions rs \
                 JOIN job_requests jr ON jr.request_id = rs.active_request_id \
                 WHERE jr.agent_job_id = ?1",
                params![agent_job_id.to_string()],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(ControlError::backend)
        })
    }

    /// Every session owning a request of `run_id`. `cancel_run`/`cancel_job`
    /// resolve these before `transact_scoped` so `settle_request` can drop
    /// moot cancellations from each owner session's inflight messages.
    pub(crate) fn find_sessions_by_run(
        &self,
        run_id: RunId,
    ) -> Result<BTreeSet<String>, ControlError> {
        self.with_reader(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT DISTINCT rs.session_id FROM runner_sessions rs \
                     JOIN job_requests jr ON jr.request_id = rs.active_request_id \
                     WHERE jr.run_id = ?1",
                )
                .map_err(ControlError::backend)?;
            let rows = stmt
                .query_map(params![run_id.0.to_string()], |r| r.get::<_, String>(0))
                .map_err(ControlError::backend)?;
            let mut sessions = BTreeSet::new();
            for row in rows {
                sessions.insert(row.map_err(ControlError::backend)?);
            }
            Ok(sessions)
        })
    }

    /// `(run_id, owner_session)` for `request_id`. `acquire_context` resolves
    pub(crate) fn find_request_context(
        &self,
        request_id: i64,
    ) -> Result<Option<(RunId, Option<String>)>, ControlError> {
        self.with_reader(|conn| {
            conn.query_row(
                "SELECT jr.run_id, rs.session_id FROM job_requests jr \
                 LEFT JOIN runner_sessions rs ON rs.active_request_id = jr.request_id \
                 WHERE jr.request_id = ?1",
                params![request_id],
                |r| {
                    Ok((
                        parse_run_id(&r.get::<_, String>(0)?),
                        r.get::<_, Option<String>>(1)?,
                    ))
                },
            )
            .optional()
            .map_err(ControlError::backend)
        })
    }
    /// Query run summaries without materializing `TxState`.
    fn list_run_rows(&self, filter: RunListFilter) -> Result<Vec<RunRecord>, ControlError> {
        self.with_reader(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT selected.run_id, selected.record_blob, j.job_id, j.status, \
                            j.queue_kind, js.steps_blob \
                     FROM ( \
                         SELECT run_id, record_blob, \
                                CASE WHEN status IN ('success','failure','skipped','cancelled') \
                                     THEN 1 ELSE 0 END AS terminal_rank, \
                                COALESCE(completed_at_us, started_at_us, created_at_us) AS sort_at \
                         FROM runs \
                         WHERE (?1 IS NULL OR instr(workflow_path, ?1) > 0) \
                           AND (?2 IS NULL OR status = ?2) \
                           AND (?3 IS NULL OR event = ?3) \
                         ORDER BY terminal_rank, sort_at DESC \
                         LIMIT ?4 \
                     ) AS selected \
                     LEFT JOIN jobs j ON j.run_id = selected.run_id \
                     LEFT JOIN job_steps js ON js.agent_job_id = ( \
                         SELECT jr.agent_job_id FROM job_requests jr \
                         WHERE jr.run_id = j.run_id AND jr.job_id = j.job_id \
                         ORDER BY jr.request_id DESC LIMIT 1 \
                     ) \
                     ORDER BY selected.terminal_rank, selected.sort_at DESC, j.job_id",
                )
                .map_err(ControlError::backend)?;
            let rows = stmt
                .query_map(
                    params![
                        filter.workflow.as_deref(),
                        filter.status.as_deref(),
                        filter.event.as_deref(),
                        filter.limit.min(200) as i64,
                    ],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Vec<u8>>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, Option<String>>(3)?,
                            row.get::<_, Option<String>>(4)?,
                            row.get::<_, Option<Vec<u8>>>(5)?,
                        ))
                    },
                )
                .map_err(ControlError::backend)?;
            let mut runs = Vec::new();
            let mut current_id: Option<String> = None;
            let mut current_run: Option<RunRecord> = None;
            let mut current_jobs = Vec::new();
            for row in rows {
                let (run_id, record, job_id, status, queue_kind, steps) =
                    row.map_err(ControlError::backend)?;
                if current_id.as_deref() != Some(run_id.as_str()) {
                    if let Some(run) = current_run.take() {
                        runs.push(project_run_rows(run, std::mem::take(&mut current_jobs)));
                    }
                    current_id = Some(run_id);
                    current_run = Some(blob(&self.cipher, &record)?);
                }
                if let (Some(job_id), Some(status), Some(queue_kind)) = (job_id, status, queue_kind)
                {
                    let steps = steps
                        .map(|bytes| blob::<Vec<crate::models::StepRecord>>(&self.cipher, &bytes))
                        .transpose()?;
                    current_jobs.push((JobId(job_id), status_parse(&status), queue_kind, steps));
                }
            }
            if let Some(run) = current_run {
                runs.push(project_run_rows(run, current_jobs));
            }
            Ok(runs)
        })
    }

    /// Append an event without reading or rewriting its run.
    fn append_event_row(
        &self,
        event: &preloop_gha_protocol::NdjsonEvent,
    ) -> Result<(), ControlError> {
        let run_id = event_run_id(event).map(|id| id.0.to_string());
        let event_blob = unblob(&self.cipher, event)?;
        run_blocking(|| {
            self.conn
                .lock()
                .execute(
                    "INSERT INTO control_events(run_id, event_blob, created_at_us) \
                     VALUES (?1, ?2, ?3)",
                    params![
                        run_id,
                        event_blob,
                        system_to_us(std::time::SystemTime::now())
                    ],
                )
                .map_err(ControlError::backend)?;
            Ok(())
        })
    }

    fn auxiliary(&self) -> Result<&crate::store::SqliteStore, ControlError> {
        self.aux
            .as_ref()
            .ok_or_else(|| ControlError::backend(anyhow::anyhow!("webhooks unavailable in-memory")))
    }
    fn terminal_job_rows(&self) -> Result<BTreeSet<(RunId, JobId)>, ControlError> {
        self.with_reader(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT run_id, job_id FROM jobs \
                     WHERE status IN ('success','failure','skipped','cancelled')",
                )
                .map_err(ControlError::backend)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        parse_run_id(&row.get::<_, String>(0)?),
                        JobId(row.get::<_, String>(1)?),
                    ))
                })
                .map_err(ControlError::backend)?;
            let mut terminal = BTreeSet::new();
            for row in rows {
                terminal.insert(row.map_err(ControlError::backend)?);
            }
            Ok(terminal)
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Load: rows → TxState
// ─────────────────────────────────────────────────────────────────────────
/// Deserialize a sealed `*_blob` column. Every blob the control backend
/// writes is AEAD-sealed; unsealed input is rejected outright — pre-seal
/// databases are not supported and must be recreated.
fn blob<T: serde::de::DeserializeOwned>(
    cipher: &store::Envelope,
    bytes: &[u8],
) -> Result<T, ControlError> {
    let plain = cipher.unseal(bytes).map_err(ControlError::backend)?;
    serde_json::from_slice(&plain).map_err(ControlError::backend)
}

/// Serialize + seal a `*_blob` column.
fn unblob<T: serde::Serialize>(cipher: &store::Envelope, v: &T) -> Result<Vec<u8>, ControlError> {
    let plain = serde_json::to_vec(v).map_err(ControlError::backend)?;
    cipher.seal(&plain).map_err(ControlError::backend)
}

fn us_to_system(us: i64) -> std::time::SystemTime {
    std::time::UNIX_EPOCH + std::time::Duration::from_micros(us.max(0) as u64)
}

fn system_to_us(t: std::time::SystemTime) -> i64 {
    t.duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

fn status_str(s: ExecutionStatus) -> &'static str {
    match s {
        ExecutionStatus::Queued => "queued",
        ExecutionStatus::Pending => "pending",
        ExecutionStatus::InProgress => "in_progress",
        ExecutionStatus::Success => "success",
        ExecutionStatus::Failure => "failure",
        ExecutionStatus::Skipped => "skipped",
        ExecutionStatus::Cancelled => "cancelled",
    }
}

fn status_parse(s: &str) -> ExecutionStatus {
    match s {
        "pending" => ExecutionStatus::Pending,
        "in_progress" => ExecutionStatus::InProgress,
        "success" => ExecutionStatus::Success,
        "failure" => ExecutionStatus::Failure,
        "skipped" => ExecutionStatus::Skipped,
        "cancelled" => ExecutionStatus::Cancelled,
        _ => ExecutionStatus::Queued,
    }
}
fn parse_run_id(s: &str) -> RunId {
    RunId(s.parse().unwrap_or_default())
}

fn parse_uuid(s: &str) -> uuid::Uuid {
    s.parse().unwrap_or_default()
}

/// Build `SELECT … WHERE <col> IN (?,…)` for a run/session-scoped family.
/// `keys: None` → no filter (load all); `Some(set)` → only those keys.
/// Returns the SQL and the bound key strings for `params_from_iter`.
fn scoped_select<I, F>(
    base: &str,
    col: &str,
    keys: Option<&BTreeSet<I>>,
    f: F,
) -> (String, Vec<String>)
where
    F: Fn(&I) -> String,
{
    let Some(set) = keys else {
        return (base.to_owned(), Vec::new());
    };
    if set.is_empty() {
        return (format!("{base} WHERE 1=0"), Vec::new());
    }
    let ph = set.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let params = set.iter().map(f).collect();
    (format!("{base} WHERE {col} IN ({ph})"), params)
}

/// Delete rows from `table` restricted to `keys` when `full` is false —
/// the write-back counterpart of a scoped load. `full` (whole working set
/// loaded) deletes every row so the reinsert rebuilds the table; a narrow
/// scope deletes only the rows it loaded, leaving the rest untouched.
fn delete_scoped<I, F>(
    conn: &Connection,
    table: &str,
    col: &str,
    keys: &BTreeSet<I>,
    f: F,
    full: bool,
) -> Result<(), ControlError>
where
    F: Fn(&I) -> String,
{
    if full {
        conn.execute(&format!("DELETE FROM {table}"), [])
            .map_err(ControlError::backend)?;
        return Ok(());
    }
    if keys.is_empty() {
        return Ok(());
    }
    let ph = keys.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let params: Vec<String> = keys.iter().map(f).collect();
    conn.execute(
        &format!("DELETE FROM {table} WHERE {col} IN ({ph})"),
        rusqlite::params_from_iter(params.iter()),
    )
    .map_err(ControlError::backend)?;
    Ok(())
}

/// Delete run-scoped rows: full scope clears the table, a `Some(runs)`
/// scope deletes only those runs' rows.
fn delete_scoped_runs(conn: &Connection, table: &str, scope: &TxScope) -> Result<(), ControlError> {
    match scope.runs.as_ref() {
        None => {
            conn.execute(&format!("DELETE FROM {table}"), [])
                .map_err(ControlError::backend)?;
        }
        Some(runs) if !runs.is_empty() => {
            let ph = runs.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let params: Vec<String> = runs.iter().map(|id| id.0.to_string()).collect();
            conn.execute(
                &format!("DELETE FROM {table} WHERE run_id IN ({ph})"),
                rusqlite::params_from_iter(params.iter()),
            )
            .map_err(ControlError::backend)?;
        }
        Some(_) => {}
    }
    Ok(())
}

/// Delete rows for the job-assignment families (`job_assignments`,
/// `pool_pending`). Their load widens to every run when `ready_queue` is set
/// (a claim scans the global ready queue), so the delete must match: full
/// delete when `ready_queue || runs.is_none()`, else only `scope.runs` rows.
fn delete_scoped_queue(
    conn: &Connection,
    table: &str,
    scope: &TxScope,
) -> Result<(), ControlError> {
    if scope.ready_queue || scope.runs.is_none() {
        conn.execute(&format!("DELETE FROM {table}"), [])
            .map_err(ControlError::backend)?;
        return Ok(());
    }
    delete_scoped_runs(conn, table, scope)
}

/// Load the scheduling working set under `scope`. `TxScope::full()` loads
/// everything (the pre-scoping behavior); a narrower scope loads only the
/// rows a command touches, and write-back touches only those rows. The
/// always-global concurrency families load under any scope that asks for
/// them — see [`TxScope`].
fn load_txstate(
    conn: &Connection,
    scope: &TxScope,
    cipher: &store::Envelope,
) -> Result<(TxState, TxScope), ControlError> {
    // Widen `scope.runs` with every run a concurrency holder references.
    // `try_acquire_concurrency`/`release_concurrency_for_*` can cancel or
    // promote a *different* run's holder; `cancel_run_inner`/`settle_request`
    // must see that run's rows or the cancellation silently no-ops and the
    // run's jobs stay queued. `concurrency_groups` is small (active gates
    // only), so the extra scan is cheap; the full load below re-reads it.
    let mut effective_scope = scope.clone();
    if let Some(runs) = effective_scope.runs.as_mut() {
        // Widen `runs` with every run a concurrency holder references.
        // `try_acquire_concurrency`/`release_concurrency_for_*` can cancel or
        // promote a *different* run's holder; `cancel_run_inner`/
        // `settle_request` must see that run's rows or the cancellation
        // silently no-ops and the run's jobs stay queued. `concurrency_groups`
        // is small (active gates only), so the extra scan is cheap.
        if scope.concurrency {
            let mut stmt = conn
                .prepare("SELECT running_holder, pending_holders FROM concurrency_groups")
                .map_err(ControlError::backend)?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((r.get::<_, Option<Vec<u8>>>(0)?, r.get::<_, Vec<u8>>(1)?))
                })
                .map_err(ControlError::backend)?;
            for row in rows {
                let (running, pending) = row.map_err(ControlError::backend)?;
                if let Some(b) = running {
                    if let Ok(h) = blob::<concurrency::Holder>(cipher, &b) {
                        runs.insert(h.run_id());
                    }
                }
                let pending: VecDeque<concurrency::Holder> =
                    blob(cipher, &pending).unwrap_or_default();
                for h in pending {
                    runs.insert(h.run_id());
                }
            }
        }
        // `runs_referenced`: widen `runs` with the runs of every row this
        // transaction can mutate indirectly — the queued jobs the scope
        // loads (a claim marks the run in-progress, a promotion summarizes
        // it) and the scoped sessions' active requests (settle drops the
        // request's run). `SELECT DISTINCT run_id` reads no `record_blob`,
        // so this stays O(#queued + #sessions), not O(#runs).
        if scope.runs_referenced {
            let mut kinds: Vec<&'static str> = Vec::new();
            if scope.ready_queue {
                kinds.push("ready");
            }
            if scope.blocked_jobs {
                kinds.push("blocked");
            }
            if scope.pending_expansions {
                kinds.push("expand");
            }
            if !kinds.is_empty() {
                let ph = kinds.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT DISTINCT run_id FROM jobs WHERE queue_kind IN ({ph})"
                    ))
                    .map_err(ControlError::backend)?;
                let rows = stmt
                    .query_map(
                        rusqlite::params_from_iter(kinds.iter().map(|k| k.to_string())),
                        |r| r.get::<_, String>(0),
                    )
                    .map_err(ControlError::backend)?;
                for row in rows {
                    runs.insert(parse_run_id(&row.map_err(ControlError::backend)?));
                }
            }
            if let Some(sessions) = scope.sessions.as_ref() {
                if !sessions.is_empty() {
                    let ph = sessions.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                    let mut stmt = conn
                        .prepare(&format!(
                            "SELECT DISTINCT jr.run_id FROM job_requests jr \
                             JOIN runner_sessions rs \
                             ON jr.request_id = rs.active_request_id \
                             WHERE rs.session_id IN ({ph})"
                        ))
                        .map_err(ControlError::backend)?;
                    let rows = stmt
                        .query_map(rusqlite::params_from_iter(sessions.iter()), |r| {
                            r.get::<_, String>(0)
                        })
                        .map_err(ControlError::backend)?;
                    for row in rows {
                        runs.insert(parse_run_id(&row.map_err(ControlError::backend)?));
                    }
                }
            }
        }
        // `runs_via_requests`: widen `runs` with the run_ids of the loaded
        // `job_requests` — a correlation lookup resolves the request, then
        // reads that run's record. `SELECT DISTINCT run_id` reads no blob.
        // Only meaningful with `job_requests_all` (a global request set);
        // a run-scoped request load already has its runs.
        if scope.runs_via_requests && scope.job_requests_all {
            let mut stmt = conn
                .prepare("SELECT DISTINCT run_id FROM job_requests")
                .map_err(ControlError::backend)?;
            let rows = stmt
                .query_map([], |r| r.get::<_, String>(0))
                .map_err(ControlError::backend)?;
            for row in rows {
                runs.insert(parse_run_id(&row.map_err(ControlError::backend)?));
            }
        }
    }
    let scope = &effective_scope;

    let mut tx = TxState {
        ready_queue_loaded: scope.runs.is_none() || scope.ready_queue,
        ..Default::default()
    };

    // Runs: the record blob carries everything except the derived `jobs`
    // map, which we rebuild from the `jobs` table. Scoped to `scope.runs`.
    {
        let (sql, params) = scoped_select(
            "SELECT run_id, record_blob FROM runs",
            "run_id",
            scope.runs.as_ref(),
            |id| id.0.to_string(),
        );
        let mut stmt = conn.prepare(&sql).map_err(ControlError::backend)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (run_id_s, record) = row.map_err(ControlError::backend)?;
            let run_id = parse_run_id(&run_id_s);
            let mut run: RunRecord = blob(cipher, &record)?;
            run.jobs.clear(); // rebuilt from the jobs table below
            tx.runs.insert(run_id, run);
            tx.loaded.runs.insert(run_id);
        }
    }

    // Jobs: route each row into its queue collection and rebuild run.jobs.
    // `seq` is the write-order counter that preserves FIFO within a kind.
    // A job row loads when its run is in scope OR it sits in a global queue
    // kind the scope asked for (ready / blocked) — those are cross-run.
    {
        let mut sql = String::from(
            "SELECT run_id, job_id, status, queue_kind, queue_position, seq, \
             reaper_first_seen_us, expand_generation, enqueued_at_us, payload_blob, \
             priority, run_order, job_order, not_before_us \
             FROM jobs",
        );
        let mut params: Vec<String> = Vec::new();
        let mut clauses: Vec<String> = Vec::new();
        // `runs == None` means the run predicate is TRUE, which makes the
        // whole OR true — emit no WHERE and load every job. Only when the
        // scope names a run set do the queue-kind clauses matter.
        if let Some(runs) = scope.runs.as_ref() {
            if runs.is_empty() {
                clauses.push("1=0".to_owned());
            } else {
                let ph = runs.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                clauses.push(format!("run_id IN ({ph})"));
                params.extend(runs.iter().map(|id| id.0.to_string()));
            }
            let mut kinds: Vec<&'static str> = Vec::new();
            if scope.ready_queue {
                kinds.push("ready");
            }
            if scope.blocked_jobs {
                kinds.push("blocked");
            }
            if scope.pending_expansions {
                kinds.push("expand");
            }
            if !kinds.is_empty() {
                let ph = kinds.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                clauses.push(format!("queue_kind IN ({ph})"));
                params.extend(kinds.iter().map(|k| k.to_string()));
            }
        }
        if !clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" OR "));
        }
        sql.push_str(" ORDER BY seq");
        let mut stmt = conn.prepare(&sql).map_err(ControlError::backend)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, Option<i64>>(4)?,
                    r.get::<_, Option<i64>>(5)?,
                    r.get::<_, Option<i64>>(6)?,
                    r.get::<_, i64>(7)?,
                    r.get::<_, Option<i64>>(8)?,
                    r.get::<_, Option<Vec<u8>>>(9)?,
                    r.get::<_, Option<i16>>(10)?,
                    r.get::<_, Option<i64>>(11)?,
                    r.get::<_, Option<i64>>(12)?,
                    r.get::<_, Option<i64>>(13)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (
                run_id_s,
                job_id_s,
                status,
                kind,
                pos,
                job_seq,
                reaper_us,
                expand_generation,
                enqueued_us,
                payload,
                priority,
                run_order,
                job_order,
                not_before_us,
            ) = row.map_err(ControlError::backend)?;
            let run_id = parse_run_id(&run_id_s);
            let job_id = JobId(job_id_s);
            let status = status_parse(&status);
            // Preserve the persisted row state for every loaded job — a job
            // whose run isn't in `tx.runs` was widened in by a queue-kind
            // clause and must be written back with these exact values.
            tx.job_row_state.insert(
                (run_id, job_id.clone()),
                crate::control::txstate::JobRowState {
                    status,
                    queue_position: pos,
                    seq: job_seq.unwrap_or(0),
                    priority: priority.unwrap_or(0),
                    run_order: run_order.unwrap_or(0),
                    job_order: job_order.unwrap_or(0),
                    not_before_us,
                },
            );
            if let Some(run) = tx.runs.get_mut(&run_id) {
                run.jobs.insert(job_id.clone(), status);
            }
            let kind = QueueKind::parse(&kind).unwrap_or(QueueKind::None);
            tx.loaded.jobs.insert((run_id, job_id.clone()), kind);
            if let Some(us) = reaper_us {
                tx.queued_at
                    .insert((run_id, job_id.clone()), us_to_system(us));
            }
            if expand_generation > 0 {
                tx.expanding.insert((run_id, job_id.clone()));
                tx.expand_generations
                    .insert((run_id, job_id.clone()), expand_generation);
            }
            if let Some(payload) = payload {
                let mut job: QueuedJob = blob(cipher, &payload)?;
                // `enqueued_at_us` is the authoritative ready-entry clock: the
                // payload blob is only re-sealed on a slot change, so a
                // rewritten enqueue time (requeue, starvation bookkeeping)
                // would otherwise be masked by the stale blob.
                if let Some(us) = enqueued_us {
                    job.enqueued_at_unix_nanos = us * 1000;
                }
                match kind {
                    QueueKind::Ready => tx.ready_index.push_back(job),
                    QueueKind::Pending => tx.pending_jobs.push_back(job),
                    QueueKind::Blocked => tx.concurrency_blocked.push_back(job),
                    QueueKind::Expand => {
                        if expand_generation == 0 {
                            tx.pending_expansions.push_back(job);
                        } else {
                            // Claimed but unapplied: keep the payload so a
                            // crash recovery can requeue it.
                            tx.expanding_jobs.insert((run_id, job_id), job);
                        }
                    }
                    QueueKind::Claimed => {
                        tx.claimed_jobs.insert((run_id, job_id), job);
                    }
                    QueueKind::Held => {
                        tx.held_runs.entry(run_id).or_default().push(job);
                    }
                    QueueKind::None => {}
                }
            }
        }
    }
    // Global ready-queue size — unscoped COUNT, not the loaded subset, so
    // `queue_depth` reporting and `apply_claim`'s decrement stay correct
    // under a narrow scope.
    tx.ready_count = conn
        .query_row(
            "SELECT COUNT(*) FROM jobs WHERE queue_kind='ready'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    tx.next_queue_position = conn
        .query_row(
            "SELECT COALESCE(MAX(queue_position),0)+1 FROM jobs WHERE queue_kind='ready'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(1);
    tx.next_seq = conn
        .query_row("SELECT COALESCE(MAX(seq),0)+1 FROM jobs", [], |r| r.get(0))
        .unwrap_or(1);
    // Global queue-front labels — unscoped, for pool next-image selection.
    tx.next_queue_labels = conn
        .query_row(
            "SELECT payload_blob FROM jobs WHERE queue_kind='ready' \
             ORDER BY queue_position LIMIT 1",
            [],
            |r| r.get::<_, Vec<u8>>(0),
        )
        .ok()
        .and_then(|b| blob::<QueuedJob>(cipher, &b).ok())
        .map(|j| j.runs_on)
        .unwrap_or_default();

    // Requests + derived indexes.
    {
        // `job_requests_all` loads the whole request table without pulling
        // `record_blob`s — a command needing the global request set (every
        // active `plan_id`) must not also load every run.
        let request_runs = if scope.job_requests_all {
            None
        } else {
            scope.runs.as_ref()
        };
        let (sql, params) = scoped_select(
            "SELECT request_id, run_id, job_id, agent_job_id, plan_id, plan_type, \
             timeline_id, result, locked_until, claimed_at_us, owner_runner_id, \
             started_at_us, last_renewed_at_us, timeout_triggered, debug_token_issued, \
             request_blob FROM job_requests",
            "run_id",
            request_runs,
            |id| id.0.to_string(),
        );
        let mut stmt = conn.prepare(&sql).map_err(ControlError::backend)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, String>(6)?,
                    r.get::<_, Option<String>>(7)?,
                    r.get::<_, String>(8)?,
                    r.get::<_, Option<i64>>(9)?,
                    r.get::<_, Option<i64>>(10)?,
                    r.get::<_, Option<i64>>(11)?,
                    r.get::<_, Option<i64>>(12)?,
                    r.get::<_, i64>(13)?,
                    r.get::<_, i64>(14)?,
                    r.get::<_, Option<Vec<u8>>>(15)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (
                request_id,
                run_id_s,
                job_id_s,
                agent_job_id_s,
                plan_id,
                plan_type,
                timeline_id_s,
                result_s,
                locked_until,
                claimed_at_us,
                owner_runner_id,
                started_at_us,
                last_renewed_at_us,
                timeout_triggered,
                debug_token_issued,
                request_blob,
            ) = row.map_err(ControlError::backend)?;
            let run_id = parse_run_id(&run_id_s);
            if !scope.job_requests_all && !scope.includes_run(&run_id) {
                continue;
            }
            let record = TaskAgentJobRequestRecord {
                request_id,
                run_id,
                job_id: JobId(job_id_s),
                agent_job_id: parse_uuid(&agent_job_id_s),
                plan_id,
                plan_type,
                timeline_id: parse_uuid(&timeline_id_s),
                result: result_s.as_deref().map(status_parse),
                locked_until,
                claimed_at: claimed_at_us.map(us_to_system),
                owner_runner_id,
                started_at: started_at_us.map(us_to_system),
                last_renewed_at: last_renewed_at_us.map(us_to_system),
                timeout_triggered: timeout_triggered != 0,
                debug_token_issued: debug_token_issued != 0,
            };
            tx.loaded.requests.insert(request_id);
            if let Some(msg) = request_blob {
                if let Ok(msg) = blob(cipher, &msg) {
                    tx.broker_messages.insert(request_id, msg);
                }
            }
            tx.insert_request(record);
        }
    }

    // Token requests, grants, OIDC contexts, steps.
    {
        // `request_id` maps to a request — push the run scope down through a
        // subquery on `job_requests` so a narrow scope skips the whole table.
        let (sql, params) = match scope.runs.as_ref() {
            None => (
                "SELECT request_id, request_blob FROM github_token_requests".to_owned(),
                Vec::new(),
            ),
            Some(set) if set.is_empty() => (
                "SELECT request_id, request_blob FROM github_token_requests WHERE 1=0".to_owned(),
                Vec::new(),
            ),
            Some(set) => {
                let ph = set.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                (
                    format!(
                        "SELECT request_id, request_blob FROM github_token_requests \
                         WHERE request_id IN (SELECT request_id FROM job_requests \
                         WHERE run_id IN ({ph}))"
                    ),
                    set.iter().map(|id| id.0.to_string()).collect(),
                )
            }
        };
        let mut stmt = conn.prepare(&sql).map_err(ControlError::backend)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (request_id, req) = row.map_err(ControlError::backend)?;
            // Token requests are request-scoped: only requests this scope loaded.
            if !tx.loaded.requests.contains(&request_id) {
                continue;
            }
            if let Ok(req) = blob(cipher, &req) {
                tx.github_token_requests.insert(request_id, req);
            }
        }
    }
    {
        let (sql, params) = scoped_select(
            "SELECT run_id, job_id, granted FROM id_token_grants",
            "run_id",
            scope.runs.as_ref(),
            |id| id.0.to_string(),
        );
        let mut stmt = conn.prepare(&sql).map_err(ControlError::backend)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (run_id_s, job_id_s, granted) = row.map_err(ControlError::backend)?;
            let run_id = parse_run_id(&run_id_s);
            tx.id_token_grants
                .insert((run_id, JobId(job_id_s)), granted != 0);
        }
    }
    {
        let (sql, params) = scoped_select(
            "SELECT run_id, job_id, context_blob FROM oidc_job_contexts",
            "run_id",
            scope.runs.as_ref(),
            |id| id.0.to_string(),
        );
        let mut stmt = conn.prepare(&sql).map_err(ControlError::backend)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (run_id_s, job_id_s, ctx) = row.map_err(ControlError::backend)?;
            let run_id = parse_run_id(&run_id_s);
            if let Ok(ctx) = blob(cipher, &ctx) {
                tx.oidc_job_contexts.insert((run_id, JobId(job_id_s)), ctx);
            }
        }
    }
    // Steps are keyed by agent_job_id, which maps to a request — load them
    // only for requests already in scope (a run-scoped command gets steps
    // for its own jobs, not the whole table).
    {
        // `agent_job_id` maps to a request, not a run column — push the run
        // scope down through a subquery on `job_requests` so a narrow scope
        // doesn't read the whole steps table.
        let (sql, params) = match scope.runs.as_ref() {
            None => (
                "SELECT agent_job_id, steps_blob, revision FROM job_steps".to_owned(),
                Vec::new(),
            ),
            Some(set) if set.is_empty() => (
                "SELECT agent_job_id, steps_blob, revision FROM job_steps WHERE 1=0".to_owned(),
                Vec::new(),
            ),
            Some(set) => {
                let ph = set.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                (
                    format!(
                        "SELECT agent_job_id, steps_blob, revision FROM job_steps \
                         WHERE agent_job_id IN (SELECT agent_job_id FROM job_requests \
                         WHERE run_id IN ({ph}))"
                    ),
                    set.iter().map(|id| id.0.to_string()).collect(),
                )
            }
        };
        let mut stmt = conn.prepare(&sql).map_err(ControlError::backend)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (agent_job_id_s, steps, revision) = row.map_err(ControlError::backend)?;
            let agent_job_id = parse_uuid(&agent_job_id_s);
            // In scope when the request carrying this agent_job_id loaded.
            if !tx.agent_job_requests.contains_key(&agent_job_id) {
                continue;
            }
            if let Ok(steps) = blob(cipher, &steps) {
                tx.job_steps.insert(agent_job_id, steps);
                tx.job_steps_revision.insert(agent_job_id, revision as u64);
            }
            tx.loaded.step_attempts.insert(agent_job_id);
        }
    }

    // Runners — always global. `claim_permitted` checks the runner holding a
    // binding and `submit_run`/`unhostable_platform` need the full registry;
    // the table is small, so it loads unconditionally.
    {
        let mut stmt = conn
            .prepare(
                "SELECT runner_id, name, labels, ephemeral, public_key, rsa_public_key, \
                 runner_group_id, runner_group_name, client_id, pool_proven, registered_at_us \
                 FROM runners",
            )
            .map_err(ControlError::backend)?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, Option<String>>(4)?,
                    r.get::<_, Option<String>>(5)?,
                    r.get::<_, Option<i64>>(6)?,
                    r.get::<_, Option<String>>(7)?,
                    r.get::<_, Option<String>>(8)?,
                    r.get::<_, i64>(9)?,
                    r.get::<_, i64>(10)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (
                runner_id,
                name,
                labels,
                ephemeral,
                public_key,
                rsa_xml,
                runner_group_id,
                runner_group_name,
                client_id,
                pool_proven,
                registered_at_us,
            ) = row.map_err(ControlError::backend)?;
            let labels: Vec<String> = serde_json::from_str(&labels).unwrap_or_default();
            tx.runners.insert(
                runner_id,
                preloop_gha_protocol::RegisteredRunner {
                    id: runner_id,
                    name,
                    labels,
                    ephemeral: ephemeral != 0,
                    public_key,
                    runner_group_id,
                    runner_group_name,
                },
            );
            tx.runner_registered_at
                .insert(runner_id, us_to_system(registered_at_us));
            if let Some(xml) = rsa_xml {
                if let Ok(key) = AgentRsaPublicKey::parse(&xml) {
                    tx.runner_rsa_public_keys.insert(runner_id, key);
                }
            }
            if let Some(client_id) = client_id {
                tx.runner_client_ids.insert(client_id, runner_id);
            }
            if pool_proven != 0 {
                tx.pool_proven_runners.insert(runner_id);
            }
            tx.loaded.runners.insert(runner_id);
        }
    }

    // Sessions.
    {
        let (sql, params) = scoped_select(
            "SELECT session_id, runner_id, protocol, encryption_blob, \
             active_request_id, last_seen_at_us, verified FROM runner_sessions",
            "session_id",
            scope.sessions.as_ref(),
            |s| s.clone(),
        );
        let mut stmt = conn.prepare(&sql).map_err(ControlError::backend)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<i64>>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<Vec<u8>>>(3)?,
                    r.get::<_, Option<i64>>(4)?,
                    r.get::<_, Option<i64>>(5)?,
                    r.get::<_, i64>(6)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (
                session_id,
                runner_id,
                protocol,
                encryption_blob,
                active_request_id,
                last_seen_at_us,
                verified,
            ) = row.map_err(ControlError::backend)?;
            match (SessionProtocol::parse(&protocol), runner_id) {
                (SessionProtocol::Broker, Some(runner_id)) => {
                    tx.broker_session_runners
                        .insert(session_id.clone(), runner_id);
                }
                (SessionProtocol::Azdo, Some(runner_id)) => {
                    tx.sessions.insert(
                        session_id.clone(),
                        preloop_gha_protocol::RunnerSession {
                            session_id: SessionId(parse_uuid(&session_id)),
                            runner_id,
                        },
                    );
                    tx.azdo_sessions.insert(session_id.clone());
                }
                // Compatibility sessions carry no runner; they exist only to
                // satisfy `broker_messages`/`active_request_id` foreign keys.
                _ => {}
            }
            if verified != 0 {
                tx.verified_sessions.insert(session_id.clone());
            }
            if let Some(key_blob) = encryption_blob {
                // Session keys are sealed on write; unsealed input is
                // rejected outright — pre-seal databases are not supported.
                let restored = cipher
                    .unseal(&key_blob)
                    .map(SessionEncryption::from_key)
                    .map_err(ControlError::backend)?;
                tx.session_keys.insert(session_id.clone(), restored);
            }
            if let Some(us) = last_seen_at_us {
                tx.session_last_seen
                    .insert(session_id.clone(), us_to_system(us));
            }

            if let Some(request_id) = active_request_id {
                tx.session_active_requests
                    .insert(session_id.clone(), request_id);
            }
            tx.loaded.sessions.insert(session_id.clone());
            tx.loaded.session_active_requests.insert(session_id);
        }
    }

    // Inflight session messages.
    {
        let mut stmt = conn
            .prepare(
                "SELECT session_id, message_id, message_blob FROM broker_messages \
                 ORDER BY message_id",
            )
            .map_err(ControlError::backend)?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (session_id, message_id, msg) = row.map_err(ControlError::backend)?;
            // Only messages for sessions this scope loaded.
            if !tx.loaded.sessions.contains(&session_id) {
                continue;
            }
            if let Ok(msg) = blob(cipher, &msg) {
                tx.inflight_messages
                    .entry(session_id)
                    .or_default()
                    .insert(message_id, msg);
            }
        }
    }

    // Concurrency — always-global (a different run's release unblocks
    // waiters), loaded fully under any scope that asks for it.
    if scope.concurrency {
        let mut stmt = conn
            .prepare(
                "SELECT repo, group_name, display_name, running_holder, pending_holders \
                 FROM concurrency_groups",
            )
            .map_err(ControlError::backend)?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<Vec<u8>>>(3)?,
                    r.get::<_, Vec<u8>>(4)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (repo, group_name, display_name, running, pending) =
                row.map_err(ControlError::backend)?;
            let running: Option<concurrency::Holder> = running.and_then(|b| blob(cipher, &b).ok());
            let pending: VecDeque<concurrency::Holder> = blob(cipher, &pending).unwrap_or_default();
            tx.concurrency_groups.insert(
                (repo.clone(), group_name.clone()),
                concurrency::ConcurrencyGroup {
                    display_name,
                    running,
                    pending,
                },
            );
            tx.loaded.groups.insert((repo, group_name));
        }
        {
            let mut stmt = conn
                .prepare("SELECT run_id, repo, group_name FROM holder_keys")
                .map_err(ControlError::backend)?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                })
                .map_err(ControlError::backend)?;
            for row in rows {
                let (run_id_s, repo, group_name) = row.map_err(ControlError::backend)?;
                let run_id = parse_run_id(&run_id_s);
                tx.holder_keys
                    .entry(run_id)
                    .or_default()
                    .push((repo, group_name));
                tx.loaded.holder_key_runs.insert(run_id);
            }
        }
        {
            let mut stmt = conn
                .prepare("SELECT run_id, job_ids, gates_blob, acquired_keys FROM jobset_admissions")
                .map_err(ControlError::backend)?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Vec<u8>>(2)?,
                        r.get::<_, Vec<u8>>(3)?,
                    ))
                })
                .map_err(ControlError::backend)?;
            for row in rows {
                let (run_id_s, job_ids_s, gates, acquired) = row.map_err(ControlError::backend)?;
                let run_id = parse_run_id(&run_id_s);
                let job_ids: BTreeSet<JobId> = serde_json::from_str::<BTreeSet<String>>(&job_ids_s)
                    .unwrap_or_default()
                    .into_iter()
                    .map(JobId)
                    .collect();
                let id = JobSetId { run_id, job_ids };
                if let (Ok(gates), Ok(acquired_keys)) =
                    (blob(cipher, &gates), blob(cipher, &acquired))
                {
                    tx.jobset_admissions.insert(
                        id.clone(),
                        crate::state::JobSetAdmission {
                            gates,
                            acquired_keys,
                        },
                    );
                    tx.loaded.jobsets.insert(id);
                }
            }
        }
        {
            let mut stmt = conn
                .prepare("SELECT run_id, job_ids FROM jobset_ready")
                .map_err(ControlError::backend)?;
            let rows = stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                .map_err(ControlError::backend)?;
            for row in rows {
                let (run_id_s, job_ids_s) = row.map_err(ControlError::backend)?;
                let run_id = parse_run_id(&run_id_s);
                let job_ids: BTreeSet<JobId> = serde_json::from_str::<BTreeSet<String>>(&job_ids_s)
                    .unwrap_or_default()
                    .into_iter()
                    .map(JobId)
                    .collect();
                tx.jobset_ready.insert(JobSetId { run_id, job_ids });
            }
        }
    }
    // run_concurrency is run-scoped (one row per run's workflow gate).
    {
        let (sql, params) = scoped_select(
            "SELECT run_id, concurrency_blob FROM run_concurrency",
            "run_id",
            scope.runs.as_ref(),
            |id| id.0.to_string(),
        );
        let mut stmt = conn.prepare(&sql).map_err(ControlError::backend)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (run_id_s, c) = row.map_err(ControlError::backend)?;
            if let Ok(c) = blob(cipher, &c) {
                tx.run_concurrency.insert(parse_run_id(&run_id_s), c);
            }
        }
    }

    // Assignments, pool pending, cancellations.
    {
        // Assignments gate claims across the whole ready queue: load all when
        // `ready_queue` (or `runs: None` = all runs); only a run-scoped
        // command without the ready queue filters to its own runs.
        let (sql, params) = match (scope.ready_queue, scope.runs.as_ref()) {
            (true, _) | (_, None) => (
                "SELECT run_id, job_id, runner_id, at_us, first_at_us FROM job_assignments"
                    .to_owned(),
                Vec::new(),
            ),
            (false, Some(set)) if set.is_empty() => (
                "SELECT run_id, job_id, runner_id, at_us, first_at_us FROM job_assignments \
                 WHERE 1=0"
                    .to_owned(),
                Vec::new(),
            ),
            (false, Some(set)) => {
                let ph = set.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                (
                    format!(
                        "SELECT run_id, job_id, runner_id, at_us, first_at_us \
                         FROM job_assignments WHERE run_id IN ({ph})"
                    ),
                    set.iter().map(|id| id.0.to_string()).collect(),
                )
            }
        };
        let mut stmt = conn.prepare(&sql).map_err(ControlError::backend)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<i64>>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (run_id_s, job_id_s, runner_id, at_us, first_at_us) =
                row.map_err(ControlError::backend)?;
            let run_id = parse_run_id(&run_id_s);
            // Assignments gate claims across the whole ready queue; a scope
            // that loads the ready queue needs them all, a run-scoped
            // command only its own.
            if !scope.ready_queue && !scope.includes_run(&run_id) {
                continue;
            }
            let job_id = JobId(job_id_s);
            tx.job_assignments.insert(
                (run_id, job_id.clone()),
                crate::models::AssignmentRecord {
                    runner_id,
                    at: us_to_system(at_us),
                    first_at: us_to_system(first_at_us),
                },
            );
            tx.loaded.assignments.insert((run_id, job_id));
        }
    }
    {
        // Same dual scope as `job_assignments`: all rows when `ready_queue`
        // or `runs: None`; only own runs for a run-scoped command.
        let (sql, params) = match (scope.ready_queue, scope.runs.as_ref()) {
            (true, _) | (_, None) => (
                "SELECT run_id, job_id, at_us FROM pool_pending".to_owned(),
                Vec::new(),
            ),
            (false, Some(set)) if set.is_empty() => (
                "SELECT run_id, job_id, at_us FROM pool_pending WHERE 1=0".to_owned(),
                Vec::new(),
            ),
            (false, Some(set)) => {
                let ph = set.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                (
                    format!(
                        "SELECT run_id, job_id, at_us FROM pool_pending \
                         WHERE run_id IN ({ph})"
                    ),
                    set.iter().map(|id| id.0.to_string()).collect(),
                )
            }
        };
        let mut stmt = conn.prepare(&sql).map_err(ControlError::backend)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (run_id_s, job_id_s, at_us) = row.map_err(ControlError::backend)?;
            let run_id = parse_run_id(&run_id_s);
            if !scope.ready_queue && !scope.includes_run(&run_id) {
                continue;
            }
            tx.pool_pending
                .insert((run_id, JobId(job_id_s)), us_to_system(at_us));
        }
    }
    {
        // `ORDER BY seq` preserved; `scoped_select` appends the WHERE before
        // it, so build the base without ORDER BY and add it back.
        let (base, params) = scoped_select(
            "SELECT run_id, job_id, agent_job_id FROM cancellation_queue",
            "run_id",
            scope.runs.as_ref(),
            |id| id.0.to_string(),
        );
        let sql = format!("{base} ORDER BY seq");
        let mut stmt = conn.prepare(&sql).map_err(ControlError::backend)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (run_id_s, job_id_s, agent_job_id_s) = row.map_err(ControlError::backend)?;
            let run_id = parse_run_id(&run_id_s);
            // A cancellation is consumed by the session owning the job's
            // run — in scope when that run is.
            if !scope.includes_run(&run_id) {
                continue;
            }
            let job_id = JobId(job_id_s);
            let agent_job_id = parse_uuid(&agent_job_id_s);
            tx.cancellation_queue
                .push_back(crate::models::QueuedCancellation {
                    run_id,
                    job_id: job_id.clone(),
                    agent_job_id,
                });
            tx.loaded
                .cancellations
                .insert((run_id, job_id, agent_job_id));
        }
    }

    // Counters — global singletons (next_message_id, next_runner_id) and
    // per-workflow run-number counters. Always loaded: the allocators must
    // never observe 0, and the tables are tiny.
    {
        let mut stmt = conn
            .prepare("SELECT name, value FROM counters")
            .map_err(ControlError::backend)?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))
            .map_err(ControlError::backend)?;
        for row in rows {
            let (name, value) = row.map_err(ControlError::backend)?;
            match name.as_str() {
                "next_message_id" => tx.next_message_id = value,
                "next_runner_id" => tx.next_runner_id = value,
                "next_request_id" => tx.next_request_id = value,
                _ => {}
            }
            tx.loaded.counters.insert(name, value);
        }
        // `next_request_id` must never re-issue an existing `job_requests`
        // primary key — including ids written before the counter row existed
        // or by a peer that has not yet committed its counter bump. Seed from
        // the table max so the first in-transaction allocation is `max + 1`.
        let max_request: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(request_id), 0) FROM job_requests",
                [],
                |r| r.get(0),
            )
            .map_err(ControlError::backend)?;
        tx.next_request_id = tx.next_request_id.max(max_request);
    }
    {
        let mut stmt = conn
            .prepare("SELECT key, value FROM workflow_run_counters")
            .map_err(ControlError::backend)?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))
            .map_err(ControlError::backend)?;
        for row in rows {
            let (key, value) = row.map_err(ControlError::backend)?;
            tx.workflow_run_counters.insert(key, value as u64);
        }
    }
    Ok((tx, effective_scope))
}

// ─────────────────────────────────────────────────────────────────────────
// Write-back: TxState delta → rows
// ─────────────────────────────────────────────────────────────────────────

/// Persist the working set inside the transaction. Upserts every present
/// row and deletes rows that were loaded but removed by the command. The
/// `loaded` bookkeeping is what makes deletion precise: a key present at
/// load but absent now was removed by the command.
fn write_txstate(
    conn: &Connection,
    tx: &TxState,
    scope: &TxScope,
    cipher: &store::Envelope,
) -> Result<(), ControlError> {
    let now_us = system_to_us(std::time::SystemTime::now());

    // Runs: upsert present, delete removed.
    for (run_id, run) in &tx.runs {
        let mut record = run.clone();
        record.jobs.clear(); // derived from the jobs table
        let value = store::run_record_value(&record).map_err(ControlError::backend)?;
        let record_blob = unblob(cipher, &value)?;
        conn.execute(
            "INSERT INTO runs (run_id, status, run_number, run_attempt, run_name, event, \
             workflow_path, conclusion, webhook_delivery_id, record_blob, created_at_us, \
             started_at_us, completed_at_us) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13) \
             ON CONFLICT(run_id) DO UPDATE SET status=excluded.status, \
             conclusion=excluded.conclusion, record_blob=excluded.record_blob, \
             started_at_us=excluded.started_at_us, completed_at_us=excluded.completed_at_us",
            params![
                run_id.0.to_string(),
                status_str(run.status),
                run.run_number as i64,
                run.run_attempt as i64,
                run.run_name,
                run.event,
                run.workflow_path_str,
                run.conclusion,
                run.webhook_delivery_id,
                record_blob,
                system_to_us(run.created_at.into()),
                run.started_at.map(|t| system_to_us(t.into())),
                run.completed_at.map(|t| system_to_us(t.into())),
            ],
        )
        .map_err(ControlError::backend)?;
    }
    for run_id in &tx.loaded.runs {
        if !tx.runs.contains_key(run_id) {
            conn.execute(
                "DELETE FROM runs WHERE run_id=?1",
                params![run_id.0.to_string()],
            )
            .map_err(ControlError::backend)?;
        }
    }
    let mut seen_jobs: BTreeSet<(RunId, JobId)> = BTreeSet::new();
    // Fresh position/seq allocators for genuinely new jobs (those not in
    // `job_row_state`); existing jobs keep their loaded values via write_job.
    let mut next_pos = tx.next_queue_position;
    let mut next_seq = tx.next_seq;

    // `write_job` resolves a job's `queue_position`/`seq` from three inputs:
    //   - `fresh`: the job is newly enqueued or requeued via `tx.queue`
    //     (push_ready) — always allocate at the back, even if a stale
    //     `job_row_state` entry exists (requeue must not reuse the old slot);
    //   - the persisted kind in `tx.loaded.jobs` vs the kind being written —
    //     a transition (ready→claimed, blocked→ready, …) clears the position
    //     and takes a fresh FIFO `seq`;
    //   - `job_row_state`: only a job staying in the SAME slot (same kind,
    //     not requeued) keeps its loaded `queue_position`/`seq` so a no-op
    //     scoped write is byte-identical and global FIFO order is preserved.
    let write_job = |conn: &Connection,
                     tx: &TxState,
                     seen: &mut BTreeSet<(RunId, JobId)>,
                     next_seq: &mut i64,
                     next_pos: &mut i64,
                     fresh: bool,
                     run_id: RunId,
                     job_id: &JobId,
                     kind: QueueKind,
                     job: Option<&QueuedJob>|
     -> Result<(), ControlError> {
        let key = (run_id, job_id.clone());
        let preserved = tx.job_row_state.get(&key);
        // Same slot = persisted kind matches the written kind AND not a
        // requeue. Only then are the loaded position/seq still valid.
        let same_slot = !fresh && tx.loaded.jobs.get(&key) == Some(&kind);
        let (position, seq) = if same_slot {
            match preserved {
                Some(p) => (p.queue_position, p.seq),
                None => (None, 0),
            }
        } else {
            // Transition or fresh/requeued: new FIFO seq; a position only if
            // the job is landing in the ready queue.
            let s = *next_seq;
            *next_seq += 1;
            let p = if kind == QueueKind::Ready {
                let v = *next_pos;
                *next_pos += 1;
                Some(v)
            } else {
                None
            };
            (p, s)
        };
        // Status precedence: an explicit `job_status` override (set via
        // `set_job_status` by promotion/requeue/claim) wins; then a widened
        // job (run not loaded) restores its persisted status verbatim — no
        // coercion, so an untouched foreign `pending` row survives; finally
        // an owned job reads `run.jobs` (canonical).
        let widened = !tx.runs.contains_key(&run_id);
        let status = if let Some(s) = tx.job_status.get(&key).copied() {
            s
        } else if widened {
            preserved
                .map(|p| p.status)
                .unwrap_or(ExecutionStatus::Queued)
        } else {
            tx.runs
                .get(&run_id)
                .and_then(|r| r.jobs.get(job_id).copied())
                .unwrap_or(ExecutionStatus::Queued)
        };
        let pool_key = match job {
            Some(j) => {
                crate::control::types::compute_pool_key(&j.runs_on, j.runner_group.as_deref())
            }
            None => String::new(),
        };
        let run_order = preserved.map(|p| p.run_order).unwrap_or_else(|| {
            tx.runs
                .get(&run_id)
                .map(|r| r.run_number as i64)
                .unwrap_or(0)
        });
        let job_order = if same_slot {
            preserved.map(|p| p.job_order).unwrap_or(0)
        } else {
            position.unwrap_or(0)
        };
        let priority = preserved.map(|p| p.priority).unwrap_or(0);
        let not_before_us = preserved.and_then(|p| p.not_before_us);
        let is_initial_insert = !tx.loaded.jobs.contains_key(&key);
        let (base_id, runs_on, runner_group, enqueued_us, payload) = match job {
            Some(j) => (
                j.base_id.clone(),
                serde_json::to_string(&j.runs_on).unwrap_or_default(),
                j.runner_group.clone(),
                Some(j.enqueued_at_unix_nanos / 1000),
                if is_initial_insert {
                    Some(unblob(cipher, j)?)
                } else {
                    None
                },
            ),
            None => (String::new(), "[]".to_owned(), None, None, None),
        };
        let reaper_us = tx
            .queued_at
            .get(&(run_id, job_id.clone()))
            .map(|t| system_to_us(*t));
        let expand_generation = tx
            .expand_generations
            .get(&(run_id, job_id.clone()))
            .copied()
            .unwrap_or(0);
        let claimed = tx.job_assignments.get(&(run_id, job_id.clone()));
        conn.execute(
            "INSERT INTO jobs (run_id, job_id, status, queue_kind, queue_position, seq, \
             base_id, runs_on, runner_group, enqueued_at_us, reaper_first_seen_us, \
             claimed_by, claimed_at_us, expand_generation, payload_blob, \
             namespace_id, pool_key, priority, run_order, job_order, not_before_us) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21) \
             ON CONFLICT(run_id,job_id) DO UPDATE SET status=excluded.status, \
             queue_kind=excluded.queue_kind, queue_position=excluded.queue_position, \
             seq=excluded.seq, enqueued_at_us=excluded.enqueued_at_us, \
             reaper_first_seen_us=excluded.reaper_first_seen_us, \
             claimed_by=excluded.claimed_by, claimed_at_us=excluded.claimed_at_us, \
             expand_generation=excluded.expand_generation, \
             payload_blob=COALESCE(excluded.payload_blob, jobs.payload_blob), \
             namespace_id=excluded.namespace_id, pool_key=excluded.pool_key, \
             priority=excluded.priority, run_order=excluded.run_order, \
             job_order=excluded.job_order, not_before_us=excluded.not_before_us",
            params![
                run_id.0.to_string(),
                job_id.0,
                status_str(status),
                kind.as_str(),
                position,
                seq,
                base_id,
                runs_on,
                runner_group,
                enqueued_us,
                reaper_us,
                claimed.and_then(|c| c.runner_id),
                claimed.map(|c| system_to_us(c.at)),
                expand_generation,
                payload,
                "default",
                pool_key,
                priority,
                run_order,
                job_order,
                not_before_us,
            ],
        )
        .map_err(ControlError::backend)?;
        seen.insert((run_id, job_id.clone()));
        Ok(())
    };

    // Ready queue: `ready_index` holds persisted ready jobs (same slot →
    // keep position/seq); `tx.queue` holds newly enqueued AND requeued jobs
    // (push_ready) — always fresh, allocated at the back even when a stale
    // `job_row_state` entry exists.
    for job in &tx.ready_index {
        write_job(
            conn,
            tx,
            &mut seen_jobs,
            &mut next_seq,
            &mut next_pos,
            false,
            job.run_id,
            &job.job_id,
            QueueKind::Ready,
            Some(job),
        )?;
    }
    for job in &tx.queue {
        write_job(
            conn,
            tx,
            &mut seen_jobs,
            &mut next_seq,
            &mut next_pos,
            true,
            job.run_id,
            &job.job_id,
            QueueKind::Ready,
            Some(job),
        )?;
    }
    for job in &tx.pending_jobs {
        write_job(
            conn,
            tx,
            &mut seen_jobs,
            &mut next_seq,
            &mut next_pos,
            false,
            job.run_id,
            &job.job_id,
            QueueKind::Pending,
            Some(job),
        )?;
    }
    for job in &tx.concurrency_blocked {
        write_job(
            conn,
            tx,
            &mut seen_jobs,
            &mut next_seq,
            &mut next_pos,
            false,
            job.run_id,
            &job.job_id,
            QueueKind::Blocked,
            Some(job),
        )?;
    }
    for job in &tx.pending_expansions {
        write_job(
            conn,
            tx,
            &mut seen_jobs,
            &mut next_seq,
            &mut next_pos,
            false,
            job.run_id,
            &job.job_id,
            QueueKind::Expand,
            Some(job),
        )?;
    }
    // Expanding (claimed, in-progress) jobs: keep queue_kind='expand' and
    // bump the generation. A node already written by the pending_expansions
    // loop (defer_expansion parks it in both) is skipped so its payload
    // isn't overwritten; the rest carry their payload from expanding_jobs.
    for (run_id, job_id) in &tx.expanding {
        if seen_jobs.contains(&(*run_id, job_id.clone())) {
            continue;
        }
        write_job(
            conn,
            tx,
            &mut seen_jobs,
            &mut next_seq,
            &mut next_pos,
            false,
            *run_id,
            job_id,
            QueueKind::Expand,
            tx.expanding_jobs.get(&(*run_id, job_id.clone())),
        )?;
    }
    for ((run_id, job_id), job) in &tx.claimed_jobs {
        write_job(
            conn,
            tx,
            &mut seen_jobs,
            &mut next_seq,
            &mut next_pos,
            false,
            *run_id,
            job_id,
            QueueKind::Claimed,
            Some(job),
        )?;
    }
    for (run_id, jobs) in &tx.held_runs {
        for job in jobs {
            write_job(
                conn,
                tx,
                &mut seen_jobs,
                &mut next_seq,
                &mut next_pos,
                false,
                *run_id,
                &job.job_id,
                QueueKind::Held,
                Some(job),
            )?;
        }
    }
    // Jobs with no queue collection but present in run.jobs (terminal or
    // placeholder nodes): write them as 'none' so their status persists.
    for (run_id, run) in &tx.runs {
        for job_id in run.jobs.keys() {
            if !seen_jobs.contains(&(*run_id, job_id.clone())) {
                write_job(
                    conn,
                    tx,
                    &mut seen_jobs,
                    &mut next_seq,
                    &mut next_pos,
                    false,
                    *run_id,
                    job_id,
                    QueueKind::None,
                    None,
                )?;
            }
        }
    }
    // Delete jobs loaded but no longer present anywhere.
    for (run_id, job_id) in tx.loaded.jobs.keys() {
        if !seen_jobs.contains(&(*run_id, job_id.clone())) {
            conn.execute(
                "DELETE FROM jobs WHERE run_id=?1 AND job_id=?2",
                params![run_id.0.to_string(), job_id.0],
            )
            .map_err(ControlError::backend)?;
        }
    }

    // Requests: upsert present, delete removed.
    for (request_id, r) in &tx.job_requests {
        let request_blob = tx
            .broker_messages
            .get(request_id)
            .map(|m| unblob(cipher, m))
            .transpose()?;
        conn.execute(
            "INSERT INTO job_requests (request_id, run_id, job_id, agent_job_id, plan_id, \
             plan_type, timeline_id, result, locked_until, claimed_at_us, owner_runner_id, \
             started_at_us, last_renewed_at_us, timeout_triggered, debug_token_issued, \
             request_blob) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16) \
             ON CONFLICT(request_id) DO UPDATE SET result=excluded.result, \
             locked_until=excluded.locked_until, claimed_at_us=excluded.claimed_at_us, \
             owner_runner_id=excluded.owner_runner_id, started_at_us=excluded.started_at_us, \
             last_renewed_at_us=excluded.last_renewed_at_us, \
             timeout_triggered=excluded.timeout_triggered, \
             debug_token_issued=excluded.debug_token_issued, \
             request_blob=excluded.request_blob",
            params![
                request_id,
                r.run_id.0.to_string(),
                r.job_id.0,
                r.agent_job_id.to_string(),
                r.plan_id,
                r.plan_type,
                r.timeline_id.to_string(),
                r.result.map(status_str),
                r.locked_until,
                r.claimed_at.map(system_to_us),
                r.owner_runner_id,
                r.started_at.map(system_to_us),
                r.last_renewed_at.map(system_to_us),
                r.timeout_triggered as i64,
                r.debug_token_issued as i64,
                request_blob,
            ],
        )
        .map_err(ControlError::backend)?;
    }
    for request_id in &tx.loaded.requests {
        if !tx.job_requests.contains_key(request_id) {
            conn.execute(
                "DELETE FROM job_requests WHERE request_id=?1",
                params![request_id],
            )
            .map_err(ControlError::backend)?;
        }
    }
    // Token requests — request-scoped: only requests this scope loaded.
    // Delete only in-scope rows so a narrow scope can't wipe the rest.
    delete_scoped(
        conn,
        "github_token_requests",
        "request_id",
        &tx.loaded.requests,
        |id| id.to_string(),
        scope.runs.is_none(),
    )?;
    for (request_id, req) in &tx.github_token_requests {
        conn.execute(
            "INSERT INTO github_token_requests (request_id, request_blob) VALUES (?1,?2)",
            params![request_id, unblob(cipher, req)?],
        )
        .map_err(ControlError::backend)?;
    }
    // Grants + OIDC — run-scoped.
    delete_scoped_runs(conn, "id_token_grants", scope)?;
    for ((run_id, job_id), granted) in &tx.id_token_grants {
        conn.execute(
            "INSERT INTO id_token_grants (run_id, job_id, granted) VALUES (?1,?2,?3)",
            params![run_id.0.to_string(), job_id.0, *granted as i64],
        )
        .map_err(ControlError::backend)?;
    }
    delete_scoped_runs(conn, "oidc_job_contexts", scope)?;
    for ((run_id, job_id), ctx) in &tx.oidc_job_contexts {
        conn.execute(
            "INSERT INTO oidc_job_contexts (run_id, job_id, context_blob) VALUES (?1,?2,?3)",
            params![run_id.0.to_string(), job_id.0, unblob(cipher, ctx)?],
        )
        .map_err(ControlError::backend)?;
    }
    // Steps.
    for (agent_job_id, steps) in &tx.job_steps {
        let revision = tx
            .job_steps_revision
            .get(agent_job_id)
            .copied()
            .unwrap_or(0);
        conn.execute(
            "INSERT INTO job_steps (agent_job_id, steps_blob, revision) VALUES (?1,?2,?3) \
             ON CONFLICT(agent_job_id) DO UPDATE SET steps_blob=excluded.steps_blob, \
             revision=excluded.revision",
            params![
                agent_job_id.to_string(),
                unblob(cipher, steps)?,
                revision as i64
            ],
        )
        .map_err(ControlError::backend)?;
    }
    for agent_job_id in &tx.loaded.step_attempts {
        if !tx.job_steps.contains_key(agent_job_id) {
            conn.execute(
                "DELETE FROM job_steps WHERE agent_job_id=?1",
                params![agent_job_id.to_string()],
            )
            .map_err(ControlError::backend)?;
        }
    }

    // Runners.
    for (runner_id, runner) in &tx.runners {
        let rsa_xml = tx
            .runner_rsa_public_keys
            .get(runner_id)
            .map(|k| k.to_xml_string());
        let client_id = tx
            .runner_client_ids
            .iter()
            .find(|(_, id)| *id == runner_id)
            .map(|(c, _)| c.clone());
        conn.execute(
            "INSERT INTO runners (runner_id, name, labels, ephemeral, public_key, \
             rsa_public_key, runner_group_id, runner_group_name, client_id, pool_proven, \
             registered_at_us) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11) \
             ON CONFLICT(runner_id) DO UPDATE SET name=excluded.name, labels=excluded.labels, \
             ephemeral=excluded.ephemeral, public_key=excluded.public_key, \
             rsa_public_key=excluded.rsa_public_key, runner_group_id=excluded.runner_group_id, \
             runner_group_name=excluded.runner_group_name, client_id=excluded.client_id, \
             pool_proven=excluded.pool_proven",
            params![
                runner_id,
                runner.name,
                serde_json::to_string(&runner.labels).unwrap_or_default(),
                runner.ephemeral as i64,
                runner.public_key,
                rsa_xml,
                runner.runner_group_id,
                runner.runner_group_name,
                client_id,
                tx.pool_proven_runners.contains(runner_id) as i64,
                tx.runner_registered_at
                    .get(runner_id)
                    .map(|t| system_to_us(*t))
                    .unwrap_or(now_us),
            ],
        )
        .map_err(ControlError::backend)?;
    }
    for runner_id in &tx.loaded.runners {
        if !tx.runners.contains_key(runner_id) {
            conn.execute("DELETE FROM runners WHERE runner_id=?1", params![runner_id])
                .map_err(ControlError::backend)?;
        }
    }

    // Sessions: rebuild from the unified maps, scoped to loaded sessions.
    delete_scoped(
        conn,
        "runner_sessions",
        "session_id",
        &tx.loaded.sessions,
        |s| s.clone(),
        scope.sessions.is_none(),
    )?;
    let write_session = |conn: &Connection,
                         session_id: &str,
                         runner_id: Option<i64>,
                         protocol: SessionProtocol,
                         tx: &TxState|
     -> Result<(), ControlError> {
        // Seal the session AES key with the store envelope — a raw key in
        // the row would let a DB reader decrypt every recorded job message.
        // The control schema stores it as one sealed blob (version||iv||ct||tag).
        let encryption = tx
            .session_keys
            .get(session_id)
            .map(|e| cipher.seal(&e.key))
            .transpose()
            .map_err(ControlError::backend)?;
        let active_request_id = tx.session_active_requests.get(session_id).copied();
        let last_seen_us = tx
            .session_last_seen
            .get(session_id)
            .map(|t| system_to_us(*t));
        let created_us = now_us;
        let verified = tx.verified_sessions.contains(session_id);
        conn.execute(
            "INSERT INTO runner_sessions (session_id, runner_id, protocol, encryption_blob, \
             active_request_id, last_seen_at_us, verified, created_at_us) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                session_id,
                runner_id,
                protocol.as_str(),
                encryption,
                active_request_id,
                last_seen_us,
                verified as i64,
                created_us
            ],
        )
        .map_err(ControlError::backend)?;
        Ok(())
    };
    for (session_id, runner_id) in &tx.broker_session_runners {
        write_session(
            conn,
            session_id,
            Some(*runner_id),
            SessionProtocol::Broker,
            tx,
        )?;
    }
    for (session_id, session) in &tx.sessions {
        write_session(
            conn,
            session_id,
            Some(session.runner_id),
            SessionProtocol::Azdo,
            tx,
        )?;
    }
    // Compatibility sessions (e.g. the implicit `default` session) own no
    // registered runner, so they are absent from `broker_session_runners` and
    // `sessions`. They still need a `runner_sessions` row — with NULL
    // `runner_id` — so that `broker_messages` and `active_request_id` foreign
    // keys resolve and the active-request mapping survives the commit.
    let compat_session_ids: BTreeSet<&String> = tx
        .session_active_requests
        .keys()
        .chain(tx.session_last_seen.keys())
        .chain(tx.inflight_messages.keys())
        .chain(tx.session_keys.keys())
        .filter(|sid| {
            !tx.broker_session_runners.contains_key(*sid) && !tx.sessions.contains_key(*sid)
        })
        .collect();
    for session_id in compat_session_ids {
        write_session(conn, session_id, None, SessionProtocol::Compat, tx)?;
    }

    // Inflight messages — session-scoped.
    delete_scoped(
        conn,
        "broker_messages",
        "session_id",
        &tx.loaded.sessions,
        |s| s.clone(),
        scope.sessions.is_none(),
    )?;
    for (session_id, messages) in &tx.inflight_messages {
        for (message_id, msg) in messages {
            conn.execute(
                "INSERT INTO broker_messages (session_id, message_id, message_blob, \
                 created_at_us) VALUES (?1,?2,?3,?4)",
                params![session_id, message_id, unblob(cipher, msg)?, now_us],
            )
            .map_err(ControlError::backend)?;
        }
    }

    // Concurrency — always-global, written only when the scope loaded it.
    if scope.concurrency {
        conn.execute("DELETE FROM concurrency_groups", [])
            .map_err(ControlError::backend)?;
        for ((repo, group_name), group) in &tx.concurrency_groups {
            conn.execute(
                "INSERT INTO concurrency_groups (repo, group_name, display_name, running_holder, \
                 pending_holders) VALUES (?1,?2,?3,?4,?5)",
                params![
                    repo,
                    group_name,
                    group.display_name,
                    group
                        .running
                        .as_ref()
                        .map(|h| unblob(cipher, h))
                        .transpose()?,
                    unblob(cipher, &group.pending)?,
                ],
            )
            .map_err(ControlError::backend)?;
        }
        conn.execute("DELETE FROM holder_keys", [])
            .map_err(ControlError::backend)?;
        for (run_id, keys) in &tx.holder_keys {
            for (repo, group_name) in keys {
                conn.execute(
                    "INSERT INTO holder_keys (run_id, repo, group_name) VALUES (?1,?2,?3)",
                    params![run_id.0.to_string(), repo, group_name],
                )
                .map_err(ControlError::backend)?;
            }
        }
        conn.execute("DELETE FROM jobset_admissions", [])
            .map_err(ControlError::backend)?;
        for (id, admission) in &tx.jobset_admissions {
            let job_ids: Vec<String> = id.job_ids.iter().map(|j| j.0.clone()).collect();
            conn.execute(
                "INSERT INTO jobset_admissions (run_id, job_ids, gates_blob, acquired_keys) \
                 VALUES (?1,?2,?3,?4)",
                params![
                    id.run_id.0.to_string(),
                    serde_json::to_string(&job_ids).unwrap_or_default(),
                    unblob(cipher, &admission.gates)?,
                    unblob(cipher, &admission.acquired_keys)?,
                ],
            )
            .map_err(ControlError::backend)?;
        }
        conn.execute("DELETE FROM jobset_ready", [])
            .map_err(ControlError::backend)?;
        for id in &tx.jobset_ready {
            let job_ids: Vec<String> = id.job_ids.iter().map(|j| j.0.clone()).collect();
            conn.execute(
                "INSERT INTO jobset_ready (run_id, job_ids) VALUES (?1,?2)",
                params![
                    id.run_id.0.to_string(),
                    serde_json::to_string(&job_ids).unwrap_or_default()
                ],
            )
            .map_err(ControlError::backend)?;
        }
    }
    delete_scoped_runs(conn, "run_concurrency", scope)?;
    for (run_id, c) in &tx.run_concurrency {
        conn.execute(
            "INSERT INTO run_concurrency (run_id, concurrency_blob) VALUES (?1,?2)",
            params![run_id.0.to_string(), unblob(cipher, c)?],
        )
        .map_err(ControlError::backend)?;
    }

    // Assignments, pool pending, cancellations — run-scoped.
    delete_scoped_queue(conn, "job_assignments", scope)?;
    for ((run_id, job_id), record) in &tx.job_assignments {
        conn.execute(
            "INSERT INTO job_assignments (run_id, job_id, runner_id, at_us, first_at_us) \
             VALUES (?1,?2,?3,?4,?5)",
            params![
                run_id.0.to_string(),
                job_id.0,
                record.runner_id,
                system_to_us(record.at),
                system_to_us(record.first_at),
            ],
        )
        .map_err(ControlError::backend)?;
    }
    delete_scoped_queue(conn, "pool_pending", scope)?;
    for ((run_id, job_id), at) in &tx.pool_pending {
        conn.execute(
            "INSERT INTO pool_pending (run_id, job_id, at_us) VALUES (?1,?2,?3)",
            params![run_id.0.to_string(), job_id.0, system_to_us(*at)],
        )
        .map_err(ControlError::backend)?;
    }
    delete_scoped_runs(conn, "cancellation_queue", scope)?;
    for c in &tx.cancellation_queue {
        conn.execute(
            "INSERT INTO cancellation_queue (run_id, job_id, agent_job_id) VALUES (?1,?2,?3)",
            params![
                c.run_id.0.to_string(),
                c.job_id.0,
                c.agent_job_id.to_string()
            ],
        )
        .map_err(ControlError::backend)?;
    }

    // Counters — always loaded, so always written.
    for (name, value) in [
        ("next_message_id", tx.next_message_id),
        ("next_runner_id", tx.next_runner_id),
        ("next_request_id", tx.next_request_id),
    ] {
        conn.execute(
            "INSERT INTO counters (name, value) VALUES (?1,?2) \
             ON CONFLICT(name) DO UPDATE SET value=excluded.value",
            params![name, value],
        )
        .map_err(ControlError::backend)?;
    }
    for (key, value) in &tx.workflow_run_counters {
        conn.execute(
            "INSERT INTO workflow_run_counters (key, value) VALUES (?1,?2) \
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![key, *value as i64],
        )
        .map_err(ControlError::backend)?;
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// ControlBackend implementation
// ─────────────────────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl ControlBackend for SqliteBackend {
    async fn submit_run(&self, submit: SubmitRun) -> Result<SubmitOutcome, ControlError> {
        // set stays narrow: `submit_run_tx` dedups by scanning `tx.runs`,
        // which only sees scoped runs — the existing run must be named in
        // the scope or the check misses it and inserts a duplicate.
        let mut run_ids = BTreeSet::from([submit.record.run_id]);
        if let (Some(delivery_id), workflow_path) = (
            submit.record.webhook_delivery_id.as_deref(),
            submit.record.workflow_path_str.as_str(),
        ) {
            if let Some(existing) = self.find_run_by_delivery(delivery_id, workflow_path)? {
                run_ids.insert(existing.run_id);
            }
        }
        // `sessions: None` — `on_job_enqueued` finds idle-runner candidates
        // through `broker_session_runners`/`sessions`/`session_active_requests`,
        // all session-scoped. `concurrency: true` widens `runs` with every
        // holder's run (a `cancel_in_progress` submit cancels the running
        // holder). `runs_referenced` is off: submit never promotes queued
        // jobs, so ready/blocked runs stay foreign.
        let scope = TxScope {
            runs: Some(run_ids),
            // `ready_queue`/`blocked_jobs` stay unloaded: `submit_run_tx`
            // only promotes this run's own pending jobs (foreign runs'
            // `needs:` never cross runs), `queue_depth` reads the O(1)
            // `ready_count`, and `next_runs_on` falls back to
            // `next_queue_labels`. Loading the global queues would parse
            // every queued `payload_blob` per submit — O(#queued) blobs.
            ready_queue: false,
            blocked_jobs: false,
            sessions: None,
            concurrency: true,
            runs_referenced: false,
            job_requests_all: false,
            pending_expansions: false,
            runs_via_requests: false,
        };
        self.transact_scoped(&scope, |tx| commands::submit_run_tx(tx, submit))
    }

    async fn allocate_run_number(&self, workflow_path: &str) -> Result<u64, ControlError> {
        let workflow_path = workflow_path.to_owned();
        // Only `workflow_run_counters` is touched — a full load would parse
        // every `record_blob`/`payload_blob` per submission for one counter.
        let scope = TxScope {
            runs: Some(BTreeSet::new()),
            ready_queue: false,
            blocked_jobs: false,
            sessions: Some(BTreeSet::new()),
            concurrency: false,
            runs_referenced: false,
            job_requests_all: false,
            pending_expansions: false,
            runs_via_requests: false,
        };
        self.transact_scoped(&scope, move |tx| {
            let counter = tx.workflow_run_counters.entry(workflow_path).or_insert(0);
            *counter += 1;
            Ok(*counter)
        })
    }

    async fn poll_session(&self, poll: PollRequest) -> Result<PollOutcome, ControlError> {
        // `runs: Some(empty)` + `runs_referenced` — the claimed job's run is
        // chosen inside the transaction (`choose_claim_position`), and the
        // session's active request's run is needed for the cancellation
        // check; neither is knowable beforehand, so the referenced set
        // (ready/blocked jobs' runs + this session's request runs +
        // concurrency holders) is widened in at load. `sessions: {session_id}`
        // narrows the session families to this poller.
        let scope = TxScope {
            runs: Some(BTreeSet::new()),
            ready_queue: true,
            // `poll` claims from the ready queue only — it never promotes or
            // releases blocked jobs (that's `complete_job`/`submit`). Loading
            // every blocked `payload_blob` is O(#blocked) for no benefit.
            blocked_jobs: false,
            sessions: Some(BTreeSet::from([poll.session_id.clone()])),
            // No concurrency families: `poll` claims ready work and reads
            // `job_assignments`/`pool_pending`/`runners` for permission —
            // `concurrency_groups`/`jobset_*` are only touched by promote.
            concurrency: false,
            runs_referenced: true,
            job_requests_all: false,
            pending_expansions: false,
            runs_via_requests: false,
        };
        self.transact_scoped(&scope, |tx| commands::poll_session_tx(tx, poll))
    }

    async fn acquire_context(&self, request_id: i64) -> Result<AcquireContext, ControlError> {
        // Read-only — the reader pool serves it concurrently with the
        // writer instead of serializing on `BEGIN IMMEDIATE`. Scoped to the
        // request's run + owner session so it never loads the whole working
        // set.
        let (run_id, session_id) = self
            .find_request_context(request_id)?
            .ok_or_else(|| ControlError::NotFound(format!("request {request_id}")))?;
        let scope = TxScope {
            runs: Some(BTreeSet::from([run_id])),
            ready_queue: false,
            blocked_jobs: false,
            sessions: Some(session_id.into_iter().collect()),
            concurrency: false,
            pending_expansions: false,
            runs_via_requests: false,
            job_requests_all: false,
            runs_referenced: false,
        };
        self.read_scoped(&scope, |tx| {
            let record = tx
                .job_requests
                .get(&request_id)
                .cloned()
                .ok_or_else(|| ControlError::NotFound(format!("request {request_id}")))?;
            let message = tx
                .broker_messages
                .get(&request_id)
                .cloned()
                .ok_or_else(|| ControlError::NotFound(format!("request {request_id} message")))?;
            let run = tx
                .runs
                .get(&record.run_id)
                .ok_or_else(|| ControlError::NotFound(format!("run {}", record.run_id)))?;
            Ok(AcquireContext {
                request: record.clone(),
                message,
                token_request: tx.github_token_requests.get(&request_id).cloned(),
                id_token_granted: tx
                    .id_token_grants
                    .get(&(record.run_id, record.job_id.clone()))
                    .copied()
                    .unwrap_or(false),
                repository: run.submission.repository.clone(),
                trust_tier: run.submission.trust_tier.clone(),
            })
        })
    }

    async fn complete_job(
        &self,
        completion: JobCompletionInput,
    ) -> Result<CompleteOutcome, ControlError> {
        // `runs: {run_id}` + `runs_referenced` — the completion mutates its
        // own run plus the queued jobs' runs it promotes (`promote_ready_jobs`
        // summarizes them). `concurrency: true` widens `runs` with every
        // holder's run (a released gate can cancel or promote a different
        // run). `sessions: {owner}` — `settle_request` drops the moot
        // cancellation from the owner session's inflight messages; the owner
        // is resolved before the transaction.
        let sessions = completion
            .agent_job_id
            .map(|id| self.find_session_by_agent_job_id(id))
            .transpose()?
            .flatten()
            .into_iter()
            .collect();
        let scope = TxScope {
            runs: Some(BTreeSet::from([completion.run_id])),
            ready_queue: true,
            blocked_jobs: true,
            pending_expansions: false,
            runs_via_requests: false,
            sessions: Some(sessions),
            job_requests_all: false,
            concurrency: true,
            runs_referenced: true,
        };
        self.transact_scoped(&scope, |tx| commands::complete_job_tx(tx, completion))
    }
    async fn cancel_run(
        &self,
        run_id: RunId,
        reason: Option<String>,
    ) -> Result<CancelOutcome, ControlError> {
        // `cancel_run_inner` touches the cancelled run plus the global
        // queues/concurrency it releases; `concurrency: true` widens `runs`
        // with every holder's run (a released gate promotes a different
        // run), and `runs_referenced` adds the queued jobs' runs it
        // promotes. `sessions: {owners}` — `settle_request` drops the moot
        // cancellation from each owner session's inflight messages; owners
        // are resolved before the transaction.
        let sessions = self.find_sessions_by_run(run_id)?;
        let scope = TxScope {
            runs: Some(BTreeSet::from([run_id])),
            pending_expansions: false,
            runs_via_requests: false,
            ready_queue: true,
            blocked_jobs: true,
            job_requests_all: false,
            sessions: Some(sessions),
            concurrency: true,
            runs_referenced: true,
        };
        self.transact_scoped(&scope, |tx| {
            let cancellations = sched::cancel_run_inner(tx, run_id, reason.as_deref());
            let run_status = tx.runs.get(&run_id).map(|r| r.status);
            Ok(CancelOutcome {
                cancellations,
                run_status,
                queue_nonempty: !tx.ready_index.is_empty()
                    || !tx.queue.is_empty()
                    || !tx.cancellation_queue.is_empty(),
            })
        })
    }

    async fn cancel_job(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<CancelOutcome, ControlError> {
        let job_id = job_id.clone();
        let sessions = self.find_sessions_by_run(run_id)?;
        let scope = TxScope {
            runs: Some(BTreeSet::from([run_id])),
            ready_queue: true,
            blocked_jobs: true,
            sessions: Some(sessions),
            concurrency: true,
            runs_referenced: true,
            job_requests_all: false,
            pending_expansions: false,
            runs_via_requests: false,
        };
        self.transact_scoped(&scope, |tx| {
            let cancellations = sched::cancel_job_inner(tx, run_id, &job_id);
            let run_status = tx.runs.get(&run_id).map(|r| r.status);
            Ok(CancelOutcome {
                cancellations,
                run_status,
                queue_nonempty: !tx.ready_index.is_empty()
                    || !tx.queue.is_empty()
                    || !tx.cancellation_queue.is_empty(),
            })
        })
    }

    async fn renew_request(
        &self,
        request_id: i64,
        runner_id: i64,
    ) -> Result<TaskAgentJobRequestRecord, ControlError> {
        self.transact(|tx| {
            let record = tx
                .job_requests
                .get_mut(&request_id)
                .ok_or_else(|| ControlError::NotFound(format!("request {request_id}")))?;
            if record.owner_runner_id != Some(runner_id) {
                return Err(ControlError::Stale(format!(
                    "request {request_id} not owned by runner {runner_id}"
                )));
            }
            record.last_renewed_at = Some(std::time::SystemTime::now());
            record.locked_until = crate::distributed_task::agent_request_locked_until();
            Ok(record.clone())
        })
    }

    async fn release_request(&self, request_id: i64) -> Result<(), ControlError> {
        self.transact(|tx| {
            sched::release_request_for_retry(tx, request_id);
            Ok(())
        })
    }

    async fn register_runner(&self, reg: RegisterRunner) -> Result<RunnerRow, ControlError> {
        self.transact(|tx| commands::register_runner_tx(tx, reg))
    }

    async fn create_session(&self, session: CreateSession) -> Result<SessionRow, ControlError> {
        self.transact(|tx| commands::create_session_tx(tx, session))
    }

    async fn delete_session(&self, session_id: &str) -> Result<(), ControlError> {
        let session_id = session_id.to_owned();
        self.transact(|tx| {
            commands::delete_session_tx(tx, &session_id);
            Ok(())
        })
    }

    async fn purge_runner(&self, runner_id: i64) -> Result<(), ControlError> {
        self.transact(move |tx| {
            commands::purge_runner_tx(tx, runner_id);
            Ok(())
        })
    }

    async fn claim_expansion(&self) -> Result<Option<ExpansionClaim>, ControlError> {
        // `runs: Some(empty)` + `pending_expansions` loads only the deferred
        // nodes; `runs_referenced` widens `runs` to their runs so
        // `plan_expansion` sees the claimed node's `RunRecord`. The claimed
        // run is unknowable before the pop, so the referenced set is widened
        // in at load — O(#expand), not O(#runs).
        let scope = TxScope {
            runs: Some(BTreeSet::new()),
            ready_queue: false,
            blocked_jobs: false,
            sessions: Some(BTreeSet::new()),
            concurrency: true,
            runs_referenced: true,
            job_requests_all: false,
            pending_expansions: true,
            runs_via_requests: false,
        };
        self.transact_scoped(&scope, |tx| {
            let Some(job) = tx.pending_expansions.pop_front() else {
                return Ok(None);
            };
            let plan = sched::plan_expansion(tx, &job);
            let generation = tx
                .expand_generations
                .get(&(job.run_id, job.job_id.clone()))
                .copied()
                .unwrap_or(0)
                + 1;
            tx.expand_generations
                .insert((job.run_id, job.job_id.clone()), generation);
            tx.expanding.insert((job.run_id, job.job_id.clone()));
            // Keep the payload so write-back preserves it and a crash can
            // requeue the node.
            tx.expanding_jobs
                .insert((job.run_id, job.job_id.clone()), job.clone());
            Ok(Some(ExpansionClaim {
                job,
                generation,
                plan,
            }))
        })
    }

    async fn apply_expansion(
        &self,
        claim: ExpansionApply,
    ) -> Result<crate::runtime_scheduling::SchedulingOutcome, ControlError> {
        // Scoped to the expanded node's run — `sched::apply_expansion`
        // registers the built legs into that same run and `promote_ready_jobs`
        // only evaluates this run's pending jobs (`needs:` never cross runs).
        let scope = TxScope {
            runs: Some(BTreeSet::from([claim.job.run_id])),
            ready_queue: false,
            blocked_jobs: false,
            sessions: Some(BTreeSet::new()),
            concurrency: true,
            runs_referenced: false,
            job_requests_all: false,
            pending_expansions: false,
            runs_via_requests: false,
        };
        self.transact_scoped(&scope, |tx| {
            let mut outcome = crate::runtime_scheduling::SchedulingOutcome::default();
            // Fence on the claim's generation: a stale build is discarded.
            let current = tx
                .expand_generations
                .get(&(claim.job.run_id, claim.job.job_id.clone()))
                .copied()
                .unwrap_or(0);
            if current != claim.generation {
                return Ok(outcome);
            }
            let key = (claim.job.run_id, claim.job.job_id.clone());
            tx.expanding_jobs.remove(&key);
            tx.expand_generations.remove(&key);
            let ready = sched::apply_expansion(tx, claim.job, claim.built, &mut outcome);
            tx.pending_jobs.extend(ready);
            let promoted = sched::promote_ready_jobs(tx);
            outcome.merge(promoted);
            Ok(outcome)
        })
    }

    async fn reconcile_on_boot(&self) -> Result<ReconcileOutcome, ControlError> {
        self.transact(|tx| {
            let mut outcome = ReconcileOutcome::default();

            // Concurrency holders whose runs are terminal or missing would
            // park the group forever; drop them.
            let before = tx
                .concurrency_groups
                .values()
                .map(|g| g.running.is_some() as usize + g.pending.len())
                .sum::<usize>();
            sched::reconcile_concurrency_groups(tx);
            let after = tx
                .concurrency_groups
                .values()
                .map(|g| g.running.is_some() as usize + g.pending.len())
                .sum::<usize>();
            outcome.holders_dropped = before.saturating_sub(after);

            // Claims orphaned by the crash: a claimed job whose request is
            // no longer live (settled, or its owner runner is gone) is
            // requeued so a replacement runner picks it up.
            let orphaned: Vec<(RunId, JobId)> = tx
                .claimed_jobs
                .keys()
                .filter(|key| {
                    !tx.job_requests.values().any(|r| {
                        r.run_id == key.0
                            && r.job_id == key.1
                            && r.result.is_none()
                            && r.owner_runner_id.is_some()
                    })
                })
                .cloned()
                .collect();
            for key in orphaned {
                if sched::requeue_claimed(tx, key.0, &key.1) {
                    outcome.recovered += 1;
                } else {
                    outcome.failed += 1;
                }
            }

            // Expansion nodes claimed but never applied: their payloads are
            // still in `expanding_jobs` (the row's blob survived), so reset
            // the generation and push them back onto the pending-expansion
            // queue — recovered, never lost.
            let stuck: Vec<(RunId, JobId)> = tx.expanding.iter().cloned().collect();
            for key in stuck {
                if let Some(job) = tx.expanding_jobs.remove(&key) {
                    tx.expanding.remove(&key);
                    tx.expand_generations.remove(&key);
                    tx.pending_expansions.push_back(job);
                    outcome.recovered += 1;
                } else {
                    // No payload to requeue: drop the reservation and count
                    // it failed so the node can't wedge the run.
                    tx.expanding.remove(&key);
                    tx.expand_generations.remove(&key);
                    outcome.failed += 1;
                }
            }

            Ok(outcome)
        })
    }

    async fn run_record(&self, run_id: RunId) -> Result<RunRecord, ControlError> {
        self.read(|tx| {
            tx.runs
                .get(&run_id)
                .cloned()
                .ok_or_else(|| ControlError::NotFound(format!("run {run_id}")))
        })
    }

    async fn list_runs(&self, filter: RunListFilter) -> Result<Vec<RunRecord>, ControlError> {
        self.list_run_rows(filter)
    }

    async fn append_event(
        &self,
        event: &preloop_gha_protocol::NdjsonEvent,
    ) -> Result<(), ControlError> {
        self.append_event_row(event)
    }

    async fn terminal_jobs(&self) -> Result<BTreeSet<(RunId, JobId)>, ControlError> {
        self.terminal_job_rows()
    }

    async fn request(&self, key: RequestKey) -> Result<TaskAgentJobRequestRecord, ControlError> {
        self.transact(|tx| {
            let record = match &key {
                RequestKey::Id(id) => tx.job_requests.get(id),
                RequestKey::PlanId(plan) => tx
                    .plan_requests
                    .get(plan)
                    .and_then(|id| tx.job_requests.get(id)),

                RequestKey::AgentJobId(id) => tx
                    .agent_job_requests
                    .get(id)
                    .and_then(|rid| tx.job_requests.get(rid)),
                RequestKey::TimelineId(id) => tx
                    .timeline_requests
                    .get(id)
                    .and_then(|rid| tx.job_requests.get(rid)),
            };
            record
                .cloned()
                .ok_or_else(|| ControlError::NotFound("request".to_owned()))
        })
    }
    async fn create_log(&self, _plan_id: &str) -> Result<i64, ControlError> {
        run_blocking(|| {
            let mut conn = self.conn.lock();
            let tx = conn.transaction().map_err(ControlError::backend)?;
            let next_id: i64 = tx
                .query_row(
                    "INSERT INTO counters(name, value) VALUES ('next_log_id', 1) \
                     ON CONFLICT(name) DO UPDATE SET value = counters.value + 1 \
                     RETURNING value - 1",
                    [],
                    |row| row.get(0),
                )
                .map_err(ControlError::backend)?;
            tx.commit().map_err(ControlError::backend)?;
            Ok(next_id)
        })
    }

    async fn store_meta(&self, meta: &crate::store::MetaSnapshot) -> Result<(), ControlError> {
        let value = unblob(&self.cipher, meta)?;
        run_blocking(|| {
            self.conn
                .lock()
                .execute(
                    "INSERT INTO meta(key, value) VALUES ('local_state', ?1) \
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    params![value],
                )
                .map_err(ControlError::backend)?;
            Ok(())
        })
    }

    async fn load_meta(&self) -> Result<Option<crate::store::MetaSnapshot>, ControlError> {
        self.with_reader(|conn| {
            let value = conn
                .query_row(
                    "SELECT value FROM meta WHERE key = 'local_state'",
                    [],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .optional()
                .map_err(ControlError::backend)?;
            value.map(|value| blob(&self.cipher, &value)).transpose()
        })
    }

    async fn enqueue_webhook_delivery(
        &self,
        delivery: &WebhookDeliveryRecord,
    ) -> Result<bool, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .enqueue_webhook_delivery(delivery)
                .map_err(ControlError::backend)
        })
    }

    async fn claim_webhook_deliveries(
        &self,
        limit: usize,
        lease_duration_secs: u64,
    ) -> Result<Vec<WebhookDeliveryRecord>, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .claim_webhook_deliveries(limit, lease_duration_secs)
                .map_err(ControlError::backend)
        })
    }

    async fn renew_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        lease_duration_secs: u64,
    ) -> Result<bool, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .renew_webhook_delivery(delivery_id, lease_token, lease_duration_secs)
                .map_err(ControlError::backend)
        })
    }

    async fn complete_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
    ) -> Result<bool, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .complete_webhook_delivery(delivery_id, lease_token)
                .map_err(ControlError::backend)
        })
    }

    async fn fail_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        error: &str,
        permanent: bool,
        retry_delay: Option<std::time::Duration>,
    ) -> Result<bool, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .fail_webhook_delivery(delivery_id, lease_token, error, permanent, retry_delay)
                .map_err(ControlError::backend)
        })
    }

    async fn get_webhook_delivery(
        &self,
        delivery_id: &str,
    ) -> Result<Option<WebhookDeliveryRecord>, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .get_webhook_delivery(delivery_id)
                .map_err(ControlError::backend)
        })
    }

    async fn count_dead_letter_webhook_deliveries(&self) -> Result<u64, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .count_dead_letter_webhook_deliveries()
                .map_err(ControlError::backend)
        })
    }

    async fn recover_webhook_deliveries(&self) -> Result<u64, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .recover_webhook_deliveries()
                .map_err(ControlError::backend)
        })
    }

    async fn prune_webhook_deliveries(
        &self,
        before_us: i64,
        limit: usize,
    ) -> Result<u64, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .prune_webhook_deliveries(before_us, limit)
                .map_err(ControlError::backend)
        })
    }

    async fn park_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        error: &str,
        retry_delay_secs: u64,
    ) -> Result<bool, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .park_webhook_delivery(delivery_id, lease_token, error, retry_delay_secs)
                .map_err(ControlError::backend)
        })
    }

    async fn requeue_webhook_delivery(&self, delivery_id: &str) -> Result<bool, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .requeue_webhook_delivery(delivery_id)
                .map_err(ControlError::backend)
        })
    }

    async fn list_webhook_deliveries(
        &self,
        state: Option<WebhookDeliveryStatus>,
        limit: usize,
    ) -> Result<Vec<WebhookDeliverySummary>, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .list_webhook_deliveries(state, limit)
                .map_err(ControlError::backend)
        })
    }

    async fn webhook_deliveries_present(
        &self,
        delivery_ids: &[String],
    ) -> Result<BTreeSet<String>, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .webhook_deliveries_present(delivery_ids)
                .map_err(ControlError::backend)
        })
    }

    async fn webhook_queue_stats(&self) -> Result<WebhookQueueStats, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .webhook_queue_stats()
                .map_err(ControlError::backend)
        })
    }

    async fn load_webhook_watchdog_cursor(
        &self,
        scope: &str,
    ) -> Result<Option<WebhookWatchdogCursor>, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .load_webhook_watchdog_cursor(scope)
                .map_err(ControlError::backend)
        })
    }

    async fn store_webhook_watchdog_cursor(
        &self,
        cursor: &WebhookWatchdogCursor,
    ) -> Result<(), ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .store_webhook_watchdog_cursor(cursor)
                .map_err(ControlError::backend)
        })
    }

    async fn upsert_webhook_redelivery(
        &self,
        record: &WebhookRedeliveryRecord,
    ) -> Result<(), ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .upsert_webhook_redelivery(record)
                .map_err(ControlError::backend)
        })
    }

    async fn load_webhook_redelivery(
        &self,
        delivery_guid: &str,
    ) -> Result<Option<WebhookRedeliveryRecord>, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .load_webhook_redelivery(delivery_guid)
                .map_err(ControlError::backend)
        })
    }

    async fn open_webhook_redeliveries(
        &self,
        limit: usize,
    ) -> Result<Vec<WebhookRedeliveryRecord>, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .open_webhook_redeliveries(limit)
                .map_err(ControlError::backend)
        })
    }

    async fn resolve_webhook_redelivery(
        &self,
        delivery_guid: &str,
        resolved_at_us: i64,
    ) -> Result<bool, ControlError> {
        run_blocking(|| {
            self.auxiliary()?
                .resolve_webhook_redelivery(delivery_guid, resolved_at_us)
                .map_err(ControlError::backend)
        })
    }

    async fn queue_stats(&self) -> Result<QueueStats, ControlError> {
        // NOTE: this endpoint must stay on `TxScope::full()` (via `read`).
        // `ready` reads the global `ready_count` counter, but `pending`,
        // `blocked`, `held`, `claimed` and `expanding` are loaded-subset
        // lengths — under a narrower scope they would undercount while
        // `ready` stayed global, giving an inconsistently half-global stat.
        self.read(|tx| {
            Ok(QueueStats {
                ready: tx.ready_count.max(0) as usize,
                pending: tx.pending_jobs.len(),
                blocked: tx.concurrency_blocked.len(),
                held: tx.held_runs.values().map(|v| v.len()).sum(),
                claimed: tx.claimed_jobs.len(),
                expanding: tx.pending_expansions.len(),
                next_runs_on: sched::next_job_labels(tx),
            })
        })
    }

    async fn live_assignments(
        &self,
    ) -> Result<Vec<preloop_observability::status::RunnerAssignment>, ControlError> {
        self.read(|tx| {
            Ok(crate::runtime_scheduling::live_runner_assignments(
                &tx.job_requests,
                &tx.session_active_requests,
                std::time::SystemTime::now(),
            ))
        })
    }
}
