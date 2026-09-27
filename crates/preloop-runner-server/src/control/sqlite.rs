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
use std::collections::BTreeMap;

use std::collections::BTreeSet;

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
        // Greenfield schema: a fresh database (version 0) gets the DDL; a
        // database stamped with any other version predates it and is
        // refused rather than half-read.
        let existing_version: i64 = conn
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .map_err(ControlError::backend)?;
        if existing_version != 0 && existing_version != SQLITE_SCHEMA_VERSION {
            return Err(ControlError::backend(anyhow::anyhow!(
                "control database {} has schema version {existing_version}; this build \
                 supports only {SQLITE_SCHEMA_VERSION}. Recreate the database.",
                path.display()
            )));
        }
        conn.execute_batch(SQLITE_DDL)
            .map_err(ControlError::backend)?;
        conn.pragma_update(None, "user_version", SQLITE_SCHEMA_VERSION)
            .map_err(ControlError::backend)?;
        // Foreign keys default OFF per connection; enable so the schema's
        // ON DELETE CASCADE clauses fire under targeted deletes.
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
        if scope.include_archived {
            return Err(ControlError::BadRequest(
                "history scope is read-only".into(),
            ));
        }
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
            let Some((_, _, mut run, _)) = load_runs(
                conn,
                &self.cipher,
                " WHERE webhook_delivery_id = ? AND workflow_path = ?",
                &[delivery_id.to_owned(), workflow_path.to_owned()],
            )?
            .pop() else {
                return Ok(None);
            };
            let run_id_s = run.run_id.0.to_string();
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
    /// Shared read for `acquire_context` / `acquire_for_runner`: the request
    /// row joined to its sealed message, token-mint request, id-token grant
    /// and the run's submission fields — one statement on the reader pool.
    fn acquire_impl(
        &self,
        request_id: i64,
        runner_id: Option<i64>,
    ) -> Result<AcquireContext, ControlError> {
        self.with_reader(move |conn| {
            let row = conn
                .query_row(
                    "SELECT jr.request_id, jr.run_id, jr.job_id, jr.agent_job_id, \
                     jr.plan_id, jr.plan_type, jr.timeline_id, jr.result, jr.locked_until, \
                     jr.claimed_at_us, jr.owner_runner_id, jr.started_at_us, \
                     jr.last_renewed_at_us, jr.timeout_triggered, jr.debug_token_issued, \
                     jr.request_blob, \
                     sub.submission_json, \
                     tok.request_blob, \
                     g.granted, \
                     (sess.session_id IS NOT NULL), sess.runner_id \
                     FROM job_requests jr \
                     JOIN run_submissions sub ON sub.run_id = jr.run_id \
                     LEFT JOIN github_token_requests tok ON tok.request_id = jr.request_id \
                     LEFT JOIN id_token_grants g \
                         ON g.run_id = jr.run_id AND g.job_id = jr.job_id \
                     LEFT JOIN runner_sessions sess ON sess.active_request_id = jr.request_id \
                     WHERE jr.request_id = ?1",
                    params![request_id],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, Option<String>>(7)?,
                            row.get::<_, String>(8)?,
                            row.get::<_, Option<i64>>(9)?,
                            row.get::<_, Option<i64>>(10)?,
                            row.get::<_, Option<i64>>(11)?,
                            row.get::<_, Option<i64>>(12)?,
                            row.get::<_, i64>(13)?,
                            row.get::<_, i64>(14)?,
                            row.get::<_, Option<Vec<u8>>>(15)?,
                            row.get::<_, String>(16)?,
                            row.get::<_, Option<Vec<u8>>>(17)?,
                            row.get::<_, Option<i64>>(18)?,
                            row.get::<_, bool>(19)?,
                            row.get::<_, Option<i64>>(20)?,
                        ))
                    },
                )
                .optional()
                .map_err(ControlError::backend)?;
            let Some((
                rid,
                run_id_s,
                job_id_s,
                agent_s,
                plan_id,
                plan_type,
                timeline_s,
                result_s,
                locked_until,
                claimed_at_us,
                owner_runner_id,
                started_at_us,
                last_renewed_at_us,
                timeout_triggered,
                debug_token_issued,
                message_blob,
                submission_json,
                token_blob,
                granted,
                has_session,
                session_runner,
            )) = row
            else {
                return Err(ControlError::NotFound(format!("request {request_id}")));
            };
            let record = TaskAgentJobRequestRecord {
                request_id: rid,
                run_id: parse_run_id(&run_id_s),
                job_id: JobId(job_id_s),
                agent_job_id: parse_uuid(&agent_s),
                plan_id,
                plan_type,
                timeline_id: parse_uuid(&timeline_s),
                result: result_s.as_deref().map(status_parse),
                locked_until,
                claimed_at: claimed_at_us.map(us_to_system),
                owner_runner_id,
                started_at: started_at_us.map(us_to_system),
                last_renewed_at: last_renewed_at_us.map(us_to_system),
                timeout_triggered: timeout_triggered != 0,
                debug_token_issued: debug_token_issued != 0,
            };
            if let Some(runner_id) = runner_id {
                ensure_request_owner(owner_runner_id, session_runner, has_session, runner_id)?;
                if record.result.is_some() {
                    return Err(ControlError::Conflict(
                        "broker request already completed".to_owned(),
                    ));
                }
            }
            let message_blob = message_blob
                .ok_or_else(|| ControlError::NotFound(format!("request {request_id} message")))?;
            let message = blob(&self.cipher, &message_blob)?;
            let token_request = token_blob.map(|raw| blob(&self.cipher, &raw)).transpose()?;
            let submission: preloop_gha_protocol::WorkflowSubmission =
                serde_json::from_str(&submission_json).map_err(ControlError::backend)?;
            Ok(AcquireContext {
                request: record,
                message,
                token_request,
                id_token_granted: granted.map(|g| g != 0),
                repository: submission.repository,
                trust_tier: submission.trust_tier,
            })
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
                    "SELECT selected.run_id,
                            COALESCE(j.job_id,h.job_id), COALESCE(j.status,h.status),
                            COALESCE(j.queue_kind,'none'),
                            COALESCE((
                                SELECT jr.agent_job_id FROM job_requests jr
                                WHERE jr.run_id=j.run_id AND jr.job_id=j.job_id
                                ORDER BY jr.request_id DESC LIMIT 1
                            ), (
                                SELECT ah.agent_job_id FROM attempt_history ah
                                WHERE ah.run_id=selected.run_id
                                  AND ah.run_attempt=selected.run_attempt
                                  AND ah.run_created_at_us=selected.created_at_us
                                  AND ah.job_id=h.job_id
                                ORDER BY ah.request_id DESC LIMIT 1
                            ))
                     FROM (
                         SELECT run_id, run_attempt, created_at_us, archived_at_us,
                                CASE WHEN status IN ('success','failure','skipped','cancelled')
                                     THEN 1 ELSE 0 END AS terminal_rank,
                                COALESCE(completed_at_us, started_at_us, created_at_us) AS sort_at
                         FROM runs
                         WHERE (?1 IS NULL OR instr(workflow_path, ?1) > 0)
                           AND (?2 IS NULL OR status = ?2)
                           AND (?3 IS NULL OR event = ?3)
                         ORDER BY terminal_rank, sort_at DESC LIMIT ?4
                     ) AS selected
                     LEFT JOIN jobs j ON j.run_id=selected.run_id AND selected.archived_at_us IS NULL
                     LEFT JOIN job_history h ON h.run_id=selected.run_id
                         AND h.run_attempt=selected.run_attempt
                         AND h.run_created_at_us=selected.created_at_us
                         AND selected.archived_at_us IS NOT NULL
                     ORDER BY selected.terminal_rank, selected.sort_at DESC,
                              COALESCE(j.job_id,h.job_id)",
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
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, Option<String>>(3)?,
                            row.get::<_, Option<String>>(4)?,
                        ))
                    },
                )
                .map_err(ControlError::backend)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(ControlError::backend)?;
            // Steps for each job's latest attempt, live or archived, in one
            // batched read instead of one decode per job.
            let attempts: Vec<String> = rows
                .iter()
                .filter_map(|(.., agent)| agent.clone())
                .collect();
            let mut steps: std::collections::BTreeMap<uuid::Uuid, Vec<crate::models::StepRecord>> =
                std::collections::BTreeMap::new();
            for chunk in attempts.chunks(500) {
                let ph = vec!["?"; chunk.len()].join(",");
                let sql = format!(
                    "SELECT {STEP_COLUMNS} FROM (
                         SELECT {STEP_COLUMNS}, position FROM job_steps
                         WHERE agent_job_id IN ({ph})
                         UNION ALL
                         SELECT {STEP_COLUMNS}, position FROM step_history
                         WHERE agent_job_id IN ({ph}))
                     ORDER BY agent_job_id, position"
                );
                let params: Vec<String> = chunk.iter().chain(chunk.iter()).cloned().collect();
                for (agent, record) in read_step_rows(conn, &sql, &params)? {
                    steps.entry(agent).or_default().push(record);
                }
            }
            // The selected runs' records, assembled in one batched read.
            let mut run_ids: Vec<String> = rows.iter().map(|(run, ..)| run.clone()).collect();
            run_ids.dedup();
            let mut records: std::collections::HashMap<String, RunRecord> =
                std::collections::HashMap::new();
            for chunk in run_ids.chunks(500) {
                let ph = vec!["?"; chunk.len()].join(",");
                for (run_id, _, record, _) in
                    load_runs(conn, &self.cipher, &format!(" WHERE run_id IN ({ph})"), chunk)?
                {
                    records.insert(run_id.0.to_string(), record);
                }
            }
            let mut runs = Vec::new();
            let mut current_id: Option<String> = None;
            let mut current_run: Option<RunRecord> = None;
            let mut current_jobs = Vec::new();
            for (run_id, job_id, status, queue_kind, agent) in rows {
                if current_id.as_deref() != Some(run_id.as_str()) {
                    if let Some(run) = current_run.take() {
                        runs.push(project_run_rows(run, std::mem::take(&mut current_jobs)));
                    }
                    current_run = records.remove(&run_id);
                    current_id = Some(run_id);
                }
                if let (Some(job_id), Some(status), Some(queue_kind)) = (job_id, status, queue_kind)
                {
                    let job_steps = agent.and_then(|a| steps.get(&parse_uuid(&a)).cloned());
                    current_jobs.push((JobId(job_id), status_parse(&status), queue_kind, job_steps));
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
                    "SELECT run_id, job_id FROM jobs
                     WHERE status IN ('success','failure','skipped','cancelled')
                     UNION ALL SELECT h.run_id,h.job_id FROM job_history h
                     JOIN runs r ON r.run_id=h.run_id
                     WHERE h.run_attempt=r.run_attempt AND h.run_created_at_us=r.created_at_us
                       AND h.status IN ('success','failure','skipped','cancelled')",
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

/// Columns every step read selects, in [`read_step_rows`] order. The same
/// list serves `job_steps` and `step_history`.
const STEP_COLUMNS: &str = "agent_job_id, step_id, kind, workflow_index, runner_number, \
     context_name, name, conclusion, started_at_us, finished_at_us";

/// Run a step query selecting [`STEP_COLUMNS`] and decode each row. Callers
/// order by `(agent_job_id, position)` so pushing preserves manifest order.
fn read_step_rows(
    conn: &Connection,
    sql: &str,
    params: &[String],
) -> Result<Vec<(uuid::Uuid, crate::models::StepRecord)>, ControlError> {
    let mut stmt = conn.prepare(sql).map_err(ControlError::backend)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params.iter()), |r| {
            Ok((
                parse_uuid(&r.get::<_, String>(0)?),
                super::rows::StepRow::into_record(
                    r.get(1)?,
                    &r.get::<_, String>(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                    r.get(8)?,
                    r.get(9)?,
                ),
            ))
        })
        .map_err(ControlError::backend)?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(ControlError::backend)
}

/// Apply one attempt's step delta: upsert changed rows, delete removed ids.
fn write_step_delta(
    conn: &Connection,
    agent_job_id: &uuid::Uuid,
    delta: &super::rows::StepDelta,
) -> Result<(), ControlError> {
    let agent = agent_job_id.to_string();
    for id in &delta.deletes {
        conn.execute(
            "DELETE FROM job_steps WHERE agent_job_id=?1 AND step_id=?2",
            params![agent, id],
        )
        .map_err(ControlError::backend)?;
    }
    for row in &delta.upserts {
        conn.execute(
            "INSERT INTO job_steps (agent_job_id, step_id, position, kind, workflow_index, \
             runner_number, context_name, name, conclusion, started_at_us, finished_at_us) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11) \
             ON CONFLICT(agent_job_id, step_id) DO UPDATE SET position=excluded.position, \
             kind=excluded.kind, workflow_index=excluded.workflow_index, \
             runner_number=excluded.runner_number, context_name=excluded.context_name, \
             name=excluded.name, conclusion=excluded.conclusion, \
             started_at_us=excluded.started_at_us, finished_at_us=excluded.finished_at_us",
            params![
                agent,
                row.step_id,
                row.position,
                row.kind,
                row.workflow_index,
                row.runner_number,
                row.context_name,
                row.name,
                row.conclusion,
                row.started_at_us,
                row.finished_at_us,
            ],
        )
        .map_err(ControlError::backend)?;
    }
    Ok(())
}

/// Persist a job's payload: queryable columns, `needs:` edges and the sealed
/// runner message + `if:` context. Runs after the `jobs` upsert (FK order).
fn write_job_payload(
    conn: &Connection,
    cipher: &store::Envelope,
    run_id: &RunId,
    job_id: &JobId,
    job: &QueuedJob,
) -> Result<(), ControlError> {
    let run = run_id.0.to_string();
    let row = super::rows::JobPayloadRow::from_job(job);
    conn.execute(
        "UPDATE jobs SET created_at_ns=?3, deps_ready_at_ns=?4, concurrency_wait_at_ns=?5, \
         concurrency_acquired_at_ns=?6, if_condition=?7, max_parallel=?8, \
         environment_json=?9, concurrency_json=?10, matrix_json=?11, deferred_matrix=?12, \
         reusable_call_json=?13 WHERE run_id=?1 AND job_id=?2",
        params![
            run,
            job_id.0,
            row.created_at_ns,
            row.deps_ready_at_ns,
            row.concurrency_wait_at_ns,
            row.concurrency_acquired_at_ns,
            row.if_condition,
            row.max_parallel,
            row.environment_json,
            row.concurrency_json,
            row.matrix_json,
            row.deferred_matrix,
            row.reusable_call_json,
        ],
    )
    .map_err(ControlError::backend)?;
    conn.execute(
        "DELETE FROM job_needs WHERE run_id=?1 AND job_id=?2",
        params![run, job_id.0],
    )
    .map_err(ControlError::backend)?;
    for (position, need) in row.needs.iter().enumerate() {
        conn.execute(
            "INSERT INTO job_needs (run_id, job_id, position, needs_job_id) \
             VALUES (?1,?2,?3,?4)",
            params![run, job_id.0, position as i64, need],
        )
        .map_err(ControlError::backend)?;
    }
    conn.execute(
        "INSERT INTO job_messages (run_id, job_id, message_blob, condition_context_blob) \
         VALUES (?1,?2,?3,?4) ON CONFLICT(run_id, job_id) DO UPDATE SET \
         message_blob=excluded.message_blob, \
         condition_context_blob=excluded.condition_context_blob",
        params![
            run,
            job_id.0,
            unblob(cipher, &job.message)?,
            unblob(cipher, &job.condition_context)?,
        ],
    )
    .map_err(ControlError::backend)?;
    Ok(())
}

/// Load run records for the runs matching `filter` (a `WHERE …` clause over
/// `runs`, or empty for all) from their decomposed tables. Returns each run's
/// namespace, its assembled record (the `jobs` status map empty — callers
/// rebuild it from `jobs`) and the row snapshot write-back diffs against.
fn load_runs(
    conn: &Connection,
    cipher: &store::Envelope,
    filter: &str,
    params: &[String],
) -> Result<Vec<(RunId, String, RunRecord, super::rows::RunParts)>, ControlError> {
    use super::rows::{RunBaseJobRow, RunJobRow, RunParts, RunScalars, RunSubmissionRow};
    let mut parts: std::collections::BTreeMap<String, (String, RunParts)> =
        std::collections::BTreeMap::new();
    {
        let sql = format!(
            "SELECT run_id, namespace, status, run_number, run_attempt, run_name, event, \
             workflow_path, conclusion, webhook_delivery_id, head_sha, workflow_ref, \
             push_state_json, snapshot_timing_json, created_at_us, started_at_us, \
             completed_at_us FROM runs{filter}"
        );
        let mut stmt = conn.prepare(&sql).map_err(ControlError::backend)?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params.iter()))
            .map_err(ControlError::backend)?;
        let default_submission = RunSubmissionRow {
            submission_json: serde_json::to_string(
                &preloop_gha_protocol::WorkflowSubmission::default(),
            )
            .map_err(ControlError::backend)?,
            secrets: std::collections::BTreeMap::new(),
            github_json: "null".to_owned(),
            workspace_snapshot_json: None,
        };
        while let Some(r) = rows.next().map_err(ControlError::backend)? {
            let run_id: String = r.get(0).map_err(ControlError::backend)?;
            let status: String = r.get(2).map_err(ControlError::backend)?;
            let scalars = RunScalars {
                status: status_parse(&status),
                run_number: r.get(3).map_err(ControlError::backend)?,
                run_attempt: r.get(4).map_err(ControlError::backend)?,
                run_name: r.get(5).map_err(ControlError::backend)?,
                event: r.get(6).map_err(ControlError::backend)?,
                workflow_path: r.get(7).map_err(ControlError::backend)?,
                conclusion: r.get(8).map_err(ControlError::backend)?,
                webhook_delivery_id: r.get(9).map_err(ControlError::backend)?,
                head_sha: r.get(10).map_err(ControlError::backend)?,
                workflow_ref: r.get(11).map_err(ControlError::backend)?,
                push_state_json: r.get(12).map_err(ControlError::backend)?,
                snapshot_timing_json: r.get(13).map_err(ControlError::backend)?,
                created_at_us: r.get(14).map_err(ControlError::backend)?,
                started_at_us: r.get(15).map_err(ControlError::backend)?,
                completed_at_us: r.get(16).map_err(ControlError::backend)?,
            };
            parts.insert(
                run_id,
                (
                    r.get(1).map_err(ControlError::backend)?,
                    RunParts {
                        scalars,
                        // Pre-v17 runs have no submission row: they load
                        // with an empty submission rather than failing.
                        submission: default_submission.clone(),
                        jobs: std::collections::BTreeMap::new(),
                        base_jobs: std::collections::BTreeMap::new(),
                        reusable_calls: std::collections::BTreeMap::new(),
                    },
                ),
            );
        }
    }
    if parts.is_empty() {
        return Ok(Vec::new());
    }
    let child = |table: &str, columns: &str| {
        format!(
            "SELECT run_id, {columns} FROM {table} \
             WHERE run_id IN (SELECT run_id FROM runs{filter})"
        )
    };
    {
        let mut stmt = conn
            .prepare(&child(
                "run_submissions",
                "submission_json, secrets_blob, github_json, workspace_snapshot_json",
            ))
            .map_err(ControlError::backend)?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params.iter()))
            .map_err(ControlError::backend)?;
        while let Some(r) = rows.next().map_err(ControlError::backend)? {
            let run_id: String = r.get(0).map_err(ControlError::backend)?;
            let Some((_, part)) = parts.get_mut(&run_id) else {
                continue;
            };
            let secrets: Vec<u8> = r.get(2).map_err(ControlError::backend)?;
            part.submission = RunSubmissionRow {
                submission_json: r.get(1).map_err(ControlError::backend)?,
                secrets: blob(cipher, &secrets)?,
                github_json: r.get(3).map_err(ControlError::backend)?,
                workspace_snapshot_json: r.get(4).map_err(ControlError::backend)?,
            };
        }
    }
    {
        let mut stmt = conn
            .prepare(&child(
                "run_jobs",
                "job_id, base_id, display_name, needs_json, outputs_json, check_run_id, \
                 detail_json, detail_position, caller_plan_json",
            ))
            .map_err(ControlError::backend)?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params.iter()))
            .map_err(ControlError::backend)?;
        while let Some(r) = rows.next().map_err(ControlError::backend)? {
            let run_id: String = r.get(0).map_err(ControlError::backend)?;
            let Some((_, part)) = parts.get_mut(&run_id) else {
                continue;
            };
            part.jobs.insert(
                r.get(1).map_err(ControlError::backend)?,
                RunJobRow {
                    base_id: r.get(2).map_err(ControlError::backend)?,
                    display_name: r.get(3).map_err(ControlError::backend)?,
                    needs_json: r.get(4).map_err(ControlError::backend)?,
                    outputs_json: r.get(5).map_err(ControlError::backend)?,
                    check_run_id: r.get(6).map_err(ControlError::backend)?,
                    detail_json: r.get(7).map_err(ControlError::backend)?,
                    detail_position: r.get(8).map_err(ControlError::backend)?,
                    caller_plan_json: r.get(9).map_err(ControlError::backend)?,
                },
            );
        }
    }
    {
        let mut stmt = conn
            .prepare(&child(
                "run_base_jobs",
                "base_id, fail_fast, continue_on_error",
            ))
            .map_err(ControlError::backend)?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params.iter()))
            .map_err(ControlError::backend)?;
        while let Some(r) = rows.next().map_err(ControlError::backend)? {
            let run_id: String = r.get(0).map_err(ControlError::backend)?;
            let Some((_, part)) = parts.get_mut(&run_id) else {
                continue;
            };
            part.base_jobs.insert(
                r.get(1).map_err(ControlError::backend)?,
                RunBaseJobRow {
                    fail_fast: r.get(2).map_err(ControlError::backend)?,
                    continue_on_error: r.get(3).map_err(ControlError::backend)?,
                },
            );
        }
    }
    {
        let mut stmt = conn
            .prepare(&child("reusable_calls", "caller_job_id, metadata_json"))
            .map_err(ControlError::backend)?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params.iter()))
            .map_err(ControlError::backend)?;
        while let Some(r) = rows.next().map_err(ControlError::backend)? {
            let run_id: String = r.get(0).map_err(ControlError::backend)?;
            let Some((_, part)) = parts.get_mut(&run_id) else {
                continue;
            };
            part.reusable_calls.insert(
                r.get(1).map_err(ControlError::backend)?,
                r.get(2).map_err(ControlError::backend)?,
            );
        }
    }
    let mut out = Vec::with_capacity(parts.len());
    for (run_id_s, (namespace, part)) in parts {
        let run_id = parse_run_id(&run_id_s);
        let record = part
            .clone()
            .into_record(run_id)
            .map_err(ControlError::backend)?;
        out.push((run_id, namespace, record, part));
    }
    Ok(out)
}

/// Write one run's decomposed rows, touching only what differs from the
/// loaded snapshot `prev` (`None` = new run: write everything).
fn write_run(
    conn: &Connection,
    cipher: &store::Envelope,
    run_id: &RunId,
    namespace: &str,
    prev: Option<&super::rows::RunParts>,
    next: &super::rows::RunParts,
) -> Result<(), ControlError> {
    let run = run_id.0.to_string();
    if prev.map(|p| &p.scalars) != Some(&next.scalars) {
        let s = &next.scalars;
        conn.execute(
            "INSERT INTO runs (run_id, namespace, status, run_number, run_attempt, run_name, \
             event, workflow_path, conclusion, webhook_delivery_id, head_sha, workflow_ref, \
             push_state_json, snapshot_timing_json, created_at_us, started_at_us, \
             completed_at_us) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17) \
             ON CONFLICT(run_id) DO UPDATE SET status=excluded.status, \
             run_name=excluded.run_name, conclusion=excluded.conclusion, \
             head_sha=excluded.head_sha, workflow_ref=excluded.workflow_ref, \
             push_state_json=excluded.push_state_json, \
             snapshot_timing_json=excluded.snapshot_timing_json, \
             started_at_us=excluded.started_at_us, completed_at_us=excluded.completed_at_us",
            params![
                run,
                namespace,
                status_str(s.status),
                s.run_number,
                s.run_attempt,
                s.run_name,
                s.event,
                s.workflow_path,
                s.conclusion,
                s.webhook_delivery_id,
                s.head_sha,
                s.workflow_ref,
                s.push_state_json,
                s.snapshot_timing_json,
                s.created_at_us,
                s.started_at_us,
                s.completed_at_us,
            ],
        )
        .map_err(ControlError::backend)?;
    }
    if prev.map(|p| &p.submission) != Some(&next.submission) {
        let s = &next.submission;
        conn.execute(
            "INSERT INTO run_submissions (run_id, submission_json, secrets_blob, github_json, \
             workspace_snapshot_json) VALUES (?1,?2,?3,?4,?5) \
             ON CONFLICT(run_id) DO UPDATE SET submission_json=excluded.submission_json, \
             secrets_blob=excluded.secrets_blob, github_json=excluded.github_json, \
             workspace_snapshot_json=excluded.workspace_snapshot_json",
            params![
                run,
                s.submission_json,
                unblob(cipher, &s.secrets)?,
                s.github_json,
                s.workspace_snapshot_json,
            ],
        )
        .map_err(ControlError::backend)?;
    }
    for (job, row) in &next.jobs {
        if prev.and_then(|p| p.jobs.get(job)) == Some(row) {
            continue;
        }
        conn.execute(
            "INSERT INTO run_jobs (run_id, job_id, base_id, display_name, needs_json, \
             outputs_json, check_run_id, detail_json, detail_position, caller_plan_json) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) \
             ON CONFLICT(run_id, job_id) DO UPDATE SET base_id=excluded.base_id, \
             display_name=excluded.display_name, needs_json=excluded.needs_json, \
             outputs_json=excluded.outputs_json, check_run_id=excluded.check_run_id, \
             detail_json=excluded.detail_json, detail_position=excluded.detail_position, \
             caller_plan_json=excluded.caller_plan_json",
            params![
                run,
                job,
                row.base_id,
                row.display_name,
                row.needs_json,
                row.outputs_json,
                row.check_run_id,
                row.detail_json,
                row.detail_position,
                row.caller_plan_json,
            ],
        )
        .map_err(ControlError::backend)?;
    }
    for (base, row) in &next.base_jobs {
        if prev.and_then(|p| p.base_jobs.get(base)) == Some(row) {
            continue;
        }
        conn.execute(
            "INSERT INTO run_base_jobs (run_id, base_id, fail_fast, continue_on_error) \
             VALUES (?1,?2,?3,?4) ON CONFLICT(run_id, base_id) DO UPDATE SET \
             fail_fast=excluded.fail_fast, continue_on_error=excluded.continue_on_error",
            params![run, base, row.fail_fast, row.continue_on_error],
        )
        .map_err(ControlError::backend)?;
    }
    for (caller, meta) in &next.reusable_calls {
        if prev.and_then(|p| p.reusable_calls.get(caller)) == Some(meta) {
            continue;
        }
        conn.execute(
            "INSERT INTO reusable_calls (run_id, caller_job_id, metadata_json) \
             VALUES (?1,?2,?3) ON CONFLICT(run_id, caller_job_id) DO UPDATE SET \
             metadata_json=excluded.metadata_json",
            params![run, caller, meta],
        )
        .map_err(ControlError::backend)?;
    }
    if let Some(prev) = prev {
        for job in prev.jobs.keys().filter(|k| !next.jobs.contains_key(*k)) {
            conn.execute(
                "DELETE FROM run_jobs WHERE run_id=?1 AND job_id=?2",
                params![run, job],
            )
            .map_err(ControlError::backend)?;
        }
        for base in prev
            .base_jobs
            .keys()
            .filter(|k| !next.base_jobs.contains_key(*k))
        {
            conn.execute(
                "DELETE FROM run_base_jobs WHERE run_id=?1 AND base_id=?2",
                params![run, base],
            )
            .map_err(ControlError::backend)?;
        }
        for caller in prev
            .reusable_calls
            .keys()
            .filter(|k| !next.reusable_calls.contains_key(*k))
        {
            conn.execute(
                "DELETE FROM reusable_calls WHERE run_id=?1 AND caller_job_id=?2",
                params![run, caller],
            )
            .map_err(ControlError::backend)?;
        }
    }
    Ok(())
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
    // run's jobs stay queued. Hold/waiter tables are small (active gates
    // only), so the extra scan is cheap; the full load below re-reads them.
    let mut effective_scope = scope.clone();
    if let Some(runs) = effective_scope.runs.as_mut() {
        if scope.concurrency {
            let mut stmt = conn
                .prepare(
                    "SELECT holder_run_id FROM concurrency_holds \
                     UNION SELECT holder_run_id FROM concurrency_waits",
                )
                .map_err(ControlError::backend)?;
            let rows = stmt
                .query_map([], |r| r.get::<_, String>(0))
                .map_err(ControlError::backend)?;
            for row in rows {
                runs.insert(parse_run_id(&row.map_err(ControlError::backend)?));
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

    // Runs: assembled from `runs` + its decomposed child tables; the
    // `jobs` status map is rebuilt from the `jobs` table below. Scoped to
    // `scope.runs`.
    {
        let (sql, params) = scoped_select("", "run_id", scope.runs.as_ref(), |id| id.0.to_string());
        for (run_id, namespace, run, parts) in load_runs(conn, cipher, &sql, &params)? {
            tx.run_namespaces.insert(run_id, namespace);
            tx.runs.insert(run_id, run);
            tx.loaded.runs.insert(run_id);
            tx.loaded.run_parts.insert(run_id, parts);
        }
    }

    // Jobs: route each row into its queue collection and rebuild run.jobs.
    // `seq` is the write-order counter that preserves FIFO within a kind.
    // A job row loads when its run is in scope OR it sits in a global queue
    // kind the scope asked for (ready / blocked) — those are cross-run.
    {
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
                clauses.push(format!("j.run_id IN ({ph})"));
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
                clauses.push(format!("j.queue_kind IN ({ph})"));
                params.extend(kinds.iter().map(|k| k.to_string()));
            }
        }
        let where_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" OR "))
        };

        // `needs:` edges for exactly the jobs this scope loads, in order.
        let mut needs: std::collections::HashMap<(String, String), Vec<String>> =
            std::collections::HashMap::new();
        {
            let sql = format!(
                "SELECT n.run_id, n.job_id, n.needs_job_id FROM job_needs n \
                 JOIN jobs j ON j.run_id=n.run_id AND j.job_id=n.job_id{where_sql} \
                 ORDER BY n.run_id, n.job_id, n.position"
            );
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
                let (run, job, need) = row.map_err(ControlError::backend)?;
                needs.entry((run, job)).or_default().push(need);
            }
        }

        let sql = format!(
            "SELECT j.run_id, j.job_id, j.status, j.queue_kind, j.queue_position, j.seq, \
             j.reaper_first_seen_us, j.expand_generation, j.enqueued_at_us, \
             j.priority, j.run_order, j.job_order, j.not_before_us, j.namespace_id, j.pool_key, \
             j.base_id, j.runs_on, j.runner_group, \
             j.created_at_ns, j.deps_ready_at_ns, j.concurrency_wait_at_ns, \
             j.concurrency_acquired_at_ns, j.if_condition, j.max_parallel, \
             j.environment_json, j.concurrency_json, j.matrix_json, j.deferred_matrix, \
             j.reusable_call_json, m.message_blob, m.condition_context_blob \
             FROM jobs j LEFT JOIN job_messages m ON m.run_id=j.run_id AND m.job_id=j.job_id\
             {where_sql} \
             ORDER BY CASE WHEN j.queue_kind='ready' THEN 0 ELSE 1 END, \
                      CASE WHEN j.queue_kind='ready' THEN -j.priority ELSE 0 END, \
                      CASE WHEN j.queue_kind='ready' THEN j.run_order ELSE 0 END, \
                      CASE WHEN j.queue_kind='ready' THEN j.job_order ELSE 0 END, \
                      j.seq, j.run_id, j.job_id"
        );
        let mut stmt = conn.prepare(&sql).map_err(ControlError::backend)?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(params.iter()))
            .map_err(ControlError::backend)?;
        while let Some(r) = rows.next().map_err(ControlError::backend)? {
            let run_id_s: String = r.get(0).map_err(ControlError::backend)?;
            let job_id_s: String = r.get(1).map_err(ControlError::backend)?;
            let status: String = r.get(2).map_err(ControlError::backend)?;
            let kind: String = r.get(3).map_err(ControlError::backend)?;
            let pos: Option<i64> = r.get(4).map_err(ControlError::backend)?;
            let job_seq: Option<i64> = r.get(5).map_err(ControlError::backend)?;
            let reaper_us: Option<i64> = r.get(6).map_err(ControlError::backend)?;
            let expand_generation: i64 = r.get(7).map_err(ControlError::backend)?;
            let enqueued_us: Option<i64> = r.get(8).map_err(ControlError::backend)?;
            let priority: Option<i16> = r.get(9).map_err(ControlError::backend)?;
            let run_order: Option<i64> = r.get(10).map_err(ControlError::backend)?;
            let job_order: Option<i64> = r.get(11).map_err(ControlError::backend)?;
            let not_before_us: Option<i64> = r.get(12).map_err(ControlError::backend)?;
            let namespace_id: String = r.get(13).map_err(ControlError::backend)?;
            let pool_key: String = r.get(14).map_err(ControlError::backend)?;
            let run_id = parse_run_id(&run_id_s);
            let job_id = JobId(job_id_s.clone());
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
                    namespace_id,
                    pool_key,
                    row_sig: None,
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
            // A job is dispatchable only with its sealed message; a row
            // without one (terminal/placeholder) routes nowhere.
            let message: Option<Vec<u8>> = r.get(29).map_err(ControlError::backend)?;
            let context: Option<Vec<u8>> = r.get(30).map_err(ControlError::backend)?;
            let (Some(message), Some(context)) = (message, context) else {
                continue;
            };
            let payload = super::rows::JobPayloadRow {
                created_at_ns: r
                    .get::<_, Option<i64>>(18)
                    .map_err(ControlError::backend)?
                    .unwrap_or(0),
                deps_ready_at_ns: r.get(19).map_err(ControlError::backend)?,
                concurrency_wait_at_ns: r.get(20).map_err(ControlError::backend)?,
                concurrency_acquired_at_ns: r.get(21).map_err(ControlError::backend)?,
                if_condition: r.get(22).map_err(ControlError::backend)?,
                max_parallel: r.get(23).map_err(ControlError::backend)?,
                environment_json: r.get(24).map_err(ControlError::backend)?,
                concurrency_json: r.get(25).map_err(ControlError::backend)?,
                matrix_json: r.get(26).map_err(ControlError::backend)?,
                deferred_matrix: r.get(27).map_err(ControlError::backend)?,
                reusable_call_json: r.get(28).map_err(ControlError::backend)?,
                needs: needs.remove(&(run_id_s, job_id_s)).unwrap_or_default(),
            };
            let runs_on: String = r.get(16).map_err(ControlError::backend)?;
            let job = payload
                .into_job(
                    run_id,
                    job_id.clone(),
                    r.get(15).map_err(ControlError::backend)?,
                    &runs_on,
                    r.get(17).map_err(ControlError::backend)?,
                    enqueued_us,
                    blob(cipher, &message)?,
                    blob(cipher, &context)?,
                )
                .map_err(ControlError::backend)?;
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
    // Global queue-front labels — unscoped, for pool next-image selection.
    // Read straight from the `runs_on` column; no payload decode.
    tx.next_queue_labels = conn
        .query_row(
            "SELECT runs_on FROM jobs WHERE queue_kind='ready' \
             ORDER BY priority DESC, run_order, job_order, seq, run_id, job_id LIMIT 1",
            [],
            |r| r.get::<_, String>(0),
        )
        .ok()
        .and_then(|labels| serde_json::from_str(&labels).ok())
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
        let (filter, params) = match scope.runs.as_ref() {
            None => (String::new(), Vec::new()),
            Some(set) if set.is_empty() => (" WHERE 1=0".to_owned(), Vec::new()),
            Some(set) => {
                let ph = set.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                (
                    format!(
                        " WHERE agent_job_id IN (SELECT agent_job_id FROM job_requests \
                         WHERE run_id IN ({ph}))"
                    ),
                    set.iter().map(|id| id.0.to_string()).collect(),
                )
            }
        };
        let sql =
            format!("SELECT {STEP_COLUMNS} FROM job_steps{filter} ORDER BY agent_job_id, position");
        for (agent_job_id, record) in read_step_rows(conn, &sql, &params)? {
            // In scope when the request carrying this agent_job_id loaded.
            if !tx.agent_job_requests.contains_key(&agent_job_id) {
                continue;
            }
            tx.job_steps.entry(agent_job_id).or_default().push(record);
        }
        tx.loaded.steps = tx.job_steps.clone();
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
    // waiters), loaded fully under any scope that asks for it. Gates load
    // from queryable hold/waiter rows; `holder_keys` is derived, never read.
    if scope.concurrency {
        let mut stmt = conn
            .prepare(
                "SELECT repo, group_name, display_name, holder_kind, holder_run_id, \
                 holder_job_id, holder_job_ids FROM concurrency_holds",
            )
            .map_err(ControlError::backend)?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, Option<String>>(5)?,
                    r.get::<_, String>(6)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (repo, group_name, display_name, kind, run_id, job_id, job_ids) =
                row.map_err(ControlError::backend)?;
            let running =
                crate::concurrency::holder_from_row(&kind, &run_id, job_id.as_deref(), &job_ids);
            let key = (repo.clone(), group_name.clone());
            let group = tx.concurrency_groups.entry(key.clone()).or_insert_with(|| {
                concurrency::ConcurrencyGroup {
                    display_name: display_name.clone(),
                    ..Default::default()
                }
            });
            if group.display_name.is_empty() {
                group.display_name = display_name;
            }
            // Self-healing: an undecodable hold marks the group loaded so
            // write-back drops the bad row instead of preserving it.
            if let Some(holder) = running {
                group.running = Some(holder.clone());
                tx.holder_keys
                    .entry(holder.run_id())
                    .or_default()
                    .push(key.clone());
            }
            tx.loaded.groups.insert(key);
        }
        {
            let mut stmt = conn
                .prepare(
                    "SELECT repo, group_name, holder_kind, holder_run_id, holder_job_id, \
                     holder_job_ids FROM concurrency_waits ORDER BY position",
                )
                .map_err(ControlError::backend)?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, Option<String>>(4)?,
                        r.get::<_, String>(5)?,
                    ))
                })
                .map_err(ControlError::backend)?;
            for row in rows {
                let (repo, group_name, kind, run_id, job_id, job_ids) =
                    row.map_err(ControlError::backend)?;
                let key = (repo.clone(), group_name.clone());
                if let Some(holder) =
                    crate::concurrency::holder_from_row(&kind, &run_id, job_id.as_deref(), &job_ids)
                {
                    tx.holder_keys
                        .entry(holder.run_id())
                        .or_default()
                        .push(key.clone());
                    tx.concurrency_groups
                        .entry(key.clone())
                        .or_default()
                        .pending
                        .push_back(holder);
                }
                tx.loaded.groups.insert(key);
            }
        }
        {
            let mut stmt = conn
                .prepare(
                    "SELECT run_id, job_ids, gate_repo, gate_group, display_name, \
                     cancel_in_progress, queue_mode, acquired FROM jobset_gates \
                     ORDER BY gate_index",
                )
                .map_err(ControlError::backend)?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, i64>(5)?,
                        r.get::<_, String>(6)?,
                        r.get::<_, i64>(7)?,
                    ))
                })
                .map_err(ControlError::backend)?;
            for row in rows {
                let (run_id_s, job_ids_s, repo, group, display_name, cancel, mode, acquired) =
                    row.map_err(ControlError::backend)?;
                let run_id = parse_run_id(&run_id_s);
                let job_ids: BTreeSet<JobId> = serde_json::from_str::<BTreeSet<String>>(&job_ids_s)
                    .unwrap_or_default()
                    .into_iter()
                    .map(JobId)
                    .collect();
                let id = JobSetId { run_id, job_ids };
                let admission = tx.jobset_admissions.entry(id.clone()).or_insert_with(|| {
                    crate::state::JobSetAdmission {
                        gates: Vec::new(),
                        acquired_keys: BTreeSet::new(),
                    }
                });
                admission.gates.push(crate::state::JobSetGate {
                    key: (repo.clone(), group.clone()),
                    display_name,
                    cancel_in_progress: cancel != 0,
                    queue: crate::concurrency::queue_mode_from_row(&mode),
                });
                if acquired != 0 {
                    admission.acquired_keys.insert((repo, group));
                }
                tx.loaded.jobsets.insert(id);
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
        // Snapshot the loaded gate state so write-back can skip unchanged
        // groups/jobsets (their rows stay byte-identical, incl. `held_at_us`).
        tx.loaded.group_snapshot = tx.concurrency_groups.clone();
        tx.loaded.jobset_snapshot = tx.jobset_admissions.clone();
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
            tx.loaded
                .workflow_run_counters
                .insert(key.clone(), value as u64);
            tx.workflow_run_counters.insert(key, value as u64);
        }
    }
    if scope.include_archived {
        load_archived_txstate(conn, &mut tx)?;
    }
    super::txstate::snapshot_row_sigs(&mut tx);
    Ok((tx, effective_scope))
}

/// Hydrate one read-only historical scope without making archived rows part
/// of a command's mutable working set.
fn load_archived_txstate(conn: &Connection, tx: &mut TxState) -> Result<(), ControlError> {
    if tx.runs.is_empty() {
        return Ok(());
    }
    let ids: Vec<String> = tx.runs.keys().map(|id| id.to_string()).collect();
    let placeholders = vec!["?"; ids.len()].join(",");
    {
        let sql = format!(
            "SELECT h.run_id,h.job_id,h.status FROM job_history h
             JOIN runs r ON r.run_id=h.run_id
             WHERE r.archived_at_us IS NOT NULL AND h.run_attempt=r.run_attempt
               AND h.run_created_at_us=r.created_at_us AND h.run_id IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&sql).map_err(ControlError::backend)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(ids.iter()), |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (run, job, status) = row.map_err(ControlError::backend)?;
            if let Some(record) = tx.runs.get_mut(&parse_run_id(&run)) {
                record.jobs.insert(JobId(job), status_parse(&status));
            }
        }
    }
    {
        let sql = format!(
            "SELECT h.request_id,h.run_id,h.job_id,h.agent_job_id,h.plan_id,h.timeline_id,
                    h.result,h.owner_runner_id,h.claimed_at_us,h.started_at_us
             FROM attempt_history h JOIN runs r ON r.run_id=h.run_id
             WHERE r.archived_at_us IS NOT NULL AND h.run_attempt=r.run_attempt
               AND h.run_created_at_us=r.created_at_us AND h.run_id IN ({placeholders})
             ORDER BY h.request_id"
        );
        let mut stmt = conn.prepare(&sql).map_err(ControlError::backend)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(ids.iter()), |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, Option<String>>(6)?,
                    r.get::<_, Option<i64>>(7)?,
                    r.get::<_, Option<i64>>(8)?,
                    r.get::<_, Option<i64>>(9)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (
                request_id,
                run_id,
                job_id,
                agent_id,
                plan_id,
                timeline_id,
                result,
                owner_runner_id,
                claimed_at,
                started_at,
            ) = row.map_err(ControlError::backend)?;
            let agent_job_id = parse_uuid(&agent_id);
            tx.insert_request(TaskAgentJobRequestRecord {
                request_id,
                run_id: parse_run_id(&run_id),
                job_id: JobId(job_id),
                agent_job_id,
                plan_id,
                plan_type: String::new(),
                timeline_id: parse_uuid(&timeline_id),
                result: result.as_deref().map(status_parse),
                locked_until: String::new(),
                claimed_at: claimed_at.map(us_to_system),
                owner_runner_id,
                started_at: started_at.map(us_to_system),
                last_renewed_at: None,
                timeout_triggered: false,
                debug_token_issued: false,
            });
        }
    }
    {
        let sql = format!(
            "SELECT {STEP_COLUMNS} FROM step_history
             WHERE (run_id, run_attempt, run_created_at_us) IN (
                 SELECT run_id, run_attempt, created_at_us FROM runs
                 WHERE archived_at_us IS NOT NULL AND run_id IN ({placeholders}))
             ORDER BY agent_job_id, position"
        );
        for (agent_job_id, record) in read_step_rows(conn, &sql, &ids)? {
            tx.job_steps.entry(agent_job_id).or_default().push(record);
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// Write-back: TxState delta → rows
// ─────────────────────────────────────────────────────────────────────────

/// Reserve a FIFO value through one atomic counter update, not MAX()+1 over
/// the job table. The counter is seeded from legacy positions by migration.
fn alloc_job_counter(conn: &Connection, name: &str) -> Result<i64, ControlError> {
    conn.query_row(
        "INSERT INTO counters(name, value) VALUES (?1, 2) \
         ON CONFLICT(name) DO UPDATE SET value=counters.value+1 \
         RETURNING value-1",
        params![name],
        |row| row.get(0),
    )
    .map_err(ControlError::backend)
}

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

    // Runs: write only the rows that differ from the loaded snapshot.
    for (run_id, run) in &tx.runs {
        let parts = super::rows::RunParts::from_record(run);
        write_run(
            conn,
            cipher,
            run_id,
            tx.run_namespaces
                .get(run_id)
                .map(String::as_str)
                .unwrap_or(DEFAULT_NAMESPACE),
            tx.loaded.run_parts.get(run_id),
            &parts,
        )?;
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
            let s = alloc_job_counter(conn, "next_job_seq")?;
            let p = if kind == QueueKind::Ready {
                Some(alloc_job_counter(conn, "next_queue_position")?)
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
            None => preserved.map(|p| p.pool_key.clone()).unwrap_or_default(),
        };
        let run_order = preserved.map(|p| p.run_order).unwrap_or_else(|| {
            tx.runs
                .get(&run_id)
                .map(|r| r.created_at.timestamp_micros())
                .unwrap_or(0)
        });
        let job_order = if kind == QueueKind::Ready && !same_slot {
            position.unwrap_or(0)
        } else {
            preserved.map(|p| p.job_order).unwrap_or(0)
        };
        let priority = preserved.map(|p| p.priority).unwrap_or(0);
        let not_before_us = preserved.and_then(|p| p.not_before_us);
        let (base_id, runs_on, runner_group, enqueued_us) = match job {
            Some(j) => (
                j.base_id.clone(),
                serde_json::to_string(&j.runs_on).unwrap_or_default(),
                j.runner_group.clone(),
                Some(j.enqueued_at_unix_nanos / 1000),
            ),
            None => (String::new(), "[]".to_owned(), None, None),
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
             claimed_by, claimed_at_us, expand_generation, \
             namespace_id, pool_key, priority, run_order, job_order, not_before_us) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20) \
             ON CONFLICT(run_id,job_id) DO UPDATE SET status=excluded.status, \
             queue_kind=excluded.queue_kind, queue_position=excluded.queue_position, \
             seq=excluded.seq, enqueued_at_us=excluded.enqueued_at_us, \
             reaper_first_seen_us=excluded.reaper_first_seen_us, \
             claimed_by=excluded.claimed_by, claimed_at_us=excluded.claimed_at_us, \
             expand_generation=excluded.expand_generation, \
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
                tx.run_namespaces
                    .get(&run_id)
                    .map(String::as_str)
                    .or_else(|| preserved.map(|p| p.namespace_id.as_str()))
                    .unwrap_or(DEFAULT_NAMESPACE),
                pool_key,
                priority,
                run_order,
                job_order,
                not_before_us,
            ],
        )
        .map_err(ControlError::backend)?;
        // Promotion/requeue stamps dependency and enqueue times in the
        // payload. Persist it on any slot change; a same-slot write leaves the
        // payload columns and sealed message untouched.
        if let Some(j) = job.filter(|_| !same_slot) {
            write_job_payload(conn, cipher, &run_id, job_id, j)?;
        }
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
        if tx.loaded.request_sigs.get(request_id)
            == super::txstate::request_sig(tx, *request_id).as_ref()
        {
            continue;
        }
        let request_blob = tx
            .broker_messages
            .get(request_id)
            .map(|m| unblob(cipher, m))
            .transpose()?;
        conn.execute(
            "INSERT INTO job_requests (request_id, run_id, job_id, agent_job_id, plan_id, \
             plan_type, timeline_id, result, locked_until, claimed_at_us, owner_runner_id, \
             started_at_us, last_renewed_at_us, timeout_triggered, debug_token_issued, \
             request_blob, job_timeout_s) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17) \
             ON CONFLICT(request_id) DO UPDATE SET result=excluded.result, \
             locked_until=excluded.locked_until, claimed_at_us=excluded.claimed_at_us, \
             owner_runner_id=excluded.owner_runner_id, started_at_us=excluded.started_at_us, \
             last_renewed_at_us=excluded.last_renewed_at_us, \
             timeout_triggered=excluded.timeout_triggered, \
             debug_token_issued=excluded.debug_token_issued, \
             request_blob=excluded.request_blob, job_timeout_s=excluded.job_timeout_s",
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
                tx.broker_messages
                    .get(request_id)
                    .and_then(|message| message.job_timeout),
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
    // Steps: per-attempt row delta against the loaded manifest, so a step
    // transition is a one-row upsert. Steps only exist under a live request
    // (FK); an attempt whose request this command removed is cascaded away
    // with it and must not be re-inserted.
    let live_attempts: std::collections::HashSet<uuid::Uuid> =
        tx.job_requests.values().map(|r| r.agent_job_id).collect();
    for (agent_job_id, steps) in &tx.job_steps {
        if !live_attempts.contains(agent_job_id) {
            continue;
        }
        let delta =
            super::rows::step_delta(tx.loaded.steps.get(agent_job_id).map(Vec::as_slice), steps);
        write_step_delta(conn, agent_job_id, &delta)?;
    }
    for agent_job_id in tx.loaded.steps.keys() {
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
        if tx.loaded.runner_sigs.get(runner_id)
            == super::txstate::runner_sig(tx, *runner_id).as_ref()
        {
            continue;
        }
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
    // Each group/jobset writes its own rows; groups present at load but gone
    // now are deleted. Untouched groups are never rewritten. `holder_keys`
    // is derived and no longer persisted.
    if scope.concurrency {
        // Skip groups whose loaded snapshot equals the working value —
        // write-back only touches gates this command actually changed.
        for ((repo, group_name), group) in &tx.concurrency_groups {
            if tx
                .loaded
                .group_snapshot
                .get(&(repo.clone(), group_name.clone()))
                == Some(group)
            {
                continue;
            }
            if let Some(holder) = &group.running {
                let (kind, run_id, job_id, job_ids) = crate::concurrency::holder_row(holder);
                conn.execute(
                    "INSERT INTO concurrency_holds (repo, group_name, display_name, holder_kind, \
                     holder_run_id, holder_job_id, holder_job_ids, held_at_us) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8) \
                     ON CONFLICT(repo,group_name) DO UPDATE SET display_name=excluded.display_name, \
                     holder_kind=excluded.holder_kind, holder_run_id=excluded.holder_run_id, \
                     holder_job_id=excluded.holder_job_id, holder_job_ids=excluded.holder_job_ids, \
                     held_at_us=excluded.held_at_us",
                    params![
                        repo, group_name, group.display_name, kind, run_id, job_id, job_ids, now_us
                    ],
                )
                .map_err(ControlError::backend)?;
            } else {
                conn.execute(
                    "DELETE FROM concurrency_holds WHERE repo=?1 AND group_name=?2",
                    params![repo, group_name],
                )
                .map_err(ControlError::backend)?;
            }
            conn.execute(
                "DELETE FROM concurrency_waits WHERE repo=?1 AND group_name=?2",
                params![repo, group_name],
            )
            .map_err(ControlError::backend)?;
            for (position, holder) in group.pending.iter().enumerate() {
                let (kind, run_id, job_id, job_ids) = crate::concurrency::holder_row(holder);
                conn.execute(
                    "INSERT INTO concurrency_waits (repo, group_name, position, holder_kind, \
                     holder_run_id, holder_job_id, holder_job_ids, queued_at_us) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                    params![
                        repo,
                        group_name,
                        position as i64,
                        kind,
                        run_id,
                        job_id,
                        job_ids,
                        now_us
                    ],
                )
                .map_err(ControlError::backend)?;
            }
        }
        for (repo, group_name) in &tx.loaded.groups {
            if !tx
                .concurrency_groups
                .contains_key(&(repo.clone(), group_name.clone()))
            {
                conn.execute(
                    "DELETE FROM concurrency_holds WHERE repo=?1 AND group_name=?2",
                    params![repo, group_name],
                )
                .map_err(ControlError::backend)?;
                conn.execute(
                    "DELETE FROM concurrency_waits WHERE repo=?1 AND group_name=?2",
                    params![repo, group_name],
                )
                .map_err(ControlError::backend)?;
            }
        }
        for (id, admission) in &tx.jobset_admissions {
            if tx.loaded.jobset_snapshot.get(id) == Some(admission) {
                continue;
            }
            let job_ids: Vec<String> = id.job_ids.iter().map(|j| j.0.clone()).collect();
            let job_ids_s = serde_json::to_string(&job_ids).unwrap_or_default();
            conn.execute(
                "DELETE FROM jobset_gates WHERE run_id=?1 AND job_ids=?2",
                params![id.run_id.0.to_string(), job_ids_s],
            )
            .map_err(ControlError::backend)?;
            for (index, gate) in admission.gates.iter().enumerate() {
                conn.execute(
                    "INSERT INTO jobset_gates (run_id, job_ids, gate_index, gate_repo, gate_group, \
                     display_name, cancel_in_progress, queue_mode, acquired) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                    params![
                        id.run_id.0.to_string(),
                        job_ids_s,
                        index as i64,
                        gate.key.0,
                        gate.key.1,
                        gate.display_name,
                        i64::from(gate.cancel_in_progress),
                        crate::concurrency::queue_mode_row(&gate.queue),
                        i64::from(admission.acquired_keys.contains(&gate.key)),
                    ],
                )
                .map_err(ControlError::backend)?;
            }
        }
        for id in &tx.loaded.jobsets {
            if !tx.jobset_admissions.contains_key(id) {
                let job_ids: Vec<String> = id.job_ids.iter().map(|j| j.0.clone()).collect();
                conn.execute(
                    "DELETE FROM jobset_gates WHERE run_id=?1 AND job_ids=?2",
                    params![
                        id.run_id.0.to_string(),
                        serde_json::to_string(&job_ids).unwrap_or_default()
                    ],
                )
                .map_err(ControlError::backend)?;
            }
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

    // Counters: write only what this transaction advanced, and never lower
    // a stored value. A transaction that merely loaded a counter must not
    // write its stale copy back over a concurrent allocation.
    for (name, value) in super::txstate::advanced_counters(tx) {
        conn.execute(
            "INSERT INTO counters (name, value) VALUES (?1,?2) \
             ON CONFLICT(name) DO UPDATE SET value=MAX(counters.value, excluded.value)",
            params![name, value],
        )
        .map_err(ControlError::backend)?;
    }
    for (key, value) in super::txstate::advanced_run_counters(tx) {
        conn.execute(
            "INSERT INTO workflow_run_counters (key, value) VALUES (?1,?2) \
             ON CONFLICT(key) DO UPDATE SET \
             value=MAX(workflow_run_counters.value, excluded.value)",
            params![key, value],
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
            include_archived: false,
            runs: Some(run_ids),
            // `ready_queue`/`blocked_jobs` stay unloaded: `submit_run_tx`
            // only promotes this run's own pending jobs (foreign runs'
            // `needs:` never cross runs), `queue_depth` reads the O(1)
            // `ready_count`, and `next_runs_on` falls back to
            // `next_queue_labels`. Loading the global queues would parse
            // every queued job payload per submit — O(#queued) blobs.
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
        // every `record_blob`/job payload per submission for one counter.
        let scope = TxScope {
            include_archived: false,
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
            include_archived: false,
            runs: Some(BTreeSet::new()),
            ready_queue: true,
            // `poll` claims from the ready queue only — it never promotes or
            // releases blocked jobs (that's `complete_job`/`submit`). Loading
            // every blocked job payload is O(#blocked) for no benefit.
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
        // Read-only point lookup on the reader pool — one joined statement
        // instead of loading the run's working set.
        self.acquire_impl(request_id, None)
    }

    async fn acquire_for_runner(
        &self,
        request_id: i64,
        runner_id: i64,
    ) -> Result<AcquireContext, ControlError> {
        self.acquire_impl(request_id, Some(runner_id))
    }

    async fn store_request_message(
        &self,
        run_id: RunId,
        request_id: i64,
        message: Option<&preloop_gha_protocol::azdo::AgentJobRequestMessage>,
        token_request: Option<&crate::models::GitHubTokenRequest>,
    ) -> Result<(), ControlError> {
        let _ = run_id; // SQLite serializes on the single writer.
        let message_blob = message.map(|m| unblob(&self.cipher, m)).transpose()?;
        let job_timeout = message.and_then(|m| m.job_timeout);
        let token_blob = token_request.map(|t| unblob(&self.cipher, t)).transpose()?;
        run_blocking(move || {
            let mut conn = self.conn.lock();
            let tx = conn.transaction().map_err(ControlError::backend)?;
            if let Some(message_blob) = message_blob {
                tx.execute(
                    "UPDATE job_requests SET request_blob = ?1, \
                     job_timeout_s = COALESCE(?2, job_timeout_s) \
                     WHERE request_id = ?3",
                    params![message_blob, job_timeout, request_id],
                )
                .map_err(ControlError::backend)?;
            }
            if let Some(blob) = token_blob {
                tx.execute(
                    "INSERT INTO github_token_requests (request_id, request_blob) VALUES (?1,?2) \
                     ON CONFLICT(request_id) DO UPDATE SET request_blob = excluded.request_blob",
                    params![request_id, blob],
                )
                .map_err(ControlError::backend)?;
            }
            tx.commit().map_err(ControlError::backend)?;
            Ok(())
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
            include_archived: false,
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
            include_archived: false,
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
            include_archived: false,
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
            include_archived: false,
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
            include_archived: false,
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
        self.read_scoped(&TxScope::run(run_id).with_history(), |tx| {
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

    async fn archive_finished_runs(&self, limit: usize) -> Result<usize, ControlError> {
        run_blocking(|| {
            let mut conn = self.conn.lock();
            let tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(ControlError::backend)?;
            // Let late Results/runner callbacks settle before discarding
            // runner-facing request data. A lost wakeup is repaired by poll.
            let cutoff = system_to_us(std::time::SystemTime::now()) - 60_000_000;
            let run_ids: Vec<String> = {
                let mut stmt = tx
                    .prepare(
                        "SELECT r.run_id FROM runs r
                     WHERE r.archived_at_us IS NULL
                       AND r.completed_at_us IS NOT NULL AND r.completed_at_us <= ?1
                       AND r.status IN ('success','failure','skipped','cancelled')
                       AND NOT EXISTS (SELECT 1 FROM job_requests q
                                       WHERE q.run_id=r.run_id AND q.result IS NULL)
                       AND NOT EXISTS (SELECT 1 FROM job_requests q
                                       JOIN runner_sessions s ON s.active_request_id=q.request_id
                                       WHERE q.run_id=r.run_id)
                       AND NOT EXISTS (SELECT 1 FROM cancellation_queue c
                                       WHERE c.run_id=r.run_id)
                     ORDER BY r.completed_at_us, r.run_id LIMIT ?2",
                    )
                    .map_err(ControlError::backend)?;
                let ids = stmt
                    .query_map(params![cutoff, limit.min(64) as i64], |row| row.get(0))
                    .map_err(ControlError::backend)?
                    .collect::<Result<Vec<String>, _>>()
                    .map_err(ControlError::backend)?;
                ids
            };
            let now = system_to_us(std::time::SystemTime::now());
            for run_id in &run_ids {
                tx.execute(
                    "INSERT INTO job_history(namespace_id,run_id,run_attempt,run_created_at_us,
                        job_id,status,base_id,pool_key,priority,run_order,job_order)
                     SELECT r.namespace,r.run_id,r.run_attempt,r.created_at_us,
                            j.job_id,j.status,j.base_id,j.pool_key,j.priority,j.run_order,j.job_order
                     FROM jobs j JOIN runs r ON r.run_id=j.run_id WHERE r.run_id=?1",
                    params![run_id],
                ).map_err(ControlError::backend)?;
                tx.execute(
                    "INSERT INTO attempt_history(namespace_id,run_id,run_attempt,run_created_at_us,
                        request_id,job_id,agent_job_id,plan_id,timeline_id,result,owner_runner_id,
                        claimed_at_us,started_at_us)
                     SELECT r.namespace,r.run_id,r.run_attempt,r.created_at_us,
                            q.request_id,q.job_id,q.agent_job_id,q.plan_id,q.timeline_id,q.result,
                            q.owner_runner_id,q.claimed_at_us,q.started_at_us
                     FROM job_requests q JOIN runs r ON r.run_id=q.run_id
                     WHERE r.run_id=?1",
                    params![run_id],
                )
                .map_err(ControlError::backend)?;
                tx.execute(
                    "INSERT INTO step_history(namespace_id,run_id,run_attempt,run_created_at_us,
                        agent_job_id,step_id,position,kind,workflow_index,runner_number,
                        context_name,name,conclusion,started_at_us,finished_at_us)
                     SELECT r.namespace,r.run_id,r.run_attempt,r.created_at_us,
                            s.agent_job_id,s.step_id,s.position,s.kind,s.workflow_index,
                            s.runner_number,s.context_name,s.name,s.conclusion,
                            s.started_at_us,s.finished_at_us
                     FROM job_steps s JOIN job_requests q ON q.agent_job_id=s.agent_job_id
                     JOIN runs r ON r.run_id=q.run_id
                     WHERE r.run_id=?1",
                    params![run_id],
                )
                .map_err(ControlError::backend)?;
                // `job_steps` rows cascade away with their `job_requests`
                // rows below.
                for table in [
                    "job_requests",
                    "id_token_grants",
                    "oidc_job_contexts",
                    "job_assignments",
                    "pool_pending",
                    "cancellation_queue",
                ] {
                    tx.execute(
                        &format!("DELETE FROM {table} WHERE run_id=?1"),
                        params![run_id],
                    )
                    .map_err(ControlError::backend)?;
                }
                tx.execute("DELETE FROM jobs WHERE run_id=?1", params![run_id])
                    .map_err(ControlError::backend)?;
                tx.execute(
                    "UPDATE runs SET archived_at_us=?2 WHERE run_id=?1",
                    params![run_id, now],
                )
                .map_err(ControlError::backend)?;
            }
            tx.commit().map_err(ControlError::backend)?;
            Ok(run_ids.len())
        })
    }

    async fn set_push_state(
        &self,
        run_id: RunId,
        state: crate::models::PushState,
    ) -> Result<(), ControlError> {
        let state_json = serde_json::to_string(&state).map_err(ControlError::backend)?;
        run_blocking(|| {
            self.conn
                .lock()
                .execute(
                    "UPDATE runs SET push_state_json = ?2 WHERE run_id = ?1",
                    params![run_id.to_string(), state_json],
                )
                .map_err(ControlError::backend)?;
            Ok(())
        })
    }

    async fn request(&self, key: RequestKey) -> Result<TaskAgentJobRequestRecord, ControlError> {
        const COLUMNS: &str = "SELECT request_id, run_id, job_id, agent_job_id, plan_id,
            plan_type, timeline_id, result, locked_until, claimed_at_us, owner_runner_id,
            started_at_us, last_renewed_at_us, timeout_triggered, debug_token_issued
            FROM job_requests";
        self.with_reader(|conn| {
            let (sql, params_vec): (String, Vec<Box<dyn rusqlite::ToSql>>) = match &key {
                RequestKey::Id(id) => (
                    format!("{COLUMNS} WHERE request_id = ?1"),
                    vec![Box::new(*id)],
                ),
                RequestKey::PlanId(plan) => (
                    format!("{COLUMNS} WHERE plan_id = ?1 ORDER BY request_id DESC LIMIT 1"),
                    vec![Box::new(plan.clone())],
                ),
                RequestKey::AgentJobId(id) => (
                    format!("{COLUMNS} WHERE agent_job_id = ?1"),
                    vec![Box::new(id.to_string())],
                ),
                RequestKey::TimelineId(id) => (
                    format!("{COLUMNS} WHERE timeline_id = ?1 ORDER BY request_id DESC LIMIT 1"),
                    vec![Box::new(id.to_string())],
                ),
                RequestKey::Job(run_id, job_id) => (
                    format!(
                        "{COLUMNS} WHERE run_id = ?1 AND job_id = ?2 \
                         ORDER BY request_id DESC LIMIT 1"
                    ),
                    vec![Box::new(run_id.0.to_string()), Box::new(job_id.0.clone())],
                ),
            };
            let refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|p| p.as_ref()).collect();
            conn.query_row(&sql, rusqlite::params_from_iter(refs), |row| {
                let result: Option<String> = row.get(7)?;
                Ok(TaskAgentJobRequestRecord {
                    request_id: row.get(0)?,
                    run_id: parse_run_id(&row.get::<_, String>(1)?),
                    job_id: JobId(row.get(2)?),
                    agent_job_id: parse_uuid(&row.get::<_, String>(3)?),
                    plan_id: row.get(4)?,
                    plan_type: row.get(5)?,
                    timeline_id: parse_uuid(&row.get::<_, String>(6)?),
                    result: result.as_deref().map(status_parse),
                    locked_until: row.get(8)?,
                    claimed_at: row.get::<_, Option<i64>>(9)?.map(us_to_system),
                    owner_runner_id: row.get(10)?,
                    started_at: row.get::<_, Option<i64>>(11)?.map(us_to_system),
                    last_renewed_at: row.get::<_, Option<i64>>(12)?.map(us_to_system),
                    timeout_triggered: row.get::<_, i64>(13)? != 0,
                    debug_token_issued: row.get::<_, i64>(14)? != 0,
                })
            })
            .optional()
            .map_err(ControlError::backend)?
            .ok_or_else(|| ControlError::NotFound("request".to_owned()))
        })
    }
    async fn artifact_scopes(
        &self,
        plan_ids: &[String],
    ) -> Result<BTreeMap<String, RunId>, ControlError> {
        if plan_ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let placeholders = std::iter::repeat_n("?", plan_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT plan_id, run_id FROM job_requests \
             WHERE plan_id IN ({placeholders}) ORDER BY request_id DESC"
        );
        let plan_ids = plan_ids.to_vec();
        self.with_reader(move |conn| {
            let mut stmt = conn.prepare(&sql).map_err(ControlError::backend)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(plan_ids.iter()), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(ControlError::backend)?;
            let mut scopes = BTreeMap::new();
            for row in rows {
                let (plan_id, run_id) = row.map_err(ControlError::backend)?;
                // Ordered by request_id DESC: the first row per plan id is the
                // latest attempt.
                scopes
                    .entry(plan_id)
                    .or_insert_with(|| parse_run_id(&run_id));
            }
            Ok(scopes)
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
        crate::store::Store::store_meta_only(self.auxiliary()?, meta)
            .await
            .map_err(ControlError::backend)
    }

    async fn load_meta(&self) -> Result<Option<crate::store::MetaSnapshot>, ControlError> {
        crate::store::Store::load_meta_only(self.auxiliary()?)
            .await
            .map_err(ControlError::backend)
    }

    async fn ensure_key_fingerprint(&self, fingerprint: &str) -> Result<(), ControlError> {
        run_blocking(|| {
            let conn = self.conn.lock();
            conn.execute(
                "INSERT INTO control_key_fingerprint(id, fingerprint) VALUES (1, ?1) \
                 ON CONFLICT(id) DO NOTHING",
                params![fingerprint.as_bytes()],
            )
            .map_err(ControlError::backend)?;
            let stored: Vec<u8> = conn
                .query_row(
                    "SELECT fingerprint FROM control_key_fingerprint WHERE id = 1",
                    [],
                    |row| row.get(0),
                )
                .map_err(ControlError::backend)?;
            super::types::check_key_fingerprint(&stored, fingerprint)
        })
    }

    async fn touch_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionProtocol>, ControlError> {
        run_blocking(|| {
            self.conn
                .lock()
                .query_row(
                    "UPDATE runner_sessions SET last_seen_at_us = ?1 WHERE session_id = ?2 \
                     RETURNING protocol",
                    params![system_to_us(std::time::SystemTime::now()), session_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(ControlError::backend)
                .map(|protocol| protocol.map(|p| SessionProtocol::parse(&p)))
        })
    }

    async fn session_owner(
        &self,
        session_id: &str,
    ) -> Result<Option<(i64, crate::models::RunnerCapabilities)>, ControlError> {
        self.with_reader(|conn| {
            conn.query_row(
                "SELECT s.runner_id, r.labels, r.runner_group_id, r.runner_group_name, \
                 r.runner_id IS NOT NULL \
                 FROM runner_sessions s LEFT JOIN runners r ON r.runner_id = s.runner_id \
                 WHERE s.session_id = ?1 AND s.runner_id IS NOT NULL",
                params![session_id],
                |row| {
                    let labels: Option<String> = row.get(1)?;
                    Ok((
                        row.get::<_, i64>(0)?,
                        crate::models::RunnerCapabilities {
                            known: row.get(4)?,
                            labels: labels
                                .and_then(|l| serde_json::from_str(&l).ok())
                                .unwrap_or_default(),
                            runner_group_id: row.get(2)?,
                            runner_group_name: row.get(3)?,
                        },
                    ))
                },
            )
            .optional()
            .map_err(ControlError::backend)
        })
    }

    async fn runner_exists(&self, runner_id: i64) -> Result<bool, ControlError> {
        self.with_reader(|conn| {
            conn.query_row(
                "SELECT 1 FROM runners WHERE runner_id=?1",
                params![runner_id],
                |_| Ok(()),
            )
            .optional()
            .map(|row| row.is_some())
            .map_err(ControlError::backend)
        })
    }

    async fn runner_for_client(&self, client_id: &str) -> Result<Option<i64>, ControlError> {
        self.with_reader(|conn| {
            conn.query_row(
                "SELECT runner_id FROM runners WHERE client_id=?1",
                params![client_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ControlError::backend)
        })
    }

    async fn run_for_attempt(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<RunId>, ControlError> {
        let agent = agent_job_id.to_string();
        self.with_reader(|conn| {
            conn.query_row(
                "SELECT run_id FROM job_requests WHERE agent_job_id=?1",
                params![agent],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map(|run| run.map(|run| parse_run_id(&run)))
            .map_err(ControlError::backend)
        })
    }

    async fn patch_steps(
        &self,
        agent_job_id: uuid::Uuid,
        patches: Vec<StepPatch>,
    ) -> Result<(), ControlError> {
        let agent = agent_job_id.to_string();
        run_blocking(|| {
            let mut conn = self.conn.lock();
            let tx = conn.transaction().map_err(ControlError::backend)?;
            for p in &patches {
                tx.execute(
                    "INSERT INTO job_steps (agent_job_id, step_id, position, kind, workflow_index, \
                 runner_number, context_name, name, conclusion, started_at_us, finished_at_us) \
                 SELECT ?1, ?2, COALESCE((SELECT MAX(position) + 1 FROM job_steps \
                 WHERE agent_job_id = ?1), 0), 'synthetic', NULL, NULL, NULL, ?3, ?4, \
                 COALESCE(?5, ?7), ?6 \
                 WHERE EXISTS (SELECT 1 FROM job_requests WHERE agent_job_id = ?1) \
                 ON CONFLICT(agent_job_id, step_id) DO UPDATE SET name = excluded.name, \
                 conclusion = excluded.conclusion, \
                 started_at_us = COALESCE(?5, job_steps.started_at_us), \
                 finished_at_us = COALESCE(?6, job_steps.finished_at_us)",
                    params![
                        agent,
                        p.id,
                        p.name,
                        p.conclusion,
                        p.started_at_us,
                        p.finished_at_us,
                        p.observed_us
                    ],
                )
                .map_err(ControlError::backend)?;
            }
            tx.commit().map_err(ControlError::backend)
        })
    }

    async fn job_detail_missing(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<bool, ControlError> {
        let run = run_id.0.to_string();
        let job = job_id.0.clone();
        self.with_reader(|conn| {
            conn.query_row(
                "SELECT detail_json IS NULL FROM run_jobs WHERE run_id = ?1 AND job_id = ?2",
                params![run, job],
                |row| row.get::<_, bool>(0),
            )
            .optional()
            .map(|missing| missing.unwrap_or(true))
            .map_err(ControlError::backend)
        })
    }

    async fn ensure_job_detail(
        &self,
        run_id: RunId,
        job_id: &JobId,
        conclusion: Option<&str>,
    ) -> Result<(), ControlError> {
        let run = run_id.0.to_string();
        let job = job_id.0.clone();
        let conclusion = conclusion.map(str::to_owned);
        run_blocking(move || {
            let mut conn = self.conn.lock();
            let tx = conn.transaction().map_err(ControlError::backend)?;
            // `JobDetail::find` semantics: match on the stable job key, with a
            // name fallback for details restored without one.
            let mut rows = tx
                .prepare(
                    "SELECT job_id, detail_json, detail_position FROM run_jobs \
                     WHERE run_id = ?1 AND detail_json IS NOT NULL",
                )
                .map_err(ControlError::backend)?;
            let details: Vec<(String, String, Option<i64>)> = rows
                .query_map(params![run], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .map_err(ControlError::backend)?
                .collect::<Result<_, _>>()
                .map_err(ControlError::backend)?;
            drop(rows);
            let mut found: Option<(String, crate::models::JobDetail, Option<i64>)> = None;
            for (row_job, json, pos) in &details {
                let detail: crate::models::JobDetail =
                    serde_json::from_str(json).map_err(ControlError::backend)?;
                if detail.job_id == job || (detail.job_id.is_empty() && detail.name == job) {
                    found = Some((row_job.clone(), detail, *pos));
                    break;
                }
            }
            let (row_job, mut detail, position) = match found {
                Some(v) => v,
                None => (
                    job.clone(),
                    crate::models::JobDetail {
                        job_id: job.clone(),
                        name: job.clone(),
                        // A timeline update means the job started; the run
                        // record's final conclusion comes from the job status
                        // map. Default to the truthful in-flight state.
                        conclusion: "in_progress".to_owned(),
                        steps: Vec::new(),
                        annotations: Vec::new(),
                    },
                    None,
                ),
            };
            let mut changed = position.is_none();
            if let Some(conclusion) = &conclusion {
                if detail.conclusion != *conclusion {
                    detail.conclusion = conclusion.clone();
                    changed = true;
                }
            }
            if !changed {
                return Ok(());
            }
            let position = match position {
                Some(p) => p,
                None => tx
                    .query_row(
                        "SELECT COALESCE(MAX(detail_position) + 1, 0) FROM run_jobs \
                         WHERE run_id = ?1",
                        params![run],
                        |row| row.get(0),
                    )
                    .map_err(ControlError::backend)?,
            };
            let json = serde_json::to_string(&detail).map_err(ControlError::backend)?;
            tx.execute(
                "INSERT INTO run_jobs (run_id, job_id, detail_json, detail_position) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(run_id, job_id) DO UPDATE SET \
                 detail_json = excluded.detail_json, \
                 detail_position = excluded.detail_position",
                params![run, row_job, json, position],
            )
            .map_err(ControlError::backend)?;
            tx.commit().map_err(ControlError::backend)
        })
    }

    async fn attempt_job(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<(RunId, JobId)>, ControlError> {
        let agent = agent_job_id.to_string();
        self.with_reader(|conn| {
            conn.query_row(
                "SELECT run_id, job_id FROM job_requests WHERE agent_job_id=?1",
                params![agent],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map(|row| row.map(|(run, job)| (parse_run_id(&run), JobId(job))))
            .map_err(ControlError::backend)
        })
    }

    async fn set_job_check_run(
        &self,
        run_id: RunId,
        job_id: &JobId,
        check_run_id: u64,
    ) -> Result<bool, ControlError> {
        let run = run_id.0.to_string();
        let job = job_id.0.clone();
        run_blocking(move || {
            let mut conn = self.conn.lock();
            let tx = conn.transaction().map_err(ControlError::backend)?;
            // `BEGIN` already serialized writers; no extra lock needed.
            let changed = tx
                .execute(
                    "INSERT INTO run_jobs (run_id, job_id, check_run_id) \
                     SELECT ?1, ?2, ?3 \
                     WHERE EXISTS (SELECT 1 FROM jobs WHERE run_id = ?1 AND job_id = ?2) \
                     ON CONFLICT(run_id, job_id) DO UPDATE SET \
                     check_run_id = excluded.check_run_id \
                     WHERE run_jobs.check_run_id IS NOT ?3",
                    params![run, job, check_run_id as i64],
                )
                .map_err(ControlError::backend)?;
            tx.commit().map_err(ControlError::backend)?;
            Ok(changed > 0)
        })
    }

    async fn clear_job_check_run(
        &self,
        run_id: RunId,
        job_id: &JobId,
        expected: u64,
    ) -> Result<(), ControlError> {
        let run = run_id.0.to_string();
        let job = job_id.0.clone();
        run_blocking(move || {
            let conn = self.conn.lock();
            conn.execute(
                "UPDATE run_jobs SET check_run_id = NULL \
                 WHERE run_id = ?1 AND job_id = ?2 AND check_run_id = ?3",
                params![run, job, expected as i64],
            )
            .map_err(ControlError::backend)?;
            Ok(())
        })
    }

    async fn job_exists(&self, run_id: RunId, job_id: &JobId) -> Result<bool, ControlError> {
        let run = run_id.0.to_string();
        let job = job_id.0.clone();
        self.with_reader(|conn| {
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE run_id = ?1 AND job_id = ?2)",
                params![run, job],
                |row| row.get::<_, bool>(0),
            )
            .map_err(ControlError::backend)
        })
    }

    async fn job_display_name(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<String>, ControlError> {
        let run = run_id.0.to_string();
        let job = job_id.0.clone();
        self.with_reader(|conn| {
            conn.query_row(
                "SELECT display_name FROM run_jobs WHERE run_id = ?1 AND job_id = ?2",
                params![run, job],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .map(|name| name.flatten())
            .map_err(ControlError::backend)
        })
    }

    async fn reap_inputs(&self) -> Result<ReapInputs, ControlError> {
        let timeout = std::time::Duration::from_nanos(
            self.runner_liveness_timeout
                .load(std::sync::atomic::Ordering::Relaxed),
        );
        let cutoff = system_to_us(std::time::SystemTime::now()) - timeout.as_micros() as i64;
        self.with_reader(|conn| {
            let mut inputs = ReapInputs::default();
            let mut stmt = conn
                .prepare("SELECT request_id, run_id, job_id, started_at_us, last_renewed_at_us, \
             timeout_triggered, job_timeout_s FROM job_requests WHERE result IS NULL")
                .map_err(ControlError::backend)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok(ActiveRequest {
                        request_id: row.get(0)?,
                        run_id: parse_run_id(&row.get::<_, String>(1)?),
                        job_id: JobId(row.get(2)?),
                        started_at: row.get::<_, Option<i64>>(3)?.map(us_to_system),
                        last_renewed_at: row.get::<_, Option<i64>>(4)?.map(us_to_system),
                        timeout_triggered: row.get::<_, i64>(5)? != 0,
                        job_timeout_s: row.get(6)?,
                    })
                })
                .map_err(ControlError::backend)?;
            for row in rows {
                inputs.active.push(row.map_err(ControlError::backend)?);
            }
            let mut stmt = conn
                .prepare("SELECT run_id, job_id, runs_on, enqueued_at_us, reaper_first_seen_us IS NOT NULL FROM jobs WHERE queue_kind = 'ready' \
             ORDER BY priority DESC, run_order, job_order, seq, run_id, job_id")
                .map_err(ControlError::backend)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok(ReadyRow {
                        run_id: parse_run_id(&row.get::<_, String>(0)?),
                        job_id: JobId(row.get(1)?),
                        runs_on: serde_json::from_str(&row.get::<_, String>(2)?)
                            .unwrap_or_default(),
                        enqueued_at_unix_nanos: row.get::<_, Option<i64>>(3)?.unwrap_or(0) * 1000,
                        observed: row.get(4)?,
                    })
                })
                .map_err(ControlError::backend)?;
            for row in rows {
                inputs.ready.push(row.map_err(ControlError::backend)?);
            }
            let mut stmt = conn
                .prepare("SELECT labels FROM runners")
                .map_err(ControlError::backend)?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(ControlError::backend)?;
            for row in rows {
                inputs.runner_labels.push(
                    serde_json::from_str(&row.map_err(ControlError::backend)?).unwrap_or_default(),
                );
            }
            inputs.has_bindings = conn
                .query_row("SELECT EXISTS(SELECT 1 FROM job_assignments) OR EXISTS(SELECT 1 FROM pool_pending)", [], |row| row.get(0))
                .map_err(ControlError::backend)?;
            for (sql, set) in [
                ("SELECT DISTINCT runner_id FROM runner_sessions \
             WHERE runner_id IS NOT NULL AND last_seen_at_us < ?1", &mut inputs.stale_runners),
                ("SELECT r.runner_id FROM runners r WHERE r.registered_at_us < ?1 \
             AND NOT EXISTS (SELECT 1 FROM runner_sessions s WHERE s.runner_id = r.runner_id)", &mut inputs.phantom_runners),
            ] {
                let mut stmt = conn.prepare(sql).map_err(ControlError::backend)?;
                let rows = stmt
                    .query_map(params![cutoff], |row| row.get::<_, i64>(0))
                    .map_err(ControlError::backend)?;
                for row in rows {
                    set.insert(row.map_err(ControlError::backend)?);
                }
            }
            Ok(inputs)
        })
    }

    async fn run_in_concurrency(&self, run_id: RunId) -> Result<bool, ControlError> {
        let run_id = run_id.0.to_string();
        self.with_reader(|conn| {
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM run_concurrency WHERE run_id=?1) \
                 OR EXISTS(SELECT 1 FROM jobset_gates WHERE run_id=?1) \
                 OR EXISTS(SELECT 1 FROM jobs WHERE run_id=?1 AND concurrency_json IS NOT NULL) \
                 OR EXISTS(SELECT 1 FROM concurrency_holds WHERE holder_run_id=?1) \
                 OR EXISTS(SELECT 1 FROM concurrency_waits WHERE holder_run_id=?1)",
                params![run_id],
                |row| row.get(0),
            )
            .map_err(ControlError::backend)
        })
    }

    async fn callback_job(
        &self,
        plan_id: &str,
        timeline_id: Option<uuid::Uuid>,
    ) -> Result<Option<CallbackJob>, ControlError> {
        let timeline = timeline_id.map(|id| id.to_string()).unwrap_or_default();
        self.with_reader(|conn| {
            conn.query_row(
                "SELECT r.request_id, r.run_id, r.job_id, r.agent_job_id, j.status \
                 FROM job_requests r LEFT JOIN jobs j ON j.run_id = r.run_id AND j.job_id = r.job_id \
                 WHERE r.plan_id = ?1 OR r.timeline_id = ?2 \
                 ORDER BY (r.plan_id = ?1) DESC, r.request_id DESC LIMIT 1",
                params![plan_id, timeline],
                |row| {
                    Ok(CallbackJob {
                        request_id: row.get(0)?,
                        run_id: parse_run_id(&row.get::<_, String>(1)?),
                        job_id: JobId(row.get(2)?),
                        agent_job_id: parse_uuid(&row.get::<_, String>(3)?),
                        job_status: row.get::<_, Option<String>>(4)?.map(|s| status_parse(&s)),
                    })
                },
            )
            .optional()
            .map_err(ControlError::backend)
        })
    }

    async fn renew_lease(
        &self,
        agent_job_id: uuid::Uuid,
        runner_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError> {
        run_blocking(|| {
            let conn = self.conn.lock();
            let agent = agent_job_id.to_string();
            let renewed = conn
                .execute(
                    "UPDATE job_requests SET locked_until = ?1, last_renewed_at_us = ?2 \
                     WHERE agent_job_id = ?3 AND result IS NULL AND owner_runner_id = ?4",
                    params![
                        locked_until,
                        system_to_us(std::time::SystemTime::now()),
                        agent,
                        runner_id
                    ],
                )
                .map_err(ControlError::backend)?;
            if renewed == 1 {
                return Ok(true);
            }
            let row = conn
                .query_row(
                    "SELECT result, owner_runner_id FROM job_requests WHERE agent_job_id = ?1",
                    params![agent],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(ControlError::backend)?;
            super::types::renew_miss(row, runner_id)
        })
    }

    async fn request_owner(
        &self,
        request_id: i64,
    ) -> Result<Option<(Option<i64>, Option<i64>)>, ControlError> {
        self.with_reader(move |conn| {
            conn.query_row(
                "SELECT jr.owner_runner_id, sess.runner_id FROM job_requests jr \
                 LEFT JOIN runner_sessions sess ON sess.active_request_id = jr.request_id \
                 WHERE jr.request_id = ?1",
                params![request_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(ControlError::backend)
        })
    }

    async fn renew_agent_request(
        &self,
        request_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError> {
        let locked_until = locked_until.to_owned();
        run_blocking(move || {
            let conn = self.conn.lock();
            let renewed = conn
                .execute(
                    "UPDATE job_requests SET locked_until = ?1, last_renewed_at_us = ?2 \
                     WHERE request_id = ?3 AND result IS NULL",
                    params![
                        locked_until,
                        system_to_us(std::time::SystemTime::now()),
                        request_id
                    ],
                )
                .map_err(ControlError::backend)?;
            Ok(renewed == 1)
        })
    }

    async fn settle_agent_request(
        &self,
        request_id: i64,
        result: ExecutionStatus,
        locked_until: &str,
    ) -> Result<Option<(RunId, JobId, uuid::Uuid)>, ControlError> {
        let status = status_str(result).to_owned();
        let locked_until = locked_until.to_owned();
        run_blocking(move || {
            let conn = self.conn.lock();
            conn.query_row(
                "UPDATE job_requests SET result = ?1, locked_until = ?2 \
                 WHERE request_id = ?3 AND result IS NULL \
                 RETURNING run_id, job_id, agent_job_id",
                params![status, locked_until, request_id],
                |row| {
                    Ok((
                        parse_run_id(&row.get::<_, String>(0)?),
                        JobId(row.get::<_, String>(1)?),
                        parse_uuid(&row.get::<_, String>(2)?),
                    ))
                },
            )
            .optional()
            .map_err(ControlError::backend)
        })
    }

    async fn delete_inflight(&self, session_id: &str, message_id: i64) -> Result<(), ControlError> {
        let session_id = session_id.to_owned();
        run_blocking(move || {
            let conn = self.conn.lock();
            conn.execute(
                "DELETE FROM broker_messages WHERE session_id = ?1 AND message_id = ?2",
                params![session_id, message_id],
            )
            .map_err(ControlError::backend)?;
            Ok(())
        })
    }

    async fn active_plan_ids(&self) -> Result<BTreeSet<String>, ControlError> {
        self.with_reader(|conn| {
            let mut stmt = conn
                .prepare("SELECT DISTINCT plan_id FROM job_requests WHERE result IS NULL")
                .map_err(ControlError::backend)?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(ControlError::backend)?;
            let mut plans = BTreeSet::new();
            for plan in rows {
                plans.insert(plan.map_err(ControlError::backend)?);
            }
            Ok(plans)
        })
    }

    async fn patch_timeline(
        &self,
        timeline_key: &str,
        mut records: Vec<preloop_gha_protocol::azdo::TimelineRecord>,
    ) -> Result<(i32, Vec<preloop_gha_protocol::azdo::TimelineRecord>), ControlError> {
        run_blocking(|| {
            let mut conn = self.conn.lock();
            let txn = conn.transaction().map_err(ControlError::backend)?;
            let now = std::time::SystemTime::now();
            let change_id: i64 = txn
                .query_row(
                    "INSERT INTO timelines (timeline_key, change_id, updated_at_us) \
                     VALUES (?1, 1, ?2) ON CONFLICT (timeline_key) DO UPDATE \
                     SET change_id = timelines.change_id + 1, updated_at_us = ?2 \
                     RETURNING change_id",
                    params![timeline_key, system_to_us(now)],
                    |row| row.get(0),
                )
                .map_err(ControlError::backend)?;
            for (id, body) in super::types::stamp_timeline_records(&mut records, change_id, now) {
                txn.execute(
                    "INSERT INTO timeline_records (timeline_key, record_id, record_json) \
                     VALUES (?1, ?2, ?3) ON CONFLICT (timeline_key, record_id) \
                     DO UPDATE SET record_json = excluded.record_json",
                    params![timeline_key, id, body],
                )
                .map_err(ControlError::backend)?;
            }
            let stored = {
                let mut stmt = txn
                    .prepare(
                        "SELECT record_json FROM timeline_records WHERE timeline_key = ?1 \
                         ORDER BY record_id LIMIT ?2",
                    )
                    .map_err(ControlError::backend)?;
                let rows = stmt
                    .query_map(
                        params![timeline_key, super::types::MAX_TIMELINE_RECORDS as i64],
                        |row| row.get::<_, String>(0),
                    )
                    .map_err(ControlError::backend)?
                    .filter_map(Result::ok)
                    .filter_map(|json| serde_json::from_str(&json).ok())
                    .collect();
                rows
            };
            txn.commit().map_err(ControlError::backend)?;
            Ok((change_id as i32, stored))
        })
    }

    async fn get_timeline(
        &self,
        timeline_key: &str,
        skip: usize,
        top: usize,
    ) -> Result<(i32, Vec<preloop_gha_protocol::azdo::TimelineRecord>), ControlError> {
        self.with_reader(|conn| {
            let change_id: i64 = conn
                .query_row(
                    "SELECT change_id FROM timelines WHERE timeline_key = ?1",
                    params![timeline_key],
                    |row| row.get(0),
                )
                .optional()
                .map_err(ControlError::backend)?
                .unwrap_or(0);
            let mut stmt = conn
                .prepare(
                    "SELECT record_json FROM timeline_records WHERE timeline_key = ?1 \
                     ORDER BY record_id LIMIT ?2 OFFSET ?3",
                )
                .map_err(ControlError::backend)?;
            let records = stmt
                .query_map(
                    params![
                        timeline_key,
                        top.min(super::types::MAX_TIMELINE_RECORDS) as i64,
                        skip.min(i64::MAX as usize) as i64
                    ],
                    |row| row.get::<_, String>(0),
                )
                .map_err(ControlError::backend)?
                .filter_map(Result::ok)
                .filter_map(|json| serde_json::from_str(&json).ok())
                .collect();
            Ok((change_id as i32, records))
        })
    }

    async fn prune_timelines(&self, before_us: i64) -> Result<u64, ControlError> {
        run_blocking(|| {
            self.conn
                .lock()
                .execute(
                    "DELETE FROM timelines WHERE updated_at_us < ?1",
                    params![before_us],
                )
                .map(|n| n as u64)
                .map_err(ControlError::backend)
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
        self.with_reader(|conn| {
            let mut stats = QueueStats::default();
            let mut stmt = conn
                .prepare(
                    "SELECT queue_kind, COUNT(*) FROM jobs
                     WHERE queue_kind NOT IN ('none', 'expand')
                     GROUP BY queue_kind
                     UNION ALL
                     SELECT 'expand', COUNT(*) FROM jobs
                     WHERE queue_kind = 'expand' AND expand_generation = 0",
                )
                .map_err(ControlError::backend)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(ControlError::backend)?;
            for row in rows {
                let (kind, count) = row.map_err(ControlError::backend)?;
                let count = count.max(0) as usize;
                match kind.as_str() {
                    "ready" => stats.ready = count,
                    "pending" => stats.pending = count,
                    "blocked" => stats.blocked = count,
                    "held" => stats.held = count,
                    "claimed" => stats.claimed = count,
                    "expand" => stats.expanding = count,
                    _ => {}
                }
            }
            stats.next_runs_on = conn
                .query_row(
                    "SELECT runs_on FROM jobs WHERE queue_kind = 'ready'
                     ORDER BY priority DESC, run_order, job_order, seq, run_id, job_id LIMIT 1",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(ControlError::backend)?
                .and_then(|runs_on| serde_json::from_str(&runs_on).ok())
                .unwrap_or_default();
            Ok(stats)
        })
    }

    async fn live_assignments(
        &self,
    ) -> Result<Vec<preloop_observability::status::RunnerAssignment>, ControlError> {
        self.with_reader(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT r.owner_runner_id, r.run_id, r.job_id, r.started_at_us
                     FROM runner_sessions s JOIN job_requests r
                       ON s.active_request_id = r.request_id
                     WHERE r.result IS NULL AND r.owner_runner_id IS NOT NULL
                     ORDER BY r.owner_runner_id",
                )
                .map_err(ControlError::backend)?;
            let now = std::time::SystemTime::now();
            let rows = stmt
                .query_map([], |row| {
                    let started: Option<i64> = row.get(3)?;
                    Ok(preloop_observability::status::RunnerAssignment {
                        runner_id: row.get(0)?,
                        run_id: row.get(1)?,
                        job_id: row.get(2)?,
                        assigned_seconds_ago: started
                            .and_then(|us| now.duration_since(us_to_system(us)).ok())
                            .map(|age| age.as_secs_f64())
                            .unwrap_or(0.0),
                    })
                })
                .map_err(ControlError::backend)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(ControlError::backend)
        })
    }
}
