//! The SQLite `ControlBackend` on the agreed control schema
//! (`docs/control-schema.sql`, translated in `lite/schema.sql`).
//!
//! No working set, no write-back: every command is one short transaction
//! of targeted statements. SQLite has exactly one writer — the `writer`
//! connection, every write transaction opened `BEGIN IMMEDIATE` — so the
//! row locks and `FOR UPDATE SKIP LOCKED` the Postgres backend needs are
//! unnecessary here: a command holding the writer already excludes every
//! other writer. Reads run on a pool of `query_only` connections under WAL
//! and never queue behind the writer.
//!
//! Transitions are still conditional statements (`WHERE status = ..`) so
//! the SQL mirrors the Postgres backend statement for statement.

mod codec;
mod queries;
mod requests;
mod runners;
mod steps;
mod timelines;
mod webhooks;

use super::types::*;
use parking_lot::{Condvar, Mutex};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// The schema this build reads and writes. Greenfield: any other stamped
/// version is refused at open (no migrations).
pub(crate) const SCHEMA_VERSION: &str = "1";

/// The translated schema (see the file header for the type mapping).
const SCHEMA_SQL: &str = include_str!("schema.sql");

/// Readers in the pool. Enough for the read-heavy paths (acquire context,
/// status, queue stats) without fd pressure.
const READERS: usize = 4;

/// The SQLite control backend.
pub(crate) struct LiteBackend {
    /// The single writer. Every write transaction is `BEGIN IMMEDIATE`, so
    /// a command never upgrades a read lock mid-transaction.
    writer: Mutex<Connection>,
    /// `query_only` connections lent by [`LiteBackend::read`]. Empty for an
    /// in-memory database (a second connection cannot see it); reads then
    /// use the writer.
    readers: Mutex<Vec<Connection>>,
    readers_idle: Condvar,
    pool_assignments_enabled: AtomicBool,
    require_job_assignments: AtomicBool,
    /// Runner liveness timeout in nanoseconds.
    runner_liveness_timeout: AtomicU64,
}

/// Run `f` without stalling the async executor. On a multi-thread runtime
/// `block_in_place` hands the blocking section a spare worker; on a
/// `current_thread` runtime (unit tests) `f` runs inline, because
/// `block_in_place` would panic there.
fn run_blocking<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

/// Map a rusqlite failure onto the backend error vocabulary.
pub(super) fn db(error: rusqlite::Error) -> ControlError {
    ControlError::backend(error)
}

/// Per-connection settings every connection needs.
fn configure(conn: &Connection) -> Result<(), ControlError> {
    // Foreign keys default OFF per connection; the schema's cascades and
    // deferred FKs only fire with them on.
    conn.pragma_update(None, "foreign_keys", true).map_err(db)?;
    conn.pragma_update(None, "busy_timeout", 5000_i64)
        .map_err(db)?;
    Ok(())
}

/// Apply the schema to a fresh database, or verify an existing one is
/// exactly [`SCHEMA_VERSION`]. Runs in one `BEGIN IMMEDIATE` transaction
/// so two processes opening the same new file cannot both create it.
fn ensure_schema(conn: &mut Connection) -> Result<(), ControlError> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(db)?;
    let has_meta: bool = tx
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM sqlite_master \
             WHERE type = 'table' AND name = 'schema_meta')",
            [],
            |row| row.get(0),
        )
        .map_err(db)?;
    if has_meta {
        let version: Option<Vec<u8>> = tx
            .query_row(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(db)?;
        let version = version.map(|v| String::from_utf8_lossy(&v).into_owned());
        if version.as_deref() != Some(SCHEMA_VERSION) {
            return Err(ControlError::backend(anyhow::anyhow!(
                "control database has schema version {}; this build supports only \
                 {SCHEMA_VERSION}. Recreate the database.",
                version.as_deref().unwrap_or("<none>")
            )));
        }
        return tx.commit().map_err(db);
    }
    let foreign: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master \
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )
        .map_err(db)?;
    if foreign != 0 {
        return Err(ControlError::backend(anyhow::anyhow!(
            "control database holds {foreign} tables but no schema_meta: it predates \
             schema version {SCHEMA_VERSION}. Recreate the database."
        )));
    }
    tx.execute_batch(SCHEMA_SQL).map_err(db)?;
    tx.execute(
        "INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?1)",
        [SCHEMA_VERSION.as_bytes()],
    )
    .map_err(db)?;
    tx.commit().map_err(db)
}

impl LiteBackend {
    /// Open (or create) the control database at `path`.
    pub(crate) fn open(
        path: &Path,
        pool_assignments_enabled: bool,
        require_job_assignments: bool,
        runner_liveness_timeout: Duration,
    ) -> Result<Self, ControlError> {
        // Create the file owner-only before SQLite materializes it, matching
        // the 0600 convention every other state artifact follows.
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .mode(0o600)
                .open(path)
                .map_err(ControlError::backend)?;
        }
        let mut writer = Connection::open(path).map_err(db)?;
        configure(&writer)?;
        writer
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(db)?;
        writer
            .pragma_update(None, "synchronous", "NORMAL")
            .map_err(db)?;
        ensure_schema(&mut writer)?;
        let mut readers = Vec::with_capacity(READERS);
        for _ in 0..READERS {
            let reader = Connection::open(path).map_err(db)?;
            configure(&reader)?;
            // A write through a reader is a hard error, never silent.
            reader.pragma_update(None, "query_only", true).map_err(db)?;
            readers.push(reader);
        }
        #[cfg(unix)]
        for suffix in ["-wal", "-shm"] {
            let sibling = std::path::PathBuf::from(format!("{}{suffix}", path.display()));
            if sibling.exists() {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(&sibling, std::fs::Permissions::from_mode(0o600));
            }
        }
        Ok(Self::from_parts(
            writer,
            readers,
            pool_assignments_enabled,
            require_job_assignments,
            runner_liveness_timeout,
        ))
    }

    /// An in-memory backend for tests. The reader pool is empty, so reads
    /// run on the writer.
    #[cfg(test)]
    pub(crate) fn in_memory() -> Result<Self, ControlError> {
        let mut writer = Connection::open_in_memory().map_err(db)?;
        configure(&writer)?;
        ensure_schema(&mut writer)?;
        Ok(Self::from_parts(
            writer,
            Vec::new(),
            false,
            false,
            Duration::from_secs(300),
        ))
    }

    fn from_parts(
        writer: Connection,
        readers: Vec<Connection>,
        pool_assignments_enabled: bool,
        require_job_assignments: bool,
        runner_liveness_timeout: Duration,
    ) -> Self {
        Self {
            writer: Mutex::new(writer),
            readers: Mutex::new(readers),
            readers_idle: Condvar::new(),
            pool_assignments_enabled: AtomicBool::new(pool_assignments_enabled),
            require_job_assignments: AtomicBool::new(require_job_assignments),
            runner_liveness_timeout: AtomicU64::new(runner_liveness_timeout.as_nanos() as u64),
        }
    }

    /// The live scheduling config: `(pool assignments, strict assignments,
    /// runner liveness timeout)`.
    pub(crate) fn config(&self) -> (bool, bool, Duration) {
        (
            self.pool_assignments_enabled.load(Ordering::Acquire),
            self.require_job_assignments.load(Ordering::Acquire),
            Duration::from_nanos(self.runner_liveness_timeout.load(Ordering::Acquire)),
        )
    }

    /// Apply the real server config once bootstrap knows it.
    pub(crate) fn set_config(
        &self,
        pool_assignments_enabled: bool,
        require_job_assignments: bool,
        runner_liveness_timeout: Duration,
    ) {
        self.pool_assignments_enabled
            .store(pool_assignments_enabled, Ordering::Release);
        self.require_job_assignments
            .store(require_job_assignments, Ordering::Release);
        self.runner_liveness_timeout
            .store(runner_liveness_timeout.as_nanos() as u64, Ordering::Release);
    }

    /// Run one command as a write transaction: `BEGIN IMMEDIATE`, `f`,
    /// `COMMIT`. An error from `f` rolls everything back (the transaction
    /// drops uncommitted).
    pub(super) fn write<T>(
        &self,
        f: impl FnOnce(&Transaction<'_>) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        run_blocking(|| {
            let mut conn = self.writer.lock();
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(db)?;
            let value = f(&tx)?;
            tx.commit().map_err(db)?;
            Ok(value)
        })
    }

    /// Run a read-only command on one consistent snapshot (a deferred read
    /// transaction) on a pooled reader, or on the writer when the pool is
    /// empty (in-memory databases).
    pub(super) fn read<T>(
        &self,
        f: impl FnOnce(&Transaction<'_>) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        run_blocking(|| {
            if self.readers.lock().is_empty() {
                let mut conn = self.writer.lock();
                let tx = conn.transaction().map_err(db)?;
                return f(&tx);
            }
            let mut conn = {
                let mut pool = self.readers.lock();
                loop {
                    if let Some(conn) = pool.pop() {
                        break conn;
                    }
                    self.readers_idle.wait(&mut pool);
                }
            };
            let result = conn.transaction().map_err(db).and_then(|tx| f(&tx));
            self.readers.lock().push(conn);
            self.readers_idle.notify_one();
            result
        })
    }

    // ── Key fingerprint / run numbers / temporary meta ─────────────────

    /// Record the cluster key fingerprint on first use; afterwards refuse
    /// a node whose key differs.
    ///
    /// `INSERT .. ON CONFLICT DO NOTHING` into `schema_meta`, then read the
    /// stored value back and compare.
    pub(crate) async fn ensure_key_fingerprint(&self, fingerprint: &str) -> Result<(), ControlError> {
        self.write(|tx| {
            tx.execute(
                "INSERT INTO schema_meta (key, value) VALUES ('key_fingerprint', ?1) \
                 ON CONFLICT (key) DO NOTHING",
                [fingerprint.as_bytes()],
            )
            .map_err(db)?;
            let stored: Vec<u8> = tx
                .query_row(
                    "SELECT value FROM schema_meta WHERE key = 'key_fingerprint'",
                    [],
                    |row| row.get(0),
                )
                .map_err(db)?;
            check_key_fingerprint(&stored, fingerprint)
        })
    }

    /// Allocate the next run number for a workflow:
    /// `INSERT .. ON CONFLICT DO UPDATE SET last_run_number = last_run_number
    /// + 1 RETURNING last_run_number`. Namespace `'default'` and repository
    /// `''` until the trait passes both (decision round 1, Q4).
    pub(crate) async fn allocate_run_number(&self, workflow_path: &str) -> Result<u64, ControlError> {
        self.write(|tx| {
            let number: i64 = tx
                .query_row(
                    "INSERT INTO workflow_run_numbers \
                         (namespace_id, repository, workflow_path, last_run_number) \
                     VALUES (?1, '', ?2, 1) \
                     ON CONFLICT (namespace_id, repository, workflow_path) \
                     DO UPDATE SET last_run_number = last_run_number + 1 \
                     RETURNING last_run_number",
                    rusqlite::params![DEFAULT_NAMESPACE, workflow_path],
                    |row| row.get(0),
                )
                .map_err(db)?;
            Ok(number as u64)
        })
    }

    /// Temporary (decision round 1, Q2): node-local metadata has no table in
    /// the agreed schema; core removes this method.
    pub(crate) async fn store_meta(
        &self,
        _meta: &crate::store::MetaSnapshot,
    ) -> Result<(), ControlError> {
        Ok(())
    }

    /// Temporary (decision round 1, Q2); see [`LiteBackend::store_meta`].
    pub(crate) async fn load_meta(
        &self,
    ) -> Result<Option<crate::store::MetaSnapshot>, ControlError> {
        Ok(None)
    }
}

#[cfg(test)]
impl LiteBackend {
    /// Run raw SQL on the writer (test fixtures only: seeding rows the
    /// scheduling commands will insert once they exist).
    pub(crate) fn exec_for_test(&self, sql: &str) {
        self.writer.lock().execute_batch(sql).unwrap();
    }
}
