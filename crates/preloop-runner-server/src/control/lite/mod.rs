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

mod acquire;
mod check_runs;
mod codec;
mod concurrency;
mod dispatch;
mod expansion;
mod fork_gate;
mod impls;
mod jobs;
mod lifecycle;
mod poll;
mod promote;
mod queries;
mod reaper;
mod requests;
mod runners;
mod settle;
mod steps;
mod submit;
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod testview;
#[cfg(any(test, feature = "test-support"))]
pub(crate) use testview::TestDb;
#[cfg(test)]
mod tests;
mod timelines;
mod webhooks;

use super::types::*;
use parking_lot::{Condvar, Mutex};
use rusqlite::{Connection, Transaction, TransactionBehavior};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// The translated schema (see the file header for the type mapping). The
/// runtime only verifies the migration ledger at open; this definition is
/// the test-side parity reference for `migrations/sqlite`, never applied by
/// a serving process. `migrations.rs` compiles against it.
#[cfg(test)]
pub(crate) const SCHEMA_SQL: &str = include_str!("schema.sql");

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
    /// Environment protection rules resolver (TOML fallback + GitHub
    /// environments API). `EnvironmentResolver::local(empty)` until
    /// bootstrap installs the shared one — every gate proceeds, the
    /// pre-rules behavior.
    environment_resolver:
        parking_lot::RwLock<Arc<crate::environment_resolver::EnvironmentResolver>>,
    /// The co-hosted runner pool's shared status handle; its advertised
    /// labels decide whether a `runs-on` is satisfiable at submit and
    /// promotion. Detached (no labels) until bootstrap hands it over.
    pool_status: parking_lot::RwLock<preloop_observability::status::PoolStatus>,
    /// Stale bindings released since this node started (node-local, not
    /// persisted).
    released_bindings: AtomicU64,
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
fn configure(conn: &mut Connection) -> Result<(), ControlError> {
    // Foreign keys default OFF per connection; the schema's cascades and
    // deferred FKs only fire with them on.
    conn.pragma_update(None, "foreign_keys", true).map_err(db)?;
    conn.pragma_update(None, "busy_timeout", 5000_i64)
        .map_err(db)?;
    Ok(())
}

/// Verify the database is exactly this build's control schema, or — only for
/// test-support — initialize a brand-new file.
///
/// A serving process never creates, migrates, recreates or adopts: the
/// ledger is the sole version authority, and anything else (a legacy store,
/// a foreign file, a pre-ledger control schema, an older/newer/divergent
/// migration set) refuses with the recovery command. `preloop store migrate`
/// (refinery over `migrations/sqlite`, see `docs/control-migrations.md`) is
/// the only writer of schema state.
fn ensure_usable(conn: &mut Connection) -> Result<(), ControlError> {
    let ledger = super::migrations::sqlite_ledger(conn).map_err(db)?;
    if let super::migrations::Ledger::Empty = ledger {
        #[cfg(any(test, feature = "test-support"))]
        {
            super::migrate_runner::initialize_empty_sqlite(conn).map_err(ControlError::backend)?;
            return Ok(());
        }
        #[cfg(not(any(test, feature = "test-support")))]
        return Err(super::migrations::backend_error(
            super::migrations::refusal(super::migrations::Ledger::Empty),
        ));
    }
    super::migrations::check_ledger(ledger).map_err(super::migrations::backend_error)
}

impl LiteBackend {
    /// Open the control database at `path` and verify its migration ledger.
    ///
    /// A serving process never creates the database: an absent (or empty)
    /// file refuses with the migration command, and an existing file's
    /// ledger must match this build exactly. Only test-support builds
    /// initialize a brand-new file (via the same migration SQL the runner
    /// applies).
    pub(crate) fn open(
        path: &Path,
        pool_assignments_enabled: bool,
        require_job_assignments: bool,
        runner_liveness_timeout: Duration,
    ) -> Result<Self, ControlError> {
        let absent = std::fs::metadata(path)
            .map(|meta| meta.len() == 0)
            .unwrap_or(true);
        #[cfg(not(any(test, feature = "test-support")))]
        if absent {
            return Err(super::migrations::backend_error(format!(
                "control database {} is not initialized (no migration ledger). Run \
                 `preloop store migrate` before starting the server; the server never \
                 creates, migrates or recreates the control schema itself \
                 (docs/control-migrations.md).",
                path.display()
            )));
        }
        // Test-support only: create the file owner-only before SQLite
        // materializes it, matching the 0600 convention every other state
        // artifact follows.
        #[cfg(any(test, feature = "test-support"))]
        if absent {
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
        }
        let mut writer = Connection::open(path).map_err(db)?;
        configure(&mut writer)?;
        writer
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(db)?;
        writer
            .pragma_update(None, "synchronous", "NORMAL")
            .map_err(db)?;
        ensure_usable(&mut writer)?;
        let mut readers = Vec::with_capacity(READERS);
        for _ in 0..READERS {
            let mut reader = Connection::open(path).map_err(db)?;
            configure(&mut reader)?;
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
    /// run on the writer. Initialized by replaying the same migration SQL the
    /// runner applies (`migrations/sqlite`), never by a second schema source.
    #[cfg(test)]
    pub(crate) fn in_memory() -> Result<Self, ControlError> {
        let mut writer = Connection::open_in_memory().map_err(db)?;
        configure(&mut writer)?;
        super::migrate_runner::initialize_empty_sqlite(&mut writer)
            .map_err(ControlError::backend)?;
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
            environment_resolver: parking_lot::RwLock::new(
                crate::environment_resolver::EnvironmentResolver::local(
                    crate::config::EnvironmentRulesMap::new(),
                ),
            ),
            pool_status: parking_lot::RwLock::new(Default::default()),
            released_bindings: AtomicU64::new(0),
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

    /// Stale bindings released since this node started.
    pub(super) fn released_bindings(&self) -> u64 {
        self.released_bindings.load(Ordering::Acquire)
    }

    /// Add to the released-bindings counter (sweep bookkeeping).
    pub(super) fn count_released_bindings(&self, released: usize) {
        self.released_bindings
            .fetch_add(released as u64, Ordering::AcqRel);
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

    /// Install the shared environment-rules resolver once bootstrap builds
    /// it. The default resolves nothing: every gate proceeds.
    pub(crate) fn set_environment_resolver(
        &self,
        resolver: Arc<crate::environment_resolver::EnvironmentResolver>,
    ) {
        *self.environment_resolver.write() = resolver;
    }

    /// The live environment-rules resolver (an `Arc` clone, cheap).
    pub(crate) fn environment_resolver(
        &self,
    ) -> Arc<crate::environment_resolver::EnvironmentResolver> {
        self.environment_resolver.read().clone()
    }

    /// Share the runner pool's status handle (bootstrap / `AppState`).
    pub(crate) fn set_pool_status(&self, status: preloop_observability::status::PoolStatus) {
        *self.pool_status.write() = status;
    }

    /// The labels the co-hosted pool advertises; empty when none published.
    pub(super) fn pool_labels(&self) -> Vec<String> {
        self.pool_status.read().labels()
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
    pub(crate) async fn ensure_key_fingerprint(
        &self,
        fingerprint: &str,
    ) -> Result<(), ControlError> {
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
    /// `''` until the trait passes both.
    /// Allocate the next run number for a workflow:
    /// `INSERT .. ON CONFLICT DO UPDATE SET last_run_number = last_run_number
    /// + 1 RETURNING last_run_number`, scoped by the
    /// `workflow_run_numbers` primary key (core's trait signature takes
    /// `(namespace_id, repository, workflow_path)`).
    pub(crate) async fn allocate_run_number(
        &self,
        namespace_id: &str,
        repository: &str,
        workflow_path: &str,
    ) -> Result<u64, ControlError> {
        self.write(|tx| submit::allocate_run_number_tx(tx, namespace_id, repository, workflow_path))
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
