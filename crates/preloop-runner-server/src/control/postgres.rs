//! The Postgres `ControlBackend`: the shared-node authority.
//!
//! Same contract as [`crate::control::sqlite`]: every command is one
//! transaction [`load_txstate`] builds the working set, the
//! shared [`crate::control::commands`] logic runs on it, [`write_txstate`]
//! persists the delta, `COMMIT`. The database is the serialization and
//! fencing boundary; a pool of connections serves concurrent commands, so
//! this backend scales past SQLite's single writer.
//!
//! The SQL is the Postgres dialect of [`crate::control::schema::POSTGRES_DDL`]
//! `$N` placeholders, `BYTEA` blobs, `BIGSERIAL`
//! sequences. Every table lives in the `control` schema (the analogue of the
//! separate `control.db` file): `POSTGRES_DDL` sets `search_path TO control`
//! on this connection, so the unqualified names below resolve to `control.*`
//! and never collide with `store_pg`'s `public` tables. A future connection
//! pool must set `search_path` on every connection it hands out.
//!
//! The behavioral suite runs the identical scenarios against both backends
//! and expects identical results.

use super::backend::*;
use super::commands;
use super::sched;
use super::schema::{POSTGRES_DDL, POSTGRES_SCHEMA_VERSION};
use super::txstate::{TxScope, TxState};
use super::types::*;
use crate::concurrency;
use crate::models::{
    QueuedJob, RunRecord, TaskAgentJobRequestRecord, WebhookDeliveryRecord, WebhookDeliveryStatus,
    WebhookDeliverySummary, WebhookQueueStats, WebhookRedeliveryRecord, WebhookWatchdogCursor,
};
use crate::state::JobSetId;
use crate::store;
use crate::store::Store as _;
use preloop_gha_protocol::crypto::{AgentRsaPublicKey, SessionEncryption};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId, SessionId};
use std::collections::BTreeSet;
use tokio_postgres::{Client, NoTls};

/// The Postgres control backend. `client` is the single writer behind a
/// mutex; `readers` is a pool of connections that serve read-only commands
/// concurrently, so a read never queues behind a write. (A `deadpool` pool
/// is the production upgrade; the channel pool here gives the same
/// read/write separation without a new dependency.)
pub(crate) struct PostgresBackend {
    writers_tx: tokio::sync::mpsc::Sender<Client>,
    writers_rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Client>>,
    /// Read connections handed out by [`PostgresBackend::read`].
    readers_tx: tokio::sync::mpsc::Sender<Client>,
    readers_rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Client>>,
    /// AEAD envelope sealing every `*_blob` column — identical to the SQLite
    /// backend so a read replica or `SELECT` yields ciphertext, not secrets.
    cipher: store::Envelope,
    /// Auxiliary SQL adapter over the same PostgreSQL `control` schema.
    aux: crate::store_pg::PgStore,
    pool_assignments_enabled: std::sync::atomic::AtomicBool,
    require_job_assignments: std::sync::atomic::AtomicBool,
    runner_liveness_timeout: std::sync::atomic::AtomicU64,
    /// Cross-node wake-ups from the `LISTEN` connection.
    wakes: tokio::sync::broadcast::Sender<super::wake::Wake>,
}

/// Run `f` without stalling the async executor. On a multi-thread runtime
/// `block_in_place` hands the blocking section a spare worker so a heavy
/// command body cannot starve concurrent requests; on a `current_thread`
/// runtime (unit tests) `f` runs inline — `block_in_place` would panic.
fn run_blocking<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

/// Advisory-lock key serializing control-plane writers across every process
/// sharing the database. Any `i64` works; it only has to be identical for all
/// engines and distinct from application-level advisory locks. `xact` scope
/// means the lock is released automatically on `COMMIT`/`ROLLBACK`.
const POSTGRES_WRITER_LOCK_KEY: i64 = 0x0070_7265_6c6f_6f70; // "preloop"
/// Session advisory lock serializing schema setup across booting nodes.
const SCHEMA_SETUP_LOCK_KEY: i64 = 0x0070_7265_6c73_6368; // "prelsch"

fn run_lock_key(run_id: &RunId) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    run_id.hash(&mut hasher);
    let h = hasher.finish() as i64;
    if h == POSTGRES_WRITER_LOCK_KEY {
        h.wrapping_add(1)
    } else {
        h
    }
}

impl PostgresBackend {
    /// Connect, migrate to the current schema version, and return the
    /// backend. `url` is a `postgres://` connection string; TLS is applied
    /// when the URL's `sslmode` asks for it (see `store_pg::tls_connector`).
    pub(crate) async fn connect(
        url: &str,
        cipher: store::Envelope,
        pool_assignments_enabled: bool,
        require_job_assignments: bool,
        runner_liveness_timeout: std::time::Duration,
    ) -> Result<Self, ControlError> {
        let client = connect_one(url).await?;
        // Several engine nodes may boot against one database at once, and
        // `CREATE … IF NOT EXISTS` is not race-safe in Postgres (the loser
        // fails on `pg_type_typname_nsp_index`). Serialize schema setup on a
        // session advisory lock; it is released right after.
        client
            .batch_execute(&format!("SELECT pg_advisory_lock({SCHEMA_SETUP_LOCK_KEY})"))
            .await
            .map_err(ControlError::backend)?;
        let setup = async {
            // Ensure the bookkeeping schema/table exist before probing the
            // version (a fresh database has neither).
            client
                .batch_execute(
                    "CREATE SCHEMA IF NOT EXISTS control; \
                     CREATE TABLE IF NOT EXISTS control.schema_migrations ( \
                         version BIGINT PRIMARY KEY, \
                         applied_at TIMESTAMPTZ NOT NULL DEFAULT now() \
                     )",
                )
                .await
                .map_err(ControlError::backend)?;
            let versions: Vec<i64> = client
                .query("SELECT version FROM control.schema_migrations", &[])
                .await
                .map_err(ControlError::backend)?
                .iter()
                .map(|row| row.get(0))
                .collect();
            if let Some(other) = versions.iter().find(|v| **v != POSTGRES_SCHEMA_VERSION) {
                return Err(ControlError::backend(anyhow::anyhow!(
                    "control schema has version {other}; this build supports only \
                     {POSTGRES_SCHEMA_VERSION}. Recreate the database."
                )));
            }
            client
                .batch_execute(POSTGRES_DDL)
                .await
                .map_err(ControlError::backend)?;
            client
                .execute(
                    "INSERT INTO schema_migrations (version) VALUES ($1) \
                     ON CONFLICT (version) DO NOTHING",
                    &[&POSTGRES_SCHEMA_VERSION],
                )
                .await
                .map_err(ControlError::backend)?;
            Ok(())
        }
        .await;
        client
            .batch_execute(&format!(
                "SELECT pg_advisory_unlock({SCHEMA_SETUP_LOCK_KEY})"
            ))
            .await
            .map_err(ControlError::backend)?;
        setup?;
        // A pool of writer connections replacing the single Mutex<Client>.
        let (writers_tx, writers_rx) = tokio::sync::mpsc::channel(4);
        writers_tx
            .send(client)
            .await
            .map_err(|_| ControlError::backend(anyhow::anyhow!("writer pool closed")))?;
        for _ in 1..4 {
            writers_tx
                .send(connect_one(url).await?)
                .await
                .map_err(|_| ControlError::backend(anyhow::anyhow!("writer pool closed")))?;
        }
        // A small pool of read connections. Read-only commands check one
        // out and run a `BEGIN` read transaction, so a read never queues
        // behind writes.
        let (readers_tx, readers_rx) = tokio::sync::mpsc::channel(4);
        for _ in 0..4 {
            readers_tx
                .send(connect_one(url).await?)
                .await
                .map_err(|_| ControlError::backend(anyhow::anyhow!("reader pool closed")))?;
        }
        let aux = crate::store_pg::PgStore::open_existing(url, cipher.clone())
            .await
            .map_err(ControlError::backend)?;
        Ok(Self {
            writers_tx,
            writers_rx: tokio::sync::Mutex::new(writers_rx),
            readers_tx,
            readers_rx: tokio::sync::Mutex::new(readers_rx),
            cipher,
            aux,
            wakes: super::wake::spawn_listener(url.to_owned()),
            pool_assignments_enabled: std::sync::atomic::AtomicBool::new(pool_assignments_enabled),
            require_job_assignments: std::sync::atomic::AtomicBool::new(require_job_assignments),
            runner_liveness_timeout: std::sync::atomic::AtomicU64::new(
                runner_liveness_timeout.as_nanos() as u64,
            ),
        })
    }

    /// Subscribe to wake-ups committed by any node on this database.
    pub(crate) fn subscribe_wakes(&self) -> tokio::sync::broadcast::Receiver<super::wake::Wake> {
        self.wakes.subscribe()
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

    /// Update the scheduling config after connect. Bootstrap applies the
    /// real server config here once it is known — the values passed to
    /// `connect` are only the recovered defaults.
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

    /// Run one command as a transaction: `BEGIN`, load the working set,
    /// run `f`, write the delta back, `COMMIT`. A failure before commit
    /// rolls the whole command back.
    pub(crate) async fn transact<T>(
        &self,
        f: impl FnOnce(&mut TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        self.transact_scoped(&TxScope::full(), f).await
    }

    /// `transact` under a scope — loads only `scope`, writes back only the
    /// loaded rows. See [`TxScope`] for the always-global concurrency rule.
    async fn checkout_writer(&self) -> Result<Client, ControlError> {
        self.writers_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| ControlError::backend(anyhow::anyhow!("writer pool closed")))
    }

    async fn return_writer(&self, client: Client) {
        let _ = self.writers_tx.send(client).await;
    }

    pub(crate) async fn transact_scoped<T>(
        &self,
        scope: &TxScope,
        f: impl FnOnce(&mut TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        if scope.include_archived {
            return Err(ControlError::BadRequest(
                "history scope is read-only".into(),
            ));
        }
        let mut client = self.checkout_writer().await?;
        let result = self.transact_on(&mut client, scope, None, f).await;
        self.return_writer(client).await;
        result
    }

    /// A poll's claim transaction: concurrent with other polls, exclusive of
    /// global transactions; see [`lock_poll_candidates`].
    async fn transact_poll<T>(
        &self,
        scope: &TxScope,
        runner: &crate::models::RunnerCapabilities,
        f: impl FnOnce(&mut TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        let mut client = self.checkout_writer().await?;
        let result = self.transact_on(&mut client, scope, Some(runner), f).await;
        self.return_writer(client).await;
        result
    }

    async fn transact_on<T>(
        &self,
        client: &mut Client,
        scope: &TxScope,
        poll_runner: Option<&crate::models::RunnerCapabilities>,
        f: impl FnOnce(&mut TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        let started = std::time::Instant::now();
        let caller = crate::control::txn_stats::caller_of(&f);
        let txn = client.transaction().await.map_err(ControlError::backend)?;
        let is_run_scoped = scope.runs.as_ref().is_some_and(|runs| {
            !runs.is_empty() && !scope.ready_queue && !scope.blocked_jobs && !scope.concurrency
        });
        let mut poll_keys = None;
        if let Some(runner) = poll_runner {
            // Polls run concurrently with each other (shared) and never with a
            // global transaction (exclusive). Each claims under the run lock of
            // its candidates, taken without waiting, so a poll never blocks
            // while holding a lock another transaction needs.
            txn.batch_execute(&format!(
                "SELECT pg_advisory_xact_lock_shared({POSTGRES_WRITER_LOCK_KEY})"
            ))
            .await
            .map_err(ControlError::backend)?;
            // Assignment modes may still refuse the head match
            // (`claim_permitted`), so they hold a few spare candidates.
            let assignments = self
                .pool_assignments_enabled
                .load(std::sync::atomic::Ordering::Relaxed)
                || self
                    .require_job_assignments
                    .load(std::sync::atomic::Ordering::Relaxed);
            let want = if assignments { 4 } else { 1 };
            poll_keys = Some(lock_poll_candidates(&txn, runner, want).await?);
        } else if is_run_scoped {
            lock_runs(&txn, scope.runs.as_ref().unwrap().iter()).await?;
        } else {
            txn.batch_execute(&format!(
                "SELECT pg_advisory_xact_lock({POSTGRES_WRITER_LOCK_KEY})"
            ))
            .await
            .map_err(ControlError::backend)?;
        }
        let locked = started.elapsed();
        // A global transaction also locks every run it loads (see
        // `load_txstate`), so it never writes a run a run-scoped writer holds.
        let lock_loaded_runs = poll_runner.is_none() && !is_run_scoped;
        let (tx, effective_scope) = load_txstate(
            &txn,
            scope,
            &self.cipher,
            poll_keys.as_deref(),
            lock_loaded_runs,
        )
        .await?;
        let loaded = started.elapsed();
        let mut tx = tx.with_config(self.config());
        let result = run_blocking(|| f(&mut tx))?;
        let decided = started.elapsed();
        write_txstate(&txn, &tx, &effective_scope, &self.cipher).await?;
        // Cross-node wake-up: delivered to every node's listener only if this
        // transaction commits. Newly ready jobs wake that many runners; new
        // cancellations wake everyone (the owning runner must see it).
        let new_ready = tx.queue.len();
        let new_cancels = tx
            .cancellation_queue
            .len()
            .saturating_sub(tx.loaded.cancellations.len());
        if new_ready > 0 || new_cancels > 0 {
            let payload = super::wake::Wake {
                ready: new_ready,
                broadcast: new_cancels > 0,
            }
            .encode();
            txn.execute(
                "SELECT pg_notify($1, $2)",
                &[&super::wake::CHANNEL, &payload],
            )
            .await
            .map_err(ControlError::backend)?;
        }
        let written = started.elapsed();
        txn.commit().await.map_err(ControlError::backend)?;
        let total = started.elapsed();
        crate::control::txn_stats::record(
            caller,
            is_run_scoped,
            locked,
            loaded - locked,
            decided - loaded,
            written - decided,
            total - written,
        );
        if total > std::time::Duration::from_millis(250) {
            tracing::warn!(
                caller,
                run_scoped = is_run_scoped,
                lock_ms = locked.as_millis() as u64,
                load_ms = (loaded - locked).as_millis() as u64,
                decide_ms = (decided - loaded).as_millis() as u64,
                write_ms = (written - decided).as_millis() as u64,
                commit_ms = (total - written).as_millis() as u64,
                "slow control transaction"
            );
        }
        Ok(result)
    }

    /// Run a read-only command on a pooled reader connection — concurrent
    /// with the writer, never queued behind it. `f` sees one consistent
    /// snapshot (a `BEGIN` read transaction, rolled back after the read).
    pub(crate) async fn read<T>(
        &self,
        f: impl FnOnce(&TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        self.read_scoped(&TxScope::full(), f).await
    }

    /// `read` under a scope — loads only `scope` on the reader snapshot.
    pub(crate) async fn read_scoped<T>(
        &self,
        scope: &TxScope,
        f: impl FnOnce(&TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        let mut client = self
            .readers_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| ControlError::backend(anyhow::anyhow!("reader pool closed")))?;
        let result = self.read_on(&mut client, scope, f).await;
        let _ = self.readers_tx.send(client).await;
        result
    }

    /// Test-only: read one job's `(status, queue_kind, queue_position, seq)`
    /// straight from the `jobs` table, bypassing `TxState`. Used by the
    /// transition regression tests to assert exact FIFO/status invariants.
    #[cfg(test)]
    pub(crate) async fn job_row(&self, job_id: &str) -> Option<(String, String, Option<i64>, i64)> {
        let client = self.checkout_reader().await.ok()?;
        let result = client
            .query_opt(
                "SELECT status, queue_kind, queue_position, seq FROM jobs WHERE job_id=$1",
                &[&job_id],
            )
            .await
            .ok()
            .flatten()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)));
        self.return_reader(client).await;
        result
    }

    /// Test-only: every ready job's `queue_position`, keyed by `job_id`.
    /// Used by the FIFO regression test to assert a new job takes the global
    /// MAX+1 and existing rows keep their exact positions.
    #[cfg(test)]
    pub(crate) async fn ready_positions(&self) -> std::collections::BTreeMap<String, i64> {
        let Ok(client) = self.checkout_reader().await else {
            return Default::default();
        };
        let result = client
            .query(
                "SELECT job_id, queue_position FROM jobs WHERE queue_kind='ready'",
                &[],
            )
            .await
            .unwrap_or_default()
            .iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, i64>(1)))
            .collect();
        self.return_reader(client).await;
        result
    }

    /// The read body: one consistent snapshot on `client`, rolled back.
    async fn read_on<T>(
        &self,
        client: &mut Client,
        scope: &TxScope,
        f: impl FnOnce(&TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        let txn = client.transaction().await.map_err(ControlError::backend)?;
        let (tx, _effective_scope) = load_txstate(&txn, scope, &self.cipher, None, false).await?;
        let tx = tx.with_config(self.config());
        let result = run_blocking(|| f(&tx))?;
        txn.rollback().await.map_err(ControlError::backend)?;
        Ok(result)
    }

    /// Check out a pooled reader connection. Pair with [`Self::return_reader`].
    /// Read-only point lookups go through here so they never queue behind the
    /// single writer.
    async fn checkout_reader(&self) -> Result<Client, ControlError> {
        self.readers_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| ControlError::backend(anyhow::anyhow!("reader pool closed")))
    }

    /// Return a reader checked out by [`Self::checkout_reader`].
    async fn return_reader(&self, client: Client) {
        let _ = self.readers_tx.send(client).await;
    }

    /// Find the run a webhook delivery already produced, by its durable
    /// `(delivery_id, workflow_path)` dedup key. O(1) via `runs_delivery` —
    /// the submit path calls this before `transact_scoped` so the replay
    /// check never scans the whole `runs` table.
    pub(crate) async fn find_run_by_delivery(
        &self,
        delivery_id: &str,
        workflow_path: &str,
    ) -> Result<Option<RunRecord>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = async {
            let Some((_, _, mut run, _)) = load_runs(
                &client,
                &self.cipher,
                " WHERE webhook_delivery_id = $1 AND workflow_path = $2",
                &[&delivery_id, &workflow_path],
            )
            .await?
            .pop() else {
                return Ok(None);
            };
            let run_id_s = run.run_id.0.to_string();
            let rows = client
                .query(
                    "SELECT job_id, status FROM jobs WHERE run_id = $1",
                    &[&run_id_s],
                )
                .await
                .map_err(ControlError::backend)?;
            for row in rows {
                let job_id_s: String = row.get(0);
                let status_s: String = row.get(1);
                run.jobs.insert(JobId(job_id_s), status_parse(&status_s));
            }
            Ok(Some(run))
        }
        .await;
        self.return_reader(client).await;
        result
    }

    /// Resolve a request's `(request_id, run_id)` from its `agent_job_id`.
    /// O(1) via `job_requests_agent` — the broker complete path calls this
    /// before `transact_scoped` so the working set stays narrow.
    pub(crate) async fn find_request_by_agent_job_id(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<(i64, RunId)>, ControlError> {
        let client = self.checkout_reader().await?;
        let agent_job_id_s = agent_job_id.to_string();
        let result = client
            .query_opt(
                "SELECT request_id, run_id FROM job_requests WHERE agent_job_id = $1",
                &[&agent_job_id_s],
            )
            .await
            .map_err(ControlError::backend)
            .map(|opt| {
                opt.map(|r| {
                    let request_id: i64 = r.get(0);
                    let run_id_s: String = r.get(1);
                    (request_id, parse_run_id(&run_id_s))
                })
            });
        self.return_reader(client).await;
        result
    }
    /// The session that owns the request for `agent_job_id`. `complete_job`
    /// resolves this before `transact_scoped` so `settle_request` can drop
    /// the moot cancellation from the owner session's inflight messages.
    pub(crate) async fn find_session_by_agent_job_id(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<String>, ControlError> {
        let client = self.checkout_reader().await?;
        let agent_job_id_s = agent_job_id.to_string();
        let result = client
            .query_opt(
                "SELECT rs.session_id FROM runner_sessions rs \
                 JOIN job_requests jr ON jr.request_id = rs.active_request_id \
                 WHERE jr.agent_job_id = $1",
                &[&agent_job_id_s],
            )
            .await
            .map_err(ControlError::backend)
            .map(|opt| opt.map(|r| r.get::<_, String>(0)));
        self.return_reader(client).await;
        result
    }
    /// moot cancellations from each owner session's inflight messages.
    pub(crate) async fn find_sessions_by_run(
        &self,
        run_id: RunId,
    ) -> Result<BTreeSet<String>, ControlError> {
        let client = self.checkout_reader().await?;
        let run_id_s = run_id.0.to_string();
        let result = client
            .query(
                "SELECT DISTINCT rs.session_id FROM runner_sessions rs \
                 JOIN job_requests jr ON jr.request_id = rs.active_request_id \
                 WHERE jr.run_id = $1",
                &[&run_id_s],
            )
            .await
            .map_err(ControlError::backend)
            .map(|rows| rows.iter().map(|r| r.get::<_, String>(0)).collect());
        self.return_reader(client).await;
        result
    }

    /// `(run_id, owner_session)` for `request_id`. `acquire_context` resolves
    /// this before `read_scoped` so the read stays narrow.
    pub(crate) async fn find_request_context(
        &self,
        request_id: i64,
    ) -> Result<Option<(RunId, Option<String>)>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query_opt(
                "SELECT jr.run_id, rs.session_id FROM job_requests jr \
                 LEFT JOIN runner_sessions rs ON rs.active_request_id = jr.request_id \
                 WHERE jr.request_id = $1",
                &[&request_id],
            )
            .await
            .map_err(ControlError::backend)
            .map(|opt| {
                opt.map(|r| {
                    let run_id_s: String = r.get(0);
                    let session_id: Option<String> = r.get(1);
                    (parse_run_id(&run_id_s), session_id)
                })
            });
        self.return_reader(client).await;
        result
    }
    /// Query run summaries without materializing `TxState`.
    async fn list_run_rows(&self, filter: RunListFilter) -> Result<Vec<RunRecord>, ControlError> {
        let client = self.checkout_reader().await?;
        let limit = filter.limit.min(200) as i64;
        let result = async {
            let rows = client
                .query(
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
                         WHERE ($1::TEXT IS NULL OR POSITION($1 IN workflow_path) > 0)
                           AND ($2::TEXT IS NULL OR status = $2)
                           AND ($3::TEXT IS NULL OR event = $3)
                         ORDER BY terminal_rank, sort_at DESC LIMIT $4
                     ) AS selected
                     LEFT JOIN jobs j ON j.run_id=selected.run_id AND selected.archived_at_us IS NULL
                     LEFT JOIN job_history h ON h.run_id=selected.run_id
                         AND h.run_attempt=selected.run_attempt
                         AND h.run_created_at_us=selected.created_at_us
                         AND selected.archived_at_us IS NOT NULL
                     ORDER BY selected.terminal_rank, selected.sort_at DESC,
                              COALESCE(j.job_id,h.job_id)",
                    &[&filter.workflow, &filter.status, &filter.event, &limit],
                )
                .await
                .map_err(ControlError::backend)?;
            // Steps for each job's latest attempt, live or archived, in one
            // batched read instead of one decode per job.
            let attempts: Vec<String> = rows
                .iter()
                .filter_map(|row| row.get::<_, Option<String>>(4))
                .collect();
            let mut steps: std::collections::BTreeMap<uuid::Uuid, Vec<crate::models::StepRecord>> =
                std::collections::BTreeMap::new();
            if !attempts.is_empty() {
                let sql = format!(
                    "SELECT {STEP_COLUMNS} FROM (
                         SELECT {STEP_COLUMNS}, position FROM job_steps
                         WHERE agent_job_id = ANY($1)
                         UNION ALL
                         SELECT {STEP_COLUMNS}, position FROM step_history
                         WHERE agent_job_id = ANY($1)) s
                     ORDER BY agent_job_id, position"
                );
                for row in client
                    .query(&sql, &[&attempts])
                    .await
                    .map_err(ControlError::backend)?
                {
                    let (agent, record) = step_from_row(&row);
                    steps.entry(agent).or_default().push(record);
                }
            }
            // The selected runs' records, assembled in one batched read.
            let mut run_ids: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
            run_ids.dedup();
            let mut records: std::collections::HashMap<String, RunRecord> =
                load_runs(&client, &self.cipher, " WHERE run_id = ANY($1)", &[&run_ids])
                    .await?
                    .into_iter()
                    .map(|(run_id, _, record, _)| (run_id.0.to_string(), record))
                    .collect();
            let mut runs = Vec::new();
            let mut current_id: Option<String> = None;
            let mut current_run: Option<RunRecord> = None;
            let mut current_jobs = Vec::new();
            for row in rows {
                let run_id: String = row.get(0);
                if current_id.as_deref() != Some(run_id.as_str()) {
                    if let Some(run) = current_run.take() {
                        runs.push(project_run_rows(run, std::mem::take(&mut current_jobs)));
                    }
                    current_run = records.remove(&run_id);
                    current_id = Some(run_id);
                }
                let job_id: Option<String> = row.get(1);
                let status: Option<String> = row.get(2);
                let queue_kind: Option<String> = row.get(3);
                let agent: Option<String> = row.get(4);
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
        }
        .await;
        self.return_reader(client).await;
        result
    }

    /// Append an event without reading or rewriting its run.
    async fn append_event_row(
        &self,
        event: &preloop_gha_protocol::NdjsonEvent,
    ) -> Result<(), ControlError> {
        let run_id = event_run_id(event).map(|id| id.0.to_string());
        let event_blob = unblob(&self.cipher, event)?;
        let client = self.checkout_writer().await?;
        let res = client
            .execute(
                "INSERT INTO control_events(run_id, event_blob, created_at_us) \
                 VALUES ($1, $2, $3)",
                &[
                    &run_id,
                    &event_blob,
                    &system_to_us(std::time::SystemTime::now()),
                ],
            )
            .await
            .map_err(ControlError::backend);
        self.return_writer(client).await;
        res?;
        Ok(())
    }
    async fn terminal_job_rows(&self) -> Result<BTreeSet<(RunId, JobId)>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query(
                "SELECT run_id, job_id FROM jobs
                 WHERE status IN ('success','failure','skipped','cancelled')
                 UNION ALL SELECT h.run_id,h.job_id FROM job_history h
                 JOIN runs r ON r.run_id=h.run_id
                 WHERE h.run_attempt=r.run_attempt AND h.run_created_at_us=r.created_at_us
                   AND h.status IN ('success','failure','skipped','cancelled')",
                &[],
            )
            .await
            .map_err(ControlError::backend)
            .map(|rows| {
                rows.into_iter()
                    .map(|row| {
                        (
                            parse_run_id(&row.get::<_, String>(0)),
                            JobId(row.get::<_, String>(1)),
                        )
                    })
                    .collect()
            });
        self.return_reader(client).await;
        result
    }
}

/// Open one connection and spawn its driver task. `Connection` is generic
/// over the TLS stream, so each arm drives its own task; only the
/// `Client` (non-generic) escapes.
async fn connect_one(url: &str) -> Result<Client, ControlError> {
    let connect_url = crate::store_pg::connect_url(url);
    // Every connection resolves unqualified names to `control.*` — the
    // writer gets it again via `POSTGRES_DDL`, the readers only here.
    let client = match crate::store_pg::tls_connector(url).map_err(ControlError::backend)? {
        Some(tls) => {
            let (client, connection) = tokio_postgres::connect(&connect_url, tls)
                .await
                .map_err(ControlError::backend)?;
            tokio::spawn(async move {
                if let Err(error) = connection.await {
                    tracing::warn!(%error, "postgres control connection closed");
                }
            });
            client
        }
        None => {
            let (client, connection) = tokio_postgres::connect(&connect_url, NoTls)
                .await
                .map_err(ControlError::backend)?;
            tokio::spawn(async move {
                if let Err(error) = connection.await {
                    tracing::warn!(%error, "postgres control connection closed");
                }
            });
            client
        }
    };
    client
        .batch_execute("SET search_path TO control")
        .await
        .map_err(ControlError::backend)?;
    Ok(client)
}

// ─────────────────────────────────────────────────────────────────────────
// Load: rows → TxState
// ─────────────────────────────────────────────────────────────────────────

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

fn parse_run_id(s: &str) -> RunId {
    RunId(s.parse().unwrap_or_default())
}

fn parse_uuid(s: &str) -> uuid::Uuid {
    s.parse().unwrap_or_default()
}

/// Build a run-scope `WHERE` clause + bind param for `= ANY($1)` pushdown.
/// `runs: None` → no filter (load all); `Some(set)` → `WHERE <col> = ANY($1)`
/// with the run-id strings to bind. An empty set yields `WHERE false`.
/// Mirrors sqlite's `scoped_select` so both backends read the same rows.
fn run_scope_clause(
    col: &str,
    runs: Option<&std::collections::BTreeSet<RunId>>,
) -> (String, Option<Vec<String>>) {
    match runs {
        None => (String::new(), None),
        Some(set) if set.is_empty() => (" WHERE false".to_owned(), None),
        Some(set) => (
            format!(" WHERE {col} = ANY($1)"),
            Some(set.iter().map(|id| id.0.to_string()).collect()),
        ),
    }
}

/// Bind the optional run-id array from `run_scope_clause` into a query param
/// list. `None` (no filter) binds nothing.
fn scope_params(ids: &Option<Vec<String>>) -> Vec<&(dyn tokio_postgres::types::ToSql + Sync)> {
    ids.iter()
        .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect()
}

type Tx<'a> = tokio_postgres::Transaction<'a>;

/// Columns every step read selects, in [`step_from_row`] order. The same
/// list serves `job_steps` and `step_history`.
const STEP_COLUMNS: &str = "agent_job_id, step_id, kind, workflow_index, runner_number, \
     context_name, name, conclusion, started_at_us, finished_at_us";

/// Decode one row selected with [`STEP_COLUMNS`].
fn step_from_row(row: &tokio_postgres::Row) -> (uuid::Uuid, crate::models::StepRecord) {
    let agent: String = row.get(0);
    let kind: String = row.get(2);
    (
        parse_uuid(&agent),
        super::rows::StepRow::into_record(
            row.get(1),
            &kind,
            row.get(3),
            row.get(4),
            row.get(5),
            row.get(6),
            row.get(7),
            row.get(8),
            row.get(9),
        ),
    )
}

/// Apply one attempt's step delta: upsert changed rows, delete removed ids.
async fn write_step_delta(
    conn: &Tx<'_>,
    agent_job_id: &uuid::Uuid,
    delta: &super::rows::StepDelta,
) -> Result<(), ControlError> {
    let agent = agent_job_id.to_string();
    for id in &delta.deletes {
        conn.execute(
            "DELETE FROM job_steps WHERE agent_job_id=$1 AND step_id=$2",
            &[&agent, id],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    for row in &delta.upserts {
        conn.execute(
            // Only while the attempt exists: `FOR KEY SHARE` waits out a
            // concurrent delete of the attempt and then inserts nothing.
            "INSERT INTO job_steps (agent_job_id, step_id, position, kind, workflow_index, \
             runner_number, context_name, name, conclusion, started_at_us, finished_at_us) \
             SELECT $1::text, $2::text, $3::bigint, $4::text, $5::bigint, $6::bigint, $7::text, $8::text, $9::text, $10::bigint, $11::bigint \
             WHERE EXISTS (SELECT 1 FROM job_requests WHERE agent_job_id=$1 FOR KEY SHARE) \
             ON CONFLICT(agent_job_id, step_id) DO UPDATE SET position=excluded.position, \
             kind=excluded.kind, workflow_index=excluded.workflow_index, \
             runner_number=excluded.runner_number, context_name=excluded.context_name, \
             name=excluded.name, conclusion=excluded.conclusion, \
             started_at_us=excluded.started_at_us, finished_at_us=excluded.finished_at_us",
            &[
                &agent,
                &row.step_id,
                &row.position,
                &row.kind,
                &row.workflow_index,
                &row.runner_number,
                &row.context_name,
                &row.name,
                &row.conclusion,
                &row.started_at_us,
                &row.finished_at_us,
            ],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    Ok(())
}

/// Load run records for the runs matching `filter` (a `WHERE …` clause over
/// `runs` binding `params`, or empty for all) from their decomposed tables.
/// Returns each run's namespace, its assembled record (the `jobs` status map
/// empty — callers rebuild it from `jobs`) and the row snapshot write-back
/// diffs against.
async fn load_runs<C: tokio_postgres::GenericClient + Sync>(
    conn: &C,
    cipher: &store::Envelope,
    filter: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<Vec<(RunId, String, RunRecord, super::rows::RunParts)>, ControlError> {
    use super::rows::{RunBaseJobRow, RunJobRow, RunParts, RunScalars, RunSubmissionRow};
    let default_submission = RunSubmissionRow {
        submission_json:
            serde_json::to_string(&preloop_gha_protocol::WorkflowSubmission::default())
                .map_err(ControlError::backend)?,
        secrets: std::collections::BTreeMap::new(),
        github_json: "null".to_owned(),
        workspace_snapshot_json: None,
    };
    let mut parts: std::collections::BTreeMap<String, (String, RunParts)> =
        std::collections::BTreeMap::new();
    for r in conn
        .query(
            &format!(
                "SELECT run_id, namespace, status, run_number, run_attempt, run_name, event, \
                 workflow_path, conclusion, webhook_delivery_id, head_sha, workflow_ref, \
                 push_state_json, snapshot_timing_json, created_at_us, started_at_us, \
                 completed_at_us FROM runs{filter}"
            ),
            params,
        )
        .await
        .map_err(ControlError::backend)?
    {
        let status: String = r.get(2);
        parts.insert(
            r.get(0),
            (
                r.get(1),
                RunParts {
                    scalars: RunScalars {
                        status: status_parse(&status),
                        run_number: r.get(3),
                        run_attempt: r.get(4),
                        run_name: r.get(5),
                        event: r.get(6),
                        workflow_path: r.get(7),
                        conclusion: r.get(8),
                        webhook_delivery_id: r.get(9),
                        head_sha: r.get(10),
                        workflow_ref: r.get(11),
                        push_state_json: r.get(12),
                        snapshot_timing_json: r.get(13),
                        created_at_us: r.get(14),
                        started_at_us: r.get(15),
                        completed_at_us: r.get(16),
                    },
                    // Pre-v16 runs have no submission row: they load with
                    // an empty submission rather than failing.
                    submission: default_submission.clone(),
                    jobs: std::collections::BTreeMap::new(),
                    base_jobs: std::collections::BTreeMap::new(),
                    reusable_calls: std::collections::BTreeMap::new(),
                },
            ),
        );
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
    for r in conn
        .query(
            &child(
                "run_submissions",
                "submission_json, secrets_blob, github_json, workspace_snapshot_json",
            ),
            params,
        )
        .await
        .map_err(ControlError::backend)?
    {
        let run_id: String = r.get(0);
        let Some((_, part)) = parts.get_mut(&run_id) else {
            continue;
        };
        let secrets: Vec<u8> = r.get(2);
        part.submission = RunSubmissionRow {
            submission_json: r.get(1),
            secrets: blob(cipher, &secrets)?,
            github_json: r.get(3),
            workspace_snapshot_json: r.get(4),
        };
    }
    for r in conn
        .query(
            &child(
                "run_jobs",
                "job_id, base_id, display_name, needs_json, outputs_json, check_run_id, \
                 detail_json, detail_position, caller_plan_json",
            ),
            params,
        )
        .await
        .map_err(ControlError::backend)?
    {
        let run_id: String = r.get(0);
        let Some((_, part)) = parts.get_mut(&run_id) else {
            continue;
        };
        part.jobs.insert(
            r.get(1),
            RunJobRow {
                base_id: r.get(2),
                display_name: r.get(3),
                needs_json: r.get(4),
                outputs_json: r.get(5),
                check_run_id: r.get(6),
                detail_json: r.get(7),
                detail_position: r.get(8),
                caller_plan_json: r.get(9),
            },
        );
    }
    for r in conn
        .query(
            &child("run_base_jobs", "base_id, fail_fast, continue_on_error"),
            params,
        )
        .await
        .map_err(ControlError::backend)?
    {
        let run_id: String = r.get(0);
        let Some((_, part)) = parts.get_mut(&run_id) else {
            continue;
        };
        let fail_fast: Option<i64> = r.get(2);
        let continue_on_error: Option<i64> = r.get(3);
        part.base_jobs.insert(
            r.get(1),
            RunBaseJobRow {
                fail_fast: fail_fast.map(|v| v != 0),
                continue_on_error: continue_on_error.map(|v| v != 0),
            },
        );
    }
    for r in conn
        .query(
            &child("reusable_calls", "caller_job_id, metadata_json"),
            params,
        )
        .await
        .map_err(ControlError::backend)?
    {
        let run_id: String = r.get(0);
        let Some((_, part)) = parts.get_mut(&run_id) else {
            continue;
        };
        part.reusable_calls.insert(r.get(1), r.get(2));
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
async fn write_run(
    conn: &Tx<'_>,
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
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) \
             ON CONFLICT(run_id) DO UPDATE SET status=excluded.status, \
             run_name=excluded.run_name, conclusion=excluded.conclusion, \
             head_sha=excluded.head_sha, workflow_ref=excluded.workflow_ref, \
             push_state_json=excluded.push_state_json, \
             snapshot_timing_json=excluded.snapshot_timing_json, \
             started_at_us=excluded.started_at_us, completed_at_us=excluded.completed_at_us",
            &[
                &run,
                &namespace,
                &status_str(s.status),
                &s.run_number,
                &s.run_attempt,
                &s.run_name,
                &s.event,
                &s.workflow_path,
                &s.conclusion,
                &s.webhook_delivery_id,
                &s.head_sha,
                &s.workflow_ref,
                &s.push_state_json,
                &s.snapshot_timing_json,
                &s.created_at_us,
                &s.started_at_us,
                &s.completed_at_us,
            ],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    if prev.map(|p| &p.submission) != Some(&next.submission) {
        let s = &next.submission;
        conn.execute(
            "INSERT INTO run_submissions (run_id, submission_json, secrets_blob, github_json, \
             workspace_snapshot_json) VALUES ($1,$2,$3,$4,$5) \
             ON CONFLICT(run_id) DO UPDATE SET submission_json=excluded.submission_json, \
             secrets_blob=excluded.secrets_blob, github_json=excluded.github_json, \
             workspace_snapshot_json=excluded.workspace_snapshot_json",
            &[
                &run,
                &s.submission_json,
                &unblob(cipher, &s.secrets)?,
                &s.github_json,
                &s.workspace_snapshot_json,
            ],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    for (job, row) in &next.jobs {
        if prev.and_then(|p| p.jobs.get(job)) == Some(row) {
            continue;
        }
        conn.execute(
            "INSERT INTO run_jobs (run_id, job_id, base_id, display_name, needs_json, \
             outputs_json, check_run_id, detail_json, detail_position, caller_plan_json) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) \
             ON CONFLICT(run_id, job_id) DO UPDATE SET base_id=excluded.base_id, \
             display_name=excluded.display_name, needs_json=excluded.needs_json, \
             outputs_json=excluded.outputs_json, check_run_id=excluded.check_run_id, \
             detail_json=excluded.detail_json, detail_position=excluded.detail_position, \
             caller_plan_json=excluded.caller_plan_json",
            &[
                &run,
                job,
                &row.base_id,
                &row.display_name,
                &row.needs_json,
                &row.outputs_json,
                &row.check_run_id,
                &row.detail_json,
                &row.detail_position,
                &row.caller_plan_json,
            ],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    for (base, row) in &next.base_jobs {
        if prev.and_then(|p| p.base_jobs.get(base)) == Some(row) {
            continue;
        }
        conn.execute(
            "INSERT INTO run_base_jobs (run_id, base_id, fail_fast, continue_on_error) \
             VALUES ($1,$2,$3,$4) ON CONFLICT(run_id, base_id) DO UPDATE SET \
             fail_fast=excluded.fail_fast, continue_on_error=excluded.continue_on_error",
            &[
                &run,
                base,
                &row.fail_fast.map(i64::from),
                &row.continue_on_error.map(i64::from),
            ],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    for (caller, meta) in &next.reusable_calls {
        if prev.and_then(|p| p.reusable_calls.get(caller)) == Some(meta) {
            continue;
        }
        conn.execute(
            "INSERT INTO reusable_calls (run_id, caller_job_id, metadata_json) \
             VALUES ($1,$2,$3) ON CONFLICT(run_id, caller_job_id) DO UPDATE SET \
             metadata_json=excluded.metadata_json",
            &[&run, caller, meta],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    if let Some(prev) = prev {
        for job in prev.jobs.keys().filter(|k| !next.jobs.contains_key(*k)) {
            conn.execute(
                "DELETE FROM run_jobs WHERE run_id=$1 AND job_id=$2",
                &[&run, job],
            )
            .await
            .map_err(ControlError::backend)?;
        }
        for base in prev
            .base_jobs
            .keys()
            .filter(|k| !next.base_jobs.contains_key(*k))
        {
            conn.execute(
                "DELETE FROM run_base_jobs WHERE run_id=$1 AND base_id=$2",
                &[&run, base],
            )
            .await
            .map_err(ControlError::backend)?;
        }
        for caller in prev
            .reusable_calls
            .keys()
            .filter(|k| !next.reusable_calls.contains_key(*k))
        {
            conn.execute(
                "DELETE FROM reusable_calls WHERE run_id=$1 AND caller_job_id=$2",
                &[&run, caller],
            )
            .await
            .map_err(ControlError::backend)?;
        }
    }
    Ok(())
}

/// Persist a job's payload: queryable columns, `needs:` edges and the sealed
/// runner message + `if:` context. Runs after the `jobs` upsert (FK order).
async fn write_job_payload(
    conn: &Tx<'_>,
    cipher: &store::Envelope,
    run_id: &RunId,
    job_id: &JobId,
    job: &QueuedJob,
) -> Result<(), ControlError> {
    let run = run_id.0.to_string();
    let row = super::rows::JobPayloadRow::from_job(job);
    conn.execute(
        "UPDATE jobs SET created_at_ns=$3, deps_ready_at_ns=$4, concurrency_wait_at_ns=$5, \
         concurrency_acquired_at_ns=$6, if_condition=$7, max_parallel=$8, \
         environment_json=$9, concurrency_json=$10, matrix_json=$11, deferred_matrix=$12, \
         reusable_call_json=$13 WHERE run_id=$1 AND job_id=$2",
        &[
            &run,
            &job_id.0,
            &row.created_at_ns,
            &row.deps_ready_at_ns,
            &row.concurrency_wait_at_ns,
            &row.concurrency_acquired_at_ns,
            &row.if_condition,
            &row.max_parallel,
            &row.environment_json,
            &row.concurrency_json,
            &row.matrix_json,
            &row.deferred_matrix,
            &row.reusable_call_json,
        ],
    )
    .await
    .map_err(ControlError::backend)?;
    conn.execute(
        "DELETE FROM job_needs WHERE run_id=$1 AND job_id=$2",
        &[&run, &job_id.0],
    )
    .await
    .map_err(ControlError::backend)?;
    if !row.needs.is_empty() {
        let positions: Vec<i64> = (0..row.needs.len() as i64).collect();
        conn.execute(
            "INSERT INTO job_needs (run_id, job_id, position, needs_job_id) \
             SELECT $1, $2, p, n FROM UNNEST($3::bigint[], $4::text[]) AS e(p, n)",
            &[&run, &job_id.0, &positions, &row.needs],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    conn.execute(
        "INSERT INTO job_messages (run_id, job_id, message_blob, condition_context_blob) \
         VALUES ($1,$2,$3,$4) ON CONFLICT(run_id, job_id) DO UPDATE SET \
         message_blob=excluded.message_blob, \
         condition_context_blob=excluded.condition_context_blob",
        &[
            &run,
            &job_id.0,
            &unblob(cipher, &job.message)?,
            &unblob(cipher, &job.condition_context)?,
        ],
    )
    .await
    .map_err(ControlError::backend)?;
    Ok(())
}

/// Ready jobs a poll may claim, locked for this transaction: walk the queue
/// head in claim order and keep a job only if its run lock is available in
/// shared mode right now (`pg_try_advisory_xact_lock_shared`: polls of one
/// run coexist, a run-scoped writer excludes them) and its row is unlocked
/// (`SKIP LOCKED`: two polls never hold the same job). Neither step waits.
/// Only jobs `runner` can run are considered. Returns at most `want`
/// `(run_id, job_id)`.
async fn lock_poll_candidates(
    conn: &Tx<'_>,
    runner: &crate::models::RunnerCapabilities,
    want: usize,
) -> Result<Vec<(String, String)>, ControlError> {
    const POLL_SCAN: i64 = 256;
    let head = conn
        .query(
            "SELECT run_id, job_id, runs_on, runner_group FROM jobs WHERE queue_kind='ready' \
             ORDER BY priority DESC, run_order, job_order, seq, run_id, job_id LIMIT $1",
            &[&POLL_SCAN],
        )
        .await
        .map_err(ControlError::backend)?;
    let mut locked = Vec::new();
    let mut busy_runs = BTreeSet::new();
    for row in head {
        let run_id: String = row.get(0);
        let job_id: String = row.get(1);
        if busy_runs.contains(&run_id) {
            continue;
        }
        let runs_on: Vec<String> =
            serde_json::from_str(&row.get::<_, String>(2)).unwrap_or_default();
        let group: Option<String> = row.get(3);
        if !super::sched::job_matches_runner(&runs_on, &runner.labels)
            || !super::sched::job_matches_runner_group(group.as_deref(), runner)
        {
            continue;
        }
        let key = run_lock_key(&parse_run_id(&run_id));
        let got: bool = conn
            .query_one("SELECT pg_try_advisory_xact_lock_shared($1)", &[&key])
            .await
            .map_err(ControlError::backend)?
            .get(0);
        if !got {
            busy_runs.insert(run_id);
            continue;
        }
        let row = conn
            .query_opt(
                "SELECT 1 FROM jobs WHERE run_id=$1 AND job_id=$2 AND queue_kind='ready' \
                 FOR UPDATE SKIP LOCKED",
                &[&run_id, &job_id],
            )
            .await
            .map_err(ControlError::backend)?;
        if row.is_some() {
            locked.push((run_id, job_id));
            if locked.len() == want {
                break;
            }
        }
    }
    Ok(locked)
}

/// Take the exclusive advisory locks of `runs`, in lock-key order. Every
/// multi-run locker uses this one order, so run locks never form a cycle.
async fn lock_runs<'a>(
    conn: &Tx<'_>,
    runs: impl Iterator<Item = &'a RunId>,
) -> Result<(), ControlError> {
    let mut keys: Vec<i64> = runs.map(run_lock_key).collect();
    keys.sort_unstable();
    keys.dedup();
    match keys.as_slice() {
        [] => Ok(()),
        [key] => conn
            .execute("SELECT pg_advisory_xact_lock($1)", &[key])
            .await
            .map(drop)
            .map_err(ControlError::backend),
        // `unnest` streams in array order, so the locks are taken in order.
        _ => conn
            .execute(
                "SELECT pg_advisory_xact_lock(k) FROM unnest($1::bigint[]) AS k",
                &[&keys],
            )
            .await
            .map(drop)
            .map_err(ControlError::backend),
    }
}

async fn load_txstate(
    conn: &Tx<'_>,
    scope: &TxScope,
    cipher: &store::Envelope,
    poll_keys: Option<&[(String, String)]>,
    lock_loaded_runs: bool,
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
            let rows = conn
                .query(
                    "SELECT holder_run_id FROM concurrency_holds \
                     UNION SELECT holder_run_id FROM concurrency_waits",
                    &[],
                )
                .await
                .map_err(ControlError::backend)?;
            for row in rows {
                let run_id_s: String = row.get(0);
                runs.insert(parse_run_id(&run_id_s));
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
            if let Some(keys) = poll_keys {
                // A poll touches only its locked candidates' runs.
                for (run_id, _) in keys {
                    runs.insert(parse_run_id(run_id));
                }
            } else if !kinds.is_empty() {
                let rows = conn
                    .query(
                        "SELECT DISTINCT run_id FROM jobs WHERE queue_kind = ANY($1)",
                        &[&kinds],
                    )
                    .await
                    .map_err(ControlError::backend)?;
                for row in rows {
                    let run_id_s: String = row.get(0);
                    runs.insert(parse_run_id(&run_id_s));
                }
            }
            if let Some(sessions) = scope.sessions.as_ref() {
                if !sessions.is_empty() {
                    let session_ids: Vec<String> = sessions.iter().cloned().collect();
                    let rows = conn
                        .query(
                            "SELECT DISTINCT jr.run_id FROM job_requests jr \
                             JOIN runner_sessions rs \
                             ON jr.request_id = rs.active_request_id \
                             WHERE rs.session_id = ANY($1)",
                            &[&session_ids],
                        )
                        .await
                        .map_err(ControlError::backend)?;
                    for row in rows {
                        let run_id_s: String = row.get(0);
                        runs.insert(parse_run_id(&run_id_s));
                    }
                }
            }
        }
        // `runs_via_requests`: widen `runs` with the run_ids of the loaded
        // `job_requests` (correlation lookup → request → run record). Only
        // meaningful with `job_requests_all`. `SELECT DISTINCT` reads no blob.
        if scope.runs_via_requests && scope.job_requests_all {
            let rows = conn
                .query("SELECT DISTINCT run_id FROM job_requests", &[])
                .await
                .map_err(ControlError::backend)?;
            for row in rows {
                let run_id_s: String = row.get(0);
                runs.insert(parse_run_id(&run_id_s));
            }
        }
    }
    let scope = &effective_scope;

    if lock_loaded_runs {
        // Every run whose rows this load can return: the scope's runs (all
        // runs when unscoped) plus the runs of any queue kind it widens in.
        let mut runs: BTreeSet<RunId> = match scope.runs.as_ref() {
            Some(runs) => runs.clone(),
            None => conn
                .query("SELECT run_id FROM runs", &[])
                .await
                .map_err(ControlError::backend)?
                .iter()
                .map(|row| parse_run_id(&row.get::<_, String>(0)))
                .collect(),
        };
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
        if scope.runs.is_some() && !kinds.is_empty() {
            for row in conn
                .query(
                    "SELECT DISTINCT run_id FROM jobs WHERE queue_kind = ANY($1)",
                    &[&kinds],
                )
                .await
                .map_err(ControlError::backend)?
            {
                runs.insert(parse_run_id(&row.get::<_, String>(0)));
            }
        }
        lock_runs(conn, runs.iter()).await?;
    }

    let mut tx = TxState {
        ready_queue_loaded: scope.runs.is_none() || scope.ready_queue,
        poll_claimable: poll_keys.map(|keys| {
            keys.iter()
                .map(|(run_id, job_id)| (parse_run_id(run_id), JobId(job_id.clone())))
                .collect()
        }),
        ..Default::default()
    };

    // Runs — scoped to `scope.runs` when set.
    let run_ids: Vec<String> = scope
        .runs
        .as_ref()
        .map(|s| s.iter().map(|id| id.0.to_string()).collect())
        .unwrap_or_default();
    let loaded_runs = match scope.runs.as_ref() {
        Some(runs) if runs.is_empty() => Vec::new(),
        Some(_) => load_runs(conn, cipher, " WHERE run_id = ANY($1)", &[&run_ids]).await?,
        None => load_runs(conn, cipher, "", &[]).await?,
    };
    for (run_id, namespace, run, parts) in loaded_runs {
        tx.run_namespaces.insert(run_id, namespace);
        tx.runs.insert(run_id, run);
        tx.loaded.runs.insert(run_id);
        tx.loaded.run_parts.insert(run_id, parts);
    }

    // Jobs — a row loads when its run is in scope OR it sits in a global
    // queue kind the scope asked for (ready / blocked).
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
    let jobs_rows = {
        let base = "SELECT j.run_id, j.job_id, j.status, j.queue_kind, j.queue_position, j.seq, \
             j.reaper_first_seen_us, j.expand_generation, j.enqueued_at_us, \
             j.priority, j.run_order, j.job_order, j.not_before_us, j.namespace_id, j.pool_key, \
             j.base_id, j.runs_on, j.runner_group, \
             j.created_at_ns, j.deps_ready_at_ns, j.concurrency_wait_at_ns, \
             j.concurrency_acquired_at_ns, j.if_condition, j.max_parallel, \
             j.environment_json, j.concurrency_json, j.matrix_json, j.deferred_matrix, \
             j.reusable_call_json, m.message_blob, m.condition_context_blob, \
             j.claimed_by, j.claimed_at_us \
             FROM jobs j LEFT JOIN job_messages m ON m.run_id=j.run_id AND m.job_id=j.job_id";
        // `runs == None` means the run predicate is TRUE, which makes the
        // whole OR true — emit no WHERE and load every job. Only when the
        // scope names a run set do the queue-kind clauses matter.
        let mut conds: Vec<String> = Vec::new();
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();
        if poll_keys.is_some() {
            // Every job of the widened runs (candidate runs + the session's
            // active-request run), never the rest of the ready queue: write-back
            // rewrites a loaded run's unloaded jobs, so a run loads whole.
            params.push(&run_ids);
            conds.push("j.run_id = ANY($1)".to_owned());
        } else if let Some(runs) = scope.runs.as_ref() {
            if runs.is_empty() {
                conds.push("false".to_owned());
            } else {
                params.push(&run_ids);
                conds.push(format!("j.run_id = ANY(${})", params.len()));
            }
            if !kinds.is_empty() {
                params.push(&kinds);
                conds.push(format!("j.queue_kind = ANY(${})", params.len()));
            }
        }
        let order = " ORDER BY CASE WHEN j.queue_kind='ready' THEN 0 ELSE 1 END, \
                     CASE WHEN j.queue_kind='ready' THEN -j.priority ELSE 0 END, \
                     CASE WHEN j.queue_kind='ready' THEN j.run_order ELSE 0 END, \
                     CASE WHEN j.queue_kind='ready' THEN j.job_order ELSE 0 END, \
                     j.seq, j.run_id, j.job_id";
        let is_poll = scope.runs.as_ref().is_some_and(|r| r.is_empty()) && scope.ready_queue;
        // Lock only the job rows: the message side of the outer join is
        // nullable and may not be locked.
        let lock_suffix = if is_poll && poll_keys.is_none() {
            " FOR UPDATE OF j SKIP LOCKED LIMIT 16"
        } else {
            ""
        };
        let sql = if conds.is_empty() {
            format!("{base}{order}{lock_suffix}")
        } else {
            format!("{base} WHERE {}{order}{lock_suffix}", conds.join(" OR "))
        };
        conn.query(&sql, &params)
            .await
            .map_err(ControlError::backend)?
    };
    // `needs:` edges for exactly the loaded jobs (never the whole queue:
    // a poll locks at most 16 rows and must not read every edge).
    let mut needs: std::collections::HashMap<(String, String), Vec<String>> =
        std::collections::HashMap::new();
    if !jobs_rows.is_empty() {
        let run_keys: Vec<String> = jobs_rows.iter().map(|r| r.get(0)).collect();
        let job_keys: Vec<String> = jobs_rows.iter().map(|r| r.get(1)).collect();
        for row in conn
            .query(
                "SELECT n.run_id, n.job_id, n.needs_job_id FROM job_needs n \
                 JOIN UNNEST($1::text[], $2::text[]) AS k(run_id, job_id) \
                   ON k.run_id=n.run_id AND k.job_id=n.job_id \
                 ORDER BY n.run_id, n.job_id, n.position",
                &[&run_keys, &job_keys],
            )
            .await
            .map_err(ControlError::backend)?
        {
            needs
                .entry((row.get(0), row.get(1)))
                .or_default()
                .push(row.get(2));
        }
    }
    for row in jobs_rows {
        let run_id_s: String = row.get(0);
        let job_id_s: String = row.get(1);
        let status: String = row.get(2);
        let kind: String = row.get(3);
        let pos: Option<i64> = row.get(4);
        let job_seq: Option<i64> = row.get(5);
        let reaper_us: Option<i64> = row.get(6);
        let expand_generation: i64 = row.get(7);
        let enqueued_us: Option<i64> = row.get(8);
        let priority: Option<i16> = row.get(9);
        let run_order: Option<i64> = row.get(10);
        let job_order: Option<i64> = row.get(11);
        let not_before_us: Option<i64> = row.get(12);
        let namespace_id: String = row.get(13);
        let pool_key: String = row.get(14);
        let row_sig = super::txstate::job_row_sig(
            &status,
            &kind,
            pos,
            job_seq.unwrap_or(0),
            &row.get::<_, String>(15),
            &row.get::<_, String>(16),
            row.get::<_, Option<String>>(17).as_deref(),
            enqueued_us,
            reaper_us,
            row.get(31),
            row.get(32),
            expand_generation,
            &namespace_id,
            &pool_key,
            priority.unwrap_or(0),
            run_order.unwrap_or(0),
            job_order.unwrap_or(0),
            not_before_us,
        );
        let run_id = parse_run_id(&run_id_s);
        let job_id = JobId(job_id_s.clone());
        let status = status_parse(&status);
        // Preserve the persisted row state for every loaded job — a job whose
        // run isn't in `tx.runs` was widened in by a queue-kind clause and must
        // be written back with these exact values.
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
                row_sig: Some(row_sig),
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
        // A job is dispatchable only with its sealed message; a row without
        // one (terminal/placeholder) routes nowhere.
        let message: Option<Vec<u8>> = row.get(29);
        let context: Option<Vec<u8>> = row.get(30);
        let (Some(message), Some(context)) = (message, context) else {
            continue;
        };
        let payload = super::rows::JobPayloadRow {
            created_at_ns: row.get::<_, Option<i64>>(18).unwrap_or(0),
            deps_ready_at_ns: row.get(19),
            concurrency_wait_at_ns: row.get(20),
            concurrency_acquired_at_ns: row.get(21),
            if_condition: row.get(22),
            max_parallel: row.get(23),
            environment_json: row.get(24),
            concurrency_json: row.get(25),
            matrix_json: row.get(26),
            deferred_matrix: row.get(27),
            reusable_call_json: row.get(28),
            needs: needs.remove(&(run_id_s, job_id_s)).unwrap_or_default(),
        };
        let runs_on: String = row.get(16);
        let job = payload
            .into_job(
                run_id,
                job_id.clone(),
                row.get(15),
                &runs_on,
                row.get(17),
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
    // Global ready-queue size — unscoped COUNT, not the loaded subset.
    tx.ready_count = conn
        .query_one("SELECT COUNT(*) FROM jobs WHERE queue_kind='ready'", &[])
        .await
        .map(|r| r.get(0))
        .unwrap_or(0);
    // Global queue-front labels — unscoped, for pool next-image selection.
    // Read straight from the `runs_on` column; no payload decode.
    tx.next_queue_labels = conn
        .query_opt(
            "SELECT runs_on FROM jobs WHERE queue_kind='ready' \
             ORDER BY priority DESC, run_order, job_order, seq, run_id, job_id LIMIT 1",
            &[],
        )
        .await
        .ok()
        .flatten()
        .and_then(|r| serde_json::from_str(&r.get::<_, String>(0)).ok())
        .unwrap_or_default();

    // Requests — run-scoped via `run_scope_clause`. `job_requests_all` loads
    // the whole table without pulling `record_blob`s.
    let (req_w, req_ids) = run_scope_clause(
        "run_id",
        if scope.job_requests_all {
            None
        } else {
            scope.runs.as_ref()
        },
    );
    for row in conn
        .query(
            &format!(
                "SELECT request_id, run_id, job_id, agent_job_id, plan_id, plan_type, \
                 timeline_id, result, locked_until, claimed_at_us, owner_runner_id, \
                 started_at_us, last_renewed_at_us, timeout_triggered, debug_token_issued, \
                 request_blob FROM job_requests{req_w}"
            ),
            &scope_params(&req_ids),
        )
        .await
        .map_err(ControlError::backend)?
    {
        let request_id: i64 = row.get(0);
        let run_id_s: String = row.get(1);
        let job_id_s: String = row.get(2);
        let agent_job_id_s: String = row.get(3);
        let plan_id: String = row.get(4);
        let plan_type: String = row.get(5);
        let timeline_id_s: String = row.get(6);
        let result_s: Option<String> = row.get(7);
        let locked_until: String = row.get(8);
        let claimed_at_us: Option<i64> = row.get(9);
        let owner_runner_id: Option<i64> = row.get(10);
        let started_at_us: Option<i64> = row.get(11);
        let last_renewed_at_us: Option<i64> = row.get(12);
        let timeout_triggered: i64 = row.get(13);
        let debug_token_issued: i64 = row.get(14);
        let request_blob: Option<Vec<u8>> = row.get(15);
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

    // Token requests, grants, OIDC, steps.
    {
        // `request_id` maps to a request — push the run scope through a
        // subquery on `job_requests`.
        let (sql, ids) = match scope.runs.as_ref() {
            None => (
                "SELECT request_id, request_blob FROM github_token_requests".to_owned(),
                None,
            ),
            Some(set) if set.is_empty() => (
                "SELECT request_id, request_blob FROM github_token_requests WHERE false".to_owned(),
                None,
            ),
            Some(set) => (
                "SELECT request_id, request_blob FROM github_token_requests \
                 WHERE request_id IN (SELECT request_id FROM job_requests \
                 WHERE run_id = ANY($1))"
                    .to_owned(),
                Some(set.iter().map(|id| id.0.to_string()).collect()),
            ),
        };
        for row in conn
            .query(&sql, &scope_params(&ids))
            .await
            .map_err(ControlError::backend)?
        {
            let request_id: i64 = row.get(0);
            let req: Vec<u8> = row.get(1);
            if let Ok(req) = blob(cipher, &req) {
                tx.github_token_requests.insert(request_id, req);
            }
        }
    }
    {
        let (w, ids) = run_scope_clause("run_id", scope.runs.as_ref());
        for row in conn
            .query(
                &format!("SELECT run_id, job_id, granted FROM id_token_grants{w}"),
                &scope_params(&ids),
            )
            .await
            .map_err(ControlError::backend)?
        {
            let run_id_s: String = row.get(0);
            let run_id = parse_run_id(&run_id_s);
            let job_id_s: String = row.get(1);
            let granted: i64 = row.get(2);
            tx.id_token_grants
                .insert((run_id, JobId(job_id_s)), granted != 0);
        }
    }
    {
        let (w, ids) = run_scope_clause("run_id", scope.runs.as_ref());
        for row in conn
            .query(
                &format!("SELECT run_id, job_id, context_blob FROM oidc_job_contexts{w}"),
                &scope_params(&ids),
            )
            .await
            .map_err(ControlError::backend)?
        {
            let run_id_s: String = row.get(0);
            let run_id = parse_run_id(&run_id_s);
            let job_id_s: String = row.get(1);
            let ctx: Vec<u8> = row.get(2);
            if let Ok(ctx) = blob(cipher, &ctx) {
                tx.oidc_job_contexts.insert((run_id, JobId(job_id_s)), ctx);
            }
        }
    }
    {
        // `agent_job_id` maps to a request — push the run scope through a
        // subquery on `job_requests` so a narrow scope skips the table.
        let (filter, ids) = match scope.runs.as_ref() {
            None => (String::new(), None),
            Some(set) if set.is_empty() => (" WHERE false".to_owned(), None),
            Some(set) => (
                " WHERE agent_job_id IN (SELECT agent_job_id FROM job_requests \
                 WHERE run_id = ANY($1))"
                    .to_owned(),
                Some(set.iter().map(|id| id.0.to_string()).collect()),
            ),
        };
        let sql =
            format!("SELECT {STEP_COLUMNS} FROM job_steps{filter} ORDER BY agent_job_id, position");
        for row in conn
            .query(&sql, &scope_params(&ids))
            .await
            .map_err(ControlError::backend)?
        {
            let (agent_job_id, record) = step_from_row(&row);
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
    for row in conn
        .query(
            "SELECT runner_id, name, labels, ephemeral, public_key, rsa_public_key, \
             runner_group_id, runner_group_name, client_id, pool_proven, registered_at_us \
             FROM runners",
            &[],
        )
        .await
        .map_err(ControlError::backend)?
    {
        let runner_id: i64 = row.get(0);
        let name: String = row.get(1);
        let labels: String = row.get(2);
        let ephemeral: i64 = row.get(3);
        let public_key: Option<String> = row.get(4);
        let rsa_xml: Option<String> = row.get(5);
        let runner_group_id: Option<i64> = row.get(6);
        let runner_group_name: Option<String> = row.get(7);
        let client_id: Option<String> = row.get(8);
        let pool_proven: i64 = row.get(9);
        let registered_at_us: i64 = row.get(10);
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

    // Sessions.
    for row in conn
        .query(
            "SELECT session_id, runner_id, protocol, encryption_blob, \
             active_request_id, last_seen_at_us, verified FROM runner_sessions",
            &[],
        )
        .await
        .map_err(ControlError::backend)?
    {
        let session_id: String = row.get(0);
        // Session-scoped: only sessions this scope asked for.
        if let Some(set) = scope.sessions.as_ref() {
            if !set.contains(&session_id) {
                continue;
            }
        }
        let runner_id: Option<i64> = row.get(1);
        let protocol: String = row.get(2);
        let encryption_blob: Option<Vec<u8>> = row.get(3);
        let active_request_id: Option<i64> = row.get(4);
        let last_seen_at_us: Option<i64> = row.get(5);
        let verified: i64 = row.get(6);
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

    // Inflight messages.
    for row in conn
        .query(
            "SELECT session_id, message_id, message_blob FROM broker_messages \
             ORDER BY message_id",
            &[],
        )
        .await
        .map_err(ControlError::backend)?
    {
        let session_id: String = row.get(0);
        // Only messages for sessions this scope loaded.
        if !tx.loaded.sessions.contains(&session_id) {
            continue;
        }
        let message_id: i64 = row.get(1);
        let msg: Vec<u8> = row.get(2);
        if let Ok(msg) = blob(cipher, &msg) {
            tx.inflight_messages
                .entry(session_id)
                .or_default()
                .insert(message_id, msg);
        }
    }

    // Concurrency — always-global, loaded only when the scope asks for it.
    // Gates load from queryable hold/waiter rows; `holder_keys` is derived.
    if scope.concurrency {
        for row in conn
            .query(
                "SELECT repo, group_name, display_name, holder_kind, holder_run_id, \
                 holder_job_id, holder_job_ids FROM concurrency_holds",
                &[],
            )
            .await
            .map_err(ControlError::backend)?
        {
            let repo: String = row.get(0);
            let group_name: String = row.get(1);
            let display_name: String = row.get(2);
            let kind: String = row.get(3);
            let run_id: String = row.get(4);
            let job_id: Option<String> = row.get(5);
            let job_ids: String = row.get(6);
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
            if let Some(holder) =
                crate::concurrency::holder_from_row(&kind, &run_id, job_id.as_deref(), &job_ids)
            {
                group.running = Some(holder.clone());
                tx.holder_keys
                    .entry(holder.run_id())
                    .or_default()
                    .push(key.clone());
            }
            tx.loaded.groups.insert(key);
        }
        for row in conn
            .query(
                "SELECT repo, group_name, holder_kind, holder_run_id, holder_job_id, \
                 holder_job_ids FROM concurrency_waits ORDER BY position",
                &[],
            )
            .await
            .map_err(ControlError::backend)?
        {
            let repo: String = row.get(0);
            let group_name: String = row.get(1);
            let kind: String = row.get(2);
            let run_id: String = row.get(3);
            let job_id: Option<String> = row.get(4);
            let job_ids: String = row.get(5);
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
        for row in conn
            .query(
                "SELECT run_id, job_ids, gate_repo, gate_group, display_name, \
                 cancel_in_progress, queue_mode, acquired FROM jobset_gates \
                 ORDER BY gate_index",
                &[],
            )
            .await
            .map_err(ControlError::backend)?
        {
            let run_id_s: String = row.get(0);
            let job_ids_s: String = row.get(1);
            let repo: String = row.get(2);
            let group: String = row.get(3);
            let display_name: String = row.get(4);
            let cancel: i64 = row.get(5);
            let mode: String = row.get(6);
            let acquired: i64 = row.get(7);
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
        for row in conn
            .query("SELECT run_id, job_ids FROM jobset_ready", &[])
            .await
            .map_err(ControlError::backend)?
        {
            let run_id_s: String = row.get(0);
            let job_ids_s: String = row.get(1);
            let run_id = parse_run_id(&run_id_s);
            let job_ids: BTreeSet<JobId> = serde_json::from_str::<BTreeSet<String>>(&job_ids_s)
                .unwrap_or_default()
                .into_iter()
                .map(JobId)
                .collect();
            tx.jobset_ready.insert(JobSetId { run_id, job_ids });
        }
        // Snapshot the loaded gate state so write-back can skip unchanged
        // groups/jobsets (their rows stay byte-identical, incl. `held_at_us`).
        tx.loaded.group_snapshot = tx.concurrency_groups.clone();
        tx.loaded.jobset_snapshot = tx.jobset_admissions.clone();
    }

    // run_concurrency is run-scoped.
    {
        let (w, ids) = run_scope_clause("run_id", scope.runs.as_ref());
        for row in conn
            .query(
                &format!("SELECT run_id, concurrency_blob FROM run_concurrency{w}"),
                &scope_params(&ids),
            )
            .await
            .map_err(ControlError::backend)?
        {
            let run_id_s: String = row.get(0);
            let run_id = parse_run_id(&run_id_s);
            let c: Vec<u8> = row.get(1);
            if let Ok(c) = blob(cipher, &c) {
                tx.run_concurrency.insert(run_id, c);
            }
        }
    }

    // Assignments, pool pending, cancellations. `job_assignments`/`pool_pending`
    // widen to every run when `ready_queue` (or `runs: None`); only a
    // run-scoped command without the ready queue filters to its own runs.
    {
        let (w, ids) = if scope.ready_queue {
            (String::new(), None)
        } else {
            run_scope_clause("run_id", scope.runs.as_ref())
        };
        for row in conn
            .query(
                &format!(
                    "SELECT run_id, job_id, runner_id, at_us, first_at_us \
                     FROM job_assignments{w}"
                ),
                &scope_params(&ids),
            )
            .await
            .map_err(ControlError::backend)?
        {
            let run_id_s: String = row.get(0);
            let job_id_s: String = row.get(1);
            let runner_id: Option<i64> = row.get(2);
            let at_us: i64 = row.get(3);
            let first_at_us: i64 = row.get(4);
            let run_id = parse_run_id(&run_id_s);
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
        let (w, ids) = if scope.ready_queue {
            (String::new(), None)
        } else {
            run_scope_clause("run_id", scope.runs.as_ref())
        };
        for row in conn
            .query(
                &format!("SELECT run_id, job_id, at_us FROM pool_pending{w}"),
                &scope_params(&ids),
            )
            .await
            .map_err(ControlError::backend)?
        {
            let run_id_s: String = row.get(0);
            let run_id = parse_run_id(&run_id_s);
            let job_id_s: String = row.get(1);
            let at_us: i64 = row.get(2);
            tx.pool_pending
                .insert((run_id, JobId(job_id_s)), us_to_system(at_us));
        }
    }
    {
        let (w, ids) = run_scope_clause("run_id", scope.runs.as_ref());
        for row in conn
            .query(
                &format!(
                    "SELECT run_id, job_id, agent_job_id FROM cancellation_queue{w} \
                     ORDER BY seq"
                ),
                &scope_params(&ids),
            )
            .await
            .map_err(ControlError::backend)?
        {
            let run_id_s: String = row.get(0);
            let job_id_s: String = row.get(1);
            let agent_job_id_s: String = row.get(2);
            let run_id = parse_run_id(&run_id_s);
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

    // Counters — global singletons and per-workflow run-number counters.
    // Always loaded: the allocators must never observe 0, tables are tiny.
    for row in conn
        .query("SELECT name, value FROM counters", &[])
        .await
        .map_err(ControlError::backend)?
    {
        let name: String = row.get(0);
        let value: i64 = row.get(1);
        match name.as_str() {
            "next_message_id" => tx.next_message_id = value,
            "next_runner_id" => tx.next_runner_id = value,
            "next_request_id" => tx.next_request_id = value,
            _ => {}
        }
        tx.loaded.counters.insert(name, value);
    }
    // `next_request_id` must never re-issue an existing `job_requests`
    // primary key — including ids written before the counter row existed or
    // by a peer that has not yet committed its counter bump. Seed from the
    // table max so the first in-transaction allocation is `max + 1`.
    {
        let max_request: i64 = conn
            .query_one("SELECT COALESCE(MAX(request_id), 0) FROM job_requests", &[])
            .await
            .map_err(ControlError::backend)?
            .get(0);
        tx.next_request_id = tx.next_request_id.max(max_request);
    }
    for row in conn
        .query("SELECT key, value FROM workflow_run_counters", &[])
        .await
        .map_err(ControlError::backend)?
    {
        let key: String = row.get(0);
        let value: i64 = row.get(1);
        tx.loaded
            .workflow_run_counters
            .insert(key.clone(), value as u64);
        tx.workflow_run_counters.insert(key, value as u64);
    }

    if scope.include_archived {
        load_archived_txstate(conn, &mut tx).await?;
    }
    super::txstate::snapshot_row_sigs(&mut tx);
    Ok((tx, effective_scope))
}

async fn load_archived_txstate(conn: &Tx<'_>, tx: &mut TxState) -> Result<(), ControlError> {
    if tx.runs.is_empty() {
        return Ok(());
    }
    let ids: Vec<String> = tx.runs.keys().map(|id| id.to_string()).collect();
    let jobs = conn
        .query(
            "SELECT h.run_id,h.job_id,h.status FROM job_history h
         JOIN runs r ON r.run_id=h.run_id
         WHERE r.archived_at_us IS NOT NULL AND h.run_attempt=r.run_attempt
           AND h.run_created_at_us=r.created_at_us AND h.run_id = ANY($1)",
            &[&ids],
        )
        .await
        .map_err(ControlError::backend)?;
    for row in jobs {
        let run: String = row.get(0);
        let job: String = row.get(1);
        let status: String = row.get(2);
        if let Some(record) = tx.runs.get_mut(&parse_run_id(&run)) {
            record.jobs.insert(JobId(job), status_parse(&status));
        }
    }
    let attempts = conn
        .query(
            "SELECT h.request_id,h.run_id,h.job_id,h.agent_job_id,h.plan_id,h.timeline_id,
                h.result,h.owner_runner_id,h.claimed_at_us,h.started_at_us
         FROM attempt_history h JOIN runs r ON r.run_id=h.run_id
         WHERE r.archived_at_us IS NOT NULL AND h.run_attempt=r.run_attempt
           AND h.run_created_at_us=r.created_at_us AND h.run_id = ANY($1)
         ORDER BY h.request_id",
            &[&ids],
        )
        .await
        .map_err(ControlError::backend)?;
    for row in attempts {
        let run_id: String = row.get(1);
        let job_id: String = row.get(2);
        let agent_id: String = row.get(3);
        let plan_id: String = row.get(4);
        let timeline_id: String = row.get(5);
        let result: Option<String> = row.get(6);
        let claimed_at: Option<i64> = row.get(8);
        let started_at: Option<i64> = row.get(9);
        let agent_job_id = parse_uuid(&agent_id);
        tx.insert_request(TaskAgentJobRequestRecord {
            request_id: row.get(0),
            run_id: parse_run_id(&run_id),
            job_id: JobId(job_id),
            agent_job_id,
            plan_id,
            plan_type: String::new(),
            timeline_id: parse_uuid(&timeline_id),
            result: result.as_deref().map(status_parse),
            locked_until: String::new(),
            claimed_at: claimed_at.map(us_to_system),
            owner_runner_id: row.get(7),
            started_at: started_at.map(us_to_system),
            last_renewed_at: None,
            timeout_triggered: false,
            debug_token_issued: false,
        });
    }
    let steps = conn
        .query(
            &format!(
                "SELECT {STEP_COLUMNS} FROM step_history
                 WHERE (run_id, run_attempt, run_created_at_us) IN (
                     SELECT run_id, run_attempt, created_at_us FROM runs
                     WHERE archived_at_us IS NOT NULL AND run_id = ANY($1))
                 ORDER BY agent_job_id, position"
            ),
            &[&ids],
        )
        .await
        .map_err(ControlError::backend)?;
    for row in steps {
        let (agent_job_id, record) = step_from_row(&row);
        tx.job_steps.entry(agent_job_id).or_default().push(record);
    }
    Ok(())
}

/// Delete rows keyed by a loaded-set: `full` clears the table, a narrow scope
/// deletes only the rows it loaded so out-of-scope rows survive commit.
async fn delete_scoped<I, T, F>(
    conn: &Tx<'_>,
    table: &str,
    col: &str,
    keys: &BTreeSet<I>,
    f: F,
    full: bool,
) -> Result<(), ControlError>
where
    T: tokio_postgres::types::ToSql + Sync + Send,
    F: Fn(&I) -> T,
{
    if full {
        conn.execute(&format!("DELETE FROM {table}"), &[])
            .await
            .map_err(ControlError::backend)?;
        return Ok(());
    }
    if keys.is_empty() {
        return Ok(());
    }
    let vals: Vec<T> = keys.iter().map(f).collect();
    conn.execute(
        &format!("DELETE FROM {table} WHERE {col} = ANY($1)"),
        &[&vals],
    )
    .await
    .map_err(ControlError::backend)?;
    Ok(())
}

/// Delete run-scoped rows: `runs == None` clears the table, `Some(runs)`
/// deletes only those runs' rows.
async fn delete_scoped_runs(
    conn: &Tx<'_>,
    table: &str,
    scope: &TxScope,
) -> Result<(), ControlError> {
    match scope.runs.as_ref() {
        None => {
            conn.execute(&format!("DELETE FROM {table}"), &[])
                .await
                .map_err(ControlError::backend)?;
        }
        Some(runs) if !runs.is_empty() => {
            let vals: Vec<String> = runs.iter().map(|id| id.0.to_string()).collect();
            conn.execute(
                &format!("DELETE FROM {table} WHERE run_id = ANY($1)"),
                &[&vals],
            )
            .await
            .map_err(ControlError::backend)?;
        }
        Some(_) => {}
    }
    Ok(())
}

/// Delete the `(run_id, job_id)` rows of `table` that were loaded (`loaded`)
/// but are no longer in the working set (`current`).
async fn delete_gone_pairs<V>(
    conn: &Tx<'_>,
    table: &str,
    loaded: &std::collections::BTreeMap<(RunId, JobId), u64>,
    current: &std::collections::BTreeMap<(RunId, JobId), V>,
) -> Result<(), ControlError> {
    for (run_id, job_id) in loaded.keys() {
        if !current.contains_key(&(*run_id, job_id.clone())) {
            conn.execute(
                &format!("DELETE FROM {table} WHERE run_id=$1 AND job_id=$2"),
                &[&run_id.0.to_string(), &job_id.0],
            )
            .await
            .map_err(ControlError::backend)?;
        }
    }
    Ok(())
}

/// Delete rows for the job-assignment families (`job_assignments`,
/// `pool_pending`). Their load widens to every run when `ready_queue` is set,
/// so the delete must match: full when `ready_queue || runs.is_none()`.
async fn delete_scoped_queue(
    conn: &Tx<'_>,
    table: &str,
    scope: &TxScope,
) -> Result<(), ControlError> {
    if scope.ready_queue || scope.runs.is_none() {
        conn.execute(&format!("DELETE FROM {table}"), &[])
            .await
            .map_err(ControlError::backend)?;
        return Ok(());
    }
    delete_scoped_runs(conn, table, scope).await
}

// ─────────────────────────────────────────────────────────────────────────
// Write-back: TxState delta → rows
// ─────────────────────────────────────────────────────────────────────────
async fn alloc_job_counter(conn: &Tx<'_>, sequence: &str) -> Result<i64, ControlError> {
    conn.query_one("SELECT nextval($1::text::regclass)", &[&sequence])
        .await
        .map(|row| row.get(0))
        .map_err(ControlError::backend)
}

async fn write_txstate(
    conn: &Tx<'_>,
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
        )
        .await?;
    }
    for run_id in &tx.loaded.runs {
        if !tx.runs.contains_key(run_id) {
            conn.execute("DELETE FROM runs WHERE run_id=$1", &[&run_id.0.to_string()])
                .await
                .map_err(ControlError::backend)?;
        }
    }

    // Jobs.
    let mut seen_jobs: BTreeSet<(RunId, JobId)> = BTreeSet::new();

    // Resolves `queue_position`/`seq` from `fresh` (newly enqueued/requeued
    // via `tx.queue`), the persisted kind in `tx.loaded.jobs` vs the written
    // kind (a transition clears position + takes a fresh FIFO seq), and
    // `job_row_state` (only a job staying in the SAME slot keeps its loaded
    // values, so a no-op scoped write is byte-identical).
    async fn write_job(
        conn: &Tx<'_>,
        tx: &TxState,
        fresh: bool,
        run_id: RunId,
        job_id: &JobId,
        kind: QueueKind,
        job: Option<&QueuedJob>,
        cipher: &store::Envelope,
    ) -> Result<(), ControlError> {
        let key = (run_id, job_id.clone());
        let preserved = tx.job_row_state.get(&key);
        let same_slot = !fresh && tx.loaded.jobs.get(&key) == Some(&kind);
        let (position, seq) = if same_slot {
            match preserved {
                Some(p) => (p.queue_position, p.seq),
                None => (None, 0),
            }
        } else {
            let s = alloc_job_counter(conn, "job_seq").await?;
            let p = if kind == QueueKind::Ready {
                Some(alloc_job_counter(conn, "queue_position_seq").await?)
            } else {
                None
            };
            (p, s)
        };
        // Status precedence: explicit `job_status` override wins; then a
        // widened job restores its persisted status verbatim (no coercion, so
        // an untouched foreign `pending` row survives); finally an owned job
        // reads `run.jobs` (canonical).
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
        let namespace_id = tx
            .run_namespaces
            .get(&run_id)
            .map(String::as_str)
            .or_else(|| preserved.map(|p| p.namespace_id.as_str()))
            .unwrap_or(DEFAULT_NAMESPACE);
        if same_slot {
            let sig = super::txstate::job_row_sig(
                status_str(status),
                kind.as_str(),
                position,
                seq,
                &base_id,
                &runs_on,
                runner_group.as_deref(),
                enqueued_us,
                reaper_us,
                claimed.and_then(|c| c.runner_id),
                claimed.map(|c| system_to_us(c.at)),
                expand_generation,
                namespace_id,
                &pool_key,
                priority,
                run_order,
                job_order,
                not_before_us,
            );
            if preserved.and_then(|p| p.row_sig) == Some(sig) {
                return Ok(());
            }
        }
        conn.execute(
            "INSERT INTO jobs (run_id, job_id, status, queue_kind, queue_position, seq, \
             base_id, runs_on, runner_group, enqueued_at_us, reaper_first_seen_us, \
             claimed_by, claimed_at_us, expand_generation, \
             namespace_id, pool_key, priority, run_order, job_order, not_before_us) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20) \
             ON CONFLICT(run_id,job_id) DO UPDATE SET status=excluded.status, \
             queue_kind=excluded.queue_kind, queue_position=excluded.queue_position, \
             seq=excluded.seq, enqueued_at_us=excluded.enqueued_at_us, \
             reaper_first_seen_us=excluded.reaper_first_seen_us, \
             claimed_by=excluded.claimed_by, claimed_at_us=excluded.claimed_at_us, \
             expand_generation=excluded.expand_generation, \
             namespace_id=excluded.namespace_id, pool_key=excluded.pool_key, \
             priority=excluded.priority, run_order=excluded.run_order, \
             job_order=excluded.job_order, not_before_us=excluded.not_before_us",
            &[
                &run_id.0.to_string(),
                &job_id.0,
                &status_str(status),
                &kind.as_str(),
                &position,
                &seq,
                &base_id,
                &runs_on,
                &runner_group,
                &enqueued_us,
                &reaper_us,
                &claimed.and_then(|c| c.runner_id),
                &claimed.map(|c| system_to_us(c.at)),
                &expand_generation,
                &namespace_id,
                &pool_key,
                &priority,
                &run_order,
                &job_order,
                &not_before_us,
            ],
        )
        .await
        .map_err(ControlError::backend)?;
        // Promotion/requeue stamps dependency and enqueue times in the
        // payload. Persist it on any slot change; a same-slot write leaves
        // the payload columns and sealed message untouched.
        if let Some(j) = job.filter(|_| !same_slot) {
            write_job_payload(conn, cipher, &run_id, job_id, j).await?;
        }
        Ok(())
    }

    // `ready_index` = persisted ready jobs (same slot → keep pos/seq);
    // `tx.queue` = newly enqueued + requeued (push_ready) → always fresh.
    for job in &tx.ready_index {
        write_job(
            conn,
            tx,
            false,
            job.run_id,
            &job.job_id,
            QueueKind::Ready,
            Some(job),
            cipher,
        )
        .await?;
        seen_jobs.insert((job.run_id, job.job_id.clone()));
    }
    for job in &tx.queue {
        write_job(
            conn,
            tx,
            true,
            job.run_id,
            &job.job_id,
            QueueKind::Ready,
            Some(job),
            cipher,
        )
        .await?;
        seen_jobs.insert((job.run_id, job.job_id.clone()));
    }
    for job in &tx.pending_jobs {
        write_job(
            conn,
            tx,
            false,
            job.run_id,
            &job.job_id,
            QueueKind::Pending,
            Some(job),
            cipher,
        )
        .await?;
        seen_jobs.insert((job.run_id, job.job_id.clone()));
    }
    for job in &tx.concurrency_blocked {
        write_job(
            conn,
            tx,
            false,
            job.run_id,
            &job.job_id,
            QueueKind::Blocked,
            Some(job),
            cipher,
        )
        .await?;
        seen_jobs.insert((job.run_id, job.job_id.clone()));
    }
    for job in &tx.pending_expansions {
        write_job(
            conn,
            tx,
            false,
            job.run_id,
            &job.job_id,
            QueueKind::Expand,
            Some(job),
            cipher,
        )
        .await?;
        seen_jobs.insert((job.run_id, job.job_id.clone()));
    }
    for (run_id, job_id) in &tx.expanding {
        if seen_jobs.contains(&(*run_id, job_id.clone())) {
            continue;
        }
        write_job(
            conn,
            tx,
            false,
            *run_id,
            job_id,
            QueueKind::Expand,
            tx.expanding_jobs.get(&(*run_id, job_id.clone())),
            cipher,
        )
        .await?;
        seen_jobs.insert((*run_id, job_id.clone()));
    }
    for ((run_id, job_id), job) in &tx.claimed_jobs {
        write_job(
            conn,
            tx,
            false,
            *run_id,
            job_id,
            QueueKind::Claimed,
            Some(job),
            cipher,
        )
        .await?;
        seen_jobs.insert((*run_id, job_id.clone()));
    }
    for (run_id, jobs) in &tx.held_runs {
        for job in jobs {
            write_job(
                conn,
                tx,
                false,
                *run_id,
                &job.job_id,
                QueueKind::Held,
                Some(job),
                cipher,
            )
            .await?;
            seen_jobs.insert((*run_id, job.job_id.clone()));
        }
    }
    for (run_id, run) in &tx.runs {
        for job_id in run.jobs.keys() {
            if !seen_jobs.contains(&(*run_id, job_id.clone())) {
                write_job(
                    conn,
                    tx,
                    false,
                    *run_id,
                    job_id,
                    QueueKind::None,
                    None,
                    cipher,
                )
                .await?;
                seen_jobs.insert((*run_id, job_id.clone()));
            }
        }
    }
    for (run_id, job_id) in tx.loaded.jobs.keys() {
        if !seen_jobs.contains(&(*run_id, job_id.clone())) {
            conn.execute(
                "DELETE FROM jobs WHERE run_id=$1 AND job_id=$2",
                &[&run_id.0.to_string(), &job_id.0],
            )
            .await
            .map_err(ControlError::backend)?;
        }
    }

    // Requests.
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
             request_blob) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) \
             ON CONFLICT(request_id) DO UPDATE SET result=excluded.result, \
             locked_until=excluded.locked_until, claimed_at_us=excluded.claimed_at_us, \
             owner_runner_id=excluded.owner_runner_id, started_at_us=excluded.started_at_us, \
             last_renewed_at_us=excluded.last_renewed_at_us, \
             timeout_triggered=excluded.timeout_triggered, \
             debug_token_issued=excluded.debug_token_issued, \
             request_blob=excluded.request_blob",
            &[
                request_id,
                &r.run_id.0.to_string(),
                &r.job_id.0,
                &r.agent_job_id.to_string(),
                &r.plan_id,
                &r.plan_type,
                &r.timeline_id.to_string(),
                &r.result.map(status_str),
                &r.locked_until,
                &r.claimed_at.map(system_to_us),
                &r.owner_runner_id,
                &r.started_at.map(system_to_us),
                &r.last_renewed_at.map(system_to_us),
                &(r.timeout_triggered as i64),
                &(r.debug_token_issued as i64),
                &request_blob,
            ],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    for request_id in &tx.loaded.requests {
        if !tx.job_requests.contains_key(request_id) {
            conn.execute(
                "DELETE FROM job_requests WHERE request_id=$1",
                &[request_id],
            )
            .await
            .map_err(ControlError::backend)?;
        }
    }
    // Token requests — request-scoped: only requests this scope loaded.
    for request_id in tx.loaded.token_sigs.keys() {
        if !tx.github_token_requests.contains_key(request_id) {
            conn.execute(
                "DELETE FROM github_token_requests WHERE request_id=$1",
                &[request_id],
            )
            .await
            .map_err(ControlError::backend)?;
        }
    }
    for (request_id, req) in &tx.github_token_requests {
        if tx.loaded.token_sigs.get(request_id) == Some(&super::txstate::value_sig(req)) {
            continue;
        }
        conn.execute(
            "INSERT INTO github_token_requests (request_id, request_blob) VALUES ($1,$2) \
             ON CONFLICT(request_id) DO UPDATE SET request_blob=excluded.request_blob",
            &[request_id, &unblob(cipher, req)?],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    // Grants + OIDC — keyed diffs.
    delete_gone_pairs(
        conn,
        "id_token_grants",
        &tx.loaded.grant_sigs,
        &tx.id_token_grants,
    )
    .await?;
    for ((run_id, job_id), granted) in &tx.id_token_grants {
        if tx.loaded.grant_sigs.get(&(*run_id, job_id.clone()))
            == Some(&super::txstate::value_sig(granted))
        {
            continue;
        }
        conn.execute(
            "INSERT INTO id_token_grants (run_id, job_id, granted) VALUES ($1,$2,$3) \
             ON CONFLICT(run_id, job_id) DO UPDATE SET granted=excluded.granted",
            &[&run_id.0.to_string(), &job_id.0, &(*granted as i64)],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    delete_gone_pairs(
        conn,
        "oidc_job_contexts",
        &tx.loaded.oidc_sigs,
        &tx.oidc_job_contexts,
    )
    .await?;
    for ((run_id, job_id), ctx) in &tx.oidc_job_contexts {
        if tx.loaded.oidc_sigs.get(&(*run_id, job_id.clone()))
            == Some(&super::txstate::value_sig(ctx))
        {
            continue;
        }
        conn.execute(
            "INSERT INTO oidc_job_contexts (run_id, job_id, context_blob) VALUES ($1,$2,$3) \
             ON CONFLICT(run_id, job_id) DO UPDATE SET context_blob=excluded.context_blob",
            &[&run_id.0.to_string(), &job_id.0, &unblob(cipher, ctx)?],
        )
        .await
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
        write_step_delta(conn, agent_job_id, &delta).await?;
    }
    for agent_job_id in tx.loaded.steps.keys() {
        if !tx.job_steps.contains_key(agent_job_id) {
            conn.execute(
                "DELETE FROM job_steps WHERE agent_job_id=$1",
                &[&agent_job_id.to_string()],
            )
            .await
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
             registered_at_us) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
             ON CONFLICT(runner_id) DO UPDATE SET name=excluded.name, labels=excluded.labels, \
             ephemeral=excluded.ephemeral, public_key=excluded.public_key, \
             rsa_public_key=excluded.rsa_public_key, runner_group_id=excluded.runner_group_id, \
             runner_group_name=excluded.runner_group_name, client_id=excluded.client_id, \
             pool_proven=excluded.pool_proven",
            &[
                runner_id,
                &runner.name,
                &serde_json::to_string(&runner.labels).unwrap_or_default(),
                &(runner.ephemeral as i64),
                &runner.public_key,
                &rsa_xml,
                &runner.runner_group_id,
                &runner.runner_group_name,
                &client_id,
                &(tx.pool_proven_runners.contains(runner_id) as i64),
                &tx.runner_registered_at
                    .get(runner_id)
                    .map(|t| system_to_us(*t))
                    .unwrap_or(now_us),
            ],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    for runner_id in &tx.loaded.runners {
        if !tx.runners.contains_key(runner_id) {
            conn.execute("DELETE FROM runners WHERE runner_id=$1", &[runner_id])
                .await
                .map_err(ControlError::backend)?;
        }
    }

    // Sessions: keyed diff. A loaded session that is gone is deleted (its
    // messages cascade); a new or changed one is upserted, keeping its
    // original `created_at_us`; an unchanged one is not touched.
    let live_sessions = super::txstate::session_ids(tx);
    for session_id in tx
        .loaded
        .session_sigs
        .keys()
        .chain(tx.loaded.sessions.iter())
    {
        if !live_sessions.contains(session_id) {
            conn.execute(
                "DELETE FROM runner_sessions WHERE session_id=$1",
                &[session_id],
            )
            .await
            .map_err(ControlError::backend)?;
        }
    }
    for session_id in &live_sessions {
        if tx.loaded.session_sigs.get(session_id)
            == Some(&super::txstate::session_sig(tx, session_id))
        {
            continue;
        }
        let (runner_id, protocol) =
            if let Some(runner_id) = tx.broker_session_runners.get(session_id) {
                (Some(*runner_id), SessionProtocol::Broker)
            } else if let Some(session) = tx.sessions.get(session_id) {
                (Some(session.runner_id), SessionProtocol::Azdo)
            } else {
                // Compatibility sessions (e.g. the implicit `default` session)
                // own no registered runner. They still need a row — with NULL
                // `runner_id` — so `broker_messages` and `active_request_id`
                // foreign keys resolve.
                (None, SessionProtocol::Compat)
            };
        // Seal the session AES key (see the SQLite backend's write_session).
        let encryption = tx
            .session_keys
            .get(session_id)
            .map(|e| cipher.seal(&e.key))
            .transpose()
            .map_err(ControlError::backend)?;
        conn.execute(
            "INSERT INTO runner_sessions (session_id, runner_id, protocol, encryption_blob, \
             active_request_id, last_seen_at_us, verified, created_at_us) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8) \
             ON CONFLICT(session_id) DO UPDATE SET runner_id=excluded.runner_id, \
             protocol=excluded.protocol, encryption_blob=excluded.encryption_blob, \
             active_request_id=excluded.active_request_id, \
             last_seen_at_us=excluded.last_seen_at_us, verified=excluded.verified",
            &[
                session_id,
                &runner_id,
                &protocol.as_str(),
                &encryption,
                &tx.session_active_requests.get(session_id).copied(),
                &tx.session_last_seen
                    .get(session_id)
                    .map(|t| system_to_us(*t)),
                &(tx.verified_sessions.contains(session_id) as i64),
                &now_us,
            ],
        )
        .await
        .map_err(ControlError::backend)?;
    }

    // Inflight messages — keyed diff.
    for (session_id, message_id) in tx.loaded.message_sigs.keys() {
        let live = tx
            .inflight_messages
            .get(session_id)
            .is_some_and(|messages| messages.contains_key(message_id));
        if !live && live_sessions.contains(session_id) {
            conn.execute(
                "DELETE FROM broker_messages WHERE session_id=$1 AND message_id=$2",
                &[session_id, message_id],
            )
            .await
            .map_err(ControlError::backend)?;
        }
    }
    for (session_id, messages) in &tx.inflight_messages {
        for (message_id, msg) in messages {
            if tx
                .loaded
                .message_sigs
                .get(&(session_id.clone(), *message_id))
                == Some(&super::txstate::value_sig(msg))
            {
                continue;
            }
            conn.execute(
                "INSERT INTO broker_messages (session_id, message_id, message_blob, \
                 created_at_us) VALUES ($1,$2,$3,$4) ON CONFLICT(session_id, message_id) \
                 DO UPDATE SET message_blob=excluded.message_blob",
                &[session_id, message_id, &unblob(cipher, msg)?, &now_us],
            )
            .await
            .map_err(ControlError::backend)?;
        }
    }

    // Concurrency — always-global, written only when the scope loaded it.
    // Each group/jobset writes its own rows; groups present at load but gone
    // now are deleted. Untouched groups are never rewritten.
    if scope.concurrency {
        for ((repo, group_name), group) in &tx.concurrency_groups {
            // Skip groups whose loaded snapshot equals the working value —
            // write-back only touches gates this command actually changed.
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
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8) \
                     ON CONFLICT(repo,group_name) DO UPDATE SET display_name=excluded.display_name, \
                     holder_kind=excluded.holder_kind, holder_run_id=excluded.holder_run_id, \
                     holder_job_id=excluded.holder_job_id, holder_job_ids=excluded.holder_job_ids, \
                     held_at_us=excluded.held_at_us",
                    &[
                        repo,
                        group_name,
                        &group.display_name,
                        &kind,
                        &run_id,
                        &job_id,
                        &job_ids,
                        &now_us,
                    ],
                )
                .await
                .map_err(ControlError::backend)?;
            } else {
                conn.execute(
                    "DELETE FROM concurrency_holds WHERE repo=$1 AND group_name=$2",
                    &[repo, group_name],
                )
                .await
                .map_err(ControlError::backend)?;
            }
            conn.execute(
                "DELETE FROM concurrency_waits WHERE repo=$1 AND group_name=$2",
                &[repo, group_name],
            )
            .await
            .map_err(ControlError::backend)?;
            for (position, holder) in group.pending.iter().enumerate() {
                let (kind, run_id, job_id, job_ids) = crate::concurrency::holder_row(holder);
                conn.execute(
                    "INSERT INTO concurrency_waits (repo, group_name, position, holder_kind, \
                     holder_run_id, holder_job_id, holder_job_ids, queued_at_us) \
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                    &[
                        repo,
                        group_name,
                        &(position as i64),
                        &kind,
                        &run_id,
                        &job_id,
                        &job_ids,
                        &now_us,
                    ],
                )
                .await
                .map_err(ControlError::backend)?;
            }
        }
        for (repo, group_name) in &tx.loaded.groups {
            if !tx
                .concurrency_groups
                .contains_key(&(repo.clone(), group_name.clone()))
            {
                conn.execute(
                    "DELETE FROM concurrency_holds WHERE repo=$1 AND group_name=$2",
                    &[repo, group_name],
                )
                .await
                .map_err(ControlError::backend)?;
                conn.execute(
                    "DELETE FROM concurrency_waits WHERE repo=$1 AND group_name=$2",
                    &[repo, group_name],
                )
                .await
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
                "DELETE FROM jobset_gates WHERE run_id=$1 AND job_ids=$2",
                &[&id.run_id.0.to_string(), &job_ids_s],
            )
            .await
            .map_err(ControlError::backend)?;
            for (index, gate) in admission.gates.iter().enumerate() {
                conn.execute(
                    "INSERT INTO jobset_gates (run_id, job_ids, gate_index, gate_repo, gate_group, \
                     display_name, cancel_in_progress, queue_mode, acquired) \
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                    &[
                        &id.run_id.0.to_string(),
                        &job_ids_s,
                        &(index as i64),
                        &gate.key.0,
                        &gate.key.1,
                        &gate.display_name,
                        &(i64::from(gate.cancel_in_progress)),
                        &crate::concurrency::queue_mode_row(&gate.queue),
                        &(i64::from(admission.acquired_keys.contains(&gate.key))),
                    ],
                )
                .await
                .map_err(ControlError::backend)?;
            }
        }
        for id in &tx.loaded.jobsets {
            if !tx.jobset_admissions.contains_key(id) {
                let job_ids: Vec<String> = id.job_ids.iter().map(|j| j.0.clone()).collect();
                conn.execute(
                    "DELETE FROM jobset_gates WHERE run_id=$1 AND job_ids=$2",
                    &[
                        &id.run_id.0.to_string(),
                        &serde_json::to_string(&job_ids).unwrap_or_default(),
                    ],
                )
                .await
                .map_err(ControlError::backend)?;
            }
        }
        conn.execute("DELETE FROM jobset_ready", &[])
            .await
            .map_err(ControlError::backend)?;
        for id in &tx.jobset_ready {
            let job_ids: Vec<String> = id.job_ids.iter().map(|j| j.0.clone()).collect();
            conn.execute(
                "INSERT INTO jobset_ready (run_id, job_ids) VALUES ($1,$2)",
                &[
                    &id.run_id.0.to_string(),
                    &serde_json::to_string(&job_ids).unwrap_or_default(),
                ],
            )
            .await
            .map_err(ControlError::backend)?;
        }
    }
    for run_id in tx.loaded.run_concurrency_sigs.keys() {
        if !tx.run_concurrency.contains_key(run_id) {
            conn.execute(
                "DELETE FROM run_concurrency WHERE run_id=$1",
                &[&run_id.0.to_string()],
            )
            .await
            .map_err(ControlError::backend)?;
        }
    }
    for (run_id, c) in &tx.run_concurrency {
        if tx.loaded.run_concurrency_sigs.get(run_id) == Some(&super::txstate::value_sig(c)) {
            continue;
        }
        conn.execute(
            "INSERT INTO run_concurrency (run_id, concurrency_blob) VALUES ($1,$2) \
             ON CONFLICT(run_id) DO UPDATE SET concurrency_blob=excluded.concurrency_blob",
            &[&run_id.0.to_string(), &unblob(cipher, c)?],
        )
        .await
        .map_err(ControlError::backend)?;
    }

    // Assignments, pool pending, cancellations — keyed diffs.
    delete_gone_pairs(
        conn,
        "job_assignments",
        &tx.loaded.assignment_sigs,
        &tx.job_assignments,
    )
    .await?;
    for ((run_id, job_id), record) in &tx.job_assignments {
        if tx.loaded.assignment_sigs.get(&(*run_id, job_id.clone()))
            == Some(&super::txstate::value_sig(record))
        {
            continue;
        }
        conn.execute(
            "INSERT INTO job_assignments (run_id, job_id, runner_id, at_us, first_at_us) \
             VALUES ($1,$2,$3,$4,$5) ON CONFLICT(run_id, job_id) DO UPDATE SET \
             runner_id=excluded.runner_id, at_us=excluded.at_us, first_at_us=excluded.first_at_us",
            &[
                &run_id.0.to_string(),
                &job_id.0,
                &record.runner_id,
                &system_to_us(record.at),
                &system_to_us(record.first_at),
            ],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    delete_gone_pairs(
        conn,
        "pool_pending",
        &tx.loaded.pool_pending_sigs,
        &tx.pool_pending,
    )
    .await?;
    for ((run_id, job_id), at) in &tx.pool_pending {
        if tx.loaded.pool_pending_sigs.get(&(*run_id, job_id.clone()))
            == Some(&super::txstate::value_sig(at))
        {
            continue;
        }
        conn.execute(
            "INSERT INTO pool_pending (run_id, job_id, at_us) VALUES ($1,$2,$3) \
             ON CONFLICT(run_id, job_id) DO UPDATE SET at_us=excluded.at_us",
            &[&run_id.0.to_string(), &job_id.0, &system_to_us(*at)],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    // The queue is FIFO by `seq`: removed entries are deleted, new ones are
    // appended (a fresh `seq`), untouched ones keep their place.
    let current: BTreeSet<(RunId, JobId, uuid::Uuid)> = tx
        .cancellation_queue
        .iter()
        .map(|c| (c.run_id, c.job_id.clone(), c.agent_job_id))
        .collect();
    for (run_id, job_id, agent_job_id) in tx.loaded.cancellations.difference(&current) {
        conn.execute(
            "DELETE FROM cancellation_queue WHERE run_id=$1 AND job_id=$2 AND agent_job_id=$3",
            &[&run_id.0.to_string(), &job_id.0, &agent_job_id.to_string()],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    for c in &tx.cancellation_queue {
        if tx
            .loaded
            .cancellations
            .contains(&(c.run_id, c.job_id.clone(), c.agent_job_id))
        {
            continue;
        }
        conn.execute(
            "INSERT INTO cancellation_queue (run_id, job_id, agent_job_id) VALUES ($1,$2,$3)",
            &[
                &c.run_id.0.to_string(),
                &c.job_id.0,
                &c.agent_job_id.to_string(),
            ],
        )
        .await
        .map_err(ControlError::backend)?;
    }

    // Counters: write only what this transaction advanced, and never lower
    // a stored value. A run-scoped transaction runs concurrently with
    // allocating ones; writing its stale loaded copy back would hand the
    // same runner/message id or run number out twice.
    for (name, value) in super::txstate::advanced_counters(tx) {
        conn.execute(
            "INSERT INTO counters (name, value) VALUES ($1,$2) \
             ON CONFLICT(name) DO UPDATE SET value=GREATEST(counters.value, excluded.value)",
            &[&name, &value],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    for (key, value) in super::txstate::advanced_run_counters(tx) {
        conn.execute(
            "INSERT INTO workflow_run_counters (key, value) VALUES ($1,$2) \
             ON CONFLICT(key) DO UPDATE SET \
             value=GREATEST(workflow_run_counters.value, excluded.value)",
            &[&key, &value],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// ControlBackend implementation
// ─────────────────────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl ControlBackend for PostgresBackend {
    async fn submit_run(&self, submit: SubmitRun) -> Result<SubmitOutcome, ControlError> {
        // Resolve the replay target before the transaction so the working
        // set stays narrow: `submit_run_tx` dedups by scanning `tx.runs`,
        // which only sees scoped runs — the existing run must be named in
        // the scope or the check misses it and inserts a duplicate.
        let mut run_ids = BTreeSet::from([submit.record.run_id]);
        if let (Some(delivery_id), workflow_path) = (
            submit.record.webhook_delivery_id.as_deref(),
            submit.record.workflow_path_str.as_str(),
        ) {
            if let Some(existing) = self
                .find_run_by_delivery(delivery_id, workflow_path)
                .await?
            {
                run_ids.insert(existing.run_id);
            }
        }
        // `sessions: None` — `on_job_enqueued` finds idle-runner candidates
        // through `broker_session_runners`/`sessions`/`session_active_requests`,
        // all session-scoped. `concurrency` (global lock) only when the run
        // declares a gate: it widens `runs` with every holder's run (a
        // `cancel_in_progress` submit cancels the running holder). Any other
        // submit touches only its own run and takes that run's lock.
        // `runs_referenced` is off: submit never promotes queued jobs, so
        // ready/blocked runs stay foreign.
        let concurrency = submit.workflow_concurrency.is_some()
            || submit.empty_concurrency_group
            || submit
                .jobs
                .iter()
                .any(|job| job.queued.concurrency.is_some());
        let scope = TxScope {
            include_archived: false,
            runs: Some(run_ids),
            ready_queue: false,
            blocked_jobs: false,
            sessions: None,
            concurrency,
            runs_referenced: false,
            job_requests_all: false,
            pending_expansions: false,
            runs_via_requests: false,
        };
        self.transact_scoped(&scope, |tx| commands::submit_run_tx(tx, submit))
            .await
    }

    async fn allocate_run_number(&self, workflow_path: &str) -> Result<u64, ControlError> {
        // One atomic upsert: the stored value is the last number handed out.
        let client = self.checkout_writer().await?;
        let result = client
            .query_one(
                "INSERT INTO workflow_run_counters (key, value) VALUES ($1, 1) \
                 ON CONFLICT(key) DO UPDATE SET value = workflow_run_counters.value + 1 \
                 RETURNING value",
                &[&workflow_path],
            )
            .await
            .map(|row| row.get::<_, i64>(0) as u64)
            .map_err(ControlError::backend);
        self.return_writer(client).await;
        result
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
            // `poll` claims ready work only — no blocked-job promotion, no
            // concurrency-gate mutation. Loading those families is O(#blocked
            // + #gates) job payloads for no benefit.
            blocked_jobs: false,
            sessions: Some(BTreeSet::from([poll.session_id.clone()])),
            concurrency: false,
            runs_referenced: true,
            pending_expansions: false,
            runs_via_requests: false,
            job_requests_all: false,
        };
        let runner = poll.runner.clone();
        self.transact_poll(&scope, &runner, |tx| commands::poll_session_tx(tx, poll))
            .await
    }

    async fn acquire_context(&self, request_id: i64) -> Result<AcquireContext, ControlError> {
        // Read-only — the reader pool serves it concurrently with the
        // writer instead of serializing on the advisory write lock. Scoped
        // to the request's run + owner session so it never loads the whole
        // working set.
        let (run_id, session_id) = self
            .find_request_context(request_id)
            .await?
            .ok_or_else(|| ControlError::NotFound(format!("request {request_id}")))?;
        let scope = TxScope {
            include_archived: false,
            runs: Some(BTreeSet::from([run_id])),
            ready_queue: false,
            blocked_jobs: false,
            sessions: Some(session_id.into_iter().collect()),
            pending_expansions: false,
            runs_via_requests: false,
            concurrency: false,
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
        .await
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
        let sessions = match completion.agent_job_id {
            Some(id) => self.find_session_by_agent_job_id(id).await?,
            None => None,
        }
        .into_iter()
        .collect();
        let scope = TxScope {
            include_archived: false,
            runs: Some(BTreeSet::from([completion.run_id])),
            ready_queue: true,
            pending_expansions: false,
            runs_via_requests: false,
            blocked_jobs: true,
            sessions: Some(sessions),
            job_requests_all: false,
            concurrency: true,
            runs_referenced: true,
        };
        self.transact_scoped(&scope, |tx| commands::complete_job_tx(tx, completion))
            .await
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
        let sessions = self.find_sessions_by_run(run_id).await?;
        let scope = TxScope {
            include_archived: false,
            pending_expansions: false,
            runs_via_requests: false,
            runs: Some(BTreeSet::from([run_id])),
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
        .await
    }

    async fn cancel_job(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<CancelOutcome, ControlError> {
        let job_id = job_id.clone();
        let sessions = self.find_sessions_by_run(run_id).await?;
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
        .await
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
        .await
    }

    async fn release_request(&self, request_id: i64) -> Result<(), ControlError> {
        self.transact(|tx| {
            sched::release_request_for_retry(tx, request_id);
            Ok(())
        })
        .await
    }

    async fn register_runner(&self, reg: RegisterRunner) -> Result<RunnerRow, ControlError> {
        self.transact(|tx| commands::register_runner_tx(tx, reg))
            .await
    }

    async fn create_session(&self, session: CreateSession) -> Result<SessionRow, ControlError> {
        self.transact(|tx| commands::create_session_tx(tx, session))
            .await
    }

    async fn delete_session(&self, session_id: &str) -> Result<(), ControlError> {
        let session_id = session_id.to_owned();
        self.transact(|tx| {
            commands::delete_session_tx(tx, &session_id);
            Ok(())
        })
        .await
    }

    async fn purge_runner(&self, runner_id: i64) -> Result<(), ControlError> {
        self.transact(|tx| {
            commands::purge_runner_tx(tx, runner_id);
            Ok(())
        })
        .await
    }

    async fn claim_expansion(&self) -> Result<Option<ExpansionClaim>, ControlError> {
        // Pick candidate runs by query, then claim under the run's own lock
        // (global only for a run in a concurrency group). The run scope loads
        // every job of the run, so its deferred nodes land in
        // `pending_expansions`. A candidate another node claimed first yields
        // nothing; try the next.
        let candidates: Vec<RunId> = {
            let client = self.checkout_reader().await?;
            let rows = client
                .query(
                    "SELECT run_id FROM jobs WHERE queue_kind='expand' AND expand_generation=0 \
                     GROUP BY run_id ORDER BY MIN(seq) LIMIT 8",
                    &[],
                )
                .await
                .map_err(ControlError::backend);
            self.return_reader(client).await;
            rows?
                .iter()
                .map(|row| parse_run_id(&row.get::<_, String>(0)))
                .collect()
        };
        for run_id in candidates {
            let scope = TxScope {
                include_archived: false,
                runs: Some(BTreeSet::from([run_id])),
                ready_queue: false,
                blocked_jobs: false,
                sessions: Some(BTreeSet::new()),
                concurrency: self.run_in_concurrency(run_id).await?,
                runs_referenced: false,
                job_requests_all: false,
                pending_expansions: false,
                runs_via_requests: false,
            };
            let claim = self
                .transact_scoped(&scope, |tx| {
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
                    tx.expanding_jobs
                        .insert((job.run_id, job.job_id.clone()), job.clone());
                    Ok(Some(ExpansionClaim {
                        job,
                        generation,
                        plan,
                    }))
                })
                .await?;
            if claim.is_some() {
                return Ok(claim);
            }
        }
        Ok(None)
    }

    async fn apply_expansion(
        &self,
        claim: ExpansionApply,
    ) -> Result<crate::runtime_scheduling::SchedulingOutcome, ControlError> {
        // Scoped to the expanded node's run — `sched::apply_expansion`
        // registers the built legs into that same run.
        let scope = TxScope {
            include_archived: false,
            runs: Some(BTreeSet::from([claim.job.run_id])),
            ready_queue: false,
            blocked_jobs: false,
            sessions: Some(BTreeSet::new()),
            concurrency: self.run_in_concurrency(claim.job.run_id).await?,
            runs_referenced: false,
            job_requests_all: false,
            pending_expansions: false,
            runs_via_requests: false,
        };
        self.transact_scoped(&scope, |tx| {
            let mut outcome = crate::runtime_scheduling::SchedulingOutcome::default();
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
        .await
    }

    async fn reconcile_on_boot(&self) -> Result<ReconcileOutcome, ControlError> {
        self.transact(|tx| {
            let mut outcome = ReconcileOutcome::default();
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

            let stuck: Vec<(RunId, JobId)> = tx.expanding.iter().cloned().collect();
            for key in stuck {
                if let Some(job) = tx.expanding_jobs.remove(&key) {
                    tx.expanding.remove(&key);
                    tx.expand_generations.remove(&key);
                    tx.pending_expansions.push_back(job);
                    outcome.recovered += 1;
                } else {
                    tx.expanding.remove(&key);
                    tx.expand_generations.remove(&key);
                    outcome.failed += 1;
                }
            }
            Ok(outcome)
        })
        .await
    }

    async fn run_record(&self, run_id: RunId) -> Result<RunRecord, ControlError> {
        self.read_scoped(&TxScope::run(run_id).with_history(), |tx| {
            tx.runs
                .get(&run_id)
                .cloned()
                .ok_or_else(|| ControlError::NotFound(format!("run {run_id}")))
        })
        .await
    }

    async fn list_runs(&self, filter: RunListFilter) -> Result<Vec<RunRecord>, ControlError> {
        self.list_run_rows(filter).await
    }

    async fn append_event(
        &self,
        event: &preloop_gha_protocol::NdjsonEvent,
    ) -> Result<(), ControlError> {
        self.append_event_row(event).await
    }

    async fn terminal_jobs(&self) -> Result<BTreeSet<(RunId, JobId)>, ControlError> {
        self.terminal_job_rows().await
    }

    async fn archive_finished_runs(&self, limit: usize) -> Result<usize, ControlError> {
        let mut client = self.checkout_writer().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            let now: i64 = tx
                .query_one(
                    "SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::BIGINT",
                    &[],
                )
                .await
                .map_err(ControlError::backend)?
                .get(0);
            let cutoff = now - 60_000_000;
            let limit = limit.min(64) as i64;
            let rows = tx
                .query(
                    "SELECT r.run_id FROM runs r
                 WHERE r.archived_at_us IS NULL
                   AND r.completed_at_us IS NOT NULL AND r.completed_at_us <= $1
                   AND r.status IN ('success','failure','skipped','cancelled')
                   AND NOT EXISTS (SELECT 1 FROM job_requests q
                                   WHERE q.run_id=r.run_id AND q.result IS NULL)
                   AND NOT EXISTS (SELECT 1 FROM job_requests q
                                   JOIN runner_sessions s ON s.active_request_id=q.request_id
                                   WHERE q.run_id=r.run_id)
                   AND NOT EXISTS (SELECT 1 FROM cancellation_queue c WHERE c.run_id=r.run_id)
                 ORDER BY r.completed_at_us, r.run_id LIMIT $2
                 FOR UPDATE OF r SKIP LOCKED",
                    &[&cutoff, &limit],
                )
                .await
                .map_err(ControlError::backend)?;
            for row in &rows {
                let run_id: String = row.get(0);
                tx.execute(
                    "INSERT INTO job_history(namespace_id,run_id,run_attempt,run_created_at_us,
                    job_id,status,base_id,pool_key,priority,run_order,job_order)
                 SELECT r.namespace,r.run_id,r.run_attempt,r.created_at_us,
                        j.job_id,j.status,j.base_id,j.pool_key,j.priority,j.run_order,j.job_order
                 FROM jobs j JOIN runs r ON r.run_id=j.run_id WHERE r.run_id=$1",
                    &[&run_id],
                )
                .await
                .map_err(ControlError::backend)?;
                tx.execute(
                    "INSERT INTO attempt_history(namespace_id,run_id,run_attempt,run_created_at_us,
                    request_id,job_id,agent_job_id,plan_id,timeline_id,result,owner_runner_id,
                    claimed_at_us,started_at_us)
                 SELECT r.namespace,r.run_id,r.run_attempt,r.created_at_us,
                        q.request_id,q.job_id,q.agent_job_id,q.plan_id,q.timeline_id,q.result,
                        q.owner_runner_id,q.claimed_at_us,q.started_at_us
                 FROM job_requests q JOIN runs r ON r.run_id=q.run_id
                 WHERE r.run_id=$1",
                    &[&run_id],
                )
                .await
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
                 WHERE r.run_id=$1",
                    &[&run_id],
                )
                .await
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
                    tx.execute(&format!("DELETE FROM {table} WHERE run_id=$1"), &[&run_id])
                        .await
                        .map_err(ControlError::backend)?;
                }
                tx.execute("DELETE FROM jobs WHERE run_id=$1", &[&run_id])
                    .await
                    .map_err(ControlError::backend)?;
                tx.execute(
                    "UPDATE runs SET archived_at_us=$2 WHERE run_id=$1",
                    &[&run_id, &now],
                )
                .await
                .map_err(ControlError::backend)?;
            }
            tx.commit().await.map_err(ControlError::backend)?;
            Ok(rows.len())
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn request(&self, key: RequestKey) -> Result<TaskAgentJobRequestRecord, ControlError> {
        // A lookup: a lock-free read of the request families only.
        self.read_scoped(&TxScope::requests_only(), |tx| {
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
        .await
    }

    async fn create_log(&self, _plan_id: &str) -> Result<i64, ControlError> {
        let mut client = self.checkout_writer().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            let row = tx
                .query_one(
                    "INSERT INTO counters(name, value) VALUES ('next_log_id', 1) \
                     ON CONFLICT(name) DO UPDATE SET value = counters.value + 1 \
                     RETURNING value - 1",
                    &[],
                )
                .await
                .map_err(ControlError::backend)?;
            tx.commit().await.map_err(ControlError::backend)?;
            Ok(row.get(0))
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn store_meta(&self, meta: &crate::store::MetaSnapshot) -> Result<(), ControlError> {
        let value = unblob(&self.cipher, meta)?;
        let client = self.checkout_writer().await?;
        let result = client
            .execute(
                "INSERT INTO meta(key, value) VALUES ('local_state', $1) \
                 ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value",
                &[&value],
            )
            .await
            .map_err(ControlError::backend);
        self.return_writer(client).await;
        result.map(|_| ())
    }

    async fn load_meta(&self) -> Result<Option<crate::store::MetaSnapshot>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query_opt("SELECT value FROM meta WHERE key = 'local_state'", &[])
            .await
            .map_err(ControlError::backend)?
            .map(|row| blob(&self.cipher, &row.get::<_, Vec<u8>>(0)))
            .transpose();
        self.return_reader(client).await;
        result
    }

    async fn ensure_key_fingerprint(&self, fingerprint: &str) -> Result<(), ControlError> {
        let client = self.checkout_writer().await?;
        let result = async {
            client
                .execute(
                    "INSERT INTO meta(key, value) VALUES ('key_fingerprint', $1) \
                     ON CONFLICT(key) DO NOTHING",
                    &[&fingerprint.as_bytes()],
                )
                .await
                .map_err(ControlError::backend)?;
            let stored: Vec<u8> = client
                .query_one("SELECT value FROM meta WHERE key = 'key_fingerprint'", &[])
                .await
                .map_err(ControlError::backend)?
                .get(0);
            super::types::check_key_fingerprint(&stored, fingerprint)
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn touch_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionProtocol>, ControlError> {
        // A single autocommit UPDATE on a pooled (non-writer) connection:
        // the heartbeat never queues behind writers holding the global lock.
        let client = self.checkout_reader().await?;
        let result = client
            .query_opt(
                "UPDATE runner_sessions SET last_seen_at_us = $1 WHERE session_id = $2 \
                 RETURNING protocol",
                &[&system_to_us(std::time::SystemTime::now()), &session_id],
            )
            .await
            .map_err(ControlError::backend)
            .map(|row| row.map(|row| SessionProtocol::parse(&row.get::<_, String>(0))));
        self.return_reader(client).await;
        result
    }

    async fn session_owner(
        &self,
        session_id: &str,
    ) -> Result<Option<(i64, crate::models::RunnerCapabilities)>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query_opt(
                "SELECT s.runner_id, r.labels, r.runner_group_id, r.runner_group_name, \
                 r.runner_id IS NOT NULL \
                 FROM runner_sessions s LEFT JOIN runners r ON r.runner_id = s.runner_id \
                 WHERE s.session_id = $1 AND s.runner_id IS NOT NULL",
                &[&session_id],
            )
            .await
            .map_err(ControlError::backend)
            .map(|row| {
                row.map(|row| {
                    let labels: Option<String> = row.get(1);
                    (
                        row.get::<_, i64>(0),
                        crate::models::RunnerCapabilities {
                            known: row.get(4),
                            labels: labels
                                .and_then(|l| serde_json::from_str(&l).ok())
                                .unwrap_or_default(),
                            runner_group_id: row.get(2),
                            runner_group_name: row.get(3),
                        },
                    )
                })
            });
        self.return_reader(client).await;
        result
    }

    async fn run_in_concurrency(&self, run_id: RunId) -> Result<bool, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM run_concurrency WHERE run_id=$1) \
                 OR EXISTS(SELECT 1 FROM jobset_gates WHERE run_id=$1) \
                 OR EXISTS(SELECT 1 FROM jobs WHERE run_id=$1 AND concurrency_json IS NOT NULL) \
                 OR EXISTS(SELECT 1 FROM concurrency_holds WHERE holder_run_id=$1) \
                 OR EXISTS(SELECT 1 FROM concurrency_waits WHERE holder_run_id=$1)",
                &[&run_id.0.to_string()],
            )
            .await
            .map(|row| row.get(0))
            .map_err(ControlError::backend);
        self.return_reader(client).await;
        result
    }

    async fn callback_job(
        &self,
        plan_id: &str,
        timeline_id: Option<uuid::Uuid>,
    ) -> Result<Option<CallbackJob>, ControlError> {
        let client = self.checkout_reader().await?;
        let timeline = timeline_id.map(|id| id.to_string()).unwrap_or_default();
        let result = client
            .query_opt(
                "SELECT r.request_id, r.run_id, r.job_id, r.agent_job_id, j.status \
                 FROM job_requests r LEFT JOIN jobs j ON j.run_id = r.run_id AND j.job_id = r.job_id \
                 WHERE r.plan_id = $1 OR r.timeline_id = $2 \
                 ORDER BY (r.plan_id = $1) DESC, r.request_id DESC LIMIT 1",
                &[&plan_id, &timeline],
            )
            .await
            .map_err(ControlError::backend)
            .map(|row| {
                row.map(|row| CallbackJob {
                    request_id: row.get(0),
                    run_id: parse_run_id(&row.get::<_, String>(1)),
                    job_id: JobId(row.get(2)),
                    agent_job_id: parse_uuid(&row.get::<_, String>(3)),
                    job_status: row.get::<_, Option<String>>(4).map(|s| status_parse(&s)),
                })
            });
        self.return_reader(client).await;
        result
    }

    async fn renew_lease(
        &self,
        agent_job_id: uuid::Uuid,
        runner_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError> {
        // One conditional UPDATE on a pooled non-writer connection: a lease
        // renewal never waits for the global writer lock.
        let client = self.checkout_reader().await?;
        let result = async {
            let agent = agent_job_id.to_string();
            let renewed = client
                .execute(
                    "UPDATE job_requests SET locked_until = $1, last_renewed_at_us = $2 \
                     WHERE agent_job_id = $3 AND result IS NULL AND owner_runner_id = $4",
                    &[
                        &locked_until,
                        &system_to_us(std::time::SystemTime::now()),
                        &agent,
                        &runner_id,
                    ],
                )
                .await
                .map_err(ControlError::backend)?;
            if renewed == 1 {
                return Ok(true);
            }
            let row = client
                .query_opt(
                    "SELECT result, owner_runner_id FROM job_requests WHERE agent_job_id = $1",
                    &[&agent],
                )
                .await
                .map_err(ControlError::backend)?
                .map(|row| (row.get(0), row.get(1)));
            super::types::renew_miss(row, runner_id)
        }
        .await;
        self.return_reader(client).await;
        result
    }

    async fn patch_timeline(
        &self,
        timeline_key: &str,
        mut records: Vec<preloop_gha_protocol::azdo::TimelineRecord>,
    ) -> Result<(i32, Vec<preloop_gha_protocol::azdo::TimelineRecord>), ControlError> {
        // Own transaction on a pooled non-writer connection: no global lock.
        // The `timelines` row lock serializes PATCHes of one timeline only.
        let mut client = self.checkout_reader().await?;
        let result = async {
            let txn = client.transaction().await.map_err(ControlError::backend)?;
            let now = std::time::SystemTime::now();
            let change_id: i64 = txn
                .query_one(
                    "INSERT INTO timelines (timeline_key, change_id, updated_at_us) \
                     VALUES ($1, 1, $2) ON CONFLICT (timeline_key) DO UPDATE \
                     SET change_id = timelines.change_id + 1, updated_at_us = $2 \
                     RETURNING change_id",
                    &[&timeline_key, &system_to_us(now)],
                )
                .await
                .map_err(ControlError::backend)?
                .get(0);
            let stamped = super::types::stamp_timeline_records(&mut records, change_id, now);
            let (ids, bodies): (Vec<String>, Vec<String>) = stamped.into_iter().unzip();
            if !ids.is_empty() {
                txn.execute(
                    "INSERT INTO timeline_records (timeline_key, record_id, record_json) \
                     SELECT $1, id, body FROM UNNEST($2::text[], $3::text[]) AS r(id, body) \
                     ON CONFLICT (timeline_key, record_id) \
                     DO UPDATE SET record_json = EXCLUDED.record_json",
                    &[&timeline_key, &ids, &bodies],
                )
                .await
                .map_err(ControlError::backend)?;
            }
            let stored = txn
                .query(
                    "SELECT record_json FROM timeline_records WHERE timeline_key = $1 \
                     ORDER BY record_id LIMIT $2",
                    &[&timeline_key, &(super::types::MAX_TIMELINE_RECORDS as i64)],
                )
                .await
                .map_err(ControlError::backend)?;
            txn.commit().await.map_err(ControlError::backend)?;
            let records = stored
                .iter()
                .filter_map(|row| serde_json::from_str(&row.get::<_, String>(0)).ok())
                .collect();
            Ok((change_id as i32, records))
        }
        .await;
        self.return_reader(client).await;
        result
    }

    async fn get_timeline(
        &self,
        timeline_key: &str,
        skip: usize,
        top: usize,
    ) -> Result<(i32, Vec<preloop_gha_protocol::azdo::TimelineRecord>), ControlError> {
        let client = self.checkout_reader().await?;
        let result = async {
            let change_id: i64 = client
                .query_opt(
                    "SELECT change_id FROM timelines WHERE timeline_key = $1",
                    &[&timeline_key],
                )
                .await
                .map_err(ControlError::backend)?
                .map(|row| row.get(0))
                .unwrap_or(0);
            let rows = client
                .query(
                    "SELECT record_json FROM timeline_records WHERE timeline_key = $1 \
                     ORDER BY record_id OFFSET $2 LIMIT $3",
                    &[
                        &timeline_key,
                        &(skip.min(i64::MAX as usize) as i64),
                        &(top.min(super::types::MAX_TIMELINE_RECORDS) as i64),
                    ],
                )
                .await
                .map_err(ControlError::backend)?;
            Ok((
                change_id as i32,
                rows.iter()
                    .filter_map(|row| serde_json::from_str(&row.get::<_, String>(0)).ok())
                    .collect(),
            ))
        }
        .await;
        self.return_reader(client).await;
        result
    }

    async fn prune_timelines(&self, before_us: i64) -> Result<u64, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .execute(
                "DELETE FROM timelines WHERE updated_at_us < $1",
                &[&before_us],
            )
            .await
            .map_err(ControlError::backend);
        self.return_reader(client).await;
        result
    }

    async fn enqueue_webhook_delivery(
        &self,
        delivery: &WebhookDeliveryRecord,
    ) -> Result<bool, ControlError> {
        self.aux
            .enqueue_webhook_delivery(delivery)
            .await
            .map_err(ControlError::backend)
    }

    async fn claim_webhook_deliveries(
        &self,
        limit: usize,
        lease_duration_secs: u64,
    ) -> Result<Vec<WebhookDeliveryRecord>, ControlError> {
        self.aux
            .claim_webhook_deliveries(limit, lease_duration_secs)
            .await
            .map_err(ControlError::backend)
    }

    async fn renew_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        lease_duration_secs: u64,
    ) -> Result<bool, ControlError> {
        self.aux
            .renew_webhook_delivery(delivery_id, lease_token, lease_duration_secs)
            .await
            .map_err(ControlError::backend)
    }

    async fn complete_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
    ) -> Result<bool, ControlError> {
        self.aux
            .complete_webhook_delivery(delivery_id, lease_token)
            .await
            .map_err(ControlError::backend)
    }

    async fn fail_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        error: &str,
        permanent: bool,
        retry_delay: Option<std::time::Duration>,
    ) -> Result<bool, ControlError> {
        self.aux
            .fail_webhook_delivery(delivery_id, lease_token, error, permanent, retry_delay)
            .await
            .map_err(ControlError::backend)
    }

    async fn get_webhook_delivery(
        &self,
        delivery_id: &str,
    ) -> Result<Option<WebhookDeliveryRecord>, ControlError> {
        self.aux
            .get_webhook_delivery(delivery_id)
            .await
            .map_err(ControlError::backend)
    }

    async fn count_dead_letter_webhook_deliveries(&self) -> Result<u64, ControlError> {
        self.aux
            .count_dead_letter_webhook_deliveries()
            .await
            .map_err(ControlError::backend)
    }

    async fn recover_webhook_deliveries(&self) -> Result<u64, ControlError> {
        self.aux
            .recover_webhook_deliveries()
            .await
            .map_err(ControlError::backend)
    }

    async fn prune_webhook_deliveries(
        &self,
        before_us: i64,
        limit: usize,
    ) -> Result<u64, ControlError> {
        self.aux
            .prune_webhook_deliveries(before_us, limit)
            .await
            .map_err(ControlError::backend)
    }

    async fn park_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        error: &str,
        retry_delay_secs: u64,
    ) -> Result<bool, ControlError> {
        self.aux
            .park_webhook_delivery(delivery_id, lease_token, error, retry_delay_secs)
            .await
            .map_err(ControlError::backend)
    }

    async fn requeue_webhook_delivery(&self, delivery_id: &str) -> Result<bool, ControlError> {
        self.aux
            .requeue_webhook_delivery(delivery_id)
            .await
            .map_err(ControlError::backend)
    }

    async fn list_webhook_deliveries(
        &self,
        state: Option<WebhookDeliveryStatus>,
        limit: usize,
    ) -> Result<Vec<WebhookDeliverySummary>, ControlError> {
        self.aux
            .list_webhook_deliveries(state, limit)
            .await
            .map_err(ControlError::backend)
    }

    async fn webhook_deliveries_present(
        &self,
        delivery_ids: &[String],
    ) -> Result<BTreeSet<String>, ControlError> {
        self.aux
            .webhook_deliveries_present(delivery_ids)
            .await
            .map_err(ControlError::backend)
    }

    async fn webhook_queue_stats(&self) -> Result<WebhookQueueStats, ControlError> {
        self.aux
            .webhook_queue_stats()
            .await
            .map_err(ControlError::backend)
    }

    async fn load_webhook_watchdog_cursor(
        &self,
        scope: &str,
    ) -> Result<Option<WebhookWatchdogCursor>, ControlError> {
        self.aux
            .load_webhook_watchdog_cursor(scope)
            .await
            .map_err(ControlError::backend)
    }

    async fn store_webhook_watchdog_cursor(
        &self,
        cursor: &WebhookWatchdogCursor,
    ) -> Result<(), ControlError> {
        self.aux
            .store_webhook_watchdog_cursor(cursor)
            .await
            .map_err(ControlError::backend)
    }

    async fn upsert_webhook_redelivery(
        &self,
        record: &WebhookRedeliveryRecord,
    ) -> Result<(), ControlError> {
        self.aux
            .upsert_webhook_redelivery(record)
            .await
            .map_err(ControlError::backend)
    }

    async fn load_webhook_redelivery(
        &self,
        delivery_guid: &str,
    ) -> Result<Option<WebhookRedeliveryRecord>, ControlError> {
        self.aux
            .load_webhook_redelivery(delivery_guid)
            .await
            .map_err(ControlError::backend)
    }

    async fn open_webhook_redeliveries(
        &self,
        limit: usize,
    ) -> Result<Vec<WebhookRedeliveryRecord>, ControlError> {
        self.aux
            .open_webhook_redeliveries(limit)
            .await
            .map_err(ControlError::backend)
    }

    async fn resolve_webhook_redelivery(
        &self,
        delivery_guid: &str,
        resolved_at_us: i64,
    ) -> Result<bool, ControlError> {
        self.aux
            .resolve_webhook_redelivery(delivery_guid, resolved_at_us)
            .await
            .map_err(ControlError::backend)
    }

    async fn queue_stats(&self) -> Result<QueueStats, ControlError> {
        // NOTE: must stay on `TxScope::full()` (via `read`) — `ready` is the
        // global `ready_count` counter while the other five counts are
        // loaded-subset lengths; a narrower scope would mix the two.
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
        .await
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
        .await
    }
}
