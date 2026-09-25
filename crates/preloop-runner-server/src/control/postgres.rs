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
use std::collections::{BTreeSet, VecDeque};
use tokio_postgres::{Client, NoTls};

/// The Postgres control backend. `client` is the single writer behind a
/// mutex; `readers` is a pool of connections that serve read-only commands
/// concurrently, so a read never queues behind a write. (A `deadpool` pool
/// is the production upgrade; the channel pool here gives the same
/// read/write separation without a new dependency.)
pub(crate) struct PostgresBackend {
    client: tokio::sync::Mutex<Client>,
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
        // Apply pending migrations (see `schema::POSTGRES_MIGRATIONS`) before
        // the idempotent DDL. `schema_migrations` doubles as the version
        // pointer (Postgres has no `PRAGMA user_version`).
        let max_version: Option<i64> = client
            .query_opt("SELECT MAX(version) FROM control.schema_migrations", &[])
            .await
            .map_err(ControlError::backend)?
            .and_then(|row| row.get(0));
        if let Some(version) = max_version {
            for (migration, sql) in super::schema::POSTGRES_MIGRATIONS {
                if *migration <= version {
                    continue;
                }
                client
                    .batch_execute(sql)
                    .await
                    .map_err(ControlError::backend)?;
            }
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
        // A small pool of read connections. Read-only commands check one
        // out and run a `BEGIN` read transaction, so a read never queues
        // behind the writer's mutex.
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
            client: tokio::sync::Mutex::new(client),
            readers_tx,
            readers_rx: tokio::sync::Mutex::new(readers_rx),
            cipher,
            aux,
            pool_assignments_enabled: std::sync::atomic::AtomicBool::new(pool_assignments_enabled),
            require_job_assignments: std::sync::atomic::AtomicBool::new(require_job_assignments),
            runner_liveness_timeout: std::sync::atomic::AtomicU64::new(
                runner_liveness_timeout.as_nanos() as u64,
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
    pub(crate) async fn transact_scoped<T>(
        &self,
        scope: &TxScope,
        f: impl FnOnce(&mut TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        let mut client = self.client.lock().await;
        let txn = client.transaction().await.map_err(ControlError::backend)?;
        // Serialize writers across processes: the `client` mutex orders
        // writers inside this process, but a second engine sharing the
        // database takes no part in it. `pg_advisory_xact_lock` is the
        // cross-process writer lock — it blocks until every other holder
        // commits or rolls back, then is released automatically at the end
        // of this transaction. Without it two processes `load_txstate` the
        // same working set under READ COMMITTED and last-write-wins on
        // `write_txstate` (lost updates). Reads stay concurrent on the
        // reader pool; only the write command serializes, matching the
        // single-writer command contract.
        txn.batch_execute(&format!(
            "SELECT pg_advisory_xact_lock({POSTGRES_WRITER_LOCK_KEY})"
        ))
        .await
        .map_err(ControlError::backend)?;
        let (tx, effective_scope) = load_txstate(&txn, scope, &self.cipher).await?;
        let mut tx = tx.with_config(self.config());
        // The command body is synchronous CPU over the working set; keep it
        // off the executor thread so a heavy command cannot starve reads.
        let result = run_blocking(|| f(&mut tx))?;
        write_txstate(&txn, &tx, &effective_scope, &self.cipher).await?;
        txn.commit().await.map_err(ControlError::backend)?;
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
        let client = self.client.lock().await;
        client
            .query_opt(
                "SELECT status, queue_kind, queue_position, seq FROM jobs WHERE job_id=$1",
                &[&job_id],
            )
            .await
            .ok()
            .flatten()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
    }

    /// Test-only: every ready job's `queue_position`, keyed by `job_id`.
    /// Used by the FIFO regression test to assert a new job takes the global
    /// MAX+1 and existing rows keep their exact positions.
    #[cfg(test)]
    pub(crate) async fn ready_positions(&self) -> std::collections::BTreeMap<String, i64> {
        let client = self.client.lock().await;
        client
            .query(
                "SELECT job_id, queue_position FROM jobs WHERE queue_kind='ready'",
                &[],
            )
            .await
            .unwrap_or_default()
            .iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, i64>(1)))
            .collect()
    }

    /// The read body: one consistent snapshot on `client`, rolled back.
    async fn read_on<T>(
        &self,
        client: &mut Client,
        scope: &TxScope,
        f: impl FnOnce(&TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        let txn = client.transaction().await.map_err(ControlError::backend)?;
        let (tx, _effective_scope) = load_txstate(&txn, scope, &self.cipher).await?;
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
            let row = client
                .query_opt(
                    "SELECT run_id, record_blob FROM runs \
                     WHERE webhook_delivery_id = $1 AND workflow_path = $2",
                    &[&delivery_id, &workflow_path],
                )
                .await
                .map_err(ControlError::backend)?;
            let Some(row) = row else {
                return Ok(None);
            };
            let run_id_s: String = row.get(0);
            let record: Vec<u8> = row.get(1);
            let mut run: RunRecord = blob(&self.cipher, &record)?;
            run.jobs.clear();
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
                    "SELECT selected.run_id, selected.record_blob, j.job_id, j.status, \
                            j.queue_kind, js.steps_blob \
                     FROM ( \
                         SELECT run_id, record_blob, \
                                CASE WHEN status IN ('success','failure','skipped','cancelled') \
                                     THEN 1 ELSE 0 END AS terminal_rank, \
                                COALESCE(completed_at_us, started_at_us, created_at_us) AS sort_at \
                         FROM runs \
                         WHERE ($1::TEXT IS NULL OR POSITION($1 IN workflow_path) > 0) \
                           AND ($2::TEXT IS NULL OR status = $2) \
                           AND ($3::TEXT IS NULL OR event = $3) \
                         ORDER BY terminal_rank, sort_at DESC \
                         LIMIT $4 \
                     ) AS selected \
                     LEFT JOIN jobs j ON j.run_id = selected.run_id \
                     LEFT JOIN job_steps js ON js.agent_job_id = ( \
                         SELECT jr.agent_job_id FROM job_requests jr \
                         WHERE jr.run_id = j.run_id AND jr.job_id = j.job_id \
                         ORDER BY jr.request_id DESC LIMIT 1 \
                     ) \
                     ORDER BY selected.terminal_rank, selected.sort_at DESC, j.job_id",
                    &[&filter.workflow, &filter.status, &filter.event, &limit],
                )
                .await
                .map_err(ControlError::backend)?;
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
                    let record: Vec<u8> = row.get(1);
                    current_id = Some(run_id);
                    current_run = Some(blob(&self.cipher, &record)?);
                }
                let job_id: Option<String> = row.get(2);
                let status: Option<String> = row.get(3);
                let queue_kind: Option<String> = row.get(4);
                let steps: Option<Vec<u8>> = row.get(5);
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
        let client = self.client.lock().await;
        client
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
            .map_err(ControlError::backend)?;
        Ok(())
    }
    async fn terminal_job_rows(&self) -> Result<BTreeSet<(RunId, JobId)>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query(
                "SELECT run_id, job_id FROM jobs \
                 WHERE status IN ('success','failure','skipped','cancelled')",
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

async fn load_txstate(
    conn: &Tx<'_>,
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
            let rows = conn
                .query(
                    "SELECT running_holder, pending_holders FROM concurrency_groups",
                    &[],
                )
                .await
                .map_err(ControlError::backend)?;
            for row in rows {
                let running: Option<Vec<u8>> = row.get(0);
                let pending: Vec<u8> = row.get(1);
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

    let mut tx = TxState {
        ready_queue_loaded: scope.runs.is_none() || scope.ready_queue,
        ..Default::default()
    };

    // Runs — scoped to `scope.runs` when set.
    let run_ids: Vec<String> = scope
        .runs
        .as_ref()
        .map(|s| s.iter().map(|id| id.0.to_string()).collect())
        .unwrap_or_default();
    let runs_rows = if let Some(runs) = scope.runs.as_ref() {
        if runs.is_empty() {
            Vec::new()
        } else {
            conn.query(
                "SELECT run_id, record_blob FROM runs WHERE run_id = ANY($1)",
                &[&run_ids],
            )
            .await
            .map_err(ControlError::backend)?
        }
    } else {
        conn.query("SELECT run_id, record_blob FROM runs", &[])
            .await
            .map_err(ControlError::backend)?
    };
    for row in runs_rows {
        let run_id_s: String = row.get(0);
        let record: Vec<u8> = row.get(1);
        let run_id = parse_run_id(&run_id_s);
        let mut run: RunRecord = blob(cipher, &record)?;
        run.jobs.clear();
        tx.runs.insert(run_id, run);
        tx.loaded.runs.insert(run_id);
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
        let base = "SELECT run_id, job_id, status, queue_kind, queue_position, seq, \
             reaper_first_seen_us, expand_generation, enqueued_at_us, payload_blob FROM jobs";
        // `runs == None` means the run predicate is TRUE, which makes the
        // whole OR true — emit no WHERE and load every job. Only when the
        // scope names a run set do the queue-kind clauses matter.
        let mut conds: Vec<String> = Vec::new();
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();
        if let Some(runs) = scope.runs.as_ref() {
            if runs.is_empty() {
                conds.push("false".to_owned());
            } else {
                params.push(&run_ids);
                conds.push(format!("run_id = ANY(${})", params.len()));
            }
            if !kinds.is_empty() {
                params.push(&kinds);
                conds.push(format!("queue_kind = ANY(${})", params.len()));
            }
        }
        let sql = if conds.is_empty() {
            format!("{base} ORDER BY seq")
        } else {
            format!("{base} WHERE {} ORDER BY seq", conds.join(" OR "))
        };
        conn.query(&sql, &params)
            .await
            .map_err(ControlError::backend)?
    };
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
        let payload: Option<Vec<u8>> = row.get(9);
        let run_id = parse_run_id(&run_id_s);
        let job_id = JobId(job_id_s);
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
            // `enqueued_at_us` is authoritative: the payload blob is only
            // re-sealed on a slot change, so a rewritten enqueue time would
            // otherwise be masked by the stale blob.
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
    // Global ready-queue size — unscoped COUNT, not the loaded subset.
    tx.ready_count = conn
        .query_one("SELECT COUNT(*) FROM jobs WHERE queue_kind='ready'", &[])
        .await
        .map(|r| r.get(0))
        .unwrap_or(0);
    tx.next_queue_position = conn
        .query_one(
            "SELECT COALESCE(MAX(queue_position),0)+1 FROM jobs WHERE queue_kind='ready'",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(1);
    tx.next_seq = conn
        .query_one("SELECT COALESCE(MAX(seq),0)+1 FROM jobs", &[])
        .await
        .map(|r| r.get(0))
        .unwrap_or(1);
    // Global queue-front labels — unscoped, for pool next-image selection.
    tx.next_queue_labels = conn
        .query_opt(
            "SELECT payload_blob FROM jobs WHERE queue_kind='ready' \
             ORDER BY queue_position LIMIT 1",
            &[],
        )
        .await
        .ok()
        .flatten()
        .and_then(|r| r.get::<_, Option<Vec<u8>>>(0))
        .and_then(|b| blob::<QueuedJob>(cipher, &b).ok())
        .map(|j| j.runs_on)
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
        let (sql, ids) = match scope.runs.as_ref() {
            None => (
                "SELECT agent_job_id, steps_blob, revision FROM job_steps".to_owned(),
                None,
            ),
            Some(set) if set.is_empty() => (
                "SELECT agent_job_id, steps_blob, revision FROM job_steps WHERE false".to_owned(),
                None,
            ),
            Some(set) => (
                "SELECT agent_job_id, steps_blob, revision FROM job_steps \
                 WHERE agent_job_id IN (SELECT agent_job_id FROM job_requests \
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
            let agent_job_id_s: String = row.get(0);
            let steps: Vec<u8> = row.get(1);
            let revision: i64 = row.get(2);
            let agent_job_id = parse_uuid(&agent_job_id_s);
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
    if scope.concurrency {
        for row in conn
            .query(
                "SELECT repo, group_name, display_name, running_holder, pending_holders \
             FROM concurrency_groups",
                &[],
            )
            .await
            .map_err(ControlError::backend)?
        {
            let repo: String = row.get(0);
            let group_name: String = row.get(1);
            let display_name: String = row.get(2);
            let running: Option<Vec<u8>> = row.get(3);
            let pending: Vec<u8> = row.get(4);
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
        for row in conn
            .query("SELECT run_id, repo, group_name FROM holder_keys", &[])
            .await
            .map_err(ControlError::backend)?
        {
            let run_id_s: String = row.get(0);
            let repo: String = row.get(1);
            let group_name: String = row.get(2);
            let run_id = parse_run_id(&run_id_s);
            tx.holder_keys
                .entry(run_id)
                .or_default()
                .push((repo, group_name));
            tx.loaded.holder_key_runs.insert(run_id);
        }
        for row in conn
            .query(
                "SELECT run_id, job_ids, gates_blob, acquired_keys FROM jobset_admissions",
                &[],
            )
            .await
            .map_err(ControlError::backend)?
        {
            let run_id_s: String = row.get(0);
            let job_ids_s: String = row.get(1);
            let gates: Vec<u8> = row.get(2);
            let acquired: Vec<u8> = row.get(3);
            let run_id = parse_run_id(&run_id_s);
            let job_ids: BTreeSet<JobId> = serde_json::from_str::<BTreeSet<String>>(&job_ids_s)
                .unwrap_or_default()
                .into_iter()
                .map(JobId)
                .collect();
            let id = JobSetId { run_id, job_ids };
            if let (Ok(gates), Ok(acquired_keys)) = (blob(cipher, &gates), blob(cipher, &acquired))
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
        tx.workflow_run_counters.insert(key, value as u64);
    }

    Ok((tx, effective_scope))
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
async fn write_txstate(
    conn: &Tx<'_>,
    tx: &TxState,
    scope: &TxScope,
    cipher: &store::Envelope,
) -> Result<(), ControlError> {
    let now_us = system_to_us(std::time::SystemTime::now());

    // Runs.
    for (run_id, run) in &tx.runs {
        let mut record = run.clone();
        record.jobs.clear();
        let value = store::run_record_value(&record).map_err(ControlError::backend)?;
        let record_blob = unblob(cipher, &value)?;
        conn.execute(
            "INSERT INTO runs (run_id, status, run_number, run_attempt, run_name, event, \
             workflow_path, conclusion, webhook_delivery_id, record_blob, created_at_us, \
             started_at_us, completed_at_us) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) \
             ON CONFLICT(run_id) DO UPDATE SET status=excluded.status, \
             conclusion=excluded.conclusion, record_blob=excluded.record_blob, \
             started_at_us=excluded.started_at_us, completed_at_us=excluded.completed_at_us",
            &[
                &run_id.0.to_string(),
                &status_str(run.status),
                &(run.run_number as i64),
                &(run.run_attempt as i64),
                &run.run_name,
                &run.event,
                &run.workflow_path_str,
                &run.conclusion,
                &run.webhook_delivery_id,
                &record_blob,
                &system_to_us(run.created_at.into()),
                &run.started_at.map(|t| system_to_us(t.into())),
                &run.completed_at.map(|t| system_to_us(t.into())),
            ],
        )
        .await
        .map_err(ControlError::backend)?;
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
    // Fresh position/seq allocators for genuinely new jobs (those not in
    // `job_row_state`); existing jobs keep their loaded values via write_job.
    let mut next_pos = tx.next_queue_position;
    let mut next_seq = tx.next_seq;

    // Resolves `queue_position`/`seq` from `fresh` (newly enqueued/requeued
    // via `tx.queue`), the persisted kind in `tx.loaded.jobs` vs the written
    // kind (a transition clears position + takes a fresh FIFO seq), and
    // `job_row_state` (only a job staying in the SAME slot keeps its loaded
    // values, so a no-op scoped write is byte-identical).
    async fn write_job(
        conn: &Tx<'_>,
        tx: &TxState,
        next_seq: &mut i64,
        next_pos: &mut i64,
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
        let (base_id, runs_on, runner_group, enqueued_us, payload) = match job {
            Some(j) => (
                j.base_id.clone(),
                serde_json::to_string(&j.runs_on).unwrap_or_default(),
                j.runner_group.clone(),
                Some(j.enqueued_at_unix_nanos / 1000),
                // Unchanged slot → unchanged payload; `None` keeps the stored
                // blob instead of re-sealing a fresh random IV every tx.
                if same_slot {
                    None
                } else {
                    Some(unblob(cipher, j)?)
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
             claimed_by, claimed_at_us, expand_generation, payload_blob) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) \
             ON CONFLICT(run_id,job_id) DO UPDATE SET status=excluded.status, \
             queue_kind=excluded.queue_kind, queue_position=excluded.queue_position, \
             seq=excluded.seq, enqueued_at_us=excluded.enqueued_at_us, \
             reaper_first_seen_us=excluded.reaper_first_seen_us, \
             claimed_by=excluded.claimed_by, claimed_at_us=excluded.claimed_at_us, \
             expand_generation=excluded.expand_generation, \
             payload_blob=COALESCE(excluded.payload_blob, jobs.payload_blob)",
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
                &payload,
            ],
        )
        .await
        .map_err(ControlError::backend)?;
        Ok(())
    }

    // `ready_index` = persisted ready jobs (same slot → keep pos/seq);
    // `tx.queue` = newly enqueued + requeued (push_ready) → always fresh.
    for job in &tx.ready_index {
        write_job(
            conn,
            tx,
            &mut next_seq,
            &mut next_pos,
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
            &mut next_seq,
            &mut next_pos,
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
            &mut next_seq,
            &mut next_pos,
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
            &mut next_seq,
            &mut next_pos,
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
            &mut next_seq,
            &mut next_pos,
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
            &mut next_seq,
            &mut next_pos,
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
            &mut next_seq,
            &mut next_pos,
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
                &mut next_seq,
                &mut next_pos,
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
                    &mut next_seq,
                    &mut next_pos,
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
    delete_scoped(
        conn,
        "github_token_requests",
        "request_id",
        &tx.loaded.requests,
        |id| *id,
        scope.runs.is_none(),
    )
    .await?;
    for (request_id, req) in &tx.github_token_requests {
        conn.execute(
            "INSERT INTO github_token_requests (request_id, request_blob) VALUES ($1,$2)",
            &[request_id, &unblob(cipher, req)?],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    // Grants + OIDC — run-scoped.
    delete_scoped_runs(conn, "id_token_grants", scope).await?;
    for ((run_id, job_id), granted) in &tx.id_token_grants {
        conn.execute(
            "INSERT INTO id_token_grants (run_id, job_id, granted) VALUES ($1,$2,$3)",
            &[&run_id.0.to_string(), &job_id.0, &(*granted as i64)],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    delete_scoped_runs(conn, "oidc_job_contexts", scope).await?;
    for ((run_id, job_id), ctx) in &tx.oidc_job_contexts {
        conn.execute(
            "INSERT INTO oidc_job_contexts (run_id, job_id, context_blob) VALUES ($1,$2,$3)",
            &[&run_id.0.to_string(), &job_id.0, &unblob(cipher, ctx)?],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    for (agent_job_id, steps) in &tx.job_steps {
        let revision = tx
            .job_steps_revision
            .get(agent_job_id)
            .copied()
            .unwrap_or(0);
        conn.execute(
            "INSERT INTO job_steps (agent_job_id, steps_blob, revision) VALUES ($1,$2,$3) \
             ON CONFLICT(agent_job_id) DO UPDATE SET steps_blob=excluded.steps_blob, \
             revision=excluded.revision",
            &[
                &agent_job_id.to_string(),
                &unblob(cipher, steps)?,
                &(revision as i64),
            ],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    for agent_job_id in &tx.loaded.step_attempts {
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

    // Sessions: rebuild from the unified maps, scoped to loaded sessions.
    delete_scoped(
        conn,
        "runner_sessions",
        "session_id",
        &tx.loaded.sessions,
        |s| s.clone(),
        scope.sessions.is_none(),
    )
    .await?;
    async fn write_session(
        conn: &Tx<'_>,
        session_id: &str,
        runner_id: Option<i64>,
        protocol: SessionProtocol,
        tx: &TxState,
        now_us: i64,
        cipher: &store::Envelope,
    ) -> Result<(), ControlError> {
        // Seal the session AES key (see the SQLite backend's write_session).
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
        let verified = tx.verified_sessions.contains(session_id) as i64;
        conn.execute(
            "INSERT INTO runner_sessions (session_id, runner_id, protocol, encryption_blob, \
             active_request_id, last_seen_at_us, verified, created_at_us) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            &[
                &session_id,
                &runner_id,
                &protocol.as_str(),
                &encryption,
                &active_request_id,
                &last_seen_us,
                &verified,
                &created_us,
            ],
        )
        .await
        .map_err(ControlError::backend)?;
        Ok(())
    }
    for (session_id, runner_id) in &tx.broker_session_runners {
        write_session(
            conn,
            session_id,
            Some(*runner_id),
            SessionProtocol::Broker,
            tx,
            now_us,
            cipher,
        )
        .await?;
    }
    for (session_id, session) in &tx.sessions {
        write_session(
            conn,
            session_id,
            Some(session.runner_id),
            SessionProtocol::Azdo,
            tx,
            now_us,
            cipher,
        )
        .await?;
    }
    // Compatibility sessions (e.g. the implicit `default` session) own no
    // registered runner, so they are absent from `broker_session_runners` and
    // `sessions`. They still need a `runner_sessions` row — with NULL
    // `runner_id` — so that `broker_messages` and `active_request_id` foreign
    // keys resolve and the active-request mapping survives the commit.
    let compat_session_ids: std::collections::BTreeSet<&String> = tx
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
        write_session(
            conn,
            session_id,
            None,
            SessionProtocol::Compat,
            tx,
            now_us,
            cipher,
        )
        .await?;
    }

    // Inflight messages — session-scoped.
    delete_scoped(
        conn,
        "broker_messages",
        "session_id",
        &tx.loaded.sessions,
        |s| s.clone(),
        scope.sessions.is_none(),
    )
    .await?;
    for (session_id, messages) in &tx.inflight_messages {
        for (message_id, msg) in messages {
            conn.execute(
                "INSERT INTO broker_messages (session_id, message_id, message_blob, \
                 created_at_us) VALUES ($1,$2,$3,$4)",
                &[session_id, message_id, &unblob(cipher, msg)?, &now_us],
            )
            .await
            .map_err(ControlError::backend)?;
        }
    }

    // Concurrency — always-global, written only when the scope loaded it.
    if scope.concurrency {
        conn.execute("DELETE FROM concurrency_groups", &[])
            .await
            .map_err(ControlError::backend)?;
        for ((repo, group_name), group) in &tx.concurrency_groups {
            conn.execute(
                "INSERT INTO concurrency_groups (repo, group_name, display_name, running_holder, \
             pending_holders) VALUES ($1,$2,$3,$4,$5)",
                &[
                    repo,
                    group_name,
                    &group.display_name,
                    &group
                        .running
                        .as_ref()
                        .map(|h| unblob(cipher, h))
                        .transpose()?,
                    &unblob(cipher, &group.pending)?,
                ],
            )
            .await
            .map_err(ControlError::backend)?;
        }
        conn.execute("DELETE FROM holder_keys", &[])
            .await
            .map_err(ControlError::backend)?;
        for (run_id, keys) in &tx.holder_keys {
            for (repo, group_name) in keys {
                conn.execute(
                    "INSERT INTO holder_keys (run_id, repo, group_name) VALUES ($1,$2,$3)",
                    &[&run_id.0.to_string(), repo, group_name],
                )
                .await
                .map_err(ControlError::backend)?;
            }
        }
        conn.execute("DELETE FROM jobset_admissions", &[])
            .await
            .map_err(ControlError::backend)?;
        for (id, admission) in &tx.jobset_admissions {
            let job_ids: Vec<String> = id.job_ids.iter().map(|j| j.0.clone()).collect();
            conn.execute(
                "INSERT INTO jobset_admissions (run_id, job_ids, gates_blob, acquired_keys) \
             VALUES ($1,$2,$3,$4)",
                &[
                    &id.run_id.0.to_string(),
                    &serde_json::to_string(&job_ids).unwrap_or_default(),
                    &unblob(cipher, &admission.gates)?,
                    &unblob(cipher, &admission.acquired_keys)?,
                ],
            )
            .await
            .map_err(ControlError::backend)?;
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
    delete_scoped_runs(conn, "run_concurrency", scope).await?;
    for (run_id, c) in &tx.run_concurrency {
        conn.execute(
            "INSERT INTO run_concurrency (run_id, concurrency_blob) VALUES ($1,$2)",
            &[&run_id.0.to_string(), &unblob(cipher, c)?],
        )
        .await
        .map_err(ControlError::backend)?;
    }

    // Assignments, pool pending, cancellations — run-scoped.
    delete_scoped_queue(conn, "job_assignments", scope).await?;
    for ((run_id, job_id), record) in &tx.job_assignments {
        conn.execute(
            "INSERT INTO job_assignments (run_id, job_id, runner_id, at_us, first_at_us) \
             VALUES ($1,$2,$3,$4,$5)",
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
    delete_scoped_queue(conn, "pool_pending", scope).await?;
    for ((run_id, job_id), at) in &tx.pool_pending {
        conn.execute(
            "INSERT INTO pool_pending (run_id, job_id, at_us) VALUES ($1,$2,$3)",
            &[&run_id.0.to_string(), &job_id.0, &system_to_us(*at)],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    delete_scoped_runs(conn, "cancellation_queue", scope).await?;
    for c in &tx.cancellation_queue {
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

    // Counters — always loaded, so always written.
    for (name, value) in [
        ("next_message_id", tx.next_message_id),
        ("next_runner_id", tx.next_runner_id),
        ("next_request_id", tx.next_request_id),
    ] {
        conn.execute(
            "INSERT INTO counters (name, value) VALUES ($1,$2) \
             ON CONFLICT(name) DO UPDATE SET value=excluded.value",
            &[&name, &value],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    for (key, value) in &tx.workflow_run_counters {
        conn.execute(
            "INSERT INTO workflow_run_counters (key, value) VALUES ($1,$2) \
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            &[key, &(*value as i64)],
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
        // all session-scoped. `concurrency: true` widens `runs` with every
        // holder's run (a `cancel_in_progress` submit cancels the running
        // holder). `runs_referenced` is off: submit never promotes queued
        // jobs, so ready/blocked runs stay foreign.
        let scope = TxScope {
            runs: Some(run_ids),
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
            .await
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
        .await
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
            // `poll` claims ready work only — no blocked-job promotion, no
            // concurrency-gate mutation. Loading those families is O(#blocked
            // + #gates) `payload_blob`s for no benefit.
            blocked_jobs: false,
            sessions: Some(BTreeSet::from([poll.session_id.clone()])),
            concurrency: false,
            runs_referenced: true,
            pending_expansions: false,
            runs_via_requests: false,
            job_requests_all: false,
        };
        self.transact_scoped(&scope, |tx| commands::poll_session_tx(tx, poll))
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
        // `runs: Some(empty)` + `pending_expansions` loads only the deferred
        // nodes; `runs_referenced` widens `runs` to their runs so
        // `plan_expansion` sees the claimed node's `RunRecord`.
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
            tx.expanding_jobs
                .insert((job.run_id, job.job_id.clone()), job.clone());
            Ok(Some(ExpansionClaim {
                job,
                generation,
                plan,
            }))
        })
        .await
    }

    async fn apply_expansion(
        &self,
        claim: ExpansionApply,
    ) -> Result<crate::runtime_scheduling::SchedulingOutcome, ControlError> {
        // Scoped to the expanded node's run — `sched::apply_expansion`
        // registers the built legs into that same run.
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
        self.read(|tx| {
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
        .await
    }

    async fn create_log(&self, plan_id: &str) -> Result<i64, ControlError> {
        let mut client = self.client.lock().await;
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
        let next_id: i64 = row.get(0);
        let key = format!("{plan_id}/{next_id}");
        let now_us = system_to_us(std::time::SystemTime::now());
        tx.execute(
            "INSERT INTO log_files(log_key, byte_count, line_count, updated_at_us) \
             VALUES ($1, 0, 0, $2)",
            &[&key, &now_us],
        )
        .await
        .map_err(ControlError::backend)?;
        tx.commit().await.map_err(ControlError::backend)?;
        Ok(next_id)
    }

    async fn append_log_chunk(
        &self,
        key: &str,
        chunk_index: i64,
        payload: &[u8],
        byte_count: i64,
        line_count: i64,
    ) -> Result<(), ControlError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(ControlError::backend)?;
        let now_us = system_to_us(std::time::SystemTime::now());
        tx.execute(
            "INSERT INTO log_files(log_key, byte_count, line_count, updated_at_us) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT(log_key) DO UPDATE SET \
               byte_count = EXCLUDED.byte_count, \
               line_count = EXCLUDED.line_count, \
               updated_at_us = EXCLUDED.updated_at_us",
            &[&key, &byte_count, &line_count, &now_us],
        )
        .await
        .map_err(ControlError::backend)?;
        tx.execute(
            "INSERT INTO log_chunks(log_key, chunk_index, payload, written_at_us) \
             VALUES ($1, $2, $3, $4)",
            &[&key, &chunk_index, &payload, &now_us],
        )
        .await
        .map_err(ControlError::backend)?;
        let cutoff = byte_count - crate::memory_caps::MAX_LOG_BYTES_PER_KEY as i64;
        if cutoff > 0 {
            tx.execute(
                "DELETE FROM log_chunks WHERE log_key = $1 AND chunk_index <= $2",
                &[&key, &cutoff],
            )
            .await
            .map_err(ControlError::backend)?;
        }
        tx.commit().await.map_err(ControlError::backend)
    }

    async fn delete_log(&self, key: &str) -> Result<(), ControlError> {
        let client = self.client.lock().await;
        client
            .execute("DELETE FROM log_files WHERE log_key = $1", &[&key])
            .await
            .map_err(ControlError::backend)?;
        Ok(())
    }

    async fn load_logs(&self) -> Result<Vec<DurableLog>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query(
                "SELECT f.log_key, f.byte_count, f.line_count, c.payload \
                 FROM log_files f \
                 LEFT JOIN log_chunks c ON c.log_key = f.log_key \
                 ORDER BY f.updated_at_us, f.log_key, c.chunk_index",
                &[],
            )
            .await
            .map_err(ControlError::backend)
            .map(|rows| {
                let mut logs: Vec<DurableLog> = Vec::new();
                for row in rows {
                    let key: String = row.get(0);
                    if logs.last().is_none_or(|log| log.key != key) {
                        logs.push(DurableLog {
                            key,
                            payload: Vec::new(),
                            byte_count: row.get::<_, i64>(1).max(0) as usize,
                            line_count: row.get::<_, i64>(2).max(0) as usize,
                        });
                    }
                    if let Some(payload) = row.get::<_, Option<Vec<u8>>>(3) {
                        logs.last_mut()
                            .expect("log row was inserted")
                            .payload
                            .extend_from_slice(&payload);
                    }
                }
                logs
            });
        self.return_reader(client).await;
        result
    }

    async fn store_meta(&self, meta: &crate::store::MetaSnapshot) -> Result<(), ControlError> {
        let value = unblob(&self.cipher, meta)?;
        let client = self.client.lock().await;
        client
            .execute(
                "INSERT INTO meta(key, value) VALUES ('local_state', $1) \
                 ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value",
                &[&value],
            )
            .await
            .map_err(ControlError::backend)?;
        Ok(())
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
