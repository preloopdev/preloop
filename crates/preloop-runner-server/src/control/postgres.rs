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
use std::collections::{BTreeMap, BTreeSet};
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

/// Writer connections per node (`PRELOOP_PG_WRITERS`, default 16).
const WRITERS_ENV: &str = "PRELOOP_PG_WRITERS";
/// Reader connections per node (`PRELOOP_PG_READERS`, default 16).
const READERS_ENV: &str = "PRELOOP_PG_READERS";
const DEFAULT_POOL_SIZE: usize = 16;

/// Pool size from `var`, clamped to at least one connection. An unset or
/// unparsable value takes the default; size the pools so every node's
/// `writers + readers + 2` (wake listener, aux store) fits `max_connections`.
fn pool_size(var: &str) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_POOL_SIZE)
        .max(1)
}

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
        // Writer and reader pools. Every claim, completion, submit, timeline
        // write and webhook write holds a writer for its whole transaction,
        // so the writer count is a hard cap on in-flight writes per node.
        let writers = pool_size(WRITERS_ENV);
        let readers = pool_size(READERS_ENV);
        let (writers_tx, writers_rx) = tokio::sync::mpsc::channel(writers);
        let (readers_tx, readers_rx) = tokio::sync::mpsc::channel(readers);
        let extra =
            futures::future::try_join_all((1..writers + readers).map(|_| connect_one(url))).await?;
        let mut extra = extra.into_iter();
        for conn in std::iter::once(client).chain(extra.by_ref().take(writers - 1)) {
            writers_tx
                .send(conn)
                .await
                .map_err(|_| ControlError::backend(anyhow::anyhow!("writer pool closed")))?;
        }
        for conn in extra {
            readers_tx
                .send(conn)
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
        self.transact_reserving(scope, 0, f).await
    }

    /// `transact_scoped` for a command that mints up to `request_ids` new
    /// `job_requests` rows. The ids are drawn from `request_id_seq` up front
    /// so concurrent writers on different runs never share a primary key.
    async fn transact_reserving<T>(
        &self,
        scope: &TxScope,
        request_ids: usize,
        f: impl FnOnce(&mut TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        if scope.include_archived {
            return Err(ControlError::BadRequest(
                "history scope is read-only".into(),
            ));
        }
        let mut client = self.checkout_writer().await?;
        let result = self
            .transact_on(&mut client, scope, None, request_ids, f)
            .await;
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
        let result = self
            .transact_on(&mut client, scope, Some(runner), 0, f)
            .await;
        self.return_writer(client).await;
        result
    }

    /// The common poll as direct statements: a clean idle session (no
    /// undelivered message, no active request) with assignments off claims
    /// the first ready job it can run, or learns there is none. `None` sends
    /// every other case to the working-set path, which owns redelivery,
    /// cancellations, busy runners and assignment rules.
    async fn poll_claim_direct(
        &self,
        poll: &PollRequest,
    ) -> Result<Option<PollOutcome>, ControlError> {
        let assignments = self
            .pool_assignments_enabled
            .load(std::sync::atomic::Ordering::Relaxed)
            || self
                .require_job_assignments
                .load(std::sync::atomic::Ordering::Relaxed);
        if poll.busy || assignments {
            return Ok(None);
        }
        let started = std::time::Instant::now();
        let mut client = self.checkout_writer().await?;
        let result = async {
            let txn = client.transaction().await.map_err(ControlError::backend)?;
            // The candidate's shared run lock and job row lock come before
            // any session/request row lock: run-scoped writers use this order.
            let candidates = lock_poll_candidates(&txn, &poll.runner, 1).await?;
            let locked = started.elapsed();
            // A concurrent completion can own the session. Never wait on it
            // while holding a run/job lock; roll back and re-evaluate.
            let Some(session) = txn
                .query_opt(
                    "SELECT runner_id, active_request_id, \
                     EXISTS(SELECT 1 FROM broker_messages m WHERE m.session_id = s.session_id) \
                     FROM runner_sessions s WHERE session_id = $1 FOR UPDATE SKIP LOCKED",
                    &[&poll.session_id],
                )
                .await
                .map_err(ControlError::backend)?
            else {
                return Ok(None);
            };
            let session_runner: Option<i64> = session.get(0);
            let active: Option<i64> = session.get(1);
            let inflight: bool = session.get(2);
            let Some(session_runner) = session_runner else {
                return Ok(None);
            };
            if active.is_some() || inflight {
                return Ok(None);
            }
            if poll
                .verified_runner_id
                .is_some_and(|id| id != session_runner)
            {
                return Err(ControlError::Forbidden(
                    "session belongs to another runner".to_owned(),
                ));
            }
            let now_us = system_to_us(std::time::SystemTime::now());
            txn.execute(
                "UPDATE runner_sessions SET last_seen_at_us = $1 WHERE session_id = $2",
                &[&now_us, &poll.session_id],
            )
            .await
            .map_err(ControlError::backend)?;
            let Some((run_s, job_s)) = candidates.into_iter().next() else {
                txn.commit().await.map_err(ControlError::backend)?;
                return Ok(Some(PollOutcome::Empty));
            };
            // A stale binding on this job is an assignment-rule decision.
            let bound: bool = txn
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM job_assignments WHERE run_id=$1 AND job_id=$2) \
                     OR EXISTS(SELECT 1 FROM pool_pending WHERE run_id=$1 AND job_id=$2)",
                    &[&run_s, &job_s],
                )
                .await
                .map_err(ControlError::backend)?
                .get(0);
            if bound {
                return Ok(None);
            }
            let Some(request_row) = txn
                .query_opt(
                    "SELECT request_id, agent_job_id, plan_id, plan_type, timeline_id, \
                     timeout_triggered, debug_token_issued FROM job_requests \
                     WHERE run_id = $1 AND job_id = $2 AND result IS NULL \
                     ORDER BY request_id LIMIT 1 FOR UPDATE SKIP LOCKED",
                    &[&run_s, &job_s],
                )
                .await
                .map_err(ControlError::backend)?
            else {
                return Ok(None);
            };
            let Some(queued) = load_queued_job(&txn, &self.cipher, &run_s, &job_s).await? else {
                return Ok(None);
            };
            let request_id: i64 = request_row.get(0);
            let locked_until = crate::distributed_task::agent_request_locked_until();
            let now = us_to_system(now_us);
            txn.execute(
                "UPDATE job_requests SET owner_runner_id = $2, claimed_at_us = $3, \
                 started_at_us = $3, last_renewed_at_us = $3, locked_until = $4 \
                 WHERE request_id = $1",
                &[&request_id, &session_runner, &now_us, &locked_until],
            )
            .await
            .map_err(ControlError::backend)?;
            let seq = alloc_job_counter(&txn, "job_seq").await?;
            txn.execute(
                "UPDATE jobs SET queue_kind = 'claimed', status = 'in_progress', \
                 queue_position = NULL, seq = $3, claimed_by = NULL, claimed_at_us = NULL \
                 WHERE run_id = $1 AND job_id = $2",
                &[&run_s, &job_s, &seq],
            )
            .await
            .map_err(ControlError::backend)?;
            txn.execute(
                "UPDATE runs SET status = 'in_progress', \
                 started_at_us = COALESCE(started_at_us, $2) WHERE run_id = $1",
                &[&run_s, &now_us],
            )
            .await
            .map_err(ControlError::backend)?;
            txn.execute(
                "UPDATE runner_sessions SET active_request_id = $2 WHERE session_id = $1",
                &[&poll.session_id, &request_id],
            )
            .await
            .map_err(ControlError::backend)?;
            let depth: i64 = txn
                .query_one("SELECT COUNT(*) FROM jobs WHERE queue_kind = 'ready'", &[])
                .await
                .map_err(ControlError::backend)?
                .get(0);
            let next_runs_on: Vec<String> = txn
                .query_opt(
                    "SELECT runs_on FROM jobs WHERE queue_kind = 'ready' \
                     ORDER BY priority DESC, run_order, job_order, seq, run_id, job_id LIMIT 1",
                    &[],
                )
                .await
                .map_err(ControlError::backend)?
                .and_then(|row| serde_json::from_str(&row.get::<_, String>(0)).ok())
                .unwrap_or_default();
            let written = started.elapsed();
            txn.commit().await.map_err(ControlError::backend)?;
            crate::control::txn_stats::record(
                "poll_claim_direct",
                true,
                locked,
                std::time::Duration::ZERO,
                std::time::Duration::ZERO,
                written - locked,
                started.elapsed() - written,
            );
            let request = TaskAgentJobRequestRecord {
                request_id,
                run_id: queued.run_id,
                job_id: queued.job_id.clone(),
                agent_job_id: parse_uuid(&request_row.get::<_, String>(1)),
                plan_id: request_row.get(2),
                plan_type: request_row.get(3),
                timeline_id: parse_uuid(&request_row.get::<_, String>(4)),
                result: None,
                locked_until,
                claimed_at: Some(now),
                owner_runner_id: Some(session_runner),
                started_at: Some(now),
                last_renewed_at: Some(now),
                timeout_triggered: request_row.get::<_, i64>(5) != 0,
                debug_token_issued: request_row.get::<_, i64>(6) != 0,
            };
            Ok(Some(PollOutcome::Claimed(Box::new(ClaimedJob {
                queued,
                request,
                runner_id: session_runner,
                queue_depth: depth.max(0) as usize,
                next_runs_on,
            }))))
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn transact_on<T>(
        &self,
        client: &mut Client,
        scope: &TxScope,
        poll_runner: Option<&crate::models::RunnerCapabilities>,
        request_ids: usize,
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
            // Polls do not take the global advisory lock. They take shared
            // locks for candidate runs below; global writers take exclusive
            // run locks before loading those same rows. This keeps unrelated
            // global work from delaying every runner poll while preserving
            // mutual exclusion on the rows a poll may claim.
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
        // Every Postgres transaction allocates from a reserved pool — empty
        // unless the command declared how many attempts it may mint — so an
        // undeclared allocation fails closed instead of racing a peer.
        let reserved = if request_ids == 0 {
            std::collections::VecDeque::new()
        } else {
            txn.query(
                "SELECT nextval('request_id_seq') FROM generate_series(1, $1::bigint)",
                &[&(request_ids as i64)],
            )
            .await
            .map_err(ControlError::backend)?
            .iter()
            .map(|row| row.get::<_, i64>(0))
            .collect()
        };
        tx.reserved_request_ids = Some(reserved);
        let result = run_blocking(|| f(&mut tx))?;
        if tx.request_id_shortfall {
            return Err(ControlError::backend(anyhow::anyhow!(
                "command minted more job requests than it reserved ({request_ids})"
            )));
        }
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

    /// Shared read for `acquire_context` / `acquire_for_runner`: the request
    /// row joined to its sealed message, token-mint request, id-token grant
    /// and the run's submission fields — one statement on the reader pool.
    async fn acquire_impl(
        &self,
        request_id: i64,
        runner_id: Option<i64>,
    ) -> Result<AcquireContext, ControlError> {
        let client = self.checkout_reader().await?;
        let result = async {
            let row = client
                .query_opt(
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
                     WHERE jr.request_id = $1",
                    &[&request_id],
                )
                .await
                .map_err(ControlError::backend)?
                .ok_or_else(|| ControlError::NotFound(format!("request {request_id}")))?;
            let record = TaskAgentJobRequestRecord {
                request_id: row.get(0),
                run_id: parse_run_id(&row.get::<_, String>(1)),
                job_id: JobId(row.get(2)),
                agent_job_id: parse_uuid(&row.get::<_, String>(3)),
                plan_id: row.get(4),
                plan_type: row.get(5),
                timeline_id: parse_uuid(&row.get::<_, String>(6)),
                result: row.get::<_, Option<String>>(7).as_deref().map(status_parse),
                locked_until: row.get(8),
                claimed_at: row.get::<_, Option<i64>>(9).map(us_to_system),
                owner_runner_id: row.get(10),
                started_at: row.get::<_, Option<i64>>(11).map(us_to_system),
                last_renewed_at: row.get::<_, Option<i64>>(12).map(us_to_system),
                timeout_triggered: row.get::<_, i64>(13) != 0,
                debug_token_issued: row.get::<_, i64>(14) != 0,
            };
            if let Some(runner_id) = runner_id {
                let has_session: bool = row.get(19);
                let session_runner: Option<i64> = row.get(20);
                ensure_request_owner(
                    record.owner_runner_id,
                    session_runner,
                    has_session,
                    runner_id,
                )?;
                if record.result.is_some() {
                    return Err(ControlError::Conflict(
                        "broker request already completed".to_owned(),
                    ));
                }
            }
            let message_blob: Option<Vec<u8>> = row.get(15);
            let message_blob = message_blob
                .ok_or_else(|| ControlError::NotFound(format!("request {request_id} message")))?;
            let message = blob(&self.cipher, &message_blob)?;
            let token_blob: Option<Vec<u8>> = row.get(17);
            let token_request = token_blob.map(|b| blob(&self.cipher, &b)).transpose()?;
            let granted: Option<i64> = row.get(18);
            let submission_json: String = row.get(16);
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
        }
        .await;
        self.return_reader(client).await;
        result
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

/// Ready jobs a poll may claim, locked for this transaction. One read of the
/// queue head, label/group matching in Rust, then one `FOR UPDATE SKIP
/// LOCKED` statement over the matches in claim order: concurrent polls get
/// disjoint rows without waiting or walking past each other row by row. A
/// locked row is kept only if its run lock is free in shared mode
/// (`pg_try_advisory_xact_lock_shared`: polls of one run coexist, a
/// run-scoped writer excludes them). Nothing here waits, so taking the run
/// lock after the row lock cannot deadlock, and later statements only touch
/// rows of runs whose shared lock is held. Returns at most `want`
/// `(run_id, job_id)`.
async fn lock_poll_candidates(
    conn: &Tx<'_>,
    runner: &crate::models::RunnerCapabilities,
    want: usize,
) -> Result<Vec<(String, String)>, ControlError> {
    const POLL_SCAN: i64 = 256;
    /// Label-matching candidates offered to one locking statement.
    const CANDIDATES: usize = 64;
    /// Locking statements per poll before giving up (each skips runs whose
    /// exclusive lock a run-scoped writer holds).
    const ATTEMPTS: usize = 3;
    let head = conn
        .query(
            "SELECT run_id, job_id, runs_on, runner_group FROM jobs WHERE queue_kind='ready' \
             ORDER BY priority DESC, run_order, job_order, seq, run_id, job_id LIMIT $1",
            &[&POLL_SCAN],
        )
        .await
        .map_err(ControlError::backend)?;
    // Label and group matching stay in Rust (hosted-label OS mapping); the
    // matches keep queue order.
    let (mut runs, mut jobs): (Vec<String>, Vec<String>) = head
        .iter()
        .filter(|row| {
            let runs_on: Vec<String> =
                serde_json::from_str(&row.get::<_, String>(2)).unwrap_or_default();
            let group: Option<String> = row.get(3);
            super::sched::job_matches_runner(&runs_on, &runner.labels)
                && super::sched::job_matches_runner_group(group.as_deref(), runner)
        })
        .take(CANDIDATES)
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .unzip();
    let mut locked = Vec::new();
    for _ in 0..ATTEMPTS {
        if runs.is_empty() || locked.len() == want {
            break;
        }
        // One statement locks the first unlocked candidates in queue order:
        // rows a concurrent poll holds are skipped, never waited on.
        let rows = conn
            .query(
                "SELECT j.run_id, j.job_id \
                 FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS c(run_id, job_id, ord) \
                 JOIN jobs j ON j.run_id = c.run_id AND j.job_id = c.job_id \
                 WHERE j.queue_kind = 'ready' \
                 ORDER BY c.ord LIMIT $3 FOR UPDATE OF j SKIP LOCKED",
                &[&runs, &jobs, &((want - locked.len()) as i64)],
            )
            .await
            .map_err(ControlError::backend)?;
        if rows.is_empty() {
            break;
        }
        let mut busy_runs = BTreeSet::new();
        for row in rows {
            let run_id: String = row.get(0);
            let job_id: String = row.get(1);
            // Shared run lock: polls of one run coexist, a run-scoped writer
            // (exclusive) excludes them. `try` never waits, so taking it
            // after the row lock cannot deadlock.
            let got: bool = conn
                .query_one(
                    "SELECT pg_try_advisory_xact_lock_shared($1)",
                    &[&run_lock_key(&parse_run_id(&run_id))],
                )
                .await
                .map_err(ControlError::backend)?
                .get(0);
            if got {
                locked.push((run_id, job_id));
            } else {
                busy_runs.insert(run_id);
            }
        }
        // Next attempt: drop every candidate already taken or in a busy run.
        let keep: Vec<bool> = runs
            .iter()
            .zip(&jobs)
            .map(|(run, job)| {
                !busy_runs.contains(run) && !locked.iter().any(|(r, j)| r == run && j == job)
            })
            .collect();
        let mut flags = keep.iter();
        runs.retain(|_| *flags.next().unwrap());
        let mut flags = keep.iter();
        jobs.retain(|_| *flags.next().unwrap());
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

/// Decode one job row (payload columns, sealed message, `needs:` edges) into
/// the dispatchable job.
async fn load_queued_job(
    conn: &Tx<'_>,
    cipher: &store::Envelope,
    run_s: &str,
    job_s: &str,
) -> Result<Option<QueuedJob>, ControlError> {
    let Some(row) = conn
        .query_opt(
            "SELECT j.base_id, j.runs_on, j.runner_group, j.enqueued_at_us, \
             j.created_at_ns, j.deps_ready_at_ns, j.concurrency_wait_at_ns, \
             j.concurrency_acquired_at_ns, j.if_condition, j.max_parallel, \
             j.environment_json, j.concurrency_json, j.matrix_json, j.deferred_matrix, \
             j.reusable_call_json, m.message_blob, m.condition_context_blob \
             FROM jobs j JOIN job_messages m ON m.run_id = j.run_id AND m.job_id = j.job_id \
             WHERE j.run_id = $1 AND j.job_id = $2",
            &[&run_s, &job_s],
        )
        .await
        .map_err(ControlError::backend)?
    else {
        return Ok(None);
    };
    let needs: Vec<String> = conn
        .query(
            "SELECT needs_job_id FROM job_needs WHERE run_id = $1 AND job_id = $2 \
             ORDER BY position",
            &[&run_s, &job_s],
        )
        .await
        .map_err(ControlError::backend)?
        .iter()
        .map(|row| row.get(0))
        .collect();
    let payload = super::rows::JobPayloadRow {
        created_at_ns: row.get::<_, Option<i64>>(4).unwrap_or(0),
        deps_ready_at_ns: row.get(5),
        concurrency_wait_at_ns: row.get(6),
        concurrency_acquired_at_ns: row.get(7),
        if_condition: row.get(8),
        max_parallel: row.get(9),
        environment_json: row.get(10),
        concurrency_json: row.get(11),
        matrix_json: row.get(12),
        deferred_matrix: row.get(13),
        reusable_call_json: row.get(14),
        needs,
    };
    let runs_on: String = row.get(1);
    let message: Vec<u8> = row.get(15);
    let context: Vec<u8> = row.get(16);
    payload
        .into_job(
            parse_run_id(run_s),
            JobId(job_s.to_owned()),
            row.get(0),
            &runs_on,
            row.get(2),
            row.get(3),
            blob(cipher, &message)?,
            blob(cipher, &context)?,
        )
        .map(Some)
        .map_err(ControlError::backend)
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
                .query("SELECT run_id FROM runs WHERE archived_at_us IS NULL", &[])
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
        // The live working set is the unarchived runs; an archived run's
        // rows live in the history tables and only a history scope reads it.
        None if scope.include_archived => load_runs(conn, cipher, "", &[]).await?,
        None => load_runs(conn, cipher, " WHERE archived_at_us IS NULL", &[]).await?,
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
            _ => {}
        }
        tx.loaded.counters.insert(name, value);
    }
    // Request ids come from `request_id_seq` (see `transact_reserving`).
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
        // A loaded request's job message never changes inside a command:
        // messages are only minted with a new request id. Existing rows get
        // their mutable columns; only new rows seal and write the message.
        if tx.loaded.requests.contains(request_id) {
            conn.execute(
                "UPDATE job_requests SET result=$2, locked_until=$3, claimed_at_us=$4, \
                 owner_runner_id=$5, started_at_us=$6, last_renewed_at_us=$7, \
                 timeout_triggered=$8, debug_token_issued=$9 WHERE request_id=$1",
                &[
                    request_id,
                    &r.result.map(status_str),
                    &r.locked_until,
                    &r.claimed_at.map(system_to_us),
                    &r.owner_runner_id,
                    &r.started_at.map(system_to_us),
                    &r.last_renewed_at.map(system_to_us),
                    &(r.timeout_triggered as i64),
                    &(r.debug_token_issued as i64),
                ],
            )
            .await
            .map_err(ControlError::backend)?;
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
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) \
             ON CONFLICT(request_id) DO UPDATE SET result=excluded.result, \
             locked_until=excluded.locked_until, claimed_at_us=excluded.claimed_at_us, \
             owner_runner_id=excluded.owner_runner_id, started_at_us=excluded.started_at_us, \
             last_renewed_at_us=excluded.last_renewed_at_us, \
             timeout_triggered=excluded.timeout_triggered, \
             debug_token_issued=excluded.debug_token_issued, \
             request_blob=excluded.request_blob, job_timeout_s=excluded.job_timeout_s",
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
                &tx.broker_messages
                    .get(request_id)
                    .and_then(|message| message.job_timeout),
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
        let request_ids = submit
            .jobs
            .iter()
            .filter(|job| job.request.is_some())
            .count();
        self.transact_reserving(&scope, request_ids, |tx| {
            commands::submit_run_tx(tx, submit)
        })
        .await
    }

    async fn allocate_run_number(
        &self,
        namespace_id: &str,
        repository: &str,
        workflow_path: &str,
    ) -> Result<u64, ControlError> {
        // One atomic upsert: the stored value is the last number handed out.
        // The legacy `workflow_run_counters` table has a single `key`; fold
        // `(repository, workflow_path)` in so the counter is unique per
        // repo+workflow as the agreed schema requires. `namespace_id` is not
        // a legacy column — single-tenant for now.
        let _ = namespace_id;
        let key = format!("{repository}\x1f{workflow_path}");
        let client = self.checkout_writer().await?;
        let result = client
            .query_one(
                "INSERT INTO workflow_run_counters (key, value) VALUES ($1, 1) \
                 ON CONFLICT(key) DO UPDATE SET value = workflow_run_counters.value + 1 \
                 RETURNING value",
                &[&key],
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
        if let Some(outcome) = self.poll_claim_direct(&poll).await? {
            return Ok(outcome);
        }
        let runner = poll.runner.clone();
        self.transact_poll(&scope, &runner, |tx| commands::poll_session_tx(tx, poll))
            .await
    }

    async fn acquire_context(&self, request_id: i64) -> Result<AcquireContext, ControlError> {
        // Read-only point lookup on the reader pool — one joined statement
        // instead of loading the run's working set.
        self.acquire_impl(request_id, None).await
    }

    async fn acquire_for_runner(
        &self,
        request_id: i64,
        runner_id: i64,
    ) -> Result<AcquireContext, ControlError> {
        self.acquire_impl(request_id, Some(runner_id)).await
    }

    async fn store_request_message(
        &self,
        run_id: RunId,
        request_id: i64,
        message: Option<&preloop_gha_protocol::azdo::AgentJobRequestMessage>,
        token_request: Option<&crate::models::GitHubTokenRequest>,
    ) -> Result<(), ControlError> {
        let message_blob = message.map(|m| unblob(&self.cipher, m)).transpose()?;
        let job_timeout = message.and_then(|m| m.job_timeout);
        let token_blob = token_request.map(|t| unblob(&self.cipher, t)).transpose()?;
        let mut client = self.checkout_writer().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            // Serializes against any scoped write-back on the request's run.
            lock_runs(&tx, std::iter::once(&run_id)).await?;
            if let Some(message_blob) = message_blob {
                tx.execute(
                    "UPDATE job_requests SET request_blob = $1, \
                     job_timeout_s = COALESCE($2, job_timeout_s) \
                     WHERE request_id = $3",
                    &[&message_blob, &job_timeout, &request_id],
                )
                .await
                .map_err(ControlError::backend)?;
            }
            if let Some(blob) = token_blob {
                tx.execute(
                    "INSERT INTO github_token_requests (request_id, request_blob) \
                     VALUES ($1,$2) ON CONFLICT(request_id) \
                     DO UPDATE SET request_blob = excluded.request_blob",
                    &[&request_id, &blob],
                )
                .await
                .map_err(ControlError::backend)?;
            }
            tx.commit().await.map_err(ControlError::backend)
        }
        .await;
        self.return_writer(client).await;
        result
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
            commands::cancel_run_tx(tx, run_id, reason.as_deref())
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
            Ok(commands::cancel_outcome(tx, run_id, cancellations))
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
                concurrency: self.run_in_concurrency(run_id).await?.any(),
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
            concurrency: self.run_in_concurrency(claim.job.run_id).await?.any(),
            runs_referenced: false,
            job_requests_all: false,
            pending_expansions: false,
            runs_via_requests: false,
        };
        let request_ids = claim
            .built
            .as_ref()
            .map_or(0, sched::BuiltExpansion::job_count);
        self.transact_reserving(&scope, request_ids, |tx| {
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
        let run = run_id.0.to_string();
        let client = self.checkout_reader().await?;
        let result = async {
            let mut loaded = load_runs(&client, &self.cipher, " WHERE run_id = $1", &[&run])
                .await?
                .into_iter();
            let Some((_, _, mut record, _)) = loaded.next() else {
                return Err(ControlError::NotFound(format!("run {run_id}")));
            };
            // Live jobs plus their archived copies — `into_record` leaves the
            // status map empty by contract.
            let rows = client
                .query(
                    "SELECT job_id, status FROM jobs WHERE run_id=$1 \
                     UNION ALL SELECT job_id, status FROM job_history WHERE run_id=$1",
                    &[&run],
                )
                .await
                .map_err(ControlError::backend)?;
            for row in &rows {
                record.jobs.insert(
                    JobId(row.get::<_, String>(0)),
                    status_parse(&row.get::<_, String>(1)),
                );
            }
            Ok(record)
        }
        .await;
        self.return_reader(client).await;
        result
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
            let mut archived = 0;
            for row in &rows {
                let run_id: String = row.get(0);
                // Archive only a run no writer holds: a late run-scoped
                // command (check-run report, timeline flush) would otherwise
                // race these deletes. Never waits; a busy run is archived on
                // a later pass.
                let free: bool = tx
                    .query_one(
                        "SELECT pg_try_advisory_xact_lock($1)",
                        &[&run_lock_key(&parse_run_id(&run_id))],
                    )
                    .await
                    .map_err(ControlError::backend)?
                    .get(0);
                if !free {
                    continue;
                }
                archived += 1;
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
            // Runs archived, not candidates: a batch with busy runs skipped
            // ends the caller's drain loop until the next tick instead of
            // re-fetching the same held runs.
            Ok(archived)
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn set_push_state(
        &self,
        run_id: RunId,
        state: crate::models::PushState,
    ) -> Result<(), ControlError> {
        let state_json = serde_json::to_string(&state).map_err(ControlError::backend)?;
        let mut client = self.checkout_writer().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            lock_runs(&tx, std::iter::once(&run_id)).await?;
            tx.execute(
                "UPDATE runs SET push_state_json = $2 WHERE run_id = $1",
                &[&run_id.to_string(), &state_json],
            )
            .await
            .map_err(ControlError::backend)?;
            tx.commit().await.map_err(ControlError::backend)
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn request(&self, key: RequestKey) -> Result<TaskAgentJobRequestRecord, ControlError> {
        const COLUMNS: &str = "SELECT request_id, run_id, job_id, agent_job_id, plan_id,
            plan_type, timeline_id, result, locked_until, claimed_at_us, owner_runner_id,
            started_at_us, last_renewed_at_us, timeout_triggered, debug_token_issued
            FROM job_requests";
        let client = self.checkout_reader().await?;
        let result = async {
            let row = match &key {
                RequestKey::Id(id) => {
                    client
                        .query_opt(&format!("{COLUMNS} WHERE request_id = $1"), &[id])
                        .await
                }
                RequestKey::PlanId(plan) => {
                    client
                        .query_opt(
                            &format!(
                                "{COLUMNS} WHERE plan_id = $1 ORDER BY request_id DESC LIMIT 1"
                            ),
                            &[plan],
                        )
                        .await
                }
                RequestKey::AgentJobId(id) => {
                    client
                        .query_opt(
                            &format!("{COLUMNS} WHERE agent_job_id = $1"),
                            &[&id.to_string()],
                        )
                        .await
                }
                RequestKey::TimelineId(id) => {
                    client
                        .query_opt(
                            &format!(
                                "{COLUMNS} WHERE timeline_id = $1 ORDER BY request_id DESC LIMIT 1"
                            ),
                            &[&id.to_string()],
                        )
                        .await
                }
                RequestKey::Job(run_id, job_id) => {
                    client
                        .query_opt(
                            &format!(
                                "{COLUMNS} WHERE run_id = $1 AND job_id = $2 \
                                 ORDER BY request_id DESC LIMIT 1"
                            ),
                            &[&run_id.0.to_string(), &job_id.0],
                        )
                        .await
                }
            }
            .map_err(ControlError::backend)?
            .ok_or_else(|| ControlError::NotFound("request".to_owned()))?;
            Ok(TaskAgentJobRequestRecord {
                request_id: row.get(0),
                run_id: parse_run_id(&row.get::<_, String>(1)),
                job_id: JobId(row.get(2)),
                agent_job_id: parse_uuid(&row.get::<_, String>(3)),
                plan_id: row.get(4),
                plan_type: row.get(5),
                timeline_id: parse_uuid(&row.get::<_, String>(6)),
                result: row.get::<_, Option<String>>(7).as_deref().map(status_parse),
                locked_until: row.get(8),
                claimed_at: row.get::<_, Option<i64>>(9).map(us_to_system),
                owner_runner_id: row.get(10),
                started_at: row.get::<_, Option<i64>>(11).map(us_to_system),
                last_renewed_at: row.get::<_, Option<i64>>(12).map(us_to_system),
                timeout_triggered: row.get::<_, i64>(13) != 0,
                debug_token_issued: row.get::<_, i64>(14) != 0,
            })
        }
        .await;
        self.return_reader(client).await;
        result
    }

    async fn run_dispatch_info(
        &self,
        run_id: RunId,
    ) -> Result<Option<RunDispatchInfo>, ControlError> {
        let run = run_id.0.to_string();
        let client = self.checkout_reader().await?;
        let result = async {
            let Some(head) = client
                .query_opt(
                    "SELECT s.submission_json, r.started_at_us, r.completed_at_us \
                     FROM run_submissions s JOIN runs r ON r.run_id=s.run_id \
                     WHERE s.run_id=$1",
                    &[&run],
                )
                .await
                .map_err(ControlError::backend)?
            else {
                return Ok(None);
            };
            let value: serde_json::Value =
                serde_json::from_str(&head.get::<_, String>(0)).map_err(ControlError::backend)?;
            let job_rows = client
                .query(
                    "SELECT j.job_id, j.status, r.display_name, r.check_run_id, \
                            r.detail_json, \
                            (SELECT q.agent_job_id FROM job_requests q \
                              WHERE q.run_id=j.run_id AND q.job_id=j.job_id \
                              ORDER BY q.request_id DESC LIMIT 1) AS agent \
                     FROM jobs j LEFT JOIN run_jobs r \
                       ON r.run_id=j.run_id AND r.job_id=j.job_id \
                     WHERE j.run_id=$1 ORDER BY j.job_id",
                    &[&run],
                )
                .await
                .map_err(ControlError::backend)?;
            let mut jobs = Vec::with_capacity(job_rows.len());
            for row in &job_rows {
                let agent: Option<String> = row.get(5);
                let mut steps = Vec::new();
                if let Some(agent) = &agent {
                    let step_rows = client
                        .query(
                            &format!(
                                "SELECT {STEP_COLUMNS} FROM job_steps \
                                 WHERE agent_job_id=$1 ORDER BY position"
                            ),
                            &[agent],
                        )
                        .await
                        .map_err(ControlError::backend)?;
                    for step_row in &step_rows {
                        steps.push(step_from_row(step_row).1);
                    }
                }
                jobs.push(RunDispatchJob {
                    job_id: JobId(row.get::<_, String>(0)),
                    status: status_parse(&row.get::<_, String>(1)),
                    display_name: row.get::<_, Option<String>>(2),
                    check_run_id: row.get::<_, Option<i64>>(3).map(|id| id as u64),
                    detail: row
                        .get::<_, Option<String>>(4)
                        .and_then(|d| serde_json::from_str(&d).ok()),
                    steps,
                });
            }
            Ok(Some(RunDispatchInfo {
                repository: value["repository"].as_str().unwrap_or_default().to_owned(),
                sha: value["sha"].as_str().unwrap_or_default().to_owned(),
                started_at: head.get::<_, Option<i64>>(1).map(us_to_system),
                completed_at: head.get::<_, Option<i64>>(2).map(us_to_system),
                jobs,
            }))
        }
        .await;
        self.return_reader(client).await;
        result
    }

    async fn submission_fields(
        &self,
        run_id: RunId,
    ) -> Result<Option<SubmissionFields>, ControlError> {
        let run = run_id.0.to_string();
        let client = self.checkout_reader().await?;
        let result = async {
            let Some(json) = client
                .query_opt(
                    "SELECT submission_json FROM run_submissions WHERE run_id=$1",
                    &[&run],
                )
                .await
                .map_err(ControlError::backend)?
                .map(|row| row.get::<_, String>(0))
            else {
                return Ok(None);
            };
            let value: serde_json::Value =
                serde_json::from_str(&json).map_err(ControlError::backend)?;
            Ok(Some(SubmissionFields {
                repository: value["repository"].as_str().unwrap_or_default().to_owned(),
                sha: value["sha"].as_str().unwrap_or_default().to_owned(),
                base_ref: value["base_ref"].as_str().map(str::to_owned),
                git_ref: value["git_ref"].as_str().unwrap_or_default().to_owned(),
            }))
        }
        .await;
        self.return_reader(client).await;
        result
    }

    async fn job_check_run_id(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<u64>, ControlError> {
        let run = run_id.0.to_string();
        let job = job_id.0.clone();
        let client = self.checkout_reader().await?;
        let result = client
            .query_opt(
                "SELECT check_run_id FROM run_jobs WHERE run_id=$1 AND job_id=$2",
                &[&run, &job],
            )
            .await
            .map(|row| {
                row.and_then(|row| row.get::<_, Option<i64>>(0))
                    .map(|id| id as u64)
            })
            .map_err(ControlError::backend);
        self.return_reader(client).await;
        result
    }

    async fn run_job_statuses(
        &self,
        run_id: RunId,
    ) -> Result<Option<Vec<(JobId, ExecutionStatus)>>, ControlError> {
        let run = run_id.0.to_string();
        let client = self.checkout_reader().await?;
        let result = async {
            let exists = client
                .query_one("SELECT EXISTS(SELECT 1 FROM runs WHERE run_id=$1)", &[&run])
                .await
                .map_err(ControlError::backend)?
                .get::<_, bool>(0);
            if !exists {
                return Ok(None);
            }
            let rows = client
                .query(
                    "SELECT job_id, status FROM jobs WHERE run_id=$1 ORDER BY job_id",
                    &[&run],
                )
                .await
                .map_err(ControlError::backend)?;
            Ok(Some(
                rows.iter()
                    .map(|row| {
                        (
                            JobId(row.get::<_, String>(0)),
                            status_parse(&row.get::<_, String>(1)),
                        )
                    })
                    .collect(),
            ))
        }
        .await;
        self.return_reader(client).await;
        result
    }

    async fn live_log_key(
        &self,
        run_id: RunId,
        job_id: &str,
    ) -> Result<Option<(String, bool)>, ControlError> {
        let run = run_id.0.to_string();
        let job = job_id.to_owned();
        let client = self.checkout_reader().await?;
        let result = async {
            let Some(status) = client
                .query_opt("SELECT status FROM runs WHERE run_id=$1", &[&run])
                .await
                .map_err(ControlError::backend)?
                .map(|row| row.get::<_, String>(0))
            else {
                return Ok(None);
            };
            let run_terminal = status_parse(&status).is_terminal();
            // A logical job key names the current attempt; an agent job id
            // names exactly one record. `ORDER BY request_id DESC` picks the
            // latest attempt for the logical key, never a dead feed.
            let record = client
                .query_opt(
                    "SELECT agent_job_id FROM job_requests \
                     WHERE run_id=$1 AND (job_id=$2 OR agent_job_id=$2) \
                     ORDER BY request_id DESC LIMIT 1",
                    &[&run, &job],
                )
                .await
                .map_err(ControlError::backend)?
                .map(|row| row.get::<_, String>(0));
            let key = match record {
                Some(agent) => agent,
                None => {
                    // A bare logical key is valid only when the run owns it —
                    // never accept an arbitrary key that could leak another
                    // run's output.
                    let owned = client
                        .query_one(
                            "SELECT EXISTS(SELECT 1 FROM jobs WHERE run_id=$1 AND job_id=$2)",
                            &[&run, &job],
                        )
                        .await
                        .map_err(ControlError::backend)?
                        .get::<_, bool>(0);
                    if !owned {
                        return Ok(None);
                    }
                    job.clone()
                }
            };
            // `job` is the logical id or a (missed) agent job id — the same
            // lookup the old code did against the in-memory map.
            let job_terminal = client
                .query_opt(
                    "SELECT status FROM jobs WHERE run_id=$1 AND job_id=$2",
                    &[&run, &job],
                )
                .await
                .map_err(ControlError::backend)?
                .map(|row| status_parse(&row.get::<_, String>(0)).is_terminal())
                .unwrap_or(false);
            Ok(Some((key, run_terminal || job_terminal)))
        }
        .await;
        self.return_reader(client).await;
        result
    }

    async fn run_requests(
        &self,
        run_id: RunId,
    ) -> Result<Vec<TaskAgentJobRequestRecord>, ControlError> {
        let run = run_id.0.to_string();
        let client = self.checkout_reader().await?;
        let result = async {
            let rows = client
                .query(
                    "SELECT request_id, run_id, job_id, agent_job_id, plan_id,
                            plan_type, timeline_id, result, locked_until, claimed_at_us,
                            owner_runner_id, started_at_us, last_renewed_at_us,
                            timeout_triggered, debug_token_issued
                     FROM job_requests WHERE run_id=$1
                     UNION ALL
                     SELECT h.request_id, h.run_id, h.job_id, h.agent_job_id,
                            h.plan_id, '', h.timeline_id, h.result, '', h.claimed_at_us,
                            h.owner_runner_id, h.started_at_us, NULL::bigint, 0, 0
                     FROM attempt_history h JOIN runs r ON r.run_id=h.run_id
                     WHERE r.archived_at_us IS NOT NULL AND h.run_attempt=r.run_attempt
                       AND h.run_created_at_us=r.created_at_us AND h.run_id=$1
                     ORDER BY 1",
                    &[&run],
                )
                .await
                .map_err(ControlError::backend)?;
            Ok(rows
                .iter()
                .map(|row| TaskAgentJobRequestRecord {
                    request_id: row.get(0),
                    run_id: parse_run_id(&row.get::<_, String>(1)),
                    job_id: JobId(row.get(2)),
                    agent_job_id: parse_uuid(&row.get::<_, String>(3)),
                    plan_id: row.get(4),
                    plan_type: row.get(5),
                    timeline_id: parse_uuid(&row.get::<_, String>(6)),
                    result: row.get::<_, Option<String>>(7).as_deref().map(status_parse),
                    locked_until: row.get(8),
                    claimed_at: row.get::<_, Option<i64>>(9).map(us_to_system),
                    owner_runner_id: row.get(10),
                    started_at: row.get::<_, Option<i64>>(11).map(us_to_system),
                    last_renewed_at: row.get::<_, Option<i64>>(12).map(us_to_system),
                    timeout_triggered: row.get::<_, i64>(13) != 0,
                    debug_token_issued: row.get::<_, i64>(14) != 0,
                })
                .collect())
        }
        .await;
        self.return_reader(client).await;
        result
    }

    async fn run_step_manifests(
        &self,
        run_id: RunId,
    ) -> Result<BTreeMap<uuid::Uuid, Vec<crate::models::StepRecord>>, ControlError> {
        let run = run_id.0.to_string();
        let client = self.checkout_reader().await?;
        let result = async {
            // Latest manifests live in `job_steps`; an archived run's are in
            // `step_history` (matched to the live row's attempt + created_at).
            let rows = client
                .query(
                    "SELECT s.agent_job_id, s.step_id, s.kind, s.workflow_index,                      s.runner_number, s.context_name, s.name, s.conclusion,                      s.started_at_us, s.finished_at_us, s.position FROM job_steps s                      JOIN job_requests r ON r.agent_job_id=s.agent_job_id                      WHERE r.run_id=$1                      UNION ALL                      SELECT h.agent_job_id, h.step_id, h.kind, h.workflow_index,                      h.runner_number, h.context_name, h.name, h.conclusion,                      h.started_at_us, h.finished_at_us, h.position FROM step_history h                      JOIN runs r2 ON r2.run_id=h.run_id AND r2.run_attempt=h.run_attempt                        AND r2.created_at_us=h.run_created_at_us                      WHERE h.run_id=$1 AND r2.archived_at_us IS NOT NULL                      ORDER BY 1, 11",
                    &[&run],
                )
                .await
                .map_err(ControlError::backend)?;
            let mut manifests: BTreeMap<uuid::Uuid, Vec<crate::models::StepRecord>> =
                BTreeMap::new();
            for row in &rows {
                let (agent, record) = step_from_row(row);
                manifests.entry(agent).or_default().push(record);
            }
            Ok(manifests)
        }
        .await;
        self.return_reader(client).await;
        result
    }

    async fn issue_debug_token(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<(RunId, String), ControlError> {
        let agent = agent_job_id.to_string();
        let mut client = self.checkout_writer().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            let Some(row) = tx
                .query_opt(
                    "SELECT request_id, run_id, plan_id, debug_token_issued \
                     FROM job_requests WHERE agent_job_id=$1 AND result IS NULL \
                     ORDER BY request_id DESC LIMIT 1",
                    &[&agent],
                )
                .await
                .map_err(ControlError::backend)?
            else {
                return Err(ControlError::NotFound(format!(
                    "no active job request for agent job {agent}"
                )));
            };
            let request_id: i64 = row.get(0);
            let run_id: String = row.get(1);
            let plan_id: String = row.get(2);
            // The runner only builds a pause client under
            // `preloopPreserveOnFailure`, so gating on the same flag issues
            // the credential exactly when it is used, and never otherwise.
            let submission: Option<String> = tx
                .query_opt(
                    "SELECT submission_json FROM run_submissions WHERE run_id=$1",
                    &[&run_id],
                )
                .await
                .map_err(ControlError::backend)?
                .map(|row| row.get(0));
            let preserve = submission
                .and_then(|json| serde_json::from_str::<serde_json::Value>(&json).ok())
                .and_then(|value| value["preserve_on_failure"].as_bool())
                .unwrap_or(false);
            if !preserve {
                return Err(ControlError::Forbidden(
                    "this run did not enable pause-on-failure".to_owned(),
                ));
            }
            if row.get::<_, i64>(3) != 0 {
                // Distinct from a 403 so a worker can tell "someone beat me
                // to it" from "not allowed at all" in its log.
                return Err(ControlError::Conflict(format!(
                    "debug-worker token already issued for agent job {agent}"
                )));
            }
            tx.execute(
                "UPDATE job_requests SET debug_token_issued=1 WHERE request_id=$1",
                &[&request_id],
            )
            .await
            .map_err(ControlError::backend)?;
            tx.commit().await.map_err(ControlError::backend)?;
            Ok((parse_run_id(&run_id), plan_id))
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn sweep_stale_bindings(&self) -> Result<usize, ControlError> {
        let pool_on = self
            .pool_assignments_enabled
            .load(std::sync::atomic::Ordering::Relaxed);
        let require_on = self
            .require_job_assignments
            .load(std::sync::atomic::Ordering::Relaxed);
        let now_us = system_to_us(std::time::SystemTime::now());
        // Freshness windows mirror `assignment_fresh`/`binding_fresh`.
        let assignment_cutoff = now_us - sched::ASSIGNMENT_TTL.as_micros() as i64;
        let binding_cutoff = now_us - sched::CLAIM_BINDING_TTL.as_micros() as i64;
        let mut client = self.checkout_writer().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            let mut swept = 0u64;
            if pool_on {
                swept += tx
                    .execute(
                        "DELETE FROM job_assignments WHERE (run_id, job_id) NOT IN \
                         (SELECT run_id, job_id FROM jobs WHERE queue_kind='ready')",
                        &[],
                    )
                    .await
                    .map_err(ControlError::backend)?;
                swept += tx
                    .execute(
                        "DELETE FROM pool_pending WHERE (run_id, job_id) NOT IN \
                         (SELECT run_id, job_id FROM jobs WHERE queue_kind='ready')",
                        &[],
                    )
                    .await
                    .map_err(ControlError::backend)?;
            }
            if !require_on && !pool_on {
                swept += tx
                    .execute(
                        "DELETE FROM job_assignments WHERE at_us < $1",
                        &[&assignment_cutoff],
                    )
                    .await
                    .map_err(ControlError::backend)?;
                swept += tx
                    .execute(
                        "DELETE FROM pool_pending WHERE at_us < $1",
                        &[&assignment_cutoff],
                    )
                    .await
                    .map_err(ControlError::backend)?;
            } else {
                // Release (not delete) stale or dead-runner bindings: the job
                // goes back to the pool waitlist.
                let stale = tx
                    .query(
                        "SELECT a.run_id, a.job_id FROM job_assignments a \
                         WHERE a.runner_id IS NOT NULL \
                         AND (a.at_us < $1 OR NOT EXISTS ( \
                             SELECT 1 FROM runners r WHERE r.runner_id=a.runner_id))",
                        &[&binding_cutoff],
                    )
                    .await
                    .map_err(ControlError::backend)?;
                for row in stale {
                    let run_id: String = row.get(0);
                    let job_id: String = row.get(1);
                    tx.execute(
                        "UPDATE job_assignments SET runner_id=NULL \
                         WHERE run_id=$1 AND job_id=$2",
                        &[&run_id, &job_id],
                    )
                    .await
                    .map_err(ControlError::backend)?;
                    tx.execute(
                        "INSERT INTO pool_pending (run_id, job_id, at_us) \
                         VALUES ($1,$2,$3) \
                         ON CONFLICT(run_id, job_id) DO UPDATE SET at_us=excluded.at_us",
                        &[&run_id, &job_id, &now_us],
                    )
                    .await
                    .map_err(ControlError::backend)?;
                    swept += 1;
                }
            }
            tx.commit().await.map_err(ControlError::backend)?;
            Ok(swept as usize)
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn check_run_target(
        &self,
        check_run_id: u64,
        repository: &str,
        head_sha: Option<&str>,
        job_name: Option<&str>,
        details_run_id: Option<RunId>,
    ) -> Result<Option<(RunId, JobId)>, ControlError> {
        let check_run_id = check_run_id as i64;
        let repository = repository.to_owned();
        let head_sha = head_sha.map(str::to_owned);
        let job_name = job_name.map(str::to_owned);
        let details = details_run_id.map(|id| id.0.to_string());
        let client = self.checkout_reader().await?;
        let result = async {
            // A rerequest targets a terminal run of this repository at this
            // head. `details_run_id` (from the check run's details_url) wins
            // when it matches; otherwise runs are considered in id order —
            // the same order the old full-map scan produced.
            let hit = client
                .query_opt(
                    "SELECT j.run_id, j.job_id FROM run_jobs j \
                     JOIN runs r ON r.run_id=j.run_id \
                     JOIN run_submissions s ON s.run_id=j.run_id \
                     WHERE j.check_run_id=$1 \
                       AND s.submission_json::jsonb->>'repository'=$2 \
                       AND ($3::text IS NULL OR r.head_sha=$3) \
                       AND r.status IN ('success','failure','skipped','cancelled') \
                     ORDER BY (j.run_id=$4) DESC, j.run_id LIMIT 1",
                    &[&check_run_id, &repository, &head_sha, &details],
                )
                .await
                .map_err(ControlError::backend)?;
            if let Some(row) = hit {
                return Ok(Some((
                    parse_run_id(&row.get::<_, String>(0)),
                    JobId(row.get::<_, String>(1)),
                )));
            }
            let Some(name) = job_name else {
                return Ok(None);
            };
            // Fallback parity: the check-run `name` against the logical job
            // key and the display name (the check run is created under the
            // display name).
            let hit = client
                .query_opt(
                    "SELECT j.run_id, j.job_id FROM run_jobs j \
                     JOIN runs r ON r.run_id=j.run_id \
                     JOIN run_submissions s ON s.run_id=j.run_id \
                     WHERE (j.job_id=$5 OR j.display_name=$5) \
                       AND s.submission_json::jsonb->>'repository'=$2 \
                       AND ($3::text IS NULL OR r.head_sha=$3) \
                       AND r.status IN ('success','failure','skipped','cancelled') \
                     ORDER BY (j.run_id=$4) DESC, j.run_id LIMIT 1",
                    &[&check_run_id, &repository, &head_sha, &details, &name],
                )
                .await
                .map_err(ControlError::backend)?;
            Ok(hit.map(|row| {
                (
                    parse_run_id(&row.get::<_, String>(0)),
                    JobId(row.get::<_, String>(1)),
                )
            }))
        }
        .await;
        self.return_reader(client).await;
        result
    }

    async fn submission_json_for_attempt(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<String>, ControlError> {
        let agent = agent_job_id.to_string();
        let client = self.checkout_reader().await?;
        let result = client
            .query_opt(
                "SELECT s.submission_json FROM job_requests q \
                 JOIN run_submissions s ON s.run_id=q.run_id \
                 WHERE q.agent_job_id=$1 ORDER BY q.request_id DESC LIMIT 1",
                &[&agent],
            )
            .await
            .map(|row| row.map(|row| row.get::<_, String>(0)))
            .map_err(ControlError::backend);
        self.return_reader(client).await;
        result
    }

    async fn published_run(
        &self,
        repository: &str,
        sha: &str,
        workflow_path: &str,
    ) -> Result<Option<RunId>, ControlError> {
        let repository = repository.to_owned();
        let sha = sha.to_owned();
        let workflow_path = workflow_path.to_owned();
        let client = self.checkout_reader().await?;
        let result = client
            .query_opt(
                "SELECT r.run_id FROM runs r JOIN run_submissions s ON s.run_id=r.run_id \
                 WHERE r.push_state_json IS NOT NULL AND r.conclusion IS NOT NULL \
                   AND s.submission_json::jsonb->>'repository'=$1 \
                   AND s.submission_json::jsonb->>'workflow_path'=$3 \
                   AND (s.submission_json::jsonb->>'sha'=$2 \
                        OR r.push_state_json::jsonb->>'effective_sha'=$2) \
                 ORDER BY r.run_id LIMIT 1",
                &[&repository, &sha, &workflow_path],
            )
            .await
            .map(|row| row.map(|row| parse_run_id(&row.get::<_, String>(0))))
            .map_err(ControlError::backend);
        self.return_reader(client).await;
        result
    }

    async fn run_held(&self, run_id: RunId) -> Result<bool, ControlError> {
        let run = run_id.0.to_string();
        let client = self.checkout_reader().await?;
        let result = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE run_id=$1 AND queue_kind='held')",
                &[&run],
            )
            .await
            .map(|row| row.get::<_, bool>(0))
            .map_err(ControlError::backend);
        self.return_reader(client).await;
        result
    }
    async fn artifact_scopes(
        &self,
        plan_ids: &[String],
    ) -> Result<BTreeMap<String, RunId>, ControlError> {
        if plan_ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let client = self.checkout_reader().await?;
        let result = async {
            let rows = client
                .query(
                    "SELECT plan_id, run_id FROM job_requests \
                     WHERE plan_id = ANY($1) ORDER BY request_id DESC",
                    &[&plan_ids],
                )
                .await
                .map_err(ControlError::backend)?;
            let mut scopes = BTreeMap::new();
            for row in rows {
                let plan_id: String = row.get(0);
                let run_id: String = row.get(1);
                scopes
                    .entry(plan_id)
                    .or_insert_with(|| parse_run_id(&run_id));
            }
            Ok(scopes)
        }
        .await;
        self.return_reader(client).await;
        result
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
        crate::store::Store::store_meta_only(&self.aux, meta)
            .await
            .map_err(ControlError::backend)
    }

    async fn load_meta(&self) -> Result<Option<crate::store::MetaSnapshot>, ControlError> {
        crate::store::Store::load_meta_only(&self.aux)
            .await
            .map_err(ControlError::backend)
    }

    async fn ensure_key_fingerprint(&self, fingerprint: &str) -> Result<(), ControlError> {
        let client = self.checkout_writer().await?;
        let result = async {
            client
                .execute(
                    "INSERT INTO control_key_fingerprint(id, fingerprint) VALUES (1, $1) \
                     ON CONFLICT(id) DO NOTHING",
                    &[&fingerprint.as_bytes()],
                )
                .await
                .map_err(ControlError::backend)?;
            let stored: Vec<u8> = client
                .query_one(
                    "SELECT fingerprint FROM control_key_fingerprint WHERE id = 1",
                    &[],
                )
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

    async fn runner_exists(&self, runner_id: i64) -> Result<bool, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query_opt("SELECT 1 FROM runners WHERE runner_id=$1", &[&runner_id])
            .await
            .map(|row| row.is_some())
            .map_err(ControlError::backend);
        self.return_reader(client).await;
        result
    }

    async fn runner_for_client(&self, client_id: &str) -> Result<Option<i64>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query_opt(
                "SELECT runner_id FROM runners WHERE client_id=$1",
                &[&client_id],
            )
            .await
            .map(|row| row.map(|row| row.get(0)))
            .map_err(ControlError::backend);
        self.return_reader(client).await;
        result
    }

    async fn run_for_attempt(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<RunId>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query_opt(
                "SELECT run_id FROM job_requests WHERE agent_job_id=$1",
                &[&agent_job_id.to_string()],
            )
            .await
            .map(|row| row.map(|row| parse_run_id(&row.get::<_, String>(0))))
            .map_err(ControlError::backend);
        self.return_reader(client).await;
        result
    }

    async fn attempt_repository(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<String>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query_opt(
                "SELECT s.submission_json FROM job_requests q \
                 JOIN run_submissions s ON s.run_id = q.run_id \
                 WHERE q.agent_job_id = $1 \
                 AND EXISTS (SELECT 1 FROM runs WHERE run_id = q.run_id)",
                &[&agent_job_id.to_string()],
            )
            .await
            .map_err(ControlError::backend)?
            .map(|row| {
                let json: String = row.get(0);
                serde_json::from_str::<serde_json::Value>(&json)
                    .ok()
                    .and_then(|v| v["repository"].as_str().map(str::to_owned))
                    .ok_or_else(|| {
                        ControlError::backend(anyhow::anyhow!("submission_json missing repository"))
                    })
            })
            .transpose();
        self.return_reader(client).await;
        result
    }

    async fn attempt_in_run(
        &self,
        run_id: RunId,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
    ) -> Result<bool, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM job_requests \
                 WHERE run_id = $1 AND plan_id = $2 AND agent_job_id = $3)",
                &[&run_id.0.to_string(), &plan_id, &agent_job_id.to_string()],
            )
            .await
            .map(|row| row.get::<_, bool>(0))
            .map_err(ControlError::backend);
        self.return_reader(client).await;
        result
    }

    async fn patch_steps(
        &self,
        agent_job_id: uuid::Uuid,
        patches: Vec<StepPatch>,
    ) -> Result<(), ControlError> {
        if patches.is_empty() {
            return Ok(());
        }
        let agent = agent_job_id.to_string();
        let mut client = self.checkout_reader().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            for p in &patches {
                tx.execute(
                    "INSERT INTO job_steps (agent_job_id, step_id, position, kind, workflow_index, \
                 runner_number, context_name, name, conclusion, started_at_us, finished_at_us) \
                 SELECT $1::text, $2::text, COALESCE((SELECT MAX(position) + 1 FROM job_steps \
                 WHERE agent_job_id = $1::text), 0), 'synthetic', NULL, NULL, NULL, $3::text, $4::text, \
                 COALESCE($5::bigint, $7::bigint), $6::bigint \
                 WHERE EXISTS (SELECT 1 FROM job_requests WHERE agent_job_id = $1::text FOR KEY SHARE) \
                 ON CONFLICT(agent_job_id, step_id) DO UPDATE SET name = excluded.name, \
                 conclusion = excluded.conclusion, \
                 started_at_us = COALESCE($5::bigint, job_steps.started_at_us), \
                 finished_at_us = COALESCE($6::bigint, job_steps.finished_at_us)",
                    &[
                        &agent,
                        &p.id,
                        &p.name,
                        &p.conclusion,
                        &p.started_at_us,
                        &p.finished_at_us,
                        &p.observed_us,
                    ],
                )
                .await
                .map_err(ControlError::backend)?;
            }
            tx.commit().await.map_err(ControlError::backend)
        }
        .await;
        self.return_reader(client).await;
        result
    }

    async fn report_steps(
        &self,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
        steps: Vec<serde_json::Value>,
    ) -> Result<bool, ControlError> {
        if steps.is_empty() {
            return Ok(true);
        }
        let agent = agent_job_id.to_string();
        let plan = plan_id.to_owned();
        let mut client = self.checkout_writer().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            // `resolve_callback_job` precedence for a twirp report: the
            // plan's latest request wins; the agent job id is the fallback.
            let resolved: Option<(String, String, bool)> = match tx
                .query_opt(
                    "SELECT q.run_id, q.job_id, j.status = 'cancelled' \
                     FROM job_requests q JOIN jobs j \
                       ON j.run_id = q.run_id AND j.job_id = q.job_id \
                     WHERE q.plan_id = $1 ORDER BY q.request_id DESC LIMIT 1",
                    &[&plan],
                )
                .await
                .map_err(ControlError::backend)?
            {
                Some(row) => Some((row.get(0), row.get(1), row.get(2))),
                None => tx
                    .query_opt(
                        "SELECT q.run_id, q.job_id, j.status = 'cancelled' \
                         FROM job_requests q JOIN jobs j \
                           ON j.run_id = q.run_id AND j.job_id = q.job_id \
                         WHERE q.agent_job_id = $1",
                        &[&agent],
                    )
                    .await
                    .map_err(ControlError::backend)?
                    .map(|row| (row.get(0), row.get(1), row.get(2))),
            };
            let Some((run_id, job_id, job_cancelled)) = resolved else {
                return Ok(false);
            };
            let observed = chrono::Utc::now();
            for step in &steps {
                let Some(report) = crate::control::types::step_report(
                    step,
                    job_cancelled,
                    observed,
                ) else {
                    tracing::warn!(
                        run_id, job = %job_id,
                        "dropping step report with no external_id"
                    );
                    continue;
                };
                // UPDATE first: a reported `NULL` name/runner_number keeps the
                // stored value; a fresh `started_at`/`finished_at` never
                // overwrites one already recorded.
                let updated = tx
                    .execute(
                        "UPDATE job_steps SET \
                         runner_number = COALESCE($3::bigint, runner_number), \
                         name = COALESCE($4::text, name), \
                         conclusion = $5::text, \
                         started_at_us = COALESCE(started_at_us, $6::bigint), \
                         finished_at_us = COALESCE(finished_at_us, $7::bigint) \
                         WHERE agent_job_id = $1 AND step_id = $2",
                        &[
                            &agent,
                            &report.step_id,
                            &report.runner_number,
                            &report.name,
                            &report.conclusion,
                            &report.started_at_us,
                            &report.finished_at_us,
                        ],
                    )
                    .await
                    .map_err(ControlError::backend)?;
                if updated == 0 {
                    tx.execute(
                        "INSERT INTO job_steps (agent_job_id, step_id, position, kind, \
                         workflow_index, runner_number, context_name, name, conclusion, \
                         started_at_us, finished_at_us) \
                         SELECT $1::text, $2::text, COALESCE((SELECT MAX(position) + 1 \
                         FROM job_steps WHERE agent_job_id = $1::text), 0), 'synthetic', \
                         NULL, $3::bigint, NULL, COALESCE($4::text, ''), $5::text, \
                         $6::bigint, $7::bigint \
                         WHERE EXISTS (SELECT 1 FROM job_requests WHERE agent_job_id = $1::text FOR KEY SHARE)",
                        &[
                            &agent,
                            &report.step_id,
                            &report.runner_number,
                            &report.name,
                            &report.conclusion,
                            &report.started_at_us,
                            &report.finished_at_us,
                        ],
                    )
                    .await
                    .map_err(ControlError::backend)?;
                }
            }
            tx.commit().await.map_err(ControlError::backend)?;
            Ok(true)
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn job_detail_missing(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<bool, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query_opt(
                "SELECT detail_json IS NULL FROM run_jobs WHERE run_id = $1 AND job_id = $2",
                &[&run_id.0.to_string(), &job_id.0],
            )
            .await
            .map(|row| row.is_none_or(|row| row.get::<_, bool>(0)))
            .map_err(ControlError::backend);
        self.return_reader(client).await;
        result
    }

    async fn run_job_ids(&self, run_id: RunId) -> Result<Vec<String>, ControlError> {
        let run = run_id.0.to_string();
        let client = self.checkout_reader().await?;
        let result = async {
            let exists: bool = client
                .query_one("SELECT EXISTS(SELECT 1 FROM runs WHERE run_id=$1)", &[&run])
                .await
                .map_err(ControlError::backend)?
                .get(0);
            if !exists {
                return Err(ControlError::NotFound("run not found".to_owned()));
            }
            client
                .query(
                    "SELECT job_id FROM jobs WHERE run_id=$1 \
                     UNION \
                     SELECT job_id FROM job_requests WHERE run_id=$1 \
                     ORDER BY 1",
                    &[&run],
                )
                .await
                .map(|rows| rows.iter().map(|row| row.get::<_, String>(0)).collect())
                .map_err(ControlError::backend)
        }
        .await;
        self.return_reader(client).await;
        result
    }

    async fn ensure_job_detail(
        &self,
        run_id: RunId,
        job_id: &JobId,
        conclusion: Option<&str>,
    ) -> Result<(), ControlError> {
        let conclusion = conclusion.map(str::to_owned);
        let mut client = self.checkout_writer().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            lock_runs(&tx, std::iter::once(&run_id)).await?;
            let run = run_id.0.to_string();
            let job = job_id.0.clone();
            // `JobDetail::find` semantics: match on the stable job key, with a
            // name fallback for details restored without one.
            let rows = tx
                .query(
                    "SELECT job_id, detail_json, detail_position FROM run_jobs \
                     WHERE run_id = $1 AND detail_json IS NOT NULL",
                    &[&run],
                )
                .await
                .map_err(ControlError::backend)?;
            let mut found: Option<(String, crate::models::JobDetail, Option<i64>)> = None;
            for row in &rows {
                let detail: crate::models::JobDetail =
                    serde_json::from_str(row.get::<_, &str>(1)).map_err(ControlError::backend)?;
                if detail.job_id == job || (detail.job_id.is_empty() && detail.name == job) {
                    found = Some((
                        row.get::<_, String>(0),
                        detail,
                        row.get::<_, Option<i64>>(2),
                    ));
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
                return tx.rollback().await.map_err(ControlError::backend);
            }
            let position = match position {
                Some(p) => p,
                None => tx
                    .query_one(
                        "SELECT COALESCE(MAX(detail_position) + 1, 0) FROM run_jobs \
                         WHERE run_id = $1",
                        &[&run],
                    )
                    .await
                    .map_err(ControlError::backend)?
                    .get(0),
            };
            let json = serde_json::to_string(&detail).map_err(ControlError::backend)?;
            tx.execute(
                "INSERT INTO run_jobs (run_id, job_id, detail_json, detail_position) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT(run_id, job_id) DO UPDATE SET \
                 detail_json = excluded.detail_json, \
                 detail_position = excluded.detail_position",
                &[&run, &row_job, &json, &position],
            )
            .await
            .map_err(ControlError::backend)?;
            tx.commit().await.map_err(ControlError::backend)
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn set_job_check_run(
        &self,
        run_id: RunId,
        job_id: &JobId,
        check_run_id: u64,
    ) -> Result<bool, ControlError> {
        let mut client = self.checkout_writer().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            lock_runs(&tx, std::iter::once(&run_id)).await?;
            let changed = tx
                .execute(
                    "INSERT INTO run_jobs (run_id, job_id, check_run_id) \
                     SELECT $1::text, $2::text, $3::bigint \
                     WHERE EXISTS (SELECT 1 FROM jobs WHERE run_id = $1 AND job_id = $2) \
                     ON CONFLICT(run_id, job_id) DO UPDATE SET \
                     check_run_id = excluded.check_run_id \
                     WHERE run_jobs.check_run_id IS DISTINCT FROM $3",
                    &[&run_id.0.to_string(), &job_id.0, &(check_run_id as i64)],
                )
                .await
                .map_err(ControlError::backend)?;
            tx.commit().await.map_err(ControlError::backend)?;
            Ok(changed > 0)
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn clear_job_check_run(
        &self,
        run_id: RunId,
        job_id: &JobId,
        expected: u64,
    ) -> Result<(), ControlError> {
        let mut client = self.checkout_writer().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            lock_runs(&tx, std::iter::once(&run_id)).await?;
            tx.execute(
                "UPDATE run_jobs SET check_run_id = NULL \
                 WHERE run_id = $1 AND job_id = $2 AND check_run_id = $3",
                &[&run_id.0.to_string(), &job_id.0, &(expected as i64)],
            )
            .await
            .map_err(ControlError::backend)?;
            tx.commit().await.map_err(ControlError::backend)
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn job_exists(&self, run_id: RunId, job_id: &JobId) -> Result<bool, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE run_id = $1 AND job_id = $2)",
                &[&run_id.0.to_string(), &job_id.0],
            )
            .await
            .map(|row| row.get::<_, bool>(0))
            .map_err(ControlError::backend);
        self.return_reader(client).await;
        result
    }

    async fn job_display_name(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<String>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query_opt(
                "SELECT display_name FROM run_jobs WHERE run_id = $1 AND job_id = $2",
                &[&run_id.0.to_string(), &job_id.0],
            )
            .await
            .map(|row| row.and_then(|row| row.get::<_, Option<String>>(0)))
            .map_err(ControlError::backend);
        self.return_reader(client).await;
        result
    }

    async fn attempt_job(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<(RunId, JobId)>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query_opt(
                "SELECT run_id, job_id FROM job_requests WHERE agent_job_id=$1",
                &[&agent_job_id.to_string()],
            )
            .await
            .map(|row| row.map(|row| (parse_run_id(&row.get::<_, String>(0)), JobId(row.get(1)))))
            .map_err(ControlError::backend);
        self.return_reader(client).await;
        result
    }

    async fn reap_inputs(&self) -> Result<ReapInputs, ControlError> {
        let timeout = std::time::Duration::from_nanos(
            self.runner_liveness_timeout
                .load(std::sync::atomic::Ordering::Relaxed),
        );
        let cutoff = system_to_us(std::time::SystemTime::now()) - timeout.as_micros() as i64;
        let client = self.checkout_reader().await?;
        let result = async {
            let mut inputs = ReapInputs::default();
            for row in client
                .query("SELECT request_id, run_id, job_id, started_at_us, last_renewed_at_us, \
             timeout_triggered, job_timeout_s FROM job_requests WHERE result IS NULL", &[])
                .await
                .map_err(ControlError::backend)?
            {
                inputs.active.push(ActiveRequest {
                    request_id: row.get(0),
                    run_id: parse_run_id(&row.get::<_, String>(1)),
                    job_id: JobId(row.get(2)),
                    started_at: row.get::<_, Option<i64>>(3).map(us_to_system),
                    last_renewed_at: row.get::<_, Option<i64>>(4).map(us_to_system),
                    timeout_triggered: row.get::<_, i64>(5) != 0,
                    job_timeout_s: row.get(6),
                });
            }
            for row in client
                .query("SELECT run_id, job_id, runs_on, enqueued_at_us, reaper_first_seen_us IS NOT NULL FROM jobs WHERE queue_kind = 'ready' \
             ORDER BY priority DESC, run_order, job_order, seq, run_id, job_id", &[])
                .await
                .map_err(ControlError::backend)?
            {
                inputs.ready.push(ReadyRow {
                    run_id: parse_run_id(&row.get::<_, String>(0)),
                    job_id: JobId(row.get(1)),
                    runs_on: serde_json::from_str(&row.get::<_, String>(2)).unwrap_or_default(),
                    enqueued_at_unix_nanos: row.get::<_, Option<i64>>(3).unwrap_or(0) * 1000,
                    observed: row.get(4),
                });
            }
            for row in client
                .query("SELECT labels FROM runners", &[])
                .await
                .map_err(ControlError::backend)?
            {
                inputs
                    .runner_labels
                    .push(serde_json::from_str(&row.get::<_, String>(0)).unwrap_or_default());
            }
            inputs.has_bindings = client
                .query_one("SELECT EXISTS(SELECT 1 FROM job_assignments) OR EXISTS(SELECT 1 FROM pool_pending)", &[])
                .await
                .map_err(ControlError::backend)?
                .get(0);
            for row in client
                .query("SELECT DISTINCT runner_id FROM runner_sessions \
             WHERE runner_id IS NOT NULL AND last_seen_at_us < $1", &[&cutoff])
                .await
                .map_err(ControlError::backend)?
            {
                inputs.stale_runners.insert(row.get(0));
            }
            for row in client
                .query("SELECT r.runner_id FROM runners r WHERE r.registered_at_us < $1 \
             AND NOT EXISTS (SELECT 1 FROM runner_sessions s WHERE s.runner_id = r.runner_id)", &[&cutoff])
                .await
                .map_err(ControlError::backend)?
            {
                inputs.phantom_runners.insert(row.get(0));
            }
            Ok(inputs)
        }
        .await;
        self.return_reader(client).await;
        result
    }

    async fn run_in_concurrency(&self, run_id: RunId) -> Result<RunConcurrency, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query_one(
                "SELECT \
                   EXISTS(SELECT 1 FROM jobset_gates WHERE run_id=$1) \
                   OR EXISTS(SELECT 1 FROM jobs WHERE run_id=$1 AND concurrency_json IS NOT NULL) \
                   OR EXISTS(SELECT 1 FROM concurrency_holds WHERE holder_run_id=$1 AND holder_kind<>'run') \
                   OR EXISTS(SELECT 1 FROM concurrency_waits WHERE holder_run_id=$1 AND holder_kind<>'run'), \
                 EXISTS(SELECT 1 FROM run_concurrency WHERE run_id=$1) \
                   OR EXISTS(SELECT 1 FROM concurrency_holds WHERE holder_run_id=$1) \
                   OR EXISTS(SELECT 1 FROM concurrency_waits WHERE holder_run_id=$1)",
                &[&run_id.0.to_string()],
            )
            .await
            .map(|row| RunConcurrency::classify(row.get(0), row.get(1)))
            .map_err(ControlError::backend);
        self.return_reader(client).await;
        result
    }

    async fn callback_job(
        &self,
        plan_id: &str,
        timeline_id: Option<uuid::Uuid>,
        agent_job_id: Option<uuid::Uuid>,
    ) -> Result<Option<CallbackJob>, ControlError> {
        let client = self.checkout_reader().await?;
        let timeline = timeline_id.map(|id| id.to_string()).unwrap_or_default();
        let agent = agent_job_id.map(|id| id.to_string()).unwrap_or_default();
        let result = client
            .query_opt(
                "SELECT r.request_id, r.run_id, r.job_id, r.agent_job_id, j.status \
                 FROM job_requests r LEFT JOIN jobs j ON j.run_id = r.run_id AND j.job_id = r.job_id \
                 WHERE r.plan_id = $1 OR r.timeline_id = $2 OR r.agent_job_id = $3 \
                 ORDER BY (r.plan_id = $1) DESC, r.request_id DESC LIMIT 1",
                &[&plan_id, &timeline, &agent],
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

    async fn run_secret_values(&self, run_id: RunId) -> Result<Option<Vec<String>>, ControlError> {
        let client = self.checkout_reader().await?;
        let run = run_id.0.to_string();
        let result = client
            .query_opt(
                "SELECT secrets_blob FROM run_submissions WHERE run_id = $1",
                &[&run],
            )
            .await
            .map_err(ControlError::backend)
            .and_then(|row| {
                row.map(|row| {
                    let sealed: Vec<u8> = row.get(0);
                    let map: BTreeMap<String, String> = blob(&self.cipher, &sealed)?;
                    Ok(map.into_values().collect())
                })
                .transpose()
            });
        self.return_reader(client).await;
        result
    }

    async fn all_secret_values(&self) -> Result<Vec<String>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = async {
            let rows = client
                .query("SELECT secrets_blob FROM run_submissions", &[])
                .await
                .map_err(ControlError::backend)?;
            let mut values = Vec::new();
            for row in rows {
                let sealed: Vec<u8> = row.get(0);
                let map: BTreeMap<String, String> = blob(&self.cipher, &sealed)?;
                values.extend(map.into_values());
            }
            Ok(values)
        }
        .await;
        self.return_reader(client).await;
        result
    }

    async fn sole_inflight_request(
        &self,
    ) -> Result<Option<(i64, RunId, JobId, uuid::Uuid)>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query(
                "SELECT r.request_id, r.run_id, r.job_id, r.agent_job_id \
                 FROM job_requests r \
                 JOIN runner_sessions s ON s.active_request_id = r.request_id \
                 WHERE r.result IS NULL LIMIT 2",
                &[],
            )
            .await
            .map_err(ControlError::backend)
            .map(|rows| {
                if rows.len() != 1 {
                    return None;
                }
                let row = &rows[0];
                Some((
                    row.get::<_, i64>(0),
                    parse_run_id(&row.get::<_, String>(1)),
                    JobId(row.get::<_, String>(2)),
                    parse_uuid(&row.get::<_, String>(3)),
                ))
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

    async fn request_owner(
        &self,
        request_id: i64,
    ) -> Result<Option<(Option<i64>, Option<i64>, bool)>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query_opt(
                "SELECT jr.owner_runner_id, sess.runner_id, sess.session_id IS NOT NULL \
                 FROM job_requests jr \
                 LEFT JOIN runner_sessions sess ON sess.active_request_id = jr.request_id \
                 WHERE jr.request_id = $1",
                &[&request_id],
            )
            .await
            .map_err(ControlError::backend)
            .map(|row| row.map(|row| (row.get(0), row.get(1), row.get(2))));
        self.return_reader(client).await;
        result
    }

    async fn renew_broker_request(
        &self,
        agent_job_id: uuid::Uuid,
        runner_id: i64,
        locked_until: &str,
    ) -> Result<(), ControlError> {
        let agent = agent_job_id.to_string();
        let mut client = self.checkout_writer().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            let row = tx
                .query_opt(
                    "SELECT jr.request_id, jr.run_id, jr.owner_runner_id, \
                     jr.result IS NOT NULL, sess.runner_id, sess.session_id IS NOT NULL \
                     FROM job_requests jr \
                     LEFT JOIN runner_sessions sess ON sess.active_request_id = jr.request_id \
                     WHERE jr.agent_job_id = $1 \
                     ORDER BY jr.request_id DESC LIMIT 1",
                    &[&agent],
                )
                .await
                .map_err(ControlError::backend)?;
            let Some(row) = row else {
                return Err(ControlError::NotFound(
                    "broker renew request not found".to_owned(),
                ));
            };
            let request_id: i64 = row.get(0);
            let run_id = parse_run_id(&row.get::<_, String>(1));
            let owner: Option<i64> = row.get(2);
            let settled: bool = row.get(3);
            let session_runner: Option<i64> = row.get(4);
            let has_session: bool = row.get(5);
            // Same ladder as ensure_broker_request_owner: recorded owner
            // first, then the owning session's runner, then replay-compat
            // "assigned but unowned".
            match owner.or(session_runner) {
                Some(owner) if owner != runner_id => {
                    return Err(ControlError::Forbidden(
                        "broker request belongs to another runner".to_owned(),
                    ));
                }
                None if !has_session => {
                    return Err(ControlError::NotFound(
                        "broker request is not assigned to a session".to_owned(),
                    ));
                }
                _ => {}
            }
            if settled {
                return Err(ControlError::Conflict(
                    "broker request already completed".to_owned(),
                ));
            }
            // The run lock serializes against a scoped write-back carrying a
            // stale copy of this request row.
            lock_runs(&tx, std::iter::once(&run_id)).await?;
            let renewed = tx
                .execute(
                    "UPDATE job_requests SET locked_until = $1, last_renewed_at_us = $2 \
                     WHERE request_id = $3 AND result IS NULL",
                    &[
                        &locked_until,
                        &system_to_us(std::time::SystemTime::now()),
                        &request_id,
                    ],
                )
                .await
                .map_err(ControlError::backend)?;
            if renewed == 0 {
                return Err(ControlError::Conflict(
                    "broker request already completed".to_owned(),
                ));
            }
            tx.commit().await.map_err(ControlError::backend)
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn create_broker_session(
        &self,
        session_id: &str,
        runner_id: i64,
        encryption: &SessionEncryption,
    ) -> Result<(), ControlError> {
        let sealed = self
            .cipher
            .seal(&encryption.key)
            .map_err(ControlError::backend)?;
        let mut client = self.checkout_writer().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            // Liveness check and insert must share a transaction: the sweep
            // may purge the runner between the two statements.
            let live = tx
                .query_opt("SELECT 1 FROM runners WHERE runner_id = $1", &[&runner_id])
                .await
                .map_err(ControlError::backend)?
                .is_some();
            if !live {
                return Err(ControlError::Forbidden(
                    "runner registration is no longer active".to_owned(),
                ));
            }
            tx.execute(
                "INSERT INTO runner_sessions (session_id, runner_id, protocol, \
                 encryption_blob, created_at_us) VALUES ($1, $2, 'broker', $3, $4) \
                 ON CONFLICT(session_id) DO UPDATE SET runner_id = excluded.runner_id, \
                 encryption_blob = excluded.encryption_blob",
                &[
                    &session_id,
                    &runner_id,
                    &sealed,
                    &system_to_us(std::time::SystemTime::now()),
                ],
            )
            .await
            .map_err(ControlError::backend)?;
            tx.commit().await.map_err(ControlError::backend)
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn delete_broker_session(
        &self,
        session_id: &str,
        runner_id: i64,
    ) -> Result<bool, ControlError> {
        let mut client = self.checkout_writer().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            let owner = tx
                .query_opt(
                    "SELECT runner_id FROM runner_sessions WHERE session_id = $1",
                    &[&session_id],
                )
                .await
                .map_err(ControlError::backend)?;
            let outcome = match owner {
                Some(row) => {
                    let owner: Option<i64> = row.get(0);
                    if owner.is_some_and(|owner| owner != runner_id) {
                        return Err(ControlError::Forbidden(
                            "broker session belongs to another runner".to_owned(),
                        ));
                    }
                    // One DELETE retires the binding, the sealed key and the
                    // active request; undelivered messages cascade.
                    tx.execute(
                        "DELETE FROM runner_sessions WHERE session_id = $1",
                        &[&session_id],
                    )
                    .await
                    .map_err(ControlError::backend)?;
                    true
                }
                None => false,
            };
            tx.commit().await.map_err(ControlError::backend)?;
            Ok(outcome)
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn orphaned_claims(&self) -> Result<Vec<(i64, RunId, JobId)>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query(
                "SELECT r.request_id, r.run_id, r.job_id FROM job_requests r \
                 WHERE r.result IS NULL \
                 AND (r.owner_runner_id IS NOT NULL OR EXISTS ( \
                     SELECT 1 FROM runner_sessions s2 WHERE s2.active_request_id = r.request_id)) \
                 AND NOT EXISTS ( \
                     SELECT 1 FROM runner_sessions s JOIN runners rn ON rn.runner_id = s.runner_id \
                     WHERE s.active_request_id = r.request_id)",
                &[],
            )
            .await
            .map_err(ControlError::backend)
            .map(|rows| {
                rows.iter()
                    .map(|row| {
                        (
                            row.get::<_, i64>(0),
                            parse_run_id(&row.get::<_, String>(1)),
                            JobId(row.get::<_, String>(2)),
                        )
                    })
                    .collect()
            });
        self.return_reader(client).await;
        result
    }

    async fn release_claimed_request(
        &self,
        request_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError> {
        let mut client = self.checkout_writer().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            let released = tx
                .execute(
                    "UPDATE job_requests SET owner_runner_id = NULL, started_at_us = NULL, \
                     last_renewed_at_us = NULL, timeout_triggered = 0, locked_until = $1 \
                     WHERE request_id = $2 AND result IS NULL",
                    &[&locked_until, &request_id],
                )
                .await
                .map_err(ControlError::backend)?;
            if released == 0 {
                tx.commit().await.map_err(ControlError::backend)?;
                return Ok(false);
            }
            tx.execute(
                "UPDATE runner_sessions SET active_request_id = NULL \
                 WHERE active_request_id = $1",
                &[&request_id],
            )
            .await
            .map_err(ControlError::backend)?;
            tx.commit().await.map_err(ControlError::backend)?;
            Ok(true)
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn settle_request(
        &self,
        request_id: i64,
        result: ExecutionStatus,
        locked_until: &str,
    ) -> Result<Option<(RunId, JobId, uuid::Uuid)>, ControlError> {
        let status = status_str(result).to_owned();
        let locked_until = locked_until.to_owned();
        let mut client = self.checkout_writer().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            let row = tx
                .query_opt(
                    "SELECT run_id, job_id, agent_job_id FROM job_requests WHERE request_id = $1",
                    &[&request_id],
                )
                .await
                .map_err(ControlError::backend)?;
            let Some(row) = row else {
                tx.commit().await.map_err(ControlError::backend)?;
                return Ok(None);
            };
            let tuple = (
                parse_run_id(&row.get::<_, String>(0)),
                JobId(row.get::<_, String>(1)),
                parse_uuid(&row.get::<_, String>(2)),
            );
            // The run lock serializes against a scoped write-back carrying a
            // stale copy of this request row.
            lock_runs(&tx, std::iter::once(&tuple.0)).await?;
            tx.execute(
                "DELETE FROM github_token_requests WHERE request_id = $1",
                &[&request_id],
            )
            .await
            .map_err(ControlError::backend)?;
            // The owner session's queued JobCancellation is moot once the
            // request settles — drop it so the next (busy) poll cannot
            // redeliver a cancellation for finished work.
            let sessions = tx
                .query(
                    "SELECT session_id FROM runner_sessions WHERE active_request_id = $1",
                    &[&request_id],
                )
                .await
                .map_err(ControlError::backend)?;
            for session in sessions {
                let session_id: String = session.get(0);
                let messages = tx
                    .query(
                        "SELECT message_id, message_blob FROM broker_messages \
                         WHERE session_id = $1",
                        &[&session_id],
                    )
                    .await
                    .map_err(ControlError::backend)?;
                for message in messages {
                    let sealed: Vec<u8> = message.get(1);
                    let msg: preloop_gha_protocol::azdo::TaskAgentMessage =
                        match blob(&self.cipher, &sealed) {
                            Ok(msg) => msg,
                            Err(_) => continue,
                        };
                    if msg.message_type == preloop_gha_protocol::azdo::message_type::JOB_CANCELLED {
                        tx.execute(
                            "DELETE FROM broker_messages WHERE session_id = $1 AND message_id = $2",
                            &[&session_id, &message.get::<_, i64>(0)],
                        )
                        .await
                        .map_err(ControlError::backend)?;
                    }
                }
            }
            tx.execute(
                "UPDATE runner_sessions SET active_request_id = NULL \
                 WHERE active_request_id = $1",
                &[&request_id],
            )
            .await
            .map_err(ControlError::backend)?;
            tx.execute(
                "UPDATE job_requests SET result = $1, locked_until = $2 \
                 WHERE request_id = $3 AND result IS NULL",
                &[&status, &locked_until, &request_id],
            )
            .await
            .map_err(ControlError::backend)?;
            tx.commit().await.map_err(ControlError::backend)?;
            Ok(Some(tuple))
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn job_queue_state(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<(String, String)>, ControlError> {
        let client = self.checkout_reader().await?;
        let run = run_id.0.to_string();
        let result = client
            .query_opt(
                "SELECT queue_kind, status FROM jobs WHERE run_id = $1 AND job_id = $2",
                &[&run, &job_id.0],
            )
            .await
            .map_err(ControlError::backend)
            .map(|row| row.map(|row| (row.get::<_, String>(0), row.get::<_, String>(1))));
        self.return_reader(client).await;
        result
    }

    async fn renew_agent_request(
        &self,
        request_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError> {
        let mut client = self.checkout_writer().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            let run = tx
                .query_opt(
                    "SELECT run_id FROM job_requests WHERE request_id = $1",
                    &[&request_id],
                )
                .await
                .map_err(ControlError::backend)?;
            let Some(run) = run else {
                // Unknown request: the PATCH renew contract is a silent
                // no-op.
                tx.commit().await.map_err(ControlError::backend)?;
                return Ok(false);
            };
            // The run lock serializes against a scoped write-back carrying
            // a stale copy of this request row.
            lock_runs(
                &tx,
                std::iter::once(&parse_run_id(&run.get::<_, String>(0))),
            )
            .await?;
            let renewed = tx
                .execute(
                    "UPDATE job_requests SET locked_until = $1, last_renewed_at_us = $2 \
                     WHERE request_id = $3 AND result IS NULL",
                    &[
                        &locked_until,
                        &system_to_us(std::time::SystemTime::now()),
                        &request_id,
                    ],
                )
                .await
                .map_err(ControlError::backend)?;
            tx.commit().await.map_err(ControlError::backend)?;
            Ok(renewed == 1)
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn settle_agent_request(
        &self,
        request_id: i64,
        result: ExecutionStatus,
        locked_until: &str,
    ) -> Result<Option<(RunId, JobId, uuid::Uuid)>, ControlError> {
        let status = status_str(result).to_owned();
        let locked_until = locked_until.to_owned();
        let mut client = self.checkout_writer().await?;
        let result = async {
            let tx = client.transaction().await.map_err(ControlError::backend)?;
            let run = tx
                .query_opt(
                    "SELECT run_id FROM job_requests WHERE request_id = $1",
                    &[&request_id],
                )
                .await
                .map_err(ControlError::backend)?;
            let Some(run) = run else {
                // Unknown request: the PATCH complete contract is a silent
                // no-op (the row never settles, so no completion fans out).
                tx.commit().await.map_err(ControlError::backend)?;
                return Ok(None);
            };
            // The run lock serializes against a scoped write-back carrying
            // a stale copy of this request row.
            lock_runs(
                &tx,
                std::iter::once(&parse_run_id(&run.get::<_, String>(0))),
            )
            .await?;
            let settled = tx
                .query_opt(
                    "UPDATE job_requests SET result = $1, locked_until = $2 \
                     WHERE request_id = $3 AND result IS NULL \
                     RETURNING run_id, job_id, agent_job_id",
                    &[&status, &locked_until, &request_id],
                )
                .await
                .map_err(ControlError::backend)?;
            tx.commit().await.map_err(ControlError::backend)?;
            Ok(settled.map(|row| {
                (
                    parse_run_id(&row.get::<_, String>(0)),
                    JobId(row.get::<_, String>(1)),
                    parse_uuid(&row.get::<_, String>(2)),
                )
            }))
        }
        .await;
        self.return_writer(client).await;
        result
    }

    async fn delete_inflight(&self, session_id: &str, message_id: i64) -> Result<(), ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .execute(
                "DELETE FROM broker_messages WHERE session_id = $1 AND message_id = $2",
                &[&session_id, &message_id],
            )
            .await
            .map_err(ControlError::backend)
            .map(|_| ());
        self.return_reader(client).await;
        result
    }

    async fn active_plan_ids(&self) -> Result<BTreeSet<String>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query(
                "SELECT DISTINCT plan_id FROM job_requests WHERE result IS NULL",
                &[],
            )
            .await
            .map_err(ControlError::backend)
            .map(|rows| rows.iter().map(|row| row.get::<_, String>(0)).collect());
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
        let client = self.checkout_reader().await?;
        let result = async {
            let mut stats = QueueStats::default();
            for row in client
                .query(
                    "SELECT queue_kind, COUNT(*) FROM jobs
                     WHERE queue_kind NOT IN ('none', 'expand')
                     GROUP BY queue_kind
                     UNION ALL
                     SELECT 'expand', COUNT(*) FROM jobs
                     WHERE queue_kind = 'expand' AND expand_generation = 0",
                    &[],
                )
                .await
                .map_err(ControlError::backend)?
            {
                let count: i64 = row.get(1);
                let count = count.max(0) as usize;
                match row.get::<_, String>(0).as_str() {
                    "ready" => stats.ready = count,
                    "pending" => stats.pending = count,
                    "blocked" => stats.blocked = count,
                    "held" => stats.held = count,
                    "claimed" => stats.claimed = count,
                    "expand" => stats.expanding = count,
                    _ => {}
                }
            }
            stats.next_runs_on = client
                .query_opt(
                    "SELECT runs_on FROM jobs WHERE queue_kind = 'ready'
                     ORDER BY priority DESC, run_order, job_order, seq, run_id, job_id LIMIT 1",
                    &[],
                )
                .await
                .map_err(ControlError::backend)?
                .and_then(|row| serde_json::from_str(&row.get::<_, String>(0)).ok())
                .unwrap_or_default();
            Ok(stats)
        }
        .await;
        self.return_reader(client).await;
        result
    }

    async fn live_assignments(
        &self,
    ) -> Result<Vec<preloop_observability::status::RunnerAssignment>, ControlError> {
        let client = self.checkout_reader().await?;
        let result = client
            .query(
                "SELECT r.owner_runner_id, r.run_id, r.job_id, r.started_at_us
                 FROM runner_sessions s JOIN job_requests r
                   ON s.active_request_id = r.request_id
                 WHERE r.result IS NULL AND r.owner_runner_id IS NOT NULL
                 ORDER BY r.owner_runner_id",
                &[],
            )
            .await
            .map(|rows| {
                let now = std::time::SystemTime::now();
                rows.into_iter()
                    .map(|row| preloop_observability::status::RunnerAssignment {
                        runner_id: row.get(0),
                        run_id: row.get(1),
                        job_id: row.get(2),
                        assigned_seconds_ago: row
                            .get::<_, Option<i64>>(3)
                            .and_then(|us| now.duration_since(us_to_system(us)).ok())
                            .map(|age| age.as_secs_f64())
                            .unwrap_or(0.0),
                    })
                    .collect()
            })
            .map_err(ControlError::backend);
        self.return_reader(client).await;
        result
    }
    async fn pair_runner(&self, runner_id: i64) -> Result<(), ControlError> {
        self.transact(move |tx| {
            sched::pair_registered_runner(tx, runner_id);
            Ok(())
        })
        .await
    }

    async fn list_runners(&self, run_id: Option<RunId>) -> Result<RunnerListing, ControlError> {
        self.read(move |tx| Ok(commands::list_runners_tx(tx, run_id)))
            .await
    }

    async fn runner_rsa_public_key(
        &self,
        runner_id: i64,
    ) -> Result<Option<AgentRsaPublicKey>, ControlError> {
        self.read(move |tx| Ok(tx.runner_rsa_public_keys.get(&runner_id).cloned()))
            .await
    }

    async fn open_runner_session(&self, open: OpenRunnerSession) -> Result<(), ControlError> {
        self.transact(move |tx| commands::open_runner_session_tx(tx, open))
            .await
    }

    async fn close_runner_session(
        &self,
        session_id: &str,
        caller_runner_id: Option<i64>,
    ) -> Result<bool, ControlError> {
        self.transact(move |tx| commands::close_runner_session_tx(tx, session_id, caller_runner_id))
            .await
    }

    async fn purge_runner_guarded(
        &self,
        runner_id: i64,
        guard: PurgeGuard,
    ) -> Result<bool, ControlError> {
        self.transact(move |tx| commands::purge_runner_guarded_tx(tx, runner_id, guard))
            .await
    }

    async fn ephemeral_runner_ids(&self) -> Result<Vec<i64>, ControlError> {
        self.read(|tx| {
            Ok(tx
                .runners
                .values()
                .filter(|runner| runner.ephemeral)
                .map(|runner| runner.id)
                .collect())
        })
        .await
    }

    async fn runner_ids_named(&self, name: &str) -> Result<Vec<i64>, ControlError> {
        self.read(move |tx| {
            Ok(tx
                .runners
                .iter()
                .filter(|(_, runner)| runner.name == name)
                .map(|(id, _)| *id)
                .collect())
        })
        .await
    }

    async fn lookup_agent(
        &self,
        name: &str,
    ) -> Result<Option<(preloop_gha_protocol::RegisteredRunner, String)>, ControlError> {
        self.transact(move |tx| Ok(commands::lookup_agent_tx(tx, name)))
            .await
    }

    async fn bind_runner_client(
        &self,
        runner_id: i64,
        client_id: &str,
        pair_with_pending_job: bool,
    ) -> Result<(), ControlError> {
        self.transact(move |tx| {
            commands::bind_runner_client_tx(tx, runner_id, client_id, pair_with_pending_job);
            Ok(())
        })
        .await
    }

    async fn update_runner(
        &self,
        runner_id: i64,
        name: Option<String>,
        labels: Option<Vec<String>>,
    ) -> Result<RunnerRow, ControlError> {
        self.transact(move |tx| commands::update_runner_tx(tx, runner_id, name, labels))
            .await
    }

    async fn reap_sweep(&self, sweep: ReapSweep) -> Result<ReapSweepOutcome, ControlError> {
        if sweep.runs.is_empty() {
            return Ok(ReapSweepOutcome::default());
        }
        let mut scope = TxScope::runs(sweep.runs.clone());
        // Settling an expired lease frees its owner session.
        scope.sessions = None;
        self.transact_scoped(&scope, move |tx| Ok(commands::reap_sweep_tx(tx, sweep)))
            .await
    }

    async fn status_inputs(
        &self,
        stale_after: std::time::Duration,
    ) -> Result<StatusInputs, ControlError> {
        self.read(move |tx| Ok(commands::status_inputs_tx(tx, stale_after)))
            .await
    }

    async fn rebuild_dispatch_intent(&self) -> Result<(), ControlError> {
        self.transact(|tx| {
            commands::rebuild_dispatch_intent_tx(tx);
            Ok(())
        })
        .await
    }

    async fn runs_for_repository(&self, repository: &str) -> Result<Vec<RunRecord>, ControlError> {
        self.read(move |tx| {
            Ok(tx
                .runs
                .values()
                .filter(|run| run.submission.repository.eq_ignore_ascii_case(repository))
                .cloned()
                .collect())
        })
        .await
    }

    async fn poll_azdo_session(&self, poll: AzdoPoll) -> Result<AzdoPollOutcome, ControlError> {
        self.transact(move |tx| Ok(commands::poll_azdo_session_tx(tx, poll)))
            .await
    }

    async fn settle_job(&self, settle: SettleJob) -> Result<SettleJobOutcome, ControlError> {
        let run_id = settle.completion.run_id;
        // `sessions: {owner}` — `settle_request` drops the moot cancellation
        // from the owner session's inflight messages; resolve the owners
        // before the transaction. Internal completions carry no attempt:
        // every session owning a request of the run.
        let sessions = match settle.completion.agent_job_id {
            Some(id) => self
                .find_session_by_agent_job_id(id)
                .await?
                .into_iter()
                .collect(),
            None => self.find_sessions_by_run(run_id).await?,
        };
        let in_concurrency = self.run_in_concurrency(run_id).await?;
        // A workflow-level group changes only when the whole run turns
        // terminal, so such a run first settles under its own run scope and
        // widens to the global scope only if this completion finishes the
        // run. Job-level gates always go global.
        let mut widen = in_concurrency == RunConcurrency::Gated;
        loop {
            let guard_terminal = !widen && in_concurrency == RunConcurrency::WorkflowOnly;
            let scope = TxScope {
                include_archived: false,
                runs: Some(BTreeSet::from([run_id])),
                // `needs:` never cross runs: this run's pending rows load via
                // `runs`; the queue snapshots stay O(1).
                ready_queue: false,
                blocked_jobs: false,
                sessions: Some(sessions.clone()),
                // Only a completion that can release a gate needs every
                // holder's run.
                concurrency: widen,
                runs_referenced: true,
                job_requests_all: false,
                pending_expansions: false,
                runs_via_requests: false,
            };
            let input = settle.clone();
            match self
                .transact_scoped(&scope, move |tx| {
                    commands::settle_job_tx(tx, input, guard_terminal)
                })
                .await
            {
                Err(ControlError::WidenScope) if !widen => widen = true,
                other => return other,
            }
        }
    }

    async fn oidc_grant(
        &self,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
    ) -> Result<OidcGrant, ControlError> {
        self.read(move |tx| commands::oidc_grant_tx(tx, plan_id, agent_job_id))
            .await
    }
}
