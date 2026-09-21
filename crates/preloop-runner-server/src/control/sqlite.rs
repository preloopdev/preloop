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
use super::txstate::TxState;
use super::types::*;
use crate::concurrency;
use crate::models::{QueuedJob, RunRecord, TaskAgentJobRequestRecord};
use crate::state::JobSetId;
use crate::store;
use parking_lot::Mutex;
use preloop_gha_protocol::crypto::{AgentRsaPublicKey, SessionEncryption};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId, SessionId};
use rusqlite::{params, Connection};
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
    pool_assignments_enabled: bool,
    require_job_assignments: bool,
    runner_liveness_timeout: std::time::Duration,
}

impl SqliteBackend {
    /// Open (or create) the control database and migrate it to the current
    /// schema version.
    pub(crate) fn open(
        path: &std::path::Path,
        pool_assignments_enabled: bool,
        require_job_assignments: bool,
        runner_liveness_timeout: std::time::Duration,
    ) -> Result<Self, ControlError> {
        let conn = Connection::open(path).map_err(ControlError::backend)?;
        conn.execute_batch(SQLITE_DDL)
            .map_err(ControlError::backend)?;
        conn.pragma_update(None, "user_version", SQLITE_SCHEMA_VERSION)
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
        Ok(Self {
            conn: Mutex::new(conn),
            readers: Mutex::new(readers),
            readers_idle: parking_lot::Condvar::new(),
            pool_assignments_enabled,
            require_job_assignments,
            runner_liveness_timeout,
        })
    }

    /// An in-memory backend for the behavioral suite.
    #[cfg(test)]
    pub(crate) fn in_memory() -> Result<Self, ControlError> {
        let conn = Connection::open_in_memory().map_err(ControlError::backend)?;
        conn.execute_batch(SQLITE_DDL)
            .map_err(ControlError::backend)?;
        Ok(Self {
            conn: Mutex::new(conn),
            // A second connection can't see an in-memory database, so the
            // pool is empty and `read` falls back to the writer.
            readers: Mutex::new(Vec::new()),
            readers_idle: parking_lot::Condvar::new(),
            pool_assignments_enabled: false,
            require_job_assignments: false,
            runner_liveness_timeout: std::time::Duration::from_secs(300),
        })
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
        let mut conn = self.conn.lock();
        let txn = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(ControlError::backend)?;
        let mut tx = load_txstate(&txn)?.with_config(
            self.pool_assignments_enabled,
            self.require_job_assignments,
            self.runner_liveness_timeout,
        );
        let result = f(&mut tx)?;
        write_txstate(&txn, &tx)?;
        txn.commit().map_err(ControlError::backend)?;
        Ok(result)
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
        // An empty pool (in-memory databases) can't lend a reader — fall
        // back to the writer, which still gives a consistent snapshot.
        if self.readers.lock().is_empty() {
            let mut conn = self.conn.lock();
            let txn = conn.transaction().map_err(ControlError::backend)?;
            let tx = load_txstate(&txn)?.with_config(
                self.pool_assignments_enabled,
                self.require_job_assignments,
                self.runner_liveness_timeout,
            );
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
            let tx = load_txstate(&txn)?.with_config(
                self.pool_assignments_enabled,
                self.require_job_assignments,
                self.runner_liveness_timeout,
            );
            let result = f(&tx)?;
            txn.rollback().map_err(ControlError::backend)?;
            Ok(result)
        })();
        self.readers.lock().push(conn);
        self.readers_idle.notify_one();
        result
    }
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

fn blob<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, ControlError> {
    serde_json::from_slice(bytes).map_err(ControlError::backend)
}

fn unblob<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, ControlError> {
    serde_json::to_vec(v).map_err(ControlError::backend)
}

fn parse_run_id(s: &str) -> RunId {
    RunId(s.parse().unwrap_or_default())
}

fn parse_uuid(s: &str) -> uuid::Uuid {
    s.parse().unwrap_or_default()
}

/// Load the full scheduling working set. For a single-node deployment the
/// working set is most of the control state — bounded by the number of
/// live runs/jobs — and loading it whole keeps the ported logic identical
/// to the in-memory version. Narrowing to per-command scopes is a later
/// optimization that must not change results.
fn load_txstate(conn: &Connection) -> Result<TxState, ControlError> {
    let mut tx = TxState::default();

    // Runs: the record blob carries everything except the derived `jobs`
    // map, which we rebuild from the `jobs` table.
    {
        let mut stmt = conn
            .prepare("SELECT run_id, record_blob FROM runs")
            .map_err(ControlError::backend)?;
        let rows = stmt
            .query_map([], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (run_id_s, record) = row.map_err(ControlError::backend)?;
            let run_id = parse_run_id(&run_id_s);
            let value: serde_json::Value = blob(&record)?;
            let mut run = store::run_record_from_value(value).map_err(ControlError::backend)?;
            run.jobs.clear(); // rebuilt from the jobs table below
            tx.runs.insert(run_id, run);
            tx.loaded.runs.insert(run_id);
        }
    }

    // Jobs: route each row into its queue collection and rebuild run.jobs.
    // `seq` is the write-order counter that preserves FIFO within a kind.
    {
        let mut stmt = conn
            .prepare(
                "SELECT run_id, job_id, status, queue_kind, queue_position, seq, \
                 reaper_first_seen_us, expand_generation, payload_blob \
                 FROM jobs ORDER BY seq",
            )
            .map_err(ControlError::backend)?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, Option<i64>>(4)?,
                    r.get::<_, Option<i64>>(5)?,
                    r.get::<_, Option<i64>>(6)?,
                    r.get::<_, i64>(7)?,
                    r.get::<_, Option<Vec<u8>>>(8)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (
                run_id_s,
                job_id_s,
                status,
                kind,
                _pos,
                _seq,
                reaper_us,
                expand_generation,
                payload,
            ) = row.map_err(ControlError::backend)?;
            let run_id = parse_run_id(&run_id_s);
            let job_id = JobId(job_id_s);
            let status = status_parse(&status);
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
                let job: QueuedJob = blob(&payload)?;
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
    tx.ready_count = tx.ready_index.len() as i64;
    tx.next_queue_position = conn
        .query_row(
            "SELECT COALESCE(MAX(queue_position),0)+1 FROM jobs WHERE queue_kind='ready'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(1);

    // Requests + derived indexes.
    {
        let mut stmt = conn
            .prepare(
                "SELECT request_id, run_id, job_id, agent_job_id, plan_id, plan_type, \
                 timeline_id, result, locked_until, claimed_at_us, owner_runner_id, \
                 started_at_us, last_renewed_at_us, timeout_triggered, debug_token_issued, \
                 request_blob FROM job_requests",
            )
            .map_err(ControlError::backend)?;
        let rows = stmt
            .query_map([], |r| {
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
            let record = TaskAgentJobRequestRecord {
                request_id,
                run_id: parse_run_id(&run_id_s),
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
                if let Ok(msg) = serde_json::from_slice(&msg) {
                    tx.broker_messages.insert(request_id, msg);
                }
            }
            tx.insert_request(record);
        }
    }

    // Token requests, grants, OIDC contexts, steps.
    {
        let mut stmt = conn
            .prepare("SELECT request_id, request_blob FROM github_token_requests")
            .map_err(ControlError::backend)?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)))
            .map_err(ControlError::backend)?;
        for row in rows {
            let (request_id, req) = row.map_err(ControlError::backend)?;
            if let Ok(req) = serde_json::from_slice(&req) {
                tx.github_token_requests.insert(request_id, req);
            }
        }
    }
    {
        let mut stmt = conn
            .prepare("SELECT run_id, job_id, granted FROM id_token_grants")
            .map_err(ControlError::backend)?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (run_id_s, job_id_s, granted) = row.map_err(ControlError::backend)?;
            tx.id_token_grants
                .insert((parse_run_id(&run_id_s), JobId(job_id_s)), granted != 0);
        }
    }
    {
        let mut stmt = conn
            .prepare("SELECT run_id, job_id, context_blob FROM oidc_job_contexts")
            .map_err(ControlError::backend)?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (run_id_s, job_id_s, ctx) = row.map_err(ControlError::backend)?;
            if let Ok(ctx) = serde_json::from_slice(&ctx) {
                tx.oidc_job_contexts
                    .insert((parse_run_id(&run_id_s), JobId(job_id_s)), ctx);
            }
        }
    }
    {
        let mut stmt = conn
            .prepare("SELECT agent_job_id, steps_blob, revision FROM job_steps")
            .map_err(ControlError::backend)?;
        let rows = stmt
            .query_map([], |r| {
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
            if let Ok(steps) = serde_json::from_slice(&steps) {
                tx.job_steps.insert(agent_job_id, steps);
                tx.job_steps_revision.insert(agent_job_id, revision as u64);
            }
            tx.loaded.step_attempts.insert(agent_job_id);
        }
    }

    // Runners.
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
        let mut stmt = conn
            .prepare(
                "SELECT session_id, runner_id, protocol, encryption_blob, \
                 active_request_id, last_seen_at_us FROM runner_sessions",
            )
            .map_err(ControlError::backend)?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<Vec<u8>>>(3)?,
                    r.get::<_, Option<i64>>(4)?,
                    r.get::<_, Option<i64>>(5)?,
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
            ) = row.map_err(ControlError::backend)?;
            match SessionProtocol::parse(&protocol) {
                SessionProtocol::Broker => {
                    tx.broker_session_runners
                        .insert(session_id.clone(), runner_id);
                }
                SessionProtocol::Azdo => {
                    tx.sessions.insert(
                        session_id.clone(),
                        preloop_gha_protocol::RunnerSession {
                            session_id: SessionId(parse_uuid(&session_id)),
                            runner_id,
                        },
                    );
                    tx.azdo_sessions.insert(session_id.clone());
                }
            }
            if let Some(key) = encryption_blob {
                tx.session_keys
                    .insert(session_id.clone(), SessionEncryption::from_key(key));
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
            if let Ok(msg) = serde_json::from_slice(&msg) {
                tx.inflight_messages
                    .entry(session_id)
                    .or_default()
                    .insert(message_id, msg);
            }
        }
    }

    // Concurrency.
    {
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
            let running: Option<concurrency::Holder> =
                running.and_then(|b| serde_json::from_slice(&b).ok());
            let pending: VecDeque<concurrency::Holder> =
                serde_json::from_slice(&pending).unwrap_or_default();
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
            if let (Ok(gates), Ok(acquired_keys)) = (
                serde_json::from_slice(&gates),
                serde_json::from_slice(&acquired),
            ) {
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
    {
        let mut stmt = conn
            .prepare("SELECT run_id, concurrency_blob FROM run_concurrency")
            .map_err(ControlError::backend)?;
        let rows = stmt
            .query_map([], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (run_id_s, c) = row.map_err(ControlError::backend)?;
            if let Ok(c) = serde_json::from_slice(&c) {
                tx.run_concurrency.insert(parse_run_id(&run_id_s), c);
            }
        }
    }

    // Assignments, pool pending, cancellations.
    {
        let mut stmt = conn
            .prepare("SELECT run_id, job_id, runner_id, at_us, first_at_us FROM job_assignments")
            .map_err(ControlError::backend)?;
        let rows = stmt
            .query_map([], |r| {
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
        let mut stmt = conn
            .prepare("SELECT run_id, job_id, at_us FROM pool_pending")
            .map_err(ControlError::backend)?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })
            .map_err(ControlError::backend)?;
        for row in rows {
            let (run_id_s, job_id_s, at_us) = row.map_err(ControlError::backend)?;
            tx.pool_pending.insert(
                (parse_run_id(&run_id_s), JobId(job_id_s)),
                us_to_system(at_us),
            );
        }
    }
    {
        let mut stmt = conn
            .prepare("SELECT run_id, job_id, agent_job_id FROM cancellation_queue ORDER BY seq")
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
            let (run_id_s, job_id_s, agent_job_id_s) = row.map_err(ControlError::backend)?;
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

    // Counters.
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
                _ => {}
            }
            tx.loaded.counters.insert(name, value);
        }
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

    Ok(tx)
}

// ─────────────────────────────────────────────────────────────────────────
// Write-back: TxState delta → rows
// ─────────────────────────────────────────────────────────────────────────

/// Persist the working set inside the transaction. Upserts every present
/// row and deletes rows that were loaded but removed by the command. The
/// `loaded` bookkeeping is what makes deletion precise: a key present at
/// load but absent now was removed by the command.
fn write_txstate(conn: &Connection, tx: &TxState) -> Result<(), ControlError> {
    let now_us = system_to_us(std::time::SystemTime::now());

    // Runs: upsert present, delete removed.
    for (run_id, run) in &tx.runs {
        let mut record = run.clone();
        record.jobs.clear(); // derived from the jobs table
        let value = store::run_record_value(&record).map_err(ControlError::backend)?;
        let record_blob = serde_json::to_vec(&value).map_err(ControlError::backend)?;
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

    // Jobs: rebuild the table from the working set's collections. A job's
    // queue_kind is derived from which collection holds it; status comes
    // from run.jobs (canonical). `seq` is a write-order counter that
    // preserves FIFO within a kind across the delete+insert rebuild.
    let mut seen_jobs: BTreeSet<(RunId, JobId)> = BTreeSet::new();
    let mut seq = 0i64;
    let mut ready_position = 0i64;

    let write_job = |conn: &Connection,
                     tx: &TxState,
                     seen: &mut BTreeSet<(RunId, JobId)>,
                     seq: i64,
                     run_id: RunId,
                     job_id: &JobId,
                     kind: QueueKind,
                     position: Option<i64>,
                     job: Option<&QueuedJob>|
     -> Result<(), ControlError> {
        let status = tx
            .runs
            .get(&run_id)
            .and_then(|r| r.jobs.get(job_id).copied())
            .unwrap_or(ExecutionStatus::Queued);
        let (base_id, runs_on, runner_group, enqueued_us, payload) = match job {
            Some(j) => (
                j.base_id.clone(),
                serde_json::to_string(&j.runs_on).unwrap_or_default(),
                j.runner_group.clone(),
                Some(j.enqueued_at_unix_nanos / 1000),
                Some(unblob(j)?),
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
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15) \
             ON CONFLICT(run_id,job_id) DO UPDATE SET status=excluded.status, \
             queue_kind=excluded.queue_kind, queue_position=excluded.queue_position, \
             seq=excluded.seq, reaper_first_seen_us=excluded.reaper_first_seen_us, \
             claimed_by=excluded.claimed_by, claimed_at_us=excluded.claimed_at_us, \
             expand_generation=excluded.expand_generation, \
             payload_blob=excluded.payload_blob",
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
            ],
        )
        .map_err(ControlError::backend)?;
        seen.insert((run_id, job_id.clone()));
        Ok(())
    };

    // Ready queue: ready_index (persisted) then queue (newly enqueued),
    // positions assigned in order.
    for job in tx.ready_index.iter().chain(tx.queue.iter()) {
        seq += 1;
        ready_position += 1;
        write_job(
            conn,
            tx,
            &mut seen_jobs,
            seq,
            job.run_id,
            &job.job_id,
            QueueKind::Ready,
            Some(ready_position),
            Some(job),
        )?;
    }
    for job in &tx.pending_jobs {
        seq += 1;
        write_job(
            conn,
            tx,
            &mut seen_jobs,
            seq,
            job.run_id,
            &job.job_id,
            QueueKind::Pending,
            None,
            Some(job),
        )?;
    }
    for job in &tx.concurrency_blocked {
        seq += 1;
        write_job(
            conn,
            tx,
            &mut seen_jobs,
            seq,
            job.run_id,
            &job.job_id,
            QueueKind::Blocked,
            None,
            Some(job),
        )?;
    }
    for job in &tx.pending_expansions {
        seq += 1;
        write_job(
            conn,
            tx,
            &mut seen_jobs,
            seq,
            job.run_id,
            &job.job_id,
            QueueKind::Expand,
            None,
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
        seq += 1;
        write_job(
            conn,
            tx,
            &mut seen_jobs,
            seq,
            *run_id,
            job_id,
            QueueKind::Expand,
            None,
            tx.expanding_jobs.get(&(*run_id, job_id.clone())),
        )?;
    }
    for ((run_id, job_id), job) in &tx.claimed_jobs {
        seq += 1;
        write_job(
            conn,
            tx,
            &mut seen_jobs,
            seq,
            *run_id,
            job_id,
            QueueKind::Claimed,
            None,
            Some(job),
        )?;
    }
    for (run_id, jobs) in &tx.held_runs {
        for job in jobs {
            seq += 1;
            write_job(
                conn,
                tx,
                &mut seen_jobs,
                seq,
                *run_id,
                &job.job_id,
                QueueKind::Held,
                None,
                Some(job),
            )?;
        }
    }
    // Jobs with no queue collection but present in run.jobs (terminal or
    // placeholder nodes): write them as 'none' so their status persists.
    for (run_id, run) in &tx.runs {
        for job_id in run.jobs.keys() {
            if !seen_jobs.contains(&(*run_id, job_id.clone())) {
                seq += 1;
                write_job(
                    conn,
                    tx,
                    &mut seen_jobs,
                    seq,
                    *run_id,
                    job_id,
                    QueueKind::None,
                    None,
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
        let request_blob = tx.broker_messages.get(request_id).map(unblob).transpose()?;
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
    // Token requests.
    conn.execute("DELETE FROM github_token_requests", [])
        .map_err(ControlError::backend)?;
    for (request_id, req) in &tx.github_token_requests {
        conn.execute(
            "INSERT INTO github_token_requests (request_id, request_blob) VALUES (?1,?2)",
            params![request_id, unblob(req)?],
        )
        .map_err(ControlError::backend)?;
    }
    // Grants + OIDC.
    conn.execute("DELETE FROM id_token_grants", [])
        .map_err(ControlError::backend)?;
    for ((run_id, job_id), granted) in &tx.id_token_grants {
        conn.execute(
            "INSERT INTO id_token_grants (run_id, job_id, granted) VALUES (?1,?2,?3)",
            params![run_id.0.to_string(), job_id.0, *granted as i64],
        )
        .map_err(ControlError::backend)?;
    }
    conn.execute("DELETE FROM oidc_job_contexts", [])
        .map_err(ControlError::backend)?;
    for ((run_id, job_id), ctx) in &tx.oidc_job_contexts {
        conn.execute(
            "INSERT INTO oidc_job_contexts (run_id, job_id, context_blob) VALUES (?1,?2,?3)",
            params![run_id.0.to_string(), job_id.0, unblob(ctx)?],
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
            params![agent_job_id.to_string(), unblob(steps)?, revision as i64],
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

    // Sessions: rebuild from the unified maps.
    conn.execute("DELETE FROM runner_sessions", [])
        .map_err(ControlError::backend)?;
    let write_session = |conn: &Connection,
                         session_id: &str,
                         runner_id: i64,
                         protocol: SessionProtocol,
                         tx: &TxState|
     -> Result<(), ControlError> {
        let encryption = tx.session_keys.get(session_id).map(|e| e.key.clone());
        let active_request_id = tx.session_active_requests.get(session_id).copied();
        let last_seen_us = tx
            .session_last_seen
            .get(session_id)
            .map(|t| system_to_us(*t));
        conn.execute(
            "INSERT INTO runner_sessions (session_id, runner_id, protocol, encryption_blob, \
             active_request_id, last_seen_at_us, created_at_us) VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![
                session_id,
                runner_id,
                protocol.as_str(),
                encryption,
                active_request_id,
                last_seen_us,
                now_us
            ],
        )
        .map_err(ControlError::backend)?;
        Ok(())
    };
    for (session_id, runner_id) in &tx.broker_session_runners {
        write_session(conn, session_id, *runner_id, SessionProtocol::Broker, tx)?;
    }
    for (session_id, session) in &tx.sessions {
        write_session(
            conn,
            session_id,
            session.runner_id,
            SessionProtocol::Azdo,
            tx,
        )?;
    }

    // Inflight messages.
    conn.execute("DELETE FROM broker_messages", [])
        .map_err(ControlError::backend)?;
    for (session_id, messages) in &tx.inflight_messages {
        for (message_id, msg) in messages {
            conn.execute(
                "INSERT INTO broker_messages (session_id, message_id, message_blob, \
                 created_at_us) VALUES (?1,?2,?3,?4)",
                params![session_id, message_id, unblob(msg)?, now_us],
            )
            .map_err(ControlError::backend)?;
        }
    }

    // Concurrency.
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
                group.running.as_ref().map(unblob).transpose()?,
                unblob(&group.pending)?,
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
                unblob(&admission.gates)?,
                unblob(&admission.acquired_keys)?,
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
    conn.execute("DELETE FROM run_concurrency", [])
        .map_err(ControlError::backend)?;
    for (run_id, c) in &tx.run_concurrency {
        conn.execute(
            "INSERT INTO run_concurrency (run_id, concurrency_blob) VALUES (?1,?2)",
            params![run_id.0.to_string(), unblob(c)?],
        )
        .map_err(ControlError::backend)?;
    }

    // Assignments, pool pending, cancellations.
    conn.execute("DELETE FROM job_assignments", [])
        .map_err(ControlError::backend)?;
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
    conn.execute("DELETE FROM pool_pending", [])
        .map_err(ControlError::backend)?;
    for ((run_id, job_id), at) in &tx.pool_pending {
        conn.execute(
            "INSERT INTO pool_pending (run_id, job_id, at_us) VALUES (?1,?2,?3)",
            params![run_id.0.to_string(), job_id.0, system_to_us(*at)],
        )
        .map_err(ControlError::backend)?;
    }
    conn.execute("DELETE FROM cancellation_queue", [])
        .map_err(ControlError::backend)?;
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

    // Counters.
    for (name, value) in [
        ("next_message_id", tx.next_message_id),
        ("next_runner_id", tx.next_runner_id),
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
        self.transact(|tx| commands::submit_run_tx(tx, submit))
    }

    async fn poll_session(&self, poll: PollRequest) -> Result<PollOutcome, ControlError> {
        self.transact(|tx| commands::poll_session_tx(tx, poll))
    }

    async fn acquire_context(&self, request_id: i64) -> Result<AcquireContext, ControlError> {
        self.transact(|tx| {
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
        self.transact(|tx| commands::complete_job_tx(tx, completion))
    }

    async fn cancel_run(
        &self,
        run_id: RunId,
        reason: Option<String>,
    ) -> Result<CancelOutcome, ControlError> {
        self.transact(|tx| {
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
        self.transact(|tx| {
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
        self.transact(|tx| {
            commands::purge_runner_tx(tx, runner_id);
            Ok(())
        })
    }

    async fn claim_expansion(&self) -> Result<Option<ExpansionClaim>, ControlError> {
        self.transact(|tx| {
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
        self.transact(|tx| {
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

    async fn sweep_expired(
        &self,
        now: std::time::SystemTime,
    ) -> Result<SweepOutcome, ControlError> {
        self.transact(|tx| {
            let bindings = sched::sweep_stale_bindings(tx, now);
            Ok(SweepOutcome {
                bindings_swept: bindings,
                ..Default::default()
            })
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
                if let Some(job) = tx.claimed_jobs.remove(&key) {
                    sched::clear_assignment(tx, key.0, &key.1);
                    tx.push_ready(job);
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

    async fn queue_stats(&self) -> Result<QueueStats, ControlError> {
        self.read(|tx| {
            Ok(QueueStats {
                ready: tx.ready_index.len() + tx.queue.len(),
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
