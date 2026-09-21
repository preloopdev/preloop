//! The Postgres `ControlBackend`: the shared-node authority.
//!
//! Same contract as [`crate::control::sqlite`]: every command is one
//! transaction — `BEGIN`, [`load_txstate`] builds the working set, the
//! shared [`crate::control::commands`] logic runs on it, [`write_txstate`]
//! persists the delta, `COMMIT`. The database is the serialization and
//! fencing boundary; a pool of connections serves concurrent commands, so
//! this backend scales past SQLite's single writer.
//!
//! The SQL is the Postgres dialect of [`crate::control::schema::POSTGRES_DDL`]
//! — the same table families, `$N` placeholders, `BYTEA` blobs, `BIGSERIAL`
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
use super::txstate::TxState;
use super::types::*;
use crate::concurrency;
use crate::models::{QueuedJob, RunRecord, TaskAgentJobRequestRecord};
use crate::state::JobSetId;
use crate::store;
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
    pool_assignments_enabled: bool,
    require_job_assignments: bool,
    runner_liveness_timeout: std::time::Duration,
}

impl PostgresBackend {
    /// Connect, migrate to the current schema version, and return the
    /// backend. `url` is a `postgres://` connection string; TLS is applied
    /// when the URL's `sslmode` asks for it (see `store_pg::tls_connector`).
    pub(crate) async fn connect(
        url: &str,
        pool_assignments_enabled: bool,
        require_job_assignments: bool,
        runner_liveness_timeout: std::time::Duration,
    ) -> Result<Self, ControlError> {
        let client = connect_one(url).await?;
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
        Ok(Self {
            client: tokio::sync::Mutex::new(client),
            readers_tx,
            readers_rx: tokio::sync::Mutex::new(readers_rx),
            pool_assignments_enabled,
            require_job_assignments,
            runner_liveness_timeout,
        })
    }

    /// Run one command as a transaction: `BEGIN`, load the working set,
    /// run `f`, write the delta back, `COMMIT`. A failure before commit
    /// rolls the whole command back.
    pub(crate) async fn transact<T>(
        &self,
        f: impl FnOnce(&mut TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        let mut client = self.client.lock().await;
        let txn = client.transaction().await.map_err(ControlError::backend)?;
        let mut tx = load_txstate(&txn).await?.with_config(
            self.pool_assignments_enabled,
            self.require_job_assignments,
            self.runner_liveness_timeout,
        );
        let result = f(&mut tx)?;
        write_txstate(&txn, &tx).await?;
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
        let mut client = self
            .readers_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| ControlError::backend(anyhow::anyhow!("reader pool closed")))?;
        let result = self.read_on(&mut client, f).await;
        let _ = self.readers_tx.send(client).await;
        result
    }

    /// The read body: one consistent snapshot on `client`, rolled back.
    async fn read_on<T>(
        &self,
        client: &mut Client,
        f: impl FnOnce(&TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        let txn = client.transaction().await.map_err(ControlError::backend)?;
        let tx = load_txstate(&txn).await?.with_config(
            self.pool_assignments_enabled,
            self.require_job_assignments,
            self.runner_liveness_timeout,
        );
        let result = f(&tx)?;
        txn.rollback().await.map_err(ControlError::backend)?;
        Ok(result)
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

type Tx<'a> = tokio_postgres::Transaction<'a>;

async fn load_txstate(conn: &Tx<'_>) -> Result<TxState, ControlError> {
    let mut tx = TxState::default();

    // Runs.
    for row in conn
        .query("SELECT run_id, record_blob FROM runs", &[])
        .await
        .map_err(ControlError::backend)?
    {
        let run_id_s: String = row.get(0);
        let record: Vec<u8> = row.get(1);
        let run_id = parse_run_id(&run_id_s);
        let value: serde_json::Value = blob(&record)?;
        let mut run = store::run_record_from_value(value).map_err(ControlError::backend)?;
        run.jobs.clear();
        tx.runs.insert(run_id, run);
        tx.loaded.runs.insert(run_id);
    }

    // Jobs.
    for row in conn
        .query(
            "SELECT run_id, job_id, status, queue_kind, queue_position, seq, \
             reaper_first_seen_us, expand_generation, payload_blob \
             FROM jobs ORDER BY seq",
            &[],
        )
        .await
        .map_err(ControlError::backend)?
    {
        let run_id_s: String = row.get(0);
        let job_id_s: String = row.get(1);
        let status: String = row.get(2);
        let kind: String = row.get(3);
        let reaper_us: Option<i64> = row.get(6);
        let expand_generation: i64 = row.get(7);
        let payload: Option<Vec<u8>> = row.get(8);
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
    tx.ready_count = tx.ready_index.len() as i64;
    tx.next_queue_position = conn
        .query_one(
            "SELECT COALESCE(MAX(queue_position),0)+1 FROM jobs WHERE queue_kind='ready'",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(1);

    // Requests.
    for row in conn
        .query(
            "SELECT request_id, run_id, job_id, agent_job_id, plan_id, plan_type, \
             timeline_id, result, locked_until, claimed_at_us, owner_runner_id, \
             started_at_us, last_renewed_at_us, timeout_triggered, debug_token_issued, \
             request_blob FROM job_requests",
            &[],
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

    // Token requests, grants, OIDC, steps.
    for row in conn
        .query(
            "SELECT request_id, request_blob FROM github_token_requests",
            &[],
        )
        .await
        .map_err(ControlError::backend)?
    {
        let request_id: i64 = row.get(0);
        let req: Vec<u8> = row.get(1);
        if let Ok(req) = serde_json::from_slice(&req) {
            tx.github_token_requests.insert(request_id, req);
        }
    }
    for row in conn
        .query("SELECT run_id, job_id, granted FROM id_token_grants", &[])
        .await
        .map_err(ControlError::backend)?
    {
        let run_id_s: String = row.get(0);
        let job_id_s: String = row.get(1);
        let granted: i64 = row.get(2);
        tx.id_token_grants
            .insert((parse_run_id(&run_id_s), JobId(job_id_s)), granted != 0);
    }
    for row in conn
        .query(
            "SELECT run_id, job_id, context_blob FROM oidc_job_contexts",
            &[],
        )
        .await
        .map_err(ControlError::backend)?
    {
        let run_id_s: String = row.get(0);
        let job_id_s: String = row.get(1);
        let ctx: Vec<u8> = row.get(2);
        if let Ok(ctx) = serde_json::from_slice(&ctx) {
            tx.oidc_job_contexts
                .insert((parse_run_id(&run_id_s), JobId(job_id_s)), ctx);
        }
    }
    for row in conn
        .query(
            "SELECT agent_job_id, steps_blob, revision FROM job_steps",
            &[],
        )
        .await
        .map_err(ControlError::backend)?
    {
        let agent_job_id_s: String = row.get(0);
        let steps: Vec<u8> = row.get(1);
        let revision: i64 = row.get(2);
        let agent_job_id = parse_uuid(&agent_job_id_s);
        if let Ok(steps) = serde_json::from_slice(&steps) {
            tx.job_steps.insert(agent_job_id, steps);
            tx.job_steps_revision.insert(agent_job_id, revision as u64);
        }
        tx.loaded.step_attempts.insert(agent_job_id);
    }

    // Runners.
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
             active_request_id, last_seen_at_us FROM runner_sessions",
            &[],
        )
        .await
        .map_err(ControlError::backend)?
    {
        let session_id: String = row.get(0);
        let runner_id: i64 = row.get(1);
        let protocol: String = row.get(2);
        let encryption_blob: Option<Vec<u8>> = row.get(3);
        let active_request_id: Option<i64> = row.get(4);
        let last_seen_at_us: Option<i64> = row.get(5);
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
        let message_id: i64 = row.get(1);
        let msg: Vec<u8> = row.get(2);
        if let Ok(msg) = serde_json::from_slice(&msg) {
            tx.inflight_messages
                .entry(session_id)
                .or_default()
                .insert(message_id, msg);
        }
    }

    // Concurrency.
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
    for row in conn
        .query("SELECT run_id, concurrency_blob FROM run_concurrency", &[])
        .await
        .map_err(ControlError::backend)?
    {
        let run_id_s: String = row.get(0);
        let c: Vec<u8> = row.get(1);
        if let Ok(c) = serde_json::from_slice(&c) {
            tx.run_concurrency.insert(parse_run_id(&run_id_s), c);
        }
    }

    // Assignments, pool pending, cancellations.
    for row in conn
        .query(
            "SELECT run_id, job_id, runner_id, at_us, first_at_us FROM job_assignments",
            &[],
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
    for row in conn
        .query("SELECT run_id, job_id, at_us FROM pool_pending", &[])
        .await
        .map_err(ControlError::backend)?
    {
        let run_id_s: String = row.get(0);
        let job_id_s: String = row.get(1);
        let at_us: i64 = row.get(2);
        tx.pool_pending.insert(
            (parse_run_id(&run_id_s), JobId(job_id_s)),
            us_to_system(at_us),
        );
    }
    for row in conn
        .query(
            "SELECT run_id, job_id, agent_job_id FROM cancellation_queue ORDER BY seq",
            &[],
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

    // Counters.
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
    for row in conn
        .query("SELECT key, value FROM workflow_run_counters", &[])
        .await
        .map_err(ControlError::backend)?
    {
        let key: String = row.get(0);
        let value: i64 = row.get(1);
        tx.workflow_run_counters.insert(key, value as u64);
    }

    Ok(tx)
}

// ─────────────────────────────────────────────────────────────────────────
// Write-back: TxState delta → rows
// ─────────────────────────────────────────────────────────────────────────

async fn write_txstate(conn: &Tx<'_>, tx: &TxState) -> Result<(), ControlError> {
    let now_us = system_to_us(std::time::SystemTime::now());

    // Runs.
    for (run_id, run) in &tx.runs {
        let mut record = run.clone();
        record.jobs.clear();
        let value = store::run_record_value(&record).map_err(ControlError::backend)?;
        let record_blob = serde_json::to_vec(&value).map_err(ControlError::backend)?;
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
    let mut seq = 0i64;
    let mut ready_position = 0i64;

    async fn write_job(
        conn: &Tx<'_>,
        tx: &TxState,
        seq: i64,
        run_id: RunId,
        job_id: &JobId,
        kind: QueueKind,
        position: Option<i64>,
        job: Option<&QueuedJob>,
    ) -> Result<(), ControlError> {
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
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) \
             ON CONFLICT(run_id,job_id) DO UPDATE SET status=excluded.status, \
             queue_kind=excluded.queue_kind, queue_position=excluded.queue_position, \
             seq=excluded.seq, reaper_first_seen_us=excluded.reaper_first_seen_us, \
             claimed_by=excluded.claimed_by, claimed_at_us=excluded.claimed_at_us, \
             expand_generation=excluded.expand_generation, \
             payload_blob=excluded.payload_blob",
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

    for job in tx.ready_index.iter().chain(tx.queue.iter()) {
        seq += 1;
        ready_position += 1;
        write_job(
            conn,
            tx,
            seq,
            job.run_id,
            &job.job_id,
            QueueKind::Ready,
            Some(ready_position),
            Some(job),
        )
        .await?;
        seen_jobs.insert((job.run_id, job.job_id.clone()));
    }
    for job in &tx.pending_jobs {
        seq += 1;
        write_job(
            conn,
            tx,
            seq,
            job.run_id,
            &job.job_id,
            QueueKind::Pending,
            None,
            Some(job),
        )
        .await?;
        seen_jobs.insert((job.run_id, job.job_id.clone()));
    }
    for job in &tx.concurrency_blocked {
        seq += 1;
        write_job(
            conn,
            tx,
            seq,
            job.run_id,
            &job.job_id,
            QueueKind::Blocked,
            None,
            Some(job),
        )
        .await?;
        seen_jobs.insert((job.run_id, job.job_id.clone()));
    }
    for job in &tx.pending_expansions {
        seq += 1;
        write_job(
            conn,
            tx,
            seq,
            job.run_id,
            &job.job_id,
            QueueKind::Expand,
            None,
            Some(job),
        )
        .await?;
        seen_jobs.insert((job.run_id, job.job_id.clone()));
    }
    for (run_id, job_id) in &tx.expanding {
        if seen_jobs.contains(&(*run_id, job_id.clone())) {
            continue;
        }
        seq += 1;
        write_job(
            conn,
            tx,
            seq,
            *run_id,
            job_id,
            QueueKind::Expand,
            None,
            tx.expanding_jobs.get(&(*run_id, job_id.clone())),
        )
        .await?;
        seen_jobs.insert((*run_id, job_id.clone()));
    }
    for ((run_id, job_id), job) in &tx.claimed_jobs {
        seq += 1;
        write_job(
            conn,
            tx,
            seq,
            *run_id,
            job_id,
            QueueKind::Claimed,
            None,
            Some(job),
        )
        .await?;
        seen_jobs.insert((*run_id, job_id.clone()));
    }
    for (run_id, jobs) in &tx.held_runs {
        for job in jobs {
            seq += 1;
            write_job(
                conn,
                tx,
                seq,
                *run_id,
                &job.job_id,
                QueueKind::Held,
                None,
                Some(job),
            )
            .await?;
            seen_jobs.insert((*run_id, job.job_id.clone()));
        }
    }
    for (run_id, run) in &tx.runs {
        for job_id in run.jobs.keys() {
            if !seen_jobs.contains(&(*run_id, job_id.clone())) {
                seq += 1;
                write_job(conn, tx, seq, *run_id, job_id, QueueKind::None, None, None).await?;
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
        let request_blob = tx.broker_messages.get(request_id).map(unblob).transpose()?;
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
    conn.execute("DELETE FROM github_token_requests", &[])
        .await
        .map_err(ControlError::backend)?;
    for (request_id, req) in &tx.github_token_requests {
        conn.execute(
            "INSERT INTO github_token_requests (request_id, request_blob) VALUES ($1,$2)",
            &[request_id, &unblob(req)?],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    conn.execute("DELETE FROM id_token_grants", &[])
        .await
        .map_err(ControlError::backend)?;
    for ((run_id, job_id), granted) in &tx.id_token_grants {
        conn.execute(
            "INSERT INTO id_token_grants (run_id, job_id, granted) VALUES ($1,$2,$3)",
            &[&run_id.0.to_string(), &job_id.0, &(*granted as i64)],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    conn.execute("DELETE FROM oidc_job_contexts", &[])
        .await
        .map_err(ControlError::backend)?;
    for ((run_id, job_id), ctx) in &tx.oidc_job_contexts {
        conn.execute(
            "INSERT INTO oidc_job_contexts (run_id, job_id, context_blob) VALUES ($1,$2,$3)",
            &[&run_id.0.to_string(), &job_id.0, &unblob(ctx)?],
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
                &unblob(steps)?,
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

    // Sessions.
    conn.execute("DELETE FROM runner_sessions", &[])
        .await
        .map_err(ControlError::backend)?;
    async fn write_session(
        conn: &Tx<'_>,
        session_id: &str,
        runner_id: i64,
        protocol: SessionProtocol,
        tx: &TxState,
        now_us: i64,
    ) -> Result<(), ControlError> {
        let encryption = tx.session_keys.get(session_id).map(|e| e.key.clone());
        let active_request_id = tx.session_active_requests.get(session_id).copied();
        let last_seen_us = tx
            .session_last_seen
            .get(session_id)
            .map(|t| system_to_us(*t));
        conn.execute(
            "INSERT INTO runner_sessions (session_id, runner_id, protocol, encryption_blob, \
             active_request_id, last_seen_at_us, created_at_us) VALUES ($1,$2,$3,$4,$5,$6,$7)",
            &[
                &session_id,
                &runner_id,
                &protocol.as_str(),
                &encryption,
                &active_request_id,
                &last_seen_us,
                &now_us,
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
            *runner_id,
            SessionProtocol::Broker,
            tx,
            now_us,
        )
        .await?;
    }
    for (session_id, session) in &tx.sessions {
        write_session(
            conn,
            session_id,
            session.runner_id,
            SessionProtocol::Azdo,
            tx,
            now_us,
        )
        .await?;
    }

    // Inflight messages.
    conn.execute("DELETE FROM broker_messages", &[])
        .await
        .map_err(ControlError::backend)?;
    for (session_id, messages) in &tx.inflight_messages {
        for (message_id, msg) in messages {
            conn.execute(
                "INSERT INTO broker_messages (session_id, message_id, message_blob, \
                 created_at_us) VALUES ($1,$2,$3,$4)",
                &[session_id, message_id, &unblob(msg)?, &now_us],
            )
            .await
            .map_err(ControlError::backend)?;
        }
    }

    // Concurrency.
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
                &group.running.as_ref().map(unblob).transpose()?,
                &unblob(&group.pending)?,
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
                &unblob(&admission.gates)?,
                &unblob(&admission.acquired_keys)?,
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
    conn.execute("DELETE FROM run_concurrency", &[])
        .await
        .map_err(ControlError::backend)?;
    for (run_id, c) in &tx.run_concurrency {
        conn.execute(
            "INSERT INTO run_concurrency (run_id, concurrency_blob) VALUES ($1,$2)",
            &[&run_id.0.to_string(), &unblob(c)?],
        )
        .await
        .map_err(ControlError::backend)?;
    }

    // Assignments, pool pending, cancellations.
    conn.execute("DELETE FROM job_assignments", &[])
        .await
        .map_err(ControlError::backend)?;
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
    conn.execute("DELETE FROM pool_pending", &[])
        .await
        .map_err(ControlError::backend)?;
    for ((run_id, job_id), at) in &tx.pool_pending {
        conn.execute(
            "INSERT INTO pool_pending (run_id, job_id, at_us) VALUES ($1,$2,$3)",
            &[&run_id.0.to_string(), &job_id.0, &system_to_us(*at)],
        )
        .await
        .map_err(ControlError::backend)?;
    }
    conn.execute("DELETE FROM cancellation_queue", &[])
        .await
        .map_err(ControlError::backend)?;
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

    // Counters.
    for (name, value) in [
        ("next_message_id", tx.next_message_id),
        ("next_runner_id", tx.next_runner_id),
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
        self.transact(|tx| commands::submit_run_tx(tx, submit))
            .await
    }

    async fn poll_session(&self, poll: PollRequest) -> Result<PollOutcome, ControlError> {
        self.transact(|tx| commands::poll_session_tx(tx, poll))
            .await
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
        .await
    }

    async fn complete_job(
        &self,
        completion: JobCompletionInput,
    ) -> Result<CompleteOutcome, ControlError> {
        self.transact(|tx| commands::complete_job_tx(tx, completion))
            .await
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
        .await
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
        self.transact(|tx| {
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
                if let Some(job) = tx.claimed_jobs.remove(&key) {
                    sched::clear_assignment(tx, key.0, &key.1);
                    tx.push_ready(job);
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
