//! Postgres-backed [`Store`] implementation.
//!
//! Selected with `PRELOOP_STORE_URL=postgres://<user>:<pass>@<host>:<port>/<db>`.
//! Mirrors the SQLite backend table-for-table (see [`MIGRATIONS`]); the
//! relational model, sealed-blob payloads, and migration steps are the same,
//! only the dialect differs (`$n` params, `EXCLUDED` upserts, native types).
//!
//! Concurrency model: one `tokio-postgres` client behind a tokio mutex, the
//! same single-writer shape as SQLite. The in-memory state remains the source
//! of truth; this is a restart store, not a shared bus.
//!
//! TLS: append `?sslmode=require` (or `verify-ca` / `verify-full`) to the URL
//! to connect with rustls over the system root store; the default and
//! `sslmode=disable` stay plaintext for loopback databases.

use super::*;
use async_trait::async_trait;
use postgres_rustls::MakeTlsConnector;
use preloop_gha_protocol::SessionId;
use std::future::Future;
use tokio_postgres::{connect, Client, NoTls};

/// Advisory-lock key guarding schema migration. Any stable constant works; it
/// only has to agree across every aksh process pointed at one database.
const MIGRATION_LOCK_KEY: i64 = 0x616b_7368_5f6d_6772; // "preloop_mgr"

/// Postgres backend: one client behind a mutex.
pub(crate) struct PgStore {
    connection: Arc<tokio::sync::Mutex<Client>>,
    cipher: Envelope,
}

/// Build a rustls connector when the URL asks for TLS (`sslmode=require`,
/// `verify-ca`, or `verify-full`); `None` for `sslmode=disable` or when no
/// `sslmode` is present. Chain + hostname verification always use the system
/// root store, so `verify-full` semantics apply to every TLS mode.
pub(crate) fn tls_connector(url: &str) -> anyhow::Result<Option<MakeTlsConnector>> {
    let query = url.split('?').nth(1).unwrap_or("");
    let sslmode = query
        .split('&')
        .find_map(|part| part.strip_prefix("sslmode="));
    match sslmode {
        Some("require" | "verify-ca" | "verify-full") => {
            // rustls 0.23 needs a process-level crypto provider. The server
            // binaries install one at startup; installing here makes the
            // library self-sufficient (tests, embedded use) without breaking
            // an already-installed provider.
            rustls::crypto::ring::default_provider()
                .install_default()
                .ok();
            let mut roots = rustls::RootCertStore::empty();
            for cert in rustls_native_certs::load_native_certs().certs {
                roots
                    .add(cert)
                    .map_err(|error| anyhow::anyhow!("adding native root cert: {error}"))?;
            }
            let mut config = rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            postgres_rustls::set_postgresql_alpn(&mut config);
            Ok(Some(MakeTlsConnector::new(
                tokio_rustls::TlsConnector::from(Arc::new(config)),
            )))
        }
        _ => Ok(None),
    }
}

/// URL to hand to `tokio-postgres::connect`. `sslmode` values `verify-ca` /
/// `verify-full` are libpq extensions that tokio-postgres rejects; the
/// rustls connector always performs full chain + hostname verification, so
/// mapping them onto `require` (TLS stays mandatory) loses nothing.
pub(crate) fn connect_url(url: &str) -> String {
    let (before, after) = match url.split_once("sslmode=verify-") {
        Some((before, after)) => (before, after),
        None => return url.to_owned(),
    };
    let (mode, rest) = match after.split_once('&') {
        Some((mode, rest)) => (mode, rest),
        None => (after, ""),
    };
    if !matches!(mode, "ca" | "full") {
        return url.to_owned();
    }
    let rest = if rest.is_empty() {
        ""
    } else {
        &format!("&{rest}")
    };
    format!("{before}sslmode=require{rest}")
}

/// Drive the tokio-postgres connection task until it ends (normally only on
/// shutdown or a fatal protocol error), logging any failure.
fn spawn_connection_task(
    connection: impl Future<Output = Result<(), tokio_postgres::Error>> + Send + 'static,
) {
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::error!(%error, "postgres connection task failed");
        }
    });
}

impl PgStore {
    /// Connect, apply pending migrations, and return the store.
    pub(crate) async fn open(url: &str, cipher: Envelope) -> anyhow::Result<Self> {
        let connect_url = connect_url(url);
        let mut client = match tls_connector(url)? {
            Some(tls) => {
                let (client, connection) = connect(&connect_url, tls).await?;
                spawn_connection_task(connection);
                client
            }
            None => {
                let (client, connection) = connect(&connect_url, NoTls).await?;
                spawn_connection_task(connection);
                client
            }
        };
        Self::migrate(&mut client).await?;
        Ok(Self {
            connection: Arc::new(tokio::sync::Mutex::new(client)),
            cipher,
        })
    }

    /// Apply pending migrations. `schema_migrations` doubles as the version
    /// pointer (SQLite uses `PRAGMA user_version`); steps are append-only and
    /// each runs in its own transaction.
    ///
    /// The whole sequence runs under a session-level advisory lock. Postgres
    /// `CREATE TABLE IF NOT EXISTS` is *not* race-safe: the existence check and
    /// the `pg_type` insert are separate, so two servers booting against one
    /// database collide with
    /// `duplicate key value violates unique constraint "pg_type_typname_nsp_index"`
    /// and the loser fails to start. The lock also serialises the
    /// read-max-version / insert-version pair below.
    async fn migrate(client: &mut Client) -> anyhow::Result<()> {
        // Arbitrary but stable key: "aksh-store" as a 64-bit constant. Session
        // scoped (not `_xact_`) so it spans the per-step transactions.
        client
            .execute("SELECT pg_advisory_lock($1)", &[&MIGRATION_LOCK_KEY])
            .await?;
        let result = Self::migrate_locked(client).await;
        // Release even when a step failed, otherwise a crashed migration wedges
        // every future boot until the connection is reaped.
        client
            .execute("SELECT pg_advisory_unlock($1)", &[&MIGRATION_LOCK_KEY])
            .await?;
        result
    }

    async fn migrate_locked(client: &mut Client) -> anyhow::Result<()> {
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations (
                   version BIGINT PRIMARY KEY,
                   name TEXT NOT NULL UNIQUE,
                   applied_at_us BIGINT NOT NULL
                 )",
            )
            .await?;
        let current: i64 = client
            .query_one(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
                &[],
            )
            .await?
            .get(0);
        for (version, name, sql) in MIGRATIONS {
            if *version as i64 <= current {
                continue;
            }
            let tx = client.transaction().await?;
            tx.batch_execute(sql).await?;
            tx.execute(
                "INSERT INTO schema_migrations(version, name, applied_at_us) VALUES ($1, $2, $3)",
                &[&(*version as i64), name, &now_us()],
            )
            .await?;
            tx.commit().await?;
        }
        Ok(())
    }

    async fn store_run_tx(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
        run: &RunRecord,
    ) -> anyhow::Result<()> {
        let value = run_record_value(run)?;
        let sealed = self.cipher.seal(&serde_json::to_vec(&value)?)?;
        tx.execute(
            "INSERT INTO runs(run_id, repository, workflow_path, status, run_number,
                              run_attempt, created_at_us, completed_at_us, record_blob)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT(run_id) DO UPDATE SET
               repository = EXCLUDED.repository,
               workflow_path = EXCLUDED.workflow_path,
               status = EXCLUDED.status,
               run_number = EXCLUDED.run_number,
               run_attempt = EXCLUDED.run_attempt,
               created_at_us = EXCLUDED.created_at_us,
               completed_at_us = EXCLUDED.completed_at_us,
               record_blob = EXCLUDED.record_blob",
            &[
                &run.run_id.to_string(),
                &run.submission.repository.clone(),
                &run.workflow_path_str.clone(),
                &status_string(run.status),
                &(run.run_number as i64),
                &(run.run_attempt as i64),
                &unix_us(run.created_at),
                &run.completed_at.map(unix_us),
                &sealed,
            ],
        )
        .await?;
        Ok(())
    }

    async fn insert_request_tx(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
        record: &TaskAgentJobRequestRecord,
    ) -> anyhow::Result<()> {
        let snapshot = request_snapshot(record);
        let blob = self.cipher.seal(&serde_json::to_vec(&snapshot)?)?;
        tx.execute(
            "INSERT INTO job_requests(request_id, run_id, job_id, agent_job_id,
                                      plan_id, timeline_id, state, request_blob)
             VALUES ($1, $2, $3, $4, $5, $6, 'active', $7)",
            &[
                &record.request_id,
                &record.run_id.to_string(),
                &record.job_id.0,
                &record.agent_job_id.to_string(),
                &record.plan_id,
                &record.timeline_id.to_string(),
                &blob,
            ],
        )
        .await?;
        Ok(())
    }

    async fn write_meta_tx(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
        meta: &MetaSnapshot,
    ) -> anyhow::Result<()> {
        let blob = self.cipher.seal(&serde_json::to_vec(&meta)?)?;
        tx.execute(
            "INSERT INTO runtime_snapshots(snapshot_id, format_version, meta_blob, written_at_us)
             VALUES (1, $1, $2, $3)
             ON CONFLICT(snapshot_id) DO UPDATE SET
               format_version = EXCLUDED.format_version,
               meta_blob = EXCLUDED.meta_blob,
               written_at_us = EXCLUDED.written_at_us",
            &[&(SNAPSHOT_FORMAT as i64), &blob, &now_us()],
        )
        .await?;
        Ok(())
    }

    /// Upsert one attempt's step rows (see the SQLite twin: keyed upsert, no
    /// per-run delete, so a step transition writes only the changed rows).
    async fn write_job_steps_tx(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
        run_id: RunId,
        agent_job_id: uuid::Uuid,
        records: &[crate::models::StepRecord],
    ) -> anyhow::Result<()> {
        for step in records {
            if step.id.is_empty() {
                continue;
            }
            let kind = match step.kind {
                crate::models::StepKind::Workflow => "workflow",
                crate::models::StepKind::Synthetic => "synthetic",
            };
            tx.execute(
                "INSERT INTO job_steps(run_id, agent_job_id, step_id, kind, workflow_index,
                                       runner_number, context_name, name_blob, conclusion,
                                       started_at_us, finished_at_us)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                 ON CONFLICT(agent_job_id, step_id) DO UPDATE SET
                   kind = EXCLUDED.kind,
                   workflow_index = EXCLUDED.workflow_index,
                   runner_number = EXCLUDED.runner_number,
                   context_name = EXCLUDED.context_name,
                   name_blob = EXCLUDED.name_blob,
                   conclusion = EXCLUDED.conclusion,
                   started_at_us = EXCLUDED.started_at_us,
                   finished_at_us = EXCLUDED.finished_at_us",
                &[
                    &run_id.to_string(),
                    &agent_job_id.to_string(),
                    &step.id,
                    &kind,
                    &step.workflow_index.map(|index| index as i64),
                    &step.runner_number.map(|number| number as i64),
                    &step.context_name,
                    &self.cipher.seal(step.name.as_bytes())?,
                    &step.conclusion,
                    &step.started_at.map(unix_us),
                    &step.finished_at.map(unix_us),
                ],
            )
            .await?;
        }
        Ok(())
    }

    /// Rewrite the claim/message tables from the captured projections (see the
    /// SQLite twin for the rationale).
    async fn write_claim_state_tx(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
        session_active_requests: &[(String, i64)],
        inflight: &[(String, i64, azdo::TaskAgentMessage)],
        broker_request_messages: &[(i64, azdo::AgentJobRequestMessage)],
    ) -> anyhow::Result<()> {
        tx.execute("DELETE FROM session_active_requests", &[])
            .await?;
        for (session_id, request_id) in session_active_requests {
            tx.execute(
                "INSERT INTO session_active_requests(session_id, active_request_id) VALUES ($1, $2)",
                &[&session_id, request_id],
            )
            .await?;
        }
        tx.execute("DELETE FROM broker_messages", &[]).await?;
        for (session_id, message_id, payload) in inflight {
            let payload_json = serde_json::to_string(payload)?;
            tx.execute(
                "INSERT INTO broker_messages(session_id, message_id, payload_json, written_at_us)
                 VALUES ($1, $2, $3, $4)",
                &[&session_id, message_id, &payload_json, &now_us()],
            )
            .await?;
        }
        tx.execute("DELETE FROM job_request_messages", &[]).await?;
        for (request_id, payload) in broker_request_messages {
            let payload_json = serde_json::to_string(payload)?;
            tx.execute(
                "INSERT INTO job_request_messages(request_id, payload_json, written_at_us)
                 VALUES ($1, $2, $3)",
                &[request_id, &payload_json, &now_us()],
            )
            .await?;
        }
        Ok(())
    }

    async fn insert_job(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
        job: &QueuedJob,
        queue_kind: &str,
        position: i64,
    ) -> anyhow::Result<()> {
        let payload = self.cipher.seal(&serde_json::to_vec(job)?)?;
        tx.execute(
            "INSERT INTO jobs(run_id, job_id, status, queue_kind, queue_position, payload_blob)
             VALUES ($1, $2, 'queued', $3, $4, $5)",
            &[
                &job.run_id.to_string(),
                &job.job_id.0,
                &queue_kind,
                &position,
                &payload,
            ],
        )
        .await?;
        for depends_on in &job.needs {
            tx.execute(
                "INSERT INTO job_dependencies(run_id, job_id, depends_on_job_id)
                 VALUES ($1, $2, $3)",
                &[&job.run_id.to_string(), &job.job_id.0, &depends_on.0],
            )
            .await?;
        }
        Ok(())
    }

    async fn insert_event_tx(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
        event: &NdjsonEvent,
    ) -> anyhow::Result<()> {
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
             VALUES ($1, $2, $3, $4, $5)",
            &[
                &run_id.to_string(),
                &job_id.map(|id| id.0),
                &event_type,
                &payload,
                &now_us(),
            ],
        )
        .await?;
        Ok(())
    }
}

#[async_trait]
impl Store for PgStore {
    async fn load_into(&self, inner: &mut InnerState) -> anyhow::Result<()> {
        let client = self.connection.lock().await;
        let rows = client
            .query("SELECT record_blob FROM runs ORDER BY created_at_us", &[])
            .await?;
        for row in rows {
            let blob: Vec<u8> = row.get(0);
            let run = restore_run_record(&self.cipher, &blob)?;
            let run_id = run.run_id;
            inner.runs.insert(run_id, run);
        }

        let rows = client
            .query(
                "SELECT run_id, job_id, queue_kind, queue_position, payload_blob
                 FROM jobs ORDER BY queue_kind, queue_position",
                &[],
            )
            .await?;
        for row in rows {
            let job_row = RowJob {
                run_id: row.get::<_, String>(0).parse()?,
                job_id: JobId(row.get(1)),
                queue_kind: row.get(2),
                queue_position: row.get(3),
                payload: row.get(4),
            };
            let job: QueuedJob = serde_json::from_slice(&self.cipher.unseal(&job_row.payload)?)?;
            match job_row.queue_kind.as_str() {
                "ready" => inner.queue.push_back(job),
                "pending" => inner.pending_jobs.push_back(job),
                "blocked" => inner.concurrency_blocked.push_back(job),
                "held" => inner.held_runs.entry(job_row.run_id).or_default().push(job),
                _ => unreachable!("schema constrains queue_kind"),
            }
        }

        let rows = client
            .query(
                "SELECT runner_id, name, ephemeral, runner_group_id, runner_group_name,
                        public_key, rsa_public_key
                 FROM runners WHERE deleted_at_us IS NULL",
                &[],
            )
            .await?;
        for row in rows {
            let runner = RegisteredRunner {
                id: row.get(0),
                name: row.get(1),
                ephemeral: row.get(2),
                runner_group_id: row.get(3),
                runner_group_name: row.get(4),
                public_key: row.get(5),
                labels: Vec::new(),
            };
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
        let rows = client
            .query(
                "SELECT runner_id, rsa_public_key FROM runners
                 WHERE deleted_at_us IS NULL AND rsa_public_key IS NOT NULL",
                &[],
            )
            .await?;
        for row in rows {
            let runner_id: i64 = row.get(0);
            let rsa_xml: String = row.get(1);
            if let Ok(parsed) = AgentRsaPublicKey::parse(&rsa_xml) {
                inner.runner_rsa_public_keys.insert(runner_id, parsed);
            }
        }
        let rows = client
            .query(
                "SELECT runner_id, label FROM runner_labels ORDER BY ordinal",
                &[],
            )
            .await?;
        for row in rows {
            let runner_id: i64 = row.get(0);
            let label: String = row.get(1);
            if let Some(runner) = inner.runners.get_mut(&runner_id) {
                runner.labels.push(label);
            }
        }
        // Restore session encryption keys (sealed). Without these, every
        // post-restart session is `encrypted:false` regardless of the runner's
        // RSA key — the AES session key is sent in plaintext to the runner.
        let rows = client
            .query(
                "SELECT session_id, session_key_blob, session_iv, session_tag
                 FROM runner_sessions
                 WHERE closed_at_us IS NULL AND session_key_blob IS NOT NULL",
                &[],
            )
            .await?;
        for row in rows {
            let session_id: String = row.get(0);
            let key_blob: Vec<u8> = row.get(1);
            let iv: Vec<u8> = row.get(2);
            let tag: Vec<u8> = row.get(3);
            match restore_session_key(&self.cipher, &key_blob, &iv, &tag) {
                Ok(enc) => {
                    inner.session_keys.insert(session_id.clone(), enc);
                }
                Err(error) => {
                    tracing::warn!(%session_id, %error, "failed to restore session_key on load");
                }
            }
        }
        let rows = client
            .query(
                "SELECT session_id, runner_id FROM runner_sessions WHERE closed_at_us IS NULL",
                &[],
            )
            .await?;
        for row in rows {
            let session = RunnerSession {
                session_id: SessionId(row.get::<_, String>(0).parse()?),
                runner_id: row.get(1),
            };
            inner
                .broker_session_runners
                .insert(session.session_id.0.to_string(), session.runner_id);
            inner
                .sessions
                .insert(session.session_id.0.to_string(), session);
        }
        // Restore `session_active_requests` so a restarted broker session
        // knows which request it had claimed but not acked.
        let rows = client
            .query(
                "SELECT session_id, active_request_id FROM session_active_requests",
                &[],
            )
            .await?;
        for row in rows {
            let session_id: String = row.get(0);
            let request_id: i64 = row.get(1);
            inner.session_active_requests.insert(session_id, request_id);
        }
        // Restore per-session broker message queues (dequeued but not yet
        // delivered to the runner) from the `broker_messages` table that
        // `store_inner` / `store_run_event` write. `inner.broker_messages`
        // (keyed by request_id) is a separate map restored from its own table
        // below.
        let rows = client
            .query(
                "SELECT session_id, message_id, payload_json FROM broker_messages
                 ORDER BY session_id, message_id",
                &[],
            )
            .await?;
        for row in rows {
            let session_id: String = row.get(0);
            let message_id: i64 = row.get(1);
            let payload_json: String = row.get(2);
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
        // Restore per-request job messages (request_id → message); the broker
        // re-delivers from `inner.broker_messages` after a restart.
        let rows = client
            .query(
                "SELECT request_id, payload_json FROM job_request_messages
                 ORDER BY request_id",
                &[],
            )
            .await?;
        for row in rows {
            let request_id: i64 = row.get(0);
            let payload_json: String = row.get(1);
            match serde_json::from_str::<azdo::AgentJobRequestMessage>(&payload_json) {
                Ok(message) => {
                    inner.broker_messages.insert(request_id, message);
                }
                Err(error) => {
                    tracing::warn!(request_id, %error, "dropping undecodable job request message");
                }
            }
        }
        // Step manifests, ordered so `--step` positions come back exactly as
        // recorded (see the SQLite twin).
        let rows = client
            .query(
                "SELECT agent_job_id, step_id, kind, workflow_index, runner_number,
                        context_name, name_blob, conclusion, started_at_us, finished_at_us
                 FROM job_steps
                 ORDER BY agent_job_id, kind, workflow_index, step_id",
                &[],
            )
            .await?;
        for row in rows {
            let agent_job_id: String = row.get(0);
            let Ok(agent_job_id) = agent_job_id.parse::<uuid::Uuid>() else {
                tracing::warn!(%agent_job_id, "dropping step row with an unparseable attempt id");
                continue;
            };
            let step_id: String = row.get(1);
            let kind: String = row.get(2);
            let workflow_index: Option<i64> = row.get(3);
            let runner_number: Option<i64> = row.get(4);
            let context_name: Option<String> = row.get(5);
            let name_blob: Vec<u8> = row.get(6);
            let conclusion: String = row.get(7);
            let started_at_us: Option<i64> = row.get(8);
            let finished_at_us: Option<i64> = row.get(9);
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
        let rows = client
            .query("SELECT request_id, request_blob FROM job_requests", &[])
            .await?;
        for row in rows {
            let request_id: i64 = row.get(0);
            let blob: Vec<u8> = row.get(1);
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

        if let Some(row) = client
            .query_opt(
                "SELECT meta_blob FROM runtime_snapshots WHERE snapshot_id = 1",
                &[],
            )
            .await?
        {
            let blob: Vec<u8> = row.get(0);
            let meta: MetaSnapshot = serde_json::from_slice(&self.cipher.unseal(&blob)?)?;
            apply_meta_snapshot(inner, meta);
        }
        let rows = client
            .query(
                "SELECT workflow_path, next_run_number
                 FROM workflow_run_counters
                 ORDER BY workflow_path",
                &[],
            )
            .await?;
        for row in rows {
            let workflow_path: String = row.get(0);
            let next_run_number: i64 = row.get(1);
            inner
                .workflow_run_counters
                .insert(workflow_path, next_run_number.saturating_sub(1) as u64);
        }
        // Log bytes live in their own table; rebuild the in-memory buffers
        // from ordered chunks. Failing to do this leaves the post-restart
        // server unable to serve GET /logs/<id> for in-flight jobs.
        let rows = client
            .query(
                "SELECT log_key, chunk_index, payload FROM log_chunks
                 ORDER BY log_key, chunk_index",
                &[],
            )
            .await?;
        for row in rows {
            let key: String = row.get(0);
            let payload: Vec<u8> = row.get(2);
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
        let rows = client
            .query("SELECT log_key, byte_count, line_count FROM log_files", &[])
            .await?;
        for row in rows {
            let key: String = row.get(0);
            let byte_count: i64 = row.get(1);
            let line_count: i64 = row.get(2);
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

    async fn store_inner(&self, snapshot: &StoreSnapshot) -> anyhow::Result<()> {
        let mut client = self.connection.lock().await;
        let tx = client.transaction().await?;
        for table in [
            "job_steps",
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
            tx.execute(&format!("DELETE FROM {table}"), &[]).await?;
        }
        for run in &snapshot.runs {
            self.store_run_tx(&tx, run).await?;
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
        for (kind, job, position) in &snapshot.jobs {
            self.insert_job(&tx, job, kind, *position).await?;
        }

        for runner in &snapshot.runners {
            // `ON CONFLICT` so re-registration (same `runner_id`, new name)
            // overwrites the row in place. Without it, `UNIQUE(name)` would
            // reject the insert whenever the in-memory map had two runners
            // that share a name — which the official `actions/runner` does
            // on every re-register / replace flow.
            //
            // SQLite's `INSERT OR REPLACE` resolves *any* unique conflict
            // (delete + insert), so two in-memory runners sharing a name
            // converge to the last one written. Postgres `ON CONFLICT` only
            // covers one constraint, so mirror the delete side explicitly:
            // drop any same-named row with a different id first (the
            // `runner_labels` cascade cleans up its labels, as SQLite's
            // replace does).
            tx.execute(
                "DELETE FROM runners WHERE name = $1 AND runner_id <> $2",
                &[&runner.name, &runner.id],
            )
            .await?;
            // Typed RSA public key is stored alongside the XML form so a
            // post-restart session can be FIPS-encrypted without re-parsing
            // and without an extra table.
            let rsa_xml = rsa_keys.get(&runner.id).map(|key| key.to_xml_string());
            tx.execute(
                "INSERT INTO runners(runner_id, name, ephemeral,
                                     runner_group_id, runner_group_name,
                                     public_key, rsa_public_key,
                                     created_at_us, updated_at_us)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
                 ON CONFLICT(runner_id) DO UPDATE SET
                   name = EXCLUDED.name,
                   ephemeral = EXCLUDED.ephemeral,
                   runner_group_id = EXCLUDED.runner_group_id,
                   runner_group_name = EXCLUDED.runner_group_name,
                   public_key = EXCLUDED.public_key,
                   rsa_public_key = EXCLUDED.rsa_public_key,
                   updated_at_us = EXCLUDED.updated_at_us",
                &[
                    &runner.id,
                    &runner.name.clone(),
                    &runner.ephemeral,
                    &runner.runner_group_id,
                    &runner.runner_group_name.clone(),
                    &runner.public_key.clone(),
                    &rsa_xml,
                    &now_us(),
                ],
            )
            .await?;
            // Defense-in-depth: handler-level `dedupe_labels_ci` should have
            // already collapsed duplicates, but if a future code path mutates
            // `inner.runners[id].labels` directly we still need a `(runner_id,
            // label)` insert that cannot violate the primary key. `DO NOTHING`
            // is idempotent: a re-persist of the same label set is a no-op.
            for (ordinal, label) in dedupe_labels_ci(&runner.labels).iter().enumerate() {
                tx.execute(
                    "INSERT INTO runner_labels(runner_id, label, ordinal) VALUES ($1, $2, $3)
                     ON CONFLICT (runner_id, label) DO NOTHING",
                    &[&runner.id, &label, &(ordinal as i64)],
                )
                .await?;
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
                        "INSERT INTO runner_sessions(session_id, runner_id, protocol,
                                                     session_key_blob, session_iv, session_tag,
                                                     created_at_us, last_seen_at_us)
                         VALUES ($1, $2, 'broker', $3, $4, $5, $6, $6)
                         ON CONFLICT(session_id) DO UPDATE SET
                           runner_id = EXCLUDED.runner_id,
                           protocol = EXCLUDED.protocol,
                           session_key_blob = EXCLUDED.session_key_blob,
                           session_iv = EXCLUDED.session_iv,
                           session_tag = EXCLUDED.session_tag,
                           last_seen_at_us = EXCLUDED.last_seen_at_us",
                        &[
                            &session.session_id.0.to_string(),
                            &session.runner_id,
                            &ct,
                            &iv,
                            &tag,
                            &now_us(),
                        ],
                    )
                    .await?;
                }
                None => {
                    tx.execute(
                        "INSERT INTO runner_sessions(session_id, runner_id, protocol,
                                                     created_at_us, last_seen_at_us)
                         VALUES ($1, $2, 'broker', $3, $3)
                         ON CONFLICT(session_id) DO UPDATE SET
                           runner_id = EXCLUDED.runner_id,
                           protocol = EXCLUDED.protocol,
                           session_key_blob = NULL,
                           session_iv = NULL,
                           session_tag = NULL,
                           last_seen_at_us = EXCLUDED.last_seen_at_us",
                        &[
                            &session.session_id.0.to_string(),
                            &session.runner_id,
                            &now_us(),
                        ],
                    )
                    .await?;
                }
            }
        }
        for record in &snapshot.requests {
            self.insert_request_tx(&tx, record).await?;
        }
        // Steps are keyed by attempt; the request records tie an attempt to
        // its run.
        for record in &snapshot.requests {
            if let Some((_, steps)) = snapshot
                .job_steps
                .iter()
                .find(|(agent_job_id, _)| *agent_job_id == record.agent_job_id)
            {
                self.write_job_steps_tx(&tx, record.run_id, record.agent_job_id, steps)
                    .await?;
            }
        }
        self.write_claim_state_tx(
            &tx,
            &snapshot.session_active_requests,
            &snapshot.inflight,
            &snapshot.broker_request_messages,
        )
        .await?;
        self.write_meta_tx(&tx, &snapshot.meta).await?;
        tx.commit()
            .await
            .map_err(|error| anyhow::anyhow!("committing store snapshot: {error}"))?;
        Ok(())
    }

    async fn store_meta_only(&self, meta: &MetaSnapshot) -> anyhow::Result<()> {
        let mut client = self.connection.lock().await;
        let tx = client.transaction().await?;
        self.write_meta_tx(&tx, meta).await?;
        tx.commit()
            .await
            .map_err(|error| anyhow::anyhow!("committing metadata: {error}"))?;
        Ok(())
    }

    async fn store_run_event(&self, projection: RunProjection) -> anyhow::Result<()> {
        let mut client = self.connection.lock().await;
        let tx = client.transaction().await?;
        let run_id = projection.run.run_id;
        self.store_run_tx(&tx, &projection.run).await?;
        tx.execute("DELETE FROM jobs WHERE run_id = $1", &[&run_id.to_string()])
            .await?;
        for (kind, job, position) in &projection.jobs {
            self.insert_job(&tx, job, kind, *position).await?;
        }
        tx.execute(
            "DELETE FROM job_requests WHERE run_id = $1",
            &[&run_id.to_string()],
        )
        .await?;
        for record in &projection.requests {
            self.insert_request_tx(&tx, record).await?;
        }
        for (agent_job_id, steps) in &projection.job_steps {
            self.write_job_steps_tx(&tx, run_id, *agent_job_id, steps)
                .await?;
        }
        // Claim state must land in the same transaction as the queue rewrite
        // (see the SQLite twin).
        self.write_claim_state_tx(
            &tx,
            &projection.session_active_requests,
            &projection.inflight,
            &projection.broker_request_messages,
        )
        .await?;
        self.insert_event_tx(&tx, &projection.event).await?;
        tx.commit()
            .await
            .map_err(|error| anyhow::anyhow!("committing run event: {error}"))?;
        Ok(())
    }

    async fn store_workflow_run_counter(
        &self,
        workflow_path: &str,
        next_run_number: u64,
    ) -> anyhow::Result<()> {
        let mut client = self.connection.lock().await;
        let tx = client.transaction().await?;
        tx.execute(
            "INSERT INTO workflow_run_counters(repository_key, workflow_path, next_run_number)
             VALUES ('', $1, $2)
             ON CONFLICT(repository_key, workflow_path) DO UPDATE SET
               next_run_number = EXCLUDED.next_run_number",
            &[&workflow_path, &(next_run_number as i64)],
        )
        .await?;
        tx.commit()
            .await
            .map_err(|error| anyhow::anyhow!("committing workflow run counter: {error}"))?;
        Ok(())
    }

    async fn store_log_chunk(
        &self,
        key: &str,
        chunk_index: i64,
        payload: &[u8],
        byte_count: i64,
        line_count: i64,
    ) -> anyhow::Result<()> {
        let mut client = self.connection.lock().await;
        let tx = client.transaction().await?;
        // UPSERT the parent row first so the FK from `log_chunks` is
        // satisfied even on the very first append.
        tx.execute(
            "INSERT INTO log_files(log_key, byte_count, line_count, updated_at_us)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT(log_key) DO UPDATE SET
               byte_count = EXCLUDED.byte_count,
               line_count = EXCLUDED.line_count,
               updated_at_us = EXCLUDED.updated_at_us",
            &[&key, &byte_count, &line_count, &now_us()],
        )
        .await?;
        tx.execute(
            "INSERT INTO log_chunks(log_key, chunk_index, payload, written_at_us)
             VALUES ($1, $2, $3, $4)",
            &[&key, &chunk_index, &payload, &now_us()],
        )
        .await?;
        // D2: bound durable bytes per log key to the in-memory retention
        // (see the SQLite backend for the rationale). `chunk_index` is the
        // cumulative byte count after this append.
        let cutoff = byte_count - crate::memory_caps::MAX_LOG_BYTES_PER_KEY as i64;
        if cutoff > 0 {
            tx.execute(
                "DELETE FROM log_chunks WHERE log_key = $1 AND chunk_index <= $2",
                &[&key, &cutoff],
            )
            .await?;
        }
        tx.commit()
            .await
            .map_err(|error| anyhow::anyhow!("committing log chunk: {error}"))?;
        Ok(())
    }

    async fn delete_log(&self, key: &str) -> anyhow::Result<()> {
        let client = self.connection.lock().await;
        client
            .execute("DELETE FROM log_files WHERE log_key = $1", &[&key])
            .await?;
        Ok(())
    }

    async fn append_event(&self, event: &NdjsonEvent) -> anyhow::Result<()> {
        let mut client = self.connection.lock().await;
        let tx = client.transaction().await?;
        self.insert_event_tx(&tx, event).await?;
        tx.commit()
            .await
            .map_err(|error| anyhow::anyhow!("committing control event: {error}"))?;
        Ok(())
    }
}

/// Postgres dialect of the control-plane schema. Same tables as the SQLite
/// backend minus `runner_commands` (nothing reads or writes it) and minus
/// SQLite-only constructs (`STRICT`, `WITHOUT ROWID`, `json_valid` checks —
/// payloads always come from `serde_json`). `BIGSERIAL` replaces
/// `AUTOINCREMENT` for `control_events.event_id`.
const MIGRATIONS: &[(u32, &str, &str)] = &[
    (
        1,
        "initial-control-plane-schema",
        r#"
        CREATE TABLE IF NOT EXISTS workflow_run_counters (
          repository_key TEXT NOT NULL,
          workflow_path TEXT NOT NULL,
          next_run_number BIGINT NOT NULL CHECK (next_run_number >= 1),
          PRIMARY KEY (repository_key, workflow_path)
        );

        CREATE TABLE IF NOT EXISTS runs (
          run_id TEXT PRIMARY KEY,
          repository TEXT NOT NULL,
          workflow_path TEXT NOT NULL,
          status TEXT NOT NULL,
          run_number BIGINT NOT NULL,
          run_attempt BIGINT NOT NULL,
          created_at_us BIGINT NOT NULL,
          completed_at_us BIGINT,
          record_blob BYTEA NOT NULL
        );

        CREATE TABLE IF NOT EXISTS run_secrets (
          run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
          crypto_version BIGINT NOT NULL DEFAULT 1,
          secret_blob BYTEA NOT NULL
        );

        CREATE TABLE IF NOT EXISTS runners (
          runner_id BIGINT PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          ephemeral BOOLEAN NOT NULL,
          runner_group_id BIGINT,
          runner_group_name TEXT,
          public_key TEXT,
          rsa_public_key TEXT,
          created_at_us BIGINT NOT NULL,
          updated_at_us BIGINT NOT NULL,
          deleted_at_us BIGINT
        );

        CREATE TABLE IF NOT EXISTS runner_labels (
          runner_id BIGINT NOT NULL REFERENCES runners(runner_id) ON DELETE CASCADE,
          label TEXT NOT NULL,
          ordinal BIGINT NOT NULL,
          PRIMARY KEY (runner_id, label)
        );

        CREATE TABLE IF NOT EXISTS runner_sessions (
          session_id TEXT PRIMARY KEY,
          runner_id BIGINT NOT NULL,
          protocol TEXT NOT NULL,
          client_id TEXT,
          session_key_blob BYTEA,
          session_iv BYTEA,
          session_tag BYTEA,
          created_at_us BIGINT NOT NULL,
          last_seen_at_us BIGINT NOT NULL,
          closed_at_us BIGINT
        );

        CREATE TABLE IF NOT EXISTS jobs (
          run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
          job_id TEXT NOT NULL,
          status TEXT NOT NULL,
          queue_kind TEXT NOT NULL CHECK (
            queue_kind IN ('ready','pending','blocked','held')
          ),
          queue_position BIGINT NOT NULL,
          payload_blob BYTEA NOT NULL,
          PRIMARY KEY (run_id, job_id)
        );

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
        );

        CREATE TABLE IF NOT EXISTS job_requests (
          request_id BIGINT PRIMARY KEY,
          run_id TEXT NOT NULL,
          job_id TEXT NOT NULL,
          agent_job_id TEXT NOT NULL UNIQUE,
          plan_id TEXT NOT NULL UNIQUE,
          timeline_id TEXT NOT NULL UNIQUE,
          state TEXT NOT NULL,
          request_blob BYTEA NOT NULL,
          FOREIGN KEY (run_id)
            REFERENCES runs(run_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS control_events (
          event_id BIGSERIAL PRIMARY KEY,
          run_id TEXT NOT NULL,
          job_id TEXT,
          event_type TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          created_at_us BIGINT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS control_events_cursor_idx
          ON control_events (run_id, event_id);

        CREATE TABLE IF NOT EXISTS session_active_requests (
          session_id TEXT PRIMARY KEY,
          active_request_id BIGINT NOT NULL
            REFERENCES job_requests(request_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS broker_messages (
          session_id TEXT NOT NULL,
          message_id BIGINT NOT NULL,
          payload_json TEXT NOT NULL,
          written_at_us BIGINT NOT NULL,
          PRIMARY KEY (session_id, message_id)
        );

        CREATE TABLE IF NOT EXISTS log_files (
          log_key TEXT PRIMARY KEY,
          byte_count BIGINT NOT NULL DEFAULT 0 CHECK (byte_count >= 0),
          line_count BIGINT NOT NULL DEFAULT 0 CHECK (line_count >= 0),
          updated_at_us BIGINT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS log_chunks (
          log_key TEXT NOT NULL REFERENCES log_files(log_key) ON DELETE CASCADE,
          chunk_index BIGINT NOT NULL,
          payload BYTEA NOT NULL,
          written_at_us BIGINT NOT NULL,
          PRIMARY KEY (log_key, chunk_index)
        );

        CREATE TABLE IF NOT EXISTS runtime_snapshots (
          snapshot_id BIGINT PRIMARY KEY CHECK (snapshot_id = 1),
          format_version BIGINT NOT NULL,
          meta_blob BYTEA NOT NULL,
          written_at_us BIGINT NOT NULL
        );
        "#,
    ),
    (
        2,
        "drop-redundant-run-secrets",
        // See the SQLite twin: this table stored `"<redacted>"`, not secrets.
        r#"
        DROP TABLE IF EXISTS run_secrets;
        "#,
    ),
    (
        3,
        "job-request-messages-table",
        // See the SQLite twin: claim state must land in the same transaction
        // as the queue rewrite.
        r#"
        CREATE TABLE IF NOT EXISTS job_request_messages (
          request_id BIGINT PRIMARY KEY,
          payload_json TEXT NOT NULL,
          written_at_us BIGINT NOT NULL
        );
        "#,
    ),
    (
        4,
        "job-steps-table",
        // See the SQLite twin: steps live outside `runs.record_blob` so a step
        // transition upserts one small row instead of resealing the whole run
        // record, and are keyed by attempt because a re-dispatch mints fresh
        // step ids.
        r#"
        CREATE TABLE IF NOT EXISTS job_steps (
          run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
          agent_job_id TEXT NOT NULL,
          step_id TEXT NOT NULL,
          kind TEXT NOT NULL CHECK (kind IN ('workflow','synthetic')),
          workflow_index BIGINT,
          runner_number BIGINT,
          context_name TEXT,
          name_blob BYTEA NOT NULL,
          conclusion TEXT NOT NULL,
          started_at_us BIGINT,
          finished_at_us BIGINT,
          PRIMARY KEY (agent_job_id, step_id)
        );

        CREATE INDEX IF NOT EXISTS job_steps_order_idx
          ON job_steps (agent_job_id, kind, workflow_index);
        "#,
    ),
];
