//! Postgres-backed [`Store`] implementation.
//!
//! Selected with `AKSH_STORE_URL=postgres://<user>:<pass>@<host>:<port>/<db>`.
//! Mirrors the SQLite backend table-for-table (see [`MIGRATIONS`]); the
//! relational model, sealed-blob payloads, and migration steps are the same,
//! only the dialect differs (`$n` params, `EXCLUDED` upserts, native types).
//!
//! Concurrency model: one `tokio-postgres` client behind a tokio mutex, the
//! same single-writer shape as SQLite. The in-memory state remains the source
//! of truth; this is a restart store, not a shared bus. TLS is not negotiated
//! (`NoTls`) — terminate TLS at a proxy/tunnel for remote databases.

use super::*;
use aksh_gha_protocol::{SecretMap, SessionId};
use async_trait::async_trait;
use tokio_postgres::{connect, Client, NoTls};

/// Postgres backend: one client behind a mutex.
pub(crate) struct PgStore {
    connection: Arc<tokio::sync::Mutex<Client>>,
    cipher: Envelope,
}

impl PgStore {
    /// Connect, apply pending migrations, and return the store.
    pub(crate) async fn open(url: &str, cipher: Envelope) -> anyhow::Result<Self> {
        let (mut client, connection) = connect(url, NoTls).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(%error, "postgres connection task failed");
            }
        });
        Self::migrate(&mut client).await?;
        Ok(Self {
            connection: Arc::new(tokio::sync::Mutex::new(client)),
            cipher,
        })
    }

    /// Apply pending migrations. `schema_migrations` doubles as the version
    /// pointer (SQLite uses `PRAGMA user_version`); steps are append-only and
    /// each runs in its own transaction.
    async fn migrate(client: &mut Client) -> anyhow::Result<()> {
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
        let sealed_secrets = self
            .cipher
            .seal(&serde_json::to_vec(&run.submission.secrets)?)?;
        tx.execute(
            "INSERT INTO run_secrets(run_id, secret_blob) VALUES ($1, $2)
             ON CONFLICT(run_id) DO UPDATE SET secret_blob = EXCLUDED.secret_blob",
            &[&run.run_id.to_string(), &sealed_secrets],
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
        inner: &InnerState,
    ) -> anyhow::Result<()> {
        let meta = build_meta_snapshot(inner);
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
            let mut run = restore_run_record(&self.cipher, &blob)?;
            let run_id = run.run_id;
            if let Some(row) = client
                .query_opt(
                    "SELECT secret_blob FROM run_secrets WHERE run_id = $1",
                    &[&run_id.to_string()],
                )
                .await?
            {
                let secret_blob: Vec<u8> = row.get(0);
                let secrets: SecretMap =
                    serde_json::from_slice(&self.cipher.unseal(&secret_blob)?)?;
                Arc::make_mut(&mut run.submission).secrets = secrets;
            }
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
        // Restore per-session broker message queues (dequeued but not delivered).
        // `broker_messages` in the in-memory state is keyed by request_id and
        // lives in the meta snapshot; `inflight_messages` (per-session) is
        // also restored from the same snapshot below.
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

    async fn store_inner(&self, inner: &InnerState) -> anyhow::Result<()> {
        let mut client = self.connection.lock().await;
        let tx = client.transaction().await?;
        for table in [
            "broker_messages",
            "session_active_requests",
            "run_secrets",
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
        for run in inner.runs.values() {
            self.store_run_tx(&tx, run).await?;
        }

        let mut position = 0i64;
        for (kind, jobs) in [
            ("ready", inner.queue.iter().collect::<Vec<_>>()),
            ("pending", inner.pending_jobs.iter().collect::<Vec<_>>()),
            (
                "blocked",
                inner.concurrency_blocked.iter().collect::<Vec<_>>(),
            ),
        ] {
            for job in jobs {
                self.insert_job(&tx, job, kind, position).await?;
                position += 1;
            }
        }
        for (run_id, jobs) in &inner.held_runs {
            for job in jobs {
                self.insert_job(&tx, job, "held", position).await?;
                position += 1;
            }
            let _ = run_id;
        }

        for runner in inner.runners.values() {
            // `ON CONFLICT` so re-registration (same `runner_id`, new name)
            // overwrites the row in place. Without it, `UNIQUE(name)` would
            // reject the insert whenever the in-memory map had two runners
            // that share a name — which the official `actions/runner` does
            // on every re-register / replace flow.
            // Typed RSA public key is stored alongside the XML form so a
            // post-restart session can be FIPS-encrypted without re-parsing
            // and without an extra table.
            let rsa_xml = inner
                .runner_rsa_public_keys
                .get(&runner.id)
                .map(|key| key.to_xml_string());
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
        for session in inner.sessions.values() {
            // `session_id` is the natural primary key; a re-persist
            // overwrites the same row.
            let key_blob = inner
                .session_keys
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
        for record in inner.job_requests.values() {
            self.insert_request_tx(&tx, record).await?;
        }
        // Persist per-session active-request assignments. A restart
        // re-derives the assignment so a dequeued-but-unacked request is
        // re-delivered to the runner when it polls again.
        tx.execute("DELETE FROM session_active_requests", &[])
            .await?;
        for (session_id, request_id) in &inner.session_active_requests {
            tx.execute(
                "INSERT INTO session_active_requests(session_id, active_request_id) VALUES ($1, $2)",
                &[&session_id, &request_id],
            )
            .await?;
        }
        // Persist per-session broker message queues.
        tx.execute("DELETE FROM broker_messages", &[]).await?;
        for (session_id, messages) in &inner.inflight_messages {
            for (message_id, payload) in messages {
                let payload_json = serde_json::to_string(payload)?;
                tx.execute(
                    "INSERT INTO broker_messages(session_id, message_id, payload_json, written_at_us)
                     VALUES ($1, $2, $3, $4)",
                    &[&session_id, &message_id, &payload_json, &now_us()],
                )
                .await?;
            }
        }
        self.write_meta_tx(&tx, inner).await?;
        tx.commit()
            .await
            .map_err(|error| anyhow::anyhow!("committing store snapshot: {error}"))?;
        Ok(())
    }

    async fn store_meta_only(&self, inner: &InnerState) -> anyhow::Result<()> {
        let mut client = self.connection.lock().await;
        let tx = client.transaction().await?;
        self.write_meta_tx(&tx, inner).await?;
        tx.commit()
            .await
            .map_err(|error| anyhow::anyhow!("committing metadata: {error}"))?;
        Ok(())
    }

    async fn store_run_event(
        &self,
        inner: &InnerState,
        run_id: RunId,
        event: &NdjsonEvent,
    ) -> anyhow::Result<()> {
        let mut client = self.connection.lock().await;
        let tx = client.transaction().await?;
        let Some(run) = inner.runs.get(&run_id) else {
            return Ok(());
        };
        self.store_run_tx(&tx, run).await?;
        tx.execute("DELETE FROM jobs WHERE run_id = $1", &[&run_id.to_string()])
            .await?;
        let mut position = 0i64;
        for (kind, jobs) in [
            (
                "ready",
                inner
                    .queue
                    .iter()
                    .filter(|job| job.run_id == run_id)
                    .collect::<Vec<_>>(),
            ),
            (
                "pending",
                inner
                    .pending_jobs
                    .iter()
                    .filter(|job| job.run_id == run_id)
                    .collect::<Vec<_>>(),
            ),
            (
                "blocked",
                inner
                    .concurrency_blocked
                    .iter()
                    .filter(|job| job.run_id == run_id)
                    .collect::<Vec<_>>(),
            ),
        ] {
            for job in jobs {
                self.insert_job(&tx, job, kind, position).await?;
                position += 1;
            }
        }
        if let Some(jobs) = inner.held_runs.get(&run_id) {
            for job in jobs {
                self.insert_job(&tx, job, "held", position).await?;
                position += 1;
            }
        }
        tx.execute(
            "DELETE FROM job_requests WHERE run_id = $1",
            &[&run_id.to_string()],
        )
        .await?;
        for record in inner
            .job_requests
            .values()
            .filter(|record| record.run_id == run_id)
        {
            self.insert_request_tx(&tx, record).await?;
        }
        self.insert_event_tx(&tx, event).await?;
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
        tx.commit()
            .await
            .map_err(|error| anyhow::anyhow!("committing log chunk: {error}"))?;
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
const MIGRATIONS: &[(u32, &str, &str)] = &[(
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
)];
