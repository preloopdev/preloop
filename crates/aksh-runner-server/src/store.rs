//! Durable SQLite state for the control plane.
//!
//! The HTTP layer still uses the in-memory structures for fast protocol
//! handling, but every correctness-bearing transition is written through to
//! this database before the transition is announced to observers.  The
//! database is also the restart source for runs, dispatch queues, runners,
//! sessions, and broker request identifiers.
//!
//! The store is **best-effort**: in-memory state is the source of truth, the
//! database is a restart source. Store failures are logged and the
//! affected event is still broadcast to subscribers. See `state.rs::emit`.

use super::*;
use aksh_gha_protocol::{SecretMap, SessionId};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use sha2::Digest;
use std::sync::Mutex as StdMutex;

const DATABASE_FILE: &str = "aksh.db";
const SNAPSHOT_FORMAT: u8 = 2;
const MIGRATION_DOMAIN: &[u8] = b"aksh-store-v2";
const KEY_INFO_ENCRYPT: &[u8] = b"aks-store-aead/v1";
const KEY_INFO_MAC: &[u8] = b"aks-store-mac/v1";

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub(crate) struct Store {
    connection: Arc<StdMutex<Connection>>,
    key: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MetaSnapshot {
    workflow_run_counters: BTreeMap<String, u64>,
    next_runner_id: i64,
    next_cache_id: i64,
    next_message_id: i64,
    next_log_id: usize,
    next_artifact_v2_id: u64,
    azdo_sessions: std::collections::HashSet<String>,
    oidc_job_contexts: Vec<(RunId, JobId, OidcJobContext)>,
    id_token_grants: Vec<(RunId, JobId, bool)>,
    concurrency_groups: Vec<((String, String), concurrency::ConcurrencyGroup)>,
    jobset_admissions: Vec<(JobSetId, JobSetAdmission)>,
    run_concurrency: Vec<(RunId, aksh_gha_parser::Concurrency)>,
    holder_keys: Vec<(RunId, Vec<(String, String)>)>,
    #[serde(default)]
    pending_caches: Vec<(i64, PendingCache)>,
    #[serde(default)]
    artifacts: Vec<(String, ArtifactRecord)>,
    #[serde(default)]
    log_metadata: Vec<(String, LogMetadata)>,
    #[serde(default)]
    timeline_events: Vec<(RunId, Vec<NdjsonEvent>)>,
    #[serde(default)]
    timeline_change_ids: Vec<(String, i32)>,
    #[serde(default)]
    timeline_records: Vec<(String, Vec<(uuid::Uuid, azdo::TimelineRecord)>)>,
    #[serde(default)]
    cache_v2_pending: Vec<(String, CacheV2Pending)>,
    #[serde(default)]
    cache_v2_dl_tokens: Vec<(String, (String, String))>,
    #[serde(default)]
    artifact_v2_pending: Vec<(String, ArtifactV2Pending)>,
    #[serde(default)]
    artifact_v2_registry: Vec<(String, ArtifactV2Entry)>,
    /// `github_token_requests` keyed by `(run_id, job_id)` so post-restart
    /// dispatch can mint the same token under the same conditions the original
    /// run recorded. Sealed on disk; the in-memory map drives the dispatcher.
    #[serde(default)]
    github_token_requests: Vec<(i64, GitHubTokenRequest)>,
    /// `cancellation_queue` queued cancels that did not yet propagate to the
    /// runner. Persisted so a restart mid-cancel still delivers the cancel.
    #[serde(default)]
    cancellation_queue: std::collections::VecDeque<QueuedCancellation>,
    /// Per-session broker message queue (dequeued but not yet delivered to
    /// the runner). Persisted so a restart re-delivers instead of dropping
    /// the assignment.
    #[serde(default)]
    broker_messages: Vec<(i64, azdo::AgentJobRequestMessage)>,
    /// `session_active_requests`: per-session currently-claimed request id.
    #[serde(default)]
    session_active_requests: Vec<(String, i64)>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RequestSnapshot {
    request_id: i64,
    run_id: RunId,
    job_id: JobId,
    agent_job_id: uuid::Uuid,
    plan_id: String,
    plan_type: String,
    timeline_id: uuid::Uuid,
    result: Option<ExecutionStatus>,
    locked_until: String,
    started_at_us: Option<i64>,
    last_renewed_at_us: Option<i64>,
    timeout_triggered: bool,
    debug_token_issued: bool,
}

#[derive(Debug)]
struct RowJob {
    run_id: RunId,
    job_id: JobId,
    queue_kind: String,
    queue_position: i64,
    payload: Vec<u8>,
}

struct DerivedKeys {
    aead: [u8; 32],
    mac: [u8; 32],
}

/// Derive independent sub-keys from the loaded HMAC key via HKDF-SHA256.
///
/// The same root key is used for both the JWT HMAC and the store AEAD
/// (so callers can keep one `<state_dir>/hmac-key.bin`); HKDF gives each
/// purpose a domain-separated 32-byte key. This costs nothing when the keys
/// are derived at startup once.
fn derive_keys(root: &[u8]) -> DerivedKeys {
    // Salt = the root key itself, so two aksh installs that happen to load
    // the same weak env var don't end up with the same sub-keys.
    let mut aead_out = [0u8; 32];
    let mut mac_out = [0u8; 32];
    let salt = Sha256::digest(root);
    hkdf::Hkdf::<Sha256>::new(Some(salt.as_slice()), root)
        .expand(KEY_INFO_ENCRYPT, &mut aead_out)
        .expect("HKDF expand to 32 bytes is infallible");
    hkdf::Hkdf::<Sha256>::new(Some(salt.as_slice()), root)
        .expand(KEY_INFO_MAC, &mut mac_out)
        .expect("HKDF expand to 32 bytes is infallible");
    DerivedKeys {
        aead: aead_out,
        mac: mac_out,
    }
}

impl Store {
    pub(crate) fn open(state_dir: &std::path::Path, key: &[u8]) -> anyhow::Result<Self> {
        std::fs::create_dir_all(state_dir)?;
        let path = state_dir.join(DATABASE_FILE);
        let connection = Connection::open(path)?;
        connection.execute_batch(
            r#"
            PRAGMA application_id = 0x414B5348;
            PRAGMA journal_mode = WAL;
            -- WAL + NORMAL preserves committed transactions across process
            -- crashes while avoiding an fsync for every runner event.
            PRAGMA synchronous = NORMAL;
            PRAGMA foreign_keys = ON;
            PRAGMA busy_timeout = 5000;
            "#,
        )?;
        Self::migrate(&connection)?;
        Ok(Self {
            connection: Arc::new(StdMutex::new(connection)),
            key: key.to_vec(),
        })
    }

    /// Apply pending migrations. Each step runs in its own transaction inside
    /// `PRAGMA user_version`; the `schema_migrations` table is a human audit
    /// trail. Steps are append-only and idempotent.
    fn migrate(connection: &Connection) -> anyhow::Result<()> {
        // Always-present bookkeeping table for the migration audit trail.
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS schema_migrations (
              version INTEGER PRIMARY KEY,
              name TEXT NOT NULL UNIQUE,
              applied_at_us INTEGER NOT NULL
            ) STRICT;
            "#,
        )?;

        let current_version: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
            .unwrap_or(0);
        for (version, name, sql) in MIGRATIONS {
            if *version as i64 <= current_version {
                continue;
            }
            let tx = connection.unchecked_transaction()?;
            tx.execute_batch(sql)?;
            tx.execute(
                "INSERT INTO schema_migrations(version, name, applied_at_us) VALUES (?1, ?2, ?3)
                 ON CONFLICT(version) DO NOTHING",
                params![*version as i64, *name, now_us()],
            )?;
            tx.execute_batch(&format!("PRAGMA user_version = {}", *version))?;
            tx.commit()?;
        }
        Ok(())
    }

    pub(crate) fn load_into(&self, inner: &mut InnerState) -> anyhow::Result<()> {
        let connection = self.connection.lock().expect("store mutex poisoned");
        let mut run_stmt =
            connection.prepare("SELECT record_blob FROM runs ORDER BY created_at_us")?;
        let runs = run_stmt
            .query_map([], |row| row.get::<_, Vec<u8>>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for blob in runs {
            let value: serde_json::Value = serde_json::from_slice(&self.unseal(&blob)?)?;
            let mut run: RunRecord = serde_json::from_value(value.clone())?;
            run.job_needs = value
                .get("job_needs")
                .cloned()
                .map(serde_json::from_value)
                .transpose()?
                .unwrap_or_default();
            let run_id = run.run_id;
            if let Some(secret_blob) = connection
                .query_row(
                    "SELECT secret_blob FROM run_secrets WHERE run_id = ?1",
                    [run_id.to_string()],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .optional()?
            {
                let secrets: SecretMap = serde_json::from_slice(&self.unseal(&secret_blob)?)?;
                Arc::make_mut(&mut run.submission).secrets = secrets;
            }
            inner.runs.insert(run_id, run);
        }

        let mut job_stmt = connection.prepare(
            "SELECT run_id, job_id, queue_kind, queue_position, payload_blob
             FROM jobs ORDER BY queue_kind, queue_position",
        )?;
        let jobs = job_stmt
            .query_map([], |row| {
                Ok(RowJob {
                    run_id: row.get::<_, String>(0)?.parse().map_err(|_| {
                        rusqlite::Error::InvalidColumnType(
                            0,
                            "run_id".to_owned(),
                            rusqlite::types::Type::Text,
                        )
                    })?,
                    job_id: JobId(row.get(1)?),
                    queue_kind: row.get(2)?,
                    queue_position: row.get(3)?,
                    payload: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for row in jobs {
            let job: QueuedJob = serde_json::from_slice(&self.unseal(&row.payload)?)?;
            match row.queue_kind.as_str() {
                "ready" => inner.queue.push_back(job),
                "pending" => inner.pending_jobs.push_back(job),
                "blocked" => inner.concurrency_blocked.push_back(job),
                "held" => inner.held_runs.entry(row.run_id).or_default().push(job),
                _ => unreachable!("schema constrains queue_kind"),
            }
        }

        let mut runner_stmt = connection.prepare(
            "SELECT runner_id, name, ephemeral, runner_group_id, runner_group_name,
                    public_key, rsa_public_key
             FROM runners WHERE deleted_at_us IS NULL",
        )?;
        for runner in runner_stmt.query_map([], |row| {
            Ok(RegisteredRunner {
                id: row.get(0)?,
                name: row.get(1)?,
                ephemeral: row.get(2)?,
                runner_group_id: row.get(3)?,
                runner_group_name: row.get(4)?,
                public_key: row.get(5)?,
                labels: Vec::new(),
            })
        })? {
            let runner = runner?;
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
        let mut rsa_stmt = connection.prepare(
            "SELECT runner_id, rsa_public_key FROM runners
             WHERE deleted_at_us IS NULL AND rsa_public_key IS NOT NULL",
        )?;
        for row in rsa_stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })? {
            let (runner_id, rsa_xml) = row?;
            if let Ok(parsed) = AgentRsaPublicKey::parse(&rsa_xml) {
                inner.runner_rsa_public_keys.insert(runner_id, parsed);
            }
        }
        let mut label_stmt =
            connection.prepare("SELECT runner_id, label FROM runner_labels ORDER BY ordinal")?;
        for label in label_stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })? {
            let (runner_id, label) = label?;
            if let Some(runner) = inner.runners.get_mut(&runner_id) {
                runner.labels.push(label);
            }
        }
        // Restore session encryption keys (sealed). Without these, every
        // post-restart session is `encrypted:false` regardless of the runner's
        // RSA key — the AES session key is sent in plaintext to the runner.
        let mut session_keys_stmt = connection.prepare(
            "SELECT session_id, session_key_blob, session_iv, session_tag
             FROM runner_sessions
             WHERE closed_at_us IS NULL AND session_key_blob IS NOT NULL",
        )?;
        for row in session_keys_stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, Vec<u8>>(3)?,
            ))
        })? {
            let (session_id, key_blob, iv, tag) = row?;
            match self.decrypt_sealed(&key_blob, &iv, &tag) {
                Ok(plaintext) => match serde_json::from_slice::<SessionKeyPayload>(&plaintext) {
                    Ok(payload) => {
                        inner
                            .session_keys
                            .insert(session_id.clone(), SessionEncryption::from_key(payload.0));
                    }
                    Err(error) => {
                        tracing::warn!(%session_id, %error, "failed to parse session_key payload on load");
                    }
                },
                Err(error) => {
                    tracing::warn!(%session_id, %error, "failed to decrypt session_key on load");
                }
            }
        }
        let mut session_stmt = connection.prepare(
            "SELECT session_id, runner_id FROM runner_sessions WHERE closed_at_us IS NULL",
        )?;
        for session in session_stmt.query_map([], |row| {
            Ok(RunnerSession {
                session_id: SessionId(row.get::<_, String>(0)?.parse().map_err(|_| {
                    rusqlite::Error::InvalidColumnType(
                        0,
                        "session_id".to_owned(),
                        rusqlite::types::Type::Text,
                    )
                })?),
                runner_id: row.get(1)?,
            })
        })? {
            let session = session?;
            inner
                .broker_session_runners
                .insert(session.session_id.0.to_string(), session.runner_id);
            inner
                .sessions
                .insert(session.session_id.0.to_string(), session);
        }
        // Restore `session_active_requests` so a restarted broker session
        // knows which request it had claimed but not acked.
        let mut sar_stmt = connection
            .prepare("SELECT session_id, active_request_id FROM session_active_requests")?;
        for row in sar_stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })? {
            let (session_id, request_id) = row?;
            inner.session_active_requests.insert(session_id, request_id);
        }
        // Restore per-session broker message queues (dequeued but not delivered).
        // `broker_messages` in the in-memory state is keyed by request_id and
        // lives in the meta snapshot; `inflight_messages` (per-session) is
        // also restored from the same snapshot below.
        let mut request_stmt =
            connection.prepare("SELECT request_id, request_blob FROM job_requests")?;
        for row in request_stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
        })? {
            let (request_id, blob) = row?;
            let snapshot: RequestSnapshot = serde_json::from_slice(&self.unseal(&blob)?)?;
            let record = TaskAgentJobRequestRecord {
                request_id,
                run_id: snapshot.run_id,
                job_id: snapshot.job_id.clone(),
                agent_job_id: snapshot.agent_job_id,
                plan_id: snapshot.plan_id.clone(),
                plan_type: snapshot.plan_type,
                timeline_id: snapshot.timeline_id,
                result: snapshot.result,
                locked_until: snapshot.locked_until,
                started_at: snapshot.started_at_us.map(system_time_from_us),
                last_renewed_at: snapshot.last_renewed_at_us.map(system_time_from_us),
                timeout_triggered: snapshot.timeout_triggered,
                debug_token_issued: snapshot.debug_token_issued,
            };
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

        if let Some(blob) = connection
            .query_row(
                "SELECT meta_blob FROM runtime_snapshots WHERE snapshot_id = 1",
                [],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?
        {
            let meta: MetaSnapshot = serde_json::from_slice(&self.unseal(&blob)?)?;
            inner.workflow_run_counters = meta.workflow_run_counters;
            inner.next_runner_id = meta.next_runner_id;
            inner.next_cache_id = meta.next_cache_id;
            inner.next_message_id = meta.next_message_id;
            inner.next_log_id = meta.next_log_id;
            inner.next_artifact_v2_id = meta.next_artifact_v2_id;
            inner.azdo_sessions = meta.azdo_sessions;
            inner.oidc_job_contexts = meta
                .oidc_job_contexts
                .into_iter()
                .map(|(run_id, job_id, context)| ((run_id, job_id), context))
                .collect();
            inner.id_token_grants = meta
                .id_token_grants
                .into_iter()
                .map(|(run_id, job_id, granted)| ((run_id, job_id), granted))
                .collect();
            inner.concurrency_groups = meta.concurrency_groups.into_iter().collect();
            inner.jobset_admissions = meta.jobset_admissions.into_iter().collect();
            inner.run_concurrency = meta.run_concurrency.into_iter().collect();
            inner.holder_keys = meta.holder_keys.into_iter().collect();
            inner.pending_caches = meta.pending_caches.into_iter().collect();
            inner.artifacts = meta.artifacts.into_iter().collect();
            inner.log_metadata = meta.log_metadata.into_iter().collect();
            inner.timeline_events = meta.timeline_events.into_iter().collect();
            inner.timeline_change_ids = meta.timeline_change_ids.into_iter().collect();
            inner.timeline_records = meta
                .timeline_records
                .into_iter()
                .map(|(key, records)| (key, records.into_iter().collect()))
                .collect();
            inner.cache_v2_pending = meta.cache_v2_pending.into_iter().collect();
            inner.cache_v2_dl_tokens = meta.cache_v2_dl_tokens.into_iter().collect();
            inner.artifact_v2_pending = meta.artifact_v2_pending.into_iter().collect();
            inner.artifact_v2_registry = meta.artifact_v2_registry.into_iter().collect();
            inner.github_token_requests = meta.github_token_requests.into_iter().collect();
            inner.cancellation_queue = meta.cancellation_queue;
            inner.session_active_requests = meta.session_active_requests.into_iter().collect();
        }
        let mut counters = connection.prepare(
            "SELECT workflow_path, next_run_number
             FROM workflow_run_counters
             ORDER BY workflow_path",
        )?;
        let rows = counters.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })?;
        for row in rows {
            let (workflow_path, next_run_number) = row?;
            inner
                .workflow_run_counters
                .insert(workflow_path, next_run_number.saturating_sub(1));
        }
        // Log bytes live in their own table; rebuild the in-memory buffers
        // from ordered chunks. Failing to do this leaves the post-restart
        // server unable to serve GET /logs/<id> for in-flight jobs.
        let mut log_chunk_stmt = connection.prepare(
            "SELECT log_key, chunk_index, payload FROM log_chunks
             ORDER BY log_key, chunk_index",
        )?;
        for row in log_chunk_stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })? {
            let (key, _index, payload) = row?;
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
        let mut log_files_stmt =
            connection.prepare("SELECT log_key, byte_count, line_count FROM log_files")?;
        for row in log_files_stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })? {
            let (key, byte_count, line_count) = row?;
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

    /// Persist a single log chunk. Replaces the old behaviour of rewriting the
    /// entire `meta_blob` on every append.
    pub(crate) fn store_log_chunk(
        &self,
        key: &str,
        chunk_index: i64,
        payload: &[u8],
        byte_count: i64,
        line_count: i64,
    ) -> anyhow::Result<()> {
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let tx = connection.transaction()?;
        // UPSERT the parent row first so the FK from `log_chunks` is
        // satisfied even on the very first append.
        tx.execute(
            "INSERT INTO log_files(log_key, byte_count, line_count, updated_at_us)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(log_key) DO UPDATE SET
               byte_count = excluded.byte_count,
               line_count = excluded.line_count,
               updated_at_us = excluded.updated_at_us",
            params![key, byte_count, line_count, now_us()],
        )?;
        tx.execute(
            "INSERT INTO log_chunks(log_key, chunk_index, payload, written_at_us)
             VALUES (?1, ?2, ?3, ?4)",
            params![key, chunk_index, payload, now_us()],
        )?;
        tx.commit()
            .map_err(|error| anyhow::anyhow!("committing log chunk: {error}"))?;
        Ok(())
    }

    pub(crate) fn store_inner(&self, inner: &InnerState) -> anyhow::Result<()> {
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let tx = connection.transaction()?;
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
            tx.execute(&format!("DELETE FROM {table}"), [])
                .map_err(|error| anyhow::anyhow!("deleting {table}: {error}"))?;
        }
        for run in inner.runs.values() {
            let run_json = {
                let mut value = serde_json::to_value(run)?;
                if let Some(object) = value.as_object_mut() {
                    object.insert("submission".to_owned(), run.submission.to_request_json()?);
                    object.insert(
                        "job_needs".to_owned(),
                        serde_json::to_value(&run.job_needs)?,
                    );
                }
                serde_json::to_vec(&value)?
            };
            let sealed_run = self.seal(&run_json)?;
            tx.execute(
                "INSERT INTO runs(run_id, repository, workflow_path, status, run_number,
                                  run_attempt, created_at_us, completed_at_us, record_blob)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    run.run_id.to_string(),
                    run.submission.repository.clone(),
                    run.workflow_path_str.clone(),
                    status_string(run.status),
                    run.run_number as i64,
                    run.run_attempt as i64,
                    unix_us(run.created_at),
                    run.completed_at.map(unix_us),
                    sealed_run,
                ],
            )?;
            let secrets = serde_json::to_vec(&run.submission.secrets)?;
            tx.execute(
                "INSERT INTO run_secrets(run_id, secret_blob) VALUES (?1, ?2)",
                params![run.run_id.to_string(), self.seal(&secrets)?],
            )?;
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
                self.insert_job(&tx, job, kind, position)?;
                position += 1;
            }
        }
        for (run_id, jobs) in &inner.held_runs {
            for job in jobs {
                self.insert_job(&tx, job, "held", position)?;
                position += 1;
            }
            let _ = run_id;
        }

        for runner in inner.runners.values() {
            // `OR REPLACE` so re-registration (same `runner_id`, new name)
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
                "INSERT OR REPLACE INTO runners(runner_id, name, ephemeral,
                                                runner_group_id, runner_group_name,
                                                public_key, rsa_public_key,
                                                created_at_us, updated_at_us)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
                params![
                    runner.id,
                    runner.name.clone(),
                    runner.ephemeral as i64,
                    runner.runner_group_id,
                    runner.runner_group_name.clone(),
                    runner.public_key.clone(),
                    rsa_xml,
                    now_us(),
                ],
            )?;
            // Defense-in-depth: handler-level `dedupe_labels_ci` should have
            // already collapsed duplicates, but if a future code path mutates
            // `inner.runners[id].labels` directly we still need a `(runner_id,
            // label)` insert that cannot violate the primary key. `OR IGNORE`
            // is idempotent: a re-persist of the same label set is a no-op.
            for (ordinal, label) in dedupe_labels_ci(&runner.labels).iter().enumerate() {
                tx.execute(
                    "INSERT OR IGNORE INTO runner_labels(runner_id, label, ordinal) VALUES (?1, ?2, ?3)",
                    params![runner.id, label, ordinal as i64],
                )?;
            }
        }
        for session in inner.sessions.values() {
            // `session_id` is the natural primary key; a re-persist
            // overwrites the same row.
            let key_blob = inner
                .session_keys
                .get(&session.session_id.0.to_string())
                .map(|enc| {
                    let payload = serde_json::to_vec(&SessionKeyPayload(enc.key.clone()))
                        .expect("SessionKeyPayload is always serializable");
                    let (ct, iv, tag) = self.encrypt_sealed(&payload);
                    (ct, iv, tag)
                });
            match key_blob {
                Some((ct, iv, tag)) => {
                    tx.execute(
                    "INSERT OR REPLACE INTO runner_sessions(session_id, runner_id, protocol,
                                                            session_key_blob, session_iv, session_tag,
                                                            created_at_us, last_seen_at_us)
                     VALUES (?1, ?2, 'broker', ?3, ?4, ?5, ?6, ?6)",
                    params![
                        session.session_id.0.to_string(),
                        session.runner_id,
                        ct,
                        iv,
                        tag,
                        now_us()
                    ],
                    )?;
                }
                None => {
                    tx.execute(
                        "INSERT OR REPLACE INTO runner_sessions(session_id, runner_id, protocol,
                                                            created_at_us, last_seen_at_us)
                     VALUES (?1, ?2, 'broker', ?3, ?3)",
                        params![
                            session.session_id.0.to_string(),
                            session.runner_id,
                            now_us()
                        ],
                    )?;
                }
            }
        }
        for record in inner.job_requests.values() {
            let snapshot = RequestSnapshot {
                request_id: record.request_id,
                run_id: record.run_id,
                job_id: record.job_id.clone(),
                agent_job_id: record.agent_job_id,
                plan_id: record.plan_id.clone(),
                plan_type: record.plan_type.clone(),
                timeline_id: record.timeline_id,
                result: record.result,
                locked_until: record.locked_until.clone(),
                started_at_us: record.started_at.map(system_time_us),
                last_renewed_at_us: record.last_renewed_at.map(system_time_us),
                timeout_triggered: record.timeout_triggered,
                debug_token_issued: record.debug_token_issued,
            };
            let blob = self.seal(&serde_json::to_vec(&snapshot)?)?;
            tx.execute(
                "INSERT INTO job_requests(request_id, run_id, job_id, agent_job_id,
                                          plan_id, timeline_id, state, request_blob)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'active', ?7)",
                params![
                    record.request_id,
                    record.run_id.to_string(),
                    record.job_id.0,
                    record.agent_job_id.to_string(),
                    record.plan_id,
                    record.timeline_id.to_string(),
                    blob,
                ],
            )?;
        }
        // Persist per-session active-request assignments. A restart
        // re-derives the assignment so a dequeued-but-unacked request is
        // re-delivered to the runner when it polls again.
        tx.execute("DELETE FROM session_active_requests", [])?;
        for (session_id, request_id) in &inner.session_active_requests {
            tx.execute(
                "INSERT INTO session_active_requests(session_id, active_request_id) VALUES (?1, ?2)",
                params![session_id, *request_id],
            )?;
        }
        // Persist per-session broker message queues.
        tx.execute("DELETE FROM broker_messages", [])?;
        for (session_id, messages) in &inner.inflight_messages {
            for (message_id, payload) in messages {
                let payload_json = serde_json::to_string(payload)?;
                tx.execute(
                    "INSERT INTO broker_messages(session_id, message_id, payload_json, written_at_us) VALUES (?1, ?2, ?3, ?4)",
                    params![session_id, *message_id, payload_json, now_us()],
                )?;
            }
        }

        let meta = MetaSnapshot {
            workflow_run_counters: inner.workflow_run_counters.clone(),
            next_runner_id: inner.next_runner_id,
            next_cache_id: inner.next_cache_id,
            next_message_id: inner.next_message_id,
            next_log_id: inner.next_log_id,
            next_artifact_v2_id: inner.next_artifact_v2_id,
            azdo_sessions: inner.azdo_sessions.clone(),
            oidc_job_contexts: inner
                .oidc_job_contexts
                .iter()
                .map(|((run_id, job_id), context)| (*run_id, job_id.clone(), context.clone()))
                .collect(),
            id_token_grants: inner
                .id_token_grants
                .iter()
                .map(|((run_id, job_id), granted)| (*run_id, job_id.clone(), *granted))
                .collect(),
            concurrency_groups: inner
                .concurrency_groups
                .iter()
                .map(|(key, group)| (key.clone(), group.clone()))
                .collect(),
            jobset_admissions: inner
                .jobset_admissions
                .iter()
                .map(|(key, admission)| (key.clone(), admission.clone()))
                .collect(),
            run_concurrency: inner
                .run_concurrency
                .iter()
                .map(|(run_id, config)| (*run_id, config.clone()))
                .collect(),
            holder_keys: inner
                .holder_keys
                .iter()
                .map(|(run_id, keys)| (*run_id, keys.clone()))
                .collect(),
            pending_caches: inner
                .pending_caches
                .iter()
                .map(|(id, cache)| (*id, cache.clone()))
                .collect(),
            artifacts: inner
                .artifacts
                .iter()
                .map(|(id, artifact)| (id.clone(), artifact.clone()))
                .collect(),
            log_metadata: inner
                .log_metadata
                .iter()
                .map(|(key, metadata)| (key.clone(), metadata.clone()))
                .collect(),
            timeline_events: inner
                .timeline_events
                .iter()
                .map(|(run_id, events)| (*run_id, events.clone()))
                .collect(),
            timeline_change_ids: inner
                .timeline_change_ids
                .iter()
                .map(|(key, change_id)| (key.clone(), *change_id))
                .collect(),
            timeline_records: inner
                .timeline_records
                .iter()
                .map(|(key, records)| {
                    (
                        key.clone(),
                        records
                            .iter()
                            .map(|(id, record)| (*id, record.clone()))
                            .collect(),
                    )
                })
                .collect(),
            cache_v2_pending: inner
                .cache_v2_pending
                .iter()
                .map(|(token, pending)| (token.clone(), pending.clone()))
                .collect(),
            cache_v2_dl_tokens: inner
                .cache_v2_dl_tokens
                .iter()
                .map(|(token, value)| (token.clone(), value.clone()))
                .collect(),
            artifact_v2_pending: inner
                .artifact_v2_pending
                .iter()
                .map(|(token, pending)| (token.clone(), pending.clone()))
                .collect(),
            artifact_v2_registry: inner
                .artifact_v2_registry
                .iter()
                .map(|(key, entry)| (key.clone(), entry.clone()))
                .collect(),
            github_token_requests: inner
                .github_token_requests
                .iter()
                .map(|(request_id, req)| (*request_id, req.clone()))
                .collect(),
            cancellation_queue: inner.cancellation_queue.clone(),
            broker_messages: inner
                .broker_messages
                .iter()
                .map(|(request_id, msg)| (*request_id, msg.clone()))
                .collect(),
            session_active_requests: inner
                .session_active_requests
                .iter()
                .map(|(session_id, request_id)| (session_id.clone(), *request_id))
                .collect(),
        };
        tx.execute(
            "INSERT INTO runtime_snapshots(snapshot_id, format_version, meta_blob, written_at_us)
             VALUES (1, ?1, ?2, ?3)
             ON CONFLICT(snapshot_id) DO UPDATE SET
               format_version = excluded.format_version,
               meta_blob = excluded.meta_blob,
               written_at_us = excluded.written_at_us",
            params![
                SNAPSHOT_FORMAT as i64,
                self.seal(&serde_json::to_vec(&meta)?)?,
                now_us()
            ],
        )?;
        tx.commit()
            .map_err(|error| anyhow::anyhow!("committing store snapshot: {error}"))?;
        Ok(())
    }

    /// Persist only one run's mutable projection. This is the hot path used
    /// after runner events; rebuilding every run on every status transition
    /// turns a burst of independent submissions into quadratic work.
    pub(crate) fn store_run_event(
        &self,
        inner: &InnerState,
        run_id: RunId,
        event: &NdjsonEvent,
    ) -> anyhow::Result<()> {
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let tx = connection.transaction()?;
        let Some(run) = inner.runs.get(&run_id) else {
            return Ok(());
        };
        self.store_run_tx(&tx, run)?;
        tx.execute("DELETE FROM jobs WHERE run_id = ?1", [run_id.to_string()])?;
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
                self.insert_job(&tx, job, kind, position)?;
                position += 1;
            }
        }
        if let Some(jobs) = inner.held_runs.get(&run_id) {
            for job in jobs {
                self.insert_job(&tx, job, "held", position)?;
                position += 1;
            }
        }
        tx.execute(
            "DELETE FROM job_requests WHERE run_id = ?1",
            [run_id.to_string()],
        )?;
        for record in inner
            .job_requests
            .values()
            .filter(|record| record.run_id == run_id)
        {
            self.insert_request_tx(&tx, record)?;
        }
        self.insert_event_tx(&tx, event)?;
        tx.commit()
            .map_err(|error| anyhow::anyhow!("committing run event: {error}"))?;
        Ok(())
    }

    /// Persist the run-number allocator without rewriting the encrypted
    /// runtime snapshot. Run submission is a hot path, and the full snapshot
    /// grows with the number of workflow paths.
    pub(crate) fn store_workflow_run_counter(
        &self,
        workflow_path: &str,
        next_run_number: u64,
    ) -> anyhow::Result<()> {
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let tx = connection.transaction()?;
        tx.execute(
            "INSERT INTO workflow_run_counters(repository_key, workflow_path, next_run_number)
             VALUES ('', ?1, ?2)
             ON CONFLICT(repository_key, workflow_path) DO UPDATE SET
               next_run_number = excluded.next_run_number",
            params![workflow_path, next_run_number as i64],
        )?;
        tx.commit()
            .map_err(|error| anyhow::anyhow!("committing workflow run counter: {error}"))?;
        Ok(())
    }

    pub(crate) fn store_meta_only(&self, inner: &InnerState) -> anyhow::Result<()> {
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let tx = connection.transaction()?;
        self.write_meta_tx(&tx, inner)?;
        tx.commit()
            .map_err(|error| anyhow::anyhow!("committing metadata: {error}"))?;
        Ok(())
    }

    fn store_run_tx(&self, tx: &Transaction<'_>, run: &RunRecord) -> anyhow::Result<()> {
        let mut value = serde_json::to_value(run)?;
        if let Some(object) = value.as_object_mut() {
            object.insert("submission".to_owned(), run.submission.to_request_json()?);
            object.insert(
                "job_needs".to_owned(),
                serde_json::to_value(&run.job_needs)?,
            );
        }
        tx.execute(
            "INSERT INTO runs(run_id, repository, workflow_path, status, run_number,
                              run_attempt, created_at_us, completed_at_us, record_blob)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(run_id) DO UPDATE SET
               repository = excluded.repository,
               workflow_path = excluded.workflow_path,
               status = excluded.status,
               run_number = excluded.run_number,
               run_attempt = excluded.run_attempt,
               created_at_us = excluded.created_at_us,
               completed_at_us = excluded.completed_at_us,
               record_blob = excluded.record_blob",
            params![
                run.run_id.to_string(),
                run.submission.repository.clone(),
                run.workflow_path_str.clone(),
                status_string(run.status),
                run.run_number as i64,
                run.run_attempt as i64,
                unix_us(run.created_at),
                run.completed_at.map(unix_us),
                self.seal(&serde_json::to_vec(&value)?)?,
            ],
        )?;
        tx.execute(
            "INSERT INTO run_secrets(run_id, secret_blob) VALUES (?1, ?2)
             ON CONFLICT(run_id) DO UPDATE SET secret_blob = excluded.secret_blob",
            params![
                run.run_id.to_string(),
                self.seal(&serde_json::to_vec(&run.submission.secrets)?)?
            ],
        )?;
        Ok(())
    }

    fn insert_request_tx(
        &self,
        tx: &Transaction<'_>,
        record: &TaskAgentJobRequestRecord,
    ) -> anyhow::Result<()> {
        let snapshot = RequestSnapshot {
            request_id: record.request_id,
            run_id: record.run_id,
            job_id: record.job_id.clone(),
            agent_job_id: record.agent_job_id,
            plan_id: record.plan_id.clone(),
            plan_type: record.plan_type.clone(),
            timeline_id: record.timeline_id,
            result: record.result,
            locked_until: record.locked_until.clone(),
            started_at_us: record.started_at.map(system_time_us),
            last_renewed_at_us: record.last_renewed_at.map(system_time_us),
            timeout_triggered: record.timeout_triggered,
            debug_token_issued: record.debug_token_issued,
        };
        tx.execute(
            "INSERT INTO job_requests(request_id, run_id, job_id, agent_job_id,
                                      plan_id, timeline_id, state, request_blob)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'active', ?7)",
            params![
                record.request_id,
                record.run_id.to_string(),
                record.job_id.0,
                record.agent_job_id.to_string(),
                record.plan_id,
                record.timeline_id.to_string(),
                self.seal(&serde_json::to_vec(&snapshot)?)?,
            ],
        )?;
        Ok(())
    }

    fn write_meta_tx(&self, tx: &Transaction<'_>, inner: &InnerState) -> anyhow::Result<()> {
        let meta = MetaSnapshot {
            workflow_run_counters: inner.workflow_run_counters.clone(),
            next_runner_id: inner.next_runner_id,
            next_cache_id: inner.next_cache_id,
            next_message_id: inner.next_message_id,
            next_log_id: inner.next_log_id,
            next_artifact_v2_id: inner.next_artifact_v2_id,
            azdo_sessions: inner.azdo_sessions.clone(),
            oidc_job_contexts: inner
                .oidc_job_contexts
                .iter()
                .map(|((run_id, job_id), context)| (*run_id, job_id.clone(), context.clone()))
                .collect(),
            id_token_grants: inner
                .id_token_grants
                .iter()
                .map(|((run_id, job_id), granted)| (*run_id, job_id.clone(), *granted))
                .collect(),
            concurrency_groups: inner
                .concurrency_groups
                .iter()
                .map(|(key, group)| (key.clone(), group.clone()))
                .collect(),
            jobset_admissions: inner
                .jobset_admissions
                .iter()
                .map(|(key, admission)| (key.clone(), admission.clone()))
                .collect(),
            run_concurrency: inner
                .run_concurrency
                .iter()
                .map(|(run_id, config)| (*run_id, config.clone()))
                .collect(),
            holder_keys: inner
                .holder_keys
                .iter()
                .map(|(run_id, keys)| (*run_id, keys.clone()))
                .collect(),
            pending_caches: inner
                .pending_caches
                .iter()
                .map(|(id, cache)| (*id, cache.clone()))
                .collect(),
            artifacts: inner
                .artifacts
                .iter()
                .map(|(id, artifact)| (id.clone(), artifact.clone()))
                .collect(),
            log_metadata: inner
                .log_metadata
                .iter()
                .map(|(key, metadata)| (key.clone(), metadata.clone()))
                .collect(),
            timeline_events: inner
                .timeline_events
                .iter()
                .map(|(run_id, events)| (*run_id, events.clone()))
                .collect(),
            timeline_change_ids: inner
                .timeline_change_ids
                .iter()
                .map(|(key, change_id)| (key.clone(), *change_id))
                .collect(),
            timeline_records: inner
                .timeline_records
                .iter()
                .map(|(key, records)| {
                    (
                        key.clone(),
                        records
                            .iter()
                            .map(|(id, record)| (*id, record.clone()))
                            .collect(),
                    )
                })
                .collect(),
            cache_v2_pending: inner
                .cache_v2_pending
                .iter()
                .map(|(token, pending)| (token.clone(), pending.clone()))
                .collect(),
            cache_v2_dl_tokens: inner
                .cache_v2_dl_tokens
                .iter()
                .map(|(token, value)| (token.clone(), value.clone()))
                .collect(),
            artifact_v2_pending: inner
                .artifact_v2_pending
                .iter()
                .map(|(token, pending)| (token.clone(), pending.clone()))
                .collect(),
            artifact_v2_registry: inner
                .artifact_v2_registry
                .iter()
                .map(|(key, entry)| (key.clone(), entry.clone()))
                .collect(),
            github_token_requests: inner
                .github_token_requests
                .iter()
                .map(|(request_id, req)| (*request_id, req.clone()))
                .collect(),
            cancellation_queue: inner.cancellation_queue.clone(),
            broker_messages: inner
                .broker_messages
                .iter()
                .map(|(request_id, msg)| (*request_id, msg.clone()))
                .collect(),
            session_active_requests: inner
                .session_active_requests
                .iter()
                .map(|(session_id, request_id)| (session_id.clone(), *request_id))
                .collect(),
        };
        tx.execute(
            "INSERT INTO runtime_snapshots(snapshot_id, format_version, meta_blob, written_at_us)
             VALUES (1, ?1, ?2, ?3)
             ON CONFLICT(snapshot_id) DO UPDATE SET
               format_version = excluded.format_version,
               meta_blob = excluded.meta_blob,
               written_at_us = excluded.written_at_us",
            params![
                SNAPSHOT_FORMAT as i64,
                self.seal(&serde_json::to_vec(&meta)?)?,
                now_us()
            ],
        )?;
        Ok(())
    }

    fn insert_job(
        &self,
        tx: &Transaction<'_>,
        job: &QueuedJob,
        queue_kind: &str,
        position: i64,
    ) -> anyhow::Result<()> {
        let payload = self.seal(&serde_json::to_vec(job)?)?;
        tx.execute(
            "INSERT INTO jobs(run_id, job_id, status, queue_kind, queue_position, payload_blob)
             VALUES (?1, ?2, 'queued', ?3, ?4, ?5)",
            params![
                job.run_id.to_string(),
                job.job_id.0,
                queue_kind,
                position,
                payload,
            ],
        )?;
        for (dependency, _) in job.needs.iter().enumerate() {
            let depends_on = &job.needs[dependency];
            tx.execute(
                "INSERT INTO job_dependencies(run_id, job_id, depends_on_job_id)
                 VALUES (?1, ?2, ?3)",
                params![job.run_id.to_string(), job.job_id.0, depends_on.0],
            )?;
        }
        Ok(())
    }

    pub(crate) fn append_event(&self, event: &NdjsonEvent) -> anyhow::Result<()> {
        let mut connection = self.connection.lock().expect("store mutex poisoned");
        let tx = connection.transaction()?;
        self.insert_event_tx(&tx, event)?;
        tx.commit()
            .map_err(|error| anyhow::anyhow!("committing control event: {error}"))?;
        Ok(())
    }

    fn insert_event_tx(&self, tx: &Transaction<'_>, event: &NdjsonEvent) -> anyhow::Result<()> {
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
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                run_id.to_string(),
                job_id.map(|id| id.0),
                event_type,
                payload,
                now_us()
            ],
        )?;
        Ok(())
    }

    fn encrypt_sealed(&self, plaintext: &[u8]) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let keys = derive_keys(&self.key);
        let cipher = SessionEncryption::from_key(keys.aead.to_vec());
        let (ciphertext, iv) = cipher
            .encrypt(plaintext)
            .expect("AES-256-CBC encrypt is infallible for in-spec inputs");
        let mut mac = HmacSha256::new_from_slice(&keys.mac).expect("HMAC accepts any key length");
        mac.update(MIGRATION_DOMAIN);
        mac.update(&iv);
        mac.update(&ciphertext);
        (ciphertext, iv, mac.finalize().into_bytes().to_vec())
    }

    fn decrypt_sealed(&self, ciphertext: &[u8], iv: &[u8], tag: &[u8]) -> anyhow::Result<Vec<u8>> {
        let keys = derive_keys(&self.key);
        let mut mac = HmacSha256::new_from_slice(&keys.mac).expect("HMAC accepts any key length");
        mac.update(MIGRATION_DOMAIN);
        mac.update(iv);
        mac.update(ciphertext);
        mac.verify_slice(tag)
            .map_err(|_| anyhow::anyhow!("store session-key envelope authentication failed"))?;
        SessionEncryption::from_key(keys.aead.to_vec())
            .decrypt(ciphertext, iv)
            .map_err(|error| anyhow::anyhow!("store session-key decryption failed: {error}"))
    }

    fn seal(&self, plaintext: &[u8]) -> anyhow::Result<Vec<u8>> {
        let (ciphertext, iv, tag) = self.encrypt_sealed(plaintext);
        let mut sealed = Vec::with_capacity(1 + iv.len() + ciphertext.len() + tag.len());
        sealed.push(SNAPSHOT_FORMAT);
        sealed.extend_from_slice(&iv);
        sealed.extend_from_slice(&ciphertext);
        sealed.extend_from_slice(&tag);
        Ok(sealed)
    }

    fn unseal(&self, sealed: &[u8]) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(sealed.len() >= 1 + 16 + 32, "invalid store envelope");
        let version = sealed[0];
        if version != SNAPSHOT_FORMAT {
            // Old envelopes (v1) used a different key derivation (raw key or
            // SHA-256 of env input) and an unauthenticated AES-CBC layer.
            // We don't try to decrypt them — the user must drop the state
            // directory to start fresh. This is a hard cut-over; see the
            // migration notes in the architecture doc.
            anyhow::bail!(
                "unsupported store envelope version {version}; current version is {SNAPSHOT_FORMAT}"
            );
        }
        let iv = &sealed[1..17];
        let tag_start = sealed.len() - 32;
        let ciphertext = &sealed[17..tag_start];
        let tag = &sealed[tag_start..];
        self.decrypt_sealed(ciphertext, iv, tag)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SessionKeyPayload(Vec<u8>);

const MIGRATIONS: &[(u32, &str, &str)] = &[(
    1,
    "initial-control-plane-schema",
    r#"
        CREATE TABLE IF NOT EXISTS workflow_run_counters (
          repository_key TEXT NOT NULL,
          workflow_path TEXT NOT NULL,
          next_run_number INTEGER NOT NULL CHECK (next_run_number >= 1),
          PRIMARY KEY (repository_key, workflow_path)
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS runs (
          run_id TEXT PRIMARY KEY,
          repository TEXT NOT NULL,
          workflow_path TEXT NOT NULL,
          status TEXT NOT NULL,
          run_number INTEGER NOT NULL,
          run_attempt INTEGER NOT NULL,
          created_at_us INTEGER NOT NULL,
          completed_at_us INTEGER,
          record_blob BLOB NOT NULL
        ) STRICT;

        CREATE TABLE IF NOT EXISTS run_secrets (
          run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
          crypto_version INTEGER NOT NULL DEFAULT 1,
          secret_blob BLOB NOT NULL
        ) STRICT;

        CREATE TABLE IF NOT EXISTS runners (
          runner_id INTEGER PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          ephemeral INTEGER NOT NULL CHECK (ephemeral IN (0,1)),
          runner_group_id INTEGER,
          runner_group_name TEXT,
          public_key TEXT,
          rsa_public_key TEXT,
          created_at_us INTEGER NOT NULL,
          updated_at_us INTEGER NOT NULL,
          deleted_at_us INTEGER
        ) STRICT;

        CREATE TABLE IF NOT EXISTS runner_labels (
          runner_id INTEGER NOT NULL REFERENCES runners(runner_id) ON DELETE CASCADE,
          label TEXT NOT NULL,
          ordinal INTEGER NOT NULL,
          PRIMARY KEY (runner_id, label)
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS runner_sessions (
          session_id TEXT PRIMARY KEY,
          runner_id INTEGER NOT NULL,
          protocol TEXT NOT NULL,
          client_id TEXT,
          session_key_blob BLOB,
          session_iv BLOB,
          session_tag BLOB,
          created_at_us INTEGER NOT NULL,
          last_seen_at_us INTEGER NOT NULL,
          closed_at_us INTEGER
        ) STRICT;

        CREATE TABLE IF NOT EXISTS jobs (
          run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
          job_id TEXT NOT NULL,
          status TEXT NOT NULL,
          queue_kind TEXT NOT NULL CHECK (
            queue_kind IN ('ready','pending','blocked','held')
          ),
          queue_position INTEGER NOT NULL,
          payload_blob BLOB NOT NULL,
          PRIMARY KEY (run_id, job_id)
        ) STRICT, WITHOUT ROWID;

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
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS job_requests (
          request_id INTEGER PRIMARY KEY,
          run_id TEXT NOT NULL,
          job_id TEXT NOT NULL,
          agent_job_id TEXT NOT NULL UNIQUE,
          plan_id TEXT NOT NULL UNIQUE,
          timeline_id TEXT NOT NULL UNIQUE,
          state TEXT NOT NULL,
          request_blob BLOB NOT NULL,
          FOREIGN KEY (run_id)
            REFERENCES runs(run_id) ON DELETE CASCADE
        ) STRICT;

        CREATE TABLE IF NOT EXISTS runner_commands (
          message_id INTEGER PRIMARY KEY,
          command_type TEXT NOT NULL,
          run_id TEXT NOT NULL,
          job_id TEXT NOT NULL,
          payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
          state TEXT NOT NULL,
          created_at_us INTEGER NOT NULL
        ) STRICT;

        CREATE TABLE IF NOT EXISTS control_events (
          event_id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id TEXT NOT NULL,
          job_id TEXT,
          event_type TEXT NOT NULL,
          payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
          created_at_us INTEGER NOT NULL
        ) STRICT;

        CREATE INDEX IF NOT EXISTS control_events_cursor_idx
          ON control_events (run_id, event_id);

        CREATE TABLE IF NOT EXISTS session_active_requests (
          session_id TEXT PRIMARY KEY,
          active_request_id INTEGER NOT NULL
            REFERENCES job_requests(request_id) ON DELETE CASCADE
        ) STRICT;

        CREATE TABLE IF NOT EXISTS broker_messages (
          session_id TEXT NOT NULL,
          message_id INTEGER NOT NULL,
          payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
          written_at_us INTEGER NOT NULL,
          PRIMARY KEY (session_id, message_id)
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS log_files (
          log_key TEXT PRIMARY KEY,
          byte_count INTEGER NOT NULL DEFAULT 0 CHECK (byte_count >= 0),
          line_count INTEGER NOT NULL DEFAULT 0 CHECK (line_count >= 0),
          updated_at_us INTEGER NOT NULL
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS log_chunks (
          log_key TEXT NOT NULL REFERENCES log_files(log_key) ON DELETE CASCADE,
          chunk_index INTEGER NOT NULL,
          payload BLOB NOT NULL,
          written_at_us INTEGER NOT NULL,
          PRIMARY KEY (log_key, chunk_index)
        ) STRICT, WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS runtime_snapshots (
          snapshot_id INTEGER PRIMARY KEY CHECK (snapshot_id = 1),
          format_version INTEGER NOT NULL,
          meta_blob BLOB NOT NULL,
          written_at_us INTEGER NOT NULL
        ) STRICT;
        "#,
)];

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(i64::MAX as u128) as i64
}

fn unix_us(value: chrono::DateTime<chrono::Utc>) -> i64 {
    value.timestamp_micros()
}

fn system_time_us(value: SystemTime) -> i64 {
    value
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(i64::MAX as u128) as i64
}

fn system_time_from_us(value: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(value.max(0) as u64)
}

/// Deduplicate label strings case-insensitively, preserving first occurrence.
///
/// The handler layer already runs this when a runner registers; this is the
/// database-side backstop so a direct mutation of `inner.runners[id].labels`
/// from another code path still produces a valid `(runner_id, label)` insert.
/// Matches the case-insensitive semantics of
/// `runtime_scheduling::job_matches_runner`.
pub(crate) fn dedupe_labels_ci(labels: &[String]) -> Vec<String> {
    let mut seen: std::collections::HashSet<String> =
        std::collections::HashSet::with_capacity(labels.len());
    let mut out: Vec<String> = Vec::with_capacity(labels.len());
    for label in labels {
        if seen.insert(label.to_lowercase()) {
            out.push(label.clone());
        }
    }
    out
}
