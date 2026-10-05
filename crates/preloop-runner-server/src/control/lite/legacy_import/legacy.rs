//! The legacy v11 durable-state store: schema expectations, sealed-blob
//! decoding, and read-only row readers for the one-time import into the
//! control schema.
//!
//! Before the control backend, the server persisted `<state_dir>/preloop.db`
//! with `PRAGMA user_version = 11` (12 once the message-payload marker
//! landed), a `schema_migrations` audit table, and sealed
//! `version || iv || ciphertext || hmac` JSON blobs for runs, queued jobs,
//! attempts, step names, and the runtime metadata snapshot.
//!
//! Nothing in this module writes. The source connection is opened
//! `SQLITE_OPEN_READ_ONLY` + `query_only`, and the importer hashes the file
//! before and after to prove it did not change.

use crate::models::{QueuedJob, RunRecord, StepKind, StepRecord};
use crate::store::Envelope;
use anyhow::{Context, bail};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::collections::BTreeSet;
use std::path::Path;

/// The last released legacy schema version (v11) and the follow-up marker
/// version (v12) that only records which message payloads were re-sealed.
/// Both carry the same table shapes; v12 is importable too.
pub(crate) const LEGACY_VERSION: i64 = 11;
pub(crate) const LEGACY_VERSION_PAYLOAD_MARKER: i64 = 12;

/// The audit rows a v11/v12 `schema_migrations` table must carry. The legacy
/// store stamped `PRAGMA user_version` with the applied version, so this list
/// is a second, independent fingerprint of the source format.
pub(crate) const LEGACY_MIGRATIONS: &[(i64, &str)] = &[
    (1, "initial-control-plane-schema"),
    (2, "drop-redundant-run-secrets"),
    (3, "job-request-messages-table"),
    (4, "job-steps-table"),
    (5, "runtime-snapshot-revision"),
    (6, "webhook-deliveries-table"),
    (7, "webhook-delivery-lease-fencing"),
    (8, "webhook-run-reservation"),
    (9, "webhook-delivery-repair-state"),
    (10, "drop-source-state-reconciler"),
    (11, "webhook-watchdog-safe-pagination"),
];

/// The v12-only migration marker. Absent on a v11 database.
pub(crate) const LEGACY_PAYLOAD_MARKER: (i64, &str) = (12, "message-payload-migration-marker");

/// Every table a legacy v11/v12 store owns. A source missing one of these is
/// not a v11 store (or is a truncated copy) and is refused before any read.
pub(crate) const LEGACY_TABLES: &[&str] = &[
    "broker_messages",
    "control_events",
    "job_dependencies",
    "job_request_messages",
    "job_requests",
    "job_steps",
    "jobs",
    "log_chunks",
    "log_files",
    "runner_commands",
    "runner_labels",
    "runner_sessions",
    "runners",
    "runs",
    "runtime_snapshots",
    "schema_migrations",
    "session_active_requests",
    "webhook_deliveries",
    "webhook_redeliveries",
    "webhook_watchdog",
    "workflow_run_counters",
];

/// The legacy `format_version` written into `runtime_snapshots`. Sealed blobs
/// are self-describing (envelope version byte), but the snapshot row carries
/// its own format tag; both must agree with the envelope the importer holds.
pub(crate) const LEGACY_SNAPSHOT_FORMAT: i64 = 2;

/// One `runs` row plus its decoded record.
pub(crate) struct LegacyRun {
    pub(crate) run_id: String,
    pub(crate) repository: String,
    pub(crate) workflow_path: String,
    pub(crate) record: RunRecord,
}

/// One `jobs` row plus its decoded payload. `queue_kind` is the legacy
/// scheduling classification (`ready` / `pending` / `blocked` / `held`).
pub(crate) struct LegacyQueuedJob {
    pub(crate) run_id: String,
    pub(crate) status: String,
    pub(crate) queue_kind: String,
    pub(crate) queue_position: i64,
    pub(crate) job: QueuedJob,
}

/// One `job_requests` row plus its decoded snapshot.
pub(crate) struct LegacyAttempt {
    pub(crate) request_id: i64,
    pub(crate) run_id: String,
    pub(crate) job_id: String,
    pub(crate) agent_job_id: String,
    pub(crate) snapshot: LegacyRequestSnapshot,
}

/// One `job_steps` row with its unsealed name.
pub(crate) struct LegacyStep {
    pub(crate) run_id: String,
    pub(crate) agent_job_id: String,
    pub(crate) step_id: String,
    pub(crate) kind: StepKind,
    pub(crate) workflow_index: Option<u64>,
    pub(crate) runner_number: Option<u32>,
    pub(crate) context_name: Option<String>,
    pub(crate) name: String,
    pub(crate) conclusion: String,
    pub(crate) started_at_us: Option<i64>,
    pub(crate) finished_at_us: Option<i64>,
}

impl LegacyStep {
    /// The legacy load order (`--step` ordering): runner position, then
    /// declared workflow position, then the step id as a stable tie-break.
    pub(crate) fn sort_key(&self) -> (i64, i64, String) {
        (
            self.runner_number.map(i64::from).unwrap_or(i64::MAX),
            self.workflow_index
                .map(|index| index as i64)
                .unwrap_or(i64::MAX),
            self.step_id.clone(),
        )
    }

    pub(crate) fn step_record(&self) -> StepRecord {
        StepRecord {
            id: self.step_id.clone(),
            kind: self.kind,
            workflow_index: self.workflow_index.map(|index| index as usize),
            runner_number: self.runner_number,
            context_name: self.context_name.clone(),
            name: self.name.clone(),
            conclusion: self.conclusion.clone(),
            started_at: self
                .started_at_us
                .and_then(chrono::DateTime::from_timestamp_micros),
            finished_at: self
                .finished_at_us
                .and_then(chrono::DateTime::from_timestamp_micros),
        }
    }
}

/// One `log_files` row with its (optional) sealed chunk payloads in order.
pub(crate) struct LegacyLogFile {
    pub(crate) log_key: String,
    pub(crate) byte_count: i64,
    pub(crate) line_count: i64,
    pub(crate) updated_at_us: i64,
    pub(crate) chunks: Vec<Vec<u8>>,
}

/// One legacy `control_events` row (the durable run/job status event log).
pub(crate) struct LegacyControlEvent {
    pub(crate) event_id: i64,
    pub(crate) run_id: String,
    pub(crate) job_id: Option<String>,
    pub(crate) event_type: String,
    pub(crate) payload_json: String,
    pub(crate) created_at_us: i64,
}

/// One `runners` row plus its ordered labels.
pub(crate) struct LegacyRunner {
    pub(crate) runner_id: i64,
    pub(crate) name: String,
    pub(crate) ephemeral: bool,
    pub(crate) runner_group_id: Option<i64>,
    pub(crate) runner_group_name: Option<String>,
    pub(crate) public_key: Option<String>,
    pub(crate) rsa_public_key: Option<String>,
    pub(crate) created_at_us: i64,
    pub(crate) updated_at_us: i64,
    pub(crate) deleted_at_us: Option<i64>,
    pub(crate) labels: Vec<String>,
}

/// One `runner_sessions` row. The session key material is deliberately not
/// read: the control backend derives session keys from the cluster key and
/// never stores them, so legacy key blobs are unrecoverable by design.
pub(crate) struct LegacySession {
    pub(crate) session_id: String,
    pub(crate) runner_id: i64,
    pub(crate) protocol: String,
    pub(crate) client_id: Option<String>,
    pub(crate) created_at_us: i64,
    pub(crate) last_seen_at_us: i64,
    pub(crate) closed_at_us: Option<i64>,
}

/// One webhook delivery row with its unsealed payload bytes.
pub(crate) struct LegacyWebhookDelivery {
    pub(crate) delivery_id: String,
    pub(crate) event: String,
    pub(crate) payload: Vec<u8>,
    pub(crate) received_at_us: i64,
    pub(crate) state: String,
    pub(crate) attempts: i64,
    pub(crate) lease_until_us: Option<i64>,
    pub(crate) lease_token: Option<String>,
    pub(crate) last_error: Option<String>,
}

/// One webhook watchdog cursor row.
pub(crate) struct LegacyWebhookWatchdog {
    pub(crate) scope: String,
    pub(crate) cursor_delivered_at_us: Option<i64>,
    pub(crate) cursor_delivered_at_guid: Option<String>,
    pub(crate) scan_cursor: Option<String>,
    pub(crate) last_poll_at_us: Option<i64>,
    pub(crate) last_success_at_us: Option<i64>,
}

/// One webhook redelivery row.
pub(crate) struct LegacyWebhookRedelivery {
    pub(crate) delivery_guid: String,
    pub(crate) github_delivery_id: i64,
    pub(crate) app_id: String,
    pub(crate) reason: String,
    pub(crate) attempts: i64,
    pub(crate) first_seen_at_us: i64,
    pub(crate) last_attempt_at_us: Option<i64>,
    pub(crate) resolved_at_us: Option<i64>,
    pub(crate) last_error: Option<String>,
}

/// Header facts checked before any state is read.
pub(crate) struct LegacyHeader {
    pub(crate) user_version: i64,
    pub(crate) migrations: Vec<(i64, String)>,
    pub(crate) tables: BTreeSet<String>,
    pub(crate) snapshot_format: Option<i64>,
}

/// Row counts for the families the importer restores or reports.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct LegacyCounts {
    pub(crate) runs: u64,
    pub(crate) jobs: u64,
    pub(crate) job_requests: u64,
    pub(crate) job_steps: u64,
    pub(crate) log_files: u64,
    pub(crate) log_chunks: u64,
    pub(crate) runners: u64,
    pub(crate) runner_labels: u64,
    pub(crate) runner_sessions: u64,
    pub(crate) webhook_deliveries: u64,
    pub(crate) webhook_watchdog: u64,
    pub(crate) webhook_redeliveries: u64,
    pub(crate) workflow_run_counters: u64,
    pub(crate) runner_commands: u64,
    pub(crate) control_events: u64,
    pub(crate) broker_messages: u64,
    pub(crate) job_request_messages: u64,
    pub(crate) session_active_requests: u64,
    pub(crate) job_dependencies: u64,
}

/// A read-only handle on the legacy database.
pub(crate) struct LegacyDb {
    conn: Connection,
}

impl LegacyDb {
    /// Open the source read-only. A live server is not a valid source: the
    /// importer refuses to read a database it cannot snapshot consistently
    /// (the caller stops the server first), and `query_only` makes an
    /// accidental write through this handle a hard error.
    pub(crate) fn open(path: &Path) -> anyhow::Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .with_context(|| {
            format!(
                "open legacy database {} read-only (stop the server first; a WAL that still \
                 needs recovery cannot be read read-only — checkpoint it with \
                 `sqlite3 {} 'PRAGMA wal_checkpoint(TRUNCATE)'`)",
                path.display(),
                path.display()
            )
        })?;
        conn.pragma_update(None, "query_only", true)
            .context("set query_only on legacy database")?;
        conn.pragma_update(None, "busy_timeout", 5000_i64)
            .context("set busy_timeout on legacy database")?;
        Ok(Self { conn })
    }

    /// Verify the source really is a v11/v12 legacy store. Fails closed with
    /// an explicit reason; the importer never guesses at a foreign format.
    pub(crate) fn header(&self) -> anyhow::Result<LegacyHeader> {
        let user_version: i64 = self.conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if user_version != LEGACY_VERSION && user_version != LEGACY_VERSION_PAYLOAD_MARKER {
            bail!(
                "legacy database has PRAGMA user_version={user_version}; this importer \
                 supports only the released v{LEGACY_VERSION} store \
                 (v{LEGACY_VERSION_PAYLOAD_MARKER} adds only the payload-migration marker). \
                 Refusing to guess at an unrecognized source format."
            );
        }
        let mut stmt = self
            .conn
            .prepare("SELECT version, name FROM schema_migrations ORDER BY version")?;
        let migrations: Vec<(i64, String)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<_, _>>()?;
        let mut tables = BTreeSet::new();
        let mut table_stmt = self
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")?;
        for row in table_stmt.query_map([], |row| row.get::<_, String>(0))? {
            tables.insert(row?);
        }
        let missing: Vec<&str> = LEGACY_TABLES
            .iter()
            .copied()
            .filter(|table| !tables.contains(*table))
            .collect();
        if !missing.is_empty() {
            bail!(
                "legacy database is missing tables {missing:?}; it is not a complete v{user_version} store"
            );
        }
        // The audit table must match the table chain for the claimed version;
        // a hand-renamed or half-migrated copy is refused.
        for (version, name) in LEGACY_MIGRATIONS {
            let found = migrations
                .iter()
                .any(|(v, n)| v == version && n == name);
            if !found {
                bail!(
                    "legacy schema_migrations is missing v{version} ({name:?}); \
                     this is not a recognized v{LEGACY_VERSION} store"
                );
            }
        }
        let expects_marker = user_version >= LEGACY_VERSION_PAYLOAD_MARKER;
        let has_marker = migrations.iter().any(|(version, name)| {
            *version == LEGACY_PAYLOAD_MARKER.0 && name == LEGACY_PAYLOAD_MARKER.1
        });
        if expects_marker != has_marker {
            bail!(
                "legacy database user_version={user_version} disagrees with its \
                 message-payload marker row (version {}); refusing a mismatched store",
                LEGACY_PAYLOAD_MARKER.0
            );
        }
        let snapshot_format: Option<i64> = self
            .conn
            .query_row(
                "SELECT format_version FROM runtime_snapshots WHERE snapshot_id = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(format) = snapshot_format
            && format != LEGACY_SNAPSHOT_FORMAT
        {
            bail!(
                "legacy runtime snapshot has format_version {format}; this importer supports \
                 only {LEGACY_SNAPSHOT_FORMAT}"
            );
        }
        Ok(LegacyHeader {
            user_version,
            migrations,
            tables,
            snapshot_format,
        })
    }

    pub(crate) fn counts(&self) -> anyhow::Result<LegacyCounts> {
        let one = |table: &str| -> anyhow::Result<u64> {
            let count: i64 = self
                .conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .with_context(|| format!("count legacy table {table}"))?;
            Ok(count.max(0) as u64)
        };
        Ok(LegacyCounts {
            runs: one("runs")?,
            jobs: one("jobs")?,
            job_requests: one("job_requests")?,
            job_steps: one("job_steps")?,
            log_files: one("log_files")?,
            log_chunks: one("log_chunks")?,
            runners: one("runners")?,
            runner_labels: one("runner_labels")?,
            runner_sessions: one("runner_sessions")?,
            webhook_deliveries: one("webhook_deliveries")?,
            webhook_watchdog: one("webhook_watchdog")?,
            webhook_redeliveries: one("webhook_redeliveries")?,
            workflow_run_counters: one("workflow_run_counters")?,
            runner_commands: one("runner_commands")?,
            control_events: one("control_events")?,
            broker_messages: one("broker_messages")?,
            job_request_messages: one("job_request_messages")?,
            session_active_requests: one("session_active_requests")?,
            job_dependencies: one("job_dependencies")?,
        })
    }

    /// `PRAGMA data_version` — changes when another connection commits. The
    /// source must be quiescent for the whole import.
    pub(crate) fn data_version(&self) -> anyhow::Result<i64> {
        Ok(self
            .conn
            .pragma_query_value(None, "data_version", |row| row.get(0))?)
    }

    pub(crate) fn runs(&self, cipher: &Envelope) -> anyhow::Result<Vec<LegacyRun>> {
        let mut stmt = self.conn.prepare(
            "SELECT run_id, repository, workflow_path, record_blob FROM runs ORDER BY created_at_us, run_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Vec<u8>>(3)?,
            ))
        })?;
        let mut runs = Vec::new();
        for row in rows {
            let (run_id, repository, workflow_path, blob) = row?;
            let record = restore_run_record(cipher, &blob)
                .with_context(|| format!("decode legacy run {run_id}"))?;
            if record.run_id.0.to_string() != run_id {
                bail!(
                    "legacy run row {run_id} carries a record for {}; refusing a \
                     referentially inconsistent source",
                    record.run_id.0
                );
            }
            if record.workflow_path_str != workflow_path {
                bail!(
                    "legacy run {run_id} workflow path disagrees between its row \
                     ({workflow_path:?}) and its record ({:?})",
                    record.workflow_path_str
                );
            }
            if record.submission.repository != repository {
                bail!(
                    "legacy run {run_id} repository disagrees between its row ({repository:?}) \
                     and its record ({:?})",
                    record.submission.repository
                );
            }
            runs.push(LegacyRun {
                run_id,
                repository,
                workflow_path,
                record,
            });
        }
        Ok(runs)
    }

    pub(crate) fn queued_jobs(&self, cipher: &Envelope) -> anyhow::Result<Vec<LegacyQueuedJob>> {
        let mut stmt = self.conn.prepare(
            "SELECT run_id, job_id, status, queue_kind, queue_position, payload_blob \
             FROM jobs ORDER BY queue_kind, queue_position",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, Vec<u8>>(5)?,
            ))
        })?;
        let mut jobs = Vec::new();
        for row in rows {
            let (run_id, job_id, status, queue_kind, queue_position, blob) = row?;
            let job: QueuedJob = serde_json::from_slice(&cipher.unseal(&blob)?)
                .with_context(|| format!("decode legacy job {run_id}/{job_id}"))?;
            if job.run_id.0.to_string() != run_id || job.job_id.0 != job_id {
                bail!(
                    "legacy job row {run_id}/{job_id} carries a payload for \
                     {}/{}; refusing a referentially inconsistent source",
                    job.run_id.0,
                    job.job_id.0
                );
            }
            jobs.push(LegacyQueuedJob {
                run_id,
                status,
                queue_kind,
                queue_position,
                job,
            });
        }
        Ok(jobs)
    }

    pub(crate) fn attempts(&self, cipher: &Envelope) -> anyhow::Result<Vec<LegacyAttempt>> {
        let mut stmt = self.conn.prepare(
            "SELECT request_id, run_id, job_id, agent_job_id, request_blob \
             FROM job_requests ORDER BY request_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Vec<u8>>(4)?,
            ))
        })?;
        let mut attempts = Vec::new();
        for row in rows {
            let (request_id, run_id, job_id, agent_job_id, blob) = row?;
            let snapshot: LegacyRequestSnapshot = serde_json::from_slice(&cipher.unseal(&blob)?)
                .with_context(|| format!("decode legacy job request {request_id}"))?;
            if snapshot.request_id != request_id
                || snapshot.run_id.0.to_string() != run_id
                || snapshot.job_id.0 != job_id
                || snapshot.agent_job_id.to_string() != agent_job_id
            {
                bail!(
                    "legacy job request {request_id} does not match its row \
                     ({run_id}/{job_id}/{agent_job_id}); refusing a referentially \
                     inconsistent source"
                );
            }
            attempts.push(LegacyAttempt {
                request_id,
                run_id,
                job_id,
                agent_job_id,
                snapshot,
            });
        }
        Ok(attempts)
    }

    pub(crate) fn steps(&self, cipher: &Envelope) -> anyhow::Result<Vec<LegacyStep>> {
        let mut stmt = self.conn.prepare(
            "SELECT run_id, agent_job_id, step_id, kind, workflow_index, runner_number, \
                    context_name, name_blob, conclusion, started_at_us, finished_at_us \
             FROM job_steps",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Vec<u8>>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, Option<i64>>(9)?,
                row.get::<_, Option<i64>>(10)?,
            ))
        })?;
        let mut steps = Vec::new();
        for row in rows {
            let (
                run_id,
                agent_job_id,
                step_id,
                kind,
                workflow_index,
                runner_number,
                context_name,
                name_blob,
                conclusion,
                started_at_us,
                finished_at_us,
            ) = row?;
            let kind = match kind.as_str() {
                "workflow" => StepKind::Workflow,
                "synthetic" => StepKind::Synthetic,
                other => bail!(
                    "legacy step {agent_job_id}/{step_id} has unknown kind {other:?}; \
                     refusing an unrecognized source"
                ),
            };
            let name = String::from_utf8(cipher.unseal(&name_blob)?)
                .with_context(|| format!("legacy step name {agent_job_id}/{step_id} is not UTF-8"))?;
            steps.push(LegacyStep {
                run_id,
                agent_job_id,
                step_id,
                kind,
                workflow_index: workflow_index.map(|index| index.max(0) as u64),
                runner_number: runner_number.map(|number| number.max(0) as u32),
                context_name,
                name,
                conclusion,
                started_at_us,
                finished_at_us,
            });
        }
        Ok(steps)
    }

    pub(crate) fn log_files(&self, _cipher: &Envelope) -> anyhow::Result<Vec<LegacyLogFile>> {
        let mut stmt = self.conn.prepare(
            "SELECT log_key, byte_count, line_count, updated_at_us FROM log_files ORDER BY log_key",
        )?;
        let files: Vec<(String, i64, i64, i64)> = stmt
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?
            .collect::<Result<_, _>>()?;
        let mut chunk_stmt = self.conn.prepare(
            "SELECT payload FROM log_chunks WHERE log_key = ?1 ORDER BY chunk_index",
        )?;
        let mut out = Vec::with_capacity(files.len());
        for (log_key, byte_count, line_count, updated_at_us) in files {
            // Legacy log chunks are stored raw (masked by the HTTP layer
            // before `store_log_chunk`), not sealed like the JSON blobs.
            let chunks: Vec<Vec<u8>> = chunk_stmt
                .query_map([&log_key], |row| row.get::<_, Vec<u8>>(0))?
                .collect::<Result<_, _>>()?;
            out.push(LegacyLogFile {
                log_key,
                byte_count,
                line_count,
                updated_at_us,
                chunks,
            });
        }
        Ok(out)
    }

    pub(crate) fn runners(&self) -> anyhow::Result<Vec<LegacyRunner>> {
        let mut stmt = self.conn.prepare(
            "SELECT runner_id, name, ephemeral, runner_group_id, runner_group_name, \
                    public_key, rsa_public_key, created_at_us, updated_at_us, deleted_at_us \
             FROM runners ORDER BY runner_id",
        )?;
        let mut runners: Vec<LegacyRunner> = stmt
            .query_map([], |row| {
                Ok(LegacyRunner {
                    runner_id: row.get(0)?,
                    name: row.get(1)?,
                    ephemeral: row.get::<_, i64>(2)? != 0,
                    runner_group_id: row.get(3)?,
                    runner_group_name: row.get(4)?,
                    public_key: row.get(5)?,
                    rsa_public_key: row.get(6)?,
                    created_at_us: row.get(7)?,
                    updated_at_us: row.get(8)?,
                    deleted_at_us: row.get(9)?,
                    labels: Vec::new(),
                })
            })?
            .collect::<Result<_, _>>()?;
        let mut label_stmt = self.conn.prepare(
            "SELECT label FROM runner_labels WHERE runner_id = ?1 ORDER BY ordinal, label",
        )?;
        for runner in &mut runners {
            runner.labels = label_stmt
                .query_map([runner.runner_id], |row| row.get::<_, String>(0))?
                .collect::<Result<_, _>>()?;
        }
        Ok(runners)
    }

    pub(crate) fn sessions(&self) -> anyhow::Result<Vec<LegacySession>> {
        let mut stmt = self.conn.prepare(
            "SELECT session_id, runner_id, protocol, client_id, created_at_us, \
                    last_seen_at_us, closed_at_us \
             FROM runner_sessions ORDER BY created_at_us, session_id",
        )?;
        let sessions = stmt
            .query_map([], |row| {
                Ok(LegacySession {
                    session_id: row.get(0)?,
                    runner_id: row.get(1)?,
                    protocol: row.get(2)?,
                    client_id: row.get(3)?,
                    created_at_us: row.get(4)?,
                    last_seen_at_us: row.get(5)?,
                    closed_at_us: row.get(6)?,
                })
            })?
            .collect::<Result<_, _>>()?;
        Ok(sessions)
    }

    pub(crate) fn session_active_requests(&self) -> anyhow::Result<Vec<(String, i64)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT session_id, active_request_id FROM session_active_requests")?;
        let rows = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    }

    /// `(run_id, job_id, depends_on_job_id)` rows, ordered.
    pub(crate) fn job_dependencies(&self) -> anyhow::Result<Vec<(String, String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT run_id, job_id, depends_on_job_id FROM job_dependencies \
             ORDER BY run_id, job_id, depends_on_job_id",
        )?;
        let rows = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    }

    /// `(run_id, job_id, event_type, payload_json, created_at_us)` rows, in
    /// append order.
    pub(crate) fn control_events(&self) -> anyhow::Result<Vec<LegacyControlEvent>> {
        let mut stmt = self.conn.prepare(
            "SELECT event_id, run_id, job_id, event_type, payload_json, created_at_us \
             FROM control_events ORDER BY event_id",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(LegacyControlEvent {
                    event_id: row.get(0)?,
                    run_id: row.get(1)?,
                    job_id: row.get(2)?,
                    event_type: row.get(3)?,
                    payload_json: row.get(4)?,
                    created_at_us: row.get(5)?,
                })
            })?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    }

    /// `(repository_key, workflow_path, next_run_number)` rows.
    pub(crate) fn workflow_run_counters(&self) -> anyhow::Result<Vec<(String, String, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT repository_key, workflow_path, next_run_number FROM workflow_run_counters",
        )?;
        let rows = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    }

    pub(crate) fn webhook_deliveries(
        &self,
        cipher: &Envelope,
    ) -> anyhow::Result<Vec<LegacyWebhookDelivery>> {
        let mut stmt = self.conn.prepare(
            "SELECT delivery_id, event, payload_blob, received_at_us, state, attempts, \
                    lease_until_us, lease_token, last_error FROM webhook_deliveries",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
            ))
        })?;
        let mut deliveries = Vec::new();
        for row in rows {
            let (
                delivery_id,
                event,
                blob,
                received_at_us,
                state,
                attempts,
                lease_until_us,
                lease_token,
                last_error,
            ) = row?;
            let payload = cipher
                .unseal(&blob)
                .with_context(|| format!("decode legacy webhook delivery {delivery_id}"))?;
            deliveries.push(LegacyWebhookDelivery {
                delivery_id,
                event,
                payload,
                received_at_us,
                state,
                attempts,
                lease_until_us,
                lease_token,
                last_error,
            });
        }
        Ok(deliveries)
    }

    pub(crate) fn webhook_watchdog(&self) -> anyhow::Result<Vec<LegacyWebhookWatchdog>> {
        let mut stmt = self.conn.prepare(
            "SELECT scope, cursor_delivered_at_us, cursor_delivered_at_guid, scan_cursor, \
                    last_poll_at_us, last_success_at_us FROM webhook_watchdog",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(LegacyWebhookWatchdog {
                    scope: row.get(0)?,
                    cursor_delivered_at_us: row.get(1)?,
                    cursor_delivered_at_guid: row.get(2)?,
                    scan_cursor: row.get(3)?,
                    last_poll_at_us: row.get(4)?,
                    last_success_at_us: row.get(5)?,
                })
            })?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    }

    pub(crate) fn webhook_redeliveries(&self) -> anyhow::Result<Vec<LegacyWebhookRedelivery>> {
        let mut stmt = self.conn.prepare(
            "SELECT delivery_guid, github_delivery_id, app_id, reason, attempts, \
                    first_seen_at_us, last_attempt_at_us, resolved_at_us, last_error \
             FROM webhook_redeliveries",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(LegacyWebhookRedelivery {
                    delivery_guid: row.get(0)?,
                    github_delivery_id: row.get(1)?,
                    app_id: row.get(2)?,
                    reason: row.get(3)?,
                    attempts: row.get(4)?,
                    first_seen_at_us: row.get(5)?,
                    last_attempt_at_us: row.get(6)?,
                    resolved_at_us: row.get(7)?,
                    last_error: row.get(8)?,
                })
            })?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    }

    /// The sealed runtime metadata snapshot, already unsealed and parsed.
    /// v11 always carries exactly one row (`snapshot_id = 1`).
    pub(crate) fn meta_snapshot(
        &self,
        cipher: &Envelope,
    ) -> anyhow::Result<serde_json::Map<String, serde_json::Value>> {
        let blob: Vec<u8> = self.conn.query_row(
            "SELECT meta_blob FROM runtime_snapshots WHERE snapshot_id = 1",
            [],
            |row| row.get(0),
        )?;
        let plaintext = cipher
            .unseal(&blob)
            .context("decode legacy runtime metadata snapshot (wrong cluster key?)")?;
        let value: serde_json::Value = serde_json::from_slice(&plaintext)
            .context("parse legacy runtime metadata snapshot JSON")?;
        match value {
            serde_json::Value::Object(map) => Ok(map),
            _ => bail!("legacy runtime metadata snapshot is not a JSON object"),
        }
    }

    /// Decode one legacy message payload: either a plain JSON value (the
    /// pre-marker encoding) or a base64 string carrying a sealed payload
    /// (the v12 encoding written by `seal_message_payload`).
    pub(crate) fn decode_message_payload(
        &self,
        cipher: &Envelope,
        raw: &str,
        label: &str,
    ) -> anyhow::Result<serde_json::Value> {
        decode_message_payload(cipher, raw, label)
    }

    /// `(request_id, payload_json)` rows of the per-attempt job-message table.
    pub(crate) fn job_request_messages(&self) -> anyhow::Result<Vec<(i64, String)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT request_id, payload_json FROM job_request_messages ORDER BY request_id")?;
        let rows = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    }
}

/// Decode a legacy message payload (see [`LegacyDb::decode_message_payload`]).
pub(crate) fn decode_message_payload(
    cipher: &Envelope,
    raw: &str,
    label: &str,
) -> anyhow::Result<serde_json::Value> {
    let parsed: serde_json::Value = serde_json::from_str(raw)
        .with_context(|| format!("parse legacy {label} payload JSON"))?;
    match parsed {
        serde_json::Value::String(encoded) => {
            use base64::Engine as _;
            let sealed = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .with_context(|| format!("decode base64 legacy {label} payload"))?;
            let plaintext = cipher
                .unseal(&sealed)
                .with_context(|| format!("unseal legacy {label} payload"))?;
            serde_json::from_slice(&plaintext)
                .with_context(|| format!("parse unsealed legacy {label} payload"))
        }
        value => Ok(value),
    }
}

/// The legacy `job_requests` snapshot blob. Field names are the persisted
/// JSON keys; the type is private to the importer because the control
/// backend normalizes attempts across `job_requests` + `job_leases`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct LegacyRequestSnapshot {
    pub(crate) request_id: i64,
    pub(crate) run_id: RunId,
    pub(crate) job_id: JobId,
    pub(crate) agent_job_id: uuid::Uuid,
    pub(crate) plan_id: String,
    pub(crate) plan_type: String,
    pub(crate) timeline_id: uuid::Uuid,
    pub(crate) result: Option<ExecutionStatus>,
    pub(crate) locked_until: String,
    #[serde(default)]
    pub(crate) claimed_at_us: Option<i64>,
    #[serde(default)]
    pub(crate) owner_runner_id: Option<i64>,
    #[serde(default)]
    pub(crate) started_at_us: Option<i64>,
    #[serde(default)]
    pub(crate) last_renewed_at_us: Option<i64>,
    pub(crate) timeout_triggered: bool,
    pub(crate) debug_token_issued: bool,
}


/// Unseal and parse a legacy run blob.
pub(crate) fn restore_run_record(cipher: &Envelope, blob: &[u8]) -> anyhow::Result<RunRecord> {
    let value: serde_json::Value = serde_json::from_slice(&cipher.unseal(blob)?)?;
    run_record_from_value(value)
}

/// Parse a run blob produced by the legacy `run_record_value` (which the
/// current `crate::store::run_record_value` still emits byte-for-byte). The
/// `#[serde(skip_serializing)]` fields are injected back into the JSON by the
/// writer, so they must be restored by hand.
pub(crate) fn run_record_from_value(value: serde_json::Value) -> anyhow::Result<RunRecord> {
    let mut run: RunRecord = serde_json::from_value(value.clone())?;
    if let Some(object) = value.as_object() {
        run.webhook_delivery_id = serde_json::from_value(
            object
                .get("webhook_delivery_id")
                .cloned()
                .unwrap_or_default(),
        )?;
        run.job_needs = object
            .get("job_needs")
            .cloned()
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_default();
        run.caller_plans = object
            .get("caller_plans")
            .cloned()
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_default();
        run.github = object
            .get("github")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        run.head_sha = object
            .get("head_sha")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        run.workflow_ref = object
            .get("workflow_ref")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        // `run_record_value` always writes the key (null when absent), so JSON
        // null restores as `None`. A snapshot shape this binary no longer
        // understands is a hard import failure, not a silent drop: the
        // importer never claims a run it could not restore.
        run.workspace_snapshot = match object.get("workspace_snapshot") {
            Some(value) if !value.is_null() => Some(
                serde_json::from_value(value.clone()).with_context(|| {
                    format!("decode workspace snapshot of run {}", run.run_id.0)
                })?,
            ),
            _ => None,
        };
    }
    Ok(run)
}

/// Hex SHA-256 of a file, used to prove the source did not change.
pub(crate) fn file_digest(path: &Path) -> anyhow::Result<String> {
    use sha2::{Digest, Sha256};
    let bytes = std::fs::read(path)
        .with_context(|| format!("hash source database {}", path.display()))?;
    let digest = Sha256::digest(&bytes);
    Ok(hex::encode(digest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_chain_is_the_released_v11_chain() {
        let versions: Vec<i64> = LEGACY_MIGRATIONS.iter().map(|(v, _)| *v).collect();
        assert_eq!(versions, (1..=11).collect::<Vec<_>>());
        assert_eq!(LEGACY_PAYLOAD_MARKER.0, 12);
    }
}
