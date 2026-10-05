//! Row-level writer: maps decoded legacy v11 state onto the control schema
//! and writes it into the staging database inside one transaction.
//!
//! The writer reuses the lite backend's own insert vocabulary
//! (`insert_job`, `insert_job_message`, `insert_run_row`) so imported jobs
//! are shaped by the same statements a live submit uses; the follow-up
//! UPDATEs fill the columns those statements derive at submit time
//! (statuses, claims, attempt timestamps).
//!
//! Secret handling is the sanctioned one: values leave the run blob for the
//! SecretProvider's run tier under `<state_dir>/run-secrets`, job messages
//! are reduced to secret-free templates through
//! [`crate::message_template::strip_template`], and every template is
//! scanned for the run's secret values before it is stored.

use super::legacy::{
    LegacyAttempt, LegacyHeader, LegacyLogFile, LegacyQueuedJob, LegacyRequestSnapshot, LegacyRun,
    LegacyRunner, LegacySession, LegacyStep, LegacyWebhookDelivery, LegacyWebhookRedelivery,
    LegacyWebhookWatchdog,
};
use super::report::{ActivePolicy, ImportedRows, SkippedFamily};
use crate::control::lite::codec;
use crate::control::lite::jobs::{self, ReusableSpec, SpecExtras};
use crate::control::lite::submit;
use crate::store::Envelope;
use anyhow::{Context, bail};
use preloop_gha_protocol::azdo::{AgentJobRequestMessage, MessageSecretSpec};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::LazyLock;

/// Everything decoded from the source store, owned so the writer can consume
/// it (the importer never re-reads a half-imported source).
pub(crate) struct SourceData {
    pub(crate) header: LegacyHeader,
    pub(crate) counts: super::legacy::LegacyCounts,
    pub(crate) runs: Vec<LegacyRun>,
    pub(crate) jobs: Vec<LegacyQueuedJob>,
    pub(crate) attempts: Vec<LegacyAttempt>,
    pub(crate) steps: Vec<LegacyStep>,
    pub(crate) logs: Vec<LegacyLogFile>,
    pub(crate) runners: Vec<LegacyRunner>,
    pub(crate) sessions: Vec<LegacySession>,
    pub(crate) session_active: Vec<(String, i64)>,
    pub(crate) dependencies: Vec<(String, String, String)>,
    pub(crate) counters: Vec<(String, String, i64)>,
    pub(crate) webhook_deliveries: Vec<LegacyWebhookDelivery>,
    pub(crate) webhook_watchdog: Vec<LegacyWebhookWatchdog>,
    pub(crate) webhook_redeliveries: Vec<LegacyWebhookRedelivery>,
    pub(crate) control_events: Vec<super::legacy::LegacyControlEvent>,
    /// `(request_id, payload_json)` from the legacy per-attempt message table.
    pub(crate) job_request_messages: Vec<(i64, String)>,
    /// Parsed `artifact_v2_registry.json` from the legacy state directory.
    pub(crate) artifact_registry_sidecar: Option<serde_json::Map<String, serde_json::Value>>,
    pub(crate) meta: serde_json::Map<String, serde_json::Value>,
}

pub(crate) struct WriteOutcome {
    pub(crate) imported: ImportedRows,
    pub(crate) skipped: Vec<SkippedFamily>,
    pub(crate) notes: Vec<String>,
}

/// One active item the importer refuses to carry without an explicit policy.
#[derive(Debug, Clone)]
pub(crate) struct ActiveItem {
    pub(crate) kind: String,
    pub(crate) id: String,
    pub(crate) detail: String,
}

fn terminal(status: ExecutionStatus) -> bool {
    status.is_terminal()
}

fn conclusion_of(status: ExecutionStatus) -> &'static str {
    match status {
        ExecutionStatus::Success => "success",
        ExecutionStatus::Failure => "failure",
        ExecutionStatus::Skipped => "skipped",
        ExecutionStatus::Cancelled => "cancelled",
        _ => "failure",
    }
}

/// Target `runs.status` / `conclusion` from the legacy record.
///
/// The legacy record's status is an `ExecutionStatus`; the control schema
/// splits it into a lifecycle (`queued` / `in_progress` / `completed`) and a
/// conclusion. A record that reached `completed_at` is completed even if its
/// status field reads `pending` (a crash between the two writes must not
/// resurrect a finished run as scheduled).
fn run_state(record: &crate::models::RunRecord) -> (&'static str, Option<String>) {
    // A recorded conclusion wins when it is one the schema accepts; anything
    // else is derived from the status and reported by the caller's notes.
    const CONCLUSIONS: &[&str] = &["success", "failure", "cancelled", "skipped", "timed_out"];
    if let Some(conclusion) = record.conclusion.as_deref()
        && CONCLUSIONS.contains(&conclusion)
    {
        return ("completed", Some(conclusion.to_owned()));
    }
    if terminal(record.status) {
        return ("completed", Some(conclusion_of(record.status).to_owned()));
    }
    if record.completed_at.is_some() {
        return ("completed", Some(conclusion_of(record.status).to_owned()));
    }
    if record.started_at.is_some() || record.status == ExecutionStatus::InProgress {
        return ("in_progress", None);
    }
    ("queued", None)
}

/// Whether a legacy request row was a never-dispatched placeholder.
fn unclaimed(snapshot: &LegacyRequestSnapshot) -> bool {
    snapshot.result.is_none()
        && snapshot.claimed_at_us.is_none()
        && snapshot.owner_runner_id.is_none()
        && snapshot.locked_until.is_empty()
}

/// Find work that must not be imported silently: claimed-but-unfinished
/// attempts, session bindings to them, and live concurrency gate state.
pub(crate) fn audit_active(source: &SourceData) -> Vec<ActiveItem> {
    let mut items = Vec::new();
    for attempt in &source.attempts {
        if attempt.snapshot.result.is_none() && !unclaimed(&attempt.snapshot) {
            items.push(ActiveItem {
                kind: "claimed-attempt".to_owned(),
                id: attempt.request_id.to_string(),
                detail: format!(
                    "{} / {} owned by runner {:?}, lease {:?}",
                    attempt.run_id,
                    attempt.job_id,
                    attempt.snapshot.owner_runner_id,
                    attempt.snapshot.locked_until
                ),
            });
        }
    }
    for (session, request) in &source.session_active {
        items.push(ActiveItem {
            kind: "session-binding".to_owned(),
            id: request.to_string(),
            detail: format!("session {session} holds request {request}"),
        });
    }
    let gate_state: Vec<String> = [
        "concurrency_groups",
        "jobset_admissions",
        "run_concurrency",
        "holder_keys",
    ]
    .iter()
    .filter_map(|key| {
        source
            .meta
            .get(*key)
            .and_then(|value| value.as_array())
            .filter(|rows| !rows.is_empty())
            .map(|rows| format!("{key}={}", rows.len()))
    })
    .collect();
    if !gate_state.is_empty() {
        items.push(ActiveItem {
            kind: "concurrency-state".to_owned(),
            id: "meta".to_owned(),
            detail: format!(
                "live concurrency gates in the runtime snapshot ({})",
                gate_state.join(", ")
            ),
        });
    }
    items
}

/// Meta keys this importer knows and handles (mapped, released, or reported).
/// An unknown key is a hard refusal: silently dropping durable metadata is the
/// failure mode this list exists to prevent.
const KNOWN_META_KEYS: &[&str] = &[
    "revision",
    "workflow_run_counters",
    "next_runner_id",
    "next_cache_id",
    "next_message_id",
    "next_log_id",
    "next_artifact_v2_id",
    "azdo_sessions",
    "oidc_job_contexts",
    "id_token_grants",
    "concurrency_groups",
    "jobset_admissions",
    "run_concurrency",
    "holder_keys",
    "artifacts",
    "log_metadata",
    "timeline_events",
    "timeline_change_ids",
    "timeline_records",
    "cache_v2_pending",
    "cache_v2_dl_tokens",
    "artifact_v2_pending",
    "artifact_v2_registry",
    "github_token_requests",
    "cancellation_queue",
    "runner_client_ids",
    "pool_proven_runners",
    "job_assignments",
    "pool_pending",
];

/// Refuse an unrecognized snapshot shape before anything is written.
pub(crate) fn audit_meta_keys(source: &SourceData) -> anyhow::Result<()> {
    let unknown: Vec<&str> = source
        .meta
        .keys()
        .map(String::as_str)
        .filter(|key| !KNOWN_META_KEYS.contains(key))
        .collect();
    if !unknown.is_empty() {
        bail!(
            "legacy runtime metadata snapshot carries keys this importer does not \
             know ({unknown:?}); refusing to silently drop durable metadata"
        );
    }
    Ok(())
}

/// Refuse a source that carries durable state the importer cannot carry
/// faithfully. These families are neither scheduling state nor provably
/// unreachable: importing would either lose data or misrepresent runtime
/// state, so the operator drains them (or restarts the legacy server and lets
/// them flush) before running the import. `control_events` is not here — it is
/// mapped into the outbox.
pub(crate) fn audit_unmappable(source: &SourceData) -> anyhow::Result<()> {
    let mut present: Vec<String> = Vec::new();
    for (family, rows) in [
        ("broker_messages", source.counts.broker_messages),
        ("runner_commands", source.counts.runner_commands),
        ("meta.pool_pending", meta_count(&source.meta, "pool_pending")),
        (
            "meta.cache_v2_pending",
            meta_count(&source.meta, "cache_v2_pending"),
        ),
        (
            "meta.artifact_v2_pending",
            meta_count(&source.meta, "artifact_v2_pending"),
        ),
    ] {
        if rows > 0 {
            present.push(format!("{family}={rows}"));
        }
    }
    if !present.is_empty() {
        bail!(
            "legacy store still holds unrelated durable state this importer cannot carry \
             faithfully ({}); stop the legacy server and let it flush/drain, or clear the \
             listed family explicitly before importing",
            present.join(", ")
        );
    }
    Ok(())
}

/// Refuse a source that cannot satisfy the control schema's unique indexes,
/// with a clear message instead of a mid-transaction SQL error.
pub(crate) fn audit_invariants(source: &SourceData) -> anyhow::Result<()> {
    // runs_number: (namespace, repository, workflow_path, number, attempt).
    let mut numbers: BTreeMap<(String, String, u64, u64), String> = BTreeMap::new();
    let mut deliveries: BTreeMap<(String, String), String> = BTreeMap::new();
    for run in &source.runs {
        let key = (
            run.record.submission.repository.clone(),
            run.record.workflow_path_str.clone(),
            run.record.run_number,
            run.record.run_attempt,
        );
        if let Some(previous) = numbers.insert(key.clone(), run.run_id.clone()) {
            bail!(
                "legacy runs {previous} and {} share \
                 (repository, workflow_path, run_number, run_attempt) {key:?}; the control \
                 schema requires that tuple to be unique",
                run.run_id
            );
        }
        if let Some(delivery) = run.record.webhook_delivery_id.as_deref() {
            let key = (delivery.to_owned(), run.record.workflow_path_str.clone());
            if let Some(previous) = deliveries.insert(key.clone(), run.run_id.clone()) {
                bail!(
                    "legacy runs {previous} and {} share webhook delivery {delivery:?} for \
                     workflow {:?}; the control schema requires that pair to be unique",
                    run.run_id,
                    run.record.workflow_path_str
                );
            }
        }
    }
    // job_requests_inflight: at most one result-NULL attempt per job.
    let mut inflight: BTreeMap<(String, String), i64> = BTreeMap::new();
    for attempt in &source.attempts {
        if attempt.snapshot.result.is_none() {
            let key = (attempt.run_id.clone(), attempt.job_id.clone());
            if let Some(previous) = inflight.insert(key.clone(), attempt.request_id) {
                bail!(
                    "legacy attempts {previous} and {} are both unfinished for {}/{}; the \
                     control schema allows one in-flight attempt per job",
                    attempt.request_id,
                    key.0,
                    key.1
                );
            }
        }
    }
    Ok(())
}

static EMPTY_META_ARRAY: LazyLock<Vec<serde_json::Value>> = LazyLock::new(Vec::new);

fn meta_array<'a>(
    meta: &'a serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> anyhow::Result<&'a Vec<serde_json::Value>> {
    match meta.get(key) {
        None | Some(serde_json::Value::Null) => Ok(&*EMPTY_META_ARRAY),
        Some(serde_json::Value::Array(rows)) => Ok(rows),
        Some(other) => bail!("legacy meta key {key} is not an array: {other}"),
    }
}

fn meta_count(meta: &serde_json::Map<String, serde_json::Value>, key: &str) -> u64 {
    meta.get(key)
        .and_then(|value| value.as_array())
        .map(|rows| rows.len() as u64)
        .unwrap_or(0)
}

/// How to compare a staged sidecar with a file already at its destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SidecarKind {
    /// Byte-for-byte identical (log segments).
    ExactBytes,
    /// A sealed JSON `name -> value` map; equal when the decrypted maps match
    /// (sealing is randomized, so bytes never match across runs).
    SealedStringMap,
}

struct StagedFile {
    staged: std::path::PathBuf,
    final_path: std::path::PathBuf,
    kind: SidecarKind,
}

/// Sidecar files (run-secret tiers, live-log segments) staged under a
/// target-owned directory and published only after the database is verified
/// and committed. Publishing never overwrites: a destination that already
/// holds equivalent content is adopted (idempotent retry), anything else is a
/// refusal. A failed publish rolls back the files it created.
pub(crate) struct Sidecars {
    staging_dir: std::path::PathBuf,
    files: Vec<StagedFile>,
    published: Vec<std::path::PathBuf>,
}

impl Sidecars {
    pub(crate) fn new(staging_dir: std::path::PathBuf) -> Self {
        Self {
            staging_dir,
            files: Vec::new(),
            published: Vec::new(),
        }
    }

    /// Stage one file. Refuses (before the database transaction commits) when
    /// the destination exists with different content.
    pub(crate) fn stage(
        &mut self,
        state_dir: &Path,
        relative: &str,
        bytes: &[u8],
        kind: SidecarKind,
        cipher: &Envelope,
    ) -> anyhow::Result<()> {
        let final_path = state_dir.join(relative);
        if final_path.exists() {
            if sidecar_equal(&final_path, bytes, kind, cipher)? {
                return Ok(());
            }
            bail!(
                "refusing to overwrite existing {}; the target state directory does not \
                 belong to this source",
                final_path.display()
            );
        }
        let staged = self.staging_dir.join(relative);
        if let Some(parent) = staged.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create sidecar staging dir {}", parent.display()))?;
        }
        {
            use std::io::Write as _;
            let mut file = std::fs::File::create(&staged)
                .with_context(|| format!("create staged sidecar {}", staged.display()))?;
            file.write_all(bytes)?;
            file.sync_all()?;
        }
        self.files.push(StagedFile {
            staged,
            final_path,
            kind,
        });
        Ok(())
    }

    /// Publish every staged file into the state directory without ever
    /// replacing an existing file (`hard_link` is atomic and fails on an
    /// existing destination). On any failure the files already published in
    /// this call are removed again.
    pub(crate) fn publish(&mut self, cipher: &Envelope) -> anyhow::Result<()> {
        // Take the staged list so the linking loop can borrow `self`
        // mutably (`published`/`rollback`) without aliasing `self.files`.
        let files = std::mem::take(&mut self.files);
        let result = self.link_staged(&files, cipher);
        // Restore the list before propagating: `rollback`/`cleanup` walk it,
        // and a retried import re-publishes (or adopts) the same sidecars.
        self.files = files;
        result?;
        for dir in self
            .files
            .iter()
            .filter_map(|file| file.final_path.parent().map(Path::to_path_buf))
            .collect::<BTreeSet<_>>()
        {
            if let Ok(handle) = std::fs::File::open(&dir) {
                let _ = handle.sync_all();
            }
        }
        Ok(())
    }

    /// Link each staged sidecar into place (see [`Sidecars::publish`]).
    fn link_staged(&mut self, files: &[StagedFile], cipher: &Envelope) -> anyhow::Result<()> {
        for file in files {
            if let Some(parent) = file.final_path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create sidecar dir {}", parent.display()))?;
            }
            match std::fs::hard_link(&file.staged, &file.final_path) {
                Ok(()) => self.published.push(file.final_path.clone()),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let bytes = std::fs::read(&file.staged)?;
                    if sidecar_equal(&file.final_path, &bytes, file.kind, cipher)? {
                        // Equivalent leftover from an earlier attempt: adopt.
                        continue;
                    }
                    self.rollback();
                    bail!(
                        "sidecar destination {} appeared during the import with different \
                         content; the import was rolled back",
                        file.final_path.display()
                    );
                }
                Err(error) => {
                    self.rollback();
                    return Err(error).with_context(|| {
                        format!(
                            "publish sidecar {} -> {}",
                            file.staged.display(),
                            file.final_path.display()
                        )
                    });
                }
            }
        }
        Ok(())
    }

    /// Remove the files this run published (best-effort; used on a later
    /// failure, e.g. the database rename).
    pub(crate) fn rollback(&mut self) {
        for path in self.published.drain(..) {
            let _ = std::fs::remove_file(path);
        }
    }

    /// Drop the staging directory (staged files that were adopted or rolled
    /// back included).
    pub(crate) fn cleanup(&self) {
        let _ = std::fs::remove_dir_all(&self.staging_dir);
    }
}

fn sidecar_equal(
    path: &Path,
    staged: &[u8],
    kind: SidecarKind,
    cipher: &Envelope,
) -> anyhow::Result<bool> {
    let existing = std::fs::read(path)?;
    match kind {
        SidecarKind::ExactBytes => Ok(existing == staged),
        SidecarKind::SealedStringMap => {
            let staged_plain = cipher.unseal(staged)?;
            let staged_map: BTreeMap<String, String> = serde_json::from_slice(&staged_plain)?;
            let Ok(existing_plain) = cipher.unseal(&existing) else {
                return Ok(false);
            };
            let Ok(existing_map) = serde_json::from_slice::<BTreeMap<String, String>>(&existing_plain)
            else {
                return Ok(false);
            };
            Ok(staged_map == existing_map)
        }
    }
}

/// Write the whole import inside the caller's transaction.
pub(crate) fn write_import(
    tx: &Transaction<'_>,
    source: &mut SourceData,
    policy: ActivePolicy,
    sidecars: &mut Sidecars,
    state_dir: &Path,
    legacy_state_dir: &Path,
    cipher: &Envelope,
) -> anyhow::Result<WriteOutcome> {
    let mut imported = ImportedRows::default();
    let mut skipped: Vec<SkippedFamily> = Vec::new();
    let mut notes: Vec<String> = Vec::new();
    let namespace = "default";

    let runs = std::mem::take(&mut source.runs);
    let jobs = std::mem::take(&mut source.jobs);
    let attempts = std::mem::take(&mut source.attempts);
    let mut attempt_index: BTreeMap<i64, (String, String, String)> = BTreeMap::new();
    for attempt in &attempts {
        attempt_index.insert(
            attempt.request_id,
            (
                attempt.run_id.clone(),
                attempt.job_id.clone(),
                attempt.agent_job_id.clone(),
            ),
        );
    }
    let mut secrets_by_run: BTreeMap<String, (BTreeMap<String, String>, BTreeSet<String>)> =
        BTreeMap::new();
    let steps = std::mem::take(&mut source.steps);
    let logs = std::mem::take(&mut source.logs);
    let runners = std::mem::take(&mut source.runners);
    let sessions = std::mem::take(&mut source.sessions);
    let dependencies = std::mem::take(&mut source.dependencies);

    // ── Indexes ─────────────────────────────────────────────────────────
    let mut jobs_by_run: BTreeMap<String, Vec<LegacyQueuedJob>> = BTreeMap::new();
    for job in jobs {
        jobs_by_run.entry(job.run_id.clone()).or_default().push(job);
    }
    let mut attempts_by_job: BTreeMap<(String, String), Vec<LegacyAttempt>> = BTreeMap::new();
    let mut attempt_agents: BTreeSet<String> = BTreeSet::new();
    for attempt in attempts {
        attempt_agents.insert(attempt.agent_job_id.clone());
        attempts_by_job
            .entry((attempt.run_id.clone(), attempt.job_id.clone()))
            .or_default()
            .push(attempt);
    }
    let mut orphan_steps = 0_u64;
    let mut steps_by_agent: BTreeMap<String, Vec<LegacyStep>> = BTreeMap::new();
    for step in steps {
        if !attempt_agents.contains(&step.agent_job_id) {
            orphan_steps += 1;
            continue;
        }
        steps_by_agent
            .entry(step.agent_job_id.clone())
            .or_default()
            .push(step);
    }
    for records in steps_by_agent.values_mut() {
        records.sort_by_key(|step| step.sort_key());
    }
    if orphan_steps > 0 {
        skipped.push(SkippedFamily {
            family: "job_steps (orphaned)".to_owned(),
            rows: orphan_steps,
            reason: "steps of attempts the legacy store no longer holds (evicted runs)".to_owned(),
        });
    }
    let session_of_request: BTreeMap<i64, String> = source
        .session_active
        .iter()
        .map(|(session, request)| (*request, session.clone()))
        .collect();
    let mut extra_needs: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for (run_id, job_id, depends_on) in dependencies {
        extra_needs
            .entry((run_id, job_id))
            .or_default()
            .push(depends_on);
    }
    let mut token_requests: BTreeMap<i64, crate::models::GitHubTokenRequest> = BTreeMap::new();
    for row in meta_array(&source.meta, "github_token_requests")? {
        let pair = row
            .as_array()
            .filter(|pair| pair.len() == 2)
            .with_context(|| format!("legacy github_token_requests row is malformed: {row}"))?;
        let request_id = pair[0]
            .as_i64()
            .context("github_token_requests request id")?;
        let request: crate::models::GitHubTokenRequest = serde_json::from_value(pair[1].clone())
            .context("decode legacy github_token_requests entry")?;
        token_requests.insert(request_id, request);
    }

    // ── Runs and their jobs ─────────────────────────────────────────────
    let mut max_run_number: BTreeMap<(String, String), i64> = BTreeMap::new();
    let mut synthesized_jobs = 0_u64;
    let mut derived_conclusions = 0_u64;
    let mut drained_claims = 0_u64;
    let mut settled_claims = 0_u64;
    for run in runs {
        if run.record.conclusion.as_deref().is_some_and(|conclusion| {
            !["success", "failure", "cancelled", "skipped", "timed_out"].contains(&conclusion)
        }) {
            derived_conclusions += 1;
        }
        let mut record = run.record;
        let repository = record.submission.repository.clone();
        let workflow_path = record.workflow_path_str.clone();
        let entry = max_run_number
            .entry((repository, workflow_path))
            .or_insert(0);
        *entry = (*entry).max(record.run_number as i64);

        let secrets = preloop_gha_protocol::masking::expose_all(&record.submission.secrets);
        secrets_by_run.insert(
            run.run_id.clone(),
            (secrets.clone(), record.submission.run_secret_names.clone()),
        );
        if !secrets.is_empty() {
            let submission = std::sync::Arc::make_mut(&mut record.submission);
            submission.run_secret_names = secrets.keys().cloned().collect();
            write_run_secrets(sidecars, state_dir, &run.run_id, &secrets, cipher)?;
            imported.run_secrets += 1;
        }
        submit::insert_run_row(tx, &record, namespace, None)
            .with_context(|| format!("insert run {}", record.run_id.0))?;
        let (status, conclusion) = run_state(&record);
        tx.execute(
            "UPDATE runs SET status = ?2, conclusion = ?3, started_at = ?4, completed_at = ?5 \
             WHERE run_id = ?1",
            params![
                run.run_id,
                status,
                conclusion,
                record.started_at.map(|at| at.timestamp_micros()),
                record.completed_at.map(|at| at.timestamp_micros()),
            ],
        )?;
        if let Some(push) = &record.push_state {
            let push_status = match push.status {
                crate::models::PushStatus::Pending => "pending",
                crate::models::PushStatus::Synced => "synced",
                crate::models::PushStatus::Blocked => "blocked",
            };
            tx.execute(
                "INSERT INTO run_push_states (run_id, status, error, pr_number, effective_sha, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                 ON CONFLICT (run_id) DO UPDATE SET status = excluded.status, \
                     error = excluded.error, pr_number = excluded.pr_number, \
                     effective_sha = excluded.effective_sha, updated_at = excluded.updated_at",
                params![
                    run.run_id,
                    push_status,
                    push.error,
                    push.pr_number.map(|number| number as i64),
                    push.effective_sha,
                    record
                        .completed_at
                        .map(|at| at.timestamp_micros())
                        .unwrap_or_else(crate::store::now_us),
                ],
            )?;
            imported.run_push_states += 1;
        }
        imported.runs += 1;

        let run_id = RunId(run.run_id.parse().unwrap_or_default());
        let mut payloads: BTreeMap<String, LegacyQueuedJob> = jobs_by_run
            .remove(&run.run_id)
            .unwrap_or_default()
            .into_iter()
            .map(|job| (job.job.job_id.0.clone(), job))
            .collect();
        let mut job_ids: Vec<String> = record.jobs.keys().map(|id| id.0.clone()).collect();
        for (id, payload) in &payloads {
            if !record.jobs.contains_key(&JobId(id.clone())) {
                bail!(
                    "legacy run {} queues job {id} that its record does not declare; \
                     refusing a referentially inconsistent source",
                    run.run_id
                );
            }
            if let Some(declared) = record.jobs.get(&JobId(id.clone()))
                && crate::control::types::status_str(*declared) != payload.status
            {
                bail!(
                    "legacy job {}/{} status disagrees between its row ({}) and the run \
                     record ({}); refusing a referentially inconsistent source",
                    run.run_id,
                    id,
                    payload.status,
                    crate::control::types::status_str(*declared)
                );
            }
            if !job_ids.contains(id) {
                job_ids.push(id.clone());
            }
        }
        let display_order: BTreeMap<String, i64> = record
            .jobs_list
            .iter()
            .enumerate()
            .map(|(index, detail)| (detail.job_id.clone(), index as i64))
            .collect();
        let run_order = record.created_at.timestamp_micros();
        let mut any_active_job = false;

        for (fallback_order, job_id) in job_ids.iter().enumerate() {
            let job_key = JobId(job_id.clone());
            let status = record
                .jobs
                .get(&job_key)
                .copied()
                .context("job declared without a status")?;
            let order = display_order
                .get(job_id)
                .copied()
                .unwrap_or(fallback_order as i64);
            let attempts_for_job = attempts_by_job
                .remove(&(run.run_id.clone(), job_id.clone()))
                .unwrap_or_default();
            let active_claim = attempts_for_job
                .iter()
                .find(|attempt| attempt.snapshot.result.is_none() && !unclaimed(&attempt.snapshot));
            let drain_cancel = policy == ActivePolicy::Cancel && active_claim.is_some();
            let job_status = if drain_cancel && !terminal(status) {
                ExecutionStatus::Cancelled
            } else if status == ExecutionStatus::Pending {
                // Legacy `pending` means "waiting on a concurrency group";
                // the control schema keeps that classification in
                // `queue_state` and the status `queued`.
                ExecutionStatus::Queued
            } else {
                status
            };
            any_active_job |= !terminal(job_status);
            let queue_state = if terminal(job_status) {
                "none".to_owned()
            } else if active_claim.is_some() && policy != ActivePolicy::Requeue {
                "claimed".to_owned()
            } else if active_claim.is_some() {
                "ready".to_owned()
            } else {
                match payloads.get(job_id) {
                    Some(payload) => match payload.queue_kind.as_str() {
                        "ready" => "ready".to_owned(),
                        // Awaiting dependency promotion (legacy `pending`);
                        // the control schema's `blocked` is exactly that slot.
                        "pending" => "blocked".to_owned(),
                        // Legacy `blocked` is concurrency-blocked and legacy
                        // `held` is a run gate; both are `held` here.
                        "blocked" | "held" => "held".to_owned(),
                        other => bail!(
                            "legacy job {}/{} has unknown queue kind {other:?}",
                            run.run_id,
                            job_id
                        ),
                    },
                    None => bail!(
                        "legacy run {} has non-terminal job {job_id} with no persisted queue row; \
                         refusing to import a job whose spec the source no longer holds",
                        run.run_id
                    ),
                }
            };

            let (oidc_environment, oidc_ref, oidc_sha, granted) =
                oidc_for(&source.meta, &run.run_id, job_id)?;
            match payloads.get_mut(job_id) {
                Some(payload) => {
                    let remaining = payload
                        .job
                        .needs
                        .iter()
                        .filter(|need| {
                            record
                                .jobs
                                .get(*need)
                                .map(|status| !terminal(*status))
                                .unwrap_or(false)
                        })
                        .count() as i32;
                    let kind = submit::kind_of(&payload.job);
                    let display_name = record
                        .job_names
                        .get(&job_key)
                        .cloned()
                        .unwrap_or_else(|| job_id.clone());
                    sanitize_message(
                        &mut payload.job.message,
                        oidc_environment.clone(),
                        &record.submission.run_secret_names,
                        &secrets,
                        &run.run_id,
                        job_id,
                    )?;
                    let spec = SpecExtras {
                        reusable_call_json: ReusableSpec::encode(
                            payload.job.reusable_call.as_ref(),
                            record.reusable_calls.get(job_id),
                            record.caller_plans.get(&job_key),
                        ),
                        fail_fast: record.job_fail_fast.get(&payload.job.base_id).copied(),
                        continue_on_error: record.job_continue_on_error.get(job_id).copied(),
                        id_token_granted: granted,
                        oidc_environment: oidc_environment.as_deref(),
                        oidc_job_workflow_ref: oidc_ref.as_deref(),
                        oidc_job_workflow_sha: oidc_sha.as_deref(),
                    };
                    jobs::insert_job(
                        tx,
                        run_id,
                        namespace,
                        &payload.job,
                        kind,
                        None,
                        &display_name,
                        order,
                        job_status,
                        &queue_state,
                        remaining,
                        run_order,
                        order,
                        &spec,
                    )
                    .with_context(|| format!("insert job {}/{}", run.run_id, job_id))?;
                    jobs::insert_job_message(
                        tx,
                        run_id,
                        &job_key,
                        &payload.job.message,
                        &payload.job.condition_context,
                    )?;
                    imported.jobs += 1;
                    imported.job_specs += 1;
                    imported.job_needs += payload.job.needs.len() as u64;
                    imported.job_messages += 1;
                }
                None => {
                    // A job that concluded before it was ever queued (`if:`
                    // false, unhostable platform, cancelled arrival). The run
                    // record still carries its status and identity; the
                    // immutable spec and message were never persisted by the
                    // legacy store, so the row is written from what exists.
                    insert_synthesized_job(
                        tx,
                        &run.run_id,
                        job_id,
                        job_status,
                        order,
                        run_order,
                        &record,
                    )?;
                    imported.jobs += 1;
                    imported.job_specs += 1;
                    synthesized_jobs += 1;
                }
            }

            // Additional dependency edges the legacy table tracked beyond the
            // payload's `needs` (normally identical; a superset is preserved).
            if let Some(extra) = extra_needs.get(&(run.run_id.clone(), job_id.clone())) {
                let declared_needs: BTreeSet<String> = payloads
                    .get(job_id)
                    .map(|payload| payload.job.needs.iter().map(|n| n.0.clone()).collect())
                    .unwrap_or_default();
                let mut position = declared_needs.len() as i64;
                for need in extra {
                    if declared_needs.contains(need) {
                        continue;
                    }
                    tx.execute(
                        "INSERT INTO job_needs (run_id, job_id, needs_job_id, position) \
                         VALUES (?1, ?2, ?3, ?4) ON CONFLICT DO NOTHING",
                        params![run.run_id, job_id, need, position],
                    )?;
                    position += 1;
                    imported.job_needs += 1;
                }
            }

            // Carry the live claim onto the job row (the row owns the claim
            // cursor; the lease itself lives in `job_leases`). Under the
            // explicit requeue policy the claim is released instead.
            if !terminal(job_status)
                && let Some(claim) = active_claim
                && policy != ActivePolicy::Requeue
            {
                tx.execute(
                    "UPDATE jobs SET claimed_by_runner_id = ?3, claimed_at = COALESCE(?4, 0) \
                     WHERE run_id = ?1 AND job_id = ?2",
                    params![
                        run.run_id,
                        job_id,
                        claim.snapshot.owner_runner_id,
                        claim.snapshot.claimed_at_us,
                    ],
                )?;
            }

            // ── Attempts of this job ────────────────────────────────────
            for attempt in &attempts_for_job {
                if attempt.snapshot.result.is_none()
                    && !unclaimed(&attempt.snapshot)
                    && policy != ActivePolicy::Refuse
                {
                    match policy {
                        ActivePolicy::Requeue => drained_claims += 1,
                        ActivePolicy::Cancel => settled_claims += 1,
                        ActivePolicy::Refuse => {}
                    }
                }
                let cancelled = drain_cancel
                    && attempt.snapshot.result.is_none()
                    && !unclaimed(&attempt.snapshot);
                let result = if cancelled {
                    Some(ExecutionStatus::Cancelled)
                } else {
                    attempt.snapshot.result
                };
                let claimed = !unclaimed(&attempt.snapshot)
                    && !cancelled
                    && policy != ActivePolicy::Requeue;
                let session = session_of_request.get(&attempt.request_id);
                let claimed_at_us = attempt
                    .snapshot
                    .claimed_at_us
                    .filter(|_| claimed)
                    .unwrap_or(0);
                let finished_at_us = result.map(|_| {
                    steps_by_agent
                        .get(&attempt.agent_job_id)
                        .and_then(|steps| steps.iter().filter_map(|step| step.finished_at_us).max())
                        .or_else(|| record.completed_at.map(|at| at.timestamp_micros()))
                        .unwrap_or_else(crate::store::now_us)
                });
                tx.execute(
                    "INSERT INTO job_requests (request_id, run_id, job_id, namespace_id, \
                         agent_job_id, timeline_id, runner_id, session_id, result, \
                         timeout_triggered, debug_token_issued, claimed_at, started_at, finished_at) \
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
                    params![
                        attempt.request_id,
                        run.run_id,
                        job_id,
                        namespace,
                        attempt.agent_job_id,
                        attempt.snapshot.timeline_id.to_string(),
                        if claimed {
                            attempt.snapshot.owner_runner_id
                        } else {
                            None
                        },
                        if claimed { session } else { None },
                        result.map(crate::control::types::status_str),
                        attempt.snapshot.timeout_triggered as i64,
                        attempt.snapshot.debug_token_issued as i64,
                        claimed_at_us,
                        attempt.snapshot.started_at_us.filter(|_| claimed),
                        finished_at_us,
                    ],
                )
                .with_context(|| format!("insert attempt {}", attempt.request_id))?;
                imported.job_requests += 1;
                if claimed {
                    let expires_at = if attempt.snapshot.locked_until.is_empty() {
                        crate::store::now_us()
                    } else {
                        codec::parse_lease(&attempt.snapshot.locked_until)
                            .unwrap_or_else(|_| crate::store::now_us())
                    };
                    tx.execute(
                        "INSERT INTO job_leases (request_id, runner_id, expires_at, renewed_at) \
                         VALUES (?1, ?2, ?3, ?4)",
                        params![
                            attempt.request_id,
                            attempt.snapshot.owner_runner_id.unwrap_or(0),
                            expires_at,
                            attempt.snapshot.last_renewed_at_us.unwrap_or(expires_at),
                        ],
                    )?;
                    imported.job_leases += 1;
                }
                // The runner-facing template carries the attempt's own id.
                tx.execute(
                    "UPDATE job_messages SET message_template = json_set(message_template, \
                         '$.requestId', ?3) WHERE run_id = ?1 AND job_id = ?2",
                    params![run.run_id, job_id, attempt.request_id],
                )?;

                // ── Step manifest of this attempt ───────────────────────
                if let Some(records) = steps_by_agent.get(&attempt.agent_job_id) {
                    for (position, step) in records.iter().enumerate() {
                        insert_step(tx, &attempt.agent_job_id, position as i64, step)?;
                        imported.job_steps += 1;
                    }
                }
            }

            // Terminal job timestamps, check ids, outputs, annotations.
            // A claim released by the explicit requeue policy leaves nothing
            // of the abandoned attempt on the job row.
            let drained = policy == ActivePolicy::Requeue && !terminal(job_status);
            let (mut started_at, mut completed_at) =
                job_times(&attempts_for_job, &record, terminal(job_status));
            if drained {
                started_at = None;
                completed_at = None;
            }
            tx.execute(
                "UPDATE jobs SET check_run_id = ?3, outputs = ?4, annotations = ?5, \
                     started_at = ?6, completed_at = ?7, created_at = ?8 \
                 WHERE run_id = ?1 AND job_id = ?2",
                params![
                    run.run_id,
                    job_id,
                    record
                        .job_check_run_ids
                        .get(&job_key)
                        .map(|id| *id as i64),
                    record
                        .job_outputs
                        .get(&job_key)
                        .map(|outputs| serde_json::to_string(outputs).unwrap_or_default()),
                    record
                        .jobs_list
                        .iter()
                        .find(|detail| detail.job_id.as_str() == job_id.as_str())
                        .and_then(|detail| serde_json::to_string(&detail.annotations).ok()),
                    started_at,
                    completed_at,
                    record.created_at.timestamp_micros(),
                ],
            )?;
        }
        // Every job of the run was cancelled or already terminal: the run
        // itself is settled (a cancel drain must not leave a lifeless
        // `in_progress` run behind).
        if policy == ActivePolicy::Cancel && !any_active_job {
            tx.execute(
                "UPDATE runs SET status = 'completed', \
                     conclusion = COALESCE(conclusion, 'cancelled'), \
                     completed_at = COALESCE(completed_at, ?2) WHERE run_id = ?1",
                params![run.run_id, crate::store::now_us()],
            )?;
        }
    }
    if synthesized_jobs > 0 {
        notes.push(format!(
            "{synthesized_jobs} terminal jobs were never queued in the legacy store, so their \
             immutable spec/message is absent from the source; they were imported from the run \
             record (status, needs, check ids, outputs, annotations all preserved)"
        ));
    }
    if derived_conclusions > 0 {
        notes.push(format!(
            "{derived_conclusions} run conclusions were outside the control schema's set and \
             were derived from the legacy status"
        ));
    }
    if drained_claims > 0 {
        notes.push(format!(
            "{drained_claims} claimed attempts were released by the explicit --active=requeue \
             policy; their runners are abandoned and the jobs are dispatchable again"
        ));
    }
    if settled_claims > 0 {
        notes.push(format!(
            "{settled_claims} claimed attempts were settled as cancelled by the explicit \
             --active=cancel policy"
        ));
    }

    // ── Counters ────────────────────────────────────────────────────────
    for (repository_key, workflow_path, next_run_number) in &source.counters {
        if repository_key.is_empty() {
            notes.push(format!(
                "legacy workflow counter ({workflow_path:?}) has an empty repository key; \
                 the counter was rebuilt from imported run numbers instead"
            ));
            continue;
        }
        let entry = max_run_number
            .entry((repository_key.clone(), workflow_path.clone()))
            .or_insert(0);
        *entry = (*entry).max(next_run_number.saturating_sub(1));
    }
    if let Some(map) = source
        .meta
        .get("workflow_run_counters")
        .and_then(|v| v.as_object())
    {
        for (workflow_path, count) in map {
            let next = count.as_u64().unwrap_or(0).saturating_sub(1) as i64;
            let mut matched = false;
            for ((_, path), entry) in max_run_number.iter_mut() {
                if path == workflow_path {
                    *entry = (*entry).max(next);
                    matched = true;
                }
            }
            if !matched {
                notes.push(format!(
                    "legacy meta counter for {workflow_path:?} matched no imported run; skipped"
                ));
            }
        }
    }
    for ((repository, workflow_path), last) in &max_run_number {
        tx.execute(
            "INSERT INTO workflow_run_numbers (namespace_id, repository, workflow_path, last_run_number) \
             VALUES (?1, ?2, ?3, ?4) ON CONFLICT (namespace_id, repository, workflow_path) \
             DO UPDATE SET last_run_number = MAX(last_run_number, excluded.last_run_number)",
            params![namespace, repository, workflow_path, last],
        )?;
        imported.workflow_run_numbers += 1;
    }

    // ── Timelines ───────────────────────────────────────────────────────
    import_timelines(tx, &source.meta, &mut imported)?;

    // ── Runners and sessions ────────────────────────────────────────────
    let client_ids: BTreeMap<i64, String> = meta_array(&source.meta, "runner_client_ids")?
        .iter()
        .filter_map(|row| {
            let pair = row.as_array()?;
            let client = pair.first()?.as_str()?.to_owned();
            let runner = pair.get(1)?.as_i64()?;
            Some((runner, client))
        })
        .collect();
    let pool_proven: BTreeSet<i64> = meta_array(&source.meta, "pool_proven_runners")?
        .iter()
        .filter_map(|row| row.as_i64())
        .collect();
    let mut seen_client: BTreeSet<String> = BTreeSet::new();
    let mut deleted = 0_u64;
    for runner in &runners {
        if runner.deleted_at_us.is_some() {
            deleted += 1;
            continue;
        }
        if let Some(client) = client_ids.get(&runner.runner_id)
            && !seen_client.insert(client.clone())
        {
            bail!(
                "legacy meta maps two runners to client_id {client}; refusing an ambiguous source"
            );
        }
        let rsa = match &runner.rsa_public_key {
            Some(xml) => match preloop_gha_protocol::crypto::AgentRsaPublicKey::parse(xml) {
                Ok(key) => Some(key.to_xml_string().into_bytes()),
                Err(error) => {
                    notes.push(format!(
                        "runner {} rsa_public_key is unreadable ({error}); imported without it",
                        runner.runner_id
                    ));
                    None
                }
            },
            None => None,
        };
        let labels = crate::store::dedupe_labels_ci(&runner.labels);
        let seen = sessions
            .iter()
            .filter(|session| session.runner_id == runner.runner_id)
            .map(|session| session.last_seen_at_us)
            .max()
            .unwrap_or(runner.updated_at_us);
        tx.execute(
            "INSERT INTO runners (runner_id, namespace_id, name, labels, ephemeral, \
                 runner_group_id, runner_group_name, client_id, public_key, rsa_public_key, \
                 pool_proven, registered_at, last_seen_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                runner.runner_id,
                namespace,
                runner.name,
                serde_json::to_string(&labels).unwrap_or_else(|_| "[]".to_owned()),
                runner.ephemeral as i64,
                runner.runner_group_id,
                runner.runner_group_name,
                client_ids.get(&runner.runner_id),
                runner.public_key,
                rsa,
                pool_proven.contains(&runner.runner_id) as i64,
                runner.created_at_us,
                seen,
            ],
        )
        .with_context(|| format!("insert runner {}", runner.runner_id))?;
        imported.runners += 1;
    }
    if deleted > 0 {
        skipped.push(SkippedFamily {
            family: "runners (soft-deleted)".to_owned(),
            rows: deleted,
            reason: "legacy tombstones; a purged runner is not re-registered by the importer"
                .to_owned(),
        });
    }
    let mut closed_sessions = 0_u64;
    for session in &sessions {
        if session.closed_at_us.is_some() {
            closed_sessions += 1;
            continue;
        }
        tx.execute(
            "INSERT INTO runner_sessions (session_id, runner_id, protocol, client_id, \
                 verified, created_at, last_seen_at) \
             VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6)",
            params![
                session.session_id,
                session.runner_id,
                session.protocol,
                session.client_id,
                session.created_at_us,
                session.last_seen_at_us,
            ],
        )
        .with_context(|| format!("insert session {}", session.session_id))?;
        imported.runner_sessions += 1;
    }
    if closed_sessions > 0 {
        skipped.push(SkippedFamily {
            family: "runner_sessions (closed)".to_owned(),
            rows: closed_sessions,
            reason: "closed sessions carry no resumable state".to_owned(),
        });
    }
    if imported.runner_sessions > 0 {
        notes.push(
            "imported open runner sessions cannot resume: the control backend derives session \
             keys from the cluster key and the legacy per-session keys are not recoverable; \
             runners re-register and open a fresh session"
                .to_owned(),
        );
    }

    // ── Webhooks ────────────────────────────────────────────────────────
    let mut recovered = 0_u64;
    for delivery in &source.webhook_deliveries {
        let payload = std::str::from_utf8(&delivery.payload).with_context(|| {
            format!("webhook delivery {} payload is not UTF-8", delivery.delivery_id)
        })?;
        let installation_id = serde_json::from_str::<serde_json::Value>(payload)
            .ok()
            .and_then(|value| value.get("installation")?.get("id")?.as_i64());
        let (state, lease_until, lease_token) = if delivery.state == "processing" {
            // A processing row's worker died with the legacy process. Requeue
            // it explicitly instead of leaving a live lease behind; webhook
            // processing is idempotent per delivery id.
            recovered += 1;
            ("received", None, None)
        } else {
            (
                delivery.state.as_str(),
                delivery.lease_until_us,
                delivery.lease_token.clone(),
            )
        };
        tx.execute(
            "INSERT INTO webhook_deliveries (delivery_id, installation_id, event, payload, \
                 received_at, state, attempts, lease_until, lease_token, last_error) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                delivery.delivery_id,
                installation_id,
                delivery.event,
                payload,
                delivery.received_at_us,
                state,
                delivery.attempts,
                lease_until,
                lease_token,
                delivery.last_error,
            ],
        )
        .with_context(|| format!("insert webhook delivery {}", delivery.delivery_id))?;
        imported.webhook_deliveries += 1;
    }
    if recovered > 0 {
        notes.push(format!(
            "{recovered} webhook deliveries in 'processing' were re-queued as 'received' \
             (their lease holder does not survive the import)"
        ));
    }
    for cursor in &source.webhook_watchdog {
        tx.execute(
            "INSERT INTO webhook_watchdog (scope, cursor_delivered_at, cursor_delivery_guid, \
                 scan_cursor, last_poll_at, last_success_at) \
             VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                cursor.scope,
                cursor.cursor_delivered_at_us,
                cursor.cursor_delivered_at_guid,
                cursor.scan_cursor,
                cursor.last_poll_at_us,
                cursor.last_success_at_us,
            ],
        )?;
        imported.webhook_watchdog_cursors += 1;
    }
    for record in &source.webhook_redeliveries {
        tx.execute(
            "INSERT INTO webhook_redeliveries (delivery_guid, github_delivery_id, app_id, \
                 reason, attempts, first_seen_at, last_attempt_at, resolved_at, last_error) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                record.delivery_guid,
                record.github_delivery_id,
                record.app_id,
                record.reason,
                record.attempts,
                record.first_seen_at_us,
                record.last_attempt_at_us,
                record.resolved_at_us,
                record.last_error,
            ],
        )?;
        imported.webhook_redeliveries += 1;
    }

    // ── Meta-derived durable state ──────────────────────────────────────
    for (position, row) in meta_array(&source.meta, "job_assignments")?.iter().enumerate() {
        let fields = row
            .as_array()
            .filter(|fields| fields.len() == 5)
            .with_context(|| format!("legacy job_assignments row {position} is malformed"))?;
        let run_id = fields[0].as_str().context("job assignment run id")?;
        let job_id = fields[1].as_str().context("job assignment job id")?;
        let runner_id = fields[2].as_i64();
        let at = fields[3].as_u64().unwrap_or(0) as i64;
        let first_at = fields[4].as_u64().unwrap_or(0) as i64;
        tx.execute(
            "INSERT INTO job_assignments (run_id, job_id, runner_id, assigned_at, first_assigned_at) \
             VALUES (?1,?2,?3,?4,?5) ON CONFLICT (run_id, job_id) DO NOTHING",
            params![run_id, job_id, runner_id, at, first_at],
        )?;
        imported.job_assignments += 1;
    }
    for (position, row) in meta_array(&source.meta, "cancellation_queue")?.iter().enumerate() {
        let run_id = row
            .get("run_id")
            .and_then(|v| v.as_str())
            .with_context(|| format!("cancellation_queue row {position} has no run_id"))?;
        let agent_job_id = row
            .get("agent_job_id")
            .and_then(|v| v.as_str())
            .with_context(|| format!("cancellation_queue row {position} has no agent_job_id"))?;
        let request_id: Option<i64> = tx
            .query_row(
                "SELECT request_id FROM job_requests WHERE agent_job_id = ?1",
                [agent_job_id],
                |row| row.get(0),
            )
            .ok();
        let Some(request_id) = request_id else {
            notes.push(format!(
                "undelivered cancellation for {run_id} references unknown attempt \
                 {agent_job_id}; skipped"
            ));
            continue;
        };
        tx.execute(
            "INSERT INTO job_cancellations (request_id, reason, requested_at) VALUES (?1, ?2, ?3)",
            params![
                request_id,
                "imported from legacy cancellation queue",
                crate::store::now_us()
            ],
        )?;
        imported.job_cancellations += 1;
    }
    for (request_id, request) in &token_requests {
        // The attempt may have been dropped (evicted run): note it rather
        // than violate the FK.
        let exists: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM job_requests WHERE request_id = ?1)",
                [request_id],
                |row| row.get(0),
            )
            .unwrap_or(false);
        if !exists {
            notes.push(format!(
                "github token request for attempt {request_id} has no imported attempt; skipped"
            ));
            continue;
        }
        tx.execute(
            "INSERT INTO github_token_requests (request_id, repository, permissions, declared, untrusted) \
             VALUES (?1,?2,?3,?4,?5)",
            params![
                request_id,
                request.repository,
                serde_json::to_string(&request.permissions).unwrap_or_else(|_| "{}".to_owned()),
                request.declared as i64,
                request.untrusted as i64,
            ],
        )?;
        imported.github_token_requests += 1;
    }

    // ── Legacy event log -> outbox ──────────────────────────────────────
    // `control_events` is the legacy store's durable run/job status log. The
    // control schema's outbox is the same shape of append-only audit data, so
    // the rows are carried byte-for-byte (topic = the legacy event type) and
    // then age out under the normal outbox retention.
    let mut max_event_id = 0_i64;
    for event in &source.control_events {
        // Ids and order are preserved; `version` stays NULL (the legacy row
        // carries no ordering claim). This is event history: nothing sends
        // anything for these rows.
        tx.execute(
            "INSERT INTO outbox_events (event_id, namespace_id, run_id, job_id, version, topic, payload, created_at) \
             VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6, ?7)",
            params![
                event.event_id,
                namespace,
                event.run_id,
                event.job_id,
                event.event_type,
                event.payload_json,
                event.created_at_us,
            ],
        )
        .with_context(|| format!("carry legacy control event for {}", event.run_id))?;
        max_event_id = max_event_id.max(event.event_id);
        imported.outbox_events += 1;
    }
    seed_sequence(tx, "outbox_events", max_event_id)?;

    // ── Per-attempt job messages: verify or reconstruct ────────────────
    // The legacy store persists the runner-facing message per attempt. The
    // control schema regenerates it from the job template; each persisted
    // frame is verified against the (already imported) template, and when the
    // legacy store no longer keeps a queue row for the job (terminal jobs),
    // the frame itself reconstructs a sanitized template.
    let mut verified_frames = 0_u64;
    for (request_id, raw) in &source.job_request_messages {
        let associated_data = super::legacy::job_request_message_payload_aad(*request_id);
        let value = super::legacy::decode_message_payload(
            cipher,
            raw,
            "job_request_message",
            &associated_data,
        )?;
        let mut message: AgentJobRequestMessage = serde_json::from_value(value)
            .with_context(|| format!("decode legacy per-attempt message {request_id}"))?;
        let Some((run_id, job_id, agent_job_id)) = attempt_index.get(request_id) else {
            bail!(
                "legacy per-attempt message {request_id} names no imported attempt;                  refusing a referentially inconsistent source"
            );
        };
        if message.request_id != *request_id || message.job_id.to_string() != *agent_job_id {
            bail!(
                "legacy per-attempt message {request_id} does not match attempt {agent_job_id}"
            );
        }
        let stored: Option<String> = tx
            .prepare_cached(
                "SELECT message_template FROM job_messages WHERE run_id = ?1 AND job_id = ?2",
            )?
            .query_row(params![run_id, job_id], |row| row.get(0))
            .optional()?;
        match stored {
            Some(template) => {
                let decoded: Option<AgentJobRequestMessage> =
                    serde_json::from_str(&template).ok();
                let matches = decoded.is_some_and(|stored| {
                    stored.job_id.to_string() == *agent_job_id
                        && stored.request_id == *request_id
                });
                if !matches {
                    bail!(
                        "legacy per-attempt message {request_id} disagrees with the stored                          job template for {run_id}/{job_id}; refusing an inconsistent source"
                    );
                }
            }
            None => {
                let (secrets, names) = secrets_by_run.get(run_id).cloned().unwrap_or_default();
                let environment = None;
                sanitize_message(&mut message, environment, &names, &secrets, run_id, job_id)?;
                jobs::insert_job_message(
                    tx,
                    RunId(run_id.parse().unwrap_or_default()),
                    &JobId(job_id.clone()),
                    &message,
                    &Default::default(),
                )?;
            }
        }
        verified_frames += 1;
    }
    imported.job_request_messages = verified_frames;

    // ── Finalized legacy artifact records -> artifacts ─────────────────
    // `meta.artifacts` keys a finished artifact by storage token; the fields
    // map directly onto the control `artifacts` row (path = storage key).
    {
        let mut max_artifact_id = 0_i64;
        for row in meta_array(&source.meta, "artifacts")? {
            let pair = row
                .as_array()
                .filter(|pair| pair.len() == 2)
                .with_context(|| format!("legacy artifacts row is malformed: {row}"))?;
            let entry = &pair[1];
            let run_id = entry
                .get("run_id")
                .and_then(|value| value.as_str())
                .with_context(|| format!("legacy artifact {} has no run id", pair[0]))?;
            let name = entry
                .get("name")
                .and_then(|value| value.as_str())
                .context("legacy artifact has no name")?;
            let path = entry
                .get("path")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            // The public id is what the v1 endpoints address the artifact
            // by (the map key is the same id; prefer the record field).
            let public_id = entry
                .get("id")
                .and_then(|value| value.as_str())
                .filter(|id| !id.is_empty())
                .or_else(|| pair[0].as_str())
                .unwrap_or_default();
            let size = entry.get("size").and_then(|value| value.as_i64());
            let file_name = entry
                .get("file_name")
                .and_then(|value| value.as_str())
                .unwrap_or(name);
            // The bytes are copied beside the target under this stable
            // relative path; the legacy `path` names a retired state
            // directory, so it cannot be the storage key.
            let relative = format!("legacy-artifacts/{run_id}/{file_name}");
            let storage_key = state_dir.join(&relative).to_string_lossy().into_owned();
            let inserted = tx.execute(
                "INSERT INTO artifacts (namespace_id, run_id, job_backend_id, name, state, \
                     size_bytes, storage_key, public_id, created_at, finalized_at) \
                 VALUES (?1, ?2, '', ?3, 'finalized', ?4, ?5, ?6, ?7, ?7) \
                 ON CONFLICT (run_id, job_backend_id, name) DO NOTHING",
                params![
                    namespace,
                    run_id,
                    name,
                    size,
                    storage_key,
                    public_id,
                    crate::store::now_us()
                ],
            )?;
            if inserted > 0 {
                imported.artifacts += 1;
                max_artifact_id = max_artifact_id.max(1);
            }
            // Preserve the bytes too (validated against the recorded size):
            // the legacy store's own directory is retired with it.
            let source_file = legacy_state_dir.join(path);
            match std::fs::read(&source_file) {
                Ok(bytes) => {
                    if let Some(expected) = size
                        && bytes.len() as i64 != expected
                    {
                        bail!(
                            "legacy artifact {name} has {} bytes on disk but {expected}                              recorded; refusing to preserve a mismatched artifact",
                            bytes.len()
                        );
                    }
                    sidecars.stage(
                        state_dir,
                        &relative,
                        &bytes,
                        SidecarKind::ExactBytes,
                        cipher,
                    )?;
                    imported.legacy_artifact_bytes += 1;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    notes.push(format!(
                        "legacy artifact {name} has no file at {}; metadata was imported and                          the record is archived",
                        source_file.display()
                    ));
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("read legacy artifact file {}", source_file.display())
                    });
                }
            }
        }
        seed_sequence(tx, "artifacts", max_artifact_id)?;
    }

    // ── Buffered timeline events -> outbox + hydration sidecar ─────────
    // The legacy store buffered NDJSON events not yet folded into timeline
    // records, and `runs::run_events` seeds its initial snapshot from an
    // in-memory map that the control backend does not persist. The events are
    // carried into the outbox (queryable, retired by normal retention) and
    // additionally preserved in `<state-dir>/timeline_events.json`, the same
    // shape the legacy map held, so boot can hydrate the SSE history with a
    // loader next to the artifact_v2_registry.json one.
    let timeline_rows = meta_array(&source.meta, "timeline_events")?;
    if !timeline_rows.is_empty() {
        let bytes = serde_json::to_vec_pretty(&serde_json::Value::Array(timeline_rows.clone()))?;
        sidecars.stage(
            state_dir,
            "timeline_events.json",
            &bytes,
            SidecarKind::ExactBytes,
            cipher,
        )?;
        notes.push(
            "buffered timeline events were preserved in <state-dir>/timeline_events.json; \
             `runs::run_events` must load that sidecar at boot (or read the imported outbox) \
             for the SSE history to surface them"
                .to_owned(),
        );
    }
    for row in timeline_rows {
        let pair = row
            .as_array()
            .filter(|pair| pair.len() == 2)
            .with_context(|| format!("legacy timeline_events row is malformed: {row}"))?;
        let run_id = pair[0].as_str().context("timeline_events run id")?;
        let events = pair[1]
            .as_array()
            .context("timeline_events entries are not an array")?;
        for event in events {
            tx.execute(
                "INSERT INTO outbox_events (namespace_id, run_id, job_id, version, topic, payload, created_at) \
                 VALUES (?1, ?2, NULL, NULL, 'timeline.event.v1', ?3, ?4)",
                params![
                    namespace,
                    run_id,
                    serde_json::to_string(event)?,
                    crate::store::now_us()
                ],
            )?;
            imported.outbox_events += 1;
        }
    }

    // ── Unmapped durable leftovers: explicit, user-accessible archive ──
    // azdo_sessions and cache_v2_dl_tokens are ephemeral by construction
    // (session ids re-register; download tokens expire), so they are dropped
    // with a note, but the raw JSON of every family the importer does not
    // carry into a table is written to an archive file beside the target so
    // historical data remains reachable and auditable.
    {
        let mut archive = serde_json::Map::new();
        for (key, family) in [
            ("artifacts", "artifacts"),
            ("timeline_events", "timeline_events"),
            ("azdo_sessions", "azdo_sessions"),
            ("cache_v2_dl_tokens", "cache_v2_dl_tokens"),
        ] {
            if let Some(rows) = source.meta.get(family).and_then(|value| value.as_array())
                && !rows.is_empty()
            {
                archive.insert(key.to_owned(), serde_json::Value::Array(rows.clone()));
            }
        }
        if !archive.is_empty() {
            let bytes = serde_json::to_vec_pretty(&serde_json::Value::Object(archive))?;
            sidecars.stage(
                state_dir,
                "legacy-import-archive.json",
                &bytes,
                SidecarKind::ExactBytes,
                cipher,
            )?;
            notes.push(
                "unmapped durable leftovers (legacy artifacts, buffered timeline events, \
                 ephemeral session ids/download tokens) were written to \
                 <state-dir>/legacy-import-archive.json; azdo_sessions and cache_v2_dl_tokens \
                 are ephemeral and intentionally not re-issued"
                    .to_owned(),
            );
        }
    }

    // ── Legacy artifact registry -> artifacts ──────────────────────────
    {
        let mut registry: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        if let Some(map) = source
            .meta
            .get("artifact_v2_registry")
            .and_then(|value| value.as_object())
        {
            for (key, entry) in map {
                registry.insert(key.clone(), entry.clone());
            }
        }
        if let Some(sidecar) = &source.artifact_registry_sidecar {
            for (key, entry) in sidecar {
                registry.insert(key.clone(), entry.clone());
            }
        }
        let mut max_artifact_id = 0_i64;
        // The v2 artifact service reads its registry from
        // `<state-dir>/artifact_v2_registry.json` and the bytes from
        // `<state-dir>/blobs/artifact/<token>/data`; carry both so migrated
        // artifacts stay downloadable after the switch.
        let mut serving_registry = serde_json::Map::new();
        let mut copied_blobs = 0_u64;
        let mut missing_blobs = 0_u64;
        for (key, entry) in registry {
            let run_id = entry
                .get("workflow_run_backend_id")
                .and_then(|value| value.as_str())
                .with_context(|| format!("artifact registry entry {key} has no run id"))?;
            let job_backend_id = entry
                .get("workflow_job_run_backend_id")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let name = entry
                .get("name")
                .and_then(|value| value.as_str())
                .with_context(|| format!("artifact registry entry {key} has no name"))?;
            let storage_key = entry
                .get("blob_token")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let size = entry.get("size").and_then(|value| value.as_i64());
            let digest = entry
                .get("digest")
                .and_then(|value| value.as_str())
                .map(str::to_owned);
            let created_at = entry
                .get("created_at")
                .and_then(|value| value.as_str())
                .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
                .map(|at| at.timestamp_micros())
                .unwrap_or_else(crate::store::now_us);
            let artifact_id = entry.get("id").and_then(|value| value.as_i64()).unwrap_or(0);
            let inserted = tx.execute(
                "INSERT INTO artifacts (artifact_id, namespace_id, run_id, job_backend_id,                      name, state, size_bytes, digest, storage_key, created_at, finalized_at)                  VALUES (?1, ?2, ?3, ?4, ?5, 'finalized', ?6, ?7, ?8, ?9, ?9)                  ON CONFLICT (run_id, job_backend_id, name) DO NOTHING",
                params![
                    artifact_id,
                    namespace,
                    run_id,
                    job_backend_id,
                    name,
                    size,
                    digest,
                    storage_key,
                    created_at,
                ],
            )?;
            if inserted > 0 {
                imported.artifacts += 1;
                max_artifact_id = max_artifact_id.max(artifact_id);
            }
            // Copy the finalized blob and expose the entry through the
            // runtime registry (keyed `{run_id}/{name}`).
            let Some(token) = uuid::Uuid::parse_str(storage_key).ok() else {
                missing_blobs += 1;
                notes.push(format!(
                    "artifact registry entry {key} has a non-UUID blob token {storage_key:?}; \
                     the row was imported but its blob is not served"
                ));
                continue;
            };
            let source_blob = legacy_state_dir
                .join("blobs")
                .join("artifact")
                .join(token.to_string())
                .join("data");
            match std::fs::read(&source_blob) {
                Ok(bytes) => {
                    if let Some(expected) = size
                        && bytes.len() as i64 != expected
                    {
                        notes.push(format!(
                            "legacy artifact {name} blob is {} bytes but the registry records \
                             {expected}",
                            bytes.len()
                        ));
                    }
                    let relative = format!("blobs/artifact/{token}/data");
                    sidecars.stage(
                        state_dir,
                        &relative,
                        &bytes,
                        SidecarKind::ExactBytes,
                        cipher,
                    )?;
                    copied_blobs += 1;
                    // The key is the registry's own identity (the canonical
                    // run id, which can differ from the entry's
                    // `workflow_run_backend_id`); copy it verbatim — the boot
                    // loader migrates pre-existing key shapes itself.
                    serving_registry.insert(key.clone(), entry.clone());
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    missing_blobs += 1;
                    notes.push(format!(
                        "legacy artifact {name} blob {token} is missing from the legacy state \
                         dir; the row was imported but its download is not served"
                    ));
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("read legacy artifact blob {}", source_blob.display())
                    });
                }
            }
        }
        seed_sequence(tx, "artifacts", max_artifact_id)?;
        if !serving_registry.is_empty() {
            let bytes = serde_json::to_vec(&serde_json::Value::Object(serving_registry))?;
            sidecars.stage(
                state_dir,
                "artifact_v2_registry.json",
                &bytes,
                SidecarKind::ExactBytes,
                cipher,
            )?;
        }
        if copied_blobs > 0 || missing_blobs > 0 {
            notes.push(format!(
                "{copied_blobs} finalized artifact blob(s) copied for serving; \
                 {missing_blobs} registry entr(ies) had no usable blob"
            ));
        }
    }

    // ── Logs ────────────────────────────────────────────────────────────
    import_logs(
        tx,
        &logs,
        &source.meta,
        sidecars,
        state_dir,
        cipher,
        &mut imported,
        &mut skipped,
        &mut notes,
    )?;

    // ── Sequences ───────────────────────────────────────────────────────
    let next_runner = source
        .meta
        .get("next_runner_id")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let next_message = source
        .meta
        .get("next_message_id")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    seed_sequence(tx, "runners", next_runner.saturating_sub(1))?;
    seed_sequence(tx, "session_messages", next_message.max(1_000_000))?;

    // ── Explicitly skipped families ─────────────────────────────────────
    for (family, count, reason) in skipped_families(source, policy) {
        if count > 0 {
            skipped.push(SkippedFamily {
                family: family.to_owned(),
                rows: count,
                reason: reason.to_owned(),
            });
        }
    }

    Ok(WriteOutcome {
        imported,
        skipped,
        notes,
    })
}

/// Reduce a legacy job message to the control backend's secret-free template.
///
/// The legacy message was built with real secret values; the stored template
/// must carry names only (they are resolved again at acquire). The strip is
/// the same one the live submit path applies, plus a final scan that refuses
/// to persist a message still containing any of the run's secret values.
fn sanitize_message(
    message: &mut AgentJobRequestMessage,
    environment: Option<String>,
    run_secret_names: &BTreeSet<String>,
    run_secrets: &BTreeMap<String, String>,
    run_id: &str,
    job_id: &str,
) -> anyhow::Result<()> {
    let names: BTreeSet<String> = message
        .variables
        .iter()
        .filter(|(_, value)| value.is_secret == Some(true))
        .map(|(name, _)| name.clone())
        .collect();
    let hints = crate::message_template::secret_hint_count(message);
    crate::message_template::strip_template(message, hints);
    // Token variables are minted per claim; a legacy template must not carry
    // (or invite re-use of) the old token value even if it was stored
    // non-secret. The live build drops them with the secret variables.
    message.variables.remove("github_token");
    message.variables.remove("system.github.token");
    if !names.is_empty() || !run_secret_names.is_empty() || environment.is_some() {
        message.preloop_secret_spec = Some(MessageSecretSpec {
            names,
            environment,
            inherit: false,
            map: BTreeMap::new(),
            run_names: run_secret_names.clone(),
        });
    }
    // Leases are re-minted per claim; a legacy lease timestamp would be stale
    // (and its holder gone).
    message.locked_until = String::new();
    let serialized = serde_json::to_string(message)?;
    for (name, value) in run_secrets {
        if !value.is_empty() && serialized.contains(value.as_str()) {
            bail!(
                "run {run_id} job {job_id}: secret {name} still appears in the job message \
                 after stripping; refusing to write a secret value into the control database"
            );
        }
    }
    Ok(())
}

/// Write the immutable shell of a job that the legacy store never queued.
fn insert_synthesized_job(
    tx: &Transaction<'_>,
    run_id: &str,
    job_id: &str,
    status: ExecutionStatus,
    order: i64,
    run_order: i64,
    record: &crate::models::RunRecord,
) -> anyhow::Result<()> {
    let job_key = JobId(job_id.to_owned());
    let display_name = record
        .job_names
        .get(&job_key)
        .cloned()
        .unwrap_or_else(|| job_id.to_owned());
    let base_id = record
        .job_base_ids
        .get(&job_key)
        .cloned()
        .unwrap_or_else(|| job_id.to_owned());
    let needs = record.job_needs.get(&job_key).cloned().unwrap_or_default();
    let remaining = needs
        .iter()
        .filter(|need| {
            record
                .jobs
                .get(*need)
                .map(|status| !terminal(*status))
                .unwrap_or(false)
        })
        .count() as i32;
    tx.execute(
        "INSERT INTO jobs (run_id, job_id, namespace_id, kind, base_id, status, queue_state, \
             remaining_needs, pool_key, runs_on, priority, run_order, job_order, created_at) \
         VALUES (?1,?2,'default',?3,?4,?5,?6,?7,'', '[]', 0, ?8, ?9, ?10)",
        params![
            run_id,
            job_id,
            if base_id == job_id { "job" } else { "matrix_leg" },
            base_id,
            crate::control::types::status_str(status),
            if status.is_terminal() { "none" } else { "blocked" },
            remaining,
            run_order,
            order,
            record.created_at.timestamp_micros(),
        ],
    )
    .with_context(|| format!("insert synthesized job {}/{}", run_id, job_id))?;
    tx.execute(
        "INSERT INTO job_specs (run_id, job_id, display_name, display_order, matrix) \
         VALUES (?1,?2,?3,?4,'{}')",
        params![run_id, job_id, display_name, order],
    )?;
    let mut position = 0_i64;
    for need in &needs {
        tx.execute(
            "INSERT INTO job_needs (run_id, job_id, needs_job_id, position) VALUES (?1,?2,?3,?4)",
            params![run_id, job_id, need.0, position],
        )?;
        position += 1;
    }
    Ok(())
}

fn insert_step(
    tx: &Transaction<'_>,
    agent_job_id: &str,
    position: i64,
    step: &LegacyStep,
) -> anyhow::Result<()> {
    let record = step.step_record();
    tx.execute(
        "INSERT INTO job_steps (agent_job_id, step_id, position, kind, workflow_index, \
             runner_number, context_name, name, conclusion, started_at, finished_at) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11) ON CONFLICT DO NOTHING",
        params![
            agent_job_id,
            record.id,
            position,
            match record.kind {
                crate::models::StepKind::Workflow => "workflow",
                crate::models::StepKind::Synthetic => "synthetic",
            },
            record.workflow_index.map(|index| index as i64),
            record.runner_number.map(|number| number as i64),
            record.context_name,
            record.name,
            record.conclusion,
            record.started_at.map(|at| at.timestamp_micros()),
            record.finished_at.map(|at| at.timestamp_micros()),
        ],
    )?;
    Ok(())
}

fn job_times(
    attempts: &[LegacyAttempt],
    record: &crate::models::RunRecord,
    is_terminal: bool,
) -> (Option<i64>, Option<i64>) {
    let started = attempts
        .iter()
        .filter_map(|attempt| attempt.snapshot.started_at_us)
        .min();
    let completed = if is_terminal {
        record
            .completed_at
            .map(|at| at.timestamp_micros())
            .or_else(|| {
                attempts
                    .iter()
                    .filter_map(|attempt| {
                        attempt
                            .snapshot
                            .result
                            .map(|_| attempt.snapshot.last_renewed_at_us.or(attempt.snapshot.started_at_us))
                    })
                    .flatten()
                    .max()
            })
    } else {
        None
    };
    (started, completed)
}

fn oidc_for(
    meta: &serde_json::Map<String, serde_json::Value>,
    run_id: &str,
    job_id: &str,
) -> anyhow::Result<(Option<String>, Option<String>, Option<String>, bool)> {
    let mut environment = None;
    let mut job_workflow_ref = None;
    let mut job_workflow_sha = None;
    for row in meta_array(meta, "oidc_job_contexts")? {
        let fields = row
            .as_array()
            .filter(|fields| fields.len() == 3)
            .context("legacy oidc_job_contexts row is malformed")?;
        if fields[0].as_str() == Some(run_id) && fields[1].as_str() == Some(job_id) {
            let context = &fields[2];
            environment = context
                .get("environment")
                .and_then(|v| v.as_str())
                .map(str::to_owned);
            job_workflow_ref = context
                .get("job_workflow_ref")
                .and_then(|v| v.as_str())
                .map(str::to_owned);
            job_workflow_sha = context
                .get("job_workflow_sha")
                .and_then(|v| v.as_str())
                .map(str::to_owned);
        }
    }
    let mut granted = false;
    for row in meta_array(meta, "id_token_grants")? {
        let fields = row
            .as_array()
            .filter(|fields| fields.len() == 3)
            .context("legacy id_token_grants row is malformed")?;
        if fields[0].as_str() == Some(run_id)
            && fields[1].as_str() == Some(job_id)
            && fields[2].as_bool().unwrap_or(false)
        {
            granted = true;
        }
    }
    Ok((environment, job_workflow_ref, job_workflow_sha, granted))
}

fn import_timelines(
    tx: &Transaction<'_>,
    meta: &serde_json::Map<String, serde_json::Value>,
    imported: &mut ImportedRows,
) -> anyhow::Result<()> {
    let mut change_ids: BTreeMap<String, i64> = BTreeMap::new();
    for row in meta_array(meta, "timeline_change_ids")? {
        let fields = row
            .as_array()
            .filter(|fields| fields.len() == 2)
            .context("legacy timeline_change_ids row is malformed")?;
        if let (Some(id), Some(change)) = (fields[0].as_str(), fields[1].as_i64()) {
            change_ids.insert(id.to_owned(), change);
        }
    }
    for row in meta_array(meta, "timeline_records")? {
        let fields = row
            .as_array()
            .filter(|fields| fields.len() == 2)
            .context("legacy timeline_records row is malformed")?;
        let timeline_id = fields[0].as_str().context("timeline id")?;
        let records = fields[1]
            .as_array()
            .context("timeline records are not an array")?;
        let mut max_change = 0_i32;
        for record in records {
            let change = record
                .get("change_id")
                .or_else(|| record.get("changeId"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as i32;
            max_change = max_change.max(change);
        }
        let timeline_change = change_ids
            .get(timeline_id)
            .copied()
            .unwrap_or(0)
            .max(max_change as i64) as i32;
        tx.execute(
            "INSERT INTO timelines (timeline_id, change_id) VALUES (?1, ?2) \
             ON CONFLICT (timeline_id) DO UPDATE SET change_id = MAX(change_id, excluded.change_id)",
            params![timeline_id, timeline_change],
        )?;
        imported.timelines += 1;
        for record in records {
            let record_id = record
                .get("id")
                .and_then(|v| v.as_str())
                .context("timeline record without an id")?;
            let change = record
                .get("change_id")
                .or_else(|| record.get("changeId"))
                .and_then(|v| v.as_i64())
                .unwrap_or(timeline_change as i64) as i32;
            tx.execute(
                "INSERT INTO timeline_records (timeline_id, record_id, change_id, record) \
                 VALUES (?1, ?2, ?3, ?4) ON CONFLICT DO NOTHING",
                params![timeline_id, record_id, change, record.to_string()],
            )?;
            imported.timeline_records += 1;
        }
    }
    Ok(())
}

/// Legacy log keys are `results:{plan}:{job}:{kind}[:{resource}]`; the control
/// backend keys logs `{plan}/{log_id}` and stores bytes under
/// `<state_dir>/live-logs` in the `LiveLogSegments` layout.
fn import_logs(
    tx: &Transaction<'_>,
    logs: &[LegacyLogFile],
    meta: &serde_json::Map<String, serde_json::Value>,
    sidecars: &mut Sidecars,
    state_dir: &Path,
    cipher: &Envelope,
    imported: &mut ImportedRows,
    skipped: &mut Vec<SkippedFamily>,
    notes: &mut Vec<String>,
) -> anyhow::Result<()> {
    // Merge the table rows with the runtime snapshot's metadata map: an entry
    // can exist in either (chunk pruning vs. row rewrite timing).
    let mut merged: BTreeMap<String, (i64, i64, i64, Vec<Vec<u8>>)> = BTreeMap::new();
    for log in logs {
        merged.insert(
            log.log_key.clone(),
            (
                log.byte_count,
                log.line_count,
                log.updated_at_us,
                log.chunks.clone(),
            ),
        );
    }
    for row in meta_array(meta, "log_metadata")? {
        let fields = row
            .as_array()
            .filter(|fields| fields.len() == 2)
            .context("legacy log_metadata row is malformed")?;
        let key = fields[0].as_str().context("log metadata key")?;
        let byte_count = fields[1]
            .get("byte_count")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let line_count = fields[1]
            .get("line_count")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        merged
            .entry(key.to_owned())
            .or_insert((byte_count, line_count, 0, Vec::new()));
    }

    let mut plan_by_agent: BTreeMap<String, String> = BTreeMap::new();
    {
        let mut stmt = tx.prepare("SELECT agent_job_id, run_id FROM job_requests")?;
        for row in stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })? {
            let (agent, run_id) = row?;
            plan_by_agent.insert(agent, run_id);
        }
    }
    let mut next_log_id: BTreeMap<String, i64> = BTreeMap::new();
    let mut unmapped = 0_u64;
    let mut pruned = 0_u64;
    for (key, (byte_count, _line_count, updated_at_us, chunks)) in merged {
        let Some(plan) = legacy_log_plan(&key) else {
            unmapped += 1;
            continue;
        };
        let Some(run_id) = plan_by_agent.get(&plan) else {
            unmapped += 1;
            continue;
        };
        let bytes: Vec<u8> = chunks.concat();
        let actual_bytes = bytes.len() as i64;
        let actual_lines = bytes.iter().filter(|byte| **byte == b'\n').count() as i64;
        let log_id = {
            let next = next_log_id.entry(plan.clone()).or_insert(1);
            let id = *next;
            *next += 1;
            id
        };
        let log_key = format!("{plan}/{log_id}");
        tx.execute(
            "INSERT INTO log_files (log_key, run_id, plan_id, log_id, byte_count, line_count, updated_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![
                log_key,
                run_id,
                plan,
                log_id,
                actual_bytes,
                actual_lines,
                if updated_at_us > 0 {
                    updated_at_us
                } else {
                    crate::store::now_us()
                },
            ],
        )?;
        if !bytes.is_empty() {
            write_log_segments(sidecars, state_dir, &plan, log_id, &bytes, cipher)?;
        }
        if byte_count > actual_bytes {
            pruned += 1;
        }
        imported.log_files += 1;
        imported.log_bytes += actual_bytes.max(0) as u64;
    }
    if unmapped > 0 {
        skipped.push(SkippedFamily {
            family: "log files (unmapped)".to_owned(),
            rows: unmapped,
            reason: "legacy log keys name no imported attempt".to_owned(),
        });
    }
    if pruned > 0 {
        notes.push(format!(
            "{pruned} logs had bytes pruned in the legacy store; imported counts reflect the \
             bytes that remain"
        ));
    }
    Ok(())
}

/// `results:{plan}:{job}:{kind}[:{resource}]` -> `{plan}` when it is a
/// UUID-shaped attempt id.
fn legacy_log_plan(key: &str) -> Option<String> {
    let rest = key.strip_prefix("results:")?;
    let plan = rest.split(':').next()?;
    if plan.parse::<uuid::Uuid>().is_ok() {
        Some(plan.to_owned())
    } else {
        None
    }
}

/// Mirror `LiveLogSegments::publish` into the sidecar staging area:
/// `live-logs/<hex(plan)>/<hex(log)>/seg-<seq>.log`. Publishing (after the
/// database is verified) links the staged file into place and never replaces
/// an existing segment.
fn write_log_segments(
    sidecars: &mut Sidecars,
    state_dir: &Path,
    plan: &str,
    log_id: i64,
    bytes: &[u8],
    cipher: &Envelope,
) -> anyhow::Result<()> {
    fn component(id: &str) -> anyhow::Result<String> {
        if id.is_empty() || id.len() > 128 {
            bail!("invalid log identity {id:?}");
        }
        let mut encoded = String::with_capacity(id.len() * 2);
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in id.bytes() {
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0xf) as usize] as char);
        }
        Ok(encoded)
    }
    let relative = format!(
        "live-logs/{}/{}/seg-00000000000000000001.log",
        component(plan)?,
        component(&log_id.to_string())?
    );
    sidecars.stage(state_dir, &relative, bytes, SidecarKind::ExactBytes, cipher)
}

/// Stage the run's secret values as the SecretProvider's run tier
/// (`run-secrets/<run_id>`, sealed with the cluster key). Publishing happens
/// with the rest of the sidecars after the database is verified; a
/// destination that already decrypts to the same map is adopted so a retried
/// import is idempotent.
fn write_run_secrets(
    sidecars: &mut Sidecars,
    state_dir: &Path,
    run_id: &str,
    secrets: &BTreeMap<String, String>,
    cipher: &Envelope,
) -> anyhow::Result<()> {
    let sealed = cipher.seal(&serde_json::to_vec(secrets)?)?;
    sidecars.stage(
        state_dir,
        &format!("run-secrets/{run_id}"),
        &sealed,
        SidecarKind::SealedStringMap,
        cipher,
    )
}

/// Seed an AUTOINCREMENT sequence so ids of rows the importer could not
/// restore are never reused. `sqlite_sequence` has no unique index on
/// `name`, so this is an UPDATE-then-INSERT rather than an upsert.
fn seed_sequence(tx: &Transaction<'_>, table: &str, seq: i64) -> anyhow::Result<()> {
    if seq <= 0 {
        return Ok(());
    }
    let updated = tx.execute(
        "UPDATE sqlite_sequence SET seq = MAX(seq, ?2) WHERE name = ?1",
        params![table, seq],
    )?;
    if updated == 0 {
        tx.execute(
            "INSERT INTO sqlite_sequence (name, seq) VALUES (?1, ?2)",
            params![table, seq],
        )?;
    }
    Ok(())
}

/// Families the importer does not carry, with reasons. `policy` decides how
/// concurrency gate state is reported (never silently).
fn skipped_families(
    source: &SourceData,
    policy: ActivePolicy,
) -> Vec<(&'static str, u64, &'static str)> {
    let count = |key: &str| meta_count(&source.meta, key);
    let gate_count = count("concurrency_groups") + count("jobset_admissions");
    let mut families: Vec<(&'static str, u64, &'static str)> = vec![];
    if gate_count > 0 {
        let reason = if policy == ActivePolicy::Refuse {
            "live concurrency gates; refused (rerun with --active=requeue|cancel to release them)"
        } else {
            "live concurrency gates released by the explicit --active policy"
        };
        families.push(("meta.concurrency_gates", gate_count, reason));
    }
    families
}
