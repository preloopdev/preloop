//! Read paths over runs and jobs, the check-run mapping, push state, and
//! archival. Reads that served archived runs fall back to the `*_history`
//! tables: an archived run has no `runs` row.

use super::codec::{self, now_us};
use super::{LiteBackend, db};
use crate::control::types::*;
use crate::models::{JobDetail, PushState, PushStatus};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use rusqlite::{OptionalExtension, params};
use std::collections::{BTreeMap, BTreeSet};
type JobQueryRow = (
    String,
    String,
    Option<String>,
    Option<i64>,
    Option<String>,
    bool,
);

/// Keep a run live while its push state may still dedup an echo webhook:
/// GitHub keeps deliveries redeliverable for three days.
const PUSH_ECHO_WINDOW_US: i64 = 3 * 24 * 60 * 60 * 1_000_000;

/// Let late runner callbacks settle before a terminal run is archived.
const ARCHIVE_GRACE_US: i64 = 60 * 1_000_000;

/// Largest archive batch per transaction.
const ARCHIVE_BATCH: usize = 64;

/// Whether a run row (live or archived) exists.
fn run_known(tx: &rusqlite::Transaction<'_>, run: &str) -> Result<bool, ControlError> {
    tx.prepare_cached(
        "SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = ?1) \
             OR EXISTS (SELECT 1 FROM run_history WHERE run_id = ?1)",
    )
    .map_err(db)?
    .query_row([run], |row| row.get(0))
    .map_err(db)
}

/// The newest attempt of a run and whether its live `runs` row still holds
/// it (`false` once the run is archived). `None` when the run is unknown.
fn latest_attempt(
    tx: &rusqlite::Transaction<'_>,
    run: &str,
) -> Result<Option<(i64, bool)>, ControlError> {
    let live: Option<i64> = tx
        .prepare_cached("SELECT run_attempt FROM runs WHERE run_id = ?1")
        .map_err(db)?
        .query_row([run], |row| row.get(0))
        .optional()
        .map_err(db)?;
    if let Some(attempt) = live {
        return Ok(Some((attempt, true)));
    }
    let archived: Option<i64> = tx
        .prepare_cached("SELECT MAX(run_attempt) FROM run_history WHERE run_id = ?1")
        .map_err(db)?
        .query_row([run], |row| row.get(0))
        .map_err(db)?;
    Ok(archived.map(|attempt| (attempt, false)))
}

/// One attempt an attempt read addresses, checked against the run's attempts.
struct ResolvedAttempt {
    number: i64,
    /// The run's newest attempt: read through the ordinary record path.
    newest: bool,
    /// The newest attempt while the run is still live: its execution
    /// pointers are the live `jobs` rows.
    live: bool,
}

/// Resolve a 1-based attempt of `run_id`. `NotFound` when the run is unknown
/// or the attempt is outside `1..=newest`.
fn resolve_attempt(
    tx: &rusqlite::Transaction<'_>,
    run_id: RunId,
    attempt: u64,
) -> Result<ResolvedAttempt, ControlError> {
    let (newest_number, live_run) = latest_attempt(tx, &codec::run_key(run_id))?
        .ok_or_else(|| ControlError::NotFound(format!("run {run_id} not found")))?;
    let number = i64::try_from(attempt)
        .ok()
        .filter(|number| (1..=newest_number).contains(number))
        .ok_or_else(|| ControlError::NotFound(format!("run {run_id} has no attempt {attempt}")))?;
    let newest = number == newest_number;
    Ok(ResolvedAttempt {
        number,
        newest,
        live: newest && live_run,
    })
}

/// The SQL window over request rows aliased `q` that selects one attempt's
/// requests. `?2` binds the attempt number and `?3` the live-newest flag.
///
/// Each job has an execution pointer for the attempt: its live `jobs` row
/// when the attempt is the live newest, else its `job_history` snapshot. A
/// request belongs to the attempt when it is that pointer, or when it lies in
/// `(floor, pointer]`. The floor is the newest pointer any earlier attempt
/// snapshotted, so the window holds the requests minted during the attempt.
/// A pointer cleared by a rerun (the job concluded without minting) is
/// `NULL` and contributes nothing; the floor ignores it, so requests minted
/// before the clear stay out of later attempts.
fn attempt_window() -> String {
    let pointer = "CASE WHEN ?3 THEN (SELECT j.request_id FROM jobs j \
                       WHERE j.run_id = q.run_id AND j.job_id = q.job_id) \
                   ELSE (SELECT h.request_id FROM job_history h \
                       WHERE h.run_id = q.run_id AND h.job_id = q.job_id \
                       AND h.run_attempt = ?2) END";
    let floor = "COALESCE((SELECT MAX(h.request_id) FROM job_history h \
                   WHERE h.run_id = q.run_id AND h.job_id = q.job_id \
                   AND h.run_attempt < ?2), 0)";
    format!(
        "(q.request_id = ({pointer}) OR \
         (q.request_id > {floor} AND q.request_id <= ({pointer})))"
    )
}

fn submission_value(json: &str) -> Result<serde_json::Value, ControlError> {
    serde_json::from_str(json).map_err(ControlError::backend)
}

fn push_status_str(status: PushStatus) -> &'static str {
    match status {
        PushStatus::Pending => "pending",
        PushStatus::Synced => "synced",
        PushStatus::Blocked => "blocked",
    }
}

impl LiteBackend {
    /// Every job id of the run (jobs ∪ its attempts' job ids; history for an
    /// archived run), sorted. `NotFound` when the run is unknown.
    pub(crate) async fn run_job_ids(&self, run_id: RunId) -> Result<Vec<String>, ControlError> {
        let run = codec::run_key(run_id);
        self.read(|tx| {
            if !run_known(tx, &run)? {
                return Err(ControlError::NotFound("run not found".to_owned()));
            }
            let mut stmt = tx
                .prepare_cached(
                    "SELECT job_id FROM jobs WHERE run_id = ?1 \
                     UNION SELECT job_id FROM job_requests WHERE run_id = ?1 \
                     UNION SELECT job_id FROM job_history WHERE run_id = ?1 \
                     UNION SELECT job_id FROM attempt_history WHERE run_id = ?1 \
                     ORDER BY 1",
                )
                .map_err(db)?;
            let rows = stmt.query_map([&run], |row| row.get(0)).map_err(db)?;
            rows.collect::<Result<_, _>>().map_err(db)
        })
    }

    /// Record the job's check-run id: one conditional UPDATE of the `jobs`
    /// row plus an upsert into `run_submissions.record_details` — the store
    /// of record for jobs with no row yet (a matrix leg minted before its
    /// expansion materializes). Returns whether the mapping changed.
    pub(crate) async fn set_job_check_run(
        &self,
        run_id: RunId,
        job_id: &JobId,
        check_run_id: u64,
    ) -> Result<bool, ControlError> {
        let run = codec::run_key(run_id);
        self.write(|tx| {
            let changed = tx
                .prepare_cached(
                    "UPDATE jobs SET check_run_id = ?3 \
                     WHERE run_id = ?1 AND job_id = ?2 AND check_run_id IS NOT ?3",
                )
                .map_err(db)?
                .execute(params![run, job_id.0, check_run_id as i64])
                .map_err(db)?;
            // A job id with `"` cannot sit in a JSON path literal — the
            // fragment embeds it escaped (`\"`), which json_set reads as the
            // key verbatim.
            let detail_changed = tx
                .prepare_cached(
                    "UPDATE run_submissions SET record_details = json_set(record_details, \
                         '$.\"job_check_run_ids\".\"' || replace(?2, '\"', '\\\"') || '\"', ?3) \
                     WHERE run_id = ?1 \
                       AND COALESCE(json_extract(record_details, \
                           '$.\"job_check_run_ids\".\"' || replace(?2, '\"', '\\\"') || '\"'), -1) <> ?3",
                )
                .map_err(db)?
                .execute(params![run, job_id.0, check_run_id as i64])
                .map_err(db)?;
            let mapping_changed = changed > 0 || detail_changed > 0;
            if mapping_changed {
                // The mapping's `CheckRunCreated` event persists atomically
                // with it; the caller broadcasts via `emit_persisted`.
                super::lifecycle::append_event_tx(
                    tx,
                    &preloop_gha_protocol::NdjsonEvent::CheckRunCreated { run_id },
                )?;
            }
            Ok(mapping_changed)
        })
    }

    /// Clear the mapping only while it still points at `expected` — in the
    /// `jobs` row and in `record_details` (the latter holds legs minted
    /// before materialization; a stale mapping there would re-mint a
    /// deleted check run forever).
    pub(crate) async fn clear_job_check_run(
        &self,
        run_id: RunId,
        job_id: &JobId,
        expected: u64,
    ) -> Result<(), ControlError> {
        let run = codec::run_key(run_id);
        self.write(|tx| {
            tx.prepare_cached(
                "UPDATE jobs SET check_run_id = NULL \
                 WHERE run_id = ?1 AND job_id = ?2 AND check_run_id = ?3",
            )
            .map_err(db)?
            .execute(params![run, job_id.0, expected as i64])
            .map_err(db)?;
            tx.prepare_cached(
                "UPDATE run_submissions SET record_details = json_remove(record_details, \
                     '$.\"job_check_run_ids\".\"' || replace(?2, '\"', '\\\"') || '\"') \
                 WHERE run_id = ?1 \
                   AND json_extract(record_details, \
                       '$.\"job_check_run_ids\".\"' || replace(?2, '\"', '\\\"') || '\"') = ?3",
            )
            .map_err(db)?
            .execute(params![run, job_id.0, expected as i64])
            .map_err(db)?;
            Ok(())
        })
    }

    /// Whether the live run has a `jobs` row for `job_id`.
    pub(crate) async fn job_exists(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<bool, ControlError> {
        let run = codec::run_key(run_id);
        self.read(|tx| {
            tx.prepare_cached(
                "SELECT EXISTS (SELECT 1 FROM jobs WHERE run_id = ?1 AND job_id = ?2)",
            )
            .map_err(db)?
            .query_row(params![run, job_id.0], |row| row.get(0))
            .map_err(db)
        })
    }

    /// The job's evaluated display name (`job_specs`, else `job_history`).
    pub(crate) async fn job_display_name(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<String>, ControlError> {
        let run = codec::run_key(run_id);
        self.read(|tx| {
            tx.prepare_cached(
                "SELECT display_name FROM job_specs WHERE run_id = ?1 AND job_id = ?2 \
                 UNION ALL \
                 SELECT display_name FROM job_history h WHERE h.run_id = ?1 AND h.job_id = ?2 \
                   AND NOT EXISTS (SELECT 1 FROM jobs j \
                       WHERE j.run_id = h.run_id AND j.job_id = h.job_id) \
                   AND h.run_attempt = (SELECT MAX(run_attempt) FROM job_history \
                       WHERE run_id = h.run_id AND job_id = h.job_id) \
                 LIMIT 1",
            )
            .map_err(db)?
            .query_row(params![run, job_id.0], |row| row.get(0))
            .optional()
            .map_err(db)
        })
    }

    /// The job's recorded check-run id: the live (then archived) `jobs`
    /// row, else `record_details` — legs minted before their expansion
    /// materializes have no row.
    pub(crate) async fn job_check_run_id(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<u64>, ControlError> {
        let run = codec::run_key(run_id);
        self.read(|tx| {
            let row_id = tx
                .prepare_cached(
                    "SELECT check_run_id FROM jobs WHERE run_id = ?1 AND job_id = ?2 \
                     UNION ALL \
                     SELECT check_run_id FROM job_history h WHERE h.run_id = ?1 AND h.job_id = ?2 \
                       AND NOT EXISTS (SELECT 1 FROM jobs j \
                           WHERE j.run_id = h.run_id AND j.job_id = h.job_id) \
                       AND h.run_attempt = (SELECT MAX(run_attempt) FROM job_history \
                           WHERE run_id = h.run_id AND job_id = h.job_id) \
                     LIMIT 1",
                )
                .map_err(db)?
                .query_row(params![run, job_id.0], |row| row.get::<_, Option<i64>>(0))
                .optional()
                .map_err(db)?
                .flatten();
            if let Some(id) = row_id {
                return Ok(Some(id as u64));
            }
            tx.prepare_cached(
                "SELECT json_extract(record_details, \
                     '$.\"job_check_run_ids\".\"' || replace(?2, '\"', '\\\"') || '\"') \
                 FROM run_submissions WHERE run_id = ?1 \
                 UNION ALL \
                 SELECT json_extract(record_details, \
                     '$.\"job_check_run_ids\".\"' || replace(?2, '\"', '\\\"') || '\"') \
                 FROM run_history WHERE run_id = ?1 AND run_attempt = ( \
                     SELECT MAX(run_attempt) FROM run_history WHERE run_id = ?1)",
            )
            .map_err(db)?
            .query_row(params![run, job_id.0], |row| row.get::<_, Option<i64>>(0))
            .optional()
            .map(|id| id.flatten().map(|id| id as u64))
            .map_err(db)
        })
    }

    /// Status per logical job, sorted by job id (history for an archived
    /// run). `None` when the run is unknown.
    pub(crate) async fn run_job_statuses(
        &self,
        run_id: RunId,
    ) -> Result<Option<Vec<(JobId, ExecutionStatus)>>, ControlError> {
        let run = codec::run_key(run_id);
        self.read(|tx| {
            if !run_known(tx, &run)? {
                return Ok(None);
            }
            let mut stmt = tx
                .prepare_cached(
                    "SELECT job_id, status FROM jobs WHERE run_id = ?1 \
                     UNION ALL \
                     SELECT job_id, status FROM job_history h WHERE h.run_id = ?1 \
                       AND NOT EXISTS (SELECT 1 FROM jobs j \
                           WHERE j.run_id = h.run_id AND j.job_id = h.job_id) \
                       AND h.run_attempt = (SELECT MAX(run_attempt) FROM job_history \
                           WHERE run_id = h.run_id AND job_id = h.job_id) \
                     ORDER BY 1",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map([&run], |row| {
                    Ok((JobId(row.get(0)?), status_parse(&row.get::<_, String>(1)?)))
                })
                .map_err(db)?;
            rows.collect::<Result<Vec<_>, _>>().map(Some).map_err(db)
        })
    }

    /// Check-run reporting inputs: repository + sha from the submission,
    /// run clocks, and per job (by job id) status, display name, check-run
    /// id, `placeholder`, annotations and the latest attempt's steps.
    /// Archived runs read `run_history`/`job_history` (steps from
    /// `step_history`).
    pub(crate) async fn run_dispatch_info(
        &self,
        run_id: RunId,
    ) -> Result<Option<RunDispatchInfo>, ControlError> {
        let run = codec::run_key(run_id);
        self.read(|tx| {
            let live = tx
                .prepare_cached(
                    "SELECT s.submission, r.started_at, r.completed_at, s.record_details \
                     FROM runs r JOIN run_submissions s ON s.run_id = r.run_id \
                     WHERE r.run_id = ?1",
                )
                .map_err(db)?
                .query_row([&run], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .optional()
                .map_err(db)?;
            type RunHead = (String, Option<i64>, Option<i64>, Option<String>);
            let (head, archived): (RunHead, bool) = match live {
                Some(head) => (head, false),
                None => match tx
                    .prepare_cached(
                        "SELECT submission, started_at, completed_at, record_details \
                         FROM run_history WHERE run_id = ?1 ORDER BY run_attempt DESC LIMIT 1",
                    )
                    .map_err(db)?
                    .query_row([&run], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                    })
                    .optional()
                    .map_err(db)?
                {
                    Some(head) => (head, true),
                    None => return Ok(None),
                },
            };
            let (submission, started_at, completed_at, details_json) = head;
            let value = submission_value(&submission)?;
            // `job_check_run_ids` keys minted before the leg materialized are
            // dispatch-relevant rows too — a terminal report mints for them.
            let stored_ids: BTreeMap<String, i64> = details_json
                .as_deref()
                .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
                .and_then(|json| json.get("job_check_run_ids").cloned())
                .and_then(|map| serde_json::from_value(map).ok())
                .unwrap_or_default();
            let job_rows: Vec<JobQueryRow> = {
                // `placeholder` marks the expandable nodes (`kind`) that never
                // dispatch: intake loops skip them so no `queued` check run is
                // minted for a node expansion replaces.
                let sql = if archived {
                    "SELECT job_id, status, display_name, check_run_id, annotations, \
                         (kind IN ('matrix_parent','reusable_caller')) \
                     FROM job_history h WHERE h.run_id = ?1 \
                       AND h.run_attempt = (SELECT MAX(run_attempt) FROM job_history \
                           WHERE run_id = h.run_id AND job_id = h.job_id) \
                     ORDER BY job_id"
                } else {
                    "SELECT j.job_id, j.status, s.display_name, j.check_run_id, j.annotations, \
                         (j.kind IN ('matrix_parent','reusable_caller')) \
                     FROM jobs j LEFT JOIN job_specs s \
                       ON s.run_id = j.run_id AND s.job_id = j.job_id \
                     WHERE j.run_id = ?1 ORDER BY j.job_id"
                };
                let mut stmt = tx.prepare_cached(sql).map_err(db)?;
                let rows = stmt
                    .query_map([&run], |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                        ))
                    })
                    .map_err(db)?;
                let mut rows: Vec<JobQueryRow> = rows.collect::<Result<_, _>>().map_err(db)?;
                // Legs minted before materialization have no jobs row; append
                // them from `record_details` so reports can mint for them.
                for (job_id, id) in &stored_ids {
                    if !rows.iter().any(|row| row.0 == *job_id) {
                        rows.push((
                            job_id.clone(),
                            "pending".to_owned(),
                            None,
                            Some(*id),
                            None,
                            false,
                        ));
                    }
                }
                rows
            };
            let mut jobs = Vec::with_capacity(job_rows.len());
            for (job, status, display_name, check_run_id, annotations, placeholder) in job_rows {
                let steps = if archived {
                    super::steps::archived_attempt_steps(tx, &run, &job)?
                } else {
                    super::steps::latest_attempt_steps(tx, &run, &job)?
                };
                let status = status_parse(&status);
                let detail = annotations
                    .and_then(|json| serde_json::from_str::<Vec<serde_json::Value>>(&json).ok())
                    .filter(|annotations| !annotations.is_empty())
                    .map(|annotations| JobDetail {
                        job_id: job.clone(),
                        name: display_name.clone().unwrap_or_else(|| job.clone()),
                        conclusion: status_str(status).to_owned(),
                        steps: Vec::new(),
                        annotations,
                    });
                jobs.push(RunDispatchJob {
                    job_id: JobId(job),
                    status,
                    display_name,
                    check_run_id: check_run_id.map(|id| id as u64),
                    placeholder,
                    detail,
                    steps,
                });
            }
            Ok(Some(RunDispatchInfo {
                repository: value["repository"].as_str().unwrap_or_default().to_owned(),
                sha: value["sha"].as_str().unwrap_or_default().to_owned(),
                started_at: started_at.map(codec::us_to_system),
                completed_at: completed_at.map(codec::us_to_system),
                jobs,
            }))
        })
    }

    /// `(repository, sha, base_ref, git_ref)` from the live submission.
    pub(crate) async fn submission_fields(
        &self,
        run_id: RunId,
    ) -> Result<Option<SubmissionFields>, ControlError> {
        let run = codec::run_key(run_id);
        self.read(|tx| {
            let Some(json) = tx
                .prepare_cached("SELECT submission FROM run_submissions WHERE run_id = ?1")
                .map_err(db)?
                .query_row([&run], |row| row.get::<_, String>(0))
                .optional()
                .map_err(db)?
            else {
                return Ok(None);
            };
            let value = submission_value(&json)?;
            Ok(Some(SubmissionFields {
                repository: value["repository"].as_str().unwrap_or_default().to_owned(),
                sha: value["sha"].as_str().unwrap_or_default().to_owned(),
                base_ref: value["base_ref"].as_str().map(str::to_owned),
                git_ref: value["git_ref"].as_str().unwrap_or_default().to_owned(),
            }))
        })
    }

    /// Resolve a check-run rerequest on a completed run of `repository`
    /// (at `head_sha` when given), preferring `details_run_id`: first an
    /// exact `check_run_id` hit, else `job_name` against job ids and display
    /// names. Live runs, then archived ones.
    pub(crate) async fn check_run_target(
        &self,
        check_run_id: u64,
        repository: &str,
        head_sha: Option<&str>,
        job_name: Option<&str>,
        details_run_id: Option<RunId>,
    ) -> Result<Option<(RunId, JobId)>, ControlError> {
        let details = details_run_id.map(codec::run_key);
        self.read(|tx| {
            // Live and archived candidates share one shape so the
            // `details_run_id` preference and the id order apply across both.
            const CANDIDATES: &str = "SELECT j.run_id, j.job_id, j.check_run_id, \
                     s.display_name AS display_name \
                 FROM jobs j JOIN runs r ON r.run_id = j.run_id \
                 LEFT JOIN job_specs s ON s.run_id = j.run_id AND s.job_id = j.job_id \
                 WHERE r.status = 'completed' AND r.repository = ?2 \
                   AND (?3 IS NULL OR r.head_sha = ?3) \
                 UNION ALL \
                 SELECT j.run_id, j.job_id, j.check_run_id, j.display_name \
                 FROM job_history j JOIN run_history r \
                   ON r.run_id = j.run_id AND r.created_at = j.run_created_at \
                      AND r.run_attempt = j.run_attempt \
                 WHERE r.repository = ?2 AND (?3 IS NULL OR r.head_sha = ?3) \
                   AND r.run_attempt = (SELECT MAX(run_attempt) FROM run_history \
                                        WHERE run_id = r.run_id) \
                   AND NOT EXISTS (SELECT 1 FROM runs live WHERE live.run_id = r.run_id) \
                 UNION ALL \
                 SELECT s.run_id, je.key, CAST(je.value AS INTEGER), je.key \
                 FROM run_submissions s JOIN runs r ON r.run_id = s.run_id, \
                     json_each(s.record_details, '$.job_check_run_ids') je \
                 WHERE r.status = 'completed' AND r.repository = ?2 \
                   AND (?3 IS NULL OR r.head_sha = ?3) \
                 UNION ALL \
                 SELECT h.run_id, je.key, CAST(je.value AS INTEGER), je.key \
                 FROM run_history h, json_each(h.record_details, '$.job_check_run_ids') je \
                 WHERE h.repository = ?2 AND (?3 IS NULL OR h.head_sha = ?3) \
                   AND h.run_attempt = (SELECT MAX(run_attempt) FROM run_history \
                                       WHERE run_id = h.run_id) \
                   AND NOT EXISTS (SELECT 1 FROM runs live WHERE live.run_id = h.run_id)";
            let exact = tx
                .prepare_cached(&format!(
                    "SELECT run_id, job_id FROM ({CANDIDATES}) WHERE check_run_id = ?1 \
                     ORDER BY (run_id = ?4) DESC, run_id LIMIT 1"
                ))
                .map_err(db)?
                .query_row(
                    params![check_run_id as i64, repository, head_sha, details],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(db)?;
            let hit = match (exact, job_name) {
                (Some(hit), _) => Some(hit),
                (None, None) => None,
                (None, Some(name)) => tx
                    .prepare_cached(&format!(
                        "SELECT run_id, job_id FROM ({CANDIDATES}) \
                         WHERE (job_id = ?5 OR display_name = ?5) \
                         ORDER BY (run_id = ?4) DESC, run_id LIMIT 1"
                    ))
                    .map_err(db)?
                    .query_row(
                        params![check_run_id as i64, repository, head_sha, details, name],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                    )
                    .optional()
                    .map_err(db)?,
            };
            Ok(hit.map(|(run, job)| (codec::run_id(&run), JobId(job))))
        })
    }

    /// The concluded run whose push already published `sha` for
    /// `workflow_path` on `repository` (matched on the submitted sha or the
    /// push's `effective_sha`). Archive keeps such runs live for the echo
    /// window, so only live runs are searched.
    pub(crate) async fn published_run(
        &self,
        repository: &str,
        sha: &str,
        workflow_path: &str,
    ) -> Result<Option<RunId>, ControlError> {
        self.read(|tx| {
            tx.prepare_cached(
                "SELECT r.run_id FROM runs r \
                 JOIN run_push_states p ON p.run_id = r.run_id \
                 JOIN run_submissions s ON s.run_id = r.run_id \
                 WHERE r.conclusion IS NOT NULL AND r.repository = ?1 \
                   AND s.submission ->> '$.workflow_path' = ?3 \
                   AND (s.submission ->> '$.sha' = ?2 OR p.effective_sha = ?2) \
                 ORDER BY r.run_id LIMIT 1",
            )
            .map_err(db)?
            .query_row(params![repository, sha, workflow_path], |row| {
                row.get::<_, String>(0)
            })
            .optional()
            .map(|run| run.map(|run| codec::run_id(&run)))
            .map_err(db)
        })
    }

    /// `run_for_webhook_delivery`: indexed `(delivery_id, workflow_path)`
    /// point read over live runs; `None` when no committed run exists yet.
    pub(crate) async fn run_for_webhook_delivery(
        &self,
        delivery_id: &str,
        workflow_path: &str,
    ) -> Result<Option<RunId>, ControlError> {
        self.read(|tx| {
            tx.prepare_cached(
                "SELECT run_id FROM runs WHERE webhook_delivery_id = ?1 \
                 AND workflow_path = ?2",
            )
            .map_err(db)?
            .query_row(params![delivery_id, workflow_path], |row| row.get(0))
            .optional()
            .map(|run| run.map(|run: String| codec::run_id(&run)))
            .map_err(db)
        })
    }

    /// Change only the run's push state (`run_push_states` upsert). An
    /// unknown or archived run writes nothing.
    pub(crate) async fn set_push_state(
        &self,
        run_id: RunId,
        state: PushState,
    ) -> Result<(), ControlError> {
        let run = codec::run_key(run_id);
        self.write(|tx| {
            tx.prepare_cached(
                "INSERT INTO run_push_states (run_id, status, error, pr_number, effective_sha, \
                     updated_at) \
                 SELECT ?1, ?2, ?3, ?4, ?5, ?6 WHERE EXISTS (SELECT 1 FROM runs WHERE run_id = ?1) \
                 ON CONFLICT (run_id) DO UPDATE SET status = excluded.status, \
                     error = excluded.error, pr_number = excluded.pr_number, \
                     effective_sha = excluded.effective_sha, updated_at = excluded.updated_at",
            )
            .map_err(db)?
            .execute(params![
                run,
                push_status_str(state.status),
                state.error,
                state.pr_number.map(|n| n as i64),
                state.effective_sha,
                now_us()
            ])
            .map_err(db)?;
            Ok(())
        })
    }

    /// Whether the run waits on a workflow-level concurrency slot.
    pub(crate) async fn run_held(&self, run_id: RunId) -> Result<bool, ControlError> {
        let run = codec::run_key(run_id);
        self.read(|tx| {
            tx.prepare_cached(
                "SELECT EXISTS (SELECT 1 FROM jobs WHERE run_id = ?1 AND queue_state = 'held')",
            )
            .map_err(db)?
            .query_row([&run], |row| row.get(0))
            .map_err(db)
        })
    }

    /// How the run takes part in concurrency groups: `Gated` when any
    /// job-level/jobset gate or non-run hold/wait exists, `WorkflowOnly`
    /// when only the workflow-level group applies, else `None`.
    pub(crate) async fn run_in_concurrency(
        &self,
        run_id: RunId,
    ) -> Result<RunConcurrency, ControlError> {
        let run = codec::run_key(run_id);
        self.read(|tx| {
            tx.prepare_cached(
                "SELECT \
                   EXISTS (SELECT 1 FROM jobsets js JOIN jobset_gates g \
                             ON g.jobset_id = js.jobset_id WHERE js.run_id = ?1) \
                   OR EXISTS (SELECT 1 FROM job_specs WHERE run_id = ?1 \
                              AND concurrency IS NOT NULL) \
                   OR EXISTS (SELECT 1 FROM concurrency_holds \
                              WHERE holder_run_id = ?1 AND holder_kind <> 'run') \
                   OR EXISTS (SELECT 1 FROM concurrency_waits \
                              WHERE holder_run_id = ?1 AND holder_kind <> 'run'), \
                 EXISTS (SELECT 1 FROM runs WHERE run_id = ?1 \
                         AND concurrency_group IS NOT NULL) \
                   OR EXISTS (SELECT 1 FROM concurrency_holds WHERE holder_run_id = ?1) \
                   OR EXISTS (SELECT 1 FROM concurrency_waits WHERE holder_run_id = ?1)",
            )
            .map_err(db)?
            .query_row([&run], |row| {
                Ok(RunConcurrency::classify(row.get(0)?, row.get(1)?))
            })
            .map_err(db)
        })
    }

    /// Queue pressure. Buckets from the queue-state combinations:
    /// `pending` = blocked without a concurrency wait (needs or
    /// max-parallel), `blocked` = blocked with a concurrency wait,
    /// `expanding` = expansion nodes not yet claimed.
    pub(crate) async fn queue_stats(&self) -> Result<QueueStats, ControlError> {
        self.read(|tx| {
            let mut stats = QueueStats::default();
            let mut stmt = tx
                .prepare_cached(&format!(
                    // The alias must not collide with a column: `jobs.kind`
                    // (job/matrix_leg/...) would capture `GROUP BY kind` and
                    // collapse every bucket into one arbitrary row.
                    "SELECT {QUEUE_KIND} AS bucket, COUNT(*) FROM jobs j \
                     WHERE j.queue_state NOT IN ('none', 'expanding') GROUP BY bucket"
                ))
                .map_err(db)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(db)?;
            for row in rows {
                let (kind, count) = row.map_err(db)?;
                let count = count.max(0) as usize;
                match QueueKind::parse(&kind) {
                    Some(QueueKind::Ready) => stats.ready = count,
                    Some(QueueKind::Pending) => stats.pending = count,
                    Some(QueueKind::Blocked) => stats.blocked = count,
                    Some(QueueKind::Held) => stats.held = count,
                    Some(QueueKind::Claimed) => stats.claimed = count,
                    Some(QueueKind::Expand) => stats.expanding = count,
                    _ => {}
                }
            }
            stats.next_runs_on = tx
                .prepare_cached(
                    "SELECT runs_on FROM jobs WHERE queue_state = 'ready' \
                     ORDER BY priority DESC, run_order, job_order, run_id, job_id LIMIT 1",
                )
                .map_err(db)?
                .query_row([], |row| row.get::<_, String>(0))
                .optional()
                .map_err(db)?
                .and_then(|runs_on| serde_json::from_str(&runs_on).ok())
                .unwrap_or_default();
            Ok(stats)
        })
    }

    /// The job's `(queue kind, status)` in the `QueueKind` vocabulary;
    /// `None` when the job has no live row.
    pub(crate) async fn job_queue_state(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<(String, String)>, ControlError> {
        let run = codec::run_key(run_id);
        self.read(|tx| {
            tx.prepare_cached(&format!(
                "SELECT {QUEUE_KIND}, j.status FROM jobs j WHERE j.run_id = ?1 AND j.job_id = ?2"
            ))
            .map_err(db)?
            .query_row(params![run, job_id.0], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()
            .map_err(db)
        })
    }

    /// Terminal logical jobs, live and archived.
    pub(crate) async fn terminal_jobs(&self) -> Result<BTreeSet<(RunId, JobId)>, ControlError> {
        self.read(|tx| {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT run_id, job_id FROM jobs \
                     WHERE status IN ('success','failure','skipped','cancelled','timed_out') \
                     UNION \
                     SELECT run_id, job_id FROM job_history h \
                     WHERE h.status IN ('success','failure','skipped','cancelled','timed_out') \
                       AND NOT EXISTS (SELECT 1 FROM jobs j \
                           WHERE j.run_id = h.run_id AND j.job_id = h.job_id)",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((codec::run_id(&row.get::<_, String>(0)?), JobId(row.get(1)?)))
                })
                .map_err(db)?;
            rows.collect::<Result<_, _>>().map_err(db)
        })
    }

    /// Move up to `min(limit, 64)` settled runs into the history tables in
    /// one transaction, then delete their `runs` rows (everything live
    /// cascades). A candidate completed at least a minute ago, has no
    /// in-flight attempt, no session-bound attempt, no undelivered
    /// cancellation, and no push state that echo dedup may still need.
    ///
    /// `rerun_hold` keeps a candidate whose completed attempt has at least
    /// one failed/cancelled/timed-out job live for that long, so a re-run can
    /// reset it in place; `None` or zero archives on the plain policy.
    /// Retention still wins: `expired_terminal_runs` selects live rows too.
    pub(crate) async fn archive_finished_runs(
        &self,
        limit: usize,
        rerun_hold: Option<std::time::Duration>,
    ) -> Result<Vec<RunId>, ControlError> {
        let hold_us: Option<i64> = rerun_hold
            .map(|hold| i64::try_from(hold.as_micros()).unwrap_or(i64::MAX))
            .filter(|hold| *hold > 0);
        let rerun_cutoff = hold_us.map(|hold| now_us() - hold);
        self.write(move |tx| {
            let now = now_us();
            let run_ids: Vec<String> = {
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT r.run_id FROM runs r \
                         WHERE r.status = 'completed' AND r.completed_at <= ?1 \
                           AND NOT EXISTS (SELECT 1 FROM job_requests q \
                                           WHERE q.run_id = r.run_id \
                                             AND (q.result IS NULL OR q.session_id IS NOT NULL)) \
                           AND NOT EXISTS (SELECT 1 FROM job_cancellations c \
                                           JOIN job_requests q ON q.request_id = c.request_id \
                                           WHERE q.run_id = r.run_id AND c.delivered_at IS NULL) \
                           AND NOT EXISTS (SELECT 1 FROM run_push_states p \
                                           WHERE p.run_id = r.run_id \
                                             AND (p.status = 'pending' OR p.updated_at > ?2)) \
                           AND (?4 IS NULL OR r.completed_at <= ?4 \
                                OR NOT EXISTS (SELECT 1 FROM jobs j \
                                               WHERE j.run_id = r.run_id \
                                                 AND j.status IN ('failure','cancelled','timed_out'))) \
                         ORDER BY r.completed_at, r.run_id LIMIT ?3",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map(
                        params![
                            now - ARCHIVE_GRACE_US,
                            now - PUSH_ECHO_WINDOW_US,
                            limit.min(ARCHIVE_BATCH) as i64,
                            rerun_cutoff,
                        ],
                        |row| row.get(0),
                    )
                    .map_err(db)?;
                rows.collect::<Result<_, _>>().map_err(db)?
            };
            for run in &run_ids {
                for sql in [
                    "INSERT INTO run_history (run_id, namespace_id, repository, workflow_path, \
                         run_number, run_attempt, run_name, event, ref, ref_type, head_ref, \
                         base_ref, head_sha, conclusion, submission, record_details, \
                         fork_approval_pending, fork_approval_requested_at, \
                         fork_approval_approved_at, fork_approval_note, \
                         reports_check_runs, \
                         created_at, started_at, completed_at) \
                     SELECT r.run_id, r.namespace_id, r.repository, r.workflow_path, \
                         r.run_number, r.run_attempt, r.run_name, r.event, r.ref, r.ref_type, \
                         r.head_ref, r.base_ref, r.head_sha, r.conclusion, \
                         COALESCE(s.submission, '{}'), \
                         COALESCE(s.record_details, '{}'), \
                         r.fork_approval_pending, r.fork_approval_requested_at, \
                         r.fork_approval_approved_at, r.fork_approval_note, \
                         r.reports_check_runs, \
                         r.created_at, r.started_at, \
                         r.completed_at \
                     FROM runs r LEFT JOIN run_submissions s ON s.run_id = r.run_id \
                     WHERE r.run_id = ?1",
                    "INSERT INTO job_history (run_id, run_created_at, run_attempt, job_id, namespace_id, \
                         kind, parent_job_id, base_id, display_name, status, pool_key, outputs, \
                         annotations, check_run_id, created_at, deps_ready_at, started_at, \
                         completed_at, request_id) \
                     SELECT j.run_id, r.created_at, r.run_attempt, j.job_id, j.namespace_id, j.kind, \
                         j.parent_job_id, j.base_id, COALESCE(s.display_name, j.job_id), \
                         j.status, j.pool_key, j.outputs, j.annotations, j.check_run_id, \
                         j.created_at, j.deps_ready_at, j.started_at, j.completed_at, j.request_id \
                     FROM jobs j JOIN runs r ON r.run_id = j.run_id \
                     LEFT JOIN job_specs s ON s.run_id = j.run_id AND s.job_id = j.job_id \
                     WHERE j.run_id = ?1",
                    "INSERT INTO attempt_history (request_id, run_id, run_created_at, job_id, \
                         namespace_id, agent_job_id, timeline_id, runner_id, result, claimed_at, \
                         started_at, finished_at) \
                     SELECT q.request_id, q.run_id, r.created_at, q.job_id, q.namespace_id, \
                         q.agent_job_id, q.timeline_id, q.runner_id, q.result, q.claimed_at, \
                         q.started_at, q.finished_at \
                     FROM job_requests q JOIN runs r ON r.run_id = q.run_id \
                     WHERE q.run_id = ?1",
                    "INSERT INTO step_history (agent_job_id, step_id, run_id, run_created_at, \
                         namespace_id, position, kind, workflow_index, runner_number, \
                         context_name, name, conclusion, started_at, finished_at) \
                     SELECT s.agent_job_id, s.step_id, q.run_id, r.created_at, q.namespace_id, \
                         s.position, s.kind, s.workflow_index, s.runner_number, s.context_name, \
                         s.name, s.conclusion, s.started_at, s.finished_at \
                     FROM job_steps s JOIN job_requests q ON q.agent_job_id = s.agent_job_id \
                     JOIN runs r ON r.run_id = q.run_id \
                     WHERE q.run_id = ?1",
                    "DELETE FROM runs WHERE run_id = ?1",
                ] {
                    tx.prepare_cached(sql)
                        .map_err(db)?
                        .execute([run])
                        .map_err(db)?;
                }
            }
            Ok(run_ids.iter().map(|run| codec::run_id(run)).collect())
        })
    }

    /// `expired_terminal_runs`: terminal runs finished (else created) before
    /// `cutoff_us`, oldest first, `limit` at most.
    ///
    /// Archived runs are selected from `run_history`: the archiver moves a
    /// settled run there within a minute of completion, so that table is
    /// where an expired run actually lives — a retention pass that only
    /// looked at `runs` would delete nothing. A live run with an unfinished
    /// or session-bound attempt is skipped (the archiver's own guard): its
    /// callbacks are still in play and deleting it would strand them.
    pub(crate) async fn expired_terminal_runs(
        &self,
        cutoff_us: i64,
        limit: usize,
    ) -> Result<Vec<RunId>, ControlError> {
        self.read(move |tx| {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT run_id FROM ( \
                         SELECT r.run_id AS run_id, \
                                COALESCE(r.completed_at, r.created_at) AS finished_at \
                         FROM runs r \
                         WHERE r.status = 'completed' \
                           AND NOT EXISTS (SELECT 1 FROM job_requests q \
                                           WHERE q.run_id = r.run_id \
                                             AND (q.result IS NULL OR q.session_id IS NOT NULL)) \
                         UNION ALL \
                         SELECT h.run_id, COALESCE(h.completed_at, h.created_at) \
                         FROM run_history h) \
                     GROUP BY run_id \
                     HAVING MAX(finished_at) < ?1 \
                     ORDER BY MAX(finished_at), run_id \
                     LIMIT ?2",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map(params![cutoff_us, limit as i64], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(db)?;
            let runs = rows.collect::<Result<Vec<_>, _>>().map_err(db)?;
            Ok(runs.iter().map(|run| codec::run_id(run)).collect())
        })
    }

    /// `delete_expired_run`: drop one terminal run for good — its live rows
    /// (the `runs` row cascades its jobs, requests, attempts, steps, logs
    /// and gates) and its history rows, plus the artifact metadata keyed by
    /// the run.
    ///
    /// A live row that is not `completed` is `Conflict` and nothing is
    /// written: retention must never delete a run that can still schedule,
    /// whatever the caller's filter said. An unknown run is `Ok(())`, so an
    /// interrupted pass can simply be repeated. On-disk artifacts and the
    /// node-local key set are the caller's to remove (the durable rows here
    /// answer neither once deleted).
    pub(crate) async fn delete_expired_run(&self, run_id: RunId) -> Result<(), ControlError> {
        self.write(move |tx| {
            let run = codec::run_key(run_id);
            let status: Option<String> = tx
                .prepare_cached("SELECT status FROM runs WHERE run_id = ?1")
                .map_err(db)?
                .query_row([&run], |row| row.get(0))
                .optional()
                .map_err(db)?;
            if let Some(status) = status.as_deref()
                && status != "completed"
            {
                return Err(ControlError::Conflict(format!(
                    "run {run_id} is {status}, not terminal"
                )));
            }
            // Timelines are keyed by id and shared across attempts of
            // different runs (a rerun correlates on the same id), and they
            // carry no FK to `runs`, so the weekly `prune_timelines` is the
            // only other owner — and it can only match while a request row
            // still names them. Drop the ones this run is the last referent
            // of, records first.
            for table in ["timeline_records", "timelines"] {
                tx.prepare_cached(&format!(
                    "DELETE FROM {table} WHERE timeline_id IN ( \
                         SELECT q.timeline_id FROM job_requests q \
                         WHERE q.run_id = ?1 \
                           AND NOT EXISTS (SELECT 1 FROM job_requests o \
                                           WHERE o.timeline_id = q.timeline_id \
                                             AND o.run_id <> ?1))"
                ))
                .map_err(db)?
                .execute([&run])
                .map_err(db)?;
            }
            // The archive tables have no FK to `runs`: without this the run
            // would survive in `job_history`/`attempt_history` (and keep
            // surfacing through `terminal_jobs`) after its record is gone.
            for table in [
                "step_history",
                "attempt_history",
                "job_history",
                "run_history",
            ] {
                tx.prepare_cached(&format!("DELETE FROM {table} WHERE run_id = ?1"))
                    .map_err(db)?
                    .execute([&run])
                    .map_err(db)?;
            }
            tx.prepare_cached("DELETE FROM artifacts WHERE run_id = ?1")
                .map_err(db)?
                .execute([&run])
                .map_err(db)?;
            tx.prepare_cached("DELETE FROM runs WHERE run_id = ?1")
                .map_err(db)?
                .execute([&run])
                .map_err(db)?;
            Ok(())
        })
    }
}

/// `blocked` is dependencies/max-parallel (`pending` in the old vocabulary);
/// `held` is any concurrency gate, with `concurrency_waits.holder_kind`
/// distinguishing workflow (`run`) from job/jobset.
const QUEUE_KIND: &str = "CASE j.queue_state \
     WHEN 'blocked' THEN 'pending' \
     WHEN 'held' THEN 'blocked' \
     WHEN 'pending_expansion' THEN 'expand' \
     WHEN 'expanding' THEN 'expand' \
     ELSE j.queue_state END";

/// An `attempt_history` row decoded into the live-request shape
/// (`RECORD_COLUMNS` parity: no lease, no timeout/debug flags).
fn attempt_history_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<crate::models::TaskAgentJobRequestRecord> {
    let agent_job_id = codec::uuid(&row.get::<_, String>(3)?);
    Ok(crate::models::TaskAgentJobRequestRecord {
        request_id: row.get(0)?,
        run_id: codec::run_id(&row.get::<_, String>(1)?),
        job_id: JobId(row.get(2)?),
        agent_job_id,
        plan_id: codec::plan_id(&agent_job_id),
        plan_type: codec::PLAN_TYPE.to_owned(),
        timeline_id: codec::uuid(&row.get::<_, String>(4)?),
        result: row
            .get::<_, Option<String>>(5)?
            .as_deref()
            .map(status_parse),
        locked_until: String::new(),
        claimed_at: row.get::<_, Option<i64>>(6)?.map(codec::us_to_system),
        owner_runner_id: row.get(7)?,
        started_at: row.get::<_, Option<i64>>(8)?.map(codec::us_to_system),
        last_renewed_at: None,
        timeout_triggered: false,
        debug_token_issued: false,
    })
}

/// The `RECORD_COLUMNS` projection over `attempt_history` (same column
/// order; lease/flags are absent in history).
/// Must stay column-for-column with [`super::requests::RECORD_COLUMNS`]:
/// archived attempts carry no lease (`expires_at`, `renewed_at` → NULL) and
/// their timeout / debug-token flags are no longer meaningful (→ 0).
const ATTEMPT_HISTORY_COLUMNS: &str = "q.request_id, q.run_id, q.job_id, q.agent_job_id, \
     q.timeline_id, q.result, NULL, q.claimed_at, q.runner_id, q.started_at, \
     NULL, 0, 0";

const ATTEMPT_HISTORY_FROM: &str = "attempt_history q";

impl LiteBackend {
    /// `run_record`: the projected run record, live rows first and
    /// `run_history`/`job_history`/`attempt_history` when archived.
    pub(crate) async fn run_record(
        &self,
        run_id: RunId,
    ) -> Result<Option<crate::models::RunRecord>, ControlError> {
        self.read(move |tx| super::jobs::run_record(tx, run_id))
    }

    /// `run_requests`: live `job_requests` UNION ALL archived
    /// `attempt_history`, request_id order (pg lookups.rs parity).
    pub(crate) async fn run_requests(
        &self,
        run_id: RunId,
    ) -> Result<Vec<crate::models::TaskAgentJobRequestRecord>, ControlError> {
        let run = codec::run_key(run_id);
        self.read(move |tx| {
            let live = format!(
                "SELECT {} FROM {} WHERE q.run_id = ?1",
                super::requests::RECORD_COLUMNS,
                super::requests::RECORD_FROM
            );
            let mut stmt = tx
                .prepare_cached(&format!(
                    "{live} UNION ALL \
                     SELECT {ATTEMPT_HISTORY_COLUMNS} FROM {ATTEMPT_HISTORY_FROM} \
                     WHERE q.run_id = ?1 ORDER BY 1"
                ))
                .map_err(db)?;
            let rows = stmt
                .query_map([&run], |row| {
                    // The UNION must decode through the live decoder; the
                    // history arm yields NULL lease columns, which
                    // `record_row` reads as absent.
                    super::requests::record_row(row)
                })
                .map_err(db)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(db)
        })
    }

    /// `run_record` for one attempt. The newest attempt reads exactly as
    /// `run_record`; an earlier one projects its `run_history` head and
    /// `job_history` snapshot (see `run_record_at`).
    pub(crate) async fn run_record_attempt(
        &self,
        run_id: RunId,
        attempt: u64,
    ) -> Result<crate::models::RunRecord, ControlError> {
        self.read(move |tx| {
            let resolved = resolve_attempt(tx, run_id, attempt)?;
            let record = if resolved.newest {
                super::jobs::run_record(tx, run_id)?
            } else {
                super::jobs::run_record_at(tx, run_id, Some(resolved.number))?
            };
            record.ok_or_else(|| {
                ControlError::NotFound(format!("run {run_id} has no attempt {attempt}"))
            })
        })
    }

    /// `run_attempt_requests`: the requests attempt `attempt` executed, in
    /// request order (see [`attempt_window`]).
    pub(crate) async fn run_attempt_requests(
        &self,
        run_id: RunId,
        attempt: u64,
    ) -> Result<Vec<crate::models::TaskAgentJobRequestRecord>, ControlError> {
        self.read(move |tx| {
            let resolved = resolve_attempt(tx, run_id, attempt)?;
            let window = attempt_window();
            let live = format!(
                "SELECT {} FROM {} WHERE q.run_id = ?1 AND {window}",
                super::requests::RECORD_COLUMNS,
                super::requests::RECORD_FROM
            );
            let mut stmt = tx
                .prepare_cached(&format!(
                    "{live} UNION ALL \
                     SELECT {ATTEMPT_HISTORY_COLUMNS} FROM {ATTEMPT_HISTORY_FROM} \
                     WHERE q.run_id = ?1 AND {window} ORDER BY 1"
                ))
                .map_err(db)?;
            let rows = stmt
                .query_map(
                    params![codec::run_key(run_id), resolved.number, resolved.live],
                    super::requests::record_row,
                )
                .map_err(db)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(db)
        })
    }

    /// `list_runs`: id order by `(terminal, last-activity desc)`, filtered
    /// on workflow/status/event, then each id's `run_record` projection
    /// (history fallback covers archived runs).
    ///
    /// `status` filters on the projected API word like `run_record`, never
    /// the raw `runs.status` storage word: a terminal run reads its
    /// conclusion (`completed` matches any terminal run, the wire's
    /// legacy alias) and a run waiting on its workflow-level gate reads
    /// `pending`, not `queued`.
    pub(crate) async fn list_runs(
        &self,
        filter: crate::control::backend::RunListFilter,
    ) -> Result<Vec<crate::models::RunRecord>, ControlError> {
        self.read(move |tx| {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT run_id FROM ( \
                         SELECT r.run_id, \
                                CASE WHEN r.status = 'completed' THEN 1 ELSE 0 END AS terminal_rank, \
                                COALESCE(r.completed_at, r.started_at, r.created_at) AS sort_at, \
                                r.workflow_path, r.event, \
                                CASE WHEN r.status = 'completed' THEN COALESCE(r.conclusion, 'success') \
                                     WHEN EXISTS (SELECT 1 FROM concurrency_waits w \
                                                  WHERE w.holder_run_id = r.run_id \
                                                    AND w.holder_kind = 'run') THEN 'pending' \
                                     ELSE r.status END AS status \
                         FROM runs r \
                         UNION ALL \
                         SELECT h.run_id, 1, \
                                COALESCE(h.completed_at, h.started_at, h.created_at), \
                                h.workflow_path, h.event, COALESCE(h.conclusion, 'success') \
                         FROM run_history h \
                         WHERE h.run_attempt = (SELECT MAX(run_attempt) FROM run_history \
                                                WHERE run_id = h.run_id) \
                           AND NOT EXISTS (SELECT 1 FROM runs live \
                                           WHERE live.run_id = h.run_id)) \
                     WHERE (?1 IS NULL OR instr(workflow_path, ?1) > 0) \
                       AND (?2 IS NULL OR status = ?2 OR (?2 = 'completed' AND terminal_rank = 1)) \
                       AND (?3 IS NULL OR event = ?3) \
                     ORDER BY terminal_rank, sort_at DESC, run_id LIMIT ?4",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map(
                    params![
                        filter.workflow.as_deref(),
                        filter.status.as_deref(),
                        filter.event.as_deref(),
                        filter.limit.min(200) as i64,
                    ],
                    |row| row.get::<_, String>(0),
                )
                .map_err(db)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(db)?;
            let mut runs = Vec::with_capacity(rows.len());
            for run in rows {
                if let Some(record) = super::jobs::run_record(tx, codec::run_id(&run))? {
                    runs.push(record);
                }
            }
            Ok(runs)
        })
    }

    /// `runs_for_repository`: every run (live or archived) of one
    /// repository, repository comparison case-insensitive like the old
    /// `eq_ignore_ascii_case` filter.
    pub(crate) async fn runs_for_repository(
        &self,
        repository: &str,
    ) -> Result<Vec<crate::models::RunRecord>, ControlError> {
        let repository = repository.to_owned();
        self.read(move |tx| {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT run_id FROM runs WHERE repository = ?1 COLLATE NOCASE \
                     UNION ALL \
                     SELECT h.run_id FROM run_history h \
                     WHERE h.repository = ?1 COLLATE NOCASE \
                       AND h.run_attempt = (SELECT MAX(run_attempt) FROM run_history \
                                            WHERE run_id = h.run_id) \
                       AND NOT EXISTS (SELECT 1 FROM runs live WHERE live.run_id = h.run_id)",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map([repository.as_str()], |row| row.get::<_, String>(0))
                .map_err(db)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(db)?;
            let mut runs = Vec::with_capacity(rows.len());
            for run in rows {
                if let Some(record) = super::jobs::run_record(tx, codec::run_id(&run))? {
                    runs.push(record);
                }
            }
            Ok(runs)
        })
    }
}

/// v1 artifact catalog + durable run-event snapshot: the read paths that
/// keep the artifact endpoints and the SSE history alive across a restart,
/// including state imported from a legacy store.
impl LiteBackend {
    /// The run's durable outbox events, `event_id` (commit) order.
    pub(crate) async fn run_event_snapshot(
        &self,
        run_id: RunId,
    ) -> Result<Vec<serde_json::Value>, ControlError> {
        let run = codec::run_key(run_id);
        self.read(move |tx| {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT payload FROM outbox_events WHERE run_id = ?1 ORDER BY event_id",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map([run.as_str()], |row| row.get::<_, String>(0))
                .map_err(db)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(db)?;
            let mut events = Vec::with_capacity(rows.len());
            for payload in rows {
                // The outbox carries the run's NDJSON events plus rows that
                // make no event claim; skip those rather than emit bad lines.
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&payload) {
                    events.push(value);
                }
            }
            Ok(events)
        })
    }

    /// Finalized v1 artifacts, one run's or every run's, insertion order.
    pub(crate) async fn artifact_catalog(
        &self,
        run_id: Option<RunId>,
    ) -> Result<Vec<ArtifactCatalogRow>, ControlError> {
        let run = run_id.map(codec::run_key);
        let rows = self.read(move |tx| {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT public_id, run_id, name, storage_key, COALESCE(size_bytes, 0) \
                     FROM artifacts \
                     WHERE state = 'finalized' AND public_id IS NOT NULL \
                       AND (?1 IS NULL OR run_id = ?1) \
                     ORDER BY artifact_id",
                )
                .map_err(db)?;
            stmt.query_map([run.as_deref()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .map_err(db)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(db)
        })?;
        let mut artifacts = Vec::with_capacity(rows.len());
        for (public_id, run, name, storage_key, size_bytes) in rows {
            artifacts.push(ArtifactCatalogRow {
                public_id,
                run_id: codec::run_id(&run),
                name,
                storage_key,
                size_bytes,
            });
        }
        Ok(artifacts)
    }

    /// One finalized v1 artifact row by its public id, served by the unique
    /// `artifacts_public_id` index (the catalog-wide scan is never needed to
    /// answer a single id).
    pub(crate) async fn artifact_by_public_id(
        &self,
        public_id: &str,
    ) -> Result<Option<ArtifactCatalogRow>, ControlError> {
        let id = public_id.to_owned();
        let row = self.read(move |tx| {
            tx.prepare_cached(
                "SELECT public_id, run_id, name, storage_key, COALESCE(size_bytes, 0) \
                 FROM artifacts \
                 WHERE state = 'finalized' AND public_id = ?1",
            )
            .map_err(db)?
            .query_row([&id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .optional()
            .map_err(db)
        })?;
        Ok(row.map(
            |(public_id, run, name, storage_key, size_bytes)| ArtifactCatalogRow {
                public_id,
                run_id: codec::run_id(&run),
                name,
                storage_key,
                size_bytes,
            },
        ))
    }

    /// Upsert a finalized v1 artifact row (native uploads and imports).
    pub(crate) async fn put_artifact_catalog(
        &self,
        row: NewArtifactRow,
    ) -> Result<(), ControlError> {
        let run = codec::run_key(row.run_id);
        let now = now_us();
        self.write(move |tx| {
            tx.prepare_cached(
                "INSERT INTO artifacts (namespace_id, run_id, job_backend_id, name, state, \
                     size_bytes, storage_key, public_id, created_at, finalized_at) \
                 VALUES ('default', ?1, '', ?2, 'finalized', ?3, ?4, ?5, ?6, ?6) \
                 ON CONFLICT (run_id, job_backend_id, name) DO UPDATE SET \
                     size_bytes = excluded.size_bytes, \
                     storage_key = excluded.storage_key, \
                     public_id = excluded.public_id, \
                     finalized_at = excluded.finalized_at",
            )
            .map_err(db)?
            .execute(params![
                run,
                row.name,
                row.size_bytes,
                row.storage_key,
                row.public_id,
                now
            ])
            .map_err(db)?;
            Ok(())
        })
    }
}
