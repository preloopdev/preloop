//! Read paths over runs and jobs, the check-run mapping, push state, and
//! archival. Reads that served archived runs fall back to the `*_history`
//! tables (decision round 4, Q10): an archived run has no `runs` row.

use super::codec::{self, now_us};
use super::{db, LiteBackend};
use crate::control::types::*;
use crate::models::{JobDetail, PushState, PushStatus};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use rusqlite::{params, OptionalExtension};
use std::collections::BTreeSet;
type JobQueryRow = (String, String, Option<String>, Option<i64>, Option<String>);

/// Keep a run live while its push state may still dedup an echo webhook:
/// GitHub keeps deliveries redeliverable for three days (round 4, Q10).
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
    /// row. Returns whether the mapping changed (no row = no change).
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
            Ok(changed > 0)
        })
    }

    /// Clear the mapping only while it still points at `expected`.
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
                 SELECT display_name FROM job_history WHERE run_id = ?1 AND job_id = ?2 \
                 LIMIT 1",
            )
            .map_err(db)?
            .query_row(params![run, job_id.0], |row| row.get(0))
            .optional()
            .map_err(db)
        })
    }

    /// The job's recorded check-run id (live, else archived).
    pub(crate) async fn job_check_run_id(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<u64>, ControlError> {
        let run = codec::run_key(run_id);
        self.read(|tx| {
            tx.prepare_cached(
                "SELECT check_run_id FROM jobs WHERE run_id = ?1 AND job_id = ?2 \
                 UNION ALL \
                 SELECT check_run_id FROM job_history WHERE run_id = ?1 AND job_id = ?2 \
                 LIMIT 1",
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
                     SELECT job_id, status FROM job_history WHERE run_id = ?1 \
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
    /// id, annotations and the latest attempt's steps. Archived runs read
    /// `run_history`/`job_history` (steps from `step_history`).
    pub(crate) async fn run_dispatch_info(
        &self,
        run_id: RunId,
    ) -> Result<Option<RunDispatchInfo>, ControlError> {
        let run = codec::run_key(run_id);
        self.read(|tx| {
            let live = tx
                .prepare_cached(
                    "SELECT s.submission, r.started_at, r.completed_at \
                     FROM runs r JOIN run_submissions s ON s.run_id = r.run_id \
                     WHERE r.run_id = ?1",
                )
                .map_err(db)?
                .query_row([&run], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                .optional()
                .map_err(db)?;
            let (head, archived): ((String, Option<i64>, Option<i64>), bool) = match live {
                Some(head) => (head, false),
                None => match tx
                    .prepare_cached(
                        "SELECT submission, started_at, completed_at FROM run_history \
                         WHERE run_id = ?1 ORDER BY created_at DESC LIMIT 1",
                    )
                    .map_err(db)?
                    .query_row([&run], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                    .optional()
                    .map_err(db)?
                {
                    Some(head) => (head, true),
                    None => return Ok(None),
                },
            };
            let (submission, started_at, completed_at) = head;
            let value = submission_value(&submission)?;
            let job_rows: Vec<JobQueryRow> = {
                let sql = if archived {
                    "SELECT job_id, status, display_name, check_run_id, annotations \
                     FROM job_history WHERE run_id = ?1 ORDER BY job_id"
                } else {
                    "SELECT j.job_id, j.status, s.display_name, j.check_run_id, j.annotations \
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
                        ))
                    })
                    .map_err(db)?;
                rows.collect::<Result<_, _>>().map_err(db)?
            };
            let mut jobs = Vec::with_capacity(job_rows.len());
            for (job, status, display_name, check_run_id, annotations) in job_rows {
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
                 WHERE r.repository = ?2 AND (?3 IS NULL OR r.head_sha = ?3)";
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

    /// Queue pressure. Buckets from the queue-state combinations (round 4,
    /// Q13): `pending` = blocked without a concurrency wait (needs or
    /// max-parallel), `blocked` = blocked with a concurrency wait,
    /// `expanding` = expansion nodes not yet claimed.
    pub(crate) async fn queue_stats(&self) -> Result<QueueStats, ControlError> {
        self.read(|tx| {
            let mut stats = QueueStats::default();
            let mut stmt = tx
                .prepare_cached(&format!(
                    "SELECT {QUEUE_KIND} AS kind, COUNT(*) FROM jobs j \
                     WHERE j.queue_state NOT IN ('none', 'expanding') GROUP BY kind"
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

    /// The job's `(queue kind, status)` in the `QueueKind` vocabulary
    /// (round 4, Q13 mapping); `None` when the job has no live row.
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
                     SELECT run_id, job_id FROM job_history \
                     WHERE status IN ('success','failure','skipped','cancelled','timed_out')",
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
    pub(crate) async fn archive_finished_runs(
        &self,
        limit: usize,
    ) -> Result<Vec<RunId>, ControlError> {
        self.write(|tx| {
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
                         ORDER BY r.completed_at, r.run_id LIMIT ?3",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map(
                        params![
                            now - ARCHIVE_GRACE_US,
                            now - PUSH_ECHO_WINDOW_US,
                            limit.min(ARCHIVE_BATCH) as i64
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
                         created_at, started_at, completed_at) \
                     SELECT r.run_id, r.namespace_id, r.repository, r.workflow_path, \
                         r.run_number, r.run_attempt, r.run_name, r.event, r.ref, r.ref_type, \
                         r.head_ref, r.base_ref, r.head_sha, r.conclusion, \
                         COALESCE(s.submission, '{}'), \
                         COALESCE(s.record_details, '{}'), \
                         r.created_at, r.started_at, \
                         r.completed_at \
                     FROM runs r LEFT JOIN run_submissions s ON s.run_id = r.run_id \
                     WHERE r.run_id = ?1",
                    "INSERT INTO job_history (run_id, run_created_at, job_id, namespace_id, \
                         kind, parent_job_id, base_id, display_name, status, pool_key, outputs, \
                         annotations, check_run_id, created_at, deps_ready_at, started_at, \
                         completed_at) \
                     SELECT j.run_id, r.created_at, j.job_id, j.namespace_id, j.kind, \
                         j.parent_job_id, j.base_id, COALESCE(s.display_name, j.job_id), \
                         j.status, j.pool_key, j.outputs, j.annotations, j.check_run_id, \
                         j.created_at, j.deps_ready_at, j.started_at, j.completed_at \
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
}

/// `blocked` is dependencies/max-parallel (`pending` in the old vocabulary);
/// `held` is any concurrency gate, with `concurrency_waits.holder_kind`
/// distinguishing workflow (`run`) from job/jobset (round 5, B1).
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
    /// `run_history`/`job_history`/`attempt_history` when archived
    /// (decision Q10).
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

    /// `list_runs`: id order by `(terminal, last-activity desc)`, filtered
    /// on workflow/status/event, then each id's `run_record` projection
    /// (history fallback covers archived runs).
    pub(crate) async fn list_runs(
        &self,
        filter: crate::control::backend::RunListFilter,
    ) -> Result<Vec<crate::models::RunRecord>, ControlError> {
        self.read(move |tx| {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT run_id FROM ( \
                         SELECT run_id, \
                                CASE WHEN status = 'completed' THEN 1 ELSE 0 END AS terminal_rank, \
                                COALESCE(completed_at, started_at, created_at) AS sort_at \
                         FROM runs \
                         WHERE (?1 IS NULL OR instr(workflow_path, ?1) > 0) \
                           AND (?2 IS NULL OR status = ?2) \
                           AND (?3 IS NULL OR event = ?3) \
                         UNION ALL \
                         SELECT run_id, 1, COALESCE(completed_at, started_at, created_at) \
                         FROM run_history \
                         WHERE (?1 IS NULL OR instr(workflow_path, ?1) > 0) \
                           AND (?2 IS NULL OR 'completed' = ?2) \
                           AND (?3 IS NULL OR event = ?3)) \
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
                     SELECT run_id FROM run_history \
                     WHERE repository = ?1 COLLATE NOCASE",
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
