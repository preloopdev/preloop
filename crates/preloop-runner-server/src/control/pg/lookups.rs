//! Request, callback, check-run and run lookups; push state; run numbers;
//! key fingerprint; queue statistics.
//!
//! Archived runs: archive moves a run into the
//! `*_history` tables and deletes its `runs` row, so the reads that served
//! archived runs before fall back to history (`UNION ALL` of the live and
//! the history query; a run lives in exactly one of them).

use super::codec::{self, us};
use super::timelines::{STEP_COLUMNS, step_from_row};
use super::{PgBackend, db};
use crate::control::backend::RequestKey;
use crate::control::types::{
    CallbackJob, ControlError, QueueStats, RunConcurrency, RunDispatchInfo, RunDispatchJob,
    SubmissionFields, check_key_fingerprint,
};
use crate::models::{JobDetail, PushState, StepRecord, TaskAgentJobRequestRecord};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use std::collections::{BTreeMap, BTreeSet};

/// `SELECT` + `FROM` of every request read, in [`request_from_row`] order.
/// Append a `WHERE` on alias `q`. `plan_id` is not stored: it is the
/// attempt's `agent_job_id`, `plan_type` is `"actions"`; `locked_until` and
/// `last_renewed_at` come from the lease row (`''` / `None` without one).
pub(super) const REQUEST_SELECT: &str = concat!(
    "SELECT q.request_id, q.run_id::text, q.job_id, q.agent_job_id::text, \
     q.timeline_id::text, q.result, ",
    us!("l.expires_at"),
    ", ",
    us!("q.claimed_at"),
    ", q.runner_id, ",
    us!("q.started_at"),
    ", ",
    us!("l.renewed_at"),
    ", q.timeout_triggered, q.debug_token_issued \
     FROM job_requests q LEFT JOIN job_leases l ON l.request_id = q.request_id"
);

/// The same columns from `attempt_history` (alias `q`): an archived attempt
/// has no lease and never carries the timeout/debug flags.
const ATTEMPT_HISTORY_SELECT: &str = concat!(
    "SELECT q.request_id, q.run_id::text, q.job_id, q.agent_job_id::text, \
     q.timeline_id::text, q.result, NULL::int8, ",
    us!("q.claimed_at"),
    ", q.runner_id, ",
    us!("q.started_at"),
    ", NULL::int8, false, false FROM attempt_history q"
);

/// Decode one row selected with [`REQUEST_SELECT`].
pub(super) fn request_from_row(
    row: &tokio_postgres::Row,
) -> Result<TaskAgentJobRequestRecord, ControlError> {
    let agent_job_id = codec::uuid(row.get(3))?;
    Ok(TaskAgentJobRequestRecord {
        request_id: row.get(0),
        run_id: codec::run_id(row.get(1))?,
        job_id: codec::job_id(row.get(2)),
        agent_job_id,
        plan_id: agent_job_id.to_string(),
        plan_type: PLAN_TYPE.to_owned(),
        timeline_id: codec::uuid(row.get(4))?,
        result: row.get::<_, Option<&str>>(5).map(codec::status),
        locked_until: codec::locked_until_string(row.get(6)),
        claimed_at: row.get::<_, Option<i64>>(7).map(codec::us_to_system),
        owner_runner_id: row.get(8),
        started_at: row.get::<_, Option<i64>>(9).map(codec::us_to_system),
        last_renewed_at: row.get::<_, Option<i64>>(10).map(codec::us_to_system),
        timeout_triggered: row.get(11),
        debug_token_issued: row.get(12),
    })
}

/// The derived plan type of every attempt.
pub(super) const PLAN_TYPE: &str = "actions";

/// Terminal `jobs.status` values.
const TERMINAL_STATUSES: &str = "('success','failure','cancelled','skipped','timed_out')";

/// `jobs.queue_state` in the shared `QueueKind` vocabulary (the SQLite twin,
/// `control::lite::queries::QUEUE_KIND`): `blocked` is dependencies/max
/// parallel (`pending`), `held` is a concurrency gate (`blocked`), and both
/// expansion states are `expand`.
const QUEUE_KIND: &str = "CASE j.queue_state \
     WHEN 'blocked' THEN 'pending' \
     WHEN 'held' THEN 'blocked' \
     WHEN 'pending_expansion' THEN 'expand' \
     WHEN 'expanding' THEN 'expand' \
     ELSE j.queue_state END";

/// A plan id is the attempt's `agent_job_id`; anything else names no plan.
fn plan_uuid(plan_id: &str) -> Option<String> {
    plan_id.parse::<uuid::Uuid>().ok().map(|id| id.to_string())
}

fn run_text(run_id: RunId) -> String {
    run_id.0.to_string()
}

impl PgBackend {
    /// A request record by id, plan id (= agent job id), agent job id,
    /// timeline id, or latest attempt of a `(run, job)`. `NotFound` when
    /// nothing matches.
    ///
    /// Statement: `SELECT <request columns> FROM job_requests q LEFT JOIN
    /// job_leases l .. WHERE <key> ORDER BY q.request_id DESC LIMIT 1`.
    pub(super) async fn request(
        &self,
        key: RequestKey,
    ) -> Result<TaskAgentJobRequestRecord, ControlError> {
        let not_found = || ControlError::NotFound("request".to_owned());
        let client = self.reader().await?;
        let row =
            match &key {
                RequestKey::Id(id) => {
                    client
                        .query_opt(&format!("{REQUEST_SELECT} WHERE q.request_id = $1"), &[id])
                        .await
                }
                RequestKey::PlanId(plan) => {
                    let plan = plan_uuid(plan).ok_or_else(not_found)?;
                    client
                        .query_opt(
                            &format!("{REQUEST_SELECT} WHERE q.agent_job_id = $1::text::uuid"),
                            &[&plan],
                        )
                        .await
                }
                RequestKey::AgentJobId(agent) => {
                    client
                        .query_opt(
                            &format!("{REQUEST_SELECT} WHERE q.agent_job_id = $1::text::uuid"),
                            &[&agent.to_string()],
                        )
                        .await
                }
                RequestKey::TimelineId(timeline) => {
                    client
                        .query_opt(
                            &format!("{REQUEST_SELECT} WHERE q.timeline_id = $1::text::uuid"),
                            &[&timeline.to_string()],
                        )
                        .await
                }
                RequestKey::Job(run_id, job_id) => client
                    .query_opt(
                        &format!(
                            "{REQUEST_SELECT} WHERE q.run_id = $1::text::uuid AND q.job_id = $2 \
                             ORDER BY q.request_id DESC LIMIT 1"
                        ),
                        &[&run_text(*run_id), &job_id.0],
                    )
                    .await,
            }
            .map_err(db)?
            .ok_or_else(not_found)?;
        request_from_row(&row)
    }

    /// The run of each known plan id (= agent job id); unknown plan ids are
    /// omitted.
    ///
    /// Statement: `SELECT agent_job_id, run_id FROM job_requests WHERE
    /// agent_job_id = ANY($1::uuid[])`.
    pub(super) async fn artifact_scopes(
        &self,
        plan_ids: &[String],
    ) -> Result<BTreeMap<String, RunId>, ControlError> {
        let plans: Vec<String> = plan_ids.iter().filter_map(|p| plan_uuid(p)).collect();
        if plans.is_empty() {
            return Ok(BTreeMap::new());
        }
        let client = self.reader().await?;
        let rows = client
            .query(
                "SELECT agent_job_id::text, run_id::text FROM job_requests \
                 WHERE agent_job_id = ANY($1::text[]::uuid[])",
                &[&plans],
            )
            .await
            .map_err(db)?;
        let by_uuid: BTreeMap<String, RunId> = rows
            .iter()
            .map(|row| Ok((row.get::<_, String>(0), codec::run_id(row.get(1))?)))
            .collect::<Result<_, ControlError>>()?;
        // Key the result by the caller's spelling of each plan id.
        Ok(plan_ids
            .iter()
            .filter_map(|plan| {
                let run = by_uuid.get(&plan_uuid(plan)?)?;
                Some((plan.clone(), *run))
            })
            .collect())
    }

    /// Record a run's push-sync state. An unknown (or archived) run writes
    /// nothing and is not an error — the same contract as the SQLite backend.
    ///
    /// Statement: `INSERT INTO run_push_states .. SELECT .. WHERE EXISTS
    /// (runs) ON CONFLICT (run_id) DO UPDATE`.
    pub(super) async fn set_push_state(
        &self,
        run_id: RunId,
        state: PushState,
    ) -> Result<(), ControlError> {
        let status = serde_json::to_value(state.status)
            .map_err(ControlError::backend)?
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let pr_number = state.pr_number.map(|n| n.min(i64::MAX as u64) as i64);
        let client = self.writer().await?;
        client
            .execute(
                "INSERT INTO run_push_states (run_id, status, error, pr_number, effective_sha, \
                 updated_at) SELECT run_id, $2, $3, $4, $5, now() FROM runs \
                 WHERE run_id = $1::text::uuid \
                 ON CONFLICT (run_id) DO UPDATE SET status = EXCLUDED.status, \
                 error = EXCLUDED.error, pr_number = EXCLUDED.pr_number, \
                 effective_sha = EXCLUDED.effective_sha, updated_at = EXCLUDED.updated_at",
                &[
                    &run_text(run_id),
                    &status,
                    &state.error,
                    &pr_number,
                    &state.effective_sha,
                ],
            )
            .await
            .map_err(db)?;
        Ok(())
    }

    /// Next run number of a workflow (per namespace + repository +
    /// workflow path; gaps allowed).
    ///
    /// Statement: `INSERT INTO workflow_run_numbers` seeds from the maximum
    /// existing run number and increments atomically on conflict.
    pub(super) async fn allocate_run_number(
        &self,
        namespace_id: &str,
        repository: &str,
        workflow_path: &str,
    ) -> Result<u64, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        // `runs` inserts the namespace in its own transaction, but the API
        // allocates the number before building/submitting that run. Create
        // the namespace here so the counter's FK is valid on first use.
        tx.execute(
            "INSERT INTO namespaces (namespace_id) VALUES ($1) ON CONFLICT DO NOTHING",
            &[&namespace_id],
        )
        .await
        .map_err(db)?;
        let number: i64 = tx
            .query_one(
                "INSERT INTO workflow_run_numbers AS w \
                 (namespace_id, repository, workflow_path, last_run_number) \
                 VALUES ($1, $2, $3, GREATEST(1, COALESCE( \
                     (SELECT MAX(run_number) + 1 FROM runs \
                      WHERE namespace_id=$1 AND repository=$2 AND workflow_path=$3), 1))) \
                 ON CONFLICT (namespace_id, repository, workflow_path) \
                 DO UPDATE SET last_run_number = GREATEST(\
                     w.last_run_number + 1,\
                     COALESCE((SELECT MAX(run_number) + 1 FROM runs \
                               WHERE namespace_id=$1 AND repository=$2 AND workflow_path=$3), 1)) \
                 RETURNING last_run_number",
                &[&namespace_id, &repository, &workflow_path],
            )
            .await
            .map_err(db)?
            .get(0);
        tx.commit().await.map_err(db)?;
        Ok(number.max(0) as u64)
    }

    /// Record the cluster key fingerprint on first use, then refuse a node
    /// whose key differs.
    ///
    /// Statements: `INSERT INTO schema_meta ('key_fingerprint', $1) ON
    /// CONFLICT DO NOTHING`; `SELECT value FROM schema_meta WHERE key =
    /// 'key_fingerprint'`.
    pub(super) async fn ensure_key_fingerprint(
        &self,
        fingerprint: &str,
    ) -> Result<(), ControlError> {
        let client = self.writer().await?;
        client
            .execute(
                "INSERT INTO schema_meta (key, value) VALUES ('key_fingerprint', $1) \
                 ON CONFLICT (key) DO NOTHING",
                &[&fingerprint.as_bytes()],
            )
            .await
            .map_err(db)?;
        let stored: Vec<u8> = client
            .query_one(
                "SELECT value FROM schema_meta WHERE key = 'key_fingerprint'",
                &[],
            )
            .await
            .map_err(db)?
            .get(0);
        check_key_fingerprint(&stored, fingerprint)
    }

    /// Every job id of a run (jobs ∪ its attempts' job ids), sorted;
    /// archived runs read history. `NotFound` for an unknown run.
    ///
    /// Statement: `SELECT job_id FROM jobs .. UNION SELECT job_id FROM
    /// job_requests .. UNION SELECT job_id FROM job_history .. UNION SELECT
    /// job_id FROM attempt_history .. ORDER BY 1`, after an existence check
    /// on `runs` / `run_history`.
    pub(super) async fn run_job_ids(&self, run_id: RunId) -> Result<Vec<String>, ControlError> {
        let run = run_text(run_id);
        let client = self.reader().await?;
        let exists: bool = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = $1::text::uuid) \
                 OR EXISTS (SELECT 1 FROM run_history WHERE run_id = $1::text::uuid)",
                &[&run],
            )
            .await
            .map_err(db)?
            .get(0);
        if !exists {
            return Err(ControlError::NotFound("run not found".to_owned()));
        }
        Ok(client
            .query(
                "SELECT job_id FROM jobs WHERE run_id = $1::text::uuid \
                 UNION SELECT job_id FROM job_requests WHERE run_id = $1::text::uuid \
                 UNION SELECT job_id FROM job_history WHERE run_id = $1::text::uuid \
                 UNION SELECT job_id FROM attempt_history WHERE run_id = $1::text::uuid \
                 ORDER BY 1",
                &[&run],
            )
            .await
            .map_err(db)?
            .iter()
            .map(|row| row.get(0))
            .collect())
    }

    /// Record a job's GitHub check-run id. Writes the `jobs` row when one
    /// exists plus `run_submissions.record_details` — the store of record
    /// for a job minted before its matrix leg materializes (lite parity).
    /// Returns whether either mapping changed.
    ///
    /// Statements: `UPDATE jobs SET check_run_id`; `UPDATE run_submissions
    /// SET record_details = jsonb_set(..) WHERE id IS DISTINCT FROM`.
    pub(super) async fn set_job_check_run(
        &self,
        run_id: RunId,
        job_id: &JobId,
        check_run_id: u64,
    ) -> Result<bool, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let changed = tx
            .execute(
                "UPDATE jobs SET check_run_id = $3 WHERE run_id = $1::text::uuid AND job_id = $2 \
                 AND check_run_id IS DISTINCT FROM $3",
                &[&run_text(run_id), &job_id.0, &(check_run_id as i64)],
            )
            .await
            .map_err(db)?;
        let detail_changed = tx
            .execute(
                "UPDATE run_submissions \
                 SET record_details = jsonb_set(record_details, \
                        ARRAY['job_check_run_ids', $2], to_jsonb($3::bigint), true) \
                 WHERE run_id = $1::text::uuid \
                   AND (record_details->'job_check_run_ids'->$2) IS DISTINCT FROM to_jsonb($3::bigint)",
                &[&run_text(run_id), &job_id.0, &(check_run_id as i64)],
            )
            .await
            .map_err(db)?;
        let mapping_changed = changed > 0 || detail_changed > 0;
        if mapping_changed {
            // The mapping's `CheckRunCreated` event persists atomically with
            // it; the caller broadcasts via `emit_persisted` (a restart must
            // not lose the mapping's event while keeping the mapping).
            super::dispatch::append_event_tx(
                &tx,
                &preloop_gha_protocol::NdjsonEvent::CheckRunCreated { run_id },
            )
            .await?;
        }
        tx.commit().await.map_err(db)?;
        Ok(mapping_changed)
    }

    /// Clear a job's check-run id only while it still equals `expected` —
    /// in the `jobs` row and in `record_details` (legs minted before
    /// materialization; a stale mapping there would re-mint a deleted check
    /// run forever).
    ///
    /// Statements: `UPDATE jobs SET check_run_id = NULL`; `UPDATE
    /// run_submissions SET record_details = record_details - $2`.
    pub(super) async fn clear_job_check_run(
        &self,
        run_id: RunId,
        job_id: &JobId,
        expected: u64,
    ) -> Result<(), ControlError> {
        let client = self.writer().await?;
        client
            .execute(
                "UPDATE jobs SET check_run_id = NULL WHERE run_id = $1::text::uuid \
                 AND job_id = $2 AND check_run_id = $3",
                &[&run_text(run_id), &job_id.0, &(expected as i64)],
            )
            .await
            .map_err(db)?;
        client
            .execute(
                "UPDATE run_submissions \
                 SET record_details = record_details #- ARRAY['job_check_run_ids', $2] \
                 WHERE run_id = $1::text::uuid \
                   AND (record_details->'job_check_run_ids'->$2) = to_jsonb($3::bigint)",
                &[&run_text(run_id), &job_id.0, &(expected as i64)],
            )
            .await
            .map_err(db)?;
        Ok(())
    }

    /// Whether the run has a `jobs` row for `job_id`.
    ///
    /// Statement: `SELECT EXISTS (SELECT 1 FROM jobs WHERE run_id, job_id)`.
    pub(super) async fn job_exists(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<bool, ControlError> {
        let client = self.reader().await?;
        Ok(client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM jobs WHERE run_id = $1::text::uuid AND job_id = $2)",
                &[&run_text(run_id), &job_id.0],
            )
            .await
            .map_err(db)?
            .get(0))
    }

    /// A job's evaluated display name — live `job_specs`, else the archived
    /// `job_history`.
    ///
    /// Statement: `SELECT display_name FROM job_specs WHERE run_id, job_id
    /// UNION ALL SELECT .. FROM job_history .. LIMIT 1`.
    pub(super) async fn job_display_name(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<String>, ControlError> {
        let client = self.reader().await?;
        Ok(client
            .query_opt(
                "SELECT display_name FROM job_specs \
                 WHERE run_id = $1::text::uuid AND job_id = $2 \
                 UNION ALL \
                 SELECT display_name FROM job_history \
                 WHERE run_id = $1::text::uuid AND job_id = $2 \
                 LIMIT 1",
                &[&run_text(run_id), &job_id.0],
            )
            .await
            .map_err(db)?
            .map(|row| row.get(0)))
    }

    /// A job's recorded check-run id: the live `jobs` row, `job_history`,
    /// then `record_details` — legs minted before their expansion
    /// materializes have no row (lite parity).
    ///
    /// Statements: `SELECT check_run_id FROM jobs UNION ALL job_history`;
    /// fallback `SELECT record_details->'job_check_run_ids'->>$2` from
    /// `run_submissions` UNION latest `run_history`.
    pub(super) async fn job_check_run_id(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<u64>, ControlError> {
        let client = self.reader().await?;
        let row_id = client
            .query_opt(
                "SELECT check_run_id FROM jobs WHERE run_id = $1::text::uuid AND job_id = $2 \
                 UNION ALL \
                 SELECT check_run_id FROM job_history \
                 WHERE run_id = $1::text::uuid AND job_id = $2 LIMIT 1",
                &[&run_text(run_id), &job_id.0],
            )
            .await
            .map_err(db)?
            .and_then(|row| row.get::<_, Option<i64>>(0));
        if let Some(id) = row_id {
            return Ok(Some(id as u64));
        }
        Ok(client
            .query_opt(
                "SELECT (record_details->'job_check_run_ids'->>$2)::bigint \
                 FROM run_submissions WHERE run_id = $1::text::uuid \
                 UNION ALL \
                 SELECT (record_details->'job_check_run_ids'->>$2)::bigint \
                 FROM run_history WHERE run_id = $1::text::uuid \
                   AND created_at = (SELECT MAX(created_at) FROM run_history \
                                     WHERE run_id = $1::text::uuid) \
                 LIMIT 1",
                &[&run_text(run_id), &job_id.0],
            )
            .await
            .map_err(db)?
            .and_then(|row| row.get::<_, Option<i64>>(0))
            .map(|id| id as u64))
    }

    /// Check-run reporting inputs for one run: repository and sha from the
    /// stored submission, run clocks, and per job (in job id order) its
    /// status, display name, check-run id, `placeholder` flag (an expandable
    /// node that never dispatches), derived detail and latest attempt's
    /// steps. Archived runs read history (no steps). `None` for an unknown
    /// run.
    ///
    /// Statements: `SELECT .. FROM runs JOIN run_submissions`; `SELECT ..
    /// FROM jobs LEFT JOIN job_specs .. latest attempt`; `SELECT <step
    /// columns> FROM job_steps WHERE agent_job_id = ANY(latest attempts)`;
    /// history fallback `SELECT .. FROM run_history` / `job_history`.
    pub(super) async fn run_dispatch_info(
        &self,
        run_id: RunId,
    ) -> Result<Option<RunDispatchInfo>, ControlError> {
        let run = run_text(run_id);
        let client = self.reader().await?;
        if let Some(head) = client
            .query_opt(
                concat!(
                    "SELECT s.submission::text, ",
                    us!("r.started_at"),
                    ", ",
                    us!("r.completed_at"),
                    ", s.record_details::text FROM runs r JOIN run_submissions s \
                     ON s.run_id = r.run_id \
                     WHERE r.run_id = $1::text::uuid"
                ),
                &[&run],
            )
            .await
            .map_err(db)?
        {
            let submission: serde_json::Value = codec::from_json(head.get(0))?;
            let job_rows = client
                .query(
                    "SELECT j.job_id, j.status, s.display_name, j.check_run_id, \
                     j.annotations::text, \
                     (SELECT q.agent_job_id::text FROM job_requests q \
                      WHERE q.run_id = j.run_id AND q.job_id = j.job_id \
                      ORDER BY q.request_id DESC LIMIT 1), \
                     (j.kind IN ('matrix_parent','reusable_caller')) \
                     FROM jobs j LEFT JOIN job_specs s ON s.run_id = j.run_id AND s.job_id = j.job_id \
                     WHERE j.run_id = $1::text::uuid ORDER BY j.job_id",
                    &[&run],
                )
                .await
                .map_err(db)?;
            let agents: Vec<String> = job_rows
                .iter()
                .filter_map(|row| row.get::<_, Option<String>>(5))
                .collect();
            let mut steps: BTreeMap<uuid::Uuid, Vec<StepRecord>> = BTreeMap::new();
            if !agents.is_empty() {
                for row in client
                    .query(
                        &format!(
                            "SELECT {STEP_COLUMNS} FROM job_steps \
                             WHERE agent_job_id = ANY($1::text[]::uuid[]) \
                             ORDER BY agent_job_id, position"
                        ),
                        &[&agents],
                    )
                    .await
                    .map_err(db)?
                {
                    let (agent, step) = step_from_row(&row)?;
                    steps.entry(agent).or_default().push(step);
                }
            }
            let mut jobs = Vec::with_capacity(job_rows.len());
            for row in &job_rows {
                let job_id: String = row.get(0);
                let status = codec::status(row.get(1));
                let display_name: Option<String> = row.get(2);
                let placeholder: bool = row.get(6);
                let job_steps = match row.get::<_, Option<&str>>(5) {
                    Some(agent) => steps.remove(&codec::uuid(agent)?).unwrap_or_default(),
                    None => Vec::new(),
                };
                let annotations: Vec<serde_json::Value> = row
                    .get::<_, Option<&str>>(4)
                    .map(codec::from_json)
                    .transpose()?
                    .unwrap_or_default();
                jobs.push(RunDispatchJob {
                    detail: Some(JobDetail {
                        job_id: job_id.clone(),
                        name: display_name.clone().unwrap_or_else(|| job_id.clone()),
                        conclusion: crate::status_string(status),
                        steps: job_steps.clone(),
                        annotations,
                    }),
                    job_id: JobId(job_id),
                    status,
                    display_name,
                    check_run_id: row.get::<_, Option<i64>>(3).map(|id| id as u64),
                    placeholder,
                    steps: job_steps,
                });
            }
            // Legs minted before materialization live only in
            // `record_details.job_check_run_ids`; reports mint for them too.
            if let Some(details) = head
                .get::<_, Option<String>>(3)
                .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
                && let Ok(stored) = serde_json::from_value::<BTreeMap<String, i64>>(
                    details
                        .get("job_check_run_ids")
                        .cloned()
                        .unwrap_or_default(),
                )
            {
                for (job_id, id) in stored {
                    if !jobs.iter().any(|job| job.job_id.0 == job_id) {
                        jobs.push(RunDispatchJob {
                            detail: None,
                            job_id: JobId(job_id.clone()),
                            status: ExecutionStatus::Pending,
                            display_name: Some(job_id),
                            check_run_id: Some(id as u64),
                            placeholder: false,
                            steps: Vec::new(),
                        });
                    }
                }
            }
            return Ok(Some(RunDispatchInfo {
                repository: submission["repository"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                sha: submission["sha"].as_str().unwrap_or_default().to_owned(),
                started_at: head.get::<_, Option<i64>>(1).map(codec::us_to_system),
                completed_at: head.get::<_, Option<i64>>(2).map(codec::us_to_system),
                jobs,
            }));
        }
        let Some(head) = client
            .query_opt(
                concat!(
                    "SELECT submission::text, ",
                    us!("started_at"),
                    ", ",
                    us!("completed_at"),
                    ", created_at, record_details::text FROM run_history WHERE run_id = $1::text::uuid"
                ),
                &[&run],
            )
            .await
            .map_err(db)?
        else {
            return Ok(None);
        };
        let submission: serde_json::Value = codec::from_json(head.get(0))?;
        let job_rows = client
            .query(
                "SELECT j.job_id, j.status, j.display_name, j.check_run_id, j.annotations::text, \
                 (j.kind IN ('matrix_parent','reusable_caller')) \
                 FROM job_history j JOIN run_history r \
                   ON r.run_id = j.run_id AND r.created_at = j.run_created_at \
                 WHERE j.run_id = $1::text::uuid ORDER BY j.job_id",
                &[&run],
            )
            .await
            .map_err(db)?;
        let mut jobs = job_rows
            .iter()
            .map(|row| {
                let job_id: String = row.get(0);
                let status = codec::status(row.get(1));
                let display_name: String = row.get(2);
                let annotations: Vec<serde_json::Value> = row
                    .get::<_, Option<&str>>(4)
                    .map(codec::from_json)
                    .transpose()?
                    .unwrap_or_default();
                Ok(RunDispatchJob {
                    detail: Some(JobDetail {
                        job_id: job_id.clone(),
                        name: display_name.clone(),
                        conclusion: crate::status_string(status),
                        steps: Vec::new(),
                        annotations,
                    }),
                    job_id: JobId(job_id),
                    status,
                    display_name: Some(display_name),
                    check_run_id: row.get::<_, Option<i64>>(3).map(|id| id as u64),
                    placeholder: row.get(5),
                    steps: Vec::new(),
                })
            })
            .collect::<Result<Vec<_>, ControlError>>()?;
        // Legs minted before materialization live only in the archived
        // `record_details`; their reports still mint.
        if let Ok(details) = serde_json::from_str::<serde_json::Value>(
            head.get::<_, Option<String>>(4).as_deref().unwrap_or("{}"),
        ) && let Ok(stored) = serde_json::from_value::<BTreeMap<String, i64>>(
            details
                .get("job_check_run_ids")
                .cloned()
                .unwrap_or_default(),
        ) {
            for (job_id, id) in stored {
                if !jobs.iter().any(|job| job.job_id.0 == job_id) {
                    jobs.push(RunDispatchJob {
                        detail: None,
                        job_id: JobId(job_id.clone()),
                        status: ExecutionStatus::Pending,
                        display_name: Some(job_id),
                        check_run_id: Some(id as u64),
                        placeholder: false,
                        steps: Vec::new(),
                    });
                }
            }
        }
        Ok(Some(RunDispatchInfo {
            repository: submission["repository"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            sha: submission["sha"].as_str().unwrap_or_default().to_owned(),
            started_at: head.get::<_, Option<i64>>(1).map(codec::us_to_system),
            completed_at: head.get::<_, Option<i64>>(2).map(codec::us_to_system),
            jobs,
        }))
    }

    /// `(repository, sha, base_ref, git_ref)` from a live run's stored
    /// submission; `None` for an unknown run.
    ///
    /// Statement: `SELECT submission FROM run_submissions WHERE run_id`.
    pub(super) async fn submission_fields(
        &self,
        run_id: RunId,
    ) -> Result<Option<SubmissionFields>, ControlError> {
        let client = self.reader().await?;
        let Some(row) = client
            .query_opt(
                "SELECT submission::text FROM run_submissions WHERE run_id = $1::text::uuid",
                &[&run_text(run_id)],
            )
            .await
            .map_err(db)?
        else {
            return Ok(None);
        };
        let value: serde_json::Value = codec::from_json(row.get(0))?;
        Ok(Some(SubmissionFields {
            repository: value["repository"].as_str().unwrap_or_default().to_owned(),
            sha: value["sha"].as_str().unwrap_or_default().to_owned(),
            base_ref: value["base_ref"].as_str().map(str::to_owned),
            git_ref: value["git_ref"].as_str().unwrap_or_default().to_owned(),
        }))
    }

    /// Status per logical job, in job id order; archived runs read
    /// history. `None` for an unknown run.
    ///
    /// Statement: `SELECT job_id, status FROM jobs WHERE run_id UNION ALL
    /// SELECT job_id, status FROM job_history WHERE run_id ORDER BY job_id`,
    /// after an existence check.
    pub(super) async fn run_job_statuses(
        &self,
        run_id: RunId,
    ) -> Result<Option<Vec<(JobId, ExecutionStatus)>>, ControlError> {
        let run = run_text(run_id);
        let client = self.reader().await?;
        let exists: bool = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = $1::text::uuid) \
                 OR EXISTS (SELECT 1 FROM run_history WHERE run_id = $1::text::uuid)",
                &[&run],
            )
            .await
            .map_err(db)?
            .get(0);
        if !exists {
            return Ok(None);
        }
        Ok(Some(
            client
                .query(
                    "SELECT job_id, status FROM jobs WHERE run_id = $1::text::uuid \
                     UNION ALL SELECT job_id, status FROM job_history WHERE run_id = $1::text::uuid \
                     ORDER BY 1",
                    &[&run],
                )
                .await
                .map_err(db)?
                .iter()
                .map(|row| (JobId(row.get(0)), codec::status(row.get(1))))
                .collect(),
        ))
    }

    /// Live-log key for `job_id` (a logical job id or an agent job id) in a
    /// run: the latest attempt's agent job id, else the logical key when the
    /// run owns that job. The flag reports whether the run or the job is
    /// terminal (an archived run always is). `None` for an unknown run, or
    /// a key the run does not own.
    ///
    /// Statements: run status from `runs` (else `run_history`); latest
    /// attempt `SELECT agent_job_id FROM job_requests|attempt_history WHERE
    /// run_id AND (job_id = $2 OR agent_job_id::text = $2) ORDER BY
    /// request_id DESC LIMIT 1`; job status from `jobs|job_history`.
    pub(super) async fn live_log_key(
        &self,
        run_id: RunId,
        job_id: &str,
    ) -> Result<Option<(String, bool)>, ControlError> {
        let run = run_text(run_id);
        let client = self.reader().await?;
        let run_status = client
            .query_opt(
                "SELECT status = 'completed', false FROM runs WHERE run_id = $1::text::uuid \
                 UNION ALL SELECT true, true FROM run_history WHERE run_id = $1::text::uuid \
                 LIMIT 1",
                &[&run],
            )
            .await
            .map_err(db)?;
        let Some(run_status) = run_status else {
            return Ok(None);
        };
        let run_terminal: bool = run_status.get(0);
        let archived: bool = run_status.get(1);
        let (attempts, jobs) = if archived {
            ("attempt_history", "job_history")
        } else {
            ("job_requests", "jobs")
        };
        let record: Option<String> = client
            .query_opt(
                &format!(
                    "SELECT agent_job_id::text FROM {attempts} WHERE run_id = $1::text::uuid \
                     AND (job_id = $2 OR agent_job_id::text = $2) \
                     ORDER BY request_id DESC LIMIT 1"
                ),
                &[&run, &job_id],
            )
            .await
            .map_err(db)?
            .map(|row| row.get(0));
        let job_status: Option<String> = client
            .query_opt(
                &format!("SELECT status FROM {jobs} WHERE run_id = $1::text::uuid AND job_id = $2"),
                &[&run, &job_id],
            )
            .await
            .map_err(db)?
            .map(|row| row.get(0));
        let key = match record {
            Some(agent) => agent,
            // A bare logical key is valid only when the run owns it.
            None if job_status.is_some() => job_id.to_owned(),
            None => return Ok(None),
        };
        let job_terminal = job_status
            .as_deref()
            .is_some_and(|status| codec::status(status).is_terminal());
        Ok(Some((key, run_terminal || job_terminal)))
    }

    /// Every request of a run sorted by request id; an archived run reads
    /// `attempt_history`.
    ///
    /// Statement: `SELECT <request columns> .. WHERE q.run_id UNION ALL
    /// SELECT <history columns> .. WHERE q.run_id ORDER BY 1`.
    pub(super) async fn run_requests(
        &self,
        run_id: RunId,
    ) -> Result<Vec<TaskAgentJobRequestRecord>, ControlError> {
        let client = self.reader().await?;
        client
            .query(
                &format!(
                    "{REQUEST_SELECT} WHERE q.run_id = $1::text::uuid UNION ALL \
                     {ATTEMPT_HISTORY_SELECT} WHERE q.run_id = $1::text::uuid ORDER BY 1"
                ),
                &[&run_text(run_id)],
            )
            .await
            .map_err(db)?
            .iter()
            .map(request_from_row)
            .collect()
    }

    /// Resolve a check-run rerequest to `(run_id, job_id)` among completed
    /// runs (live or archived) of `repository` at `head_sha`: an exact
    /// check-run id hit first, else the check-run name against job ids and
    /// display names. `details_run_id` wins ties; then run id order.
    ///
    /// Statement (twice, id then name): `SELECT run_id, job_id FROM (<live
    /// jobs of completed runs> UNION ALL <job_history>) c WHERE <match>
    /// ORDER BY (run_id = $details) DESC, run_id LIMIT 1`.
    pub(super) async fn check_run_target(
        &self,
        check_run_id: u64,
        repository: &str,
        head_sha: Option<&str>,
        job_name: Option<&str>,
        details_run_id: Option<RunId>,
    ) -> Result<Option<(RunId, JobId)>, ControlError> {
        const CANDIDATES: &str = "SELECT run_id::text AS run_id, job_id, check_run_id, \
            display_name FROM (\
              SELECT j.run_id, j.job_id, j.check_run_id, s.display_name FROM jobs j \
              JOIN runs r ON r.run_id = j.run_id \
              LEFT JOIN job_specs s ON s.run_id = j.run_id AND s.job_id = j.job_id \
              WHERE r.status = 'completed' AND r.repository = $1 \
                AND ($2::text IS NULL OR r.head_sha = $2) \
              UNION ALL \
              SELECT j.run_id, j.job_id, j.check_run_id, j.display_name FROM job_history j \
              JOIN run_history r ON r.run_id = j.run_id AND r.created_at = j.run_created_at \
              WHERE r.repository = $1 AND ($2::text IS NULL OR r.head_sha = $2) \
              UNION ALL \
              SELECT s.run_id, je.key, (je.value::text)::bigint, je.key \
              FROM run_submissions s \
              JOIN runs r ON r.run_id = s.run_id, \
              jsonb_each(s.record_details->'job_check_run_ids') je \
              WHERE r.status = 'completed' AND r.repository = $1 \
                AND ($2::text IS NULL OR r.head_sha = $2) \
              UNION ALL \
              SELECT h.run_id, je.key, (je.value::text)::bigint, je.key \
              FROM run_history h, \
              jsonb_each(h.record_details->'job_check_run_ids') je \
              WHERE h.repository = $1 AND ($2::text IS NULL OR h.head_sha = $2) \
                AND h.created_at = (SELECT MAX(created_at) FROM run_history \
                                    WHERE run_id = h.run_id)) c";
        let details = details_run_id.map(run_text);
        let client = self.reader().await?;
        let by_id = client
            .query_opt(
                &format!(
                    "{CANDIDATES} WHERE check_run_id = $4 \
                     ORDER BY COALESCE(run_id = $3::text, false) DESC, run_id LIMIT 1"
                ),
                &[&repository, &head_sha, &details, &(check_run_id as i64)],
            )
            .await
            .map_err(db)?;
        let hit = match (by_id, job_name) {
            (Some(row), _) => Some(row),
            (None, Some(name)) => client
                .query_opt(
                    &format!(
                        "{CANDIDATES} WHERE job_id = $4 OR display_name = $4 \
                         ORDER BY COALESCE(run_id = $3::text, false) DESC, run_id LIMIT 1"
                    ),
                    &[&repository, &head_sha, &details, &name],
                )
                .await
                .map_err(db)?,
            (None, None) => None,
        };
        hit.map(|row| Ok((codec::run_id(row.get(0))?, JobId(row.get(1)))))
            .transpose()
    }

    /// The stored submission JSON of the run owning an attempt.
    ///
    /// Statement: `SELECT s.submission FROM job_requests q JOIN
    /// run_submissions s .. WHERE q.agent_job_id`.
    pub(super) async fn submission_json_for_attempt(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<String>, ControlError> {
        let client = self.reader().await?;
        Ok(client
            .query_opt(
                "SELECT s.submission::text FROM job_requests q \
                 JOIN run_submissions s ON s.run_id = q.run_id \
                 WHERE q.agent_job_id = $1::text::uuid",
                &[&agent_job_id.to_string()],
            )
            .await
            .map_err(db)?
            .map(|row| row.get(0)))
    }

    /// The completed run whose push already published `sha` for
    /// `workflow_path` on `repository`: matched on the submitted sha or the
    /// push's effective sha. Archive keeps such runs live while the push is
    /// pending or recent, so this reads live tables only.
    ///
    /// Statement: `SELECT r.run_id FROM runs r JOIN run_push_states p ..
    /// JOIN run_submissions s .. WHERE r.status = 'completed' AND repository
    /// AND workflow_path AND (s.submission->>'sha' = $2 OR p.effective_sha =
    /// $2) ORDER BY r.run_id LIMIT 1`.
    pub(super) async fn published_run(
        &self,
        repository: &str,
        sha: &str,
        workflow_path: &str,
    ) -> Result<Option<RunId>, ControlError> {
        let client = self.reader().await?;
        client
            .query_opt(
                "SELECT r.run_id::text FROM runs r \
                 JOIN run_push_states p ON p.run_id = r.run_id \
                 JOIN run_submissions s ON s.run_id = r.run_id \
                 WHERE r.status = 'completed' AND r.repository = $1 AND r.workflow_path = $3 \
                   AND (s.submission->>'sha' = $2 OR p.effective_sha = $2) \
                 ORDER BY r.run_id LIMIT 1",
                &[&repository, &sha, &workflow_path],
            )
            .await
            .map_err(db)?
            .map(|row| codec::run_id(row.get(0)))
            .transpose()
    }

    /// `run_for_webhook_delivery`: indexed `(delivery_id, workflow_path)`
    /// point read over live runs; `None` when no committed run exists yet.
    pub(super) async fn run_for_webhook_delivery(
        &self,
        delivery_id: &str,
        workflow_path: &str,
    ) -> Result<Option<RunId>, ControlError> {
        let client = self.reader().await?;
        client
            .query_opt(
                "SELECT run_id::text FROM runs WHERE webhook_delivery_id = $1 \
                 AND workflow_path = $2",
                &[&delivery_id, &workflow_path],
            )
            .await
            .map_err(db)?
            .map(|row| codec::run_id(row.get(0)))
            .transpose()
    }

    /// Whether the run waits behind a workflow-level concurrency gate: a
    /// `held` job whose run has a `holder_kind = 'run'` wait.
    ///
    /// Statement: `SELECT EXISTS (jobs WHERE run_id AND queue_state = 'held')
    /// AND EXISTS (concurrency_waits WHERE holder_run_id AND holder_kind =
    /// 'run')`.
    pub(super) async fn run_held(&self, run_id: RunId) -> Result<bool, ControlError> {
        let client = self.reader().await?;
        Ok(client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM jobs WHERE run_id = $1::text::uuid \
                   AND queue_state = 'held') \
                 AND EXISTS (SELECT 1 FROM concurrency_waits WHERE holder_run_id = $1::text::uuid \
                   AND holder_kind = 'run')",
                &[&run_text(run_id)],
            )
            .await
            .map_err(db)?
            .get(0))
    }

    /// Whether a runner is registered.
    ///
    /// Statement: `SELECT 1 FROM runners WHERE runner_id = $1`.
    pub(super) async fn runner_exists(&self, runner_id: i64) -> Result<bool, ControlError> {
        let client = self.reader().await?;
        Ok(client
            .query_opt("SELECT 1 FROM runners WHERE runner_id = $1", &[&runner_id])
            .await
            .map_err(db)?
            .is_some())
    }

    /// The runner registered under an OAuth client id.
    ///
    /// Statement: `SELECT runner_id FROM runners WHERE client_id = $1`.
    pub(super) async fn runner_for_client(
        &self,
        client_id: &str,
    ) -> Result<Option<i64>, ControlError> {
        let client = self.reader().await?;
        Ok(client
            .query_opt(
                "SELECT runner_id FROM runners WHERE client_id = $1",
                &[&client_id],
            )
            .await
            .map_err(db)?
            .map(|row| row.get(0)))
    }

    /// The `(run, job)` an attempt belongs to, live or archived.
    ///
    /// Statement: `SELECT run_id, job_id FROM job_requests WHERE
    /// agent_job_id UNION ALL SELECT .. FROM attempt_history WHERE
    /// agent_job_id LIMIT 1`.
    pub(super) async fn attempt_job(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<(RunId, JobId)>, ControlError> {
        let client = self.reader().await?;
        client
            .query_opt(
                "SELECT run_id::text, job_id FROM job_requests WHERE agent_job_id = $1::text::uuid \
                 UNION ALL SELECT run_id::text, job_id FROM attempt_history \
                 WHERE agent_job_id = $1::text::uuid LIMIT 1",
                &[&agent_job_id.to_string()],
            )
            .await
            .map_err(db)?
            .map(|row| Ok((codec::run_id(row.get(0))?, JobId(row.get(1)))))
            .transpose()
    }

    /// The run an attempt belongs to, live or archived.
    ///
    /// Statement: see [`Self::attempt_job`].
    pub(super) async fn run_for_attempt(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<RunId>, ControlError> {
        Ok(self.attempt_job(agent_job_id).await?.map(|(run, _)| run))
    }

    /// Repository of the live run owning an attempt; `None` when the attempt
    /// is unknown or its run archived.
    ///
    /// Statement: `SELECT r.repository FROM job_requests q JOIN runs r ..
    /// WHERE q.agent_job_id`.
    pub(super) async fn attempt_repository(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<String>, ControlError> {
        let client = self.reader().await?;
        Ok(client
            .query_opt(
                "SELECT r.repository FROM job_requests q JOIN runs r ON r.run_id = q.run_id \
                 WHERE q.agent_job_id = $1::text::uuid",
                &[&agent_job_id.to_string()],
            )
            .await
            .map_err(db)?
            .map(|row| row.get(0)))
    }

    /// Whether `agent_job_id` is an attempt of `run_id` under `plan_id`
    /// (plan id = the attempt's agent job id).
    ///
    /// Statement: `SELECT EXISTS (job_requests WHERE run_id AND agent_job_id)`.
    pub(super) async fn attempt_in_run(
        &self,
        run_id: RunId,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
    ) -> Result<bool, ControlError> {
        if plan_uuid(plan_id) != Some(agent_job_id.to_string()) {
            return Ok(false);
        }
        let client = self.reader().await?;
        Ok(client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM job_requests WHERE run_id = $1::text::uuid \
                 AND agent_job_id = $2::text::uuid)",
                &[&run_text(run_id), &agent_job_id.to_string()],
            )
            .await
            .map_err(db)?
            .get(0))
    }

    /// How a run takes part in concurrency groups. Gated: any job-level or
    /// jobset gate (a job spec with `concurrency:`, a jobset gate, or a
    /// non-run hold/wait). Any: gated, a workflow-level group on the run, or
    /// any hold/wait of the run.
    ///
    /// Statement: one `SELECT` of two `EXISTS` disjunctions.
    pub(super) async fn run_in_concurrency(
        &self,
        run_id: RunId,
    ) -> Result<RunConcurrency, ControlError> {
        let client = self.reader().await?;
        let row = client
            .query_one(
                "SELECT \
                   EXISTS (SELECT 1 FROM jobset_gates g JOIN jobsets s ON s.jobset_id = g.jobset_id \
                           WHERE s.run_id = $1::text::uuid) \
                   OR EXISTS (SELECT 1 FROM job_specs WHERE run_id = $1::text::uuid \
                              AND concurrency IS NOT NULL AND concurrency <> 'null'::jsonb) \
                   OR EXISTS (SELECT 1 FROM concurrency_holds WHERE holder_run_id = $1::text::uuid \
                              AND holder_kind <> 'run') \
                   OR EXISTS (SELECT 1 FROM concurrency_waits WHERE holder_run_id = $1::text::uuid \
                              AND holder_kind <> 'run'), \
                 EXISTS (SELECT 1 FROM runs WHERE run_id = $1::text::uuid \
                         AND concurrency_group IS NOT NULL) \
                   OR EXISTS (SELECT 1 FROM concurrency_holds WHERE holder_run_id = $1::text::uuid) \
                   OR EXISTS (SELECT 1 FROM concurrency_waits WHERE holder_run_id = $1::text::uuid)",
                &[&run_text(run_id)],
            )
            .await
            .map_err(db)?;
        let gated: bool = row.get(0);
        let any: bool = row.get(1);
        Ok(RunConcurrency::classify(gated, gated || any))
    }

    /// Resolve a runner callback to its attempt: plan id (= agent job id)
    /// first, then timeline id, then agent job id; newest attempt wins.
    ///
    /// Statement: `SELECT q.request_id, q.run_id, q.job_id, q.agent_job_id,
    /// j.status FROM job_requests q LEFT JOIN jobs j .. WHERE q.agent_job_id
    /// = $plan OR q.timeline_id = $2 OR q.agent_job_id = $3 ORDER BY
    /// (q.agent_job_id = $plan) DESC, q.request_id DESC LIMIT 1`.
    pub(super) async fn callback_job(
        &self,
        plan_id: &str,
        timeline_id: Option<uuid::Uuid>,
        agent_job_id: Option<uuid::Uuid>,
    ) -> Result<Option<CallbackJob>, ControlError> {
        let plan = plan_uuid(plan_id);
        let timeline = timeline_id.map(|id| id.to_string());
        let agent = agent_job_id.map(|id| id.to_string());
        if plan.is_none() && timeline.is_none() && agent.is_none() {
            return Ok(None);
        }
        let client = self.reader().await?;
        client
            .query_opt(
                "SELECT q.request_id, q.run_id::text, q.job_id, q.agent_job_id::text, j.status \
                 FROM job_requests q LEFT JOIN jobs j ON j.run_id = q.run_id AND j.job_id = q.job_id \
                 WHERE q.agent_job_id = $1::text::uuid OR q.timeline_id = $2::text::uuid \
                    OR q.agent_job_id = $3::text::uuid \
                 ORDER BY COALESCE(q.agent_job_id = $1::text::uuid, false) DESC, \
                          COALESCE(q.timeline_id = $2::text::uuid, false) DESC, \
                          q.request_id DESC LIMIT 1",
                &[&plan, &timeline, &agent],
            )
            .await
            .map_err(db)?
            .map(|row| {
                Ok(CallbackJob {
                    request_id: row.get(0),
                    run_id: codec::run_id(row.get(1))?,
                    job_id: JobId(row.get(2)),
                    agent_job_id: codec::uuid(row.get(3))?,
                    job_status: row.get::<_, Option<&str>>(4).map(codec::status),
                })
            })
            .transpose()
    }

    /// The one in-flight request a session still claims, when exactly one
    /// exists.
    ///
    /// The session binding counts only while its `runner_sessions` row
    /// exists (`job_requests.session_id` has no foreign key).
    ///
    /// Statement: `SELECT .. FROM job_requests q JOIN runner_sessions s ON
    /// s.session_id = q.session_id WHERE q.result IS NULL LIMIT 2`.
    pub(super) async fn sole_inflight_request(
        &self,
    ) -> Result<Option<(i64, RunId, JobId, uuid::Uuid)>, ControlError> {
        let client = self.reader().await?;
        let rows = client
            .query(
                "SELECT request_id, run_id::text, job_id, agent_job_id::text \
                 FROM job_requests q JOIN runner_sessions s \
                 ON s.session_id = q.session_id \
                 WHERE q.result IS NULL LIMIT 2",
                &[],
            )
            .await
            .map_err(db)?;
        let [row] = rows.as_slice() else {
            return Ok(None);
        };
        Ok(Some((
            row.get(0),
            codec::run_id(row.get(1))?,
            JobId(row.get(2)),
            codec::uuid(row.get(3))?,
        )))
    }

    /// Plan ids (= agent job ids) of every in-flight request.
    ///
    /// Statement: `SELECT agent_job_id FROM job_requests WHERE result IS
    /// NULL`.
    pub(super) async fn active_plan_ids(&self) -> Result<BTreeSet<String>, ControlError> {
        let client = self.reader().await?;
        Ok(client
            .query(
                "SELECT agent_job_id::text FROM job_requests WHERE result IS NULL",
                &[],
            )
            .await
            .map_err(db)?
            .iter()
            .map(|row| row.get(0))
            .collect())
    }

    /// `(queue_kind, status)` of one job, in the `QueueKind` vocabulary.
    ///
    /// Statement: `SELECT <QUEUE_KIND>, status FROM jobs WHERE run_id,
    /// job_id`.
    pub(super) async fn job_queue_state(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<(String, String)>, ControlError> {
        let client = self.reader().await?;
        Ok(client
            .query_opt(
                &format!(
                    "SELECT {QUEUE_KIND}, j.status FROM jobs j \
                     WHERE j.run_id = $1::text::uuid AND j.job_id = $2"
                ),
                &[&run_text(run_id), &job_id.0],
            )
            .await
            .map_err(db)?
            .map(|row| (row.get(0), row.get(1))))
    }

    /// Every terminal logical job, live or archived.
    ///
    /// Statement: `SELECT run_id, job_id FROM jobs WHERE status IN
    /// <terminal> UNION ALL SELECT .. FROM job_history WHERE status IN
    /// <terminal>`.
    pub(super) async fn terminal_jobs(&self) -> Result<BTreeSet<(RunId, JobId)>, ControlError> {
        let client = self.reader().await?;
        client
            .query(
                &format!(
                    "SELECT run_id::text, job_id FROM jobs WHERE status IN {TERMINAL_STATUSES} \
                     UNION ALL SELECT run_id::text, job_id FROM job_history \
                     WHERE status IN {TERMINAL_STATUSES}"
                ),
                &[],
            )
            .await
            .map_err(db)?
            .iter()
            .map(|row| Ok((codec::run_id(row.get(0))?, JobId(row.get(1)))))
            .collect()
    }

    /// Queue pressure snapshot. Buckets: `ready`,
    /// `claimed`; `pending` = `blocked` (needs / max-parallel); `blocked` =
    /// `held` jobs waiting on a job/jobset gate; `held` = `held` jobs of runs
    /// waiting on a workflow-level gate; `expanding` = `pending_expansion`
    /// only (leased `expanding` nodes excluded, like the old unleased-only
    /// count). `next_runs_on` = the ready-queue front's labels.
    ///
    /// Statements: one grouped count over `jobs` joined to the run's
    /// workflow-level wait; `SELECT runs_on FROM jobs WHERE queue_state =
    /// 'ready' ORDER BY priority DESC, run_order, job_order, run_id, job_id
    /// LIMIT 1` (the `jobs_ready_global` partial index feeds the order).
    pub(super) async fn queue_stats(&self) -> Result<QueueStats, ControlError> {
        let client = self.reader().await?;
        let mut stats = QueueStats::default();
        for row in client
            .query(
                "SELECT CASE WHEN j.queue_state = 'held' AND EXISTS \
                          (SELECT 1 FROM concurrency_waits w WHERE w.holder_run_id = j.run_id \
                           AND w.holder_kind = 'run') THEN 'held_run' \
                        ELSE j.queue_state END AS bucket, count(*) \
                 FROM jobs j WHERE j.queue_state <> 'none' GROUP BY bucket",
                &[],
            )
            .await
            .map_err(db)?
        {
            let count = row.get::<_, i64>(1).max(0) as usize;
            match row.get::<_, &str>(0) {
                "ready" => stats.ready = count,
                "blocked" => stats.pending = count,
                "held" => stats.blocked = count,
                "held_run" => stats.held = count,
                "claimed" => stats.claimed = count,
                "pending_expansion" => stats.expanding = count,
                _ => {}
            }
        }
        stats.next_runs_on = match client
            .query_opt(
                "SELECT runs_on::text FROM jobs WHERE queue_state = 'ready' \
                 ORDER BY priority DESC, run_order, job_order, run_id, job_id LIMIT 1",
                &[],
            )
            .await
            .map_err(db)?
        {
            // A stored `runs_on` that is not a JSON string list yields no
            // labels: the gauge must not fail on data.
            Some(row) => codec::from_json::<Vec<String>>(row.get(0)).unwrap_or_default(),
            None => Vec::new(),
        };
        Ok(stats)
    }

    /// Owned, session-bound, in-flight attempts per runner (status page).
    /// The session binding counts only while its `runner_sessions` row exists
    /// (`job_requests.session_id` has no foreign key).
    ///
    /// Statement: `SELECT q.runner_id, q.run_id, q.job_id, q.started_at FROM
    /// job_requests q JOIN runner_sessions s ON s.session_id = q.session_id
    /// WHERE q.result IS NULL AND q.runner_id IS NOT NULL ORDER BY
    /// q.runner_id`.
    pub(super) async fn live_assignments(
        &self,
    ) -> Result<Vec<preloop_observability::status::RunnerAssignment>, ControlError> {
        let client = self.reader().await?;
        let now = std::time::SystemTime::now();
        Ok(client
            .query(
                concat!(
                    "SELECT q.runner_id, q.run_id::text, q.job_id, ",
                    us!("q.started_at"),
                    " FROM job_requests q JOIN runner_sessions s \
                     ON s.session_id = q.session_id \
                     WHERE q.result IS NULL AND q.runner_id IS NOT NULL \
                     ORDER BY q.runner_id, q.request_id"
                ),
                &[],
            )
            .await
            .map_err(db)?
            .iter()
            .map(|row| preloop_observability::status::RunnerAssignment {
                runner_id: row.get(0),
                run_id: row.get(1),
                job_id: row.get(2),
                assigned_seconds_ago: row
                    .get::<_, Option<i64>>(3)
                    .and_then(|us| now.duration_since(codec::us_to_system(us)).ok())
                    .map(|age| age.as_secs_f64())
                    .unwrap_or(0.0),
            })
            .collect())
    }

    /// Runs matching the list filter, active before terminal and newest
    /// first inside each group (live and archived interleaved). Job rows
    /// carry (status, queue kind, latest attempt) so `project_run_rows`
    /// rebuilds the same `jobs`/`jobs_list` shape the single-run read
    /// serves; archived jobs project `none` queue kind and lose caller
    /// metadata; the archived projection simply carries less of it.
    ///
    /// Statements: one `UNION ALL` selection over `runs`/`run_history`;
    /// per run the `load_graph` or the archived `run_history` +
    /// `job_history`/`attempt_history` reads; one batched `job_steps` ∪
    /// `step_history` read for the latest attempts' step manifests.
    pub(super) async fn list_runs(
        &self,
        filter: crate::control::backend::RunListFilter,
    ) -> Result<Vec<crate::models::RunRecord>, ControlError> {
        let limit = filter.limit.min(200) as i64;
        let mut client = self.reader().await?;
        let tx = client.transaction().await.map_err(db)?;
        // The selected runs: live and archived, terminal_rank ordering
        // (`active before newer terminal runs` parity). `status` filters on
        // the API word: live `status` except a workflow-gated run reads
        // 'pending'; archived reads the conclusion ('completed' matches any).
        let selected = tx
            .query(
                concat!(
                    "SELECT run_id::text, archived, has_run_wait, terminal_rank, sort_at FROM (\
                       SELECT r.run_id, false AS archived, \
                         EXISTS (SELECT 1 FROM concurrency_waits w \
                                 WHERE w.holder_run_id = r.run_id AND w.holder_kind = 'run') \
                           AS has_run_wait, \
                         CASE WHEN r.status = 'completed' THEN 1 ELSE 0 END AS terminal_rank, \
                         COALESCE(",
                    us!("r.completed_at"),
                    ", ",
                    us!("r.started_at"),
                    ", ",
                    us!("r.created_at"),
                    ") AS sort_at, \
                         r.workflow_path, r.event, \
                         CASE WHEN r.status = 'completed' THEN COALESCE(r.conclusion,'success') \
                              WHEN EXISTS (SELECT 1 FROM concurrency_waits w \
                                           WHERE w.holder_run_id = r.run_id \
                                             AND w.holder_kind = 'run') THEN 'pending' \
                              ELSE r.status END AS status \
                       FROM runs r \
                       UNION ALL \
                       SELECT h.run_id, true, false, 1, COALESCE(",
                    us!("h.completed_at"),
                    ", ",
                    us!("h.started_at"),
                    ", ",
                    us!("h.created_at"),
                    "), h.workflow_path, h.event, \
                         COALESCE(h.conclusion,'success') \
                       FROM run_history h\
                     ) s \
                     WHERE ($1::text IS NULL OR position($1 in s.workflow_path) > 0) \
                       AND ($2::text IS NULL OR s.status = $2 OR \
                            ($2 = 'completed' AND s.terminal_rank = 1)) \
                       AND ($3::text IS NULL OR s.event = $3) \
                     ORDER BY s.terminal_rank, s.sort_at DESC, s.run_id LIMIT $4"
                ),
                &[&filter.workflow, &filter.status, &filter.event, &limit],
            )
            .await
            .map_err(db)?;
        let mut runs = Vec::with_capacity(selected.len());
        for row in &selected {
            let run_id = codec::run_id(row.get::<_, &str>(0))?;
            let archived: bool = row.get(1);
            let run_wait: bool = row.get(2);
            let (record, mut jobs) = if archived {
                (
                    archived_record_tx(&tx, run_id).await?,
                    archived_job_rows(&tx, run_id).await?,
                )
            } else {
                match PgBackend::load_graph(self, &tx, run_id).await? {
                    Some(graph) => (graph.record, live_job_rows(&tx, run_id).await?),
                    None => {
                        // Archived between select and read: fall back.
                        (
                            archived_record_tx(&tx, run_id).await?,
                            archived_job_rows(&tx, run_id).await?,
                        )
                    }
                }
            };
            hydrate_steps(&tx, &mut jobs).await?;
            let jobs: Vec<(JobId, ExecutionStatus, String, Option<Vec<StepRecord>>)> = jobs
                .into_iter()
                .map(|(job_id, status, kind, steps, _)| (job_id, status, kind, steps))
                .collect();
            let mut projected = crate::control::backend::project_run_rows(record, jobs);
            // `record.status` is the ExecutionStatus word; a run parked on
            // its workflow-level gate reads `Pending` (submit's `held`).
            if run_wait && projected.status == ExecutionStatus::Queued {
                projected.status = ExecutionStatus::Pending;
            }
            runs.push(projected);
        }
        tx.commit().await.map_err(db)?;
        Ok(runs)
    }

    /// Every run (live or archived) of `repository`, case-insensitive —
    /// `runs_for_repository` parity (map-order by run id).
    ///
    /// Statements: `SELECT run_id FROM runs UNION ALL run_history WHERE
    /// lower(repository) = lower($1)`; per run the `load_graph` or archived
    /// decode.
    pub(super) async fn runs_for_repository(
        &self,
        repository: &str,
    ) -> Result<Vec<crate::models::RunRecord>, ControlError> {
        let mut client = self.reader().await?;
        let tx = client.transaction().await.map_err(db)?;
        let rows = tx
            .query(
                "SELECT run_id::text, false FROM runs \
                 WHERE lower(repository) = lower($1) \
                 UNION ALL SELECT run_id::text, true FROM run_history \
                 WHERE lower(repository) = lower($1) \
                 ORDER BY run_id",
                &[&repository],
            )
            .await
            .map_err(db)?;
        let mut runs = Vec::with_capacity(rows.len());
        for row in rows {
            let run_id = codec::run_id(row.get::<_, &str>(0))?;
            if row.get::<_, bool>(1) {
                runs.push(archived_record_tx(&tx, run_id).await?);
            } else if let Some(graph) = PgBackend::load_graph(self, &tx, run_id).await? {
                runs.push(graph.record);
            } else {
                runs.push(archived_record_tx(&tx, run_id).await?);
            }
        }
        tx.commit().await.map_err(db)?;
        Ok(runs)
    }
}

// ─────────────────────────────────────────────────────────────────────────
// List-run assembly
// ─────────────────────────────────────────────────────────────────────────

/// One `project_run_rows` input plus the job's latest attempt (step-manifest
/// key), before the batch step read resolves it.
type JobProjection = (
    JobId,
    ExecutionStatus,
    String,
    Option<Vec<StepRecord>>,
    Option<String>,
);

/// The run's live job rows as `(job_id, status, queue_state, latest
/// agent_job_id)`. An expanded matrix parent does not appear — it left
/// `run.jobs` when its legs registered.
///
/// Statement: `SELECT .. FROM jobs WHERE run_id AND NOT (matrix_parent with
/// children) ORDER BY job_id`.
async fn live_job_rows(
    tx: &tokio_postgres::Transaction<'_>,
    run_id: RunId,
) -> Result<Vec<JobProjection>, ControlError> {
    tx.query(
        "SELECT j.job_id, j.status, j.queue_state, \
         (SELECT q.agent_job_id::text FROM job_requests q \
          WHERE q.run_id = j.run_id AND q.job_id = j.job_id \
          ORDER BY q.request_id DESC LIMIT 1) \
         FROM jobs j WHERE j.run_id = $1::text::uuid \
         AND NOT (j.kind = 'matrix_parent' AND EXISTS (\
              SELECT 1 FROM jobs c WHERE c.run_id = j.run_id \
              AND c.parent_job_id = j.job_id)) \
         ORDER BY j.job_id",
        &[&run_text(run_id)],
    )
    .await
    .map_err(db)?
    .iter()
    .map(|row| {
        Ok((
            JobId(row.get::<_, String>(0)),
            codec::status(row.get(1)),
            row.get::<_, String>(2),
            None,
            row.get::<_, Option<String>>(3),
        ))
    })
    .collect()
}

/// The archived run's job rows from the latest archive snapshot
/// (`run_created_at` = its `run_history.created_at`). Expanded parents
/// (matrix or caller) are excluded by their surviving children — the
/// caller's `inner_job_ids` are not archived, so children presence is the
/// only expansion signal.
///
/// Statement: `SELECT .. FROM job_history WHERE run_id AND run_created_at =
/// latest ORDER BY job_id`.
async fn archived_job_rows(
    tx: &tokio_postgres::Transaction<'_>,
    run_id: RunId,
) -> Result<Vec<JobProjection>, ControlError> {
    tx.query(
        "SELECT j.job_id, j.status, 'none', \
         (SELECT a.agent_job_id::text FROM attempt_history a \
          WHERE a.run_id = j.run_id AND a.run_created_at = j.run_created_at \
            AND a.job_id = j.job_id ORDER BY a.request_id DESC LIMIT 1) \
         FROM job_history j WHERE j.run_id = $1::text::uuid \
         AND j.run_created_at = (SELECT max(created_at) FROM run_history \
                                 WHERE run_id = $1::text::uuid) \
         AND NOT (j.kind IN ('matrix_parent','reusable_caller') AND EXISTS (\
              SELECT 1 FROM job_history c WHERE c.run_id = j.run_id \
              AND c.run_created_at = j.run_created_at \
              AND c.parent_job_id = j.job_id)) \
         ORDER BY j.job_id",
        &[&run_text(run_id)],
    )
    .await
    .map_err(db)?
    .iter()
    .map(|row| {
        Ok((
            JobId(row.get::<_, String>(0)),
            codec::status(row.get(1)),
            row.get::<_, String>(2),
            None,
            row.get::<_, Option<String>>(3),
        ))
    })
    .collect()
}

/// Fill the step slot of each job row from the latest attempt's manifest —
/// `job_steps` ∪ `step_history` in one batched read.
async fn hydrate_steps(
    tx: &tokio_postgres::Transaction<'_>,
    jobs: &mut [JobProjection],
) -> Result<(), ControlError> {
    let attempts: Vec<String> = jobs.iter().filter_map(|job| job.4.clone()).collect();
    if attempts.is_empty() {
        return Ok(());
    }
    let mut steps: BTreeMap<uuid::Uuid, Vec<StepRecord>> = BTreeMap::new();
    for chunk in attempts.chunks(500) {
        let key_list: Vec<String> = chunk.to_vec();
        for row in tx
            .query(
                &format!(
                    // PostgreSQL names the cast expressions in the derived
                    // table (`int8`, not `started_at`), so re-selecting
                    // `{STEP_COLUMNS}` from it fails; project it wholesale.
                    "SELECT s.* FROM (\
                       SELECT {STEP_COLUMNS}, position FROM job_steps \
                       WHERE agent_job_id = ANY($1::text[]::uuid[]) \
                       UNION ALL \
                       SELECT {STEP_COLUMNS}, position FROM step_history \
                       WHERE agent_job_id = ANY($1::text[]::uuid[])) s \
                     ORDER BY s.agent_job_id, s.position"
                ),
                &[&key_list],
            )
            .await
            .map_err(db)?
        {
            let (agent, step) = step_from_row(&row)?;
            steps.entry(agent).or_default().push(step);
        }
    }
    for job in jobs.iter_mut() {
        if let Some(agent) = &job.4
            && let Ok(agent) = uuid::Uuid::parse_str(agent)
        {
            job.3 = steps.remove(&agent);
        }
    }
    Ok(())
}

/// The archived `RunRecord`: `run_history` head row plus its latest
/// `job_history` snapshot as `jobs`/`job_names`/`jobs_list` (the blob
/// fields the archive keeps are narrower — outputs, caller plans, needs
/// and gate state are gone with the live row).
///
/// Statements: `SELECT .. FROM run_history WHERE run_id ORDER BY created_at
/// DESC LIMIT 1`; `SELECT job_id, status, display_name FROM job_history`
/// of that snapshot.
pub(super) async fn archived_record_tx(
    tx: &tokio_postgres::Transaction<'_>,
    run_id: RunId,
) -> Result<crate::models::RunRecord, ControlError> {
    let run = run_text(run_id);
    let row = tx
        .query_opt(
            concat!(
                "SELECT namespace_id, repository, workflow_path, run_number, \
                 run_attempt, run_name, event, conclusion, head_sha, \
                 submission::text, ",
                us!("created_at"),
                ", ",
                us!("started_at"),
                ", ",
                us!("completed_at"),
                ", fork_approval_pending, fork_approval_requested_at, \
                 fork_approval_approved_at, fork_approval_note, \
                 reports_check_runs, record_details::text \
                 FROM run_history WHERE run_id = $1::text::uuid \
                 ORDER BY created_at DESC LIMIT 1"
            ),
            &[&run],
        )
        .await
        .map_err(db)?
        .ok_or_else(|| ControlError::NotFound(format!("run {run_id}")))?;
    let submission: crate::WorkflowSubmission =
        serde_json::from_str(row.get::<_, String>(9).as_str()).map_err(ControlError::backend)?;
    let status = match row.get::<_, Option<&str>>(7) {
        Some("success") => ExecutionStatus::Success,
        Some("failure") => ExecutionStatus::Failure,
        Some("cancelled") => ExecutionStatus::Cancelled,
        Some("skipped") => ExecutionStatus::Skipped,
        _ => ExecutionStatus::Failure,
    };
    let created_at = codec::us_to_system(row.get::<_, Option<i64>>(10).unwrap_or(0));
    let jobs_rows = tx
        .query(
            "SELECT job_id, status, display_name, check_run_id FROM job_history \
             WHERE run_id = $1::text::uuid \
             AND run_created_at = (SELECT max(created_at) FROM run_history \
                                   WHERE run_id = $1::text::uuid) \
             AND NOT (kind IN ('matrix_parent','reusable_caller') AND EXISTS (\
                  SELECT 1 FROM job_history c WHERE c.run_id = $1::text::uuid \
                  AND c.run_created_at = job_history.run_created_at \
                  AND c.parent_job_id = job_history.job_id)) \
             ORDER BY job_id",
            &[&run],
        )
        .await
        .map_err(db)?;
    let mut jobs = BTreeMap::new();
    let mut job_names = BTreeMap::new();
    let mut jobs_list = Vec::new();
    let mut job_check_run_ids: BTreeMap<JobId, u64> = BTreeMap::new();
    for job_row in &jobs_rows {
        let job_id = JobId(job_row.get::<_, String>(0));
        let status = codec::status(job_row.get::<_, &str>(1));
        let name: String = job_row.get(2);
        jobs.insert(job_id.clone(), status);
        job_names.insert(job_id.clone(), name.clone());
        if let Some(id) = job_row.get::<_, Option<i64>>(3) {
            job_check_run_ids.insert(job_id.clone(), id as u64);
        }
        jobs_list.push(JobDetail {
            job_id: job_id.0.clone(),
            name,
            conclusion: crate::status_string(status),
            steps: Vec::new(),
            annotations: Vec::new(),
        });
    }
    // Ids minted for jobs that never materialized live in the archived
    // `record_details`; history rows overlay them.
    if let Ok(details) = serde_json::from_str::<serde_json::Value>(
        row.get::<_, Option<String>>(18).as_deref().unwrap_or("{}"),
    ) && let Some(map) = details.get("job_check_run_ids")
        && let Ok(stored) = serde_json::from_value::<BTreeMap<JobId, u64>>(map.clone())
    {
        for (job_id, id) in stored {
            job_check_run_ids.entry(job_id).or_insert(id);
        }
    }
    Ok(crate::models::RunRecord {
        run_id,
        webhook_delivery_id: None,
        run_name: row.get(5),
        submission: std::sync::Arc::new(submission),
        jobs,
        status,
        job_outputs: BTreeMap::new(),
        job_base_ids: BTreeMap::new(),
        job_needs: BTreeMap::new(),
        caller_plans: BTreeMap::new(),
        job_names,
        github: serde_json::Value::Null,
        head_sha: row.get(8),
        workflow_ref: String::new(),
        workspace_snapshot: None,
        job_fail_fast: BTreeMap::new(),
        job_continue_on_error: BTreeMap::new(),
        job_check_run_ids,
        reusable_calls: BTreeMap::new(),
        jobs_list,
        created_at: created_at.into(),
        started_at: row
            .get::<_, Option<i64>>(11)
            .map(|us| codec::us_to_system(us).into()),
        completed_at: row
            .get::<_, Option<i64>>(12)
            .map(|us| codec::us_to_system(us).into()),
        run_number: row.get::<_, i64>(3).max(0) as u64,
        run_attempt: row.get::<_, i32>(4).max(0) as u64,
        workflow_path_str: row.get(2),
        event: row.get(6),
        conclusion: row.get(7),
        push_state: None,
        snapshot_timing: None,
        fork_approval_pending: row.get::<_, Option<bool>>(13).unwrap_or(false),
        fork_approval_requested_at_unix_nanos: row.get(14),
        fork_approved_at_unix_nanos: row.get(15),
        fork_approval_note: row.get(16),
        reports_check_runs: row.get::<_, Option<bool>>(17).unwrap_or(false),
    })
}
