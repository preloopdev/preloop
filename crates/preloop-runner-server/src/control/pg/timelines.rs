//! Runner timelines (`timelines`, `timeline_records`), per-attempt step
//! manifests (`job_steps`) and per-plan log ids (`log_files`).
//!
//! A timeline belongs to one execution attempt (`timelines.timeline_id`
//! references `job_requests.timeline_id`). Callers address it by the
//! protocol key `{plan_id}/{timeline_id}`; the uuid after the last `/` is
//! the row key.

use super::codec::{self, ts, us};
use super::{PgBackend, db};
use crate::control::types::{
    ControlError, MAX_TIMELINE_RECORDS, StepPatch, StepReportOutcome, StoredStep, merge_step_patch,
    step_report, timeline_record_bytes,
};
use crate::memory_caps::MAX_TIMELINE_BYTES_PER_TIMELINE;
use crate::models::{StepKind, StepRecord};
use preloop_gha_protocol::azdo::TimelineRecord;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use std::collections::BTreeMap;

/// Bounded retries for the per-plan log id race (two appenders computing the
/// same `MAX(log_id) + 1`; the unique index lets exactly one win).
const LOG_ID_ATTEMPTS: usize = 8;

/// The timeline row key inside a `{plan_id}/{timeline_id}` protocol key.
fn timeline_uuid(timeline_key: &str) -> Option<String> {
    timeline_key
        .rsplit('/')
        .next()
        .and_then(|id| id.parse::<uuid::Uuid>().ok())
        .map(|id| id.to_string())
}

/// Columns every step read selects, in [`step_from_row`] order (`job_steps`
/// and `step_history` share them).
pub(super) const STEP_COLUMNS: &str = concat!(
    "agent_job_id::text, step_id, kind, workflow_index, runner_number, context_name, name, \
     conclusion, ",
    us!("started_at"),
    ", ",
    us!("finished_at")
);

/// Decode one row selected with [`STEP_COLUMNS`].
pub(super) fn step_from_row(
    row: &tokio_postgres::Row,
) -> Result<(uuid::Uuid, StepRecord), ControlError> {
    let agent = codec::uuid(row.get(0))?;
    let kind = match row.get::<_, &str>(2) {
        "workflow" => StepKind::Workflow,
        _ => StepKind::Synthetic,
    };
    Ok((
        agent,
        StepRecord {
            id: row.get(1),
            kind,
            workflow_index: row.get::<_, Option<i32>>(3).map(|i| i.max(0) as usize),
            runner_number: row.get::<_, Option<i32>>(4).map(|n| n.max(0) as u32),
            context_name: row.get(5),
            name: row.get(6),
            conclusion: row.get(7),
            started_at: row.get::<_, Option<i64>>(8).and_then(codec::us_to_chrono),
            finished_at: row.get::<_, Option<i64>>(9).and_then(codec::us_to_chrono),
        },
    ))
}

fn decode_records(rows: &[tokio_postgres::Row]) -> Vec<TimelineRecord> {
    rows.iter()
        .filter_map(|row| serde_json::from_str(row.get::<_, &str>(0)).ok())
        .collect()
}

impl PgBackend {
    /// Apply one timeline PATCH: bump the change counter, stamp and upsert
    /// the patched records, evict the lowest record ids past
    /// [`MAX_TIMELINE_RECORDS`] (protecting the patched ones), and return
    /// the new change id and every stored record ordered by record id.
    /// `NotFound` when no attempt owns the timeline.
    ///
    /// Statements (one transaction): `INSERT INTO timelines .. SELECT ..
    /// WHERE EXISTS (job_requests.timeline_id) ON CONFLICT DO UPDATE SET
    /// change_id = change_id + 1 RETURNING change_id` (the row lock
    /// serializes PATCHes of one timeline); `INSERT INTO timeline_records ..
    /// SELECT FROM unnest(..) ON CONFLICT DO UPDATE`; the cap `DELETE`;
    /// `SELECT record FROM timeline_records ORDER BY record_id LIMIT n`.
    pub(super) async fn patch_timeline(
        &self,
        timeline_key: &str,
        mut records: Vec<TimelineRecord>,
        events: &[preloop_gha_protocol::NdjsonEvent],
    ) -> Result<(i32, Vec<TimelineRecord>), ControlError> {
        let not_found = || ControlError::NotFound(format!("timeline {timeline_key} not found"));
        let timeline = timeline_uuid(timeline_key).ok_or_else(not_found)?;
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let change_id: i32 = tx
            .query_opt(
                "INSERT INTO timelines (timeline_id, change_id) \
                 SELECT $1::text::uuid, 1 WHERE EXISTS \
                   (SELECT 1 FROM job_requests WHERE timeline_id = $1::text::uuid) \
                 ON CONFLICT (timeline_id) DO UPDATE SET change_id = timelines.change_id + 1 \
                 RETURNING change_id",
                &[&timeline],
            )
            .await
            .map_err(db)?
            .ok_or_else(not_found)?
            .get(0);
        let stamped = crate::control::types::stamp_timeline_records(
            &mut records,
            i64::from(change_id),
            std::time::SystemTime::now(),
        );
        let (ids, bodies): (Vec<String>, Vec<String>) = stamped.into_iter().unzip();
        if !ids.is_empty() {
            tx.execute(
                "INSERT INTO timeline_records (timeline_id, record_id, change_id, record) \
                 SELECT $1::text::uuid, r.id::uuid, $2, r.body::jsonb \
                 FROM unnest($3::text[], $4::text[]) AS r(id, body) \
                 ON CONFLICT (timeline_id, record_id) \
                 DO UPDATE SET change_id = EXCLUDED.change_id, record = EXCLUDED.record",
                &[&timeline, &change_id, &ids, &bodies],
            )
            .await
            .map_err(db)?;
        }
        // Write-side bound: the read LIMIT alone let `timeline_records`
        // grow without limit and could omit a record this PATCH wrote from
        // the response. Evict the lowest record ids past the cap, never a
        // record stamped with this PATCH's `change_id`; a PATCH larger than
        // the cap falls through to evicting the lowest regardless so the
        // bound always holds.
        let cap = MAX_TIMELINE_RECORDS as i64;
        let count: i64 = tx
            .query_one(
                "SELECT count(*) FROM timeline_records WHERE timeline_id = $1::text::uuid",
                &[&timeline],
            )
            .await
            .map_err(db)?
            .get(0);
        if count > cap {
            let excess = count - cap;
            tx.execute(
                "DELETE FROM timeline_records WHERE timeline_id = $1::text::uuid \
                   AND change_id <> $2 AND record_id IN \
                   (SELECT record_id FROM timeline_records \
                    WHERE timeline_id = $1::text::uuid AND change_id <> $2 \
                    ORDER BY record_id LIMIT $3)",
                &[&timeline, &change_id, &excess],
            )
            .await
            .map_err(db)?;
            let count: i64 = tx
                .query_one(
                    "SELECT count(*) FROM timeline_records WHERE timeline_id = $1::text::uuid",
                    &[&timeline],
                )
                .await
                .map_err(db)?
                .get(0);
            if count > cap {
                tx.execute(
                    "DELETE FROM timeline_records WHERE timeline_id = $1::text::uuid \
                       AND record_id IN \
                       (SELECT record_id FROM timeline_records \
                        WHERE timeline_id = $1::text::uuid \
                        ORDER BY record_id LIMIT $2)",
                    &[&timeline, &(count - cap)],
                )
                .await
                .map_err(db)?;
            }
        }
        let rows = tx
            .query(
                "SELECT record_id::text, change_id, record::text \
                 FROM timeline_records WHERE timeline_id = $1::text::uuid \
                 ORDER BY record_id",
                &[&timeline],
            )
            .await
            .map_err(db)?;
        let mut sized = Vec::with_capacity(rows.len());
        for row in rows {
            let record: TimelineRecord =
                serde_json::from_str(row.get(2)).map_err(ControlError::backend)?;
            sized.push((
                row.get::<_, String>(0),
                row.get::<_, i32>(1),
                timeline_record_bytes(&record),
            ));
        }
        let mut total_bytes: usize = sized.iter().map(|(_, _, size)| *size).sum();
        while total_bytes > MAX_TIMELINE_BYTES_PER_TIMELINE && sized.len() > 1 {
            let index = sized
                .iter()
                .position(|(_, row_change, _)| *row_change != change_id)
                .unwrap_or(0);
            let (record_id, _, size) = sized.remove(index);
            tx.execute(
                "DELETE FROM timeline_records WHERE timeline_id = $1::text::uuid \
                 AND record_id = $2::text::uuid",
                &[&timeline, &record_id],
            )
            .await
            .map_err(db)?;
            total_bytes = total_bytes.saturating_sub(size);
        }
        let stored = tx
            .query(
                "SELECT record::text FROM timeline_records WHERE timeline_id = $1::text::uuid \
                 ORDER BY record_id LIMIT $2",
                &[&timeline, &(MAX_TIMELINE_RECORDS as i64)],
            )
            .await
            .map_err(db)?;
        // Annotation events projected from this PATCH persist atomically
        // with the records; the handler broadcasts them via `emit_persisted`.
        for event in events {
            super::dispatch::append_event_tx(&tx, event).await?;
        }
        tx.commit().await.map_err(db)?;
        Ok((change_id, decode_records(&stored)))
    }

    /// A timeline's change id and a `skip`/`top` page of its records
    /// ordered by record id. An unknown timeline reads as change 0, empty.
    ///
    /// One repeatable-read, read-only transaction on one reader: the counter
    /// and the records it describes must come from one snapshot (READ
    /// COMMITTED would let a concurrent PATCH commit between them and stamp
    /// records with the next change id).
    ///
    /// Statements: `SELECT change_id FROM timelines WHERE timeline_id`;
    /// `SELECT record FROM timeline_records .. ORDER BY record_id OFFSET
    /// LIMIT`.
    pub(super) async fn get_timeline(
        &self,
        timeline_key: &str,
        skip: usize,
        top: usize,
    ) -> Result<(i32, Vec<TimelineRecord>), ControlError> {
        let Some(timeline) = timeline_uuid(timeline_key) else {
            return Ok((0, Vec::new()));
        };
        let mut client = self.reader().await?;
        let tx = client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .map_err(db)?;
        let change_id: i32 = tx
            .query_opt(
                "SELECT change_id FROM timelines WHERE timeline_id = $1::text::uuid",
                &[&timeline],
            )
            .await
            .map_err(db)?
            .map(|row| row.get(0))
            .unwrap_or(0);
        let rows = tx
            .query(
                "SELECT record::text FROM timeline_records WHERE timeline_id = $1::text::uuid \
                 ORDER BY record_id OFFSET $2 LIMIT $3",
                &[
                    &timeline,
                    &codec::limit(skip),
                    &codec::limit(top.min(MAX_TIMELINE_RECORDS)),
                ],
            )
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok((change_id, decode_records(&rows)))
    }

    /// Drop the timelines of attempts that settled before `before_us`
    /// (records cascade). Returns timelines removed.
    ///
    /// Statement: `DELETE FROM timelines t USING job_requests q WHERE
    /// q.timeline_id = t.timeline_id AND q.result IS NOT NULL AND
    /// q.finished_at < $1`.
    pub(super) async fn prune_timelines(&self, before_us: i64) -> Result<u64, ControlError> {
        let client = self.writer().await?;
        client
            .execute(
                concat!(
                    "DELETE FROM timelines t USING job_requests q \
                     WHERE q.timeline_id = t.timeline_id AND q.result IS NOT NULL \
                     AND q.finished_at < ",
                    ts!("$1")
                ),
                &[&before_us],
            )
            .await
            .map_err(db)
    }

    /// Upsert runner-reported steps for one attempt. Each patch merges into
    /// the stored row through [`merge_step_patch`] — the terminal guard and
    /// the "fill in only when missing" timing rules live there — and a new
    /// step is appended as `synthetic` after the current last position. An
    /// unknown attempt writes nothing.
    ///
    /// Every row that actually changed also appends its `StepStatus` event to
    /// the outbox in this same transaction, so the event is in the run's
    /// `events.ndjson` snapshot the moment the row is readable; the caller
    /// only broadcasts.
    ///
    /// Statements (one transaction): `SELECT 1 FROM job_requests WHERE
    /// agent_job_id = $1 FOR NO KEY UPDATE` (serializes position allocation
    /// per attempt); per patch one row read plus `UPDATE job_steps ..` or
    /// `INSERT INTO job_steps .. SELECT COALESCE(MAX(position) + 1, 0)`.
    pub(super) async fn patch_steps(
        &self,
        agent_job_id: uuid::Uuid,
        patches: Vec<StepPatch>,
    ) -> Result<Vec<preloop_gha_protocol::NdjsonEvent>, ControlError> {
        if patches.is_empty() {
            return Ok(Vec::new());
        }
        let agent = agent_job_id.to_string();
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let Some((run_id, job_id)) = step_attempt(&tx, &agent).await? else {
            return Ok(Vec::new());
        };
        let mut events = Vec::new();
        for patch in &patches {
            let current = stored_step(&tx, &agent, &patch.id).await?;
            let Some(merged) = merge_step_patch(current.as_ref(), patch) else {
                continue;
            };
            write_step(&tx, &agent, current.is_some(), &merged.row).await?;
            if merged.changed {
                let event = merged.row.event(run_id, &job_id);
                super::dispatch::append_event_tx(&tx, &event).await?;
                events.push(event);
            }
        }
        tx.commit().await.map_err(db)?;
        Ok(events)
    }

    /// Reconcile a `WorkflowStepsUpdate` report into `job_steps`. The
    /// attempt resolves by `plan_id` (= its `agent_job_id`) first, then by
    /// `agent_job_id`; `matched: false` when neither matches. Each reported
    /// step merges into its row (an empty name or absent number keeps the
    /// stored value; the wire's own `started_at`/`completed_at` win while the
    /// step is unfinished); an unknown step is appended as synthetic. Changed
    /// rows append their `StepStatus` event to the outbox in the same
    /// transaction.
    ///
    /// Statements (one transaction): `SELECT q.agent_job_id, j.status =
    /// 'cancelled' FROM job_requests q JOIN jobs j .. WHERE q.agent_job_id =
    /// $1 FOR NO KEY UPDATE OF q`; per step one row read plus `UPDATE
    /// job_steps ..` or `INSERT INTO job_steps .. SELECT MAX(position) + 1`.
    pub(super) async fn report_steps(
        &self,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
        steps: Vec<serde_json::Value>,
    ) -> Result<StepReportOutcome, ControlError> {
        if steps.is_empty() {
            return Ok(StepReportOutcome {
                matched: true,
                events: Vec::new(),
            });
        }
        let mut candidates = Vec::with_capacity(2);
        if let Ok(plan) = plan_id.parse::<uuid::Uuid>() {
            candidates.push(plan.to_string());
        }
        candidates.push(agent_job_id.to_string());
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let mut resolved = None;
        for candidate in &candidates {
            if let Some(row) = tx
                .query_opt(
                    "SELECT q.agent_job_id::text, q.run_id::text, q.job_id, \
                     j.status = 'cancelled' \
                     FROM job_requests q JOIN jobs j ON j.run_id = q.run_id \
                       AND j.job_id = q.job_id \
                     WHERE q.agent_job_id = $1::text::uuid FOR NO KEY UPDATE OF q",
                    &[candidate],
                )
                .await
                .map_err(db)?
            {
                resolved = Some((
                    row.get::<_, String>(0),
                    codec::run_id(row.get::<_, &str>(1))?,
                    JobId(row.get::<_, String>(2)),
                    row.get::<_, bool>(3),
                ));
                break;
            }
        }
        let Some((agent, run_id, job_id, job_cancelled)) = resolved else {
            return Ok(StepReportOutcome::default());
        };
        let observed = chrono::Utc::now();
        let mut events = Vec::new();
        for step in &steps {
            let Some(report) = step_report(step, job_cancelled, observed) else {
                tracing::warn!(agent_job_id = %agent, "dropping step report with no external_id");
                continue;
            };
            let current = stored_step(&tx, &agent, &report.id).await?;
            let Some(merged) = merge_step_patch(current.as_ref(), &report) else {
                continue;
            };
            write_step(&tx, &agent, current.is_some(), &merged.row).await?;
            if merged.changed {
                let event = merged.row.event(run_id, &job_id);
                super::dispatch::append_event_tx(&tx, &event).await?;
                events.push(event);
            }
        }
        tx.commit().await.map_err(db)?;
        Ok(StepReportOutcome {
            matched: true,
            events,
        })
    }

    /// A run's stored step manifests keyed by attempt, each in position
    /// order: live attempts from `job_steps`, archived ones from
    /// `step_history` (the live rows go with the run).
    ///
    /// Statement: `SELECT <step columns>, position FROM (job_steps ⋈
    /// job_requests of the run UNION ALL step_history of the run) ORDER BY
    /// agent_job_id, position`.
    pub(super) async fn run_step_manifests(
        &self,
        run_id: RunId,
    ) -> Result<BTreeMap<uuid::Uuid, Vec<StepRecord>>, ControlError> {
        let client = self.reader().await?;
        let sql = format!(
            "SELECT agent_job_id, step_id, kind, workflow_index, runner_number, context_name, \
             name, conclusion, started_at, finished_at FROM ( \
               SELECT agent_job_id::text AS agent_job_id, step_id, kind, workflow_index, \
                      runner_number, context_name, name, conclusion, {live_start} AS started_at, \
                      {live_finish} AS finished_at, position \
               FROM job_steps WHERE agent_job_id IN \
                 (SELECT agent_job_id FROM job_requests WHERE run_id = $1::text::uuid) \
               UNION ALL \
               SELECT h.agent_job_id::text, h.step_id, h.kind, h.workflow_index, \
                      h.runner_number, h.context_name, h.name, h.conclusion, \
                      {arch_start} AS started_at, {arch_finish} AS finished_at, h.position \
               FROM step_history h WHERE h.run_id = $1::text::uuid \
             ) s ORDER BY agent_job_id, position",
            live_start = us!("started_at"),
            live_finish = us!("finished_at"),
            arch_start = us!("h.started_at"),
            arch_finish = us!("h.finished_at"),
        );
        let rows = client
            .query(&sql, &[&run_id.0.to_string()])
            .await
            .map_err(db)?;
        let mut manifests: BTreeMap<uuid::Uuid, Vec<StepRecord>> = BTreeMap::new();
        for row in &rows {
            let (agent, step) = step_from_row(row)?;
            manifests.entry(agent).or_default().push(step);
        }
        Ok(manifests)
    }

    /// Allocate the next log id of `plan_id` (1-based, per plan) and create
    /// its empty `log_files` row keyed `{plan_id}/{log_id}`. The plan is an
    /// attempt (`plan_id = agent_job_id`); `NotFound` when no live attempt
    /// has it. Two appenders racing for one id are arbitrated by the
    /// `(plan_id, log_id)` unique index; the loser retries (bounded).
    ///
    /// Statement: `INSERT INTO log_files (log_key, run_id, plan_id, log_id)
    /// SELECT .., COALESCE(MAX(l.log_id), 0) + 1 FROM job_requests q LEFT
    /// JOIN log_files l .. WHERE q.agent_job_id = $1 ON CONFLICT DO NOTHING
    /// RETURNING log_id`.
    pub(super) async fn create_log(&self, plan_id: &str) -> Result<i64, ControlError> {
        let not_found = || ControlError::NotFound(format!("plan {plan_id} not found"));
        let plan = plan_id
            .parse::<uuid::Uuid>()
            .map_err(|_| not_found())?
            .to_string();
        let client = self.writer().await?;
        for _ in 0..LOG_ID_ATTEMPTS {
            let known = client
                .query_opt(
                    "SELECT 1 FROM job_requests WHERE agent_job_id = $1::text::uuid",
                    &[&plan],
                )
                .await
                .map_err(db)?
                .is_some();
            if !known {
                return Err(not_found());
            }
            let inserted = client
                .query_opt(
                    "INSERT INTO log_files (log_key, run_id, plan_id, log_id) \
                     SELECT $1::text || '/' || n.next, q.run_id, q.agent_job_id, n.next \
                     FROM job_requests q, \
                          (SELECT COALESCE(MAX(log_id), 0) + 1 AS next FROM log_files \
                           WHERE plan_id = $1::text::uuid) n \
                     WHERE q.agent_job_id = $1::text::uuid \
                     ON CONFLICT DO NOTHING RETURNING log_id",
                    &[&plan],
                )
                .await
                .map_err(db)?;
            if let Some(row) = inserted {
                return Ok(i64::from(row.get::<_, i32>(0)));
            }
        }
        Err(ControlError::Conflict(format!(
            "log id allocation for plan {plan_id} kept losing races"
        )))
    }
}

/// Lock one attempt's `job_requests` row for the rest of the transaction;
/// `false` when the attempt does not exist.
async fn lock_attempt(
    tx: &tokio_postgres::Transaction<'_>,
    agent: &str,
) -> Result<bool, ControlError> {
    Ok(tx
        .query_opt(
            "SELECT 1 FROM job_requests WHERE agent_job_id = $1::text::uuid FOR NO KEY UPDATE",
            &[&agent],
        )
        .await
        .map_err(db)?
        .is_some())
}

// ── Step rows: shared by the two reporting paths and the settle closures ──

/// The run and logical job an attempt (`agent_job_id`) belongs to.
pub(super) async fn step_attempt(
    tx: &tokio_postgres::Transaction<'_>,
    agent: &str,
) -> Result<Option<(RunId, JobId)>, ControlError> {
    let row = tx
        .query_opt(
            "SELECT run_id::text, job_id FROM job_requests WHERE agent_job_id = $1::text::uuid",
            &[&agent],
        )
        .await
        .map_err(db)?;
    Ok(match row {
        Some(row) => Some((codec::run_id(row.get::<_, &str>(0))?, JobId(row.get(1)))),
        None => None,
    })
}

/// One stored step row of an attempt, in the merge's neutral shape.
pub(super) async fn stored_step(
    tx: &tokio_postgres::Transaction<'_>,
    agent: &str,
    step_id: &str,
) -> Result<Option<StoredStep>, ControlError> {
    let sql = format!(
        "SELECT {STEP_COLUMNS} FROM job_steps \
         WHERE agent_job_id = $1::text::uuid AND step_id = $2"
    );
    let row = tx.query_opt(&sql, &[&agent, &step_id]).await.map_err(db)?;
    match row {
        Some(row) => Ok(Some(StoredStep::from_record(&step_from_row(&row)?.1))),
        None => Ok(None),
    }
}

/// Store a merged step row: an update of every merged column, or a new
/// synthetic row at the manifest's tail.
pub(super) async fn write_step(
    tx: &tokio_postgres::Transaction<'_>,
    agent: &str,
    existed: bool,
    row: &StoredStep,
) -> Result<(), ControlError> {
    let runner_number = row.runner_number.map(|n| n.min(i32::MAX as u32) as i32);
    if existed {
        tx.execute(
            concat!(
                "UPDATE job_steps SET name = $3, runner_number = $4::int4, conclusion = $5, \
                 started_at = ",
                ts!("$6"),
                ", finished_at = ",
                ts!("$7"),
                " WHERE agent_job_id = $1::text::uuid AND step_id = $2"
            ),
            &[
                &agent,
                &row.id,
                &row.name,
                &runner_number,
                &row.conclusion,
                &row.started_at_us,
                &row.finished_at_us,
            ],
        )
        .await
        .map_err(db)?;
        return Ok(());
    }
    tx.execute(
        concat!(
            "INSERT INTO job_steps (agent_job_id, step_id, position, kind, runner_number, \
             name, conclusion, started_at, finished_at) \
             SELECT $1::text::uuid, $2, COALESCE(MAX(position) + 1, 0), 'synthetic', \
             $3::int4, $4, $5, ",
            ts!("$6"),
            ", ",
            ts!("$7"),
            " FROM job_steps WHERE agent_job_id = $1::text::uuid"
        ),
        &[
            &agent,
            &row.id,
            &runner_number,
            &row.name,
            &row.conclusion,
            &row.started_at_us,
            &row.finished_at_us,
        ],
    )
    .await
    .map_err(db)?;
    Ok(())
}

/// Close the unfinished steps of a settled job (`job: Some`, its newest
/// attempt) or of every attempt of a cancelled run (`job: None`).
///
/// Settle used to close only `in_progress` rows, so a job that settled or was
/// cancelled while steps were still `pending` kept them `pending` forever.
/// Returns the `StepStatus` events for the rows that changed.
pub(super) async fn close_unfinished_steps(
    tx: &tokio_postgres::Transaction<'_>,
    run_id: RunId,
    job: Option<&JobId>,
    outcome: ExecutionStatus,
) -> Result<Vec<preloop_gha_protocol::NdjsonEvent>, ControlError> {
    let sql = match job {
        Some(_) => {
            "SELECT q.agent_job_id::text, q.job_id FROM job_requests q \
             WHERE q.run_id = $1::text::uuid AND q.job_id = $2 \
               AND q.request_id = (SELECT MAX(request_id) FROM job_requests \
                                   WHERE run_id = $1::text::uuid AND job_id = $2)"
        }
        None => "SELECT agent_job_id::text, job_id FROM job_requests WHERE run_id = $1::text::uuid",
    };
    let run = run_id.0.to_string();
    let rows = match job {
        Some(job_id) => tx.query(sql, &[&run, &job_id.0]).await.map_err(db)?,
        None => tx.query(sql, &[&run]).await.map_err(db)?,
    };
    let attempts: Vec<(String, String)> = rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect();
    let finished_us = codec::now_us();
    let mut events = Vec::new();
    for (agent, job_id) in attempts {
        let sql = format!(
            "SELECT {STEP_COLUMNS} FROM job_steps \
             WHERE agent_job_id = $1::text::uuid \
               AND conclusion NOT IN ('success', 'failure', 'cancelled', 'skipped', 'failed') \
             ORDER BY position"
        );
        let rows = tx.query(&sql, &[&agent]).await.map_err(db)?;
        for row in &rows {
            let current = StoredStep::from_record(&step_from_row(row)?.1);
            let patch = StepPatch {
                id: current.id.clone(),
                name: String::new(),
                conclusion: Some(crate::control::types::settled_step_conclusion(
                    &current.conclusion,
                    outcome,
                )),
                runner_number: None,
                started_at_us: None,
                finished_at_us: Some(finished_us),
                observed_us: None,
            };
            let Some(merged) = merge_step_patch(Some(&current), &patch) else {
                continue;
            };
            write_step(tx, &agent, true, &merged.row).await?;
            if merged.changed {
                let event = merged.row.event(run_id, &JobId(job_id.clone()));
                super::dispatch::append_event_tx(tx, &event).await?;
                events.push(event);
            }
        }
    }
    Ok(events)
}
