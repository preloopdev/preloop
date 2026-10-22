//! Per-attempt step manifests (`job_steps`, archived in `step_history`).

use super::codec;
use super::{LiteBackend, db};
use crate::control::types::{
    ControlError, StepPatch, StepReportOutcome, StoredStep, merge_step_patch, step_report,
};
use crate::models::{StepKind, StepRecord};
use preloop_gha_protocol::{ExecutionStatus, JobId, NdjsonEvent, RunId};
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::BTreeMap;

/// Step columns in [`step_row`] order; valid for `job_steps` and
/// `step_history`.
pub(super) const STEP_COLUMNS: &str = "agent_job_id, step_id, kind, workflow_index, \
     runner_number, context_name, name, conclusion, started_at, finished_at";

/// Decode a [`STEP_COLUMNS`] row into `(agent_job_id, record)`. Unknown
/// kinds decode as `Synthetic`, which refuses `--step` resolution.
pub(super) fn step_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(uuid::Uuid, StepRecord)> {
    let kind: String = row.get(2)?;
    Ok((
        codec::uuid(&row.get::<_, String>(0)?),
        StepRecord {
            id: row.get(1)?,
            kind: if kind == "workflow" {
                StepKind::Workflow
            } else {
                StepKind::Synthetic
            },
            workflow_index: row
                .get::<_, Option<i64>>(3)?
                .and_then(|i| usize::try_from(i).ok()),
            runner_number: row
                .get::<_, Option<i64>>(4)?
                .and_then(|n| u32::try_from(n).ok()),
            context_name: row.get(5)?,
            name: row.get(6)?,
            conclusion: row.get(7)?,
            started_at: row.get::<_, Option<i64>>(8)?.and_then(codec::us_to_utc),
            finished_at: row.get::<_, Option<i64>>(9)?.and_then(codec::us_to_utc),
        },
    ))
}

/// Append a new synthetic step at the end of the attempt's manifest, only
/// while the attempt exists. Positions are manifest order, not ids.
const INSERT_SYNTHETIC: &str = "INSERT INTO job_steps (agent_job_id, step_id, position, kind, \
         workflow_index, runner_number, context_name, name, conclusion, started_at, finished_at) \
     SELECT ?1, ?2, COALESCE((SELECT MAX(position) + 1 FROM job_steps WHERE agent_job_id = ?1), \
                             0), \
            'synthetic', NULL, ?3, NULL, ?4, ?5, ?6, ?7 \
     WHERE EXISTS (SELECT 1 FROM job_requests WHERE agent_job_id = ?1)";

impl LiteBackend {
    /// Upsert runner-reported steps for one attempt. Each patch merges into
    /// the stored row through [`merge_step_patch`] — the terminal guard and
    /// the "fill in only when missing" timing rules live there — and a new
    /// step is appended as synthetic. Unknown attempts write nothing.
    ///
    /// Every row that actually changed also appends its `StepStatus` event to
    /// the outbox in this same transaction, so the event is in the run's
    /// `events.ndjson` snapshot the moment the row is readable, and the
    /// caller only broadcasts.
    pub(crate) async fn patch_steps(
        &self,
        agent_job_id: uuid::Uuid,
        patches: Vec<StepPatch>,
    ) -> Result<Vec<NdjsonEvent>, ControlError> {
        if patches.is_empty() {
            return Ok(Vec::new());
        }
        let agent = agent_job_id.to_string();
        self.write(|tx| {
            let Some((run_id, job_id)) = step_attempt(tx, &agent)? else {
                return Ok(Vec::new());
            };
            let mut events = Vec::new();
            for patch in &patches {
                let current = stored_step(tx, &agent, &patch.id)?;
                let Some(merged) = merge_step_patch(current.as_ref(), patch) else {
                    continue;
                };
                write_step(tx, &agent, current.is_some(), &merged.row)?;
                if merged.changed {
                    let event = merged.row.event(run_id, &job_id);
                    super::lifecycle::append_event_tx(tx, &event)?;
                    events.push(event);
                }
            }
            Ok(events)
        })
    }

    /// Reconcile a `WorkflowStepsUpdate` report. The attempt resolves by
    /// plan id (= agent job id) first, then `agent_job_id`; `matched: false`
    /// when neither matches. Each report updates the stored step (an empty
    /// name or absent number keeps the stored value; the wire's own
    /// `started_at`/`completed_at` win while the step is unfinished) or
    /// appends it as synthetic. Steps are written under `agent_job_id`, and
    /// changed rows append their `StepStatus` event to the outbox in the same
    /// transaction.
    pub(crate) async fn report_steps(
        &self,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
        steps: Vec<serde_json::Value>,
    ) -> Result<StepReportOutcome, ControlError> {
        let agent = agent_job_id.to_string();
        let plan = codec::plan_agent_job_id(plan_id).map(|id| id.to_string());
        self.write(|tx| {
            let resolved: Option<(String, String, bool)> = tx
                .prepare_cached(
                    "SELECT q.run_id, q.job_id, j.status = 'cancelled' \
                     FROM job_requests q \
                     JOIN jobs j ON j.run_id = q.run_id AND j.job_id = q.job_id \
                     WHERE q.agent_job_id = ?1 OR q.agent_job_id = ?2 \
                     ORDER BY (q.agent_job_id = ?1) DESC LIMIT 1",
                )
                .map_err(db)?
                .query_row(params![plan, agent], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .optional()
                .map_err(db)?;
            let Some((run_id, job_id, job_cancelled)) = resolved else {
                return Ok(StepReportOutcome::default());
            };
            let run_id = codec::run_id(&run_id);
            let job_id = JobId(job_id);
            let observed = chrono::Utc::now();
            let mut events = Vec::new();
            for step in &steps {
                let Some(report) = step_report(step, job_cancelled, observed) else {
                    tracing::warn!(%run_id, job = %job_id.0, "dropping step report with no external_id");
                    continue;
                };
                let current = stored_step(tx, &agent, &report.id)?;
                let Some(merged) =
                    merge_step_patch(current.as_ref(), &report)
                else {
                    continue;
                };
                write_step(tx, &agent, current.is_some(), &merged.row)?;
                if merged.changed {
                    let event = merged.row.event(run_id, &job_id);
                    super::lifecycle::append_event_tx(tx, &event)?;
                    events.push(event);
                }
            }
            Ok(StepReportOutcome {
                matched: true,
                events,
            })
        })
    }

    /// A run's step manifests keyed by agent job id, in manifest order:
    /// live attempts from `job_steps`, archived ones from `step_history`.
    pub(crate) async fn run_step_manifests(
        &self,
        run_id: RunId,
    ) -> Result<BTreeMap<uuid::Uuid, Vec<StepRecord>>, ControlError> {
        let run = codec::run_key(run_id);
        self.read(|tx| {
            let mut stmt = tx
                .prepare_cached(&format!(
                    "SELECT {STEP_COLUMNS}, position FROM ( \
                         SELECT s.*, 0 AS archived FROM job_steps s \
                         JOIN job_requests q ON q.agent_job_id = s.agent_job_id \
                         WHERE q.run_id = ?1 \
                         UNION ALL \
                         SELECT h.agent_job_id, h.step_id, h.position, h.kind, h.workflow_index, \
                                h.runner_number, h.context_name, h.name, h.conclusion, \
                                h.started_at, h.finished_at, 1 AS archived \
                         FROM step_history h WHERE h.run_id = ?1) \
                     ORDER BY agent_job_id, position"
                ))
                .map_err(db)?;
            let rows = stmt.query_map([&run], step_row).map_err(db)?;
            let mut manifests: BTreeMap<uuid::Uuid, Vec<StepRecord>> = BTreeMap::new();
            for row in rows {
                let (agent, record) = row.map_err(db)?;
                manifests.entry(agent).or_default().push(record);
            }
            Ok(manifests)
        })
    }
}

/// The run and logical job an attempt (`agent_job_id`) belongs to.
fn step_attempt(tx: &Transaction<'_>, agent: &str) -> Result<Option<(RunId, JobId)>, ControlError> {
    tx.prepare_cached("SELECT run_id, job_id FROM job_requests WHERE agent_job_id = ?1")
        .map_err(db)?
        .query_row([agent], |row| {
            Ok((codec::run_id(&row.get::<_, String>(0)?), JobId(row.get(1)?)))
        })
        .optional()
        .map_err(db)
}

/// One stored step row of an attempt, in the merge's neutral shape.
fn stored_step(
    tx: &Transaction<'_>,
    agent: &str,
    step_id: &str,
) -> Result<Option<StoredStep>, ControlError> {
    let row = tx
        .prepare_cached(&format!(
            "SELECT {STEP_COLUMNS} FROM job_steps WHERE agent_job_id = ?1 AND step_id = ?2"
        ))
        .map_err(db)?
        .query_row(params![agent, step_id], step_row)
        .optional()
        .map_err(db)?;
    Ok(row.map(|(_, record)| StoredStep::from_record(&record)))
}

/// Store a merged step row: an update of every merged column, or a new
/// synthetic row at the manifest's tail.
fn write_step(
    tx: &Transaction<'_>,
    agent: &str,
    existed: bool,
    row: &StoredStep,
) -> Result<(), ControlError> {
    let runner_number = row.runner_number.map(i64::from);
    if existed {
        tx.prepare_cached(
            "UPDATE job_steps SET name = ?3, runner_number = ?4, conclusion = ?5, \
                 started_at = ?6, finished_at = ?7 \
             WHERE agent_job_id = ?1 AND step_id = ?2",
        )
        .map_err(db)?
        .execute(params![
            agent,
            row.id,
            row.name,
            runner_number,
            row.conclusion,
            row.started_at_us,
            row.finished_at_us
        ])
        .map_err(db)?;
        return Ok(());
    }
    tx.prepare_cached(INSERT_SYNTHETIC)
        .map_err(db)?
        .execute(params![
            agent,
            row.id,
            runner_number,
            row.name,
            row.conclusion,
            row.started_at_us,
            row.finished_at_us
        ])
        .map_err(db)?;
    Ok(())
}

/// Apply one `completejob` `stepResults` conclusion to an attempt's
/// manifest, through the same terminal guard and change comparison as every
/// other step update. Returns the `StepStatus` event when the row changed.
///
/// `completejob` only refines a step the manifest already holds: an unknown
/// id is runner bookkeeping ("Complete job", a hook), not a manifest entry.
pub(super) fn apply_step_result(
    tx: &Transaction<'_>,
    agent: &str,
    run_id: RunId,
    job_id: &JobId,
    step_id: &str,
    conclusion: &str,
    runner_number: Option<u32>,
) -> Result<Vec<NdjsonEvent>, ControlError> {
    let Some(current) = stored_step(tx, agent, step_id)? else {
        return Ok(Vec::new());
    };
    let patch = StepPatch {
        id: step_id.to_owned(),
        name: String::new(),
        conclusion: Some(conclusion.to_owned()),
        runner_number,
        started_at_us: None,
        finished_at_us: Some(codec::now_us()),
        observed_us: None,
    };
    let Some(merged) = merge_step_patch(Some(&current), &patch) else {
        return Ok(Vec::new());
    };
    write_step(tx, agent, true, &merged.row)?;
    if !merged.changed {
        return Ok(Vec::new());
    }
    let event = merged.row.event(run_id, job_id);
    super::lifecycle::append_event_tx(tx, &event)?;
    Ok(vec![event])
}

/// Close the unfinished steps of a settled job (`job: Some`) or of every
/// attempt of a cancelled run (`job: None`) with that job's outcome.
///
/// Settle used to close only `in_progress` rows, so a job that settled or was
/// cancelled while steps were still `pending` kept them `pending` forever —
/// a cancelled job could show work that never ran as still waiting to.
/// Returns the `StepStatus` events for the rows that changed.
pub(super) fn close_unfinished_steps(
    tx: &Transaction<'_>,
    run_id: RunId,
    job: Option<&JobId>,
    outcome: ExecutionStatus,
) -> Result<Vec<NdjsonEvent>, ControlError> {
    let run = codec::run_key(run_id);
    // The attempts to close: one job's newest attempt (settle addresses what
    // the runner last ran), or every attempt of the run (cancel kills all).
    let attempts: Vec<(String, JobId)> = {
        let (sql, args): (&str, Vec<&dyn rusqlite::ToSql>) = match job {
            Some(job_id) => (
                "SELECT agent_job_id, job_id FROM job_requests \
                 WHERE run_id = ?1 AND job_id = ?2 \
                   AND request_id = (SELECT MAX(request_id) FROM job_requests \
                                     WHERE run_id = ?1 AND job_id = ?2)",
                vec![&run, &job_id.0],
            ),
            None => (
                "SELECT agent_job_id, job_id FROM job_requests WHERE run_id = ?1",
                vec![&run],
            ),
        };
        let mut stmt = tx.prepare_cached(sql).map_err(db)?;
        let rows = stmt
            .query_map(args.as_slice(), |row| {
                Ok((row.get::<_, String>(0)?, JobId(row.get::<_, String>(1)?)))
            })
            .map_err(db)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db)?
    };
    let finished_us = codec::now_us();
    let mut events = Vec::new();
    for (agent, job_id) in attempts {
        let steps: Vec<StoredStep> = {
            let mut stmt = tx
                .prepare_cached(&format!(
                    "SELECT {STEP_COLUMNS} FROM job_steps WHERE agent_job_id = ?1 \
                     AND conclusion NOT IN ('success', 'failure', 'cancelled', 'skipped', 'failed') \
             ORDER BY position"
                ))
                .map_err(db)?;
            let rows = stmt.query_map([agent.as_str()], step_row).map_err(db)?;
            rows.map(|row| row.map(|(_, record)| StoredStep::from_record(&record)))
                .collect::<Result<Vec<_>, _>>()
                .map_err(db)?
        };
        for current in steps {
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
            write_step(tx, &agent, true, &merged.row)?;
            if merged.changed {
                let event = merged.row.event(run_id, &job_id);
                super::lifecycle::append_event_tx(tx, &event)?;
                events.push(event);
            }
        }
    }
    Ok(events)
}

/// The latest attempt's step manifest for one logical job (dispatch info).
pub(super) fn latest_attempt_steps(
    tx: &rusqlite::Transaction<'_>,
    run: &str,
    job: &str,
) -> Result<Vec<StepRecord>, ControlError> {
    let mut stmt = tx
        .prepare_cached(&format!(
            "SELECT {STEP_COLUMNS} FROM job_steps WHERE agent_job_id = ( \
                 SELECT agent_job_id FROM job_requests WHERE run_id = ?1 AND job_id = ?2 \
                 ORDER BY request_id DESC LIMIT 1) \
             ORDER BY position"
        ))
        .map_err(db)?;
    let rows = stmt.query_map(params![run, job], step_row).map_err(db)?;
    rows.map(|row| row.map(|(_, record)| record).map_err(db))
        .collect()
}

/// The newest archived attempt's steps for one job.
pub(crate) fn archived_attempt_steps(
    tx: &rusqlite::Transaction<'_>,
    run: &str,
    job: &str,
) -> Result<Vec<crate::models::StepRecord>, ControlError> {
    let mut stmt = tx
        .prepare_cached(&format!(
            "SELECT {} FROM step_history WHERE agent_job_id = ( \
                 SELECT agent_job_id FROM attempt_history WHERE run_id = ?1 AND job_id = ?2 \
                 ORDER BY request_id DESC LIMIT 1) \
             ORDER BY position",
            STEP_COLUMNS
        ))
        .map_err(db)?;
    let rows = stmt.query_map(params![run, job], step_row).map_err(db)?;
    rows.map(|row| row.map(|(_, record)| record).map_err(db))
        .collect()
}
