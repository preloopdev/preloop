//! Per-attempt step manifests (`job_steps`, archived in `step_history`).

use super::codec;
use super::{db, LiteBackend};
use crate::control::types::{step_report, ControlError, StepPatch};
use crate::models::{StepKind, StepRecord};
use preloop_gha_protocol::RunId;
use rusqlite::{params, OptionalExtension};
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
    /// Upsert runner-reported steps for one attempt: an existing step takes
    /// the new name/conclusion and only fills in missing timestamps; a new
    /// step is appended as synthetic (start defaults to the observation
    /// time). Unknown attempts write nothing.
    pub(crate) async fn patch_steps(
        &self,
        agent_job_id: uuid::Uuid,
        patches: Vec<StepPatch>,
    ) -> Result<(), ControlError> {
        let agent = agent_job_id.to_string();
        self.write(|tx| {
            for p in &patches {
                let updated = tx
                    .prepare_cached(
                        "UPDATE job_steps SET name = ?3, conclusion = ?4, \
                             started_at = COALESCE(?5, started_at), \
                             finished_at = COALESCE(?6, finished_at) \
                         WHERE agent_job_id = ?1 AND step_id = ?2",
                    )
                    .map_err(db)?
                    .execute(params![
                        agent,
                        p.id,
                        p.name,
                        p.conclusion,
                        p.started_at_us,
                        p.finished_at_us
                    ])
                    .map_err(db)?;
                if updated == 0 {
                    tx.prepare_cached(INSERT_SYNTHETIC)
                        .map_err(db)?
                        .execute(params![
                            agent,
                            p.id,
                            None::<i64>,
                            p.name,
                            p.conclusion,
                            p.started_at_us.unwrap_or(p.observed_us),
                            p.finished_at_us
                        ])
                        .map_err(db)?;
                }
            }
            Ok(())
        })
    }

    /// Reconcile a `WorkflowStepsUpdate` report. The attempt resolves by
    /// plan id (= agent job id) first, then `agent_job_id`; `false` when
    /// neither matches. Each report updates the stored step (a `NULL` name
    /// or runner number keeps the stored value; timestamps only fill in) or
    /// appends it as synthetic. Steps are written under `agent_job_id`.
    pub(crate) async fn report_steps(
        &self,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
        steps: Vec<serde_json::Value>,
    ) -> Result<bool, ControlError> {
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
                return Ok(false);
            };
            let observed = chrono::Utc::now();
            for step in &steps {
                let Some(report) = step_report(step, job_cancelled, observed) else {
                    tracing::warn!(run_id, job = %job_id, "dropping step report with no external_id");
                    continue;
                };
                let updated = tx
                    .prepare_cached(
                        "UPDATE job_steps SET runner_number = COALESCE(?3, runner_number), \
                             name = COALESCE(?4, name), conclusion = ?5, \
                             started_at = COALESCE(started_at, ?6), \
                             finished_at = COALESCE(finished_at, ?7) \
                         WHERE agent_job_id = ?1 AND step_id = ?2",
                    )
                    .map_err(db)?
                    .execute(params![
                        agent,
                        report.step_id,
                        report.runner_number,
                        report.name,
                        report.conclusion,
                        report.started_at_us,
                        report.finished_at_us
                    ])
                    .map_err(db)?;
                if updated == 0 {
                    tx.prepare_cached(INSERT_SYNTHETIC)
                        .map_err(db)?
                        .execute(params![
                            agent,
                            report.step_id,
                            report.runner_number,
                            report.name.as_deref().unwrap_or(""),
                            report.conclusion,
                            report.started_at_us,
                            report.finished_at_us
                        ])
                        .map_err(db)?;
                }
            }
            Ok(true)
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
