//! The jobs one job's `needs:` reach, loaded without the run.
//!
//! A dependent's `if:` decision and its hydrated `needs` context read only
//! the jobs its declared needs name and, for the status aggregate, their
//! transitive ancestors. [`NeedsView::load`] reads exactly those `jobs` rows
//! with one recursive query, so deciding or hydrating one dependent costs a
//! read proportional to its own cone, not to the run's width.
//!
//! The matching rule is the one `RunRecord` applies and
//! `Sweep::refresh_remaining_needs` counts with: a declared need names the job
//! with that id and every job whose base id is that id, except an expanded
//! matrix parent (its legs speak for it). The recursion follows the matched
//! rows' own `job_needs` through base ids as well, so an ancestor reached
//! through a matrix base is part of the view.

use super::db;
use super::graph::job_status;
use crate::control::types::ControlError;
use crate::runtime_scheduling::NeedsSource;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use std::collections::BTreeMap;
use tokio_postgres::Transaction;

/// One reachable job: what a decision reads about it.
struct NeedRow {
    status: ExecutionStatus,
    base_id: String,
    outputs: Option<BTreeMap<String, serde_json::Value>>,
    needs: Vec<JobId>,
}

/// The rows a job's declared needs reach, transitively. A
/// [`NeedsSource`], so the shared decision and hydration functions evaluate
/// it exactly as they evaluate a whole [`RunRecord`](crate::models::RunRecord).
pub(super) struct NeedsView {
    rows: BTreeMap<JobId, NeedRow>,
}

impl NeedsView {
    /// Load every job reachable from `job_id`'s `job_needs` (the job itself
    /// is not part of its own view).
    pub(super) async fn load(
        tx: &Transaction<'_>,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Self, ControlError> {
        let rows = tx
            .query(
                // `seed` marks the starting row (the job itself); `UNION`
                // de-duplicates `(job_id, seed)`, which also ends a
                // (malformed) needs cycle.
                "WITH RECURSIVE reach(job_id, seed) AS ( \
                   SELECT $2::text, true \
                   UNION \
                   SELECT j.job_id, false \
                   FROM reach r \
                   JOIN job_needs n ON n.run_id = $1::text::uuid AND n.job_id = r.job_id \
                   JOIN jobs j ON j.run_id = n.run_id \
                        AND (j.job_id = n.needs_job_id OR j.base_id = n.needs_job_id) \
                        AND NOT (j.kind = 'matrix_parent' AND EXISTS ( \
                              SELECT 1 FROM jobs c WHERE c.run_id = j.run_id \
                                AND c.parent_job_id = j.job_id)) \
                 ) \
                 SELECT j.job_id, j.base_id, j.status, j.outputs::text, \
                        COALESCE((SELECT array_agg(n.needs_job_id ORDER BY n.position) \
                                  FROM job_needs n \
                                  WHERE n.run_id = j.run_id AND n.job_id = j.job_id), \
                                 ARRAY[]::text[]) \
                 FROM (SELECT DISTINCT job_id FROM reach WHERE NOT seed) r \
                 JOIN jobs j ON j.run_id = $1::text::uuid AND j.job_id = r.job_id",
                &[&run_id.0.to_string(), &job_id.0],
            )
            .await
            .map_err(db)?;
        let rows = rows
            .iter()
            .map(|row| {
                let outputs = row
                    .get::<_, Option<String>>(3)
                    .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
                    .and_then(|value| serde_json::from_value(value).ok());
                (
                    JobId(row.get::<_, String>(0)),
                    NeedRow {
                        status: job_status(row.get::<_, String>(2).as_str()),
                        base_id: row.get(1),
                        outputs,
                        needs: row
                            .get::<_, Vec<String>>(4)
                            .into_iter()
                            .map(JobId)
                            .collect(),
                    },
                )
            })
            .collect();
        Ok(Self { rows })
    }

    /// The outputs of the jobs `needs` name, merged under each job's base id
    /// (a matrix base collects every leg's outputs; a later leg overwrites an
    /// earlier one's key). A job with no recorded base id merges under the
    /// need that named it. This is what a deferred expansion evaluates its
    /// `needs.*.outputs` expressions against.
    pub(super) fn outputs_by_base(
        &self,
        needs: &[JobId],
    ) -> BTreeMap<String, BTreeMap<String, serde_json::Value>> {
        let mut merged: BTreeMap<String, BTreeMap<String, serde_json::Value>> = BTreeMap::new();
        for need in needs {
            for matched in self.matching_ids(need) {
                if let Some(outputs) = self.need_outputs(&matched) {
                    let base = self
                        .rows
                        .get(&matched)
                        .map_or_else(|| need.0.clone(), |row| row.base_id.clone());
                    merged.entry(base).or_default().extend(outputs.clone());
                }
            }
        }
        merged
    }
}

impl NeedsSource for NeedsView {
    fn matching_ids(&self, need: &JobId) -> Vec<JobId> {
        self.rows
            .iter()
            .filter(|(job_id, row)| *job_id == need || row.base_id == need.0)
            .map(|(job_id, _)| job_id.clone())
            .collect()
    }

    fn need_status(&self, job_id: &JobId) -> Option<ExecutionStatus> {
        self.rows.get(job_id).map(|row| row.status)
    }

    fn need_outputs(&self, job_id: &JobId) -> Option<&BTreeMap<String, serde_json::Value>> {
        self.rows.get(job_id)?.outputs.as_ref()
    }

    fn declared_needs(&self, job_id: &JobId) -> Option<&[JobId]> {
        self.rows.get(job_id).map(|row| row.needs.as_slice())
    }
}
