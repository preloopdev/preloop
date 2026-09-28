//! Runner registrations: point reads and live assignments.

use super::codec;
use super::{db, LiteBackend};
use crate::control::types::ControlError;
use preloop_observability::status::RunnerAssignment;
use rusqlite::OptionalExtension;

impl LiteBackend {
    /// Whether `runner_id` is registered.
    pub(crate) async fn runner_exists(&self, runner_id: i64) -> Result<bool, ControlError> {
        self.read(|tx| {
            tx.prepare_cached("SELECT EXISTS (SELECT 1 FROM runners WHERE runner_id = ?1)")
                .map_err(db)?
                .query_row([runner_id], |row| row.get(0))
                .map_err(db)
        })
    }

    /// The runner registered under OAuth `client_id`.
    pub(crate) async fn runner_for_client(
        &self,
        client_id: &str,
    ) -> Result<Option<i64>, ControlError> {
        self.read(|tx| {
            tx.prepare_cached("SELECT runner_id FROM runners WHERE client_id = ?1")
                .map_err(db)?
                .query_row([client_id], |row| row.get(0))
                .optional()
                .map_err(db)
        })
    }

    /// Owned, session-bound, in-flight attempts per runner (status page).
    pub(crate) async fn live_assignments(&self) -> Result<Vec<RunnerAssignment>, ControlError> {
        self.read(|tx| {
            let now = std::time::SystemTime::now();
            let mut stmt = tx
                .prepare_cached(
                    "SELECT q.runner_id, q.run_id, q.job_id, q.started_at \
                     FROM job_requests q JOIN runner_sessions s ON s.session_id = q.session_id \
                     WHERE q.result IS NULL AND q.runner_id IS NOT NULL \
                     ORDER BY q.runner_id",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map([], |row| {
                    let started: Option<i64> = row.get(3)?;
                    Ok(RunnerAssignment {
                        runner_id: row.get(0)?,
                        run_id: row.get(1)?,
                        job_id: row.get(2)?,
                        assigned_seconds_ago: started
                            .and_then(|us| now.duration_since(codec::us_to_system(us)).ok())
                            .map(|age| age.as_secs_f64())
                            .unwrap_or(0.0),
                    })
                })
                .map_err(db)?;
            rows.collect::<Result<_, _>>().map_err(db)
        })
    }
}
