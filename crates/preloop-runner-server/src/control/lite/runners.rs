//! Runner registrations: point reads and live assignments.

use super::codec;
use super::{LiteBackend, db};
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

    /// Whether this runner may still use (or mint) runner credentials.
    pub(crate) async fn runner_credentials_usable(
        &self,
        runner_id: i64,
    ) -> Result<bool, ControlError> {
        self.read(|tx| runner_credentials_usable_tx(tx, runner_id))
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
        self.read(live_assignments_tx)
    }
}

/// [`LiteBackend::runner_credentials_usable`] inside an existing writer
/// transaction. Shared with the claim fence so eligibility and the claim read
/// the same rule atomically.
pub(super) fn runner_credentials_usable_tx(
    tx: &rusqlite::Transaction<'_>,
    runner_id: i64,
) -> Result<bool, ControlError> {
    tx.prepare_cached(
        "SELECT EXISTS (SELECT 1 FROM runners r WHERE r.runner_id = ?1 AND (\
            NOT r.ephemeral OR NOT EXISTS (\
                SELECT 1 FROM job_requests q \
                WHERE q.runner_id = r.runner_id AND q.result IS NOT NULL)))",
    )
    .map_err(db)?
    .query_row([runner_id], |row| row.get(0))
    .map_err(db)
}

/// Whether a runner may take a new job. A single-use runner may own at most
/// one attempt ever: once the bind transaction has inserted its request row,
/// even an in-flight (result-less) row blocks a second claim. Checked and
/// claimed in the same transaction, so the first claim's insert fences every
/// later claim.
pub(super) fn runner_can_claim_tx(
    tx: &rusqlite::Transaction<'_>,
    runner_id: i64,
) -> Result<bool, ControlError> {
    tx.prepare_cached(
        "SELECT EXISTS (SELECT 1 FROM runners r WHERE r.runner_id = ?1 AND (\
            NOT r.ephemeral OR NOT EXISTS (\
                SELECT 1 FROM job_requests q WHERE q.runner_id = r.runner_id)))",
    )
    .map_err(db)?
    .query_row([runner_id], |row| row.get(0))
    .map_err(db)
}

/// In-flight attempts a live session still owns: the live runner → job
/// pairings `status_inputs` and the status page's `live_assignments` both
/// report.
pub(super) fn live_assignments_tx(
    tx: &rusqlite::Transaction<'_>,
) -> Result<Vec<RunnerAssignment>, ControlError> {
    let now = std::time::SystemTime::now();
    let mut stmt = tx
        .prepare_cached(
            "SELECT q.runner_id, q.run_id, q.job_id, q.started_at \
             FROM job_requests q JOIN runner_sessions s ON s.session_id = q.session_id \
             WHERE q.result IS NULL AND q.runner_id IS NOT NULL \
             ORDER BY q.runner_id, q.request_id",
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
}
