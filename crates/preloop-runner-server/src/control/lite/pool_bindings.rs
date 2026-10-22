//! `pool_bindings`: `GET /api/v1/pool/bindings` read — one row per job that
//! is either waiting in the ready queue (ready/assigned/provisioning) or
//! currently held by a runner (running).
//!
//! The ready-queue read mirrors `claim_one`'s batch query (same join shape,
//! same `priority DESC, run_order, job_order, run_id, job_id` order) so the
//! reported waitlist position matches the order dispatch actually serves.

use super::codec::{self, now_us};
use super::{LiteBackend, db};
use crate::control::types::*;
use preloop_gha_protocol::JobId;

impl LiteBackend {
    pub(crate) async fn pool_bindings(&self) -> Result<Vec<PoolBindingRecord>, ControlError> {
        self.read(move |tx| {
            let now = now_us();
            let mut records = Vec::new();
            let mut waitlist = 0u32;
            {
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT j.run_id, j.job_id, j.enqueued_at, \
                         a.runner_id, a.assigned_at, ar.name, p.requested_at \
                         FROM jobs j \
                         LEFT JOIN job_assignments a ON a.run_id = j.run_id \
                            AND a.job_id = j.job_id \
                         LEFT JOIN runners ar ON ar.runner_id = a.runner_id \
                         LEFT JOIN provision_requests p ON p.run_id = j.run_id \
                            AND p.job_id = j.job_id \
                         WHERE j.queue_state = 'ready' \
                         ORDER BY j.priority DESC, j.run_order, j.job_order, \
                            j.run_id, j.job_id",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Option<i64>>(2)?,
                            row.get::<_, Option<i64>>(3)?,
                            row.get::<_, Option<i64>>(4)?,
                            row.get::<_, Option<String>>(5)?,
                            row.get::<_, Option<i64>>(6)?,
                        ))
                    })
                    .map_err(db)?;
                for row in rows {
                    let (
                        run,
                        job,
                        enqueued_at,
                        assigned_runner_id,
                        assigned_at,
                        assigned_name,
                        provision_requested_at,
                    ) = row.map_err(db)?;
                    let (state, since_at, runner_id, runner_name) =
                        if let Some(runner_id) = assigned_runner_id {
                            (
                                PoolBindingState::Assigned,
                                assigned_at.unwrap_or(now),
                                Some(runner_id),
                                assigned_name,
                            )
                        } else if let Some(requested_at) = provision_requested_at {
                            (PoolBindingState::Provisioning, requested_at, None, None)
                        } else {
                            (
                                PoolBindingState::Ready,
                                enqueued_at.unwrap_or(now),
                                None,
                                None,
                            )
                        };
                    let waitlist_position = match state {
                        PoolBindingState::Ready | PoolBindingState::Provisioning => {
                            waitlist += 1;
                            Some(waitlist)
                        }
                        PoolBindingState::Assigned | PoolBindingState::Running => None,
                    };
                    records.push(PoolBindingRecord {
                        run_id: codec::run_id(&run),
                        job_id: JobId(job),
                        state,
                        runner_id,
                        runner_name,
                        since_seconds: (now.saturating_sub(since_at).max(0) / 1_000_000) as u64,
                        waitlist_position,
                    });
                }
            }
            {
                // A runner currently holds the job: the live (unsettled)
                // attempt, same predicate `status_inputs`'s busy tally uses.
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT q.run_id, q.job_id, q.runner_id, q.started_at, r.name \
                         FROM job_requests q \
                         LEFT JOIN runners r ON r.runner_id = q.runner_id \
                         WHERE q.result IS NULL AND q.runner_id IS NOT NULL",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, Option<i64>>(3)?,
                            row.get::<_, Option<String>>(4)?,
                        ))
                    })
                    .map_err(db)?;
                for row in rows {
                    let (run, job, runner_id, started_at, runner_name) = row.map_err(db)?;
                    let since_at = started_at.unwrap_or(now);
                    records.push(PoolBindingRecord {
                        run_id: codec::run_id(&run),
                        job_id: JobId(job),
                        state: PoolBindingState::Running,
                        runner_id: Some(runner_id),
                        runner_name,
                        since_seconds: (now.saturating_sub(since_at).max(0) / 1_000_000) as u64,
                        waitlist_position: None,
                    });
                }
            }
            Ok(records)
        })
    }
}
