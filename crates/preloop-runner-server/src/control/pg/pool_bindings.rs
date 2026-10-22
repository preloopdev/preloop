//! `pool_bindings`: `GET /api/v1/pool/bindings` read — one row per job that
//! is either waiting in the ready queue (ready/assigned/provisioning) or
//! currently held by a runner (running). Mirrors `control/lite/pool_bindings`
//! statement for statement.

use super::codec::{self, now_us, us};
use super::{PgBackend, db};
use crate::control::types::*;
use preloop_gha_protocol::JobId;

impl PgBackend {
    pub(super) async fn pool_bindings(&self) -> Result<Vec<PoolBindingRecord>, ControlError> {
        let client = self.reader().await?;
        let now = now_us();
        let mut records = Vec::new();
        let mut waitlist = 0u32;
        let ready_rows = client
            .query(
                concat!(
                    "SELECT j.run_id::text, j.job_id, ",
                    us!("j.enqueued_at"),
                    ", a.runner_id, ",
                    us!("a.assigned_at"),
                    ", ar.name, ",
                    us!("p.requested_at"),
                    " FROM jobs j \
                     LEFT JOIN job_assignments a ON a.run_id = j.run_id \
                        AND a.job_id = j.job_id \
                     LEFT JOIN runners ar ON ar.runner_id = a.runner_id \
                     LEFT JOIN provision_requests p ON p.run_id = j.run_id \
                        AND p.job_id = j.job_id \
                     WHERE j.queue_state = 'ready' \
                     ORDER BY j.priority DESC, j.run_order, j.job_order, \
                        j.run_id, j.job_id"
                ),
                &[],
            )
            .await
            .map_err(db)?;
        for row in &ready_rows {
            let run_id = codec::run_id(row.get::<_, String>(0).as_str())?;
            let job_id = JobId(row.get::<_, String>(1));
            let enqueued_at: Option<i64> = row.get(2);
            let assigned_runner_id: Option<i64> = row.get(3);
            let assigned_at: Option<i64> = row.get(4);
            let assigned_name: Option<String> = row.get(5);
            let provision_requested_at: Option<i64> = row.get(6);
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
                run_id,
                job_id,
                state,
                runner_id,
                runner_name,
                since_seconds: (now.saturating_sub(since_at).max(0) / 1_000_000) as u64,
                waitlist_position,
            });
        }
        // A runner currently holds the job: the live (unsettled) attempt,
        // same predicate `status_inputs`'s busy tally uses.
        let running_rows = client
            .query(
                concat!(
                    "SELECT q.run_id::text, q.job_id, q.runner_id, ",
                    us!("q.started_at"),
                    ", r.name \
                     FROM job_requests q \
                     LEFT JOIN runners r ON r.runner_id = q.runner_id \
                     WHERE q.result IS NULL AND q.runner_id IS NOT NULL"
                ),
                &[],
            )
            .await
            .map_err(db)?;
        for row in &running_rows {
            let run_id = codec::run_id(row.get::<_, String>(0).as_str())?;
            let job_id = JobId(row.get::<_, String>(1));
            let runner_id: i64 = row.get(2);
            let started_at: Option<i64> = row.get(3);
            let runner_name: Option<String> = row.get(4);
            let since_at = started_at.unwrap_or(now);
            records.push(PoolBindingRecord {
                run_id,
                job_id,
                state: PoolBindingState::Running,
                runner_id: Some(runner_id),
                runner_name,
                since_seconds: (now.saturating_sub(since_at).max(0) / 1_000_000) as u64,
                waitlist_position: None,
            });
        }
        Ok(records)
    }
}
