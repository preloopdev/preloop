//! Fork-PR approval holds and environment protection gates on SQLite.
//!
//! A run the fork policy holds, and a job whose `[environment_rules]` gate is
//! not satisfied, are parked at submit (`queue_state = 'held'`, status
//! `pending`, no concurrency wait of their own). Nothing in the ordinary
//! promotion path moves them: `promote_run` only ever admits `blocked` rows.
//! [`LiteBackend::promote_ready_jobs`] is the one way out — it re-evaluates
//! every parked job's gate, fails closed the ones that resolved to failure,
//! hands the rest back as `blocked`, and then runs the ordinary promotion
//! sweep. The reaper drives it on wall-clock time (wait timers, approval
//! windows); the approve endpoints drive it per run.

use super::codec;
use super::promote;
use super::{LiteBackend, db, jobs};
use crate::control::backend::{EnvironmentGateRead, PromoteOutcome};
use crate::control::types::ControlError;
use crate::models::EnvironmentGateState;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use rusqlite::{OptionalExtension, Transaction, params};

impl LiteBackend {
    /// `promote_ready_jobs`: see the trait contract. `None` sweeps every run
    /// parking a gate-armed job (the reaper's wall-clock sweep); a run with
    /// no parked job is a no-op.
    pub(crate) async fn promote_ready_jobs(
        &self,
        run: Option<RunId>,
        rules: &crate::config::EnvironmentRulesMap,
    ) -> Result<PromoteOutcome, ControlError> {
        let rules = rules.clone();
        self.write(move |tx| promote_ready_jobs_tx(tx, self, run, &rules))
    }

    /// `environment_gate`: one job's stored environment, armed gate and
    /// status. `None` when the run or job does not exist.
    pub(crate) async fn environment_gate(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<EnvironmentGateRead>, ControlError> {
        let job_id = job_id.clone();
        self.read(move |tx| {
            let Some(job) = jobs::job(tx, run_id, &job_id)? else {
                return Ok(None);
            };
            let spec = jobs::load_spec(tx, run_id, &job_id)?;
            Ok(Some(EnvironmentGateRead {
                environment: spec.and_then(|spec| spec.environment),
                gate: job.environment_gate,
                status: job.status,
            }))
        })
    }
}

fn promote_ready_jobs_tx(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    run: Option<RunId>,
    rules: &crate::config::EnvironmentRulesMap,
) -> Result<PromoteOutcome, ControlError> {
    let now_unix_nanos = crate::models::now_unix_nanos();
    let mut outcome = PromoteOutcome::default();
    for run_id in sweep_runs(tx, run)? {
        release_parked_jobs(tx, backend, run_id, rules, now_unix_nanos, &mut outcome)?;
        let mut scheduling = crate::runtime_scheduling::SchedulingOutcome::default();
        promote::promote_run(tx, backend, run_id, &mut scheduling)?;
        outcome.promoted += scheduling.promoted;
        outcome.failed += scheduling.failed.len();
    }
    outcome.queue_depth = jobs::ready_count(tx)?;
    outcome.next_runs_on = jobs::next_ready_labels(tx)?;
    Ok(outcome)
}

/// The runs this pass must visit: the named one, or — for the reaper's
/// wall-clock sweep — every run currently parking a gate-armed job. A run
/// parked only by the fork hold is never reached here: it has no clock of its
/// own (its window is the expiry sweep's business), and the approve path
/// names it explicitly.
fn sweep_runs(tx: &Transaction<'_>, run: Option<RunId>) -> Result<Vec<RunId>, ControlError> {
    if let Some(run_id) = run {
        return Ok(vec![run_id]);
    }
    let mut stmt = tx
        .prepare_cached(
            "SELECT DISTINCT run_id FROM jobs \
             WHERE queue_state = 'held' AND status = 'pending' \
               AND environment_gate IS NOT NULL ORDER BY run_id",
        )
        .map_err(db)?;
    let rows = stmt
        .query_map(params![], |row| row.get::<_, String>(0))
        .map_err(db)?;
    Ok(rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(db)?
        .iter()
        .map(|run| codec::run_id(run))
        .collect())
}

/// Re-evaluate every job `run_id` parked at submit and release the ones whose
/// hold lifted. A run still awaiting fork approval admits nothing at all —
/// that is the whole point of the hold.
fn release_parked_jobs(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    run_id: RunId,
    rules: &crate::config::EnvironmentRulesMap,
    now_unix_nanos: i64,
    outcome: &mut PromoteOutcome,
) -> Result<(), ControlError> {
    let run = codec::run_key(run_id);
    let Some((repository, git_ref, fork_pending)) = tx
        .prepare_cached("SELECT repository, ref, fork_approval_pending FROM runs WHERE run_id = ?1")
        .map_err(db)?
        .query_row([&run], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
            ))
        })
        .optional()
        .map_err(db)?
    else {
        return Ok(());
    };
    if fork_pending {
        return Ok(());
    }
    for job in parked_jobs(tx, run_id)? {
        let spec = jobs::load_spec(tx, run_id, &job.job_id)?;
        let environment = spec.and_then(|spec| spec.environment);
        let mut gate = job.environment_gate.clone();
        let verdict = crate::runtime_scheduling::evaluate_environment_gate(
            rules,
            &repository,
            &git_ref,
            run_id,
            &job.job_id,
            environment.as_ref(),
            &mut gate,
            now_unix_nanos,
        );
        match verdict {
            crate::runtime_scheduling::EnvironmentGateOutcome::Proceed => {
                // The gate is satisfied (or no rule applies): drop the stamp
                // and hand the job back to the ordinary promotion sweep.
                write_gate(tx, run_id, &job.job_id, None)?;
                tx.prepare_cached(
                    "UPDATE jobs SET queue_state = 'blocked', status = 'queued' \
                     WHERE run_id = ?1 AND job_id = ?2 AND queue_state = 'held'",
                )
                .map_err(db)?
                .execute(params![run, job.job_id.0])
                .map_err(db)?;
            }
            crate::runtime_scheduling::EnvironmentGateOutcome::Wait => {
                // Still gated: keep the progress stamps (the wait deadline
                // the timer armed, the approval request time) so the next
                // sweep resumes rather than restarts the gate.
                write_gate(tx, run_id, &job.job_id, gate)?;
            }
            crate::runtime_scheduling::EnvironmentGateOutcome::Failed => {
                // Fail closed: the job never dispatches, and its dependents
                // settle through the ordinary sweep.
                write_gate(tx, run_id, &job.job_id, None)?;
                super::settle::settle_node(
                    tx,
                    backend,
                    run_id,
                    &job.job_id,
                    ExecutionStatus::Failure,
                )?;
                outcome.failed += 1;
            }
        }
    }
    Ok(())
}

/// The jobs `run_id` parked at submit: `held` and `pending` with no
/// concurrency wait of their own (`concurrency_waits` is what re-arms a
/// gate-parked job, so a row naming this run, job or jobset means the job is
/// waiting on a concurrency slot, not on a hold).
fn parked_jobs(tx: &Transaction<'_>, run_id: RunId) -> Result<Vec<jobs::JobRow>, ControlError> {
    let run = codec::run_key(run_id);
    let mut stmt = tx
        .prepare_cached(&format!(
            "SELECT {} FROM jobs j WHERE j.run_id = ?1 \
               AND j.queue_state = 'held' AND j.status = 'pending' \
               AND NOT EXISTS (SELECT 1 FROM concurrency_waits w \
                     WHERE w.holder_run_id = j.run_id \
                       AND (w.holder_kind = 'run' \
                            OR (w.holder_kind = 'job' AND w.holder_job_id = j.job_id) \
                            OR (w.holder_kind = 'jobset' AND EXISTS ( \
                                SELECT 1 FROM jobsets s \
                                JOIN json_each(s.job_ids) je ON je.value = j.job_id \
                                WHERE s.jobset_id = w.holder_jobset_id)))) \
             ORDER BY j.job_order",
            jobs::JOB_COLUMNS
        ))
        .map_err(db)?;
    let rows = stmt.query_map([run], jobs::job_row).map_err(db)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(db)
}

/// Persist one job's gate progress (`None` clears it).
fn write_gate(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    gate: Option<EnvironmentGateState>,
) -> Result<(), ControlError> {
    let json = gate
        .map(|gate| serde_json::to_string(&gate).map_err(ControlError::backend))
        .transpose()?;
    tx.prepare_cached("UPDATE jobs SET environment_gate = ?3 WHERE run_id = ?1 AND job_id = ?2")
        .map_err(db)?
        .execute(params![codec::run_key(run_id), job_id.0, json])
        .map_err(db)?;
    Ok(())
}
