//! Settlement, cancellation, fail-fast, and concurrency release.
//!
//! The terminal-state machine of a job, translated from `sched.rs`:
//! `settle_request` / `retire_node_requests` / `release_concurrency_for_job`
//! / `release_concurrency_for_run` / `promote_next_from_group` /
//! `cancel_run_inner` / `cancel_job_inner` / `apply_matrix_fail_fast`.
//!
//! Every write is a conditional statement on `jobs` / `job_requests` /
//! `concurrency_holds`; the run's public status is always recomputed from
//! the leaf rows (`jobs::summarize_run_row`) so a partial write can never
//! publish an inconsistent summary.

use super::codec::{self, now_us};
use super::concurrency as cg;
use super::{LiteBackend, db, jobs, promote};
use crate::concurrency::{self, Holder};
use crate::control::logic;
use crate::control::types::*;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::BTreeSet;

/// The cancellation callback `concurrency::acquire` needs: displaced
/// holders are cancelled through the same paths a `cancel_job` command uses.
pub(super) fn canceler(
    backend: &LiteBackend,
) -> impl FnMut(&Transaction<'_>, &Holder) -> Result<(), ControlError> + '_ {
    move |tx, holder| cancel_holder(tx, backend, holder)
}

/// What `settle_node` changed, for the handler-facing outcomes.
#[derive(Debug, Clone)]
pub(super) struct SettleEffects {
    pub(super) effective_status: ExecutionStatus,
    pub(super) replayed: bool,
    pub(super) cancelled_siblings: Vec<JobId>,
    pub(super) newly_terminal_success: bool,
}

impl SettleEffects {
    fn unchanged(status: ExecutionStatus) -> Self {
        Self {
            effective_status: status,
            replayed: true,
            cancelled_siblings: Vec::new(),
            newly_terminal_success: false,
        }
    }
}

// ── Requests ─────────────────────────────────────────────────────────

/// `settle_request`: first result wins, and the attempt is dropped from its
/// session (a moot `JobCancellation` message is not redelivered).
pub(super) fn settle_requests_for_job(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    status: ExecutionStatus,
) -> Result<(), ControlError> {
    let run = codec::run_key(run_id);
    let mut stmt = tx
        .prepare_cached(
            "SELECT request_id FROM job_requests \
             WHERE run_id = ?1 AND job_id = ?2 AND result IS NULL",
        )
        .map_err(db)?;
    let ids: Vec<i64> = stmt
        .query_map(params![run, job_id.0], |row| row.get(0))
        .map_err(db)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(db)?;
    drop(stmt);
    for request_id in ids {
        super::requests::stamp_result_row(
            tx,
            request_id,
            status,
            &crate::distributed_task::agent_request_locked_until(),
        )?;
        undo_pending_cancellation(tx, request_id)?;
    }
    Ok(())
}

/// A settled attempt's queued cancellation is moot: clear the queue row and
/// delete the session message so a busy poll cannot redeliver it.
fn undo_pending_cancellation(tx: &Transaction<'_>, request_id: i64) -> Result<(), ControlError> {
    tx.prepare_cached(
        "UPDATE job_cancellations SET delivered_at = ?2 \
         WHERE request_id = ?1 AND delivered_at IS NULL",
    )
    .map_err(db)?
    .execute(params![request_id, now_us()])
    .map_err(db)?;
    tx.prepare_cached("DELETE FROM session_messages WHERE request_id = ?1")
        .map_err(db)?
        .execute([request_id])
        .map_err(db)?;
    Ok(())
}

/// `retire_node_requests(Purge)`: drop an expansion node's correlation rows
/// (their steps/timelines/logs cascade) once its subtree replaced it.
pub(super) fn purge_node_requests(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<(), ControlError> {
    let run = codec::run_key(run_id);
    let ids: Vec<i64> = {
        let mut stmt = tx
            .prepare_cached("SELECT request_id FROM job_requests WHERE run_id = ?1 AND job_id = ?2")
            .map_err(db)?;
        let rows = stmt
            .query_map(params![run, job_id.0], |row| row.get(0))
            .map_err(db)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db)?
    };
    for request_id in ids {
        tx.prepare_cached("DELETE FROM job_requests WHERE request_id = ?1")
            .map_err(db)?
            .execute([request_id])
            .map_err(db)?;
    }
    Ok(())
}

/// An expandable node (deferred matrix / reusable caller): its request rows
/// are placeholders, settled or purged when the node concludes.
pub(super) fn is_expandable(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<bool, ControlError> {
    tx.prepare_cached(
        "SELECT EXISTS(SELECT 1 FROM job_specs WHERE run_id = ?1 AND job_id = ?2 \
         AND (deferred_matrix IS NOT NULL OR reusable_call IS NOT NULL))",
    )
    .map_err(db)?
    .query_row(params![codec::run_key(run_id), job_id.0], |row| row.get(0))
    .map_err(db)
}

/// `retire_node_requests(Settle)`: settle every request the node minted.
pub(super) fn retire_node_requests(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    status: ExecutionStatus,
) -> Result<(), ControlError> {
    settle_requests_for_job(tx, run_id, job_id, status)
}

// ── Job status writes ────────────────────────────────────────────────

/// Flip one job terminal: status, queue state, clock, and dispatch-intent
/// cleanup (an assignment for a dead job would strand pool provisioning).
fn finish_job_row(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    status: ExecutionStatus,
) -> Result<(), ControlError> {
    let run = codec::run_key(run_id);
    tx.prepare_cached(
        "UPDATE jobs SET status = ?3, queue_state = 'none', completed_at = ?4, \
             started_at = COALESCE(started_at, ?4), claimed_by_runner_id = NULL, \
             claimed_at = NULL \
         WHERE run_id = ?1 AND job_id = ?2 AND status NOT IN \
             ('success','failure','cancelled','skipped')",
    )
    .map_err(db)?
    .execute(params![run, job_id.0, status_str(status), now_us()])
    .map_err(db)?;
    tx.prepare_cached("DELETE FROM job_assignments WHERE run_id = ?1 AND job_id = ?2")
        .map_err(db)?
        .execute(params![run, job_id.0])
        .map_err(db)?;
    tx.prepare_cached("DELETE FROM provision_requests WHERE run_id = ?1 AND job_id = ?2")
        .map_err(db)?
        .execute(params![run, job_id.0])
        .map_err(db)?;
    Ok(())
}

/// `continue-on-error` for a job (`job_specs.continue_on_error`, false when
/// unset).
fn continue_on_error(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<bool, ControlError> {
    Ok(tx
        .prepare_cached("SELECT continue_on_error FROM job_specs WHERE run_id = ?1 AND job_id = ?2")
        .map_err(db)?
        .query_row(params![codec::run_key(run_id), job_id.0], |row| {
            row.get::<_, Option<i64>>(0)
        })
        .optional()
        .map_err(db)?
        .flatten()
        .map(|flag| flag != 0)
        .unwrap_or(false))
}

/// `fail-fast` for a matrix base (`run.job_fail_fast`, default true).
fn fail_fast_for_base(
    tx: &Transaction<'_>,
    run_id: RunId,
    base_id: &str,
) -> Result<bool, ControlError> {
    let stored: Option<Option<i64>> = tx
        .prepare_cached(
            "SELECT s.fail_fast FROM job_specs s JOIN jobs j \
               ON j.run_id = s.run_id AND j.job_id = s.job_id \
             WHERE s.run_id = ?1 AND j.base_id = ?2 AND s.fail_fast IS NOT NULL LIMIT 1",
        )
        .map_err(db)?
        .query_row(params![codec::run_key(run_id), base_id], |row| row.get(0))
        .optional()
        .map_err(db)?;
    Ok(stored.flatten().map(|flag| flag != 0).unwrap_or(true))
}

// ── Fail-fast ────────────────────────────────────────────────────────

/// `apply_matrix_fail_fast`: a failed leg cancels its base's non-terminal
/// siblings (queue message for in-flight ones, concurrency released).
pub(super) fn apply_fail_fast(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    run_id: RunId,
    failed_job_id: &JobId,
) -> Result<Vec<JobId>, ControlError> {
    let Some(failed) = jobs::job(tx, run_id, failed_job_id)? else {
        return Ok(Vec::new());
    };
    if !fail_fast_for_base(tx, run_id, &failed.base_id)? {
        return Ok(Vec::new());
    }
    let run = codec::run_key(run_id);
    let siblings: Vec<(String, String, Option<i64>)> = {
        let mut stmt = tx
            .prepare_cached(
                "SELECT j.job_id, j.status, (SELECT q.request_id FROM job_requests q \
                     WHERE q.run_id = j.run_id AND q.job_id = j.job_id AND q.result IS NULL \
                     ORDER BY q.request_id DESC LIMIT 1) \
                 FROM jobs j WHERE j.run_id = ?1 AND j.base_id = ?2 AND j.job_id <> ?3 \
                   AND j.status IN ('pending','queued','in_progress') \
                 ORDER BY j.job_order",
            )
            .map_err(db)?;
        let rows = stmt
            .query_map(params![run, failed.base_id, failed_job_id.0], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            })
            .map_err(db)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db)?
    };
    let mut cancelled = Vec::new();
    for (job_id, status, request_id) in siblings {
        if status == "in_progress"
            && let Some(request_id) = request_id
        {
            queue_cancellation(tx, request_id, Some("fail_fast"))?;
        }
        let job_id = JobId(job_id);
        finish_job_row(tx, run_id, &job_id, ExecutionStatus::Cancelled)?;
        release_concurrency_for_job(tx, backend, run_id, &job_id)?;
        // `finish_job_row` bypasses `settle_node`, so a cancelled sibling
        // would otherwise leave the base's dependents on a stale
        // `remaining_needs` and the run hanging.
        jobs::refresh_remaining_needs(tx, run_id, &job_id, &failed.base_id)?;
        cancelled.push(job_id);
    }
    if !cancelled.is_empty() {
        jobs::summarize_run_row(tx, run_id)?;
    }
    Ok(cancelled)
}

/// Queue one `JobCancellation` for an in-flight attempt (deduplicated).
/// `reason` matches pg's `job_cancellations.reason` (`fail_fast` for a
/// fail-fast sibling, `None` elsewhere).
fn queue_cancellation(
    tx: &Transaction<'_>,
    request_id: i64,
    reason: Option<&str>,
) -> Result<(), ControlError> {
    tx.prepare_cached(
        "INSERT INTO job_cancellations (request_id, reason) \
         SELECT ?1, ?2 WHERE NOT EXISTS ( \
             SELECT 1 FROM job_cancellations WHERE request_id = ?1 AND delivered_at IS NULL)",
    )
    .map_err(db)?
    .execute(params![request_id, reason])
    .map_err(db)?;
    Ok(())
}

// ── Concurrency release ──────────────────────────────────────────────

/// `release_concurrency_for_job`: drop this job's presence in every group —
/// its own hold, a run/jobset hold it is a member of (only once that holder
/// is terminal), and any wait row naming it.
pub(super) fn release_concurrency_for_job(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    run_id: RunId,
    job_id: &JobId,
) -> Result<(), ControlError> {
    let statuses = jobs::run_graph(tx, run_id)?.statuses;
    for (ns, repo, group, holder) in cg::job_holds(tx, run_id, job_id)? {
        let release = match &holder {
            Holder::Job { .. } => true,
            other => concurrency::holder_is_terminal(other, &statuses),
        };
        if !release {
            continue;
        }
        let display = cg::hold_display_name(tx, &ns, &repo, &group)?;
        if cg::release_hold(tx, &ns, &repo, &group, &holder)? {
            promote_next_in_group(tx, backend, &ns, &repo, &group, display.as_deref())?;
        }
    }
    for (ns, repo, group, holder) in cg::job_waits(tx, run_id, job_id)? {
        cg::remove_wait(tx, &ns, &repo, &group, &holder)?;
    }
    Ok(())
}

/// `release_concurrency_for_run`: every hold and wait the run owns (used by
/// cancellation and once a run is terminal).
pub(super) fn release_concurrency_for_run(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    run_id: RunId,
) -> Result<(), ControlError> {
    for (ns, repo, group, holder) in cg::run_holds(tx, run_id)? {
        let display = cg::hold_display_name(tx, &ns, &repo, &group)?;
        if cg::release_hold(tx, &ns, &repo, &group, &holder)? {
            promote_next_in_group(tx, backend, &ns, &repo, &group, display.as_deref())?;
        }
    }
    tx.prepare_cached("DELETE FROM concurrency_waits WHERE holder_run_id = ?1")
        .map_err(db)?
        .execute([codec::run_key(run_id)])
        .map_err(db)?;
    Ok(())
}

/// Drop a jobset: its wait rows and holds, promoting each freed group.
fn release_jobset(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    set_id: i64,
) -> Result<(), ControlError> {
    tx.prepare_cached("DELETE FROM concurrency_waits WHERE holder_jobset_id = ?1")
        .map_err(db)?
        .execute([set_id])
        .map_err(db)?;
    for (ns, repo, group) in cg::jobset_holds(tx, set_id)? {
        let display = cg::hold_display_name(tx, &ns, &repo, &group)?;
        let holder: Option<Holder> = cg::hold_row(tx, &ns, &repo, &group)?.map(|(h, _)| h);
        if let Some(holder) = holder
            && cg::release_hold(tx, &ns, &repo, &group, &holder)?
        {
            promote_next_in_group(tx, backend, &ns, &repo, &group, display.as_deref())?;
        }
    }
    tx.prepare_cached("DELETE FROM jobsets WHERE jobset_id = ?1")
        .map_err(db)?
        .execute([set_id])
        .map_err(db)?;
    Ok(())
}

/// `promote_next_from_group`: hand a freed group slot to its oldest waiter
/// and make that holder runnable.
pub(super) fn promote_next_in_group(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    ns: &str,
    repo: &str,
    group: &str,
    display: Option<&str>,
) -> Result<(), ControlError> {
    let Some((wait_id, holder)) = cg::first_waiter(tx, ns, repo, group)? else {
        return Ok(());
    };
    cg::remove_wait_by_id(tx, wait_id)?;
    let display = display.unwrap_or(group);
    match holder.clone() {
        Holder::Job { run_id, job_id } => {
            let Some(job) = jobs::job(tx, run_id, &job_id)? else {
                return Ok(());
            };
            // A stale waiter (already terminal, or parked elsewhere) is
            // consumed without granting anything.
            if job.status.is_terminal() || job.queue_state != "held" {
                return Ok(());
            }
            let spec = jobs::load_spec(tx, run_id, &job_id)?;
            if !promote::under_max_parallel_job(tx, &job, spec.as_ref())? {
                // Re-park at the original FIFO position: a younger waiter
                // must not jump ahead of a saturated base.
                cg::requeue_wait(tx, ns, repo, group, &holder, wait_id)?;
                return Ok(());
            }
            cg::take_hold(tx, ns, repo, group, display, &holder)?;
            promote::enqueue_ready_job(tx, backend, &job, true)?;
        }
        Holder::Run(run_id) => {
            let held = held_jobs_of_run(tx, run_id)?;
            if held.is_empty() {
                // Foreign or finished run: nothing to hand the slot to.
                return Ok(());
            }
            cg::take_hold(tx, ns, repo, group, display, &holder)?;
            for job_id in &held {
                tx.prepare_cached(
                    "UPDATE jobs SET queue_state = 'blocked', status = 'queued' \
                     WHERE run_id = ?1 AND job_id = ?2",
                )
                .map_err(db)?
                .execute(params![codec::run_key(run_id), job_id.0])
                .map_err(db)?;
            }
            let mut outcome = crate::runtime_scheduling::SchedulingOutcome::default();
            promote::promote_run(tx, backend, run_id, &mut outcome)?;
            tx.prepare_cached(
                "UPDATE runs SET status = 'in_progress' \
                 WHERE run_id = ?1 AND status = 'queued'",
            )
            .map_err(db)?
            .execute([codec::run_key(run_id)])
            .map_err(db)?;
        }
        Holder::JobSet { run_id, job_ids } => {
            let Some(set_id) = cg::find_jobset(tx, run_id, &job_ids)? else {
                return Ok(());
            };
            cg::take_hold(tx, ns, repo, group, display, &holder)?;
            match promote::advance_jobset(tx, backend, set_id, Some((ns, repo, group)))? {
                promote::Advance::Blocked => {}
                promote::Advance::Fail(status) => {
                    // The gate admission failed: the whole subtree is
                    // cancelled/failed (matches `cancel_holder(JobSet)`).
                    for job_id in &job_ids {
                        let _ = job_id;
                    }
                    fail_jobset(tx, backend, run_id, &job_ids, status)?;
                }
                promote::Advance::Ready => {
                    for job_id in &job_ids {
                        tx.prepare_cached(
                            "UPDATE jobs SET queue_state = 'blocked', status = 'queued' \
                             WHERE run_id = ?1 AND job_id = ?2 AND queue_state = 'held'",
                        )
                        .map_err(db)?
                        .execute(params![codec::run_key(run_id), job_id.0])
                        .map_err(db)?;
                    }
                    let mut outcome = crate::runtime_scheduling::SchedulingOutcome::default();
                    promote::promote_run(tx, backend, run_id, &mut outcome)?;
                }
            }
        }
    }
    Ok(())
}

/// The jobs a run parked behind its own workflow-level gate (no per-job wait
/// of their own).
fn held_jobs_of_run(tx: &Transaction<'_>, run_id: RunId) -> Result<Vec<JobId>, ControlError> {
    let run = codec::run_key(run_id);
    let mut stmt = tx
        .prepare_cached(
            "SELECT j.job_id FROM jobs j WHERE j.run_id = ?1 \
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
        )
        .map_err(db)?;
    let rows = stmt
        .query_map([run], |row| Ok(JobId(row.get::<_, String>(0)?)))
        .map_err(db)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(db)
}

/// Conclude a whole jobset that could not be admitted.
fn fail_jobset(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    run_id: RunId,
    job_ids: &BTreeSet<JobId>,
    status: ExecutionStatus,
) -> Result<(), ControlError> {
    if let Some(set_id) = cg::find_jobset(tx, run_id, job_ids)? {
        release_jobset(tx, backend, set_id)?;
    }
    for job_id in job_ids {
        tx.prepare_cached(
            "UPDATE jobs SET expand_generation = expand_generation + 1 \
             WHERE run_id = ?1 AND job_id = ?2",
        )
        .map_err(db)?
        .execute(params![codec::run_key(run_id), job_id.0])
        .map_err(db)?;
        finish_job_row(tx, run_id, job_id, status)?;
        if is_expandable(tx, run_id, job_id)? {
            retire_node_requests(tx, run_id, job_id, status)?;
        }
        release_concurrency_for_job(tx, backend, run_id, job_id)?;
    }
    jobs::summarize_run_row(tx, run_id)?;
    Ok(())
}

// ── Cancellation ─────────────────────────────────────────────────────

/// The cancellation paths a concurrency holder can be ended through.
pub(super) fn cancel_holder(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    holder: &Holder,
) -> Result<(), ControlError> {
    match holder {
        Holder::Run(run_id) => {
            cancel_run_inner(tx, backend, *run_id)?;
        }
        Holder::Job { run_id, job_id } => {
            cancel_job_inner(tx, backend, *run_id, job_id)?;
        }
        Holder::JobSet { run_id, job_ids } => {
            fail_jobset(tx, backend, *run_id, job_ids, ExecutionStatus::Cancelled)?;
        }
    }
    Ok(())
}

/// `cancel_run_inner`: cancel every non-terminal job of a run, queue
/// cancellations for the in-flight ones, retire expandable node requests,
/// release the run's concurrency, and clear its dispatch intent. Returns
/// cancellations queued and whether this transaction cancelled a live run.
pub(super) fn cancel_run_inner(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    run_id: RunId,
) -> Result<(usize, bool), ControlError> {
    let run = codec::run_key(run_id);
    let exists: bool = tx
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM runs WHERE run_id = ?1)")
        .map_err(db)?
        .query_row([&run], |row| row.get(0))
        .map_err(db)?;
    if !exists {
        return Ok((0, false));
    }
    // In-flight attempts get a cancellation message; the rest simply turn
    // terminal (their runners hold nothing).
    let in_flight: Vec<(String, Option<i64>)> = {
        let mut stmt = tx
            .prepare_cached(
                "SELECT j.job_id, (SELECT q.request_id FROM job_requests q \
                     WHERE q.run_id = j.run_id AND q.job_id = j.job_id AND q.result IS NULL \
                     ORDER BY q.request_id DESC LIMIT 1) \
                 FROM jobs j WHERE j.run_id = ?1 AND j.status = 'in_progress'",
            )
            .map_err(db)?;
        let rows = stmt
            .query_map([&run], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
            })
            .map_err(db)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db)?
    };
    let mut cancellations = 0;
    for (_, request_id) in &in_flight {
        if let Some(request_id) = request_id {
            queue_cancellation(tx, *request_id, None)?;
            cancellations += 1;
        }
    }
    // Every expandable node (deferred matrix parent / reusable caller) minted
    // a request at submit even though it never ran — settle them now,
    // whatever queue slot they were parked in.
    let nodes: Vec<JobId> = {
        let mut stmt = tx
            .prepare_cached(
                "SELECT job_id FROM jobs WHERE run_id = ?1 \
                   AND status NOT IN ('success','failure','cancelled','skipped')",
            )
            .map_err(db)?;
        let rows = stmt
            .query_map([&run], |row| Ok(JobId(row.get::<_, String>(0)?)))
            .map_err(db)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db)?
    };
    tx.prepare_cached(
        "UPDATE jobs SET status = 'cancelled', queue_state = 'none', \
             expand_generation = expand_generation + 1, \
             completed_at = ?2, started_at = COALESCE(started_at, ?2), \
             claimed_by_runner_id = NULL, claimed_at = NULL \
         WHERE run_id = ?1 AND status NOT IN ('success','failure','cancelled','skipped')",
    )
    .map_err(db)?
    .execute(params![run, now_us()])
    .map_err(db)?;
    for job_id in &nodes {
        if is_expandable(tx, run_id, job_id)? {
            retire_node_requests(tx, run_id, job_id, ExecutionStatus::Cancelled)?;
        }
    }
    let finalized = tx
        .prepare_cached(
            "UPDATE runs SET status = 'completed', conclusion = 'cancelled', \
                 completed_at = COALESCE(completed_at, ?2), \
                 started_at = COALESCE(started_at, ?2) \
             WHERE run_id = ?1 AND status <> 'completed'",
        )
        .map_err(db)?
        .execute(params![run, now_us()])
        .map_err(db)?;
    release_concurrency_for_run(tx, backend, run_id)?;
    tx.prepare_cached("DELETE FROM jobsets WHERE run_id = ?1")
        .map_err(db)?
        .execute([&run])
        .map_err(db)?;
    jobs::clear_run_dispatch_intent(tx, run_id)?;
    // Only a run that just became terminal emits its completion: a repeat
    // cancel must not append a second `run.completed.v1`.
    if finalized > 0 {
        jobs::emit_outbox(
            tx,
            jobs::namespace_of(tx, run_id)?.as_str(),
            Some(run_id),
            "run.completed.v1",
            serde_json::json!({"status": "cancelled"}),
        )?;
    }
    Ok((cancellations, finalized > 0))
}

/// `cancel_job_inner`: cancel one job and its subtree, queueing a
/// cancellation for the in-flight attempt.
pub(super) fn cancel_job_inner(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    run_id: RunId,
    job_id: &JobId,
) -> Result<usize, ControlError> {
    let Some(job) = jobs::job(tx, run_id, job_id)? else {
        return Ok(0);
    };
    let expandable = is_expandable(tx, run_id, job_id)?;
    let live_request: Option<i64> = tx
        .prepare_cached(
            "SELECT request_id FROM job_requests WHERE run_id = ?1 AND job_id = ?2 \
               AND result IS NULL ORDER BY request_id DESC LIMIT 1",
        )
        .map_err(db)?
        .query_row(params![codec::run_key(run_id), job_id.0], |row| row.get(0))
        .optional()
        .map_err(db)?;
    let mut count = 0;
    if job.status == ExecutionStatus::InProgress
        && let Some(request_id) = live_request
    {
        queue_cancellation(tx, request_id, None)?;
        count = 1;
    }
    // A cancelled node's in-flight expansion lease is stale: bumping the
    // generation makes `apply_expansion` discard the build.
    tx.prepare_cached(
        "UPDATE jobs SET expand_generation = expand_generation + 1 \
         WHERE run_id = ?1 AND job_id = ?2",
    )
    .map_err(db)?
    .execute(params![codec::run_key(run_id), job_id.0])
    .map_err(db)?;
    finish_job_row(tx, run_id, job_id, ExecutionStatus::Cancelled)?;
    // Reusable callers own their callee subtree; a cancelled caller cancels
    // it (matrix legs included — they carry `parent_job_id` too).
    let children: Vec<JobId> = {
        let mut stmt = tx
            .prepare_cached(
                "SELECT job_id FROM jobs WHERE run_id = ?1 AND parent_job_id = ?2 \
                 AND status NOT IN ('success','failure','cancelled','skipped')",
            )
            .map_err(db)?;
        let rows = stmt
            .query_map(params![codec::run_key(run_id), job_id.0], |row| {
                Ok(JobId(row.get::<_, String>(0)?))
            })
            .map_err(db)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db)?
    };
    for child in children {
        count += cancel_job_inner(tx, backend, run_id, &child)?;
    }
    if expandable {
        retire_node_requests(tx, run_id, job_id, ExecutionStatus::Cancelled)?;
    }
    release_concurrency_for_job(tx, backend, run_id, job_id)?;
    jobs::summarize_run_row(tx, run_id)?;
    Ok(count)
}

// ── Settlement ───────────────────────────────────────────────────────

/// Settle one job to a terminal status (first result wins; `continue-on-
/// error` tolerated; a cancelled job stays cancelled), then release its
/// concurrency and run the fail-fast sweep. Returns what changed.
pub(super) fn settle_node(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    run_id: RunId,
    job_id: &JobId,
    reported: ExecutionStatus,
) -> Result<SettleEffects, ControlError> {
    let Some(job) = jobs::job(tx, run_id, job_id)? else {
        return Ok(SettleEffects::unchanged(reported));
    };
    let prior = job.status;
    let replayed = prior.is_terminal() && prior != ExecutionStatus::Cancelled;
    if replayed {
        return Ok(SettleEffects::unchanged(prior));
    }
    let tolerated = continue_on_error(tx, run_id, job_id)?;
    let decision = logic::completion_decision(logic::CompletionRow {
        job_id: job_id.clone(),
        prior_status: prior,
        reported_status: reported,
        continue_on_error: tolerated,
        // Filled from the post-write summary below; the field only selects
        // whether the workflow hold is released, which this function derives
        // from the run's own status.
        run_will_be_terminal: false,
    });
    let effective = decision.effective_status;
    let run_status_before: Option<String> = tx
        .prepare_cached("SELECT status FROM runs WHERE run_id = ?1")
        .map_err(db)?
        .query_row([codec::run_key(run_id)], |row| row.get(0))
        .optional()
        .map_err(db)?;
    finish_job_row(tx, run_id, job_id, effective)?;
    jobs::refresh_remaining_needs(tx, run_id, job_id, &job.base_id)?;
    let summary = jobs::summarize_run_row(tx, run_id)?;
    let newly_terminal_success =
        summary == ExecutionStatus::Success && run_status_before.as_deref() != Some("completed");
    let cancelled_siblings = if effective == ExecutionStatus::Failure {
        apply_fail_fast(tx, backend, run_id, job_id)?
    } else {
        Vec::new()
    };
    // Fail-fast can cancel the last non-terminal siblings and finalize the
    // run; recompute so its completion event and gate release are not lost.
    let summary = if cancelled_siblings.is_empty() {
        summary
    } else {
        jobs::summarize_run_row(tx, run_id)?
    };
    settle_requests_for_job(tx, run_id, job_id, effective)?;
    release_concurrency_for_job(tx, backend, run_id, job_id)?;
    if summary.is_terminal() {
        // A terminal run must not hold a workflow-level slot.
        release_concurrency_for_run(tx, backend, run_id)?;
        jobs::emit_outbox(
            tx,
            jobs::namespace_of(tx, run_id)?.as_str(),
            Some(run_id),
            "run.completed.v1",
            serde_json::json!({"status": status_str(summary)}),
        )?;
    }
    jobs::emit_outbox(
        tx,
        jobs::namespace_of(tx, run_id)?.as_str(),
        Some(run_id),
        "job.completed.v1",
        serde_json::json!({"job_id": job_id.0, "status": status_str(effective)}),
    )?;
    Ok(SettleEffects {
        effective_status: effective,
        replayed: decision.replayed,
        cancelled_siblings,
        newly_terminal_success,
    })
}

// ── Fork-PR approval window ──────────────────────────────────────────

/// `expire_fork_approvals`: fail closed every run whose fork-PR approval
/// hold lapsed before `expired_before_unix_nanos`. A held run's jobs never
/// left admission — they carry no runner and hold nothing — so expiry turns
/// them `failure` outright (the operator can still see why on the run),
/// clears the hold, finalizes the run as a `failure`, drops its scheduling
/// rows and releases its concurrency holder so the group is not wedged by a
/// run that will never start.
///
/// A run whose hold stamp is missing is left held: a bookkeeping gap must
/// not auto-fail a run, and the operator can still approve or cancel it.
///
/// Statements: one candidate `SELECT .. FROM runs WHERE fork_approval_pending
/// = 1 AND status <> 'completed' AND fork_approval_requested_at < ?1` at `now
/// = ?2`, then per run one `UPDATE jobs ..` (non-terminal → `failure`),
/// `retire_node_requests` for its expandable nodes, one `UPDATE runs ..
/// fork_approval_pending = 0, status = 'completed', conclusion = 'failure'`,
/// `DELETE FROM jobsets`, the dispatch-intent clear, the concurrency release
/// (with waiter promotion) and one `run.completed.v1` outbox row.
pub(super) fn expire_fork_approvals_inner(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    expired_before_unix_nanos: i64,
) -> Result<Vec<RunId>, ControlError> {
    let expired: Vec<RunId> = {
        let mut stmt = tx
            .prepare_cached(
                "SELECT run_id FROM runs \
                 WHERE fork_approval_pending = 1 AND status <> 'completed' \
                   AND fork_approval_requested_at IS NOT NULL \
                   AND fork_approval_requested_at < ?1 \
                 ORDER BY run_id",
            )
            .map_err(db)?;
        let rows = stmt
            .query_map([expired_before_unix_nanos], |row| {
                Ok(codec::run_id(&row.get::<_, String>(0)?))
            })
            .map_err(db)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db)?
    };
    let now = now_us();
    let mut failed = Vec::with_capacity(expired.len());
    for run_id in expired {
        let run = codec::run_key(run_id);
        // Expandable nodes (deferred matrix parents, reusable callers) minted
        // placeholder requests at submit even though they never ran; settle
        // them with the node so no in-flight marker outlives the run.
        let nodes: Vec<JobId> = {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT job_id FROM jobs WHERE run_id = ?1 \
                       AND status NOT IN ('success','failure','cancelled','skipped','timed_out')",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map([&run], |row| Ok(JobId(row.get::<_, String>(0)?)))
                .map_err(db)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(db)?
        };
        tx.prepare_cached(
            "UPDATE jobs SET status = 'failure', queue_state = 'none', \
                 completed_at = ?2, started_at = COALESCE(started_at, ?2), \
                 claimed_by_runner_id = NULL, claimed_at = NULL \
             WHERE run_id = ?1 \
               AND status NOT IN ('success','failure','cancelled','skipped','timed_out')",
        )
        .map_err(db)?
        .execute(params![run, now])
        .map_err(db)?;
        for job_id in &nodes {
            if is_expandable(tx, run_id, job_id)? {
                retire_node_requests(tx, run_id, job_id, ExecutionStatus::Failure)?;
            }
        }
        let finalized = tx
            .prepare_cached(
                "UPDATE runs SET fork_approval_pending = 0, status = 'completed', \
                     conclusion = 'failure', \
                     completed_at = COALESCE(completed_at, ?2), \
                     started_at = COALESCE(started_at, ?2) \
                 WHERE run_id = ?1 AND status <> 'completed'",
            )
            .map_err(db)?
            .execute(params![run, now])
            .map_err(db)?;
        if finalized == 0 {
            // A concurrent cancel/complete won the transition: it owns the
            // run's terminal bookkeeping, but the hold must still be gone.
            tx.prepare_cached("UPDATE runs SET fork_approval_pending = 0 WHERE run_id = ?1")
                .map_err(db)?
                .execute([&run])
                .map_err(db)?;
            continue;
        }
        tx.prepare_cached("DELETE FROM jobsets WHERE run_id = ?1")
            .map_err(db)?
            .execute([&run])
            .map_err(db)?;
        jobs::clear_run_dispatch_intent(tx, run_id)?;
        release_concurrency_for_run(tx, backend, run_id)?;
        jobs::emit_outbox(
            tx,
            jobs::namespace_of(tx, run_id)?.as_str(),
            Some(run_id),
            "run.completed.v1",
            serde_json::json!({"status": "failure"}),
        )?;
        failed.push(run_id);
    }
    Ok(failed)
}

impl LiteBackend {
    /// `expire_fork_approvals`: one transaction over every run whose hold
    /// lapsed (see [`expire_fork_approvals_inner`]). Returns the failed run
    /// ids for the caller's event and check-run fan-out.
    pub(crate) async fn expire_fork_approvals(
        &self,
        expired_before_unix_nanos: i64,
    ) -> Result<Vec<RunId>, ControlError> {
        self.write(move |tx| expire_fork_approvals_inner(tx, self, expired_before_unix_nanos))
    }
}
