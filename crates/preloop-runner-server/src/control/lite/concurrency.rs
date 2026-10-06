//! Concurrency gates over `concurrency_holds` / `concurrency_waits` /
//! `jobsets` / `jobset_gates`.
//!
//! `concurrency_holds` has one row per `(namespace, repository, group)`: the
//! current holder. `concurrency_waits` is the FIFO pending queue (`wait_id`
//! order). A `jobset` (expanded reusable caller or other multi-job holder)
//! acquires its gates one at a time; `jobset_gates` tracks which keys it has
//! taken (`acquired`), and each acquired gate's hold row references the
//! jobset via `holder_jobset_id`.

use super::codec;
use super::db;
use super::jobs;
use crate::concurrency::{self, Holder};
use crate::control::logic::{self, ConcurrencyRow};
use crate::control::types::*;
use preloop_gha_protocol::{JobId, RunId};
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::BTreeSet;

/// `(holder_kind, holder_run_id, holder_job_id, holder_jobset_id)` for the
/// holds/waits columns.
type HolderColumns = (&'static str, String, Option<String>, Option<i64>);

/// Encode a holder into `(holder_kind, holder_run_id, holder_job_id,
/// holder_jobset_id)`; a `JobSet` must already have its `jobsets` row.
fn holder_columns(tx: &Transaction<'_>, holder: &Holder) -> Result<HolderColumns, ControlError> {
    match holder {
        Holder::Run(run_id) => Ok(("run", run_id.to_string(), None, None)),
        Holder::Job { run_id, job_id } => {
            Ok(("job", run_id.to_string(), Some(job_id.0.clone()), None))
        }
        Holder::JobSet { run_id, job_ids } => {
            let set_id = jobset_id(tx, *run_id, job_ids)?;
            Ok(("jobset", run_id.to_string(), None, Some(set_id)))
        }
    }
}

/// The `jobsets` row for this member set, creating it on first use.
pub(super) fn jobset_id(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_ids: &BTreeSet<JobId>,
) -> Result<i64, ControlError> {
    let ids_json = serde_json::to_string(&job_ids.iter().map(|j| &j.0).collect::<Vec<_>>())
        .unwrap_or_default();
    tx.prepare_cached(
        "INSERT INTO jobsets (run_id, job_ids, state) VALUES (?1, ?2, 'waiting') \
         ON CONFLICT (run_id, job_ids) DO NOTHING",
    )
    .map_err(db)?
    .execute(params![codec::run_key(run_id), ids_json])
    .map_err(db)?;
    tx.prepare_cached("SELECT jobset_id FROM jobsets WHERE run_id = ?1 AND job_ids = ?2")
        .map_err(db)?
        .query_row(params![codec::run_key(run_id), ids_json], |row| row.get(0))
        .map_err(db)
}

/// Decode a holds/waits row into a `Holder` (`jobset` needs the member list).
fn holder_of(
    tx: &Transaction<'_>,
    kind: &str,
    run_id: &str,
    job_id: Option<&str>,
    jobset_id: Option<i64>,
) -> Result<Option<Holder>, ControlError> {
    match kind {
        "jobset" => {
            let Some(set_id) = jobset_id else {
                return Ok(None);
            };
            let ids_json: String = tx
                .prepare_cached("SELECT job_ids FROM jobsets WHERE jobset_id = ?1")
                .map_err(db)?
                .query_row([set_id], |row| row.get(0))
                .optional()
                .map_err(db)?
                .unwrap_or_else(|| "[]".to_owned());
            let ids: BTreeSet<JobId> = serde_json::from_str::<Vec<String>>(&ids_json)
                .unwrap_or_default()
                .into_iter()
                .map(JobId)
                .collect();
            Ok(run_id.parse::<RunId>().ok().map(|run_id| Holder::JobSet {
                run_id,
                job_ids: ids,
            }))
        }
        other => Ok(concurrency::holder_from_row(other, run_id, job_id, "[]")),
    }
}

/// The current hold row for a group, decoded (`group_name` is the lowercase
/// `(repository, group)` key — [`concurrency::concurrency_key`]).
pub(super) fn hold_row(
    tx: &Transaction<'_>,
    namespace_id: &str,
    repository: &str,
    group_name: &str,
) -> Result<Option<(Holder, String)>, ControlError> {
    let row = tx
        .prepare_cached(
            "SELECT holder_kind, holder_run_id, holder_job_id, holder_jobset_id, \
                    display_name \
             FROM concurrency_holds \
             WHERE namespace_id = ?1 AND repository = ?2 AND group_name = ?3",
        )
        .map_err(db)?
        .query_row(params![namespace_id, repository, group_name], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .optional()
        .map_err(db)?;
    let Some((kind, run_id, job_id, set_id, display)) = row else {
        return Ok(None);
    };
    Ok(holder_of(tx, &kind, &run_id, job_id.as_deref(), set_id)?.map(|holder| (holder, display)))
}

/// FIFO waiters of a group, oldest first.
pub(super) fn waiters(
    tx: &Transaction<'_>,
    namespace_id: &str,
    repository: &str,
    group_name: &str,
) -> Result<Vec<(i64, Holder)>, ControlError> {
    let mut stmt = tx
        .prepare_cached(
            "SELECT wait_id, holder_kind, holder_run_id, holder_job_id, \
                    holder_jobset_id \
             FROM concurrency_waits \
             WHERE namespace_id = ?1 AND repository = ?2 AND group_name = ?3 \
             ORDER BY wait_id",
        )
        .map_err(db)?;
    let rows = stmt
        .query_map(params![namespace_id, repository, group_name], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<i64>>(4)?,
            ))
        })
        .map_err(db)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(db)?;
    let mut out = Vec::with_capacity(rows.len());
    for (wait_id, kind, run_id, job_id, set_id) in rows {
        if let Some(holder) = holder_of(tx, &kind, &run_id, job_id.as_deref(), set_id)? {
            out.push((wait_id, holder));
        }
    }
    Ok(out)
}

/// Insert a hold row (upsert on display_name only — callers only write a
/// hold after deciding the slot is theirs).
pub(super) fn take_hold(
    tx: &Transaction<'_>,
    namespace_id: &str,
    repository: &str,
    group_name: &str,
    display_name: &str,
    holder: &Holder,
) -> Result<(), ControlError> {
    let (kind, run_id, job_id, set_id) = holder_columns(tx, holder)?;
    tx.prepare_cached(
        "INSERT INTO concurrency_holds (namespace_id, repository, group_name, \
             display_name, holder_kind, holder_run_id, holder_job_id, \
             holder_jobset_id) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
    )
    .map_err(db)?
    .execute(params![
        namespace_id,
        repository,
        group_name,
        display_name,
        kind,
        run_id,
        job_id,
        set_id
    ])
    .map_err(db)?;
    Ok(())
}

/// Delete the hold row, if the given holder still owns it.
pub(super) fn release_hold(
    tx: &Transaction<'_>,
    namespace_id: &str,
    repository: &str,
    group_name: &str,
    holder: &Holder,
) -> Result<bool, ControlError> {
    let (kind, run_id, job_id, set_id) = holder_columns(tx, holder)?;
    let n = tx
        .prepare_cached(
            "DELETE FROM concurrency_holds WHERE namespace_id = ?1 AND repository = ?2 \
                 AND group_name = ?3 AND holder_kind = ?4 AND holder_run_id = ?5 \
                 AND holder_job_id IS NOT DISTINCT FROM ?6 \
                 AND holder_jobset_id IS NOT DISTINCT FROM ?7",
        )
        .map_err(db)?
        .execute(params![
            namespace_id,
            repository,
            group_name,
            kind,
            run_id,
            job_id,
            set_id
        ])
        .map_err(db)?;
    Ok(n == 1)
}

/// Park a holder behind the group (FIFO `wait_id`).
pub(super) fn enqueue_wait(
    tx: &Transaction<'_>,
    namespace_id: &str,
    repository: &str,
    group_name: &str,
    holder: &Holder,
) -> Result<(), ControlError> {
    let (kind, run_id, job_id, set_id) = holder_columns(tx, holder)?;
    tx.prepare_cached(
        "INSERT INTO concurrency_waits (namespace_id, repository, group_name, \
             holder_kind, holder_run_id, holder_job_id, holder_jobset_id) \
         VALUES (?1,?2,?3,?4,?5,?6,?7)",
    )
    .map_err(db)?
    .execute(params![
        namespace_id,
        repository,
        group_name,
        kind,
        run_id,
        job_id,
        set_id
    ])
    .map_err(db)?;
    Ok(())
}

/// Remove a wait row (same identity match as `release_hold`).
pub(super) fn remove_wait(
    tx: &Transaction<'_>,
    namespace_id: &str,
    repository: &str,
    group_name: &str,
    holder: &Holder,
) -> Result<bool, ControlError> {
    let (kind, run_id, job_id, set_id) = holder_columns(tx, holder)?;
    let n = tx
        .prepare_cached(
            "DELETE FROM concurrency_waits WHERE namespace_id = ?1 AND repository = ?2 \
                 AND group_name = ?3 AND holder_kind = ?4 AND holder_run_id = ?5 \
                 AND holder_job_id IS NOT DISTINCT FROM ?6 \
                 AND holder_jobset_id IS NOT DISTINCT FROM ?7",
        )
        .map_err(db)?
        .execute(params![
            namespace_id,
            repository,
            group_name,
            kind,
            run_id,
            job_id,
            set_id
        ])
        .map_err(db)?;
    Ok(n > 0)
}

/// A job's wait rows (as a `job` holder, or covered by its run's `run`
/// holder or by a jobset wait containing it).
pub(super) fn job_has_wait(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<bool, ControlError> {
    let run = codec::run_key(run_id);
    tx.prepare_cached(
        "SELECT EXISTS( \
             SELECT 1 FROM concurrency_waits w \
             WHERE (w.holder_kind = 'run' AND w.holder_run_id = ?1) \
                OR (w.holder_kind = 'job' AND w.holder_run_id = ?1 AND w.holder_job_id = ?2) \
                OR (w.holder_kind = 'jobset' AND w.holder_run_id = ?1 AND EXISTS ( \
                    SELECT 1 FROM jobsets s WHERE s.jobset_id = w.holder_jobset_id \
                    AND EXISTS (SELECT 1 FROM json_each(s.job_ids) je WHERE je.value = ?2))))",
    )
    .map_err(db)?
    .query_row(params![run, job_id.0], |row| row.get(0))
    .map_err(db)
}

/// A run's own wait rows: run holder + every job/jobset wait covering its
/// jobs. Used by `job_queue_state('held')` and cancellation cleanup.
pub(super) fn run_waits(
    tx: &Transaction<'_>,
    run_id: RunId,
) -> Result<Vec<(String, String, String)>, ControlError> {
    let run = codec::run_key(run_id);
    let mut stmt = tx
        .prepare_cached(
            "SELECT DISTINCT namespace_id, repository, group_name \
             FROM concurrency_waits WHERE holder_run_id = ?1",
        )
        .map_err(db)?;
    let rows = stmt
        .query_map([run], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .map_err(db)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(db)
}

/// A run's own hold rows.
pub(super) fn run_holds(
    tx: &Transaction<'_>,
    run_id: RunId,
) -> Result<Vec<(String, String, String, Holder)>, ControlError> {
    let run = codec::run_key(run_id);
    let mut stmt = tx
        .prepare_cached(
            "SELECT namespace_id, repository, group_name, holder_kind, \
                    holder_run_id, holder_job_id, holder_jobset_id \
             FROM concurrency_holds WHERE holder_run_id = ?1",
        )
        .map_err(db)?;
    let rows = stmt
        .query_map([run], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<i64>>(6)?,
            ))
        })
        .map_err(db)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(db)?;
    let mut out = Vec::with_capacity(rows.len());
    for (ns, repo, group, kind, run_id, job_id, set_id) in rows {
        if let Some(holder) = holder_of(tx, &kind, &run_id, job_id.as_deref(), set_id)? {
            out.push((ns, repo, group, holder));
        }
    }
    Ok(out)
}

/// A job's hold rows: as `job` holder, or as a member of a `jobset` hold.
pub(super) fn job_holds(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<Vec<(String, String, String, Holder)>, ControlError> {
    let run = codec::run_key(run_id);
    let mut stmt = tx
        .prepare_cached(
            "SELECT h.namespace_id, h.repository, h.group_name, h.holder_kind, \
                    h.holder_run_id, h.holder_job_id, h.holder_jobset_id \
             FROM concurrency_holds h \
             WHERE (h.holder_kind = 'job' AND h.holder_run_id = ?1 AND h.holder_job_id = ?2) \
                OR (h.holder_kind = 'run' AND h.holder_run_id = ?1) \
                OR (h.holder_kind = 'jobset' AND h.holder_run_id = ?1 AND EXISTS ( \
                    SELECT 1 FROM jobsets s \
                    JOIN json_each(s.job_ids) je ON je.value = ?2 \
                    WHERE s.jobset_id = h.holder_jobset_id))",
        )
        .map_err(db)?;
    let rows = stmt
        .query_map(params![run, job_id.0], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<i64>>(6)?,
            ))
        })
        .map_err(db)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(db)?;
    let mut out = Vec::with_capacity(rows.len());
    for (ns, repo, group, kind, run_id, job_id, set_id) in rows {
        if let Some(holder) = holder_of(tx, &kind, &run_id, job_id.as_deref(), set_id)? {
            out.push((ns, repo, group, holder));
        }
    }
    Ok(out)
}

/// `run_stuck_on_external_hosts`: the holder's run has no claimable work
/// except external-host jobs (macos/windows) that no runner can serve.
/// Missing run = stuck. `queue_state='ready'` is the claimable check.
fn run_stuck_on_external_hosts(tx: &Transaction<'_>, run_id: RunId) -> Result<bool, ControlError> {
    let external_host_available = jobs::registered_platforms(tx)?
        .iter()
        .any(|os| *os == "macos" || *os == "windows");
    if external_host_available {
        return Ok(false);
    }
    let run = codec::run_key(run_id);
    let exists = tx
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM runs WHERE run_id = ?1)")
        .map_err(db)?
        .query_row([&run], |row| row.get::<_, bool>(0))
        .map_err(db)?;
    if !exists {
        return Ok(true);
    }
    // Stuck iff every job is terminal, or its claimable copy (queue_state
    // 'ready') is present only when its labels need an external host.
    let mut stmt = tx
        .prepare_cached("SELECT status, queue_state, runs_on FROM jobs WHERE run_id = ?1")
        .map_err(db)?;
    let rows = stmt
        .query_map([run], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(db)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(db)?;
    Ok(rows.iter().all(|(status, queue_state, runs_on)| {
        if status_parse(status).is_terminal() {
            return true;
        }
        if *queue_state != "ready" {
            return false;
        }
        let labels: Vec<String> = serde_json::from_str(runs_on).unwrap_or_default();
        labels.iter().any(|label| {
            let label = label.to_ascii_lowercase();
            label.starts_with("macos") || label.starts_with("windows")
        })
    }))
}

/// `holder_event_order` over `run_submissions.submission`.
fn event_order_of(
    tx: &Transaction<'_>,
    holder: &Holder,
) -> Result<Option<concurrency::EventOrder>, ControlError> {
    let submission_json: Option<String> = tx
        .prepare_cached("SELECT submission FROM run_submissions WHERE run_id = ?1")
        .map_err(db)?
        .query_row([codec::run_key(holder.run_id())], |row| row.get(0))
        .optional()
        .map_err(db)?;
    let Some(json) = submission_json else {
        return Ok(None);
    };
    let submission: preloop_gha_protocol::WorkflowSubmission = serde_json::from_str(&json)
        .map_err(|e| ControlError::backend(anyhow::anyhow!("submission decode: {e}")))?;
    Ok(concurrency::event_order(
        &submission.event,
        &submission.repository,
        &submission.payload,
    ))
}

/// One gate's declared shape for a jobset member (`caller_jobset_gates`).
pub(super) struct JobSetGateDecl {
    pub(super) repository: String,
    pub(super) group_name: String,
    pub(super) display_name: String,
    pub(super) cancel_in_progress: bool,
    pub(super) queue: preloop_gha_parser::ConcurrencyQueue,
}

/// What `acquire` decided for one holder on one group.
pub(super) enum AcqOutcome {
    /// The holder owns the slot (hold row written).
    Acquired,
    /// The holder was parked behind the group.
    Parked,
    /// The arrival was cancelled on arrival (queue overflow / stale event).
    ArrivalCancelled,
    /// An unexpected gate error (evaluation failed upstream — here a
    /// backend failure): settle the arrival as failure.
    Failed,
}

/// Acquire a concurrency slot, running the shared admission decisions
/// (`logic::concurrency_admission`, `concurrency_queue_decision`) plus the
/// two preemption rules from `try_acquire_concurrency`: a stuck holder is
/// displaced unconditionally; a stale arrival loses instead of preempting.
///
/// `cancel` is invoked for each displaced holder (current holder on
/// cancel-in-progress, or single-queue waiters). It runs inside the same
/// transaction.
pub(super) fn acquire(
    tx: &Transaction<'_>,
    namespace_id: &str,
    repository: &str,
    group_name: &str,
    display_name: &str,
    holder: &Holder,
    cancel_in_progress: bool,
    queue: preloop_gha_parser::ConcurrencyQueue,
    mut cancel: impl FnMut(&Transaction<'_>, &Holder) -> Result<(), ControlError>,
) -> Result<AcqOutcome, ControlError> {
    let holder_row = |holder: &Holder, cancel_flag: bool| ConcurrencyRow {
        group: group_name.to_owned(),
        run_id: holder.run_id(),
        job_id: match holder {
            Holder::Job { job_id, .. } => Some(job_id.clone()),
            _ => None,
        },
        wait_id: 0,
        cancel_in_progress: cancel_flag,
    };

    let current = hold_row(tx, namespace_id, repository, group_name)?;
    let pending = waiters(tx, namespace_id, repository, group_name)?;

    // A late delivery must not preempt a newer holder: when the arrival
    // would displace someone, its triggering event must not be older than
    // every existing holder's. (`try_acquire_concurrency`'s superseded
    // check, verbatim.)
    let displacement = cancel_in_progress || queue == preloop_gha_parser::ConcurrencyQueue::Single;
    if displacement && let Some(arrival) = event_order_of(tx, holder)? {
        let mut superseded = false;
        for existing in current
            .iter()
            .map(|(h, _)| h)
            .chain(pending.iter().map(|(_, h)| h))
        {
            if existing.run_id() == holder.run_id() {
                continue;
            }
            if let Some(existing) = event_order_of(tx, existing)?
                && arrival.is_older_than(&existing)
            {
                superseded = true;
                break;
            }
        }
        if superseded {
            return Ok(AcqOutcome::ArrivalCancelled);
        }
    }

    // A holder wedged on unhostable external jobs is displaced outright.
    if let Some((current_holder, _)) = &current
        && run_stuck_on_external_hosts(tx, current_holder.run_id())?
    {
        // The stuck holder is evicted without a cancellation cascade:
        // it never ran (it is still queued on external hosts), and
        // GitHub treats its slot as abandoned.
        release_hold(tx, namespace_id, repository, group_name, current_holder)?;
        take_hold(
            tx,
            namespace_id,
            repository,
            group_name,
            display_name,
            holder,
        )?;
        return Ok(AcqOutcome::Acquired);
    }

    let arrival = holder_row(holder, cancel_in_progress);
    let current_row = current.as_ref().map(|(h, _)| holder_row(h, false));
    match logic::concurrency_admission(&arrival, current_row.as_ref()) {
        logic::ConcurrencyAdmission::Acquired => {
            take_hold(
                tx,
                namespace_id,
                repository,
                group_name,
                display_name,
                holder,
            )?;
            Ok(AcqOutcome::Acquired)
        }
        logic::ConcurrencyAdmission::CancelCurrent(_) => {
            let (current_holder, _) = current.clone().expect("hold row present");
            for (_, waiter) in &pending {
                if waiter.run_id() != holder.run_id() {
                    cancel(tx, waiter)?;
                }
                remove_wait(tx, namespace_id, repository, group_name, waiter)?;
            }
            release_hold(tx, namespace_id, repository, group_name, &current_holder)?;
            if current_holder.run_id() != holder.run_id() {
                cancel(tx, &current_holder)?;
            }
            take_hold(
                tx,
                namespace_id,
                repository,
                group_name,
                display_name,
                holder,
            )?;
            Ok(AcqOutcome::Acquired)
        }
        logic::ConcurrencyAdmission::Cancelled => Ok(AcqOutcome::ArrivalCancelled),
        logic::ConcurrencyAdmission::Waiting => {
            let existing: Vec<ConcurrencyRow> = pending
                .iter()
                .map(|(wait_id, h)| {
                    let mut row = holder_row(h, false);
                    row.wait_id = *wait_id as u64;
                    row
                })
                .collect();
            let mode = match queue {
                preloop_gha_parser::ConcurrencyQueue::Single => logic::ConcurrencyQueueMode::Single,
                preloop_gha_parser::ConcurrencyQueue::Max => logic::ConcurrencyQueueMode::Max,
            };
            let decision = logic::concurrency_queue_decision(mode, &existing);
            if decision.cancel_arrival {
                return Ok(AcqOutcome::ArrivalCancelled);
            }
            for waiter in &decision.cancel_pending {
                let (_, w_holder) = pending
                    .iter()
                    .find(|(id, _)| *id == waiter.wait_id as i64)
                    .map(|(id, h)| (*id, h.clone()))
                    .expect("waiter row present");
                if w_holder.run_id() != holder.run_id() {
                    cancel(tx, &w_holder)?;
                }
                remove_wait(tx, namespace_id, repository, group_name, &w_holder)?;
            }
            if decision.park_arrival {
                enqueue_wait(tx, namespace_id, repository, group_name, holder)?;
                return Ok(AcqOutcome::Parked);
            }
            Ok(AcqOutcome::Acquired)
        }
    }
}

/// The display name currently recorded for a group (`None` when no hold).
pub(super) fn hold_display_name(
    tx: &Transaction<'_>,
    namespace_id: &str,
    repository: &str,
    group_name: &str,
) -> Result<Option<String>, ControlError> {
    tx.prepare_cached(
        "SELECT display_name FROM concurrency_holds \
         WHERE namespace_id = ?1 AND repository = ?2 AND group_name = ?3",
    )
    .map_err(db)?
    .query_row(params![namespace_id, repository, group_name], |row| {
        row.get(0)
    })
    .optional()
    .map_err(db)
}

/// The oldest waiter of a group (`wait_id` order).
pub(super) fn first_waiter(
    tx: &Transaction<'_>,
    namespace_id: &str,
    repository: &str,
    group_name: &str,
) -> Result<Option<(i64, Holder)>, ControlError> {
    Ok(waiters(tx, namespace_id, repository, group_name)?
        .into_iter()
        .next())
}

/// Drop one wait row by its primary key.
pub(super) fn remove_wait_by_id(tx: &Transaction<'_>, wait_id: i64) -> Result<(), ControlError> {
    tx.prepare_cached("DELETE FROM concurrency_waits WHERE wait_id = ?1")
        .map_err(db)?
        .execute([wait_id])
        .map_err(db)?;
    Ok(())
}

/// Re-park a waiter at its original FIFO position (same `wait_id`) — used
/// when a promotion attempt finds max-parallel saturated and must not let a
/// younger waiter jump the queue.
pub(super) fn requeue_wait(
    tx: &Transaction<'_>,
    namespace_id: &str,
    repository: &str,
    group_name: &str,
    holder: &Holder,
    wait_id: i64,
) -> Result<(), ControlError> {
    let (kind, run_id, job_id, set_id) = holder_columns(tx, holder)?;
    tx.prepare_cached(
        "INSERT INTO concurrency_waits (wait_id, namespace_id, repository, \
             group_name, holder_kind, holder_run_id, holder_job_id, holder_jobset_id) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
    )
    .map_err(db)?
    .execute(params![
        wait_id,
        namespace_id,
        repository,
        group_name,
        kind,
        run_id,
        job_id,
        set_id
    ])
    .map_err(db)?;
    Ok(())
}

/// A job's wait rows (its own, its run's, or a jobset it belongs to).
pub(super) fn job_waits(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<Vec<(String, String, String, Holder)>, ControlError> {
    let run = codec::run_key(run_id);
    let mut stmt = tx
        .prepare_cached(
            "SELECT w.namespace_id, w.repository, w.group_name, w.holder_kind, \
                    w.holder_run_id, w.holder_job_id, w.holder_jobset_id \
             FROM concurrency_waits w \
             WHERE (w.holder_kind = 'run' AND w.holder_run_id = ?1) \
                OR (w.holder_kind = 'job' AND w.holder_run_id = ?1 AND w.holder_job_id = ?2) \
                OR (w.holder_kind = 'jobset' AND w.holder_run_id = ?1 AND EXISTS ( \
                    SELECT 1 FROM jobsets s \
                    JOIN json_each(s.job_ids) je ON je.value = ?2 \
                    WHERE s.jobset_id = w.holder_jobset_id))",
        )
        .map_err(db)?;
    let rows = stmt
        .query_map(params![run, job_id.0], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<i64>>(6)?,
            ))
        })
        .map_err(db)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(db)?;
    let mut out = Vec::with_capacity(rows.len());
    for (ns, repo, group, kind, run_id, job_id, set_id) in rows {
        if let Some(holder) = holder_of(tx, &kind, &run_id, job_id.as_deref(), set_id)? {
            out.push((ns, repo, group, holder));
        }
    }
    Ok(out)
}

/// Non-creating jobset lookup by member set.
pub(super) fn find_jobset(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_ids: &BTreeSet<JobId>,
) -> Result<Option<i64>, ControlError> {
    let ids_json = serde_json::to_string(&job_ids.iter().map(|j| &j.0).collect::<Vec<_>>())
        .unwrap_or_default();
    tx.prepare_cached("SELECT jobset_id FROM jobsets WHERE run_id = ?1 AND job_ids = ?2")
        .map_err(db)?
        .query_row(params![codec::run_key(run_id), ids_json], |row| row.get(0))
        .optional()
        .map_err(db)
}

/// Every hold row of a group's waiters/members that names this jobset.
pub(super) fn jobset_holds(
    tx: &Transaction<'_>,
    set_id: i64,
) -> Result<Vec<(String, String, String)>, ControlError> {
    let mut stmt = tx
        .prepare_cached(
            "SELECT namespace_id, repository, group_name FROM concurrency_holds \
             WHERE holder_jobset_id = ?1",
        )
        .map_err(db)?;
    let rows = stmt
        .query_map([set_id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .map_err(db)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(db)
}
