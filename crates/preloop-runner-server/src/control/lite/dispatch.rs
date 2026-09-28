//! Job completion and cancellation commands: the runner `completejob`
//! path, the broker `settle_job` path, run/job cancellation, and the
//! request renew/release leases.
//!
//! Completion semantics are shared with the Postgres backend
//! (`control/pg/dispatch.rs::complete_node`): outputs/annotations land on
//! the jobs row, `settle_node` owns the status transition + concurrency
//! release + dependent decrement, reusable callers fold their callee
//! outputs once every inner job is terminal, in-progress step rows of the
//! attempt settle with the job, then `promote_run` runs the dependent
//! sweep. On SQLite the whole command is one `BEGIN IMMEDIATE` writer
//! transaction — the run-row lock `pg` takes is implicit.

use super::codec::{self, now_us};
use super::jobs;
use super::requests;
use super::{db, promote, settle, LiteBackend};
use crate::control::backend::JobCompletionInput;
use crate::control::types::*;
use crate::models::TaskAgentJobRequestRecord;
use preloop_gha_protocol::{azdo, ExecutionStatus, JobId, RunId};
use rusqlite::{params, OptionalExtension, Transaction};
use std::collections::BTreeMap;

/// Settle one request row in place (the tx half of
/// [`LiteBackend::settle_request`]): drop the deferred token request, drop
/// undelivered cancellations (queued rows and the session's moot
/// `JobCancellation` message), clear the session binding, stamp the result
/// and the final lease expiry.
fn settle_request_tx(
    tx: &Transaction<'_>,
    request_id: i64,
    result: ExecutionStatus,
) -> Result<(), ControlError> {
    tx.prepare_cached("DELETE FROM github_token_requests WHERE request_id = ?1")
        .map_err(db)?
        .execute([request_id])
        .map_err(db)?;
    tx.prepare_cached(
        "DELETE FROM job_cancellations WHERE request_id = ?1 AND delivered_at IS NULL",
    )
    .map_err(db)?
    .execute([request_id])
    .map_err(db)?;
    tx.prepare_cached("DELETE FROM session_messages WHERE request_id = ?1 AND message_type = ?2")
        .map_err(db)?
        .execute(params![request_id, azdo::message_type::JOB_CANCELLED])
        .map_err(db)?;
    tx.prepare_cached("UPDATE job_requests SET session_id = NULL WHERE request_id = ?1")
        .map_err(db)?
        .execute([request_id])
        .map_err(db)?;
    requests::stamp_result_row(
        tx,
        request_id,
        result,
        &crate::distributed_task::agent_request_locked_until(),
    )
}

/// The live attempt's request id for a `(run, job)`: the explicit agent
/// job id when given, else the newest in-flight attempt.
fn attempt_request_id(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    explicit: Option<uuid::Uuid>,
) -> Result<Option<i64>, ControlError> {
    let run = codec::run_key(run_id);
    let row = match explicit {
        Some(agent) => tx
            .prepare_cached(
                "SELECT request_id FROM job_requests WHERE agent_job_id = ?1 \
                 AND run_id = ?2 AND job_id = ?3",
            )
            .map_err(db)?
            .query_row(params![agent.to_string(), run, job_id.0], |row| {
                row.get::<_, i64>(0)
            })
            .optional()
            .map_err(db)?,
        None => tx
            .prepare_cached(
                "SELECT request_id FROM job_requests WHERE run_id = ?1 \
                 AND job_id = ?2 AND result IS NULL ORDER BY request_id DESC LIMIT 1",
            )
            .map_err(db)?
            .query_row(params![run, job_id.0], |row| row.get::<_, i64>(0))
            .optional()
            .map_err(db)?,
    };
    Ok(row)
}

/// The live-log key of a job's newest attempt (`agent_job_id`, else the
/// logical job id — the convention `live_log_key` already exposes).
fn attempt_log_key(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    explicit: Option<uuid::Uuid>,
) -> Result<String, ControlError> {
    let agent = match explicit {
        Some(agent) => Some(agent.to_string()),
        None => tx
            .prepare_cached(
                "SELECT agent_job_id FROM job_requests WHERE run_id = ?1 \
                 AND job_id = ?2 ORDER BY request_id DESC LIMIT 1",
            )
            .map_err(db)?
            .query_row(params![codec::run_key(run_id), job_id.0], |row| {
                row.get::<_, String>(0)
            })
            .optional()
            .map_err(db)?,
    };
    Ok(agent.unwrap_or_else(|| job_id.0.clone()))
}

/// What one completion decided (pg's `CompletionApplied`).
struct CompletionApplied {
    effective_status: ExecutionStatus,
    cancelled_siblings: Vec<JobId>,
    newly_terminal_success: bool,
    replayed: bool,
    live_log_key: String,
}

/// Fold completed callee subtrees into their reusable callers
/// (`propagate_reusable_outputs` over the SQL projection): for every caller
/// whose inner jobs are all terminal, write the resolved `outputs` map and
/// the aggregate status, release its JobSet gates and retire the
/// placeholder requests it minted. Returns the caller ids finalized.
///
/// The record is loaded via `jobs::run_record` and the *shared* pure
/// propagation (`reusable_workflows::propagate_reusable_outputs`, the same
/// function pg's Sweep drives over its graph) decides the fold; the writes
/// land with targeted UPDATEs.
fn finalize_reusable_callers(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    run_id: RunId,
) -> Result<Vec<JobId>, ControlError> {
    let Some(mut record) = jobs::run_record(tx, run_id)? else {
        return Ok(Vec::new());
    };
    let finalized = crate::reusable_workflows::propagate_reusable_outputs(&mut record);
    for caller_id in &finalized {
        let status = record
            .jobs
            .get(caller_id)
            .copied()
            .unwrap_or(ExecutionStatus::Failure);
        let outputs = record
            .job_outputs
            .get(caller_id)
            .map(serde_json::to_string)
            .transpose()
            .map_err(ControlError::backend)?;
        tx.prepare_cached(
            "UPDATE jobs SET outputs = ?3, status = ?4, queue_state = 'none', \
             completed_at = COALESCE(completed_at, ?5) \
             WHERE run_id = ?1 AND job_id = ?2",
        )
        .map_err(db)?
        .execute(params![
            codec::run_key(run_id),
            caller_id.0,
            outputs,
            status_str(status),
            now_us()
        ])
        .map_err(db)?;
        // The caller node minted placeholder requests at expansion; a
        // terminal caller must not leave them in flight.
        settle::retire_node_requests(tx, run_id, caller_id, status)?;
        // Dependents waiting on the caller unblock: recompute their counts
        // so the promotion sweep admits them.
        jobs::refresh_remaining_needs(tx, run_id, caller_id, &caller_id.0)?;
        settle::release_concurrency_for_job(tx, backend, run_id, caller_id)?;
        jobs::emit_outbox(
            tx,
            jobs::namespace_of(tx, run_id)?.as_str(),
            Some(run_id),
            "job.completed.v1",
            serde_json::json!({"job_id": caller_id.0, "status": status_str(status)}),
        )?;
    }
    Ok(finalized)
}

/// `complete_node` (pg dispatch.rs): apply one terminal completion —
/// first-verdict rule, outputs/annotations, request retirement, gate
/// release, fail-fast siblings, caller folding, dependent promotion —
/// returning the decided effects.
fn complete_node(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    run_id: RunId,
    job_id: &JobId,
    reported: ExecutionStatus,
    outputs: &BTreeMap<String, serde_json::Value>,
    annotations: &[serde_json::Value],
    explicit_attempt: Option<uuid::Uuid>,
) -> Result<CompletionApplied, ControlError> {
    let Some(job) = jobs::job(tx, run_id, job_id)? else {
        // pg's complete_node: the run is locked and a job outside it is a
        // contract violation.
        return Err(ControlError::backend(anyhow::anyhow!(
            "job {job_id} does not belong to run {run_id}"
        )));
    };
    let prior = job.status;
    // Terminal (non-cancelled) verdicts stick; a cancellation reported as
    // success/failure also keeps `cancelled`.
    let replayed = prior.is_terminal() && prior != ExecutionStatus::Cancelled;
    if replayed {
        return Ok(CompletionApplied {
            effective_status: prior,
            cancelled_siblings: Vec::new(),
            newly_terminal_success: false,
            replayed: true,
            live_log_key: attempt_log_key(tx, run_id, job_id, explicit_attempt)?,
        });
    }
    // Outputs + annotations land on the node (masked by the caller).
    if !outputs.is_empty() {
        tx.prepare_cached("UPDATE jobs SET outputs = ?3 WHERE run_id = ?1 AND job_id = ?2")
            .map_err(db)?
            .execute(params![
                codec::run_key(run_id),
                job_id.0,
                serde_json::to_string(outputs).map_err(ControlError::backend)?
            ])
            .map_err(db)?;
    }
    if !annotations.is_empty() {
        tx.prepare_cached("UPDATE jobs SET annotations = ?3 WHERE run_id = ?1 AND job_id = ?2")
            .map_err(db)?
            .execute(params![
                codec::run_key(run_id),
                job_id.0,
                serde_json::to_string(annotations).map_err(ControlError::backend)?
            ])
            .map_err(db)?;
    }
    // Terminal transition + gate release + dependents' needs (settle_node
    // internally applies fail-fast, settles every request of the job,
    // re-summarizes the run and emits job.completed.v1 / run.completed.v1).
    let effects = settle::settle_node(tx, backend, run_id, job_id, reported)?;
    // Reusable callers fold their callee outputs into terminal statuses.
    let finalized = finalize_reusable_callers(tx, backend, run_id)?;
    let _ = finalized;
    // Re-summarize after caller folds: a caller becoming terminal may
    // itself complete the run.
    let summary = jobs::summarize_run_row(tx, run_id)?;
    if summary.is_terminal() {
        settle::release_concurrency_for_run(tx, backend, run_id)?;
    }
    // Orphaned in-progress steps of this attempt settle with the job.
    tx.prepare_cached(
        "UPDATE job_steps SET conclusion = ?3, finished_at = COALESCE(finished_at, ?4) \
         WHERE agent_job_id = (SELECT agent_job_id FROM job_requests \
           WHERE run_id = ?1 AND job_id = ?2 ORDER BY request_id DESC LIMIT 1) \
           AND conclusion = 'in_progress'",
    )
    .map_err(db)?
    .execute(params![
        codec::run_key(run_id),
        job_id.0,
        crate::runtime_scheduling::status_string(effects.effective_status),
        now_us()
    ])
    .map_err(db)?;
    Ok(CompletionApplied {
        effective_status: effects.effective_status,
        cancelled_siblings: effects.cancelled_siblings,
        newly_terminal_success: effects.newly_terminal_success,
        replayed: false,
        live_log_key: attempt_log_key(tx, run_id, job_id, explicit_attempt)?,
    })
}

/// Ready-queue gauges a completion/cancellation reports back: depth,
/// front labels, and whether any work (or pending cancellation) remains.
fn queue_gauges(tx: &Transaction<'_>) -> Result<(usize, Vec<String>, bool), ControlError> {
    let depth = jobs::ready_count(tx)?;
    let next = jobs::next_ready_labels(tx)?;
    let nonempty = jobs::queue_nonempty(tx)?
        || tx
            .prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM job_cancellations \
                 WHERE delivered_at IS NULL)",
            )
            .map_err(db)?
            .query_row([], |row| row.get::<_, bool>(0))
            .map_err(db)?;
    Ok((depth, next, nonempty))
}

impl LiteBackend {
    /// `complete_job` (pg dispatch.rs): settle the attempt, flip the job,
    /// propagate outputs and dependents, fail-fast siblings. `NotFound`
    /// when the run does not exist.
    pub(crate) async fn complete_job(
        &self,
        completion: JobCompletionInput,
    ) -> Result<CompleteOutcome, ControlError> {
        let run_id = completion.run_id;
        let job_id = completion.job_id.clone();
        self.write(|tx| {
            if jobs::run_record(tx, run_id)?.is_none() {
                return Err(ControlError::NotFound(format!("run {run_id}")));
            }
            // Settle this attempt (first result wins on the request row).
            if let Some(request_id) =
                attempt_request_id(tx, run_id, &job_id, completion.agent_job_id)?
            {
                settle_request_tx(tx, request_id, completion.status)?;
            }
            let applied = complete_node(
                tx,
                self,
                run_id,
                &job_id,
                completion.status,
                &completion.outputs,
                &[],
                completion.agent_job_id,
            )?;
            if !applied.replayed {
                // The claimed marker is gone once the job is terminal.
                tx.prepare_cached(
                    "UPDATE job_assignments SET runner_id = NULL \
                     WHERE run_id = ?1 AND job_id = ?2",
                )
                .map_err(db)?
                .execute(params![codec::run_key(run_id), job_id.0])
                .map_err(db)?;
            }
            let mut scheduling = crate::runtime_scheduling::SchedulingOutcome::default();
            promote::promote_run(tx, self, run_id, &mut scheduling)?;
            let (queue_depth, _next_runs_on, queue_nonempty) = queue_gauges(tx)?;
            let record = jobs::run_record(tx, run_id)?
                .ok_or_else(|| ControlError::NotFound(format!("run {run_id}")))?;
            Ok(CompleteOutcome {
                record,
                effective_status: applied.effective_status,
                newly_terminal_success: applied.newly_terminal_success,
                cancelled_siblings: applied.cancelled_siblings,
                scheduling,
                live_log_key: applied.live_log_key,
                queue_nonempty,
                queue_depth,
                replayed: applied.replayed,
            })
        })
    }

    /// `settle_job` (pg dispatch.rs): the broker-shaped completion — the
    /// reporting runner must own the attempt (recorded owner or its
    /// session's runner); a duplicate/terminal report is `Unchanged`.
    pub(crate) async fn settle_job(
        &self,
        settle: SettleJob,
    ) -> Result<SettleJobOutcome, ControlError> {
        let comp = settle.completion.clone();
        let run_id = comp.run_id;
        let job_id = comp.job_id.clone();
        self.write(|tx| {
            if jobs::run_record(tx, run_id)?.is_none() {
                return Err(ControlError::NotFound("run not found".to_owned()));
            }
            // Ownership: a broker completion must name an attempt this
            // runner owns (recorded owner or the session runner).
            let mut attempt = None;
            if let Some(settle_attempt) = &settle.settle {
                let request_id =
                    attempt_request_id(tx, run_id, &job_id, Some(settle_attempt.agent_job_id))?
                        .ok_or_else(|| {
                            ControlError::NotFound("broker complete request not found".to_owned())
                        })?;
                let owner: Option<(Option<i64>, Option<i64>, bool)> = tx
                    .prepare_cached(
                        "SELECT q.runner_id, s.runner_id, s.session_id IS NOT NULL \
                         FROM job_requests q LEFT JOIN runner_sessions s \
                         ON s.session_id = q.session_id WHERE q.request_id = ?1",
                    )
                    .map_err(db)?
                    .query_row([request_id], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })
                    .optional()
                    .map_err(db)?;
                if let Some((owner, session_runner, has_session)) = owner {
                    ensure_request_owner(
                        owner,
                        session_runner,
                        has_session,
                        settle_attempt.runner_id,
                    )?;
                }
                attempt = Some(request_id);
            }
            // Mask annotations with the run's stored (redacted) secrets —
            // the values are unavailable at rest, so this strips only what
            // plaintext the submission still carries.
            let annotations = {
                let record = jobs::run_record(tx, run_id)?.expect("existence checked");
                crate::distributed_task::mask_completion_annotations(&record, &comp)
            };
            let outputs: BTreeMap<String, serde_json::Value> = comp
                .outputs
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            let applied = complete_node(
                tx,
                self,
                run_id,
                &job_id,
                comp.status,
                &outputs,
                &annotations,
                comp.agent_job_id,
            )?;
            if applied.replayed {
                let record = jobs::run_record(tx, run_id)?.expect("existence checked");
                return Ok(SettleJobOutcome::Unchanged(Box::new(record)));
            }
            if let Some(request_id) = attempt {
                settle_request_tx(tx, request_id, applied.effective_status)?;
                // The attempt's step results land on its manifest.
                if let Some(agent) = comp.agent_job_id {
                    for wire in &comp.step_results {
                        let Some(external_id) = wire.external_id.as_deref() else {
                            continue;
                        };
                        if let Some(conclusion) =
                            crate::distributed_task::completion_step_conclusion(wire)
                        {
                            tx.prepare_cached(
                                "UPDATE job_steps SET conclusion = ?3, \
                                 runner_number = COALESCE(?4, runner_number), \
                                 finished_at = COALESCE(finished_at, ?5) \
                                 WHERE agent_job_id = ?1 AND step_id = ?2",
                            )
                            .map_err(db)?
                            .execute(params![
                                agent.to_string(),
                                external_id,
                                conclusion,
                                wire.number.and_then(|n| i64::try_from(n).ok()),
                                now_us()
                            ])
                            .map_err(db)?;
                        }
                    }
                }
            }
            let mut scheduling = crate::runtime_scheduling::SchedulingOutcome::default();
            promote::promote_run(tx, self, run_id, &mut scheduling)?;
            let (queue_len, next_runs_on, _pending) = queue_gauges(tx)?;
            let queue_nonempty = queue_len > 0;
            Ok(SettleJobOutcome::Settled(Box::new(JobSettled {
                effective_status: applied.effective_status,
                cancelled_siblings: applied.cancelled_siblings,
                scheduling,
                queue_nonempty,
                newly_terminal_success: applied.newly_terminal_success,
                live_log_key: applied.live_log_key,
                queue_len,
                next_runs_on,
            })))
        })
    }

    /// `cancel_run` (pg cancel_run_tx over lite's settle::cancel_run_inner):
    /// `NotFound` for an unknown run; otherwise every non-terminal job is
    /// cancelled, in-flight attempts get a queued cancellation, concurrency
    /// releases, expandable node requests retire.
    pub(crate) async fn cancel_run(
        &self,
        run_id: RunId,
        reason: Option<String>,
    ) -> Result<CancelOutcome, ControlError> {
        let _ = reason;
        self.write(|tx| {
            let exists = tx
                .prepare_cached("SELECT EXISTS(SELECT 1 FROM runs WHERE run_id = ?1)")
                .map_err(db)?
                .query_row([codec::run_key(run_id)], |row| row.get::<_, bool>(0))
                .map_err(db)?;
            if !exists {
                return Err(ControlError::NotFound(format!("run {run_id}")));
            }
            let cancellations = settle::cancel_run_inner(tx, self, run_id)?;
            let cancelled_jobs: Vec<JobId> = {
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT job_id FROM jobs WHERE run_id = ?1 \
                         AND status = 'cancelled' ORDER BY job_id",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([codec::run_key(run_id)], |row| {
                        Ok(JobId(row.get::<_, String>(0)?))
                    })
                    .map_err(db)?;
                rows.collect::<Result<Vec<_>, _>>().map_err(db)?
            };
            let (queue_depth, next_runs_on, queue_nonempty) = queue_gauges(tx)?;
            let record = jobs::run_record(tx, run_id)?;
            Ok(CancelOutcome {
                cancellations,
                run_status: record.as_ref().map(|r| r.status),
                queue_nonempty,
                record,
                cancelled_jobs,
                queue_depth,
                next_runs_on,
            })
        })
    }

    /// `cancel_job` (pg cancel_job_tx over lite's settle::cancel_job_inner):
    /// cancel one job and its subtree, queueing a cancellation for the
    /// in-flight attempt.
    pub(crate) async fn cancel_job(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<CancelOutcome, ControlError> {
        let job_id = job_id.clone();
        self.write(|tx| {
            let cancellations = settle::cancel_job_inner(tx, self, run_id, &job_id)?;
            let cancelled_jobs: Vec<JobId> = {
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT job_id FROM jobs WHERE run_id = ?1 \
                         AND status = 'cancelled' ORDER BY job_id",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([codec::run_key(run_id)], |row| {
                        Ok(JobId(row.get::<_, String>(0)?))
                    })
                    .map_err(db)?;
                rows.collect::<Result<Vec<_>, _>>().map_err(db)?
            };
            let (queue_depth, next_runs_on, queue_nonempty) = queue_gauges(tx)?;
            let record = jobs::run_record(tx, run_id)?;
            Ok(CancelOutcome {
                cancellations,
                run_status: record.as_ref().map(|r| r.status),
                queue_nonempty,
                record,
                cancelled_jobs,
                queue_depth,
                next_runs_on,
            })
        })
    }

    /// `renew_request` (pg runners.rs): renew a claimed attempt's lease by
    /// request id for its recorded owner. `NotFound` for an unknown
    /// request, `Stale` when `runner_id` is not the owner.
    pub(crate) async fn renew_request(
        &self,
        request_id: i64,
        runner_id: i64,
    ) -> Result<TaskAgentJobRequestRecord, ControlError> {
        self.write(|tx| {
            let owner: Option<Option<i64>> = tx
                .prepare_cached("SELECT runner_id FROM job_requests WHERE request_id = ?1")
                .map_err(db)?
                .query_row([request_id], |row| row.get(0))
                .optional()
                .map_err(db)?;
            let Some(owner) = owner else {
                return Err(ControlError::NotFound(format!("request {request_id}")));
            };
            if owner != Some(runner_id) {
                return Err(ControlError::Stale(format!(
                    "request {request_id} not owned by runner {runner_id}"
                )));
            }
            let expires_at =
                codec::parse_lease(&crate::distributed_task::agent_request_locked_until())?;
            let now = now_us();
            tx.prepare_cached(
                "INSERT INTO job_leases (request_id, runner_id, expires_at, renewed_at) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT (request_id) DO UPDATE SET \
                     expires_at = excluded.expires_at, renewed_at = excluded.renewed_at",
            )
            .map_err(db)?
            .execute(params![request_id, runner_id, expires_at, now])
            .map_err(db)?;
            let record = tx
                .prepare_cached(&format!(
                    "SELECT {} FROM {} WHERE q.request_id = ?1",
                    requests::RECORD_COLUMNS,
                    requests::RECORD_FROM
                ))
                .map_err(db)?
                .query_row([request_id], requests::record_row)
                .map_err(db)?;
            Ok(record)
        })
    }

    /// `release_request` (pg runners.rs): release an interrupted claim so
    /// the request can be redelivered — clears owner/session/start/renew
    /// stamps and drops the lease row; an unknown or settled request is a
    /// no-op.
    pub(crate) async fn release_request(&self, request_id: i64) -> Result<(), ControlError> {
        self.write(|tx| {
            tx.prepare_cached(
                "UPDATE job_requests SET runner_id = NULL, session_id = NULL, \
                 started_at = NULL, timeout_triggered = 0 \
                 WHERE request_id = ?1 AND result IS NULL",
            )
            .map_err(db)?
            .execute([request_id])
            .map_err(db)?;
            tx.prepare_cached("DELETE FROM job_leases WHERE request_id = ?1")
                .map_err(db)?
                .execute([request_id])
                .map_err(db)?;
            Ok(())
        })
    }
}
