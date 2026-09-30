//! Reaper commands: `reap_inputs` (the tick's read-only inputs),
//! `reap_sweep` (starvation, timeout, lease-expiry writes over the due
//! runs), `sweep_stale_bindings` (dispatch-intent hygiene), and
//! `status_inputs` (operational status reads).
//!
//! Reaper sweeps and status reads over the agreed tables. Starvation
//! first-seen marks are node-local (decisions-5 B2): the caller passes them
//! in `ReapSweep::first_seen`; nothing about them is persisted.

use super::codec::{self, now_us};
use super::jobs;
use super::{LiteBackend, db};
use crate::control::logic::{self, StarvationCandidate, StarvationVerdict};
use crate::control::types::*;
use preloop_gha_protocol::{JobId, RunId, azdo};
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::BTreeSet;

/// One runner's `(labels, group)` for starvation matching.
fn runner_label_sets(tx: &Transaction<'_>) -> Result<Vec<Vec<String>>, ControlError> {
    let mut stmt = tx
        .prepare_cached("SELECT labels FROM runners ORDER BY runner_id")
        .map_err(db)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(db)?;
    rows.map(|row| {
        row.map_err(db)
            .map(|labels| serde_json::from_str(&labels).unwrap_or_default())
    })
    .collect()
}

/// Queue a `JobCancellation` for a job's unfinished attempt: the
/// cancellation row plus the session message when the attempt is bound to
/// a session.
fn queue_job_cancellation(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<bool, ControlError> {
    let run = codec::run_key(run_id);
    let attempt: Option<(i64, Option<String>, Option<String>)> = tx
        .prepare_cached(
            "SELECT request_id, session_id, agent_job_id FROM job_requests \
             WHERE run_id = ?1 AND job_id = ?2 AND result IS NULL \
             ORDER BY request_id DESC LIMIT 1",
        )
        .map_err(db)?
        .query_row(params![run, job_id.0], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .optional()
        .map_err(db)?;
    let Some((request_id, session, agent)) = attempt else {
        return Ok(false);
    };
    tx.prepare_cached(
        "INSERT INTO job_cancellations (request_id, reason, requested_at) \
         VALUES (?1,'timeout',?2) ON CONFLICT DO NOTHING",
    )
    .map_err(db)?
    .execute(params![request_id, now_us()])
    .map_err(db)?;
    if let (Some(session), Some(agent)) = (session, agent) {
        let agent = uuid::Uuid::parse_str(&agent).unwrap_or_default();
        let body = crate::concurrency::job_cancel_body(agent);
        tx.prepare_cached(
            "INSERT INTO session_messages (session_id, message_type, request_id, body) \
             VALUES (?1,?2,?3,?4)",
        )
        .map_err(db)?
        .execute(params![
            session,
            azdo::message_type::JOB_CANCELLED,
            request_id,
            body
        ])
        .map_err(db)?;
    }
    Ok(true)
}

/// Fail one ready job for starvation: leave the ready queue, land the
/// terminal row, recompute the run status (no dependent promotion, no
/// concurrency release — the job never held a gate).
fn starve_job(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    run_id: RunId,
    job_id: &JobId,
) -> Result<(), ControlError> {
    let run = codec::run_key(run_id);
    tx.prepare_cached(
        "UPDATE jobs SET status = 'failure', queue_state = 'none', \
         completed_at = ?3 \
         WHERE run_id = ?1 AND job_id = ?2 AND queue_state = 'ready'",
    )
    .map_err(db)?
    .execute(params![run, job_id.0, now_us()])
    .map_err(db)?;
    tx.prepare_cached("DELETE FROM job_assignments WHERE run_id = ?1 AND job_id = ?2")
        .map_err(db)?
        .execute(params![run, job_id.0])
        .map_err(db)?;
    tx.prepare_cached("DELETE FROM provision_requests WHERE run_id = ?1 AND job_id = ?2")
        .map_err(db)?
        .execute(params![run, job_id.0])
        .map_err(db)?;
    let summary = jobs::summarize_run_row(tx, run_id)?;
    if summary.is_terminal() {
        super::settle::release_concurrency_for_run(tx, backend, run_id)?;
    }
    Ok(())
}

impl LiteBackend {
    /// One reaper tick's inputs (`reap_inputs`): in-flight attempts with
    /// the job's `timeout-minutes`, the ready queue in claim order, every
    /// runner's labels, whether any dispatch binding exists, and the
    /// stale-session / session-less registrations past the liveness
    /// timeout.
    pub(crate) async fn reap_inputs(&self) -> Result<ReapInputs, ControlError> {
        let (_pool, _require, liveness) = self.config();
        let cutoff = now_us() - liveness.as_micros() as i64;
        self.read(move |tx| {
            let mut inputs = ReapInputs::default();
            {
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT q.request_id, q.run_id, q.job_id, q.started_at, \
                                l.renewed_at, q.timeout_triggered, \
                                CAST(json_extract(m.message_template, '$.jobTimeout') AS INTEGER) \
                         FROM job_requests q \
                         LEFT JOIN job_leases l ON l.request_id = q.request_id \
                         LEFT JOIN job_messages m ON m.run_id = q.run_id AND m.job_id = q.job_id \
                         WHERE q.result IS NULL ORDER BY q.request_id",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok(ActiveRequest {
                            request_id: row.get(0)?,
                            run_id: codec::run_id(&row.get::<_, String>(1)?),
                            job_id: JobId(row.get(2)?),
                            started_at: row.get::<_, Option<i64>>(3)?.map(codec::us_to_system),
                            last_renewed_at: row.get::<_, Option<i64>>(4)?.map(codec::us_to_system),
                            timeout_triggered: row.get::<_, i64>(5)? != 0,
                            job_timeout_s: row.get(6)?,
                        })
                    })
                    .map_err(db)?;
                for row in rows {
                    inputs.active.push(row.map_err(db)?);
                }
            }
            {
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT run_id, job_id, runs_on, enqueued_at \
                         FROM jobs WHERE queue_state = 'ready' \
                         ORDER BY priority DESC, run_order, job_order, run_id, job_id",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok(ReadyRow {
                            run_id: codec::run_id(&row.get::<_, String>(0)?),
                            job_id: JobId(row.get(1)?),
                            runs_on: serde_json::from_str(&row.get::<_, String>(2)?)
                                .unwrap_or_default(),
                            enqueued_at_unix_nanos: row.get::<_, Option<i64>>(3)?.unwrap_or(0)
                                * 1000,
                        })
                    })
                    .map_err(db)?;
                for row in rows {
                    inputs.ready.push(row.map_err(db)?);
                }
            }
            inputs.runner_labels = runner_label_sets(tx)?;
            inputs.has_bindings = tx
                .prepare_cached(
                    "SELECT EXISTS(SELECT 1 FROM job_assignments) \
                     OR EXISTS(SELECT 1 FROM provision_requests)",
                )
                .map_err(db)?
                .query_row([], |row| row.get(0))
                .map_err(db)?;
            {
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT DISTINCT runner_id FROM runner_sessions \
                         WHERE runner_id IS NOT NULL AND last_seen_at < ?1",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([cutoff], |row| row.get::<_, i64>(0))
                    .map_err(db)?;
                for row in rows {
                    inputs.stale_runners.insert(row.map_err(db)?);
                }
            }
            {
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT r.runner_id FROM runners r WHERE r.registered_at < ?1 \
                         AND NOT EXISTS (SELECT 1 FROM runner_sessions s \
                                         WHERE s.runner_id = r.runner_id)",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([cutoff], |row| row.get::<_, i64>(0))
                    .map_err(db)?;
                for row in rows {
                    inputs.phantom_runners.insert(row.map_err(db)?);
                }
            }
            Ok(inputs)
        })
    }

    /// `reap_sweep`: starvation verdicts
    /// over the caller's node-local marks, job `timeout-minutes` enforcement (queued
    /// cancellations), lease-expiry failure. Rows outside `sweep.runs` are
    /// never written.
    pub(crate) async fn reap_sweep(
        &self,
        sweep: ReapSweep,
    ) -> Result<ReapSweepOutcome, ControlError> {
        if sweep.runs.is_empty() {
            return Ok(ReapSweepOutcome::default());
        }
        self.write(move |tx| {
            let ReapSweep {
                now,
                runs,
                ready,
                active,
                paused,
                pool_preparing,
                warm_window_open,
                first_seen,
            } = sweep;
            let runner_labels = runner_label_sets(tx)?;
            // Only runs in scope may change.
            let in_scope = |run_id: RunId| runs.contains(&run_id);
            // ── Starvation ────────────────────────────────────────────
            // Marks of jobs no longer in the ready snapshot clear (their
            // claim took them off the queue).
            let mut starved = Vec::new();
            for job in &ready {
                if !in_scope(job.run_id) {
                    continue;
                }
                let candidate = StarvationCandidate {
                    runs_on: &job.runs_on,
                    enqueued_at: std::time::UNIX_EPOCH
                        + std::time::Duration::from_nanos(job.enqueued_at_unix_nanos as u64),
                    first_seen: first_seen.get(&(job.run_id, job.job_id.clone())).copied(),
                    any_runner_matches: runner_labels.iter().any(|labels| {
                        crate::runtime_scheduling::job_matches_runner(&job.runs_on, labels)
                    }),
                };
                match logic::starvation_verdict(&candidate, now, pool_preparing, warm_window_open) {
                    // Marks are node-local (decisions-5 B2): the caller
                    // owns stamping/clearing; nothing persists here.
                    StarvationVerdict::ClearMark | StarvationVerdict::Mark { .. } => {}
                    StarvationVerdict::Starve { reason, grace } => {
                        tracing::warn!(
                            run_id = %job.run_id,
                            job_id = %job.job_id.0,
                            labels = ?job.runs_on,
                            "starving queued job failed after {}s without a matching runner",
                            grace.as_secs()
                        );
                        starve_job(tx, self, job.run_id, &job.job_id)?;
                        starved.push((job.run_id, job.job_id.clone(), reason));
                    }
                }
            }
            // ── Timeouts + lease expiry ────────────────────────────────
            let mut cancellations = 0usize;
            let mut expired = Vec::new();
            for request in &active {
                if !in_scope(request.run_id) {
                    continue;
                }
                let (request_id, run_id, job_id) =
                    (request.request_id, request.run_id, request.job_id.clone());
                if let Some(started_at) = request.started_at
                    && !request.timeout_triggered
                {
                    let pause = paused.get(&request_id).copied().unwrap_or_default();
                    let elapsed = now
                        .duration_since(started_at)
                        .unwrap_or_default()
                        .saturating_sub(pause);
                    let job_timeout = request.job_timeout_s.unwrap_or(21600).max(0) as u64;
                    if elapsed >= std::time::Duration::from_secs(job_timeout) {
                        tracing::info!(
                            %run_id,
                            %job_id,
                            request_id,
                            "Job timed out after {}s",
                            job_timeout
                        );
                        tx.prepare_cached(
                            "UPDATE job_requests SET timeout_triggered = 1 \
                                 WHERE request_id = ?1",
                        )
                        .map_err(db)?
                        .execute([request_id])
                        .map_err(db)?;
                        if queue_job_cancellation(tx, run_id, &job_id)? {
                            cancellations += 1;
                        }
                    }
                }
                if let Some(last_renewed_at) = request.last_renewed_at {
                    let elapsed = now.duration_since(last_renewed_at).unwrap_or_default();
                    if elapsed
                        >= std::time::Duration::from_secs(
                            crate::distributed_task::JOB_LEASE_SECONDS,
                        )
                    {
                        tracing::info!(
                            %run_id,
                            %job_id,
                            request_id,
                            "Runner lease expired (last renewed {}s ago). Marking job as failed.",
                            elapsed.as_secs()
                        );
                        let agent: Option<String> = tx
                            .prepare_cached(
                                "SELECT agent_job_id FROM job_requests \
                                 WHERE request_id = ?1",
                            )
                            .map_err(db)?
                            .query_row([request_id], |row| row.get(0))
                            .optional()
                            .map_err(db)?;
                        tx.prepare_cached(
                            "UPDATE job_requests SET result = 'failure', \
                             finished_at = ?2, session_id = NULL \
                             WHERE request_id = ?1 AND result IS NULL",
                        )
                        .map_err(db)?
                        .execute(params![request_id, now_us()])
                        .map_err(db)?;
                        tx.prepare_cached("DELETE FROM job_leases WHERE request_id = ?1")
                            .map_err(db)?
                            .execute([request_id])
                            .map_err(db)?;
                        expired.push(ExpiredLease {
                            request_id,
                            run_id,
                            job_id,
                            agent_job_id: agent.and_then(|a| uuid::Uuid::parse_str(&a).ok()),
                        });
                    }
                }
            }
            Ok(ReapSweepOutcome {
                cancellations,
                expired,
                starved,
            })
        })
    }

    /// `sweep_stale_bindings`: drop or release stale job → runner bindings.
    /// Pool assignments on: delete bindings of jobs no longer ready.
    /// Both modes off: delete bindings past the assignment TTL. Otherwise
    /// release bindings past the claim-binding TTL or naming a dead
    /// runner — the job keeps its row, loses the runner, and re-enters the
    /// provision queue.
    pub(crate) async fn sweep_stale_bindings(&self) -> Result<usize, ControlError> {
        let (pool_on, require_on, _liveness) = self.config();
        self.write(move |tx| {
            let now = now_us();
            let assignment_cutoff = now - crate::control::logic::ASSIGNMENT_TTL.as_micros() as i64;
            let binding_cutoff = now - crate::control::logic::CLAIM_BINDING_TTL.as_micros() as i64;
            let mut swept = 0usize;
            if pool_on {
                for sql in [
                    "DELETE FROM job_assignments WHERE NOT EXISTS (\
                     SELECT 1 FROM jobs j WHERE j.run_id = job_assignments.run_id \
                     AND j.job_id = job_assignments.job_id AND j.queue_state = 'ready')",
                    "DELETE FROM provision_requests WHERE NOT EXISTS (\
                     SELECT 1 FROM jobs j WHERE j.run_id = provision_requests.run_id \
                     AND j.job_id = provision_requests.job_id AND j.queue_state = 'ready')",
                ] {
                    swept += tx.execute(sql, []).map_err(db)?;
                }
            }
            if !require_on && !pool_on {
                swept += tx
                    .prepare_cached("DELETE FROM job_assignments WHERE assigned_at < ?1")
                    .map_err(db)?
                    .execute([assignment_cutoff])
                    .map_err(db)?;
                swept += tx
                    .prepare_cached("DELETE FROM provision_requests WHERE requested_at < ?1")
                    .map_err(db)?
                    .execute([assignment_cutoff])
                    .map_err(db)?;
            } else {
                // Release (not delete) stale bindings: runner_id -> NULL,
                // and the job re-enters the provision queue.
                let stale: Vec<(String, String)> = {
                    let mut stmt = tx
                        .prepare_cached(
                            "UPDATE job_assignments SET runner_id = NULL \
                             WHERE runner_id IS NOT NULL AND (assigned_at < ?1 \
                                OR NOT EXISTS (SELECT 1 FROM runners r \
                                               WHERE r.runner_id = job_assignments.runner_id)) \
                             RETURNING run_id, job_id",
                        )
                        .map_err(db)?;
                    let rows = stmt
                        .query_map([binding_cutoff], |row| {
                            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                        })
                        .map_err(db)?;
                    rows.collect::<Result<Vec<_>, _>>().map_err(db)?
                };
                swept += stale.len();
                for (run, job) in stale {
                    tx.prepare_cached(
                        "INSERT INTO provision_requests (run_id, job_id, namespace_id, \
                         pool_key, labels, requested_at) \
                         SELECT j.run_id, j.job_id, j.namespace_id, j.pool_key, \
                         j.runs_on, ?3 FROM jobs j \
                         WHERE j.run_id = ?1 AND j.job_id = ?2 \
                         ON CONFLICT (run_id, job_id) DO UPDATE SET \
                             requested_at = excluded.requested_at",
                    )
                    .map_err(db)?
                    .execute(params![run, job, now])
                    .map_err(db)?;
                }
            }
            Ok(swept)
        })
    }

    /// `status_inputs`: the
    /// operational status read — run counts, queue depths, runner
    /// activity, concurrency-group gauges.
    pub(crate) async fn status_inputs(
        &self,
        stale_after: std::time::Duration,
    ) -> Result<StatusInputs, ControlError> {
        self.read(move |tx| {
            let mut inputs = StatusInputs::default();
            // Coarse run counts.
            {
                let mut stmt = tx
                    .prepare_cached("SELECT status, COUNT(*) FROM runs GROUP BY status")
                    .map_err(db)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?))
                    })
                    .map_err(db)?;
                for row in rows {
                    let (status, count) = row.map_err(db)?;
                    match status.as_str() {
                        "queued" => inputs.runs_queued += count,
                        "completed" => inputs.runs_completed += count,
                        _ => inputs.runs_in_progress += count,
                    }
                }
            }
            inputs.queue_len = jobs::ready_count(tx)?;
            inputs.pending_jobs_len = tx
                .prepare_cached("SELECT COUNT(*) FROM jobs WHERE queue_state = 'blocked'")
                .map_err(db)?
                .query_row([], |row| row.get::<_, u32>(0))
                .map_err(db)? as usize;
            inputs.pending_expansions_len = tx
                .prepare_cached("SELECT COUNT(*) FROM jobs WHERE queue_state = 'pending_expansion'")
                .map_err(db)?
                .query_row([], |row| row.get::<_, u32>(0))
                .map_err(db)? as usize;
            inputs.expanding_len = tx
                .prepare_cached("SELECT COUNT(*) FROM jobs WHERE queue_state = 'expanding'")
                .map_err(db)?
                .query_row([], |row| row.get::<_, u32>(0))
                .map_err(db)? as usize;
            inputs.concurrency_blocked = tx
                .prepare_cached("SELECT COUNT(*) FROM jobs WHERE queue_state = 'held'")
                .map_err(db)?
                .query_row([], |row| row.get::<_, u32>(0))
                .map_err(db)?;
            // Active runs + bound runner names (assignment or unfinished
            // owned attempt).
            let mut runner_names = std::collections::BTreeMap::new();
            {
                let mut stmt = tx
                    .prepare_cached("SELECT runner_id, name FROM runners")
                    .map_err(db)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                    })
                    .map_err(db)?;
                for row in rows {
                    let (id, name) = row.map_err(db)?;
                    runner_names.insert(id, name);
                }
            }
            {
                let mut by_run: std::collections::BTreeMap<String, BTreeSet<i64>> =
                    std::collections::BTreeMap::new();
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT run_id, runner_id FROM job_assignments \
                         WHERE runner_id IS NOT NULL",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                    })
                    .map_err(db)?;
                for row in rows {
                    let (run, runner) = row.map_err(db)?;
                    by_run.entry(run).or_default().insert(runner);
                }
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT run_id, runner_id FROM job_requests \
                         WHERE result IS NULL AND runner_id IS NOT NULL",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                    })
                    .map_err(db)?;
                for row in rows {
                    let (run, runner) = row.map_err(db)?;
                    by_run.entry(run).or_default().insert(runner);
                }
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT run_id, workflow_path, status, event, started_at \
                         FROM runs WHERE status <> 'completed'",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, Option<i64>>(4)?,
                        ))
                    })
                    .map_err(db)?;
                for row in rows {
                    let (run_id, workflow, status, event, started_at) = row.map_err(db)?;
                    let assigned_runners = by_run
                        .get(&run_id)
                        .into_iter()
                        .flatten()
                        .filter_map(|id| runner_names.get(id).cloned())
                        .collect();
                    inputs
                        .active_runs
                        .push(preloop_observability::status::ActiveRunSnapshot {
                            run_id: run_id.clone(),
                            workflow,
                            status,
                            event,
                            started_at: started_at.and_then(codec::us_to_utc),
                            assigned_runners,
                        });
                }
                inputs.active_runs.sort_by(|left, right| {
                    right
                        .started_at
                        .cmp(&left.started_at)
                        .then_with(|| right.run_id.cmp(&left.run_id))
                });
            }
            // Orphaned `in_progress` runs: nothing queued, claimed, or
            // blocked and no runner bound.
            inputs.orphaned_run_ids = inputs
                .active_runs
                .iter()
                .filter(|run| run.status == "in_progress" && run.assigned_runners.is_empty())
                .filter(|run| {
                    let Ok(uuid) = run.run_id.parse::<uuid::Uuid>() else {
                        return false;
                    };
                    let run_key = codec::run_key(RunId(uuid));
                    tx.prepare_cached(
                        "SELECT NOT EXISTS(SELECT 1 FROM jobs j WHERE j.run_id = ?1 \
                         AND j.queue_state IN ('ready','claimed','blocked'))",
                    )
                    .and_then(|mut s| s.query_row([&run_key], |row| row.get::<_, bool>(0)))
                    .unwrap_or(false)
                })
                .map(|run| run.run_id.clone())
                .collect();
            // Runner/session tallies.
            inputs.registered = tx
                .prepare_cached("SELECT COUNT(*) FROM runners")
                .map_err(db)?
                .query_row([], |row| row.get::<_, u32>(0))
                .map_err(db)?;
            inputs.sessions = tx
                .prepare_cached("SELECT COUNT(*) FROM runner_sessions")
                .map_err(db)?
                .query_row([], |row| row.get::<_, u32>(0))
                .map_err(db)?;
            // busy = an owned session holds an active request; stale =
            // every owned session stopped polling; idle = ≥1 session and
            // neither. A session never seen is not stale.
            let stale_cutoff = now_us() - stale_after.as_micros() as i64;
            {
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT DISTINCT runner_id FROM runner_sessions \
                         WHERE runner_id IS NOT NULL",
                    )
                    .map_err(db)?;
                let owners = stmt
                    .query_map([], |row| row.get::<_, i64>(0))
                    .map_err(db)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(db)?;
                for runner_id in owners {
                    let busy = tx
                        .prepare_cached(
                            "SELECT EXISTS(SELECT 1 FROM job_requests q \
                             JOIN runner_sessions s ON s.session_id = q.session_id \
                             WHERE s.runner_id = ?1 AND q.result IS NULL)",
                        )
                        .map_err(db)?
                        .query_row([runner_id], |row| row.get::<_, bool>(0))
                        .map_err(db)?;
                    let stale = tx
                        .prepare_cached(
                            "SELECT EXISTS(SELECT 1 FROM runner_sessions s \
                             WHERE s.runner_id = ?1) AND NOT EXISTS(\
                             SELECT 1 FROM runner_sessions s WHERE s.runner_id = ?1 \
                             AND (s.last_seen_at IS NULL OR s.last_seen_at >= ?2))",
                        )
                        .map_err(db)?
                        .query_row(params![runner_id, stale_cutoff], |row| {
                            row.get::<_, bool>(0)
                        })
                        .map_err(db)?;
                    if busy {
                        inputs.runner_busy += 1;
                        // Pool gauge: only the pool's own machines count. A
                        // runner row that is gone cannot be pool-proven.
                        inputs.pool_busy += match tx
                            .prepare_cached("SELECT pool_proven FROM runners WHERE runner_id = ?1")
                            .map_err(db)?
                            .query_row([runner_id], |row| row.get::<_, i64>(0))
                        {
                            Ok(flag) => u32::from(flag != 0),
                            Err(rusqlite::Error::QueryReturnedNoRows) => 0,
                            Err(error) => return Err(db(error)),
                        };
                    }
                    if stale {
                        inputs.runner_stale += 1;
                    }
                    if !busy && !stale {
                        inputs.runner_idle += 1;
                    }
                }
            }
            // Live runner -> job pairings from unfinished owned attempts.
            {
                let now = std::time::SystemTime::now();
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT q.runner_id, q.run_id, q.job_id, q.started_at \
                         FROM job_requests q \
                         WHERE q.result IS NULL AND q.runner_id IS NOT NULL \
                         ORDER BY q.runner_id",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, Option<i64>>(3)?,
                        ))
                    })
                    .map_err(db)?;
                for row in rows {
                    let (runner_id, run_id, job_id, started_at) = row.map_err(db)?;
                    inputs.runner_assignments.push(
                        preloop_observability::status::RunnerAssignment {
                            runner_id,
                            run_id,
                            job_id,
                            assigned_seconds_ago: started_at
                                .and_then(|us| now.duration_since(codec::us_to_system(us)).ok())
                                .map(|age| age.as_secs_f64())
                                .unwrap_or(0.0),
                        },
                    );
                }
            }
            // Oldest ready job.
            {
                let oldest: Option<(String, String, i64)> = tx
                    .prepare_cached(
                        "SELECT run_id, job_id, enqueued_at FROM jobs \
                         WHERE queue_state = 'ready' AND enqueued_at IS NOT NULL \
                         ORDER BY enqueued_at LIMIT 1",
                    )
                    .map_err(db)?
                    .query_row([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                    .optional()
                    .map_err(db)?;
                if let Some((run_id, job_id, enqueued_at)) = oldest {
                    inputs.oldest_ready_seconds =
                        Some((now_us().saturating_sub(enqueued_at)) as f64 / 1_000_000.0);
                    inputs.oldest_ready_run_id = Some(run_id);
                    inputs.oldest_ready_job_id = Some(job_id);
                }
            }
            // Ready jobs' requirements, in queue order.
            {
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT runs_on, runner_group FROM jobs WHERE queue_state = 'ready' \
                         ORDER BY pool_key, priority DESC, run_order, job_order",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            serde_json::from_str(&row.get::<_, String>(0)?).unwrap_or_default(),
                            row.get::<_, Option<String>>(1)?,
                        ))
                    })
                    .map_err(db)?;
                for row in rows {
                    inputs.queue_runner_reqs.push(row.map_err(db)?);
                }
            }
            // Runner capabilities (for the pool-status page).
            {
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT labels, runner_group_id, runner_group_name FROM runners \
                         ORDER BY runner_id",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok(crate::models::RunnerCapabilities {
                            known: true,
                            labels: serde_json::from_str(&row.get::<_, String>(0)?)
                                .unwrap_or_default(),
                            runner_group_id: row.get(1)?,
                            runner_group_name: row.get(2)?,
                        })
                    })
                    .map_err(db)?;
                for row in rows {
                    inputs.runner_caps.push(row.map_err(db)?);
                }
            }
            // Concurrency-group gauges.
            {
                let (active, contended): (u32, Option<u32>) = tx
                    .prepare_cached(
                        "SELECT COUNT(*), \
                         SUM(CASE WHEN EXISTS(SELECT 1 FROM concurrency_waits w \
                              WHERE w.namespace_id = h.namespace_id \
                              AND w.repository = h.repository \
                              AND w.group_name = h.group_name) THEN 1 ELSE 0 END) \
                         FROM concurrency_holds h \
                         WHERE EXISTS(SELECT 1 FROM concurrency_waits w \
                              WHERE w.namespace_id = h.namespace_id \
                              AND w.repository = h.repository \
                              AND w.group_name = h.group_name) \
                         OR h.holder_run_id IS NOT NULL OR h.holder_job_id IS NOT NULL \
                         OR h.holder_jobset_id IS NOT NULL",
                    )
                    .map_err(db)?
                    .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
                    .map_err(db)?;
                inputs.concurrency_groups_active = active;
                inputs.concurrency_groups_contended = contended.unwrap_or(0);
                let (pending, deepest): (u32, Option<u32>) = tx
                    .prepare_cached(
                        "SELECT COUNT(*), MAX(cnt) FROM (\
                         SELECT COUNT(*) AS cnt FROM concurrency_waits \
                         GROUP BY namespace_id, repository, group_name)",
                    )
                    .map_err(db)?
                    .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
                    .map_err(db)?;
                inputs.concurrency_pending_holders = pending;
                inputs.concurrency_deepest_group_pending = deepest.unwrap_or(0);
            }
            inputs.released_bindings = 0;
            Ok(inputs)
        })
    }
}
