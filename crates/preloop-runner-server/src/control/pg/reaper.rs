//! Reaper commands and the operational status snapshot.
//!
//! `reap_sweep` is the authoritative half of one reaper tick: the caller
//! (`bootstrap::reap_once`) reads `reap_inputs`, decides which runs have
//! something due, and hands the sweep its verdicts' inputs. Every write here
//! is conditional — a second node sweeping the same tick loses the
//! transition and reports nothing for that row (contract rule 5: runs are
//! locked in ascending id order).
//!
//! `status_inputs` is read-only: one snapshot query per counter group.

use super::codec::{self, now_us, ts, us};
use super::dispatch::{
    Retirement, clear_assignment, emit_outbox, enqueue_cancellation_job,
    release_concurrency_for_job, release_concurrency_for_run, retire_node_requests,
    summarize_run_tx,
};
use super::{PgBackend, db};
use crate::control::logic::{StarvationCandidate, StarvationVerdict, starvation_verdict};
use crate::control::types::{
    ControlError, ExpiredLease, ReapSweep, ReapSweepOutcome, StarvedJob, StatusInputs,
};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};

impl PgBackend {
    /// `reap_sweep`: starvation verdicts for unmatched ready jobs
    /// (marks are node-local, passed in as
    /// `ReapSweep::first_seen`; unmarked rows fall back to `enqueued_at`), timeout
    /// `job_cancellations` markers, and expired-lease settlements, all
    /// conditional on the row still being unsettled/ready. Only writes to
    /// `sweep.runs`' runs.
    ///
    /// Statements per verdict: `UPDATE jobs SET status='failure',
    /// queue_state='none' .. WHERE queue_state='ready'` (the
    /// one-writer-wins transition), request settle, assignment drop, gate
    /// release, run resummary; `UPDATE job_requests SET
    /// timeout_triggered=true WHERE result IS NULL` + a deduplicated
    /// `job_cancellations` row; `UPDATE job_requests SET result='failure'
    /// .. WHERE result IS NULL RETURNING agent_job_id` + `settle_request`
    /// cleanup.
    pub(super) async fn reap_sweep(
        &self,
        sweep: ReapSweep,
    ) -> Result<ReapSweepOutcome, ControlError> {
        use std::time::SystemTime;
        let ReapSweep {
            now,
            runs,
            ready,
            active,
            paused,
            pool_preparing,
            booted_at,
            pool_labels,
            first_seen,
        } = sweep;
        let now_us = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64;
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        // Runner label sets for `any_runner_matches` — one read per sweep,
        // matching the old in-memory scan.
        let runner_labels: Vec<Vec<String>> = tx
            .query("SELECT labels::text FROM runners", &[])
            .await
            .map_err(db)?
            .iter()
            .map(|row| codec::from_json::<Vec<String>>(row.get(0)).unwrap_or_default())
            .collect();
        let mut outcome = ReapSweepOutcome::default();

        // ── Starvation sweep ───────────────────────────────────────────
        for job in &ready {
            if !runs.contains(&job.run_id) {
                continue;
            }
            let candidate = StarvationCandidate {
                runs_on: &job.runs_on,
                enqueued_at: SystemTime::UNIX_EPOCH
                    + std::time::Duration::from_nanos(job.enqueued_at_unix_nanos as u64),
                // No durable first-seen column: the mark
                // lives in the caller's memory (`ReapSweep::first_seen`);
                // unmarked rows fall back to the enqueue instant or boot.
                first_seen: first_seen.get(&(job.run_id, job.job_id.clone())).copied(),
                any_runner_matches: runner_labels.iter().any(|labels| {
                    crate::runtime_scheduling::job_matches_runner(&job.runs_on, labels)
                }),
            };
            let verdict =
                starvation_verdict(&candidate, now, pool_preparing, booted_at, &pool_labels);
            let (reason, unschedulable) = match verdict {
                StarvationVerdict::ClearMark | StarvationVerdict::Mark { .. } => continue,
                StarvationVerdict::Starve { reason, grace } => {
                    tracing::warn!(
                        run_id = %job.run_id,
                        job_id = %job.job_id.0,
                        labels = ?job.runs_on,
                        "starving queued job failed after {}s without a matching runner",
                        grace.as_secs()
                    );
                    (reason, false)
                }
                StarvationVerdict::Unschedulable { reason } => {
                    tracing::warn!(
                        run_id = %job.run_id,
                        job_id = %job.job_id.0,
                        labels = ?job.runs_on,
                        pool_labels = ?pool_labels,
                        "queued job failed fast: runs-on unsatisfiable by runner pool"
                    );
                    (reason, true)
                }
            };
            {
                // The conditional UPDATE is the multi-node guard: whoever
                // turns the row terminal first owns the failure.
                let changed = tx
                    .execute(
                        concat!(
                            "UPDATE jobs SET status='failure', queue_state='none', \
                             completed_at=",
                            ts!("$3"),
                            ", claimed_by_runner_id=NULL, claimed_at=NULL \
                             WHERE run_id=$1::text::uuid AND job_id=$2 \
                             AND queue_state='ready'"
                        ),
                        &[&job.run_id.0.to_string(), &job.job_id.0, &now_us],
                    )
                    .await
                    .map_err(db)?;
                if changed == 0 {
                    continue;
                }
                retire_node_requests(
                    &tx,
                    job.run_id,
                    &job.job_id,
                    Retirement::Settle(ExecutionStatus::Failure),
                )
                .await?;
                clear_assignment(&tx, job.run_id, &job.job_id).await?;
                release_concurrency_for_job(self, &tx, job.run_id, &job.job_id).await?;
                summarize_run_tx(&tx, job.run_id).await?;
                emit_outbox(
                    &tx,
                    Some(job.run_id),
                    "job.completed.v1",
                    serde_json::json!({"job_id": job.job_id.0, "status": "failure"}),
                )
                .await?;
                let terminal: bool = tx
                    .query_one(
                        "SELECT status = 'completed' FROM runs WHERE run_id = $1::text::uuid",
                        &[&job.run_id.0.to_string()],
                    )
                    .await
                    .map_err(db)?
                    .get(0);
                if terminal {
                    emit_outbox(
                        &tx,
                        Some(job.run_id),
                        "run.completed.v1",
                        serde_json::json!({"conclusion": "failure"}),
                    )
                    .await?;
                }
                outcome.starved.push(StarvedJob {
                    run_id: job.run_id,
                    job_id: job.job_id.clone(),
                    reason,
                    unschedulable,
                });
            }
        }

        // ── Timeout + lease expiry over active requests ───────────────
        for request in &active {
            if let Some(started_at) = request.started_at {
                // Already timed out: the arm is spent, but the lease check
                // below still applies (trait doc `reap_sweep` step 3 is
                // unconditional), so this must not skip it.
                if !request.timeout_triggered {
                    let paused_s = paused.get(&request.request_id).copied().unwrap_or_default();
                    let elapsed = now
                        .duration_since(started_at)
                        .unwrap_or_default()
                        .saturating_sub(paused_s);
                    let job_timeout = request.job_timeout_s.unwrap_or(21600).max(0) as u64;
                    if elapsed >= std::time::Duration::from_secs(job_timeout) {
                        // Conditional: only the first sweep flags the attempt.
                        let flagged = tx
                            .execute(
                                "UPDATE job_requests SET timeout_triggered = true \
                                 WHERE request_id = $1 AND result IS NULL \
                                 AND timeout_triggered = false",
                                &[&request.request_id],
                            )
                            .await
                            .map_err(db)?;
                        if flagged > 0
                            && enqueue_cancellation_job(&tx, request.run_id, &request.job_id)
                                .await?
                        {
                            outcome.cancellations += 1;
                        }
                    }
                }
            }
            if let Some(renewed) = request.last_renewed_at {
                let elapsed = now.duration_since(renewed).unwrap_or_default();
                let lease_seconds = if request.session_live {
                    crate::distributed_task::HUNG_WORKER_LEASE_SECONDS
                } else {
                    crate::distributed_task::DEAD_SESSION_LEASE_SECONDS
                };
                if elapsed >= std::time::Duration::from_secs(lease_seconds) {
                    // Conditional settle: whoever stamps the result reports
                    // the expiry (one node fails it through `settle_job`).
                    let row = tx
                        .query_opt(
                            "UPDATE job_requests SET result='failure', finished_at=now(), \
                             session_id=NULL WHERE request_id=$1 AND result IS NULL \
                             RETURNING agent_job_id::text",
                            &[&request.request_id],
                        )
                        .await
                        .map_err(db)?;
                    if let Some(row) = row {
                        // Lease + undelivered cancellation cleanup, same as
                        // `settle_request_tx` (which would no-op — the
                        // result is already stamped).
                        tx.execute(
                            "DELETE FROM job_leases WHERE request_id = $1",
                            &[&request.request_id],
                        )
                        .await
                        .map_err(db)?;
                        tx.execute(
                            "UPDATE job_cancellations SET delivered_at = now() \
                             WHERE request_id = $1 AND delivered_at IS NULL",
                            &[&request.request_id],
                        )
                        .await
                        .map_err(db)?;
                        outcome.expired.push(ExpiredLease {
                            request_id: request.request_id,
                            run_id: request.run_id,
                            job_id: request.job_id.clone(),
                            agent_job_id: codec::uuid(row.get::<_, String>(0).as_str()).ok(),
                        });
                    }
                }
            }
        }
        tx.commit().await.map_err(db)?;
        Ok(outcome)
    }

    /// `status_inputs` over live tables (see the trait contract). Read-only;
    /// each count group is one `SELECT`.
    pub(super) async fn status_inputs(
        &self,
        stale_after: std::time::Duration,
    ) -> Result<StatusInputs, ControlError> {
        // Checked out (and released) before this function's own connection:
        // one caller must never hold two pooled connections at once.
        let runner_assignments = self.live_assignments().await?;
        let client = self.reader().await?;
        let mut out = StatusInputs::default();
        // Queue buckets by the mapping the shared suite adopted.
        for row in client
            .query(
                "SELECT CASE WHEN queue_state = 'held' AND EXISTS (\
                       SELECT 1 FROM concurrency_waits w \
                       WHERE w.holder_run_id = j.run_id AND w.holder_kind = 'run') \
                     THEN 'held_run' ELSE queue_state END AS bucket, count(*) \
                 FROM jobs j WHERE queue_state <> 'none' GROUP BY bucket",
                &[],
            )
            .await
            .map_err(db)?
        {
            let count = row.get::<_, i64>(1).max(0) as u32;
            match row.get::<_, &str>(0) {
                "ready" => out.queue_len = count as usize,
                "blocked" => out.pending_jobs_len = count as usize,
                "held" => out.concurrency_blocked = count,
                "pending_expansion" => out.pending_expansions_len = count as usize,
                "expanding" => out.expanding_len = count as usize,
                _ => {}
            }
        }
        // Run counts: `count_run_statuses` parity — queued stays queued,
        // workflow-gated ('pending') counts with in-progress, terminal is
        // everything concluded.
        let row = client
            .query_one(
                "SELECT count(*) FILTER (WHERE r.status = 'queued' AND NOT EXISTS (\
                       SELECT 1 FROM concurrency_waits w WHERE w.holder_run_id = r.run_id \
                       AND w.holder_kind = 'run')), \
                    count(*) FILTER (WHERE r.status = 'in_progress' OR EXISTS (\
                       SELECT 1 FROM concurrency_waits w WHERE w.holder_run_id = r.run_id \
                       AND w.holder_kind = 'run')), \
                    count(*) FILTER (WHERE r.status = 'completed') \
                 FROM runs r",
                &[],
            )
            .await
            .map_err(db)?;
        out.runs_queued = row.get::<_, i64>(0).max(0) as u32;
        out.runs_in_progress = row.get::<_, i64>(1).max(0) as u32;
        out.runs_completed = row.get::<_, i64>(2).max(0) as u32;

        // Non-terminal runs newest-started first with their bound runner
        // names (assignment rows ∪ in-flight owned attempts).
        let rows = client
            .query(
                concat!(
                    "SELECT r.run_id::text, r.workflow_path, r.status, r.event, ",
                    us!("r.started_at"),
                    ", COALESCE(array_agg(n.name ORDER BY n.name) \
                       FILTER (WHERE n.name IS NOT NULL), '{}') AS runners \
                     FROM runs r \
                     LEFT JOIN (\
                       SELECT DISTINCT a.run_id, a.runner_id FROM job_assignments a \
                       WHERE a.runner_id IS NOT NULL \
                       UNION \
                       SELECT DISTINCT q.run_id, q.runner_id FROM job_requests q \
                       WHERE q.runner_id IS NOT NULL AND q.result IS NULL) bound \
                       ON bound.run_id = r.run_id \
                     LEFT JOIN runners n ON n.runner_id = bound.runner_id \
                     WHERE r.status <> 'completed' \
                     GROUP BY r.run_id, r.workflow_path, r.status, r.event, r.started_at \
                     ORDER BY r.started_at DESC NULLS LAST, r.run_id DESC"
                ),
                &[],
            )
            .await
            .map_err(db)?;
        for row in &rows {
            let status_word = match row.get::<_, &str>(2) {
                "in_progress" => "in_progress",
                _ => "queued",
            };
            out.active_runs
                .push(preloop_observability::status::ActiveRunSnapshot {
                    run_id: row.get(0),
                    workflow: row.get(1),
                    status: status_word.to_owned(),
                    event: row.get(3),
                    started_at: row.get::<_, Option<i64>>(4).and_then(codec::us_to_chrono),
                    assigned_runners: row.get::<_, Vec<String>>(5),
                });
        }
        // Runs in progress with no bound runner and no live job
        // (ready/claimed/blocked) are orphaned.
        out.orphaned_run_ids = client
            .query(
                "SELECT r.run_id::text FROM runs r \
                 WHERE r.status = 'in_progress' \
                 AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.run_id = r.run_id \
                                 AND j.queue_state IN ('ready','claimed','blocked')) \
                 AND NOT EXISTS (SELECT 1 FROM job_assignments a WHERE a.run_id = r.run_id) \
                 AND NOT EXISTS (SELECT 1 FROM job_requests q WHERE q.run_id = r.run_id \
                                 AND q.runner_id IS NOT NULL AND q.result IS NULL)",
                &[],
            )
            .await
            .map_err(db)?
            .iter()
            .map(|row| row.get(0))
            .collect();
        out.registered = client
            .query_one("SELECT count(*) FROM runners", &[])
            .await
            .map_err(db)?
            .get::<_, i64>(0)
            .max(0) as u32;
        out.sessions = client
            .query_one("SELECT count(*) FROM runner_sessions", &[])
            .await
            .map_err(db)?
            .get::<_, i64>(0)
            .max(0) as u32;
        // Runner liveness buckets: busy = a session holds an active request;
        // stale = every session's last_seen is older than `stale_after`;
        // idle = has sessions and is neither.
        let cutoff = now_us() - stale_after.as_micros() as i64;
        for row in client
            .query(
                concat!(
                    "SELECT r.runner_id, \
                       EXISTS (SELECT 1 FROM runner_sessions s JOIN job_requests q \
                               ON q.session_id = s.session_id AND q.result IS NULL \
                               WHERE s.runner_id = r.runner_id) AS busy, \
                       count(s.session_id) > 0 AS has_sessions, \
                       bool_and(s.last_seen_at < ",
                    ts!("$1"),
                    ") FILTER (WHERE s.session_id IS NOT NULL) AS all_stale, \
                     r.pool_proven \
                     FROM runners r LEFT JOIN runner_sessions s ON s.runner_id = r.runner_id \
                     GROUP BY r.runner_id"
                ),
                &[&cutoff],
            )
            .await
            .map_err(db)?
        {
            let busy: bool = row.get(1);
            let has_sessions: bool = row.get(2);
            let stale = has_sessions && row.get::<_, Option<bool>>(3).unwrap_or(false);
            if busy {
                out.runner_busy += 1;
                // Pool gauge: only the pool's own machines count.
                if row.get::<_, bool>(4) {
                    out.pool_busy += 1;
                }
            }
            if stale {
                out.runner_stale += 1;
            }
            if has_sessions && !busy && !stale {
                out.runner_idle += 1;
            }
        }
        out.runner_assignments = runner_assignments;
        // Oldest ready job with a known enqueue instant.
        let oldest = client
            .query_opt(
                concat!(
                    "SELECT run_id::text, job_id, ",
                    us!("enqueued_at"),
                    " FROM jobs WHERE queue_state = 'ready' AND enqueued_at IS NOT NULL \
                     ORDER BY enqueued_at LIMIT 1"
                ),
                &[],
            )
            .await
            .map_err(db)?;
        if let Some(oldest) = oldest {
            let enqueued: i64 = oldest.get(2);
            out.oldest_ready_seconds = Some((now_us() - enqueued).max(0) as f64 / 1_000_000.0);
            out.oldest_ready_run_id = Some(oldest.get(0));
            out.oldest_ready_job_id = Some(oldest.get(1));
        }
        out.queue_runner_reqs = client
            .query(
                "SELECT runs_on::text, runner_group FROM jobs WHERE queue_state = 'ready' \
                 ORDER BY priority DESC, run_order, job_order, run_id, job_id",
                &[],
            )
            .await
            .map_err(db)?
            .iter()
            .map(|row| {
                Ok((
                    codec::from_json::<Vec<String>>(row.get(0)).unwrap_or_default(),
                    row.get::<_, Option<String>>(1),
                ))
            })
            .collect::<Result<Vec<_>, ControlError>>()?;
        out.runner_caps = client
            .query(
                "SELECT labels::text, runner_group_id, runner_group_name FROM runners \
                 ORDER BY runner_id",
                &[],
            )
            .await
            .map_err(db)?
            .iter()
            .map(|row| {
                Ok(crate::models::RunnerCapabilities {
                    known: true,
                    labels: codec::from_json::<Vec<String>>(row.get(0)).unwrap_or_default(),
                    runner_group_id: row.get::<_, Option<i64>>(1),
                    runner_group_name: row.get(2),
                })
            })
            .collect::<Result<Vec<_>, ControlError>>()?;
        // Concurrency pressure: groups with a holder / with waiters, total
        // waiters, deepest queue.
        for row in client
            .query(
                "SELECT (SELECT count(DISTINCT (namespace_id, repository, group_name)) \
                           FROM concurrency_holds), \
                        (SELECT count(*) FROM concurrency_waits), \
                        (SELECT COALESCE(max(n),0) FROM (SELECT count(*) n \
                           FROM concurrency_waits \
                           GROUP BY namespace_id, repository, group_name) g), \
                        (SELECT count(DISTINCT (namespace_id, repository, group_name)) \
                           FROM concurrency_waits)",
                &[],
            )
            .await
            .map_err(db)?
        {
            out.concurrency_groups_active = row.get::<_, i64>(0).max(0) as u32;
            out.concurrency_pending_holders = row.get::<_, i64>(1).max(0) as u32;
            out.concurrency_deepest_group_pending = row.get::<_, i64>(2).max(0) as u32;
            out.concurrency_groups_contended = row.get::<_, i64>(3).max(0) as u32;
        }
        out.released_bindings = self
            .released_bindings
            .load(std::sync::atomic::Ordering::Relaxed);
        Ok(out)
    }

    /// `expire_fork_approvals`: fail closed every run whose fork-PR approval
    /// hold lapsed before `expired_before_unix_nanos`. A held run's jobs
    /// never left admission — they carry no runner and hold nothing — so
    /// expiry turns them `failure` outright, clears the hold, finalizes the
    /// run as a `failure`, drops its scheduling rows and releases its
    /// concurrency holder so the group is not wedged by a run that will
    /// never start. A run whose hold stamp is missing is left held: a
    /// bookkeeping gap must not auto-fail a run, and the operator can still
    /// approve or cancel it.
    ///
    /// One transaction (the run advisory lock serializes with an approve or
    /// a cancel). Statements: one candidate `SELECT .. FROM runs WHERE
    /// fork_approval_pending AND status <> 'completed' AND
    /// fork_approval_requested_at < $1 ORDER BY run_id`; per run
    /// `lock_run`, one guarded `UPDATE runs .. fork_approval_pending=false,
    /// status='completed', conclusion='failure'` (0 rows = another writer
    /// won: only the hold is cleared), one `UPDATE jobs ..` non-terminal →
    /// `failure`, `retire_node_requests` for its expandable nodes, the
    /// `DELETE`s of `job_assignments`/`provision_requests`/`jobsets`, the
    /// concurrency release (with waiter promotion) and one
    /// `run.completed.v1` outbox row.
    pub(super) async fn expire_fork_approvals(
        &self,
        expired_before_unix_nanos: i64,
    ) -> Result<Vec<RunId>, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let candidates: Vec<String> = tx
            .query(
                "SELECT run_id::text FROM runs \
                 WHERE fork_approval_pending AND status <> 'completed' \
                   AND fork_approval_requested_at IS NOT NULL \
                   AND fork_approval_requested_at < $1 \
                 ORDER BY run_id",
                &[&expired_before_unix_nanos],
            )
            .await
            .map_err(db)?
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        let now = now_us();
        let mut failed = Vec::new();
        for candidate in candidates {
            let run_id = codec::run_id(&candidate)?;
            if !PgBackend::lock_run(&tx, run_id).await? {
                continue;
            }
            let claimed = tx
                .execute(
                    concat!(
                        "UPDATE runs SET fork_approval_pending = false, \
                             status = 'completed', conclusion = 'failure', \
                             completed_at = COALESCE(completed_at, ",
                        ts!("$2"),
                        "), started_at = COALESCE(started_at, ",
                        ts!("$2"),
                        ") \
                         WHERE run_id = $1::text::uuid AND fork_approval_pending \
                           AND status <> 'completed' \
                           AND fork_approval_requested_at IS NOT NULL \
                           AND fork_approval_requested_at < $3"
                    ),
                    &[&candidate, &now, &expired_before_unix_nanos],
                )
                .await
                .map_err(db)?;
            if claimed == 0 {
                // Approved, completed or cancelled concurrently: the winner
                // owns the run's terminal bookkeeping, but a hold that is
                // still set must not survive.
                tx.execute(
                    "UPDATE runs SET fork_approval_pending = false \
                     WHERE run_id = $1::text::uuid AND fork_approval_pending",
                    &[&candidate],
                )
                .await
                .map_err(db)?;
                continue;
            }
            // Expandable nodes (deferred matrix parents, reusable callers)
            // minted placeholder requests at submit even though they never
            // ran; settle them with the node so no in-flight marker outlives
            // the run.
            let expandable: Vec<JobId> = tx
                .query(
                    "SELECT j.job_id FROM jobs j LEFT JOIN job_specs s \
                     ON s.run_id = j.run_id AND s.job_id = j.job_id \
                     WHERE j.run_id = $1::text::uuid \
                       AND j.status NOT IN \
                           ('success','failure','cancelled','skipped','timed_out') \
                       AND ((s.deferred_matrix IS NOT NULL AND s.deferred_matrix <> 'null') \
                            OR (s.reusable_call IS NOT NULL AND s.reusable_call <> 'null'::jsonb) \
                            OR j.queue_state IN ('pending_expansion','expanding'))",
                    &[&candidate],
                )
                .await
                .map_err(db)?
                .iter()
                .map(|row| JobId(row.get::<_, String>(0)))
                .collect();
            tx.execute(
                concat!(
                    "UPDATE jobs SET status='failure', queue_state='none', \
                         completed_at=",
                    ts!("$2"),
                    ", started_at=COALESCE(started_at,",
                    ts!("$2"),
                    "), claimed_by_runner_id=NULL, claimed_at=NULL \
                     WHERE run_id=$1::text::uuid AND status NOT IN \
                         ('success','failure','cancelled','skipped','timed_out')"
                ),
                &[&candidate, &now],
            )
            .await
            .map_err(db)?;
            for job_id in &expandable {
                retire_node_requests(
                    &tx,
                    run_id,
                    job_id,
                    Retirement::Settle(ExecutionStatus::Failure),
                )
                .await?;
            }
            // Clear dispatch intent for this run.
            for sql in [
                "DELETE FROM job_assignments WHERE run_id=$1::text::uuid",
                "DELETE FROM provision_requests WHERE run_id=$1::text::uuid",
                "DELETE FROM jobsets WHERE run_id=$1::text::uuid",
            ] {
                tx.execute(sql, &[&candidate]).await.map_err(db)?;
            }
            release_concurrency_for_run(self, &tx, run_id).await?;
            emit_outbox(
                &tx,
                Some(run_id),
                "run.completed.v1",
                serde_json::json!({"status": "failure"}),
            )
            .await?;
            failed.push(run_id);
        }
        tx.commit().await.map_err(db)?;
        Ok(failed)
    }
}
