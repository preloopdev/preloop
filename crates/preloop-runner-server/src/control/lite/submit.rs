//! `submit_run`: insert a run and its jobs, evaluating the workflow-level
//! concurrency gate, per-job gates, and the unhostable-platform check, then
//! run the promotion sweep for `needs:`-gated jobs.
//!
//! `submit_run` over the agreed tables: every classification is written directly onto `jobs`
//! (`status` = workflow truth, `queue_state` = dispatch copy).

use super::codec::{self, now_us};
use super::concurrency as cg;
use super::{LiteBackend, db, jobs, promote, settle};
use crate::concurrency::{self, Holder};
use crate::control::logic;
use crate::control::types::*;
use crate::models::{QueuedJob, StepRecord, TaskAgentJobRequestRecord};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::BTreeMap;

impl LiteBackend {
    /// Insert a submitted run and its jobs. Idempotent on
    /// `(webhook_delivery_id, workflow_path)`: a replay returns the existing
    /// run instead of a duplicate.
    pub(crate) async fn submit_run(
        &self,
        submit: SubmitRun,
    ) -> Result<SubmitOutcome, ControlError> {
        self.write(|tx| submit_run_tx(tx, self, submit))
    }
}

/// `ref_type` derived from the git ref (the schema's four-way check).
fn ref_type_of(git_ref: &str) -> &'static str {
    if git_ref.starts_with("refs/heads/") {
        "branch"
    } else if git_ref.starts_with("refs/tags/") {
        "tag"
    } else if git_ref.starts_with("refs/pull/") {
        "pull_request"
    } else {
        "other"
    }
}

/// A run's provenance: a durable webhook delivery made it, otherwise the
/// native API did (the CLI and schedules both submit through that path).
fn origin_of(record: &crate::models::RunRecord) -> &'static str {
    if record.webhook_delivery_id.is_some() {
        "webhook"
    } else {
        "api"
    }
}

/// `jobs.kind` for a freshly submitted node.
pub(super) fn kind_of(job: &QueuedJob) -> &'static str {
    if job.deferred_matrix.is_some() {
        "matrix_parent"
    } else if job.reusable_call.is_some() {
        "reusable_caller"
    } else if job.base_id != job.job_id.0 {
        "matrix_leg"
    } else {
        "job"
    }
}

/// Pull `pull_request.{head,base}.ref` out of an event payload.
fn pull_request_refs(payload: &serde_json::Value) -> (Option<String>, Option<String>) {
    let pull = &payload["pull_request"];
    (
        pull["head"]["ref"].as_str().map(str::to_owned),
        pull["base"]["ref"].as_str().map(str::to_owned),
    )
}

/// The immutable per-job spec fields the run record and the schedulers read
/// back (`job_specs`).
pub(super) fn spec_extras<'a>(
    record: &crate::models::RunRecord,
    job: &QueuedJob,
    id_token_granted: bool,
    oidc_context: Option<&'a crate::state::OidcJobContext>,
) -> jobs::SpecExtras<'a> {
    jobs::SpecExtras {
        reusable_call_json: jobs::ReusableSpec::encode(
            job.reusable_call.as_ref(),
            record.reusable_calls.get(&job.job_id.0),
            record.caller_plans.get(&job.job_id),
        ),
        fail_fast: record.job_fail_fast.get(&job.base_id).copied(),
        continue_on_error: record.job_continue_on_error.get(&job.job_id.0).copied(),
        id_token_granted,
        oidc_environment: oidc_context.and_then(|ctx| ctx.environment.as_deref()),
        oidc_job_workflow_ref: oidc_context.and_then(|ctx| ctx.job_workflow_ref.as_deref()),
        oidc_job_workflow_sha: oidc_context.and_then(|ctx| ctx.job_workflow_sha.as_deref()),
    }
}

fn submit_run_tx(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    submit: SubmitRun,
) -> Result<SubmitOutcome, ControlError> {
    let SubmitRun {
        namespace,
        mut record,
        jobs: submit_jobs,
        workflow_concurrency,
        empty_concurrency_group,
        check_hostable,
    } = submit;
    let run_id = record.run_id;

    // ── Idempotent replay ───────────────────────────────────────────────
    if let Some(delivery_id) = record.webhook_delivery_id.as_deref() {
        let existing: Option<String> = tx
            .prepare_cached(
                "SELECT run_id FROM runs WHERE webhook_delivery_id = ?1 \
                 AND workflow_path = ?2",
            )
            .map_err(db)?
            .query_row(params![delivery_id, record.workflow_path_str], |row| {
                row.get(0)
            })
            .optional()
            .map_err(db)?;
        if let Some(existing) = existing
            && let Some(existing) = jobs::run_record(tx, codec::run_id(&existing))?
        {
            return Ok(SubmitOutcome {
                run_id: existing.run_id,
                run_number: existing.run_number,
                queued_jobs: 0,
                status: existing.status,
                concluded: Vec::new(),
                held: false,
                rejected: None,
                existing: Some(Box::new(existing)),
                queue_depth: jobs::ready_count(tx)?,
                next_runs_on: jobs::next_ready_labels(tx)?,
                events: Vec::new(),
            });
        }
    }

    // ── Empty workflow concurrency group: reject without inserting ──────
    if empty_concurrency_group {
        return Ok(SubmitOutcome {
            run_id,
            run_number: record.run_number,
            queued_jobs: 0,
            status: ExecutionStatus::Failure,
            concluded: vec![(
                JobId("*".to_owned()),
                ExecutionStatus::Failure,
                Some("empty concurrency group".to_owned()),
            )],
            held: false,
            rejected: Some(ExecutionStatus::Failure),
            existing: None,
            queue_depth: jobs::ready_count(tx)?,
            next_runs_on: jobs::next_ready_labels(tx)?,
            events: Vec::new(),
        });
    }

    // ── The run row (FK target for jobs and the gate's event ordering) ──
    // `runs_number` is unique on (namespace, repo, path, number, attempt):
    // a caller that reuses a number (suite submissions) gets the next one,
    // matching GitHub — a run's number is never duplicated in one workflow.
    if tx
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM runs WHERE namespace_id = ?1 \
             AND repository = ?2 AND workflow_path = ?3 AND run_number = ?4 \
             AND run_attempt = ?5)",
        )
        .map_err(db)?
        .query_row(
            params![
                namespace,
                record.submission.repository,
                record.workflow_path_str,
                record.run_number as i64,
                record.run_attempt as i64
            ],
            |row| row.get::<_, bool>(0),
        )
        .map_err(db)?
    {
        record.run_number = allocate_run_number_tx(
            tx,
            &namespace,
            &record.submission.repository,
            &record.workflow_path_str,
        )?;
    }
    // `runs.namespace_id` FKs into `namespaces`; the namespace upsert mirrors
    // pg's `allocate_run_number` so a first-seen tenant cannot violate the
    // constraint.
    tx.prepare_cached("INSERT INTO namespaces (namespace_id) VALUES (?1) ON CONFLICT DO NOTHING")
        .map_err(db)?
        .execute(params![namespace])
        .map_err(db)?;
    insert_run_row(tx, &record, &namespace, workflow_concurrency.as_ref())?;

    // A run's jobs all share one queue sequence number; `created_at` is
    // monotonic per submit and needs no table scan (`job_order` orders
    // within the run).
    let run_order = record.created_at.timestamp_micros();
    // Legs of one matrix already handed a slot in this submit (max-parallel).
    let mut active_by_base: BTreeMap<String, u64> = BTreeMap::new();
    let display_order: BTreeMap<String, i64> = record
        .jobs_list
        .iter()
        .enumerate()
        .map(|(index, detail)| (detail.job_id.clone(), index as i64))
        .collect();
    // ── Workflow-level gate ─────────────────────────────────────────────
    // Workless submissions (every job gated off / unhostable / unsatisfiable)
    // take no admission: a Holder::Run taken here is never released through
    // the completion path and would park every later submission in the group.
    let platforms = jobs::registered_platforms(tx)?;
    let pool_labels = if check_hostable {
        backend.pool_labels()
    } else {
        Vec::new()
    };
    let runner_labels = if pool_labels.is_empty() {
        Vec::new()
    } else {
        jobs::runner_label_sets(tx)?
    };
    let has_runnable = submit_jobs.iter().any(|job| {
        !logic::concludes_at_submit(
            job.initially_skipped,
            !job.queued.needs.is_empty(),
            job.queued.reusable_call.is_some() || job.queued.deferred_matrix.is_some(),
            &job.queued.runs_on,
            check_hostable,
            platforms.iter().copied(),
            &pool_labels,
            runner_labels.iter().any(|labels| {
                crate::runtime_scheduling::job_matches_runner(&job.queued.runs_on, labels)
            }),
        )
    });
    let mut held = false;
    if has_runnable && let Some(wf) = &workflow_concurrency {
        let key = concurrency::concurrency_key(&record.submission.repository, &wf.group);
        let holder = Holder::Run(run_id);
        let mut cancel = settle::canceler(backend);
        match cg::acquire(
            tx,
            &namespace,
            &key.0,
            &key.1,
            &wf.group,
            &holder,
            wf.cancel_in_progress,
            wf.queue,
            &mut cancel,
        )? {
            cg::AcqOutcome::Acquired => {}
            cg::AcqOutcome::Parked => held = true,
            cg::AcqOutcome::ArrivalCancelled => {
                // Cancelled on arrival (queue overflow, or a newer event
                // supersedes this one): every job lands terminal. Request
                // correlation was minted at submit (placeholder requests
                // exist even for nodes that never dispatch), so mint then
                // settle each as cancelled rather than leaking inflight rows.
                for (index, submit_job) in submit_jobs.iter().enumerate() {
                    let job = &submit_job.queued;
                    jobs::insert_job(
                        tx,
                        run_id,
                        &namespace,
                        job,
                        kind_of(job),
                        None,
                        record
                            .job_names
                            .get(&job.job_id)
                            .map(String::as_str)
                            .unwrap_or(&job.job_id.0),
                        display_order
                            .get(&job.job_id.0)
                            .copied()
                            .unwrap_or(index as i64),
                        ExecutionStatus::Cancelled,
                        "none",
                        job.needs.len() as i32,
                        run_order,
                        index as i64,
                        &spec_extras(
                            &record,
                            job,
                            submit_job.id_token_granted,
                            submit_job.oidc_context.as_ref(),
                        ),
                    )?;
                    jobs::insert_job_message(
                        tx,
                        run_id,
                        &job.job_id,
                        &job.message,
                        &job.condition_context,
                    )?;
                    mint_request(
                        tx,
                        &namespace,
                        run_id,
                        &job.job_id,
                        submit_job.request.clone(),
                        submit_job.token_request.clone(),
                        submit_job.step_manifest.clone(),
                    )?;
                }
                tx.prepare_cached(
                    "UPDATE job_requests SET result = 'cancelled', finished_at = ?2 \
                     WHERE run_id = ?1 AND result IS NULL",
                )
                .map_err(db)?
                .execute(params![codec::run_key(run_id), now_us()])
                .map_err(db)?;
                tx.prepare_cached(
                    "UPDATE runs SET status = 'completed', conclusion = 'cancelled', \
                         completed_at = ?2, started_at = COALESCE(started_at, ?2) \
                     WHERE run_id = ?1",
                )
                .map_err(db)?
                .execute(params![codec::run_key(run_id), now_us()])
                .map_err(db)?;
                let concluded = submit_jobs
                    .iter()
                    .map(|submit_job| {
                        (
                            submit_job.queued.job_id.clone(),
                            ExecutionStatus::Cancelled,
                            concurrency::cancelled_reason(),
                        )
                    })
                    .collect();
                // A cancelled arrival is still a created (and immediately
                // completed) run: emit both durable events, matching pg.
                jobs::emit_outbox(
                    tx,
                    &namespace,
                    Some(run_id),
                    "run.created.v1",
                    serde_json::json!({"status": "cancelled", "jobs": submit_jobs.len()}),
                )?;
                jobs::emit_outbox(
                    tx,
                    &namespace,
                    Some(run_id),
                    "run.completed.v1",
                    serde_json::json!({"status": "cancelled"}),
                )?;
                return Ok(SubmitOutcome {
                    run_id,
                    run_number: record.run_number,
                    queued_jobs: submit_jobs.len(),
                    status: ExecutionStatus::Cancelled,
                    concluded,
                    held: false,
                    rejected: Some(ExecutionStatus::Cancelled),
                    existing: None,
                    queue_depth: jobs::ready_count(tx)?,
                    next_runs_on: jobs::next_ready_labels(tx)?,
                    events: Vec::new(),
                });
            }
            cg::AcqOutcome::Failed => {
                return Err(ControlError::backend(anyhow::anyhow!(
                    "workflow concurrency admission failed"
                )));
            }
        }
    }

    // ── Per-job classification ──────────────────────────────────────────
    let mut concluded: Vec<(JobId, ExecutionStatus, Option<String>)> = Vec::new();
    // `inserted` counts `jobs` rows (outbox detail); `queued` counts
    // dispatchable jobs — `RunAccepted.queued_jobs` and the waiter wake.
    let mut inserted = 0usize;
    let mut queued = 0usize;
    for (job_order, submit_job) in submit_jobs.into_iter().enumerate() {
        let SubmitJob {
            queued: job,
            request,
            token_request,
            id_token_granted,
            oidc_context,
            step_manifest,
            initially_skipped,
        } = submit_job;
        let job_id = job.job_id.clone();
        let order = display_order
            .get(&job_id.0)
            .copied()
            .unwrap_or(job_order as i64);
        let spec = spec_extras(&record, &job, id_token_granted, oidc_context.as_ref());

        // Unhostable platform: no registered runner can ever take this job.
        // Deferred for needs-gated jobs — they park until their `if:` can be
        // evaluated, and a job GitHub would skip must not fail here on labels
        // it will never need; promotion re-runs this check on every Run
        // decision.
        let unhostable = if check_hostable && job.needs.is_empty() {
            crate::control::logic::unhostable_platform(&job.runs_on, platforms.iter().copied())
        } else {
            None
        };
        if let Some(platform) = unhostable {
            insert_classified_job(
                tx,
                &namespace,
                run_id,
                &job,
                &job_id,
                order,
                run_order,
                job_order as i64,
                ExecutionStatus::Failure,
                "none",
                &spec,
            )?;
            concluded.push((
                job_id,
                ExecutionStatus::Failure,
                Some(crate::control::logic::unhostable_reason(
                    platform,
                    &job.runs_on,
                )),
            ));
            inserted += 1;
            continue;
        }

        // Concluded at submit by its own `if:`.
        if initially_skipped {
            insert_classified_job(
                tx,
                &namespace,
                run_id,
                &job,
                &job_id,
                order,
                run_order,
                job_order as i64,
                ExecutionStatus::Skipped,
                "none",
                &spec,
            )?;
            concluded.push((job_id.clone(), ExecutionStatus::Skipped, None));
            inserted += 1;
            continue;
        }

        // Labels the co-hosted pool can never satisfy and no registered
        // runner serves: conclude now instead of starving in the queue.
        // Placeholders are skipped — expansion materializes their real jobs.
        // Needs-gated jobs defer too: they park until their `if:` can be
        // evaluated, and the promotion path re-runs this check.
        if job.reusable_call.is_none()
            && job.deferred_matrix.is_none()
            && job.needs.is_empty()
            && let Some(reason) = crate::control::logic::unschedulable_reason(
                &job.runs_on,
                &pool_labels,
                runner_labels.iter().any(|labels| {
                    crate::runtime_scheduling::job_matches_runner(&job.runs_on, labels)
                }),
            )
        {
            tracing::warn!(
                %run_id,
                job = %job_id.0,
                labels = ?job.runs_on,
                pool_labels = ?pool_labels,
                "runs-on unsatisfiable by runner pool; failing the job at enqueue"
            );
            insert_classified_job(
                tx,
                &namespace,
                run_id,
                &job,
                &job_id,
                order,
                run_order,
                job_order as i64,
                ExecutionStatus::Failure,
                "none",
                &spec,
            )?;
            concluded.push((job_id, ExecutionStatus::Failure, Some(reason)));
            inserted += 1;
            continue;
        }

        // A job that reaches admission behind a hold parks: the workflow
        // gate is not this run's turn yet (`held`), the run is held by the
        // fork-PR approval policy, or its environment protection gate is not
        // satisfied. All three park identically — held, pending, with no
        // concurrency wait row of their own — and are released by
        // `promote_ready_jobs` once the hold lifts.
        if held || record.fork_approval_pending || job.environment_gate.is_some() {
            insert_classified_job(
                tx,
                &namespace,
                run_id,
                &job,
                &job_id,
                order,
                run_order,
                job_order as i64,
                ExecutionStatus::Pending,
                "held",
                &spec,
            )?;
            jobs::insert_job_message(tx, run_id, &job_id, &job.message, &job.condition_context)?;
            mint_request(
                tx,
                &namespace,
                run_id,
                &job_id,
                request,
                token_request,
                step_manifest,
            )?;
            inserted += 1;
            queued += 1;
            continue;
        }

        // `max-parallel` for this matrix base: legs already handed a slot in
        // this submit count against the limit.
        let max_parallel_ok = job
            .max_parallel
            .is_none_or(|limit| active_by_base.get(&job.base_id).copied().unwrap_or(0) < limit);
        if job.reusable_call.is_some() || !job.needs.is_empty() || !max_parallel_ok {
            // Caller placeholders wait as `pending` (the sweep routes them to
            // their jobset gate once their `if:` passes); needs-gated and
            // over-max-parallel jobs wait as `queued`.
            let status = if job.reusable_call.is_some() && job.needs.is_empty() {
                ExecutionStatus::Pending
            } else {
                ExecutionStatus::Queued
            };
            insert_classified_job(
                tx,
                &namespace,
                run_id,
                &job,
                &job_id,
                order,
                run_order,
                job_order as i64,
                status,
                "blocked",
                &spec,
            )?;
            // The template lands even while the job waits on needs/gates:
            // promotion hydrates it (needs context), and the acquire path
            // refuses a job with no message row.
            jobs::insert_job_message(tx, run_id, &job_id, &job.message, &job.condition_context)?;
            mint_request(
                tx,
                &namespace,
                run_id,
                &job_id,
                request,
                token_request,
                step_manifest,
            )?;
            inserted += 1;
            queued += 1;
            continue;
        }

        // Ready path: evaluate the job gate, then enqueue.
        insert_classified_job(
            tx,
            &namespace,
            run_id,
            &job,
            &job_id,
            order,
            run_order,
            job_order as i64,
            ExecutionStatus::Queued,
            "none",
            &spec,
        )?;
        jobs::insert_job_message(tx, run_id, &job_id, &job.message, &job.condition_context)?;
        let row = jobs::job(tx, run_id, &job_id)?
            .ok_or_else(|| ControlError::backend(anyhow::anyhow!("job row vanished")))?;
        match promote::acquire_job_gate(tx, backend, run_id, &job_id)? {
            promote::GateOutcome::Proceed => {
                promote::enqueue_ready_job(tx, backend, &row, job.concurrency.is_some())?;
                *active_by_base.entry(job.base_id.clone()).or_default() += 1;
            }
            promote::GateOutcome::Parked => {
                tx.prepare_cached(
                    "UPDATE jobs SET queue_state = 'held', status = 'pending', \
                         concurrency_wait_at = COALESCE(concurrency_wait_at, ?3) \
                     WHERE run_id = ?1 AND job_id = ?2",
                )
                .map_err(db)?
                .execute(params![codec::run_key(run_id), job_id.0, now_us()])
                .map_err(db)?;
            }
            promote::GateOutcome::Failed(status) => {
                tx.prepare_cached(
                    "UPDATE jobs SET status = ?3, queue_state = 'none', completed_at = ?4 \
                     WHERE run_id = ?1 AND job_id = ?2",
                )
                .map_err(db)?
                .execute(params![
                    codec::run_key(run_id),
                    job_id.0,
                    status_str(status),
                    now_us()
                ])
                .map_err(db)?;
                concluded.push((job_id.clone(), status, None));
            }
        }
        // Mint the attempt's request row and patch the message's requestId
        // to the allocated id (the runner echoes it back on acquire).
        mint_request(
            tx,
            &namespace,
            run_id,
            &job_id,
            request,
            token_request,
            step_manifest,
        )?;
        inserted += 1;
        queued += 1;
    }

    // Jobs inserted terminal (initially_skipped / unhostable) settle their
    // dependents' edges before promotion reads `remaining_needs = 0`. One
    // whole-run recompute — a dependent inserted after its terminal parent
    // was seeded with the raw declared count.
    jobs::refresh_run_remaining_needs(tx, run_id)?;

    // Submit-seeded outputs (a rerun carry `record.job_outputs`) land on
    // the matching job rows.
    for (job_id, outputs) in &record.job_outputs {
        if !outputs.is_empty() {
            tx.prepare_cached("UPDATE jobs SET outputs = ?3 WHERE run_id = ?1 AND job_id = ?2")
                .map_err(db)?
                .execute(params![
                    codec::run_key(run_id),
                    job_id.0.as_str(),
                    serde_json::to_string(outputs).map_err(ControlError::backend)?
                ])
                .map_err(db)?;
        }
    }

    // ── Promotion sweep for needs-gated jobs ────────────────────────────
    let submit_concluded = concluded.len();
    let mut outcome = crate::runtime_scheduling::SchedulingOutcome::default();
    promote::promote_run(tx, backend, run_id, &mut outcome)?;
    for (rid, jid) in outcome.skipped.iter().chain(outcome.failed.iter()) {
        if *rid == run_id
            && let Some(status) = jobs::job(tx, run_id, jid)?.map(|job| job.status)
        {
            concluded.push((jid.clone(), status, None));
        }
    }
    // A job settled by the sweep emitted `run.completed.v1` through
    // `settle_node`; only a run the submit itself concluded needs one here.
    let sweep_settled = concluded.len() > submit_concluded;

    // ── Run status ──────────────────────────────────────────────────────
    let summary = jobs::summarize_run_row(tx, run_id)?;
    if summary.is_terminal() {
        // A run that concluded at submit took its workflow gate before
        // classification knew it was workless: drop the hold and the
        // recorded group so it never admits.
        settle::release_concurrency_for_run(tx, backend, run_id)?;
        tx.prepare_cached("UPDATE runs SET concurrency_group = NULL WHERE run_id = ?1")
            .map_err(db)?
            .execute([codec::run_key(run_id)])
            .map_err(db)?;
        held = false;
    }
    let status = if summary.is_terminal() {
        summary
    } else if held {
        ExecutionStatus::Pending
    } else {
        ExecutionStatus::Queued
    };
    // A held run reports `queued` on the row; `run_record` reconstructs
    // Pending from its `holder_kind = 'run'` wait row.
    tx.prepare_cached(
        "UPDATE runs SET status = 'queued' WHERE run_id = ?1 AND status <> 'completed'",
    )
    .map_err(db)?
    .execute([codec::run_key(run_id)])
    .map_err(db)?;
    jobs::emit_outbox(
        tx,
        &namespace,
        Some(run_id),
        "run.created.v1",
        serde_json::json!({"status": status_str(status), "jobs": inserted}),
    )?;
    if status.is_terminal() && !sweep_settled {
        jobs::emit_outbox(
            tx,
            &namespace,
            Some(run_id),
            "run.completed.v1",
            serde_json::json!({"status": status_str(status)}),
        )?;
    }

    // The events the handler used to emit post-commit — `RunAccepted`,
    // concluded `JobStatus`s, a held `RunStatus` — are written inside this
    // transaction so they persist atomically with the run. The handler
    // replays them via `emit_persisted`.
    let mut events: Vec<preloop_gha_protocol::NdjsonEvent> = concluded
        .iter()
        .filter(|(job_id, _, _)| job_id.0 != "*")
        .map(
            |(job_id, status, reason)| preloop_gha_protocol::NdjsonEvent::JobStatus {
                run_id,
                job_id: job_id.clone(),
                status: *status,
                reason: reason.clone(),
            },
        )
        .collect();
    events.push(preloop_gha_protocol::NdjsonEvent::RunAccepted {
        run_id,
        queued_jobs: queued,
    });
    if held {
        events.push(preloop_gha_protocol::NdjsonEvent::RunStatus {
            run_id,
            status: ExecutionStatus::Pending,
            reason: crate::concurrency::pending_reason(),
        });
    }
    for event in &events {
        super::lifecycle::append_event_tx(tx, event)?;
    }
    Ok(SubmitOutcome {
        run_id,
        run_number: record.run_number,
        queued_jobs: queued,
        status,
        concluded,
        held,
        rejected: None,
        existing: None,
        queue_depth: jobs::ready_count(tx)?,
        next_runs_on: jobs::next_ready_labels(tx)?,
        events,
    })
}

/// Insert one classified job (row + spec + needs) with the run's display
/// order and the submission's queue sequence.
#[allow(clippy::too_many_arguments)]
pub(super) fn insert_classified_job(
    tx: &Transaction<'_>,
    namespace: &str,
    run_id: RunId,
    job: &QueuedJob,
    job_id: &JobId,
    display_order: i64,
    run_order: i64,
    job_order: i64,
    status: ExecutionStatus,
    queue_state: &str,
    spec: &jobs::SpecExtras<'_>,
) -> Result<(), ControlError> {
    jobs::insert_job(
        tx,
        run_id,
        namespace,
        job,
        kind_of(job),
        None,
        job_id.0.as_str(),
        display_order,
        status,
        queue_state,
        job.needs.len() as i32,
        run_order,
        job_order,
        spec,
    )
}

/// Insert the `runs` + `run_submissions` rows for a new run.
fn insert_run_row(
    tx: &Transaction<'_>,
    record: &crate::models::RunRecord,
    namespace: &str,
    workflow_concurrency: Option<&WorkflowConcurrency>,
) -> Result<(), ControlError> {
    let submission = &record.submission;
    let (head_ref, base_ref) = pull_request_refs(&submission.payload);
    let tree_digest = record
        .workspace_snapshot
        .as_ref()
        .map(|snapshot| snapshot.tree_sha.clone());
    tx.prepare_cached(
        "INSERT INTO runs (run_id, namespace_id, repository, workflow_path, \
             run_number, run_attempt, run_name, event, ref, ref_type, head_ref, \
             base_ref, head_sha, workflow_ref, status, conclusion, \
             webhook_delivery_id, origin, actor, tree_digest, concurrency_group, \
             concurrency_cancel_in_progress, fork_approval_pending, \
             fork_approval_requested_at, fork_approval_approved_at, \
             fork_approval_note, reports_check_runs, created_at, started_at, \
             completed_at) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30) \
         ON CONFLICT (run_id) DO NOTHING",
    )
    .map_err(db)?
    .execute(params![
        codec::run_key(record.run_id),
        namespace,
        submission.repository,
        record.workflow_path_str,
        record.run_number as i64,
        record.run_attempt as i64,
        record.run_name,
        record.event,
        submission.git_ref,
        ref_type_of(&submission.git_ref),
        head_ref,
        base_ref,
        record.head_sha,
        record.workflow_ref,
        "queued",
        Option::<String>::None,
        record.webhook_delivery_id,
        origin_of(record),
        submission.actor,
        tree_digest,
        workflow_concurrency.map(|wf| wf.group.clone()),
        workflow_concurrency
            .map(|wf| wf.cancel_in_progress as i64)
            .unwrap_or(0),
        record.fork_approval_pending as i64,
        record.fork_approval_requested_at_unix_nanos,
        record.fork_approved_at_unix_nanos,
        record.fork_approval_note,
        record.reports_check_runs as i64,
        record.created_at.timestamp_micros(),
        record.started_at.map(|at| at.timestamp_micros()),
        record.completed_at.map(|at| at.timestamp_micros()),
    ])
    .map_err(db)?;
    // A push-request submission owns a 'pending' push state from the start:
    // `already_published`'s echo check JOINs this row, so it must exist before
    // the first sync writes 'synced'.
    if submission.push.is_some() {
        tx.prepare_cached(
            "INSERT INTO run_push_states (run_id, status) VALUES (?1, 'pending') \
             ON CONFLICT (run_id) DO NOTHING",
        )
        .map_err(db)?
        .execute(params![codec::run_key(record.run_id)])
        .map_err(db)?;
    }
    // Secret values never reach the database (the SecretProvider holds
    // them); clear defensively at the boundary.
    let mut stored_submission = (*record.submission).clone();
    stored_submission.secrets.clear();
    let details = serde_json::json!({
        "job_base_ids": record.job_base_ids,
        "job_names": record.job_names,
        "job_needs": record.job_needs,
        "job_check_run_ids": record.job_check_run_ids,
        "caller_plans": record.caller_plans,
        "reusable_calls": record.reusable_calls,
        "job_fail_fast": record.job_fail_fast,
        "job_continue_on_error": record.job_continue_on_error,
        "jobs_list": record.jobs_list,
    });
    tx.prepare_cached(
        "INSERT INTO run_submissions (run_id, submission, github_context, \
             workspace_snapshot, snapshot_timing, record_details) \
         VALUES (?1,?2,?3,?4,?5,?6) \
         ON CONFLICT (run_id) DO UPDATE SET submission = excluded.submission, \
             github_context = excluded.github_context, \
             workspace_snapshot = excluded.workspace_snapshot, \
             snapshot_timing = excluded.snapshot_timing, \
             record_details = excluded.record_details",
    )
    .map_err(db)?
    .execute(params![
        codec::run_key(record.run_id),
        stored_submission
            .to_request_json()
            .map_err(|e| { ControlError::backend(anyhow::anyhow!("submission encode: {e}")) })?
            .to_string(),
        record.github.to_string(),
        record
            .workspace_snapshot
            .as_ref()
            .map(|snapshot| serde_json::to_string(snapshot).unwrap_or_default()),
        record
            .snapshot_timing
            .as_ref()
            .map(|timing| serde_json::to_string(timing).unwrap_or_default()),
        details.to_string(),
    ])
    .map_err(db)?;
    Ok(())
}

/// Mint the attempt row(s) for a freshly inserted job: the `job_requests`
/// correlation (with the message's `requestId` patched to match), the
/// deferred token-mint request, and the declared step manifest.
pub(super) fn mint_request(
    tx: &Transaction<'_>,
    namespace: &str,
    run_id: RunId,
    job_id: &JobId,
    request: Option<TaskAgentJobRequestRecord>,
    token_request: Option<crate::models::GitHubTokenRequest>,
    step_manifest: Vec<StepRecord>,
) -> Result<(), ControlError> {
    let Some(request) = request else {
        // Caller placeholder: the node never dispatches, so it owns no
        // attempt; its request correlation is minted at expansion.
        return Ok(());
    };
    let request_id = insert_attempt(
        tx,
        namespace,
        run_id,
        job_id,
        request.agent_job_id,
        request.timeline_id,
        step_manifest,
    )?;
    // The runner-facing message carries the allocated request id.
    if let Some(mut message) = jobs::stored_job_message(tx, run_id, job_id)? {
        message.request_id = request_id;
        jobs::update_job_message(tx, run_id, job_id, &message)?;
    }
    if let Some(token_request) = token_request {
        tx.prepare_cached(
            "INSERT INTO github_token_requests (request_id, repository, permissions, \
                 declared, untrusted) VALUES (?1,?2,?3,?4,?5) \
             ON CONFLICT (request_id) DO NOTHING",
        )
        .map_err(db)?
        .execute(params![
            request_id,
            token_request.repository,
            serde_json::to_string(&token_request.permissions).unwrap_or_else(|_| "{}".to_owned()),
            token_request.declared as i64,
            token_request.untrusted as i64,
        ])
        .map_err(db)?;
    }
    Ok(())
}

/// Insert one unclaimed attempt — its `job_requests` row and step manifest —
/// and return the allocated `request_id`. The caller owns the message
/// template and the token-mint recipe.
pub(super) fn insert_attempt(
    tx: &Transaction<'_>,
    namespace: &str,
    run_id: RunId,
    job_id: &JobId,
    agent_job_id: uuid::Uuid,
    timeline_id: uuid::Uuid,
    step_manifest: Vec<StepRecord>,
) -> Result<i64, ControlError> {
    let agent_job_id = agent_job_id.to_string();
    tx.prepare_cached(
        "INSERT INTO job_requests (run_id, job_id, namespace_id, agent_job_id, \
             timeline_id, runner_id, claimed_at) \
         VALUES (?1,?2,?3,?4,?5,NULL,0)",
    )
    .map_err(db)?
    .execute(params![
        codec::run_key(run_id),
        job_id.0,
        namespace,
        agent_job_id,
        timeline_id.to_string()
    ])
    .map_err(db)?;
    let request_id = tx.last_insert_rowid();
    for (position, step) in step_manifest.into_iter().enumerate() {
        tx.prepare_cached(
            "INSERT INTO job_steps (agent_job_id, step_id, position, kind, \
                 workflow_index, runner_number, context_name, name, conclusion, \
                 started_at, finished_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11) \
             ON CONFLICT (agent_job_id, step_id) DO NOTHING",
        )
        .map_err(db)?
        .execute(params![
            agent_job_id,
            step.id,
            position as i64,
            match step.kind {
                crate::models::StepKind::Workflow => "workflow",
                crate::models::StepKind::Synthetic => "synthetic",
            },
            step.workflow_index.map(|index| index as i64),
            step.runner_number.map(|number| number as i64),
            step.context_name,
            step.name,
            step.conclusion,
            step.started_at.map(|at| at.timestamp_micros()),
            step.finished_at.map(|at| at.timestamp_micros()),
        ])
        .map_err(db)?;
    }
    Ok(request_id)
}

/// `allocate_run_number` inside an open transaction: upsert the
/// `(namespace, repository, workflow_path)` counter and return the next
/// number. Used by `submit_run` for natural-key collisions and by the
/// trait's `allocate_run_number` (which wraps it in `write`).
pub(super) fn allocate_run_number_tx(
    tx: &Transaction<'_>,
    namespace_id: &str,
    repository: &str,
    workflow_path: &str,
) -> Result<u64, ControlError> {
    let number: i64 = tx
        .query_row(
            "INSERT INTO workflow_run_numbers \
                 (namespace_id, repository, workflow_path, last_run_number) \
             VALUES (?1, ?2, ?3, \
                 COALESCE((SELECT MAX(r.run_number) FROM runs r \
                     WHERE r.namespace_id = ?1 AND r.repository = ?2 \
                       AND r.workflow_path = ?3), 0) + 1) \
             ON CONFLICT (namespace_id, repository, workflow_path) \
             DO UPDATE SET last_run_number = last_run_number + 1 \
             RETURNING last_run_number",
            params![namespace_id, repository, workflow_path],
            |row| row.get(0),
        )
        .map_err(db)?;
    Ok(number.max(0) as u64)
}
