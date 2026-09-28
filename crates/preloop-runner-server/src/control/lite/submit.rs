//! `submit_run`: insert a run and its jobs, evaluating the workflow-level
//! concurrency gate, per-job gates, and the unhostable-platform check, then
//! run the promotion sweep for `needs:`-gated jobs.
//!
//! Translation of `commands::submit_run_tx` over the agreed tables. The
//! working set is gone: every classification is written directly onto `jobs`
//! (`status` = workflow truth, `queue_state` = dispatch copy).

use super::codec::{self, now_us};
use super::concurrency as cg;
use super::{db, jobs, promote, settle, LiteBackend};
use crate::concurrency::{self, Holder};
use crate::control::types::*;
use crate::models::{QueuedJob, StepRecord, TaskAgentJobRequestRecord};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use rusqlite::{params, OptionalExtension, Transaction};
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
        record,
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
        if let Some(existing) = existing {
            if let Some(existing) = jobs::run_record(tx, codec::run_id(&existing))? {
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
                });
            }
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
        });
    }

    // ── The run row (FK target for jobs and the gate's event ordering) ──
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
    let mut held = false;
    if let Some(wf) = &workflow_concurrency {
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
                // supersedes this one): every job lands terminal. No request
                // rows are minted — the run never dispatched.
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
                        display_order.get(&job.job_id.0).copied().unwrap_or(index as i64),
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
                }
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
    let platforms = jobs::registered_platforms(tx)?;
    let mut concluded: Vec<(JobId, ExecutionStatus, Option<String>)> = Vec::new();
    let mut inserted = 0usize;

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
        let unhostable = if check_hostable {
            crate::control::sched::unhostable_platform(&job.runs_on, platforms.iter().copied())
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
                Some(format!("no {platform} runner registered")),
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
            concluded.push((job_id, ExecutionStatus::Skipped, None));
            inserted += 1;
            continue;
        }

        // Held run: every job parks behind the workflow gate.
        if held {
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
            inserted += 1;
            continue;
        }

        // `max-parallel` for this matrix base: legs already handed a slot in
        // this submit count against the limit.
        let max_parallel_ok = job.max_parallel.is_none_or(|limit| {
            active_by_base.get(&job.base_id).copied().unwrap_or(0) < limit
        });
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
            inserted += 1;
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
        mint_request(tx, &namespace, run_id, &job_id, request, token_request, step_manifest)?;
        inserted += 1;
    }

    // ── Promotion sweep for needs-gated jobs ────────────────────────────
    let mut outcome = crate::runtime_scheduling::SchedulingOutcome::default();
    promote::promote_run(tx, backend, run_id, &mut outcome)?;
    for (rid, jid) in outcome.skipped.iter().chain(outcome.failed.iter()) {
        if *rid == run_id {
            if let Some(status) = jobs::job(tx, run_id, jid)?.map(|job| job.status) {
                concluded.push((jid.clone(), status, None));
            }
        }
    }

    // ── Run status ──────────────────────────────────────────────────────
    let summary = jobs::summarize_run_row(tx, run_id)?;
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
    if !held {
        jobs::emit_outbox(
            tx,
            &namespace,
            Some(run_id),
            "run.created.v1",
            serde_json::json!({"status": status_str(status), "jobs": inserted}),
        )?;
    }

    Ok(SubmitOutcome {
        run_id,
        run_number: record.run_number,
        queued_jobs: inserted,
        status,
        concluded,
        held,
        rejected: None,
        existing: None,
        queue_depth: jobs::ready_count(tx)?,
        next_runs_on: jobs::next_ready_labels(tx)?,
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
             concurrency_cancel_in_progress, created_at, started_at, completed_at) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25) \
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
        record.created_at.timestamp_micros(),
        record.started_at.map(|at| at.timestamp_micros()),
        record.completed_at.map(|at| at.timestamp_micros()),
    ])
    .map_err(db)?;
    tx.prepare_cached(
        "INSERT INTO run_submissions (run_id, submission, github_context, \
             workspace_snapshot, snapshot_timing) VALUES (?1,?2,?3,?4,?5) \
         ON CONFLICT (run_id) DO UPDATE SET submission = excluded.submission, \
             github_context = excluded.github_context, \
             workspace_snapshot = excluded.workspace_snapshot, \
             snapshot_timing = excluded.snapshot_timing",
    )
    .map_err(db)?
    .execute(params![
        codec::run_key(record.run_id),
        serde_json::to_string(submission.as_ref())
            .map_err(|e| ControlError::backend(anyhow::anyhow!("submission encode: {e}")))?,
        record.github.to_string(),
        record
            .workspace_snapshot
            .as_ref()
            .map(|snapshot| serde_json::to_string(snapshot).unwrap_or_default()),
        record
            .snapshot_timing
            .as_ref()
            .map(|timing| serde_json::to_string(timing).unwrap_or_default()),
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
    let agent_job_id = request.agent_job_id.to_string();
    let timeline_id = request.timeline_id.to_string();
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
        timeline_id
    ])
    .map_err(db)?;
    let request_id = tx.last_insert_rowid();
    // The runner-facing message carries the allocated request id.
    let patched = tx
        .prepare_cached(
            "SELECT message_template FROM job_messages WHERE run_id = ?1 AND job_id = ?2",
        )
        .map_err(db)?
        .query_row(params![codec::run_key(run_id), job_id.0], |row| {
            row.get::<_, String>(0)
        })
        .optional()
        .map_err(db)?
        .and_then(|json| {
            serde_json::from_str::<preloop_gha_protocol::azdo::AgentJobRequestMessage>(&json).ok()
        });
    if let Some(mut message) = patched {
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
    for (position, step) in step_manifest.iter().enumerate() {
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
    Ok(())
}
