//! The backend-neutral command bodies: the load→sched→write-back glue that
//! runs every `ControlBackend` command against a [`TxState`] working set.
//!
//! Both backends (SQLite and Postgres) load the same `TxState`, call these
//! functions, and write the delta back — so the scheduling semantics live in
//! exactly one place and a backend can never diverge from them. These are
//! pure Rust over the working set; they never touch SQL.

use super::backend::*;
use super::sched;
use super::txstate::TxState;
use super::types::*;
use crate::concurrency;
use preloop_gha_protocol::{azdo, ExecutionStatus, JobId, RunId, SessionId};
use std::collections::BTreeMap;

fn system_to_us(t: std::time::SystemTime) -> i64 {
    t.duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

fn parse_uuid(s: &str) -> uuid::Uuid {
    super::logic::session_uuid(s)
}

pub(crate) fn submit_run_tx(
    tx: &mut TxState,
    mut submit: SubmitRun,
) -> Result<SubmitOutcome, ControlError> {
    // Idempotent replay: a webhook delivery that already produced a run for
    // *this* workflow path returns that run instead of a duplicate. One push
    // delivery legitimately fans out to several workflow files, so the match
    // must be on `(delivery_id, workflow_path)`, not the delivery alone.
    if let Some(delivery_id) = &submit.record.webhook_delivery_id {
        if let Some(existing) = tx
            .runs
            .values()
            .find(|r| {
                r.webhook_delivery_id.as_deref() == Some(delivery_id.as_str())
                    && r.workflow_path_str == submit.record.workflow_path_str
            })
            .cloned()
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
                queue_depth: tx.ready_count.max(0) as usize,
                next_runs_on: sched::next_job_labels(tx),
            });
        }
    }

    let run_id = submit.record.run_id;
    let mut record = submit.record;
    let mut concluded = Vec::new();

    // Reject outright on an empty concurrency group.
    if submit.empty_concurrency_group {
        record.status = ExecutionStatus::Failure;
        return Ok(SubmitOutcome {
            run_id,
            run_number: record.run_number,
            queued_jobs: 0,
            status: record.status,
            concluded: vec![(
                JobId("*".to_owned()),
                ExecutionStatus::Failure,
                Some("empty concurrency group".to_owned()),
            )],
            held: false,
            rejected: Some(ExecutionStatus::Failure),
            existing: None,
            queue_depth: tx.ready_count.max(0) as usize,
            next_runs_on: sched::next_job_labels(tx),
        });
    }

    // Mint every job's request correlation inside the writer transaction so
    // the `job_requests` primary key is allocated under the cross-process
    // writer lock — a process-local atomic would let two engines sharing one
    // database hand out the same id. `build_job_artifacts` stamps a
    // placeholder (0); the real id is minted here and patched onto both the
    // request record and the runner-facing message before they are stored.
    // Mint before the concurrency gate: a run cancelled on arrival (queue
    // overflow) must still settle the placeholder requests it minted,
    // matching the pre-backend ordering.
    for submit_job in &mut submit.jobs {
        if let Some(request) = &mut submit_job.request {
            let request_id = tx.alloc_request_id();
            request.request_id = request_id;
            submit_job.queued.message.request_id = request_id;
            tx.insert_request(request.clone());
            tx.broker_messages
                .insert(request_id, submit_job.queued.message.clone());
        }
        if let Some(token_request) = &submit_job.token_request {
            if let Some(request_id) = tx
                .job_requests
                .values()
                .find(|r| r.run_id == run_id && r.job_id == submit_job.queued.job_id)
                .map(|r| r.request_id)
            {
                tx.github_token_requests
                    .insert(request_id, token_request.clone());
            }
        }
    }

    // Workflow-level concurrency gate.
    let mut held = false;
    // The gate compares the arrival's triggering event against existing
    // holders' runs, so the arriving run must be visible to it. A rejected
    // submit aborts the transaction, so this never persists on error.
    tx.runs.insert(run_id, record.clone());
    if let Some(wf) = &submit.workflow_concurrency {
        let key = concurrency::concurrency_key(&record.submission.repository, &wf.group);
        let holder = concurrency::Holder::Run(run_id);
        match sched::try_acquire_concurrency(
            tx,
            key,
            wf.group.clone(),
            holder,
            wf.cancel_in_progress,
            wf.queue,
        ) {
            Ok(true) => {}
            Ok(false) => held = true,
            Err(e) if e == concurrency::ARRIVAL_CANCELLED => {
                // The run died on arrival: every job is Cancelled and the
                // expandable nodes' minted request correlation is settled
                // here, exactly like a cancellation (MC-3). `pending_jobs`
                // is not yet populated, so expandable nodes come from
                // `submit.jobs` (deferred matrix / reusable call), not the
                // working set.
                record.status = ExecutionStatus::Cancelled;
                for job in &submit.jobs {
                    record
                        .jobs
                        .insert(job.queued.job_id.clone(), ExecutionStatus::Cancelled);
                }
                record.completed_at = Some(record.created_at);
                record.conclusion = Some("cancelled".to_owned());
                tx.runs.insert(run_id, record.clone());
                for job in &submit.jobs {
                    let node_id = &job.queued.job_id;
                    if job.queued.deferred_matrix.is_some() || job.queued.reusable_call.is_some() {
                        let status = sched::node_settle_status(tx, run_id, node_id);
                        sched::retire_node_requests(
                            tx,
                            run_id,
                            node_id,
                            sched::RequestRetirement::Settle(status),
                        );
                    }
                }
                return Ok(SubmitOutcome {
                    run_id,
                    run_number: record.run_number,
                    queued_jobs: record.jobs.len(),
                    status: record.status,
                    concluded: record
                        .jobs
                        .keys()
                        .map(|job_id| {
                            (
                                job_id.clone(),
                                ExecutionStatus::Cancelled,
                                crate::concurrency::cancelled_reason(),
                            )
                        })
                        .collect(),
                    held: false,
                    rejected: Some(ExecutionStatus::Cancelled),
                    existing: None,
                    queue_depth: tx.ready_count.max(0) as usize,
                    next_runs_on: sched::next_job_labels(tx),
                });
            }
            Err(e) => {
                return Err(ControlError::BadRequest(e));
            }
        }
    }

    // Unhostable-platform check against live runners.
    let platforms = sched::registered_runner_platforms(tx);

    // Insert the run record, then classify each job.
    record.status = if held {
        ExecutionStatus::Pending
    } else {
        ExecutionStatus::Queued
    };
    tx.run_namespaces.insert(run_id, submit.namespace);
    tx.runs.insert(run_id, record.clone());
    if let Some(wf) = &submit.workflow_concurrency {
        tx.run_concurrency.insert(run_id, wf.raw.clone());
    }

    let mut held_jobs = Vec::new();
    for submit_job in submit.jobs {
        let job = submit_job.queued;
        let job_id = job.job_id.clone();

        // Unhostable platform: conclude immediately.
        if submit.check_hostable {
            if let Some(platform) = sched::unhostable_platform(&job.runs_on, platforms.clone()) {
                record.jobs.insert(job_id.clone(), ExecutionStatus::Failure);
                concluded.push((
                    job_id.clone(),
                    ExecutionStatus::Failure,
                    Some(format!("no {platform} runner registered")),
                ));
                continue;
            }
        }

        // Request correlation was minted before the gate (see above); the
        // loop only classifies the job now.
        tx.id_token_grants
            .insert((run_id, job_id.clone()), submit_job.id_token_granted);
        if let Some(oidc) = submit_job.oidc_context {
            tx.oidc_job_contexts.insert((run_id, job_id.clone()), oidc);
        }
        if !submit_job.step_manifest.is_empty() {
            if let Some(agent_job_id) = tx
                .job_requests
                .values()
                .find(|r| r.run_id == run_id && r.job_id == job_id)
                .map(|r| r.agent_job_id)
            {
                tx.job_steps.insert(agent_job_id, submit_job.step_manifest);
            }
        }

        if submit_job.initially_skipped {
            record.jobs.insert(job_id.clone(), ExecutionStatus::Skipped);
            concluded.push((job_id.clone(), ExecutionStatus::Skipped, None));
            continue;
        }

        // Held run: every job parks in `held_runs` as Pending until the
        // workflow-level gate frees the slot (ported verbatim — held jobs
        // skip per-job classification entirely).
        if held {
            record.jobs.insert(job_id.clone(), ExecutionStatus::Pending);
            held_jobs.push(job);
            continue;
        }

        // Deferred reusable-caller nodes are scheduling-only: they wait in
        // pending_jobs until their `if:` gate passes, when the scheduler
        // acquires caller/embedded JobSet concurrency gates and expands the
        // callee subtree (mirroring GitHub, which evaluates caller
        // concurrency when the caller job starts). Needs-driven matrix nodes
        // land here too via the needs check below.
        if job.reusable_call.is_some() {
            record.jobs.insert(job_id.clone(), ExecutionStatus::Pending);
            tx.pending_jobs.push_back(job);
            continue;
        }

        // Needs-gated, deferred-matrix, and over-max-parallel jobs wait in
        // pending_jobs: a dynamic matrix or reusable caller must not expand
        // before its `needs:` producers complete, and a matrix leg past
        // `max-parallel` must not dispatch early (the promote sweep routes
        // deferred nodes to `pending_expansions` on DependencyDecision::Run).
        if !job.needs.is_empty() || !sched::under_max_parallel(tx, &job) {
            record.jobs.insert(job_id.clone(), ExecutionStatus::Queued);
            tx.pending_jobs.push_back(job);
            continue;
        }

        // Ready job: evaluate the job-level gate, then enqueue.
        let mut statuses = BTreeMap::new();
        let submission = record.submission.clone();
        let github = record.github.clone();
        match sched::try_enqueue_with_job_concurrency(tx, &github, &submission, job, &mut statuses)
        {
            Ok(true) => {
                record.jobs.insert(job_id.clone(), ExecutionStatus::Queued);
            }
            Ok(false) => {
                record.jobs.insert(job_id.clone(), ExecutionStatus::Pending);
            }
            Err(()) => {
                let status = statuses
                    .get(&job_id)
                    .copied()
                    .unwrap_or(ExecutionStatus::Failure);
                record.jobs.insert(job_id.clone(), status);
                concluded.push((job_id.clone(), status, None));
            }
        }
    }

    if held {
        tx.held_runs.insert(run_id, held_jobs);
    }

    // Write the classified record before the promotion sweep: the sweep
    // mutates `tx.runs` in place (Skip/Error arms set job status and
    // re-summarize the run), so the working-set copy must already carry
    // this run's job statuses for `dependency_decision` to see them.
    tx.runs.insert(run_id, record.clone());

    // Promote any needs-gated jobs already satisfiable.
    let promoted = sched::promote_ready_jobs(tx);

    // Refetch the post-sweep record — the sweep's in-place updates are the
    // ones that must persist (a stale clone re-inserted here would revert
    // promoted Skip/Failure statuses back to Queued).
    let mut record = tx.runs.get(&run_id).cloned().unwrap_or(record);
    for (rid, jid) in promoted.skipped.iter().chain(promoted.failed.iter()) {
        if *rid == run_id {
            if let Some(s) = record.jobs.get(jid) {
                concluded.push((jid.clone(), *s, None));
            }
        }
    }

    // C-05: derive the initial run status from job statuses so eval failures
    // surface immediately. `summarize_run` returns InProgress for any mix of
    // Queued/Pending jobs; a held run stays Pending, a runnable run Queued.
    record.status = if held {
        ExecutionStatus::Pending
    } else {
        let status = sched::summarize_run(record.jobs.values().copied());
        if status == ExecutionStatus::InProgress {
            ExecutionStatus::Queued
        } else {
            status
        }
    };
    sched::finalize_run_if_complete(&mut record);
    tx.runs.insert(run_id, record.clone());

    // `next_runs_on` names the platform of the job at the front of the
    // ready queue. `next_queue_labels` is the pre-transaction front — when
    // the queue was empty it is empty, and this submit's first enqueued
    // job is the new front. `ready_index` holds only this tx's enqueues
    // under a narrow scope, so `front()` is exactly that job.
    let next_runs_on = if !tx.next_queue_labels.is_empty() {
        tx.next_queue_labels.clone()
    } else {
        tx.ready_index
            .front()
            .map(|job| job.runs_on.clone())
            .unwrap_or_default()
    };
    Ok(SubmitOutcome {
        run_id,
        run_number: record.run_number,
        // `queued_jobs` is the accepted job count — every submitted job,
        // ready or pending (needs-gated jobs queue as pending and promote
        // later). The replay conformance check compares it against the
        // golden's delivered job count.
        queued_jobs: record.jobs.len(),
        status: record.status,
        concluded,
        held,
        rejected: None,
        existing: None,
        // `ready_count` is the pre-transaction global count; this tx's
        // enqueues live in `ready_index` (narrow scope holds only them).
        queue_depth: tx.ready_count.max(0) as usize + tx.ready_index.len(),
        next_runs_on,
    })
}

pub(crate) fn poll_session_tx(
    tx: &mut TxState,
    poll: PollRequest,
) -> Result<PollOutcome, ControlError> {
    // Revalidate session ownership inside the claim transaction. The handler
    // caches `runner_id`/capabilities across a long-poll, but a liveness
    // sweep can purge the session while `notified()` is pending; without this
    // check the resumed poll can claim work for a dead or mismatched runner.
    let session_runner_id = tx
        .runner_id_for_session(&poll.session_id)
        .ok_or_else(|| ControlError::Forbidden("session has no runner owner".to_owned()))?;
    if let Some(verified_runner_id) = poll.verified_runner_id {
        if verified_runner_id != session_runner_id {
            return Err(ControlError::Forbidden(
                "session belongs to another runner".to_owned(),
            ));
        }
    }
    tx.mark_session_seen(&poll.session_id);

    // Redeliver an unacknowledged inflight message first.
    if let Some(message) = tx
        .inflight_messages
        .get(&poll.session_id)
        .and_then(|messages| messages.values().next().cloned())
    {
        return Ok(PollOutcome::Inflight(message));
    }

    // A queued cancellation for this session's active job wins over new work.
    if let Some(request_id) = tx.session_active_requests.get(&poll.session_id).copied() {
        if let Some(request) = tx.job_requests.get(&request_id).cloned() {
            if let Some(pos) = tx
                .cancellation_queue
                .iter()
                .position(|c| c.run_id == request.run_id && c.job_id == request.job_id)
            {
                let cancellation = tx.cancellation_queue.remove(pos).unwrap();
                let message = broker_message(
                    tx,
                    &poll.session_id,
                    azdo::message_type::JOB_CANCELLED,
                    concurrency::job_cancel_body(cancellation.agent_job_id),
                );
                return Ok(PollOutcome::Cancel(message));
            }
            if request.result.is_none() {
                return Ok(PollOutcome::ActiveRequest {
                    request,
                    runner_id: session_runner_id,
                });
            }
            tx.session_active_requests.remove(&poll.session_id);
        } else {
            tx.session_active_requests.remove(&poll.session_id);
        }
    }

    // A busy runner takes no new work — inflight/cancel/active already
    // handled above; skip the claim entirely.
    if poll.busy {
        return Ok(PollOutcome::Empty);
    }

    // Pick a claimable job.
    let Some(pos) = sched::choose_claim_position(tx, &poll.runner, poll.verified_runner_id) else {
        return Ok(PollOutcome::Empty);
    };
    let Some(job) = sched::apply_claim(tx, pos) else {
        return Ok(PollOutcome::Empty);
    };

    // Find the request record for this job's live attempt.
    let request_id = tx
        .job_requests
        .values()
        .find(|r| r.run_id == job.run_id && r.job_id == job.job_id && r.result.is_none())
        .map(|r| r.request_id)
        .or_else(|| {
            tx.job_requests
                .values()
                .find(|r| r.run_id == job.run_id && r.job_id == job.job_id)
                .map(|r| r.request_id)
        });
    let Some(request_id) = request_id else {
        return Ok(PollOutcome::Empty);
    };
    if let Some(record) = tx.job_requests.get_mut(&request_id) {
        let now = std::time::SystemTime::now();
        // The owner is the session's resolved runner, not the verified id:
        // an unverified (compat) poll still owns the claim it was handed.
        record.owner_runner_id = Some(session_runner_id);
        record.claimed_at = Some(now);
        record.started_at = Some(now);
        record.last_renewed_at = Some(now);
        record.locked_until = crate::distributed_task::agent_request_locked_until();
    }
    tx.session_active_requests
        .insert(poll.session_id.clone(), request_id);
    // `set_job_status` records the override even when the run isn't loaded,
    // and mirrors into `run.jobs` when it is — a claimed job is `in_progress`.
    tx.set_job_status(job.run_id, job.job_id.clone(), ExecutionStatus::InProgress);
    if let Some(run) = tx.runs.get_mut(&job.run_id) {
        run.status = ExecutionStatus::InProgress;
        run.started_at.get_or_insert_with(chrono::Utc::now);
    }
    let request = tx
        .job_requests
        .get(&request_id)
        .cloned()
        .ok_or_else(|| ControlError::NotFound(format!("request {request_id}")))?;
    Ok(PollOutcome::Claimed(Box::new(ClaimedJob {
        queued: job,
        request,
        runner_id: session_runner_id,
        queue_depth: tx.ready_count.max(0) as usize,
        next_runs_on: sched::next_job_labels(tx),
    })))
}

/// Mint a session-unique broker message and park it in inflight for
/// redelivery-until-ack.
pub(crate) fn broker_message(
    tx: &mut TxState,
    session_id: &str,
    message_type: &str,
    body_json: String,
) -> azdo::TaskAgentMessage {
    // Broker-protocol messageIds must live in a high range: `RunnerJobRequest`
    // messages reuse the job `request_id` (starting at 1) as their messageId,
    // so a cancel/refresh message allocated from the low counter would collide
    // with a live request_id and the runner could not tell them apart. Ratchet
    // the shared counter into the broker range first — this mirrors the old
    // `broker::next_broker_message_id` base and is monotonic across restarts
    // because `next_message_id` is persisted in `counters`.
    const MESSAGE_ID_BASE: i64 = 1_000_000;
    if tx.next_message_id < MESSAGE_ID_BASE {
        tx.next_message_id = MESSAGE_ID_BASE;
    }
    tx.next_message_id += 1;
    let message_id = tx.next_message_id;
    let message = azdo::TaskAgentMessage {
        message_id,
        message_type: message_type.to_owned(),
        body: body_json,
        iv: None,
    };
    tx.inflight_messages
        .entry(session_id.to_owned())
        .or_default()
        .insert(message_id, message.clone());
    message
}

pub(crate) fn complete_job_tx(
    tx: &mut TxState,
    completion: JobCompletionInput,
) -> Result<CompleteOutcome, ControlError> {
    let run_id = completion.run_id;
    let job_id = completion.job_id.clone();

    // Settle the request for this attempt.
    let request_id = completion
        .agent_job_id
        .and_then(|id| tx.agent_job_requests.get(&id).copied())
        .or_else(|| {
            tx.job_requests
                .values()
                .find(|r| r.run_id == run_id && r.job_id == job_id && r.result.is_none())
                .map(|r| r.request_id)
        });
    if let Some(request_id) = request_id {
        sched::settle_request(tx, request_id, completion.status);
    }

    // Flip the job and run status, record outputs.
    let was_terminal_success = tx
        .runs
        .get(&run_id)
        .map(|r| r.status == ExecutionStatus::Success)
        .unwrap_or(false);
    let run = tx
        .runs
        .get_mut(&run_id)
        .ok_or_else(|| ControlError::NotFound(format!("run {run_id}")))?;
    let prior = run.jobs.get(&job_id).copied();
    let replayed = matches!(
        prior,
        Some(ExecutionStatus::Success)
            | Some(ExecutionStatus::Failure)
            | Some(ExecutionStatus::Skipped)
            | Some(ExecutionStatus::Cancelled)
    );
    let effective_status = if replayed {
        prior.unwrap()
    } else {
        run.jobs.insert(job_id.clone(), completion.status);
        completion.status
    };
    if !completion.outputs.is_empty() {
        run.job_outputs
            .insert(job_id.clone(), completion.outputs.clone());
    }
    run.status = sched::summarize_run(run.jobs.values().copied());
    sched::finalize_run_if_complete(run);
    let newly_terminal_success = !was_terminal_success && run.status == ExecutionStatus::Success;
    let run = run.clone();

    // Fail-fast siblings, concurrency release, dependency promotion.
    let cancelled_siblings = if effective_status == ExecutionStatus::Failure {
        sched::apply_matrix_fail_fast(tx, run_id, &job_id)
    } else {
        Vec::new()
    };
    sched::release_concurrency_for_job(tx, run_id, &job_id);
    tx.claimed_jobs.remove(&(run_id, job_id.clone()));
    let scheduling = sched::promote_ready_jobs(tx);

    Ok(CompleteOutcome {
        record: run,
        effective_status,
        newly_terminal_success,
        cancelled_siblings,
        scheduling,
        live_log_key: format!("{}:{}", run_id.0, job_id.0),
        queue_nonempty: tx.queue_nonempty(),
        queue_depth: tx.ready_count.max(0) as usize,
        replayed,
    })
}

pub(crate) fn register_runner_tx(
    tx: &mut TxState,
    reg: RegisterRunner,
) -> Result<RunnerRow, ControlError> {
    // Dedup on client_id: a re-register returns the existing runner.
    if let Some(client_id) = &reg.client_id {
        if let Some(runner_id) = tx.runner_client_ids.get(client_id).copied() {
            if let Some(runner) = tx.runners.get(&runner_id).cloned() {
                return Ok(RunnerRow {
                    runner,
                    rsa_public_key: tx.runner_rsa_public_keys.get(&runner_id).cloned(),
                    client_id: Some(client_id.clone()),
                    pool_proven: tx.pool_proven_runners.contains(&runner_id),
                    registered_at_us: tx
                        .runner_registered_at
                        .get(&runner_id)
                        .map(|t| system_to_us(*t))
                        .unwrap_or(0),
                });
            }
        }
    }

    tx.next_runner_id += 1;
    let runner_id = tx.next_runner_id;
    let runner = preloop_gha_protocol::RegisteredRunner {
        id: runner_id,
        name: reg.name,
        labels: reg.labels,
        ephemeral: reg.ephemeral,
        public_key: reg.public_key,
        runner_group_id: reg.runner_group_id,
        runner_group_name: reg.runner_group_name,
    };
    tx.runners.insert(runner_id, runner.clone());
    tx.runner_registered_at
        .insert(runner_id, std::time::SystemTime::now());
    if let Some(key) = reg.rsa_public_key {
        tx.runner_rsa_public_keys.insert(runner_id, key);
    }
    if let Some(client_id) = &reg.client_id {
        tx.runner_client_ids.insert(client_id.clone(), runner_id);
    }
    if reg.pool_proven {
        tx.pool_proven_runners.insert(runner_id);
        // Pairing is gated on pool proof: `pair_registered_runner` marks the
        // runner pool-proven, so calling it for an unproven registration
        // would let a rogue process steal pairings without the token.
        sched::pair_registered_runner(tx, runner_id);
    }
    Ok(RunnerRow {
        runner,
        rsa_public_key: tx.runner_rsa_public_keys.get(&runner_id).cloned(),
        client_id: reg.client_id,
        pool_proven: reg.pool_proven,
        registered_at_us: system_to_us(std::time::SystemTime::now()),
    })
}

/// In-place agent update for the runner's `PUT
/// /_apis/distributedtask/pools/{pool}/agents/{id}` call. Unlike
/// `register_runner_tx` (which dedups on client_id or allocates a fresh id)
/// this mutates the existing runner row and returns it unchanged in id —
/// the official runner PUTs label/name updates against the id it already
/// holds and expects the same id back.
pub(crate) fn update_runner_tx(
    tx: &mut TxState,
    runner_id: i64,
    name: Option<String>,
    labels: Option<Vec<String>>,
) -> Result<RunnerRow, ControlError> {
    let runner = tx
        .runners
        .get_mut(&runner_id)
        .ok_or_else(|| ControlError::NotFound(format!("runner {runner_id} not found")))?;
    if let Some(name) = name {
        runner.name = name;
    }
    if let Some(labels) = labels {
        runner.labels = labels;
    }
    let runner = runner.clone();
    Ok(RunnerRow {
        runner,
        rsa_public_key: tx.runner_rsa_public_keys.get(&runner_id).cloned(),
        client_id: None,
        pool_proven: tx.pool_proven_runners.contains(&runner_id),
        registered_at_us: tx
            .runner_registered_at
            .get(&runner_id)
            .map(|t| system_to_us(*t))
            .unwrap_or(0),
    })
}

pub(crate) fn create_session_tx(
    tx: &mut TxState,
    session: CreateSession,
) -> Result<SessionRow, ControlError> {
    let session_id = uuid::Uuid::new_v4().to_string();
    let encryption = session.encryption;
    match session.protocol {
        SessionProtocol::Broker => {
            tx.broker_session_runners
                .insert(session_id.clone(), session.runner_id);
        }
        SessionProtocol::Azdo => {
            tx.sessions.insert(
                session_id.clone(),
                preloop_gha_protocol::RunnerSession {
                    session_id: SessionId(parse_uuid(&session_id)),
                    runner_id: session.runner_id,
                },
            );
            tx.azdo_sessions.insert(session_id.clone());
        }
        // Compatibility sessions own no runner; they are persisted with a NULL
        // `runner_id` purely to satisfy `broker_messages`/`active_request_id`
        // foreign keys, so no runner map is populated.
        SessionProtocol::Compat => {}
    }
    if let Some(enc) = &encryption {
        tx.session_keys.insert(session_id.clone(), enc.clone());
    }
    tx.mark_session_seen(&session_id);
    Ok(SessionRow {
        session_id,
        runner_id: session.runner_id,
        protocol: session.protocol,
        client_id: session.client_id,
        encryption,
        active_request_id: None,
        last_seen_at_us: Some(system_to_us(std::time::SystemTime::now())),
    })
}

pub(crate) fn delete_session_tx(tx: &mut TxState, session_id: &str) {
    // Release the session's active request so it can be redelivered.
    if let Some(request_id) = tx.session_active_requests.get(session_id).copied() {
        sched::release_request_for_retry(tx, request_id);
    }
    tx.session_active_requests.remove(session_id);
    tx.broker_session_runners.remove(session_id);
    tx.sessions.remove(session_id);
    tx.azdo_sessions.remove(session_id);
    tx.verified_sessions.remove(session_id);
    tx.session_keys.remove(session_id);
    tx.session_last_seen.remove(session_id);
    tx.inflight_messages.remove(session_id);
}

pub(crate) fn purge_runner_tx(tx: &mut TxState, runner_id: i64) {
    tx.runners.remove(&runner_id);
    tx.runner_client_ids.retain(|_, id| *id != runner_id);
    tx.runner_rsa_public_keys.remove(&runner_id);
    tx.pool_proven_runners.remove(&runner_id);
    tx.runner_registered_at.remove(&runner_id);
    let doomed: Vec<String> = tx
        .broker_session_runners
        .iter()
        .filter(|(_, id)| **id == runner_id)
        .map(|(s, _)| s.clone())
        .chain(
            tx.sessions
                .iter()
                .filter(|(_, s)| s.runner_id == runner_id)
                .map(|(s, _)| s.clone()),
        )
        .collect();

    // Capture this runner's claimed jobs BEFORE deleting its sessions —
    // `delete_session_tx` clears `session_active_requests`, so ownership must
    // be read now. A claimed job belongs to the runner when ANY of:
    //   - its live request records `owner_runner_id` (durable, set on claim);
    //   - a doomed session's `session_active_requests` maps to its request
    //     (covers ownerless claims where `verified_runner_id` was `None`);
    //   - the assignment names the runner (when assignments are enabled).
    let active_request_ids: std::collections::BTreeSet<i64> = doomed
        .iter()
        .filter_map(|s| tx.session_active_requests.get(s).copied())
        .collect();
    let mut requeue: Vec<(RunId, JobId)> = tx
        .claimed_jobs
        .keys()
        .filter(|key| {
            let owned_by_request = tx.job_requests.values().any(|r| {
                r.run_id == key.0
                    && r.job_id == key.1
                    && r.result.is_none()
                    && r.owner_runner_id == Some(runner_id)
            });
            let owned_by_session = tx.job_requests.values().any(|r| {
                active_request_ids.contains(&r.request_id)
                    && r.run_id == key.0
                    && r.job_id == key.1
                    && r.result.is_none()
            });
            let owned_by_assignment =
                tx.job_assignments.get(key).and_then(|r| r.runner_id) == Some(runner_id);
            owned_by_request || owned_by_session || owned_by_assignment
        })
        .cloned()
        .collect();
    requeue.sort();
    requeue.dedup();
    for session_id in doomed {
        delete_session_tx(tx, &session_id);
    }
    // Requeue the captured claims so a replacement runner picks them up.
    for key in requeue {
        sched::requeue_claimed(tx, key.0, &key.1);
    }
    // Assignments this runner never claimed: release each job back to
    // pool-pending so the pool provisions a replacement machine. `retain`
    // alone would strand the job — it would vanish from the provisioning
    // queue even though no runner ever picked it up.
    let orphaned: Vec<(RunId, JobId)> = tx
        .job_assignments
        .iter()
        .filter(|(_, record)| record.runner_id == Some(runner_id))
        .map(|(key, _)| key.clone())
        .collect();
    for key in orphaned {
        if sched::clear_assignment(tx, key.0, &key.1) && tx.pool_assignments_enabled {
            tx.pool_pending
                .entry(key)
                .or_insert_with(std::time::SystemTime::now);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Handler commands (the former `TxState` closures of the HTTP handlers)
// ─────────────────────────────────────────────────────────────────────────

/// `ControlBackend::list_runners` over the working set.
pub(crate) fn list_runners_tx(tx: &TxState, run_id: Option<RunId>) -> RunnerListing {
    let runners: Vec<preloop_gha_protocol::RegisteredRunner> =
        tx.runners.values().cloned().collect();
    let run_queue = run_id.map(|run_id| {
        let queued = tx
            .ready_index
            .iter()
            .filter(|job| job.run_id == run_id)
            .count();
        let claimable = tx
            .runners
            .values()
            .filter(|runner| {
                let caps = crate::runtime_scheduling::capabilities_of(runner);
                tx.ready_index.iter().any(|job| {
                    job.run_id == run_id
                        && crate::runtime_scheduling::job_matches_runner_capabilities(job, &caps)
                })
            })
            .count();
        RunQueueClaimability { queued, claimable }
    });
    RunnerListing { runners, run_queue }
}

/// `ControlBackend::open_runner_session` over the working set.
pub(crate) fn open_runner_session_tx(
    tx: &mut TxState,
    open: OpenRunnerSession,
) -> Result<(), ControlError> {
    let Some(runner_id) = open.runner_id else {
        return Ok(());
    };
    let sid = open.session_id;
    if open.verified {
        // The official control plane answers 409 when the runner already
        // holds a live session. Only verified sessions count: an unverified
        // compat session must not squat a runner id.
        let already_live = tx
            .verified_sessions
            .iter()
            .filter_map(|sid| tx.runner_id_for_session(sid))
            .any(|id| id == runner_id);
        if already_live {
            return Err(ControlError::Conflict(format!(
                "runner {runner_id} already has a live session"
            )));
        }
        tx.verified_sessions.insert(sid.clone());
    }
    match open.protocol {
        SessionProtocol::Azdo => {
            tx.sessions.insert(
                sid.clone(),
                preloop_gha_protocol::RunnerSession {
                    session_id: SessionId(parse_uuid(&sid)),
                    runner_id,
                },
            );
            tx.azdo_sessions.insert(sid.clone());
        }
        // Broker session: `broker_session_runners` only. `sessions` is the
        // AzDO projection — inserting into both emits two `runner_sessions`
        // rows and violates the session_id PK.
        SessionProtocol::Broker => {
            tx.broker_session_runners.insert(sid.clone(), runner_id);
        }
        SessionProtocol::Compat => {
            return Err(ControlError::BadRequest(
                "compat sessions are not opened by runners".to_owned(),
            ));
        }
    }
    tx.mark_session_seen(&sid);
    Ok(())
}

/// `ControlBackend::close_runner_session` over the working set.
pub(crate) fn close_runner_session_tx(
    tx: &mut TxState,
    session_id: &str,
    caller_runner_id: Option<i64>,
) -> Result<bool, ControlError> {
    if let Some(runner_id) = caller_runner_id {
        match tx.runner_id_for_session(session_id) {
            Some(owner) if owner == runner_id => {}
            // Ending another runner's session strands its in-flight job
            // until the lease reaper notices.
            Some(_) => {
                return Err(ControlError::Forbidden(
                    "session belongs to another runner".to_owned(),
                ));
            }
            // Unknown session: nothing to strand, stay idempotent.
            None => return Ok(false),
        }
    }
    tx.sessions.remove(session_id);
    tx.broker_session_runners.remove(session_id);
    tx.verified_sessions.remove(session_id);
    Ok(true)
}

fn runner_has_session(tx: &TxState, runner_id: i64) -> bool {
    tx.sessions
        .values()
        .any(|session| session.runner_id == runner_id)
        || tx
            .broker_session_runners
            .values()
            .any(|id| *id == runner_id)
}

/// `ControlBackend::purge_runner_guarded` over the working set.
pub(crate) fn purge_runner_guarded_tx(
    tx: &mut TxState,
    runner_id: i64,
    guard: PurgeGuard,
) -> Result<bool, ControlError> {
    match guard {
        PurgeGuard::System => {}
        PurgeGuard::Runner(caller) => {
            if caller != runner_id {
                return Err(ControlError::Forbidden(
                    "a runner may only deregister itself".to_owned(),
                ));
            }
        }
        PurgeGuard::RegistrationToken => {
            if runner_has_session(tx, runner_id) {
                return Err(ControlError::Forbidden(
                    "cannot delete an active runner using a registration token".to_owned(),
                ));
            }
        }
        PurgeGuard::IfPhantom => {
            let exists = tx.runners.contains_key(&runner_id)
                || tx.runner_client_ids.values().any(|id| *id == runner_id);
            if !exists || runner_has_session(tx, runner_id) {
                return Ok(false);
            }
        }
    }
    purge_runner_tx(tx, runner_id);
    Ok(true)
}

/// `ControlBackend::lookup_agent` over the working set.
pub(crate) fn lookup_agent_tx(
    tx: &mut TxState,
    name: &str,
) -> Option<(preloop_gha_protocol::RegisteredRunner, String)> {
    let runner = tx.runners.values().find(|r| r.name == name).cloned()?;
    let client_id = tx
        .runner_client_ids
        .iter()
        .find(|(_, &id)| id == runner.id)
        .map(|(k, _)| k.clone())
        .unwrap_or_else(|| format!("{:08x}-0000-4000-8000-000000000000", runner.id as u32));
    tx.runner_client_ids.insert(client_id.clone(), runner.id);
    Some((runner, client_id))
}

/// `ControlBackend::bind_runner_client` over the working set.
pub(crate) fn bind_runner_client_tx(
    tx: &mut TxState,
    runner_id: i64,
    client_id: &str,
    pair_with_pending_job: bool,
) {
    // The OAuth client id must be durable before the runner's next token
    // request, or a restart between registration and commit rejects it.
    tx.runner_client_ids.insert(client_id.to_owned(), runner_id);
    if pair_with_pending_job {
        sched::pair_registered_runner(tx, runner_id);
    }
}

/// `ControlBackend::reap_sweep` over the working set.
pub(crate) fn reap_sweep_tx(tx: &mut TxState, sweep: ReapSweep) -> ReapSweepOutcome {
    use super::logic::{starvation_verdict, StarvationCandidate, StarvationVerdict};
    let ReapSweep {
        now,
        runs: _,
        ready,
        active,
        paused,
        pool_preparing,
        warm_window_open,
    } = sweep;

    let in_queue: std::collections::BTreeSet<(RunId, JobId)> = ready
        .iter()
        .map(|job| (job.run_id, job.job_id.clone()))
        .collect();
    tx.queued_at.retain(|key, _| in_queue.contains(key));
    let mut starved: Vec<(RunId, JobId, String)> = Vec::new();
    for job in &ready {
        let key = (job.run_id, job.job_id.clone());
        let candidate = StarvationCandidate {
            runs_on: &job.runs_on,
            enqueued_at: std::time::UNIX_EPOCH
                + std::time::Duration::from_nanos(job.enqueued_at_unix_nanos as u64),
            first_seen: tx.queued_at.get(&key).copied(),
            any_runner_matches: tx.runners.values().any(|runner| {
                crate::runtime_scheduling::job_matches_runner(&job.runs_on, &runner.labels)
            }),
        };
        match starvation_verdict(&candidate, now, pool_preparing, warm_window_open) {
            StarvationVerdict::ClearMark => {
                tx.queued_at.remove(&key);
            }
            StarvationVerdict::Mark { first_seen } => {
                tx.queued_at.entry(key).or_insert(first_seen);
            }
            StarvationVerdict::Starve { reason, grace } => {
                tracing::warn!(
                    run_id = %job.run_id,
                    job_id = %job.job_id.0,
                    labels = ?job.runs_on,
                    "starving queued job failed after {}s without a matching runner",
                    grace.as_secs()
                );
                tx.queued_at.remove(&key);
                starved.push((job.run_id, job.job_id.clone(), reason));
            }
        }
    }

    let mut cancellations = Vec::new();
    let mut expired = Vec::new();
    for request in &active {
        let (request_id, run_id, job_id) =
            (request.request_id, request.run_id, request.job_id.clone());
        // 1. Timeout enforcement. Time paused at a failed step is debugging,
        // not execution.
        if let Some(started_at) = request.started_at {
            if !request.timeout_triggered {
                let paused = paused.get(&request_id).copied().unwrap_or_default();
                let elapsed = now
                    .duration_since(started_at)
                    .unwrap_or_default()
                    .saturating_sub(paused);
                let job_timeout = tx
                    .broker_messages
                    .get(&request_id)
                    .and_then(|msg| msg.job_timeout)
                    .unwrap_or(21600); // 360 minutes in seconds
                if elapsed >= std::time::Duration::from_secs(job_timeout as u64) {
                    tracing::info!(
                        %run_id,
                        %job_id,
                        request_id,
                        "Job timed out after {}s",
                        job_timeout
                    );
                    if let Some(req) = tx.job_requests.get_mut(&request_id) {
                        req.timeout_triggered = true;
                    }
                    if let Some(agent_job_id) = sched::agent_job_id_for(tx, run_id, &job_id) {
                        cancellations.push(crate::models::QueuedCancellation {
                            run_id,
                            job_id: job_id.clone(),
                            agent_job_id,
                        });
                    }
                }
            }
        }
        // 2. Lease expiration / disconnect reaper.
        if let Some(last_renewed_at) = request.last_renewed_at {
            let elapsed = now.duration_since(last_renewed_at).unwrap_or_default();
            if elapsed >= std::time::Duration::from_secs(crate::distributed_task::JOB_LEASE_SECONDS)
            {
                tracing::info!(
                    %run_id,
                    %job_id,
                    request_id,
                    "Runner lease expired (last renewed {}s ago). Marking job as failed.",
                    elapsed.as_secs()
                );
                if let Some(req) = tx.job_requests.get_mut(&request_id) {
                    req.result = Some(ExecutionStatus::Failure);
                }
                expired.push(ExpiredLease {
                    request_id,
                    run_id,
                    job_id: job_id.clone(),
                    // The lease that expired names the attempt exactly.
                    agent_job_id: tx
                        .job_requests
                        .get(&request_id)
                        .map(|record| record.agent_job_id),
                });
            }
        }
    }
    for lease in &expired {
        tx.inflight_requests.remove(&lease.request_id);
        tx.session_active_requests
            .retain(|_, &mut v| v != lease.request_id);
    }
    let cancellation_count = cancellations.len();
    tx.cancellation_queue.extend(cancellations);

    // Starvation failures: leave the ready queue and turn terminal so a run
    // with no surviving jobs concludes.
    for (run_id, job_id, _) in &starved {
        tx.ready_index
            .retain(|job| job.run_id != *run_id || job.job_id != *job_id);
        tx.queue
            .retain(|job| job.run_id != *run_id || job.job_id != *job_id);
        if let Some(run) = tx.runs.get_mut(run_id) {
            run.jobs.insert(job_id.clone(), ExecutionStatus::Failure);
            run.status = crate::runtime_scheduling::summarize_run(run.jobs.values().copied());
            crate::runtime_scheduling::finalize_run_if_complete(run);
        }
    }
    ReapSweepOutcome {
        cancellations: cancellation_count,
        expired,
        starved,
    }
}

fn count_run_statuses(statuses: impl IntoIterator<Item = ExecutionStatus>) -> (u32, u32, u32) {
    let mut queued = 0;
    let mut in_progress = 0;
    let mut completed = 0;
    for status in statuses {
        match status {
            ExecutionStatus::Queued => queued += 1,
            ExecutionStatus::Pending | ExecutionStatus::InProgress => in_progress += 1,
            ExecutionStatus::Success
            | ExecutionStatus::Failure
            | ExecutionStatus::Skipped
            | ExecutionStatus::Cancelled => completed += 1,
        }
    }
    (queued, in_progress, completed)
}

/// `ControlBackend::status_inputs` over the working set.
pub(crate) fn status_inputs_tx(tx: &TxState, stale_after: std::time::Duration) -> StatusInputs {
    let (runs_queued, runs_in_progress, runs_completed) =
        count_run_statuses(tx.runs.values().map(|run| run.status));
    let mut runner_ids_by_run: BTreeMap<RunId, std::collections::BTreeSet<i64>> = BTreeMap::new();
    for (key, assignment) in &tx.job_assignments {
        if let Some(runner_id) = assignment.runner_id {
            runner_ids_by_run
                .entry(key.0)
                .or_default()
                .insert(runner_id);
        }
    }
    for request in tx.job_requests.values() {
        if request.result.is_none() {
            if let Some(runner_id) = request.owner_runner_id {
                runner_ids_by_run
                    .entry(request.run_id)
                    .or_default()
                    .insert(runner_id);
            }
        }
    }
    let mut active_runs: Vec<_> = tx
        .runs
        .values()
        .filter(|run| !run.status.is_terminal())
        .map(|run| {
            let assigned_runners = runner_ids_by_run
                .get(&run.run_id)
                .into_iter()
                .flatten()
                .filter_map(|runner_id| tx.runners.get(runner_id))
                .map(|runner| runner.name.clone())
                .collect();
            preloop_observability::status::ActiveRunSnapshot {
                run_id: run.run_id.to_string(),
                workflow: run.workflow_path_str.clone(),
                status: serde_json::to_value(run.status)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .unwrap_or_else(|| "unknown".to_owned()),
                event: run.event.clone(),
                started_at: run.started_at,
                assigned_runners,
            }
        })
        .collect();
    active_runs.sort_by(|left, right| {
        right
            .started_at
            .cmp(&left.started_at)
            .then_with(|| right.run_id.cmp(&left.run_id))
    });
    // A run can sit `in_progress` while nothing is executing it: its claim
    // leaked, its machine died, or a restart dropped the pairing.
    let live_run_ids: std::collections::BTreeSet<RunId> = tx
        .queue
        .iter()
        .map(|job| job.run_id)
        .chain(tx.claimed_jobs.keys().map(|(run_id, _)| *run_id))
        .chain(tx.pending_jobs.iter().map(|job| job.run_id))
        .collect();
    let orphaned_run_ids: Vec<String> = active_runs
        .iter()
        .filter(|run| run.status == "in_progress" && run.assigned_runners.is_empty())
        .filter(|run| {
            run.run_id
                .parse::<uuid::Uuid>()
                .map(RunId)
                .is_ok_and(|run_id| !live_run_ids.contains(&run_id))
        })
        .map(|run| run.run_id.clone())
        .collect();
    let now_unix_nanos = crate::models::now_unix_nanos();
    let oldest_ready = sched::ready_jobs(tx)
        .filter(|job| job.enqueued_at_unix_nanos != 0)
        .min_by_key(|job| job.enqueued_at_unix_nanos);
    let oldest_ready_seconds = oldest_ready.map(|job| {
        now_unix_nanos.saturating_sub(job.enqueued_at_unix_nanos) as f64 / 1_000_000_000.0
    });
    let session_ids: std::collections::BTreeSet<&String> = tx
        .broker_session_runners
        .keys()
        .chain(tx.sessions.keys())
        .collect();

    // busy = an owned session holds an active request; stale = every owned
    // session stopped polling; idle = ≥1 session and neither. A session never
    // seen (restored) is not stale.
    let now = std::time::SystemTime::now();
    let mut runner_idle = 0u32;
    let mut runner_busy = 0u32;
    let mut runner_stale = 0u32;
    for runner_id in tx.runners.keys() {
        let mut owned: std::collections::BTreeSet<&String> = tx
            .broker_session_runners
            .iter()
            .filter(|(_, owner)| **owner == *runner_id)
            .map(|(session_id, _)| session_id)
            .collect();
        owned.extend(
            tx.sessions
                .iter()
                .filter(|(_, session)| session.runner_id == *runner_id)
                .map(|(session_id, _)| session_id),
        );
        let busy = owned
            .iter()
            .any(|session_id| tx.session_active_requests.contains_key(*session_id));
        let stale = !owned.is_empty()
            && owned.iter().all(|session_id| {
                tx.session_last_seen
                    .get(*session_id)
                    .and_then(|seen| now.duration_since(*seen).ok())
                    .is_some_and(|elapsed| elapsed > stale_after)
            });
        if busy {
            runner_busy += 1;
        }
        if stale {
            runner_stale += 1;
        }
        if !owned.is_empty() && !busy && !stale {
            runner_idle += 1;
        }
    }
    let runner_assignments = crate::runtime_scheduling::live_runner_assignments(
        &tx.job_requests,
        &tx.session_active_requests,
        std::time::SystemTime::now(),
    );
    let mut concurrency_groups_active = 0u32;
    let mut concurrency_groups_contended = 0u32;
    let mut concurrency_pending_holders = 0u32;
    let mut concurrency_deepest_group_pending = 0u32;
    for group in tx.concurrency_groups.values() {
        if group.running.is_some() {
            concurrency_groups_active += 1;
        }
        if !group.pending.is_empty() {
            concurrency_groups_contended += 1;
        }
        concurrency_pending_holders += group.pending.len() as u32;
        concurrency_deepest_group_pending =
            concurrency_deepest_group_pending.max(group.pending.len() as u32);
    }
    StatusInputs {
        queue_len: tx.ready_count.max(0) as usize,
        pending_jobs_len: tx.pending_jobs.len(),
        pending_expansions_len: tx.pending_expansions.len(),
        expanding_len: tx.expanding.len(),
        runs_queued,
        runs_in_progress,
        runs_completed,
        active_runs,
        orphaned_run_ids,
        registered: tx.runners.len() as u32,
        sessions: session_ids.len() as u32,
        runner_idle,
        runner_busy,
        runner_stale,
        runner_assignments,
        oldest_ready_seconds,
        oldest_ready_run_id: oldest_ready.map(|job| job.run_id.to_string()),
        oldest_ready_job_id: oldest_ready.map(|job| job.job_id.0.clone()),
        queue_runner_reqs: sched::ready_jobs(tx)
            .map(|job| (job.runs_on.clone(), job.runner_group.clone()))
            .collect(),
        runner_caps: tx
            .runners
            .values()
            .map(crate::runtime_scheduling::capabilities_of)
            .collect(),
        concurrency_blocked: tx.concurrency_blocked.len() as u32,
        concurrency_groups_active,
        concurrency_groups_contended,
        concurrency_pending_holders,
        concurrency_deepest_group_pending,
        released_bindings: tx.released_bindings_count,
    }
}

/// `ControlBackend::rebuild_dispatch_intent` over the working set.
pub(crate) fn rebuild_dispatch_intent_tx(tx: &mut TxState) {
    let ready: Vec<crate::models::QueuedJob> = sched::ready_jobs(tx).cloned().collect();
    for job in &ready {
        sched::on_job_enqueued(tx, job);
    }
}

/// Queue a per-session message in the working set's inflight map. Job
/// assignments store only the request id (as the body) so the handler builds
/// and encrypts the message when it answers the poll.
fn queue_session_message(
    tx: &mut TxState,
    session_id: &str,
    message_type: &str,
    request_id: Option<i64>,
    body: Option<String>,
) -> SessionMessage {
    tx.next_message_id += 1;
    let message_id = tx.next_message_id;
    let stored = azdo::TaskAgentMessage {
        message_id,
        message_type: message_type.to_owned(),
        body: request_id
            .map(|id| id.to_string())
            .or_else(|| body.clone())
            .unwrap_or_default(),
        iv: None,
    };
    tx.inflight_messages
        .entry(session_id.to_owned())
        .or_default()
        .insert(message_id, stored);
    SessionMessage {
        message_id,
        message_type: message_type.to_owned(),
        request_id,
        body,
    }
}

/// Decode an inflight message queued by [`queue_session_message`].
fn session_message_of(stored: &azdo::TaskAgentMessage) -> SessionMessage {
    let is_job = stored.message_type == azdo::message_type::PIPELINE_AGENT_JOB_REQUEST;
    SessionMessage {
        message_id: stored.message_id,
        message_type: stored.message_type.clone(),
        request_id: is_job.then(|| stored.body.parse().ok()).flatten(),
        body: (!is_job).then(|| stored.body.clone()),
    }
}

/// `ControlBackend::poll_azdo_session` over the working set.
pub(crate) fn poll_azdo_session_tx(tx: &mut TxState, poll: AzdoPoll) -> AzdoPollOutcome {
    let sid = poll.session_id;
    if let Some(runner_id) = poll.verified_runner_id {
        if tx.runner_id_for_session(&sid) != Some(runner_id) {
            return AzdoPollOutcome::Forbidden;
        }
    }
    tx.mark_session_seen(&sid);
    if let Some(stored) = tx
        .inflight_messages
        .get(&sid)
        .and_then(|messages| messages.values().next())
    {
        return AzdoPollOutcome::Redeliver(session_message_of(stored));
    }
    if let Some(request_id) = tx.session_active_requests.get(&sid).copied() {
        let request_finished = tx
            .job_requests
            .get(&request_id)
            .is_none_or(|request| request.result.is_some());
        if request_finished {
            tx.session_active_requests.remove(&sid);
        } else {
            let cancellation_pos = tx.job_requests.get(&request_id).and_then(|request| {
                tx.cancellation_queue.iter().position(|cancellation| {
                    cancellation.run_id == request.run_id && cancellation.job_id == request.job_id
                })
            });
            let Some(pos) = cancellation_pos else {
                return AzdoPollOutcome::Wait;
            };
            let cancellation = tx
                .cancellation_queue
                .remove(pos)
                .expect("cancellation position was found in the queue");
            return AzdoPollOutcome::Cancel(queue_session_message(
                tx,
                &sid,
                azdo::message_type::JOB_CANCELLED,
                None,
                Some(concurrency::job_cancel_body(cancellation.agent_job_id)),
            ));
        }
    }
    let runner = tx.runner_capabilities_for_session(&sid);
    // The poll already proved `verified_runner_id` owns the session.
    let verified_claim = poll.verified_runner_id;
    let Some(queued) = sched::choose_claim_position(tx, &runner, verified_claim)
        .and_then(|pos| sched::apply_claim(tx, pos))
    else {
        return AzdoPollOutcome::Wait;
    };
    let claimed_at = std::time::SystemTime::now();
    if let Some(run) = tx.runs.get_mut(&queued.run_id) {
        run.status = ExecutionStatus::InProgress;
        run.jobs
            .insert(queued.job_id.clone(), ExecutionStatus::InProgress);
    }
    let request_id = queued.message.request_id;
    let owner_runner_id = verified_claim.or_else(|| tx.runner_id_for_session(&sid));
    tx.session_active_requests.insert(sid.clone(), request_id);
    if let Some(request) = tx.job_requests.get_mut(&request_id) {
        request.owner_runner_id = owner_runner_id;
        request.claimed_at = Some(claimed_at);
        request.started_at = Some(claimed_at);
        request.last_renewed_at = Some(claimed_at);
    }
    let message = queue_session_message(
        tx,
        &sid,
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST,
        Some(request_id),
        None,
    );
    AzdoPollOutcome::Claimed {
        message,
        run_id: queued.run_id,
        job_id: queued.job_id,
    }
}

/// The broker ownership ladder over the working set: the immutable claim
/// owner wins; else the runner of the session whose active request it is.
pub(crate) fn ensure_broker_request_owner(
    tx: &TxState,
    request_id: i64,
    runner_id: i64,
) -> Result<(), ControlError> {
    let owner_runner_id = tx
        .job_requests
        .get(&request_id)
        .and_then(|request| request.owner_runner_id);
    let session_id = tx
        .session_active_requests
        .iter()
        .find_map(|(session_id, active)| (*active == request_id).then_some(session_id.clone()));
    let has_session = session_id.is_some();
    let session_runner = session_id.and_then(|sid| tx.runner_id_for_session(&sid));
    ensure_request_owner(owner_runner_id, session_runner, has_session, runner_id)
}

fn latest_agent_job_id(tx: &TxState, run_id: RunId, job_id: &JobId) -> Option<uuid::Uuid> {
    tx.job_requests
        .values()
        .filter(|record| record.run_id == run_id && record.job_id == *job_id)
        .max_by_key(|record| record.request_id)
        .map(|record| record.agent_job_id)
}

/// `ControlBackend::settle_job` over the working set. `guard_terminal`: the
/// working set is narrow (a workflow-grouped run without global concurrency
/// state), so a completion that finishes the run returns
/// [`ControlError::WidenScope`] and the caller reruns it globally.
pub(crate) fn settle_job_tx(
    tx: &mut TxState,
    input: SettleJob,
    guard_terminal: bool,
) -> Result<SettleJobOutcome, ControlError> {
    let SettleJob {
        completion: comp,
        settle,
    } = input;
    let unchanged = |tx: &TxState| {
        tx.runs
            .get(&comp.run_id)
            .cloned()
            .map(|run| SettleJobOutcome::Unchanged(Box::new(run)))
            .ok_or_else(|| ControlError::NotFound("run not found".to_owned()))
    };
    if let Some(settle) = settle {
        let request_id = tx
            .agent_job_requests
            .get(&settle.agent_job_id)
            .copied()
            .ok_or_else(|| {
                ControlError::NotFound("broker complete request not found".to_owned())
            })?;
        ensure_broker_request_owner(tx, request_id, settle.runner_id)?;
        if tx
            .job_requests
            .get(&request_id)
            .is_some_and(|record| record.result.is_some())
        {
            tracing::info!(request_id, "broker complete: ignoring duplicate completion");
            return unchanged(tx);
        }
        if let Some(record) = tx.job_requests.get_mut(&request_id) {
            record.result = Some(comp.status);
            record.locked_until = crate::distributed_task::agent_request_locked_until();
        }
        // Free the session so the next poll can take a new job now.
        tx.session_active_requests
            .retain(|_, &mut rid| rid != request_id);
        if tx.inflight_requests.remove(&request_id).is_none() {
            tracing::warn!(
                request_id,
                "broker complete: no inflight_requests entry found"
            );
            return unchanged(tx);
        }
    }
    let mut newly_terminal_success = false;
    tx.claimed_jobs.remove(&(comp.run_id, comp.job_id.clone()));
    let finalized_callers: Vec<JobId>;
    {
        let run = tx
            .runs
            .get_mut(&comp.run_id)
            .ok_or_else(|| ControlError::NotFound("run not found".to_owned()))?;
        let prior =
            run.jobs.get(&comp.job_id).copied().ok_or_else(|| {
                ControlError::Backend(anyhow::anyhow!("job does not belong to run"))
            })?;
        if prior.is_terminal() && prior != ExecutionStatus::Cancelled {
            return Ok(SettleJobOutcome::Unchanged(Box::new(run.clone())));
        }
        let tolerated = run
            .job_continue_on_error
            .get(&comp.job_id.to_string())
            .copied()
            .unwrap_or(false);
        let reported_status = if tolerated && comp.status == ExecutionStatus::Failure {
            ExecutionStatus::Success
        } else {
            comp.status
        };
        let effective = match (prior, reported_status) {
            (ExecutionStatus::Cancelled, ExecutionStatus::Success)
            | (ExecutionStatus::Cancelled, ExecutionStatus::Failure) => ExecutionStatus::Cancelled,
            _ => reported_status,
        };
        run.jobs.insert(comp.job_id.clone(), effective);
        let job_name = comp.job_id.0.clone();
        let annotations = crate::distributed_task::mask_completion_annotations(run, &comp);
        if let Some(detail) = crate::models::JobDetail::find(&mut run.jobs_list, &job_name) {
            detail.conclusion = format!("{:?}", effective).to_lowercase();
            if !comp.annotations.is_empty() {
                detail.annotations = annotations;
            }
        } else {
            run.jobs_list.push(crate::models::JobDetail {
                job_id: job_name.clone(),
                name: job_name,
                conclusion: format!("{:?}", effective).to_lowercase(),
                steps: Vec::new(),
                annotations,
            });
        }
        run.job_outputs.insert(
            comp.job_id.clone(),
            comp.outputs
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );
        finalized_callers = crate::reusable_workflows::propagate_reusable_outputs(run);
        run.status = crate::runtime_scheduling::summarize_run(run.jobs.values().copied());
        if run.started_at.is_none() {
            run.started_at = Some(chrono::Utc::now());
        }
        if matches!(
            run.status,
            ExecutionStatus::Success
                | ExecutionStatus::Failure
                | ExecutionStatus::Cancelled
                | ExecutionStatus::Skipped
        ) && run.completed_at.is_none()
        {
            run.completed_at = Some(chrono::Utc::now());
            run.conclusion = Some(crate::runtime_scheduling::status_string(run.status));
            newly_terminal_success = run.status == ExecutionStatus::Success;
        }
    }
    let attempt = comp
        .agent_job_id
        .or_else(|| latest_agent_job_id(tx, comp.run_id, &comp.job_id));
    let live_log_key = attempt
        .map(|agent_job_id| agent_job_id.to_string())
        .unwrap_or_else(|| comp.job_id.0.clone());
    let effective_status = tx
        .runs
        .get(&comp.run_id)
        .and_then(|r| r.jobs.get(&comp.job_id).copied())
        .unwrap_or(comp.status);
    if let Some(manifest) = attempt.and_then(|id| tx.job_steps.get_mut(&id)) {
        for wire in &comp.step_results {
            let Some(external_id) = wire.external_id.as_deref() else {
                continue;
            };
            let Some(pos) = crate::models::StepRecord::find_by_id(manifest, external_id) else {
                continue;
            };
            if let Some(conclusion) = crate::distributed_task::completion_step_conclusion(wire) {
                manifest[pos].conclusion = conclusion;
            }
            if let Some(number) = wire.number.and_then(|n| u32::try_from(n).ok()) {
                manifest[pos].runner_number = Some(number);
            }
        }
        let orphan_conclusion = crate::runtime_scheduling::status_string(effective_status);
        for step in manifest.iter_mut() {
            if step.conclusion == "in_progress" {
                step.conclusion = orphan_conclusion.clone();
                step.finished_at = step.finished_at.or(Some(chrono::Utc::now()));
            }
        }
    }
    let cancelled_siblings = if effective_status == ExecutionStatus::Failure {
        sched::apply_matrix_fail_fast(tx, comp.run_id, &comp.job_id)
    } else {
        Vec::new()
    };
    tx.retain_ready(|job| !(job.run_id == comp.run_id && job.job_id == comp.job_id));
    tx.pending_jobs
        .retain(|job| !(job.run_id == comp.run_id && job.job_id == comp.job_id));
    tx.concurrency_blocked
        .retain(|job| !(job.run_id == comp.run_id && job.job_id == comp.job_id));
    if let Some(held) = tx.held_runs.get_mut(&comp.run_id) {
        held.retain(|job| job.job_id != comp.job_id);
        if held.is_empty() {
            tx.held_runs.remove(&comp.run_id);
        }
    }
    sched::release_concurrency_for_job(tx, comp.run_id, &comp.job_id);
    for caller_id in &finalized_callers {
        sched::release_concurrency_for_job(tx, comp.run_id, caller_id);
    }
    let scheduling = sched::promote_ready_jobs(tx);
    let finished_request_ids: Vec<i64> = tx
        .job_requests
        .iter()
        .filter(|(_, r)| r.run_id == comp.run_id && r.job_id == comp.job_id)
        .map(|(id, _)| *id)
        .collect();
    for request_id in finished_request_ids {
        // Shared settlement also clears the session's moot `JobCancellation`
        // inflight message so it is not redelivered on the next busy poll.
        sched::settle_request(tx, request_id, effective_status);
    }
    // `ready_count` is the O(1) pre-tx global count; `ready_index` holds this
    // tx's promotions under the narrow scope.
    let queue_nonempty =
        tx.ready_count > 0 || !tx.ready_index.is_empty() || !tx.cancellation_queue.is_empty();
    let queue_len = tx.ready_count.max(0) as usize;
    // The load-time queue front, or this completion's first promotion when
    // the queue was empty.
    let next_runs_on = match sched::next_job_labels(tx) {
        labels if labels.is_empty() => tx
            .queue
            .front()
            .map(|job| job.runs_on.clone())
            .unwrap_or_default(),
        labels => labels,
    };
    // This completion finished the run: its workflow-level hold must be
    // released, which needs every group and holder run.
    if guard_terminal
        && tx.runs.get(&comp.run_id).is_some_and(|run| {
            concurrency::holder_is_terminal(&concurrency::Holder::Run(comp.run_id), &run.jobs)
        })
    {
        return Err(ControlError::WidenScope);
    }
    Ok(SettleJobOutcome::Settled(Box::new(JobSettled {
        effective_status,
        cancelled_siblings,
        scheduling,
        queue_nonempty,
        newly_terminal_success,
        live_log_key,
        queue_len,
        next_runs_on,
    })))
}

/// `ControlBackend::oidc_grant` over the working set.
pub(crate) fn oidc_grant_tx(
    tx: &TxState,
    plan_id: &str,
    agent_job_id: uuid::Uuid,
) -> Result<OidcGrant, ControlError> {
    let request_id = tx
        .plan_requests
        .get(plan_id)
        .copied()
        .ok_or_else(|| ControlError::NotFound("OIDC: plan not found".to_owned()))?;
    let request = tx
        .job_requests
        .get(&request_id)
        .ok_or_else(|| ControlError::NotFound("OIDC: job request not found".to_owned()))?;
    if request.agent_job_id != agent_job_id {
        return Err(ControlError::NotFound(
            "OIDC: plan and job do not match".to_owned(),
        ));
    }
    let key = (request.run_id, request.job_id.clone());
    let granted = tx.id_token_grants.get(&key).copied().unwrap_or(false);
    let context = tx.oidc_job_contexts.get(&key).cloned().ok_or_else(|| {
        ControlError::Backend(anyhow::anyhow!("OIDC context missing for dispatched job"))
    })?;
    let run = tx
        .runs
        .get(&request.run_id)
        .cloned()
        .ok_or_else(|| ControlError::NotFound("OIDC: run not found".to_owned()))?;
    Ok(OidcGrant {
        run,
        job_id: key.1,
        granted,
        context,
    })
}

/// `ControlBackend::cancel_run` over the working set.
pub(crate) fn cancel_run_tx(
    tx: &mut TxState,
    run_id: RunId,
    reason: Option<&str>,
) -> Result<CancelOutcome, ControlError> {
    if !tx.runs.contains_key(&run_id) {
        return Err(ControlError::NotFound("run not found".to_owned()));
    }
    let cancellations = sched::cancel_run_inner(tx, run_id, reason);
    Ok(cancel_outcome(tx, run_id, cancellations))
}

/// The shared `CancelOutcome` projection after a cancel transition.
pub(crate) fn cancel_outcome(tx: &TxState, run_id: RunId, cancellations: usize) -> CancelOutcome {
    let record = tx.runs.get(&run_id).cloned();
    let cancelled_jobs = record
        .as_ref()
        .map(|run| {
            run.jobs
                .iter()
                .filter(|(_, status)| **status == ExecutionStatus::Cancelled)
                .map(|(job_id, _)| job_id.clone())
                .collect()
        })
        .unwrap_or_default();
    CancelOutcome {
        cancellations,
        run_status: record.as_ref().map(|r| r.status),
        queue_nonempty: !tx.ready_index.is_empty()
            || !tx.queue.is_empty()
            || !tx.cancellation_queue.is_empty(),
        record,
        cancelled_jobs,
        queue_depth: tx.ready_index.len(),
        next_runs_on: sched::next_job_labels(tx),
    }
}
