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
    s.parse().unwrap_or_default()
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
            Err(e) if e == "concurrency_queue_overflow" => {
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
