use super::*;

/// Enqueue a ready job, applying job-level concurrency if present.
/// Returns Ok(true) if pushed to ready queue, Ok(false) if parked, Err if cancelled.
pub(crate) fn try_enqueue_with_job_concurrency(
    inner: &mut InnerState,
    github: &serde_json::Value,
    submission: &WorkflowSubmission,
    queued_job: QueuedJob,
    statuses: &mut BTreeMap<JobId, ExecutionStatus>,
) -> Result<bool, ()> {
    let Some(raw) = queued_job.concurrency.clone() else {
        statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Queued);
        on_job_enqueued(inner, &queued_job);
        inner.queue.push_back(queued_job);
        return Ok(true);
    };

    let strategy = queued_job
        .message
        .context_data
        .get("strategy")
        .map(azdo::PipelineContextData::to_json)
        .unwrap_or_else(|| json!({}));
    let eval_ctx = concurrency::ConcurrencyContext {
        scope: concurrency::ConcurrencyScope::Job,
        github,
        vars: &submission.vars,
        inputs: &submission.inputs,
        matrix: Some(&queued_job.matrix),
        strategy: Some(&strategy),
        needs: None,
    };
    let eval = concurrency::evaluate_concurrency(&raw, &eval_ctx);
    let (group, cancel, queue) = match eval {
        Ok(v) => v,
        Err(e) => {
            concurrency::log_eval_error("job concurrency", &e);
            statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Failure);
            return Err(());
        }
    };
    if group.trim().is_empty() {
        statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Failure);
        return Err(());
    }

    let key = concurrency::concurrency_key(&submission.repository, &group);
    let holder = concurrency::Holder::Job {
        run_id: queued_job.run_id,
        job_id: queued_job.job_id.clone(),
    };
    match try_acquire_concurrency(inner, key, group, holder, cancel, queue) {
        Ok(true) => {
            statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Queued);
            on_job_enqueued(inner, &queued_job);
            inner.queue.push_back(queued_job);
            Ok(true)
        }
        Ok(false) => {
            statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Pending);
            inner.concurrency_blocked.push_back(queued_job);
            Ok(false)
        }
        Err(e) if e == "concurrency_queue_overflow" => {
            statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Cancelled);
            let _ = queued_job;
            Err(())
        }
        Err(_) => {
            statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Failure);
            Err(())
        }
    }
}
/// Resolve the agent job GUID for an in-flight job, if any.
pub(crate) fn agent_job_id_for(
    inner: &InnerState,
    run_id: RunId,
    job_id: &JobId,
) -> Option<uuid::Uuid> {
    inner
        .job_requests
        .values()
        .find(|r| r.run_id == run_id && r.job_id == *job_id && r.result.is_none())
        .map(|r| r.agent_job_id)
        .or_else(|| {
            // Also check via inflight_requests if result already set but still relevant.
            inner
                .job_requests
                .values()
                .find(|r| r.run_id == run_id && r.job_id == *job_id)
                .map(|r| r.agent_job_id)
        })
}

/// Cancel a run: mark non-terminal jobs Cancelled, enqueue JobCancellation for
/// in-flight jobs, remove from queues/held/blocked, and release concurrency.
/// Returns the number of cancellation messages enqueued.
pub(crate) fn cancel_run_inner(
    inner: &mut InnerState,
    run_id: RunId,
    reason: Option<&str>,
) -> usize {
    let mut in_progress: Vec<JobId> = Vec::new();
    {
        let Some(record) = inner.runs.get_mut(&run_id) else {
            return 0;
        };
        record.status = ExecutionStatus::Cancelled;
        for (job_id, status) in &mut record.jobs {
            if matches!(*status, ExecutionStatus::InProgress) {
                in_progress.push(job_id.clone());
            }
            if matches!(
                *status,
                ExecutionStatus::Queued | ExecutionStatus::Pending | ExecutionStatus::InProgress
            ) {
                *status = ExecutionStatus::Cancelled;
            }
        }
    }

    let mut cancellations = Vec::new();
    for job_id in in_progress {
        if let Some(agent_job_id) = agent_job_id_for(inner, run_id, &job_id) {
            cancellations.push(QueuedCancellation {
                run_id,
                job_id,
                agent_job_id,
            });
        }
    }
    let count = cancellations.len();
    inner.cancellation_queue.extend(cancellations);

    inner.queue.retain(|job| job.run_id != run_id);
    inner.pending_jobs.retain(|job| job.run_id != run_id);
    inner.held_runs.remove(&run_id);
    inner.job_assignments.retain(|(id, _), _| *id != run_id);
    inner.pool_pending.retain(|(id, _), _| *id != run_id);
    inner.concurrency_blocked.retain(|job| job.run_id != run_id);
    inner.dap_ports.remove(&run_id);
    // Drop any deferred subtree work for this run. Clearing the `expanding`
    // reservation is what makes an in-flight build discard its result instead
    // of folding cancelled jobs back into the run.
    inner.pending_expansions.retain(|job| job.run_id != run_id);
    inner.expanding.retain(|(id, _)| *id != run_id);

    // Release any concurrency holders belonging to this run and promote next.
    release_concurrency_for_run(inner, run_id);
    inner.jobset_admissions.retain(|id, _| id.run_id != run_id);
    inner.jobset_ready.retain(|id| id.run_id != run_id);

    let _ = reason; // events emitted by caller when needed
    count
}

/// Cancel a single job (job-level concurrency / fail-fast style).
pub(crate) fn cancel_job_inner(inner: &mut InnerState, run_id: RunId, job_id: &JobId) -> usize {
    let was_in_progress = {
        let Some(record) = inner.runs.get_mut(&run_id) else {
            return 0;
        };
        let Some(status) = record.jobs.get_mut(job_id) else {
            return 0;
        };
        let in_progress = matches!(*status, ExecutionStatus::InProgress);
        if matches!(
            *status,
            ExecutionStatus::Queued | ExecutionStatus::Pending | ExecutionStatus::InProgress
        ) {
            *status = ExecutionStatus::Cancelled;
        }
        record.status = summarize_run(record.jobs.values().copied());
        in_progress
    };

    let mut count = 0;
    if was_in_progress {
        if let Some(agent_job_id) = agent_job_id_for(inner, run_id, job_id) {
            inner.cancellation_queue.push_back(QueuedCancellation {
                run_id,
                job_id: job_id.clone(),
                agent_job_id,
            });
            count = 1;
        }
    }
    inner
        .queue
        .retain(|j| !(j.run_id == run_id && j.job_id == *job_id));
    inner
        .job_assignments
        .retain(|(id, jid), _| !(*id == run_id && *jid == *job_id));
    inner
        .pool_pending
        .retain(|(id, jid), _| !(*id == run_id && *jid == *job_id));
    inner
        .pending_jobs
        .retain(|j| !(j.run_id == run_id && j.job_id == *job_id));
    inner
        .concurrency_blocked
        .retain(|j| !(j.run_id == run_id && j.job_id == *job_id));
    if let Some(held) = inner.held_runs.get_mut(&run_id) {
        held.retain(|j| j.job_id != *job_id);
    }
    // Same for a single node: dropping the reservation makes any in-flight
    // build of its subtree discard itself when it tries to apply.
    inner
        .pending_expansions
        .retain(|j| !(j.run_id == run_id && j.job_id == *job_id));
    inner.expanding.remove(&(run_id, job_id.clone()));

    // Cancelling a reusable caller cancels its materialized subtree with it.
    let inner_ids = inner
        .runs
        .get(&run_id)
        .and_then(|run| run.reusable_calls.get(&job_id.0))
        .map(|call| call.inner_job_ids.clone())
        .unwrap_or_default();
    for inner_id in inner_ids {
        count += cancel_job_inner(inner, run_id, &JobId(inner_id));
    }

    release_concurrency_for_job(inner, run_id, job_id);
    count
}

pub(crate) fn release_concurrency_for_run(inner: &mut InnerState, run_id: RunId) {
    let keys: Vec<(String, String)> = inner.holder_keys.get(&run_id).cloned().unwrap_or_default();
    for key in keys {
        if let Some(group) = inner.concurrency_groups.get_mut(&key) {
            let running_match = group
                .running
                .as_ref()
                .is_some_and(|h| h.is_run_holder(run_id) || h.run_id() == run_id);
            if running_match {
                let done = group.running.take();
                if let Some(done) = done {
                    // Only release if all jobs terminal OR this was a cancel of the whole run.
                    promote_next_from_group(inner, &key, done);
                }
            } else {
                // Remove from pending queue.
                if let Some(group) = inner.concurrency_groups.get_mut(&key) {
                    group.pending.retain(|h| h.run_id() != run_id);
                    if group.running.is_none() && group.pending.is_empty() {
                        inner.concurrency_groups.remove(&key);
                    }
                }
            }
        }
    }
    // C-07: discard all key tracking for this run now that every group has been released.
    inner.holder_keys.remove(&run_id);
}

pub(crate) fn release_concurrency_for_job(inner: &mut InnerState, run_id: RunId, job_id: &JobId) {
    let keys: Vec<(String, String)> = inner.concurrency_groups.keys().cloned().collect();
    for key in keys {
        let should_release = {
            let Some(group) = inner.concurrency_groups.get(&key) else {
                continue;
            };
            match &group.running {
                Some(h) if h.contains_job(run_id, job_id) => {
                    // Job holders release immediately; Run/JobSet when all terminal.
                    match h {
                        concurrency::Holder::Job { .. } => true,
                        concurrency::Holder::Run(_) | concurrency::Holder::JobSet { .. } => inner
                            .runs
                            .get(&run_id)
                            .is_some_and(|r| concurrency::holder_is_terminal(h, &r.jobs)),
                    }
                }
                _ => false,
            }
        };
        // Also drop pending entries for this job.
        if let Some(group) = inner.concurrency_groups.get_mut(&key) {
            group.pending.retain(|h| !h.contains_job(run_id, job_id));
        }
        if should_release {
            if let Some(group) = inner.concurrency_groups.get_mut(&key) {
                if let Some(done) = group.running.take() {
                    promote_next_from_group(inner, &key, done);
                }
            }
        } else if let Some(group) = inner.concurrency_groups.get(&key) {
            if group.running.is_none() && group.pending.is_empty() {
                inner.concurrency_groups.remove(&key);
            }
        }
        // C-07: prune this key from holder_keys when the run has no remaining
        // presence in the group (neither running nor pending).
        let run_still_present = inner.concurrency_groups.get(&key).is_some_and(|g| {
            g.running.as_ref().is_some_and(|h| h.run_id() == run_id)
                || g.pending.iter().any(|h| h.run_id() == run_id)
        });
        if !run_still_present {
            if let Some(rkeys) = inner.holder_keys.get_mut(&run_id) {
                rkeys.retain(|k| k != &key);
                if rkeys.is_empty() {
                    inner.holder_keys.remove(&run_id);
                }
            }
        }
    }
}

/// Release a single concurrency key acquired by a JobSet whose members all
/// became terminal before any could dispatch (e.g. embedded gate overflow).
/// Removes the running holder from the group and promotes the next pending.
pub(crate) fn merge_jobset_gate(gates: &mut Vec<JobSetGate>, mut gate: JobSetGate) {
    if let Some(existing) = gates.iter_mut().find(|existing| existing.key == gate.key) {
        existing.cancel_in_progress |= gate.cancel_in_progress;
        if gate.queue == aksh_gha_parser::ConcurrencyQueue::Single {
            existing.queue = aksh_gha_parser::ConcurrencyQueue::Single;
        }
        return;
    }
    gate.display_name = gate.display_name.trim().to_owned();
    gates.push(gate);
    gates.sort_by(|left, right| left.key.cmp(&right.key));
}

pub(crate) fn release_holder_key(
    inner: &mut InnerState,
    key: &(String, String),
    holder: &concurrency::Holder,
) {
    let mut promote = None;
    if let Some(group) = inner.concurrency_groups.get_mut(key) {
        if group.running.as_ref() == Some(holder) {
            promote = group.running.take();
        } else {
            group.pending.retain(|pending| pending != holder);
        }
    }
    if let Some(done) = promote {
        promote_next_from_group(inner, key, done);
    }
    if inner
        .concurrency_groups
        .get(key)
        .is_some_and(|group| group.running.is_none() && group.pending.is_empty())
    {
        inner.concurrency_groups.remove(key);
    }

    let run_id = holder.run_id();
    let run_still_present = inner.concurrency_groups.get(key).is_some_and(|group| {
        group
            .running
            .as_ref()
            .is_some_and(|candidate| candidate.run_id() == run_id)
            || group
                .pending
                .iter()
                .any(|candidate| candidate.run_id() == run_id)
    });
    if !run_still_present {
        if let Some(keys) = inner.holder_keys.get_mut(&run_id) {
            keys.retain(|candidate| candidate != key);
            if keys.is_empty() {
                inner.holder_keys.remove(&run_id);
            }
        }
    }
}

pub(crate) fn release_jobset_admission(inner: &mut InnerState, id: &JobSetId) {
    let Some(admission) = inner.jobset_admissions.remove(id) else {
        return;
    };
    let holder = id.holder();
    for key in admission.acquired_keys {
        release_holder_key(inner, &key, &holder);
    }
}

pub(crate) fn advance_jobset_admission(
    inner: &mut InnerState,
    id: &JobSetId,
    promoted_key: Option<&(String, String)>,
) -> Result<JobSetAdmissionResult, String> {
    if let Some(key) = promoted_key {
        if let Some(admission) = inner.jobset_admissions.get_mut(id) {
            admission.acquired_keys.insert(key.clone());
        }
    }

    loop {
        let next_gate = {
            let Some(admission) = inner.jobset_admissions.get(id) else {
                return Ok(JobSetAdmissionResult::Ready);
            };
            admission
                .gates
                .iter()
                .find(|gate| !admission.acquired_keys.contains(&gate.key))
                .cloned()
        };
        let Some(gate) = next_gate else {
            inner.jobset_admissions.remove(id);
            return Ok(JobSetAdmissionResult::Ready);
        };

        let holder = id.holder();
        match try_acquire_concurrency(
            inner,
            gate.key.clone(),
            gate.display_name,
            holder,
            gate.cancel_in_progress,
            gate.queue,
        ) {
            Ok(true) => {
                if let Some(admission) = inner.jobset_admissions.get_mut(id) {
                    admission.acquired_keys.insert(gate.key);
                }
            }
            Ok(false) => return Ok(JobSetAdmissionResult::Blocked),
            Err(error) => {
                release_jobset_admission(inner, id);
                return Err(error);
            }
        }
    }
}

/// After a holder finishes, promote the next pending holder for the group.
pub(crate) fn promote_next_from_group(
    inner: &mut InnerState,
    key: &(String, String),
    _done: concurrency::Holder,
) {
    let next = {
        let Some(group) = inner.concurrency_groups.get_mut(key) else {
            return;
        };
        group.pending.pop_front()
    };

    let Some(next) = next else {
        if let Some(group) = inner.concurrency_groups.get(key) {
            if group.running.is_none() && group.pending.is_empty() {
                inner.concurrency_groups.remove(key);
            }
        }
        return;
    };

    // Install as running immediately for Run and JobSet; for Holder::Job, defer
    // until max-parallel is confirmed free so the job cannot contend with its
    // own pending holder (C-01).
    if !matches!(&next, concurrency::Holder::Job { .. }) {
        if let Some(group) = inner.concurrency_groups.get_mut(key) {
            group.running = Some(next.clone());
        }
    }

    match next {
        concurrency::Holder::Run(run_id) => {
            if let Some(jobs) = inner.held_runs.remove(&run_id) {
                for mut job in jobs {
                    if let Some(run) = inner.runs.get_mut(&run_id) {
                        run.jobs.insert(job.job_id.clone(), ExecutionStatus::Queued);
                    }
                    // Re-check needs/max_parallel before queueing.
                    let needs_ok = inner.runs.get(&run_id).is_some_and(|run| {
                        job.needs
                            .iter()
                            .all(|n| scheduling::need_satisfied(&run.jobs, n))
                    });
                    if needs_ok && under_max_parallel(inner, &job) {
                        if let Some(run) = inner.runs.get_mut(&run_id) {
                            hydrate_needs_context(&mut job, run);
                        }
                        on_job_enqueued(inner, &job);
                        inner.queue.push_back(job);
                    } else {
                        if let Some(run) = inner.runs.get_mut(&run_id) {
                            // keep Queued status in pending_jobs path
                            run.jobs.insert(job.job_id.clone(), ExecutionStatus::Queued);
                        }
                        inner.pending_jobs.push_back(job);
                    }
                }
                if let Some(run) = inner.runs.get_mut(&run_id) {
                    if run.status == ExecutionStatus::Pending {
                        run.status = ExecutionStatus::Queued;
                    }
                }
            }
        }
        concurrency::Holder::Job { run_id, job_id } => {
            let pos = inner
                .concurrency_blocked
                .iter()
                .position(|j| j.run_id == run_id && j.job_id == job_id);
            let Some(pos) = pos else { return };
            // Remove the job temporarily so we can call under_max_parallel
            // without a mutable/immutable borrow conflict on inner.
            let mut job = inner.concurrency_blocked.remove(pos).unwrap();
            if !under_max_parallel(inner, &job) {
                // max-parallel still full: restore the holder at the front of
                // the pending queue and put the job back where it was so the
                // next release event or promote_ready_jobs sweep can retry.
                inner.concurrency_blocked.insert(pos, job);
                if let Some(group) = inner.concurrency_groups.get_mut(key) {
                    group
                        .pending
                        .push_front(concurrency::Holder::Job { run_id, job_id });
                }
                return;
            }
            // Both gates clear: atomically install as running and dispatch.
            if let Some(group) = inner.concurrency_groups.get_mut(key) {
                group.running = Some(concurrency::Holder::Job { run_id, job_id });
            }
            if let Some(run) = inner.runs.get_mut(&run_id) {
                run.jobs.insert(job.job_id.clone(), ExecutionStatus::Queued);
                hydrate_needs_context(&mut job, run);
            }
            on_job_enqueued(inner, &job);
            inner.queue.push_back(job);
        }
        concurrency::Holder::JobSet { run_id, job_ids } => {
            let id = JobSetId {
                run_id,
                job_ids: job_ids.clone(),
            };
            match advance_jobset_admission(inner, &id, Some(key)) {
                Ok(JobSetAdmissionResult::Blocked) => return,
                Err(_) => {
                    cancel_holder(
                        inner,
                        &concurrency::Holder::JobSet { run_id, job_ids },
                        concurrency::cancelled_reason().as_deref(),
                    );
                    return;
                }
                Ok(JobSetAdmissionResult::Ready) => {}
            }

            // Gates acquired. Caller placeholder nodes do not dispatch: they go
            // back to pending_jobs flagged ready, and the next promote sweep
            // materializes the callee subtree.
            inner.jobset_ready.insert(id.clone());
            let mut to_queue = Vec::new();
            inner.concurrency_blocked.retain(|job| {
                if job.run_id == run_id && job_ids.contains(&job.job_id) {
                    to_queue.push(job.clone());
                    false
                } else {
                    true
                }
            });
            for mut job in to_queue {
                if job.reusable_call.is_some() {
                    // Caller nodes terminate through their subtree, never
                    // through dispatch.
                    if let Some(run) = inner.runs.get_mut(&run_id) {
                        run.jobs
                            .insert(job.job_id.clone(), ExecutionStatus::Pending);
                    }
                    inner.pending_jobs.push_back(job);
                    continue;
                }
                if under_max_parallel(inner, &job) {
                    if let Some(run) = inner.runs.get_mut(&run_id) {
                        run.jobs.insert(job.job_id.clone(), ExecutionStatus::Queued);
                        hydrate_needs_context(&mut job, run);
                    }
                    on_job_enqueued(inner, &job);
                    inner.queue.push_back(job);
                } else {
                    if let Some(run) = inner.runs.get_mut(&run_id) {
                        run.jobs.insert(job.job_id.clone(), ExecutionStatus::Queued);
                    }
                    inner.pending_jobs.push_back(job);
                }
            }
        }
    }
}

/// Try to acquire a concurrency slot for a holder. Returns:
/// - `Ok(true)` if the holder may proceed (slot acquired / free)
/// - `Ok(false)` if parked as pending
/// - `Err("cancelled")` if the arrival itself was cancelled (queue max overflow)
/// - `Err(msg)` for evaluation / empty-group errors
pub(crate) fn try_acquire_concurrency(
    inner: &mut InnerState,
    key: (String, String),
    display_name: String,
    holder: concurrency::Holder,
    cancel_in_progress: bool,
    queue: aksh_gha_parser::ConcurrencyQueue,
) -> Result<bool, String> {
    let group = inner
        .concurrency_groups
        .entry(key.clone())
        .or_insert_with(|| concurrency::ConcurrencyGroup {
            display_name: display_name.clone(),
            running: None,
            pending: VecDeque::new(),
        });
    if group.display_name.is_empty() {
        group.display_name = display_name;
    }

    if group.running.is_none() {
        group.running = Some(holder.clone());
        let _ = group;
        track_holder_key(inner, &holder, key);
        return Ok(true);
    }

    if cancel_in_progress {
        let prev = group.running.take();
        // Docs: "any existing pending job or workflow in the same concurrency
        // group will be canceled" — drain all pending holders too.
        let stale_pending: Vec<concurrency::Holder> = group.pending.drain(..).collect();
        group.running = Some(holder.clone());
        let _ = group;
        track_holder_key(inner, &holder, key.clone());
        if let Some(prev) = prev {
            cancel_holder(inner, &prev, concurrency::cancelled_reason().as_deref());
        }
        for pending in stale_pending {
            cancel_holder(inner, &pending, concurrency::cancelled_reason().as_deref());
        }
        return Ok(true);
    }
    let _ = group;

    // Contended — apply queue mode for this arrival.
    let join = {
        let group = inner.concurrency_groups.get(&key).unwrap();
        concurrency::apply_queue_mode(queue, &group.pending)
    };

    for pending_holder in join.cancel_pending {
        if pending_holder.run_id() == holder.run_id() {
            continue;
        }
        cancel_holder(
            inner,
            &pending_holder,
            concurrency::cancelled_reason().as_deref(),
        );
        if let Some(group) = inner.concurrency_groups.get_mut(&key) {
            group.pending.retain(|h| h != &pending_holder);
        }
    }

    if join.cancel_arrival {
        return Err("concurrency_queue_overflow".to_owned());
    }

    if join.park_arrival {
        if let Some(group) = inner.concurrency_groups.get_mut(&key) {
            // After single-mode clears, re-push.
            group.pending.push_back(holder.clone());
        }
        track_holder_key(inner, &holder, key);
        return Ok(false);
    }

    Ok(true)
}

pub(crate) fn track_holder_key(
    inner: &mut InnerState,
    holder: &concurrency::Holder,
    key: (String, String),
) {
    let run_id = holder.run_id();
    let keys = inner.holder_keys.entry(run_id).or_default();
    if !keys.contains(&key) {
        keys.push(key);
    }
}

pub(crate) fn cancel_holder(
    inner: &mut InnerState,
    holder: &concurrency::Holder,
    _reason: Option<&str>,
) {
    match holder {
        concurrency::Holder::Run(run_id) => {
            cancel_run_inner(inner, *run_id, Some("concurrency_cancelled"));
        }
        concurrency::Holder::Job { run_id, job_id } => {
            cancel_job_inner(inner, *run_id, job_id);
        }
        concurrency::Holder::JobSet { run_id, job_ids } => {
            inner.jobset_admissions.remove(&JobSetId {
                run_id: *run_id,
                job_ids: job_ids.clone(),
            });
            inner.jobset_ready.remove(&JobSetId {
                run_id: *run_id,
                job_ids: job_ids.clone(),
            });
            for job_id in job_ids {
                cancel_job_inner(inner, *run_id, job_id);
            }
            // If all jobs cancelled, mark run cancelled when appropriate.
            if let Some(run) = inner.runs.get_mut(run_id) {
                if run.jobs.values().all(|status| status.is_terminal()) {
                    run.status = summarize_run(run.jobs.values().copied());
                }
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct SchedulingOutcome {
    pub(crate) promoted: usize,
    pub(crate) skipped: Vec<(RunId, JobId)>,
    pub(crate) failed: Vec<(RunId, JobId)>,
}

impl SchedulingOutcome {
    /// Fold a later sweep's result into this one, so a caller that promotes,
    /// expands and promotes again reports one combined outcome.
    pub(crate) fn merge(&mut self, other: SchedulingOutcome) {
        self.promoted += other.promoted;
        self.skipped.extend(other.skipped);
        self.failed.extend(other.failed);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DependencyDecision {
    Wait,
    Run,
    Skip,
    Error,
}

/// Promote or skip pending jobs once every declared dependency is terminal.
///
/// Deliberately performs no subtree expansion: a ready reusable-caller or
/// dynamic-matrix node is handed to `inner.pending_expansions` so the heavy
/// build happens outside the global lock. Callers run [`drain_expansions`]
/// once they have released the guard.
pub(crate) fn promote_ready_jobs(inner: &mut InnerState) -> SchedulingOutcome {
    let mut outcome = SchedulingOutcome::default();
    loop {
        let mut promoted_by_base: BTreeMap<(RunId, String), u64> = BTreeMap::new();
        let mut promoted = Vec::new();
        let mut remaining = VecDeque::new();
        let mut settled = false;

        while let Some(mut job) = inner.pending_jobs.pop_front() {
            let decision = inner
                .runs
                .get(&job.run_id)
                .map(|run| dependency_decision(run, &job))
                .unwrap_or(DependencyDecision::Wait);
            match decision {
                DependencyDecision::Run if job.reusable_call.is_some() => {
                    // Deferred reusable caller: the `if:` gate passed. Acquire
                    // caller+embedded JobSet concurrency gates, then
                    // materialize the callee subtree. A false gate never
                    // reaches here — the generic Skip arm recorded the single
                    // skipped entry.
                    settled = true;
                    let set_id = JobSetId {
                        run_id: job.run_id,
                        job_ids: BTreeSet::from([job.job_id.clone()]),
                    };
                    let ready = inner.jobset_ready.remove(&set_id);
                    if !ready {
                        if inner.jobset_admissions.contains_key(&set_id) {
                            // Still waiting on a gate; park until a release
                            // event routes it back via jobset_ready.
                            inner.concurrency_blocked.push_back(job);
                            continue;
                        }
                        match caller_jobset_gates(inner, &job) {
                            Err(status) => {
                                if let Some(run) = inner.runs.get_mut(&job.run_id) {
                                    run.jobs.insert(job.job_id.clone(), status);
                                    run.status = summarize_run(run.jobs.values().copied());
                                    finalize_run_if_complete(run);
                                }
                                outcome.failed.push((job.run_id, job.job_id));
                                continue;
                            }
                            Ok(Some(gates)) => {
                                inner.jobset_admissions.insert(
                                    set_id.clone(),
                                    JobSetAdmission {
                                        gates,
                                        acquired_keys: BTreeSet::new(),
                                    },
                                );
                                match advance_jobset_admission(inner, &set_id, None) {
                                    Ok(JobSetAdmissionResult::Ready) => {}
                                    Ok(JobSetAdmissionResult::Blocked) => {
                                        inner.concurrency_blocked.push_back(job);
                                        continue;
                                    }
                                    Err(error) => {
                                        let status = if error == "concurrency_queue_overflow" {
                                            ExecutionStatus::Cancelled
                                        } else {
                                            ExecutionStatus::Failure
                                        };
                                        if let Some(run) = inner.runs.get_mut(&job.run_id) {
                                            run.jobs.insert(job.job_id.clone(), status);
                                            run.status = summarize_run(run.jobs.values().copied());
                                            finalize_run_if_complete(run);
                                        }
                                        outcome.failed.push((job.run_id, job.job_id));
                                        continue;
                                    }
                                }
                            }
                            Ok(None) => {}
                        }
                    }
                    // Gates are held. Building the callee subtree is heavy,
                    // so hand it to `drain_expansions` rather than doing it
                    // with the global lock held.
                    defer_expansion(inner, job);
                }
                DependencyDecision::Run if job.deferred_matrix.is_some() => {
                    // Dynamic `needs`-driven matrix. Expanding it re-parses the
                    // workflow and builds a runner message per combination, so
                    // it is deferred exactly like a reusable caller.
                    settled = true;
                    defer_expansion(inner, job);
                }
                DependencyDecision::Run
                    if under_max_parallel(inner, &job)
                        && promoted_by_base
                            .get(&(job.run_id, job.base_id.clone()))
                            .copied()
                            .unwrap_or(0)
                            < job.max_parallel.unwrap_or(u64::MAX) =>
                {
                    if let Some(run) = inner.runs.get(&job.run_id) {
                        hydrate_needs_context(&mut job, run);
                    }
                    *promoted_by_base
                        .entry((job.run_id, job.base_id.clone()))
                        .or_default() += 1;
                    promoted.push(job);
                }
                DependencyDecision::Skip | DependencyDecision::Error => {
                    if let Some(run) = inner.runs.get_mut(&job.run_id) {
                        let status = if decision == DependencyDecision::Skip {
                            ExecutionStatus::Skipped
                        } else {
                            ExecutionStatus::Failure
                        };
                        run.jobs.insert(job.job_id.clone(), status);
                        run.status = summarize_run(run.jobs.values().copied());
                        finalize_run_if_complete(run);
                    }
                    if decision == DependencyDecision::Skip {
                        outcome.skipped.push((job.run_id, job.job_id));
                    } else {
                        outcome.failed.push((job.run_id, job.job_id));
                    }
                    settled = true;
                }
                DependencyDecision::Wait | DependencyDecision::Run => remaining.push_back(job),
            }
        }

        outcome.promoted += promoted.len();
        inner.pending_jobs = remaining;
        inner.queue.extend(promoted);
        if !settled {
            return outcome;
        }
    }
}

pub(crate) fn dependency_decision(run: &RunRecord, job: &QueuedJob) -> DependencyDecision {
    if job.needs.is_empty() {
        return DependencyDecision::Run;
    }
    let direct_statuses = job
        .needs
        .iter()
        .flat_map(|need| matching_need_statuses(run, need))
        .collect::<Vec<_>>();
    if direct_statuses.is_empty() || direct_statuses.iter().any(|status| !status.is_terminal()) {
        return DependencyDecision::Wait;
    }
    let statuses = ancestor_statuses(run, job);
    let aggregate = aggregate_need_status(&statuses).unwrap_or(ExecutionStatus::Skipped);
    let context = job.condition_context.clone().with_status(
        aggregate == ExecutionStatus::Success,
        aggregate == ExecutionStatus::Failure,
        aggregate == ExecutionStatus::Cancelled,
    );
    let mut context = context;
    context.insert("needs", needs_json_context(run, &job.needs));
    let condition = aksh_gha_expressions::effective_condition(job.if_condition.as_deref());
    match aksh_gha_expressions::eval_bool(&condition, &context) {
        Ok(true) => DependencyDecision::Run,
        Ok(false) => DependencyDecision::Skip,
        Err(_) => DependencyDecision::Error,
    }
}

pub(crate) fn matching_need_ids(run: &RunRecord, need: &JobId) -> Vec<JobId> {
    run.jobs
        .keys()
        .filter(|job_id| {
            *job_id == need
                || run
                    .job_base_ids
                    .get(*job_id)
                    .is_some_and(|base| base == &need.0)
        })
        .cloned()
        .collect()
}

pub(crate) fn matching_need_statuses(run: &RunRecord, need: &JobId) -> Vec<ExecutionStatus> {
    matching_need_ids(run, need)
        .iter()
        .filter_map(|job_id| run.jobs.get(job_id).copied())
        .collect()
}

pub(crate) fn ancestor_statuses(run: &RunRecord, job: &QueuedJob) -> Vec<ExecutionStatus> {
    let mut pending = job
        .needs
        .iter()
        .flat_map(|need| matching_need_ids(run, need))
        .collect::<Vec<_>>();
    let mut visited = std::collections::BTreeSet::new();
    let mut statuses = Vec::new();

    while let Some(job_id) = pending.pop() {
        if !visited.insert(job_id.clone()) {
            continue;
        }
        if let Some(status) = run.jobs.get(&job_id) {
            statuses.push(*status);
        }
        if let Some(needs) = run.job_needs.get(&job_id) {
            pending.extend(needs.iter().flat_map(|need| matching_need_ids(run, need)));
        }
    }
    statuses
}

/// Check if a job's `runs-on` labels match a runner's registered labels.
///
/// A job matches when every label in the job's `runs-on` is present in the
/// runner's label set (case-insensitive). GitHub-hosted runner labels like
/// Match required labels against a runner's labels.
pub(crate) fn job_matches_runner(job_labels: &[String], runner_labels: &[String]) -> bool {
    if job_labels.is_empty() {
        return true;
    }
    // Unknown runner (no session→runner mapping) matches any job labels.
    if runner_labels.is_empty() {
        return true;
    }
    let runner_set: std::collections::HashSet<String> =
        runner_labels.iter().map(|l| l.to_lowercase()).collect();
    job_labels.iter().all(|required| {
        let req = required.to_lowercase();
        if runner_set.contains(&req) {
            return true;
        }
        if req.starts_with("ubuntu") && runner_set.contains("linux") {
            return true;
        }
        if req.starts_with("macos") && runner_set.contains("macos") {
            return true;
        }
        if req.starts_with("windows") && runner_set.contains("windows") {
            return true;
        }
        runner_set.contains("self-hosted")
            && (req.starts_with("ubuntu") || req.starts_with("macos") || req.starts_with("windows"))
    })
}

/// Match an explicit job group against a registered runner's group.
/// Group is separate from labels; missing metadata on a known runner is the
/// default group (id 1, name `Default`).
pub(crate) fn job_matches_runner_group(
    required_group: Option<&str>,
    runner: &RunnerCapabilities,
) -> bool {
    let Some(required) = required_group.map(str::trim).filter(|v| !v.is_empty()) else {
        return true;
    };
    if !runner.known {
        return false;
    }
    if let Ok(required_id) = required.parse::<i64>() {
        return match runner.runner_group_id {
            Some(actual_id) => actual_id == required_id,
            None => runner.runner_group_name.is_none() && required_id == 1,
        };
    }
    match (&runner.runner_group_id, &runner.runner_group_name) {
        (Some(id), Some(name)) if *id != 1 => name.eq_ignore_ascii_case(required),
        (_, Some(name)) => name.eq_ignore_ascii_case(required),
        (None, None) | (Some(1), None) => "Default".eq_ignore_ascii_case(required),
        (Some(_), None) => false,
    }
}

pub(crate) fn job_matches_runner_capabilities(
    job: &QueuedJob,
    runner: &RunnerCapabilities,
) -> bool {
    job_matches_runner(&job.runs_on, &runner.labels)
        && job_matches_runner_group(job.runner_group.as_deref(), runner)
}

/// Find and remove the first job matching the given runner's labels and group.
pub(crate) fn take_matching_job(
    inner: &mut InnerState,
    runner: &RunnerCapabilities,
    verified_runner_id: Option<i64>,
) -> Option<QueuedJob> {
    // Drop stale bookkeeping so nothing expires into an effective grant and
    // the maps cannot grow without bound across a long-lived server.
    let now = std::time::SystemTime::now();
    inner
        .job_assignments
        .retain(|_, record| assignment_fresh(record.at, now));
    inner
        .pool_pending
        .retain(|_, at| assignment_fresh(*at, now));
    let pos = inner.queue.iter().position(|job| {
        if !job_matches_runner_capabilities(job, runner) {
            return false;
        }
        claim_permitted(inner, job, verified_runner_id)
    })?;
    let job = inner.queue.remove(pos)?;
    let key = (job.run_id, job.job_id.clone());
    inner.job_assignments.remove(&key);
    inner.pool_pending.remove(&key);
    inner.claimed_jobs.insert(key, job.clone());
    Some(job)
}

/// How long an assignment or pool-pending mark stays authoritative. After
/// expiry a job falls back to ordinary permissive scheduling so a crashed
/// pool or dead machine can never wedge a queued job forever.
pub(crate) const ASSIGNMENT_TTL: std::time::Duration = std::time::Duration::from_secs(600);

/// How long a pre-claim assignment stays exclusive. Provisioning is bursty
/// and pool runners exit unexpectedly; if the paired runner has not claimed
/// within this window, any *verified* runner may take the job (and later
/// registrations steal the pairing), because an already-dead owner can
/// otherwise hold a job hostage for the full [`ASSIGNMENT_TTL`].
pub(crate) const CLAIM_BINDING_TTL: std::time::Duration = std::time::Duration::from_secs(120);

fn assignment_fresh(at: std::time::SystemTime, now: std::time::SystemTime) -> bool {
    now.duration_since(at)
        .map(|age| age < ASSIGNMENT_TTL)
        .unwrap_or(false)
}

fn binding_fresh(at: std::time::SystemTime, now: std::time::SystemTime) -> bool {
    now.duration_since(at)
        .map(|age| age < CLAIM_BINDING_TTL)
        .unwrap_or(false)
}

/// Whether `verified_runner_id` may claim `job` right now, independent of
/// runner capabilities.
///
/// `verified_runner_id` is the runner proven by a listen token — never the
/// session's self-declared `agent.id`, which untrusted code inside a pool
/// machine can fabricate. That distinction is what stops a compromised
/// machine from pulling jobs assigned to other machines.
fn claim_permitted(inner: &InnerState, job: &QueuedJob, verified_runner_id: Option<i64>) -> bool {
    let key = (job.run_id, job.job_id.clone());
    let now = std::time::SystemTime::now();
    if let Some(record) = inner.job_assignments.get(&key) {
        if !binding_fresh(record.first_at, now) {
            // The job has been bound to *some* machine since longer than the
            // binding window without ever being claimed. A pool that keeps
            // provisioning and losing machines re-stamps `at` on every
            // registration, so without this ceiling an established, capable
            // runner is starved for as long as the churn continues.
            return verified_runner_id.is_some();
        }
        if !binding_fresh(record.at, now) {
            // Stale pairing: the owner is presumed dead. Only a verified
            // runner identity may take over — unverified sessions keep the
            // old permissive-rules treatment below.
            return verified_runner_id.is_some();
        }
        return Some(record.runner_id) == verified_runner_id;
    }
    if let Some(marked_at) = inner.pool_pending.get(&key) {
        // A machine is being provisioned for this job; nobody claims it
        // until that machine registers and the assignment is stamped. The
        // hold is bounded the same way: provisioning that never lands must
        // not starve a healthy runner past the binding window.
        if binding_fresh(*marked_at, now) {
            return false;
        }
        return verified_runner_id.is_some();
    }
    !inner.require_job_assignments
}

/// Record dispatch intent for a newly queued job.
///
/// Preference order:
///  1. an idle, capable, session-bound runner already registered
///  2. the pool-pending set (a machine will be provisioned / is being
///     provisioned), blocking all claims until registration pairs the job
///
/// With no pool and no strict flag this is a no-op, leaving external-runner
/// installs on the historical first-poller-wins behavior.
pub(crate) fn on_job_enqueued(inner: &mut InnerState, job: &QueuedJob) {
    if !inner.pool_assignments_enabled && !inner.require_job_assignments {
        return;
    }
    let key = (job.run_id, job.job_id.clone());
    if inner.job_assignments.contains_key(&key) {
        return;
    }
    inner.pool_pending.remove(&key);
    let mut busy: std::collections::BTreeSet<i64> = inner
        .job_assignments
        .values()
        .map(|record| record.runner_id)
        .collect();
    for session_id in inner.session_active_requests.keys() {
        if let Some(runner_id) = inner.runner_id_for_session(session_id) {
            busy.insert(runner_id);
        }
    }
    let mut candidates: std::collections::BTreeSet<i64> =
        inner.broker_session_runners.values().copied().collect();
    candidates.extend(inner.sessions.values().map(|session| session.runner_id));
    for runner_id in candidates {
        if busy.contains(&runner_id) {
            continue;
        }
        let Some(runner) = inner.runners.get(&runner_id) else {
            continue;
        };
        if job_matches_runner_capabilities(job, &capabilities_of(runner))
            && inner
                .job_assignments
                .insert(
                    key.clone(),
                    AssignmentRecord {
                        runner_id,
                        at: std::time::SystemTime::now(),
                        first_at: std::time::SystemTime::now(),
                    },
                )
                .is_none()
        {
            return;
        }
    }
    if inner.pool_assignments_enabled {
        inner.pool_pending.insert(key, std::time::SystemTime::now());
    }
}

/// Pair a just-registered pool runner with the earliest pending job it can
/// serve. Called from the registration path; the returned runner then claims
/// the job by polling.
pub(crate) fn pair_registered_runner(inner: &mut InnerState, runner_id: i64) {
    if !inner.pool_assignments_enabled && !inner.require_job_assignments {
        return;
    }
    let Some(runner) = inner.runners.get(&runner_id).cloned() else {
        return;
    };
    let caps = capabilities_of(&runner);
    let now = std::time::SystemTime::now();
    // Also adopt jobs whose pairing went stale: the pool provisions a burst
    // of machines per backlog entry and individual runners die — the next
    // registration for the same job must take the pairing over or dispatch
    // hangs behind a dead owner.
    let stale_owned = inner
        .job_assignments
        .iter()
        .filter(|(_, record)| !binding_fresh(record.at, now))
        .filter(|(key, _)| {
            inner
                .queue
                .iter()
                .find(|job| job.run_id == key.0 && job.job_id == key.1)
                .map(|job| job_matches_runner_capabilities(job, &caps))
                .unwrap_or(false)
        })
        .min_by_key(|(_, record)| record.at)
        .map(|(key, _)| key.clone());
    let chosen = stale_owned.or_else(|| {
        inner
            .pool_pending
            .iter()
            .filter(|(_, at)| assignment_fresh(**at, now))
            .filter(|(key, _)| {
                inner
                    .queue
                    .iter()
                    .find(|job| job.run_id == key.0 && job.job_id == key.1)
                    .map(|job| job_matches_runner_capabilities(job, &caps))
                    .unwrap_or(false)
            })
            .min_by_key(|(_, at)| **at)
            .map(|(key, _)| key.clone())
    });
    if let Some(key) = chosen {
        // Rebinding to a replacement machine keeps the original first-bound
        // stamp, so repeated provisioning failures cannot extend the window
        // during which only the paired machine may claim.
        let first_at = inner
            .job_assignments
            .get(&key)
            .map(|record| record.first_at)
            .unwrap_or_else(std::time::SystemTime::now);
        inner.pool_pending.remove(&key);
        info!(runner_id, run_id = %key.0, job_id = %key.1.0, "job assignment paired to registered runner");
        inner.job_assignments.insert(
            key,
            AssignmentRecord {
                runner_id,
                at: std::time::SystemTime::now(),
                first_at,
            },
        );
    }
}

/// Drop the assignment for one job (requeue paths, deregistration purge).
/// Returns whether the job is still queued so callers can re-mark it
/// pool-pending when a replacement runner must be provisioned.
pub(crate) fn clear_assignment(inner: &mut InnerState, run_id: RunId, job_id: &JobId) -> bool {
    inner.job_assignments.remove(&(run_id, job_id.clone()));
    inner.pool_pending.remove(&(run_id, job_id.clone()));
    inner
        .queue
        .iter()
        .any(|job| job.run_id == run_id && job.job_id == *job_id)
}

fn capabilities_of(runner: &RegisteredRunner) -> RunnerCapabilities {
    RunnerCapabilities {
        known: true,
        labels: runner.labels.clone(),
        runner_group_id: runner.runner_group_id,
        runner_group_name: runner.runner_group_name.clone(),
    }
}

pub(crate) fn under_max_parallel(inner: &InnerState, job: &QueuedJob) -> bool {
    let Some(max_parallel) = job.max_parallel else {
        return true;
    };
    let active_in_queue = inner
        .queue
        .iter()
        .filter(|queued| queued.run_id == job.run_id && queued.base_id == job.base_id)
        .count() as u64;
    let active_running = inner
        .runs
        .get(&job.run_id)
        .map(|run| {
            run.jobs
                .iter()
                .filter(|(job_id, status)| {
                    run.job_base_ids.get(*job_id) == Some(&job.base_id)
                        && matches!(status, ExecutionStatus::InProgress)
                })
                .count() as u64
        })
        .unwrap_or(0);

    active_in_queue + active_running < max_parallel
}

pub(crate) fn apply_matrix_fail_fast(
    inner: &mut InnerState,
    run_id: RunId,
    failed_job: &JobId,
) -> Vec<JobId> {
    let Some(run) = inner.runs.get_mut(&run_id) else {
        return Vec::new();
    };
    let Some(base_id) = run.job_base_ids.get(failed_job).cloned() else {
        return Vec::new();
    };
    if !run.job_fail_fast.get(&base_id).copied().unwrap_or(true) {
        return Vec::new();
    }

    // Track in-progress siblings: they need a JOB_CANCELLED message so the
    // runner aborts the worker. Queued siblings only need their state flipped
    // — they were never dispatched.
    let mut cancelled_jobs = Vec::new();
    let mut cancellations = Vec::new();
    for (job_id, status) in &mut run.jobs {
        if job_id != failed_job
            && run.job_base_ids.get(job_id) == Some(&base_id)
            && matches!(
                status,
                ExecutionStatus::Queued | ExecutionStatus::Pending | ExecutionStatus::InProgress
            )
        {
            if matches!(status, ExecutionStatus::InProgress) {
                // Resolve agent_job_id after loop (borrow checker).
                cancellations.push(QueuedCancellation {
                    run_id,
                    job_id: job_id.clone(),
                    agent_job_id: uuid::Uuid::nil(), // filled below
                });
            }
            cancelled_jobs.push(job_id.clone());
            *status = ExecutionStatus::Cancelled;
        }
    }
    run.status = summarize_run(run.jobs.values().copied());
    inner
        .queue
        .retain(|job| !(job.run_id == run_id && job.base_id == base_id));
    inner
        .pending_jobs
        .retain(|job| !(job.run_id == run_id && job.base_id == base_id));
    // Fill real agent_job_ids; drop cancellations for jobs not in flight.
    cancellations.retain_mut(|c| {
        if let Some(id) = agent_job_id_for(inner, c.run_id, &c.job_id) {
            c.agent_job_id = id;
            true
        } else {
            false
        }
    });
    inner.cancellation_queue.extend(cancellations);
    cancelled_jobs
}

pub(crate) fn hydrate_needs_context(job: &mut QueuedJob, run: &RunRecord) {
    let needs = job
        .needs
        .iter()
        .filter_map(|need| need_context(run, need).map(|context| (need.0.clone(), context)))
        .collect();
    job.message
        .context_data
        .insert("needs".to_owned(), azdo::PipelineContextData::Dict(needs));
}
pub(crate) fn needs_json_context(run: &RunRecord, needs: &[JobId]) -> serde_json::Value {
    let values = needs
        .iter()
        .filter_map(|need| {
            let statuses = matching_need_statuses(run, need);
            let result = aggregate_need_status(&statuses)?;
            let matching_ids = matching_need_ids(run, need);
            let mut outputs = serde_json::Map::new();
            for job_id in matching_ids {
                if let Some(job_outputs) = run.job_outputs.get(&job_id) {
                    outputs.extend(job_outputs.clone());
                }
            }
            Some((
                need.0.clone(),
                json!({
                    "result": status_string(result),
                    "outputs": outputs,
                }),
            ))
        })
        .collect::<serde_json::Map<_, _>>();
    serde_json::Value::Object(values)
}

/// Evaluate the caller and embedded concurrency gates for one deferred
/// reusable-call invocation. Mirrors GitHub evaluating caller concurrency when
/// the caller job starts (after its needs complete and the `if:` gate passes).
///
/// `Ok(None)` means neither gate is declared. `Err(status)` records how the
/// caller node terminates when gate evaluation itself fails.
fn caller_jobset_gates(
    inner: &InnerState,
    job: &QueuedJob,
) -> Result<Option<Vec<JobSetGate>>, ExecutionStatus> {
    let Some(run) = inner.runs.get(&job.run_id) else {
        return Err(ExecutionStatus::Failure);
    };
    let Some(call) = run.reusable_calls.get(&job.job_id.0) else {
        return Ok(None);
    };
    let submission = &run.submission;
    let mut gates = Vec::new();
    for (raw, scope, label, inputs) in [
        (
            call.caller_concurrency.as_ref(),
            concurrency::ConcurrencyScope::Job,
            "caller concurrency (JobSet)",
            &submission.inputs,
        ),
        (
            call.embedded_concurrency.as_ref(),
            concurrency::ConcurrencyScope::Workflow,
            "embedded concurrency (JobSet)",
            &call.inputs,
        ),
    ] {
        let Some(raw) = raw else { continue };
        let eval_ctx = concurrency::ConcurrencyContext {
            scope,
            github: &run.github,
            vars: &submission.vars,
            inputs,
            matrix: Some(&job.matrix),
            strategy: None,
            needs: None,
        };
        match concurrency::evaluate_concurrency(raw, &eval_ctx) {
            Ok((group, cancel_in_progress, queue)) if !group.trim().is_empty() => {
                merge_jobset_gate(
                    &mut gates,
                    JobSetGate {
                        key: concurrency::concurrency_key(&submission.repository, &group),
                        display_name: group,
                        cancel_in_progress,
                        queue,
                    },
                );
            }
            Ok((_, _, _)) => return Err(ExecutionStatus::Failure),
            Err(error) => {
                concurrency::log_eval_error(label, &error);
                return Err(ExecutionStatus::Failure);
            }
        }
    }
    Ok((!gates.is_empty()).then_some(gates))
}

/// Everything a deferred node needs in order to build its subtree, cloned out
/// of the run record while the lock is held.
///
/// Snapshotting up front is what lets the expensive part — parsing workflow
/// YAML, building one runner message per inner job, minting a runtime token
/// per job — run with the global mutex released.
struct ExpansionContext {
    run_id: RunId,
    submission: Arc<WorkflowSubmission>,
    snapshot: Option<crate::snapshots::WorkspaceSnapshot>,
    github_json: serde_json::Value,
    workflow_path: String,
    workflow_ref: String,
    head_sha: String,
}

struct ReusableExpansionInputs {
    ctx: ExpansionContext,
    caller_id: JobId,
    caller_plan: aksh_gha_protocol::JobPlan,
    call: aksh_gha_protocol::ReusableCallPlan,
}

struct MatrixExpansionInputs {
    ctx: ExpansionContext,
    node_id: JobId,
    base_id: String,
    expression: String,
    needs_outputs: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
}

enum ExpansionPlan {
    Reusable(Box<ReusableExpansionInputs>),
    Matrix(Box<MatrixExpansionInputs>),
}

/// One fully built inner job, still detached from the run.
struct BuiltJob {
    plan: aksh_gha_protocol::JobPlan,
    condition_context: aksh_gha_expressions::Context,
    artifacts: crate::runs::BuiltJobArtifacts,
}

enum BuiltExpansion {
    Reusable {
        caller_id: JobId,
        jobs: Vec<BuiltJob>,
        reusable_calls: BTreeMap<String, aksh_gha_parser::ReusableCallMetadata>,
    },
    Matrix {
        jobs: Vec<BuiltJob>,
    },
}

/// Hand a gated node to [`drain_expansions`], which builds its subtree with
/// the global lock released.
fn defer_expansion(inner: &mut InnerState, job: QueuedJob) {
    inner.expanding.insert((job.run_id, job.job_id.clone()));
    inner.pending_expansions.push_back(job);
}

/// Snapshot the inputs a deferred node needs, while the lock is held.
fn plan_expansion(inner: &InnerState, job: &QueuedJob) -> Option<ExpansionPlan> {
    let run = inner.runs.get(&job.run_id)?;
    let ctx = ExpansionContext {
        run_id: job.run_id,
        submission: run.submission.clone(),
        snapshot: run.workspace_snapshot.clone(),
        github_json: run.github.clone(),
        workflow_path: run.workflow_path_str.clone(),
        workflow_ref: run.workflow_ref.clone(),
        head_sha: run.head_sha.clone(),
    };
    if let Some(call) = job.reusable_call.clone() {
        let caller_plan = run.caller_plans.get(&job.job_id).cloned()?;
        return Some(ExpansionPlan::Reusable(Box::new(ReusableExpansionInputs {
            ctx,
            caller_id: job.job_id.clone(),
            caller_plan,
            call,
        })));
    }
    let expression = job.deferred_matrix.clone()?;
    // `needs` outputs feed the matrix expression, so they are resolved here
    // rather than in the build phase, which no longer sees the run record.
    let mut needs_outputs: BTreeMap<String, BTreeMap<String, serde_json::Value>> = BTreeMap::new();
    for need_id in &job.needs {
        for matched in matching_need_ids(run, need_id) {
            if let Some(outputs) = run.job_outputs.get(&matched) {
                let base = run
                    .job_base_ids
                    .get(&matched)
                    .cloned()
                    .unwrap_or_else(|| need_id.0.clone());
                needs_outputs
                    .entry(base)
                    .or_default()
                    .extend(outputs.clone());
            }
        }
    }
    Some(ExpansionPlan::Matrix(Box::new(MatrixExpansionInputs {
        ctx,
        node_id: job.job_id.clone(),
        base_id: job.base_id.clone(),
        expression,
        needs_outputs,
    })))
}

/// Build one job's runner artifacts per plan. Runs with the lock released.
fn build_jobs<F>(
    shared: &SharedState,
    ctx: &ExpansionContext,
    plans: &[aksh_gha_protocol::JobPlan],
    condition_context: F,
) -> Result<Vec<BuiltJob>, ExecutionStatus>
where
    F: Fn(&aksh_gha_protocol::JobPlan, &BTreeMap<String, String>) -> aksh_gha_expressions::Context,
{
    let base_url = runner_base_url();
    let normalized_github =
        aksh_gha_parser::job_builder::normalize_github_context(&ctx.github_json);
    // Resolve the run's secrets to plaintext exactly once, at the boundary,
    // instead of re-exposing them per job.
    let secrets_exposed = aksh_gha_protocol::masking::expose_all(&ctx.submission.secrets);
    // PATs are static: embed at build time. App installation tokens are minted
    // by the broker at dispatch.
    let pat_override = if shared.state.github_app.is_none() {
        shared.state.static_github_pat()
    } else {
        None
    };
    let mut built = Vec::with_capacity(plans.len());
    for plan in plans {
        let artifacts = crate::runs::build_job_artifacts(
            shared,
            &ctx.submission,
            ctx.run_id,
            &ctx.workflow_path,
            &ctx.workflow_ref,
            &ctx.head_sha,
            &normalized_github,
            &secrets_exposed,
            &base_url,
            ctx.snapshot.as_ref(),
            plan,
            pat_override.clone(),
        )
        .map_err(|error| {
            tracing::warn!(
                run_id = %ctx.run_id,
                job = %plan.id,
                ?error,
                "job message build failed during expansion"
            );
            ExecutionStatus::Failure
        })?;
        built.push(BuiltJob {
            plan: plan.clone(),
            condition_context: condition_context(plan, &secrets_exposed),
            artifacts,
        });
    }
    Ok(built)
}

fn build_expansion(
    shared: &SharedState,
    plan: ExpansionPlan,
) -> Result<BuiltExpansion, ExecutionStatus> {
    match plan {
        ExpansionPlan::Reusable(inputs) => build_reusable_expansion(shared, *inputs),
        ExpansionPlan::Matrix(inputs) => build_matrix_expansion(shared, *inputs),
    }
}

/// Materialize a deferred reusable caller's callee subtree. Nested reusable
/// callers inside the callee come back as deferred caller nodes of their own.
fn build_reusable_expansion(
    shared: &SharedState,
    inputs: ReusableExpansionInputs,
) -> Result<BuiltExpansion, ExecutionStatus> {
    let ReusableExpansionInputs {
        ctx,
        caller_id,
        caller_plan,
        call,
    } = inputs;
    let run_id = ctx.run_id;
    let yaml = ctx
        .submission
        .reusable_workflows
        .get(&call.uses)
        .or_else(|| ctx.submission.reusable_workflows.get(&call.workflow_file));
    let Some(yaml) = yaml else {
        tracing::warn!(%run_id, job = %caller_id, "reusable workflow YAML missing at expansion");
        return Err(ExecutionStatus::Failure);
    };
    let called = aksh_gha_parser::parse_workflow(yaml).map_err(|error| {
        tracing::warn!(%run_id, job = %caller_id, %error, "callee re-parse failed at expansion");
        ExecutionStatus::Failure
    })?;
    let expanded = aksh_gha_parser::expand_reusable_call(
        &called,
        &caller_plan,
        &ctx.submission.reusable_workflows,
        &ctx.submission.reusable_workflow_shas,
    )
    .map_err(|error| {
        tracing::warn!(%run_id, job = %caller_id, %error, "reusable subtree expansion failed");
        ExecutionStatus::Failure
    })?;

    let github_json = ctx.github_json.clone();
    let vars = ctx.submission.vars.clone();
    let jobs = build_jobs(shared, &ctx, &expanded.jobs, |plan, _secrets| {
        aksh_gha_parser::eval::build_context(
            &github_json,
            &BTreeMap::new(),
            &vars,
            &indexmap::IndexMap::new(),
            &serde_json::json!({}),
            &BTreeMap::new(),
            &plan.inputs,
        )
    })?;
    Ok(BuiltExpansion::Reusable {
        caller_id,
        jobs,
        reusable_calls: expanded.reusable_calls,
    })
}

/// Materialize a dynamic `needs`-driven matrix.
///
/// An expression that fails to parse, evaluate or expand is a workflow error
/// and concludes the job as failed, exactly as GitHub does. Only a valid
/// expression that yields no combinations is a skip, and that is signalled by
/// an empty job list rather than by an error.
fn build_matrix_expansion(
    shared: &SharedState,
    inputs: MatrixExpansionInputs,
) -> Result<BuiltExpansion, ExecutionStatus> {
    let MatrixExpansionInputs {
        ctx,
        node_id,
        base_id,
        expression,
        needs_outputs,
    } = inputs;
    let run_id = ctx.run_id;
    let workflow = aksh_gha_parser::parse_workflow(&ctx.submission.workflow_yaml).map_err(|error| {
        tracing::warn!(%run_id, job = %node_id, %error, "workflow re-parse failed for dynamic matrix");
        ExecutionStatus::Failure
    })?;
    let plans = aksh_gha_parser::expand_deferred_matrix_job(
        &workflow,
        &base_id,
        &expression,
        &needs_outputs,
        Some(&ctx.submission.inputs),
    )
    .map_err(|error| {
        tracing::warn!(%run_id, job = %node_id, %error, "dynamic matrix expansion failed");
        ExecutionStatus::Failure
    })?;

    let github_json = ctx.github_json.clone();
    let vars = ctx.submission.vars.clone();
    let submission_inputs = ctx.submission.inputs.clone();
    let jobs = build_jobs(shared, &ctx, &plans, |plan, secrets| {
        aksh_gha_parser::eval::build_context(
            &github_json,
            &BTreeMap::new(),
            &vars,
            &plan
                .matrix
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            &serde_json::json!({}),
            secrets,
            &submission_inputs,
        )
    })?;
    Ok(BuiltExpansion::Matrix { jobs })
}

/// Insert correlation records and run bookkeeping for freshly built inner
/// jobs, returning the queue entries to hand back to the scheduler.
fn register_expanded_jobs(
    inner: &mut InnerState,
    run_id: RunId,
    jobs: Vec<BuiltJob>,
) -> Vec<QueuedJob> {
    let mut queued = Vec::with_capacity(jobs.len());
    for BuiltJob {
        plan,
        condition_context,
        artifacts,
    } in jobs
    {
        let job_request = artifacts.job_request;
        inner
            .id_token_grants
            .insert((run_id, plan.id.clone()), artifacts.id_token_granted);
        inner
            .oidc_job_contexts
            .insert((run_id, plan.id.clone()), artifacts.oidc_ctx);
        inner
            .inflight_requests
            .insert(job_request.request_id, (run_id, plan.id.clone()));
        inner
            .plan_requests
            .insert(job_request.plan_id.clone(), job_request.request_id);
        inner
            .agent_job_requests
            .insert(job_request.agent_job_id, job_request.request_id);
        inner
            .timeline_requests
            .insert(job_request.timeline_id, job_request.request_id);
        if let Some(request) = artifacts.github_token_request {
            inner
                .github_token_requests
                .insert(job_request.request_id, request);
        }
        inner
            .job_requests
            .insert(job_request.request_id, job_request);

        if let Some(run) = inner.runs.get_mut(&run_id) {
            // Same flavor as submission for needs-waiting jobs (`Queued`,
            // parked in pending_jobs): `Pending` is reserved for
            // concurrency-blocked work.
            run.jobs.insert(plan.id.clone(), ExecutionStatus::Queued);
            run.job_base_ids
                .insert(plan.id.clone(), plan.base_id.clone());
            run.job_needs.insert(plan.id.clone(), plan.needs.clone());
            run.job_fail_fast
                .insert(plan.base_id.clone(), plan.fail_fast);
            run.job_continue_on_error
                .insert(plan.id.to_string(), plan.continue_on_error);
            run.job_names.insert(plan.id.clone(), plan.name.clone());
            if plan.reusable_call.is_some() {
                run.caller_plans.insert(plan.id.clone(), plan.clone());
            }
        }

        queued.push(QueuedJob {
            run_id,
            job_id: plan.id.clone(),
            base_id: plan.base_id.clone(),
            needs: plan.needs.clone(),
            if_condition: plan.if_condition.clone(),
            condition_context,
            max_parallel: plan.max_parallel,
            runs_on: plan.runs_on.clone(),
            runner_group: plan.runner_group.clone(),
            message: artifacts.agent_msg,
            concurrency: concurrency::concurrency_from_plan_fields(
                plan.concurrency_group.as_deref(),
                plan.concurrency_cancel_in_progress.as_deref(),
                plan.concurrency_queue.as_deref(),
            ),
            matrix: plan
                .matrix
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            deferred_matrix: plan.deferred_matrix.clone(),
            reusable_call: plan.reusable_call.clone(),
        });
    }
    queued
}

/// Conclude a node whose subtree could not be built.
///
/// The JobSet gates were acquired before the build started, so they are
/// released here: nothing else will, and a leaked gate blocks every other run
/// in the same concurrency group.
fn fail_expansion_node(
    inner: &mut InnerState,
    run_id: RunId,
    node_id: &JobId,
    status: ExecutionStatus,
    outcome: &mut SchedulingOutcome,
) {
    if let Some(run) = inner.runs.get_mut(&run_id) {
        run.jobs.insert(node_id.clone(), status);
        run.status = summarize_run(run.jobs.values().copied());
        finalize_run_if_complete(run);
    }
    release_concurrency_for_job(inner, run_id, node_id);
    outcome.failed.push((run_id, node_id.clone()));
}

/// Fold a built subtree back into the run, under a freshly taken lock.
fn apply_expansion(
    inner: &mut InnerState,
    job: QueuedJob,
    built: Result<BuiltExpansion, ExecutionStatus>,
    outcome: &mut SchedulingOutcome,
) -> Vec<QueuedJob> {
    let run_id = job.run_id;
    let node_id = job.job_id;
    // The reservation is the proof this expansion is still wanted. Cancellation
    // drops it, so a build that finished after the run was cancelled must not
    // resurrect the subtree.
    if !inner.expanding.remove(&(run_id, node_id.clone())) {
        return Vec::new();
    }
    let built = match built {
        Ok(built) => built,
        Err(status) => {
            fail_expansion_node(inner, run_id, &node_id, status, outcome);
            return Vec::new();
        }
    };
    match built {
        BuiltExpansion::Matrix { jobs } if jobs.is_empty() => {
            if let Some(run) = inner.runs.get_mut(&run_id) {
                run.jobs.insert(node_id.clone(), ExecutionStatus::Skipped);
                run.status = summarize_run(run.jobs.values().copied());
                finalize_run_if_complete(run);
            }
            outcome.skipped.push((run_id, node_id));
            Vec::new()
        }
        BuiltExpansion::Matrix { jobs } => {
            let queued = register_expanded_jobs(inner, run_id, jobs);
            if let Some(run) = inner.runs.get_mut(&run_id) {
                // The placeholder is replaced by its combinations; GitHub shows
                // the fan-out, never the node that produced it.
                run.jobs.remove(&node_id);
                run.job_base_ids.remove(&node_id);
                run.job_needs.remove(&node_id);
                run.status = summarize_run(run.jobs.values().copied());
            }
            queued
        }
        BuiltExpansion::Reusable {
            caller_id,
            jobs,
            reusable_calls,
        } => {
            let inner_ids: Vec<String> = jobs.iter().map(|job| job.plan.id.0.clone()).collect();
            let queued = register_expanded_jobs(inner, run_id, jobs);
            if let Some(run) = inner.runs.get_mut(&run_id) {
                if let Some(meta) = run.reusable_calls.get_mut(&caller_id.0) {
                    meta.inner_job_ids = inner_ids;
                }
                run.reusable_calls.extend(reusable_calls);
                run.jobs.insert(caller_id, ExecutionStatus::InProgress);
                if run.started_at.is_none() {
                    run.started_at = Some(chrono::Utc::now());
                }
                run.status = summarize_run(run.jobs.values().copied());
            }
            queued
        }
    }
}

/// Build and apply every deferred subtree, then keep promoting until the
/// scheduler is quiet.
///
/// The build phase deliberately runs with the global lock released: it parses
/// workflow YAML and constructs a runner message plus a runtime token per
/// inner job, which scales with the width of the callee matrix. Holding the
/// mutex across that stalls every other request.
pub(crate) async fn drain_expansions(shared: &SharedState) -> SchedulingOutcome {
    let mut outcome = SchedulingOutcome::default();
    loop {
        // Phase 1 (locked): claim one node and snapshot its inputs.
        let (job, plan) = {
            let mut inner = shared.state.inner.lock().await;
            let Some(job) = inner.pending_expansions.pop_front() else {
                return outcome;
            };
            let plan = plan_expansion(&inner, &job);
            (job, plan)
        };

        // Phase 2 (unlocked): the expensive part.
        let built = match plan {
            Some(plan) => build_expansion(shared, plan),
            None => {
                tracing::warn!(
                    run_id = %job.run_id,
                    job = %job.job_id,
                    "expansion inputs vanished before build"
                );
                Err(ExecutionStatus::Failure)
            }
        };

        // Phase 3 (locked): apply, then promote whatever the subtree unblocked.
        let mut inner = shared.state.inner.lock().await;
        let ready = apply_expansion(&mut inner, job, built, &mut outcome);
        inner.pending_jobs.extend(ready);
        let promoted = promote_ready_jobs(&mut inner);
        outcome.merge(promoted);
        shared
            .state
            .queue_depth
            .store(inner.queue.len(), std::sync::atomic::Ordering::Release);
    }
}

pub(crate) fn aggregate_need_status(statuses: &[ExecutionStatus]) -> Option<ExecutionStatus> {
    if statuses.contains(&ExecutionStatus::Failure) {
        Some(ExecutionStatus::Failure)
    } else if statuses.contains(&ExecutionStatus::Cancelled) {
        Some(ExecutionStatus::Cancelled)
    } else if statuses.contains(&ExecutionStatus::Skipped) {
        Some(ExecutionStatus::Skipped)
    } else if !statuses.is_empty()
        && statuses
            .iter()
            .all(|status| *status == ExecutionStatus::Success)
    {
        Some(ExecutionStatus::Success)
    } else {
        None
    }
}

pub(crate) fn need_context(run: &RunRecord, need: &JobId) -> Option<azdo::PipelineContextData> {
    let statuses = matching_need_statuses(run, need);
    let result = aggregate_need_status(&statuses)?;
    let mut outputs = BTreeMap::new();
    for job_id in matching_need_ids(run, need) {
        if let Some(job_outputs) = run.job_outputs.get(&job_id) {
            for (key, value) in job_outputs {
                outputs.insert(key.clone(), azdo::PipelineContextData::from_json(value));
            }
        }
    }

    let mut context = BTreeMap::new();
    context.insert(
        "result".to_owned(),
        azdo::PipelineContextData::String(status_string(result)),
    );
    context.insert(
        "outputs".to_owned(),
        azdo::PipelineContextData::Dict(outputs),
    );
    Some(azdo::PipelineContextData::Dict(context))
}

pub(crate) fn status_string(status: ExecutionStatus) -> String {
    match status {
        ExecutionStatus::Queued
        | ExecutionStatus::Pending
        | ExecutionStatus::InProgress
        | ExecutionStatus::Success => "success",
        ExecutionStatus::Failure => "failure",
        ExecutionStatus::Skipped => "skipped",
        ExecutionStatus::Cancelled => "cancelled",
    }
    .to_owned()
}

/// Stamp completion metadata once every job is terminal. Job completions via
/// the broker/results path do this in `complete_job_inner`; runs whose last
/// transitions happen inside the scheduler (gated callers skipping, expansion
/// failures) need it here too.
pub(crate) fn finalize_run_if_complete(run: &mut RunRecord) {
    if matches!(
        run.status,
        ExecutionStatus::Success
            | ExecutionStatus::Failure
            | ExecutionStatus::Cancelled
            | ExecutionStatus::Skipped
    ) && run.completed_at.is_none()
        && run.jobs.values().all(|status| status.is_terminal())
    {
        run.completed_at = Some(chrono::Utc::now());
        run.conclusion = Some(status_string(run.status));
    }
}

pub(crate) fn summarize_run(statuses: impl Iterator<Item = ExecutionStatus>) -> ExecutionStatus {
    let statuses = statuses.collect::<Vec<_>>();
    if statuses.iter().any(|status| {
        matches!(
            status,
            ExecutionStatus::Queued | ExecutionStatus::Pending | ExecutionStatus::InProgress
        )
    }) {
        ExecutionStatus::InProgress
    } else if statuses.contains(&ExecutionStatus::Failure) {
        ExecutionStatus::Failure
    } else if statuses.contains(&ExecutionStatus::Cancelled) {
        ExecutionStatus::Cancelled
    } else {
        ExecutionStatus::Success
    }
}

/// Refresh the shared next-job labels from the front of the dispatch queue.
///
/// Called after every claim so a co-hosted runner pool can select the correct
/// base-image golden before provisioning the next runner.
pub(crate) fn sync_next_job_labels(inner: &InnerState, shared: &std::sync::RwLock<Vec<String>>) {
    let labels = inner
        .queue
        .front()
        .map(|job| job.runs_on.clone())
        .unwrap_or_default();
    let _ = shared.write().map(|mut guard| *guard = labels);
}

#[cfg(test)]
mod runner_group_tests {
    use super::*;

    fn runner(group_id: Option<i64>, group_name: Option<&str>) -> RunnerCapabilities {
        RunnerCapabilities {
            known: true,
            labels: vec!["self-hosted".to_owned(), "linux".to_owned()],
            runner_group_id: group_id,
            runner_group_name: group_name.map(str::to_owned),
        }
    }

    #[test]
    fn restricted_group_rejects_wrong_runner() {
        assert!(!job_matches_runner_group(
            Some("release"),
            &runner(Some(2), Some("build")),
        ));
        assert!(job_matches_runner_group(
            Some("release"),
            &runner(Some(2), Some("Release")),
        ));
    }

    #[test]
    fn group_is_not_treated_as_a_label() {
        let mut capabilities = runner(Some(2), Some("build"));
        capabilities.labels.push("release".to_owned());
        assert!(!job_matches_runner_group(Some("deploy"), &capabilities));
    }

    #[test]
    fn missing_group_metadata_uses_default_group() {
        let default_runner = runner(None, None);
        assert!(job_matches_runner_group(Some("Default"), &default_runner));
        let custom_name_only = runner(None, Some("private"));
        assert!(!job_matches_runner_group(Some("1"), &custom_name_only));
        assert!(job_matches_runner_group(Some("1"), &default_runner));
        assert!(job_matches_runner_group(None, &default_runner));
        assert!(!job_matches_runner_group(
            Some("private"),
            &RunnerCapabilities::default(),
        ));
    }
}
