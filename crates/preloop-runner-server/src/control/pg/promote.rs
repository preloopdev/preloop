//! Job-scoped promotion: the steps a job takes once its needs are settled —
//! `max-parallel` slot, needs/`runs-on`/environment hydration, label check,
//! environment approval, `concurrency:` gate, enqueue — over one job's own
//! rows, never the run's graph.
//!
//! [`Sweep::promote`](super::dispatch::Sweep) walks the same steps over a
//! loaded [`RunGraph`](super::graph::RunGraph); the pure decisions (the
//! environment name, the environment verdict, the gate evaluation) live here
//! and both call them, so the two paths cannot drift. The only input read
//! beyond the job's own `jobs`/`job_specs`/`job_messages`/`job_needs` rows is
//! the run's `run_submissions` payload (environment lookup and gate
//! evaluation) and the job's direct needs, via
//! [`NeedsView`](super::needs::NeedsView).
//!
//! Callers hold the run lock. A job that settles without running goes through
//! [`settle_unrun`], which writes the job's terminal row, releases what it
//! held, refreshes its dependents' counters and stamps the run through
//! `summarize_run_tx`.

use super::codec::now_us;
use super::dispatch::{
    EnqueueTarget, GateHolder, GateOutcome, Retirement, Sweep, acquire_gate,
    apply_resolved_runs_on, clear_assignment, emit_outbox, flush_node, mark_ready, mark_terminal,
    notify_wake, on_job_enqueued, promotion_label_reason, release_concurrency_for_job,
    release_concurrency_for_run, retire_node_requests, summarize_run_tx,
};
use super::graph::{self, Node, RunScope};
use super::needs::NeedsView;
use super::{PgBackend, db};
use crate::concurrency;
use crate::control::logic;
use crate::control::types::{ControlError, status_str};
use crate::models::EnvironmentGateState;
use crate::runtime_scheduling::{
    self as sched_helpers, DependencyDecision, EnvironmentGateOutcome, SchedulingOutcome,
};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId, WorkflowSubmission, azdo};
use tokio_postgres::Transaction;

/// Where a job enters the promotion steps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PromoteStep {
    /// Matrix leg behind a `max-parallel` cap: check the cohort has a free
    /// slot first, and stay `blocked` when it does not.
    MaxParallel,
    /// Needs are settled and any cohort slot is taken: hydrate and go on.
    Hydrate,
}

/// What [`promote_one`] did with the job.
#[derive(Debug)]
pub(super) enum PromoteResult {
    /// The job is `ready` (outbox row, wake and runner pairing done).
    Enqueued,
    /// The cohort is at its `max-parallel` cap; the job stays `blocked`.
    Parked,
    /// The job is `held`: behind an environment approval or a busy
    /// `concurrency:` group.
    Held,
    /// The job ended without running (unservable labels, denied environment,
    /// failed or cancelled gate). Carries the dependents this left `blocked`
    /// with no needs remaining, sorted by job id.
    Settled(ExecutionStatus, Vec<JobId>),
    /// The job has no row.
    Gone,
}

/// The inputs a job's `concurrency:` evaluation reads off its run.
pub(super) struct GateScope<'a> {
    pub(super) namespace: &'a str,
    pub(super) submission: &'a WorkflowSubmission,
    pub(super) github: &'a serde_json::Value,
}

/// The environment name a job's gate evaluates against: the hydrated
/// message's `actions_environment` (deferred names resolved), else the armed
/// gate's stamp, else the spec literal. `None` for environment-less jobs.
///
/// The message wins over the stamp: a gate armed while the name was still a
/// `${{ needs.* }}` template stamped that text, and re-evaluating the template
/// after `hydrate_needs_context` resolved the real name would decide against
/// an environment nobody configured.
pub(super) fn environment_name_of(
    message: Option<&azdo::AgentJobRequestMessage>,
    node: &Node,
) -> Option<String> {
    let message_name = message.and_then(|message| {
        message
            .actions_environment
            .as_ref()
            .map(|environment| environment.name.clone())
    });
    if let Some(name) = message_name
        .as_deref()
        .and_then(crate::runtime_scheduling::resolved_environment_name_of)
    {
        return Some(name.to_owned());
    }
    if let Some(name) = node
        .environment_gate
        .as_ref()
        .and_then(|gate| gate.environment_name.clone())
    {
        return Some(name);
    }
    message_name.or_else(|| {
        crate::runtime_scheduling::environment_gate_name_of(node.environment.as_ref())
            .map(str::to_owned)
    })
}

/// The environment protection verdict for a job against the resolver's
/// current entry for its repository. `gate` carries the armed gate's progress
/// in and out.
pub(super) fn evaluate_job_environment(
    backend: &PgBackend,
    run_id: RunId,
    job_id: &JobId,
    env_name: &str,
    submission: &WorkflowSubmission,
    gate: &mut Option<EnvironmentGateState>,
) -> EnvironmentGateOutcome {
    let lookup = backend
        .environment_resolver()
        .lookup_sync(&submission.repository, env_name);
    crate::runtime_scheduling::evaluate_environment_gate(
        &lookup,
        &submission.git_ref,
        run_id,
        job_id,
        env_name,
        gate,
        crate::models::now_unix_nanos(),
    )
}

/// Evaluate and acquire a job's own `concurrency:` gate. A job without one
/// acquires trivially.
pub(super) async fn acquire_job_gate(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    scope: GateScope<'_>,
    run_id: RunId,
    job_id: &JobId,
    node: &Node,
    message: Option<&azdo::AgentJobRequestMessage>,
) -> Result<GateOutcome, ControlError> {
    let Some(raw) = node.concurrency.as_ref() else {
        return Ok(GateOutcome::Acquired);
    };
    let strategy = message
        .and_then(|message| message.context_data.get("strategy"))
        .map(azdo::PipelineContextData::to_json)
        .unwrap_or_else(|| serde_json::json!({}));
    let eval_ctx = concurrency::ConcurrencyContext {
        scope: concurrency::ConcurrencyScope::Job,
        github: scope.github,
        vars: &scope.submission.vars,
        inputs: &scope.submission.inputs,
        matrix: Some(&node.matrix),
        strategy: Some(&strategy),
        needs: None,
    };
    let (group, cancel, queue) = match concurrency::evaluate_concurrency(raw, &eval_ctx) {
        Ok(v) => v,
        Err(e) => {
            concurrency::log_eval_error("job concurrency", &e);
            return Ok(GateOutcome::Failed);
        }
    };
    if group.trim().is_empty() {
        return Ok(GateOutcome::Failed);
    }
    let key = concurrency::concurrency_key(&scope.submission.repository, &group);
    acquire_gate(
        backend,
        tx,
        scope.namespace,
        &key,
        &group,
        &GateHolder {
            holder: concurrency::Holder::Job {
                run_id,
                job_id: job_id.clone(),
            },
            jobset_id: None,
        },
        cancel,
        queue,
    )
    .await
}

/// Settle a job that never ran: its terminal row (with whatever the caller
/// changed on `node` — hydrated placement, an environment gate's progress),
/// the concurrency it held, its requests and assignment, its dependents'
/// `remaining_needs`, the run's status and the `job.completed.v1` /
/// `run.completed.v1` outbox rows.
///
/// Returns the dependents now `blocked` with no needs remaining, sorted by job
/// id; the caller decides them (they may skip or fail in turn).
pub(super) async fn settle_unrun(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    node: &mut Node,
    status: ExecutionStatus,
    out: &mut SchedulingOutcome,
) -> Result<Vec<JobId>, ControlError> {
    let run = run_id.0.to_string();
    let run_status_before: Option<String> = tx
        .query_opt(
            "SELECT status FROM runs WHERE run_id=$1::text::uuid",
            &[&run],
        )
        .await
        .map_err(db)?
        .map(|row| row.get(0));
    mark_terminal(node, status, now_us());
    flush_node(tx, run_id, job_id, node).await?;
    release_concurrency_for_job(backend, tx, run_id, job_id).await?;
    if node.expandable() {
        retire_node_requests(tx, run_id, job_id, Retirement::Settle(status)).await?;
    }
    clear_assignment(tx, run_id, job_id).await?;
    let unblocked = Sweep::refresh_remaining_needs(tx, run_id, job_id, &node.base_id).await?;
    out.push(status, run_id, job_id.clone());
    summarize_run_tx(tx, run_id).await?;
    emit_outbox(
        tx,
        Some(run_id),
        "job.completed.v1",
        serde_json::json!({"job_id": job_id.0, "status": status_str(status)}),
    )
    .await?;
    let (run_status, conclusion): (String, Option<String>) = {
        let row = tx
            .query_one(
                "SELECT status, conclusion FROM runs WHERE run_id=$1::text::uuid",
                &[&run],
            )
            .await
            .map_err(db)?;
        (row.get(0), row.get(1))
    };
    if run_status == "completed" && run_status_before.as_deref() != Some("completed") {
        // A terminal run must not hold a workflow-level slot: release the
        // group and promote its next waiter.
        release_concurrency_for_run(backend, tx, run_id).await?;
        emit_outbox(
            tx,
            Some(run_id),
            "run.completed.v1",
            serde_json::json!({
                "conclusion": conclusion.unwrap_or_else(|| status_str(status).to_owned())
            }),
        )
        .await?;
    }
    Ok(unblocked)
}

/// How many legs of a cohort occupy a `max-parallel` slot: running, claimed or
/// ready.
async fn active_legs(
    tx: &Transaction<'_>,
    run_id: RunId,
    base_id: &str,
) -> Result<i64, ControlError> {
    Ok(tx
        .query_one(
            "SELECT count(*) FROM jobs WHERE run_id=$1::text::uuid AND base_id=$2 \
             AND (status='in_progress' OR queue_state IN ('ready','claimed'))",
            &[&run_id.0.to_string(), &base_id],
        )
        .await
        .map_err(db)?
        .get(0))
}

/// Promote one job whose needs are settled, from `from` onward, over its own
/// rows. Folds what it did into `out` (`promoted`, and `skipped`/`failed` for
/// a job it settles) the way the sweep's outcome does.
///
/// `needs` is the job's [`NeedsView`] when the caller already holds it; it is
/// loaded here otherwise. Not for reusable callers or deferred matrices: those
/// expand instead of dispatching.
pub(super) async fn promote_one(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    from: PromoteStep,
    needs: Option<&NeedsView>,
    out: &mut SchedulingOutcome,
) -> Result<PromoteResult, ControlError> {
    let Some(node) = PgBackend::load_node(tx, run_id, job_id).await? else {
        return Ok(PromoteResult::Gone);
    };
    promote_node(backend, tx, run_id, job_id, node, from, needs, out).await
}

/// [`promote_one`] for a caller that already read the job's node.
#[allow(clippy::too_many_arguments)]
async fn promote_node(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    mut node: Node,
    from: PromoteStep,
    needs: Option<&NeedsView>,
    out: &mut SchedulingOutcome,
) -> Result<PromoteResult, ControlError> {
    if from == PromoteStep::MaxParallel
        && let Some(limit) = node.max_parallel
        && active_legs(tx, run_id, &node.base_id).await? >= i64::try_from(limit).unwrap_or(i64::MAX)
    {
        // Stays `blocked` — a later leg completion re-admits it.
        return Ok(PromoteResult::Parked);
    }
    node.deps_ready_at_us = Some(now_us());

    // Deferred `runs-on` and environment names resolve here, once needs
    // outputs exist.
    let mut message = PgBackend::node_message(tx, run_id, job_id).await?;
    if let Some(stored) = message.take() {
        let loaded;
        let view = match needs {
            Some(view) => view,
            None => {
                loaded = NeedsView::load(tx, run_id, job_id).await?;
                &loaded
            }
        };
        let mut queued = graph::queued_of(job_id, run_id, &node, stored);
        sched_helpers::hydrate_needs_context(&mut queued, view);
        PgBackend::write_node_message(tx, run_id, job_id, &queued.message).await?;
        node.environment_gate = queued.environment_gate;
        apply_resolved_runs_on(&mut node, queued.runs_on);
        message = Some(queued.message);
    }
    // Fail before occupying a concurrency slot if the concrete labels can
    // never be served.
    if let Some(reason) = promotion_label_reason(backend, tx, &node.runs_on).await? {
        tracing::warn!(
            %run_id,
            job = %job_id.0,
            labels = ?node.runs_on,
            pool_labels = ?backend.pool_labels(),
            %reason,
            "runs-on unsatisfiable by runner pool; failing the job"
        );
        let unblocked = settle_unrun(
            backend,
            tx,
            run_id,
            job_id,
            &mut node,
            ExecutionStatus::Failure,
            out,
        )
        .await?;
        return Ok(PromoteResult::Settled(ExecutionStatus::Failure, unblocked));
    }

    // The run-level inputs, read once and only if a step needs them.
    let environment_name = environment_name_of(message.as_ref(), &node);
    let scope: Option<RunScope> = if environment_name.is_some() || node.concurrency.is_some() {
        Some(
            PgBackend::load_run_scope(tx, run_id)
                .await?
                .ok_or_else(|| ControlError::NotFound(format!("run {run_id}")))?,
        )
    } else {
        None
    };

    // Environment protection rules run ahead of concurrency gating: a denied
    // or waiting job never occupies a slot, and a gate failure is the reason
    // the job ends, not "no runner could match".
    if let (Some(env_name), Some(scope)) = (environment_name.as_deref(), scope.as_ref()) {
        let mut gate = node.environment_gate.clone();
        let verdict = evaluate_job_environment(
            backend,
            run_id,
            job_id,
            env_name,
            &scope.submission,
            &mut gate,
        );
        match verdict {
            EnvironmentGateOutcome::Wait => {
                // Armed gates stamp progress on the node; a `Pending` hold
                // (GitHub rules not fetched yet) records only the resolved
                // name — the reaper sweep re-evaluates once the resolver
                // fills the entry.
                node.environment_gate = gate;
                node.queue_state = logic::QueueState::Held;
                node.status = ExecutionStatus::Pending;
                flush_node(tx, run_id, job_id, &node).await?;
                return Ok(PromoteResult::Held);
            }
            EnvironmentGateOutcome::Failed => {
                node.environment_gate = gate;
                let unblocked = settle_unrun(
                    backend,
                    tx,
                    run_id,
                    job_id,
                    &mut node,
                    ExecutionStatus::Failure,
                    out,
                )
                .await?;
                return Ok(PromoteResult::Settled(ExecutionStatus::Failure, unblocked));
            }
            EnvironmentGateOutcome::Proceed => {
                if gate.is_some() || node.environment_gate.is_some() {
                    node.environment_gate = gate;
                }
            }
        }
    }

    // Job-level gate.
    if node.concurrency.is_some() {
        let scope = scope.as_ref().expect("loaded for a job with a gate");
        let gate = acquire_job_gate(
            backend,
            tx,
            GateScope {
                namespace: &scope.namespace,
                submission: &scope.submission,
                github: &scope.github,
            },
            run_id,
            job_id,
            &node,
            message.as_ref(),
        )
        .await?;
        match gate {
            GateOutcome::Acquired => node.concurrency_acquired_at_us = Some(now_us()),
            GateOutcome::Parked => {
                node.status = ExecutionStatus::Pending;
                node.queue_state = logic::QueueState::Held;
                node.concurrency_wait_at_us = Some(now_us());
                flush_node(tx, run_id, job_id, &node).await?;
                return Ok(PromoteResult::Held);
            }
            GateOutcome::Cancelled | GateOutcome::Failed => {
                let status = if matches!(gate, GateOutcome::Cancelled) {
                    ExecutionStatus::Cancelled
                } else {
                    ExecutionStatus::Failure
                };
                let unblocked =
                    settle_unrun(backend, tx, run_id, job_id, &mut node, status, out).await?;
                return Ok(PromoteResult::Settled(status, unblocked));
            }
        }
    }

    // Enqueue.
    mark_ready(&mut node, now_us());
    flush_node(tx, run_id, job_id, &node).await?;
    out.promoted += 1;
    emit_outbox(
        tx,
        Some(run_id),
        "job.queued.v1",
        serde_json::json!({"job_id": job_id.0, "status": "queued"}),
    )
    .await?;
    notify_wake(
        tx,
        crate::control::wake::Wake {
            ready: 1,
            broadcast: false,
        },
    )
    .await?;
    let (pool_assignments, require_assignments, _liveness) = backend.config();
    if (pool_assignments || require_assignments)
        && let Some(target) = EnqueueTarget::load(tx, run_id, job_id).await?
    {
        on_job_enqueued(backend, tx, &target).await?;
    }
    Ok(PromoteResult::Enqueued)
}

/// Admit parked legs of the given `max-parallel` cohorts while they have a
/// free slot: the leg a freed slot was waiting for, then the next, until the
/// cohort is full or has no admissible leg left.
///
/// Legs go in job-id order (the order a sweep over the run visits them). Each
/// gets the dependency decision a sweep would give it: a leg whose `if:` is
/// false or errors settles instead of running, which frees its slot for the
/// next one. Returns the dependents those settles left `blocked` with no needs
/// remaining; the caller decides them.
pub(super) async fn admit_cohorts(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    run_id: RunId,
    bases: &[String],
    out: &mut SchedulingOutcome,
) -> Result<Vec<JobId>, ControlError> {
    let mut unblocked = Vec::new();
    for base_id in bases {
        // Legs the decision says wait: their needs are not all terminal yet.
        let mut waiting: Vec<String> = Vec::new();
        while let Some(job_id) = next_admissible_leg(tx, run_id, base_id, &waiting).await? {
            let Some(mut node) = PgBackend::load_node(tx, run_id, &job_id).await? else {
                break;
            };
            let view = NeedsView::load(tx, run_id, &job_id).await?;
            let decision = sched_helpers::dependency_decision_of(
                &view,
                &node.needs,
                node.if_condition.as_deref(),
                &node.condition_context,
            );
            match decision {
                DependencyDecision::Run => {
                    match promote_node(
                        backend,
                        tx,
                        run_id,
                        &job_id,
                        node,
                        PromoteStep::Hydrate,
                        Some(&view),
                        out,
                    )
                    .await?
                    {
                        PromoteResult::Settled(_, freed) => unblocked.extend(freed),
                        PromoteResult::Enqueued | PromoteResult::Held => {}
                        PromoteResult::Parked | PromoteResult::Gone => break,
                    }
                }
                DependencyDecision::Skip | DependencyDecision::Error => {
                    let status = if decision == DependencyDecision::Skip {
                        ExecutionStatus::Skipped
                    } else {
                        ExecutionStatus::Failure
                    };
                    unblocked.extend(
                        settle_unrun(backend, tx, run_id, &job_id, &mut node, status, out).await?,
                    );
                }
                DependencyDecision::Wait => waiting.push(job_id.0),
            }
        }
    }
    unblocked.sort();
    unblocked.dedup();
    Ok(unblocked)
}

/// The next leg of a `max-parallel` cohort a free slot can take: `blocked`
/// with no needs remaining, in job-id order, provided the cohort is under its
/// cap. Reusable callers and deferred matrices are never capped (they expand).
async fn next_admissible_leg(
    tx: &Transaction<'_>,
    run_id: RunId,
    base_id: &str,
    skip: &[String],
) -> Result<Option<JobId>, ControlError> {
    let row = tx
        .query_opt(
            "SELECT j.job_id, s.max_parallel, \
                    (SELECT count(*) FROM jobs a \
                      WHERE a.run_id = j.run_id AND a.base_id = j.base_id \
                        AND (a.status='in_progress' OR a.queue_state IN ('ready','claimed'))) \
             FROM jobs j \
             JOIN job_specs s ON s.run_id = j.run_id AND s.job_id = j.job_id \
             WHERE j.run_id = $1::text::uuid AND j.base_id = $2 \
               AND j.queue_state = 'blocked' AND j.remaining_needs <= 0 \
               AND j.status NOT IN ('success','failure','cancelled','skipped','timed_out') \
               AND s.max_parallel IS NOT NULL AND s.deferred_matrix IS NULL \
               AND NOT (s.reusable_call IS NOT NULL AND s.reusable_call ? 'call') \
               AND j.job_id <> ALL($3::text[]) \
             ORDER BY j.job_id COLLATE \"C\" LIMIT 1",
            &[&run_id.0.to_string(), &base_id, &skip],
        )
        .await
        .map_err(db)?;
    Ok(row.and_then(|row| {
        let cap: i32 = row.get(1);
        let active: i64 = row.get(2);
        (active < i64::from(cap)).then(|| JobId(row.get(0)))
    }))
}
