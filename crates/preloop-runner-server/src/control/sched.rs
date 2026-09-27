//! The scheduling state machine, ported from `runtime_scheduling.rs` to run
//! inside a [`TxState`] working set.
//!
//! Every function here is pure Rust over the working set: no I/O, no locks,
//! no timers. The backend loads the rows a command needs, runs these
//! functions, and writes the delta back in the same transaction — which is
//! what makes the database (not a process mutex) the serialization and
//! fencing boundary.
//!
//! Ported semantics are identical to the in-memory versions. The only
//! structural divergence is the ready queue: the global queue is the `jobs`
//! table, surfaced in the working set as `ready_index` (every ready row,
//! in order) plus `queue` (jobs this transaction newly enqueued). Functions
//! that scanned `inner.queue` scan `ready_index` + `queue` together.

use super::txstate::TxState;
use crate::concurrency;
use crate::info;
use crate::models::{AssignmentRecord, QueuedCancellation, QueuedJob, RunRecord};
use crate::state::{JobSetAdmission, JobSetAdmissionResult, JobSetGate, JobSetId};
use preloop_gha_protocol::azdo;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId, WorkflowSubmission};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

// Re-export the pure helpers the ported code shares with the old module.
pub(crate) use crate::runtime_scheduling::{
    aggregate_need_status, finalize_run_if_complete, matching_need_ids, matching_need_statuses,
    need_context, needs_json_context, summarize_run, DependencyDecision, SchedulingOutcome,
};

/// How long an assignment or pool-pending mark stays authoritative.
pub(crate) const ASSIGNMENT_TTL: std::time::Duration = std::time::Duration::from_secs(600);
/// How long a pre-claim assignment stays exclusive.
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

/// Iterate the global ready queue: every persisted ready row (`ready_index`)
/// followed by jobs this transaction newly enqueued (`queue`).
pub(crate) fn ready_jobs(tx: &TxState) -> impl Iterator<Item = &QueuedJob> {
    tx.ready_index.iter().chain(tx.queue.iter())
}

/// The `runs-on` labels of the global queue front, for the pool's next-image
/// selection (`sync_next_job_labels` in the old code). When the ready queue
/// was loaded this transaction, the live front is exact — it reflects the
/// claim that just ran and any new enqueues. When the scope skipped the
/// ready queue, fall back to the unscoped load-time snapshot so a narrow
/// scope never names the wrong platform.
pub(crate) fn next_job_labels(tx: &TxState) -> Vec<String> {
    if tx.ready_queue_loaded {
        return ready_jobs(tx)
            .next()
            .map(|j| j.runs_on.clone())
            .unwrap_or_default();
    }
    tx.next_queue_labels.clone()
}

// ─────────────────────────────────────────────────────────────────────────
// Stamps
// ─────────────────────────────────────────────────────────────────────────

pub(crate) fn stamp_dependencies_ready(job: &mut QueuedJob) {
    if job.dependencies_ready_at_unix_nanos.is_none() {
        job.dependencies_ready_at_unix_nanos = Some(crate::models::now_unix_nanos());
    }
}

pub(crate) fn stamp_concurrency_wait_started(job: &mut QueuedJob) {
    if job.concurrency_wait_started_at_unix_nanos.is_none() {
        job.concurrency_wait_started_at_unix_nanos = Some(crate::models::now_unix_nanos());
    }
}

pub(crate) fn stamp_concurrency_acquired(job: &mut QueuedJob) {
    if job.concurrency_acquired_at_unix_nanos.is_none() {
        job.concurrency_acquired_at_unix_nanos = Some(crate::models::now_unix_nanos());
    }
}

pub(crate) fn stamp_ready_enqueue(job: &mut QueuedJob) {
    stamp_dependencies_ready(job);
    job.enqueued_at_unix_nanos = crate::models::now_unix_nanos();
}

// ─────────────────────────────────────────────────────────────────────────
// Runner matching (pure)
// ─────────────────────────────────────────────────────────────────────────

fn hosted_label_os(required: &str) -> Option<&'static str> {
    if required.starts_with("ubuntu") {
        Some("linux")
    } else if required.starts_with("macos") {
        Some("macos")
    } else if required.starts_with("windows") {
        Some("windows")
    } else {
        None
    }
}

pub(crate) fn unhostable_platform(
    job_labels: &[String],
    runners: impl IntoIterator<Item = &'static str>,
) -> Option<&'static str> {
    let needed = job_labels
        .iter()
        .filter_map(|label| hosted_label_os(&label.to_lowercase()))
        .find(|os| *os == "macos" || *os == "windows")?;
    let hosted_by_someone = runners.into_iter().any(|os| os == needed);
    (!hosted_by_someone).then_some(needed)
}

pub(crate) fn registered_runner_platforms(tx: &TxState) -> Vec<&'static str> {
    tx.runners
        .values()
        .filter_map(|runner| {
            runner
                .labels
                .iter()
                .find_map(|label| match label.to_lowercase().as_str() {
                    "linux" => Some("linux"),
                    "macos" => Some("macos"),
                    "windows" => Some("windows"),
                    _ => None,
                })
        })
        .collect()
}

pub(crate) fn job_matches_runner(job_labels: &[String], runner_labels: &[String]) -> bool {
    if job_labels.is_empty() {
        return true;
    }
    if runner_labels.is_empty() {
        return true;
    }
    let runner_set: std::collections::HashSet<String> =
        runner_labels.iter().map(|l| l.to_lowercase()).collect();
    let runner_os = ["linux", "macos", "windows"]
        .into_iter()
        .find(|os| runner_set.contains(*os));
    job_labels.iter().all(|required| {
        let req = required.to_lowercase();
        if runner_set.contains(&req) {
            return true;
        }
        let Some(required_os) = hosted_label_os(&req) else {
            return false;
        };
        match runner_os {
            Some(os) => os == required_os,
            None => runner_set.contains("self-hosted"),
        }
    })
}

pub(crate) fn job_matches_runner_group(
    required_group: Option<&str>,
    runner: &crate::models::RunnerCapabilities,
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
    runner: &crate::models::RunnerCapabilities,
) -> bool {
    job_matches_runner(&job.runs_on, &runner.labels)
        && job_matches_runner_group(job.runner_group.as_deref(), runner)
}

fn job_labels_covered_exactly(job_labels: &[String], runner_labels: &[String]) -> bool {
    if job_labels.is_empty() {
        return true;
    }
    if runner_labels.is_empty() {
        return false;
    }
    let runner_set: std::collections::HashSet<String> =
        runner_labels.iter().map(|l| l.to_lowercase()).collect();
    job_labels
        .iter()
        .all(|required| runner_set.contains(&required.to_lowercase()))
}

pub(crate) fn capabilities_of(
    runner: &preloop_gha_protocol::RegisteredRunner,
) -> crate::models::RunnerCapabilities {
    crate::models::RunnerCapabilities {
        known: true,
        labels: runner.labels.clone(),
        runner_group_id: runner.runner_group_id,
        runner_group_name: runner.runner_group_name.clone(),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Claim eligibility and selection
// ─────────────────────────────────────────────────────────────────────────

/// Whether `verified_runner_id` may claim `job` right now, independent of
/// runner capabilities. Ported verbatim.
fn claim_permitted(tx: &TxState, job: &QueuedJob, verified_runner_id: Option<i64>) -> bool {
    let key = (job.run_id, job.job_id.clone());
    let now = std::time::SystemTime::now();

    let enqueued_at =
        std::time::UNIX_EPOCH + std::time::Duration::from_nanos(job.enqueued_at_unix_nanos as u64);
    let enqueue_ceiling_expired = now
        .duration_since(enqueued_at)
        .map(|age| age >= CLAIM_BINDING_TTL)
        .unwrap_or(true);

    if let Some(record) = tx.job_assignments.get(&key) {
        if !binding_fresh(record.first_at, now) || enqueue_ceiling_expired {
            return verified_runner_id.is_some();
        }
        match record.runner_id {
            None => return verified_runner_id.is_some(),
            Some(id) => {
                if !tx.runners.contains_key(&id) || !binding_fresh(record.at, now) {
                    return verified_runner_id.is_some();
                }
                return Some(id) == verified_runner_id;
            }
        }
    }
    if let Some(marked_at) = tx.pool_pending.get(&key) {
        if binding_fresh(*marked_at, now) && !enqueue_ceiling_expired {
            return false;
        }
        return verified_runner_id.is_some();
    }
    !tx.require_job_assignments
}

/// Choose the position in `ready_index` of the job a session may claim.
/// This is the selection half of the old `take_matching_job`, split so the
/// backend can hydrate the chosen row: selection runs over the global ready
/// index, then [`apply_claim`] mutates the working set. The stale-bookkeeping
/// sweep and the four-tier preference cascade are preserved verbatim.
pub(crate) fn choose_claim_position(
    tx: &mut TxState,
    runner: &crate::models::RunnerCapabilities,
    verified_runner_id: Option<i64>,
) -> Option<usize> {
    let now = std::time::SystemTime::now();
    if !tx.require_job_assignments {
        tx.job_assignments
            .retain(|_, record| assignment_fresh(record.at, now));
        tx.pool_pending.retain(|_, at| assignment_fresh(*at, now));
    }
    let claimable = |job: &QueuedJob| {
        tx.poll_claimable
            .as_ref()
            .is_none_or(|locked| locked.contains(&(job.run_id, job.job_id.clone())))
            && job_matches_runner_capabilities(job, runner)
            && claim_permitted(tx, job, verified_runner_id)
    };
    let assigned_to_this_runner = |job: &QueuedJob| {
        let Some(runner_id) = verified_runner_id else {
            return false;
        };
        tx.job_assignments
            .get(&(job.run_id, job.job_id.clone()))
            .is_some_and(|record| {
                record.runner_id == Some(runner_id) && binding_fresh(record.at, now)
            })
    };
    tx.ready_index
        .iter()
        .position(|job| {
            assigned_to_this_runner(job)
                && job_labels_covered_exactly(&job.runs_on, &runner.labels)
                && claimable(job)
        })
        .or_else(|| {
            tx.ready_index
                .iter()
                .position(|job| assigned_to_this_runner(job) && claimable(job))
        })
        .or_else(|| {
            tx.ready_index.iter().position(|job| {
                job_labels_covered_exactly(&job.runs_on, &runner.labels) && claimable(job)
            })
        })
        .or_else(|| tx.ready_index.iter().position(claimable))
}

/// Apply a chosen claim: remove from the ready index, drop assignment
/// bookkeeping, stash in `claimed_jobs` for requeue-on-runner-death.
pub(crate) fn apply_claim(tx: &mut TxState, pos: usize) -> Option<QueuedJob> {
    let job = tx.ready_index.remove(pos)?;
    tx.ready_count -= 1;
    let key = (job.run_id, job.job_id.clone());
    tx.job_assignments.remove(&key);
    tx.pool_pending.remove(&key);
    tx.claimed_jobs.insert(key, job.clone());
    Some(job)
}

/// Record dispatch intent for a newly queued job (ported verbatim).
pub(crate) fn on_job_enqueued(tx: &mut TxState, job: &QueuedJob) {
    if !tx.pool_assignments_enabled && !tx.require_job_assignments {
        return;
    }
    let key = (job.run_id, job.job_id.clone());
    if tx.job_assignments.contains_key(&key) {
        return;
    }
    tx.pool_pending.remove(&key);
    let mut busy: std::collections::BTreeSet<i64> = tx
        .job_assignments
        .values()
        .filter_map(|record| record.runner_id)
        .collect();
    for session_id in tx.session_active_requests.keys() {
        if let Some(runner_id) = tx.runner_id_for_session(session_id) {
            busy.insert(runner_id);
        }
    }
    let mut candidates: std::collections::BTreeSet<i64> =
        tx.broker_session_runners.values().copied().collect();
    candidates.extend(tx.sessions.values().map(|session| session.runner_id));
    if tx.pool_assignments_enabled {
        candidates.retain(|runner_id| tx.pool_proven_runners.contains(runner_id));
    }
    for runner_id in candidates {
        if busy.contains(&runner_id) {
            continue;
        }
        let Some(runner) = tx.runners.get(&runner_id) else {
            continue;
        };
        if job_matches_runner_capabilities(job, &capabilities_of(runner))
            && tx
                .job_assignments
                .insert(
                    key.clone(),
                    AssignmentRecord {
                        runner_id: Some(runner_id),
                        at: std::time::SystemTime::now(),
                        first_at: std::time::SystemTime::now(),
                    },
                )
                .is_none()
        {
            return;
        }
    }
    if tx.pool_assignments_enabled {
        tx.pool_pending.insert(key, std::time::SystemTime::now());
    }
}

/// Pair a just-registered pool runner with the earliest pending job it can
/// serve. Ported verbatim; `ready_index` supplies the global queue scan.
pub(crate) fn pair_registered_runner(tx: &mut TxState, runner_id: i64) {
    if !tx.pool_assignments_enabled && !tx.require_job_assignments {
        return;
    }
    tx.pool_proven_runners.insert(runner_id);
    let Some(runner) = tx.runners.get(&runner_id).cloned() else {
        return;
    };
    let caps = capabilities_of(&runner);
    let now = std::time::SystemTime::now();
    let dead: Vec<(RunId, JobId)> = tx
        .job_assignments
        .iter()
        .filter(|(_, record)| {
            !binding_fresh(record.at, now)
                || record
                    .runner_id
                    .is_some_and(|id| !tx.runners.contains_key(&id))
        })
        .map(|(key, _)| key.clone())
        .collect();
    for key in dead {
        if !tx.pool_assignments_enabled {
            continue;
        }
        if let Some(record) = tx.job_assignments.get_mut(&key) {
            if record.runner_id.is_some() {
                record.runner_id = None;
                info!(
                    run_id = %key.0,
                    job_id = %key.1.0,
                    "stale binding released; job requeued at back of pool waitlist"
                );
                tx.pool_pending.insert(key, now);
                tx.released_bindings_count = tx.released_bindings_count.saturating_add(1);
            }
        }
    }

    let queue_positions: std::collections::HashMap<(RunId, JobId), usize> = ready_jobs(tx)
        .enumerate()
        .filter(|(_, job)| job_matches_runner_capabilities(job, &caps))
        .map(|(idx, job)| ((job.run_id, job.job_id.clone()), idx))
        .collect();

    let chosen = tx
        .pool_pending
        .iter()
        .filter_map(|(key, at)| queue_positions.get(key).map(|&pos| (key, *at, pos)))
        .min_by_key(|(_, at, pos)| (*at, *pos))
        .map(|(key, _, _)| key.clone());
    if let Some(key) = chosen {
        let first_at = tx
            .job_assignments
            .get(&key)
            .map(|record| record.first_at)
            .unwrap_or_else(std::time::SystemTime::now);
        tx.pool_pending.remove(&key);
        info!(runner_id, run_id = %key.0, job_id = %key.1.0, "job assignment paired to registered runner");
        tx.job_assignments.insert(
            key,
            AssignmentRecord {
                runner_id: Some(runner_id),
                at: std::time::SystemTime::now(),
                first_at,
            },
        );
    }
}

/// Sweep stale job bindings on a timer (ported verbatim).
pub(crate) fn sweep_stale_bindings(tx: &mut TxState, now: std::time::SystemTime) -> usize {
    let mut swept = 0;
    let queued_keys: std::collections::BTreeSet<(RunId, JobId)> = ready_jobs(tx)
        .map(|job| (job.run_id, job.job_id.clone()))
        .collect();
    if tx.pool_assignments_enabled {
        let initial_assignments = tx.job_assignments.len();
        tx.job_assignments
            .retain(|key, _| queued_keys.contains(key));
        swept += initial_assignments - tx.job_assignments.len();
        let initial_pending = tx.pool_pending.len();
        tx.pool_pending.retain(|key, _| queued_keys.contains(key));
        swept += initial_pending - tx.pool_pending.len();
    }
    if !tx.require_job_assignments && !tx.pool_assignments_enabled {
        let initial_assignments = tx.job_assignments.len();
        let initial_pending = tx.pool_pending.len();
        tx.job_assignments
            .retain(|_, record| assignment_fresh(record.at, now));
        tx.pool_pending.retain(|_, at| assignment_fresh(*at, now));
        swept += (initial_assignments - tx.job_assignments.len())
            + (initial_pending - tx.pool_pending.len());
    } else {
        let dead: Vec<(RunId, JobId)> = tx
            .job_assignments
            .iter()
            .filter(|(_, record)| {
                !binding_fresh(record.at, now)
                    || record
                        .runner_id
                        .is_some_and(|id| !tx.runners.contains_key(&id))
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in dead {
            if let Some(record) = tx.job_assignments.get_mut(&key) {
                if record.runner_id.is_some() {
                    record.runner_id = None;
                    info!(
                        run_id = %key.0,
                        job_id = %key.1.0,
                        "stale binding released on timer; job requeued at back of pool waitlist"
                    );
                    if tx.pool_assignments_enabled && queued_keys.contains(&key) {
                        tx.pool_pending.insert(key, now);
                    }
                    tx.released_bindings_count = tx.released_bindings_count.saturating_add(1);
                    swept += 1;
                }
            }
        }
    }
    swept
}

/// Drop the assignment for one job; returns whether it is still queued.
pub(crate) fn clear_assignment(tx: &mut TxState, run_id: RunId, job_id: &JobId) -> bool {
    tx.job_assignments.remove(&(run_id, job_id.clone()));
    tx.pool_pending.remove(&(run_id, job_id.clone()));
    ready_jobs(tx).any(|job| job.run_id == run_id && job.job_id == *job_id)
}

/// Requeue a claimed job back onto the ready queue after its owner runner is
/// gone (crash recovery, runner purge). Releases the assignment, resets the
/// job's canonical status to `Queued` and recomputes the run summary — the
/// job returns to the dispatch queue for a fresh runner, no longer
/// `in_progress` under the dead runner's claim. Returns `true` if the job was
/// claimed and requeued.
pub(crate) fn requeue_claimed(tx: &mut TxState, run_id: RunId, job_id: &JobId) -> bool {
    let key = (run_id, job_id.clone());
    let Some(job) = tx.claimed_jobs.remove(&key) else {
        return false;
    };
    clear_assignment(tx, run_id, job_id);
    // `set_job_status` records the override even when the run isn't loaded
    // (a widened requeue), and mirrors into `run.jobs` when it is.
    tx.set_job_status(run_id, job_id.clone(), ExecutionStatus::Queued);
    if let Some(run) = tx.runs.get_mut(&run_id) {
        run.status = summarize_run(run.jobs.values().copied());
    }
    tx.push_ready(job);
    true
}

// ─────────────────────────────────────────────────────────────────────────
// Job-level concurrency gate
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobGateOutcome {
    Proceed,
    Parked,
    Failed(ExecutionStatus),
}

pub(crate) fn try_acquire_job_gate(
    tx: &mut TxState,
    github: &serde_json::Value,
    submission: &WorkflowSubmission,
    queued_job: &QueuedJob,
) -> JobGateOutcome {
    let Some(raw) = queued_job.concurrency.clone() else {
        return JobGateOutcome::Proceed;
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
            return JobGateOutcome::Failed(ExecutionStatus::Failure);
        }
    };
    if group.trim().is_empty() {
        return JobGateOutcome::Failed(ExecutionStatus::Failure);
    }

    let key = concurrency::concurrency_key(&submission.repository, &group);
    let holder = concurrency::Holder::Job {
        run_id: queued_job.run_id,
        job_id: queued_job.job_id.clone(),
    };
    match try_acquire_concurrency(tx, key, group, holder, cancel, queue) {
        Ok(true) => JobGateOutcome::Proceed,
        Ok(false) => JobGateOutcome::Parked,
        Err(e) if e == concurrency::ARRIVAL_CANCELLED => {
            JobGateOutcome::Failed(ExecutionStatus::Cancelled)
        }
        Err(_) => JobGateOutcome::Failed(ExecutionStatus::Failure),
    }
}

pub(crate) fn try_enqueue_with_job_concurrency(
    tx: &mut TxState,
    github: &serde_json::Value,
    submission: &WorkflowSubmission,
    mut queued_job: QueuedJob,
    statuses: &mut BTreeMap<JobId, ExecutionStatus>,
) -> Result<bool, ()> {
    match try_acquire_job_gate(tx, github, submission, &queued_job) {
        JobGateOutcome::Proceed => {
            if queued_job.concurrency.is_some() {
                stamp_concurrency_acquired(&mut queued_job);
            }
            statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Queued);
            stamp_ready_enqueue(&mut queued_job);
            on_job_enqueued(tx, &queued_job);
            tx.push_ready(queued_job);
            Ok(true)
        }
        JobGateOutcome::Parked => {
            stamp_concurrency_wait_started(&mut queued_job);
            statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Pending);
            tx.concurrency_blocked.push_back(queued_job);
            Ok(false)
        }
        JobGateOutcome::Failed(status) => {
            statuses.insert(queued_job.job_id.clone(), status);
            Err(())
        }
    }
}

/// Resolve the agent job GUID for an in-flight job, if any.
pub(crate) fn agent_job_id_for(tx: &TxState, run_id: RunId, job_id: &JobId) -> Option<uuid::Uuid> {
    tx.job_requests
        .values()
        .find(|r| r.run_id == run_id && r.job_id == *job_id && r.result.is_none())
        .map(|r| r.agent_job_id)
        .or_else(|| {
            tx.job_requests
                .values()
                .find(|r| r.run_id == run_id && r.job_id == *job_id)
                .map(|r| r.agent_job_id)
        })
}

// ─────────────────────────────────────────────────────────────────────────
// Cancellation
// ─────────────────────────────────────────────────────────────────────────

/// Cancel every non-terminal job of a run (ported verbatim).
/// Returns the number of cancellation messages enqueued.
pub(crate) fn cancel_run_inner(tx: &mut TxState, run_id: RunId, reason: Option<&str>) -> usize {
    let expandable = expandable_job_ids(tx, run_id);
    let mut in_progress: Vec<JobId> = Vec::new();
    {
        let Some(record) = tx.runs.get_mut(&run_id) else {
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
        // A cancelled run is terminal: stamp completion metadata so the
        // record carries `completed_at`/`conclusion` like the old handler did.
        finalize_run_if_complete(record);
    }

    let mut cancellations = Vec::new();
    for job_id in in_progress {
        if let Some(agent_job_id) = agent_job_id_for(tx, run_id, &job_id) {
            cancellations.push(QueuedCancellation {
                run_id,
                job_id,
                agent_job_id,
            });
        }
    }
    let count = cancellations.len();
    tx.cancellation_queue.extend(cancellations);

    tx.retain_ready(|job| job.run_id != run_id);
    tx.pending_jobs.retain(|job| job.run_id != run_id);
    tx.held_runs.remove(&run_id);
    tx.job_assignments.retain(|(id, _), _| *id != run_id);
    tx.pool_pending.retain(|(id, _), _| *id != run_id);
    tx.concurrency_blocked.retain(|job| job.run_id != run_id);
    tx.side.dap_removals.push(run_id);
    tx.pending_expansions.retain(|job| job.run_id != run_id);
    tx.expanding.retain(|(id, _)| *id != run_id);
    for node_id in expandable {
        let status = node_settle_status(tx, run_id, &node_id);
        retire_node_requests(tx, run_id, &node_id, RequestRetirement::Settle(status));
    }

    release_concurrency_for_run(tx, run_id);
    tx.jobset_admissions.retain(|id, _| id.run_id != run_id);
    tx.jobset_ready.retain(|id| id.run_id != run_id);

    let _ = reason;
    count
}

/// Cancel a single job (ported verbatim).
pub(crate) fn cancel_job_inner(tx: &mut TxState, run_id: RunId, job_id: &JobId) -> usize {
    let expandable = is_expandable_node(tx, run_id, job_id);
    let was_in_progress = {
        let Some(record) = tx.runs.get_mut(&run_id) else {
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
        if let Some(agent_job_id) = agent_job_id_for(tx, run_id, job_id) {
            tx.cancellation_queue.push_back(QueuedCancellation {
                run_id,
                job_id: job_id.clone(),
                agent_job_id,
            });
            count = 1;
        }
    }
    tx.retain_ready(|j| !(j.run_id == run_id && j.job_id == *job_id));
    tx.job_assignments
        .retain(|(id, jid), _| !(*id == run_id && *jid == *job_id));
    tx.pool_pending
        .retain(|(id, jid), _| !(*id == run_id && *jid == *job_id));
    tx.pending_jobs
        .retain(|j| !(j.run_id == run_id && j.job_id == *job_id));
    tx.concurrency_blocked
        .retain(|j| !(j.run_id == run_id && j.job_id == *job_id));
    if let Some(held) = tx.held_runs.get_mut(&run_id) {
        held.retain(|j| j.job_id != *job_id);
    }
    tx.pending_expansions
        .retain(|j| !(j.run_id == run_id && j.job_id == *job_id));
    tx.expanding.remove(&(run_id, job_id.clone()));

    let inner_ids = tx
        .runs
        .get(&run_id)
        .and_then(|run| run.reusable_calls.get(&job_id.0))
        .map(|call| call.inner_job_ids.clone())
        .unwrap_or_default();
    for inner_id in inner_ids {
        count += cancel_job_inner(tx, run_id, &JobId(inner_id));
    }

    if expandable {
        let status = node_settle_status(tx, run_id, job_id);
        retire_node_requests(tx, run_id, job_id, RequestRetirement::Settle(status));
    }
    release_concurrency_for_job(tx, run_id, job_id);
    count
}

// ─────────────────────────────────────────────────────────────────────────
// Concurrency groups
// ─────────────────────────────────────────────────────────────────────────

pub(crate) fn release_concurrency_for_run(tx: &mut TxState, run_id: RunId) {
    let keys: Vec<(String, String)> = tx.holder_keys.get(&run_id).cloned().unwrap_or_default();
    for key in keys {
        if let Some(group) = tx.concurrency_groups.get_mut(&key) {
            let running_match = group
                .running
                .as_ref()
                .is_some_and(|h| h.is_run_holder(run_id) || h.run_id() == run_id);
            if running_match {
                let done = group.running.take();
                if let Some(done) = done {
                    promote_next_from_group(tx, &key, done);
                }
            } else {
                if let Some(group) = tx.concurrency_groups.get_mut(&key) {
                    group.pending.retain(|h| h.run_id() != run_id);
                    if group.running.is_none() && group.pending.is_empty() {
                        tx.concurrency_groups.remove(&key);
                    }
                }
            }
        }
    }
    tx.holder_keys.remove(&run_id);
}

/// Release every group presence of one job (ported verbatim: scans every
/// group, not only the run's `holder_keys`, because a pending holder can
/// outlive the bookkeeping that created it).
pub(crate) fn release_concurrency_for_job(tx: &mut TxState, run_id: RunId, job_id: &JobId) {
    let keys: Vec<(String, String)> = tx.concurrency_groups.keys().cloned().collect();
    for key in keys {
        let should_release = {
            let Some(group) = tx.concurrency_groups.get(&key) else {
                continue;
            };
            match &group.running {
                Some(h) if h.contains_job(run_id, job_id) => match h {
                    concurrency::Holder::Job { .. } => true,
                    concurrency::Holder::Run(_) | concurrency::Holder::JobSet { .. } => tx
                        .runs
                        .get(&run_id)
                        .is_some_and(|r| concurrency::holder_is_terminal(h, &r.jobs)),
                },
                _ => false,
            }
        };
        if let Some(group) = tx.concurrency_groups.get_mut(&key) {
            group.pending.retain(|h| !h.contains_job(run_id, job_id));
        }
        if should_release {
            if let Some(group) = tx.concurrency_groups.get_mut(&key) {
                if let Some(done) = group.running.take() {
                    promote_next_from_group(tx, &key, done);
                }
            }
        } else if let Some(group) = tx.concurrency_groups.get(&key) {
            if group.running.is_none() && group.pending.is_empty() {
                tx.concurrency_groups.remove(&key);
            }
        }
        let run_still_present = tx.concurrency_groups.get(&key).is_some_and(|g| {
            g.running.as_ref().is_some_and(|h| h.run_id() == run_id)
                || g.pending.iter().any(|h| h.run_id() == run_id)
        });
        if !run_still_present {
            if let Some(rkeys) = tx.holder_keys.get_mut(&run_id) {
                rkeys.retain(|k| k != &key);
                if rkeys.is_empty() {
                    tx.holder_keys.remove(&run_id);
                }
            }
        }
    }
}

pub(crate) fn merge_jobset_gate(gates: &mut Vec<JobSetGate>, mut gate: JobSetGate) {
    if let Some(existing) = gates.iter_mut().find(|existing| existing.key == gate.key) {
        existing.cancel_in_progress |= gate.cancel_in_progress;
        if gate.queue == preloop_gha_parser::ConcurrencyQueue::Single {
            existing.queue = preloop_gha_parser::ConcurrencyQueue::Single;
        }
        return;
    }
    gate.display_name = gate.display_name.trim().to_owned();
    gates.push(gate);
    gates.sort_by(|left, right| left.key.cmp(&right.key));
}

pub(crate) fn release_holder_key(
    tx: &mut TxState,
    key: &(String, String),
    holder: &concurrency::Holder,
) {
    let mut promote = None;
    if let Some(group) = tx.concurrency_groups.get_mut(key) {
        if group.running.as_ref() == Some(holder) {
            promote = group.running.take();
        } else {
            group.pending.retain(|pending| pending != holder);
        }
    }
    if let Some(done) = promote {
        promote_next_from_group(tx, key, done);
    }
    if tx
        .concurrency_groups
        .get(key)
        .is_some_and(|group| group.running.is_none() && group.pending.is_empty())
    {
        tx.concurrency_groups.remove(key);
    }

    let run_id = holder.run_id();
    let run_still_present = tx.concurrency_groups.get(key).is_some_and(|group| {
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
        if let Some(keys) = tx.holder_keys.get_mut(&run_id) {
            keys.retain(|candidate| candidate != key);
            if keys.is_empty() {
                tx.holder_keys.remove(&run_id);
            }
        }
    }
}

pub(crate) fn release_jobset_admission(tx: &mut TxState, id: &JobSetId) {
    let Some(admission) = tx.jobset_admissions.remove(id) else {
        return;
    };
    let holder = id.holder();
    for key in admission.acquired_keys {
        release_holder_key(tx, &key, &holder);
    }
}

pub(crate) fn advance_jobset_admission(
    tx: &mut TxState,
    id: &JobSetId,
    promoted_key: Option<&(String, String)>,
) -> Result<JobSetAdmissionResult, String> {
    if let Some(key) = promoted_key {
        if let Some(admission) = tx.jobset_admissions.get_mut(id) {
            admission.acquired_keys.insert(key.clone());
        }
    }

    loop {
        let next_gate = {
            let Some(admission) = tx.jobset_admissions.get(id) else {
                return Ok(JobSetAdmissionResult::Ready);
            };
            admission
                .gates
                .iter()
                .find(|gate| !admission.acquired_keys.contains(&gate.key))
                .cloned()
        };
        let Some(gate) = next_gate else {
            tx.jobset_admissions.remove(id);
            return Ok(JobSetAdmissionResult::Ready);
        };

        let holder = id.holder();
        match try_acquire_concurrency(
            tx,
            gate.key.clone(),
            gate.display_name,
            holder,
            gate.cancel_in_progress,
            gate.queue,
        ) {
            Ok(true) => {
                if let Some(admission) = tx.jobset_admissions.get_mut(id) {
                    admission.acquired_keys.insert(gate.key);
                }
            }
            Ok(false) => return Ok(JobSetAdmissionResult::Blocked),
            Err(error) => {
                release_jobset_admission(tx, id);
                return Err(error);
            }
        }
    }
}

/// After a holder finishes, promote the next pending holder for the group
/// (ported verbatim).
pub(crate) fn promote_next_from_group(
    tx: &mut TxState,
    key: &(String, String),
    _done: concurrency::Holder,
) {
    let next = {
        let Some(group) = tx.concurrency_groups.get_mut(key) else {
            return;
        };
        group.pending.pop_front()
    };

    let Some(next) = next else {
        if let Some(group) = tx.concurrency_groups.get(key) {
            if group.running.is_none() && group.pending.is_empty() {
                tx.concurrency_groups.remove(key);
            }
        }
        return;
    };

    // Mark the group running for Job/JobSet holders up front. `Holder::Run`
    // is marked inside its arm only after `held_runs.remove` confirms the
    // run's held jobs are actually loaded — a foreign run (not in this tx's
    // scope) has no `held_runs` entry, and marking the group running while
    // its jobs stay persisted `held` would strand them and wedge the slot.
    if matches!(&next, concurrency::Holder::JobSet { .. }) {
        if let Some(group) = tx.concurrency_groups.get_mut(key) {
            group.running = Some(next.clone());
        }
    }
    match next {
        concurrency::Holder::Run(run_id) => {
            if let Some(jobs) = tx.held_runs.remove(&run_id) {
                // The run's held jobs are loaded — safe to occupy the slot.
                if let Some(group) = tx.concurrency_groups.get_mut(key) {
                    group.running = Some(concurrency::Holder::Run(run_id));
                }
                for mut job in jobs {
                    if let Some(run) = tx.runs.get_mut(&run_id) {
                        run.jobs.insert(job.job_id.clone(), ExecutionStatus::Queued);
                    }
                    let needs_ok = tx.runs.get(&run_id).is_some_and(|run| {
                        job.needs
                            .iter()
                            .all(|n| crate::scheduling::need_satisfied(&run.jobs, n))
                    });
                    if needs_ok && under_max_parallel(tx, &job) {
                        let gate = tx
                            .runs
                            .get(&run_id)
                            .map(|run| (run.github.clone(), run.submission.clone()));
                        let gate_outcome = if let Some((github, submission)) = gate {
                            try_acquire_job_gate(tx, &github, &submission, &job)
                        } else {
                            JobGateOutcome::Proceed
                        };
                        match gate_outcome {
                            JobGateOutcome::Proceed => {
                                stamp_concurrency_acquired(&mut job);
                                if let Some(run) = tx.runs.get_mut(&run_id) {
                                    hydrate_needs_context(&mut job, run);
                                }
                                sync_broker_message(tx, &job);
                                stamp_ready_enqueue(&mut job);
                                on_job_enqueued(tx, &job);
                                tx.push_ready(job);
                            }
                            JobGateOutcome::Parked => {
                                if let Some(run) = tx.runs.get_mut(&run_id) {
                                    run.jobs
                                        .insert(job.job_id.clone(), ExecutionStatus::Pending);
                                }
                                stamp_concurrency_wait_started(&mut job);
                                tx.concurrency_blocked.push_back(job);
                            }
                            JobGateOutcome::Failed(status) => {
                                if let Some(run) = tx.runs.get_mut(&run_id) {
                                    run.jobs.insert(job.job_id.clone(), status);
                                    run.status = summarize_run(run.jobs.values().copied());
                                    finalize_run_if_complete(run);
                                }
                            }
                        }
                    } else {
                        if let Some(run) = tx.runs.get_mut(&run_id) {
                            run.jobs.insert(job.job_id.clone(), ExecutionStatus::Queued);
                        }
                        tx.pending_jobs.push_back(job);
                    }
                }
                if let Some(run) = tx.runs.get_mut(&run_id) {
                    if run.status == ExecutionStatus::Pending {
                        run.status = ExecutionStatus::InProgress;
                    }
                }
            } else if !tx.runs.contains_key(&run_id) {
                // Foreign run: its held jobs aren't in this tx's scope, so
                // `held_runs` has no entry. Don't mark the group running or
                // drop the holder — push it back to the FRONT of pending so a
                // later transaction that DOES load the run promotes it.
                if let Some(group) = tx.concurrency_groups.get_mut(key) {
                    group.pending.push_front(concurrency::Holder::Run(run_id));
                }
            }
            // else: run IS loaded but has no held jobs left (already
            // promoted) — consume the holder; nothing to requeue.
        }
        concurrency::Holder::Job { run_id, job_id } => {
            let pos = tx
                .concurrency_blocked
                .iter()
                .position(|j| j.run_id == run_id && j.job_id == job_id);
            let Some(pos) = pos else { return };
            let mut job = tx.concurrency_blocked.remove(pos).unwrap();
            if !under_max_parallel(tx, &job) {
                tx.concurrency_blocked.insert(pos, job);
                if let Some(group) = tx.concurrency_groups.get_mut(key) {
                    group
                        .pending
                        .push_front(concurrency::Holder::Job { run_id, job_id });
                }
                return;
            }
            if let Some(group) = tx.concurrency_groups.get_mut(key) {
                group.running = Some(concurrency::Holder::Job { run_id, job_id });
            }
            // `set_job_status` records the override even when the run isn't
            // loaded (a widened promotion), and mirrors into `run.jobs` when
            // it is — a blocked job promoted to ready becomes `queued`.
            tx.set_job_status(run_id, job.job_id.clone(), ExecutionStatus::Queued);
            if let Some(run) = tx.runs.get_mut(&run_id) {
                hydrate_needs_context(&mut job, run);
            }
            sync_broker_message(tx, &job);
            stamp_concurrency_acquired(&mut job);
            stamp_ready_enqueue(&mut job);
            on_job_enqueued(tx, &job);
            tx.push_ready(job);
        }
        concurrency::Holder::JobSet { run_id, job_ids } => {
            let id = JobSetId {
                run_id,
                job_ids: job_ids.clone(),
            };
            match advance_jobset_admission(tx, &id, Some(key)) {
                Ok(JobSetAdmissionResult::Blocked) => return,
                Err(_) => {
                    cancel_holder(
                        tx,
                        &concurrency::Holder::JobSet { run_id, job_ids },
                        concurrency::cancelled_reason().as_deref(),
                    );
                    return;
                }
                Ok(JobSetAdmissionResult::Ready) => {}
            }

            tx.jobset_ready.insert(id.clone());
            let mut to_queue = Vec::new();
            tx.concurrency_blocked.retain(|job| {
                if job.run_id == run_id && job_ids.contains(&job.job_id) {
                    to_queue.push(job.clone());
                    false
                } else {
                    true
                }
            });
            for mut job in to_queue {
                stamp_concurrency_acquired(&mut job);
                if job.reusable_call.is_some() {
                    tx.set_job_status(run_id, job.job_id.clone(), ExecutionStatus::Pending);
                    tx.pending_jobs.push_back(job);
                    continue;
                }
                if under_max_parallel(tx, &job) {
                    tx.set_job_status(run_id, job.job_id.clone(), ExecutionStatus::Queued);
                    if let Some(run) = tx.runs.get_mut(&run_id) {
                        hydrate_needs_context(&mut job, run);
                    }
                    sync_broker_message(tx, &job);
                    stamp_ready_enqueue(&mut job);
                    on_job_enqueued(tx, &job);
                    tx.push_ready(job);
                } else {
                    tx.set_job_status(run_id, job.job_id.clone(), ExecutionStatus::Queued);
                    tx.pending_jobs.push_back(job);
                }
            }
        }
    }
}

/// Try to acquire a concurrency slot for a holder.
///
/// `Err(ARRIVAL_CANCELLED)` means the arrival is cancelled on arrival: the
/// queue overflowed, or a newer holder already supersedes it (see below).
pub(crate) fn try_acquire_concurrency(
    tx: &mut TxState,
    key: (String, String),
    display_name: String,
    holder: concurrency::Holder,
    cancel_in_progress: bool,
    queue: preloop_gha_parser::ConcurrencyQueue,
) -> Result<bool, String> {
    let running_stuck = tx
        .concurrency_groups
        .get(&key)
        .and_then(|group| group.running.as_ref())
        .is_some_and(|running| run_stuck_on_external_hosts(tx, &running.run_id()));
    // A late delivery (GitHub reorders webhooks; retries and watchdog
    // redeliveries arrive late by design) must not pre-empt a newer holder.
    // In the modes where an arrival displaces others — cancel-in-progress and
    // the single pending slot — GitHub would already have cancelled the older
    // run when the newer one arrived, so cancel the stale arrival instead of
    // letting arrival order decide.
    if cancel_in_progress || queue == preloop_gha_parser::ConcurrencyQueue::Single {
        if let Some(arrival) = holder_event_order(tx, &holder) {
            let superseded = tx.concurrency_groups.get(&key).is_some_and(|group| {
                group
                    .running
                    .iter()
                    .chain(group.pending.iter())
                    .filter(|existing| existing.run_id() != holder.run_id())
                    .filter_map(|existing| holder_event_order(tx, existing))
                    .any(|existing| arrival.is_older_than(&existing))
            });
            if superseded {
                return Err(concurrency::ARRIVAL_CANCELLED.to_owned());
            }
        }
    }
    let group =
        tx.concurrency_groups
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
        track_holder_key(tx, &holder, key);
        return Ok(true);
    }
    if running_stuck {
        group.running = None;
        group.running = Some(holder.clone());
        let _ = group;
        track_holder_key(tx, &holder, key);
        return Ok(true);
    }
    if cancel_in_progress {
        let prev = group.running.take();
        let stale_pending: Vec<concurrency::Holder> = group.pending.drain(..).collect();
        group.running = Some(holder.clone());
        let _ = group;
        track_holder_key(tx, &holder, key.clone());
        if let Some(prev) = prev {
            if prev.run_id() != holder.run_id() {
                cancel_holder(tx, &prev, concurrency::cancelled_reason().as_deref());
            }
        }
        for pending in stale_pending {
            if pending.run_id() != holder.run_id() {
                cancel_holder(tx, &pending, concurrency::cancelled_reason().as_deref());
            }
        }
        return Ok(true);
    }
    let _ = group;

    let join = {
        let group = tx.concurrency_groups.get(&key).unwrap();
        concurrency::apply_queue_mode(queue, &group.pending)
    };

    for pending_holder in join.cancel_pending {
        if pending_holder.run_id() == holder.run_id() {
            continue;
        }
        cancel_holder(
            tx,
            &pending_holder,
            concurrency::cancelled_reason().as_deref(),
        );
        if let Some(group) = tx.concurrency_groups.get_mut(&key) {
            group.pending.retain(|h| h != &pending_holder);
        }
    }

    if join.cancel_arrival {
        return Err(concurrency::ARRIVAL_CANCELLED.to_owned());
    }

    if join.park_arrival {
        if let Some(group) = tx.concurrency_groups.get_mut(&key) {
            group.pending.push_back(holder.clone());
        }
        track_holder_key(tx, &holder, key);
        return Ok(false);
    }

    Ok(true)
}

/// GitHub's ordering for the event that triggered `holder`'s run.
fn holder_event_order(
    tx: &TxState,
    holder: &concurrency::Holder,
) -> Option<concurrency::EventOrder> {
    let submission = &tx.runs.get(&holder.run_id())?.submission;
    concurrency::event_order(
        &submission.event,
        &submission.repository,
        &submission.payload,
    )
}

pub(crate) fn track_holder_key(
    tx: &mut TxState,
    holder: &concurrency::Holder,
    key: (String, String),
) {
    let run_id = holder.run_id();
    let keys = tx.holder_keys.entry(run_id).or_default();
    if !keys.contains(&key) {
        keys.push(key);
    }
}

pub(crate) fn cancel_holder(tx: &mut TxState, holder: &concurrency::Holder, _reason: Option<&str>) {
    match holder {
        concurrency::Holder::Run(run_id) => {
            cancel_run_inner(tx, *run_id, Some("concurrency_cancelled"));
        }
        concurrency::Holder::Job { run_id, job_id } => {
            cancel_job_inner(tx, *run_id, job_id);
        }
        concurrency::Holder::JobSet { run_id, job_ids } => {
            tx.jobset_admissions.remove(&JobSetId {
                run_id: *run_id,
                job_ids: job_ids.clone(),
            });
            tx.jobset_ready.remove(&JobSetId {
                run_id: *run_id,
                job_ids: job_ids.clone(),
            });
            for job_id in job_ids {
                cancel_job_inner(tx, *run_id, job_id);
            }
            if let Some(run) = tx.runs.get_mut(run_id) {
                if run.jobs.values().all(|status| status.is_terminal()) {
                    run.status = summarize_run(run.jobs.values().copied());
                }
            }
        }
    }
}

/// Drop concurrency-group holders whose runs are terminal or missing
/// (startup reconcile, ported verbatim).
pub(crate) fn reconcile_concurrency_groups(tx: &mut TxState) {
    let terminal = |tx: &TxState, run_id: &RunId| {
        tx.runs.get(run_id).is_none_or(|run| {
            matches!(
                run.status,
                ExecutionStatus::Success
                    | ExecutionStatus::Failure
                    | ExecutionStatus::Cancelled
                    | ExecutionStatus::Skipped
            )
        })
    };
    let external_host_available = tx.runners.values().any(|runner| {
        runner.labels.iter().any(|label| {
            let label = label.to_ascii_lowercase();
            label.starts_with("macos") || label.starts_with("windows")
        })
    });
    let stuck = |tx: &TxState, run_id: &RunId| {
        if external_host_available {
            return false;
        }
        run_stuck_on_external_hosts(tx, run_id)
    };
    let dead_running: Vec<(String, String)> =
        tx.concurrency_groups
            .iter()
            .filter(|(_, group)| {
                group.running.as_ref().is_some_and(|holder| {
                    terminal(tx, &holder.run_id()) || stuck(tx, &holder.run_id())
                })
            })
            .map(|(key, _)| key.clone())
            .collect();
    for key in &dead_running {
        if let Some(group) = tx.concurrency_groups.get_mut(key) {
            group.running = None;
        }
    }
    let dead_pending: Vec<((String, String), Vec<concurrency::Holder>)> = tx
        .concurrency_groups
        .iter()
        .map(|(key, group)| {
            let dead = group
                .pending
                .iter()
                .filter(|holder| terminal(tx, &holder.run_id()) || stuck(tx, &holder.run_id()))
                .cloned()
                .collect();
            (key.clone(), dead)
        })
        .collect();
    for (key, dead) in dead_pending {
        if let Some(group) = tx.concurrency_groups.get_mut(&key) {
            group.pending.retain(|holder| !dead.contains(holder));
        }
    }
    tx.concurrency_groups
        .retain(|_, group| group.running.is_some() || !group.pending.is_empty());
    let dead_keys: Vec<RunId> = tx
        .holder_keys
        .keys()
        .filter(|run_id| terminal(tx, run_id) || stuck(tx, run_id))
        .cloned()
        .collect();
    for run_id in dead_keys {
        tx.holder_keys.remove(&run_id);
    }
}

pub(crate) fn run_stuck_on_external_hosts(tx: &TxState, run_id: &RunId) -> bool {
    let Some(run) = tx.runs.get(run_id) else {
        return true;
    };
    run.jobs.iter().all(|(job_id, status)| {
        matches!(
            status,
            ExecutionStatus::Success
                | ExecutionStatus::Failure
                | ExecutionStatus::Cancelled
                | ExecutionStatus::Skipped
        ) || ready_jobs(tx).any(|queued| {
            queued.run_id == *run_id
                && queued.job_id == *job_id
                && queued.runs_on.iter().any(|label| {
                    let label = label.to_ascii_lowercase();
                    label.starts_with("macos") || label.starts_with("windows")
                })
        })
    })
}

// ─────────────────────────────────────────────────────────────────────────
// Dependency promotion
// ─────────────────────────────────────────────────────────────────────────

/// Promote or skip pending jobs once every declared dependency is terminal
/// (ported verbatim; `tx.pending_jobs` is the working set's pending list —
/// scoped to the affected runs, which is the only set a completion can
/// unblock since `needs:` never cross runs).
pub(crate) fn promote_ready_jobs(tx: &mut TxState) -> SchedulingOutcome {
    let mut outcome = SchedulingOutcome::default();
    loop {
        let mut promoted_by_base: BTreeMap<(RunId, String), u64> = BTreeMap::new();
        let mut promoted = Vec::new();
        let mut remaining = VecDeque::new();
        let mut settled = false;

        while let Some(mut job) = tx.pending_jobs.pop_front() {
            let decision = tx
                .runs
                .get(&job.run_id)
                .map(|run| dependency_decision(run, &job))
                .unwrap_or(DependencyDecision::Wait);
            if decision == DependencyDecision::Run {
                stamp_dependencies_ready(&mut job);
            }
            match decision {
                DependencyDecision::Run if job.reusable_call.is_some() => {
                    settled = true;
                    let set_id = JobSetId {
                        run_id: job.run_id,
                        job_ids: BTreeSet::from([job.job_id.clone()]),
                    };
                    let ready = tx.jobset_ready.remove(&set_id);
                    let mut concurrency_acquired = ready;
                    if !ready {
                        if tx.jobset_admissions.contains_key(&set_id) {
                            stamp_concurrency_wait_started(&mut job);
                            tx.concurrency_blocked.push_back(job);
                            continue;
                        }
                        match caller_jobset_gates(tx, &job) {
                            Err(status) => {
                                if let Some(run) = tx.runs.get_mut(&job.run_id) {
                                    run.jobs.insert(job.job_id.clone(), status);
                                    run.status = summarize_run(run.jobs.values().copied());
                                    finalize_run_if_complete(run);
                                }
                                outcome.failed.push((job.run_id, job.job_id));
                                continue;
                            }
                            Ok(Some(gates)) => {
                                tx.jobset_admissions.insert(
                                    set_id.clone(),
                                    JobSetAdmission {
                                        gates,
                                        acquired_keys: BTreeSet::new(),
                                    },
                                );
                                match advance_jobset_admission(tx, &set_id, None) {
                                    Ok(JobSetAdmissionResult::Ready) => {
                                        concurrency_acquired = true;
                                    }
                                    Ok(JobSetAdmissionResult::Blocked) => {
                                        stamp_concurrency_wait_started(&mut job);
                                        tx.concurrency_blocked.push_back(job);
                                        continue;
                                    }
                                    Err(error) => {
                                        let status = if error == concurrency::ARRIVAL_CANCELLED {
                                            ExecutionStatus::Cancelled
                                        } else {
                                            ExecutionStatus::Failure
                                        };
                                        if let Some(run) = tx.runs.get_mut(&job.run_id) {
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
                    if concurrency_acquired {
                        stamp_concurrency_acquired(&mut job);
                    }
                    defer_expansion(tx, job);
                }
                DependencyDecision::Run if job.deferred_matrix.is_some() => {
                    settled = true;
                    defer_expansion(tx, job);
                }
                DependencyDecision::Run
                    if under_max_parallel(tx, &job)
                        && promoted_by_base
                            .get(&(job.run_id, job.base_id.clone()))
                            .copied()
                            .unwrap_or(0)
                            < job.max_parallel.unwrap_or(u64::MAX) =>
                {
                    if let Some(run) = tx.runs.get(&job.run_id) {
                        hydrate_needs_context(&mut job, run);
                    }
                    sync_broker_message(tx, &job);
                    let gate = tx
                        .runs
                        .get(&job.run_id)
                        .map(|run| (run.github.clone(), run.submission.clone()));
                    let gate_outcome = if let Some((github, submission)) = gate {
                        try_acquire_job_gate(tx, &github, &submission, &job)
                    } else {
                        JobGateOutcome::Proceed
                    };
                    match gate_outcome {
                        JobGateOutcome::Proceed => {
                            if job.concurrency.is_some() {
                                stamp_concurrency_acquired(&mut job);
                            }
                            *promoted_by_base
                                .entry((job.run_id, job.base_id.clone()))
                                .or_default() += 1;
                            promoted.push(job);
                        }
                        JobGateOutcome::Parked => {
                            stamp_concurrency_wait_started(&mut job);
                            if let Some(run) = tx.runs.get_mut(&job.run_id) {
                                run.jobs
                                    .insert(job.job_id.clone(), ExecutionStatus::Pending);
                            }
                            tx.concurrency_blocked.push_back(job);
                        }
                        JobGateOutcome::Failed(status) => {
                            if let Some(run) = tx.runs.get_mut(&job.run_id) {
                                run.jobs.insert(job.job_id.clone(), status);
                                run.status = summarize_run(run.jobs.values().copied());
                                finalize_run_if_complete(run);
                            }
                            outcome.failed.push((job.run_id, job.job_id));
                            settled = true;
                        }
                    }
                }
                DependencyDecision::Skip | DependencyDecision::Error => {
                    let status = if decision == DependencyDecision::Skip {
                        ExecutionStatus::Skipped
                    } else {
                        ExecutionStatus::Failure
                    };
                    if let Some(run) = tx.runs.get_mut(&job.run_id) {
                        run.jobs.insert(job.job_id.clone(), status);
                        run.status = summarize_run(run.jobs.values().copied());
                        finalize_run_if_complete(run);
                    }
                    if job.deferred_matrix.is_some() || job.reusable_call.is_some() {
                        retire_node_requests(
                            tx,
                            job.run_id,
                            &job.job_id,
                            RequestRetirement::Settle(status),
                        );
                    }
                    release_concurrency_for_job(tx, job.run_id, &job.job_id);
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
        for job in &mut promoted {
            stamp_ready_enqueue(job);
        }
        tx.pending_jobs = remaining;
        for job in promoted {
            tx.push_ready(job);
        }
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
    let condition = preloop_gha_expressions::effective_condition(job.if_condition.as_deref());
    match preloop_gha_expressions::eval_bool(&condition, &context) {
        Ok(true) => DependencyDecision::Run,
        Ok(false) => DependencyDecision::Skip,
        Err(_) => DependencyDecision::Error,
    }
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

pub(crate) fn under_max_parallel(tx: &TxState, job: &QueuedJob) -> bool {
    let Some(max_parallel) = job.max_parallel else {
        return true;
    };
    let active_in_queue = ready_jobs(tx)
        .filter(|queued| queued.run_id == job.run_id && queued.base_id == job.base_id)
        .count() as u64;
    let active_running = tx
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
    tx: &mut TxState,
    run_id: RunId,
    failed_job: &JobId,
) -> Vec<JobId> {
    let Some(run) = tx.runs.get_mut(&run_id) else {
        return Vec::new();
    };
    let Some(base_id) = run.job_base_ids.get(failed_job).cloned() else {
        return Vec::new();
    };
    if !run.job_fail_fast.get(&base_id).copied().unwrap_or(true) {
        return Vec::new();
    }

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
                cancellations.push(QueuedCancellation {
                    run_id,
                    job_id: job_id.clone(),
                    agent_job_id: uuid::Uuid::nil(),
                });
            }
            cancelled_jobs.push(job_id.clone());
            *status = ExecutionStatus::Cancelled;
        }
    }
    run.status = summarize_run(run.jobs.values().copied());
    tx.retain_ready(|job| !(job.run_id == run_id && job.base_id == base_id));
    tx.pending_jobs
        .retain(|job| !(job.run_id == run_id && job.base_id == base_id));
    cancellations.retain_mut(|c| {
        if let Some(id) = agent_job_id_for(tx, c.run_id, &c.job_id) {
            c.agent_job_id = id;
            true
        } else {
            false
        }
    });
    tx.cancellation_queue.extend(cancellations);
    for job_id in &cancelled_jobs {
        release_concurrency_for_job(tx, run_id, job_id);
    }
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

    let Some(environment) = job.environment.as_ref() else {
        return;
    };
    let Some(name) = (match environment {
        serde_json::Value::String(name) => Some(name.as_str()),
        serde_json::Value::Object(map) => map.get("name").and_then(serde_json::Value::as_str),
        _ => None,
    }) else {
        return;
    };
    if !preloop_gha_parser::eval::resolves_after_job_build(name) {
        return;
    }
    let Some(actions_environment) = job.message.actions_environment.as_mut() else {
        return;
    };
    let mut context = preloop_gha_expressions::Context::new();
    for (key, value) in &job.message.context_data {
        context.insert(key, value.to_json());
    }
    match preloop_gha_parser::eval::resolve_string(name, &context) {
        Ok(resolved) => actions_environment.name = resolved,
        Err(error) => {
            tracing::error!(
                run_id = %job.run_id.0,
                job = %job.job_id.0,
                environment = %name,
                %error,
                "deployment environment expression failed to evaluate after needs completed"
            );
        }
    }
}

/// Mirror a hydrated job message into `broker_messages` so `acquirejob`
/// serves the post-hydration payload. `submit_run_tx` stores the message at
/// submit time (needs context empty); `hydrate_needs_context` mutates the
/// `QueuedJob` copy at promotion. Without this sync the delivered message
/// keeps the empty `needs` context and `needs:`-gated consumers see no
/// upstream outputs.
pub(crate) fn sync_broker_message(tx: &mut TxState, job: &QueuedJob) {
    let request_id = job.message.request_id;
    if request_id != 0 {
        tx.broker_messages.insert(request_id, job.message.clone());
    }
}

fn caller_jobset_gates(
    tx: &TxState,
    job: &QueuedJob,
) -> Result<Option<Vec<JobSetGate>>, ExecutionStatus> {
    let Some(run) = tx.runs.get(&job.run_id) else {
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

// ─────────────────────────────────────────────────────────────────────────
// Expansion reservations
// ─────────────────────────────────────────────────────────────────────────

/// Hand a gated node to the expansion queue (ported verbatim).
pub(crate) fn defer_expansion(tx: &mut TxState, job: QueuedJob) {
    tx.expanding.insert((job.run_id, job.job_id.clone()));
    tx.pending_expansions.push_back(job);
}

/// Whether a single job is an expandable node (ported verbatim; `ready_index`
/// joins the scanned collections as the persisted ready queue).
fn is_expandable_node(tx: &TxState, run_id: RunId, job_id: &JobId) -> bool {
    tx.pending_jobs
        .iter()
        .chain(tx.pending_expansions.iter())
        .chain(ready_jobs(tx))
        .chain(tx.concurrency_blocked.iter())
        .chain(tx.held_runs.get(&run_id).into_iter().flatten())
        .any(|job| {
            job.run_id == run_id
                && job.job_id == *job_id
                && (job.deferred_matrix.is_some() || job.reusable_call.is_some())
        })
        || tx.expanding.contains(&(run_id, job_id.clone()))
        || tx
            .runs
            .get(&run_id)
            .is_some_and(|run| run.caller_plans.contains_key(job_id))
}

/// The ids of every expandable node in a run (ported verbatim).
pub(crate) fn expandable_job_ids(tx: &TxState, run_id: RunId) -> BTreeSet<JobId> {
    let mut ids = BTreeSet::new();
    for job in tx
        .pending_jobs
        .iter()
        .chain(tx.pending_expansions.iter())
        .chain(ready_jobs(tx))
        .chain(tx.concurrency_blocked.iter())
        .chain(tx.held_runs.get(&run_id).into_iter().flatten())
    {
        if job.run_id == run_id && (job.deferred_matrix.is_some() || job.reusable_call.is_some()) {
            ids.insert(job.job_id.clone());
        }
    }
    for (id, job_id) in &tx.expanding {
        if *id == run_id {
            ids.insert(job_id.clone());
        }
    }
    if let Some(run) = tx.runs.get(&run_id) {
        ids.extend(run.caller_plans.keys().cloned());
    }
    ids
}

pub(crate) fn node_settle_status(tx: &TxState, run_id: RunId, node_id: &JobId) -> ExecutionStatus {
    tx.runs
        .get(&run_id)
        .and_then(|run| run.jobs.get(node_id).copied())
        .unwrap_or(ExecutionStatus::Cancelled)
}

// ─────────────────────────────────────────────────────────────────────────
// Request lifecycle
// ─────────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
pub(crate) enum RequestRetirement {
    Settle(ExecutionStatus),
    Purge,
}

/// Settle one request whose logical job is terminal (ported verbatim).
pub(crate) fn settle_request(tx: &mut TxState, request_id: i64, status: ExecutionStatus) {
    // Capture the session that owned this request before releasing it: a
    // `JobCancellation` still sitting in that session's inflight messages is
    // moot once the job settles — the runner already stopped the work — so it
    // must not be redelivered on the next (busy) poll.
    let owner_session = tx
        .session_active_requests
        .iter()
        .find(|(_, &rid)| rid == request_id)
        .map(|(sid, _)| sid.clone());
    tx.session_active_requests
        .retain(|_, &mut rid| rid != request_id);
    tx.inflight_requests.remove(&request_id);
    tx.github_token_requests.remove(&request_id);
    if let Some(session_id) = owner_session {
        if let Some(messages) = tx.inflight_messages.get_mut(&session_id) {
            messages.retain(|_, msg| msg.message_type != azdo::message_type::JOB_CANCELLED);
            if messages.is_empty() {
                tx.inflight_messages.remove(&session_id);
            }
        }
    }
    if let Some(record) = tx.job_requests.get_mut(&request_id) {
        if record.result.is_none() {
            record.result = Some(status);
        }
    }
}

/// Release an interrupted claim so the same request can be delivered again
/// (ported verbatim).
pub(crate) fn release_request_for_retry(tx: &mut TxState, request_id: i64) {
    tx.session_active_requests
        .retain(|_, &mut rid| rid != request_id);
    if let Some(record) = tx.job_requests.get_mut(&request_id) {
        if record.result.is_none() {
            record.owner_runner_id = None;
            record.started_at = None;
            record.last_renewed_at = None;
            record.timeout_triggered = false;
            record.locked_until = crate::distributed_task::agent_request_locked_until();
        }
    }
}

/// Retire the request records an expandable node acquired at submit
/// (ported verbatim; node-local live-log state goes through `tx.side`).
pub(crate) fn retire_node_requests(
    tx: &mut TxState,
    run_id: RunId,
    node_id: &JobId,
    retirement: RequestRetirement,
) {
    let request_ids: Vec<i64> = tx
        .job_requests
        .iter()
        .filter(|(_, record)| record.run_id == run_id && record.job_id == *node_id)
        .map(|(id, _)| *id)
        .collect();
    for request_id in request_ids {
        match retirement {
            RequestRetirement::Settle(status) => {
                settle_request(tx, request_id, status);
            }
            RequestRetirement::Purge => {
                tx.session_active_requests
                    .retain(|_, &mut rid| rid != request_id);
                tx.inflight_requests.remove(&request_id);
                tx.github_token_requests.remove(&request_id);
                let Some(record) = tx.remove_request(request_id) else {
                    continue;
                };
                let agent_key = record.agent_job_id.to_string();
                tx.side.live_log_removals.push(agent_key);
                tx.job_steps.remove(&record.agent_job_id);
            }
        }
    }
    if matches!(retirement, RequestRetirement::Purge) {
        tx.id_token_grants.remove(&(run_id, node_id.clone()));
        tx.oidc_job_contexts.remove(&(run_id, node_id.clone()));
    }
}

/// Conclude a node whose subtree could not be built (ported verbatim).
pub(crate) fn fail_expansion_node(
    tx: &mut TxState,
    run_id: RunId,
    node_id: &JobId,
    status: ExecutionStatus,
    outcome: &mut SchedulingOutcome,
) {
    if let Some(run) = tx.runs.get_mut(&run_id) {
        run.jobs.insert(node_id.clone(), status);
        run.status = summarize_run(run.jobs.values().copied());
        finalize_run_if_complete(run);
    }
    release_concurrency_for_job(tx, run_id, node_id);
    retire_node_requests(tx, run_id, node_id, RequestRetirement::Settle(status));
    outcome.failed.push((run_id, node_id.clone()));
}

// ─────────────────────────────────────────────────────────────────────────
// Expansion: plan (in-transaction), build (outside), apply (in-transaction)
// ─────────────────────────────────────────────────────────────────────────

/// Everything a deferred node needs in order to build its subtree, cloned out
/// of the run record inside the claim transaction.
pub(crate) struct ExpansionContext {
    pub(crate) run_id: RunId,
    pub(crate) submission: Arc<WorkflowSubmission>,
    pub(crate) snapshot: Option<crate::snapshots::WorkspaceSnapshot>,
    pub(crate) github_json: serde_json::Value,
    pub(crate) workflow_path: String,
    pub(crate) workflow_ref: String,
    pub(crate) head_sha: String,
}

pub(crate) struct ReusableExpansionInputs {
    pub(crate) ctx: ExpansionContext,
    pub(crate) caller_id: JobId,
    pub(crate) caller_plan: preloop_gha_protocol::JobPlan,
    pub(crate) call: preloop_gha_protocol::ReusableCallPlan,
    pub(crate) needs_outputs: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
}

pub(crate) struct MatrixExpansionInputs {
    pub(crate) ctx: ExpansionContext,
    pub(crate) node_id: JobId,
    pub(crate) base_id: String,
    pub(crate) expression: String,
    pub(crate) needs_outputs: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    pub(crate) workflow_file: Option<String>,
}

pub(crate) enum ExpansionPlan {
    Reusable(Box<ReusableExpansionInputs>),
    Matrix(Box<MatrixExpansionInputs>),
}

/// One fully built inner job, still detached from the run.
pub(crate) struct BuiltJob {
    pub(crate) plan: preloop_gha_protocol::JobPlan,
    pub(crate) condition_context: preloop_gha_expressions::Context,
    pub(crate) artifacts: crate::runs::BuiltJobArtifacts,
}

pub(crate) enum BuiltExpansion {
    Reusable {
        caller_id: JobId,
        jobs: Vec<BuiltJob>,
        reusable_calls: BTreeMap<String, preloop_gha_parser::ReusableCallMetadata>,
    },
    Matrix {
        jobs: Vec<BuiltJob>,
    },
}

/// Snapshot the inputs a deferred node needs, inside the claim transaction
/// (ported verbatim).
pub(crate) fn plan_expansion(tx: &TxState, job: &QueuedJob) -> Option<ExpansionPlan> {
    let run = tx.runs.get(&job.run_id)?;
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
            needs_outputs: collect_needs_outputs(run, job),
        })));
    }
    let expression = job.deferred_matrix.clone()?;
    let workflow_file = run
        .caller_plans
        .get(&job.job_id)
        .and_then(|plan| plan.workflow_file.clone());
    Some(ExpansionPlan::Matrix(Box::new(MatrixExpansionInputs {
        ctx,
        node_id: job.job_id.clone(),
        base_id: job.base_id.clone(),
        expression,
        needs_outputs: collect_needs_outputs(run, job),
        workflow_file,
    })))
}

/// Collect each completed need's outputs for a deferred expression (ported
/// verbatim).
fn collect_needs_outputs(
    run: &RunRecord,
    job: &QueuedJob,
) -> BTreeMap<String, BTreeMap<String, serde_json::Value>> {
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
    needs_outputs
}

/// Build one job's runner artifacts per plan. Runs outside the transaction
/// (ported verbatim).
fn build_jobs<F>(
    shared: &crate::state::SharedState,
    ctx: &ExpansionContext,
    plans: &[preloop_gha_protocol::JobPlan],
    condition_context: F,
) -> Result<Vec<BuiltJob>, ExecutionStatus>
where
    F: Fn(
        &preloop_gha_protocol::JobPlan,
        &BTreeMap<String, String>,
    ) -> preloop_gha_expressions::Context,
{
    let base_url = crate::broker::runner_base_url();
    let normalized_github =
        preloop_gha_parser::job_builder::normalize_github_context(&ctx.github_json);
    let secrets_exposed = preloop_gha_protocol::masking::expose_all(&ctx.submission.secrets);
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

pub(crate) fn build_expansion(
    shared: &crate::state::SharedState,
    plan: ExpansionPlan,
) -> Result<BuiltExpansion, ExecutionStatus> {
    match plan {
        ExpansionPlan::Reusable(inputs) => build_reusable_expansion(shared, *inputs),
        ExpansionPlan::Matrix(inputs) => build_matrix_expansion(shared, *inputs),
    }
}

/// The workflow that contains a deferred reusable caller (ported verbatim).
fn caller_workflow_of(
    ctx: &ExpansionContext,
    caller_plan: &preloop_gha_protocol::JobPlan,
) -> Result<preloop_gha_parser::Workflow, ExecutionStatus> {
    let tail = caller_plan
        .base_id
        .rsplit_once('/')
        .map(|(_, tail)| tail)
        .unwrap_or(&caller_plan.base_id);
    let holds_caller = |workflow: &preloop_gha_parser::Workflow| {
        workflow.jobs.contains_key(&caller_plan.base_id) || workflow.jobs.contains_key(tail)
    };
    let yaml = caller_plan
        .workflow_file
        .as_deref()
        .and_then(|file| ctx.submission.reusable_workflows.get(file))
        .filter(|yaml| {
            preloop_gha_parser::parse_workflow(yaml)
                .map(|workflow| holds_caller(&workflow))
                .unwrap_or(false)
        })
        .map(String::as_str)
        .unwrap_or(ctx.submission.workflow_yaml.as_str());
    preloop_gha_parser::parse_workflow(yaml).map_err(|error| {
        tracing::warn!(
            run_id = %ctx.run_id,
            job = %caller_plan.id,
            %error,
            "caller workflow re-parse failed at expansion"
        );
        ExecutionStatus::Failure
    })
}

/// Materialize a deferred reusable caller's callee subtree (ported verbatim).
fn build_reusable_expansion(
    shared: &crate::state::SharedState,
    inputs: ReusableExpansionInputs,
) -> Result<BuiltExpansion, ExecutionStatus> {
    let ReusableExpansionInputs {
        ctx,
        caller_id,
        caller_plan,
        call,
        needs_outputs,
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
    let called = preloop_gha_parser::parse_workflow(yaml).map_err(|error| {
        tracing::warn!(%run_id, job = %caller_id, %error, "callee re-parse failed at expansion");
        ExecutionStatus::Failure
    })?;
    let expanded = if caller_plan.deferred_matrix.is_some() {
        let caller_workflow = caller_workflow_of(&ctx, &caller_plan)?;
        preloop_gha_parser::expand_deferred_reusable_call(
            &called,
            &caller_workflow,
            &caller_plan,
            &needs_outputs,
            &ctx.submission.reusable_workflows,
            &ctx.submission.reusable_workflow_shas,
        )
    } else {
        preloop_gha_parser::expand_reusable_call(
            &called,
            &caller_plan,
            &ctx.submission.reusable_workflows,
            &ctx.submission.reusable_workflow_shas,
        )
    }
    .map_err(|error| {
        tracing::warn!(%run_id, job = %caller_id, %error, "reusable subtree expansion failed");
        ExecutionStatus::Failure
    })?;
    if expanded.jobs.is_empty() {
        return Ok(BuiltExpansion::Matrix { jobs: Vec::new() });
    }

    let github_json = ctx.github_json.clone();
    let vars = ctx.submission.vars.clone();
    let jobs = build_jobs(shared, &ctx, &expanded.jobs, |plan, _secrets| {
        preloop_gha_parser::eval::build_context(
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

/// Materialize a dynamic `needs`-driven matrix (ported verbatim).
fn build_matrix_expansion(
    shared: &crate::state::SharedState,
    inputs: MatrixExpansionInputs,
) -> Result<BuiltExpansion, ExecutionStatus> {
    let MatrixExpansionInputs {
        ctx,
        node_id,
        base_id,
        expression,
        needs_outputs,
        workflow_file,
    } = inputs;
    let run_id = ctx.run_id;
    let workflow_yaml = workflow_file
        .as_deref()
        .and_then(|file| ctx.submission.reusable_workflows.get(file))
        .map(String::as_str)
        .unwrap_or(ctx.submission.workflow_yaml.as_str());
    let workflow = preloop_gha_parser::parse_workflow(workflow_yaml).map_err(|error| {
        tracing::warn!(%run_id, job = %node_id, %error, "workflow re-parse failed for dynamic matrix");
        ExecutionStatus::Failure
    })?;
    let plans = preloop_gha_parser::expand_deferred_matrix_job(
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
    let jobs = build_jobs(shared, &ctx, &plans, |plan, _secrets| {
        preloop_gha_parser::eval::build_context(
            &github_json,
            &BTreeMap::new(),
            &vars,
            &plan
                .matrix
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            &serde_json::json!({}),
            &BTreeMap::new(),
            &submission_inputs,
        )
    })?;
    Ok(BuiltExpansion::Matrix { jobs })
}

/// Insert correlation records and run bookkeeping for freshly built inner
/// jobs (ported verbatim; the five index inserts collapse into
/// `tx.insert_request`).
fn register_expanded_jobs(tx: &mut TxState, run_id: RunId, jobs: Vec<BuiltJob>) -> Vec<QueuedJob> {
    let mut queued = Vec::with_capacity(jobs.len());
    let platforms = registered_runner_platforms(tx);
    let mut unhostable: Vec<(JobId, String)> = Vec::new();
    for BuiltJob {
        plan,
        condition_context,
        mut artifacts,
    } in jobs
    {
        if let Some(platform) = unhostable_platform(&plan.runs_on, platforms.clone()) {
            unhostable.push((
                plan.id.clone(),
                format!(
                    "no {platform} runner is registered with this server, so `runs-on: {}` \
                     cannot be scheduled",
                    plan.runs_on.join(", ")
                ),
            ));
            if let Some(run) = tx.runs.get_mut(&run_id) {
                run.jobs.insert(plan.id.clone(), ExecutionStatus::Failure);
                run.job_base_ids
                    .insert(plan.id.clone(), plan.base_id.clone());
                run.job_needs.insert(plan.id.clone(), plan.needs.clone());
                run.job_names.insert(plan.id.clone(), plan.name.clone());
            }
            continue;
        }
        // Mint the `job_requests` primary key inside the writer transaction
        // (cross-process-safe) and stamp it on both the request record and
        // the runner-facing message — `build_job_artifacts` leaves a
        // placeholder (0) because it runs outside the transaction.
        let request_id = tx.alloc_request_id();
        artifacts.job_request.request_id = request_id;
        artifacts.agent_msg.request_id = request_id;
        let job_request = artifacts.job_request;
        tx.id_token_grants
            .insert((run_id, plan.id.clone()), artifacts.id_token_granted);
        tx.oidc_job_contexts
            .insert((run_id, plan.id.clone()), artifacts.oidc_ctx);
        tx.job_steps.insert(
            job_request.agent_job_id,
            crate::models::StepRecord::manifest(&artifacts.agent_msg.steps),
        );
        if let Some(request) = artifacts.github_token_request {
            tx.github_token_requests
                .insert(job_request.request_id, request);
            tracing::debug!(
                request_id = job_request.request_id,
                job = %plan.id,
                "build: dispatch token request inserted"
            );
        } else {
            tracing::debug!(
                request_id = job_request.request_id,
                job = %plan.id,
                "build: job has no dispatch token request"
            );
        }
        tx.insert_request(job_request);

        if let Some(run) = tx.runs.get_mut(&run_id) {
            run.jobs.insert(plan.id.clone(), ExecutionStatus::Queued);
            run.job_base_ids
                .insert(plan.id.clone(), plan.base_id.clone());
            run.job_needs.insert(plan.id.clone(), plan.needs.clone());
            run.job_fail_fast
                .insert(plan.base_id.clone(), plan.fail_fast);
            run.job_continue_on_error
                .insert(plan.id.to_string(), plan.continue_on_error);
            run.job_names.insert(plan.id.clone(), plan.name.clone());
            if plan.reusable_call.is_some() || plan.deferred_matrix.is_some() {
                run.caller_plans.insert(plan.id.clone(), plan.clone());
            }
        }
        let created_at_unix_nanos = crate::models::now_unix_nanos();
        queued.push(QueuedJob {
            run_id,
            job_id: plan.id.clone(),
            base_id: plan.base_id.clone(),
            created_at_unix_nanos,
            dependencies_ready_at_unix_nanos: plan
                .needs
                .is_empty()
                .then_some(created_at_unix_nanos),
            concurrency_wait_started_at_unix_nanos: None,
            concurrency_acquired_at_unix_nanos: None,
            enqueued_at_unix_nanos: 0,
            needs: plan.needs.clone(),
            if_condition: plan.if_condition.clone(),
            condition_context,
            max_parallel: plan.max_parallel,
            runs_on: plan.runs_on.clone(),
            runner_group: plan.runner_group.clone(),
            environment: plan.environment.clone(),
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
    for (job_id, reason) in unhostable {
        tracing::warn!(
            run_id = %run_id.0,
            job = %job_id.0,
            %reason,
            "materialized callee job is unhostable; failing it"
        );
    }
    queued
}

/// Fold a built subtree back into the run (ported verbatim; the `expanding`
/// reservation check is the fencing that discards a stale build).
pub(crate) fn apply_expansion(
    tx: &mut TxState,
    job: QueuedJob,
    built: Result<BuiltExpansion, ExecutionStatus>,
    outcome: &mut SchedulingOutcome,
) -> Vec<QueuedJob> {
    let run_id = job.run_id;
    let node_id = job.job_id;
    if !tx.expanding.remove(&(run_id, node_id.clone())) {
        return Vec::new();
    }
    let built = match built {
        Ok(built) => built,
        Err(status) => {
            fail_expansion_node(tx, run_id, &node_id, status, outcome);
            return Vec::new();
        }
    };
    match built {
        BuiltExpansion::Matrix { jobs } if jobs.is_empty() => {
            if let Some(run) = tx.runs.get_mut(&run_id) {
                run.jobs.insert(node_id.clone(), ExecutionStatus::Skipped);
                run.status = summarize_run(run.jobs.values().copied());
                finalize_run_if_complete(run);
            }
            release_concurrency_for_job(tx, run_id, &node_id);
            retire_node_requests(
                tx,
                run_id,
                &node_id,
                RequestRetirement::Settle(ExecutionStatus::Skipped),
            );
            outcome.skipped.push((run_id, node_id));
            Vec::new()
        }
        BuiltExpansion::Matrix { jobs } => {
            let queued = register_expanded_jobs(tx, run_id, jobs);
            if let Some(run) = tx.runs.get_mut(&run_id) {
                run.jobs.remove(&node_id);
                run.job_base_ids.remove(&node_id);
                run.job_needs.remove(&node_id);
                run.status = summarize_run(run.jobs.values().copied());
                let leg_ids: Vec<String> = queued.iter().map(|job| job.job_id.0.clone()).collect();
                if !leg_ids.is_empty() {
                    for meta in run.reusable_calls.values_mut() {
                        if let Some(pos) = meta.inner_job_ids.iter().position(|id| id == &node_id.0)
                        {
                            meta.inner_job_ids.splice(pos..pos + 1, leg_ids.clone());
                        }
                    }
                }
            }
            retire_node_requests(tx, run_id, &node_id, RequestRetirement::Purge);
            queued
        }
        BuiltExpansion::Reusable {
            caller_id,
            jobs,
            reusable_calls,
        } => {
            let inner_ids: Vec<String> = jobs.iter().map(|job| job.plan.id.0.clone()).collect();
            let queued = register_expanded_jobs(tx, run_id, jobs);
            if let Some(run) = tx.runs.get_mut(&run_id) {
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
