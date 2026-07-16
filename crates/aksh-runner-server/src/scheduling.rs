//! Pure job-graph scheduler model.
//!
//! Mirrors official GitHub Actions dispatch rules for `needs` and matrix
//! fan-out so property tests can exercise thousands of graphs without HTTP.
//!
//! Oracle sources (pinned where source code is used):
//! - Workflow contract: <https://docs.github.com/en/actions/writing-workflows/workflow-syntax-for-github-actions#jobsjob_idneeds>.
//! - Status functions: <https://docs.github.com/en/actions/learn-github-actions/expressions#status-check-functions>.
//! - Runner v2.335.1 condition implementation:
//!   `src/Runner.Worker/StepsRunner.cs`,
//!   `src/Runner.Worker/Expressions/SuccessFunction.cs`,
//!   `src/Runner.Worker/Expressions/FailureFunction.cs`,
//!   `src/Runner.Worker/Expressions/CancelledFunction.cs`, and
//!   `src/Runner.Worker/Expressions/AlwaysFunction.cs`.
//!
//! Properties below encode these observable rules:
//! - A dependent waits until every needed job is terminal.
//! - Failed or skipped dependencies skip a default dependent; explicit
//!   `always()`, `failure()`, or `cancelled()` conditions may override that.
//! - `failure()` observes failures in the transitive needs ancestry.
//! - Matrix siblings share a base id; needing `build` waits for every
//!   expanded `build (...)` sibling and respects `max-parallel`.
//! - Cycles and unknown needs are invalid and must be rejected before dispatch.
//! - A job is never dispatched twice; promotion is deterministic given a
//!   stable pending order; terminal statuses do not regress.
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use aksh_gha_protocol::{ExecutionStatus, JobId};

/// One schedulable job node after matrix expansion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedJob {
    /// Expanded job id (`build (ubuntu, 20)` or plain `build`).
    pub id: JobId,
    /// Workflow job id before matrix suffixing.
    pub base_id: String,
    /// Declared `needs` (base or expanded ids).
    pub needs: Vec<JobId>,
    /// Optional matrix max-parallel for this base id.
    pub max_parallel: Option<u64>,
    /// Job-level condition expression, if any.
    pub if_condition: Option<String>,
}

/// Mutable scheduler snapshot for one workflow run.
#[derive(Debug, Clone, Default)]
pub struct SchedulerState {
    /// Terminal/non-terminal status per expanded job id.
    pub jobs: BTreeMap<JobId, ExecutionStatus>,
    /// Expanded id → base id.
    pub job_base_ids: BTreeMap<JobId, String>,
    /// Expanded id → declared dependency ids.
    pub job_needs: BTreeMap<JobId, Vec<JobId>>,
    /// Jobs waiting on dependencies (stable insertion order).
    pub pending: VecDeque<SchedJob>,
    /// Jobs ready for runner acquisition (stable FIFO).
    pub queue: VecDeque<SchedJob>,
}

/// Re-export cycle detection from the parser crate (single source of truth).
pub use aksh_gha_parser::dag::detect_needs_cycle;

/// Whether a single `needs` entry completed successfully.
///
/// The need may name a concrete expanded id or a base id matching matrix
/// siblings. Every match must be successful and at least one match is required.
/// A skipped need does not satisfy GitHub's default dependency condition.
pub fn need_satisfied(jobs: &BTreeMap<JobId, ExecutionStatus>, need: &JobId) -> bool {
    let statuses = matching_need_statuses(jobs, need);
    !statuses.is_empty()
        && statuses
            .iter()
            .all(|status| *status == ExecutionStatus::Success)
}

fn matching_need_statuses(
    jobs: &BTreeMap<JobId, ExecutionStatus>,
    need: &JobId,
) -> Vec<ExecutionStatus> {
    let matrix_prefix = format!("{} (", need.0);
    jobs.iter()
        .filter_map(|(job_id, status)| {
            (job_id == need || job_id.0.starts_with(&matrix_prefix)).then_some(*status)
        })
        .collect()
}

fn matching_need_ids(state: &SchedulerState, need: &JobId) -> Vec<JobId> {
    state
        .jobs
        .keys()
        .filter(|job_id| {
            *job_id == need
                || state
                    .job_base_ids
                    .get(*job_id)
                    .is_some_and(|base| base == &need.0)
        })
        .cloned()
        .collect()
}

fn state_need_statuses(state: &SchedulerState, need: &JobId) -> Vec<ExecutionStatus> {
    matching_need_ids(state, need)
        .iter()
        .filter_map(|job_id| state.jobs.get(job_id).copied())
        .collect()
}

fn ancestor_statuses(state: &SchedulerState, job: &SchedJob) -> Vec<ExecutionStatus> {
    let mut pending = job
        .needs
        .iter()
        .flat_map(|need| matching_need_ids(state, need))
        .collect::<Vec<_>>();
    let mut visited = BTreeSet::new();
    let mut statuses = Vec::new();
    while let Some(job_id) = pending.pop() {
        if !visited.insert(job_id.clone()) {
            continue;
        }
        if let Some(status) = state.jobs.get(&job_id) {
            statuses.push(*status);
        }
        if let Some(needs) = state.job_needs.get(&job_id) {
            pending.extend(needs.iter().flat_map(|need| matching_need_ids(state, need)));
        }
    }
    statuses
}

fn aggregate_status(statuses: &[ExecutionStatus]) -> ExecutionStatus {
    if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Failure)
    {
        ExecutionStatus::Failure
    } else if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Cancelled)
    {
        ExecutionStatus::Cancelled
    } else if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Skipped)
    {
        ExecutionStatus::Skipped
    } else {
        ExecutionStatus::Success
    }
}

fn terminal(status: ExecutionStatus) -> bool {
    matches!(
        status,
        ExecutionStatus::Success
            | ExecutionStatus::Failure
            | ExecutionStatus::Skipped
            | ExecutionStatus::Cancelled
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DependencyDecision {
    Wait,
    Run,
    Skip,
    Error,
}

fn dependency_decision(state: &SchedulerState, job: &SchedJob) -> DependencyDecision {
    if job.needs.is_empty() {
        return DependencyDecision::Run;
    }
    let direct_statuses = job
        .needs
        .iter()
        .flat_map(|need| state_need_statuses(state, need))
        .collect::<Vec<_>>();
    if direct_statuses.is_empty() || direct_statuses.iter().any(|status| !terminal(*status)) {
        return DependencyDecision::Wait;
    }

    let statuses = ancestor_statuses(state, job);
    let aggregate = aggregate_status(&statuses);
    let success = aggregate == ExecutionStatus::Success;
    let failure = aggregate == ExecutionStatus::Failure;
    let cancelled = aggregate == ExecutionStatus::Cancelled;
    let condition = aksh_gha_expressions::effective_condition(job.if_condition.as_deref());
    let context = aksh_gha_expressions::Context::default().with_status(success, failure, cancelled);
    match aksh_gha_expressions::eval_bool(&condition, &context) {
        Ok(true) => DependencyDecision::Run,
        Ok(false) => DependencyDecision::Skip,
        Err(_) => DependencyDecision::Error,
    }
}

/// Whether `job` is under its matrix `max-parallel` budget.
pub fn under_max_parallel(state: &SchedulerState, job: &SchedJob) -> bool {
    let Some(max_parallel) = job.max_parallel else {
        return true;
    };
    let active_in_queue = state
        .queue
        .iter()
        .filter(|queued| queued.base_id == job.base_id)
        .count() as u64;
    let active_running = state
        .jobs
        .iter()
        .filter(|(job_id, status)| {
            state.job_base_ids.get(*job_id) == Some(&job.base_id)
                && matches!(status, ExecutionStatus::InProgress)
        })
        .count() as u64;

    active_in_queue + active_running < max_parallel
}

/// Resolve pending jobs whose dependencies have become terminal.
///
/// Runnable jobs enter the FIFO queue. Jobs whose condition is false become
/// terminal `Skipped`. The pass repeats after skips so downstream chains settle.
/// Returns the number of jobs promoted to the queue.
pub fn promote_ready_jobs(state: &mut SchedulerState) -> usize {
    let mut promoted_count = 0;
    loop {
        let mut promoted_by_base: BTreeMap<String, u64> = BTreeMap::new();
        let mut promoted = Vec::new();
        let mut remaining = VecDeque::new();
        let mut skipped = false;

        while let Some(job) = state.pending.pop_front() {
            match dependency_decision(state, &job) {
                DependencyDecision::Run
                    if under_max_parallel(state, &job)
                        && promoted_by_base.get(&job.base_id).copied().unwrap_or(0)
                            < job.max_parallel.unwrap_or(u64::MAX) =>
                {
                    *promoted_by_base.entry(job.base_id.clone()).or_default() += 1;
                    promoted.push(job)
                }
                DependencyDecision::Skip => {
                    state.jobs.insert(job.id, ExecutionStatus::Skipped);
                    skipped = true;
                }
                DependencyDecision::Error => {
                    state.jobs.insert(job.id, ExecutionStatus::Failure);
                    skipped = true;
                }
                DependencyDecision::Wait | DependencyDecision::Run => remaining.push_back(job),
            }
        }

        promoted_count += promoted.len();
        state.pending = remaining;
        state.queue.extend(promoted);
        if !skipped {
            return promoted_count;
        }
    }
}

/// Seed a scheduler from expanded job definitions.
///
/// Jobs with empty `needs` enter the queue immediately; others stay pending.
/// All jobs start as `Queued`.
pub fn seed_from_jobs(jobs: Vec<SchedJob>) -> SchedulerState {
    let mut state = SchedulerState::default();
    for job in jobs {
        if state.jobs.contains_key(&job.id) {
            continue;
        }
        state.jobs.insert(job.id.clone(), ExecutionStatus::Queued);
        state
            .job_base_ids
            .insert(job.id.clone(), job.base_id.clone());
        state.job_needs.insert(job.id.clone(), job.needs.clone());
        if job.needs.is_empty() {
            state.queue.push_back(job);
        } else {
            state.pending.push_back(job);
        }
    }
    state
}

/// Mark a job terminal and re-run promotion.
pub fn complete_job(state: &mut SchedulerState, job_id: &JobId, status: ExecutionStatus) -> usize {
    let Some(current) = state.jobs.get_mut(job_id) else {
        return 0;
    };
    if terminal(*current) {
        return 0;
    }
    *current = status;
    state.queue.retain(|job| &job.id != job_id);
    state.pending.retain(|job| &job.id != job_id);
    promote_ready_jobs(state)
}

/// Simulate dispatch: move the front queued job to `InProgress` and return it.
pub fn acquire_next(state: &mut SchedulerState) -> Option<SchedJob> {
    let job = state.queue.pop_front()?;
    if let Some(slot) = state.jobs.get_mut(&job.id) {
        *slot = ExecutionStatus::InProgress;
    }
    Some(job)
}

/// True when every job is in a terminal status.
pub fn run_settled(state: &SchedulerState) -> bool {
    state.jobs.values().all(|s| {
        matches!(
            s,
            ExecutionStatus::Success
                | ExecutionStatus::Failure
                | ExecutionStatus::Skipped
                | ExecutionStatus::Cancelled
        )
    }) && state.pending.is_empty()
        && state.queue.is_empty()
}

/// Jobs currently `InProgress`.
pub fn in_progress_ids(state: &SchedulerState) -> BTreeSet<JobId> {
    state
        .jobs
        .iter()
        .filter_map(|(id, status)| {
            if matches!(status, ExecutionStatus::InProgress) {
                Some(id.clone())
            } else {
                None
            }
        })
        .collect()
}

/// Count how many times a job id appears across queue + pending (must be ≤ 1).
pub fn placement_count(state: &SchedulerState, job_id: &JobId) -> usize {
    state.queue.iter().filter(|j| &j.id == job_id).count()
        + state.pending.iter().filter(|j| &j.id == job_id).count()
}

#[cfg(test)]
#[path = "scheduling_tests.rs"]
mod tests;
