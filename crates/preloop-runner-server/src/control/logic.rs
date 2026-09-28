//! Backend-neutral scheduling decisions.
//!
//! Pure functions over small row structs: no I/O, no locks, no working set,
//! plus the expansion-build machinery that produces [`BuiltExpansion`] for
//! `apply_expansion`. Both backends call these so they cannot diverge.

use crate::models::{QueuedJob, RunRecord};
use crate::state::JobSetGate;
use preloop_gha_protocol::azdo;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId, WorkflowSubmission};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

// Pure helpers shared with the runtime scheduler.
pub(crate) use crate::runtime_scheduling::{
    aggregate_need_status, matching_need_ids, matching_need_statuses, need_context,
    needs_json_context, DependencyDecision, SchedulingOutcome,
};

/// Namespace used to deterministically encode legacy non-UUID session ids.
pub(crate) const SESSION_ID_NAMESPACE: uuid::Uuid =
    uuid::uuid!("7d3c0a1c-6f4a-4d4c-a5d4-8d27b5a3c1f0");

/// Convert a session id to its stored UUID, using UUID v5 for legacy ids.
pub(crate) fn session_uuid(id: &str) -> uuid::Uuid {
    id.parse()
        .unwrap_or_else(|_| uuid::Uuid::new_v5(&SESSION_ID_NAMESPACE, id.as_bytes()))
}

/// How long an unmatched ready job may wait for a matching runner.
pub(crate) const QUEUED_JOB_GRACE: Duration = Duration::from_secs(120);
/// Absolute queue-age ceiling while a pool is preparing.
pub(crate) const MAX_QUEUED_GRACE: Duration = Duration::from_secs(600);

/// Ready-job inputs for starvation evaluation.
#[derive(Debug, Clone)]
pub(crate) struct StarvationCandidate<'a> {
    pub(crate) runs_on: &'a [String],
    pub(crate) enqueued_at: SystemTime,
    pub(crate) first_seen: Option<SystemTime>,
    pub(crate) any_runner_matches: bool,
}

/// Starvation outcome: clear, mark, or fail the ready job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StarvationVerdict {
    ClearMark,
    Mark { first_seen: SystemTime },
    Starve { reason: String, grace: Duration },
}

/// Decide starvation using the production 120/600 second grace rules.
pub(crate) fn starvation_verdict(
    job: &StarvationCandidate<'_>,
    now: SystemTime,
    pool_preparing: bool,
    warm_window_open: bool,
) -> StarvationVerdict {
    if job.any_runner_matches
        || job.runs_on.iter().any(|label| {
            let label = label.to_ascii_lowercase();
            label.starts_with("macos") || label.starts_with("windows")
        })
    {
        return StarvationVerdict::ClearMark;
    }
    let grace = if pool_preparing {
        let expired = now
            .duration_since(job.enqueued_at)
            .map(|age| age >= MAX_QUEUED_GRACE)
            .unwrap_or(true);
        if warm_window_open || !expired {
            return StarvationVerdict::ClearMark;
        }
        MAX_QUEUED_GRACE
    } else {
        let first_seen = job.first_seen.unwrap_or(job.enqueued_at);
        if now
            .duration_since(first_seen)
            .map(|age| age < QUEUED_JOB_GRACE)
            .unwrap_or(false)
        {
            return StarvationVerdict::Mark { first_seen };
        }
        QUEUED_JOB_GRACE
    };
    StarvationVerdict::Starve {
        reason: format!(
            "no runner is registered for `runs-on: {}` and none appeared within {}s, so the job cannot be scheduled",
            job.runs_on.join(", "),
            grace.as_secs()
        ),
        grace,
    }
}

/// Backend-neutral runner capabilities needed by dispatch matching.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RunnerMatchRow {
    /// Labels reported by the runner; matching is ASCII-case-insensitive.
    pub(crate) labels: Vec<String>,
    /// Whether this row identifies a registered runner. Unknown/compat
    /// runners have permissive labels but never satisfy an explicit group.
    pub(crate) known: bool,
    pub(crate) group_id: Option<i64>,
    pub(crate) group_name: Option<String>,
}

/// Return whether every job label is compatible with a runner's labels.
///
/// Empty job labels and unknown runner labels match everything. Exact labels
/// match case-insensitively. Hosted OS labels (`ubuntu-*`, `macos-*`,
/// `windows-*`) match a runner's corresponding `linux`, `macos`, or `windows`
/// OS label; a known runner with no OS label only matches such a label when it
/// is `self-hosted`.
pub(crate) fn runner_labels_match(job_labels: &[String], runner_labels: &[String]) -> bool {
    if job_labels.is_empty() || runner_labels.is_empty() {
        return true;
    }
    let set: HashSet<String> = runner_labels
        .iter()
        .map(|v| v.to_ascii_lowercase())
        .collect();
    let os = ["linux", "macos", "windows"]
        .into_iter()
        .find(|value| set.contains(*value));
    job_labels.iter().all(|required| {
        let required = required.to_ascii_lowercase();
        if set.contains(&required) {
            return true;
        }
        let required_os = if required.starts_with("ubuntu-") || required.starts_with("linux-") {
            Some("linux")
        } else if required.starts_with("macos-") || required.starts_with("osx-") {
            Some("macos")
        } else if required.starts_with("windows-") {
            Some("windows")
        } else {
            None
        };
        match (required_os, os) {
            (Some(required), Some(actual)) => required == actual,
            (Some(_), None) => set.contains("self-hosted"),
            (None, _) => false,
        }
    })
}

/// Match a required runner group. Numeric requirements match group ids;
/// omitted group metadata means the default group id/name (`1`/`Default`).
pub(crate) fn runner_group_matches(required: Option<&str>, runner: &RunnerMatchRow) -> bool {
    let Some(required) = required.map(str::trim).filter(|v| !v.is_empty()) else {
        return true;
    };
    if !runner.known {
        return false;
    }
    if let Ok(id) = required.parse::<i64>() {
        return runner.group_id == Some(id)
            || (runner.group_id.is_none() && runner.group_name.is_none() && id == 1);
    }
    match (&runner.group_id, &runner.group_name) {
        (Some(id), Some(name)) if *id != 1 => name.eq_ignore_ascii_case(required),
        (_, Some(name)) => name.eq_ignore_ascii_case(required),
        (None, None) | (Some(1), None) => "Default".eq_ignore_ascii_case(required),
        (Some(_), None) => false,
    }
}

/// Match both labels and the explicit runner group.
pub(crate) fn runner_matches(
    job_labels: &[String],
    required_group: Option<&str>,
    runner: &RunnerMatchRow,
) -> bool {
    runner_labels_match(job_labels, &runner.labels) && runner_group_matches(required_group, runner)
}

/// Candidate row used by claim preference ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClaimCandidate {
    pub(crate) run_id: RunId,
    pub(crate) job_id: JobId,
    pub(crate) runs_on: Vec<String>,
    pub(crate) runner_group: Option<String>,
    pub(crate) assigned_runner_id: Option<i64>,
    pub(crate) assignment_fresh: bool,
    pub(crate) queue_position: u64,
    pub(crate) claimable: bool,
}

/// Every required label present
/// verbatim (case-insensitive) on the runner. The capability matcher's OS
/// fallbacks do NOT count — tier three exists to prefer true matches.
pub(crate) fn job_labels_covered_exactly(job_labels: &[String], runner_labels: &[String]) -> bool {
    if job_labels.is_empty() {
        return true;
    }
    if runner_labels.is_empty() {
        return false;
    }
    let runner_set: HashSet<String> = runner_labels.iter().map(|l| l.to_lowercase()).collect();
    job_labels
        .iter()
        .all(|required| runner_set.contains(&required.to_lowercase()))
}

/// Select the candidate index using the production four-tier preference:
/// fresh assignment to this runner with exact labels, assignment to this
/// runner, exact labels, then any claimable candidate. Relative order within
/// a tier is the supplied queue order; `None` means no candidate is eligible.
pub(crate) fn claim_preference(
    candidates: &[ClaimCandidate],
    runner_id: Option<i64>,
    runner_labels: &[String],
    required_group: Option<&str>,
    runner: &RunnerMatchRow,
) -> Option<usize> {
    // "Exact" is the strict subset match (every job label present verbatim
    // on the runner) — NOT the capability matcher, which admits the
    // ubuntu-* -> os-name fallback. `assigned_to_this_runner` in the old
    // code paired with the exact-label check, never freshness.
    let exact = |candidate: &ClaimCandidate| {
        job_labels_covered_exactly(&candidate.runs_on, runner_labels)
            && runner_group_matches(candidate.runner_group.as_deref().or(required_group), runner)
    };
    let assigned = |candidate: &ClaimCandidate| {
        runner_id.is_some()
            && candidate.assigned_runner_id == runner_id
            && candidate.assignment_fresh
    };
    let eligible = |candidate: &ClaimCandidate| {
        candidate.claimable
            && runner_matches(
                &candidate.runs_on,
                candidate.runner_group.as_deref(),
                runner,
            )
    };
    candidates
        .iter()
        .position(|candidate| eligible(candidate) && assigned(candidate) && exact(candidate))
        .or_else(|| {
            candidates
                .iter()
                .position(|candidate| eligible(candidate) && assigned(candidate))
        })
        .or_else(|| {
            candidates
                .iter()
                .position(|candidate| eligible(candidate) && exact(candidate))
        })
        .or_else(|| candidates.iter().position(eligible))
}
/// Aggregate dependency status with GitHub's precedence: failure, cancelled,
/// skipped, all-success. `None` means the set is empty or *any* dependency is
/// still non-terminal — a job's needs are undecided until every declared
/// dependency reaches a terminal state, so a failure among unfinished needs
/// does not conclude the aggregation early.
pub(crate) fn aggregate_needs_status(statuses: &[ExecutionStatus]) -> Option<ExecutionStatus> {
    if statuses.is_empty() || statuses.iter().any(|status| !status.is_terminal()) {
        return None;
    }
    // Every status is terminal here; precedence decides the verdict.
    if statuses.contains(&ExecutionStatus::Failure) {
        Some(ExecutionStatus::Failure)
    } else if statuses.contains(&ExecutionStatus::Cancelled) {
        Some(ExecutionStatus::Cancelled)
    } else if statuses.contains(&ExecutionStatus::Skipped) {
        Some(ExecutionStatus::Skipped)
    } else if statuses
        .iter()
        .all(|status| *status == ExecutionStatus::Success)
    {
        Some(ExecutionStatus::Success)
    } else {
        // Terminal but none of the four — unreachable for the closed
        // ExecutionStatus set; keep the total function honest.
        None
    }
}

/// Build the JSON `needs` context sent to a runner. Each key contains its
/// aggregate result and merged outputs; non-terminal dependencies are omitted.
pub(crate) fn needs_context(needs: &[NeedRow]) -> serde_json::Value {
    let values = needs
        .iter()
        .filter_map(|need| {
            let result = aggregate_needs_status(&[need.status])?;
            let result = match result {
                ExecutionStatus::Success => "success",
                ExecutionStatus::Failure => "failure",
                ExecutionStatus::Cancelled => "cancelled",
                ExecutionStatus::Skipped => "skipped",
                _ => "unknown",
            };
            Some((
                need.job_id.0.clone(),
                serde_json::json!({"result": result, "outputs": need.outputs}),
            ))
        })
        .collect::<serde_json::Map<_, _>>();
    serde_json::Value::Object(values)
}

/// Status and outputs of one declared `needs` dependency.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NeedRow {
    pub(crate) job_id: JobId,
    pub(crate) status: ExecutionStatus,
    pub(crate) outputs: BTreeMap<String, serde_json::Value>,
}

/// Decision after all direct needs are inspected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NeedsDecision {
    /// At least one direct dependency is non-terminal or missing.
    Wait,
    /// Dependencies settled and the condition is true.
    Promote,
    /// Dependencies settled and the condition is false.
    Skip,
    /// Condition evaluation failed.
    Error,
}

/// Minimal condition context for a needs decision. `Always` corresponds to
/// GitHub's implicit `success()` default; the other variants are the common
/// backend-evaluable predicates. Backends may evaluate richer expressions and
/// pass the resulting boolean through [`needs_decision_from_bool`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NeedsCondition {
    DefaultSuccess,
    Always,
    AnyFailure,
    AnyCancelled,
    Explicit(bool),
}

/// Decide promotion once the backend has loaded direct needs and their
/// statuses. Missing/non-terminal dependencies wait; terminal dependencies
/// use the supplied condition and never mutate rows.
pub(crate) fn needs_decision(needs: &[NeedRow], condition: NeedsCondition) -> NeedsDecision {
    if needs.is_empty() {
        return needs_decision_from_bool(condition, true);
    }
    if needs.iter().any(|need| !need.status.is_terminal()) {
        return NeedsDecision::Wait;
    }
    let success = needs
        .iter()
        .all(|need| need.status == ExecutionStatus::Success);
    let failure = needs
        .iter()
        .any(|need| need.status == ExecutionStatus::Failure);
    let cancelled = needs
        .iter()
        .any(|need| need.status == ExecutionStatus::Cancelled);
    let value = match condition {
        NeedsCondition::DefaultSuccess => success,
        NeedsCondition::Always => true,
        NeedsCondition::AnyFailure => failure,
        NeedsCondition::AnyCancelled => cancelled,
        NeedsCondition::Explicit(value) => value,
    };
    needs_decision_from_bool(condition, value)
}

/// Convert a pre-evaluated condition into the promotion/skip/error result.
pub(crate) fn needs_decision_from_bool(condition: NeedsCondition, value: bool) -> NeedsDecision {
    match condition {
        NeedsCondition::Explicit(_) if !value => NeedsDecision::Skip,
        _ if value => NeedsDecision::Promote,
        NeedsCondition::DefaultSuccess
        | NeedsCondition::Always
        | NeedsCondition::AnyFailure
        | NeedsCondition::AnyCancelled => NeedsDecision::Skip,
        NeedsCondition::Explicit(_) => NeedsDecision::Skip,
    }
}

/// Decrement remaining needs for a dependent row, clamping at zero. The
/// returned boolean is true exactly when the row becomes promotable.
pub(crate) fn decrement_remaining_needs(remaining: i32, settled_dependency: bool) -> (i32, bool) {
    let next = if settled_dependency {
        remaining.saturating_sub(1)
    } else {
        remaining
    };
    (next, settled_dependency && next == 0)
}

/// Matrix leg state needed by max-parallel admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MatrixLegRow {
    pub(crate) job_id: JobId,
    pub(crate) status: ExecutionStatus,
    pub(crate) queue_state: QueueState,
    pub(crate) base_id: String,
    pub(crate) order: u64,
}

/// Admit pending matrix legs in stable order until `max_parallel` active
/// legs (queued, claimed, or in progress) exist. A missing limit admits all.
pub(crate) fn max_parallel_admission(
    legs: &[MatrixLegRow],
    max_parallel: Option<u64>,
) -> Vec<JobId> {
    let Some(limit) = max_parallel else {
        return legs
            .iter()
            .filter(|leg| leg.queue_state == QueueState::Blocked)
            .map(|leg| leg.job_id.clone())
            .collect();
    };
    let active = legs
        .iter()
        .filter(|leg| {
            matches!(leg.status, ExecutionStatus::InProgress)
                || matches!(leg.queue_state, QueueState::Ready | QueueState::Claimed)
        })
        .count() as u64;
    let available = limit.saturating_sub(active);
    let mut selected = Vec::new();
    for leg in legs
        .iter()
        .filter(|leg| leg.queue_state == QueueState::Blocked)
    {
        if (selected.len() as u64) >= available {
            break;
        }
        selected.push(leg.job_id.clone());
    }
    selected
}

/// Matrix sibling row used by fail-fast.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FailFastSibling {
    pub(crate) job_id: JobId,
    pub(crate) base_id: String,
    pub(crate) status: ExecutionStatus,
    pub(crate) agent_job_id: Option<uuid::Uuid>,
}

/// Select queued/pending/in-progress siblings of a failed leg when the
/// matrix's `fail-fast` flag is enabled. Returned ids preserve row order;
/// the backend conditionally updates each selected row and queues cancellation
/// only for in-progress rows.
pub(crate) fn matrix_fail_fast(
    failed_job_id: &JobId,
    failed_base_id: Option<&str>,
    fail_fast: bool,
    siblings: &[FailFastSibling],
) -> Vec<JobId> {
    if !fail_fast {
        return Vec::new();
    }
    let Some(base_id) = failed_base_id else {
        return Vec::new();
    };
    siblings
        .iter()
        .filter(|sibling| {
            sibling.job_id != *failed_job_id
                && sibling.base_id == base_id
                && matches!(
                    sibling.status,
                    ExecutionStatus::Queued
                        | ExecutionStatus::Pending
                        | ExecutionStatus::InProgress
                )
        })
        .map(|sibling| sibling.job_id.clone())
        .collect()
}

/// A reusable caller's completed callee output.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReusableOutputRow {
    pub(crate) name: String,
    pub(crate) value: serde_json::Value,
}

/// Result of reusable-workflow output propagation. `finalize_caller` is true
/// when every declared callee output is now available and the caller may be
/// settled/promoted by the backend.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReusableOutputDecision {
    pub(crate) outputs: BTreeMap<String, serde_json::Value>,
    pub(crate) finalize_caller: bool,
}

/// Merge callee outputs into a caller's output map. Missing values are not
/// invented; existing values are replaced only by the same named output.
pub(crate) fn reusable_output_decision(
    declared_names: &[String],
    completed: &[ReusableOutputRow],
) -> ReusableOutputDecision {
    let mut outputs = BTreeMap::new();
    for row in completed {
        if declared_names.is_empty() || declared_names.iter().any(|name| name == &row.name) {
            outputs.insert(row.name.clone(), row.value.clone());
        }
    }
    let finalize_caller = declared_names.iter().all(|name| outputs.contains_key(name));
    ReusableOutputDecision {
        outputs,
        finalize_caller,
    }
}
/// One completed callee job for reusable-caller output evaluation.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReusableInnerRow {
    pub(crate) job_id: String,
    pub(crate) status: ExecutionStatus,
    pub(crate) outputs: BTreeMap<String, serde_json::Value>,
}

/// Immutable reusable caller inputs. Output definitions use workflow-call
/// output expression syntax.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReusableCallerRow {
    pub(crate) caller_id: JobId,
    pub(crate) inner: Vec<ReusableInnerRow>,
    pub(crate) output_definitions: BTreeMap<String, String>,
    pub(crate) inputs: BTreeMap<String, serde_json::Value>,
}

/// Evaluate reusable-workflow output definitions and aggregate callee status.
/// The backend applies outputs and status only when `finalize_caller` is true.
pub(crate) fn reusable_caller_decision(
    row: &ReusableCallerRow,
) -> (ReusableOutputDecision, ExecutionStatus) {
    let all_complete =
        !row.inner.is_empty() && row.inner.iter().all(|inner| inner.status.is_terminal());
    if !all_complete {
        return (
            ReusableOutputDecision {
                outputs: BTreeMap::new(),
                finalize_caller: false,
            },
            ExecutionStatus::InProgress,
        );
    }
    let mut jobs = serde_json::Map::new();
    for inner in &row.inner {
        let outputs = inner
            .outputs
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<serde_json::Map<_, _>>();
        jobs.insert(
            inner.job_id.clone(),
            serde_json::json!({"outputs": serde_json::Value::Object(outputs)}),
        );
    }
    let mut context = preloop_gha_expressions::Context::default();
    context.insert("jobs", serde_json::Value::Object(jobs));
    context.insert(
        "inputs",
        serde_json::Value::Object(row.inputs.clone().into_iter().collect()),
    );
    let outputs = row
        .output_definitions
        .iter()
        .map(|(name, expression)| {
            let value = preloop_gha_parser::eval::resolve_string(expression, &context)
                .unwrap_or_else(|_| expression.clone());
            (name.clone(), serde_json::Value::String(value))
        })
        .collect();
    let statuses = row
        .inner
        .iter()
        .map(|inner| inner.status)
        .collect::<Vec<_>>();
    let status = aggregate_needs_status(&statuses).unwrap_or(ExecutionStatus::Skipped);
    (
        ReusableOutputDecision {
            outputs,
            finalize_caller: true,
        },
        status,
    )
}

/// A concurrency holder/waiter identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConcurrencyRow {
    pub(crate) group: String,
    pub(crate) run_id: RunId,
    pub(crate) job_id: Option<JobId>,
    pub(crate) wait_id: u64,
    pub(crate) cancel_in_progress: bool,
}

/// Result of attempting to insert a concurrency hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ConcurrencyAdmission {
    Acquired,
    Waiting,
    CancelCurrent(ConcurrencyRow),
    Cancelled,
}

/// Decide hold admission. The backend performs the insert; this function
/// decides the outcome from the existing holder, FIFO waiters, and the new
/// arrival's cancel-in-progress flag.
///
/// Same-run rules (matching `logic::try_acquire_concurrency`): a different job
/// of the *same run* never grants admission on its own — with
/// `cancel_in_progress` unset it waits behind the group like any other
/// arrival. The same-run exclusion only suppresses *cancellation*: when the
/// arrival does displace a holder (cancel-in-progress), a holder belonging to
/// the same run is replaced without being cancelled.
pub(crate) fn concurrency_admission(
    arrival: &ConcurrencyRow,
    holder: Option<&ConcurrencyRow>,
) -> ConcurrencyAdmission {
    let Some(current) = holder else {
        return ConcurrencyAdmission::Acquired;
    };
    if arrival.cancel_in_progress {
        if current.run_id == arrival.run_id {
            // Same run already holds the group: the new job takes over the hold
            // with no cancellation emitted (the old holder is this same run).
            return ConcurrencyAdmission::Acquired;
        }
        return ConcurrencyAdmission::CancelCurrent(current.clone());
    }
    // No holder-free path and no cancel-in-progress: park regardless of
    // whether a same-run waiter already exists — FIFO order decides.
    ConcurrencyAdmission::Waiting
}

/// Promote the oldest waiter after a hold is released. FIFO is by `wait_id`;
/// the backend deletes the selected wait row and inserts its hold in the same
/// SQL transaction. `None` means no waiter remains.
pub(crate) fn concurrency_fifo_promotion(waiters: &[ConcurrencyRow]) -> Option<ConcurrencyRow> {
    waiters.iter().min_by_key(|waiter| waiter.wait_id).cloned()
}
/// Queue policy for a contended concurrency group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConcurrencyQueueMode {
    /// New arrival replaces all existing waiters and waits.
    Single,
    /// New arrival waits until the bounded pending queue is full, then is
    /// cancelled instead.
    Max,
}

/// Pure queue-mode result; the backend conditionally settles
/// `cancel_pending`, `cancel_arrival`, and inserts the arrival when parked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConcurrencyQueueDecision {
    pub(crate) cancel_pending: Vec<ConcurrencyRow>,
    pub(crate) cancel_arrival: bool,
    pub(crate) park_arrival: bool,
}

/// Maximum pending holders for `queue: max`.
pub(crate) const MAX_CONCURRENCY_WAITERS: usize = 100;

/// Apply GitHub's FIFO queue policy to existing waiters.
pub(crate) fn concurrency_queue_decision(
    mode: ConcurrencyQueueMode,
    existing: &[ConcurrencyRow],
) -> ConcurrencyQueueDecision {
    match mode {
        ConcurrencyQueueMode::Single => ConcurrencyQueueDecision {
            cancel_pending: existing.to_vec(),
            cancel_arrival: false,
            park_arrival: true,
        },
        ConcurrencyQueueMode::Max if existing.len() >= MAX_CONCURRENCY_WAITERS => {
            ConcurrencyQueueDecision {
                cancel_pending: Vec::new(),
                cancel_arrival: true,
                park_arrival: false,
            }
        }
        ConcurrencyQueueMode::Max => ConcurrencyQueueDecision {
            cancel_pending: Vec::new(),
            cancel_arrival: false,
            park_arrival: true,
        },
    }
}

/// Queue-state values in the agreed v1 schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueState {
    None,
    Ready,
    Claimed,
    Blocked,
    Held,
    PendingExpansion,
    Expanding,
}

/// Inputs needed to map a job's status and scheduling waits to `jobs.queue_state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueueStateInput {
    pub(crate) status: ExecutionStatus,
    pub(crate) remaining_needs: i32,
    pub(crate) ready: bool,
    pub(crate) claimed: bool,
    /// A row in `concurrency_waits`, regardless of whether its holder is a
    /// workflow/run or a job/jobset.  The holder kind is used by queue stats,
    /// not by the queue-state string.
    pub(crate) concurrency_wait: bool,
    /// Waiting for a max-parallel slot.  This is distinct from dependency
    /// blocking because both map to `blocked`, while concurrency maps to
    /// `held`.
    pub(crate) max_parallel_wait: bool,
    pub(crate) pending_expansion: bool,
    pub(crate) expanding: bool,
}

/// Apply decisions-5 queue mapping exactly:
/// needs/max-parallel become `blocked`; any concurrency wait becomes `held`.
/// The holder kind distinguishes workflow-level from job/jobset-level waits
/// in queue statistics. Expansion states are fenced before ordinary queues.
pub(crate) fn queue_state(input: QueueStateInput) -> QueueState {
    if input.status.is_terminal() {
        return QueueState::None;
    }
    if input.expanding {
        return QueueState::Expanding;
    }
    if input.pending_expansion {
        return QueueState::PendingExpansion;
    }
    if input.concurrency_wait {
        return QueueState::Held;
    }
    if input.claimed {
        return QueueState::Claimed;
    }
    if input.ready {
        return QueueState::Ready;
    }
    if input.remaining_needs > 0 || input.max_parallel_wait {
        return QueueState::Blocked;
    }
    QueueState::None
}

/// Expansion build result supplied by an expander worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ExpansionFence {
    pub(crate) expected_generation: i32,
    pub(crate) current_generation: i32,
}

/// Whether an expansion result may be applied and what queue state follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExpansionDecision {
    Stale,
    Failed(ExecutionStatus),
    Empty(ExecutionStatus),
    Apply,
}

/// Fence an expansion result: stale generations are discarded; failed builds
/// settle the expansion node as failure; an empty successful matrix settles it
/// skipped; non-empty success applies its materialized jobs.
pub(crate) fn expansion_decision(
    fence: ExpansionFence,
    build_status: Result<usize, ExecutionStatus>,
) -> ExpansionDecision {
    if fence.expected_generation != fence.current_generation {
        return ExpansionDecision::Stale;
    }
    match build_status {
        Err(status) => ExpansionDecision::Failed(status),
        Ok(0) => ExpansionDecision::Empty(ExecutionStatus::Skipped),
        Ok(_) => ExpansionDecision::Apply,
    }
}

/// Completion row used by pure settlement decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompletionRow {
    pub(crate) job_id: JobId,
    pub(crate) prior_status: ExecutionStatus,
    pub(crate) reported_status: ExecutionStatus,
    pub(crate) continue_on_error: bool,
    pub(crate) run_will_be_terminal: bool,
}

/// Pure result of settling one completion. The backend applies the returned
/// status conditionally, then uses `promote_dependents` to update rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompletionDecision {
    pub(crate) effective_status: ExecutionStatus,
    pub(crate) replayed: bool,
    pub(crate) release_workflow_hold: bool,
    pub(crate) promote_dependents: bool,
}

/// Decide first-result-wins, continue-on-error, workflow-hold release, and
/// dependent promotion. Cancelled jobs remain cancelled against late success
/// or failure reports.
pub(crate) fn completion_decision(row: CompletionRow) -> CompletionDecision {
    let replayed = row.prior_status.is_terminal() && row.prior_status != ExecutionStatus::Cancelled;
    let reported = if row.continue_on_error && row.reported_status == ExecutionStatus::Failure {
        ExecutionStatus::Success
    } else {
        row.reported_status
    };
    let effective_status = match (row.prior_status, reported) {
        (ExecutionStatus::Cancelled, ExecutionStatus::Success | ExecutionStatus::Failure) => {
            ExecutionStatus::Cancelled
        }
        _ if replayed => row.prior_status,
        _ => reported,
    };
    CompletionDecision {
        effective_status,
        replayed,
        release_workflow_hold: row.run_will_be_terminal,
        promote_dependents: !replayed && effective_status.is_terminal(),
    }
}

/// Dependent rows that become eligible after a completed job. A row is
/// promotable only when every listed need is terminal and the needs decision
/// is `Promote`; skipped/error rows are returned separately for settlement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DependentPromotion {
    pub(crate) job_id: JobId,
    pub(crate) decision: NeedsDecision,
}

/// Decide each dependent's next action without mutating any backend row.
pub(crate) fn dependent_promotions(
    dependents: &[(JobId, Vec<NeedRow>, NeedsCondition)],
) -> Vec<DependentPromotion> {
    dependents
        .iter()
        .map(|(job_id, needs, condition)| DependentPromotion {
            job_id: job_id.clone(),
            decision: needs_decision(needs, *condition),
        })
        .collect()
}

/// Coarse `(queued, in_progress, completed)` run counts for status: `Queued`
/// is queued, `Pending` (held) and `InProgress` are in progress, every
/// terminal status is completed.
pub(crate) fn count_run_statuses(
    statuses: impl IntoIterator<Item = ExecutionStatus>,
) -> (u32, u32, u32) {
    let mut counts = (0, 0, 0);
    for status in statuses {
        match status {
            ExecutionStatus::Queued => counts.0 += 1,
            ExecutionStatus::Pending | ExecutionStatus::InProgress => counts.1 += 1,
            ExecutionStatus::Success
            | ExecutionStatus::Failure
            | ExecutionStatus::Skipped
            | ExecutionStatus::Cancelled => counts.2 += 1,
        }
    }
    counts
}

#[cfg(test)]
mod decision_tests {
    use super::*;

    fn jid(value: &str) -> JobId {
        JobId(value.to_owned())
    }
    fn rid(value: u128) -> RunId {
        RunId(uuid::Uuid::from_u128(value))
    }
    fn labels(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn pending_runs_count_as_in_progress() {
        let counts = count_run_statuses([
            ExecutionStatus::Queued,
            ExecutionStatus::Pending,
            ExecutionStatus::InProgress,
            ExecutionStatus::Success,
            ExecutionStatus::Failure,
            ExecutionStatus::Skipped,
            ExecutionStatus::Cancelled,
        ]);
        assert_eq!(counts, (1, 2, 4));
    }

    fn starvation_candidate<'a>(
        runs_on: &'a [String],
        enqueued_ago: Duration,
        first_seen_ago: Option<Duration>,
        now: SystemTime,
    ) -> StarvationCandidate<'a> {
        StarvationCandidate {
            runs_on,
            enqueued_at: now - enqueued_ago,
            first_seen: first_seen_ago.map(|ago| now - ago),
            any_runner_matches: false,
        }
    }

    #[test]
    fn matched_or_external_host_jobs_never_starve() {
        let now = SystemTime::now();
        let linux = labels(&["ubuntu-latest"]);
        let mut job = starvation_candidate(&linux, Duration::from_secs(10_000), None, now);
        job.any_runner_matches = true;
        assert_eq!(
            starvation_verdict(&job, now, false, false),
            StarvationVerdict::ClearMark
        );
        let mac = labels(&["macOS-14"]);
        let job = starvation_candidate(&mac, Duration::from_secs(10_000), None, now);
        assert_eq!(
            starvation_verdict(&job, now, false, false),
            StarvationVerdict::ClearMark
        );
    }

    #[test]
    fn starvation_grace_uses_first_seen_or_enqueue_time() {
        let now = SystemTime::now();
        let linux = labels(&["ubuntu-latest"]);
        let job = starvation_candidate(
            &linux,
            Duration::from_secs(1_000),
            Some(Duration::from_secs(30)),
            now,
        );
        assert_eq!(
            starvation_verdict(&job, now, false, false),
            StarvationVerdict::Mark {
                first_seen: now - Duration::from_secs(30)
            }
        );
        let job = starvation_candidate(&linux, Duration::from_secs(121), None, now);
        let StarvationVerdict::Starve { reason, grace } =
            starvation_verdict(&job, now, false, false)
        else {
            panic!("an unmatched job past the grace window must starve");
        };
        assert_eq!(grace, QUEUED_JOB_GRACE);
        assert!(reason.contains("within 120s"));
    }

    #[test]
    fn preparing_pool_protects_until_enqueue_ceiling() {
        let now = SystemTime::now();
        let linux = labels(&["self-hosted", "linux"]);
        let young = starvation_candidate(&linux, Duration::from_secs(599), None, now);
        assert_eq!(
            starvation_verdict(&young, now, true, false),
            StarvationVerdict::ClearMark
        );
        let old = starvation_candidate(&linux, Duration::from_secs(600), None, now);
        assert!(matches!(
            starvation_verdict(&old, now, true, false),
            StarvationVerdict::Starve { grace, .. } if grace == MAX_QUEUED_GRACE
        ));
        assert_eq!(
            starvation_verdict(&old, now, true, true),
            StarvationVerdict::ClearMark
        );
    }

    #[test]
    fn runner_matching_handles_hosted_os_and_groups() {
        let runner = RunnerMatchRow {
            labels: vec!["self-hosted".into(), "Linux".into(), "arm64".into()],
            known: true,
            group_id: None,
            group_name: None,
        };
        assert!(runner_matches(
            &["ubuntu-latest".into(), "arm64".into()],
            None,
            &runner
        ));
        assert!(!runner_matches(&["windows-latest".into()], None, &runner));
        assert!(runner_group_matches(Some("Default"), &runner));
    }

    #[test]
    fn claim_preference_is_four_tiered() {
        let runner = RunnerMatchRow {
            labels: vec!["linux".into()],
            known: true,
            ..Default::default()
        };
        let rows = vec![
            ClaimCandidate {
                run_id: rid(1),
                job_id: jid("generic"),
                runs_on: vec!["linux".into()],
                runner_group: None,
                assigned_runner_id: None,
                assignment_fresh: false,
                queue_position: 0,
                claimable: true,
            },
            ClaimCandidate {
                run_id: rid(1),
                job_id: jid("assigned"),
                runs_on: vec!["linux".into()],
                runner_group: None,
                assigned_runner_id: Some(7),
                assignment_fresh: true,
                queue_position: 1,
                claimable: true,
            },
        ];
        assert_eq!(
            claim_preference(&rows, Some(7), &["linux".into()], None, &runner),
            Some(1)
        );
    }

    #[test]
    fn needs_context_and_reusable_outputs_are_backend_neutral() {
        let mut outputs = BTreeMap::new();
        outputs.insert("answer".to_owned(), serde_json::json!("42"));
        let need = NeedRow {
            job_id: jid("build"),
            status: ExecutionStatus::Success,
            outputs: outputs.clone(),
        };
        let context = needs_context(&[need]);
        assert_eq!(context["build"]["result"], "success");
        assert_eq!(context["build"]["outputs"]["answer"], "42");
        let caller = ReusableCallerRow {
            caller_id: jid("call"),
            inner: vec![ReusableInnerRow {
                job_id: "build".to_owned(),
                status: ExecutionStatus::Success,
                outputs,
            }],
            output_definitions: BTreeMap::from([(
                "answer".to_owned(),
                "${{ jobs.build.outputs.answer }}".to_owned(),
            )]),
            inputs: BTreeMap::new(),
        };
        let (decision, status) = reusable_caller_decision(&caller);
        assert!(decision.finalize_caller);
        assert_eq!(decision.outputs["answer"], "42");
        assert_eq!(status, ExecutionStatus::Success);
    }

    #[test]
    fn needs_waits_until_terminal_and_decrements() {
        let need = NeedRow {
            job_id: jid("a"),
            status: ExecutionStatus::InProgress,
            outputs: BTreeMap::new(),
        };
        assert_eq!(
            needs_decision(&[need], NeedsCondition::DefaultSuccess),
            NeedsDecision::Wait
        );
        assert_eq!(decrement_remaining_needs(2, true), (1, false));
        assert_eq!(decrement_remaining_needs(1, true), (0, true));
    }

    #[test]
    fn matrix_and_parallel_decisions_preserve_order() {
        let siblings = vec![
            FailFastSibling {
                job_id: jid("a"),
                base_id: "m".into(),
                status: ExecutionStatus::Queued,
                agent_job_id: None,
            },
            FailFastSibling {
                job_id: jid("b"),
                base_id: "m".into(),
                status: ExecutionStatus::InProgress,
                agent_job_id: None,
            },
        ];
        assert_eq!(
            matrix_fail_fast(&jid("x"), Some("m"), true, &siblings),
            vec![jid("a"), jid("b")]
        );
        let legs = siblings
            .into_iter()
            .map(|s| MatrixLegRow {
                job_id: s.job_id.clone(),
                status: if s.job_id == jid("b") {
                    ExecutionStatus::Success
                } else {
                    s.status
                },
                queue_state: QueueState::Blocked,
                base_id: "m".into(),
                order: 0,
            })
            .collect::<Vec<_>>();
        assert_eq!(max_parallel_admission(&legs, Some(1)), vec![jid("a")]);
    }

    #[test]
    fn concurrency_and_queue_mapping_follow_contract() {
        let arrival = ConcurrencyRow {
            group: "g".into(),
            run_id: rid(2),
            job_id: None,
            wait_id: 9,
            cancel_in_progress: true,
        };
        let holder = ConcurrencyRow {
            group: "g".into(),
            run_id: rid(1),
            job_id: None,
            wait_id: 1,
            cancel_in_progress: false,
        };
        assert!(matches!(
            concurrency_admission(&arrival, Some(&holder)),
            ConcurrencyAdmission::CancelCurrent(_)
        ));
        assert_eq!(
            concurrency_fifo_promotion(&[holder.clone(), arrival.clone()]),
            Some(holder)
        );
        assert_eq!(
            queue_state(QueueStateInput {
                status: ExecutionStatus::Pending,
                remaining_needs: 1,
                ready: false,
                claimed: false,
                concurrency_wait: false,
                max_parallel_wait: false,
                pending_expansion: false,
                expanding: false
            }),
            QueueState::Blocked
        );
        assert_eq!(
            queue_state(QueueStateInput {
                status: ExecutionStatus::Pending,
                remaining_needs: 0,
                ready: false,
                claimed: false,
                concurrency_wait: true,
                max_parallel_wait: false,
                pending_expansion: false,
                expanding: false
            }),
            QueueState::Held
        );
        assert_eq!(
            queue_state(QueueStateInput {
                status: ExecutionStatus::Pending,
                remaining_needs: 0,
                ready: false,
                claimed: false,
                concurrency_wait: false,
                max_parallel_wait: true,
                pending_expansion: false,
                expanding: false
            }),
            QueueState::Blocked
        );
        assert_eq!(
            queue_state(QueueStateInput {
                status: ExecutionStatus::Pending,
                remaining_needs: 0,
                ready: false,
                claimed: false,
                concurrency_wait: false,
                max_parallel_wait: false,
                pending_expansion: false,
                expanding: false
            }),
            QueueState::None
        );
    }

    #[test]
    fn concurrency_queue_modes_cancel_expected_rows() {
        let arrival = ConcurrencyRow {
            group: "g".into(),
            run_id: rid(3),
            job_id: None,
            wait_id: 3,
            cancel_in_progress: false,
        };
        let single = concurrency_queue_decision(
            ConcurrencyQueueMode::Single,
            std::slice::from_ref(&arrival),
        );
        assert_eq!(single.cancel_pending, vec![arrival.clone()]);
        assert!(single.park_arrival && !single.cancel_arrival);
        let existing = (0..MAX_CONCURRENCY_WAITERS)
            .map(|wait_id| ConcurrencyRow {
                wait_id: wait_id as u64,
                ..arrival.clone()
            })
            .collect::<Vec<_>>();
        let maxed = concurrency_queue_decision(ConcurrencyQueueMode::Max, &existing);
        assert!(maxed.cancel_arrival && !maxed.park_arrival);
    }

    #[test]
    fn expansion_and_completion_are_fenced() {
        assert_eq!(
            expansion_decision(
                ExpansionFence {
                    expected_generation: 1,
                    current_generation: 2
                },
                Ok(2)
            ),
            ExpansionDecision::Stale
        );
        assert_eq!(
            expansion_decision(
                ExpansionFence {
                    expected_generation: 1,
                    current_generation: 1
                },
                Ok(0)
            ),
            ExpansionDecision::Empty(ExecutionStatus::Skipped)
        );
        let decision = completion_decision(CompletionRow {
            job_id: jid("a"),
            prior_status: ExecutionStatus::Failure,
            reported_status: ExecutionStatus::Failure,
            continue_on_error: false,
            run_will_be_terminal: true,
        });
        assert!(
            decision.replayed && decision.release_workflow_hold && !decision.promote_dependents
        );
    }

    #[test]
    fn aggregate_needs_status_waits_for_all_terminal() {
        // Mixed terminal + non-terminal must not conclude: a failure among
        // still-running needs does not aggregate early (the job cannot run
        // until every dependency settles).
        assert_eq!(
            aggregate_needs_status(&[ExecutionStatus::Failure, ExecutionStatus::InProgress]),
            None
        );
        assert_eq!(
            aggregate_needs_status(&[ExecutionStatus::Success, ExecutionStatus::Pending]),
            None
        );
        assert_eq!(aggregate_needs_status(&[]), None);
        // All-terminal precedence still holds.
        assert_eq!(
            aggregate_needs_status(&[ExecutionStatus::Success, ExecutionStatus::Failure]),
            Some(ExecutionStatus::Failure)
        );
        assert_eq!(
            aggregate_needs_status(&[ExecutionStatus::Skipped, ExecutionStatus::Cancelled]),
            Some(ExecutionStatus::Cancelled)
        );
        assert_eq!(
            aggregate_needs_status(&[ExecutionStatus::Success, ExecutionStatus::Success]),
            Some(ExecutionStatus::Success)
        );
    }

    #[test]
    fn concurrency_admission_same_run_waits_without_cancel_in_progress() {
        // Two jobs of one run sharing a group: with cancel_in_progress unset
        // the second job must WAIT on the group, not acquire it. The same-run
        // exclusion only suppresses cancellation, never grants admission.
        let holder = ConcurrencyRow {
            group: "g".into(),
            run_id: rid(1),
            job_id: Some(jid("job-a")),
            wait_id: 1,
            cancel_in_progress: false,
        };
        let same_run_arrival = ConcurrencyRow {
            group: "g".into(),
            run_id: rid(1), // same run as the holder
            job_id: Some(jid("job-b")),
            wait_id: 2,
            cancel_in_progress: false,
        };
        assert_eq!(
            concurrency_admission(&same_run_arrival, Some(&holder)),
            ConcurrencyAdmission::Waiting
        );

        // Same run + cancel_in_progress: the arrival takes over the hold with
        // no CancelCurrent emitted for the same-run holder.
        let cancelling_same_run = ConcurrencyRow {
            cancel_in_progress: true,
            ..same_run_arrival.clone()
        };
        assert_eq!(
            concurrency_admission(&cancelling_same_run, Some(&holder)),
            ConcurrencyAdmission::Acquired
        );

        // Different run + cancel_in_progress: holder is cancelled.
        let foreign_cancel = ConcurrencyRow {
            run_id: rid(2),
            cancel_in_progress: true,
            ..same_run_arrival.clone()
        };
        assert!(matches!(
            concurrency_admission(&foreign_cancel, Some(&holder)),
            ConcurrencyAdmission::CancelCurrent(_)
        ));

        // No holder: acquires regardless of run.
        assert_eq!(
            concurrency_admission(&same_run_arrival, None),
            ConcurrencyAdmission::Acquired
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Queue stamps, runner matching, dependencies and expansion build
// ─────────────────────────────────────────────────────────────────────────
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
// Job-level concurrency gate
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobGateOutcome {
    Proceed,
    Parked,
    Failed(ExecutionStatus),
}

/// Release every group presence of one job (ported verbatim: scans every
/// group, not only the run's `holder_keys`, because a pending holder can
/// outlive the bookkeeping that created it).
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

/// Whether a pending job may run, must wait, or is skipped, from its
/// declared `needs:` results on the run (dependencies never cross runs).
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

// ─────────────────────────────────────────────────────────────────────────
// Request lifecycle
// ─────────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
pub(crate) enum RequestRetirement {
    Settle(ExecutionStatus),
    Purge,
}

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

impl BuiltExpansion {
    /// Jobs this expansion registers — each mints one `job_requests` row.
    pub(crate) fn job_count(&self) -> usize {
        match self {
            Self::Reusable { jobs, .. } | Self::Matrix { jobs } => jobs.len(),
        }
    }
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
    F: Fn(&preloop_gha_protocol::JobPlan) -> preloop_gha_expressions::Context,
{
    let base_url = crate::broker::runner_base_url();
    let normalized_github =
        preloop_gha_parser::job_builder::normalize_github_context(&ctx.github_json);
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
            &base_url,
            ctx.snapshot.as_ref(),
            plan,
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
            condition_context: condition_context(plan),
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
    let jobs = build_jobs(shared, &ctx, &expanded.jobs, |plan| {
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
    let jobs = build_jobs(shared, &ctx, &plans, |plan| {
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
