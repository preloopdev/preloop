//! Backend-neutral scheduling decisions.
//!
//! Pure functions over small row structs: no I/O, no locks, no working set,
//! plus the expansion-build machinery that produces [`BuiltExpansion`] for
//! `apply_expansion`. Both backends call these so they cannot diverge.

use crate::models::{QueuedJob, RunRecord};
use crate::state::JobSetGate;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId, WorkflowSubmission};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

// Pure helpers shared with the runtime scheduler.
pub(crate) use crate::runtime_scheduling::{
    DependencyDecision, SchedulingOutcome, aggregate_need_status, matching_need_ids,
    matching_need_statuses, needs_json_context,
};

/// Namespace used to deterministically encode legacy non-UUID session ids.
pub(crate) const SESSION_ID_NAMESPACE: uuid::Uuid =
    uuid::uuid!("7d3c0a1c-6f4a-4d4c-a5d4-8d27b5a3c1f0");

/// Convert a session id to its stored UUID, hashing legacy ids with
/// RFC 4122 v5 (SHA-1) so the mapping of a stored id never changes.
///
/// Uses the workspace's `sha1` crate rather than `uuid`'s `v5` feature: the
/// latter pulls the unvetted `sha1_smol` into the dependency graph.
pub fn session_uuid(id: &str) -> uuid::Uuid {
    id.parse()
        .unwrap_or_else(|_| uuid_v5(&SESSION_ID_NAMESPACE, id))
}

/// RFC 4122 UUID v5: SHA-1 of the namespace bytes then the name, first 16
/// bytes, with the version and variant bits set.
fn uuid_v5(namespace: &uuid::Uuid, name: &str) -> uuid::Uuid {
    use sha1::{Digest, Sha1};

    let mut hasher = Sha1::new();
    hasher.update(namespace.as_bytes());
    hasher.update(name.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid::Builder::from_sha1_bytes(bytes).into_uuid()
}

/// How long an unmatched ready job may wait for a matching runner.
pub(crate) const QUEUED_JOB_GRACE: Duration = Duration::from_secs(120);
/// Absolute backstop, measured from ready-enqueue, on how long a job whose
/// labels the pool can satisfy — or whose runner a preparing pool may still
/// provide — waits for a matching runner. One hour covers a full golden
/// rebuild plus several failed provision rounds.
pub(crate) const MAX_QUEUED_GRACE: Duration = Duration::from_secs(3600);

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
    Mark {
        first_seen: SystemTime,
    },
    /// No matching runner appeared within `grace`.
    Starve {
        reason: String,
        grace: Duration,
    },
    /// The co-hosted pool's advertised labels can never satisfy the job.
    Unschedulable {
        reason: String,
    },
}

/// Labels only an external (macOS/Windows) host can serve: such a job waits
/// for that host to register rather than failing on the Linux pool's terms.
pub(crate) fn needs_external_host(runs_on: &[String]) -> bool {
    runs_on.iter().any(|label| {
        let label = label.to_ascii_lowercase();
        label.starts_with("macos") || label.starts_with("windows")
    })
}

/// Why a job fails fast against the co-hosted pool: the pool published its
/// advertised labels and they can never satisfy `runs_on`. `None` when the
/// pool published none (external-only deployments) or the labels match.
pub(crate) fn pool_unsatisfiable_reason(
    runs_on: &[String],
    pool_labels: &[String],
) -> Option<String> {
    (!pool_labels.is_empty()
        && !crate::runtime_scheduling::job_matches_runner(runs_on, pool_labels))
    .then(|| {
        format!(
            "no runner is registered for `runs-on: {}` and the runner pool's advertised \
             labels ({}) can never satisfy it, so the job cannot be scheduled",
            runs_on.join(", "),
            pool_labels.join(", ")
        )
    })
}

/// A `runs-on` still carrying an unevaluated `${{ }}` template: its labels
/// read `needs.*` and only become concrete at promotion.
pub(crate) fn runs_on_deferred(runs_on: &[String]) -> bool {
    runs_on.iter().any(|label| label.contains("${{"))
}

/// Why a concrete `runs-on` fails at submit or promotion instead of queueing:
/// the pool's advertised labels can never satisfy it and nothing else can
/// serve it — no registered runner matches and it does not wait for an
/// external host (the starvation sweep's rule). `None` otherwise, including
/// while the labels are still templates or the pool published none.
pub(crate) fn unschedulable_reason(
    runs_on: &[String],
    pool_labels: &[String],
    any_runner_matches: bool,
) -> Option<String> {
    if any_runner_matches || needs_external_host(runs_on) || runs_on_deferred(runs_on) {
        return None;
    }
    pool_unsatisfiable_reason(runs_on, pool_labels)
}

/// After deferred `runs-on` becomes concrete at promotion: fail if the
/// platform is unhostable or the co-hosted pool can never serve it and no
/// registered runner / external host will. `None` queues the job.
pub(crate) fn promotion_unsatisfiable_reason(
    runs_on: &[String],
    platforms: impl IntoIterator<Item = &'static str>,
    pool_labels: &[String],
    any_runner_matches: bool,
) -> Option<String> {
    if let Some(platform) = unhostable_platform(runs_on, platforms) {
        return Some(unhostable_reason(platform, runs_on));
    }
    unschedulable_reason(runs_on, pool_labels, any_runner_matches)
}

/// Whether a job is already terminal at submit (gated `if:`, unhostable, or
/// pool-unsatisfiable). Placeholders expand later and never conclude here.
pub(crate) fn concludes_at_submit(
    initially_skipped: bool,
    has_needs: bool,
    placeholder: bool,
    runs_on: &[String],
    check_hostable: bool,
    platforms: impl IntoIterator<Item = &'static str>,
    pool_labels: &[String],
    any_runner_matches: bool,
) -> bool {
    if initially_skipped {
        return true;
    }
    if placeholder || has_needs {
        // Placeholders expand later, and needs-gated jobs park until their
        // `if:` can be evaluated — label gates belong to promotion, so
        // neither concludes here.
        return false;
    }
    if check_hostable && unhostable_platform(runs_on, platforms).is_some() {
        return true;
    }
    unschedulable_reason(runs_on, pool_labels, any_runner_matches).is_some()
}

/// Decide starvation using the production 120/3600 second grace rules.
///
/// Ages are measured on this node's clock; an enqueue instant another node
/// stamped slightly in the future counts as age zero rather than as expired.
/// `enqueued_at == UNIX_EPOCH` means the enqueue instant is unknown.
pub(crate) fn starvation_verdict(
    job: &StarvationCandidate<'_>,
    now: SystemTime,
    pool_preparing: bool,
    warm_window_open: bool,
    pool_labels: &[String],
) -> StarvationVerdict {
    // Non-Linux jobs can only run on a registered host; keep them queued
    // until it appears rather than failing a temporarily empty host pool.
    if job.any_runner_matches || needs_external_host(job.runs_on) {
        return StarvationVerdict::ClearMark;
    }
    let enqueue_age = (job.enqueued_at != SystemTime::UNIX_EPOCH)
        .then(|| now.duration_since(job.enqueued_at).unwrap_or_default());
    let within_ceiling = enqueue_age.is_some_and(|age| age < MAX_QUEUED_GRACE);
    // A job the pool's advertised labels can satisfy is exempt from the short
    // grace — its runner appears once the pool warms — so only the backstop
    // applies; one they can never satisfy fails fast instead of starving.
    if !pool_labels.is_empty() {
        if let Some(reason) = pool_unsatisfiable_reason(job.runs_on, pool_labels) {
            return StarvationVerdict::Unschedulable { reason };
        }
        if within_ceiling {
            return StarvationVerdict::ClearMark;
        }
    }
    let grace = if pool_preparing {
        // A known enqueue instant is protected until the ceiling; a restored
        // job whose instant was lost gets this process's warm window only.
        if within_ceiling || (enqueue_age.is_none() && warm_window_open) {
            // Provisioning time does not consume the short grace. Re-stamp
            // at every protected tick so a retry gap starts a fresh window.
            return StarvationVerdict::Mark { first_seen: now };
        }
        MAX_QUEUED_GRACE
    } else {
        let first_seen = job.first_seen.unwrap_or(job.enqueued_at);
        if now.duration_since(first_seen).unwrap_or_default() < QUEUED_JOB_GRACE {
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
    // One label matcher for claim and pairing: the pre-refactor
    // `take_matching_job` rule the pairing, reaper, and starvation paths
    // already call.
    crate::runtime_scheduling::job_matches_runner(job_labels, &runner.labels)
        && runner_group_matches(required_group, runner)
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

/// Apply the queue mapping exactly:
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

/// How an outbox event relates to the state row it reports on, decided when
/// the event is appended in its own transaction after the change committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EventStamp {
    /// The row still holds the reported state: the event carries the row's
    /// version, so a consumer can order it against other events.
    Version(i64),
    /// The row has moved to a different state that is not final yet: the
    /// event is published with no ordering claim.
    Unversioned,
    /// The row has already settled on a different final state. A newer
    /// event describes that state; this one must not be published.
    Stale,
}

/// [`EventStamp`] for a `JobStatus` event against the job's `status` and
/// `version`. A `timed_out` row reports as `Failure`, the only form the
/// event enum has.
pub(crate) fn job_event_stamp(
    event: ExecutionStatus,
    row_status: &str,
    row_version: i64,
) -> EventStamp {
    let row = if row_status == "timed_out" {
        ExecutionStatus::Failure
    } else {
        crate::control::types::status_parse(row_status)
    };
    if row == event {
        EventStamp::Version(row_version)
    } else if row.is_terminal() {
        EventStamp::Stale
    } else {
        EventStamp::Unversioned
    }
}

/// [`EventStamp`] for a `RunStatus` event against the run's `status`,
/// `conclusion` and `version`. A run held for a gate is `Pending` on the
/// wire while its row says `queued`.
pub(crate) fn run_event_stamp(
    event: ExecutionStatus,
    row_status: &str,
    row_conclusion: Option<&str>,
    row_version: i64,
) -> EventStamp {
    let settled = row_status == "completed";
    let row = match (row_status, row_conclusion) {
        ("completed", Some("timed_out")) | ("completed", None) => ExecutionStatus::Failure,
        ("completed", Some(conclusion)) => crate::control::types::status_parse(conclusion),
        (status, _) => crate::control::types::status_parse(status),
    };
    let matches =
        row == event || (row == ExecutionStatus::Queued && event == ExecutionStatus::Pending);
    if matches {
        EventStamp::Version(row_version)
    } else if settled {
        EventStamp::Stale
    } else {
        EventStamp::Unversioned
    }
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
            starvation_verdict(&job, now, false, false, &[]),
            StarvationVerdict::ClearMark
        );
        let mac = labels(&["macOS-14"]);
        let job = starvation_candidate(&mac, Duration::from_secs(10_000), None, now);
        assert_eq!(
            starvation_verdict(&job, now, false, false, &[]),
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
            starvation_verdict(&job, now, false, false, &[]),
            StarvationVerdict::Mark {
                first_seen: now - Duration::from_secs(30)
            }
        );
        let job = starvation_candidate(&linux, Duration::from_secs(121), None, now);
        let StarvationVerdict::Starve { reason, grace } =
            starvation_verdict(&job, now, false, false, &[])
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
        // Protected ticks re-stamp the observation clock.
        let young = starvation_candidate(&linux, Duration::from_secs(3599), None, now);
        assert_eq!(
            starvation_verdict(&young, now, true, false, &[]),
            StarvationVerdict::Mark { first_seen: now }
        );
        // The ceiling holds even inside this process's warm window: a known
        // enqueue instant past it starves.
        let old = starvation_candidate(&linux, MAX_QUEUED_GRACE, None, now);
        for warm_window_open in [false, true] {
            assert!(matches!(
                starvation_verdict(&old, now, true, warm_window_open, &[]),
                StarvationVerdict::Starve { grace, .. } if grace == MAX_QUEUED_GRACE
            ));
        }
        // A restored job whose enqueue instant was lost gets the warm window
        // only.
        let unknown = StarvationCandidate {
            enqueued_at: SystemTime::UNIX_EPOCH,
            ..starvation_candidate(&linux, Duration::ZERO, None, now)
        };
        assert_eq!(
            starvation_verdict(&unknown, now, true, true, &[]),
            StarvationVerdict::Mark { first_seen: now }
        );
        assert!(matches!(
            starvation_verdict(&unknown, now, true, false, &[]),
            StarvationVerdict::Starve { .. }
        ));
    }

    #[test]
    fn pool_labels_fail_fast_or_exempt_until_the_ceiling() {
        let now = SystemTime::now();
        let pool = labels(&["self-hosted", "linux", "x64"]);
        let gpu = labels(&["self-hosted", "gpu"]);
        let fresh_gpu = starvation_candidate(&gpu, Duration::from_secs(1), None, now);
        let StarvationVerdict::Unschedulable { reason } =
            starvation_verdict(&fresh_gpu, now, true, true, &pool)
        else {
            panic!("labels the pool can never satisfy must fail fast");
        };
        assert!(reason.contains("can never satisfy"), "{reason}");
        // An external-host job waits for its host regardless of pool labels.
        let mac = labels(&["macos-14"]);
        let mac_job = starvation_candidate(&mac, Duration::from_secs(1), None, now);
        assert_eq!(
            starvation_verdict(&mac_job, now, false, false, &pool),
            StarvationVerdict::ClearMark
        );
        // A job the pool can satisfy skips the short grace until the ceiling.
        let linux = labels(&["self-hosted", "linux"]);
        let waiting = starvation_candidate(&linux, Duration::from_secs(600), None, now);
        assert_eq!(
            starvation_verdict(&waiting, now, false, false, &pool),
            StarvationVerdict::ClearMark
        );
        let stuck = starvation_candidate(&linux, MAX_QUEUED_GRACE, None, now);
        assert!(matches!(
            starvation_verdict(&stuck, now, false, false, &pool),
            StarvationVerdict::Starve { .. }
        ));
    }

    #[test]
    fn unschedulable_reason_exempts_external_hosts_and_templates() {
        let pool = labels(&["self-hosted", "linux", "x64"]);
        assert!(unschedulable_reason(&labels(&["macos-14"]), &pool, false).is_none());
        assert!(
            unschedulable_reason(&["${{ needs.plan.outputs.runner }}".into()], &pool, false)
                .is_none()
        );
        assert!(unschedulable_reason(&labels(&["gpu-large"]), &pool, true).is_none());
        let reason = unschedulable_reason(&labels(&["gpu-large"]), &pool, false)
            .expect("unmatched pool-incompatible labels fail");
        assert!(reason.contains("can never satisfy"), "{reason}");
        assert!(unschedulable_reason(&labels(&["gpu-large"]), &[], false).is_none());
    }

    #[test]
    fn promotion_rejects_unhostable_and_unsatisfiable_resolved_labels() {
        let pool = labels(&["ubuntu-latest"]);
        let reason = promotion_unsatisfiable_reason(
            &labels(&["windows-latest"]),
            std::iter::empty(),
            &pool,
            false,
        )
        .expect("windows with no windows runner is unhostable");
        assert!(reason.contains("windows"), "{reason}");
        assert!(reason.contains("registered with this server"), "{reason}");
        let reason =
            promotion_unsatisfiable_reason(&labels(&["gpu-large"]), ["linux"], &pool, false)
                .expect("resolved label the pool cannot serve fails");
        assert!(reason.contains("can never satisfy"), "{reason}");
        assert!(
            promotion_unsatisfiable_reason(&labels(&["ubuntu-latest"]), ["linux"], &pool, false)
                .is_none()
        );
    }

    #[test]
    fn workless_jobs_conclude_at_submit_placeholders_do_not() {
        let pool = labels(&["ubuntu-latest"]);
        let linux = ["linux"];
        assert!(concludes_at_submit(
            true,
            false,
            false,
            &labels(&["ubuntu-latest"]),
            true,
            linux,
            &pool,
            false
        ));
        assert!(!concludes_at_submit(
            false,
            false,
            true,
            &labels(&["gpu-large"]),
            true,
            linux,
            &pool,
            false
        ));
        assert!(concludes_at_submit(
            false,
            false,
            false,
            &labels(&["gpu-large"]),
            true,
            linux,
            &pool,
            false
        ));
        assert!(!concludes_at_submit(
            false,
            false,
            false,
            &labels(&["ubuntu-latest"]),
            true,
            linux,
            &pool,
            false
        ));
        // Needs-gated jobs never conclude at submit — even with labels the
        // pool cannot serve — because their `if:` may skip them at promotion.
        assert!(!concludes_at_submit(
            false,
            true,
            false,
            &labels(&["gpu-large"]),
            true,
            linux,
            &pool,
            false
        ));
    }

    #[test]
    fn an_enqueue_stamped_ahead_of_this_clock_is_fresh() {
        let now = SystemTime::now();
        let linux = labels(&["self-hosted", "linux"]);
        let ahead = StarvationCandidate {
            enqueued_at: now + Duration::from_secs(2),
            ..starvation_candidate(&linux, Duration::ZERO, None, now)
        };
        assert!(matches!(
            starvation_verdict(&ahead, now, false, false, &[]),
            StarvationVerdict::Mark { .. }
        ));
        assert_eq!(
            starvation_verdict(&ahead, now, true, false, &[]),
            StarvationVerdict::Mark { first_seen: now }
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

    /// A 24.04 machine may stand in for an `ubuntu-22.04` job, but it must not
    /// take one while a job it exactly matches is claimable: the pool is
    /// usually already building the 22.04 machine that job asked for, and the
    /// stand-in would hand it a different base image for no reason.
    #[test]
    fn exact_labels_beat_an_earlier_stand_in_candidate() {
        let runner = RunnerMatchRow {
            labels: labels(&[
                "self-hosted",
                "Linux",
                "X64",
                "ubuntu-24.04",
                "ubuntu-latest",
            ]),
            known: true,
            ..Default::default()
        };
        // `pinned` is first in the queue, so only the preference can reorder
        // it: it is a stand-in candidate, never an exact match.
        let pinned = ClaimCandidate {
            run_id: rid(1),
            job_id: jid("pinned"),
            runs_on: labels(&["ubuntu-22.04"]),
            runner_group: None,
            assigned_runner_id: None,
            assignment_fresh: false,
            queue_position: 0,
            claimable: true,
        };
        let wide = ClaimCandidate {
            run_id: rid(1),
            job_id: jid("wide"),
            runs_on: labels(&["self-hosted"]),
            runner_group: None,
            assigned_runner_id: None,
            assignment_fresh: false,
            queue_position: 1,
            claimable: true,
        };
        assert_eq!(
            claim_preference(&[pinned.clone(), wide], None, &runner.labels, None, &runner),
            Some(1),
            "the exact `self-hosted` match must win over the 22.04 stand-in"
        );
        // And the stand-in still happens rather than starving the pinned job.
        assert_eq!(
            claim_preference(&[pinned], None, &runner.labels, None, &runner),
            Some(0),
            "the pinned job stays claimable once the exact match is gone"
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

/// Why a job whose platform no registered runner hosts is concluded
/// (`bounded_termination_reason` classifies this prose as
/// `no_platform_runner`).
pub(crate) fn unhostable_reason(platform: &str, runs_on: &[String]) -> String {
    format!(
        "no {platform} runner is registered with this server, so `runs-on: {}` \
         cannot be scheduled",
        runs_on.join(", ")
    )
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
    /// The deferred node's own `inputs` context. For a top-level node this is
    /// the run's dispatch inputs (stamped on the plan at submit time); for a
    /// node inside a reusable workflow it is the caller's `with` values that
    /// the callee subtree was expanded with. The fan-out cells must see these
    /// scoped inputs, not the root dispatch inputs: GitHub scopes `inputs` to
    /// the workflow that declares the job, so a callee cell reading
    /// `inputs.dry_run` gets the caller's `with` value even on a
    /// push-triggered run whose dispatch map is empty.
    pub(crate) scoped_inputs: BTreeMap<String, serde_json::Value>,
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
        scoped_inputs,
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
        // Fan out with the deferred node's own scoped inputs: the root run's
        // dispatch inputs for a top-level node, or the caller's `with` values
        // for a node inside a reusable workflow. The legacy
        // `submission.inputs` field is empty for workflow_dispatch runs, so
        // using it here would fan out cells with an empty `inputs` context
        // while plain jobs see the values.
        Some(&scoped_inputs),
    )
    .map_err(|error| {
        tracing::warn!(%run_id, job = %node_id, %error, "dynamic matrix expansion failed");
        ExecutionStatus::Failure
    })?;

    let github_json = ctx.github_json.clone();
    let vars = ctx.submission.vars.clone();
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
            // The fan-out plans carry the node's scoped inputs on the plan
            // itself, so the cell `if:` sees the same `inputs` the steps will.
            &plan.inputs,
        )
    })?;
    Ok(BuiltExpansion::Matrix { jobs })
}

#[cfg(test)]
mod matrix_expansion_tests {
    use super::*;

    /// The workflow the fan-out tests expand: a needs-deferred matrix whose
    /// cell gates on `inputs.dry_run`.
    const DEFERRED_MATRIX_WORKFLOW: &str = r#"
on: push
jobs:
  gen:
    runs-on: ubuntu-latest
    steps:
      - run: echo gen
  downstream:
    needs: [gen]
    runs-on: ubuntu-latest
    if: ${{ inputs.dry_run }}
    strategy:
      matrix: ${{ fromJSON(needs.gen.outputs.matrix) }}
    steps:
      - run: echo dynamic
"#;

    /// A deferred matrix node fans out with its own scoped `inputs`: the
    /// root run's dispatch inputs for a top-level node (stamped on the plan at
    /// submit time), or the caller's `with` values for a node inside a
    /// reusable workflow. The legacy `submission.inputs` field is empty for
    /// workflow_dispatch runs, so fanning out with it handed the cells an
    /// empty `inputs` context while plain jobs saw the values, and both the
    /// step expressions and the job-level `if:` resolved against nothing
    /// (#285).
    #[tokio::test]
    async fn matrix_expansion_uses_deferred_node_scoped_inputs() {
        let temp = tempfile::tempdir().unwrap();
        let state = crate::AppState::new(temp.path().to_path_buf())
            .await
            .unwrap();
        let shared = state.shared();

        let scoped_inputs = BTreeMap::from([("dry_run".to_owned(), serde_json::json!(true))]);
        let submission = Arc::new(WorkflowSubmission {
            workflow_yaml: DEFERRED_MATRIX_WORKFLOW.to_owned(),
            event: "push".to_owned(),
            repository: "owner/repo".to_owned(),
            ..Default::default()
        });
        // The legacy field is empty — as it is on every workflow_dispatch
        // run — so only `scoped_inputs` can carry the fan-out's values.
        assert!(submission.inputs.is_empty());

        let built = build_expansion(
            &shared,
            ExpansionPlan::Matrix(Box::new(MatrixExpansionInputs {
                ctx: ExpansionContext {
                    run_id: RunId::new(),
                    submission,
                    snapshot: None,
                    github_json: serde_json::json!({}),
                    workflow_path: ".github/workflows/ci.yml".to_owned(),
                    workflow_ref: "owner/repo/.github/workflows/ci.yml@refs/heads/main".to_owned(),
                    head_sha: "0".repeat(40),
                },
                node_id: JobId("downstream".to_owned()),
                base_id: "downstream".to_owned(),
                expression: "${{ fromJSON(needs.gen.outputs.matrix) }}".to_owned(),
                needs_outputs: BTreeMap::from([(
                    "gen".to_owned(),
                    BTreeMap::from([(
                        "matrix".to_owned(),
                        serde_json::json!({
                            "include": [
                                {"os": "ubuntu-latest"},
                                {"os": "ubuntu-22.04"},
                            ],
                        }),
                    )]),
                )]),
                workflow_file: None,
                scoped_inputs: scoped_inputs.clone(),
            })),
        )
        .expect("the deferred matrix must expand");

        let BuiltExpansion::Matrix { jobs } = built else {
            panic!("a matrix node must fan out");
        };
        assert_eq!(jobs.len(), 2, "both matrix combinations must fan out");
        for job in &jobs {
            assert_eq!(
                job.plan.inputs, scoped_inputs,
                "cell `{}` must carry the node's scoped inputs",
                job.plan.id.0
            );
            // Evaluate exactly as the promotion path does: the cell `if:`
            // gates on `inputs.dry_run`, which is true only when the
            // condition context was built from the plan's own inputs.
            let condition =
                preloop_gha_expressions::effective_condition(job.plan.if_condition.as_deref());
            let context = job
                .condition_context
                .clone()
                .with_status(true, false, false);
            assert!(
                preloop_gha_expressions::eval_bool(&condition, &context).unwrap(),
                "cell `{}` must see `inputs.dry_run` in its `if:` context",
                job.plan.id.0
            );
        }
    }
}
