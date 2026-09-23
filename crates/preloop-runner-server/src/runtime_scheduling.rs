use super::*;

/// Outcome of evaluating a job's environment protection rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvironmentGateOutcome {
    /// No rules configured, or every rule satisfied — the job may proceed to
    /// concurrency gating.
    Proceed,
    /// A wait timer has not expired yet, or required approvals are still
    /// outstanding — keep the job pending; a later sweep re-evaluates.
    Wait,
    /// A rule failed closed (branch policy mismatch, approval window
    /// expired) — the job must be concluded as a failure.
    Failed,
}

/// How long a pending-approval gate stays open before the job fails closed.
pub const ENVIRONMENT_APPROVAL_WINDOW_NANOS: i64 = 24 * 60 * 60 * 1_000_000_000;

/// Resolve the literal `environment:` name for rule lookup. Expression-based
/// names arrive here unresolvable (same as the registry existence check in
/// `build_job_artifacts`); they match no rules and proceed.
fn environment_gate_name(job: &QueuedJob) -> Option<&str> {
    match job.environment.as_ref()? {
        serde_json::Value::String(name) => Some(name.as_str()),
        serde_json::Value::Object(map) => map.get("name").and_then(serde_json::Value::as_str),
        _ => None,
    }
}

/// Compare a configured branch entry against the run's git ref, ignoring a
/// leading `refs/heads/` on either side so `main` matches `refs/heads/main`.
fn branch_allowed(allowed: &[String], git_ref: &str) -> bool {
    let run_branch = git_ref.strip_prefix("refs/heads/").unwrap_or(git_ref);
    allowed.iter().any(|entry| {
        let entry_branch = entry.strip_prefix("refs/heads/").unwrap_or(entry);
        !entry_branch.is_empty() && entry_branch == run_branch
    })
}

/// Evaluate the operator's `[environment_rules]` for one job at scheduler
/// admission. Runs before concurrency gating so a denied or waiting job never
/// occupies a concurrency slot, and before queueing so a denied job's
/// environment secrets never reach a runner.
///
/// Gate progress is stamped on `job.environment_gate`, which travels in the
/// persisted job snapshot: a restart re-arms from the stamps rather than
/// dropping an armed gate (fail closed). The config file is the source of
/// truth — removing an environment's rules releases its armed gates.
pub fn check_environment_gates(
    rules: &crate::config::EnvironmentRulesMap,
    repository: &str,
    git_ref: &str,
    job: &mut QueuedJob,
    now_unix_nanos: i64,
) -> EnvironmentGateOutcome {
    let env_name = environment_gate_name(job).map(str::to_owned);
    let Some(env_name) = env_name.as_deref() else {
        return EnvironmentGateOutcome::Proceed;
    };
    let Some(rule) = rules.get(repository).and_then(|envs| envs.get(env_name)) else {
        // No rules for this environment: release any stale gate state and
        // proceed. Today's behavior is preserved exactly.
        job.environment_gate = None;
        return EnvironmentGateOutcome::Proceed;
    };
    let gate = job
        .environment_gate
        .get_or_insert_with(EnvironmentGateState::default);

    // 1. Deployment branch policy: fail closed on mismatch.
    if !rule.deployment_branches.is_empty() && !branch_allowed(&rule.deployment_branches, git_ref) {
        tracing::warn!(
            run_id = %job.run_id.0,
            job_id = %job.job_id.0,
            environment = env_name,
            git_ref,
            "environment gate denied: ref not in deployment_branches"
        );
        return EnvironmentGateOutcome::Failed;
    }

    // 2. Wait timer: arm once, then hold until the deadline passes.
    if gate.wait_until_unix_nanos.is_none() && rule.wait_timer_minutes > 0 {
        // Safe: config load rejects wait_timer_minutes above the i64-nanos
        // bound, so this cast is the identity and the mul cannot saturate.
        let wait_nanos = (rule.wait_timer_minutes as i64).saturating_mul(60_000_000_000);
        gate.wait_until_unix_nanos = Some(now_unix_nanos.saturating_add(wait_nanos));
        tracing::info!(
            run_id = %job.run_id.0,
            job_id = %job.job_id.0,
            environment = env_name,
            wait_timer_minutes = rule.wait_timer_minutes,
            "environment gate: wait timer armed"
        );
    }
    if let Some(deadline) = gate.wait_until_unix_nanos {
        if now_unix_nanos < deadline {
            return EnvironmentGateOutcome::Wait;
        }
        gate.wait_until_unix_nanos = None;
    }

    // 3. Required reviewers: hold until enough approvals are recorded, fail
    // closed when the window expires.
    if gate.approval_requested_at_unix_nanos.is_none() && rule.required_reviewers > 0 {
        gate.approval_requested_at_unix_nanos = Some(now_unix_nanos);
        tracing::info!(
            run_id = %job.run_id.0,
            job_id = %job.job_id.0,
            environment = env_name,
            required_reviewers = rule.required_reviewers,
            "environment gate: waiting for approvals"
        );
    }
    if let Some(requested_at) = gate.approval_requested_at_unix_nanos {
        if (gate.approvals_unix_nanos.len() as u32) >= rule.required_reviewers {
            return EnvironmentGateOutcome::Proceed;
        }
        if now_unix_nanos.saturating_sub(requested_at) > ENVIRONMENT_APPROVAL_WINDOW_NANOS {
            tracing::warn!(
                run_id = %job.run_id.0,
                job_id = %job.job_id.0,
                environment = env_name,
                "environment gate denied: approval window expired"
            );
            return EnvironmentGateOutcome::Failed;
        }
        return EnvironmentGateOutcome::Wait;
    }

    EnvironmentGateOutcome::Proceed
}

/// Outcome of evaluating and acquiring a job-level concurrency gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobGateOutcome {
    /// No gate declared, or the gate was acquired — the job may be queued.
    Proceed,
    /// The gate is busy — park the job in `concurrency_blocked`; the group
    /// release path (`promote_next_from_group`) re-promotes it later.
    Parked,
    /// Gate evaluation failed or the queue overflowed — the job must be
    /// concluded with the given terminal status.
    Failed(ExecutionStatus),
}

/// Release a single concurrency key acquired by a JobSet whose members all
/// became terminal before any could dispatch (e.g. embedded gate overflow).
/// Removes the running holder from the group and promotes the next pending.
pub fn merge_jobset_gate(gates: &mut Vec<JobSetGate>, mut gate: JobSetGate) {
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

#[derive(Debug, Default)]
pub struct SchedulingOutcome {
    pub promoted: usize,
    pub skipped: Vec<(RunId, JobId)>,
    pub failed: Vec<(RunId, JobId)>,
}

impl SchedulingOutcome {
    /// Fold a later sweep's result into this one, so a caller that promotes,
    /// expands and promotes again reports one combined outcome.
    pub fn merge(&mut self, other: SchedulingOutcome) {
        self.promoted += other.promoted;
        self.skipped.extend(other.skipped);
        self.failed.extend(other.failed);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DependencyDecision {
    Wait,
    Run,
    Skip,
    Error,
}

pub fn dependency_decision(run: &RunRecord, job: &QueuedJob) -> DependencyDecision {
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

pub fn matching_need_ids(run: &RunRecord, need: &JobId) -> Vec<JobId> {
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

pub fn matching_need_statuses(run: &RunRecord, need: &JobId) -> Vec<ExecutionStatus> {
    matching_need_ids(run, need)
        .iter()
        .filter_map(|job_id| run.jobs.get(job_id).copied())
        .collect()
}

pub fn ancestor_statuses(run: &RunRecord, job: &QueuedJob) -> Vec<ExecutionStatus> {
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

/// The OS a GitHub-hosted image label names, if it names one.
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

/// The platform a job needs that this deployment cannot host, if any.
///
/// The microVM pool only builds Linux guests; macOS and Windows need a runner
/// process on such a machine, registered against this control plane. When none
/// is registered, a `runs-on: windows-latest` job can never be claimed, and
/// leaving it queued means a wave that never finishes and a check that never
/// reports — the cause invisible unless you read the scheduler's mind. Skip it
/// instead, the way GitHub skips a job whose `if:` excludes it: the run
/// completes, dependents skip, and the reason is in the log.
///
/// Deliberately narrow. A Linux label is never skipped, because the pool
/// provisions Linux on demand and momentarily having no registered runner is
/// normal for an ephemeral pool. And a macOS label is only skipped when no
/// macOS runner is registered — a Mac host serving `macos-latest` is a
/// supported deployment, not an unsupported platform.
pub fn unhostable_platform(
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

/// Check if a job's `runs-on` labels match a runner's registered labels.
///
/// A job matches when every label in the job's `runs-on` is present in the
/// runner's label set (case-insensitive). A GitHub-hosted image label
/// (`ubuntu-latest`, `macos-14`, `windows-latest`) additionally matches a
/// self-hosted runner of the same OS, so a workflow written for hosted
/// runners runs unmodified here.
///
/// That stand-in never crosses operating systems: the official service would
/// never put an `ubuntu-latest` job on a macOS runner, and doing so is worse
/// than leaving the job queued — the job fails deep inside a step on a
/// platform its workflow never targeted (a mac host claiming tokio's
/// Linux-only `taskdump` build, say). A runner that declares no OS label at
/// all stays eligible for any of them: it has told us nothing to contradict.
pub fn job_matches_runner(job_labels: &[String], runner_labels: &[String]) -> bool {
    if job_labels.is_empty() {
        return true;
    }
    // Unknown runner (no session→runner mapping) matches any job labels.
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

/// Match an explicit job group against a registered runner's group.
/// Group is separate from labels; missing metadata on a known runner is the
/// default group (id 1, name `Default`).
pub fn job_matches_runner_group(required_group: Option<&str>, runner: &RunnerCapabilities) -> bool {
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

pub fn job_matches_runner_capabilities(job: &QueuedJob, runner: &RunnerCapabilities) -> bool {
    job_matches_runner(&job.runs_on, &runner.labels)
        && job_matches_runner_group(job.runner_group.as_deref(), runner)
}

/// Whether the runner carries every label the job asked for, verbatim.
///
/// The difference from [`job_matches_runner`] is the hosted-image stand-in: a
/// 24.04 machine *may* run an `ubuntu-22.04` job, but it is not what the job
/// asked for, and the pool is usually already building the machine that is.
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

/// How long an assignment or pool-pending mark stays authoritative. After
/// expiry a job falls back to ordinary permissive scheduling so a crashed
/// pool or dead machine can never wedge a queued job forever.
pub const ASSIGNMENT_TTL: std::time::Duration = std::time::Duration::from_secs(600);

/// How long a pre-claim assignment stays exclusive. Provisioning is bursty
/// and pool runners exit unexpectedly; if the paired runner has not claimed
/// within this window, any *verified* runner may take the job (and later
/// registrations steal the pairing), because an already-dead owner can
/// otherwise hold a job hostage for the full [`ASSIGNMENT_TTL`].
pub const CLAIM_BINDING_TTL: std::time::Duration = std::time::Duration::from_secs(120);

/// Record the lifecycle transition where all `needs:` dependencies are
/// satisfied. Jobs with no dependencies are stamped at construction time.
pub fn stamp_dependencies_ready(job: &mut QueuedJob) {
    if job.dependencies_ready_at_unix_nanos.is_none() {
        job.dependencies_ready_at_unix_nanos = Some(crate::models::now_unix_nanos());
    }
}

/// Record the first time a job waits behind a concurrency gate.
pub fn stamp_concurrency_wait_started(job: &mut QueuedJob) {
    if job.concurrency_wait_started_at_unix_nanos.is_none() {
        job.concurrency_wait_started_at_unix_nanos = Some(crate::models::now_unix_nanos());
    }
}

/// Record the first time an applicable concurrency gate admits this job.
pub fn stamp_concurrency_acquired(job: &mut QueuedJob) {
    if job.concurrency_acquired_at_unix_nanos.is_none() {
        job.concurrency_acquired_at_unix_nanos = Some(crate::models::now_unix_nanos());
    }
}

/// Stamp the instant a job actually enters the ready queue.
///
/// `enqueued_at_unix_nanos` is deliberately not stamped at job construction:
/// a job held for needs, workflow/job concurrency, or max-parallel would
/// otherwise report dependency time as queue wait. Only the promotion sites —
/// where the job is pushed into `tx.queue` — stamp it. Requeues (a claimed
/// job bouncing off a purged runner) preserve the original stamp so total
/// queue time is still measured.
pub fn stamp_ready_enqueue(job: &mut QueuedJob) {
    stamp_dependencies_ready(job);
    job.enqueued_at_unix_nanos = crate::models::now_unix_nanos();
}

/// Live runner -> job pairings for status reporting, sorted by runner id.
///
/// Iterate only active session requests, not the historical request table.
/// The latter retains completed records for late protocol reads and grows for
/// the process lifetime.
pub fn live_runner_assignments(
    requests: &std::collections::BTreeMap<i64, crate::models::TaskAgentJobRequestRecord>,
    active_requests: &std::collections::BTreeMap<String, i64>,
    now: std::time::SystemTime,
) -> Vec<preloop_observability::status::RunnerAssignment> {
    let mut out: Vec<preloop_observability::status::RunnerAssignment> = active_requests
        .values()
        .filter_map(|request_id| requests.get(request_id))
        .filter(|record| record.result.is_none())
        .filter_map(|record| record.owner_runner_id.map(|runner_id| (runner_id, record)))
        .map(
            |(runner_id, record)| preloop_observability::status::RunnerAssignment {
                runner_id,
                run_id: record.run_id.to_string(),
                job_id: record.job_id.0.clone(),
                assigned_seconds_ago: record
                    .started_at
                    .and_then(|at| now.duration_since(at).ok())
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0),
            },
        )
        .collect();
    out.sort_by_key(|a| a.runner_id);
    out
}
pub fn capabilities_of(runner: &RegisteredRunner) -> RunnerCapabilities {
    RunnerCapabilities {
        known: true,
        labels: runner.labels.clone(),
        runner_group_id: runner.runner_group_id,
        runner_group_name: runner.runner_group_name.clone(),
    }
}

pub fn hydrate_needs_context(job: &mut QueuedJob, run: &RunRecord) {
    let needs = job
        .needs
        .iter()
        .filter_map(|need| need_context(run, need).map(|context| (need.0.clone(), context)))
        .collect();
    job.message
        .context_data
        .insert("needs".to_owned(), azdo::PipelineContextData::Dict(needs));

    // `runs-on` labels reading `needs.*` were deliberately left as raw
    // templates at build time (see `resolved_runs_on` in the parser): the
    // needed jobs had not run yet, so evaluating them then would have resolved
    // to "" and the job could never match a runner. Now that the needs are
    // complete, finish them against the completed context.
    let runs_on_deferred = job.runs_on.iter().any(|label| label.contains("${{"));

    // The environment name is the one field the runner never evaluates: it
    // ships as a plain string, so a name reading `needs.*` was deliberately
    // left as a template by the job builder and is finished here, now that the
    // context is complete. Everything else needing `needs` travels as a
    // template token and is evaluated in-VM against the map installed above.
    let deferred_environment_name: Option<String> = job
        .environment
        .as_ref()
        .and_then(|environment| match environment {
            serde_json::Value::String(name) => Some(name.as_str()),
            serde_json::Value::Object(map) => map.get("name").and_then(serde_json::Value::as_str),
            _ => None,
        })
        .filter(|name| preloop_gha_parser::eval::resolves_after_job_build(name))
        .map(str::to_owned);

    if !runs_on_deferred && deferred_environment_name.is_none() {
        return;
    }

    let mut context = preloop_gha_expressions::Context::new();
    for (key, value) in &job.message.context_data {
        context.insert(key, value.to_json());
    }

    if runs_on_deferred {
        resolve_deferred_runs_on(job, &context);
    }

    let Some(name) = deferred_environment_name.as_deref() else {
        return;
    };
    let Some(actions_environment) = job.message.actions_environment.as_mut() else {
        return;
    };
    match preloop_gha_parser::eval::resolve_string(name, &context) {
        Ok(resolved) => actions_environment.name = resolved,
        Err(error) => {
            // Nothing downstream re-resolves this, so a raw template would
            // become the deployment's name in the environment record.
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

/// Finish `runs-on` labels that were left as raw `${{ }}` templates at build
/// time because they read `needs.*` (see `resolved_runs_on` in
/// preloop-gha-parser). Called from [`hydrate_needs_context`], i.e. exactly
/// when the needed jobs have completed and their outputs are in the context,
/// and from the submit path for needs-less jobs, which never pass through
/// promotion and whose `needs` context is already complete (empty).
///
/// A label that is a single expression evaluating to an array contributes one
/// label per element, mirroring GitHub's `runs-on` accepting a string or an
/// array of strings. A label that evaluates to an empty string is kept as an
/// empty label — it matches no runner, so a job whose label genuinely comes
/// back empty (a `needs` output that was never set) starves exactly the way
/// the official service leaves a job with an unmatchable label. Dropping it
/// instead would leave `runs_on` empty, and an empty label list matches every
/// runner: the job would silently run on an arbitrary machine. A label that
/// fails to evaluate keeps its raw template (and is logged) rather than
/// failing the job: an unevaluated label matches nothing, which is the same
/// outcome the build-time fallback has always had.
pub fn resolve_deferred_runs_on(job: &mut QueuedJob, context: &preloop_gha_expressions::Context) {
    if !job.runs_on.iter().any(|label| label.contains("${{")) {
        return;
    }
    let resolved: Vec<String> = job
        .runs_on
        .iter()
        .flat_map(|label| preloop_gha_parser::eval::resolve_runs_on_label(label, context))
        .collect();
    if resolved.iter().any(|label| label.contains("${{")) {
        tracing::warn!(
            run_id = %job.run_id.0,
            job = %job.job_id.0,
            labels = ?resolved,
            "runs-on expression could not be resolved after needs completed; the job may never match a runner"
        );
    }
    job.runs_on = if resolved.is_empty() {
        // Degenerate: every template contributed zero labels (an expression
        // evaluating to an empty array). Keep the raw templates so the job
        // stays unschedulable — an empty label list would match every runner.
        // The warning above fires for these, since they still contain `${{`.
        job.runs_on.clone()
    } else {
        resolved
    };
}

pub fn needs_json_context(run: &RunRecord, needs: &[JobId]) -> serde_json::Value {
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

/// Everything a deferred node needs in order to build its subtree, cloned out
/// of the run record while the lock is held.
///
/// Snapshotting up front is what lets the expensive part — parsing workflow
/// YAML, building one runner message per tx job, minting a runtime token
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
    caller_plan: preloop_gha_protocol::JobPlan,
    call: preloop_gha_protocol::ReusableCallPlan,
    needs_outputs: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
}

struct MatrixExpansionInputs {
    ctx: ExpansionContext,
    node_id: JobId,
    base_id: String,
    expression: String,
    needs_outputs: BTreeMap<String, BTreeMap<String, serde_json::Value>>,
    /// Home workflow of the deferred node: set when the node lives inside a
    /// reusable callee, so the build phase parses the callee YAML rather than
    /// the root workflow.
    workflow_file: Option<String>,
    /// Callee identity of the deferred node (`workflow_ref`, `workflow_sha`,
    /// `workflow_repository` of the workflow defining it): set together with
    /// `workflow_file` when the node lives inside a reusable callee, so the
    /// fan-out cells report the callee in their `job.workflow_*` context
    /// values rather than the root workflow.
    workflow_ref: Option<String>,
    workflow_sha: Option<String>,
    workflow_repository: Option<String>,
    /// The deferred node's own `inputs` context. For a top-level node this is
    /// the run's dispatch inputs (stamped on the plan at submit time); for a
    /// node inside a reusable workflow it is the caller's `with` values that
    /// the callee subtree was expanded with. The fan-out cells must see these
    /// scoped inputs, not the root dispatch inputs: GitHub scopes `inputs` to
    /// the workflow that declares the job, so a callee cell reading
    /// `inputs.dry_run` gets the caller's `with` value even on a
    /// push-triggered run whose dispatch map is empty.
    scoped_inputs: BTreeMap<String, serde_json::Value>,
}

enum ExpansionPlan {
    Reusable(Box<ReusableExpansionInputs>),
    Matrix(Box<MatrixExpansionInputs>),
}

/// One fully built tx job, still detached from the run.
struct BuiltJob {
    plan: preloop_gha_protocol::JobPlan,
    condition_context: preloop_gha_expressions::Context,
    artifacts: crate::runs::BuiltJobArtifacts,
}

enum BuiltExpansion {
    Reusable {
        caller_id: JobId,
        jobs: Vec<BuiltJob>,
        reusable_calls: BTreeMap<String, preloop_gha_parser::ReusableCallMetadata>,
    },
    Matrix {
        jobs: Vec<BuiltJob>,
    },
}

/// How to retire the request correlation an expandable node minted at submit.
#[derive(Clone, Copy)]
pub enum RequestRetirement {
    /// The node stays in the run as a terminal job: record the result and drop
    /// the live claim state (inflight + session claims), exactly as the
    /// completion path does for a job a runner actually finished.
    ///
    /// `id_token_grants` and `oidc_job_contexts` are deliberately kept: no
    /// path outside the Purge arm ever removes them, for any job — completed
    /// and cancelled real jobs keep theirs for the life of the process and
    /// the store, so a settled placeholder keeps the same shape.
    Settle(ExecutionStatus),
    /// The node no longer exists in the run, so nothing can reference it
    /// again: every correlation entry goes.
    Purge,
}

/// Build and apply every deferred subtree, then keep promoting until the
/// scheduler is quiet.
///
/// The build phase deliberately runs with the global lock released: it parses
/// workflow YAML and constructs a runner message plus a runtime token per
/// tx job, which scales with the width of the callee matrix. Holding the
/// mutex across that stalls every other request.
pub async fn drain_expansions(shared: &Arc<SharedState>) -> SchedulingOutcome {
    let mut outcome = SchedulingOutcome::default();
    loop {
        // Phase 1 (transactional): atomically claim one pending-expansion
        // node and snapshot its build plan under a generation fence. The
        // claim is a single command — no clone-front needed, the fence is
        // what discards a stale build at apply time.
        let claim =
            match crate::control::backend::ControlBackend::claim_expansion(&*shared.state.backend)
                .await
            {
                Ok(Some(claim)) => claim,
                Ok(None) => return outcome,
                Err(error) => {
                    tracing::warn!(?error, "drain_expansions: claim failed");
                    return outcome;
                }
            };

        // Phase 2 (unlocked): the expensive part — parse workflow YAML, build
        // one runner message per tx job, mint runtime tokens. Runs against
        // SharedState, never inside the scheduling transaction.
        let built = match claim.plan {
            Some(plan) => crate::control::sched::build_expansion(shared, plan),
            None => {
                tracing::warn!(
                    run_id = %claim.job.run_id,
                    job = %claim.job.job_id,
                    "expansion inputs vanished before build"
                );
                Err(ExecutionStatus::Failure)
            }
        };

        // Phase 3 (transactional): fold the built subtree back under the
        // claim's generation fence, then promote whatever it unblocked. A
        // stale generation (node cancelled/re-leased mid-build) discards the
        // build inside the command.
        match crate::control::backend::ControlBackend::apply_expansion(
            &*shared.state.backend,
            crate::control::backend::ExpansionApply {
                job: claim.job,
                generation: claim.generation,
                built,
            },
        )
        .await
        {
            Ok(promoted) => outcome.merge(promoted),
            Err(error) => {
                tracing::warn!(?error, "drain_expansions: apply failed");
                return outcome;
            }
        }

        // Refresh node-local scheduling mirrors from committed state.
        if let Ok((queue_len, labels)) = shared
            .state
            .backend
            .read_scoped(
                &crate::control::txstate::TxScope::runs(Default::default()),
                |tx| {
                    Ok((
                        tx.ready_count.max(0) as usize,
                        crate::control::sched::next_job_labels(tx),
                    ))
                },
            )
            .await
        {
            shared
                .state
                .queue_depth
                .store(queue_len, std::sync::atomic::Ordering::Release);
            *shared.state.next_job_runs_on.write().unwrap() = labels;
        }
    }
}

pub fn aggregate_need_status(statuses: &[ExecutionStatus]) -> Option<ExecutionStatus> {
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

pub fn need_context(run: &RunRecord, need: &JobId) -> Option<azdo::PipelineContextData> {
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

pub fn status_string(status: ExecutionStatus) -> String {
    match status {
        ExecutionStatus::Queued | ExecutionStatus::Pending | ExecutionStatus::InProgress => {
            "in_progress"
        }
        ExecutionStatus::Success => "success",
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
pub fn finalize_run_if_complete(run: &mut RunRecord) {
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

pub fn summarize_run(statuses: impl Iterator<Item = ExecutionStatus>) -> ExecutionStatus {
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
    } else if !statuses.is_empty()
        && statuses
            .iter()
            .all(|status| *status == ExecutionStatus::Skipped)
    {
        // Every job was gated off: GitHub reports the run itself as skipped,
        // not green, so a run of `if:`-excluded jobs cannot read as a pass.
        ExecutionStatus::Skipped
    } else {
        ExecutionStatus::Success
    }
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
}

#[cfg(test)]
mod assignment_tests {
    use super::*;

    /// Status pairings come from claimed job requests carrying their runner,
    /// not from the pre-claim assignment table: finished requests and
    /// unclaimed ones must not appear, and output is runner-sorted.
    #[test]
    fn live_assignments_reflect_claimed_requests() {
        use crate::models::TaskAgentJobRequestRecord;
        use std::collections::BTreeMap;

        fn record(
            run_id: RunId,
            job: &str,
            owner: Option<i64>,
            result: Option<preloop_gha_protocol::ExecutionStatus>,
            started_ago_secs: u64,
        ) -> TaskAgentJobRequestRecord {
            TaskAgentJobRequestRecord {
                request_id: 0,
                run_id,
                job_id: JobId(job.to_owned()),
                agent_job_id: uuid::Uuid::nil(),
                plan_id: String::new(),
                plan_type: String::new(),
                timeline_id: uuid::Uuid::nil(),
                result,
                locked_until: String::new(),
                claimed_at: None,
                owner_runner_id: owner,
                started_at: std::time::SystemTime::now()
                    .checked_sub(std::time::Duration::from_secs(started_ago_secs)),
                last_renewed_at: None,
                timeout_triggered: false,
                debug_token_issued: false,
            }
        }

        let run_a = RunId::new();
        let mut table: BTreeMap<i64, TaskAgentJobRequestRecord> = BTreeMap::new();
        // Finished requests stay in the map (late reads stay bound) but must
        // not report as live; unclaimed ones have no runner yet.
        table.insert(
            1,
            record(
                run_a,
                "done",
                Some(3),
                Some(preloop_gha_protocol::ExecutionStatus::Success),
                90,
            ),
        );
        table.insert(2, record(RunId::new(), "queued", None, None, 10));
        // Insert out of runner order; output must still be runner-sorted.
        table.insert(3, record(RunId::new(), "build", Some(7), None, 90));
        let run_test = RunId::new();
        table.insert(4, record(run_test, "test", Some(3), None, 30));

        let active = BTreeMap::from([
            ("finished".to_owned(), 1),
            ("build".to_owned(), 3),
            ("test".to_owned(), 4),
        ]);
        let now = std::time::SystemTime::now();
        let live = live_runner_assignments(&table, &active, now);

        assert_eq!(live.len(), 2);
        assert_eq!(live[0].runner_id, 3);
        assert_eq!(live[0].run_id, run_test.to_string());
        assert_eq!(live[0].job_id, "test");
        assert!((live[0].assigned_seconds_ago - 30.0).abs() < 5.0);
        assert_eq!(live[1].runner_id, 7);
        assert_eq!(live[1].job_id, "build");
        assert!(live_runner_assignments(&BTreeMap::new(), &BTreeMap::new(), now).is_empty());
    }

    fn self_hosted_caps() -> RunnerCapabilities {
        RunnerCapabilities {
            known: true,
            labels: vec!["self-hosted".to_owned()],
            runner_group_id: None,
            runner_group_name: None,
        }
    }

    fn test_queued_job(job_id: &str) -> QueuedJob {
        let created_at_unix_nanos = crate::models::now_unix_nanos();
        QueuedJob {
            run_id: RunId::new(),
            job_id: JobId(job_id.to_owned()),
            base_id: job_id.to_owned(),
            created_at_unix_nanos,
            dependencies_ready_at_unix_nanos: Some(created_at_unix_nanos),
            concurrency_wait_started_at_unix_nanos: None,
            concurrency_acquired_at_unix_nanos: None,
            enqueued_at_unix_nanos: 0,
            needs: Vec::new(),
            if_condition: None,
            condition_context: preloop_gha_expressions::Context::default(),
            max_parallel: None,
            runs_on: vec!["self-hosted".to_owned()],
            runner_group: None,
            environment: None,
            message: serde_json::from_value(serde_json::json!({
                "jobId": "00000000-0000-0000-0000-000000000001",
                "requestId": 1,
                "plan": {"planId": "plan", "planType": "build", "version": 1, "artifactUri": "", "artifactLocation": ""},
                "timeline": {"id": "00000000-0000-0000-0000-000000000002", "changeId": 0, "location": null},
                "jobName": job_id,
                "lockedUntil": "",
                "resources": {"endpoints": []},
                "steps": [],
                "snapshot": null
            }))
            .unwrap(),
            concurrency: None,
            matrix: BTreeMap::new(),
            deferred_matrix: None,
            reusable_call: None,
                environment_gate: None,
        }
    }

    #[test]
    fn lifecycle_timestamps_record_and_round_trip() {
        let mut job = test_queued_job("build");
        assert!(job.created_at_unix_nanos > 0);
        assert!(job.dependencies_ready_at_unix_nanos.is_some());
        assert_eq!(job.concurrency_wait_started_at_unix_nanos, None);
        assert_eq!(job.concurrency_acquired_at_unix_nanos, None);
        assert_eq!(job.enqueued_at_unix_nanos, 0);

        stamp_concurrency_wait_started(&mut job);
        let wait_started = job.concurrency_wait_started_at_unix_nanos;
        assert!(wait_started.is_some());

        stamp_concurrency_acquired(&mut job);
        let acquired = job.concurrency_acquired_at_unix_nanos;
        assert!(acquired.is_some());

        stamp_ready_enqueue(&mut job);
        assert!(job.enqueued_at_unix_nanos > 0);
        assert_eq!(job.concurrency_wait_started_at_unix_nanos, wait_started);
        assert_eq!(job.concurrency_acquired_at_unix_nanos, acquired);

        let restored: QueuedJob =
            serde_json::from_slice(&serde_json::to_vec(&job).unwrap()).unwrap();
        assert_eq!(restored.created_at_unix_nanos, job.created_at_unix_nanos);
        assert_eq!(
            restored.dependencies_ready_at_unix_nanos,
            job.dependencies_ready_at_unix_nanos
        );
        assert_eq!(
            restored.concurrency_wait_started_at_unix_nanos,
            job.concurrency_wait_started_at_unix_nanos
        );
        assert_eq!(
            restored.concurrency_acquired_at_unix_nanos,
            job.concurrency_acquired_at_unix_nanos
        );
        assert_eq!(restored.enqueued_at_unix_nanos, job.enqueued_at_unix_nanos);
    }

    /// A job whose `runs-on` reads `needs.*` is queued with the raw template
    /// and the label must be finished once the needed job completes. The
    /// server used to evaluate it at submission time, when the output did not
    /// exist yet, so the label resolved to "" and the job starved every time.
    #[test]
    fn promote_resolves_needs_runs_on_after_needs_complete() {
        let run_id = RunId::new();
        let plan_id = JobId("plan".to_owned());
        let release_id = JobId("release".to_owned());

        let mut release = test_queued_job("release");
        release.run_id = run_id;
        release.needs = vec![plan_id.clone()];
        release.runs_on =
            vec!["${{ fromJSON(format('[\"{0}\"]', needs.plan.outputs.runner)) }}".to_owned()];

        let mut inner = InnerState {
            ..Default::default()
        };
        inner.runs.insert(
            run_id,
            RunRecord {
                run_id,
                webhook_delivery_id: None,
                run_name: None,
                submission: Arc::new(preloop_gha_protocol::WorkflowSubmission::default()),
                jobs: BTreeMap::from([
                    (plan_id.clone(), ExecutionStatus::Success),
                    (release_id.clone(), ExecutionStatus::Queued),
                ]),
                job_outputs: BTreeMap::from([(
                    plan_id.clone(),
                    BTreeMap::from([("runner".to_owned(), serde_json::json!("ubuntu-latest"))]),
                )]),
                job_base_ids: BTreeMap::from([
                    (plan_id.clone(), "plan".to_owned()),
                    (release_id.clone(), "release".to_owned()),
                ]),
                job_needs: BTreeMap::from([(release_id.clone(), vec![plan_id.clone()])]),
                caller_plans: BTreeMap::new(),
                job_names: BTreeMap::new(),
                github: serde_json::Value::Null,
                head_sha: String::new(),
                workflow_ref: String::new(),
                workspace_snapshot: None,
                job_fail_fast: BTreeMap::new(),
                job_continue_on_error: BTreeMap::new(),
                job_check_run_ids: BTreeMap::new(),
                reports_check_runs: false,
                reusable_calls: BTreeMap::new(),
                jobs_list: Vec::new(),
                created_at: chrono::Utc::now(),
                started_at: None,
                completed_at: None,
                run_number: 1,
                run_attempt: 1,
                workflow_path_str: String::new(),
                event: "push".to_owned(),
                conclusion: None,
                push_state: None,
                snapshot_timing: None,
                fork_approval_pending: false,
                fork_approval_requested_at_unix_nanos: None,
                fork_approved_at_unix_nanos: None,
                fork_approval_note: None,
                status: ExecutionStatus::Queued,
            },
        );
        inner.pending_jobs.push_back(release);

        let rules: crate::config::EnvironmentRulesMap = BTreeMap::new();
        let outcome = promote_ready_jobs(&mut inner, &rules, &[]);

        assert_eq!(outcome.promoted, 1);
        let queued = inner.queue.front().expect("promoted job must be queued");
        assert_eq!(queued.runs_on, vec!["ubuntu-latest".to_owned()]);
    }

    /// A deferred label whose output is genuinely missing resolves to an empty
    /// label, which is kept: an empty label matches no runner, so the job
    /// starves the way the official service leaves a job with an unmatchable
    /// label. Dropping it would leave `runs_on` empty, and an empty label
    /// list matches every runner — the job must not silently run on an
    /// arbitrary machine.
    #[test]
    fn promote_keeps_needs_runs_on_label_that_resolves_empty() {
        let run_id = RunId::new();
        let plan_id = JobId("plan".to_owned());
        let release_id = JobId("release".to_owned());

        let mut release = test_queued_job("release");
        release.run_id = run_id;
        release.needs = vec![plan_id.clone()];
        release.runs_on = vec!["${{ needs.plan.outputs.runner }}".to_owned()];

        let mut inner = InnerState {
            ..Default::default()
        };
        inner.runs.insert(
            run_id,
            RunRecord {
                run_id,
                webhook_delivery_id: None,
                run_name: None,
                submission: Arc::new(preloop_gha_protocol::WorkflowSubmission::default()),
                jobs: BTreeMap::from([
                    (plan_id.clone(), ExecutionStatus::Success),
                    (release_id.clone(), ExecutionStatus::Queued),
                ]),
                job_outputs: BTreeMap::new(),
                job_base_ids: BTreeMap::from([
                    (plan_id.clone(), "plan".to_owned()),
                    (release_id.clone(), "release".to_owned()),
                ]),
                job_needs: BTreeMap::from([(release_id.clone(), vec![plan_id.clone()])]),
                caller_plans: BTreeMap::new(),
                job_names: BTreeMap::new(),
                github: serde_json::Value::Null,
                head_sha: String::new(),
                workflow_ref: String::new(),
                workspace_snapshot: None,
                job_fail_fast: BTreeMap::new(),
                job_continue_on_error: BTreeMap::new(),
                job_check_run_ids: BTreeMap::new(),
                reports_check_runs: false,
                reusable_calls: BTreeMap::new(),
                jobs_list: Vec::new(),
                created_at: chrono::Utc::now(),
                started_at: None,
                completed_at: None,
                run_number: 1,
                run_attempt: 1,
                workflow_path_str: String::new(),
                event: "push".to_owned(),
                conclusion: None,
                push_state: None,
                snapshot_timing: None,
                fork_approval_pending: false,
                fork_approval_requested_at_unix_nanos: None,
                fork_approved_at_unix_nanos: None,
                fork_approval_note: None,
                status: ExecutionStatus::Queued,
            },
        );
        inner.pending_jobs.push_back(release);

        let rules: crate::config::EnvironmentRulesMap = BTreeMap::new();
        let outcome = promote_ready_jobs(&mut inner, &rules, &[]);

        assert_eq!(outcome.promoted, 1);
        let queued = inner.queue.front().expect("promoted job must be queued");
        assert_eq!(queued.runs_on, vec![String::new()]);
        // The empty label must not match an arbitrary runner: the job stays
        // queued (starves) instead of running on a machine it never asked for.
        assert!(!job_matches_runner(
            &queued.runs_on,
            &["ubuntu-latest".to_owned()]
        ));
    }

    /// A deferred label that resolves to a platform no registered runner can
    /// host fails at promotion, mirroring the submit-time check that raw
    /// templates skip: the labels only became known now, so this is their
    /// first validation. Without it the job would queue behind a host that
    /// may never appear, and its dependents would never see a terminal
    /// status.
    #[test]
    fn promote_fails_deferred_runs_on_resolving_to_unhostable_platform() {
        let run_id = RunId::new();
        let plan_id = JobId("plan".to_owned());
        let release_id = JobId("release".to_owned());

        let mut release = test_queued_job("release");
        release.run_id = run_id;
        release.needs = vec![plan_id.clone()];
        release.runs_on = vec!["${{ needs.plan.outputs.runner }}".to_owned()];

        let mut inner = InnerState {
            ..Default::default()
        };
        inner.runs.insert(
            run_id,
            RunRecord {
                run_id,
                webhook_delivery_id: None,
                run_name: None,
                submission: Arc::new(preloop_gha_protocol::WorkflowSubmission::default()),
                jobs: BTreeMap::from([
                    (plan_id.clone(), ExecutionStatus::Success),
                    (release_id.clone(), ExecutionStatus::Queued),
                ]),
                job_outputs: BTreeMap::from([(
                    plan_id.clone(),
                    BTreeMap::from([("runner".to_owned(), serde_json::json!("windows-latest"))]),
                )]),
                job_base_ids: BTreeMap::from([
                    (plan_id.clone(), "plan".to_owned()),
                    (release_id.clone(), "release".to_owned()),
                ]),
                job_needs: BTreeMap::from([(release_id.clone(), vec![plan_id.clone()])]),
                caller_plans: BTreeMap::new(),
                job_names: BTreeMap::new(),
                github: serde_json::Value::Null,
                head_sha: String::new(),
                workflow_ref: String::new(),
                workspace_snapshot: None,
                job_fail_fast: BTreeMap::new(),
                job_continue_on_error: BTreeMap::new(),
                job_check_run_ids: BTreeMap::new(),
                reports_check_runs: false,
                reusable_calls: BTreeMap::new(),
                jobs_list: Vec::new(),
                created_at: chrono::Utc::now(),
                started_at: None,
                completed_at: None,
                run_number: 1,
                run_attempt: 1,
                workflow_path_str: String::new(),
                event: "push".to_owned(),
                conclusion: None,
                push_state: None,
                snapshot_timing: None,
                fork_approval_pending: false,
                fork_approval_requested_at_unix_nanos: None,
                fork_approved_at_unix_nanos: None,
                fork_approval_note: None,
                status: ExecutionStatus::Queued,
            },
        );
        inner.pending_jobs.push_back(release);

        let rules: crate::config::EnvironmentRulesMap = BTreeMap::new();
        let outcome = promote_ready_jobs(&mut inner, &rules, &[]);

        assert_eq!(outcome.promoted, 0);
        assert!(
            outcome.failed.contains(&(run_id, release_id.clone())),
            "unhostable deferred label must fail the job"
        );
        assert!(inner.queue.is_empty(), "failed job must not be queued");
        let run = inner.runs.get(&run_id).expect("run must exist");
        assert_eq!(
            run.jobs.get(&release_id),
            Some(&ExecutionStatus::Failure),
            "dependents must see a terminal status"
        );
    }

    /// A deferred label that resolves to a concrete label the runner pool
    /// never advertised fails at promotion, mirroring the submit-time
    /// pool-label check that raw templates skip. Without it the job would
    /// sit in the queue behind runners that can never claim it.
    #[test]
    fn promote_fails_deferred_runs_on_pool_cannot_satisfy() {
        let run_id = RunId::new();
        let plan_id = JobId("plan".to_owned());
        let release_id = JobId("release".to_owned());

        let mut release = test_queued_job("release");
        release.run_id = run_id;
        release.needs = vec![plan_id.clone()];
        release.runs_on = vec!["${{ needs.plan.outputs.runner }}".to_owned()];

        let mut inner = InnerState {
            ..Default::default()
        };
        inner.runs.insert(
            run_id,
            RunRecord {
                run_id,
                webhook_delivery_id: None,
                run_name: None,
                submission: Arc::new(preloop_gha_protocol::WorkflowSubmission::default()),
                jobs: BTreeMap::from([
                    (plan_id.clone(), ExecutionStatus::Success),
                    (release_id.clone(), ExecutionStatus::Queued),
                ]),
                job_outputs: BTreeMap::from([(
                    plan_id.clone(),
                    BTreeMap::from([("runner".to_owned(), serde_json::json!("gpu-large"))]),
                )]),
                job_base_ids: BTreeMap::from([
                    (plan_id.clone(), "plan".to_owned()),
                    (release_id.clone(), "release".to_owned()),
                ]),
                job_needs: BTreeMap::from([(release_id.clone(), vec![plan_id.clone()])]),
                caller_plans: BTreeMap::new(),
                job_names: BTreeMap::new(),
                github: serde_json::Value::Null,
                head_sha: String::new(),
                workflow_ref: String::new(),
                workspace_snapshot: None,
                job_fail_fast: BTreeMap::new(),
                job_continue_on_error: BTreeMap::new(),
                job_check_run_ids: BTreeMap::new(),
                reports_check_runs: false,
                reusable_calls: BTreeMap::new(),
                jobs_list: Vec::new(),
                created_at: chrono::Utc::now(),
                started_at: None,
                completed_at: None,
                run_number: 1,
                run_attempt: 1,
                workflow_path_str: String::new(),
                event: "push".to_owned(),
                conclusion: None,
                push_state: None,
                snapshot_timing: None,
                fork_approval_pending: false,
                fork_approval_requested_at_unix_nanos: None,
                fork_approved_at_unix_nanos: None,
                fork_approval_note: None,
                status: ExecutionStatus::Queued,
            },
        );
        inner.pending_jobs.push_back(release);

        let rules: crate::config::EnvironmentRulesMap = BTreeMap::new();
        let pool_labels = vec!["ubuntu-latest".to_owned()];
        let outcome = promote_ready_jobs(&mut inner, &rules, &pool_labels);

        assert_eq!(outcome.promoted, 0);
        assert!(
            outcome.failed.contains(&(run_id, release_id.clone())),
            "deferred label the pool cannot satisfy must fail the job"
        );
        assert!(inner.queue.is_empty(), "failed job must not be queued");
        let run = inner.runs.get(&run_id).expect("run must exist");
        assert_eq!(
            run.jobs.get(&release_id),
            Some(&ExecutionStatus::Failure),
            "dependents must see a terminal status"
        );
    }

    /// A restored concurrency group whose holder's run is terminal (or
    /// missing) must not park later submissions forever.
    #[test]
    fn reconcile_concurrency_groups_drops_terminal_holders() {
        let mut tx = crate::control::txstate::TxState {
            pool_assignments_enabled: false,
            ..Default::default()
        };
        let zombie = RunId::new();
        let live = RunId::new();
        tx.runs.insert(
            live,
            RunRecord {
                run_id: live,
                webhook_delivery_id: None,
                run_name: None,
                submission: Arc::new(preloop_gha_protocol::WorkflowSubmission::default()),
                jobs: BTreeMap::from([(JobId("build".to_owned()), ExecutionStatus::Queued)]),
                job_outputs: BTreeMap::new(),
                job_base_ids: BTreeMap::new(),
                job_needs: BTreeMap::new(),
                caller_plans: BTreeMap::new(),
                job_names: BTreeMap::new(),
                github: serde_json::Value::Null,
                head_sha: String::new(),
                workflow_ref: String::new(),
                workspace_snapshot: None,
                job_fail_fast: BTreeMap::new(),
                job_continue_on_error: BTreeMap::new(),
                job_check_run_ids: BTreeMap::new(),
                reports_check_runs: false,
                reusable_calls: BTreeMap::new(),
                jobs_list: Vec::new(),
                created_at: chrono::Utc::now(),
                started_at: None,
                completed_at: None,
                run_number: 1,
                run_attempt: 1,
                workflow_path_str: String::new(),
                event: "push".to_owned(),
                conclusion: None,
                push_state: None,
                snapshot_timing: None,
                fork_approval_pending: false,
                fork_approval_requested_at_unix_nanos: None,
                fork_approved_at_unix_nanos: None,
                fork_approval_note: None,
                status: ExecutionStatus::Queued,
            },
        );
        let mut queued = test_queued_job("build");
        queued.run_id = live;
        tx.ready_index.push_back(queued);
        let group_key = ("repo".to_owned(), "g".to_owned());
        tx.concurrency_groups.insert(
            group_key.clone(),
            crate::concurrency::ConcurrencyGroup {
                display_name: "g".to_owned(),
                running: Some(crate::concurrency::Holder::Run(zombie)),
                pending: VecDeque::from([crate::concurrency::Holder::Run(live)]),
            },
        );
        tx.holder_keys.insert(zombie, vec![group_key.clone()]);
        tx.holder_keys.insert(live, vec![group_key.clone()]);

        crate::control::sched::reconcile_concurrency_groups(&mut tx);

        let group = tx.concurrency_groups.get(&group_key).unwrap();
        assert!(
            group.running.is_none(),
            "a terminal/missing holder must not keep the group occupied"
        );
        assert_eq!(
            group.pending.len(),
            1,
            "the live pending holder must survive"
        );
        assert!(
            !tx.holder_keys.contains_key(&zombie),
            "holder-key tracking for the dead run must be pruned"
        );
    }

    /// Build a deferred matrix node plan with controlled `inputs`, standing
    /// in for the node the expansion pass stored in `run.caller_plans`.
    fn stored_deferred_plan(
        id: &str,
        inputs: BTreeMap<String, serde_json::Value>,
    ) -> preloop_gha_protocol::JobPlan {
        let workflow = preloop_gha_parser::parse_workflow(
            "on: push\njobs:\n  gen:\n    runs-on: ubuntu-latest\n    strategy:\n      matrix:\n        target: [a]\n    steps:\n      - run: echo hi\n",
        )
        .expect("the test workflow must parse");
        let expanded =
            preloop_gha_parser::expand_jobs_with_reusables_and_shas_and_inputs_and_event(
                &workflow,
                &BTreeMap::new(),
                &BTreeMap::new(),
                None,
                Some("push"),
            )
            .expect("the test workflow must expand");
        let mut plan = expanded
            .jobs
            .into_iter()
            .find(|job| job.base_id == "gen")
            .expect("the gen job must expand");
        plan.id = JobId(id.to_owned());
        plan.inputs = inputs;
        plan
    }

    /// Minimal run state holding one deferred matrix node, optionally with
    /// its stored plan (as `register_expanded_jobs` keeps deferred nodes).
    fn deferred_expansion_state(
        dispatch_inputs: BTreeMap<String, serde_json::Value>,
        stored_plan: Option<preloop_gha_protocol::JobPlan>,
        job_id: &str,
    ) -> (InnerState, QueuedJob) {
        let run_id = RunId::new();
        let mut caller_plans = BTreeMap::new();
        if let Some(plan) = stored_plan {
            caller_plans.insert(JobId(job_id.to_owned()), plan);
        }
        let mut inner = InnerState {
            pool_assignments_enabled: false,
            ..Default::default()
        };
        inner.runs.insert(
            run_id,
            RunRecord {
                run_id,
                webhook_delivery_id: None,
                run_name: None,
                submission: std::sync::Arc::new(preloop_gha_protocol::WorkflowSubmission {
                    dispatch_inputs,
                    ..Default::default()
                }),
                jobs: BTreeMap::new(),
                job_outputs: BTreeMap::new(),
                job_base_ids: BTreeMap::new(),
                job_needs: BTreeMap::new(),
                caller_plans,
                job_names: BTreeMap::new(),
                github: serde_json::Value::Null,
                head_sha: String::new(),
                workflow_ref: String::new(),
                workspace_snapshot: None,
                job_fail_fast: BTreeMap::new(),
                job_continue_on_error: BTreeMap::new(),
                job_check_run_ids: BTreeMap::new(),
                reports_check_runs: false,
                reusable_calls: BTreeMap::new(),
                jobs_list: Vec::new(),
                created_at: chrono::Utc::now(),
                started_at: None,
                completed_at: None,
                run_number: 1,
                run_attempt: 1,
                workflow_path_str: String::new(),
                event: "push".to_owned(),
                conclusion: None,
                push_state: None,
                snapshot_timing: None,
                fork_approval_pending: false,
                fork_approval_requested_at_unix_nanos: None,
                fork_approved_at_unix_nanos: None,
                fork_approval_note: None,
                status: ExecutionStatus::Queued,
            },
        );
        let mut job = test_queued_job(job_id);
        job.run_id = run_id;
        job.deferred_matrix = Some("${{ fromJSON(needs.plan.outputs.targets) }}".to_owned());
        (inner, job)
    }

    fn matrix_scoped_inputs(
        inner: &InnerState,
        job: &QueuedJob,
    ) -> BTreeMap<String, serde_json::Value> {
        match plan_expansion(inner, job).expect("the deferred node must plan an expansion") {
            ExpansionPlan::Matrix(inputs) => inputs.scoped_inputs,
            ExpansionPlan::Reusable(_) => panic!("expected a matrix expansion plan"),
        }
    }

    /// A deferred matrix node fans out with its own scoped `inputs`: the
    /// root run's dispatch inputs for a top-level node, or the caller's
    /// `with` values for a node inside a reusable workflow. A callee cell
    /// must not see the root dispatch inputs, which are empty on a
    /// push-triggered run while the caller passed real values.
    #[test]
    fn matrix_expansion_uses_deferred_node_scoped_inputs() {
        // A top-level node has no stored plan; the fan-out falls back to the
        // dispatch inputs the submit path stamped on every plan.
        let dispatch = BTreeMap::from([("dry_run".to_owned(), serde_json::json!(true))]);
        let (inner, job) = deferred_expansion_state(dispatch.clone(), None, "gen");
        assert_eq!(
            matrix_scoped_inputs(&inner, &job),
            dispatch,
            "a top-level deferred node must fan out with the dispatch inputs"
        );

        // A node inside a reusable workflow carries the caller's `with`
        // values on its stored plan; the empty dispatch map of a
        // push-triggered run must not erase them.
        let with_values = BTreeMap::from([("mode".to_owned(), serde_json::json!("plan"))]);
        let plan = stored_deferred_plan("call/gen", with_values.clone());
        let (inner, job) = deferred_expansion_state(BTreeMap::new(), Some(plan), "call/gen");
        assert_eq!(
            matrix_scoped_inputs(&inner, &job),
            with_values,
            "a callee node must fan out with the caller's with values"
        );

        // When the run also has dispatch inputs, the node's own scoped
        // inputs still win over the root map.
        let plan = stored_deferred_plan("call/gen", with_values.clone());
        let (inner, job) = deferred_expansion_state(dispatch, Some(plan), "call/gen");
        assert_eq!(
            matrix_scoped_inputs(&inner, &job),
            with_values,
            "the callee-scoped inputs must win over the root dispatch inputs"
        );
    }

    #[test]
    fn strict_pool_pending_job_survives_assignment_ttl_expiry() {
        // A strict-pool job whose 10-minute assignment TTL expired with no
        // machine ever registering used to have its pool-pending mark dropped
        // by the claim-time cleanup. `claim_permitted` then fell through to the
        // permissive default, which is false in strict mode, so EVERY runner
        // was denied and the job wedged forever. The expired mark must be
        // preserved so a verified runner can take the job over.
        let mut tx = crate::control::txstate::TxState {
            pool_assignments_enabled: true,
            require_job_assignments: true,
            ..Default::default()
        };
        let job = test_queued_job("build");
        let key = (job.run_id, job.job_id.clone());
        tx.ready_index.push_back(job);
        tx.pool_pending.insert(
            key,
            std::time::SystemTime::now() - ASSIGNMENT_TTL - std::time::Duration::from_secs(1),
        );

        let claimed =
            crate::control::sched::choose_claim_position(&mut tx, &self_hosted_caps(), Some(7))
                .and_then(|p| crate::control::sched::apply_claim(&mut tx, p));
        assert!(
            claimed.is_some(),
            "verified runner must be able to claim the expired strict-pool job"
        );
        assert!(
            tx.pool_pending.is_empty(),
            "the claim must consume the preserved mark"
        );
    }

    #[test]
    fn strict_pool_assignment_expired_is_takeable_by_a_verified_runner() {
        let mut tx = crate::control::txstate::TxState {
            pool_assignments_enabled: true,
            require_job_assignments: true,
            ..Default::default()
        };
        let job = test_queued_job("build");
        let key = (job.run_id, job.job_id.clone());
        tx.ready_index.push_back(job);
        let stale =
            std::time::SystemTime::now() - ASSIGNMENT_TTL - std::time::Duration::from_secs(1);
        tx.job_assignments.insert(
            key,
            AssignmentRecord {
                runner_id: Some(1),
                at: stale,
                first_at: stale,
            },
        );

        let claimed =
            crate::control::sched::choose_claim_position(&mut tx, &self_hosted_caps(), Some(9))
                .and_then(|p| crate::control::sched::apply_claim(&mut tx, p));
        assert!(
            claimed.is_some(),
            "verified runner must take over the expired strict-mode assignment"
        );
    }

    #[test]
    fn freshly_paired_runner_prefers_its_assignment_over_stale_fifo_work() {
        let mut tx = crate::control::txstate::TxState {
            pool_assignments_enabled: true,
            require_job_assignments: true,
            ..Default::default()
        };
        let stale_job = test_queued_job("stale");
        let stale_key = (stale_job.run_id, stale_job.job_id.clone());
        let assigned_job = test_queued_job("assigned");
        let assigned_key = (assigned_job.run_id, assigned_job.job_id.clone());
        tx.ready_index.push_back(stale_job);
        tx.ready_index.push_back(assigned_job);

        let now = std::time::SystemTime::now();
        tx.job_assignments.insert(
            stale_key.clone(),
            AssignmentRecord {
                runner_id: Some(40),
                at: now,
                first_at: now - CLAIM_BINDING_TTL - std::time::Duration::from_secs(1),
            },
        );
        tx.pool_pending.insert(stale_key, now);
        tx.job_assignments.insert(
            assigned_key,
            AssignmentRecord {
                runner_id: Some(41),
                at: now,
                first_at: now,
            },
        );

        let claimed =
            crate::control::sched::choose_claim_position(&mut tx, &self_hosted_caps(), Some(41))
                .and_then(|p| crate::control::sched::apply_claim(&mut tx, p))
                .expect("the freshly paired runner has a claimable job");

        assert_eq!(
            claimed.job_id.0, "assigned",
            "a stale takeover candidate must not displace the runner's fresh assignment"
        );
    }

    #[test]
    fn permissive_mode_still_expires_marks_into_an_open_grant() {
        // Non-strict pool: the TTL cleanup must keep dropping stale marks so an
        // expired hold falls back to ordinary permissive scheduling instead of
        // blocking a crashed pool's backlog forever.
        let mut tx = crate::control::txstate::TxState {
            pool_assignments_enabled: true,
            ..Default::default()
        };
        let job = test_queued_job("build");
        let key = (job.run_id, job.job_id.clone());
        tx.ready_index.push_back(job);
        tx.pool_pending.insert(
            key,
            std::time::SystemTime::now() - ASSIGNMENT_TTL - std::time::Duration::from_secs(1),
        );

        let claimed =
            crate::control::sched::choose_claim_position(&mut tx, &self_hosted_caps(), None)
                .and_then(|p| crate::control::sched::apply_claim(&mut tx, p));
        assert!(
            claimed.is_some(),
            "permissive mode must fall back to an open grant after the TTL"
        );
    }

    #[test]
    fn pool_job_is_not_bound_to_an_external_runner_at_queue_time() {
        // A pool-managed job must not be queue-time bound to a runner that
        // registered before the job existed without a provision token: the
        // binding would bypass the pool's provisioning contract and the job
        // would never become pool-pending. It stays pool-pending until a
        // token-backed registration pairs it.
        let mut tx = crate::control::txstate::TxState {
            pool_assignments_enabled: true,
            ..Default::default()
        };
        tx.runners.insert(
            1,
            RegisteredRunner {
                id: 1,
                name: "external".to_owned(),
                labels: vec!["self-hosted".to_owned()],
                ephemeral: true,
                public_key: None,
                runner_group_id: None,
                runner_group_name: None,
            },
        );
        tx.sessions.insert(
            "sess-1".to_owned(),
            RunnerSession {
                session_id: preloop_gha_protocol::SessionId::new(),
                runner_id: 1,
            },
        );
        let job = test_queued_job("build");
        let key = (job.run_id, job.job_id.clone());
        crate::control::sched::on_job_enqueued(&mut tx, &job);

        assert!(
            tx.pool_pending.contains_key(&key),
            "external runner must not steal the binding; the job stays pool-pending"
        );
        assert!(
            tx.job_assignments.is_empty(),
            "no assignment may be stamped for the external runner"
        );
    }

    #[test]
    fn pool_job_binds_to_a_token_proven_idle_runner_at_queue_time() {
        // The pool's own machine registered earlier with a matching provision
        // token (pair_registered_runner recorded the proof); an idle, capable,
        // proven runner is still the preferred queue-time binding.
        let mut tx = crate::control::txstate::TxState {
            pool_assignments_enabled: true,
            ..Default::default()
        };
        tx.pool_proven_runners.insert(1);
        tx.runners.insert(
            1,
            RegisteredRunner {
                id: 1,
                name: "machine-a".to_owned(),
                labels: vec!["self-hosted".to_owned()],
                ephemeral: true,
                public_key: None,
                runner_group_id: None,
                runner_group_name: None,
            },
        );
        tx.sessions.insert(
            "sess-1".to_owned(),
            RunnerSession {
                session_id: preloop_gha_protocol::SessionId::new(),
                runner_id: 1,
            },
        );
        let job = test_queued_job("build");
        let key = (job.run_id, job.job_id.clone());
        crate::control::sched::on_job_enqueued(&mut tx, &job);

        assert_eq!(
            tx.job_assignments
                .get(&key)
                .and_then(|record| record.runner_id),
            Some(1),
            "token-proven idle runner must receive the queue-time binding"
        );
        assert!(tx.pool_pending.is_empty());
    }
    #[test]
    fn continuous_runner_churn_cannot_starve_idle_verified_runners() {
        // Production incident reproduction:
        // N claimable jobs, M idle verified runners.
        // Repeated registrations keep re-stamping pool_pending marks for the jobs
        // to `now`, refreshing the 120s window constantly.
        // Once the job's enqueue ceiling (CLAIM_BINDING_TTL) passes, verified idle
        // runners must be permitted to claim the jobs rather than being locked out indefinitely.
        let mut tx = crate::control::txstate::TxState {
            pool_assignments_enabled: true,
            require_job_assignments: true,
            ..Default::default()
        };

        // Enqueue 4 jobs 150s in the past (past CLAIM_BINDING_TTL = 120s)
        let enqueued_time =
            std::time::SystemTime::now() - CLAIM_BINDING_TTL - std::time::Duration::from_secs(30);
        let enqueued_nanos = enqueued_time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        for i in 1..=4 {
            let mut job = test_queued_job(&format!("job-{i}"));
            job.enqueued_at_unix_nanos = enqueued_nanos;
            let key = (job.run_id, job.job_id.clone());
            tx.ready_index.push_back(job);
            // Simulate that runner churn just re-stamped pool_pending to `now` (0s ago)
            tx.pool_pending.insert(key, std::time::SystemTime::now());
        }

        // Register 2 verified idle runners (e.g. runner IDs 101, 102)
        tx.pool_proven_runners.insert(101);
        tx.pool_proven_runners.insert(102);
        tx.runners.insert(
            101,
            RegisteredRunner {
                id: 101,
                name: "idle-runner-1".to_owned(),
                labels: vec!["self-hosted".to_owned()],
                ephemeral: true,
                public_key: None,
                runner_group_id: None,
                runner_group_name: None,
            },
        );
        tx.runners.insert(
            102,
            RegisteredRunner {
                id: 102,
                name: "idle-runner-2".to_owned(),
                labels: vec!["self-hosted".to_owned()],
                ephemeral: true,
                public_key: None,
                runner_group_id: None,
                runner_group_name: None,
            },
        );

        // Idle verified runner 101 polls: must be able to claim a job!
        let claimed_1 =
            crate::control::sched::choose_claim_position(&mut tx, &self_hosted_caps(), Some(101))
                .and_then(|p| crate::control::sched::apply_claim(&mut tx, p));
        assert!(
            claimed_1.is_some(),
            "idle verified runner must not be starved by continuously refreshed pool_pending mark"
        );

        // Idle verified runner 102 polls: must be able to claim a job!
        let claimed_2 =
            crate::control::sched::choose_claim_position(&mut tx, &self_hosted_caps(), Some(102))
                .and_then(|p| crate::control::sched::apply_claim(&mut tx, p));
        assert!(
            claimed_2.is_some(),
            "second idle verified runner must also claim work"
        );

        assert_eq!(tx.ready_index.len(), 2);
    }

    #[test]
    fn restored_ready_job_without_enqueue_timestamp_is_not_held_by_fresh_pool_mark() {
        let mut tx = crate::control::txstate::TxState {
            pool_assignments_enabled: true,
            require_job_assignments: true,
            ..Default::default()
        };
        let job = test_queued_job("restored");
        let key = (job.run_id, job.job_id.clone());
        tx.ready_index.push_back(job);
        tx.pool_pending.insert(key, std::time::SystemTime::now());

        let claimed =
            crate::control::sched::choose_claim_position(&mut tx, &self_hosted_caps(), Some(42))
                .and_then(|p| crate::control::sched::apply_claim(&mut tx, p));
        assert!(
            claimed.is_some(),
            "a restored job without a ready timestamp must not receive a new pool grace window"
        );
    }

    #[test]
    fn stale_pool_pending_entries_for_removed_jobs_are_pruned() {
        let mut tx = crate::control::txstate::TxState {
            pool_assignments_enabled: true,
            require_job_assignments: true,
            ..Default::default()
        };
        let queued = test_queued_job("queued");
        let queued_key = (queued.run_id, queued.job_id.clone());
        let orphan = test_queued_job("orphan");
        let orphan_key = (orphan.run_id, orphan.job_id.clone());
        tx.ready_index.push_back(queued);
        tx.pool_pending
            .insert(queued_key.clone(), std::time::SystemTime::now());
        tx.pool_pending
            .insert(orphan_key.clone(), std::time::SystemTime::now());
        let stale = std::time::SystemTime::UNIX_EPOCH;
        tx.job_assignments.insert(
            orphan_key.clone(),
            AssignmentRecord {
                runner_id: Some(5),
                at: stale,
                first_at: stale,
            },
        );

        let swept =
            crate::control::sched::sweep_stale_bindings(&mut tx, std::time::SystemTime::now());
        assert_eq!(
            swept, 2,
            "orphaned assignment and pending entries should both be swept"
        );
        assert!(tx.pool_pending.contains_key(&queued_key));
        assert!(!tx.pool_pending.contains_key(&orphan_key));
        assert!(!tx.job_assignments.contains_key(&orphan_key));
    }

    #[test]
    fn sweep_stale_bindings_heals_pool_without_runner_polls() {
        let mut tx = crate::control::txstate::TxState {
            pool_assignments_enabled: true,
            require_job_assignments: true,
            ..Default::default()
        };

        let job = test_queued_job("build");
        let key = (job.run_id, job.job_id.clone());
        tx.ready_index.push_back(job);

        let stale =
            std::time::SystemTime::now() - CLAIM_BINDING_TTL - std::time::Duration::from_secs(10);
        tx.job_assignments.insert(
            key.clone(),
            AssignmentRecord {
                runner_id: Some(5),
                at: stale,
                first_at: stale,
            },
        );

        let now = std::time::SystemTime::now();
        let swept = crate::control::sched::sweep_stale_bindings(&mut tx, now);
        assert_eq!(swept, 1, "stale binding must be swept on timer");
        assert_eq!(tx.released_bindings_count, 1);
        assert!(tx.pool_pending.contains_key(&key));
    }
    #[test]
    fn released_binding_is_immediately_claimable_by_different_verified_runner() {
        // Fix 1 verification: when a binding is released (runner_id set to None),
        // any verified runner must be able to claim the job immediately,
        // without waiting for CLAIM_BINDING_TTL to elapse.
        let mut tx = crate::control::txstate::TxState {
            pool_assignments_enabled: true,
            require_job_assignments: true,
            ..Default::default()
        };

        let job = test_queued_job("build");
        let key = (job.run_id, job.job_id.clone());
        tx.ready_index.push_back(job);

        // Released binding: runner_id is None, at is fresh (0s ago).
        tx.job_assignments.insert(
            key,
            AssignmentRecord {
                runner_id: None,
                at: std::time::SystemTime::now(),
                first_at: std::time::SystemTime::now(),
            },
        );

        // Runner 42 (verified) polls: must be allowed to claim immediately!
        let claimed =
            crate::control::sched::choose_claim_position(&mut tx, &self_hosted_caps(), Some(42))
                .and_then(|p| crate::control::sched::apply_claim(&mut tx, p));
        assert!(
            claimed.is_some(),
            "released ownerless binding must be immediately claimable by any verified runner"
        );
    }

    #[test]
    fn repeated_churn_and_pairing_assigns_all_jobs_in_bounded_iterations() {
        // Production incident simulation:
        // 4 claimable jobs, 4 idle verified runners.
        // Churning registrations repeatedly call pair_registered_runner,
        // which sweeps stale bindings and marks pool_pending.
        // Under Fix 1 + Fix 2, idle verified runners claim all jobs within bounded iterations.
        let mut tx = crate::control::txstate::TxState {
            pool_assignments_enabled: true,
            require_job_assignments: true,
            ..Default::default()
        };

        // Enqueue 4 jobs with valid ready timestamps
        let now = std::time::SystemTime::now();
        for i in 1..=4 {
            let mut job = test_queued_job(&format!("job-{i}"));
            job.enqueued_at_unix_nanos = crate::models::now_unix_nanos();
            let key = (job.run_id, job.job_id.clone());
            tx.ready_index.push_back(job);
            tx.pool_pending.insert(key, now);
        }

        // Register 4 pool runners (1..=4)
        for id in 1..=4 {
            tx.pool_proven_runners.insert(id);
            tx.runners.insert(
                id,
                RegisteredRunner {
                    id,
                    name: format!("runner-{id}"),
                    labels: vec!["self-hosted".to_owned()],
                    ephemeral: true,
                    public_key: None,
                    runner_group_id: None,
                    runner_group_name: None,
                },
            );
            // Simulate churn registration
            crate::control::sched::pair_registered_runner(&mut tx, id);
        }

        // Simulate 4 verified runners polling to claim their work
        let mut assigned_count = 0;
        for id in 1..=4 {
            if crate::control::sched::choose_claim_position(&mut tx, &self_hosted_caps(), Some(id))
                .and_then(|p| crate::control::sched::apply_claim(&mut tx, p))
                .is_some()
            {
                assigned_count += 1;
            }
        }

        assert_eq!(
            assigned_count, 4,
            "all 4 jobs must be claimed by the verified runners without starvation"
        );
        assert!(tx.ready_index.is_empty());
    }
}

#[cfg(test)]
mod environment_gate_tests {
    use super::*;

    fn gate_job(env: &str) -> QueuedJob {
        let mut job = QueuedJob {
            run_id: RunId::new(),
            job_id: JobId("deploy".to_owned()),
            base_id: "deploy".to_owned(),
            created_at_unix_nanos: 0,
            dependencies_ready_at_unix_nanos: None,
            concurrency_wait_started_at_unix_nanos: None,
            concurrency_acquired_at_unix_nanos: None,
            enqueued_at_unix_nanos: 0,
            needs: Vec::new(),
            if_condition: None,
            condition_context: preloop_gha_expressions::Context::default(),
            max_parallel: None,
            runs_on: vec!["self-hosted".to_owned()],
            runner_group: None,
            message: serde_json::from_value(serde_json::json!({
                "jobId": "00000000-0000-0000-0000-000000000001",
                "requestId": 1,
                "plan": {"planId": "plan", "planType": "build", "version": 1, "artifactUri": "", "artifactLocation": ""},
                "timeline": {"id": "00000000-0000-0000-0000-000000000002", "changeId": 0, "location": null},
                "jobName": "deploy",
                "lockedUntil": "",
                "resources": {"endpoints": []},
                "variables": {},
                "mask": [],
                "steps": []
            }))
            .unwrap(),
            environment: Some(serde_json::Value::String(env.to_owned())),
            concurrency: None,
            matrix: BTreeMap::new(),
            deferred_matrix: None,
            reusable_call: None,
            environment_gate: None,
        };
        // Silence unused-mut; the caller mutates through the gate check.
        let _ = &mut job;
        job
    }

    fn rules_for(env_rules: crate::config::EnvironmentRules) -> crate::config::EnvironmentRulesMap {
        BTreeMap::from([(
            "owner/repo".to_owned(),
            BTreeMap::from([("prod".to_owned(), env_rules)]),
        )])
    }

    const NOW: i64 = 1_700_000_000_000_000_000;
    const MIN: i64 = 60_000_000_000;

    #[test]
    fn no_rules_proceeds_without_stamping() {
        let mut job = gate_job("prod");
        let outcome = check_environment_gates(
            &crate::config::EnvironmentRulesMap::new(),
            "owner/repo",
            "refs/heads/main",
            &mut job,
            NOW,
        );
        assert_eq!(outcome, EnvironmentGateOutcome::Proceed);
        assert!(
            job.environment_gate.is_none(),
            "no rules must not arm gate state"
        );
    }

    #[test]
    fn job_without_environment_proceeds() {
        let mut job = gate_job("prod");
        job.environment = None;
        let rules = rules_for(crate::config::EnvironmentRules {
            required_reviewers: 1,
            ..Default::default()
        });
        let outcome =
            check_environment_gates(&rules, "owner/repo", "refs/heads/main", &mut job, NOW);
        assert_eq!(outcome, EnvironmentGateOutcome::Proceed);
    }

    #[test]
    fn branch_policy_allows_listed_ref() {
        let mut job = gate_job("prod");
        let rules = rules_for(crate::config::EnvironmentRules {
            deployment_branches: vec!["main".to_owned()],
            ..Default::default()
        });
        let outcome =
            check_environment_gates(&rules, "owner/repo", "refs/heads/main", &mut job, NOW);
        assert_eq!(outcome, EnvironmentGateOutcome::Proceed);
    }

    #[test]
    fn branch_policy_matches_full_ref_form() {
        let mut job = gate_job("prod");
        let rules = rules_for(crate::config::EnvironmentRules {
            deployment_branches: vec!["refs/heads/main".to_owned()],
            ..Default::default()
        });
        let outcome =
            check_environment_gates(&rules, "owner/repo", "refs/heads/main", &mut job, NOW);
        assert_eq!(outcome, EnvironmentGateOutcome::Proceed);
    }

    #[test]
    fn branch_policy_denies_unlisted_ref() {
        let mut job = gate_job("prod");
        let rules = rules_for(crate::config::EnvironmentRules {
            deployment_branches: vec!["main".to_owned()],
            ..Default::default()
        });
        let outcome =
            check_environment_gates(&rules, "owner/repo", "refs/heads/feature-x", &mut job, NOW);
        assert_eq!(outcome, EnvironmentGateOutcome::Failed);
    }

    #[test]
    fn wait_timer_arms_holds_then_releases() {
        let mut job = gate_job("prod");
        let rules = rules_for(crate::config::EnvironmentRules {
            wait_timer_minutes: 10,
            ..Default::default()
        });
        // First pass arms the timer.
        let outcome =
            check_environment_gates(&rules, "owner/repo", "refs/heads/main", &mut job, NOW);
        assert_eq!(outcome, EnvironmentGateOutcome::Wait);
        let deadline = job
            .environment_gate
            .as_ref()
            .and_then(|gate| gate.wait_until_unix_nanos)
            .expect("wait deadline must be stamped");
        assert_eq!(deadline, NOW + 10 * MIN);
        // Still waiting just before the deadline.
        let outcome = check_environment_gates(
            &rules,
            "owner/repo",
            "refs/heads/main",
            &mut job,
            deadline - 1,
        );
        assert_eq!(outcome, EnvironmentGateOutcome::Wait);
        // Past the deadline the gate clears.
        let outcome = check_environment_gates(
            &rules,
            "owner/repo",
            "refs/heads/main",
            &mut job,
            deadline + 1,
        );
        assert_eq!(outcome, EnvironmentGateOutcome::Proceed);
        assert!(
            job.environment_gate
                .as_ref()
                .and_then(|gate| gate.wait_until_unix_nanos)
                .is_none(),
            "satisfied timer must be cleared"
        );
    }

    #[test]
    fn wait_timer_releases_when_rules_removed() {
        let mut job = gate_job("prod");
        let rules = rules_for(crate::config::EnvironmentRules {
            wait_timer_minutes: 10,
            ..Default::default()
        });
        let outcome =
            check_environment_gates(&rules, "owner/repo", "refs/heads/main", &mut job, NOW);
        assert_eq!(outcome, EnvironmentGateOutcome::Wait);
        // The config file is the source of truth: removing the rules
        // releases the armed gate and clears its stamps.
        let outcome = check_environment_gates(
            &crate::config::EnvironmentRulesMap::new(),
            "owner/repo",
            "refs/heads/main",
            &mut job,
            NOW + MIN,
        );
        assert_eq!(outcome, EnvironmentGateOutcome::Proceed);
        assert!(job.environment_gate.is_none());
    }

    #[test]
    fn approval_releases_on_single_operator_confirmation() {
        // Config validation caps required_reviewers at 1 (no user identities
        // exist), so one operator approval must release the gate.
        let mut job = gate_job("prod");
        let rules = rules_for(crate::config::EnvironmentRules {
            required_reviewers: 1,
            ..Default::default()
        });
        let outcome =
            check_environment_gates(&rules, "owner/repo", "refs/heads/main", &mut job, NOW);
        assert_eq!(outcome, EnvironmentGateOutcome::Wait);
        assert!(
            job.environment_gate
                .as_ref()
                .and_then(|gate| gate.approval_requested_at_unix_nanos)
                == Some(NOW)
        );
        // The single operator confirmation releases the gate.
        job.environment_gate
            .as_mut()
            .unwrap()
            .approvals_unix_nanos
            .push(NOW + 1);
        let outcome =
            check_environment_gates(&rules, "owner/repo", "refs/heads/main", &mut job, NOW + 2);
        assert_eq!(outcome, EnvironmentGateOutcome::Proceed);
    }

    #[test]
    fn approval_window_expires_fail_closed() {
        let mut job = gate_job("prod");
        let rules = rules_for(crate::config::EnvironmentRules {
            required_reviewers: 1,
            ..Default::default()
        });
        let outcome =
            check_environment_gates(&rules, "owner/repo", "refs/heads/main", &mut job, NOW);
        assert_eq!(outcome, EnvironmentGateOutcome::Wait);
        // 25 hours later with no approval: fail closed.
        let outcome = check_environment_gates(
            &rules,
            "owner/repo",
            "refs/heads/main",
            &mut job,
            NOW + 25 * 60 * MIN,
        );
        assert_eq!(outcome, EnvironmentGateOutcome::Failed);
    }

    #[test]
    fn approval_gate_releases_when_rules_removed() {
        let mut job = gate_job("prod");
        let rules = rules_for(crate::config::EnvironmentRules {
            required_reviewers: 1,
            ..Default::default()
        });
        let outcome =
            check_environment_gates(&rules, "owner/repo", "refs/heads/main", &mut job, NOW);
        assert_eq!(outcome, EnvironmentGateOutcome::Wait);
        // Rules removed mid-wait: the armed approval gate is released.
        let outcome = check_environment_gates(
            &crate::config::EnvironmentRulesMap::new(),
            "owner/repo",
            "refs/heads/main",
            &mut job,
            NOW + MIN,
        );
        assert_eq!(outcome, EnvironmentGateOutcome::Proceed);
    }

    #[test]
    fn object_form_environment_name_matches_rules() {
        let mut job = gate_job("prod");
        job.environment = Some(serde_json::json!({"name": "prod", "url": "https://example.com"}));
        let rules = rules_for(crate::config::EnvironmentRules {
            deployment_branches: vec!["main".to_owned()],
            ..Default::default()
        });
        let outcome =
            check_environment_gates(&rules, "owner/repo", "refs/heads/other", &mut job, NOW);
        assert_eq!(outcome, EnvironmentGateOutcome::Failed);
    }

    #[test]
    fn rules_parse_from_toml() {
        let config: crate::config::ConfigFile = toml::from_str(
            r#"
[environment_rules."owner/repo".prod]
deployment_branches = ["main"]
wait_timer_minutes = 10
required_reviewers = 1
[environment_rules."owner/repo".staging]
"#,
        )
        .expect("rules table must parse");
        let prod = &config.environment_rules["owner/repo"]["prod"];
        assert_eq!(prod.deployment_branches, vec!["main"]);
        assert_eq!(prod.wait_timer_minutes, 10);
        assert_eq!(prod.required_reviewers, 1);
        let staging = &config.environment_rules["owner/repo"]["staging"];
        assert_eq!(*staging, crate::config::EnvironmentRules::default());
    }

    #[test]
    fn unknown_rule_field_is_rejected() {
        let result = toml::from_str::<crate::config::ConfigFile>(
            r#"
[environment_rules."owner/repo".prod]
bogus_field = true
"#,
        );
        assert!(result.is_err(), "unknown rule fields must fail closed");
    }

    #[test]
    fn gate_state_survives_job_serialization_round_trip() {
        // QueuedJob snapshots (payload_blob) are the restart path: the gate
        // stamps must survive JSON serialization so a restart re-arms an
        // armed wait timer / approval gate instead of dropping it.
        let mut job = gate_job("prod");
        job.environment_gate = Some(EnvironmentGateState {
            wait_until_unix_nanos: Some(NOW + 10 * MIN),
            approval_requested_at_unix_nanos: Some(NOW),
            approvals_unix_nanos: vec![NOW + 1],
        });
        let round_tripped: QueuedJob =
            serde_json::from_value(serde_json::to_value(&job).unwrap()).unwrap();
        let gate = round_tripped.environment_gate.expect("gate must persist");
        assert_eq!(gate.wait_until_unix_nanos, Some(NOW + 10 * MIN));
        assert_eq!(gate.approval_requested_at_unix_nanos, Some(NOW));
        assert_eq!(gate.approvals_unix_nanos, vec![NOW + 1]);
    }

    #[test]
    fn missing_table_means_no_rules() {
        let config: crate::config::ConfigFile =
            toml::from_str("[environments]\n\"owner/repo\" = [\"prod\"]\n")
                .expect("bare registry must still parse");
        assert!(config.environment_rules.is_empty());
    }
}
