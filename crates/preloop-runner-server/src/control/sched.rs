//! Shared scheduling helpers for both control backends: pure functions
//! over plain arguments (no working set, no I/O) plus the expansion-build
//! machinery that produces [`BuiltExpansion`] for `apply_expansion`.
//! Backend-neutral so SQLite and Postgres can never diverge on policy.

use crate::models::{QueuedJob, RunRecord};
use crate::state::JobSetGate;
use preloop_gha_protocol::azdo;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId, WorkflowSubmission};
use std::collections::BTreeMap;
use std::sync::Arc;

// Re-export the pure helpers the ported code shares with the old module.
pub(crate) use crate::runtime_scheduling::{
    aggregate_need_status, matching_need_ids, matching_need_statuses, need_context,
    needs_json_context, DependencyDecision, SchedulingOutcome,
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

/// Promote or skip pending jobs once every declared dependency is terminal
/// (ported verbatim; `tx.pending_jobs` is the working set's pending list —
/// scoped to the affected runs, which is the only set a completion can
/// unblock since `needs:` never cross runs).
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
    F: Fn(
        &preloop_gha_protocol::JobPlan,
        &BTreeMap<String, String>,
    ) -> preloop_gha_expressions::Context,
{
    let base_url = crate::broker::runner_base_url();
    let normalized_github =
        preloop_gha_parser::job_builder::normalize_github_context(&ctx.github_json);
    let secrets_exposed = preloop_gha_protocol::masking::expose_all(&ctx.submission.secrets);
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
