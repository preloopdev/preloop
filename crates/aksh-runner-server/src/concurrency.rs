//! GitHub Actions `concurrency:` group enforcement (control-plane only).
//!
//! Runners never learn about concurrency groups; they only observe
//! `JobCancellation` when a holder is cancelled.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use aksh_gha_expressions::{eval_bool, Context};
use aksh_gha_parser::eval::{build_context, resolve_string};
use aksh_gha_parser::{Concurrency, ConcurrencyQueue};
use aksh_gha_protocol::{azdo, ExecutionStatus, JobId, RunId};
use serde_json::{json, Value};
use tracing::warn;

/// Official cancel grace period body value (TimeSpan).
pub const CANCEL_TIMEOUT: &str = "00:05:00";

/// A concurrency-group holder (workflow run, single job, or reusable JobSet).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Holder {
    /// Workflow-level concurrency covers an entire run.
    Run(RunId),
    /// Job-level concurrency covers one job.
    Job { run_id: RunId, job_id: JobId },
    /// Reusable workflow invocation (caller/embedded) covers a set of jobs.
    JobSet {
        run_id: RunId,
        job_ids: BTreeSet<JobId>,
    },
}

impl Holder {
    pub fn run_id(&self) -> RunId {
        match self {
            Self::Run(id) => *id,
            Self::Job { run_id, .. } | Self::JobSet { run_id, .. } => *run_id,
        }
    }

    pub fn contains_job(&self, run_id: RunId, job_id: &JobId) -> bool {
        match self {
            Self::Run(id) => *id == run_id,
            Self::Job {
                run_id: r,
                job_id: j,
            } => *r == run_id && j == job_id,
            Self::JobSet { run_id: r, job_ids } => *r == run_id && job_ids.contains(job_id),
        }
    }

    pub fn is_run_holder(&self, run_id: RunId) -> bool {
        matches!(self, Self::Run(id) if *id == run_id)
            || matches!(self, Self::JobSet { run_id: r, .. } if *r == run_id)
    }
}

/// One concurrency group (repo + group name, case-insensitive key).
#[derive(Debug, Clone, Default)]
pub struct ConcurrencyGroup {
    /// Display-case group name as first evaluated.
    pub display_name: String,
    pub running: Option<Holder>,
    pub pending: VecDeque<Holder>,
}

/// Which GitHub Actions scope a concurrency expression is evaluated in.
///
/// Workflow scope allows `github`, `inputs`, `vars` only.
/// Job scope additionally allows `needs`, `strategy`, `matrix`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcurrencyScope {
    Workflow,
    Job,
}

/// Typed input for concurrency expression evaluation.
pub struct ConcurrencyContext<'a> {
    pub scope: ConcurrencyScope,
    pub github: &'a Value,
    pub vars: &'a BTreeMap<String, String>,
    pub inputs: &'a BTreeMap<String, Value>,
    /// Job scope only — `matrix.*` values.
    pub matrix: Option<&'a BTreeMap<String, Value>>,
    /// Job scope only — `strategy` context object.
    pub strategy: Option<&'a Value>,
    /// Job scope only — `needs.<job>.result/outputs`.
    pub needs: Option<&'a Value>,
}

/// Evaluate a raw concurrency config against a typed scope-aware context.
///
/// Returns `(group_name, cancel_in_progress, queue_mode)` or an error string.
/// Errors include expression evaluation failures, empty group names, and the
/// `queue: max` + `cancel-in-progress: true` incompatibility (GitHub invariant 3).
pub fn evaluate_concurrency(
    raw: &Concurrency,
    ctx: &ConcurrencyContext<'_>,
) -> Result<(String, bool, ConcurrencyQueue), String> {
    let expr_ctx = build_eval_context(ctx);
    let group = resolve_string(&raw.group, &expr_ctx)?;
    let cancel = match &raw.cancel_in_progress {
        None => false,
        Some(expr) => eval_bool(expr, &expr_ctx).map_err(|e| format!("{e}"))?,
    };
    if cancel && raw.queue == ConcurrencyQueue::Max {
        return Err("queue: max and cancel-in-progress: true are incompatible".to_owned());
    }
    Ok((group, cancel, raw.queue))
}

/// Build the expression `Context` for a concurrency evaluation, enforcing the
/// GitHub scope allowlist: workflow scope never receives `matrix`, `strategy`,
/// or `needs`; job scope receives all allowed contexts.
fn build_eval_context(ctx: &ConcurrencyContext<'_>) -> Context {
    match ctx.scope {
        ConcurrencyScope::Workflow => {
            // Only github, inputs, vars are valid at workflow scope.
            let mut c = Context::default();
            c.insert("github", ctx.github.clone());
            let vars_val = Value::Object(
                ctx.vars
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                    .collect(),
            );
            c.insert("vars", vars_val);
            let inputs_val = Value::Object(
                ctx.inputs
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            );
            c.insert("inputs", inputs_val);
            c
        }
        ConcurrencyScope::Job => {
            let empty_matrix = indexmap::IndexMap::new();
            let matrix_im: indexmap::IndexMap<String, Value> = ctx
                .matrix
                .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or(empty_matrix);
            let empty_strategy = json!({});
            let strategy = ctx.strategy.unwrap_or(&empty_strategy);
            let mut c = build_context(
                ctx.github,
                &BTreeMap::new(),
                ctx.vars,
                &matrix_im,
                strategy,
                &BTreeMap::new(),
                ctx.inputs,
            );
            if let Some(needs) = ctx.needs {
                c.insert("needs", needs.clone());
            }
            c
        }
    }
}

/// Lowercased (repo, group) key.
pub fn concurrency_key(repo: &str, group: &str) -> (String, String) {
    (repo.to_ascii_lowercase(), group.to_ascii_lowercase())
}

/// Parse concurrency fields stored on a queued job plan.
pub fn concurrency_from_plan_fields(
    group: Option<&str>,
    cancel: Option<&str>,
    queue: Option<&str>,
) -> Option<Concurrency> {
    let group = group?.to_owned();
    let queue = match queue.unwrap_or("single") {
        "max" => ConcurrencyQueue::Max,
        _ => ConcurrencyQueue::Single,
    };
    Some(Concurrency {
        group,
        cancel_in_progress: cancel.map(|s| s.to_owned()),
        queue,
    })
}

/// Official JobCancellation body.
pub fn job_cancel_body(agent_job_id: uuid::Uuid) -> String {
    json!({
        "jobId": agent_job_id,
        "timeout": CANCEL_TIMEOUT,
    })
    .to_string()
}

/// Build a cancellation NDJSON reason helper.
pub fn pending_reason() -> Option<String> {
    Some("concurrency_pending".to_owned())
}

pub fn cancelled_reason() -> Option<String> {
    Some("concurrency_cancelled".to_owned())
}

/// Whether a status is non-terminal for concurrency release purposes.
pub fn is_terminal(status: ExecutionStatus) -> bool {
    matches!(
        status,
        ExecutionStatus::Success
            | ExecutionStatus::Failure
            | ExecutionStatus::Cancelled
            | ExecutionStatus::Skipped
    )
}

/// Whether a job status is still awaiting assignment (queued or concurrency-pending).
pub fn is_awaiting_execution(status: ExecutionStatus) -> bool {
    matches!(status, ExecutionStatus::Queued | ExecutionStatus::Pending)
}

/// Helper: all jobs of a holder are terminal?
pub fn holder_is_terminal(holder: &Holder, jobs: &BTreeMap<JobId, ExecutionStatus>) -> bool {
    match holder {
        Holder::Run(_) => jobs.values().all(|s| is_terminal(*s)),
        Holder::Job { job_id, .. } => jobs.get(job_id).copied().is_some_and(is_terminal),
        Holder::JobSet { job_ids, .. } => job_ids
            .iter()
            .all(|id| jobs.get(id).copied().is_some_and(is_terminal)),
    }
}

/// Max pending holders for `queue: max`.
pub const QUEUE_MAX_PENDING: usize = 100;

/// Decide how an arrival joins a contended group under its own queue mode.
/// Returns holders that should be cancelled, and whether the arrival itself is cancelled.
#[derive(Debug)]
pub struct QueueJoinResult {
    /// Existing pending holders to cancel (queue: single replacement).
    pub cancel_pending: Vec<Holder>,
    /// If true, cancel the arrival instead of parking it (queue: max overflow).
    pub cancel_arrival: bool,
    /// If true, park the arrival as pending.
    pub park_arrival: bool,
}

pub fn apply_queue_mode(
    queue: ConcurrencyQueue,
    existing_pending: &VecDeque<Holder>,
) -> QueueJoinResult {
    match queue {
        ConcurrencyQueue::Single => QueueJoinResult {
            cancel_pending: existing_pending.iter().cloned().collect(),
            cancel_arrival: false,
            park_arrival: true,
        },
        ConcurrencyQueue::Max => {
            if existing_pending.len() >= QUEUE_MAX_PENDING {
                QueueJoinResult {
                    cancel_pending: Vec::new(),
                    cancel_arrival: true,
                    park_arrival: false,
                }
            } else {
                QueueJoinResult {
                    cancel_pending: Vec::new(),
                    cancel_arrival: false,
                    park_arrival: true,
                }
            }
        }
    }
}

/// Build a needs context JSON object from run job outputs for expression eval.
pub fn needs_json_from_context_data(needs: &BTreeMap<String, azdo::PipelineContextData>) -> Value {
    // Best-effort: expose result/outputs as JSON for expression evaluation.
    let mut map = serde_json::Map::new();
    for (k, v) in needs {
        map.insert(k.clone(), context_data_to_json(v));
    }
    Value::Object(map)
}

pub fn context_data_to_json(data: &azdo::PipelineContextData) -> Value {
    match data {
        azdo::PipelineContextData::String(s) => Value::String(s.clone()),
        azdo::PipelineContextData::Bool(b) => Value::Bool(*b),
        azdo::PipelineContextData::Number(n) => json!(n),
        azdo::PipelineContextData::Array(items) => {
            Value::Array(items.iter().map(context_data_to_json).collect())
        }
        azdo::PipelineContextData::Dict(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(k.clone(), context_data_to_json(v));
            }
            Value::Object(obj)
        }
    }
}

/// Log and ignore evaluation failures (job-level marks failure at call site).
pub fn log_eval_error(context: &str, err: &str) {
    warn!(%context, %err, "concurrency expression evaluation failed");
}
