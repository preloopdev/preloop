//! Workflow job expansion.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use indexmap::IndexMap;
use preloop_gha_expressions::{eval_expression, Context};
use preloop_gha_protocol::{JobId, JobPlan, ReusableCallPlan, StepPlan};
use serde_json::Value;

use crate::{
    dag, matrix_expand, parse_workflow, Concurrency, ConcurrencyQueue, DeferredBool,
    DeferredNumber, ExpandedWorkflows, InputType, Job, JobContinueOnError, JobDefaults,
    MatrixValue, ParserError, ReusableCallMetadata, Step, Workflow,
};

/// GitHub display name for one expanded job.
///
/// When the job declares `name:`, expressions are resolved against the
/// matrix cell and (for reusable-workflow contexts) the caller's inputs —
/// e.g. `name: "${{ matrix.repo }}"` renders as the cell's repo value.
/// Without `name:`, GitHub displays the expanded job id, which already
/// carries the matrix suffix (`build (ubuntu-latest, 3.9)`).
fn resolved_job_name(
    name: Option<&str>,
    expanded_id: &str,
    matrix: &IndexMap<String, Value>,
    inputs: Option<&BTreeMap<String, Value>>,
) -> String {
    match name {
        Some(raw) => crate::eval::resolve_string(raw, &expression_context(matrix, inputs))
            .unwrap_or_else(|_| raw.to_owned()),
        None => expanded_id.to_owned(),
    }
}

/// Resolve `runs-on` for one concrete matrix combination.
///
/// `runs-on: ${{ matrix.os }}` is the single most common shape in real
/// workflows (tokio, caddy and uv all use it), and the label decides which
/// machine the job can run on. Leaving the raw `${{ … }}` in place produces a
/// label no runner can ever advertise, so the cell is expanded, queued, and
/// then waits forever — the failure looks like a scheduling bug rather than an
/// unevaluated expression. GitHub evaluates `runs-on` with the matrix in
/// context, so every other per-cell field here already resolves the same way.
fn resolved_runs_on(
    labels: Vec<String>,
    matrix: &IndexMap<String, Value>,
    inputs: Option<&BTreeMap<String, Value>>,
) -> Vec<String> {
    let context = expression_context(matrix, inputs);
    labels
        .into_iter()
        .map(|label| {
            if !label.contains("${{") {
                return label;
            }
            crate::eval::resolve_string(&label, &context).unwrap_or(label)
        })
        .filter(|label| !label.trim().is_empty())
        .collect()
}

fn resolved_continue_on_error(
    job_id: &str,
    value: Option<&JobContinueOnError>,
    matrix: &IndexMap<String, Value>,
) -> Result<bool, ParserError> {
    let Some(value) = value else {
        return Ok(false);
    };
    match value {
        JobContinueOnError::Bool(value) => Ok(*value),
        JobContinueOnError::Expression(value) => {
            let trimmed = value.trim();
            if trimmed.eq_ignore_ascii_case("true") {
                return Ok(true);
            }
            if trimmed.eq_ignore_ascii_case("false") {
                return Ok(false);
            }
            let expression = trimmed
                .strip_prefix("${{")
                .and_then(|value| value.strip_suffix("}}"))
                .map(str::trim)
                .ok_or_else(|| ParserError::InvalidContinueOnError {
                    job_id: job_id.to_owned(),
                    message: "expected a boolean or `${{ }}` expression".to_owned(),
                })?;
            let mut context = Context::default();
            context.insert(
                "matrix",
                Value::Object(
                    matrix
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                ),
            );
            let result = eval_expression(expression, &context).map_err(|error| {
                ParserError::InvalidContinueOnError {
                    job_id: job_id.to_owned(),
                    message: error.to_string(),
                }
            })?;
            result
                .as_bool()
                .ok_or_else(|| ParserError::InvalidContinueOnError {
                    job_id: job_id.to_owned(),
                    message: format!("expression returned {result}, expected a boolean"),
                })
        }
    }
}

fn expression_context(
    matrix: &IndexMap<String, Value>,
    inputs: Option<&BTreeMap<String, Value>>,
) -> Context {
    let mut context = Context::default();
    context.insert(
        "matrix",
        Value::Object(
            matrix
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        ),
    );
    if let Some(inputs) = inputs {
        context.insert(
            "inputs",
            Value::Object(
                inputs
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            ),
        );
    }
    context
}

fn resolve_deferred_bool(
    value: Option<&DeferredBool>,
    matrix: &IndexMap<String, Value>,
    inputs: Option<&BTreeMap<String, Value>>,
    default: bool,
) -> Result<bool, ParserError> {
    let Some(value) = value else {
        return Ok(default);
    };
    match value {
        DeferredBool::Literal(value) => Ok(*value),
        DeferredBool::Expression(expression) => {
            let result = eval_expression(expression, &expression_context(matrix, inputs))
                .map_err(|error| ParserError::InvalidExpression(error.to_string()))?;
            result.as_bool().ok_or_else(|| {
                ParserError::InvalidExpression(format!(
                    "expression returned {result}, expected a boolean"
                ))
            })
        }
    }
}

fn resolve_deferred_number(
    value: Option<&DeferredNumber>,
    matrix: &IndexMap<String, Value>,
    inputs: Option<&BTreeMap<String, Value>>,
) -> Result<Option<u64>, ParserError> {
    let Some(value) = value else { return Ok(None) };
    match value {
        DeferredNumber::Literal(value) => Ok(Some(*value)),
        DeferredNumber::Expression(expression) => {
            let result = eval_expression(expression, &expression_context(matrix, inputs))
                .map_err(|error| ParserError::InvalidExpression(error.to_string()))?;
            result.as_u64().map(Some).ok_or_else(|| {
                ParserError::InvalidExpression(format!(
                    "expression returned {result}, expected a number"
                ))
            })
        }
    }
}

/// Omit empty `services: {}` to match `EmitDefaultValue=false` behavior.
fn non_empty_services(services: Option<serde_json::Value>) -> Option<serde_json::Value> {
    match &services {
        Some(serde_json::Value::Object(m)) if m.is_empty() => None,
        _ => services,
    }
}

/// Every permission scope a workflow `permissions:` block can name, in workflow
/// (kebab-case) spelling. `read-all` and `write-all` expand to all of them.
pub const PERMISSION_SCOPES: [&str; 14] = [
    "actions",
    "attestations",
    "checks",
    "contents",
    "deployments",
    "discussions",
    "id-token",
    "issues",
    "packages",
    "pages",
    "pull-requests",
    "repository-projects",
    "security-events",
    "statuses",
];

/// `GITHUB_TOKEN` permissions for a job whose workflow declares no
/// `permissions:` block at all.
///
/// GitHub's restricted default, and exactly what the official runner prints in
/// its `GITHUB_TOKEN Permissions` setup group for such a workflow. This is the
/// single source of truth for the `system.github.token.permissions` runner
/// variable and the GitHub App installation-token request, so a job is never
/// told it holds authority its token does not carry — nor handed authority it
/// was never told about.
pub const DEFAULT_TOKEN_PERMISSIONS: [(&str, &str); 3] = [
    ("contents", "read"),
    ("metadata", "read"),
    ("packages", "read"),
];

/// Effective `GITHUB_TOKEN` permissions for a job, given its resolved
/// [`JobPlan::permissions`].
///
/// `None` means neither the job nor its workflow said anything, so
/// [`DEFAULT_TOKEN_PERMISSIONS`] applies. `Some` is authoritative *even when
/// empty*: `permissions: {}` withholds every scope, and widening that back to
/// the default would grant a job more than it asked for.
pub fn effective_token_permissions(
    declared: Option<&BTreeMap<String, String>>,
) -> Cow<'_, BTreeMap<String, String>> {
    match declared {
        Some(declared) => Cow::Borrowed(declared),
        None => Cow::Owned(
            DEFAULT_TOKEN_PERMISSIONS
                .iter()
                .map(|&(scope, level)| (scope.to_owned(), level.to_owned()))
                .collect(),
        ),
    }
}

/// Resolve the effective permissions map from workflow/job YAML.
///
/// `permissions: read-all` → all scopes set to `read`.
/// `permissions: write-all` → all scopes set to `write`.
/// `permissions: {}` → empty map (no permissions).
/// `permissions: { contents: read, issues: write }` → explicit map.
/// Job-level overrides workflow-level entirely (not merged).
fn resolve_permissions(
    job_permissions: Option<&Value>,
    workflow_permissions: Option<&Value>,
) -> Option<std::collections::BTreeMap<String, String>> {
    let effective = job_permissions.or(workflow_permissions)?;
    match effective {
        Value::String(value) => {
            let level = match value.as_str() {
                "read-all" => "read",
                "write-all" => "write",
                _ => return Some(std::collections::BTreeMap::new()),
            };
            Some(
                PERMISSION_SCOPES
                    .into_iter()
                    .map(|s| (s.to_owned(), level.to_owned()))
                    .collect(),
            )
        }
        Value::Object(map) => Some(
            map.iter()
                .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_owned())))
                .collect(),
        ),
        _ => None,
    }
}

fn intersect_permissions(
    called: Option<&BTreeMap<String, String>>,
    caller: Option<&BTreeMap<String, String>>,
) -> BTreeMap<String, String> {
    let called = effective_token_permissions(called);
    let caller = effective_token_permissions(caller);
    called
        .iter()
        .filter_map(|(scope, called_level)| {
            let caller_level = caller.get(scope)?;
            let level = match (called_level.as_str(), caller_level.as_str()) {
                ("write", "write") => "write",
                ("write" | "read", "write" | "read") => "read",
                _ => return None,
            };
            Some((scope.clone(), level.to_owned()))
        })
        .collect()
}

fn id_token_granted(permissions: Option<&Value>) -> bool {
    match permissions {
        Some(Value::String(value)) => value == "write-all",
        Some(Value::Object(values)) => values
            .get("id-token")
            .and_then(Value::as_str)
            .is_some_and(|value| value == "write"),
        _ => false,
    }
}

fn resolved_environment(
    environment: Option<&Value>,
    matrix: &indexmap::IndexMap<String, Value>,
) -> Option<String> {
    let value = match environment? {
        Value::String(value) => value,
        Value::Object(values) => values.get("name")?.as_str()?,
        _ => return None,
    };
    let trimmed = value.trim();
    let expression = trimmed.strip_prefix("${{")?.strip_suffix("}}")?.trim();
    let key = expression.strip_prefix("matrix.")?;
    matrix.get(key).and_then(|value| match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    })
}

fn oidc_environment(
    environment: Option<&Value>,
    matrix: &indexmap::IndexMap<String, Value>,
) -> Option<String> {
    match environment? {
        Value::String(value) if !value.trim().starts_with("${{") => Some(value.clone()),
        Value::Object(values) => match values.get("name")?.as_str()? {
            value if !value.trim().starts_with("${{") => Some(value.to_owned()),
            _ => resolved_environment(environment, matrix),
        },
        Value::String(_) => resolved_environment(environment, matrix),
        _ => None,
    }
}

/// Expand all jobs for a workflow.
pub fn expand_jobs(workflow: &Workflow) -> Result<Vec<JobPlan>, ParserError> {
    let mut plans = Vec::new();
    let global_env = workflow.env.clone().into_strings();
    for (job_id, job) in &workflow.jobs {
        let (matrixes, deferred_matrix) =
            expand_matrix(job_id, job.strategy.matrix.as_ref(), None)?.into_cells();
        let matrix_deferred = deferred_matrix.is_some();
        let matrix_count = matrixes.len();
        for (matrix_index, matrix) in matrixes.into_iter().enumerate() {
            let oidc_environment = oidc_environment(job.environment.as_ref(), &matrix);
            let expanded_id = matrix_expand::expanded_job_id(job_id, &matrix);
            let mut env = global_env.clone();
            env.extend(job.env.clone().into_strings());
            let mut plan = job_plan_from_job(
                job_id,
                job,
                expanded_id,
                matrix,
                (matrix_count > 1).then_some(matrix_index + 1),
                (matrix_count > 1).then_some(matrix_count),
                env,
                oidc_environment,
                workflow.permissions.as_ref(),
                None,
                matrix_deferred,
            )?;
            plan.deferred_matrix = deferred_matrix.clone();
            plans.push(plan);
        }
    }
    dag::validate_job_plans(&plans)?;
    Ok(plans)
}

#[allow(clippy::too_many_arguments)]
fn job_plan_from_job(
    job_id: &str,
    job: &Job,
    expanded_id: String,
    matrix: IndexMap<String, Value>,
    matrix_index: Option<usize>,
    matrix_total: Option<usize>,
    env: BTreeMap<String, String>,
    oidc_environment: Option<String>,
    workflow_permissions: Option<&Value>,
    inputs: Option<&BTreeMap<String, Value>>,
    matrix_deferred: bool,
) -> Result<JobPlan, ParserError> {
    let (concurrency_group, concurrency_cancel_in_progress, concurrency_queue) =
        concurrency_fields(job.concurrency.as_ref());
    // A needs-deferred matrix leaves a placeholder node with an intentionally
    // empty matrix. Matrix-dependent `continue-on-error`/`fail-fast`/
    // `max-parallel` expressions cannot be evaluated against it (they resolve
    // to null and would be rejected), so they are deferred: literals still
    // apply, expressions keep their defaults until the runtime fan-out
    // re-resolves them per concrete combination.
    let continue_on_error = if matrix_deferred {
        match job.continue_on_error {
            Some(JobContinueOnError::Bool(value)) => value,
            _ => false,
        }
    } else {
        resolved_continue_on_error(job_id, job.continue_on_error.as_ref(), &matrix)?
    };
    let fail_fast = if matrix_deferred {
        match job.strategy.fail_fast {
            Some(DeferredBool::Literal(value)) => value,
            _ => true,
        }
    } else {
        resolve_deferred_bool(job.strategy.fail_fast.as_ref(), &matrix, inputs, true)?
    };
    let max_parallel = if matrix_deferred {
        match job.strategy.max_parallel {
            Some(DeferredNumber::Literal(value)) => Some(value),
            _ => None,
        }
    } else {
        resolve_deferred_number(job.strategy.max_parallel.as_ref(), &matrix, inputs)?
    };
    let steps = job
        .steps
        .iter()
        .cloned()
        .map(|step| step_plan(step, &job.defaults, &matrix, inputs, matrix_deferred))
        .collect::<Result<Vec<_>, _>>()?;
    let name = resolved_job_name(job.name.as_deref(), &expanded_id, &matrix, inputs);
    Ok(JobPlan {
        id: JobId(expanded_id),
        base_id: job_id.to_owned(),
        name,
        runner_group: job.runs_on.group(),
        runs_on: resolved_runs_on(job.runs_on.labels(), &matrix, inputs),
        needs: job.needs.ids(),
        matrix,
        matrix_index,
        matrix_total,
        // Set by the caller, which is what knows whether the matrix was
        // deferred; a concrete combination never carries an expression.
        deferred_matrix: None,
        env,
        steps,
        if_condition: job.if_condition.clone(),
        continue_on_error,
        fail_fast,
        max_parallel,
        secrets_inherit: false,
        container: job.container.clone(),
        services: non_empty_services(job.services.clone()),
        inputs: BTreeMap::new(),
        workflow_file: None,
        workflow_ref: None,
        workflow_sha: None,
        workflow_repository: None,
        secrets_map: BTreeMap::new(),
        job_outputs: job
            .outputs
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or(&v.to_string()).to_string()))
            .collect(),
        oidc_id_token_granted: id_token_granted(job.permissions.as_ref().or(workflow_permissions)),
        permissions: resolve_permissions(job.permissions.as_ref(), workflow_permissions),
        oidc_environment,
        oidc_job_workflow_ref: None,
        concurrency_group,
        concurrency_cancel_in_progress,
        concurrency_queue,
        reusable_call: None,
    })
}

fn concurrency_fields(
    concurrency: Option<&Concurrency>,
) -> (Option<String>, Option<String>, Option<String>) {
    match concurrency {
        Some(c) => (
            Some(c.group.clone()),
            c.cancel_in_progress.clone(),
            Some(match c.queue {
                ConcurrencyQueue::Single => "single".to_owned(),
                ConcurrencyQueue::Max => "max".to_owned(),
            }),
        ),
        None => (None, None, None),
    }
}

/// Coerce value to the declared input type, matching GitHub's validation.
///
/// GitHub rejects values that cannot be cleanly coerced (e.g. `"0"` for boolean,
/// `"abc"` for number). Only literal bools, `"true"`/`"false"` strings, and
/// expression placeholders are accepted for boolean inputs. Only literal numbers,
/// numeric strings, and expressions are accepted for number inputs.
pub(crate) fn coerce_value(
    val: &Value,
    input_type: InputType,
    input_name: &str,
) -> Result<Value, ParserError> {
    match input_type {
        InputType::Boolean => match val {
            Value::Bool(b) => Ok(Value::Bool(*b)),
            Value::String(s) => {
                let s_trimmed = s.trim();
                if s_trimmed.starts_with("${{") && s_trimmed.ends_with("}}") {
                    Ok(val.clone())
                } else if s_trimmed.eq_ignore_ascii_case("true") {
                    Ok(Value::Bool(true))
                } else if s_trimmed.eq_ignore_ascii_case("false") {
                    Ok(Value::Bool(false))
                } else {
                    Err(ParserError::InvalidInputValue {
                        name: input_name.to_string(),
                        value: s.clone(),
                        expected_type: "boolean".to_string(),
                    })
                }
            }
            _ => Err(ParserError::InvalidInputValue {
                name: input_name.to_string(),
                value: val.to_string(),
                expected_type: "boolean".to_string(),
            }),
        },
        InputType::Number => match val {
            Value::Number(_) => Ok(val.clone()),
            Value::String(s) => {
                let s_trimmed = s.trim();
                if s_trimmed.starts_with("${{") && s_trimmed.ends_with("}}") {
                    Ok(val.clone())
                } else if let Ok(n) = s_trimmed.parse::<i64>() {
                    Ok(Value::Number(n.into()))
                } else if let Ok(f) = s_trimmed.parse::<f64>() {
                    if let Some(n) = serde_json::Number::from_f64(f) {
                        Ok(Value::Number(n))
                    } else {
                        Err(ParserError::InvalidInputValue {
                            name: input_name.to_string(),
                            value: s.clone(),
                            expected_type: "number".to_string(),
                        })
                    }
                } else {
                    Err(ParserError::InvalidInputValue {
                        name: input_name.to_string(),
                        value: s.clone(),
                        expected_type: "number".to_string(),
                    })
                }
            }
            _ => Err(ParserError::InvalidInputValue {
                name: input_name.to_string(),
                value: val.to_string(),
                expected_type: "number".to_string(),
            }),
        },
        InputType::String => match val {
            Value::String(_) => Ok(val.clone()),
            other => Ok(Value::String(other.to_string())),
        },
        InputType::Choice => match val {
            Value::String(_) => Ok(val.clone()),
            other => Ok(Value::String(other.to_string())),
        },
        InputType::Environment => match val {
            Value::String(_) => Ok(val.clone()),
            other => Ok(Value::String(other.to_string())),
        },
    }
}

fn expand_jobs_with_reusables_internal(
    workflow: &Workflow,
    reusable_workflows: &BTreeMap<String, String>,
    reusable_workflow_shas: &BTreeMap<String, String>,
    depth: usize,
    reusable_calls: &mut BTreeMap<String, ReusableCallMetadata>,
    inputs: Option<&BTreeMap<String, Value>>,
) -> Result<Vec<JobPlan>, ParserError> {
    let mut plans = Vec::new();
    let global_env = workflow.env.clone().into_strings();
    for (job_id, job) in &workflow.jobs {
        if let Some(uses) = &job.uses {
            if depth >= 4 {
                return Err(ParserError::MaxNestingDepthExceeded);
            }
            let path = normalize_reusable_path(uses);
            let yaml = reusable_workflows
                .get(uses)
                .or_else(|| reusable_workflows.get(&path))
                .ok_or_else(|| ParserError::MissingReusableWorkflow { path: uses.clone() })?;
            let called = parse_workflow(yaml)?;
            let trigger = called
                .workflow_call_trigger()?
                .ok_or(ParserError::MissingWorkflowCallTrigger)?;

            // Validate inputs
            for caller_input_name in job.with.keys() {
                let declared = trigger
                    .inputs
                    .keys()
                    .any(|k| k.eq_ignore_ascii_case(caller_input_name));
                if !declared {
                    return Err(ParserError::UndeclaredInput {
                        name: caller_input_name.clone(),
                    });
                }
            }

            let mut resolved_inputs = BTreeMap::new();
            for (name, def) in &trigger.inputs {
                let caller_val = job
                    .with
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .map(|(_, v)| v.clone());
                let coerced_val = match caller_val {
                    Some(val) => coerce_value(&val, def.input_type, name)?,
                    None => {
                        if let Some(default_val) = &def.default {
                            default_val.clone()
                        } else if def.required {
                            return Err(ParserError::MissingRequiredInput { name: name.clone() });
                        } else {
                            match def.input_type {
                                InputType::String => Value::String(String::new()),
                                InputType::Number => Value::Number(0.into()),
                                InputType::Boolean => Value::Bool(false),
                                InputType::Choice | InputType::Environment => {
                                    Value::String(String::new())
                                }
                            }
                        }
                    }
                };
                resolved_inputs.insert(name.clone(), coerced_val);
            }

            // Validate secrets
            let secrets_inherit = is_secrets_inherit(&job.secrets);
            let mut secrets_map = BTreeMap::new();
            if !secrets_inherit {
                let caller_secrets = match &job.secrets {
                    Some(Value::Object(map)) => Some(map),
                    _ => None,
                };
                if !trigger.secrets.is_empty() {
                    if let Some(c_secrets) = caller_secrets {
                        for caller_sec_name in c_secrets.keys() {
                            let declared = trigger
                                .secrets
                                .keys()
                                .any(|k| k.eq_ignore_ascii_case(caller_sec_name));
                            if !declared {
                                return Err(ParserError::UndeclaredSecret {
                                    name: caller_sec_name.clone(),
                                });
                            }
                        }
                    }
                }
                for (sec_name, sec_def) in &trigger.secrets {
                    if sec_def.required {
                        let provided = caller_secrets
                            .map(|cs| cs.keys().any(|k| k.eq_ignore_ascii_case(sec_name)))
                            .unwrap_or(false);
                        if !provided {
                            return Err(ParserError::MissingRequiredSecret {
                                name: sec_name.clone(),
                            });
                        }
                    }
                }
                if let Some(c_secrets) = caller_secrets {
                    for (k, v) in c_secrets {
                        if let Some(s) = v.as_str() {
                            secrets_map.insert(k.clone(), s.to_string());
                        } else {
                            secrets_map.insert(k.clone(), v.to_string());
                        }
                    }
                }
            }

            let output_definitions: BTreeMap<String, String> = trigger
                .outputs
                .iter()
                .map(|(k, v)| (k.clone(), v.value.clone()))
                .collect();

            let (matrices, deferred_matrix) =
                expand_matrix(job_id, job.strategy.matrix.as_ref(), Some(&resolved_inputs))?
                    .into_cells();
            let matrix_count = matrices.len();
            for (matrix_index, matrix) in matrices.into_iter().enumerate() {
                let expanded_job_id = matrix_expand::expanded_job_id(job_id, &matrix);
                reusable_calls.insert(
                    expanded_job_id.clone(),
                    ReusableCallMetadata {
                        caller_job_id: expanded_job_id.clone(),
                        output_definitions: output_definitions.clone(),
                        // Deferred materialization: filled by the server when
                        // the caller's `if:` gate passes at runtime.
                        inner_job_ids: Vec::new(),
                        inputs: resolved_inputs.clone(),
                        caller_concurrency: job.concurrency.clone(),
                        embedded_concurrency: called.concurrency.clone(),
                        matrix: matrix.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                        workflow_sha: reusable_workflow_shas.get(uses).cloned(),
                        workflow_repository: uses.split_once('/').and_then(|(owner, rest)| {
                            rest.split_once('/')
                                .map(|(repo, _)| format!("{owner}/{repo}"))
                        }),
                        if_condition: job.if_condition.clone(),
                    },
                );

                // Emit a single caller placeholder node. GitHub evaluates the
                // caller's `if:` once its needs complete: when the gate fails
                // the run record holds exactly this one skipped entry; when it
                // passes the callee subtree is materialized then. Inlining it
                // eagerly would inflate the run record with every matrix combo
                // of jobs GitHub never shows.
                let mut env = global_env.clone();
                env.extend(job.env.clone().into_strings());
                plans.push(JobPlan {
                    id: JobId(expanded_job_id.clone()),
                    base_id: job_id.clone(),
                    name: resolved_job_name(
                        job.name.as_deref(),
                        &expanded_job_id,
                        &matrix,
                        Some(&resolved_inputs),
                    ),
                    runner_group: None,
                    runs_on: Vec::new(),
                    needs: job.needs.ids(),
                    matrix,
                    matrix_index: (matrix_count > 1).then_some(matrix_index + 1),
                    matrix_total: (matrix_count > 1).then_some(matrix_count),
                    deferred_matrix: deferred_matrix.clone(),
                    env,
                    steps: Vec::new(),
                    if_condition: job.if_condition.clone(),
                    fail_fast: true,
                    continue_on_error: false,
                    max_parallel: None,
                    container: None,
                    services: None,
                    inputs: resolved_inputs.clone(),
                    workflow_file: Some(path.clone()),
                    workflow_ref: Some(uses.clone()),
                    workflow_sha: reusable_workflow_shas.get(uses).cloned(),
                    workflow_repository: uses.split_once('/').and_then(|(owner, rest)| {
                        rest.split_once('/')
                            .map(|(repo, _)| format!("{owner}/{repo}"))
                    }),
                    secrets_inherit,
                    secrets_map: secrets_map.clone(),
                    job_outputs: BTreeMap::new(),
                    oidc_id_token_granted: id_token_granted(
                        job.permissions.as_ref().or(workflow.permissions.as_ref()),
                    ),
                    permissions: resolve_permissions(
                        job.permissions.as_ref(),
                        workflow.permissions.as_ref(),
                    ),
                    oidc_environment: None,
                    oidc_job_workflow_ref: None,
                    // Caller/embedded concurrency is gated as a JobSet from
                    // ReusableCallMetadata at runtime; the placeholder node
                    // itself must not take a job-level gate.
                    concurrency_group: None,
                    concurrency_cancel_in_progress: None,
                    concurrency_queue: None,
                    reusable_call: Some(ReusableCallPlan {
                        uses: uses.clone(),
                        workflow_file: path.clone(),
                        workflow_sha: reusable_workflow_shas.get(uses).cloned(),
                        workflow_repository: uses.split_once('/').and_then(|(owner, rest)| {
                            rest.split_once('/')
                                .map(|(repo, _)| format!("{owner}/{repo}"))
                        }),
                        depth: depth + 1,
                    }),
                });
            }
            continue;
        }

        // A matrix expression that reads `needs.*` cannot be evaluated until
        // its upstream jobs finish, so `expand_matrix` reports it as deferred
        // rather than as zero cells. The job keeps one un-suffixed DAG node
        // (and its `if:` gating) until the runtime fan-out replaces it.
        let (matrix_cells, deferred_matrix) =
            expand_matrix(job_id, job.strategy.matrix.as_ref(), inputs)?.into_cells();
        let matrix_deferred = deferred_matrix.is_some();
        let matrix_count = matrix_cells.len();
        for (matrix_index, matrix) in matrix_cells.into_iter().enumerate() {
            let oidc_environment = oidc_environment(job.environment.as_ref(), &matrix);
            let expanded_id = matrix_expand::expanded_job_id(job_id, &matrix);
            let mut env = global_env.clone();
            env.extend(job.env.clone().into_strings());
            let mut plan = job_plan_from_job(
                job_id,
                job,
                expanded_id,
                matrix,
                (matrix_count > 1).then_some(matrix_index + 1),
                (matrix_count > 1).then_some(matrix_count),
                env,
                oidc_environment,
                workflow.permissions.as_ref(),
                inputs,
                matrix_deferred,
            )?;
            plan.deferred_matrix = deferred_matrix.clone();
            plans.push(plan);
        }
    }
    Ok(plans)
}

/// Expand jobs and inline local reusable workflows when their YAML is supplied.
pub fn expand_jobs_with_reusables(
    workflow: &Workflow,
    reusable_workflows: &BTreeMap<String, String>,
) -> Result<ExpandedWorkflows, ParserError> {
    expand_jobs_with_reusables_and_shas(workflow, reusable_workflows, &BTreeMap::new())
}

/// Expand jobs and inline reusable workflows with resolved remote metadata.
pub fn expand_jobs_with_reusables_and_shas(
    workflow: &Workflow,
    reusable_workflows: &BTreeMap<String, String>,
    reusable_workflow_shas: &BTreeMap<String, String>,
) -> Result<ExpandedWorkflows, ParserError> {
    let mut reusable_calls = BTreeMap::new();
    let mut plans = expand_jobs_with_reusables_internal(
        workflow,
        reusable_workflows,
        reusable_workflow_shas,
        0,
        &mut reusable_calls,
        None,
    )?;

    // Post-process: Rewrite needs to replace base job IDs of reusable calls with their expanded inner job IDs.
    let expanded_ids: std::collections::HashSet<String> =
        plans.iter().map(|p| p.id.0.clone()).collect();
    for plan in &mut plans {
        let mut new_needs = Vec::new();
        for need in &plan.needs {
            if expanded_ids.contains(&need.0) {
                new_needs.push(need.clone());
            } else {
                let prefix = format!("{}/", need.0);
                let prefix_matrix = format!("{} (", need.0);
                let mut matched = false;
                for id in &expanded_ids {
                    if id.starts_with(&prefix) || id.starts_with(&prefix_matrix) {
                        new_needs.push(JobId(id.clone()));
                        matched = true;
                    }
                }
                if !matched {
                    new_needs.push(need.clone());
                }
            }
        }
        plan.needs = new_needs;
    }

    dag::validate_job_plans(&plans)?;

    Ok(ExpandedWorkflows {
        jobs: plans,
        reusable_calls,
    })
}

/// Validate a runtime-expanded callee subtree. The caller's own needs were
/// appended to every inner plan; they resolve outside the subtree, so they
/// are excluded from unknown-need/cycle checking here.
fn validate_expanded_subtree(plans: &[JobPlan], external: &[JobId]) -> Result<(), ParserError> {
    let mut scoped: Vec<JobPlan> = plans.to_vec();
    for plan in &mut scoped {
        plan.needs.retain(|need| !external.contains(need));
    }
    dag::validate_job_plans(&scoped)
}

/// Expand a deferred reusable-workflow caller node into its callee subtree.
///
/// The server calls this once the caller's `needs` are complete and its `if:`
/// gate evaluated true. Inner jobs are prefixed with the caller's expanded id
/// (`caller/inner`), inherit the caller's needs/env/permissions, and carry the
/// caller's `if:` conjoined with any inner job-level condition — the same
/// shape parse-time inlining produced, materialized lazily. Nested callers
/// inside the callee remain deferred caller nodes. Every input the expansion
/// needs (inputs, secrets, env, permissions, matrix cell, `if:`) was captured
/// on the caller plan and its `ReusableCallPlan` at parse time.
pub fn expand_reusable_call(
    called: &Workflow,
    caller_plan: &JobPlan,
    reusable_workflows: &BTreeMap<String, String>,
    reusable_workflow_shas: &BTreeMap<String, String>,
) -> Result<ExpandedWorkflows, ParserError> {
    let Some(call) = &caller_plan.reusable_call else {
        return Err(ParserError::InvalidExpression(format!(
            "job `{}` is not a reusable-workflow caller",
            caller_plan.id.0
        )));
    };
    if caller_plan.deferred_matrix.is_some() {
        // A caller whose matrix reads `needs.*` cannot be materialized from
        // the empty placeholder matrix: the matrix must be resolved against
        // the completed needs outputs first, then one callee leg per
        // combination. Refuse the single-empty-matrix expansion.
        return Err(ParserError::InvalidExpression(format!(
            "job `{}` has a needs-deferred matrix; resolve it with expand_deferred_reusable_call before materializing the callee subtree",
            caller_plan.id.0
        )));
    }
    let uses = &call.uses;
    let mut reusable_calls = BTreeMap::new();
    let mut inner = expand_jobs_with_reusables_internal(
        called,
        reusable_workflows,
        reusable_workflow_shas,
        call.depth,
        &mut reusable_calls,
        Some(&caller_plan.inputs),
    )?;

    let caller_id = caller_plan.id.0.clone();
    for plan in &mut inner {
        plan.id = JobId(format!("{caller_id}/{}", plan.id.0));
        plan.base_id = format!("{caller_id}/{}", plan.base_id);
        plan.needs = plan
            .needs
            .iter()
            .map(|need| JobId(format!("{caller_id}/{}", need.0)))
            .collect();
        for outer_need in &caller_plan.needs {
            if !plan.needs.contains(outer_need) {
                plan.needs.push(outer_need.clone());
            }
        }
        // The caller's env (workflow-level then job-level) overrides the
        // callee's. The caller plan already merged both in that order, so a
        // single extend reproduces the final winner of the parse-time path.
        plan.env.extend(caller_plan.env.clone());
        plan.inputs.extend(caller_plan.inputs.clone());
        plan.secrets_inherit = caller_plan.secrets_inherit;
        plan.secrets_map.extend(caller_plan.secrets_map.clone());
        plan.workflow_file = Some(call.workflow_file.clone());
        plan.workflow_ref = Some(uses.clone());
        plan.workflow_sha = call.workflow_sha.clone();
        plan.workflow_repository = call.workflow_repository.clone();
        plan.if_condition = merge_job_conditions(
            caller_plan.if_condition.as_deref(),
            plan.if_condition.as_deref(),
        );
        plan.oidc_id_token_granted &= caller_plan.oidc_id_token_granted;
        plan.permissions = Some(intersect_permissions(
            plan.permissions.as_ref(),
            caller_plan.permissions.as_ref(),
        ));
        plan.oidc_job_workflow_ref = Some(uses.clone());
        plan.matrix.extend(caller_plan.matrix.clone());
        // GitHub renders callee jobs as `<caller display name> / <inner name>`.
        plan.name = format!("{} / {}", caller_plan.name, plan.name);
    }

    // Nested callers were recorded with ids relative to the callee; prefix
    // their metadata into the run's namespace.
    let prefixed: BTreeMap<String, ReusableCallMetadata> = reusable_calls
        .into_iter()
        .map(|(id, mut metadata)| {
            metadata.caller_job_id = format!("{caller_id}/{}", metadata.caller_job_id);
            (format!("{caller_id}/{id}"), metadata)
        })
        .collect();

    validate_expanded_subtree(&inner, &caller_plan.needs)?;

    Ok(ExpandedWorkflows {
        jobs: inner,
        reusable_calls: prefixed,
    })
}
fn is_secrets_inherit(secrets: &Option<Value>) -> bool {
    match secrets {
        Some(Value::String(s)) => s == "inherit",
        _ => false,
    }
}

/// Conjoin a caller-level `if:` with an inner job-level `if:`.
///
/// GitHub evaluates the caller gate first and only starts inner jobs under
/// it, so the flattened equivalent is the conjunction of both conditions.
/// Marker delimiters are stripped so the combined string parses as one
/// expression; a single condition is preserved verbatim.
fn merge_job_conditions(outer: Option<&str>, inner: Option<&str>) -> Option<String> {
    match (outer, inner) {
        (None, None) => None,
        (Some(condition), None) | (None, Some(condition)) => Some(condition.to_owned()),
        (Some(outer), Some(inner)) => {
            let outer = preloop_gha_expressions::trim_expression_markers(outer).trim();
            let inner = preloop_gha_expressions::trim_expression_markers(inner).trim();
            Some(format!("({outer}) && ({inner})"))
        }
    }
}
fn normalize_reusable_path(uses: &str) -> String {
    let without_ref = uses.split('@').next().unwrap_or(uses);
    let path = without_ref.strip_prefix("./").unwrap_or(without_ref);
    Path::new(path)
        .components()
        .collect::<PathBuf>()
        .to_string_lossy()
        .into_owned()
}

fn step_plan(
    step: Step,
    defaults: &Option<JobDefaults>,
    matrix: &IndexMap<String, Value>,
    inputs: Option<&BTreeMap<String, Value>>,
    matrix_deferred: bool,
) -> Result<StepPlan, ParserError> {
    // Merge job-level defaults into step — step values take precedence.
    let working_directory = step.working_directory.or_else(|| {
        defaults
            .as_ref()
            .and_then(|d| d.run.as_ref())
            .and_then(|r| r.working_directory.clone())
    });
    let shell = step.shell.or_else(|| {
        defaults
            .as_ref()
            .and_then(|d| d.run.as_ref())
            .and_then(|r| r.shell.clone())
    });
    Ok(StepPlan {
        id: step.id,
        name: step.name,
        run: step.run,
        uses: step.uses,
        env: step.env.into_strings(),
        with: step.with,
        if_condition: step.if_condition,
        working_directory,
        shell,
        continue_on_error: step.continue_on_error.as_ref().map_or(Ok(None), |value| {
            if matrix_deferred {
                // Same deferral as the job-level scalars: the placeholder's
                // matrix is empty, so expressions wait for the fan-out.
                Ok(match value {
                    DeferredBool::Literal(value) => Some(*value),
                    DeferredBool::Expression(_) => None,
                })
            } else {
                resolve_deferred_bool(Some(value), matrix, inputs, false).map(Some)
            }
        })?,
    })
}

/// The outcome of expanding a job's `strategy.matrix`.
enum MatrixExpansion {
    /// Concrete combinations, one job per entry. An empty vector means the
    /// matrix legitimately produced no jobs.
    Combinations(Vec<IndexMap<String, Value>>),
    /// The expression reads `needs.*`, so it cannot be evaluated until the
    /// upstream jobs finish. GitHub keeps a single un-suffixed node in the
    /// meantime and fans out at runtime, so the node carries the raw
    /// expression rather than any matrix values.
    Deferred(String),
}

impl MatrixExpansion {
    /// Flatten into the cells to iterate over, plus the deferred expression.
    ///
    /// A deferred matrix contributes exactly one cell with no values, so the
    /// job keeps its plain id (`build`, not `build (...)`) until the runtime
    /// fan-out replaces it with the real combinations.
    fn into_cells(self) -> (Vec<IndexMap<String, Value>>, Option<String>) {
        match self {
            MatrixExpansion::Combinations(cells) => (cells, None),
            MatrixExpansion::Deferred(expression) => (vec![IndexMap::new()], Some(expression)),
        }
    }
}

fn expand_matrix(
    job_id: &str,
    matrix: Option<&MatrixValue>,
    inputs: Option<&BTreeMap<String, Value>>,
) -> Result<MatrixExpansion, ParserError> {
    let Some(matrix) = matrix else {
        return Ok(MatrixExpansion::Combinations(vec![IndexMap::new()]));
    };

    let mut matrix = match matrix {
        MatrixValue::Static(matrix) => matrix.clone(),
        MatrixValue::Expression(expression) => {
            let value = eval_expression(expression, &expression_context(&IndexMap::new(), inputs))
                .map_err(|error| ParserError::InvalidExpression(error.to_string()))?;
            if value.is_null() {
                if expression.contains("needs.") || expression.contains("needs[") {
                    return Ok(MatrixExpansion::Deferred(expression.clone()));
                }
                return Ok(MatrixExpansion::Combinations(Vec::new()));
            }
            let spec = matrix_expand::value_to_matrix_spec(job_id, &value)?;
            return Ok(MatrixExpansion::Combinations(
                matrix_expand::expand_matrix_spec(&spec)
                    .into_iter()
                    .map(|combination| combination.values)
                    .collect(),
            ));
        }
    };
    for (field, values) in [
        ("include", &mut matrix.include),
        ("exclude", &mut matrix.exclude),
    ] {
        if values.len() == 1 {
            if let Value::String(expression) = &values[0] {
                let resolved =
                    eval_expression(expression, &expression_context(&IndexMap::new(), inputs))
                        .map_err(|error| ParserError::InvalidExpression(error.to_string()))?;
                if resolved.is_null() {
                    return Ok(MatrixExpansion::Combinations(vec![IndexMap::new()]));
                }
                *values = resolved.as_array().cloned().ok_or_else(|| {
                    ParserError::InvalidExpression(format!(
                        "matrix {field} expression did not return an array"
                    ))
                })?;
            }
        }
    }
    let spec = matrix_expand::matrix_to_spec(job_id, &matrix)?;
    Ok(MatrixExpansion::Combinations(
        matrix_expand::expand_matrix_spec(&spec)
            .into_iter()
            .map(|combination| combination.values)
            .collect(),
    ))
}

/// Dynamically expand a deferred matrix job given resolved `needs` outputs.
///
/// The node may live inside a reusable-workflow subtree: its runtime base id
/// is caller-prefixed (`call/build`) for run-namespace uniqueness, but the
/// called workflow's jobs are keyed by the callee-local id (`build`), and its
/// expression references callee-local needs (`needs.setup`, not
/// `needs.call/setup`). The prefixed run ids of the produced cells are kept;
/// only the workflow lookup and the `needs` context use the callee-local
/// forms.
pub fn expand_deferred_matrix_job(
    workflow: &Workflow,
    job_id: &str,
    expression: &str,
    needs_outputs: &BTreeMap<String, BTreeMap<String, Value>>,
    inputs: Option<&BTreeMap<String, Value>>,
) -> Result<Vec<JobPlan>, ParserError> {
    let job = workflow
        .jobs
        .get(job_id)
        .or_else(|| workflow.jobs.get(callee_local_key(job_id)))
        .ok_or_else(|| {
            ParserError::InvalidExpression(format!("job `{job_id}` not found in workflow"))
        })?;

    let combinations = resolve_deferred_matrix_cells(job_id, expression, needs_outputs, inputs)?;
    let matrix_count = combinations.len();

    let global_env = workflow.env.clone().into_strings();
    let mut plans = Vec::new();
    // The fan-out cells live in the run's namespace, where the callee's own
    // needs are caller-prefixed (`call/setup`); re-apply the node's prefix so
    // their dependencies resolve. A root-level node has no prefix.
    let needs_prefix = job_id
        .rsplit_once('/')
        .map(|(prefix, _)| format!("{prefix}/"));

    for (matrix_index, matrix) in combinations.into_iter().enumerate() {
        let oidc_environment = oidc_environment(job.environment.as_ref(), &matrix);
        let expanded_id = matrix_expand::expanded_job_id(job_id, &matrix);
        let mut env = global_env.clone();
        env.extend(job.env.clone().into_strings());
        let mut plan = job_plan_from_job(
            job_id,
            job,
            expanded_id,
            matrix,
            (matrix_count > 1).then_some(matrix_index + 1),
            (matrix_count > 1).then_some(matrix_count),
            env,
            oidc_environment,
            workflow.permissions.as_ref(),
            inputs,
            false,
        )?;
        if let Some(prefix) = &needs_prefix {
            plan.needs = plan
                .needs
                .iter()
                .map(|need| JobId(format!("{prefix}{}", need.0)))
                .collect();
        }
        plans.push(plan);
    }

    Ok(plans)
}

/// Strip the reusable-caller prefix from a runtime job key, recovering the
/// callee-local id that the called workflow's own expressions and job map
/// use. Run ids keep their prefix; only workflow lookups and `needs` context
/// keys take the callee-local form. A root-level key has no prefix.
fn callee_local_key(key: &str) -> &str {
    key.rsplit_once('/').map(|(_, tail)| tail).unwrap_or(key)
}

/// Resolve a deferred matrix expression against completed `needs` outputs,
/// returning the concrete combinations in GitHub's matrix order.
///
/// `needs` outputs arrive keyed by the run's base ids — caller-prefixed when
/// the node lives in a reusable subtree (`call/setup`) — while the expression
/// references callee-local ids (`needs.setup`), so keys are normalized with
/// [`callee_local_key`] when building the evaluation context.
fn resolve_deferred_matrix_cells(
    job_id: &str,
    expression: &str,
    needs_outputs: &BTreeMap<String, BTreeMap<String, Value>>,
    inputs: Option<&BTreeMap<String, Value>>,
) -> Result<Vec<IndexMap<String, Value>>, ParserError> {
    let mut ctx = Context::default();
    let mut needs_map = serde_json::Map::new();
    for (need_id, outputs) in needs_outputs {
        let mut job_map = serde_json::Map::new();
        let mut out_map = serde_json::Map::new();
        for (k, v) in outputs {
            out_map.insert(k.clone(), v.clone());
        }
        job_map.insert("outputs".to_string(), Value::Object(out_map));
        needs_map.insert(
            callee_local_key(need_id).to_string(),
            Value::Object(job_map),
        );
    }
    ctx.insert("needs", Value::Object(needs_map));

    if let Some(inputs) = inputs {
        let mut inputs_map = serde_json::Map::new();
        for (k, v) in inputs {
            inputs_map.insert(k.clone(), v.clone());
        }
        ctx.insert("inputs", Value::Object(inputs_map));
    }

    let value = eval_expression(expression, &ctx)
        .map_err(|error| ParserError::InvalidExpression(error.to_string()))?;
    let spec = matrix_expand::value_to_matrix_spec(job_id, &value)?;
    Ok(matrix_expand::expand_matrix_spec(&spec)
        .into_iter()
        .map(|combination| combination.values)
        .collect())
}

/// Expand a deferred reusable-workflow caller whose matrix depends on
/// `needs.*`, composing the two deferred expansions in the right order.
///
/// The caller node keeps a single un-suffixed placeholder at parse time — its
/// matrix cannot be evaluated until `needs` outputs exist — but it cannot be
/// materialized from that empty matrix either. At runtime the matrix is
/// resolved against the completed `needs` outputs first, then the callee
/// subtree is materialized once per combination, with each leg inheriting the
/// caller's matrix cell: the same shape parse-time expansion produces for a
/// static-matrix caller, where every cell has its own caller node.
///
/// `caller_workflow` must be the workflow containing the caller job: the root
/// workflow for a root-level caller, or the called workflow that holds the
/// caller for a nested one. Nested callers keep their prefixed ids and are
/// expanded through this same path at their own runtime promotion.
pub fn expand_deferred_reusable_call(
    called: &Workflow,
    caller_workflow: &Workflow,
    caller_plan: &JobPlan,
    needs_outputs: &BTreeMap<String, BTreeMap<String, Value>>,
    reusable_workflows: &BTreeMap<String, String>,
    reusable_workflow_shas: &BTreeMap<String, String>,
) -> Result<ExpandedWorkflows, ParserError> {
    if caller_plan.reusable_call.is_none() {
        return Err(ParserError::InvalidExpression(format!(
            "job `{}` is not a reusable-workflow caller",
            caller_plan.id.0
        )));
    }
    let Some(expression) = caller_plan.deferred_matrix.as_deref() else {
        return Err(ParserError::InvalidExpression(format!(
            "job `{}` has no deferred matrix to resolve",
            caller_plan.id.0
        )));
    };

    // The caller's raw `name:` template renders each cell's display name the
    // way parse-time expansion renders a static-matrix caller. The caller job
    // lives in `caller_workflow` under the callee-local id (its base id minus
    // any caller prefix).
    let raw_name = caller_workflow
        .jobs
        .get(&caller_plan.base_id)
        .or_else(|| {
            caller_plan
                .base_id
                .rsplit_once('/')
                .and_then(|(_, tail)| caller_workflow.jobs.get(tail))
        })
        .and_then(|job| job.name.clone());

    let cells = resolve_deferred_matrix_cells(
        &caller_plan.base_id,
        expression,
        needs_outputs,
        Some(&caller_plan.inputs),
    )?;
    let matrix_count = cells.len();
    let mut jobs = Vec::new();
    let mut reusable_calls = BTreeMap::new();
    for (matrix_index, cell) in cells.into_iter().enumerate() {
        let mut cell_plan = caller_plan.clone();
        cell_plan.id = JobId(matrix_expand::expanded_job_id(&caller_plan.id.0, &cell));
        cell_plan.name = resolved_job_name(
            raw_name.as_deref(),
            &cell_plan.id.0,
            &cell,
            Some(&caller_plan.inputs),
        );
        cell_plan.matrix = cell;
        cell_plan.matrix_index = (matrix_count > 1).then_some(matrix_index + 1);
        cell_plan.deferred_matrix = None;
        let expanded = expand_reusable_call(
            called,
            &cell_plan,
            reusable_workflows,
            reusable_workflow_shas,
        )?;
        jobs.extend(expanded.jobs);
        reusable_calls.extend(expanded.reusable_calls);
    }
    Ok(ExpandedWorkflows {
        jobs,
        reusable_calls,
    })
}
