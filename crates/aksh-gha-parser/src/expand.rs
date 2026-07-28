//! Workflow job expansion.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use aksh_gha_expressions::{eval_expression, Context};
use aksh_gha_protocol::{JobId, JobPlan, StepPlan};
use indexmap::IndexMap;
use serde_json::Value;

use crate::{
    dag, matrix_expand, parse_workflow, Concurrency, ConcurrencyQueue, DeferredBool,
    DeferredNumber, ExpandedWorkflows, InputType, Job, JobContinueOnError, JobDefaults,
    MatrixValue, ParserError, ReusableCallMetadata, Step, Workflow,
};

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
            // GitHub's known permission scopes.
            let scopes = [
                "actions",
                "attestations",
                "checks",
                "contents",
                "deployments",
                "id-token",
                "issues",
                "discussions",
                "packages",
                "pages",
                "pull-requests",
                "repository-projects",
                "security-events",
                "statuses",
            ];
            Some(
                scopes
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
        for matrix in expand_matrix(job_id, job.strategy.matrix.as_ref(), None)? {
            let oidc_environment = oidc_environment(job.environment.as_ref(), &matrix);
            let expanded_id = matrix_expand::expanded_job_id(job_id, &matrix);
            let mut env = global_env.clone();
            env.extend(job.env.clone().into_strings());
            plans.push(job_plan_from_job(
                job_id,
                job,
                expanded_id,
                matrix,
                env,
                oidc_environment,
                workflow.permissions.as_ref(),
                None,
            )?);
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
    env: BTreeMap<String, String>,
    oidc_environment: Option<String>,
    workflow_permissions: Option<&Value>,
    inputs: Option<&BTreeMap<String, Value>>,
) -> Result<JobPlan, ParserError> {
    let (concurrency_group, concurrency_cancel_in_progress, concurrency_queue) =
        concurrency_fields(job.concurrency.as_ref());
    let continue_on_error =
        resolved_continue_on_error(job_id, job.continue_on_error.as_ref(), &matrix)?;
    let fail_fast = resolve_deferred_bool(job.strategy.fail_fast.as_ref(), &matrix, inputs, true)?;
    let max_parallel =
        resolve_deferred_number(job.strategy.max_parallel.as_ref(), &matrix, inputs)?;
    let steps = job
        .steps
        .iter()
        .cloned()
        .map(|step| step_plan(step, &job.defaults, &matrix, inputs))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(JobPlan {
        id: JobId(expanded_id),
        base_id: job_id.to_owned(),
        name: job.name.clone().unwrap_or_else(|| job_id.to_owned()),
        runner_group: job.runs_on.group(),
        runs_on: job.runs_on.labels(),
        needs: job.needs.ids(),
        matrix,
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

            let matrices =
                expand_matrix(job_id, job.strategy.matrix.as_ref(), Some(&resolved_inputs))?;
            for matrix in matrices {
                let expanded_job_id = matrix_expand::expanded_job_id(job_id, &matrix);
                let mut called_plans = expand_jobs_with_reusables_internal(
                    &called,
                    reusable_workflows,
                    reusable_workflow_shas,
                    depth + 1,
                    reusable_calls,
                    Some(&resolved_inputs),
                )?;

                let mut inner_job_ids = Vec::new();
                for called_plan in &mut called_plans {
                    let old_id = called_plan.id.0.clone();
                    let new_id = format!("{expanded_job_id}/{old_id}");
                    called_plan.id = JobId(new_id.clone());
                    inner_job_ids.push(new_id);
                    called_plan.base_id = format!("{expanded_job_id}/{}", called_plan.base_id);
                    called_plan.needs = called_plan
                        .needs
                        .iter()
                        .map(|need| JobId(format!("{expanded_job_id}/{}", need.0)))
                        .collect();
                    for outer_need in &job.needs.ids() {
                        if !called_plan.needs.contains(outer_need) {
                            called_plan.needs.push(outer_need.clone());
                        }
                    }
                    called_plan.env.extend(global_env.clone());
                    called_plan.env.extend(job.env.clone().into_strings());

                    called_plan.inputs.extend(resolved_inputs.clone());
                    called_plan.secrets_inherit = secrets_inherit;
                    called_plan.secrets_map.extend(secrets_map.clone());
                    called_plan.workflow_file = Some(path.clone());
                    called_plan.workflow_ref = Some(uses.clone());
                    called_plan.workflow_sha = reusable_workflow_shas.get(uses).cloned();
                    called_plan.workflow_repository =
                        uses.split_once('/').and_then(|(owner, rest)| {
                            rest.split_once('/')
                                .map(|(repo, _)| format!("{owner}/{repo}"))
                        });
                    called_plan.oidc_id_token_granted &= id_token_granted(
                        job.permissions.as_ref().or(workflow.permissions.as_ref()),
                    );
                    called_plan.oidc_job_workflow_ref = Some(uses.clone());
                    called_plan.matrix.extend(matrix.clone());
                }

                reusable_calls.insert(
                    expanded_job_id.clone(),
                    ReusableCallMetadata {
                        caller_job_id: expanded_job_id,
                        output_definitions: output_definitions.clone(),
                        inner_job_ids,
                        inputs: resolved_inputs.clone(),
                        caller_concurrency: job.concurrency.clone(),
                        embedded_concurrency: called.concurrency.clone(),
                        matrix: matrix.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                        workflow_sha: reusable_workflow_shas.get(uses).cloned(),
                        workflow_repository: uses.split_once('/').and_then(|(owner, rest)| {
                            rest.split_once('/')
                                .map(|(repo, _)| format!("{owner}/{repo}"))
                        }),
                    },
                );

                plans.extend(called_plans);
            }
            continue;
        }

        for matrix in expand_matrix(job_id, job.strategy.matrix.as_ref(), inputs)? {
            let oidc_environment = oidc_environment(job.environment.as_ref(), &matrix);
            let expanded_id = matrix_expand::expanded_job_id(job_id, &matrix);
            let mut env = global_env.clone();
            env.extend(job.env.clone().into_strings());
            plans.push(job_plan_from_job(
                job_id,
                job,
                expanded_id,
                matrix,
                env,
                oidc_environment,
                workflow.permissions.as_ref(),
                inputs,
            )?);
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

fn is_secrets_inherit(secrets: &Option<Value>) -> bool {
    match secrets {
        Some(Value::String(s)) => s == "inherit",
        _ => false,
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
            resolve_deferred_bool(Some(value), matrix, inputs, false).map(Some)
        })?,
    })
}

fn expand_matrix(
    job_id: &str,
    matrix: Option<&MatrixValue>,
    inputs: Option<&BTreeMap<String, Value>>,
) -> Result<Vec<IndexMap<String, Value>>, ParserError> {
    let Some(matrix) = matrix else {
        return Ok(vec![IndexMap::new()]);
    };

    let mut matrix = match matrix {
        MatrixValue::Static(matrix) => matrix.clone(),
        MatrixValue::Expression(expression) => {
            let value = eval_expression(expression, &expression_context(&IndexMap::new(), inputs))
                .map_err(|error| ParserError::InvalidExpression(error.to_string()))?;
            if value.is_null() {
                return Ok(vec![IndexMap::new()]);
            }
            serde_json::from_value(value).map_err(|error| {
                ParserError::InvalidExpression(format!(
                    "matrix expression did not return a matrix object: {error}"
                ))
            })?
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
                    return Ok(vec![IndexMap::new()]);
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
    Ok(matrix_expand::expand_matrix_spec(&spec)
        .into_iter()
        .map(|combination| combination.values)
        .collect())
}
