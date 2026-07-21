//! Expression evaluation for workflow fields.
//!
//! Wires `aksh-gha-expressions` to resolve `${{ }}` in workflow fields
//! and build the `contextData` the runner needs for expression evaluation.

//!
//! Design principle: we resolve `${{ }}` in string fields that the
//! *server* owns (env, with, run, runs-on). We emit the raw expression
//! string for `if` conditions — the runner evaluates those itself.

use std::collections::BTreeMap;

use crate::models::{EnvValue, JobContinueOnError, ParserError, RunsOn, Workflow};
use aksh_gha_expressions::{eval_expression, Context};
use indexmap::IndexMap;
use serde_json::{Map, Value};

/// Resolve all `${{ }}` expressions in a string using the given context.
pub fn resolve_string(input: &str, context: &Context) -> Result<String, String> {
    if !input.contains("${{") {
        return Ok(input.to_owned());
    }

    let mut result = String::new();
    let mut remaining = input;

    while let Some(start) = remaining.find("${{") {
        result.push_str(&remaining[..start]);
        remaining = &remaining[start + 3..];

        let end = find_expression_end(remaining).ok_or("unclosed ${{ expression")?;
        let expr = &remaining[..end].trim();
        remaining = &remaining[end + 2..];

        let value = eval_expression(expr, context).map_err(|e| format!("{e}"))?;
        result.push_str(&stringify_value(&value));
    }

    result.push_str(remaining);
    Ok(result)
}

fn find_expression_end(input: &str) -> Option<usize> {
    // Mirror the single-quoted string rules of `aksh-gha-expressions`'s lexer:
    // only `'` opens/closes a string, doubled `''` is an escaped quote, and
    // backslash is treated as an ordinary character (no C-style escapes).
    let mut chars = input.char_indices().peekable();
    let mut in_string = false;
    while let Some((index, ch)) = chars.next() {
        if in_string {
            if ch == '\'' {
                if matches!(chars.peek(), Some(&(_, '\''))) {
                    chars.next();
                    continue;
                }
                in_string = false;
            }
            continue;
        }
        match ch {
            '\'' => in_string = true,
            '}' if input[index..].starts_with("}}") => return Some(index),
            _ => {}
        }
    }
    None
}

/// Check if a string contains any `${{ }}` expressions.
pub fn has_expressions(input: &str) -> bool {
    input.contains("${{")
}

/// Resolve a map of string values, evaluating all `${{ }}` expressions.
pub fn resolve_map(
    map: &BTreeMap<String, String>,
    context: &Context,
) -> Result<BTreeMap<String, String>, String> {
    let mut resolved = BTreeMap::new();
    for (key, value) in map {
        resolved.insert(key.clone(), resolve_string(value, context)?);
    }
    Ok(resolved)
}

/// Resolve a JSON value, evaluating any `${{ }}` expressions in strings.
pub fn resolve_json(value: &Value, context: &Context) -> Result<Value, String> {
    match value {
        Value::String(s) => {
            if has_expressions(s) {
                Ok(Value::String(resolve_string(s, context)?))
            } else {
                Ok(value.clone())
            }
        }
        Value::Array(arr) => {
            let resolved: Result<Vec<_>, _> =
                arr.iter().map(|v| resolve_json(v, context)).collect();
            Ok(Value::Array(resolved?))
        }
        Value::Object(map) => {
            let mut resolved = Map::new();
            for (k, v) in map {
                resolved.insert(k.clone(), resolve_json(v, context)?);
            }
            Ok(Value::Object(resolved))
        }
        _ => Ok(value.clone()),
    }
}

/// Convert a JSON value to its string representation for template substitution.
///
/// GitHub Actions renders whole-number values as integers — `1.0` → `"1"`.
/// serde_yaml 0.9 may produce `f64(1.0)` for a YAML integer `1`; normalise
/// before embedding in a step command string.
fn stringify_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                return i.to_string();
            }
            if let Some(u) = n.as_u64() {
                return u.to_string();
            }
            if let Some(f) = n.as_f64() {
                if f.fract() == 0.0 && f.abs() < 1e15 {
                    return (f as i64).to_string();
                }
            }
            n.to_string()
        }
        Value::Array(a) => serde_json::to_string(a).unwrap_or_default(),
        Value::Object(o) => serde_json::to_string(o).unwrap_or_default(),
    }
}

/// Validate expressions in a string.
pub fn validate_expressions_in_string(
    input: &str,
    is_condition: bool,
    allowed_contexts: Option<&[&str]>,
) -> Result<(), String> {
    if is_condition && !input.contains("${{") {
        let effective = aksh_gha_expressions::effective_condition(Some(input));
        aksh_gha_expressions::validate_expression(&effective)
            .map_err(|e| format!("invalid condition `{input}`: {e}"))?;
        validate_contexts(&effective, allowed_contexts, input)?;
        return Ok(());
    }

    let mut remaining = input;
    while let Some(start) = remaining.find("${{") {
        remaining = &remaining[start + 3..];
        let end = find_expression_end(remaining)
            .ok_or_else(|| format!("unclosed ${{ expression in `{input}`"))?;
        let expr = remaining[..end].trim();
        remaining = &remaining[end + 2..];

        aksh_gha_expressions::validate_expression(expr)
            .map_err(|e| format!("invalid expression `${{{{ {expr} }}}}` in `{input}`: {e}"))?;
        validate_contexts(expr, allowed_contexts, input)?;
    }
    Ok(())
}

fn validate_contexts(
    expr: &str,
    allowed_contexts: Option<&[&str]>,
    input: &str,
) -> Result<(), String> {
    let Some(allowed) = allowed_contexts else {
        return Ok(());
    };
    let contexts = aksh_gha_expressions::collect_contexts(expr)
        .map_err(|e| format!("invalid expression in `{input}`: {e}"))?;
    for ctx in &contexts {
        if !allowed.iter().any(|a| a.eq_ignore_ascii_case(ctx)) {
            return Err(format!(
                "context \"{ctx}\" is not allowed here. available contexts are {}.",
                allowed
                    .iter()
                    .map(|a| format!("\"{a}\""))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }
    Ok(())
}

/// Recursively validate expressions inside a serde_json::Value.
pub fn validate_value_expressions(
    value: &Value,
    allowed_contexts: Option<&[&str]>,
) -> Result<(), String> {
    match value {
        Value::String(s) => validate_expressions_in_string(s, false, allowed_contexts),
        Value::Array(arr) => {
            for item in arr {
                validate_value_expressions(item, allowed_contexts)?;
            }
            Ok(())
        }
        Value::Object(map) => {
            for val in map.values() {
                validate_value_expressions(val, allowed_contexts)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_env_expressions(
    env: &crate::models::Env,
    allowed_contexts: Option<&[&str]>,
) -> Result<(), String> {
    match env {
        crate::models::Env::Empty => Ok(()),
        crate::models::Env::Expression(expr) => {
            validate_expressions_in_string(expr, false, allowed_contexts)
        }
        crate::models::Env::Map(map) => {
            for val in map.values() {
                if let EnvValue::String(s) = val {
                    validate_expressions_in_string(s, false, allowed_contexts)?;
                }
            }
            Ok(())
        }
    }
}

const CTX_RUN_NAME: &[&str] = &["github", "inputs", "vars"];
const CTX_WORKFLOW_ENV: &[&str] = &["github", "inputs", "vars", "secrets"];
const CTX_WORKFLOW_CONCURRENCY: &[&str] = &["github", "inputs", "vars"];
const CTX_JOB_IF: &[&str] = &[
    "github",
    "inputs",
    "vars",
    "needs",
    "always",
    "failure",
    "cancelled",
    "success",
];
const CTX_JOB_RUNS_ON: &[&str] = &["github", "inputs", "vars", "needs", "matrix", "strategy"];
const CTX_JOB_ENV: &[&str] = &[
    "github", "inputs", "vars", "needs", "strategy", "matrix", "secrets", "env",
];
const CTX_JOB_CONCURRENCY: &[&str] = &["github", "inputs", "vars", "needs", "strategy", "matrix"];
const CTX_JOB_DEFAULTS_RUN: &[&str] = &["github", "strategy", "matrix", "needs", "env", "vars"];
const CTX_JOB_CONTAINER: &[&str] = &["github", "needs", "strategy", "matrix", "vars"];
const CTX_CONTAINER_CREDENTIALS: &[&str] = &["secrets", "env", "github", "vars"];
const CTX_RUNNER: &[&str] = &[
    "github", "needs", "strategy", "matrix", "secrets", "steps", "job", "runner", "env", "vars",
];
const CTX_STEP_IF: &[&str] = &[
    "github",
    "inputs",
    "vars",
    "needs",
    "strategy",
    "matrix",
    "secrets",
    "steps",
    "job",
    "runner",
    "env",
    "always",
    "failure",
    "cancelled",
    "success",
    "hashfiles",
];
const CTX_STEP_ENV: &[&str] = &[
    "github",
    "inputs",
    "vars",
    "needs",
    "strategy",
    "matrix",
    "secrets",
    "steps",
    "job",
    "runner",
    "env",
    "hashfiles",
];
const CTX_STEP_WITH: &[&str] = CTX_STEP_ENV;
const CTX_STEP_RUN: &[&str] = CTX_STEP_ENV;
const CTX_STEP_NAME: &[&str] = CTX_STEP_ENV;
const CTX_STEP_WORKING_DIR: &[&str] = CTX_STEP_ENV;

/// Validate a job container or service value. Container images/options inherit
/// the job-container context, while `credentials` and `env` have their own
/// narrower schema-defined contexts.
fn validate_container_expressions(value: &Value) -> Result<(), String> {
    let Value::Object(map) = value else {
        return validate_value_expressions(value, Some(CTX_JOB_CONTAINER));
    };

    for (key, value) in map {
        let allowed = match key.as_str() {
            "credentials" => CTX_CONTAINER_CREDENTIALS,
            "env" => CTX_RUNNER,
            _ => CTX_JOB_CONTAINER,
        };
        validate_value_expressions(value, Some(allowed))?;
    }
    Ok(())
}

/// Validate all `${{ }}` expressions in a workflow.
pub fn validate_workflow_expressions(workflow: &Workflow) -> Result<(), ParserError> {
    if let Some(run_name) = &workflow.run_name {
        validate_expressions_in_string(run_name, false, Some(CTX_RUN_NAME))
            .map_err(ParserError::InvalidExpression)?;
    }
    validate_env_expressions(&workflow.env, Some(CTX_WORKFLOW_ENV))
        .map_err(ParserError::InvalidExpression)?;

    if let Some(concurrency) = &workflow.concurrency {
        validate_expressions_in_string(&concurrency.group, false, Some(CTX_WORKFLOW_CONCURRENCY))
            .map_err(ParserError::InvalidExpression)?;
        if let Some(expr) = &concurrency.cancel_in_progress {
            validate_expressions_in_string(expr, false, Some(CTX_WORKFLOW_CONCURRENCY))
                .map_err(ParserError::InvalidExpression)?;
        }
    }

    for (job_id, job) in &workflow.jobs {
        if let Some(name) = &job.name {
            validate_expressions_in_string(name, false, Some(CTX_JOB_RUNS_ON))
                .map_err(|e| ParserError::InvalidExpression(format!("job `{job_id}`: {e}")))?;
        }
        match &job.runs_on {
            RunsOn::Single(s) => {
                validate_expressions_in_string(s, false, Some(CTX_JOB_RUNS_ON)).map_err(|e| {
                    ParserError::InvalidExpression(format!("job `{job_id}` runs-on: {e}"))
                })?;
            }
            RunsOn::Many(list) => {
                for s in list {
                    validate_expressions_in_string(s, false, Some(CTX_JOB_RUNS_ON)).map_err(
                        |e| ParserError::InvalidExpression(format!("job `{job_id}` runs-on: {e}")),
                    )?;
                }
            }
            RunsOn::Dynamic(v) => {
                validate_value_expressions(v, Some(CTX_JOB_RUNS_ON)).map_err(|e| {
                    ParserError::InvalidExpression(format!("job `{job_id}` runs-on: {e}"))
                })?;
            }
        }
        if let Some(cond) = &job.if_condition {
            validate_expressions_in_string(cond, true, Some(CTX_JOB_IF)).map_err(|e| {
                ParserError::InvalidExpression(format!("job `{job_id}` if condition: {e}"))
            })?;
        }
        validate_env_expressions(&job.env, Some(CTX_JOB_ENV))
            .map_err(|e| ParserError::InvalidExpression(format!("job `{job_id}` env: {e}")))?;

        if let Some(concurrency) = &job.concurrency {
            validate_expressions_in_string(&concurrency.group, false, Some(CTX_JOB_CONCURRENCY))
                .map_err(|e| {
                    ParserError::InvalidExpression(format!("job `{job_id}` concurrency: {e}"))
                })?;
            if let Some(expr) = &concurrency.cancel_in_progress {
                validate_expressions_in_string(expr, false, Some(CTX_JOB_CONCURRENCY)).map_err(
                    |e| {
                        ParserError::InvalidExpression(format!(
                            "job `{job_id}` concurrency cancel-in-progress: {e}"
                        ))
                    },
                )?;
            }
        }
        if let Some(JobContinueOnError::Expression(expr)) = &job.continue_on_error {
            validate_expressions_in_string(expr, false, Some(CTX_JOB_RUNS_ON)).map_err(|e| {
                ParserError::InvalidExpression(format!("job `{job_id}` continue-on-error: {e}"))
            })?;
        }

        if let Some(container) = &job.container {
            validate_container_expressions(container).map_err(|e| {
                ParserError::InvalidExpression(format!("job `{job_id}` container: {e}"))
            })?;
        }
        if let Some(services) = &job.services {
            if let Value::Object(services) = services {
                for (service_name, service) in services {
                    validate_container_expressions(service).map_err(|e| {
                        ParserError::InvalidExpression(format!(
                            "job `{job_id}` service `{service_name}`: {e}"
                        ))
                    })?;
                }
            } else {
                validate_value_expressions(services, Some(CTX_JOB_CONTAINER)).map_err(|e| {
                    ParserError::InvalidExpression(format!("job `{job_id}` services: {e}"))
                })?;
            }
        }
        if let Some(defaults) = &job.defaults {
            if let Some(run) = &defaults.run {
                if let Some(shell) = &run.shell {
                    validate_expressions_in_string(shell, false, Some(CTX_JOB_DEFAULTS_RUN))
                        .map_err(|e| {
                            ParserError::InvalidExpression(format!(
                                "job `{job_id}` defaults.run.shell: {e}"
                            ))
                        })?;
                }
                if let Some(working_directory) = &run.working_directory {
                    validate_expressions_in_string(
                        working_directory,
                        false,
                        Some(CTX_JOB_DEFAULTS_RUN),
                    )
                    .map_err(|e| {
                        ParserError::InvalidExpression(format!(
                            "job `{job_id}` defaults.run.working-directory: {e}"
                        ))
                    })?;
                }
            }
        }
        for (output_name, output) in &job.outputs {
            validate_value_expressions(output, Some(CTX_RUNNER)).map_err(|e| {
                ParserError::InvalidExpression(format!(
                    "job `{job_id}` output `{output_name}`: {e}"
                ))
            })?;
        }

        for (step_idx, step) in job.steps.iter().enumerate() {
            let step_ref = step
                .name
                .as_deref()
                .map(|n| format!("step `{n}`"))
                .unwrap_or_else(|| format!("step #{step_idx}"));
            if let Some(name) = &step.name {
                validate_expressions_in_string(name, false, Some(CTX_STEP_NAME)).map_err(|e| {
                    ParserError::InvalidExpression(format!("job `{job_id}` {step_ref}: {e}"))
                })?;
            }
            if let Some(cond) = &step.if_condition {
                validate_expressions_in_string(cond, true, Some(CTX_STEP_IF)).map_err(|e| {
                    ParserError::InvalidExpression(format!(
                        "job `{job_id}` {step_ref} if condition: {e}"
                    ))
                })?;
            }
            validate_env_expressions(&step.env, Some(CTX_STEP_ENV)).map_err(|e| {
                ParserError::InvalidExpression(format!("job `{job_id}` {step_ref} env: {e}"))
            })?;
            for val in step.with.values() {
                validate_value_expressions(val, Some(CTX_STEP_WITH)).map_err(|e| {
                    ParserError::InvalidExpression(format!("job `{job_id}` {step_ref} with: {e}"))
                })?;
            }
            if let Some(run) = &step.run {
                validate_expressions_in_string(run, false, Some(CTX_STEP_RUN)).map_err(|e| {
                    ParserError::InvalidExpression(format!("job `{job_id}` {step_ref} run: {e}"))
                })?;
            }
            if let Some(wd) = &step.working_directory {
                validate_expressions_in_string(wd, false, Some(CTX_STEP_WORKING_DIR)).map_err(
                    |e| {
                        ParserError::InvalidExpression(format!(
                            "job `{job_id}` {step_ref} working-directory: {e}"
                        ))
                    },
                )?;
            }
        }
    }

    Ok(())
}

/// Build an expression evaluation context from workflow data.
pub fn build_context(
    github: &Value,
    env: &BTreeMap<String, String>,
    vars: &BTreeMap<String, String>,
    matrix: &IndexMap<String, Value>,
    strategy: &Value,
    secrets: &BTreeMap<String, String>,
    inputs: &BTreeMap<String, Value>,
) -> Context {
    let mut ctx = Context::default();

    ctx.insert("github", github.clone());

    let env_value: Map<String, Value> = env
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    ctx.insert("env", Value::Object(env_value));

    let vars_value: Map<String, Value> = vars
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    ctx.insert("vars", Value::Object(vars_value));

    let matrix_value: Map<String, Value> =
        matrix.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    ctx.insert("matrix", Value::Object(matrix_value));

    ctx.insert("strategy", strategy.clone());
    ctx.insert("needs", Value::Object(Map::new()));

    let secrets_value: Map<String, Value> = secrets
        .keys()
        .map(|k| (k.clone(), Value::String("***".to_owned())))
        .collect();
    ctx.insert("secrets", Value::Object(secrets_value));

    let inputs_value: Map<String, Value> =
        inputs.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    ctx.insert("inputs", Value::Object(inputs_value));
    ctx
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_context() -> Context {
        let mut ctx = Context::default();
        ctx.insert(
            "github",
            json!({
                "event_name": "push",
                "ref": "refs/heads/main",
                "sha": "abc123"
            }),
        );
        let mut env = Map::new();
        env.insert("MY_VAR".to_owned(), Value::String("hello".to_owned()));
        ctx.insert("env", Value::Object(env));
        ctx.insert("matrix", json!({"os": "ubuntu-latest", "node": "18"}));
        ctx
    }

    #[test]
    fn resolve_literal_string() {
        let ctx = make_context();
        assert_eq!(
            resolve_string("no expressions", &ctx).unwrap(),
            "no expressions"
        );
    }

    #[test]
    fn resolve_single_expression() {
        let ctx = make_context();
        assert_eq!(
            resolve_string("${{ github.event_name }}", &ctx).unwrap(),
            "push"
        );
    }

    #[test]
    fn resolve_mixed_literal_and_expression() {
        let ctx = make_context();
        assert_eq!(
            resolve_string("ref=${{ github.ref }}", &ctx).unwrap(),
            "ref=refs/heads/main"
        );
    }

    #[test]
    fn expression_end_ignores_braces_inside_string_literals() {
        let source = r#" format('value }} still inside') }}"#;
        assert_eq!(find_expression_end(source), Some(source.len() - 2));
    }

    #[test]
    fn expression_end_treats_backslash_as_literal_in_single_quotes() {
        // Backslash is NOT an escape in the lexer's single-quoted strings.
        // The first `'` closes the string, then `}}` should terminate.
        let source = r"'C:\' }}";
        assert_eq!(find_expression_end(source), Some(source.len() - 2));
    }

    #[test]
    fn expression_end_handles_doubled_quote_escape() {
        // `''` inside a single-quoted string is an escaped quote, not a close.
        let source = r"'it''s fine' }}";
        assert_eq!(find_expression_end(source), Some(source.len() - 2));
    }

    #[test]
    fn resolve_matrix_value() {
        let ctx = make_context();
        assert_eq!(
            resolve_string("${{ matrix.os }}", &ctx).unwrap(),
            "ubuntu-latest"
        );
    }

    #[test]
    fn resolve_env_value() {
        let ctx = make_context();
        assert_eq!(resolve_string("${{ env.MY_VAR }}", &ctx).unwrap(), "hello");
    }

    #[test]
    fn resolve_map_expressions() {
        let ctx = make_context();
        let mut map = BTreeMap::new();
        map.insert("key".to_owned(), "value".to_owned());
        map.insert("expr".to_owned(), "${{ matrix.os }}".to_owned());
        let resolved = resolve_map(&map, &ctx).unwrap();
        assert_eq!(resolved["key"], "value");
        assert_eq!(resolved["expr"], "ubuntu-latest");
    }

    #[test]
    fn unclosed_expression_returns_error() {
        let ctx = make_context();
        assert!(resolve_string("${{ github.event_name", &ctx).is_err());
    }

    #[test]
    fn resolve_string_integer_matrix_value_no_decimal_suffix() {
        // GH-MATRIX-INT: ${{ matrix.val }} where val is f64(1.0) must produce
        // "1" not "1.0" — serde_yaml 0.9 may deserialise YAML int as f64.
        let mut ctx = make_context();
        ctx.insert("matrix", serde_json::json!({"val": 1.0_f64}));
        assert_eq!(
            resolve_string("DONE=${{ matrix.val }}", &ctx).unwrap(),
            "DONE=1",
        );
        ctx.insert("matrix", serde_json::json!({"val": 3.0_f64}));
        assert_eq!(
            resolve_string("echo ${{ matrix.val }}", &ctx).unwrap(),
            "echo 3",
        );
    }
}
