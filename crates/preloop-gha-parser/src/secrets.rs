//! Secret names a workflow tree reads from the engine's secret store.
//!
//! `preloop run` and the submission path use this to tell an operator which
//! secrets a workflow expects *before* the run reaches a step that reads an
//! empty value. The engine never reads GitHub's secret store — values are
//! write-only there — so a name that is referenced but unset has to be seeded
//! with `preloop secret set`.

use std::collections::{BTreeMap, BTreeSet};

use preloop_gha_protocol::{JobPlan, StepPlan};
use serde_json::Value;

/// Tokens the engine mints per job and injects into every step. A workflow
/// reads them like secrets but never stores them, so they are never reported
/// as missing.
pub const ENGINE_PROVIDED_SECRETS: &[&str] = &[
    "GITHUB_TOKEN",
    "ACTIONS_ID_TOKEN_REQUEST_TOKEN",
    "ACTIONS_ID_TOKEN_REQUEST_URL",
    "ACTIONS_RUNTIME_TOKEN",
    "ACTIONS_RUNTIME_URL",
    "ACTIONS_CACHE_URL",
    "ACTIONS_RESULTS_URL",
];

/// Secret names a workflow tree reads, and where it reads them.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SecretRequirements {
    /// Every `${{ secrets.NAME }}` a job reads, engine-provided tokens
    /// excluded.
    pub names: BTreeSet<String>,
    /// The subset read by jobs that declare a literal `environment:`, keyed by
    /// environment name. GitHub's per-environment tier overrides the global
    /// and per-repository tiers for those jobs.
    pub by_environment: BTreeMap<String, BTreeSet<String>>,
    /// Jobs that pass `secrets: inherit` to a reusable workflow. Their callee
    /// reads whatever the caller holds, so no static list covers them.
    pub inherits: BTreeSet<String>,
}

impl SecretRequirements {
    /// No secret references at all, inherited ones included.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty() && self.inherits.is_empty()
    }

    /// Referenced names that `configured` does not cover.
    pub fn missing_from<'a>(
        &self,
        configured: impl IntoIterator<Item = &'a str>,
    ) -> BTreeSet<String> {
        let configured: BTreeSet<&str> = configured.into_iter().collect();
        self.names
            .iter()
            .filter(|name| !configured.contains(name.as_str()))
            .cloned()
            .collect()
    }
}

/// Collect the secret names `jobs` read through `${{ secrets.NAME }}`.
pub fn collect_secret_requirements(jobs: &[JobPlan]) -> SecretRequirements {
    let mut requirements = SecretRequirements::default();
    for job in jobs {
        let mut names = BTreeSet::new();
        for value in job.env.values() {
            collect_from_text(value, &mut names);
        }
        if let Some(condition) = &job.if_condition {
            collect_from_condition(condition, &mut names);
        }
        for value in job.container.iter().chain(job.services.iter()) {
            collect_from_json(value, &mut names);
        }
        // A reusable call maps callee secret names to caller expressions; the
        // expressions are the caller's references.
        for expression in job.secrets_map.values() {
            collect_from_text(expression, &mut names);
        }
        for step in &job.steps {
            collect_from_step(step, &mut names);
        }
        names.retain(|name| !is_engine_provided(name));

        if job.secrets_inherit {
            requirements.inherits.insert(job.id.0.clone());
        }
        if let Some(environment) = literal_environment(job) {
            requirements
                .by_environment
                .entry(environment)
                .or_default()
                .extend(names.iter().cloned());
        }
        requirements.names.extend(names);
    }
    requirements
}

fn collect_from_step(step: &StepPlan, names: &mut BTreeSet<String>) {
    if let Some(run) = &step.run {
        collect_from_text(run, names);
    }
    for value in step.env.values() {
        collect_from_text(value, names);
    }
    for value in step.with.values() {
        collect_from_json(value, names);
    }
    if let Some(condition) = &step.if_condition {
        collect_from_condition(condition, names);
    }
    for value in step.working_directory.iter().chain(step.shell.iter()) {
        collect_from_text(value, names);
    }
}

/// Scan every `${{ … }}` span in a field that also carries literal text.
fn collect_from_text(text: &str, names: &mut BTreeSet<String>) {
    let mut rest = text;
    while let Some(start) = rest.find("${{") {
        let after = &rest[start + 3..];
        let Some(end) = after.find("}}") else {
            break;
        };
        collect_from_expression(&after[..end], names);
        rest = &after[end + 2..];
    }
}

/// An `if:` value is an expression with or without the `${{ }}` markers.
fn collect_from_condition(condition: &str, names: &mut BTreeSet<String>) {
    if condition.contains("${{") {
        collect_from_text(condition, names);
    } else {
        collect_from_expression(condition, names);
    }
}

fn collect_from_json(value: &Value, names: &mut BTreeSet<String>) {
    match value {
        Value::String(text) => collect_from_text(text, names),
        Value::Array(items) => {
            for item in items {
                collect_from_json(item, names);
            }
        }
        Value::Object(map) => {
            for item in map.values() {
                collect_from_json(item, names);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// A malformed expression is the submission path's problem to report, not
/// this preflight's: it contributes no names rather than failing the run here.
fn collect_from_expression(expression: &str, names: &mut BTreeSet<String>) {
    let Ok(properties) = preloop_gha_expressions::collect_context_properties(expression) else {
        return;
    };
    if let Some(referenced) = properties.get("secrets") {
        names.extend(referenced.iter().cloned());
    }
}

fn is_engine_provided(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    ENGINE_PROVIDED_SECRETS.contains(&upper.as_str())
}

/// The job's deployment environment when it is a literal name; an expression
/// (`environment: ${{ inputs.stage }}`) has no knowable tier at parse time.
///
/// Expansion already resolves the raw `environment:` value (string or mapping,
/// with matrix substitution) into `oidc_environment`, which is the name the
/// secret tiers key on.
fn literal_environment(job: &JobPlan) -> Option<String> {
    let name = job
        .oidc_environment
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .or_else(|| match job.environment.as_ref() {
            Some(Value::String(name)) => Some(name.trim()),
            Some(Value::Object(map)) => map.get("name").and_then(Value::as_str).map(str::trim),
            _ => None,
        })?;
    if name.is_empty() || name.contains("${{") {
        return None;
    }
    Some(name.to_owned())
}
