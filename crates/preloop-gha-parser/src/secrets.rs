//! Secret names a workflow tree reads from the engine's secret store.
//!
//! `preloop run` and the submission path use [`collect_secret_requirements`]
//! to tell an operator which secrets a workflow expects *before* the run
//! reaches a step that reads an empty value. The engine never reads GitHub's
//! secret store — values are write-only there — so a name that is referenced
//! but unset has to be seeded with `preloop secret set`. The strict
//! [`collect_job_secret_reads`] variant additionally flags jobs whose reads
//! cannot be enumerated (dynamic indexing, object filters), which the
//! server's injection scoping treats as "the whole scope".

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
        let reads = collect_job_secret_reads(job);
        if job.secrets_inherit {
            requirements.inherits.insert(job.id.0.clone());
        }
        if let Some(environment) = literal_environment(job) {
            requirements
                .by_environment
                .entry(environment)
                .or_default()
                .extend(reads.names.iter().cloned());
        }
        requirements.names.extend(reads.names);
    }
    requirements
}

/// Every stored secret one expanded job may read, for injection scoping.
///
/// This is the strict variant of the preflight walk: the name set is the
/// same literal-reference collection, but `dynamic` marks the job as
/// reading an unprovable name set — `secrets[matrix.pick]`, a `*` object
/// filter, a bare `secrets` argument, or an expression that fails to parse.
/// A consumer narrowing secret delivery must inject the full scope for such
/// a job rather than trusting `names`.
///
/// Coverage: every field whose `${{ }}` is evaluated on the runner or at
/// message fill — `env`, `if`, container/services (credentials read
/// secrets), step `name`/`env`/`with`/`run`/`if`/`working-directory`/`shell`,
/// `environment.url`, job `outputs`, the concurrency strings, and reusable
/// `secrets:` map expressions (the caller's reads). A step with a
/// non-Docker `uses:` marks the job dynamic: composite inner steps evaluate
/// against the job's `secrets` context, and remote (or local) action bodies
/// cannot be inspected at submit time, so the job keeps the full scope
/// rather than silently starving the composite. `defaults` carry
/// TemplateTokens for `shell`/`working-directory`, contexts the schema
/// already excludes `secrets` from.
pub fn collect_job_secret_reads(job: &JobPlan) -> preloop_gha_expressions::SecretReads {
    let mut reads = preloop_gha_expressions::SecretReads::default();
    for value in job.env.values() {
        collect_reads_from_text(value, &mut reads);
    }
    if let Some(condition) = &job.if_condition {
        collect_reads_from_condition(condition, &mut reads);
    }
    for value in job.container.iter().chain(job.services.iter()) {
        collect_reads_from_json(value, &mut reads);
    }
    if let Some(environment) = &job.environment {
        collect_reads_from_json(environment, &mut reads);
    }
    // A reusable call maps callee secret names to caller expressions; the
    // expressions are the caller's references.
    for expression in job.secrets_map.values() {
        collect_reads_from_text(expression, &mut reads);
    }
    for expression in job.job_outputs.values() {
        collect_reads_from_text(expression, &mut reads);
    }
    for value in job
        .concurrency_group
        .iter()
        .chain(job.concurrency_cancel_in_progress.iter())
        .chain(std::iter::once(&job.name))
    {
        collect_reads_from_text(value, &mut reads);
    }
    for step in &job.steps {
        collect_reads_from_step(step, &mut reads);
    }
    reads.names.retain(|name| !is_engine_provided(name));
    reads
}

fn collect_reads_from_step(step: &StepPlan, reads: &mut preloop_gha_expressions::SecretReads) {
    for text in step.name.iter().chain(step.run.iter()) {
        collect_reads_from_text(text, reads);
    }
    for value in step.env.values() {
        collect_reads_from_text(value, reads);
    }
    for value in step.with.values() {
        collect_reads_from_json(value, reads);
    }
    if let Some(condition) = &step.if_condition {
        collect_reads_from_condition(condition, reads);
    }
    for value in step.working_directory.iter().chain(step.shell.iter()) {
        collect_reads_from_text(value, reads);
    }
    // A step running an action may execute composite inner steps against the
    // job's `secrets` context. Remote action bodies cannot be inspected at
    // submit time, so any non-Docker `uses:` marks the job dynamic (fail
    // closed: the full scope is injected) rather than risking a silent empty
    // secret inside the composite. Docker actions have no composite steps.
    // Local composites (`./path`) are equally uninspectable here — the
    // collector has no workspace — so they fail closed the same way.
    if let Some(uses) = step.uses.as_deref()
        && !uses.starts_with("docker://")
    {
        reads.dynamic = true;
    }
}

/// Scan every `${{ … }}` span in a field that also carries literal text.
fn collect_reads_from_text(text: &str, reads: &mut preloop_gha_expressions::SecretReads) {
    let mut rest = text;
    while let Some(start) = rest.find("${{") {
        let after = &rest[start + 3..];
        let Some(end) = after.find("}}") else {
            break;
        };
        collect_reads_from_expression(&after[..end], reads);
        rest = &after[end + 2..];
    }
}

/// An `if:` value is an expression with or without the `${{ }}` markers.
fn collect_reads_from_condition(condition: &str, reads: &mut preloop_gha_expressions::SecretReads) {
    if condition.contains("${{") {
        collect_reads_from_text(condition, reads);
    } else {
        collect_reads_from_expression(condition, reads);
    }
}

fn collect_reads_from_json(value: &Value, reads: &mut preloop_gha_expressions::SecretReads) {
    match value {
        Value::String(text) => collect_reads_from_text(text, reads),
        Value::Array(items) => {
            for item in items {
                collect_reads_from_json(item, reads);
            }
        }
        Value::Object(map) => {
            for item in map.values() {
                collect_reads_from_json(item, reads);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// An expression that fails to parse is treated as dynamic: for injection
/// scoping, failing closed (whole scope) beats silently dropping a real
/// reference. Callers after names only (the preflight) ignore `dynamic` and
/// get the same literal-name set as before.
fn collect_reads_from_expression(
    expression: &str,
    reads: &mut preloop_gha_expressions::SecretReads,
) {
    match preloop_gha_expressions::collect_secret_reads(expression) {
        Ok(found) => {
            reads.names.extend(found.names);
            reads.dynamic |= found.dynamic;
        }
        Err(_) => reads.dynamic = true,
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
