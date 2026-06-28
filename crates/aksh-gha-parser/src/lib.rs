//! Typed GitHub Actions workflow parser and job expander.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use aksh_gha_protocol::{JobId, JobPlan, StepPlan};
use indexmap::IndexMap;

/// Expression evaluation for workflow fields.
pub mod eval;

/// Build `AgentJobRequestMessage` from parsed workflow data.
pub mod job_builder;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Parser error.
#[derive(Debug, thiserror::Error)]
pub enum ParserError {
    /// YAML deserialization failed.
    #[error("workflow yaml error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    /// Workflow did not define jobs.
    #[error("workflow does not define any jobs")]
    EmptyJobs,
    /// Matrix include/exclude entries must be objects.
    #[error("matrix entry for `{job_id}` in `{field}` must be an object")]
    InvalidMatrixEntry {
        /// Job id.
        job_id: String,
        /// Matrix field.
        field: &'static str,
    },
    /// Local reusable workflow was referenced but not supplied.
    #[error("local reusable workflow `{path}` was not found")]
    MissingReusableWorkflow {
        /// Referenced workflow path.
        path: String,
    },
}

/// GitHub Actions workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    /// Workflow name.
    #[serde(default)]
    pub name: Option<String>,
    /// Trigger block.
    #[serde(default, rename = "on", alias = "true")]
    pub on: Trigger,
    /// Global environment.
    #[serde(default)]
    pub env: Env,
    /// Job definitions.
    pub jobs: IndexMap<String, Job>,
}

/// Trigger syntax.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Trigger {
    /// Single event.
    Single(String),
    /// List of events.
    Many(Vec<String>),
    /// Mapping of event name to options.
    Map(IndexMap<String, Value>),
}

impl Default for Trigger {
    fn default() -> Self {
        Self::Many(Vec::new())
    }
}

impl Trigger {
    /// Returns true when the workflow should run for an event.
    pub fn matches(&self, event: &str) -> bool {
        match self {
            Trigger::Single(value) => value == event,
            Trigger::Many(values) => values.iter().any(|value| value == event),
            Trigger::Map(values) => values.contains_key(event),
        }
    }

    /// Returns true when the workflow should run for an event with context.
    /// Supports branch/tag/path filtering.
    pub fn matches_with_context(
        &self,
        event: &str,
        branch: Option<&str>,
        tag: Option<&str>,
        paths: &[String],
    ) -> bool {
        match self {
            Trigger::Single(value) => value == event,
            Trigger::Many(values) => values.iter().any(|value| value == event),
            Trigger::Map(values) => {
                if !values.contains_key(event) {
                    return false;
                }
                // Check branch/tag/path filters
                if let Some(config) = values.get(event) {
                    if let Some(obj) = config.as_object() {
                        // branches filter
                        if let Some(branches) = obj.get("branches") {
                            if let Some(branch) = branch {
                                if !matches_filter(branches, branch) {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        }
                        // branches-ignore
                        if let Some(ignore) = obj.get("branches-ignore") {
                            if let Some(branch) = branch {
                                if matches_filter(ignore, branch) {
                                    return false;
                                }
                            }
                        }
                        // tags filter
                        if let Some(tags) = obj.get("tags") {
                            if let Some(tag) = tag {
                                if !matches_filter(tags, tag) {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        }
                        // tags-ignore
                        if let Some(ignore) = obj.get("tags-ignore") {
                            if let Some(tag) = tag {
                                if matches_filter(ignore, tag) {
                                    return false;
                                }
                            }
                        }
                        // paths filter
                        if let Some(path_filters) = obj.get("paths") {
                            if !paths.is_empty()
                                && !paths.iter().any(|p| matches_filter(path_filters, p))
                            {
                                return false;
                            }
                        }
                        // paths-ignore
                        if let Some(ignore) = obj.get("paths-ignore") {
                            if paths.iter().any(|p| matches_filter(ignore, p)) {
                                return false;
                            }
                        }
                    }
                }
                true
            }
        }
    }
}

/// Check if a value matches a filter pattern (string or array of strings with globs).
fn matches_filter(filter: &Value, value: &str) -> bool {
    match filter {
        Value::String(pattern) => glob_match(pattern, value),
        Value::Array(patterns) => patterns.iter().any(|p| {
            if let Value::String(pattern) = p {
                glob_match(pattern, value)
            } else {
                false
            }
        }),
        _ => false,
    }
}

/// Simple glob matching (supports * and **).
fn glob_match(pattern: &str, value: &str) -> bool {
    // Simple glob: * matches any characters, ** matches path separators too
    if pattern == "*" {
        return true;
    }
    if pattern.contains("**") {
        // Double star: match across path separators
        let parts: Vec<&str> = pattern.split("**").collect();
        if parts.len() == 2 {
            let prefix = parts[0].trim_end_matches('/');
            let suffix = parts[1].trim_start_matches('/');
            if !prefix.is_empty() && !value.starts_with(prefix) {
                return false;
            }
            if !suffix.is_empty() {
                let remaining = if prefix.is_empty() {
                    value
                } else {
                    value.strip_prefix(prefix).unwrap_or(value)
                };
                return remaining.ends_with(suffix) || remaining.contains(suffix);
            }
            return true;
        }
    }
    // Single star: match within a path segment
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 2 {
        let prefix = parts[0];
        let suffix = parts[1];
        return value.starts_with(prefix) && value.ends_with(suffix);
    }
    pattern == value
}

/// Environment map with scalar values normalized to strings.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Env {
    /// No environment.
    #[default]
    Empty,
    /// Mapping environment.
    Map(BTreeMap<String, EnvValue>),
    /// Expression-valued environment such as `${{ secrets }}`.
    Expression(String),
}

impl Env {
    fn into_strings(self) -> BTreeMap<String, String> {
        match self {
            Self::Empty => BTreeMap::new(),
            Self::Map(values) => values
                .into_iter()
                .map(|(key, value)| (key, value.into_string()))
                .collect(),
            Self::Expression(value) => {
                BTreeMap::from([("__aksh_env_expression".to_owned(), value)])
            }
        }
    }
}

/// Environment scalar.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EnvValue {
    /// String.
    String(String),
    /// Bool.
    Bool(bool),
    /// Number.
    Number(serde_json::Number),
    /// Null.
    Null,
}

impl EnvValue {
    fn into_string(self) -> String {
        match self {
            Self::String(value) => value,
            Self::Bool(value) => value.to_string(),
            Self::Number(value) => value.to_string(),
            Self::Null => String::new(),
        }
    }
}

/// Workflow job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// Display name.
    #[serde(default)]
    pub name: Option<String>,
    /// Runner labels.
    #[serde(default, rename = "runs-on")]
    pub runs_on: RunsOn,
    /// Dependencies.
    #[serde(default)]
    pub needs: Needs,
    /// Optional if condition.
    #[serde(default, rename = "if")]
    pub if_condition: Option<String>,
    /// Strategy block.
    #[serde(default)]
    pub strategy: Strategy,
    /// Job environment.
    #[serde(default)]
    pub env: Env,
    /// Steps.
    #[serde(default)]
    pub steps: Vec<Step>,
    /// Reusable workflow reference.
    #[serde(default)]
    pub uses: Option<String>,
    /// Inputs for reusable workflows or actions.
    #[serde(default)]
    pub with: BTreeMap<String, Value>,
    /// Secrets policy for reusable workflows.
    #[serde(default)]
    pub secrets: Option<Value>,
}

/// `runs-on` syntax.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RunsOn {
    /// Single label.
    Single(String),
    /// Multiple labels.
    Many(Vec<String>),
    /// Expression or object-valued runner selector.
    Dynamic(Value),
}

impl Default for RunsOn {
    fn default() -> Self {
        Self::Many(Vec::new())
    }
}

impl RunsOn {
    fn labels(&self) -> Vec<String> {
        match self {
            Self::Single(value) => vec![value.clone()],
            Self::Many(values) => values.clone(),
            Self::Dynamic(Value::String(value)) => vec![value.clone()],
            Self::Dynamic(_) => Vec::new(),
        }
    }
}

/// `needs` syntax.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Needs {
    /// No dependencies.
    #[default]
    None,
    /// Single dependency.
    Single(String),
    /// Multiple dependencies.
    Many(Vec<String>),
}

impl Needs {
    fn ids(&self) -> Vec<JobId> {
        match self {
            Self::None => Vec::new(),
            Self::Single(value) => vec![JobId(value.clone())],
            Self::Many(values) => values.iter().cloned().map(JobId).collect(),
        }
    }
}

/// Strategy block.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Strategy {
    /// Matrix block.
    #[serde(default)]
    pub matrix: Option<Matrix>,
    /// Fail-fast flag.
    #[serde(default, rename = "fail-fast")]
    pub fail_fast: Option<bool>,
    /// Max parallel jobs.
    #[serde(default, rename = "max-parallel")]
    pub max_parallel: Option<u64>,
}

/// Matrix definition.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Matrix {
    /// Include entries.
    #[serde(default)]
    pub include: Vec<Value>,
    /// Exclude entries.
    #[serde(default)]
    pub exclude: Vec<Value>,
    /// Axes captured as arbitrary values.
    #[serde(flatten)]
    pub axes: IndexMap<String, Value>,
}

/// Workflow step.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Step {
    /// Step id.
    #[serde(default)]
    pub id: Option<String>,
    /// Step display name.
    #[serde(default)]
    pub name: Option<String>,
    /// Shell script.
    #[serde(default)]
    pub run: Option<String>,
    /// Action reference.
    #[serde(default)]
    pub uses: Option<String>,
    /// Step environment.
    #[serde(default)]
    pub env: Env,
    /// Step inputs.
    #[serde(default)]
    pub with: BTreeMap<String, Value>,
    /// Optional if condition.
    #[serde(default, rename = "if")]
    pub if_condition: Option<String>,
}

/// Action metadata from `action.yml` or `action.yaml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionMetadata {
    /// Action display name.
    pub name: String,
    /// Action description.
    #[serde(default)]
    pub description: Option<String>,
    /// Action inputs.
    #[serde(default)]
    pub inputs: BTreeMap<String, ActionInput>,
    /// Action outputs.
    #[serde(default)]
    pub outputs: BTreeMap<String, ActionOutput>,
    /// Runtime definition.
    pub runs: ActionRuns,
}

/// Action input metadata.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActionInput {
    /// Description.
    #[serde(default)]
    pub description: Option<String>,
    /// Required flag.
    #[serde(default)]
    pub required: bool,
    /// Default value.
    #[serde(default)]
    pub default: Option<Value>,
}

/// Action output metadata.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActionOutput {
    /// Description.
    #[serde(default)]
    pub description: Option<String>,
    /// Output value expression.
    #[serde(default)]
    pub value: Option<String>,
}

/// Action runtime metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "using", rename_all = "kebab-case")]
pub enum ActionRuns {
    /// Composite action.
    Composite {
        /// Composite action steps.
        #[serde(default)]
        steps: Vec<Step>,
    },
    /// Node 12 action.
    Node12 {
        /// Main script.
        main: String,
        /// Optional pre script.
        #[serde(default)]
        pre: Option<String>,
        /// Optional post script.
        #[serde(default)]
        post: Option<String>,
    },
    /// Node 16 action.
    Node16 {
        /// Main script.
        main: String,
        /// Optional pre script.
        #[serde(default)]
        pre: Option<String>,
        /// Optional post script.
        #[serde(default)]
        post: Option<String>,
    },
    /// Node 20 action.
    Node20 {
        /// Main script.
        main: String,
        /// Optional pre script.
        #[serde(default)]
        pre: Option<String>,
        /// Optional post script.
        #[serde(default)]
        post: Option<String>,
    },
    /// Docker action.
    Docker {
        /// Docker image or Dockerfile.
        image: String,
        /// Entrypoint override.
        #[serde(default)]
        entrypoint: Option<String>,
        /// Arguments.
        #[serde(default)]
        args: Vec<String>,
    },
}

/// Parse workflow YAML.
pub fn parse_workflow(input: &str) -> Result<Workflow, ParserError> {
    let mut value: serde_yaml::Value = serde_yaml::from_str(input)?;
    normalize_yaml_keys(&mut value);
    let workflow: Workflow = serde_yaml::from_value(value)?;
    if workflow.jobs.is_empty() {
        return Err(ParserError::EmptyJobs);
    }
    Ok(workflow)
}

/// Parse local action metadata from `action.yml` or `action.yaml`.
pub fn parse_action_metadata(input: &str) -> Result<ActionMetadata, ParserError> {
    let mut value: serde_yaml::Value = serde_yaml::from_str(input)?;
    normalize_yaml_keys(&mut value);
    Ok(serde_yaml::from_value(value)?)
}

fn normalize_yaml_keys(value: &mut serde_yaml::Value) {
    match value {
        serde_yaml::Value::Mapping(map) => {
            if let Some(on_value) = map.remove(serde_yaml::Value::Bool(true)) {
                map.insert(serde_yaml::Value::String("on".to_owned()), on_value);
            }
            let keys = map.keys().cloned().collect::<Vec<_>>();
            for key in keys {
                if !matches!(key, serde_yaml::Value::String(_)) {
                    if let Some(value) = map.remove(key.clone()) {
                        map.insert(serde_yaml::Value::String(yaml_key_to_string(&key)), value);
                    }
                }
            }
            for value in map.values_mut() {
                normalize_yaml_keys(value);
            }
        }
        serde_yaml::Value::Sequence(values) => {
            for value in values {
                normalize_yaml_keys(value);
            }
        }
        _ => {}
    }
}

fn yaml_key_to_string(value: &serde_yaml::Value) -> String {
    match value {
        serde_yaml::Value::String(value) => value.clone(),
        serde_yaml::Value::Bool(value) => value.to_string(),
        serde_yaml::Value::Number(value) => value.to_string(),
        serde_yaml::Value::Null => "null".to_owned(),
        other => serde_yaml::to_string(other)
            .unwrap_or_default()
            .trim()
            .to_owned(),
    }
}

/// Expand all jobs for a workflow.
pub fn expand_jobs(workflow: &Workflow) -> Result<Vec<JobPlan>, ParserError> {
    let mut plans = Vec::new();
    let global_env = workflow.env.clone().into_strings();
    for (job_id, job) in &workflow.jobs {
        for matrix in expand_matrix(job_id, job.strategy.matrix.as_ref())? {
            let expanded_id = expanded_job_id(job_id, &matrix);
            let mut env = global_env.clone();
            env.extend(job.env.clone().into_strings());
            plans.push(JobPlan {
                id: JobId(expanded_id),
                base_id: job_id.clone(),
                name: job.name.clone().unwrap_or_else(|| job_id.clone()),
                runs_on: job.runs_on.labels(),
                needs: job.needs.ids(),
                matrix,
                env,
                steps: job.steps.iter().cloned().map(step_plan).collect(),
                if_condition: job.if_condition.clone(),
            });
        }
    }
    Ok(plans)
}

/// Expand jobs and inline local reusable workflows when their YAML is supplied.
pub fn expand_jobs_with_reusables(
    workflow: &Workflow,
    reusable_workflows: &BTreeMap<String, String>,
) -> Result<Vec<JobPlan>, ParserError> {
    let mut plans = Vec::new();
    let global_env = workflow.env.clone().into_strings();
    for (job_id, job) in &workflow.jobs {
        if let Some(uses) = &job.uses {
            if is_local_reusable_workflow(uses) {
                let path = normalize_reusable_path(uses);
                let yaml = reusable_workflows
                    .get(&path)
                    .ok_or_else(|| ParserError::MissingReusableWorkflow { path: path.clone() })?;
                let called = parse_workflow(yaml)?;
                let mut called_plans = expand_jobs(&called)?;
                for called_plan in &mut called_plans {
                    let old_id = called_plan.id.0.clone();
                    called_plan.id = JobId(format!("{job_id}/{old_id}"));
                    called_plan.base_id = format!("{job_id}/{}", called_plan.base_id);
                    called_plan.needs = called_plan
                        .needs
                        .iter()
                        .map(|need| JobId(format!("{job_id}/{}", need.0)))
                        .collect();
                    called_plan.env.extend(global_env.clone());
                    called_plan.env.extend(job.env.clone().into_strings());
                }
                plans.extend(called_plans);
                continue;
            }
        }

        for matrix in expand_matrix(job_id, job.strategy.matrix.as_ref())? {
            let expanded_id = expanded_job_id(job_id, &matrix);
            let mut env = global_env.clone();
            env.extend(job.env.clone().into_strings());
            plans.push(JobPlan {
                id: JobId(expanded_id),
                base_id: job_id.clone(),
                name: job.name.clone().unwrap_or_else(|| job_id.clone()),
                runs_on: job.runs_on.labels(),
                needs: job.needs.ids(),
                matrix,
                env,
                steps: job.steps.iter().cloned().map(step_plan).collect(),
                if_condition: job.if_condition.clone(),
            });
        }
    }
    Ok(plans)
}

fn is_local_reusable_workflow(uses: &str) -> bool {
    uses.starts_with("./") || uses.starts_with(".github/")
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

fn step_plan(step: Step) -> StepPlan {
    StepPlan {
        id: step.id,
        name: step.name,
        run: step.run,
        uses: step.uses,
        env: step.env.into_strings(),
        with: step.with,
        if_condition: step.if_condition,
    }
}

fn expanded_job_id(base: &str, matrix: &IndexMap<String, Value>) -> String {
    if matrix.is_empty() {
        return base.to_owned();
    }
    // GitHub format: "name (v1, v2)" with values in declaration order
    let values: Vec<String> = matrix.values().map(value_key).collect();
    format!("{base} ({})", values.join(", "))
}

fn value_key(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn expand_matrix(
    job_id: &str,
    matrix: Option<&Matrix>,
) -> Result<Vec<IndexMap<String, Value>>, ParserError> {
    let Some(matrix) = matrix else {
        return Ok(vec![IndexMap::new()]);
    };

    // Use IndexMap to preserve declaration order
    let mut combinations: Vec<IndexMap<String, Value>> = vec![IndexMap::new()];
    for (axis, values) in &matrix.axes {
        let axis_values = match values {
            Value::Array(values) => values.clone(),
            value => vec![value.clone()],
        };
        combinations = combinations
            .into_iter()
            .flat_map(|existing| {
                axis_values.iter().cloned().map(move |value| {
                    let mut next = existing.clone();
                    next.insert(axis.clone(), value);
                    next
                })
            })
            .collect();
    }

    for excluded in &matrix.exclude {
        let excluded = object_entry_indexed(job_id, "exclude", excluded)?;
        combinations.retain(|candidate| !matches_partial(candidate, &excluded));
    }

    for included in &matrix.include {
        let included = object_entry_indexed(job_id, "include", included)?;
        if let Some(existing) = combinations
            .iter_mut()
            .find(|candidate| can_merge_include_indexed(candidate, &included))
        {
            existing.extend(included);
        } else {
            combinations.push(included);
        }
    }

    if combinations.is_empty() {
        combinations.push(IndexMap::new());
    }

    Ok(combinations)
}

fn object_entry_indexed(
    job_id: &str,
    field: &'static str,
    value: &Value,
) -> Result<IndexMap<String, Value>, ParserError> {
    let Value::Object(map) = value else {
        return Err(ParserError::InvalidMatrixEntry {
            job_id: job_id.to_owned(),
            field,
        });
    };
    Ok(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
}

fn can_merge_include_indexed(
    candidate: &IndexMap<String, Value>,
    include: &IndexMap<String, Value>,
) -> bool {
    include
        .iter()
        .all(|(key, value)| candidate.get(key).is_none_or(|existing| existing == value))
}

fn object_entry(
    job_id: &str,
    field: &'static str,
    value: &Value,
) -> Result<BTreeMap<String, Value>, ParserError> {
    let Value::Object(map) = value else {
        return Err(ParserError::InvalidMatrixEntry {
            job_id: job_id.to_owned(),
            field,
        });
    };
    Ok(map
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect())
}

fn matches_partial(candidate: &IndexMap<String, Value>, partial: &IndexMap<String, Value>) -> bool {
    partial.iter().all(|(key, value)| {
        candidate
            .get(key)
            .is_some_and(|candidate| candidate == value)
    })
}

fn can_merge_include(
    candidate: &IndexMap<String, Value>,
    include: &IndexMap<String, Value>,
) -> bool {
    include
        .iter()
        .all(|(key, value)| candidate.get(key).is_none_or(|existing| existing == value))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_and_expands_matrix() {
        let workflow = parse_workflow(
            r#"
name: ci
on: [push]
env:
  GLOBAL: true
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
        node: [20, 22]
        exclude:
          - os: macos-latest
            node: 20
        include:
          - os: ubuntu-latest
            node: 24
            experimental: true
    steps:
      - run: echo hi
"#,
        )
        .unwrap();

        assert!(workflow.on.matches("push"));
        let jobs = expand_jobs(&workflow).unwrap();
        assert_eq!(jobs.len(), 4);
        assert!(jobs.iter().any(|job| {
            job.matrix.get("node") == Some(&json!(24))
                && job.matrix.get("experimental") == Some(&json!(true))
        }));
        assert_eq!(jobs[0].env.get("GLOBAL"), Some(&"true".to_owned()));
    }

    #[test]
    fn parses_local_action_metadata() {
        let action = parse_action_metadata(
            r#"
name: local composite
description: test action
inputs:
  who:
    required: true
    default: world
runs:
  using: composite
  steps:
    - run: echo "hello ${{ inputs.who }}"
"#,
        )
        .unwrap();

        assert_eq!(action.name, "local composite");
        assert!(matches!(action.runs, ActionRuns::Composite { .. }));
        assert!(action.inputs.get("who").is_some_and(|input| input.required));
    }

    #[test]
    fn expands_local_reusable_workflow_call_jobs() {
        let caller = parse_workflow(
            r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/reusable.yml
"#,
        )
        .unwrap();
        let mut reusable = BTreeMap::new();
        reusable.insert(
            ".github/workflows/reusable.yml".to_owned(),
            r#"
on:
  workflow_call:
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo reusable
"#
            .to_owned(),
        );

        let jobs = expand_jobs_with_reusables(&caller, &reusable).unwrap();

        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].id.0, "call/test");
        assert_eq!(jobs[0].runs_on, vec!["ubuntu-latest"]);
    }
}
