//! Typed GitHub Actions workflow parser and job expander.

use std::collections::BTreeMap;

use indexmap::IndexMap;
use preloop_gha_protocol::{JobId, JobPlan, StepPlan};
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
}

/// Environment map with scalar values normalized to strings.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Env(pub BTreeMap<String, EnvValue>);

impl Env {
    fn into_strings(self) -> BTreeMap<String, String> {
        self.0
            .into_iter()
            .map(|(key, value)| (key, value.into_string()))
            .collect()
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

fn normalize_yaml_keys(value: &mut serde_yaml::Value) {
    match value {
        serde_yaml::Value::Mapping(map) => {
            if let Some(on_value) = map.remove(&serde_yaml::Value::Bool(true)) {
                map.insert(serde_yaml::Value::String("on".to_owned()), on_value);
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

fn expanded_job_id(base: &str, matrix: &BTreeMap<String, Value>) -> String {
    if matrix.is_empty() {
        return base.to_owned();
    }
    let suffix = matrix
        .iter()
        .map(|(key, value)| format!("{key}={}", value_key(value)))
        .collect::<Vec<_>>()
        .join(",");
    format!("{base}[{suffix}]")
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
) -> Result<Vec<BTreeMap<String, Value>>, ParserError> {
    let Some(matrix) = matrix else {
        return Ok(vec![BTreeMap::new()]);
    };

    let mut combinations = vec![BTreeMap::new()];
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
        let excluded = object_entry(job_id, "exclude", excluded)?;
        combinations.retain(|candidate| !matches_partial(candidate, &excluded));
    }

    for included in &matrix.include {
        let included = object_entry(job_id, "include", included)?;
        if let Some(existing) = combinations
            .iter_mut()
            .find(|candidate| can_merge_include(candidate, &included))
        {
            existing.extend(included);
        } else {
            combinations.push(included);
        }
    }

    if combinations.is_empty() {
        combinations.push(BTreeMap::new());
    }

    Ok(combinations)
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
    Ok(map.iter().map(|(key, value)| (key.clone(), value.clone())).collect())
}

fn matches_partial(candidate: &BTreeMap<String, Value>, partial: &BTreeMap<String, Value>) -> bool {
    partial
        .iter()
        .all(|(key, value)| candidate.get(key).is_some_and(|candidate| candidate == value))
}

fn can_merge_include(candidate: &BTreeMap<String, Value>, include: &BTreeMap<String, Value>) -> bool {
    include
        .iter()
        .all(|(key, value)| candidate.get(key).map_or(true, |existing| existing == value))
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
}
