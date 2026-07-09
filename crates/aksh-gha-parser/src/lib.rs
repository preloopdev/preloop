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
    /// Invalid workflow_call trigger definition.
    #[error("invalid workflow_call trigger: {0}")]
    InvalidWorkflowCallTrigger(String),
    /// Maximum nesting depth for reusable workflows exceeded.
    #[error("maximum nested reusable workflows depth (4) exceeded")]
    MaxNestingDepthExceeded,
    /// Called workflow does not declare `on: workflow_call` trigger.
    #[error("called workflow does not declare `on: workflow_call` trigger")]
    MissingWorkflowCallTrigger,
    /// Missing required input.
    #[error("missing required input `{name}` for reusable workflow")]
    MissingRequiredInput {
        /// Name of the missing input.
        name: String,
    },
    /// Undeclared input.
    #[error("caller provided input `{name}` which is not declared by the callee workflow")]
    UndeclaredInput {
        /// Name of the undeclared input.
        name: String,
    },
    /// Undeclared secret.
    #[error("caller provided secret `{name}` which is not declared by the callee workflow")]
    UndeclaredSecret {
        /// Name of the undeclared secret.
        name: String,
    },
    /// Missing required secret.
    #[error("missing required secret `{name}` for reusable workflow")]
    MissingRequiredSecret {
        /// Name of the missing secret.
        name: String,
    },
    /// Input value cannot be coerced to the declared type.
    #[error("unexpected value `{value}` for input `{name}` of type {expected_type}")]
    InvalidInputValue {
        /// Input name.
        name: String,
        /// The value that failed coercion.
        value: String,
        /// The expected type.
        expected_type: String,
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

impl Workflow {
    /// Returns the workflow_call trigger definition if the workflow is callable.
    pub fn workflow_call_trigger(&self) -> Result<Option<WorkflowCallTrigger>, ParserError> {
        match &self.on {
            Trigger::Single(s) => {
                if s == "workflow_call" {
                    Ok(Some(WorkflowCallTrigger {
                        inputs: BTreeMap::new(),
                        secrets: BTreeMap::new(),
                        outputs: BTreeMap::new(),
                    }))
                } else {
                    Ok(None)
                }
            }
            Trigger::Many(v) => {
                if v.iter().any(|s| s == "workflow_call") {
                    Ok(Some(WorkflowCallTrigger {
                        inputs: BTreeMap::new(),
                        secrets: BTreeMap::new(),
                        outputs: BTreeMap::new(),
                    }))
                } else {
                    Ok(None)
                }
            }
            Trigger::Map(map) => {
                if let Some(val) = map.get("workflow_call") {
                    if val.is_null() {
                        Ok(Some(WorkflowCallTrigger {
                            inputs: BTreeMap::new(),
                            secrets: BTreeMap::new(),
                            outputs: BTreeMap::new(),
                        }))
                    } else {
                        let trigger: WorkflowCallTrigger = serde_json::from_value(val.clone())
                            .map_err(|e| ParserError::InvalidWorkflowCallTrigger(e.to_string()))?;
                        Ok(Some(trigger))
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }
}

/// Trigger definitions for `on: workflow_call`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowCallTrigger {
    /// Inputs required or accepted by the workflow.
    #[serde(default)]
    pub inputs: BTreeMap<String, InputDefinition>,
    /// Secrets required or accepted by the workflow.
    #[serde(default)]
    pub secrets: BTreeMap<String, SecretDefinition>,
    /// Outputs produced by the workflow.
    #[serde(default)]
    pub outputs: BTreeMap<String, OutputDefinition>,
}

/// Input definition in `workflow_call`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InputDefinition {
    /// Type of the input.
    #[serde(default = "default_input_type", rename = "type")]
    pub input_type: InputType,
    /// Whether the input is required.
    #[serde(default)]
    pub required: bool,
    /// Default value for the input.
    #[serde(default)]
    pub default: Option<Value>,
    /// Description.
    #[serde(default)]
    pub description: Option<String>,
}

fn default_input_type() -> InputType {
    InputType::String
}

/// Allowed input types.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InputType {
    /// String type.
    String,
    /// Number type.
    Number,
    /// Boolean type.
    Boolean,
}

/// Secret definition in `workflow_call`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecretDefinition {
    /// Whether the secret is required.
    #[serde(default)]
    pub required: bool,
    /// Description.
    #[serde(default)]
    pub description: Option<String>,
}

/// Output definition in `workflow_call`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputDefinition {
    /// Value expression of the output (e.g. `${{ jobs.job1.outputs.out1 }}`).
    pub value: String,
    /// Description.
    #[serde(default)]
    pub description: Option<String>,
}

/// Expanded workflows and metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpandedWorkflows {
    /// Expanded job plans.
    pub jobs: Vec<JobPlan>,
    /// Metadata for reusable calls.
    pub reusable_calls: BTreeMap<String, ReusableCallMetadata>,
}

/// Metadata for a reusable workflow call.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReusableCallMetadata {
    /// Expanded caller job ID.
    pub caller_job_id: String,
    /// Output definitions (name -> value expression).
    pub output_definitions: BTreeMap<String, String>,
    /// List of expanded inner job IDs that must complete.
    pub inner_job_ids: Vec<String>,
    /// Evaluated inputs.
    #[serde(default)]
    pub inputs: BTreeMap<String, Value>,
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
        activity_type: Option<&str>,
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
                        // activity types filter
                        if let Some(types) = obj.get("types") {
                            if let Some(activity_type) = activity_type {
                                if !matches_filter(types, activity_type) {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        }
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

/// Glob matching for trigger filters.
///
/// `*` matches within a single path segment; `**` matches across path
/// separators. This intentionally keeps matching anchored to the whole value.
fn glob_match(pattern: &str, value: &str) -> bool {
    fn matches(pattern: &[char], value: &[char], pi: usize, vi: usize) -> bool {
        if pi == pattern.len() {
            return vi == value.len();
        }

        if pattern[pi] == '*' {
            let double_star = pattern.get(pi + 1) == Some(&'*');
            let next_pi = if double_star { pi + 2 } else { pi + 1 };

            if matches(pattern, value, next_pi, vi) {
                return true;
            }

            let mut next_vi = vi;
            while next_vi < value.len() {
                if !double_star && value[next_vi] == '/' {
                    break;
                }
                next_vi += 1;
                if matches(pattern, value, next_pi, next_vi) {
                    return true;
                }
            }

            return false;
        }

        vi < value.len() && pattern[pi] == value[vi] && matches(pattern, value, pi + 1, vi + 1)
    }

    let pattern: Vec<char> = pattern.chars().collect();
    let value: Vec<char> = value.chars().collect();
    matches(&pattern, &value, 0, 0)
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
    /// Job container (`container:`) — raw value, evaluated runner-side.
    #[serde(default)]
    pub container: Option<Value>,
    /// Service containers (`services:`) — raw value, evaluated runner-side.
    pub services: Option<Value>,
    /// Job-level defaults for run steps (`defaults.run`).
    #[serde(default)]
    pub defaults: Option<JobDefaults>,
    /// Job-level outputs (`outputs:` block in workflow YAML).
    /// Maps output name to value expression, e.g. `z: ${{ steps.step1.outputs.out1 }}`.
    #[serde(default)]
    pub outputs: BTreeMap<String, Value>,
}

/// `defaults:` block in a job definition.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobDefaults {
    /// Defaults for `run` steps.
    #[serde(default)]
    pub run: Option<DefaultsRun>,
}

/// `defaults.run` — default shell and working-directory for script steps.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DefaultsRun {
    /// Default shell for `run` steps.
    #[serde(default)]
    pub shell: Option<String>,
    /// Default working directory for `run` steps.
    #[serde(default, rename = "working-directory")]
    pub working_directory: Option<String>,
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
    pub if_condition: Option<String>,
    /// Working directory override.
    #[serde(default, rename = "working-directory")]
    pub working_directory: Option<String>,
    /// Shell override.
    #[serde(default)]
    pub shell: Option<String>,
    /// Whether to continue on error.
    #[serde(default, rename = "continue-on-error")]
    pub continue_on_error: Option<bool>,
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

/// Omit empty `services: {}` to match `EmitDefaultValue=false` behavior.
fn non_empty_services(services: Option<serde_json::Value>) -> Option<serde_json::Value> {
    match &services {
        Some(serde_json::Value::Object(m)) if m.is_empty() => None,
        _ => services,
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
                steps: job
                    .steps
                    .iter()
                    .cloned()
                    .map(|s| step_plan(s, &job.defaults))
                    .collect(),
                if_condition: job.if_condition.clone(),
                fail_fast: job.strategy.fail_fast.unwrap_or(true),
                max_parallel: job.strategy.max_parallel,
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
            });
        }
    }
    Ok(plans)
}

/// Coerce value to the declared input type, matching GitHub's validation.
///
/// GitHub rejects values that cannot be cleanly coerced (e.g. `"0"` for boolean,
/// `"abc"` for number). Only literal bools, `"true"`/`"false"` strings, and
/// expression placeholders are accepted for boolean inputs. Only literal numbers,
/// numeric strings, and expressions are accepted for number inputs.
fn coerce_value(
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
    }
}

fn expand_jobs_with_reusables_internal(
    workflow: &Workflow,
    reusable_workflows: &BTreeMap<String, String>,
    depth: usize,
    reusable_calls: &mut BTreeMap<String, ReusableCallMetadata>,
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

            let matrices = expand_matrix(job_id, job.strategy.matrix.as_ref())?;
            for matrix in matrices {
                let expanded_job_id = expanded_job_id(job_id, &matrix);
                let mut called_plans = expand_jobs_with_reusables_internal(
                    &called,
                    reusable_workflows,
                    depth + 1,
                    reusable_calls,
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
                    called_plan.matrix.extend(matrix.clone());
                }

                reusable_calls.insert(
                    expanded_job_id.clone(),
                    ReusableCallMetadata {
                        caller_job_id: expanded_job_id,
                        output_definitions: output_definitions.clone(),
                        inner_job_ids,
                        inputs: resolved_inputs.clone(),
                    },
                );

                plans.extend(called_plans);
            }
            continue;
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
                steps: job
                    .steps
                    .iter()
                    .cloned()
                    .map(|s| step_plan(s, &job.defaults))
                    .collect(),
                if_condition: job.if_condition.clone(),
                fail_fast: job.strategy.fail_fast.unwrap_or(true),
                max_parallel: job.strategy.max_parallel,
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
            });
        }
    }
    Ok(plans)
}

/// Expand jobs and inline local reusable workflows when their YAML is supplied.
pub fn expand_jobs_with_reusables(
    workflow: &Workflow,
    reusable_workflows: &BTreeMap<String, String>,
) -> Result<ExpandedWorkflows, ParserError> {
    let mut reusable_calls = BTreeMap::new();
    let mut plans =
        expand_jobs_with_reusables_internal(workflow, reusable_workflows, 0, &mut reusable_calls)?;

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
                let mut matched = false;
                for id in &expanded_ids {
                    if id.starts_with(&prefix) {
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

fn step_plan(step: Step, defaults: &Option<JobDefaults>) -> StepPlan {
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
    StepPlan {
        id: step.id,
        name: step.name,
        run: step.run,
        uses: step.uses,
        env: step.env.into_strings(),
        with: step.with,
        if_condition: step.if_condition,
        working_directory,
        shell,
        continue_on_error: step.continue_on_error,
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

fn _object_entry(
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

fn _can_merge_include(
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
    fn glob_match_handles_multiple_wildcards() {
        assert!(glob_match("feature/*/*", "feature/auth/login"));
        assert!(glob_match("release-*-rc*", "release-2026-rc1"));
        assert!(!glob_match("feature/*", "feature/auth/login"));
        assert!(glob_match("src/**", "src/bin/main.rs"));
    }

    #[test]
    fn trigger_context_matches_activity_types() {
        let workflow = parse_workflow(
            r#"
on:
  pull_request:
    types: [opened, synchronize]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo ok
"#,
        )
        .unwrap();

        assert!(workflow
            .on
            .matches_with_context("pull_request", None, None, &[], Some("opened")));
        assert!(!workflow
            .on
            .matches_with_context("pull_request", None, None, &[], Some("closed")));
    }

    #[test]
    fn schedule_trigger_matches_event_name() {
        let workflow = parse_workflow(
            r#"
on:
  schedule:
    - cron: '0 0 * * *'
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo ok
"#,
        )
        .unwrap();

        assert!(workflow.on.matches("schedule"));
        assert!(!workflow.on.matches("push"));
    }

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

        let jobs = expand_jobs_with_reusables(&caller, &reusable).unwrap().jobs;

        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].id.0, "call/test");
        assert_eq!(jobs[0].runs_on, vec!["ubuntu-latest"]);
    }

    #[test]
    fn reusable_workflow_secrets_inherit_flag() {
        let caller = parse_workflow(
            r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/reusable.yml
    secrets: inherit
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

        let jobs = expand_jobs_with_reusables(&caller, &reusable).unwrap().jobs;
        assert_eq!(jobs.len(), 1);
        assert!(jobs[0].secrets_inherit);
    }
    #[test]
    fn reusable_workflow_input_validation_and_coercion() {
        let caller = parse_workflow(
            r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/reusable.yml
    with:
      name: "Alice"
      enable: "true"
      count: "42"
"#,
        )
        .unwrap();

        let mut reusable = BTreeMap::new();
        reusable.insert(
            ".github/workflows/reusable.yml".to_owned(),
            r#"
on:
  workflow_call:
    inputs:
      name:
        type: string
        required: true
      enable:
        type: boolean
        required: false
      count:
        type: number
        required: false
        default: 100
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo reusable
"#
            .to_owned(),
        );

        let expanded = expand_jobs_with_reusables(&caller, &reusable).unwrap();
        let jobs = expanded.jobs;
        assert_eq!(jobs.len(), 1);
        assert_eq!(
            jobs[0].inputs.get("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(jobs[0].inputs.get("enable"), Some(&Value::Bool(true)));
        assert_eq!(jobs[0].inputs.get("count"), Some(&Value::Number(42.into())));
    }

    #[test]
    fn reusable_workflow_missing_required_input() {
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
    inputs:
      name:
        type: string
        required: true
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo reusable
"#
            .to_owned(),
        );

        let res = expand_jobs_with_reusables(&caller, &reusable);
        assert!(res.is_err());
        assert!(matches!(
            res.unwrap_err(),
            ParserError::MissingRequiredInput { .. }
        ));
    }

    #[test]
    fn reusable_workflow_undeclared_input() {
        let caller = parse_workflow(
            r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/reusable.yml
    with:
      age: 25
"#,
        )
        .unwrap();

        let mut reusable = BTreeMap::new();
        reusable.insert(
            ".github/workflows/reusable.yml".to_owned(),
            r#"
on:
  workflow_call:
    inputs:
      name:
        type: string
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo reusable
"#
            .to_owned(),
        );

        let res = expand_jobs_with_reusables(&caller, &reusable);
        assert!(res.is_err());
        assert!(matches!(
            res.unwrap_err(),
            ParserError::UndeclaredInput { .. }
        ));
    }

    #[test]
    fn reusable_workflow_max_depth_exceeded() {
        let caller = parse_workflow(
            r#"
on: push
jobs:
  call1:
    uses: ./.github/workflows/level1.yml
"#,
        )
        .unwrap();

        let mut reusable = BTreeMap::new();
        reusable.insert(
            ".github/workflows/level1.yml".to_owned(),
            "on: { workflow_call: {} }\njobs:\n  call2:\n    uses: ./.github/workflows/level2.yml"
                .to_owned(),
        );
        reusable.insert(
            ".github/workflows/level2.yml".to_owned(),
            "on: { workflow_call: {} }\njobs:\n  call3:\n    uses: ./.github/workflows/level3.yml"
                .to_owned(),
        );
        reusable.insert(
            ".github/workflows/level3.yml".to_owned(),
            "on: { workflow_call: {} }\njobs:\n  call4:\n    uses: ./.github/workflows/level4.yml"
                .to_owned(),
        );
        reusable.insert(
            ".github/workflows/level4.yml".to_owned(),
            "on: { workflow_call: {} }\njobs:\n  call5:\n    uses: ./.github/workflows/level5.yml"
                .to_owned(),
        );
        reusable.insert(
            ".github/workflows/level5.yml".to_owned(),
            "on: { workflow_call: {} }\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo leaf".to_owned(),
        );

        let res = expand_jobs_with_reusables(&caller, &reusable);
        assert!(res.is_err());
        assert!(matches!(
            res.unwrap_err(),
            ParserError::MaxNestingDepthExceeded
        ));
    }

    #[test]
    fn reusable_workflow_outer_needs_propagated() {
        let caller = parse_workflow(
            r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo build
  call:
    needs: build
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
  test1:
    runs-on: ubuntu-latest
    steps:
      - run: echo test1
  test2:
    needs: test1
    runs-on: ubuntu-latest
    steps:
      - run: echo test2
"#
            .to_owned(),
        );

        let expanded = expand_jobs_with_reusables(&caller, &reusable).unwrap();
        let jobs = expanded.jobs;
        assert_eq!(jobs.len(), 3);
        let test1 = jobs.iter().find(|j| j.id.0 == "call/test1").unwrap();
        let test2 = jobs.iter().find(|j| j.id.0 == "call/test2").unwrap();

        assert!(test1.needs.contains(&JobId("build".to_string())));
        assert!(test2.needs.contains(&JobId("call/test1".to_string())));
        assert!(test2.needs.contains(&JobId("build".to_string())));
    }

    mod coerce_value_properties {
        use super::coerce_value;
        use crate::InputType;
        use proptest::prelude::*;
        use serde_json::Value;

        /// Values that should be accepted for boolean coercion.
        fn arb_valid_bool_value() -> impl Strategy<Value = Value> {
            prop_oneof![
                any::<bool>().prop_map(Value::Bool),
                Just(Value::String("true".into())),
                Just(Value::String("false".into())),
                Just(Value::String("TRUE".into())),
                Just(Value::String("False".into())),
                Just(Value::String("${{ inputs.x }}".into())),
            ]
        }

        /// Values that should be accepted for number coercion.
        fn arb_valid_num_value() -> impl Strategy<Value = Value> {
            prop_oneof![
                (-1000i64..1000i64).prop_map(|n| Value::Number(n.into())),
                Just(Value::String("42".into())),
                Just(Value::String("-7".into())),
                Just(Value::String("3.14".into())),
                Just(Value::String("${{ inputs.x }}".into())),
            ]
        }

        proptest! {
            /// Idempotence: coercing an already-valid value twice = coercing once.
            #[test]
            fn coercion_idempotent_bool(val in arb_valid_bool_value()) {
                let once = coerce_value(&val, InputType::Boolean, "test").unwrap();
                let twice = coerce_value(&once, InputType::Boolean, "test").unwrap();
                prop_assert_eq!(once, twice);
            }

            #[test]
            fn coercion_idempotent_number(val in arb_valid_num_value()) {
                let once = coerce_value(&val, InputType::Number, "test").unwrap();
                let twice = coerce_value(&once, InputType::Number, "test").unwrap();
                prop_assert_eq!(once, twice);
            }

            /// Never panics — any value + any type produces a Result (Ok or Err), never a panic.
            #[test]
            fn coercion_never_panics(val in prop_oneof![
                Just(Value::Null),
                any::<bool>().prop_map(Value::Bool),
                (-1000i64..1000i64).prop_map(|n| Value::Number(n.into())),
                r#"[a-zA-Z0-9_ -]{0,30}"#.prop_map(Value::String),
            ]) {
                let _ = coerce_value(&val, InputType::Boolean, "test");
                let _ = coerce_value(&val, InputType::Number, "test");
                let _ = coerce_value(&val, InputType::String, "test");
            }

            /// Boolean coercion of valid values produces only Bool (or expression passthrough).
            #[test]
            fn boolean_coercion_always_bool(val in arb_valid_bool_value()) {
                let result = coerce_value(&val, InputType::Boolean, "test").unwrap();
                // Expression passthrough stays as string
                if !val.as_str().is_some_and(|s| s.starts_with("${{")) {
                    prop_assert!(result.is_boolean(),
                        "boolean coercion of {:?} produced {:?}", val, result);
                }
            }

            /// Number coercion of valid values produces only Number (or expression passthrough).
            #[test]
            fn number_coercion_always_number(val in arb_valid_num_value()) {
                let result = coerce_value(&val, InputType::Number, "test").unwrap();
                if !val.as_str().is_some_and(|s| s.starts_with("${{")) {
                    prop_assert!(result.is_number(),
                        "number coercion of {:?} produced {:?}", val, result);
                }
            }

            /// String coercion always succeeds.
            #[test]
            fn string_coercion_always_ok(val in prop_oneof![
                Just(Value::Null),
                any::<bool>().prop_map(Value::Bool),
                (-1000i64..1000i64).prop_map(|n| Value::Number(n.into())),
                r#"[a-zA-Z0-9_ -]{0,30}"#.prop_map(Value::String),
            ]) {
                let result = coerce_value(&val, InputType::String, "test");
                prop_assert!(result.is_ok(), "string coercion should always succeed");
                prop_assert!(result.unwrap().is_string());
            }

            /// Roundtrip: bool→string→bool preserves truth value.
            #[test]
            fn bool_string_bool_roundtrip(val in arb_valid_bool_value()) {
                if let Ok(b) = coerce_value(&val, InputType::Boolean, "test") {
                    let s = coerce_value(&b, InputType::String, "test").unwrap();
                    if let Ok(b2) = coerce_value(&s, InputType::Boolean, "test") {
                        if b.is_boolean() {
                            prop_assert_eq!(b, b2);
                        }
                    }
                }
            }
        }

        // Deterministic rejection tests matching GitHub behavior.

        #[test]
        fn rejects_arbitrary_string_as_boolean() {
            let result = coerce_value(&Value::String("0".into()), InputType::Boolean, "flag");
            assert!(result.is_err(), "\"0\" should be rejected for boolean");
            let result = coerce_value(&Value::String("sure".into()), InputType::Boolean, "flag");
            assert!(result.is_err(), "\"sure\" should be rejected for boolean");
            let result = coerce_value(&Value::String("yes".into()), InputType::Boolean, "flag");
            assert!(result.is_err(), "\"yes\" should be rejected for boolean");
        }

        #[test]
        fn rejects_number_as_boolean() {
            let result = coerce_value(&Value::Number(1.into()), InputType::Boolean, "flag");
            assert!(result.is_err(), "number 1 should be rejected for boolean");
        }

        #[test]
        fn rejects_null_as_boolean() {
            let result = coerce_value(&Value::Null, InputType::Boolean, "flag");
            assert!(result.is_err(), "null should be rejected for boolean");
        }

        #[test]
        fn rejects_arbitrary_string_as_number() {
            let result = coerce_value(&Value::String("abc".into()), InputType::Number, "count");
            assert!(result.is_err(), "\"abc\" should be rejected for number");
        }

        #[test]
        fn rejects_bool_as_number() {
            let result = coerce_value(&Value::Bool(true), InputType::Number, "count");
            assert!(result.is_err(), "bool should be rejected for number");
        }

        #[test]
        fn rejects_null_as_number() {
            let result = coerce_value(&Value::Null, InputType::Number, "count");
            assert!(result.is_err(), "null should be rejected for number");
        }

        #[test]
        fn accepts_true_false_strings_for_boolean() {
            assert_eq!(
                coerce_value(&Value::String("true".into()), InputType::Boolean, "f").unwrap(),
                Value::Bool(true)
            );
            assert_eq!(
                coerce_value(&Value::String("false".into()), InputType::Boolean, "f").unwrap(),
                Value::Bool(false)
            );
            assert_eq!(
                coerce_value(&Value::String("TRUE".into()), InputType::Boolean, "f").unwrap(),
                Value::Bool(true)
            );
        }

        #[test]
        fn accepts_numeric_strings_for_number() {
            assert_eq!(
                coerce_value(&Value::String("42".into()), InputType::Number, "n").unwrap(),
                Value::Number(42.into())
            );
            assert_eq!(
                coerce_value(&Value::String("-7".into()), InputType::Number, "n").unwrap(),
                Value::Number((-7).into())
            );
        }

        #[test]
        fn expression_passthrough() {
            let expr = Value::String("${{ inputs.x }}".into());
            assert_eq!(coerce_value(&expr, InputType::Boolean, "f").unwrap(), expr);
            assert_eq!(coerce_value(&expr, InputType::Number, "n").unwrap(), expr);
            assert_eq!(coerce_value(&expr, InputType::String, "s").unwrap(), expr);
        }
    }
}
