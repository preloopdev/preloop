//! Workflow parser data models.

use std::collections::BTreeMap;

use crate::expand::coerce_value;
use aksh_gha_protocol::{JobId, JobPlan};
use indexmap::IndexMap;
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
    /// A job references a dependency that does not exist after expansion.
    #[error("job `{job_id}` needs unknown job `{need}`")]
    UnknownNeed {
        /// Dependent job id.
        job_id: String,
        /// Missing dependency id.
        need: String,
    },
    /// The expanded workflow dependency graph contains a cycle.
    #[error("workflow job dependency cycle contains `{witness}`")]
    NeedsCycle {
        /// One job participating in the cycle.
        witness: String,
    },
    /// A job-level condition is syntactically invalid.
    #[error("invalid condition for job `{job_id}`: {message}")]
    InvalidJobCondition {
        /// Expanded job id.
        job_id: String,
        /// Expression parser error.
        message: String,
    },
    /// A job-level continue-on-error value is invalid or non-boolean.
    #[error("invalid continue-on-error for job `{job_id}`: {message}")]
    InvalidContinueOnError {
        /// Expanded job id.
        job_id: String,
        /// Expression or type error.
        message: String,
    },

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
    /// Invalid concurrency configuration.
    #[error("{0}")]
    InvalidConcurrency(String),
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
    /// A trigger filter key is not valid for this event.
    /// GitHub only warns, does not reject, but we flag it.
    #[error("filter key `{key}` is not valid for `on.{event}` — GitHub ignores it at runtime")]
    InvalidFilterForKey {
        /// Event name.
        event: String,
        /// The invalid filter key.
        key: String,
    },
    /// Mutually exclusive filters are both present (e.g. branches +
    /// branches-ignore).
    #[error("`{a}` and `{b}` are mutually exclusive in `on.{event}`")]
    ConflictingFilters {
        /// Event name.
        event: String,
        /// First filter.
        a: String,
        /// Second filter.
        b: String,
    },
}

/// GitHub Actions workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    /// Workflow name.
    #[serde(default)]
    pub name: Option<String>,
    /// Custom run name with expression interpolation (`run-name:`).
    #[serde(default, rename = "run-name")]
    pub run_name: Option<String>,
    /// Trigger block.
    #[serde(default, rename = "on", alias = "true")]
    pub on: Trigger,
    /// Global environment.
    #[serde(default)]
    pub env: Env,
    /// Workflow-level permissions.
    #[serde(default)]
    pub permissions: Option<Value>,
    /// Workflow-level concurrency group.
    #[serde(default)]
    pub concurrency: Option<Concurrency>,
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
                            .map_err(|error| {
                                ParserError::InvalidWorkflowCallTrigger(error.to_string())
                            })?;
                        trigger.validate()?;
                        Ok(Some(trigger))
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Apply `workflow_dispatch` defaults and validate declared input values.
    pub fn apply_workflow_dispatch_inputs(&self, payload: &mut Value) -> Result<(), ParserError> {
        let Trigger::Map(triggers) = &self.on else {
            return Ok(());
        };
        let Some(config) = triggers.get("workflow_dispatch").and_then(Value::as_object) else {
            return Ok(());
        };
        let Some(definitions) = config.get("inputs").and_then(Value::as_object) else {
            return Ok(());
        };
        let inputs = payload
            .as_object_mut()
            .ok_or_else(|| {
                ParserError::InvalidWorkflowCallTrigger(
                    "workflow_dispatch payload must be an object".to_owned(),
                )
            })?
            .entry("inputs")
            .or_insert_with(|| Value::Object(serde_json::Map::new()))
            .as_object_mut()
            .ok_or_else(|| {
                ParserError::InvalidWorkflowCallTrigger(
                    "workflow_dispatch inputs must be an object".to_owned(),
                )
            })?;
        for (name, definition) in definitions {
            let definition = definition.as_object().ok_or_else(|| {
                ParserError::InvalidWorkflowCallTrigger(format!(
                    "workflow_dispatch input `{name}` must be an object"
                ))
            })?;
            let input_type = definition
                .get("type")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .map_err(|error| ParserError::InvalidWorkflowCallTrigger(error.to_string()))?
                .unwrap_or(InputType::String);
            let supplied = inputs.remove(name).filter(|value| !value.is_null());
            let value = match supplied {
                Some(value) => coerce_value(&value, input_type, name)?,
                None => {
                    if let Some(default) = definition.get("default") {
                        default.clone()
                    } else if definition
                        .get("required")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                    {
                        return Err(ParserError::MissingRequiredInput { name: name.clone() });
                    } else {
                        match input_type {
                            InputType::Boolean => Value::Bool(false),
                            InputType::Number => Value::Number(0.into()),
                            InputType::Choice => definition
                                .get("options")
                                .and_then(Value::as_array)
                                .and_then(|options| options.first())
                                .cloned()
                                .unwrap_or_else(|| Value::String(String::new())),
                            InputType::String | InputType::Environment => {
                                Value::String(String::new())
                            }
                        }
                    }
                }
            };
            if input_type == InputType::Choice {
                let valid = definition
                    .get("options")
                    .and_then(Value::as_array)
                    .map(|options| options.iter().any(|option| option == &value))
                    .unwrap_or(false);
                if !valid {
                    return Err(ParserError::InvalidInputValue {
                        name: name.clone(),
                        value: value.to_string(),
                        expected_type: "declared choice".to_owned(),
                    });
                }
            }
            inputs.insert(name.clone(), value);
        }
        Ok(())
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
    /// Type of the reusable-workflow input. GitHub permits only boolean,
    /// number, and string for `workflow_call`.
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

/// Allowed input types. `Choice` and `Environment` are dispatch-only and are
/// rejected when a reusable workflow declares them.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InputType {
    /// String type.
    String,
    /// Number type.
    Number,
    /// Boolean type.
    Boolean,
    /// Dispatch-only choice type.
    Choice,
    /// Dispatch-only environment type.
    Environment,
}

impl WorkflowCallTrigger {
    fn validate(&self) -> Result<(), ParserError> {
        for (name, definition) in &self.inputs {
            if matches!(
                definition.input_type,
                InputType::Choice | InputType::Environment
            ) {
                let kind = if definition.input_type == InputType::Choice {
                    "choice"
                } else {
                    "environment"
                };
                return Err(ParserError::InvalidWorkflowCallTrigger(format!(
                    "input `{name}` uses `{kind}`; workflow_call supports only boolean, number, and string"
                )));
            }
        }
        Ok(())
    }
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
    /// Caller job-level concurrency (applies to the whole invocation as a JobSet).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caller_concurrency: Option<Concurrency>,
    /// Callee workflow-level concurrency (`EmbeddedConcurrency`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedded_concurrency: Option<Concurrency>,
    /// Caller strategy matrix values.
    #[serde(default)]
    pub matrix: BTreeMap<String, Value>,
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
    pub(crate) fn into_strings(self) -> BTreeMap<String, String> {
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

/// Job-level failure tolerance, either a literal or an expression evaluated
/// for each expanded matrix job.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JobContinueOnError {
    /// Literal failure tolerance.
    Bool(bool),
    /// Expression-valued failure tolerance.
    Expression(String),
}

impl EnvValue {
    pub(crate) fn into_string(self) -> String {
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
    /// Job-level failure tolerance.
    #[serde(default, rename = "continue-on-error")]
    pub continue_on_error: Option<JobContinueOnError>,
    /// Strategy block.
    #[serde(default)]
    pub strategy: Strategy,
    /// Job environment variables.
    /// Job-level concurrency group.
    #[serde(default)]
    pub concurrency: Option<Concurrency>,
    /// Job environment.
    #[serde(default)]
    pub env: Env,
    /// Deployment environment, scalar or `{ name, url }` mapping.
    #[serde(default)]
    pub environment: Option<Value>,
    /// Job-level permissions.
    #[serde(default)]
    pub permissions: Option<Value>,
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
    pub(crate) fn labels(&self) -> Vec<String> {
        match self {
            Self::Single(value) => vec![value.clone()],
            Self::Many(values) => values.clone(),
            Self::Dynamic(Value::String(value)) => vec![value.clone()],
            Self::Dynamic(Value::Object(object)) => object
                .get("labels")
                .map(|labels| match labels {
                    Value::String(value) => vec![value.clone()],
                    Value::Array(values) => values
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect(),
                    _ => Vec::new(),
                })
                .unwrap_or_default(),
            Self::Dynamic(_) => Vec::new(),
        }
    }

    /// Return the explicit runner group from object-valued `runs-on`.
    pub(crate) fn group(&self) -> Option<String> {
        match self {
            Self::Dynamic(Value::Object(object)) => object
                .get("group")
                .and_then(Value::as_str)
                .map(str::to_owned),
            _ => None,
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
    pub(crate) fn ids(&self) -> Vec<JobId> {
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

/// Concurrency queue mode for a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConcurrencyQueue {
    /// At most one pending holder; newer arrivals replace existing pending.
    #[default]
    Single,
    /// Up to 100 pending holders wait FIFO.
    Max,
}

/// Workflow- or job-level concurrency configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Concurrency {
    /// Raw group string; may contain `${{ }}` — evaluated server-side.
    pub group: String,
    /// Raw `cancel-in-progress` value: "true" / "false" / a `${{ }}` expression.
    pub cancel_in_progress: Option<String>,
    /// Queue mode.
    pub queue: ConcurrencyQueue,
}

impl<'de> Deserialize<'de> for Concurrency {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct ConcurrencyMap {
            group: String,
            #[serde(default, rename = "cancel-in-progress")]
            cancel_in_progress: Option<CancelInProgressValue>,
            #[serde(default)]
            queue: Option<String>,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum CancelInProgressValue {
            Bool(bool),
            String(String),
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum ConcurrencyRaw {
            String(String),
            Map(ConcurrencyMap),
        }

        match ConcurrencyRaw::deserialize(deserializer)? {
            ConcurrencyRaw::String(group) => Ok(Self {
                group,
                cancel_in_progress: None,
                queue: ConcurrencyQueue::Single,
            }),
            ConcurrencyRaw::Map(map) => {
                let queue = match map.queue.as_deref() {
                    None | Some("single") => ConcurrencyQueue::Single,
                    Some("max") => ConcurrencyQueue::Max,
                    Some(other) => {
                        return Err(serde::de::Error::custom(format!(
                            "concurrency.queue must be `single` or `max`, got `{other}`"
                        )));
                    }
                };
                let cancel_in_progress = map.cancel_in_progress.map(|v| match v {
                    CancelInProgressValue::Bool(b) => b.to_string(),
                    CancelInProgressValue::String(s) => s,
                });
                if matches!(queue, ConcurrencyQueue::Max)
                    && cancel_in_progress.as_deref() == Some("true")
                {
                    return Err(serde::de::Error::custom(
                        "concurrency: `queue: max` cannot be combined with `cancel-in-progress: true`",
                    ));
                }
                Ok(Self {
                    group: map.group,
                    cancel_in_progress,
                    queue,
                })
            }
        }
    }
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
