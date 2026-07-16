//! Typed GitHub Actions workflow parser and job expander.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use aksh_gha_protocol::{JobId, JobPlan, StepPlan};
use indexmap::IndexMap;

/// Workflow dependency graph validation.
pub mod dag;
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

impl Trigger {
    /// Returns true when the workflow should run for an event.
    pub fn matches(&self, event: &str) -> bool {
        match self {
            Trigger::Single(value) => value == event,
            Trigger::Many(values) => values.iter().any(|value| value == event),
            Trigger::Map(values) => values.contains_key(event),
        }
    }

    /// Whether the event configuration contains path-based filters.
    pub fn has_path_filters(&self, event: &str) -> bool {
        matches!(
            self,
            Trigger::Map(values)
                if values.get(event).and_then(Value::as_object).is_some_and(|config| {
                    config.contains_key("paths") || config.contains_key("paths-ignore")
                })
        )
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
        upstream_workflow_paths: &[String],
    ) -> bool {
        match self {
            Trigger::Single(value) => value == event,
            Trigger::Many(values) => values.iter().any(|value| value == event),
            Trigger::Map(values) => {
                if !values.contains_key(event) {
                    return false;
                }
                // Check branch/tag/path filters
                let config_val = values.get(event);
                if let Some(config) = config_val {
                    if let Some(obj) = config.as_object() {
                        // activity types filter
                        let types_val = obj.get("types");
                        if types_val.is_some() {
                            let types = types_val.unwrap();
                            if let Some(activity_type) = activity_type {
                                if !matches_filter(types, activity_type) {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        } else if event == "pull_request" || event == "pull_request_target" {
                            // Default types per MessageController.cs:1259-1268
                            const PR_DEFAULT_TYPES: &[&str] =
                                &["opened", "synchronize", "synchronized", "reopened"];
                            if let Some(activity_type) = activity_type {
                                if !PR_DEFAULT_TYPES.contains(&activity_type) {
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
                        // A `paths` filter requires at least one known changed
                        // path matching the positive pattern.
                        if let Some(path_filters) = obj.get("paths") {
                            if paths.is_empty()
                                || !paths.iter().any(|path| matches_filter(path_filters, path))
                            {
                                return false;
                            }
                        }
                        // `paths-ignore` suppresses only when every changed
                        // path is ignored. A mixed change set must still run.
                        if let Some(ignore) = obj.get("paths-ignore") {
                            if !paths.is_empty()
                                && paths.iter().all(|path| matches_filter(ignore, path))
                            {
                                return false;
                            }
                        }
                        // `workflow_run.workflows` matches the upstream
                        // workflow display name, not its file path.
                        if let Some(wf_filter) = obj.get("workflows") {
                            if upstream_workflow_paths.is_empty()
                                || !upstream_workflow_paths
                                    .iter()
                                    .any(|name| matches_filter(wf_filter, name))
                            {
                                return false;
                            }
                        }
                    } else if event == "pull_request" || event == "pull_request_target" {
                        // Config exists but is null/empty (e.g. `on:\n  pull_request:`).
                        // Apply default types per MessageController.cs:1259-1268.
                        const PR_DEFAULT_TYPES: &[&str] =
                            &["opened", "synchronize", "synchronized", "reopened"];
                        if let Some(activity_type) = activity_type {
                            if !PR_DEFAULT_TYPES.contains(&activity_type) {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                }
                true
            }
        }
    }

    /// Returns the set of valid filter keys for a given event name.
    /// Mirrors MessageController.cs:994-1020.
    pub fn valid_filter_keys(event: &str) -> &'static [&'static str] {
        match event {
            "push" => &[
                "branches",
                "branches-ignore",
                "tags",
                "tags-ignore",
                "paths",
                "paths-ignore",
            ],
            "pull_request" | "pull_request_target" => &[
                "types",
                "branches",
                "branches-ignore",
                "paths",
                "paths-ignore",
            ],
            "workflow_run" => &["types", "branches", "branches-ignore", "workflows"],
            "schedule" => &["cron", "timezone"],
            _ => &["types"],
        }
    }

    /// Validate filter keys for an event. Returns Ok(()) or
    /// ParserError::InvalidFilterForKey (a warning — GitHub only warns,
    /// does not reject the workflow).
    pub fn validate_filters(&self, event: &str) -> Result<(), ParserError> {
        if let Trigger::Map(values) = self {
            if let Some(config) = values.get(event) {
                if let Some(obj) = config.as_object() {
                    let valid = Self::valid_filter_keys(event);
                    for key in obj.keys() {
                        if !valid.contains(&key.as_str()) {
                            return Err(ParserError::InvalidFilterForKey {
                                event: event.to_owned(),
                                key: key.clone(),
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Check for mutually exclusive filter pairs. Mirrors
    /// MessageController.cs:1236-1250.
    pub fn check_conflicting_filters(&self, event: &str) -> Result<(), ParserError> {
        if let Trigger::Map(values) = self {
            if let Some(config) = values.get(event) {
                if let Some(obj) = config.as_object() {
                    let pairs: &[(&str, &str)] = &[
                        ("branches", "branches-ignore"),
                        ("tags", "tags-ignore"),
                        ("paths", "paths-ignore"),
                    ];
                    for &(a, b) in pairs {
                        if obj.contains_key(a) && obj.contains_key(b) {
                            return Err(ParserError::ConflictingFilters {
                                event: event.to_owned(),
                                a: a.to_owned(),
                                b: b.to_owned(),
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Check whether a filter value matches GitHub's ordered pattern semantics.
fn matches_filter(filter: &Value, value: &str) -> bool {
    matches_filter_with_default(filter, value, false)
}

fn matches_filter_with_default(filter: &Value, value: &str, default: bool) -> bool {
    let patterns: Vec<&str> = match filter {
        Value::String(pattern) => vec![pattern.as_str()],
        Value::Array(patterns) => patterns.iter().filter_map(Value::as_str).collect(),
        _ => return false,
    };
    if patterns.is_empty() {
        return default;
    }
    let mut matched = default;
    for pattern in patterns {
        let (negative, pattern) = pattern
            .strip_prefix('!')
            .map_or((false, pattern), |p| (true, p));
        if glob_match(pattern, value) {
            matched = !negative;
        }
    }
    matched
}

/// GitHub-style glob matching anchored to the whole value.
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
        if pattern[pi] == '?' {
            return vi < value.len() && value[vi] != '/' && matches(pattern, value, pi + 1, vi + 1);
        }
        vi < value.len() && pattern[pi] == value[vi] && matches(pattern, value, pi + 1, vi + 1)
    }
    matches(
        &pattern.chars().collect::<Vec<_>>(),
        &value.chars().collect::<Vec<_>>(),
        0,
        0,
    )
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
        for matrix in expand_matrix(job_id, job.strategy.matrix.as_ref())? {
            let oidc_environment = oidc_environment(job.environment.as_ref(), &matrix);
            let expanded_id = expanded_job_id(job_id, &matrix);
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
            ));
        }
    }
    dag::validate_job_plans(&plans)?;
    Ok(plans)
}

fn job_plan_from_job(
    job_id: &str,
    job: &Job,
    expanded_id: String,
    matrix: IndexMap<String, Value>,
    env: BTreeMap<String, String>,
    oidc_environment: Option<String>,
    workflow_permissions: Option<&Value>,
) -> JobPlan {
    let (concurrency_group, concurrency_cancel_in_progress, concurrency_queue) =
        concurrency_fields(job.concurrency.as_ref());
    JobPlan {
        id: JobId(expanded_id),
        base_id: job_id.to_owned(),
        name: job.name.clone().unwrap_or_else(|| job_id.to_owned()),
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
        oidc_id_token_granted: id_token_granted(job.permissions.as_ref().or(workflow_permissions)),
        oidc_environment,
        oidc_job_workflow_ref: None,
        concurrency_group,
        concurrency_cancel_in_progress,
        concurrency_queue,
    }
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
                    },
                );

                plans.extend(called_plans);
            }
            continue;
        }

        for matrix in expand_matrix(job_id, job.strategy.matrix.as_ref())? {
            let oidc_environment = oidc_environment(job.environment.as_ref(), &matrix);
            let expanded_id = expanded_job_id(job_id, &matrix);
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
            ));
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

        assert!(workflow.on.matches_with_context(
            "pull_request",
            None,
            None,
            &[],
            Some("opened"),
            &[]
        ));
        assert!(!workflow.on.matches_with_context(
            "pull_request",
            None,
            None,
            &[],
            Some("closed"),
            &[]
        ));
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
    fn records_oidc_permission_and_matrix_environment() {
        let workflow = parse_workflow(
            r#"
on: push
permissions:
  id-token: write
jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: ${{ matrix.environment }}
    strategy:
      matrix:
        environment: [staging, production]
    steps:
      - run: echo deploy
"#,
        )
        .unwrap();

        let jobs = expand_jobs(&workflow).unwrap();
        assert_eq!(jobs.len(), 2);
        assert!(jobs.iter().all(|job| job.oidc_id_token_granted));
        assert_eq!(jobs[0].oidc_environment.as_deref(), Some("staging"));
        assert_eq!(jobs[1].oidc_environment.as_deref(), Some("production"));
    }

    #[test]
    fn reusable_oidc_permission_requires_caller_grant() {
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
permissions:
  id-token: write
jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: production
    steps:
      - run: echo deploy
"#
            .to_owned(),
        );

        let jobs = expand_jobs_with_reusables(&caller, &reusable).unwrap().jobs;
        assert_eq!(jobs.len(), 1);
        assert!(!jobs[0].oidc_id_token_granted);
        assert_eq!(jobs[0].oidc_environment.as_deref(), Some("production"));
        assert_eq!(
            jobs[0].oidc_job_workflow_ref.as_deref(),
            Some("./.github/workflows/reusable.yml")
        );
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
    #[test]
    fn preserves_job_output_expressions() {
        let workflow = parse_workflow(
            r#"jobs:
  producer:
    runs-on: self-hosted
    outputs:
      value: ${{ steps.gen.outputs.value }}
    steps:
      - id: gen
        run: echo value=42 >> "$GITHUB_OUTPUT"
"#,
        )
        .unwrap();
        let plans = expand_jobs(&workflow).unwrap();
        assert_eq!(
            plans[0].job_outputs.get("value").map(String::as_str),
            Some("${{ steps.gen.outputs.value }}")
        );
    }
    #[test]
    fn concurrency_bare_string_shorthand() {
        let wf = parse_workflow(
            r#"
on: push
concurrency: ci-${{ github.ref }}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#,
        )
        .unwrap();
        let c = wf.concurrency.unwrap();
        assert_eq!(c.group, "ci-${{ github.ref }}");
        assert_eq!(c.cancel_in_progress, None);
        assert_eq!(c.queue, ConcurrencyQueue::Single);
    }

    #[test]
    fn concurrency_mapping_form() {
        let wf = parse_workflow(
            r#"
on: push
concurrency:
  group: g
  cancel-in-progress: true
  queue: single
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#,
        )
        .unwrap();
        let c = wf.concurrency.unwrap();
        assert_eq!(c.group, "g");
        assert_eq!(c.cancel_in_progress.as_deref(), Some("true"));
    }

    #[test]
    fn concurrency_preserves_expression_cancel() {
        let wf = parse_workflow(
            r#"
on: push
concurrency:
  group: g
  cancel-in-progress: ${{ github.ref == 'refs/heads/main' }}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#,
        )
        .unwrap();
        let c = wf.concurrency.unwrap();
        assert_eq!(
            c.cancel_in_progress.as_deref(),
            Some("${{ github.ref == 'refs/heads/main' }}")
        );
    }

    #[test]
    fn concurrency_queue_max_with_literal_cancel_is_error() {
        let err = parse_workflow(
            r#"
on: push
concurrency:
  group: g
  cancel-in-progress: true
  queue: max
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("queue: max") && msg.contains("cancel-in-progress"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn job_level_concurrency_on_plan() {
        let wf = parse_workflow(
            r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    concurrency:
      group: jg
      cancel-in-progress: false
    steps:
      - run: echo hi
"#,
        )
        .unwrap();
        let plans = expand_jobs(&wf).unwrap();
        assert_eq!(plans[0].concurrency_group.as_deref(), Some("jg"));
        assert_eq!(
            plans[0].concurrency_cancel_in_progress.as_deref(),
            Some("false")
        );
    }
}
