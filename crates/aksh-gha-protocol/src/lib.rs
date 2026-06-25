//! Shared domain and wire models for aksh's GitHub Actions control plane.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use indexmap::IndexMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

/// Azure DevOps wire-format DTOs for the official runner protocol.
pub mod azdo;

/// RSA/AES session encryption for the runner protocol.
pub mod crypto;

/// Protocol version exposed by this crate's runner-compatible DTOs.
pub const PROTOCOL_VERSION: &str = "2026-06-25.aksh.v1";

/// Stable identifier for a workflow run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(pub Uuid);

impl RunId {
    /// Create a fresh run id.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for RunId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for RunId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

/// Stable identifier for an expanded job.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(pub String);

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Stable identifier for a runner session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(pub Uuid);

impl SessionId {
    /// Create a fresh runner session id.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

/// Secret value that redacts itself in logs and serialized output.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);

impl SecretString {
    /// Wrap a secret value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the secret value for explicit use at the protocol boundary.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretString(<redacted>)")
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl Serialize for SecretString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str("<redacted>")
    }
}

impl<'de> Deserialize<'de> for SecretString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self)
    }
}

/// Redaction-safe map of secret names to values.
pub type SecretMap = BTreeMap<String, SecretString>;

/// Incoming workflow submission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowSubmission {
    /// YAML workflow contents.
    pub workflow_yaml: String,
    /// GitHub event name such as `push` or `workflow_dispatch`.
    pub event: String,
    /// Event payload JSON.
    #[serde(default)]
    pub payload: serde_json::Value,
    /// Repository slug or local identifier.
    pub repository: String,
    /// Git ref for the run.
    #[serde(default = "default_ref")]
    pub git_ref: String,
    /// Caller-provided variables.
    #[serde(default)]
    pub vars: BTreeMap<String, String>,
    /// Caller-provided secrets.
    #[serde(default)]
    pub secrets: SecretMap,
    /// Local reusable workflow YAML keyed by repository-relative path.
    #[serde(default)]
    pub reusable_workflows: BTreeMap<String, String>,
}

fn default_ref() -> String {
    "refs/heads/main".to_owned()
}

/// Result returned after accepting a workflow run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunAccepted {
    /// New run id.
    pub run_id: RunId,
    /// Number of expanded jobs queued for runners.
    pub queued_jobs: usize,
}

/// Status of a workflow run or job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    /// Object exists but has not started.
    Queued,
    /// Object is currently running.
    InProgress,
    /// Object completed successfully.
    Success,
    /// Object completed with a failure.
    Failure,
    /// Object was skipped by condition or dependency.
    Skipped,
    /// Object was cancelled.
    Cancelled,
}

/// A parsed and expanded job ready to send to a runner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobPlan {
    /// Expanded job id.
    pub id: JobId,
    /// Original workflow job id before matrix suffixing.
    pub base_id: String,
    /// Display name.
    pub name: String,
    /// Required runner labels.
    pub runs_on: Vec<String>,
    /// Dependency job ids.
    #[serde(default)]
    pub needs: Vec<JobId>,
    /// Matrix values for this expansion.
    #[serde(default)]
    pub matrix: BTreeMap<String, serde_json::Value>,
    /// Job-level environment.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Ordered step plan.
    #[serde(default)]
    pub steps: Vec<StepPlan>,
    /// Optional `if` expression.
    #[serde(default)]
    pub if_condition: Option<String>,
}

/// A workflow step after normalization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepPlan {
    /// Stable step id if provided.
    #[serde(default)]
    pub id: Option<String>,
    /// Display name.
    #[serde(default)]
    pub name: Option<String>,
    /// Shell script body.
    #[serde(default)]
    pub run: Option<String>,
    /// Action reference from `uses`.
    #[serde(default)]
    pub uses: Option<String>,
    /// Step environment.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Step inputs.
    #[serde(default)]
    pub with: BTreeMap<String, serde_json::Value>,
    /// Optional `if` expression.
    #[serde(default)]
    pub if_condition: Option<String>,
}

/// Context material sent to a runner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerJobMessage {
    /// Protocol version.
    pub protocol_version: String,
    /// Run id.
    pub run_id: RunId,
    /// Job plan.
    pub job: JobPlan,
    /// GitHub context.
    pub github: serde_json::Value,
    /// Variables context.
    #[serde(default)]
    pub vars: BTreeMap<String, String>,
    /// Environment context.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

impl RunnerJobMessage {
    /// Build a message with the current protocol version.
    pub fn new(run_id: RunId, job: JobPlan, github: serde_json::Value) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            run_id,
            job,
            github,
            vars: BTreeMap::new(),
            env: BTreeMap::new(),
        }
    }
}

/// Runner registration request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerRegistrationRequest {
    /// Runner name.
    pub name: String,
    /// Runner labels.
    #[serde(default)]
    pub labels: Vec<String>,
    /// Ephemeral runner flag.
    #[serde(default)]
    pub ephemeral: bool,
}

/// Registered runner state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredRunner {
    /// Numeric id used by GitHub runner APIs.
    pub id: i64,
    /// Runner name.
    pub name: String,
    /// Runner labels.
    pub labels: Vec<String>,
    /// Ephemeral runner flag.
    pub ephemeral: bool,
}

/// Runner session creation request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerSessionRequest {
    /// Runner id.
    pub runner_id: i64,
    /// Runner name.
    pub name: String,
}

/// Runner session response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerSession {
    /// Session id.
    pub session_id: SessionId,
    /// Runner id.
    pub runner_id: i64,
}

/// Step or job outputs.
pub type OutputMap = IndexMap<String, serde_json::Value>;

/// Job completion request from a runner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCompletion {
    /// Run id.
    pub run_id: RunId,
    /// Job id.
    pub job_id: JobId,
    /// Final status.
    pub status: ExecutionStatus,
    /// Outputs captured by the runner.
    #[serde(default)]
    pub outputs: OutputMap,
}

/// Machine-readable event emitted as NDJSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NdjsonEvent {
    /// Run was accepted.
    RunAccepted {
        /// Run id.
        run_id: RunId,
        /// Number of queued jobs.
        queued_jobs: usize,
    },
    /// Job status changed.
    JobStatus {
        /// Run id.
        run_id: RunId,
        /// Job id.
        job_id: JobId,
        /// New status.
        status: ExecutionStatus,
    },
    /// Log line was appended.
    Log {
        /// Run id.
        run_id: RunId,
        /// Job id.
        job_id: JobId,
        /// Raw log line.
        line: String,
    },
    /// Annotation was emitted.
    Annotation {
        /// Run id.
        run_id: RunId,
        /// Job id.
        job_id: JobId,
        /// Annotation level.
        level: AnnotationLevel,
        /// Message.
        message: String,
        /// Optional file path.
        file: Option<String>,
        /// Optional line number.
        line: Option<u64>,
    },
    /// Run-level status changed.
    RunStatus {
        /// Run id.
        run_id: RunId,
        /// New status.
        status: ExecutionStatus,
    },
}

/// Annotation severity.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnnotationLevel {
    /// Notice.
    Notice,
    /// Warning.
    Warning,
    /// Error.
    Error,
}

/// Protocol crate error.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// JSON serialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Serialize an event as a single NDJSON line.
pub fn event_to_ndjson(event: &NdjsonEvent) -> Result<String, ProtocolError> {
    let mut line = serde_json::to_string(event)?;
    line.push('\n');
    Ok(line)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_debug_display_and_json_are_redacted() {
        let secret = SecretString::new("super-secret");

        assert_eq!(format!("{secret:?}"), "SecretString(<redacted>)");
        assert_eq!(secret.to_string(), "<redacted>");
        assert_eq!(serde_json::to_string(&secret).unwrap(), "\"<redacted>\"");
        assert_eq!(secret.expose(), "super-secret");
    }
}
