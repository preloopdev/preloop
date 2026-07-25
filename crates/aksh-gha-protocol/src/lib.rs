//! Shared domain and wire models for aksh's GitHub Actions control plane.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use indexmap::IndexMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

/// Azure DevOps wire-format DTOs for the official runner protocol.
pub mod azdo;
pub use azdo::DebuggerTunnelInfo;

/// RSA/AES session encryption for the runner protocol.
pub mod crypto;

/// Shared secret-masking logic (longest-first, exclusion-aware).
pub mod masking;

/// Protocol version exposed by this crate's runner-compatible DTOs.
pub const PROTOCOL_VERSION: &str = "2026-06-25.aksh.v1";

/// Line a runner prints on stdout the moment it accepts a job.
///
/// An ephemeral runner is single-use, so the orchestrator supervising it can
/// start building its replacement as soon as the current one is spoken for
/// rather than waiting for the job to end. This is a private channel between
/// our runner and our orchestrator; it never crosses the GitHub wire protocol.
pub const RUNNER_BUSY_SENTINEL: &str = "::preloop-runner::job-acquired";

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

/// Complete workflow request submitted to the control plane.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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
    /// Repository-relative path of the submitted workflow file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_path: Option<String>,
    /// Canonical host workspace used only by a trusted local control plane.
    ///
    /// This is accepted on input but never returned from run APIs, so host
    /// filesystem layout does not leak into run metadata.
    #[serde(default, skip_serializing)]
    pub local_workspace: Option<String>,
    /// Caller-provided variables.
    #[serde(default)]
    pub vars: BTreeMap<String, String>,
    /// Workflow dispatch or call inputs.
    #[serde(default)]
    pub inputs: BTreeMap<String, serde_json::Value>,
    /// Caller-provided secrets.
    #[serde(default)]
    pub secrets: SecretMap,
    /// Local reusable workflow YAML keyed by repository-relative path.
    #[serde(default)]
    pub reusable_workflows: BTreeMap<String, String>,
    /// Resolved commit SHA for each remote reusable workflow reference.
    #[serde(default)]
    pub reusable_workflow_shas: BTreeMap<String, String>,
    /// Enable DAP debugger for the run's jobs.
    #[serde(default)]
    pub enable_debugger: bool,
    /// Welcome message to show when debugger attaches.
    #[serde(default)]
    pub debugger_welcome_message: Option<String>,
    /// Commit SHA for the run. Defaults to zeroes if not supplied.
    #[serde(default = "default_sha")]
    pub sha: String,
    /// The actor (user) who initiated the run. Defaults to `"aksh-system"`.
    #[serde(default = "default_actor")]
    pub actor: String,
    /// Deployment environment name (for OIDC `sub` claim formatting).
    #[serde(default)]
    pub environment: Option<String>,
    /// Workflow filename (e.g. `"ci.yml"`). Derived from YAML or overridden.
    #[serde(default)]
    pub workflow_file: Option<String>,
    /// Trust tier assigned by the webhook dispatcher. The server enforces its
    /// repository-secret policy before building a job; it does not grant an
    /// untrusted payload permission to select a more trusted tier.
    #[serde(default)]
    pub trust_tier: Option<String>,
    /// Upstream workflow display names for `on.workflow_run.workflows:` filter
    /// enforcement. Populated from `workflow_run.name` by the adapter.
    #[serde(default)]
    pub workflow_run_upstream_names: Vec<String>,
    /// Activity type for the event (for example `opened`, `synchronize`, or
    /// `submitted`). Set by the dispatcher so submission does not reinterpret
    /// event-specific payload fields.
    #[serde(default)]
    pub activity_type: Option<String>,
    /// Resolved SHA for the run's `github.sha` context. A webhook adapter owns
    /// this value because it differs from payload `after` for PR-family events.
    #[serde(default)]
    pub resolved_sha: Option<String>,
    /// Explicitly resolved changed paths. An empty list is meaningful only
    /// when `changed_paths_known` is true.
    #[serde(default)]
    pub changed_paths: Vec<String>,
    /// Whether `changed_paths` represents a complete change set.
    #[serde(default)]
    pub changed_paths_known: bool,
    /// Branch used for trigger filtering, independent of `git_ref`.
    #[serde(default)]
    pub filter_branch: Option<String>,
    /// Typed workflow_dispatch inputs.
    #[serde(default)]
    pub dispatch_inputs: BTreeMap<String, serde_json::Value>,
    /// String-valued workflow_dispatch inputs for `github.event.inputs`.
    #[serde(default)]
    pub dispatch_inputs_stringified: BTreeMap<String, String>,
    /// Run only these jobs (by YAML key) and their transitive `needs:`
    /// dependencies. An empty list means run all jobs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub selected_jobs: Vec<String>,
    /// Explicit base ref for the run (populates `github.base_ref`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_ref: Option<String>,
    /// Keep the failed job VM alive for interactive debugging.
    #[serde(default)]
    pub preserve_on_failure: bool,
}

impl WorkflowSubmission {
    /// Serialize for transmission *to* the control plane, exposing secret values.
    ///
    /// [`SecretString`] redacts on `Serialize` so that a submission embedded in
    /// server state (for example `RunRecord`) can never leak secrets through an
    /// API response. Sending secrets is the one legitimate exception, so it is
    /// opt-in at the call site rather than a property of the type.
    pub fn to_request_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut value = serde_json::to_value(self)?;
        if self.secrets.is_empty() {
            return Ok(value);
        }
        let exposed = self
            .secrets
            .iter()
            .map(|(name, secret)| {
                (
                    name.clone(),
                    serde_json::Value::String(secret.expose().to_owned()),
                )
            })
            .collect();
        if let Some(object) = value.as_object_mut() {
            object.insert("secrets".to_owned(), serde_json::Value::Object(exposed));
        }
        Ok(value)
    }
}

fn default_ref() -> String {
    "refs/heads/main".to_owned()
}

fn default_sha() -> String {
    "0000000000000000000000000000000000000000".to_owned()
}

fn default_actor() -> String {
    "aksh-system".to_owned()
}

/// Result returned after accepting a workflow run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunAccepted {
    /// New run id.
    pub run_id: RunId,
    /// Monotonic run number for this workflow path.
    pub run_number: u64,
    /// Number of expanded jobs queued for runners.
    pub queued_jobs: usize,
}

/// Status of a workflow run or job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    /// Object exists but has not started.
    Queued,
    /// Object is waiting on a concurrency group (not runnable yet).
    Pending,
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

impl ExecutionStatus {
    /// Whether the run or job has reached a final state and will not change.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Success | Self::Failure | Self::Skipped | Self::Cancelled
        )
    }
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
    /// Display name of the required runner group from object-valued `runs-on`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_group: Option<String>,
    /// Required runner labels.
    pub runs_on: Vec<String>,
    /// Dependency job ids.
    #[serde(default)]
    pub needs: Vec<JobId>,
    /// Matrix values for this expansion.
    #[serde(default)]
    pub matrix: IndexMap<String, serde_json::Value>,
    /// Job-level environment.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Ordered step plan.
    #[serde(default)]
    pub steps: Vec<StepPlan>,
    /// Optional `if` expression.
    #[serde(default)]
    pub if_condition: Option<String>,
    /// Whether sibling matrix jobs should be cancelled after a failure.
    #[serde(default = "default_fail_fast")]
    pub fail_fast: bool,
    /// Whether a failed job is allowed to complete as success.
    #[serde(default)]
    pub continue_on_error: bool,
    /// Maximum concurrent matrix jobs for this base job.
    #[serde(default)]
    pub max_parallel: Option<u64>,
    /// Whether this job inherits all parent secrets (reusable workflow `secrets: inherit`).
    #[serde(default)]
    pub secrets_inherit: bool,
    /// Raw `container:` value, string or mapping, un-evaluated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<serde_json::Value>,
    /// Raw `services:` mapping, un-evaluated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub services: Option<serde_json::Value>,
    /// Inputs passed to a reusable workflow.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub inputs: BTreeMap<String, serde_json::Value>,
    /// Path of the called workflow file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_file: Option<String>,
    /// Resolved workflow ref.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_ref: Option<String>,
    /// Resolved workflow commit SHA.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_sha: Option<String>,
    /// Resolved workflow repository.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_repository: Option<String>,
    /// Map of called secret name to the expression/value passed by the caller.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secrets_map: BTreeMap<String, String>,
    /// Job-level output declarations: output name → value expression.
    /// The runner evaluates these after step execution and includes results in completejob.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub job_outputs: BTreeMap<String, String>,
    /// Effective `id-token: write` permission after reusable-workflow reduction.
    #[serde(default)]
    pub oidc_id_token_granted: bool,
    /// Resolved deployment environment used for OIDC claims.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oidc_environment: Option<String>,
    /// Executing reusable workflow reference, when this job came from one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oidc_job_workflow_ref: Option<String>,
    /// Raw job-level concurrency group expression/string (server-evaluated).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_group: Option<String>,
    /// Raw job-level `cancel-in-progress` value: "true"/"false"/expression.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_cancel_in_progress: Option<String>,
    /// Job-level concurrency queue mode: `"single"` or `"max"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_queue: Option<String>,
}

fn default_fail_fast() -> bool {
    true
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
    pub if_condition: Option<String>,
    /// Working directory for `run` steps.
    #[serde(default)]
    pub working_directory: Option<String>,
    /// Shell override for `run` steps.
    #[serde(default)]
    pub shell: Option<String>,
    /// Whether to continue on error.
    #[serde(default)]
    pub continue_on_error: Option<bool>,
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
    /// Runner RSA public key material (XML/JWK/PEM depending on client).
    #[serde(default)]
    pub public_key: Option<String>,
    /// Numeric runner group identifier. Missing means the default group.
    #[serde(
        default,
        alias = "runnerGroupId",
        skip_serializing_if = "Option::is_none"
    )]
    pub runner_group_id: Option<i64>,
    /// Runner group display name. Missing means the default group.
    #[serde(
        default,
        alias = "runnerGroupName",
        skip_serializing_if = "Option::is_none"
    )]
    pub runner_group_name: Option<String>,
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
    /// Runner RSA public key material, if supplied at registration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_key: Option<String>,
    /// Numeric runner group identifier. Missing means the default group.
    #[serde(
        default,
        alias = "runnerGroupId",
        skip_serializing_if = "Option::is_none"
    )]
    pub runner_group_id: Option<i64>,
    /// Runner group display name. Missing means the default group.
    #[serde(
        default,
        alias = "runnerGroupName",
        skip_serializing_if = "Option::is_none"
    )]
    pub runner_group_name: Option<String>,
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
        /// Optional status reason (`concurrency_pending`, `concurrency_cancelled`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        file: Option<String>,
        /// Optional start line number.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        line: Option<u64>,
        /// Optional end line number.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end_line: Option<u64>,
        /// Optional start column number.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        col: Option<u64>,
        /// Optional end column number.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end_column: Option<u64>,
        /// Optional annotation title.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// Optional step/record ID.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        step_id: Option<String>,
    },
    /// Run-level status changed.
    RunStatus {
        /// Run id.
        run_id: RunId,
        /// New status.
        status: ExecutionStatus,
        /// Optional status reason (`concurrency_pending`, `concurrency_cancelled`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// Job completed with result and outputs.
    JobCompleted {
        /// Run id.
        run_id: RunId,
        /// Job id.
        job_id: JobId,
        /// Final status.
        status: ExecutionStatus,
        /// Job outputs.
        #[serde(default)]
        outputs: BTreeMap<String, String>,
    },
}

impl NdjsonEvent {
    /// Run this event belongs to.
    pub fn run_id(&self) -> RunId {
        match self {
            Self::RunAccepted { run_id, .. }
            | Self::JobStatus { run_id, .. }
            | Self::Log { run_id, .. }
            | Self::Annotation { run_id, .. }
            | Self::RunStatus { run_id, .. }
            | Self::JobCompleted { run_id, .. } => *run_id,
        }
    }

    /// Final run-level status carried by this event, if any.
    ///
    /// Only a terminal `RunStatus` closes an event stream: job-level terminals
    /// still leave sibling jobs running.
    pub fn terminal_run_status(&self) -> Option<ExecutionStatus> {
        match self {
            Self::RunStatus { status, .. } if status.is_terminal() => Some(*status),
            _ => None,
        }
    }
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

/// JSON payload exchanged between the runner and server over the live console
/// feed WebSocket. Matches the official runner's `TimelineRecordFeedLinesWrapper`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveLogFeedLinesWrapper {
    /// Step/timeline record GUID.
    pub step_id: String,
    /// First line number in this batch, 1-indexed within the step.
    pub start_line: u64,
    /// Number of lines in `value`.
    pub count: usize,
    /// Console lines for this step.
    pub value: Vec<String>,
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

    #[test]
    fn workflow_submission_redacts_secrets_on_plain_serialization() {
        let submission = submission_with_secrets();

        // Anything that embeds a submission in server state (RunRecord) and
        // serializes it must never emit the real values.
        let json = serde_json::to_string(&submission).unwrap();

        assert!(
            !json.contains("actual-value") && !json.contains("another-secret"),
            "plain serialization leaked a secret value: {json}"
        );
        assert!(json.contains("<redacted>"), "expected redaction marker");

        #[derive(Serialize)]
        struct Nested {
            submission: WorkflowSubmission,
        }
        let nested = serde_json::to_string(&Nested { submission }).unwrap();
        assert!(
            !nested.contains("actual-value") && !nested.contains("another-secret"),
            "nested serialization leaked a secret value: {nested}"
        );
    }

    #[test]
    fn workflow_submission_request_json_exposes_secrets_for_the_wire() {
        let submission = submission_with_secrets();

        let value = submission.to_request_json().unwrap();

        assert_eq!(value["secrets"]["TOKEN"], "actual-value");
        assert_eq!(value["secrets"]["KEY"], "another-secret");
        assert!(
            !value.to_string().contains("<redacted>"),
            "request JSON still contains a redaction marker"
        );

        // The server recovers the real values from the request body.
        let deserialized: WorkflowSubmission = serde_json::from_value(value).unwrap();
        assert_eq!(deserialized.secrets["TOKEN"].expose(), "actual-value");
        assert_eq!(deserialized.secrets["KEY"].expose(), "another-secret");
    }

    #[test]
    fn workflow_submission_request_json_matches_plain_shape_without_secrets() {
        let submission = WorkflowSubmission::default();

        assert_eq!(
            submission.to_request_json().unwrap(),
            serde_json::to_value(&submission).unwrap()
        );
    }

    fn submission_with_secrets() -> WorkflowSubmission {
        let mut submission = WorkflowSubmission::default();
        submission
            .secrets
            .insert("TOKEN".to_owned(), SecretString::new("actual-value"));
        submission
            .secrets
            .insert("KEY".to_owned(), SecretString::new("another-secret"));
        submission
    }

    #[test]
    fn annotation_event_serializes_optional_source_fields() {
        let event = NdjsonEvent::Annotation {
            run_id: RunId(uuid::Uuid::nil()),
            job_id: JobId("job".into()),
            level: AnnotationLevel::Warning,
            message: "warning".into(),
            file: Some("src/lib.rs".into()),
            line: Some(7),
            end_line: Some(8),
            col: Some(3),
            end_column: Some(9),
            title: Some("Compiler".into()),
            step_id: None,
        };
        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["type"], "annotation");
        assert_eq!(value["file"], "src/lib.rs");
        assert_eq!(value["line"], 7);
        assert_eq!(value["end_line"], 8);
        assert_eq!(value["col"], 3);
        assert_eq!(value["end_column"], 9);
        assert_eq!(value["title"], "Compiler");
        assert!(value.get("step_id").is_none());
    }
}
