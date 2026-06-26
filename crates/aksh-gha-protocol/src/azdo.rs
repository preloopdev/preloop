//! Azure DevOps wire-format DTOs for the runner protocol.
//!
//! These types model the exact JSON shapes the official `actions/runner`
//! (`Runner.Listener`) sends and expects. Field names follow the C#
//! property casing conventions from `GitHub.DistributedTask.WebApi`.
//!
//! Source of truth:
//! - `actions/runner` (C# client side): `src/Runner.Common/Util/RunnerServer.cs`
//! - `runner.server` (C# server side): `src/Runner.Server/Controllers/MessageController.cs`
//! - `GitHub.DistributedTask.WebApi` NuGet package (shared DTOs)

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ─── Runner lifecycle DTOs ────────────────────────────────────────────────

/// Service location data returned by `GET _apis/connectionData`.
///
/// The runner calls this first to discover which service GUIDs map to
/// which base URLs. The response is a JSON document with `locationServiceData`
/// containing a `serviceDefinitions` array.
///
/// Upstream source: `ConnectionDataController.cs`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionData {
    #[serde(rename = "instanceId", skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    #[serde(rename = "locationServiceData", skip_serializing_if = "Option::is_none")]
    pub location_service_data: Option<LocationServiceData>,
}

/// Access mapping for location service resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessMapping {
    #[serde(rename = "moniker", skip_serializing_if = "Option::is_none")]
    pub moniker: Option<String>,
    #[serde(rename = "displayName", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "accessPoint", skip_serializing_if = "Option::is_none")]
    pub access_point: Option<String>,
}

/// Location service data — maps service GUIDs to URL locations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationServiceData {
    #[serde(rename = "serviceDefinitions", default, skip_serializing_if = "Vec::is_empty")]
    pub service_definitions: Vec<ServiceDefinition>,
    #[serde(rename = "accessMappings", default, skip_serializing_if = "Vec::is_empty")]
    pub access_mappings: Vec<AccessMapping>,
    #[serde(rename = "defaultAccessMappingMoniker", skip_serializing_if = "Option::is_none")]
    pub default_access_mapping_moniker: Option<String>,
}

/// A location mapping for a service definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationMapping {
    #[serde(rename = "accessMappingMoniker", skip_serializing_if = "Option::is_none")]
    pub access_mapping_moniker: Option<String>,
    #[serde(rename = "location", skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
}

/// A single service definition mapping a GUID to a URL location.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceDefinition {
    #[serde(rename = "serviceType", skip_serializing_if = "Option::is_none")]
    pub service_type: Option<String>,
    #[serde(rename = "identifier", skip_serializing_if = "Option::is_none")]
    pub identifier: Option<String>,
    #[serde(rename = "displayName", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "relativePath", skip_serializing_if = "Option::is_none")]
    pub relative_path: Option<String>,
    #[serde(rename = "description", skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "toolId", skip_serializing_if = "Option::is_none")]
    pub tool_id: Option<String>,
    #[serde(rename = "locationMappings", skip_serializing_if = "Option::is_none")]
    pub location_mappings: Option<Vec<LocationMapping>>,
}

/// Runner agent registration request.
///
/// The runner sends its RSA public key during registration.
/// Upstream source: `AgentController.cs`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAgent {
    #[serde(rename = "id", skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "version", skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(
        rename = "osDescription",
        skip_serializing_if = "Option::is_none"
    )]
    pub os_description: Option<String>,
}

/// Encryption key for a session.
///
/// If `encrypted` is true, the `value` is RSA-OAEP wrapped and must be
/// decrypted with the runner's private key before use as an AES key.
///
/// Upstream source: `AgentSessionController.cs`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionKey {
    /// The raw or wrapped key bytes (base64-encoded in JSON).
    #[serde(rename = "value")]
    pub value: Vec<u8>,
    /// Whether this key is RSA-wrapped (true) or plaintext (false).
    #[serde(rename = "encrypted")]
    pub encrypted: bool,
}

/// Agent session creation response.
///
/// Returned after `POST .../pools/{poolId}/sessions`. Contains the
/// AES encryption key (possibly RSA-wrapped) that the runner uses to
/// decrypt all subsequent `TaskAgentMessage` bodies.
///
/// Upstream source: `AgentSessionController.cs`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAgentSession {
    #[serde(rename = "sessionId")]
    pub session_id: uuid::Uuid,
    #[serde(rename = "encryptionKey")]
    pub encryption_key: EncryptionKey,
}

/// Runner session creation request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionRequest {
    #[serde(rename = "agent")]
    pub agent: TaskAgent,
    #[serde(rename = "sessionName", skip_serializing_if = "Option::is_none")]
    pub session_name: Option<String>,
}

// ─── Message queue DTOs ───────────────────────────────────────────────────

/// An encrypted message from the server to the runner.
///
/// The runner long-polls `GET .../messages?sessionId=X&lastMessageId=Y`
/// and receives this. The `body` field is base64-encoded and, if the
/// session has encryption enabled, must be AES-decrypted using the
/// session's `encryptionKey` and the `iv` field.
///
/// Upstream source: `MessageController.cs` (server) and
/// `MessageListener.cs` (runner)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAgentMessage {
    #[serde(rename = "messageId")]
    pub message_id: i64,
    #[serde(rename = "messageType")]
    pub message_type: String,
    /// Base64-encoded body. Encrypted if the session uses encryption.
    #[serde(rename = "body")]
    pub body: String,
    /// Base64-encoded initialization vector for AES decryption.
    /// Present only when the message body is encrypted.
    #[serde(rename = "iv", skip_serializing_if = "Option::is_none")]
    pub iv: Option<Vec<u8>>,
}

/// Known message types the runner handles.
pub mod message_type {
    /// A job request — body contains an encrypted `AgentJobRequestMessage`.
    pub const PIPELINE_AGENT_JOB_REQUEST: &str = "PipelineAgentJobRequest";
    /// Cancellation signal — runner should abort the current job.
    pub const CANCEL_JOB: &str = "CancelJob";
    /// Job cancellation (newer API).
    pub const JOB_CANCELLED: &str = "JobCancelled";
    /// Runner should shut down gracefully.
    pub const RUNNER_SHUTDOWN: &str = "RunnerShutdown";
}

// ─── Job message DTOs ─────────────────────────────────────────────────────

/// The full job payload — the most complex DTO in the protocol.
///
/// After decryption, the `AgentJobRequestMessage` contains everything
/// the runner needs to execute a job: the plan reference, job definition,
/// timeline, variables, secrets, service endpoints, steps, and all context
/// data.
///
/// This is what the runner receives from the message queue and uses to
/// start executing steps.
///
/// Upstream source: `AgentJobRequestMessage.cs` in the WebApi package
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentJobRequestMessage {
    /// The orchestration plan reference (run ID + job ID).
    #[serde(rename = "jobId")]
    pub job_id: uuid::Uuid,

    /// The request ID for this job dispatch (a sequential integer).
    #[serde(rename = "requestId")]
    pub request_id: i64,

    /// The plan reference — plan ID and type.
    #[serde(rename = "plan")]
    pub plan: PlanReference,

    /// The timeline reference for this job's records.
    #[serde(rename = "timeline")]
    pub timeline: TimelineReference,

    /// The job display name.
    #[serde(rename = "displayName", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,

    /// The job's `if` condition expression string.
    /// The runner evaluates this — do NOT pre-collapse.
    #[serde(rename = "condition", skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,

    /// Variables available to the job (env + system vars + secrets).
    #[serde(rename = "variables", default)]
    pub variables: BTreeMap<String, VariableValue>,

    /// Mask hints for secret values — tells the runner what to redact in logs.
    #[serde(rename = "maskHints", default)]
    pub mask_hints: Vec<MaskHint>,

    /// Service endpoints (e.g. SystemVssConnection with OAuth token).
    #[serde(rename = "resources")]
    pub resources: TaskResources,

    /// Context data for expression evaluation.
    /// Contains `github`, `env`, `vars`, `matrix`, `strategy`, `needs`, etc.
    #[serde(rename = "contextData", default)]
    pub context_data: BTreeMap<String, PipelineContextData>,

    /// The steps to execute.
    #[serde(rename = "steps", default)]
    pub steps: Vec<TaskStep>,

    /// Actions download info — maps `uses:` references to download URLs.
    #[serde(rename = "actionsDownloadInfo", default)]
    pub actions_download_info: BTreeMap<String, ActionsDownloadInfo>,

    /// The job's `runs-on` labels.
    #[serde(rename = "jobDisplayName", skip_serializing_if = "Option::is_none")]
    pub job_display_name: Option<String>,

    /// Whether this is a retry attempt.
    #[serde(rename = "retryCount", skip_serializing_if = "Option::is_none")]
    pub retry_count: Option<i32>,

    /// Pre-job timeout (seconds).
    #[serde(rename = "preJobTimeout", skip_serializing_if = "Option::is_none")]
    pub pre_job_timeout: Option<i64>,

    /// Job timeout (seconds).
    #[serde(rename = "jobTimeout", skip_serializing_if = "Option::is_none")]
    pub job_timeout: Option<i64>,
}

/// Plan reference — identifies the orchestration plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanReference {
    #[serde(rename = "planId")]
    pub plan_id: String,
    #[serde(rename = "planType", skip_serializing_if = "Option::is_none")]
    pub plan_type: Option<String>,
}

/// Task step — a single unit of work within a job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStep {
    #[serde(rename = "id")]
    pub id: uuid::Uuid,
    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "displayName", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// The `if` condition expression. Runner evaluates this.
    #[serde(rename = "condition", skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    /// Shell script body for `run:` steps.
    #[serde(rename = "script", skip_serializing_if = "Option::is_none")]
    pub script: Option<String>,
    /// Action reference for `uses:` steps.
    #[serde(rename = "reference", skip_serializing_if = "Option::is_none")]
    pub reference: Option<TaskReference>,
    /// Step inputs (the `with:` block).
    #[serde(rename = "inputs", default)]
    pub inputs: BTreeMap<String, String>,
    /// Step environment variables.
    #[serde(rename = "env", default)]
    pub env: BTreeMap<String, String>,
    /// Whether this step should continue on error.
    #[serde(rename = "continueOnError", skip_serializing_if = "Option::is_none")]
    pub continue_on_error: Option<bool>,
    /// Working directory override.
    #[serde(rename = "workingDirectory", skip_serializing_if = "Option::is_none")]
    pub working_directory: Option<String>,
    /// Timeout in minutes.
    #[serde(rename = "timeoutInMinutes", skip_serializing_if = "Option::is_none")]
    pub timeout_in_minutes: Option<u32>,
}

/// Reference to an action or task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskReference {
    #[serde(rename = "id", skip_serializing_if = "Option::is_none")]
    pub id: Option<uuid::Uuid>,
    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "version", skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub reference_type: Option<String>,
}

/// How to download an action's source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionsDownloadInfo {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub download_type: Option<String>,
    #[serde(rename = "url", skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(rename = "auth", skip_serializing_if = "Option::is_none")]
    pub auth: Option<String>,
}

// ─── Variable and masking DTOs ────────────────────────────────────────────

/// A variable value with optional secret flag.
///
/// Variables are sent to the runner as `VariableValue` objects.
/// The runner uses `isSecret` to decide whether to mask the value in logs.
///
/// Upstream source: `VariableValue.cs`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableValue {
    #[serde(rename = "value", skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(rename = "isSecret", skip_serializing_if = "Option::is_none")]
    pub is_secret: Option<bool>,
}

impl VariableValue {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: Some(value.into()),
            is_secret: None,
        }
    }

    pub fn secret(value: impl Into<String>) -> Self {
        Self {
            value: Some(value.into()),
            is_secret: Some(true),
        }
    }
}

/// A masking hint — tells the runner to redact a value in log output.
///
/// The runner applies these hints when writing to the log feed.
///
/// Upstream source: `MaskHint.cs`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaskHint {
    #[serde(rename = "type")]
    pub hint_type: MaskType,
    #[serde(rename = "value")]
    pub value: String,
}

/// Type of masking hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MaskType {
    /// A literal string to redact.
    Hash,
}

// ─── Timeline and recording DTOs ──────────────────────────────────────────

/// Reference to a timeline — a collection of timeline records for a job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineReference {
    #[serde(rename = "id")]
    pub id: uuid::Uuid,
}

/// A single timeline record — represents the status of a job or step.
///
/// The runner PATCHes these as steps execute. Each record tracks
/// state transitions, timing, result, and any issues (annotations).
///
/// Upstream source: `TimelineRecord.cs`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineRecord {
    #[serde(rename = "id")]
    pub id: uuid::Uuid,
    /// Parent record ID (job → step relationship).
    #[serde(rename = "parentId", skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<uuid::Uuid>,
    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "displayName", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub record_type: Option<TimelineRecordType>,
    #[serde(rename = "state", skip_serializing_if = "Option::is_none")]
    pub state: Option<TimelineRecordState>,
    #[serde(rename = "result", skip_serializing_if = "Option::is_none")]
    pub result: Option<TaskResult>,
    #[serde(rename = "startTime", skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(rename = "finishTime", skip_serializing_if = "Option::is_none")]
    pub finish_time: Option<String>,
    #[serde(rename = "issues", default)]
    pub issues: Vec<Issue>,
    #[serde(rename = "variables", default)]
    pub variables: BTreeMap<String, VariableValue>,
    #[serde(rename = "currentOperation", skip_serializing_if = "Option::is_none")]
    pub current_operation: Option<String>,
    #[serde(rename = "percentComplete", skip_serializing_if = "Option::is_none")]
    pub percent_complete: Option<i32>,
    #[serde(rename = "workerName", skip_serializing_if = "Option::is_none")]
    pub worker_name: Option<String>,
    #[serde(rename = "errorCount", skip_serializing_if = "Option::is_none")]
    pub error_count: Option<i32>,
    #[serde(rename = "warningCount", skip_serializing_if = "Option::is_none")]
    pub warning_count: Option<i32>,
    #[serde(rename = "steps", default)]
    pub steps: Vec<TimelineRecord>,
}

/// Type of timeline record (job vs step).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TimelineRecordType {
    Job,
    Step,
    Phase,
    Stage,
}

/// Current state of a timeline record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TimelineRecordState {
    Pending,
    InProgress,
    Completed,
}

/// Task result — the final outcome of a job or step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TaskResult {
    Succeeded,
    SucceededWithIssues,
    Failed,
    Cancelled,
    Skipped,
}

/// An issue (annotation) attached to a timeline record.
///
/// The runner emits these for `::error::` and `::warning::` annotations,
/// plus any step/job errors.
///
/// Upstream source: `Issue.cs`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Issue {
    #[serde(rename = "type")]
    pub issue_type: IssueType,
    #[serde(rename = "category", skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(rename = "message", skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(rename = "data", default)]
    pub data: BTreeMap<String, String>,
    #[serde(rename = "isInfrastructureIssue", skip_serializing_if = "Option::is_none")]
    pub is_infrastructure_issue: Option<bool>,
}

/// Issue severity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum IssueType {
    Error,
    Warning,
    Info,
}

// ─── Resources and endpoints ──────────────────────────────────────────────

/// Resources block in a job message — contains service endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResources {
    #[serde(rename = "endpoints", default)]
    pub endpoints: Vec<ServiceEndpoint>,
    #[serde(rename = "repositories", default)]
    pub repositories: Vec<RepositoryReference>,
}

/// A service endpoint — connection to an external service.
///
/// The most important one is `SystemVssConnection` which carries the
/// OAuth token the runner uses for all subsequent API calls.
///
/// Upstream source: `ServiceEndpoint.cs`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceEndpoint {
    #[serde(rename = "data", default)]
    pub data: BTreeMap<String, String>,
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub endpoint_type: Option<String>,
    #[serde(rename = "url", skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(rename = "authorization")]
    pub authorization: EndpointAuthorization,
    #[serde(rename = "isShared", skip_serializing_if = "Option::is_none")]
    pub is_shared: Option<bool>,
    #[serde(rename = "serviceOwner", skip_serializing_if = "Option::is_none")]
    pub service_owner: Option<String>,
}

/// Authorization data for a service endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointAuthorization {
    #[serde(rename = "parameters", default)]
    pub parameters: BTreeMap<String, String>,
    #[serde(rename = "scheme", skip_serializing_if = "Option::is_none")]
    pub scheme: Option<String>,
}

/// Repository reference in job resources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepositoryReference {
    #[serde(rename = "repository", skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,
    #[serde(rename = "ref", skip_serializing_if = "Option::is_none")]
    pub git_ref: Option<String>,
    #[serde(rename = "connector", skip_serializing_if = "Option::is_none")]
    pub connector: Option<RepositoryConnector>,
}

/// Connector for a repository reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepositoryConnector {
    #[serde(rename = "id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

// ─── Context data DTOs ────────────────────────────────────────────────────

/// Pipeline context data — the union type for all context values.
///
/// In GitHub's SDK this is `PipelineContextData`, a discriminated union
/// that can hold a string, number, boolean, array, dictionary, or
/// `ContextDictionary`. We model it as a tagged enum.
///
/// Upstream source: `Pipelines.ContextData.PipelineContextData.cs`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PipelineContextData {
    String(String),
    Bool(bool),
    Number(f64),
    Array(Vec<PipelineContextData>),
    Dict(BTreeMap<String, PipelineContextData>),
}

// ─── Job completion DTOs ──────────────────────────────────────────────────

/// Job completed event — sent by the runner when a job finishes.
///
/// The runner PATCHes this to the server to report the final result.
/// The server uses this to update the run status and trigger downstream jobs.
///
/// Upstream source: `FinishJobController.cs`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCompletedEvent {
    #[serde(rename = "jobId")]
    pub job_id: uuid::Uuid,
    #[serde(rename = "result")]
    pub result: TaskResult,
    #[serde(rename = "timelineId")]
    pub timeline_id: uuid::Uuid,
    #[serde(rename = "outputs", default)]
    pub outputs: BTreeMap<String, String>,
}

// ─── Log upload DTOs ──────────────────────────────────────────────────────

/// Log file reference — returned when creating a log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogReference {
    #[serde(rename = "id")]
    pub id: i64,
    #[serde(rename = "path", skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

// ─── Request/response helpers ─────────────────────────────────────────────

/// Generic Azure DevOps error response envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VssError {
    #[serde(rename = "errorCode", skip_serializing_if = "Option::is_none")]
    pub error_code: Option<i64>,
    #[serde(rename = "message", skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_agent_message_roundtrip() {
        let msg = TaskAgentMessage {
            message_id: 1,
            message_type: "PipelineAgentJobRequest".to_owned(),
            body: "aGVsbG8=".to_owned(),
            iv: Some(vec![1, 2, 3]),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"messageId\":1"));
        assert!(json.contains("\"messageType\":\"PipelineAgentJobRequest\""));
        let back: TaskAgentMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(back.message_id, 1);
        assert_eq!(back.body, "aGVsbG8=");
    }

    #[test]
    fn task_agent_message_no_iv() {
        let json = r#"{"messageId":42,"messageType":"Test","body":"dGVzdA=="}"#;
        let msg: TaskAgentMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.message_id, 42);
        assert!(msg.iv.is_none());
    }

    #[test]
    fn variable_value_secret_roundtrip() {
        let v = VariableValue::secret("my-secret-val");
        let json = serde_json::to_string(&v).unwrap();
        assert!(json.contains("\"isSecret\":true"));
        let back: VariableValue = serde_json::from_str(&json).unwrap();
        assert_eq!(back.value.unwrap(), "my-secret-val");
        assert_eq!(back.is_secret, Some(true));
    }

    #[test]
    fn timeline_record_state_serialization() {
        let record = TimelineRecord {
            id: uuid::Uuid::nil(),
            parent_id: None,
            name: None,
            display_name: None,
            record_type: Some(TimelineRecordType::Job),
            state: Some(TimelineRecordState::InProgress),
            result: None,
            start_time: None,
            finish_time: None,
            issues: vec![],
            variables: BTreeMap::new(),
            current_operation: None,
            percent_complete: Some(50),
            worker_name: None,
            error_count: None,
            warning_count: None,
            steps: vec![],
        };
        let json = serde_json::to_string(&record).unwrap();
        assert!(json.contains("\"state\":\"inProgress\""));
        assert!(json.contains("\"type\":\"job\""));
    }

    #[test]
    fn task_result_serialization() {
        assert_eq!(
            serde_json::to_string(&TaskResult::Succeeded).unwrap(),
            "\"succeeded\""
        );
        assert_eq!(
            serde_json::to_string(&TaskResult::Failed).unwrap(),
            "\"failed\""
        );
        assert_eq!(
            serde_json::to_string(&TaskResult::Cancelled).unwrap(),
            "\"cancelled\""
        );
    }

    #[test]
    fn pipeline_context_data_variants() {
        let json = r#""hello""#;
        let data: PipelineContextData = serde_json::from_str(json).unwrap();
        assert!(matches!(data, PipelineContextData::String(_)));

        let json = "42";
        let data: PipelineContextData = serde_json::from_str(json).unwrap();
        assert!(matches!(data, PipelineContextData::Number(_)));

        let json = "true";
        let data: PipelineContextData = serde_json::from_str(json).unwrap();
        assert!(matches!(data, PipelineContextData::Bool(_)));

        let json = r#"["a","b"]"#;
        let data: PipelineContextData = serde_json::from_str(json).unwrap();
        assert!(matches!(data, PipelineContextData::Array(_)));
    }

    #[test]
    fn issue_roundtrip() {
        let issue = Issue {
            issue_type: IssueType::Error,
            category: Some("LoggingCommand".to_owned()),
            message: Some("::error::something broke".to_owned()),
            data: BTreeMap::new(),
            is_infrastructure_issue: None,
        };
        let json = serde_json::to_string(&issue).unwrap();
        assert!(json.contains("\"type\":\"error\""));
        let back: Issue = serde_json::from_str(&json).unwrap();
        assert_eq!(back.issue_type, IssueType::Error);
    }
}
