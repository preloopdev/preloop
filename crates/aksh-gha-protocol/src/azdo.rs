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
#![allow(missing_docs)]

use serde::{Deserialize, Deserializer, Serialize, Serializer};
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
    #[serde(
        rename = "locationServiceData",
        skip_serializing_if = "Option::is_none"
    )]
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
    #[serde(
        rename = "serviceDefinitions",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub service_definitions: Vec<ServiceDefinition>,
    #[serde(
        rename = "accessMappings",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub access_mappings: Vec<AccessMapping>,
    #[serde(
        rename = "defaultAccessMappingMoniker",
        skip_serializing_if = "Option::is_none"
    )]
    pub default_access_mapping_moniker: Option<String>,
}

/// A location mapping for a service definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationMapping {
    #[serde(
        rename = "accessMappingMoniker",
        skip_serializing_if = "Option::is_none"
    )]
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
    #[serde(rename = "osDescription", skip_serializing_if = "Option::is_none")]
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
    /// Serialized as a base64 string (matching the official runner wire format).
    /// Present only when the message body is encrypted.
    #[serde(rename = "iv", skip_serializing_if = "Option::is_none")]
    pub iv: Option<String>,
}

/// Known message types the runner handles.
pub mod message_type {
    /// A job request — body contains an encrypted `AgentJobRequestMessage`.
    pub const PIPELINE_AGENT_JOB_REQUEST: &str = "PipelineAgentJobRequest";
    /// A run-service job request returned from the broker `acquirejob` API.
    pub const RUNNER_JOB_REQUEST: &str = "RunnerJobRequest";
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
    /// The concrete job transport. Broker `acquirejob` responses must use
    /// `RunnerJobRequest` so the official runner renews the job through the
    /// run-service broker instead of the legacy AgentRequest API.
    #[serde(rename = "messageType", skip_serializing_if = "Option::is_none")]
    pub message_type: Option<String>,

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

    /// Job container spec (`container:`) — TemplateToken-compatible JSON.
    #[serde(
        rename = "jobContainer",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub job_container: Option<serde_json::Value>,

    /// Service container specs (`services:`) — alias → spec mapping.
    #[serde(
        rename = "jobServiceContainers",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub job_service_containers: Option<serde_json::Value>,

    /// Job-level output declarations as a TemplateToken map.
    /// GitHub sends `{type:2,map:[{Key:{...},Value:{...}}]}` and the runner
    /// evaluates the expression tokens after step execution.
    #[serde(
        rename = "jobOutputs",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub job_outputs: Option<serde_json::Value>,

    /// Whether the debugger is enabled for this job.
    /// Mirrors `AgentJobRequestMessage.EnableDebugger` in `actions/runner` v2.335.0+.
    #[serde(rename = "enableDebugger", default, skip_serializing_if = "is_false")]
    pub enable_debugger: bool,

    /// Dev Tunnel info for remote debugging.
    /// Mirrors `AgentJobRequestMessage.DebuggerTunnel`.
    #[serde(
        rename = "debuggerTunnel",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub debugger_tunnel: Option<DebuggerTunnelInfo>,

    /// Optional welcome message for the debugger console.
    /// Mirrors `AgentJobRequestMessage.DebuggerWelcomeMessage`.
    #[serde(
        rename = "debuggerWelcomeMessage",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub debugger_welcome_message: Option<String>,

    /// aksh extension: workflow run id for local DAP proxy port registration.
    #[serde(
        rename = "akshDebugRunId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub aksh_debug_run_id: Option<String>,

    /// aksh extension: transport mode for DAP traffic.
    #[serde(
        rename = "akshDebugTransport",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub aksh_debug_transport: Option<String>,
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// Plan reference — identifies the orchestration plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanReference {
    #[serde(rename = "planId")]
    pub plan_id: String,
    #[serde(rename = "planType", skip_serializing_if = "Option::is_none")]
    pub plan_type: Option<String>,
}

/// Dev Tunnel info for remote debugging.
///
/// 1:1 port of `src/Sdk/DTPipelines/Pipelines/DebuggerTunnelInfo.cs`
/// in `actions/runner` v2.335.1.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct DebuggerTunnelInfo {
    #[serde(rename = "tunnelId", default)]
    pub tunnel_id: String,
    #[serde(rename = "clusterId", default)]
    pub cluster_id: String,
    #[serde(rename = "hostToken", default)]
    pub host_token: String,
    #[serde(rename = "port", default)]
    pub port: u16,
}

/// Task step — a single unit of work within a job.
#[derive(Debug, Clone)]
pub struct TaskStep {
    pub id: uuid::Uuid,
    /// User-facing step name (from workflow `name:` field).
    pub name: Option<String>,
    /// Expression context key — user `id:` or auto-generated `__run`/`__run_N`.
    /// Serialized as `contextName` to match GitHub's wire format.
    pub context_name: Option<String>,
    pub display_name: Option<String>,
    /// TemplateToken for the display name — `{type:1, lit:"...", col, file, line}`.
    /// Serialized as `displayNameToken` to match GitHub's wire format.
    pub display_name_token: Option<serde_json::Value>,
    pub condition: Option<String>,
    pub script: Option<String>,
    pub reference: Option<TaskReference>,
    pub inputs: BTreeMap<String, String>,
    pub env: BTreeMap<String, String>,
    pub continue_on_error: Option<bool>,
    pub working_directory: Option<String>,
    pub timeout_in_minutes: Option<u32>,
}

impl Serialize for TaskStep {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;

        let mut inputs = self.inputs.clone();
        if let Some(script) = &self.script {
            inputs.insert("script".to_owned(), script.clone());
        }

        let field_count = 5
            + usize::from(self.name.is_some())
            + usize::from(self.context_name.is_some())
            + usize::from(self.display_name.is_some())
            + usize::from(self.display_name_token.is_some())
            + usize::from(self.condition.is_some())
            + usize::from(self.continue_on_error.is_some())
            + usize::from(self.working_directory.is_some())
            + usize::from(self.timeout_in_minutes.is_some());
        let mut map = serializer.serialize_map(Some(field_count))?;
        map.serialize_entry("type", "action")?;
        map.serialize_entry("reference", &SerializedActionReference { step: self })?;
        map.serialize_entry("environment", &TemplateStringMap(&self.env))?;
        map.serialize_entry("inputs", &TemplateStringMap(&inputs))?;
        map.serialize_entry("id", &self.id)?;
        if let Some(name) = &self.name {
            map.serialize_entry("name", name)?;
        }
        if let Some(context_name) = &self.context_name {
            map.serialize_entry("contextName", context_name)?;
        }
        if let Some(display_name) = &self.display_name {
            map.serialize_entry("displayName", display_name)?;
        }
        if let Some(token) = &self.display_name_token {
            map.serialize_entry("displayNameToken", token)?;
        }
        if let Some(condition) = &self.condition {
            map.serialize_entry("condition", condition)?;
        }
        if let Some(continue_on_error) = self.continue_on_error {
            map.serialize_entry("continueOnError", &continue_on_error)?;
        }
        if let Some(working_directory) = &self.working_directory {
            map.serialize_entry("workingDirectory", working_directory)?;
        }
        if let Some(timeout_in_minutes) = self.timeout_in_minutes {
            map.serialize_entry("timeoutInMinutes", &timeout_in_minutes)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for TaskStep {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let obj = value
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("expected object"))?;

        let env = extract_template_map(obj.get("env")).unwrap_or_default();
        let inputs = extract_template_map(obj.get("inputs")).unwrap_or_default();

        // In the new serialization format, `script` lives inside the `inputs`
        // TemplateToken map instead of as a top-level field.
        let script = obj
            .get("script")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .or_else(|| inputs.get("script").cloned());

        Ok(TaskStep {
            id: obj
                .get("id")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_else(uuid::Uuid::nil),
            name: obj.get("name").and_then(|v| v.as_str()).map(str::to_owned),
            context_name: obj
                .get("contextName")
                .and_then(|v| v.as_str())
                .map(str::to_owned),
            display_name: obj
                .get("displayName")
                .and_then(|v| v.as_str())
                .map(str::to_owned),
            display_name_token: obj.get("displayNameToken").cloned(),
            condition: obj
                .get("condition")
                .and_then(|v| v.as_str())
                .map(str::to_owned),
            script,
            reference: obj
                .get("reference")
                .and_then(|v| serde_json::from_value(v.clone()).ok()),
            env,
            inputs,
            continue_on_error: obj.get("continueOnError").and_then(|v| v.as_bool()),
            working_directory: obj
                .get("workingDirectory")
                .and_then(|v| v.as_str())
                .map(str::to_owned),
            timeout_in_minutes: obj
                .get("timeoutInMinutes")
                .and_then(|v| v.as_u64())
                .map(|n| n as u32),
        })
    }
}

/// Extract a BTreeMap<String, String> from either a plain JSON object or a
/// TemplateToken map of shape `{"type": 2, "map": [{"key": "k", "value": "v"}]}`.
fn extract_template_map(value: Option<&serde_json::Value>) -> Option<BTreeMap<String, String>> {
    let value = value?;
    if value.as_object()?.is_empty() {
        return Some(BTreeMap::new());
    }
    // TemplateToken map: {"type": 2, "map": [{"key": ..., "value": ...}]}
    if let Some(pairs) = value.get("map").and_then(|v| v.as_array()) {
        let mut map = BTreeMap::new();
        for pair in pairs {
            let key_val = pair.get("Key").or_else(|| pair.get("key"));
            let val_val = pair.get("Value").or_else(|| pair.get("value"));
            let key = key_val
                .and_then(|v| v.as_str().map(str::to_owned).or_else(|| v.get("lit").and_then(|l| l.as_str()).map(str::to_owned)))?;
            let val = val_val
                .and_then(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .or_else(|| v.get("lit").and_then(|l| l.as_str()).map(str::to_owned))
                })
                .unwrap_or_default();
            map.insert(key, val);
        }
        return Some(map);
    }
    // Plain map: {"KEY": "VALUE", ...}
    if let Some(obj) = value.as_object() {
        let mut map = BTreeMap::new();
        for (k, v) in obj {
            let val = v
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| v.to_string());
            map.insert(k.clone(), val);
        }
        return Some(map);
    }
    None
}

/// Serializes environment/inputs as TemplateToken maps.
struct SerializedActionReference<'a> {
    step: &'a TaskStep,
}

impl Serialize for SerializedActionReference<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;

        let Some(reference) = &self.step.reference else {
            let mut map = serializer.serialize_map(Some(1))?;
            map.serialize_entry("type", "script")?;
            return map.end();
        };

        let field_count = 1
            + usize::from(reference.name.is_some())
            + usize::from(reference.version.is_some())
            + usize::from(reference.reference_type.is_some());
        let mut map = serializer.serialize_map(Some(field_count))?;
        map.serialize_entry(
            "type",
            reference.reference_type.as_deref().unwrap_or("repository"),
        )?;
        if let Some(name) = &reference.name {
            map.serialize_entry("name", name)?;
        }
        if let Some(version) = &reference.version {
            map.serialize_entry("ref", version)?;
        }
        if reference.reference_type.is_none() {
            map.serialize_entry("repositoryType", "GitHub")?;
        }
        map.end()
    }
}

struct TemplateStringMap<'a>(&'a BTreeMap<String, String>);

impl Serialize for TemplateStringMap<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(if self.0.is_empty() { 1 } else { 2 }))?;
        map.serialize_entry("type", &2)?;
        if !self.0.is_empty() {
            let pairs: Vec<TemplateStringMapPair<'_>> = self
                .0
                .iter()
                .map(|(key, value)| TemplateStringMapPair { key, value })
                .collect();
            map.serialize_entry("map", &pairs)?;
        }
        map.end()
    }
}

struct TemplateStringMapPair<'a> {
    key: &'a str,
    value: &'a str,
}

impl Serialize for TemplateStringMapPair<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("Key", &serde_json::json!({"type": 0, "lit": self.key}))?;
        map.serialize_entry("Value", &template_string_token(self.value))?;
        map.end()
    }
}

fn template_string_token(value: &str) -> serde_json::Value {
    let Some(first) = value.find("${{") else {
        return serde_json::json!({"type": 0, "lit": value});
    };
    let mut literal = String::new();
    let mut expressions = Vec::new();
    let mut rest = value;
    loop {
        let Some(start) = rest.find("${{") else {
            literal.push_str(rest);
            break;
        };
        literal.push_str(&rest[..start]);
        let after = &rest[start + 3..];
        // Find the closing }} that isn't inside a string literal.
        let Some(end) = find_expression_end(after) else {
            return serde_json::json!({"type": 0, "lit": value});
        };
        expressions.push(after[..end].trim().to_owned());
        literal.push_str(&format!("{{{}}}", expressions.len() - 1));
        rest = &after[end + 2..];
    }
    if first == 0 && literal == "{0}" && expressions.len() == 1 {
        return serde_json::json!({"type": 3, "expr": expressions[0]});
    }
    let escaped = literal.replace('\'', "''");
    let expr = format!("format('{}', {})", escaped, expressions.join(", "));
    serde_json::json!({"type": 3, "expr": expr})
}

/// Find the position of `}}` that closes a `${{ ... }}` expression,
/// skipping over `}}` that appears inside string literals (single-quoted).
fn find_expression_end(s: &str) -> Option<usize> {
    let mut in_string = false;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if in_string {
            if bytes[i] == b'\'' {
                // Check for escaped quote ''
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                } else {
                    in_string = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
        } else if bytes[i] == b'\'' {
            in_string = true;
            i += 1;
        } else if i + 1 < bytes.len() && bytes[i] == b'}' && bytes[i + 1] == b'}' {
            return Some(i);
        } else {
            i += 1;
        }
    }
    None
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
    #[serde(rename = "isBackground", skip_serializing_if = "Option::is_none")]
    pub is_background: Option<bool>,
    #[serde(
        rename = "backgroundControlType",
        skip_serializing_if = "Option::is_none"
    )]
    pub background_control_type: Option<String>,
    #[serde(rename = "backgroundControlStepIds", default)]
    pub background_control_step_ids: Vec<uuid::Uuid>,
    #[serde(rename = "parallelGroupId", skip_serializing_if = "Option::is_none")]
    pub parallel_group_id: Option<String>,
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
    #[serde(rename = "canceled", alias = "cancelled")]
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
    #[serde(
        rename = "isInfrastructureIssue",
        skip_serializing_if = "Option::is_none"
    )]
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
#[derive(Debug, Clone)]
pub enum PipelineContextData {
    String(String),
    Bool(bool),
    Number(f64),
    Array(Vec<PipelineContextData>),
    Dict(BTreeMap<String, PipelineContextData>),
}

impl Serialize for PipelineContextData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::{SerializeMap, SerializeSeq};

        match self {
            PipelineContextData::String(value) => serializer.serialize_str(value),
            PipelineContextData::Bool(value) => serializer.serialize_bool(*value),
            PipelineContextData::Number(value) => serializer.serialize_f64(*value),
            PipelineContextData::Array(values) => {
                let mut map =
                    serializer.serialize_map(Some(if values.is_empty() { 1 } else { 2 }))?;
                map.serialize_entry("t", &1)?;
                if !values.is_empty() {
                    struct ArrayValues<'a>(&'a [PipelineContextData]);

                    impl Serialize for ArrayValues<'_> {
                        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
                        where
                            S: Serializer,
                        {
                            let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
                            for value in self.0 {
                                seq.serialize_element(value)?;
                            }
                            seq.end()
                        }
                    }

                    map.serialize_entry("a", &ArrayValues(values))?;
                }
                map.end()
            }
            PipelineContextData::Dict(values) => {
                let mut map =
                    serializer.serialize_map(Some(if values.is_empty() { 1 } else { 2 }))?;
                map.serialize_entry("t", &2)?;
                if !values.is_empty() {
                    let pairs: Vec<PipelineContextDataPair<'_>> = values
                        .iter()
                        .map(|(key, value)| PipelineContextDataPair { key, value })
                        .collect();
                    map.serialize_entry("d", &pairs)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for PipelineContextData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        pipeline_context_from_json(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Serialize)]
struct PipelineContextDataPair<'a> {
    #[serde(rename = "k")]
    key: &'a str,
    #[serde(rename = "v")]
    value: &'a PipelineContextData,
}

fn pipeline_context_from_json(value: serde_json::Value) -> Result<PipelineContextData, String> {
    match value {
        serde_json::Value::String(value) => Ok(PipelineContextData::String(value)),
        serde_json::Value::Bool(value) => Ok(PipelineContextData::Bool(value)),
        serde_json::Value::Number(value) => Ok(PipelineContextData::Number(
            value.as_f64().unwrap_or_default(),
        )),
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(pipeline_context_from_json)
            .collect::<Result<Vec<_>, _>>()
            .map(PipelineContextData::Array),
        serde_json::Value::Object(mut object) => {
            match object.remove("t").and_then(|value| value.as_i64()) {
                None | Some(0) => Ok(PipelineContextData::String(
                    object
                        .remove("s")
                        .and_then(|value| value.as_str().map(str::to_owned))
                        .unwrap_or_default(),
                )),
                Some(1) => object
                    .remove("a")
                    .and_then(|value| value.as_array().cloned())
                    .unwrap_or_default()
                    .into_iter()
                    .map(pipeline_context_from_json)
                    .collect::<Result<Vec<_>, _>>()
                    .map(PipelineContextData::Array),
                Some(2) | Some(5) => {
                    let mut values = BTreeMap::new();
                    let pairs = object
                        .remove("d")
                        .and_then(|value| value.as_array().cloned())
                        .unwrap_or_default();
                    for pair in pairs {
                        let Some(pair) = pair.as_object() else {
                            continue;
                        };
                        let Some(key) = pair.get("k").and_then(|value| value.as_str()) else {
                            continue;
                        };
                        let value = pair.get("v").cloned().unwrap_or(serde_json::Value::Null);
                        values.insert(key.to_owned(), pipeline_context_from_json(value)?);
                    }
                    Ok(PipelineContextData::Dict(values))
                }
                Some(3) => Ok(PipelineContextData::Bool(
                    object
                        .remove("b")
                        .and_then(|value| value.as_bool())
                        .unwrap_or_default(),
                )),
                Some(4) => Ok(PipelineContextData::Number(
                    object
                        .remove("n")
                        .and_then(|value| value.as_f64())
                        .unwrap_or_default(),
                )),
                Some(other) => Err(format!("unsupported PipelineContextData type {other}")),
            }
        }
        serde_json::Value::Null => Ok(PipelineContextData::String(String::new())),
    }
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

/// VSS JSON collection wrapper — the standard AzDO/REST envelope for arrays.
///
/// The official runner sends and expects timeline records, job events, and
/// other collections wrapped as `{"count": N, "value": [...]}`.
///
/// This matches the C# `VssJsonCollectionWrapper<T>` from
/// `Microsoft.VisualStudio.Services.WebApi`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VssJsonCollectionWrapper<T> {
    #[serde(default)]
    pub count: usize,
    pub value: Vec<T>,
}

/// Task log — sent by the runner when creating a log container.
///
/// The runner POSTs this to `/_apis/v1/Logfiles/{scope}/{hub}/{planId}`.
/// The server assigns an `id` and returns the object.
///
/// Upstream source: `TaskLog.cs` in `GitHub.DistributedTask.WebApi`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskLog {
    #[serde(rename = "id")]
    #[serde(default)]
    pub id: i64,
    #[serde(rename = "path")]
    pub path: String,
    #[serde(rename = "createdOn", skip_serializing_if = "Option::is_none")]
    pub created_on: Option<String>,
    #[serde(rename = "lastChangedOn", skip_serializing_if = "Option::is_none")]
    pub last_changed_on: Option<String>,
    #[serde(rename = "lineCount")]
    #[serde(default)]
    pub line_count: i64,
    #[serde(rename = "timelineId", skip_serializing_if = "Option::is_none")]
    pub timeline_id: Option<uuid::Uuid>,
    #[serde(rename = "location", skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
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
            iv: Some("AQID".to_owned()),
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
            is_background: None,
            background_control_type: None,
            background_control_step_ids: vec![],
            parallel_group_id: None,
            steps: vec![],
        };
        let json = serde_json::to_string(&record).unwrap();
        assert!(json.contains("\"state\":\"inProgress\""));
        assert!(json.contains("\"type\":\"job\""));
    }

    #[test]
    fn timeline_record_background_fields_roundtrip() {
        let step_id = uuid::Uuid::new_v4();
        let record: TimelineRecord = serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::nil(),
            "isBackground": true,
            "backgroundControlType": "wait",
            "backgroundControlStepIds": [step_id],
            "parallelGroupId": "group-1"
        }))
        .unwrap();

        assert_eq!(record.is_background, Some(true));
        assert_eq!(record.background_control_type.as_deref(), Some("wait"));
        assert_eq!(record.background_control_step_ids, vec![step_id]);
        assert_eq!(record.parallel_group_id.as_deref(), Some("group-1"));

        let json = serde_json::to_string(&record).unwrap();
        assert!(json.contains("\"isBackground\":true"));
        assert!(json.contains("\"backgroundControlType\":\"wait\""));
        assert!(json.contains("\"backgroundControlStepIds\""));
        assert!(json.contains("\"parallelGroupId\":\"group-1\""));
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
            "\"canceled\""
        );
        assert_eq!(
            serde_json::from_str::<TaskResult>("\"cancelled\"").unwrap(),
            TaskResult::Cancelled
        );
    }

    #[test]
    fn task_step_serializes_as_runner_action_step() {
        let step = TaskStep {
            id: uuid::Uuid::nil(),
            name: None,
            context_name: None,
            display_name: None,
            display_name_token: None,
            condition: None,
            script: Some("echo hi".to_owned()),
            reference: None,
            inputs: BTreeMap::new(),
            env: BTreeMap::new(),
            continue_on_error: None,
            working_directory: None,
            timeout_in_minutes: None,
        };

        let json = serde_json::to_value(&step).unwrap();

        assert_eq!(json["type"], "action");
        assert_eq!(json["reference"]["type"], "script");
        assert_eq!(json["environment"]["type"], 2);
        assert_eq!(json["inputs"]["type"], 2);
        assert_eq!(json["inputs"]["map"][0]["Key"]["type"], 0);
        assert_eq!(json["inputs"]["map"][0]["Key"]["lit"], "script");
        assert_eq!(json["inputs"]["map"][0]["Value"]["type"], 0);
        assert_eq!(json["inputs"]["map"][0]["Value"]["lit"], "echo hi");
    }

    #[test]
    fn task_step_serializes_expression_as_format_token() {
        let mut inputs = BTreeMap::new();
        inputs.insert(
            "script".to_owned(),
            "OUTPUT='${{ steps.make.outputs.value }}'".to_owned(),
        );
        let step = TaskStep {
            id: uuid::Uuid::nil(),
            name: None,
            context_name: None,
            display_name: None,
            display_name_token: None,
            condition: None,
            script: None,
            reference: None,
            inputs,
            env: BTreeMap::new(),
            continue_on_error: None,
            working_directory: None,
            timeout_in_minutes: None,
        };
        let value = serde_json::to_value(step).unwrap();
        let token = &value["inputs"]["map"][0]["Value"];
        assert_eq!(token["type"], 3);
        assert_eq!(
            token["expr"],
            "format('OUTPUT=''{0}''', steps.make.outputs.value)"
        );
    }

    #[test]
    fn template_string_token_handles_braces_inside_string_literals() {
        // Expression containing }} inside a single-quoted JSON string
        let token = template_string_token(
            r#"${{ fromJSON('{"a":{"b":{"c":"deep"}}}')['a']['b']['c'] }}"#,
        );
        assert_eq!(token["type"], 3);
        let expr = token["expr"].as_str().unwrap();
        // Should preserve the full expression, not truncate at the first }}
        assert!(
            expr.contains("fromJSON") && expr.contains("deep"),
            "expression was truncated: {expr}"
        );
    }

    #[test]
    fn find_expression_end_skips_braces_in_strings() {
        // }} inside a string should be skipped
        assert_eq!(
            find_expression_end(" fromJSON('{}}')'a' }}"),
            Some(20)
        );
        // Plain expression
        assert_eq!(find_expression_end(" x }}"), Some(3));
        // No closing
        assert_eq!(find_expression_end(" x "), None);
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
    fn pipeline_context_data_uses_runner_wire_shape_for_collections() {
        let mut github = BTreeMap::new();
        github.insert(
            "event_name".to_owned(),
            PipelineContextData::String("push".to_owned()),
        );
        github.insert("run_id".to_owned(), PipelineContextData::Number(42.0));

        let json = serde_json::to_value(PipelineContextData::Dict(github)).unwrap();

        assert_eq!(json["t"], 2);
        assert_eq!(json["d"][0]["k"], "event_name");
        assert_eq!(json["d"][0]["v"], "push");
        assert_eq!(json["d"][1]["k"], "run_id");
        assert_eq!(json["d"][1]["v"], 42.0);

        let roundtrip: PipelineContextData = serde_json::from_value(json).unwrap();
        let PipelineContextData::Dict(roundtrip) = roundtrip else {
            panic!("expected dictionary context");
        };
        assert!(matches!(
            roundtrip.get("event_name"),
            Some(PipelineContextData::String(value)) if value == "push"
        ));
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
