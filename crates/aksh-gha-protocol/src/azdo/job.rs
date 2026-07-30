use super::{MaskHint, PipelineContextData, TaskResources, TimelineReference, VariableValue};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

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

    /// Human-readable job display name.
    #[serde(rename = "jobDisplayName", skip_serializing_if = "Option::is_none")]
    pub job_display_name: Option<String>,

    /// Stable workflow job key used by the runner context.
    #[serde(rename = "jobName")]
    pub job_name: String,

    /// Broker lease expiry. GitHub uses the minimum DateTime for newly acquired jobs.
    #[serde(rename = "lockedUntil")]
    pub locked_until: String,

    /// Billing owner echoed from the acquire request.
    #[serde(rename = "billingOwnerId", skip_serializing_if = "Option::is_none")]
    pub billing_owner_id: Option<String>,

    /// Source workflow files referenced by template-token coordinates.
    #[serde(rename = "fileTable", default)]
    pub file_table: Vec<String>,

    /// Workflow/job defaults and environment overlays.
    #[serde(rename = "defaults", default)]
    pub defaults: Vec<serde_json::Value>,
    #[serde(rename = "environmentVariables", default)]
    pub environment_variables: Vec<serde_json::Value>,

    /// Snapshot token; emitted as null when snapshots are not used.
    #[serde(rename = "snapshot", default)]
    pub snapshot: Option<serde_json::Value>,

    /// The job's `if` condition expression string.
    #[serde(rename = "condition", skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,

    /// Variables available to the job (env + system vars + secrets).
    #[serde(rename = "variables", default)]
    pub variables: BTreeMap<String, VariableValue>,

    /// Mask hints for secret values — tells the runner what to redact in logs.
    #[serde(rename = "mask", default)]
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

    /// Whether this is a retry attempt.
    #[serde(rename = "retryCount", skip_serializing_if = "Option::is_none")]
    pub retry_count: Option<i32>,

    /// Pre-job timeout (seconds).
    #[serde(rename = "preJobTimeout", skip_serializing_if = "Option::is_none")]
    pub pre_job_timeout: Option<i64>,

    /// Job timeout (seconds).
    #[serde(rename = "jobTimeout", skip_serializing_if = "Option::is_none")]
    pub job_timeout: Option<i64>,

    /// Job container spec (`container:`) — explicit null when absent.
    #[serde(rename = "jobContainer", default)]
    pub job_container: Option<serde_json::Value>,

    /// Service container specs (`services:`) — explicit null when absent.
    #[serde(rename = "jobServiceContainers", default)]
    pub job_service_containers: Option<serde_json::Value>,

    /// Job-level output declarations — explicit null when absent.
    #[serde(rename = "jobOutputs", default)]
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

    /// Preloop extension: keep this job's runner VM alive if the job fails, so
    /// the user can attach with `preloop shell`.
    ///
    /// Absent for every run that did not request it, so the default wire shape
    /// is unchanged and an official runner is unaffected.
    #[serde(
        rename = "preloopPreserveOnFailure",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub preloop_preserve_on_failure: Option<bool>,

    /// aksh extension: commit of the immutable workspace snapshot this job
    /// checked out.
    ///
    /// The pristine ref a debug session diffs the live workspace against. It
    /// is what makes change detection free — tracked files are restorable from
    /// this commit, so no pre-image ever has to be stored.
    #[serde(
        rename = "akshSnapshotCommit",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub aksh_snapshot_commit: Option<String>,
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// Plan reference — identifies the orchestration plan.
///
/// 1:1 port of `Sdk/DTWebApi/WebApi/TaskOrchestrationPlanReference.cs`
/// in `actions/runner` v2.335.1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanReference {
    #[serde(
        rename = "scopeIdentifier",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub scope_identifier: String,
    #[serde(rename = "planId")]
    pub plan_id: String,
    #[serde(rename = "planType")]
    pub plan_type: String,
    #[serde(rename = "version")]
    pub version: i32,
    #[serde(rename = "artifactUri")]
    pub artifact_uri: String,
    #[serde(rename = "artifactLocation")]
    pub artifact_location: String,
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

        let field_count = 7
            + usize::from(!inputs.is_empty())
            + usize::from(self.context_name.is_some())
            + usize::from(self.display_name_token.is_some())
            + usize::from(self.condition.is_some())
            + usize::from(!self.env.is_empty())
            + usize::from(self.working_directory.is_some());
        let mut map = serializer.serialize_map(Some(field_count))?;
        map.serialize_entry("type", "action")?;
        map.serialize_entry("reference", &SerializedActionReference { step: self })?;
        if !self.env.is_empty() {
            map.serialize_entry("environment", &TemplateStringMap(&self.env, true))?;
        }
        if !inputs.is_empty() {
            let inputs_with_loc = self
                .reference
                .as_ref()
                .is_some_and(|r| r.reference_type.as_deref() != Some("script"));
            map.serialize_entry("inputs", &TemplateStringMap(&inputs, inputs_with_loc))?;
        }
        map.serialize_entry("id", &self.id)?;
        let name = self.context_name.as_ref().or(self.name.as_ref());
        map.serialize_entry("name", &name)?;
        if let Some(context_name) = &self.context_name {
            map.serialize_entry("contextName", context_name)?;
        }
        if let Some(token) = &self.display_name_token {
            map.serialize_entry("displayNameToken", token)?;
        }
        if let Some(condition) = &self.condition {
            map.serialize_entry("condition", condition)?;
        }
        let continue_on_error = self.continue_on_error.map(|value| {
            serde_json::json!({
                "type": 5,
                "file": 1,
                "line": 0,
                "col": 0,
                "bool": value
            })
        });
        map.serialize_entry("continueOnError", &continue_on_error)?;
        if let Some(working_directory) = &self.working_directory {
            map.serialize_entry("workingDirectory", working_directory)?;
        }
        map.serialize_entry("timeoutInMinutes", &self.timeout_in_minutes)?;
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

        let env = extract_template_map(obj.get("environment").or_else(|| obj.get("env")))
            .unwrap_or_default();
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
            continue_on_error: obj.get("continueOnError").and_then(|v| {
                v.as_bool()
                    .or_else(|| v.get("bool").and_then(serde_json::Value::as_bool))
            }),
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
    // TemplateToken mapping: type 2 with no `map` member is the canonical
    // empty mapping emitted by `TemplateStringMap::serialize` below.
    if value.get("type").and_then(serde_json::Value::as_u64) == Some(2)
        && value.get("map").is_none()
    {
        return Some(BTreeMap::new());
    }
    // TemplateToken map: {"type": 2, "map": [{"key": ..., "value": ...}]}
    if let Some(pairs) = value.get("map").and_then(|v| v.as_array()) {
        let mut map = BTreeMap::new();
        for pair in pairs {
            let key_val = pair.get("Key").or_else(|| pair.get("key"));
            let val_val = pair.get("Value").or_else(|| pair.get("value"));
            let key = key_val.and_then(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .or_else(|| v.get("lit").and_then(|l| l.as_str()).map(str::to_owned))
            })?;
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

        let is_self = reference.path.is_some();
        let is_container_registry =
            reference.reference_type.as_deref() == Some("containerRegistry");
        let field_count = 1
            + usize::from(reference.name.is_some() || is_self)
            + usize::from(reference.version.is_some())
            + usize::from(reference.reference_type.is_some())
            + usize::from(is_self);
        let mut map = serializer.serialize_map(Some(field_count))?;
        map.serialize_entry(
            "type",
            reference.reference_type.as_deref().unwrap_or("repository"),
        )?;
        if is_self {
            map.serialize_entry("repositoryType", "self")?;
            if let Some(path) = &reference.path {
                map.serialize_entry("path", path)?;
            }
        } else if is_container_registry {
            if let Some(image) = &reference.name {
                map.serialize_entry("image", image)?;
            }
        } else {
            if let Some(name) = &reference.name {
                map.serialize_entry("name", name)?;
            }
            if let Some(version) = &reference.version {
                map.serialize_entry("ref", version)?;
            }
            if reference.reference_type.is_none() {
                map.serialize_entry("repositoryType", "GitHub")?;
            }
        }
        map.end()
    }
}

struct TemplateStringMap<'a>(&'a BTreeMap<String, String>, bool);

impl Serialize for TemplateStringMap<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;

        let with_loc = self.1;
        let extra = usize::from(with_loc) * 3;
        let mut map = serializer.serialize_map(Some(if self.0.is_empty() {
            1 + extra
        } else {
            2 + extra
        }))?;
        map.serialize_entry("type", &2)?;
        if with_loc {
            map.serialize_entry("col", &0)?;
            map.serialize_entry("file", &1)?;
            map.serialize_entry("line", &0)?;
        }
        if !self.0.is_empty() {
            let pairs: Vec<TemplateStringMapPair<'_>> = self
                .0
                .iter()
                .map(|(key, value)| TemplateStringMapPair {
                    key,
                    value,
                    with_loc,
                })
                .collect();
            map.serialize_entry("map", &pairs)?;
        }
        map.end()
    }
}

struct TemplateStringMapPair<'a> {
    key: &'a str,
    value: &'a str,
    with_loc: bool,
}

impl Serialize for TemplateStringMapPair<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;
        let mut value = template_string_token(self.value);
        if let Some(token) = value.as_object_mut() {
            token.insert("file".to_owned(), serde_json::json!(1));
            token.insert("line".to_owned(), serde_json::json!(0));
            token.insert("col".to_owned(), serde_json::json!(0));
        }
        let key_token = if self.with_loc {
            serde_json::json!({"type": 0, "lit": self.key, "col": 0, "file": 1, "line": 0})
        } else {
            serde_json::json!({"type": 0, "lit": self.key})
        };
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("Key", &key_token)?;
        map.serialize_entry("Value", &value)?;
        map.end()
    }
}

pub(crate) fn template_string_token(value: &str) -> serde_json::Value {
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
pub(crate) fn find_expression_end(s: &str) -> Option<usize> {
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
    #[serde(
        rename = "name",
        alias = "image",
        skip_serializing_if = "Option::is_none"
    )]
    pub name: Option<String>,
    #[serde(rename = "path", skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
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
