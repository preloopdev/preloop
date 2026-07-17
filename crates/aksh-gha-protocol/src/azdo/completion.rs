use super::TaskResult;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
