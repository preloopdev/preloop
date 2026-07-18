use super::VariableValue;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ─── Timeline and recording DTOs ──────────────────────────────────────────

/// Reference to a timeline — a collection of timeline records for a job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineReference {
    #[serde(rename = "id")]
    pub id: uuid::Uuid,
    #[serde(rename = "changeId")]
    pub change_id: i32,
    #[serde(rename = "location")]
    pub location: Option<String>,
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
    #[serde(rename = "changeId", skip_serializing_if = "Option::is_none")]
    pub change_id: Option<i32>,
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
