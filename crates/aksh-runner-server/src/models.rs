use super::*;

#[derive(Debug, Clone)]
pub(crate) struct DapPortRegistration {
    pub(crate) port: u16,
    pub(crate) job_id: JobId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StepRecord {
    pub(crate) name: String,
    pub(crate) conclusion: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JobDetail {
    pub(crate) name: String,
    pub(crate) conclusion: String,
    pub(crate) steps: Vec<StepRecord>,
}

/// Metadata tracked per log file for results-service Twirp retrieval.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct LogMetadata {
    /// Total bytes appended so far.
    pub(crate) byte_count: usize,
    /// Total lines appended so far.
    pub(crate) line_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RunRecord {
    pub(crate) run_id: RunId,
    pub(crate) submission: WorkflowSubmission,
    pub(crate) jobs: BTreeMap<JobId, ExecutionStatus>,
    pub(crate) status: ExecutionStatus,
    pub(crate) job_outputs: BTreeMap<JobId, BTreeMap<String, serde_json::Value>>,
    pub(crate) job_base_ids: BTreeMap<JobId, String>,
    #[serde(skip)]
    pub(crate) job_needs: BTreeMap<JobId, Vec<JobId>>,
    pub(crate) job_fail_fast: BTreeMap<String, bool>,
    #[serde(default)]
    pub(crate) job_check_run_ids: BTreeMap<JobId, u64>,
    #[serde(default)]
    pub(crate) reusable_calls: BTreeMap<String, aksh_gha_parser::ReusableCallMetadata>,
    #[serde(default)]
    pub(crate) jobs_list: Vec<JobDetail>,
}

#[derive(Debug, Clone)]
pub(crate) struct TaskAgentJobRequestRecord {
    pub(crate) request_id: i64,
    pub(crate) run_id: RunId,
    pub(crate) job_id: JobId,
    pub(crate) agent_job_id: uuid::Uuid,
    pub(crate) plan_id: String,
    pub(crate) plan_type: String,
    pub(crate) timeline_id: uuid::Uuid,
    pub(crate) result: Option<ExecutionStatus>,
    pub(crate) locked_until: String,
    pub(crate) started_at: Option<std::time::SystemTime>,
    pub(crate) last_renewed_at: Option<std::time::SystemTime>,
    pub(crate) timeout_triggered: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct QueuedJob {
    pub(crate) run_id: RunId,
    pub(crate) job_id: JobId,
    pub(crate) base_id: String,
    pub(crate) needs: Vec<JobId>,
    pub(crate) if_condition: Option<String>,
    pub(crate) condition_context: aksh_gha_expressions::Context,
    pub(crate) max_parallel: Option<u64>,
    /// Required runner labels from `runs-on`.
    pub(crate) runs_on: Vec<String>,
    pub(crate) message: azdo::AgentJobRequestMessage,
    /// Raw job-level concurrency (evaluated when the job becomes ready).
    pub(crate) concurrency: Option<aksh_gha_parser::Concurrency>,
    /// Matrix values for this expansion (for concurrency expression eval).
    pub(crate) matrix: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone)]
pub(crate) struct QueuedCancellation {
    pub(crate) run_id: RunId,
    pub(crate) job_id: JobId,
    /// Agent job GUID from the job message (`jobId`), required for official JobCancelMessage.
    pub(crate) agent_job_id: uuid::Uuid,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingCache {
    pub(crate) key: String,
    pub(crate) version: String,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ArtifactRecord {
    pub(crate) id: String,
    pub(crate) run_id: RunId,
    pub(crate) name: String,
    pub(crate) file_name: String,
    pub(crate) path: String,
    pub(crate) size: u64,
}

/// Pending cache v2 upload (Twirp CacheService).
#[derive(Debug)]
pub(crate) struct CacheV2Pending {
    pub(crate) key: String,
    pub(crate) version: String,
}

/// Pending artifact v2 upload (Twirp ArtifactService).
#[derive(Debug)]
pub(crate) struct ArtifactV2Pending {
    /// Registry key = "{run_backend_id}/{job_backend_id}/{name}".
    pub(crate) registry_key: String,
}

/// Finalized artifact v2 entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ArtifactV2Entry {
    pub(crate) id: u64,
    pub(crate) workflow_run_backend_id: String,
    pub(crate) workflow_job_run_backend_id: String,
    pub(crate) name: String,
    pub(crate) size: u64,
    pub(crate) created_at: String,
    pub(crate) digest: Option<String>,
    /// Upload token used to find the assembled blob on disk.
    pub(crate) blob_token: String,
}
