//! Background server reporting queue.
//!
//! Batches step status updates and log uploads to the server,
//! flushing periodically and at step boundaries.
//!
//! F014: The WorkflowStepsUpdate Twirp body uses these fields (from golden flow 24):
//! - `steps[{external_id, number, name, status, started_at, completed_at, conclusion}]`
//! - `change_order` (monotonic counter)
//! - `workflow_job_run_backend_id` (= jobId from the job message)
//! - `workflow_run_backend_id` (= planId from the job message)
//!
//! Status enum: 6 = completed
//! Conclusion enum: 2 = succeeded, 3 = failed, 7 = skipped

use std::collections::HashMap;
use tracing::debug;

pub mod step_status {
    /// Step is in progress.
    pub const IN_PROGRESS: u32 = 2;
    /// Step has completed.
    pub const COMPLETED: u32 = 6;
}

/// Step conclusion values matching the Twirp proto enum.
pub mod step_conclusion {
    /// Step succeeded.
    pub const SUCCEEDED: u32 = 2;
    /// Step failed.
    pub const FAILED: u32 = 3;
    /// Step was cancelled.
    pub const CANCELLED: u32 = 4;
    /// Step was skipped.
    pub const SKIPPED: u32 = 7;
}

/// A step update matching the WorkflowStepsUpdate Twirp schema.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StepUpdate {
    /// Step external ID (UUID from the job message step).
    pub external_id: String,
    /// Step ordinal number (1-based).
    pub number: u32,
    /// Step display name.
    pub name: String,
    /// Step status (see `step_status` constants).
    pub status: u32,
    /// ISO 8601 timestamp when the step started.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    /// ISO 8601 timestamp when the step completed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    /// Step conclusion (see `step_conclusion` constants).
    pub conclusion: u32,
}

/// The full WorkflowStepsUpdate request body.
#[derive(Debug, Clone, serde::Serialize)]
pub struct WorkflowStepsUpdateBody {
    /// Step updates.
    pub steps: Vec<StepUpdate>,
    /// Monotonically increasing change counter.
    pub change_order: u64,
    /// Job ID from the job message.
    pub workflow_job_run_backend_id: String,
    /// Plan ID from the job message.
    pub workflow_run_backend_id: String,
}

/// Queued log lines for a step.
#[derive(Debug, Clone)]
pub struct StepLog {
    /// Step external ID.
    pub step_id: String,
    /// Log lines.
    pub lines: Vec<String>,
}

/// The server reporting queue.
pub struct ServerQueue {
    pending_updates: Vec<StepUpdate>,
    pending_logs: HashMap<String, Vec<String>>,
    /// Accumulated log content temp file (used for job log).
    job_log_file: std::io::BufWriter<std::fs::File>,
    change_order: u64,
    job_id: String,
    plan_id: String,
    /// All updates ever queued — populated only in test builds.
    #[cfg(test)]
    all_updates_log: Vec<StepUpdate>,
}

impl ServerQueue {
    /// Create a new server queue for a specific job.
    pub fn new(job_id: String, plan_id: String) -> Self {
        Self {
            pending_updates: Vec::new(),
            pending_logs: HashMap::new(),
            job_log_file: std::io::BufWriter::new(tempfile::tempfile().expect("failed to create job log temp file")),
            change_order: 0,
            job_id,
            plan_id,
            #[cfg(test)]
            all_updates_log: Vec::new(),
        }
    }

    /// Queue a step status update.
    pub fn queue_update(&mut self, update: StepUpdate) {
        debug!(
            "Queued update for step {}: status={} conclusion={}",
            update.external_id, update.status, update.conclusion
        );
        #[cfg(test)]
        self.all_updates_log.push(update.clone());
        self.pending_updates.push(update);
    }

    /// Queue log lines for a step.
    pub fn queue_log_lines(&mut self, step_id: &str, lines: Vec<String>) {
        let entry = self.pending_logs.entry(step_id.to_string()).or_default();
        entry.extend(lines);
    }

    /// Build the WorkflowStepsUpdate request body and drain pending updates.
    ///
    /// Deduplicates by `external_id`, keeping only the latest update per step.
    /// This matches official runner behavior: the final batch contains each step
    /// in its terminal state only (no InProgress → Completed pairs).
    pub fn take_steps_update_body(&mut self) -> Option<WorkflowStepsUpdateBody> {
        if self.pending_updates.is_empty() {
            return None;
        }
        // Dedup: keep last update per external_id (Completed overwrites InProgress)
        let mut seen = HashMap::new();
        for update in std::mem::take(&mut self.pending_updates) {
            seen.insert(update.external_id.clone(), update);
        }
        let mut steps: Vec<StepUpdate> = seen.into_values().collect();
        steps.sort_by_key(|s| s.number);
        self.change_order += 1;
        Some(WorkflowStepsUpdateBody {
            steps,
            change_order: self.change_order,
            workflow_job_run_backend_id: self.job_id.clone(),
            workflow_run_backend_id: self.plan_id.clone(),
        })
    }

    /// Take all pending logs (drains the queue).
    pub fn take_logs(&mut self) -> HashMap<String, Vec<String>> {
        std::mem::take(&mut self.pending_logs)
    }

    /// Check if there are any pending items.
    pub fn has_pending(&self) -> bool {
        !self.pending_updates.is_empty() || !self.pending_logs.is_empty()
    }

    /// Map a conclusion string to the proto enum value.
    pub fn conclusion_to_proto(conclusion: &str) -> u32 {
        match conclusion.to_lowercase().as_str() {
            "success" | "succeeded" => step_conclusion::SUCCEEDED,
            "failure" | "failed" => step_conclusion::FAILED,
            "skipped" => step_conclusion::SKIPPED,
            "cancelled" | "canceled" => step_conclusion::CANCELLED,
            _ => step_conclusion::SUCCEEDED,
        }
    }

    /// Record completed step logs into the accumulated store (for job log assembly).
    pub fn record_step_logs(&mut self, _step_id: &str, content: &str) {
        use std::io::Write;
        let _ = write!(self.job_log_file, "{}", content);
        if !content.ends_with('\n') && !content.is_empty() {
            let _ = writeln!(self.job_log_file);
        }
    }

    /// Return concatenated content of all accumulated step logs (for job log upload).
    pub fn all_step_log_content(&mut self) -> String {
        use std::io::{Read, Seek, SeekFrom, Write};
        let _ = self.job_log_file.flush();
        let file = self.job_log_file.get_ref();
        if let Ok(mut cloned) = file.try_clone() {
            let mut content = String::new();
            let _ = cloned.seek(SeekFrom::Start(0));
            let _ = cloned.read_to_string(&mut content);
            content
        } else {
            String::new()
        }
    }
}

#[cfg(test)]
impl ServerQueue {
    /// Return every StepUpdate that was ever passed to queue_update (test-only).
    pub fn all_queued_updates(&self) -> &[StepUpdate] {
        &self.all_updates_log
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_and_take_steps_update() {
        let mut q = ServerQueue::new("job-1".into(), "plan-1".into());
        assert!(!q.has_pending());

        q.queue_update(StepUpdate {
            external_id: "step-uuid-1".into(),
            number: 1,
            name: "Set up job".into(),
            status: step_status::COMPLETED,
            started_at: Some("2024-01-01T00:00:00Z".into()),
            completed_at: Some("2024-01-01T00:00:01Z".into()),
            conclusion: step_conclusion::SUCCEEDED,
        });
        assert!(q.has_pending());

        let body = q.take_steps_update_body().unwrap();
        assert_eq!(body.steps.len(), 1);
        assert_eq!(body.steps[0].external_id, "step-uuid-1");
        assert_eq!(body.steps[0].status, 6);
        assert_eq!(body.steps[0].conclusion, 2);
        assert_eq!(body.change_order, 1);
        assert_eq!(body.workflow_job_run_backend_id, "job-1");
        assert_eq!(body.workflow_run_backend_id, "plan-1");
        assert!(!q.has_pending());

        // Serializes correctly
        let json = serde_json::to_value(&body).unwrap();
        assert!(json.get("change_order").is_some());
        assert!(json.get("workflow_job_run_backend_id").is_some());
        assert!(json.get("workflow_run_backend_id").is_some());
    }

    #[test]
    fn queue_and_take_logs() {
        let mut q = ServerQueue::new("j".into(), "p".into());
        q.queue_log_lines("s1", vec!["line1".into(), "line2".into()]);
        q.queue_log_lines("s1", vec!["line3".into()]);
        q.queue_log_lines("s2", vec!["other".into()]);

        assert!(q.has_pending());
        let logs = q.take_logs();
        assert_eq!(logs.get("s1").unwrap().len(), 3);
        assert_eq!(logs.get("s2").unwrap().len(), 1);
    }

    #[test]
    fn record_and_assemble_logs() {
        let mut q = ServerQueue::new("j".into(), "p".into());
        q.record_step_logs("s1", "line1\nline2\n");
        q.record_step_logs("s2", "line3");
        let content = q.all_step_log_content();
        assert_eq!(content, "line1\nline2\nline3\n");
    }

    #[test]
    fn change_order_increments() {
        let mut q = ServerQueue::new("j".into(), "p".into());
        q.queue_update(StepUpdate {
            external_id: "a".into(),
            number: 1,
            name: "step".into(),
            status: step_status::COMPLETED,
            started_at: None,
            completed_at: None,
            conclusion: step_conclusion::SUCCEEDED,
        });
        let b1 = q.take_steps_update_body().unwrap();
        assert_eq!(b1.change_order, 1);

        q.queue_update(StepUpdate {
            external_id: "b".into(),
            number: 2,
            name: "step 2".into(),
            status: step_status::COMPLETED,
            started_at: None,
            completed_at: None,
            conclusion: step_conclusion::FAILED,
        });
        let b2 = q.take_steps_update_body().unwrap();
        assert_eq!(b2.change_order, 2);
    }

    #[test]
    fn conclusion_mapping() {
        assert_eq!(
            ServerQueue::conclusion_to_proto("Success"),
            step_conclusion::SUCCEEDED
        );
        assert_eq!(
            ServerQueue::conclusion_to_proto("Failure"),
            step_conclusion::FAILED
        );
        assert_eq!(
            ServerQueue::conclusion_to_proto("Skipped"),
            step_conclusion::SKIPPED
        );
    }
}
