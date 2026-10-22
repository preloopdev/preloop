//! Run, job and step records, as `GET /api/v1/runs` and `GET /api/v1/runs/{id}`
//! return them.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::feed::Annotation;

/// Status of a run, job or step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecStatus {
    /// Accepted, waiting for a runner.
    Queued,
    /// Waiting on a dependency, a concurrency group or a provisioning request.
    Pending,
    /// Executing now.
    InProgress,
    /// Finished successfully.
    Success,
    /// Finished with a failure.
    Failure,
    /// Not run because a dependency did not succeed.
    Skipped,
    /// Stopped by a cancel.
    Cancelled,
    /// A status this build does not know.
    #[default]
    #[serde(other)]
    Unknown,
}

impl ExecStatus {
    /// Parse a stored step conclusion (`pending`, `in_progress`, `failed`, ...).
    pub fn from_conclusion(text: &str) -> Self {
        match text {
            "queued" => Self::Queued,
            "pending" => Self::Pending,
            "in_progress" | "running" => Self::InProgress,
            "success" => Self::Success,
            "failure" | "failed" => Self::Failure,
            "skipped" => Self::Skipped,
            "cancelled" => Self::Cancelled,
            _ => Self::Unknown,
        }
    }

    /// True once no further change is expected.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Success | Self::Failure | Self::Skipped | Self::Cancelled
        )
    }

    /// True while the item is queued, pending or running.
    pub fn is_active(self) -> bool {
        matches!(self, Self::Queued | Self::Pending | Self::InProgress)
    }

    /// Lowercase word used in tables and footers.
    pub fn label(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Pending => "pending",
            Self::InProgress => "running",
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Skipped => "skipped",
            Self::Cancelled => "cancelled",
            Self::Unknown => "unknown",
        }
    }

    /// One-column glyph shown in every list.
    pub fn glyph(self) -> &'static str {
        match self {
            Self::Queued | Self::Pending => "◌",
            Self::InProgress => "●",
            Self::Success => "✓",
            Self::Failure => "✗",
            Self::Skipped | Self::Cancelled => "⊘",
            Self::Unknown => "?",
        }
    }
}

/// What triggered a run, from its stored submission.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Submission {
    /// GitHub event name: `push`, `pull_request`, `schedule`, ...
    pub event: String,
    /// `owner/name` of the repository.
    pub repository: String,
    /// Git ref, such as `refs/heads/main` or `refs/pull/619/merge`.
    pub git_ref: String,
    /// Commit under test.
    pub sha: String,
    /// Login that triggered the run.
    pub actor: String,
    /// Workflow file name, when known.
    pub workflow_file: Option<String>,
    /// Raw webhook payload. Pull request fields live under `pull_request`.
    pub payload: Value,
}

/// Publication state of a run's result (`pending`, `synced`, `blocked`, ...).
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct PushState {
    /// Lowercase push status.
    pub status: String,
    /// Error text when the push is blocked.
    pub error: Option<String>,
    /// Pull request number the result was published to.
    pub pr_number: Option<u64>,
    /// Commit SHA the result was published as.
    pub effective_sha: Option<String>,
}

/// One step of a job.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct StepRecord {
    /// Stable step id. Live log lines carry the same value as `stepId`.
    pub id: String,
    /// `workflow` for declared steps, `synthetic` for runner-added steps.
    pub kind: String,
    /// 0-based declared position. Set only for workflow steps; the `step`
    /// log query takes this value plus one.
    pub workflow_index: Option<usize>,
    /// The runner's 1-based timeline position (counts synthetic steps too).
    pub runner_number: Option<u32>,
    /// Expression-context key derived from the workflow YAML.
    pub context_name: Option<String>,
    /// Display name.
    pub name: String,
    /// `pending`, `in_progress`, `success`, `failure`, `cancelled`, `skipped`
    /// or `failed`.
    pub conclusion: String,
    /// When the step first appeared.
    pub started_at: Option<DateTime<Utc>>,
    /// When the step turned terminal.
    pub finished_at: Option<DateTime<Utc>>,
}

impl StepRecord {
    /// The step's status as a typed value.
    pub fn status(&self) -> ExecStatus {
        ExecStatus::from_conclusion(&self.conclusion)
    }

    /// Elapsed time: final for finished steps, running total at `now` for live
    /// ones. `None` until the step has started.
    pub fn elapsed(&self, now: DateTime<Utc>) -> Option<TimeDelta> {
        let start = self.started_at?;
        let end = self.finished_at.unwrap_or(now);
        Some((end - start).max(TimeDelta::zero()))
    }
}

/// Order steps the way the engine does: runner timeline position, then start
/// time, then declared position. The list endpoint does not sort.
pub fn sort_steps(steps: &mut [StepRecord]) {
    steps.sort_by_key(|step| {
        (
            step.runner_number.unwrap_or(u32::MAX),
            step.started_at,
            step.workflow_index.unwrap_or(usize::MAX),
        )
    });
}

/// One job of a run, with its steps.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct JobDetail {
    /// Stable workflow job key (`build`, `test (ubuntu-latest)`).
    pub job_id: String,
    /// GitHub display name.
    pub name: String,
    /// Job conclusion as the engine reports it.
    pub conclusion: String,
    /// Steps. Not guaranteed to be sorted; see [`sort_steps`].
    pub steps: Vec<StepRecord>,
    /// Raw annotation values. Read them with [`JobDetail::annotations`].
    pub annotations: Vec<Value>,
}

impl JobDetail {
    /// Annotations that parse; malformed entries are skipped.
    pub fn annotations(&self) -> Vec<Annotation> {
        self.annotations
            .iter()
            .filter_map(|value| serde_json::from_value(value.clone()).ok())
            .collect()
    }
}

/// One workflow run, as the engine's run record.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct RunRecord {
    /// Run id (UUID, as a string).
    pub run_id: String,
    /// Display name, when the workflow sets one.
    pub run_name: Option<String>,
    /// What triggered the run.
    pub submission: Submission,
    /// Run status.
    pub status: ExecStatus,
    /// Status of every job, keyed by job id.
    pub jobs: BTreeMap<String, ExecStatus>,
    /// GitHub display names, keyed by job id.
    pub job_names: BTreeMap<String, String>,
    /// Matrix base id for each job. Legs of one matrix share a base.
    pub job_base_ids: BTreeMap<String, String>,
    /// Check run id per job, once created.
    pub job_check_run_ids: BTreeMap<String, u64>,
    /// Job details with steps.
    pub jobs_list: Vec<JobDetail>,
    /// When the run was accepted.
    pub created_at: Option<DateTime<Utc>>,
    /// When the first job started.
    pub started_at: Option<DateTime<Utc>>,
    /// When the run finished.
    pub completed_at: Option<DateTime<Utc>>,
    /// Monotonic run number for this workflow.
    pub run_number: u64,
    /// Attempt number (`github.run_attempt`).
    pub run_attempt: u64,
    /// Workflow file path, as the repository stores it.
    pub workflow_path_str: String,
    /// Event name, duplicated from the submission for list queries.
    pub event: String,
    /// Final conclusion, once the run is terminal.
    pub conclusion: Option<String>,
    /// Publication state of the run's result.
    pub push_state: Option<PushState>,
    /// Whether a fork run is waiting for approval.
    pub fork_approval_pending: bool,
    /// Note recorded with the fork approval request.
    pub fork_approval_note: Option<String>,
    /// Whether check runs are reported for this run.
    pub reports_check_runs: bool,
}

impl RunRecord {
    /// First eight characters of the run id.
    pub fn short_id(&self) -> &str {
        self.run_id.get(..8).unwrap_or(&self.run_id)
    }

    /// Workflow file name (`ci.yml`), from the path or the submission.
    pub fn workflow_name(&self) -> String {
        let from_path = self
            .workflow_path_str
            .rsplit('/')
            .next()
            .filter(|name| !name.is_empty());
        from_path
            .or(self.submission.workflow_file.as_deref())
            .unwrap_or("workflow")
            .to_owned()
    }

    /// The pull request payload object, when the run came from one.
    pub fn pull_request(&self) -> Option<&Value> {
        self.submission.payload.get("pull_request")
    }

    /// Pull request number, from the payload or the push state.
    pub fn pr_number(&self) -> Option<u64> {
        self.pull_request()
            .and_then(|pr| pr.get("number"))
            .and_then(Value::as_u64)
            .or_else(|| self.push_state.as_ref().and_then(|state| state.pr_number))
    }

    /// `#619 sandbox-service` for pull requests, otherwise the branch name.
    pub fn ref_label(&self) -> String {
        match self.pr_number() {
            Some(number) => {
                let head = self
                    .pull_request()
                    .and_then(|pr| pr.pointer("/head/ref"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                format!("#{number} {head}").trim_end().to_owned()
            }
            None => self
                .submission
                .git_ref
                .strip_prefix("refs/heads/")
                .unwrap_or(&self.submission.git_ref)
                .to_owned(),
        }
    }

    /// Status of one job. Unknown jobs read as [`ExecStatus::Unknown`].
    pub fn job_status(&self, job_id: &str) -> ExecStatus {
        self.jobs.get(job_id).copied().unwrap_or_default()
    }

    /// Display name of a job, falling back to its id.
    pub fn job_label<'a>(&'a self, job_id: &'a str) -> &'a str {
        self.job_names
            .get(job_id)
            .map(String::as_str)
            .unwrap_or(job_id)
    }

    /// The job's detail record, when the engine sent one.
    pub fn job_detail(&self, job_id: &str) -> Option<&JobDetail> {
        self.jobs_list.iter().find(|job| job.job_id == job_id)
    }

    /// Mutable job detail record, for applying live updates.
    pub fn job_detail_mut(&mut self, job_id: &str) -> Option<&mut JobDetail> {
        self.jobs_list.iter_mut().find(|job| job.job_id == job_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conclusions_map_to_statuses() {
        assert_eq!(ExecStatus::from_conclusion("failed"), ExecStatus::Failure);
        assert_eq!(ExecStatus::from_conclusion("in_progress"), ExecStatus::InProgress);
        assert_eq!(ExecStatus::from_conclusion("weird"), ExecStatus::Unknown);
    }

    #[test]
    fn unknown_status_strings_do_not_fail_parsing() {
        let run: RunRecord = serde_json::from_str(r#"{"run_id":"r","status":"sleeping"}"#)
            .expect("unknown status is lenient");
        assert_eq!(run.status, ExecStatus::Unknown);
    }

    #[test]
    fn sort_steps_orders_by_runner_number_then_index() {
        let mut steps = vec![
            StepRecord { id: "b".into(), runner_number: Some(3), ..Default::default() },
            StepRecord { id: "a".into(), runner_number: Some(2), ..Default::default() },
            StepRecord { id: "setup".into(), runner_number: Some(1), ..Default::default() },
        ];
        sort_steps(&mut steps);
        let order: Vec<&str> = steps.iter().map(|step| step.id.as_str()).collect();
        assert_eq!(order, ["setup", "a", "b"]);
    }

    #[test]
    fn ref_label_prefers_pull_request_number() {
        let run = RunRecord {
            submission: Submission {
                event: "pull_request".into(),
                payload: serde_json::json!({
                    "pull_request": {"number": 619, "head": {"ref": "sandbox-service"}}
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(run.ref_label(), "#619 sandbox-service");
    }
}
