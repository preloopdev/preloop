//! Backend-neutral row codecs for decomposed table families.
//!
//! Each family that used to be one sealed blob per key is stored as plain
//! rows. The codecs here convert between the domain struct and a flat row so
//! SQLite and Postgres bind/read the exact same columns and can never drift
//! in encoding.

use crate::models::{QueuedJob, StepKind, StepRecord};
use preloop_gha_protocol::{JobId, RunId};

/// One `job_steps` / `step_history` row. `position` preserves the manifest
/// order (`Vec` index); `step_id` is the protocol identity (`TaskStep.id`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StepRow {
    pub(crate) step_id: String,
    pub(crate) position: i64,
    pub(crate) kind: &'static str,
    pub(crate) workflow_index: Option<i64>,
    pub(crate) runner_number: Option<i64>,
    pub(crate) context_name: Option<String>,
    pub(crate) name: String,
    pub(crate) conclusion: String,
    pub(crate) started_at_us: Option<i64>,
    pub(crate) finished_at_us: Option<i64>,
}

pub(crate) fn step_kind_str(kind: StepKind) -> &'static str {
    match kind {
        StepKind::Workflow => "workflow",
        StepKind::Synthetic => "synthetic",
    }
}

/// Unknown kinds decode as `Synthetic`, the variant that refuses `--step`
/// resolution rather than guessing.
pub(crate) fn step_kind_parse(kind: &str) -> StepKind {
    match kind {
        "workflow" => StepKind::Workflow,
        _ => StepKind::Synthetic,
    }
}

impl StepRow {
    pub(crate) fn from_record(position: usize, record: &StepRecord) -> Self {
        StepRow {
            step_id: record.id.clone(),
            position: position as i64,
            kind: step_kind_str(record.kind),
            workflow_index: record.workflow_index.map(|i| i as i64),
            runner_number: record.runner_number.map(i64::from),
            context_name: record.context_name.clone(),
            name: record.name.clone(),
            conclusion: record.conclusion.clone(),
            started_at_us: record.started_at.map(|t| t.timestamp_micros()),
            finished_at_us: record.finished_at.map(|t| t.timestamp_micros()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn into_record(
        step_id: String,
        kind: &str,
        workflow_index: Option<i64>,
        runner_number: Option<i64>,
        context_name: Option<String>,
        name: String,
        conclusion: String,
        started_at_us: Option<i64>,
        finished_at_us: Option<i64>,
    ) -> StepRecord {
        StepRecord {
            id: step_id,
            kind: step_kind_parse(kind),
            workflow_index: workflow_index.and_then(|i| usize::try_from(i).ok()),
            runner_number: runner_number.and_then(|n| u32::try_from(n).ok()),
            context_name,
            name,
            conclusion,
            started_at: started_at_us.and_then(chrono::DateTime::from_timestamp_micros),
            finished_at: finished_at_us.and_then(chrono::DateTime::from_timestamp_micros),
        }
    }
}

/// Row-level delta for one attempt's manifest against its loaded snapshot.
/// `upserts` are rows new or changed (content or position); `deletes` are
/// step ids present at load but gone now. An unchanged manifest yields an
/// empty delta, so write-back touches nothing.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct StepDelta {
    pub(crate) upserts: Vec<StepRow>,
    pub(crate) deletes: Vec<String>,
}

pub(crate) fn step_delta(before: Option<&[StepRecord]>, after: &[StepRecord]) -> StepDelta {
    let before_rows: std::collections::HashMap<&str, StepRow> = before
        .unwrap_or_default()
        .iter()
        .enumerate()
        .map(|(i, r)| (r.id.as_str(), StepRow::from_record(i, r)))
        .collect();
    let mut delta = StepDelta::default();
    let mut kept = std::collections::HashSet::new();
    for (i, record) in after.iter().enumerate() {
        let row = StepRow::from_record(i, record);
        kept.insert(record.id.as_str());
        if before_rows.get(record.id.as_str()) != Some(&row) {
            delta.upserts.push(row);
        }
    }
    for id in before_rows.keys() {
        if !kept.contains(id) {
            delta.deletes.push((*id).to_owned());
        }
    }
    delta.deletes.sort();
    delta
}

/// Queryable `jobs` columns decoded from a [`QueuedJob`]. The runner message
/// and the `if:` evaluation context carry secrets, so they are sealed by the
/// backend into `job_messages` and never appear here. `needs` becomes
/// `job_needs` rows (one per edge, in declaration order).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JobPayloadRow {
    pub(crate) created_at_ns: i64,
    pub(crate) deps_ready_at_ns: Option<i64>,
    pub(crate) concurrency_wait_at_ns: Option<i64>,
    pub(crate) concurrency_acquired_at_ns: Option<i64>,
    pub(crate) if_condition: Option<String>,
    pub(crate) max_parallel: Option<i64>,
    pub(crate) environment_json: Option<String>,
    pub(crate) concurrency_json: Option<String>,
    pub(crate) matrix_json: String,
    pub(crate) deferred_matrix: Option<String>,
    pub(crate) reusable_call_json: Option<String>,
    pub(crate) needs: Vec<String>,
}

fn to_json<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("job payload fields serialize to JSON")
}

impl JobPayloadRow {
    pub(crate) fn from_job(job: &QueuedJob) -> Self {
        JobPayloadRow {
            created_at_ns: job.created_at_unix_nanos,
            deps_ready_at_ns: job.dependencies_ready_at_unix_nanos,
            concurrency_wait_at_ns: job.concurrency_wait_started_at_unix_nanos,
            concurrency_acquired_at_ns: job.concurrency_acquired_at_unix_nanos,
            if_condition: job.if_condition.clone(),
            max_parallel: job.max_parallel.map(|n| n as i64),
            environment_json: job.environment.as_ref().map(to_json),
            concurrency_json: job.concurrency.as_ref().map(to_json),
            matrix_json: to_json(&job.matrix),
            deferred_matrix: job.deferred_matrix.clone(),
            reusable_call_json: job.reusable_call.as_ref().map(to_json),
            needs: job.needs.iter().map(|n| n.0.clone()).collect(),
        }
    }

    /// Rebuild the job from its row, edges and the unsealed message/context.
    /// Fails closed on a malformed JSON column rather than guessing.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn into_job(
        self,
        run_id: RunId,
        job_id: JobId,
        base_id: String,
        runs_on_json: &str,
        runner_group: Option<String>,
        enqueued_at_us: Option<i64>,
        message: preloop_gha_protocol::azdo::AgentJobRequestMessage,
        condition_context: preloop_gha_expressions::Context,
    ) -> Result<QueuedJob, serde_json::Error> {
        Ok(QueuedJob {
            run_id,
            job_id,
            base_id,
            created_at_unix_nanos: self.created_at_ns,
            dependencies_ready_at_unix_nanos: self.deps_ready_at_ns,
            concurrency_wait_started_at_unix_nanos: self.concurrency_wait_at_ns,
            concurrency_acquired_at_unix_nanos: self.concurrency_acquired_at_ns,
            enqueued_at_unix_nanos: enqueued_at_us.map(|us| us * 1000).unwrap_or(0),
            needs: self.needs.into_iter().map(JobId).collect(),
            if_condition: self.if_condition,
            condition_context,
            max_parallel: self.max_parallel.and_then(|n| u64::try_from(n).ok()),
            runs_on: serde_json::from_str(runs_on_json)?,
            runner_group,
            message,
            environment: self
                .environment_json
                .as_deref()
                .map(serde_json::from_str)
                .transpose()?,
            concurrency: self
                .concurrency_json
                .as_deref()
                .map(serde_json::from_str)
                .transpose()?,
            matrix: serde_json::from_str(&self.matrix_json)?,
            deferred_matrix: self.deferred_matrix,
            reusable_call: self
                .reusable_call_json
                .as_deref()
                .map(serde_json::from_str)
                .transpose()?,
        })
    }
}

/// A [`RunRecord`](crate::models::RunRecord) decomposed into its tables.
///
/// - `runs` scalars (`RunScalars`) — one row, plain columns;
/// - `run_submissions` — the immutable-ish request the run was created from,
///   with secrets split out so only they are sealed;
/// - `run_jobs` — one row per job id carrying the per-job workflow facts
///   (name, base, needs, outputs, check run, detail, caller plan);
/// - `run_base_jobs` — per base-job flags (`fail-fast`, `continue-on-error`);
/// - `reusable_calls` — one row per reusable caller.
///
/// Nested structures are JSON text; equality on these rows is what lets
/// write-back touch only what a command changed.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RunParts {
    pub(crate) scalars: RunScalars,
    pub(crate) submission: RunSubmissionRow,
    pub(crate) jobs: std::collections::BTreeMap<String, RunJobRow>,
    pub(crate) base_jobs: std::collections::BTreeMap<String, RunBaseJobRow>,
    pub(crate) reusable_calls: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RunScalars {
    pub(crate) status: preloop_gha_protocol::ExecutionStatus,
    pub(crate) run_number: i64,
    pub(crate) run_attempt: i64,
    pub(crate) run_name: Option<String>,
    pub(crate) event: String,
    pub(crate) workflow_path: String,
    pub(crate) conclusion: Option<String>,
    pub(crate) webhook_delivery_id: Option<String>,
    pub(crate) head_sha: String,
    pub(crate) workflow_ref: String,
    pub(crate) push_state_json: Option<String>,
    pub(crate) snapshot_timing_json: Option<String>,
    pub(crate) created_at_us: i64,
    pub(crate) started_at_us: Option<i64>,
    pub(crate) completed_at_us: Option<i64>,
}

/// `submission_json` never contains secret values (the `secrets` key is
/// removed); `secrets` holds the exposed values the backend seals.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RunSubmissionRow {
    pub(crate) submission_json: String,
    pub(crate) secrets: std::collections::BTreeMap<String, String>,
    pub(crate) github_json: String,
    pub(crate) workspace_snapshot_json: Option<String>,
}

/// Per-job workflow facts. Each field is `None` when the corresponding
/// run-record map has no entry for the job (the maps are sparse).
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct RunJobRow {
    pub(crate) base_id: Option<String>,
    pub(crate) display_name: Option<String>,
    pub(crate) needs_json: Option<String>,
    pub(crate) outputs_json: Option<String>,
    pub(crate) check_run_id: Option<i64>,
    pub(crate) detail_json: Option<String>,
    /// Position of `detail_json` in the run's `jobs_list` (display order).
    pub(crate) detail_position: Option<i64>,
    pub(crate) caller_plan_json: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct RunBaseJobRow {
    pub(crate) fail_fast: Option<bool>,
    pub(crate) continue_on_error: Option<bool>,
}

fn job_row<'a>(
    jobs: &'a mut std::collections::BTreeMap<String, RunJobRow>,
    job: &str,
) -> &'a mut RunJobRow {
    jobs.entry(job.to_owned()).or_default()
}

fn time_us(t: chrono::DateTime<chrono::Utc>) -> i64 {
    t.timestamp_micros()
}

fn us_time(us: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_micros(us).unwrap_or_default()
}

impl RunParts {
    pub(crate) fn from_record(run: &crate::models::RunRecord) -> Self {
        let mut submission = serde_json::to_value(run.submission.as_ref())
            .expect("workflow submission serializes to JSON");
        if let Some(object) = submission.as_object_mut() {
            object.remove("secrets");
        }
        let mut jobs: std::collections::BTreeMap<String, RunJobRow> =
            std::collections::BTreeMap::new();
        for (job, base) in &run.job_base_ids {
            job_row(&mut jobs, &job.0).base_id = Some(base.clone());
        }
        for (job, name) in &run.job_names {
            job_row(&mut jobs, &job.0).display_name = Some(name.clone());
        }
        for (job, needs) in &run.job_needs {
            job_row(&mut jobs, &job.0).needs_json = Some(to_json(needs));
        }
        for (job, outputs) in &run.job_outputs {
            job_row(&mut jobs, &job.0).outputs_json = Some(to_json(outputs));
        }
        for (job, id) in &run.job_check_run_ids {
            job_row(&mut jobs, &job.0).check_run_id = Some(*id as i64);
        }
        for (job, plan) in &run.caller_plans {
            job_row(&mut jobs, &job.0).caller_plan_json = Some(to_json(plan));
        }
        for (position, detail) in run.jobs_list.iter().enumerate() {
            let entry = job_row(&mut jobs, &detail.job_id);
            entry.detail_json = Some(to_json(detail));
            entry.detail_position = Some(position as i64);
        }
        let mut base_jobs: std::collections::BTreeMap<String, RunBaseJobRow> =
            std::collections::BTreeMap::new();
        for (base, flag) in &run.job_fail_fast {
            base_jobs.entry(base.clone()).or_default().fail_fast = Some(*flag);
        }
        for (base, flag) in &run.job_continue_on_error {
            base_jobs.entry(base.clone()).or_default().continue_on_error = Some(*flag);
        }
        RunParts {
            scalars: RunScalars {
                status: run.status,
                run_number: run.run_number as i64,
                run_attempt: run.run_attempt as i64,
                run_name: run.run_name.clone(),
                event: run.event.clone(),
                workflow_path: run.workflow_path_str.clone(),
                conclusion: run.conclusion.clone(),
                webhook_delivery_id: run.webhook_delivery_id.clone(),
                head_sha: run.head_sha.clone(),
                workflow_ref: run.workflow_ref.clone(),
                push_state_json: run.push_state.as_ref().map(to_json),
                snapshot_timing_json: run.snapshot_timing.as_ref().map(to_json),
                created_at_us: time_us(run.created_at),
                started_at_us: run.started_at.map(time_us),
                completed_at_us: run.completed_at.map(time_us),
            },
            submission: RunSubmissionRow {
                submission_json: submission.to_string(),
                secrets: preloop_gha_protocol::masking::expose_all(&run.submission.secrets),
                github_json: run.github.to_string(),
                workspace_snapshot_json: run.workspace_snapshot.as_ref().map(to_json),
            },
            jobs,
            base_jobs,
            reusable_calls: run
                .reusable_calls
                .iter()
                .map(|(caller, meta)| (caller.clone(), to_json(meta)))
                .collect(),
        }
    }

    /// Rebuild the run record. `jobs` (the status map) is left empty: it is
    /// derived from the `jobs` table by the loader. A malformed JSON column
    /// fails the load rather than silently dropping workflow state, except
    /// the workspace snapshot, whose shape may legitimately lag the binary.
    pub(crate) fn into_record(
        self,
        run_id: RunId,
    ) -> Result<crate::models::RunRecord, serde_json::Error> {
        let mut submission: preloop_gha_protocol::WorkflowSubmission =
            serde_json::from_str(&self.submission.submission_json)?;
        submission.secrets = self
            .submission
            .secrets
            .into_iter()
            .map(|(name, value)| (name, preloop_gha_protocol::SecretString::new(value)))
            .collect();
        let workspace_snapshot = self
            .submission
            .workspace_snapshot_json
            .as_deref()
            .and_then(|json| match serde_json::from_str(json) {
                Ok(snapshot) => Some(snapshot),
                Err(error) => {
                    tracing::warn!(%run_id, %error, "dropping undecodable workspace snapshot on load");
                    None
                }
            });
        let mut record = crate::models::RunRecord {
            run_id,
            webhook_delivery_id: self.scalars.webhook_delivery_id,
            run_name: self.scalars.run_name,
            submission: std::sync::Arc::new(submission),
            jobs: std::collections::BTreeMap::new(),
            status: self.scalars.status,
            job_outputs: std::collections::BTreeMap::new(),
            job_base_ids: std::collections::BTreeMap::new(),
            job_needs: std::collections::BTreeMap::new(),
            caller_plans: std::collections::BTreeMap::new(),
            job_names: std::collections::BTreeMap::new(),
            github: serde_json::from_str(&self.submission.github_json)?,
            head_sha: self.scalars.head_sha,
            workflow_ref: self.scalars.workflow_ref,
            workspace_snapshot,
            job_fail_fast: std::collections::BTreeMap::new(),
            job_continue_on_error: std::collections::BTreeMap::new(),
            job_check_run_ids: std::collections::BTreeMap::new(),
            reusable_calls: std::collections::BTreeMap::new(),
            jobs_list: Vec::new(),
            created_at: us_time(self.scalars.created_at_us),
            started_at: self.scalars.started_at_us.map(us_time),
            completed_at: self.scalars.completed_at_us.map(us_time),
            run_number: self.scalars.run_number as u64,
            run_attempt: self.scalars.run_attempt as u64,
            workflow_path_str: self.scalars.workflow_path,
            event: self.scalars.event,
            conclusion: self.scalars.conclusion,
            push_state: self
                .scalars
                .push_state_json
                .as_deref()
                .map(serde_json::from_str)
                .transpose()?,
            snapshot_timing: self
                .scalars
                .snapshot_timing_json
                .as_deref()
                .map(serde_json::from_str)
                .transpose()?,
        };
        let mut details: Vec<(i64, crate::models::JobDetail)> = Vec::new();
        for (job, row) in self.jobs {
            let id = JobId(job.clone());
            if let Some(base) = row.base_id {
                record.job_base_ids.insert(id.clone(), base);
            }
            if let Some(name) = row.display_name {
                record.job_names.insert(id.clone(), name);
            }
            if let Some(needs) = row.needs_json {
                record
                    .job_needs
                    .insert(id.clone(), serde_json::from_str(&needs)?);
            }
            if let Some(outputs) = row.outputs_json {
                record
                    .job_outputs
                    .insert(id.clone(), serde_json::from_str(&outputs)?);
            }
            if let Some(check) = row.check_run_id {
                record.job_check_run_ids.insert(id.clone(), check as u64);
            }
            if let Some(plan) = row.caller_plan_json {
                record
                    .caller_plans
                    .insert(id.clone(), serde_json::from_str(&plan)?);
            }
            if let Some(detail) = row.detail_json {
                details.push((
                    row.detail_position.unwrap_or(i64::MAX),
                    serde_json::from_str(&detail)?,
                ));
            }
        }
        details.sort_by_key(|(position, _)| *position);
        record.jobs_list = details.into_iter().map(|(_, detail)| detail).collect();
        for (base, row) in self.base_jobs {
            if let Some(flag) = row.fail_fast {
                record.job_fail_fast.insert(base.clone(), flag);
            }
            if let Some(flag) = row.continue_on_error {
                record.job_continue_on_error.insert(base, flag);
            }
        }
        for (caller, meta) in self.reusable_calls {
            record
                .reusable_calls
                .insert(caller, serde_json::from_str(&meta)?);
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step(id: &str, conclusion: &str) -> StepRecord {
        StepRecord {
            id: id.to_owned(),
            kind: StepKind::Workflow,
            workflow_index: Some(0),
            runner_number: Some(2),
            context_name: Some("compile".to_owned()),
            name: "cargo build".to_owned(),
            conclusion: conclusion.to_owned(),
            started_at: chrono::DateTime::from_timestamp_micros(1_700_000_000_123_456),
            finished_at: None,
        }
    }

    #[test]
    fn row_round_trip_is_lossless() {
        let record = step("s1", "success");
        let row = StepRow::from_record(3, &record);
        let back = StepRow::into_record(
            row.step_id.clone(),
            row.kind,
            row.workflow_index,
            row.runner_number,
            row.context_name.clone(),
            row.name.clone(),
            row.conclusion.clone(),
            row.started_at_us,
            row.finished_at_us,
        );
        assert_eq!(StepRow::from_record(3, &back), row);
    }

    #[test]
    fn delta_touches_only_changed_steps() {
        let before = vec![step("s1", "success"), step("s2", "in_progress")];
        let after = vec![step("s1", "success"), step("s2", "failure")];
        let delta = step_delta(Some(&before), &after);
        assert_eq!(delta.upserts.len(), 1);
        assert_eq!(delta.upserts[0].step_id, "s2");
        assert!(delta.deletes.is_empty());
        assert_eq!(step_delta(Some(&after), &after), StepDelta::default());
    }

    #[test]
    fn delta_reports_removed_and_reordered_steps() {
        let before = vec![step("s1", "success"), step("s2", "success")];
        let after = vec![step("s2", "success")];
        let delta = step_delta(Some(&before), &after);
        assert_eq!(delta.deletes, vec!["s1".to_owned()]);
        // s2 moved from position 1 to 0: position is part of the row.
        assert_eq!(delta.upserts.len(), 1);
    }
}
