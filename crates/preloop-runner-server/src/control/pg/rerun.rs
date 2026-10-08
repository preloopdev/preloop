//! `rerun_run` on PostgreSQL: re-execute a completed run as a new attempt,
//! mirroring github.com's "Re-run jobs" behavior
//! (<https://docs.github.com/en/actions/how-tos/manage-workflow-runs/re-run-workflows-and-jobs>)
//! and the SQLite backend (`control/lite/rerun.rs`) decision for decision.
//!
//! One writer transaction, serialized on the run row ([`PgBackend::lock_run`]
//! — the schema's per-run mutex). The attempt boundary is the `runs`/`jobs`
//! reset: `job_requests`, `job_steps`, timelines and logs accumulate across
//! attempts untouched (they are the attempt archive), while the live `jobs`
//! row per logical job is snapshotted into `job_history` before it is reset —
//! `run_history`/`job_history` keyed by `(run_id, run_attempt)` reproduce
//! GitHub's per-attempt views.
//!
//! The classification loops mirrors `submit_run` step for step: workflow
//! concurrency is re-acquired first, then each selected job re-runs the
//! `if:`/platform/environment/concurrency/max-parallel admission chain and
//! mints a fresh `job_requests` attempt whose stored message carries the
//! bumped `github.run_attempt` and a new `jobId`. Jobs outside the selection
//! keep their rows — status and outputs both — so dependents see the
//! carried-forward `needs` context.
//!
//! The working set is the ordinary [`Sweep`] graph (loaded, mutated through
//! its nodes, flushed once): the promotion sweep that follows is the same one
//! `submit_run` runs, so a re-run's admission decisions cannot drift from a
//! submit's.

use super::codec::{self, now_us, us};
use super::dispatch::{
    GateHolder, GateOutcome, Sweep, acquire_gate, emit_outbox, insert_request_row,
    release_concurrency_for_run,
};
use super::graph::{NodeKind, RunGraph};
use super::lookups::PLAN_TYPE;
use super::{PgBackend, db};
use crate::concurrency::{self, Holder};
use crate::control::logic::{self, QueueState};
use crate::control::types::*;
use crate::models::{
    EnvironmentGateState, GitHubTokenRequest, StepRecord, TaskAgentJobRequestRecord,
};
use crate::runtime_scheduling::{self, EnvironmentGateOutcome};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId, azdo};
use std::collections::{BTreeMap, BTreeSet};
use tokio_postgres::{GenericClient, Transaction};

/// GitHub refuses a re-run more than 30 days after the run's creation and
/// caps attempts at ~50 (the request layer rejects `#51`).
const RERUN_WINDOW_US: i64 = 30 * 24 * 60 * 60 * 1_000_000;
const MAX_RUN_ATTEMPT: i64 = 51;

/// One `jobs` row decoded for the selector: status arrives as the raw stored
/// string so `timed_out` reaches [`logic::rerun_set`].
struct RerunRow {
    job_id: JobId,
    kind: String,
    base_id: String,
    parent_job_id: Option<String>,
    status_raw: String,
    needs: Vec<JobId>,
    job_order: i32,
}

/// Selector projection of the decoded rows for [`logic::rerun_set`].
fn selector_rows(rows: &[RerunRow]) -> Vec<logic::RerunJobRow> {
    rows.iter()
        .map(|member| logic::RerunJobRow {
            job_id: member.job_id.clone(),
            kind: member.kind.clone(),
            base_id: member.base_id.clone(),
            parent_job_id: member.parent_job_id.clone(),
            status: member.status_raw.clone(),
            needs: member.needs.clone(),
        })
        .collect()
}

impl PgBackend {
    /// `rerun_plan` (lite `rerun.rs`): the mode-resolved selection and which
    /// members lack a runnable `job_messages` template. Read-only; mirrors the
    /// write path's selection so templates are rebuilt for exactly the jobs
    /// that need them.
    pub(crate) async fn rerun_plan(
        &self,
        run_id: RunId,
        mode: &RerunMode,
    ) -> Result<RerunPlan, ControlError> {
        let client = self.reader().await?;
        let run = run_id.0.to_string();
        let exists: bool = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = $1::text::uuid)",
                &[&run],
            )
            .await
            .map_err(db)?
            .get(0);
        if !exists {
            return Err(ControlError::NotFound(format!("run {run_id}")));
        }

        // Job ids can contain commas, so needs load as their own rows.
        let job_rows = client
            .query(
                "SELECT job_id, kind, parent_job_id, base_id, status, job_order FROM jobs \
                 WHERE run_id = $1::text::uuid ORDER BY job_order",
                &[&run],
            )
            .await
            .map_err(db)?;
        let need_rows = client
            .query(
                "SELECT job_id, needs_job_id FROM job_needs \
                 WHERE run_id = $1::text::uuid ORDER BY job_id, position",
                &[&run],
            )
            .await
            .map_err(db)?;
        let mut needs: BTreeMap<JobId, Vec<JobId>> = BTreeMap::new();
        for row in &need_rows {
            needs
                .entry(JobId(row.get(0)))
                .or_default()
                .push(JobId(row.get(1)));
        }
        let rows: Vec<RerunRow> = job_rows
            .iter()
            .map(|row| RerunRow {
                job_id: JobId(row.get(0)),
                kind: row.get(1),
                parent_job_id: row.get(2),
                base_id: row.get(3),
                status_raw: row.get(4),
                needs: needs.get(&JobId(row.get(0))).cloned().unwrap_or_default(),
                job_order: row.get(5),
            })
            .collect();
        let set: BTreeSet<JobId> = logic::rerun_set(mode, &selector_rows(&rows))?;

        let mut missing_templates = Vec::new();
        for row in &rows {
            if !set.contains(&row.job_id) {
                continue;
            }
            // Expandable nodes mint runnable messages at expansion; a
            // placeholder row or no row is not a gap for them.
            let expandable: Option<(Option<String>, Option<String>)> = client
                .query_opt(
                    "SELECT deferred_matrix, reusable_call::text FROM job_specs \
                     WHERE run_id = $1::text::uuid AND job_id = $2",
                    &[&run, &row.job_id.0],
                )
                .await
                .map_err(db)?
                .map(|spec| (spec.get(0), spec.get(1)));
            let expandable = match expandable {
                None => false,
                Some((deferred, reusable)) => {
                    deferred.is_some()
                        || reusable
                            .map(|text| {
                                codec::from_json::<super::graph::ReusableNodeSpec>(&text)
                                    .map(|spec| spec.call.is_some())
                            })
                            .transpose()?
                            .unwrap_or(false)
                }
            };
            if expandable {
                continue;
            }
            if stored_job_message(&*client, run_id, &row.job_id)
                .await?
                .is_none()
            {
                missing_templates.push(row.job_id.clone());
            }
        }
        Ok(RerunPlan {
            set,
            missing_templates,
        })
    }

    /// `rerun_run` (lite `rerun.rs`): snapshot the completed attempt into the
    /// history tables, reset the selected jobs, and re-admit them through the
    /// submit classification chain. `NotFound`/`Conflict` per the trait
    /// contract.
    ///
    /// Statements: run-row lock + head read; `run_history` / `job_history`
    /// snapshots; `UPDATE runs` (new attempt); `run_submissions` github
    /// context + record details; stale `concurrency_waits` / `jobsets`;
    /// per-member `job_assignments` / `provision_requests`; the workflow gate;
    /// per-member `jobs` reset + `job_messages` attempt rewrite +
    /// `job_requests` (+ `github_token_requests`, `job_steps`) mint; the
    /// promotion sweep's own statements; `run.rerun.v1`.
    pub(crate) async fn rerun_run(&self, rerun: RerunRun) -> Result<RerunOutcome, ControlError> {
        let RerunRun {
            run_id,
            mode,
            workflow_concurrency,
            environment_rules: _environment_rules,
            templates,
        } = rerun;
        let resolver = self.environment_resolver();
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let run = run_id.0.to_string();

        // ── The run row: identity, clocks, guards ──────────────────────────
        if !PgBackend::lock_run(&tx, run_id).await? {
            return Err(ControlError::NotFound(format!("run {run_id}")));
        }
        let Some(head) = tx
            .query_opt(
                concat!(
                    "SELECT run_attempt, run_number, namespace_id, repository, ref, \
                     status, concurrency_group, concurrency_cancel_in_progress, ",
                    us!("created_at"),
                    " FROM runs WHERE run_id = $1::text::uuid"
                ),
                &[&run],
            )
            .await
            .map_err(db)?
        else {
            return Err(ControlError::NotFound(format!("run {run_id}")));
        };
        let prev_attempt = head.get::<_, i32>(0) as i64;
        let run_number = head.get::<_, i64>(1);
        let namespace: String = head.get(2);
        let repository: String = head.get(3);
        let git_ref: String = head.get(4);
        let run_status: String = head.get(5);
        let _prev_group: Option<String> = head.get(6);
        let _prev_cancel_in_progress: bool = head.get(7);
        let created_at_us: i64 = head.get::<_, Option<i64>>(8).unwrap_or(0);
        if run_status != "completed" {
            return Err(ControlError::Conflict(format!(
                "run {run_id} is {run_status}; only a completed run re-runs"
            )));
        }
        if prev_attempt + 1 > MAX_RUN_ATTEMPT {
            return Err(ControlError::Conflict(format!(
                "run {run_id} has reached the {MAX_RUN_ATTEMPT}-attempt limit"
            )));
        }
        if now_us() - created_at_us > RERUN_WINDOW_US {
            return Err(ControlError::Conflict(format!(
                "run {run_id} cannot be re-run more than 30 days after its creation"
            )));
        }
        let run_attempt = prev_attempt + 1;

        // ── Selection ──────────────────────────────────────────────────────
        let Some(mut graph) = PgBackend::load_graph(self, &tx, run_id).await? else {
            return Err(ControlError::NotFound(format!("run {run_id}")));
        };
        let mut raw_status: BTreeMap<JobId, String> = tx
            .query(
                "SELECT job_id, status FROM jobs WHERE run_id = $1::text::uuid",
                &[&run],
            )
            .await
            .map_err(db)?
            .iter()
            .map(|row| (JobId(row.get::<_, String>(0)), row.get::<_, String>(1)))
            .collect();
        let mut rows: Vec<RerunRow> = graph
            .nodes
            .iter()
            .map(|(job_id, node)| RerunRow {
                job_id: job_id.clone(),
                kind: node.kind.as_str().to_owned(),
                base_id: node.base_id.clone(),
                parent_job_id: node.parent_job_id.clone(),
                status_raw: raw_status.get(job_id).cloned().unwrap_or_default(),
                needs: node.needs.clone(),
                job_order: node.job_order,
            })
            .collect();
        rows.sort_by_key(|member| member.job_order);
        if rows.is_empty() {
            return Err(ControlError::backend(anyhow::anyhow!(
                "run {run_id} has no jobs rows to re-run"
            )));
        }
        let set: BTreeSet<JobId> = logic::rerun_set(&mode, &selector_rows(&rows))?;

        // ── Snapshot the completed attempt ─────────────────────────────────
        // `run_history`/`job_history` key the snapshot on the *old* attempt;
        // the run row itself is reset next. `job_requests`/`job_steps` already
        // accumulate per attempt — no copy.
        snapshot_attempt(&tx, &run, prev_attempt).await?;

        // ── The new attempt on the run row ─────────────────────────────────
        // Clear the clocks a fresh run would not carry: GitHub's new attempt
        // has its own started/completed pair and no conclusion. The stored
        // concurrency group is re-derived below (the gate re-acquires it).
        tx.execute(
            "UPDATE runs SET run_attempt = $2, status = 'queued', conclusion = NULL, \
                 started_at = NULL, completed_at = NULL, \
                 concurrency_group = NULL, concurrency_cancel_in_progress = false \
             WHERE run_id = $1::text::uuid",
            &[&run, &(run_attempt as i32)],
        )
        .await
        .map_err(db)?;

        // Bump `github.run_attempt` in the stored context: rebuilt templates
        // read it, and the gate contexts serve it.
        let github = bump_run_attempt(&tx, &run, run_attempt).await?;

        // Check-run ids are per-attempt: a re-run mints fresh `check_run`s and
        // the old ids stay on the attempt-1 history rows. Drop the mapping for
        // every selected job so the new attempt's `job.check_run_id` starts
        // empty (the message carries `0` until the reporter back-fills).
        clear_recorded_check_runs(&tx, &run, &set).await?;

        // A completed run holds no concurrency rows; a rerun of one still on
        // the books (e.g. a waiter the completion path left) is cleaned here
        // rather than trusted absent.
        clear_stale_concurrency(&tx, &run, &set).await?;
        settle_stale_requests(&tx, &run, &rows, &set).await?;

        // The new attempt claims its own dispatch intents: pooled provisioning
        // and runner assignments mint again at enqueue.
        for member in &set {
            super::dispatch::clear_assignment(&tx, run_id, member).await?;
        }

        // ── Workflow-level gate ────────────────────────────────────────────
        // Same shape as submit: acquire before classification, cancel the
        // selection on ArrivalCancelled, park it held on a busy group.
        let mut held = false;
        if let Some(wf) = &workflow_concurrency {
            let key = concurrency::concurrency_key(&repository, &wf.group);
            match acquire_gate(
                self,
                &tx,
                &namespace,
                &key,
                &wf.group,
                &GateHolder {
                    holder: Holder::Run(run_id),
                    jobset_id: None,
                },
                wf.cancel_in_progress,
                wf.queue,
            )
            .await?
            {
                GateOutcome::Acquired => {}
                GateOutcome::Parked => held = true,
                GateOutcome::Cancelled => {
                    let concluded =
                        arrival_cancelled(&tx, &run, run_id, &set, &rows, run_attempt).await?;
                    tx.commit().await.map_err(db)?;
                    drop(client);
                    return Ok(RerunOutcome {
                        run_id,
                        run_attempt: run_attempt as u64,
                        run_number: run_number as u64,
                        queued_jobs: 0,
                        status: ExecutionStatus::Cancelled,
                        concluded,
                        rerun_jobs: Vec::new(),
                        selected: set.clone(),
                        held: false,
                        next_runs_on: self.ready_front_labels().await?,
                    });
                }
                GateOutcome::Failed => {
                    return Err(ControlError::backend(anyhow::anyhow!(
                        "workflow concurrency admission failed"
                    )));
                }
            }
            // The gate won is recorded on the run (submit stores the evaluated
            // group at insert; the rerun rewrites it after re-acquiring).
            tx.execute(
                "UPDATE runs SET concurrency_group = $2, \
                     concurrency_cancel_in_progress = $3 \
                 WHERE run_id = $1::text::uuid",
                &[&run, &wf.group, &wf.cancel_in_progress],
            )
            .await
            .map_err(db)?;
        }
        // ── The projection takes the new attempt's identity ────────────────
        // `flush_run` writes status/clocks back from the record, so the row's
        // reset is mirrored in memory; the gate contexts read the bumped
        // `github.run_attempt`.
        graph.record.run_attempt = run_attempt as u64;
        graph.record.status = ExecutionStatus::Queued;
        graph.record.conclusion = None;
        graph.record.started_at = None;
        graph.record.completed_at = None;
        graph.github = github.clone();
        graph.record.github = github.clone();
        for member in &set {
            graph.record.job_check_run_ids.remove(member);
        }

        let mut sweep = Sweep::new(self, &tx).await?;
        sweep.graphs.insert(run_id, graph);

        // ── Per-job reset + classification ─────────────────────────────────
        // Mirrors `submit_run`'s per-job chain: `if:` (needs-less jobs only —
        // needs-gated `if:`s re-evaluate in the promotion sweep), platform and
        // pool-label checks, environment gate, concurrency gate, max-parallel.
        let templates: BTreeMap<&str, &RerunJobTemplate> = templates
            .iter()
            .map(|template| (template.job_id.0.as_str(), template))
            .collect();
        let mut concluded: Vec<(JobId, ExecutionStatus, Option<String>)> = Vec::new();
        let mut rerun_jobs: Vec<(JobId, Option<String>)> = Vec::new();
        let mut queued = 0usize;
        let platforms = PgBackend::registered_platforms_on(&tx).await?;
        let pool_labels = self.pool_labels();
        let runner_labels: Vec<Vec<String>> = if pool_labels.is_empty() {
            Vec::new()
        } else {
            tx.query("SELECT labels::text FROM runners", &[])
                .await
                .map_err(db)?
                .iter()
                .map(|row| codec::from_json(&row.get::<_, String>(0)).unwrap_or_default())
                .collect()
        };
        let mut active_by_base: BTreeMap<String, u64> = BTreeMap::new();

        for member in &rows {
            if !set.contains(&member.job_id) {
                continue;
            }
            let job_id = member.job_id.clone();
            let base_id = member.base_id.clone();
            let (
                if_condition,
                max_parallel,
                runs_on,
                environment,
                expandable,
                caller_with_children,
                condition_context,
                needs_len,
            ) = {
                let Some(node) = sweep
                    .graphs
                    .get(&run_id)
                    .and_then(|graph| graph.nodes.get(&job_id))
                else {
                    continue;
                };
                (
                    node.if_condition.clone(),
                    node.max_parallel,
                    node.runs_on.clone(),
                    node.environment.clone(),
                    node.expandable(),
                    node.reusable
                        .as_ref()
                        .is_some_and(|spec| spec.call.is_some())
                        && rows.iter().any(|candidate| {
                            candidate.parent_job_id.as_deref() == Some(job_id.0.as_str())
                        }),
                    node.condition_context.clone(),
                    node.needs.len(),
                )
            };
            let has_needs = needs_len > 0;

            // A caller whose subtree re-runs returns to `in_progress`: its own
            // status is derived from the children, which
            // `finalize_reusable_callers` folds back when they settle.
            if caller_with_children {
                if let Some(node) = sweep.node_mut(run_id, &job_id) {
                    node.status = ExecutionStatus::InProgress;
                    node.queue_state = QueueState::None;
                    node.outputs = None;
                    node.completed_at_us = None;
                }
                sweep.mark(run_id, &job_id);
                note_reset(&mut sweep, run_id, &job_id, ExecutionStatus::InProgress);
                rerun_jobs.push((job_id, None));
                continue;
            }

            // `if:` re-evaluates exactly where submit evaluated it: needs-less
            // jobs here, needs-gated jobs inside the promotion sweep below.
            if !has_needs {
                // A handler-supplied rebuild carries the fresh context; the
                // stored one is still the previous attempt's until the message
                // rewrite below, so patch `run_attempt` for evaluation parity.
                let mut context = match templates.get(job_id.0.as_str()) {
                    Some(template) => template.condition_context.clone(),
                    None => condition_context.clone(),
                };
                context.insert("github", github.clone());
                let condition =
                    preloop_gha_expressions::effective_condition(if_condition.as_deref());
                match preloop_gha_expressions::eval_bool(&condition, &context) {
                    Ok(true) => {}
                    Ok(false) => {
                        reset_concluded(&mut sweep, run_id, &job_id, ExecutionStatus::Skipped);
                        concluded.push((job_id, ExecutionStatus::Skipped, None));
                        continue;
                    }
                    Err(error) => {
                        tracing::warn!(
                            %run_id,
                            job = %job_id.0,
                            %error,
                            "re-run `if:` evaluation failed; concluding the job"
                        );
                        reset_concluded(&mut sweep, run_id, &job_id, ExecutionStatus::Failure);
                        concluded.push((job_id, ExecutionStatus::Failure, Some(error.to_string())));
                        continue;
                    }
                }
            }

            if let Some(platform) = logic::unhostable_platform(&runs_on, platforms.iter().copied())
            {
                reset_concluded(&mut sweep, run_id, &job_id, ExecutionStatus::Failure);
                concluded.push((
                    job_id,
                    ExecutionStatus::Failure,
                    Some(logic::unhostable_reason(platform, &runs_on)),
                ));
                continue;
            }

            if !expandable
                && let Some(reason) = logic::unschedulable_reason(
                    &runs_on,
                    &pool_labels,
                    runner_labels
                        .iter()
                        .any(|labels| runtime_scheduling::job_matches_runner(&runs_on, labels)),
                )
            {
                reset_concluded(&mut sweep, run_id, &job_id, ExecutionStatus::Failure);
                concluded.push((job_id, ExecutionStatus::Failure, Some(reason)));
                continue;
            }

            // Environment protection re-evaluates exactly like submit: the old
            // gate state is discarded (`None`) and a wait/approval re-arms.
            let mut gate: Option<EnvironmentGateState> = None;
            if let Some(environment_name) =
                runtime_scheduling::environment_gate_name_of(environment.as_ref())
            {
                let lookup = resolver.lookup_sync(&repository, environment_name);
                match runtime_scheduling::evaluate_environment_gate(
                    &lookup,
                    &git_ref,
                    run_id,
                    &job_id,
                    environment_name,
                    &mut gate,
                    crate::models::now_unix_nanos(),
                ) {
                    EnvironmentGateOutcome::Proceed => {}
                    // Submit parks a `Wait`/`Failed` verdict as held+pending; the
                    // reaper's `promote_ready_jobs` sweep releases or fails it.
                    EnvironmentGateOutcome::Wait | EnvironmentGateOutcome::Failed => {}
                }
            }

            // Classification, mirroring submit's ordering.
            let gate_armed = gate.is_some();
            let max_parallel_ok = max_parallel
                .is_none_or(|limit| active_by_base.get(&base_id).copied().unwrap_or(0) < limit);
            let (status, queue_state) = if held || gate_armed {
                (ExecutionStatus::Pending, QueueState::Held)
            } else if expandable || has_needs || !max_parallel_ok {
                let status = if expandable && !has_needs {
                    ExecutionStatus::Pending
                } else {
                    ExecutionStatus::Queued
                };
                (status, QueueState::Blocked)
            } else {
                (ExecutionStatus::Queued, QueueState::None)
            };
            // `reset_job_row`: classification fields get the fresh values,
            // every per-attempt stamp clears.
            if let Some(node) = sweep.node_mut(run_id, &job_id) {
                node.status = status;
                node.queue_state = queue_state;
                node.remaining_needs = needs_len as i32;
                node.outputs = None;
                node.annotations = None;
                node.check_run_id = None;
                node.enqueued_at_us = None;
                node.claimed_by_runner_id = None;
                node.claimed_at_us = None;
                node.deps_ready_at_us = None;
                node.concurrency_wait_at_us = None;
                node.concurrency_acquired_at_us = None;
                node.started_at_us = None;
                node.completed_at_us = None;
                node.environment_gate = gate;
            }
            sweep.mark(run_id, &job_id);
            note_reset(&mut sweep, run_id, &job_id, status);
            raw_status.insert(job_id.clone(), status_str(status).to_owned());

            // Message template: a handler-rebuilt template wins outright (the
            // job had no stored one); otherwise patch the stored template's
            // attempt identity. A job with neither cannot mint an attempt.
            if let Some(template) = templates.get(job_id.0.as_str()) {
                insert_job_message(
                    &tx,
                    run_id,
                    &job_id,
                    &template.message,
                    &template.condition_context,
                )
                .await?;
            } else if let Some(mut message) = stored_job_message(&tx, run_id, &job_id).await? {
                patch_message_attempt(&mut message, run_attempt)?;
                PgBackend::write_node_message(&tx, run_id, &job_id, &message).await?;
                // The stored `if:` context is the previous attempt's; refresh
                // the attempt stamp inside it so the promotion sweep sees the
                // same context the runner will.
                patch_condition_context(&tx, run_id, &job_id, run_attempt).await?;
            } else if expandable {
                // An expandable node re-mints its runnable messages at
                // expansion; a `pending`/`blocked` placeholder is correct here.
            } else {
                // No template and no rebuild: minting an attempt row would name
                // a message that cannot dispatch. Fail the job like a build
                // error rather than stranding it ready.
                tracing::warn!(
                    %run_id,
                    job = %job_id.0,
                    "re-run job has no stored message and no rebuilt template; failing"
                );
                reset_concluded(&mut sweep, run_id, &job_id, ExecutionStatus::Failure);
                concluded.push((
                    job_id,
                    ExecutionStatus::Failure,
                    Some("job has no stored job message to re-run".to_owned()),
                ));
                continue;
            }

            // The ready path: job-concurrency gate, then enqueue — submit's
            // order. `blocked`/`held` jobs mint immediately so the attempt
            // exists for whatever path promotes them.
            if status == ExecutionStatus::Queued && queue_state == QueueState::None {
                match sweep.job_gate(run_id, &job_id).await? {
                    GateOutcome::Acquired => {
                        sweep.enqueue(run_id, &job_id).await?;
                        *active_by_base.entry(base_id.clone()).or_default() += 1;
                    }
                    GateOutcome::Parked => {
                        sweep.hold_node(run_id, &job_id);
                        raw_status.insert(
                            job_id.clone(),
                            status_str(ExecutionStatus::Pending).to_owned(),
                        );
                    }
                    // `ArrivalCancelled` inside the job gate is a terminal
                    // cancel; a failed evaluation is a failure — pg's
                    // `Sweep::promote` settles the same two ways.
                    GateOutcome::Cancelled => {
                        reset_concluded(&mut sweep, run_id, &job_id, ExecutionStatus::Cancelled);
                        concluded.push((job_id.clone(), ExecutionStatus::Cancelled, None));
                    }
                    GateOutcome::Failed => {
                        reset_concluded(&mut sweep, run_id, &job_id, ExecutionStatus::Failure);
                        concluded.push((job_id.clone(), ExecutionStatus::Failure, None));
                    }
                }
            }

            // Mint the attempt: a fresh `jobId`/request for dispatchable nodes.
            if expandable {
                rerun_jobs.push((job_id, None));
            } else {
                let agent_job_id = {
                    let graph = sweep.graphs.get(&run_id).expect("inserted above");
                    mint_attempt(
                        &tx,
                        graph,
                        run_id,
                        &job_id,
                        templates.get(job_id.0.as_str()).map(|t| &t.token_request),
                    )
                    .await?
                };
                rerun_jobs.push((job_id, Some(agent_job_id.to_string())));
            }
            queued += 1;
        }

        // Dependents of the reset jobs re-count: an edge lands on a live
        // (non-terminal) row only once the attempt resets it.
        refresh_run_remaining_needs(&mut sweep, run_id, &raw_status);

        // Promotion sweep: needs-gated members with satisfied ancestors
        // (carried-forward successes) advance through their `if:` + gates.
        sweep.sweep().await?;
        let sweep_concluded: Vec<(JobId, ExecutionStatus)> = sweep
            .outcome
            .skipped
            .iter()
            .chain(sweep.outcome.failed.iter())
            .filter(|(settled_run, _)| *settled_run == run_id)
            .filter_map(|(_, settled)| {
                sweep
                    .graphs
                    .get(&run_id)
                    .and_then(|graph| graph.nodes.get(settled))
                    .map(|node| (settled.clone(), node.status))
            })
            .collect();
        for (job_id, status) in sweep_concluded {
            concluded.push((job_id, status, None));
        }

        // ── Run status ─────────────────────────────────────────────────────
        let terminal = {
            let graph = sweep.graphs.get_mut(&run_id).expect("inserted above");
            graph.resummarize();
            if graph.record.status.is_terminal() {
                graph.record.started_at.get_or_insert_with(chrono::Utc::now);
                graph.touched = true;
                true
            } else {
                false
            }
        };
        if terminal {
            // The workflow gate this attempt took must not outlive the run.
            release_concurrency_for_run(self, &tx, run_id).await?;
            tx.execute(
                "UPDATE runs SET concurrency_group = NULL WHERE run_id = $1::text::uuid",
                &[&run],
            )
            .await
            .map_err(db)?;
            held = false;
        }
        let status = if terminal {
            sweep
                .graphs
                .get(&run_id)
                .expect("inserted above")
                .record
                .status
        } else if held {
            ExecutionStatus::Pending
        } else {
            ExecutionStatus::Queued
        };

        sweep.flush().await?;
        emit_outbox(
            &tx,
            Some(run_id),
            "run.rerun.v1",
            serde_json::json!({
                "status": status_str(status),
                "run_attempt": run_attempt,
                "mode": mode_name(&mode),
                "jobs": set.len(),
            }),
        )
        .await?;
        tx.commit().await.map_err(db)?;
        drop(client);

        Ok(RerunOutcome {
            run_id,
            run_attempt: run_attempt as u64,
            run_number: run_number as u64,
            queued_jobs: queued,
            status,
            concluded,
            rerun_jobs,
            selected: set.clone(),
            held,
            next_runs_on: self.ready_front_labels().await?,
        })
    }
}

/// `ArrivalCancelled` inside the workflow gate: every selected job lands
/// `cancelled` (no mint — GitHub does not create a second attempt for a
/// re-run the queue rejected), the run re-completes, and the event pair
/// mirrors submit's arrival-cancelled emissions. The caller commits (it owns
/// the transaction) and returns the outcome.
async fn arrival_cancelled(
    tx: &Transaction<'_>,
    run: &str,
    run_id: RunId,
    set: &BTreeSet<JobId>,
    rows: &[RerunRow],
    run_attempt: i64,
) -> Result<Vec<(JobId, ExecutionStatus, Option<String>)>, ControlError> {
    for member in rows {
        if set.contains(&member.job_id) {
            reset_concluded_sql(tx, run, &member.job_id, ExecutionStatus::Cancelled).await?;
        }
    }
    tx.execute(
        "UPDATE runs SET status = 'completed', conclusion = 'cancelled', \
             completed_at = now(), started_at = COALESCE(started_at, now()) \
         WHERE run_id = $1::text::uuid",
        &[&run],
    )
    .await
    .map_err(db)?;
    let concluded: Vec<(JobId, ExecutionStatus, Option<String>)> = rows
        .iter()
        .filter(|member| set.contains(&member.job_id))
        .map(|member| {
            (
                member.job_id.clone(),
                ExecutionStatus::Cancelled,
                concurrency::cancelled_reason(),
            )
        })
        .collect();
    emit_outbox(
        tx,
        Some(run_id),
        "run.rerun.v1",
        serde_json::json!({
            "status": "cancelled",
            "run_attempt": run_attempt,
            "jobs": set.len(),
        }),
    )
    .await?;
    emit_outbox(
        tx,
        Some(run_id),
        "run.completed.v1",
        serde_json::json!({"conclusion": "cancelled"}),
    )
    .await?;
    Ok(concluded)
}

/// Copy the current attempt's `runs`/`jobs` rows into the history tables
/// under the previous `run_attempt`. `job_requests`/`job_steps`/timelines
/// need no copy — they accumulate per attempt and the archive move at
/// retention carries them all.
async fn snapshot_attempt(
    tx: &Transaction<'_>,
    run: &str,
    prev_attempt: i64,
) -> Result<(), ControlError> {
    tx.execute(
        "INSERT INTO run_history (run_id, namespace_id, repository, workflow_path, \
             run_number, run_attempt, run_name, event, ref, ref_type, head_ref, \
             base_ref, head_sha, conclusion, submission, record_details, \
             fork_approval_pending, fork_approval_requested_at, \
             fork_approval_approved_at, fork_approval_note, reports_check_runs, \
             created_at, started_at, completed_at) \
         SELECT r.run_id, r.namespace_id, r.repository, r.workflow_path, \
             r.run_number, r.run_attempt, r.run_name, r.event, r.ref, r.ref_type, \
             r.head_ref, r.base_ref, r.head_sha, r.conclusion, \
             COALESCE(s.submission, '{}'::jsonb), COALESCE(s.record_details, '{}'::jsonb), \
             r.fork_approval_pending, r.fork_approval_requested_at, \
             r.fork_approval_approved_at, r.fork_approval_note, r.reports_check_runs, \
             r.created_at, r.started_at, r.completed_at \
         FROM runs r LEFT JOIN run_submissions s ON s.run_id = r.run_id \
         WHERE r.run_id = $1::text::uuid",
        &[&run],
    )
    .await
    .map_err(db)?;
    tx.execute(
        "INSERT INTO job_history (run_id, run_created_at, run_attempt, job_id, \
             namespace_id, kind, parent_job_id, base_id, display_name, status, \
             pool_key, outputs, annotations, check_run_id, created_at, \
             deps_ready_at, started_at, completed_at) \
         SELECT j.run_id, r.created_at, $2, j.job_id, j.namespace_id, j.kind, \
             j.parent_job_id, j.base_id, COALESCE(s.display_name, j.job_id), \
             j.status, j.pool_key, j.outputs, j.annotations, j.check_run_id, \
             j.created_at, j.deps_ready_at, j.started_at, j.completed_at \
         FROM jobs j JOIN runs r ON r.run_id = j.run_id \
         LEFT JOIN job_specs s ON s.run_id = j.run_id AND s.job_id = j.job_id \
         WHERE j.run_id = $1::text::uuid",
        &[&run, &(prev_attempt as i32)],
    )
    .await
    .map_err(db)?;
    Ok(())
}

/// Rewrite `github.run_attempt` in the stored `run_submissions.github_context`.
async fn bump_run_attempt(
    tx: &Transaction<'_>,
    run: &str,
    run_attempt: i64,
) -> Result<serde_json::Value, ControlError> {
    let json: Option<String> = tx
        .query_opt(
            "SELECT github_context::text FROM run_submissions WHERE run_id = $1::text::uuid",
            &[&run],
        )
        .await
        .map_err(db)?
        .map(|row| row.get(0));
    let mut github: serde_json::Value = json
        .and_then(|stored| serde_json::from_str(&stored).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    if let Some(object) = github.as_object_mut() {
        object.insert("run_attempt".to_owned(), serde_json::json!(run_attempt));
    }
    tx.execute(
        "UPDATE run_submissions SET github_context = $2::text::jsonb \
         WHERE run_id = $1::text::uuid",
        &[&run, &github.to_string()],
    )
    .await
    .map_err(db)?;
    Ok(github)
}

/// Remove the per-attempt check-run mapping for selected jobs: the new
/// attempt mints fresh check runs (`jobs.check_run_id` is cleared by the node
/// reset; `record_details.job_check_run_ids` is the pre-creation mapping the
/// GitHub reporter reads).
async fn clear_recorded_check_runs(
    tx: &Transaction<'_>,
    run: &str,
    set: &BTreeSet<JobId>,
) -> Result<(), ControlError> {
    let details: Option<String> = tx
        .query_opt(
            "SELECT record_details::text FROM run_submissions WHERE run_id = $1::text::uuid",
            &[&run],
        )
        .await
        .map_err(db)?
        .map(|row| row.get(0));
    let Some(details) = details else {
        return Ok(());
    };
    let mut json: serde_json::Value = serde_json::from_str(&details).map_err(|error| {
        ControlError::backend(anyhow::anyhow!("record_details decode: {error}"))
    })?;
    if let Some(map) = json
        .get_mut("job_check_run_ids")
        .and_then(|value| value.as_object_mut())
    {
        for job_id in set {
            map.remove(&job_id.0);
        }
    }
    tx.execute(
        "UPDATE run_submissions SET record_details = $2::text::jsonb \
         WHERE run_id = $1::text::uuid",
        &[&run, &json.to_string()],
    )
    .await
    .map_err(db)?;
    Ok(())
}

/// Drop wait/hold rows a completed run should never own, and per-job waits for
/// selected members (their gates re-acquire below). Jobset rows that named a
/// consumed caller expansion are stale for the new attempt.
async fn clear_stale_concurrency(
    tx: &Transaction<'_>,
    run: &str,
    set: &BTreeSet<JobId>,
) -> Result<(), ControlError> {
    // Run-scoped leftovers (defensive — `release_concurrency_for_run` owns the
    // completed-run invariant).
    tx.execute(
        "DELETE FROM concurrency_waits WHERE holder_run_id = $1::text::uuid",
        &[&run],
    )
    .await
    .map_err(db)?;
    // Job-keyed waits of selected members.
    for member in set {
        tx.execute(
            "DELETE FROM concurrency_waits WHERE holder_run_id = $1::text::uuid \
             AND holder_kind = 'job' AND holder_job_id = $2",
            &[&run, &member.0],
        )
        .await
        .map_err(db)?;
    }
    // Jobsets minted for caller expansions are consumed state: the member rows
    // persist (FK-safe), the gate rows do not re-arm.
    tx.execute("DELETE FROM jobsets WHERE run_id = $1::text::uuid", &[&run])
        .await
        .map_err(db)?;
    Ok(())
}

/// Close any unclaimed request left by the completed attempt before the new
/// attempt mints its request. Normally terminal settlement already wrote the
/// result; blocked jobs that were concluded by a dependency sweep can still
/// retain a NULL result, and the partial unique index permits only one such
/// row per logical job.
async fn settle_stale_requests(
    tx: &Transaction<'_>,
    run: &str,
    rows: &[RerunRow],
    set: &BTreeSet<JobId>,
) -> Result<(), ControlError> {
    for member in rows {
        if !set.contains(&member.job_id) {
            continue;
        }
        let result = match member.status_raw.as_str() {
            "success" | "failure" | "cancelled" | "skipped" | "timed_out" => {
                member.status_raw.as_str()
            }
            _ => "cancelled",
        };
        tx.execute(
            "UPDATE job_requests SET result = $3, finished_at = COALESCE(finished_at, now()) \
             WHERE run_id = $1::text::uuid AND job_id = $2 AND result IS NULL",
            &[&run, &member.job_id.0, &result],
        )
        .await
        .map_err(db)?;
    }
    Ok(())
}

/// Land a selected job terminal on its `jobs` row alone — the arrival-cancel
/// path, which runs before the working set exists (lite's `reset_concluded`).
async fn reset_concluded_sql(
    tx: &Transaction<'_>,
    run: &str,
    job_id: &JobId,
    status: ExecutionStatus,
) -> Result<(), ControlError> {
    tx.execute(
        "UPDATE jobs SET status = $3, queue_state = 'none', remaining_needs = 0, \
             outputs = NULL, annotations = NULL, check_run_id = NULL, \
             enqueued_at = NULL, claimed_by_runner_id = NULL, claimed_at = NULL, \
             deps_ready_at = NULL, concurrency_wait_at = NULL, \
             concurrency_acquired_at = NULL, started_at = NULL, \
             completed_at = now(), environment_gate = NULL \
         WHERE run_id = $1::text::uuid AND job_id = $2",
        &[&run, &job_id.0, &status_str(status)],
    )
    .await
    .map_err(db)?;
    Ok(())
}

/// Land a job terminal without a message or mint (submit's `insert` +
/// `concluded` shape for skipped/unhostable jobs).
fn reset_concluded(sweep: &mut Sweep<'_>, run_id: RunId, job_id: &JobId, status: ExecutionStatus) {
    let now = now_us();
    if let Some(node) = sweep.node_mut(run_id, job_id) {
        node.status = status;
        node.queue_state = QueueState::None;
        node.remaining_needs = 0;
        node.outputs = None;
        node.annotations = None;
        node.check_run_id = None;
        node.enqueued_at_us = None;
        node.claimed_by_runner_id = None;
        node.claimed_at_us = None;
        node.deps_ready_at_us = None;
        node.concurrency_wait_at_us = None;
        node.concurrency_acquired_at_us = None;
        node.started_at_us = None;
        node.completed_at_us = Some(now);
        node.environment_gate = None;
    }
    sweep.mark(run_id, job_id);
    note_reset(sweep, run_id, job_id, status);
}

/// Mirror a reset node into the record projections the sweep and dependent
/// hydration read (`jobs` statuses, `job_outputs`, `job_check_run_ids`).
fn note_reset(sweep: &mut Sweep<'_>, run_id: RunId, job_id: &JobId, status: ExecutionStatus) {
    if let Some(graph) = sweep.graphs.get_mut(&run_id) {
        if graph
            .nodes
            .get(job_id)
            .is_some_and(|node| node.contributes())
        {
            graph.record.jobs.insert(job_id.clone(), status);
        }
        graph.record.job_outputs.remove(job_id);
        graph.record.job_check_run_ids.remove(job_id);
    }
}

/// Whole-run recompute of `remaining_needs` over the working set — lite's
/// `refresh_run_remaining_needs` SQL (a declared need matches a job id or a
/// base id; an expanded matrix parent is replaced by its legs), applied in
/// memory so the promotion sweep sees the counters the SQL would have written.
fn refresh_run_remaining_needs(
    sweep: &mut Sweep<'_>,
    run_id: RunId,
    raw_status: &BTreeMap<JobId, String>,
) {
    let mut next: Vec<(JobId, i32)> = Vec::new();
    if let Some(graph) = sweep.graphs.get(&run_id) {
        for (job_id, node) in &graph.nodes {
            if node.queue_state == QueueState::None {
                continue;
            }
            let remaining: i32 = node
                .needs
                .iter()
                .filter(|need| {
                    graph.nodes.iter().any(|(id, dependency)| {
                        (id == *need || dependency.base_id == need.0)
                            && !(dependency.kind == NodeKind::MatrixParent
                                && dependency.has_children)
                            && !raw_status
                                .get(id)
                                .is_some_and(|status| raw_terminal(status))
                    })
                })
                .count() as i32;
            if remaining != node.remaining_needs {
                next.push((job_id.clone(), remaining));
            }
        }
    }
    for (job_id, remaining) in next {
        if let Some(node) = sweep.node_mut(run_id, &job_id) {
            node.remaining_needs = remaining;
        }
        sweep.mark(run_id, &job_id);
    }
}

/// `jobs.status` values that never keep a declared need open (the SQLite
/// backend's `d.status NOT IN ('success','failure','cancelled','skipped')`).
fn raw_terminal(status: &str) -> bool {
    matches!(status, "success" | "failure" | "cancelled" | "skipped")
}

/// Re-key a stored message template for the new attempt: new `jobId`
/// (== `planId` — preloop derives plan ids from the attempt), fresh
/// `requestId` (the mint stamps the real one), fresh `timeline.id`, the
/// bumped `github.run_attempt`, a cleared `job.check_run_id`, and a
/// re-prefixed `system.orchestrationId`.
fn patch_message_attempt(
    message: &mut azdo::AgentJobRequestMessage,
    run_attempt: i64,
) -> Result<uuid::Uuid, ControlError> {
    let agent_job_id = uuid::Uuid::new_v4();
    message.job_id = agent_job_id;
    message.plan.plan_id = agent_job_id.to_string();
    message.request_id = 0;
    message.timeline.id = uuid::Uuid::new_v4();
    if let Some(azdo::PipelineContextData::Dict(github)) = message.context_data.get_mut("github") {
        github.insert(
            "run_attempt".to_owned(),
            azdo::PipelineContextData::Number(run_attempt as f64),
        );
    }
    if let Some(azdo::PipelineContextData::Dict(job)) = message.context_data.get_mut("job") {
        job.insert(
            "check_run_id".to_owned(),
            azdo::PipelineContextData::Number(0.0),
        );
    }
    // `system.orchestrationId` embeds the plan id (`{plan}.{job}.{__default|
    // _N}`): re-prefix it on the new attempt id, keeping the job suffix.
    if let Some(value) = message.variables.get_mut("system.orchestrationId")
        && let Some(old) = value.value.as_deref()
    {
        let suffix = old.split_once('.').map(|(_, tail)| tail).unwrap_or("");
        value.value = Some(format!("{agent_job_id}.{suffix}"));
    }
    Ok(agent_job_id)
}

/// Point the stored `if:`/`needs:` context at the new attempt (the
/// `github.run_attempt` inside it would otherwise keep reporting the old one).
async fn patch_condition_context(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    run_attempt: i64,
) -> Result<(), ControlError> {
    let Some(row) = tx
        .query_opt(
            "SELECT condition_context::text FROM job_messages \
             WHERE run_id = $1::text::uuid AND job_id = $2",
            &[&run_id.0.to_string(), &job_id.0],
        )
        .await
        .map_err(db)?
    else {
        return Ok(());
    };
    let mut context: preloop_gha_expressions::Context =
        serde_json::from_str(&row.get::<_, String>(0)).unwrap_or_default();
    context.merge_root("github", serde_json::json!({"run_attempt": run_attempt}));
    tx.execute(
        "UPDATE job_messages SET condition_context = $3::text::jsonb \
         WHERE run_id = $1::text::uuid AND job_id = $2",
        &[
            &run_id.0.to_string(),
            &job_id.0,
            &serde_json::to_string(&context).map_err(|error| {
                ControlError::backend(anyhow::anyhow!("context encode: {error}"))
            })?,
        ],
    )
    .await
    .map_err(db)?;
    Ok(())
}

/// Persist a handler-rebuilt message template + `if:` context (lite's
/// `insert_job_message`: a fresh template wins outright, and the previous
/// attempt's `secret_names` are kept — only the template and context move).
async fn insert_job_message(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    message: &azdo::AgentJobRequestMessage,
    condition_context: &preloop_gha_expressions::Context,
) -> Result<(), ControlError> {
    tx.execute(
        "INSERT INTO job_messages (run_id, job_id, message_template, \
             secret_names, condition_context) \
         VALUES ($1::text::uuid,$2,$3::text::jsonb,'[]'::jsonb,$4::text::jsonb) \
         ON CONFLICT (run_id, job_id) DO UPDATE SET \
             message_template = EXCLUDED.message_template, \
             condition_context = EXCLUDED.condition_context",
        &[
            &run_id.0.to_string(),
            &job_id.0,
            &serde_json::to_string(message).map_err(|error| {
                ControlError::backend(anyhow::anyhow!("job message encode: {error}"))
            })?,
            &serde_json::to_string(condition_context).map_err(|error| {
                ControlError::backend(anyhow::anyhow!("context encode: {error}"))
            })?,
        ],
    )
    .await
    .map_err(db)?;
    Ok(())
}

/// The job's stored runner message, or `None` when the job has no row or its
/// row is not a runnable message (lite's `stored_job_message`).
async fn stored_job_message(
    client: &impl GenericClient,
    run_id: RunId,
    job_id: &JobId,
) -> Result<Option<azdo::AgentJobRequestMessage>, ControlError> {
    let row = client
        .query_opt(
            "SELECT message_template::text FROM job_messages \
             WHERE run_id = $1::text::uuid AND job_id = $2",
            &[&run_id.0.to_string(), &job_id.0],
        )
        .await
        .map_err(db)?;
    Ok(row.and_then(|row| serde_json::from_str(&row.get::<_, String>(0)).ok()))
}

/// Mint one attempt row for a reset job: fresh `jobId`/`timeline` in the
/// stored template (already rewritten), the `job_requests` correlation, and
/// the deferred token-mint recipe copied forward.
async fn mint_attempt(
    tx: &Transaction<'_>,
    graph: &RunGraph,
    run_id: RunId,
    job_id: &JobId,
    token_request_override: Option<&Option<GitHubTokenRequest>>,
) -> Result<uuid::Uuid, ControlError> {
    // Read the agent_job_id the template patch (or rebuild) just wrote.
    let mut message = stored_job_message(tx, run_id, job_id)
        .await?
        .ok_or_else(|| {
            ControlError::backend(anyhow::anyhow!(
                "re-run job {job_id} has no message to mint"
            ))
        })?;
    let agent_job_id = message.job_id;
    let timeline_id = message.timeline.id;

    // The token-mint recipe is keyed by request id: carry the previous
    // attempt's forward unless a rebuilt template supplies its own.
    let token_request: Option<GitHubTokenRequest> = match token_request_override {
        Some(over) => over.clone(),
        None => tx
            .query_opt(
                "SELECT repository, permissions::text, declared, untrusted \
                 FROM github_token_requests WHERE request_id = (\
                     SELECT request_id FROM job_requests \
                     WHERE run_id = $1::text::uuid AND job_id = $2 \
                     ORDER BY request_id DESC LIMIT 1)",
                &[&run_id.0.to_string(), &job_id.0],
            )
            .await
            .map_err(db)?
            .map(|row| {
                Ok(GitHubTokenRequest {
                    repository: row.get(0),
                    permissions: codec::from_json(&row.get::<_, String>(1))?,
                    declared: row.get(2),
                    untrusted: row.get(3),
                })
            })
            .transpose()?,
    };

    let manifest = StepRecord::manifest(&message.steps);
    let request = TaskAgentJobRequestRecord {
        request_id: 0,
        run_id,
        job_id: job_id.clone(),
        agent_job_id,
        plan_id: agent_job_id.to_string(),
        plan_type: PLAN_TYPE.to_owned(),
        timeline_id,
        result: None,
        locked_until: crate::distributed_task::agent_request_locked_until(),
        claimed_at: None,
        owner_runner_id: None,
        started_at: None,
        last_renewed_at: None,
        timeout_triggered: false,
        debug_token_issued: false,
    };
    let request_id =
        insert_request_row(tx, graph, &request, token_request.as_ref(), &manifest).await?;
    // The runner-facing stored message carries the allocated request id
    // (`submit_run` stamps it the same way).
    message.request_id = request_id;
    PgBackend::write_node_message(tx, run_id, job_id, &message).await?;
    Ok(agent_job_id)
}

fn mode_name(mode: &RerunMode) -> &'static str {
    match mode {
        RerunMode::All => "all",
        RerunMode::Failed => "failed",
        RerunMode::Job(_) => "job",
    }
}
