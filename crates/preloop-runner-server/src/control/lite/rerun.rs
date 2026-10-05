//! `rerun_run` on SQLite: re-execute a completed run as a new attempt,
//! mirroring github.com's "Re-run jobs" behavior
//! (<https://docs.github.com/en/actions/how-tos/manage-workflow-runs/re-run-workflows-and-jobs>).
//!
//! One write transaction. The attempt boundary is the `jobs`/`runs` reset:
//! `job_requests`, `job_steps`, timelines and logs accumulate across attempts
//! untouched (they are the attempt archive), while the single live `jobs`
//! row per logical job is snapshotted into `job_history` before it is
//! reset — `run_history`/`job_history` keyed by `(run_id, run_attempt)`
//! reproduce GitHub's per-attempt views.
//!
//! The classification loop mirrors `submit_run_tx` step for step: workflow
//! concurrency is re-acquired first, then each selected job re-runs the
//! `if:`/platform/environment/concurrency/max-parallel admission chain and
//! mints a fresh `job_requests` attempt whose message template carries the
//! bumped `github.run_attempt` and a new `jobId`. Jobs outside the
//! selection keep their rows — status and outputs both — so dependents see
//! the carried-forward `needs` context.

use super::codec::{self, now_us};
use super::concurrency as cg;
use super::jobs::{self, JobRow};
use super::{LiteBackend, db, promote, settle, submit};
use crate::concurrency::{self, Holder};
use crate::control::logic;
use crate::control::types::*;
use crate::models::TaskAgentJobRequestRecord;
use crate::runtime_scheduling;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId, azdo};
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::{BTreeMap, BTreeSet};

/// GitHub refuses a re-run more than 30 days after the run's creation and
/// caps attempts at ~50 (the request layer rejects `#51`).
const RERUN_WINDOW_US: i64 = 30 * 24 * 60 * 60 * 1_000_000;
const MAX_RUN_ATTEMPT: i64 = 51;

/// One `jobs` row decoded for the selector: status arrives as the raw
/// stored string so `timed_out` reaches [`logic::rerun_set`] (the
/// `JobRow.status` field already parsed it — the raw column is re-read
/// alongside).
struct RerunRow {
    row: JobRow,
    /// Raw `jobs.status` (pre-`status_parse`: keeps `timed_out`).
    status_raw: String,
    needs: Vec<JobId>,
}

/// The `runs` row fields `rerun_run_tx` reads before resetting the attempt.
struct RunHead {
    prev_attempt: i64,
    run_number: i64,
    namespace: String,
    repository: String,
    git_ref: String,
    run_status: String,
    prev_group: String,
    prev_cancel_in_progress: i64,
    created_at_us: i64,
}

impl LiteBackend {
    /// `rerun_run` (pg dispatch.rs): snapshot the completed attempt into the
    /// history tables, reset the selected jobs, and re-admit them through
    /// the submit classification chain. `NotFound`/`Conflict` per the trait
    /// contract.
    pub(crate) async fn rerun_run(&self, rerun: RerunRun) -> Result<RerunOutcome, ControlError> {
        self.write(move |tx| rerun_run_tx(tx, self, rerun))
    }

    /// `rerun_plan` (pg dispatch.rs): the mode-resolved selection and which
    /// members lack a runnable `job_messages` template. Read-only; mirrors
    /// the write path's selection so templates are rebuilt for exactly the
    /// jobs that need them.
    pub(crate) async fn rerun_plan(
        &self,
        run_id: RunId,
        mode: &RerunMode,
    ) -> Result<RerunPlan, ControlError> {
        let mode = mode.clone();
        self.read(move |tx| {
            let run = codec::run_key(run_id);
            let exists: bool = tx
                .prepare_cached("SELECT EXISTS(SELECT 1 FROM runs WHERE run_id = ?1)")
                .map_err(db)?
                .query_row([&run], |row| row.get(0))
                .map_err(db)?;
            if !exists {
                return Err(ControlError::NotFound(format!("run {run_id}")));
            }
            let rows = load_rerun_rows(tx, run_id, &run)?;
            let set = logic::rerun_set(&mode, &selector_rows_of(&rows))?;
            let mut missing = Vec::new();
            for row in &rows {
                if !set.contains(&row.row.job_id) {
                    continue;
                }
                // Expandable nodes mint runnable messages at expansion; a
                // placeholder `{}` row or no row is not a gap for them.
                let expandable = jobs::load_spec(tx, run_id, &row.row.job_id)?
                    .is_some_and(|s| s.reusable_call.is_some() || s.deferred_matrix.is_some());
                if expandable {
                    continue;
                }
                if jobs::stored_job_message(tx, run_id, &row.row.job_id)?.is_none() {
                    missing.push(row.row.job_id.clone());
                }
            }
            Ok(RerunPlan {
                set,
                missing_templates: missing,
            })
        })
    }
}

/// Every `jobs` row of the run as `(row, raw status, needs)`.
fn load_rerun_rows(
    tx: &Transaction<'_>,
    run_id: RunId,
    run: &str,
) -> Result<Vec<RerunRow>, ControlError> {
    let mut stmt = tx
        .prepare_cached(&format!(
            "SELECT {}, j.status FROM jobs j WHERE j.run_id = ?1 ORDER BY j.job_order",
            jobs::JOB_COLUMNS
        ))
        .map_err(db)?;
    let rows = stmt
        .query_map([run], |row| {
            Ok((
                jobs::job_row(row)?,
                row.get::<_, String>(row.as_ref().column_count() - 1)?,
            ))
        })
        .map_err(db)?;
    let mut out = Vec::new();
    for row in rows {
        let (job, status_raw) = row.map_err(db)?;
        let needs = jobs::job_needs(tx, run_id, &job.job_id)?;
        out.push(RerunRow {
            row: job,
            status_raw,
            needs,
        });
    }
    Ok(out)
}

/// Selector projection of `load_rerun_rows` for `logic::rerun_set`.
fn selector_rows_of(rows: &[RerunRow]) -> Vec<logic::RerunJobRow> {
    rows.iter()
        .map(|r| logic::RerunJobRow {
            job_id: r.row.job_id.clone(),
            kind: r.row.kind.clone(),
            base_id: r.row.base_id.clone(),
            parent_job_id: r.row.parent_job_id.clone(),
            status: r.status_raw.clone(),
            needs: r.needs.clone(),
        })
        .collect()
}

fn rerun_run_tx(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    rerun: RerunRun,
) -> Result<RerunOutcome, ControlError> {
    let RerunRun {
        run_id,
        mode,
        workflow_concurrency,
        environment_rules,
        templates,
    } = rerun;
    let run = codec::run_key(run_id);

    // ── The run row: identity, clocks, guards ──────────────────────────
    let head: Option<RunHead> = tx
        .prepare_cached(
            "SELECT run_attempt, run_number, namespace_id, repository, ref, \
                    status, concurrency_group, \
                    COALESCE(concurrency_cancel_in_progress, 0), created_at \
             FROM runs WHERE run_id = ?1",
        )
        .map_err(db)?
        .query_row([&run], |row| {
            Ok(RunHead {
                prev_attempt: row.get(0)?,
                run_number: row.get(1)?,
                namespace: row.get(2)?,
                repository: row.get(3)?,
                git_ref: row.get(4)?,
                run_status: row.get(5)?,
                prev_group: row.get::<_, Option<String>>(6)?.unwrap_or_default(),
                // `concurrency_cancel_in_progress` is informational here:
                // the re-acquired gate below rewrites the columns.
                prev_cancel_in_progress: row.get(7)?,
                created_at_us: row.get(8)?,
            })
        })
        .optional()
        .map_err(db)?;
    let Some(RunHead {
        prev_attempt,
        run_number,
        namespace,
        repository,
        git_ref,
        run_status,
        prev_group,
        prev_cancel_in_progress: _,
        created_at_us,
    }) = head
    else {
        return Err(ControlError::NotFound(format!("run {run_id}")));
    };
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
    let rows: Vec<RerunRow> = load_rerun_rows(tx, run_id, &run)?;
    if rows.is_empty() {
        return Err(ControlError::backend(anyhow::anyhow!(
            "run {run_id} has no jobs rows to re-run"
        )));
    }
    let set: BTreeSet<JobId> = logic::rerun_set(&mode, &selector_rows_of(&rows))?;

    // ── Snapshot the completed attempt ─────────────────────────────────
    // `run_history`/`job_history` key the snapshot on the *old* attempt;
    // the run row itself is reset next. `job_requests`/`job_steps` already
    // accumulate per attempt — no copy.
    snapshot_attempt(tx, &run, prev_attempt)?;

    // ── The new attempt on the run row ─────────────────────────────────
    // Clear the clocks a fresh run would not carry: GitHub's new attempt
    // has its own started/completed pair and no conclusion. The stored
    // concurrency group is re-derived below (the gate re-acquires it).
    tx.prepare_cached(
        "UPDATE runs SET run_attempt = ?2, status = 'queued', conclusion = NULL, \
             started_at = NULL, completed_at = NULL, \
             concurrency_group = NULL, concurrency_cancel_in_progress = 0 \
         WHERE run_id = ?1",
    )
    .map_err(db)?
    .execute(params![run, run_attempt])
    .map_err(db)?;

    // Bump `github.run_attempt` in the stored context: rebuilt templates
    // read it, and `run_context` serves it to gate evaluation.
    let github = bump_run_attempt(tx, &run, run_attempt)?;

    // Check-run ids are per-attempt: a re-run mints fresh `check_run`s and
    // the old ids stay on the attempt-1 history rows. Drop the mapping for
    // every selected job so the new attempt's `job.check_run_id` starts
    // empty (the message carries `0` until the reporter back-fills).
    clear_recorded_check_runs(tx, &run, &set)?;

    // A completed run holds no concurrency rows; a rerun of one still on
    // the books (e.g. a waiter the completion path left) is cleaned here
    // rather than trusted absent.
    clear_stale_concurrency(tx, &run, &set)?;

    // Close stale in-flight requests (a dependency-skipped or
    // arrival-cancelled job can leave a NULL-result row) before the new
    // attempt mints its own: `job_requests_inflight` refuses a second live
    // request for the same job.
    settle_stale_requests(tx, &run, &rows, &set)?;

    // The new attempt claims its own dispatch intents: pooled provisioning
    // and runner assignments mint again at enqueue.
    for member in &set {
        tx.prepare_cached("DELETE FROM job_assignments WHERE run_id = ?1 AND job_id = ?2")
            .map_err(db)?
            .execute(params![run, member.0])
            .map_err(db)?;
        tx.prepare_cached("DELETE FROM provision_requests WHERE run_id = ?1 AND job_id = ?2")
            .map_err(db)?
            .execute(params![run, member.0])
            .map_err(db)?;
    }

    // ── Workflow-level gate ────────────────────────────────────────────
    // Same shape as submit: acquire before classification, cancel the
    // selection on ArrivalCancelled, park it held on a busy group.
    let mut held = false;
    if let Some(wf) = &workflow_concurrency {
        let key = concurrency::concurrency_key(&repository, &wf.group);
        let holder = Holder::Run(run_id);
        let mut cancel = settle::canceler(backend);
        match cg::acquire(
            tx,
            &namespace,
            &key.0,
            &key.1,
            &wf.group,
            &holder,
            wf.cancel_in_progress,
            wf.queue,
            &mut cancel,
        )? {
            cg::AcqOutcome::Acquired => {}
            cg::AcqOutcome::Parked => held = true,
            cg::AcqOutcome::ArrivalCancelled => {
                return arrival_cancelled(
                    tx,
                    &run,
                    run_id,
                    &namespace,
                    &set,
                    &rows,
                    run_number,
                    run_attempt,
                );
            }
            cg::AcqOutcome::Failed => {
                return Err(ControlError::backend(anyhow::anyhow!(
                    "workflow concurrency admission failed"
                )));
            }
        }
        // The gate won is recorded on the run (submit stores the evaluated
        // group at insert; the rerun rewrites it after re-acquiring).
        tx.prepare_cached(
            "UPDATE runs SET concurrency_group = ?2, concurrency_cancel_in_progress = ?3 \
             WHERE run_id = ?1",
        )
        .map_err(db)?
        .execute(params![run, wf.group, wf.cancel_in_progress as i64])
        .map_err(db)?;
    }
    let _ = prev_group; // the stored group is informational; re-acquired above.

    // ── Per-job reset + classification ─────────────────────────────────
    // Mirrors `submit_run_tx`'s per-job chain: `if:` (needs-less jobs only —
    // needs-gated `if:`s re-evaluate in the promotion sweep), platform and
    // pool-label checks, environment gate, concurrency gate, max-parallel.
    let templates: BTreeMap<&str, &RerunJobTemplate> =
        templates.iter().map(|t| (t.job_id.0.as_str(), t)).collect();
    let mut concluded: Vec<(JobId, ExecutionStatus, Option<String>)> = Vec::new();
    let mut rerun_jobs: Vec<(JobId, Option<String>)> = Vec::new();
    let mut queued = 0usize;
    let platforms = jobs::registered_platforms(tx)?;
    let pool_labels = backend.pool_labels();
    let runner_labels = if pool_labels.is_empty() {
        Vec::new()
    } else {
        jobs::runner_label_sets(tx)?
    };
    let mut active_by_base: BTreeMap<String, u64> = BTreeMap::new();

    for member in &rows {
        if !set.contains(&member.row.job_id) {
            continue;
        }
        let job = &member.row;
        let job_id = job.job_id.clone();
        let spec = jobs::load_spec(tx, run_id, &job_id)?.unwrap_or_else(|| jobs::SpecRow {
            display_name: job_id.0.clone(),
            if_condition: None,
            matrix: BTreeMap::new(),
            deferred_matrix: None,
            max_parallel: None,
            environment: None,
            concurrency: None,
            reusable_call: None,
            reusable_meta: None,
            caller_plan: None,
            fail_fast: None,
            continue_on_error: None,
            id_token_granted: false,
            oidc_context: None,
        });
        let expandable = spec.reusable_call.is_some() || spec.deferred_matrix.is_some();
        let caller_with_children = spec.reusable_call.is_some()
            && rows
                .iter()
                .any(|r| r.row.parent_job_id.as_deref() == Some(job_id.0.as_str()));

        // A caller whose subtree re-runs returns to `in_progress`: its own
        // status is derived from the children, which
        // `finalize_reusable_callers` folds back when they settle.
        if caller_with_children {
            tx.prepare_cached(
                "UPDATE jobs SET status = 'in_progress', queue_state = 'none', \
                     outputs = NULL, completed_at = NULL \
                 WHERE run_id = ?1 AND job_id = ?2",
            )
            .map_err(db)?
            .execute(params![run, job_id.0])
            .map_err(db)?;
            rerun_jobs.push((job_id, None));
            continue;
        }

        // `if:` re-evaluates exactly where submit evaluated it: needs-less
        // jobs here, needs-gated jobs inside `promote_run` below.
        if member.needs.is_empty() {
            let ctx_json: String = tx
                .prepare_cached(
                    "SELECT condition_context FROM job_messages WHERE run_id = ?1 AND job_id = ?2",
                )
                .map_err(db)?
                .query_row(params![run, job_id.0], |row| row.get(0))
                .optional()
                .map_err(db)?
                .unwrap_or_else(|| "{}".to_owned());
            // A handler-supplied rebuild carries the fresh context; the
            // stored one is still the previous attempt's until the message
            // rewrite below, so patch `run_attempt` for evaluation parity.
            let mut context: preloop_gha_expressions::Context = templates
                .get(job_id.0.as_str())
                .map(|t| t.condition_context.clone())
                .unwrap_or_else(|| serde_json::from_str(&ctx_json).unwrap_or_default());
            context.insert("github", github.clone());
            let condition =
                preloop_gha_expressions::effective_condition(spec.if_condition.as_deref());
            match preloop_gha_expressions::eval_bool(&condition, &context) {
                Ok(true) => {}
                Ok(false) => {
                    reset_concluded(tx, &run, &job_id, ExecutionStatus::Skipped)?;
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
                    reset_concluded(tx, &run, &job_id, ExecutionStatus::Failure)?;
                    concluded.push((job_id, ExecutionStatus::Failure, Some(error.to_string())));
                    continue;
                }
            }
        }

        if let Some(platform) = logic::unhostable_platform(&job.runs_on, platforms.iter().copied())
        {
            reset_concluded(tx, &run, &job_id, ExecutionStatus::Failure)?;
            concluded.push((
                job_id,
                ExecutionStatus::Failure,
                Some(logic::unhostable_reason(platform, &job.runs_on)),
            ));
            continue;
        }

        if !expandable
            && let Some(reason) = logic::unschedulable_reason(
                &job.runs_on,
                &pool_labels,
                runner_labels.iter().any(|labels| {
                    crate::runtime_scheduling::job_matches_runner(&job.runs_on, labels)
                }),
            )
        {
            reset_concluded(tx, &run, &job_id, ExecutionStatus::Failure)?;
            concluded.push((job_id, ExecutionStatus::Failure, Some(reason)));
            continue;
        }

        // Environment protection re-evaluates exactly like submit: the old
        // gate state is discarded (`None`) and a wait/approval re-arms.
        let mut gate = None;
        let _env_outcome = runtime_scheduling::evaluate_environment_gate(
            &environment_rules,
            &repository,
            &git_ref,
            run_id,
            &job_id,
            spec.environment.as_ref(),
            &mut gate,
            crate::models::now_unix_nanos(),
        );
        // Submit parks a `Wait`/`Failed` verdict as held+pending; the
        // reaper's `promote_ready_jobs` sweep releases or fails it.
        let gate_json = gate
            .as_ref()
            .map(|g| serde_json::to_string(g).map_err(ControlError::backend))
            .transpose()?;

        // Classification, mirroring submit's ordering.
        let has_needs = !member.needs.is_empty();
        let max_parallel_ok = spec
            .max_parallel
            .is_none_or(|limit| active_by_base.get(&job.base_id).copied().unwrap_or(0) < limit);
        let (status, queue_state) = if held || gate.is_some() {
            (ExecutionStatus::Pending, "held")
        } else if expandable || has_needs || !max_parallel_ok {
            let status = if expandable && !has_needs {
                ExecutionStatus::Pending
            } else {
                ExecutionStatus::Queued
            };
            (status, "blocked")
        } else {
            (ExecutionStatus::Queued, "none")
        };
        reset_job_row(
            tx,
            &run,
            &job_id,
            status,
            queue_state,
            member.needs.len(),
            gate_json.as_deref(),
        )?;

        // Message template: a handler-rebuilt template wins outright (the
        // job had no stored one); otherwise patch the stored template's
        // attempt identity. A job with neither cannot mint an attempt.
        if let Some(template) = templates.get(job_id.0.as_str()) {
            jobs::insert_job_message(
                tx,
                run_id,
                &job_id,
                &template.message,
                &template.condition_context,
            )?;
        } else if let Some(mut message) = jobs::stored_job_message(tx, run_id, &job_id)? {
            patch_message_attempt(&mut message, run_attempt)?;
            jobs::update_job_message(tx, run_id, &job_id, &message)?;
            // The stored `if:` context is the previous attempt's; refresh the
            // attempt stamp inside it so the promotion sweep sees the same
            // context the runner will.
            patch_condition_context(tx, &run, &job_id, run_attempt)?;
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
            reset_concluded(tx, &run, &job_id, ExecutionStatus::Failure)?;
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
        if status == ExecutionStatus::Queued && queue_state == "none" {
            let row = jobs::job(tx, run_id, &job_id)?
                .ok_or_else(|| ControlError::backend(anyhow::anyhow!("job row vanished")))?;
            match promote::acquire_job_gate(tx, backend, run_id, &job_id)? {
                promote::GateOutcome::Proceed => {
                    promote::enqueue_ready_job(tx, backend, &row, spec.concurrency.is_some())?;
                    *active_by_base.entry(job.base_id.clone()).or_default() += 1;
                }
                promote::GateOutcome::Parked => {
                    tx.prepare_cached(
                        "UPDATE jobs SET queue_state = 'held', status = 'pending', \
                             concurrency_wait_at = COALESCE(concurrency_wait_at, ?3) \
                         WHERE run_id = ?1 AND job_id = ?2",
                    )
                    .map_err(db)?
                    .execute(params![run, job_id.0, now_us()])
                    .map_err(db)?;
                }
                promote::GateOutcome::Failed(status) => {
                    tx.prepare_cached(
                        "UPDATE jobs SET status = ?3, queue_state = 'none', completed_at = ?4 \
                         WHERE run_id = ?1 AND job_id = ?2",
                    )
                    .map_err(db)?
                    .execute(params![run, job_id.0, status_str(status), now_us()])
                    .map_err(db)?;
                    concluded.push((job_id.clone(), status, None));
                }
            }
        }

        // Mint the attempt: a fresh `jobId`/request for dispatchable nodes.
        if expandable {
            rerun_jobs.push((job_id, None));
        } else {
            let agent_job_id = mint_attempt(
                tx,
                &namespace,
                run_id,
                &job_id,
                templates.get(job_id.0.as_str()).map(|t| &t.token_request),
            )?;
            rerun_jobs.push((job_id, Some(agent_job_id.to_string())));
        }
        queued += 1;
    }

    // Dependents of the reset jobs re-count: an edge lands on a live
    // (non-terminal) row only once the attempt resets it.
    jobs::refresh_run_remaining_needs(tx, run_id)?;

    // Promotion sweep: needs-gated members with satisfied ancestors
    // (carried-forward successes) advance through their `if:` + gates.
    let mut outcome = crate::runtime_scheduling::SchedulingOutcome::default();
    promote::promote_run(tx, backend, run_id, &mut outcome)?;
    for (rid, jid) in outcome.skipped.iter().chain(outcome.failed.iter()) {
        if *rid == run_id
            && let Some(status) = jobs::job(tx, run_id, jid)?.map(|job| job.status)
        {
            concluded.push((jid.clone(), status, None));
        }
    }

    // ── Run status ─────────────────────────────────────────────────────
    let summary = jobs::summarize_run_row(tx, run_id)?;
    if summary.is_terminal() {
        // The workflow gate this attempt took must not outlive the run.
        settle::release_concurrency_for_run(tx, backend, run_id)?;
        tx.prepare_cached("UPDATE runs SET concurrency_group = NULL WHERE run_id = ?1")
            .map_err(db)?
            .execute([run.as_str()])
            .map_err(db)?;
        held = false;
    }
    let status = if summary.is_terminal() {
        summary
    } else if held {
        ExecutionStatus::Pending
    } else {
        ExecutionStatus::Queued
    };
    jobs::emit_outbox(
        tx,
        &namespace,
        Some(run_id),
        "run.rerun.v1",
        serde_json::json!({
            "status": status_str(status),
            "run_attempt": run_attempt,
            "mode": mode_name(&mode),
            "jobs": set.len(),
        }),
    )?;

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
        queue_depth: jobs::ready_count(tx)?,
        next_runs_on: jobs::next_ready_labels(tx)?,
    })
}

/// Copy the current attempt's `runs`/`jobs` rows into the history tables
/// under the previous `run_attempt`. `job_requests`/`job_steps`/timelines
/// need no copy — they accumulate per attempt and the archive move at
/// retention carries them all.
fn snapshot_attempt(
    tx: &Transaction<'_>,
    run: &str,
    prev_attempt: i64,
) -> Result<(), ControlError> {
    tx.prepare_cached(
        "INSERT INTO run_history (run_id, namespace_id, repository, workflow_path, \
             run_number, run_attempt, run_name, event, ref, ref_type, head_ref, \
             base_ref, head_sha, conclusion, submission, record_details, \
             fork_approval_pending, fork_approval_requested_at, \
             fork_approval_approved_at, fork_approval_note, reports_check_runs, \
             created_at, started_at, completed_at) \
         SELECT r.run_id, r.namespace_id, r.repository, r.workflow_path, \
             r.run_number, r.run_attempt, r.run_name, r.event, r.ref, r.ref_type, \
             r.head_ref, r.base_ref, r.head_sha, r.conclusion, \
             COALESCE(s.submission, '{}'), COALESCE(s.record_details, '{}'), \
             r.fork_approval_pending, r.fork_approval_requested_at, \
             r.fork_approval_approved_at, r.fork_approval_note, r.reports_check_runs, \
             r.created_at, r.started_at, r.completed_at \
         FROM runs r LEFT JOIN run_submissions s ON s.run_id = r.run_id \
         WHERE r.run_id = ?1",
    )
    .map_err(db)?
    .execute([run])
    .map_err(db)?;
    tx.prepare_cached(
        "INSERT INTO job_history (run_id, run_created_at, run_attempt, job_id, \
             namespace_id, kind, parent_job_id, base_id, display_name, status, \
             pool_key, outputs, annotations, check_run_id, created_at, \
             deps_ready_at, started_at, completed_at) \
         SELECT j.run_id, r.created_at, ?3, j.job_id, j.namespace_id, j.kind, \
             j.parent_job_id, j.base_id, COALESCE(s.display_name, j.job_id), \
             j.status, j.pool_key, j.outputs, j.annotations, j.check_run_id, \
             j.created_at, j.deps_ready_at, j.started_at, j.completed_at \
         FROM jobs j JOIN runs r ON r.run_id = j.run_id \
         LEFT JOIN job_specs s ON s.run_id = j.run_id AND s.job_id = j.job_id \
         WHERE j.run_id = ?1",
    )
    .map_err(db)?
    .execute(params![run, run, prev_attempt])
    .map_err(db)?;
    Ok(())
}

/// Rewrite `github.run_attempt` in the stored `run_submissions.github_context`.
fn bump_run_attempt(
    tx: &Transaction<'_>,
    run: &str,
    run_attempt: i64,
) -> Result<serde_json::Value, ControlError> {
    let json: Option<String> = tx
        .prepare_cached("SELECT github_context FROM run_submissions WHERE run_id = ?1")
        .map_err(db)?
        .query_row([run], |row| row.get(0))
        .optional()
        .map_err(db)?;
    let mut github: serde_json::Value = json
        .and_then(|j| serde_json::from_str(&j).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    if let Some(object) = github.as_object_mut() {
        object.insert("run_attempt".to_owned(), serde_json::json!(run_attempt));
    }
    tx.prepare_cached("UPDATE run_submissions SET github_context = ?2 WHERE run_id = ?1")
        .map_err(db)?
        .execute(params![run, github.to_string()])
        .map_err(db)?;
    Ok(github)
}

/// Remove the per-attempt check-run mapping for selected jobs: the new
/// attempt mints fresh check runs (`jobs.check_run_id` is cleared by
/// `reset_job_row`; `record_details.job_check_run_ids` is the pre-creation
/// mapping the GitHub reporter reads).
fn clear_recorded_check_runs(
    tx: &Transaction<'_>,
    run: &str,
    set: &BTreeSet<JobId>,
) -> Result<(), ControlError> {
    let details: Option<String> = tx
        .prepare_cached("SELECT record_details FROM run_submissions WHERE run_id = ?1")
        .map_err(db)?
        .query_row([run], |row| row.get(0))
        .optional()
        .map_err(db)?;
    let Some(details) = details else {
        return Ok(());
    };
    let mut json: serde_json::Value = serde_json::from_str(&details)
        .map_err(|e| ControlError::backend(anyhow::anyhow!("record_details decode: {e}")))?;
    if let Some(map) = json
        .get_mut("job_check_run_ids")
        .and_then(|v| v.as_object_mut())
    {
        for job_id in set {
            map.remove(&job_id.0);
        }
    }
    tx.prepare_cached("UPDATE run_submissions SET record_details = ?2 WHERE run_id = ?1")
        .map_err(db)?
        .execute(params![run, json.to_string()])
        .map_err(db)?;
    Ok(())
}

/// Drop wait/hold rows a completed run should never own, and per-job waits
/// for selected members (their gates re-acquire below). Jobset rows that
/// named a consumed caller expansion are stale for the new attempt.
fn clear_stale_concurrency(
    tx: &Transaction<'_>,
    run: &str,
    set: &BTreeSet<JobId>,
) -> Result<(), ControlError> {
    // Run-scoped leftovers (defensive — `release_concurrency_for_run` owns
    // the completed-run invariant).
    tx.prepare_cached("DELETE FROM concurrency_waits WHERE holder_run_id = ?1")
        .map_err(db)?
        .execute([run])
        .map_err(db)?;
    // Job-keyed waits of selected members.
    for member in set {
        tx.prepare_cached(
            "DELETE FROM concurrency_waits WHERE holder_run_id = ?1 \
             AND holder_kind = 'job' AND holder_job_id = ?2",
        )
        .map_err(db)?
        .execute(params![run, member.0])
        .map_err(db)?;
    }
    // Jobsets minted for caller expansions are consumed state: the children
    // rows persist (FK-safe), the gate rows do not re-arm.
    tx.prepare_cached("DELETE FROM jobsets WHERE run_id = ?1")
        .map_err(db)?
        .execute([run])
        .map_err(db)?;
    Ok(())
}

/// Close stale in-flight request rows for the selected jobs: a completed
/// run's requests should already carry a result, but a dependency-skipped or
/// arrival-cancelled job can leave a NULL-result row whose job row the rerun
/// is about to reset. The partial unique index `job_requests_inflight`
/// refuses the next attempt's request otherwise. The closed row keeps the
/// old attempt's outcome (the raw status; anything non-terminal reads
/// `cancelled`, mirroring the PG path).
fn settle_stale_requests(
    tx: &Transaction<'_>,
    run: &str,
    rows: &[RerunRow],
    set: &BTreeSet<JobId>,
) -> Result<(), ControlError> {
    let now = now_us();
    let mut stmt = tx
        .prepare_cached(
            "UPDATE job_requests SET result = ?3, \
                 finished_at = COALESCE(finished_at, ?4) \
             WHERE run_id = ?1 AND job_id = ?2 AND result IS NULL",
        )
        .map_err(db)?;
    for member in rows {
        if !set.contains(&member.row.job_id) {
            continue;
        }
        let result = match member.status_raw.as_str() {
            "success" | "failure" | "cancelled" | "skipped" | "timed_out" => {
                member.status_raw.as_str()
            }
            _ => "cancelled",
        };
        stmt.execute(params![run, member.row.job_id.0, result, now])
            .map_err(db)?;
    }
    Ok(())
}

/// Reset one `jobs` row for the new attempt: classification fields get the
/// fresh values; every per-attempt stamp clears.
fn reset_job_row(
    tx: &Transaction<'_>,
    run: &str,
    job_id: &JobId,
    status: ExecutionStatus,
    queue_state: &str,
    remaining_needs: usize,
    environment_gate: Option<&str>,
) -> Result<(), ControlError> {
    tx.prepare_cached(
        "UPDATE jobs SET status = ?3, queue_state = ?4, remaining_needs = ?5, \
             outputs = NULL, annotations = NULL, check_run_id = NULL, \
             enqueued_at = NULL, claimed_by_runner_id = NULL, claimed_at = NULL, \
             deps_ready_at = NULL, concurrency_wait_at = NULL, \
             concurrency_acquired_at = NULL, started_at = NULL, completed_at = NULL, \
             environment_gate = ?6 \
         WHERE run_id = ?1 AND job_id = ?2",
    )
    .map_err(db)?
    .execute(params![
        run,
        job_id.0,
        status_str(status),
        queue_state,
        remaining_needs as i64,
        environment_gate,
    ])
    .map_err(db)?;
    Ok(())
}

/// Land a job terminal without a message or mint (submit's `insert` +
/// `concluded` shape for skipped/unhostable jobs).
fn reset_concluded(
    tx: &Transaction<'_>,
    run: &str,
    job_id: &JobId,
    status: ExecutionStatus,
) -> Result<(), ControlError> {
    tx.prepare_cached(
        "UPDATE jobs SET status = ?3, queue_state = 'none', remaining_needs = 0, \
             outputs = NULL, annotations = NULL, check_run_id = NULL, \
             enqueued_at = NULL, claimed_by_runner_id = NULL, claimed_at = NULL, \
             deps_ready_at = NULL, concurrency_wait_at = NULL, \
             concurrency_acquired_at = NULL, started_at = NULL, \
             completed_at = ?4, environment_gate = NULL \
         WHERE run_id = ?1 AND job_id = ?2",
    )
    .map_err(db)?
    .execute(params![run, job_id.0, status_str(status), now_us()])
    .map_err(db)?;
    Ok(())
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
/// `github.run_attempt` inside it would otherwise keep reporting 1).
fn patch_condition_context(
    tx: &Transaction<'_>,
    run: &str,
    job_id: &JobId,
    run_attempt: i64,
) -> Result<(), ControlError> {
    let ctx_json: Option<String> = tx
        .prepare_cached(
            "SELECT condition_context FROM job_messages WHERE run_id = ?1 AND job_id = ?2",
        )
        .map_err(db)?
        .query_row(params![run, job_id.0], |row| row.get(0))
        .optional()
        .map_err(db)?;
    let Some(ctx_json) = ctx_json else {
        return Ok(());
    };
    let mut context: preloop_gha_expressions::Context =
        serde_json::from_str(&ctx_json).unwrap_or_default();
    context.merge_root("github", serde_json::json!({"run_attempt": run_attempt}));
    tx.prepare_cached(
        "UPDATE job_messages SET condition_context = ?3 WHERE run_id = ?1 AND job_id = ?2",
    )
    .map_err(db)?
    .execute(params![
        run,
        job_id.0,
        serde_json::to_string(&context)
            .map_err(|e| ControlError::backend(anyhow::anyhow!("context encode: {e}")))?
    ])
    .map_err(db)?;
    Ok(())
}

/// Mint one attempt row for a reset job: fresh `jobId`/`timeline` in the
/// stored template (already rewritten), the `job_requests` correlation, and
/// the deferred token-mint recipe copied forward (`submit::mint_request`
/// patches `requestId` into the template).
fn mint_attempt(
    tx: &Transaction<'_>,
    namespace: &str,
    run_id: RunId,
    job_id: &JobId,
    token_request_override: Option<&Option<crate::models::GitHubTokenRequest>>,
) -> Result<uuid::Uuid, ControlError> {
    let run = codec::run_key(run_id);
    // Read the agent_job_id the template patch (or rebuild) just wrote.
    let message = jobs::stored_job_message(tx, run_id, job_id)?.ok_or_else(|| {
        ControlError::backend(anyhow::anyhow!(
            "re-run job {job_id} has no message to mint"
        ))
    })?;
    let agent_job_id = message.job_id;
    let timeline_id = message.timeline.id;

    // The token-mint recipe is keyed by request id: carry the previous
    // attempt's forward unless a rebuilt template supplies its own.
    let token_request: Option<crate::models::GitHubTokenRequest> = match token_request_override {
        Some(over) => over.clone(),
        None => tx
            .prepare_cached(
                "SELECT repository, permissions, declared, untrusted \
                     FROM github_token_requests WHERE request_id = (\
                         SELECT request_id FROM job_requests \
                         WHERE run_id = ?1 AND job_id = ?2 \
                         ORDER BY request_id DESC LIMIT 1)",
            )
            .map_err(db)?
            .query_row(params![run, job_id.0], |row| {
                Ok(crate::models::GitHubTokenRequest {
                    repository: row.get(0)?,
                    permissions: serde_json::from_str(&row.get::<_, String>(1)?)
                        .unwrap_or_default(),
                    declared: row.get::<_, bool>(2)?,
                    untrusted: row.get::<_, bool>(3)?,
                })
            })
            .optional()
            .map_err(db)?,
    };

    let step_manifest = crate::models::StepRecord::manifest(&message.steps);
    submit::mint_request(
        tx,
        namespace,
        run_id,
        job_id,
        Some(TaskAgentJobRequestRecord {
            request_id: 0,
            run_id,
            job_id: job_id.clone(),
            agent_job_id,
            plan_id: message.plan.plan_id.clone(),
            plan_type: codec::PLAN_TYPE.to_owned(),
            timeline_id,
            result: None,
            locked_until: crate::distributed_task::agent_request_locked_until(),
            claimed_at: None,
            owner_runner_id: None,
            started_at: None,
            last_renewed_at: None,
            timeout_triggered: false,
            debug_token_issued: false,
        }),
        token_request,
        step_manifest,
    )?;
    Ok(agent_job_id)
}

/// `ArrivalCancelled` inside the workflow gate: every selected job lands
/// `cancelled` (no mint — GitHub does not create a second attempt for a
/// re-run the queue rejected), the run re-completes, and the event pair
/// mirrors submit's arrival-cancelled emissions.
fn arrival_cancelled(
    tx: &Transaction<'_>,
    run: &str,
    run_id: RunId,
    namespace: &str,
    set: &BTreeSet<JobId>,
    rows: &[RerunRow],
    run_number: i64,
    run_attempt: i64,
) -> Result<RerunOutcome, ControlError> {
    for member in rows {
        if set.contains(&member.row.job_id) {
            reset_concluded(tx, run, &member.row.job_id, ExecutionStatus::Cancelled)?;
        }
    }
    tx.prepare_cached(
        "UPDATE runs SET status = 'completed', conclusion = 'cancelled', \
             completed_at = ?2, started_at = COALESCE(started_at, ?2) \
         WHERE run_id = ?1",
    )
    .map_err(db)?
    .execute(params![run, now_us()])
    .map_err(db)?;
    let concluded: Vec<(JobId, ExecutionStatus, Option<String>)> = rows
        .iter()
        .filter(|m| set.contains(&m.row.job_id))
        .map(|m| {
            (
                m.row.job_id.clone(),
                ExecutionStatus::Cancelled,
                concurrency::cancelled_reason(),
            )
        })
        .collect();
    jobs::emit_outbox(
        tx,
        namespace,
        Some(run_id),
        "run.rerun.v1",
        serde_json::json!({
            "status": "cancelled",
            "run_attempt": run_attempt,
            "jobs": set.len(),
        }),
    )?;
    jobs::emit_outbox(
        tx,
        namespace,
        Some(run_id),
        "run.completed.v1",
        serde_json::json!({"status": "cancelled"}),
    )?;
    Ok(RerunOutcome {
        run_id,
        run_attempt: run_attempt as u64,
        run_number: run_number as u64,
        queued_jobs: 0,
        status: ExecutionStatus::Cancelled,
        concluded,
        rerun_jobs: Vec::new(),
        selected: set.clone(),
        held: false,
        queue_depth: jobs::ready_count(tx)?,
        next_runs_on: jobs::next_ready_labels(tx)?,
    })
}

fn mode_name(mode: &RerunMode) -> &'static str {
    match mode {
        RerunMode::All => "all",
        RerunMode::Failed => "failed",
        RerunMode::Job(_) => "job",
    }
}
