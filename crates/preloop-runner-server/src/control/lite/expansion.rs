//! Deferred-expansion commands: `claim_expansion` leases the oldest
//! `pending_expansion` node under an `expand_generation` fence and
//! snapshots its build inputs; `apply_expansion` folds the built subtree in
//! iff the fence still matches, then promotes whatever unblocked.
//!
//! Translated from `control/pg/dispatch.rs` (`claim_expansion`,
//! `apply_expansion`, `register_expansion_jobs`, `expansion_plan`). On
//! SQLite there is one writer: `FOR UPDATE SKIP LOCKED` and the run-row
//! lock are implicit; the generation conditional stays the apply fence.
//! Plan assembly and the subtree build are the shared `control/sched.rs`
//! machinery (`ExpansionPlan`, `BuiltJob`, `BuiltExpansion`) — the backend
//! only moves rows.

use super::codec::{self, now_us};
use super::jobs::{self, ReusableSpec};
use super::{LiteBackend, db, promote, settle, submit};
use crate::control::backend::ExpansionApply;
use crate::control::logic::{
    BuiltExpansion, BuiltJob, ExpansionContext, ExpansionPlan, MatrixExpansionInputs,
    ReusableExpansionInputs,
};
use crate::control::types::*;
use crate::models::QueuedJob;
use crate::runtime_scheduling::SchedulingOutcome;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::{BTreeMap, BTreeSet};

/// The build inputs a deferred node snapshots under its generation fence
/// (pg's `expansion_plan`): the run context fields plus every needed job's
/// resolved outputs. Data assembly only — what expands and when was the
/// `ExpansionDecision` made by `logic.rs` upstream of `submit`.
fn expansion_plan(record: &crate::models::RunRecord, job: &QueuedJob) -> Option<ExpansionPlan> {
    let ctx = ExpansionContext {
        run_id: job.run_id,
        submission: record.submission.clone(),
        snapshot: record.workspace_snapshot.clone(),
        github_json: record.github.clone(),
        workflow_path: record.workflow_path_str.clone(),
        workflow_ref: record.workflow_ref.clone(),
        head_sha: record.head_sha.clone(),
    };
    let mut needs_outputs: BTreeMap<String, BTreeMap<String, serde_json::Value>> = BTreeMap::new();
    for need in &job.needs {
        for matched in crate::runtime_scheduling::matching_need_ids(record, need) {
            if let Some(outputs) = record.job_outputs.get(&matched) {
                let base = record
                    .job_base_ids
                    .get(&matched)
                    .cloned()
                    .unwrap_or_else(|| need.0.clone());
                needs_outputs
                    .entry(base)
                    .or_default()
                    .extend(outputs.clone());
            }
        }
    }
    if let Some(call) = job.reusable_call.clone() {
        let caller_plan = record.caller_plans.get(&job.job_id)?.clone();
        return Some(ExpansionPlan::Reusable(Box::new(ReusableExpansionInputs {
            ctx,
            caller_id: job.job_id.clone(),
            caller_plan,
            call,
            needs_outputs,
        })));
    }
    let expression = job.deferred_matrix.clone()?;
    let workflow_file = record
        .caller_plans
        .get(&job.job_id)
        .and_then(|plan| plan.workflow_file.clone());
    Some(ExpansionPlan::Matrix(Box::new(MatrixExpansionInputs {
        ctx,
        node_id: job.job_id.clone(),
        base_id: job.base_id.clone(),
        expression,
        needs_outputs,
        workflow_file,
    })))
}

/// A built job's parent: its matrix parent when it expanded from one
/// (`node_parent` in pg's dispatch).
fn node_parent(plan: &preloop_gha_protocol::JobPlan, job_id: &JobId) -> Option<String> {
    if plan.base_id != job_id.0 && plan.matrix_index.is_some() {
        Some(plan.base_id.clone())
    } else {
        None
    }
}

/// Strip runner-bound secret material out of a stored message template
/// (pg's `strip_secret_values`): the template keeps the variable *names*
/// for the acquire-time rebuild; values are never stored.
fn strip_secret_values(
    message: &mut preloop_gha_protocol::azdo::AgentJobRequestMessage,
) -> Vec<String> {
    let mut names = Vec::new();
    for (name, value) in message.variables.iter_mut() {
        if value.is_secret == Some(true) {
            names.push(name.clone());
            value.value = None;
        }
    }
    message.mask_hints.clear();
    for endpoint in &mut message.resources.endpoints {
        endpoint.authorization.parameters.clear();
    }
    names
}

/// The `QueuedJob` a `BuiltJob`'s plan describes (`Node` construction in
/// pg's `register_expansion_jobs`). Ordering columns are 0 — expansion
/// legs join the queue below every submitted job, like pg.
fn built_queued_job(run_id: RunId, built: &BuiltJob) -> QueuedJob {
    let plan = &built.plan;
    QueuedJob {
        run_id,
        job_id: plan.id.clone(),
        base_id: plan.base_id.clone(),
        created_at_unix_nanos: 0,
        dependencies_ready_at_unix_nanos: plan
            .needs
            .is_empty()
            .then(|| now_us().saturating_mul(1000)),
        concurrency_wait_started_at_unix_nanos: None,
        concurrency_acquired_at_unix_nanos: None,
        enqueued_at_unix_nanos: 0,
        needs: plan.needs.clone(),
        if_condition: plan.if_condition.clone(),
        condition_context: built.condition_context.clone(),
        max_parallel: plan.max_parallel,
        runs_on: plan.runs_on.clone(),
        runner_group: plan.runner_group.clone(),
        message: built.artifacts.agent_msg.clone(),
        environment: plan.environment.clone(),
        concurrency: crate::concurrency::concurrency_from_plan_fields(
            plan.concurrency_group.as_deref(),
            plan.concurrency_cancel_in_progress.as_deref(),
            plan.concurrency_queue.as_deref(),
        ),
        matrix: plan
            .matrix
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        deferred_matrix: plan.deferred_matrix.clone(),
        reusable_call: plan.reusable_call.clone(),
        // Freshly registered legs are armed only at scheduler admission.
        environment_gate: None,
    }
}

/// Register one built subtree's jobs (pg's `register_expansion_jobs`):
/// `jobs` + `job_specs` + `job_needs` + `job_messages` + `job_requests` +
/// `github_token_requests` + `job_steps`, all `Pending`/`Blocked` — the
/// promotion sweep admits them. Unhostable legs settle `failure`
/// immediately (their placeholder request is retired, none minted).
/// `callee_meta` is the `BuiltExpansion::Reusable` metadata map, keyed by
/// callee job id; it lands in the callee spec envelopes since SQL has no
/// in-session record to carry it. Returns the registered count.
/// Append each expanded job's resolved display name to
/// `record_details.job_names` — the field is verbatim record detail (submit
/// writes only the declared jobs), so runtime-materialized legs would render
/// as raw ids without this.
fn merge_expanded_job_names(
    tx: &Transaction<'_>,
    run_id: RunId,
    names: &[(String, String)],
) -> Result<(), ControlError> {
    if names.is_empty() {
        return Ok(());
    }
    let run = codec::run_key(run_id);
    let mut details: serde_json::Value = tx
        .prepare_cached("SELECT record_details FROM run_submissions WHERE run_id = ?1")
        .map_err(db)?
        .query_row([&run], |row| row.get::<_, Option<String>>(0))
        .optional()
        .map_err(db)?
        .flatten()
        .and_then(|j| serde_json::from_str(&j).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    let entry = details
        .as_object_mut()
        .expect("record_details is a JSON object")
        .entry("job_names".to_owned())
        .or_insert_with(|| serde_json::json!({}));
    if let Some(map) = entry.as_object_mut() {
        for (job_id, name) in names {
            map.insert(job_id.clone(), serde_json::Value::String(name.clone()));
        }
    }
    tx.prepare_cached("UPDATE run_submissions SET record_details = ?2 WHERE run_id = ?1")
        .map_err(db)?
        .execute(params![
            run,
            serde_json::to_string(&details).map_err(ControlError::backend)?
        ])
        .map_err(db)?;
    Ok(())
}

fn register_built_jobs(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    run_id: RunId,
    namespace: &str,
    jobs: Vec<BuiltJob>,
    callee_meta: &BTreeMap<String, preloop_gha_parser::ReusableCallMetadata>,
) -> Result<usize, ControlError> {
    let platforms = jobs::registered_platforms(tx)?;
    let mut registered = 0usize;
    // Matrix legs reference their base as `parent_job_id` — an FK target.
    // Materialize a `matrix_parent` row for any covered parent first (the
    // callee subtree's legs replaced it at build time, so it never mints a
    // request); the run graph excludes it via the has-children predicate.
    let mut parents_needed: BTreeSet<String> = BTreeSet::new();
    for built in &jobs {
        if let Some(parent) = node_parent(&built.plan, &built.plan.id) {
            parents_needed.insert(parent);
        }
    }
    for parent_id in parents_needed {
        tx.prepare_cached(
            "INSERT INTO jobs (run_id, job_id, namespace_id, kind, \
                 parent_job_id, base_id, status, queue_state, remaining_needs, \
                 pool_key, runs_on, priority, run_order, job_order, created_at) \
             SELECT ?1, ?2, ?3, 'matrix_parent', NULL, ?2, 'pending', 'none', 0, \
                    '', '[]', 0, 0, 0, ?4 \
             WHERE NOT EXISTS (SELECT 1 FROM jobs WHERE run_id = ?1 AND job_id = ?2)",
        )
        .map_err(db)?
        .execute(params![
            codec::run_key(run_id),
            parent_id,
            namespace,
            now_us()
        ])
        .map_err(db)?;
    }
    for built in jobs {
        let plan = built.plan.clone();
        let job_id = plan.id.clone();
        let unhostable = crate::runtime_scheduling::unhostable_platform(
            &plan.runs_on,
            platforms.iter().copied(),
        );
        let meta = callee_meta.get(&job_id.0).cloned();
        let spec = jobs::SpecExtras {
            reusable_call_json: ReusableSpec::encode(
                plan.reusable_call.as_ref(),
                meta.as_ref(),
                Some(&plan),
            ),
            fail_fast: Some(plan.fail_fast),
            continue_on_error: Some(plan.continue_on_error),
            id_token_granted: built.artifacts.id_token_granted,
            oidc_environment: built.artifacts.oidc_ctx.environment.as_deref(),
            oidc_job_workflow_ref: built.artifacts.oidc_ctx.job_workflow_ref.as_deref(),
            oidc_job_workflow_sha: built.artifacts.oidc_ctx.job_workflow_sha.as_deref(),
        };
        let queued = built_queued_job(run_id, &built);
        let kind = submit::kind_of(&queued);
        let parent = node_parent(&plan, &job_id);
        let (status, queue_state) = if unhostable.is_some() {
            (ExecutionStatus::Failure, "none")
        } else {
            (ExecutionStatus::Pending, "blocked")
        };
        jobs::insert_job(
            tx,
            run_id,
            namespace,
            &queued,
            kind,
            parent.as_deref(),
            &plan.name,
            plan.matrix_index.map(|i| i as i64).unwrap_or(0),
            status,
            queue_state,
            plan.needs.len() as i32,
            0,
            0,
            &spec,
        )?;
        // A check run minted before this leg materialized lives in
        // `record_details.job_check_run_ids` — promote it onto the row so
        // `jobs.check_run_id` stays the canonical live lookup.
        tx.prepare_cached(
            "UPDATE jobs SET check_run_id = json_extract( \
                 (SELECT record_details FROM run_submissions WHERE run_id = ?1), \
                 '$.\"job_check_run_ids\".\"' || replace(?2, '\"', '\\\"') || '\"') \
             WHERE run_id = ?1 AND job_id = ?2 AND check_run_id IS NULL \
               AND EXISTS (SELECT 1 FROM run_submissions WHERE run_id = ?1)",
        )
        .map_err(db)?
        .execute(params![codec::run_key(run_id), job_id.0])
        .map_err(db)?;
        if let Some(platform) = unhostable {
            tracing::warn!(
                run_id = %run_id.0,
                job = %job_id.0,
                "materialized callee job is unhostable: no {platform} runner"
            );
            settle::settle_node(tx, backend, run_id, &job_id, ExecutionStatus::Failure)?;
            continue;
        }
        // Strip secret values before persistence; the acquire path rebuilds
        // them from the named secrets.
        let mut message = built.artifacts.agent_msg.clone();
        let secret_names = strip_secret_values(&mut message);
        // The message row must exist before `mint_request` patches its
        // requestId (submit's order).
        jobs::insert_job_message(tx, run_id, &job_id, &message, &built.condition_context)?;
        tx.prepare_cached(
            "UPDATE job_messages SET secret_names = ?3 \
             WHERE run_id = ?1 AND job_id = ?2",
        )
        .map_err(db)?
        .execute(params![
            codec::run_key(run_id),
            job_id.0,
            serde_json::to_string(&secret_names).unwrap_or_else(|_| "[]".to_owned())
        ])
        .map_err(db)?;
        submit::mint_request(
            tx,
            namespace,
            run_id,
            &job_id,
            Some(built.artifacts.job_request.clone()),
            built.artifacts.github_token_request.clone(),
            crate::models::StepRecord::manifest(&built.artifacts.agent_msg.steps),
        )?;
        registered += 1;
    }
    Ok(registered)
}

/// Merge callee `ReusableCallMetadata` into the callee spec envelopes and
/// splice the caller's `inner_job_ids` — both `run.reusable_calls` writes
/// of the old apply path, carried by the spec envelope since SQL has no
/// record to mutate.
fn write_reusable_meta(
    tx: &Transaction<'_>,
    run_id: RunId,
    caller_id: &JobId,
    inner_ids: &[String],
    callee_meta: &BTreeMap<String, preloop_gha_parser::ReusableCallMetadata>,
) -> Result<(), ControlError> {
    let run = codec::run_key(run_id);
    for (job_id, meta) in callee_meta {
        let existing: Option<String> = tx
            .prepare_cached("SELECT reusable_call FROM job_specs WHERE run_id = ?1 AND job_id = ?2")
            .map_err(db)?
            .query_row(params![run, job_id.as_str()], |row| row.get(0))
            .optional()
            .map_err(db)?;
        let (call, plan) = match existing.as_deref().map(ReusableSpec::decode) {
            Some(ReusableSpec::Full { call, plan, .. }) => (call, plan),
            Some(ReusableSpec::Call(call)) => (Some(call), None),
            None => (None, None),
        };
        if let Some(json) = ReusableSpec::encode(call.as_ref(), Some(meta), plan.as_ref()) {
            tx.prepare_cached(
                "UPDATE job_specs SET reusable_call = ?3 \
                 WHERE run_id = ?1 AND job_id = ?2",
            )
            .map_err(db)?
            .execute(params![run, job_id.as_str(), json])
            .map_err(db)?;
        }
    }
    // The caller's own meta gains its subtree: `inner_job_ids = inner_ids`.
    let existing: Option<String> = tx
        .prepare_cached("SELECT reusable_call FROM job_specs WHERE run_id = ?1 AND job_id = ?2")
        .map_err(db)?
        .query_row(params![run, caller_id.0], |row| row.get(0))
        .optional()
        .map_err(db)?;
    if let Some(ReusableSpec::Full {
        call,
        meta: Some(mut meta),
        plan,
    }) = existing.as_deref().map(ReusableSpec::decode)
    {
        meta.inner_job_ids = inner_ids.to_vec();
        if let Some(encoded) = ReusableSpec::encode(call.as_ref(), Some(&meta), plan.as_ref()) {
            tx.prepare_cached(
                "UPDATE job_specs SET reusable_call = ?3 \
                 WHERE run_id = ?1 AND job_id = ?2",
            )
            .map_err(db)?
            .execute(params![run, caller_id.0, encoded])
            .map_err(db)?;
        }
    }
    Ok(())
}

/// The matrix-parent `inner_job_ids` splice across every caller meta
/// mentioning the expanded node (old `apply_expansion`: replace the parent
/// id with its leg ids inside each caller's inner list).
fn splice_matrix_parents(
    tx: &Transaction<'_>,
    run_id: RunId,
    node_id: &JobId,
    leg_ids: &[String],
) -> Result<(), ControlError> {
    let run = codec::run_key(run_id);
    let rows: Vec<(String, Option<String>)> = {
        let mut stmt = tx
            .prepare_cached(
                "SELECT job_id, reusable_call FROM job_specs WHERE run_id = ?1 \
                 AND reusable_call IS NOT NULL",
            )
            .map_err(db)?;
        let rows = stmt
            .query_map([&run], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(db)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db)?
    };
    for (job_id, json) in rows {
        let Some(json) = json else { continue };
        if let ReusableSpec::Full {
            call,
            meta: Some(mut meta),
            plan,
        } = ReusableSpec::decode(&json)
        {
            if let Some(pos) = meta.inner_job_ids.iter().position(|id| id == &node_id.0) {
                meta.inner_job_ids
                    .splice(pos..pos + 1, leg_ids.iter().cloned());
                if let Some(encoded) =
                    ReusableSpec::encode(call.as_ref(), Some(&meta), plan.as_ref())
                {
                    tx.prepare_cached(
                        "UPDATE job_specs SET reusable_call = ?3 \
                         WHERE run_id = ?1 AND job_id = ?2",
                    )
                    .map_err(db)?
                    .execute(params![run, job_id, encoded])
                    .map_err(db)?;
                }
            }
        }
    }
    Ok(())
}

impl LiteBackend {
    /// `claim_expansion`: lease the oldest deferred node for a build.
    ///
    /// The lease bumps `expand_generation`; `apply_expansion` refuses a
    /// build whose generation no longer matches, so a node cancelled or
    /// re-leased while the build ran cannot fold a stale subtree in.
    pub(crate) async fn claim_expansion(&self) -> Result<Option<ExpansionClaim>, ControlError> {
        self.write(|tx| {
            let Some((run, job)) = tx
                .prepare_cached(
                    "SELECT run_id, job_id FROM jobs \
                     WHERE queue_state = 'pending_expansion' \
                     ORDER BY enqueued_at, run_id, job_id LIMIT 1",
                )
                .map_err(db)?
                .query_row([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .optional()
                .map_err(db)?
            else {
                return Ok(None);
            };
            let run_id = codec::run_id(&run);
            let job_id = JobId(job.clone());
            let generation: i64 = tx
                .prepare_cached(
                    "UPDATE jobs SET queue_state = 'expanding', \
                     expand_generation = expand_generation + 1 \
                     WHERE run_id = ?1 AND job_id = ?2 \
                     AND queue_state = 'pending_expansion' \
                     RETURNING expand_generation",
                )
                .map_err(db)?
                .query_row(params![run, job], |row| row.get(0))
                .map_err(db)?;
            let Some(record) = jobs::run_record(tx, run_id)? else {
                return Ok(None);
            };
            let queued = jobs::queued_job(tx, run_id, &job_id)?;
            let plan = queued.as_ref().and_then(|job| expansion_plan(&record, job));
            let job = queued.ok_or_else(|| {
                // Every submitted/expanded node writes a message template; a
                // missing one means the row was lost, not that expansion is
                // optional.
                ControlError::backend(anyhow::anyhow!("expansion node has no message template"))
            })?;
            Ok(Some(ExpansionClaim {
                job,
                generation,
                plan,
            }))
        })
    }

    /// `apply_expansion`: fold a built subtree back into the run under the
    /// claim's generation fence, then promote what it unblocked. A stale
    /// generation (the node was cancelled or re-leased mid-build) discards
    /// the build.
    pub(crate) async fn apply_expansion(
        &self,
        claim: ExpansionApply,
    ) -> Result<SchedulingOutcome, ControlError> {
        let run_id = claim.job.run_id;
        let node_id = claim.job.job_id.clone();
        self.write(|tx| {
            let current: Option<i64> = tx
                .prepare_cached(
                    "SELECT expand_generation FROM jobs \
                     WHERE run_id = ?1 AND job_id = ?2",
                )
                .map_err(db)?
                .query_row(params![codec::run_key(run_id), node_id.0], |row| row.get(0))
                .optional()
                .map_err(db)?;
            let Some(current) = current else {
                return Ok(SchedulingOutcome::default());
            };
            if current != claim.generation {
                // Stale lease: the node was cancelled or re-leased while the
                // build ran. Discard the subtree.
                return Ok(SchedulingOutcome::default());
            }
            let namespace = jobs::namespace_of(tx, run_id)?;
            let mut outcome = SchedulingOutcome::default();
            let built = match claim.built {
                Ok(built) => built,
                Err(status) => {
                    settle::settle_node(tx, self, run_id, &node_id, status)?;
                    outcome.failed.push((run_id, node_id));
                    // Children materialize *after* the caller's own needs may have
                    // settled (they inherit them): recompute before promoting, same
                    // as the submit path.
                    jobs::refresh_run_remaining_needs(tx, run_id)?;
                    promote::promote_run(tx, self, run_id, &mut outcome)?;
                    return Ok(outcome);
                }
            };
            match built {
                BuiltExpansion::Matrix { jobs } if jobs.is_empty() => {
                    // An empty matrix concludes the node as skipped.
                    settle::settle_node(tx, self, run_id, &node_id, ExecutionStatus::Skipped)?;
                    outcome.skipped.push((run_id, node_id));
                }
                BuiltExpansion::Matrix { jobs } => {
                    let leg_ids: Vec<String> =
                        jobs.iter().map(|job| job.plan.id.0.clone()).collect();
                    let names: Vec<(String, String)> = jobs
                        .iter()
                        .map(|j| (j.plan.id.0.clone(), j.plan.name.clone()))
                        .collect();
                    register_built_jobs(tx, self, run_id, &namespace, jobs, &BTreeMap::new())?;
                    merge_expanded_job_names(tx, run_id, &names)?;
                    // The parent leaves the run's status map; its legs take
                    // over (`has_children` is derived: `run_graph` excludes
                    // a matrix parent with a child row, so no flag exists).
                    tx.prepare_cached(
                        "UPDATE jobs SET queue_state = 'none' \
                         WHERE run_id = ?1 AND job_id = ?2",
                    )
                    .map_err(db)?
                    .execute(params![codec::run_key(run_id), node_id.0])
                    .map_err(db)?;
                    splice_matrix_parents(tx, run_id, &node_id, &leg_ids)?;
                    // The placeholder's request row is deleted (not settled):
                    // plan/agent-job correlation must not resolve to a node
                    // the subtree replaced.
                    settle::purge_node_requests(tx, run_id, &node_id)?;
                }
                BuiltExpansion::Reusable {
                    caller_id,
                    jobs,
                    reusable_calls,
                } => {
                    let inner_ids: Vec<String> =
                        jobs.iter().map(|job| job.plan.id.0.clone()).collect();
                    let names: Vec<(String, String)> = jobs
                        .iter()
                        .map(|j| (j.plan.id.0.clone(), j.plan.name.clone()))
                        .collect();
                    register_built_jobs(tx, self, run_id, &namespace, jobs, &reusable_calls)?;
                    merge_expanded_job_names(tx, run_id, &names)?;
                    // The caller is now executing inside its subtree: its
                    // row is `in_progress`/unqueued; the callee metadata and
                    // `inner_job_ids` land in the spec envelopes.
                    tx.prepare_cached(
                        "UPDATE jobs SET status = 'in_progress', queue_state = 'none' \
                         WHERE run_id = ?1 AND job_id = ?2",
                    )
                    .map_err(db)?
                    .execute(params![codec::run_key(run_id), caller_id.0])
                    .map_err(db)?;
                    write_reusable_meta(tx, run_id, &caller_id, &inner_ids, &reusable_calls)?;
                    tx.prepare_cached(
                        "UPDATE runs SET started_at = COALESCE(started_at, ?2) \
                         WHERE run_id = ?1",
                    )
                    .map_err(db)?
                    .execute(params![codec::run_key(run_id), now_us()])
                    .map_err(db)?;
                    settle::retire_node_requests(
                        tx,
                        run_id,
                        &caller_id,
                        ExecutionStatus::InProgress,
                    )?;
                }
            }
            // Children materialize *after* the caller's own needs may have
            // settled (they inherit them): recompute before promoting, same
            // as the submit path.
            jobs::refresh_run_remaining_needs(tx, run_id)?;
            promote::promote_run(tx, self, run_id, &mut outcome)?;
            Ok(outcome)
        })
    }
}
