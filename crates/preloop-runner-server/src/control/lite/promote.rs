//! The promotion sweep: move blocked jobs to `ready` (or terminal) once
//! `needs:` settle, evaluating job/jobset gates and max-parallel in SQL.
//!
//! Dependency verdicts come from `logic::dependency_decision`. `jobs.status`
//! carries the
//! workflow truth; `queue_state` is the derived dispatch copy.

use super::codec::{self, now_us};
use super::concurrency as cg;
use super::jobs::{self, JobRow, RunGraph};
use super::{LiteBackend, db};
use crate::concurrency::{self, Holder};
use crate::control::logic;
use crate::control::types::*;
use crate::runtime_scheduling::DependencyDecision;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId, azdo};
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::{BTreeMap, BTreeSet};

/// A job's declared `needs:` aggregate (`aggregate_need_status` over the
/// matching ancestor statuses) evaluated against its `if:` condition —
/// `dependency_decision` on the `RunGraph` view.
fn dependency_decision(
    graph: &RunGraph,
    needs: &[JobId],
    if_condition: Option<&str>,
    condition_context: &preloop_gha_expressions::Context,
) -> Result<DependencyDecision, ControlError> {
    if needs.is_empty() {
        return Ok(DependencyDecision::Run);
    }
    let direct: Vec<ExecutionStatus> = needs
        .iter()
        .flat_map(|need| graph.matching_statuses(need))
        .collect();
    if direct.is_empty() || direct.iter().any(|status| !status.is_terminal()) {
        return Ok(DependencyDecision::Wait);
    }
    let aggregate =
        crate::runtime_scheduling::aggregate_need_status(&graph.ancestor_statuses(needs))
            .unwrap_or(ExecutionStatus::Skipped);
    let mut context = condition_context.clone().with_status(
        aggregate == ExecutionStatus::Success,
        aggregate == ExecutionStatus::Failure,
        aggregate == ExecutionStatus::Cancelled,
    );
    // `needs_json_context` shape: {need_id: {result, outputs}}.
    let needs_rows: Vec<logic::NeedRow> = needs
        .iter()
        .filter_map(|need| {
            // one row per *declared* need id: status = aggregate over its
            // matches; outputs merged across legs.
            let statuses = graph.matching_statuses(need);
            let status = crate::runtime_scheduling::aggregate_need_status(&statuses)?;
            let mut outputs = BTreeMap::new();
            for id in graph.matching(need) {
                if let Some(map) = graph.outputs.get(&id) {
                    outputs.extend(map.clone());
                }
            }
            Some(logic::NeedRow {
                job_id: need.clone(),
                status,
                outputs,
            })
        })
        .collect();
    context.insert("needs", logic::needs_context(&needs_rows));
    let condition = preloop_gha_expressions::effective_condition(if_condition);
    match preloop_gha_expressions::eval_bool(&condition, &context) {
        Ok(true) => Ok(DependencyDecision::Run),
        Ok(false) => Ok(DependencyDecision::Skip),
        Err(_) => Ok(DependencyDecision::Error),
    }
}

/// `try_acquire_job_gate` on SQL rows: evaluate the job's concurrency
/// expression, then `cg::acquire`. On `Parked` the wait row already exists
/// (inserted by `acquire`); the caller flips `queue_state` to `held`.
pub(super) fn acquire_job_gate(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    run_id: RunId,
    job_id: &JobId,
) -> Result<GateOutcome, ControlError> {
    let spec = match jobs::load_spec(tx, run_id, job_id)? {
        Some(spec) => spec,
        None => return Ok(GateOutcome::Proceed),
    };
    let Some(raw) = spec.concurrency.clone() else {
        return Ok(GateOutcome::Proceed);
    };
    let (github, submission) = jobs::run_context(tx, run_id)?;
    // `strategy` rides the message template's context_data (job-builder
    // computed); needs are intentionally absent (gate runs pre-needs).
    let (template, _ctx): (String, String) = tx
        .prepare_cached(
            "SELECT message_template, condition_context FROM job_messages \
             WHERE run_id = ?1 AND job_id = ?2",
        )
        .map_err(db)?
        .query_row(params![codec::run_key(run_id), job_id.0], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()
        .map_err(db)?
        .unwrap_or_else(|| ("{}".to_owned(), "{}".to_owned()));
    let message: azdo::AgentJobRequestMessage = serde_json::from_str(&template)
        .map_err(|e| ControlError::backend(anyhow::anyhow!("job message decode: {e}")))?;
    let strategy = message
        .context_data
        .get("strategy")
        .map(azdo::PipelineContextData::to_json)
        .unwrap_or_else(|| serde_json::json!({}));
    let eval_ctx = concurrency::ConcurrencyContext {
        scope: concurrency::ConcurrencyScope::Job,
        github: &github,
        vars: &submission.vars,
        inputs: &submission.inputs,
        matrix: Some(&spec.matrix),
        strategy: Some(&strategy),
        needs: None,
    };
    let (group, cancel_in_progress, queue) =
        match concurrency::evaluate_concurrency(&raw, &eval_ctx) {
            Ok(v) => v,
            Err(error) => {
                concurrency::log_eval_error("job concurrency", &error);
                return Ok(GateOutcome::Failed(ExecutionStatus::Failure));
            }
        };
    if group.trim().is_empty() {
        return Ok(GateOutcome::Failed(ExecutionStatus::Failure));
    }
    let key = concurrency::concurrency_key(&submission.repository, &group);
    let holder = Holder::Job {
        run_id,
        job_id: job_id.clone(),
    };
    let namespace_id: String = tx
        .prepare_cached("SELECT namespace_id FROM jobs WHERE run_id = ?1 AND job_id = ?2")
        .map_err(db)?
        .query_row(params![codec::run_key(run_id), job_id.0], |row| row.get(0))
        .map_err(db)?;
    let mut cancel = super::settle::canceler(backend);
    match cg::acquire(
        tx,
        &namespace_id,
        &key.0,
        &key.1,
        &group,
        &holder,
        cancel_in_progress,
        queue,
        &mut cancel,
    )? {
        cg::AcqOutcome::Acquired => Ok(GateOutcome::Proceed),
        cg::AcqOutcome::Parked => Ok(GateOutcome::Parked),
        cg::AcqOutcome::ArrivalCancelled => Ok(GateOutcome::Failed(ExecutionStatus::Cancelled)),
        cg::AcqOutcome::Failed => Ok(GateOutcome::Failed(ExecutionStatus::Failure)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GateOutcome {
    Proceed,
    Parked,
    Failed(ExecutionStatus),
}

/// `under_max_parallel`: ready/claimed legs + in_progress rows of the same
/// base id below `max_parallel`.
fn under_max_parallel(
    tx: &Transaction<'_>,
    run_id: RunId,
    base_id: &str,
    max_parallel: Option<u64>,
) -> Result<bool, ControlError> {
    let Some(limit) = max_parallel else {
        return Ok(true);
    };
    let active: i64 = tx
        .prepare_cached(
            "SELECT COUNT(*) FROM jobs WHERE run_id = ?1 AND base_id = ?2 \
             AND (status = 'in_progress' OR queue_state IN ('ready','claimed'))",
        )
        .map_err(db)?
        .query_row(params![codec::run_key(run_id), base_id], |row| row.get(0))
        .map_err(db)?;
    Ok((active as u64) < limit)
}

/// Hydrate `context_data.needs` (and a deferred environment name) into the
/// stored message template — `hydrate_needs_context` + `sync_broker_message`.
/// Returns the job's post-hydration environment name when it has one: a
/// deferred `environment:` expression resolved against the needs context,
/// else the literal spec name. The environment gate evaluates against this
/// resolved name (GitHub evaluates rules against the resolved environment).
fn hydrate_message(
    tx: &Transaction<'_>,
    graph: &RunGraph,
    job: &mut JobRow,
) -> Result<Option<String>, ControlError> {
    let run = codec::run_key(job.run_id);
    let row: Option<(String, String)> = tx
        .prepare_cached(
            "SELECT message_template, condition_context FROM job_messages \
             WHERE run_id = ?1 AND job_id = ?2",
        )
        .map_err(db)?
        .query_row(params![run, job.job_id.0], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()
        .map_err(db)?;
    let Some((template, _ctx)) = row else {
        return Ok(None);
    };
    // The spec's literal `environment:` name is the gate fallback when
    // nothing deferred resolves below (the same name
    // `environment_gate_name_of` reports).
    let mut environment_name = jobs::load_spec(tx, job.run_id, &job.job_id)?
        .as_ref()
        .and_then(|spec| {
            crate::runtime_scheduling::environment_gate_name_of(spec.environment.as_ref())
        })
        .map(str::to_owned);
    let mut message: azdo::AgentJobRequestMessage = serde_json::from_str(&template)
        .map_err(|e| ControlError::backend(anyhow::anyhow!("job message decode: {e}")))?;
    // needs context from the graph (only terminal needs produce entries).
    let mut needs_map: BTreeMap<String, azdo::PipelineContextData> = BTreeMap::new();
    for need in jobs::job_needs(tx, job.run_id, &job.job_id)? {
        let statuses = graph.matching_statuses(&need);
        let Some(result) = crate::runtime_scheduling::aggregate_need_status(&statuses) else {
            continue;
        };
        let mut outputs = BTreeMap::new();
        for id in graph.matching(&need) {
            if let Some(map) = graph.outputs.get(&id) {
                for (key, value) in map {
                    outputs.insert(key.clone(), azdo::PipelineContextData::from_json(value));
                }
            }
        }
        let mut entry = BTreeMap::new();
        entry.insert(
            "result".to_owned(),
            azdo::PipelineContextData::String(crate::runtime_scheduling::status_string(result)),
        );
        entry.insert(
            "outputs".to_owned(),
            azdo::PipelineContextData::Dict(outputs),
        );
        needs_map.insert(need.0.clone(), azdo::PipelineContextData::Dict(entry));
    }
    if !needs_map.is_empty() {
        message.context_data.insert(
            "needs".to_owned(),
            azdo::PipelineContextData::Dict(needs_map),
        );
    }
    let spec = jobs::load_spec(tx, job.run_id, &job.job_id)?;
    let deferred_environment = spec.as_ref().and_then(|spec| {
        spec.environment.as_ref().and_then(|environment| {
            let name = match environment {
                serde_json::Value::String(name) => Some(name.as_str()),
                serde_json::Value::Object(map) => {
                    map.get("name").and_then(serde_json::Value::as_str)
                }
                _ => None,
            }?;
            preloop_gha_parser::eval::resolves_after_job_build(name).then(|| name.to_owned())
        })
    });
    let runs_on_deferred = logic::runs_on_deferred(&job.runs_on);
    if runs_on_deferred || deferred_environment.is_some() {
        let mut context = preloop_gha_expressions::Context::new();
        for (key, value) in &message.context_data {
            context.insert(key, value.to_json());
        }
        if runs_on_deferred {
            crate::runtime_scheduling::resolve_deferred_runs_on_labels(
                &mut job.runs_on,
                &context,
                &job.run_id,
                &job.job_id.0,
            );
            job.pool_key = compute_pool_key(&job.runs_on, job.runner_group.as_deref());
            tx.prepare_cached(
                "UPDATE jobs SET runs_on = ?3, pool_key = ?4 \
                 WHERE run_id = ?1 AND job_id = ?2",
            )
            .map_err(db)?
            .execute(params![
                run,
                job.job_id.0,
                serde_json::to_string(&job.runs_on).unwrap_or_else(|_| "[]".into()),
                job.pool_key,
            ])
            .map_err(db)?;
        }
        if let Some(name) = deferred_environment.as_deref() {
            // The message carries the environment object only when the job
            // builder resolved (or deferred) one; a deferred name without
            // it can never resolve — the same verdict an eval error lands on.
            if let Some(actions_environment) = message.actions_environment.as_mut() {
                match preloop_gha_parser::eval::resolve_string(name, &context) {
                    Ok(resolved) => {
                        actions_environment.name = resolved.clone();
                        environment_name = Some(resolved);
                    }
                    Err(error) => {
                        // Nothing downstream re-resolves this: the raw
                        // template must not become the deployment's name,
                        // and the gate must not hold the job forever. Stamp
                        // the marker — `evaluate_environment_gate` fails the
                        // job closed on the check just below.
                        tracing::error!(
                            run_id = %job.run_id,
                            job = %job.job_id.0,
                            environment = %name,
                            %error,
                            "deployment environment expression failed to evaluate after needs completed"
                        );
                        job.environment_gate
                            .get_or_insert_with(crate::models::EnvironmentGateState::default)
                            .unresolvable_name = Some(name.to_owned());
                    }
                }
            } else {
                job.environment_gate
                    .get_or_insert_with(crate::models::EnvironmentGateState::default)
                    .unresolvable_name = Some(name.to_owned());
            }
        }
    }
    tx.prepare_cached(
        "UPDATE job_messages SET message_template = ?3 \
         WHERE run_id = ?1 AND job_id = ?2",
    )
    .map_err(db)?
    .execute(params![
        run,
        job.job_id.0,
        serde_json::to_string(&message)
            .map_err(|e| ControlError::backend(anyhow::anyhow!("job message encode: {e}")))?,
    ])
    .map_err(db)?;
    Ok(environment_name)
}

/// `on_job_enqueued`: record dispatch intent for a fresh ready row
/// (`job_assignments`/`provision_requests` under the configured policy).
pub(super) fn on_job_enqueued(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    job: &JobRow,
) -> Result<(), ControlError> {
    let (pool_enabled, require_assignments, _) = backend.config();
    if !pool_enabled && !require_assignments {
        return Ok(());
    }
    let run = codec::run_key(job.run_id);
    let exists = tx
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM job_assignments WHERE run_id = ?1 AND job_id = ?2)",
        )
        .map_err(db)?
        .query_row(params![run, job.job_id.0], |row| row.get::<_, bool>(0))
        .map_err(db)?;
    if exists {
        return Ok(());
    }
    // Drop any pool-pending mark: the job is being assigned now.
    tx.prepare_cached("DELETE FROM provision_requests WHERE run_id = ?1 AND job_id = ?2")
        .map_err(db)?
        .execute(params![run, job.job_id.0])
        .map_err(db)?;
    // A job its namespace would not let start (state or running caps) must
    // not reserve an idle runner; it waits pool-pending like any job with no
    // free runner, and the claim admits it once the namespace does.
    let admitted: bool = tx
        .prepare_cached(&format!(
            "SELECT {} FROM (SELECT ?1 AS namespace_id, ?2 AS pool_key) j",
            crate::control::types::NAMESPACE_ADMITS_CLAIM
        ))
        .map_err(db)?
        .query_row(params![job.namespace_id, job.pool_key], |row| row.get(0))
        .map_err(db)?;

    // Busy runners: claimed assignments plus sessions holding live requests.
    let mut busy: BTreeSet<i64> = BTreeSet::new();
    let mut stmt = tx
        .prepare_cached("SELECT runner_id FROM job_assignments WHERE runner_id IS NOT NULL")
        .map_err(db)?;
    for row in stmt.query_map([], |row| row.get::<_, i64>(0)).map_err(db)? {
        busy.insert(row.map_err(db)?);
    }
    let mut stmt = tx
        .prepare_cached(
            "SELECT DISTINCT q.runner_id FROM job_requests q \
             WHERE q.result IS NULL AND q.runner_id IS NOT NULL",
        )
        .map_err(db)?;
    for row in stmt.query_map([], |row| row.get::<_, i64>(0)).map_err(db)? {
        busy.insert(row.map_err(db)?);
    }

    // Candidate runners: every broker/azdo session's runner, restricted to
    // pool-proven runners when the pool drives assignment.
    let mut candidates: BTreeSet<i64> = BTreeSet::new();
    let mut stmt = tx
        .prepare_cached(
            "SELECT DISTINCT runner_id FROM runner_sessions WHERE runner_id IS NOT NULL",
        )
        .map_err(db)?;
    for row in stmt.query_map([], |row| row.get::<_, i64>(0)).map_err(db)? {
        candidates.insert(row.map_err(db)?);
    }
    if !admitted {
        candidates.clear();
    }
    if pool_enabled {
        candidates.retain(|runner_id| {
            tx.query_row(
                "SELECT pool_proven FROM runners WHERE runner_id = ?1",
                [*runner_id],
                |row| row.get::<_, bool>(0),
            )
            .unwrap_or(false)
        });
    }
    for runner_id in candidates {
        if busy.contains(&runner_id) {
            continue;
        }
        let runner = tx
            .prepare_cached(
                "SELECT labels, runner_group_id, runner_group_name FROM runners \
                 WHERE runner_id = ?1",
            )
            .map_err(db)?
            .query_row([runner_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .optional()
            .map_err(db)?;
        let Some((labels_json, group_id, group_name)) = runner else {
            continue;
        };
        let labels: Vec<String> = serde_json::from_str(&labels_json).unwrap_or_default();
        let caps = crate::models::RunnerCapabilities {
            known: true,
            labels,
            runner_group_id: group_id,
            runner_group_name: group_name,
        };
        let match_row = logic::RunnerMatchRow {
            labels: caps.labels.clone(),
            known: true,
            group_id: caps.runner_group_id,
            group_name: caps.runner_group_name.clone(),
        };
        if !logic::runner_matches(&job.runs_on, job.runner_group.as_deref(), &match_row) {
            continue;
        }
        // First match wins the assignment (candidate set order is runner-id
        // order — deterministic, matching the BTreeMap iteration).
        tx.prepare_cached(
            "INSERT INTO job_assignments (run_id, job_id, runner_id) \
             VALUES (?1, ?2, ?3) ON CONFLICT (run_id, job_id) DO NOTHING",
        )
        .map_err(db)?
        .execute(params![run, job.job_id.0, runner_id])
        .map_err(db)?;
        return Ok(());
    }
    if pool_enabled {
        // No free runner: mark pool-pending so provisioning sees it.
        tx.prepare_cached(
            "INSERT INTO provision_requests (run_id, job_id, namespace_id, pool_key, labels) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT (run_id, job_id) DO NOTHING",
        )
        .map_err(db)?
        .execute(params![
            run,
            job.job_id.0,
            job.namespace_id,
            job.pool_key,
            serde_json::to_string(&job.runs_on).unwrap_or_default()
        ])
        .map_err(db)?;
    }
    Ok(())
}

/// Enqueue one job onto the ready queue: stamps, status, assignment intent,
/// outbox `job.queued.v1`.
fn enqueue_ready(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    job: &JobRow,
    concurrency_acquired: bool,
) -> Result<(), ControlError> {
    let now = now_us();
    tx.prepare_cached(
        "UPDATE jobs SET queue_state = 'ready', status = 'queued', \
             enqueued_at = ?3, deps_ready_at = COALESCE(deps_ready_at, ?3), \
             concurrency_acquired_at = CASE WHEN ?4 THEN COALESCE(concurrency_acquired_at, ?3) ELSE concurrency_acquired_at END \
         WHERE run_id = ?1 AND job_id = ?2",
    )
    .map_err(db)?
    .execute(params![
        codec::run_key(job.run_id),
        job.job_id.0,
        now,
        concurrency_acquired as i64
    ])
    .map_err(db)?;
    let fresh = jobs::JobRow {
        queue_state: "ready".to_owned(),
        ..job.clone()
    };
    on_job_enqueued(tx, backend, &fresh)?;
    jobs::emit_outbox(
        tx,
        &job.namespace_id,
        Some(job.run_id),
        "job.queued.v1",
        serde_json::json!({"job_id": job.job_id.0, "status": "queued"}),
    )?;
    Ok(())
}

/// Settle a job to a terminal status from inside the sweep: status flip,
/// optional request retirement, concurrency release, run re-summarize.
fn settle_job_row(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    job: &JobRow,
    status: ExecutionStatus,
) -> Result<(), ControlError> {
    super::settle::settle_node(tx, backend, job.run_id, &job.job_id, status).map(|_| ())
}

/// The caller's JobSet gate declarations (`caller_jobset_gates`): caller +
/// embedded concurrency, each evaluated in its scope.
fn caller_jobset_gates(
    tx: &Transaction<'_>,
    job: &JobRow,
) -> Result<Result<Vec<cg::JobSetGateDecl>, ExecutionStatus>, ControlError> {
    let spec = jobs::load_spec(tx, job.run_id, &job.job_id)?;
    let (github, submission) = jobs::run_context(tx, job.run_id)?;
    let Some(meta) = spec.and_then(|s| s.reusable_meta) else {
        return Ok(Ok(Vec::new()));
    };
    let mut gates: Vec<cg::JobSetGateDecl> = Vec::new();
    for (raw, scope, label, inputs) in [
        (
            meta.caller_concurrency.as_ref(),
            concurrency::ConcurrencyScope::Job,
            "caller concurrency (JobSet)",
            &submission.inputs,
        ),
        (
            meta.embedded_concurrency.as_ref(),
            concurrency::ConcurrencyScope::Workflow,
            "embedded concurrency (JobSet)",
            &meta.inputs,
        ),
    ] {
        let Some(raw) = raw else { continue };
        let eval_ctx = concurrency::ConcurrencyContext {
            scope,
            github: &github,
            vars: &submission.vars,
            inputs,
            matrix: Some(&spec_matrix(tx, job)?),
            strategy: None,
            needs: None,
        };
        match concurrency::evaluate_concurrency(raw, &eval_ctx) {
            Ok((group, cancel_in_progress, queue)) if !group.trim().is_empty() => {
                let key = concurrency::concurrency_key(&submission.repository, &group);
                let decl = cg::JobSetGateDecl {
                    repository: key.0,
                    group_name: key.1,
                    display_name: group.trim().to_owned(),
                    cancel_in_progress,
                    queue,
                };
                // merge_jobset_gate: same key folds (OR cancel; single wins).
                if let Some(existing) = gates
                    .iter_mut()
                    .find(|g| g.repository == decl.repository && g.group_name == decl.group_name)
                {
                    existing.cancel_in_progress |= decl.cancel_in_progress;
                    if decl.queue == preloop_gha_parser::ConcurrencyQueue::Single {
                        existing.queue = preloop_gha_parser::ConcurrencyQueue::Single;
                    }
                } else {
                    gates.push(decl);
                }
            }
            Ok(_) => return Ok(Err(ExecutionStatus::Failure)),
            Err(error) => {
                concurrency::log_eval_error(label, &error);
                return Ok(Err(ExecutionStatus::Failure));
            }
        }
    }
    gates.sort_by(|l, r| (&l.repository, &l.group_name).cmp(&(&r.repository, &r.group_name)));
    Ok(Ok(gates))
}

fn spec_matrix(
    tx: &Transaction<'_>,
    job: &JobRow,
) -> Result<BTreeMap<String, serde_json::Value>, ControlError> {
    Ok(jobs::load_spec(tx, job.run_id, &job.job_id)?
        .map(|s| s.matrix)
        .unwrap_or_default())
}

/// `advance_jobset_admission`: take the jobset's next unacquired gate;
/// `promoted_key` records a gate the FIFO promotion just granted.
/// Returns `Ready`, `Blocked` (wait row already written), or a settle
/// status for the whole node.
pub(super) enum Advance {
    Ready,
    Blocked,
    Fail(ExecutionStatus),
}

pub(super) fn advance_jobset(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    jobset: i64,
    promoted: Option<(&str, &str, &str)>,
) -> Result<Advance, ControlError> {
    if let Some((ns, repo, group)) = promoted {
        tx.prepare_cached(
            "UPDATE jobset_gates SET acquired = 1 WHERE jobset_id = ?1 \
             AND repository = ?2 AND group_name = ?3",
        )
        .map_err(db)?
        .execute(params![jobset, repo, group])
        .map_err(db)?;
        let _ = ns;
    }
    loop {
        let gate = tx
            .prepare_cached(
                "SELECT gate_index, repository, group_name, display_name, \
                        cancel_in_progress, queue_mode \
                 FROM jobset_gates WHERE jobset_id = ?1 AND acquired = 0 \
                 ORDER BY gate_index LIMIT 1",
            )
            .map_err(db)?
            .query_row([jobset], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })
            .optional()
            .map_err(db)?;
        let Some((gate_index, repo, group, display, cancel_flag, queue_mode)) = gate else {
            // All gates acquired: the jobset is ready.
            tx.prepare_cached("UPDATE jobsets SET state = 'ready' WHERE jobset_id = ?1")
                .map_err(db)?
                .execute([jobset])
                .map_err(db)?;
            return Ok(Advance::Ready);
        };
        // The gate's hold/wait rows live under the caller's namespace —
        // `cg::acquire` must scope to it or it never sees the holder.
        let (run_id_txt, namespace_id): (String, String) = tx
            .prepare_cached(
                "SELECT js.run_id, COALESCE((                    SELECT j.namespace_id FROM jobs j                     WHERE j.run_id = js.run_id                       AND j.job_id = json_extract(js.job_ids, '$[0]')                 ), 'default') FROM jobsets js WHERE js.jobset_id = ?1",
            )
            .map_err(db)?
            .query_row([jobset], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(db)?;
        let run_id = codec::run_id(&run_id_txt);
        let member_ids: BTreeSet<JobId> = {
            let json: String = tx
                .prepare_cached("SELECT job_ids FROM jobsets WHERE jobset_id = ?1")
                .map_err(db)?
                .query_row([jobset], |row| row.get(0))
                .map_err(db)?;
            serde_json::from_str::<Vec<String>>(&json)
                .unwrap_or_default()
                .into_iter()
                .map(JobId)
                .collect()
        };
        let holder = Holder::JobSet {
            run_id,
            job_ids: member_ids,
        };
        let queue = concurrency::queue_mode_from_row(&queue_mode);
        let mut cancel = super::settle::canceler(backend);
        match cg::acquire(
            tx,
            &namespace_id,
            &repo,
            &group,
            &display,
            &holder,
            cancel_flag != 0,
            queue,
            &mut cancel,
        )? {
            cg::AcqOutcome::Acquired => {
                tx.prepare_cached(
                    "UPDATE jobset_gates SET acquired = 1 \
                     WHERE jobset_id = ?1 AND gate_index = ?2",
                )
                .map_err(db)?
                .execute(params![jobset, gate_index])
                .map_err(db)?;
            }
            cg::AcqOutcome::Parked => return Ok(Advance::Blocked),
            cg::AcqOutcome::ArrivalCancelled => {
                return Ok(Advance::Fail(ExecutionStatus::Cancelled));
            }
            cg::AcqOutcome::Failed => return Ok(Advance::Fail(ExecutionStatus::Failure)),
        }
    }
}

/// Register a deferred caller's jobset (jobs row + gates), then advance.
fn ensure_jobset(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    job: &JobRow,
) -> Result<Advance, ControlError> {
    let gates = match caller_jobset_gates(tx, job)? {
        Ok(gates) => gates,
        Err(status) => return Ok(Advance::Fail(status)),
    };
    if gates.is_empty() {
        return Ok(Advance::Ready);
    }
    let member = BTreeSet::from([job.job_id.clone()]);
    let set_id = cg::jobset_id(tx, job.run_id, &member)?;
    for (index, gate) in gates.iter().enumerate() {
        tx.prepare_cached(
            "INSERT INTO jobset_gates (jobset_id, gate_index, repository, \
                 group_name, display_name, cancel_in_progress, queue_mode) \
             VALUES (?1,?2,?3,?4,?5,?6,?7) ON CONFLICT DO NOTHING",
        )
        .map_err(db)?
        .execute(params![
            set_id,
            index as i64,
            gate.repository,
            gate.group_name,
            gate.display_name,
            gate.cancel_in_progress as i64,
            concurrency::queue_mode_row(&gate.queue),
        ])
        .map_err(db)?;
    }
    advance_jobset(tx, backend, set_id, None)
}

/// Whether this job's jobset is already parked (a wait row naming it) —
/// the promote loop leaves it blocked until the gate frees.
fn jobset_parked(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<bool, ControlError> {
    let set_id: Option<i64> = tx
        .prepare_cached(
            "SELECT jobset_id FROM jobsets WHERE run_id = ?1 \
             AND EXISTS (SELECT 1 FROM json_each(jobsets.job_ids) je WHERE je.value = ?2)",
        )
        .map_err(db)?
        .query_row(params![codec::run_key(run_id), job_id.0], |row| row.get(0))
        .optional()
        .map_err(db)?;
    let Some(set_id) = set_id else {
        return Ok(false);
    };
    tx.prepare_cached("SELECT EXISTS(SELECT 1 FROM concurrency_waits WHERE holder_jobset_id = ?1)")
        .map_err(db)?
        .query_row([set_id], |row| row.get(0))
        .map_err(db)
}

/// `promote_ready_jobs` for one run: sweep `queue_state='blocked'` +
/// `remaining_needs=0` candidates until a pass settles nothing. Deferred
/// nodes route to `pending_expansion`; ordinary jobs take the job gate.
/// Every mutation is one conditional write on `jobs`.
pub(super) fn promote_run(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    run_id: RunId,
    outcome: &mut crate::runtime_scheduling::SchedulingOutcome,
) -> Result<(), ControlError> {
    // The environment gate resolves rules against the run's repo + ref.
    let (repository, git_ref): (String, String) = tx
        .prepare_cached("SELECT repository, ref FROM runs WHERE run_id = ?1")
        .map_err(db)?
        .query_row([codec::run_key(run_id)], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .optional()
        .map_err(db)?
        .unwrap_or_default();
    let resolver = backend.environment_resolver();
    loop {
        let mut settled = false;
        // Candidates in job_order: declared needs drained (remaining_needs
        // is the fast-path bound; the decision recomputes from statuses).
        let candidates: Vec<JobRow> = {
            let mut stmt = tx
                .prepare_cached(&format!(
                    "SELECT {} FROM jobs WHERE run_id = ?1 \
                     AND queue_state = 'blocked' AND remaining_needs = 0 \
                     ORDER BY job_order",
                    jobs::JOB_COLUMNS
                ))
                .map_err(db)?;
            let rows = stmt
                .query_map([codec::run_key(run_id)], jobs::job_row)
                .map_err(db)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(db)?
        };
        if candidates.is_empty() {
            return Ok(());
        }
        let platforms = jobs::registered_platforms(tx)?;
        let pool_labels = backend.pool_labels();
        let runner_labels = if pool_labels.is_empty() {
            Vec::new()
        } else {
            jobs::runner_label_sets(tx)?
        };
        let graph = jobs::run_graph(tx, run_id)?;
        for mut job in candidates {
            let needs = jobs::job_needs(tx, run_id, &job.job_id)?;
            let (if_condition, ctx_json) = {
                let spec = jobs::load_spec(tx, run_id, &job.job_id)?;
                let ctx: String = tx
                    .prepare_cached(
                        "SELECT condition_context FROM job_messages \
                         WHERE run_id = ?1 AND job_id = ?2",
                    )
                    .map_err(db)?
                    .query_row(params![codec::run_key(run_id), job.job_id.0], |row| {
                        row.get(0)
                    })
                    .optional()
                    .map_err(db)?
                    .unwrap_or_else(|| "{}".to_owned());
                (spec.and_then(|s| s.if_condition), ctx)
            };
            let condition_context: preloop_gha_expressions::Context =
                serde_json::from_str(&ctx_json).unwrap_or_default();
            let decision =
                dependency_decision(&graph, &needs, if_condition.as_deref(), &condition_context)?;
            if decision == DependencyDecision::Run {
                tx.prepare_cached(
                    "UPDATE jobs SET deps_ready_at = COALESCE(deps_ready_at, ?3) \
                     WHERE run_id = ?1 AND job_id = ?2",
                )
                .map_err(db)?
                .execute(params![codec::run_key(run_id), job.job_id.0, now_us()])
                .map_err(db)?;
            }
            let spec = jobs::load_spec(tx, run_id, &job.job_id)?;
            let reusable = spec.as_ref().and_then(|s| s.reusable_call.clone());
            let deferred = spec.as_ref().and_then(|s| s.deferred_matrix.clone());
            match decision {
                DependencyDecision::Run if reusable.is_some() => {
                    settled = true;
                    // JobSet admission for the caller (gates on the call).
                    let advanced = if jobset_parked(tx, run_id, &job.job_id)? {
                        Advance::Blocked
                    } else {
                        ensure_jobset(tx, backend, &job)?
                    };
                    match advanced {
                        Advance::Ready => {
                            tx.prepare_cached(
                                "UPDATE jobs SET queue_state = 'pending_expansion', \
                                     concurrency_acquired_at = COALESCE(concurrency_acquired_at, ?3) \
                                 WHERE run_id = ?1 AND job_id = ?2",
                            )
                            .map_err(db)?
                            .execute(params![
                                codec::run_key(run_id),
                                job.job_id.0,
                                now_us()
                            ])
                            .map_err(db)?;
                            jobs::emit_outbox(
                                tx,
                                &job.namespace_id,
                                Some(run_id),
                                "expansion.queued.v1",
                                serde_json::json!({"job_id": job.job_id.0}),
                            )?;
                        }
                        Advance::Blocked => {
                            // Parked on the gate: flip to `held` like the
                            // job-gate arm does — leaving the row `blocked`
                            // would re-enter this loop every pass (it stays a
                            // candidate), and `concurrency_waits` is what
                            // actually re-arms it on release.
                            tx.prepare_cached(
                                "UPDATE jobs SET queue_state = 'held', \
                                     status = 'pending', \
                                     concurrency_wait_at = COALESCE(concurrency_wait_at, ?3) \
                                 WHERE run_id = ?1 AND job_id = ?2",
                            )
                            .map_err(db)?
                            .execute(params![codec::run_key(run_id), job.job_id.0, now_us()])
                            .map_err(db)?;
                        }
                        Advance::Fail(status) => {
                            settle_job_row(tx, backend, &job, status)?;
                            outcome.failed.push((run_id, job.job_id.clone()));
                        }
                    }
                }
                DependencyDecision::Run if deferred.is_some() => {
                    settled = true;
                    tx.prepare_cached(
                        "UPDATE jobs SET queue_state = 'pending_expansion' \
                         WHERE run_id = ?1 AND job_id = ?2",
                    )
                    .map_err(db)?
                    .execute(params![codec::run_key(run_id), job.job_id.0])
                    .map_err(db)?;
                    jobs::emit_outbox(
                        tx,
                        &job.namespace_id,
                        Some(run_id),
                        "expansion.queued.v1",
                        serde_json::json!({"job_id": job.job_id.0}),
                    )?;
                }
                DependencyDecision::Run
                    if under_max_parallel(
                        tx,
                        run_id,
                        &job.base_id,
                        spec.as_ref().and_then(|s| s.max_parallel),
                    )? =>
                {
                    let environment_name = hydrate_message(tx, &graph, &mut job)?;
                    // Environment protection rules run ahead of the
                    // satisfiability check: a gate failure is the reason the
                    // job ends, not "no runner could match".
                    if let Some(env_name) = environment_name.as_deref() {
                        let mut gate = job.environment_gate.clone();
                        let lookup = resolver.lookup_sync(&repository, env_name);
                        let verdict = crate::runtime_scheduling::evaluate_environment_gate(
                            &lookup,
                            &git_ref,
                            run_id,
                            &job.job_id,
                            env_name,
                            &mut gate,
                            crate::models::now_unix_nanos(),
                        );
                        match verdict {
                            crate::runtime_scheduling::EnvironmentGateOutcome::Wait => {
                                // Armed gates stamp their progress on the
                                // job row; a `Pending` hold (GitHub rules not
                                // fetched yet) records only the resolved
                                // name — the reaper sweep re-evaluates once
                                // the resolver fills the entry.
                                super::fork_gate::write_gate(
                                    tx,
                                    run_id,
                                    &job.job_id,
                                    gate.clone(),
                                )?;
                                job.environment_gate = gate;
                                tx.prepare_cached(
                                    "UPDATE jobs SET queue_state = 'held', status = 'pending' \
                                     WHERE run_id = ?1 AND job_id = ?2",
                                )
                                .map_err(db)?
                                .execute(params![codec::run_key(run_id), job.job_id.0])
                                .map_err(db)?;
                                continue;
                            }
                            crate::runtime_scheduling::EnvironmentGateOutcome::Failed => {
                                super::fork_gate::write_gate(tx, run_id, &job.job_id, gate)?;
                                settle_job_row(tx, backend, &job, ExecutionStatus::Failure)?;
                                outcome.failed.push((run_id, job.job_id.clone()));
                                settled = true;
                                continue;
                            }
                            crate::runtime_scheduling::EnvironmentGateOutcome::Proceed => {
                                if job.environment_gate.is_some() || gate.is_some() {
                                    super::fork_gate::write_gate(
                                        tx,
                                        run_id,
                                        &job.job_id,
                                        gate.clone(),
                                    )?;
                                    job.environment_gate = gate;
                                }
                            }
                        }
                    }
                    if let Some(reason) = logic::promotion_unsatisfiable_reason(
                        &job.runs_on,
                        platforms.iter().copied(),
                        &pool_labels,
                        runner_labels.iter().any(|labels| {
                            crate::runtime_scheduling::job_matches_runner(&job.runs_on, labels)
                        }),
                    ) {
                        tracing::warn!(
                            %run_id,
                            job = %job.job_id.0,
                            labels = ?job.runs_on,
                            pool_labels = ?pool_labels,
                            %reason,
                            "runs-on unsatisfiable by runner pool; failing the job"
                        );
                        settle_job_row(tx, backend, &job, ExecutionStatus::Failure)?;
                        outcome.failed.push((run_id, job.job_id.clone()));
                        settled = true;
                    } else {
                        match acquire_job_gate(tx, backend, run_id, &job.job_id)? {
                            GateOutcome::Proceed => {
                                enqueue_ready(
                                    tx,
                                    backend,
                                    &job,
                                    spec.as_ref()
                                        .map(|s| s.concurrency.is_some())
                                        .unwrap_or(false),
                                )?;
                                outcome.promoted += 1;
                            }
                            GateOutcome::Parked => {
                                tx.prepare_cached(
                                    "UPDATE jobs SET queue_state = 'held', status = 'pending', \
                                         concurrency_wait_at = COALESCE(concurrency_wait_at, ?3) \
                                     WHERE run_id = ?1 AND job_id = ?2",
                                )
                                .map_err(db)?
                                .execute(params![codec::run_key(run_id), job.job_id.0, now_us()])
                                .map_err(db)?;
                            }
                            GateOutcome::Failed(status) => {
                                settle_job_row(tx, backend, &job, status)?;
                                outcome.failed.push((run_id, job.job_id.clone()));
                                settled = true;
                            }
                        }
                    }
                }
                DependencyDecision::Skip | DependencyDecision::Error => {
                    let status = if decision == DependencyDecision::Skip {
                        ExecutionStatus::Skipped
                    } else {
                        ExecutionStatus::Failure
                    };
                    settle_job_row(tx, backend, &job, status)?;
                    if decision == DependencyDecision::Skip {
                        outcome.skipped.push((run_id, job.job_id.clone()));
                    } else {
                        outcome.failed.push((run_id, job.job_id.clone()));
                    }
                    settled = true;
                }
                // Wait, or Run-but-over-max-parallel: stays blocked.
                _ => {}
            }
        }
        if !settled {
            return Ok(());
        }
    }
}

/// `under_max_parallel` for a decoded row + its spec (gate promotion path).
pub(super) fn under_max_parallel_job(
    tx: &Transaction<'_>,
    job: &JobRow,
    spec: Option<&jobs::SpecRow>,
) -> Result<bool, ControlError> {
    under_max_parallel(
        tx,
        job.run_id,
        &job.base_id,
        spec.and_then(|s| s.max_parallel),
    )
}

/// `enqueue_ready` for the gate-promotion path.
pub(super) fn enqueue_ready_job(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    job: &JobRow,
    concurrency_acquired: bool,
) -> Result<(), ControlError> {
    enqueue_ready(tx, backend, job, concurrency_acquired)
}
