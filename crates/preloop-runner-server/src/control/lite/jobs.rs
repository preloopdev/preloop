//! Row codecs and shared queries for the scheduling commands (`jobs`,
//! `job_specs`, `job_needs`, `job_messages`, `runs`, `run_submissions`).
//!
//! One command = one `BEGIN IMMEDIATE` transaction; these helpers are the
//! statement vocabulary the commands compose. Conditional UPDATEs
//! (`WHERE status = ..` / `queue_state = ..`) carry the transition fencing.

use super::codec::{self, now_us};
use super::db;
use crate::control::types::*;
use crate::models::{QueuedJob, RunRecord};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use rusqlite::{params, OptionalExtension, Transaction};
use std::collections::{BTreeMap, BTreeSet};

/// A `jobs` row decoded for scheduling decisions.
#[derive(Debug, Clone)]
pub(super) struct JobRow {
    pub(super) run_id: RunId,
    pub(super) job_id: JobId,
    pub(super) namespace_id: String,
    pub(super) kind: String,
    pub(super) parent_job_id: Option<String>,
    pub(super) base_id: String,
    pub(super) status: ExecutionStatus,
    pub(super) queue_state: String,
    pub(super) remaining_needs: i32,
    pub(super) pool_key: String,
    pub(super) runs_on: Vec<String>,
    pub(super) runner_group: Option<String>,
    pub(super) priority: i64,
    pub(super) run_order: i64,
    pub(super) job_order: i64,
    pub(super) enqueued_at: Option<i64>,
    pub(super) claimed_by_runner_id: Option<i64>,
    pub(super) expand_generation: i64,
    pub(super) deps_ready_at: Option<i64>,
    pub(super) concurrency_wait_at: Option<i64>,
    pub(super) concurrency_acquired_at: Option<i64>,
    pub(super) started_at: Option<i64>,
}

pub(super) const JOB_COLUMNS: &str = "run_id, job_id, namespace_id, kind, \
     parent_job_id, base_id, status, queue_state, remaining_needs, pool_key, \
     runs_on, runner_group, priority, run_order, job_order, enqueued_at, \
     claimed_by_runner_id, expand_generation, deps_ready_at, \
     concurrency_wait_at, concurrency_acquired_at, started_at";

pub(super) fn job_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<JobRow> {
    Ok(JobRow {
        run_id: codec::run_id(&row.get::<_, String>(0)?),
        job_id: JobId(row.get(1)?),
        namespace_id: row.get(2)?,
        kind: row.get(3)?,
        parent_job_id: row.get(4)?,
        base_id: row.get(5)?,
        status: status_parse(&row.get::<_, String>(6)?),
        queue_state: row.get(7)?,
        remaining_needs: row.get(8)?,
        pool_key: row.get(9)?,
        runs_on: serde_json::from_str(&row.get::<_, String>(10)?).unwrap_or_default(),
        runner_group: row.get(11)?,
        priority: row.get(12)?,
        run_order: row.get(13)?,
        job_order: row.get(14)?,
        enqueued_at: row.get(15)?,
        claimed_by_runner_id: row.get(16)?,
        expand_generation: row.get(17)?,
        deps_ready_at: row.get(18)?,
        concurrency_wait_at: row.get(19)?,
        concurrency_acquired_at: row.get(20)?,
        started_at: row.get(21)?,
    })
}

/// The immutable spec of one job (`job_specs`), joined fields decoded.
#[derive(Debug, Clone)]
pub(super) struct SpecRow {
    pub(super) display_name: String,
    pub(super) if_condition: Option<String>,
    pub(super) matrix: BTreeMap<String, serde_json::Value>,
    pub(super) deferred_matrix: Option<String>,
    pub(super) max_parallel: Option<u64>,
    pub(super) environment: Option<serde_json::Value>,
    pub(super) concurrency: Option<preloop_gha_parser::Concurrency>,
    pub(super) reusable_call: Option<preloop_gha_protocol::ReusableCallPlan>,
    /// Caller bookkeeping (`ReusableCallMetadata` JSON) — not the call plan.
    pub(super) reusable_meta: Option<preloop_gha_parser::ReusableCallMetadata>,
    /// The deferred caller's own `JobPlan` (expansion input).
    pub(super) caller_plan: Option<preloop_gha_protocol::JobPlan>,
    pub(super) fail_fast: Option<bool>,
    pub(super) continue_on_error: Option<bool>,
    pub(super) id_token_granted: bool,
    pub(super) oidc_context: Option<crate::state::OidcJobContext>,
}

/// `job_specs.reusable_call` stores a tagged envelope: the `uses:` plan for
/// expansion (`"call"`), the caller metadata the run record projects
/// (`"meta"`), or the caller `JobPlan` an expandable node rebuilds from
/// (`"plan"`). One column, three roles — a caller node writes all it has.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub(super) enum ReusableSpec {
    Full {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call: Option<preloop_gha_protocol::ReusableCallPlan>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        meta: Option<preloop_gha_parser::ReusableCallMetadata>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        plan: Option<preloop_gha_protocol::JobPlan>,
    },
    Call(preloop_gha_protocol::ReusableCallPlan),
}

impl ReusableSpec {
    pub(super) fn encode(
        call: Option<&preloop_gha_protocol::ReusableCallPlan>,
        meta: Option<&preloop_gha_parser::ReusableCallMetadata>,
        plan: Option<&preloop_gha_protocol::JobPlan>,
    ) -> Option<String> {
        if call.is_none() && meta.is_none() && plan.is_none() {
            return None;
        }
        serde_json::to_string(&ReusableSpec::Full {
            call: call.cloned(),
            meta: meta.cloned(),
            plan: plan.cloned(),
        })
        .ok()
    }

    pub(super) fn decode(json: &str) -> Self {
        serde_json::from_str(json).unwrap_or(ReusableSpec::Full {
            call: None,
            meta: None,
            plan: None,
        })
    }
}

fn opt_json<'a, T: serde::de::DeserializeOwned>(value: &'a Option<String>) -> Option<T> {
    value.as_deref().and_then(|v| serde_json::from_str(v).ok())
}

pub(super) fn spec_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SpecRow> {
    let reusable_json: Option<String> = row.get(7)?;
    let reusable = reusable_json.as_deref().map(ReusableSpec::decode);
    let (call, meta, plan) = match reusable {
        Some(ReusableSpec::Full { call, meta, plan }) => (call, meta, plan),
        Some(ReusableSpec::Call(call)) => (Some(call), None, None),
        None => (None, None, None),
    };
    let oidc_env: Option<String> = row.get(11)?;
    let oidc_ref: Option<String> = row.get(12)?;
    let oidc_sha: Option<String> = row.get(13)?;
    #[allow(clippy::redundant_clone)]
    let oidc_context = match (oidc_env, oidc_ref, oidc_sha) {
        (environment, reference, sha) => {
            (reference.is_some() || sha.is_some()).then_some(crate::state::OidcJobContext {
                environment,
                job_workflow_ref: reference,
                job_workflow_sha: sha,
            })
        }
    };
    Ok(SpecRow {
        display_name: row.get(0)?,
        if_condition: row.get(1)?,
        matrix: serde_json::from_str(&row.get::<_, String>(2)?).unwrap_or_default(),
        deferred_matrix: row.get(3)?,
        max_parallel: row.get::<_, Option<i64>>(4)?.and_then(|v| u64::try_from(v).ok()),
        environment: opt_json(&row.get(5)?),
        concurrency: opt_json(&row.get(6)?),
        reusable_call: call,
        reusable_meta: meta,
        caller_plan: plan,
        fail_fast: row.get::<_, Option<i64>>(8)?.map(|v| v != 0),
        continue_on_error: row.get::<_, Option<i64>>(9)?.map(|v| v != 0),
        id_token_granted: row.get::<_, i64>(10)? != 0,
        oidc_context,
    })
}

pub(super) const SPEC_COLUMNS: &str = "display_name, if_condition, matrix, \
     deferred_matrix, max_parallel, environment, concurrency, reusable_call, \
     fail_fast, continue_on_error, id_token_granted, oidc_environment, \
     oidc_job_workflow_ref, oidc_job_workflow_sha";

/// Load one job's spec (`None` when the job row exists but has no spec —
/// caller placeholders always carry one).
pub(super) fn load_spec(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<Option<SpecRow>, ControlError> {
    tx.prepare_cached(&format!(
        "SELECT {SPEC_COLUMNS} FROM job_specs WHERE run_id = ?1 AND job_id = ?2"
    ))
    .map_err(db)?
    .query_row(params![codec::run_key(run_id), job_id.0], spec_row)
    .optional()
    .map_err(db)
}

/// Declared `needs:` of one job, in declaration order.
pub(super) fn job_needs(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<Vec<JobId>, ControlError> {
    let mut stmt = tx
        .prepare_cached(
            "SELECT needs_job_id FROM job_needs WHERE run_id = ?1 AND job_id = ?2 \
             ORDER BY position",
        )
        .map_err(db)?;
    let rows = stmt
        .query_map(params![codec::run_key(run_id), job_id.0], |row| {
            Ok(JobId(row.get::<_, String>(0)?))
        })
        .map_err(db)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(db)
}

/// Rebuild the runnable view of one job (`QueuedJob`). `message` is the
/// stored template — secret values and tokens are absent by construction.
pub(super) fn queued_job(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<Option<QueuedJob>, ControlError> {
    let run_key = codec::run_key(run_id);
    let Some(job) = tx
        .prepare_cached(&format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE run_id = ?1 AND job_id = ?2"
        ))
        .map_err(db)?
        .query_row(params![run_key, job_id.0], job_row)
        .optional()
        .map_err(db)?
    else {
        return Ok(None);
    };
    Ok(Some(queued_job_of(tx, job)?))
}

/// Rebuild a `QueuedJob` from its decoded `jobs` row (spec + message +
/// needs joins).
pub(super) fn queued_job_of(
    tx: &Transaction<'_>,
    job: JobRow,
) -> Result<QueuedJob, ControlError> {
    let spec = load_spec(tx, job.run_id, &job.job_id)?.unwrap_or_else(|| SpecRow {
        display_name: job.job_id.0.clone(),
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
    let (template, context_json) = tx
        .prepare_cached(
            "SELECT message_template, condition_context FROM job_messages \
             WHERE run_id = ?1 AND job_id = ?2",
        )
        .map_err(db)?
        .query_row(params![codec::run_key(job.run_id), job.job_id.0], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .optional()
        .map_err(db)?
        .unwrap_or_else(|| ("{}".to_owned(), "{}".to_owned()));
    let message: preloop_gha_protocol::azdo::AgentJobRequestMessage =
        serde_json::from_str(&template)
            .map_err(|e| ControlError::backend(anyhow::anyhow!("job message decode: {e}")))?;
    let condition_context: preloop_gha_expressions::Context =
        serde_json::from_str(&context_json).unwrap_or_default();
    let needs = job_needs(tx, job.run_id, &job.job_id)?;
    Ok(QueuedJob {
        run_id: job.run_id,
        job_id: job.job_id.clone(),
        base_id: job.base_id,
        created_at_unix_nanos: job.run_order * 1000,
        dependencies_ready_at_unix_nanos: job.deps_ready_at.map(|us| us * 1000),
        concurrency_wait_started_at_unix_nanos: job.concurrency_wait_at.map(|us| us * 1000),
        concurrency_acquired_at_unix_nanos: job.concurrency_acquired_at.map(|us| us * 1000),
        enqueued_at_unix_nanos: job.enqueued_at.map(|us| us * 1000).unwrap_or(0),
        needs,
        if_condition: spec.if_condition,
        condition_context,
        max_parallel: spec.max_parallel,
        runs_on: job.runs_on,
        runner_group: job.runner_group,
        message,
        environment: spec.environment,
        concurrency: spec.concurrency,
        matrix: spec.matrix,
        deferred_matrix: spec.deferred_matrix,
        reusable_call: spec.reusable_call,
    })
}

/// Insert one job row plus its spec, needs edges, and message template.
/// `queue_state`/`status` are the classified values; `remaining_needs` is
/// the declared-need count (a need on a matrix base counts once — the edge
/// settles when every matching leg is terminal).
#[allow(clippy::too_many_arguments)]
pub(super) fn insert_job(
    tx: &Transaction<'_>,
    run_id: RunId,
    namespace_id: &str,
    job: &QueuedJob,
    kind: &str,
    parent_job_id: Option<&str>,
    display_name: &str,
    display_order: i64,
    status: ExecutionStatus,
    queue_state: &str,
    remaining_needs: i32,
    run_order: i64,
    job_order: i64,
    spec_extras: &SpecExtras<'_>,
) -> Result<(), ControlError> {
    let now = now_us();
    tx.prepare_cached(
        "INSERT INTO jobs (run_id, job_id, namespace_id, kind, parent_job_id, \
             base_id, status, queue_state, remaining_needs, pool_key, runs_on, \
             runner_group, priority, run_order, job_order, enqueued_at, \
             deps_ready_at, concurrency_wait_at, concurrency_acquired_at, \
             created_at) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,0,?13,?14,?15,?16,?17,?18,?19) \
         ON CONFLICT (run_id, job_id) DO NOTHING",
    )
    .map_err(db)?
    .execute(params![
        codec::run_key(run_id),
        job.job_id.0,
        namespace_id,
        kind,
        parent_job_id,
        job.base_id,
        status_str(status),
        queue_state,
        remaining_needs,
        compute_pool_key(&job.runs_on, job.runner_group.as_deref()),
        serde_json::to_string(&job.runs_on).unwrap_or_default(),
        job.runner_group,
        run_order,
        job_order,
        job.enqueued_at_unix_nanos.checked_div(1000).filter(|v| *v > 0),
        job.dependencies_ready_at_unix_nanos.map(|v| v / 1000),
        job.concurrency_wait_started_at_unix_nanos.map(|v| v / 1000),
        job.concurrency_acquired_at_unix_nanos.map(|v| v / 1000),
        now,
    ])
    .map_err(db)?;
    tx.prepare_cached(
        "INSERT INTO job_specs (run_id, job_id, display_name, display_order, \
             if_condition, matrix, deferred_matrix, max_parallel, environment, \
             concurrency, reusable_call, fail_fast, continue_on_error, \
             id_token_granted, oidc_environment, oidc_job_workflow_ref, \
             oidc_job_workflow_sha) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
    )
    .map_err(db)?
    .execute(params![
        codec::run_key(run_id),
        job.job_id.0,
        display_name,
        display_order,
        job.if_condition,
        serde_json::to_string(&job.matrix).unwrap_or_else(|_| "{}".to_owned()),
        job.deferred_matrix,
        job.max_parallel.map(|v| v as i64),
        job.environment.as_ref().map(|v| v.to_string()),
        job.concurrency
            .as_ref()
            .map(|c| serde_json::to_string(c).unwrap_or_default()),
        spec_extras.reusable_call_json,
        spec_extras.fail_fast.map(|v| v as i64),
        spec_extras.continue_on_error.map(|v| v as i64),
        spec_extras.id_token_granted as i64,
        spec_extras.oidc_environment,
        spec_extras.oidc_job_workflow_ref,
        spec_extras.oidc_job_workflow_sha,
    ])
    .map_err(db)?;
    for (position, need) in job.needs.iter().enumerate() {
        tx.prepare_cached(
            "INSERT INTO job_needs (run_id, job_id, needs_job_id, position) \
             VALUES (?1,?2,?3,?4) ON CONFLICT DO NOTHING",
        )
        .map_err(db)?
        .execute(params![
            codec::run_key(run_id),
            job.job_id.0,
            need.0,
            position as i64
        ])
        .map_err(db)?;
    }
    Ok(())
}

/// Per-job spec fields not carried by `QueuedJob` (caller metadata, OIDC).
#[derive(Default)]
pub(super) struct SpecExtras<'a> {
    pub(super) reusable_call_json: Option<String>,
    pub(super) fail_fast: Option<bool>,
    pub(super) continue_on_error: Option<bool>,
    pub(super) id_token_granted: bool,
    pub(super) oidc_environment: Option<&'a str>,
    pub(super) oidc_job_workflow_ref: Option<&'a str>,
    pub(super) oidc_job_workflow_sha: Option<&'a str>,
}

/// Persist the job's message template + `if:` context (`job_messages`).
pub(super) fn insert_job_message(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    message: &preloop_gha_protocol::azdo::AgentJobRequestMessage,
    condition_context: &preloop_gha_expressions::Context,
) -> Result<(), ControlError> {
    tx.prepare_cached(
        "INSERT INTO job_messages (run_id, job_id, message_template, \
             secret_names, condition_context) VALUES (?1,?2,?3,'[]',?4) \
         ON CONFLICT (run_id, job_id) DO UPDATE SET \
             message_template = excluded.message_template, \
             condition_context = excluded.condition_context",
    )
    .map_err(db)?
    .execute(params![
        codec::run_key(run_id),
        job_id.0,
        serde_json::to_string(message)
            .map_err(|e| ControlError::backend(anyhow::anyhow!("job message encode: {e}")))?,
        serde_json::to_string(condition_context)
            .map_err(|e| ControlError::backend(anyhow::anyhow!("context encode: {e}")))?,
    ])
    .map_err(db)?;
    Ok(())
}

/// Rewrite only the message template (promotion hydration); leaves the
/// `if:` context untouched.
pub(super) fn update_job_message(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    message: &preloop_gha_protocol::azdo::AgentJobRequestMessage,
) -> Result<(), ControlError> {
    tx.prepare_cached(
        "UPDATE job_messages SET message_template = ?3 \
         WHERE run_id = ?1 AND job_id = ?2",
    )
    .map_err(db)?
    .execute(params![
        codec::run_key(run_id),
        job_id.0,
        serde_json::to_string(message)
            .map_err(|e| ControlError::backend(anyhow::anyhow!("job message encode: {e}")))?,
    ])
    .map_err(db)?;
    Ok(())
}

/// One conditional status write (`jobs.status` + `queue_state`), the
/// transition primitive every command composes. `expected` pairs are the
/// `WHERE` fencing; `None` disables a clause.
pub(super) fn set_job_status(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    status: ExecutionStatus,
    queue_state: &str,
) -> Result<(), ControlError> {
    tx.prepare_cached(
        "UPDATE jobs SET status = ?3, queue_state = ?4 \
         WHERE run_id = ?1 AND job_id = ?2",
    )
    .map_err(db)?
    .execute(params![
        codec::run_key(run_id),
        job_id.0,
        status_str(status),
        queue_state
    ])
    .map_err(db)?;
    Ok(())
}

/// The run's job status map (`jobs.status` per job id).
pub(super) fn job_statuses(
    tx: &Transaction<'_>,
    run_id: RunId,
) -> Result<BTreeMap<JobId, ExecutionStatus>, ControlError> {
    let mut stmt = tx
        .prepare_cached("SELECT job_id, status FROM jobs WHERE run_id = ?1")
        .map_err(db)?;
    let rows = stmt
        .query_map([codec::run_key(run_id)], |row| {
            Ok((
                JobId(row.get::<_, String>(0)?),
                status_parse(&row.get::<_, String>(1)?),
            ))
        })
        .map_err(db)?;
    rows.collect::<Result<BTreeMap<_, _>, _>>().map_err(db)
}

/// Ready-queue jobs in claim order (`priority DESC, run_order, job_order`).
pub(super) fn ready_jobs(tx: &Transaction<'_>) -> Result<Vec<JobRow>, ControlError> {
    let mut stmt = tx
        .prepare_cached(&format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE queue_state = 'ready' \
             ORDER BY priority DESC, run_order, job_order"
        ))
        .map_err(db)?;
    let rows = stmt.query_map([], job_row).map_err(db)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(db)
}

/// Global ready-queue depth (the gauge commands report back).
pub(super) fn ready_count(tx: &Transaction<'_>) -> Result<usize, ControlError> {
    tx.prepare_cached("SELECT COUNT(*) FROM jobs WHERE queue_state = 'ready'")
        .map_err(db)?
        .query_row([], |row| row.get::<_, i64>(0))
        .map(|n| n as usize)
        .map_err(db)
}

/// `runs-on` labels of the ready-queue front, in claim order.
pub(super) fn next_ready_labels(tx: &Transaction<'_>) -> Result<Vec<String>, ControlError> {
    tx.prepare_cached(
        "SELECT runs_on FROM jobs WHERE queue_state = 'ready' \
         ORDER BY priority DESC, run_order, job_order LIMIT 1",
    )
    .map_err(db)?
    .query_row([], |row| row.get::<_, String>(0))
    .optional()
    .map_err(db)?
    .map(|json| serde_json::from_str(&json).unwrap_or_default())
    .map_or_else(|| Ok(Vec::new()), Ok)
}

/// Whether any ready or undelivered-cancellation work exists (the wake
/// decision).
pub(super) fn queue_nonempty(tx: &Transaction<'_>) -> Result<bool, ControlError> {
    tx.prepare_cached(
        "SELECT EXISTS(SELECT 1 FROM jobs WHERE queue_state = 'ready') \
            OR EXISTS(SELECT 1 FROM job_cancellations WHERE delivered_at IS NULL)",
    )
    .map_err(db)?
    .query_row([], |row| row.get(0))
    .map_err(db)
}

/// Every status-bearing fact `dependency_decision` needs for one job: the
/// run's job map (leaves only — expanded parents are excluded by having
/// children), plus declared needs edges per job for ancestor traversal.
pub(super) struct RunGraph {
    pub(super) statuses: BTreeMap<JobId, ExecutionStatus>,
    pub(super) base_ids: BTreeMap<JobId, String>,
    pub(super) needs: BTreeMap<JobId, Vec<JobId>>,
    pub(super) outputs: BTreeMap<JobId, BTreeMap<String, serde_json::Value>>,
}

/// Load the run's dependency graph. An expanded `matrix_parent` is excluded
/// from the status map — its legs replaced it — but stays in `job_needs`
/// edges so dependents of the parent resolve through the legs. A
/// `reusable_caller` stays: it is a real node (InProgress while its callee
/// runs, then its aggregate status).
pub(super) fn run_graph(tx: &Transaction<'_>, run_id: RunId) -> Result<RunGraph, ControlError> {
    let run = codec::run_key(run_id);
    let mut stmt = tx
        .prepare_cached(
            "SELECT job_id, status, base_id, outputs FROM jobs WHERE run_id = ?1 \
             AND NOT (kind = 'matrix_parent' AND EXISTS ( \
                 SELECT 1 FROM jobs c WHERE c.run_id = jobs.run_id \
                 AND c.parent_job_id = jobs.job_id))",
        )
        .map_err(db)?;
    let rows = stmt
        .query_map([&run], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(db)?;
    let mut statuses = BTreeMap::new();
    let mut base_ids = BTreeMap::new();
    let mut outputs = BTreeMap::new();
    for row in rows {
        let (job_id, status, base_id, out) = row.map_err(db)?;
        let id = JobId(job_id);
        statuses.insert(id.clone(), status_parse(&status));
        base_ids.insert(id.clone(), base_id);
        if let Some(json) = out {
            if let Ok(map) = serde_json::from_str::<BTreeMap<String, serde_json::Value>>(&json) {
                outputs.insert(id, map);
            }
        }
    }
    let mut stmt = tx
        .prepare_cached("SELECT job_id, needs_job_id FROM job_needs WHERE run_id = ?1 ORDER BY position")
        .map_err(db)?;
    let rows = stmt
        .query_map([&run], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(db)?;
    let mut needs: BTreeMap<JobId, Vec<JobId>> = BTreeMap::new();
    for row in rows {
        let (job_id, need) = row.map_err(db)?;
        needs.entry(JobId(job_id)).or_default().push(JobId(need));
    }
    Ok(RunGraph {
        statuses,
        base_ids,
        needs,
        outputs,
    })
}

impl RunGraph {
    /// `matching_need_ids`: the declared need expands to every leaf job
    /// with that id or base id.
    pub(super) fn matching(&self, need: &JobId) -> Vec<JobId> {
        self.statuses
            .keys()
            .filter(|job_id| {
                **job_id == *need || self.base_ids.get(*job_id).is_some_and(|b| b == &need.0)
            })
            .cloned()
            .collect()
    }

    /// `matching_need_statuses`.
    pub(super) fn matching_statuses(&self, need: &JobId) -> Vec<ExecutionStatus> {
        self.matching(need)
            .iter()
            .filter_map(|id| self.statuses.get(id).copied())
            .collect()
    }

    /// `ancestor_statuses`: transitively expand needs, collecting statuses.
    pub(super) fn ancestor_statuses(&self, needs: &[JobId]) -> Vec<ExecutionStatus> {
        let mut pending: Vec<JobId> = needs
            .iter()
            .flat_map(|need| self.matching(need))
            .collect();
        let mut visited = BTreeSet::new();
        let mut statuses = Vec::new();
        while let Some(job_id) = pending.pop() {
            if !visited.insert(job_id.clone()) {
                continue;
            }
            if let Some(status) = self.statuses.get(&job_id) {
                statuses.push(*status);
            }
            if let Some(next) = self.needs.get(&job_id) {
                pending.extend(next.iter().flat_map(|need| self.matching(need)));
            }
        }
        statuses
    }
}

/// The platform labels every registered runner hosts (for
/// `unhostable_platform`), deduplicated.
pub(super) fn registered_platforms(tx: &Transaction<'_>) -> Result<Vec<&'static str>, ControlError> {
    let mut stmt = tx
        .prepare_cached("SELECT labels FROM runners")
        .map_err(db)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(db)?;
    let mut platforms = Vec::new();
    for row in rows {
        let labels: Vec<String> = serde_json::from_str(&row.map_err(db)?).unwrap_or_default();
        for label in labels {
            let os = match label.to_ascii_lowercase().as_str() {
                "linux" => Some("linux"),
                "macos" => Some("macos"),
                "windows" => Some("windows"),
                _ => None,
            };
            if let Some(os) = os {
                if !platforms.contains(&os) {
                    platforms.push(os);
                }
            }
        }
    }
    Ok(platforms)
}

/// Every registered runner's label set (the reaper's starvation check).
pub(super) fn runner_label_sets(tx: &Transaction<'_>) -> Result<Vec<Vec<String>>, ControlError> {
    let mut stmt = tx
        .prepare_cached("SELECT labels FROM runners")
        .map_err(db)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(db)?;
    let mut out = Vec::new();
    for row in rows {
        out.push(serde_json::from_str(&row.map_err(db)?).unwrap_or_default());
    }
    Ok(out)
}

/// A run's `(github_context, submission)` for gate evaluation.
pub(super) fn run_context(
    tx: &Transaction<'_>,
    run_id: RunId,
) -> Result<(serde_json::Value, preloop_gha_protocol::WorkflowSubmission), ControlError> {
    let (github_json, submission_json) = tx
        .prepare_cached(
            "SELECT github_context, submission FROM run_submissions WHERE run_id = ?1",
        )
        .map_err(db)?
        .query_row([codec::run_key(run_id)], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(db)?;
    let github = serde_json::from_str(&github_json)
        .map_err(|e| ControlError::backend(anyhow::anyhow!("github context decode: {e}")))?;
    let submission: preloop_gha_protocol::WorkflowSubmission =
        serde_json::from_str(&submission_json).map_err(|e| {
            ControlError::backend(anyhow::anyhow!("submission decode for {run_id}: {e}"))
        })?;
    Ok((github, submission))
}

/// Recompute `runs.status`/`conclusion`/`completed_at`/`started_at` from the
/// leaf job rows — `summarize_run` + `finalize_run_if_complete` in SQL.
///
/// `held` marks a run parked behind a workflow-level gate: the summary
/// stays `queued` (`RunRecord.status = Pending` is reconstructed on read).
/// Returns the run's terminal status when it finalized this write.
pub(super) fn summarize_run_row(
    tx: &Transaction<'_>,
    run_id: RunId,
) -> Result<ExecutionStatus, ControlError> {
    let graph = run_graph(tx, run_id)?;
    let status = crate::runtime_scheduling::summarize_run(graph.statuses.values().copied());
    let now = now_us();
    let terminal = matches!(
        status,
        ExecutionStatus::Success
            | ExecutionStatus::Failure
            | ExecutionStatus::Cancelled
            | ExecutionStatus::Skipped
    ) && graph.statuses.values().all(|s| s.is_terminal())
        && !graph.statuses.is_empty();
    if terminal {
        tx.prepare_cached(
            "UPDATE runs SET status = 'completed', conclusion = ?2, \
                 started_at = COALESCE(started_at, ?3), completed_at = COALESCE(completed_at, ?3) \
             WHERE run_id = ?1",
        )
        .map_err(db)?
        .execute(params![
            codec::run_key(run_id),
            crate::runtime_scheduling::status_string(status),
            now
        ])
        .map_err(db)?;
    } else {
        let wire = match status {
            ExecutionStatus::InProgress => "in_progress",
            _ => "queued",
        };
        tx.prepare_cached(
            "UPDATE runs SET status = ?2, started_at = COALESCE(started_at, ?3) \
             WHERE run_id = ?1 AND status <> 'completed'",
        )
        .map_err(db)?
        .execute(params![codec::run_key(run_id), wire, now])
        .map_err(db)?;
    }
    Ok(status)
}

/// Write an `outbox_events` row and stamp `run_seq` from the run's
/// incremented `event_seq` (contract rule 9). `payload` is ids/states only.
pub(super) fn emit_outbox(
    tx: &Transaction<'_>,
    namespace_id: &str,
    run_id: Option<RunId>,
    topic: &str,
    payload: serde_json::Value,
) -> Result<(), ControlError> {
    let run_seq = match run_id {
        Some(run_id) => {
            let seq: i64 = tx
                .prepare_cached(
                    "UPDATE runs SET event_seq = event_seq + 1 WHERE run_id = ?1 \
                     RETURNING event_seq",
                )
                .map_err(db)?
                .query_row([codec::run_key(run_id)], |row| row.get(0))
                .optional()
                .map_err(db)?
                .unwrap_or(0);
            Some(seq)
        }
        None => None,
    };
    tx.prepare_cached(
        "INSERT INTO outbox_events (namespace_id, run_id, run_seq, topic, payload) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
    )
    .map_err(db)?
    .execute(params![
        namespace_id,
        run_id.map(|r| codec::run_key(r)),
        run_seq,
        topic,
        payload.to_string(),
    ])
    .map_err(db)?;
    Ok(())
}

/// Rebuild the public `RunRecord`: `runs` scalars + `run_submissions` +
/// the leaf `jobs` status map + spec joins (`project_run_rows` shape).
pub(super) fn run_record(
    tx: &Transaction<'_>,
    run_id: RunId,
) -> Result<Option<RunRecord>, ControlError> {
    let run = codec::run_key(run_id);
    let archived = tx
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM run_history WHERE run_id = ?1)")
        .map_err(db)?
        .query_row([&run], |row| row.get::<_, bool>(0))
        .map_err(db)?;
    // An archived run's live row is gone: the head read falls back to
    // `run_history` (decision Q10), the job projection to `job_history`.
    let head = if archived {
        tx.prepare_cached(
            "SELECT 'completed', r.conclusion, r.run_number, r.run_attempt, \
                    r.run_name, r.event, r.head_sha, r.workflow_path, \
                    NULL, r.created_at, r.started_at, r.completed_at, \
                    r.workflow_path, r.submission, NULL, NULL, NULL \
             FROM run_history r WHERE r.run_id = ?1",
        )
        .map_err(db)?
        .query_row([&run], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, i64>(9)?,
                row.get::<_, Option<i64>>(10)?,
                row.get::<_, Option<i64>>(11)?,
                row.get::<_, String>(12)?,
                row.get::<_, Option<String>>(13)?,
                row.get::<_, Option<String>>(14)?,
                row.get::<_, Option<String>>(15)?,
                row.get::<_, Option<String>>(16)?,
            ))
        })
        .optional()
        .map_err(db)?
    } else {
        tx.prepare_cached(
            "SELECT r.status, r.conclusion, r.run_number, r.run_attempt, \
                    r.run_name, r.event, r.head_sha, r.workflow_ref, \
                    r.webhook_delivery_id, r.created_at, r.started_at, r.completed_at, \
                    r.workflow_path, s.submission, s.github_context, \
                    s.workspace_snapshot, s.snapshot_timing \
             FROM runs r LEFT JOIN run_submissions s ON s.run_id = r.run_id \
             WHERE r.run_id = ?1",
        )
        .map_err(db)?
        .query_row([&run], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, i64>(9)?,
                row.get::<_, Option<i64>>(10)?,
                row.get::<_, Option<i64>>(11)?,
                row.get::<_, String>(12)?,
                row.get::<_, Option<String>>(13)?,
                row.get::<_, Option<String>>(14)?,
                row.get::<_, Option<String>>(15)?,
                row.get::<_, Option<String>>(16)?,
            ))
        })
        .optional()
        .map_err(db)?
    };
    let Some((
        status,
        conclusion,
        run_number,
        run_attempt,
        run_name,
        event,
        head_sha,
        workflow_ref,
        delivery_id,
        created_at,
        started_at,
        completed_at,
        workflow_path,
        submission_json,
        github_json,
        snapshot_json,
        timing_json,
    )) = head
    else {
        return Ok(None);
    };
    let submission: preloop_gha_protocol::WorkflowSubmission =
        serde_json::from_str(&submission_json.unwrap_or_else(|| "{}".to_owned()))
            .map_err(|e| ControlError::backend(anyhow::anyhow!("submission decode: {e}")))?;
    let github: serde_json::Value =
        serde_json::from_str(&github_json.unwrap_or_else(|| "{}".to_owned()))
            .unwrap_or_else(|_| serde_json::json!({}));
    // Leaf statuses, plus held detection (a 'held' job of a holder_kind
    // 'run' wait means the whole run parks — RunRecord.status = Pending).
    let graph = if archived {
        // job_history carries kind/parent_job_id, so the matrix-parent
        // exclusion still applies; needs edges were never archived.
        let mut stmt = tx
            .prepare_cached(
                "SELECT job_id, status, base_id, outputs FROM job_history h \
                 WHERE h.run_id = ?1 AND h.run_created_at = ( \
                     SELECT MAX(run_created_at) FROM job_history \
                     WHERE run_id = ?1) \
                 AND NOT (h.kind = 'matrix_parent' AND EXISTS ( \
                     SELECT 1 FROM job_history c WHERE c.run_id = h.run_id \
                     AND c.run_created_at = h.run_created_at \
                     AND c.parent_job_id = h.job_id))",
            )
            .map_err(db)?;
        let rows = stmt
            .query_map([&run], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .map_err(db)?;
        let mut statuses = BTreeMap::new();
        let mut base_ids = BTreeMap::new();
        let mut outputs = BTreeMap::new();
        for row in rows {
            let (job_id, status, base_id, out) = row.map_err(db)?;
            let id = JobId(job_id);
            statuses.insert(id.clone(), status_parse(&status));
            base_ids.insert(id.clone(), base_id);
            if let Some(json) = out {
                if let Ok(map) =
                    serde_json::from_str::<BTreeMap<String, serde_json::Value>>(&json)
                {
                    outputs.insert(id, map);
                }
            }
        }
        RunGraph {
            statuses,
            base_ids,
            needs: BTreeMap::new(),
            outputs,
        }
    } else {
        run_graph(tx, run_id)?
    };
    let held = !archived
        && tx
            .prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM concurrency_waits w \
                 WHERE w.holder_run_id = ?1 AND w.holder_kind = 'run')",
            )
            .map_err(db)?
            .query_row([&run], |row| row.get::<_, bool>(0))
            .map_err(db)?;
    let mut record_status = match (status.as_str(), conclusion.as_deref()) {
        ("completed", Some("success")) => ExecutionStatus::Success,
        ("completed", Some("failure")) | ("completed", Some("timed_out")) => {
            ExecutionStatus::Failure
        }
        ("completed", Some("cancelled")) => ExecutionStatus::Cancelled,
        ("completed", Some("skipped")) => ExecutionStatus::Skipped,
        ("completed", _) => ExecutionStatus::Success,
        ("in_progress", _) => ExecutionStatus::InProgress,
        _ if held => ExecutionStatus::Pending,
        _ => ExecutionStatus::Queued,
    };
    if record_status == ExecutionStatus::InProgress
        && !graph
            .statuses
            .values()
            .any(|s| *s == ExecutionStatus::InProgress)
    {
        record_status = if held {
            ExecutionStatus::Pending
        } else {
            ExecutionStatus::Queued
        };
    }
    // Per-job workflow facts the record carries: base ids, names, needs,
    // outputs, check runs, caller plans, reusable metadata, detail order.
    let mut job_names = BTreeMap::new();
    let mut job_check_run_ids = BTreeMap::new();
    let mut caller_plans = BTreeMap::new();
    let mut reusable_calls = BTreeMap::new();
    let mut job_fail_fast = BTreeMap::new();
    let mut job_continue_on_error = BTreeMap::new();
    let mut jobs_list = Vec::new();
    // job_history carries no spec envelope: spec columns project as NULL
    // and display order falls back to job-id order.
    let spec_rows: Vec<(String, String, Option<String>, Option<i64>, Option<i64>, Option<i64>, Option<String>, String, i64, String)> = if archived {
        tx.prepare_cached(
            "SELECT job_id, display_name, NULL, NULL, NULL, check_run_id, \
                    annotations, base_id, 0, status FROM job_history \
             WHERE run_id = ?1 AND run_created_at = ( \
                 SELECT MAX(run_created_at) FROM job_history WHERE run_id = ?1) \
             ORDER BY job_id",
        )
        .map_err(db)?
        .query_map([&run], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, String>(9)?,
            ))
        })
        .map_err(db)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(db)?
    } else {
        tx.prepare_cached(
            "SELECT s.job_id, s.display_name, s.reusable_call, s.fail_fast, \
                    s.continue_on_error, j.check_run_id, j.annotations, \
                    j.base_id, s.display_order, j.status \
             FROM job_specs s JOIN jobs j \
               ON j.run_id = s.run_id AND j.job_id = s.job_id \
             WHERE s.run_id = ?1 ORDER BY s.display_order",
        )
        .map_err(db)?
        .query_map([&run], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, String>(9)?,
            ))
        })
        .map_err(db)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(db)?
    };
    for (
        job_id,
        display_name,
        reusable_json,
        fail_fast,
        continue_on_error,
        check_run_id,
        annotations,
        base_id,
        _order,
        status,
    ) in spec_rows {
        let id = JobId(job_id.clone());
        job_names.insert(id.clone(), display_name.clone());
        if let Some(id) = check_run_id {
            job_check_run_ids.insert(JobId(job_id.clone()), id as u64);
        }
        if let Some(json) = &reusable_json {
            match ReusableSpec::decode(json) {
                ReusableSpec::Full { meta, plan, .. } => {
                    if let Some(meta) = meta {
                        reusable_calls.insert(job_id.clone(), meta);
                    }
                    if let Some(plan) = plan {
                        caller_plans.insert(id.clone(), plan);
                    }
                }
                ReusableSpec::Call(_) => {}
            }
        }
        if let Some(flag) = fail_fast {
            job_fail_fast.insert(base_id.clone(), flag != 0);
        }
        if let Some(flag) = continue_on_error {
            job_continue_on_error.insert(job_id.clone(), flag != 0);
        }
        jobs_list.push(crate::models::JobDetail {
            job_id: job_id.clone(),
            name: display_name,
            conclusion: crate::runtime_scheduling::status_string(status_parse(&status)),
            steps: Vec::new(),
            annotations: annotations
                .and_then(|j| serde_json::from_str::<Vec<serde_json::Value>>(&j).ok())
                .unwrap_or_default(),
        });
    }
    let push_state: Option<crate::models::PushState> = tx
        .prepare_cached(
            "SELECT status, error, pr_number, effective_sha FROM run_push_states \
             WHERE run_id = ?1",
        )
        .map_err(db)?
        .query_row([&run], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .optional()
        .map_err(db)?
        .map(|(status, error, pr_number, effective_sha)| crate::models::PushState {
            status: match status.as_str() {
                "synced" => crate::models::PushStatus::Synced,
                "blocked" => crate::models::PushStatus::Blocked,
                _ => crate::models::PushStatus::Pending,
            },
            error,
            pr_number: pr_number.map(|n| n as u64),
            effective_sha,
        });
    let record = RunRecord {
        run_id,
        webhook_delivery_id: delivery_id,
        run_name,
        submission: std::sync::Arc::new(submission),
        jobs: graph.statuses,
        status: record_status,
        job_outputs: graph.outputs,
        job_base_ids: graph.base_ids,
        job_needs: graph.needs,
        caller_plans,
        job_names,
        github,
        head_sha,
        workflow_ref,
        workspace_snapshot: snapshot_json
            .as_deref()
            .and_then(|j| serde_json::from_str(j).ok()),
        job_fail_fast,
        job_continue_on_error,
        job_check_run_ids,
        reusable_calls,
        jobs_list,
        created_at: codec::us_to_utc(created_at).unwrap_or_default(),
        started_at: started_at.and_then(codec::us_to_utc),
        completed_at: completed_at.and_then(codec::us_to_utc),
        run_number: run_number as u64,
        run_attempt: run_attempt as u64,
        workflow_path_str: workflow_path,
        event,
        conclusion,
        push_state,
        snapshot_timing: timing_json
            .as_deref()
            .and_then(|j| serde_json::from_str(j).ok()),
    };
    // `project_run_rows` parity: callers expanded into subtrees do not
    // appear in `run.jobs`; `jobs_list` keeps `display_order` (the SELECT
    // ordering above).
    Ok(Some(record))
}

/// Load one job row by identity.
pub(super) fn job(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<Option<JobRow>, ControlError> {
    tx.prepare_cached(&format!(
        "SELECT {JOB_COLUMNS} FROM jobs WHERE run_id = ?1 AND job_id = ?2"
    ))
    .map_err(db)?
    .query_row(params![codec::run_key(run_id), job_id.0], job_row)
    .optional()
    .map_err(db)
}

/// Recompute `remaining_needs` for every dependent of a freshly settled job
/// (a declared need matches a job id or a base id; a matrix base's edge
/// settles only once every matching leg is terminal). `matrix_parent` rows
/// with children are not legs — they are replaced by them.
pub(super) fn refresh_remaining_needs(
    tx: &Transaction<'_>,
    run_id: RunId,
    settled_job_id: &JobId,
    settled_base_id: &str,
) -> Result<(), ControlError> {
    let run = codec::run_key(run_id);
    tx.prepare_cached(
        "UPDATE jobs SET remaining_needs = ( \
             SELECT COUNT(*) FROM job_needs n WHERE n.run_id = jobs.run_id \
               AND n.job_id = jobs.job_id AND EXISTS ( \
                 SELECT 1 FROM jobs d WHERE d.run_id = n.run_id \
                   AND (d.job_id = n.needs_job_id OR d.base_id = n.needs_job_id) \
                   AND NOT (d.kind = 'matrix_parent' AND EXISTS ( \
                       SELECT 1 FROM jobs c WHERE c.run_id = d.run_id \
                         AND c.parent_job_id = d.job_id)) \
                   AND d.status NOT IN ('success','failure','cancelled','skipped'))) \
         WHERE run_id = ?1 AND queue_state <> 'none' AND EXISTS ( \
             SELECT 1 FROM job_needs n2 WHERE n2.run_id = jobs.run_id \
               AND n2.job_id = jobs.job_id AND (n2.needs_job_id = ?2 OR n2.needs_job_id = ?3))",
    )
    .map_err(db)?
    .execute(params![run, settled_job_id.0, settled_base_id])
    .map_err(db)?;
    Ok(())
}

/// Delete the run's dispatch bookkeeping (assignments + provisioning
/// intents) — a cancelled run must not be offered to a pool.
pub(super) fn clear_run_dispatch_intent(
    tx: &Transaction<'_>,
    run_id: RunId,
) -> Result<(), ControlError> {
    let run = codec::run_key(run_id);
    for sql in [
        "DELETE FROM job_assignments WHERE run_id = ?1",
        "DELETE FROM provision_requests WHERE run_id = ?1",
    ] {
        tx.prepare_cached(sql)
            .map_err(db)?
            .execute([run.clone()])
            .map_err(db)?;
    }
    Ok(())
}

/// The namespace a run belongs to (`'default'` when the row is missing).
pub(super) fn namespace_of(
    tx: &Transaction<'_>,
    run_id: RunId,
) -> Result<String, ControlError> {
    tx.prepare_cached("SELECT namespace_id FROM runs WHERE run_id = ?1")
        .map_err(db)?
        .query_row([codec::run_key(run_id)], |row| row.get(0))
        .optional()
        .map_err(db)
        .map(|value: Option<String>| value.unwrap_or_else(|| DEFAULT_NAMESPACE.to_owned()))
}
