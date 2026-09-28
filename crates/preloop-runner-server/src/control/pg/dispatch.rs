//! Scheduling commands: run submission, completion settlement, cancellation,
//! expansion and the promotion sweep, plus the concurrency-gate machinery
//! those commands share (gate ops live at the bottom of this file).
//!
//! Every command is one transaction: lock the touched `runs` rows (ascending
//! `run_id` — the schema's per-run mutex), load each run's graph, decide with
//! `control/logic.rs` + `runtime_scheduling.rs`, write the deltas back.
//! Gate acquisition takes the `concurrency_holds` row first, then locks the
//! foreign run it cancels/promotes — group-before-run order, always, so two
//! cross-run commands cannot deadlock.

use super::codec::{self, from_json, json, now_us, us};
use super::graph::{self, queue_state_str, Node, NodeKind, ReusableNodeSpec, RunGraph};
use super::{db, lookups, PgBackend};
use crate::concurrency;
use crate::control::backend::PollRequest;
use crate::control::logic;
use crate::control::types::{
    status_str, ClaimedJob, ControlError, PollOutcome, SessionMessage, SubmitJob, SubmitOutcome,
    SubmitRun,
};
use crate::models::{QueuedJob, RunRecord, RunnerCapabilities, TaskAgentJobRequestRecord};
use crate::runtime_scheduling::{self as sched_helpers, DependencyDecision, SchedulingOutcome};
use crate::state::JobSetGate;
use preloop_gha_parser::ConcurrencyQueue;
use preloop_gha_protocol::{azdo, ExecutionStatus, JobId, RunId};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use tokio_postgres::Transaction;

// ─────────────────────────────────────────────────────────────────────────
// Node writes
// ─────────────────────────────────────────────────────────────────────────

/// Persist the mutable columns of one node (`jobs` row). Spec columns are
/// immutable after insert.
async fn flush_node(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    node: &Node,
) -> Result<(), ControlError> {
    tx.execute(
        concat!(
            "UPDATE jobs SET status=$3, queue_state=$4, remaining_needs=$5, \
             claimed_by_runner_id=$6, claimed_at=",
            us!("$7"),
            ", enqueued_at=",
            us!("$8"),
            ", deps_ready_at=",
            us!("$9"),
            ", concurrency_wait_at=",
            us!("$10"),
            ", concurrency_acquired_at=",
            us!("$11"),
            ", started_at=",
            us!("$12"),
            ", completed_at=",
            us!("$13"),
            ", outputs=$14::text::jsonb, annotations=$15::text::jsonb, \
             check_run_id=$16, expand_generation=$17 \
             WHERE run_id=$1::text::uuid AND job_id=$2"
        ),
        &[
            &run_id.0.to_string(),
            &job_id.0,
            &status_str(node.status),
            &queue_state_str(node.queue_state),
            &node.remaining_needs,
            &node.claimed_by_runner_id,
            &node.claimed_at_us,
            &node.enqueued_at_us,
            &node.deps_ready_at_us,
            &node.concurrency_wait_at_us,
            &node.concurrency_acquired_at_us,
            &node.started_at_us,
            &node.completed_at_us,
            &node.outputs.as_ref().map(|v| v.to_string()),
            &node.annotations.as_ref().map(|v| v.to_string()),
            &node.check_run_id,
            &node.expand_generation,
        ],
    )
    .await
    .map_err(db)?;
    Ok(())
}

/// Persist `runs.status`/`conclusion`/timestamps from the graph's record.
async fn flush_run(tx: &Transaction<'_>, graph: &RunGraph) -> Result<(), ControlError> {
    let record = &graph.record;
    let status = match record.status {
        ExecutionStatus::InProgress => "in_progress",
        ExecutionStatus::Success
        | ExecutionStatus::Failure
        | ExecutionStatus::Cancelled
        | ExecutionStatus::Skipped => "completed",
        _ => "queued",
    };
    let started_us = record.started_at.map(|at| codec::system_to_us(at.into()));
    let completed_us = record.completed_at.map(|at| codec::system_to_us(at.into()));
    tx.execute(
        concat!(
            "UPDATE runs SET status=$3, conclusion=$4, started_at=",
            us!("$5"),
            ", completed_at=",
            us!("$6"),
            " WHERE run_id=$1::text::uuid AND namespace_id=$2"
        ),
        &[
            &record.run_id.0.to_string(),
            &graph.namespace,
            &status,
            &record.conclusion.as_deref(),
            &started_us,
            &completed_us,
        ],
    )
    .await
    .map_err(db)?;
    Ok(())
}

/// Insert the `jobs` row for a node (submit-time and expansion-time share it).
async fn insert_job_row(
    tx: &Transaction<'_>,
    graph: &RunGraph,
    node: &Node,
    job_id: &JobId,
) -> Result<(), ControlError> {
    tx.execute(
        concat!(
            "INSERT INTO jobs (run_id, job_id, namespace_id, kind, parent_job_id, \
             base_id, status, queue_state, remaining_needs, pool_key, runs_on, \
             runner_group, priority, run_order, job_order, enqueued_at, \
             deps_ready_at, concurrency_wait_at, concurrency_acquired_at, \
             expand_generation, outputs, annotations, check_run_id, created_at, \
             started_at, completed_at) \
             VALUES ($1::text::uuid,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::text::jsonb,\
             $12,$13,$14,$15,",
            us!("$16"),
            ",",
            us!("$17"),
            ",",
            us!("$18"),
            ",",
            us!("$19"),
            ",$20,$21::text::jsonb,$22::text::jsonb,$23,",
            us!("$24"),
            ",",
            us!("$25"),
            ",",
            us!("$26"),
            ")"
        ),
        &[
            &graph.record.run_id.0.to_string(),
            &job_id.0,
            &graph.namespace,
            &node.kind.as_str(),
            &node.parent_job_id,
            &node.base_id,
            &status_str(node.status),
            &queue_state_str(node.queue_state),
            &node.remaining_needs,
            &node.pool_key,
            &serde_json::to_string(&node.runs_on).unwrap_or_else(|_| "[]".into()),
            &node.runner_group,
            &node.priority,
            &node.run_order,
            &node.job_order,
            &node.enqueued_at_us,
            &node.deps_ready_at_us,
            &node.concurrency_wait_at_us,
            &node.concurrency_acquired_at_us,
            &node.expand_generation,
            &node.outputs.as_ref().map(|v| v.to_string()),
            &node.annotations.as_ref().map(|v| v.to_string()),
            &node.check_run_id,
            &node.created_at_us,
            &node.started_at_us,
            &node.completed_at_us,
        ],
    )
    .await
    .map_err(db)?;
    Ok(())
}

/// Insert `job_specs` + `job_needs` + `job_messages` for a node.
async fn insert_spec_rows(
    tx: &Transaction<'_>,
    graph: &RunGraph,
    node: &Node,
    job_id: &JobId,
    message: Option<&azdo::AgentJobRequestMessage>,
) -> Result<(), ControlError> {
    let run = graph.record.run_id.0.to_string();
    let concurrency_json = node
        .concurrency
        .as_ref()
        .map(|c| serde_json::to_string(c))
        .transpose()
        .map_err(ControlError::backend)?;
    let reusable_json = node
        .reusable
        .as_ref()
        .map(|r| serde_json::to_string(r))
        .transpose()
        .map_err(ControlError::backend)?;
    tx.execute(
        "INSERT INTO job_specs (run_id, job_id, display_name, display_order, \
         if_condition, matrix, deferred_matrix, max_parallel, environment, \
         concurrency, reusable_call, fail_fast, continue_on_error, \
         id_token_granted, oidc_environment, oidc_job_workflow_ref, \
         oidc_job_workflow_sha) \
         VALUES ($1::text::uuid,$2,$3,$4,$5,$6::text::jsonb,$7,$8,$9::text::jsonb,\
         $10::text::jsonb,$11::text::jsonb,$12,$13,$14,$15,$16,$17)",
        &[
            &run,
            &job_id.0,
            &node.display_name,
            &node.display_order,
            &node.if_condition,
            &serde_json::to_string(&node.matrix).unwrap_or_else(|_| "{}".into()),
            &node.deferred_matrix,
            &node.max_parallel.map(|v| v as i32),
            &node.environment.as_ref().map(|v| v.to_string()),
            &concurrency_json,
            &reusable_json,
            &node.fail_fast,
            &node.continue_on_error,
            &node.id_token_granted,
            &node.oidc.environment,
            &node.oidc.job_workflow_ref,
            &node.oidc.job_workflow_sha,
        ],
    )
    .await
    .map_err(db)?;
    for (position, need) in node.needs.iter().enumerate() {
        tx.execute(
            "INSERT INTO job_needs (run_id, job_id, needs_job_id, position) \
             VALUES ($1::text::uuid,$2,$3,$4) ON CONFLICT DO NOTHING",
            &[&run, &job_id.0, &need.0, &(position as i32)],
        )
        .await
        .map_err(db)?;
    }
    if let Some(message) = message {
        tx.execute(
            "INSERT INTO job_messages (run_id, job_id, message_template, \
             secret_names, condition_context) \
             VALUES ($1::text::uuid,$2,$3::text::jsonb,$4::text::jsonb,$5::text::jsonb)",
            &[
                &run,
                &job_id.0,
                &json(message)?,
                &serde_json::to_string(&node.secret_names).unwrap_or_else(|_| "[]".into()),
                &serde_json::to_string(&node.condition_context).map_err(ControlError::backend)?,
            ],
        )
        .await
        .map_err(db)?;
    }
    Ok(())
}

/// Mint a `job_requests` row (identity `request_id`), a `job_leases` row and
/// the step manifest. Returns the minted id.
async fn insert_request_row(
    tx: &Transaction<'_>,
    graph: &RunGraph,
    request: &TaskAgentJobRequestRecord,
    token: Option<&crate::models::GitHubTokenRequest>,
    manifest: &[crate::models::StepRecord],
) -> Result<i64, ControlError> {
    let request_id: i64 = tx
        .query_one(
            "INSERT INTO job_requests (run_id, job_id, namespace_id, agent_job_id, \
             timeline_id, result, timeout_triggered, debug_token_issued) \
             VALUES ($1::text::uuid,$2,$3,$4::text::uuid,$5::text::uuid,$6,false,$7) \
             RETURNING request_id",
            &[
                &request.run_id.0.to_string(),
                &request.job_id.0,
                &graph.namespace,
                &request.agent_job_id.to_string(),
                &request.timeline_id.to_string(),
                &request.result.map(status_str),
                &request.debug_token_issued,
            ],
        )
        .await
        .map_err(db)?
        .get(0);
    tx.execute(
        "INSERT INTO job_leases (request_id, runner_id, expires_at, renewed_at) \
         VALUES ($1, NULL, NULL, NULL) ON CONFLICT DO NOTHING",
        &[&request_id],
    )
    .await
    .map_err(db)?;
    if let Some(token) = token {
        tx.execute(
            "INSERT INTO github_token_requests (request_id, repository, \
             permissions, declared, untrusted) \
             VALUES ($1,$2,$3::text::jsonb,$4,$5)",
            &[
                &request_id,
                &token.repository,
                &json(&token.permissions)?,
                &token.declared,
                &token.untrusted,
            ],
        )
        .await
        .map_err(db)?;
    }
    for (position, step) in manifest.iter().enumerate() {
        let kind = match step.kind {
            crate::models::StepKind::Workflow => "workflow",
            crate::models::StepKind::Synthetic => "synthetic",
        };
        tx.execute(
            concat!(
                "INSERT INTO job_steps (agent_job_id, step_id, position, kind, \
                 workflow_index, runner_number, context_name, name, conclusion, \
                 started_at, finished_at) \
                 VALUES ($1::text::uuid,$2,$3,$4,$5,$6,$7,$8,$9,",
                us!("$10"),
                ",",
                us!("$11"),
                ") ON CONFLICT (agent_job_id, step_id) DO NOTHING"
            ),
            &[
                &request.agent_job_id.to_string(),
                &step.id,
                &(position as i32),
                &kind,
                &step.workflow_index.map(|i| i as i32),
                &step.runner_number.map(|n| n as i32),
                &step.context_name,
                &step.name,
                &step.conclusion,
                &step.started_at.map(|t| t.timestamp_micros()),
                &step.finished_at.map(|t| t.timestamp_micros()),
            ],
        )
        .await
        .map_err(db)?;
    }
    Ok(request_id)
}

/// Settle one request whose job is terminal: stamp the verdict, drop the
/// lease, unbind the session and retire undelivered cancellation messages
/// for that session (the runner already stopped the work — a queued
/// `job.cancellation` must not redeliver). Settled requests keep their row
/// (acquire reads remain routable).
async fn settle_request_tx(
    tx: &Transaction<'_>,
    request_id: i64,
    status: ExecutionStatus,
) -> Result<(), ControlError> {
    // Capture the session binding before clearing it.
    let session = tx
        .query_opt(
            "SELECT session_id::text FROM job_requests WHERE request_id=$1",
            &[&request_id],
        )
        .await
        .map_err(db)?
        .and_then(|row| row.get::<_, Option<String>>(0));
    tx.execute(
        "UPDATE job_requests SET result=$2, finished_at=now(), session_id=NULL \
         WHERE request_id=$1 AND result IS NULL",
        &[&request_id, &status_str(status)],
    )
    .await
    .map_err(db)?;
    tx.execute("DELETE FROM job_leases WHERE request_id=$1", &[&request_id])
        .await
        .map_err(db)?;
    tx.execute(
        "DELETE FROM github_token_requests WHERE request_id=$1",
        &[&request_id],
    )
    .await
    .map_err(db)?;
    if let Some(session_id) = session {
        tx.execute(
            "DELETE FROM session_messages WHERE session_id=$1::text::uuid \
             AND message_type='job.cancellation'",
            &[&session_id],
        )
        .await
        .map_err(db)?;
    }
    Ok(())
}

/// Drop an unclaimed attempt's request row entirely (matrix expansion
/// replaces its node) — request, lease, token request and step manifest.
async fn purge_request_tx(
    tx: &Transaction<'_>,
    request_id: i64,
) -> Result<Option<TaskAgentJobRequestRecord>, ControlError> {
    let select = format!(
        "{} WHERE q.request_id = $1 FOR UPDATE OF q",
        lookups::REQUEST_SELECT
    );
    let row = tx.query_opt(&select, &[&request_id]).await.map_err(db)?;
    let Some(row) = row else { return Ok(None) };
    let record = lookups::request_from_row(&row)?;
    tx.execute(
        "DELETE FROM job_requests WHERE request_id=$1",
        &[&request_id],
    )
    .await
    .map_err(db)?;
    tx.execute(
        "DELETE FROM job_steps WHERE agent_job_id=$1::text::uuid",
        &[&record.agent_job_id.to_string()],
    )
    .await
    .map_err(db)?;
    Ok(Some(record))
}

/// Retire every request of an expandable node (`RequestRetirement` parity).
pub(super) enum Retirement {
    Settle(ExecutionStatus),
    Purge,
}

pub(super) async fn retire_node_requests(
    tx: &Transaction<'_>,
    run_id: RunId,
    node_id: &JobId,
    retirement: Retirement,
) -> Result<Vec<String>, ControlError> {
    let select = format!(
        "{} WHERE q.run_id = $1::text::uuid AND q.job_id = $2 FOR UPDATE OF q",
        lookups::REQUEST_SELECT
    );
    let rows = tx
        .query(&select, &[&run_id.0.to_string(), &node_id.0])
        .await
        .map_err(db)?;
    let mut removed_logs = Vec::new();
    for row in rows {
        let record = lookups::request_from_row(&row)?;
        match retirement {
            Retirement::Settle(status) => {
                settle_request_tx(tx, record.request_id, status).await?;
            }
            Retirement::Purge => {
                purge_request_tx(tx, record.request_id).await?;
                removed_logs.push(record.agent_job_id.to_string());
            }
        }
    }
    Ok(removed_logs)
}

/// The agent job GUID of a job's live (or latest) attempt.
pub(super) async fn agent_job_id(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<Option<uuid::Uuid>, ControlError> {
    Ok(tx
        .query_opt(
            "SELECT agent_job_id::text FROM job_requests \
             WHERE run_id=$1::text::uuid AND job_id=$2 \
             ORDER BY (result IS NULL) DESC, request_id DESC LIMIT 1",
            &[&run_id.0.to_string(), &job_id.0],
        )
        .await
        .map_err(db)?
        .map(|row| codec::uuid(row.get(0)))
        .transpose()?)
}

/// Enqueue a `job.cancellation` session message for the runner holding the
/// attempt plus a `job_cancellations` bookkeeping row. No-ops when the
/// attempt is already settled or unclaimed (no session to reach).
pub(super) async fn enqueue_cancellation(
    tx: &Transaction<'_>,
    graph: &RunGraph,
    job_id: &JobId,
    reason: Option<&str>,
) -> Result<bool, ControlError> {
    let Some(row) = tx
        .query_opt(
            "SELECT q.request_id, q.agent_job_id::text, q.session_id::text \
             FROM job_requests q WHERE q.run_id=$1::text::uuid AND q.job_id=$2 \
             AND q.result IS NULL ORDER BY q.request_id DESC LIMIT 1",
            &[&graph.record.run_id.0.to_string(), &job_id.0],
        )
        .await
        .map_err(db)?
    else {
        return Ok(false);
    };
    let request_id: i64 = row.get(0);
    let agent_job_id: String = row.get(1);
    let Some(session_id) = row.get::<_, Option<String>>(2) else {
        return Ok(false);
    };
    let uuid = codec::uuid(&agent_job_id)?;
    tx.execute(
        "INSERT INTO job_cancellations (request_id, reason) VALUES ($1,$2)",
        &[&request_id, &reason],
    )
    .await
    .map_err(db)?;
    tx.execute(
        "INSERT INTO session_messages (session_id, message_type, request_id, body) \
         VALUES ($1::text::uuid,'job.cancellation',$2,$3::text::jsonb)",
        &[
            &session_id,
            &request_id,
            &concurrency::job_cancel_body(uuid),
        ],
    )
    .await
    .map_err(db)?;
    Ok(true)
}

// ─────────────────────────────────────────────────────────────────────────
// Job node transitions
// ─────────────────────────────────────────────────────────────────────────

/// A node became ready: status + queue_state + enqueue clock (caller flushes).
fn mark_ready(node: &mut Node, now_us: i64) {
    node.status = ExecutionStatus::Queued;
    node.queue_state = logic::QueueState::Ready;
    node.enqueued_at_us = Some(now_us);
}

/// A node settled terminal: status + queue_state + completion clock.
fn mark_terminal(node: &mut Node, status: ExecutionStatus, now_us: i64) {
    node.status = status;
    node.queue_state = logic::QueueState::None;
    node.completed_at_us = Some(now_us);
    node.claimed_by_runner_id = None;
}

/// Wire `run.jobs` view + record sync after a node status change. Callers
/// mutate `node.status` directly; this mirrors it into `record.jobs` only
/// for nodes that contribute (matrix parents with children do not).
fn sync_status(graph: &mut RunGraph, job_id: &JobId, status: ExecutionStatus) {
    if graph
        .nodes
        .get(job_id)
        .is_some_and(|node| node.contributes())
    {
        graph.record.jobs.insert(job_id.clone(), status);
    }
}

/// Set a node's status (and record view) without touching queue fields.
fn set_status(graph: &mut RunGraph, job_id: &JobId, status: ExecutionStatus) {
    if let Some(node) = graph.nodes.get_mut(job_id) {
        node.status = status;
    }
    sync_status(graph, job_id, status);
}

/// The node ids that have at least one child (expanded parents): they no
/// longer contribute to `record.jobs`.
fn child_ids(graph: &RunGraph) -> BTreeSet<String> {
    graph
        .nodes
        .values()
        .filter_map(|n| n.parent_job_id.clone())
        .collect()
}

/// Recompute `record.jobs` from scratch (after bulk changes). Contributing:
/// every node except a matrix/reusable parent that already has children.
fn rebuild_jobs(graph: &mut RunGraph) {
    let parents = child_ids(graph);
    graph.record.jobs = graph
        .nodes
        .iter()
        .filter(|(id, node)| !(node.kind != NodeKind::Job && parents.contains(&id.0)))
        .map(|(id, node)| (id.clone(), node.status))
        .collect();
}

// ─────────────────────────────────────────────────────────────────────────
// Concurrency gates (job / run / jobset)
// ─────────────────────────────────────────────────────────────────────────

/// A holder or waiter in `concurrency_holds`/`concurrency_waits`.
#[derive(Debug, Clone)]
pub(super) struct GateHolder {
    pub(crate) holder: concurrency::Holder,
    pub(crate) jobset_id: Option<i64>,
}

fn gate_row(holder: &GateHolder) -> (&'static str, String, Option<String>, Option<i64>) {
    let (kind, run_id, job_id, _ids) = concurrency::holder_row(&holder.holder);
    (kind, run_id, job_id, holder.jobset_id)
}

fn holder_of(kind: &str, run_id: &str, job_id: Option<&str>) -> Option<concurrency::Holder> {
    concurrency::holder_from_row(kind, run_id, job_id, "[]")
}

/// The group's current state, locked by `INSERT .. ON CONFLICT DO NOTHING`
/// probe + `SELECT .. FOR UPDATE`.
struct GroupState {
    holder: Option<GateHolder>,
    waiters: Vec<(i64, GateHolder)>,
    display_name: String,
}

/// Lock one concurrency group: probe-insert a `holds` row (the group's
/// mutex), then read holder + FIFO waiters. Returns `None` only when this
/// transaction already holds the row (self-probe inserted it).
async fn lock_group(
    tx: &Transaction<'_>,
    namespace: &str,
    key: &(String, String),
) -> Result<Option<GroupState>, ControlError> {
    let (repository, group_name) = key;
    // The probe INSERT is also the mutex: whoever lands the row owns the
    // slot; losers see the existing row, which the FOR UPDATE below locks.
    tx.execute(
        "INSERT INTO concurrency_holds (namespace_id, repository, group_name, \
         display_name, holder_kind, holder_run_id) \
         VALUES ($1,$2,$3,'','run',$4::text::uuid) ON CONFLICT DO NOTHING",
        &[
            &namespace,
            &repository,
            &group_name,
            &uuid::Uuid::nil().to_string(),
        ],
    )
    .await
    .map_err(db)?;
    let holder = tx
        .query_opt(
            "SELECT holder_kind, holder_run_id::text, holder_job_id, \
             holder_jobset_id, display_name FROM concurrency_holds \
             WHERE namespace_id=$1 AND repository=$2 AND group_name=$3 \
             FOR UPDATE",
            &[&namespace, &repository, &group_name],
        )
        .await
        .map_err(db)?;
    let Some(holder_row) = holder else {
        return Ok(None);
    };
    let holder = if holder_row.get::<_, String>(4).is_empty()
        && holder_row.get::<_, String>(1) == uuid::Uuid::nil().to_string()
    {
        // The probe row itself: the group is empty and this transaction
        // holds the mutex.
        None
    } else {
        holder_of(
            holder_row.get(0),
            holder_row.get::<_, String>(1).as_str(),
            holder_row.get::<_, Option<String>>(2).as_deref(),
        )
        .map(|holder| GateHolder {
            holder,
            jobset_id: holder_row.get(3),
        })
    };
    let waiters = tx
        .query(
            "SELECT wait_id, holder_kind, holder_run_id::text, holder_job_id, \
             holder_jobset_id FROM concurrency_waits \
             WHERE namespace_id=$1 AND repository=$2 AND group_name=$3 \
             ORDER BY wait_id",
            &[&namespace, &repository, &group_name],
        )
        .await
        .map_err(db)?
        .iter()
        .filter_map(|row| {
            holder_of(
                row.get(1),
                row.get::<_, String>(2).as_str(),
                row.get::<_, Option<String>>(3).as_deref(),
            )
            .map(|holder| {
                (
                    row.get::<_, i64>(0),
                    GateHolder {
                        holder,
                        jobset_id: row.get(4),
                    },
                )
            })
        })
        .collect();
    Ok(Some(GroupState {
        holder,
        waiters,
        display_name: holder_row.get(4),
    }))
}

/// Write the group's holder row (the probe row gets its real identity via
/// UPDATE).
async fn set_holder(
    tx: &Transaction<'_>,
    namespace: &str,
    key: &(String, String),
    display_name: &str,
    holder: &GateHolder,
) -> Result<(), ControlError> {
    let (kind, run_id, job_id, jobset_id) = gate_row(holder);
    tx.execute(
        "UPDATE concurrency_holds SET holder_kind=$4, holder_run_id=$5::text::uuid, \
         holder_job_id=$6, holder_jobset_id=$7, display_name=$8, held_at=now() \
         WHERE namespace_id=$1 AND repository=$2 AND group_name=$3",
        &[
            &namespace,
            &key.0,
            &key.1,
            &kind,
            &run_id,
            &job_id,
            &jobset_id,
            &display_name,
        ],
    )
    .await
    .map_err(db)?;
    Ok(())
}

/// Clear the group's holder row entirely (empty group → delete the mutex).
async fn clear_group(
    tx: &Transaction<'_>,
    namespace: &str,
    key: &(String, String),
) -> Result<(), ControlError> {
    tx.execute(
        "DELETE FROM concurrency_holds WHERE namespace_id=$1 AND repository=$2 \
         AND group_name=$3",
        &[&namespace, &key.0, &key.1],
    )
    .await
    .map_err(db)?;
    Ok(())
}

/// Park a holder on the group's FIFO.
async fn park_waiter(
    tx: &Transaction<'_>,
    namespace: &str,
    key: &(String, String),
    holder: &GateHolder,
) -> Result<i64, ControlError> {
    let (kind, run_id, job_id, jobset_id) = gate_row(holder);
    Ok(tx
        .query_one(
            "INSERT INTO concurrency_waits (namespace_id, repository, group_name, \
             holder_kind, holder_run_id, holder_job_id, holder_jobset_id) \
             VALUES ($1,$2,$3,$4,$5::text::uuid,$6,$7) RETURNING wait_id",
            &[
                &namespace, &key.0, &key.1, &kind, &run_id, &job_id, &jobset_id,
            ],
        )
        .await
        .map_err(db)?
        .get(0))
}

/// Remove a holder's wait row(s).
async fn remove_waiter(
    tx: &Transaction<'_>,
    namespace: &str,
    holder_run_id: RunId,
    job_id: Option<&JobId>,
    jobset_id: Option<i64>,
) -> Result<usize, ControlError> {
    let removed = tx
        .execute(
            "DELETE FROM concurrency_waits WHERE namespace_id=$1 \
             AND holder_run_id=$2::text::uuid \
             AND (($3::text IS NOT NULL AND holder_job_id=$3) \
                  OR ($4::bigint IS NOT NULL AND holder_jobset_id=$4) \
                  OR ($3::text IS NULL AND $4::bigint IS NULL \
                      AND holder_kind='run'))",
            &[
                &namespace,
                &holder_run_id.0.to_string(),
                &job_id.map(|id| id.0.as_str()),
                &jobset_id,
            ],
        )
        .await
        .map_err(db)?;
    Ok(removed as usize)
}

/// `holder_event_order` for a run: event + repository + payload out of the
/// submission (read unlocked — submission is immutable).
async fn run_event_order(
    tx: &Transaction<'_>,
    run_id: RunId,
) -> Result<Option<concurrency::EventOrder>, ControlError> {
    let row = tx
        .query_opt(
            "SELECT submission::text FROM run_submissions WHERE run_id=$1::text::uuid",
            &[&run_id.0.to_string()],
        )
        .await
        .map_err(db)?;
    let Some(row) = row else { return Ok(None) };
    let submission: serde_json::Value =
        serde_json::from_str(row.get::<_, String>(0).as_str()).map_err(ControlError::backend)?;
    Ok(concurrency::event_order(
        submission
            .get("event")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
        submission
            .get("repository")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
        submission
            .get("payload")
            .unwrap_or(&serde_json::Value::Null),
    ))
}

/// Whether a run's remaining work is unschedulable without external hosts.
async fn run_stuck_on_external_hosts(
    tx: &Transaction<'_>,
    run_id: RunId,
) -> Result<bool, ControlError> {
    // No registered macOS/Windows runner anywhere → a run whose only
    // remaining jobs need those labels is structurally stuck.
    let external = tx
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM runners r \
             WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text(r.labels) l \
             WHERE lower(l.value) LIKE 'macos%' OR lower(l.value) LIKE 'windows%'))",
            &[],
        )
        .await
        .map_err(db)?
        .get::<_, bool>(0);
    if external {
        return Ok(false);
    }
    Ok(tx
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM jobs \
             WHERE run_id=$1::text::uuid AND status NOT IN \
             ('success','failure','cancelled','skipped','timed_out')) \
             AND NOT EXISTS (SELECT 1 FROM jobs \
             WHERE run_id=$1::text::uuid AND status NOT IN \
             ('success','failure','cancelled','skipped','timed_out') \
             AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(jobs.runs_on) l \
             WHERE lower(l.value) LIKE 'macos%' OR lower(l.value) LIKE 'windows%'))",
            &[&run_id.0.to_string()],
        )
        .await
        .map_err(db)?
        .get::<_, bool>(0))
}

/// Cancel a holder's work (same-run holders are exempt — arrival replaces
/// without cancelling its own run's jobs).
pub(super) struct CanceledWork {
    pub(crate) cancellations: usize,
}

// Implemented in the command section (needs graph mutation helpers).
#[allow(clippy::type_complexity)]
pub(super) fn cancel_holder<'a>(
    backend: &'a PgBackend,
    tx: &'a Transaction<'a>,
    holder: &'a concurrency::Holder,
    reason: Option<&'a str>,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<CanceledWork, ControlError>> + Send + 'a>,
> {
    Box::pin(async move { cancel_holder_inner(backend, tx, holder, reason).await })
}

async fn cancel_holder_inner(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    holder: &concurrency::Holder,
    reason: Option<&str>,
) -> Result<CanceledWork, ControlError> {
    match holder {
        concurrency::Holder::Run(run_id) => {
            let cancelled = cancel_run_tx(backend, tx, *run_id, reason).await?;
            Ok(CanceledWork {
                cancellations: cancelled,
            })
        }
        concurrency::Holder::Job { run_id, job_id } => {
            let cancelled = cancel_job_tx(backend, tx, *run_id, job_id, reason).await?;
            Ok(CanceledWork {
                cancellations: cancelled,
            })
        }
        concurrency::Holder::JobSet { run_id, job_ids } => {
            // Drop the set's remaining admission rows, then cancel members.
            tx.execute(
                "DELETE FROM jobsets WHERE run_id=$1::text::uuid AND job_ids=$2::text::jsonb",
                &[
                    &run_id.0.to_string(),
                    &serde_json::to_string(
                        &job_ids.iter().map(|j| j.0.clone()).collect::<Vec<_>>(),
                    )
                    .unwrap_or_default(),
                ],
            )
            .await
            .map_err(db)?;
            let mut cancellations = 0;
            for job_id in job_ids {
                cancellations += cancel_job_tx(backend, tx, *run_id, job_id, reason).await?;
            }
            // Terminal-settle the run when every member ended.
            tx.execute(
                "UPDATE runs SET status='completed', conclusion='cancelled', \
                 completed_at=now() WHERE run_id=$1::text::uuid \
                 AND status <> 'completed' AND NOT EXISTS (SELECT 1 FROM jobs \
                 WHERE run_id=$1::text::uuid AND status NOT IN \
                 ('success','failure','cancelled','skipped','timed_out'))",
                &[&run_id.0.to_string()],
            )
            .await
            .map_err(db)?;
            Ok(CanceledWork { cancellations })
        }
    }
}

/// Outcome of one gate-acquisition attempt.
pub(super) enum GateOutcome {
    Acquired,
    Parked,
    /// The arrival itself was cancelled (queue overflow / superseded).
    Cancelled,
    Failed,
}

/// Try to take a concurrency group for `holder`. Mirrors
/// `sched::try_acquire_concurrency` including the stale-arrival check and
/// queue-mode displacement. Cancelling a foreign holder/parked waiters is
/// the caller-visible side effect.
pub(super) async fn acquire_gate(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    namespace: &str,
    key: &(String, String),
    display_name: &str,
    holder: &GateHolder,
    cancel_in_progress: bool,
    queue: ConcurrencyQueue,
) -> Result<GateOutcome, ControlError> {
    let Some(group) = lock_group(tx, namespace, key).await? else {
        // Probe landed: no holder, no waiters — occupy the group.
        set_holder(tx, namespace, key, display_name, holder).await?;
        return Ok(GateOutcome::Acquired);
    };
    let running_stuck = match &group.holder {
        Some(existing) => run_stuck_on_external_hosts(tx, existing.holder.run_id()).await?,
        None => false,
    };
    // Late deliveries must not pre-empt a newer holder.
    if cancel_in_progress || queue == ConcurrencyQueue::Single {
        if let Some(arrival) = run_event_order(tx, holder.holder.run_id()).await? {
            let mut superseded = false;
            for existing in group
                .holder
                .iter()
                .chain(group.waiters.iter().map(|(_, h)| h))
            {
                if existing.holder.run_id() == holder.holder.run_id() {
                    continue;
                }
                if let Some(existing_order) = run_event_order(tx, existing.holder.run_id()).await? {
                    if arrival.is_older_than(&existing_order) {
                        superseded = true;
                        break;
                    }
                }
            }
            if superseded {
                return Ok(GateOutcome::Cancelled);
            }
        }
    }
    if group.holder.is_none() {
        set_holder(tx, namespace, key, display_name, holder).await?;
        return Ok(GateOutcome::Acquired);
    }
    if running_stuck {
        // The running run is wedged (no runner can take its jobs): displace
        // it without a cancellation — its jobs can't run anyway.
        set_holder(tx, namespace, key, display_name, holder).await?;
        return Ok(GateOutcome::Acquired);
    }
    if cancel_in_progress {
        let prev = group.holder.clone();
        let stale_pending = group.waiters.clone();
        set_holder(tx, namespace, key, display_name, holder).await?;
        if let Some(prev) = prev {
            if prev.holder.run_id() != holder.holder.run_id() {
                let (pk, prun, pjob, _) = concurrency::holder_row(&prev.holder);
                let _ = (pk, prun, pjob);
                tx.execute(
                    "DELETE FROM concurrency_waits WHERE namespace_id=$1 \
                     AND repository=$2 AND group_name=$3",
                    &[&namespace, &key.0, &key.1],
                )
                .await
                .map_err(db)?;
                cancel_holder(
                    backend,
                    tx,
                    &prev.holder,
                    concurrency::cancelled_reason().as_deref(),
                )
                .await?;
            }
        }
        for pending in stale_pending {
            if pending.1.holder.run_id() != holder.holder.run_id() {
                cancel_holder(
                    backend,
                    tx,
                    &pending.1.holder,
                    concurrency::cancelled_reason().as_deref(),
                )
                .await?;
            }
        }
        return Ok(GateOutcome::Acquired);
    }
    let join = concurrency::apply_queue_mode(
        queue,
        &group
            .waiters
            .iter()
            .map(|(_, h)| h.holder.clone())
            .collect::<VecDeque<_>>(),
    );
    for pending_holder in join.cancel_pending {
        if pending_holder.run_id() == holder.holder.run_id() {
            continue;
        }
        cancel_holder(
            backend,
            tx,
            &pending_holder,
            concurrency::cancelled_reason().as_deref(),
        )
        .await?;
        remove_waiter_for(tx, namespace, key, &pending_holder).await?;
    }
    if join.cancel_arrival {
        return Ok(GateOutcome::Cancelled);
    }
    if join.park_arrival {
        park_waiter(tx, namespace, key, holder).await?;
        return Ok(GateOutcome::Parked);
    }
    Ok(GateOutcome::Acquired)
}

/// Delete the wait rows for one holder in one group.
async fn remove_waiter_for(
    tx: &Transaction<'_>,
    namespace: &str,
    key: &(String, String),
    holder: &concurrency::Holder,
) -> Result<(), ControlError> {
    let (kind, run_id, job_id, ids_json) = concurrency::holder_row(holder);
    let _ = ids_json;
    tx.execute(
        "DELETE FROM concurrency_waits WHERE namespace_id=$1 AND repository=$2 \
         AND group_name=$3 AND holder_kind=$4 AND holder_run_id=$5::text::uuid \
         AND holder_job_id IS NOT DISTINCT FROM $6",
        &[&namespace, &key.0, &key.1, &kind, &run_id, &job_id],
    )
    .await
    .map_err(db)?;
    Ok(())
}

/// Release every concurrency presence of a job — running holds it owns
/// (Job holds and JobSet holds containing it once the set is terminal), and
/// its wait rows. Promotes the oldest waiter of each freed group.
pub(super) async fn release_concurrency_for_job(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<(), ControlError> {
    // Groups this job might occupy: every group row mentioning it. The scan
    // mirrors the old code's all-groups sweep — a pending holder can outlive
    // the bookkeeping that created it.
    let rows = tx
        .query(
            "SELECT namespace_id, repository, group_name, holder_kind, \
             holder_run_id::text, holder_job_id, holder_jobset_id \
             FROM concurrency_holds \
             WHERE holder_run_id=$1::text::uuid \
             UNION SELECT namespace_id, repository, group_name, holder_kind, \
             holder_run_id::text, holder_job_id, holder_jobset_id \
             FROM concurrency_waits WHERE holder_run_id=$1::text::uuid",
            &[&run_id.0.to_string()],
        )
        .await
        .map_err(db)?;
    for row in rows {
        let namespace: String = row.get(0);
        let key = (row.get::<_, String>(1), row.get::<_, String>(2));
        let Some(holder) = holder_of(
            row.get(3),
            row.get::<_, String>(4).as_str(),
            row.get::<_, Option<String>>(5).as_deref(),
        ) else {
            continue;
        };
        let jobset_id: Option<i64> = row.get(6);
        if !holder.contains_job(run_id, job_id) {
            continue;
        }
        let is_holder = tx
            .query_opt(
                "SELECT 1 FROM concurrency_holds WHERE namespace_id=$1 \
                 AND repository=$2 AND group_name=$3 AND holder_kind=$4 \
                 AND holder_run_id=$5::text::uuid \
                 AND holder_job_id IS NOT DISTINCT FROM $6 \
                 AND holder_jobset_id IS NOT DISTINCT FROM $7",
                &[
                    &namespace,
                    &key.0,
                    &key.1,
                    &concurrency::holder_row(&holder).0,
                    &run_id.0.to_string(),
                    &concurrency::holder_row(&holder).2,
                    &jobset_id,
                ],
            )
            .await
            .map_err(db)?
            .is_some();
        // Wait-row only: drop it. Hold rows are released only when the
        // holder is terminal (a Job hold ends with its job; Run/JobSet hold
        // release requires the whole holder terminal, checked by the caller
        // flow — Run releases happen via release_concurrency_for_run).
        if is_holder {
            let release = match &holder {
                concurrency::Holder::Job { .. } => true,
                _ => {
                    // Only when the holder's members are all terminal.
                    match &holder {
                        concurrency::Holder::JobSet { job_ids, .. } => {
                            let ids: Vec<String> = job_ids.iter().map(|j| j.0.clone()).collect();
                            tx.query_one(
                                "SELECT NOT EXISTS (SELECT 1 FROM jobs \
                                 WHERE run_id=$1::text::uuid AND job_id=ANY($2) \
                                 AND status NOT IN \
                                 ('success','failure','cancelled','skipped','timed_out'))",
                                &[&run_id.0.to_string(), &ids],
                            )
                            .await
                            .map_err(db)?
                            .get(0)
                        }
                        concurrency::Holder::Run(_) => false,
                        concurrency::Holder::Job { .. } => unreachable!(),
                    }
                }
            };
            if release {
                promote_after_release(backend, tx, &namespace, &key).await?;
            }
        } else {
            remove_waiter_for(tx, &namespace, &key, &holder).await?;
        }
    }
    Ok(())
}

/// Release every presence of a run (all its holders and waiters), promoting
/// waiters as slots free.
pub(super) async fn release_concurrency_for_run(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    run_id: RunId,
) -> Result<(), ControlError> {
    let rows = tx
        .query(
            "SELECT namespace_id, repository, group_name FROM concurrency_holds \
             WHERE holder_run_id=$1::text::uuid \
             UNION SELECT namespace_id, repository, group_name \
             FROM concurrency_waits WHERE holder_run_id=$1::text::uuid",
            &[&run_id.0.to_string()],
        )
        .await
        .map_err(db)?;
    for row in rows {
        let namespace: String = row.get(0);
        let key = (row.get::<_, String>(1), row.get::<_, String>(2));
        // Drop this run's wait rows, release its hold, promote FIFO.
        tx.execute(
            "DELETE FROM concurrency_waits WHERE namespace_id=$1 \
             AND repository=$2 AND group_name=$3 AND holder_run_id=$4::text::uuid",
            &[&namespace, &key.0, &key.1, &run_id.0.to_string()],
        )
        .await
        .map_err(db)?;
        let mine = tx
            .query_opt(
                "SELECT 1 FROM concurrency_holds WHERE namespace_id=$1 \
                 AND repository=$2 AND group_name=$3 \
                 AND holder_run_id=$4::text::uuid FOR UPDATE",
                &[&namespace, &key.0, &key.1, &run_id.0.to_string()],
            )
            .await
            .map_err(db)?
            .is_some();
        if mine {
            promote_after_release(backend, tx, &namespace, &key).await?;
        }
    }
    Ok(())
}

/// Drop the group's hold row and promote the oldest waiter, if any.
#[allow(clippy::type_complexity)]
pub(super) fn promote_after_release<'a>(
    backend: &'a PgBackend,
    tx: &'a Transaction<'a>,
    namespace: &'a str,
    key: &'a (String, String),
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ControlError>> + Send + 'a>> {
    Box::pin(async move { promote_after_release_inner(backend, tx, namespace, key).await })
}

async fn promote_after_release_inner(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    namespace: &str,
    key: &(String, String),
) -> Result<(), ControlError> {
    let next = tx
        .query_opt(
            "SELECT wait_id, holder_kind, holder_run_id::text, holder_job_id, \
             holder_jobset_id FROM concurrency_waits \
             WHERE namespace_id=$1 AND repository=$2 AND group_name=$3 \
             ORDER BY wait_id LIMIT 1 FOR UPDATE SKIP LOCKED",
            &[&namespace, &key.0, &key.1],
        )
        .await
        .map_err(db)?;
    match next {
        Some(row) => {
            let wait_id: i64 = row.get(0);
            let Some(holder) = holder_of(
                row.get(1),
                row.get::<_, String>(2).as_str(),
                row.get::<_, Option<String>>(3).as_deref(),
            ) else {
                return Ok(());
            };
            let jobset_id: Option<i64> = row.get(4);
            tx.execute(
                "DELETE FROM concurrency_waits WHERE wait_id=$1",
                &[&wait_id],
            )
            .await
            .map_err(db)?;
            promote_waiter(backend, tx, namespace, key, wait_id, &holder, jobset_id).await
        }
        None => clear_group(tx, namespace, key).await,
    }
}

/// A promoted waiter takes the hold and resumes its work.
async fn promote_waiter(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    namespace: &str,
    key: &(String, String),
    _wait_id: i64,
    holder: &concurrency::Holder,
    jobset_id: Option<i64>,
) -> Result<(), ControlError> {
    match holder {
        concurrency::Holder::Run(run_id) => {
            set_holder(
                tx,
                &namespace,
                key,
                key.1.as_str(),
                &GateHolder {
                    holder: holder.clone(),
                    jobset_id: None,
                },
            )
            .await?;
            // Resume the run's held jobs.
            resume_held_run(backend, tx, *run_id).await
        }
        concurrency::Holder::Job { run_id, job_id } => {
            // Max-parallel back-pressure: a leg under the cap re-waits.
            let under = job_under_max_parallel(tx, *run_id, job_id).await?;
            if !under {
                park_waiter(
                    tx,
                    &namespace,
                    key,
                    &GateHolder {
                        holder: holder.clone(),
                        jobset_id: None,
                    },
                )
                .await?;
                return Ok(());
            }
            set_holder(
                tx,
                &namespace,
                key,
                key.1.as_str(),
                &GateHolder {
                    holder: holder.clone(),
                    jobset_id: None,
                },
            )
            .await?;
            resume_held_job(backend, tx, *run_id, job_id).await
        }
        concurrency::Holder::JobSet { run_id, job_ids } => {
            let Some(jobset_id) = jobset_id else {
                return Ok(());
            };
            // Advance the set's remaining gates; taking this hold is one.
            let advanced = advance_jobset(backend, tx, *run_id, jobset_id, Some(key)).await?;
            match advanced {
                JobsetAdvance::Blocked => Ok(()),
                JobsetAdvance::Failed => {
                    cancel_holder(
                        backend,
                        tx,
                        holder,
                        concurrency::cancelled_reason().as_deref(),
                    )
                    .await?;
                    Ok(())
                }
                JobsetAdvance::Ready => {
                    tx.execute(
                        "UPDATE jobsets SET state='ready' WHERE jobset_id=$1",
                        &[&jobset_id],
                    )
                    .await
                    .map_err(db)?;
                    let ids: Vec<String> = job_ids.iter().map(|j| j.0.clone()).collect();
                    let rows = tx
                        .query(
                            "SELECT job_id FROM jobs WHERE run_id=$1::text::uuid \
                             AND job_id=ANY($2) AND queue_state='held'",
                            &[&run_id.0.to_string(), &ids],
                        )
                        .await
                        .map_err(db)?;
                    for row in rows {
                        let job_id = JobId(row.get(0));
                        resume_held_job(backend, tx, *run_id, &job_id).await?;
                    }
                    Ok(())
                }
            }
        }
    }
}

/// Advance a jobset through its remaining gates (the promoted gate counts as
/// acquired). `Ready` when every gate row is `acquired`.
enum JobsetAdvance {
    Ready,
    Blocked,
    Failed,
}

async fn advance_jobset(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    run_id: RunId,
    jobset_id: i64,
    acquired_key: Option<&(String, String)>,
) -> Result<JobsetAdvance, ControlError> {
    if let Some(key) = acquired_key {
        tx.execute(
            "UPDATE jobset_gates SET acquired=true WHERE jobset_id=$1 \
             AND repository=$2 AND group_name=$3",
            &[&jobset_id, &key.0, &key.1],
        )
        .await
        .map_err(db)?;
    }
    loop {
        let gate = tx
            .query_opt(
                "SELECT gate_index, repository, group_name, display_name, \
                 cancel_in_progress, queue_mode FROM jobset_gates \
                 WHERE jobset_id=$1 AND NOT acquired ORDER BY gate_index LIMIT 1",
                &[&jobset_id],
            )
            .await
            .map_err(db)?;
        let Some(gate) = gate else {
            return Ok(JobsetAdvance::Ready);
        };
        let gate_index: i32 = gate.get(0);
        let key = (gate.get::<_, String>(1), gate.get::<_, String>(2));
        let display: String = gate.get(3);
        let cancel: bool = gate.get(4);
        let queue = concurrency::queue_mode_from_row(gate.get(5));
        let holder = GateHolder {
            holder: concurrency::Holder::JobSet {
                run_id,
                job_ids: tx
                    .query_one(
                        "SELECT job_ids::text FROM jobsets WHERE jobset_id=$1",
                        &[&jobset_id],
                    )
                    .await
                    .map_err(db)
                    .and_then(|row| {
                        from_json::<Vec<String>>(row.get::<_, String>(0).as_str())
                            .map(|ids| ids.into_iter().map(JobId).collect())
                            .map_err(ControlError::backend)
                    })?,
            },
            jobset_id: Some(jobset_id),
        };
        match acquire_gate(
            backend, tx, "default", &key, &display, &holder, cancel, queue,
        )
        .await?
        {
            GateOutcome::Acquired => {
                tx.execute(
                    "UPDATE jobset_gates SET acquired=true WHERE jobset_id=$1 \
                     AND gate_index=$2",
                    &[&jobset_id, &gate_index],
                )
                .await
                .map_err(db)?;
            }
            GateOutcome::Parked => return Ok(JobsetAdvance::Blocked),
            GateOutcome::Cancelled | GateOutcome::Failed => {
                // Release the gates already taken before failing.
                release_jobset_gates(backend, tx, jobset_id).await?;
                return Ok(JobsetAdvance::Failed);
            }
        }
    }
}

/// Drop every hold this jobset acquired (gate by gate).
async fn release_jobset_gates(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    jobset_id: i64,
) -> Result<(), ControlError> {
    let gates = tx
        .query(
            "SELECT repository, group_name FROM jobset_gates \
             WHERE jobset_id=$1 AND acquired",
            &[&jobset_id],
        )
        .await
        .map_err(db)?;
    for gate in gates {
        let key = (gate.get::<_, String>(0), gate.get::<_, String>(1));
        promote_after_release(backend, tx, "default", &key).await?;
    }
    Ok(())
}

/// Whether one job's matrix cohort is under its `max_parallel` cap.
async fn job_under_max_parallel(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<bool, ControlError> {
    let row = tx
        .query_opt(
            "SELECT j.base_id, s.max_parallel FROM jobs j \
             LEFT JOIN job_specs s ON s.run_id=j.run_id AND s.job_id=j.job_id \
             WHERE j.run_id=$1::text::uuid AND j.job_id=$2",
            &[&run_id.0.to_string(), &job_id.0],
        )
        .await
        .map_err(db)?;
    let Some(row) = row else { return Ok(true) };
    let Some(limit) = row.get::<_, Option<i32>>(1) else {
        return Ok(true);
    };
    let active = tx
        .query_one(
            "SELECT count(*) FROM jobs WHERE run_id=$1::text::uuid AND base_id=$2 \
             AND (status='in_progress' OR queue_state IN ('ready','claimed'))",
            &[&run_id.0.to_string(), &row.get::<_, String>(0)],
        )
        .await
        .map_err(db)?
        .get::<_, i64>(0);
    Ok(active < limit as i64)
}

/// A run-waiter promoted: its held jobs resume through the normal path.
async fn resume_held_run(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    run_id: RunId,
) -> Result<(), ControlError> {
    let mut graph = match backend.load_graph(tx, run_id).await? {
        Some(graph) => graph,
        // Foreign run not loadable in this transaction's scope → leave the
        // wait consumed; the run's own commands re-park if still needed
        // (matches the old "foreign run stays parked" behavior).
        None => return Ok(()),
    };
    let now = now_us();
    let mut promoted = Vec::new();
    for (job_id, node) in graph.nodes.iter_mut() {
        if node.queue_state == logic::QueueState::Held && node.status == ExecutionStatus::Pending {
            promoted.push(job_id.clone());
        }
    }
    for job_id in promoted {
        resume_held_node(backend, tx, &mut graph, &job_id, now).await?;
    }
    if graph.record.status == ExecutionStatus::Pending {
        graph.record.status = ExecutionStatus::InProgress;
    }
    graph.resummarize();
    flush_run(tx, &graph).await?;
    for (job_id, node) in &graph.nodes {
        flush_node(tx, run_id, job_id, node).await?;
    }
    Ok(())
}

/// A job-waiter promoted: hydrate, stamp, enqueue.
async fn resume_held_job(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<(), ControlError> {
    let mut graph = match backend.load_graph(tx, run_id).await? {
        Some(graph) => graph,
        None => return Ok(()),
    };
    resume_held_node(backend, tx, &mut graph, job_id, now_us()).await?;
    graph.resummarize();
    flush_run(tx, &graph).await?;
    if let Some(node) = graph.nodes.get(job_id) {
        flush_node(tx, run_id, job_id, node).await?;
    }
    Ok(())
}

/// One held node resuming: needs context hydrated, gate acquired-stamp,
/// ready-queue entry, assignment pairing.
async fn resume_held_node(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    graph: &mut RunGraph,
    job_id: &JobId,
    now: i64,
) -> Result<(), ControlError> {
    let Some(node) = graph.nodes.get_mut(job_id) else {
        return Ok(());
    };
    // Hydrate needs + environment into the stored template.
    if let Some(mut message) = PgBackend::node_message(tx, graph.record.run_id, job_id).await? {
        let mut queued = graph::queued_of(job_id, graph.record.run_id, node, message);
        sched_helpers::hydrate_needs_context(&mut queued, &graph.record);
        message = queued.message;
        PgBackend::write_node_message(tx, graph.record.run_id, job_id, &message).await?;
    }
    let node = graph.nodes.get_mut(job_id).expect("node present");
    let reusable = node
        .reusable
        .as_ref()
        .is_some_and(|spec| spec.call.is_some());
    if reusable {
        // Caller nodes park for expansion, not dispatch.
        node.status = ExecutionStatus::Pending;
        node.queue_state = logic::QueueState::Blocked;
        return Ok(());
    }
    node.concurrency_acquired_at_us = Some(now);
    mark_ready(node, now);
    set_status(graph, job_id, ExecutionStatus::Queued);
    on_job_enqueued(backend, tx, graph, job_id).await?;
    Ok(())
}

/// `on_job_enqueued` parity: pair the job with an idle registered runner
/// (pool/strict assignment modes) or mark it pool-pending.
pub(super) async fn on_job_enqueued(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    graph: &RunGraph,
    job_id: &JobId,
) -> Result<(), ControlError> {
    let (pool_assignments, require_assignments, _liveness) = backend.config();
    if !pool_assignments && !require_assignments {
        return Ok(());
    }
    let run_id = graph.record.run_id.0.to_string();
    let exists = tx
        .query_opt(
            "SELECT 1 FROM job_assignments WHERE run_id=$1::text::uuid AND job_id=$2",
            &[&run_id, &job_id.0],
        )
        .await
        .map_err(db)?
        .is_some();
    if exists {
        return Ok(());
    }
    tx.execute(
        "DELETE FROM provision_requests WHERE run_id=$1::text::uuid AND job_id=$2",
        &[&run_id, &job_id.0],
    )
    .await
    .map_err(db)?;
    let node = match graph.nodes.get(job_id) {
        Some(node) => node.clone(),
        None => return Ok(()),
    };
    // Busy runners: assigned or holding a live request.
    let busy_rows = tx
        .query(
            "SELECT runner_id FROM job_assignments WHERE runner_id IS NOT NULL \
             UNION SELECT runner_id FROM runner_sessions WHERE runner_id IS NOT NULL \
             AND EXISTS (SELECT 1 FROM job_requests q WHERE q.session_id=runner_sessions.session_id \
             AND q.result IS NULL)",
            &[],
        )
        .await
        .map_err(db)?;
    let busy: BTreeSet<i64> = busy_rows.iter().map(|r| r.get(0)).collect();
    let candidates = tx
        .query(
            "SELECT DISTINCT s.runner_id FROM runner_sessions s \
             JOIN runners r ON r.runner_id=s.runner_id WHERE s.runner_id IS NOT NULL",
            &[],
        )
        .await
        .map_err(db)?;
    for row in candidates {
        let runner_id: i64 = row.get(0);
        if busy.contains(&runner_id) {
            continue;
        }
        let runner_row = tx
            .query_opt(
                "SELECT labels::text, runner_group_id, runner_group_name, pool_proven \
                 FROM runners WHERE runner_id=$1",
                &[&runner_id],
            )
            .await
            .map_err(db)?;
        let Some(runner_row) = runner_row else {
            continue;
        };
        if pool_assignments && !runner_row.get::<_, bool>(3) {
            continue;
        }
        let caps = crate::models::RunnerCapabilities {
            known: true,
            labels: from_json(runner_row.get::<_, String>(0).as_str())?,
            runner_group_id: runner_row.get(1),
            runner_group_name: runner_row.get(2),
        };
        // Capability match uses the two shared predicates directly: a
        // `QueuedJob` needs a full message, which dispatch pairing never reads.
        if sched_helpers::job_matches_runner(&node.runs_on, &caps.labels)
            && sched_helpers::job_matches_runner_group(node.runner_group.as_deref(), &caps)
        {
            tx.execute(
                "INSERT INTO job_assignments (run_id, job_id, runner_id) \
                 VALUES ($1::text::uuid,$2,$3)",
                &[&run_id, &job_id.0, &runner_id],
            )
            .await
            .map_err(db)?;
            return Ok(());
        }
    }
    if pool_assignments {
        tx.execute(
            "INSERT INTO provision_requests (run_id, job_id, namespace_id, pool_key, labels) \
             VALUES ($1::text::uuid,$2,$3,$4,$5::text::jsonb) ON CONFLICT DO NOTHING",
            &[
                &run_id,
                &job_id.0,
                &graph.namespace,
                &node.pool_key,
                &serde_json::to_string(&node.runs_on).unwrap_or_else(|_| "[]".into()),
            ],
        )
        .await
        .map_err(db)?;
    }
    Ok(())
}

/// `clear_assignment`: drop the assignment/pool-pending rows for a job.
async fn clear_assignment(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<(), ControlError> {
    tx.execute(
        "DELETE FROM job_assignments WHERE run_id=$1::text::uuid AND job_id=$2",
        &[&run_id.0.to_string(), &job_id.0],
    )
    .await
    .map_err(db)?;
    tx.execute(
        "DELETE FROM provision_requests WHERE run_id=$1::text::uuid AND job_id=$2",
        &[&run_id.0.to_string(), &job_id.0],
    )
    .await
    .map_err(db)?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// Cancellation
// ─────────────────────────────────────────────────────────────────────────

/// `cancel_run_inner` parity: settle every non-terminal job, queue a
/// cancellation for in-flight attempts, retire expandable-node requests,
/// release every concurrency presence. Returns cancellations queued.
async fn cancel_run_tx(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    run_id: RunId,
    reason: Option<&str>,
) -> Result<usize, ControlError> {
    let _ = reason;
    let rows = tx
        .query(
            "SELECT job_id, status FROM jobs WHERE run_id=$1::text::uuid \
             FOR UPDATE",
            &[&run_id.0.to_string()],
        )
        .await
        .map_err(db)?;
    let now = now_us();
    let mut cancellations = 0;
    for row in &rows {
        let job_id = JobId(row.get::<_, String>(0));
        let status: String = row.get(1);
        let in_progress = status == "in_progress";
        if matches!(status.as_str(), "queued" | "in_progress") {
            cancellations += if enqueue_cancellation_job(tx, run_id, &job_id, in_progress).await? {
                1
            } else {
                0
            };
            tx.execute(
                concat!(
                    "UPDATE jobs SET status='cancelled', queue_state='none', \
                     completed_at=",
                    us!("$3"),
                    " WHERE run_id=$1::text::uuid AND job_id=$2"
                ),
                &[&run_id.0.to_string(), &job_id.0, &now],
            )
            .await
            .map_err(db)?;
            retire_node_requests(
                tx,
                run_id,
                &job_id,
                Retirement::Settle(ExecutionStatus::Cancelled),
            )
            .await?;
        }
    }
    // Pending jobs (status 'pending' — concurrency-held/blocked nodes) also
    // settle; they carry no claimable attempt yet.
    tx.execute(
        concat!(
            "UPDATE jobs SET status='cancelled', queue_state='none', completed_at=",
            us!("$2"),
            " WHERE run_id=$1::text::uuid AND status='pending'"
        ),
        &[&run_id.0.to_string(), &now],
    )
    .await
    .map_err(db)?;
    tx.execute(
        "UPDATE runs SET status='completed', conclusion='cancelled', \
         completed_at=now() WHERE run_id=$1::text::uuid AND status <> 'completed'",
        &[&run_id.0.to_string()],
    )
    .await
    .map_err(db)?;
    // Clear dispatch side queues for this run.
    tx.execute(
        "DELETE FROM job_assignments WHERE run_id=$1::text::uuid",
        &[&run_id.0.to_string()],
    )
    .await
    .map_err(db)?;
    tx.execute(
        "DELETE FROM provision_requests WHERE run_id=$1::text::uuid",
        &[&run_id.0.to_string()],
    )
    .await
    .map_err(db)?;
    tx.execute(
        "DELETE FROM jobsets WHERE run_id=$1::text::uuid",
        &[&run_id.0.to_string()],
    )
    .await
    .map_err(db)?;
    release_concurrency_for_run(backend, tx, run_id).await?;
    Ok(cancellations)
}

/// Cancellation bookkeeping: queue the runner-facing message only for an
/// in-flight attempt.
async fn enqueue_cancellation_job(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    _in_progress: bool,
) -> Result<bool, ControlError> {
    let Some(row) = tx
        .query_opt(
            "SELECT request_id, agent_job_id::text, session_id::text \
             FROM job_requests WHERE run_id=$1::text::uuid AND job_id=$2 \
             AND result IS NULL ORDER BY request_id DESC LIMIT 1",
            &[&run_id.0.to_string(), &job_id.0],
        )
        .await
        .map_err(db)?
    else {
        return Ok(false);
    };
    let request_id: i64 = row.get(0);
    let Some(session_id) = row.get::<_, Option<String>>(2) else {
        return Ok(false);
    };
    tx.execute(
        "INSERT INTO job_cancellations (request_id) VALUES ($1)",
        &[&request_id],
    )
    .await
    .map_err(db)?;
    let agent = row.get::<_, String>(1);
    tx.execute(
        "INSERT INTO session_messages (session_id, message_type, request_id, body) \
         VALUES ($1::text::uuid,'job.cancellation',$2,$3::text::jsonb)",
        &[
            &session_id,
            &request_id,
            &concurrency::job_cancel_body(codec::uuid(&agent)?),
        ],
    )
    .await
    .map_err(db)?;
    Ok(true)
}

/// `cancel_job_inner` parity: cancel one job plus the inner jobs of an
/// expanded reusable caller.
async fn cancel_job_tx(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    reason: Option<&str>,
) -> Result<usize, ControlError> {
    let _ = reason;
    let row = tx
        .query_opt(
            "SELECT status, kind FROM jobs WHERE run_id=$1::text::uuid \
             AND job_id=$2 FOR UPDATE",
            &[&run_id.0.to_string(), &job_id.0],
        )
        .await
        .map_err(db)?;
    let Some(row) = row else { return Ok(0) };
    let status: String = row.get(0);
    let now = now_us();
    let mut count = 0;
    if matches!(status.as_str(), "queued" | "in_progress" | "pending") {
        if enqueue_cancellation_job(tx, run_id, job_id, true).await? {
            count = 1;
        }
        tx.execute(
            concat!(
                "UPDATE jobs SET status='cancelled', queue_state='none', \
                 completed_at=",
                us!("3"),
                " WHERE run_id=$1::text::uuid AND job_id=$2"
            ),
            &[&run_id.0.to_string(), &job_id.0, &now],
        )
        .await
        .map_err(db)?;
        clear_assignment(tx, run_id, job_id).await?;
        retire_node_requests(
            tx,
            run_id,
            job_id,
            Retirement::Settle(ExecutionStatus::Cancelled),
        )
        .await?;
        release_concurrency_for_job(backend, tx, run_id, job_id).await?;
    }
    // Recurse into an expanded caller's inner jobs.
    if row.get::<_, String>(1) == "reusable_caller" {
        let spec = tx
            .query_opt(
                "SELECT reusable_call::text FROM job_specs \
                 WHERE run_id=$1::text::uuid AND job_id=$2",
                &[&run_id.0.to_string(), &job_id.0],
            )
            .await
            .map_err(db)?;
        if let Some(spec) = spec {
            let spec: ReusableNodeSpec = serde_json::from_str(spec.get::<_, String>(0).as_str())
                .map_err(ControlError::backend)?;
            if let Some(meta) = spec.meta {
                for inner in meta.inner_job_ids {
                    count +=
                        Box::pin(cancel_job_tx(backend, tx, run_id, &JobId(inner), reason)).await?;
                }
            }
        }
    }
    Ok(count)
}

// ─────────────────────────────────────────────────────────────────────────
// Promotion sweep
// ─────────────────────────────────────────────────────────────────────────

/// One command's mutable run graphs and the scheduling state that crosses
/// their borders (gates can cancel/promote foreign runs).
pub(super) struct Sweep<'a> {
    backend: &'a PgBackend,
    tx: &'a Transaction<'a>,
    pub(crate) graphs: BTreeMap<RunId, RunGraph>,
    /// Nodes whose `jobs` row changed this command.
    dirty: BTreeSet<(RunId, JobId)>,
    /// Node ids whose message rows were hydrated.
    hydrated: BTreeSet<(RunId, JobId)>,
    /// Message templates loaded for decisions/hydration, by node.
    messages: BTreeMap<(RunId, JobId), Option<azdo::AgentJobRequestMessage>>,
    pub(crate) outcome: SchedulingOutcome,
    /// `(run, job)` of nodes concluded this command (for outcome lists).
    pub(crate) concluded: Vec<(JobId, ExecutionStatus)>,
    /// Cancellation messages queued (holder displacement, cancels).
    pub(crate) cancellations: usize,
    now: i64,
}

impl<'a> Sweep<'a> {
    pub(super) async fn new(
        backend: &'a PgBackend,
        tx: &'a Transaction<'a>,
    ) -> Result<Self, ControlError> {
        Ok(Self {
            backend,
            tx,
            graphs: BTreeMap::new(),
            dirty: BTreeSet::new(),
            hydrated: BTreeSet::new(),
            messages: BTreeMap::new(),
            outcome: SchedulingOutcome::default(),
            concluded: Vec::new(),
            cancellations: 0,
            now: now_us(),
        })
    }

    /// The graph for `run_id`, loading (and marking dirty on flush) on miss.
    pub(super) async fn graph(&mut self, run_id: RunId) -> Result<&mut RunGraph, ControlError> {
        if !self.graphs.contains_key(&run_id) {
            let graph = self
                .backend
                .load_graph(self.tx, run_id)
                .await?
                .ok_or_else(|| ControlError::NotFound(format!("run {run_id}")))?;
            self.graphs.insert(run_id, graph);
        }
        Ok(self.graphs.get_mut(&run_id).expect("just inserted"))
    }

    /// A node's message template, loaded once per command.
    async fn message(
        &mut self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<azdo::AgentJobRequestMessage>, ControlError> {
        let key = (run_id, job_id.clone());
        if let Some(cached) = self.messages.get(&key) {
            return Ok(cached.clone());
        }
        let loaded = PgBackend::node_message(self.tx, run_id, job_id).await?;
        self.messages.insert(key, loaded.clone());
        Ok(loaded)
    }

    /// A node's `QueuedJob` view for the decision functions.
    async fn node_job(
        &mut self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<QueuedJob>, ControlError> {
        if !self
            .graphs
            .get(&run_id)
            .is_some_and(|graph| graph.nodes.contains_key(job_id))
        {
            return Ok(None);
        }
        let Some(message) = self.message(run_id, job_id).await? else {
            return Ok(None);
        };
        let graph = self.graphs.get(&run_id).expect("checked above");
        let node = graph.nodes.get(job_id).expect("checked above");
        Ok(Some(QueuedJob {
            run_id,
            job_id: job_id.clone(),
            base_id: node.base_id.clone(),
            created_at_unix_nanos: node.created_at_us.saturating_mul(1000),
            dependencies_ready_at_unix_nanos: node.deps_ready_at_us.map(|v| v.saturating_mul(1000)),
            concurrency_wait_started_at_unix_nanos: node
                .concurrency_wait_at_us
                .map(|v| v.saturating_mul(1000)),
            concurrency_acquired_at_unix_nanos: node
                .concurrency_acquired_at_us
                .map(|v| v.saturating_mul(1000)),
            enqueued_at_unix_nanos: node.enqueued_at_us.unwrap_or(0).saturating_mul(1000),
            needs: node.needs.clone(),
            if_condition: node.if_condition.clone(),
            condition_context: node.condition_context.clone(),
            max_parallel: node.max_parallel,
            runs_on: node.runs_on.clone(),
            runner_group: node.runner_group.clone(),
            environment: node.environment.clone(),
            concurrency: node.concurrency.clone(),
            matrix: node.matrix.clone(),
            deferred_matrix: node.deferred_matrix.clone(),
            reusable_call: node.reusable.as_ref().and_then(|s| s.call.clone()),
            message,
        }))
    }

    fn mark(&mut self, run_id: RunId, job_id: &JobId) {
        self.dirty.insert((run_id, job_id.clone()));
    }

    fn node_mut(&mut self, run_id: RunId, job_id: &JobId) -> Option<&mut Node> {
        self.graphs.get_mut(&run_id)?.nodes.get_mut(job_id)
    }

    /// `dependency_decision` against the current graph. The record's `jobs`
    /// map is the read view — callers keep it synchronized through
    /// `set_status`/`rebuild_jobs`.
    async fn decide(
        &mut self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<DependencyDecision, ControlError> {
        let Some(job) = self.node_job(run_id, job_id).await? else {
            return Ok(DependencyDecision::Wait);
        };
        let Some(graph) = self.graphs.get(&run_id) else {
            return Ok(DependencyDecision::Wait);
        };
        Ok(sched_helpers::dependency_decision(&graph.record, &job))
    }

    /// Flush every dirty node + touched run row.
    pub(super) async fn flush(self) -> Result<(), ControlError> {
        for (run_id, job_id) in &self.dirty {
            if let Some(node) = self
                .graphs
                .get(run_id)
                .and_then(|graph| graph.nodes.get(job_id))
            {
                flush_node(self.tx, *run_id, job_id, node).await?;
            }
        }
        for graph in self.graphs.values() {
            if graph.touched {
                flush_run(self.tx, graph).await?;
            }
        }
        Ok(())
    }

    /// `promote_ready_jobs` parity: sweep `blocked`/`held`-adjacent nodes
    /// until fixpoint. Operates over every loaded graph plus newly settled
    /// dependencies.
    pub(super) async fn sweep(&mut self) -> Result<(), ControlError> {
        loop {
            let mut settled = false;
            let mut promoted_by_base: BTreeMap<(RunId, String), u64> = BTreeMap::new();
            let candidates: Vec<(RunId, JobId)> = self
                .graphs
                .iter()
                .flat_map(|(run_id, graph)| {
                    graph
                        .nodes
                        .iter()
                        .filter(|(_, node)| {
                            node.queue_state == logic::QueueState::Blocked
                                && node.remaining_needs <= 0
                        })
                        .map(|(job_id, _)| (*run_id, job_id.clone()))
                        .collect::<Vec<_>>()
                })
                .collect();
            for (run_id, job_id) in candidates {
                let decision = self.decide(run_id, &job_id).await?;
                match decision {
                    DependencyDecision::Run => {
                        self.promote(run_id, &job_id, &mut promoted_by_base).await?;
                    }
                    DependencyDecision::Skip | DependencyDecision::Error => {
                        let status = if decision == DependencyDecision::Skip {
                            ExecutionStatus::Skipped
                        } else {
                            ExecutionStatus::Failure
                        };
                        self.settle_node(run_id, &job_id, status).await?;
                        settled = true;
                    }
                    DependencyDecision::Wait => {}
                }
            }
            // Max-parallel admissions: legs admitted this pass count toward
            // the cap.
            let _ = promoted_by_base;
            if !settled && promoted_by_base.is_empty() {
                return Ok(());
            }
            if !settled {
                return Ok(());
            }
        }
    }

    /// A blocked node whose needs settled promotes: jobset gates for a
    /// reusable caller, expansion for a deferred node, gate + ready for a
    /// dispatchable job.
    async fn promote(
        &mut self,
        run_id: RunId,
        job_id: &JobId,
        promoted_by_base: &mut BTreeMap<(RunId, String), u64>,
    ) -> Result<(), ControlError> {
        let (is_caller, is_matrix, max_parallel, base_id, has_gates) = {
            let Some(node) = self.graphs.get(&run_id).and_then(|g| g.nodes.get(job_id)) else {
                return Ok(());
            };
            (
                node.reusable
                    .as_ref()
                    .is_some_and(|spec| spec.call.is_some()),
                node.deferred_matrix.is_some(),
                node.max_parallel,
                node.base_id.clone(),
                node.concurrency.is_some(),
            )
        };
        let now = self.now;
        if let Some(node) = self.node_mut(run_id, job_id) {
            node.deps_ready_at_us = Some(now);
        }
        if is_caller {
            // JobSet admission: caller/embedded gates in one acquisition set.
            let gates = self.caller_gates(run_id, job_id).await?;
            match gates {
                Err(status) => {
                    self.settle_node(run_id, job_id, status).await?;
                    return Ok(());
                }
                Ok(gates) => {
                    if !gates.is_empty() {
                        let jobset_id = self.ensure_jobset(run_id, job_id, &gates).await?;
                        match advance_jobset(self.backend, self.tx, run_id, jobset_id, None).await?
                        {
                            JobsetAdvance::Ready => {
                                self.set_jobset_ready(run_id, jobset_id).await?;
                            }
                            JobsetAdvance::Blocked => {
                                self.hold_node(run_id, job_id);
                                return Ok(());
                            }
                            JobsetAdvance::Failed => {
                                self.settle_node(run_id, job_id, ExecutionStatus::Failure)
                                    .await?;
                                return Ok(());
                            }
                        }
                    }
                    self.defer_expansion(run_id, job_id).await?;
                    return Ok(());
                }
            }
        }
        if is_matrix {
            self.defer_expansion(run_id, job_id).await?;
            return Ok(());
        }
        // Max-parallel: matrix legs queue behind the cohort cap.
        if let Some(limit) = max_parallel {
            let admitted_so_far = promoted_by_base
                .get(&(run_id, base_id.clone()))
                .copied()
                .unwrap_or(0);
            if !self.under_max_parallel(run_id, job_id, &base_id, limit, admitted_so_far) {
                // Stays `blocked` — a later leg completion re-admits it.
                return Ok(());
            }
            *promoted_by_base
                .entry((run_id, base_id.clone()))
                .or_insert(0) += 1;
        }
        // Job-level gate.
        if has_gates {
            match self.job_gate(run_id, job_id).await? {
                GateOutcome::Acquired => {}
                GateOutcome::Parked => {
                    self.hold_node(run_id, job_id);
                    return Ok(());
                }
                GateOutcome::Cancelled => {
                    self.settle_node(run_id, job_id, ExecutionStatus::Cancelled)
                        .await?;
                    return Ok(());
                }
                GateOutcome::Failed => {
                    self.settle_node(run_id, job_id, ExecutionStatus::Failure)
                        .await?;
                    return Ok(());
                }
            }
            let now = self.now;
            if let Some(node) = self.node_mut(run_id, job_id) {
                node.concurrency_acquired_at_us = Some(now);
            }
        }
        self.enqueue(run_id, job_id).await
    }

    /// Hydrate + enqueue one promotable job.
    async fn enqueue(&mut self, run_id: RunId, job_id: &JobId) -> Result<(), ControlError> {
        // Hydrate needs context into the stored message template.
        if let Some(message) = self.message(run_id, job_id).await? {
            let (mut queued, record) = {
                let graph = self.graphs.get(&run_id).expect("loaded");
                let node = graph.nodes.get(job_id).expect("loaded");
                (
                    graph::queued_of(job_id, run_id, node, message),
                    graph.record.clone(),
                )
            };
            sched_helpers::hydrate_needs_context(&mut queued, &record);
            PgBackend::write_node_message(self.tx, run_id, job_id, &queued.message).await?;
            self.messages
                .insert((run_id, job_id.clone()), Some(queued.message));
            self.hydrated.insert((run_id, job_id.clone()));
        }
        let now = self.now;
        let graph = self.graph(run_id).await?;
        let Some(node) = graph.nodes.get_mut(job_id) else {
            return Ok(());
        };
        mark_ready(node, now);
        graph
            .record
            .jobs
            .insert(job_id.clone(), ExecutionStatus::Queued);
        self.mark(run_id, job_id);
        self.outcome.promoted += 1;
        let graph = self.graphs.get(&run_id).expect("loaded");
        on_job_enqueued(self.backend, self.tx, graph, job_id).await
    }

    /// Park a promotable node behind concurrency (`held` state).
    fn hold_node(&mut self, run_id: RunId, job_id: &JobId) {
        let now = self.now;
        if let Some(node) = self.node_mut(run_id, job_id) {
            node.status = ExecutionStatus::Pending;
            node.queue_state = logic::QueueState::Held;
            node.concurrency_wait_at_us = Some(now);
            self.mark(run_id, job_id);
            let graph = self.graphs.get_mut(&run_id).expect("loaded");
            graph
                .record
                .jobs
                .insert(job_id.clone(), ExecutionStatus::Pending);
        }
    }

    /// Defer a node to the expansion queue (`pending_expansion`).
    async fn defer_expansion(&mut self, run_id: RunId, job_id: &JobId) -> Result<(), ControlError> {
        let now = self.now;
        if let Some(node) = self.node_mut(run_id, job_id) {
            node.queue_state = logic::QueueState::PendingExpansion;
            node.enqueued_at_us = Some(now);
            self.mark(run_id, job_id);
        }
        Ok(())
    }

    /// Settle a node terminal: status, release gates, retire requests, run
    /// resummary, decrement dependent `remaining_needs` rows.
    pub(super) async fn settle_node(
        &mut self,
        run_id: RunId,
        job_id: &JobId,
        status: ExecutionStatus,
    ) -> Result<(), ControlError> {
        // The node's own transition.
        let now = self.now;
        {
            let Some(node) = self.node_mut(run_id, job_id) else {
                return Ok(());
            };
            mark_terminal(node, status, now);
        }
        set_status_in(self.graphs.get_mut(&run_id), job_id, status);
        self.mark(run_id, job_id);
        let _ = release_concurrency_for_job(self.backend, self.tx, run_id, job_id).await;
        let expandable = self
            .graphs
            .get(&run_id)
            .and_then(|g| g.nodes.get(job_id))
            .is_some_and(|node| node.expandable());
        if expandable {
            retire_node_requests(self.tx, run_id, job_id, Retirement::Settle(status)).await?;
        }
        clear_assignment(self.tx, run_id, job_id).await?;
        // Dependents' remaining_needs decrement.
        let dependents: Vec<String> = self
            .tx
            .query(
                "SELECT job_id FROM job_needs WHERE run_id=$1::text::uuid \
                 AND needs_job_id=$2",
                &[&run_id.0.to_string(), &job_id.0],
            )
            .await
            .map_err(db)?
            .iter()
            .map(|row| row.get(0))
            .collect();
        for dependent in dependents {
            let dependent = JobId(dependent);
            let Some(node) = self.node_mut(run_id, &dependent) else {
                continue;
            };
            let (next, promotable) = logic::decrement_remaining_needs(node.remaining_needs, true);
            node.remaining_needs = next;
            if promotable {
                node.deps_ready_at_us = Some(now);
            }
            self.mark(run_id, &dependent);
        }
        self.outcome.push(status, run_id, job_id.clone());
        self.concluded.push((job_id.clone(), status));
        if let Some(graph) = self.graphs.get_mut(&run_id) {
            graph.resummarize();
            graph.touched = true;
        }
        Ok(())
    }

    /// The reusable caller's merged JobSet gates (`caller_jobset_gates`
    /// parity), evaluated against the run's contexts.
    async fn caller_gates(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Result<Vec<JobSetGate>, ExecutionStatus>, ControlError> {
        let graph = self.graphs.get(&run_id).expect("caller graph loaded");
        let Some(node) = graph.nodes.get(job_id) else {
            return Ok(Err(ExecutionStatus::Failure));
        };
        let Some(meta) = node.reusable.as_ref().and_then(|spec| spec.meta.clone()) else {
            return Ok(Ok(Vec::new()));
        };
        let submission = &graph.submission;
        let mut gates = Vec::new();
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
                github: &graph.github,
                vars: &submission.vars,
                inputs,
                matrix: Some(&node.matrix),
                strategy: None,
                needs: None,
            };
            match concurrency::evaluate_concurrency(raw, &eval_ctx) {
                Ok((group, cancel_in_progress, queue)) if !group.trim().is_empty() => {
                    sched_merge_gate(
                        &mut gates,
                        JobSetGate {
                            key: concurrency::concurrency_key(&submission.repository, &group),
                            display_name: group,
                            cancel_in_progress,
                            queue,
                        },
                    );
                }
                Ok(_) => return Ok(Err(ExecutionStatus::Failure)),
                Err(error) => {
                    concurrency::log_eval_error(label, &error);
                    return Ok(Err(ExecutionStatus::Failure));
                }
            }
        }
        Ok(Ok(gates))
    }

    /// `merge_jobset_gate` parity.
    fn merge_gate(gates: &mut Vec<JobSetGate>, gate: JobSetGate) {
        sched_merge_gate(gates, gate)
    }

    /// Register (or find) the jobset row for a caller and its gate rows.
    async fn ensure_jobset(
        &mut self,
        run_id: RunId,
        job_id: &JobId,
        gates: &[JobSetGate],
    ) -> Result<i64, ControlError> {
        let ids = serde_json::to_string(&[job_id.0.clone()]).unwrap_or_default();
        let row = self
            .tx
            .query_opt(
                "SELECT jobset_id FROM jobsets WHERE run_id=$1::text::uuid \
                 AND job_ids=$2::text::jsonb",
                &[&run_id.0.to_string(), &ids],
            )
            .await
            .map_err(db)?;
        if let Some(row) = row {
            return Ok(row.get(0));
        }
        let jobset_id: i64 = self
            .tx
            .query_one(
                "INSERT INTO jobsets (run_id, job_ids, state) \
                 VALUES ($1::text::uuid,$2::text::jsonb,'waiting') \
                 RETURNING jobset_id",
                &[&run_id.0.to_string(), &ids],
            )
            .await
            .map_err(db)?
            .get(0);
        for (index, gate) in gates.iter().enumerate() {
            self.tx
                .execute(
                    "INSERT INTO jobset_gates (jobset_id, gate_index, repository, \
                     group_name, display_name, cancel_in_progress, queue_mode) \
                     VALUES ($1,$2,$3,$4,$5,$6,$7)",
                    &[
                        &jobset_id,
                        &(index as i32),
                        &gate.key.0,
                        &gate.key.1,
                        &gate.display_name,
                        &gate.cancel_in_progress,
                        &concurrency::queue_mode_row(&gate.queue),
                    ],
                )
                .await
                .map_err(db)?;
        }
        Ok(jobset_id)
    }

    /// Mark a jobset fully admitted; members parked behind it resume.
    async fn set_jobset_ready(
        &mut self,
        run_id: RunId,
        jobset_id: i64,
    ) -> Result<(), ControlError> {
        self.tx
            .execute(
                "UPDATE jobsets SET state='ready' WHERE jobset_id=$1",
                &[&jobset_id],
            )
            .await
            .map_err(db)?;
        let _ = run_id;
        Ok(())
    }

    /// Whether the cohort of `base_id` legs is under `limit` (queue + running
    /// + admitted this pass).
    fn under_max_parallel(
        &self,
        run_id: RunId,
        _job_id: &JobId,
        base_id: &str,
        limit: u64,
        admitted_this_pass: u64,
    ) -> bool {
        let Some(graph) = self.graphs.get(&run_id) else {
            return true;
        };
        let mut active = admitted_this_pass;
        for node in graph.nodes.values() {
            if node.base_id == base_id
                && (node.status == ExecutionStatus::InProgress
                    || matches!(
                        node.queue_state,
                        logic::QueueState::Ready | logic::QueueState::Claimed
                    ))
            {
                active += 1;
            }
        }
        active < limit
    }

    /// Evaluate + acquire a job's own `concurrency:` gate.
    async fn job_gate(
        &mut self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<GateOutcome, ControlError> {
        let (raw, matrix) = {
            let Some(node) = self.graphs.get(&run_id).and_then(|g| g.nodes.get(job_id)) else {
                return Ok(GateOutcome::Acquired);
            };
            (node.concurrency.clone(), node.matrix.clone())
        };
        let Some(raw) = raw else {
            return Ok(GateOutcome::Acquired);
        };
        let (github, submission) = {
            let graph = self.graphs.get(&run_id).expect("loaded");
            (graph.github.clone(), graph.submission.clone())
        };
        let message = self.message(run_id, job_id).await?;
        let strategy = message
            .as_ref()
            .and_then(|m| m.context_data.get("strategy"))
            .map(azdo::PipelineContextData::to_json)
            .unwrap_or_else(|| serde_json::json!({}));
        let eval_ctx = concurrency::ConcurrencyContext {
            scope: concurrency::ConcurrencyScope::Job,
            github: &github,
            vars: &submission.vars,
            inputs: &submission.inputs,
            matrix: Some(&matrix),
            strategy: Some(&strategy),
            needs: None,
        };
        let (group, cancel, queue) = match concurrency::evaluate_concurrency(&raw, &eval_ctx) {
            Ok(v) => v,
            Err(e) => {
                concurrency::log_eval_error("job concurrency", &e);
                return Ok(GateOutcome::Failed);
            }
        };
        if group.trim().is_empty() {
            return Ok(GateOutcome::Failed);
        }
        let key = concurrency::concurrency_key(&submission.repository, &group);
        let namespace = self
            .graphs
            .get(&run_id)
            .map(|g| g.namespace.clone())
            .unwrap_or_else(|| "default".to_owned());
        acquire_gate(
            self.backend,
            self.tx,
            &namespace,
            &key,
            &group,
            &GateHolder {
                holder: concurrency::Holder::Job {
                    run_id,
                    job_id: job_id.clone(),
                },
                jobset_id: None,
            },
            cancel,
            queue,
        )
        .await
    }
}

/// `merge_jobset_gate` parity: same-key gates merge (stricter cancel/queue).
fn sched_merge_gate(gates: &mut Vec<JobSetGate>, mut gate: JobSetGate) {
    if let Some(existing) = gates.iter_mut().find(|existing| existing.key == gate.key) {
        existing.cancel_in_progress |= gate.cancel_in_progress;
        if gate.queue == ConcurrencyQueue::Single {
            existing.queue = ConcurrencyQueue::Single;
        }
        return;
    }
    gate.display_name = gate.display_name.trim().to_owned();
    gates.push(gate);
    gates.sort_by(|left, right| left.key.cmp(&right.key));
}

/// Mirror a node status into `record.jobs` (contributing nodes only).
fn set_status_in(graph: Option<&mut RunGraph>, job_id: &JobId, status: ExecutionStatus) {
    if let Some(graph) = graph {
        if graph
            .nodes
            .get(job_id)
            .is_some_and(|node| node.contributes())
        {
            graph.record.jobs.insert(job_id.clone(), status);
        }
    }
}

impl SchedulingOutcome {
    fn push(&mut self, status: ExecutionStatus, run_id: RunId, job_id: JobId) {
        match status {
            ExecutionStatus::Skipped => self.skipped.push((run_id, job_id)),
            ExecutionStatus::Failure | ExecutionStatus::Cancelled => {
                self.failed.push((run_id, job_id))
            }
            _ => {}
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Outbox + claim requeue
// ─────────────────────────────────────────────────────────────────────────

/// Append one durable event to the transactional outbox (insert-only).
/// `run_seq` is the run's next sequence; events without a run carry none.
pub(super) async fn append_event_tx(
    tx: &Transaction<'_>,
    event: &preloop_gha_protocol::NdjsonEvent,
) -> Result<(), ControlError> {
    let run_id = crate::control::backend::event_run_id(event);
    let payload = serde_json::to_string(event).map_err(ControlError::backend)?;
    // The versioned topic is the event's serde tag (`job_status` -> `job_status.v1`).
    let topic = serde_json::from_str::<serde_json::Value>(&payload)
        .ok()
        .and_then(|value| {
            value
                .get("type")
                .and_then(|t| t.as_str())
                .map(str::to_owned)
        })
        .map(|kind| format!("{kind}.v1"))
        .unwrap_or_else(|| "event.v1".to_owned());
    let namespace = match run_id {
        Some(run_id) => tx
            .query_opt(
                "SELECT namespace_id FROM runs WHERE run_id = $1::text::uuid",
                &[&run_id.0.to_string()],
            )
            .await
            .map_err(db)?
            .map(|row| row.get::<_, String>(0))
            .unwrap_or_else(|| crate::control::types::DEFAULT_NAMESPACE.to_owned()),
        None => crate::control::types::DEFAULT_NAMESPACE.to_owned(),
    };
    let run_seq: Option<i64> = match run_id {
        Some(run_id) => tx
            .query_opt(
                "UPDATE runs SET event_seq = event_seq + 1 \
                 WHERE run_id = $1::text::uuid RETURNING event_seq",
                &[&run_id.0.to_string()],
            )
            .await
            .map_err(db)?
            .map(|row| row.get(0)),
        None => None,
    };
    tx.execute(
        "INSERT INTO outbox_events (namespace_id, run_id, run_seq, topic, payload) \
         VALUES ($1,$2::text::uuid,$3,$4,$5::text::jsonb)",
        &[
            &namespace,
            &run_id.map(|id| id.0.to_string()),
            &run_seq,
            &topic,
            &payload,
        ],
    )
    .await
    .map_err(db)?;
    Ok(())
}

/// Requeue a claimed job whose runner is gone: drop the claim (owner,
/// session, start stamp, lease) and put the node back at the head of the
/// ready queue (its `jobs` row goes back to `queued`/`ready`).
pub(super) async fn requeue_claimed_tx(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<bool, ControlError> {
    let run = run_id.0.to_string();
    let Some(row) = tx
        .query_opt(
            "SELECT status, queue_state FROM jobs WHERE run_id = $1::text::uuid \
             AND job_id = $2 FOR UPDATE",
            &[&run, &job_id.0],
        )
        .await
        .map_err(db)?
    else {
        return Ok(false);
    };
    let state: String = row.get(1);
    if state != "claimed" {
        return Ok(false);
    }
    let request = tx
        .query_opt(
            "SELECT request_id FROM job_requests WHERE run_id = $1::text::uuid \
             AND job_id = $2 AND result IS NULL ORDER BY request_id DESC LIMIT 1 \
             FOR NO KEY UPDATE",
            &[&run, &job_id.0],
        )
        .await
        .map_err(db)?;
    if let Some(row) = request {
        let request_id: i64 = row.get(0);
        tx.execute(
            "UPDATE job_requests SET runner_id = NULL, session_id = NULL, \
             started_at = NULL, timeout_triggered = false \
             WHERE request_id = $1 AND result IS NULL",
            &[&request_id],
        )
        .await
        .map_err(db)?;
        tx.execute(
            "DELETE FROM job_leases WHERE request_id = $1",
            &[&request_id],
        )
        .await
        .map_err(db)?;
    }
    tx.execute(
        "UPDATE jobs SET status = 'queued', queue_state = 'ready', \
         claimed_by_runner_id = NULL, claimed_at = NULL, \
         enqueued_at = COALESCE(enqueued_at, now()) \
         WHERE run_id = $1::text::uuid AND job_id = $2",
        &[&run, &job_id.0],
    )
    .await
    .map_err(db)?;
    tx.execute(
        "DELETE FROM job_assignments WHERE run_id = $1::text::uuid AND job_id = $2",
        &[&run, &job_id.0],
    )
    .await
    .map_err(db)?;
    Ok(true)
}

// ─────────────────────────────────────────────────────────────────────────
// Run submission
// ─────────────────────────────────────────────────────────────────────────

/// `runs.ref_type` from the trigger ref (`github.ref` shape).
pub(super) fn ref_type(git_ref: &str, event: &str) -> &'static str {
    if git_ref.starts_with("refs/pull/") {
        "pull_request"
    } else if git_ref.starts_with("refs/tags/") {
        "tag"
    } else if git_ref.starts_with("refs/heads/") {
        "branch"
    } else if event == "release" || event == "create" {
        "tag"
    } else {
        "other"
    }
}

/// `runs.origin` for a submission: the delivery id names a webhook, a
/// schedule event names a schedule, a local workspace names the CLI; anything
/// else arrived over the API.
fn run_origin(record: &RunRecord) -> &'static str {
    if record.webhook_delivery_id.is_some() {
        "webhook"
    } else if record.event == "schedule" {
        "schedule"
    } else if record.submission.local_workspace.is_some() {
        "cli"
    } else {
        "api"
    }
}

/// The node classification of a submitted job (`jobs.kind`).
fn submit_kind(job: &QueuedJob) -> NodeKind {
    if job.reusable_call.is_some() {
        NodeKind::ReusableCaller
    } else if job.deferred_matrix.is_some() {
        NodeKind::MatrixParent
    } else if !job.matrix.is_empty() && job.base_id != job.job_id.0 {
        NodeKind::MatrixLeg
    } else {
        NodeKind::Job
    }
}

/// Secret names carried by a message template, with every secret value (and
/// the mask hints / endpoint credentials derived from them) removed. The
/// acquire path resolves the names through the `SecretProvider` and fills the
/// template; the runner still sees which variables are secret.
fn strip_secret_values(message: &mut azdo::AgentJobRequestMessage) -> Vec<String> {
    let mut names = Vec::new();
    for (name, value) in message.variables.iter_mut() {
        if value.is_secret == Some(true) {
            names.push(name.clone());
            value.value = None;
        }
    }
    // Mask hints carry the secret values they redact; the acquire path
    // rebuilds them from the resolved values.
    message.mask_hints.clear();
    for endpoint in &mut message.resources.endpoints {
        endpoint.authorization.parameters.clear();
    }
    names
}

/// A node for one submitted job: spec columns from the `QueuedJob`, mutable
/// columns at their submit-time defaults (`Pending` + `Blocked`; the
/// promotion sweep moves it on).
fn submit_node(
    record: &RunRecord,
    job: QueuedJob,
    position: usize,
    now_us: i64,
) -> (Node, azdo::AgentJobRequestMessage, Vec<String>) {
    let job_id = job.job_id.clone();
    let kind = submit_kind(&job);
    let mut message = job.message.clone();
    let secret_names = strip_secret_values(&mut message);
    let condition_context = job.condition_context.clone();
    let reusable = record
        .caller_plans
        .get(&job_id)
        .map(|plan| ReusableNodeSpec {
            call: job.reusable_call.clone(),
            meta: record.reusable_calls.get(&job_id.0).cloned(),
            plan: Some(plan.clone()),
        });
    let node = Node {
        kind,
        status: ExecutionStatus::Pending,
        queue_state: logic::QueueState::Blocked,
        remaining_needs: job.needs.len() as i32,
        base_id: job.base_id.clone(),
        parent_job_id: None,
        pool_key: crate::control::types::compute_pool_key(
            &job.runs_on,
            job.runner_group.as_deref(),
        ),
        runs_on: job.runs_on.clone(),
        runner_group: job.runner_group.clone(),
        priority: 0,
        run_order: record.run_number as i64,
        job_order: position as i32,
        enqueued_at_us: None,
        claimed_by_runner_id: None,
        claimed_at_us: None,
        expand_generation: 0,
        outputs: None,
        annotations: None,
        check_run_id: record.job_check_run_ids.get(&job_id).map(|id| *id as i64),
        created_at_us: now_us,
        deps_ready_at_us: job.needs.is_empty().then_some(now_us),
        concurrency_wait_at_us: None,
        concurrency_acquired_at_us: None,
        started_at_us: None,
        completed_at_us: None,
        display_name: message
            .job_display_name
            .clone()
            .unwrap_or_else(|| message.job_name.clone()),
        display_order: position as i32,
        if_condition: job.if_condition.clone(),
        matrix: job.matrix.clone(),
        deferred_matrix: job.deferred_matrix.clone(),
        max_parallel: job.max_parallel,
        environment: job.environment.clone(),
        concurrency: job.concurrency.clone(),
        reusable,
        fail_fast: Some(
            record
                .job_fail_fast
                .get(&job.base_id)
                .copied()
                .unwrap_or(true),
        ),
        continue_on_error: record.job_continue_on_error.get(&job_id.0).copied(),
        id_token_granted: false,
        oidc: crate::state::OidcJobContext {
            environment: None,
            job_workflow_ref: None,
            job_workflow_sha: None,
        },
        needs: job.needs.clone(),
        condition_context,
        secret_names: secret_names.clone(),
        has_children: false,
    };
    (node, message, secret_names)
}

impl PgBackend {
    /// `submit_run`: one transaction that records the run, its jobs and their
    /// dispatch state, acquires the workflow-level gate, and runs the first
    /// promotion sweep.
    ///
    /// Statements: replay `SELECT run_id` (webhook delivery + path); namespace
    /// upsert; `INSERT INTO runs`; `run_submissions`; `run_push_states`;
    /// per job `jobs` + `job_specs` + `job_needs` + `job_messages` +
    /// `job_requests` (+ `github_token_requests`, `job_steps`); gate rows;
    /// the sweep's own statements.
    pub(super) async fn submit_run(
        &self,
        submit: SubmitRun,
    ) -> Result<SubmitOutcome, ControlError> {
        let SubmitRun {
            namespace,
            record,
            jobs,
            workflow_concurrency,
            empty_concurrency_group,
            check_hostable,
        } = submit;
        // A webhook delivery that already produced a run for this workflow
        // path replays that run instead of duplicating it.
        if let Some(delivery_id) = &record.webhook_delivery_id {
            let existing = self
                .existing_delivery_run(delivery_id, &record.workflow_path_str)
                .await?;
            if let Some(existing) = existing {
                return Ok(SubmitOutcome {
                    run_id: existing.run_id,
                    run_number: existing.run_number,
                    queued_jobs: 0,
                    status: existing.status,
                    concluded: Vec::new(),
                    held: existing.status == ExecutionStatus::Pending,
                    rejected: None,
                    queue_depth: self.queue_depth().await?,
                    next_runs_on: self.ready_front_labels().await?,
                    existing: Some(Box::new(existing)),
                });
            }
        }
        // Reject outright on an empty concurrency group (nothing persists).
        if empty_concurrency_group {
            return Ok(SubmitOutcome {
                run_id: record.run_id,
                run_number: record.run_number,
                queued_jobs: 0,
                status: ExecutionStatus::Failure,
                concluded: vec![(
                    JobId("*".to_owned()),
                    ExecutionStatus::Failure,
                    Some("empty concurrency group".to_owned()),
                )],
                held: false,
                rejected: Some(ExecutionStatus::Failure),
                existing: None,
                queue_depth: self.queue_depth().await?,
                next_runs_on: Vec::new(),
            });
        }

        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let now = now_us();
        let run = record.run_id.0.to_string();
        tx.execute(
            "INSERT INTO namespaces (namespace_id) VALUES ($1) ON CONFLICT DO NOTHING",
            &[&namespace],
        )
        .await
        .map_err(db)?;
        tx.execute(
            concat!(
                "INSERT INTO runs (run_id, namespace_id, repository, workflow_path, \
                 run_number, run_attempt, run_name, event, ref, ref_type, head_ref, \
                 base_ref, head_sha, workflow_ref, status, webhook_delivery_id, origin, \
                 actor, tree_digest, concurrency_group, concurrency_cancel_in_progress, \
                 created_at, started_at) VALUES ($1::text::uuid,$2,$3,$4,$5,$6,$7,$8,$9,\
                 $10,$11,$12,$13,$14,'queued',$15,$16,$17,$18,$19,$20,",
                us!("$21"),
                ",",
                us!("$22"),
                ")"
            ),
            &[
                &run,
                &namespace,
                &record.submission.repository,
                &record.workflow_path_str,
                &(record.run_number as i64),
                &(record.run_attempt as i32),
                &record.run_name,
                &record.event,
                &record.submission.git_ref,
                &ref_type(&record.submission.git_ref, &record.event),
                &Option::<String>::None,
                &Option::<String>::None,
                &record.head_sha,
                &record.workflow_ref,
                &record.webhook_delivery_id,
                &run_origin(&record),
                &record.submission.actor,
                &record.submission.push_tree,
                &workflow_concurrency.as_ref().map(|wf| wf.group.clone()),
                &workflow_concurrency
                    .as_ref()
                    .is_some_and(|wf| wf.cancel_in_progress),
                &now,
                &record.started_at.map(|at| codec::system_to_us(at.into())),
            ],
        )
        .await
        .map_err(db)?;
        // The submission record minus secrets (`secret_refs` names them).
        let mut stored_submission = (*record.submission).clone();
        let secret_refs: serde_json::Value = stored_submission
            .secrets
            .keys()
            .map(|name| (name.clone(), serde_json::json!({"scope": "run"})))
            .collect::<serde_json::Map<_, _>>()
            .into();
        stored_submission.secrets.clear();
        tx.execute(
            "INSERT INTO run_submissions (run_id, submission, github_context, \
             workspace_snapshot, snapshot_timing, secret_refs) \
             VALUES ($1::text::uuid,$2::text::jsonb,$3::text::jsonb,$4::text::jsonb,\
             $5::text::jsonb,$6::text::jsonb)",
            &[
                &run,
                &json(&stored_submission)?,
                &record.github.to_string(),
                &record
                    .workspace_snapshot
                    .as_ref()
                    .map(|s| serde_json::to_string(s).unwrap_or_default())
                    .unwrap_or_else(|| "null".to_owned()),
                &record
                    .snapshot_timing
                    .as_ref()
                    .map(|t| serde_json::to_string(t).unwrap_or_default()),
                &secret_refs.to_string(),
            ],
        )
        .await
        .map_err(db)?;
        if record.submission.push.is_some() {
            tx.execute(
                "INSERT INTO run_push_states (run_id, status) VALUES ($1::text::uuid,'pending')",
                &[&run],
            )
            .await
            .map_err(db)?;
        }

        // Workflow-level concurrency gate: the run either occupies the slot,
        // parks behind it, or dies on arrival.
        let mut held = false;
        if let Some(wf) = &workflow_concurrency {
            let key = concurrency::concurrency_key(&record.submission.repository, &wf.group);
            match acquire_gate(
                self,
                &tx,
                &namespace,
                &key,
                &wf.group,
                &GateHolder {
                    holder: concurrency::Holder::Run(record.run_id),
                    jobset_id: None,
                },
                wf.cancel_in_progress,
                wf.queue,
            )
            .await?
            {
                GateOutcome::Acquired => {}
                GateOutcome::Parked => held = true,
                GateOutcome::Cancelled | GateOutcome::Failed => {
                    tx.execute(
                        "UPDATE runs SET status='completed', conclusion='cancelled', \
                         completed_at = now() WHERE run_id = $1::text::uuid",
                        &[&run],
                    )
                    .await
                    .map_err(db)?;
                    tx.commit().await.map_err(db)?;
                    return Ok(SubmitOutcome {
                        run_id: record.run_id,
                        run_number: record.run_number,
                        queued_jobs: 0,
                        status: ExecutionStatus::Cancelled,
                        concluded: Vec::new(),
                        held: false,
                        rejected: Some(ExecutionStatus::Cancelled),
                        existing: None,
                        queue_depth: self.queue_depth().await?,
                        next_runs_on: self.ready_front_labels().await?,
                    });
                }
            }
        }

        let platforms = self.registered_platforms().await?;
        let accepted = record.jobs.len();
        let final_status;
        let mut concluded: Vec<(JobId, ExecutionStatus, Option<String>)> = Vec::new();
        let mut graph = RunGraph {
            record: record.clone(),
            namespace: namespace.clone(),
            nodes: BTreeMap::new(),
            submission: record.submission.clone(),
            github: record.github.clone(),
            touched: true,
        };
        for (position, submit_job) in jobs.into_iter().enumerate() {
            let SubmitJob {
                queued,
                request,
                token_request,
                id_token_granted,
                oidc_context,
                step_manifest,
                initially_skipped,
            } = submit_job;
            let job_id = queued.job_id.clone();
            let (mut node, mut message, _secret_names) =
                submit_node(&record, queued.clone(), position, now);
            node.id_token_granted = id_token_granted;
            if let Some(oidc) = oidc_context {
                node.oidc = oidc;
            }
            // Unhostable platform: conclude immediately.
            if check_hostable {
                if let Some(platform) =
                    crate::runtime_scheduling::unhostable_platform(&node.runs_on, platforms.clone())
                {
                    node.status = ExecutionStatus::Failure;
                    node.queue_state = logic::QueueState::None;
                    node.completed_at_us = Some(now);
                    concluded.push((
                        job_id.clone(),
                        ExecutionStatus::Failure,
                        Some(format!("no {platform} runner registered")),
                    ));
                }
            }
            if initially_skipped && node.status != ExecutionStatus::Failure {
                node.status = ExecutionStatus::Skipped;
                node.queue_state = logic::QueueState::None;
                node.completed_at_us = Some(now);
                concluded.push((job_id.clone(), ExecutionStatus::Skipped, None));
            }
            if held
                && !matches!(
                    node.status,
                    ExecutionStatus::Failure | ExecutionStatus::Skipped
                )
            {
                node.queue_state = logic::QueueState::Held;
                node.concurrency_wait_at_us = Some(now);
            }
            insert_job_row(&tx, &graph, &node, &job_id).await?;
            insert_spec_rows(&tx, &graph, &node, &job_id, None).await?;
            // Mint the request correlation inside the writer transaction so
            // the id is allocated under the cross-process writer lock.
            if let Some(request) = &request {
                let request_id = insert_request_row(
                    &tx,
                    &graph,
                    request,
                    token_request.as_ref(),
                    &step_manifest,
                )
                .await?;
                message.request_id = request_id;
                message.job_id = request.agent_job_id;
            }
            tx.execute(
                "INSERT INTO job_messages (run_id, job_id, message_template, secret_names, \
                 condition_context) VALUES ($1::text::uuid,$2,$3::text::jsonb,$4::text::jsonb,\
                 $5::text::jsonb)",
                &[
                    &run,
                    &job_id.0,
                    &json(&message)?,
                    &json(&node.secret_names)?,
                    &json(&node.condition_context)?,
                ],
            )
            .await
            .map_err(db)?;
            graph.record.jobs.insert(job_id.clone(), node.status);
            graph.nodes.insert(job_id, node);
        }

        // One promotion sweep: needs-satisfied nodes enqueue (or park behind
        // their own gate / expansion queue); unsatisfiable ones settle.
        if !held {
            let mut sweep = Sweep::new(self, &tx).await?;
            sweep.graphs.insert(record.run_id, graph);
            sweep.sweep().await?;
            for (job_id, status) in &sweep.concluded {
                concluded.push((job_id.clone(), *status, None));
            }
            let status = sweep
                .graphs
                .get(&record.run_id)
                .map(|graph| graph.record.status)
                .unwrap_or(record.status);
            sweep.flush().await?;
            final_status = status;
        } else {
            graph.resummarize();
            final_status = graph.record.status;
            flush_run(&tx, &graph).await?;
            for (job_id, node) in &graph.nodes {
                flush_node(&tx, record.run_id, job_id, node).await?;
            }
        }
        tx.commit().await.map_err(db)?;
        Ok(SubmitOutcome {
            run_id: record.run_id,
            run_number: record.run_number,
            queued_jobs: accepted,
            status: final_status,
            concluded,
            held,
            rejected: None,
            queue_depth: self.queue_depth().await?,
            next_runs_on: self.ready_front_labels().await?,
            existing: None,
        })
    }
}

impl PgBackend {
    /// Platform names (`linux` / `macos` / `windows`) some registered runner
    /// can host — the unhostable-platform check's input.
    pub(super) async fn registered_platforms(&self) -> Result<Vec<&'static str>, ControlError> {
        let client = self.reader().await?;
        let rows = client
            .query("SELECT labels::text FROM runners", &[])
            .await
            .map_err(db)?;
        let mut platforms = Vec::new();
        for row in rows {
            let labels: Vec<String> = codec::from_json(row.get::<_, String>(0).as_str())?;
            for label in labels {
                let label = label.to_ascii_lowercase();
                let os = ["linux", "macos", "windows"]
                    .into_iter()
                    .find(|os| label == *os || label.starts_with(os));
                if let Some(os) = os {
                    if !platforms.contains(&os) {
                        platforms.push(os);
                    }
                }
            }
        }
        Ok(platforms)
    }

    /// Ready-queue depth (the node-local gauge the runner supervisor reads).
    pub(super) async fn queue_depth(&self) -> Result<usize, ControlError> {
        let client = self.reader().await?;
        let count = client
            .query_one("SELECT count(*) FROM jobs WHERE queue_state = 'ready'", &[])
            .await
            .map_err(db)?
            .get::<_, i64>(0);
        Ok(count.max(0) as usize)
    }

    /// `runs-on` labels of the ready-queue front, for `next_job_runs_on`.
    pub(super) async fn ready_front_labels(&self) -> Result<Vec<String>, ControlError> {
        let client = self.reader().await?;
        let row = client
            .query_opt(
                "SELECT runs_on::text FROM jobs WHERE queue_state = 'ready' \
                 ORDER BY pool_key, priority DESC, run_order, job_order LIMIT 1",
                &[],
            )
            .await
            .map_err(db)?;
        match row {
            Some(row) => codec::from_json(row.get::<_, String>(0).as_str()),
            None => Ok(Vec::new()),
        }
    }

    /// A run already produced for this `(webhook delivery, workflow path)` —
    /// a push delivery legitimately fans out to several workflow files, so the
    /// match is on the pair, never the delivery alone.
    pub(super) async fn existing_delivery_run(
        &self,
        delivery_id: &str,
        workflow_path: &str,
    ) -> Result<Option<RunRecord>, ControlError> {
        let client = self.reader().await?;
        let row = client
            .query_opt(
                "SELECT run_id::text FROM runs WHERE webhook_delivery_id = $1 \
                 AND workflow_path = $2 ORDER BY created_at DESC LIMIT 1",
                &[&delivery_id, &workflow_path],
            )
            .await
            .map_err(db)?;
        match row {
            Some(row) => Ok(Some(
                self.run_record(codec::run_id(&row.get::<_, String>(0))?)
                    .await?,
            )),
            None => Ok(None),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Claim path
// ─────────────────────────────────────────────────────────────────────────

/// One session row's identity as the claim path needs it.
pub(super) struct SessionRef {
    pub(crate) session_uuid: String,
    pub(crate) runner_id: Option<i64>,
    pub(crate) protocol: &'static str,
    pub(crate) live: bool,
}

impl PgBackend {
    /// The session's row (mapped id), or `None` when it is unknown/expired.
    pub(super) async fn session_ref(
        &self,
        tx: &Transaction<'_>,
        session_id: &str,
    ) -> Result<Option<SessionRef>, ControlError> {
        let uuid = logic::session_uuid(session_id).to_string();
        Ok(tx
            .query_opt(
                "SELECT runner_id, protocol FROM runner_sessions WHERE session_id = $1::text::uuid",
                &[&uuid],
            )
            .await
            .map_err(db)?
            .map(|row| SessionRef {
                session_uuid: uuid,
                runner_id: row.get(0),
                protocol: match row.get::<_, String>(1).as_str() {
                    "broker" => "broker",
                    _ => "azdo",
                },
                live: true,
            }))
    }

    /// Touch a session's liveness stamp (a poll proves the runner is alive).
    pub(super) async fn touch_session_row(
        &self,
        tx: &Transaction<'_>,
        session_uuid: &str,
    ) -> Result<(), ControlError> {
        tx.execute(
            "UPDATE runner_sessions SET last_seen_at = now() WHERE session_id = $1::text::uuid",
            &[&session_uuid],
        )
        .await
        .map_err(db)?;
        Ok(())
    }

    /// The oldest unacknowledged message of a session (redelivery-first).
    pub(super) async fn oldest_session_message(
        &self,
        tx: &Transaction<'_>,
        session_uuid: &str,
    ) -> Result<Option<SessionMessage>, ControlError> {
        Ok(tx
            .query_opt(
                "SELECT message_id, message_type, request_id, body::text \
                 FROM session_messages WHERE session_id = $1::text::uuid \
                 ORDER BY message_id LIMIT 1",
                &[&session_uuid],
            )
            .await
            .map_err(db)?
            .map(|row| SessionMessage {
                message_id: row.get(0),
                message_type: row.get(1),
                request_id: row.get(2),
                body: row.get(3),
            }))
    }

    /// The session's live request, when it holds one.
    pub(super) async fn session_active_request(
        &self,
        tx: &Transaction<'_>,
        session_uuid: &str,
    ) -> Result<Option<TaskAgentJobRequestRecord>, ControlError> {
        let select = format!(
            "{} WHERE q.session_id = $1::text::uuid AND q.result IS NULL \
             ORDER BY q.request_id DESC LIMIT 1",
            lookups::REQUEST_SELECT
        );
        match tx.query_opt(&select, &[&session_uuid]).await.map_err(db)? {
            Some(row) => Ok(Some(lookups::request_from_row(&row)?)),
            None => Ok(None),
        }
    }

    /// Queue a `JobCancellation` for a session's active attempt: the
    /// cancellation row is marked delivered and a session message carrying the
    /// official body is appended.
    pub(super) async fn queue_cancellation_message(
        &self,
        tx: &Transaction<'_>,
        session_uuid: &str,
        request_id: i64,
        agent_job_id: uuid::Uuid,
    ) -> Result<SessionMessage, ControlError> {
        tx.execute(
            "UPDATE job_cancellations SET delivered_at = now() \
             WHERE request_id = $1 AND delivered_at IS NULL",
            &[&request_id],
        )
        .await
        .map_err(db)?;
        let body = concurrency::job_cancel_body(agent_job_id);
        let message_id: i64 = tx
            .query_one(
                "INSERT INTO session_messages (session_id, message_type, request_id, body) \
                 VALUES ($1::text::uuid,$2,$3,$4::text::jsonb) RETURNING message_id",
                &[
                    &session_uuid,
                    &azdo::message_type::JOB_CANCELLED,
                    &request_id,
                    &body,
                ],
            )
            .await
            .map_err(db)?
            .get(0);
        Ok(SessionMessage {
            message_id,
            message_type: azdo::message_type::JOB_CANCELLED.to_owned(),
            request_id: Some(request_id),
            body: Some(body),
        })
    }

    /// A pending (undelivered) cancellation for the attempt, if any.
    async fn pending_cancellation(
        &self,
        tx: &Transaction<'_>,
        request_id: i64,
    ) -> Result<Option<u64>, ControlError> {
        Ok(tx
            .query_opt(
                "SELECT request_id FROM job_cancellations \
                 WHERE request_id = $1 AND delivered_at IS NULL LIMIT 1",
                &[&request_id],
            )
            .await
            .map_err(db)?
            .map(|row| row.get::<_, i64>(0).max(0) as u64))
    }

    /// `claim_position`: the ready job this runner should take, using the
    /// shared four-tier preference over a `SKIP LOCKED` candidate batch.
    ///
    /// Statements: `SELECT .. FROM jobs WHERE queue_state='ready' AND
    /// pool_key = ANY(..) ORDER BY priority DESC, run_order, job_order LIMIT
    /// 64 FOR UPDATE SKIP LOCKED` (pool filter), the assignment read, then the
    /// candidate's conditional claim.
    async fn claim_one(
        &self,
        tx: &Transaction<'_>,
        runner_id: Option<i64>,
        caps: &RunnerCapabilities,
    ) -> Result<Option<(RunId, JobId)>, ControlError> {
        // The pool key prunes by label set; the shared matcher still decides.
        let rows = tx
            .query(
                "SELECT j.run_id::text, j.job_id, j.runs_on::text, j.runner_group, j.base_id \
                 FROM jobs j WHERE j.queue_state = 'ready' \
                 ORDER BY j.pool_key, j.priority DESC, j.run_order, j.job_order \
                 LIMIT 64 FOR UPDATE OF j SKIP LOCKED",
                &[],
            )
            .await
            .map_err(db)?;
        let mut candidates = Vec::with_capacity(rows.len());
        for (position, row) in rows.iter().enumerate() {
            let runs_on: Vec<String> = codec::from_json(row.get::<_, String>(2).as_str())?;
            let runner_group: Option<String> = row.get(3);
            let run_id = codec::run_id(&row.get::<_, String>(0))?;
            let job_id = JobId(row.get::<_, String>(1));
            let assignment = tx
                .query_opt(
                    "SELECT runner_id, (assigned_at > now() - interval '120 seconds') \
                     FROM job_assignments WHERE run_id = $1::text::uuid AND job_id = $2",
                    &[&row.get::<_, String>(0), &job_id.0],
                )
                .await
                .map_err(db)?
                .map(|row| (row.get::<_, Option<i64>>(0), row.get::<_, bool>(1)));
            let (assigned_runner_id, assignment_fresh) = assignment.unwrap_or((None, false));
            candidates.push(logic::ClaimCandidate {
                run_id,
                job_id,
                runs_on,
                runner_group,
                assigned_runner_id,
                assignment_fresh,
                queue_position: position as u64,
                claimable: true,
            });
        }
        let runner_match = logic::RunnerMatchRow {
            labels: caps.labels.clone(),
            known: caps.known,
            group_id: caps.runner_group_id,
            group_name: caps.runner_group_name.clone(),
        };
        let Some(index) =
            logic::claim_preference(&candidates, runner_id, &caps.labels, None, &runner_match)
        else {
            return Ok(None);
        };
        let chosen = candidates.swap_remove(index);
        let claimed = tx
            .execute(
                "UPDATE jobs SET queue_state = 'claimed', status = 'in_progress', \
                 claimed_by_runner_id = $3, claimed_at = now() \
                 WHERE run_id = $1::text::uuid AND job_id = $2 AND queue_state = 'ready'",
                &[&chosen.run_id.0.to_string(), &chosen.job_id.0, &runner_id],
            )
            .await
            .map_err(db)?;
        if claimed == 0 {
            return Ok(None);
        }
        Ok(Some((chosen.run_id, chosen.job_id)))
    }

    /// Bind the claimed attempt: request owner/session/start stamps, the lease
    /// row, the run's `in_progress` transition and the job's assignment drop.
    async fn bind_claim(
        &self,
        tx: &Transaction<'_>,
        run_id: RunId,
        job_id: &JobId,
        session_uuid: &str,
        runner_id: Option<i64>,
    ) -> Result<Option<TaskAgentJobRequestRecord>, ControlError> {
        let run = run_id.0.to_string();
        let request = tx
            .query_opt(
                &format!(
                    "{} WHERE q.run_id = $1::text::uuid AND q.job_id = $2 AND q.result IS NULL \
                     ORDER BY q.request_id DESC LIMIT 1 FOR NO KEY UPDATE OF q",
                    lookups::REQUEST_SELECT
                ),
                &[&run, &job_id.0],
            )
            .await
            .map_err(db)?;
        let Some(row) = request else {
            return Ok(None);
        };
        let record = lookups::request_from_row(&row)?;
        tx.execute(
            "UPDATE job_requests SET session_id = $2::text::uuid, runner_id = $3, \
             claimed_at = now(), started_at = now() WHERE request_id = $1",
            &[&record.request_id, &session_uuid, &runner_id],
        )
        .await
        .map_err(db)?;
        // The lease is the heartbeat target; only claimed attempts carry one.
        if let Some(runner_id) = runner_id {
            let expires =
                codec::locked_until_us(&crate::distributed_task::agent_request_locked_until())?
                    .ok_or_else(|| {
                        ControlError::backend(anyhow::anyhow!("empty lease deadline"))
                    })?;
            tx.execute(
                "INSERT INTO job_leases (request_id, runner_id, expires_at, renewed_at) \
                 VALUES ($1,$2,$3::int8::text::timestamptz,now()) \
                 ON CONFLICT (request_id) DO UPDATE SET runner_id = EXCLUDED.runner_id, \
                 expires_at = EXCLUDED.expires_at, renewed_at = EXCLUDED.renewed_at",
                &[&record.request_id, &runner_id, &expires],
            )
            .await
            .map_err(db)?;
        }
        tx.execute(
            "DELETE FROM job_assignments WHERE run_id = $1::text::uuid AND job_id = $2",
            &[&run, &job_id.0],
        )
        .await
        .map_err(db)?;
        tx.execute(
            "DELETE FROM provision_requests WHERE run_id = $1::text::uuid AND job_id = $2",
            &[&run, &job_id.0],
        )
        .await
        .map_err(db)?;
        tx.execute(
            "UPDATE runs SET status = 'in_progress', started_at = COALESCE(started_at, now()) \
             WHERE run_id = $1::text::uuid AND status = 'queued'",
            &[&run],
        )
        .await
        .map_err(db)?;
        tx.execute(
            "UPDATE runner_sessions SET last_seen_at = now() \
             WHERE session_id = $1::text::uuid",
            &[&session_uuid],
        )
        .await
        .map_err(db)?;
        Ok(Some(record))
    }

    /// Append the job-request session message a poll answers with.
    async fn queue_job_message(
        &self,
        tx: &Transaction<'_>,
        session_uuid: &str,
        request_id: i64,
    ) -> Result<SessionMessage, ControlError> {
        let message_id: i64 = tx
            .query_one(
                "INSERT INTO session_messages (session_id, message_type, request_id) \
                 VALUES ($1::text::uuid,$2,$3) RETURNING message_id",
                &[
                    &session_uuid,
                    &azdo::message_type::PIPELINE_AGENT_JOB_REQUEST,
                    &request_id,
                ],
            )
            .await
            .map_err(db)?
            .get(0);
        Ok(SessionMessage {
            message_id,
            message_type: azdo::message_type::PIPELINE_AGENT_JOB_REQUEST.to_owned(),
            request_id: Some(request_id),
            body: None,
        })
    }

    /// `poll_session`: redelivery, cancellation, active request, then a claim.
    pub(super) async fn poll_session(
        &self,
        poll: PollRequest,
    ) -> Result<PollOutcome, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        // Ownership is revalidated inside the claim transaction: the handler
        // caches the runner across a long poll, and a liveness sweep can purge
        // the session while it waits.
        let Some(session) = self.session_ref(&tx, &poll.session_id).await? else {
            return Err(ControlError::Forbidden(
                "session has no runner owner".to_owned(),
            ));
        };
        if poll.verified_runner_id.is_some() && poll.verified_runner_id != session.runner_id {
            return Err(ControlError::Forbidden(
                "session belongs to another runner".to_owned(),
            ));
        }
        self.touch_session_row(&tx, &session.session_uuid).await?;
        if let Some(message) = self
            .oldest_session_message(&tx, &session.session_uuid)
            .await?
        {
            tx.commit().await.map_err(db)?;
            return Ok(PollOutcome::Inflight(azdo::TaskAgentMessage {
                message_id: message.message_id,
                message_type: message.message_type,
                body: message
                    .request_id
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
                iv: None,
            }));
        }
        if let Some(request) = self
            .session_active_request(&tx, &session.session_uuid)
            .await?
        {
            if let Some(_pending) = self.pending_cancellation(&tx, request.request_id).await? {
                let message = self
                    .queue_cancellation_message(
                        &tx,
                        &session.session_uuid,
                        request.request_id,
                        request.agent_job_id,
                    )
                    .await?;
                tx.commit().await.map_err(db)?;
                return Ok(PollOutcome::Cancel(azdo::TaskAgentMessage {
                    message_id: message.message_id,
                    message_type: message.message_type,
                    body: message.body.unwrap_or_default(),
                    iv: None,
                }));
            }
            let runner_id = session.runner_id.unwrap_or(0);
            tx.commit().await.map_err(db)?;
            return Ok(PollOutcome::ActiveRequest { request, runner_id });
        }
        if poll.busy {
            tx.commit().await.map_err(db)?;
            return Ok(PollOutcome::Empty);
        }
        let Some((run_id, job_id)) = self.claim_one(&tx, session.runner_id, &poll.runner).await?
        else {
            tx.commit().await.map_err(db)?;
            return Ok(PollOutcome::Empty);
        };
        let Some(request) = self
            .bind_claim(
                &tx,
                run_id,
                &job_id,
                &session.session_uuid,
                session.runner_id,
            )
            .await?
        else {
            tx.commit().await.map_err(db)?;
            return Ok(PollOutcome::Empty);
        };
        let message = self
            .queue_job_message(&tx, &session.session_uuid, request.request_id)
            .await?;
        let mut graph = match PgBackend::load_graph(self, &tx, run_id).await? {
            Some(graph) => graph,
            None => {
                tx.commit().await.map_err(db)?;
                return Ok(PollOutcome::Empty);
            }
        };
        let queued = match self.queued_job(&tx, &graph, &job_id).await? {
            Some(queued) => queued,
            None => {
                tx.commit().await.map_err(db)?;
                return Ok(PollOutcome::Empty);
            }
        };
        let node = graph
            .nodes
            .get_mut(&job_id)
            .expect("graph holds the claimed job");
        node.claimed_by_runner_id = session.runner_id;
        node.claimed_at_us = Some(now_us());
        node.started_at_us = Some(now_us());
        graph
            .record
            .jobs
            .insert(job_id.clone(), ExecutionStatus::InProgress);
        graph.touched = true;
        flush_node(&tx, run_id, &job_id, node).await?;
        flush_run(&tx, &graph).await?;
        let runner_id = session.runner_id.unwrap_or(0);
        tx.commit().await.map_err(db)?;
        let _ = message;
        Ok(PollOutcome::Claimed(Box::new(ClaimedJob {
            queued,
            request,
            runner_id,
            queue_depth: self.queue_depth().await?,
            next_runs_on: self.ready_front_labels().await?,
        })))
    }
}
