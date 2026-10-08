//! Scheduling commands: run submission, completion settlement, cancellation,
//! expansion and the promotion sweep, plus the concurrency-gate machinery
//! those commands share (gate ops live at the bottom of this file).
//!
//! Every command is one transaction: lock the touched `runs` rows (ascending
//! `run_id` — the schema's per-run mutex), load each run's graph, decide with
//! `control/logic.rs` + `runtime_scheduling.rs`, write the deltas back.
//! Gate acquisition pre-locks every foreign holder/waiter run in ascending
//! order, then locks the group row. This keeps cross-run cancellation and
//! promotion in the same run-before-group order as ordinary commands.

use super::codec::{self, from_json, json, now_us, ts};
use super::graph::{self, Node, NodeKind, ReusableNodeSpec, RunGraph, queue_state_str};
use super::{PgBackend, db, lookups};
use crate::concurrency;
use crate::control::backend::{
    EnvironmentGateRead, ExpansionApply, JobCompletionInput, PollRequest, PromoteOutcome,
};
use crate::control::logic;
use crate::control::logic::{
    BuiltExpansion, BuiltJob, ExpansionContext, ExpansionPlan, MatrixExpansionInputs,
    ReusableExpansionInputs,
};
use crate::control::txn_stats::RunRowOp;
use crate::control::types::EnvironmentDeploymentRow;
use crate::control::types::{
    AzdoPoll, AzdoPollOutcome, CancelOutcome, ClaimedJob, CompleteOutcome, ControlError,
    ExpansionClaim, JobSettled, PollOutcome, SessionMessage, SettleJob, SettleJobOutcome,
    SubmitJob, SubmitOutcome, SubmitRun, status_str,
};
use crate::models::{
    EnvironmentGateState, QueuedJob, RunRecord, RunnerCapabilities, TaskAgentJobRequestRecord,
};
use crate::runtime_scheduling::{self as sched_helpers, DependencyDecision, SchedulingOutcome};
use crate::state::JobSetGate;
use preloop_gha_parser::ConcurrencyQueue;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId, azdo};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use tokio_postgres::{GenericClient, Transaction};

/// Claim-race retries inside one `claim_one` call: losing the conditional
/// `UPDATE` re-reads the ready set before giving up.
const CLAIM_ATTEMPTS: usize = 4;

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
    let environment_gate_json = node
        .environment_gate
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(ControlError::backend)?;
    tx.execute(
        concat!(
            "UPDATE jobs SET status=$3, queue_state=$4, remaining_needs=$5, \
             claimed_by_runner_id=$6, claimed_at=",
            ts!("$7"),
            ", enqueued_at=",
            ts!("$8"),
            ", deps_ready_at=",
            ts!("$9"),
            ", concurrency_wait_at=",
            ts!("$10"),
            ", concurrency_acquired_at=",
            ts!("$11"),
            ", started_at=",
            ts!("$12"),
            ", completed_at=",
            ts!("$13"),
            ", outputs=$14::text::jsonb, annotations=$15::text::jsonb, \
             check_run_id=$16, expand_generation=$17, \
             environment_gate=$18::text::jsonb, \
             pool_key=$19, runs_on=$20::text::jsonb \
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
            &environment_gate_json,
            &node.pool_key,
            &serde_json::to_string(&node.runs_on).unwrap_or_else(|_| "[]".into()),
        ],
    )
    .await
    .map_err(db)?;
    Ok(())
}

fn apply_resolved_runs_on(node: &mut Node, runs_on: Vec<String>) {
    if node.runs_on == runs_on {
        return;
    }
    node.runs_on = runs_on;
    node.pool_key =
        crate::control::types::compute_pool_key(&node.runs_on, node.runner_group.as_deref());
}

async fn promotion_label_reason(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    runs_on: &[String],
) -> Result<Option<String>, ControlError> {
    let platforms = PgBackend::registered_platforms_on(tx).await?;
    let pool_labels = backend.pool_labels();
    let any_runner_matches = if pool_labels.is_empty() {
        false
    } else {
        tx.query("SELECT labels::text FROM runners", &[])
            .await
            .map_err(db)?
            .iter()
            .any(|row| {
                let text: String = row.get(0);
                let labels = codec::from_json::<Vec<String>>(&text).unwrap_or_default();
                sched_helpers::job_matches_runner(runs_on, &labels)
            })
    };
    Ok(logic::promotion_unsatisfiable_reason(
        runs_on,
        platforms,
        &pool_labels,
        any_runner_matches,
    ))
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
    let started = std::time::Instant::now();
    tx.execute(
        concat!(
            "UPDATE runs SET status=$3, conclusion=$4, started_at=",
            ts!("$5"),
            ", completed_at=",
            ts!("$6"),
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
    crate::control::txn_stats::record(RunRowOp::FlushRun, started.elapsed());
    Ok(())
}

/// `resummarize` + `finalize_run_if_complete` for commands that mutate job
/// rows directly instead of through a `RunGraph`: recompute `runs.status` /
/// `conclusion` / `completed_at` from the contributing job rows. An
/// expanded matrix parent does not contribute (its legs speak for it); a
/// run whose nodes sit `held` reports `queued` here (`Pending` is
/// reconstructed on read from its `holder_kind='run'` wait).
///
/// Statements: one aggregate `SELECT` over `jobs`, then one conditional
/// `UPDATE runs` (a `completed` run is never resurrected).
pub(super) async fn summarize_run_tx(
    tx: &Transaction<'_>,
    run_id: RunId,
) -> Result<(), ControlError> {
    let run = run_id.0.to_string();
    let summary = tx
        .query_one(
            "SELECT bool_or(j.status IN ('pending','queued','in_progress')), \
                    bool_or(j.status = 'in_progress'), \
                    bool_or(j.status IN ('failure','timed_out')), \
                    bool_or(j.status = 'cancelled'), \
                    count(*), \
                    bool_or(j.queue_state = 'held') \
             FROM jobs j WHERE j.run_id=$1::text::uuid \
             AND NOT (j.kind = 'matrix_parent' AND EXISTS (\
                  SELECT 1 FROM jobs c WHERE c.run_id = j.run_id \
                  AND c.parent_job_id = j.job_id))",
            &[&run],
        )
        .await
        .map_err(db)?;
    let any_live: bool = summary.get::<_, Option<bool>>(0).unwrap_or(false);
    let any_running: bool = summary.get::<_, Option<bool>>(1).unwrap_or(false);
    let any_failure: bool = summary.get::<_, Option<bool>>(2).unwrap_or(false);
    let any_cancelled: bool = summary.get::<_, Option<bool>>(3).unwrap_or(false);
    let contributing: i64 = summary.get(4);
    let held: bool = summary.get::<_, Option<bool>>(5).unwrap_or(false);
    if any_live {
        // summarize_run: any non-terminal job means the run is in motion; a
        // held run (or one with no live attempt) reports queued.
        let wire = if held || !any_running {
            "queued"
        } else {
            "in_progress"
        };
        tx.execute(
            "UPDATE runs SET status=$2 WHERE run_id=$1::text::uuid AND status <> 'completed'",
            &[&run, &wire],
        )
        .await
        .map_err(db)?;
    } else {
        // Terminal summary (empty job set is `success`, matching
        // `summarize_run` over zero statuses).
        let conclusion = if contributing > 0 && any_failure {
            "failure"
        } else if contributing > 0 && any_cancelled {
            "cancelled"
        } else {
            "success"
        };
        tx.execute(
            "UPDATE runs SET status='completed', conclusion=$2, \
             completed_at=COALESCE(completed_at, now()) \
             WHERE run_id=$1::text::uuid AND status <> 'completed'",
            &[&run, &conclusion],
        )
        .await
        .map_err(db)?;
    }
    Ok(())
}

/// Insert the `jobs` row for a node (submit-time and expansion-time share it).
async fn insert_job_row(
    tx: &Transaction<'_>,
    graph: &RunGraph,
    node: &Node,
    job_id: &JobId,
) -> Result<(), ControlError> {
    let environment_gate_json = node
        .environment_gate
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(ControlError::backend)?;
    tx.execute(
        concat!(
            "INSERT INTO jobs (run_id, job_id, namespace_id, kind, parent_job_id, \
             base_id, status, queue_state, remaining_needs, pool_key, runs_on, \
             runner_group, priority, run_order, job_order, enqueued_at, \
             deps_ready_at, concurrency_wait_at, concurrency_acquired_at, \
             expand_generation, outputs, annotations, check_run_id, created_at, \
             started_at, completed_at, environment_gate) \
             VALUES ($1::text::uuid,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::text::jsonb,\
             $12,$13,$14,$15,",
            ts!("$16"),
            ",",
            ts!("$17"),
            ",",
            ts!("$18"),
            ",",
            ts!("$19"),
            ",$20,$21::text::jsonb,$22::text::jsonb,$23,",
            ts!("$24"),
            ",",
            ts!("$25"),
            ",",
            ts!("$26"),
            ",$27::text::jsonb)"
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
            &environment_gate_json,
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
        .map(serde_json::to_string)
        .transpose()
        .map_err(ControlError::backend)?;
    let reusable_json = node
        .reusable
        .as_ref()
        .map(serde_json::to_string)
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

/// Mint a `job_requests` row (identity `request_id`) and the step manifest.
/// No `job_leases` row: the schema's lease exists only while an attempt is
/// claimed (`bind_claim` inserts it); the row is NOT NULL on runner/expiry.
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
    insert_step_manifest(tx, request.agent_job_id, manifest).await?;
    Ok(request_id)
}

/// Insert an attempt's step manifest, keyed by its runtime identity.
async fn insert_step_manifest(
    tx: &Transaction<'_>,
    agent_job_id: uuid::Uuid,
    manifest: &[crate::models::StepRecord],
) -> Result<(), ControlError> {
    let agent_job_id = agent_job_id.to_string();
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
                ts!("$10"),
                ",",
                ts!("$11"),
                ") ON CONFLICT (agent_job_id, step_id) DO NOTHING"
            ),
            &[
                &agent_job_id,
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
    Ok(())
}

/// Settle one request whose job is terminal: stamp the verdict, move the
/// lease expiry to `now + lease`, unbind the session and retire undelivered
/// cancellation messages for that session (the runner already stopped the
/// work — a queued `job.cancellation` must not redeliver). Settled requests
/// keep their row (acquire reads remain routable).
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
    // The settled attempt keeps a lease row carrying its final expiry, so the
    // record still reports `lockedUntil` (lite's `set_lease`). An attempt with
    // no recorded owner has none to stamp: `job_leases.runner_id` is NOT NULL.
    let lease = codec::locked_until_us(&crate::distributed_task::agent_request_locked_until())?;
    if let Some(expires) = lease {
        tx.execute(
            concat!(
                "INSERT INTO job_leases (request_id, runner_id, expires_at, renewed_at) \
                 SELECT q.request_id, q.runner_id, ",
                ts!("$2"),
                ", ",
                ts!("$3"),
                " FROM job_requests q \
                 WHERE q.request_id = $1 AND q.runner_id IS NOT NULL \
                 ON CONFLICT (request_id) DO UPDATE SET expires_at = EXCLUDED.expires_at, \
                   renewed_at = EXCLUDED.renewed_at"
            ),
            &[&request_id, &expires, &now_us()],
        )
        .await
        .map_err(db)?;
    }
    // The job is terminal, so its deferred App-token request must not
    // outlive it (lite drops the same row in `settle_request_tx`).
    tx.execute(
        "DELETE FROM github_token_requests WHERE request_id=$1",
        &[&request_id],
    )
    .await
    .map_err(db)?;
    if let Some(session_id) = session {
        // Both the cancelled-but-undelivered marker shape and a delivered
        // JobCancellation must not redeliver for settled work.
        tx.execute(
            "DELETE FROM session_messages WHERE session_id=$1::text::uuid \
             AND message_type IN ('job.cancellation','JobCancellation')",
            &[&session_id],
        )
        .await
        .map_err(db)?;
    }
    // The pending marker itself is moot once the request settled.
    tx.execute(
        "UPDATE job_cancellations SET delivered_at=now() \
         WHERE request_id=$1 AND delivered_at IS NULL",
        &[&request_id],
    )
    .await
    .map_err(db)?;
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
    // Settling is first-result-wins, so an already-settled request is a pure
    // no-op: don't even select it. (`complete_job` settles the attempt before
    // the sweep retires the node; without this the settle statements run a
    // second time and match zero rows.) Purge keeps the unfiltered select —
    // it replaces the node's rows wholesale.
    let pending_only = matches!(retirement, Retirement::Settle(_));
    let select = format!(
        "{} WHERE q.run_id = $1::text::uuid AND q.job_id = $2{} FOR UPDATE OF q",
        lookups::REQUEST_SELECT,
        if pending_only {
            " AND q.result IS NULL"
        } else {
            ""
        },
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
    tx.query_opt(
        "SELECT agent_job_id::text FROM job_requests \
         WHERE run_id=$1::text::uuid AND job_id=$2 \
         ORDER BY (result IS NULL) DESC, request_id DESC LIMIT 1",
        &[&run_id.0.to_string(), &job_id.0],
    )
    .await
    .map_err(db)?
    .map(|row| codec::uuid(row.get(0)))
    .transpose()
}

/// Mark a cancellation pending for the runner holding the attempt: one
/// `job_cancellations` row (deduplicated while undelivered). The poll path
/// turns it into the `JobCancellation` session message at delivery — an
/// eager message would redeliver `request_id` as a bogus body and refire
/// `pending_cancellation`. No-ops when the attempt is already settled
/// (nothing to cancel). `true` when a cancellation is (or already was)
/// queued for a live request.
pub(super) async fn enqueue_cancellation(
    tx: &Transaction<'_>,
    graph: &RunGraph,
    job_id: &JobId,
    reason: Option<&str>,
) -> Result<bool, ControlError> {
    let Some(row) = tx
        .query_opt(
            "SELECT q.request_id FROM job_requests q \
             WHERE q.run_id=$1::text::uuid AND q.job_id=$2 \
             AND q.result IS NULL ORDER BY q.request_id DESC LIMIT 1",
            &[&graph.record.run_id.0.to_string(), &job_id.0],
        )
        .await
        .map_err(db)?
    else {
        return Ok(false);
    };
    let request_id: i64 = row.get(0);
    tx.execute(
        "INSERT INTO job_cancellations (request_id, reason) \
         SELECT $1, $2 WHERE NOT EXISTS (\
         SELECT 1 FROM job_cancellations WHERE request_id = $1 \
         AND delivered_at IS NULL)",
        &[&request_id, &reason],
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
/// Lock one concurrency group: probe-insert a `holds` row keyed on the
/// caller's run (the group's mutex), then read holder + FIFO waiters.
/// `holder_run_id` is `NOT NULL REFERENCES runs` — there is no empty-holder
/// row — so the probe carries the claimant's real run id and is rewritten by
/// `set_holder` when it wins. Returns `None` only when this transaction
/// landed the probe (the group was empty).
async fn lock_group(
    tx: &Transaction<'_>,
    namespace: &str,
    key: &(String, String),
    probe_run: &str,
) -> Result<Option<GroupState>, ControlError> {
    let (repository, group_name) = key;
    // The probe INSERT is also the mutex: whoever lands the row owns the
    // slot; losers see the existing row, which the FOR UPDATE below locks.
    tx.execute(
        "INSERT INTO concurrency_holds (namespace_id, repository, group_name, \
         display_name, holder_kind, holder_run_id) \
         VALUES ($1,$2,$3,'','run',$4::text::uuid) ON CONFLICT DO NOTHING",
        &[&namespace, &repository, &group_name, &probe_run],
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
    let holder = if holder_row.get::<_, String>(4).is_empty() {
        // The probe row itself (display_name stays '' until set_holder
        // writes the real identity): the group is empty and this
        // transaction holds the mutex.
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

/// Drop the group's mutex row when the caller probed an empty group but
/// declines the slot (parked, cancelled, failed): leaving the probe would
/// let the next acquirer see an empty group while waiters sit queued.
async fn unlock_group(
    tx: &Transaction<'_>,
    namespace: &str,
    key: &(String, String),
) -> Result<(), ControlError> {
    let (repository, group_name) = key;
    tx.execute(
        "DELETE FROM concurrency_holds WHERE namespace_id=$1 AND repository=$2 \
         AND group_name=$3 AND display_name=''",
        &[&namespace, &repository, &group_name],
    )
    .await
    .map_err(db)?;
    Ok(())
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

/// Re-park a waiter at its original FIFO position (same `wait_id`): a
/// promotion attempt that found max-parallel saturated must not let a
/// younger waiter jump the queue.
async fn requeue_waiter(
    tx: &Transaction<'_>,
    namespace: &str,
    key: &(String, String),
    wait_id: i64,
    holder: &GateHolder,
) -> Result<(), ControlError> {
    let (kind, run_id, job_id, jobset_id) = gate_row(holder);
    tx.execute(
        "INSERT INTO concurrency_waits (wait_id, namespace_id, repository, \
         group_name, holder_kind, holder_run_id, holder_job_id, holder_jobset_id) \
         OVERRIDING SYSTEM VALUE VALUES ($1,$2,$3,$4,$5,$6::text::uuid,$7,$8)",
        &[
            &wait_id, &namespace, &key.0, &key.1, &kind, &run_id, &job_id, &jobset_id,
        ],
    )
    .await
    .map_err(db)?;
    Ok(())
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
/// `logic::try_acquire_concurrency` including the stale-arrival check and
/// queue-mode displacement. Cancelling a foreign holder/parked waiters is
/// the caller-visible side effect.
/// Lock every existing foreign run named by a group before taking the group
/// row. Cancellation and promotion then cannot invert the command-wide
/// run-before-group lock order.
async fn lock_gate_runs(
    tx: &Transaction<'_>,
    namespace: &str,
    key: &(String, String),
    arriving_run: RunId,
) -> Result<(), ControlError> {
    let rows = tx
        .query(
            "SELECT holder_run_id::text FROM concurrency_holds \
             WHERE namespace_id=$1 AND repository=$2 AND group_name=$3 \
             UNION \
             SELECT holder_run_id::text FROM concurrency_waits \
             WHERE namespace_id=$1 AND repository=$2 AND group_name=$3",
            &[&namespace, &key.0, &key.1],
        )
        .await
        .map_err(db)?;
    let mut runs = std::collections::BTreeSet::new();
    for row in rows {
        if let Ok(run) = row.get::<_, String>(0).parse::<RunId>()
            && run != arriving_run
        {
            runs.insert(run);
        }
    }
    for run in runs {
        PgBackend::lock_run(tx, run).await?;
    }
    Ok(())
}

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
    lock_gate_runs(tx, namespace, key, holder.holder.run_id()).await?;
    let probe_run = holder.holder.run_id().0.to_string();
    let Some(group) = lock_group(tx, namespace, key, &probe_run).await? else {
        // Unreachable in one transaction (the probe self-lands and `FOR
        // UPDATE` sees it), but a vanished row means the group is ours:
        // occupy it.
        set_holder(tx, namespace, key, display_name, holder).await?;
        return Ok(GateOutcome::Acquired);
    };
    let running_stuck = match &group.holder {
        Some(existing) => run_stuck_on_external_hosts(tx, existing.holder.run_id()).await?,
        None => false,
    };
    // Late deliveries must not pre-empt a newer holder.
    if (cancel_in_progress || queue == ConcurrencyQueue::Single)
        && let Some(arrival) = run_event_order(tx, holder.holder.run_id()).await?
    {
        let mut superseded = false;
        for existing in group
            .holder
            .iter()
            .chain(group.waiters.iter().map(|(_, h)| h))
        {
            if existing.holder.run_id() == holder.holder.run_id() {
                continue;
            }
            if let Some(existing_order) = run_event_order(tx, existing.holder.run_id()).await?
                && arrival.is_older_than(&existing_order)
            {
                superseded = true;
                break;
            }
        }
        if superseded {
            // Declining a group we only probed: drop the empty probe row
            // so the next acquirer doesn't see a phantom empty mutex.
            if group.holder.is_none() {
                unlock_group(tx, namespace, key).await?;
            }
            return Ok(GateOutcome::Cancelled);
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
        if let Some(prev) = prev
            && prev.holder.run_id() != holder.holder.run_id()
        {
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
    // Lock the runs this release will promote before taking any group-row
    // lock: every command locks `runs` first and group rows second, so a
    // promotion that wrote a foreign run's rows under a group lock would
    // invert that order and deadlock against a command on that run.
    let mut promoted_runs = std::collections::BTreeSet::new();
    for row in &rows {
        let namespace: String = row.get(0);
        let key = (row.get::<_, String>(1), row.get::<_, String>(2));
        let waiter = tx
            .query_opt(
                "SELECT holder_run_id::text FROM concurrency_waits \
                 WHERE namespace_id=$1 AND repository=$2 AND group_name=$3 \
                 ORDER BY wait_id LIMIT 1",
                &[&namespace, &key.0, &key.1],
            )
            .await
            .map_err(db)?;
        if let Some(waiter) = waiter
            && let Ok(promoted) = waiter.get::<_, String>(0).parse::<RunId>()
            && promoted != run_id
        {
            promoted_runs.insert(promoted);
        }
    }
    for promoted in promoted_runs {
        PgBackend::lock_run(tx, promoted).await?;
    }
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
    // Take the promoted run's row lock before touching any group row: every
    // command locks `runs` first and group rows second, so promoting (and
    // then writing) a foreign run while holding a group-row lock would
    // invert that order and deadlock against a command on that run.
    let next_run = tx
        .query_opt(
            "SELECT holder_run_id::text FROM concurrency_waits \
             WHERE namespace_id=$1 AND repository=$2 AND group_name=$3 \
             ORDER BY wait_id LIMIT 1",
            &[&namespace, &key.0, &key.1],
        )
        .await
        .map_err(db)?;
    if let Some(row) = next_run
        && let Ok(run_id) = row.get::<_, String>(0).parse::<RunId>()
    {
        PgBackend::lock_run(tx, run_id).await?;
    }
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
    wait_id: i64,
    holder: &concurrency::Holder,
    jobset_id: Option<i64>,
) -> Result<(), ControlError> {
    match holder {
        concurrency::Holder::Run(run_id) => {
            set_holder(
                tx,
                namespace,
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
                // Re-park at the original FIFO position and release the
                // group: no holder owns it (lite's `requeue_wait` after
                // `release_hold`), so the finished holder's row cannot wedge
                // every later arrival.
                requeue_waiter(
                    tx,
                    namespace,
                    key,
                    wait_id,
                    &GateHolder {
                        holder: holder.clone(),
                        jobset_id: None,
                    },
                )
                .await?;
                clear_group(tx, namespace, key).await?;
                return Ok(());
            }
            set_holder(
                tx,
                namespace,
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
    // The jobset's gates live in the run's namespace, like every other
    // concurrency key (lite reads it from the run's job row).
    let namespace: String = tx
        .query_opt(
            "SELECT namespace_id FROM runs WHERE run_id=$1::text::uuid",
            &[&run_id.0.to_string()],
        )
        .await
        .map_err(db)?
        .map(|row| row.get(0))
        .unwrap_or_else(|| "default".to_owned());
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
            backend, tx, &namespace, &key, &display, &holder, cancel, queue,
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
            "SELECT g.repository, g.group_name, r.namespace_id FROM jobset_gates g \
             JOIN jobsets s ON s.jobset_id = g.jobset_id \
             JOIN runs r ON r.run_id = s.run_id \
             WHERE g.jobset_id=$1 AND g.acquired",
            &[&jobset_id],
        )
        .await
        .map_err(db)?;
    for gate in gates {
        let key = (gate.get::<_, String>(0), gate.get::<_, String>(1));
        let namespace: String = gate.get(2);
        promote_after_release(backend, tx, &namespace, &key).await?;
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

/// A run-waiter promoted: the jobs it parked behind its own workflow gate go
/// back to `blocked` and the normal promotion sweep decides what may run
/// (needs, job-level gates, max-parallel) — mirroring lite's
/// `held_jobs_of_run` → `promote_run`.
async fn resume_held_run(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    run_id: RunId,
) -> Result<(), ControlError> {
    let Some(graph) = backend.load_graph(tx, run_id).await? else {
        // Foreign run not loadable in this transaction's scope → leave the
        // wait consumed; the run's own commands re-park if still needed
        // (matches the old "foreign run stays parked" behavior).
        return Ok(());
    };
    // Only the jobs parked behind this run's workflow gate: a job with its
    // own wait row stays with that gate.
    let parked = held_jobs_of_run(tx, run_id).await?;
    let mut sweep = Sweep::new(backend, tx).await?;
    sweep.graphs.insert(run_id, graph);
    let now = sweep.now;
    let jobs_status = sweep
        .graphs
        .get(&run_id)
        .expect("inserted")
        .record
        .jobs
        .clone();
    {
        let Sweep { graphs, dirty, .. } = &mut sweep;
        let graph = graphs.get_mut(&run_id).expect("inserted");
        for job_id in &parked {
            let Some(node) = graph.nodes.get_mut(job_id) else {
                continue;
            };
            // Recount from the graph: submit skipped the retiming pass for a
            // held run, so a need that was already terminal at submit would
            // otherwise keep the node out of the sweep forever.
            let unsettled = node
                .needs
                .iter()
                .filter(|need| {
                    jobs_status
                        .get(*need)
                        .is_none_or(|status| !status.is_terminal())
                })
                .count() as i32;
            node.remaining_needs = unsettled;
            if unsettled == 0 {
                node.deps_ready_at_us = Some(now);
            }
            node.queue_state = logic::QueueState::Blocked;
            dirty.insert((run_id, job_id.clone()));
        }
        graph.resummarize();
    }
    sweep.sweep().await?;
    sweep.flush().await?;
    Ok(())
}

/// The jobs a run parked behind its own workflow gate (no per-job wait row
/// of their own), in `job_order`.
async fn held_jobs_of_run(tx: &Transaction<'_>, run_id: RunId) -> Result<Vec<JobId>, ControlError> {
    Ok(tx
        .query(
            "SELECT j.job_id FROM jobs j WHERE j.run_id=$1::text::uuid \
             AND j.queue_state='held' AND j.status='pending' \
             AND NOT EXISTS (SELECT 1 FROM concurrency_waits w \
                   WHERE w.holder_run_id = j.run_id \
                     AND (w.holder_kind = 'run' \
                          OR (w.holder_kind = 'job' AND w.holder_job_id = j.job_id) \
                          OR (w.holder_kind = 'jobset' AND EXISTS ( \
                              SELECT 1 FROM jobsets s \
                              WHERE s.jobset_id = w.holder_jobset_id \
                                AND s.job_ids ? j.job_id)))) \
             ORDER BY j.job_order",
            &[&run_id.0.to_string()],
        )
        .await
        .map_err(db)?
        .iter()
        .map(|row| JobId(row.get::<_, String>(0)))
        .collect())
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
        apply_resolved_runs_on(node, queued.runs_on);
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
    emit_outbox(
        tx,
        Some(graph.record.run_id),
        "job.queued.v1",
        serde_json::json!({"job_id": job_id.0, "status": "queued"}),
    )
    .await?;
    notify_wake(
        tx,
        crate::control::wake::Wake {
            ready: 1,
            broadcast: false,
        },
    )
    .await?;
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
    // A job its namespace would not let start (state or running caps) must
    // not reserve an idle runner; it waits pool-pending like any job with no
    // free runner, and the claim admits it once the namespace does. The gate
    // is an uncorrelated sub-select over the node's own namespace and pool
    // key (evaluated once per statement, and valid whether or not the node's
    // row is flushed yet), so a gated job simply sees no candidates.
    let candidates = tx
        .query(
            &format!(
                "SELECT DISTINCT s.runner_id FROM runner_sessions s \
                 JOIN runners r ON r.runner_id=s.runner_id WHERE s.runner_id IS NOT NULL \
                 AND (SELECT {} FROM (SELECT $1::text AS namespace_id, \
                      $2::text AS pool_key) j)",
                crate::control::types::NAMESPACE_ADMITS_CLAIM
            ),
            &[&graph.namespace, &node.pool_key],
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
pub(super) async fn clear_assignment(
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
///
/// Statements: `SELECT job_id, status FROM jobs .. FOR UPDATE`; per
/// in-flight job one deduped `INSERT INTO job_cancellations`; one `UPDATE
/// jobs .. SET status='cancelled'` over non-terminal rows; `UPDATE runs ..
/// SET status='completed', conclusion='cancelled'`; per expandable node
/// `retire_node_requests`; `DELETE`s of `job_assignments` /
/// `provision_requests` / `jobsets`; concurrency release+promotion; one
/// `run.completed.v1` outbox row when the run newly went terminal.
async fn cancel_run_tx(
    backend: &PgBackend,
    tx: &Transaction<'_>,
    run_id: RunId,
    reason: Option<&str>,
) -> Result<usize, ControlError> {
    let _ = reason;
    // In-flight attempts get a pending cancellation the next poll delivers;
    // every other non-terminal job simply turns terminal (its runner holds
    // nothing to interrupt).
    let in_flight: Vec<JobId> = tx
        .query(
            "SELECT job_id FROM jobs WHERE run_id=$1::text::uuid \
             AND status = 'in_progress' ORDER BY job_id FOR UPDATE",
            &[&run_id.0.to_string()],
        )
        .await
        .map_err(db)?
        .iter()
        .map(|row| JobId(row.get::<_, String>(0)))
        .collect();
    let mut cancellations = 0;
    for job_id in &in_flight {
        if enqueue_cancellation_job(tx, run_id, job_id).await? {
            cancellations += 1;
        }
    }
    // Expandable nodes minted placeholder requests at submit/expansion;
    // those settle with the node so no in-flight marker outlives it.
    let expandable: Vec<JobId> = tx
        .query(
            "SELECT j.job_id FROM jobs j LEFT JOIN job_specs s \
             ON s.run_id = j.run_id AND s.job_id = j.job_id \
             WHERE j.run_id=$1::text::uuid \
            AND j.status NOT IN ('success','failure','cancelled','skipped','timed_out') \
            AND ((s.deferred_matrix IS NOT NULL AND s.deferred_matrix <> 'null') \
                  OR (s.reusable_call IS NOT NULL AND s.reusable_call <> 'null'::jsonb) \
                  OR j.queue_state IN ('pending_expansion','expanding'))",
            &[&run_id.0.to_string()],
        )
        .await
        .map_err(db)?
        .iter()
        .map(|row| JobId(row.get::<_, String>(0)))
        .collect();
    let now = now_us();
    tx.execute(
        concat!(
            "UPDATE jobs SET status='cancelled', queue_state='none', \
             expand_generation=expand_generation+1, \
             completed_at=",
            ts!("$2"),
            ", started_at=COALESCE(started_at,",
            ts!("$2"),
            "), claimed_by_runner_id=NULL, claimed_at=NULL \
             WHERE run_id=$1::text::uuid AND status NOT IN \
             ('success','failure','cancelled','skipped','timed_out')"
        ),
        &[&run_id.0.to_string(), &now],
    )
    .await
    .map_err(db)?;
    for job_id in &expandable {
        retire_node_requests(
            tx,
            run_id,
            job_id,
            Retirement::Settle(ExecutionStatus::Cancelled),
        )
        .await?;
    }
    // A cancelled run is terminal: stamp completion metadata so the record
    // carries `completed_at`/`conclusion` like a settled run does.
    let finalized = tx
        .execute(
            "UPDATE runs SET status='completed', conclusion='cancelled', \
             completed_at=COALESCE(completed_at, now()), \
             started_at=COALESCE(started_at, now()) \
             WHERE run_id=$1::text::uuid AND status <> 'completed'",
            &[&run_id.0.to_string()],
        )
        .await
        .map_err(db)?;
    // Clear dispatch intent for this run.
    for sql in [
        "DELETE FROM job_assignments WHERE run_id=$1::text::uuid",
        "DELETE FROM provision_requests WHERE run_id=$1::text::uuid",
        "DELETE FROM jobsets WHERE run_id=$1::text::uuid",
    ] {
        tx.execute(sql, &[&run_id.0.to_string()])
            .await
            .map_err(db)?;
    }
    release_concurrency_for_run(backend, tx, run_id).await?;
    if finalized > 0 {
        emit_outbox(
            tx,
            Some(run_id),
            "run.completed.v1",
            serde_json::json!({"status": "cancelled"}),
        )
        .await?;
    }
    Ok(cancellations)
}

/// Cancellation bookkeeping for the cancel commands: mark a
/// `job_cancellations` row pending for the job's live attempt
/// (deduplicated while undelivered). The poll path turns the marker into
/// the runner-facing `JobCancellation` message — an eager `session_messages`
/// row would redeliver a bogus `request_id` body and refire
/// `pending_cancellation`. `true` when the live attempt is now pending
/// cancellation.
pub(super) async fn enqueue_cancellation_job(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<bool, ControlError> {
    let Some(row) = tx
        .query_opt(
            "SELECT request_id FROM job_requests WHERE run_id=$1::text::uuid \
             AND job_id=$2 AND result IS NULL ORDER BY request_id DESC LIMIT 1",
            &[&run_id.0.to_string(), &job_id.0],
        )
        .await
        .map_err(db)?
    else {
        return Ok(false);
    };
    let request_id: i64 = row.get(0);
    tx.execute(
        "INSERT INTO job_cancellations (request_id) \
         SELECT $1 WHERE NOT EXISTS (\
         SELECT 1 FROM job_cancellations WHERE request_id = $1 \
         AND delivered_at IS NULL)",
        &[&request_id],
    )
    .await
    .map_err(db)?;
    // The owning runner must poll to learn about the cancellation: wake every
    // waiter on every node (a hint; the poll re-reads its own session).
    notify_wake(
        tx,
        crate::control::wake::Wake {
            ready: 0,
            broadcast: true,
        },
    )
    .await?;
    Ok(true)
}

/// `cancel_job_inner` parity: cancel one job plus its expanded subtree,
/// queueing a cancellation for the in-flight attempt, retiring an
/// expandable node's placeholder requests and releasing its gate presence.
/// Re-summarizes the run after the subtree settles.
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
            "SELECT status, queue_state, EXISTS (SELECT 1 FROM job_specs s \
             WHERE s.run_id = jobs.run_id AND s.job_id = jobs.job_id \
             AND ((s.deferred_matrix IS NOT NULL AND s.deferred_matrix <> 'null') \
                  OR (s.reusable_call IS NOT NULL AND s.reusable_call <> 'null'::jsonb))) \
             FROM jobs WHERE run_id=$1::text::uuid AND job_id=$2 FOR UPDATE",
            &[&run_id.0.to_string(), &job_id.0],
        )
        .await
        .map_err(db)?;
    let Some(row) = row else { return Ok(0) };
    let status: String = row.get(0);
    let queue_state: String = row.get(1);
    let expandable: bool =
        row.get::<_, bool>(2) || matches!(queue_state.as_str(), "pending_expansion" | "expanding");
    let now = now_us();
    let mut count = 0;
    // Only a running attempt can be interrupted; queued/blocked jobs simply
    // turn terminal (their placeholder request retires below when the node
    // is expandable).
    if status == "in_progress" && enqueue_cancellation_job(tx, run_id, job_id).await? {
        count = 1;
    }
    if !matches!(
        status.as_str(),
        "success" | "failure" | "cancelled" | "skipped" | "timed_out"
    ) {
        tx.execute(
            concat!(
                "UPDATE jobs SET status='cancelled', queue_state='none', \
                 expand_generation=expand_generation+1, \
                 completed_at=",
                ts!("$3"),
                ", started_at=COALESCE(started_at,",
                ts!("$3"),
                "), claimed_by_runner_id=NULL, claimed_at=NULL \
                 WHERE run_id=$1::text::uuid AND job_id=$2"
            ),
            &[&run_id.0.to_string(), &job_id.0, &now],
        )
        .await
        .map_err(db)?;
        clear_assignment(tx, run_id, job_id).await?;
        if expandable {
            retire_node_requests(
                tx,
                run_id,
                job_id,
                Retirement::Settle(ExecutionStatus::Cancelled),
            )
            .await?;
        }
        release_concurrency_for_job(backend, tx, run_id, job_id).await?;
    }
    // An expanded caller owns its callee subtree (matrix legs included —
    // they carry `parent_job_id` too); a cancelled caller cancels it.
    let children: Vec<JobId> = tx
        .query(
            "SELECT job_id FROM jobs WHERE run_id=$1::text::uuid AND parent_job_id=$2 \
             AND status NOT IN ('success','failure','cancelled','skipped','timed_out')",
            &[&run_id.0.to_string(), &job_id.0],
        )
        .await
        .map_err(db)?
        .iter()
        .map(|row| JobId(row.get::<_, String>(0)))
        .collect();
    for child in children {
        count += Box::pin(cancel_job_tx(backend, tx, run_id, &child, reason)).await?;
    }
    // Re-summarize the run once the subtree is settled (idempotent on the
    // recursion: each level recomputes the same aggregate).
    summarize_run_tx(tx, run_id).await?;
    Ok(count)
}

impl PgBackend {
    /// `cancel_run`: `NotFound` when the run does not exist; otherwise one
    /// transaction — the run row is the mutex — cancelling every
    /// non-terminal job, queueing in-flight cancellations, releasing
    /// concurrency presence and promoting released waiters. The outcome is
    /// the post-transition record plus the ready-queue gauges.
    pub(super) async fn cancel_run(
        &self,
        run_id: RunId,
        reason: Option<String>,
    ) -> Result<CancelOutcome, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        if !PgBackend::lock_run(&tx, run_id).await? {
            tx.rollback().await.map_err(db)?;
            return Err(ControlError::NotFound(format!("run {run_id}")));
        }
        let cancellations = cancel_run_tx(self, &tx, run_id, reason.as_deref()).await?;
        let (cancelled_jobs, queue_nonempty, next_runs_on, pending_cancels) =
            cancel_outcome_gauges(&tx, run_id).await?;
        let record = self
            .load_graph(&tx, run_id)
            .await?
            .map(|graph| graph.record);
        tx.commit().await.map_err(db)?;
        Ok(CancelOutcome {
            cancellations,
            run_status: record.as_ref().map(|record| record.status),
            queue_nonempty: queue_nonempty || pending_cancels,
            record,
            cancelled_jobs,
            next_runs_on,
        })
    }

    /// `cancel_job`: cancel one job (and an expanded caller's subtree)
    /// under the run-row mutex; unknown run or job is a no-op that still
    /// reports the post-transition gauges.
    pub(super) async fn cancel_job(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<CancelOutcome, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        PgBackend::lock_run(&tx, run_id).await?;
        let cancellations = cancel_job_tx(self, &tx, run_id, job_id, None).await?;
        let (cancelled_jobs, queue_nonempty, next_runs_on, pending_cancels) =
            cancel_outcome_gauges(&tx, run_id).await?;
        let record = self
            .load_graph(&tx, run_id)
            .await?
            .map(|graph| graph.record);
        tx.commit().await.map_err(db)?;
        Ok(CancelOutcome {
            cancellations,
            run_status: record.as_ref().map(|record| record.status),
            queue_nonempty: queue_nonempty || pending_cancels,
            record,
            cancelled_jobs,
            next_runs_on,
        })
    }
}

/// The shared `CancelOutcome` reads after a cancel transition: the run's
/// cancelled jobs (id order), the global ready depth and front `runs-on`
/// labels, and whether any cancellation still awaits delivery.
async fn cancel_outcome_gauges(
    tx: &Transaction<'_>,
    run_id: RunId,
) -> Result<(Vec<JobId>, bool, Vec<String>, bool), ControlError> {
    let cancelled_jobs = tx
        .query(
            "SELECT job_id FROM jobs WHERE run_id=$1::text::uuid \
             AND status='cancelled' ORDER BY job_id",
            &[&run_id.0.to_string()],
        )
        .await
        .map_err(db)?
        .iter()
        .map(|row| JobId(row.get::<_, String>(0)))
        .collect();
    let queue_nonempty: bool = tx
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM jobs WHERE queue_state='ready')",
            &[],
        )
        .await
        .map_err(db)?
        .get(0);
    let next_runs_on: Vec<String> = tx
        .query_opt(
            "SELECT runs_on::text FROM jobs WHERE queue_state='ready' \
             ORDER BY priority DESC, run_order, job_order, run_id, job_id LIMIT 1",
            &[],
        )
        .await
        .map_err(db)?
        .map(|row| from_json(row.get(0)))
        .transpose()?
        .unwrap_or_default();
    let pending_cancels: bool = tx
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM job_cancellations WHERE delivered_at IS NULL)",
            &[],
        )
        .await
        .map_err(db)?
        .get(0);
    Ok((
        cancelled_jobs,
        queue_nonempty,
        next_runs_on,
        pending_cancels,
    ))
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
            environment_gate: node.environment_gate.clone(),
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
                        self.promote(run_id, &job_id).await?;
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
            if !settled {
                return Ok(());
            }
        }
    }

    /// A blocked node whose needs settled promotes: jobset gates for a
    /// reusable caller, expansion for a deferred node, gate + ready for a
    /// dispatchable job.
    async fn promote(&mut self, run_id: RunId, job_id: &JobId) -> Result<(), ControlError> {
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
        // Max-parallel: matrix legs queue behind the cohort cap. A leg
        // admitted earlier in this pass is already `ready` in the graph, so
        // the scan counts it exactly once.
        if let Some(limit) = max_parallel
            && !self.under_max_parallel(run_id, &base_id, limit)
        {
            // Stays `blocked` — a later leg completion re-admits it.
            return Ok(());
        }
        // Deferred `runs-on` resolves here, once needs outputs exist. Fail
        // before occupying a concurrency slot if the concrete labels can
        // never be served.
        if self.hydrate_and_reject_labels(run_id, job_id).await? {
            return Ok(());
        }
        // Environment protection rules run ahead of concurrency gating: a
        // denied or waiting job never occupies a slot, and a gate failure
        // is the reason the job ends, not "no runner could match".
        {
            let environment_name = self.environment_name_for(run_id, job_id).await?;
            if let Some(env_name) = environment_name {
                let resolver = self.backend.environment_resolver();
                let (repository, git_ref) = {
                    let graph = self.graphs.get(&run_id).expect("loaded");
                    (
                        graph.record.submission.repository.clone(),
                        graph.record.submission.git_ref.clone(),
                    )
                };
                let lookup = resolver.lookup_sync(&repository, &env_name);
                let mut gate = self
                    .graphs
                    .get(&run_id)
                    .and_then(|g| g.nodes.get(job_id))
                    .and_then(|n| n.environment_gate.clone());
                let verdict = crate::runtime_scheduling::evaluate_environment_gate(
                    &lookup,
                    &git_ref,
                    run_id,
                    job_id,
                    &env_name,
                    &mut gate,
                    crate::models::now_unix_nanos(),
                );
                match verdict {
                    crate::runtime_scheduling::EnvironmentGateOutcome::Wait => {
                        // Armed gates stamp progress on the node; a `Pending`
                        // hold (GitHub rules not fetched yet) records only
                        // the resolved name — the reaper sweep re-evaluates
                        // once the resolver fills the entry.
                        if let Some(node) = self.node_mut(run_id, job_id) {
                            node.environment_gate = gate;
                            node.queue_state = logic::QueueState::Held;
                            node.status = ExecutionStatus::Pending;
                        }
                        self.mark(run_id, job_id);
                        let graph = self.graphs.get_mut(&run_id).expect("loaded");
                        graph
                            .record
                            .jobs
                            .insert(job_id.clone(), ExecutionStatus::Pending);
                        return Ok(());
                    }
                    crate::runtime_scheduling::EnvironmentGateOutcome::Failed => {
                        if let Some(node) = self.node_mut(run_id, job_id) {
                            node.environment_gate = gate;
                        }
                        self.mark(run_id, job_id);
                        // `settle_node` records the failure in the sweep's
                        // outcome; pushing it a second time would count the
                        // job twice and re-report its check run.
                        self.settle_node(run_id, job_id, ExecutionStatus::Failure)
                            .await?;
                        return Ok(());
                    }
                    crate::runtime_scheduling::EnvironmentGateOutcome::Proceed => {
                        if gate.is_some()
                            || self
                                .graphs
                                .get(&run_id)
                                .and_then(|g| g.nodes.get(job_id))
                                .is_some_and(|n| n.environment_gate.is_some())
                        {
                            if let Some(node) = self.node_mut(run_id, job_id) {
                                node.environment_gate = gate;
                            }
                            self.mark(run_id, job_id);
                        }
                    }
                }
            }
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

    /// Hydrate needs/`runs-on` and fail the job when the resolved labels are
    /// unhostable or unsatisfiable. `true` means the node was settled.
    async fn hydrate_and_reject_labels(
        &mut self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<bool, ControlError> {
        let mut resolved = None;
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
            resolved = Some(queued.runs_on.clone());
            self.messages
                .insert((run_id, job_id.clone()), Some(queued.message));
            self.hydrated.insert((run_id, job_id.clone()));
        }
        if let Some(runs_on) = resolved.clone()
            && let Some(node) = self.node_mut(run_id, job_id)
        {
            apply_resolved_runs_on(node, runs_on);
            self.mark(run_id, job_id);
        }
        let runs_on = resolved.unwrap_or_else(|| {
            self.graphs
                .get(&run_id)
                .and_then(|graph| graph.nodes.get(job_id))
                .map(|node| node.runs_on.clone())
                .unwrap_or_default()
        });
        if let Some(reason) = promotion_label_reason(self.backend, self.tx, &runs_on).await? {
            tracing::warn!(
                %run_id,
                job = %job_id.0,
                labels = ?runs_on,
                pool_labels = ?self.backend.pool_labels(),
                %reason,
                "runs-on unsatisfiable by runner pool; failing the job"
            );
            // `settle_node` records the failure in the sweep's outcome;
            // pushing it a second time would count the job twice and
            // re-report its check run.
            self.settle_node(run_id, job_id, ExecutionStatus::Failure)
                .await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// The environment name a job's gate evaluates against: the hydrated
    /// message's `actions_environment` (deferred names resolved), else the
    /// armed gate's stamp, else the spec literal. `None` for environment-less
    /// jobs.
    ///
    /// The message wins over the stamp: a gate armed while the name was still
    /// a `${{ needs.* }}` template stamped that text, and re-evaluating the
    /// template after `hydrate_needs_context` resolved the real name would
    /// decide against an environment nobody configured.
    async fn environment_name_for(
        &mut self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<String>, ControlError> {
        let message_name = self.message(run_id, job_id).await?.and_then(|message| {
            message
                .actions_environment
                .map(|environment| environment.name)
        });
        if let Some(name) = message_name
            .as_deref()
            .and_then(crate::runtime_scheduling::resolved_environment_name_of)
        {
            return Ok(Some(name.to_owned()));
        }
        let node_gate = self
            .graphs
            .get(&run_id)
            .and_then(|g| g.nodes.get(job_id))
            .and_then(|n| n.environment_gate.clone());
        if let Some(name) = node_gate.as_ref().and_then(|g| g.environment_name.clone()) {
            return Ok(Some(name));
        }
        Ok(message_name.or_else(|| {
            self.graphs
                .get(&run_id)
                .and_then(|g| g.nodes.get(job_id))
                .and_then(|node| {
                    crate::runtime_scheduling::environment_gate_name_of(node.environment.as_ref())
                })
                .map(str::to_owned)
        }))
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
            let runs_on = queued.runs_on.clone();
            self.messages
                .insert((run_id, job_id.clone()), Some(queued.message));
            self.hydrated.insert((run_id, job_id.clone()));
            if let Some(node) = self.node_mut(run_id, job_id) {
                apply_resolved_runs_on(node, runs_on);
            }
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
        emit_outbox(
            self.tx,
            Some(run_id),
            "job.queued.v1",
            serde_json::json!({"job_id": job_id.0, "status": "queued"}),
        )
        .await?;
        notify_wake(
            self.tx,
            crate::control::wake::Wake {
                ready: 1,
                broadcast: false,
            },
        )
        .await?;
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
        emit_outbox(
            self.tx,
            Some(run_id),
            "expansion.queued.v1",
            serde_json::json!({"job_id": job_id.0}),
        )
        .await
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
        release_concurrency_for_job(self.backend, self.tx, run_id, job_id).await?;
        let expandable = self
            .graphs
            .get(&run_id)
            .and_then(|g| g.nodes.get(job_id))
            .is_some_and(|node| node.expandable());
        if expandable {
            retire_node_requests(self.tx, run_id, job_id, Retirement::Settle(status)).await?;
        }
        clear_assignment(self.tx, run_id, job_id).await?;
        // Dependents' `remaining_needs` recompute. A declared need matches a
        // job id or its matrix base, and an expanded matrix parent is replaced
        // by its legs — so the fast-path counter cannot be decremented per
        // settled job: a base need must wait for *every* leg, and a leg's id
        // never appears in the dependent's `job_needs` row.
        let base_id = self
            .graphs
            .get(&run_id)
            .and_then(|graph| graph.nodes.get(job_id))
            .map(|node| node.base_id.clone());
        let dependents: Vec<JobId> = self
            .graphs
            .get(&run_id)
            .map(|graph| {
                graph
                    .nodes
                    .iter()
                    .filter(|(_, node)| {
                        !node.status.is_terminal()
                            && node.queue_state != logic::QueueState::None
                            && node.needs.iter().any(|need| {
                                need.0 == job_id.0 || base_id.as_deref() == Some(need.0.as_str())
                            })
                    })
                    .map(|(dependent, _)| dependent.clone())
                    .collect()
            })
            .unwrap_or_default();
        for dependent in dependents {
            let remaining: i32 = {
                let Some(graph) = self.graphs.get(&run_id) else {
                    break;
                };
                let Some(node) = graph.nodes.get(&dependent) else {
                    continue;
                };
                node.needs
                    .iter()
                    .map(|need| {
                        graph
                            .nodes
                            .iter()
                            .filter(|(id, dependency)| {
                                (id == &need || dependency.base_id == need.0)
                                    && !(dependency.kind == NodeKind::MatrixParent
                                        && dependency.has_children)
                                    && !dependency.status.is_terminal()
                            })
                            .count() as i32
                    })
                    .sum()
            };
            if let Some(node) = self.node_mut(run_id, &dependent) {
                node.remaining_needs = remaining;
                if remaining <= 0 {
                    node.deps_ready_at_us = Some(now);
                }
            }
            self.mark(run_id, &dependent);
        }
        self.outcome.push(status, run_id, job_id.clone());
        self.concluded.push((job_id.clone(), status));
        // Outbox: the job's terminal state, and the run's first transition
        // into `completed` (a run already terminal emits nothing again).
        let was_completed = self
            .graphs
            .get(&run_id)
            .is_some_and(|graph| graph.record.status.is_terminal());
        if let Some(graph) = self.graphs.get_mut(&run_id) {
            graph.resummarize();
            graph.touched = true;
        }
        emit_outbox(
            self.tx,
            Some(run_id),
            "job.completed.v1",
            serde_json::json!({"job_id": job_id.0, "status": status_str(status)}),
        )
        .await?;
        let now_completed = self
            .graphs
            .get(&run_id)
            .is_some_and(|graph| graph.record.status.is_terminal());
        if now_completed && !was_completed {
            // A terminal run must not hold a workflow-level slot: release the
            // group and promote its next waiter (lite's `settle_node` does the
            // same after `release_concurrency_for_job`).
            release_concurrency_for_run(self.backend, self.tx, run_id).await?;
            let conclusion = self
                .graphs
                .get(&run_id)
                .and_then(|graph| graph.record.conclusion.clone())
                .unwrap_or_else(|| status_str(status).to_owned());
            emit_outbox(
                self.tx,
                Some(run_id),
                "run.completed.v1",
                serde_json::json!({"conclusion": conclusion}),
            )
            .await?;
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
        let ids = serde_json::to_string(&[job_id.0.as_str()]).unwrap_or_default();
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

    /// Whether the cohort of `base_id` legs is under `limit`: legs that are
    /// ready, claimed or running (lite's `under_max_parallel` rule).
    fn under_max_parallel(&self, run_id: RunId, base_id: &str, limit: u64) -> bool {
        let Some(graph) = self.graphs.get(&run_id) else {
            return true;
        };
        let mut active = 0;
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
    if let Some(graph) = graph
        && graph
            .nodes
            .get(job_id)
            .is_some_and(|node| node.contributes())
    {
        graph.record.jobs.insert(job_id.clone(), status);
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

/// Append one outbox row for a state change a command just made. `payload`
/// carries ids and states only. Rows written here are not `NdjsonEvent`s
/// (the stream consumer skips them) and carry no version; a `job_id` in the
/// payload is copied to the row's column. A `run_id` with no live `runs` row
/// (archived between events) leaves the namespace at its default.
pub(super) async fn emit_outbox(
    tx: &Transaction<'_>,
    run_id: Option<RunId>,
    topic: &str,
    payload: serde_json::Value,
) -> Result<(), ControlError> {
    let job_id = payload
        .get("job_id")
        .and_then(|value| value.as_str())
        .map(str::to_owned);
    insert_outbox(tx, run_id, job_id.as_deref(), None, topic, &payload).await
}

/// The one `INSERT INTO outbox_events`. It touches no run or job row, so
/// emitting an event takes no row lock; the namespace is resolved inside the
/// statement and `origin` is this connection's `preloop.origin`.
async fn insert_outbox(
    tx: &Transaction<'_>,
    run_id: Option<RunId>,
    job_id: Option<&str>,
    version: Option<i64>,
    topic: &str,
    payload: &serde_json::Value,
) -> Result<(), ControlError> {
    let run_text = run_id.map(|id| id.0.to_string());
    let payload_text = serde_json::to_string(payload).map_err(ControlError::backend)?;
    tx.execute(
        "INSERT INTO outbox_events (namespace_id, run_id, job_id, version, origin, topic, payload) \
         VALUES (COALESCE((SELECT namespace_id FROM runs WHERE run_id = $1::text::uuid), $2), \
                 $1::text::uuid, $3, $4, COALESCE(current_setting('preloop.origin', true), ''), \
                 $5, $6::text::jsonb)",
        &[
            &run_text,
            &crate::control::types::DEFAULT_NAMESPACE,
            &job_id,
            &version,
            &topic,
            &payload_text,
        ],
    )
    .await
    .map_err(db)?;
    Ok(())
}

/// Publish a wake hint for work this transaction made available. `pg_notify`
/// delivers at COMMIT, so every node's `LISTEN` connection
/// ([`crate::control::wake`]) wakes its long-poll waiters exactly when the
/// work becomes visible. Waiters on this node are woken by the handler; the
/// notification is what reaches the others.
pub(super) async fn notify_wake(
    tx: &Transaction<'_>,
    wake: crate::control::wake::Wake,
) -> Result<(), ControlError> {
    tx.execute(
        "SELECT pg_notify($1, $2)",
        &[&crate::control::wake::CHANNEL, &wake.encode()],
    )
    .await
    .map_err(db)?;
    Ok(())
}

/// Append one durable event to the transactional outbox (insert-only). The
/// versioned topic is the event's serde tag (`job_status` ->
/// `job_status.v1`). A status event is stamped with the version of the row it
/// reports on, or not published at all when that row has already settled on a
/// different final state (see [`logic::EventStamp`]). Reads the row with a
/// plain `SELECT`: no row lock is taken.
pub(super) async fn append_event_tx(
    tx: &Transaction<'_>,
    event: &preloop_gha_protocol::NdjsonEvent,
) -> Result<(), ControlError> {
    use preloop_gha_protocol::NdjsonEvent;
    let run_id = crate::control::backend::event_run_id(event);
    let payload = serde_json::to_value(event).map_err(ControlError::backend)?;
    let topic = payload
        .get("type")
        .and_then(|t| t.as_str())
        .map(|kind| format!("{kind}.v1"))
        .unwrap_or_else(|| "event.v1".to_owned());
    let (job_id, stamp) = match event {
        NdjsonEvent::JobStatus {
            run_id,
            job_id,
            status,
            ..
        } => {
            let stamp = tx
                .query_opt(
                    "SELECT status, version FROM jobs \
                     WHERE run_id = $1::text::uuid AND job_id = $2",
                    &[&run_id.0.to_string(), &job_id.0],
                )
                .await
                .map_err(db)?
                .map(|row| logic::job_event_stamp(*status, row.get(0), row.get(1)))
                .unwrap_or(logic::EventStamp::Unversioned);
            (Some(job_id.0.as_str()), stamp)
        }
        NdjsonEvent::RunStatus { run_id, status, .. } => {
            let stamp = tx
                .query_opt(
                    "SELECT status, conclusion, version FROM runs \
                     WHERE run_id = $1::text::uuid",
                    &[&run_id.0.to_string()],
                )
                .await
                .map_err(db)?
                .map(|row| logic::run_event_stamp(*status, row.get(0), row.get(1), row.get(2)))
                .unwrap_or(logic::EventStamp::Unversioned);
            (None, stamp)
        }
        NdjsonEvent::Annotation { job_id, .. } => {
            (Some(job_id.0.as_str()), logic::EventStamp::Unversioned)
        }
        _ => (None, logic::EventStamp::Unversioned),
    };
    let version = match stamp {
        logic::EventStamp::Stale => return Ok(()),
        logic::EventStamp::Version(version) => Some(version),
        logic::EventStamp::Unversioned => None,
    };
    insert_outbox(tx, run_id, job_id, version, &topic, &payload).await
}

/// Requeue a claimed job whose runner is gone: drop the claim — its attempt
/// is replaced via [`retry_attempt_tx`], whose abandoned runtime identity
/// lands in `retired` — and put the node back at the head of the ready
/// queue (its `jobs` row goes back to `queued`/`ready`).
pub(super) async fn requeue_claimed_tx(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    retired: &mut Vec<uuid::Uuid>,
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
    retired.extend(retry_attempt_tx(tx, run_id, job_id).await?);
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
    // The job is requestable again: `job.requested.v1` marks the fresh
    // head-of-queue request (same transition the initial queue observed).
    emit_outbox(
        tx,
        Some(run_id),
        "job.requested.v1",
        serde_json::json!({"job_id": job_id.0, "status": "queued"}),
    )
    .await?;
    notify_wake(
        tx,
        crate::control::wake::Wake {
            ready: 1,
            broadcast: false,
        },
    )
    .await?;
    Ok(true)
}

/// Replace the job's in-flight attempt so no credential of the abandoned
/// one survives the retry — the SQLite backend's `retry_attempt` contract.
/// The old row settles `cancelled` — its runtime token, debug token and
/// renewals stop validating — while its step manifest, logs and timeline
/// stay addressable. The replacement gets a fresh runtime identity (`jobId`
/// and the derived plan id), its own timeline, a pending step manifest and
/// an unissued debug token; the stored message and the deferred token-mint
/// recipe move to it. Returns the abandoned runtime identity, `None` when
/// nothing was in flight.
async fn retry_attempt_tx(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<Option<uuid::Uuid>, ControlError> {
    let run = run_id.0.to_string();
    let Some(row) = tx
        .query_opt(
            "SELECT request_id, agent_job_id::text, namespace_id FROM job_requests \
             WHERE run_id = $1::text::uuid AND job_id = $2 AND result IS NULL \
             ORDER BY request_id DESC LIMIT 1 FOR NO KEY UPDATE",
            &[&run, &job_id.0],
        )
        .await
        .map_err(db)?
    else {
        return Ok(None);
    };
    let request_id: i64 = row.get(0);
    let abandoned = codec::uuid(row.get(1))?;
    let namespace: String = row.get(2);
    let Some(mut message) = PgBackend::node_message(tx, run_id, job_id).await? else {
        return Err(ControlError::backend(anyhow::anyhow!(
            "request {request_id} has no message template"
        )));
    };
    tx.execute(
        "UPDATE job_requests SET result = 'cancelled', finished_at = now(), \
         runner_id = NULL, session_id = NULL WHERE request_id = $1",
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
    let agent_job_id = uuid::Uuid::new_v4();
    let timeline_id = uuid::Uuid::new_v4();
    let retry: i64 = tx
        .query_one(
            "INSERT INTO job_requests (run_id, job_id, namespace_id, agent_job_id, \
             timeline_id) VALUES ($1::text::uuid,$2,$3,$4::text::uuid,$5::text::uuid) \
             RETURNING request_id",
            &[
                &run,
                &job_id.0,
                &namespace,
                &agent_job_id.to_string(),
                &timeline_id.to_string(),
            ],
        )
        .await
        .map_err(db)?
        .get(0);
    insert_step_manifest(
        tx,
        agent_job_id,
        &crate::models::StepRecord::manifest(&message.steps),
    )
    .await?;
    message.job_id = agent_job_id;
    message.request_id = retry;
    message.plan.plan_id = agent_job_id.to_string();
    message.timeline.id = timeline_id;
    PgBackend::write_node_message(tx, run_id, job_id, &message).await?;
    tx.execute(
        "UPDATE github_token_requests SET request_id = $2 WHERE request_id = $1",
        &[&request_id, &retry],
    )
    .await
    .map_err(db)?;
    Ok(Some(abandoned))
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
    // The builder emits its baseline regexes first and appends one hint per
    // non-empty secret value last: only that tail encodes a value. Count it
    // before blanking the values, then drop exactly it (the rule
    // `message_template::strip_template` applies) — the acquire path only
    // re-adds value-derived hints, so a full clear would deliver every job
    // with no baseline mask.
    let derived_hints = crate::message_template::secret_hint_count(message);
    let mut names = Vec::new();
    for (name, value) in message.variables.iter_mut() {
        if value.is_secret == Some(true) {
            names.push(name.clone());
            value.value = None;
        }
    }
    message
        .mask_hints
        .truncate(message.mask_hints.len().saturating_sub(derived_hints));
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
        environment_gate: job.environment_gate.clone(),
        has_children: false,
    };
    (node, message, secret_names)
}

/// The committed run for this `(delivery, workflow path)` as seen INSIDE a
/// write transaction: the replay check has to share the inserting
/// transaction's snapshot so two nodes cannot both pass it.
async fn delivery_run_in_tx(
    tx: &Transaction<'_>,
    delivery_id: &str,
    workflow_path: &str,
) -> Result<Option<RunId>, ControlError> {
    tx.query_opt(
        "SELECT run_id::text FROM runs WHERE webhook_delivery_id = $1 \
         AND workflow_path = $2",
        &[&delivery_id, &workflow_path],
    )
    .await
    .map_err(db)?
    .map(|row| codec::run_id(&row.get::<_, String>(0)))
    .transpose()
}

/// Submit admission for `namespace`, inside the submit transaction: the
/// namespace state must admit the run, and for API/CLI submits each
/// configured submit-time limit (`max_jobs_per_run`, `submit_rate_per_minute`,
/// `max_queued_jobs`) must admit it too. Webhook-originated runs (`webhook`)
/// skip the limits: a refusal would lose the push, so they are recorded and
/// wait at claim. The limit row is locked before counting so concurrent
/// submits cannot both take the last slots; namespaces without a limit row
/// take no lock and run no counts.
async fn admit_submit(
    tx: &Transaction<'_>,
    namespace: &str,
    submitting: usize,
    webhook: bool,
) -> Result<(), ControlError> {
    use crate::control::types as t;
    let state: String = tx
        .query_one(
            "SELECT state FROM namespaces WHERE namespace_id = $1",
            &[&namespace],
        )
        .await
        .map_err(db)?
        .get(0);
    t::namespace_submit_admission(namespace, &state, webhook)?;
    if webhook {
        return Ok(());
    }
    let Some(limits) = tx
        .query_opt(
            "SELECT max_jobs_per_run, submit_rate_per_minute, max_queued_jobs \
             FROM namespace_limits WHERE namespace_id = $1 FOR UPDATE",
            &[&namespace],
        )
        .await
        .map_err(db)?
    else {
        return Ok(());
    };
    let (per_run, per_minute, max_queued): (Option<i32>, Option<i32>, Option<i32>) =
        (limits.get(0), limits.get(1), limits.get(2));
    if let Some(per_run) = per_run {
        t::namespace_run_size_admission(namespace, i64::from(per_run), submitting)?;
    }
    if let Some(per_minute) = per_minute {
        let recent: i64 = tx
            .query_one(
                "SELECT count(*) FROM runs WHERE namespace_id = $1 \
                 AND created_at > now() - interval '1 minute'",
                &[&namespace],
            )
            .await
            .map_err(db)?
            .get(0);
        t::namespace_rate_admission(namespace, i64::from(per_minute), recent)?;
    }
    if let Some(max_queued) = max_queued {
        let queued: i64 = tx
            .query_one(
                &format!(
                    "SELECT count(*) FROM jobs WHERE namespace_id = $1 AND queue_state IN {}",
                    t::NAMESPACE_QUEUED_STATES
                ),
                &[&namespace],
            )
            .await
            .map_err(db)?
            .get(0);
        t::namespace_queue_admission(namespace, i64::from(max_queued), queued, submitting)?;
    }
    Ok(())
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
            mut record,
            jobs,
            workflow_concurrency,
            empty_concurrency_group,
            check_hostable,
        } = submit;
        // A webhook delivery that already produced a run for this workflow
        // path replays that run instead of duplicating it. The check must see
        // the inserting transaction's snapshot: a redelivery racing on another
        // node has to be caught by `runs_delivery`, never as a duplicate-key
        // failure.
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        if let Some(delivery_id) = &record.webhook_delivery_id {
            let existing = delivery_run_in_tx(&tx, delivery_id, &record.workflow_path_str).await?;
            if let Some(existing) = existing {
                drop(tx);
                drop(client);
                let existing = self.run_record(existing).await?;
                return self.replay_outcome(existing).await;
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
                next_runs_on: Vec::new(),
                events: Vec::new(),
            });
        }

        let now = now_us();
        let run = record.run_id.0.to_string();
        tx.execute(
            "INSERT INTO namespaces (namespace_id) VALUES ($1) ON CONFLICT DO NOTHING",
            &[&namespace],
        )
        .await
        .map_err(db)?;
        admit_submit(
            &tx,
            &namespace,
            jobs.len(),
            record.webhook_delivery_id.is_some(),
        )
        .await?;
        // A caller may replay a run number (tests and push-back retries do);
        // GitHub allocates the next number rather than surfacing a unique-key
        // failure. Keep the counter ahead of both its row and existing runs.
        let collision: bool = tx
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM runs WHERE namespace_id=$1 \
                 AND repository=$2 AND workflow_path=$3 AND run_number=$4 \
                 AND run_attempt=$5)",
                &[
                    &namespace,
                    &record.submission.repository,
                    &record.workflow_path_str,
                    &(record.run_number as i64),
                    &(record.run_attempt as i32),
                ],
            )
            .await
            .map_err(db)?
            .get(0);
        if collision {
            record.run_number = tx
                .query_one(
                    "INSERT INTO workflow_run_numbers \
                     (namespace_id, repository, workflow_path, last_run_number) \
                     VALUES ($1,$2,$3, GREATEST(1, COALESCE( \
                         (SELECT MAX(run_number) + 1 FROM runs \
                          WHERE namespace_id=$1 AND repository=$2 AND workflow_path=$3), 1))) \
                     ON CONFLICT (namespace_id, repository, workflow_path) DO UPDATE SET \
                       last_run_number = GREATEST(\
                         workflow_run_numbers.last_run_number + 1,\
                         COALESCE((SELECT MAX(run_number) + 1 FROM runs \
                                   WHERE namespace_id=$1 AND repository=$2 AND workflow_path=$3), 1)) \
                     RETURNING last_run_number",
                    &[
                        &namespace,
                        &record.submission.repository,
                        &record.workflow_path_str,
                    ],
                )
                .await
                .map_err(db)?
                .get::<_, i64>(0)
                .max(0) as u64;
        }
        // With a delivery id the insert tolerates the winner of a concurrent
        // redelivery (it waits for that transaction, then does nothing); the
        // `runs_delivery` index is the arbiter. Without one, the plain insert
        // keeps reporting every conflict as an error. The index is partial, so
        // the inference predicate must repeat it.
        let conflict = if record.webhook_delivery_id.is_some() {
            " ON CONFLICT (webhook_delivery_id, workflow_path) \
              WHERE webhook_delivery_id IS NOT NULL DO NOTHING"
        } else {
            ""
        };
        let inserted = tx
            .execute(
                &format!(
                    "{}{conflict}",
                    concat!(
                        "INSERT INTO runs (run_id, namespace_id, repository, workflow_path, \
                         run_number, run_attempt, run_name, event, ref, ref_type, head_ref, \
                         base_ref, head_sha, workflow_ref, status, webhook_delivery_id, origin, \
                         actor, tree_digest, concurrency_group, concurrency_cancel_in_progress, \
                         fork_approval_pending, fork_approval_requested_at, \
                         fork_approval_approved_at, fork_approval_note, \
                         reports_check_runs, \
                         created_at, started_at) VALUES ($1::text::uuid,$2,$3,$4,$5,$6,$7,$8,$9,\
                         $10,$11,$12,$13,$14,'queued',$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,",
                        ts!("$26"),
                        ",",
                        ts!("$27"),
                        ")"
                    )
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
                    &record.fork_approval_pending,
                    &record.fork_approval_requested_at_unix_nanos,
                    &record.fork_approved_at_unix_nanos,
                    &record.fork_approval_note,
                    &record.reports_check_runs,
                    &now,
                    &record.started_at.map(|at| codec::system_to_us(at.into())),
                ],
            )
            .await
            .map_err(db)?;
        if inserted == 0 {
            // A concurrent redelivery of the same delivery won: its run is
            // the answer for this one too.
            let delivery_id = record
                .webhook_delivery_id
                .as_deref()
                .expect("a conflict is only tolerated for a delivery id");
            let existing = delivery_run_in_tx(&tx, delivery_id, &record.workflow_path_str)
                .await?
                .ok_or_else(|| {
                    ControlError::backend(anyhow::anyhow!(
                        "run insert conflicted with no conflicting delivery"
                    ))
                })?;
            drop(tx);
            drop(client);
            let existing = self.run_record(existing).await?;
            return self.replay_outcome(existing).await;
        }
        // The submission record. Secret values never reach the database
        // (the SecretProvider holds them); clear defensively at the boundary.
        let mut stored_submission = (*record.submission).clone();
        stored_submission.secrets.clear();
        tx.execute(
            "INSERT INTO run_submissions (run_id, submission, github_context, \
             workspace_snapshot, snapshot_timing, record_details) \
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
                // Parity with lite: record-level maps live here for jobs
                // with no `jobs` row. Only `job_check_run_ids` is consumed
                // today (mint-before-materialize); the other detail maps are
                // first-class columns in pg.
                &serde_json::json!({
                    "job_check_run_ids": record.job_check_run_ids
                })
                .to_string(),
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
        emit_outbox(
            &tx,
            Some(record.run_id),
            "run.created.v1",
            serde_json::json!({
                "run_number": record.run_number,
                "repository": record.submission.repository,
                "workflow_path": record.workflow_path_str,
            }),
        )
        .await?;

        // Workless submissions take no workflow admission: a Holder::Run
        // taken here is never released through the completion path.
        let platforms = Self::registered_platforms_on(&tx).await?;
        let pool_labels = if check_hostable {
            self.pool_labels()
        } else {
            Vec::new()
        };
        let runner_labels: Vec<Vec<String>> = if pool_labels.is_empty() {
            Vec::new()
        } else {
            tx.query("SELECT labels::text FROM runners", &[])
                .await
                .map_err(db)?
                .iter()
                .map(|row| {
                    let text: String = row.get(0);
                    codec::from_json::<Vec<String>>(&text).unwrap_or_default()
                })
                .collect()
        };
        let has_runnable = jobs.iter().any(|job| {
            !logic::concludes_at_submit(
                job.initially_skipped,
                !job.queued.needs.is_empty(),
                job.queued.reusable_call.is_some() || job.queued.deferred_matrix.is_some(),
                &job.queued.runs_on,
                check_hostable,
                platforms.iter().copied(),
                &pool_labels,
                runner_labels
                    .iter()
                    .any(|labels| sched_helpers::job_matches_runner(&job.queued.runs_on, labels)),
            )
        });
        let mut held = false;
        if has_runnable && let Some(wf) = &workflow_concurrency {
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
                    emit_outbox(
                        &tx,
                        Some(record.run_id),
                        "run.completed.v1",
                        serde_json::json!({"conclusion": "cancelled"}),
                    )
                    .await?;
                    tx.commit().await.map_err(db)?;
                    drop(client);
                    return Ok(SubmitOutcome {
                        run_id: record.run_id,
                        run_number: record.run_number,
                        queued_jobs: 0,
                        status: ExecutionStatus::Cancelled,
                        concluded: Vec::new(),
                        held: false,
                        rejected: Some(ExecutionStatus::Cancelled),
                        existing: None,
                        next_runs_on: self.ready_front_labels().await?,
                        events: Vec::new(),
                    });
                }
            }
        }

        // Jobs admitted (not concluded at submit) — `RunAccepted.queued_jobs`.
        let mut accepted = 0usize;
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
            // Unhostable platform: conclude immediately. Deferred for
            // needs-gated jobs — they park until their `if:` can be
            // evaluated, and a job GitHub would skip must not fail here on
            // labels it will never need; promotion re-runs this check.
            if check_hostable
                && queued.needs.is_empty()
                && let Some(platform) =
                    crate::runtime_scheduling::unhostable_platform(&node.runs_on, platforms.clone())
            {
                node.status = ExecutionStatus::Failure;
                node.queue_state = logic::QueueState::None;
                node.completed_at_us = Some(now);
                concluded.push((
                    job_id.clone(),
                    ExecutionStatus::Failure,
                    Some(logic::unhostable_reason(platform, &node.runs_on)),
                ));
            }
            if initially_skipped && node.status != ExecutionStatus::Failure {
                node.status = ExecutionStatus::Skipped;
                node.queue_state = logic::QueueState::None;
                node.completed_at_us = Some(now);
                concluded.push((job_id.clone(), ExecutionStatus::Skipped, None));
            }
            // Labels the co-hosted pool can never satisfy and no registered
            // runner serves: conclude now instead of starving in the queue.
            // Placeholders are skipped — expansion materializes their jobs.
            // Needs-gated jobs defer too: they park until their `if:` can be
            // evaluated, and the promotion path re-runs this check.
            if !matches!(
                node.status,
                ExecutionStatus::Failure | ExecutionStatus::Skipped
            ) && queued.reusable_call.is_none()
                && queued.deferred_matrix.is_none()
                && queued.needs.is_empty()
                && let Some(reason) = logic::unschedulable_reason(
                    &node.runs_on,
                    &pool_labels,
                    runner_labels.iter().any(|labels| {
                        crate::runtime_scheduling::job_matches_runner(&node.runs_on, labels)
                    }),
                )
            {
                tracing::warn!(
                    run_id = %record.run_id,
                    job = %job_id.0,
                    labels = ?node.runs_on,
                    pool_labels = ?pool_labels,
                    "runs-on unsatisfiable by runner pool; failing the job at enqueue"
                );
                node.status = ExecutionStatus::Failure;
                node.queue_state = logic::QueueState::None;
                node.completed_at_us = Some(now);
                concluded.push((job_id.clone(), ExecutionStatus::Failure, Some(reason)));
            }
            if !matches!(
                node.status,
                ExecutionStatus::Failure | ExecutionStatus::Skipped
            ) {
                accepted += 1;
            }
            // A node that reaches admission behind a hold parks: the workflow
            // gate is not this run's turn yet (`held`), the run is held by the
            // fork-PR approval policy, or its environment protection gate is
            // not satisfied. All three park identically — held, pending, no
            // promotion candidate — and are released by `promote_ready_jobs`
            // once the hold lifts.
            if (held || record.fork_approval_pending || node.environment_gate.is_some())
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
            // Skipped nodes carry a placeholder message and mint nothing —
            // `job_messages` (like the request row) exists only for
            // dispatchable jobs.
            if !initially_skipped {
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
            }
            graph.record.jobs.insert(job_id.clone(), node.status);
            graph.nodes.insert(job_id, node);
        }

        // `remaining_needs` was initialized as `needs.len()` before submit-time
        // conclusions (unhostable, `if: false`) settled their parents, so it
        // overcounts for dependents of already-terminal needs — the sweep only
        // sees `Blocked` nodes at `remaining_needs <= 0`, and a terminal need
        // will never decrement it again. Recount now that every job's status
        // is final in `record.jobs` (this also covers needs declared later in
        // the job list, which were never counted at insert).
        {
            let jobs_status = &graph.record.jobs;
            let mut retimed = Vec::new();
            for (job_id, node) in graph.nodes.iter_mut() {
                if node.queue_state != logic::QueueState::Blocked {
                    continue;
                }
                let unsettled = node
                    .needs
                    .iter()
                    .filter(|need| {
                        jobs_status
                            .get(*need)
                            .is_none_or(|status| !status.is_terminal())
                    })
                    .count() as i32;
                if unsettled != node.remaining_needs {
                    node.remaining_needs = unsettled;
                    if unsettled == 0 {
                        node.deps_ready_at_us = Some(now);
                    }
                    retimed.push(job_id.clone());
                }
            }
            for job_id in retimed {
                let node = &graph.nodes[&job_id];
                flush_node(&tx, record.run_id, &job_id, node).await?;
            }
        }
        // Submit-time conclusions (unhostable, `if: false`) stay out of the
        // sweep, so `settle_node` never emits `job.completed.v1` for them —
        // split them from the sweep's own conclusions to emit only once.
        let submit_concluded = concluded.len();
        // One promotion sweep: needs-satisfied nodes enqueue (or park behind
        // their own gate / expansion queue); unsatisfiable ones settle.
        if !held {
            let mut sweep = Sweep::new(self, &tx).await?;
            sweep.graphs.insert(record.run_id, graph);
            sweep.sweep().await?;
            for (job_id, status) in &sweep.concluded {
                concluded.push((job_id.clone(), *status, None));
            }
            // `finalize_run_if_complete` parity: even when the sweep settled
            // nothing (every job concluded at submit time), the run's own
            // status must be resummarized and the row flushed.
            let graph = sweep
                .graphs
                .get_mut(&record.run_id)
                .expect("inserted above");
            graph.resummarize();
            graph.touched = true;
            let status = graph.record.status;
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
        for (job_id, status, _) in &concluded[..submit_concluded] {
            emit_outbox(
                &tx,
                Some(record.run_id),
                "job.completed.v1",
                serde_json::json!({"job_id": job_id.0, "status": status_str(*status)}),
            )
            .await?;
        }
        // `settle_node` already emitted `run.completed.v1` for a run the
        // sweep turned terminal; emit only when nothing settled in the
        // sweep (all conclusions were submit-time, or the run was held).
        let sweep_settled = !held && concluded.len() > submit_concluded;
        if final_status.is_terminal() && !sweep_settled {
            // Terminal at submit: the run still owns its workflow-group
            // hold; release the slot and the recorded group so it never
            // admits.
            release_concurrency_for_run(self, &tx, record.run_id).await?;
            tx.execute(
                "UPDATE runs SET concurrency_group = NULL WHERE run_id = $1::text::uuid",
                &[&record.run_id.to_string()],
            )
            .await
            .map_err(db)?;
            emit_outbox(
                &tx,
                Some(record.run_id),
                "run.completed.v1",
                serde_json::json!({"conclusion": status_str(final_status)}),
            )
            .await?;
        }
        // The events the handler used to emit post-commit — `RunAccepted`,
        // concluded `JobStatus`s, a held `RunStatus` — are written inside this
        // transaction so they persist atomically with the run and carry the
        // versions this command just set, not a re-read after other writers.
        // The handler replays them via `emit_persisted`.
        let mut events: Vec<preloop_gha_protocol::NdjsonEvent> = concluded
            .iter()
            .filter(|(job_id, _, _)| job_id.0 != "*")
            .map(
                |(job_id, status, reason)| preloop_gha_protocol::NdjsonEvent::JobStatus {
                    run_id: record.run_id,
                    job_id: job_id.clone(),
                    status: *status,
                    reason: reason.clone(),
                },
            )
            .collect();
        events.push(preloop_gha_protocol::NdjsonEvent::RunAccepted {
            run_id: record.run_id,
            queued_jobs: accepted,
        });
        if held {
            events.push(preloop_gha_protocol::NdjsonEvent::RunStatus {
                run_id: record.run_id,
                status: ExecutionStatus::Pending,
                reason: crate::concurrency::pending_reason(),
            });
        }
        for event in &events {
            append_event_tx(&tx, event).await?;
        }
        tx.commit().await.map_err(db)?;
        drop(client);
        Ok(SubmitOutcome {
            run_id: record.run_id,
            run_number: record.run_number,
            queued_jobs: accepted,
            status: final_status,
            concluded,
            held,
            rejected: None,
            next_runs_on: self.ready_front_labels().await?,
            existing: None,
            events,
        })
    }
}

impl PgBackend {
    /// Platform names (`linux` / `macos` / `windows`) some registered runner
    /// can host — the unhostable-platform check's input.
    pub(super) async fn registered_platforms(&self) -> Result<Vec<&'static str>, ControlError> {
        Self::registered_platforms_on(&*self.reader().await?).await
    }

    /// [`Self::registered_platforms`] on a caller's connection or open transaction.
    pub(super) async fn registered_platforms_on(
        client: &impl GenericClient,
    ) -> Result<Vec<&'static str>, ControlError> {
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
                if let Some(os) = os
                    && !platforms.contains(&os)
                {
                    platforms.push(os);
                }
            }
        }
        Ok(platforms)
    }

    /// `runs-on` labels of the ready-queue front, for `next_job_runs_on`.
    pub(super) async fn ready_front_labels(&self) -> Result<Vec<String>, ControlError> {
        Self::ready_front_labels_on(&*self.reader().await?).await
    }

    /// [`Self::ready_front_labels`] on a caller's connection or open transaction.
    pub(super) async fn ready_front_labels_on(
        client: &impl GenericClient,
    ) -> Result<Vec<String>, ControlError> {
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

    /// The answer to a redelivered webhook: the run the delivery already
    /// produced, with no new work queued.
    async fn replay_outcome(&self, existing: RunRecord) -> Result<SubmitOutcome, ControlError> {
        Ok(SubmitOutcome {
            run_id: existing.run_id,
            run_number: existing.run_number,
            queued_jobs: 0,
            status: existing.status,
            concluded: Vec::new(),
            held: existing.status == ExecutionStatus::Pending,
            rejected: None,
            next_runs_on: self.ready_front_labels().await?,
            existing: Some(Box::new(existing)),
            events: Vec::new(),
        })
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
        client: &impl GenericClient,
        session_id: &str,
    ) -> Result<Option<SessionRef>, ControlError> {
        let uuid = logic::session_uuid(session_id).to_string();
        Ok(client
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
        client: &impl GenericClient,
        session_uuid: &str,
    ) -> Result<Option<SessionMessage>, ControlError> {
        Ok(client
            .query_opt(
                "SELECT m.message_id, m.message_type, m.request_id, m.body::text, \
                 s.runner_id IS NULL \
                 FROM session_messages m \
                 LEFT JOIN runner_sessions s ON s.session_id = m.session_id \
                 WHERE m.session_id = $1::text::uuid \
                 ORDER BY m.message_id LIMIT 1",
                &[&session_uuid],
            )
            .await
            .map_err(db)?
            .map(|row| SessionMessage {
                message_id: row.get(0),
                message_type: row.get(1),
                request_id: row.get(2),
                body: row.get(3),
                // A session without a runner never did the key exchange, so
                // its messages stay plaintext (compat/default sessions).
                plaintext: row.get::<_, Option<bool>>(4).unwrap_or(true),
            }))
    }

    /// The session's live request, when it holds one.
    pub(super) async fn session_active_request(
        &self,
        client: &impl GenericClient,
        session_uuid: &str,
    ) -> Result<Option<TaskAgentJobRequestRecord>, ControlError> {
        let select = format!(
            "{} WHERE q.session_id = $1::text::uuid AND q.result IS NULL \
             ORDER BY q.request_id DESC LIMIT 1",
            lookups::REQUEST_SELECT
        );
        match client
            .query_opt(&select, &[&session_uuid])
            .await
            .map_err(db)?
        {
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
        runner_id: Option<i64>,
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
            // Session-less (compat) runners never exchanged a key.
            plaintext: runner_id.is_none(),
        })
    }

    /// A pending (undelivered) cancellation for the attempt, if any.
    async fn pending_cancellation(
        &self,
        client: &impl GenericClient,
        request_id: i64,
    ) -> Result<Option<u64>, ControlError> {
        Ok(client
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
    /// shared four-tier preference over the current ready batch.
    ///
    /// Statements: a read of the ready batch (`LIMIT 64` in queue order with
    /// assignments and pool-pending rows joined in), then the preferred
    /// candidate's claim. The claim locks the job row with `FOR UPDATE SKIP
    /// LOCKED`: concurrent pollers collapse onto the same head candidate, and
    /// a row another poller is claiming must be skipped at once — a plain
    /// conditional `UPDATE` would queue on that row until the winner's whole
    /// transaction commits, then match nothing. No lock is taken on the rest
    /// of the batch. The batch is paged until a candidate matches or the
    /// ready set is exhausted: a runner whose pool sorts past the first batch
    /// must still see the jobs it can serve.
    async fn claim_one(
        &self,
        tx: &Transaction<'_>,
        runner_id: Option<i64>,
        verified_runner_id: Option<i64>,
        caps: &RunnerCapabilities,
        require_assignments: bool,
    ) -> Result<Option<(RunId, JobId)>, ControlError> {
        let verified = verified_runner_id.is_some();
        let runner_match = logic::RunnerMatchRow {
            labels: caps.labels.clone(),
            known: caps.known,
            group_id: caps.runner_group_id,
            group_name: caps.runner_group_name.clone(),
        };
        // Jobs whose namespace admits no new claim (state or running caps)
        // never become candidates; `capped` marks the ones whose claim must
        // first serialize on the namespace's limit rows.
        let batch_sql = format!(
            "SELECT j.run_id::text, j.job_id, j.runs_on::text, j.runner_group, \
                    a.runner_id, \
                    COALESCE(a.assigned_at > now() - interval '120 seconds', false), \
                    COALESCE(a.first_assigned_at > now() - interval '120 seconds', \
                             false), \
                    COALESCE(a.runner_id IS NOT NULL AND EXISTS( \
                        SELECT 1 FROM runners r WHERE r.runner_id = a.runner_id), \
                        false), \
                    a.run_id IS NOT NULL, \
                    (j.enqueued_at IS NOT NULL \
                     AND j.enqueued_at <= now() - interval '120 seconds'), \
                    (p.requested_at > now() - interval '120 seconds'), \
                    j.namespace_id, j.pool_key, {capped} \
             FROM jobs j \
             LEFT JOIN job_assignments a ON a.run_id = j.run_id \
                AND a.job_id = j.job_id \
             LEFT JOIN provision_requests p ON p.run_id = j.run_id \
                AND p.job_id = j.job_id \
             WHERE j.queue_state = 'ready' AND ({admits}) \
             ORDER BY j.pool_key, j.priority DESC, j.run_order, j.job_order \
             LIMIT 64 OFFSET $1",
            capped = crate::control::types::NAMESPACE_CLAIM_CAPPED,
            admits = crate::control::types::NAMESPACE_ADMITS_CLAIM,
        );
        for _attempt in 0..CLAIM_ATTEMPTS {
            let mut offset: i64 = 0;
            loop {
                // The pool key prunes by label set; the shared eligibility
                // ladder and matcher still decide.
                let rows = tx.query(batch_sql.as_str(), &[&offset]).await.map_err(db)?;
                let exhausted = rows.len() < 64;
                let mut candidates = Vec::with_capacity(rows.len());
                // `(namespace, pool_key, capped)` per candidate, kept index-
                // aligned with `candidates` (both are `swap_remove`d together).
                let mut namespaces: Vec<(String, String, bool)> = Vec::with_capacity(rows.len());
                for (position, row) in rows.iter().enumerate() {
                    let assigned: Option<i64> = row.get(4);
                    let assignment_fresh: bool = row.get(5);
                    let first_assigned_fresh: bool = row.get(6);
                    let registered: bool = row.get(7);
                    let assignment = row.get::<_, bool>(8).then_some((
                        assigned,
                        assignment_fresh,
                        first_assigned_fresh,
                        registered,
                    ));
                    let enqueue_ceiling_expired: bool = row.get(9);
                    // A NULL marks a non-matching LEFT JOIN (no provision
                    // row); `provision_fresh` is tri-state.
                    let provision_fresh: Option<bool> = row.get(10);
                    // `claim_permitted` verbatim (lite/poll.rs): assignment
                    // rows bind verified sessions while fresh, stale/orphaned
                    // bindings open to any verified caller, a fresh
                    // pool-pending row blocks everyone, and unassigned jobs
                    // are only claimable when strict assignments are off.
                    // The old model swept stale bindings before checking in
                    // permissive mode — a stale assignment/pool_pending row
                    // counts as absent there.
                    let assignment = if !require_assignments && !assignment_fresh {
                        None
                    } else {
                        assignment
                    };
                    let provision_fresh = if !require_assignments && provision_fresh == Some(false)
                    {
                        None
                    } else {
                        provision_fresh
                    };
                    let claimable = if assignment.is_some() {
                        if !first_assigned_fresh || enqueue_ceiling_expired {
                            verified
                        } else {
                            match assigned {
                                None => verified,
                                Some(id) => {
                                    if !registered || !assignment_fresh {
                                        verified
                                    } else {
                                        Some(id) == verified_runner_id
                                    }
                                }
                            }
                        }
                    } else if provision_fresh == Some(true) && !enqueue_ceiling_expired {
                        false
                    } else if provision_fresh.is_some() {
                        verified
                    } else {
                        !require_assignments
                    };
                    candidates.push(logic::ClaimCandidate {
                        run_id: codec::run_id(&row.get::<_, String>(0))?,
                        job_id: JobId(row.get::<_, String>(1)),
                        runs_on: codec::from_json(row.get::<_, String>(2).as_str())?,
                        runner_group: row.get(3),
                        assigned_runner_id: assigned,
                        assignment_fresh,
                        queue_position: position as u64,
                        claimable,
                    });
                    namespaces.push((row.get(11), row.get(12), row.get(13)));
                }
                // Walk the candidates in preference order, claiming the first
                // whose row lock lands. A skipped (locked) or already-claimed
                // row advances to the next-eligible candidate.
                let mut raced = false;
                while let Some(index) = logic::claim_preference(
                    &candidates,
                    verified_runner_id,
                    &caps.labels,
                    None,
                    &runner_match,
                ) {
                    let chosen = candidates.swap_remove(index);
                    let (namespace, pool_key, capped) = namespaces.swap_remove(index);
                    if capped
                        && !self
                            .capped_claim_admitted(tx, &namespace, &pool_key, &chosen)
                            .await?
                    {
                        // Another claim took the namespace's last slot after
                        // the batch was read.
                        raced = true;
                        continue;
                    }
                    let claimed = tx
                        .execute(
                            "UPDATE jobs j SET queue_state = 'claimed', status = 'in_progress', \
                             claimed_by_runner_id = $3, claimed_at = now() \
                             FROM (SELECT run_id, job_id FROM jobs \
                                   WHERE run_id = $1::text::uuid AND job_id = $2 \
                                     AND queue_state = 'ready' \
                                   FOR UPDATE SKIP LOCKED) c \
                             WHERE j.run_id = c.run_id AND j.job_id = c.job_id",
                            &[&chosen.run_id.0.to_string(), &chosen.job_id.0, &runner_id],
                        )
                        .await
                        .map_err(db)?;
                    if claimed > 0 {
                        return Ok(Some((chosen.run_id, chosen.job_id)));
                    }
                    raced = true;
                }
                if raced {
                    // A concurrent claim moved rows under us: re-read from the
                    // head of the queue instead of paging past a shifted set.
                    break;
                }
                if exhausted {
                    return Ok(None);
                }
                offset += 64;
            }
        }
        Ok(None)
    }

    /// Re-check a capped namespace's admission under its limit-row locks.
    ///
    /// The batch read decided admission from a snapshot; two nodes could both
    /// see the last free slot. Locking the namespace's `namespace_limits` and
    /// matching `namespace_pool_limits` rows `FOR UPDATE` (held to commit)
    /// serializes claims within the capped namespace, and the re-count runs
    /// as a fresh statement, so it sees every claim committed by the previous
    /// lock holder. Uncapped namespaces never reach this.
    async fn capped_claim_admitted(
        &self,
        tx: &Transaction<'_>,
        namespace: &str,
        pool_key: &str,
        chosen: &logic::ClaimCandidate,
    ) -> Result<bool, ControlError> {
        tx.query(
            "SELECT 1 FROM namespace_limits WHERE namespace_id = $1 FOR UPDATE",
            &[&namespace],
        )
        .await
        .map_err(db)?;
        tx.query(
            "SELECT 1 FROM namespace_pool_limits WHERE namespace_id = $1 AND pool_key = $2 \
             FOR UPDATE",
            &[&namespace, &pool_key],
        )
        .await
        .map_err(db)?;
        let admitted = tx
            .query_opt(
                &format!(
                    "SELECT {} FROM jobs j WHERE j.run_id = $1::text::uuid AND j.job_id = $2",
                    crate::control::types::NAMESPACE_ADMITS_CLAIM
                ),
                &[&chosen.run_id.0.to_string(), &chosen.job_id.0],
            )
            .await
            .map_err(db)?
            .is_some_and(|row| row.get::<_, bool>(0));
        Ok(admitted)
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
                 VALUES ($1,$2,(timestamptz 'epoch' + $3::int8 * interval '1 microsecond'),now()) \
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
        runner_id: Option<i64>,
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
            plaintext: runner_id.is_none(),
        })
    }

    /// A registered runner's capabilities for matching (labels + group).
    async fn runner_capabilities_row(
        &self,
        tx: &Transaction<'_>,
        runner_id: i64,
    ) -> Result<Option<RunnerCapabilities>, ControlError> {
        tx.query_opt(
            "SELECT labels::text, runner_group_id, runner_group_name \
             FROM runners WHERE runner_id = $1",
            &[&runner_id],
        )
        .await
        .map_err(db)?
        .map(|row| -> Result<RunnerCapabilities, ControlError> {
            Ok(RunnerCapabilities {
                known: true,
                labels: codec::from_json(row.get::<_, String>(0).as_str())?,
                runner_group_id: row.get(1),
                runner_group_name: row.get(2),
            })
        })
        .transpose()
    }

    /// `poll_session` probe: the read-only pre-check, run on the reader pool.
    ///
    /// An idle poll — no session message, no active request, no pending
    /// cancellation, no ready work — is the common case for long-polling
    /// runners, and answering it here keeps thousands of them off the writer
    /// pool entirely. Anything that needs a write (delivering a cancellation,
    /// claiming a job) returns `None` and the caller runs the full writer
    /// transaction, which re-checks everything under its locks: a probe hit
    /// that loses a race just comes back `Empty`, exactly as before.
    async fn probe_poll(
        &self,
        client: &impl GenericClient,
        poll: &PollRequest,
    ) -> Result<Option<PollOutcome>, ControlError> {
        // Ownership is revalidated inside the claim transaction too: the
        // handler caches the runner across a long poll, and a liveness sweep
        // can purge the session while it waits.
        let Some(session) = self.session_ref(client, &poll.session_id).await? else {
            return Err(ControlError::Forbidden(
                "session has no runner owner".to_owned(),
            ));
        };
        if poll.verified_runner_id.is_some() && poll.verified_runner_id != session.runner_id {
            return Err(ControlError::Forbidden(
                "session belongs to another runner".to_owned(),
            ));
        }
        if let Some(message) = self
            .oldest_session_message(client, &session.session_uuid)
            .await?
        {
            return Ok(Some(PollOutcome::Inflight(azdo::TaskAgentMessage {
                message_id: message.message_id,
                message_type: message.message_type.clone(),
                body: message.runner_body(),
                iv: None,
            })));
        }
        if let Some(request) = self
            .session_active_request(client, &session.session_uuid)
            .await?
        {
            if self
                .pending_cancellation(client, request.request_id)
                .await?
                .is_some()
            {
                // The writer delivers the cancellation below.
                return Ok(None);
            }
            let runner_id = session.runner_id.unwrap_or(0);
            return Ok(Some(PollOutcome::ActiveRequest { request, runner_id }));
        }
        if poll.busy {
            return Ok(Some(PollOutcome::Empty));
        }
        let ready = client
            .query_opt(
                "SELECT 1 FROM jobs WHERE queue_state = 'ready' LIMIT 1",
                &[],
            )
            .await
            .map_err(db)?
            .is_some();
        if ready {
            // The writer runs the claim below.
            return Ok(None);
        }
        Ok(Some(PollOutcome::Empty))
    }

    /// `poll_session`: redelivery, cancellation, active request, then a claim.
    pub(super) async fn poll_session(
        &self,
        poll: PollRequest,
    ) -> Result<PollOutcome, ControlError> {
        // Fast path first: idle polls never take a writer. The probe reads
        // from this node's own pool — the claim-enabling probe (and every
        // state read that can promote to a write) must NEVER route to a
        // read replica: a replica's snapshot lags the writer's commits, so
        // it could answer "empty" for a claim the writer just made, or
        // claimable for one another node already took. Read your writes.
        let reader = self.reader().await?;
        if let Some(outcome) = self.probe_poll(&*reader, &poll).await? {
            return Ok(outcome);
        }
        drop(reader);
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
                message_type: message.message_type.clone(),
                body: message.runner_body(),
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
                        session.runner_id,
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
        let (_, require_assignments, _) = self.config();
        let Some((run_id, job_id)) = self
            .claim_one(
                &tx,
                session.runner_id,
                session.runner_id,
                &poll.runner,
                require_assignments,
            )
            .await?
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
        // The broker claim is answered inline (`PollOutcome::Claimed`); no
        // `session_messages` row — a lost response re-polls into
        // `ActiveRequest`, not an inflight redelivery. The azdo poll shape
        // inserts its message row because the answer IS the message.
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
        emit_outbox(
            &tx,
            Some(run_id),
            "job.started.v1",
            serde_json::json!({
                "job_id": job_id.0,
                "runner_id": session.runner_id,
                "request_id": request.request_id,
            }),
        )
        .await?;
        let runner_id = session.runner_id.unwrap_or(0);
        tx.commit().await.map_err(db)?;
        drop(client);

        Ok(PollOutcome::Claimed(Box::new(ClaimedJob {
            queued,
            request,
            runner_id,
            next_runs_on: self.ready_front_labels().await?,
        })))
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Completion
// ─────────────────────────────────────────────────────────────────────────

/// What one completion decided, before the caller projects it.
pub(super) struct CompletionApplied {
    pub(crate) effective_status: ExecutionStatus,
    pub(crate) cancelled_siblings: Vec<JobId>,
    pub(crate) newly_terminal_success: bool,
    pub(crate) replayed: bool,
    pub(crate) live_log_key: String,
}

impl<'a> Sweep<'a> {
    /// Apply one terminal completion to a node: outputs, the first-verdict
    /// rule, fail-fast siblings, gate release, request retirement, dependent
    /// promotion and the run's terminal stamp.
    ///
    /// The caller settles the *attempt* (request row) separately — this half
    /// only owns workflow state.
    pub(super) async fn complete_node(
        &mut self,
        run_id: RunId,
        job_id: &JobId,
        reported: ExecutionStatus,
        outputs: &BTreeMap<String, serde_json::Value>,
        annotations: &[serde_json::Value],
        environment_url: Option<&str>,
    ) -> Result<CompletionApplied, ControlError> {
        let was_terminal_success = self
            .graphs
            .get(&run_id)
            .is_some_and(|graph| graph.record.status == ExecutionStatus::Success);
        // A terminal job keeps its first verdict, except that a cancellation
        // reported as success/failure stays cancelled.
        let (prior, base_id, continue_on_error) = {
            let Some(graph) = self.graphs.get(&run_id) else {
                return Err(ControlError::NotFound(format!("run {run_id}")));
            };
            let Some(node) = graph.nodes.get(job_id) else {
                return Err(ControlError::backend(anyhow::anyhow!(
                    "job {job_id} does not belong to run {run_id}"
                )));
            };
            (
                node.status,
                node.base_id.clone(),
                node.continue_on_error.unwrap_or(false),
            )
        };
        let tolerated = continue_on_error && reported == ExecutionStatus::Failure;
        let reported = if tolerated {
            ExecutionStatus::Success
        } else {
            reported
        };
        let replayed = prior.is_terminal() && prior != ExecutionStatus::Cancelled;
        let effective = match (prior, reported) {
            (ExecutionStatus::Cancelled, ExecutionStatus::Success)
            | (ExecutionStatus::Cancelled, ExecutionStatus::Failure) => ExecutionStatus::Cancelled,
            _ if replayed => prior,
            _ => reported,
        };
        if replayed {
            return Ok(CompletionApplied {
                effective_status: effective,
                cancelled_siblings: Vec::new(),
                newly_terminal_success: false,
                replayed: true,
                live_log_key: attempt_log_key(self.tx, run_id, job_id, None).await?,
            });
        }
        // Outputs + annotations land on the node.
        if !outputs.is_empty() {
            let value = serde_json::to_value(outputs).map_err(ControlError::backend)?;
            if let Some(node) = self.node_mut(run_id, job_id) {
                node.outputs = Some(value);
            }
            // The record the caller fold resolves against must see them too:
            // lite reloads its record from the written rows, so the in-memory
            // record has to be kept in step here.
            if let Some(graph) = self.graphs.get_mut(&run_id) {
                graph
                    .record
                    .job_outputs
                    .insert(job_id.clone(), outputs.clone());
            }
        }
        if !annotations.is_empty() {
            let value = serde_json::to_value(annotations).map_err(ControlError::backend)?;
            if let Some(node) = self.node_mut(run_id, job_id) {
                node.annotations = Some(value);
            }
        }
        // The runner evaluated `environment.url` after its steps ran; the
        // value rides the completion and is what the deployment status
        // reports. Written straight to the row: no in-memory node field
        // mirrors it, and no later node flush touches the column.
        if let Some(url) = environment_url.filter(|url| !url.is_empty()) {
            self.tx
                .execute(
                    "UPDATE jobs SET environment_url = $3 \
                     WHERE run_id = $1::text::uuid AND job_id = $2",
                    &[&run_id.0.to_string(), &job_id.0, &url],
                )
                .await
                .map_err(db)?;
        }
        self.mark(run_id, job_id);
        // Retire the node's attempts (settled rows stay readable).
        retire_node_requests(self.tx, run_id, job_id, Retirement::Settle(effective)).await?;
        // Terminal transition + gate release + dependent decrement.
        self.settle_node(run_id, job_id, effective).await?;
        // Fail-fast siblings of a failed matrix leg.
        let mut cancelled_siblings = Vec::new();
        if effective == ExecutionStatus::Failure {
            cancelled_siblings = self.fail_fast_siblings(run_id, job_id, &base_id).await?;
        }
        // Reusable callers fold their callee outputs into terminal statuses.
        let now = self.now;
        let finalized_callers = {
            let graph = self.graphs.get_mut(&run_id).expect("loaded");
            let finalized =
                crate::reusable_workflows::propagate_reusable_outputs(&mut graph.record);
            // The fold lands in the record; project it onto the caller nodes
            // so the flush persists the resolved outputs, the aggregate
            // status and the queue exit (lite writes the same columns).
            for caller_id in &finalized {
                let status = graph
                    .record
                    .jobs
                    .get(caller_id)
                    .copied()
                    .unwrap_or(ExecutionStatus::Failure);
                let outputs = graph
                    .record
                    .job_outputs
                    .get(caller_id)
                    .map(|outputs| serde_json::to_value(outputs).unwrap_or_default());
                if let Some(node) = graph.nodes.get_mut(caller_id) {
                    node.status = status;
                    node.outputs = outputs;
                    node.queue_state = logic::QueueState::None;
                    if node.completed_at_us.is_none() {
                        node.completed_at_us = Some(now);
                    }
                }
            }
            finalized
        };
        for caller_id in &finalized_callers {
            self.dirty.insert((run_id, caller_id.clone()));
            release_concurrency_for_job(self.backend, self.tx, run_id, caller_id).await?;
        }
        // Run-level stamps: completion metadata + first terminal success.
        let newly_terminal_success = {
            let graph = self.graphs.get_mut(&run_id).expect("loaded");
            let record = &mut graph.record;
            if record.started_at.is_none() {
                record.started_at = Some(chrono::Utc::now());
            }
            if record.status.is_terminal() && record.completed_at.is_none() {
                record.completed_at = Some(chrono::Utc::now());
                record.conclusion = Some(crate::runtime_scheduling::status_string(record.status));
            }
            !was_terminal_success && record.status == ExecutionStatus::Success
        };
        // Orphaned in-progress steps of this attempt settle with the job.
        let orphan_conclusion = crate::runtime_scheduling::status_string(effective);
        self.tx
            .execute(
                concat!(
                    "UPDATE job_steps SET conclusion = $3, finished_at = COALESCE(finished_at, ",
                    ts!("$4"),
                    ") WHERE agent_job_id = (SELECT agent_job_id FROM job_requests \
                     WHERE run_id = $1::text::uuid AND job_id = $2 \
                     ORDER BY request_id DESC LIMIT 1) AND conclusion = 'in_progress'"
                ),
                &[
                    &run_id.0.to_string(),
                    &job_id.0,
                    &orphan_conclusion,
                    &now_us(),
                ],
            )
            .await
            .map_err(db)?;
        let live_log_key = attempt_log_key(self.tx, run_id, job_id, None).await?;
        Ok(CompletionApplied {
            effective_status: effective,
            cancelled_siblings,
            newly_terminal_success,
            replayed: false,
            live_log_key,
        })
    }

    /// Cancel the failed leg's matrix siblings (fail-fast), queueing runner
    /// cancellations for the in-flight ones.
    async fn fail_fast_siblings(
        &mut self,
        run_id: RunId,
        failed: &JobId,
        base_id: &str,
    ) -> Result<Vec<JobId>, ControlError> {
        let fail_fast = self
            .graphs
            .get(&run_id)
            .and_then(|graph| graph.nodes.get(failed))
            .and_then(|node| node.fail_fast)
            .unwrap_or(true);
        if !fail_fast {
            return Ok(Vec::new());
        }
        let siblings: Vec<JobId> = {
            let graph = self.graphs.get(&run_id).expect("loaded");
            let rows: Vec<logic::FailFastSibling> = graph
                .nodes
                .iter()
                .map(|(job_id, node)| logic::FailFastSibling {
                    job_id: job_id.clone(),
                    base_id: node.base_id.clone(),
                    status: node.status,
                    agent_job_id: None,
                })
                .collect();
            logic::matrix_fail_fast(failed, Some(base_id), fail_fast, &rows)
        };
        let mut cancelled = Vec::new();
        for sibling in siblings {
            let in_flight = self
                .graphs
                .get(&run_id)
                .and_then(|graph| graph.nodes.get(&sibling))
                .is_some_and(|node| node.status == ExecutionStatus::InProgress);
            if in_flight {
                enqueue_cancellation(
                    self.tx,
                    self.graphs.get(&run_id).expect("loaded"),
                    &sibling,
                    Some("fail_fast"),
                )
                .await?;
            }
            self.settle_node(run_id, &sibling, ExecutionStatus::Cancelled)
                .await?;
            cancelled.push(sibling);
        }
        Ok(cancelled)
    }
}

/// The live-log key of a job's newest attempt (`agent_job_id`, else the
/// logical id for a job that never minted one).
pub(super) async fn attempt_log_key(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    explicit: Option<uuid::Uuid>,
) -> Result<String, ControlError> {
    let agent = match explicit {
        Some(agent) => Some(agent),
        None => agent_job_id(tx, run_id, job_id).await?,
    };
    Ok(agent
        .map(|id| id.to_string())
        .unwrap_or_else(|| format!("{}:{}", run_id.0, job_id.0)))
}

impl PgBackend {
    /// `complete_job`: settle the attempt, flip the job, propagate outputs and
    /// dependents, fail-fast siblings.
    pub(super) async fn complete_job(
        &self,
        completion: JobCompletionInput,
    ) -> Result<CompleteOutcome, ControlError> {
        let run_id = completion.run_id;
        let job_id = completion.job_id.clone();
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        if !PgBackend::lock_run(&tx, run_id).await? {
            return Err(ControlError::NotFound(format!("run {run_id}")));
        }
        // Settle this attempt (first result wins on the request row).
        if let Some(request_id) = self
            .attempt_request_id(&tx, run_id, &job_id, completion.agent_job_id)
            .await?
        {
            settle_request_tx(&tx, request_id, completion.status).await?;
        }
        let mut sweep = Sweep::new(self, &tx).await?;
        let graph = PgBackend::load_graph(self, &tx, run_id)
            .await?
            .ok_or_else(|| ControlError::NotFound(format!("run {run_id}")))?;
        sweep.graphs.insert(run_id, graph);
        let applied = sweep
            .complete_node(
                run_id,
                &job_id,
                completion.status,
                &completion.outputs,
                &[],
                // The legacy `complete_job` shape carries no runner-reported
                // environment URL; the broker settle path does.
                None,
            )
            .await?;
        if !applied.replayed {
            // The claimed marker is gone once the job is terminal.
            sweep
                .tx
                .execute(
                    "UPDATE job_assignments SET runner_id = NULL \
                     WHERE run_id = $1::text::uuid AND job_id = $2",
                    &[&run_id.0.to_string(), &job_id.0],
                )
                .await
                .map_err(db)?;
            sweep.sweep().await?;
        }
        let scheduling = std::mem::take(&mut sweep.outcome);
        let queue_nonempty = Self::work_pending_on(&tx).await?;
        let record = sweep
            .graphs
            .get(&run_id)
            .map(|graph| graph.record.clone())
            .ok_or_else(|| ControlError::NotFound(format!("run {run_id}")))?;
        sweep.flush().await?;
        tx.commit().await.map_err(db)?;
        drop(client);
        Ok(CompleteOutcome {
            record,
            effective_status: applied.effective_status,
            newly_terminal_success: applied.newly_terminal_success,
            cancelled_siblings: applied.cancelled_siblings,
            scheduling,
            live_log_key: applied.live_log_key,
            queue_nonempty,
            replayed: applied.replayed,
        })
    }

    /// `settle_job`: the broker-shaped completion (attempt ownership checked
    /// against the reporting runner) returning the settled projection.
    pub(super) async fn settle_job(
        &self,
        settle: SettleJob,
    ) -> Result<SettleJobOutcome, ControlError> {
        let comp = settle.completion.clone();
        let run_id = comp.run_id;
        let job_id = comp.job_id.clone();
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        if !PgBackend::lock_run(&tx, run_id).await? {
            return Err(ControlError::NotFound("run not found".to_owned()));
        }
        // Ownership: a broker completion must name an attempt this runner owns
        // (recorded owner or the session runner).
        let mut attempt = None;
        if let Some(settle_attempt) = &settle.settle {
            let request_id = self
                .attempt_request_id(&tx, run_id, &job_id, Some(settle_attempt.agent_job_id))
                .await?
                .ok_or_else(|| {
                    ControlError::NotFound("broker complete request not found".to_owned())
                })?;
            let owner = Self::request_owner_on(&tx, request_id).await?;
            // The same ladder `renew_broker_request` uses: an attempt with
            // neither a recorded owner nor a live session is unknown to the
            // broker path (`NotFound`), never free for anyone to settle.
            if let Some((owner, session_runner, has_session)) = owner {
                crate::control::types::ensure_request_owner(
                    owner,
                    session_runner,
                    has_session,
                    settle_attempt.runner_id,
                )?;
            }
            attempt = Some(request_id);
        }
        let mut sweep = Sweep::new(self, &tx).await?;
        let graph = PgBackend::load_graph(self, &tx, run_id)
            .await?
            .ok_or_else(|| ControlError::NotFound("run not found".to_owned()))?;
        sweep.graphs.insert(run_id, graph);
        // The handler masked `comp.annotations` against the provider.
        let annotations = comp.annotations.clone();
        let outputs: BTreeMap<String, serde_json::Value> = comp
            .outputs
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let applied = sweep
            .complete_node(
                run_id,
                &job_id,
                comp.status,
                &outputs,
                &annotations,
                comp.environment_url.as_deref(),
            )
            .await?;
        if applied.replayed {
            let record = sweep
                .graphs
                .get(&run_id)
                .map(|graph| graph.record.clone())
                .expect("loaded");
            tx.commit().await.map_err(db)?;
            return Ok(SettleJobOutcome::Unchanged(Box::new(record)));
        }
        if let Some(request_id) = attempt {
            settle_request_tx(&tx, request_id, applied.effective_status).await?;
            // The attempt's step results land on its manifest.
            for wire in &comp.step_results {
                let Some(external_id) = wire.external_id.as_deref() else {
                    continue;
                };
                if let Some(conclusion) = crate::distributed_task::completion_step_conclusion(wire)
                {
                    let agent = comp.agent_job_id.ok_or_else(|| {
                        ControlError::NotFound("settle_job without agent job id".to_owned())
                    })?;
                    let number = wire.number.and_then(|number| i32::try_from(number).ok());
                    self.patch_step_conclusion(&tx, agent, external_id, &conclusion, number)
                        .await?;
                }
            }
        }
        sweep.sweep().await?;
        let scheduling = std::mem::take(&mut sweep.outcome);
        // Cheap existence check: the handler only needs to know whether to
        // wake pollers, not the exact depth (the supervisor reads that from
        // the 5s sampler snapshot).
        let queue_nonempty = Self::work_pending_on(&tx).await?;
        let next_runs_on = Self::ready_front_labels_on(&tx).await?;
        sweep.flush().await?;
        tx.commit().await.map_err(db)?;
        Ok(SettleJobOutcome::Settled(Box::new(JobSettled {
            effective_status: applied.effective_status,
            cancelled_siblings: applied.cancelled_siblings,
            scheduling,
            queue_nonempty,
            newly_terminal_success: applied.newly_terminal_success,
            live_log_key: applied.live_log_key,
            next_runs_on,
        })))
    }

    /// The live attempt's request id for a `(run, job)`: the explicit agent
    /// job id when given, else the newest in-flight attempt.
    async fn attempt_request_id(
        &self,
        tx: &Transaction<'_>,
        run_id: RunId,
        job_id: &JobId,
        explicit: Option<uuid::Uuid>,
    ) -> Result<Option<i64>, ControlError> {
        let run = run_id.0.to_string();
        let row = match explicit {
            Some(agent) => tx
                .query_opt(
                    "SELECT request_id FROM job_requests WHERE agent_job_id = $1::text::uuid \
                     AND run_id = $2::text::uuid AND job_id = $3 FOR NO KEY UPDATE",
                    &[&agent.to_string(), &run, &job_id.0],
                )
                .await
                .map_err(db)?,
            None => tx
                .query_opt(
                    "SELECT request_id FROM job_requests WHERE run_id = $1::text::uuid \
                     AND job_id = $2 AND result IS NULL ORDER BY request_id DESC LIMIT 1 \
                     FOR NO KEY UPDATE",
                    &[&run, &job_id.0],
                )
                .await
                .map_err(db)?,
        };
        Ok(row.map(|row| row.get::<_, i64>(0)))
    }

    /// Set one step's conclusion on an attempt's manifest.
    async fn patch_step_conclusion(
        &self,
        tx: &Transaction<'_>,
        agent_job_id: uuid::Uuid,
        step_id: &str,
        conclusion: &str,
        runner_number: Option<i32>,
    ) -> Result<(), ControlError> {
        tx.execute(
            concat!(
                "UPDATE job_steps SET conclusion = $3, \
                 runner_number = COALESCE($5::int4, runner_number), \
                 finished_at = COALESCE(finished_at, ",
                ts!("$4"),
                ") WHERE agent_job_id = $1::text::uuid AND step_id = $2"
            ),
            &[
                &agent_job_id.to_string(),
                &step_id,
                &conclusion,
                &now_us(),
                &runner_number,
            ],
        )
        .await
        .map_err(db)?;
        Ok(())
    }

    /// Whether any ready/claimed work or pending cancellation exists.
    pub(super) async fn work_pending(&self) -> Result<bool, ControlError> {
        Self::work_pending_on(&*self.reader().await?).await
    }

    /// [`Self::work_pending`] on a caller's connection or open transaction.
    pub(super) async fn work_pending_on(client: &impl GenericClient) -> Result<bool, ControlError> {
        let pending: bool = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM jobs \
                 WHERE queue_state IN ('ready','claimed','pending_expansion','expanding')) \
                 OR EXISTS (SELECT 1 FROM job_cancellations WHERE delivered_at IS NULL)",
                &[],
            )
            .await
            .map_err(db)?
            .get(0);
        Ok(pending)
    }
}

impl PgBackend {
    /// `poll_azdo_session`: the distributedtask poll shape — redeliver, cancel,
    /// then claim — returning the session message row the handler renders.
    /// An unknown non-UUID session id is the implicit compat session and is
    /// materialized runner-less, like the SQLite backend's.
    ///
    /// Statements: session read + touch; `session_messages` oldest; active
    /// request read; cancellation read/insert; the claim batch and its
    /// conditional update; `job_requests`/`job_leases`/`job_assignments`
    /// binding; the job-message insert.
    pub(super) async fn poll_azdo_session(
        &self,
        poll: AzdoPoll,
    ) -> Result<AzdoPollOutcome, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let session = match self.session_ref(&tx, &poll.session_id).await? {
            Some(session) => session,
            None if poll.session_id.parse::<uuid::Uuid>().is_err() => {
                // The implicit compat session (legacy `sessionId=default`,
                // any non-UUID id) owns no runner: its row is materialized
                // lazily so `session_messages`/`job_requests` keys resolve,
                // and `plaintext` rendering keeps messages decodable without
                // a key exchange.
                let uuid = logic::session_uuid(&poll.session_id).to_string();
                let now = now_us();
                tx.execute(
                    concat!(
                        "INSERT INTO runner_sessions (session_id, runner_id, protocol, \
                         verified, created_at, last_seen_at) \
                         VALUES ($1::text::uuid, NULL, 'azdo', false, ",
                        ts!("$2"),
                        ", ",
                        ts!("$2"),
                        ") ON CONFLICT DO NOTHING"
                    ),
                    &[&uuid, &now],
                )
                .await
                .map_err(db)?;
                SessionRef {
                    session_uuid: uuid,
                    runner_id: None,
                    protocol: "azdo",
                    live: true,
                }
            }
            // An unknown keyed session is answered like a foreign one: the
            // runner must re-register rather than receive work it cannot
            // decode.
            None => return Ok(AzdoPollOutcome::Forbidden),
        };
        if let Some(verified) = poll.verified_runner_id
            && session.runner_id != Some(verified)
        {
            return Ok(AzdoPollOutcome::Forbidden);
        }
        self.touch_session_row(&tx, &session.session_uuid).await?;
        if let Some(message) = self
            .oldest_session_message(&tx, &session.session_uuid)
            .await?
        {
            tx.commit().await.map_err(db)?;
            return Ok(AzdoPollOutcome::Redeliver(message));
        }
        if let Some(request) = self
            .session_active_request(&tx, &session.session_uuid)
            .await?
        {
            if self
                .pending_cancellation(&tx, request.request_id)
                .await?
                .is_some()
            {
                let message = self
                    .queue_cancellation_message(
                        &tx,
                        &session.session_uuid,
                        session.runner_id,
                        request.request_id,
                        request.agent_job_id,
                    )
                    .await?;
                tx.commit().await.map_err(db)?;
                return Ok(AzdoPollOutcome::Cancel(message));
            }
            // Still executing: the runner keeps polling until it finishes.
            tx.commit().await.map_err(db)?;
            return Ok(AzdoPollOutcome::Wait);
        }
        let caps = match session.runner_id {
            Some(runner_id) => match self.runner_capabilities_row(&tx, runner_id).await? {
                Some(caps) => caps,
                None => {
                    tx.commit().await.map_err(db)?;
                    return Ok(AzdoPollOutcome::Wait);
                }
            },
            // A compat session has no registered runner: its labels are
            // unknown, which the shared matcher treats as permissive.
            None => RunnerCapabilities {
                known: false,
                labels: Vec::new(),
                runner_group_id: None,
                runner_group_name: None,
            },
        };
        let (_, require_assignments, _) = self.config();
        let Some((run_id, job_id)) = self
            .claim_one(
                &tx,
                session.runner_id,
                poll.verified_runner_id,
                &caps,
                require_assignments,
            )
            .await?
        else {
            tx.commit().await.map_err(db)?;
            return Ok(AzdoPollOutcome::Wait);
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
            return Ok(AzdoPollOutcome::Wait);
        };
        let message = self
            .queue_job_message(
                &tx,
                &session.session_uuid,
                session.runner_id,
                request.request_id,
            )
            .await?;
        let mut graph = match PgBackend::load_graph(self, &tx, run_id).await? {
            Some(graph) => graph,
            None => {
                tx.commit().await.map_err(db)?;
                return Ok(AzdoPollOutcome::Wait);
            }
        };
        let stamp = now_us();
        if let Some(node) = graph.nodes.get_mut(&job_id) {
            node.claimed_by_runner_id = session.runner_id;
            node.claimed_at_us = Some(stamp);
            node.started_at_us = Some(stamp);
            graph
                .record
                .jobs
                .insert(job_id.clone(), ExecutionStatus::InProgress);
            graph.touched = true;
            flush_node(&tx, run_id, &job_id, node).await?;
        }
        flush_run(&tx, &graph).await?;
        emit_outbox(
            &tx,
            Some(run_id),
            "job.started.v1",
            serde_json::json!({
                "job_id": job_id.0,
                "runner_id": session.runner_id,
                "request_id": request.request_id,
            }),
        )
        .await?;
        tx.commit().await.map_err(db)?;
        Ok(AzdoPollOutcome::Claimed {
            message,
            run_id,
            job_id,
            next_runs_on: self.ready_front_labels().await?,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Expansion
// ─────────────────────────────────────────────────────────────────────────

/// The build inputs a deferred node snapshots under its generation fence.
///
/// Data assembly only: which nodes expand and when is `logic.rs`'s decision
/// (`ExpansionDecision`), and the subtree itself is built by the shared
/// `logic::build_expansion` outside the transaction. Needs outputs reuse the
/// shared `runtime_scheduling::matching_need_ids` fan-out.
fn expansion_plan(graph: &RunGraph, job: &QueuedJob) -> Option<ExpansionPlan> {
    let record = &graph.record;
    let ctx = ExpansionContext {
        run_id: job.run_id,
        submission: graph.submission.clone(),
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
    // A node materialized inside a reusable callee carries its own plan (the
    // expansion pass stores it alongside the node), whose `inputs` are the
    // caller's `with` values the callee subtree was expanded with. A node
    // that was never stored (a top-level node from the initial submit, which
    // only stores reusable callers) falls back to the dispatch inputs the
    // submit path stamped on every plan: the fan-out cells inherit the
    // node's own `inputs` context, not the root dispatch map.
    let stored_plan = record.caller_plans.get(&job.job_id);
    let workflow_file = stored_plan.and_then(|plan| plan.workflow_file.clone());
    let scoped_inputs = stored_plan
        .map(|plan| plan.inputs.clone())
        .unwrap_or_else(|| ctx.submission.dispatch_inputs.clone());
    Some(ExpansionPlan::Matrix(Box::new(MatrixExpansionInputs {
        ctx,
        node_id: job.job_id.clone(),
        base_id: job.base_id.clone(),
        expression,
        needs_outputs,
        workflow_file,
        scoped_inputs,
    })))
}

impl PgBackend {
    /// `claim_expansion`: lease the oldest deferred node for a build.
    ///
    /// The lease bumps `expand_generation`; `apply_expansion` refuses a build
    /// whose generation no longer matches, so a node cancelled or re-leased
    /// while the build ran cannot fold a stale subtree in.
    ///
    /// Statements (one transaction): `SELECT .. FROM jobs WHERE queue_state =
    /// 'pending_expansion' ORDER BY enqueued_at LIMIT 1 FOR UPDATE SKIP
    /// LOCKED`; `UPDATE jobs SET queue_state = 'expanding', expand_generation
    /// = expand_generation + 1 .. RETURNING expand_generation`; the graph load.
    pub(super) async fn claim_expansion(&self) -> Result<Option<ExpansionClaim>, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let Some(row) = tx
            .query_opt(
                "SELECT run_id::text, job_id FROM jobs WHERE queue_state = 'pending_expansion' \
                 ORDER BY enqueued_at NULLS FIRST, run_id, job_id LIMIT 1 \
                 FOR UPDATE SKIP LOCKED",
                &[],
            )
            .await
            .map_err(db)?
        else {
            tx.commit().await.map_err(db)?;
            return Ok(None);
        };
        let run_id = codec::run_id(&row.get::<_, String>(0))?;
        let job_id = codec::job_id(row.get::<_, String>(1));
        let generation: i64 = tx
            .query_one(
                "UPDATE jobs SET queue_state = 'expanding', \
                 expand_generation = expand_generation + 1 \
                 WHERE run_id = $1::text::uuid AND job_id = $2 \
                 AND queue_state = 'pending_expansion' \
                 RETURNING expand_generation::int8",
                &[&run_id.0.to_string(), &job_id.0],
            )
            .await
            .map_err(db)?
            .get(0);
        let graph = match PgBackend::load_graph(self, &tx, run_id).await? {
            Some(graph) => graph,
            None => {
                tx.commit().await.map_err(db)?;
                return Ok(None);
            }
        };
        let queued = self.queued_job(&tx, &graph, &job_id).await?;
        let plan = queued.as_ref().and_then(|job| expansion_plan(&graph, job));
        let job = queued.ok_or_else(|| {
            // Every submitted/expanded node writes a message template; a
            // missing one means the row was lost, not that expansion is
            // optional.
            ControlError::backend(anyhow::anyhow!("expansion node has no message template"))
        })?;
        tx.commit().await.map_err(db)?;
        Ok(Some(ExpansionClaim {
            job,
            generation,
            plan,
        }))
    }

    /// `apply_expansion`: fold a built subtree in under the claim's generation
    /// fence, then promote what it unblocked.
    pub(super) async fn apply_expansion(
        &self,
        claim: ExpansionApply,
    ) -> Result<SchedulingOutcome, ControlError> {
        let run_id = claim.job.run_id;
        let node_id = claim.job.job_id.clone();
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        if !PgBackend::lock_run(&tx, run_id).await? {
            return Err(ControlError::NotFound(format!("run {run_id}")));
        }
        let current: Option<i64> = tx
            .query_opt(
                "SELECT expand_generation::int8 FROM jobs WHERE run_id = $1::text::uuid \
                 AND job_id = $2 FOR UPDATE",
                &[&run_id.0.to_string(), &node_id.0],
            )
            .await
            .map_err(db)?
            .map(|row| row.get(0));
        let Some(current) = current else {
            tx.commit().await.map_err(db)?;
            return Ok(SchedulingOutcome::default());
        };
        if current != claim.generation {
            // Stale lease: the node was cancelled or re-leased while the build
            // ran. Discard the subtree.
            tx.commit().await.map_err(db)?;
            return Ok(SchedulingOutcome::default());
        }
        let mut sweep = Sweep::new(self, &tx).await?;
        let graph = PgBackend::load_graph(self, &tx, run_id)
            .await?
            .ok_or_else(|| ControlError::NotFound(format!("run {run_id}")))?;
        sweep.graphs.insert(run_id, graph);
        let built = match claim.built {
            Ok(built) => built,
            Err(status) => {
                // `settle_node` records the failure in the sweep's outcome;
                // pushing it a second time would count the job twice and
                // re-report its check run.
                sweep.settle_node(run_id, &node_id, status).await?;
                let outcome = std::mem::take(&mut sweep.outcome);
                sweep.flush().await?;
                tx.commit().await.map_err(db)?;
                return Ok(outcome);
            }
        };
        let jobs = match &built {
            BuiltExpansion::Matrix { jobs } | BuiltExpansion::Reusable { jobs, .. } => jobs,
        };
        let job_count = jobs.len();
        match built {
            BuiltExpansion::Matrix { jobs } if jobs.is_empty() => {
                // An empty matrix concludes the node as skipped.
                sweep
                    .settle_node(run_id, &node_id, ExecutionStatus::Skipped)
                    .await?;
                sweep.outcome.skipped.push((run_id, node_id));
            }
            BuiltExpansion::Matrix { jobs } => {
                let registered = self
                    .register_expansion_jobs(&tx, &mut sweep, run_id, jobs)
                    .await?;
                // The parent leaves the run's status map; its legs take over.
                if let Some(node) = sweep.node_mut(run_id, &node_id) {
                    node.has_children = true;
                    node.queue_state = logic::QueueState::None;
                }
                if let Some(graph) = sweep.graphs.get_mut(&run_id) {
                    rebuild_jobs(graph);
                    graph.touched = true;
                }
                sweep.mark(run_id, &node_id);
                retirements_ok(
                    retire_node_requests(&tx, run_id, &node_id, Retirement::Purge).await,
                )?;
                debug_assert_eq!(registered, job_count);
            }
            BuiltExpansion::Reusable {
                caller_id,
                jobs,
                reusable_calls,
            } => {
                let registered = self
                    .register_expansion_jobs(&tx, &mut sweep, run_id, jobs)
                    .await?;
                if let Some(node) = sweep.node_mut(run_id, &caller_id) {
                    node.status = ExecutionStatus::InProgress;
                    node.queue_state = logic::QueueState::None;
                }
                {
                    let graph = sweep.graphs.get_mut(&run_id).expect("loaded");
                    graph.record.reusable_calls.extend(reusable_calls);
                    if graph.record.started_at.is_none() {
                        graph.record.started_at = Some(chrono::Utc::now());
                    }
                    graph
                        .record
                        .jobs
                        .insert(caller_id.clone(), ExecutionStatus::InProgress);
                    graph.touched = true;
                }
                if let Some(graph) = sweep.graphs.get_mut(&run_id) {
                    rebuild_jobs(graph);
                    graph.touched = true;
                }
                sweep.mark(run_id, &caller_id);
                debug_assert_eq!(registered, job_count);
            }
        }
        sweep.sweep().await?;
        let outcome = std::mem::take(&mut sweep.outcome);
        sweep.flush().await?;
        tx.commit().await.map_err(db)?;
        Ok(outcome)
    }

    /// Register one built subtree's jobs: `jobs` + `job_specs` + `job_needs` +
    /// `job_messages` + `job_requests` (+ token request, step manifest).
    /// New nodes start `pending`/`blocked`; the sweep admits them.
    async fn register_expansion_jobs(
        &self,
        tx: &Transaction<'_>,
        sweep: &mut Sweep<'_>,
        run_id: RunId,
        jobs: Vec<BuiltJob>,
    ) -> Result<usize, ControlError> {
        let platforms = Self::registered_platforms_on(tx).await?;
        // Check runs minted before their leg materialized live in
        // `record_details.job_check_run_ids`; seed them onto the new rows
        // (lite expansion parity).
        let stored_check_runs: BTreeMap<String, i64> = tx
            .query_opt(
                "SELECT record_details::text FROM run_submissions \
                 WHERE run_id = $1::text::uuid",
                &[&run_id.0.to_string()],
            )
            .await
            .map_err(db)?
            .and_then(|row| row.get::<_, Option<String>>(0))
            .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
            .and_then(|json| json.get("job_check_run_ids").cloned())
            .and_then(|map| serde_json::from_value::<BTreeMap<String, u64>>(map).ok())
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, v as i64))
            .collect();
        let now = now_us();
        let mut registered = 0usize;
        for built in jobs {
            let BuiltJob {
                plan,
                condition_context,
                mut artifacts,
            } = built;
            let job_id = plan.id.clone();
            let unhostable =
                crate::runtime_scheduling::unhostable_platform(&plan.runs_on, platforms.clone());
            let display_name = plan.name.clone();
            let mut node = Node {
                kind: if plan.reusable_call.is_some() {
                    NodeKind::ReusableCaller
                } else if plan.deferred_matrix.is_some() {
                    NodeKind::MatrixParent
                } else if !plan.matrix.is_empty() && plan.base_id != job_id.0 {
                    NodeKind::MatrixLeg
                } else {
                    NodeKind::Job
                },
                status: ExecutionStatus::Pending,
                queue_state: logic::QueueState::Blocked,
                remaining_needs: plan.needs.len() as i32,
                base_id: plan.base_id.clone(),
                parent_job_id: node_parent(&plan, &job_id),
                pool_key: crate::control::types::compute_pool_key(
                    &plan.runs_on,
                    plan.runner_group.as_deref(),
                ),
                runs_on: plan.runs_on.clone(),
                runner_group: plan.runner_group.clone(),
                priority: 0,
                run_order: 0,
                job_order: 0,
                enqueued_at_us: None,
                claimed_by_runner_id: None,
                claimed_at_us: None,
                expand_generation: 0,
                outputs: None,
                annotations: None,
                check_run_id: None,
                created_at_us: now,
                deps_ready_at_us: plan.needs.is_empty().then_some(now),
                concurrency_wait_at_us: None,
                concurrency_acquired_at_us: None,
                started_at_us: None,
                completed_at_us: None,
                display_name: display_name.clone(),
                display_order: plan.matrix_index.map(|i| i as i32).unwrap_or(0),
                if_condition: plan.if_condition.clone(),
                matrix: plan
                    .matrix
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
                deferred_matrix: plan.deferred_matrix.clone(),
                max_parallel: plan.max_parallel,
                environment: plan.environment.clone(),
                concurrency: concurrency::concurrency_from_plan_fields(
                    plan.concurrency_group.as_deref(),
                    plan.concurrency_cancel_in_progress.as_deref(),
                    plan.concurrency_queue.as_deref(),
                ),
                reusable: plan.reusable_call.clone().map(|call| ReusableNodeSpec {
                    call: Some(call),
                    meta: None,
                    plan: Some(plan.clone()),
                }),
                fail_fast: Some(plan.fail_fast),
                continue_on_error: Some(plan.continue_on_error),
                id_token_granted: artifacts.id_token_granted,
                oidc: artifacts.oidc_ctx.clone(),
                needs: plan.needs.clone(),
                condition_context: condition_context.clone(),
                secret_names: Vec::new(),
                // Freshly materialized legs are armed only at admission.
                environment_gate: None,
                has_children: false,
            };
            node.check_run_id = stored_check_runs.get(&job_id.0).copied();
            if let Some(platform) = unhostable {
                node.status = ExecutionStatus::Failure;
                node.queue_state = logic::QueueState::None;
                node.completed_at_us = Some(now);
                tracing::warn!(
                    run_id = %run_id.0,
                    job = %job_id.0,
                    "materialized callee job is unhostable: no {platform} runner"
                );
            }
            let graph = sweep.graphs.get(&run_id).expect("loaded");
            insert_job_row(tx, graph, &node, &job_id).await?;
            insert_spec_rows(tx, graph, &node, &job_id, None).await?;
            if node.queue_state == logic::QueueState::None {
                // Unhostable: the placeholder request is retired, none minted.
                sweep
                    .settle_node(run_id, &job_id, ExecutionStatus::Failure)
                    .await?;
                continue;
            }
            let (message, secret_names) = {
                let mut message = artifacts.agent_msg.clone();
                let names = strip_secret_values(&mut message);
                (message, names)
            };
            let request_id = insert_request_row(
                tx,
                graph,
                &artifacts.job_request,
                artifacts.github_token_request.as_ref(),
                &crate::models::StepRecord::manifest(&artifacts.agent_msg.steps),
            )
            .await?;
            artifacts.job_request.request_id = request_id;
            let mut message = message;
            message.request_id = request_id;
            tx.execute(
                "INSERT INTO job_messages (run_id, job_id, message_template, secret_names, \
                 condition_context) VALUES ($1::text::uuid,$2,$3::text::jsonb,$4::text::jsonb,\
                 $5::text::jsonb)",
                &[
                    &run_id.0.to_string(),
                    &job_id.0,
                    &json(&message)?,
                    &json(&secret_names)?,
                    &json(&condition_context)?,
                ],
            )
            .await
            .map_err(db)?;
            let graph = sweep.graphs.get_mut(&run_id).expect("loaded");
            graph.nodes.insert(job_id.clone(), node);
            graph.touched = true;
            sweep.mark(run_id, &job_id);
            registered += 1;
        }
        Ok(registered)
    }
}

/// A built job's parent: its matrix parent when it expanded from one.
fn node_parent(plan: &preloop_gha_protocol::JobPlan, job_id: &JobId) -> Option<String> {
    if plan.base_id != job_id.0 && plan.matrix_index.is_some() {
        Some(plan.base_id.clone())
    } else {
        None
    }
}

/// `retire_node_requests` returns the removed log keys; expansion discards
/// them (the live-log close is driven by the caller's post-commit fan-out).
fn retirements_ok(result: Result<Vec<String>, ControlError>) -> Result<(), ControlError> {
    result.map(|_| ())
}

// ─────────────────────────────────────────────────────────────────────────
// Fork-PR approval holds and environment protection gates
//
// A run the fork policy holds, and a job whose `[environment_rules]` gate is
// not satisfied, are parked at submit (`queue_state = 'held'`, status
// `pending`, no concurrency wait of their own). The ordinary promotion sweep
// only ever admits `blocked` nodes, so nothing else moves them:
// [`PgBackend::promote_ready_jobs`] is the one way out.

impl PgBackend {
    /// `promote_ready_jobs`: see the trait contract. `None` sweeps every run
    /// parking a gate-armed job (the reaper's wall-clock sweep).
    pub(crate) async fn promote_ready_jobs(
        &self,
        run: Option<RunId>,
    ) -> Result<PromoteOutcome, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let mut outcome = PromoteOutcome::default();
        for run_id in parked_gate_runs(&tx, run).await? {
            // The per-run mutex, in the schema's ascending-run order: each
            // run is locked and promoted before the next is touched, so a
            // multi-run sweep cannot deadlock with a single-run command.
            if !PgBackend::lock_run(&tx, run_id).await? {
                continue;
            }
            let Some(graph) = PgBackend::load_graph(self, &tx, run_id).await? else {
                continue;
            };
            let mut sweep = Sweep::new(self, &tx).await?;
            sweep.graphs.insert(run_id, graph);
            release_parked_nodes(&tx, &mut sweep, run_id).await?;
            sweep.sweep().await?;
            outcome.promoted += sweep.outcome.promoted;
            outcome.failed += sweep.outcome.failed.len();
            outcome
                .failed_jobs
                .extend(sweep.outcome.failed.iter().map(|(_, job)| job.clone()));
            if let Some(graph) = sweep.graphs.get_mut(&run_id) {
                graph.resummarize();
                graph.touched = true;
            }
            sweep.flush().await?;
        }
        outcome.next_runs_on = Self::ready_front_labels_on(&tx).await?;
        tx.commit().await.map_err(db)?;
        Ok(outcome)
    }

    /// `environment_gate`: one job's stored environment, armed gate and
    /// status. `None` when the run or job does not exist.
    pub(crate) async fn environment_gate(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<EnvironmentGateRead>, ControlError> {
        let client = self.reader().await?;
        let row = client
            .query_opt(
                "SELECT j.status, j.environment_gate::text, s.environment::text \
                 FROM jobs j \
                 LEFT JOIN job_specs s ON s.run_id = j.run_id AND s.job_id = j.job_id \
                 WHERE j.run_id = $1::text::uuid AND j.job_id = $2",
                &[&run_id.0.to_string(), &job_id.0],
            )
            .await
            .map_err(db)?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(EnvironmentGateRead {
            environment: row
                .get::<_, Option<String>>(2)
                .and_then(|json| from_json(&json).ok()),
            gate: row
                .get::<_, Option<String>>(1)
                .and_then(|json| from_json(&json).ok()),
            status: crate::control::types::status_parse(row.get::<_, String>(0).as_str()),
        }))
    }

    /// `environment_deployment`: check run + deployment + resolved
    /// environment for one job, assembled from `jobs`, `runs`, `job_specs`
    /// and `job_messages`. Works on any job state (the terminal deployment
    /// status posts after the row leaves `held`). `None` when the job is
    /// missing or no environment name resolves.
    pub(crate) async fn environment_deployment(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<EnvironmentDeploymentRow>, ControlError> {
        let client = self.reader().await?;
        let row = client
            .query_opt(
                "SELECT r.repository, r.head_sha, j.environment_gate::text, \
                        j.check_run_id, j.deployment_id, j.environment_url, \
                        s.environment::text, m.message_template::text \
                 FROM jobs j \
                 JOIN runs r ON r.run_id = j.run_id \
                 LEFT JOIN job_specs s ON s.run_id = j.run_id AND s.job_id = j.job_id \
                 LEFT JOIN job_messages m ON m.run_id = j.run_id AND m.job_id = j.job_id \
                 WHERE j.run_id = $1::text::uuid AND j.job_id = $2",
                &[&run_id.0.to_string(), &job_id.0],
            )
            .await
            .map_err(db)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let gate: Option<crate::models::EnvironmentGateState> = row
            .get::<_, Option<String>>(2)
            .and_then(|json| from_json(&json).ok());
        // Column order: 5 `jobs.environment_url`, 6 `job_specs.environment`,
        // 7 `job_messages.message_template`.
        let template_env: Option<serde_json::Value> = row
            .get::<_, Option<String>>(7)
            .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
            .and_then(|message| {
                message
                    .get("environment")
                    .or_else(|| message.get("actionsEnvironment"))
                    .cloned()
            });
        let spec_env: Option<serde_json::Value> = row
            .get::<_, Option<String>>(6)
            .and_then(|json| from_json(&json).ok());
        let environment = gate
            .and_then(|gate| gate.environment_name)
            .or_else(|| {
                template_env
                    .as_ref()
                    .and_then(|env| env.get("name"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .or_else(|| {
                crate::runtime_scheduling::environment_gate_name_of(spec_env.as_ref())
                    .map(str::to_owned)
            });
        let Some(environment) = environment else {
            return Ok(None);
        };
        // The runner-evaluated URL — reported with the job's completion and
        // stamped on the job row — wins: it is the only source that can
        // resolve `steps.<id>.outputs`, and what GitHub's own service
        // receives at job completion. Until it lands, only a literal from
        // the message or the spec is known; an unevaluated `${{ }}` template
        // is never posted.
        let environment_url = row
            .get::<_, Option<String>>(5)
            .filter(|url| !url.is_empty())
            .or_else(|| {
                template_env
                    .as_ref()
                    .and_then(sched_helpers::environment_url_literal)
                    .map(str::to_owned)
            })
            .or_else(|| {
                spec_env
                    .as_ref()
                    .and_then(sched_helpers::environment_url_literal)
                    .map(str::to_owned)
            });
        Ok(Some(EnvironmentDeploymentRow {
            repository: row.get(0),
            head_sha: row.get(1),
            environment,
            environment_url,
            check_run_id: row.get::<_, Option<i64>>(3).map(|id| id as u64),
            deployment_id: row.get::<_, Option<i64>>(4).map(|id| id as u64),
        }))
    }
}

/// The runs a pass must visit: the named one, or — for the reaper's
/// wall-clock sweep — every run currently parking a gate-armed job. A run
/// parked only by the fork hold has no clock of its own (its window belongs
/// to the expiry sweep), and the approve path names it explicitly.
async fn parked_gate_runs(
    tx: &Transaction<'_>,
    run: Option<RunId>,
) -> Result<Vec<RunId>, ControlError> {
    if let Some(run_id) = run {
        return Ok(vec![run_id]);
    }
    let rows = tx
        .query(
            "SELECT DISTINCT run_id::text FROM jobs \
             WHERE queue_state = 'held' AND status = 'pending' \
               AND environment_gate IS NOT NULL ORDER BY 1",
            &[],
        )
        .await
        .map_err(db)?;
    rows.iter()
        .map(|row| codec::run_id(&row.get::<_, String>(0)))
        .collect()
}

/// Re-evaluate every node `run_id` parked at submit and release the ones
/// whose hold lifted. A run still awaiting fork approval admits nothing at
/// all — that is the whole point of the hold.
///
/// Gate satisfaction is stamped, never cleared: a released job hands its
/// gate row to the promotion sweep, which re-evaluates it against the same
/// rules — a still-running wait timer re-arms instead of losing its
/// deadline. Only a rule resolving to "no protection" clears the row
/// (inside `evaluate_environment_gate`).
async fn release_parked_nodes(
    tx: &Transaction<'_>,
    sweep: &mut Sweep<'_>,
    run_id: RunId,
) -> Result<(), ControlError> {
    let Some(row) = tx
        .query_opt(
            "SELECT repository, ref, fork_approval_pending FROM runs \
             WHERE run_id = $1::text::uuid",
            &[&run_id.0.to_string()],
        )
        .await
        .map_err(db)?
    else {
        return Ok(());
    };
    let repository: String = row.get(0);
    let git_ref: String = row.get(1);
    let fork_pending: bool = row.get(2);
    if fork_pending {
        return Ok(());
    }
    let parked = tx
        .query(
            "SELECT j.job_id, j.environment_gate::text, s.environment::text \
             FROM jobs j \
             LEFT JOIN job_specs s ON s.run_id = j.run_id AND s.job_id = j.job_id \
             WHERE j.run_id = $1::text::uuid AND j.queue_state = 'held' \
               AND j.status = 'pending' \
               AND NOT EXISTS (SELECT 1 FROM concurrency_waits w \
                     WHERE w.holder_run_id = j.run_id \
                       AND (w.holder_kind = 'run' \
                            OR (w.holder_kind = 'job' AND w.holder_job_id = j.job_id) \
                            OR (w.holder_kind = 'jobset' AND EXISTS ( \
                                SELECT 1 FROM jobsets s2 \
                                WHERE s2.jobset_id = w.holder_jobset_id \
                                  AND s2.job_ids ? j.job_id)))) \
             ORDER BY j.job_order",
            &[&run_id.0.to_string()],
        )
        .await
        .map_err(db)?;
    let now_unix_nanos = crate::models::now_unix_nanos();
    let resolver = sweep.backend.environment_resolver();
    for row in parked {
        let job_id = JobId(row.get::<_, String>(0));
        let mut gate: Option<EnvironmentGateState> = row
            .get::<_, Option<String>>(1)
            .and_then(|json| from_json(&json).ok());
        let environment: Option<serde_json::Value> = row
            .get::<_, Option<String>>(2)
            .and_then(|json| from_json(&json).ok());
        // The resolved name wins: a `Pending` arm records the post-hydration
        // name so the sweep re-evaluates against the environment GitHub
        // knows, not the expression text. A message whose name is still a
        // template (`${{ needs.* }}` before its needs complete) proves nothing
        // beyond what the stamp already records.
        let message_name = sweep
            .message(run_id, &job_id)
            .await?
            .and_then(|message| message.actions_environment)
            .map(|environment| environment.name);
        let env_name = message_name
            .as_deref()
            .and_then(crate::runtime_scheduling::resolved_environment_name_of)
            .map(str::to_owned)
            .or_else(|| {
                gate.as_ref()
                    .and_then(|gate| gate.environment_name.clone())
            })
            .or_else(|| {
                crate::runtime_scheduling::environment_gate_name_of(environment.as_ref())
                    .map(str::to_owned)
            });
        let Some(env_name) = env_name else {
            // No environment on the node: the (now lifted) fork hold is the
            // only thing that parked it, so hand it back to the ordinary
            // promotion path — same as a gate that resolved to "no rules".
            if let Some(node) = sweep.node_mut(run_id, &job_id) {
                node.queue_state = logic::QueueState::Blocked;
                node.status = ExecutionStatus::Queued;
            }
            sweep.mark(run_id, &job_id);
            continue;
        };
        let lookup = resolver.lookup_sync(&repository, &env_name);
        let verdict = crate::runtime_scheduling::evaluate_environment_gate(
            &lookup,
            &git_ref,
            run_id,
            &job_id,
            &env_name,
            &mut gate,
            now_unix_nanos,
        );
        match verdict {
            crate::runtime_scheduling::EnvironmentGateOutcome::Proceed => {
                // The gate is satisfied: keep the stamps (the promotion
                // sweep re-evaluates them — losing them here would re-arm a
                // satisfied wait timer as a fresh gate) and hand the node
                // back to the ordinary promotion path.
                if let Some(node) = sweep.node_mut(run_id, &job_id) {
                    node.environment_gate = gate;
                    node.queue_state = logic::QueueState::Blocked;
                    node.status = ExecutionStatus::Queued;
                }
                sweep.mark(run_id, &job_id);
            }
            crate::runtime_scheduling::EnvironmentGateOutcome::Wait => {
                // Still gated: keep the progress stamps (the wait deadline
                // the timer armed, the approval request time) so the next
                // sweep resumes rather than restarts the gate.
                if let Some(node) = sweep.node_mut(run_id, &job_id) {
                    node.environment_gate = gate;
                }
                sweep.mark(run_id, &job_id);
            }
            crate::runtime_scheduling::EnvironmentGateOutcome::Failed => {
                // Fail closed: the node never dispatches, and its dependents
                // settle through the ordinary sweep.
                if let Some(node) = sweep.node_mut(run_id, &job_id) {
                    node.environment_gate = gate;
                }
                sweep.mark(run_id, &job_id);
                // `settle_node` records the failure in the sweep's outcome,
                // which the caller folds into `PromoteOutcome::failed`.
                sweep
                    .settle_node(run_id, &job_id, ExecutionStatus::Failure)
                    .await?;
            }
        }
    }
    Ok(())
}
