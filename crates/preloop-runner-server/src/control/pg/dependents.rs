//! Promotion of the dependents a settle unblocked, on that job's own rows.
//!
//! `settle_job_targeted` leaves each dependent whose last need just turned
//! terminal `blocked` with no needs outstanding. This module decides each
//! one — its `if:` against the statuses and outputs its needs reach — and
//! carries the verdict out:
//! - skip / error: the dependent settles terminal without running, which may
//!   unblock its own dependents, so those join the next pass of the work
//!   queue (a skip cascade runs in the same transaction);
//! - run: a reusable caller takes its JobSet gates and a deferred matrix goes
//!   straight to the expansion queue; everything else is admitted by
//!   [`promote_one`] (max-parallel cap, needs hydration, environment gate,
//!   concurrency gate, enqueue).
//!
//! Nothing here loads the run: a decision reads the dependent's own node plus
//! the [`NeedsView`] of the jobs its needs reach, and the run's submission only
//! when a caller declares concurrency.

use super::PgBackend;
use super::codec::now_us;
use super::dispatch::{
    JobsetAdvance, advance_jobset, caller_jobset_gates, emit_outbox, ensure_jobset, flush_node,
    set_jobset_ready,
};
use super::graph::Node;
use super::needs::NeedsView;
use super::promote::{PromoteResult, PromoteStep, promote_one, settle_unrun};
use crate::control::logic::QueueState;
use crate::control::types::ControlError;
use crate::runtime_scheduling::{DependencyDecision, SchedulingOutcome, dependency_decision_of};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use std::collections::BTreeSet;
use tokio_postgres::Transaction;

/// The still-blocked, needs-free subset of `ids`, as their nodes. A dependent
/// an earlier step of this transaction already moved (a cancelled sibling, a
/// settled caller) is not returned.
async fn load_dependents(
    tx: &Transaction<'_>,
    run_id: RunId,
    ids: &[JobId],
) -> Result<Vec<(JobId, Node)>, ControlError> {
    let mut dependents = Vec::with_capacity(ids.len());
    for id in ids {
        let Some(node) = PgBackend::load_node(tx, run_id, id).await? else {
            continue;
        };
        if node.queue_state == QueueState::Blocked
            && node.remaining_needs <= 0
            && !node.status.is_terminal()
        {
            dependents.push((id.clone(), node));
        }
    }
    Ok(dependents)
}

impl PgBackend {
    /// Decide and carry out the promotion of every dependent in `unblocked`,
    /// then of whatever their skips and failures unblock in turn.
    ///
    /// A pass handles its dependents in job-id order (the order the shared
    /// sweep walked); dependents unblocked by a pass's own settles form the
    /// next pass. The caller holds the run lock.
    pub(super) async fn promote_dependents(
        &self,
        tx: &Transaction<'_>,
        run_id: RunId,
        unblocked: &[JobId],
        out: &mut SchedulingOutcome,
    ) -> Result<(), ControlError> {
        let mut pass: Vec<JobId> = unblocked.to_vec();
        while !pass.is_empty() {
            let mut next: BTreeSet<JobId> = BTreeSet::new();
            for (job_id, node) in load_dependents(tx, run_id, &pass).await? {
                next.extend(
                    self.promote_dependent(tx, run_id, &job_id, node, out)
                        .await?,
                );
            }
            pass = next.into_iter().collect();
        }
        Ok(())
    }

    /// One dependent's verdict and its effect. Returns the dependents the
    /// effect unblocked (a settle).
    async fn promote_dependent(
        &self,
        tx: &Transaction<'_>,
        run_id: RunId,
        job_id: &JobId,
        mut node: Node,
        out: &mut SchedulingOutcome,
    ) -> Result<Vec<JobId>, ControlError> {
        let view = NeedsView::load(tx, run_id, job_id).await?;
        let decision = dependency_decision_of(
            &view,
            &node.needs,
            node.if_condition.as_deref(),
            &node.condition_context,
        );
        let status = match decision {
            DependencyDecision::Wait => return Ok(Vec::new()),
            DependencyDecision::Skip => ExecutionStatus::Skipped,
            DependencyDecision::Error => ExecutionStatus::Failure,
            DependencyDecision::Run => {
                return self
                    .start_dependent(tx, run_id, job_id, node, &view, out)
                    .await;
            }
        };
        // The instant every need of this job was met.
        node.deps_ready_at_us = Some(now_us());
        settle_unrun(self, tx, run_id, job_id, &mut node, status, out).await
    }

    /// A dependent whose `if:` passed: hand a reusable caller or a deferred
    /// matrix to expansion, admit anything else.
    async fn start_dependent(
        &self,
        tx: &Transaction<'_>,
        run_id: RunId,
        job_id: &JobId,
        mut node: Node,
        view: &NeedsView,
        out: &mut SchedulingOutcome,
    ) -> Result<Vec<JobId>, ControlError> {
        if node
            .reusable
            .as_ref()
            .is_some_and(|spec| spec.call.is_some())
        {
            return self.start_caller(tx, run_id, job_id, node, out).await;
        }
        if node.deferred_matrix.is_some() {
            node.deps_ready_at_us = Some(now_us());
            defer_expansion(tx, run_id, job_id, &mut node).await?;
            return Ok(Vec::new());
        }
        let step = if node.max_parallel.is_some() {
            PromoteStep::MaxParallel
        } else {
            PromoteStep::Hydrate
        };
        Ok(
            match promote_one(self, tx, run_id, job_id, step, Some(view), out).await? {
                PromoteResult::Settled(_, unblocked) => unblocked,
                PromoteResult::Enqueued
                | PromoteResult::Parked
                | PromoteResult::Held
                | PromoteResult::Gone => Vec::new(),
            },
        )
    }

    /// A reusable caller admitted by its `if:`: its caller and embedded
    /// `concurrency:` form one JobSet acquisition; once every gate is held the
    /// caller joins the expansion queue.
    async fn start_caller(
        &self,
        tx: &Transaction<'_>,
        run_id: RunId,
        job_id: &JobId,
        mut node: Node,
        out: &mut SchedulingOutcome,
    ) -> Result<Vec<JobId>, ControlError> {
        node.deps_ready_at_us = Some(now_us());
        let meta = node
            .reusable
            .as_ref()
            .and_then(|spec| spec.meta.as_ref())
            .filter(|meta| {
                meta.caller_concurrency.is_some() || meta.embedded_concurrency.is_some()
            });
        let gates = match meta {
            None => Ok(Vec::new()),
            Some(meta) => {
                let scope = PgBackend::load_run_scope(tx, run_id)
                    .await?
                    .ok_or_else(|| ControlError::NotFound(format!("run {run_id}")))?;
                caller_jobset_gates(meta, &node.matrix, &scope.submission, &scope.github)
            }
        };
        let gates = match gates {
            Ok(gates) => gates,
            Err(status) => {
                return settle_unrun(self, tx, run_id, job_id, &mut node, status, out).await;
            }
        };
        if !gates.is_empty() {
            let jobset_id = ensure_jobset(tx, run_id, job_id, &gates).await?;
            match advance_jobset(self, tx, run_id, jobset_id, None).await? {
                JobsetAdvance::Ready => set_jobset_ready(tx, jobset_id).await?,
                JobsetAdvance::Blocked => {
                    node.status = ExecutionStatus::Pending;
                    node.queue_state = QueueState::Held;
                    node.concurrency_wait_at_us = Some(now_us());
                    flush_node(tx, run_id, job_id, &node).await?;
                    return Ok(Vec::new());
                }
                JobsetAdvance::Failed => {
                    return settle_unrun(
                        self,
                        tx,
                        run_id,
                        job_id,
                        &mut node,
                        ExecutionStatus::Failure,
                        out,
                    )
                    .await;
                }
            }
        }
        defer_expansion(tx, run_id, job_id, &mut node).await?;
        Ok(Vec::new())
    }
}

/// Queue a node for expansion (`pending_expansion`).
async fn defer_expansion(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    node: &mut Node,
) -> Result<(), ControlError> {
    node.queue_state = QueueState::PendingExpansion;
    node.enqueued_at_us = Some(now_us());
    flush_node(tx, run_id, job_id, node).await?;
    emit_outbox(
        tx,
        Some(run_id),
        "expansion.queued.v1",
        serde_json::json!({"job_id": job_id.0}),
    )
    .await
}
