//! Deferred expansion: a reusable caller or a `fromJSON` matrix leaves the
//! promotion path as `pending_expansion`; [`PgBackend::claim_expansion`] leases
//! it and snapshots the build inputs, the subtree is built outside the
//! transaction, and [`PgBackend::apply_expansion`] folds it back in under the
//! lease's generation fence.
//!
//! Neither command loads the run's graph. A claim reads the expanding job's own
//! rows, the [`NeedsView`] its declared needs reach (their statuses and
//! outputs) and the run's planning inputs (submission, snapshot, GitHub
//! context). An apply writes the subtree's rows, then promotes only what the
//! subtree makes startable — its dependency-free jobs, and whatever their and
//! the unhostable jobs' settles unblock — through the same job-scoped
//! [`promote_dependents`](PgBackend::promote_dependents) a completion uses, and
//! stamps the run through `summarize_run_tx`.
//!
//! Data assembly only: which nodes expand and when is `logic.rs`'s decision
//! (`ExpansionDecision`), and the subtree itself is built by the shared
//! `logic::build_expansion`.

use super::codec::{self, json, now_us, us};
use super::dispatch::{
    Retirement, Sweep, insert_job_row, insert_request_row, insert_spec_rows, retire_node_requests,
    strip_secret_values,
};
use super::graph::{self, Node, NodeKind, ReusableNodeSpec};
use super::needs::NeedsView;
use super::promote::{conclude_run, settle_unrun};
use super::{PgBackend, db};
use crate::concurrency;
use crate::control::backend::ExpansionApply;
use crate::control::logic;
use crate::control::logic::{
    BuiltExpansion, BuiltJob, ExpansionContext, ExpansionPlan, MatrixExpansionInputs,
    ReusableExpansionInputs,
};
use crate::control::types::{ControlError, ExpansionClaim};
use crate::models::QueuedJob;
use crate::runtime_scheduling::SchedulingOutcome;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId, WorkflowSubmission};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio_postgres::Transaction;

/// The run-level inputs a build reads, as one targeted read: the run's
/// workflow identity plus its submission payload, GitHub context and
/// workspace snapshot. `None` when the run has no row.
async fn load_expansion_context(
    tx: &Transaction<'_>,
    run_id: RunId,
) -> Result<Option<ExpansionContext>, ControlError> {
    let Some(row) = tx
        .query_opt(
            "SELECT r.workflow_path, r.workflow_ref, r.head_sha, s.submission::text, \
                    s.github_context::text, s.workspace_snapshot::text \
             FROM runs r \
             LEFT JOIN run_submissions s ON s.run_id = r.run_id \
             WHERE r.run_id = $1::text::uuid",
            &[&run_id.0.to_string()],
        )
        .await
        .map_err(db)?
    else {
        return Ok(None);
    };
    let (submission, github_json, snapshot) = match row.get::<_, Option<String>>(3) {
        Some(submission) => (
            serde_json::from_str::<WorkflowSubmission>(&submission)
                .map_err(ControlError::backend)?,
            serde_json::from_str(row.get::<_, String>(4).as_str())
                .map_err(ControlError::backend)?,
            row.get::<_, Option<String>>(5)
                .and_then(|text| serde_json::from_str(&text).ok()),
        ),
        None => (WorkflowSubmission::default(), serde_json::Value::Null, None),
    };
    Ok(Some(ExpansionContext {
        run_id,
        submission: Arc::new(submission),
        snapshot,
        github_json,
        workflow_path: row.get(0),
        workflow_ref: row.get(1),
        head_sha: row.get(2),
    }))
}

/// The build inputs a deferred node snapshots under its generation fence.
///
/// `node` is the expanding job's own row (its stored plan, when it has one);
/// `needs` is the view of the jobs its declared needs reach. Needs outputs are
/// merged per base id exactly as the whole-run record merged them.
fn expansion_plan(
    ctx: ExpansionContext,
    job: &QueuedJob,
    node: &Node,
    needs: &NeedsView,
) -> Option<ExpansionPlan> {
    let needs_outputs = needs.outputs_by_base(&job.needs);
    let stored_plan = node.reusable.as_ref().and_then(|spec| spec.plan.as_ref());
    if let Some(call) = job.reusable_call.clone() {
        let caller_plan = stored_plan?.clone();
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

/// What registering a subtree left to do.
struct Registered {
    /// The registered jobs whose needs are all terminal (dependency-free ones
    /// included), in job-id order: the jobs the subtree can start.
    startable: Vec<JobId>,
    /// Dependents the unhostable jobs' settles left startable.
    unblocked: Vec<JobId>,
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
    /// = expand_generation + 1 .. RETURNING expand_generation`; the node's own
    /// rows, its needs view and the run's planning inputs.
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
        let Some(ctx) = load_expansion_context(&tx, run_id).await? else {
            tx.commit().await.map_err(db)?;
            return Ok(None);
        };
        let node = PgBackend::load_node(&tx, run_id, &job_id).await?;
        let message = PgBackend::node_message(&tx, run_id, &job_id).await?;
        // Every submitted/expanded node writes a message template; a missing
        // one means the row was lost, not that expansion is optional.
        let (Some(node), Some(message)) = (node, message) else {
            return Err(ControlError::backend(anyhow::anyhow!(
                "expansion node has no message template"
            )));
        };
        let job = graph::queued_of(&job_id, run_id, &node, message);
        let needs = NeedsView::load(&tx, run_id, &job_id).await?;
        let plan = expansion_plan(ctx, &job, &node, &needs);
        tx.commit().await.map_err(db)?;
        Ok(Some(ExpansionClaim {
            job,
            generation,
            plan,
        }))
    }

    /// `apply_expansion`: fold a built subtree in under the claim's generation
    /// fence, then promote what it unblocked.
    ///
    /// Statements (one transaction, run locked): the generation check; for a
    /// subtree its rows, the caller's/parent's own row updates and the
    /// startable jobs' admission; for a failed or empty build the node's own
    /// settle and its dependents' promotion; then the run's status.
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
        let mut out = SchedulingOutcome::default();
        match claim.built {
            // `settle_unrun` records the settle in the outcome; pushing it a
            // second time would count the job twice and re-report its check
            // run.
            Err(status) => {
                self.settle_expanding_node(&tx, run_id, &node_id, status, &mut out)
                    .await?;
            }
            // An empty matrix concludes the node as skipped.
            Ok(BuiltExpansion::Matrix { jobs }) if jobs.is_empty() => {
                self.settle_expanding_node(
                    &tx,
                    run_id,
                    &node_id,
                    ExecutionStatus::Skipped,
                    &mut out,
                )
                .await?;
            }
            Ok(BuiltExpansion::Matrix { jobs }) => {
                let leg_ids: Vec<String> = jobs.iter().map(|job| job.plan.id.0.clone()).collect();
                let registered = self
                    .register_expansion_jobs(&tx, run_id, &node_id, jobs, &mut out)
                    .await?;
                splice_matrix_parent(&tx, run_id, &node_id, &leg_ids).await?;
                // The parent leaves the run's status map (its legs have it as
                // `parent_job_id`); its own placeholder request goes.
                tx.execute(
                    "UPDATE jobs SET queue_state = 'none' \
                     WHERE run_id = $1::text::uuid AND job_id = $2",
                    &[&run_id.0.to_string(), &node_id.0],
                )
                .await
                .map_err(db)?;
                retire_node_requests(&tx, run_id, &node_id, Retirement::Purge).await?;
                self.start_registered(&tx, run_id, registered, &mut out)
                    .await?;
            }
            Ok(BuiltExpansion::Reusable {
                caller_id,
                jobs,
                reusable_calls,
            }) => {
                let inner_ids: Vec<String> = jobs.iter().map(|job| job.plan.id.0.clone()).collect();
                let registered = self
                    .register_expansion_jobs(&tx, run_id, &caller_id, jobs, &mut out)
                    .await?;
                write_reusable_meta(&tx, run_id, &caller_id, &inner_ids, &reusable_calls).await?;
                // The caller is now executing inside its subtree.
                tx.execute(
                    "UPDATE jobs SET status = 'in_progress', queue_state = 'none' \
                     WHERE run_id = $1::text::uuid AND job_id = $2",
                    &[&run_id.0.to_string(), &caller_id.0],
                )
                .await
                .map_err(db)?;
                tx.execute(
                    "UPDATE runs SET started_at = COALESCE(started_at, now()) \
                     WHERE run_id = $1::text::uuid",
                    &[&run_id.0.to_string()],
                )
                .await
                .map_err(db)?;
                self.start_registered(&tx, run_id, registered, &mut out)
                    .await?;
            }
        }
        conclude_run(self, &tx, run_id, "success").await?;
        tx.commit().await.map_err(db)?;
        Ok(out)
    }

    /// Settle the node being expanded without a subtree (a failed build, an
    /// empty matrix) and promote the dependents that settle unblocked.
    async fn settle_expanding_node(
        &self,
        tx: &Transaction<'_>,
        run_id: RunId,
        node_id: &JobId,
        status: ExecutionStatus,
        out: &mut SchedulingOutcome,
    ) -> Result<(), ControlError> {
        let Some(mut node) = PgBackend::load_node(tx, run_id, node_id).await? else {
            return Ok(());
        };
        let unblocked = settle_unrun(self, tx, run_id, node_id, &mut node, status, out).await?;
        self.promote_dependents(tx, run_id, &unblocked, out).await
    }

    /// Promote what a registered subtree can start: the jobs whose needs are
    /// all terminal and the dependents the unhostable jobs' settles unblocked,
    /// in job-id order, with the skips and failures that cascade from them.
    async fn start_registered(
        &self,
        tx: &Transaction<'_>,
        run_id: RunId,
        registered: Registered,
        out: &mut SchedulingOutcome,
    ) -> Result<(), ControlError> {
        let mut ids = registered.startable;
        ids.extend(registered.unblocked);
        ids.sort();
        ids.dedup();
        self.promote_dependents(tx, run_id, &ids, out).await
    }

    /// Register one built subtree's jobs: `jobs` + `job_specs` + `job_needs` +
    /// `job_messages` + `job_requests` (+ token request, step manifest).
    /// New nodes start `pending`/`blocked`; their `remaining_needs` is counted
    /// once every row exists, and the unhostable ones settle `failure`.
    async fn register_expansion_jobs(
        &self,
        tx: &Transaction<'_>,
        run_id: RunId,
        placeholder: &JobId,
        jobs: Vec<BuiltJob>,
        out: &mut SchedulingOutcome,
    ) -> Result<Registered, ControlError> {
        let scope = PgBackend::load_run_scope(tx, run_id)
            .await?
            .ok_or_else(|| ControlError::NotFound(format!("run {run_id}")))?;
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
        // Expanded jobs keep their run's place in the queue: the run's
        // `run_order` and the placeholder's `job_order`. A literal 0/0 would
        // sort them ahead of every other run's jobs.
        let (run_order, job_order): (i64, i32) = {
            let row = tx
                .query_one(
                    concat!(
                        "SELECT COALESCE(j.run_order, ",
                        us!("r.created_at"),
                        "), COALESCE(j.job_order, 0) FROM runs r \
                         LEFT JOIN jobs j ON j.run_id = r.run_id AND j.job_id = $2 \
                         WHERE r.run_id = $1::text::uuid"
                    ),
                    &[&run_id.0.to_string(), &placeholder.0],
                )
                .await
                .map_err(db)?;
            (row.get(0), row.get(1))
        };
        let mut registered: Vec<String> = Vec::with_capacity(jobs.len());
        let mut unhostable_nodes: Vec<(JobId, Node)> = Vec::new();
        for built in jobs {
            let BuiltJob {
                plan,
                condition_context,
                artifacts,
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
                run_order,
                job_order,
                enqueued_at_us: None,
                claimed_by_runner_id: None,
                claimed_at_us: None,
                expand_generation: 0,
                outputs: None,
                annotations: None,
                check_run_id: None,
                request_id: None,
                created_at_us: now,
                deps_ready_at_us: plan.needs.is_empty().then_some(now),
                concurrency_wait_at_us: None,
                concurrency_acquired_at_us: None,
                started_at_us: None,
                completed_at_us: None,
                display_name,
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
                // Every node that expands later reads its own plan back: a
                // caller's `call`, a deferred matrix's `workflow_file` and
                // `inputs` (the callee YAML and `with` values it fans out
                // with). Leaf nodes have no reader, so they store nothing.
                reusable: (plan.reusable_call.is_some() || plan.deferred_matrix.is_some()).then(
                    || ReusableNodeSpec {
                        call: plan.reusable_call.clone(),
                        meta: None,
                        plan: Some(plan.clone()),
                    },
                ),
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
            insert_job_row(
                tx,
                run_id,
                &scope.namespace,
                &scope.submission.event,
                &node,
                &job_id,
            )
            .await?;
            insert_spec_rows(tx, run_id, &node, &job_id, None).await?;
            if node.queue_state == logic::QueueState::None {
                // Unhostable: the placeholder request is retired, none minted.
                // It settles once every row of the subtree exists.
                unhostable_nodes.push((job_id, node));
                continue;
            }
            let (mut message, secret_names) = {
                let mut message = artifacts.agent_msg.clone();
                let names = strip_secret_values(&mut message);
                (message, names)
            };
            let request_id = insert_request_row(
                tx,
                &scope.namespace,
                &artifacts.job_request,
                artifacts.github_token_request.as_ref(),
                &crate::models::StepRecord::manifest(&artifacts.agent_msg.steps),
            )
            .await?;
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
            registered.push(job_id.0);
        }
        // A job registered with `needs.len()` outstanding overcounts for needs
        // that are already terminal (a matrix leg inherits its node's `needs`,
        // which settled before it expanded) or that name several legs; recount
        // from the rows now that every job of the subtree exists.
        let startable = Sweep::recount_remaining_needs(tx, run_id, &registered).await?;
        let mut unblocked = Vec::new();
        for (job_id, mut node) in unhostable_nodes {
            unblocked.extend(
                settle_unrun(
                    self,
                    tx,
                    run_id,
                    &job_id,
                    &mut node,
                    ExecutionStatus::Failure,
                    out,
                )
                .await?,
            );
        }
        Ok(Registered {
            startable,
            unblocked,
        })
    }
}

/// A job's reusable spec envelope (`job_specs.reusable_call`), if it has one.
async fn load_reusable_spec(
    tx: &Transaction<'_>,
    run: &str,
    job_id: &str,
) -> Result<Option<ReusableNodeSpec>, ControlError> {
    let json: Option<String> = tx
        .query_opt(
            "SELECT reusable_call::text FROM job_specs \
             WHERE run_id=$1::text::uuid AND job_id=$2",
            &[&run, &job_id],
        )
        .await
        .map_err(db)?
        .and_then(|row| row.get::<_, Option<String>>(0));
    json.map(|json| serde_json::from_str(&json).map_err(ControlError::backend))
        .transpose()
}

async fn store_reusable_spec(
    tx: &Transaction<'_>,
    run: &str,
    job_id: &str,
    spec: &ReusableNodeSpec,
) -> Result<(), ControlError> {
    let json = serde_json::to_string(spec).map_err(ControlError::backend)?;
    tx.execute(
        "UPDATE job_specs SET reusable_call=$3::text::jsonb \
         WHERE run_id=$1::text::uuid AND job_id=$2",
        &[&run, &job_id, &json],
    )
    .await
    .map_err(db)?;
    Ok(())
}

/// The reusable-metadata half of a deferred reusable expansion (lite's
/// `write_reusable_meta`): each callee's metadata lands on its spec row, and the
/// caller's `inner_job_ids` become the subtree just registered, so the fold
/// waits on exactly that subtree.
async fn write_reusable_meta(
    tx: &Transaction<'_>,
    run_id: RunId,
    caller_id: &JobId,
    inner_ids: &[String],
    callee_meta: &BTreeMap<String, preloop_gha_parser::ReusableCallMetadata>,
) -> Result<(), ControlError> {
    let run = run_id.0.to_string();
    for (job_id, meta) in callee_meta {
        let mut spec = load_reusable_spec(tx, &run, job_id)
            .await?
            .unwrap_or_default();
        spec.meta = Some(meta.clone());
        store_reusable_spec(tx, &run, job_id, &spec).await?;
    }
    let Some(mut spec) = load_reusable_spec(tx, &run, &caller_id.0).await? else {
        return Ok(());
    };
    let Some(meta) = spec.meta.as_mut() else {
        return Ok(());
    };
    meta.inner_job_ids = inner_ids.to_vec();
    store_reusable_spec(tx, &run, &caller_id.0, &spec).await
}

/// The matrix-parent half of a deferred matrix expansion (lite's
/// `splice_matrix_parents`): each caller whose recorded subtree names the
/// expanded parent has that id replaced by its legs, so the fold waits for the
/// legs rather than a placeholder that never completes.
async fn splice_matrix_parent(
    tx: &Transaction<'_>,
    run_id: RunId,
    node_id: &JobId,
    leg_ids: &[String],
) -> Result<(), ControlError> {
    let run = run_id.0.to_string();
    let callers = tx
        .query(
            "SELECT job_id FROM job_specs WHERE run_id=$1::text::uuid \
             AND reusable_call #> '{meta,inner_job_ids}' ? $2::text",
            &[&run, &node_id.0],
        )
        .await
        .map_err(db)?;
    for row in callers {
        let caller: String = row.get(0);
        let Some(mut spec) = load_reusable_spec(tx, &run, &caller).await? else {
            continue;
        };
        let Some(meta) = spec.meta.as_mut() else {
            continue;
        };
        let Some(pos) = meta.inner_job_ids.iter().position(|id| id == &node_id.0) else {
            continue;
        };
        meta.inner_job_ids
            .splice(pos..=pos, leg_ids.iter().cloned());
        store_reusable_spec(tx, &run, &caller, &spec).await?;
    }
    Ok(())
}

/// A built job's parent: its matrix parent when it expanded from one.
fn node_parent(plan: &preloop_gha_protocol::JobPlan, job_id: &JobId) -> Option<String> {
    if plan.base_id != job_id.0 && plan.matrix_index.is_some() {
        Some(plan.base_id.clone())
    } else {
        None
    }
}
