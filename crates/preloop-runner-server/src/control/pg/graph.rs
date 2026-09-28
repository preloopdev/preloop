//! The per-run scheduling projection every scheduling command shares.
//!
//! One locked read builds the in-memory run graph a command mutates: the
//! `runs` row (locked `FOR NO KEY UPDATE` — the per-run mutex of the schema's
//! design rules), the `run_submissions` payload, and one [`Node`] per
//! `jobs` row joining `job_specs` / `job_needs` / `job_messages` data that
//! the shared scheduling decisions need. Commands mutate the projection and
//! flush the touched rows back inside the same transaction; nothing in this
//! module writes SQL on its own — it owns *shapes*, [`dispatch`] owns
//! *statements*.
//!
//! Two schema invariants this projection encodes:
//! - `jobs.parent_job_id` cascades, so an expanded node's row is never
//!   deleted — it settles terminal (`queue_state = 'none'`) and keeps its
//!   materialized legs beneath it.
//! - `concurrency_waits.holder_kind = 'run'` marks a workflow-held run;
//!   `jobs.queue_state = 'held'` covers both kinds of gate (decisions-5 B1).

use super::codec::{self, us, us_to_system};
use super::{db, PgBackend};
use crate::control::logic::QueueState;
use crate::control::types::{status_parse, ControlError};
use crate::models::{QueuedJob, RunRecord};
use crate::snapshots::WorkspaceSnapshot;
use preloop_gha_protocol::{azdo, ExecutionStatus, JobId, RunId, WorkflowSubmission};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio_postgres::Transaction;

/// One `jobs` row joined with its immutable spec, decoded for scheduling.
#[derive(Debug, Clone)]
pub(super) struct Node {
    pub(crate) kind: NodeKind,
    pub(crate) status: ExecutionStatus,
    pub(crate) queue_state: QueueState,
    pub(crate) remaining_needs: i32,
    pub(crate) base_id: String,
    pub(crate) parent_job_id: Option<String>,
    pub(crate) pool_key: String,
    pub(crate) runs_on: Vec<String>,
    pub(crate) runner_group: Option<String>,
    pub(crate) priority: i32,
    pub(crate) run_order: i64,
    pub(crate) job_order: i32,
    pub(crate) enqueued_at_us: Option<i64>,
    pub(crate) claimed_by_runner_id: Option<i64>,
    pub(crate) claimed_at_us: Option<i64>,
    pub(crate) expand_generation: i32,
    pub(crate) outputs: Option<serde_json::Value>,
    pub(crate) annotations: Option<serde_json::Value>,
    pub(crate) check_run_id: Option<i64>,
    pub(crate) created_at_us: i64,
    pub(crate) deps_ready_at_us: Option<i64>,
    pub(crate) concurrency_wait_at_us: Option<i64>,
    pub(crate) concurrency_acquired_at_us: Option<i64>,
    pub(crate) started_at_us: Option<i64>,
    pub(crate) completed_at_us: Option<i64>,
    // spec columns
    pub(crate) display_name: String,
    pub(crate) display_order: i32,
    pub(crate) if_condition: Option<String>,
    pub(crate) matrix: BTreeMap<String, serde_json::Value>,
    pub(crate) deferred_matrix: Option<String>,
    pub(crate) max_parallel: Option<u64>,
    pub(crate) environment: Option<serde_json::Value>,
    /// Raw job-level `concurrency:` (server-evaluated at promotion).
    pub(crate) concurrency: Option<preloop_gha_parser::Concurrency>,
    /// Deferred `uses:` plan inputs (caller metadata + JobPlan + call plan).
    pub(crate) reusable: Option<ReusableNodeSpec>,
    pub(crate) fail_fast: Option<bool>,
    pub(crate) continue_on_error: Option<bool>,
    pub(crate) id_token_granted: bool,
    pub(crate) oidc: crate::state::OidcJobContext,
    /// `needs:` in declared order.
    pub(crate) needs: Vec<JobId>,
    /// The `if:`/`needs` expression context (secrets by name only).
    pub(crate) condition_context: preloop_gha_expressions::Context,
    /// Secret names the message template resolves at acquire.
    pub(crate) secret_names: Vec<String>,
    /// Another node names this one as `parent_job_id` (a matrix parent whose
    /// legs exist — it left `record.jobs` when the expansion was applied).
    pub(crate) has_children: bool,
}

/// Everything `job_specs.reusable_call` stores: the deferred `uses:` plan,
/// the caller metadata (`ReusableCallMetadata`), and for deferred matrix
/// nodes the node's `JobPlan` (its `workflow_file` picks the callee YAML).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub(super) struct ReusableNodeSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) call: Option<preloop_gha_protocol::ReusableCallPlan>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) meta: Option<preloop_gha_parser::ReusableCallMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) plan: Option<preloop_gha_protocol::JobPlan>,
}

/// The node's `jobs.kind` classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeKind {
    Job,
    MatrixParent,
    MatrixLeg,
    ReusableCaller,
}

impl NodeKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Job => "job",
            Self::MatrixParent => "matrix_parent",
            Self::MatrixLeg => "matrix_leg",
            Self::ReusableCaller => "reusable_caller",
        }
    }
    fn parse(s: &str) -> Self {
        match s {
            "matrix_parent" => Self::MatrixParent,
            "matrix_leg" => Self::MatrixLeg,
            "reusable_caller" => Self::ReusableCaller,
            _ => Self::Job,
        }
    }
}

impl Node {
    /// Whether this node expands at runtime (deferred matrix or reusable
    /// caller — the old `pending_jobs`-loop expansion predicate).
    pub(crate) fn expandable(&self) -> bool {
        self.deferred_matrix.is_some()
            || self
                .reusable
                .as_ref()
                .is_some_and(|spec| spec.call.is_some())
    }
}

/// The in-memory run graph for one locked run row.
pub(super) struct RunGraph {
    /// The reconstructed record handlers read (`jobs`, `job_outputs`,
    /// `job_base_ids`, `job_needs`, `job_names`, `caller_plans`,
    /// `reusable_calls`, `job_fail_fast`, `job_continue_on_error`,
    /// `job_check_run_ids` are projections of `jobs`/`job_specs`/`job_needs`).
    pub(crate) record: RunRecord,
    pub(crate) namespace: String,
    pub(crate) nodes: BTreeMap<JobId, Node>,
    /// `submission` minus secrets (`run_submissions.submission`).
    pub(crate) submission: Arc<WorkflowSubmission>,
    pub(crate) github: serde_json::Value,
    /// A command changed this run's row or one of its nodes: flush it.
    pub(crate) touched: bool,
}

impl RunGraph {
    /// The working view: job id -> status, in `jobs`-row order.
    pub(crate) fn statuses(&self) -> BTreeMap<JobId, ExecutionStatus> {
        self.nodes
            .iter()
            .map(|(id, node)| (id.clone(), node.status))
            .collect()
    }

    /// Whether any node sits behind a concurrency gate (`held`), which keeps
    /// the run at `Pending` instead of `Queued`.
    pub(crate) fn held(&self) -> bool {
        self.nodes
            .values()
            .any(|node| node.queue_state == QueueState::Held)
    }

    /// Recompute `runs.status`/`conclusion`/`completed_at`/`started_at` from
    /// the nodes, mirroring `summarize_run` + `finalize_run_if_complete`.
    /// A held run stays `Pending` (never `Queued`).
    pub(crate) fn resummarize(&mut self) {
        let held = self.held();
        self.touched = true;
        let record = &mut self.record;
        let jobs = record.jobs.clone();
        let mut status = crate::runtime_scheduling::summarize_run(jobs.values().copied());
        if status == ExecutionStatus::InProgress
            && (held
                || jobs
                    .values()
                    .all(|s| !matches!(s, ExecutionStatus::InProgress)))
        {
            status = ExecutionStatus::Queued;
        }
        record.status = status;
        crate::runtime_scheduling::finalize_run_if_complete(record);
    }
}

impl Node {
    /// Whether the node's `jobs` row contributes to the run status map
    /// (`record.jobs`). Every node does except an expanded matrix parent,
    /// which `apply_expansion` removes from `record.jobs` (its legs take its
    /// place). A reusable caller keeps contributing as `InProgress` while its
    /// callee subtree runs.
    pub(crate) fn contributes(&self) -> bool {
        !(self.kind == NodeKind::MatrixParent && self.has_children)
    }
}

/// `QueuedJob` out of a node plus its message template (which is loaded
/// only for nodes a decision needs it on).
pub(crate) fn queued_of(
    node_id: &JobId,
    run_id: RunId,
    node: &Node,
    message: azdo::AgentJobRequestMessage,
) -> QueuedJob {
    QueuedJob {
        run_id,
        job_id: node_id.clone(),
        base_id: node.base_id.clone(),
        created_at_unix_nanos: node.created_at_us.saturating_mul(1000),
        dependencies_ready_at_unix_nanos: node.deps_ready_at_us.map(|us| us.saturating_mul(1000)),
        concurrency_wait_started_at_unix_nanos: node
            .concurrency_wait_at_us
            .map(|us| us.saturating_mul(1000)),
        concurrency_acquired_at_unix_nanos: node
            .concurrency_acquired_at_us
            .map(|us| us.saturating_mul(1000)),
        enqueued_at_unix_nanos: node.enqueued_at_us.unwrap_or(0).saturating_mul(1000),
        needs: node.needs.clone(),
        if_condition: node.if_condition.clone(),
        condition_context: node.condition_context.clone(),
        max_parallel: node.max_parallel,
        runs_on: node.runs_on.clone(),
        runner_group: node.runner_group.clone(),
        message,
        environment: node.environment.clone(),
        concurrency: node.concurrency.clone(),
        matrix: node.matrix.clone(),
        deferred_matrix: node.deferred_matrix.clone(),
        reusable_call: node.reusable.clone().and_then(|spec| spec.call),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Row decoding
// ─────────────────────────────────────────────────────────────────────────

fn decode_context(text: &str) -> Result<preloop_gha_expressions::Context, ControlError> {
    serde_json::from_str(text).map_err(ControlError::backend)
}

fn decode_matrix(text: &str) -> Result<BTreeMap<String, serde_json::Value>, ControlError> {
    serde_json::from_str::<BTreeMap<String, serde_json::Value>>(text).map_err(ControlError::backend)
}

fn decode_reusable(text: &str) -> Result<ReusableNodeSpec, ControlError> {
    serde_json::from_str(text).map_err(ControlError::backend)
}

fn decode_concurrency(text: &str) -> Result<preloop_gha_parser::Concurrency, ControlError> {
    serde_json::from_str(text).map_err(ControlError::backend)
}

/// `jobs.status` -> `ExecutionStatus` (`timed_out` is `Failure`: the wire
/// enum has no timed-out variant).
fn job_status(text: &str) -> ExecutionStatus {
    if text == "timed_out" {
        ExecutionStatus::Failure
    } else {
        status_parse(text)
    }
}

/// `jobs.queue_state` -> shared [`QueueState`].
fn queue_state(text: &str) -> QueueState {
    match text {
        "ready" => QueueState::Ready,
        "claimed" => QueueState::Claimed,
        "blocked" => QueueState::Blocked,
        "held" => QueueState::Held,
        "pending_expansion" => QueueState::PendingExpansion,
        "expanding" => QueueState::Expanding,
        _ => QueueState::None,
    }
}

/// [`QueueState`] -> the schema's string.
pub(crate) fn queue_state_str(state: QueueState) -> &'static str {
    match state {
        QueueState::None => "none",
        QueueState::Ready => "ready",
        QueueState::Claimed => "claimed",
        QueueState::Blocked => "blocked",
        QueueState::Held => "held",
        QueueState::PendingExpansion => "pending_expansion",
        QueueState::Expanding => "expanding",
    }
}

impl PgBackend {
    /// Lock a run row for this command's writes (`FOR NO KEY UPDATE` — the
    /// per-run mutex: exclusive between commands, but it does not block the
    /// `FOR KEY SHARE` foreign-key checks of child-row inserts). `false` when
    /// the run does not exist (it may live in history; callers that read
    /// archived runs UNION separately).
    pub(super) async fn lock_run(
        tx: &Transaction<'_>,
        run_id: RunId,
    ) -> Result<bool, ControlError> {
        Ok(tx
            .query_opt(
                "SELECT 1 FROM runs WHERE run_id = $1::text::uuid FOR NO KEY UPDATE",
                &[&run_id.0.to_string()],
            )
            .await
            .map_err(db)?
            .is_some())
    }

    /// Load the run row + submission + every node (jobs ⨝ specs ⨝ needs ⨝
    /// message contexts) into a [`RunGraph`]. Assumes the run row is already
    /// locked or the caller only reads.
    pub(super) async fn load_graph(
        &self,
        tx: &Transaction<'_>,
        run_id: RunId,
    ) -> Result<Option<RunGraph>, ControlError> {
        let run = run_id.0.to_string();
        let Some(run_row) = tx
            .query_opt(
                concat!(
                    "SELECT namespace_id, repository, workflow_path, run_number, run_attempt, \
                     run_name, event, ref, ref_type, head_ref, base_ref, head_sha, \
                     workflow_ref, status, conclusion, webhook_delivery_id, \
                     concurrency_group, concurrency_cancel_in_progress, ",
                    us!("created_at"),
                    ", ",
                    us!("started_at"),
                    ", ",
                    us!("completed_at"),
                    " FROM runs WHERE run_id = $1::text::uuid"
                ),
                &[&run],
            )
            .await
            .map_err(db)?
        else {
            return Ok(None);
        };

        let sub_row = tx
            .query_opt(
                "SELECT submission::text, github_context::text, \
                 workspace_snapshot::text, snapshot_timing::text \
                 FROM run_submissions WHERE run_id = $1::text::uuid",
                &[&run],
            )
            .await
            .map_err(db)?;

        let push_state = tx
            .query_opt(
                "SELECT status, error, pr_number, effective_sha \
                 FROM run_push_states WHERE run_id = $1::text::uuid",
                &[&run],
            )
            .await
            .map_err(db)?
            .map(|row| {
                (
                    row.get::<_, String>(0),
                    row.get::<_, Option<String>>(1),
                    row.get::<_, Option<i64>>(2),
                    row.get::<_, Option<String>>(3),
                )
            });

        let mut nodes: BTreeMap<JobId, Node> = BTreeMap::new();
        let job_rows = tx
            .query(
                concat!(
                    "SELECT j.job_id, j.kind, j.parent_job_id, j.base_id, j.status, \
                     j.queue_state, j.remaining_needs, j.pool_key, j.runs_on::text, \
                     j.runner_group, j.priority, j.run_order, j.job_order, \
                     j.outputs::text, j.annotations::text, j.check_run_id, ",
                    us!("j.created_at"),
                    ", ",
                    us!("j.deps_ready_at"),
                    ", ",
                    us!("j.concurrency_wait_at"),
                    ", ",
                    us!("j.concurrency_acquired_at"),
                    ", ",
                    us!("j.started_at"),
                    ", ",
                    us!("j.completed_at"),
                    ", j.claimed_by_runner_id, ",
                    us!("j.claimed_at"),
                    ", ",
                    us!("j.enqueued_at"),
                    ", j.expand_generation, \
                     s.display_name, s.display_order, s.if_condition, s.matrix::text, \
                     s.deferred_matrix, s.max_parallel, s.environment::text, \
                     s.concurrency::text, s.reusable_call::text, s.fail_fast, \
                     s.continue_on_error, s.id_token_granted, s.oidc_environment, \
                     s.oidc_job_workflow_ref, s.oidc_job_workflow_sha, \
                     m.condition_context::text, m.secret_names::text \
                     FROM jobs j \
                     LEFT JOIN job_specs s ON s.run_id = j.run_id AND s.job_id = j.job_id \
                     LEFT JOIN job_messages m ON m.run_id = j.run_id AND m.job_id = j.job_id \
                     WHERE j.run_id = $1::text::uuid ORDER BY j.job_id"
                ),
                &[&run],
            )
            .await
            .map_err(db)?;

        let need_rows = tx
            .query(
                "SELECT job_id, needs_job_id FROM job_needs WHERE run_id = $1::text::uuid \
                 ORDER BY job_id, position",
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

        for row in &job_rows {
            let job_id = JobId(row.get::<_, String>(0));
            let concurrency: Option<preloop_gha_parser::Concurrency> = row
                .get::<_, Option<String>>(33)
                .map(|text| decode_concurrency(&text))
                .transpose()?;
            let reusable: Option<ReusableNodeSpec> = row
                .get::<_, Option<String>>(34)
                .map(|text| decode_reusable(&text))
                .transpose()?;
            let node = Node {
                kind: NodeKind::parse(row.get(1)),
                parent_job_id: row.get(2),
                base_id: row.get(3),
                status: job_status(row.get(4)),
                queue_state: queue_state(row.get(5)),
                remaining_needs: row.get(6),
                pool_key: row.get(7),
                runs_on: row
                    .get::<_, Option<String>>(8)
                    .map(|t| codec::from_json(&t))
                    .transpose()?
                    .unwrap_or_default(),
                runner_group: row.get(9),
                priority: row.get::<_, i32>(10),
                run_order: row.get(11),
                job_order: row.get(12),
                outputs: row
                    .get::<_, Option<String>>(13)
                    .map(|t| serde_json::from_str(&t))
                    .transpose()
                    .map_err(ControlError::backend)?,
                annotations: row
                    .get::<_, Option<String>>(14)
                    .map(|t| serde_json::from_str(&t))
                    .transpose()
                    .map_err(ControlError::backend)?,
                check_run_id: row.get(15),
                created_at_us: row.get::<_, Option<i64>>(16).unwrap_or(0),
                deps_ready_at_us: row.get(17),
                concurrency_wait_at_us: row.get(18),
                concurrency_acquired_at_us: row.get(19),
                started_at_us: row.get(20),
                completed_at_us: row.get(21),
                claimed_by_runner_id: row.get(22),
                claimed_at_us: row.get(23),
                enqueued_at_us: row.get(24),
                expand_generation: row.get(25),
                display_name: row
                    .get::<_, Option<String>>(26)
                    .unwrap_or_else(|| job_id.0.clone()),
                display_order: row.get::<_, Option<i32>>(27).unwrap_or(0),
                if_condition: row.get(28),
                matrix: row
                    .get::<_, Option<String>>(29)
                    .map(|t| decode_matrix(&t))
                    .transpose()?
                    .unwrap_or_default(),
                deferred_matrix: row.get(30),
                max_parallel: row.get::<_, Option<i32>>(31).map(|v| v.max(0) as u64),
                environment: row
                    .get::<_, Option<String>>(32)
                    .map(|t| serde_json::from_str(&t))
                    .transpose()
                    .map_err(ControlError::backend)?,
                concurrency,
                reusable,
                fail_fast: row.get(35),
                continue_on_error: row.get(36),
                id_token_granted: row.get::<_, Option<bool>>(37).unwrap_or(false),
                oidc: crate::state::OidcJobContext {
                    environment: row.get(38),
                    job_workflow_ref: row.get(39),
                    job_workflow_sha: row.get(40),
                },
                needs: needs.get(&job_id).cloned().unwrap_or_default(),
                condition_context: row
                    .get::<_, Option<String>>(41)
                    .map(|t| decode_context(&t))
                    .transpose()?
                    .unwrap_or_default(),
                secret_names: row
                    .get::<_, Option<String>>(42)
                    .map(|t| codec::from_json(&t))
                    .transpose()?
                    .unwrap_or_default(),
                has_children: false,
            };
            nodes.insert(job_id, node);
        }

        let (submission, github, snapshot, snapshot_timing) = match sub_row {
            Some(row) => {
                let submission_json: String = row.get(0);
                let submission: WorkflowSubmission =
                    serde_json::from_str(&submission_json).map_err(ControlError::backend)?;
                let github: serde_json::Value =
                    serde_json::from_str(row.get::<_, String>(1).as_str())
                        .map_err(ControlError::backend)?;
                let snapshot: Option<WorkspaceSnapshot> = row
                    .get::<_, Option<String>>(2)
                    .and_then(|t| serde_json::from_str(&t).ok());
                let timing: Option<crate::models::SnapshotTiming> = row
                    .get::<_, Option<String>>(3)
                    .map(|t| serde_json::from_str(&t))
                    .transpose()
                    .map_err(ControlError::backend)?;
                (Arc::new(submission), github, snapshot, timing)
            }
            None => (
                Arc::new(WorkflowSubmission::default()),
                serde_json::Value::Null,
                None,
                None,
            ),
        };

        // Project the RunRecord fields the schema carries.
        let mut jobs = BTreeMap::new();
        let mut job_outputs = BTreeMap::new();
        let mut job_base_ids = BTreeMap::new();
        let mut job_needs = BTreeMap::new();
        let mut job_names = BTreeMap::new();
        let mut caller_plans = BTreeMap::new();
        let mut reusable_calls = BTreeMap::new();
        let mut job_fail_fast = BTreeMap::new();
        let mut job_continue_on_error = BTreeMap::new();
        let mut job_check_run_ids = BTreeMap::new();
        let mut jobs_list: Vec<crate::models::JobDetail> = Vec::new();
        for (job_id, node) in &nodes {
            if node.contributes() {
                jobs.insert(job_id.clone(), node.status);
            }
            job_base_ids.insert(job_id.clone(), node.base_id.clone());
            if !node.needs.is_empty() {
                job_needs.insert(job_id.clone(), node.needs.clone());
            }
            job_names.insert(job_id.clone(), node.display_name.clone());
            if let Some(outputs) = &node.outputs {
                if let Ok(map) =
                    serde_json::from_value::<BTreeMap<String, serde_json::Value>>(outputs.clone())
                {
                    job_outputs.insert(job_id.clone(), map);
                }
            }
            if let Some(spec) = &node.reusable {
                if let Some(plan) = &spec.plan {
                    caller_plans.insert(job_id.clone(), plan.clone());
                }
                if let Some(meta) = &spec.meta {
                    reusable_calls.insert(job_id.0.clone(), meta.clone());
                }
            }
            if let Some(fail_fast) = node.fail_fast {
                job_fail_fast.insert(node.base_id.clone(), fail_fast);
            }
            if let Some(coe) = node.continue_on_error {
                job_continue_on_error.insert(job_id.0.clone(), coe);
            }
            if let Some(id) = node.check_run_id {
                job_check_run_ids.insert(job_id.clone(), id as u64);
            }
            jobs_list.push(crate::models::JobDetail {
                job_id: job_id.0.clone(),
                name: node.display_name.clone(),
                conclusion: crate::runtime_scheduling::status_string(node.status).to_owned(),
                steps: Vec::new(),
                annotations: node
                    .annotations
                    .as_ref()
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .unwrap_or_default(),
            });
        }

        let record = RunRecord {
            run_id,
            webhook_delivery_id: run_row.get(15),
            run_name: run_row.get(5),
            submission: submission.clone(),
            jobs,
            status: {
                let status: &str = run_row.get(13);
                match status {
                    "completed" => run_row
                        .get::<_, Option<&str>>(14)
                        .map(|c| match c {
                            "cancelled" => ExecutionStatus::Cancelled,
                            "skipped" => ExecutionStatus::Skipped,
                            "timed_out" | "failure" => ExecutionStatus::Failure,
                            _ => ExecutionStatus::Success,
                        })
                        .unwrap_or(ExecutionStatus::Success),
                    "in_progress" => ExecutionStatus::InProgress,
                    // `queued` covers both queued and held; a `run` wait row
                    // means the old model's `Pending`.
                    _ => ExecutionStatus::Queued,
                }
            },
            job_outputs,
            job_base_ids,
            job_needs,
            caller_plans,
            job_names,
            github: github.clone(),
            head_sha: run_row.get(11),
            workflow_ref: run_row.get(12),
            workspace_snapshot: snapshot,
            job_fail_fast,
            job_continue_on_error,
            job_check_run_ids,
            reusable_calls,
            jobs_list,
            created_at: us_to_system(run_row.get::<_, Option<i64>>(18).unwrap_or(0)).into(),
            started_at: run_row
                .get::<_, Option<i64>>(19)
                .map(|us| us_to_system(us).into()),
            completed_at: run_row
                .get::<_, Option<i64>>(20)
                .map(|us| us_to_system(us).into()),
            run_number: run_row.get::<_, i64>(3).max(0) as u64,
            run_attempt: run_row.get::<_, i32>(4).max(0) as u64,
            workflow_path_str: run_row.get(2),
            event: run_row.get(6),
            conclusion: run_row.get(14),
            push_state: push_state.map(|(status, error, pr_number, effective_sha)| {
                crate::models::PushState {
                    status: match status.as_str() {
                        "synced" => crate::models::PushStatus::Synced,
                        "blocked" => crate::models::PushStatus::Blocked,
                        _ => crate::models::PushStatus::Pending,
                    },
                    error,
                    pr_number: pr_number.map(|n| n as u64),
                    effective_sha,
                }
            }),
            snapshot_timing,
        };

        // A node whose `parent_job_id` names another node marks the parent as
        // expanded (matrix parents leave `record.jobs`, reusable callers stay).
        let parents: BTreeSet<String> = nodes
            .values()
            .filter_map(|node| node.parent_job_id.clone())
            .collect();
        for (job_id, node) in nodes.iter_mut() {
            node.has_children = parents.contains(&job_id.0);
        }

        Ok(Some(RunGraph {
            record,
            namespace: run_row.get(0),
            nodes,
            submission,
            github,
            touched: false,
        }))
    }

    /// A node's runner-facing message template, decoded. `None` when the
    /// node has no `job_messages` row (expandable caller placeholders mint
    /// no message).
    pub(super) async fn node_message(
        tx: &Transaction<'_>,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<azdo::AgentJobRequestMessage>, ControlError> {
        let row = tx
            .query_opt(
                "SELECT message_template::text FROM job_messages \
                 WHERE run_id = $1::text::uuid AND job_id = $2",
                &[&run_id.0.to_string(), &job_id.0],
            )
            .await
            .map_err(db)?;
        row.map(|row| serde_json::from_str(row.get::<_, String>(0).as_str()))
            .transpose()
            .map_err(ControlError::backend)
    }

    /// A node's message template replaced (post-`hydrate_needs_context`).
    pub(super) async fn write_node_message(
        tx: &Transaction<'_>,
        run_id: RunId,
        job_id: &JobId,
        message: &azdo::AgentJobRequestMessage,
    ) -> Result<(), ControlError> {
        let json = serde_json::to_string(message).map_err(ControlError::backend)?;
        tx.execute(
            "UPDATE job_messages SET message_template = $3::text::jsonb \
             WHERE run_id = $1::text::uuid AND job_id = $2",
            &[&run_id.0.to_string(), &job_id.0, &json],
        )
        .await
        .map_err(db)?;
        Ok(())
    }

    /// Rebuild `QueuedJob` for one node (template fetch included). `None`
    /// when the node has no message (a scheduling-only caller).
    pub(super) async fn queued_job(
        &self,
        tx: &Transaction<'_>,
        graph: &RunGraph,
        job_id: &JobId,
    ) -> Result<Option<QueuedJob>, ControlError> {
        let Some(node) = graph.nodes.get(job_id) else {
            return Ok(None);
        };
        let Some(message) = Self::node_message(tx, graph.record.run_id, job_id).await? else {
            return Ok(None);
        };
        Ok(Some(queued_of(job_id, graph.record.run_id, node, message)))
    }
}
