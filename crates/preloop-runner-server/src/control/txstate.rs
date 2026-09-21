//! Transaction-scoped working set for control commands.
//!
//! `TxState` mirrors the scheduling fields of the old `InnerState` so the
//! scheduling state machine runs unchanged inside a database transaction.
//! A backend loads the rows a command needs, runs the shared logic, then
//! writes back the delta — all inside one transaction, so the database (not
//! a process-local mutex) is what serializes and fences mutations.
//!
//! Node-local state never enters `TxState`: live-log buffers, DAP ports and
//! debug registries are reached through [`TxSideEffects`], which the caller
//! applies to `InnerState` after the transaction commits.

use super::*;
use crate::concurrency;
use crate::models::{
    GitHubTokenRequest, QueuedCancellation, QueuedJob, RunRecord, TaskAgentJobRequestRecord,
};
use crate::state::{JobSetAdmission, JobSetId, OidcJobContext};
use preloop_gha_protocol::azdo;
use preloop_gha_protocol::crypto::{AgentRsaPublicKey, SessionEncryption};
use preloop_gha_protocol::{JobId, RegisteredRunner, RunId, RunnerSession};
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};

/// The scheduling working set one command operates on.
///
/// Field names deliberately match the old `InnerState` scheduling subset —
/// the ported logic reads identically, and every field here is either
/// loaded from / written back to the database inside the command's
/// transaction, or seeded from configuration at load time.
#[derive(Default)]
pub(crate) struct TxState {
    // ── Runs and queue collections ────────────────────────────────────
    pub(crate) runs: BTreeMap<RunId, RunRecord>,
    pub(crate) workflow_run_counters: BTreeMap<String, u64>,
    /// Ready-queue jobs *in this working set*. The global queue is the
    /// `jobs` table (`queue_kind='ready'` ordered by `queue_position`); this
    /// deque holds the candidates a poll inspected plus jobs this command
    /// newly enqueued, in order.
    pub(crate) queue: VecDeque<QueuedJob>,
    /// The global ready queue: every `jobs` row with `queue_kind='ready'`,
    /// in `queue_position` order. Commands that scan the queue (claim
    /// selection, capability matching, stuck-on-external-hosts) read this;
    /// `queue` holds only jobs this transaction newly enqueued.
    pub(crate) ready_index: VecDeque<QueuedJob>,
    /// Global ready-queue size at load time, adjusted as this transaction
    /// pushes/pops — reported back for the supervisor atomic.
    pub(crate) ready_count: i64,
    /// Next free `queue_position` for newly enqueued jobs.
    pub(crate) next_queue_position: i64,
    /// Reaper bookkeeping: when each ready job was first observed queued.
    pub(crate) queued_at: BTreeMap<(RunId, JobId), std::time::SystemTime>,
    pub(crate) pending_jobs: VecDeque<QueuedJob>,
    pub(crate) pending_expansions: VecDeque<QueuedJob>,
    /// Expansion reservations held by this working set.
    pub(crate) expanding: BTreeSet<(RunId, JobId)>,
    /// Payloads of claimed-but-unapplied expansion nodes, kept at load so a
    /// crash recovery can push them back onto `pending_expansions` instead
    /// of losing the node (the row's `payload_blob` survives write-back).
    pub(crate) expanding_jobs: BTreeMap<(RunId, JobId), QueuedJob>,
    /// Generation this transaction claimed each expanding node under.
    pub(crate) expand_generations: BTreeMap<(RunId, JobId), i64>,
    pub(crate) concurrency_blocked: VecDeque<QueuedJob>,
    pub(crate) held_runs: BTreeMap<RunId, Vec<QueuedJob>>,
    pub(crate) claimed_jobs: BTreeMap<(RunId, JobId), QueuedJob>,

    // ── Runners and sessions ──────────────────────────────────────────
    pub(crate) runners: BTreeMap<i64, RegisteredRunner>,
    pub(crate) runner_registered_at: BTreeMap<i64, std::time::SystemTime>,
    pub(crate) runner_rsa_public_keys: BTreeMap<i64, AgentRsaPublicKey>,
    pub(crate) runner_client_ids: BTreeMap<String, i64>,
    pub(crate) pool_proven_runners: BTreeSet<i64>,
    /// AzDO-protocol sessions (`sessions` rows).
    pub(crate) sessions: BTreeMap<String, RunnerSession>,
    /// Broker-protocol session → runner.
    pub(crate) broker_session_runners: BTreeMap<String, i64>,
    /// Session crypto material, sealed into `runner_sessions` on write-back.
    pub(crate) session_keys: BTreeMap<String, SessionEncryption>,
    pub(crate) session_last_seen: BTreeMap<String, std::time::SystemTime>,
    pub(crate) session_active_requests: BTreeMap<String, i64>,
    pub(crate) azdo_sessions: HashSet<String>,
    /// Undelivered session messages (broker_messages table).
    pub(crate) inflight_messages: BTreeMap<String, BTreeMap<i64, azdo::TaskAgentMessage>>,

    // ── Requests, messages, grants ────────────────────────────────────
    pub(crate) job_requests: BTreeMap<i64, TaskAgentJobRequestRecord>,
    /// request_id → undelivered job message (job_request_messages table).
    pub(crate) broker_messages: BTreeMap<i64, azdo::AgentJobRequestMessage>,
    pub(crate) github_token_requests: BTreeMap<i64, GitHubTokenRequest>,
    pub(crate) id_token_grants: BTreeMap<(RunId, JobId), bool>,
    pub(crate) oidc_job_contexts: BTreeMap<(RunId, JobId), OidcJobContext>,
    /// Derived lookup maps — rebuilt on load, never persisted directly.
    pub(crate) inflight_requests: BTreeMap<i64, (RunId, JobId)>,
    pub(crate) plan_requests: BTreeMap<String, i64>,
    pub(crate) agent_job_requests: BTreeMap<uuid::Uuid, i64>,
    pub(crate) timeline_requests: BTreeMap<uuid::Uuid, i64>,
    pub(crate) next_message_id: i64,
    pub(crate) next_runner_id: i64,

    // ── Steps ─────────────────────────────────────────────────────────
    pub(crate) job_steps: BTreeMap<uuid::Uuid, Vec<crate::models::StepRecord>>,
    pub(crate) job_steps_revision: BTreeMap<uuid::Uuid, u64>,

    // ── Concurrency and jobsets ───────────────────────────────────────
    pub(crate) concurrency_groups: BTreeMap<(String, String), concurrency::ConcurrencyGroup>,
    pub(crate) holder_keys: BTreeMap<RunId, Vec<(String, String)>>,
    pub(crate) jobset_admissions: BTreeMap<JobSetId, JobSetAdmission>,
    pub(crate) jobset_ready: BTreeSet<JobSetId>,
    pub(crate) run_concurrency: BTreeMap<RunId, preloop_gha_parser::Concurrency>,

    // ── Assignments and cancellations ─────────────────────────────────
    pub(crate) job_assignments: BTreeMap<(RunId, JobId), crate::models::AssignmentRecord>,
    pub(crate) pool_pending: BTreeMap<(RunId, JobId), std::time::SystemTime>,
    pub(crate) cancellation_queue: VecDeque<QueuedCancellation>,

    // ── Configuration seeded at load (not persisted) ──────────────────
    pub(crate) pool_assignments_enabled: bool,
    pub(crate) require_job_assignments: bool,
    pub(crate) runner_liveness_timeout: std::time::Duration,
    /// Observable count of stale bindings released this transaction.
    pub(crate) released_bindings_count: u64,

    // ── Write-back bookkeeping ────────────────────────────────────────
    /// Identity of every row loaded into this working set, per family.
    /// Write-back upserts present rows and deletes loaded-but-removed rows.
    pub(crate) loaded: LoadedRows,
    /// Node-local side effects to apply after commit.
    pub(crate) side: TxSideEffects,
}

/// Rows loaded into a [`TxState`], used to compute deletions at write-back:
/// a key present at load but absent from the working set afterwards was
/// removed by the command and must be deleted in the same transaction.
#[derive(Default)]
pub(crate) struct LoadedRows {
    pub(crate) runs: BTreeSet<RunId>,
    /// (run_id, job_id) → queue kind the row held at load.
    pub(crate) jobs: BTreeMap<(RunId, JobId), QueueKind>,
    pub(crate) requests: BTreeSet<i64>,
    pub(crate) broker_messages: BTreeSet<i64>,
    pub(crate) sessions: BTreeSet<String>,
    pub(crate) session_active_requests: BTreeSet<String>,
    pub(crate) inflight_sessions: BTreeSet<String>,
    pub(crate) runners: BTreeSet<i64>,
    pub(crate) groups: BTreeSet<(String, String)>,
    pub(crate) jobsets: BTreeSet<JobSetId>,
    pub(crate) assignments: BTreeSet<(RunId, JobId)>,
    pub(crate) cancellations: BTreeSet<(RunId, JobId, uuid::Uuid)>,
    pub(crate) step_attempts: BTreeSet<uuid::Uuid>,
    pub(crate) holder_key_runs: BTreeSet<RunId>,
    /// Counters loaded (name → value at load).
    pub(crate) counters: BTreeMap<String, i64>,
}

/// Mutations of node-local state the command recorded; the caller applies
/// them to `InnerState` only after the transaction commits, so a rolled-back
/// command never leaves a live-log feed closed or a DAP port forgotten.
#[derive(Default)]
pub(crate) struct TxSideEffects {
    /// Live-log keys to drop entirely (attempt purge).
    pub(crate) live_log_removals: Vec<String>,
    /// Live-log keys to close (keep snapshot, end the feed).
    pub(crate) live_log_closes: Vec<String>,
    /// Runs whose DAP port registration is gone.
    pub(crate) dap_removals: Vec<RunId>,
}

impl TxState {
    /// Seed configuration that used to live on `InnerState`.
    pub(crate) fn with_config(
        mut self,
        pool_assignments_enabled: bool,
        require_job_assignments: bool,
        runner_liveness_timeout: std::time::Duration,
    ) -> Self {
        self.pool_assignments_enabled = pool_assignments_enabled;
        self.require_job_assignments = require_job_assignments;
        self.runner_liveness_timeout = runner_liveness_timeout;
        self
    }

    /// Rebuild the derived request lookup maps after `job_requests` changed.
    /// Called by the backend after loading and by ported code that inserts
    /// or removes request records.
    pub(crate) fn reindex_requests(&mut self) {
        self.inflight_requests = self
            .job_requests
            .iter()
            .filter(|(_, record)| record.result.is_none())
            .map(|(id, record)| (*id, (record.run_id, record.job_id.clone())))
            .collect();
        self.plan_requests = self
            .job_requests
            .iter()
            .map(|(id, record)| (record.plan_id.clone(), *id))
            .collect();
        self.agent_job_requests = self
            .job_requests
            .iter()
            .map(|(id, record)| (record.agent_job_id, *id))
            .collect();
        self.timeline_requests = self
            .job_requests
            .iter()
            .map(|(id, record)| (record.timeline_id, *id))
            .collect();
    }

    /// Register a request record and its derived index entries.
    pub(crate) fn insert_request(&mut self, record: TaskAgentJobRequestRecord) {
        let request_id = record.request_id;
        if record.result.is_none() {
            self.inflight_requests
                .insert(request_id, (record.run_id, record.job_id.clone()));
        }
        self.plan_requests
            .insert(record.plan_id.clone(), request_id);
        self.agent_job_requests
            .insert(record.agent_job_id, request_id);
        self.timeline_requests
            .insert(record.timeline_id, request_id);
        self.job_requests.insert(request_id, record);
    }

    /// Remove a request record and its derived index entries.
    pub(crate) fn remove_request(&mut self, request_id: i64) -> Option<TaskAgentJobRequestRecord> {
        let record = self.job_requests.remove(&request_id)?;
        self.inflight_requests.remove(&request_id);
        if self.plan_requests.get(&record.plan_id) == Some(&request_id) {
            self.plan_requests.remove(&record.plan_id);
        }
        if self.agent_job_requests.get(&record.agent_job_id) == Some(&request_id) {
            self.agent_job_requests.remove(&record.agent_job_id);
        }
        if self.timeline_requests.get(&record.timeline_id) == Some(&request_id) {
            self.timeline_requests.remove(&record.timeline_id);
        }
        Some(record)
    }

    /// Push a job onto the ready queue (working-set copy) and count it.
    pub(crate) fn push_ready(&mut self, job: QueuedJob) {
        self.queue.push_back(job);
        self.ready_count += 1;
    }

    /// Remove the job at `pos` from the ready queue (working-set copy).
    pub(crate) fn remove_ready(&mut self, pos: usize) -> Option<QueuedJob> {
        let job = self.queue.remove(pos)?;
        self.ready_count -= 1;
        Some(job)
    }

    /// Whether the global ready queue is non-empty after this transaction's
    /// changes (used for the notify decision).
    pub(crate) fn queue_nonempty(&self) -> bool {
        self.ready_count > 0 || !self.cancellation_queue.is_empty()
    }

    /// Runner that owns a session, checking broker then AzDO maps — the
    /// same precedence the old `InnerState::runner_id_for_session` used.
    pub(crate) fn runner_id_for_session(&self, session_id: &str) -> Option<i64> {
        self.broker_session_runners
            .get(session_id)
            .copied()
            .or_else(|| {
                self.sessions
                    .get(session_id)
                    .map(|session| session.runner_id)
            })
    }

    /// Dispatch metadata for the runner owning `session_id`.
    pub(crate) fn runner_capabilities_for_session(
        &self,
        session_id: &str,
    ) -> crate::models::RunnerCapabilities {
        self.runner_id_for_session(session_id)
            .and_then(|runner_id| self.runners.get(&runner_id))
            .map(|runner| crate::models::RunnerCapabilities {
                known: true,
                labels: runner.labels.clone(),
                runner_group_id: runner.runner_group_id,
                runner_group_name: runner.runner_group_name.clone(),
            })
            .unwrap_or_default()
    }

    /// Record a session poll (durable `last_seen_at_us` on write-back).
    pub(crate) fn mark_session_seen(&mut self, session_id: &str) {
        self.session_last_seen
            .insert(session_id.to_owned(), std::time::SystemTime::now());
    }

    /// Allocate the next broker message id (counters table on write-back).
    pub(crate) fn next_broker_message_id(&mut self) -> i64 {
        self.next_message_id += 1;
        self.next_message_id
    }
}
