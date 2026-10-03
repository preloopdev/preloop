//! Test-only read view of the control database.
//!
//! `TestState` is the flat field set test assertions read. It is a
//! *snapshot*: each backend fills it from its tables in `test_working_set()`
//! and there is no write-back. Node-local fields are always empty; fields
//! that have no column in the schema (e.g. legacy `queued_at` timing data
//! superseded by `jobs.*_at` timestamps) stay empty as well.

use super::*;
use crate::concurrency;
use crate::models::{
    GitHubTokenRequest, QueuedCancellation, QueuedJob, RunRecord, TaskAgentJobRequestRecord,
};
use crate::state::{JobSetAdmission, JobSetId, OidcJobContext};
use preloop_gha_protocol::azdo;
use preloop_gha_protocol::crypto::AgentRsaPublicKey;
use preloop_gha_protocol::{JobId, RegisteredRunner, RunId, RunnerSession};
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};

/// Field names deliberately match the old `InnerState` scheduling subset —
/// the ported logic reads identically, and every field here is either
/// loaded from the database or seeded from configuration at load time.
#[derive(Clone)]
pub struct JobRowState {
    pub status: ExecutionStatus,
    pub queue_position: Option<i64>,
    pub seq: i64,
    pub priority: i16,
    pub run_order: i64,
    pub job_order: i64,
    pub not_before_us: Option<i64>,
    pub namespace_id: String,
    pub pool_key: String,
    pub row_sig: Option<u64>,
}

#[derive(Default, Clone)]
pub struct TestState {
    // ── Runs and queue collections ────────────────────────────────────
    pub runs: BTreeMap<RunId, RunRecord>,
    /// Tenant identity persisted alongside each run, including scoped
    /// foreign runs selected through the ready queue.
    pub run_namespaces: BTreeMap<RunId, String>,
    pub workflow_run_counters: BTreeMap<String, u64>,
    /// Ready-queue jobs *in this working set*. The global queue is the
    /// `jobs` table (`queue_kind='ready'` ordered by `queue_position`); this
    /// deque holds the candidates a poll inspected plus jobs this command
    /// newly enqueued, in order.
    pub queue: VecDeque<QueuedJob>,
    /// The global ready queue: every `jobs` row with `queue_kind='ready'`,
    /// in `queue_position` order. Commands that scan the queue (claim
    /// selection, capability matching, stuck-on-external-hosts) read this;
    /// `queue` holds only jobs this transaction newly enqueued.
    pub ready_index: VecDeque<QueuedJob>,
    /// Global ready-queue size at load time, adjusted as this transaction
    /// pushes/pops — reported back for the supervisor atomic.
    pub ready_count: i64,
    /// Set by a Postgres poll: the ready jobs this transaction locked. Other
    /// loaded ready jobs (siblings in a candidate run) may be claimed by a
    /// concurrent poll and must not be chosen here. `None`: every loaded
    /// ready job is claimable (single-writer and global transactions).
    pub poll_claimable: Option<BTreeSet<(RunId, JobId)>>,
    /// `runs-on` labels of the global ready-queue front, captured unscoped.
    /// Pool scaling reads this; a scoped `ready_index` head can name the
    /// wrong platform.
    pub next_queue_labels: Vec<String>,
    /// Whether the global ready queue was loaded into `ready_index` this
    /// transaction (`scope.ready_queue`). When true, `next_job_labels` reads
    /// the live front (post-claim); when false, it falls back to the
    /// load-time `next_queue_labels` snapshot.
    pub ready_queue_loaded: bool,
    /// Reaper bookkeeping: when each ready job was first observed queued.
    pub queued_at: BTreeMap<(RunId, JobId), std::time::SystemTime>,
    /// Persisted `(status, queue_position, seq)` for every `jobs` row this
    /// transaction loaded. A job whose run is NOT in `tx.runs` was widened in
    /// by a global queue-kind clause — its run record, queue position and
    /// FIFO seq are foreign. Write-back restores position/seq exactly only
    /// for a row staying in the SAME slot; a transition or requeue allocates
    /// fresh. Status is preserved verbatim for same-slot widened rows and
    /// otherwise comes from `job_status` (explicit override) or `run.jobs`.
    pub job_row_state: BTreeMap<(RunId, JobId), JobRowState>,
    /// Explicit per-job status overrides set by commands via
    /// [`TestState::set_job_status`]. Checked first at write-back so a command
    /// can mark a job `queued`/`in_progress` even when its run record isn't
    /// loaded (a widened foreign job being promoted or requeued).
    pub job_status: BTreeMap<(RunId, JobId), ExecutionStatus>,
    pub pending_jobs: VecDeque<QueuedJob>,
    pub pending_expansions: VecDeque<QueuedJob>,
    /// Expansion reservations held by this working set.
    pub expanding: BTreeSet<(RunId, JobId)>,
    /// Payloads of claimed-but-unapplied expansion nodes, kept at load so a
    /// crash recovery can push them back onto `pending_expansions` instead
    /// of losing the node (the row's payload columns and sealed message survive write-back).
    pub expanding_jobs: BTreeMap<(RunId, JobId), QueuedJob>,
    /// Generation this transaction claimed each expanding node under.
    pub expand_generations: BTreeMap<(RunId, JobId), i64>,
    pub concurrency_blocked: VecDeque<QueuedJob>,
    pub held_runs: BTreeMap<RunId, Vec<QueuedJob>>,
    pub claimed_jobs: BTreeMap<(RunId, JobId), QueuedJob>,

    // ── Runners and sessions ──────────────────────────────────────────
    pub runners: BTreeMap<i64, RegisteredRunner>,
    pub runner_registered_at: BTreeMap<i64, std::time::SystemTime>,
    pub runner_rsa_public_keys: BTreeMap<i64, AgentRsaPublicKey>,
    pub runner_client_ids: BTreeMap<String, i64>,
    pub pool_proven_runners: BTreeSet<i64>,
    /// AzDO-protocol sessions (`sessions` rows).
    pub sessions: BTreeMap<String, RunnerSession>,
    /// Broker-protocol session → runner.
    pub broker_session_runners: BTreeMap<String, i64>,
    pub session_last_seen: BTreeMap<String, std::time::SystemTime>,
    pub session_active_requests: BTreeMap<String, i64>,
    pub azdo_sessions: HashSet<String>,
    /// Sessions created under a verified listen token (the token named the
    /// runner). Only these count toward the duplicate-live-session conflict:
    /// an unverified compat session must never squat a runner id and block
    /// the legitimate runner's own session.
    pub verified_sessions: HashSet<String>,
    /// Undelivered session messages (broker_messages table).
    pub inflight_messages: BTreeMap<String, BTreeMap<i64, azdo::TaskAgentMessage>>,

    // ── Requests, messages, grants ────────────────────────────────────
    pub job_requests: BTreeMap<i64, TaskAgentJobRequestRecord>,
    /// request_id → undelivered job message (job_request_messages table).
    pub broker_messages: BTreeMap<i64, azdo::AgentJobRequestMessage>,
    pub github_token_requests: BTreeMap<i64, GitHubTokenRequest>,
    pub id_token_grants: BTreeMap<(RunId, JobId), bool>,
    pub oidc_job_contexts: BTreeMap<(RunId, JobId), OidcJobContext>,
    /// Derived lookup maps — rebuilt on load, never persisted directly.
    pub inflight_requests: BTreeMap<i64, (RunId, JobId)>,
    pub plan_requests: BTreeMap<String, i64>,
    pub agent_job_requests: BTreeMap<uuid::Uuid, i64>,
    pub timeline_requests: BTreeMap<uuid::Uuid, i64>,
    pub next_message_id: i64,
    pub next_runner_id: i64,
    /// Next job-request correlation id on SQLite, where the single writer
    /// serializes every allocation.
    pub next_request_id: i64,
    /// Request ids reserved from Postgres `request_id_seq` before this
    /// transaction loaded. `Some` means allocation must come from the pool:
    /// run-scoped writers on different runs run concurrently, so an
    /// in-memory `max + 1` would hand two of them the same primary key.
    pub reserved_request_ids: Option<VecDeque<i64>>,
    /// Set when a command allocated more request ids than were reserved.
    /// Write-back refuses to commit such a transaction.
    pub request_id_shortfall: bool,

    // ── Steps ─────────────────────────────────────────────────────────
    /// Step manifest per execution attempt (`agent_job_id`), in manifest
    /// order. Persisted one row per step in `job_steps`.
    pub job_steps: BTreeMap<uuid::Uuid, Vec<crate::models::StepRecord>>,

    // ── Concurrency and jobsets ───────────────────────────────────────
    pub concurrency_groups: BTreeMap<(String, String), concurrency::ConcurrencyGroup>,
    pub holder_keys: BTreeMap<RunId, Vec<(String, String)>>,
    pub jobset_admissions: BTreeMap<JobSetId, JobSetAdmission>,
    pub jobset_ready: BTreeSet<JobSetId>,
    pub run_concurrency: BTreeMap<RunId, preloop_gha_parser::Concurrency>,

    // ── Assignments and cancellations ─────────────────────────────────
    pub job_assignments: BTreeMap<(RunId, JobId), crate::models::AssignmentRecord>,
    pub pool_pending: BTreeMap<(RunId, JobId), std::time::SystemTime>,
    pub cancellation_queue: VecDeque<QueuedCancellation>,
    /// Durable outbox rows written in this working set: `(run, topic)`, in
    /// `event_id` order. Test-only read for the transactional event contract.
    pub outbox_topics: Vec<(Option<RunId>, String)>,
    /// `(topic, job_id, version)` of the same outbox rows, in the same order.
    pub outbox_stamps: Vec<(String, Option<String>, Option<i64>)>,
    /// Every queued job cancellation with its recorded reason
    /// (`job_cancellations.reason`), in cancellation order.
    pub cancellation_reasons: Vec<(RunId, JobId, Option<String>)>,

    // ── Configuration seeded at load (not persisted) ──────────────────
    pub pool_assignments_enabled: bool,
    pub require_job_assignments: bool,
    pub runner_liveness_timeout: std::time::Duration,
    /// Observable count of stale bindings released this transaction.
    pub released_bindings_count: u64,
}

/// Every session id with a `runner_sessions` row in the working set.
pub fn session_ids(tx: &TestState) -> BTreeSet<String> {
    tx.broker_session_runners
        .keys()
        .chain(tx.sessions.keys())
        .chain(tx.session_active_requests.keys())
        .chain(tx.session_last_seen.keys())
        .chain(tx.inflight_messages.keys())
        .cloned()
        .collect()
}

/// Everything the `runner_sessions` row of `session_id` is written from.
impl TestState {
    /// Seed configuration that used to live on `InnerState`.
    pub fn with_config(mut self, config: (bool, bool, std::time::Duration)) -> Self {
        self.pool_assignments_enabled = config.0;
        self.require_job_assignments = config.1;
        self.runner_liveness_timeout = config.2;
        self
    }

    /// Iterate the global ready queue: every persisted ready row
    /// (`ready_index`) followed by jobs this transaction newly enqueued
    /// (`queue`). The method
    /// form auto-borrows, so it works on owned and `&mut` receivers alike.
    pub fn ready(&self) -> impl Iterator<Item = &QueuedJob> {
        self.ready_index.iter().chain(self.queue.iter())
    }

    // Build a `TestState` from a recovered `InnerState` for the one-time
    // legacy→control import. Copies every migrated scheduling field; the
    // legacy `queue` (the persisted ready queue) lands in `ready_index`.
    // Node-local fields (logs, timeline, artifacts, crypto, debug) are
    // never copied — they do not belong to the scheduling working set.

    /// Rebuild the derived request lookup maps after `job_requests` changed.
    /// Called by the backend after loading and by ported code that inserts
    /// or removes request records.
    pub fn reindex_requests(&mut self) {
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
    pub fn insert_request(&mut self, record: TaskAgentJobRequestRecord) {
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
    pub fn remove_request(&mut self, request_id: i64) -> Option<TaskAgentJobRequestRecord> {
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
    pub fn push_ready(&mut self, job: QueuedJob) {
        self.queue.push_back(job);
        self.ready_count += 1;
    }

    /// Set a job's canonical execution status. Records an explicit override
    /// in `job_status` (consulted first at write-back) and, when the run is
    /// loaded, mirrors it into `run.jobs` so `summarize_run` and `run_record`
    /// see the same value. Use this instead of `run.jobs.insert` for any
    /// transition that may touch a widened job whose run isn't loaded.
    pub fn set_job_status(&mut self, run_id: RunId, job_id: JobId, status: ExecutionStatus) {
        self.job_status.insert((run_id, job_id.clone()), status);
        if let Some(run) = self.runs.get_mut(&run_id) {
            run.jobs.insert(job_id, status);
        }
    }

    /// Remove the job at `pos` from the ready queue (working-set copy).
    pub fn remove_ready(&mut self, pos: usize) -> Option<QueuedJob> {
        let job = self.queue.remove(pos)?;
        self.ready_count -= 1;
        Some(job)
    }

    /// Retain ready jobs matching `pred` across both halves of the ready
    /// queue (`ready_index` + this-transaction `queue`), decrementing
    /// `ready_count` for each removal. Raw `retain` on either deque leaves
    /// the scalar stale-high, so every removal path must go through here.
    pub fn retain_ready(&mut self, mut pred: impl FnMut(&QueuedJob) -> bool) {
        let before = self.queue.len() + self.ready_index.len();
        self.queue.retain(|job| pred(job));
        self.ready_index.retain(|job| pred(job));
        let after = self.queue.len() + self.ready_index.len();
        self.ready_count -= (before - after) as i64;
    }

    /// Whether the global ready queue is non-empty after this transaction's
    /// changes (used for the notify decision).
    pub fn queue_nonempty(&self) -> bool {
        self.ready_count > 0 || !self.cancellation_queue.is_empty()
    }

    /// Runner that owns a session, checking broker then AzDO maps — the
    /// same precedence the old `InnerState::runner_id_for_session` used.
    pub fn runner_id_for_session(&self, session_id: &str) -> Option<i64> {
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
    pub fn runner_capabilities_for_session(
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
    pub fn mark_session_seen(&mut self, session_id: &str) {
        self.session_last_seen
            .insert(session_id.to_owned(), std::time::SystemTime::now());
    }

    /// Allocate the next broker message id (counters table on write-back).
    pub fn next_broker_message_id(&mut self) -> i64 {
        self.next_message_id += 1;
        self.next_message_id
    }

    /// Allocate the next job-request correlation id. Postgres transactions
    /// consume ids reserved from `request_id_seq` before load; running dry
    /// marks the transaction so write-back refuses it (and returns a
    /// placeholder that is never persisted). SQLite's single writer
    /// serializes allocation, so it advances the loaded counter.
    pub fn alloc_request_id(&mut self) -> i64 {
        if let Some(pool) = &mut self.reserved_request_ids {
            if let Some(id) = pool.pop_front() {
                return id;
            }
            self.request_id_shortfall = true;
            return 0;
        }
        self.next_request_id += 1;
        self.next_request_id
    }
}
