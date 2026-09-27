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
#[derive(Default, Clone)]
pub(crate) struct TxState {
    // ── Runs and queue collections ────────────────────────────────────
    pub(crate) runs: BTreeMap<RunId, RunRecord>,
    /// Tenant identity persisted alongside each run, including scoped
    /// foreign runs selected through the ready queue.
    pub(crate) run_namespaces: BTreeMap<RunId, String>,
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
    /// Set by a Postgres poll: the ready jobs this transaction locked. Other
    /// loaded ready jobs (siblings in a candidate run) may be claimed by a
    /// concurrent poll and must not be chosen here. `None`: every loaded
    /// ready job is claimable (single-writer and global transactions).
    pub(crate) poll_claimable: Option<BTreeSet<(RunId, JobId)>>,
    /// `runs-on` labels of the global ready-queue front, captured unscoped.
    /// Pool scaling reads this; a scoped `ready_index` head can name the
    /// wrong platform.
    pub(crate) next_queue_labels: Vec<String>,
    /// Whether the global ready queue was loaded into `ready_index` this
    /// transaction (`scope.ready_queue`). When true, `next_job_labels` reads
    /// the live front (post-claim); when false, it falls back to the
    /// load-time `next_queue_labels` snapshot.
    pub(crate) ready_queue_loaded: bool,
    /// Reaper bookkeeping: when each ready job was first observed queued.
    pub(crate) queued_at: BTreeMap<(RunId, JobId), std::time::SystemTime>,
    /// Persisted `(status, queue_position, seq)` for every `jobs` row this
    /// transaction loaded. A job whose run is NOT in `tx.runs` was widened in
    /// by a global queue-kind clause — its run record, queue position and
    /// FIFO seq are foreign. Write-back restores position/seq exactly only
    /// for a row staying in the SAME slot; a transition or requeue allocates
    /// fresh. Status is preserved verbatim for same-slot widened rows and
    /// otherwise comes from `job_status` (explicit override) or `run.jobs`.
    pub(crate) job_row_state: BTreeMap<(RunId, JobId), JobRowState>,
    /// Explicit per-job status overrides set by commands via
    /// [`TxState::set_job_status`]. Checked first at write-back so a command
    /// can mark a job `queued`/`in_progress` even when its run record isn't
    /// loaded (a widened foreign job being promoted or requeued).
    pub(crate) job_status: BTreeMap<(RunId, JobId), ExecutionStatus>,
    pub(crate) pending_jobs: VecDeque<QueuedJob>,
    pub(crate) pending_expansions: VecDeque<QueuedJob>,
    /// Expansion reservations held by this working set.
    pub(crate) expanding: BTreeSet<(RunId, JobId)>,
    /// Payloads of claimed-but-unapplied expansion nodes, kept at load so a
    /// crash recovery can push them back onto `pending_expansions` instead
    /// of losing the node (the row's payload columns and sealed message survive write-back).
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
    /// Sessions created under a verified listen token (the token named the
    /// runner). Only these count toward the duplicate-live-session conflict:
    /// an unverified compat session must never squat a runner id and block
    /// the legitimate runner's own session.
    pub(crate) verified_sessions: HashSet<String>,
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
    /// Next job-request correlation id on SQLite, where the single writer
    /// serializes every allocation.
    pub(crate) next_request_id: i64,
    /// Request ids reserved from Postgres `request_id_seq` before this
    /// transaction loaded. `Some` means allocation must come from the pool:
    /// run-scoped writers on different runs run concurrently, so an
    /// in-memory `max + 1` would hand two of them the same primary key.
    pub(crate) reserved_request_ids: Option<VecDeque<i64>>,
    /// Set when a command allocated more request ids than were reserved.
    /// Write-back refuses to commit such a transaction.
    pub(crate) request_id_shortfall: bool,

    // ── Steps ─────────────────────────────────────────────────────────
    /// Step manifest per execution attempt (`agent_job_id`), in manifest
    /// order. Persisted one row per step in `job_steps`.
    pub(crate) job_steps: BTreeMap<uuid::Uuid, Vec<crate::models::StepRecord>>,

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

/// Persisted columns of a loaded job, preserved for widened foreign rows.
#[derive(Clone)]
pub(crate) struct JobRowState {
    pub(crate) status: ExecutionStatus,
    pub(crate) queue_position: Option<i64>,
    pub(crate) seq: i64,
    pub(crate) priority: i16,
    pub(crate) run_order: i64,
    pub(crate) job_order: i64,
    pub(crate) not_before_us: Option<i64>,
    pub(crate) namespace_id: String,
    pub(crate) pool_key: String,
    /// Signature of every `jobs` column as loaded ([`job_row_sig`]). A
    /// same-slot job whose written columns hash identically is not
    /// rewritten, so an unchanged stale row never overwrites a concurrent
    /// writer's claim. `None` (SQLite: single writer) always writes.
    pub(crate) row_sig: Option<u64>,
}

/// Hash of one `jobs` row's written columns, in their stored forms.
#[allow(clippy::too_many_arguments)]
pub(crate) fn job_row_sig(
    status: &str,
    kind: &str,
    queue_position: Option<i64>,
    seq: i64,
    base_id: &str,
    runs_on: &str,
    runner_group: Option<&str>,
    enqueued_us: Option<i64>,
    reaper_us: Option<i64>,
    claimed_by: Option<i64>,
    claimed_at_us: Option<i64>,
    expand_generation: i64,
    namespace_id: &str,
    pool_key: &str,
    priority: i16,
    run_order: i64,
    job_order: i64,
    not_before_us: Option<i64>,
) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    (
        status,
        kind,
        queue_position,
        seq,
        base_id,
        runs_on,
        runner_group,
        enqueued_us,
        reaper_us,
    )
        .hash(&mut h);
    (
        claimed_by,
        claimed_at_us,
        expand_generation,
        namespace_id,
        pool_key,
        priority,
        run_order,
        job_order,
        not_before_us,
    )
        .hash(&mut h);
    h.finish()
}

/// Rows loaded into a [`TxState`], used to compute deletions at write-back:
/// a key present at load but absent from the working set afterwards was
/// removed by the command and must be deleted in the same transaction.
#[derive(Default, Clone)]
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
    /// Loaded `concurrency_groups` snapshots, keyed by (repo, group). A group
    /// whose snapshot equals the working value is written as nothing — its
    /// hold/waiter rows stay byte-identical (`held_at_us` is not re-stamped).
    pub(crate) group_snapshot: BTreeMap<(String, String), concurrency::ConcurrencyGroup>,
    pub(crate) jobsets: BTreeSet<JobSetId>,
    /// Loaded `jobset_admissions` snapshots for the same change-skip rule.
    pub(crate) jobset_snapshot: BTreeMap<JobSetId, JobSetAdmission>,
    pub(crate) assignments: BTreeSet<(RunId, JobId)>,
    pub(crate) cancellations: BTreeSet<(RunId, JobId, uuid::Uuid)>,
    /// Step manifests as loaded, per attempt. Write-back diffs against this
    /// so a step transition is a one-row upsert and untouched steps are never
    /// rewritten.
    pub(crate) steps: BTreeMap<uuid::Uuid, Vec<crate::models::StepRecord>>,
    /// Run records as loaded, decomposed into their table rows. Write-back
    /// diffs against these so only changed runs/jobs/submissions are written.
    pub(crate) run_parts: BTreeMap<RunId, super::rows::RunParts>,
    pub(crate) holder_key_runs: BTreeSet<RunId>,
    /// Counters loaded (name → value at load).
    pub(crate) counters: BTreeMap<String, i64>,
    /// Per-workflow run numbers loaded (key → value at load).
    pub(crate) workflow_run_counters: BTreeMap<String, u64>,
    /// Signature of each runner / request row as loaded. Write-back skips
    /// rows whose signature is unchanged: re-writing an unchanged loaded row
    /// is at best a no-op and at worst reverts a concurrent writer.
    /// Per-key signatures of the keyed side families, for the same
    /// skip-unchanged rule: write-back upserts changed keys and deletes only
    /// loaded keys that are gone. (A scoped delete + reinsert races any
    /// concurrent writer of the same scope: its insert collides with rows
    /// the other committed after this delete began.)
    pub(crate) session_sigs: BTreeMap<String, u64>,
    pub(crate) message_sigs: BTreeMap<(String, i64), u64>,
    pub(crate) token_sigs: BTreeMap<i64, u64>,
    pub(crate) grant_sigs: BTreeMap<(RunId, JobId), u64>,
    pub(crate) oidc_sigs: BTreeMap<(RunId, JobId), u64>,
    pub(crate) run_concurrency_sigs: BTreeMap<RunId, u64>,
    pub(crate) assignment_sigs: BTreeMap<(RunId, JobId), u64>,
    pub(crate) pool_pending_sigs: BTreeMap<(RunId, JobId), u64>,
    pub(crate) runner_sigs: BTreeMap<i64, u64>,
    pub(crate) request_sigs: BTreeMap<i64, u64>,
}

/// Which rows a command's transaction loads and writes back.
///
/// The default [`TxScope::full`] loads the whole working set — identical to
/// the pre-scoping behavior. A narrower scope loads only the rows a command
/// touches, and write-back only touches those same rows (the `loaded`
/// bookkeeping already scopes deletions to what was read).
///
/// # Invariants (a scope that violates these corrupts state)
///
/// - **A scoped run loads ALL its jobs.** `run.jobs` is rebuilt from the job
///   rows loaded; `summarize_run`/`finalize_run_if_complete` would mark a run
///   complete over a partial map. The jobs predicate is
///   `run_id IN (scope) OR queue_kind IN (ready,blocked)` — the run clause has
///   no `queue_kind` filter, so a scoped run always loads every one of its
///   jobs; the kind clauses only *widen* the set to global queues.
/// - **Delete-set == load-set, per family.** A family deleted by a narrower
///   key set than it was loaded by re-INSERTs the survivors against the PK
///   with no `ON CONFLICT` → constraint failure. `job_assignments`/
///   `pool_pending` load under `ready_queue || includes_run`, so they delete
///   via `delete_scoped_queue` (full when `ready_queue`), not `scope.runs`.
/// - **Always-global families are never scoped.** `runners` (claim_permitted,
///   unhostable_platform), `concurrency_*`/`holder_keys`/`jobset_*` (a different
///   run's release unblocks a waiter), and `counters`/`workflow_run_counters`
///   (allocators must never read 0) load unconditionally.
/// - **Global scalars are unscoped queries.** `ready_count` and
///   `next_queue_labels` come from the whole ready table, not the loaded set.
/// - **Concurrency is always global.** `concurrency_groups`, `holder_keys`,
///   `jobset_admissions`, `jobset_ready` and `concurrency_blocked` jobs are
///   never run-scoped: a *different* run releasing a shared `(repo, group)`
///   gate is what unblocks a blocked job, so scoping them by `run_id` would
///   permanently strand waiters. They load fully under any non-`None` scope.
///
/// # Known traps for future narrow scopes
///
/// - `choose_claim_position`'s GC sweep (`job_assignments`/`pool_pending`
///   `.retain`) only expires the loaded subset — stale bindings for unloaded
///   runs survive (the "leaked job bindings" failure mode). A narrow poll
///   scope must move expiry to an unscoped `DELETE WHERE at_us < cutoff`.
/// - FIFO positions/seqs come from atomic database counters. A scoped writer
///   must never derive them from the subset it happened to load.
#[derive(Debug, Clone)]
pub(crate) struct TxScope {
    /// Run-scoped families (`runs`, `jobs`, `job_requests`, `run_concurrency`,
    /// `id_token_grants`, `oidc_job_contexts`, `job_steps`, `job_assignments`,
    /// `pool_pending`, `cancellation_queue`, `held_runs`, `pending_jobs`,
    /// `pending_expansions`, `claimed_jobs`, `expanding`). `None` = load all;
    /// `Some(set)` = only these runs (plus the always-global queue kinds).
    pub(crate) runs: Option<BTreeSet<RunId>>,
    /// Load the global ready queue (`queue_kind='ready'` jobs). `poll` needs
    /// it to pick a claim; a single-run command does not.
    pub(crate) ready_queue: bool,
    /// Load concurrency-blocked jobs (`queue_kind='blocked'`). Needed by any
    /// command that can release a gate.
    pub(crate) blocked_jobs: bool,
    /// Sessions to load (`runner_sessions`, `broker_messages`, inflight).
    /// `None` = all; `Some(set)` = only these session ids.
    pub(crate) sessions: Option<BTreeSet<String>>,
    /// Load the always-global concurrency + jobset families.
    pub(crate) concurrency: bool,
    /// Widen `runs` with every run this transaction can touch indirectly:
    /// the runs of the queued jobs the scope loads (ready/blocked), the runs
    /// of the scoped sessions' active requests, and concurrency holders'
    /// runs. `poll`/`complete`/`cancel` mutate those runs (claim marks the
    /// run in-progress, promotion summarizes it, settle drops its request),
    /// so they must be in the working set — but loading ALL runs (`runs:
    /// None`) re-reads every `record_blob` per call. The referenced set is
    /// computed with `SELECT DISTINCT run_id` queries (no blob) before the
    /// run load. Ignored when `runs` is `None` (already everything).
    pub(crate) runs_referenced: bool,
    /// Load every `job_requests` row regardless of `runs`. `runs` scopes the
    /// request table by `run_id`; a command that needs the global request
    /// set (e.g. collecting every active `plan_id`) sets this so it does not
    /// also pull every `record_blob`. Ignored when `runs` is `None`.
    pub(crate) job_requests_all: bool,
    /// Load every `queue_kind='expand'` job (deferred matrix/reusable nodes
    /// awaiting expansion) regardless of `runs`. `claim_expansion` sets this
    /// — it cannot name the run until it pops a node. Ignored when `runs`
    /// is `None`.
    pub(crate) pending_expansions: bool,
    /// Widen `runs` with the `run_id` of every loaded `job_requests` row.
    /// For correlation lookups (agent_job/plan/timeline → request → run)
    /// that must then read the run's `record_blob`: `job_requests_all`
    /// resolves the request, this loads exactly the runs those requests
    /// point to — not every run in the table. Ignored when `runs` is
    /// `None` (already everything).
    pub(crate) runs_via_requests: bool,
    /// Read-only projection for a named historical run. Write commands must
    /// never load archived rows into their mutable working set.
    pub(crate) include_archived: bool,
}

impl Default for TxScope {
    /// The only safe default is the full working set — a partial scope must be
    /// chosen deliberately per command, never implied by `..Default::default()`.
    fn default() -> Self {
        Self::full()
    }
}

impl TxScope {
    /// The whole working set — identical to the pre-scoping load.
    pub(crate) fn full() -> Self {
        Self {
            runs: None,
            ready_queue: true,
            blocked_jobs: true,
            sessions: None,
            concurrency: true,
            runs_referenced: false,
            job_requests_all: false,
            pending_expansions: true,
            runs_via_requests: false,
            include_archived: false,
        }
    }

    /// A single run and nothing else — no queue, sessions, or concurrency.
    /// The common case for run-scoped commands (cancel, detail reads, log
    /// callbacks that already know their `run_id`).
    pub(crate) fn run(run_id: RunId) -> Self {
        Self::runs(std::iter::once(run_id).collect())
    }

    /// A set of runs and nothing else. `Some(set)` scopes every run-keyed
    /// family to those runs; the always-global queue kinds stay unloaded.
    pub(crate) fn runs(runs: BTreeSet<RunId>) -> Self {
        Self {
            runs: Some(runs),
            ready_queue: false,
            blocked_jobs: false,
            sessions: Some(BTreeSet::new()),
            concurrency: false,
            runs_referenced: false,
            job_requests_all: false,
            pending_expansions: false,
            runs_via_requests: false,
            include_archived: false,
        }
    }

    /// Only the global request-correlation maps (`job_requests`,
    /// `plan_requests`, `timeline_requests`, `agent_job_requests`) — no run
    /// `record_blob`s. For callback/correlation lookups that resolve a
    /// `plan_id`/`timeline_id`/`agent_job_id` to a `(run_id, job_id)` and
    /// never touch the run record itself. `runs: Some({})` loads zero run
    /// blobs; `job_requests_all` loads the request table unscoped.
    pub(crate) fn requests_only() -> Self {
        Self {
            runs: Some(BTreeSet::new()),
            ready_queue: false,
            blocked_jobs: false,
            sessions: Some(BTreeSet::new()),
            concurrency: false,
            runs_referenced: false,
            job_requests_all: true,
            pending_expansions: false,
            runs_via_requests: false,
            include_archived: false,
        }
    }

    /// Resolve a request correlation (agent_job/plan/timeline → request →
    /// run) and load the run it points to. `job_requests_all` loads the
    /// request maps; `runs_via_requests` widens `runs` to exactly the runs
    /// those requests reference — not every `record_blob` in the table.
    pub(crate) fn request_correlation() -> Self {
        Self {
            runs: Some(BTreeSet::new()),
            ready_queue: false,
            blocked_jobs: false,
            sessions: Some(BTreeSet::new()),
            concurrency: false,
            runs_referenced: false,
            job_requests_all: true,
            pending_expansions: false,
            runs_via_requests: true,
            include_archived: false,
        }
    }

    pub(crate) fn with_history(mut self) -> Self {
        self.include_archived = true;
        self
    }

    /// True when a run-scoped family should load a given `run_id`.
    pub(crate) fn includes_run(&self, run_id: &RunId) -> bool {
        self.runs.as_ref().is_none_or(|set| set.contains(run_id))
    }
}

/// Mutations of node-local state the command recorded; the caller applies
/// them to `InnerState` only after the transaction commits, so a rolled-back
/// command never leaves a live-log feed closed or a DAP port forgotten.
#[derive(Default, Clone)]
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
    pub(crate) fn with_config(mut self, config: (bool, bool, std::time::Duration)) -> Self {
        self.pool_assignments_enabled = config.0;
        self.require_job_assignments = config.1;
        self.runner_liveness_timeout = config.2;
        self
    }

    /// Iterate the global ready queue: every persisted ready row
    /// (`ready_index`) followed by jobs this transaction newly enqueued
    /// (`queue`). Mirrors [`crate::control::sched::ready_jobs`]; the method
    /// form auto-borrows, so it works on owned and `&mut` receivers alike.
    pub(crate) fn ready(&self) -> impl Iterator<Item = &QueuedJob> {
        self.ready_index.iter().chain(self.queue.iter())
    }

    // Build a `TxState` from a recovered `InnerState` for the one-time
    // legacy→control import. Copies every migrated scheduling field; the
    // legacy `queue` (the persisted ready queue) lands in `ready_index`.
    // Node-local fields (logs, timeline, artifacts, crypto, debug) are
    // never copied — they do not belong to the scheduling working set.

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

    /// Set a job's canonical execution status. Records an explicit override
    /// in `job_status` (consulted first at write-back) and, when the run is
    /// loaded, mirrors it into `run.jobs` so `summarize_run` and `run_record`
    /// see the same value. Use this instead of `run.jobs.insert` for any
    /// transition that may touch a widened job whose run isn't loaded.
    pub(crate) fn set_job_status(&mut self, run_id: RunId, job_id: JobId, status: ExecutionStatus) {
        self.job_status.insert((run_id, job_id.clone()), status);
        if let Some(run) = self.runs.get_mut(&run_id) {
            run.jobs.insert(job_id, status);
        }
    }

    /// Remove the job at `pos` from the ready queue (working-set copy).
    pub(crate) fn remove_ready(&mut self, pos: usize) -> Option<QueuedJob> {
        let job = self.queue.remove(pos)?;
        self.ready_count -= 1;
        Some(job)
    }

    /// Retain ready jobs matching `pred` across both halves of the ready
    /// queue (`ready_index` + this-transaction `queue`), decrementing
    /// `ready_count` for each removal. Raw `retain` on either deque leaves
    /// the scalar stale-high, so every removal path must go through here.
    pub(crate) fn retain_ready(&mut self, mut pred: impl FnMut(&QueuedJob) -> bool) {
        let before = self.queue.len() + self.ready_index.len();
        self.queue.retain(|job| pred(job));
        self.ready_index.retain(|job| pred(job));
        let after = self.queue.len() + self.ready_index.len();
        self.ready_count -= (before - after) as i64;
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

    /// Allocate the next job-request correlation id. Postgres transactions
    /// consume ids reserved from `request_id_seq` before load; running dry
    /// marks the transaction so write-back refuses it (and returns a
    /// placeholder that is never persisted). SQLite's single writer
    /// serializes allocation, so it advances the loaded counter.
    pub(crate) fn alloc_request_id(&mut self) -> i64 {
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

/// Global counters this transaction advanced past their loaded value.
pub(crate) fn advanced_counters(tx: &TxState) -> Vec<(&'static str, i64)> {
    [
        ("next_message_id", tx.next_message_id),
        ("next_runner_id", tx.next_runner_id),
        ("next_request_id", tx.next_request_id),
    ]
    .into_iter()
    .filter(|(name, value)| {
        tx.loaded
            .counters
            .get(*name)
            .is_none_or(|loaded| value > loaded)
    })
    .collect()
}

/// Per-workflow run numbers this transaction advanced past their loaded value.
pub(crate) fn advanced_run_counters(tx: &TxState) -> Vec<(String, i64)> {
    tx.workflow_run_counters
        .iter()
        .filter(|(key, value)| {
            tx.loaded
                .workflow_run_counters
                .get(*key)
                .is_none_or(|loaded| *value > loaded)
        })
        .map(|(key, value)| (key.clone(), *value as i64))
        .collect()
}

fn sig_of(parts: &[&dyn std::fmt::Debug]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for part in parts {
        format!("{part:?}").hash(&mut hasher);
    }
    hasher.finish()
}

/// Everything the `runners` row is written from.
pub(crate) fn runner_sig(tx: &TxState, runner_id: i64) -> Option<u64> {
    let runner = tx.runners.get(&runner_id)?;
    let rsa = tx
        .runner_rsa_public_keys
        .get(&runner_id)
        .map(|k| k.to_xml_string());
    let client = tx
        .runner_client_ids
        .iter()
        .find(|(_, id)| **id == runner_id)
        .map(|(c, _)| c);
    Some(sig_of(&[
        runner,
        &rsa,
        &client,
        &tx.pool_proven_runners.contains(&runner_id),
        &tx.runner_registered_at.get(&runner_id),
    ]))
}

/// Everything a loaded `job_requests` row is rewritten from. The job message
/// is excluded: it is sealed once when the request is minted and never
/// changes inside a command, so write-back leaves it alone on existing rows.
pub(crate) fn request_sig(tx: &TxState, request_id: i64) -> Option<u64> {
    tx.job_requests
        .get(&request_id)
        .map(|record| value_sig(record))
}

/// Signature of one family value.
pub(crate) fn value_sig(value: &dyn std::fmt::Debug) -> u64 {
    sig_of(&[value])
}

fn sigs<K: Ord + Clone, V: std::fmt::Debug>(map: &BTreeMap<K, V>) -> BTreeMap<K, u64> {
    map.iter().map(|(k, v)| (k.clone(), value_sig(v))).collect()
}

/// Every session id with a `runner_sessions` row in the working set.
pub(crate) fn session_ids(tx: &TxState) -> BTreeSet<String> {
    tx.broker_session_runners
        .keys()
        .chain(tx.sessions.keys())
        .chain(tx.session_active_requests.keys())
        .chain(tx.session_last_seen.keys())
        .chain(tx.inflight_messages.keys())
        .chain(tx.session_keys.keys())
        .cloned()
        .collect()
}

/// Everything the `runner_sessions` row of `session_id` is written from.
pub(crate) fn session_sig(tx: &TxState, session_id: &str) -> u64 {
    sig_of(&[
        &tx.broker_session_runners.get(session_id),
        &tx.sessions.get(session_id),
        &tx.session_keys.get(session_id).map(|k| &k.key),
        &tx.session_active_requests.get(session_id),
        &tx.session_last_seen.get(session_id),
        &tx.verified_sessions.contains(session_id),
    ])
}

/// Record load-time signatures (call once, at the end of a load).
pub(crate) fn snapshot_row_sigs(tx: &mut TxState) {
    tx.loaded.session_sigs = session_ids(tx)
        .into_iter()
        .map(|id| {
            let sig = session_sig(tx, &id);
            (id, sig)
        })
        .collect();
    tx.loaded.message_sigs = tx
        .inflight_messages
        .iter()
        .flat_map(|(session_id, messages)| {
            messages
                .iter()
                .map(move |(id, m)| ((session_id.clone(), *id), value_sig(m)))
        })
        .collect();
    tx.loaded.token_sigs = sigs(&tx.github_token_requests);
    tx.loaded.grant_sigs = sigs(&tx.id_token_grants);
    tx.loaded.oidc_sigs = sigs(&tx.oidc_job_contexts);
    tx.loaded.run_concurrency_sigs = sigs(&tx.run_concurrency);
    tx.loaded.assignment_sigs = sigs(&tx.job_assignments);
    tx.loaded.pool_pending_sigs = sigs(&tx.pool_pending);
    tx.loaded.cancellations = tx
        .cancellation_queue
        .iter()
        .map(|c| (c.run_id, c.job_id.clone(), c.agent_job_id))
        .collect();
    tx.loaded.runner_sigs = tx
        .runners
        .keys()
        .filter_map(|id| runner_sig(tx, *id).map(|sig| (*id, sig)))
        .collect();
    tx.loaded.request_sigs = tx
        .job_requests
        .keys()
        .filter_map(|id| request_sig(tx, *id).map(|sig| (*id, sig)))
        .collect();
}
