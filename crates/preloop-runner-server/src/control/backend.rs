//! The `ControlBackend` contract: every durable control mutation is one typed
//! command with transactional authority, idempotency and fencing.
//!
//! This is the only surface the server sees for durable control state.
//! Handlers parse requests and map domain results onto wire responses; they
//! never touch a transaction handle, a SQL string, or a mutable record. A
//! backend runs each command as one transaction: load the working set, run
//! the shared scheduling logic ([`crate::control::sched`]), write the delta
//! back, return the domain result.
//!
//! SQLite is the default backend (single writer, `BEGIN IMMEDIATE`, WAL).
//! Postgres implements the identical contract for shared-node deployments
//! (a pool of connections, `SELECT … FOR UPDATE SKIP LOCKED` for leases).
//! Both produce the same domain results from the same inputs — the shared
//! behavioral suite in `control::tests` proves it.

use super::sched::{BuiltExpansion, SchedulingOutcome};
use super::types::*;
use crate::models::{QueuedJob, RunRecord, TaskAgentJobRequestRecord};
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use std::collections::BTreeMap;

/// The control-plane authority. Object-safe so `AppState` holds
/// `Arc<dyn ControlBackend>` and the backend is swappable by configuration.
///
/// Every method is one transaction. `&self` (not `&mut`) because the
/// serialization lives in the database, not in the object — a Postgres
/// backend serves concurrent commands from a pool; a SQLite backend
/// serializes writers on the single connection.
#[async_trait::async_trait]
pub(crate) trait ControlBackend: Send + Sync {
    // ── Run lifecycle ─────────────────────────────────────────────────

    /// Insert a submitted run and its jobs, evaluating workflow/job/jobset
    /// concurrency gates inside the transaction. Idempotent on the run's
    /// dedup key (webhook delivery / request identity): a replay returns
    /// `SubmitOutcome::existing` instead of a second run.
    async fn submit_run(&self, submit: SubmitRun) -> Result<SubmitOutcome, ControlError>;

    /// Allocate the next run number for a workflow path from the durable
    /// counter. Called before the job messages are built (the number is
    /// embedded in `github.run_number`), so it is its own transaction — a
    /// crash between this and `submit_run` burns a number, which is
    /// acceptable (run numbers may have gaps).
    async fn allocate_run_number(&self, workflow_path: &str) -> Result<u64, ControlError>;

    /// Claim the next dispatchable job for a session. Marks the session
    /// seen, picks a ready job the runner may claim (capability + binding
    /// rules), mints its request/message correlation, and returns the
    /// claim — or a queued cancellation, or `PollOutcome::Empty`.
    async fn poll_session(&self, poll: PollRequest) -> Result<PollOutcome, ControlError>;

    /// Everything `acquirejob` needs in one read: the request record, its
    /// job message, the id-token grant and the deferred token-mint request.
    async fn acquire_context(&self, request_id: i64) -> Result<AcquireContext, ControlError>;

    /// Record a runner's job completion: flip the job (and run) status,
    /// settle the request, release concurrency, promote newly-unblocked
    /// dependents, and return the post-transition run plus the effects the
    /// handler must fan out (events, check-run updates, live-log close).
    async fn complete_job(
        &self,
        completion: JobCompletionInput,
    ) -> Result<CompleteOutcome, ControlError>;

    /// Cancel a run or one job: mark non-terminal jobs cancelled, enqueue
    /// cancellation messages for in-flight jobs, release concurrency, and
    /// settle expandable-node request records.
    async fn cancel_run(
        &self,
        run_id: RunId,
        reason: Option<String>,
    ) -> Result<CancelOutcome, ControlError>;
    async fn cancel_job(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<CancelOutcome, ControlError>;

    /// Renew a claimed request's lease. Returns the record on success;
    /// `Stale` if the runner no longer owns it, `NotFound` if unknown.
    async fn renew_request(
        &self,
        request_id: i64,
        runner_id: i64,
    ) -> Result<TaskAgentJobRequestRecord, ControlError>;

    /// Release an interrupted claim so the request can be redelivered
    /// (runner disconnect / retry). Keeps inflight + token records, drops
    /// the dead owner.
    async fn release_request(&self, request_id: i64) -> Result<(), ControlError>;

    // ── Runner and session lifecycle ──────────────────────────────────

    /// Register or refresh a runner. Dedup on `client_id`: a re-register
    /// returns the existing runner id. Pairs the runner with a pending job
    /// it can serve when pool/strict assignment is enabled.
    async fn register_runner(&self, reg: RegisterRunner) -> Result<RunnerRow, ControlError>;

    /// Open a session for a runner (broker or AzDO protocol). Returns the
    /// session id and the sealed crypto material the handler wraps for the
    /// wire.
    async fn create_session(&self, session: CreateSession) -> Result<SessionRow, ControlError>;

    /// Close a session: drop it, release its active request for retry, and
    /// requeue or fail its claimed job per the disconnect rules.
    async fn delete_session(&self, session_id: &str) -> Result<(), ControlError>;

    /// Remove a runner identity and every session/binding it owned;
    /// requeue its claimed jobs so a replacement can pick them up.
    async fn purge_runner(&self, runner_id: i64) -> Result<(), ControlError>;

    // ── Expansion ─────────────────────────────────────────────────────

    /// Lease the next pending expansion node: pop it, stamp an
    /// `expand_generation` fence, and snapshot the inputs the build needs.
    /// `None` when the expansion queue is empty.
    async fn claim_expansion(&self) -> Result<Option<ExpansionClaim>, ControlError>;

    /// Fold a built subtree back into the run under the claim's generation
    /// fence, then promote whatever the subtree unblocked. A stale
    /// generation (the node was cancelled/re-leased mid-build) discards the
    /// build.
    async fn apply_expansion(
        &self,
        claim: ExpansionApply,
    ) -> Result<SchedulingOutcome, ControlError>;

    // ── Reaper / reconcile ────────────────────────────────────────────

    /// Startup reconcile: drop concurrency holders whose runs are terminal
    /// or missing, and recover or fail claims orphaned by the restart.
    async fn reconcile_on_boot(&self) -> Result<ReconcileOutcome, ControlError>;

    // ── Queries (read-only, no transaction needed) ────────────────────

    /// The run record with its job statuses, for status APIs and event
    /// fan-out.
    async fn run_record(&self, run_id: RunId) -> Result<RunRecord, ControlError>;

    /// A request record by id, plan id, agent job id or timeline id.
    async fn request(&self, key: RequestKey) -> Result<TaskAgentJobRequestRecord, ControlError>;

    /// Queue pressure snapshot for status/metrics.
    async fn queue_stats(&self) -> Result<QueueStats, ControlError>;

    /// Live runner → job assignments for status reporting.
    async fn live_assignments(
        &self,
    ) -> Result<Vec<preloop_observability::status::RunnerAssignment>, ControlError>;
}

// ─────────────────────────────────────────────────────────────────────────
// Command inputs (owned, backend-neutral)
// ─────────────────────────────────────────────────────────────────────────

/// What a session poll carries: who is asking and what they can run.
#[derive(Debug)]
pub(crate) struct PollRequest {
    pub(crate) session_id: String,
    /// Runner identity proven by the listen token — never the session's
    /// self-declared id.
    pub(crate) verified_runner_id: Option<i64>,
    /// Capabilities resolved for the session's runner.
    pub(crate) runner: crate::models::RunnerCapabilities,
    /// Runner self-reports busy (`status=busy`): deliver inflight/cancel/
    /// active-request outcomes but do NOT claim new work.
    pub(crate) busy: bool,
    /// Long-poll: the caller may hold the request until work appears. The
    /// backend returns `PollOutcome::Empty` immediately; the handler decides
    /// whether to wait on the notify channel.
    pub(crate) wait_ms: u64,
}

/// A runner job completion, normalized from the protocol report.
#[derive(Debug)]
pub(crate) struct JobCompletionInput {
    pub(crate) run_id: RunId,
    pub(crate) job_id: JobId,
    /// The attempt this completion belongs to, when the caller resolved one.
    pub(crate) agent_job_id: Option<uuid::Uuid>,
    pub(crate) status: ExecutionStatus,
    /// Outputs captured by the runner.
    pub(crate) outputs: BTreeMap<String, serde_json::Value>,
    /// Runner that reported (for ownership checks).
    pub(crate) runner_id: Option<i64>,
}

/// Runner registration input.
pub(crate) struct RegisterRunner {
    pub(crate) name: String,
    pub(crate) labels: Vec<String>,
    pub(crate) ephemeral: bool,
    pub(crate) public_key: Option<String>,
    /// Parsed RSA public key, when supplied.
    pub(crate) rsa_public_key: Option<preloop_gha_protocol::crypto::AgentRsaPublicKey>,
    /// Stable client identity for re-registration dedup.
    pub(crate) client_id: Option<String>,
    pub(crate) runner_group_id: Option<i64>,
    pub(crate) runner_group_name: Option<String>,
    /// Registration presented pool proof (provision token / engine bearer).
    pub(crate) pool_proven: bool,
}

/// Session creation input.
pub(crate) struct CreateSession {
    pub(crate) runner_id: i64,
    pub(crate) protocol: SessionProtocol,
    pub(crate) client_id: Option<String>,
    /// Session crypto material, sealed into the row.
    pub(crate) encryption: Option<preloop_gha_protocol::crypto::SessionEncryption>,
}

/// An expansion apply: the claim's identity plus the build result.
pub(crate) struct ExpansionApply {
    pub(crate) job: QueuedJob,
    /// Generation the node was claimed under — the apply is fenced on it.
    pub(crate) generation: i64,
    pub(crate) built: Result<BuiltExpansion, ExecutionStatus>,
}

/// A request lookup by any of its correlation keys.
#[derive(Debug)]
pub(crate) enum RequestKey {
    Id(i64),
    PlanId(String),
    AgentJobId(uuid::Uuid),
    TimelineId(uuid::Uuid),
}

/// What boot reconcile recovered.
#[derive(Debug, Default)]
pub(crate) struct ReconcileOutcome {
    /// Claims orphaned by the restart that were recovered for retry.
    pub(crate) recovered: usize,
    /// Claims that could not be recovered and were failed.
    pub(crate) failed: usize,
    /// Concurrency holders dropped.
    pub(crate) holders_dropped: usize,
}

// ─────────────────────────────────────────────────────────────────────────
// The concrete backend
// ─────────────────────────────────────────────────────────────────────────

/// The configured control backend: SQLite (default, single node) or
/// Postgres (shared nodes). `AppState` holds `Arc<Backend>` — a concrete
/// enum, not `Arc<dyn ControlBackend>`, so [`Backend::transact`] can stay
/// generic (a generic method is not object-safe over `dyn`).
///
/// `transact` is the cutover escape hatch: any `inner.lock()` site that
/// only touches scheduling fields becomes `backend.transact(|tx| …)` with
/// the same body — the closure runs inside the transaction on the working
/// set. Typed commands ([`ControlBackend`]) cover the shaped operations;
/// `transact` covers everything else until a command earns its own name.
pub(crate) enum Backend {
    /// Single-node default: one writer on a `control.db` file.
    Sqlite(super::sqlite::SqliteBackend),
    /// Shared-node: a connection pool against the `control` schema.
    Postgres(super::postgres::PostgresBackend),
}

impl Backend {
    /// Open the backend selected by `store_url`: `postgres://…` → Postgres,
    /// anything else (`sqlite://<path>`, a bare path) → SQLite at
    /// `<state_dir>/control.db`. The control database is deliberately a
    /// separate file/schema from the legacy `preloop.db` store so the two
    /// never share a table namespace.
    pub(crate) async fn open(
        store_url: Option<&str>,
        state_dir: &std::path::Path,
        cipher: crate::store::Envelope,
        pool_assignments_enabled: bool,
        require_job_assignments: bool,
        runner_liveness_timeout: std::time::Duration,
    ) -> Result<Self, ControlError> {
        if let Some(url) =
            store_url.filter(|u| u.starts_with("postgres://") || u.starts_with("postgresql://"))
        {
            let backend = super::postgres::PostgresBackend::connect(
                url,
                cipher,
                pool_assignments_enabled,
                require_job_assignments,
                runner_liveness_timeout,
            )
            .await?;
            return Ok(Self::Postgres(backend));
        }
        let path = state_dir.join("control.db");
        let backend = super::sqlite::SqliteBackend::open(
            &path,
            cipher,
            pool_assignments_enabled,
            require_job_assignments,
            runner_liveness_timeout,
        )?;
        Ok(Self::Sqlite(backend))
    }

    /// Run `f` inside one transaction on the scheduling working set.
    /// `&self` — serialization lives in the database. The SQLite arm runs
    /// its synchronous transaction inline (a short local `BEGIN
    /// IMMEDIATE`); the Postgres arm awaits its connection.
    pub(crate) async fn transact<T>(
        &self,
        f: impl FnOnce(&mut super::txstate::TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.transact(f),
            Self::Postgres(backend) => backend.transact(f).await,
        }
    }

    /// Run `f` on a read-only pooled connection — concurrent with the
    /// writer, never queued behind it. Use for commands that only read the
    /// working set (`acquire_context`, `run_record`, `queue_stats`, …);
    /// read-modify-write commands go through [`Backend::transact`].
    pub(crate) async fn read<T>(
        &self,
        f: impl FnOnce(&super::txstate::TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.read(f),
            Self::Postgres(backend) => backend.read(f).await,
        }
    }

    /// `read` under an explicit [`TxScope`] — loads only that working set on
    /// a pooled read connection. Hot read paths (`append_log`, `console_log`)
    /// use a narrow scope instead of materializing the full working set.
    pub(crate) async fn read_scoped<T>(
        &self,
        scope: &super::txstate::TxScope,
        f: impl FnOnce(&super::txstate::TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.read_scoped(scope, f),
            Self::Postgres(backend) => backend.read_scoped(scope, f).await,
        }
    }

    /// `transact` under an explicit [`TxScope`] — loads only that working
    /// set. Used by the boot import (full scope) and hot paths that opt into
    /// a narrow scope.
    pub(crate) async fn transact_scoped<T>(
        &self,
        scope: &super::txstate::TxScope,
        f: impl FnOnce(&mut super::txstate::TxState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.transact_scoped(scope, f),
            Self::Postgres(backend) => backend.transact_scoped(scope, f).await,
        }
    }

    /// Find the run a webhook delivery already produced, by its durable
    /// `(delivery_id, workflow_path)` dedup key. O(1) via `runs_delivery` —
    /// the submit path calls this before `transact_scoped` so the replay
    /// check never scans the whole `runs` table.
    pub(crate) async fn find_run_by_delivery(
        &self,
        delivery_id: &str,
        workflow_path: &str,
    ) -> Result<Option<crate::models::RunRecord>, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.find_run_by_delivery(delivery_id, workflow_path),
            Self::Postgres(backend) => {
                backend
                    .find_run_by_delivery(delivery_id, workflow_path)
                    .await
            }
        }
    }

    /// Resolve a request's `(request_id, run_id)` from its `agent_job_id`.
    /// O(1) via `job_requests_agent` — the broker complete path calls this
    /// before `transact_scoped` so the working set stays narrow.
    pub(crate) async fn find_request_by_agent_job_id(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<(i64, RunId)>, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.find_request_by_agent_job_id(agent_job_id),
            Self::Postgres(backend) => backend.find_request_by_agent_job_id(agent_job_id).await,
        }
    }

    /// The session that owns the request for `agent_job_id`. `complete_job`
    /// resolves this before `transact_scoped` so `settle_request` can drop
    /// the moot cancellation from the owner session's inflight messages.
    pub(crate) async fn find_session_by_agent_job_id(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<String>, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.find_session_by_agent_job_id(agent_job_id),
            Self::Postgres(backend) => backend.find_session_by_agent_job_id(agent_job_id).await,
        }
    }

    /// Every session owning a request of `run_id`. `cancel_run`/`cancel_job`
    /// resolve these before `transact_scoped` so `settle_request` can drop
    /// moot cancellations from each owner session's inflight messages.
    pub(crate) async fn find_sessions_by_run(
        &self,
        run_id: RunId,
    ) -> Result<std::collections::BTreeSet<String>, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.find_sessions_by_run(run_id),
            Self::Postgres(backend) => backend.find_sessions_by_run(run_id).await,
        }
    }

    /// `(run_id, owner_session)` for `request_id`. `acquire_context` resolves
    /// this before `read_scoped` so the read stays narrow.
    pub(crate) async fn find_request_context(
        &self,
        request_id: i64,
    ) -> Result<Option<(RunId, Option<String>)>, ControlError> {
        match self {
            Self::Sqlite(backend) => backend.find_request_context(request_id),
            Self::Postgres(backend) => backend.find_request_context(request_id).await,
        }
    }

    /// Update the scheduling config after open. Bootstrap applies the real
    /// server config here once known — `open` only receives the recovered
    /// defaults.
    pub(crate) fn set_config(
        &self,
        pool_assignments_enabled: bool,
        require_job_assignments: bool,
        runner_liveness_timeout: std::time::Duration,
    ) {
        match self {
            Self::Sqlite(backend) => backend.set_config(
                pool_assignments_enabled,
                require_job_assignments,
                runner_liveness_timeout,
            ),
            Self::Postgres(backend) => backend.set_config(
                pool_assignments_enabled,
                require_job_assignments,
                runner_liveness_timeout,
            ),
        }
    }

    /// Current scheduling config — the counterpart to [`Backend::set_config`]
    /// for callers that need to flip one flag without knowing the rest.
    #[cfg(test)]
    pub(crate) fn config(&self) -> (bool, bool, std::time::Duration) {
        match self {
            Self::Sqlite(backend) => backend.config(),
            Self::Postgres(backend) => backend.config(),
        }
    }

    /// One-time legacy→control import, atomic and idempotent.
    ///
    /// Inside a single full-scope writer transaction, seeds the control
    /// schema from a recovered legacy `InnerState` **only if the schema is
    /// empty**. The emptiness check runs inside the same transaction as the
    /// write, so two engines racing a fresh database cannot both seed, and a
    /// restart against a live `control.db` never re-imports stale `preloop.db`
    /// state over committed work.
    ///
    /// Returns `true` when this call performed the import, `false` when the
    /// schema already held rows (nothing written).
    pub(crate) async fn import_from_tx_if_empty(
        &self,
        seed: super::txstate::TxState,
    ) -> Result<bool, ControlError> {
        self.transact_scoped(&super::txstate::TxScope::full(), move |tx| {
            let empty = tx.runs.is_empty()
                && tx.job_requests.is_empty()
                && tx.runners.is_empty()
                && tx.ready_index.is_empty()
                && tx.sessions.is_empty();
            if !empty {
                return Ok(false);
            }
            *tx = seed;
            // A restored concurrency group may name a holder whose run is
            // already terminal (the snapshot predates the completion) or
            // missing entirely; leaving it parks every later submission in
            // that group forever. Reconcile before anything dispatches, then
            // re-promote whatever the freed slots unblock. Runs on `tx` so it
            // sees the fully-populated working set (jobset admissions, run
            // concurrency, holder keys).
            super::sched::reconcile_concurrency_groups(tx);
            super::sched::promote_ready_jobs(tx);
            Ok(true)
        })
        .await
    }
}

#[async_trait::async_trait]
impl ControlBackend for Backend {
    async fn submit_run(&self, submit: SubmitRun) -> Result<SubmitOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.submit_run(submit).await,
            Self::Postgres(b) => b.submit_run(submit).await,
        }
    }
    async fn allocate_run_number(&self, workflow_path: &str) -> Result<u64, ControlError> {
        match self {
            Self::Sqlite(b) => b.allocate_run_number(workflow_path).await,
            Self::Postgres(b) => b.allocate_run_number(workflow_path).await,
        }
    }
    async fn poll_session(&self, poll: PollRequest) -> Result<PollOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.poll_session(poll).await,
            Self::Postgres(b) => b.poll_session(poll).await,
        }
    }
    async fn acquire_context(&self, request_id: i64) -> Result<AcquireContext, ControlError> {
        match self {
            Self::Sqlite(b) => b.acquire_context(request_id).await,
            Self::Postgres(b) => b.acquire_context(request_id).await,
        }
    }
    async fn complete_job(
        &self,
        completion: JobCompletionInput,
    ) -> Result<CompleteOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.complete_job(completion).await,
            Self::Postgres(b) => b.complete_job(completion).await,
        }
    }
    async fn cancel_run(
        &self,
        run_id: RunId,
        reason: Option<String>,
    ) -> Result<CancelOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.cancel_run(run_id, reason).await,
            Self::Postgres(b) => b.cancel_run(run_id, reason).await,
        }
    }
    async fn cancel_job(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<CancelOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.cancel_job(run_id, job_id).await,
            Self::Postgres(b) => b.cancel_job(run_id, job_id).await,
        }
    }
    async fn renew_request(
        &self,
        request_id: i64,
        runner_id: i64,
    ) -> Result<TaskAgentJobRequestRecord, ControlError> {
        match self {
            Self::Sqlite(b) => b.renew_request(request_id, runner_id).await,
            Self::Postgres(b) => b.renew_request(request_id, runner_id).await,
        }
    }
    async fn request(&self, key: RequestKey) -> Result<TaskAgentJobRequestRecord, ControlError> {
        match self {
            Self::Sqlite(b) => b.request(key).await,
            Self::Postgres(b) => b.request(key).await,
        }
    }
    async fn release_request(&self, request_id: i64) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.release_request(request_id).await,
            Self::Postgres(b) => b.release_request(request_id).await,
        }
    }
    async fn register_runner(&self, reg: RegisterRunner) -> Result<RunnerRow, ControlError> {
        match self {
            Self::Sqlite(b) => b.register_runner(reg).await,
            Self::Postgres(b) => b.register_runner(reg).await,
        }
    }
    async fn create_session(&self, session: CreateSession) -> Result<SessionRow, ControlError> {
        match self {
            Self::Sqlite(b) => b.create_session(session).await,
            Self::Postgres(b) => b.create_session(session).await,
        }
    }
    async fn delete_session(&self, session_id: &str) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.delete_session(session_id).await,
            Self::Postgres(b) => b.delete_session(session_id).await,
        }
    }
    async fn purge_runner(&self, runner_id: i64) -> Result<(), ControlError> {
        match self {
            Self::Sqlite(b) => b.purge_runner(runner_id).await,
            Self::Postgres(b) => b.purge_runner(runner_id).await,
        }
    }
    async fn claim_expansion(&self) -> Result<Option<ExpansionClaim>, ControlError> {
        match self {
            Self::Sqlite(b) => b.claim_expansion().await,
            Self::Postgres(b) => b.claim_expansion().await,
        }
    }
    async fn apply_expansion(
        &self,
        claim: ExpansionApply,
    ) -> Result<SchedulingOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.apply_expansion(claim).await,
            Self::Postgres(b) => b.apply_expansion(claim).await,
        }
    }
    async fn reconcile_on_boot(&self) -> Result<ReconcileOutcome, ControlError> {
        match self {
            Self::Sqlite(b) => b.reconcile_on_boot().await,
            Self::Postgres(b) => b.reconcile_on_boot().await,
        }
    }
    async fn run_record(&self, run_id: RunId) -> Result<RunRecord, ControlError> {
        match self {
            Self::Sqlite(b) => b.run_record(run_id).await,
            Self::Postgres(b) => b.run_record(run_id).await,
        }
    }
    async fn queue_stats(&self) -> Result<QueueStats, ControlError> {
        match self {
            Self::Sqlite(b) => b.queue_stats().await,
            Self::Postgres(b) => b.queue_stats().await,
        }
    }
    async fn live_assignments(
        &self,
    ) -> Result<Vec<preloop_observability::status::RunnerAssignment>, ControlError> {
        match self {
            Self::Sqlite(b) => b.live_assignments().await,
            Self::Postgres(b) => b.live_assignments().await,
        }
    }
}
