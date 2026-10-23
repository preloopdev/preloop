//! The `SandboxBackend` contract: every durable sandbox mutation is one typed
//! command with transactional authority, idempotency and fencing.
//!
//! The service holds an `Arc<dyn SandboxBackend>` and never sees a SQL string,
//! transaction handle or mutable record map. Each command runs as one short
//! transaction of targeted SQL; multi-row decisions use conditional updates
//! and the shared pure functions in [`crate::backend::logic`]. SQLite and
//! Postgres implement the identical contract and the shared suite in
//! `backend::tests` proves it.
//!
//! Lock order for commands that touch more than one table is fixed:
//! `pools` → `providers` → `sandboxes` → `leases` → `work_items` → runtime
//! handles (`execs`, `tickets`). No command takes a global scheduler lock.

use std::time::Duration;

use async_trait::async_trait;

use crate::api::*;
use crate::error::Result;
use crate::service::ProviderIdentity;

use super::types::*;

/// Durable sandbox state.
#[async_trait]
pub trait SandboxBackend: Send + Sync {
    /// Schema version and fingerprints this backend opened with.
    async fn schema_info(&self) -> Result<SchemaInfo>;

    /// Record the shared TLS CA fingerprint, failing when another node
    /// registered a different one. Idempotent.
    async fn set_trust_fingerprint(&self, fingerprint: &str) -> Result<()>;

    /// Cheap liveness check.
    async fn health(&self) -> Result<()>;

    /// Stop background listeners and close pooled connections.
    async fn close(&self);

    // ── Read/version surface (the cache contract) ─────────────────

    /// A template payload and its authoritative version in one read.
    async fn template_get(&self, name: &str) -> Result<Option<Versioned<Template>>>;

    /// A pool payload and its authoritative row version in one read.
    async fn pool_get(&self, name: &str) -> Result<Option<Versioned<PoolSpec>>>;

    /// The pool's build revision (template digest + build inputs, excluding
    /// scaling and TTL). Distinct from the row version.
    async fn pool_revision(&self, name: &str) -> Result<Option<u64>>;

    /// One provider driver capability record, versioned by provider
    /// generation.
    async fn driver_get(
        &self,
        provider_id: &str,
        driver: &str,
    ) -> Result<Option<Versioned<DriverInfo>>>;

    /// Cheap authoritative version read, `None` when the row is absent or
    /// retired.
    async fn resource_version(&self, kind: ResourceKind, id: &str) -> Result<Option<u64>>;

    /// A page of template ids and versions without payloads. `has_more`
    /// reports whether another page follows; ordering is by name.
    async fn template_page_meta(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<ResourceMeta>, bool)>;

    /// A page of pool ids and versions without payloads.
    async fn pool_page_meta(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<ResourceMeta>, bool)>;

    /// The template payload at exactly `version`, or `None` when that version
    /// no longer exists (the caller re-reads metadata).
    async fn template_get_at(&self, name: &str, version: u64)
    -> Result<Option<Versioned<Template>>>;

    /// The pool payload at exactly `version`.
    async fn pool_get_at(&self, name: &str, version: u64)
    -> Result<Option<Versioned<PoolSpec>>>;

    // ── Templates and pools ───────────────────────────────────────

    /// `PutTemplate`, idempotent for an identical digest.
    async fn put_template(&self, spec: TemplateSpec) -> Result<Template>;

    /// `DeleteTemplate`; refuses while a pool references it. Snapshot
    /// templates are retired only after every queued create that embeds them
    /// finishes.
    async fn delete_template(&self, name: &str) -> Result<()>;

    /// One `ListTemplates` page, ordered by name.
    async fn list_templates(&self, request: &ListRequest) -> Result<ListTemplatesResponse>;

    /// Every live template, ordered by name.
    async fn list_templates_all(&self) -> Result<Vec<Template>>;

    /// `PutPool`.
    async fn put_pool(&self, pool: PoolSpec) -> Result<PoolSpec>;

    /// `DeletePool`; idle members are destroyed.
    async fn delete_pool(&self, name: &str) -> Result<()>;

    /// One `ListPools` page, ordered by name.
    async fn list_pools(&self, request: &ListRequest) -> Result<ListPoolsResponse>;

    /// Every pool, ordered by name.
    async fn list_pools_all(&self) -> Result<Vec<PoolSpec>>;

    // ── Placement and leases ──────────────────────────────────────

    /// Providers plus live sandbox counts for one placement decision.
    async fn placement_snapshot(&self) -> Result<PlacementSnapshot>;

    /// `Acquire`'s transactional decision.
    async fn acquire(&self, command: AcquireCommand) -> Result<AcquireStep>;

    /// Create one warm-pool member, re-checking warm/max and driver slots.
    async fn create_pool_member(
        &self,
        member: NewSandbox,
        create_timeout: Duration,
    ) -> Result<RefillOutcome>;

    /// Mark the caller's injection claim complete (or failed, freeing it).
    async fn finish_injection(&self, lease_id: &str, owner: &Owner, ok: bool) -> Result<()>;

    /// `Renew`.
    async fn renew(&self, lease_id: &str, ttl_secs: u64, default_ttl_secs: u64) -> Result<Lease>;

    /// `Release`.
    async fn release(
        &self,
        lease_id: &str,
        mode: ReleaseMode,
        preserve_ttl_secs: u64,
        default_preserve_secs: u64,
    ) -> Result<()>;

    /// `Get`, authorized by the lease capability.
    async fn get(&self, id: &str, lease_id: &str) -> Result<Sandbox>;

    /// One `List` page with lease ids redacted.
    async fn list(&self, request: &ListRequest) -> Result<ListSandboxesResponse>;

    /// Every sandbox with lease ids redacted.
    async fn list_all(&self) -> Result<Vec<Sandbox>>;

    /// The lease's injected env, unsealed from the durable envelope.
    async fn lease_env(&self, lease_id: &str) -> Result<StringMap>;

    /// Register/renew this acquire's durable waiter so another replica's
    /// reapers do not expire a sandbox it is still waiting for.
    async fn register_waiter(&self, waiter: Waiter) -> Result<()>;

    /// Drop this acquire's waiter.
    async fn clear_waiter(&self, key: &str, owner: &Owner) -> Result<()>;

    // ── Providers and enrollment ──────────────────────────────────

    /// `Register`.
    async fn register(
        &self,
        request: RegisterRequest,
        enrolled: Option<&str>,
        owner: &Owner,
    ) -> Result<RegisterResponse>;

    /// `Heartbeat` (also refreshes durable liveness).
    async fn heartbeat(&self, request: HeartbeatRequest) -> Result<()>;

    /// `CordonProvider` / `UncordonProvider`.
    async fn set_cordon(&self, reference: &str, cordoned: bool) -> Result<Provider>;

    /// `DrainProvider`.
    async fn drain(&self, reference: &str) -> Result<DrainProviderResult>;

    /// `DeleteProvider`.
    async fn delete_provider(&self, name: &str) -> Result<()>;

    /// One `ListProviders` page.
    async fn list_providers(&self, request: &ListRequest) -> Result<ListProvidersResponse>;

    /// Every provider.
    async fn list_providers_all(&self) -> Result<Vec<Provider>>;

    /// The provider a client certificate fingerprint authenticates.
    async fn provider_identity(&self, fingerprint_sha256: &str) -> Result<ProviderIdentity>;

    /// Store a single-use join token's hash.
    async fn create_join_token(&self, id: &str, secret_sha256: &str, expires_at: i64)
    -> Result<()>;

    /// Atomically consume a join token and bind the provider certificate.
    async fn enroll(
        &self,
        token_id: &str,
        secret_sha256: &str,
        name: &str,
        cert_sha256: &str,
        cert_expires_at: i64,
    ) -> Result<()>;

    /// Add a renewed certificate for an enrolled fingerprint.
    async fn renew_certificate(
        &self,
        fingerprint_sha256: &str,
        cert_sha256: &str,
        cert_expires_at: i64,
    ) -> Result<()>;

    // ── Durable work ──────────────────────────────────────────────

    /// Claim up to `batch` queued items for a fenced provider generation.
    /// Returned `WorkItem.id` is the delivery identity (durable work id plus
    /// attempt); `Report` echoes it.
    async fn poll_work(
        &self,
        provider_id: &str,
        generation: u64,
        batch: usize,
        delivery_timeout: Duration,
    ) -> Result<Vec<WorkItem>>;

    /// The stored result of a completed work item, if any.
    async fn work_result(&self, work_id: &str) -> Result<Option<WorkResult>>;

    /// `Report`; applies the durable state transition in one transaction.
    async fn report(&self, request: ReportRequest) -> Result<()>;

    /// `Reconcile`.
    async fn reconcile(&self, request: ReconcileRequest) -> Result<ReconcileResponse>;

    /// Queue one Copy work item for an already-issued ticket. Returns its
    /// work id; the result is read with `work_result`.
    async fn start_copy(
        &self,
        sandbox_id: &str,
        ticket: &str,
        guest_path: &str,
        kind: StreamKind,
        owner: &Owner,
    ) -> Result<String>;

    // ── Execs ─────────────────────────────────────────────────────

    /// Validate and create an exec plus its queued work.
    async fn create_exec(
        &self,
        exec_id: &str,
        request: ExecRequest,
        timeout_secs: u64,
        owner: &Owner,
    ) -> Result<ExecHandle>;

    /// Validate a provider's output batch at the owning node.
    async fn report_exec(&self, request: ReportExecRequest) -> Result<ReportExecOutcome>;

    /// Record a terminal exit code (idempotent).
    async fn finish_exec(&self, exec_id: &str, code: i32) -> Result<()>;

    /// Fail a running exec and queue its cancellation.
    async fn fail_exec(&self, exec_id: &str, reason: &str) -> Result<()>;

    /// The caller went away: drop the exec and queue a cancellation.
    async fn cancel_exec(&self, exec_id: &str, owner: &Owner) -> Result<()>;

    /// The durable state of one exec.
    async fn exec_status(&self, exec_id: &str) -> Result<Option<ExecStatus>>;

    // ── Transfer tickets ──────────────────────────────────────────

    /// Insert a ticket bound to an active lease. The token hash is stored,
    /// never the token.
    #[allow(clippy::too_many_arguments)]
    async fn create_ticket(
        &self,
        id: &str,
        sandbox_id: &str,
        lease_id: &str,
        kind: StreamKind,
        guest_path: &str,
        token_sha256: &str,
        expires_at: i64,
        owner: &Owner,
    ) -> Result<()>;

    /// Atomically claim one side after proving caller token, provider
    /// generation and the still-active lease.
    async fn claim_ticket(
        &self,
        id: &str,
        side: TicketSide,
        expected_kind: StreamKind,
        claim_ttl_secs: i64,
        authority: TicketAuthority<'_>,
    ) -> Result<TicketClaim>;

    /// Mark a ticket finished (`done`) or failed (`aborted`).
    async fn finish_ticket(&self, id: &str, status: &str, reason: &str) -> Result<()>;

    /// The owning node/session of a live ticket.
    async fn ticket_owner(&self, id: &str) -> Result<Option<Owner>>;

    // ── Snapshot, power, stats ────────────────────────────────────

    /// Validate and queue a snapshot, or replay a completed receipt.
    async fn snapshot_start(&self, command: SnapshotCommand) -> Result<SnapshotStart>;

    /// Validate and queue a power operation; returns its work id.
    async fn power(
        &self,
        sandbox_id: &str,
        lease_id: &str,
        op: PowerOp,
        timeout: Duration,
    ) -> Result<String>;

    /// Aggregated provider/sandbox stats.
    async fn stats(&self) -> Result<StatsResponse>;

    // ── Node sessions (live-channel routing) ──────────────────────

    /// Register this node/session and fence the previous session of the same
    /// node.
    async fn node_register(&self, config: &BackendConfig) -> Result<()>;

    /// Refresh this node's liveness.
    async fn node_heartbeat(&self, node_id: &str, session_id: &str) -> Result<()>;

    /// Verify a peer's node id, session and credential hash.
    async fn node_authenticate(
        &self,
        node_id: &str,
        session_id: &str,
        credential_sha256: &str,
    ) -> Result<bool>;

    /// Mark a session dead and fail its live channels.
    async fn node_close(&self, node_id: &str, session_id: &str) -> Result<()>;

    /// The node/session owning a live exec or ticket.
    async fn owner_of(&self, kind: OwnerKind, id: &str) -> Result<Option<Owner>>;

    // ── Maintenance ──────────────────────────────────────────────

    /// Run one leaderless reaper pass.
    async fn maintain(&self, config: &MaintenanceConfig) -> Result<MaintenanceOutcome>;
}

/// Schema identity read at open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaInfo {
    /// Schema version.
    pub version: u64,
    /// Fingerprint of the envelope key (hex SHA-256).
    pub seal_fingerprint: String,
    /// Fingerprint of the shared TLS CA, once registered.
    pub trust_fingerprint: Option<String>,
}

/// One acquire waiter's durable claim.
#[derive(Debug, Clone)]
pub struct Waiter {
    /// Idempotency key.
    pub key: String,
    /// Sandbox being waited for.
    pub sandbox_id: String,
    /// Lease, once one exists.
    pub lease_id: String,
    /// This node/session.
    pub owner: Owner,
    /// Absolute expiry (epoch milliseconds).
    pub expires_at_ms: i64,
}

/// Input to [`SandboxBackend::snapshot_start`].
#[derive(Debug, Clone)]
pub struct SnapshotCommand {
    /// Source sandbox.
    pub sandbox_id: String,
    /// New template name.
    pub name: String,
    /// Source lease capability.
    pub lease_id: String,
    /// Stable retry identity supplied by the caller.
    pub idempotency_key: String,
    /// This node/session, for the work's origin.
    pub owner: Owner,
    /// How long the provider has to report.
    pub timeout: Duration,
}

/// The durable state of an exec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecStatus {
    /// Running.
    Running,
    /// Finished with this exit code.
    Done {
        /// Exit code.
        code: i32,
    },
    /// Failed; the caller gets this reason.
    Failed {
        /// Failure reason.
        reason: String,
    },
}
