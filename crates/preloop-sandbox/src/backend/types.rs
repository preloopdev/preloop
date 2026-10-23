//! Command and result types shared by every [`SandboxBackend`] implementation.
//!
//! These are domain values, not wire messages: a command carries everything a
//! short SQL transaction needs to decide, and a result carries the durable
//! outcome the service projects onto the API. Nothing here is an in-memory
//! working set.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::api::*;
use crate::error::{Code, Error};

/// A row payload paired with its authoritative version.
///
/// The version is assigned from the database's global monotonic sequence on
/// every mutation and is never reused, so a cache keyed by
/// `(kind, id, version)` can never serve an older incarnation after a delete
/// and recreate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Versioned<T> {
    /// The row at exactly `version`.
    pub value: T,
    /// Monotonic row version.
    pub version: u64,
}

/// One listing entry: stable id and authoritative row version, no payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceMeta {
    /// Template or pool name.
    pub id: String,
    /// Authoritative row version.
    pub version: u64,
}

/// Resource families exposed to the read/version surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    /// A template.
    Template,
    /// A pool.
    Pool,
    /// A sandbox.
    Sandbox,
    /// A provider.
    Provider,
    /// A provider driver capability record (versioned by generation).
    Driver,
}

impl ResourceKind {
    /// Stable storage label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Template => "template",
            Self::Pool => "pool",
            Self::Sandbox => "sandbox",
            Self::Provider => "provider",
            Self::Driver => "driver",
        }
    }
}

/// Which durable runtime handle a node owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerKind {
    /// An exec output stream.
    Exec,
    /// A transfer ticket.
    Ticket,
}

/// The node/session that owns a live channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owner {
    /// Node id.
    pub node_id: String,
    /// Node session (a restart gets a new one).
    pub session_id: String,
}

/// A peer node as configured locally; never taken from a request body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerConfig {
    /// Peer node id.
    pub node_id: String,
    /// Peer base URL, e.g. `https://10.0.0.2:8443`.
    pub url: String,
}

/// Backend construction options.
#[derive(Debug, Clone)]
pub struct BackendConfig {
    /// Store URL: `postgres://…`, `sqlite://<path>`, a bare path, or
    /// `memory:` (a real SQLite `:memory:` database).
    pub url: String,
    /// State directory used for the default seal-key file.
    pub state_dir: std::path::PathBuf,
    /// 32-byte envelope key; `None` loads or creates
    /// `<state_dir>/sandbox-seal.key`.
    pub seal_key: Option<[u8; 32]>,
    /// Postgres connection pool ceiling (ignored by SQLite).
    pub pool_size: usize,
    /// This service node's id.
    pub node_id: String,
    /// This process's node session.
    pub session_id: String,
    /// Hash of this node's credential (the credential itself is not stored).
    pub node_credential_sha256: String,
    /// This node's advertised base URL, for the node registry.
    pub peer_url: String,
    /// How long a node may go unheard from before its channels are reaped.
    pub node_lost_after: Duration,
}

/// One entry of the placement snapshot: a provider and how many live
/// sandboxes it holds per driver.
#[derive(Debug, Clone, Default)]
pub struct PlacementSnapshot {
    /// Registered providers.
    pub providers: Vec<Provider>,
    /// Live (non-terminal) sandbox count per `(provider id, driver)`.
    pub live: BTreeMap<(String, String), u32>,
}

/// A resolved placement the service wants turned into a sandbox.
#[derive(Debug, Clone)]
pub struct NewSandbox {
    /// Template the sandbox starts from.
    pub template: Template,
    /// Pool name (empty for a direct template acquire).
    pub pool: String,
    /// Pool build revision at selection time.
    pub pool_revision: u64,
    /// Why the sandbox exists.
    pub purpose: Purpose,
    /// Caller-chosen live name (empty when unnamed).
    pub name: String,
    /// Selected provider.
    pub provider_id: String,
    /// Selected driver.
    pub driver: String,
    /// Requested shape.
    pub shape: Shape,
    /// Requested egress.
    pub network: NetworkPolicy,
    /// Requested host mounts.
    pub mounts: Vec<Mount>,
    /// Requested socket mounts.
    pub sockets: Vec<SocketMount>,
    /// Driver options.
    pub options: StringMap,
    /// Extra requirements.
    pub requires: Requirements,
    /// Whether the driver must be snapshot capable.
    pub snapshot_capable: bool,
}

/// Input to `Acquire`.
#[derive(Debug, Clone)]
pub struct AcquireCommand {
    /// The caller's request.
    pub request: AcquireRequest,
    /// Fingerprint of the fields that decide which sandbox is acquired.
    pub fingerprint: String,
    /// Lease TTL to record as the caller's requested TTL.
    pub ttl_secs: u64,
    /// Cap on an undelivered lease's expiry: a caller whose provisioning
    /// outlasts a short TTL still gets its lease once the machine is ready.
    pub unclaimed_ttl_secs: u64,
    /// Create deadline, for the work item.
    pub create_timeout: Duration,
    /// Placement to use when no warm member can be claimed. The service
    /// computes it from a [`PlacementSnapshot`]; `None` asks the backend to
    /// report whether one is needed.
    pub placement: Option<NewSandbox>,
    /// This node/session, for durable waiter and injection claims.
    pub owner: Owner,
}

/// What `Acquire` decided.
#[derive(Debug, Clone)]
pub enum AcquireStep {
    /// The lease is ready; no injection is owed.
    Ready {
        /// Active lease.
        lease: Lease,
        /// Its sandbox.
        sandbox: Sandbox,
    },
    /// The caller owns the injection claim and must upload `inject.files`,
    /// then call [`SandboxBackend::finish_injection`].
    Inject {
        /// Active lease.
        lease: Lease,
        /// Its sandbox.
        sandbox: Sandbox,
    },
    /// Another replica is injecting this lease (or it is provisioning).
    /// Re-check after the hint.
    Wait {
        /// Error to report if the caller's deadline expires.
        last: Option<Error>,
        /// Suggested recheck delay.
        retry_after: Duration,
        /// Sandbox to register a durable waiter for, when one exists.
        sandbox_id: Option<String>,
        /// Lease to register a durable waiter for, when one exists.
        lease_id: Option<String>,
    },
    /// No warm member exists; the service must rank a placement and call
    /// `acquire` again with `placement`.
    PlacementNeeded,
    /// A committed domain failure (cleanup already happened). The service
    /// returns this to the caller without waiting.
    Failed(Error),
}

/// Outcome of one pool-refill create.
#[derive(Debug, Clone)]
pub enum RefillOutcome {
    /// A member was created and its create work queued.
    Created(Sandbox),
    /// The pool is already at its warm/max target or its revision moved.
    NotNeeded,
    /// Every candidate is at its driver slot cap.
    Full,
}

/// An exec handle created by the service before queueing its work.
#[derive(Debug, Clone)]
pub struct ExecHandle {
    /// Exec id.
    pub exec_id: String,
    /// Work item id.
    pub work_id: String,
    /// Absolute deadline in epoch seconds.
    pub deadline: i64,
    /// Provider the work targets.
    pub provider_id: String,
}

/// Result of validating a `ReportExec` batch at the owning node.
#[derive(Debug, Clone)]
pub enum ReportExecOutcome {
    /// Apply these events locally. `terminal` means the exit event was
    /// included and the durable exec is now finished.
    Events {
        /// Exec the events belong to.
        exec_id: String,
        /// Events in order.
        events: Vec<ExecEvent>,
        /// Whether the batch contained the exit event.
        terminal: bool,
    },
    /// A batch the service already applied (a retry).
    Duplicate,
    /// A different delivery tried to start an exec that already started.
    /// The exec is durably failed; the caller's stream must end with `reason`
    /// rather than replay the command.
    AlreadyStarted {
        /// Exec whose start conflicted.
        exec_id: String,
        /// Explanation for the caller.
        reason: String,
    },
    /// The exec is gone or belongs to another provider; the agent must stop.
    Gone,
}

/// A durable work result as stored by `Report`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkResult {
    /// Whether the operation succeeded.
    pub ok: bool,
    /// Native reference on success.
    pub native_ref: String,
    /// Reported error on failure.
    pub error: String,
    /// Snapshot receipt: the template this work created, retained after the
    /// source sandbox and lease were consumed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<Template>,
}

impl WorkResult {
    /// Success with a native reference.
    pub fn ok(native_ref: impl Into<String>) -> Self {
        Self {
            ok: true,
            native_ref: native_ref.into(),
            error: String::new(),
            template: None,
        }
    }

    /// Failure with a reported error.
    pub fn failed(error: impl Into<String>) -> Self {
        Self {
            ok: false,
            native_ref: String::new(),
            error: error.into(),
            template: None,
        }
    }

    /// Convert into a service result.
    pub fn into_result(self) -> crate::error::Result<String> {
        if self.ok {
            Ok(self.native_ref)
        } else {
            Err(crate::error::from_reported(self.error))
        }
    }
}

/// A transfer ticket's durable metadata (never its token or bytes).
#[derive(Debug, Clone)]
pub struct TicketRecord {
    /// Ticket id.
    pub id: String,
    /// Sandbox the transfer targets.
    pub sandbox_id: String,
    /// Lease that was active when the ticket was issued.
    pub lease_id: String,
    /// Direction.
    pub kind: StreamKind,
    /// Guest path.
    pub guest_path: String,
    /// Expiry (epoch seconds).
    pub expires_at: i64,
    /// Producer claimed.
    pub producer_claimed: bool,
    /// Consumer claimed.
    pub consumer_claimed: bool,
    /// `open`, `ready`, `aborted` or `done`.
    pub status: String,
    /// Owning node.
    pub owner: Owner,
}

/// Result of a durable ticket claim.
#[derive(Debug, Clone)]
pub struct TicketClaim {
    /// Ticket metadata.
    pub ticket: TicketRecord,
    /// This claim took the last free side.
    pub both_claimed: bool,
}

/// Which end of a transfer a durable claim names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TicketSide {
    /// Supplies bytes.
    Producer,
    /// Consumes bytes.
    Consumer,
}

impl TicketSide {
    /// Storage label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Producer => "producer",
            Self::Consumer => "consumer",
        }
    }
}

/// Credentials a ticket claim must prove.
#[derive(Debug, Clone)]
pub enum TicketAuthority<'a> {
    /// A caller presenting the ticket's bearer token.
    Caller {
        /// Presented token.
        token: &'a str,
    },
    /// A provider presenting its certificate-derived identity and generation.
    Provider {
        /// Provider id.
        provider_id: &'a str,
        /// Provider generation.
        generation: u64,
    },
    /// The service's own in-process side (upload/download helpers).
    Internal,
}

/// Outcome of a join-token exchange.
#[derive(Debug, Clone)]
pub enum EnrollOutcome {
    /// The token was consumed and the provider's certificate bound.
    Issued,
    /// This exact enrollment already succeeded; the stored certificate is
    /// returned so a caller whose response was lost can retry.
    Replay {
        /// The certificate issued by the original enrollment.
        cert_pem: String,
        /// Its expiry.
        expires_at: i64,
    },
}

/// Outcome of starting a snapshot request.
#[derive(Debug, Clone)]
pub enum SnapshotStart {
    /// The work was queued (or is already in flight); wait for its result.
    Queued {
        /// Work id.
        work_id: String,
    },
    /// A completed receipt for this request: the stored template.
    Replay(Template),
}

/// A power operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PowerOp {
    /// Stop the machine.
    Stop,
    /// Start it, optionally as a fork base.
    Start {
        /// Whether to start it forkable.
        forkable: bool,
    },
    /// Pause.
    Pause,
    /// Resume.
    Resume,
}

impl PowerOp {
    /// Storage label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Start { .. } => "start",
            Self::Pause => "pause",
            Self::Resume => "resume",
        }
    }
}

/// Tuning values a maintenance pass needs.
#[derive(Debug, Clone)]
pub struct MaintenanceConfig {
    /// A create not reported within this window fails.
    pub create_timeout: Duration,
    /// A provider silent this long is not placed on.
    pub provider_timeout: Duration,
    /// Multiplier after which a silent provider is lost.
    pub provider_lost_factor: u32,
    /// How long an exec may pass its deadline before it is swept.
    pub exec_sweep_grace: Duration,
    /// Maximum destroy requeues per terminating record.
    pub max_destroy_attempts: u32,
    /// How long a node may go unheard from before its channels are reaped.
    pub node_lost_after: Duration,
    /// How long an injection claim is valid.
    pub injection_timeout: Duration,
    /// How long an acquire waiter is valid without renewal.
    pub waiter_timeout: Duration,
}

/// Work a maintenance pass requires the service to do outside SQL.
#[derive(Debug, Clone, Default)]
pub struct MaintenanceOutcome {
    /// Pools whose warm target is below its current depth.
    pub pool_deficits: Vec<PoolDeficit>,
    /// Exec ids whose local stream must be closed with a failure.
    pub failed_execs: Vec<(String, String)>,
    /// Tickets whose local rendezvous must be aborted.
    pub aborted_tickets: Vec<String>,
    /// Whether any row changed (a local wake hint).
    pub changed: bool,
}

/// One pool's refill need.
#[derive(Debug, Clone)]
pub struct PoolDeficit {
    /// Pool name.
    pub pool: String,
    /// Template name.
    pub template: String,
    /// Current build revision.
    pub revision: u64,
    /// How many members to add.
    pub missing: u32,
}

/// One observed provider's durable facts.
#[derive(Debug, Clone)]
pub struct ProviderStatsRow {
    /// The provider row.
    pub provider: Provider,
    /// Last host sample, if any.
    pub host: HostSample,
    /// Age of the sample in seconds.
    pub host_sample_age_secs: i64,
    /// Latest per-sandbox runtime facts.
    pub sandbox_stats: Vec<SandboxRuntimeStats>,
}

/// Error for a database operation, preserving uniqueness classification.
pub(crate) fn unique_violation(error: &str) -> Error {
    Error::new(
        Code::AlreadyExists,
        format!("durable sandbox state conflict: {error}"),
    )
}
