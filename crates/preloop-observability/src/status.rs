#![allow(missing_docs)]

//!OperationalSnapshot and supporting types.
//!

use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Overall
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Overall {
    #[default]
    Ok,
    Degraded,
    Blocked,
    ShuttingDown,
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceSnapshot {
    pub version: String,
    pub instance_id: String,
    pub uptime_seconds: u64,
    pub shutdown_requested: bool,
}

// ---------------------------------------------------------------------------
// Runs / Jobs / Concurrency / Scheduler / Runners
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunsSnapshot {
    pub queued: u32,
    pub in_progress: u32,
    pub completed: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobsSnapshot {
    pub ready: u32,
    pub dependency_blocked: u32,
    pub concurrency_blocked: u32,
    pub pending_expansion: u32,
    pub expanding: u32,
    pub claimable: u32,
    pub unclaimable: u32,
    pub oldest_ready_seconds: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConcurrencySnapshot {
    pub groups_active: u32,
    pub groups_contended: u32,
    pub pending_holders: u32,
    pub deepest_group_pending: u32,
    pub queue_max_pending: usize,
    pub overflow_cancellations: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchedulerSnapshot {
    pub enabled: bool,
    pub schedules: u32,
    pub last_scan_at: Option<DateTime<Utc>>,
    pub next_fire_at: Option<DateTime<Utc>>,
    pub fired: u64,
    pub skipped_overlapping: u64,
    pub late_fires: u64,
    pub max_fire_delay_seconds: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunnersSnapshot {
    pub registered: u32,
    pub sessions: u32,
    pub idle: u32,
    pub busy: u32,
    pub stale: u32,
    pub max_poll_age_seconds: Option<f64>,
    pub max_lease_age_seconds: Option<f64>,
}

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolMode {
    #[default]
    Warm,
    OnDemand,
    External,
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolSnapshot {
    pub mode: PoolMode,
    pub desired: u32,
    pub preparing: bool,
    pub building: u32,
    pub provisioning: u32,
    pub idle: u32,
    pub busy: u32,
    pub paused: u32,
    pub consecutive_provision_failures: u32,
    pub last_transition_at: Option<DateTime<Utc>>,
    /// Consolidated queue depth (server -> pool signal, now via PoolStatus).
    #[serde(default)]
    pub queue_depth: u32,
    /// Next job labels for golden selection (server -> pool).
    #[serde(default)]
    pub next_job_runs_on: Vec<String>,
    /// Pending provision token count (pool -> server).
    #[serde(default)]
    pub pending_registrations: u32,
}

impl Default for PoolSnapshot {
    fn default() -> Self {
        Self {
            mode: PoolMode::Warm,
            desired: 0,
            preparing: false,
            building: 0,
            provisioning: 0,
            idle: 0,
            busy: 0,
            paused: 0,
            consecutive_provision_failures: 0,
            last_transition_at: None,
            queue_depth: 0,
            next_job_runs_on: Vec::new(),
            pending_registrations: 0,
        }
    }
}

/// Shared handle that the pool updates and the sampler reads.
///
/// Consolidates the four ad-hoc `Option<Arc<…>>` handles. Single writer (pool)
/// + multiple readers (sampler, status) — cheap `RwLock`.
#[derive(Debug, Clone, Default)]
pub struct PoolStatus {
    inner: Arc<RwLock<PoolSnapshot>>,
    /// One-time provision tokens (separate from snapshot to avoid cloning large map on every snapshot).
    pending_tokens: Arc<RwLock<std::collections::BTreeMap<String, std::time::SystemTime>>>,
}

impl PoolStatus {
    pub fn new(snapshot: PoolSnapshot) -> Self {
        Self {
            inner: Arc::new(RwLock::new(snapshot)),
            pending_tokens: Arc::new(RwLock::new(std::collections::BTreeMap::new())),
        }
    }

    pub fn snapshot(&self) -> PoolSnapshot {
        let mut snap = self.inner.read().clone();
        snap.pending_registrations = self.pending_tokens.read().len() as u32;
        snap
    }

    pub fn set_desired(&self, desired: u32) {
        self.inner.write().desired = desired;
    }

    pub fn set_preparing(&self, preparing: bool) {
        self.inner.write().preparing = preparing;
    }

    /// Mirror one pool count into the snapshot. Per-field setters, not
    /// `set_counts`, so a RAII guard updating only the count it owns cannot
    /// clobber the fields another guard just wrote.
    pub fn set_idle(&self, idle: u32) {
        self.inner.write().idle = idle;
    }

    pub fn set_building(&self, building: u32) {
        self.inner.write().building = building;
    }

    pub fn set_provisioning(&self, provisioning: u32) {
        self.inner.write().provisioning = provisioning;
    }

    pub fn set_counts(&self, idle: u32, busy: u32, building: u32, provisioning: u32, paused: u32) {
        let mut g = self.inner.write();
        g.idle = idle;
        g.busy = busy;
        g.building = building;
        g.provisioning = provisioning;
        g.paused = paused;
    }

    pub fn record_provision_failure(&self) {
        self.inner.write().consecutive_provision_failures += 1;
    }

    pub fn clear_provision_failures(&self) {
        self.inner.write().consecutive_provision_failures = 0;
    }

    pub fn set_queue_depth(&self, depth: u32) {
        self.inner.write().queue_depth = depth;
    }

    pub fn set_next_job_runs_on(&self, labels: Vec<String>) {
        self.inner.write().next_job_runs_on = labels;
    }

    pub fn insert_pending(&self, token: String, at: std::time::SystemTime) {
        self.pending_tokens.write().insert(token, at);
        // `snapshot()` derives `pending_registrations` from `pending_tokens`;
        // writing it here too would be dead state plus a lock-order coupling
        // between the two guards.
    }

    pub fn remove_pending(&self, token: &str) -> bool {
        self.pending_tokens.write().remove(token).is_some()
    }

    pub fn pending_tokens_snapshot(
        &self,
    ) -> std::collections::BTreeMap<String, std::time::SystemTime> {
        self.pending_tokens.read().clone()
    }
}

// ---------------------------------------------------------------------------
// VMs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VmSource {
    CgroupV2,
    Process,
    Mixed,
    #[default]
    Unavailable,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VmFleetSnapshot {
    pub source: VmSource,
    pub sample_age_seconds: Option<f64>,
    pub capabilities: HashMap<String, bool>,
    pub count: VmCount,
    pub configured: VmConfigured,
    pub host_usage: VmHostUsage,
    pub top_consumers: Vec<VmTopConsumer>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VmCount {
    pub runner: u32,
    pub golden: u32,
    pub unavailable: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VmConfigured {
    pub vcpus: u32,
    pub memory_bytes: u64,
    pub storage_bytes: u64,
    pub overlay_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VmHostUsage {
    /// `None` means "not measured yet" — absent in JSON, not a real zero.
    /// A consumer of `/api/v1/status` must not read an idle fleet from an
    /// unmeasured one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_cores: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sparse_disk_allocated_bytes: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VmTopConsumer {
    pub machine_name: String,
    pub role: String,
    pub activity: String,
    pub cpu_cores: f64,
    pub memory_bytes: u64,
    pub sparse_disk_allocated_bytes: u64,
}

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Store / Storage / Github / Debug / Telemetry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreBackend {
    #[default]
    Sqlite,
    Postgres,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StoreSnapshot {
    pub backend: StoreBackend,
    pub consecutive_failures: u32,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_failure_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StorageComponent {
    pub store: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StorageSnapshot {
    pub state_dir: String,
    pub state_fs_free_bytes: Option<u64>,
    pub state_fs_free_ratio: Option<f64>,
    pub components: Vec<StorageComponent>,
    pub last_gc_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GithubSnapshot {
    pub configured: bool,
    pub last_webhook_at: Option<DateTime<Utc>>,
    pub pending_check_updates: u32,
    pub last_check_success_at: Option<DateTime<Utc>>,
    pub last_check_failure_at: Option<DateTime<Utc>>,
    pub rate_limit: Option<GithubRateLimit>,
    pub installation_token_expires_in_seconds: Option<u64>,
    pub token_cache: Option<TokenCacheSnapshot>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GithubRateLimit {
    pub resource: String,
    pub limit: u32,
    pub remaining: u32,
    pub reset_at: Option<DateTime<Utc>>,
    pub observed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenCacheSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub ttl_seconds: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DebugSnapshot {
    pub active_sessions: u32,
    pub oldest_session_seconds: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetrySnapshot {
    pub otlp_enabled: bool,
    pub last_export_success_at: Option<DateTime<Utc>>,
    pub last_export_failure_at: Option<DateTime<Utc>>,
    pub dropped_records: u64,
}

// ---------------------------------------------------------------------------
// Limits / Tasks (from heartbeat/limit registries)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LimitEntry {
    pub limit: String,
    pub value: usize,
    pub dropped: u64,
    pub rejected: u64,
    pub last_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskEntry {
    pub name: String,
    pub critical: bool,
    pub heartbeat_age_seconds: f64,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    pub code: String,
    pub severity: String,
    pub message: String,
    pub exemplars: Vec<ConditionExemplar>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionExemplar {
    pub run_id: Option<String>,
    pub job_id: Option<String>,
    pub runner_id: Option<String>,
    pub machine_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationalSnapshot {
    pub schema_version: u32,
    pub observed_at: DateTime<Utc>,
    pub snapshot_age_seconds: f64,
    pub overall: Overall,
    pub service: ServiceSnapshot,
    pub runs: RunsSnapshot,
    pub jobs: JobsSnapshot,
    pub concurrency: ConcurrencySnapshot,
    pub scheduler: SchedulerSnapshot,
    pub runners: RunnersSnapshot,
    pub pool: PoolSnapshot,
    pub vms: VmFleetSnapshot,
    pub store: StoreSnapshot,
    pub storage: StorageSnapshot,
    pub limits: Vec<LimitEntry>,
    pub tasks: Vec<TaskEntry>,
    pub github: GithubSnapshot,
    pub debug: DebugSnapshot,
    pub telemetry: TelemetrySnapshot,
    pub conditions: Vec<Condition>,
}

impl Default for OperationalSnapshot {
    fn default() -> Self {
        Self {
            schema_version: 1,
            observed_at: Utc::now(),
            snapshot_age_seconds: 0.0,
            overall: Overall::Ok,
            service: ServiceSnapshot {
                // The host binary owns this value; this crate's version is
                // meaningless to an operator reading status, and a stale
                // value is worse than an empty one.
                version: String::new(),
                instance_id: String::new(),
                uptime_seconds: 0,
                shutdown_requested: false,
            },
            runs: RunsSnapshot::default(),
            jobs: JobsSnapshot::default(),
            concurrency: ConcurrencySnapshot::default(),
            scheduler: SchedulerSnapshot::default(),
            runners: RunnersSnapshot::default(),
            pool: PoolSnapshot::default(),
            vms: VmFleetSnapshot::default(),
            store: StoreSnapshot::default(),
            storage: StorageSnapshot::default(),
            limits: Vec::new(),
            tasks: Vec::new(),
            github: GithubSnapshot::default(),
            debug: DebugSnapshot::default(),
            telemetry: TelemetrySnapshot::default(),
            conditions: Vec::new(),
        }
    }
}
