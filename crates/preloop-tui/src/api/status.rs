//! `GET /api/v1/status`: the operational snapshot behind the overview.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Overall health the engine reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Overall {
    /// Everything nominal.
    #[default]
    Ok,
    /// Working, with something to look at.
    Degraded,
    /// Work is blocked.
    Blocked,
    /// Shutting down.
    ShuttingDown,
    /// A value this build does not know.
    #[serde(other)]
    Unknown,
}

impl Overall {
    /// Header word for this health state.
    pub fn label(self) -> &'static str {
        match self {
            Self::Ok => "healthy",
            Self::Degraded => "degraded",
            Self::Blocked => "blocked",
            Self::ShuttingDown => "shutting down",
            Self::Unknown => "unknown",
        }
    }
}

/// The whole snapshot. Missing sections read as zero values.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct OperationalSnapshot {
    /// Snapshot schema version.
    pub schema_version: u32,
    /// When the engine observed this state.
    pub observed_at: Option<DateTime<Utc>>,
    /// Age of the snapshot when it was served.
    pub snapshot_age_seconds: f64,
    /// Overall health.
    pub overall: Overall,
    /// Engine identity and uptime.
    pub service: ServiceInfo,
    /// Run counts.
    pub runs: RunCounts,
    /// Runs in flight.
    pub active_runs: Vec<ActiveRun>,
    /// Queue counts.
    pub jobs: JobCounts,
    /// Runner counts and assignments.
    pub runners: RunnerCounts,
    /// Warm pool state.
    pub pool: PoolInfo,
    /// VM fleet and host usage.
    pub vms: VmFleet,
    /// GitHub integration state.
    pub github: GithubInfo,
    /// Debug session counts.
    pub debug: DebugInfo,
    /// Active conditions the engine has raised.
    pub conditions: Vec<Condition>,
}

/// Engine identity.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ServiceInfo {
    /// Engine version string.
    pub version: String,
    /// Per-process instance id.
    pub instance_id: String,
    /// Seconds since the engine started.
    pub uptime_seconds: u64,
    /// True while a shutdown is in progress.
    pub shutdown_requested: bool,
}

/// Counts of runs by state.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct RunCounts {
    /// Accepted, not yet running.
    pub queued: u32,
    /// Running.
    pub in_progress: u32,
    /// Finished.
    pub completed: u32,
}

/// A run in flight.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct ActiveRun {
    /// Run id.
    pub run_id: String,
    /// Workflow name.
    pub workflow: String,
    /// Run status, lowercase.
    pub status: String,
    /// Event name.
    pub event: String,
    /// When the run started.
    pub started_at: Option<DateTime<Utc>>,
    /// Runners holding its jobs.
    pub assigned_runners: Vec<String>,
}

/// Job queue counts.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct JobCounts {
    /// Jobs ready to run.
    pub ready: u32,
    /// Jobs waiting on a `needs` dependency.
    pub dependency_blocked: u32,
    /// Jobs waiting on a concurrency group.
    pub concurrency_blocked: u32,
    /// Matrix jobs waiting to expand.
    pub pending_expansion: u32,
    /// Matrix jobs expanding now.
    pub expanding: u32,
    /// Ready jobs a runner can take right now.
    pub claimable: u32,
    /// Ready jobs no runner can take.
    pub unclaimable: u32,
    /// Age of the oldest ready job in seconds.
    pub oldest_ready_seconds: Option<f64>,
}

/// Runner counts.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct RunnerCounts {
    /// Registered runners.
    pub registered: u32,
    /// Live poll sessions.
    pub sessions: u32,
    /// Registered and free.
    pub idle: u32,
    /// Registered and running a job.
    pub busy: u32,
    /// Registered but not polling.
    pub stale: u32,
    /// Age of the stalest poll, in seconds.
    pub max_poll_age_seconds: Option<f64>,
    /// Age of the stalest lease, in seconds.
    pub max_lease_age_seconds: Option<f64>,
    /// Job-to-runner bindings the engine holds.
    pub assignments: Vec<RunnerAssignment>,
}

/// One job bound to one runner.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct RunnerAssignment {
    /// Runner id.
    pub runner_id: i64,
    /// Run id.
    pub run_id: String,
    /// Job id.
    pub job_id: String,
    /// Seconds since the binding was made.
    pub assigned_seconds_ago: f64,
}

/// Warm pool state.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct PoolInfo {
    /// `warm`, `on_demand`, `external` or `disabled`.
    pub mode: String,
    /// Desired warm slots.
    pub desired: u32,
    /// Whether the pool is preparing.
    pub preparing: bool,
    /// Golden images building.
    pub building: u32,
    /// Machines provisioning.
    pub provisioning: u32,
    /// Idle slots.
    pub idle: u32,
    /// Busy slots.
    pub busy: u32,
    /// Paused slots.
    pub paused: u32,
    /// Provision failures in a row.
    pub consecutive_provision_failures: u32,
    /// Jobs waiting for a slot.
    pub queue_depth: u32,
    /// Runners registered but not yet bound.
    pub pending_registrations: u32,
    /// Bindings released back to the pool.
    pub released_bindings: u64,
    /// Labels the pool serves.
    pub labels: Vec<String>,
}

/// VM fleet counts and host usage.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct VmFleet {
    /// How many VMs of each kind exist.
    pub count: VmCount,
    /// Host resource usage.
    pub host_usage: HostUsage,
}

/// VM counts.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct VmCount {
    /// Runner VMs.
    pub runner: u32,
    /// Golden snapshot VMs.
    pub golden: u32,
    /// VMs that could not be inspected.
    pub unavailable: u32,
}

/// Host resource usage.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct HostUsage {
    /// CPU cores in use.
    pub cpu_cores: Option<f64>,
    /// Host memory total.
    pub mem_total_bytes: Option<u64>,
    /// Host memory available.
    pub mem_available_bytes: Option<u64>,
}

/// GitHub integration state.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct GithubInfo {
    /// Whether GitHub credentials are configured.
    pub configured: bool,
    /// When the last webhook arrived.
    pub last_webhook_at: Option<DateTime<Utc>>,
    /// Check updates waiting to be sent.
    pub pending_check_updates: u32,
    /// Current API rate limit, when known.
    pub rate_limit: Option<RateLimit>,
}

/// GitHub API rate limit.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct RateLimit {
    /// Rate limit bucket (`core`, `graphql`, ...).
    pub resource: String,
    /// Requests allowed per window.
    pub limit: u32,
    /// Requests left in the window.
    pub remaining: u32,
    /// When the window resets.
    pub reset_at: Option<DateTime<Utc>>,
}

/// Debug session counts.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct DebugInfo {
    /// Sessions holding a VM.
    pub active_sessions: u32,
    /// Age of the oldest session in seconds.
    pub oldest_session_seconds: Option<f64>,
}

/// A condition the engine raised.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Condition {
    /// Stable condition code, such as `webhook_dead_letters`.
    pub code: String,
    /// `error`, `warning` or `info`.
    pub severity: String,
    /// Human-readable message.
    pub message: String,
    /// Objects the condition is about.
    pub exemplars: Vec<Exemplar>,
}

/// One object a condition refers to.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Exemplar {
    /// Run id.
    pub run_id: Option<String>,
    /// Job id.
    pub job_id: Option<String>,
    /// Runner id.
    pub runner_id: Option<String>,
    /// Machine name.
    pub machine_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_overall_is_tolerated() {
        let snapshot: OperationalSnapshot =
            serde_json::from_str(r#"{"overall":"sideways"}"#).expect("lenient");
        assert_eq!(snapshot.overall, Overall::Unknown);
    }

    #[test]
    fn overall_labels_match_header_words() {
        assert_eq!(Overall::Ok.label(), "healthy");
        assert_eq!(Overall::ShuttingDown.label(), "shutting down");
    }
}
