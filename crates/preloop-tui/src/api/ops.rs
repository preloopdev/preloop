//! Operator endpoints: pool bindings, runners, the webhook queue, debug
//! sessions, secrets and run acceptance.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `GET /api/v1/pool/bindings`: which job holds which runner, or waits for one.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct PoolBindings {
    /// Every binding the engine holds.
    pub bindings: Vec<PoolBinding>,
    /// When the engine produced this list.
    pub observed_at: Option<DateTime<Utc>>,
}

/// One job's binding to the pool.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct PoolBinding {
    /// Run id.
    pub run_id: String,
    /// Job id.
    pub job_id: String,
    /// `running`, `assigned`, `ready` or `provisioning`.
    pub state: String,
    /// Runner the job is on, when one is bound.
    pub runner_id: Option<i64>,
    /// That runner's name.
    pub runner_name: Option<String>,
    /// Seconds since the binding was made.
    pub since_seconds: Option<u64>,
    /// 1-based queue position for `ready` and `provisioning`.
    pub waitlist_position: Option<u32>,
}

/// `GET /api/v1/runners`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct RunnersList {
    /// Number of runners in `runners`.
    pub count: u64,
    /// Registered runners.
    pub runners: Vec<RunnerInfo>,
    /// Queued jobs, when the engine reports them.
    pub queued: Option<u64>,
    /// Claimable jobs, when the engine reports them.
    pub claimable: Option<u64>,
}

/// One registered runner.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct RunnerInfo {
    /// Runner id.
    pub id: i64,
    /// Runner name.
    pub name: String,
    /// Labels the runner advertises.
    pub labels: Vec<String>,
}

/// `GET /api/v1/webhooks/deliveries`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct WebhookDeliveries {
    /// Recent deliveries, newest first.
    pub deliveries: Vec<Delivery>,
    /// Queue counts.
    pub stats: WebhookStats,
}

/// One webhook delivery in the durable queue.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Delivery {
    /// GitHub delivery id.
    pub delivery_id: String,
    /// GitHub event name.
    pub event: String,
    /// `received`, `processing`, `done` or `failed` (dead letter).
    pub state: String,
    /// Attempts so far.
    pub attempts: u32,
    /// When the delivery was received.
    pub received_at: Option<DateTime<Utc>>,
    /// Age in seconds when the list was built.
    pub age_seconds: Option<f64>,
    /// When the current lease expires, while processing.
    pub lease_until: Option<DateTime<Utc>>,
    /// Last error text.
    pub last_error: Option<String>,
}

/// Queue counts.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct WebhookStats {
    /// Deliveries received and not yet picked up.
    pub received: u64,
    /// Deliveries being processed.
    pub processing: u64,
    /// Deliveries finished.
    pub done: u64,
    /// Deliveries that exhausted their retries.
    pub dead_letters: u64,
    /// When the oldest pending delivery arrived.
    pub oldest_pending_at: Option<DateTime<Utc>>,
    /// Age of the oldest pending delivery in seconds.
    pub oldest_pending_age_seconds: Option<f64>,
}

/// `GET /api/v1/webhooks/health`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct WebhookHealth {
    /// Queue stats, or `{"error": ...}` when the store failed. Read it with
    /// [`WebhookHealth::queue_stats`].
    pub queue: Value,
    /// Repair watchdog state.
    pub watchdog: Watchdog,
    /// GitHub API circuit breakers.
    pub github_breaker: Vec<Breaker>,
    /// GitHub Apps the engine checks.
    pub apps: Vec<AppHealth>,
    /// When the app configuration was last checked.
    pub app_config_checked_at: Option<DateTime<Utc>>,
}

impl WebhookHealth {
    /// The queue stats, when the store answered.
    pub fn queue_stats(&self) -> Option<WebhookStats> {
        if self.queue.get("error").is_some() {
            return None;
        }
        serde_json::from_value(self.queue.clone()).ok()
    }
}

/// Watchdog that repairs missed deliveries.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Watchdog {
    /// Whether the watchdog runs.
    pub enabled: bool,
    /// Last poll time.
    pub last_poll_at: Option<DateTime<Utc>>,
    /// Last successful poll time.
    pub last_success_at: Option<DateTime<Utc>>,
    /// Age of the last success in seconds.
    pub last_success_age_seconds: Option<f64>,
    /// Deliveries examined on the last pass.
    pub last_examined: u64,
    /// Redeliveries requested from GitHub.
    pub redeliveries_requested: u64,
    /// Repairs still open.
    pub open_repairs: u64,
    /// Last error text.
    pub last_error: Option<String>,
}

/// One GitHub API circuit breaker.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct Breaker {
    /// Rate-limit bucket.
    pub bucket: String,
    /// True while the breaker is open.
    pub open: bool,
    /// Seconds until a retry.
    pub retry_in_seconds: Option<f64>,
    /// Failures in a row.
    pub consecutive_failures: u64,
    /// Times the breaker has opened.
    pub trips: u64,
    /// True when the bucket is rate limited.
    pub rate_limited: bool,
    /// Last error text.
    pub last_error: Option<String>,
}

/// One GitHub App's webhook configuration check.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct AppHealth {
    /// App id, as the engine reports it.
    pub app_id: Value,
    /// Webhook URL GitHub has.
    pub hook_url: Option<String>,
    /// Webhook URL the engine expects.
    pub expected_url: Option<String>,
    /// True when the two URLs differ.
    pub url_drifted: bool,
    /// Events the app is missing.
    pub missing_events: Vec<String>,
    /// Permissions the app is missing.
    pub missing_permissions: Vec<String>,
    /// Overall result.
    pub healthy: bool,
    /// When this app was checked.
    pub checked_at: Option<DateTime<Utc>>,
    /// Check error, if any.
    pub error: Option<String>,
}

/// The step a debug session paused on.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct FailedStep {
    /// 0-based index among the job's steps.
    pub index: usize,
    /// Number of steps in the job.
    pub total: usize,
    /// Expression-context key.
    pub context_name: String,
    /// Display name.
    pub display_name: String,
    /// Command the step ran.
    pub command: Option<String>,
    /// Working directory.
    pub working_directory: Option<String>,
    /// Exit code, when the step failed with one.
    pub exit_code: Option<i32>,
}

/// A paused job's debug session.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct DebugSession {
    /// Session id.
    pub session_id: String,
    /// Run id.
    pub run_id: String,
    /// Job id.
    pub job_id: String,
    /// Job display name.
    pub job_name: String,
    /// `paused`, `attached`, `retrying`, `resumed`, `aborted` or `abandoned`.
    pub state: String,
    /// Session version; changes on every operation.
    pub version: u64,
    /// The step the job paused on.
    pub step: FailedStep,
    /// VM the job runs in.
    pub machine: Option<String>,
    /// Workspace path inside the VM.
    pub workspace: Option<String>,
    /// Controller holding the lease, if any.
    pub controller: Option<String>,
    /// When the session was created, in ms since the epoch.
    pub created_at_ms: u64,
    /// How long the session has been paused, in seconds.
    pub paused_seconds: u64,
}

/// `GET /api/v1/debug/sessions`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct DebugSessions {
    /// Every session the engine holds.
    pub sessions: Vec<DebugSession>,
}

/// One entry of a session's audit trail.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct AuditEntry {
    /// Request id.
    pub request_id: String,
    /// Controller that made the request.
    pub controller: String,
    /// Operation name.
    pub operation: String,
    /// Outcome.
    pub status: String,
    /// Version before the operation.
    pub prev_version: u64,
    /// Version after the operation.
    pub new_version: u64,
    /// When the operation ran, in ms since the epoch.
    pub timestamp_ms: u64,
}

/// One stored secret. Values are never returned.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct SecretEntry {
    /// Secret name.
    pub name: String,
    /// `owner/name` scope, when repository-scoped.
    pub repo: Option<String>,
    /// Environment scope, when environment-scoped.
    pub env: Option<String>,
}

/// `GET /api/v1/secrets`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct SecretList {
    /// Stored secrets.
    pub secrets: Vec<SecretEntry>,
}

/// `POST /api/v1/runs/{id}/rerun` response.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct RunAccepted {
    /// Run id.
    pub run_id: String,
    /// Run number.
    pub run_number: u64,
    /// Jobs queued.
    pub queued_jobs: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_stats_reads_only_when_store_answered() {
        let ok: WebhookHealth = serde_json::from_str(
            r#"{"queue":{"received":3,"processing":0,"done":2,"dead_letters":1}}"#,
        )
        .expect("health parses");
        assert_eq!(ok.queue_stats().map(|s| s.dead_letters), Some(1));

        let failed: WebhookHealth =
            serde_json::from_str(r#"{"queue":{"error":"store unavailable"}}"#).expect("parses");
        assert_eq!(failed.queue_stats(), None);
    }

    #[test]
    fn debug_session_list_is_wrapped() {
        let list: DebugSessions = serde_json::from_str(
            r#"{"sessions":[{"session_id":"ds-7c1e","state":"paused","step":{"index":3}}]}"#,
        )
        .expect("wrapped list");
        assert_eq!(list.sessions[0].session_id, "ds-7c1e");
        assert_eq!(list.sessions[0].step.index, 3);
    }
}
