//! Live status shared by the webhook repair layers.
//!
//! The delivery watchdog and App health monitor each run on their own cadence,
//! and each produces a fact an operator needs *before* something breaks: when
//! the delivery history was last read, when the App's event subscription was
//! last verified, and how many repairs are outstanding. A blind watchdog is
//! indistinguishable from a quiet one unless its last successful poll is
//! published somewhere, so this type is the single place both layers write and
//! the status snapshot reads.

use std::time::{SystemTime, UNIX_EPOCH};

/// What GitHub reports about one App's webhook wiring, plus how it compares
/// to what this server needs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppWebhookConfigStatus {
    pub app_id: String,
    /// Delivery URL currently configured on the App.
    pub hook_url: Option<String>,
    /// URL this server expects to be called on, when it can know it
    /// (`PRELOOP_PUBLIC_URL`).
    pub expected_url: Option<String>,
    /// Required trigger events the App is not subscribed to. GitHub has no
    /// API to fix these — only the App settings UI does — so they are
    /// reported, never repaired.
    pub missing_events: Vec<String>,
    /// Repository permissions the App lacks, which is *why* some events
    /// cannot even be subscribed to.
    pub missing_permissions: Vec<String>,
    pub error: Option<String>,
}

impl AppWebhookConfigStatus {
    /// True when GitHub will deliver somewhere other than this server.
    ///
    /// Only decidable when the operator told us our public URL; without it
    /// any URL is as plausible as any other and claiming drift would be a
    /// guess.
    pub fn url_drifted(&self) -> bool {
        match (&self.hook_url, &self.expected_url) {
            (Some(actual), Some(expected)) => {
                actual.trim_end_matches('/') != expected.trim_end_matches('/')
            }
            _ => false,
        }
    }

    pub fn healthy(&self) -> bool {
        self.error.is_none()
            && self.missing_events.is_empty()
            && self.missing_permissions.is_empty()
            && self
                .hook_url
                .as_deref()
                .is_some_and(|u| !u.trim().is_empty())
            && !self.url_drifted()
    }
}

/// Delivery-watchdog progress.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatchdogStatus {
    /// The first attempted poll since startup. Unlike `last_poll_at_us`, this
    /// does not move during a persistent outage, so a never-successful
    /// watchdog eventually becomes visibly stale.
    pub first_poll_at_us: Option<i64>,
    pub last_poll_at_us: Option<i64>,
    pub last_success_at_us: Option<i64>,
    pub last_examined: u64,
    pub redeliveries_requested: u64,
    pub open_repairs: u64,
    pub last_error: Option<String>,
    /// False when no GitHub App is configured: the watchdog cannot run
    /// without an App JWT, and reporting that as "stale" would be a
    /// permanent false alarm.
    pub enabled: bool,
}

/// A single publication of the App configuration verdict and its timestamp.
///
/// Keeping both fields under one lock prevents an API read from combining the
/// statuses from one health check with the timestamp from another.
#[derive(Debug, Clone, Default)]
struct AppConfigSnapshot {
    statuses: Vec<AppWebhookConfigStatus>,
    checked_at_us: Option<i64>,
}

/// Everything the repair layers publish, behind one lock.
#[derive(Debug, Default)]
pub struct WebhookResilienceStatus {
    watchdog: parking_lot::RwLock<WatchdogStatus>,
    app_config: parking_lot::RwLock<AppConfigSnapshot>,
    /// Queue counters as last published by the webhook queue worker.
    ///
    /// The status sampler reads these instead of the store: a quiet engine
    /// would otherwise take the store's single connection every few seconds to
    /// learn that nothing changed. `None` means no read has happened yet —
    /// distinct from "the queue is empty", which is what a default would
    /// silently claim.
    queue_stats: parking_lot::RwLock<Option<crate::models::WebhookQueueStats>>,
}

impl WebhookResilienceStatus {
    pub fn watchdog(&self) -> WatchdogStatus {
        self.watchdog.read().clone()
    }

    pub fn update_watchdog(&self, update: impl FnOnce(&mut WatchdogStatus)) {
        update(&mut self.watchdog.write());
    }

    /// Read the App verdict and publication timestamp from one snapshot.
    pub fn app_config_snapshot(&self) -> (Vec<AppWebhookConfigStatus>, Option<i64>) {
        let snapshot = self.app_config.read();
        (snapshot.statuses.clone(), snapshot.checked_at_us)
    }

    pub fn set_app_config(&self, statuses: Vec<AppWebhookConfigStatus>, checked_at_us: i64) {
        *self.app_config.write() = AppConfigSnapshot {
            statuses,
            checked_at_us: Some(checked_at_us),
        };
    }

    /// Last published queue counters, or `None` when nothing has read them.
    pub fn queue_stats(&self) -> Option<crate::models::WebhookQueueStats> {
        *self.queue_stats.read()
    }

    pub fn set_queue_stats(&self, stats: crate::models::WebhookQueueStats) {
        *self.queue_stats.write() = Some(stats);
    }
}

/// Current time in the microsecond epoch every status field uses.
pub fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(i64::MAX as u128) as i64
}

/// Age in seconds of a microsecond timestamp, for staleness checks.
pub fn age_seconds(timestamp_us: i64, now_us: i64) -> f64 {
    ((now_us - timestamp_us).max(0) as f64) / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_drift_needs_both_sides_known() {
        let mut status = AppWebhookConfigStatus {
            app_id: "1".to_owned(),
            hook_url: Some("https://a.example/api/v1/github/webhooks".to_owned()),
            ..Default::default()
        };
        assert!(!status.url_drifted(), "unknown expectation is not drift");
        status.expected_url = Some("https://a.example/api/v1/github/webhooks/".to_owned());
        assert!(!status.url_drifted(), "trailing slash is not drift");
        status.expected_url = Some("https://b.example/api/v1/github/webhooks".to_owned());
        assert!(status.url_drifted());
        assert!(!status.healthy());
    }
}
