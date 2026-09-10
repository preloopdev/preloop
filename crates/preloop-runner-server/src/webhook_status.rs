//! Live status shared by the webhook repair layers.
//!
//! The watchdog, the source-state reconciler and the App health monitor each
//! run on their own cadence, and each produces a fact an operator needs
//! *before* something breaks: when the delivery history was last read, when
//! the App's event subscription was last verified, how many repairs are
//! outstanding. A blind watchdog is indistinguishable from a quiet one
//! unless its last successful poll is published somewhere, so this type is
//! the single place all three write and the status snapshot reads.

use std::time::{SystemTime, UNIX_EPOCH};

/// What GitHub reports about one App's webhook wiring, plus how it compares
/// to what this server needs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct AppWebhookConfigStatus {
    pub(crate) app_id: String,
    /// Delivery URL currently configured on the App.
    pub(crate) hook_url: Option<String>,
    /// URL this server expects to be called on, when it can know it
    /// (`PRELOOP_PUBLIC_URL`).
    pub(crate) expected_url: Option<String>,
    /// Required trigger events the App is not subscribed to. GitHub has no
    /// API to fix these — only the App settings UI does — so they are
    /// reported, never repaired.
    pub(crate) missing_events: Vec<String>,
    /// Repository permissions the App lacks, which is *why* some events
    /// cannot even be subscribed to.
    pub(crate) missing_permissions: Vec<String>,
    pub(crate) error: Option<String>,
}

impl AppWebhookConfigStatus {
    /// True when GitHub will deliver somewhere other than this server.
    ///
    /// Only decidable when the operator told us our public URL; without it
    /// any URL is as plausible as any other and claiming drift would be a
    /// guess.
    pub(crate) fn url_drifted(&self) -> bool {
        match (&self.hook_url, &self.expected_url) {
            (Some(actual), Some(expected)) => {
                actual.trim_end_matches('/') != expected.trim_end_matches('/')
            }
            _ => false,
        }
    }

    pub(crate) fn healthy(&self) -> bool {
        self.error.is_none()
            && self.missing_events.is_empty()
            && self.missing_permissions.is_empty()
            && !self.url_drifted()
    }
}

/// Delivery-watchdog progress.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct WatchdogStatus {
    pub(crate) last_poll_at_us: Option<i64>,
    /// Last poll that completed without error. Staleness here is the alert.
    pub(crate) last_success_at_us: Option<i64>,
    /// Deliveries examined in the last successful poll.
    pub(crate) last_examined: u64,
    /// Redeliveries requested since boot.
    pub(crate) redeliveries_requested: u64,
    /// Repairs that have not yet shown up locally.
    pub(crate) open_repairs: u64,
    pub(crate) last_error: Option<String>,
    /// False when no GitHub App is configured: the watchdog cannot run
    /// without an App JWT, and reporting that as "stale" would be a
    /// permanent false alarm.
    pub(crate) enabled: bool,
}

/// Source-state reconciler progress.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReconcilerStatus {
    pub(crate) enabled: bool,
    pub(crate) last_run_at_us: Option<i64>,
    pub(crate) last_success_at_us: Option<i64>,
    /// Events synthesized since boot — every one of these is a run GitHub
    /// never asked for, so the number is worth watching.
    pub(crate) synthesized: u64,
    pub(crate) repositories_scanned: u64,
    pub(crate) last_error: Option<String>,
}

/// Everything the repair layers publish, behind one lock.
#[derive(Debug, Default)]
pub(crate) struct WebhookResilienceStatus {
    watchdog: parking_lot::RwLock<WatchdogStatus>,
    reconciler: parking_lot::RwLock<ReconcilerStatus>,
    app_config: parking_lot::RwLock<Vec<AppWebhookConfigStatus>>,
    config_checked_at_us: parking_lot::RwLock<Option<i64>>,
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
    pub(crate) fn watchdog(&self) -> WatchdogStatus {
        self.watchdog.read().clone()
    }

    pub(crate) fn update_watchdog(&self, update: impl FnOnce(&mut WatchdogStatus)) {
        update(&mut self.watchdog.write());
    }

    pub(crate) fn reconciler(&self) -> ReconcilerStatus {
        self.reconciler.read().clone()
    }

    pub(crate) fn update_reconciler(&self, update: impl FnOnce(&mut ReconcilerStatus)) {
        update(&mut self.reconciler.write());
    }

    pub(crate) fn app_config(&self) -> Vec<AppWebhookConfigStatus> {
        self.app_config.read().clone()
    }

    pub(crate) fn config_checked_at_us(&self) -> Option<i64> {
        *self.config_checked_at_us.read()
    }

    pub(crate) fn set_app_config(&self, statuses: Vec<AppWebhookConfigStatus>, checked_at_us: i64) {
        *self.app_config.write() = statuses;
        *self.config_checked_at_us.write() = Some(checked_at_us);
    }

    /// Last published queue counters, or `None` when nothing has read them.
    pub(crate) fn queue_stats(&self) -> Option<crate::models::WebhookQueueStats> {
        *self.queue_stats.read()
    }

    pub(crate) fn set_queue_stats(&self, stats: crate::models::WebhookQueueStats) {
        *self.queue_stats.write() = Some(stats);
    }
}

/// Current time in the microsecond epoch every status field uses.
pub(crate) fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(i64::MAX as u128) as i64
}

/// Age in seconds of a microsecond timestamp, for staleness checks.
pub(crate) fn age_seconds(timestamp_us: i64, now_us: i64) -> f64 {
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
