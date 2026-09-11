//! Delivery watchdog: repairs deliveries GitHub recorded but preloop never
//! processed.
//!
//! GitHub sends each webhook once. A non-2xx (or a host that was simply not
//! there) becomes a red row in the App's delivery history and nothing else —
//! no automatic retry, ever. That history is the only other copy of the
//! payload, it lasts three days, and the only way to use it is to ask GitHub
//! to attempt the delivery again.
//!
//! Two failure shapes are repaired here, and they look nothing alike:
//!
//! * **Edge failure** — GitHub recorded a failure. Funnel was stale, the host
//!   was rebooting, TLS was broken. Obvious once you look at the history.
//! * **Phantom ack** — GitHub recorded *success* and preloop has no row. A
//!   restore from an old snapshot, a deleted state dir, corruption. Both
//!   sides look healthy; only a join between GitHub's GUIDs and the local
//!   `webhook_deliveries` table reveals it. This is the dangerous one,
//!   because nothing anywhere is alarming.
//!
//! Everything here is deliberately conservative:
//!
//! * A **grace window** keeps a merely-late delivery from being declared
//!   lost. GitHub deliveries are not immediate, and "missing after 30s"
//!   would manufacture duplicate work every day.
//! * The watermark **never advances on a failed poll**, so an error cannot
//!   silently skip a range of history.
//! * Nothing is redelivered while the local store is unhealthy — replaying a
//!   payload into a broken store loses it a second time, and burns one of
//!   the finite redelivery opportunities doing it.
//! * The breaker gates the poll: during a GitHub outage a poll would fail
//!   anyway, and hammering a failing dependency is what the breaker exists
//!   to prevent.

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use crate::github_app::GitHubAppCredentials;
use crate::models::{WebhookRedeliveryRecord, WebhookRepairReason, WebhookWatchdogCursor};
use crate::state::SharedState;
use crate::webhook_status::now_us;

const DEFAULT_INTERVAL_SECS: u64 = 300;
const MIN_INTERVAL_SECS: u64 = 10;
const DEFAULT_GRACE_SECS: i64 = 120;
const DEFAULT_MAX_ATTEMPTS: u32 = 5;
const DEFAULT_MAX_PAGES: usize = 5;
const PER_PAGE: usize = 100;
/// Cap on repair rows read for the backlog gauge. A number this large is
/// already an incident; the exact count past it changes no decision.
const OPEN_REPAIR_SCAN_LIMIT: usize = 1000;

/// What one full watchdog pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct WatchdogPollOutcome {
    /// Deliveries old enough to judge, and judged.
    pub(crate) examined: u64,
    /// Examined deliveries with no local row, whatever GitHub thinks.
    pub(crate) missing_locally: u64,
    /// Redelivery attempts actually requested from GitHub.
    pub(crate) redelivered: u64,
    /// Deliveries too young to judge yet.
    pub(crate) skipped_grace: u64,
}

impl WatchdogPollOutcome {
    fn merge(&mut self, other: &Self) {
        self.examined += other.examined;
        self.missing_locally += other.missing_locally;
        self.redelivered += other.redelivered;
        self.skipped_grace += other.skipped_grace;
    }
}

/// One row of `GET /app/hook/deliveries`.
///
/// Only the fields the repair decision needs are modelled; GitHub adds
/// fields over time and an unknown one must never fail the poll.
#[derive(Debug, Deserialize)]
struct DeliveryItem {
    id: i64,
    guid: String,
    delivered_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    status_code: u16,
    #[serde(default)]
    event: String,
    /// Present when GitHub delayed the delivery. Purely informational here,
    /// but it is the tell that a "missing" delivery was really just slow.
    #[serde(default)]
    throttled_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl DeliveryItem {
    fn delivered_at_us(&self) -> Option<i64> {
        self.delivered_at.map(crate::store::unix_us)
    }

    fn remote_succeeded(&self) -> bool {
        (200..300).contains(&self.status_code)
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn watchdog_enabled() -> bool {
    !std::env::var("PRELOOP_WEBHOOK_WATCHDOG")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "off" | "0" | "false" | "disabled"
            )
        })
        .unwrap_or(false)
}

fn poll_interval() -> Duration {
    Duration::from_secs(
        env_u64(
            "PRELOOP_WEBHOOK_WATCHDOG_INTERVAL_SECS",
            DEFAULT_INTERVAL_SECS,
        )
        .max(MIN_INTERVAL_SECS),
    )
}

fn grace_us() -> i64 {
    env_u64(
        "PRELOOP_WEBHOOK_WATCHDOG_GRACE_SECS",
        DEFAULT_GRACE_SECS as u64,
    ) as i64
        * 1_000_000
}

fn max_attempts() -> u32 {
    env_u64(
        "PRELOOP_WEBHOOK_WATCHDOG_MAX_ATTEMPTS",
        DEFAULT_MAX_ATTEMPTS as u64,
    ) as u32
}

fn max_pages() -> usize {
    env_u64(
        "PRELOOP_WEBHOOK_WATCHDOG_MAX_PAGES",
        DEFAULT_MAX_PAGES as u64,
    ) as usize
}

/// Backoff before the next redelivery of the same GUID.
///
/// A redelivery only *schedules* an attempt; GitHub may fail it again for
/// the same reason (our ingress is still down). Spacing attempts out means
/// the three-day history window is not spent in the first minute.
fn redelivery_backoff_us(attempts: u32) -> i64 {
    let secs: i64 = match attempts {
        0 => 0,
        1 => 5 * 60,
        2 => 15 * 60,
        3 => 60 * 60,
        _ => 6 * 60 * 60,
    };
    secs * 1_000_000
}

/// Background loop. One pass per interval, plus status publication so a
/// stalled watchdog is visible instead of silently absent.
pub(crate) async fn run_webhook_watchdog(shared: Arc<SharedState>) {
    let interval = poll_interval();
    loop {
        if shared.shutdown.is_cancelled() {
            break;
        }
        match watchdog_poll_once(&shared).await {
            Ok(outcome) if outcome.redelivered > 0 => tracing::info!(
                examined = outcome.examined,
                missing = outcome.missing_locally,
                redelivered = outcome.redelivered,
                "webhook delivery watchdog requested redeliveries"
            ),
            Ok(outcome) => tracing::debug!(
                examined = outcome.examined,
                skipped_grace = outcome.skipped_grace,
                "webhook delivery watchdog poll complete"
            ),
            Err(error) => tracing::warn!(?error, "webhook delivery watchdog poll failed"),
        }
        tokio::select! {
            _ = shared.shutdown.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

/// One pass over every configured App's delivery history.
pub(crate) async fn watchdog_poll_once(
    shared: &Arc<SharedState>,
) -> anyhow::Result<WatchdogPollOutcome> {
    let apps = crate::github_app::registered_apps(&shared.state);
    if apps.is_empty() || !watchdog_enabled() {
        // No App JWT means no delivery history to read. Report it as
        // disabled rather than letting "last success" age into a false
        // alarm forever.
        shared.state.webhook_status.update_watchdog(|status| {
            status.enabled = false;
        });
        return Ok(WatchdogPollOutcome::default());
    }
    shared.state.webhook_status.update_watchdog(|status| {
        status.enabled = true;
        status.last_poll_at_us = Some(now_us());
    });

    if let Some(retry_after) = shared.state.github_breaker.retry_after() {
        // Deliberately not an error, and deliberately does not touch
        // `last_success_at_us`: the poll did not happen, and staleness must
        // keep growing until one does.
        tracing::debug!(
            retry_in_secs = retry_after.as_secs(),
            "GitHub breaker open; skipping delivery watchdog poll"
        );
        return Ok(WatchdogPollOutcome::default());
    }

    // One store probe per pass decides whether repairs are safe at all.
    let store_healthy = shared.state.store.webhook_queue_stats().await.is_ok();
    if !store_healthy {
        tracing::warn!("local webhook store is unhealthy; watchdog will observe but not redeliver");
    }

    let mut total = WatchdogPollOutcome::default();
    let mut errors: Vec<String> = Vec::new();
    for app in apps {
        match poll_app(shared, &app, store_healthy).await {
            Ok(outcome) => total.merge(&outcome),
            Err(error) => {
                tracing::warn!(app_id = %app.app_id, ?error, "watchdog poll failed for App");
                errors.push(format!("app {}: {error}", app.app_id));
            }
        }
    }

    let open_repairs = shared
        .state
        .store
        .open_webhook_redeliveries(OPEN_REPAIR_SCAN_LIMIT)
        .await
        .map(|repairs| repairs.len() as u64)
        .unwrap_or_default();
    let succeeded = errors.is_empty();
    let redelivered = total.redelivered;
    let examined = total.examined;
    shared.state.webhook_status.update_watchdog(|status| {
        status.enabled = true;
        status.open_repairs = open_repairs;
        status.redeliveries_requested = status.redeliveries_requested.saturating_add(redelivered);
        if succeeded {
            status.last_success_at_us = Some(now_us());
            status.last_examined = examined;
            status.last_error = None;
        } else {
            status.last_error = Some(errors.join("; "));
        }
    });
    if !errors.is_empty() {
        anyhow::bail!("watchdog poll failed: {}", errors.join("; "));
    }
    Ok(total)
}

async fn poll_app(
    shared: &Arc<SharedState>,
    app: &GitHubAppCredentials,
    store_healthy: bool,
) -> anyhow::Result<WatchdogPollOutcome> {
    let jwt = crate::github_app::sign_app_jwt(&app.app_id, &app.private_key)?;
    let api_base = crate::github::github_api_base();
    let previous = shared
        .state
        .store
        .load_webhook_watchdog_cursor(&app.app_id)
        .await?;
    let watermark = previous
        .as_ref()
        .and_then(|cursor| cursor.cursor_delivered_at_us);
    let grace_boundary = now_us() - grace_us();

    let mut outcome = WatchdogPollOutcome::default();
    let mut newest_examined: Option<i64> = None;
    let mut cursor: Option<String> = None;
    let mut reached_watermark = false;
    let mut has_more_pages = false;
    for _page in 0..max_pages() {
        let (items, next_cursor) = list_deliveries(shared, &api_base, &jwt, cursor.as_deref())
            .await
            .map_err(|error| anyhow::anyhow!("listing deliveries: {error}"))?;
        if items.is_empty() {
            break;
        }

        let mut examined: Vec<DeliveryItem> = Vec::with_capacity(items.len());
        for item in items {
            let Some(delivered_at_us) = item.delivered_at_us() else {
                // No timestamp means the item cannot be ordered against the
                // watermark; judging it would risk repairing the same
                // delivery on every poll forever.
                continue;
            };
            if watermark.is_some_and(|mark| delivered_at_us < mark) {
                reached_watermark = true;
                continue;
            }
            if delivered_at_us > grace_boundary {
                outcome.skipped_grace += 1;
                continue;
            }
            newest_examined =
                Some(newest_examined.map_or(delivered_at_us, |newest| newest.max(delivered_at_us)));
            examined.push(item);
        }

        if !examined.is_empty() {
            outcome.examined += examined.len() as u64;
            let guids: Vec<String> = examined.iter().map(|item| item.guid.clone()).collect();
            let present = shared
                .state
                .store
                .webhook_deliveries_present(&guids)
                .await?;
            for item in &examined {
                if present.contains(&item.guid) {
                    // A repair that finally landed. Closing it here is what
                    // keeps the backlog gauge meaningful.
                    if let Err(error) = shared
                        .state
                        .store
                        .resolve_webhook_redelivery(&item.guid, now_us())
                        .await
                    {
                        tracing::warn!(guid = %item.guid, ?error, "failed to close webhook repair");
                    }
                    continue;
                }
                outcome.missing_locally += 1;
                let reason = if item.remote_succeeded() {
                    WebhookRepairReason::PhantomAck
                } else {
                    WebhookRepairReason::RemoteFailure
                };
                if store_healthy
                    && repair_delivery(shared, app, item, reason, &api_base, &jwt).await?
                {
                    outcome.redelivered += 1;
                }
            }
        }

        if reached_watermark {
            has_more_pages = false;
            break;
        }
        match next_cursor {
            Some(next) => {
                cursor = Some(next);
                has_more_pages = true;
            }
            None => {
                has_more_pages = false;
                break;
            }
        }
    }

    let now = now_us();
    let advanced = if reached_watermark || !has_more_pages {
        match (watermark, newest_examined) {
            (Some(mark), Some(newest)) => Some(mark.max(newest)),
            (None, Some(newest)) => Some(newest),
            (mark, None) => mark,
        }
    } else {
        // Truncated at max_pages(): do not advance the watermark across an
        // unvisited range, otherwise skipped older deliveries are permanently lost.
        watermark
    };
    shared
        .state
        .store
        .store_webhook_watchdog_cursor(&WebhookWatchdogCursor {
            scope: app.app_id.clone(),
            cursor_delivered_at_us: advanced,
            last_poll_at_us: Some(now),
            last_success_at_us: Some(now),
        })
        .await?;
    Ok(outcome)
}

/// Ask GitHub to attempt one delivery again, subject to per-GUID backoff and
/// the attempt cap. Returns whether an attempt was actually requested.
async fn repair_delivery(
    shared: &Arc<SharedState>,
    app: &GitHubAppCredentials,
    item: &DeliveryItem,
    reason: WebhookRepairReason,
    api_base: &str,
    jwt: &str,
) -> anyhow::Result<bool> {
    let now = now_us();
    let existing = shared
        .state
        .store
        .load_webhook_redelivery(&item.guid)
        .await?;
    let mut record = existing.unwrap_or(WebhookRedeliveryRecord {
        delivery_guid: item.guid.clone(),
        github_delivery_id: item.id,
        app_id: app.app_id.clone(),
        reason,
        attempts: 0,
        first_seen_at_us: now,
        last_attempt_at_us: None,
        resolved_at_us: None,
        last_error: None,
    });
    if record.resolved_at_us.is_some() {
        // Reopened: the delivery is missing again (restore, prune, replay).
        record.resolved_at_us = None;
    }
    record.reason = reason;
    record.github_delivery_id = item.id;

    if record.attempts >= max_attempts() {
        // Kept open on purpose. A GUID we could never get back is a standing
        // finding for an operator, not something to forget.
        record.last_error = Some(format!(
            "redelivery cap of {} attempts reached; repair the ingress and replay manually",
            max_attempts()
        ));
        shared
            .state
            .store
            .upsert_webhook_redelivery(&record)
            .await?;
        return Ok(false);
    }
    if let Some(last_attempt) = record.last_attempt_at_us {
        if now - last_attempt < redelivery_backoff_us(record.attempts) {
            return Ok(false);
        }
    }

    let url = format!(
        "{}/app/hook/deliveries/{}/attempts",
        api_base.trim_end_matches('/'),
        item.id
    );
    let response = crate::github_breaker::send_observed(
        &shared.state.github_breaker,
        crate::shared_http::CLIENT
            .clone()
            .post(&url)
            .header("User-Agent", "preloop")
            .header("Authorization", format!("Bearer {jwt}"))
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28"),
    )
    .await;

    record.attempts = record.attempts.saturating_add(1);
    record.last_attempt_at_us = Some(now);
    let requested = match response {
        Ok(response) if response.status().is_success() => {
            record.last_error = None;
            tracing::warn!(
                guid = %item.guid,
                delivery_id = item.id,
                event = %item.event,
                reason = reason.as_str(),
                attempt = record.attempts,
                throttled = item.throttled_at.is_some(),
                "requested GitHub webhook redelivery"
            );
            true
        }
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            record.last_error = Some(format!("redelivery request failed: {status}: {body}"));
            tracing::warn!(guid = %item.guid, %status, "GitHub refused the redelivery request");
            false
        }
        Err(error) => {
            record.last_error = Some(format!("redelivery request failed: {error}"));
            tracing::warn!(guid = %item.guid, %error, "redelivery request could not be sent");
            false
        }
    };
    shared
        .state
        .store
        .upsert_webhook_redelivery(&record)
        .await?;
    Ok(requested)
}

/// One page of delivery history plus the cursor for the next page.
async fn list_deliveries(
    shared: &Arc<SharedState>,
    api_base: &str,
    jwt: &str,
    cursor: Option<&str>,
) -> anyhow::Result<(Vec<DeliveryItem>, Option<String>)> {
    let mut url = format!(
        "{}/app/hook/deliveries?per_page={PER_PAGE}",
        api_base.trim_end_matches('/')
    );
    if let Some(cursor) = cursor {
        url.push_str(&format!("&cursor={cursor}"));
    }
    let response = crate::github_breaker::send_observed(
        &shared.state.github_breaker,
        crate::shared_http::CLIENT
            .clone()
            .get(&url)
            .header("User-Agent", "preloop")
            .header("Authorization", format!("Bearer {jwt}"))
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28"),
    )
    .await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("GitHub returned {status}: {body}");
    }
    let next_cursor = response
        .headers()
        .get(axum::http::header::LINK)
        .and_then(|value| value.to_str().ok())
        .and_then(next_cursor_from_link);
    let items: Vec<DeliveryItem> = response.json().await?;
    Ok((items, next_cursor))
}

/// Extract the `cursor` query parameter of the `rel="next"` link.
///
/// The delivery-history endpoint pages by opaque cursor, not by page number,
/// and only the `Link` header carries it.
fn next_cursor_from_link(link_header: &str) -> Option<String> {
    for part in link_header.split(',') {
        let part = part.trim();
        if !part.contains("rel=\"next\"") {
            continue;
        }
        let start = part.find('<')?;
        let end = part.find('>')?;
        let url = part.get(start + 1..end)?;
        let query = url.split_once('?').map(|(_, query)| query)?;
        for pair in query.split('&') {
            if let Some(value) = pair.strip_prefix("cursor=") {
                return Some(value.to_owned());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{WebhookDeliveryRecord, WebhookDeliveryStatus};
    use axum::extract::State;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// 2048-bit key generation costs about a second; every test in this
    /// module only needs *a* valid key, so it is generated once.
    static TEST_KEY: std::sync::LazyLock<rsa::RsaPrivateKey> = std::sync::LazyLock::new(|| {
        rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).expect("test key")
    });

    #[derive(Clone)]
    struct StubState {
        deliveries: Arc<Vec<serde_json::Value>>,
        attempts: Arc<AtomicUsize>,
        list_status: axum::http::StatusCode,
    }

    async fn stub_github(
        deliveries: Vec<serde_json::Value>,
        list_status: axum::http::StatusCode,
    ) -> (String, Arc<AtomicUsize>) {
        let attempts = Arc::new(AtomicUsize::new(0));
        let state = StubState {
            deliveries: Arc::new(deliveries),
            attempts: attempts.clone(),
            list_status,
        };
        let router = Router::new()
            .route(
                "/app/hook/deliveries",
                get(|State(state): State<StubState>| async move {
                    if !state.list_status.is_success() {
                        return (state.list_status, Json(json!({"message": "boom"})));
                    }
                    (
                        axum::http::StatusCode::OK,
                        Json(serde_json::Value::Array(state.deliveries.as_ref().clone())),
                    )
                }),
            )
            .route(
                "/app/hook/deliveries/:id/attempts",
                post(|State(state): State<StubState>| async move {
                    state.attempts.fetch_add(1, Ordering::SeqCst);
                    (axum::http::StatusCode::ACCEPTED, Json(json!({})))
                }),
            )
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        (format!("http://{addr}"), attempts)
    }

    async fn shared_with_app(api_base: &str) -> Arc<SharedState> {
        let temp = tempfile::tempdir().unwrap();
        // The tempdir must outlive the state; leak it for the test's life.
        let path = temp.keep();
        let mut state = crate::AppState::new(path).await.unwrap();
        state.github_app = Some(GitHubAppCredentials::for_tests(
            "424",
            TEST_KEY.clone(),
            crate::github_app::MintFailurePolicy::LocalJwt,
        ));
        let _ = api_base;
        Arc::new(SharedState {
            state,
            shutdown: tokio_util::sync::CancellationToken::new(),
        })
    }

    fn delivery(id: i64, guid: &str, status_code: u16, age_secs: i64) -> serde_json::Value {
        let delivered_at = chrono::Utc::now() - chrono::Duration::seconds(age_secs);
        json!({
            "id": id,
            "guid": guid,
            "delivered_at": delivered_at.to_rfc3339(),
            "status": if (200..300).contains(&status_code) { "OK" } else { "failed" },
            "status_code": status_code,
            "event": "push",
        })
    }

    #[tokio::test]
    async fn redelivers_a_failed_delivery_that_never_landed() {
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let (api_base, attempts) = stub_github(
            vec![delivery(7, "guid-fail", 502, 600)],
            axum::http::StatusCode::OK,
        )
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let shared = shared_with_app(&api_base).await;

        let outcome = watchdog_poll_once(&shared).await.unwrap();

        assert_eq!(outcome.examined, 1);
        assert_eq!(outcome.missing_locally, 1);
        assert_eq!(outcome.redelivered, 1);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        let repair = shared
            .state
            .store
            .load_webhook_redelivery("guid-fail")
            .await
            .unwrap()
            .expect("repair recorded");
        assert_eq!(repair.reason, WebhookRepairReason::RemoteFailure);
        assert_eq!(repair.attempts, 1);
    }

    #[tokio::test]
    async fn redelivers_a_phantom_ack() {
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let (api_base, attempts) = stub_github(
            vec![delivery(9, "guid-phantom", 202, 600)],
            axum::http::StatusCode::OK,
        )
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let shared = shared_with_app(&api_base).await;

        let outcome = watchdog_poll_once(&shared).await.unwrap();

        assert_eq!(
            outcome.redelivered, 1,
            "a remote success with no local row must still be repaired"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(
            shared
                .state
                .store
                .load_webhook_redelivery("guid-phantom")
                .await
                .unwrap()
                .expect("repair recorded")
                .reason,
            WebhookRepairReason::PhantomAck
        );
    }

    #[tokio::test]
    async fn locally_present_delivery_is_not_redelivered() {
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let (api_base, attempts) = stub_github(
            vec![delivery(11, "guid-present", 500, 600)],
            axum::http::StatusCode::OK,
        )
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let shared = shared_with_app(&api_base).await;
        shared
            .state
            .store
            .enqueue_webhook_delivery(&WebhookDeliveryRecord {
                delivery_id: "guid-present".to_owned(),
                event: "push".to_owned(),
                payload: b"{}".to_vec(),
                received_at_us: now_us(),
                state: WebhookDeliveryStatus::Received,
                attempts: 0,
                lease_until_us: None,
                lease_token: None,
                last_error: None,
            })
            .await
            .unwrap();

        let outcome = watchdog_poll_once(&shared).await.unwrap();

        assert_eq!(outcome.examined, 1);
        assert_eq!(outcome.missing_locally, 0);
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn delivery_inside_the_grace_window_is_left_alone() {
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let (api_base, attempts) = stub_github(
            vec![delivery(13, "guid-fresh", 500, 5)],
            axum::http::StatusCode::OK,
        )
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let shared = shared_with_app(&api_base).await;

        let outcome = watchdog_poll_once(&shared).await.unwrap();

        assert_eq!(outcome.skipped_grace, 1);
        assert_eq!(outcome.examined, 0);
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        let cursor = shared
            .state
            .store
            .load_webhook_watchdog_cursor("424")
            .await
            .unwrap()
            .expect("cursor persisted");
        assert!(
            cursor.cursor_delivered_at_us.is_none(),
            "the watermark must not skip past a delivery that was never judged"
        );
    }

    #[tokio::test]
    async fn failed_poll_does_not_advance_the_watermark() {
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let (api_base, attempts) = stub_github(
            vec![delivery(15, "guid-any", 500, 600)],
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let shared = shared_with_app(&api_base).await;

        let result = watchdog_poll_once(&shared).await;

        assert!(result.is_err(), "a 500 from the history API is a failure");
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        assert!(
            shared
                .state
                .store
                .load_webhook_watchdog_cursor("424")
                .await
                .unwrap()
                .is_none(),
            "no cursor may be written for a poll that never read history"
        );
        let status = shared.state.webhook_status.watchdog();
        assert!(status.last_error.is_some());
        assert!(status.last_success_at_us.is_none());
    }

    #[test]
    fn next_cursor_is_read_from_the_link_header() {
        let header =
            "<https://api.github.com/app/hook/deliveries?per_page=100&cursor=v1_123>; rel=\"next\"";
        assert_eq!(next_cursor_from_link(header).as_deref(), Some("v1_123"));
        assert_eq!(next_cursor_from_link("<https://x/y>; rel=\"prev\""), None);
    }
}
