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
//!   restore from an old snapshot, corruption. Both sides look healthy; only
//!   a join between GitHub's GUIDs and the local `webhook_deliveries` table
//!   reveals it. This is the dangerous one, because nothing anywhere is
//!   alarming.
//!
//! Everything here is deliberately conservative:
//!
//! * A **grace window** keeps a merely-late delivery from being declared
//!   lost. GitHub deliveries are not immediate, and "missing after 30s"
//!   would manufacture duplicate work every day.
//! * A store's **first poll adopts the history** before it. A new store
//!   (cutover, deleted state dir) has no rows for deliveries another store
//!   already handled; repairing them would replay three days of closed PRs
//!   and superseded pushes. The adoption line is drawn at the first poll
//!   *attempt* and persisted before the history call, so a history outage
//!   cannot move it forward past deliveries that failed while polling was
//!   down. Recover a known gap by asking GitHub to redeliver it
//!   (`POST /app/hook/deliveries/{id}/attempts`).
//! * An **open repair is retried** until the delivery lands locally or the
//!   attempt cap is hit, whatever the watermark says. Judging is one-shot:
//!   the watermark advances past everything examined, so a delivery judged
//!   missing below it would otherwise never be looked at again, and the
//!   repair row would sit open (and the webhook stay missing) forever.
//! * The watermark **never advances on a failed poll**, so an error cannot
//!   silently skip a range of history.
//! * An **attempt is claimed before the request**: the attempt count and the
//!   last-attempt clock move in one conditional store write that only
//!   succeeds if the row still looks like the pass's read. Two watchdogs
//!   overlapping (a restart, a second server on the store) cannot both ask
//!   GitHub to redeliver the same delivery, nor charge it twice.
//! * Nothing is redelivered while the local store is unhealthy — replaying a
//!   payload into a broken store loses it a second time, and burns one of
//!   the finite redelivery opportunities doing it.
//! * The breaker gates the poll: during a GitHub outage a poll would fail
//!   anyway, and hammering a failing dependency is what the breaker exists
//!   to prevent.

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use crate::ControlBackend;
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
/// Cap on repair rows read for the backlog gauge and the retry pass. A
/// number this large is already an incident; the exact count past it changes
/// no decision.
const OPEN_REPAIR_SCAN_LIMIT: usize = 1000;

/// What one full watchdog pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WatchdogPollOutcome {
    /// Deliveries old enough to judge, and judged.
    pub examined: u64,
    /// Examined deliveries with no local row, whatever GitHub thinks.
    pub missing_locally: u64,
    /// Redelivery attempts actually requested from GitHub.
    pub redelivered: u64,
    /// Deliveries too young to judge yet.
    pub skipped_grace: u64,
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
/// Compare GitHub history positions without relying on timestamp uniqueness.
fn item_is_at_or_before(
    delivered_at_us: i64,
    guid: &str,
    watermark_us: i64,
    watermark_guid: Option<&str>,
) -> bool {
    delivered_at_us < watermark_us
        || (delivered_at_us == watermark_us
            && watermark_guid.is_some_and(|watermark_guid| guid <= watermark_guid))
}

fn newest_marker(
    current: Option<(i64, String)>,
    delivered_at_us: i64,
    guid: &str,
) -> Option<(i64, String)> {
    let candidate = (delivered_at_us, guid.to_owned());
    Some(match current {
        Some(current) => current.max(candidate),
        None => candidate,
    })
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
pub async fn run_webhook_watchdog(shared: Arc<SharedState>) {
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
pub async fn watchdog_poll_once(shared: &Arc<SharedState>) -> anyhow::Result<WatchdogPollOutcome> {
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
        let now = now_us();
        status.first_poll_at_us.get_or_insert(now);
        status.last_poll_at_us = Some(now);
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
    let store_healthy = shared.state.backend.webhook_queue_stats().await.is_ok();
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
        .backend
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
        .backend
        .load_webhook_watchdog_cursor(&app.app_id)
        .await?;
    let attempted_at = now_us();
    let grace_boundary = attempted_at - grace_us();
    let previous_watermark = previous
        .as_ref()
        .and_then(|cursor| cursor.cursor_delivered_at_us);
    // A store with no watermark has never judged this App's history, so
    // everything GitHub logged before it was acked (or failed) by some other
    // store. Replaying that history re-runs closed PRs and superseded
    // pushes, so the first poll adopts it: the watermark starts at the grace
    // boundary, and only later deliveries are judged. The boundary is drawn
    // and persisted now, before the history call — not at the first
    // *successful* poll — so a history outage cannot push the adoption line
    // past deliveries that failed while polling was down and make their
    // edge failures look adopted instead of missing.
    let (watermark, watermark_guid) = match previous_watermark {
        Some(watermark) => (
            watermark,
            previous
                .as_ref()
                .and_then(|cursor| cursor.cursor_delivered_at_guid.clone()),
        ),
        None => {
            let adopted = WebhookWatchdogCursor {
                scope: app.app_id.clone(),
                cursor_delivered_at_us: Some(grace_boundary),
                cursor_delivered_at_guid: None,
                // A scan cursor saved by a build that had no watermark yet
                // belongs to a scan of history this store now adopts; the
                // repairs that scan opened are retried below instead.
                scan_cursor: None,
                last_poll_at_us: Some(attempted_at),
                last_success_at_us: previous
                    .as_ref()
                    .and_then(|cursor| cursor.last_success_at_us),
            };
            shared
                .state
                .backend
                .store_webhook_watchdog_cursor(&adopted)
                .await?;
            // The adoption line is not a judged delivery, so it has no guid
            // tie-breaker for deliveries sharing its timestamp.
            (grace_boundary, None)
        }
    };

    let mut outcome = WatchdogPollOutcome::default();
    if store_healthy {
        // Judging is one-shot: anything the scan has examined is at or below
        // the watermark and will never be examined again, so repairs the
        // scan opened must be driven from the repair table itself.
        outcome.redelivered += retry_open_repairs(shared, app, &api_base, &jwt).await?;
    }
    let mut newest_examined: Option<(i64, String)> = None;
    let mut cursor = previous
        .filter(|_| previous_watermark.is_some())
        .and_then(|cursor| cursor.scan_cursor);
    let mut reached_watermark = false;
    let mut has_more_pages = false;
    for _page in 0..max_pages().max(1) {
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
            if item_is_at_or_before(
                delivered_at_us,
                &item.guid,
                watermark,
                watermark_guid.as_deref(),
            ) {
                reached_watermark = true;
                continue;
            }
            if delivered_at_us > grace_boundary {
                outcome.skipped_grace += 1;
                continue;
            }
            newest_examined = newest_marker(newest_examined, delivered_at_us, &item.guid);
            examined.push(item);
        }

        if !examined.is_empty() {
            outcome.examined += examined.len() as u64;
            let guids: Vec<String> = examined.iter().map(|item| item.guid.clone()).collect();
            let present = shared
                .state
                .backend
                .webhook_deliveries_present(&guids)
                .await?;
            for item in &examined {
                if present.contains(&item.guid) {
                    // A repair that finally landed. Closing it here is what
                    // keeps the backlog gauge meaningful.
                    if let Err(error) = shared
                        .state
                        .backend
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
                    && repair_delivery(
                        shared,
                        app,
                        RepairTarget {
                            delivery_id: item.id,
                            guid: &item.guid,
                            event: Some(item.event.as_str()),
                            reason,
                            throttled: item.throttled_at.is_some(),
                        },
                        &api_base,
                        &jwt,
                    )
                    .await?
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
    let (advanced, advanced_guid) = if reached_watermark || !has_more_pages {
        match newest_examined {
            Some((newest, guid))
                if newest > watermark
                    || (newest == watermark
                        && watermark_guid
                            .as_ref()
                            .is_none_or(|previous_guid| guid > *previous_guid)) =>
            {
                (newest, Some(guid))
            }
            _ => (watermark, watermark_guid),
        }
    } else {
        // Truncated at max_pages(): do not advance the watermark across an
        // unvisited range. Resume from the saved opaque cursor next pass.
        (watermark, watermark_guid)
    };
    let scan_cursor = if reached_watermark || !has_more_pages {
        None
    } else {
        cursor
    };
    shared
        .state
        .backend
        .store_webhook_watchdog_cursor(&WebhookWatchdogCursor {
            scope: app.app_id.clone(),
            cursor_delivered_at_us: Some(advanced),
            cursor_delivered_at_guid: advanced_guid,
            scan_cursor,
            last_poll_at_us: Some(now),
            last_success_at_us: Some(now),
        })
        .await?;
    Ok(outcome)
}

/// One delivery the watchdog wants GitHub to attempt again.
struct RepairTarget<'a> {
    /// Numeric delivery id: the redelivery endpoint takes this, not the GUID.
    delivery_id: i64,
    guid: &'a str,
    /// Event name, for the log line only. Absent when the target comes from
    /// the repair table, which does not store it.
    event: Option<&'a str>,
    reason: WebhookRepairReason,
    /// GitHub had already delayed this delivery once.
    throttled: bool,
}

/// Retry the App's open repairs, independently of the history scan.
///
/// A repair row is a delivery the watchdog already judged missing; its saved
/// delivery id is all the redelivery endpoint needs. The watermark decides
/// only which *unjudged* history a pass examines, so without this pass a
/// judged-missing delivery below it — a redelivery request GitHub refused, a
/// redelivery that has not landed, or a repair left by an older build whose
/// first scan never finished — is never looked at again, and its row stays
/// open (with the webhook still missing) forever. Rows whose delivery has
/// landed locally are closed; the rest are requested again under the same
/// per-GUID backoff and attempt cap as the scan, which keeps the retry rate
/// bounded and never replays untracked history.
///
/// The scan is App-scoped and skips rows at the attempt cap *in the query*:
/// the window is bounded, and a global scan would let another App's backlog,
/// or rows already at the cap (never retried again), fill it and starve this
/// App's repairs out of it.
async fn retry_open_repairs(
    shared: &Arc<SharedState>,
    app: &GitHubAppCredentials,
    api_base: &str,
    jwt: &str,
) -> anyhow::Result<u64> {
    let open = shared
        .state
        .backend
        .retryable_webhook_redeliveries(&app.app_id, max_attempts(), OPEN_REPAIR_SCAN_LIMIT)
        .await?;
    if open.is_empty() {
        return Ok(0);
    }
    let guids: Vec<String> = open
        .iter()
        .map(|record| record.delivery_guid.clone())
        .collect();
    let present = shared
        .state
        .backend
        .webhook_deliveries_present(&guids)
        .await?;
    let now = now_us();
    let mut redelivered = 0;
    for record in &open {
        if present.contains(&record.delivery_guid) {
            // A repair that finally landed. Closing it here is what keeps
            // the backlog gauge meaningful. The close is best-effort: one
            // store hiccup must not abort the pass before the history scan
            // runs — the row simply closes on a later pass.
            if let Err(error) = shared
                .state
                .backend
                .resolve_webhook_redelivery(&record.delivery_guid, now)
                .await
            {
                tracing::warn!(guid = %record.delivery_guid, ?error, "failed to close webhook repair");
            }
            continue;
        }
        let target = RepairTarget {
            delivery_id: record.github_delivery_id,
            guid: &record.delivery_guid,
            event: None,
            reason: record.reason,
            throttled: false,
        };
        if repair_delivery(shared, app, target, api_base, jwt).await? {
            redelivered += 1;
        }
    }
    Ok(redelivered)
}

/// Ask GitHub to attempt one delivery again, subject to per-GUID backoff and
/// the attempt cap. Returns whether an attempt was actually requested.
///
/// The attempt is claimed in the store before the request goes out, so a
/// pass that loses the claim (another watchdog got there first, or the
/// delivery landed and its row was closed) sends nothing.
async fn repair_delivery(
    shared: &Arc<SharedState>,
    app: &GitHubAppCredentials,
    target: RepairTarget<'_>,
    api_base: &str,
    jwt: &str,
) -> anyhow::Result<bool> {
    let now = now_us();
    let observed = shared
        .state
        .backend
        .load_webhook_redelivery(target.guid)
        .await?
        .unwrap_or(WebhookRedeliveryRecord {
            delivery_guid: target.guid.to_owned(),
            github_delivery_id: target.delivery_id,
            app_id: app.app_id.clone(),
            reason: target.reason,
            attempts: 0,
            first_seen_at_us: now,
            last_attempt_at_us: None,
            resolved_at_us: None,
            last_error: None,
        });
    // This pass's view of the row, with the reason and delivery id the
    // history scan just read: the claim persists both.
    let candidate = WebhookRedeliveryRecord {
        github_delivery_id: target.delivery_id,
        reason: target.reason,
        ..observed.clone()
    };

    if observed.attempts >= max_attempts() {
        // Kept open on purpose. A GUID we could never get back is a standing
        // finding for an operator, not something to forget.
        let message = format!(
            "redelivery cap of {} attempts reached; repair the ingress and replay manually",
            max_attempts()
        );
        if observed.last_error.as_deref() != Some(message.as_str()) {
            let mut capped = candidate.clone();
            capped.last_error = Some(message);
            shared
                .state
                .backend
                .upsert_webhook_redelivery(&capped)
                .await?;
        }
        return Ok(false);
    }
    if let Some(last_attempt) = observed.last_attempt_at_us
        && now - last_attempt < redelivery_backoff_us(observed.attempts)
    {
        return Ok(false);
    }

    // The attempt is taken *before* the request, in one conditional store
    // write. A row that moved since the read above — another watchdog
    // claiming the same delivery during a restart overlap, or the scan path
    // closing a delivery that just landed — fails the compare, and this pass
    // sends nothing: exactly one request and one charged attempt per
    // delivery, whoever wins. A row the caller read as resolved reopens here
    // (the claim writes `resolved_at = NULL`): the delivery is missing again
    // after a restore, a prune or a replay.
    let Some(claimed) = shared
        .state
        .backend
        .claim_webhook_redelivery(&candidate, now)
        .await?
    else {
        tracing::debug!(
            guid = %target.guid,
            "webhook repair claimed elsewhere; leaving this attempt to the claim"
        );
        return Ok(false);
    };
    let mut record = claimed;

    let url = format!(
        "{}/app/hook/deliveries/{}/attempts",
        api_base.trim_end_matches('/'),
        target.delivery_id
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

    // The claim already charged this attempt, moved the last-attempt clock
    // and cleared the previous attempt's error, so a request that went
    // through has nothing left to record; only a failure writes.
    let requested = match response {
        Ok(response) if response.status().is_success() => {
            tracing::warn!(
                guid = %target.guid,
                delivery_id = target.delivery_id,
                event = %target.event.unwrap_or_default(),
                reason = target.reason.as_str(),
                attempt = record.attempts,
                throttled = target.throttled,
                "requested GitHub webhook redelivery"
            );
            return Ok(true);
        }
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            record.last_error = Some(format!("redelivery request failed: {status}: {body}"));
            tracing::warn!(guid = %target.guid, %status, "GitHub refused the redelivery request");
            false
        }
        Err(error) => {
            record.last_error = Some(format!("redelivery request failed: {error}"));
            tracing::warn!(guid = %target.guid, %error, "redelivery request could not be sent");
            false
        }
    };
    shared
        .state
        .backend
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
    use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};

    /// 2048-bit key generation costs about a second; every test in this
    /// module only needs *a* valid key, so it is generated once.
    static TEST_KEY: std::sync::LazyLock<rsa::RsaPrivateKey> = std::sync::LazyLock::new(|| {
        rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).expect("test key")
    });

    #[derive(Clone)]
    struct StubState {
        deliveries: Arc<parking_lot::Mutex<Vec<serde_json::Value>>>,
        attempts: Arc<AtomicUsize>,
        list_status: Arc<AtomicU16>,
    }

    /// Handle to a stub whose history status and delivery list can change
    /// mid-test.
    struct GithubStub {
        api_base: String,
        attempts: Arc<AtomicUsize>,
        list_status: Arc<AtomicU16>,
        deliveries: Arc<parking_lot::Mutex<Vec<serde_json::Value>>>,
    }

    impl GithubStub {
        fn set_list_status(&self, status: axum::http::StatusCode) {
            self.list_status.store(status.as_u16(), Ordering::SeqCst);
        }

        /// Append a delivery the next history read returns.
        fn push_delivery(&self, item: serde_json::Value) {
            self.deliveries.lock().push(item);
        }

        fn attempts(&self) -> usize {
            self.attempts.load(Ordering::SeqCst)
        }
    }

    async fn stub_github(
        deliveries: Vec<serde_json::Value>,
        list_status: axum::http::StatusCode,
    ) -> (String, Arc<AtomicUsize>) {
        let stub = stub_github_mutable(deliveries, list_status).await;
        (stub.api_base.clone(), stub.attempts.clone())
    }

    async fn stub_github_mutable(
        deliveries: Vec<serde_json::Value>,
        list_status: axum::http::StatusCode,
    ) -> GithubStub {
        let attempts = Arc::new(AtomicUsize::new(0));
        let list_status = Arc::new(AtomicU16::new(list_status.as_u16()));
        let deliveries = Arc::new(parking_lot::Mutex::new(deliveries));
        let state = StubState {
            deliveries: deliveries.clone(),
            attempts: attempts.clone(),
            list_status: list_status.clone(),
        };
        let router = Router::new()
            .route(
                "/app/hook/deliveries",
                get(|State(state): State<StubState>| async move {
                    let status =
                        axum::http::StatusCode::from_u16(state.list_status.load(Ordering::SeqCst))
                            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                    if !status.is_success() {
                        return (status, Json(json!({"message": "boom"})));
                    }
                    let items = state.deliveries.lock().clone();
                    (
                        axum::http::StatusCode::OK,
                        Json(serde_json::Value::Array(items)),
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
        GithubStub {
            api_base: format!("http://{addr}"),
            attempts,
            list_status,
            deliveries,
        }
    }

    async fn shared_with_app(api_base: &str) -> (tempfile::TempDir, Arc<SharedState>) {
        let temp = tempfile::tempdir().unwrap();
        let mut state = crate::AppState::new(temp.path().to_path_buf())
            .await
            .unwrap();
        state.github_app = Some(GitHubAppCredentials::for_tests(
            "424",
            TEST_KEY.clone(),
            crate::github_app::MintFailurePolicy::LocalJwt,
        ));
        let _ = api_base;
        (
            temp,
            Arc::new(SharedState {
                state,
                shutdown: tokio_util::sync::CancellationToken::new(),
            }),
        )
    }

    fn delivery(id: i64, guid: &str, status_code: u16, age_secs: i64) -> serde_json::Value {
        delivery_at(id, guid, status_code, now_us() - age_secs * 1_000_000)
    }

    /// A delivery with an exact `delivered_at`, for tests that must place it
    /// relative to a boundary the code persisted.
    fn delivery_at(
        id: i64,
        guid: &str,
        status_code: u16,
        delivered_at_us: i64,
    ) -> serde_json::Value {
        let delivered_at = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(delivered_at_us)
            .expect("test delivery time");
        json!({
            "id": id,
            "guid": guid,
            "delivered_at": delivered_at.to_rfc3339(),
            "status": if (200..300).contains(&status_code) { "OK" } else { "failed" },
            "status_code": status_code,
            "event": "push",
        })
    }

    /// The store already judged GitHub's history up to `age_secs` ago.
    async fn seed_watermark(shared: &Arc<SharedState>, age_secs: i64) {
        shared
            .state
            .backend
            .store_webhook_watchdog_cursor(&WebhookWatchdogCursor {
                scope: "424".to_owned(),
                cursor_delivered_at_us: Some(now_us() - age_secs * 1_000_000),
                cursor_delivered_at_guid: None,
                scan_cursor: None,
                last_poll_at_us: None,
                last_success_at_us: None,
            })
            .await
            .unwrap();
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
        let (_temp, shared) = shared_with_app(&api_base).await;
        seed_watermark(&shared, 3600).await;

        let outcome = watchdog_poll_once(&shared).await.unwrap();

        assert_eq!(outcome.examined, 1);
        assert_eq!(outcome.missing_locally, 1);
        assert_eq!(outcome.redelivered, 1);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        let repair = shared
            .state
            .backend
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
        let (_temp, shared) = shared_with_app(&api_base).await;
        seed_watermark(&shared, 3600).await;

        let outcome = watchdog_poll_once(&shared).await.unwrap();

        assert_eq!(
            outcome.redelivered, 1,
            "a remote success with no local row must still be repaired"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(
            shared
                .state
                .backend
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
        let (_temp, shared) = shared_with_app(&api_base).await;
        seed_watermark(&shared, 3600).await;
        shared
            .state
            .backend
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
    async fn first_poll_adopts_history_from_before_the_store() {
        // A new store (cutover, deleted state dir) finds GitHub's whole
        // history without local rows; another store already handled it.
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let (api_base, attempts) = stub_github(
            vec![
                delivery(21, "guid-old-ack", 202, 600),
                delivery(22, "guid-old-fail", 502, 900),
            ],
            axum::http::StatusCode::OK,
        )
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let (_temp, shared) = shared_with_app(&api_base).await;

        let outcome = watchdog_poll_once(&shared).await.unwrap();

        assert_eq!(outcome.redelivered, 0);
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        for guid in ["guid-old-ack", "guid-old-fail"] {
            assert!(
                shared
                    .state
                    .backend
                    .load_webhook_redelivery(guid)
                    .await
                    .unwrap()
                    .is_none(),
                "{guid} predates the store and must not become a repair"
            );
        }
        let watermark = shared
            .state
            .backend
            .load_webhook_watchdog_cursor("424")
            .await
            .unwrap()
            .and_then(|cursor| cursor.cursor_delivered_at_us)
            .expect("the adopted range is persisted");
        assert!(
            watermark > now_us() - 600 * 1_000_000,
            "the watermark covers the adopted history"
        );
    }

    #[tokio::test]
    async fn delivery_inside_the_grace_window_is_left_alone() {
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let delivered_before = now_us() - 5_000_000;
        let (api_base, attempts) = stub_github(
            vec![delivery(13, "guid-fresh", 500, 5)],
            axum::http::StatusCode::OK,
        )
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let (_temp, shared) = shared_with_app(&api_base).await;

        let outcome = watchdog_poll_once(&shared).await.unwrap();

        assert_eq!(outcome.skipped_grace, 1);
        assert_eq!(outcome.examined, 0);
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        let cursor = shared
            .state
            .backend
            .load_webhook_watchdog_cursor("424")
            .await
            .unwrap()
            .expect("cursor persisted");
        assert!(
            cursor
                .cursor_delivered_at_us
                .is_some_and(|watermark| watermark < delivered_before),
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
        let (_temp, shared) = shared_with_app(&api_base).await;
        seed_watermark(&shared, 3600).await;
        let seeded = shared
            .state
            .backend
            .load_webhook_watchdog_cursor("424")
            .await
            .unwrap()
            .and_then(|cursor| cursor.cursor_delivered_at_us);

        let result = watchdog_poll_once(&shared).await;

        assert!(result.is_err(), "a 500 from the history API is a failure");
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        assert_eq!(
            shared
                .state
                .backend
                .load_webhook_watchdog_cursor("424")
                .await
                .unwrap()
                .and_then(|cursor| cursor.cursor_delivered_at_us),
            seeded,
            "a failed poll must leave the watermark exactly where it was"
        );
        let status = shared.state.webhook_status.watchdog();
        assert!(status.last_error.is_some());
        assert!(status.last_success_at_us.is_none());
    }

    #[tokio::test]
    async fn failed_first_poll_persists_the_adoption_boundary() {
        // The adoption line must be fixed at the first attempt. Redrawing it
        // at the first successful poll would push it past every delivery
        // that failed while history calls were down.
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _grace = crate::state::TestEnvVar::set("PRELOOP_WEBHOOK_WATCHDOG_GRACE_SECS", "120");
        let (api_base, attempts) =
            stub_github(Vec::new(), axum::http::StatusCode::INTERNAL_SERVER_ERROR).await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let (_temp, shared) = shared_with_app(&api_base).await;

        assert!(watchdog_poll_once(&shared).await.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 0);

        let cursor = shared
            .state
            .backend
            .load_webhook_watchdog_cursor("424")
            .await
            .unwrap()
            .expect("the adopted range is persisted before the history call");
        let watermark = cursor.cursor_delivered_at_us.expect("adoption watermark");
        let (grace, now) = (grace_us(), now_us());
        assert!(
            watermark <= now - grace && watermark > now - grace - 60_000_000,
            "expected the first attempt's grace boundary ({}), got {watermark}",
            now - grace
        );
        assert!(cursor.scan_cursor.is_none());
    }

    #[tokio::test]
    async fn delayed_first_poll_judges_deliveries_from_the_outage() {
        // A store starts at T; history calls then fail for a while; a
        // delivery fails during that outage. When history recovers it must
        // be judged and repaired: it sits above the boundary drawn at T, so
        // adoption must not swallow it the way a recovery-time boundary
        // would.
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _grace = crate::state::TestEnvVar::set("PRELOOP_WEBHOOK_WATCHDOG_GRACE_SECS", "120");
        let stub =
            stub_github_mutable(Vec::new(), axum::http::StatusCode::INTERNAL_SERVER_ERROR).await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &stub.api_base);
        let (_temp, shared) = shared_with_app(&stub.api_base).await;

        assert!(watchdog_poll_once(&shared).await.is_err());
        assert_eq!(stub.attempts(), 0);

        // The delivery arrives one second after the boundary the failed
        // attempt persisted — i.e. while polling is down.
        let boundary = shared
            .state
            .backend
            .load_webhook_watchdog_cursor("424")
            .await
            .unwrap()
            .and_then(|cursor| cursor.cursor_delivered_at_us)
            .expect("a failed first poll persists the adoption boundary");
        stub.push_delivery(delivery_at(31, "guid-outage", 502, boundary + 1_000_000));

        // Recover more than a second after the attempt, so the delivery is
        // older than the recovery grace boundary and gets judged rather than
        // skipped as merely late.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        stub.set_list_status(axum::http::StatusCode::OK);

        let outcome = watchdog_poll_once(&shared).await.unwrap();

        assert_eq!(outcome.examined, 1, "the outage delivery must be judged");
        assert_eq!(outcome.redelivered, 1, "and repaired");
        assert_eq!(stub.attempts(), 1);
    }

    /// One repair row with a chosen owner, attempt count and history:
    /// first seen `first_seen_secs_ago`, last tried `last_attempt_secs_ago`
    /// (never, when `None`).
    async fn seed_repair_row(
        shared: &Arc<SharedState>,
        guid: &str,
        delivery_id: i64,
        app_id: &str,
        attempts: u32,
        first_seen_secs_ago: i64,
        last_attempt_secs_ago: Option<i64>,
    ) {
        shared
            .state
            .backend
            .upsert_webhook_redelivery(&WebhookRedeliveryRecord {
                delivery_guid: guid.to_owned(),
                github_delivery_id: delivery_id,
                app_id: app_id.to_owned(),
                reason: WebhookRepairReason::RemoteFailure,
                attempts,
                first_seen_at_us: now_us() - first_seen_secs_ago * 1_000_000,
                last_attempt_at_us: last_attempt_secs_ago.map(|secs| now_us() - secs * 1_000_000),
                resolved_at_us: None,
                last_error: Some("redelivery request failed: 500".to_owned()),
            })
            .await
            .unwrap();
    }

    /// An open repair row for `guid`, as an older pass would have left it:
    /// one attempt, long past its backoff.
    async fn seed_open_repair(shared: &Arc<SharedState>, guid: &str, delivery_id: i64) {
        seed_repair_row(shared, guid, delivery_id, "424", 1, 3600, Some(3600)).await;
    }

    #[tokio::test]
    async fn open_repairs_below_the_watermark_are_retried() {
        // The scan never looks below the watermark, so a repair row is the
        // only thing keeping a judged-missing delivery alive. Its saved
        // delivery id is enough to ask GitHub again.
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let (api_base, attempts) = stub_github(
            vec![delivery(42, "guid-stranded", 502, 900)],
            axum::http::StatusCode::OK,
        )
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let (_temp, shared) = shared_with_app(&api_base).await;
        seed_watermark(&shared, 600).await;
        seed_open_repair(&shared, "guid-stranded", 42).await;

        let outcome = watchdog_poll_once(&shared).await.unwrap();

        assert_eq!(outcome.examined, 0, "the old delivery stays adopted");
        assert_eq!(outcome.redelivered, 1, "but its repair is retried");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(
            shared
                .state
                .backend
                .load_webhook_redelivery("guid-stranded")
                .await
                .unwrap()
                .unwrap()
                .attempts,
            2
        );
    }

    #[tokio::test]
    async fn open_repairs_close_when_the_delivery_landed() {
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let (api_base, attempts) = stub_github(Vec::new(), axum::http::StatusCode::OK).await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let (_temp, shared) = shared_with_app(&api_base).await;
        seed_open_repair(&shared, "guid-landed", 7).await;
        shared
            .state
            .backend
            .enqueue_webhook_delivery(&WebhookDeliveryRecord {
                delivery_id: "guid-landed".to_owned(),
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

        assert_eq!(
            outcome.redelivered, 0,
            "a landed delivery is not redelivered"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        assert!(
            shared
                .state
                .backend
                .load_webhook_redelivery("guid-landed")
                .await
                .unwrap()
                .unwrap()
                .resolved_at_us
                .is_some(),
            "the repair row must close once the delivery is present"
        );
    }

    #[tokio::test]
    async fn upgrade_from_a_null_watermark_cursor_retries_its_repairs() {
        // An older build could persist a scan cursor with no watermark when
        // its first scan hit the page limit. That unfinished scan is
        // adopted — not resumed, and its untracked history is not replayed —
        // but the repairs it opened are still retried.
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let (api_base, attempts) = stub_github(
            vec![delivery(42, "guid-upgraded", 502, 900)],
            axum::http::StatusCode::OK,
        )
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let (_temp, shared) = shared_with_app(&api_base).await;
        shared
            .state
            .backend
            .store_webhook_watchdog_cursor(&WebhookWatchdogCursor {
                scope: "424".to_owned(),
                cursor_delivered_at_us: None,
                cursor_delivered_at_guid: None,
                scan_cursor: Some("v1_old_page".to_owned()),
                last_poll_at_us: None,
                last_success_at_us: None,
            })
            .await
            .unwrap();
        seed_open_repair(&shared, "guid-upgraded", 42).await;

        let outcome = watchdog_poll_once(&shared).await.unwrap();

        assert_eq!(outcome.examined, 0, "the old scan's range is adopted");
        assert_eq!(outcome.redelivered, 1, "its repair is preserved");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        let cursor = shared
            .state
            .backend
            .load_webhook_watchdog_cursor("424")
            .await
            .unwrap()
            .unwrap();
        assert!(
            cursor.cursor_delivered_at_us.is_some(),
            "boundary persisted"
        );
        assert!(
            cursor.scan_cursor.is_none(),
            "the pre-adoption scan must not be resumed"
        );
    }

    #[tokio::test]
    async fn retry_scan_reaches_repairs_behind_a_full_backlog() {
        // The retry pass reads a bounded window. Filled by another App's
        // backlog or by rows of this App that have burned every attempt,
        // the window never reaches a repair that can still be made; the
        // scan is scoped and capped in the query, so the starvers cannot
        // occupy it.
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let (api_base, attempts) = stub_github(Vec::new(), axum::http::StatusCode::OK).await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let (_temp, shared) = shared_with_app(&api_base).await;
        seed_watermark(&shared, 600).await;
        // Oldest first: half the window belongs to another App, half to
        // capped rows of this App.
        for i in 0..500 {
            seed_repair_row(
                &shared,
                &format!("guid-other-{i}"),
                1000 + i,
                "999",
                1,
                7200,
                Some(7200),
            )
            .await;
        }
        for i in 0..500 {
            seed_repair_row(
                &shared,
                &format!("guid-capped-{i}"),
                2000 + i,
                "424",
                max_attempts(),
                7100,
                Some(7100),
            )
            .await;
        }
        seed_repair_row(&shared, "guid-retryable", 3000, "424", 1, 3600, Some(3600)).await;

        let outcome = watchdog_poll_once(&shared).await.unwrap();

        assert_eq!(
            outcome.redelivered, 1,
            "the one repair that can still be made is reached"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(
            shared
                .state
                .backend
                .load_webhook_redelivery("guid-retryable")
                .await
                .unwrap()
                .unwrap()
                .attempts,
            2
        );
        // The backlog gauge still reads the global query: capped rows stay
        // counted (up to its own window).
        assert_eq!(
            shared
                .state
                .backend
                .open_webhook_redeliveries(OPEN_REPAIR_SCAN_LIMIT)
                .await
                .unwrap()
                .len(),
            OPEN_REPAIR_SCAN_LIMIT,
            "capped and other-App rows must remain in the gauge's backlog"
        );
    }

    #[tokio::test]
    async fn retry_scan_prefers_the_longest_waiting_repairs() {
        // A window full of rows still in backoff must not hide a row that
        // is already due: rows are read longest-since-last-attempt first,
        // not oldest-seen first.
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let (api_base, attempts) = stub_github(Vec::new(), axum::http::StatusCode::OK).await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let (_temp, shared) = shared_with_app(&api_base).await;
        seed_watermark(&shared, 600).await;
        // Seen two hours ago, tried one second ago: five minutes of backoff
        // left, and older by first sight than the row that is due.
        for i in 0..1000 {
            seed_repair_row(
                &shared,
                &format!("guid-backoff-{i}"),
                4000 + i,
                "424",
                1,
                7200,
                Some(1),
            )
            .await;
        }
        // Seen an hour ago, last tried fifty minutes ago: due now, but the
        // newest by first sight.
        seed_repair_row(&shared, "guid-due", 5000, "424", 1, 3600, Some(3000)).await;

        let outcome = watchdog_poll_once(&shared).await.unwrap();

        assert_eq!(outcome.redelivered, 1, "the due repair is reached");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(
            shared
                .state
                .backend
                .load_webhook_redelivery("guid-due")
                .await
                .unwrap()
                .unwrap()
                .attempts,
            2
        );
        assert_eq!(
            shared
                .state
                .backend
                .load_webhook_redelivery("guid-backoff-0")
                .await
                .unwrap()
                .unwrap()
                .attempts,
            1,
            "rows inside their backoff window are not re-requested"
        );
    }

    #[test]
    fn next_cursor_is_read_from_the_link_header() {
        let header =
            "<https://api.github.com/app/hook/deliveries?per_page=100&cursor=v1_123>; rel=\"next\"";
        assert_eq!(next_cursor_from_link(header).as_deref(), Some("v1_123"));
        assert_eq!(next_cursor_from_link("<https://x/y>; rel=\"prev\""), None);
    }
}
