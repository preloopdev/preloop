//! Operator surface over the durable webhook queue.
//!
//! Three things an operator needs during an incident, none of which the
//! existing status snapshot answers:
//!
//! * **What is in the queue** — including dead letters, with their errors.
//! * **Replay** — preloop retains delivery payloads for 30 days, ten times
//!   GitHub's three-day redelivery window. Long after GitHub can no longer
//!   resend a delivery, the local copy can still be requeued, and that path
//!   needs neither GitHub nor an App JWT.
//! * **One health document** — queue depth, watchdog freshness, breaker
//!   state and App config drift in one read, because at 03:00 nobody
//!   correlates four endpoints.
//!
//! Payloads are never returned. They are up to 25 MiB, routinely contain
//! repository content, and no listing needs them.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::models::{WebhookDeliveryStatus, WebhookQueueStats};
use crate::state::SharedState;
use crate::webhook_status::{age_seconds, now_us};
use crate::ApiError;

const DEFAULT_LIST_LIMIT: usize = 50;
const MAX_LIST_LIMIT: usize = 500;

#[derive(Debug, Deserialize)]
pub(crate) struct ListQuery {
    state: Option<String>,
    limit: Option<usize>,
}

/// Microsecond epoch to RFC3339, the format every other native endpoint uses.
fn rfc3339(timestamp_us: i64) -> Option<String> {
    chrono::DateTime::from_timestamp_micros(timestamp_us).map(|value| value.to_rfc3339())
}

fn stats_json(stats: &WebhookQueueStats, now: i64) -> Value {
    json!({
        "received": stats.received,
        "processing": stats.processing,
        "done": stats.done,
        "dead_letters": stats.failed,
        "oldest_pending_at": stats.oldest_pending_received_at_us.and_then(rfc3339),
        "oldest_pending_age_seconds": stats
            .oldest_pending_received_at_us
            .map(|received_at| age_seconds(received_at, now)),
    })
}

/// `GET /api/v1/webhooks/deliveries`
pub(crate) async fn list_webhook_deliveries(
    State(shared): State<Arc<SharedState>>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    let state = match query
        .state
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(raw) => Some(WebhookDeliveryStatus::parse(raw).ok_or_else(|| {
            ApiError::bad_request(format!(
                "unknown delivery state {raw:?}; expected received, processing, done or failed"
            ))
        })?),
        None => None,
    };
    let limit = query
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT);
    let deliveries = shared
        .state
        .store
        .list_webhook_deliveries(state, limit)
        .await
        .map_err(|error| ApiError::internal(format!("listing webhook deliveries: {error}")))?;
    let stats = shared
        .state
        .store
        .webhook_queue_stats()
        .await
        .map_err(|error| ApiError::internal(format!("reading webhook queue stats: {error}")))?;
    let now = now_us();
    let deliveries: Vec<Value> = deliveries
        .into_iter()
        .map(|delivery| {
            json!({
                "delivery_id": delivery.delivery_id,
                "event": delivery.event,
                "state": delivery.state.as_str(),
                "attempts": delivery.attempts,
                "received_at": rfc3339(delivery.received_at_us),
                "age_seconds": age_seconds(delivery.received_at_us, now),
                "lease_until": delivery.lease_until_us.and_then(rfc3339),
                "last_error": delivery.last_error,
            })
        })
        .collect();
    Ok(Json(json!({
        "deliveries": deliveries,
        "stats": stats_json(&stats, now),
    })))
}

/// `POST /api/v1/webhooks/deliveries/{delivery_id}/replay`
///
/// Requeues the retained local payload. Distinguishing "unknown" from
/// "already queued" matters: the first means the payload is gone and only
/// GitHub can help, the second means the work is already coming.
pub(crate) async fn replay_webhook_delivery(
    State(shared): State<Arc<SharedState>>,
    Path(delivery_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let requeued = shared
        .state
        .store
        .requeue_webhook_delivery(&delivery_id)
        .await
        .map_err(|error| ApiError::internal(format!("requeueing webhook delivery: {error}")))?;
    if requeued {
        shared.state.webhook_queue_notify.notify_one();
        tracing::warn!(%delivery_id, "webhook delivery replayed from the local payload copy");
        return Ok(Json(json!({
            "delivery_id": delivery_id,
            "status": "requeued",
        })));
    }
    let existing = shared
        .state
        .store
        .get_webhook_delivery(&delivery_id)
        .await
        .map_err(|error| ApiError::internal(format!("reading webhook delivery: {error}")))?;
    match existing {
        Some(delivery) => Err(ApiError::conflict(format!(
            "delivery {delivery_id} is {} and already queued for processing",
            delivery.state.as_str()
        ))),
        None => Err(ApiError::not_found(format!(
            "no retained webhook delivery {delivery_id}; \
             ask GitHub to redeliver it if it is still inside the three-day history window"
        ))),
    }
}

/// `GET /api/v1/webhooks/health`
pub(crate) async fn webhook_health(
    State(shared): State<Arc<SharedState>>,
) -> Result<Json<Value>, ApiError> {
    let stats = shared
        .state
        .store
        .webhook_queue_stats()
        .await
        .map_err(|error| ApiError::internal(format!("reading webhook queue stats: {error}")))?;
    let now = now_us();
    let watchdog = shared.state.webhook_status.watchdog();
    let reconciler = shared.state.webhook_status.reconciler();
    let breaker = shared.state.github_breaker.snapshot();
    let apps: Vec<Value> = shared
        .state
        .webhook_status
        .app_config()
        .into_iter()
        .map(|app| {
            json!({
                "app_id": app.app_id,
                "hook_url": app.hook_url,
                "expected_url": app.expected_url,
                "url_drifted": app.url_drifted(),
                "missing_events": app.missing_events,
                "missing_permissions": app.missing_permissions,
                "healthy": app.healthy(),
                "error": app.error,
            })
        })
        .collect();

    Ok(Json(json!({
        "queue": stats_json(&stats, now),
        "watchdog": {
            "enabled": watchdog.enabled,
            "last_poll_at": watchdog.last_poll_at_us.and_then(rfc3339),
            "last_success_at": watchdog.last_success_at_us.and_then(rfc3339),
            "last_success_age_seconds": watchdog
                .last_success_at_us
                .map(|at| age_seconds(at, now)),
            "last_examined": watchdog.last_examined,
            "redeliveries_requested": watchdog.redeliveries_requested,
            "open_repairs": watchdog.open_repairs,
            "last_error": watchdog.last_error,
        },
        "reconciler": {
            "enabled": reconciler.enabled,
            "last_run_at": reconciler.last_run_at_us.and_then(rfc3339),
            "last_success_at": reconciler.last_success_at_us.and_then(rfc3339),
            "synthesized": reconciler.synthesized,
            "repositories_scanned": reconciler.repositories_scanned,
            "last_error": reconciler.last_error,
        },
        "github_breaker": {
            "open": breaker.open,
            "retry_in_seconds": breaker.retry_in_seconds,
            "consecutive_failures": breaker.consecutive_failures,
            "trips": breaker.trips,
            "rate_limited": breaker.rate_limited,
            "last_error": breaker.last_error,
        },
        "apps": apps,
        "app_config_checked_at": shared
            .state
            .webhook_status
            .config_checked_at_us()
            .and_then(rfc3339),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::WebhookDeliveryRecord;

    async fn shared_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let temp = tempfile::tempdir().unwrap();
        let state = crate::AppState::new(temp.path().to_path_buf())
            .await
            .unwrap();
        (
            Arc::new(SharedState {
                state,
                shutdown: tokio_util::sync::CancellationToken::new(),
            }),
            temp,
        )
    }

    async fn enqueue(shared: &Arc<SharedState>, id: &str) {
        shared
            .state
            .store
            .enqueue_webhook_delivery(&WebhookDeliveryRecord {
                delivery_id: id.to_owned(),
                event: "push".to_owned(),
                payload: br#"{"secret":"do-not-leak"}"#.to_vec(),
                received_at_us: now_us(),
                state: WebhookDeliveryStatus::Received,
                attempts: 0,
                lease_until_us: None,
                lease_token: None,
                last_error: None,
            })
            .await
            .unwrap();
    }

    /// Move a delivery to the dead-letter state the way the queue worker
    /// does, so the replay path is exercised against a real terminal row.
    async fn dead_letter(shared: &Arc<SharedState>, id: &str) {
        // The queue claims strictly FIFO, so claim until this row comes up
        // rather than assuming it is first. Rows claimed on the way are
        // returned to `received` with their attempt refunded, leaving the
        // rest of the queue exactly as the test set it up.
        let mut delivery = None;
        for _ in 0..8 {
            let claimed = shared
                .state
                .store
                .claim_webhook_deliveries(1, 60)
                .await
                .unwrap();
            let Some(claimed) = claimed.into_iter().next() else {
                break;
            };
            if claimed.delivery_id == id {
                delivery = Some(claimed);
                break;
            }
            shared
                .state
                .store
                .park_webhook_delivery(
                    &claimed.delivery_id,
                    claimed.lease_token.as_deref().unwrap(),
                    "not the row under test",
                    // Non-zero, or this row stays the oldest eligible one
                    // and the loop keeps reclaiming it.
                    300,
                )
                .await
                .unwrap();
        }
        let delivery = delivery.expect("claimed the delivery under test");
        shared
            .state
            .store
            .fail_webhook_delivery(
                id,
                delivery.lease_token.as_deref().unwrap(),
                "unreportable",
                true,
                None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn listing_filters_by_state_and_never_returns_payloads() {
        let (shared, _temp) = shared_state().await;
        enqueue(&shared, "queued-one").await;
        enqueue(&shared, "dead-one").await;
        dead_letter(&shared, "dead-one").await;

        let all = list_webhook_deliveries(
            State(shared.clone()),
            Query(ListQuery {
                state: None,
                limit: None,
            }),
        )
        .await
        .unwrap();
        let body = serde_json::to_string(&all.0).unwrap();
        assert!(
            !body.contains("do-not-leak"),
            "payloads must never be listed"
        );
        assert_eq!(all.0["deliveries"].as_array().unwrap().len(), 2);
        assert_eq!(all.0["stats"]["dead_letters"], 1);

        let failed = list_webhook_deliveries(
            State(shared.clone()),
            Query(ListQuery {
                state: Some("failed".to_owned()),
                limit: None,
            }),
        )
        .await
        .unwrap();
        let rows = failed.0["deliveries"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["delivery_id"], "dead-one");
        assert_eq!(rows[0]["last_error"], "unreportable");
    }

    #[tokio::test]
    async fn replaying_a_dead_letter_requeues_it_for_the_worker() {
        let (shared, _temp) = shared_state().await;
        enqueue(&shared, "dead-two").await;
        dead_letter(&shared, "dead-two").await;

        let response = replay_webhook_delivery(State(shared.clone()), Path("dead-two".to_owned()))
            .await
            .unwrap();

        assert_eq!(response.0["status"], "requeued");
        let claimed = shared
            .state
            .store
            .claim_webhook_deliveries(1, 60)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1, "a replayed delivery must be claimable");
        assert_eq!(claimed[0].delivery_id, "dead-two");
        assert_eq!(
            claimed[0].attempts, 1,
            "the replay resets the attempt count"
        );
    }

    #[tokio::test]
    async fn replay_distinguishes_unknown_from_already_queued() {
        let (shared, _temp) = shared_state().await;
        enqueue(&shared, "queued-two").await;

        let unknown = replay_webhook_delivery(State(shared.clone()), Path("nope".to_owned()))
            .await
            .expect_err("unknown delivery");
        assert_eq!(unknown.status(), axum::http::StatusCode::NOT_FOUND);

        let active = replay_webhook_delivery(State(shared.clone()), Path("queued-two".to_owned()))
            .await
            .expect_err("already queued");
        assert_eq!(active.status(), axum::http::StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn health_reports_queue_watchdog_and_breaker_in_one_document() {
        let (shared, _temp) = shared_state().await;
        enqueue(&shared, "health-one").await;

        let health = webhook_health(State(shared.clone())).await.unwrap();

        assert_eq!(health.0["queue"]["received"], 1);
        assert!(health.0["queue"]["oldest_pending_age_seconds"].is_number());
        assert_eq!(health.0["watchdog"]["enabled"], false);
        assert_eq!(health.0["github_breaker"]["open"], false);
        assert!(health.0["apps"].is_array());
    }
}
