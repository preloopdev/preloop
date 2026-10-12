//! Webhook inbox (`webhook_deliveries`), watchdog cursor and redelivery
//! repair rows. Every method is one statement or one short transaction.

use super::codec::now_us;
use super::{LiteBackend, db};
use crate::control::types::ControlError;
use crate::models::{
    WebhookDeliveryRecord, WebhookDeliveryStatus, WebhookDeliverySummary, WebhookQueueStats,
    WebhookRedeliveryRecord, WebhookRepairReason, WebhookWatchdogCursor,
};
use rusqlite::{OptionalExtension, params};
use std::collections::BTreeSet;

/// A still-owned processing lease: the fence every lease-holder write
/// carries (`?token`, `?now` bound by the caller).
const OWNED_LEASE: &str = "state = 'processing' AND lease_token = :token \
     AND lease_until IS NOT NULL AND lease_until > :now";

fn lease_until(now: i64, secs: u64) -> i64 {
    now.saturating_add((secs as i64).saturating_mul(1_000_000))
}

fn parse_state(state: &str) -> Result<WebhookDeliveryStatus, ControlError> {
    WebhookDeliveryStatus::parse(state).ok_or_else(|| {
        ControlError::backend(anyhow::anyhow!("invalid webhook delivery state: {state}"))
    })
}

fn redelivery_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(WebhookRedeliveryRecord, String)> {
    let reason: String = row.get(3)?;
    let attempts: i64 = row.get(4)?;
    Ok((
        WebhookRedeliveryRecord {
            delivery_guid: row.get(0)?,
            github_delivery_id: row.get(1)?,
            app_id: row.get(2)?,
            // Placeholder; replaced after the reason string is validated.
            reason: WebhookRepairReason::RemoteFailure,
            attempts: attempts.max(0) as u32,
            first_seen_at_us: row.get(5)?,
            last_attempt_at_us: row.get(6)?,
            resolved_at_us: row.get(7)?,
            last_error: row.get(8)?,
        },
        reason,
    ))
}

fn finish_redelivery(
    (mut record, reason): (WebhookRedeliveryRecord, String),
) -> Result<WebhookRedeliveryRecord, ControlError> {
    record.reason = WebhookRepairReason::parse(&reason).ok_or_else(|| {
        ControlError::backend(anyhow::anyhow!(
            "invalid webhook redelivery reason: {reason}"
        ))
    })?;
    Ok(record)
}

const REDELIVERY_COLUMNS: &str = "delivery_guid, github_delivery_id, app_id, reason, attempts, \
     first_seen_at, last_attempt_at, resolved_at, last_error";

impl LiteBackend {
    /// Insert a received delivery. Dedup on `delivery_id`: an existing row
    /// is replaced only when it `failed` (a GitHub redelivery of a
    /// dead-lettered event restarts it); otherwise returns `false`.
    ///
    /// The payload is stored as JSON text; a body that is not UTF-8 JSON is
    /// `BadRequest`. `installation_id` comes from `installation.id`.
    pub(crate) async fn enqueue_webhook_delivery(
        &self,
        delivery: &WebhookDeliveryRecord,
    ) -> Result<bool, ControlError> {
        let payload = std::str::from_utf8(&delivery.payload)
            .map_err(|_| ControlError::BadRequest("webhook payload is not UTF-8".to_owned()))?;
        let value: serde_json::Value = serde_json::from_str(payload).map_err(|error| {
            ControlError::BadRequest(format!("webhook payload is not JSON: {error}"))
        })?;
        let installation_id = value["installation"]["id"].as_i64();
        self.write(|tx| {
            let changed = tx
                .execute(
                    "INSERT INTO webhook_deliveries (delivery_id, installation_id, event, \
                         payload, received_at, state, attempts, lease_until, lease_token, \
                         last_error) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) \
                     ON CONFLICT (delivery_id) DO UPDATE SET \
                         installation_id = excluded.installation_id, \
                         event = excluded.event, \
                         payload = excluded.payload, \
                         received_at = excluded.received_at, \
                         state = 'received', attempts = 0, lease_until = NULL, \
                         lease_token = NULL, last_error = NULL \
                     WHERE webhook_deliveries.state = 'failed'",
                    params![
                        delivery.delivery_id,
                        installation_id,
                        delivery.event,
                        payload,
                        delivery.received_at_us,
                        delivery.state.as_str(),
                        delivery.attempts as i64,
                        delivery.lease_until_us,
                        delivery.lease_token,
                        delivery.last_error,
                    ],
                )
                .map_err(db)?;
            Ok(changed > 0)
        })
    }

    /// Lease up to `limit` claimable deliveries, oldest first: `received`
    /// rows whose retry delay has passed, and `processing` rows whose lease
    /// expired. Each claim charges one attempt and mints a fresh lease
    /// token (`UPDATE .. RETURNING`).
    pub(crate) async fn claim_webhook_deliveries(
        &self,
        limit: usize,
        lease_duration_secs: u64,
    ) -> Result<Vec<WebhookDeliveryRecord>, ControlError> {
        self.write(|tx| {
            let now = now_us();
            let until = lease_until(now, lease_duration_secs);
            let ids: Vec<String> = {
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT delivery_id FROM webhook_deliveries \
                         WHERE (state = 'received' AND (lease_until IS NULL OR lease_until <= ?1)) \
                            OR (state = 'processing' AND lease_until IS NOT NULL \
                                AND lease_until < ?1) \
                         ORDER BY received_at ASC LIMIT ?2",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map(params![now, limit.min(i64::MAX as usize) as i64], |row| {
                        row.get(0)
                    })
                    .map_err(db)?;
                rows.collect::<Result<_, _>>().map_err(db)?
            };
            let mut stmt = tx
                .prepare_cached(
                    "UPDATE webhook_deliveries \
                     SET state = 'processing', lease_until = ?1, attempts = attempts + 1, \
                         lease_token = ?2 \
                     WHERE delivery_id = ?3 \
                     RETURNING delivery_id, event, payload, received_at, attempts, last_error",
                )
                .map_err(db)?;
            let mut claimed = Vec::with_capacity(ids.len());
            for id in ids {
                let token = uuid::Uuid::new_v4().to_string();
                let record = stmt
                    .query_row(params![until, token, &id], |row| {
                        let payload: rusqlite::types::Value = row.get(2)?;
                        let attempts: i64 = row.get(4)?;
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            payload,
                            row.get::<_, i64>(3)?,
                            attempts,
                            row.get::<_, Option<String>>(5)?,
                        ))
                    })
                    .map_err(db)?;
                // Payload must be UTF-8 JSON text; anything else is a
                // poisoned row the handler can never deliver — dead-letter
                // it instead of wedging the queue.
                let payload = match record.2 {
                    rusqlite::types::Value::Text(text) => Some(text),
                    rusqlite::types::Value::Blob(bytes) => String::from_utf8(bytes).ok(),
                    _ => None,
                }
                .filter(|text| serde_json::from_str::<serde_json::Value>(text).is_ok());
                let Some(payload) = payload else {
                    tx.prepare_cached(
                        "UPDATE webhook_deliveries SET state = 'failed', \
                         lease_until = NULL, last_error = 'undeliverable payload' \
                         WHERE delivery_id = ?1",
                    )
                    .map_err(db)?
                    .execute([&id])
                    .map_err(db)?;
                    continue;
                };
                claimed.push(WebhookDeliveryRecord {
                    delivery_id: record.0,
                    event: record.1,
                    payload: payload.into_bytes(),
                    received_at_us: record.3,
                    state: WebhookDeliveryStatus::Processing,
                    attempts: record.4.max(0) as u32,
                    lease_until_us: Some(until),
                    lease_token: Some(token),
                    last_error: record.5,
                });
            }
            Ok(claimed)
        })
    }

    /// Extend a still-owned lease without charging an attempt.
    pub(crate) async fn renew_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        lease_duration_secs: u64,
    ) -> Result<bool, ControlError> {
        self.write(|tx| {
            let now = now_us();
            let changed = tx
                .execute(
                    &format!(
                        "UPDATE webhook_deliveries SET lease_until = :until \
                         WHERE delivery_id = :id AND {OWNED_LEASE}"
                    ),
                    rusqlite::named_params! {
                        ":until": lease_until(now, lease_duration_secs),
                        ":id": delivery_id,
                        ":token": lease_token,
                        ":now": now,
                    },
                )
                .map_err(db)?;
            Ok(changed > 0)
        })
    }

    /// `processing` → `done` under the owned lease.
    pub(crate) async fn complete_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
    ) -> Result<bool, ControlError> {
        self.write(|tx| {
            let changed = tx
                .execute(
                    &format!(
                        "UPDATE webhook_deliveries SET state = 'done', lease_until = NULL, \
                             lease_token = NULL, last_error = NULL \
                         WHERE delivery_id = :id AND {OWNED_LEASE}"
                    ),
                    rusqlite::named_params! {
                        ":id": delivery_id,
                        ":token": lease_token,
                        ":now": now_us(),
                    },
                )
                .map_err(db)?;
            Ok(changed > 0)
        })
    }

    /// Record a processing failure under the owned lease: `permanent` →
    /// `failed` (dead letter); otherwise back to `received`, claimable
    /// again after `retry_delay` (immediately when `None`).
    pub(crate) async fn fail_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        error: &str,
        permanent: bool,
        retry_delay: Option<std::time::Duration>,
    ) -> Result<bool, ControlError> {
        self.write(|tx| {
            let now = now_us();
            let changed = if permanent {
                tx.execute(
                    &format!(
                        "UPDATE webhook_deliveries SET state = 'failed', lease_until = NULL, \
                             lease_token = NULL, last_error = :error \
                         WHERE delivery_id = :id AND {OWNED_LEASE}"
                    ),
                    rusqlite::named_params! {
                        ":error": error,
                        ":id": delivery_id,
                        ":token": lease_token,
                        ":now": now,
                    },
                )
            } else {
                let retry_at = retry_delay.map(|delay| crate::store::retry_deadline_us(now, delay));
                tx.execute(
                    &format!(
                        "UPDATE webhook_deliveries SET state = 'received', \
                             lease_until = :retry_at, lease_token = NULL, last_error = :error \
                         WHERE delivery_id = :id AND {OWNED_LEASE}"
                    ),
                    rusqlite::named_params! {
                        ":retry_at": retry_at,
                        ":error": error,
                        ":id": delivery_id,
                        ":token": lease_token,
                        ":now": now,
                    },
                )
            }
            .map_err(db)?;
            Ok(changed > 0)
        })
    }

    /// One delivery with its payload.
    pub(crate) async fn get_webhook_delivery(
        &self,
        delivery_id: &str,
    ) -> Result<Option<WebhookDeliveryRecord>, ControlError> {
        self.read(|tx| {
            let row = tx
                .query_row(
                    "SELECT delivery_id, event, payload, received_at, state, attempts, \
                         lease_until, lease_token, last_error \
                     FROM webhook_deliveries WHERE delivery_id = ?1",
                    [delivery_id],
                    |row| {
                        let payload: String = row.get(2)?;
                        let attempts: i64 = row.get(5)?;
                        Ok((
                            WebhookDeliveryRecord {
                                delivery_id: row.get(0)?,
                                event: row.get(1)?,
                                payload: payload.into_bytes(),
                                received_at_us: row.get(3)?,
                                state: WebhookDeliveryStatus::Received,
                                attempts: attempts.max(0) as u32,
                                lease_until_us: row.get(6)?,
                                lease_token: row.get(7)?,
                                last_error: row.get(8)?,
                            },
                            row.get::<_, String>(4)?,
                        ))
                    },
                )
                .optional()
                .map_err(db)?;
            row.map(|(mut record, state)| {
                record.state = parse_state(&state)?;
                Ok(record)
            })
            .transpose()
        })
    }

    /// Dead-letter depth (`state = 'failed'`).
    pub(crate) async fn count_dead_letter_webhook_deliveries(&self) -> Result<u64, ControlError> {
        self.read(|tx| {
            tx.query_row(
                "SELECT COUNT(*) FROM webhook_deliveries WHERE state = 'failed'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|n| n.max(0) as u64)
            .map_err(db)
        })
    }

    /// Return expired `processing` leases to `received` (boot recovery).
    pub(crate) async fn recover_webhook_deliveries(&self) -> Result<u64, ControlError> {
        self.write(|tx| {
            tx.execute(
                "UPDATE webhook_deliveries \
                 SET state = 'received', lease_until = NULL, lease_token = NULL \
                 WHERE state = 'processing' AND lease_until IS NOT NULL AND lease_until < ?1",
                [now_us()],
            )
            .map(|n| n as u64)
            .map_err(db)
        })
    }

    /// Delete up to `limit` terminal rows received before `before_us`,
    /// oldest first (the dedup retention window).
    pub(crate) async fn prune_webhook_deliveries(
        &self,
        before_us: i64,
        limit: usize,
    ) -> Result<u64, ControlError> {
        self.write(|tx| {
            tx.execute(
                "DELETE FROM webhook_deliveries WHERE delivery_id IN ( \
                     SELECT delivery_id FROM webhook_deliveries \
                     WHERE state IN ('done', 'failed') AND received_at < ?1 \
                     ORDER BY received_at ASC LIMIT ?2)",
                params![before_us, limit.min(i64::MAX as usize) as i64],
            )
            .map(|n| n as u64)
            .map_err(db)
        })
    }

    /// Park a delivery blocked on an unavailable dependency: back to
    /// `received` after `retry_delay_secs`, refunding the attempt the claim
    /// charged.
    pub(crate) async fn park_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        error: &str,
        retry_delay_secs: u64,
    ) -> Result<bool, ControlError> {
        self.write(|tx| {
            let now = now_us();
            let changed = tx
                .execute(
                    &format!(
                        "UPDATE webhook_deliveries SET state = 'received', \
                             lease_until = :retry_at, lease_token = NULL, \
                             attempts = MAX(attempts - 1, 0), last_error = :error \
                         WHERE delivery_id = :id AND {OWNED_LEASE}"
                    ),
                    rusqlite::named_params! {
                        ":retry_at": lease_until(now, retry_delay_secs),
                        ":error": error,
                        ":id": delivery_id,
                        ":token": lease_token,
                        ":now": now,
                    },
                )
                .map_err(db)?;
            Ok(changed > 0)
        })
    }

    /// Operator requeue of a terminal (`done`/`failed`) delivery with a
    /// fresh attempt budget.
    pub(crate) async fn requeue_webhook_delivery(
        &self,
        delivery_id: &str,
    ) -> Result<bool, ControlError> {
        self.write(|tx| {
            let changed = tx
                .execute(
                    "UPDATE webhook_deliveries SET state = 'received', attempts = 0, \
                         lease_until = NULL, lease_token = NULL, last_error = NULL \
                     WHERE delivery_id = ?1 AND state IN ('done', 'failed')",
                    [delivery_id],
                )
                .map_err(db)?;
            Ok(changed > 0)
        })
    }

    /// Deliveries without payloads, newest first, optionally one state.
    pub(crate) async fn list_webhook_deliveries(
        &self,
        state: Option<WebhookDeliveryStatus>,
        limit: usize,
    ) -> Result<Vec<WebhookDeliverySummary>, ControlError> {
        self.read(|tx| {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT delivery_id, event, received_at, state, attempts, lease_until, \
                         last_error \
                     FROM webhook_deliveries WHERE (?1 IS NULL OR state = ?1) \
                     ORDER BY received_at DESC LIMIT ?2",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map(
                    params![
                        state.map(|s| s.as_str()),
                        limit.min(i64::MAX as usize) as i64
                    ],
                    |row| {
                        let attempts: i64 = row.get(4)?;
                        Ok((
                            WebhookDeliverySummary {
                                delivery_id: row.get(0)?,
                                event: row.get(1)?,
                                received_at_us: row.get(2)?,
                                state: WebhookDeliveryStatus::Received,
                                attempts: attempts.max(0) as u32,
                                lease_until_us: row.get(5)?,
                                last_error: row.get(6)?,
                            },
                            row.get::<_, String>(3)?,
                        ))
                    },
                )
                .map_err(db)?;
            let mut out = Vec::new();
            for row in rows {
                let (mut summary, state) = row.map_err(db)?;
                summary.state = parse_state(&state)?;
                out.push(summary);
            }
            Ok(out)
        })
    }

    /// Which of `delivery_ids` have a local row (any state). One statement,
    /// parameterized by count — delivery ids are request headers and never
    /// reach SQL as literals.
    pub(crate) async fn webhook_deliveries_present(
        &self,
        delivery_ids: &[String],
    ) -> Result<BTreeSet<String>, ControlError> {
        if delivery_ids.is_empty() {
            return Ok(BTreeSet::new());
        }
        self.read(|tx| {
            let placeholders = vec!["?"; delivery_ids.len()].join(",");
            let mut stmt = tx
                .prepare(&format!(
                    "SELECT delivery_id FROM webhook_deliveries \
                     WHERE delivery_id IN ({placeholders})"
                ))
                .map_err(db)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(delivery_ids), |row| row.get(0))
                .map_err(db)?;
            rows.collect::<Result<_, _>>().map_err(db)
        })
    }

    /// Per-state counts plus the oldest unfinished receipt time.
    pub(crate) async fn webhook_queue_stats(&self) -> Result<WebhookQueueStats, ControlError> {
        self.read(|tx| {
            let mut stats = WebhookQueueStats::default();
            let mut stmt = tx
                .prepare_cached("SELECT state, COUNT(*) FROM webhook_deliveries GROUP BY state")
                .map_err(db)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(db)?;
            for row in rows {
                let (state, count) = row.map_err(db)?;
                let count = count.max(0) as u64;
                match parse_state(&state)? {
                    WebhookDeliveryStatus::Received => stats.received = count,
                    WebhookDeliveryStatus::Processing => stats.processing = count,
                    WebhookDeliveryStatus::Done => stats.done = count,
                    WebhookDeliveryStatus::Failed => stats.failed = count,
                }
            }
            stats.oldest_pending_received_at_us = tx
                .query_row(
                    "SELECT MIN(received_at) FROM webhook_deliveries \
                     WHERE state IN ('received', 'processing')",
                    [],
                    |row| row.get(0),
                )
                .map_err(db)?;
            Ok(stats)
        })
    }

    pub(crate) async fn load_webhook_watchdog_cursor(
        &self,
        scope: &str,
    ) -> Result<Option<WebhookWatchdogCursor>, ControlError> {
        self.read(|tx| {
            tx.query_row(
                "SELECT scope, cursor_delivered_at, cursor_delivery_guid, scan_cursor, \
                     last_poll_at, last_success_at \
                 FROM webhook_watchdog WHERE scope = ?1",
                [scope],
                |row| {
                    Ok(WebhookWatchdogCursor {
                        scope: row.get(0)?,
                        cursor_delivered_at_us: row.get(1)?,
                        cursor_delivered_at_guid: row.get(2)?,
                        scan_cursor: row.get(3)?,
                        last_poll_at_us: row.get(4)?,
                        last_success_at_us: row.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(db)
        })
    }

    /// Upsert the watchdog cursor. The `(delivered_at, guid)` watermark and
    /// the poll clocks only move forward; `scan_cursor` is replaced.
    pub(crate) async fn store_webhook_watchdog_cursor(
        &self,
        cursor: &WebhookWatchdogCursor,
    ) -> Result<(), ControlError> {
        self.write(|tx| {
            tx.execute(
                "INSERT INTO webhook_watchdog (scope, cursor_delivered_at, \
                     cursor_delivery_guid, scan_cursor, last_poll_at, last_success_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                 ON CONFLICT (scope) DO UPDATE SET \
                     cursor_delivered_at = CASE \
                         WHEN webhook_watchdog.cursor_delivered_at IS NULL \
                             THEN excluded.cursor_delivered_at \
                         WHEN excluded.cursor_delivered_at IS NULL \
                             THEN webhook_watchdog.cursor_delivered_at \
                         ELSE MAX(webhook_watchdog.cursor_delivered_at, \
                                  excluded.cursor_delivered_at) \
                     END, \
                     cursor_delivery_guid = CASE \
                         WHEN excluded.cursor_delivered_at IS NULL \
                             THEN webhook_watchdog.cursor_delivery_guid \
                         WHEN webhook_watchdog.cursor_delivered_at IS NULL \
                              OR excluded.cursor_delivered_at \
                                 > webhook_watchdog.cursor_delivered_at \
                             THEN excluded.cursor_delivery_guid \
                         WHEN excluded.cursor_delivered_at \
                              < webhook_watchdog.cursor_delivered_at \
                             THEN webhook_watchdog.cursor_delivery_guid \
                         WHEN excluded.cursor_delivery_guid IS NULL \
                             THEN webhook_watchdog.cursor_delivery_guid \
                         WHEN webhook_watchdog.cursor_delivery_guid IS NULL \
                              OR excluded.cursor_delivery_guid \
                                 > webhook_watchdog.cursor_delivery_guid \
                             THEN excluded.cursor_delivery_guid \
                         ELSE webhook_watchdog.cursor_delivery_guid \
                     END, \
                     scan_cursor = excluded.scan_cursor, \
                     last_poll_at = CASE \
                         WHEN webhook_watchdog.last_poll_at IS NULL THEN excluded.last_poll_at \
                         WHEN excluded.last_poll_at IS NULL THEN webhook_watchdog.last_poll_at \
                         ELSE MAX(webhook_watchdog.last_poll_at, excluded.last_poll_at) \
                     END, \
                     last_success_at = CASE \
                         WHEN webhook_watchdog.last_success_at IS NULL \
                             THEN excluded.last_success_at \
                         WHEN excluded.last_success_at IS NULL \
                             THEN webhook_watchdog.last_success_at \
                         ELSE MAX(webhook_watchdog.last_success_at, excluded.last_success_at) \
                     END",
                params![
                    cursor.scope,
                    cursor.cursor_delivered_at_us,
                    cursor.cursor_delivered_at_guid,
                    cursor.scan_cursor,
                    cursor.last_poll_at_us,
                    cursor.last_success_at_us,
                ],
            )
            .map_err(db)?;
            Ok(())
        })
    }

    /// Upsert a repair row. `attempts` and `last_attempt_at` never move
    /// backwards; `resolved_at` is sticky once set.
    pub(crate) async fn upsert_webhook_redelivery(
        &self,
        record: &WebhookRedeliveryRecord,
    ) -> Result<(), ControlError> {
        self.write(|tx| {
            tx.execute(
                "INSERT INTO webhook_redeliveries (delivery_guid, github_delivery_id, app_id, \
                     reason, attempts, first_seen_at, last_attempt_at, resolved_at, last_error) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) \
                 ON CONFLICT (delivery_guid) DO UPDATE SET \
                     github_delivery_id = excluded.github_delivery_id, \
                     app_id = excluded.app_id, \
                     reason = excluded.reason, \
                     attempts = MAX(webhook_redeliveries.attempts, excluded.attempts), \
                     last_attempt_at = CASE \
                         WHEN webhook_redeliveries.last_attempt_at IS NULL \
                             THEN excluded.last_attempt_at \
                         WHEN excluded.last_attempt_at IS NULL \
                             THEN webhook_redeliveries.last_attempt_at \
                         ELSE MAX(webhook_redeliveries.last_attempt_at, excluded.last_attempt_at) \
                     END, \
                     resolved_at = COALESCE(webhook_redeliveries.resolved_at, \
                                            excluded.resolved_at), \
                     last_error = excluded.last_error",
                params![
                    record.delivery_guid,
                    record.github_delivery_id,
                    record.app_id,
                    record.reason.as_str(),
                    record.attempts as i64,
                    record.first_seen_at_us,
                    record.last_attempt_at_us,
                    record.resolved_at_us,
                    record.last_error,
                ],
            )
            .map_err(db)?;
            Ok(())
        })
    }

    pub(crate) async fn load_webhook_redelivery(
        &self,
        delivery_guid: &str,
    ) -> Result<Option<WebhookRedeliveryRecord>, ControlError> {
        self.read(|tx| {
            tx.query_row(
                &format!(
                    "SELECT {REDELIVERY_COLUMNS} FROM webhook_redeliveries \
                     WHERE delivery_guid = ?1"
                ),
                [delivery_guid],
                redelivery_row,
            )
            .optional()
            .map_err(db)?
            .map(finish_redelivery)
            .transpose()
        })
    }

    /// Unresolved repairs, oldest first.
    pub(crate) async fn open_webhook_redeliveries(
        &self,
        limit: usize,
    ) -> Result<Vec<WebhookRedeliveryRecord>, ControlError> {
        self.read(|tx| {
            let mut stmt = tx
                .prepare_cached(&format!(
                    "SELECT {REDELIVERY_COLUMNS} FROM webhook_redeliveries \
                     WHERE resolved_at IS NULL ORDER BY first_seen_at ASC LIMIT ?1"
                ))
                .map_err(db)?;
            let rows = stmt
                .query_map([limit.min(i64::MAX as usize) as i64], redelivery_row)
                .map_err(db)?;
            rows.map(|row| row.map_err(db).and_then(finish_redelivery))
                .collect()
        })
    }

    /// Unresolved repairs for one App that are still below the attempt cap,
    /// in retry order: the row that has waited longest since its last
    /// attempt first (never-attempted rows at the front), then oldest-first.
    ///
    /// Scoping and the cap live in SQL on purpose: the retry pass reads a
    /// bounded window, and a global query would let another App's backlog —
    /// or rows already at the cap, which are never retried — fill it and
    /// starve this App's repairs.
    pub(crate) async fn retryable_webhook_redeliveries(
        &self,
        app_id: &str,
        attempt_cap: u32,
        limit: usize,
    ) -> Result<Vec<WebhookRedeliveryRecord>, ControlError> {
        self.read(|tx| {
            let mut stmt = tx
                .prepare_cached(&format!(
                    "SELECT {REDELIVERY_COLUMNS} FROM webhook_redeliveries \
                     WHERE resolved_at IS NULL AND app_id = ?1 AND attempts < ?2 \
                     ORDER BY last_attempt_at ASC NULLS FIRST, first_seen_at ASC LIMIT ?3"
                ))
                .map_err(db)?;
            let rows = stmt
                .query_map(
                    params![
                        app_id,
                        attempt_cap.min(i32::MAX as u32) as i64,
                        limit.min(i64::MAX as usize) as i64
                    ],
                    redelivery_row,
                )
                .map_err(db)?;
            rows.map(|row| row.map_err(db).and_then(finish_redelivery))
                .collect()
        })
    }

    /// Take one redelivery attempt for a repair row, or report that someone
    /// else already did.
    ///
    /// The `WHERE` on the conflict path is the compare of a compare-and-swap
    /// over the row the caller read: an attempt committed since that read
    /// (another watchdog, a restart overlap, a second server on the store)
    /// no longer matches, so the update is skipped, nothing is returned, and
    /// the caller sends no request. The charge and the last-attempt clock
    /// move in the same statement, so a claimed attempt is exactly one
    /// request. `IS` is null-safe equality, so an open row (`resolved_at`
    /// NULL) matches a NULL `resolved_at` and a closed row only reopens
    /// under the resolution the caller read.
    pub(crate) async fn claim_webhook_redelivery(
        &self,
        observed: &WebhookRedeliveryRecord,
        claimed_at_us: i64,
    ) -> Result<Option<WebhookRedeliveryRecord>, ControlError> {
        self.write(|tx| {
            tx.query_row(
                &format!(
                    "INSERT INTO webhook_redeliveries (delivery_guid, github_delivery_id, app_id, \
                     reason, attempts, first_seen_at, last_attempt_at, resolved_at, last_error) \
                     VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6, NULL, NULL) \
                     ON CONFLICT (delivery_guid) DO UPDATE SET \
                     github_delivery_id = excluded.github_delivery_id, \
                     app_id = excluded.app_id, reason = excluded.reason, \
                     attempts = webhook_redeliveries.attempts + 1, \
                     last_attempt_at = excluded.last_attempt_at, \
                     resolved_at = NULL, last_error = NULL \
                     WHERE webhook_redeliveries.attempts = ?7 \
                     AND webhook_redeliveries.resolved_at IS ?8 \
                     RETURNING {REDELIVERY_COLUMNS}"
                ),
                params![
                    observed.delivery_guid,
                    observed.github_delivery_id,
                    observed.app_id,
                    observed.reason.as_str(),
                    observed.first_seen_at_us,
                    claimed_at_us,
                    observed.attempts as i64,
                    observed.resolved_at_us,
                ],
                redelivery_row,
            )
            .optional()
            .map_err(db)?
            .map(finish_redelivery)
            .transpose()
        })
    }

    /// Close an open repair; `false` when unknown or already resolved.
    pub(crate) async fn resolve_webhook_redelivery(
        &self,
        delivery_guid: &str,
        resolved_at_us: i64,
    ) -> Result<bool, ControlError> {
        self.write(|tx| {
            let changed = tx
                .execute(
                    "UPDATE webhook_redeliveries SET resolved_at = ?2 \
                     WHERE delivery_guid = ?1 AND resolved_at IS NULL",
                    params![delivery_guid, resolved_at_us],
                )
                .map_err(db)?;
            Ok(changed > 0)
        })
    }
}
