//! Webhook inbox (`webhook_deliveries`), the delivery-history watchdog
//! cursor (`webhook_watchdog`) and redelivery repair rows
//! (`webhook_redeliveries`).
//!
//! The inbox is a lease queue: claim is `FOR UPDATE SKIP LOCKED`, every
//! later transition is a conditional `UPDATE` fenced on
//! `(state = 'processing', lease_token, lease_until > now)`, so a worker
//! whose lease expired can neither renew nor finalize a delivery another
//! worker reclaimed.
//!
//! The payload is stored as `jsonb` (signature verification already ran on
//! the raw bytes at ingress); a body that is not UTF-8 JSON is refused at
//! enqueue. Reads return the canonical JSON bytes.

use super::codec::{self, now_us, ts, us};
use super::{db, PgBackend};
use crate::control::types::ControlError;
use crate::models::{
    WebhookDeliveryRecord, WebhookDeliveryStatus, WebhookDeliverySummary, WebhookQueueStats,
    WebhookRedeliveryRecord, WebhookRepairReason, WebhookWatchdogCursor,
};
use std::collections::BTreeSet;

/// Columns selected by every full delivery read, in [`delivery_from_row`]
/// order.
const DELIVERY_COLUMNS: &str = concat!(
    "delivery_id, event, payload::text, ",
    us!("received_at"),
    ", state, attempts, ",
    us!("lease_until"),
    ", lease_token::text, last_error"
);

fn delivery_from_row(row: &tokio_postgres::Row) -> Result<WebhookDeliveryRecord, ControlError> {
    let payload: String = row.get(2);
    Ok(WebhookDeliveryRecord {
        delivery_id: row.get(0),
        event: row.get(1),
        payload: payload.into_bytes(),
        received_at_us: row.get(3),
        state: parse_state(row.get(4))?,
        attempts: row.get::<_, i32>(5).max(0) as u32,
        lease_until_us: row.get(6),
        lease_token: row.get(7),
        last_error: row.get(8),
    })
}

fn parse_state(state: &str) -> Result<WebhookDeliveryStatus, ControlError> {
    WebhookDeliveryStatus::parse(state).ok_or_else(|| {
        ControlError::backend(anyhow::anyhow!("invalid webhook delivery state: {state}"))
    })
}

/// A lease token presented by a worker. Tokens are minted as uuids at claim,
/// so anything else can never match a row.
fn lease_token(token: &str) -> Option<String> {
    token.parse::<uuid::Uuid>().ok().map(|t| t.to_string())
}

const REDELIVERY_COLUMNS: &str = concat!(
    "delivery_guid, github_delivery_id, app_id, reason, attempts, ",
    us!("first_seen_at"),
    ", ",
    us!("last_attempt_at"),
    ", ",
    us!("resolved_at"),
    ", last_error"
);

fn redelivery_from_row(row: &tokio_postgres::Row) -> Result<WebhookRedeliveryRecord, ControlError> {
    let reason: String = row.get(3);
    Ok(WebhookRedeliveryRecord {
        delivery_guid: row.get(0),
        github_delivery_id: row.get(1),
        app_id: row.get(2),
        reason: WebhookRepairReason::parse(&reason).ok_or_else(|| {
            ControlError::backend(anyhow::anyhow!(
                "invalid webhook redelivery reason: {reason}"
            ))
        })?,
        attempts: row.get::<_, i32>(4).max(0) as u32,
        first_seen_at_us: row.get(5),
        last_attempt_at_us: row.get(6),
        resolved_at_us: row.get(7),
        last_error: row.get(8),
    })
}

impl PgBackend {
    /// Durably enqueue a delivery. A new id inserts; a known id is reset to
    /// `received` only when its previous processing failed permanently
    /// (redelivery of a dead letter). Returns whether a row was written.
    ///
    /// Statement: `INSERT INTO webhook_deliveries .. ON CONFLICT
    /// (delivery_id) DO UPDATE .. WHERE webhook_deliveries.state =
    /// 'failed'`. `installation_id` is `payload.installation.id`.
    pub(super) async fn enqueue_webhook_delivery(
        &self,
        delivery: &WebhookDeliveryRecord,
    ) -> Result<bool, ControlError> {
        let payload: serde_json::Value = std::str::from_utf8(&delivery.payload)
            .ok()
            .and_then(|text| serde_json::from_str(text).ok())
            .ok_or_else(|| {
                ControlError::BadRequest("webhook payload is not UTF-8 JSON".to_owned())
            })?;
        let installation_id = payload["installation"]["id"].as_i64();
        let payload = codec::json(&payload)?;
        let token = delivery.lease_token.as_deref().and_then(lease_token);
        let client = self.writer().await?;
        let written = client
            .execute(
                concat!(
                    "INSERT INTO webhook_deliveries (delivery_id, installation_id, event, \
                     payload, received_at, state, attempts, lease_until, lease_token, \
                     last_error) VALUES ($1, $2, $3, $4::text::jsonb, ",
                    ts!("$5"),
                    ", $6, $7, ",
                    ts!("$8"),
                    ", $9::text::uuid, $10) \
                     ON CONFLICT (delivery_id) DO UPDATE SET \
                     installation_id = EXCLUDED.installation_id, event = EXCLUDED.event, \
                     payload = EXCLUDED.payload, received_at = EXCLUDED.received_at, \
                     state = 'received', attempts = 0, lease_until = NULL, \
                     lease_token = NULL, last_error = NULL \
                     WHERE webhook_deliveries.state = 'failed'"
                ),
                &[
                    &delivery.delivery_id,
                    &installation_id,
                    &delivery.event,
                    &payload,
                    &delivery.received_at_us,
                    &delivery.state.as_str(),
                    &(delivery.attempts.min(i32::MAX as u32) as i32),
                    &delivery.lease_until_us,
                    &token,
                    &delivery.last_error,
                ],
            )
            .await
            .map_err(db)?;
        Ok(written > 0)
    }

    /// Lease up to `limit` claimable deliveries, oldest first: `received`
    /// rows whose retry delay elapsed, and `processing` rows whose lease
    /// expired (a crashed worker). Each claimed row gets a fresh uuid lease
    /// token and `attempts + 1`.
    ///
    /// Statement: one `UPDATE .. FROM (SELECT .. ORDER BY received_at LIMIT
    /// $n FOR UPDATE SKIP LOCKED) RETURNING ..`.
    pub(super) async fn claim_webhook_deliveries(
        &self,
        limit: usize,
        lease_duration_secs: u64,
    ) -> Result<Vec<WebhookDeliveryRecord>, ControlError> {
        let now = now_us();
        let lease_until = now.saturating_add((lease_duration_secs as i64).saturating_mul(1_000_000));
        let client = self.writer().await?;
        let rows = client
            .query(
                concat!(
                    "UPDATE webhook_deliveries d SET state = 'processing', \
                     lease_until = ",
                    ts!("$2"),
                    ", lease_token = gen_random_uuid(), attempts = d.attempts + 1 \
                     FROM (SELECT delivery_id FROM webhook_deliveries \
                           WHERE (state = 'received' AND (lease_until IS NULL OR lease_until <= ",
                    ts!("$1"),
                    ")) OR (state = 'processing' AND lease_until < ",
                    ts!("$1"),
                    ") ORDER BY received_at LIMIT $3 FOR UPDATE SKIP LOCKED) c \
                     WHERE d.delivery_id = c.delivery_id \
                     RETURNING d.delivery_id, d.event, d.payload::text, ",
                    us!("d.received_at"),
                    ", d.state, d.attempts, ",
                    us!("d.lease_until"),
                    ", d.lease_token::text, d.last_error"
                ),
                &[&now, &lease_until, &codec::limit(limit)],
            )
            .await
            .map_err(db)?;
        let mut records = rows
            .iter()
            .map(delivery_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        records.sort_by(|a, b| {
            (a.received_at_us, &a.delivery_id).cmp(&(b.received_at_us, &b.delivery_id))
        });
        Ok(records)
    }

    /// Extend a live lease held under `lease_token`.
    ///
    /// Statement: `UPDATE webhook_deliveries SET lease_until = $n WHERE
    /// delivery_id AND state = 'processing' AND lease_token AND lease_until
    /// > now`.
    pub(super) async fn renew_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        lease_duration_secs: u64,
    ) -> Result<bool, ControlError> {
        let Some(token) = self::lease_token(lease_token) else {
            return Ok(false);
        };
        let now = now_us();
        let lease_until = now.saturating_add((lease_duration_secs as i64).saturating_mul(1_000_000));
        let client = self.writer().await?;
        let renewed = client
            .execute(
                concat!(
                    "UPDATE webhook_deliveries SET lease_until = ",
                    ts!("$1"),
                    " WHERE delivery_id = $2 AND state = 'processing' \
                     AND lease_token = $3::text::uuid AND lease_until > ",
                    ts!("$4")
                ),
                &[&lease_until, &delivery_id, &token, &now],
            )
            .await
            .map_err(db)?;
        Ok(renewed > 0)
    }

    /// Finalize a delivery as `done` under a live lease.
    ///
    /// Statement: `UPDATE webhook_deliveries SET state = 'done', lease
    /// cleared, last_error = NULL WHERE <live lease fence>`.
    pub(super) async fn complete_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
    ) -> Result<bool, ControlError> {
        let Some(token) = self::lease_token(lease_token) else {
            return Ok(false);
        };
        let client = self.writer().await?;
        let done = client
            .execute(
                concat!(
                    "UPDATE webhook_deliveries SET state = 'done', lease_until = NULL, \
                     lease_token = NULL, last_error = NULL \
                     WHERE delivery_id = $1 AND state = 'processing' \
                     AND lease_token = $2::text::uuid AND lease_until > ",
                    ts!("$3")
                ),
                &[&delivery_id, &token, &now_us()],
            )
            .await
            .map_err(db)?;
        Ok(done > 0)
    }

    /// Fail a delivery under a live lease: `permanent` dead-letters it
    /// (`failed`); otherwise it returns to `received`, claimable again after
    /// `retry_delay` (immediately when `None`).
    ///
    /// Statement: `UPDATE webhook_deliveries SET state = 'failed' | 'received'
    /// .. WHERE <live lease fence>`.
    pub(super) async fn fail_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        error: &str,
        permanent: bool,
        retry_delay: Option<std::time::Duration>,
    ) -> Result<bool, ControlError> {
        let Some(token) = self::lease_token(lease_token) else {
            return Ok(false);
        };
        let now = now_us();
        let (state, retry_at) = if permanent {
            ("failed", None)
        } else {
            (
                "received",
                retry_delay.map(|delay| crate::store::retry_deadline_us(now, delay)),
            )
        };
        let client = self.writer().await?;
        let failed = client
            .execute(
                concat!(
                    "UPDATE webhook_deliveries SET state = $1, lease_until = ",
                    ts!("$2"),
                    ", lease_token = NULL, last_error = $3 \
                     WHERE delivery_id = $4 AND state = 'processing' \
                     AND lease_token = $5::text::uuid AND lease_until > ",
                    ts!("$6")
                ),
                &[&state, &retry_at, &error, &delivery_id, &token, &now],
            )
            .await
            .map_err(db)?;
        Ok(failed > 0)
    }

    /// One delivery with its payload.
    ///
    /// Statement: `SELECT <delivery columns> FROM webhook_deliveries WHERE
    /// delivery_id = $1`.
    pub(super) async fn get_webhook_delivery(
        &self,
        delivery_id: &str,
    ) -> Result<Option<WebhookDeliveryRecord>, ControlError> {
        let client = self.reader().await?;
        let sql = format!("SELECT {DELIVERY_COLUMNS} FROM webhook_deliveries WHERE delivery_id = $1");
        client
            .query_opt(&sql, &[&delivery_id])
            .await
            .map_err(db)?
            .as_ref()
            .map(delivery_from_row)
            .transpose()
    }

    /// Dead-lettered (`failed`) deliveries.
    ///
    /// Statement: `SELECT count(*) FROM webhook_deliveries WHERE state =
    /// 'failed'`.
    pub(super) async fn count_dead_letter_webhook_deliveries(&self) -> Result<u64, ControlError> {
        let client = self.reader().await?;
        let count: i64 = client
            .query_one(
                "SELECT count(*) FROM webhook_deliveries WHERE state = 'failed'",
                &[],
            )
            .await
            .map_err(db)?
            .get(0);
        Ok(count.max(0) as u64)
    }

    /// Return every expired `processing` lease to `received` (boot
    /// recovery). Returns rows recovered.
    ///
    /// Statement: `UPDATE webhook_deliveries SET state = 'received', lease
    /// cleared WHERE state = 'processing' AND lease_until < now`.
    pub(super) async fn recover_webhook_deliveries(&self) -> Result<u64, ControlError> {
        let client = self.writer().await?;
        client
            .execute(
                concat!(
                    "UPDATE webhook_deliveries SET state = 'received', lease_until = NULL, \
                     lease_token = NULL WHERE state = 'processing' AND lease_until < ",
                    ts!("$1")
                ),
                &[&now_us()],
            )
            .await
            .map_err(db)
    }

    /// Delete up to `limit` terminal (`done`/`failed`) deliveries received
    /// before `before_us`, oldest first. Returns rows deleted.
    ///
    /// Statement: `DELETE FROM webhook_deliveries WHERE delivery_id IN
    /// (SELECT .. WHERE state IN ('done','failed') AND received_at < $1
    /// ORDER BY received_at LIMIT $2)` (served by `webhook_deliveries_prune`).
    pub(super) async fn prune_webhook_deliveries(
        &self,
        before_us: i64,
        limit: usize,
    ) -> Result<u64, ControlError> {
        let client = self.writer().await?;
        client
            .execute(
                concat!(
                    "DELETE FROM webhook_deliveries WHERE delivery_id IN (\
                     SELECT delivery_id FROM webhook_deliveries \
                     WHERE state IN ('done', 'failed') AND received_at < ",
                    ts!("$1"),
                    " ORDER BY received_at LIMIT $2)"
                ),
                &[&before_us, &codec::limit(limit)],
            )
            .await
            .map_err(db)
    }

    /// Put a leased delivery back without charging the attempt: `received`,
    /// claimable after `retry_delay_secs`, `attempts - 1` (never below 0).
    ///
    /// Statement: `UPDATE webhook_deliveries SET state = 'received',
    /// lease_until = now + delay, attempts = GREATEST(attempts - 1, 0) ..
    /// WHERE <live lease fence>`.
    pub(super) async fn park_webhook_delivery(
        &self,
        delivery_id: &str,
        lease_token: &str,
        error: &str,
        retry_delay_secs: u64,
    ) -> Result<bool, ControlError> {
        let Some(token) = self::lease_token(lease_token) else {
            return Ok(false);
        };
        let now = now_us();
        let retry_at = now.saturating_add((retry_delay_secs as i64).saturating_mul(1_000_000));
        let client = self.writer().await?;
        let parked = client
            .execute(
                concat!(
                    "UPDATE webhook_deliveries SET state = 'received', lease_until = ",
                    ts!("$1"),
                    ", lease_token = NULL, attempts = GREATEST(attempts - 1, 0), \
                     last_error = $2 WHERE delivery_id = $3 AND state = 'processing' \
                     AND lease_token = $4::text::uuid AND lease_until > ",
                    ts!("$5")
                ),
                &[&retry_at, &error, &delivery_id, &token, &now],
            )
            .await
            .map_err(db)?;
        Ok(parked > 0)
    }

    /// Operator requeue of a terminal delivery: back to `received` with a
    /// fresh attempt budget.
    ///
    /// Statement: `UPDATE webhook_deliveries SET state = 'received',
    /// attempts = 0, lease/error cleared WHERE delivery_id = $1 AND state IN
    /// ('done','failed')`.
    pub(super) async fn requeue_webhook_delivery(
        &self,
        delivery_id: &str,
    ) -> Result<bool, ControlError> {
        let client = self.writer().await?;
        let requeued = client
            .execute(
                "UPDATE webhook_deliveries SET state = 'received', attempts = 0, \
                 lease_until = NULL, lease_token = NULL, last_error = NULL \
                 WHERE delivery_id = $1 AND state IN ('done', 'failed')",
                &[&delivery_id],
            )
            .await
            .map_err(db)?;
        Ok(requeued > 0)
    }

    /// Newest-first delivery summaries (no payload), optionally one state.
    ///
    /// Statement: `SELECT .. FROM webhook_deliveries WHERE ($1 IS NULL OR
    /// state = $1) ORDER BY received_at DESC LIMIT $2`.
    pub(super) async fn list_webhook_deliveries(
        &self,
        state: Option<WebhookDeliveryStatus>,
        limit: usize,
    ) -> Result<Vec<WebhookDeliverySummary>, ControlError> {
        let state = state.map(|state| state.as_str());
        let client = self.reader().await?;
        let rows = client
            .query(
                concat!(
                    "SELECT delivery_id, event, ",
                    us!("received_at"),
                    ", state, attempts, ",
                    us!("lease_until"),
                    ", last_error FROM webhook_deliveries \
                     WHERE ($1::text IS NULL OR state = $1::text) \
                     ORDER BY received_at DESC, delivery_id DESC LIMIT $2"
                ),
                &[&state, &codec::limit(limit)],
            )
            .await
            .map_err(db)?;
        rows.iter()
            .map(|row| {
                Ok(WebhookDeliverySummary {
                    delivery_id: row.get(0),
                    event: row.get(1),
                    received_at_us: row.get(2),
                    state: parse_state(row.get(3))?,
                    attempts: row.get::<_, i32>(4).max(0) as u32,
                    lease_until_us: row.get(5),
                    last_error: row.get(6),
                })
            })
            .collect()
    }

    /// Which of `delivery_ids` exist locally (watchdog phantom-ack check).
    ///
    /// Statement: `SELECT delivery_id FROM webhook_deliveries WHERE
    /// delivery_id = ANY($1)`.
    pub(super) async fn webhook_deliveries_present(
        &self,
        delivery_ids: &[String],
    ) -> Result<BTreeSet<String>, ControlError> {
        if delivery_ids.is_empty() {
            return Ok(BTreeSet::new());
        }
        let client = self.reader().await?;
        let rows = client
            .query(
                "SELECT delivery_id FROM webhook_deliveries WHERE delivery_id = ANY($1)",
                &[&delivery_ids],
            )
            .await
            .map_err(db)?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    /// Per-state counts plus the oldest non-terminal receipt time.
    ///
    /// Statements: `SELECT state, count(*) .. GROUP BY state`; `SELECT
    /// min(received_at) .. WHERE state IN ('received','processing')`.
    pub(super) async fn webhook_queue_stats(&self) -> Result<WebhookQueueStats, ControlError> {
        let client = self.reader().await?;
        let mut stats = WebhookQueueStats::default();
        for row in client
            .query(
                "SELECT state, count(*) FROM webhook_deliveries GROUP BY state",
                &[],
            )
            .await
            .map_err(db)?
        {
            let count = row.get::<_, i64>(1).max(0) as u64;
            match parse_state(row.get(0))? {
                WebhookDeliveryStatus::Received => stats.received = count,
                WebhookDeliveryStatus::Processing => stats.processing = count,
                WebhookDeliveryStatus::Done => stats.done = count,
                WebhookDeliveryStatus::Failed => stats.failed = count,
            }
        }
        stats.oldest_pending_received_at_us = client
            .query_one(
                concat!(
                    "SELECT ",
                    us!("min(received_at)"),
                    " FROM webhook_deliveries WHERE state IN ('received', 'processing')"
                ),
                &[],
            )
            .await
            .map_err(db)?
            .get(0);
        Ok(stats)
    }

    /// The watchdog cursor for one App scope.
    ///
    /// Statement: `SELECT .. FROM webhook_watchdog WHERE scope = $1`.
    pub(super) async fn load_webhook_watchdog_cursor(
        &self,
        scope: &str,
    ) -> Result<Option<WebhookWatchdogCursor>, ControlError> {
        let client = self.reader().await?;
        let row = client
            .query_opt(
                concat!(
                    "SELECT scope, ",
                    us!("cursor_delivered_at"),
                    ", cursor_delivery_guid, scan_cursor, ",
                    us!("last_poll_at"),
                    ", ",
                    us!("last_success_at"),
                    " FROM webhook_watchdog WHERE scope = $1"
                ),
                &[&scope],
            )
            .await
            .map_err(db)?;
        Ok(row.map(|row| WebhookWatchdogCursor {
            scope: row.get(0),
            cursor_delivered_at_us: row.get(1),
            cursor_delivered_at_guid: row.get(2),
            scan_cursor: row.get(3),
            last_poll_at_us: row.get(4),
            last_success_at_us: row.get(5),
        }))
    }

    /// Upsert the watchdog cursor, never moving the high-water mark or the
    /// poll/success clocks backwards: `(cursor_delivered_at, guid)` only
    /// advances lexicographically; `scan_cursor` is replaced.
    ///
    /// Statement: `INSERT INTO webhook_watchdog .. ON CONFLICT (scope) DO
    /// UPDATE SET <monotonic GREATEST/CASE merge>`.
    pub(super) async fn store_webhook_watchdog_cursor(
        &self,
        cursor: &WebhookWatchdogCursor,
    ) -> Result<(), ControlError> {
        let client = self.writer().await?;
        client
            .execute(
                concat!(
                    "INSERT INTO webhook_watchdog AS w (scope, cursor_delivered_at, \
                     cursor_delivery_guid, scan_cursor, last_poll_at, last_success_at) \
                     VALUES ($1, ",
                    ts!("$2"),
                    ", $3, $4, ",
                    ts!("$5"),
                    ", ",
                    ts!("$6"),
                    ") ON CONFLICT (scope) DO UPDATE SET \
                     cursor_delivered_at = GREATEST(w.cursor_delivered_at, \
                         EXCLUDED.cursor_delivered_at), \
                     cursor_delivery_guid = CASE \
                         WHEN EXCLUDED.cursor_delivered_at IS NULL THEN w.cursor_delivery_guid \
                         WHEN w.cursor_delivered_at IS NULL \
                              OR EXCLUDED.cursor_delivered_at > w.cursor_delivered_at \
                             THEN EXCLUDED.cursor_delivery_guid \
                         WHEN EXCLUDED.cursor_delivered_at < w.cursor_delivered_at \
                             THEN w.cursor_delivery_guid \
                         WHEN EXCLUDED.cursor_delivery_guid IS NULL THEN w.cursor_delivery_guid \
                         WHEN w.cursor_delivery_guid IS NULL \
                              OR EXCLUDED.cursor_delivery_guid > w.cursor_delivery_guid \
                             THEN EXCLUDED.cursor_delivery_guid \
                         ELSE w.cursor_delivery_guid END, \
                     scan_cursor = EXCLUDED.scan_cursor, \
                     last_poll_at = GREATEST(w.last_poll_at, EXCLUDED.last_poll_at), \
                     last_success_at = GREATEST(w.last_success_at, EXCLUDED.last_success_at)"
                ),
                &[
                    &cursor.scope,
                    &cursor.cursor_delivered_at_us,
                    &cursor.cursor_delivered_at_guid,
                    &cursor.scan_cursor,
                    &cursor.last_poll_at_us,
                    &cursor.last_success_at_us,
                ],
            )
            .await
            .map_err(db)?;
        Ok(())
    }

    /// Upsert a repair row. Attempts and the last-attempt clock only grow;
    /// a resolved row stays resolved at its first resolution time.
    ///
    /// Statement: `INSERT INTO webhook_redeliveries .. ON CONFLICT
    /// (delivery_guid) DO UPDATE SET <monotonic merge>`.
    pub(super) async fn upsert_webhook_redelivery(
        &self,
        record: &WebhookRedeliveryRecord,
    ) -> Result<(), ControlError> {
        let client = self.writer().await?;
        client
            .execute(
                concat!(
                    "INSERT INTO webhook_redeliveries AS r (delivery_guid, github_delivery_id, \
                     app_id, reason, attempts, first_seen_at, last_attempt_at, resolved_at, \
                     last_error) VALUES ($1, $2, $3, $4, $5, ",
                    ts!("$6"),
                    ", ",
                    ts!("$7"),
                    ", ",
                    ts!("$8"),
                    ", $9) ON CONFLICT (delivery_guid) DO UPDATE SET \
                     github_delivery_id = EXCLUDED.github_delivery_id, \
                     app_id = EXCLUDED.app_id, reason = EXCLUDED.reason, \
                     attempts = GREATEST(r.attempts, EXCLUDED.attempts), \
                     last_attempt_at = GREATEST(r.last_attempt_at, EXCLUDED.last_attempt_at), \
                     resolved_at = COALESCE(r.resolved_at, EXCLUDED.resolved_at), \
                     last_error = EXCLUDED.last_error"
                ),
                &[
                    &record.delivery_guid,
                    &record.github_delivery_id,
                    &record.app_id,
                    &record.reason.as_str(),
                    &(record.attempts.min(i32::MAX as u32) as i32),
                    &record.first_seen_at_us,
                    &record.last_attempt_at_us,
                    &record.resolved_at_us,
                    &record.last_error,
                ],
            )
            .await
            .map_err(db)?;
        Ok(())
    }

    /// One repair row.
    ///
    /// Statement: `SELECT .. FROM webhook_redeliveries WHERE delivery_guid =
    /// $1`.
    pub(super) async fn load_webhook_redelivery(
        &self,
        delivery_guid: &str,
    ) -> Result<Option<WebhookRedeliveryRecord>, ControlError> {
        let client = self.reader().await?;
        let sql = format!(
            "SELECT {REDELIVERY_COLUMNS} FROM webhook_redeliveries WHERE delivery_guid = $1"
        );
        client
            .query_opt(&sql, &[&delivery_guid])
            .await
            .map_err(db)?
            .as_ref()
            .map(redelivery_from_row)
            .transpose()
    }

    /// Unresolved repair rows, oldest first.
    ///
    /// Statement: `SELECT .. FROM webhook_redeliveries WHERE resolved_at IS
    /// NULL ORDER BY first_seen_at LIMIT $1` (served by
    /// `webhook_redeliveries_open`).
    pub(super) async fn open_webhook_redeliveries(
        &self,
        limit: usize,
    ) -> Result<Vec<WebhookRedeliveryRecord>, ControlError> {
        let client = self.reader().await?;
        let sql = format!(
            "SELECT {REDELIVERY_COLUMNS} FROM webhook_redeliveries \
             WHERE resolved_at IS NULL ORDER BY first_seen_at, delivery_guid LIMIT $1"
        );
        client
            .query(&sql, &[&codec::limit(limit)])
            .await
            .map_err(db)?
            .iter()
            .map(redelivery_from_row)
            .collect()
    }

    /// Mark a repair row resolved (first resolution wins).
    ///
    /// Statement: `UPDATE webhook_redeliveries SET resolved_at = $2 WHERE
    /// delivery_guid = $1 AND resolved_at IS NULL`.
    pub(super) async fn resolve_webhook_redelivery(
        &self,
        delivery_guid: &str,
        resolved_at_us: i64,
    ) -> Result<bool, ControlError> {
        let client = self.writer().await?;
        let resolved = client
            .execute(
                concat!(
                    "UPDATE webhook_redeliveries SET resolved_at = ",
                    ts!("$2"),
                    " WHERE delivery_guid = $1 AND resolved_at IS NULL"
                ),
                &[&delivery_guid, &resolved_at_us],
            )
            .await
            .map_err(db)?;
        Ok(resolved > 0)
    }
}
