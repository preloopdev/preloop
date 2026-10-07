//! The consumer side of `outbox_events`: ordered reads below the safe point,
//! the "events were committed" notification, and retention.

use super::codec;
use super::{PgBackend, db};
use crate::control::types::{ControlError, OutboxBookmark, OutboxRow};

impl PgBackend {
    /// This node process's id, stamped on every outbox row it writes.
    pub(crate) fn origin(&self) -> &str {
        &self.origin
    }

    /// Notifications that another node committed events (payload: the
    /// writer's origin; empty after the listener reconnected).
    pub(crate) fn subscribe_event_notifications(&self) -> tokio::sync::broadcast::Receiver<String> {
        self.event_wakes.subscribe()
    }

    /// `outbox_head`: the bookmark a consumer that starts now reads from.
    /// Every transaction that finishes after this call has a txid at or
    /// above the snapshot's `xmin`, so a read after `(xmin, 0)` sees all of
    /// them and none of the history.
    ///
    /// Statements: `SELECT pg_snapshot_xmin(pg_current_snapshot())`.
    pub(crate) async fn outbox_head(&self) -> Result<OutboxBookmark, ControlError> {
        let client = self.reader().await?;
        let row = client
            .query_one("SELECT pg_snapshot_xmin(pg_current_snapshot())::text", &[])
            .await
            .map_err(db)?;
        let txid = row
            .get::<_, String>(0)
            .parse()
            .map_err(ControlError::backend)?;
        Ok(OutboxBookmark { txid, event_id: 0 })
    }

    /// `outbox_read`: up to `limit` rows after `after`, in `(txid,
    /// event_id)` order, from finished transactions only. A transaction still
    /// open has a txid at or above the snapshot's `xmin`, so rows below it
    /// cannot be joined by a later commit and a bookmark that has passed them
    /// never skips one.
    ///
    /// Statements: one `SELECT .. FROM outbox_events WHERE (txid, event_id) >
    /// $1 AND txid < pg_snapshot_xmin(pg_current_snapshot()) ORDER BY txid,
    /// event_id LIMIT $2` on `outbox_events_read`.
    pub(crate) async fn outbox_read(
        &self,
        after: OutboxBookmark,
        limit: usize,
    ) -> Result<Vec<OutboxRow>, ControlError> {
        let client = self.reader().await?;
        let rows = client
            .query(
                "SELECT txid::text, event_id, run_id::text, job_id, version, origin, topic, \
                 payload::text \
                 FROM outbox_events \
                 WHERE (txid, event_id) > ($1::text::xid8, $2) \
                   AND txid < pg_snapshot_xmin(pg_current_snapshot()) \
                 ORDER BY txid, event_id LIMIT $3",
                &[&after.txid.to_string(), &after.event_id, &(limit as i64)],
            )
            .await
            .map_err(db)?;
        rows.iter()
            .map(|row| {
                Ok(OutboxRow {
                    bookmark: OutboxBookmark {
                        txid: row
                            .get::<_, String>(0)
                            .parse()
                            .map_err(ControlError::backend)?,
                        event_id: row.get(1),
                    },
                    run_id: row
                        .get::<_, Option<String>>(2)
                        .map(|run| codec::run_id(&run))
                        .transpose()?,
                    job_id: row.get(3),
                    version: row.get(4),
                    origin: row.get(5),
                    topic: row.get(6),
                    payload: codec::from_json(row.get::<_, String>(7).as_str())?,
                })
            })
            .collect()
    }

    /// `notify_events`: tell every node (this one included) that events were
    /// committed. One autocommit `pg_notify` on its own connection: a
    /// transaction that calls NOTIFY holds a database-wide lock through its
    /// commit, so event writers never notify inside their transactions.
    ///
    /// Statements: `SELECT pg_notify('preloop_events', origin)`.
    pub(crate) async fn notify_events(&self) -> Result<(), ControlError> {
        let client = self.writer().await?;
        client
            .execute(
                "SELECT pg_notify($1, $2)",
                &[&crate::control::wake::EVENTS_CHANNEL, &self.origin],
            )
            .await
            .map_err(db)?;
        Ok(())
    }

    /// `prune_outbox`: delete up to `limit` rows older than `older_than`,
    /// oldest first. Returns rows removed.
    ///
    /// Statements: `DELETE FROM outbox_events WHERE (event_id, created_at) IN
    /// (SELECT .. WHERE created_at < now() - $1 ORDER BY created_at LIMIT $2)`
    /// on `outbox_events_age`.
    pub(super) async fn prune_outbox(
        &self,
        older_than: std::time::Duration,
        limit: usize,
    ) -> Result<u64, ControlError> {
        let client = self.writer().await?;
        client
            .execute(
                "DELETE FROM outbox_events o WHERE (o.event_id,o.created_at) IN \
                 (SELECT e.event_id,e.created_at FROM outbox_events e \
                  WHERE e.created_at < now() - make_interval(secs => $1) \
                    AND (NOT EXISTS (SELECT 1 FROM consumer_offsets) OR \
                         (e.txid,e.event_id) <= \
                         (SELECT last_txid,last_event_id FROM consumer_offsets \
                          ORDER BY last_txid,last_event_id LIMIT 1)) \
                  ORDER BY e.created_at LIMIT $2)",
                &[&older_than.as_secs_f64(), &(limit as i64)],
            )
            .await
            .map_err(db)
    }
}
