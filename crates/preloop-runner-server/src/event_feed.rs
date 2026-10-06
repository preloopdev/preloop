//! Cross-node live events.
//!
//! A node broadcasts the events its own commands produce straight to its
//! attached clients ([`crate::state::AppState::emit`]). Events committed
//! through another node reach it here, by way of the transactional outbox:
//!
//! ```text
//! writer node : command txn -> outbox_events row -> commit
//!               emit()      -> mark `dirty`
//! notifier    : on `dirty`, wait ~15 ms, one autocommit NOTIFY preloop_events
//! every node  : on NOTIFY (or every second, for a lost one) read the rows after
//!               its bookmark from finished transactions, skip its own rows,
//!               drop states older than ones already delivered, broadcast the rest
//! ```
//!
//! NOTIFY is only a hint: the table is the source of truth, and the one-second
//! read is what covers a lost or coalesced notification. The notifier is a
//! single task, outside the command transactions, because a transaction that
//! calls NOTIFY holds a database-wide lock until its commit is flushed.
//!
//! Rows arrive in `(txid, event_id)` order, which is not commit order, so a
//! status event carries the version of the row it reports and the consumer
//! keeps only the newest per run and per job. The bookmark is in memory: a
//! restarted node has no attached clients to catch up, and a client that
//! reconnects starts from a database snapshot.

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use preloop_gha_protocol::{NdjsonEvent, RunId};
use tokio::sync::{Notify, broadcast};

use crate::control::Backend;
use crate::control::types::OutboxRow;

/// Events committed within this window share one notification.
const NOTIFY_COALESCE: Duration = Duration::from_millis(15);
/// Read the outbox at least this often, notification or not.
const POLL_FALLBACK: Duration = Duration::from_secs(1);
/// Rows per read.
const READ_BATCH: usize = 500;
/// How long the newest delivered version of a run or job is remembered.
const SEEN_TTL: Duration = Duration::from_secs(120);

/// Start the notifier and the consumer for a Postgres backend. A SQLite
/// backend has one node and nothing to receive, so nothing is started. The
/// tasks hold the backend weakly and end when it is dropped.
pub(crate) fn spawn(
    backend: &Arc<Backend>,
    events: broadcast::Sender<NdjsonEvent>,
    dirty: Arc<Notify>,
) {
    let Some(origin) = backend.event_origin().map(str::to_owned) else {
        return;
    };
    tokio::spawn(run_notifier(Arc::downgrade(backend), dirty));
    tokio::spawn(run_consumer(Arc::downgrade(backend), events, origin));
}

async fn run_notifier(backend: Weak<Backend>, dirty: Arc<Notify>) {
    loop {
        // A bounded wait, so a dropped backend ends the task.
        if tokio::time::timeout(Duration::from_secs(60), dirty.notified())
            .await
            .is_err()
        {
            if backend.strong_count() == 0 {
                return;
            }
            continue;
        }
        tokio::time::sleep(NOTIFY_COALESCE).await;
        let Some(backend) = backend.upgrade() else {
            return;
        };
        if let Err(error) = backend.notify_events().await {
            tracing::warn!(
                ?error,
                "event notification failed; readers fall back to polling"
            );
        }
    }
}

async fn run_consumer(
    backend: Weak<Backend>,
    events: broadcast::Sender<NdjsonEvent>,
    origin: String,
) {
    let Some(mut notifications) = backend
        .upgrade()
        .and_then(|backend| backend.subscribe_event_notifications())
    else {
        return;
    };
    let mut bookmark = loop {
        let Some(live) = backend.upgrade() else {
            return;
        };
        match live.outbox_head().await {
            Ok(Some(head)) => break head,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(?error, "outbox head read failed; retrying");
                drop(live);
                tokio::time::sleep(POLL_FALLBACK).await;
            }
        }
    };
    let mut filter = VersionFilter::default();
    loop {
        loop {
            let Some(live) = backend.upgrade() else {
                return;
            };
            let rows = match live.outbox_read(bookmark, READ_BATCH).await {
                Ok(rows) => rows,
                Err(error) => {
                    tracing::warn!(?error, "outbox read failed; retrying");
                    break;
                }
            };
            let count = rows.len();
            for row in rows {
                bookmark = row.bookmark;
                if let Some(event) = admit(&mut filter, &origin, row, Instant::now()) {
                    let _ = events.send(event);
                }
            }
            if count < READ_BATCH {
                break;
            }
        }
        tokio::select! {
            note = notifications.recv() => {
                if matches!(note, Err(broadcast::error::RecvError::Closed)) {
                    return;
                }
            }
            _ = tokio::time::sleep(POLL_FALLBACK) => {}
        }
    }
}

/// The event an outbox row delivers to this node's clients, if any: not one
/// this node wrote (it broadcast those itself), an `NdjsonEvent` (the outbox
/// also carries plain state-change rows), and not older than a state already
/// delivered.
fn admit(
    filter: &mut VersionFilter,
    origin: &str,
    row: OutboxRow,
    now: Instant,
) -> Option<NdjsonEvent> {
    if row.origin == origin {
        return None;
    }
    let event: NdjsonEvent = serde_json::from_value(row.payload).ok()?;
    filter
        .admit(event.run_id(), row.job_id.as_deref(), row.version, now)
        .then_some(event)
}

/// Keeps the newest delivered version per run and per job, so a state that
/// arrives after a newer one is dropped.
#[derive(Default)]
struct VersionFilter {
    seen: HashMap<(RunId, Option<String>), (i64, Instant)>,
    last_sweep: Option<Instant>,
}

impl VersionFilter {
    /// Whether to deliver an event. Events with no version make no ordering
    /// claim and always pass.
    fn admit(
        &mut self,
        run_id: RunId,
        job_id: Option<&str>,
        version: Option<i64>,
        now: Instant,
    ) -> bool {
        self.sweep(now);
        let Some(version) = version else {
            return true;
        };
        let key = (run_id, job_id.map(str::to_owned));
        match self.seen.get_mut(&key) {
            Some((newest, at)) => {
                if version <= *newest {
                    return false;
                }
                *newest = version;
                *at = now;
            }
            None => {
                self.seen.insert(key, (version, now));
            }
        }
        true
    }

    /// Forget entries not updated within [`SEEN_TTL`], at most every 30s.
    fn sweep(&mut self, now: Instant) {
        if self
            .last_sweep
            .is_some_and(|at| now.duration_since(at) < Duration::from_secs(30))
        {
            return;
        }
        self.last_sweep = Some(now);
        self.seen
            .retain(|_, (_, at)| now.duration_since(*at) < SEEN_TTL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::backend::ControlBackend;
    use crate::control::types::OutboxBookmark;
    use preloop_gha_protocol::{ExecutionStatus, JobId};

    fn row(
        run: RunId,
        job: &str,
        version: Option<i64>,
        status: ExecutionStatus,
        origin: &str,
    ) -> OutboxRow {
        let event = NdjsonEvent::JobStatus {
            run_id: run,
            job_id: JobId(job.to_owned()),
            status,
            reason: None,
        };
        OutboxRow {
            bookmark: OutboxBookmark::default(),
            run_id: Some(run),
            job_id: Some(job.to_owned()),
            version,
            origin: origin.to_owned(),
            topic: "job_status.v1".to_owned(),
            payload: serde_json::to_value(event).unwrap(),
        }
    }

    /// A state that arrives after a newer one of the same job is dropped, a
    /// newer one passes, and another job or an unversioned event is
    /// unaffected.
    #[test]
    fn older_states_are_dropped_per_job() {
        let run = RunId::new();
        let mut filter = VersionFilter::default();
        let now = Instant::now();
        let deliver = |filter: &mut VersionFilter, job: &str, version: Option<i64>| {
            admit(
                filter,
                "me",
                row(run, job, version, ExecutionStatus::InProgress, "other"),
                now,
            )
            .is_some()
        };
        assert!(deliver(&mut filter, "build", Some(2)));
        assert!(!deliver(&mut filter, "build", Some(1)), "older state");
        assert!(!deliver(&mut filter, "build", Some(2)), "duplicate");
        assert!(deliver(&mut filter, "build", Some(3)));
        assert!(deliver(&mut filter, "test", Some(1)), "other job");
        assert!(deliver(&mut filter, "build", None), "no ordering claim");
    }

    /// A node does not re-deliver the rows it wrote, and rows that are not
    /// stream events are skipped.
    #[test]
    fn own_rows_and_plain_state_rows_are_skipped() {
        let run = RunId::new();
        let mut filter = VersionFilter::default();
        let now = Instant::now();
        let own = row(run, "build", Some(1), ExecutionStatus::Success, "me");
        assert!(admit(&mut filter, "me", own, now).is_none());
        let mut plain = row(run, "build", None, ExecutionStatus::Success, "other");
        plain.topic = "job.completed.v1".to_owned();
        plain.payload = serde_json::json!({"job_id": "build", "status": "success"});
        assert!(admit(&mut filter, "me", plain, now).is_none());
    }

    /// Versions are forgotten after the TTL, so the map is bounded.
    #[test]
    fn seen_versions_expire() {
        let run = RunId::new();
        let mut filter = VersionFilter::default();
        let start = Instant::now();
        assert!(filter.admit(run, Some("build"), Some(5), start));
        let later = start + SEEN_TTL + Duration::from_secs(31);
        assert!(filter.admit(RunId::new(), Some("x"), Some(1), later));
        assert_eq!(filter.seen.len(), 1, "the expired entry was swept");
    }

    async fn node(url: &str) -> Arc<Backend> {
        Arc::new(Backend::Postgres(
            crate::control::pg::PgBackend::connect(url, false, false, Duration::from_secs(300))
                .await
                .expect("test database connection failed"),
        ))
    }

    fn accepted(run_id: RunId) -> NdjsonEvent {
        NdjsonEvent::RunAccepted {
            run_id,
            queued_jobs: 1,
        }
    }

    /// An event committed through node A reaches node B's channel, and an
    /// event node B wrote itself is not delivered a second time.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn events_committed_through_another_node_reach_this_nodes_channel() {
        let Some((_db, url)) = crate::test_pg::fresh_database().await else {
            eprintln!("skipping: set PRELOOP_TEST_POSTGRES_URL to a Postgres server");
            return;
        };
        let node_a = node(&url).await;
        let node_b = node(&url).await;
        let (events, mut received) = broadcast::channel(64);
        spawn(&node_b, events, Arc::new(Notify::new()));

        // The consumer reads its start position asynchronously, and rows
        // committed before it counts as history: keep writing until one is
        // delivered.
        let own = RunId::new();
        node_b.append_event(&accepted(own)).await.unwrap();
        let mut delivered = None;
        for _ in 0..50 {
            let remote = RunId::new();
            node_a.append_event(&accepted(remote)).await.unwrap();
            node_a.notify_events().await.unwrap();
            if let Ok(Ok(event)) =
                tokio::time::timeout(Duration::from_millis(300), received.recv()).await
            {
                delivered = Some((remote, event));
                break;
            }
        }
        let (_, event) = delivered.expect("an event from node A must reach node B");
        assert!(matches!(event, NdjsonEvent::RunAccepted { .. }));
        while let Ok(event) = received.try_recv() {
            assert_ne!(
                event.run_id(),
                own,
                "a node's own rows are not re-delivered"
            );
        }
        assert_ne!(event.run_id(), own);
    }

    /// `emit`'s dirty mark becomes a notification on the other nodes, and many
    /// marks inside the coalescing window do not each send one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dirty_marks_are_coalesced_into_one_notification() {
        let Some((_db, url)) = crate::test_pg::fresh_database().await else {
            eprintln!("skipping: set PRELOOP_TEST_POSTGRES_URL to a Postgres server");
            return;
        };
        let node_a = node(&url).await;
        let node_b = node(&url).await;
        let dirty = Arc::new(Notify::new());
        let (events, _unused) = broadcast::channel(8);
        spawn(&node_a, events, dirty.clone());
        let mut notes = node_b.subscribe_event_notifications().unwrap();

        // The LISTEN connection registers asynchronously: retry the first
        // notification until it lands. An empty payload is the listener's own
        // "reconnected, re-read" hint, not a notification from a node.
        let mut first = None;
        for _ in 0..50 {
            dirty.notify_one();
            if let Ok(Ok(origin)) =
                tokio::time::timeout(Duration::from_millis(300), notes.recv()).await
                && !origin.is_empty()
            {
                first = Some(origin);
                break;
            }
        }
        assert_eq!(
            first.as_deref(),
            node_a.event_origin(),
            "the notification names the writing node"
        );
        // Drain stragglers from the retries, then send a burst.
        tokio::time::sleep(Duration::from_millis(200)).await;
        while notes.try_recv().is_ok() {}
        for _ in 0..200 {
            dirty.notify_one();
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
        let mut count = 0;
        while notes.try_recv().is_ok() {
            count += 1;
        }
        assert!(
            (1..=3).contains(&count),
            "200 marks inside the window sent {count} notifications"
        );
    }
}
