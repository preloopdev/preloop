//! Durable check-run projection and sender support.
//!
//! The projector is deliberately separate from the GitHub client: it only
//! reads committed outbox rows and writes the coalesced desired-state table.

use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::Notify;

use crate::control::Backend;

const BATCH: usize = 256;
const LEASE: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(250);

/// Start the durable projector for either backend. PostgreSQL notifications are
/// converted into the same local dirty signal used by SQLite and by writers on
/// this node; polling remains the recovery path for a lost notification.
pub(crate) fn spawn_consumer(backend: &Arc<Backend>, dirty: Arc<Notify>) {
    if let Some(mut notifications) = backend.subscribe_event_notifications() {
        let wake = Arc::clone(&dirty);
        tokio::spawn(async move {
            loop {
                match notifications.recv().await {
                    Ok(_) => wake.notify_one(),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => wake.notify_one(),
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }
    tokio::spawn(run_consumer(Arc::downgrade(backend), dirty));
}

async fn run_consumer(backend: Weak<Backend>, dirty: Arc<Notify>) {
    let owner = uuid::Uuid::new_v4().to_string();
    loop {
        let Some(backend_ref) = backend.upgrade() else {
            return;
        };
        let mut processed = 0usize;
        loop {
            match backend_ref
                .consume_check_run_outbox(&owner, LEASE, BATCH)
                .await
            {
                Ok(n) => {
                    processed += n;
                    if n < BATCH {
                        break;
                    }
                }
                Err(error) => {
                    tracing::warn!(?error, "check-run outbox projection failed");
                    break;
                }
            }
        }
        drop(backend_ref);
        if processed > 0 {
            continue;
        }
        tokio::select! {
            _ = dirty.notified() => {},
            _ = tokio::time::sleep(POLL) => {},
        }
    }
}
