//! Cross-node hints: runner wake-ups, "events were committed" and
//! "webhook deliveries became claimable".
//!
//! A node wakes its own waiters directly. On Postgres, the committing
//! transaction also issues `pg_notify(CHANNEL, …)`; every node keeps one
//! dedicated `LISTEN` connection and turns notifications into local wakes,
//! so a job submitted through one node wakes runners polling another.
//! The same connection carries [`EVENTS_CHANNEL`], which tells the event
//! consumer ([`crate::event_feed`]) to read the outbox, and [`WEBHOOK_CHANNEL`],
//! which tells the webhook worker that deliveries became claimable.
//! Notifications are hints: a lost one only delays a poll until its long-poll
//! window (or the consumer's one-second read, or the worker's five-second tick)
//! re-checks.

use std::time::Duration;

use tokio::sync::broadcast;

/// Postgres notification channel.
pub(crate) const CHANNEL: &str = "preloop_wake";

/// Postgres notification channel for "events were committed" hints. Sent by
/// each node's event notifier outside any command transaction: a transaction
/// that calls NOTIFY takes a database-wide lock until its commit is flushed,
/// so notifying from command transactions would serialize them.
pub(crate) const EVENTS_CHANNEL: &str = "preloop_events";

/// Postgres notification channel for "webhook deliveries became claimable"
/// hints. Each node's webhook notifier sends it outside any transaction, for
/// the reason given at [`EVENTS_CHANNEL`]. The payload is the writing node's
/// origin, so a node can drop its own echo: its enqueue already woke its worker.
pub(crate) const WEBHOOK_CHANNEL: &str = "preloop_webhooks";

/// One wake signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Wake {
    /// Jobs that became claimable: wake at most this many waiters.
    pub(crate) ready: usize,
    /// Wake every waiter (cancellations must reach the owning runner).
    pub(crate) broadcast: bool,
}

impl Wake {
    pub(crate) fn encode(self) -> String {
        format!("{}:{}", self.ready, u8::from(self.broadcast))
    }

    pub(crate) fn decode(payload: &str) -> Option<Self> {
        let (ready, broadcast) = payload.split_once(':')?;
        Some(Self {
            ready: ready.parse().ok()?,
            broadcast: broadcast == "1",
        })
    }
}

/// The channels a node's dedicated connection republishes.
pub(crate) struct Listener {
    /// Runner wake hints ([`CHANNEL`]).
    pub(crate) wakes: broadcast::Sender<Wake>,
    /// "Events were committed" hints ([`EVENTS_CHANNEL`]); the payload is
    /// the writing node's origin.
    pub(crate) events: broadcast::Sender<String>,
    /// "Webhook deliveries became claimable" hints ([`WEBHOOK_CHANNEL`]); the
    /// payload is the writing node's origin.
    pub(crate) webhooks: broadcast::Sender<String>,
}

/// Spawn the listener: a dedicated connection that `LISTEN`s on [`CHANNEL`],
/// [`EVENTS_CHANNEL`] and [`WEBHOOK_CHANNEL`] and republishes every notification
/// on the returned channels. Reconnects with backoff when the connection
/// drops (failover, restart).
pub(crate) fn spawn_listener(url: String) -> Listener {
    let (wakes, _) = broadcast::channel(1024);
    let (events, _) = broadcast::channel(1024);
    let (webhooks, _) = broadcast::channel(1024);
    let listener = Listener {
        wakes: wakes.clone(),
        events: events.clone(),
        webhooks: webhooks.clone(),
    };
    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(200);
        loop {
            match listen_once(&url, &wakes, &events, &webhooks).await {
                Ok(()) => backoff = Duration::from_millis(200),
                Err(error) => {
                    tracing::warn!(%error, "wake listener disconnected; reconnecting");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(10));
                }
            }
        }
    });
    listener
}

async fn listen_once(
    url: &str,
    wakes: &broadcast::Sender<Wake>,
    events: &broadcast::Sender<String>,
    webhooks: &broadcast::Sender<String>,
) -> anyhow::Result<()> {
    let connect_url = crate::store_pg::connect_url(url);
    // The connection must be polled for the client's LISTEN to complete, so
    // it runs on its own task; notifications come back over `notes`.
    let (notes_tx, mut notes) = tokio::sync::mpsc::unbounded_channel();
    let client = match crate::store_pg::tls_connector(url)? {
        Some(tls) => {
            let (client, connection) = tokio_postgres::connect(&connect_url, tls).await?;
            tokio::spawn(drive(connection, notes_tx));
            client
        }
        None => {
            let (client, connection) =
                tokio_postgres::connect(&connect_url, tokio_postgres::NoTls).await?;
            tokio::spawn(drive(connection, notes_tx));
            client
        }
    };
    client
        .batch_execute(&format!(
            "LISTEN {CHANNEL}; LISTEN {EVENTS_CHANNEL}; LISTEN {WEBHOOK_CHANNEL}"
        ))
        .await?;
    // A reconnect may have missed events committed while disconnected: wake
    // the event consumers once so they re-read from their bookmark.
    let _ = events.send(String::new());
    // The webhook worker re-reads the queue on any wake, so it gets one too.
    let _ = webhooks.send(String::new());
    while let Some((channel, payload)) = notes.recv().await {
        if channel == CHANNEL {
            if let Some(wake) = Wake::decode(&payload) {
                let _ = wakes.send(wake);
            }
        } else if channel == EVENTS_CHANNEL {
            let _ = events.send(payload);
        } else if channel == WEBHOOK_CHANNEL {
            let _ = webhooks.send(payload);
        }
    }
    anyhow::bail!("wake listener connection closed")
}

/// Forward other nodes' webhook hints to this node's worker until the channel
/// closes. A hint with this node's own origin is dropped: the enqueue or requeue
/// that sent it already woke the worker. A lagged receiver wakes the worker,
/// since it may have missed hints.
pub(crate) async fn forward_webhook_wakes(
    mut hints: broadcast::Receiver<String>,
    own_origin: String,
    worker: std::sync::Arc<tokio::sync::Notify>,
) {
    loop {
        match hints.recv().await {
            // This node's own enqueue or requeue already woke the worker.
            Ok(origin) if origin == own_origin => {}
            // A foreign hint, or hints missed while lagging: either way the
            // worker re-reads the queue.
            Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => worker.notify_one(),
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}

/// Poll a connection, forwarding `(channel, payload)` of each notification
/// until it closes.
async fn drive<S, T>(
    mut connection: tokio_postgres::Connection<S, T>,
    notes: tokio::sync::mpsc::UnboundedSender<(String, String)>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use futures::StreamExt;
    let mut messages = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
    while let Some(Ok(message)) = messages.next().await {
        if let tokio_postgres::AsyncMessage::Notification(note) = message
            && notes
                .send((note.channel().to_owned(), note.payload().to_owned()))
                .is_err()
        {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Wake, forward_webhook_wakes};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{Notify, broadcast};

    #[test]
    fn wake_round_trips_and_rejects_garbage() {
        for wake in [
            Wake {
                ready: 0,
                broadcast: true,
            },
            Wake {
                ready: 17,
                broadcast: false,
            },
        ] {
            assert_eq!(Wake::decode(&wake.encode()), Some(wake));
        }
        assert_eq!(Wake::decode("nope"), None);
        assert_eq!(Wake::decode("x:1"), None);
    }

    /// Whether `worker` is woken within a short window. A permit an earlier
    /// `notify_one` left behind counts, and the call consumes it.
    async fn woken(worker: &Notify) -> bool {
        tokio::time::timeout(Duration::from_millis(100), worker.notified())
            .await
            .is_ok()
    }

    /// A forwarder for `own` on a fresh worker. It subscribes before returning,
    /// so hints sent afterwards are not missed.
    fn forwarder(hints: &broadcast::Sender<String>, own: &str) -> Arc<Notify> {
        let worker = Arc::new(Notify::new());
        tokio::spawn(forward_webhook_wakes(
            hints.subscribe(),
            own.to_owned(),
            worker.clone(),
        ));
        worker
    }

    #[tokio::test]
    async fn own_webhook_echo_does_not_wake_the_worker() {
        let (hints, _) = broadcast::channel(16);
        let worker = forwarder(&hints, "node-a");
        hints.send("node-a".to_owned()).unwrap();
        assert!(
            !woken(&worker).await,
            "a node's own echo must not wake its worker"
        );
    }

    #[tokio::test]
    async fn foreign_webhook_hint_wakes_the_worker() {
        let (hints, _) = broadcast::channel(16);
        let worker = forwarder(&hints, "node-a");
        hints.send("node-b".to_owned()).unwrap();
        assert!(woken(&worker).await);
    }

    #[tokio::test]
    async fn reconnect_marker_wakes_the_worker() {
        // The listener sends an empty payload after it reconnects.
        let (hints, _) = broadcast::channel(16);
        let worker = forwarder(&hints, "node-a");
        hints.send(String::new()).unwrap();
        assert!(woken(&worker).await);
    }

    #[tokio::test]
    async fn lagged_receiver_wakes_the_worker() {
        let (hints, _) = broadcast::channel(2);
        let worker = forwarder(&hints, "node-a");
        // Overflow the two-slot buffer: the receiver misses hints, own ones too.
        for _ in 0..4 {
            hints.send("node-a".to_owned()).unwrap();
        }
        assert!(
            woken(&worker).await,
            "a lagged receiver may have missed a foreign hint"
        );
    }

    #[tokio::test]
    async fn forwarder_ends_when_the_channel_closes() {
        let (hints, _) = broadcast::channel::<String>(4);
        let worker = Arc::new(Notify::new());
        let task = tokio::spawn(forward_webhook_wakes(
            hints.subscribe(),
            "node-a".to_owned(),
            worker,
        ));
        drop(hints);
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("the forwarder ends with its channel")
            .unwrap();
    }
}
