//! Cross-node hints: runner wake-ups and "events were committed".
//!
//! A node wakes its own waiters directly. On Postgres, the committing
//! transaction also issues `pg_notify(CHANNEL, …)`; every node keeps one
//! dedicated `LISTEN` connection and turns notifications into local wakes,
//! so a job submitted through one node wakes runners polling another.
//! The same connection carries [`EVENTS_CHANNEL`], which tells the event
//! consumer ([`crate::event_feed`]) to read the outbox.
//! Notifications are hints: a lost one only delays a poll until its
//! long-poll window (or the consumer's one-second read) re-checks.

use std::time::Duration;

use tokio::sync::broadcast;

/// Postgres notification channel.
pub(crate) const CHANNEL: &str = "preloop_wake";

/// Postgres notification channel for "events were committed" hints. Sent by
/// each node's event notifier outside any command transaction: a transaction
/// that calls NOTIFY takes a database-wide lock until its commit is flushed,
/// so notifying from command transactions would serialize them.
pub(crate) const EVENTS_CHANNEL: &str = "preloop_events";

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
}

/// Spawn the listener: a dedicated connection that `LISTEN`s on
/// [`CHANNEL`] and [`EVENTS_CHANNEL`] and republishes every notification
/// on the returned channels. Reconnects with backoff when the connection
/// drops (failover, restart).
pub(crate) fn spawn_listener(url: String) -> Listener {
    let (wakes, _) = broadcast::channel(1024);
    let (events, _) = broadcast::channel(1024);
    let listener = Listener {
        wakes: wakes.clone(),
        events: events.clone(),
    };
    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(200);
        loop {
            match listen_once(&url, &wakes, &events).await {
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
        .batch_execute(&format!("LISTEN {CHANNEL}; LISTEN {EVENTS_CHANNEL}"))
        .await?;
    // A reconnect may have missed events committed while disconnected: wake
    // the event consumers once so they re-read from their bookmark.
    let _ = events.send(String::new());
    while let Some((channel, payload)) = notes.recv().await {
        if channel == CHANNEL {
            if let Some(wake) = Wake::decode(&payload) {
                let _ = wakes.send(wake);
            }
        } else if channel == EVENTS_CHANNEL {
            let _ = events.send(payload);
        }
    }
    anyhow::bail!("wake listener connection closed")
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
    use super::Wake;

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
}
