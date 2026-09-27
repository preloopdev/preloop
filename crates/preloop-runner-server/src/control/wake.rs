//! Runner wake-ups: how a committed enqueue reaches waiting long-polls.
//!
//! A node wakes its own waiters directly. On Postgres, the committing
//! transaction also issues `pg_notify(CHANNEL, …)`; every node keeps one
//! dedicated `LISTEN` connection and turns notifications into local wakes,
//! so a job submitted through one node wakes runners polling another.
//! Notifications are hints: a lost one only delays a poll until its
//! long-poll window re-checks the queue.

use std::time::Duration;

use tokio::sync::broadcast;

/// Postgres notification channel.
pub(crate) const CHANNEL: &str = "preloop_wake";

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

/// Spawn the listener: a dedicated connection that `LISTEN`s on
/// [`CHANNEL`] and republishes every notification on the returned channel.
/// Reconnects with backoff when the connection drops (failover, restart).
pub(crate) fn spawn_listener(url: String) -> broadcast::Sender<Wake> {
    let (sender, _) = broadcast::channel(1024);
    let tx = sender.clone();
    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(200);
        loop {
            match listen_once(&url, &tx).await {
                Ok(()) => backoff = Duration::from_millis(200),
                Err(error) => {
                    tracing::warn!(%error, "wake listener disconnected; reconnecting");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(10));
                }
            }
        }
    });
    sender
}

async fn listen_once(url: &str, tx: &broadcast::Sender<Wake>) -> anyhow::Result<()> {
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
    client.batch_execute(&format!("LISTEN {CHANNEL}")).await?;
    while let Some(payload) = notes.recv().await {
        if let Some(wake) = Wake::decode(&payload) {
            let _ = tx.send(wake);
        }
    }
    anyhow::bail!("wake listener connection closed")
}

/// Poll a connection, forwarding notification payloads until it closes.
async fn drive<S, T>(
    mut connection: tokio_postgres::Connection<S, T>,
    notes: tokio::sync::mpsc::UnboundedSender<String>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use futures::StreamExt;
    let mut messages = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
    while let Some(Ok(message)) = messages.next().await {
        if let tokio_postgres::AsyncMessage::Notification(note) = message {
            if notes.send(note.payload().to_owned()).is_err() {
                return;
            }
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
