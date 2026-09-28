//! The PostgreSQL `ControlBackend` against the agreed schema
//! (`docs/control-schema.sql`, copied verbatim into `schema.sql` plus the
//! `schema_meta` bookkeeping table).
//!
//! Every command is one short transaction of targeted statements: no
//! working-set load, no write-back, no advisory locks. Transitions are
//! conditional `UPDATE`s (zero rows = someone else won); queues are consumed
//! with `FOR UPDATE SKIP LOCKED`; ids come from identity columns. A command
//! that needs a consistent view of one run's jobs serializes on the run row
//! (`SELECT .. FROM runs WHERE run_id = $1 FOR UPDATE`).
//!
//! Layout: one command per function, each with a doc comment naming its
//! statements, so the SQLite backend (`control/lite`) can mirror it:
//! - [`codec`]: SQL fragments and row codecs shared by every command.
//! - [`webhooks`]: webhook inbox, watchdog cursor, redelivery repair rows.
//! - [`timelines`]: timelines, timeline records, step manifests, log ids.
//! - [`runners`]: runner registration, sessions, leases and renewals.
//! - [`lookups`]: request/callback/check-run/run lookups, push state,
//!   run numbers, key fingerprint, archival and queue statistics.

mod codec;
mod dispatch;
mod graph;
mod lifecycle;
mod lookups;
mod runners;
mod timelines;
mod webhooks;

use super::types::ControlError;
use tokio_postgres::{Client, NoTls};

/// The schema this build creates and accepts. Greenfield v1: there are no
/// migrations, a database at any other version is refused.
pub(crate) const SCHEMA_VERSION: &str = "1";

/// The agreed schema plus `schema_meta`.
const SCHEMA_SQL: &str = include_str!("schema.sql");

/// Writer connections per node (`PRELOOP_PG_WRITERS`, default 16).
const WRITERS_ENV: &str = "PRELOOP_PG_WRITERS";
/// Reader connections per node (`PRELOOP_PG_READERS`, default 16).
const READERS_ENV: &str = "PRELOOP_PG_READERS";
const DEFAULT_POOL_SIZE: usize = 16;

/// How often a booting node re-checks schema setup after losing the
/// creation race to another node.
const SCHEMA_SETUP_ATTEMPTS: usize = 5;

/// Pool size from `var`, clamped to at least one connection. Size the pools
/// so every node's `writers + readers + 1` (wake listener) fits
/// `max_connections`.
fn pool_size(var: &str) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_POOL_SIZE)
        .max(1)
}

/// A fixed-size connection pool. Checkout blocks until a connection is
/// free; a connection whose socket died is replaced on checkout.
struct Pool {
    url: String,
    tx: tokio::sync::mpsc::Sender<Client>,
    rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Client>>,
}

impl Pool {
    async fn open(url: &str, size: usize) -> Result<Self, ControlError> {
        let (tx, rx) = tokio::sync::mpsc::channel(size);
        let clients = futures::future::try_join_all((0..size).map(|_| connect_one(url))).await?;
        for client in clients {
            tx.try_send(client)
                .map_err(|_| ControlError::backend(anyhow::anyhow!("pool channel full")))?;
        }
        Ok(Self {
            url: url.to_owned(),
            tx,
            rx: tokio::sync::Mutex::new(rx),
        })
    }

    async fn checkout(&self) -> Result<Pooled<'_>, ControlError> {
        let client = self
            .rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| ControlError::backend(anyhow::anyhow!("connection pool closed")))?;
        let client = if client.is_closed() {
            match connect_one(&self.url).await {
                Ok(fresh) => fresh,
                Err(error) => {
                    // Keep the pool at size: hand the dead client back so the
                    // next checkout retries the reconnect.
                    let _ = self.tx.try_send(client);
                    return Err(error);
                }
            }
        } else {
            client
        };
        Ok(Pooled {
            pool: self,
            client: Some(client),
        })
    }
}

/// A checked-out connection, returned to its pool on drop.
pub(super) struct Pooled<'a> {
    pool: &'a Pool,
    client: Option<Client>,
}

impl std::ops::Deref for Pooled<'_> {
    type Target = Client;
    fn deref(&self) -> &Client {
        self.client
            .as_ref()
            .expect("pooled client present until drop")
    }
}

impl std::ops::DerefMut for Pooled<'_> {
    fn deref_mut(&mut self) -> &mut Client {
        self.client
            .as_mut()
            .expect("pooled client present until drop")
    }
}

impl Drop for Pooled<'_> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            // Capacity equals the pool size, so returning never blocks.
            let _ = self.pool.tx.try_send(client);
        }
    }
}

/// The PostgreSQL control backend: a writer pool for commands, a reader
/// pool for queries, and the cross-node wake listener.
pub(crate) struct PgBackend {
    writers: Pool,
    readers: Pool,
    pool_assignments_enabled: std::sync::atomic::AtomicBool,
    require_job_assignments: std::sync::atomic::AtomicBool,
    runner_liveness_timeout: std::sync::atomic::AtomicU64,
    wakes: tokio::sync::broadcast::Sender<super::wake::Wake>,
}

impl PgBackend {
    /// Connect, create or verify the v1 schema, open the pools and start the
    /// wake listener. `url` is a `postgres://` connection string; TLS follows
    /// its `sslmode` (see `store_pg::tls_connector`).
    pub(crate) async fn connect(
        url: &str,
        pool_assignments_enabled: bool,
        require_job_assignments: bool,
        runner_liveness_timeout: std::time::Duration,
    ) -> Result<Self, ControlError> {
        let mut setup = connect_one(url).await?;
        ensure_schema(&mut setup).await?;
        drop(setup);
        let writers = Pool::open(url, pool_size(WRITERS_ENV)).await?;
        let readers = Pool::open(url, pool_size(READERS_ENV)).await?;
        Ok(Self {
            writers,
            readers,
            pool_assignments_enabled: std::sync::atomic::AtomicBool::new(pool_assignments_enabled),
            require_job_assignments: std::sync::atomic::AtomicBool::new(require_job_assignments),
            runner_liveness_timeout: std::sync::atomic::AtomicU64::new(
                runner_liveness_timeout.as_nanos() as u64,
            ),
            wakes: super::wake::spawn_listener(url.to_owned()),
        })
    }

    /// Subscribe to wake-ups committed by any node on this database.
    pub(crate) fn subscribe_wakes(&self) -> tokio::sync::broadcast::Receiver<super::wake::Wake> {
        self.wakes.subscribe()
    }

    /// The live scheduling config `(pool_assignments, require_assignments,
    /// liveness_timeout)`.
    pub(crate) fn config(&self) -> (bool, bool, std::time::Duration) {
        use std::sync::atomic::Ordering::Acquire;
        (
            self.pool_assignments_enabled.load(Acquire),
            self.require_job_assignments.load(Acquire),
            std::time::Duration::from_nanos(self.runner_liveness_timeout.load(Acquire)),
        )
    }

    /// Apply the real server config once bootstrap knows it.
    pub(crate) fn set_config(
        &self,
        pool_assignments_enabled: bool,
        require_job_assignments: bool,
        runner_liveness_timeout: std::time::Duration,
    ) {
        use std::sync::atomic::Ordering::Release;
        self.pool_assignments_enabled
            .store(pool_assignments_enabled, Release);
        self.require_job_assignments
            .store(require_job_assignments, Release);
        self.runner_liveness_timeout
            .store(runner_liveness_timeout.as_nanos() as u64, Release);
    }

    /// A connection for a command transaction.
    pub(super) async fn writer(&self) -> Result<Pooled<'_>, ControlError> {
        self.writers.checkout().await
    }

    /// A connection for a read-only query (never queued behind writers).
    pub(super) async fn reader(&self) -> Result<Pooled<'_>, ControlError> {
        self.readers.checkout().await
    }
}

/// Map a driver error into the backend error.
pub(super) fn db(error: tokio_postgres::Error) -> ControlError {
    ControlError::backend(error)
}

/// Create the v1 schema on a fresh database, or verify an existing one.
///
/// Nodes may boot against one database at once. Setup is one transaction
/// that starts with a strict `CREATE SCHEMA control` (no `IF NOT EXISTS`):
/// the first node creates everything atomically; a racing node blocks on
/// that catalog row, fails with `duplicate_schema` once the winner commits,
/// and re-reads `schema_meta`. A `control` schema without a readable
/// `schema_version` (the pre-rewrite layout, a half-created schema) is
/// refused rather than adopted.
///
/// Statements: `SELECT value FROM control.schema_meta WHERE key =
/// 'schema_version'`; else `BEGIN; CREATE SCHEMA control; <schema.sql>;
/// INSERT INTO schema_meta ('schema_version', '1'); INSERT INTO namespaces
/// ('default'); COMMIT`.
async fn ensure_schema(client: &mut Client) -> Result<(), ControlError> {
    for _ in 0..SCHEMA_SETUP_ATTEMPTS {
        if let Some(version) = stored_schema_version(client).await? {
            if version != SCHEMA_VERSION {
                return Err(ControlError::backend(anyhow::anyhow!(
                    "control schema has version {version}; this build supports only \
                     {SCHEMA_VERSION}. Recreate the database."
                )));
            }
            return Ok(());
        }
        let tx = client.transaction().await.map_err(db)?;
        let created = async {
            tx.batch_execute("CREATE SCHEMA control").await?;
            tx.batch_execute(SCHEMA_SQL).await?;
            tx.execute(
                "INSERT INTO schema_meta (key, value) VALUES ('schema_version', $1)",
                &[&SCHEMA_VERSION.as_bytes()],
            )
            .await?;
            tx.execute(
                "INSERT INTO namespaces (namespace_id) VALUES ($1)",
                &[&super::types::DEFAULT_NAMESPACE],
            )
            .await?;
            Ok::<_, tokio_postgres::Error>(())
        }
        .await;
        match created {
            Ok(()) => {
                tx.commit().await.map_err(db)?;
                return Ok(());
            }
            Err(error)
                if error.code() == Some(&tokio_postgres::error::SqlState::DUPLICATE_SCHEMA)
                    || error.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) =>
            {
                // Lost the race (or the schema predates this layout); the
                // next iteration reads what the winner committed.
                drop(tx);
                continue;
            }
            Err(error) => return Err(db(error)),
        }
    }
    Err(ControlError::backend(anyhow::anyhow!(
        "a `control` schema exists without a v1 `schema_meta` version; this build \
         supports only schema version {SCHEMA_VERSION}. Recreate the database."
    )))
}

/// `schema_meta.schema_version`, or `None` when the table (or row) does not
/// exist yet.
async fn stored_schema_version(client: &Client) -> Result<Option<String>, ControlError> {
    let exists: bool = client
        .query_one("SELECT to_regclass('control.schema_meta') IS NOT NULL", &[])
        .await
        .map_err(db)?
        .get(0);
    if !exists {
        return Ok(None);
    }
    let row = client
        .query_opt(
            "SELECT value FROM control.schema_meta WHERE key = 'schema_version'",
            &[],
        )
        .await
        .map_err(db)?;
    Ok(row.map(|row| String::from_utf8_lossy(row.get::<_, &[u8]>(0)).into_owned()))
}

/// Open one connection, spawn its driver task and resolve unqualified names
/// to `control.*`.
async fn connect_one(url: &str) -> Result<Client, ControlError> {
    let connect_url = crate::store_pg::connect_url(url);
    let client = match crate::store_pg::tls_connector(url).map_err(ControlError::backend)? {
        Some(tls) => {
            let (client, connection) = tokio_postgres::connect(&connect_url, tls)
                .await
                .map_err(db)?;
            tokio::spawn(async move {
                if let Err(error) = connection.await {
                    tracing::warn!(%error, "postgres control connection closed");
                }
            });
            client
        }
        None => {
            let (client, connection) = tokio_postgres::connect(&connect_url, NoTls)
                .await
                .map_err(db)?;
            tokio::spawn(async move {
                if let Err(error) = connection.await {
                    tracing::warn!(%error, "postgres control connection closed");
                }
            });
            client
        }
    };
    client
        .batch_execute("SET search_path TO control")
        .await
        .map_err(db)?;
    Ok(client)
}
