//! Postgres engine: a bounded connection pool and a persistent LISTEN
//! connection with reconnect/backoff.
//!
//! Every command runs in its own transaction on a checked-out connection, so
//! replicas make progress concurrently. `LISTEN sandbox_changed` is only a
//! wake hint: every waiter also rechecks SQL on a bounded timer, so a missed
//! notification costs latency, never correctness.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, NoTls, Row as PgRow};

use crate::error::{Code, Error, Result};

use super::engine::{Dialect, Row, SqlEngine, SqlTx, SqlValue};

/// Postgres channel used for optional wake hints.
pub(crate) const EVENTS_CHANNEL: &str = "sandbox_changed";

pub(crate) struct PgEngine {
    pool: Arc<PgPool>,
    listener: Mutex<Option<tokio::task::JoinHandle<()>>>,
    closed: std::sync::atomic::AtomicBool,
}

struct PgPool {
    url: String,
    tls: Option<postgres_rustls::MakeTlsConnector>,
    idle: Mutex<Vec<Arc<Client>>>,
    permits: Arc<Semaphore>,
    max: usize,
}

impl PgEngine {
    /// Connect and apply the schema.
    pub(crate) async fn open(url: &str, pool_size: usize) -> Result<Self> {
        let tls = pg_tls(url)?;
        let pool = Arc::new(PgPool {
            url: url.to_owned(),
            tls,
            idle: Mutex::new(Vec::new()),
            permits: Arc::new(Semaphore::new(pool_size.max(1))),
            max: pool_size.max(1),
        });
        let engine = Self {
            pool,
            listener: Mutex::new(None),
            closed: std::sync::atomic::AtomicBool::new(false),
        };
        Ok(engine)
    }

    /// Apply the schema on a dedicated connection.
    pub(crate) async fn apply_schema(&self, schema: &str) -> Result<u64> {
        let client = self.pool.connect().await?;
        client.batch_execute(schema).await.map_err(map_error)?;
        let row = client
            .query_one(
                "SELECT COALESCE((SELECT value FROM schema_meta WHERE key = 'schema_version'), '1')",
                &[],
            )
            .await
            .map_err(map_error)?;
        let value: String = row.get(0);
        value
            .parse::<u64>()
            .map_err(|_| Error::internal("sandbox Postgres schema_version is not an integer"))
    }

    /// Spawn the LISTEN loop; notifications only wake local waiters.
    pub(crate) async fn listen(&self, notify: Arc<Notify>) {
        if self.listener.lock().await.is_some() {
            return;
        }
        let pool = self.pool.clone();
        let handle = tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                match listen_once(&pool, &notify).await {
                    Ok(()) => backoff = Duration::from_secs(1),
                    Err(error) => {
                        tracing::warn!(%error, "sandbox Postgres LISTEN connection lost; reconnecting");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                    }
                }
            }
        });
        *self.listener.lock().await = Some(handle);
    }
}

#[async_trait]
impl SqlEngine for PgEngine {
    async fn begin(&self) -> Result<Box<dyn SqlTx>> {
        if self
            .closed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::unavailable("sandbox Postgres backend is closed"));
        }
        let permit = self
            .pool
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::unavailable("sandbox Postgres pool is closed"))?;
        let client = match self.pool.idle.lock().await.pop() {
            Some(client) => client,
            None => self.pool.connect().await?,
        };
        if let Err(error) = client.batch_execute("BEGIN").await {
            return Err(map_error(error));
        }
        Ok(Box::new(PgTx {
            client: Some(client),
            pool: self.pool.clone(),
            permit: Some(permit),
            finished: false,
        }))
    }

    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }

    fn label(&self) -> &'static str {
        "postgres"
    }

    async fn close(&self) {
        self.closed
            .store(true, std::sync::atomic::Ordering::Release);
        if let Some(handle) = self.listener.lock().await.take() {
            handle.abort();
        }
        self.pool.idle.lock().await.clear();
        self.pool.permits.close();
    }
}

impl PgPool {
    async fn connect(&self) -> Result<Arc<Client>> {
        let client = match &self.tls {
            Some(tls) => {
                let (client, connection) = tokio_postgres::connect(&self.url, tls.clone())
                    .await
                    .map_err(map_error)?;
                tokio::spawn(async move {
                    if let Err(error) = connection.await {
                        tracing::warn!(%error, "sandbox Postgres connection ended");
                    }
                });
                client
            }
            None => {
                let (client, connection) = tokio_postgres::connect(&self.url, NoTls)
                    .await
                    .map_err(map_error)?;
                tokio::spawn(async move {
                    if let Err(error) = connection.await {
                        tracing::warn!(%error, "sandbox Postgres connection ended");
                    }
                });
                client
            }
        };
        client
            .batch_execute("SET search_path TO sandbox")
            .await
            .map_err(map_error)?;
        Ok(Arc::new(client))
    }

    async fn put_back(&self, client: Arc<Client>) {
        let mut idle = self.idle.lock().await;
        if idle.len() < self.max {
            idle.push(client);
        }
    }
}

struct PgTx {
    client: Option<Arc<Client>>,
    pool: Arc<PgPool>,
    permit: Option<OwnedSemaphorePermit>,
    finished: bool,
}

#[async_trait]
impl SqlTx for PgTx {
    async fn execute(&mut self, sql: &str, params: &[SqlValue]) -> Result<u64> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::internal("Postgres transaction is closed"))?;
        let params = pg_params(params);
        let refs: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|value| value as &(dyn ToSql + Sync))
            .collect();
        client.execute(sql, &refs).await.map_err(map_error)
    }

    async fn query(&mut self, sql: &str, params: &[SqlValue]) -> Result<Vec<Row>> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::internal("Postgres transaction is closed"))?;
        let params = pg_params(params);
        let refs: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|value| value as &(dyn ToSql + Sync))
            .collect();
        let rows = client.query(sql, &refs).await.map_err(map_error)?;
        rows.into_iter().map(decode_row).collect()
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        let client = self
            .client
            .take()
            .ok_or_else(|| Error::internal("Postgres transaction is closed"))?;
        self.finished = true;
        client.batch_execute("COMMIT").await.map_err(map_error)?;
        self.pool.put_back(client).await;
        self.permit.take();
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        let client = self
            .client
            .take()
            .ok_or_else(|| Error::internal("Postgres transaction is closed"))?;
        self.finished = true;
        client.batch_execute("ROLLBACK").await.map_err(map_error)?;
        self.pool.put_back(client).await;
        self.permit.take();
        Ok(())
    }

    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }
}

impl Drop for PgTx {
    fn drop(&mut self) {
        // A cancelled command must not return a dirty connection to the pool.
        // Dropping the last Arc closes the session; the pool opens a fresh one
        // on the next begin.
        self.permit.take();
        self.client.take();
    }
}

#[derive(Debug)]
enum PgParam {
    Null,
    Int(i64),
    Text(String),
    Bytes(Vec<u8>),
}

impl ToSql for PgParam {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> std::result::Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    {
        match self {
            Self::Null => Ok(tokio_postgres::types::IsNull::Yes),
            Self::Int(value) => value.to_sql(ty, out),
            Self::Text(value) => value.to_sql(ty, out),
            Self::Bytes(value) => value.to_sql(ty, out),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> std::result::Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    {
        self.to_sql(ty, out)
    }
}

fn pg_params(params: &[SqlValue]) -> Vec<PgParam> {
    params
        .iter()
        .map(|value| match value {
            SqlValue::Null => PgParam::Null,
            SqlValue::Int(value) => PgParam::Int(*value),
            SqlValue::Text(value) => PgParam::Text(value.clone()),
            SqlValue::Bytes(value) => PgParam::Bytes(value.clone()),
        })
        .collect()
}

fn decode_row(row: PgRow) -> Result<Row> {
    let mut values = std::collections::HashMap::new();
    for (index, column) in row.columns().iter().enumerate() {
        let name = column.name().to_owned();
        let value = match *column.type_() {
            Type::INT2 | Type::INT4 | Type::INT8 => row
                .try_get::<_, Option<i64>>(index)
                .map_err(map_error)?
                .map_or(SqlValue::Null, SqlValue::Int),
            Type::BOOL => row
                .try_get::<_, Option<bool>>(index)
                .map_err(map_error)?
                .map_or(SqlValue::Null, |value| SqlValue::Int(i64::from(value))),
            Type::BYTEA => row
                .try_get::<_, Option<Vec<u8>>>(index)
                .map_err(map_error)?
                .map_or(SqlValue::Null, SqlValue::Bytes),
            _ => row
                .try_get::<_, Option<String>>(index)
                .map_err(map_error)?
                .map_or(SqlValue::Null, SqlValue::Text),
        };
        values.insert(name, value);
    }
    Ok(Row::new(values))
}

async fn listen_once(pool: &PgPool, notify: &Arc<Notify>) -> Result<()> {
    match &pool.tls {
        Some(tls) => {
            let (client, connection) = tokio_postgres::connect(&pool.url, tls.clone())
                .await
                .map_err(map_error)?;
            client
                .batch_execute(&format!("LISTEN {EVENTS_CHANNEL}"))
                .await
                .map_err(map_error)?;
            drive_notifications(client, connection, notify.clone()).await
        }
        None => {
            let (client, connection) = tokio_postgres::connect(&pool.url, NoTls)
                .await
                .map_err(map_error)?;
            client
                .batch_execute(&format!("LISTEN {EVENTS_CHANNEL}"))
                .await
                .map_err(map_error)?;
            drive_notifications(client, connection, notify.clone()).await
        }
    }
}

async fn drive_notifications<S, T>(
    _client: Client,
    mut connection: tokio_postgres::Connection<S, T>,
    notify: Arc<Notify>,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut connection = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
    while let Some(message) = futures::StreamExt::next(&mut connection).await {
        match message.map_err(map_error)? {
            tokio_postgres::AsyncMessage::Notification(notification) => {
                tracing::trace!(channel = %notification.channel(), "sandbox wake hint");
                notify.notify_waiters();
            }
            tokio_postgres::AsyncMessage::Notice(notice) => {
                tracing::debug!(%notice, "sandbox Postgres notice");
            }
            _ => {}
        }
    }
    Err(Error::unavailable("sandbox Postgres LISTEN stream ended"))
}

fn pg_tls(url: &str) -> Result<Option<postgres_rustls::MakeTlsConnector>> {
    let query = url.split('?').nth(1).unwrap_or("");
    let secure = query
        .split('&')
        .filter_map(|part| part.strip_prefix("sslmode="))
        .any(|mode| matches!(mode, "require" | "verify-ca" | "verify-full"));
    if !secure {
        return Ok(None);
    }
    let mut roots = rustls::RootCertStore::empty();
    for certificate in rustls_native_certs::load_native_certs().certs {
        roots.add(certificate).map_err(|error| Error::internal(format!("sandbox Postgres root: {error}")))?;
    }
    let mut config = crate::tls::client_config_builder()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    postgres_rustls::set_postgresql_alpn(&mut config);
    Ok(Some(postgres_rustls::MakeTlsConnector::new(
        tokio_rustls::TlsConnector::from(Arc::new(config)),
    )))
}

pub(crate) fn map_error(error: tokio_postgres::Error) -> Error {
    if let Some(db_error) = error.as_db_error() {
        if db_error.code().code() == "23505" {
            return Error::new(
                Code::AlreadyExists,
                format!("sandbox Postgres constraint: {}", db_error.message()),
            );
        }
        if db_error.code().code() == "40001" || db_error.code().code() == "40P01" {
            return Error::unavailable("sandbox Postgres serialization failure");
        }
    }
    Error::internal(format!("sandbox Postgres: {error}"))
}
