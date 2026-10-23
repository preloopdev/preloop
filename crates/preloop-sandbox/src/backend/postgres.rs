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

use super::engine::{Dialect, RetryableTxConflict, Row, SqlEngine, SqlTx, SqlValue};

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
    ///
    /// Replicas may all start against an empty database at once, so the DDL
    /// runs inside one transaction-scoped advisory lock: the losers wait for
    /// the first replica's tables instead of racing `CREATE TABLE IF NOT
    /// EXISTS`, which can still fail on the catalog's unique index when two
    /// sessions create the same relation concurrently. The lock is keyed on
    /// the sandbox schema name space-wide, not per replica.
    pub(crate) async fn apply_schema(&self, schema: &str) -> Result<u64> {
        let client = self.pool.connect().await?;
        client.batch_execute("BEGIN").await.map_err(map_error)?;
        if let Err(error) = client
            .batch_execute(
                "SELECT pg_advisory_xact_lock(hashtext('preloop_sandbox_schema')::bigint)",
            )
            .await
        {
            let _ = client.batch_execute("ROLLBACK").await;
            return Err(map_error(error));
        }
        if let Err(error) = client.batch_execute(schema).await {
            let _ = client.batch_execute("ROLLBACK").await;
            return Err(map_error(error));
        }
        client.batch_execute("COMMIT").await.map_err(map_error)?;
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
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::unavailable("sandbox Postgres backend is closed"));
        }
        let permit = self
            .pool
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::unavailable("sandbox Postgres pool is closed"))?;
        // An idle connection may have died while the store was unreachable
        // (a network cut, a server restart). Drop and replace it instead of
        // failing the caller: otherwise recovery costs one failed request per
        // stale socket in the pool.
        let mut stale = 0usize;
        loop {
            let pooled = self.pool.idle.lock().await.pop();
            let from_idle = pooled.is_some();
            let client = match pooled {
                Some(client) => client,
                None => self.pool.connect().await?,
            };
            if from_idle && client.is_closed() && stale < self.pool.max {
                stale += 1;
                continue;
            }
            match client.batch_execute("BEGIN").await {
                Ok(()) => {
                    return Ok(Box::new(PgTx {
                        client: Some(client),
                        pool: self.pool.clone(),
                        permit: Some(permit),
                        finished: false,
                    }));
                }
                Err(_) if from_idle && stale < self.pool.max => {
                    stale += 1;
                    continue;
                }
                Err(error) => return Err(map_error(error)),
            }
        }
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
        client
            .execute(sql, &refs)
            .await
            .map_err(|error| statement_error(error, sql))
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
        let rows = client
            .query(sql, &refs)
            .await
            .map_err(|error| statement_error(error, sql))?;
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
    Bool(bool),
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
            Self::Int(value) => match *ty {
                Type::INT2 => i16::try_from(*value).unwrap_or_default().to_sql(ty, out),
                Type::INT4 => i32::try_from(*value).unwrap_or_default().to_sql(ty, out),
                _ => value.to_sql(ty, out),
            },
            Self::Bool(value) => value.to_sql(ty, out),
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
            SqlValue::Bool(value) => PgParam::Bool(*value),
            SqlValue::Text(value) => PgParam::Text(value.clone()),
            SqlValue::Bytes(value) => PgParam::Bytes(value.clone()),
        })
        .collect()
}

fn decode_row(row: PgRow) -> Result<Row> {
    let mut values = std::collections::HashMap::new();
    for (index, column) in row.columns().iter().enumerate() {
        let name = column.name().to_owned();
        let column_type = column.type_().clone();
        let value = match *column.type_() {
            Type::INT2 => row
                .try_get::<_, Option<i16>>(index)
                .map_err(|error| {
                    Error::internal(format!(
                        "decoding column `{name}` type {column_type:?}: {error}"
                    ))
                })?
                .map_or(SqlValue::Null, |value| SqlValue::Int(i64::from(value))),
            Type::INT4 => row
                .try_get::<_, Option<i32>>(index)
                .map_err(|error| {
                    Error::internal(format!(
                        "decoding column `{name}` type {column_type:?}: {error}"
                    ))
                })?
                .map_or(SqlValue::Null, |value| SqlValue::Int(i64::from(value))),
            Type::INT8 => row
                .try_get::<_, Option<i64>>(index)
                .map_err(|error| {
                    Error::internal(format!(
                        "decoding column `{name}` type {column_type:?}: {error}"
                    ))
                })?
                .map_or(SqlValue::Null, SqlValue::Int),
            Type::BOOL => row
                .try_get::<_, Option<bool>>(index)
                .map_err(|error| {
                    Error::internal(format!(
                        "decoding column `{name}` type {column_type:?}: {error}"
                    ))
                })?
                .map_or(SqlValue::Null, SqlValue::Bool),
            Type::BYTEA => row
                .try_get::<_, Option<Vec<u8>>>(index)
                .map_err(|error| {
                    Error::internal(format!(
                        "decoding column `{name}` type {column_type:?}: {error}"
                    ))
                })?
                .map_or(SqlValue::Null, SqlValue::Bytes),
            _ => row
                .try_get::<_, Option<String>>(index)
                .map_err(|error| {
                    Error::internal(format!(
                        "decoding column `{name}` type {column_type:?}: {error}"
                    ))
                })?
                .map_or(SqlValue::Null, SqlValue::Text),
        };
        values.insert(name.clone(), value);
        if std::env::var("PRELOOP_DEBUG_PG").is_ok() {
            eprintln!("pg column {index} {name} {column_type:?}");
        }
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
        roots
            .add(certificate)
            .map_err(|error| Error::internal(format!("sandbox Postgres root: {error}")))?;
    }
    let mut config = crate::tls::client_config_builder()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    postgres_rustls::set_postgresql_alpn(&mut config);
    Ok(Some(postgres_rustls::MakeTlsConnector::new(
        tokio_rustls::TlsConnector::from(Arc::new(config)),
    )))
}

/// Attach the failing SQL to a statement error so an operator can see which
/// command lost the race, keeping the mapped error's retry marker: the
/// transaction wrapper decides retries from the source, never from the text.
fn statement_error(error: tokio_postgres::Error, sql: &str) -> Error {
    let mapped = map_error(error);
    let message = format!(
        "{} [sql: {sql}]",
        mapped.message.as_deref().unwrap_or_default()
    );
    match mapped.source_arc() {
        Some(source) => Error::internal(message).with_shared_source(source),
        None => Error::internal(message),
    }
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
            return Error::unavailable("sandbox Postgres serialization failure")
                .with_shared_source(Arc::new(RetryableTxConflict));
        }
        return Error::internal(format!(
            "sandbox Postgres ({}): {}",
            db_error.code().code(),
            db_error.message()
        ));
    }
    Error::internal(format!("sandbox Postgres: {error}"))
}
