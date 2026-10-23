//! SQLite engine: one connection, WAL, `synchronous=FULL`, `BEGIN IMMEDIATE`.
//!
//! SQLite has a single writer, so a transaction takes the one connection out
//! of the pool for its whole lifetime. rusqlite is blocking; statements run
//! under `block_in_place` on a multi-thread runtime (production) and inline on
//! a current-thread runtime (unit tests). A dropped transaction rolls back and
//! returns the connection, never leaving it dirty.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::Connection;
use rusqlite::types::Value as SqliteValue;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::{Code, Error, Result};

use super::engine::{Dialect, Row, SqlEngine, SqlTx, SqlValue, sqlite_placeholders};

/// SQLite connection wrapper.
pub(crate) struct SqliteEngine {
    connection: Arc<Mutex<Option<Connection>>>,
    permits: Arc<Semaphore>,
}

impl SqliteEngine {
    /// Open `path` (or `:memory:`) and configure durability.
    pub(crate) fn open(path: &str) -> Result<Self> {
        let connection = Connection::open(path).map_err(map_error)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(map_error)?;
        if path != ":memory:" {
            let _: String = connection
                .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
                .map_err(map_error)?;
        }
        connection
            .execute_batch("PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;")
            .map_err(map_error)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(Some(connection))),
            permits: Arc::new(Semaphore::new(1)),
        })
    }

    /// Apply the schema and return the stored version.
    pub(crate) async fn apply_schema(&self, schema: &str) -> Result<u64> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::internal("sandbox SQLite engine is closed"))?;
        let connection = self.take_connection()?;
        let schema = schema.to_owned();
        let result = run_blocking(move || {
            let existing: i64 = connection
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .map_err(map_error)?;
            if existing > 1 {
                return Err(Error::failed_precondition(format!(
                    "sandbox SQLite schema version {existing} is newer than this binary (1)"
                )));
            }
            connection.execute_batch(&schema).map_err(map_error)?;
            let version: i64 = connection
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .map_err(map_error)?;
            self.put_connection(connection);
            Ok(u64::try_from(version).unwrap_or_default())
        })
        .await;
        drop(permit);
        result
    }

    fn take_connection(&self) -> Result<Connection> {
        self.connection
            .lock()
            .expect("sandbox SQLite pool")
            .take()
            .ok_or_else(|| Error::internal("sandbox SQLite connection is busy"))
    }

    fn put_connection(&self, connection: Connection) {
        let mut slot = self.connection.lock().expect("sandbox SQLite pool");
        if slot.is_none() {
            *slot = Some(connection);
        }
    }

    async fn close(&self) {
        self.permits.close();
        let _ = self.connection.lock().expect("sandbox SQLite pool").take();
    }
}

#[async_trait]
impl SqlEngine for SqliteEngine {
    async fn begin(&self) -> Result<Box<dyn SqlTx>> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::unavailable("sandbox SQLite engine is closed"))?;
        let mut connection = self.take_connection()?;
        run_blocking(move || match connection.execute_batch("BEGIN IMMEDIATE") {
            Ok(()) => Ok(Box::new(SqliteTx {
                connection: Some(connection),
                pool: self.connection.clone(),
                permit: Some(permit),
            }) as Box<dyn SqlTx>),
            Err(error) => {
                self.put_connection(connection);
                Err(map_error(error))
            }
        })
        .await
    }

    fn dialect(&self) -> Dialect {
        Dialect::Sqlite
    }

    fn label(&self) -> &'static str {
        "sqlite"
    }

    async fn close(&self) {
        self.close().await;
    }
}

struct SqliteTx {
    connection: Option<Connection>,
    pool: Arc<Mutex<Option<Connection>>>,
    permit: Option<OwnedSemaphorePermit>,
}

impl SqliteTx {
    fn put_back(&mut self, connection: Connection) {
        let mut slot = self.pool.lock().expect("sandbox SQLite pool");
        if slot.is_none() {
            *slot = Some(connection);
        }
        drop(slot);
        self.permit.take();
    }
}

impl Drop for SqliteTx {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            let _ = connection.execute_batch("ROLLBACK");
            self.put_back(connection);
        }
    }
}

#[async_trait]
impl SqlTx for SqliteTx {
    async fn execute(&mut self, sql: &str, params: &[SqlValue]) -> Result<u64> {
        let connection = self
            .connection
            .as_mut()
            .ok_or_else(|| Error::internal("SQLite transaction is closed"))?;
        let sql = sqlite_placeholders(sql);
        let params = to_sqlite(params);
        run_blocking(move || {
            let mut statement = connection.prepare_cached(&sql).map_err(map_error)?;
            let changed = statement
                .execute(rusqlite::params_from_iter(params.iter()))
                .map_err(map_error)?;
            Ok(u64::try_from(changed).unwrap_or(u64::MAX))
        })
        .await
    }

    async fn query(&mut self, sql: &str, params: &[SqlValue]) -> Result<Vec<Row>> {
        let connection = self
            .connection
            .as_mut()
            .ok_or_else(|| Error::internal("SQLite transaction is closed"))?;
        let sql = sqlite_placeholders(sql);
        let params = to_sqlite(params);
        run_blocking(move || {
            let mut statement = connection.prepare_cached(&sql).map_err(map_error)?;
            let names: Vec<String> = statement
                .column_names()
                .into_iter()
                .map(str::to_owned)
                .collect();
            let mut rows = statement
                .query(rusqlite::params_from_iter(params.iter()))
                .map_err(map_error)?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().map_err(map_error)? {
                let mut values = std::collections::HashMap::with_capacity(names.len());
                for (index, name) in names.iter().enumerate() {
                    let value = row.get::<_, SqliteValue>(index).map_err(map_error)?;
                    values.insert(name.clone(), from_sqlite(value));
                }
                out.push(Row::new(values));
            }
            Ok(out)
        })
        .await
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        let connection = self
            .connection
            .take()
            .ok_or_else(|| Error::internal("SQLite transaction is closed"))?;
        let (connection, result) = run_blocking(move || {
            let result = connection.execute_batch("COMMIT").map_err(map_error);
            Ok((connection, result))
        })
        .await?;
        self.put_back(connection);
        result
    }

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        let connection = self
            .connection
            .take()
            .ok_or_else(|| Error::internal("SQLite transaction is closed"))?;
        let (connection, result) = run_blocking(move || {
            let result = connection.execute_batch("ROLLBACK").map_err(map_error);
            Ok((connection, result))
        })
        .await?;
        self.put_back(connection);
        result
    }

    fn dialect(&self) -> Dialect {
        Dialect::Sqlite
    }
}

async fn run_blocking<T, F>(f: F) -> Result<T>
where
    T: Send,
    F: FnOnce() -> Result<T>,
{
    if tokio::runtime::Handle::current().runtime_flavor()
        == tokio::runtime::RuntimeFlavor::MultiThread
    {
        tokio::task::block_in_place(f)
    } else {
        f()
    }
}

fn to_sqlite(params: &[SqlValue]) -> Vec<SqliteValue> {
    params
        .iter()
        .map(|value| match value {
            SqlValue::Null => SqliteValue::Null,
            SqlValue::Int(value) => SqliteValue::Integer(*value),
            SqlValue::Bool(value) => SqliteValue::Integer(i64::from(*value)),
            SqlValue::Text(value) => SqliteValue::Text(value.clone()),
            SqlValue::Bytes(value) => SqliteValue::Blob(value.clone()),
        })
        .collect()
}

fn from_sqlite(value: SqliteValue) -> SqlValue {
    match value {
        SqliteValue::Null => SqlValue::Null,
        SqliteValue::Integer(value) => SqlValue::Int(value),
        SqliteValue::Real(value) => SqlValue::Text(value.to_string()),
        SqliteValue::Text(value) => SqlValue::Text(value),
        SqliteValue::Blob(value) => SqlValue::Bytes(value),
    }
}

pub(crate) fn map_error(error: rusqlite::Error) -> Error {
    if let rusqlite::Error::SqliteFailure(code, message) = &error {
        if code.code == rusqlite::ErrorCode::ConstraintViolation {
            return Error::new(
                Code::AlreadyExists,
                format!(
                    "sandbox SQLite constraint: {}",
                    message.clone().unwrap_or_else(|| code.to_string())
                ),
            );
        }
        if code.code == rusqlite::ErrorCode::DatabaseBusy
            || code.code == rusqlite::ErrorCode::DatabaseLocked
        {
            return Error::unavailable("sandbox SQLite database is busy");
        }
    }
    Error::internal(format!("sandbox SQLite: {error}"))
}
