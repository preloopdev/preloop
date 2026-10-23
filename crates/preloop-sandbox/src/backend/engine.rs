//! A tiny portable SQL surface shared by the SQLite and Postgres backends.
//!
//! Commands are written once against [`SqlTx`]; the engines differ only in
//! connection management, placeholder syntax (`$1` rewritten to `?1` for
//! SQLite), locking clauses and DDL. Rows are decoded by column name.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::error::{Error, Result};

/// A SQL parameter.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    /// SQL NULL.
    Null,
    /// An integer.
    Int(i64),
    /// A boolean.
    Bool(bool),
    /// UTF-8 text.
    Text(String),
    /// Opaque bytes.
    Bytes(Vec<u8>),
}

impl From<i64> for SqlValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<u64> for SqlValue {
    fn from(value: u64) -> Self {
        Self::Int(i64::try_from(value).unwrap_or(i64::MAX))
    }
}

impl From<bool> for SqlValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<&str> for SqlValue {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<String> for SqlValue {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<Vec<u8>> for SqlValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

impl<T: Into<SqlValue>> From<Option<T>> for SqlValue {
    fn from(value: Option<T>) -> Self {
        value.map_or(Self::Null, Into::into)
    }
}

/// One decoded row.
#[derive(Debug, Clone, Default)]
pub struct Row {
    values: HashMap<String, SqlValue>,
}

impl Row {
    /// Build from decoded column pairs.
    pub fn new(values: HashMap<String, SqlValue>) -> Self {
        Self { values }
    }

    /// A required integer column.
    pub fn int(&self, name: &str) -> Result<i64> {
        match self.values.get(name) {
            Some(SqlValue::Int(value)) => Ok(*value),
            Some(SqlValue::Bool(value)) => Ok(i64::from(*value)),
            Some(other) => Err(decode_error(name, other)),
            None => Err(decode_error(name, &SqlValue::Null)),
        }
    }

    /// A required unsigned integer column.
    pub fn u64(&self, name: &str) -> Result<u64> {
        Ok(u64::try_from(self.int(name)?).unwrap_or_default())
    }

    /// A required boolean column (0/1).
    pub fn bool(&self, name: &str) -> Result<bool> {
        match self.values.get(name) {
            Some(SqlValue::Bool(value)) => Ok(*value),
            Some(SqlValue::Int(value)) => Ok(*value != 0),
            Some(other) => Err(decode_error(name, other)),
            None => Err(decode_error(name, &SqlValue::Null)),
        }
    }

    /// An optional integer column.
    pub fn opt_int(&self, name: &str) -> Result<Option<i64>> {
        match self.values.get(name) {
            Some(SqlValue::Int(value)) => Ok(Some(*value)),
            Some(SqlValue::Bool(value)) => Ok(Some(i64::from(*value))),
            Some(SqlValue::Null) | None => Ok(None),
            Some(other) => Err(decode_error(name, other)),
        }
    }

    /// A required text column.
    pub fn text(&self, name: &str) -> Result<&str> {
        match self.values.get(name) {
            Some(SqlValue::Text(value)) => Ok(value),
            Some(other) => Err(decode_error(name, other)),
            None => Err(decode_error(name, &SqlValue::Null)),
        }
    }

    /// An optional text column.
    pub fn opt_text(&self, name: &str) -> Result<Option<&str>> {
        match self.values.get(name) {
            Some(SqlValue::Text(value)) => Ok(Some(value)),
            Some(SqlValue::Null) | None => Ok(None),
            Some(other) => Err(decode_error(name, other)),
        }
    }

    /// An optional byte column.
    pub fn opt_bytes(&self, name: &str) -> Result<Option<&[u8]>> {
        match self.values.get(name) {
            Some(SqlValue::Bytes(value)) => Ok(Some(value)),
            Some(SqlValue::Null) | None => Ok(None),
            Some(other) => Err(decode_error(name, other)),
        }
    }
}

fn decode_error(name: &str, value: &SqlValue) -> Error {
    Error::internal(format!(
        "sandbox store: column `{name}` has unexpected value {value:?}"
    ))
}

/// Which SQL dialect an engine speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    /// SQLite (single writer, `BEGIN IMMEDIATE`).
    Sqlite,
    /// Postgres (concurrent transactions, `SKIP LOCKED`).
    Postgres,
}

/// One transaction. Commands commit or roll back explicitly; a dropped
/// transaction never leaves a dirty connection in the pool.
#[async_trait]
pub trait SqlTx: Send {
    /// Execute a statement and return affected rows.
    async fn execute(&mut self, sql: &str, params: &[SqlValue]) -> Result<u64>;
    /// Query rows.
    async fn query(&mut self, sql: &str, params: &[SqlValue]) -> Result<Vec<Row>>;
    /// Commit and release the connection.
    async fn commit(self: Box<Self>) -> Result<()>;
    /// Roll back and release the connection.
    async fn rollback(self: Box<Self>) -> Result<()>;
    /// Dialect of this transaction.
    fn dialect(&self) -> Dialect;
}

/// A SQL engine.
#[async_trait]
pub trait SqlEngine: Send + Sync {
    /// Begin one transaction.
    async fn begin(&self) -> Result<Box<dyn SqlTx>>;
    /// Stop background listeners and close pooled connections.
    async fn close(&self) {}
}

/// Rewrite canonical `$N` placeholders to SQLite's `?N`.
pub(crate) fn sqlite_placeholders(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '$' {
            let mut digits = String::new();
            while chars.peek().is_some_and(|next| next.is_ascii_digit()) {
                digits.push(chars.next().expect("peeked"));
            }
            if digits.is_empty() {
                out.push(ch);
            } else {
                out.push('?');
                out.push_str(&digits);
            }
        } else {
            out.push(ch);
        }
    }
    out
}
