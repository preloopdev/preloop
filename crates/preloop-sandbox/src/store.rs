//! Durable sandbox state: SQLite or Postgres, separate from the control schema.
//!
//! The service keeps its working set in memory and writes every change
//! through; the database is the restart source (the repo-wide convention).
//! Records are stored as JSON documents keyed by id, one table per kind,
//! so both backends share one encoding and the schema never needs to track
//! wire-type field additions. Postgres tables live in their own `sandbox`
//! schema; SQLite uses its own file. Neither references control tables.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::error::{Error, Result};

/// Record kinds (one table each).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    /// Templates by name.
    Templates,
    /// Pools by name.
    Pools,
    /// Sandboxes by id.
    Sandboxes,
    /// Leases by id.
    Leases,
    /// Providers by id.
    Providers,
    /// Single-use join tokens by id.
    JoinTokens,
}

impl Kind {
    /// Every kind.
    pub const ALL: [Kind; 6] = [
        Kind::Templates,
        Kind::Pools,
        Kind::Sandboxes,
        Kind::Leases,
        Kind::Providers,
        Kind::JoinTokens,
    ];

    fn table(self) -> &'static str {
        match self {
            Kind::Templates => "templates",
            Kind::Pools => "pools",
            Kind::Sandboxes => "sandboxes",
            Kind::Leases => "leases",
            Kind::Providers => "providers",
            Kind::JoinTokens => "join_tokens",
        }
    }
}

/// Storage backend.
#[async_trait]
pub trait Store: Send + Sync {
    /// Insert or replace a record.
    async fn put(&self, kind: Kind, id: &str, value: &Value) -> Result<()>;
    /// Delete a record (missing is fine).
    async fn delete(&self, kind: Kind, id: &str) -> Result<()>;
    /// Load every record of a kind.
    async fn load(&self, kind: Kind) -> Result<Vec<Value>>;
}

fn db_error(error: impl std::fmt::Display) -> Error {
    Error::internal(format!("sandbox store: {error}"))
}

/// Open a store from a URL: `postgres://…`, `sqlite://<path>`, a bare path,
/// or `memory:` (tests).
pub async fn open(url: &str) -> Result<Arc<dyn Store>> {
    if url == "memory:" {
        return Ok(Arc::new(MemoryStore::default()));
    }
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        return Ok(Arc::new(PgStore::open(url).await?));
    }
    let path = url.strip_prefix("sqlite://").unwrap_or(url);
    Ok(Arc::new(SqliteStore::open(Path::new(path))?))
}

/// In-memory store.
#[derive(Default)]
pub struct MemoryStore {
    rows: parking_lot::Mutex<std::collections::BTreeMap<(&'static str, String), Value>>,
}

#[async_trait]
impl Store for MemoryStore {
    async fn put(&self, kind: Kind, id: &str, value: &Value) -> Result<()> {
        self.rows
            .lock()
            .insert((kind.table(), id.to_owned()), value.clone());
        Ok(())
    }
    async fn delete(&self, kind: Kind, id: &str) -> Result<()> {
        self.rows.lock().remove(&(kind.table(), id.to_owned()));
        Ok(())
    }
    async fn load(&self, kind: Kind) -> Result<Vec<Value>> {
        Ok(self
            .rows
            .lock()
            .iter()
            .filter(|((table, _), _)| *table == kind.table())
            .map(|(_, value)| value.clone())
            .collect())
    }
}

/// SQLite schema version (PRAGMA user_version).
const SQLITE_MIGRATIONS: &[&str] = &[
    "CREATE TABLE templates (id TEXT PRIMARY KEY, doc TEXT NOT NULL);
     CREATE TABLE pools (id TEXT PRIMARY KEY, doc TEXT NOT NULL);
     CREATE TABLE sandboxes (id TEXT PRIMARY KEY, doc TEXT NOT NULL);
     CREATE TABLE leases (id TEXT PRIMARY KEY, doc TEXT NOT NULL);
     CREATE TABLE providers (id TEXT PRIMARY KEY, doc TEXT NOT NULL);",
    "CREATE TABLE join_tokens (id TEXT PRIMARY KEY, doc TEXT NOT NULL);",
];

/// SQLite store in its own file (default `<state>/sandbox.db`).
pub struct SqliteStore {
    connection: Arc<parking_lot::Mutex<rusqlite::Connection>>,
    #[allow(dead_code)]
    path: PathBuf,
}

impl SqliteStore {
    /// Open (creating) and migrate.
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).map_err(db_error)?;
        }
        let connection = rusqlite::Connection::open(path).map_err(db_error)?;
        connection
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
            .map_err(db_error)?;
        let version: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(db_error)?;
        for (index, migration) in SQLITE_MIGRATIONS.iter().enumerate().skip(version as usize) {
            connection
                .execute_batch(&format!(
                    "BEGIN; {migration} PRAGMA user_version = {}; COMMIT;",
                    index + 1
                ))
                .map_err(db_error)?;
        }
        Ok(Self {
            connection: Arc::new(parking_lot::Mutex::new(connection)),
            path: path.to_path_buf(),
        })
    }

    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&rusqlite::Connection) -> rusqlite::Result<T> + Send + 'static,
    ) -> Result<T> {
        let connection = self.connection.clone();
        tokio::task::spawn_blocking(move || f(&connection.lock()))
            .await
            .map_err(db_error)?
            .map_err(db_error)
    }
}

#[async_trait]
impl Store for SqliteStore {
    async fn put(&self, kind: Kind, id: &str, value: &Value) -> Result<()> {
        let sql = format!(
            "INSERT INTO {} (id, doc) VALUES (?1, ?2) ON CONFLICT(id) DO UPDATE SET doc = excluded.doc",
            kind.table()
        );
        let (id, doc) = (id.to_owned(), value.to_string());
        self.blocking(move |c| c.execute(&sql, rusqlite::params![id, doc]).map(|_| ()))
            .await
    }
    async fn delete(&self, kind: Kind, id: &str) -> Result<()> {
        let sql = format!("DELETE FROM {} WHERE id = ?1", kind.table());
        let id = id.to_owned();
        self.blocking(move |c| c.execute(&sql, [id]).map(|_| ()))
            .await
    }
    async fn load(&self, kind: Kind) -> Result<Vec<Value>> {
        let sql = format!("SELECT doc FROM {} ORDER BY id", kind.table());
        let docs: Vec<String> = self
            .blocking(move |c| {
                let mut statement = c.prepare(&sql)?;
                let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
                rows.collect()
            })
            .await?;
        docs.iter()
            .map(|doc| serde_json::from_str(doc).map_err(db_error))
            .collect()
    }
}

/// Postgres migrations; `sandbox.schema_migrations` tracks the version.
const PG_MIGRATIONS: &[&str] = &[
    "CREATE TABLE sandbox.templates (id text PRIMARY KEY, doc jsonb NOT NULL);
     CREATE TABLE sandbox.pools (id text PRIMARY KEY, doc jsonb NOT NULL);
     CREATE TABLE sandbox.sandboxes (id text PRIMARY KEY, doc jsonb NOT NULL);
     CREATE TABLE sandbox.leases (id text PRIMARY KEY, doc jsonb NOT NULL);
     CREATE TABLE sandbox.providers (id text PRIMARY KEY, doc jsonb NOT NULL);",
    "CREATE TABLE sandbox.join_tokens (id text PRIMARY KEY, doc jsonb NOT NULL);",
];

/// Postgres store in the `sandbox` schema.
pub struct PgStore {
    client: tokio::sync::Mutex<tokio_postgres::Client>,
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
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().certs {
        roots.add(cert).map_err(db_error)?;
    }
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    postgres_rustls::set_postgresql_alpn(&mut config);
    Ok(Some(postgres_rustls::MakeTlsConnector::new(
        tokio_rustls::TlsConnector::from(Arc::new(config)),
    )))
}

impl PgStore {
    /// Connect and migrate.
    pub async fn open(url: &str) -> Result<Self> {
        // tokio-postgres rejects libpq's verify-* modes; rustls always
        // verifies chain and hostname, so `require` loses nothing.
        let connect_url = url
            .replace("sslmode=verify-full", "sslmode=require")
            .replace("sslmode=verify-ca", "sslmode=require");
        let client = match pg_tls(url)? {
            Some(tls) => {
                let (client, connection) = tokio_postgres::connect(&connect_url, tls)
                    .await
                    .map_err(db_error)?;
                tokio::spawn(connection);
                client
            }
            None => {
                let (client, connection) =
                    tokio_postgres::connect(&connect_url, tokio_postgres::NoTls)
                        .await
                        .map_err(db_error)?;
                tokio::spawn(connection);
                client
            }
        };
        client
            .batch_execute(
                "CREATE SCHEMA IF NOT EXISTS sandbox;
                 CREATE TABLE IF NOT EXISTS sandbox.schema_migrations (version integer PRIMARY KEY);",
            )
            .await
            .map_err(db_error)?;
        let applied: i32 = client
            .query_one(
                "SELECT COALESCE(MAX(version), 0) FROM sandbox.schema_migrations",
                &[],
            )
            .await
            .map_err(db_error)?
            .get(0);
        for (index, migration) in PG_MIGRATIONS.iter().enumerate().skip(applied as usize) {
            let version = index as i32 + 1;
            client
                .batch_execute(&format!(
                    "BEGIN; {migration} INSERT INTO sandbox.schema_migrations VALUES ({version}); COMMIT;"
                ))
                .await
                .map_err(db_error)?;
        }
        Ok(Self {
            client: tokio::sync::Mutex::new(client),
        })
    }
}

#[async_trait]
impl Store for PgStore {
    async fn put(&self, kind: Kind, id: &str, value: &Value) -> Result<()> {
        let sql = format!(
            "INSERT INTO sandbox.{} (id, doc) VALUES ($1, $2::text::jsonb) \
             ON CONFLICT (id) DO UPDATE SET doc = excluded.doc",
            kind.table()
        );
        self.client
            .lock()
            .await
            .execute(&sql, &[&id, &value.to_string()])
            .await
            .map(|_| ())
            .map_err(db_error)
    }
    async fn delete(&self, kind: Kind, id: &str) -> Result<()> {
        let sql = format!("DELETE FROM sandbox.{} WHERE id = $1", kind.table());
        self.client
            .lock()
            .await
            .execute(&sql, &[&id])
            .await
            .map(|_| ())
            .map_err(db_error)
    }
    async fn load(&self, kind: Kind) -> Result<Vec<Value>> {
        let sql = format!("SELECT doc::text FROM sandbox.{} ORDER BY id", kind.table());
        let rows = self
            .client
            .lock()
            .await
            .query(&sql, &[])
            .await
            .map_err(db_error)?;
        rows.iter()
            .map(|row| serde_json::from_str(row.get::<_, &str>(0)).map_err(db_error))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn exercise(store: &dyn Store) {
        store
            .put(
                Kind::Pools,
                "p1",
                &serde_json::json!({"name": "p1", "warm": 1}),
            )
            .await
            .unwrap();
        store
            .put(
                Kind::Pools,
                "p1",
                &serde_json::json!({"name": "p1", "warm": 3}),
            )
            .await
            .unwrap();
        store
            .put(Kind::Leases, "l1", &serde_json::json!({"id": "l1"}))
            .await
            .unwrap();
        let pools = store.load(Kind::Pools).await.unwrap();
        assert_eq!(pools, vec![serde_json::json!({"name": "p1", "warm": 3})]);
        store.delete(Kind::Pools, "p1").await.unwrap();
        store.delete(Kind::Pools, "missing").await.unwrap();
        assert!(store.load(Kind::Pools).await.unwrap().is_empty());
        assert_eq!(store.load(Kind::Leases).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn sqlite_upserts_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sandbox.db");
        exercise(&SqliteStore::open(&path).unwrap()).await;
        let reopened = SqliteStore::open(&path).unwrap();
        assert_eq!(reopened.load(Kind::Leases).await.unwrap().len(), 1);
    }

    /// Runs against a real server when `PRELOOP_SANDBOX_TEST_PG` is set.
    #[tokio::test]
    async fn postgres_uses_its_own_schema() {
        let Ok(url) = std::env::var("PRELOOP_SANDBOX_TEST_PG") else {
            return;
        };
        let store = PgStore::open(&url).await.unwrap();
        for kind in Kind::ALL {
            for row in store.load(kind).await.unwrap() {
                let id = row["id"]
                    .as_str()
                    .or(row["name"].as_str())
                    .unwrap_or_default()
                    .to_owned();
                store.delete(kind, &id).await.unwrap();
            }
        }
        store.delete(Kind::Leases, "l1").await.unwrap();
        exercise(&store).await;
        // Reopen: migrations are idempotent.
        let again = PgStore::open(&url).await.unwrap();
        assert_eq!(again.load(Kind::Leases).await.unwrap().len(), 1);
        again.delete(Kind::Leases, "l1").await.unwrap();
    }
}
