//! Database-authoritative Sandbox Service state.
//!
//! The backend owns durable inventory, leases, provider generations, work
//! queues and runtime handles. The service keeps only bounded live channels
//! (exec output, transfer rendezvous) and asks this module for every durable
//! decision. SQLite is the single-node default; Postgres serves multi-replica
//! deployments. Both implement the same [`SandboxBackend`] contract.

mod backend;
mod engine;
mod logic;
mod postgres;
mod seal;
mod sqlite;
mod store;
mod types;

pub(crate) mod commands_leases;
pub(crate) mod commands_providers;
pub(crate) mod commands_runtime;
pub(crate) mod commands_templates;
pub(crate) mod rows;

pub use backend::{ExecStatus, SandboxBackend, SchemaInfo, SnapshotCommand, Waiter};
pub use types::*;

use std::sync::Arc;

use crate::error::{Error, Result};

/// Open a backend from a store URL.
///
/// `postgres://` and `postgresql://` select Postgres; `sqlite://<path>`, a
/// bare path, and `memory:` (a real SQLite `:memory:` database) select SQLite.
pub async fn open(url: &str) -> Result<Arc<dyn SandboxBackend>> {
    let config = BackendConfig {
        url: url.to_owned(),
        state_dir: std::env::temp_dir().join("preloop-sandbox"),
        seal_key: None,
        pool_size: 8,
        node_id: default_node_id(),
        session_id: uuid::Uuid::new_v4().simple().to_string(),
        node_credential_sha256: String::new(),
        peer_url: String::new(),
        node_lost_after: std::time::Duration::from_secs(30),
    };
    open_with(config).await
}

/// Open a backend with explicit configuration.
pub async fn open_with(config: BackendConfig) -> Result<Arc<dyn SandboxBackend>> {
    store::SqlStore::open(config).await
}

/// Open a backend without registering this process as a node (embedded
/// callers and tests that do not serve live channels).
pub async fn open_embedded(config: BackendConfig) -> Result<Arc<dyn SandboxBackend>> {
    store::SqlStore::open_embedded(config).await
}

fn default_node_id() -> String {
    hostname::get()
        .ok()
        .and_then(|name| name.into_string().ok())
        .unwrap_or_else(|| "sandbox-node".to_owned())
}

/// Parse a store URL into a backend family.
pub(crate) fn backend_kind(url: &str) -> &'static str {
    let trimmed = url.trim();
    if trimmed.starts_with("postgres://") || trimmed.starts_with("postgresql://") {
        "postgres"
    } else if trimmed.starts_with("sqlite:")
        || trimmed == "memory:"
        || !trimmed.contains("://")
    {
        "sqlite"
    } else {
        "unsupported"
    }
}

/// Validate a store URL, returning a message for an unsupported scheme.
pub(crate) fn validate_url(url: &str) -> Result<()> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return Err(Error::invalid_argument("sandbox store URL is empty"));
    }
    if backend_kind(trimmed) == "unsupported" {
        return Err(Error::invalid_argument(format!(
            "unsupported sandbox store URL {trimmed:?}: expected postgres://, sqlite://, a bare path, or memory:"
        )));
    }
    Ok(())
}
