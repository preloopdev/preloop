//! Database-authoritative control plane.
//!
//! The `ControlBackend` trait is the only surface the server sees for durable
//! control state: every mutation is one typed command with transactional
//! authority, idempotency and fencing. Handlers parse and map responses; they
//! never own state or a database transaction. SQLite is the default backend;
//! Postgres implements the same contract for shared-node deployments.
//!
//! Layout:
//! - [`types`]: backend-neutral domain types, command inputs/outputs, errors.
//! - [`txstate`]: the transaction-scoped working set (`TxState`) that mirrors
//!   the old `InnerState` scheduling fields.
//! - [`sched`]: the scheduling state machine ported to run on `TxState`.
//! - [`schema`]: the unified table families both backends implement.
//! - [`rows`]: backend-neutral row codecs for decomposed families (steps).
//! - [`sqlite`] / [`postgres`]: the two `ControlBackend` implementations.

// The submodules use `use super::*` to reach the crate prelude, matching how
// `runtime_scheduling.rs` (the module this replaces) sees the whole crate
// through lib.rs's glob imports. Re-exporting the crate root here gives them
// the same view without a per-file import list.
#[allow(unused_imports)]
pub(crate) use crate::*;

pub mod backend;
pub mod commands;
#[allow(dead_code)]
pub(crate) mod lite;
pub(crate) mod logic;
// New PostgreSQL backend (agreed schema); unused until cutover.
#[allow(dead_code)]
pub(crate) mod pg;
pub mod postgres;
pub mod rows;
pub mod sched;
pub mod schema;
pub mod sqlite;
#[cfg(test)]
mod tests;
pub(crate) mod txn_stats;
pub mod txstate;
pub mod types;
pub(crate) mod wake;

pub(crate) use backend::Backend;
pub(crate) use types::*;
