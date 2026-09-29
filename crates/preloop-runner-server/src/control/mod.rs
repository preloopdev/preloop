//! Database-authoritative control plane.
//!
//! The `ControlBackend` trait is the only surface the server sees for durable
//! control state: every mutation is one typed command with transactional
//! authority, idempotency and fencing. Handlers parse and map responses; they
//! never own state or a database transaction. SQLite is the default backend;
//! Postgres implements the same contract for shared-node deployments.
//!
//! Layout:
//! - [`backend`]: the `ControlBackend` trait itself — one documented contract
//!   per command.
//! - [`types`]: backend-neutral domain types, command inputs/outputs, errors.
//! - [`lite`] / [`pg`]: the two `ControlBackend` implementations (SQLite is
//!   the default; Postgres serves shared-node deployments).
//! - [`logic`]: pure decision functions both backends share.
//! - [`wake`]: how a committed enqueue reaches waiting long-polls.
//! - [`txn_stats`]: per-phase control-transaction timings for operators.
//! - `tests` / `testview`: the shared behavioral suite and its test-only
//!   read view of the control database.

// The submodules use `use super::*` to reach the crate prelude, matching how
// `runtime_scheduling.rs` (the module this replaces) sees the whole crate
// through lib.rs's glob imports. Re-exporting the crate root here gives them
// the same view without a per-file import list.
#[allow(unused_imports)]
pub(crate) use crate::*;

pub mod backend;
pub(crate) mod lite;
pub(crate) mod logic;
pub(crate) mod pg;
#[cfg(test)]
mod tests;
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod testview;
pub(crate) mod txn_stats;
pub mod types;
pub(crate) mod wake;

pub(crate) use backend::Backend;
pub(crate) use types::*;
