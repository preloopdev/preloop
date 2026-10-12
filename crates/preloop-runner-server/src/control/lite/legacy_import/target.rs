//! Target-database preparation for the importer.
//!
//! The target is created through the embedded migrations and their ledger —
//! `control::migrate_runner::initialize_empty_sqlite`, the same initializer
//! `preloop store migrate` and the test-support backends use — so an
//! imported store is an ordinary ledgered control database and the migration
//! ledger, not `schema_meta`, is its version authority.
//!
//! The importer never migrates a non-fresh database: an existing control
//! schema (or any foreign table) is a refusal, not an upgrade.

use anyhow::{Context, bail};
use rusqlite::Connection;

/// Connection settings the importer's own connection needs (the import is a
/// sequence of raw statements, not a `LiteBackend`).
pub(crate) fn configure(conn: &mut Connection) -> anyhow::Result<()> {
    conn.pragma_update(None, "foreign_keys", true)
        .context("enable foreign keys on the staging database")?;
    conn.pragma_update(None, "busy_timeout", 5000_i64)
        .context("set busy_timeout on the staging database")?;
    Ok(())
}

/// Create the control schema in a brand-new database file.
pub(crate) fn initialize_fresh_target(conn: &mut Connection) -> anyhow::Result<()> {
    crate::control::migrate_runner::initialize_empty_sqlite(conn)
        .context("initialize the staging database")
}

/// Verify the target carries exactly the control schema this build ships.
///
/// The migration ledger is the version authority, exactly as at serve time
/// (a stale pre-ledger `schema_meta.schema_version` row is ignored there
/// too), and every table the importer writes into must exist.
pub(crate) fn verify_target_ledger(conn: &Connection) -> anyhow::Result<()> {
    let ledger = crate::control::migrations::sqlite_ledger(conn)
        .context("read the staging database's migration ledger")?;
    if let Err(error) = crate::control::migrations::check_ledger(ledger) {
        anyhow::bail!("imported database is not a control schema this build serves: {error}");
    }
    for table in [
        "runs",
        "run_submissions",
        "jobs",
        "job_specs",
        "job_needs",
        "job_messages",
        "job_requests",
        "job_leases",
        "job_steps",
        "timelines",
        "timeline_records",
        "log_files",
        "runners",
        "runner_sessions",
        "webhook_deliveries",
        "workflow_run_numbers",
        "run_history",
        "job_history",
        "attempt_history",
        "step_history",
        "schema_meta",
    ] {
        let exists: bool = conn.query_row(
            "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
            [table],
            |row| row.get(0),
        )?;
        if !exists {
            bail!("imported database is missing required table {table}");
        }
    }
    Ok(())
}
