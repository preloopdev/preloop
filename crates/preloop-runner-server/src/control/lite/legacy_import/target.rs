//! Target-database preparation for the importer.
//!
//! **Swap point.** The migrations branch owns schema initialization and the
//! version ledger; when the two land together this module's body is replaced
//! by `control::migrations::{initialize_empty_sqlite, verify_sqlite_ledger}`
//! (embedded migrations, `preloop store migrate`), and the importer keeps
//! calling [`initialize_fresh_target`] / [`verify_target_ledger`]. Until
//! then the body mirrors the lite backend's `ensure_schema` exactly: apply
//! `lite/schema.sql`, seed `sqlite_sequence`, stamp `schema_meta`.
//!
//! The importer never migrates a non-fresh database: an existing
//! `schema_meta` (or any foreign table) is a refusal, not an upgrade.

use anyhow::{Context, bail};
use rusqlite::{Connection, TransactionBehavior};

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
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .context("begin target initialization")?;
    let has_meta: bool = tx.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master \
         WHERE type = 'table' AND name = 'schema_meta')",
        [],
        |row| row.get(0),
    )?;
    if has_meta {
        bail!("staging database already carries a control schema; refusing to re-initialize it");
    }
    let foreign: i64 = tx.query_row(
        "SELECT COUNT(*) FROM sqlite_master \
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    if foreign != 0 {
        bail!("staging database holds {foreign} tables but no schema_meta; refusing to adopt it");
    }
    tx.execute_batch(include_str!("../schema.sql"))
        .context("apply control schema to staging database")?;
    tx.execute(
        "INSERT INTO sqlite_sequence (name, seq) VALUES ('session_messages', 1000000)",
        [],
    )?;
    tx.execute(
        "INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?1)",
        [crate::control::lite::SCHEMA_VERSION.as_bytes()],
    )?;
    tx.commit().context("commit target initialization")
}

/// Verify the target carries the control schema this build reads.
///
/// The standalone-branch check mirrors the lite backend's `ensure_schema`:
/// the recorded schema version must match exactly. The migrations branch
/// replaces this with its ledger verification, which is the schema-version
/// authority there.
pub(crate) fn verify_target_ledger(conn: &Connection) -> anyhow::Result<()> {
    let version: Option<Vec<u8>> = conn
        .query_row(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .ok();
    let version = version.map(|value| String::from_utf8_lossy(&value).into_owned());
    if version.as_deref() != Some(crate::control::lite::SCHEMA_VERSION) {
        bail!(
            "imported database schema version {:?} does not match this build's {}",
            version.as_deref().unwrap_or("<none>"),
            crate::control::lite::SCHEMA_VERSION
        );
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
