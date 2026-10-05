//! One-time import of a legacy v11 durable-state SQLite store into a fresh
//! control database.
//!
//! The import is explicit, offline, and fail-closed:
//!
//! 1. the source (`<state_dir>/preloop.db` from a released pre-control-backend
//!    build) is opened read-only and hashed;
//! 2. every persisted record is decoded and audited before anything is
//!    written — a wrong cluster key, an unrecognized source version, an
//!    unreadable blob, or live claims abort the import with a report;
//! 3. the target is built as `<target>.importing` in one transaction, verified
//!    (`PRAGMA foreign_key_check` + row counts + the schema ledger), then
//!    atomically renamed into place. A failure leaves no target behind, so a
//!    retry starts from a clean slate;
//! 4. the source digest is re-checked after the read and the import fails if
//!    the source changed underneath it.
//!
//! The target schema is initialized through
//! [`target::initialize_fresh_target`] — the swap point the migrations work
//! replaces with the embedded-migration initializer (`preloop store migrate`)
//! once the two land together.

pub(crate) mod legacy;
mod report;
mod target;
mod writer;
#[cfg(any(test, feature = "test-support"))]
pub mod fixture;
#[cfg(test)]
mod tests;

pub use report::{ActivePolicy, ImportReport, ImportedRows, SkippedFamily};

use crate::store::Envelope;
use anyhow::{Context, bail};
use std::path::{Path, PathBuf};

/// Options for one import run. Paths are filesystem paths (the CLI owns
/// argument parsing; the library only consults `PRELOOP_HMAC_KEY` through
/// [`load_import_key`]).
pub struct ImportOptions {
    /// The legacy `preloop.db` to read (opened read-only, never written).
    pub source: PathBuf,
    /// The control database to create. Must not exist yet.
    pub target: PathBuf,
    /// The server state directory the imported control DB will live in.
    /// Secret tiers and log segments are written here.
    pub state_dir: PathBuf,
    /// The 32-byte cluster key. Pass [`load_import_key`]'s result.
    pub key: Vec<u8>,
    /// What to do with in-flight work: refuse (default), requeue, or cancel.
    pub active: ActivePolicy,
}

/// Load the cluster key the way the server does, but never generate one: an
/// import with the wrong key must fail, not silently mint a key that cannot
/// decrypt the source.
///
/// Resolution order: `--key <file>`, then `PRELOOP_HMAC_KEY`, then
/// `<state_dir>/hmac-key.bin`.
pub fn load_import_key(state_dir: &Path, explicit: Option<&Path>) -> anyhow::Result<Vec<u8>> {
    if let Some(path) = explicit {
        let key = std::fs::read(path)
            .with_context(|| format!("read cluster key {}", path.display()))?;
        check_key_size(&key, &path.display().to_string())?;
        return Ok(key);
    }
    if let Ok(value) = std::env::var("PRELOOP_HMAC_KEY")
        && !value.trim().is_empty()
    {
        return crate::state::parse_hmac_key(&value);
    }
    let path = state_dir.join("hmac-key.bin");
    if !path.exists() {
        bail!(
            "no cluster key: {} does not exist and PRELOOP_HMAC_KEY is unset; \
             point --key at the key used by the legacy server",
            path.display()
        );
    }
    let key = std::fs::read(&path)
        .with_context(|| format!("read cluster key {}", path.display()))?;
    check_key_size(&key, &path.display().to_string())?;
    Ok(key)
}

fn check_key_size(key: &[u8], source: &str) -> anyhow::Result<()> {
    if key.len() != 32 {
        bail!("cluster key {source} must be 32 bytes, got {}", key.len());
    }
    Ok(())
}

/// Run the import. Returns the report on success. On failure the target does
/// not exist and the staging file has been removed, so the operation is safe
/// to retry after fixing the reported condition.
pub fn run_import(options: &ImportOptions) -> anyhow::Result<ImportReport> {
    // ── Paths ───────────────────────────────────────────────────────────
    let source = &options.source;
    let target = &options.target;
    if !source.is_file() {
        bail!("legacy source {} does not exist", source.display());
    }
    if target.exists() {
        bail!(
            "target {} already exists; the importer only creates a fresh control \
             database and refuses to overwrite anything",
            target.display()
        );
    }
    let source_canonical = std::fs::canonicalize(source)
        .with_context(|| format!("resolve source path {}", source.display()))?;
    let target_parent = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(target_parent)
        .with_context(|| format!("create target directory {}", target_parent.display()))?;
    let target_canonical = std::fs::canonicalize(target_parent)
        .map(|parent| parent.join(target.file_name().unwrap_or_default()))?;
    if target_canonical == source_canonical {
        bail!("source and target are the same file; refusing to overwrite the legacy store");
    }

    let digest_before = legacy::file_digest(source)?;
    let db = legacy::LegacyDb::open(source)?;
    let data_version_before = db.data_version()?;
    let header = db.header()?;
    let counts = db.counts()?;
    let cipher = Envelope::new(&options.key);
    let legacy_state_dir = source
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();

    // Decode everything up front: a decryption or shape failure aborts before
    // a single byte is written, and the wrong key is caught here.
    let mut source_data = writer::SourceData {
        header,
        counts,
        runs: db.runs(&cipher)?,
        jobs: db.queued_jobs(&cipher)?,
        attempts: db.attempts(&cipher)?,
        steps: db.steps(&cipher)?,
        logs: db.log_files(&cipher)?,
        runners: db.runners()?,
        sessions: db.sessions()?,
        session_active: db.session_active_requests()?,
        dependencies: db.job_dependencies()?,
        counters: db.workflow_run_counters()?,
        webhook_deliveries: db.webhook_deliveries(&cipher)?,
        webhook_watchdog: db.webhook_watchdog()?,
        webhook_redeliveries: db.webhook_redeliveries()?,
        control_events: db.control_events()?,
        job_request_messages: db.job_request_messages()?,
        artifact_registry_sidecar: read_artifact_registry_sidecar(
            &source
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("artifact_v2_registry.json"),
        )?,
        meta: db.meta_snapshot(&cipher)?,
    };
    writer::audit_meta_keys(&source_data)?;
    writer::audit_unmappable(&source_data)?;
    writer::audit_invariants(&source_data)?;
    let active = writer::audit_active(&source_data);
    if options.active == ActivePolicy::Refuse && !active.is_empty() {
        let mut listed: Vec<String> = active
            .iter()
            .take(20)
            .map(|item| format!("  {} {}: {}", item.kind, item.id, item.detail))
            .collect();
        if active.len() > listed.len() {
            listed.push(format!("  … and {} more", active.len() - listed.len()));
        }
        bail!(
            "legacy store has {} item(s) that a silent import would lose or double-run:\n{}\n\
             Stop the legacy server and let its work drain, or rerun with --active=requeue \
             (release claims and requeue) or --active=cancel (settle them as cancelled).",
            active.len(),
            listed.join("\n")
        );
    }

    // ── Build the target ────────────────────────────────────────────────
    // Sidecars (run-secret tiers, live-log segments) stage under a
    // target-owned directory and publish only after the database commit and
    // the source re-check; nothing is written into the live state directory
    // before that.
    std::fs::create_dir_all(&options.state_dir).with_context(|| {
        format!("create state directory {}", options.state_dir.display())
    })?;
    let mut sidecars = writer::Sidecars::new(
        options
            .state_dir
            .join(format!(".import-{}", uuid::Uuid::new_v4())),
    );
    let staging = staging_path(target);
    if staging.exists() {
        std::fs::remove_file(&staging)
            .with_context(|| format!("remove stale staging file {}", staging.display()))?;
    }
    let build = (|| -> anyhow::Result<(ImportReport, String)> {
        let mut conn = rusqlite::Connection::open(&staging)
            .with_context(|| format!("create staging database {}", staging.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(0o600))?;
        }
        target::configure(&mut conn)?;
        target::initialize_fresh_target(&mut conn)?;

        let outcome = {
            let tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .context("begin import transaction")?;
            let outcome = writer::write_import(
                &tx,
                &mut source_data,
                options.active,
                &mut sidecars,
                &options.state_dir,
                &legacy_state_dir,
                &cipher,
            )?;
            verify_invariants(&tx, &outcome.imported)?;
            target::verify_target_ledger(&tx)?;
            tx.commit().context("commit import transaction")?;
            outcome
        };
        let _ = conn.close();

        // ── Prove the source survived, then publish the target ──────────
        let data_version_after = db.data_version()?;
        let digest_after = legacy::file_digest(source)?;
        if digest_after != digest_before || data_version_after != data_version_before {
            bail!(
                "legacy source {} changed while it was being read (digest {} -> {}); the \
                 import was discarded — stop the server and retry",
                source.display(),
                &digest_before[..16.min(digest_before.len())],
                &digest_after[..16.min(digest_after.len())]
            );
        }
        // Everything verified: publish the sidecars (never overwriting), then
        // the caller renames the database. A sidecar failure rolls back the
        // sidecars it created and leaves the database unpublished.
        sidecars.publish(&cipher)?;
        let report = ImportReport {
            source: source.clone(),
            target: target.clone(),
            state_dir: options.state_dir.clone(),
            source_digest_before: digest_before.clone(),
            source_digest_after: digest_after.clone(),
            legacy_user_version: source_data.header.user_version,
            active_policy: options.active,
            imported: outcome.imported,
            skipped: outcome.skipped,
            notes: outcome.notes,
        };
        Ok((report, digest_after))
    })();

    let (report, _digest_after) = match build {
        Ok(value) => value,
        Err(error) => {
            sidecars.rollback();
            sidecars.cleanup();
            let _ = std::fs::remove_file(&staging);
            return Err(error);
        }
    };
    drop(db);
    // Atomic no-replace publication: `hard_link` fails if anything appeared
    // at the target between the preflight and now, so an existing file is
    // never overwritten (rename(2) would replace it silently).
    if let Err(error) = publish_no_replace(&staging, target) {
        sidecars.rollback();
        sidecars.cleanup();
        let _ = std::fs::remove_file(&staging);
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            return Err(error).with_context(|| {
                format!(
                    "target {} appeared during the import; nothing was overwritten",
                    target.display()
                )
            });
        }
        return Err(error)
            .with_context(|| format!("publish imported database {}", target.display()));
    }
    if let Ok(handle) = std::fs::File::open(target) {
        let _ = handle.sync_all();
    }
    let _ = std::fs::remove_file(&staging);
    sidecars.cleanup();
    if let Ok(dir) = std::fs::File::open(target_parent) {
        let _ = dir.sync_all();
    }
    Ok(report)
}

/// Publish a fully-built staging file at `target` without ever replacing an
/// existing file: `hard_link` is atomic and fails with `AlreadyExists`, where
/// `rename(2)` would silently overwrite a target that appeared after the
/// preflight.
pub(crate) fn publish_no_replace(staging: &Path, target: &Path) -> std::io::Result<()> {
    std::fs::hard_link(staging, target)
}

/// The legacy node keeps the finalized artifact registry in a JSON sidecar
/// next to the database (written by `save_artifact_v2_registry`); it is
/// authoritative over the copy embedded in the metadata snapshot when both
/// exist, so the importer reads it from the source's directory.
fn read_artifact_registry_sidecar(
    path: &Path,
) -> anyhow::Result<Option<serde_json::Map<String, serde_json::Value>>> {
    match std::fs::read_to_string(path) {
        Ok(raw) => {
            let value: serde_json::Value = serde_json::from_str(&raw)
                .with_context(|| format!("parse artifact registry sidecar {}", path.display()))?;
            match value {
                serde_json::Value::Object(map) => Ok(Some(map)),
                other => bail!(
                    "artifact registry sidecar {} is not a JSON object: {other}",
                    path.display()
                ),
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error)
            .with_context(|| format!("read artifact registry sidecar {}", path.display())),
    }
}

/// `<target>.importing` in the target's own directory, so the final rename is
/// atomic on the same filesystem.
fn staging_path(target: &Path) -> PathBuf {
    PathBuf::from(format!("{}.importing", target.display()))
}

/// Row-level invariants the target must satisfy before it can be published.
fn verify_invariants(
    tx: &rusqlite::Transaction<'_>,
    imported: &ImportedRows,
) -> anyhow::Result<()> {
    let count = |table: &str| -> anyhow::Result<u64> {
        let value: i64 = tx.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })?;
        Ok(value.max(0) as u64)
    };
    for (table, expected) in [
        ("runs", imported.runs),
        ("jobs", imported.jobs),
        ("job_requests", imported.job_requests),
        ("job_steps", imported.job_steps),
        ("run_submissions", imported.runs),
    ] {
        let actual = count(table)?;
        if actual != expected {
            bail!("target {table} row count {actual} != imported {expected}");
        }
    }
    let foreign_violations: i64 = tx.query_row(
        "SELECT COUNT(*) FROM pragma_foreign_key_check",
        [],
        |row| row.get(0),
    )?;
    if foreign_violations != 0 {
        bail!("target has {foreign_violations} foreign-key violations; refusing to publish");
    }
    Ok(())
}
