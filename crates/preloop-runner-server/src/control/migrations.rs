//! The control-database migration ledger: the versions this build ships and
//! the verification both backends run at open.
//!
//! The control schema is created and upgraded by the migration runner
//! (`preloop store migrate`, refinery over the embedded
//! `migrations/{sqlite,postgres}` — see `docs/control-migrations.md`). A
//! running server never creates, migrates or recreates the schema: it
//! verifies, and refuses anything that is not exactly this build's migration
//! set. Ledger rows are the sole version authority: `schema_meta` stays for
//! `key_fingerprint`, and a stale pre-ledger `schema_version` row is ignored
//! (retired by `--adopt-baseline`).
//!
//! Refinery's history table shape (`refinery-core` `traits`):
//! `refinery_schema_history(version int4 PRIMARY KEY, name VARCHAR(255),
//! applied_on VARCHAR(255), checksum VARCHAR(255))`. Only `version` is read
//! here; checksum divergence and missing/unknown migrations are the runner's
//! checks (`abort_divergent`/`abort_missing`) and the serve-time set compare
//! below.

use crate::control::types::ControlError;

/// The migration ledger table the runner records applied migrations in
/// (refinery's schema history; its default name, pinned explicitly by
/// [`crate::control::migrate_runner`] so the two cannot drift).
pub(crate) const LEDGER_TABLE: &str = "refinery_schema_history";

/// Versions this build ships, ascending. Mirrors the `V<version>__` filename
/// prefixes under `migrations/{sqlite,postgres}`; `tests::migration_files_match`
/// and `migrate_runner::tests` fail if either dialect drifts, so a new
/// migration cannot be added without the build knowing it.
///
/// Versions fit refinery's default `int4` version type (the workspace enables
/// refinery without `int8-versions`): 10-digit `YYYYMMDDNN` stamps, not
/// 14-digit timestamps.
pub(crate) const MIGRATIONS: &[i32] = &[2026100501, 2026100502, 2026100503, 2026100504];

/// The newest migration this build ships.
pub(crate) fn latest() -> i32 {
    *MIGRATIONS.last().expect("at least the baseline migration")
}

/// The operator-facing refusal for a non-verified database state.
pub(crate) fn refusal(ledger: Ledger) -> String {
    match ledger {
        Ledger::Empty => "control database is not initialized (no migration ledger). Run \
             `preloop store migrate` before starting the server; the server never creates or \
             upgrades the control schema itself (docs/control-migrations.md)."
            .to_owned(),
        Ledger::Legacy => "control database is a legacy preloop store (store schema v11): a \
             different data model this build never adopts or rewrites. Migrate it once with \
             `preloop store import-legacy --source <old preloop.db> --target <new control.db>`, \
             then point the server at the target (docs/control-migrations.md)."
            .to_owned(),
        Ledger::Foreign => "control database holds tables but no control schema; refusing to \
             initialize over existing state. Point the server at a dedicated control database, \
             or import a legacy store (`preloop store import-legacy`, docs/control-migrations.md)."
            .to_owned(),
        Ledger::Unledgered => "control database has a control schema but no \
             `refinery_schema_history` ledger: it predates tracked migrations (an unreleased \
             pre-ledger build: SQLite v4 / Postgres v5). Adopt it explicitly with \
             `preloop store migrate --adopt-baseline`, which verifies the schema first, or \
             recreate/import; the server never adopts it silently."
            .to_owned(),
        Ledger::MissingMeta => "control database has a migration ledger but no `schema_meta` \
             table: the schema is incomplete (a partial create or manual surgery). Restore the \
             pre-migration backup or recreate the database (docs/control-migrations.md)."
            .to_owned(),
        Ledger::Applied(versions) => check_applied(&versions)
            .expect_err("an applied ledger in refusal() is divergent by definition"),
    }
}

/// What a control database looks like at open, before any verification.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Ledger {
    /// Nothing but (at most) SQLite internals: a brand-new database.
    Empty,
    /// A legacy preloop store (SQLite `PRAGMA user_version`/`schema_migrations`,
    /// or the Postgres `public` legacy tables): a different data model that
    /// must go through the one-time import, never be adopted as a control DB.
    Legacy,
    /// Existing tables but no control schema: not ours to initialize over.
    Foreign,
    /// A control schema that predates the migration ledger (unreleased
    /// v3/v4 builds, or a half-created schema).
    Unledgered,
    /// A ledger-only database whose `schema_meta` is missing: the schema is
    /// incomplete and must not be served.
    MissingMeta,
    /// The ledger exists; versions are applied in order (as read).
    Applied(Vec<i32>),
}

/// Human-readable version list for error messages.
fn version_list(versions: &[i32]) -> String {
    versions
        .iter()
        .map(i32::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Verify the applied ledger equals exactly what this build ships.
///
/// The serve-time guard: an older database (missing migrations), a newer one
/// (unknown migrations), or a divergent set refuses with the recovery
/// command rather than being silently upgraded, recreated or adopted.
pub(crate) fn check_applied(applied: &[i32]) -> Result<(), String> {
    if applied == MIGRATIONS {
        return Ok(());
    }
    let missing: Vec<i32> = MIGRATIONS
        .iter()
        .copied()
        .filter(|version| !applied.contains(version))
        .collect();
    let unknown: Vec<i32> = applied
        .iter()
        .copied()
        .filter(|version| !MIGRATIONS.contains(version))
        .collect();
    if !missing.is_empty() && unknown.is_empty() {
        return Err(format!(
            "control database is older than this preloop build: migration(s) {} are not applied \
             (latest is {}). Run `preloop store migrate` before starting the server \
             (docs/control-migrations.md).",
            version_list(&missing),
            latest()
        ));
    }
    if missing.is_empty() {
        return Err(format!(
            "control database was migrated by a newer preloop build: migration(s) {} are unknown \
             to this build (latest is {}). Upgrade preloop, or restore the pre-migration backup \
             and run `preloop store migrate`.",
            version_list(&unknown),
            latest()
        ));
    }
    Err(format!(
        "control database migration ledger diverges from this build: missing {}, unknown {}. \
         Run `preloop store migrate` or restore the pre-migration backup \
         (docs/control-migrations.md).",
        version_list(&missing),
        version_list(&unknown)
    ))
}

/// Verify a database state read from either backend.
pub(crate) fn check_ledger(ledger: Ledger) -> Result<(), String> {
    if let Ledger::Applied(versions) = &ledger {
        return check_applied(versions);
    }
    Err(refusal(ledger))
}

/// Map a verification failure onto the backend error.
pub(crate) fn backend_error(message: impl Into<String>) -> ControlError {
    ControlError::backend(anyhow::anyhow!(message.into()))
}

/// Classify a SQLite database: ledger state from `sqlite_master`, the legacy
/// `schema_migrations` table and `PRAGMA user_version` (the released legacy
/// store stamps 11; any non-zero value without a control schema is "not
/// ours", which the refusal turns into the import guidance).
pub(crate) fn sqlite_ledger(conn: &rusqlite::Connection) -> rusqlite::Result<Ledger> {
    let user_tables: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    if user_tables == 0 {
        return Ok(Ledger::Empty);
    }
    let table_exists = |name: &str| -> rusqlite::Result<bool> {
        conn.query_row(
            "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
            [name],
            |row| row.get(0),
        )
    };
    let has_meta = table_exists("schema_meta")?;
    let has_ledger = table_exists(LEDGER_TABLE)?;
    let user_version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let has_legacy_ledger = table_exists("schema_migrations")?;
    if has_ledger {
        let mut statement = conn.prepare(&format!(
            "SELECT version FROM \"{LEDGER_TABLE}\" ORDER BY version"
        ))?;
        let versions = statement
            .query_map([], |row| row.get::<_, i32>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if !has_meta {
            return Ok(Ledger::MissingMeta);
        }
        return Ok(Ledger::Applied(versions));
    }
    if has_legacy_ledger || user_version != 0 {
        return Ok(Ledger::Legacy);
    }
    if has_meta {
        return Ok(Ledger::Unledgered);
    }
    Ok(Ledger::Foreign)
}

/// Verify a SQLite control database: ledger present and exactly this build's
/// migrations. `Err` is the operator-facing message (see [`check_ledger`]).
pub(crate) fn verify_sqlite(conn: &rusqlite::Connection) -> Result<(), String> {
    let ledger = sqlite_ledger(conn)
        .map_err(|error| format!("control database migration ledger is unreadable: {error}"))?;
    check_ledger(ledger)
}

/// Postgres `public`-schema tables written by the released legacy store. Any
/// of these next to a missing `control` schema means "refuse to create an
/// empty control schema beside old data".
pub(crate) const LEGACY_PUBLIC_TABLES: &[&str] = &[
    "schema_migrations",
    "runtime_snapshots",
    "job_request_messages",
    "message_payload_migrations",
    "workflow_run_counters",
];

/// Classify a Postgres database. The client resolves `control.*` (the
/// backend sets `search_path`), but every read is schema-qualified anyway.
pub(crate) async fn postgres_ledger(client: &tokio_postgres::Client) -> anyhow::Result<Ledger> {
    let has_ledger: bool = client
        .query_one(
            "SELECT to_regclass('control.refinery_schema_history') IS NOT NULL",
            &[],
        )
        .await?
        .get(0);
    let has_meta: bool = client
        .query_one("SELECT to_regclass('control.schema_meta') IS NOT NULL", &[])
        .await?
        .get(0);
    if has_ledger {
        let rows = client
            .query(
                "SELECT version FROM control.refinery_schema_history ORDER BY version",
                &[],
            )
            .await?;
        let versions = rows.iter().map(|row| row.get::<_, i32>(0)).collect();
        if !has_meta {
            return Ok(Ledger::MissingMeta);
        }
        return Ok(Ledger::Applied(versions));
    }
    let has_control_schema: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'control')",
            &[],
        )
        .await?
        .get(0);
    if has_control_schema || has_meta {
        return Ok(Ledger::Unledgered);
    }
    let legacy: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_name = ANY($1)",
            &[&LEGACY_PUBLIC_TABLES],
        )
        .await?
        .get(0);
    if legacy != 0 {
        return Ok(Ledger::Legacy);
    }
    Ok(Ledger::Empty)
}

/// Verify a Postgres control database (see [`verify_sqlite`]).
pub(crate) async fn verify_postgres(client: &tokio_postgres::Client) -> Result<(), String> {
    let ledger = postgres_ledger(client)
        .await
        .map_err(|error| format!("control database migration ledger is unreadable: {error}"))?;
    check_ledger(ledger)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a ledger table with the given versions, as the runner writes it.
    fn ledger_with(conn: &rusqlite::Connection, versions: &[i32]) {
        conn.execute_batch(&format!(
            "CREATE TABLE \"{LEDGER_TABLE}\"(version int4 PRIMARY KEY, name VARCHAR(255), \
             applied_on VARCHAR(255), checksum VARCHAR(255));"
        ))
        .unwrap();
        for version in versions {
            conn.execute(
                &format!(
                    "INSERT INTO \"{LEDGER_TABLE}\" (version, name, applied_on, checksum) \
                          VALUES (?1, 'x', '2026-10-05T00:00:00Z', '0')"
                ),
                [version],
            )
            .unwrap();
        }
    }

    /// Both dialects must ship exactly the versions this build expects: a new
    /// migration file without a matching const (or vice versa) fails here.
    #[test]
    fn migration_files_match() {
        let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .canonicalize()
            .expect("canonical repo root");
        for dialect in ["sqlite", "postgres"] {
            let dir = repo_root.join("migrations").join(dialect);
            let mut versions = Vec::new();
            for entry in std::fs::read_dir(&dir).expect("migrations dir") {
                let name = entry.expect("dir entry").file_name();
                let name = name.to_string_lossy();
                let Some(rest) = name.strip_prefix('V') else {
                    panic!("migration file {name} must start with V<version>__");
                };
                let version = rest
                    .split_once("__")
                    .unwrap_or_else(|| {
                        panic!("migration file {name} must be V<version>__<name>.sql")
                    })
                    .0
                    .parse::<i32>()
                    .unwrap_or_else(|_| panic!("migration file {name} has a non-int4 version"));
                versions.push(version);
            }
            versions.sort_unstable();
            assert_eq!(versions, MIGRATIONS, "{dialect} migrations");
        }
    }

    /// Older, newer and divergent ledgers all refuse with recovery guidance.
    #[test]
    fn applied_set_must_match_exactly() {
        let older = vec![MIGRATIONS[0]];
        let error = check_applied(&older).unwrap_err();
        assert!(error.contains("older"), "{error}");
        assert!(error.contains("preloop store migrate"), "{error}");

        let mut newer = MIGRATIONS.to_vec();
        newer.push(latest() + 1);
        let error = check_applied(&newer).unwrap_err();
        assert!(error.contains("newer"), "{error}");

        let divergent = vec![MIGRATIONS[0], latest() + 1];
        let error = check_applied(&divergent).unwrap_err();
        assert!(error.contains("diverges"), "{error}");
        assert!(error.contains(&(latest() + 1).to_string()), "{error}");
    }

    /// Classification: legacy stores, foreign files, pre-ledger control
    /// schemas and incomplete (ledger-less `schema_meta`) states each refuse
    /// with their own guidance.
    #[test]
    fn database_states_classify() {
        let legacy = rusqlite::Connection::open_in_memory().unwrap();
        legacy
            .execute_batch(
                "PRAGMA user_version = 11;
                 CREATE TABLE schema_migrations (version INTEGER, name TEXT);
                 CREATE TABLE runs (run_id TEXT PRIMARY KEY);",
            )
            .unwrap();
        assert_eq!(sqlite_ledger(&legacy).unwrap(), Ledger::Legacy);
        let error = verify_sqlite(&legacy).unwrap_err();
        assert!(error.contains("legacy"), "{error}");
        assert!(error.contains("preloop store import"), "{error}");

        let foreign = rusqlite::Connection::open_in_memory().unwrap();
        foreign
            .execute_batch("CREATE TABLE someone_elses (id INTEGER)")
            .unwrap();
        assert_eq!(sqlite_ledger(&foreign).unwrap(), Ledger::Foreign);

        let unledgered = rusqlite::Connection::open_in_memory().unwrap();
        unledgered
            .execute_batch("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value BLOB)")
            .unwrap();
        assert_eq!(sqlite_ledger(&unledgered).unwrap(), Ledger::Unledgered);
        let error = verify_sqlite(&unledgered).unwrap_err();
        assert!(error.contains("--adopt-baseline"), "{error}");

        let broken = rusqlite::Connection::open_in_memory().unwrap();
        ledger_with(&broken, MIGRATIONS);
        assert_eq!(sqlite_ledger(&broken).unwrap(), Ledger::MissingMeta);

        let empty = rusqlite::Connection::open_in_memory().unwrap();
        assert_eq!(sqlite_ledger(&empty).unwrap(), Ledger::Empty);
    }

    /// A fully-stamped ledger with a stale pre-ledger `schema_version` row is
    /// accepted: the ledger is the sole authority, `schema_meta` is not.
    #[test]
    fn ledger_is_the_only_version_authority() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value BLOB);
             INSERT INTO schema_meta (key, value) VALUES ('schema_version', '3');",
        )
        .unwrap();
        ledger_with(&conn, MIGRATIONS);
        assert_eq!(
            sqlite_ledger(&conn).unwrap(),
            Ledger::Applied(MIGRATIONS.to_vec())
        );
        verify_sqlite(&conn).unwrap();
    }
}
