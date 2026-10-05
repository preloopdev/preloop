//! The one writer of control schema state: refinery over the embedded
//! migration sets in `migrations/{sqlite,postgres}`.
//!
//! `preloop store migrate` (and the brand-new local preparation) call here; a
//! serving process never does — it only verifies ([`super::migrations`]).
//! Guarantees this module leans on:
//!
//! - pending migrations and their ledger rows are applied in ONE transaction
//!   (refinery's grouped mode), so a failed migration rolls the whole batch
//!   back and the ledger is unchanged;
//! - `abort_divergent`/`abort_missing` fail a changed migration file or a
//!   ledger row the embedded set does not ship (checksum/missing), so an
//!   edited or removed history is loud, never silently ignored;
//! - refinery has no down: rollback means restoring the SQLite backup the
//!   caller takes before applying (see `store_admin`), the operator's
//!   Postgres backup, or a forward corrective migration.
//!
//! Adoption of a pre-ledger control database uses refinery's `Target::Fake`
//! modes: ledger rows are stamped from the embedded migrations (real
//! checksums) after [`verify_shape_sqlite`] / [`verify_shape_postgres`]
//! confirm the database is structurally the schema those migrations build.
//! Never a silent adoption: the caller must ask for it explicitly
//! (`preloop store migrate --adopt-baseline`).

use crate::control::migrations::{LEDGER_TABLE, MIGRATIONS, latest};
use refinery::Target;

mod sqlite {
    refinery::embed_migrations!("../../migrations/sqlite");
}
mod postgres {
    refinery::embed_migrations!("../../migrations/postgres");
}

/// Advisory-lock key serializing Postgres schema setup / migration across
/// nodes and operators (`pg_advisory_lock`; session-scoped).
pub(crate) const POSTGRES_SETUP_LOCK: i64 = 2026100501;

fn sqlite_runner(target: Target) -> refinery::Runner {
    let mut runner = sqlite::migrations::runner()
        .set_abort_divergent(true)
        .set_abort_missing(true)
        .set_grouped(true)
        .set_target(target);
    runner.set_migration_table_name(LEDGER_TABLE);
    runner
}

fn postgres_runner(target: Target) -> refinery::Runner {
    let mut runner = postgres::migrations::runner()
        .set_abort_divergent(true)
        .set_abort_missing(true)
        .set_grouped(true)
        .set_target(target);
    runner.set_migration_table_name(LEDGER_TABLE);
    runner
}

/// Migrations embedded for this dialect, ascending — the compile-time mirror
/// of [`MIGRATIONS`]. Test-only: the parity test asserts the two agree.
#[cfg(test)]
fn embedded_versions(runner: &refinery::Runner) -> Vec<i32> {
    runner.get_migrations().iter().map(|m| m.version()).collect()
}

/// Apply every pending SQLite migration, returning the versions applied.
pub(crate) fn run_sqlite(conn: &mut rusqlite::Connection) -> anyhow::Result<Vec<i32>> {
    let report = sqlite_runner(Target::Latest)
        .run(conn)
        .map_err(|error| anyhow::anyhow!("control migration failed: {error}"))?;
    Ok(report
        .applied_migrations()
        .iter()
        .map(|m| m.version())
        .collect())
}

/// Apply every pending Postgres migration, returning the versions applied.
pub(crate) async fn run_postgres(client: &mut tokio_postgres::Client) -> anyhow::Result<Vec<i32>> {
    let report = postgres_runner(Target::Latest)
        .run_async(client)
        .await
        .map_err(|error| anyhow::anyhow!("control migration failed: {error}"))?;
    Ok(report
        .applied_migrations()
        .iter()
        .map(|m| m.version())
        .collect())
}

/// Initialize an EMPTY SQLite database (no user tables) through the embedded
/// migrations and their ledger. Refuses anything that already holds state:
/// this is a create-only path, never an upgrade.
pub(crate) fn initialize_empty_sqlite(conn: &mut rusqlite::Connection) -> anyhow::Result<()> {
    let tables: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    anyhow::ensure!(
        tables == 0,
        "refusing to initialize a control database that already holds {tables} tables"
    );
    run_sqlite(conn)?;
    Ok(())
}

/// Initialize an EMPTY Postgres database (no `control` schema) through the
/// embedded migrations and their ledger, serialized by the setup advisory
/// lock so two booting test-support nodes cannot race the create.
pub(crate) async fn initialize_empty_postgres(
    client: &mut tokio_postgres::Client,
) -> anyhow::Result<()> {
    client
        .batch_execute(&format!(
            "SELECT pg_advisory_lock({POSTGRES_SETUP_LOCK})"
        ))
        .await?;
    let result = async {
        let exists: bool = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'control')",
                &[],
            )
            .await?
            .get(0);
        if !exists {
            // Refinery's ledger table is created before the migrations run and
            // needs the schema to exist (the baseline migration itself uses
            // `CREATE SCHEMA IF NOT EXISTS`, but that runs after).
            client.batch_execute("CREATE SCHEMA control").await?;
        }
        run_postgres(client).await.map(|_| ())
    }
    .await;
    let _ = client
        .batch_execute(&format!(
            "SELECT pg_advisory_unlock({POSTGRES_SETUP_LOCK})"
        ))
        .await;
    result
}

/// Adopt a pre-ledger SQLite control database: stamp ledger rows for the
/// embedded migrations through `through` without executing their SQL
/// (refinery `Target::FakeVersion`), then apply the pending migrations.
///
/// The caller MUST have verified the database shape first
/// ([`verify_shape_sqlite`]); this function only writes the ledger.
pub(crate) fn adopt_sqlite(
    conn: &mut rusqlite::Connection,
    through: i32,
) -> anyhow::Result<Vec<i32>> {
    sqlite_runner(Target::FakeVersion(through))
        .run(conn)
        .map_err(|error| anyhow::anyhow!("control ledger adoption failed: {error}"))?;
    run_sqlite(conn)
}

/// Adopt a pre-ledger Postgres control database (see [`adopt_sqlite`]).
pub(crate) async fn adopt_postgres(
    client: &mut tokio_postgres::Client,
    through: i32,
) -> anyhow::Result<Vec<i32>> {
    postgres_runner(Target::FakeVersion(through))
        .run_async(client)
        .await
        .map_err(|error| anyhow::anyhow!("control ledger adoption failed: {error}"))?;
    run_postgres(client).await
}

// ── Shape verification for adoption ─────────────────────────────────────

/// Collapse whitespace runs so DDL text differences in line wrapping never
/// decide a shape comparison.
fn normalize(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// SQLite structural fingerprint: every `sqlite_master` object except
/// internals and the ledger table, normalized and ordered by name.
fn sqlite_fingerprint(conn: &rusqlite::Connection) -> rusqlite::Result<String> {
    let mut statement = conn.prepare(
        "SELECT type || '|' || name || '|' || tbl_name || '|' || \
         COALESCE(sql, '') FROM sqlite_master \
         WHERE name NOT LIKE 'sqlite_%' AND name <> ?1 ORDER BY name",
    )?;
    let rows = statement
        .query_map([LEDGER_TABLE], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(normalize(&rows.join("\n")))
}

/// Verify an unledgered SQLite database is structurally the schema built by
/// the embedded migrations up to and including `through` (an exact
/// `sqlite_master` match; no data is compared). `Err` names the first
/// difference so an operator can see why adoption refused.
pub(crate) fn verify_shape_sqlite(
    conn: &rusqlite::Connection,
    through: i32,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        MIGRATIONS.contains(&through),
        "unknown adoption version {through}"
    );
    let reference = rusqlite::Connection::open_in_memory()?;
    for migration in sqlite_runner(Target::Latest).get_migrations() {
        if migration.version() > through {
            continue;
        }
        let sql = migration
            .sql()
            .expect("embedded migrations always carry SQL");
        reference.execute_batch(sql)?;
    }
    let expected = sqlite_fingerprint(&reference)?;
    let actual = sqlite_fingerprint(conn)?;
    if actual == expected {
        return Ok(());
    }
    let first_difference = expected
        .lines()
        .zip(actual.lines())
        .find(|(expected, actual)| expected != actual);
    match first_difference {
        Some((expected, actual)) => anyhow::bail!(
            "database shape does not match control migrations through {through} \
             (expected `{expected}`, found `{actual}`); refusing to adopt"
        ),
        None => anyhow::bail!(
            "database shape does not match control migrations through {through} \
             (object count {} vs {}); refusing to adopt",
            expected.lines().count(),
            actual.lines().count()
        ),
    }
}

/// Postgres structural fingerprint: tables and columns, indexes, constraints,
/// non-internal triggers and functions, normalized with the schema name
/// stripped so a reference schema and `control` compare equal.
const POSTGRES_FINGERPRINT_SQL: &str = r#"
WITH objects AS (
    SELECT 'T|' || c.relname || '|' || a.attname || '|' ||
           format_type(a.atttypid, a.atttypmod) || '|' || a.attnotnull::text || '|' ||
           COALESCE(pg_get_expr(d.adbin, d.adrelid), '') AS line
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
    LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
    WHERE n.nspname = $1 AND c.relkind IN ('r', 'p')
    UNION ALL
    SELECT 'I|' || indexname || '|' || indexdef
    FROM pg_indexes WHERE schemaname = $1
    UNION ALL
    SELECT 'C|' || conname || '|' || contype::text || '|' || pg_get_constraintdef(c.oid)
    FROM pg_constraint c
    JOIN pg_namespace n ON n.oid = c.connamespace
    WHERE n.nspname = $1
    UNION ALL
    SELECT 'G|' || t.tgname || '|' || pg_get_triggerdef(t.oid)
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1 AND NOT t.tgisinternal
    UNION ALL
    SELECT 'P|' || p.proname || '|' || pg_get_function_arguments(p.oid)
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = $1
)
SELECT line FROM objects ORDER BY line
"#;

async fn postgres_fingerprint<C>(client: &C, schema: &str) -> anyhow::Result<String>
where
    C: tokio_postgres::GenericClient,
{
    let rows = client.query(POSTGRES_FINGERPRINT_SQL, &[&schema]).await?;
    let lines = rows
        .iter()
        .map(|row| {
            let line: String = row.get(0);
            // A definition printed while `search_path` resolves the schema
            // prints it unqualified; elsewhere it prints `schema.` — strip
            // both so the two sides compare equal.
            line.replace(&format!("{schema}."), "").replace("control.", "")
        })
        .collect::<Vec<_>>();
    Ok(lines.join("\n"))
}

/// Verify an unledgered Postgres database's `control` schema is structurally
/// the schema built by the embedded migrations through `through`. The
/// reference is built in a scratch schema inside a transaction that is always
/// rolled back, so nothing persists.
pub(crate) async fn verify_shape_postgres(
    client: &mut tokio_postgres::Client,
    through: i32,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        MIGRATIONS.contains(&through),
        "unknown adoption version {through}"
    );
    let scratch = format!(
        "preloop_shape_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let tx = client.transaction().await?;
    let verification = async {
        for migration in postgres_runner(Target::Latest).get_migrations() {
            if migration.version() > through {
                continue;
            }
            let sql = migration
                .sql()
                .expect("embedded migrations always carry SQL")
                // Retarget every reference at `control` onto the scratch
                // schema: the baseline prologue creates it, later files may
                // qualify objects explicitly (`ALTER TABLE control.…`), and
                // the rest is unqualified DDL that follows `search_path`.
                .replace(
                    "CREATE SCHEMA IF NOT EXISTS control;",
                    &format!("CREATE SCHEMA {scratch};"),
                )
                .replace("SET search_path = control;", &format!("SET search_path = {scratch};"))
                .replace("control.", &format!("{scratch}."));
            tx.batch_execute(&sql).await?;
        }
        let expected = postgres_fingerprint(&tx, &scratch).await?;
        let actual = postgres_fingerprint(&tx, "control").await?;
        if expected == actual {
            return Ok(());
        }
        let first_difference = expected
            .lines()
            .zip(actual.lines())
            .find(|(expected, actual)| expected != actual);
        match first_difference {
            Some((expected, actual)) => anyhow::bail!(
                "database shape does not match control migrations through {through} \
                 (expected `{expected}`, found `{actual}`); refusing to adopt"
            ),
            None => anyhow::bail!(
                "database shape does not match control migrations through {through} \
                 (object count {} vs {}); refusing to adopt",
                expected.lines().count(),
                actual.lines().count()
            ),
        }
    }
    .await;
    // Always drop the scratch schema, even on failure (DDL is transactional).
    tx.rollback().await?;
    verification
}

/// The versions at which an unledgered database might legitimately sit:
/// the baseline or the current latest, newest first — the shapes adoption
/// will try before refusing.
pub(crate) fn adoption_candidates() -> Vec<i32> {
    let mut candidates = vec![latest()];
    if MIGRATIONS.first() != MIGRATIONS.last() {
        candidates.push(MIGRATIONS[0]);
    }
    candidates
}

/// A verified adoption point: the database's shape matched this version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ShapeProbe {
    pub(crate) version: i32,
}

/// Try every plausible pre-ledger shape (newest first) and return the one the
/// database matches. `Err` aggregates why each candidate refused.
pub(crate) fn probe_shape_sqlite(conn: &rusqlite::Connection) -> anyhow::Result<ShapeProbe> {
    let mut reasons = Vec::new();
    for version in adoption_candidates() {
        match verify_shape_sqlite(conn, version) {
            Ok(()) => return Ok(ShapeProbe { version }),
            Err(error) => reasons.push(format!("{version}: {error}")),
        }
    }
    anyhow::bail!(
        "database shape matches no known control schema; refusing adoption ({})",
        reasons.join("; ")
    )
}

/// Postgres [`probe_shape_sqlite`] (see [`verify_shape_postgres`]).
pub(crate) async fn probe_shape_postgres(
    client: &mut tokio_postgres::Client,
) -> anyhow::Result<ShapeProbe> {
    let mut reasons = Vec::new();
    for version in adoption_candidates() {
        match verify_shape_postgres(client, version).await {
            Ok(()) => return Ok(ShapeProbe { version }),
            Err(error) => reasons.push(format!("{version}: {error}")),
        }
    }
    anyhow::bail!(
        "database shape matches no known control schema; refusing adoption ({})",
        reasons.join("; ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::migrations::{Ledger, check_applied, sqlite_ledger, verify_sqlite};

    fn fixture(name: &str) -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("fixtures")
            .join("control-migrations")
            .join(name);
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
    }

    fn baseline_sql(runner: &refinery::Runner) -> String {
        runner
            .get_migrations()
            .iter()
            .find(|migration| migration.version() == MIGRATIONS[0])
            .expect("baseline migration embedded")
            .sql()
            .expect("embedded migrations always carry SQL")
            .to_owned()
    }

    /// The embedded sets are the compile-time mirror of `MIGRATIONS`; a new
    /// file missing from the const (or vice versa) fails here, per dialect.
    #[test]
    fn embedded_migrations_match_const() {
        assert_eq!(
            embedded_versions(&sqlite_runner(Target::Latest)),
            MIGRATIONS,
            "embedded sqlite migrations"
        );
        assert_eq!(
            embedded_versions(&postgres_runner(Target::Latest)),
            MIGRATIONS,
            "embedded postgres migrations"
        );
    }

    /// Applying every embedded SQLite migration must produce exactly the
    /// runtime's `schema.sql` DDL (internals and the ledger aside): the
    /// declared schema cannot drift from the migration history.
    #[test]
    fn sqlite_migrations_match_schema_sql() {
        fn master(conn: &rusqlite::Connection) -> String {
            let mut statement = conn
                .prepare(
                    "SELECT type || '|' || name || '|' || tbl_name || '|' || COALESCE(sql, '') \
                     FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' AND name <> ?1 \
                     ORDER BY name",
                )
                .unwrap();
            let rows = statement
                .query_map([LEDGER_TABLE], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            normalize(&rows.join("\n"))
        }
        let mut migrated = rusqlite::Connection::open_in_memory().unwrap();
        run_sqlite(&mut migrated).unwrap();
        let declared = rusqlite::Connection::open_in_memory().unwrap();
        declared
            .execute_batch(crate::control::lite::SCHEMA_SQL)
            .unwrap();
        assert_eq!(
            master(&migrated),
            master(&declared),
            "migrations/sqlite drifted from control/lite/schema.sql"
        );
    }

    /// A fresh run creates the schema and a real ledger (checksums parse as
    /// u64, the shape refinery reads back), and the guard accepts it.
    #[test]
    fn fresh_init_then_verify() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        initialize_empty_sqlite(&mut conn).unwrap();
        assert_eq!(
            sqlite_ledger(&conn).unwrap(),
            Ledger::Applied(MIGRATIONS.to_vec())
        );
        verify_sqlite(&conn).unwrap();
        let checksums: Vec<String> = conn
            .prepare(&format!(
                "SELECT checksum FROM \"{LEDGER_TABLE}\" ORDER BY version"
            ))
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(checksums.len(), MIGRATIONS.len());
        for checksum in checksums {
            checksum
                .parse::<u64>()
                .expect("refinery reads checksums as u64");
        }
        // A second initialize refuses: create-only, never an upgrade path.
        assert!(initialize_empty_sqlite(&mut conn).is_err());
    }

    /// The gate's upgrade case: a populated baseline ("previous release")
    /// database, upgraded in place, keeps every row, gains the new index and
    /// ends structurally identical to a fresh migration run.
    #[test]
    fn populated_baseline_upgrades_losslessly() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(&baseline_sql(&sqlite_runner(Target::Latest)))
            .unwrap();
        conn.execute_batch(&fixture("seed_sqlite.sql")).unwrap();
        // The pre-upgrade store has no ledger yet: the old runtime stamped
        // only `schema_meta`.
        assert_eq!(sqlite_ledger(&conn).unwrap(), Ledger::Unledgered);

        // A populated pre-ledger baseline upgrades through the same probe →
        // adopt → apply sequence `preloop store migrate --adopt-baseline`
        // uses; applying the migration set directly would re-create the
        // baseline.
        let probe = probe_shape_sqlite(&conn).unwrap();
        assert_eq!(probe.version, MIGRATIONS[0]);
        let applied = adopt_sqlite(&mut conn, probe.version).unwrap();
        assert_eq!(applied, MIGRATIONS[1..].to_vec());
        verify_sqlite(&conn).unwrap();

        // Structural parity with a fresh database.
        let mut fresh = rusqlite::Connection::open_in_memory().unwrap();
        run_sqlite(&mut fresh).unwrap();
        let fingerprint = |conn: &rusqlite::Connection| {
            sqlite_fingerprint(conn).unwrap()
        };
        assert_eq!(
            fingerprint(&conn),
            fingerprint(&fresh),
            "upgraded database did not converge on the fresh schema"
        );

        // Every object the upgrade adds exists.
        for (kind, name) in [
            ("index", "runs_fork_approval_sweep"),
            ("table", "environment_approvals"),
            ("index", "environment_approvals_gate"),
        ] {
            let found: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = ?1 AND name = ?2",
                    rusqlite::params![kind, name],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(found, 1, "{kind} {name} missing after upgrade");
        }
        // History rows are attempt-1 by definition after the rebuild.
        let history_attempts: (i64, i64) = conn
            .query_row(
                "SELECT (SELECT run_attempt FROM run_history), \
                        (SELECT run_attempt FROM job_history)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(history_attempts, (1, 1));
        // A rerun-2 history row now coexists with attempt 1.
        conn.execute(
            "INSERT INTO run_history (run_id, namespace_id, repository, workflow_path, \
             run_number, run_attempt, event, ref, ref_type, head_sha, submission, created_at) \
             SELECT run_id, namespace_id, repository, workflow_path, run_number, 2, event, \
             ref, ref_type, head_sha, submission, created_at + 1000 FROM run_history",
            [],
        )
        .unwrap();
        let attempts: Vec<i64> = conn
            .prepare("SELECT run_attempt FROM run_history ORDER BY run_attempt")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(attempts, vec![1, 2]);
        // The pre-existing job survives and gains NULL deployment columns.
        let deploy: (i64, i64) = conn
            .query_row(
                "SELECT deployment_id IS NULL, environment_url IS NULL FROM jobs \
                 WHERE run_id = '22222222-2222-4222-8222-222222222222' AND job_id = 'deploy'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(deploy, (1, 1));

        // Data invariants: every seeded row survived byte-for-byte.
        let counts: Vec<(&str, i64)> = vec![
            ("namespaces", 2),
            ("runs", 2),
            ("jobs", 3),
            ("job_specs", 3),
            ("job_needs", 1),
            ("runners", 1),
            ("runner_sessions", 1),
            ("job_requests", 1),
            ("job_leases", 1),
            ("job_steps", 1),
            ("timelines", 1),
            ("timeline_records", 1),
            ("log_files", 1),
            ("session_messages", 1),
            ("webhook_deliveries", 2),
            ("webhook_watchdog", 1),
            ("webhook_redeliveries", 1),
            ("check_run_updates", 1),
            ("outbox_events", 1),
            // The attempt-2 row this test inserts above coexists with the
            // attempt-1 history row.
            ("run_history", 2),
            ("job_history", 1),
            ("attempt_history", 1),
            ("step_history", 1),
        ];
        for (table, expected) in counts {
            let found: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row.get(0))
                .unwrap();
            assert_eq!(found, expected, "row count changed for {table}");
        }
        let (status, blob, labels): (String, Vec<u8>, String) = conn
            .query_row(
                "SELECT r.status, ru.rsa_public_key, ru.labels FROM runs r \
                 JOIN runners ru ON ru.runner_id = 42 \
                 WHERE r.run_id = '11111111-1111-4111-8111-111111111111'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(status, "completed");
        assert_eq!(blob, vec![1, 2, 3, 4, 5]);
        assert!(labels.contains("self-hosted"));

        // Integrity of the upgraded file.
        let integrity: String = conn
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
        let violations: i64 = conn
            .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(violations, 0);

        // Auto-increment continues above the seeded ids (session messages
        // must stay above the broker request-id range).
        conn.execute(
            "INSERT INTO session_messages (session_id, message_type, created_at) \
             VALUES ('33333333-3333-4333-8333-333333333333', 'JobCancellation', 1759277100000000)",
            [],
        )
        .unwrap();
        let next_id: i64 = conn
            .query_row("SELECT MAX(message_id) FROM session_messages", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(next_id, 1_000_002);
    }

    /// A migration that fails mid-batch rolls back the whole batch and its
    /// ledger rows; the fixed retry then applies cleanly.
    #[test]
    fn failed_batch_rolls_back_and_retry_applies() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        let failing = vec![
            refinery::Migration::unapplied("V1__first", "CREATE TABLE t1 (id INTEGER);").unwrap(),
            refinery::Migration::unapplied(
                "V2__second",
                "INSERT INTO missing_table VALUES (1);",
            )
            .unwrap(),
        ];
        let runner = |migrations: &[refinery::Migration]| {
            let mut runner = refinery::Runner::new(migrations)
                .set_abort_divergent(true)
                .set_abort_missing(true)
                .set_grouped(true);
            runner.set_migration_table_name(LEDGER_TABLE);
            runner
        };
        let error = runner(&failing).run(&mut conn).unwrap_err();
        assert!(
            error.to_string().contains("missing_table"),
            "unexpected error: {error}"
        );
        let tables: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 't1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tables, 0, "the failed batch must roll back its DDL");
        let ledger: i64 = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM \"{LEDGER_TABLE}\""),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ledger, 0, "the failed batch must roll back its ledger rows");

        let fixed = vec![
            refinery::Migration::unapplied("V1__first", "CREATE TABLE t1 (id INTEGER);").unwrap(),
            refinery::Migration::unapplied(
                "V2__second",
                "CREATE TABLE t2 (id INTEGER); INSERT INTO t2 VALUES (1);",
            )
            .unwrap(),
        ];
        let report = runner(&fixed).run(&mut conn).unwrap();
        assert_eq!(report.applied_migrations().len(), 2);
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM t2", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 1);
    }

    /// A changed applied migration (checksum divergence) and an unknown
    /// applied version both refuse; the ledger is left unchanged.
    #[test]
    fn divergence_and_unknown_versions_refuse() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        initialize_empty_sqlite(&mut conn).unwrap();
        conn.execute(
            &format!(
                "UPDATE \"{LEDGER_TABLE}\" SET checksum = '0' WHERE version = ?1"
            ),
            [MIGRATIONS[0]],
        )
        .unwrap();
        let error = run_sqlite(&mut conn).unwrap_err();
        let message = error.to_string().to_lowercase();
        assert!(
            message.contains("divergent") || message.contains("different"),
            "unexpected error: {error}"
        );
        assert_eq!(
            sqlite_ledger(&conn).unwrap(),
            Ledger::Applied(MIGRATIONS.to_vec()),
            "the refused run must not rewrite the ledger"
        );

        // Unknown (newer) versions refuse both the runner and the guard.
        let mut newer = rusqlite::Connection::open_in_memory().unwrap();
        initialize_empty_sqlite(&mut newer).unwrap();
        newer
            .execute(
                &format!(
                    "INSERT INTO \"{LEDGER_TABLE}\" (version, name, applied_on, checksum) \
                     VALUES (999999999, 'future', '2026-10-05T00:00:00Z', '1')"
                ),
                [],
            )
            .unwrap();
        assert!(run_sqlite(&mut newer).is_err());
        let mut applied_with_future = MIGRATIONS.to_vec();
        applied_with_future.push(999_999_999);
        let error = check_applied(&applied_with_future).unwrap_err();
        assert!(error.contains("newer"), "{error}");
    }

    /// Explicit adoption: a pre-ledger control database verifies against the
    /// baseline shape, is stamped, and stops at the pending migration.
    #[test]
    fn adopt_pre_ledger_baseline() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(&baseline_sql(&sqlite_runner(Target::Latest)))
            .unwrap();
        conn.execute_batch(&fixture("seed_sqlite.sql")).unwrap();

        let probe = probe_shape_sqlite(&conn).unwrap();
        assert_eq!(probe.version, MIGRATIONS[0]);
        let applied = adopt_sqlite(&mut conn, probe.version).unwrap();
        assert_eq!(applied, MIGRATIONS[1..].to_vec());
        verify_sqlite(&conn).unwrap();

        // An unrecognizable database is never adopted.
        let junk = rusqlite::Connection::open_in_memory().unwrap();
        junk.execute_batch(
            "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value BLOB);
             CREATE TABLE not_the_schema (id INTEGER);",
        )
        .unwrap();
        assert!(probe_shape_sqlite(&junk).is_err());
    }

    /// Brand-new prep (the zero-config first-run path) initializes only an
    /// absent/empty SQLite store and never touches an existing one.
    #[tokio::test]
    async fn brand_new_preparation_is_create_only() {
        let _guard = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _env = crate::state::TestEnvVar::unset(crate::store::STORE_URL_ENV);
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        assert!(
            crate::store_admin::prepare_brand_new_local(&state_dir).unwrap(),
            "an absent store is initialized"
        );
        let path = state_dir.join("preloop.db");
        let conn = rusqlite::Connection::open(&path).unwrap();
        verify_sqlite(&conn).unwrap();
        drop(conn);
        assert!(
            !crate::store_admin::prepare_brand_new_local(&state_dir).unwrap(),
            "an existing store is never re-initialized"
        );
    }

    // ── PostgreSQL ──────────────────────────────────────────────────────

    async fn pg_client(url: &str) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
            .await
            .unwrap_or_else(|error| panic!("connect {url}: {error}"));
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute("SET search_path TO control")
            .await
            .ok();
        client
    }

    /// The Postgres arm of the gate: fresh init + verify, an upgrade from a
    /// populated baseline fixture, adoption of a pre-ledger baseline, and the
    /// refusal paths. Skips (with a notice) when no server is configured —
    /// the control-plane workflow sets `PRELOOP_TEST_POSTGRES_URL`, so the
    /// zero-PG guard in CI fails if this never runs there.
    #[tokio::test]
    async fn postgres_migrations() {
        let Some((_db, url)) = crate::test_pg::fresh_database().await else {
            assert!(
                std::env::var("PRELOOP_TEST_REQUIRE_POSTGRES").is_err(),
                "{} must name a Postgres server (PRELOOP_TEST_REQUIRE_POSTGRES is set)",
                crate::test_pg::TEST_POSTGRES_URL_ENV
            );
            eprintln!(
                "skipping: set {} to a Postgres server",
                crate::test_pg::TEST_POSTGRES_URL_ENV
            );
            return;
        };

        // Fresh initialize + verify.
        let mut client = pg_client(&url).await;
        initialize_empty_postgres(&mut client).await.unwrap();
        crate::control::migrations::verify_postgres(&client)
            .await
            .unwrap();
        let ledger: Vec<i32> = client
            .query(
                "SELECT version FROM control.refinery_schema_history ORDER BY version",
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(ledger, MIGRATIONS);

        // Populated baseline upgrade (a fresh database for the old store).
        let Some((_db2, url2)) = crate::test_pg::fresh_database().await else {
            return;
        };
        let mut old = pg_client(&url2).await;
        old.batch_execute(&baseline_sql(&postgres_runner(Target::Latest)))
            .await
            .unwrap();
        old.batch_execute(&fixture("seed_postgres.sql"))
            .await
            .unwrap();
        assert_eq!(
            crate::control::migrations::postgres_ledger(&old).await.unwrap(),
            Ledger::Unledgered
        );
        let applied = run_postgres(&mut old).await.unwrap();
        assert_eq!(applied, MIGRATIONS[1..].to_vec());
        crate::control::migrations::verify_postgres(&old).await.unwrap();
        let index: i64 = old
            .query_one(
                "SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'control' \
                 AND indexname = 'runs_fork_approval_sweep'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(index, 1);
        for (table, expected) in [
            ("namespaces", 2i64),
            ("runs", 2),
            ("jobs", 3),
            ("job_specs", 3),
            ("session_messages", 1),
            ("webhook_deliveries", 2),
            ("run_history", 1),
            ("job_history", 1),
            ("attempt_history", 1),
            ("step_history", 1),
        ] {
            let found: i64 = old
                .query_one(&format!("SELECT COUNT(*) FROM control.{table}"), &[])
                .await
                .unwrap()
                .get(0);
            assert_eq!(found, expected, "row count changed for {table}");
        }
        // History rows are attempt-1 after the rerun-key migration, the
        // environment objects exist, and the pre-existing job gained NULL
        // deployment columns.
        let checks: Vec<(i64, i64)> = old
            .query(
                "SELECT (SELECT run_attempt FROM control.run_history), \
                        (SELECT run_attempt FROM control.job_history) \
                 UNION ALL \
                 SELECT (SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'control' \
                         AND indexname = 'environment_approvals_gate'), \
                        (SELECT COUNT(*) FROM information_schema.columns \
                         WHERE table_schema = 'control' AND table_name = 'jobs' \
                         AND column_name IN ('deployment_id', 'environment_url'))",
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        assert_eq!(checks, vec![(1, 1), (1, 2)]);

        // Identity sequences continue above the seeded ids.
        old.batch_execute(
            "INSERT INTO control.session_messages (session_id, message_type, created_at) \
             VALUES ('33333333-3333-4333-8333-333333333333', 'JobCancellation', now())",
        )
        .await
        .unwrap();
        let next: i64 = old
            .query_one("SELECT MAX(message_id) FROM control.session_messages", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(next, 1_000_002);

        // Adoption of a pre-ledger baseline: shape probe + explicit stamp.
        let Some((_db3, url3)) = crate::test_pg::fresh_database().await else {
            return;
        };
        let mut legacy = pg_client(&url3).await;
        legacy
            .batch_execute(&baseline_sql(&postgres_runner(Target::Latest)))
            .await
            .unwrap();
        legacy
            .batch_execute(&fixture("seed_postgres.sql"))
            .await
            .unwrap();
        let probe = probe_shape_postgres(&mut legacy).await.unwrap();
        assert_eq!(probe.version, MIGRATIONS[0]);
        let applied = adopt_postgres(&mut legacy, probe.version).await.unwrap();
        assert_eq!(applied, MIGRATIONS[1..].to_vec());
        crate::control::migrations::verify_postgres(&legacy)
            .await
            .unwrap();

        // A database whose shape does not match any known schema refuses.
        let Some((_db4, url4)) = crate::test_pg::fresh_database().await else {
            return;
        };
        let mut junk = pg_client(&url4).await;
        junk.batch_execute(
            "CREATE SCHEMA control; \
             CREATE TABLE control.schema_meta (key text PRIMARY KEY, value bytea); \
             CREATE TABLE control.not_the_schema (id integer);",
        )
        .await
        .unwrap();
        assert!(probe_shape_postgres(&mut junk).await.is_err());
    }
}
