# Control database migrations

The control schema is created and upgraded by **refinery 0.10** over the
embedded migration sets in `migrations/{sqlite,postgres}`. `preloop serve`
checks the applied migration ledger against the versions expected by this build
and refuses an incompatible schema; a serving process never migrates a store
that already exists. The one thing it creates is a **brand-new** (absent or
empty) local SQLite store, which it initializes through the embedded migration
set so a fresh install serves with no extra step. Checksum validation is
performed when migrations are applied, not as a full historical SQL audit on
every server open. There is no down/rollback: refinery is forward-only, so
rollback is a backup restore or a forward corrective migration.

## Ledger

- Table `refinery_schema_history` (refinery's schema history; the name is
  pinned by `control::migrate_runner`): `version int4 PRIMARY KEY`,
  `name`, `applied_on`, `checksum`.
- The ledger is the **sole version authority**. `schema_meta` stays for
  `key_fingerprint` only; a stale pre-ledger `schema_version` row is ignored
  and retired by `--adopt-baseline`.
- Checksums are refinery SipHash13 over name/version/SQL. Never edit an
  applied migration: `abort_divergent` fails a changed file. Never renumber.

## Files

```
migrations/sqlite/V<version>__<name>.sql
migrations/postgres/V<version>__<name>.sql
```

- `<version>` is a 10-digit `YYYYMMDDNN` stamp that fits refinery's default
  `int4` version type (the workspace enables refinery without
  `int8-versions`). Pick a stamp later than every existing one; siblings add
  their own without renumbering.
- `<name>` is `\w+` (no dashes). The file is plain up-only SQL (no markers, no
  down section). PG files start with `SET search_path = control;`.
- Files are applied in one transaction per run (refinery grouped mode): a
  failure rolls back the whole batch and its ledger rows.
- Versions this build ships are listed in `control/migrations.rs`
  (`MIGRATIONS`); a test fails if the files and the const drift, per dialect.

## Adding a migration

1. Add the file to **both** dialect directories (SQLite gets the translated
   DDL; `ALTER TABLE` appends columns at the end, so the fresh
   `control/lite/schema.sql` must list new columns last).
2. Add the same DDL to `crates/preloop-runner-server/src/control/lite/schema.sql`,
   `crates/preloop-runner-server/src/control/pg/schema.sql` **and**
   `docs/control-schema.sql` (two executable parity tests enforce this).
3. Append the version to `MIGRATIONS` in
   `crates/preloop-runner-server/src/control/migrations.rs`.
4. Keep it additive and data-preserving. For a lossy change, write a forward
   corrective migration instead of a down; operators restore the pre-migration
   backup.
5. Run `just test-control-migrations` (SQLite always; Postgres with
   `PRELOOP_TEST_POSTGRES_URL`).

Populated fixtures live in `fixtures/control-migrations/seed_{sqlite,postgres}.sql`.
They are the "previous release" data: the gate applies them to a
baseline-migration database, upgrades with the real runner and asserts every
row survives, identities continue above seeded ids, and the upgraded schema is
structurally identical to a fresh run.

## Operating

- `preloop store migrate` — initialize a fresh store or apply pending
  migrations. For an existing, non-empty SQLite store with pending migrations,
  the command creates a consistent backup first
  (`VACUUM INTO <db>.pre-migrate-<unix>.bak`); fresh stores and runs with no
  pending migrations do not need that backup. Restore the backup to roll back.
  Postgres restores from the operator's backup. `--dry-run` does not apply
  migration SQL, but opens SQLite read/write and may create an empty database
  or normalize file permissions, so it is not a strictly read-only probe.
  `--adopt-baseline` adopts a pre-ledger control database **after** verifying
  its structure matches a known migration point (never silently).
- `preloop store status` — reports applied / pending / unknown versions and
  does not apply migration SQL. Opening SQLite can create the database file
  (and parent directory) and normalize its permissions, so it is not a
  strictly read-only filesystem probe.
- `preloop serve` initializes a **brand-new** local SQLite store (absent or
  empty) through the embedded migration set, and otherwise **never** migrates:
  an older, newer or divergent database refuses with `preloop store migrate`
  in the message. A legacy store (`PRAGMA user_version` 11 /
  `schema_migrations`, or the Postgres `public` legacy tables) refuses with
  `preloop store import-legacy --source <old> --target <new>`. A Postgres
  store is never initialized here.
- Other local setup paths initialize a missing SQLite store the same way:
  `preloop init` run in the foreground, and `preloop server install` (as the
  service account). Provision and upgrade stores explicitly with
  `preloop store migrate`; existing stores are not upgraded implicitly.

## CI

`just test-ci` runs `just test-control-migrations`. The control-plane
workflow runs the same suite inside its `postgres: [16, 17, 18]` matrix with
SQLite in every leg; `PRELOOP_TEST_REQUIRE_POSTGRES=1` turns a missing
Postgres server into a failure, and a nextest list guard fails the job if the
migration tests (including the Postgres arm) are not present.
