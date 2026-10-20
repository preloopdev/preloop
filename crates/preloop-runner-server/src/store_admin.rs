//! Control-store administration: the public surface behind
//! `preloop store migrate|status` and the brand-new local preparation.
//!
//! The server crate owns this so the CLI needs no database driver of its
//! own. The policy is the same everywhere:
//!
//! - a serving process never writes schema state — it only verifies
//!   (`control::migrations`);
//! - `migrate` (refinery over the embedded migrations) is the one writer:
//!   it creates a fresh database, applies pending migrations in a
//!   transaction, and refuses legacy stores, foreign databases and
//!   pre-ledger control schemas unless `--adopt-baseline` asks for the
//!   shape-verified adoption;
//! - an existing database is never auto-upgraded by the server or by
//!   first-run preparation: only an explicit `store migrate` applies
//!   migrations to a database that already exists;
//! - SQLite migrations take a consistent pre-migration backup first
//!   (`VACUUM INTO`), the rollback path since refinery has no down.

use crate::control::migrate_runner;
use crate::control::migrations::{self, Ledger, MIGRATIONS};
use crate::store::{STORE_URL_ENV, StoreUrl, parse_store_url};
use anyhow::Context;
use std::path::{Path, PathBuf};

/// Where the control store lives (the same grammar as `preloop serve
/// --store` / `PRELOOP_STORE_URL`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreTarget {
    /// `sqlite://<path>` or a bare path.
    Sqlite(PathBuf),
    /// `postgres://…`.
    Postgres(String),
}

impl StoreTarget {
    /// Resolve an explicit `--store`, else `PRELOOP_STORE_URL`, else SQLite at
    /// `<state_dir>/preloop.db` — the same precedence `preloop serve` uses.
    pub fn resolve(store: Option<&str>, state_dir: &Path) -> anyhow::Result<StoreTarget> {
        let raw = match store {
            Some(value) if !value.trim().is_empty() => value.to_owned(),
            _ => std::env::var(STORE_URL_ENV).unwrap_or_default(),
        };
        match parse_store_url(&raw)? {
            StoreUrl::Sqlite(path) => {
                let path = if path.as_os_str().is_empty() {
                    state_dir.join("preloop.db")
                } else {
                    path
                };
                Ok(StoreTarget::Sqlite(path))
            }
            StoreUrl::Postgres(url) => Ok(StoreTarget::Postgres(url)),
        }
    }

    /// Human description for CLI output.
    pub fn describe(&self) -> String {
        match self {
            StoreTarget::Sqlite(path) => format!("sqlite:{}", path.display()),
            StoreTarget::Postgres(url) => url.clone(),
        }
    }
}

/// What a `migrate` run should do beyond the defaults.
#[derive(Debug, Clone)]
pub struct MigrateOptions {
    /// Adopt a pre-ledger control database after verifying its shape
    /// (an unreleased build: SQLite v4 / Postgres v5), then apply the
    /// pending migrations.
    pub adopt_baseline: bool,
    /// Take the pre-migration SQLite backup (default on; set false to skip).
    pub backup: bool,
    /// Report what would happen without changing anything.
    pub dry_run: bool,
}

impl MigrateOptions {
    /// Defaults for the CLI: backup on.
    pub fn new() -> Self {
        Self {
            adopt_baseline: false,
            backup: true,
            dry_run: false,
        }
    }
}

impl Default for MigrateOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// What a `migrate` run did (or would do, for `dry_run`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrateReport {
    /// Versions applied by this run (empty when already at latest).
    pub applied: Vec<i32>,
    /// The version the database was adopted at, when `--adopt-baseline` ran.
    pub adopted: Option<i32>,
    /// Pending versions seen before the run.
    pub pending_before: Vec<i32>,
    /// The SQLite backup written before migrating, when one was taken.
    pub backup: Option<PathBuf>,
}

/// Read-only view of a control store's migration state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreStatus {
    /// `sqlite:<path>` or the Postgres URL.
    pub target: String,
    /// `initialized`, `uninitialized`, `legacy`, `foreign`, `pre-ledger`, or
    /// `incomplete` (ledger without `schema_meta`).
    pub state: &'static str,
    /// Versions recorded in the ledger, ascending.
    pub applied: Vec<i32>,
    /// Expected versions not yet applied.
    pub pending: Vec<i32>,
    /// Applied versions this build does not ship (a newer preloop).
    pub unknown: Vec<i32>,
}

impl StoreStatus {
    /// Whether the store is exactly this build's migration set.
    pub fn is_current(&self) -> bool {
        self.pending.is_empty() && self.unknown.is_empty() && self.state == "initialized"
    }
}

fn ledger_state_name(ledger: &Ledger) -> &'static str {
    match ledger {
        Ledger::Empty => "uninitialized",
        Ledger::Legacy => "legacy",
        Ledger::Foreign => "foreign",
        Ledger::Unledgered => "pre-ledger",
        Ledger::MissingMeta => "incomplete",
        Ledger::Applied(_) => "initialized",
    }
}

fn split_versions(applied: &[i32]) -> (Vec<i32>, Vec<i32>) {
    let pending = MIGRATIONS
        .iter()
        .copied()
        .filter(|version| !applied.contains(version))
        .collect();
    let unknown = applied
        .iter()
        .copied()
        .filter(|version| !MIGRATIONS.contains(version))
        .collect();
    (pending, unknown)
}

/// Initialize or upgrade the control store (see the module docs). Never
/// adopts or upgrades unless asked: `adopt_baseline` is explicit, and an
/// existing older database is only moved forward by this explicit command.
pub async fn migrate(
    target: StoreTarget,
    options: MigrateOptions,
) -> anyhow::Result<MigrateReport> {
    match target {
        StoreTarget::Sqlite(path) => {
            tokio::task::spawn_blocking(move || migrate_sqlite(&path, &options)).await?
        }
        StoreTarget::Postgres(url) => migrate_postgres(&url, &options).await,
    }
}

/// Report the store's migration state without changing anything.
pub async fn status(target: StoreTarget) -> anyhow::Result<StoreStatus> {
    match target {
        StoreTarget::Sqlite(path) => {
            tokio::task::spawn_blocking(move || status_sqlite(&path)).await?
        }
        StoreTarget::Postgres(url) => status_postgres(&url).await,
    }
}

/// First-run preparation for a local install: initialize ONLY a brand-new
/// (absent or empty) SQLite control database, so a fresh `PRELOOP_HOME`
/// serves with no extra step. An existing database — any state other than
/// empty — is left untouched: upgrading is `preloop store migrate`, never
/// implicit.
///
/// Returns whether the database was created. A Postgres store is never
/// initialized here (the operator runs `store migrate`). Synchronous on
/// purpose: the installer calls it outside any runtime, and the work is one
/// file check plus (only on a fresh install) the embedded migrations.
///
/// `store_url` is the caller's explicit selection (`preloop serve --store`);
/// `None` falls back to `PRELOOP_STORE_URL` and then to `<state_dir>/preloop.db`.
/// That is [`StoreTarget::resolve`]'s precedence — the one the boot itself
/// uses — so the preparation initializes the database the server will open:
/// prepared against anything else, `preloop serve --store sqlite:///elsewhere.db`
/// on a fresh home would create the state-dir default and then refuse the
/// empty store it was pointed at.
///
/// The database path is never followed: a symlink there would aim the
/// create, the migration and the chmod at its target, so one is refused when
/// the preparation would write through it (a link to an existing store is
/// simply nothing to prepare). A privileged caller (euid 0) additionally
/// requires the parent directory to be exclusively its own: a root-owned
/// create inside a directory the service account can rewrite is the same
/// attack one level up. `preloop server install` no longer takes that path —
/// it runs the preparation as the service account itself, so root never
/// creates, writes or chmods anything inside the service-owned tree.
pub fn prepare_brand_new_local(state_dir: &Path, store_url: Option<&str>) -> anyhow::Result<bool> {
    let StoreTarget::Sqlite(path) = StoreTarget::resolve(store_url, state_dir)? else {
        return Ok(false);
    };
    // Brand-new means absent or empty, judged with `symlink_metadata` (never
    // `exists`/`metadata`, which follow links — the very move this refuses):
    // a dangling link reads as absent, and an empty root-owned target reads
    // as empty, either of which would hand the create below to the link.
    match std::fs::symlink_metadata(&path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            if std::fs::metadata(&path).is_ok_and(|target| target.is_file() && target.len() > 0) {
                // An existing store through a link: nothing to prepare.
                return Ok(false);
            }
            anyhow::bail!(
                "refusing to initialize {}: it is a symlink; remove it, or select \
                 the real path (`--store` / {STORE_URL_ENV})",
                path.display()
            );
        }
        Ok(meta) if !meta.is_file() => anyhow::bail!(
            "refusing to initialize {}: it is not a regular file",
            path.display()
        ),
        Ok(meta) if meta.len() > 0 => return Ok(false),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).with_context(|| format!("inspect {}", path.display())),
    }
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        prepare_parent_directory(parent)?;
    }
    let mut conn = open_sqlite(&path)?;
    if !matches!(migrations::sqlite_ledger(&conn)?, Ledger::Empty) {
        return Ok(false);
    }
    match migrate_runner::initialize_empty_sqlite(&mut conn) {
        Ok(()) => Ok(true),
        Err(error) => {
            // Another process may have initialized it concurrently; only a
            // now-complete ledger turns that into success.
            if let Ok(Ledger::Applied(versions)) = migrations::sqlite_ledger(&conn)
                && migrations::check_applied(&versions).is_ok()
            {
                return Ok(false);
            }
            Err(error)
        }
    }
}

// ── SQLite ──────────────────────────────────────────────────────────────

/// Create the database's parent directory (if needed). A privileged caller
/// (euid 0) is held to an exclusive tree: the create, the write and the
/// chmod below must not land anywhere another account can steer them with a
/// symlink or a directory it can rewrite.
// SAFETY: `geteuid` takes no arguments and only reads the process's
// effective uid; there is no other way to ask "am I privileged?" and the
// answer only selects a stricter check.
#[allow(unsafe_code)]
fn prepare_parent_directory(parent: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    if unsafe { libc::geteuid() } == 0 {
        return guarded_create_directory(parent);
    }
    std::fs::create_dir_all(parent)?;
    Ok(())
}

/// [`prepare_parent_directory`] for a privileged caller: refuse an existing
/// directory that is not exclusively this account's own, create it, then
/// check what exists now (`create_dir_all` follows a symlink).
// SAFETY: `geteuid` as in [`prepare_parent_directory`].
#[allow(unsafe_code)]
#[cfg(unix)]
fn guarded_create_directory(parent: &Path) -> anyhow::Result<()> {
    // `uid_t` is `u32` on every unix target this crate builds for.
    let uid = unsafe { libc::geteuid() };
    match std::fs::symlink_metadata(parent) {
        Ok(_) => exclusive_directory(parent, uid)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| format!("inspect {}", parent.display()));
        }
    }
    std::fs::create_dir_all(parent)?;
    exclusive_directory(parent, uid)
}

/// A directory a privileged preparation writes into must be a real directory
/// owned by the preparing uid and writable by no one else: anything else can
/// place a symlink between the check and the write, which is how a
/// service-owned state tree aims a root-owned create at a target of its
/// choosing.
#[cfg(unix)]
fn exclusive_directory(dir: &Path, uid: u32) -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let meta =
        std::fs::symlink_metadata(dir).with_context(|| format!("inspect {}", dir.display()))?;
    anyhow::ensure!(
        meta.is_dir() && meta.uid() == uid && meta.mode() & 0o022 == 0,
        "refusing to initialize the control database in {} (owner uid {}, mode {:o}): \
         the directory is not exclusively this process's own; run the preparation as \
         the account that owns the store",
        dir.display(),
        meta.uid(),
        meta.mode() & 0o7777,
    );
    Ok(())
}

/// Owner-only, matching every other state artifact the engine writes.
fn private_file(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    let _ = path;
    Ok(())
}

fn open_sqlite(path: &Path) -> anyhow::Result<rusqlite::Connection> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    // SQLite's own open never follows a symlink at the database itself (its
    // unix VFS passes O_NOFOLLOW); `prepare_brand_new_local` refuses one
    // earlier so the error names the path instead of SQLite's.
    let conn = rusqlite::Connection::open(path)?;
    private_file(path)?;
    conn.pragma_update(None, "foreign_keys", true)?;
    conn.pragma_update(None, "busy_timeout", 5000_i64)?;
    Ok(conn)
}

/// Refuse states a migration run must not touch. Returns the adoption point
/// when `--adopt-baseline` applies, whether the database is brand-new, and
/// the pending versions.
fn sqlite_preflight(
    conn: &rusqlite::Connection,
    options: &MigrateOptions,
) -> anyhow::Result<Preflight> {
    let ledger = migrations::sqlite_ledger(conn)?;
    match ledger {
        Ledger::Empty => {
            anyhow::ensure!(
                !options.adopt_baseline,
                "nothing to adopt: the database is empty; run `preloop store migrate` \
                 without --adopt-baseline"
            );
            Ok(Preflight {
                adopted: None,
                fresh: true,
                pending: MIGRATIONS.to_vec(),
            })
        }
        Ledger::Applied(applied) => {
            anyhow::ensure!(
                !options.adopt_baseline,
                "nothing to adopt: the database already has a migration ledger"
            );
            let (pending, unknown) = split_versions(&applied);
            anyhow::ensure!(
                unknown.is_empty(),
                "control database was migrated by a newer preloop build (unknown migration(s) {}); \
                 upgrade preloop instead of migrating",
                unknown
                    .iter()
                    .map(i32::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            Ok(Preflight {
                adopted: None,
                fresh: false,
                pending,
            })
        }
        Ledger::Unledgered => {
            anyhow::ensure!(
                options.adopt_baseline,
                "{}",
                migrations::refusal(Ledger::Unledgered)
            );
            let probe = migrate_runner::probe_shape_sqlite(conn)?;
            Ok(Preflight {
                adopted: Some(probe.version),
                fresh: false,
                pending: MIGRATIONS.to_vec(),
            })
        }
        other => anyhow::bail!("{}", migrations::refusal(other)),
    }
}

/// What a migration run will do: the adoption point (if any), whether the
/// database is brand-new, and the versions pending before the run.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Preflight {
    adopted: Option<i32>,
    fresh: bool,
    pending: Vec<i32>,
}

fn backup_path(path: &Path, unix_seconds: u64) -> PathBuf {
    PathBuf::from(format!("{}.pre-migrate-{unix_seconds}.bak", path.display()))
}

fn migrate_sqlite(path: &Path, options: &MigrateOptions) -> anyhow::Result<MigrateReport> {
    let existed = path.exists() && std::fs::metadata(path)?.len() > 0;
    let mut conn = open_sqlite(path)?;
    let preflight = sqlite_preflight(&conn, options)?;
    if options.dry_run {
        return Ok(MigrateReport {
            applied: Vec::new(),
            adopted: preflight.adopted,
            pending_before: preflight.pending,
            backup: None,
        });
    }
    let backup = if options.backup && existed && !preflight.pending.is_empty() {
        let unix_seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or_default();
        let backup = backup_path(path, unix_seconds);
        // `VACUUM INTO` takes a consistent snapshot of the live database.
        let sql = format!(
            "VACUUM INTO '{}'",
            backup.display().to_string().replace('\'', "''")
        );
        conn.execute_batch(&sql)?;
        Some(backup)
    } else {
        None
    };
    let applied = match preflight.adopted {
        Some(through) => migrate_runner::adopt_sqlite(&mut conn, through)?,
        None => migrate_runner::run_sqlite(&mut conn)?,
    };
    Ok(MigrateReport {
        applied,
        adopted: preflight.adopted,
        pending_before: preflight.pending,
        backup,
    })
}

fn status_sqlite(path: &Path) -> anyhow::Result<StoreStatus> {
    // Read-only: never create the file just to report on it.
    let absent = !path.exists() || std::fs::metadata(path)?.len() == 0;
    if absent {
        return Ok(StoreStatus {
            target: format!("sqlite:{}", path.display()),
            state: "uninitialized",
            applied: Vec::new(),
            pending: MIGRATIONS.to_vec(),
            unknown: Vec::new(),
        });
    }
    let conn = open_sqlite(path)?;
    let ledger = migrations::sqlite_ledger(&conn)?;
    let applied = match &ledger {
        Ledger::Applied(versions) => versions.clone(),
        _ => Vec::new(),
    };
    let (pending, unknown) = split_versions(&applied);
    Ok(StoreStatus {
        target: format!("sqlite:{}", path.display()),
        state: ledger_state_name(&ledger),
        applied,
        pending,
        unknown,
    })
}

// ── Postgres ────────────────────────────────────────────────────────────

/// Deadline for a Postgres connection to become usable: TCP connect, startup
/// packet, TLS, authentication, and session setup.
///
/// `preloop store` is an operator command, so a database that accepts the
/// socket and then stops talking used to park the operator's terminal with no
/// way out. `tokio-postgres`'s own `connect_timeout` bounds only the TCP
/// connect, not the handshake after it, which is why both are set here. A
/// healthy local or LAN database completes all of this in milliseconds, so 10 s
/// is generous; it is the same 10 s value the serving pool uses
/// (`store_pg::CONNECT_TIMEOUT`, owned by the Postgres slice) so the two paths
/// stay consistent until they are unified.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

async fn postgres_client(url: &str) -> anyhow::Result<tokio_postgres::Client> {
    postgres_client_within(url, CONNECT_TIMEOUT).await
}

/// [`postgres_client`] with an explicit usable-connection deadline, so a test
/// can prove that a stalled database fails instead of hanging.
///
/// *One* deadline covers the whole sequence — TCP connect, startup handshake,
/// authentication and session setup — because the connection is unusable until
/// the last of them finishes. Bounding each phase with a fresh copy of the
/// deadline would multiply it; every phase instead gets [`remaining`].
async fn postgres_client_within(
    url: &str,
    deadline: std::time::Duration,
) -> anyhow::Result<tokio_postgres::Client> {
    let started = std::time::Instant::now();
    let connect_url = crate::store_pg::connect_url(url);
    // Parse once so `connect_timeout` (the TCP connect) is set on the driver
    // config too; the `timeout` below covers the handshake that follows it.
    let mut config: tokio_postgres::Config = connect_url
        .parse()
        .context("parsing the Postgres connection string")?;
    config.connect_timeout(deadline);
    let client = match crate::store_pg::tls_connector(url)? {
        Some(tls) => {
            let (client, connection) =
                tokio::time::timeout(remaining(deadline, started), config.connect(tls))
                    .await
                    .map_err(|_| postgres_timeout("complete the connection handshake", deadline))?
                    .context("connecting to Postgres")?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
        }
        None => {
            let (client, connection) = tokio::time::timeout(
                remaining(deadline, started),
                config.connect(tokio_postgres::NoTls),
            )
            .await
            .map_err(|_| postgres_timeout("complete the connection handshake", deadline))?
            .context("connecting to Postgres")?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
        }
    };
    // `search_path` naming a not-yet-created schema is fine: the runner's
    // baseline creates `control` first, and every admin read is qualified.
    //
    // Session setup is part of "usable", so it is part of the same deadline:
    // a database that authenticates and then stops answering used to pass the
    // handshake bound and hang here, which parked `preloop store` exactly the
    // way the bound exists to prevent — one step later.
    tokio::time::timeout(
        remaining(deadline, started),
        client.batch_execute("SET search_path TO control"),
    )
    .await
    .map_err(|_| postgres_timeout("finish session setup (`SET search_path`)", deadline))?
    .context("setting the session's search_path")?;
    Ok(client)
}

/// The part of `deadline` that is still unspent at `started`.
///
/// One Postgres deadline covers several phases, so each phase must be bounded
/// by what is left of it rather than by a fresh copy.
fn remaining(deadline: std::time::Duration, started: std::time::Instant) -> std::time::Duration {
    deadline.saturating_sub(started.elapsed())
}

/// The error a Postgres wait reports when its deadline expires.
///
/// It names the dependency, the phase that stalled, and the deadline, so an
/// operator can tell a database that accepted the socket and stopped answering
/// from one that refused the connection or rejected the credentials.
fn postgres_timeout(stage: &str, deadline: std::time::Duration) -> anyhow::Error {
    anyhow::anyhow!(
        "Postgres did not {stage} within {deadline:?}: the database accepted the socket but is \
         not answering, so the wait was abandoned rather than left hanging"
    )
}

async fn postgres_preflight(
    client: &mut tokio_postgres::Client,
    options: &MigrateOptions,
) -> anyhow::Result<Preflight> {
    let ledger = migrations::postgres_ledger(client).await?;
    match ledger {
        Ledger::Empty => {
            anyhow::ensure!(
                !options.adopt_baseline,
                "nothing to adopt: the database has no control schema; run \
                 `preloop store migrate` without --adopt-baseline"
            );
            Ok(Preflight {
                adopted: None,
                fresh: true,
                pending: MIGRATIONS.to_vec(),
            })
        }
        Ledger::Applied(applied) => {
            anyhow::ensure!(
                !options.adopt_baseline,
                "nothing to adopt: the database already has a migration ledger"
            );
            let (pending, unknown) = split_versions(&applied);
            anyhow::ensure!(
                unknown.is_empty(),
                "control database was migrated by a newer preloop build (unknown migration(s) {}); \
                 upgrade preloop instead of migrating",
                unknown
                    .iter()
                    .map(i32::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            Ok(Preflight {
                adopted: None,
                fresh: false,
                pending,
            })
        }
        Ledger::Unledgered => {
            anyhow::ensure!(
                options.adopt_baseline,
                "{}",
                migrations::refusal(Ledger::Unledgered)
            );
            let probe = migrate_runner::probe_shape_postgres(client).await?;
            Ok(Preflight {
                adopted: Some(probe.version),
                fresh: false,
                pending: MIGRATIONS.to_vec(),
            })
        }
        other => anyhow::bail!("{}", migrations::refusal(other)),
    }
}

/// How long `preloop store migrate` waits for another administrator's migration
/// lock before giving up.
///
/// The lock is held for a whole migration — DDL plus backfills — which on a
/// large database is minutes, so the wait has to outlast real work. 10 minutes
/// covers that while replacing the previous silent, indefinite wait with an
/// error that names the lock and says who to look for.
const MIGRATION_LOCK_WAIT: std::time::Duration = std::time::Duration::from_secs(600);

/// How often the bounded migration-lock wait retries `pg_try_advisory_lock`.
///
/// A poll rather than a blocking `pg_advisory_lock`: polling is what makes the
/// wait cancellable at the deadline. Two seconds keeps the retries cheap.
const MIGRATION_LOCK_RETRY: std::time::Duration = std::time::Duration::from_secs(2);

async fn migrate_postgres(url: &str, options: &MigrateOptions) -> anyhow::Result<MigrateReport> {
    migrate_postgres_within(url, options, MIGRATION_LOCK_WAIT).await
}

/// [`migrate_postgres`] with an explicit lock-wait deadline, so a test can hold
/// the lock elsewhere and prove the second admin fails instead of hanging.
async fn migrate_postgres_within(
    url: &str,
    options: &MigrateOptions,
    lock_wait: std::time::Duration,
) -> anyhow::Result<MigrateReport> {
    let mut client = postgres_client(url).await?;
    // `pg_advisory_lock` waits forever when another session holds the lock, so
    // a concurrently running migration (or a stale session) parked a second
    // `preloop store migrate` indefinitely. Trying, then giving up at a named
    // deadline, turns that into an actionable error. The lock stays
    // session-level, so `initialize_empty_postgres`'s own `pg_advisory_lock`
    // for the same id re-enters instead of blocking.
    let started = std::time::Instant::now();
    loop {
        // The deadline is checked *before* the probe, not after it: against a
        // healthy database the probe answers in milliseconds, so reaching the
        // deadline here means another session holds the lock, and that is what
        // the error below must name.
        anyhow::ensure!(
            started.elapsed() < lock_wait,
            "another Preloop administrator is holding the migration lock (Postgres advisory \
             lock {}): waited {lock_wait:?} for it and gave up rather than hang. Wait for the \
             other `preloop store migrate` to finish, then retry; `SELECT * FROM pg_locks WHERE \
             locktype = 'advisory'` shows the holder.",
            migrate_runner::POSTGRES_SETUP_LOCK
        );
        let left = remaining(lock_wait, started);
        // Each probe carries what is left of the lock wait. Polling bounds
        // *contention*, but a database that stops answering mid-probe never
        // returns from this `.await`, so the elapsed-time check above is never
        // reached and the command hangs past its advertised deadline.
        let acquired: bool = tokio::time::timeout(
            left,
            client.query_one(
                &format!(
                    "SELECT pg_try_advisory_lock({})",
                    migrate_runner::POSTGRES_SETUP_LOCK
                ),
                &[],
            ),
        )
        .await
        .map_err(|_| postgres_timeout("answer the migration-lock probe", lock_wait))?
        .context("taking the migration lock")?
        .get(0);
        if acquired {
            break;
        }
        // Never sleep past the deadline: the loop's first act is the check.
        tokio::time::sleep(MIGRATION_LOCK_RETRY.min(left)).await;
    }
    let result = migrate_postgres_locked(&mut client, options).await;
    let _ = client
        .batch_execute(&format!(
            "SELECT pg_advisory_unlock({})",
            migrate_runner::POSTGRES_SETUP_LOCK
        ))
        .await;
    result
}

async fn migrate_postgres_locked(
    client: &mut tokio_postgres::Client,
    options: &MigrateOptions,
) -> anyhow::Result<MigrateReport> {
    let preflight = postgres_preflight(client, options).await?;
    if options.dry_run {
        return Ok(MigrateReport {
            applied: Vec::new(),
            adopted: preflight.adopted,
            pending_before: preflight.pending,
            backup: None,
        });
    }
    let applied = match preflight.adopted {
        Some(through) => migrate_runner::adopt_postgres(client, through).await?,
        None if preflight.fresh => {
            migrate_runner::initialize_empty_postgres(client).await?;
            MIGRATIONS.to_vec()
        }
        None => migrate_runner::run_postgres(client).await?,
    };
    Ok(MigrateReport {
        applied,
        adopted: preflight.adopted,
        pending_before: preflight.pending,
        backup: None,
    })
}

async fn status_postgres(url: &str) -> anyhow::Result<StoreStatus> {
    let client = postgres_client(url).await?;
    let ledger = migrations::postgres_ledger(&client).await?;
    let applied = match &ledger {
        Ledger::Applied(versions) => versions.clone(),
        _ => Vec::new(),
    };
    let (pending, unknown) = split_versions(&applied);
    Ok(StoreStatus {
        target: url.to_owned(),
        state: ledger_state_name(&ledger),
        applied,
        pending,
        unknown,
    })
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    /// A privileged preparation writes only into a directory exclusively its
    /// own: the owner must be the preparing uid and neither group nor others
    /// may write. A directory owned by another account — the service-owned
    /// state tree of a previous install — is the case the guard exists for.
    #[test]
    fn a_privileged_preparation_needs_an_exclusive_directory() {
        let dir = tempfile::tempdir().unwrap();
        let uid = std::fs::metadata(dir.path()).unwrap().uid();
        exclusive_directory(dir.path(), uid).unwrap();

        let error = exclusive_directory(dir.path(), uid.wrapping_add(1)).unwrap_err();
        assert!(error.to_string().contains("exclusively"), "{error}");

        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
        let error = exclusive_directory(dir.path(), uid).unwrap_err();
        assert!(error.to_string().contains("exclusively"), "{error}");
    }

    /// A symlink is never an exclusive directory: it resolves somewhere the
    /// preparer did not verify.
    #[test]
    fn a_symlinked_directory_is_never_exclusive() {
        let dir = tempfile::tempdir().unwrap();
        let uid = std::fs::metadata(dir.path()).unwrap().uid();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(dir.path(), &link).unwrap();
        let error = exclusive_directory(&link, uid).unwrap_err();
        assert!(error.to_string().contains("exclusively"), "{error}");
    }

    /// A database that accepts the socket and then never answers must fail the
    /// connect at the deadline: before the bound, `preloop store` parked the
    /// operator's terminal for as long as the stall lasted.
    #[tokio::test]
    async fn a_stalled_postgres_connect_ends_at_the_deadline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            // Accept, then never write a byte: exactly the wedged-database
            // symptom (a `postgres` that took the connection and stopped).
            let Ok((_socket, _peer)) = listener.accept().await else {
                return;
            };
            std::future::pending::<()>().await
        });

        let started = std::time::Instant::now();
        let result = postgres_client_within(
            &format!("postgres://postgres@{address}/preloop"),
            CONNECT_TIMEOUT,
        )
        .await;
        let error = match result {
            Ok(_) => panic!("a database that never answers must not connect"),
            Err(error) => error,
        };
        let text = format!("{error:#}");
        assert!(text.contains("Postgres"), "{text}");
        assert!(text.contains("handshake"), "{text}");
        assert!(text.contains(&format!("{CONNECT_TIMEOUT:?}")), "{text}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "the connect must end at the deadline, not later: {:?}",
            started.elapsed()
        );
    }

    /// A fake Postgres that completes the startup handshake, answers the first
    /// `answers` simple queries, and then never answers again.
    ///
    /// It speaks just enough of the wire protocol for `tokio-postgres` to reach
    /// `ReadyForQuery`: AuthenticationOk (`R`), then ReadyForQuery (`Z`), and
    /// an empty CommandComplete for each answered query. That is what makes it
    /// a *partially* responsive database — the case the deadline covers beyond
    /// authentication — rather than a socket that never talks at all.
    async fn fake_postgres(listener: tokio::net::TcpListener, answers: usize) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let Ok((mut socket, _peer)) = listener.accept().await else {
            return;
        };
        // StartupMessage: an int32 total length, then protocol version and
        // parameters. The fake answers whatever arrives.
        let mut header = [0u8; 4];
        if socket.read_exact(&mut header).await.is_err() {
            return;
        }
        let payload = u32::from_be_bytes(header).saturating_sub(4) as usize;
        let mut startup = vec![0u8; payload];
        if socket.read_exact(&mut startup).await.is_err() {
            return;
        }
        if socket
            .write_all(b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x05I")
            .await
            .is_err()
        {
            return;
        }
        for _ in 0..answers {
            // One simple query (`Q`): tag, int32 length, then the statement.
            let mut message = [0u8; 5];
            if socket.read_exact(&mut message).await.is_err() {
                return;
            }
            let length = u32::from_be_bytes([message[1], message[2], message[3], message[4]]);
            let mut body = vec![0u8; length.saturating_sub(4) as usize];
            if socket.read_exact(&mut body).await.is_err() {
                return;
            }
            // An empty CommandComplete, then ReadyForQuery: the statement
            // succeeded. `batch_execute` asks for nothing more.
            if socket
                .write_all(b"C\0\0\0\x05\0Z\0\0\0\x05I")
                .await
                .is_err()
            {
                return;
            }
        }
        std::future::pending::<()>().await
    }

    /// A database that authenticates and then stops answering must fail at the
    /// same deadline: `SET search_path` is part of making the connection
    /// usable, so leaving it unbounded parked `preloop store` exactly the way
    /// the bound exists to prevent — one step past the handshake.
    #[tokio::test]
    async fn a_postgres_that_stalls_after_the_handshake_ends_at_the_deadline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(fake_postgres(listener, 0));

        let deadline = std::time::Duration::from_millis(500);
        let started = std::time::Instant::now();
        let result =
            postgres_client_within(&format!("postgres://postgres@{address}/preloop"), deadline)
                .await;
        let error = match result {
            Ok(_) => panic!("a database that stalls before session setup must not connect"),
            Err(error) => error,
        };
        let text = format!("{error:#}");
        assert!(text.contains("session setup"), "{text}");
        assert!(text.contains(&format!("{deadline:?}")), "{text}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "session setup must end at the deadline: {:?}",
            started.elapsed()
        );
    }

    /// A database that stops answering *during* the advisory-lock probe must
    /// reach the lock deadline like any other contention: unbounded, the probe
    /// never returned, so the elapsed-time check never ran and the migration
    /// hung past its documented ceiling.
    #[tokio::test]
    async fn a_probe_that_stops_answering_ends_at_the_lock_deadline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        // The session-setup query is answered; the probe after it is not.
        tokio::spawn(fake_postgres(listener, 1));

        let lock_wait = std::time::Duration::from_millis(500);
        let started = std::time::Instant::now();
        let result = migrate_postgres_within(
            &format!("postgres://postgres@{address}/preloop"),
            &MigrateOptions::default(),
            lock_wait,
        )
        .await;
        let error = match result {
            Ok(_) => panic!("a database that stalls mid-probe must not migrate"),
            Err(error) => error,
        };
        let text = format!("{error:#}");
        assert!(text.contains("migration-lock probe"), "{text}");
        assert!(text.contains(&format!("{lock_wait:?}")), "{text}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "the probe must end at the deadline: {:?}",
            started.elapsed()
        );
    }

    /// A second administrator must fail at the lock deadline, not hang, when a
    /// concurrent migration holds the setup lock. Skipped when no Postgres is
    /// reachable (`PRELOOP_TEST_POSTGRES_URL`), the repo's convention for
    /// optional database coverage.
    #[tokio::test]
    async fn a_held_migration_lock_fails_instead_of_hanging() {
        let Some((_database, url)) = crate::test_pg::fresh_database().await else {
            eprintln!(
                "skipping: set {} to a Postgres server to cover the migration lock",
                crate::test_pg::TEST_POSTGRES_URL_ENV
            );
            return;
        };
        // Hold the lock from a separate session, as a concurrent migration does.
        let holder = postgres_client(&url).await.unwrap();
        holder
            .batch_execute(&format!(
                "SELECT pg_advisory_lock({})",
                migrate_runner::POSTGRES_SETUP_LOCK
            ))
            .await
            .unwrap();

        let lock_wait = std::time::Duration::from_millis(500);
        let started = std::time::Instant::now();
        let result = migrate_postgres_within(&url, &MigrateOptions::default(), lock_wait).await;
        let error = match result {
            Ok(_) => panic!("migrating under a held lock must not succeed or hang"),
            Err(error) => error,
        };
        let text = format!("{error:#}");
        assert!(text.contains("holding the migration lock"), "{text}");
        assert!(
            text.contains(&format!("{}", migrate_runner::POSTGRES_SETUP_LOCK)),
            "{text}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "the lock wait must end at the deadline: {:?}",
            started.elapsed()
        );

        holder
            .batch_execute(&format!(
                "SELECT pg_advisory_unlock({})",
                migrate_runner::POSTGRES_SETUP_LOCK
            ))
            .await
            .unwrap();
    }
}
