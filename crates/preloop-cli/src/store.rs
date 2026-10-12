//! `preloop store` — operator commands for the control database.
//!
//! Schema administration (`migrate`/`status`) lives in
//! `preloop_runner_server::store_admin` (that crate owns the database drivers
//! and the embedded migrations); this module owns only the CLI surface and
//! its output. `preloop serve` never migrates: a missing/older/newer control
//! schema refuses at boot and names `preloop store migrate` as the explicit
//! recovery.
//!
//! `import-legacy` is the one-time, explicit move from a released v11
//! `preloop.db` (the durable-state store the control backend replaced) to a
//! fresh control SQLite database. It never runs from `preloop serve`: the
//! operator stops the server, imports, and moves the target into place (or
//! points `--store`/`PRELOOP_STORE_URL` at it).

use anyhow::Context;
use clap::{Args, Subcommand};
use preloop_runner_server::legacy_import::{self, ActivePolicy, ImportOptions};
use preloop_runner_server::store_admin::{self, MigrateOptions, StoreTarget};
use std::path::{Path, PathBuf};

#[derive(Debug, Args)]
pub(crate) struct StoreArgs {
    #[command(subcommand)]
    command: StoreCommand,
}

#[derive(Debug, Subcommand)]
enum StoreCommand {
    /// Initialize or upgrade the control database schema (refinery, forward
    /// only; a SQLite run takes a consistent pre-migration backup first).
    Migrate(MigrateArgs),
    /// Show the control database's applied, pending and unknown migrations.
    /// Read-only: never creates or changes the database.
    Status(StatusArgs),
    /// Import a legacy v11 `preloop.db` into a fresh control database.
    ///
    /// The source is opened read-only and hashed before and after; the target
    /// is built in `<target>.importing` and only renamed into place once every
    /// row is written and verified, so a failure leaves nothing to clean up
    /// and a retry is safe.
    #[command(name = "import-legacy")]
    ImportLegacy(ImportLegacyArgs),
    /// Internal: initialize only a brand-new local store, as the account
    /// that will own it. `preloop server install` runs this as the service
    /// account so root never creates, writes or chmods inside the
    /// service-owned state tree; an existing store is never touched
    /// (upgrading is `migrate`).
    #[command(hide = true)]
    InitLocal,
}

#[derive(Debug, Args)]
struct MigrateArgs {
    /// Store URL: `sqlite://<path>`, a bare path, or `postgres://…`.
    /// Defaults to PRELOOP_STORE_URL, then <PRELOOP_HOME>/state/preloop.db.
    #[arg(long, value_name = "URL")]
    store: Option<String>,

    /// Adopt a pre-ledger control database after verifying its shape
    /// (an unreleased build: SQLite v4 / Postgres v5), then apply pending
    /// migrations. Refused unless the database's structure is exactly a
    /// known migration point.
    #[arg(long)]
    adopt_baseline: bool,

    /// Skip the automatic SQLite pre-migration backup.
    #[arg(long)]
    no_backup: bool,

    /// Report what would happen without changing anything.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Args)]
struct StatusArgs {
    /// Store URL: `sqlite://<path>`, a bare path, or `postgres://…`.
    /// Defaults to PRELOOP_STORE_URL, then <PRELOOP_HOME>/state/preloop.db.
    #[arg(long, value_name = "URL")]
    store: Option<String>,
}

#[derive(Debug, Args)]
struct ImportLegacyArgs {
    /// Legacy `preloop.db` to read (never modified).
    #[arg(long, value_name = "LEGACY_DB")]
    source: PathBuf,

    /// Control database to create. Must not exist yet.
    #[arg(long, value_name = "CONTROL_DB")]
    target: PathBuf,

    /// Server state directory the target will live in. Secret tiers and log
    /// segments are written here. Defaults to the target's directory.
    #[arg(long, value_name = "DIR")]
    state_dir: Option<PathBuf>,

    /// Cluster key file (32 bytes). Defaults to `PRELOOP_HMAC_KEY`, then
    /// `<state_dir>/hmac-key.bin`. The importer never generates a key.
    #[arg(long, value_name = "FILE")]
    key: Option<PathBuf>,

    /// Policy for work that was in flight when the legacy server stopped:
    /// `refuse` (default, list it and abort), `requeue` (release claims and
    /// requeue), or `cancel` (settle it as cancelled).
    #[arg(long, default_value = "refuse", value_name = "POLICY")]
    active: String,

    /// Print the machine-readable report.
    #[arg(long)]
    json: bool,
}

pub(crate) async fn run(args: StoreArgs) -> anyhow::Result<()> {
    match args.command {
        StoreCommand::Migrate(args) => migrate(args).await,
        StoreCommand::Status(args) => status(args).await,
        StoreCommand::ImportLegacy(args) => import_legacy(args),
        StoreCommand::InitLocal => init_local(),
    }
}

/// Initialize only a brand-new local store (see `prepare_brand_new_local`).
/// A no-op when the store already exists, so it is safe as the installer's
/// preparation step for both a fresh and a repeated install.
fn init_local() -> anyhow::Result<()> {
    let state_dir = crate::preloop_home().join("state");
    if store_admin::prepare_brand_new_local(&state_dir)? {
        eprintln!(
            "[preloop] initialized the control database at {}",
            state_dir.join("preloop.db").display()
        );
    }
    Ok(())
}

/// Resolve the target exactly like `preloop serve` resolves its store.
fn target(store: Option<&str>) -> anyhow::Result<StoreTarget> {
    let state_dir = crate::preloop_home().join("state");
    StoreTarget::resolve(store, &state_dir)
}

async fn migrate(args: MigrateArgs) -> anyhow::Result<()> {
    let target = target(args.store.as_deref())?;
    let options = MigrateOptions {
        adopt_baseline: args.adopt_baseline,
        backup: !args.no_backup,
        dry_run: args.dry_run,
    };
    let report = store_admin::migrate(target.clone(), options).await?;
    if let Some(adopted) = report.adopted {
        let verb = if args.dry_run {
            "would adopt"
        } else {
            "adopted"
        };
        println!("[preloop] {verb} pre-ledger control database at migration {adopted}");
    }
    if report.applied.is_empty() && report.pending_before.is_empty() {
        println!("[preloop] control database already at the latest migration");
    } else if !report.applied.is_empty() {
        let verb = if args.dry_run {
            "would apply"
        } else {
            "applied"
        };
        println!(
            "[preloop] {verb} migrations: {}",
            version_list(&report.applied)
        );
    } else if args.dry_run {
        println!(
            "[preloop] would apply migrations: {}",
            version_list(&report.pending_before)
        );
    }
    if let Some(backup) = &report.backup {
        println!("[preloop] pre-migration backup: {}", backup.display());
    }
    if args.dry_run {
        println!("[preloop] dry run: nothing was changed");
    }
    Ok(())
}

async fn status(args: StatusArgs) -> anyhow::Result<()> {
    let target = target(args.store.as_deref())?;
    let status = store_admin::status(target).await?;
    println!("[preloop] {}: {}", status.target, status.state);
    println!("  applied: {}", version_list(&status.applied));
    println!("  pending: {}", version_list(&status.pending));
    println!("  unknown: {}", version_list(&status.unknown));
    if !status.is_current() {
        println!("[preloop] not at this build's schema; run `preloop store migrate`");
    }
    Ok(())
}

fn import_legacy(args: ImportLegacyArgs) -> anyhow::Result<()> {
    let state_dir = args
        .state_dir
        .clone()
        .unwrap_or_else(|| target_dir(&args.target));
    let key = legacy_import::load_import_key(&state_dir, args.key.as_deref())?;
    let active = ActivePolicy::parse(&args.active)?;
    let options = ImportOptions {
        source: args.source.clone(),
        target: args.target.clone(),
        state_dir,
        key,
        active,
    };
    let report = legacy_import::run_import(&options)?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).context("serialize import report")?
        );
    } else {
        print!("{}", report.render_human());
    }
    Ok(())
}

fn target_dir(target: &Path) -> PathBuf {
    target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

fn version_list(versions: &[i32]) -> String {
    if versions.is_empty() {
        "none".to_owned()
    } else {
        versions
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }
}
