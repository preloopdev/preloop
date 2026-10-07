//! `preloop store` — control-database schema administration.
//!
//! The implementation lives in `preloop_runner_server::store_admin` (that
//! crate owns the database drivers and the embedded migrations); this module
//! owns only the CLI surface and its output. `preloop serve` never migrates:
//! a missing/older/newer control schema refuses at boot and names
//! `preloop store migrate` as the explicit recovery.

use clap::{Args, Subcommand};
use preloop_runner_server::store_admin::{self, MigrateOptions, StoreTarget};

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

pub(crate) async fn run(args: StoreArgs) -> anyhow::Result<()> {
    match args.command {
        StoreCommand::Migrate(args) => migrate(args).await,
        StoreCommand::Status(args) => status(args).await,
    }
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
        let verb = if args.dry_run { "would adopt" } else { "adopted" };
        println!("[preloop] {verb} pre-ledger control database at migration {adopted}");
    }
    if report.applied.is_empty() && report.pending_before.is_empty() {
        println!("[preloop] control database already at the latest migration");
    } else if !report.applied.is_empty() {
        let verb = if args.dry_run { "would apply" } else { "applied" };
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
