//! `preloop store` — operator commands for the control database.
//!
//! `import-legacy` is the one-time, explicit move from a released v11
//! `preloop.db` (the durable-state store the control backend replaced) to a
//! fresh control SQLite database. It never runs from `preloop serve`: the
//! operator stops the server, imports, and moves the target into place (or
//! points `--store`/`PRELOOP_STORE_URL` at it).

use anyhow::Context;
use clap::{Args, Subcommand};
use preloop_runner_server::legacy_import::{self, ActivePolicy, ImportOptions};
use std::path::{Path, PathBuf};

#[derive(Debug, Args)]
pub struct StoreArgs {
    #[command(subcommand)]
    pub command: StoreCommand,
}

#[derive(Debug, Subcommand)]
pub enum StoreCommand {
    /// Import a legacy v11 `preloop.db` into a fresh control database.
    ///
    /// The source is opened read-only and hashed before and after; the target
    /// is built in `<target>.importing` and only renamed into place once every
    /// row is written and verified, so a failure leaves nothing to clean up
    /// and a retry is safe.
    #[command(name = "import-legacy")]
    ImportLegacy(ImportLegacyArgs),
}

#[derive(Debug, Args)]
pub struct ImportLegacyArgs {
    /// Legacy `preloop.db` to read (never modified).
    #[arg(long, value_name = "LEGACY_DB")]
    pub source: PathBuf,

    /// Control database to create. Must not exist yet.
    #[arg(long, value_name = "CONTROL_DB")]
    pub target: PathBuf,

    /// Server state directory the target will live in. Secret tiers and log
    /// segments are written here. Defaults to the target's directory.
    #[arg(long, value_name = "DIR")]
    pub state_dir: Option<PathBuf>,

    /// Cluster key file (32 bytes). Defaults to `PRELOOP_HMAC_KEY`, then
    /// `<state_dir>/hmac-key.bin`. The importer never generates a key.
    #[arg(long, value_name = "FILE")]
    pub key: Option<PathBuf>,

    /// Policy for work that was in flight when the legacy server stopped:
    /// `refuse` (default, list it and abort), `requeue` (release claims and
    /// requeue), or `cancel` (settle it as cancelled).
    #[arg(long, default_value = "refuse", value_name = "POLICY")]
    pub active: String,

    /// Print the machine-readable report.
    #[arg(long)]
    pub json: bool,
}

pub fn run(args: StoreArgs) -> anyhow::Result<()> {
    match args.command {
        StoreCommand::ImportLegacy(args) => import_legacy(args),
    }
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
