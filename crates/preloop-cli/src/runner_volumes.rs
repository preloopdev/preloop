//! `preloop runner-volumes` — list and reclaim the read-only volumes that
//! AgentENV runners mount.
//!
//! The engine creates a read-only volume the first time a runner tree is needed
//! (see `preloop_vm::agentenv`), one per distinct content of the runner bundle
//! and of the Node externals. Nothing removes a superseded volume on its own,
//! because an unmounted volume may be about to be mounted. `prune` is the
//! explicit reclaim step: it keeps the trees named with `--keep`, and deletes
//! every other runner volume that no sandbox mounts.

use anyhow::Context;
use clap::{Args, Subcommand};
use preloop_vm::agentenv::AgentEnvProvider;
use std::path::PathBuf;

#[derive(Debug, Args)]
pub(crate) struct RunnerVolumesArgs {
    #[command(subcommand)]
    command: RunnerVolumesCommand,
}

#[derive(Debug, Subcommand)]
enum RunnerVolumesCommand {
    /// List the runner volumes AgentENV holds, with mode, size and status.
    List,
    /// Delete runner volumes that no sandbox mounts.
    ///
    /// Keeps the trees named with `--keep`; by default the runner bundle
    /// (`PRELOOP_RUNNER_BUNDLE`) and the Node externals the engine mounts beside
    /// it. Deleting an unused volume is harmless: its next start repopulates it.
    /// Run this while no engine is starting runners.
    Prune(PruneArgs),
}

#[derive(Debug, Args)]
struct PruneArgs {
    /// A host tree whose volume is kept (repeatable).
    #[arg(long = "keep", value_name = "DIR")]
    keep: Vec<PathBuf>,
    /// Report what would be deleted without deleting anything.
    #[arg(long)]
    dry_run: bool,
}

pub(crate) async fn run(args: RunnerVolumesArgs) -> anyhow::Result<()> {
    let provider = AgentEnvProvider::from_environment();
    match args.command {
        RunnerVolumesCommand::List => list(&provider).await,
        RunnerVolumesCommand::Prune(args) => prune(&provider, args).await,
    }
}

async fn list(provider: &AgentEnvProvider) -> anyhow::Result<()> {
    let volumes = provider.list_runner_volumes().await?;
    if volumes.is_empty() {
        println!("no runner volumes");
        return Ok(());
    }
    println!("{:<30} {:<10} {:>8}  STATUS", "NAME", "MODE", "SIZE_MB");
    for volume in volumes {
        println!(
            "{:<30} {:<10} {:>8}  {}",
            volume.name, volume.mode, volume.size_mb, volume.status
        );
    }
    Ok(())
}

async fn prune(provider: &AgentEnvProvider, args: PruneArgs) -> anyhow::Result<()> {
    let trees = if args.keep.is_empty() {
        default_keep_trees()
    } else {
        args.keep
    };
    if trees.is_empty() {
        eprintln!(
            "warning: nothing to keep (no --keep, and PRELOOP_RUNNER_BUNDLE is unset): \
             every unmounted runner volume is a candidate"
        );
    }
    let mut keep = Vec::with_capacity(trees.len());
    for tree in &trees {
        let name = provider
            .runner_volume_name(tree)
            .await
            .with_context(|| format!("naming the runner volume for {}", tree.display()))?;
        keep.push(name);
    }
    let report = provider.prune_runner_volumes(&keep, args.dry_run).await?;
    let verb = if args.dry_run {
        "would delete"
    } else {
        "deleted"
    };
    for name in &report.deleted {
        println!("{verb} {name}");
    }
    for name in &report.kept {
        println!("kept {name} (named by --keep)");
    }
    for name in &report.in_use {
        println!("in use {name} (a sandbox mounts it; left in place)");
    }
    for (name, reason) in &report.failed {
        println!("failed {name}: {reason}");
    }
    if report.failed.is_empty() {
        Ok(())
    } else {
        anyhow::bail!(
            "{} runner volume(s) could not be deleted",
            report.failed.len()
        )
    }
}

/// The trees the engine mounts by default: the runner bundle, and the externals
/// directory beside it (the engine mounts `<externals>/externals`).
fn default_keep_trees() -> Vec<PathBuf> {
    let mut trees = Vec::new();
    if let Some(bundle) = std::env::var_os("PRELOOP_RUNNER_BUNDLE") {
        trees.push(PathBuf::from(bundle));
    }
    let externals = std::env::var_os("PRELOOP_RUNNER_EXTERNALS")
        .map(PathBuf::from)
        .or_else(|| preloop_home().map(|home| home.join("externals")));
    if let Some(externals) = externals {
        trees.push(externals.join("externals"));
    }
    trees
}

/// The state home the engine uses: `PRELOOP_HOME`, else `~/.preloop`.
fn preloop_home() -> Option<PathBuf> {
    std::env::var_os("PRELOOP_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".preloop")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Debug, Parser)]
    struct Harness {
        #[command(flatten)]
        args: RunnerVolumesArgs,
    }

    #[test]
    fn prune_takes_repeated_keep_trees_and_a_dry_run_flag() {
        let harness = Harness::try_parse_from([
            "runner-volumes",
            "prune",
            "--keep",
            "/opt/bundle",
            "--keep",
            "/var/externals/externals",
            "--dry-run",
        ])
        .unwrap();
        match harness.args.command {
            RunnerVolumesCommand::Prune(args) => {
                assert_eq!(
                    args.keep,
                    vec![
                        PathBuf::from("/opt/bundle"),
                        PathBuf::from("/var/externals/externals"),
                    ]
                );
                assert!(args.dry_run);
            }
            other => panic!("expected prune, got {other:?}"),
        }
    }

    #[test]
    fn list_takes_no_arguments_and_prune_defaults_to_a_real_run() {
        let harness = Harness::try_parse_from(["runner-volumes", "list"]).unwrap();
        assert!(matches!(harness.args.command, RunnerVolumesCommand::List));
        assert!(Harness::try_parse_from(["runner-volumes", "list", "--keep", "/x"]).is_err());

        let harness = Harness::try_parse_from(["runner-volumes", "prune"]).unwrap();
        match harness.args.command {
            RunnerVolumesCommand::Prune(args) => {
                assert!(args.keep.is_empty());
                assert!(!args.dry_run);
            }
            other => panic!("expected prune, got {other:?}"),
        }
    }
}
