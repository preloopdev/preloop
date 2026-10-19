//! `preloop volumes` — list and reclaim the read-only volumes that
//! AgentENV runners mount.
//!
//! The engine creates a read-only volume the first time a runner tree is needed
//! (see `preloop_vm::agentenv`), one per distinct content of the runner bundle.
//! Nothing removes a superseded volume on its own, because an unmounted volume
//! may be about to be mounted. `prune` is the explicit reclaim step: it keeps
//! the tree the engine mounts, and deletes every other runner volume that no
//! sandbox mounts.
//!
//! Pruning is safe while an engine is running: the provider re-derives a tree's
//! fingerprint and checks that a volume still exists before reusing it, so a
//! volume deleted here is populated again on the next start that needs it (a
//! ~1 minute populate, not a failure). It is *not* safe while a volume is being
//! populated: one deleted between its create and its first mount fails that
//! populate.
//!
//! `list` cannot read inside a volume — AgentENV has no API for that — so it
//! answers from the index the engine writes when it populates one.

use anyhow::Context;
use clap::{Args, Subcommand};
use preloop_vm::agentenv::gc as volume_gc;
use preloop_vm::agentenv::volume;
use preloop_vm::agentenv::{AgentEnvProvider, AgentEnvVolume, VolumeIndex};
use std::path::PathBuf;

#[derive(Debug, Args)]
pub(crate) struct VolumesArgs {
    #[command(subcommand)]
    command: VolumesCommand,
}

#[derive(Debug, Subcommand)]
enum VolumesCommand {
    /// List the runner volumes AgentENV holds, with mode, size, status and the
    /// versions they were populated from.
    List,
    /// Delete runner volumes that no sandbox mounts.
    ///
    /// Keeps the bundle the engine mounts (the same tree selection the pool
    /// makes, including its own mirror) and the standalone Node externals when
    /// one is installed; `--keep` replaces that list. Safe while an engine runs
    /// — a deleted volume is populated again on the next start that needs it —
    /// but not while a volume is being populated. A mounted volume is reported
    /// as in use and left alone.
    Prune(PruneArgs),
    /// Collect the volumes nothing needs any more.
    ///
    /// Deletes only what the retention policy calls superseded: volumes no
    /// sandbox mounts, past the grace period, beyond the rollback depth, and not
    /// a tree the engine mounts (the same tree selection `prune` keeps, or
    /// `--keep`). A volume nothing recorded a creation time for is kept, so a
    /// leftover from an older build needs `prune`. Prints the plan: one row per
    /// volume with its size, kind, age and the reason it is kept or deleted.
    ///
    /// The engine runs the same plan by itself after it populates a volume and
    /// every `PRELOOP_VOLUMES_GC_INTERVAL_SECS`; `PRELOOP_VOLUMES_GC=off` stops
    /// those and leaves this command.
    Gc(GcArgs),
}

#[derive(Debug, Args)]
struct GcArgs {
    /// A host tree whose volume is kept (repeatable). Without it, the engine's
    /// bundle and the installed Node externals are kept.
    #[arg(long = "keep", value_name = "DIR")]
    keep: Vec<PathBuf>,
    /// Report the plan without deleting anything.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Args)]
struct PruneArgs {
    /// A host tree whose volume is kept (repeatable). Without it, the engine's
    /// bundle and the installed Node externals are kept.
    #[arg(long = "keep", value_name = "DIR")]
    keep: Vec<PathBuf>,
    /// Report what would be deleted without deleting anything.
    ///
    /// A dry run cannot tell which volumes a sandbox holds: AgentENV reveals a
    /// reservation only when a delete is attempted. It therefore reports
    /// candidates, not deletions.
    #[arg(long)]
    dry_run: bool,
}

pub(crate) async fn run(args: VolumesArgs) -> anyhow::Result<()> {
    let provider = AgentEnvProvider::from_environment()
        .with_bundle_manifest(preloop_orchestrator::runner_bundle_manifest());
    match args.command {
        VolumesCommand::List => list(&provider).await,
        VolumesCommand::Prune(args) => prune(provider, args).await,
        VolumesCommand::Gc(args) => gc(provider, args).await,
    }
}

async fn list(provider: &AgentEnvProvider) -> anyhow::Result<()> {
    let volumes = provider.list_volumes().await?;
    let index = provider.volume_index().await;
    print!("{}", render_list(&volumes, &index));
    Ok(())
}

/// The `list` report: one row per volume, with its kind and the versions the
/// index recorded for it, or a note that nothing recorded what it holds.
fn render_list(volumes: &[AgentEnvVolume], index: &VolumeIndex) -> String {
    if volumes.is_empty() {
        return "no runner volumes\n".to_owned();
    }
    let mut out = String::new();
    out.push_str(&format!(
        "{:<30} {:<24} {:<10} {:>8}  {:<7}  {:<12} {:<12} {}\n",
        "NAME", "KIND", "MODE", "SIZE_MB", "STATUS", "RUNNER", "NODE20", "NODE24"
    ));
    for volume in volumes {
        let (runner, node20, node24) = match index.get(&volume.name) {
            Some(entry) => (
                entry.manifest.runner_version().to_owned(),
                entry.manifest.node20_version().to_owned(),
                entry.manifest.node24_version().to_owned(),
            ),
            None => (
                "unknown (pre-manifest or created elsewhere)".to_owned(),
                "unknown".to_owned(),
                "unknown".to_owned(),
            ),
        };
        out.push_str(&format!(
            "{:<30} {:<24} {:<10} {:>8}  {:<7}  {:<12} {:<12} {}\n",
            volume.name,
            volume::volume_kind(&volume.name),
            volume.mode,
            volume.size_mb,
            volume.status,
            runner,
            node20,
            node24
        ));
    }
    out
}

/// Collect what the retention policy calls superseded, and report it.
///
/// The plan is what a pass acts on: `--dry-run` prints it and stops there, and a
/// real run deletes exactly the volumes it names (a mounted volume comes back as
/// in use, never as deleted). A deletion the server refused for any other reason
/// exits nonzero, like `prune`, so a caller can tell that the cleanup it asked
/// for did not happen.
async fn gc(provider: AgentEnvProvider, args: GcArgs) -> anyhow::Result<()> {
    let trees = if args.keep.is_empty() {
        default_keep_trees()
    } else {
        args.keep
    };
    if trees.is_empty() {
        eprintln!(
            "warning: no runner bundle is installed and none was named with --keep: \
             only mounted, young and rollback volumes are kept"
        );
    }
    let mut live = Vec::with_capacity(trees.len());
    for tree in &trees {
        live.push(
            provider
                .runner_volume_name(tree)
                .await
                .with_context(|| format!("naming the runner volume for {}", tree.display()))?,
        );
    }
    let request = volume_gc::Request::new()
        .with_live(live)
        .dry_run(args.dry_run);
    let outcome = volume_gc::run_pass(&provider, request).await?;
    print!("{}", outcome.render(volume_gc::unix_now()));
    if outcome.report.failed.is_empty() {
        Ok(())
    } else {
        anyhow::bail!(
            "{} runner volume(s) could not be deleted",
            outcome.report.failed.len()
        )
    }
}

async fn prune(provider: AgentEnvProvider, args: PruneArgs) -> anyhow::Result<()> {
    let trees = if args.keep.is_empty() {
        default_keep_trees()
    } else {
        args.keep
    };
    let mut keep = Vec::with_capacity(trees.len());
    for tree in &trees {
        let name = provider
            .runner_volume_name(tree)
            .await
            .with_context(|| format!("naming the runner volume for {}", tree.display()))?;
        keep.push(name);
    }
    if keep.is_empty() {
        eprintln!(
            "warning: nothing to keep (no --keep, and no runner bundle is installed): \
             every unmounted runner volume is a candidate"
        );
    }
    let report = provider.prune_volumes(&keep, args.dry_run).await?;
    for name in &report.deleted {
        println!("deleted {name}");
    }
    for name in &report.kept {
        println!("kept {name} (named by --keep)");
    }
    for name in &report.in_use {
        println!("in use {name} (a sandbox mounts it; left in place)");
    }
    for name in &report.candidates {
        println!("candidate {name} (a mounted volume is reported as in use by a real run)");
    }
    for (name, reason) in &report.failed {
        println!("failed {name}: {reason}");
    }
    if args.dry_run {
        println!("dry run: nothing was deleted");
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

/// The trees the engine mounts by default: the bundle the pool selects, and the
/// standalone Node externals tree when one is installed.
///
/// AgentENV mounts only the bundle — its Node externals ride inside it — so the
/// externals tree is kept only when it exists, and never blocks a prune.
fn default_keep_trees() -> Vec<PathBuf> {
    keep_trees_with(effective_bundle(), standalone_externals_tree())
}

/// [`default_keep_trees`] from its two candidates, so a test needs neither an
/// environment nor an installed engine. A candidate that is not a directory is
/// dropped rather than turned into a keep entry that cannot be hashed.
fn keep_trees_with(bundle: Option<PathBuf>, externals: Option<PathBuf>) -> Vec<PathBuf> {
    let mut trees = Vec::new();
    if let Some(bundle) = bundle {
        trees.push(bundle);
    }
    if let Some(externals) = externals.filter(|tree| tree.is_dir()) {
        trees.push(externals);
    }
    trees
}

/// The bundle [`default_keep_trees`] keeps: whatever the engine would mount for
/// the configured bundle, including the engine-owned mirror. When neither
/// candidate validates, the configured tree is kept: pruning must never delete
/// the volume of the bundle an engine is about to mount.
fn effective_bundle() -> Option<PathBuf> {
    let bundle = crate::discovered_runner_bundle()?;
    let Some(externals_dir) = crate::local_externals_dir() else {
        return Some(bundle);
    };
    match preloop_orchestrator::effective_runner_bundle_path(
        &bundle,
        &externals_dir,
        crate::RUNNER_BINARY_NAME,
    ) {
        Ok(path) => Some(path),
        Err(_) => Some(bundle),
    }
}

/// The standalone externals tree the engine mounts from, if an operator
/// installed one; `None` when it is absent, which is the normal case for the
/// AgentENV backend.
fn standalone_externals_tree() -> Option<PathBuf> {
    Some(crate::local_externals_dir()?.join("externals"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use preloop_vm::agentenv::IndexedVolume;
    use preloop_vm::agentenv::volume::BundleManifest;
    use std::collections::BTreeMap;

    #[derive(Debug, Parser)]
    struct Harness {
        #[command(flatten)]
        args: VolumesArgs,
    }

    fn volume(name: &str) -> AgentEnvVolume {
        AgentEnvVolume {
            volume_id: "v1".to_owned(),
            name: name.to_owned(),
            mode: "ro".to_owned(),
            size_mb: 1024,
            status: "ready".to_owned(),
        }
    }

    #[test]
    fn prune_takes_repeated_keep_trees_and_a_dry_run_flag() {
        let harness = Harness::try_parse_from([
            "volumes",
            "prune",
            "--keep",
            "/opt/bundle",
            "--keep",
            "/var/externals/externals",
            "--dry-run",
        ])
        .unwrap();
        match harness.args.command {
            VolumesCommand::Prune(args) => {
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
        let harness = Harness::try_parse_from(["volumes", "list"]).unwrap();
        assert!(matches!(harness.args.command, VolumesCommand::List));
        assert!(Harness::try_parse_from(["volumes", "list", "--keep", "/x"]).is_err());

        let harness = Harness::try_parse_from(["volumes", "prune"]).unwrap();
        match harness.args.command {
            VolumesCommand::Prune(args) => {
                assert!(args.keep.is_empty());
                assert!(!args.dry_run);
            }
            other => panic!("expected prune, got {other:?}"),
        }
    }

    #[test]
    fn list_reports_the_recorded_versions_and_marks_unrecorded_volumes() {
        let volumes = [
            volume("preloop-runner-0123456789ab"),
            volume("preloop-runner-abcdef012345"),
        ];
        let index = VolumeIndex {
            volumes: BTreeMap::from([(
                "preloop-runner-0123456789ab".to_owned(),
                IndexedVolume {
                    manifest: BundleManifest::new("2.336.0", "20.20.2", "24.19.0"),
                    bundle_files: 11_157,
                    bundle_bytes: 422_129_834,
                    host: "/opt/preloop/bundle".to_owned(),
                    created: 1_760_000_000,
                },
            )]),
        };
        let rendered = render_list(&volumes, &index);
        assert!(
            rendered.contains("NAME") && rendered.contains("RUNNER"),
            "{rendered}"
        );
        assert!(
            rendered.contains("preloop-runner-0123456789ab") && rendered.contains("2.336.0"),
            "{rendered}"
        );
        assert!(
            rendered.contains("20.20.2") && rendered.contains("24.19.0"),
            "{rendered}"
        );
        assert!(
            rendered.contains("unknown (pre-manifest or created elsewhere)"),
            "{rendered}"
        );
        assert_eq!(render_list(&[], &index), "no runner volumes\n".to_owned());
    }

    #[test]
    fn the_default_keep_list_holds_only_trees_that_exist() {
        let temporary = tempfile::tempdir().unwrap();
        let bundle = temporary.path().join("bundle");
        std::fs::create_dir_all(&bundle).unwrap();
        std::fs::write(bundle.join(crate::RUNNER_BINARY_NAME), b"ELF").unwrap();
        // No externals tree beside it: the list is the bundle alone, and prune
        // is not blocked by the missing tree.
        assert_eq!(
            keep_trees_with(Some(bundle.clone()), None),
            vec![bundle.clone()]
        );
        let externals = temporary.path().join("externals/externals");
        std::fs::create_dir_all(&externals).unwrap();
        assert_eq!(
            keep_trees_with(Some(bundle.clone()), Some(externals.clone())),
            vec![bundle, externals]
        );
        // A configured tree that is not there, and no bundle discovered at all.
        assert_eq!(
            keep_trees_with(None, Some(temporary.path().join("missing"))),
            Vec::<PathBuf>::new()
        );
    }
}
