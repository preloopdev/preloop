//! `aksh-runner` — Rust reimplementation of the GitHub Actions runner.
//!
//! Subcommands: `configure`, `remove`, `run`, `worker` (hidden).

use anyhow::Result;
use clap::Parser;
use std::collections::BTreeMap;

use aksh_runner::cli::{Cli, Commands};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    match cli.command {
        Commands::Configure(args) => aksh_runner::configure::run_configure(args, &cli.global).await,
        Commands::Remove(args) => aksh_runner::configure::run_remove(args, &cli.global).await,
        Commands::Run(args) => aksh_runner::listener::run_listener(args, &cli.global).await,
        Commands::Worker(args) => aksh_runner::worker::run_worker(args).await,
        Commands::Lint(args) => {
            let workflow_yaml = tokio::fs::read_to_string(&args.workflow)
                .await
                .map_err(|e| anyhow::anyhow!("read workflow {}: {e}", args.workflow.display()))?;
            let parsed = aksh_gha_parser::parse_workflow(&workflow_yaml)
                .map_err(|e| anyhow::anyhow!("parse workflow {}: {e}", args.workflow.display()))?;
            let reusable_workflows =
                collect_reusable_workflows(args.workspace_root.as_deref(), &args.workflow).await?;
            let expanded =
                aksh_gha_parser::expand_jobs_with_reusables(&parsed, &reusable_workflows).map_err(
                    |e| anyhow::anyhow!("expand workflow {}: {e}", args.workflow.display()),
                )?;

            let step_count: usize = expanded.jobs.iter().map(|j| j.steps.len()).sum();
            println!(
                "✓ Workflow {} is valid: parsed {} job plan(s) and {} total step(s).",
                args.workflow.display(),
                expanded.jobs.len(),
                step_count
            );
            for job in &expanded.jobs {
                println!("  - Job: {} ({})", job.id.0, job.name);
            }
            Ok(())
        }
    }
}

async fn collect_reusable_workflows(
    workspace_root: Option<&std::path::Path>,
    submitted_workflow: &std::path::Path,
) -> Result<BTreeMap<String, String>> {
    let Some(root) = workspace_root else {
        return Ok(BTreeMap::new());
    };
    let workflow_dir = root.join(".github").join("workflows");
    let mut out = BTreeMap::new();
    let mut entries = match tokio::fs::read_dir(&workflow_dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(error) => {
            return Err(anyhow::anyhow!("read {}: {error}", workflow_dir.display()));
        }
    };
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if !matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("yml" | "yaml")
        ) || same_file_path(&path, submitted_workflow)
        {
            continue;
        }
        let relative = path
            .strip_prefix(root)
            .map_err(|error| {
                anyhow::anyhow!(
                    "make {} relative to {}: {error}",
                    path.display(),
                    root.display()
                )
            })?
            .to_string_lossy()
            .into_owned();
        let yaml = tokio::fs::read_to_string(&path).await.map_err(|error| {
            anyhow::anyhow!("read reusable workflow {}: {error}", path.display())
        })?;
        out.insert(relative, yaml);
    }
    Ok(out)
}

fn same_file_path(left: &std::path::Path, right: &std::path::Path) -> bool {
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}
