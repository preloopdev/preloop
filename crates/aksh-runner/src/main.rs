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
            let reusable_workflows = BTreeMap::new();
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
