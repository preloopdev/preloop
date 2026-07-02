//! `aksh-runner` — Rust reimplementation of the GitHub Actions runner.
//!
//! Subcommands: `configure`, `remove`, `run`, `worker` (hidden).

use anyhow::Result;
use clap::Parser;

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
    }
}
