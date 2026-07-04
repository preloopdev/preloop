//! Preloop runner server binary.

use std::net::SocketAddr;
use std::path::PathBuf;

use aksh_runner_server::{serve, ServerConfig};
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "aksh-server")]
#[command(about = "Preloop local GitHub Actions control plane")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start the HTTP server.
    Serve {
        /// Address to listen on.
        #[arg(long, default_value = "127.0.0.1:8080")]
        listen: SocketAddr,
        /// State directory.
        #[arg(long, default_value = ".aksh")]
        state_dir: PathBuf,
        /// File path to write recorded flows to (NDJSON format).
        #[arg(long)]
        record_flows: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Serve {
            listen,
            state_dir,
            record_flows,
        } => {
            serve(ServerConfig {
                listen,
                state_dir,
                record_flows,
            })
            .await?;
        }
    }
    Ok(())
}
