//! Preloop runner server binary.

use std::net::SocketAddr;
use std::path::PathBuf;

use aksh_runner_server::{serve, ServerConfig, TlsMode};
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
    /// Start the server.
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
        /// Path to TLS certificate PEM file.
        #[arg(long, requires = "tls_key")]
        tls_cert: Option<PathBuf>,
        /// Path to TLS private key PEM file.
        #[arg(long, requires = "tls_cert")]
        tls_key: Option<PathBuf>,
        /// Generate an ephemeral self-signed cert (local dev only).
        #[arg(long, conflicts_with = "tls_cert")]
        tls_self_signed: bool,
        /// Enable privileged local/CI simulation endpoints (loopback only).
        #[arg(long)]
        enable_test_api: bool,
        /// Bearer token for privileged simulation endpoints.
        #[arg(long, requires = "enable_test_api")]
        test_api_token: Option<String>,
    },
    /// Generate a persistent self-signed TLS certificate (no openssl needed).
    Cert {
        /// Directory to write cert.pem and key.pem into.
        #[arg(long, default_value = ".")]
        output: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Install rustls ring crypto provider before any TLS operations.
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Cert { output } => {
            std::fs::create_dir_all(&output)?;
            let cert = aksh_runner_server::generate_self_signed_cert()?;
            let cert_path = output.join("cert.pem");
            let key_path = output.join("key.pem");
            std::fs::write(&cert_path, &cert.cert)?;
            std::fs::write(&key_path, &cert.key)?;
            println!("Wrote {}", cert_path.display());
            println!("Wrote {}", key_path.display());
            println!();
            println!("Use with:");
            println!(
                "  aksh-runner-server serve --tls-cert {} --tls-key {}",
                cert_path.display(),
                key_path.display()
            );
        }
        Command::Serve {
            listen,
            state_dir,
            record_flows,
            tls_cert,
            tls_key,
            tls_self_signed,
            enable_test_api,
            test_api_token,
        } => {
            let tls = match (tls_cert, tls_key, tls_self_signed) {
                (Some(cert), Some(key), false) => TlsMode::PemFiles { cert, key },
                (None, None, true) => TlsMode::SelfSigned,
                (None, None, false) => TlsMode::None,
                _ => unreachable!("clap ensures mutual exclusion"),
            };
            serve(ServerConfig {
                listen,
                state_dir,
                record_flows,
                tls,
                enable_test_api,
                test_api_token,
            })
            .await?;
        }
    }
    Ok(())
}
