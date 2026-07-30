//! CLI argument definitions.

use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

/// Rust reimplementation of the GitHub Actions runner.
#[derive(Debug, Parser)]
#[command(name = "preloop-runner")]
#[command(version = concat!(env!("CARGO_PKG_VERSION"), " (protocol-compat 2.335.1)"))]
#[command(about = "GitHub Actions runner — Rust implementation")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    #[command(flatten)]
    pub global: GlobalArgs,
}

/// Global arguments available to all subcommands.
#[derive(Debug, Clone, clap::Args)]
pub struct GlobalArgs {
    /// Path to a PEM CA bundle for custom certificate trust (also honors SSL_CERT_FILE).
    #[arg(long = "ca-bundle", global = true, env = "SSL_CERT_FILE")]
    pub ca_bundle: Option<PathBuf>,

    /// Runner root directory (defaults to current directory).
    #[arg(long = "runner-root", global = true)]
    pub runner_root: Option<PathBuf>,
}

impl GlobalArgs {
    /// Resolve the runner root directory.
    pub fn runner_root(&self) -> PathBuf {
        self.runner_root
            .clone()
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
    }
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Register this runner with a GitHub repository or organization.
    Configure(ConfigureArgs),

    /// Deregister this runner.
    Remove(RemoveArgs),

    /// Start the runner listener (polls for jobs, spawns workers).
    Run(RunArgs),

    /// Lint and dry-run workflow parsing and job expansion without running it.
    Lint(LintArgs),

    /// Internal: worker process spawned per job (reads NDJSON on stdin).
    #[command(hide = true)]
    Worker(WorkerArgs),
}

/// Arguments for `configure`.
#[derive(Debug, Clone, clap::Args)]
pub struct ConfigureArgs {
    /// URL of the GitHub repository or organization.
    #[arg(long)]
    pub url: String,

    /// Registration token.
    #[arg(long, env = "PRELOOP_RUNNER_TOKEN")]
    pub token: String,

    /// Runner name (defaults to hostname).
    #[arg(long)]
    pub name: Option<String>,

    /// Comma-separated labels.
    #[arg(long, value_delimiter = ',')]
    pub labels: Option<Vec<String>>,

    /// Work directory name (relative to runner root).
    #[arg(long, default_value = "_work")]
    pub work: String,

    /// Runner group.
    #[arg(long, default_value = "default")]
    pub runner_group: String,

    /// Run non-interactively.
    #[arg(long)]
    pub unattended: bool,

    /// Replace existing runner with the same name.
    #[arg(long)]
    pub replace: bool,

    /// Configure as ephemeral (single job, then deregister).
    #[arg(long)]
    pub ephemeral: bool,

    /// Skip downloading Node.js externals.
    #[arg(long)]
    pub no_externals: bool,
}

/// Arguments for `remove`.
#[derive(Debug, Clone, clap::Args)]
pub struct RemoveArgs {
    /// Registration token for removal.
    #[arg(long)]
    pub token: String,
}

/// Arguments for `run`.
#[derive(Debug, Clone, clap::Args)]
pub struct RunArgs {
    /// Exit after completing one job.
    #[arg(long)]
    pub once: bool,

    /// Protocol path to use.
    #[arg(long, default_value = "broker")]
    pub via: ProtocolPath,
}

/// Arguments for `lint`.
#[derive(Debug, Clone, clap::Args)]
pub struct LintArgs {
    /// Workflow YAML path.
    #[arg(short = 'W', long)]
    pub workflow: PathBuf,

    /// Repository workspace root used to collect local reusable workflows.
    #[arg(long)]
    pub workspace_root: Option<PathBuf>,
}

/// Protocol path selection.
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum ProtocolPath {
    /// GitHub-current: broker/run-service path.
    Broker,
    /// Legacy AzDO: full message queue + Timeline/Logfiles.
    Azdo,
}

/// Arguments for the hidden `worker` subcommand.
#[derive(Debug, Clone, clap::Args)]
pub struct WorkerArgs {
    /// Protocol path used by the listener (passed through for reporting).
    #[arg(long, default_value = "broker")]
    pub via: ProtocolPath,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    #[test]
    fn parse_configure() {
        let cli = Cli::try_parse_from([
            "aksh-runner",
            "configure",
            "--url",
            "https://github.com/test/repo",
            "--token",
            "ATOKEN123",
            "--name",
            "my-runner",
            "--labels",
            "gpu,fast",
            "--ephemeral",
        ])
        .unwrap();

        match &cli.command {
            Commands::Configure(args) => {
                assert_eq!(args.url, "https://github.com/test/repo");
                assert_eq!(args.token, "ATOKEN123");
                assert_eq!(args.name.as_deref(), Some("my-runner"));
                assert_eq!(
                    args.labels.as_deref(),
                    Some(&["gpu".to_string(), "fast".to_string()][..])
                );
                assert!(args.ephemeral);
                assert!(!args.replace);
            }
            _ => panic!("expected Configure"),
        }
    }

    #[test]
    fn parse_run_defaults() {
        let cli = Cli::try_parse_from(["aksh-runner", "run"]).unwrap();
        match &cli.command {
            Commands::Run(args) => {
                assert!(!args.once);
                assert_eq!(args.via, ProtocolPath::Broker);
            }
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn parse_run_azdo() {
        let cli = Cli::try_parse_from(["aksh-runner", "run", "--via", "azdo", "--once"]).unwrap();
        match &cli.command {
            Commands::Run(args) => {
                assert!(args.once);
                assert_eq!(args.via, ProtocolPath::Azdo);
            }
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn parse_remove() {
        let cli = Cli::try_parse_from(["aksh-runner", "remove", "--token", "RMTOKEN"]).unwrap();
        match &cli.command {
            Commands::Remove(args) => {
                assert_eq!(args.token, "RMTOKEN");
            }
            _ => panic!("expected Remove"),
        }
    }

    #[test]
    fn parse_worker() {
        let cli = Cli::try_parse_from(["aksh-runner", "worker"]).unwrap();
        match &cli.command {
            Commands::Worker(args) => {
                assert_eq!(args.via, ProtocolPath::Broker);
            }
            _ => panic!("expected Worker"),
        }
    }

    #[test]
    fn version_string_contains_protocol_compat() {
        let version = Cli::command().get_version().unwrap().to_string();
        assert!(version.contains("protocol-compat 2.335.1"));
    }

    #[test]
    fn global_ca_bundle_arg() {
        let cli =
            Cli::try_parse_from(["aksh-runner", "--ca-bundle", "/tmp/ca.pem", "run"]).unwrap();
        assert_eq!(
            cli.global.ca_bundle.unwrap().to_str().unwrap(),
            "/tmp/ca.pem"
        );
    }

    // --- P1 CLI configuration gap coverage ---

    #[test]
    fn parse_configure_replace_flag() {
        let cli = Cli::try_parse_from([
            "aksh-runner",
            "configure",
            "--url",
            "https://github.com/org/repo",
            "--token",
            "TOKEN",
            "--replace",
        ])
        .unwrap();
        match &cli.command {
            Commands::Configure(args) => {
                assert!(args.replace);
                assert!(!args.ephemeral);
            }
            _ => panic!("expected Configure"),
        }
    }

    #[test]
    fn parse_configure_ephemeral_replace() {
        let cli = Cli::try_parse_from([
            "aksh-runner",
            "configure",
            "--url",
            "https://github.com/org/repo",
            "--token",
            "TOKEN",
            "--ephemeral",
            "--replace",
            "--unattended",
        ])
        .unwrap();
        match &cli.command {
            Commands::Configure(args) => {
                assert!(args.ephemeral);
                assert!(args.replace);
                assert!(args.unattended);
            }
            _ => panic!("expected Configure"),
        }
    }

    #[test]
    fn parse_configure_no_externals() {
        let cli = Cli::try_parse_from([
            "aksh-runner",
            "configure",
            "--url",
            "https://github.com/org/repo",
            "--token",
            "TOKEN",
            "--no-externals",
        ])
        .unwrap();
        match &cli.command {
            Commands::Configure(args) => {
                assert!(args.no_externals);
            }
            _ => panic!("expected Configure"),
        }
    }

    #[test]
    fn parse_configure_custom_work_dir() {
        let cli = Cli::try_parse_from([
            "aksh-runner",
            "configure",
            "--url",
            "https://github.com/org/repo",
            "--token",
            "TOKEN",
            "--work",
            "custom_work",
        ])
        .unwrap();
        match &cli.command {
            Commands::Configure(args) => {
                assert_eq!(args.work, "custom_work");
            }
            _ => panic!("expected Configure"),
        }
    }

    #[test]
    fn parse_configure_runner_group() {
        let cli = Cli::try_parse_from([
            "aksh-runner",
            "configure",
            "--url",
            "https://github.com/org/repo",
            "--token",
            "TOKEN",
            "--runner-group",
            "gpu-runners",
        ])
        .unwrap();
        match &cli.command {
            Commands::Configure(args) => {
                assert_eq!(args.runner_group, "gpu-runners");
            }
            _ => panic!("expected Configure"),
        }
    }

    #[test]
    fn parse_configure_defaults() {
        let cli = Cli::try_parse_from([
            "aksh-runner",
            "configure",
            "--url",
            "https://github.com/test/repo",
            "--token",
            "TOKEN",
        ])
        .unwrap();
        match &cli.command {
            Commands::Configure(args) => {
                assert_eq!(args.work, "_work");
                assert_eq!(args.runner_group, "default");
                assert!(!args.replace);
                assert!(!args.ephemeral);
                assert!(!args.unattended);
                assert!(!args.no_externals);
                assert!(args.name.is_none());
                assert!(args.labels.is_none());
            }
            _ => panic!("expected Configure"),
        }
    }

    #[test]
    fn parse_run_once_broker() {
        let cli = Cli::try_parse_from(["aksh-runner", "run", "--once"]).unwrap();
        match &cli.command {
            Commands::Run(args) => {
                assert!(args.once);
                assert_eq!(args.via, ProtocolPath::Broker);
            }
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn parse_worker_azdo() {
        let cli = Cli::try_parse_from(["aksh-runner", "worker", "--via", "azdo"]).unwrap();
        match &cli.command {
            Commands::Worker(args) => {
                assert_eq!(args.via, ProtocolPath::Azdo);
            }
            _ => panic!("expected Worker"),
        }
    }
}
