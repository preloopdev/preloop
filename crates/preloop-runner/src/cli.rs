//! CLI argument definitions.

use clap::{Parser, Subcommand, ValueEnum};
use preloop_gha_protocol::SecretString;
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
    #[arg(long, default_value = "broker", conflicts_with = "jitconfig")]
    pub via: ProtocolPath,

    /// Base64-encoded just-in-time runner config (`encoded_jit_config`).
    ///
    /// Materializes `.runner`/`.credentials`/`.credentials_rsaparams` into the
    /// runner root and runs a single job — the runner is already registered
    /// server-side, so `configure` is not needed (and `--via` is meaningless:
    /// the payload carries its own endpoints).
    ///
    /// The payload holds the runner's private RSA key, so the official
    /// `ACTIONS_RUNNER_INPUT_JITCONFIG` environment variable is the safer
    /// input: argv is world-readable in process listings. An explicit
    /// `--jitconfig` wins over the environment, as in `Runner.Listener`.
    #[arg(long, env = "ACTIONS_RUNNER_INPUT_JITCONFIG", hide_env_values = true)]
    pub jitconfig: Option<String>,
}

impl RunArgs {
    /// Take the JIT payload, if one was supplied.
    ///
    /// Wrapped in a [`SecretString`] — it carries the runner's private RSA key
    /// — and an empty value (a set-but-empty container environment variable) is
    /// treated as absent, matching the official runner's `string.IsNullOrEmpty`
    /// check.
    pub fn take_jit_payload(&mut self) -> Option<SecretString> {
        let payload = self.jitconfig.take()?;
        if payload.trim().is_empty() {
            return None;
        }
        Some(SecretString::new(payload.trim()))
    }
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
            "preloop-runner",
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
        let cli = Cli::try_parse_from(["preloop-runner", "run"]).unwrap();
        match &cli.command {
            Commands::Run(args) => {
                assert!(!args.once);
                assert_eq!(args.via, ProtocolPath::Broker);
            }
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn parse_run_jitconfig() {
        let cli = Cli::try_parse_from(["preloop-runner", "run", "--jitconfig", "encoded-payload"])
            .unwrap();
        match &cli.command {
            Commands::Run(args) => {
                assert_eq!(args.jitconfig.as_deref(), Some("encoded-payload"));
                assert_eq!(args.via, ProtocolPath::Broker);
            }
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn jitconfig_rejects_explicit_protocol_override() {
        let err = Cli::try_parse_from([
            "preloop-runner",
            "run",
            "--jitconfig",
            "encoded-payload",
            "--via",
            "broker",
        ])
        .unwrap_err();
        assert!(
            err.to_string().contains("cannot be used with"),
            "got: {err}"
        );
    }

    #[test]
    fn jitconfig_rejects_registration_flags() {
        for flag in ["--token", "--url"] {
            let result = Cli::try_parse_from([
                "preloop-runner",
                "run",
                "--jitconfig",
                "encoded-payload",
                flag,
                "value",
            ]);
            assert!(
                result.is_err(),
                "{flag} must not be accepted with --jitconfig"
            );
        }
    }

    #[test]
    fn jitconfig_reads_the_official_environment_input() {
        // The payload carries the runner's private key, so the arg must also
        // read `ACTIONS_RUNNER_INPUT_JITCONFIG` and never echo its value.
        // Asserted through the command metadata: this crate cannot call
        // `std::env::set_var` (unsafe under edition 2024, and denied).
        let command = Cli::command();
        let run = command
            .get_subcommands()
            .find(|subcommand| subcommand.get_name() == "run")
            .expect("run subcommand");
        let jitconfig = run
            .get_arguments()
            .find(|arg| arg.get_id() == "jitconfig")
            .expect("jitconfig argument");
        assert_eq!(
            jitconfig
                .get_env()
                .map(|name| name.to_string_lossy().into_owned()),
            Some("ACTIONS_RUNNER_INPUT_JITCONFIG".to_string())
        );
        assert!(jitconfig.is_hide_env_values_set());
    }

    #[test]
    fn take_jit_payload_redacts_and_ignores_blank() {
        let mut args = RunArgs {
            once: false,
            via: ProtocolPath::Broker,
            jitconfig: Some(" configured-payload ".to_string()),
        };
        let payload = args.take_jit_payload().expect("payload");
        assert_eq!(payload.expose(), "configured-payload");
        assert_eq!(format!("{payload:?}"), "SecretString(<redacted>)");
        assert!(
            args.jitconfig.is_none(),
            "payload must not stay in argv state"
        );

        for blank in [String::new(), "   ".to_string()] {
            let mut args = RunArgs {
                once: false,
                via: ProtocolPath::Broker,
                jitconfig: Some(blank.clone()),
            };
            assert!(
                args.take_jit_payload().is_none(),
                "blank payload {blank:?} must read as absent"
            );
        }

        let mut absent = RunArgs {
            once: false,
            via: ProtocolPath::Broker,
            jitconfig: None,
        };
        assert!(absent.take_jit_payload().is_none());
    }

    #[test]
    fn parse_run_azdo() {
        let cli =
            Cli::try_parse_from(["preloop-runner", "run", "--via", "azdo", "--once"]).unwrap();
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
        let cli = Cli::try_parse_from(["preloop-runner", "remove", "--token", "RMTOKEN"]).unwrap();
        match &cli.command {
            Commands::Remove(args) => {
                assert_eq!(args.token, "RMTOKEN");
            }
            _ => panic!("expected Remove"),
        }
    }

    #[test]
    fn parse_worker() {
        let cli = Cli::try_parse_from(["preloop-runner", "worker"]).unwrap();
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
            Cli::try_parse_from(["preloop-runner", "--ca-bundle", "/tmp/ca.pem", "run"]).unwrap();
        assert_eq!(
            cli.global.ca_bundle.unwrap().to_str().unwrap(),
            "/tmp/ca.pem"
        );
    }

    // --- CLI configuration gap coverage ---

    #[test]
    fn parse_configure_replace_flag() {
        let cli = Cli::try_parse_from([
            "preloop-runner",
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
            "preloop-runner",
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
            "preloop-runner",
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
            "preloop-runner",
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
            "preloop-runner",
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
            "preloop-runner",
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
        let cli = Cli::try_parse_from(["preloop-runner", "run", "--once"]).unwrap();
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
        let cli = Cli::try_parse_from(["preloop-runner", "worker", "--via", "azdo"]).unwrap();
        match &cli.command {
            Commands::Worker(args) => {
                assert_eq!(args.via, ProtocolPath::Azdo);
            }
            _ => panic!("expected Worker"),
        }
    }
}
