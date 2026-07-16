//! Preloop runner client binary.

use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;

use aksh_gha_protocol::RunId;
use anyhow::Context;
use clap::{Parser, Subcommand};
use reqwest::Url;
use serde_json::Value;

#[derive(Debug, Parser)]
#[command(name = "aksh")]
#[command(about = "Submit and manage local Preloop GitHub Actions runs")]
struct Cli {
    /// Server base URL.
    #[arg(long, env = "PRELOOP_SERVER", default_value = "http://127.0.0.1:8080")]
    server: Url,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Submit a workflow.
    Submit {
        /// Workflow YAML path.
        #[arg(short = 'W', long)]
        workflow: PathBuf,
        /// Repository workspace root used to collect local reusable workflows.
        #[arg(long)]
        workspace_root: Option<PathBuf>,
        /// GitHub event name.
        #[arg(long, default_value = "push")]
        event: String,
        /// Event payload JSON path.
        #[arg(long)]
        payload: Option<PathBuf>,
        /// Repository slug or local id.
        #[arg(long, default_value = "local/aksh")]
        repository: String,
        /// Git ref.
        #[arg(long, default_value = "refs/heads/main")]
        git_ref: String,
        /// Variable in KEY=VALUE form.
        #[arg(long = "var")]
        vars: Vec<String>,
        /// Secret in KEY=VALUE form. Values are redacted in JSON output.
        #[arg(long = "secret")]
        secrets: Vec<String>,
        /// Enable the DAP debugger for this run.
        #[arg(long)]
        debug: bool,
        /// Welcome message displayed by the debugger after attach.
        #[arg(long)]
        debugger_welcome_message: Option<String>,
    },
    /// Show a run.
    Run {
        /// Run id.
        run_id: RunId,
    },
    /// Cancel a run.
    Cancel {
        /// Run id.
        run_id: RunId,
    },
    /// Rerun a previous submission.
    Rerun {
        /// Run id.
        run_id: RunId,
    },
    /// Print current NDJSON events for a run.
    Events {
        /// Run id.
        run_id: RunId,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let native_api_token =
        env::var("AKSH_SYSTEM_TOKEN").unwrap_or_else(|_| "aksh-system-token".to_owned());
    let cli = Cli::parse();
    let http = reqwest::Client::new();

    match cli.command {
        Command::Submit {
            workflow,
            workspace_root,
            event,
            payload,
            repository,
            git_ref,
            vars,
            secrets,
            debug,
            debugger_welcome_message,
        } => {
            let workflow_yaml = tokio::fs::read_to_string(&workflow)
                .await
                .with_context(|| format!("read workflow {}", workflow.display()))?;
            let payload = match payload {
                Some(path) => {
                    let text = tokio::fs::read_to_string(&path)
                        .await
                        .with_context(|| format!("read payload {}", path.display()))?;
                    serde_json::from_str(&text)
                        .with_context(|| format!("parse {}", path.display()))?
                }
                None => serde_json::json!({}),
            };
            let submission = SubmitWire {
                workflow_yaml,
                event,
                payload,
                repository,
                git_ref,
                vars: parse_pairs(vars)?,
                secrets: parse_pairs(secrets)?,
                reusable_workflows: collect_reusable_workflows(
                    workspace_root.as_deref(),
                    &workflow,
                )
                .await?,
                enable_debugger: debug,
                debugger_welcome_message,
            };
            let response = http
                .post(cli.server.join("/api/v1/runs")?)
                .bearer_auth(&native_api_token)
                .json(&submission)
                .send()
                .await?
                .error_for_status()?
                .text()
                .await?;
            println!("{response}");
        }
        Command::Run { run_id } => {
            print_response(
                http.get(cli.server.join(&format!("/api/v1/runs/{run_id}"))?)
                    .bearer_auth(&native_api_token)
                    .send()
                    .await?,
            )
            .await?;
        }
        Command::Cancel { run_id } => {
            print_response(
                http.post(cli.server.join(&format!("/api/v1/runs/{run_id}/cancel"))?)
                    .bearer_auth(&native_api_token)
                    .send()
                    .await?,
            )
            .await?;
        }
        Command::Rerun { run_id } => {
            print_response(
                http.post(cli.server.join(&format!("/api/v1/runs/{run_id}/rerun"))?)
                    .bearer_auth(&native_api_token)
                    .send()
                    .await?,
            )
            .await?;
        }
        Command::Events { run_id } => {
            print_response(
                http.get(
                    cli.server
                        .join(&format!("/api/v1/runs/{run_id}/events.ndjson"))?,
                )
                .bearer_auth(&native_api_token)
                .send()
                .await?,
            )
            .await?;
        }
    }

    Ok(())
}

fn parse_pairs(values: Vec<String>) -> anyhow::Result<BTreeMap<String, String>> {
    values
        .into_iter()
        .map(|value| {
            let (key, val) = value
                .split_once('=')
                .with_context(|| format!("expected KEY=VALUE, got `{value}`"))?;
            Ok((key.to_owned(), val.to_owned()))
        })
        .collect()
}

#[derive(serde::Serialize)]
struct SubmitWire {
    workflow_yaml: String,
    event: String,
    payload: Value,
    repository: String,
    git_ref: String,
    vars: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
    reusable_workflows: BTreeMap<String, String>,
    enable_debugger: bool,
    debugger_welcome_message: Option<String>,
}

async fn collect_reusable_workflows(
    workspace_root: Option<&std::path::Path>,
    submitted_workflow: &std::path::Path,
) -> anyhow::Result<BTreeMap<String, String>> {
    let Some(root) = workspace_root else {
        return Ok(BTreeMap::new());
    };
    let workflow_dir = root.join(".github").join("workflows");
    let mut out = BTreeMap::new();
    let mut entries = match tokio::fs::read_dir(&workflow_dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(error) => {
            return Err(error).with_context(|| format!("read {}", workflow_dir.display()))
        }
    };
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if !matches!(
            path.extension().and_then(|ext| ext.to_str()),
            Some("yml" | "yaml")
        ) || same_file_path(&path, submitted_workflow)
        {
            continue;
        }
        let relative = path
            .strip_prefix(root)
            .with_context(|| format!("make {} relative to {}", path.display(), root.display()))?
            .to_string_lossy()
            .into_owned();
        let yaml = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("read reusable workflow {}", path.display()))?;
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

async fn print_response(response: reqwest::Response) -> anyhow::Result<()> {
    let text = response.error_for_status()?.text().await?;
    println!("{text}");
    Ok(())
}
