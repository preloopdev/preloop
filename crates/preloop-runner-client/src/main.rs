//! Preloop runner client binary.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use preloop_gha_protocol::{RunId, SecretString, WorkflowSubmission};
use reqwest::Url;

#[derive(Debug, Parser)]
#[command(name = "preloop")]
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
        /// GitHub event name.
        #[arg(long, default_value = "push")]
        event: String,
        /// Event payload JSON path.
        #[arg(long)]
        payload: Option<PathBuf>,
        /// Repository slug or local id.
        #[arg(long, default_value = "local/preloop")]
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
    let cli = Cli::parse();
    let http = reqwest::Client::new();

    match cli.command {
        Command::Submit {
            workflow,
            event,
            payload,
            repository,
            git_ref,
            vars,
            secrets,
        } => {
            let workflow_yaml = tokio::fs::read_to_string(&workflow)
                .await
                .with_context(|| format!("read workflow {}", workflow.display()))?;
            let payload = match payload {
                Some(path) => {
                    let text = tokio::fs::read_to_string(&path)
                        .await
                        .with_context(|| format!("read payload {}", path.display()))?;
                    serde_json::from_str(&text).with_context(|| format!("parse {}", path.display()))?
                }
                None => serde_json::json!({}),
            };
            let submission = WorkflowSubmission {
                workflow_yaml,
                event,
                payload,
                repository,
                git_ref,
                vars: parse_pairs(vars)?,
                secrets: parse_pairs(secrets)?
                    .into_iter()
                    .map(|(key, value)| (key, SecretString::new(value)))
                    .collect(),
            };
            let response = http
                .post(cli.server.join("/api/v1/runs")?)
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
                    .send()
                    .await?,
            )
            .await?;
        }
        Command::Cancel { run_id } => {
            print_response(
                http.post(cli.server.join(&format!("/api/v1/runs/{run_id}/cancel"))?)
                    .send()
                    .await?,
            )
            .await?;
        }
        Command::Rerun { run_id } => {
            print_response(
                http.post(cli.server.join(&format!("/api/v1/runs/{run_id}/rerun"))?)
                    .send()
                    .await?,
            )
            .await?;
        }
        Command::Events { run_id } => {
            print_response(
                http.get(cli.server.join(&format!("/api/v1/runs/{run_id}/events.ndjson"))?)
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

async fn print_response(response: reqwest::Response) -> anyhow::Result<()> {
    let text = response.error_for_status()?.text().await?;
    println!("{text}");
    Ok(())
}

