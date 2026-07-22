//! Preloop runner client binary.

use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;

use aksh_gha_protocol::RunId;
use anyhow::Context;
use clap::{Parser, Subcommand};
use reqwest::Url;
use serde_json::Value;

const MAX_REUSABLE_WORKFLOW_DEPTH: usize = 4;

#[derive(Debug, Parser)]
#[command(name = "aksh")]
#[command(about = "Submit and manage local Preloop GitHub Actions runs")]
struct Cli {
    #[arg(long, env = "PRELOOP_SERVER", default_value = "http://127.0.0.1:8080")]
    server: Url,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)]
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
        #[arg(long, default_value = "refs/heads/main")]
        git_ref: String,
        /// Variable in KEY=VALUE form.
        #[arg(long = "var")]
        vars: Vec<String>,
        /// Secret in KEY=VALUE form. Values are redacted in JSON output.
        #[arg(long = "secret")]
        secrets: Vec<String>,
        /// Workflow dispatch input in KEY=VALUE form (value treated as JSON, falling back to string).
        #[arg(long = "input")]
        inputs: Vec<String>,
        /// Enable the DAP debugger for this run.
        #[arg(long)]
        debug: bool,
        /// Welcome message displayed by the debugger after attach.
        #[arg(long)]
        debugger_welcome_message: Option<String>,
    },
    Run {
        /// Run d.
        run_id: RunId,
    },
    Cancel {
        run_id: RunId,
    },
    Rerun {
        run_id: RunId,
    },
    Events {
        /// Run id.
        run_id: RunId,
    },
    /// Lint and dry-run workflow parsing and job expansion without running it.
    Lint {
        #[arg(short = 'W', long)]
        workflow: PathBuf,
        /// Repository workspace root used to collect local reusable workflows.
        #[arg(long)]
        workspace_root: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let native_api_token =
        env::var("AKSH_SYSTEM_TOKEN").unwrap_or_else(|_| "aksh-system-token".to_owned());
    let cli = Cli::parse();
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .user_agent("aksh-runner-client")
        .build()?;

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
            inputs,
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
            let inputs: BTreeMap<String, Value> = inputs
                .into_iter()
                .map(|kv| {
                    let (k, v) = kv
                        .split_once('=')
                        .map(|(k, v)| (k.to_owned(), v.to_owned()))
                        .unwrap_or_else(|| (kv.clone(), String::new()));
                    let json_val = serde_json::from_str::<Value>(&v).unwrap_or(Value::String(v));
                    (k, json_val)
                })
                .collect();
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
                inputs,
            };
            let response = http
                .post(cli.server.join("/api/v1/runs")?)
                .bearer_auth(&native_api_token)
                .json(&submission)
                .send()
                .await?;
            let status = response.status();
            let body = response.text().await?;
            if !status.is_success() {
                anyhow::bail!("Server returned {} - Error: {}", status, body);
            }
            println!("{body}");
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
        Command::Lint {
            workflow,
            workspace_root,
        } => {
            let workflow_yaml = tokio::fs::read_to_string(&workflow)
                .await
                .with_context(|| format!("read workflow {}", workflow.display()))?;
            let parsed = aksh_gha_parser::parse_workflow(&workflow_yaml)
                .with_context(|| format!("parse workflow {}", workflow.display()))?;
            let reusable_workflows =
                collect_reusable_workflows(workspace_root.as_deref(), &workflow).await?;
            let reusable_workflows =
                resolve_remote_workflows(reusable_workflows, &workflow_yaml).await?;
            let expanded =
                aksh_gha_parser::expand_jobs_with_reusables(&parsed, &reusable_workflows)
                    .with_context(|| format!("expand workflow {}", workflow.display()))?;

            let step_count: usize = expanded.jobs.iter().map(|j| j.steps.len()).sum();
            println!(
                "✓ Workflow {} is valid: parsed {} job plan(s) and {} total step(s).",
                workflow.display(),
                expanded.jobs.len(),
                step_count
            );
            for job in &expanded.jobs {
                println!("  - Job: {} ({})", job.id.0, job.name);
            }
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
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    inputs: BTreeMap<String, Value>,
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

async fn resolve_remote_workflows(
    mut workflows: BTreeMap<String, String>,
    root_yaml: &str,
) -> anyhow::Result<BTreeMap<String, String>> {
    let client = reqwest::Client::builder()
        .user_agent("aksh-runner-client")
        .build()?;
    let token = std::env::var("AKSH_GITHUB_TOKEN").ok();
    let mut queue = vec![(root_yaml.to_owned(), 0usize)];
    // Seed the queue with locally-resolved workflows so their remote
    // references are also discovered and fetched.
    for contents in workflows.values() {
        queue.push((contents.clone(), 0usize));
    }
    let mut visited = std::collections::BTreeSet::new();
    while let Some((yaml, depth)) = queue.pop() {
        if depth >= MAX_REUSABLE_WORKFLOW_DEPTH {
            anyhow::bail!("nested reusable workflow depth exceeded");
        }
        let workflow = aksh_gha_parser::parse_workflow(&yaml)?;
        for job in workflow.jobs.values() {
            let Some(reference) = job.uses.as_deref() else {
                continue;
            };
            if reference.starts_with("./") {
                continue;
            }
            if let Some(contents) = workflows.get(reference).cloned() {
                if visited.insert(reference.to_owned()) {
                    queue.push((contents, depth + 1));
                }
                continue;
            }
            let Some((owner, repo, path, git_ref)) = parse_remote_reference(reference) else {
                // Not a reusable workflow reference (e.g. a regular composite action);
                // skip instead of bailing.
                continue;
            };
            if !visited.insert(reference.to_owned()) {
                continue;
            }
            let mut request = client
                .get(format!(
                    "https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={git_ref}"
                ))
                .header(reqwest::header::ACCEPT, "application/vnd.github.raw+json");
            if let Some(token) = token.as_deref() {
                request = request.bearer_auth(token);
            }
            let contents = request.send().await?.error_for_status()?.text().await?;
            workflows.insert(reference.to_owned(), contents.clone());
            queue.push((contents, depth + 1));
        }
    }
    Ok(workflows)
}

fn parse_remote_reference(reference: &str) -> Option<(&str, &str, &str, &str)> {
    let (repository_path, git_ref) = reference.rsplit_once('@')?;
    let mut parts = repository_path.splitn(3, '/');
    let owner = parts.next()?;
    let repo = parts.next()?;
    let path = parts.next()?;
    if owner.is_empty() || repo.is_empty() || !path.starts_with(".github/workflows/") {
        return None;
    }
    Some((owner, repo, path, git_ref))
}

async fn print_response(response: reqwest::Response) -> anyhow::Result<()> {
    let text = response.error_for_status()?.text().await?;
    println!("{text}");
    Ok(())
}
