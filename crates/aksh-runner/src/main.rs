//! `aksh-runner` — Rust reimplementation of the GitHub Actions runner.
//!
//! Subcommands: `configure`, `remove`, `run`, `worker` (hidden).

use anyhow::Result;
use clap::Parser;
use std::collections::BTreeMap;

use aksh_runner::cli::{Cli, Commands};

const MAX_REUSABLE_WORKFLOW_DEPTH: usize = 4;

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
            let reusable_workflows =
                resolve_remote_workflows(reusable_workflows, &workflow_yaml).await?;
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

async fn resolve_remote_workflows(
    mut workflows: BTreeMap<String, String>,
    root_yaml: &str,
) -> Result<BTreeMap<String, String>> {
    let client = reqwest::Client::builder()
        .user_agent("aksh-runner")
        .build()?;
    let token = std::env::var("AKSH_GITHUB_TOKEN").ok();
    let mut queue = vec![(root_yaml.to_owned(), 0usize)];
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
            if reference.starts_with("./") || workflows.contains_key(reference) {
                continue;
            }
            let Some((owner, repo, path, git_ref)) = parse_remote_reference(reference) else {
                anyhow::bail!("unsupported reusable workflow reference `{reference}`");
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
