//! Conformance harness for comparing aksh with ChristopherHX/runner.server.

use std::path::PathBuf;
use std::process::Stdio;

use aksh_gha_parser::{expand_jobs, parse_workflow};
use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use tokio::process::Command;
use walkdir::WalkDir;

#[derive(Debug, Parser)]
#[command(name = "aksh-conformance")]
#[command(about = "Run aksh conformance checks against upstream runner.server fixtures")]
struct Cli {
    #[command(subcommand)]
    command: CommandKind,
}

#[derive(Debug, Subcommand)]
enum CommandKind {
    /// Parse and expand all GitHub Actions workflow fixtures that are in scope.
    ExpandFixtures {
        /// Fixture root.
        #[arg(long, default_value = "fixtures/upstream-workflows")]
        fixtures: PathBuf,
    },
    /// Run an upstream command and an aksh command, then compare stdout exactly.
    CompareCommand {
        /// Upstream command executable.
        #[arg(long)]
        upstream: String,
        /// aksh command executable.
        #[arg(long)]
        aksh: String,
        /// Arguments passed to both commands.
        #[arg(last = true)]
        args: Vec<String>,
    },
    /// Placeholder for provider-based Runner.Listener integration tests.
    LibkrunPlan,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        CommandKind::ExpandFixtures { fixtures } => expand_fixtures(fixtures).await,
        CommandKind::CompareCommand {
            upstream,
            aksh,
            args,
        } => compare_command(upstream, aksh, args).await,
        CommandKind::LibkrunPlan => {
            println!("{}", include_str!("libkrun-plan.md"));
            Ok(())
        }
    }
}

async fn expand_fixtures(fixtures: PathBuf) -> anyhow::Result<()> {
    let mut parsed = 0usize;
    let mut skipped = 0usize;
    let mut failures = Vec::new();

    for entry in WalkDir::new(&fixtures)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
    {
        let path = entry.path();
        let Some(ext) = path.extension().and_then(|ext| ext.to_str()) else {
            continue;
        };
        if !matches!(ext, "yml" | "yaml")
            || path.components().any(|c| c.as_os_str() == "azpipelines")
            || path.file_name().is_some_and(|name| name == "action.yml")
        {
            skipped += 1;
            continue;
        }
        let text = tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("read {}", path.display()))?;
        if !looks_like_workflow(&text) {
            skipped += 1;
            continue;
        }
        match parse_workflow(&text)
            .and_then(|workflow| expand_jobs(&workflow).map(|jobs| jobs.len()))
        {
            Ok(job_count) => {
                parsed += 1;
                println!(
                    "{}\t{}",
                    path.strip_prefix(&fixtures).unwrap_or(path).display(),
                    job_count
                );
            }
            Err(error) => failures.push(format!("{}\t{error}", path.display())),
        }
    }

    if !failures.is_empty() {
        for failure in &failures {
            eprintln!("{failure}");
        }
        bail!(
            "fixture expansion failed for {} files; parsed {}, skipped {}",
            failures.len(),
            parsed,
            skipped
        );
    }
    eprintln!("parsed {parsed}, skipped {skipped}");
    Ok(())
}

fn looks_like_workflow(text: &str) -> bool {
    text.lines().any(|line| {
        let trimmed = line.trim_start();
        !line.starts_with(' ') && (trimmed == "jobs:" || trimmed.starts_with("jobs: "))
    })
}

async fn compare_command(upstream: String, aksh: String, args: Vec<String>) -> anyhow::Result<()> {
    let upstream_output = Command::new(&upstream)
        .args(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .with_context(|| format!("run upstream command `{upstream}`"))?;
    let aksh_output = Command::new(&aksh)
        .args(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .with_context(|| format!("run aksh command `{aksh}`"))?;

    if upstream_output.stdout != aksh_output.stdout {
        bail!(
            "stdout mismatch\n--- upstream ---\n{}\n--- aksh ---\n{}",
            String::from_utf8_lossy(&upstream_output.stdout),
            String::from_utf8_lossy(&aksh_output.stdout)
        );
    }
    if upstream_output.status.success() != aksh_output.status.success() {
        bail!(
            "exit status mismatch: upstream={} aksh={}",
            upstream_output.status,
            aksh_output.status
        );
    }
    Ok(())
}
