//! `preloop-runner` — Rust reimplementation of the GitHub Actions runner.
//!
//! Subcommands: `configure`, `remove`, `run`, `worker` (hidden).

use anyhow::Result;
use clap::Parser;
use std::collections::BTreeMap;

use preloop_runner::cli::{Cli, Commands};

const MAX_REUSABLE_WORKFLOW_DEPTH: usize = 4;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Runner gets structured local logging only — never OTLP export by default.
    // `PRELOOP_LOG_FORMAT` still controls pretty/json/auto for consistency.
    let obs_config = preloop_observability::ObservabilityConfig::from_env()
        .with_service_version(env!("CARGO_PKG_VERSION"));
    let (_observability, observability_runtime) =
        preloop_observability::Observability::from_config(obs_config);
    observability_runtime.install_fmt_subscriber();

    let result = match cli.command {
        Commands::Configure(args) => {
            preloop_runner::configure::run_configure(args, &cli.global).await
        }
        Commands::Remove(args) => preloop_runner::configure::run_remove(args, &cli.global).await,
        Commands::Run(args) => preloop_runner::listener::run_listener(args, &cli.global).await,
        Commands::Worker(args) => preloop_runner::worker::run_worker(args).await,
        Commands::Lint(args) => {
            let workflow_yaml = tokio::fs::read_to_string(&args.workflow)
                .await
                .map_err(|e| anyhow::anyhow!("read workflow {}: {e}", args.workflow.display()))?;
            let parsed = preloop_gha_parser::parse_workflow(&workflow_yaml)
                .map_err(|e| anyhow::anyhow!("parse workflow {}: {e}", args.workflow.display()))?;
            let reusable_workflows =
                collect_reusable_workflows(args.workspace_root.as_deref(), &args.workflow).await?;
            let reusable_workflows =
                resolve_remote_workflows(reusable_workflows, &workflow_yaml).await?;
            let expanded =
                preloop_gha_parser::expand_jobs_with_reusables(&parsed, &reusable_workflows)
                    .map_err(|e| {
                        anyhow::anyhow!("expand workflow {}: {e}", args.workflow.display())
                    })?;

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
    };
    // Bounded 2s flush of buffered telemetry before exit; a clean shutdown
    // must not drop the last flush window's records.
    observability_runtime.shutdown().await;
    result
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
        .user_agent("preloop-runner")
        .build()?;
    let token = env_or_test("PRELOOP_GITHUB_TOKEN");
    // Honour the engine's `PRELOOP_GITHUB_API_URL` override so `lint` against a
    // redirected forge resolves reusable workflows from it, not api.github.com.
    let api_base = env_or_test("PRELOOP_GITHUB_API_URL")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "https://api.github.com".to_owned());
    let api_base = api_base.trim_end_matches('/');
    let mut queue = vec![(root_yaml.to_owned(), 0usize)];
    let mut visited = std::collections::BTreeSet::new();
    while let Some((yaml, depth)) = queue.pop() {
        if depth >= MAX_REUSABLE_WORKFLOW_DEPTH {
            anyhow::bail!("nested reusable workflow depth exceeded");
        }
        let workflow = preloop_gha_parser::parse_workflow(&yaml)?;
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
                    "{api_base}/repos/{owner}/{repo}/contents/{path}?ref={git_ref}"
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

/// `std::env::var` with a test-only thread-local override. Edition 2024 makes
/// `std::env::set_var` unsafe and the workspace denies `unsafe`, so tests
/// redirect reads rather than write the process environment.
fn env_or_test(name: &str) -> Option<String> {
    #[cfg(test)]
    if let Some(value) = TEST_ENV.with(|cell| cell.borrow().get(name).cloned()) {
        // Blank override reads as unset, matching the empty-value filters the
        // production callers apply.
        if !value.trim().is_empty() {
            return Some(value);
        }
        return None;
    }
    std::env::var(name).ok()
}

#[cfg(test)]
thread_local! {
    static TEST_ENV: std::cell::RefCell<std::collections::HashMap<&'static str, String>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
}

/// Run `body` with `vars` visible to `env_or_test` on this thread only.
/// Replaces `std::env::set_var`; the override survives `await` points because
/// `#[tokio::test]` drives the whole future on one thread.
#[cfg(test)]
async fn with_test_env<T>(
    vars: &[(&'static str, Option<String>)],
    body: impl Future<Output = T>,
) -> T {
    let saved = TEST_ENV.with(|cell| cell.borrow().clone());
    TEST_ENV.with(|cell| {
        let mut map = cell.borrow_mut();
        for (name, value) in vars {
            match value {
                Some(value) => {
                    map.insert(*name, value.clone());
                }
                None => {
                    map.insert(*name, String::new());
                }
            }
        }
    });
    let result = body.await;
    TEST_ENV.with(|cell| *cell.borrow_mut() = saved);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serve exactly one HTTP request, capture its request line, and reply
    /// with `body`. A hand-rolled listener keeps `resolve_remote_workflows`
    /// on a real reqwest round-trip without a mock-server dependency.
    async fn serve_once(body: &'static str) -> (String, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 4096];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = socket.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..n]);
            }
            let request = String::from_utf8_lossy(&request).into_owned();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/vnd.github.raw+json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            request
        });
        (base, handle)
    }

    /// `PRELOOP_GITHUB_API_URL` must steer remote `uses:` resolution at the
    /// override, not api.github.com. A reusable workflow reachable only on
    /// the mock resolves under the override — the same call would
    /// `error_for_status` a 404 from the real API.
    #[tokio::test]
    async fn resolve_remote_workflows_honours_api_url_override() {
        const WORKFLOW: &str = "on: workflow_call\njobs:\n  inner:\n    runs-on: self-hosted\n    steps:\n      - run: echo hi\n";
        let (api_base, server) = serve_once(WORKFLOW).await;

        let root =
            "on: push\njobs:\n  call:\n    uses: octo/hello/.github/workflows/reusable.yml@main\n";
        let workflows = with_test_env(
            &[
                ("PRELOOP_GITHUB_API_URL", Some(api_base)),
                ("PRELOOP_GITHUB_TOKEN", None),
            ],
            resolve_remote_workflows(BTreeMap::new(), root),
        )
        .await
        .unwrap();

        let request = server.await.unwrap();
        assert!(
            request.starts_with(
                "GET /repos/octo/hello/contents/.github/workflows/reusable.yml?ref=main "
            ),
            "expected the contents endpoint on the mock, got: {}",
            request.lines().next().unwrap_or("")
        );
        assert_eq!(
            workflows["octo/hello/.github/workflows/reusable.yml@main"],
            WORKFLOW
        );
    }
}
