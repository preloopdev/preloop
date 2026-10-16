//! Preloop runner client binary.

use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;

use anyhow::Context;
use base64::Engine;
use clap::{Parser, Subcommand};
use preloop_gha_protocol::RunId;
use reqwest::Url;
use serde_json::Value;

const MAX_REUSABLE_WORKFLOW_DEPTH: usize = 4;

fn resolve_native_api_token(server: &Url) -> anyhow::Result<String> {
    if let Ok(token) = env::var("PRELOOP_SYSTEM_TOKEN") {
        return Ok(token);
    }

    let is_local_server = matches!(server.host_str(), Some("127.0.0.1" | "localhost" | "::1"));
    if is_local_server {
        let storage_dir = env::var_os("PRELOOP_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(".preloop"));
        if let Some(token) =
            preloop_runner_server::credential_store::load_engine_token(&storage_dir)
                .context("load local engine token")?
        {
            return Ok(token);
        }
    }

    anyhow::bail!(
        "PRELOOP_SYSTEM_TOKEN must be set for runner-client (automatic discovery is limited to local servers)"
    );
}

#[derive(Debug, Parser)]
#[command(name = "preloop")]
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
        #[arg(long, default_value = "local/preloop")]
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
    /// Approve a job waiting on its environment's required-reviewer gate.
    Approve {
        /// Run id.
        run_id: RunId,
        /// Job id within the run.
        job_id: String,
        /// Optional note recorded with the approval.
        #[arg(long)]
        note: Option<String>,
    },
    Rerun {
        run_id: RunId,
    },
    /// Approve a run held by the fork-PR workflow policy
    /// (`[fork_policy] require_approval = true`).
    ApproveFork {
        /// Run id.
        run_id: RunId,
        /// Optional note recorded with the approval.
        #[arg(long)]
        note: Option<String>,
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
    let cli = Cli::parse();
    let timeout_seconds = env::var("PRELOOP_CLIENT_TIMEOUT_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(30);
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_seconds))
        .user_agent("preloop-runner-client")
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
            let native_api_token = resolve_native_api_token(&cli.server)?;
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
            let mut request = http
                .post(cli.server.join("/api/v1/runs")?)
                .bearer_auth(native_api_token);
            // Hand the workspace to the server so it can snapshot the local
            // tree and redirect default-source checkout steps to the snapshot
            // (mirrors `preloop run`). Without this, `actions/checkout` with no
            // `repository:` input tries to clone `github.repository` against
            // the real GitHub host, which fails for local submissions.
            if let Some(root) = &workspace_root {
                let canonical = std::fs::canonicalize(root)
                    .with_context(|| format!("resolve workspace root {}", root.display()))?;
                let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(canonical.as_os_str().to_string_lossy().as_bytes());
                request = request.header("x-preloop-local-workspace", encoded);
            }
            let response = request.json(&submission).send().await?;
            let status = response.status();
            let body = response.text().await?;
            if !status.is_success() {
                anyhow::bail!("server rejected workflow submission ({status}): {body}");
            }
            let response = body;
            println!("{response}");
        }
        Command::Run { run_id } => {
            let native_api_token = resolve_native_api_token(&cli.server)?;
            print_response(
                http.get(cli.server.join(&format!("/api/v1/runs/{run_id}"))?)
                    .bearer_auth(native_api_token)
                    .send()
                    .await?,
            )
            .await?;
        }
        Command::Cancel { run_id } => {
            let native_api_token = resolve_native_api_token(&cli.server)?;
            print_response(
                http.post(cli.server.join(&format!("/api/v1/runs/{run_id}/cancel"))?)
                    .bearer_auth(native_api_token)
                    .send()
                    .await?,
            )
            .await?;
        }
        Command::Approve {
            run_id,
            job_id,
            note,
        } => {
            let native_api_token = resolve_native_api_token(&cli.server)?;
            let body = serde_json::json!({ "note": note });
            print_response(
                http.post(
                    cli.server
                        .join(&format!("/api/v1/runs/{run_id}/jobs/{job_id}/approve"))?,
                )
                .bearer_auth(native_api_token)
                .json(&body)
                .send()
                .await?,
            )
            .await?;
        }
        Command::Rerun { run_id } => {
            let native_api_token = resolve_native_api_token(&cli.server)?;
            print_response(
                http.post(cli.server.join(&format!("/api/v1/runs/{run_id}/rerun"))?)
                    .bearer_auth(native_api_token)
                    .send()
                    .await?,
            )
            .await?;
        }
        Command::ApproveFork { run_id, note } => {
            let native_api_token = resolve_native_api_token(&cli.server)?;
            let body = serde_json::json!({ "note": note });
            print_response(
                http.post(
                    cli.server
                        .join(&format!("/api/v1/runs/{run_id}/approve-fork"))?,
                )
                .bearer_auth(native_api_token)
                .json(&body)
                .send()
                .await?,
            )
            .await?;
        }
        Command::Events { run_id } => {
            let native_api_token = resolve_native_api_token(&cli.server)?;
            print_response(
                http.get(
                    cli.server
                        .join(&format!("/api/v1/runs/{run_id}/events.ndjson"))?,
                )
                .bearer_auth(native_api_token)
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
            let parsed = preloop_gha_parser::parse_workflow(&workflow_yaml)
                .with_context(|| format!("parse workflow {}", workflow.display()))?;
            let reusable_workflows =
                collect_reusable_workflows(workspace_root.as_deref(), &workflow).await?;
            let reusable_workflows =
                resolve_remote_workflows(reusable_workflows, &workflow_yaml, &http).await?;
            let expanded =
                preloop_gha_parser::expand_jobs_with_reusables(&parsed, &reusable_workflows)
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
            return Err(error).with_context(|| format!("read {}", workflow_dir.display()));
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

/// The remote reusable workflow a queued workflow was fetched from: its
/// `owner/repo/path@ref` key and the ref it is pinned to. `None` marks the
/// submitted repository.
type RemoteOrigin = Option<(String, String)>;

async fn resolve_remote_workflows(
    mut workflows: BTreeMap<String, String>,
    root_yaml: &str,
    client: &reqwest::Client,
) -> anyhow::Result<BTreeMap<String, String>> {
    let token = env_or_test("PRELOOP_GITHUB_TOKEN");
    // Same override the engine reads (`PRELOOP_GITHUB_API_URL`), so a client
    // driving a redirected forge fetches reusable workflows from it instead of
    // hardcoded api.github.com.
    let api_base = env_or_test("PRELOOP_GITHUB_API_URL")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "https://api.github.com".to_owned());
    let api_base = api_base.trim_end_matches('/');
    // Each queued workflow carries the remote reference it was fetched from and
    // the ref it is pinned to. `None` marks the submitted repository (the root
    // and workspace files), where a same-repository reference names a workspace
    // file.
    let mut queue: Vec<(String, usize, RemoteOrigin)> = vec![(root_yaml.to_owned(), 0, None)];
    let mut visited = std::collections::BTreeSet::new();
    while let Some((yaml, depth, origin)) = queue.pop() {
        if depth >= MAX_REUSABLE_WORKFLOW_DEPTH {
            anyhow::bail!("nested reusable workflow depth exceeded");
        }
        let workflow = preloop_gha_parser::parse_workflow(&yaml)?;
        for job in workflow.jobs.values() {
            let Some(declared) = job.uses.as_deref() else {
                continue;
            };
            // A same-repository reference inside a fetched workflow names that
            // workflow's own repository at its pinned ref.
            let reference = match &origin {
                Some((key, pin)) if preloop_gha_parser::is_local_workflow_ref(declared) => {
                    preloop_gha_parser::scope_same_repository_ref(declared, key, pin).ok_or_else(
                        || anyhow::anyhow!("unsupported reusable workflow reference `{declared}`"),
                    )?
                }
                _ => declared.to_owned(),
            };
            if preloop_gha_parser::is_local_workflow_ref(&reference) {
                // A workspace file is already in `workflows`, keyed by its path in
                // the repository. The path is validated before the lookup, then the
                // file is scanned once so the remote references it makes are fetched.
                let path = preloop_gha_parser::same_repository_path(&reference).ok_or_else(|| {
                    anyhow::anyhow!(
                        "reusable workflow reference `{reference}` must name a plain path under `.github/workflows/`"
                    )
                })?;
                if visited.insert(reference.clone())
                    && let Some(local) = workflows.get(path)
                {
                    queue.push((local.clone(), depth + 1, None));
                }
                continue;
            }
            if let Some(contents) = workflows.get(&reference).cloned() {
                if visited.insert(reference.clone()) {
                    let pin = reference
                        .rsplit_once('@')
                        .map_or("", |(_, git_ref)| git_ref);
                    queue.push((
                        contents,
                        depth + 1,
                        Some((reference.clone(), pin.to_owned())),
                    ));
                }
                continue;
            }
            let Some((owner, repo, path, git_ref)) = parse_remote_reference(&reference) else {
                anyhow::bail!("unsupported reusable workflow reference `{reference}`");
            };
            if !visited.insert(reference.clone()) {
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
            let pin = git_ref.to_owned();
            let contents = request.send().await?.error_for_status()?.text().await?;
            workflows.insert(reference.clone(), contents.clone());
            queue.push((contents, depth + 1, Some((reference, pin))));
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
    if !preloop_gha_parser::is_plain_segment(owner)
        || !preloop_gha_parser::is_plain_segment(repo)
        || !preloop_gha_parser::is_reusable_workflow_path(path)
        || !preloop_gha_parser::is_plain_path(git_ref)
    {
        return None;
    }
    Some((owner, repo, path, git_ref))
}

async fn print_response(response: reqwest::Response) -> anyhow::Result<()> {
    let text = response.error_for_status()?.text().await?;
    println!("{text}");
    Ok(())
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
    /// on a real reqwest round-trip without pulling a mock-server dependency
    /// into the client crate.
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
        let client = reqwest::Client::new();
        let workflows = with_test_env(
            &[
                ("PRELOOP_GITHUB_API_URL", Some(api_base)),
                ("PRELOOP_GITHUB_TOKEN", None),
            ],
            resolve_remote_workflows(BTreeMap::new(), root, &client),
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

    /// `uses: $/...` names the submitting repository. The gate must leave it
    /// to local expansion instead of parsing it as `owner/repo/path@ref` and
    /// fetching it from the forge. The API base is a closed port, so any
    /// remote request fails the test.
    #[tokio::test]
    async fn resolve_remote_workflows_keeps_self_repository_refs_local() {
        const WORKFLOW: &str = "on: workflow_call\njobs:\n  inner:\n    runs-on: self-hosted\n    steps:\n      - run: echo hi\n";
        let root = "on: push\njobs:\n  call:\n    uses: $/.github/workflows/reusable.yml\n";
        let workflows = BTreeMap::from([(
            ".github/workflows/reusable.yml".to_owned(),
            WORKFLOW.to_owned(),
        )]);
        let client = reqwest::Client::new();
        let resolved = with_test_env(
            &[(
                "PRELOOP_GITHUB_API_URL",
                Some("http://127.0.0.1:1".to_owned()),
            )],
            resolve_remote_workflows(workflows.clone(), root, &client),
        )
        .await
        .unwrap();
        assert_eq!(resolved, workflows);

        let parsed = preloop_gha_parser::parse_workflow(root).unwrap();
        let expanded = preloop_gha_parser::expand_jobs_with_reusables(&parsed, &resolved).unwrap();
        assert_eq!(expanded.jobs.len(), 1);
        assert_eq!(expanded.jobs[0].id.0, "call");
        let call = expanded.jobs[0]
            .reusable_call
            .as_ref()
            .expect("reusable call");
        assert_eq!(call.workflow_file, ".github/workflows/reusable.yml");
    }

    /// Serve `routes` (request target → body) for exactly `expected` requests
    /// and return their request lines. An unknown target gets a 404, which
    /// `error_for_status` turns into a resolution failure.
    async fn serve_routes(
        routes: Vec<(&'static str, &'static str)>,
        expected: usize,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut seen = Vec::new();
            for _ in 0..expected {
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
                let target = request.split_whitespace().nth(1).unwrap_or_default();
                let (status, body) = match routes.iter().find(|(route, _)| *route == target) {
                    Some((_, body)) => ("200 OK", *body),
                    None => ("404 Not Found", ""),
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/vnd.github.raw+json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
                seen.push(request.lines().next().unwrap_or_default().to_owned());
            }
            seen
        });
        (base, handle)
    }

    /// A `$/` callee in the workspace that calls a remote reusable workflow gets
    /// that remote call fetched: the callee's YAML is scanned, not skipped as a
    /// same-repository reference.
    #[tokio::test]
    async fn resolve_remote_workflows_fetches_remote_calls_of_same_repository_callees() {
        const CALLEE: &str = "on: workflow_call\njobs:\n  inner:\n    uses: octo/hello/.github/workflows/inner.yml@main\n";
        const INNER: &str = "on: workflow_call\njobs:\n  leaf:\n    runs-on: self-hosted\n    steps:\n      - run: echo hi\n";
        let (api_base, server) = serve_routes(
            vec![(
                "/repos/octo/hello/contents/.github/workflows/inner.yml?ref=main",
                INNER,
            )],
            1,
        )
        .await;
        let root = "on: push\njobs:\n  call:\n    uses: $/.github/workflows/callee.yml\n";
        let workflows =
            BTreeMap::from([(".github/workflows/callee.yml".to_owned(), CALLEE.to_owned())]);
        let client = reqwest::Client::new();
        let resolved = with_test_env(
            &[
                ("PRELOOP_GITHUB_API_URL", Some(api_base)),
                ("PRELOOP_GITHUB_TOKEN", None),
            ],
            resolve_remote_workflows(workflows, root, &client),
        )
        .await
        .unwrap();

        assert_eq!(
            server.await.unwrap(),
            ["GET /repos/octo/hello/contents/.github/workflows/inner.yml?ref=main HTTP/1.1"]
        );
        assert_eq!(
            resolved["octo/hello/.github/workflows/inner.yml@main"],
            INNER
        );
    }

    /// `$/` inside a remote reusable workflow names that workflow's repository
    /// at its ref. The callee's own copy is fetched, and the submitted tree's
    /// same-named file is never selected for it.
    #[tokio::test]
    async fn resolve_remote_workflows_scopes_nested_same_repository_refs_to_the_callee() {
        const OUTER: &str =
            "on: workflow_call\njobs:\n  nested:\n    uses: $/.github/workflows/inner.yml\n";
        const CALLEE_INNER: &str = "on: workflow_call\njobs:\n  leaf:\n    runs-on: self-hosted\n    steps:\n      - run: echo callee\n";
        const CALLER_INNER: &str = "on: workflow_call\njobs:\n  leaf:\n    runs-on: self-hosted\n    steps:\n      - run: echo caller\n";
        let (api_base, server) = serve_routes(
            vec![
                (
                    "/repos/octo/shared/contents/.github/workflows/outer.yml?ref=v1",
                    OUTER,
                ),
                (
                    "/repos/octo/shared/contents/.github/workflows/inner.yml?ref=v1",
                    CALLEE_INNER,
                ),
            ],
            2,
        )
        .await;
        let root =
            "on: push\njobs:\n  call:\n    uses: octo/shared/.github/workflows/outer.yml@v1\n";
        let workflows = BTreeMap::from([(
            ".github/workflows/inner.yml".to_owned(),
            CALLER_INNER.to_owned(),
        )]);
        let client = reqwest::Client::new();
        let resolved = with_test_env(
            &[
                ("PRELOOP_GITHUB_API_URL", Some(api_base)),
                ("PRELOOP_GITHUB_TOKEN", None),
            ],
            resolve_remote_workflows(workflows, root, &client),
        )
        .await
        .unwrap();
        assert_eq!(server.await.unwrap().len(), 2);
        assert_eq!(
            resolved["octo/shared/.github/workflows/inner.yml@v1"],
            CALLEE_INNER
        );
        assert_eq!(resolved[".github/workflows/inner.yml"], CALLER_INNER);

        // Expansion of the nested call names the callee's repository, so it
        // looks up the copy fetched above.
        let parsed = preloop_gha_parser::parse_workflow(root).unwrap();
        let expanded = preloop_gha_parser::expand_jobs_with_reusables(&parsed, &resolved).unwrap();
        let outer = preloop_gha_parser::parse_workflow(OUTER).unwrap();
        let callee = preloop_gha_parser::expand_reusable_call(
            &outer,
            &expanded.jobs[0],
            &resolved,
            &BTreeMap::new(),
        )
        .unwrap();
        let nested = callee
            .jobs
            .iter()
            .find(|job| job.id.0 == "call/nested")
            .expect("nested call placeholder");
        let call = nested.reusable_call.as_ref().expect("reusable call");
        assert_eq!(call.uses, "octo/shared/.github/workflows/inner.yml@v1");
        assert_eq!(call.workflow_repository.as_deref(), Some("octo/shared"));
    }

    /// Remote references build the contents URL, so the owner, repository, path
    /// and ref must each be plain: a `..` in any of them would escape the named
    /// repository.
    #[test]
    fn remote_reference_components_must_be_plain() {
        assert!(parse_remote_reference("octo/demo/.github/workflows/build.yml@v1").is_some());
        for reference in [
            "octo/demo/.github/workflows/../../../../repos/other/private/contents/x.yml@v1",
            "../demo/.github/workflows/build.yml@v1",
            "octo/../.github/workflows/build.yml@v1",
            "octo/demo/.github/workflows/build.yml@../../repos/other/commits/main",
            "octo/demo/.github/workflows/build.yml@",
        ] {
            assert!(
                parse_remote_reference(reference).is_none(),
                "`{reference}` must be refused"
            );
        }
    }

    /// A top-level `$/` reference must name a plain path under `.github/workflows/`
    /// before the workspace is searched for it. Each candidate below is in the
    /// workspace under the path it would normalize to, so an unchecked lookup
    /// would resolve it.
    #[tokio::test]
    async fn resolve_remote_workflows_refuses_same_repository_paths_that_are_not_plain() {
        const CALLEE: &str = "on: workflow_call\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
        let client = reqwest::Client::new();
        for uses in [
            "$/../../escape.yml",
            "$/.github/workflows/../../escape.yml",
            "$/x.yml",
            "$/.github/workflows/a b.yml",
        ] {
            let workflows = BTreeMap::from([
                ("../../escape.yml".to_owned(), CALLEE.to_owned()),
                (
                    ".github/workflows/../../escape.yml".to_owned(),
                    CALLEE.to_owned(),
                ),
                ("x.yml".to_owned(), CALLEE.to_owned()),
                (".github/workflows/a b.yml".to_owned(), CALLEE.to_owned()),
            ]);
            let root = format!("on: push\njobs:\n  call:\n    uses: {uses}\n");
            let result = with_test_env(
                &[
                    (
                        "PRELOOP_GITHUB_API_URL",
                        Some("http://127.0.0.1:1".to_owned()),
                    ),
                    ("PRELOOP_GITHUB_TOKEN", None),
                ],
                resolve_remote_workflows(workflows, &root, &client),
            )
            .await;
            assert!(result.is_err(), "`{uses}` must be refused");
        }
    }

    /// A top-level `$/` reference resolves from the workspace only when it names a
    /// file under `.github/workflows/`. Every other spelling is refused before the
    /// workspace is searched.
    #[tokio::test]
    async fn resolve_remote_workflows_accepts_only_workflow_files_under_github_workflows() {
        const CI: &str = "on: workflow_call\njobs:\n  ci:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ci\n";
        let client = reqwest::Client::new();
        let env = [
            (
                "PRELOOP_GITHUB_API_URL",
                Some("http://127.0.0.1:1".to_owned()),
            ),
            ("PRELOOP_GITHUB_TOKEN", None),
        ];
        let workflows = BTreeMap::from([(".github/workflows/ci.yml".to_owned(), CI.to_owned())]);
        let root = "on: push\njobs:\n  call:\n    uses: $/.github/workflows/ci.yml\n";
        let resolved = with_test_env(
            &env,
            resolve_remote_workflows(workflows.clone(), root, &client),
        )
        .await
        .expect("a workflow file under .github/workflows/ resolves from the workspace");
        assert_eq!(resolved, workflows);
        for uses in [
            "$/src/x.yml",
            "./../x.yml",
            "$/../../../repos/other/private/contents/.github/workflows/x.yml",
        ] {
            let root = format!("on: push\njobs:\n  call:\n    uses: {uses}\n");
            let result = with_test_env(
                &env,
                resolve_remote_workflows(workflows.clone(), &root, &client),
            )
            .await;
            assert!(result.is_err(), "`{uses}` must be refused");
        }
    }
}
