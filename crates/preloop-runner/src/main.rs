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

/// The remote reusable workflow a queued workflow was fetched from: its
/// `owner/repo/path@ref` key and the ref it is pinned to. `None` marks the
/// submitted repository.
type RemoteOrigin = Option<(String, String)>;

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

    /// Serve `routes` (request target → body) for exactly `expected` requests
    /// and return their request lines. An unknown target gets a 404.
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
        let resolved = with_test_env(
            &[
                ("PRELOOP_GITHUB_API_URL", Some(api_base)),
                ("PRELOOP_GITHUB_TOKEN", None),
            ],
            resolve_remote_workflows(workflows, root),
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
                resolve_remote_workflows(workflows, &root),
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
        let env = [
            (
                "PRELOOP_GITHUB_API_URL",
                Some("http://127.0.0.1:1".to_owned()),
            ),
            ("PRELOOP_GITHUB_TOKEN", None),
        ];
        let workflows = BTreeMap::from([(".github/workflows/ci.yml".to_owned(), CI.to_owned())]);
        let root = "on: push\njobs:\n  call:\n    uses: $/.github/workflows/ci.yml\n";
        let resolved = with_test_env(&env, resolve_remote_workflows(workflows.clone(), root))
            .await
            .expect("a workflow file under .github/workflows/ resolves from the workspace");
        assert_eq!(resolved, workflows);
        for uses in [
            "$/src/x.yml",
            "./../x.yml",
            "$/../../../repos/other/private/contents/.github/workflows/x.yml",
        ] {
            let root = format!("on: push\njobs:\n  call:\n    uses: {uses}\n");
            let result =
                with_test_env(&env, resolve_remote_workflows(workflows.clone(), &root)).await;
            assert!(result.is_err(), "`{uses}` must be refused");
        }
    }
}
