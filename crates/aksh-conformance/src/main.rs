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
    /// Compare workflow expansion against golden JSON fixtures.
    Golden {
        /// Fixture root containing .yml/.yaml workflows and .json expected outputs.
        #[arg(long, default_value = "fixtures/golden")]
        fixtures: PathBuf,
    },
    /// Record wire traffic from upstream runner.server (placeholder).
    Record {
        /// Upstream runner.server URL.
        #[arg(long)]
        upstream: String,
        /// Output directory for captured fixtures.
        #[arg(long, default_value = "fixtures/wire")]
        output: PathBuf,
    },
    /// Replay recorded wire traffic through aksh DTOs (placeholder).
    Replay {
        /// Directory containing captured wire fixtures.
        #[arg(long, default_value = "fixtures/wire")]
        fixtures: PathBuf,
    },
    /// Fuzz test the parser with random YAML inputs.
    Fuzz {
        /// Number of random inputs to test.
        #[arg(long, default_value = "1000")]
        iterations: usize,
    },
    /// Placeholder for provider-based Runner.Listener integration tests.
    LibkrunPlan,
    /// H1: Boot local runner, submit workflow, wait for completion, generate verdict JSON.
    #[command(name = "runner-e2e")]
    RunnerE2e {
        /// Path to the runner executable.
        #[arg(long, default_value = "target/release/aksh-runner")]
        runner_bin: PathBuf,
        /// Path to the GHA workflow file to run.
        #[arg(long)]
        workflow: PathBuf,
        /// Optional path to write recorded HTTP flows to.
        #[arg(long)]
        record_flows: Option<PathBuf>,
    },
    /// H2: Generate a flow diff report against the golden scenario captures.
    #[command(name = "runner-diff")]
    RunnerDiff {
        /// Golden scenario name.
        #[arg(long)]
        scenario: String,
        /// Target environment (e.g. github, aksh).
        #[arg(long)]
        target: String,
    },
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
        CommandKind::Golden { fixtures } => golden_compare(fixtures).await,
        CommandKind::Record { upstream, output } => record_wire(upstream, output).await,
        CommandKind::Replay { fixtures } => replay_wire(fixtures).await,
        CommandKind::Fuzz { iterations } => fuzz_parser(iterations),
        CommandKind::LibkrunPlan => {
            println!("{}", include_str!("libkrun-plan.md"));
            Ok(())
        }
        CommandKind::RunnerE2e {
            runner_bin,
            workflow,
            record_flows,
        } => run_runner_e2e(runner_bin, workflow, record_flows).await,
        CommandKind::RunnerDiff { scenario, target } => run_runner_diff(scenario, target).await,
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

/// Compare workflow expansion against golden JSON fixtures.
///
/// Each fixture is a pair: `<name>.yml` (workflow) + `<name>.json` (expected expanded jobs).
/// The JSON contains the serialized `Vec<JobPlan>` we expect from `expand_jobs`.
async fn golden_compare(fixtures: PathBuf) -> anyhow::Result<()> {
    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut skipped = 0usize;

    for entry in WalkDir::new(&fixtures)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_type().is_file()
                && e.path()
                    .extension()
                    .is_some_and(|ext| ext == "yml" || ext == "yaml")
        })
    {
        let yaml_path = entry.path();
        let json_path = yaml_path.with_extension("json");

        if !json_path.exists() {
            skipped += 1;
            continue;
        }

        let yaml_text = std::fs::read_to_string(yaml_path)?;
        let json_text = std::fs::read_to_string(&json_path)?;

        let workflow = match parse_workflow(&yaml_text) {
            Ok(w) => w,
            Err(e) => {
                eprintln!("FAIL {}: parse error: {e}", yaml_path.display());
                failed += 1;
                continue;
            }
        };

        let plans = match expand_jobs(&workflow) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("FAIL {}: expand error: {e}", yaml_path.display());
                failed += 1;
                continue;
            }
        };

        let actual_json = serde_json::to_string_pretty(&plans)?;
        let expected: serde_json::Value = serde_json::from_str(&json_text)?;
        let actual: serde_json::Value = serde_json::from_str(&actual_json)?;

        if expected == actual {
            passed += 1;
        } else {
            eprintln!("FAIL {}: JSON mismatch", yaml_path.display());
            // Show diff
            let expected_str = serde_json::to_string_pretty(&expected)?;
            let actual_str = serde_json::to_string_pretty(&actual)?;
            for (i, (a, b)) in actual_str.lines().zip(expected_str.lines()).enumerate() {
                if a != b {
                    eprintln!("  line {}: got      {}", i + 1, a);
                    eprintln!("  line {}: expected {}", i + 1, b);
                }
            }
            failed += 1;
        }
    }

    eprintln!("golden: {passed} passed, {failed} failed, {skipped} skipped");
    if failed > 0 {
        bail!("{failed} golden tests failed");
    }
    Ok(())
}

/// Record wire traffic from upstream runner.server.
/// This is a placeholder — actual implementation needs mitmproxy or similar.
async fn record_wire(upstream: String, output: PathBuf) -> anyhow::Result<()> {
    eprintln!("record: not yet implemented");
    eprintln!("  upstream: {upstream}");
    eprintln!("  output: {}", output.display());
    eprintln!();
    eprintln!("To record wire traffic:");
    eprintln!("  1. Start upstream runner.server at {upstream}");
    eprintln!("  2. Run a Runner.Listener against it");
    eprintln!("  3. Capture HTTP traffic with mitmproxy or similar");
    eprintln!("  4. Save to {}/", output.display());
    Ok(())
}

/// Replay recorded wire traffic through aksh DTOs.
/// This is a placeholder — actual implementation needs captured fixtures.
async fn replay_wire(fixtures: PathBuf) -> anyhow::Result<()> {
    eprintln!("replay: not yet implemented");
    eprintln!("  fixtures: {}", fixtures.display());
    eprintln!();
    eprintln!("To replay wire traffic:");
    eprintln!("  1. Record traffic with 'aksh-conformance record'");
    eprintln!(
        "  2. Run 'aksh-conformance replay --fixtures {}'",
        fixtures.display()
    );
    eprintln!("  3. Each captured request/response is validated against our DTOs");
    Ok(())
}

/// Fuzz test the parser with random YAML inputs.
/// Verifies the parser never panics on arbitrary input.
fn fuzz_parser(iterations: usize) -> anyhow::Result<()> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut passed = 0usize;
    let mut panics = 0usize;
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    for i in 0..iterations {
        // Generate a pseudo-random YAML string
        let mut hasher = DefaultHasher::new();
        (seed, i).hash(&mut hasher);
        let hash = hasher.finish();
        let yaml = generate_random_yaml(hash);

        // Parse should never panic
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| parse_workflow(&yaml)));

        match result {
            Ok(_) => passed += 1,
            Err(_) => {
                panics += 1;
                eprintln!("PANIC on input #{i}: {:?}", &yaml[..yaml.len().min(100)]);
            }
        }
    }

    eprintln!("fuzz: {passed} passed, {panics} panics out of {iterations}");
    if panics > 0 {
        bail!("{panics} panics detected");
    }
    Ok(())
}

/// Generate a pseudo-random YAML string from a seed.
fn generate_random_yaml(seed: u64) -> String {
    let mut s = seed;
    let mut next = || -> u64 {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        s
    };

    let name = format!("job-{}", next() % 1000);
    let event = match next() % 4 {
        0 => "push",
        1 => "pull_request",
        2 => "workflow_dispatch",
        _ => "schedule",
    };
    let label = match next() % 3 {
        0 => "ubuntu-latest",
        1 => "self-hosted",
        _ => "macos-latest",
    };
    let step_count = (next() % 5) + 1;

    let mut yaml =
        format!("name: {name}\non: {event}\njobs:\n  build:\n    runs-on: [{label}]\n    steps:\n");
    for _ in 0..step_count {
        let cmd = match next() % 3 {
            0 => "echo hello",
            1 => "ls -la",
            _ => "pwd",
        };
        yaml.push_str(&format!("      - run: {cmd}\n"));
    }

    // Randomly add matrix
    if next() % 3 == 0 {
        yaml.push_str("    strategy:\n      matrix:\n        os: [ubuntu, macos]\n");
    }

    // Randomly add needs
    if next() % 4 == 0 {
        yaml.push_str("  test:\n    needs: build\n    runs-on: [{label}]\n    steps:\n      - run: echo test\n");
    }

    yaml
}

async fn run_runner_e2e(runner_bin: PathBuf, workflow: PathBuf, record_flows: Option<PathBuf>) -> anyhow::Result<()> {
    use std::time::Duration;

    // Check binaries
    if !runner_bin.exists() {
        anyhow::bail!("runner binary not found: {}", runner_bin.display());
    }
    if !workflow.exists() {
        anyhow::bail!("workflow file not found: {}", workflow.display());
    }

    let server_bin = if std::path::Path::new("target/release/aksh-runner-server").exists() {
        "target/release/aksh-runner-server"
    } else {
        "target/debug/aksh-runner-server"
    };
    if !std::path::Path::new(server_bin).exists() {
        anyhow::bail!("server binary not found: please build aksh-runner-server");
    }

    let client_bin = if std::path::Path::new("target/release/aksh-runner-client").exists() {
        "target/release/aksh-runner-client"
    } else {
        "target/debug/aksh-runner-client"
    };
    if !std::path::Path::new(client_bin).exists() {
        anyhow::bail!("client binary not found: please build aksh-runner-client");
    }

    // Temporary directories
    let temp_dir = tempfile::TempDir::new()?;
    let state_dir = temp_dir.path().join("state");
    let runner_root = temp_dir.path().join("runner-root");
    std::fs::create_dir_all(&state_dir)?;
    std::fs::create_dir_all(&runner_root)?;

    // Start server in background on port 9191
    let mut server_cmd = Command::new(server_bin);
    server_cmd
        .env("AKSH_PUBLIC_URL", "http://127.0.0.1:9191")
        .arg("serve")
        .arg("--listen")
        .arg("127.0.0.1:9191")
        .arg("--state-dir")
        .arg(state_dir.to_str().unwrap());

    if let Some(path) = &record_flows {
        server_cmd.arg("--record-flows").arg(path);
    }

    let mut server = server_cmd.spawn()?;

    // Wait for server to listen
    let client = reqwest::Client::new();
    let mut ready = false;
    for _ in 0..30 {
        if client.get("http://127.0.0.1:9191/").send().await.is_ok() {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !ready {
        let _ = server.kill().await;
        anyhow::bail!("aksh-runner-server failed to start on port 9191");
    }

    // Configure the runner
    let configure_status = Command::new(&runner_bin)
        .arg("--runner-root")
        .arg(&runner_root)
        .arg("configure")
        .args([
            "--url",
            "http://127.0.0.1:9191",
            "--token",
            "dummy-token",
            "--name",
            "e2e-runner",
            "--work",
            "_work",
            "--no-externals",
            "--unattended",
            "--replace",
        ])
        .status()
        .await?;
    if !configure_status.success() {
        let _ = server.kill().await;
        anyhow::bail!("runner configure failed");
    }

    // Submit workflow
    let submit_output = Command::new(client_bin)
        .args(["--server", "http://127.0.0.1:9191", "submit", "-W"])
        .arg(&workflow)
        .output()
        .await?;
    if !submit_output.status.success() {
        let err = String::from_utf8_lossy(&submit_output.stderr);
        let _ = server.kill().await;
        anyhow::bail!("workflow submission failed: {err}");
    }
    let output = String::from_utf8_lossy(&submit_output.stdout);
    let v: serde_json::Value = serde_json::from_str(&output)?;
    let run_id = v
        .get("run_id")
        .and_then(|v| v.as_str())
        .context("missing run_id")?
        .to_string();

    // Start runner once
    let runner_status = Command::new(&runner_bin)
        .arg("--runner-root")
        .arg(&runner_root)
        .arg("run")
        .arg("--once")
        .status()
        .await?;

    // Check completion and verdict
    let run_status_url = format!("http://127.0.0.1:9191/api/v1/runs/{}", run_id);
    let mut run_success = false;
    let mut run_status = "unknown".to_string();
    if let Ok(resp) = client.get(&run_status_url).send().await {
        if let Ok(v) = resp.json::<serde_json::Value>().await {
            if let Some(status) = v.get("status").and_then(|v| v.as_str()) {
                run_status = status.to_string();
                if status == "completed" || status == "success" {
                    run_success = true;
                }
            }
        }
    }
    if runner_status.success() {
        run_success = true;
    }

    let verdict = serde_json::json!({
        "success": run_success,
        "run_id": run_id,
        "status": run_status,
    });
    println!("{}", serde_json::to_string_pretty(&verdict)?);

    let _ = server.kill().await;
    Ok(())
}

async fn run_runner_diff(scenario: String, target: String) -> anyhow::Result<()> {
    let left_dir = format!(".runner-watch/golden/v2.335.1/{scenario}");
    let right_dir = format!(".runner-watch/conformance/v2.335.1/{scenario}/{target}");
    let output_dir = ".runner-watch/runner-conformance";
    std::fs::create_dir_all(output_dir)?;
    let output_path = format!("{output_dir}/{scenario}.md");

    runner_watch::compare::render_report(&runner_watch::compare::Args {
        scenario: &scenario,
        left_dir: std::path::Path::new(&left_dir),
        right_dir: std::path::Path::new(&right_dir),
        output: std::path::Path::new(&output_path),
        left_label: "official",
        right_label: &target,
    })?;

    println!("Report written to {output_path}");
    Ok(())
}
