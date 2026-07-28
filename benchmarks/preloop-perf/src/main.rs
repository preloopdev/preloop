//! Preloop comprehensive performance benchmark.
//!
//! Exercises the server control plane, parser, expression evaluator, and
//! workspace snapshotting under load. Emits `METRIC name=value` lines to
//! stdout for the autoresearch harness.
//!
//! Subcommands:
//!   server-load   — concurrent workflow submissions + polling through the
//!                   in-process axum router (zero network overhead).
//!   parser        — parse + expand a matrix workflow N times.
//!   expressions   — evaluate a battery of expressions N times.
//!   snapshot      — create workspace snapshots for varying project sizes.
//!   all           — run every benchmark and emit all metrics.

use anyhow::{Context, Result};
use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use serde_json::{json, Value};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

// ── constants ───────────────────────────────────────────────────────────────

const SYSTEM_TOKEN: &str = "aksh-system-token";

const SIMPLE_WORKFLOW: &str = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hello
"#;

const MATRIX_WORKFLOW: &str = r#"
name: matrix-bench
on: push
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        shard: [0, 1, 2, 3]
      fail-fast: false
    steps:
      - run: echo "shard ${{ matrix.shard }}"
      - run: |
          for i in $(seq 1 100); do
            echo "line $i"
          done
"#;

const COMPLEX_WORKFLOW: &str = r#"
name: complex-ci
on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: echo "linting"
  test:
    needs: lint
    runs-on: ubuntu-latest
    strategy:
      matrix:
        os: [ubuntu-latest]
        node: ['18', '20', '22']
      fail-fast: true
      max-parallel: 2
    steps:
      - uses: actions/checkout@v4
      - run: echo "testing node ${{ matrix.node }} on ${{ matrix.os }}"
  build:
    needs: [lint, test]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: echo "building"
  deploy:
    needs: build
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: production
    steps:
      - run: echo "deploying"
"#;

const EXPRESSION_BATTERY: &[&str] = &[
    "github.event_name == 'push'",
    "contains(github.ref, 'main')",
    "startsWith(github.repository, 'owner/')",
    "endsWith(github.ref, '/main')",
    "format('{0}-{1}', github.run_id, github.run_number)",
    "join(github.event.commits.*.message, ', ')",
    "toJSON(github.event)",
    "fromJSON('{\"key\": \"value\"}').key",
    "hashFiles('**/*.rs')",
    "always()",
    "success()",
    "failure()",
    "cancelled()",
    "github.event_name == 'push' && github.ref == 'refs/heads/main'",
    "github.event_name == 'pull_request' || github.event_name == 'push'",
    "!(github.event.pull_request.draft)",
    "matrix.os == 'ubuntu-latest' && matrix.node == '20'",
    "needs.build.result == 'success'",
    "steps.test.outcome == 'success'",
    "runner.os == 'Linux'",
];

// ── helpers ─────────────────────────────────────────────────────────────────

async fn make_app(state_dir: &Path) -> Result<(Router, aksh_runner_server::AppState)> {
    let state = aksh_runner_server::AppState::new(state_dir.to_path_buf())
        .await
        .context("creating AppState")?;
    let shutdown = CancellationToken::new();
    let app = aksh_runner_server::app_with_test_api(state.clone(), shutdown, "test-token");
    Ok((app, state))
}

async fn request(app: &Router, method: Method, uri: &str, body: Value) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    builder = builder.header("authorization", format!("Bearer {SYSTEM_TOKEN}"));
    let request = if body.is_null() {
        builder.body(Body::empty()).unwrap()
    } else {
        builder
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

fn metric(name: &str, value: f64, digits: u32) {
    let factor = 10f64.powi(digits as i32);
    println!("METRIC {}={}", name, (value * factor).round() / factor);
}

// ── server load benchmark ───────────────────────────────────────────────────

async fn bench_server_load(state_dir: &Path) -> Result<()> {
    eprintln!("[loadtest] === Server Load Benchmark ===");

    let (app, _state) = make_app(state_dir).await?;
    let app = Arc::new(app);

    // Warmup: a few submissions to prime internal state
    for _ in 0..3 {
        let (status, _) = request(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": SIMPLE_WORKFLOW,
                "event": "push",
                "repository": "bench/repo"
            }),
        )
        .await;
        assert!(status.is_success(), "warmup failed: {status}");
    }

    // ── Phase 1: Sequential baseline ────────────────────────────────────
    let sequential_count = 100;
    let start = Instant::now();
    for _ in 0..sequential_count {
        let (status, _) = request(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": SIMPLE_WORKFLOW,
                "event": "push",
                "repository": "bench/repo"
            }),
        )
        .await;
        assert!(status.is_success());
    }
    let sequential_elapsed = start.elapsed();
    let sequential_rps = sequential_count as f64 / sequential_elapsed.as_secs_f64();
    let sequential_latency_ms = sequential_elapsed.as_secs_f64() * 1000.0 / sequential_count as f64;
    eprintln!(
        "[loadtest]   sequential: {sequential_count} reqs in {:.1}ms = {sequential_rps:.0} rps, {sequential_latency_ms:.2}ms/req",
        sequential_elapsed.as_secs_f64() * 1000.0
    );

    // ── Phase 2: Concurrent submissions (the main metric) ───────────────
    // Sweep concurrency levels: 4, 16, 64, 128
    let mut best_rps = 0.0f64;
    let mut best_concurrency = 0usize;

    for concurrency in [4, 16, 64, 128] {
        let requests_per_worker = 50;
        let total_requests = concurrency * requests_per_worker;
        let success_count = Arc::new(AtomicU64::new(0));
        let error_count = Arc::new(AtomicU64::new(0));
        let total_latency_us = Arc::new(AtomicU64::new(0));

        let start = Instant::now();
        let mut handles = Vec::new();

        for _ in 0..concurrency {
            let app = app.clone();
            let success = success_count.clone();
            let errors = error_count.clone();
            let latency = total_latency_us.clone();

            handles.push(tokio::spawn(async move {
                for _ in 0..requests_per_worker {
                    let req_start = Instant::now();
                    let (status, _) = request(
                        &app,
                        Method::POST,
                        "/api/v1/runs",
                        json!({
                            "workflow_yaml": SIMPLE_WORKFLOW,
                            "event": "push",
                            "repository": "bench/repo"
                        }),
                    )
                    .await;
                    let elapsed = req_start.elapsed();
                    latency.fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
                    if status.is_success() {
                        success.fetch_add(1, Ordering::Relaxed);
                    } else {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for handle in handles {
            handle.await?;
        }

        let elapsed = start.elapsed();
        let successes = success_count.load(Ordering::Relaxed);
        let errs = error_count.load(Ordering::Relaxed);
        let rps = total_requests as f64 / elapsed.as_secs_f64();
        let avg_latency_ms =
            total_latency_us.load(Ordering::Relaxed) as f64 / 1000.0 / total_requests as f64;

        eprintln!(
            "[loadtest]   c={concurrency}: {total_requests} reqs in {:.1}ms = {rps:.0} rps, avg {avg_latency_ms:.2}ms, ok={successes} err={errs}",
            elapsed.as_secs_f64() * 1000.0
        );

        if rps > best_rps {
            best_rps = rps;
            best_concurrency = concurrency;
        }

        metric(&format!("server_rps_c{concurrency}"), rps, 0);
        metric(&format!("server_avg_ms_c{concurrency}"), avg_latency_ms, 2);
        metric(&format!("server_errors_c{concurrency}"), errs as f64, 0);
    }

    // ── Phase 3: Matrix workflow concurrent submissions ─────────────────
    let matrix_concurrency = 16;
    let matrix_requests = 50;
    let total_matrix = matrix_concurrency * matrix_requests;
    let start = Instant::now();
    let mut handles = Vec::new();
    let success_count = Arc::new(AtomicU64::new(0));

    for _ in 0..matrix_concurrency {
        let app = app.clone();
        let success = success_count.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..matrix_requests {
                let (status, _) = request(
                    &app,
                    Method::POST,
                    "/api/v1/runs",
                    json!({
                        "workflow_yaml": MATRIX_WORKFLOW,
                        "event": "push",
                        "repository": "bench/matrix-repo"
                    }),
                )
                .await;
                if status.is_success() {
                    success.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for handle in handles {
        handle.await?;
    }
    let matrix_elapsed = start.elapsed();
    let matrix_rps = total_matrix as f64 / matrix_elapsed.as_secs_f64();
    eprintln!(
        "[loadtest]   matrix c={matrix_concurrency}: {total_matrix} reqs in {:.1}ms = {matrix_rps:.0} rps",
        matrix_elapsed.as_secs_f64() * 1000.0
    );

    // ── Phase 4: Complex DAG workflow submissions ───────────────────────
    let complex_count = 100;
    let start = Instant::now();
    for _ in 0..complex_count {
        let (status, _) = request(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": COMPLEX_WORKFLOW,
                "event": "push",
                "repository": "bench/complex-repo",
                "ref": "refs/heads/main",
                "sha": "abc123"
            }),
        )
        .await;
        assert!(status.is_success());
    }
    let complex_elapsed = start.elapsed();
    let complex_rps = complex_count as f64 / complex_elapsed.as_secs_f64();
    eprintln!(
        "[loadtest]   complex-dag: {complex_count} reqs in {:.1}ms = {complex_rps:.0} rps",
        complex_elapsed.as_secs_f64() * 1000.0
    );

    // ── Phase 5: GET /api/v1/runs polling under load ────────────────────
    let poll_count = 500;
    let start = Instant::now();
    for _ in 0..poll_count {
        let (status, _) = request(&app, Method::GET, "/api/v1/runs?limit=50", Value::Null).await;
        assert!(status.is_success());
    }
    let poll_elapsed = start.elapsed();
    let poll_rps = poll_count as f64 / poll_elapsed.as_secs_f64();
    eprintln!(
        "[loadtest]   polling: {poll_count} GETs in {:.1}ms = {poll_rps:.0} rps",
        poll_elapsed.as_secs_f64() * 1000.0
    );

    // Emit primary and secondary metrics
    metric("server_rps", best_rps, 0);
    metric("server_best_concurrency", best_concurrency as f64, 0);
    metric("server_sequential_rps", sequential_rps, 0);
    metric("server_sequential_latency_ms", sequential_latency_ms, 2);
    metric("server_matrix_rps", matrix_rps, 0);
    metric("server_complex_dag_rps", complex_rps, 0);
    metric("server_poll_rps", poll_rps, 0);

    Ok(())
}

// ── parser benchmark ────────────────────────────────────────────────────────

fn bench_parser() -> Result<()> {
    eprintln!("[loadtest] === Parser Benchmark ===");

    let iterations = 1000;

    // Simple workflow parse
    let start = Instant::now();
    for _ in 0..iterations {
        let wf =
            aksh_gha_parser::parse_workflow(SIMPLE_WORKFLOW).expect("simple workflow must parse");
        std::hint::black_box(&wf);
    }
    let simple_us = start.elapsed().as_micros() as f64 / iterations as f64;

    // Matrix workflow parse + expand
    let start = Instant::now();
    for _ in 0..iterations {
        let wf =
            aksh_gha_parser::parse_workflow(MATRIX_WORKFLOW).expect("matrix workflow must parse");
        let expanded = aksh_gha_parser::expand_jobs(&wf);
        std::hint::black_box(&expanded);
    }
    let matrix_us = start.elapsed().as_micros() as f64 / iterations as f64;

    // Complex DAG workflow parse + expand
    let start = Instant::now();
    for _ in 0..iterations {
        let wf =
            aksh_gha_parser::parse_workflow(COMPLEX_WORKFLOW).expect("complex workflow must parse");
        let expanded = aksh_gha_parser::expand_jobs(&wf);
        std::hint::black_box(&expanded);
    }
    let complex_us = start.elapsed().as_micros() as f64 / iterations as f64;

    eprintln!("[loadtest]   simple parse: {simple_us:.1} µs/iter");
    eprintln!("[loadtest]   matrix parse+expand: {matrix_us:.1} µs/iter");
    eprintln!("[loadtest]   complex parse+expand: {complex_us:.1} µs/iter");

    metric("parse_simple_us", simple_us, 1);
    metric("parse_matrix_us", matrix_us, 1);
    metric("parse_complex_us", complex_us, 1);

    Ok(())
}

// ── expression evaluator benchmark ──────────────────────────────────────────

fn bench_expressions() -> Result<()> {
    eprintln!("[loadtest] === Expression Evaluator Benchmark ===");

    let mut ctx = aksh_gha_expressions::Context::new();
    ctx.insert(
        "github",
        json!({
            "event_name": "push",
            "ref": "refs/heads/main",
            "repository": "owner/repo",
            "run_id": "12345",
            "run_number": "42",
            "event": {
                "commits": [
                    {"message": "fix: bug"},
                    {"message": "feat: new feature"}
                ],
                "pull_request": {"draft": false}
            }
        }),
    );
    ctx.insert("matrix", json!({"os": "ubuntu-latest", "node": "20"}));
    ctx.insert(
        "needs",
        json!({"build": {"result": "success", "outputs": {}}}),
    );
    ctx.insert(
        "steps",
        json!({"test": {"outcome": "success", "outputs": {}}}),
    );
    ctx.insert("runner", json!({"os": "Linux", "arch": "X64"}));

    let iterations = 5000;

    // Evaluate each expression
    let start = Instant::now();
    for _ in 0..iterations {
        for expr in EXPRESSION_BATTERY {
            // hashFiles needs working directory context — skip if it errors
            let _ = aksh_gha_expressions::eval_expression(expr, &ctx);
        }
    }
    let total_evals = iterations * EXPRESSION_BATTERY.len();
    let elapsed = start.elapsed();
    let per_eval_us = elapsed.as_micros() as f64 / total_evals as f64;
    let evals_per_sec = total_evals as f64 / elapsed.as_secs_f64();

    eprintln!(
        "[loadtest]   {total_evals} evals in {:.1}ms = {per_eval_us:.2} µs/eval = {evals_per_sec:.0} evals/s",
        elapsed.as_secs_f64() * 1000.0
    );

    // Validate expressions (parse-only, no eval)
    let start = Instant::now();
    for _ in 0..iterations {
        for expr in EXPRESSION_BATTERY {
            let _ = aksh_gha_expressions::validate_expression(expr);
        }
    }
    let validate_elapsed = start.elapsed();
    let validate_us = validate_elapsed.as_micros() as f64 / total_evals as f64;

    eprintln!("[loadtest]   validate: {validate_us:.2} µs/expr");

    metric("expr_eval_us", per_eval_us, 2);
    metric("expr_evals_per_sec", evals_per_sec, 0);
    metric("expr_validate_us", validate_us, 2);

    Ok(())
}

// ── snapshot benchmark ──────────────────────────────────────────────────────

async fn bench_snapshots(_state_dir: &Path) -> Result<()> {
    eprintln!("[loadtest] === Snapshot Benchmark ===");

    // Create test workspaces of varying sizes
    for (label, file_count, file_size_bytes) in [
        ("small", 100, 256),
        ("medium", 1_000, 1024),
        ("large", 5_000, 2048),
        ("xlarge", 10_000, 1024),
    ] {
        let workspace = tempfile::tempdir()?;
        let ws_path = workspace.path();

        // Initialize git repo
        let status = std::process::Command::new("git")
            .args(["init", "--quiet"])
            .current_dir(ws_path)
            .status()?;
        assert!(status.success(), "git init failed");

        // Configure git user
        for args in [
            &["config", "user.email", "bench@preloop.dev"][..],
            &["config", "user.name", "Benchmark"],
        ] {
            std::process::Command::new("git")
                .args(args)
                .current_dir(ws_path)
                .status()?;
        }

        // Generate deterministic files
        let content: Vec<u8> = (0..file_size_bytes).map(|i| (i % 256) as u8).collect();
        let dirs = (file_count as f64).sqrt() as usize;
        for i in 0..file_count {
            let dir_idx = i % dirs.max(1);
            let dir = ws_path.join(format!("dir-{dir_idx:04}"));
            std::fs::create_dir_all(&dir)?;
            std::fs::write(dir.join(format!("file-{i:06}.txt")), &content)?;
        }

        // Initial commit
        std::process::Command::new("git")
            .args(["add", "--all"])
            .current_dir(ws_path)
            .env("GIT_AUTHOR_DATE", "2024-01-01T00:00:00Z")
            .env("GIT_COMMITTER_DATE", "2024-01-01T00:00:00Z")
            .status()?;
        std::process::Command::new("git")
            .args(["commit", "-m", "initial", "--quiet"])
            .current_dir(ws_path)
            .env("GIT_AUTHOR_DATE", "2024-01-01T00:00:00Z")
            .env("GIT_COMMITTER_DATE", "2024-01-01T00:00:00Z")
            .status()?;

        // Measure snapshot creation (cold — no cache)
        let snap_state = tempfile::tempdir()?;
        let state = aksh_runner_server::AppState::new(snap_state.path().to_path_buf())
            .await
            .context("creating AppState for snapshot bench")?;
        // Set local_workspace on the state so submit_run triggers snapshot
        let mut state_with_ws = state.clone();
        state_with_ws.local_workspace = Some(ws_path.to_path_buf());
        let shutdown = CancellationToken::new();
        let app = aksh_runner_server::app_with_test_api(state_with_ws, shutdown, "test-token");

        let iterations = 3;
        let mut times_ms = Vec::new();

        for _i in 0..iterations {
            let start = Instant::now();
            let (status, _) = request(
                &app,
                Method::POST,
                "/api/v1/runs",
                json!({
                    "workflow_yaml": SIMPLE_WORKFLOW,
                    "event": "push",
                    "repository": format!("bench/snap-{label}")
                }),
            )
            .await;
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            times_ms.push(elapsed_ms);
            eprintln!(
                "[loadtest]   {label} ({file_count} files, {file_size_bytes}B each) iter {}: {elapsed_ms:.1}ms status={status}",
                _i + 1
            );
        }

        let cold_ms = times_ms[0];
        let warm_ms = if times_ms.len() > 1 {
            times_ms[1..].iter().sum::<f64>() / (times_ms.len() - 1) as f64
        } else {
            cold_ms
        };

        metric(&format!("snapshot_{label}_cold_ms"), cold_ms, 1);
        metric(&format!("snapshot_{label}_warm_ms"), warm_ms, 1);
    }

    Ok(())
}

// ── cold boot benchmark ────────────────────────────────────────────────────

async fn bench_cold_boot(_state_dir: &Path) -> Result<()> {
    eprintln!("[loadtest] === Cold Boot Benchmark ===");

    let iterations = 5;
    let mut times_ms = Vec::new();

    for _ in 0..iterations {
        let temp = tempfile::tempdir()?;
        let start = Instant::now();
        let state = aksh_runner_server::AppState::new(temp.path().to_path_buf()).await?;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        times_ms.push(elapsed_ms);
        std::hint::black_box(&state);
    }

    times_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = times_ms[times_ms.len() / 2];
    let min = times_ms[0];
    let max = times_ms[times_ms.len() - 1];

    eprintln!("[loadtest]   AppState::new: min={min:.1}ms median={median:.1}ms max={max:.1}ms");

    metric("cold_boot_median_ms", median, 1);
    metric("cold_boot_min_ms", min, 1);

    Ok(())
}

// ── mutex contention benchmark ──────────────────────────────────────────────

async fn bench_contention(state_dir: &Path) -> Result<()> {
    eprintln!("[loadtest] === Mutex Contention Benchmark ===");

    let (app, _state) = make_app(state_dir).await?;
    let app = Arc::new(app);

    // Simulate a realistic mixed workload: submissions + polls + run lookups
    // all hitting the same Arc<Mutex<InnerState>> concurrently.
    let duration = Duration::from_secs(5);
    let concurrency = 32;

    let total_ops = Arc::new(AtomicU64::new(0));
    let submit_ops = Arc::new(AtomicU64::new(0));
    let poll_ops = Arc::new(AtomicU64::new(0));
    let error_ops = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut handles = Vec::new();

    for worker_id in 0..concurrency {
        let app = app.clone();
        let total = total_ops.clone();
        let submits = submit_ops.clone();
        let polls = poll_ops.clone();
        let errors = error_ops.clone();
        let deadline = start + duration;

        handles.push(tokio::spawn(async move {
            let mut i = 0u64;
            while Instant::now() < deadline {
                i += 1;
                // Alternate between submissions and polls with 70/30 split
                if i % 10 < 7 {
                    let (status, _) = request(
                        &app,
                        Method::POST,
                        "/api/v1/runs",
                        json!({
                            "workflow_yaml": SIMPLE_WORKFLOW,
                            "event": "push",
                            "repository": format!("bench/contention-{worker_id}")
                        }),
                    )
                    .await;
                    if status.is_success() {
                        submits.fetch_add(1, Ordering::Relaxed);
                    } else {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    let (status, _) =
                        request(&app, Method::GET, "/api/v1/runs?limit=20", Value::Null).await;
                    if status.is_success() {
                        polls.fetch_add(1, Ordering::Relaxed);
                    } else {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                total.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for handle in handles {
        handle.await?;
    }

    let elapsed = start.elapsed();
    let total = total_ops.load(Ordering::Relaxed);
    let submits = submit_ops.load(Ordering::Relaxed);
    let polls = poll_ops.load(Ordering::Relaxed);
    let errs = error_ops.load(Ordering::Relaxed);
    let mixed_rps = total as f64 / elapsed.as_secs_f64();

    eprintln!(
        "[loadtest]   mixed workload c={concurrency} for {:.1}s: {total} ops = {mixed_rps:.0} ops/s (submits={submits}, polls={polls}, errors={errs})",
        elapsed.as_secs_f64()
    );

    metric("contention_mixed_rps", mixed_rps, 0);
    metric("contention_submits", submits as f64, 0);
    metric("contention_polls", polls as f64, 0);
    metric("contention_errors", errs as f64, 0);

    Ok(())
}

// ── protocol serialization benchmark ────────────────────────────────────────

fn bench_protocol_serde() -> Result<()> {
    eprintln!("[loadtest] === Protocol Serialization Benchmark ===");

    // Build a representative AgentJobRequestMessage via the parser pipeline
    let wf = aksh_gha_parser::parse_workflow(COMPLEX_WORKFLOW)?;
    let expanded = aksh_gha_parser::expand_jobs(&wf)?;

    // Serialize/deserialize the expanded jobs
    let iterations = 5000;
    let payload = serde_json::to_string(&expanded)?;
    let payload_size = payload.len();

    let start = Instant::now();
    for _ in 0..iterations {
        let bytes = serde_json::to_string(&expanded).unwrap();
        std::hint::black_box(&bytes);
    }
    let ser_us = start.elapsed().as_micros() as f64 / iterations as f64;

    let start = Instant::now();
    for _ in 0..iterations {
        let _: Value = serde_json::from_str(&payload).unwrap();
    }
    let de_us = start.elapsed().as_micros() as f64 / iterations as f64;

    eprintln!(
        "[loadtest]   expanded jobs ({payload_size} bytes): ser={ser_us:.1}µs de={de_us:.1}µs"
    );

    metric("serde_expanded_ser_us", ser_us, 1);
    metric("serde_expanded_de_us", de_us, 1);
    metric("serde_payload_bytes", payload_size as f64, 0);

    Ok(())
}

// ── entrypoint ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let subcommand = args.get(1).map(String::as_str).unwrap_or("all");

    let state_dir = tempfile::tempdir().context("creating temp state dir")?;

    match subcommand {
        "server-load" => {
            bench_server_load(state_dir.path()).await?;
        }
        "parser" => {
            bench_parser()?;
        }
        "expressions" => {
            bench_expressions()?;
        }
        "snapshot" => {
            bench_snapshots(state_dir.path()).await?;
        }
        "cold-boot" => {
            bench_cold_boot(state_dir.path()).await?;
        }
        "contention" => {
            bench_contention(state_dir.path()).await?;
        }
        "serde" => {
            bench_protocol_serde()?;
        }
        "all" => {
            // Run everything, emit all metrics
            bench_cold_boot(state_dir.path()).await?;
            bench_parser()?;
            bench_expressions()?;
            bench_protocol_serde()?;
            bench_server_load(state_dir.path()).await?;
            bench_contention(state_dir.path()).await?;
            bench_snapshots(state_dir.path()).await?;
        }
        other => {
            eprintln!("Unknown subcommand: {other}");
            eprintln!("Usage: preloop-loadtest [all|server-load|parser|expressions|snapshot|cold-boot|contention|serde]");
            std::process::exit(1);
        }
    }

    Ok(())
}
