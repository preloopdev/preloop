//! Preloop comprehensive performance benchmark.
//!
//! Exercises the server control plane, parser, expression evaluator, and
//! workspace snapshotting under load. Emits `METRIC name=value` lines to
//! stdout for the autoresearch harness.
//!
//! ## Methodology
//!
//! Every measured quantity is sampled over independent *trials*. Each trial
//! builds its own `AppState` on a fresh temporary state directory, so a trial
//! never observes state accumulated by an earlier trial or by an earlier phase.
//! For each quantity the benchmark reports the median plus the observed
//! min/max/relative spread, so a reader can tell a real regression from
//! workstation noise instead of trusting a single sample.
//!
//! The concurrency sweep order is permuted per trial from a fixed seed and
//! mirrored on odd trials, so each concurrency level occupies early and late
//! sweep positions equally often across a trial pair. That cancels the
//! machine-warm-up drift the old ascending-only sweep silently folded into the
//! highest concurrency level.
//!
//! Environment knobs (all optional):
//!   PRELOOP_BENCH_TRIALS  — trials per quantity (default 3, must be >= 1)
//!   PRELOOP_BENCH_SEED    — permutation seed for the concurrency sweep
//!
//! Subcommands:
//!   server-load   — concurrent workflow submissions + polling through the
//!                   in-process axum router (zero network overhead).
//!   parser        — parse + expand a matrix workflow N times.
//!   expressions   — evaluate a battery of expressions N times.
//!   snapshot      — create workspace snapshots for varying project sizes.
//!   cold-boot     — repeated `AppState::new` on fresh state directories.
//!   contention    — sustained mixed read/write workload.
//!   serde         — protocol serialization round trip.
//!   all           — run every benchmark and emit all metrics.

use anyhow::{bail, Context, Result};
use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use serde_json::{json, Value};
use std::collections::BTreeMap;
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

/// Battery entries allowed to fail the evaluation preflight: they need
/// filesystem context the benchmark deliberately does not set up.
const EXPRESSION_MAY_FAIL: &[&str] = &["hashFiles('**/*.rs')"];

// ── measurement configuration ───────────────────────────────────────────────

const DEFAULT_TRIALS: usize = 3;
const DEFAULT_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

const CONCURRENCY_LEVELS: [usize; 4] = [4, 16, 64, 128];
const REQUESTS_PER_WORKER: usize = 50;
const SEQUENTIAL_REQUESTS: usize = 100;
const MATRIX_CONCURRENCY: usize = 16;
const MATRIX_REQUESTS_PER_WORKER: usize = 50;
const COMPLEX_REQUESTS: usize = 100;
/// Runs submitted before the polling phase, so `GET /api/v1/runs` always reads a
/// list of a known, fixed size instead of whatever earlier phases left behind.
const POLL_SEED_RUNS: usize = 200;
const POLL_REQUESTS: usize = 500;
const CONTENTION_CONCURRENCY: usize = 32;
const CONTENTION_DURATION: Duration = Duration::from_secs(5);
const PARSER_ITERATIONS: usize = 1000;
const EXPRESSION_ITERATIONS: usize = 5000;
const SERDE_ITERATIONS: usize = 5000;
const COLD_BOOT_ITERATIONS: usize = 5;
/// Warm snapshot submissions measured per trial, after the cold one.
const SNAPSHOT_WARM_REPEATS: usize = 2;
const SNAPSHOT_SIZES: [(&str, usize, usize); 4] = [
    ("small", 100, 256),
    ("medium", 1_000, 1024),
    ("large", 5_000, 2048),
    ("xlarge", 10_000, 1024),
];

/// Resolved measurement configuration, read once at startup so every benchmark
/// in an `all` run uses identical settings.
#[derive(Clone, Copy, Debug)]
struct BenchConfig {
    trials: usize,
    seed: u64,
}

impl BenchConfig {
    fn from_env() -> Result<Self> {
        let trials = env_u64("PRELOOP_BENCH_TRIALS", DEFAULT_TRIALS as u64)? as usize;
        if trials == 0 {
            bail!("PRELOOP_BENCH_TRIALS must be >= 1");
        }
        let seed = env_u64("PRELOOP_BENCH_SEED", DEFAULT_SEED)?;
        Ok(Self { trials, seed })
    }
}

fn env_u64(key: &str, default: u64) -> Result<u64> {
    match std::env::var(key) {
        Ok(raw) => raw
            .trim()
            .parse()
            .with_context(|| format!("{key} must be an unsigned integer, got {raw:?}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(err).with_context(|| format!("reading {key}")),
    }
}

// ── helpers ─────────────────────────────────────────────────────────────────

async fn make_app(state_dir: &Path) -> Result<(Router, aksh_runner_server::AppState)> {
    let state = aksh_runner_server::AppState::new(state_dir.to_path_buf())
        .await
        .context("creating AppState")?;
    let shutdown = CancellationToken::new();
    let app = aksh_runner_server::app_with_test_api(state.clone(), shutdown, "test-token");
    Ok((app, state))
}

/// Fresh router on a fresh state directory. The returned `TempDir` must outlive
/// the router: dropping it deletes the state the server is reading.
async fn fresh_app() -> Result<(Router, aksh_runner_server::AppState, tempfile::TempDir)> {
    let dir = tempfile::tempdir().context("creating temp state dir")?;
    let (app, state) = make_app(dir.path()).await?;
    Ok((app, state, dir))
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

fn submission(workflow: &str, repository: &str) -> Value {
    json!({
        "workflow_yaml": workflow,
        "event": "push",
        "repository": repository,
    })
}

/// Submit one run and fail closed on any non-success status. Sequential phases
/// use this so a server that starts rejecting submissions can never be reported
/// as "fast".
async fn submit_run(app: &Router, body: Value) -> Result<()> {
    let (status, response) = request(app, Method::POST, "/api/v1/runs", body).await;
    if !status.is_success() {
        bail!("POST /api/v1/runs returned {status}: {response}");
    }
    Ok(())
}

async fn submit_sequential(
    app: &Router,
    workflow: &str,
    repository: &str,
    count: usize,
) -> Result<Duration> {
    let start = Instant::now();
    for _ in 0..count {
        submit_run(app, submission(workflow, repository)).await?;
    }
    Ok(start.elapsed())
}

fn metric(name: &str, value: f64, digits: u32) {
    let factor = 10f64.powi(digits as i32);
    println!("METRIC {}={}", name, (value * factor).round() / factor);
}

/// Emit a metric whose value is not a rounded float (counts, seeds, labels).
fn metric_text(name: &str, value: &str) {
    println!("METRIC {name}={value}");
}

fn sorted(samples: &[f64]) -> Vec<f64> {
    let mut out = samples.to_vec();
    out.sort_by(f64::total_cmp);
    out
}

fn median_of_sorted(sorted: &[f64]) -> f64 {
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        sorted[mid]
    } else {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    }
}

/// Emit repeated-measurement statistics for `name`.
///
/// `name` keeps the plain metric key and carries the **median**, so existing
/// consumers stay valid; `name_min` / `name_max` / `name_spread_pct` describe
/// run-to-run stability and `name_samples` records how many samples backed it.
fn metric_stats(name: &str, samples: &[f64], digits: u32) {
    assert!(
        !samples.is_empty(),
        "metric {name} was emitted without samples"
    );
    let ordered = sorted(samples);
    let median = median_of_sorted(&ordered);
    let min = ordered[0];
    let max = ordered[ordered.len() - 1];
    metric(name, median, digits);
    metric(&format!("{name}_min"), min, digits);
    metric(&format!("{name}_max"), max, digits);
    let spread_pct = if median > 0.0 {
        (max - min) / median * 100.0
    } else {
        0.0
    };
    metric(&format!("{name}_spread_pct"), spread_pct, 1);
    metric_text(&format!("{name}_samples"), &ordered.len().to_string());
}

/// Deterministic xorshift64* — enough for reproducible small permutations
/// without pulling `rand` into the benchmark's dependency set.
fn next_rand(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

/// Concurrency sweep order for `trial`.
///
/// Trials `2k` and `2k+1` share one seeded permutation, with the odd trial
/// running it in reverse. Across a trial pair every level therefore occupies
/// mirrored sweep positions, so residual machine warm-up (page cache, CPU
/// boost, thermals) cannot accumulate onto whichever level ran last.
fn sweep_order(seed: u64, trial: usize) -> Vec<usize> {
    let mut levels = CONCURRENCY_LEVELS.to_vec();
    let mut state = seed ^ (trial as u64 / 2).wrapping_mul(DEFAULT_SEED);
    if state == 0 {
        state = DEFAULT_SEED;
    }
    for i in (1..levels.len()).rev() {
        let j = (next_rand(&mut state) % (i as u64 + 1)) as usize;
        levels.swap(i, j);
    }
    if trial % 2 == 1 {
        levels.reverse();
    }
    levels
}

// ── server load benchmark ───────────────────────────────────────────────────

/// One concurrency-level measurement.
struct SweepSample {
    rps: f64,
    avg_latency_ms: f64,
    errors: u64,
}

async fn measure_concurrency_level(app: Arc<Router>, concurrency: usize) -> Result<SweepSample> {
    let total_requests = concurrency * REQUESTS_PER_WORKER;
    let success_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    let total_latency_us = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let app = app.clone();
        let success = success_count.clone();
        let errors = error_count.clone();
        let latency = total_latency_us.clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..REQUESTS_PER_WORKER {
                let req_start = Instant::now();
                let (status, _) = request(
                    &app,
                    Method::POST,
                    "/api/v1/runs",
                    submission(SIMPLE_WORKFLOW, "bench/repo"),
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
    let errors = error_count.load(Ordering::Relaxed);
    if successes + errors != total_requests as u64 {
        bail!(
            "c={concurrency}: accounted {} of {total_requests} requests",
            successes + errors
        );
    }

    Ok(SweepSample {
        rps: total_requests as f64 / elapsed.as_secs_f64(),
        avg_latency_ms: total_latency_us.load(Ordering::Relaxed) as f64
            / 1000.0
            / total_requests as f64,
        errors,
    })
}

async fn bench_server_load(cfg: BenchConfig) -> Result<()> {
    eprintln!(
        "[loadtest] === Server Load Benchmark ({} trial(s)) ===",
        cfg.trials
    );

    let mut sequential_rps = Vec::new();
    let mut sequential_latency_ms = Vec::new();
    let mut level_rps: BTreeMap<usize, Vec<f64>> = BTreeMap::new();
    let mut level_latency_ms: BTreeMap<usize, Vec<f64>> = BTreeMap::new();
    let mut level_errors: BTreeMap<usize, u64> = BTreeMap::new();
    let mut best_rps = Vec::new();
    let mut best_concurrency_wins: BTreeMap<usize, usize> = BTreeMap::new();
    let mut matrix_rps = Vec::new();
    let mut complex_rps = Vec::new();
    let mut poll_rps = Vec::new();

    for trial in 0..cfg.trials {
        let order = sweep_order(cfg.seed, trial);
        eprintln!(
            "[loadtest]  -- trial {}/{}, sweep order {order:?}",
            trial + 1,
            cfg.trials
        );

        // ── Phase 1: Sequential baseline ────────────────────────────────
        // Fresh state, plus a small warmup that primes lazily-built internals
        // without being counted.
        {
            let (app, _state, _dir) = fresh_app().await?;
            for _ in 0..3 {
                submit_run(&app, submission(SIMPLE_WORKFLOW, "bench/warmup")).await?;
            }
            let elapsed =
                submit_sequential(&app, SIMPLE_WORKFLOW, "bench/repo", SEQUENTIAL_REQUESTS).await?;
            let rps = SEQUENTIAL_REQUESTS as f64 / elapsed.as_secs_f64();
            let latency_ms = elapsed.as_secs_f64() * 1000.0 / SEQUENTIAL_REQUESTS as f64;
            eprintln!(
                "[loadtest]   sequential: {SEQUENTIAL_REQUESTS} reqs in {:.1}ms = {rps:.0} rps, {latency_ms:.2}ms/req",
                elapsed.as_secs_f64() * 1000.0
            );
            sequential_rps.push(rps);
            sequential_latency_ms.push(latency_ms);
        }

        // ── Phase 2: Concurrent submissions (the primary metric) ────────
        // Each level gets its own server, so sweep position cannot leak run
        // list growth into the next level's numbers.
        let mut trial_best = (0.0f64, 0usize);
        for &concurrency in &order {
            let (app, _state, _dir) = fresh_app().await?;
            let app = Arc::new(app);
            for _ in 0..3 {
                submit_run(&app, submission(SIMPLE_WORKFLOW, "bench/warmup")).await?;
            }

            let sample = measure_concurrency_level(app, concurrency).await?;
            eprintln!(
                "[loadtest]   c={concurrency}: {} reqs = {:.0} rps, avg {:.2}ms, err={}",
                concurrency * REQUESTS_PER_WORKER,
                sample.rps,
                sample.avg_latency_ms,
                sample.errors
            );

            level_rps.entry(concurrency).or_default().push(sample.rps);
            level_latency_ms
                .entry(concurrency)
                .or_default()
                .push(sample.avg_latency_ms);
            *level_errors.entry(concurrency).or_default() += sample.errors;

            if sample.rps > trial_best.0 {
                trial_best = (sample.rps, concurrency);
            }
        }
        best_rps.push(trial_best.0);
        *best_concurrency_wins.entry(trial_best.1).or_default() += 1;

        // ── Phase 3: Matrix workflow concurrent submissions ─────────────
        {
            let (app, _state, _dir) = fresh_app().await?;
            let app = Arc::new(app);
            let total = MATRIX_CONCURRENCY * MATRIX_REQUESTS_PER_WORKER;
            let errors = Arc::new(AtomicU64::new(0));
            let start = Instant::now();
            let mut handles = Vec::with_capacity(MATRIX_CONCURRENCY);
            for _ in 0..MATRIX_CONCURRENCY {
                let app = app.clone();
                let errors = errors.clone();
                handles.push(tokio::spawn(async move {
                    for _ in 0..MATRIX_REQUESTS_PER_WORKER {
                        let (status, _) = request(
                            &app,
                            Method::POST,
                            "/api/v1/runs",
                            submission(MATRIX_WORKFLOW, "bench/matrix-repo"),
                        )
                        .await;
                        if !status.is_success() {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }));
            }
            for handle in handles {
                handle.await?;
            }
            let elapsed = start.elapsed();
            let errors = errors.load(Ordering::Relaxed);
            if errors > 0 {
                bail!("matrix phase: {errors} of {total} submissions were rejected");
            }
            let rps = total as f64 / elapsed.as_secs_f64();
            eprintln!(
                "[loadtest]   matrix c={MATRIX_CONCURRENCY}: {total} reqs in {:.1}ms = {rps:.0} rps",
                elapsed.as_secs_f64() * 1000.0
            );
            matrix_rps.push(rps);
        }

        // ── Phase 4: Complex DAG workflow submissions ───────────────────
        {
            let (app, _state, _dir) = fresh_app().await?;
            let start = Instant::now();
            for _ in 0..COMPLEX_REQUESTS {
                submit_run(
                    &app,
                    json!({
                        "workflow_yaml": COMPLEX_WORKFLOW,
                        "event": "push",
                        "repository": "bench/complex-repo",
                        "ref": "refs/heads/main",
                        "sha": "abc123"
                    }),
                )
                .await?;
            }
            let elapsed = start.elapsed();
            let rps = COMPLEX_REQUESTS as f64 / elapsed.as_secs_f64();
            eprintln!(
                "[loadtest]   complex-dag: {COMPLEX_REQUESTS} reqs in {:.1}ms = {rps:.0} rps",
                elapsed.as_secs_f64() * 1000.0
            );
            complex_rps.push(rps);
        }

        // ── Phase 5: GET /api/v1/runs polling ───────────────────────────
        // Seeded to a fixed list length so poll throughput is comparable across
        // trials and across harness runs.
        {
            let (app, _state, _dir) = fresh_app().await?;
            submit_sequential(&app, SIMPLE_WORKFLOW, "bench/poll-repo", POLL_SEED_RUNS).await?;
            let start = Instant::now();
            for _ in 0..POLL_REQUESTS {
                let (status, _) =
                    request(&app, Method::GET, "/api/v1/runs?limit=50", Value::Null).await;
                if !status.is_success() {
                    bail!("GET /api/v1/runs returned {status}");
                }
            }
            let elapsed = start.elapsed();
            let rps = POLL_REQUESTS as f64 / elapsed.as_secs_f64();
            eprintln!(
                "[loadtest]   polling ({POLL_SEED_RUNS} runs seeded): {POLL_REQUESTS} GETs in {:.1}ms = {rps:.0} rps",
                elapsed.as_secs_f64() * 1000.0
            );
            poll_rps.push(rps);
        }
    }

    for (&concurrency, samples) in &level_rps {
        metric_stats(&format!("server_rps_c{concurrency}"), samples, 0);
    }
    for (&concurrency, samples) in &level_latency_ms {
        metric_stats(&format!("server_avg_ms_c{concurrency}"), samples, 2);
    }
    for (&concurrency, errors) in &level_errors {
        metric_text(
            &format!("server_errors_c{concurrency}"),
            &errors.to_string(),
        );
    }

    metric_stats("server_rps", &best_rps, 0);
    metric_stats("server_sequential_rps", &sequential_rps, 0);
    metric_stats("server_sequential_latency_ms", &sequential_latency_ms, 2);
    metric_stats("server_matrix_rps", &matrix_rps, 0);
    metric_stats("server_complex_dag_rps", &complex_rps, 0);
    metric_stats("server_poll_rps", &poll_rps, 0);

    // The winning concurrency level can differ between trials; report the level
    // that won most often rather than pretending one level always wins.
    let (best_concurrency, wins) = best_concurrency_wins
        .iter()
        .max_by_key(|(_, wins)| **wins)
        .map(|(level, wins)| (*level, *wins))
        .context("no concurrency level was measured")?;
    metric_text("server_best_concurrency", &best_concurrency.to_string());
    metric_text("server_best_concurrency_wins", &wins.to_string());

    let total_errors: u64 = level_errors.values().sum();
    if total_errors > 0 {
        bail!("concurrency sweep recorded {total_errors} rejected submissions: {level_errors:?}");
    }

    Ok(())
}

// ── parser benchmark ────────────────────────────────────────────────────────

fn bench_parser(cfg: BenchConfig) -> Result<()> {
    eprintln!(
        "[loadtest] === Parser Benchmark ({} trial(s)) ===",
        cfg.trials
    );

    let mut simple_us = Vec::new();
    let mut matrix_us = Vec::new();
    let mut complex_us = Vec::new();

    for _ in 0..cfg.trials {
        let start = Instant::now();
        for _ in 0..PARSER_ITERATIONS {
            let wf = aksh_gha_parser::parse_workflow(SIMPLE_WORKFLOW)
                .context("simple workflow must parse")?;
            std::hint::black_box(&wf);
        }
        simple_us.push(start.elapsed().as_micros() as f64 / PARSER_ITERATIONS as f64);

        let start = Instant::now();
        for _ in 0..PARSER_ITERATIONS {
            let wf = aksh_gha_parser::parse_workflow(MATRIX_WORKFLOW)
                .context("matrix workflow must parse")?;
            let expanded = aksh_gha_parser::expand_jobs(&wf);
            std::hint::black_box(&expanded);
        }
        matrix_us.push(start.elapsed().as_micros() as f64 / PARSER_ITERATIONS as f64);

        let start = Instant::now();
        for _ in 0..PARSER_ITERATIONS {
            let wf = aksh_gha_parser::parse_workflow(COMPLEX_WORKFLOW)
                .context("complex workflow must parse")?;
            let expanded = aksh_gha_parser::expand_jobs(&wf);
            std::hint::black_box(&expanded);
        }
        complex_us.push(start.elapsed().as_micros() as f64 / PARSER_ITERATIONS as f64);
    }

    eprintln!(
        "[loadtest]   simple / matrix / complex medians: {:.1} / {:.1} / {:.1} µs/iter",
        median_of_sorted(&sorted(&simple_us)),
        median_of_sorted(&sorted(&matrix_us)),
        median_of_sorted(&sorted(&complex_us))
    );

    metric_stats("parse_simple_us", &simple_us, 1);
    metric_stats("parse_matrix_us", &matrix_us, 1);
    metric_stats("parse_complex_us", &complex_us, 1);

    Ok(())
}

// ── expression evaluator benchmark ──────────────────────────────────────────

fn expression_context() -> aksh_gha_expressions::Context {
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
    ctx
}

fn bench_expressions(cfg: BenchConfig) -> Result<()> {
    eprintln!(
        "[loadtest] === Expression Evaluator Benchmark ({} trial(s)) ===",
        cfg.trials
    );

    let ctx = expression_context();

    // Preflight: an evaluator that errored on every input would make the timing
    // loop measure the error path and look *fast*. Fail closed on anything
    // outside the documented filesystem-dependent exceptions.
    let mut evaluated = 0usize;
    for expr in EXPRESSION_BATTERY {
        match aksh_gha_expressions::eval_expression(expr, &ctx) {
            Ok(_) => evaluated += 1,
            Err(err) if EXPRESSION_MAY_FAIL.contains(expr) => {
                eprintln!("[loadtest]   skipped (needs filesystem context): {expr} — {err}");
            }
            Err(err) => bail!("expression battery entry failed to evaluate: {expr}: {err}"),
        }
    }
    metric_text("expr_evaluated_count", &evaluated.to_string());
    metric_text("expr_battery_size", &EXPRESSION_BATTERY.len().to_string());

    let total_evals = EXPRESSION_ITERATIONS * EXPRESSION_BATTERY.len();
    let mut per_eval_us = Vec::new();
    let mut evals_per_sec = Vec::new();
    let mut validate_us = Vec::new();

    for _ in 0..cfg.trials {
        let start = Instant::now();
        for _ in 0..EXPRESSION_ITERATIONS {
            for expr in EXPRESSION_BATTERY {
                std::hint::black_box(aksh_gha_expressions::eval_expression(expr, &ctx).is_ok());
            }
        }
        let elapsed = start.elapsed();
        per_eval_us.push(elapsed.as_micros() as f64 / total_evals as f64);
        evals_per_sec.push(total_evals as f64 / elapsed.as_secs_f64());

        let start = Instant::now();
        for _ in 0..EXPRESSION_ITERATIONS {
            for expr in EXPRESSION_BATTERY {
                std::hint::black_box(aksh_gha_expressions::validate_expression(expr).is_ok());
            }
        }
        validate_us.push(start.elapsed().as_micros() as f64 / total_evals as f64);
    }

    eprintln!(
        "[loadtest]   {total_evals} evals/trial: median {:.2} µs/eval, validate {:.2} µs/expr",
        median_of_sorted(&sorted(&per_eval_us)),
        median_of_sorted(&sorted(&validate_us))
    );

    metric_stats("expr_eval_us", &per_eval_us, 2);
    metric_stats("expr_evals_per_sec", &evals_per_sec, 0);
    metric_stats("expr_validate_us", &validate_us, 2);

    Ok(())
}

// ── snapshot benchmark ──────────────────────────────────────────────────────

fn run_git(args: &[&str], cwd: &Path) -> Result<()> {
    let status = std::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .env("GIT_AUTHOR_DATE", "2024-01-01T00:00:00Z")
        .env("GIT_COMMITTER_DATE", "2024-01-01T00:00:00Z")
        .status()
        .with_context(|| format!("spawning git {args:?}"))?;
    if !status.success() {
        bail!("git {args:?} failed with {status}");
    }
    Ok(())
}

/// Deterministic git workspace with `file_count` files of `file_size_bytes`.
fn build_workspace(file_count: usize, file_size_bytes: usize) -> Result<tempfile::TempDir> {
    let workspace = tempfile::tempdir().context("creating temp workspace")?;
    let ws_path = workspace.path();

    run_git(&["init", "--quiet"], ws_path)?;
    run_git(&["config", "user.email", "bench@preloop.dev"], ws_path)?;
    run_git(&["config", "user.name", "Benchmark"], ws_path)?;

    let content: Vec<u8> = (0..file_size_bytes).map(|i| (i % 256) as u8).collect();
    let dirs = (file_count as f64).sqrt() as usize;
    for i in 0..file_count {
        let dir_idx = i % dirs.max(1);
        let dir = ws_path.join(format!("dir-{dir_idx:04}"));
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join(format!("file-{i:06}.txt")), &content)?;
    }

    run_git(&["add", "--all"], ws_path)?;
    run_git(&["commit", "-m", "initial", "--quiet"], ws_path)?;
    Ok(workspace)
}

async fn bench_snapshots(cfg: BenchConfig) -> Result<()> {
    eprintln!(
        "[loadtest] === Snapshot Benchmark ({} trial(s)) ===",
        cfg.trials
    );

    for (label, file_count, file_size_bytes) in SNAPSHOT_SIZES {
        // The workspace is deterministic input, not the measured subject, so it
        // is built once. The *cache* is what must be reset: every trial gets a
        // brand new server state directory, so "cold" really is cold.
        let workspace = build_workspace(file_count, file_size_bytes)?;
        let ws_path = workspace.path().to_path_buf();
        let repository = format!("bench/snap-{label}");

        let mut cold_ms = Vec::new();
        let mut warm_ms = Vec::new();

        for trial in 0..cfg.trials {
            let snap_state = tempfile::tempdir().context("creating snapshot state dir")?;
            let (_, state) = make_app(snap_state.path()).await?;
            let mut state_with_ws = state.clone();
            state_with_ws.local_workspace = Some(ws_path.clone());
            let shutdown = CancellationToken::new();
            let app = aksh_runner_server::app_with_test_api(state_with_ws, shutdown, "test-token");

            for iteration in 0..(1 + SNAPSHOT_WARM_REPEATS) {
                let start = Instant::now();
                submit_run(&app, submission(SIMPLE_WORKFLOW, &repository)).await?;
                let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
                if iteration == 0 {
                    cold_ms.push(elapsed_ms);
                } else {
                    warm_ms.push(elapsed_ms);
                }
                eprintln!(
                    "[loadtest]   {label} ({file_count} files, {file_size_bytes}B) trial {} {}: {elapsed_ms:.1}ms",
                    trial + 1,
                    if iteration == 0 { "cold" } else { "warm" }
                );
            }
        }

        metric_stats(&format!("snapshot_{label}_cold_ms"), &cold_ms, 1);
        metric_stats(&format!("snapshot_{label}_warm_ms"), &warm_ms, 1);
        metric_text(&format!("snapshot_{label}_files"), &file_count.to_string());
    }

    Ok(())
}

// ── cold boot benchmark ────────────────────────────────────────────────────

async fn bench_cold_boot(cfg: BenchConfig) -> Result<()> {
    eprintln!(
        "[loadtest] === Cold Boot Benchmark ({} trial(s) x {COLD_BOOT_ITERATIONS}) ===",
        cfg.trials
    );

    let mut times_ms = Vec::new();
    for _ in 0..cfg.trials {
        for _ in 0..COLD_BOOT_ITERATIONS {
            let temp = tempfile::tempdir().context("creating temp state dir")?;
            let start = Instant::now();
            let state = aksh_runner_server::AppState::new(temp.path().to_path_buf())
                .await
                .context("creating AppState")?;
            times_ms.push(start.elapsed().as_secs_f64() * 1000.0);
            std::hint::black_box(&state);
        }
    }

    let ordered = sorted(&times_ms);
    eprintln!(
        "[loadtest]   AppState::new: min={:.1}ms median={:.1}ms max={:.1}ms over {} samples",
        ordered[0],
        median_of_sorted(&ordered),
        ordered[ordered.len() - 1],
        ordered.len()
    );

    metric_stats("cold_boot_ms", &times_ms, 1);
    // Historical key names kept so existing dashboards keep resolving.
    metric("cold_boot_median_ms", median_of_sorted(&ordered), 1);
    metric("cold_boot_min_ms", ordered[0], 1);

    Ok(())
}

// ── mutex contention benchmark ──────────────────────────────────────────────

async fn bench_contention(cfg: BenchConfig) -> Result<()> {
    eprintln!(
        "[loadtest] === Mutex Contention Benchmark ({} trial(s)) ===",
        cfg.trials
    );

    let mut mixed_rps = Vec::new();
    let mut submit_totals = Vec::new();
    let mut poll_totals = Vec::new();
    let mut error_total = 0u64;

    for _ in 0..cfg.trials {
        // Fresh server per trial: a run list carried over from a previous trial
        // would make later polls progressively more expensive.
        let (app, _state, _dir) = fresh_app().await?;
        let app = Arc::new(app);

        let total_ops = Arc::new(AtomicU64::new(0));
        let submit_ops = Arc::new(AtomicU64::new(0));
        let poll_ops = Arc::new(AtomicU64::new(0));
        let error_ops = Arc::new(AtomicU64::new(0));

        let start = Instant::now();
        let mut handles = Vec::with_capacity(CONTENTION_CONCURRENCY);

        for worker_id in 0..CONTENTION_CONCURRENCY {
            let app = app.clone();
            let total = total_ops.clone();
            let submits = submit_ops.clone();
            let polls = poll_ops.clone();
            let errors = error_ops.clone();
            let deadline = start + CONTENTION_DURATION;

            handles.push(tokio::spawn(async move {
                let mut i = 0u64;
                while Instant::now() < deadline {
                    i += 1;
                    // Alternate between submissions and polls with a 70/30 split.
                    if i % 10 < 7 {
                        let (status, _) = request(
                            &app,
                            Method::POST,
                            "/api/v1/runs",
                            submission(SIMPLE_WORKFLOW, &format!("bench/contention-{worker_id}")),
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
        let errors = error_ops.load(Ordering::Relaxed);
        let rps = total as f64 / elapsed.as_secs_f64();

        eprintln!(
            "[loadtest]   mixed c={CONTENTION_CONCURRENCY} for {:.1}s: {total} ops = {rps:.0} ops/s (submits={submits}, polls={polls}, errors={errors})",
            elapsed.as_secs_f64()
        );

        mixed_rps.push(rps);
        submit_totals.push(submits as f64);
        poll_totals.push(polls as f64);
        error_total += errors;
    }

    metric_stats("contention_mixed_rps", &mixed_rps, 0);
    metric_stats("contention_submits", &submit_totals, 0);
    metric_stats("contention_polls", &poll_totals, 0);
    metric_text("contention_errors", &error_total.to_string());

    if error_total > 0 {
        bail!("mixed contention workload recorded {error_total} rejected requests");
    }

    Ok(())
}

// ── protocol serialization benchmark ────────────────────────────────────────

fn bench_protocol_serde(cfg: BenchConfig) -> Result<()> {
    eprintln!(
        "[loadtest] === Protocol Serialization Benchmark ({} trial(s)) ===",
        cfg.trials
    );

    let wf = aksh_gha_parser::parse_workflow(COMPLEX_WORKFLOW)?;
    let expanded = aksh_gha_parser::expand_jobs(&wf)?;
    let payload = serde_json::to_string(&expanded)?;
    let payload_size = payload.len();

    let mut ser_us = Vec::new();
    let mut de_us = Vec::new();

    for _ in 0..cfg.trials {
        let start = Instant::now();
        for _ in 0..SERDE_ITERATIONS {
            let bytes = serde_json::to_string(&expanded)?;
            std::hint::black_box(&bytes);
        }
        ser_us.push(start.elapsed().as_micros() as f64 / SERDE_ITERATIONS as f64);

        let start = Instant::now();
        for _ in 0..SERDE_ITERATIONS {
            let value: Value = serde_json::from_str(&payload)?;
            std::hint::black_box(&value);
        }
        de_us.push(start.elapsed().as_micros() as f64 / SERDE_ITERATIONS as f64);
    }

    eprintln!(
        "[loadtest]   expanded jobs ({payload_size} bytes): ser={:.1}µs de={:.1}µs (medians)",
        median_of_sorted(&sorted(&ser_us)),
        median_of_sorted(&sorted(&de_us))
    );

    metric_stats("serde_expanded_ser_us", &ser_us, 1);
    metric_stats("serde_expanded_de_us", &de_us, 1);
    metric_text("serde_payload_bytes", &payload_size.to_string());

    Ok(())
}

// ── entrypoint ──────────────────────────────────────────────────────────────

/// Emit the knobs and build facts a reader needs to reproduce the numbers.
fn emit_run_provenance(cfg: BenchConfig) -> Result<()> {
    metric_text("bench_trials", &cfg.trials.to_string());
    metric_text("bench_seed", &cfg.seed.to_string());
    metric_text(
        "bench_profile",
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
    );
    let parallelism = std::thread::available_parallelism()
        .context("querying available parallelism")?
        .get();
    metric_text("bench_available_parallelism", &parallelism.to_string());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let subcommand = args.get(1).map(String::as_str).unwrap_or("all");
    let cfg = BenchConfig::from_env()?;

    emit_run_provenance(cfg)?;

    match subcommand {
        "server-load" => bench_server_load(cfg).await?,
        "parser" => bench_parser(cfg)?,
        "expressions" => bench_expressions(cfg)?,
        "snapshot" => bench_snapshots(cfg).await?,
        "cold-boot" => bench_cold_boot(cfg).await?,
        "contention" => bench_contention(cfg).await?,
        "serde" => bench_protocol_serde(cfg)?,
        "all" => {
            bench_cold_boot(cfg).await?;
            bench_parser(cfg)?;
            bench_expressions(cfg)?;
            bench_protocol_serde(cfg)?;
            bench_server_load(cfg).await?;
            bench_contention(cfg).await?;
            bench_snapshots(cfg).await?;
        }
        other => {
            eprintln!("Unknown subcommand: {other}");
            eprintln!("Usage: preloop-loadtest [all|server-load|parser|expressions|snapshot|cold-boot|contention|serde]");
            std::process::exit(1);
        }
    }

    Ok(())
}
