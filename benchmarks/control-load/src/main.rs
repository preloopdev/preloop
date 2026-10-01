//! Open-loop load and chaos harness for the Postgres-backed control plane.
//!
//! `run` drives real engine processes over HTTP: Poisson workflow arrivals
//! (REST submissions and signed GitHub push webhooks) with optional burst
//! multipliers, and a fleet of mock runners speaking the broker protocol.
//! After the submission window it keeps the runners going until the backlog
//! drains (or a drain timeout), then writes `summary.json` and
//! `timeline.csv`.
//!
//! `proxy` is a fault-injecting TCP proxy to put between engines and
//! Postgres. `prepare-workspace` writes the webhook repository.

mod chaos;
mod db;
mod metrics;
mod runner;
mod workload;

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::{Parser, Subcommand};
use rand::Rng;

use metrics::Metrics;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write the webhook workspace (a git repo) and print its HEAD sha.
    PrepareWorkspace {
        #[arg(long)]
        dir: std::path::PathBuf,
    },
    /// Fault-injecting TCP proxy.
    Proxy {
        #[arg(long)]
        listen: String,
        #[arg(long)]
        upstream: String,
        /// `kind@start+duration[:param]`: latency (ms), partition, reset.
        #[arg(long = "fault")]
        faults: Vec<chaos::Fault>,
    },
    /// One load round.
    Run(Box<RunArgs>),
}

#[derive(clap::Args, Clone)]
struct RunArgs {
    /// Engine base URLs (comma-separated); traffic is spread across them.
    #[arg(long, value_delimiter = ',')]
    servers: Vec<String>,
    #[arg(long, default_value = "preloop-system-token")]
    system_token: String,
    #[arg(long, default_value = "load-webhook-secret")]
    webhook_secret: String,
    /// HEAD sha of the webhook workspace (`prepare-workspace`).
    #[arg(long)]
    webhook_sha: Option<String>,
    /// Round label (output directory name).
    #[arg(long)]
    label: String,
    #[arg(long, default_value = "load-results")]
    out: std::path::PathBuf,
    /// Base workflow-run arrival rate (runs/second, Poisson).
    #[arg(long)]
    runs_per_sec: f64,
    /// Bursts: `start+duration:multiplier` (seconds), e.g. `60+30:10`.
    #[arg(long = "burst")]
    bursts: Vec<String>,
    /// Fraction of arrivals delivered as GitHub push webhooks.
    #[arg(long, default_value_t = 0.5)]
    webhook_fraction: f64,
    /// Distinct repositories (tenancy spread).
    #[arg(long, default_value_t = 200)]
    repos: u32,
    /// Submission window.
    #[arg(long)]
    duration_secs: u64,
    /// Extra time for the runner fleet to drain the backlog.
    #[arg(long, default_value_t = 300)]
    drain_secs: u64,
    /// Mock runners.
    #[arg(long)]
    runners: u32,
    #[arg(long, default_value_t = 4000)]
    job_median_ms: u64,
    #[arg(long, default_value_t = 0.8)]
    job_sigma: f64,
    #[arg(long, default_value_t = 6)]
    timeline_updates: u32,
    #[arg(long, default_value_t = 60)]
    renew_secs: u64,
    #[arg(long, default_value_t = 0.03)]
    failure_rate: f64,
    /// Postgres URL for stats sampling.
    #[arg(long)]
    pg_url: Option<String>,
    /// Max in-flight submissions (bounds harness memory, not the rate).
    #[arg(long, default_value_t = 4096)]
    max_in_flight: usize,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::PrepareWorkspace { dir } => {
            let sha = workload::prepare_workspace(&dir, &workload::Mix::new())?;
            println!("{sha}");
            Ok(())
        }
        Command::Proxy {
            listen,
            upstream,
            faults,
        } => {
            let metrics = Arc::new(Metrics::new());
            chaos::run_proxy(listen, upstream, faults, metrics, Instant::now()).await
        }
        Command::Run(args) => run(*args).await,
    }
}

/// Arrival-rate multiplier at `t` from the burst schedule.
fn multiplier(bursts: &[(u64, u64, f64)], t: u64) -> f64 {
    bursts
        .iter()
        .filter(|(start, dur, _)| t >= *start && t < start + dur)
        .map(|(_, _, m)| *m)
        .fold(1.0, f64::max)
}

/// Exponential inter-arrival gap for a Poisson process at `rate` per second
/// (inverse transform). `rand_distr`'s `Exp` is not used because that crate
/// is unvetted in this workspace.
fn exponential_gap(rng: &mut impl Rng, rate: f64) -> f64 {
    -(1.0 - rng.gen::<f64>()).ln() / rate
}

async fn run(args: RunArgs) -> Result<()> {
    anyhow::ensure!(!args.servers.is_empty(), "--servers required");
    let bursts: Vec<(u64, u64, f64)> = args
        .bursts
        .iter()
        .map(|b| -> Result<_> {
            let (window, m) = b
                .split_once(':')
                .ok_or_else(|| anyhow::anyhow!("burst {b}"))?;
            let (s, d) = window
                .split_once('+')
                .ok_or_else(|| anyhow::anyhow!("burst {b}"))?;
            Ok((s.parse()?, d.parse()?, m.parse()?))
        })
        .collect::<Result<_>>()?;
    let metrics = Arc::new(Metrics::new());
    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(2048)
        .timeout(Duration::from_secs(60))
        .build()?;
    let mix = Arc::new(workload::Mix::new());
    let submit_end = Instant::now() + Duration::from_secs(args.duration_secs);
    let fleet_end = submit_end + Duration::from_secs(args.drain_secs);

    if let Some(url) = &args.pg_url {
        db::reset_statements(url).await;
        tokio::spawn(db::sample_loop(url.clone(), metrics.clone(), fleet_end));
    }

    // Runner fleet.
    let model = runner::JobModel {
        median: Duration::from_millis(args.job_median_ms),
        sigma: args.job_sigma,
        timeline_updates: args.timeline_updates,
        renew_every: Duration::from_secs(args.renew_secs),
        failure_rate: args.failure_rate,
    };
    let mut fleet = Vec::new();
    for i in 0..args.runners {
        let runner = runner::Runner {
            http: http.clone(),
            base: args.servers[i as usize % args.servers.len()].clone(),
            system_token: args.system_token.clone(),
            name: format!("load-runner-{i}"),
            metrics: metrics.clone(),
            model: model.clone(),
        };
        fleet.push(tokio::spawn(runner.run(fleet_end)));
        // Stagger registration so it is not itself a thundering herd.
        if i % 50 == 49 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // Open-loop Poisson arrivals.
    let in_flight = Arc::new(tokio::sync::Semaphore::new(args.max_in_flight));
    let mut sequence: u64 = 0;
    let mut next = Instant::now();
    let mut rng = rand::rngs::StdRng::from_entropy();
    while Instant::now() < submit_end {
        let t = metrics.elapsed().as_secs();
        let rate = args.runs_per_sec * multiplier(&bursts, t);
        let gap = exponential_gap(&mut rng, rate);
        next += Duration::from_secs_f64(gap);
        if let Some(wait) = next.checked_duration_since(Instant::now()) {
            tokio::time::sleep(wait).await;
        }
        let intended = next;
        sequence += 1;
        let repo = format!(
            "load-org-{}/repo-{}",
            sequence % 37,
            rng.gen_range(0..args.repos)
        );
        let webhook = args.webhook_sha.is_some() && rng.gen_bool(args.webhook_fraction);
        let base = args.servers[sequence as usize % args.servers.len()].clone();
        let Ok(permit) = in_flight.clone().try_acquire_owned() else {
            // The harness itself is saturated: count it, never block the clock.
            metrics.incr("harness.dropped_arrival");
            continue;
        };
        let (http, metrics, mix) = (http.clone(), metrics.clone(), mix.clone());
        let (token, secret, sha) = (
            args.system_token.clone(),
            args.webhook_secret.clone(),
            args.webhook_sha.clone(),
        );
        let shape_index = mix.pick_index(&mut rng);
        tokio::spawn(async move {
            let _permit = permit;
            if webhook {
                let (body, signature, delivery) =
                    workload::webhook_push(&repo, sha.as_deref().unwrap(), &secret);
                let result = http
                    .post(format!("{base}/api/v1/github/webhooks"))
                    .header("x-github-event", "push")
                    .header("x-github-delivery", delivery)
                    .header("x-hub-signature-256", signature)
                    .header("content-type", "application/json")
                    .body(body)
                    .send()
                    .await;
                match result {
                    Ok(r) if r.status().is_success() => {
                        metrics.latency("webhook_ack", intended);
                        metrics.incr("webhook.accepted");
                        // lint (1) + ci (3) jobs per push.
                        metrics.add("jobs.submitted", 4);
                        metrics.add("workflows.submitted", 2);
                    }
                    Ok(r) => metrics.incr(&format!("webhook.status_{}", r.status().as_u16())),
                    Err(_) => metrics.incr("webhook.transport_error"),
                }
            } else {
                let shape = &mix.shapes[shape_index];
                let result = http
                    .post(format!("{base}/api/v1/runs"))
                    .bearer_auth(&token)
                    .json(&workload::api_submission(shape, &repo, sequence))
                    .send()
                    .await;
                match result {
                    Ok(r) if r.status().is_success() => {
                        metrics.latency("submit", intended);
                        metrics.incr("api.accepted");
                        metrics.incr(&format!("shape.{}", shape.name));
                        metrics.add("jobs.submitted", shape.jobs as u64);
                        metrics.incr("workflows.submitted");
                    }
                    Ok(r) => metrics.incr(&format!("api.status_{}", r.status().as_u16())),
                    Err(_) => metrics.incr("api.transport_error"),
                }
            }
        });
    }
    let submitted_at_stop = metrics.counter("jobs.submitted");
    eprintln!("[load] submission window over: {submitted_at_stop} jobs submitted; draining");

    // Drain: wait until completions catch up with submissions.
    let drain_started = Instant::now();
    loop {
        let submitted = metrics.counter("jobs.submitted");
        let done = metrics.counter("job.completed");
        if done >= submitted || Instant::now() >= fleet_end {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let drain_secs = drain_started.elapsed().as_secs_f64();
    metrics.add("drain.backlog_left", {
        let s = metrics.counter("jobs.submitted");
        s.saturating_sub(metrics.counter("job.completed"))
    });
    for handle in fleet {
        handle.abort();
    }

    let statements = match &args.pg_url {
        Some(url) => db::top_statements(url).await,
        None => serde_json::Value::Null,
    };
    let config = serde_json::json!({
        "servers": args.servers,
        "runs_per_sec": args.runs_per_sec,
        "bursts": args.bursts,
        "webhook_fraction": args.webhook_fraction,
        "repos": args.repos,
        "duration_secs": args.duration_secs,
        "runners": args.runners,
        "job_median_ms": args.job_median_ms,
        "mean_jobs_per_run": mix.mean_jobs(),
        "target_jobs_per_day": args.runs_per_sec * mix.mean_jobs() * 86_400.0,
    });
    let mut summary = metrics.summary(&args.label, config);
    summary["drain_secs"] = drain_secs.into();
    summary["top_statements"] = statements;
    let dir = args.out.join(&args.label);
    std::fs::create_dir_all(&dir)?;
    std::fs::write(
        dir.join("summary.json"),
        serde_json::to_string_pretty(&summary)?,
    )?;
    std::fs::write(dir.join("timeline.csv"), metrics.timeline_csv())?;
    println!("{}", serde_json::to_string_pretty(&summary["totals"])?);
    println!("rates: {}", summary["rate_per_sec"]);
    println!("latency_ms: {}", summary["latency_ms"]);
    println!("drain_secs: {drain_secs:.1}");
    Ok(())
}

use rand::SeedableRng;
