#![allow(missing_docs)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

const SCOPED_ENV: [&str; 7] = [
    "PRELOOP_GITHUB_TOKEN",
    "PRELOOP_GITHUB_API_URL",
    "PRELOOP_GITHUB_REPOSITORY",
    "PRELOOP_GITHUB_APP_DEFAULT_EVENTS",
    "PRELOOP_TEST_PG_URL",
    "PRELOOP_TEST_POSTGRES_URL",
    "PRELOOP_TEST_REQUIRE_POSTGRES",
];

const INITDB_DIRS: [&str; 10] = [
    "/Applications/Postgres.app/Contents/Versions/latest/bin",
    "/opt/homebrew/opt/postgresql@18/bin",
    "/opt/homebrew/opt/postgresql@17/bin",
    "/opt/homebrew/opt/postgresql@16/bin",
    "/usr/local/opt/postgresql@18/bin",
    "/usr/local/opt/postgresql@17/bin",
    "/usr/local/opt/postgresql@16/bin",
    "/usr/lib/postgresql/18/bin",
    "/usr/lib/postgresql/17/bin",
    "/usr/lib/postgresql/16/bin",
];

struct Target {
    id: &'static str,
    args: &'static [&'static str],
    exclude_re: &'static str,
    tests: &'static [&'static str],
    needs_postgres: bool,
}

const T1: Target = Target {
    id: "T1",
    args: &[
        "--package",
        "preloop-runner-server",
        "--features",
        "test-support",
        "--file",
        "crates/preloop-runner-server/src/control/logic.rs",
        "--file",
        "crates/preloop-runner-server/src/control/lite/poll.rs",
        "--file",
        "crates/preloop-runner-server/src/control/lite/promote.rs",
        "--file",
        "crates/preloop-runner-server/src/control/lite/lifecycle.rs",
        "--file",
        "crates/preloop-runner-server/src/control/lite/reaper.rs",
        "--file",
        "crates/preloop-runner-server/src/control/pg/dispatch.rs",
        "--file",
        "crates/preloop-runner-server/src/control/pg/lifecycle.rs",
        "--file",
        "crates/preloop-runner-server/src/control/pg/runners.rs",
        "--re",
        "claim_one|claim_candidate|on_job_enqueued|clear_assignment|runner_matches|sweep_stale_bindings|requeue_claimed",
    ],
    exclude_re: "^$",
    tests: &[
        "--lib",
        "--test",
        "concurrency",
        "--test",
        "webhooks",
        "--test",
        "registration",
        "--test",
        "broker",
    ],
    needs_postgres: true,
};

const T2: Target = Target {
    id: "T2",
    args: &[
        "--package",
        "preloop-runner-server",
        "--features",
        "test-support",
        "--file",
        "crates/preloop-runner-server/src/github.rs",
        "--file",
        "crates/preloop-runner-server/src/events/mod.rs",
        "--re",
        "verify_signature|decode_hex|claimed_repository|webhook_retry_backoff|handle_github_webhook|adapter_for|make_default_branch_events",
    ],
    exclude_re: "all_event_names|send_github_check_request|resolve_repo_token|resolve_check_run_token|find_existing_check_run|report_check_run_queued|report_existing_check_run_queued|ensure_check_run_mapped|drain_check_run_sender|github_api_base",
    tests: &[
        "--lib",
        "--test",
        "broker",
        "--test",
        "registration",
        "--test",
        "webhooks",
    ],
    needs_postgres: true,
};

const T3A: Target = Target {
    id: "T3a",
    args: &[
        "--package",
        "preloop-runner-server",
        "--features",
        "test-support",
        "--file",
        "crates/preloop-runner-server/src/control/logic.rs",
        "--file",
        "crates/preloop-runner-server/src/control/lite/lifecycle.rs",
        "--file",
        "crates/preloop-runner-server/src/control/pg/lifecycle.rs",
        "--re",
        "claim_preference|job_labels_covered_exactly|pair_runner",
    ],
    exclude_re: "^$",
    tests: &[
        "--lib",
        "--test",
        "webhooks",
        "--test",
        "registration",
        "--test",
        "broker",
    ],
    needs_postgres: true,
};

const T3B: Target = Target {
    id: "T3b",
    args: &[
        "--package",
        "preloop-cli",
        "--file",
        "crates/preloop-cli/src/main.rs",
        "--re",
        "host_runner_pool_size",
    ],
    exclude_re: "host_memory_mib|local_runner_pool_config",
    tests: &["--bin", "preloop"],
    needs_postgres: false,
};

const T3C: Target = Target {
    id: "T3c",
    args: &[
        "--package",
        "preloop-orchestrator",
        "--file",
        "crates/preloop-orchestrator/src/lib.rs",
        "--re",
        "on_demand_memory_cap|host_vm_capacity",
    ],
    exclude_re: "host_memory_mib|run_on_demand|golden_budget|wait_for_|ensure_host_externals|preload_images|ensure_golden_payload",
    tests: &["--lib", "--test", "runner_pool_lifecycle"],
    needs_postgres: false,
};

const T4: Target = Target {
    id: "T4",
    args: &[
        "--package",
        "preloop-runner-server",
        "--features",
        "test-support",
        "--file",
        "crates/preloop-runner-server/src/bootstrap.rs",
        "--file",
        "crates/preloop-runner-server/src/control/logic.rs",
        "--file",
        "crates/preloop-runner-server/src/control/lite/lifecycle.rs",
        "--file",
        "crates/preloop-runner-server/src/control/lite/queries.rs",
        "--file",
        "crates/preloop-runner-server/src/control/lite/reaper.rs",
        "--file",
        "crates/preloop-runner-server/src/control/pg/lifecycle.rs",
        "--file",
        "crates/preloop-runner-server/src/control/pg/lookups.rs",
        "--file",
        "crates/preloop-runner-server/src/control/pg/reaper.rs",
        "--re",
        "build_operational_snapshot_sync|read_snapshot_inputs|sample_state_once|count_run_statuses|status_inputs|list_runners|\\bqueue_stats\\b",
    ],
    exclude_re: "run_state_sampler|publish_snapshot|collect_storage_components",
    tests: &[
        "--lib", "--test", "broker", "--test", "logs", "--test", "runs_api", "--test", "recovery",
    ],
    needs_postgres: true,
};

const T5: Target = Target {
    id: "T5",
    args: &[
        "--package",
        "preloop-cli",
        "--file",
        "crates/preloop-cli/src/main.rs",
        "--re",
        "env_flag|parse_flag|logs_query|WorkflowSubmission",
    ],
    exclude_re: "cmd_engine|cmd_build_golden|cmd_logs|local_runner_pool_config|fetch_run_job_logs|follow_run_logs",
    tests: &["--bin", "preloop"],
    needs_postgres: false,
};

#[test]
#[ignore = "hours: mutation run, invoke it explicitly"]
fn t1_job_binding_lifecycle() {
    run_target_set("T1", &[&T1]);
}

#[test]
#[ignore = "hours: mutation run, invoke it explicitly"]
fn t2_webhook_signature() {
    run_target_set("T2", &[&T2]);
}

#[test]
#[ignore = "hours: mutation run, invoke it explicitly"]
fn t3_pool_warm_sizing() {
    run_target_set("T3", &[&T3A, &T3B, &T3C]);
}

#[test]
#[ignore = "hours: mutation run, invoke it explicitly"]
fn t4_queue_status_aggregation() {
    run_target_set("T4", &[&T4]);
}

#[test]
#[ignore = "hours: mutation run, invoke it explicitly"]
fn t5_cli_parsing() {
    run_target_set("T5", &[&T5]);
}

fn run_target_set(id: &str, targets: &[&Target]) {
    let root = workspace_root();
    let pg_url = std::env::var("PRELOOP_TEST_POSTGRES_URL").ok();
    let pg_required = std::env::var("PRELOOP_TEST_REQUIRE_POSTGRES").ok();
    if pg_url.is_none() && targets.iter().any(|target| target.needs_postgres) {
        assert!(
            initdb_available(),
            "the pg test suites need a Postgres: set PRELOOP_TEST_POSTGRES_URL or make initdb discoverable"
        );
    }

    let started = Instant::now();
    let mut merged = std::collections::BTreeMap::from([
        ("caught", String::new()),
        ("missed", String::new()),
        ("timeout", String::new()),
        ("unviable", String::new()),
    ]);
    let mut exit_ok = true;
    for target in targets {
        exit_ok &= run_sub(
            root.as_path(),
            target,
            pg_url.as_deref(),
            pg_required.as_deref(),
        );
        let report = root.join("mutants.out").join(target.id).join("mutants.out");
        for kind in ["caught", "missed", "timeout", "unviable"] {
            let text = fs::read_to_string(report.join(format!("{kind}.txt"))).unwrap_or_default();
            merged.get_mut(kind).unwrap().push_str(&text);
        }
    }

    let merged_dir = root.join("mutants.out").join(id).join("mutants.out");
    fs::create_dir_all(&merged_dir).expect("create merged report directory");
    for (kind, text) in &merged {
        fs::write(merged_dir.join(format!("{kind}.txt")), text).expect("write merged report");
    }

    let summary = merged
        .iter()
        .map(|(kind, text)| {
            format!(
                "{kind}={}",
                text.lines().filter(|line| !line.trim().is_empty()).count()
            )
        })
        .collect::<Vec<_>>()
        .join(" ");
    eprintln!(
        "mutation target {id}: {summary} in {:.1?} ({})",
        started.elapsed(),
        if exit_ok {
            "clean exit"
        } else {
            "cargo mutants failed"
        }
    );

    let missed = merged["missed"].clone();
    assert!(
        missed.trim().is_empty(),
        "target {id}: surviving mutants (no test failed with them applied):\n{missed}"
    );
    assert!(exit_ok, "target {id}: cargo mutants exited with an error");
}

fn run_sub(root: &Path, target: &Target, pg_url: Option<&str>, pg_required: Option<&str>) -> bool {
    let out = root.join("mutants.out").join(target.id);
    let scratch = root.join("target").join("mutants-tmp").join(target.id);
    fs::create_dir_all(&scratch).expect("create mutation scratch dir");
    fs::create_dir_all(&out).expect("create mutants.out target directory");

    let mut cmd = Command::new("cargo");
    cmd.args(["mutants", "--baseline", "run"])
        .args(["--exclude-re", target.exclude_re])
        .arg("--output")
        .arg(&out)
        .args(target.args);
    if let Some(shard) = shard() {
        cmd.args(["--shard", &shard]);
    }
    cmd.arg("--")
        .args(target.tests)
        .current_dir(root)
        .env("TMPDIR", &scratch)
        .env("PROPTEST_CASES", "8")
        .env("RUST_TEST_THREADS", "4")
        .env("RUSTFLAGS", "-A warnings");
    for var in SCOPED_ENV {
        cmd.env_remove(var);
    }
    if let Some(url) = pg_url {
        cmd.env("PRELOOP_TEST_POSTGRES_URL", url);
    }
    if let Some(required) = pg_required {
        cmd.env("PRELOOP_TEST_REQUIRE_POSTGRES", required);
    }

    eprintln!(
        "mutation sub-run {}: cargo mutants {} -- {}",
        target.id,
        target.args.join(" "),
        target.tests.join(" ")
    );
    let started = Instant::now();
    let status = cmd
        .status()
        .expect("failed to start cargo mutants; install it with `brew install cargo-mutants`");
    let report = out.join("mutants.out");
    assert!(
        report.join("outcomes.json").exists(),
        "sub-run {}: cargo-mutants produced no report in {}",
        target.id,
        report.display()
    );
    let mutants: usize = ["caught", "missed", "timeout", "unviable"]
        .iter()
        .map(|kind| {
            fs::read_to_string(report.join(format!("{kind}.txt")))
                .map(|text| text.lines().filter(|line| !line.trim().is_empty()).count())
                .unwrap_or(0)
        })
        .sum();
    assert!(
        mutants > 0,
        "sub-run {}: no mutants were generated — its --file/--re filters match no code",
        target.id
    );
    eprintln!(
        "mutation sub-run {}: cargo mutants exit {} in {:.1?}",
        target.id,
        status
            .code()
            .map_or_else(|| "signal".to_string(), |code| code.to_string()),
        started.elapsed()
    );
    status.success()
}

fn shard() -> Option<String> {
    let raw = std::env::var("PRELOOP_MUTANTS_SHARD").ok()?;
    let (index, count) = raw
        .split_once('/')
        .unwrap_or_else(|| panic!("PRELOOP_MUTANTS_SHARD must be <index>/<count>, got {raw}"));
    let index: u32 = index
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("PRELOOP_MUTANTS_SHARD index must be a number, got {raw}"));
    let count: u32 = count
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("PRELOOP_MUTANTS_SHARD count must be a number, got {raw}"));
    assert!(
        index >= 1 && index <= count,
        "PRELOOP_MUTANTS_SHARD must be 1..={count}, got {raw}"
    );
    Some(format!("{index}/{count}"))
}

fn initdb_available() -> bool {
    if INITDB_DIRS
        .iter()
        .any(|dir| Path::new(dir).join("initdb").exists())
    {
        return true;
    }
    std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join("initdb").exists()))
        .unwrap_or(false)
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}
