//! Mutation-testing targets: one `cargo-mutants` run per risk area.
//!
//! Each test here drives a full mutation run over a slice of the codebase and
//! fails when any mutant survives. They are `#[ignore]`d because one target
//! takes hours and they must not share the tree's scratch space, so run
//! exactly one at a time:
//!
//! ```sh
//! cargo test --locked -p preloop-mutation --test mutation_targets -- \
//!     --ignored --exact t1_job_binding_lifecycle --nocapture
//! ```
//!
//! Reports land in `mutants.out/<target>/` (`missed.txt` lists survivors).
//!
//! Both the lite and the Postgres-backed `control/pg/` implementations are
//! mutated. The pg suites use `PRELOOP_TEST_POSTGRES_URL` when it is set (each
//! test then creates its own database on that server) and otherwise bootstrap
//! a disposable cluster from `initdb`; the tests refuse to run when neither is
//! available, because a skipped pg suite would make every `control/pg` mutant
//! survive and drown the real gaps.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

/// Process-global variables read by the test suites. An ambient value changes
/// test behavior (see `preloop-runner-server`'s `state.rs` guards), so they are
/// stripped from the mutation run and only the Postgres URL is restored when
/// the caller opted into pg coverage.
const SCOPED_ENV: [&str; 7] = [
    "PRELOOP_GITHUB_TOKEN",
    "PRELOOP_GITHUB_API_URL",
    "PRELOOP_GITHUB_REPOSITORY",
    "PRELOOP_GITHUB_APP_DEFAULT_EVENTS",
    "PRELOOP_TEST_PG_URL",
    "PRELOOP_TEST_POSTGRES_URL",
    "PRELOOP_TEST_REQUIRE_POSTGRES",
];

/// Postgres installs the pg suites can bootstrap their disposable clusters
/// from when no `PRELOOP_TEST_POSTGRES_URL` is set.
const INITDB_DIRS: [&str; 3] = [
    "/Applications/Postgres.app/Contents/Versions/latest/bin",
    "/opt/homebrew/bin",
    "/usr/local/bin",
];

/// One mutation-testing target: a package/file scope narrowed by a regex over
/// the mutant names that `cargo mutants --list` shows.
struct Target {
    /// Report directory name under `mutants.out/` and scratch dir name.
    id: &'static str,
    /// Forwarded verbatim to `cargo mutants`.
    args: &'static [&'static str],
    /// Mutants to drop: process-, network- or VM-bound code that host tests
    /// cannot kill, so keeping it would only add noise to the survival rate.
    exclude_re: &'static str,
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
};

const T3: Target = Target {
    id: "T3",
    args: &[
        "--package",
        "preloop-runner-server",
        "--package",
        "preloop-orchestrator",
        "--package",
        "preloop-cli",
        "--features",
        "preloop-runner-server/test-support",
        "--file",
        "crates/preloop-runner-server/src/control/logic.rs",
        "--file",
        "crates/preloop-runner-server/src/control/lite/lifecycle.rs",
        "--file",
        "crates/preloop-runner-server/src/control/pg/lifecycle.rs",
        "--file",
        "crates/preloop-orchestrator/src/lib.rs",
        "--file",
        "crates/preloop-cli/src/main.rs",
        "--re",
        "claim_preference|job_labels_covered_exactly|pair_runner|host_runner_pool_size|on_demand_memory_cap|host_vm_capacity",
    ],
    exclude_re: "host_memory_mib|local_runner_pool_config|run_on_demand|golden_budget|wait_for_|ensure_host_externals|preload_images|ensure_golden_payload",
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
};

const T5: Target = Target {
    id: "T5",
    args: &[
        "--package",
        "preloop-cli",
        "--file",
        "crates/preloop-cli/src/main.rs",
        "--re",
        "env_flag|parse_flag|logs_query",
    ],
    exclude_re: "cmd_engine|cmd_build_golden|cmd_logs|local_runner_pool_config|fetch_run_job_logs|follow_run_logs",
};

/// Job/binding lifecycle: claim gating, assignment exclusivity, stale-binding
/// release and requeue — the leaked-binding failure mode.
#[test]
#[ignore = "hours: run one target at a time, see module docs"]
fn t1_job_binding_lifecycle() {
    run(&T1);
}

/// GitHub webhook HMAC verification (`X-Hub-Signature-256`) and event dispatch.
#[test]
#[ignore = "hours: run one target at a time, see module docs"]
fn t2_webhook_signature() {
    run(&T2);
}

/// Pool/waitlist ordering and warm-pool sizing across server, orchestrator, CLI.
#[test]
#[ignore = "hours: run one target at a time, see module docs"]
fn t3_pool_warm_sizing() {
    run(&T3);
}

/// Queue depth and run-status aggregation behind `preloop status`.
#[test]
#[ignore = "hours: run one target at a time, see module docs"]
fn t4_queue_status_aggregation() {
    run(&T4);
}

/// CLI argument parsing helpers and their boolean/filter parsing.
#[test]
#[ignore = "hours: run one target at a time, see module docs"]
fn t5_cli_parsing() {
    run(&T5);
}

fn run(target: &Target) {
    let root = workspace_root();
    let out = root.join("mutants.out").join(target.id);
    // Scratch trees live under target/ — same filesystem as the workspace,
    // gitignored, and on real disk even when $TMPDIR is a small tmpfs (the
    // runner guests' temp is one). cargo-mutants deletes them on exit.
    let scratch = root.join("target").join("mutants-tmp").join(target.id);
    fs::create_dir_all(&scratch).expect("create mutation scratch dir");
    // cargo-mutants writes its report into this directory but does not create
    // the parent chain.
    fs::create_dir_all(&out).expect("create mutants.out target directory");

    let pg_url = std::env::var("PRELOOP_TEST_POSTGRES_URL").ok();
    let pg_required = std::env::var("PRELOOP_TEST_REQUIRE_POSTGRES").ok();
    if pg_url.is_none() {
        // The pg suites bootstrap disposable clusters from `initdb` when no
        // server URL is given, and skip themselves when they cannot find it.
        // A skipped pg suite would make every `control/pg` mutant survive and
        // drown the real gaps, so refuse to run instead.
        assert!(
            initdb_available(),
            "the pg test suites need a Postgres: set PRELOOP_TEST_POSTGRES_URL \
             or install one (initdb must be discoverable)"
        );
    }
    let mut cmd = Command::new("cargo");
    cmd.args(["mutants", "--baseline", "run", "--test-tool", test_tool()])
        .args(["--exclude-re", target.exclude_re])
        .arg("--output")
        .arg(&out)
        .args(target.args)
        .current_dir(&root)
        .env("TMPDIR", &scratch)
        // Keep test runs deterministic and bounded, like ci.yml's fast shards.
        .env("PROPTEST_CASES", "8")
        // Same flags as the rust-test shards so build fingerprints match.
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
        "mutation target {}: cargo mutants {}",
        target.id,
        target.args.join(" ")
    );
    let started = Instant::now();
    let status = cmd
        .status()
        .expect("failed to start cargo mutants; install it with `brew install cargo-mutants`");
    let elapsed = started.elapsed();

    // cargo-mutants nests its report at `<output>/mutants.out/`.
    let report = out.join("mutants.out");
    let counts: Vec<(String, usize)> = ["caught", "missed", "timeout", "unviable"]
        .iter()
        .map(|kind| {
            let n = fs::read_to_string(report.join(format!("{kind}.txt")))
                .map(|text| text.lines().filter(|line| !line.trim().is_empty()).count())
                .unwrap_or(0);
            ((*kind).to_string(), n)
        })
        .collect();
    let summary = counts
        .iter()
        .map(|(kind, n)| format!("{kind}={n}"))
        .collect::<Vec<_>>()
        .join(" ");
    eprintln!(
        "mutation target {}: {} in {:.1?} (cargo mutants exit {})",
        target.id,
        summary,
        elapsed,
        status
            .code()
            .map_or_else(|| "signal".to_string(), |c| c.to_string())
    );

    assert!(
        report.join("outcomes.json").exists(),
        "target {}: cargo-mutants produced no report in {}",
        target.id,
        report.display()
    );
    let missed = fs::read_to_string(report.join("missed.txt")).unwrap_or_default();
    assert!(
        missed.trim().is_empty(),
        "target {}: surviving mutants (no test failed with them applied):\n{}",
        target.id,
        missed
    );
    assert!(
        status.success(),
        "target {}: cargo mutants exited without survivors listed in missed.txt",
        target.id
    );
}

/// Whether the pg suites can bootstrap their disposable clusters here.
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

/// The mutation runs execute the affected package's suite per mutant; nextest
/// is faster at that than plain `cargo test`, and CI installs it, but fall back
/// so the tests stay runnable in a bare checkout.
fn test_tool() -> &'static str {
    let nextest = Command::new("cargo")
        .args(["nextest", "--version"])
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false);
    if nextest { "nextest" } else { "cargo" }
}
