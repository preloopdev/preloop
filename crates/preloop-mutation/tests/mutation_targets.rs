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
//! Reports land in `mutants.out/<target>/mutants.out/` (`missed.txt` lists
//! survivors); `t3` merges its three package-scoped sub-runs there.
//!
//! Both the lite and the Postgres-backed `control/pg/` implementations are
//! mutated. The pg suites use `PRELOOP_TEST_POSTGRES_URL` when it is set (each
//! test then creates its own database on that server) and otherwise bootstrap
//! a disposable cluster from `initdb`; the tests refuse to run when neither is
//! available, because a skipped pg suite would make every `control/pg` mutant
//! survive and drown the real gaps.
//!
//! Per-mutant test runs are scoped to the test targets listed in `Target::tests`
//! instead of the whole package suite: those are the targets that exercise the
//! scoped functions, and running the rest per mutant only adds hours. The one
//! failure mode is over-reporting a gap — a mutant killed only by a test target
//! outside the list would show up as missed — so survivors are checked against
//! the code before conclusions are drawn.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

/// Process-global variables read by the test suites. An ambient value changes
/// test behavior (see `preloop-runner-server`'s `state.rs` guards), so they are
/// stripped from the mutation run and only the Postgres settings are restored
/// when the caller provided them.
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

/// One mutation-testing sub-run: a package/file scope narrowed by a regex over
/// the mutant names that `cargo mutants --list` shows.
struct Target {
    /// Report directory name under `mutants.out/` and scratch dir name.
    id: &'static str,
    /// Forwarded verbatim to `cargo mutants`.
    args: &'static [&'static str],
    /// Mutants to drop: process-, network- or VM-bound code that host tests
    /// cannot kill, so keeping it would only add noise to the survival rate.
    exclude_re: &'static str,
    /// Arguments forwarded to `cargo test` for the mutated package, selecting
    /// the test targets that cover this scope (see the module docs).
    tests: &'static [&'static str],
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
};

/// Waitlist ordering (runner-server half of T3).
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
};

/// Warm-pool sizing (CLI half of T3).
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
    tests: &["--lib"],
};

/// On-demand memory cap (orchestrator half of T3).
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
    tests: &["--lib"],
};

/// Job/binding lifecycle: claim gating, assignment exclusivity, stale-binding
/// release and requeue — the leaked-binding failure mode.
#[test]
#[ignore = "hours: run one target at a time, see module docs"]
fn t1_job_binding_lifecycle() {
    run_target_set("T1", &[&T1]);
}

/// GitHub webhook HMAC verification (`X-Hub-Signature-256`) and event dispatch.
#[test]
#[ignore = "hours: run one target at a time, see module docs"]
fn t2_webhook_signature() {
    run_target_set("T2", &[&T2]);
}

/// Pool/waitlist ordering and warm-pool sizing across server, orchestrator, CLI.
#[test]
#[ignore = "hours: run one target at a time, see module docs"]
fn t3_pool_warm_sizing() {
    run_target_set("T3", &[&T3A, &T3B, &T3C]);
}

/// Queue depth and run-status aggregation behind `preloop status`.
#[test]
#[ignore = "hours: run one target at a time, see module docs"]
fn t4_queue_status_aggregation() {
    run_target_set("T4", &[&T4]);
}

/// CLI argument parsing helpers and their boolean/filter parsing.
#[test]
#[ignore = "hours: run one target at a time, see module docs"]
fn t5_cli_parsing() {
    run_target_set("T5", &[&T5]);
}

fn run_target_set(id: &str, targets: &[&Target]) {
    let root = workspace_root();
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

    let started = Instant::now();
    let mut merged = std::collections::BTreeMap::from([
        ("caught", String::new()),
        ("missed", String::new()),
        ("timeout", String::new()),
        ("unviable", String::new()),
    ]);
    let mut exit_ok = true;
    for target in targets {
        let outcome = run_sub(
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
        exit_ok &= outcome;
    }

    // Merged report, so every target's summary step reads the same layout.
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

/// Runs one cargo-mutants sub-run. Returns whether cargo-mutants exited
/// cleanly; the report is read back from `mutants.out/<id>/mutants.out/`.
fn run_sub(root: &Path, target: &Target, pg_url: Option<&str>, pg_required: Option<&str>) -> bool {
    let out = root.join("mutants.out").join(target.id);
    // Scratch trees live under target/ — same filesystem as the workspace,
    // gitignored, and on real disk even when $TMPDIR is a small tmpfs (the
    // runner guests' temp is one). cargo-mutants deletes them on exit.
    let scratch = root.join("target").join("mutants-tmp").join(target.id);
    fs::create_dir_all(&scratch).expect("create mutation scratch dir");
    // cargo-mutants writes its report into this directory but does not create
    // the parent chain.
    fs::create_dir_all(&out).expect("create mutants.out target directory");

    let mut cmd = Command::new("cargo");
    cmd.args(["mutants", "--baseline", "run"])
        .args(["--exclude-re", target.exclude_re])
        .arg("--output")
        .arg(&out)
        .args(target.args)
        // Scoping the per-mutant test run: only these test targets are built
        // and run for each mutant (see the module docs).
        .arg("--")
        .args(target.tests)
        .current_dir(root)
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
