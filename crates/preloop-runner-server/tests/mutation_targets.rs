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

const EXCLUDED: &[(&str, &str)] = &[];

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
    exclude_re: "send_github_check_request|resolve_repo_token|resolve_check_run_token|find_existing_check_run|report_check_run_queued|report_existing_check_run_queued|ensure_check_run_mapped|drain_check_run_sender|github_api_base",
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

#[test]
#[ignore = "triage an existing mutants.out report without rerunning it"]
fn triage_existing_report() {
    let id = std::env::var("PRELOOP_TRIAGE_TARGET").unwrap_or_else(|_| "T5".to_string());
    let report = workspace_root()
        .join("mutants.out")
        .join(&id)
        .join("mutants.out");
    assert!(report.is_dir(), "no report at {}", report.display());
    let scope = std::env::var("PRELOOP_TRIAGE_SCOPE").unwrap_or_default();
    let files = std::env::var("PRELOOP_TRIAGE_FILES")
        .unwrap_or_default()
        .split(',')
        .filter(|file| !file.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    triage(&id, &report, &scope, &files);
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
    let scope = targets
        .iter()
        .flat_map(|target| target.tests.iter().copied())
        .collect::<Vec<_>>()
        .join(" ");
    let files = targets
        .iter()
        .flat_map(|target| {
            target
                .args
                .windows(2)
                .filter(|pair| pair[0] == "--file")
                .map(|pair| pair[1].to_string())
        })
        .collect::<Vec<_>>();
    triage(id, &merged_dir, &scope, &files);

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

    let exclude_re = std::iter::once(target.exclude_re.to_string())
        .chain(EXCLUDED.iter().map(|(pattern, _)| (*pattern).to_string()))
        .collect::<Vec<_>>()
        .join("|");
    let mut cmd = Command::new("cargo");
    cmd.args(["mutants", "--baseline", "run"])
        .args(["--exclude-re", exclude_re.as_str()])
        .arg("--output")
        .arg(&out)
        .args(target.args);
    if let Some(shard) = shard() {
        cmd.args(["--shard", &shard]);
    }
    if let Some(jobs) = std::env::var_os("PRELOOP_MUTANTS_JOBS") {
        cmd.arg("--jobs").arg(jobs);
    }
    cmd.arg("--")
        .args(target.tests)
        .current_dir(root)
        .env("TMPDIR", &scratch)
        .env("PROPTEST_CASES", "8")
        .env(
            "RUST_TEST_THREADS",
            std::env::var("RUST_TEST_THREADS").unwrap_or_else(|_| "4".to_string()),
        )
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
    if !status.success() && mutants == 0 {
        panic!(
            "sub-run {}: cargo mutants exited {} without scoring a single mutant — the unmutated \
             baseline failed (report in {})",
            target.id,
            status
                .code()
                .map_or_else(|| "signal".to_string(), |code| code.to_string()),
            report.display()
        );
    }
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
        index < count,
        "PRELOOP_MUTANTS_SHARD must be 0..{count}, got {raw}"
    );
    Some(format!("{index}/{count}"))
}

const JEV_URL: &str = "https://api.typesafe.ai/v1/systemone";

fn triage(target_id: &str, report: &Path, scope: &str, files: &[String]) {
    let Ok(key) = std::env::var("JEV_API_KEY") else {
        return;
    };
    if key.is_empty() {
        return;
    }
    let missed = fs::read_to_string(report.join("missed.txt")).unwrap_or_default();
    let survivors: Vec<&str> = missed
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    if survivors.is_empty() {
        return;
    }

    let started = Instant::now();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime for triage");
    let mut rows = Vec::new();
    for (batch_index, batch) in survivors.chunks(50).enumerate() {
        let mut questions = serde_json::Map::new();
        for (index, mutant) in batch.iter().enumerate() {
            questions.insert(
                format!("s{index}"),
                serde_json::json!({
                    "type": "choice",
                    "instructions": {
                        "mutant": mutant,
                        "source_line": source_line(mutant),
                        "test_targets": scope,
                        "test_excerpt": test_excerpts(files, mutant),
                        "question": "This mutant survived: every test in the selected scope still passed with it applied. Classify it."
                    },
                    "criteria": {
                        "real_gap": "Behavior a user can observe is wrong when the mutant is applied, and no test asserts it",
                        "dead_code": "The code path cannot run in practice, so the change is unobservable",
                        "equivalent": "The change is semantically identical to the original for every input",
                        "scoping_artifact": "A test outside the selected test targets would likely catch it"
                    }
                }),
            );
        }
        let body = serde_json::json!({
            "state": { "target": target_id },
            "model": "jev-latest",
            "questions": questions
        });
        let response: Option<serde_json::Value> = runtime.block_on(async {
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .ok()?;
            let response = client
                .post(JEV_URL)
                .bearer_auth(&key)
                .json(&body)
                .send()
                .await
                .ok()?;
            if !response.status().is_success() {
                eprintln!("triage: jev returned {}", response.status());
                return None;
            }
            response.json().await.ok()
        });
        let Some(response) = response else {
            continue;
        };
        for (index, mutant) in batch.iter().enumerate() {
            let answer = response
                .pointer(&format!("/answers/s{index}"))
                .cloned()
                .unwrap_or_default();
            let verdict = answer["choice"].as_str().unwrap_or("unknown");
            let confidence = answer["confidence"].as_f64().unwrap_or(0.0);
            rows.push(format!(
                "| {} | {} | {verdict} | {confidence:.2} |",
                batch_index * 50 + index + 1,
                mutant.replace('|', "\\|")
            ));
        }
    }
    if rows.is_empty() {
        return;
    }
    let mut verdicts = std::collections::BTreeMap::new();
    for row in &rows {
        let mut fields = row.split('|');
        let verdict = fields.nth(3).unwrap_or("unknown").trim().to_string();
        *verdicts.entry(verdict).or_insert(0usize) += 1;
    }
    let caught = fs::read_to_string(report.join("caught.txt"))
        .map(|text| text.lines().filter(|line| !line.trim().is_empty()).count())
        .unwrap_or(0);
    let gaps = verdicts.get("real_gap").copied().unwrap_or(0);
    let score = if caught + gaps == 0 {
        "n/a".to_string()
    } else {
        format!("{:.0}%", 100.0 * caught as f64 / (caught + gaps) as f64)
    };
    let breakdown = verdicts
        .iter()
        .map(|(verdict, count)| format!("{verdict}={count}"))
        .collect::<Vec<_>>()
        .join(" ");
    eprintln!(
        "mutation target {target_id}: triage {breakdown} caught={caught} score={score} \
         (caught / (caught + real_gap))"
    );

    let mut markdown =
        String::from("| # | survivor | verdict | confidence |\n| --- | --- | --- | --- |\n");
    for row in &rows {
        markdown.push_str(row);
        markdown.push('\n');
    }
    let path = report.parent().unwrap_or(report).join("triage.md");
    fs::write(&path, &markdown).expect("write triage report");
    eprintln!(
        "mutation target {target_id}: triaged {} survivor(s) in {:.1?} -> {}",
        rows.len(),
        started.elapsed(),
        path.display()
    );
}

fn test_excerpts(files: &[String], mutant: &str) -> String {
    let mut keywords: Vec<&str> = Vec::new();
    if let Some((_, enclosing)) = mutant.rsplit_once(" in ") {
        keywords.push(enclosing.trim());
    }
    if let Some(rest) = mutant.split("delete field ").nth(1) {
        if let Some((field, tail)) = rest.split_once(" from struct ") {
            keywords.push(field.trim());
            if let Some(ty) = tail.split_whitespace().next() {
                let ty = ty.trim_end_matches(',');
                keywords.push(ty.rsplit("::").next().unwrap_or(ty));
            }
        }
    }
    keywords.retain(|keyword| !keyword.is_empty());
    if keywords.is_empty() {
        return String::new();
    }

    let root = workspace_root();
    let mut excerpt = String::new();
    let mut budget = 80usize;
    for file in files {
        let Ok(text) = fs::read_to_string(root.join(file)) else {
            continue;
        };
        let lines: Vec<&str> = text.lines().collect();
        let start = if file.contains("/tests/") {
            0
        } else {
            lines
                .iter()
                .position(|line| line.contains("mod tests") || line.contains("#[cfg(test)]"))
                .unwrap_or(lines.len())
        };
        for (index, line) in lines.iter().enumerate().skip(start) {
            if !keywords.iter().any(|keyword| line.contains(*keyword)) {
                continue;
            }
            for line in &lines[index.saturating_sub(6)..(index + 7).min(lines.len())] {
                if budget == 0 {
                    break;
                }
                excerpt.push_str(line);
                excerpt.push('\n');
                budget -= 1;
            }
            excerpt.push_str("---\n");
            if budget == 0 {
                break;
            }
        }
        if budget == 0 {
            break;
        }
    }
    if excerpt.is_empty() {
        "(no test code references the mutated symbols)".to_string()
    } else {
        excerpt
    }
}

fn source_line(mutant: &str) -> String {
    let mut parts = mutant.splitn(4, ':');
    let (Some(path), Some(line)) = (parts.next(), parts.next()) else {
        return String::new();
    };
    let Ok(line) = line.parse::<usize>() else {
        return String::new();
    };
    fs::read_to_string(workspace_root().join(path))
        .ok()
        .and_then(|text| {
            text.lines()
                .nth(line.saturating_sub(1))
                .map(|text| text.trim().to_string())
        })
        .unwrap_or_default()
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
