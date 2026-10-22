//! Integration tests for the engine wire mirrors and the NDJSON/SSE decoders,
//! driven by the recorded fixtures under `tests/fixtures/`.
//!
//! Only the crate's public API is used, so these tests exercise the same
//! parsing path the client takes for a real engine response.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Utc};
use preloop_tui::api::{
    DebugSessions, ExecStatus, FeedEvent, LiveLogChunk, OperationalSnapshot, Overall, PoolBindings,
    RunRecord, RunnersList, SecretList, StepRecord, WebhookDeliveries, WebhookHealth,
    parse_feed_line, sort_steps,
};
use preloop_tui::client::engine::{NdjsonDecoder, SseDecoder, SseEvent};

/// The instant the fixtures were recorded at.
const NOW: &str = "2026-10-22T12:20:00Z";
/// Run the first three fixtures agree on.
const RUN_ID: &str = "8f3a91c2-5d10-4b7e-9a10-2c4e7f1a9b01";
/// The `cargo test` step of that run's sandbox leg.
const CARGO_TEST_STEP_ID: &str = "c4e1a7d2-3f0b-4b8a-9d61-7e2a0c5b1f01";
/// Micros since the epoch for 2026-10-22T12:14:33Z.
const CARGO_TEST_STARTED_AT_US: i64 = 1_792_671_273_000_000;
/// Micros since the epoch for 2026-10-22T12:18:54Z.
const CARGO_TEST_FINISHED_AT_US: i64 = 1_792_671_534_000_000;

fn fixture(name: &str) -> String {
    let path = format!("{}/tests/fixtures/{}", env!("CARGO_MANIFEST_DIR"), name);
    std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {path}: {error}"))
}

fn anchor() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(NOW)
        .expect("anchor parses")
        .with_timezone(&Utc)
}

fn runs() -> Vec<RunRecord> {
    serde_json::from_str(&fixture("runs.json")).expect("runs.json parses")
}

fn run(number: u64) -> RunRecord {
    runs()
        .into_iter()
        .find(|run| run.run_number == number)
        .unwrap_or_else(|| panic!("run {number} is in the fixture"))
}

/// The span the engine shows for a job: last step finish minus first step start.
fn job_span(steps: &[StepRecord]) -> Option<TimeDelta> {
    let started = steps.iter().filter_map(|step| step.started_at).min()?;
    let finished = steps.iter().filter_map(|step| step.finished_at).max()?;
    Some(finished - started)
}

#[test]
fn status_snapshot_parses() {
    let snapshot: OperationalSnapshot =
        serde_json::from_str(&fixture("status.json")).expect("status.json parses");

    assert_eq!(snapshot.schema_version, 2);
    assert_eq!(snapshot.overall, Overall::Ok);
    assert_eq!(snapshot.snapshot_age_seconds, 0.4);
    assert_eq!(snapshot.service.uptime_seconds, 273600);
    assert_eq!(snapshot.service.instance_id, "cpane-7");
    assert!(!snapshot.service.shutdown_requested);
    assert_eq!(snapshot.runs.queued, 1);
    assert_eq!(snapshot.runs.in_progress, 3);
    assert_eq!(snapshot.runs.completed, 388);
    assert_eq!(snapshot.active_runs.len(), 3);
    assert!(
        snapshot.active_runs.iter().all(|run| run.run_id != RUN_ID),
        "the failed run is not in flight"
    );
    assert_eq!(snapshot.jobs.ready, 2);
    assert_eq!(snapshot.jobs.dependency_blocked, 3);
    assert_eq!(snapshot.jobs.oldest_ready_seconds, Some(42.0));
    assert_eq!(snapshot.runners.registered, 5);
    assert_eq!(snapshot.runners.max_lease_age_seconds, Some(840.0));
    assert_eq!(snapshot.runners.assignments.len(), 1);
    assert_eq!(snapshot.runners.assignments[0].runner_id, 4);
    assert_eq!(snapshot.runners.assignments[0].job_id, "test (sandbox)");
    assert_eq!(snapshot.pool.mode, "warm");
    assert_eq!(snapshot.pool.idle, 3);
    assert_eq!(snapshot.pool.queue_depth, 2);
    assert_eq!(
        snapshot.pool.labels,
        vec!["linux".to_owned(), "x64".to_owned()]
    );
    assert_eq!(snapshot.vms.count.runner, 5);
    assert_eq!(snapshot.vms.count.golden, 1);
    assert_eq!(snapshot.vms.host_usage.cpu_cores, Some(9.2));
    assert_eq!(snapshot.vms.host_usage.mem_total_bytes, Some(68719476736));
    assert_eq!(
        snapshot.vms.host_usage.mem_available_bytes,
        Some(24696061952)
    );
    assert!(snapshot.github.configured);
    assert_eq!(
        snapshot
            .github
            .rate_limit
            .as_ref()
            .map(|limit| limit.remaining),
        Some(4812)
    );
    assert_eq!(snapshot.debug.active_sessions, 1);
    assert_eq!(snapshot.debug.oldest_session_seconds, Some(846.0));
    assert_eq!(snapshot.conditions.len(), 1);
    assert_eq!(snapshot.conditions[0].code, "webhook_dead_letters");
    assert_eq!(snapshot.conditions[0].severity, "error");
    assert_eq!(
        snapshot.conditions[0].message,
        "2 deliveries dead-lettered (oldest 3h12m)"
    );
    assert!(snapshot.conditions[0].exemplars.is_empty());
}

#[test]
fn status_earlier_is_the_same_snapshot_earlier() {
    let later: OperationalSnapshot =
        serde_json::from_str(&fixture("status.json")).expect("status.json parses");
    let earlier: OperationalSnapshot =
        serde_json::from_str(&fixture("status_earlier.json")).expect("status_earlier.json parses");

    assert_eq!(
        earlier.observed_at,
        Some(
            DateTime::parse_from_rfc3339("2026-10-22T12:15:48Z")
                .expect("timestamp parses")
                .with_timezone(&Utc)
        )
    );
    assert_eq!(later.observed_at, Some(anchor()));
    assert!(earlier.observed_at < later.observed_at);
    assert_eq!(earlier.debug.oldest_session_seconds, Some(600.0));
    assert_eq!(later.debug.oldest_session_seconds, Some(846.0));
    assert_eq!(earlier.overall, later.overall);
    assert_eq!(earlier.service.uptime_seconds, later.service.uptime_seconds);
}

#[test]
fn runs_fixture_filters_to_the_ci_runs_that_are_red_or_running() {
    let runs = runs();
    assert_eq!(runs.len(), 41);
    assert_eq!(runs.first().map(|run| run.run_number), Some(412));
    assert_eq!(runs.last().map(|run| run.run_number), Some(372));

    let matched: Vec<u64> = runs
        .iter()
        .filter(|run| {
            run.workflow_name() == "ci.yml"
                && matches!(run.status, ExecStatus::Failure | ExecStatus::InProgress)
        })
        .map(|run| run.run_number)
        .collect();
    assert_eq!(matched, vec![412, 411, 398]);

    let failed = run(412);
    assert_eq!(failed.status, ExecStatus::Failure);
    assert_eq!(failed.short_id(), "8f3a91c2");
    assert_eq!(failed.run_id, RUN_ID);
    assert_eq!(failed.ref_label(), "#619 sandbox-service");
    assert_eq!(failed.pr_number(), Some(619));
    assert_eq!(failed.workflow_name(), "ci.yml");
    assert_eq!(failed.job_status("test (sandbox)"), ExecStatus::Failure);
    assert_eq!(failed.job_status("test (runner)"), ExecStatus::Success);
    assert_eq!(failed.job_status("e2e"), ExecStatus::Pending);
    assert_eq!(failed.job_status("nothing here"), ExecStatus::Unknown);
    assert_eq!(failed.job_label("test (sandbox)"), "test (sandbox)");
    assert_eq!(failed.job_label("nothing here"), "nothing here");
    assert_eq!(
        failed.job_base_ids.get("test (runner)").map(String::as_str),
        Some("test")
    );
    assert_eq!(
        failed
            .job_base_ids
            .get("test (sandbox)")
            .map(String::as_str),
        Some("test")
    );
    assert_eq!(
        failed
            .job_detail("test (sandbox)")
            .map(|job| job.steps.len()),
        Some(6)
    );
    let dogfood = failed.job_detail("dogfood").expect("dogfood job detail");
    assert_eq!(dogfood.conclusion, "pending");
    assert!(dogfood.steps.iter().all(|step| step.started_at.is_none()));
    let push_state = failed.push_state.as_ref().expect("push state");
    assert_eq!(push_state.status, "synced");
    assert_eq!(push_state.pr_number, Some(619));
    assert_eq!(push_state.error, None);
    assert_eq!(push_state.effective_sha, None);

    let running = run(411);
    assert_eq!(running.status, ExecStatus::InProgress);
    assert_eq!(running.ref_label(), "#751 sbx-observability");
    assert_eq!(running.job_status("test (runner)"), ExecStatus::InProgress);
    assert_eq!(running.job_status("e2e"), ExecStatus::Queued);
    assert_eq!(running.completed_at, None);

    let queued = run(408);
    assert_eq!(queued.status, ExecStatus::Queued);
    assert_eq!(queued.started_at, None);
    assert_eq!(queued.ref_label(), "main");

    let pushed = run(409);
    assert_eq!(pushed.workflow_name(), "supply-chain.yml");
    assert_eq!(pushed.ref_label(), "main");
    assert_eq!(pushed.push_state, None);
}

#[test]
fn sandbox_steps_sort_into_runner_order_and_time_the_failure() {
    let failed = run(412);
    let job = failed
        .job_detail("test (sandbox)")
        .expect("sandbox job detail");
    assert_eq!(job.conclusion, "failure");

    let mut steps: Vec<StepRecord> = job.steps.clone();
    steps.reverse();
    sort_steps(&mut steps);
    let names: Vec<&str> = steps.iter().map(|step| step.name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "Set up job",
            "checkout",
            "rust cache",
            "cargo test",
            "upload junit",
            "Complete job"
        ]
    );
    let runner_numbers: Vec<Option<u32>> = steps.iter().map(|step| step.runner_number).collect();
    assert_eq!(
        runner_numbers,
        vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)]
    );

    let cargo = steps
        .iter()
        .find(|step| step.name == "cargo test")
        .expect("cargo test step");
    assert_eq!(cargo.id, CARGO_TEST_STEP_ID);
    assert_eq!(cargo.status(), ExecStatus::Failure);
    assert_eq!(cargo.workflow_index, Some(2));
    assert_eq!(cargo.context_name.as_deref(), Some("cargo_test"));
    assert_eq!(cargo.elapsed(anchor()), Some(TimeDelta::seconds(261)));

    let skipped = steps
        .iter()
        .find(|step| step.name == "upload junit")
        .expect("upload junit step");
    assert_eq!(skipped.status(), ExecStatus::Skipped);
    assert_eq!(skipped.elapsed(anchor()), None);

    let setup = steps.first().expect("first step");
    assert_eq!(setup.status(), ExecStatus::Success);
    assert_eq!(setup.kind, "synthetic");
    assert_eq!(setup.elapsed(anchor()), Some(TimeDelta::seconds(1)));

    // The job spans the fixture is built to reproduce.
    assert_eq!(job_span(&steps), Some(TimeDelta::seconds(280)));
    for (job_id, seconds) in [("fmt", 38i64), ("clippy", 182), ("test (runner)", 371)] {
        let job = failed.job_detail(job_id).expect("job detail");
        assert_eq!(
            job_span(&job.steps),
            Some(TimeDelta::seconds(seconds)),
            "{job_id} span"
        );
    }
}

#[test]
fn live_log_chunks_assemble_the_step_output() {
    let chunks: Vec<LiveLogChunk> =
        serde_json::from_str(&fixture("live_8f3a.json")).expect("live_8f3a.json parses");
    assert!(chunks.len() >= 3, "the decoder path wants several chunks");

    let mut lines: BTreeMap<u64, String> = BTreeMap::new();
    for chunk in &chunks {
        assert_eq!(chunk.step_id, CARGO_TEST_STEP_ID);
        assert_eq!(chunk.count, chunk.value.len());
        for (offset, line) in chunk.value.iter().enumerate() {
            lines.insert(chunk.start_line + offset as u64, line.clone());
        }
    }

    assert_eq!(lines.len(), 138);
    assert_eq!(lines.keys().next(), Some(&1204));
    assert_eq!(lines.keys().next_back(), Some(&1341));
    assert!(
        lines
            .get(&1204)
            .expect("line 1204")
            .starts_with("##[group]")
    );
    assert_eq!(
        lines.get(&1288).map(String::as_str),
        Some("running 53 tests")
    );
    assert_eq!(
        lines.get(&1292).map(String::as_str),
        Some("test reconcile_orphan ... FAILED")
    );
    assert_eq!(
        lines.get(&1340).map(String::as_str),
        Some("test result: FAILED. 52 passed; 1 failed")
    );
    assert_eq!(
        lines.get(&1341).map(String::as_str),
        Some("##[error]Process completed with exit code 101.")
    );

    let running: Vec<LiveLogChunk> =
        serde_json::from_str(&fixture("live_c01d.json")).expect("live_c01d.json parses");
    assert_eq!(running.len(), 2);
    let mut running_lines: BTreeMap<u64, String> = BTreeMap::new();
    for chunk in &running {
        assert_eq!(chunk.step_id, "9b2f4c10-7e3d-4a55-8c21-0d6e1f7a3c42");
        assert_eq!(chunk.count, chunk.value.len());
        for (offset, line) in chunk.value.iter().enumerate() {
            running_lines.insert(chunk.start_line + offset as u64, line.clone());
        }
    }
    assert_eq!(running_lines.len(), 9);
    assert_eq!(
        running_lines.get(&3).map(String::as_str),
        Some("running 9 tests")
    );
}

#[test]
fn operator_fixtures_parse() {
    let bindings: PoolBindings =
        serde_json::from_str(&fixture("pool_bindings.json")).expect("pool_bindings.json parses");
    assert_eq!(bindings.bindings.len(), 4);
    assert_eq!(bindings.observed_at, Some(anchor()));
    let sandbox = bindings
        .bindings
        .iter()
        .find(|binding| binding.job_id == "test (sandbox)")
        .expect("sandbox binding");
    assert_eq!(sandbox.state, "running");
    assert_eq!(sandbox.runner_id, Some(4));
    assert_eq!(sandbox.runner_name.as_deref(), Some("plr-cpane-4"));
    assert_eq!(sandbox.since_seconds, Some(840));
    assert_eq!(sandbox.waitlist_position, None);
    let waiting = bindings
        .bindings
        .iter()
        .find(|binding| binding.job_id == "e2e")
        .expect("e2e binding");
    assert_eq!(waiting.state, "ready");
    assert_eq!(waiting.runner_id, None);
    assert_eq!(waiting.waitlist_position, Some(1));
    let provisioning = bindings
        .bindings
        .iter()
        .find(|binding| binding.job_id == "dogfood")
        .expect("dogfood binding");
    assert_eq!(provisioning.state, "provisioning");
    assert_eq!(provisioning.waitlist_position, Some(2));

    let runners: RunnersList =
        serde_json::from_str(&fixture("runners.json")).expect("runners.json parses");
    assert_eq!(runners.count, 5);
    assert_eq!(runners.runners.len(), 5);
    assert_eq!(runners.runners[0].id, 1);
    assert_eq!(runners.runners[0].name, "plr-cpane-1");
    assert_eq!(runners.runners[4].name, "plr-main-1");
    assert_eq!(runners.runners[4].labels.len(), 3);
    assert_eq!(runners.queued, Some(2));
    assert_eq!(runners.claimable, Some(0));

    let deliveries: WebhookDeliveries = serde_json::from_str(&fixture("webhook_deliveries.json"))
        .expect("webhook_deliveries.json parses");
    assert_eq!(deliveries.deliveries.len(), 7);
    assert_eq!(deliveries.stats.received, 1284);
    assert_eq!(deliveries.stats.done, 1280);
    assert_eq!(deliveries.stats.dead_letters, 2);
    assert_eq!(deliveries.stats.oldest_pending_at, None);
    let dead: Vec<&str> = deliveries
        .deliveries
        .iter()
        .filter(|delivery| delivery.state == "failed")
        .map(|delivery| delivery.delivery_id.as_str())
        .collect();
    assert_eq!(dead, vec!["8c1f0a2e-e02", "7a90c3b1-11b"]);
    assert_eq!(deliveries.deliveries[0].attempts, 5);
    assert_eq!(
        deliveries.deliveries[0].last_error.as_deref(),
        Some("github 502 creating check run")
    );
    assert_eq!(deliveries.deliveries[2].state, "done");
    assert_eq!(deliveries.deliveries[2].event, "push");
    assert_eq!(deliveries.deliveries[4].attempts, 2);

    let health: WebhookHealth =
        serde_json::from_str(&fixture("webhook_health.json")).expect("webhook_health.json parses");
    let stats = health.queue_stats().expect("queue stats are present");
    assert_eq!(stats.dead_letters, 2);
    assert_eq!(stats.received, 1284);
    assert!(health.watchdog.enabled);
    assert_eq!(health.watchdog.last_success_age_seconds, Some(18.0));
    assert_eq!(health.watchdog.open_repairs, 0);
    assert_eq!(health.github_breaker.len(), 1);
    assert_eq!(health.github_breaker[0].bucket, "core");
    assert!(!health.github_breaker[0].open);
    assert!(!health.github_breaker[0].rate_limited);
    assert_eq!(health.apps.len(), 1);
    assert!(health.apps[0].healthy);
    assert!(!health.apps[0].url_drifted);
    assert_eq!(
        health.apps[0].hook_url.as_deref(),
        Some("https://preloop.example/api/v1/github/webhooks")
    );

    let sessions: DebugSessions =
        serde_json::from_str(&fixture("debug_sessions.json")).expect("debug_sessions.json parses");
    assert_eq!(sessions.sessions.len(), 3);
    let paused = sessions
        .sessions
        .iter()
        .find(|session| session.session_id == "ds-7c1e")
        .expect("ds-7c1e");
    assert_eq!(paused.state, "paused");
    assert_eq!(paused.version, 7);
    assert_eq!(paused.step.index, 3);
    assert_eq!(paused.step.total, 6);
    assert_eq!(paused.step.display_name, "cargo test");
    assert_eq!(paused.step.exit_code, Some(101));
    assert_eq!(paused.machine.as_deref(), Some("plr-cpane-4"));
    assert_eq!(paused.controller, None);
    assert_eq!(paused.paused_seconds, 846);
    let attached = sessions
        .sessions
        .iter()
        .find(|session| session.session_id == "ds-21aa")
        .expect("ds-21aa");
    assert_eq!(attached.controller.as_deref(), Some("agent:omp"));
    assert_eq!(attached.step.command, None);
    assert_eq!(sessions.sessions[2].state, "abandoned");

    let secrets: SecretList =
        serde_json::from_str(&fixture("secrets.json")).expect("secrets.json parses");
    assert_eq!(secrets.secrets.len(), 2);
    assert_eq!(secrets.secrets[0].name, "GH_TOKEN");
    assert_eq!(secrets.secrets[0].repo, None);
    assert_eq!(secrets.secrets[1].name, "DEPLOY_KEY");
    assert_eq!(
        secrets.secrets[1].repo.as_deref(),
        Some("preloopdev/preloop")
    );
}

#[test]
fn feed_lines_parse_and_the_ndjson_decoder_is_chunk_agnostic() {
    let text = fixture("feed_8f3a.ndjson");
    let lines: Vec<String> = text.lines().map(str::to_owned).collect();
    assert_eq!(lines.len(), 8);

    let events: Vec<FeedEvent> = lines
        .iter()
        .map(|line| parse_feed_line(line).expect("feed line parses"))
        .collect();

    assert_eq!(
        events[0],
        FeedEvent::RunStatus {
            run_id: RUN_ID.to_owned(),
            status: ExecStatus::InProgress,
        }
    );
    assert_eq!(
        events[1],
        FeedEvent::JobStatus {
            run_id: RUN_ID.to_owned(),
            job_id: "test (sandbox)".to_owned(),
            status: ExecStatus::InProgress,
        }
    );

    match &events[2] {
        FeedEvent::StepStatus(step) => {
            assert_eq!(step.run_id, RUN_ID);
            assert_eq!(step.job_id, "test (sandbox)");
            assert_eq!(step.step_id, CARGO_TEST_STEP_ID);
            assert_eq!(step.name, "cargo test");
            assert_eq!(step.conclusion, "in_progress");
            assert_eq!(step.workflow_index, Some(2));
            assert_eq!(step.runner_number, Some(4));
            assert_eq!(
                step.started_at.map(|started| started.timestamp_micros()),
                Some(CARGO_TEST_STARTED_AT_US)
            );
            assert_eq!(step.finished_at, None);
        }
        other => panic!("expected StepStatus, got {other:?}"),
    }

    match &events[3] {
        FeedEvent::StepStatus(step) => {
            assert_eq!(step.conclusion, "failure");
            assert_eq!(
                step.finished_at.map(|finished| finished.timestamp_micros()),
                Some(CARGO_TEST_FINISHED_AT_US)
            );
            assert_eq!(
                step.finished_at
                    .zip(step.started_at)
                    .map(|(end, start)| end - start),
                Some(TimeDelta::seconds(261))
            );
        }
        other => panic!("expected StepStatus, got {other:?}"),
    }

    match &events[4] {
        FeedEvent::Annotation {
            run_id,
            job_id,
            annotation,
        } => {
            assert_eq!(run_id, RUN_ID);
            assert_eq!(job_id, "test (sandbox)");
            assert_eq!(annotation.level, "error");
            assert_eq!(annotation.message, "orphan reported as Failed");
            assert_eq!(annotation.file.as_deref(), Some("service.rs"));
            assert_eq!(annotation.line, Some(812));
            assert_eq!(annotation.title, None);
            assert_eq!(annotation.step_id.as_deref(), Some(CARGO_TEST_STEP_ID));
        }
        other => panic!("expected Annotation, got {other:?}"),
    }

    assert_eq!(
        events[5],
        FeedEvent::JobCompleted {
            run_id: RUN_ID.to_owned(),
            job_id: "test (sandbox)".to_owned(),
            status: ExecStatus::Failure,
        }
    );
    match &events[6] {
        FeedEvent::Other(kind) => assert_eq!(kind.as_str(), "log"),
        other => panic!("expected Other, got {other:?}"),
    }
    match &events[7] {
        FeedEvent::Other(kind) => assert_eq!(kind.as_str(), "future_event"),
        other => panic!("expected Other, got {other:?}"),
    }
    assert_eq!(events[6].run_id(), None);

    let mut whole = NdjsonDecoder::default();
    let mut whole_lines = whole.push(text.as_bytes());
    whole_lines.extend(whole.finish());
    assert_eq!(whole_lines, lines);

    let mut bytewise = NdjsonDecoder::default();
    let mut bytewise_lines: Vec<String> = Vec::new();
    for byte in text.as_bytes() {
        bytewise_lines.extend(bytewise.push(std::slice::from_ref(byte)));
    }
    bytewise_lines.extend(bytewise.finish());
    assert_eq!(bytewise_lines, lines);
    for line in &bytewise_lines {
        parse_feed_line(line).expect("bytewise feed line parses");
    }
}

#[test]
fn sse_live_log_decodes_and_ignores_comments_and_pings() {
    let text = fixture("sse_live_log.sse.txt");

    let mut whole = SseDecoder::default();
    let whole_events: Vec<SseEvent> = whole.push(text.as_bytes());
    assert_eq!(whole_events.len(), 4, "comment lines produce no event");
    assert_eq!(
        whole_events
            .iter()
            .filter(|event| event.event == "live-log")
            .count(),
        3
    );
    let ping = whole_events
        .iter()
        .find(|event| event.event == "ping")
        .expect("ping frame");
    assert_eq!(ping.data, "{}");

    let chunks: Vec<LiveLogChunk> = whole_events
        .iter()
        .filter(|event| event.event == "live-log")
        .map(|event| serde_json::from_str(&event.data).expect("live-log data parses"))
        .collect();
    assert_eq!(
        chunks
            .iter()
            .map(|chunk| chunk.start_line)
            .collect::<Vec<u64>>(),
        vec![1204, 1251, 1301]
    );
    assert!(
        chunks
            .iter()
            .all(|chunk| chunk.step_id == CARGO_TEST_STEP_ID)
    );
    assert!(chunks[0].value[0].starts_with("##[group]"));
    assert_eq!(
        chunks[2].value.last().map(String::as_str),
        Some("##[error]Process completed with exit code 101.")
    );

    let mut bytewise = SseDecoder::default();
    let mut bytewise_events: Vec<SseEvent> = Vec::new();
    for byte in text.as_bytes() {
        bytewise_events.extend(bytewise.push(std::slice::from_ref(byte)));
    }
    assert_eq!(bytewise_events, whole_events);
}

#[test]
fn unknown_fields_and_statuses_do_not_fail_parsing() {
    let run: RunRecord = serde_json::from_str(
        r#"{
            "run_id": "8f3a91c2-5d10-4b7e-9a10-2c4e7f1a9b01",
            "status": "quarantined",
            "conclusion": "quarantined",
            "jobs": {"fmt": "exploded", "e2e": "success"},
            "run_number": 412,
            "future_field": {"nested": [1, 2, 3]},
            "another_future_field": null
        }"#,
    )
    .expect("unknown fields and statuses are lenient");

    assert_eq!(run.status, ExecStatus::Unknown);
    assert_eq!(run.job_status("fmt"), ExecStatus::Unknown);
    assert_eq!(run.job_status("e2e"), ExecStatus::Success);
    assert_eq!(run.job_status("missing"), ExecStatus::Unknown);
    assert_eq!(run.run_number, 412);
    assert_eq!(run.submission.repository, "");

    let event = parse_feed_line(r#"{"type":"step_status","future":true}"#)
        .expect("a short step_status line still parses");
    match event {
        FeedEvent::StepStatus(step) => {
            assert_eq!(step.conclusion, "");
            assert_eq!(step.started_at, None);
        }
        other => panic!("expected StepStatus, got {other:?}"),
    }
}
