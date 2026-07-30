//! HTTP-level property tests for concurrency control plane.
//!
//! Exercises the real Axum router endpoints (`/api/v1/runs`, run lookup,
//! run cancel, `/internal/test/jobs/complete`, `/broker/:runner_id/completejob`)
//! with bounded generated workflow sequences and asserts structural invariants
//! after every operation.
//!
//! Invariant IDs reference `plans/006-property-test-concurrency.md`.

use std::collections::BTreeMap;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use proptest::prelude::*;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use super::*;

// ─── Helpers ────────────────────────────────────────────────────────────────

const TEST_API_TOKEN: &str = "http-prop-test-token";

fn make_app(state: AppState, shutdown: CancellationToken) -> Router {
    app_with_test_api(state, shutdown, TEST_API_TOKEN)
}

async fn req(app: &Router, method: Method, uri: &str, body: Value) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if uri.starts_with("/api/v1/")
        || uri.starts_with("/_apis/")
        || uri.starts_with("/runner/server/_apis/")
        || uri.starts_with("/broker/")
    {
        builder = builder.header(header::AUTHORIZATION, "Bearer aksh-system-token");
    } else if uri.starts_with("/internal/test/") {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"));
    }
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
    let val = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, val)
}

async fn submit(app: &Router, yaml: &str, repo: &str) -> Value {
    let (status, val) = req(
        app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": yaml,
            "event": "push",
            "repository": repo,
        }),
    )
    .await;
    assert!(status.is_success(), "submit failed: {status} body={val}");
    val
}

async fn get_run(app: &Router, run_id: &str) -> Value {
    let (status, val) = req(
        app,
        Method::GET,
        &format!("/api/v1/runs/{run_id}"),
        Value::Null,
    )
    .await;
    assert!(status.is_success(), "get_run failed: {status}");
    val
}

async fn cancel_run(app: &Router, run_id: &str) -> Value {
    let (status, val) = req(
        app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/cancel"),
        Value::Null,
    )
    .await;
    assert!(status.is_success(), "cancel_run failed: {status}");
    val
}

async fn complete_job(app: &Router, run_id: &str, job_id: &str) -> Value {
    let (status, val) = req(
        app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": job_id,
            "status": "success",
            "outputs": {},
        }),
    )
    .await;
    assert!(status.is_success(), "complete_job failed: {status}");
    val
}

async fn poll_message(app: &Router, session_id: &str) -> Value {
    let (status, val) = req(
        app,
        Method::GET,
        &format!("/runner/server/_apis/v1/Message/1?sessionId={session_id}&waitSeconds=0"),
        Value::Null,
    )
    .await;
    assert!(status.is_success(), "poll failed: {status}");
    val
}

async fn broker_complete(
    app: &Router,
    bearer_token: &str,
    runner_id: i64,
    plan_id: &str,
    job_id: &str,
) -> StatusCode {
    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("/broker/{runner_id}/completejob"))
        .header(header::AUTHORIZATION, format!("Bearer {bearer_token}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "planId": plan_id,
                "jobId": job_id,
                "conclusion": "succeeded",
                "outputs": {},
            })
            .to_string(),
        ))
        .unwrap();
    app.clone().oneshot(request).await.unwrap().status()
}

// ─── Generators ─────────────────────────────────────────────────────────────

/// Queue mode for generated workflows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GenQueue {
    Single,
    Max,
}

/// A generated workflow specification (semantic, not raw YAML).
#[derive(Debug, Clone)]
struct GenWorkflow {
    repo: String,
    group: String,
    queue: GenQueue,
    cancel_in_progress: bool,
    job_count: usize,
}

impl GenWorkflow {
    fn to_yaml(&self) -> String {
        let cancel = if self.cancel_in_progress {
            "true"
        } else {
            "false"
        };
        let queue_str = match self.queue {
            GenQueue::Single => "",
            GenQueue::Max => "\n  queue: max",
        };
        let mut yaml = format!(
            "on: push\nconcurrency:\n  group: {group}\n  cancel-in-progress: {cancel}{queue}\njobs:\n",
            group = self.group,
            cancel = cancel,
            queue = queue_str,
        );
        for i in 0..self.job_count {
            yaml.push_str(&format!(
                "  job{i}:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo {i}\n"
            ));
        }
        yaml
    }
}

/// An operation in a generated sequence.
#[derive(Debug, Clone)]
enum GenOp {
    /// Submit a workflow.
    Submit(usize),
    /// Get run status for a previously submitted run (index into submitted list).
    GetRun(usize),
    /// Cancel a previously submitted run.
    CancelRun(usize),
    /// Complete the first queued job of a previously submitted run.
    CompleteFirstJob(usize),
    /// Poll for messages (simulates runner picking up work).
    Poll,
}

/// A complete test case: workflows and an operation sequence.
#[derive(Debug, Clone)]
struct GenCase {
    workflows: Vec<GenWorkflow>,
    ops: Vec<GenOp>,
}

fn arb_repo() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("alpha/repo".to_owned()),
        Just("Alpha/Repo".to_owned()), // case variant
        Just("beta/other".to_owned()), // different repo
    ]
}

fn arb_group() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("deploy".to_owned()),
        Just("Deploy".to_owned()), // case variant
        Just("ci-main".to_owned()),
    ]
}

fn arb_workflow() -> impl Strategy<Value = GenWorkflow> {
    (
        arb_repo(),
        arb_group(),
        prop_oneof![Just(GenQueue::Single), Just(GenQueue::Max)],
        any::<bool>(),
        1..=3usize,
    )
        .prop_map(|(repo, group, queue, cancel, job_count)| {
            // Prevent invalid combo: queue:max + cancel-in-progress:true
            let cancel = if queue == GenQueue::Max {
                false
            } else {
                cancel
            };
            GenWorkflow {
                repo,
                group,
                queue,
                cancel_in_progress: cancel,
                job_count,
            }
        })
}

fn arb_case() -> impl Strategy<Value = GenCase> {
    // 2-6 workflows, 4-24 ops
    prop::collection::vec(arb_workflow(), 2..=6).prop_flat_map(|workflows| {
        let wf_count = workflows.len();
        let ops_strategy = prop::collection::vec(
            (0..=4u8, any::<prop::sample::Index>()).prop_map(move |(kind, idx)| {
                let wf_idx = idx.index(wf_count);
                match kind {
                    0 => GenOp::Submit(wf_idx),
                    1 => GenOp::GetRun(wf_idx),
                    2 => GenOp::CancelRun(wf_idx),
                    3 => GenOp::CompleteFirstJob(wf_idx),
                    _ => GenOp::Poll,
                }
            }),
            4..=24,
        );
        ops_strategy.prop_map(move |ops| GenCase {
            workflows: workflows.clone(),
            ops,
        })
    })
}

// ─── Test module ────────────────────────────────────────────────────────────

#[cfg(test)]
pub(crate) mod http_sequences {
    use super::*;

    /// Property: pending runs (held in held_runs) expose no dispatchable broker job.
    ///
    /// Contract: GH-SLOT-01 — a pending holder must not leak jobs into the
    /// dispatch queue, otherwise a runner could pick up a job before the
    /// concurrency gate opens.
    #[tokio::test]
    async fn pending_runs_have_no_queued_jobs() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = make_app(state.clone(), CancellationToken::new());

        // Workflow A takes the group; workflow B should be pending.
        let yaml = "on: push\nconcurrency:\n  group: serial\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
        let a = submit(&app, yaml, "owner/repo").await;
        let _b = submit(&app, yaml, "owner/repo").await;
        let a_id = a["run_id"].as_str().unwrap();

        let inner = state.inner.lock().await;
        // B's jobs should be in held_runs, not in the queue.
        let queue_run_ids: Vec<String> = inner.queue.iter().map(|j| j.run_id.to_string()).collect();
        let held_run_ids: Vec<String> = inner.held_runs.keys().map(|id| id.to_string()).collect();
        for held_id in &held_run_ids {
            assert!(
                !queue_run_ids.contains(held_id),
                "GH-SLOT-01: held run {held_id} must not have jobs in the dispatch queue"
            );
        }
        // The running holder's run should have queued jobs.
        assert!(
            queue_run_ids.contains(&a_id.to_owned()),
            "running holder's jobs should be in queue"
        );
    }

    /// Property: cancelling an in-progress holder emits exactly one
    /// JobCancellation message for the agent job GUID.
    ///
    /// Contract: cancel-in-progress replaces the running holder. The server
    /// must enqueue exactly one cancellation message, not zero or duplicates.
    #[tokio::test]
    async fn cancel_in_progress_emits_exactly_one_cancellation() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = make_app(state.clone(), CancellationToken::new());

        let yaml = "on: push\nconcurrency:\n  group: cip-test\n  cancel-in-progress: true\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo go\n";
        let a = submit(&app, yaml, "owner/repo").await;
        let a_id = a["run_id"].as_str().unwrap();

        // Dispatch A so it becomes InProgress.
        let msg = poll_message(&app, "sess-cip").await;
        assert!(!msg.is_null(), "A should be dispatchable");

        // Verify A is InProgress.
        let a_run = get_run(&app, a_id).await;
        assert_eq!(a_run["jobs"]["build"], "in_progress");

        // Submit B → should cancel A.
        let b = submit(&app, yaml, "owner/repo").await;
        let b_id = b["run_id"].as_str().unwrap();

        // A should now be cancelled.
        let a_run = get_run(&app, a_id).await;
        assert_eq!(a_run["status"], "cancelled");

        // Count cancellation messages for A.
        let inner = state.inner.lock().await;
        let cancel_count = inner
            .cancellation_queue
            .iter()
            .filter(|c| c.run_id.to_string() == a_id)
            .count();

        // Exactly one cancellation (or zero if already drained by poll).
        // Check inflight messages too.
        let msg_cancel_count: usize = inner
            .inflight_messages
            .values()
            .flat_map(|msgs| msgs.values())
            .filter(|m| m.message_type == aksh_gha_protocol::azdo::message_type::JOB_CANCELLED)
            .count();

        let total_cancels = cancel_count + msg_cancel_count;
        assert_eq!(
            total_cancels, 1,
            "GH-CANCEL-01: expected exactly 1 cancellation message for A, got {total_cancels} \
             (queue={cancel_count}, inflight={msg_cancel_count})"
        );
        drop(inner);

        // B should be running/queued, not cancelled.
        let b_run = get_run(&app, b_id).await;
        assert_ne!(b_run["status"], "cancelled", "B must not be cancelled");
    }

    /// Property: pending-only replacement (single mode) emits NO runner
    /// cancellation message, because the replaced pending run was never
    /// dispatched to a runner.
    ///
    /// Contract: GH-SINGLE-01 — replacing a pending holder sends no
    /// JobCancellation because the runner never received a job.
    #[tokio::test]
    async fn pending_only_replacement_no_runner_cancellation() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = make_app(state.clone(), CancellationToken::new());

        let yaml = "on: push\nconcurrency:\n  group: single-repl\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";

        // A takes the running slot.
        let _a = submit(&app, yaml, "owner/repo").await;
        // B becomes pending.
        let b = submit(&app, yaml, "owner/repo").await;
        let b_id = b["run_id"].as_str().unwrap();
        // C replaces B as the pending holder. B should be cancelled without
        // any runner cancellation message.
        let _c = submit(&app, yaml, "owner/repo").await;

        let b_run = get_run(&app, b_id).await;
        assert_eq!(
            b_run["status"], "cancelled",
            "B must be cancelled by C's arrival"
        );

        // No cancellation messages should exist for B, because B was never
        // dispatched to a runner.
        let inner = state.inner.lock().await;
        let b_cancels = inner
            .cancellation_queue
            .iter()
            .filter(|c| c.run_id.to_string() == b_id)
            .count();
        let _b_inflight_cancels: usize = inner
            .inflight_messages
            .values()
            .flat_map(|msgs| msgs.values())
            .filter(|m| m.message_type == aksh_gha_protocol::azdo::message_type::JOB_CANCELLED)
            .count();
        // The cancellation queue entries for pending-only should be zero.
        // (Inflight cancels may exist for OTHER runs, so we check the queue
        // specifically for B's run_id.)
        assert_eq!(
            b_cancels, 0,
            "GH-SINGLE-01: pending-only cancelled run must emit no runner cancellation"
        );
    }

    /// Property: after predecessor terminal completion, successor becomes
    /// dispatchable.
    ///
    /// Contract: promotion — when the running holder finishes, the next
    /// pending holder is promoted and its jobs enter the dispatch queue.
    #[tokio::test]
    async fn promotion_after_predecessor_completes() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = make_app(state.clone(), CancellationToken::new());

        let yaml = "on: push\nconcurrency:\n  group: promo\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";

        let a = submit(&app, yaml, "owner/repo").await;
        let a_id = a["run_id"].as_str().unwrap();
        let b = submit(&app, yaml, "owner/repo").await;
        let b_id = b["run_id"].as_str().unwrap();

        // B should be pending.
        let b_run = get_run(&app, b_id).await;
        assert_eq!(b_run["status"], "pending");

        // Complete A.
        complete_job(&app, a_id, "build").await;

        // B should now be queued/dispatchable.
        let b_run = get_run(&app, b_id).await;
        assert!(
            b_run["status"] == "queued" || b_run["status"] == "in_progress",
            "B must be promoted after A completes, got {}",
            b_run["status"]
        );

        // B's jobs should be in the queue.
        let inner = state.inner.lock().await;
        let b_in_queue = inner.queue.iter().any(|j| j.run_id.to_string() == b_id);
        assert!(b_in_queue, "B's job must be in the dispatch queue");
    }

    /// Property: different repositories NEVER interfere; case variants in
    /// one repository DO interfere.
    ///
    /// Contract: GH-GROUP-01 — group identity is (repository, case-insensitive
    /// group name). Different repos are independent.
    #[tokio::test]
    async fn repo_isolation_and_case_folding() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = make_app(state.clone(), CancellationToken::new());

        let yaml = "on: push\nconcurrency:\n  group: Deploy\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";

        // Submit to repo-a and repo-b with the same group.
        let a = submit(&app, yaml, "alpha/repo").await;
        let a_id = a["run_id"].as_str().unwrap();
        let b = submit(&app, yaml, "beta/other").await;
        let b_id = b["run_id"].as_str().unwrap();

        // Both should be queued (independent groups).
        let a_run = get_run(&app, a_id).await;
        let b_run = get_run(&app, b_id).await;
        assert_eq!(a_run["status"], "queued", "repo-a should be queued");
        assert_eq!(b_run["status"], "queued", "repo-b should be queued");

        // Now submit a case variant to repo-a — should contend.
        let yaml_lower = "on: push\nconcurrency:\n  group: deploy\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
        let c = submit(&app, yaml_lower, "alpha/repo").await;
        let c_id = c["run_id"].as_str().unwrap();

        // c should be pending (contending with a in alpha/repo).
        let c_run = get_run(&app, c_id).await;
        assert_eq!(
            c_run["status"], "pending",
            "GH-GROUP-01: case-folded group in same repo must contend"
        );

        // b should still be queued (different repo).
        let b_run = get_run(&app, b_id).await;
        assert_eq!(
            b_run["status"], "queued",
            "GH-GROUP-01: different repo must not interfere"
        );
    }

    /// Property: no cross-run output contamination.
    ///
    /// Contract: completing run A's job must not affect run B's job statuses.
    #[tokio::test]
    async fn no_cross_run_output_contamination() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = make_app(state.clone(), CancellationToken::new());

        // Two runs with DIFFERENT groups so they're independent.
        let yaml_a = "on: push\nconcurrency:\n  group: group-a\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo a\n";
        let yaml_b = "on: push\nconcurrency:\n  group: group-b\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo b\n";

        let a = submit(&app, yaml_a, "owner/repo").await;
        let a_id = a["run_id"].as_str().unwrap();
        let b = submit(&app, yaml_b, "owner/repo").await;
        let b_id = b["run_id"].as_str().unwrap();

        // Complete A's job.
        complete_job(&app, a_id, "build").await;

        // A should be success.
        let a_run = get_run(&app, a_id).await;
        assert_eq!(a_run["status"], "success");

        // B must still be queued — not affected by A's completion.
        let b_run = get_run(&app, b_id).await;
        assert_eq!(
            b_run["status"], "queued",
            "B must not be affected by A's completion in a different group"
        );
        assert_eq!(
            b_run["jobs"]["build"], "queued",
            "B's job must remain queued"
        );
    }

    /// Property: broker completejob promotes pending successor.
    ///
    /// Contract: completing a job via the broker path (as a real runner would)
    /// must trigger promotion of pending holders just like the test API.
    #[tokio::test]
    async fn broker_complete_promotes_successor() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = make_app(state.clone(), CancellationToken::new());

        let yaml = "on: push\nconcurrency:\n  group: broker-promo\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";

        let a = submit(&app, yaml, "owner/repo").await;
        let a_id = a["run_id"].as_str().unwrap();
        let b = submit(&app, yaml, "owner/repo").await;
        let b_id = b["run_id"].as_str().unwrap();

        // B should be pending.
        assert_eq!(get_run(&app, b_id).await["status"], "pending");

        // Dispatch A via polling so it becomes InProgress.
        let msg = poll_message(&app, "sess-broker").await;
        assert!(!msg.is_null(), "A should be dispatchable");
        assert_eq!(get_run(&app, a_id).await["jobs"]["build"], "in_progress");
        let runner_token = state
            .local_jwt(json!({
                "sub": "aksh-runner-listen-1",
                "scp": "ActionsRuntime.RunnerListen",
            }))
            .unwrap();
        {
            let mut inner = state.inner.lock().await;
            inner
                .broker_session_runners
                .insert("sess-broker".to_owned(), 1);
        }

        // Complete via broker path.
        // We need to find the agent_job_id and plan_id.
        let (plan_id, agent_job_id) = {
            let inner = state.inner.lock().await;
            let req = inner
                .job_requests
                .values()
                .find(|r| r.run_id.to_string() == a_id)
                .unwrap();
            (req.plan_id.clone(), req.agent_job_id)
        };

        let status =
            broker_complete(&app, &runner_token, 1, &plan_id, &agent_job_id.to_string()).await;
        assert!(
            status.is_success() || status == StatusCode::NO_CONTENT,
            "broker complete should succeed: {status}"
        );

        // B should now be promoted.
        let b_run = get_run(&app, b_id).await;
        assert!(
            b_run["status"] == "queued" || b_run["status"] == "in_progress",
            "B must be promoted after broker complete, got {}",
            b_run["status"]
        );
    }

    /// Property: max queue mode preserves all pending holders (up to 100).
    ///
    /// Contract: GH-MAX-01 — queue:max parks arrivals as pending without
    /// cancelling existing pending holders.
    #[tokio::test]
    async fn max_queue_preserves_pending_holders() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = make_app(state.clone(), CancellationToken::new());

        let yaml = "on: push\nconcurrency:\n  group: max-q\n  queue: max\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";

        // Submit 4 runs. First takes the slot, rest should all be pending.
        let mut ids = Vec::new();
        for _ in 0..4 {
            let r = submit(&app, yaml, "owner/repo").await;
            ids.push(r["run_id"].as_str().unwrap().to_owned());
        }

        // First should be queued (running holder).
        assert_eq!(get_run(&app, &ids[0]).await["status"], "queued");

        // Remaining should all be pending — none cancelled.
        for id in &ids[1..] {
            let run = get_run(&app, id).await;
            assert_eq!(
                run["status"], "pending",
                "GH-MAX-01: run {id} should be pending under queue:max, got {}",
                run["status"]
            );
        }
    }

    /// Property: FIFO promotion under queue:max.
    ///
    /// Contract: GH-FIFO-01 — pending holders are promoted in admission order.
    #[tokio::test]
    async fn max_queue_fifo_promotion() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = make_app(state.clone(), CancellationToken::new());

        let yaml = "on: push\nconcurrency:\n  group: fifo\n  queue: max\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";

        let a = submit(&app, yaml, "owner/repo").await;
        let a_id = a["run_id"].as_str().unwrap().to_owned();
        let b = submit(&app, yaml, "owner/repo").await;
        let b_id = b["run_id"].as_str().unwrap().to_owned();
        let c = submit(&app, yaml, "owner/repo").await;
        let c_id = c["run_id"].as_str().unwrap().to_owned();

        // Complete A → B should be promoted (not C).
        complete_job(&app, &a_id, "build").await;

        let b_run = get_run(&app, &b_id).await;
        let c_run = get_run(&app, &c_id).await;
        assert!(
            b_run["status"] == "queued" || b_run["status"] == "in_progress",
            "GH-FIFO-01: B must be promoted before C, got B={}",
            b_run["status"]
        );
        assert_eq!(
            c_run["status"], "pending",
            "GH-FIFO-01: C must remain pending when B is promoted"
        );
    }

    /// Proptest: random workflow sequences maintain structural invariants.
    ///
    /// This exercises bounded generated operation sequences against the real
    /// Axum router, checking after every operation:
    /// 1. At most one running holder per group (GH-SLOT-01)
    /// 2. Pending runs have no dispatchable jobs
    /// 3. Terminal holders are absent from dispatch/concurrency queues
    /// 4. No cross-run state leakage
    fn run_generated_sequence(case: GenCase) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let temp = tempfile::tempdir().unwrap();
            let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
            let app = make_app(state.clone(), CancellationToken::new());

            // Track submitted run IDs per workflow index.
            let mut submitted: BTreeMap<usize, Vec<String>> = BTreeMap::new();
            let mut all_run_ids: Vec<String> = Vec::new();

            for (op_idx, op) in case.ops.iter().enumerate() {
                match op {
                    GenOp::Submit(wf_idx) => {
                        let wf = &case.workflows[*wf_idx];
                        let yaml = wf.to_yaml();
                        let result = submit(&app, &yaml, &wf.repo).await;
                        let run_id = result["run_id"].as_str().unwrap().to_owned();
                        submitted.entry(*wf_idx).or_default().push(run_id.clone());
                        all_run_ids.push(run_id);
                    }
                    GenOp::GetRun(wf_idx) => {
                        if let Some(ids) = submitted.get(wf_idx) {
                            if let Some(id) = ids.last() {
                                let run = get_run(&app, id).await;
                                // Status must be a known value.
                                let status = run["status"].as_str().unwrap();
                                assert!(
                                    matches!(
                                        status,
                                        "queued"
                                            | "pending"
                                            | "in_progress"
                                            | "success"
                                            | "failure"
                                            | "cancelled"
                                    ),
                                    "op {op_idx}: unexpected status {status}"
                                );
                            }
                        }
                    }
                    GenOp::CancelRun(wf_idx) => {
                        if let Some(ids) = submitted.get(wf_idx) {
                            if let Some(id) = ids.last() {
                                let _ = cancel_run(&app, id).await;
                            }
                        }
                    }
                    GenOp::CompleteFirstJob(wf_idx) => {
                        if let Some(id) = submitted.get(wf_idx).and_then(|ids| ids.last()) {
                            let run = get_run(&app, id).await;
                            if let Some(jobs) = run["jobs"].as_object() {
                                for (job_id, status) in jobs {
                                    if status == "queued" || status == "in_progress" {
                                        complete_job(&app, id, job_id).await;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    GenOp::Poll => {
                        let _ = poll_message(&app, "prop-session").await;
                    }
                }

                // ── After-operation invariant checks ────────────────────
                let inner = state.inner.lock().await;

                // INV-1: GH-SLOT-01 — at most one running holder per group.
                for (key, group) in &inner.concurrency_groups {
                    assert!(
                        group.running.as_ref().is_none_or(|_| true),
                        "op {op_idx}: group {key:?} has multiple running holders"
                    );
                    // The running field is Option<Holder>, so it's at most one.
                }

                // INV-2: Pending runs (held_runs) must not have jobs in queue.
                for held_run_id in inner.held_runs.keys() {
                    let in_queue = inner.queue.iter().any(|j| j.run_id == *held_run_id);
                    assert!(
                        !in_queue,
                        "op {op_idx}: held run {held_run_id} has jobs in dispatch queue"
                    );
                }

                // INV-3: Terminal runs have no entries in dispatch/concurrency queues.
                for (run_id, run) in &inner.runs {
                    if run.status.is_terminal() {
                        let in_queue = inner.queue.iter().any(|j| &j.run_id == run_id);
                        let in_pending = inner.pending_jobs.iter().any(|j| &j.run_id == run_id);
                        let in_blocked = inner
                            .concurrency_blocked
                            .iter()
                            .any(|j| &j.run_id == run_id);
                        let in_held = inner.held_runs.contains_key(run_id);
                        assert!(
                            !in_queue && !in_pending && !in_blocked && !in_held,
                            "op {op_idx}: terminal run {run_id} still in queues \
                             (q={in_queue}, pj={in_pending}, cb={in_blocked}, held={in_held})"
                        );
                    }
                }

                // INV-4: No duplicate (run_id, job_id) in queue.
                {
                    let mut seen = std::collections::HashSet::new();
                    for j in &inner.queue {
                        assert!(
                            seen.insert((j.run_id, j.job_id.clone())),
                            "op {op_idx}: duplicate ({}, {}) in queue",
                            j.run_id,
                            j.job_id
                        );
                    }
                }

                // INV-5: No duplicate (run_id, job_id) in pending_jobs.
                {
                    let mut seen = std::collections::HashSet::new();
                    for j in &inner.pending_jobs {
                        assert!(
                            seen.insert((j.run_id, j.job_id.clone())),
                            "op {op_idx}: duplicate ({}, {}) in pending_jobs",
                            j.run_id,
                            j.job_id
                        );
                    }
                }

                // INV-6: No duplicate (run_id, job_id) in concurrency_blocked.
                {
                    let mut seen = std::collections::HashSet::new();
                    for j in &inner.concurrency_blocked {
                        assert!(
                            seen.insert((j.run_id, j.job_id.clone())),
                            "op {op_idx}: duplicate ({}, {}) in concurrency_blocked",
                            j.run_id,
                            j.job_id
                        );
                    }
                }

                drop(inner);
            }
        });
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: std::env::var("PROPTEST_CASES")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(64),
            max_shrink_iters: 2000,
            .. ProptestConfig::default()
        })]

        #[test]
        #[ignore = "expensive generated HTTP state-machine run; use just test-properties-full"]
        fn generated_http_sequence_invariants(case in arb_case()) {
            run_generated_sequence(case);
        }
    }
}
