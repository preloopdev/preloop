//! Golden tests for stable protocol JSON shapes.

use preloop_gha_protocol::{ExecutionStatus, JobId, NdjsonEvent, RunId, event_to_ndjson};
use uuid::Uuid;

#[test]
fn ndjson_event_shape_is_stable() {
    let run_id = RunId(Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap());
    let line = event_to_ndjson(&NdjsonEvent::JobStatus {
        run_id,
        job_id: JobId("test[os=ubuntu-latest]".to_owned()),
        status: ExecutionStatus::Success,
        reason: None,
    })
    .unwrap();

    assert_eq!(
        line,
        "{\"type\":\"job_status\",\"run_id\":\"00000000-0000-0000-0000-000000000001\",\"job_id\":\"test[os=ubuntu-latest]\",\"status\":\"success\"}\n"
    );
}

#[test]
fn step_status_event_shape_is_stable() {
    let run_id = RunId(Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap());
    let event = NdjsonEvent::StepStatus {
        run_id,
        job_id: JobId("test[os=ubuntu-latest]".to_owned()),
        step_id: "8d0a2f6c-4a1e-4f63-9a3d-0b1f2c3d4e5f".to_owned(),
        workflow_index: Some(0),
        runner_number: Some(2),
        name: "cargo test".to_owned(),
        conclusion: "success".to_owned(),
        started_at_us: Some(1_761_090_000_000_000),
        finished_at_us: Some(1_761_090_012_500_000),
    };
    assert_eq!(
        event_to_ndjson(&event).unwrap(),
        "{\"type\":\"step_status\",\"run_id\":\"00000000-0000-0000-0000-000000000001\",\"job_id\":\"test[os=ubuntu-latest]\",\"step_id\":\"8d0a2f6c-4a1e-4f63-9a3d-0b1f2c3d4e5f\",\"workflow_index\":0,\"runner_number\":2,\"name\":\"cargo test\",\"conclusion\":\"success\",\"started_at_us\":1761090000000000,\"finished_at_us\":1761090012500000}\n"
    );

    // Optional fields are omitted, not nulled: a synthetic step has no
    // workflow index, and a pending one has no timings yet.
    let pending = NdjsonEvent::StepStatus {
        run_id,
        job_id: JobId("test".to_owned()),
        step_id: "step-1".to_owned(),
        workflow_index: None,
        runner_number: None,
        name: "Set up job".to_owned(),
        conclusion: "pending".to_owned(),
        started_at_us: None,
        finished_at_us: None,
    };
    assert_eq!(
        event_to_ndjson(&pending).unwrap(),
        "{\"type\":\"step_status\",\"run_id\":\"00000000-0000-0000-0000-000000000001\",\"job_id\":\"test\",\"step_id\":\"step-1\",\"name\":\"Set up job\",\"conclusion\":\"pending\"}\n"
    );
    // Round-trips, and carries its run identity.
    let parsed: NdjsonEvent =
        serde_json::from_str(event_to_ndjson(&pending).unwrap().trim()).unwrap();
    assert_eq!(parsed, pending);
    assert_eq!(parsed.run_id(), run_id);
}

#[test]
fn workflow_submission_accepts_local_workspace_input_without_serializing_host_path() {
    let trusted_input = serde_json::json!({
        "workflow_yaml": "name: CI\njobs: {}",
        "event": "push",
        "repository": "local/example",
        "local_workspace": "/Users/alice/src/example",
    });
    let submission: preloop_gha_protocol::WorkflowSubmission =
        serde_json::from_value(trusted_input).expect("trusted local submission should deserialize");

    assert_eq!(
        submission.local_workspace.as_deref(),
        Some("/Users/alice/src/example")
    );

    let serialized = serde_json::to_value(submission).expect("submission should serialize");
    assert!(serialized.get("local_workspace").is_none());
    assert!(!serialized.to_string().contains("/Users/alice/src/example"));
}
