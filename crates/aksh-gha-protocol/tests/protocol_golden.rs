//! Golden tests for stable protocol JSON shapes.

use aksh_gha_protocol::{event_to_ndjson, ExecutionStatus, JobId, NdjsonEvent, RunId};
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
fn workflow_submission_accepts_local_workspace_input_without_serializing_host_path() {
    let trusted_input = serde_json::json!({
        "workflow_yaml": "name: CI\njobs: {}",
        "event": "push",
        "repository": "local/example",
        "local_workspace": "/Users/alice/src/example",
    });
    let submission: aksh_gha_protocol::WorkflowSubmission =
        serde_json::from_value(trusted_input).expect("trusted local submission should deserialize");

    assert_eq!(
        submission.local_workspace.as_deref(),
        Some("/Users/alice/src/example")
    );

    let serialized = serde_json::to_value(submission).expect("submission should serialize");
    assert!(serialized.get("local_workspace").is_none());
    assert!(!serialized.to_string().contains("/Users/alice/src/example"));
}
