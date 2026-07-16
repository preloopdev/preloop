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
