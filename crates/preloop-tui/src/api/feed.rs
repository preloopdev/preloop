//! Live feed records: the `events.ndjson` lines and the `live-log` SSE payload.
//!
//! Lines are parsed in two steps: first to a JSON value, then by the value's
//! `type`. Unknown types become [`FeedEvent::Other`] and are ignored, so a
//! newer engine cannot break the stream.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::run::ExecStatus;

/// An annotation a job or step emitted.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct Annotation {
    /// `notice`, `warning` or `error`.
    pub level: String,
    /// Annotation text.
    pub message: String,
    /// Source file the annotation points at.
    pub file: Option<String>,
    /// Source line the annotation points at.
    pub line: Option<u64>,
    /// Short title.
    pub title: Option<String>,
    /// Step the annotation belongs to, when the engine knows it.
    pub step_id: Option<String>,
}

/// A step's status changed. Mirrors the engine's `step_status` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepStatus {
    /// Run the step belongs to.
    pub run_id: String,
    /// Job the step belongs to.
    pub job_id: String,
    /// Step id (`StepRecord.id`).
    pub step_id: String,
    /// Declared position, for workflow steps.
    pub workflow_index: Option<usize>,
    /// Runner timeline position.
    pub runner_number: Option<u32>,
    /// Display name.
    pub name: String,
    /// `pending`, `in_progress`, `success`, `failure`, `cancelled` or `skipped`.
    pub conclusion: String,
    /// When the step started.
    pub started_at: Option<DateTime<Utc>>,
    /// When the step finished.
    pub finished_at: Option<DateTime<Utc>>,
}

/// One parsed feed event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedEvent {
    /// A run was accepted.
    RunAccepted {
        /// Run id.
        run_id: String,
        /// Jobs queued for the run.
        queued_jobs: usize,
    },
    /// The run's status changed.
    RunStatus {
        /// Run id.
        run_id: String,
        /// New status.
        status: ExecStatus,
    },
    /// A job's status changed.
    JobStatus {
        /// Run id.
        run_id: String,
        /// Job id.
        job_id: String,
        /// New status.
        status: ExecStatus,
    },
    /// A job reached a terminal status.
    JobCompleted {
        /// Run id.
        run_id: String,
        /// Job id.
        job_id: String,
        /// Final status.
        status: ExecStatus,
    },
    /// A step's status changed.
    StepStatus(StepStatus),
    /// An annotation was emitted.
    Annotation {
        /// Run id.
        run_id: String,
        /// Job id.
        job_id: String,
        /// The annotation.
        annotation: Annotation,
    },
    /// An event type this build does not model. Carries the `type` tag.
    Other(String),
}

impl FeedEvent {
    /// The run the event refers to, when it names one.
    pub fn run_id(&self) -> Option<&str> {
        match self {
            Self::RunAccepted { run_id, .. }
            | Self::RunStatus { run_id, .. }
            | Self::JobStatus { run_id, .. }
            | Self::JobCompleted { run_id, .. }
            | Self::Annotation { run_id, .. } => Some(run_id),
            Self::StepStatus(step) => Some(&step.run_id),
            Self::Other(_) => None,
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct RunAcceptedWire {
    run_id: String,
    queued_jobs: usize,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct RunStatusWire {
    run_id: String,
    status: ExecStatus,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct JobStatusWire {
    run_id: String,
    job_id: String,
    status: ExecStatus,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct AnnotationWire {
    run_id: String,
    job_id: String,
    #[serde(flatten)]
    annotation: Annotation,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct StepStatusWire {
    run_id: String,
    job_id: String,
    step_id: String,
    workflow_index: Option<usize>,
    runner_number: Option<u32>,
    name: String,
    conclusion: String,
    started_at_us: Option<i64>,
    finished_at_us: Option<i64>,
}

fn from_micros(value: Option<i64>) -> Option<DateTime<Utc>> {
    value.and_then(DateTime::<Utc>::from_timestamp_micros)
}

/// Parse one NDJSON line from `events.ndjson`.
///
/// Returns `Err` only when the line is not JSON or a known event type has the
/// wrong shape. Unknown types return `Ok(FeedEvent::Other(..))`.
pub fn parse_feed_line(line: &str) -> Result<FeedEvent, serde_json::Error> {
    let value: Value = serde_json::from_str(line)?;
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let event = match kind.as_str() {
        "run_accepted" => {
            let wire: RunAcceptedWire = serde_json::from_value(value)?;
            FeedEvent::RunAccepted {
                run_id: wire.run_id,
                queued_jobs: wire.queued_jobs,
            }
        }
        "run_status" => {
            let wire: RunStatusWire = serde_json::from_value(value)?;
            FeedEvent::RunStatus {
                run_id: wire.run_id,
                status: wire.status,
            }
        }
        "job_status" => {
            let wire: JobStatusWire = serde_json::from_value(value)?;
            FeedEvent::JobStatus {
                run_id: wire.run_id,
                job_id: wire.job_id,
                status: wire.status,
            }
        }
        "job_completed" => {
            let wire: JobStatusWire = serde_json::from_value(value)?;
            FeedEvent::JobCompleted {
                run_id: wire.run_id,
                job_id: wire.job_id,
                status: wire.status,
            }
        }
        "step_status" => {
            let wire: StepStatusWire = serde_json::from_value(value)?;
            FeedEvent::StepStatus(StepStatus {
                run_id: wire.run_id,
                job_id: wire.job_id,
                step_id: wire.step_id,
                workflow_index: wire.workflow_index,
                runner_number: wire.runner_number,
                name: wire.name,
                conclusion: wire.conclusion,
                started_at: from_micros(wire.started_at_us),
                finished_at: from_micros(wire.finished_at_us),
            })
        }
        "annotation" => {
            let wire: AnnotationWire = serde_json::from_value(value)?;
            FeedEvent::Annotation {
                run_id: wire.run_id,
                job_id: wire.job_id,
                annotation: wire.annotation,
            }
        }
        _ => FeedEvent::Other(kind),
    };
    Ok(event)
}

/// The payload of one SSE `live-log` event: console lines for one step.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct LiveLogChunk {
    /// Step id; equals [`StepRecord::id`](super::run::StepRecord).
    pub step_id: String,
    /// 1-based line number of the first line in `value`, within the step.
    pub start_line: u64,
    /// Number of lines in `value`.
    pub count: usize,
    /// The console lines.
    pub value: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_type_is_other_not_error() {
        let event = parse_feed_line(r#"{"type":"log","run_id":"r","job_id":"j","line":"x"}"#)
            .expect("unknown types parse");
        assert_eq!(event, FeedEvent::Other("log".to_owned()));
    }

    #[test]
    fn step_status_converts_micros() {
        let event = parse_feed_line(
            r#"{"type":"step_status","run_id":"r","job_id":"j","step_id":"s",
                "workflow_index":0,"runner_number":2,"name":"cargo test",
                "conclusion":"in_progress","started_at_us":1761090000000000}"#,
        )
        .expect("step_status parses");
        match event {
            FeedEvent::StepStatus(step) => {
                assert_eq!(step.step_id, "s");
                assert_eq!(step.conclusion, "in_progress");
                assert_eq!(step.workflow_index, Some(0));
                assert_eq!(step.finished_at, None);
                assert_eq!(
                    step.started_at.map(|t| t.timestamp_micros()),
                    Some(1761090000000000)
                );
            }
            other => panic!("expected StepStatus, got {other:?}"),
        }
    }

    #[test]
    fn job_status_reads_status() {
        let event = parse_feed_line(
            r#"{"type":"job_status","run_id":"r","job_id":"test (sandbox)","status":"failure"}"#,
        )
        .expect("job_status parses");
        assert_eq!(
            event,
            FeedEvent::JobStatus {
                run_id: "r".into(),
                job_id: "test (sandbox)".into(),
                status: ExecStatus::Failure,
            }
        );
    }

    #[test]
    fn live_log_chunk_is_camel_case() {
        let chunk: LiveLogChunk = serde_json::from_str(
            r#"{"stepId":"s","startLine":1204,"count":1,"value":["hello"]}"#,
        )
        .expect("camelCase wire");
        assert_eq!(chunk.start_line, 1204);
        assert_eq!(chunk.value, vec!["hello".to_owned()]);
    }
}
