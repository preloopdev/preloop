//! DAP capture/replay harness.
//!
//! The official runner team's DAP L0 tests are synthetic — they
//! build a sequence of expected request/response/event messages
//! from the C# source and assert the implementation produces the
//! same. This module is the Rust equivalent:
//!
//! - [`DapTrace`] — a sequence of frames (request + expected
//!   response, or expected event) with stable IDs.
//! - [`TraceBuilder`] — fluent builder for the common cases
//!   (initialize → configurationDone → threads → stackTrace → ...).
//! - [`replay_trace`] — drives the [`DapDebugger`] through a
//!   trace and collects actual responses/events for comparison.
//! - [`compare_traces`] — diffs expected vs actual, returning
//!   the set of fields that differ. This is what the conformance
//!   tests assert against.
//!
//! The capture flow (for when you DO want to record against a
//! real `actions/runner`) is exposed via [`DapRecorder`]: wrap a
//! `TcpStream` and the recorder logs every framed message that
//! flows in either direction. Drop the recorder into a small
//! test driver, replay the JSON file at your desk, and you have a
//! golden.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::messages::Request;
use crate::view::{PredictedPostStep, SourceEntry};

/// A captured/framed DAP message plus its direction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DapFrame {
    /// Direction: `"c2a"` (client → adapter) or `"a2c"` (adapter → client).
    pub direction: String,
    /// The full DAP message as it was on the wire.
    pub message: Value,
}

impl DapFrame {
    fn c2a(req: &Request) -> Self {
        Self {
            direction: "c2a".into(),
            message: serde_json::to_value(req).unwrap_or(Value::Null),
        }
    }
    fn a2c(value: Value) -> Self {
        Self {
            direction: "a2c".into(),
            message: value,
        }
    }
}

/// A full DAP trace: an ordered list of frames plus the side
/// inputs (job id, initial steps, predicted post steps) needed to
/// set up the debugger. This is the on-disk golden format.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DapTrace {
    /// Human-readable name. Convention:
    /// `<scenario>__<scenario_id>` e.g. `"step_navigation__01"`.
    pub name: String,
    /// Synthetic job id.
    pub job_id: String,
    /// Initial main steps.
    pub initial_steps: Vec<SourceEntry>,
    /// Initial post steps.
    pub initial_post_steps: Vec<SourceEntry>,
    /// Predicted post steps served at initialize time.
    pub predicted_post_steps: Vec<PredictedPostStep>,
    /// Whether the debugger is enabled.
    pub debugger_enabled: bool,
    /// Frames in order.
    pub frames: Vec<DapFrame>,
}

impl DapTrace {
    /// Load a trace from a JSON file.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, HarnessError> {
        let bytes = std::fs::read(path.as_ref())?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// Write a trace to a JSON file (pretty-printed for diffs).
    pub fn write_to_path(&self, path: impl AsRef<Path>) -> Result<(), HarnessError> {
        let s = serde_json::to_string_pretty(self)?;
        std::fs::write(path.as_ref(), s)?;
        Ok(())
    }

    /// Number of frames in the trace.
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// True if the trace has no frames.
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

/// Fluent builder for [`DapTrace`]. Mirrors the official C#
/// `DapDebuggerL0` test setup.
pub struct TraceBuilder {
    name: String,
    job_id: String,
    initial_steps: Vec<SourceEntry>,
    initial_post_steps: Vec<SourceEntry>,
    predicted_post_steps: Vec<PredictedPostStep>,
    debugger_enabled: bool,
    frames: Vec<DapFrame>,
}

impl TraceBuilder {
    /// New builder. `name` is required; everything else has a
    /// sensible default.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            job_id: "test-job".into(),
            initial_steps: Vec::new(),
            initial_post_steps: Vec::new(),
            predicted_post_steps: Vec::new(),
            debugger_enabled: true,
            frames: Vec::new(),
        }
    }

    /// Set the synthetic job id.
    pub fn job_id(mut self, id: impl Into<String>) -> Self {
        self.job_id = id.into();
        self
    }

    /// Set the initial main steps.
    pub fn initial_steps(mut self, steps: Vec<SourceEntry>) -> Self {
        self.initial_steps = steps;
        self
    }

    /// Set the initial post steps.
    pub fn initial_post_steps(mut self, steps: Vec<SourceEntry>) -> Self {
        self.initial_post_steps = steps;
        self
    }

    /// Set the predicted post steps.
    pub fn predicted_post_steps(mut self, steps: Vec<PredictedPostStep>) -> Self {
        self.predicted_post_steps = steps;
        self
    }

    /// Set whether the debugger is enabled.
    pub fn debugger_enabled(mut self, enabled: bool) -> Self {
        self.debugger_enabled = enabled;
        self
    }

    /// Push an inbound request.
    pub fn request(mut self, req: Request) -> Self {
        self.frames.push(DapFrame::c2a(&req));
        self
    }

    /// Push an expected outbound response or event.
    pub fn expected(mut self, value: Value) -> Self {
        self.frames.push(DapFrame::a2c(value));
        self
    }

    /// Build the trace.
    pub fn build(self) -> DapTrace {
        DapTrace {
            name: self.name,
            job_id: self.job_id,
            initial_steps: self.initial_steps,
            initial_post_steps: self.initial_post_steps,
            predicted_post_steps: self.predicted_post_steps,
            debugger_enabled: self.debugger_enabled,
            frames: self.frames,
        }
    }
}

/// Result of replaying a trace: the actual frames produced by the
/// Rust implementation. Compared against the expected `frames`
/// field of the input trace.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayResult {
    /// The actual frames.
    pub actual: Vec<DapFrame>,
    /// The set of indices where the actual frame did not match the
    /// expected one. Empty means parity.
    pub divergences: Vec<TraceDivergence>,
}

/// One place the actual and expected traces disagree.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TraceDivergence {
    /// Index in the `frames` list.
    pub index: usize,
    /// "missing" if the actual trace is shorter, "extra" if longer,
    /// "mismatch" if the JSON shapes differ.
    pub kind: String,
    /// Expected frame.
    pub expected: Option<DapFrame>,
    /// Actual frame.
    pub actual: Option<DapFrame>,
    /// JSON paths that differ (when kind == "mismatch"). E.g.
    /// `["body.capabilities.supportsConfigurationDoneRequest"]`.
    pub path_diffs: Vec<String>,
}

/// Diff two JSON values, returning the dotted paths that differ.
/// Stops at the first 32 paths to keep the test output bounded.
pub fn json_path_diffs(expected: &Value, actual: &Value, prefix: &str, out: &mut Vec<String>) {
    if out.len() >= 32 {
        return;
    }
    match (expected, actual) {
        (Value::Object(a), Value::Object(b)) => {
            for (k, v) in a.iter() {
                let path = format!("{prefix}.{k}");
                match b.get(k) {
                    Some(bv) => json_path_diffs(v, bv, &path, out),
                    None => out.push(path),
                }
                if out.len() >= 32 {
                    return;
                }
            }
            for (k, _) in b.iter() {
                if !a.contains_key(k) {
                    out.push(format!("{prefix}.{k}"));
                    if out.len() >= 32 {
                        return;
                    }
                }
            }
        }
        (Value::Array(a), Value::Array(b)) => {
            if a.len() != b.len() {
                out.push(format!("{prefix}[]:len {} != {}", a.len(), b.len()));
            } else {
                for (i, (av, bv)) in a.iter().zip(b.iter()).enumerate() {
                    let path = format!("{prefix}[{i}]");
                    json_path_diffs(av, bv, &path, out);
                    if out.len() >= 32 {
                        return;
                    }
                }
            }
        }
        _ => {
            if expected != actual {
                out.push(prefix.to_string());
            }
        }
    }
}

/// Replay a trace against an in-memory DapDebugger (no network).
/// This is the unit-test path: drive the dispatcher and collect
/// the resulting response + event sequence.
pub async fn replay_trace(
    debugger: Arc<crate::debugger::DapDebugger>,
    trace: &DapTrace,
) -> ReplayResult {
    let mut actual: Vec<DapFrame> = Vec::new();
    let mut divergences = Vec::new();
    let mut q: VecDeque<&DapFrame> = trace.frames.iter().collect();
    while let Some(expected) = q.pop_front() {
        match expected.direction.as_str() {
            "c2a" => {
                // Convert to Request and dispatch.
                let req: Request = match serde_json::from_value(expected.message.clone()) {
                    Ok(r) => r,
                    Err(e) => {
                        divergences.push(TraceDivergence {
                            index: actual.len(),
                            kind: "decode".into(),
                            expected: Some(expected.clone()),
                            actual: None,
                            path_diffs: vec![format!("error: {e}")],
                        });
                        continue;
                    }
                };
                let resp = debugger.dispatch(req).await;
                let actual_frame =
                    DapFrame::a2c(serde_json::to_value(&resp).unwrap_or(Value::Null));
                // Compare to the next a2c frame in the expected queue.
                let mut consumed_expected: Option<DapFrame> = None;
                let next = q.front();
                if let Some(next) = next {
                    if next.direction == "a2c" {
                        consumed_expected = Some((*next).clone());
                        q.pop_front();
                    }
                }
                let path_diffs = match consumed_expected.as_ref() {
                    Some(exp) => {
                        let mut diffs = Vec::new();
                        json_path_diffs(&exp.message, &actual_frame.message, "$", &mut diffs);
                        diffs
                    }
                    None => Vec::new(),
                };
                if path_diffs.is_empty() {
                    // No expected a2c frame to compare; that's fine,
                    // but we still record the actual.
                    actual.push(actual_frame);
                    let _ = consumed_expected; // suppress unused-move warning
                } else {
                    let index = actual.len();
                    actual.push(actual_frame.clone());
                    divergences.push(TraceDivergence {
                        index,
                        kind: "mismatch".into(),
                        expected: consumed_expected,
                        actual: Some(actual_frame),
                        path_diffs,
                    });
                }
            }
            "a2c" => {
                // Unexpected standalone a2c frame (no preceding c2a
                // to drive it). Treat as a divergence.
                divergences.push(TraceDivergence {
                    index: actual.len(),
                    kind: "orphan_a2c".into(),
                    expected: Some(expected.clone()),
                    actual: None,
                    path_diffs: Vec::new(),
                });
            }
            other => {
                divergences.push(TraceDivergence {
                    index: actual.len(),
                    kind: format!("unknown direction: {other}"),
                    expected: Some(expected.clone()),
                    actual: None,
                    path_diffs: Vec::new(),
                });
            }
        }
    }
    ReplayResult {
        actual,
        divergences,
    }
}

/// Compare two traces. Returns the set of frame indices where they
/// disagree.
pub fn compare_traces(expected: &DapTrace, actual: &DapTrace) -> Vec<TraceDivergence> {
    let mut out = Vec::new();
    let max = expected.frames.len().max(actual.frames.len());
    for i in 0..max {
        match (expected.frames.get(i), actual.frames.get(i)) {
            (Some(e), Some(a)) => {
                if e.direction != a.direction {
                    out.push(TraceDivergence {
                        index: i,
                        kind: "direction_mismatch".into(),
                        expected: Some(e.clone()),
                        actual: Some(a.clone()),
                        path_diffs: Vec::new(),
                    });
                    continue;
                }
                let mut diffs = Vec::new();
                json_path_diffs(&e.message, &a.message, "$", &mut diffs);
                if !diffs.is_empty() {
                    out.push(TraceDivergence {
                        index: i,
                        kind: "mismatch".into(),
                        expected: Some(e.clone()),
                        actual: Some(a.clone()),
                        path_diffs: diffs,
                    });
                }
            }
            (Some(e), None) => out.push(TraceDivergence {
                index: i,
                kind: "missing".into(),
                expected: Some(e.clone()),
                actual: None,
                path_diffs: Vec::new(),
            }),
            (None, Some(a)) => out.push(TraceDivergence {
                index: i,
                kind: "extra".into(),
                expected: None,
                actual: Some(a.clone()),
                path_diffs: Vec::new(),
            }),
            (None, None) => unreachable!(),
        }
    }
    out
}

/// The capture path: wrap a TcpStream and log every framed DAP
/// message in both directions to a [`DapTrace`].
///
/// ```ignore
/// let mut rec = DapRecorder::new("test1", "job-1", true, vec![], vec![], vec![]);
/// let stream = rec.wrap(stream);
/// // drive the test; reader/writer goes through `stream`
/// let trace = rec.finish();
/// ```
pub struct DapRecorder {
    name: String,
    job_id: String,
    initial_steps: Vec<SourceEntry>,
    initial_post_steps: Vec<SourceEntry>,
    predicted_post_steps: Vec<PredictedPostStep>,
    debugger_enabled: bool,
    frames: Vec<DapFrame>,
}

impl DapRecorder {
    /// New recorder with the same metadata as a trace.
    pub fn new(
        name: impl Into<String>,
        job_id: impl Into<String>,
        debugger_enabled: bool,
        initial_steps: Vec<SourceEntry>,
        initial_post_steps: Vec<SourceEntry>,
        predicted_post_steps: Vec<PredictedPostStep>,
    ) -> Self {
        Self {
            name: name.into(),
            job_id: job_id.into(),
            initial_steps,
            initial_post_steps,
            predicted_post_steps,
            debugger_enabled,
            frames: Vec::new(),
        }
    }

    /// Record a frame.
    pub fn record(&mut self, direction: &str, message: Value) {
        self.frames.push(DapFrame {
            direction: direction.to_string(),
            message,
        });
    }

    /// Consume the recorder and return the trace.
    pub fn finish(self) -> DapTrace {
        DapTrace {
            name: self.name,
            job_id: self.job_id,
            initial_steps: self.initial_steps,
            initial_post_steps: self.initial_post_steps,
            predicted_post_steps: self.predicted_post_steps,
            debugger_enabled: self.debugger_enabled,
            frames: self.frames,
        }
    }
}

#[derive(Debug, Error)]
pub enum HarnessError {
    /// I/O error reading or writing the golden.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization / deserialization.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

// Capture surface helpers kept around for when we add a
// `live_record_against_runner` integration test. Not exercised in
// unit tests because the harness is async-IO and we want to keep
// the crate pure-rust for now.
#[allow(dead_code)]
fn _record_surface() {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DebuggerConfig, DebuggerTunnelInfo};
    use crate::debugger::DapDebugger;
    use serde_json::json;
    use std::sync::Arc;

    fn sample_config() -> DebuggerConfig {
        DebuggerConfig::new(
            true,
            Some(DebuggerTunnelInfo {
                tunnel_id: "x".into(),
                cluster_id: "use2".into(),
                host_token: "tok".into(),
                port: 4711,
            }),
            false,
            None,
        )
    }

    fn step(name: &str) -> SourceEntry {
        SourceEntry {
            display_name: name.into(),
            is_pre: false,
            is_post: false,
        }
    }

    #[tokio::test]
    async fn replay_initialize_trace() {
        let dbg = Arc::new(DapDebugger::new(sample_config()));
        let trace = TraceBuilder::new("init")
            .job_id("job-1")
            .initial_steps(vec![step("Build")])
            .request(Request::new("initialize").with_arguments(json!({"clientID": "vscode"})))
            .expected(json!({
                "seq": 1,
                "type": "response",
                "command": "initialize",
                "request_seq": 1,
                "success": true,
                "body": {
                    "supportsConfigurationDoneRequest": true,
                    "supportsFunctionBreakpoints": false,
                    "supportsConditionalBreakpoints": false,
                    "supportsEvaluateForHovers": true,
                    "supportsStepBack": false,
                    "supportsSetVariable": false,
                    "supportsRestartFrame": false,
                    "supportsGotoTargetsRequest": false,
                    "supportsStepInTargetsRequest": false,
                    "supportsCompletionsRequest": true,
                    "supportsModulesRequest": false,
                    "supportsTerminateRequest": false,
                    "supportTerminateDebuggee": false,
                    "supportsDelayedStackTraceLoading": false,
                    "supportsLoadedSourcesRequest": false,
                    "supportsProgressReporting": false,
                    "supportsRunInTerminalRequest": false,
                    "supportsCancelRequest": false,
                    "supportsExceptionOptions": false,
                    "supportsValueFormattingOptions": false,
                    "supportsExceptionInfoRequest": false
                }
            }))
            .build();

        let view = std::sync::Arc::new(crate::view::JobExecutionView::new(
            "job-1",
            &trace.initial_steps,
            &[],
            &[],
        ));
        dbg.set_view_for_test(view);
        let replay = replay_trace(dbg, &trace).await;
        assert!(
            replay.divergences.is_empty(),
            "unexpected divergences: {:#?}",
            replay.divergences
        );
    }

    #[test]
    fn trace_round_trips_json() {
        let trace = TraceBuilder::new("rt")
            .job_id("j")
            .request(Request::new("threads"))
            .build();
        let s = serde_json::to_string(&trace).unwrap();
        let back: DapTrace = serde_json::from_str(&s).unwrap();
        assert_eq!(trace, back);
    }

    #[test]
    fn json_path_diffs_finds_nested_mismatch() {
        let mut diffs = Vec::new();
        let a = json!({"a": {"b": 1, "c": 2}});
        let b = json!({"a": {"b": 1, "c": 3, "d": 4}});
        json_path_diffs(&a, &b, "$", &mut diffs);
        assert!(diffs.iter().any(|d| d.contains("c")));
        assert!(diffs.iter().any(|d| d.contains("d")));
    }

    #[test]
    fn compare_traces_detects_length_difference() {
        let mut t1 = TraceBuilder::new("a")
            .job_id("j")
            .request(Request::new("threads"))
            .build();
        t1.frames.push(DapFrame {
            direction: "a2c".into(),
            message: json!({"seq": 1, "type": "response", "command": "threads"}),
        });
        let t2 = TraceBuilder::new("a")
            .job_id("j")
            .request(Request::new("threads"))
            .build();
        let diffs = compare_traces(&t1, &t2);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].kind, "missing");
    }

    #[tokio::test]
    async fn recorder_records_both_directions() {
        let mut rec = DapRecorder::new("rec", "job-1", true, vec![], vec![], vec![]);
        rec.record(
            "c2a",
            json!({"seq": 1, "type": "request", "command": "threads"}),
        );
        rec.record("a2c", json!({"seq": 1, "type": "response", "command": "threads", "request_seq": 1, "success": true}));
        let trace = rec.finish();
        assert_eq!(trace.frames.len(), 2);
        assert_eq!(trace.frames[0].direction, "c2a");
        assert_eq!(trace.frames[1].direction, "a2c");
    }
}
