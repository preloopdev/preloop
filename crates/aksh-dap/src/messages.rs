//! DAP protocol message types.
//!
//! 1:1 port of `src/Runner.Worker/Dap/DapMessages.cs` in
//! `actions/runner` v2.335.1. The shape of every variant below is
//! determined by the upstream source — do not invent new fields.
//!
//! The base [`ProtocolMessage`] carries the DAP `seq` counter (1-based,
//! monotonically increasing per direction) and the message `type`
//! discriminator (`request`, `response`, `event`). Concrete subtypes
//! fill in `command` (requests), `request_seq`/`success`/`command` (responses),
//! and `event` (events). Body fields are passed through as `serde_json::Value`
//! because the DAP spec allows arbitrary arguments and the runner only
//! inspects a small known set.

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ─── DAP command enum (REPL-facing subset) ────────────────────────────────

/// The subset of DAP commands the runner's REPL needs to dispatch.
///
/// Mirrors `DapMessages.cs::DapCommand` in upstream. Other commands
/// (`threads`, `stackTrace`, `source`, `scopes`, `variables`, `next`,
/// `stepIn`, `stepOut`, `pause`, `disconnect`, `terminate`, etc.) are
/// handled by the [`crate::debugger::DapDebugger`] state machine and
/// do not need to be enumerated here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DapCommand {
    Continue,
    Next,
    StepIn,
    StepOut,
    Disconnect,
}

impl DapCommand {
    /// Parse a DAP `command` string into the enum, returning `None`
    /// for commands that the REPL does not handle.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "continue" => Some(Self::Continue),
            "next" => Some(Self::Next),
            "stepIn" => Some(Self::StepIn),
            "stepOut" => Some(Self::StepOut),
            "disconnect" => Some(Self::Disconnect),
            _ => None,
        }
    }
}

// ─── Well-known DAP events emitted by the runner ──────────────────────────
//
// These constants are the canonical `event` field values. Listed here
// in one place so the test suite and the debugger don't drift apart.

/// `initialized` — sent by the debug adapter after `initialize` is
/// received and the session is ready to accept `configurationDone`.
pub const EVENT_INITIALIZED: &str = "initialized";
/// `stopped` — sent when execution pauses (e.g. on step entry, breakpoint).
pub const EVENT_STOPPED: &str = "stopped";
/// `continued` — sent when execution resumes after a stopped event.
pub const EVENT_CONTINUED: &str = "continued";
/// `thread` — a thread event (started/exited). Used for synthetic
/// thread lifecycle in the runner debugger.
pub const EVENT_THREAD: &str = "thread";
/// `output` — streamed step/REPL output (cat stdout, etc.).
pub const EVENT_OUTPUT: &str = "output";
/// `terminated` — debuggee has terminated (job completed/cancelled).
pub const EVENT_TERMINATED: &str = "terminated";
/// `exited` — debuggee process exit (always follows `terminated`).
pub const EVENT_EXITED: &str = "exited";

// ─── Base message ─────────────────────────────────────────────────────────

/// Base class of requests, responses, and events per the DAP spec.
///
/// The `seq` (sequence) field is 1-based and monotonically increasing
/// for each direction (client→adapter and adapter→client maintain
/// independent counters). The first message a peer sends has `seq=1`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProtocolMessage {
    /// Sequence number of the message. The seq for the first
    /// message sent by a client or debug adapter is 1, and for each
    /// subsequent message is 1 greater than the previous message.
    #[serde(rename = "seq")]
    pub seq: i64,

    /// Message type: `"request"`, `"response"`, or `"event"`.
    #[serde(rename = "type")]
    pub message_type: String,
}

impl ProtocolMessage {
    /// Build a new message header. The body is filled in by the
    /// request/response/event subtypes.
    pub fn new(seq: i64, message_type: impl Into<String>) -> Self {
        Self {
            seq,
            message_type: message_type.into(),
        }
    }
}

// ─── Structured error message ─────────────────────────────────────────────

/// A structured error message — used inside `Response.message` and
/// inside `Event.body.message` for human-readable diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Message {
    /// Unique identifier for the message (opaque string).
    #[serde(rename = "id", skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,

    /// Format string, e.g. `"key.not.found"`.
    #[serde(rename = "format", skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,

    /// Whether the message is a fatal/unrecoverable error.
    #[serde(rename = "showUser", skip_serializing_if = "Option::is_none")]
    pub show_user: Option<bool>,

    /// Replacement variables for `{name}` placeholders in `format`.
    #[serde(rename = "variables", skip_serializing_if = "Option::is_none")]
    pub variables: Option<Value>,

    /// Fallback human-readable text the client may display verbatim.
    #[serde(rename = "body", skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,

    /// Optional structured payload. Some clients (and our REPL)
    /// attach a richer object here.
    #[serde(rename = "details", skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

// ─── Request ──────────────────────────────────────────────────────────────

/// A DAP `request` — a message from the client to the adapter asking
/// it to do something. The runner's debugger handles the standard
/// subset described in the DAP spec.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Request {
    /// Common header.
    #[serde(flatten)]
    pub header: ProtocolMessage,

    /// The command being requested, e.g. `"initialize"`, `"continue"`.
    #[serde(rename = "command")]
    pub command: String,

    /// Optional command arguments. Each command defines its own shape.
    #[serde(rename = "arguments", default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Value>,
}

impl Request {
    /// Build a request with seq 1 and the given command. Use
    /// [`Request::with_seq`] when emitting multi-message traces.
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            header: ProtocolMessage::new(1, "request"),
            command: command.into(),
            arguments: None,
        }
    }

    /// Override the seq. Used in golden-fixture construction and
    /// tests.
    pub fn with_seq(mut self, seq: i64) -> Self {
        self.header.seq = seq;
        self
    }

    /// Attach command arguments.
    pub fn with_arguments(mut self, args: Value) -> Self {
        self.arguments = Some(args);
        self
    }
}

// ─── Response ─────────────────────────────────────────────────────────────

/// A DAP `response` — the adapter's reply to a `request`. Each
/// response carries the `request_seq` it is answering, the original
/// `command`, a `success` boolean, and an optional `message` for
/// human-readable errors.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Response {
    /// Common header.
    #[serde(flatten)]
    pub header: ProtocolMessage,

    /// The `command` from the request being answered.
    #[serde(rename = "command")]
    pub command: String,

    /// The `seq` of the request being answered.
    #[serde(rename = "request_seq")]
    pub request_seq: i64,

    /// `true` if the request succeeded. `false` if `message` describes
    /// an error and `body` is typically absent.
    #[serde(rename = "success")]
    pub success: bool,

    /// Optional human-readable error description when `success=false`.
    #[serde(rename = "message", skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Optional body. Each command defines its own shape. For
    /// `initialize` this carries the `Capabilities`.
    #[serde(rename = "body", default, skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,
}

impl Response {
    /// Construct a success response. `body` defaults to `Null`.
    pub fn success(seq: i64, request_seq: i64, command: impl Into<String>) -> Self {
        Self {
            header: ProtocolMessage::new(seq, "response"),
            command: command.into(),
            request_seq,
            success: true,
            message: None,
            body: Some(Value::Null),
        }
    }

    /// Construct a failure response. The `body` is omitted.
    pub fn failure(
        seq: i64,
        request_seq: i64,
        command: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            header: ProtocolMessage::new(seq, "response"),
            command: command.into(),
            request_seq,
            success: false,
            message: Some(message.into()),
            body: None,
        }
    }

    /// Set the body. Returns self for chaining.
    pub fn with_body(mut self, body: Value) -> Self {
        self.body = Some(body);
        self
    }
}

// ─── Event ────────────────────────────────────────────────────────────────

/// A DAP `event` — an unsolicited message from the adapter to the
/// client (e.g. `stopped`, `continued`, `output`, `terminated`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    /// Common header.
    #[serde(flatten)]
    pub header: ProtocolMessage,

    /// The event name, e.g. `"initialized"`, `"stopped"`.
    #[serde(rename = "event")]
    pub event: String,

    /// Optional event body. Each event defines its own shape.
    #[serde(rename = "body", default, skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,
}

impl Event {
    /// Build a new event.
    pub fn new(seq: i64, event: impl Into<String>) -> Self {
        Self {
            header: ProtocolMessage::new(seq, "event"),
            event: event.into(),
            body: None,
        }
    }

    /// Set the body. Returns self for chaining.
    pub fn with_body(mut self, body: Value) -> Self {
        self.body = Some(body);
        self
    }
}

// ─── Standard DAP capabilities returned in `initialize` ───────────────────

/// A subset of the `Capabilities` object the runner returns in the
/// `initialize` response. The full spec is many dozens of optional
/// boolean fields; we only enumerate the ones the runner actually
/// sends (mirroring `DapDebugger.cs::HandleInitialize`).
///
/// All fields are `bool` and default to `false`. Serialization is
/// hand-rolled so that only the `true` flags appear in the wire
/// body — matching the C# `JsonProperty(EmitDefaultValue = false)`
/// behavior.
#[derive(Debug, Clone, Deserialize, PartialEq, Default)]
pub struct Capabilities {
    /// The debug adapter supports the `configurationDone` request.
    #[serde(rename = "supportsConfigurationDoneRequest", default)]
    pub supports_configuration_done_request: bool,

    /// The debug adapter supports `conditionalBreakpoints`.
    #[serde(rename = "supportsConditionalBreakpoints", default)]
    pub supports_conditional_breakpoints: bool,

    /// The debug adapter supports `hitConditionalBreakpoints`.
    #[serde(rename = "supportsHitConditionalBreakpoints", default)]
    pub supports_hit_conditional_breakpoints: bool,

    /// The debug adapter supports function breakpoints.
    #[serde(rename = "supportsFunctionBreakpoints", default)]
    pub supports_function_breakpoints: bool,

    /// The debug adapter supports breakpoints in exceptions.
    #[serde(rename = "supportsExceptionFilterOptions", default)]
    pub supports_exception_filter_options: bool,

    /// The debug adapter supports `evaluate` requests for the
    /// REPL DSL (`help`, `run(...)`).
    #[serde(rename = "supportsEvaluateForHovers", default)]
    pub supports_evaluate_for_hovers: bool,

    /// `ExceptionBreakpointsFilters` advertised by the adapter.
    #[serde(rename = "exceptionBreakpointFilters", skip_serializing_if = "Option::is_none")]
    pub exception_breakpoint_filters: Option<Value>,

    /// The debug adapter supports the `terminate` request.
    #[serde(rename = "supportsTerminateRequest", default)]
    pub supports_terminate_request: bool,

    /// The debug adapter supports `stepBack`.
    #[serde(rename = "supportsStepBack", default)]
    pub supports_step_back: bool,
}

impl Capabilities {
    /// Build the default capabilities the runner advertises. Mirrors
    /// the C# `HandleInitialize` response body. Only the `true` flags
    /// are non-default; the rest are left at the C# default of
    /// `EmitDefaultValue = false`, so they don't appear in the
    /// serialized body.
    pub fn runner_default() -> Self {
        Self {
            supports_configuration_done_request: true,
            supports_evaluate_for_hovers: true,
            supports_terminate_request: true,
            ..Default::default()
        }
    }
}

impl Serialize for Capabilities {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        if self.supports_configuration_done_request {
            map.serialize_entry("supportsConfigurationDoneRequest", &true)?;
        }
        if self.supports_conditional_breakpoints {
            map.serialize_entry("supportsConditionalBreakpoints", &true)?;
        }
        if self.supports_hit_conditional_breakpoints {
            map.serialize_entry("supportsHitConditionalBreakpoints", &true)?;
        }
        if self.supports_function_breakpoints {
            map.serialize_entry("supportsFunctionBreakpoints", &true)?;
        }
        if self.supports_exception_filter_options {
            map.serialize_entry("supportsExceptionFilterOptions", &true)?;
        }
        if self.supports_evaluate_for_hovers {
            map.serialize_entry("supportsEvaluateForHovers", &true)?;
        }
        if self.supports_terminate_request {
            map.serialize_entry("supportsTerminateRequest", &true)?;
        }
        if self.supports_step_back {
            map.serialize_entry("supportsStepBack", &true)?;
        }
        if let Some(ebpf) = &self.exception_breakpoint_filters {
            map.serialize_entry("exceptionBreakpointFilters", ebpf)?;
        }
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_round_trips_json() {
        let req = Request::new("initialize").with_arguments(json!({
            "clientID": "vscode",
            "adapterID": "actions",
        }));
        let s = serde_json::to_string(&req).unwrap();
        let back: Request = serde_json::from_str(&s).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn response_success_carries_body() {
        let resp = Response::success(1, 1, "initialize")
            .with_body(json!({"supportsConfigurationDoneRequest": true}));
        let s = serde_json::to_string(&resp).unwrap();
        assert!(s.contains("\"success\":true"));
        assert!(s.contains("\"supportsConfigurationDoneRequest\":true"));
    }

    #[test]
    fn response_failure_carries_message() {
        let resp = Response::failure(1, 1, "continue", "no active step");
        let s = serde_json::to_string(&resp).unwrap();
        assert!(s.contains("\"success\":false"));
        assert!(s.contains("\"no active step\""));
    }

    #[test]
    fn event_with_body_round_trips() {
        let ev = Event::new(1, "stopped").with_body(json!({
            "reason": "step",
            "threadId": 1,
            "allThreadsContinued": false,
        }));
        let v: Value = serde_json::to_value(&ev).unwrap();
        assert_eq!(v["seq"], 1);
        assert_eq!(v["type"], "event");
        assert_eq!(v["event"], "stopped");
        assert_eq!(v["body"]["reason"], "step");
    }

    #[test]
    fn dap_command_parses_known_strings() {
        assert_eq!(DapCommand::from_str("continue"), Some(DapCommand::Continue));
        assert_eq!(DapCommand::from_str("next"), Some(DapCommand::Next));
        assert_eq!(DapCommand::from_str("stepIn"), Some(DapCommand::StepIn));
        assert_eq!(DapCommand::from_str("stepOut"), Some(DapCommand::StepOut));
        assert_eq!(DapCommand::from_str("disconnect"), Some(DapCommand::Disconnect));
        assert_eq!(DapCommand::from_str("nope"), None);
    }

    #[test]
    fn capabilities_default_serializes_only_runner_set_flags() {
        let caps = Capabilities::runner_default();
        let v: Value = serde_json::to_value(&caps).unwrap();
        // Only the `true` flags should be present.
        assert_eq!(v["supportsConfigurationDoneRequest"], true);
        assert_eq!(v["supportsEvaluateForHovers"], true);
        assert_eq!(v["supportsTerminateRequest"], true);
        // Unsupported ones should not appear at all.
        assert!(v.get("supportsStepBack").is_none());
    }
}
