//! aksh-dap — Debug Adapter Protocol implementation for the aksh runner.
//!
//! This crate is a 1:1 Rust port of the DAP subsystem added to
//! `actions/runner` in v2.335.0. The source of truth is
//! `src/Runner.Worker/Dap/*` and the public `IDapDebugger` interface
//! from `src/Runner.Worker/Dap/IDapDebugger.cs`.
//!
//! Layout (mirrors the C# folder):
//! - [`messages`] — `ProtocolMessage`, `Message`, request/response/event DTOs,
//!   and the `DapCommand` enum used by the REPL executor.
//! - [`framing`] — `Content-Length` framed reader/writer used by both
//!   raw-TCP and WebSocket transports (LSP / DAP standard framing).
//! - [`bridge`] — `WebSocketDapBridge` — HTTP/2/TLS/WebSocket
//!   prefix detector + bidirectional pump between the DAP TCP server
//!   and a WebSocket (port from upstream `WebSocketDapBridge.cs`).
//! - [`debugger`] — `DapDebugger` — the DAP server itself: state
//!   machine, pause/resume, content-length framed TCP listener,
//!   integration with the synthetic `execution.yml` source view,
//!   and the devtunnel relay integration via subprocess.
//! - [`repl`] — `DapReplParser` / `DapReplExecutor` — the REPL DSL
//!   (`help`, `run("...")`) the user can `evaluate` against the
//!   current job context.
//! - [`variables`] — `DapVariableProvider` — converts the runner's
//!   `PipelineContextData` into DAP `Scope`s and `Variable`s with
//!   secret masking.
//! - [`view`] — `JobExecutionView` — generates the synthetic
//!   `execution.yml` and the `Source` returned to the client.
//! - [`config`] — `DebuggerConfig` and `DebuggerTunnelInfo`, the
//!   per-job debug configuration carried in the acquire response.
//! - [`harness`] — capture/replay harness used for golden-fixture
//!   parity tests against `actions/runner`.
//!
//! All public APIs intentionally mirror the C# surface so that the
//! existing C# L0 tests can be ported with a one-to-one
//! `name: action` mapping.

#![doc(html_root_url = "https://docs.rs/aksh-dap/0.1.0")]

pub mod bridge;
pub mod config;
pub mod debugger;
pub mod framing;
pub mod harness;
pub mod messages;
pub mod repl;
pub mod variables;
pub mod view;

/// Re-export of the most commonly used types so callers can
/// `use aksh_dap::*;` without listing every module.
pub use bridge::{IncomingStreamPrefixKind, WebSocketDapBridge};
pub use config::{DebuggerConfig, DebuggerTransportMode, DebuggerTunnelInfo};
pub use debugger::{DapDebugger, DapSessionState, IDapDebugger};
// Re-export at the top level for downstream consumers that want
// `aksg_dap::IDapDebugger` without reaching into the `debugger` module.
pub use framing::{
    read_message, write_message, FrameError, MAX_HEADER_LINE_LENGTH, MAX_MESSAGE_SIZE,
};
pub use messages::{
    DapCommand, Event, Message, ProtocolMessage, Request, Response, EVENT_CONTINUED, EVENT_EXITED,
    EVENT_INITIALIZED, EVENT_OUTPUT, EVENT_STOPPED, EVENT_TERMINATED, EVENT_THREAD,
};
pub use repl::{DapReplCommand, DapReplExecutor, DapReplParser, HelpCommand, RunCommand};
pub use variables::{DapScope, DapVariable, DapVariableProvider};
pub use view::{JobExecutionView, PredictedPostStep, SourceEntry, StepLine};

/// Time spent waiting for a debugger client to connect before the
/// `DapDebugger` gives up and the job fails with "The debugger
/// failed to start or no debugger client connected in time." Mirrors
/// `Constants.cs::_defaultTimeoutMinutes = 15` in upstream.
pub const DEFAULT_CONNECTION_TIMEOUT_MINUTES: u32 = 15;

/// Default tunnel connect timeout in seconds. Mirrors
/// `Constants.cs::_defaultTunnelConnectTimeoutSeconds = 30`.
pub const DEFAULT_TUNNEL_CONNECT_TIMEOUT_SECONDS: u32 = 30;

/// Hard-coded DAP port per upstream
/// `DapDebugger.cs::StartAsyncUsesPortFromTunnelConfig`. The runner
/// refuses to bind anything else and the server refuses to issue a
/// token for anything else, by design.
pub const DAP_TUNNEL_PORT: u16 = 4711;

/// Environment variable names mirrored from `Constants.cs`.
pub mod env_vars {
    /// Custom DAP connection timeout (minutes).
    pub const DAP_CONNECTION_TIMEOUT: &str = "ACTIONS_RUNNER_DAP_CONNECTION_TIMEOUT";
    /// Custom DAP tunnel connect timeout (seconds).
    pub const DAP_TUNNEL_CONNECT_TIMEOUT_SECONDS: &str =
        "ACTIONS_RUNNER_DAP_TUNNEL_CONNECT_TIMEOUT_SECONDS";
}

/// Secret-masker allow-list. The upstream C# code teaches its
/// secret masker to permit these protocol keywords so the literal
/// strings `response`, `initialize`, and `event` (which appear in
/// DAP frames) are not redacted.
///
/// See `DapMessagesL0.cs::InitializeRequestOverSocketPreservesProtocolMetadataWhenSecretsCollide`.
pub const DAP_PROTOCOL_KEYWORDS: &[&str] = &["response", "initialize", "event"];
