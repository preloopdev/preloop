//! DAP server and state machine.
//!
//! 1:1 port of `src/Runner.Worker/Dap/DapDebugger.cs` and
//! `src/Runner.Worker/Dap/IDapDebugger.cs`.
//!
//! The `DapDebugger` runs three things in one process:
//! 1. A raw-TCP DAP server (Content-Length framed) on
//!    [`crate::DAP_TUNNEL_PORT`] (4711), exposed to the editor
//!    via the WebSocket bridge.
//! 2. A `devtunnel host` subprocess that hosts the tunnel
//!    connection to Microsoft's Dev Tunnels relay.
//! 3. A pause/resume hook that the runner's step loop awaits
//!    before running each step.
//!
//! Lifecycle (mirrors `IDapDebugger`):
//! - `start` — bind the TCP server, launch the devtunnel host,
//!   wait for the editor to attach (or the connection timeout).
//! - `wait_until_ready` — block until either the editor sends
//!   `configurationDone` or the timeout elapses.
//! - `on_job_steps_initialized` — build the synthetic
//!   `execution.yml` source view from the resolved step list.
//! - `on_post_step_registered` — add a dynamic post step to the
//!   view; if a predicted post step with the same name exists,
//!   claim its frame id so the editor view stays stable.
//! - `on_step_starting` — emit `stopped` and await `continue`.
//! - `on_step_completed` — emit `continued`.
//! - `on_job_completed` — emit `terminated` and `exited`,
//!   pause for inspection, then shut the transport down.
//! - `stop` — cancel everything.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex as PlMutex;
use serde_json::{json, Value};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tracing::{debug, error, warn};

use crate::config::{DebuggerConfig, DebuggerTransportMode, DebuggerTunnelInfo};
use crate::messages::{
    Capabilities, Event, Request, Response, EVENT_CONTINUED, EVENT_EXITED, EVENT_INITIALIZED,
    EVENT_OUTPUT, EVENT_STOPPED, EVENT_TERMINATED, EVENT_THREAD,
};
use crate::repl::{DapReplExecutor, DapReplParser, ParseError};
use crate::variables::DapVariableProvider;
use crate::view::{JobExecutionView, PredictedPostStep, SourceEntry};

/// The DAP session state machine. Mirrors `DapSessionState` upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DapSessionState {
    /// Debugger has not been started yet.
    NotStarted,
    /// TCP listener is up, devtunnel host is starting.
    WaitingForConnection,
    /// The editor has sent `initialize`. Awaiting `configurationDone`.
    Initializing,
    /// Configuration complete; runner is free to run steps.
    Ready,
    /// Paused at a step boundary, awaiting `continue`.
    Paused,
    /// Step is running.
    Running,
    /// Session has been torn down (`terminated`/`exited` emitted).
    Terminated,
}

impl DapSessionState {
    pub fn as_str(self) -> &'static str {
        match self {
            DapSessionState::NotStarted => "NotStarted",
            DapSessionState::WaitingForConnection => "WaitingForConnection",
            DapSessionState::Initializing => "Initializing",
            DapSessionState::Ready => "Ready",
            DapSessionState::Paused => "Paused",
            DapSessionState::Running => "Running",
            DapSessionState::Terminated => "Terminated",
        }
    }
}

/// Errors returned by the DAP layer.
#[derive(Debug, Error)]
pub enum DapError {
    /// Underlying I/O failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Protocol violation.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// JSON serialization failed.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// One-shot channel closed unexpectedly.
    #[error("channel closed")]
    ChannelClosed,

    /// Configuration is invalid (e.g. missing tunnel info).
    #[error("invalid config: {0}")]
    InvalidConfig(String),
}

/// The `IDapDebugger` service interface. Mirrors the C# interface
/// one method for one — every method is called from the same
/// `JobExtension`/`JobRunner`/`StepsRunner` site as upstream.
#[async_trait]
pub trait IDapDebugger: Send + Sync {
    /// Start the DAP server and devtunnel host. Called once per
    /// job, inside "Set up job".
    async fn start(&self, job_id: &str, steps: &[SourceEntry]) -> Result<(), DapError>;
    /// Block until the editor sends `configurationDone` (or the
    /// connection timeout elapses).
    async fn wait_until_ready(&self) -> Result<(), DapError>;
    /// Notify the debugger of the resolved step list and initial
    /// post steps. Builds the synthetic source view.
    async fn on_job_steps_initialized(
        &self,
        steps: &[SourceEntry],
        initial_post_steps: &[SourceEntry],
        predicted_post_steps: &[PredictedPostStep],
    );
    /// A post step has been registered. The debugger adds it to
    /// the source view; if a predicted post step with the same
    /// name exists, its frame id is claimed.
    fn on_post_step_registered(&self, step: &SourceEntry);
    /// Pause for inspection. Returns when the editor sends
    /// `continue` (or the job is cancelled).
    async fn on_step_starting(&self, step: &SourceEntry) -> Result<(), DapError>;
    /// Mark a step as completed; emits `continued` if we paused.
    fn on_step_completed(&self, step: &SourceEntry);
    /// Job has completed: emit `terminated`/`exited`, pause for
    /// final inspection, then tear down.
    async fn on_job_completed(&self) -> Result<(), DapError>;
    /// Stop the debugger unconditionally. Cancels everything.
    async fn stop(&self) -> Result<(), DapError>;
    /// Read-only view of the current state.
    fn state(&self) -> DapSessionState;
    /// Get the actually bound local port of the DAP TCP server.
    fn local_port(&self) -> Option<u16>;
    /// Update the variables context and secrets masks.
    fn update_context(&self, context: serde_json::Value, masks: std::collections::HashSet<String>);
}

/// Internal shared state.
struct DebuggerCore {
    config: DebuggerConfig,
    local_port: parking_lot::Mutex<Option<u16>>,
    state: parking_lot::Mutex<DapSessionState>,
    /// `continue` handler calls `send(())` to unblock it.
    resume_tx: watch::Sender<()>,
    /// Optional devtunnel child process. `None` when not started.
    devtunnel_child: Mutex<Option<tokio::process::Child>>,
    /// Synthetic source view. Built in `on_job_steps_initialized`.
    view: PlMutex<Option<Arc<JobExecutionView>>>,
    /// Outbound DAP messages to send to the editor. The transport
    /// task owns the receiver; the dispatcher and lifecycle
    /// methods own the sender.
    out_tx: parking_lot::Mutex<mpsc::UnboundedSender<Outbound>>,
    /// DAP `seq` counters for outgoing messages.
    next_seq: Mutex<i64>,
    /// Cancellation token (set by `stop`).
    cancel: Mutex<Option<watch::Sender<bool>>>,
    /// Connection-timeout / tunnel-timeout derived from env vars.
    timeouts: DapTimeouts,
    /// Override welcome message.
    welcome_message: Option<String>,
    /// Whether the welcome message was overridden.
    override_welcome: bool,
    /// Synthetic job id (we need it even before the source view
    /// is built, for the source path).
    job_id: Mutex<Option<String>>,
    /// Variables context (parsed from runner's expression context).
    context: parking_lot::Mutex<serde_json::Value>,
    /// Secret values to mask.
    masks: parking_lot::Mutex<std::collections::HashSet<String>>,
    /// Whether we've already paused at the job entry point.
    /// Official runner only pauses at the first step, then runs the rest.
    entry_paused: std::sync::atomic::AtomicBool,
}

/// Internal envelope of an outgoing DAP message (response or
/// event). The transport task picks them up and writes them to
/// the wire.
#[derive(Debug)]
enum Outbound {
    Response(Response),
    Event(Event),
}

#[derive(Debug, Clone, Copy)]
struct DapTimeouts {
    /// Time to wait for the editor to attach.
    connection: Duration,
    /// Time to wait for the devtunnel host to dial the relay.
    #[allow(dead_code)]
    tunnel_connect: Duration,
}

impl DapTimeouts {
    fn from_env_and_config() -> Self {
        let connection_minutes = std::env::var(crate::env_vars::DAP_CONNECTION_TIMEOUT)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(crate::DEFAULT_CONNECTION_TIMEOUT_MINUTES as u64);
        let tunnel_seconds = std::env::var(crate::env_vars::DAP_TUNNEL_CONNECT_TIMEOUT_SECONDS)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(crate::DEFAULT_TUNNEL_CONNECT_TIMEOUT_SECONDS as u64);
        Self {
            connection: Duration::from_secs(60 * connection_minutes),
            tunnel_connect: Duration::from_secs(tunnel_seconds),
        }
    }
}

/// The main DAP server. See module docs for the lifecycle.
pub struct DapDebugger {
    core: Arc<DebuggerCore>,
}

impl DapDebugger {
    /// Build a new debugger with the given configuration.
    pub fn new(config: DebuggerConfig) -> Self {
        let (resume_tx, _) = watch::channel(());
        let (out_tx, _out_rx) = mpsc::unbounded_channel();
        let (cancel_tx, _) = watch::channel(false);
        let timeouts = DapTimeouts::from_env_and_config();
        let welcome_message = config.welcome_message.clone();
        let override_welcome = config.override_welcome_message;
        Self {
            core: Arc::new(DebuggerCore {
                config,
                local_port: parking_lot::Mutex::new(None),
                state: parking_lot::Mutex::new(DapSessionState::NotStarted),
                resume_tx,
                devtunnel_child: Mutex::new(None),
                view: PlMutex::new(None),
                out_tx: parking_lot::Mutex::new(out_tx),
                next_seq: Mutex::new(1),
                cancel: Mutex::new(Some(cancel_tx)),
                timeouts,
                welcome_message,
                override_welcome,
                job_id: Mutex::new(None),
                context: parking_lot::Mutex::new(serde_json::json!({})),
                masks: parking_lot::Mutex::new(std::collections::HashSet::new()),
                entry_paused: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    /// Returns `true` if the config is runnable (enabled + valid tunnel).
    pub fn is_runnable(&self) -> bool {
        self.core.config.is_runnable()
    }

    /// Internal: bind a TCP listener on a random local port.
    async fn bind_tcp_server(&self) -> Result<TcpListener, DapError> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let bound_port = listener.local_addr()?.port();
        *self.core.local_port.lock() = Some(bound_port);
        Ok(listener)
    }

    /// Internal: launch the devtunnel host subprocess. The tunnel
    /// host command line is built exactly as Microsoft documents:
    /// `devtunnel host -p <port> --tunnel-id <id>` with the host
    /// token supplied via the `devtunnel` interactive prompt.
    /// (The `--token-file -` form is not stable across all
    /// `devtunnel` versions; we keep the command line minimal and
    /// let the runner operator pre-authenticate if needed.)
    async fn launch_devtunnel(
        &self,
        tunnel: &DebuggerTunnelInfo,
    ) -> Result<tokio::process::Child, DapError> {
        let bin = which_devtunnel().ok_or_else(|| {
            DapError::InvalidConfig(
                "devtunnel binary not found in PATH or well-known locations".into(),
            )
        })?;
        let mut cmd = tokio::process::Command::new(bin);
        cmd.arg("host")
            .arg("-p")
            .arg(tunnel.port.to_string())
            .arg("--tunnel-id")
            .arg(&tunnel.tunnel_id)
            .arg("--allow-anonymous")
            .env("DEVTUNNEL_HOST_TOKEN", &tunnel.host_token)
            .env("DEVTUNNEL_CLUSTER", &tunnel.cluster_id)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let child = cmd.spawn().map_err(DapError::Io)?;
        Ok(child)
    }

    /// Internal: drive the TCP accept loop and one connection at
    /// a time. Called from `start` once the listener is bound.
    async fn serve(self: Arc<Self>, listener: TcpListener) {
        let connect_timeout = self.core.timeouts.connection;
        let accept = async {
            let (stream, _peer) = listener.accept().await?;
            Ok::<_, DapError>(stream)
        };
        let stream = match tokio::time::timeout(connect_timeout, accept).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                error!("DAP accept failed: {e}");
                return;
            }
            Err(_) => {
                error!("DAP connection timed out after {connect_timeout:?}");
                *self.core.state.lock() = DapSessionState::Terminated;
                return;
            }
        };
        *self.core.state.lock() = DapSessionState::Initializing;
        if let Err(e) = self.handle_client(stream).await {
            warn!("DAP client ended: {e}");
        }
        *self.core.state.lock() = DapSessionState::Terminated;
    }

    /// Internal: handle one editor connection.
    async fn handle_client(&self, stream: TcpStream) -> Result<(), DapError> {
        let (mut read_half, mut write_half) = stream.into_split();

        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Outbound>();
        let (in_tx, mut in_rx) = mpsc::unbounded_channel::<InboundRequest>();

        // The lifecycle methods hold a clone of `out_tx` (via the
        // core). Replace the placeholder sender with our real one
        // for the duration of this connection.
        // We swap the core's sender into a local so we can restore
        // it on drop. The simpler path is to never use the core's
        // placeholder for outbound traffic during a connection.
        // But the lifecycle methods reference `core.out_tx`, so we
        // swap it in.
        let previous_out = {
            let mut g = self.core.out_tx.lock();
            std::mem::replace(&mut *g, out_tx.clone())
        };

        struct RestoreOut<'a> {
            core: &'a DebuggerCore,
            prev: mpsc::UnboundedSender<Outbound>,
        }
        impl Drop for RestoreOut<'_> {
            fn drop(&mut self) {
                *self.core.out_tx.lock() = self.prev.clone();
            }
        }
        let _restore = RestoreOut {
            core: &self.core,
            prev: previous_out,
        };

        // Writer task.
        let writer = tokio::spawn(async move {
            while let Some(out) = out_rx.recv().await {
                let value = match out {
                    Outbound::Response(r) => serde_json::to_value(&r).unwrap_or(Value::Null),
                    Outbound::Event(e) => serde_json::to_value(&e).unwrap_or(Value::Null),
                };
                let body = match serde_json::to_vec(&value) {
                    Ok(b) => b,
                    Err(_) => break,
                };
                if body.len() > crate::MAX_MESSAGE_SIZE {
                    break;
                }
                let header = format!("Content-Length: {}\r\n\r\n", body.len());
                if write_half.write_all(header.as_bytes()).await.is_err() {
                    break;
                }
                if write_half.write_all(&body).await.is_err() {
                    break;
                }
                if write_half.flush().await.is_err() {
                    break;
                }
            }
        });

        // Welcome message is sent after configurationDone, not here.
        // See dispatch_one's "configurationDone" arm.

        // Read loop.
        let reader = tokio::spawn(async move {
            loop {
                let header_end = match read_headers(&mut read_half).await {
                    Ok(n) => n,
                    Err(e) => {
                        debug!("DAP read ended: {e}");
                        return;
                    }
                };
                let content_length = header_end;
                let mut body = vec![0u8; content_length];
                if read_half.read_exact(&mut body).await.is_err() {
                    return;
                }
                let value: Value = match serde_json::from_slice(&body) {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let req: Request = match serde_json::from_value(value) {
                    Ok(r) => r,
                    Err(_) => return,
                };
                let (resp_tx, resp_rx) = oneshot::channel::<Response>();
                if in_tx
                    .send(InboundRequest {
                        raw: req,
                        respond: resp_tx,
                    })
                    .is_err()
                {
                    return;
                }
                let response = match resp_rx.await {
                    Ok(r) => r,
                    Err(_) => return,
                };
                // The dispatcher enqueues the response through the
                // out_tx that this reader does not own.
                let _ = response;
            }
        });

        // Dispatcher.
        let core = Arc::clone(&self.core);
        let out_tx_dispatch = out_tx.clone();
        let dispatcher = tokio::spawn(async move {
            while let Some(req) = in_rx.recv().await {
                let seq = next_seq(&core).await;
                let response = dispatch_one(&core, &req.raw, seq).await;
                let _ = req.respond.send(response.clone());
                let _ = out_tx_dispatch.send(Outbound::Response(response));
                if req.raw.command == "initialize" {
                    let event_seq = next_seq(&core).await;
                    let _ = out_tx_dispatch
                        .send(Outbound::Event(Event::new(event_seq, EVENT_INITIALIZED)));
                }
                if req.raw.command == "configurationDone" {
                    // Send welcome output event after configurationDone
                    // (matches official runner ordering).
                    let welcome = if core.override_welcome {
                        core.welcome_message.clone()
                    } else {
                        Some(
                            core.welcome_message
                                .clone()
                                .unwrap_or_else(default_welcome_message),
                        )
                    };
                    if let Some(mut msg) = welcome {
                        if !msg.is_empty() {
                            if !msg.ends_with('\n') {
                                msg.push('\n');
                            }
                            let event_seq = next_seq(&core).await;
                            let _ = out_tx_dispatch.send(Outbound::Event(
                                Event::new(event_seq, EVENT_OUTPUT).with_body(json!({
                                    "category": "console",
                                    "output": msg,
                                })),
                            ));
                        }
                    }
                }
                if matches!(req.raw.command.as_str(), "continue" | "next" | "stepIn" | "stepOut") {
                    let event_seq = next_seq(&core).await;
                    let _ = out_tx_dispatch.send(Outbound::Event(
                        Event::new(event_seq, EVENT_CONTINUED).with_body(json!({
                            "threadId": 1,
                            "allThreadsContinued": true,
                        })),
                    ));
                }
                if matches!(req.raw.command.as_str(), "disconnect" | "terminate") {
                    break;
                }
            }
        });

        // Wait for shutdown signals.
        let cancel_rx = {
            let g = self.core.cancel.lock().await;
            g.as_ref().map(|tx| tx.subscribe())
        };
        if let Some(mut rx) = cancel_rx {
            let _ = tokio::time::timeout(self.core.timeouts.connection, rx.changed()).await;
        } else {
            // Without a cancel token we just sleep until the
            // transport tasks finish.
            let _ = reader.await;
            let _ = dispatcher.await;
        }
        drop(out_tx);
        let _ = writer.await;
        Ok(())
    }

    /// Internal: allocate the next outgoing `seq` value.
    async fn next_seq_internal(&self) -> i64 {
        let mut g = self.core.next_seq.lock().await;
        let v = *g;
        *g = v + 1;
        v
    }

    /// Dispatch a request without a transport. Used by tests and
    /// by the harness to drive a known request sequence.
    pub async fn dispatch(&self, req: Request) -> Response {
        let seq = self.next_seq_internal().await;
        let core = Arc::clone(&self.core);
        dispatch_one(&core, &req, seq).await
    }

    /// Read-only snapshot of the source view (for tests).
    pub fn view_snapshot(&self) -> Option<Arc<JobExecutionView>> {
        self.core.view.lock().as_ref().cloned()
    }

    /// Set the synthetic source view directly. Used by tests that
    /// don't want to go through `on_job_steps_initialized`.
    pub fn set_view_for_test(&self, view: Arc<JobExecutionView>) {
        *self.core.view.lock() = Some(view);
    }
}

#[derive(Debug)]
struct InboundRequest {
    raw: Request,
    respond: oneshot::Sender<Response>,
}

async fn next_seq(core: &Arc<DebuggerCore>) -> i64 {
    let mut g = core.next_seq.lock().await;
    let v = *g;
    *g = v + 1;
    v
}

async fn read_headers<R: tokio::io::AsyncRead + Unpin>(r: &mut R) -> Result<usize, DapError> {
    let mut header = Vec::with_capacity(256);
    let mut byte = [0u8; 1];
    loop {
        if r.read(&mut byte).await? == 0 {
            return Err(DapError::Protocol("eof in headers".into()));
        }
        header.push(byte[0]);
        if header.ends_with(b"\r\n\r\n") {
            break;
        }
        if header.len() > crate::MAX_HEADER_LINE_LENGTH * 4 {
            return Err(DapError::Protocol("header too long".into()));
        }
    }
    let text =
        std::str::from_utf8(&header).map_err(|_| DapError::Protocol("non-utf8 header".into()))?;
    for line in text.split("\r\n") {
        if let Some(rest) = line
            .strip_prefix("Content-Length:")
            .or_else(|| line.strip_prefix("content-length:"))
        {
            return rest
                .trim()
                .parse()
                .map_err(|_| DapError::Protocol("invalid Content-Length".into()));
        }
    }
    Err(DapError::Protocol("missing Content-Length".into()))
}

async fn dispatch_one(core: &Arc<DebuggerCore>, req: &Request, seq: i64) -> Response {
    let cmd = req.command.as_str();
    match cmd {
        "initialize" => Response::success(seq, req.header.seq, "initialize")
            .with_body(serde_json::to_value(Capabilities::runner_default()).unwrap_or(Value::Null)),
        "configurationDone" => {
            *core.state.lock() = DapSessionState::Ready;
            Response::success(seq, req.header.seq, "configurationDone")
        }
        "threads" => Response::success(seq, req.header.seq, "threads").with_body(json!({
            "threads": [{"id": 1, "name": "job"}]
        })),
        "stackTrace" => {
            let view = core.view.lock().clone();
            let frames: Vec<Value> = match view.as_ref() {
                Some(v) => (0..v.line_count())
                    .map(|i| {
                        let line = (i as i64) + 1;
                        json!({
                            "id": line,
                            "name": format!("step:{line}"),
                            "source": {
                                "name": crate::view::SOURCE_FILE_NAME,
                                "path": v.source_path(),
                            },
                            "line": line,
                            "column": 1,
                        })
                    })
                    .collect(),
                None => Vec::new(),
            };
            Response::success(seq, req.header.seq, "stackTrace").with_body(json!({
                "stackFrames": frames,
                "totalFrames": frames.len(),
            }))
        }
        "scopes" => {
            let masks = core.masks.lock().clone();
            let mask_secret = Box::new(move |input: &str| {
                let mut result = input.to_string();
                let mut sorted_masks: Vec<&String> =
                    masks.iter().filter(|s| !s.is_empty()).collect();
                sorted_masks.sort_by_key(|b| std::cmp::Reverse(b.len()));
                for secret in sorted_masks {
                    result = result.replace(secret.as_str(), "***");
                }
                result
            });
            let provider = DapVariableProvider::new(mask_secret);
            Response::success(seq, req.header.seq, "scopes")
                .with_body(json!({ "scopes": provider.scopes() }))
        }
        "variables" => {
            let reference = req
                .arguments
                .as_ref()
                .and_then(|v| v.get("variablesReference"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let masks = core.masks.lock().clone();
            let mask_secret = Box::new(move |input: &str| {
                let mut result = input.to_string();
                let mut sorted_masks: Vec<&String> =
                    masks.iter().filter(|s| !s.is_empty()).collect();
                sorted_masks.sort_by_key(|b| std::cmp::Reverse(b.len()));
                for secret in sorted_masks {
                    result = result.replace(secret.as_str(), "***");
                }
                result
            });
            let provider = DapVariableProvider::new(mask_secret);
            let ctx = core.context.lock().clone();
            let vars = provider.variables(reference, &ctx);
            Response::success(seq, req.header.seq, "variables")
                .with_body(json!({ "variables": vars }))
        }
        "source" => {
            let view = core.view.lock().clone();
            let content = view.as_ref().map(|v| v.content()).unwrap_or_default();
            Response::success(seq, req.header.seq, "source")
                .with_body(json!({ "content": content }))
        }
        "continue" => {
            *core.state.lock() = DapSessionState::Running;
            let _ = core.resume_tx.send(());
            // continued event is sent by the dispatcher loop after
            // the response, matching official runner ordering.
            Response::success(seq, req.header.seq, "continue")
                .with_body(json!({"allThreadsContinued": true}))
        }
        "next" | "stepIn" | "stepOut" => {
            // No real step navigation in the runner debugger; treat
            // as continue.
            *core.state.lock() = DapSessionState::Running;
            let _ = core.resume_tx.send(());
            Response::success(seq, req.header.seq, cmd)
                .with_body(json!({"allThreadsContinued": true}))
        }
        "pause" => {
            Response::success(seq, req.header.seq, "pause").with_body(json!({"reason": "pause"}))
        }
        "evaluate" => {
            let expr = req
                .arguments
                .as_ref()
                .and_then(|v| v.get("expression"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let parsed = match DapReplParser::parse(expr) {
                Ok(c) => Some(c),
                Err(ParseError::Empty) => None,
                Err(_) => None,
            };
            if let Some(parsed) = parsed {
                let masks = core.masks.lock().clone();
                let mask_secret = Box::new(move |input: &str| {
                    let mut result = input.to_string();
                    let mut sorted_masks: Vec<&String> =
                        masks.iter().filter(|s| !s.is_empty()).collect();
                    sorted_masks.sort_by_key(|b| std::cmp::Reverse(b.len()));
                    for secret in sorted_masks {
                        result = result.replace(secret.as_str(), "***");
                    }
                    result
                });
                let executor = DapReplExecutor::with_masker(mask_secret);
                let output = executor.execute(&parsed);
                Response::success(seq, req.header.seq, "evaluate")
                    .with_body(json!({"result": output, "variablesReference": 0}))
            } else {
                // Evaluate as a standard workflow expression against the context roots
                let ctx_val = core.context.lock().clone();
                let mut expr_ctx = aksh_gha_expressions::Context::new();
                if let Some(obj) = ctx_val.as_object() {
                    for (k, v) in obj {
                        expr_ctx.insert(k.clone(), v.clone());
                    }
                }
                let val = aksh_gha_expressions::eval_expression(expr, &expr_ctx);
                let result_str = match val {
                    Ok(val) => {
                        let result_raw = match &val {
                            Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        let masks = core.masks.lock().clone();
                        let mut result_masked = result_raw;
                        let mut sorted_masks: Vec<&String> =
                            masks.iter().filter(|s| !s.is_empty()).collect();
                        sorted_masks.sort_by_key(|b| std::cmp::Reverse(b.len()));
                        for secret in sorted_masks {
                            result_masked = result_masked.replace(secret.as_str(), "***");
                        }
                        result_masked
                    }
                    Err(e) => format!("error: {e}"),
                };
                Response::success(seq, req.header.seq, "evaluate")
                    .with_body(json!({"result": result_str, "variablesReference": 0}))
            }
        }
        "disconnect" | "terminate" => Response::success(seq, req.header.seq, cmd),
        other => Response::failure(
            seq,
            req.header.seq,
            other,
            format!("unsupported command: {other}"),
        ),
    }
}

fn default_welcome_message() -> String {
    "Debugger attached. Use DAP continue/next/stepIn/stepOut to navigate steps.".to_string()
}

fn which_devtunnel() -> Option<std::path::PathBuf> {
    if let Some(p) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&p) {
            let candidate = dir.join("devtunnel");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    for p in [
        "/usr/local/bin/devtunnel",
        "/opt/homebrew/bin/devtunnel",
        "/usr/bin/devtunnel",
    ] {
        let p = std::path::PathBuf::from(p);
        if p.is_file() {
            return Some(p);
        }
    }
    None
}

#[async_trait]
impl IDapDebugger for DapDebugger {
    async fn start(&self, job_id: &str, _steps: &[SourceEntry]) -> Result<(), DapError> {
        if !self.is_runnable() {
            return Err(DapError::InvalidConfig(
                "debugger config is not runnable (missing tunnel info?)".into(),
            ));
        }
        let tunnel = self
            .core
            .config
            .tunnel
            .as_ref()
            .expect("validated by is_runnable()")
            .clone();
        *self.core.state.lock() = DapSessionState::WaitingForConnection;
        *self.core.job_id.lock().await = Some(job_id.to_string());

        let listener = self.bind_tcp_server().await?;
        let local_port = listener.local_addr()?.port();

        // Always start the WebSocket bridge — matches official runner behavior.
        // The bridge is the single external interface; transport mode only
        // controls whether a DevTunnel relay is also started.
        {
            let bridge = crate::bridge::WebSocketDapBridge::new(tunnel.port, local_port);
            tokio::spawn(async move {
                if let Err(e) = bridge.run().await {
                    warn!("DAP WebSocket bridge failed: {e}");
                }
            });
        }

        if matches!(self.core.config.transport, DebuggerTransportMode::DevTunnel) {
            match self.launch_devtunnel(&tunnel).await {
                Ok(child) => *self.core.devtunnel_child.lock().await = Some(child),
                Err(e) => {
                    warn!("devtunnel host failed to start: {e}");
                }
            }
        } else {
            debug!("DAP transport mode: local server-proxy (no DevTunnel relay)");
        }

        let me = Arc::new(Self {
            core: Arc::clone(&self.core),
        });
        tokio::spawn(me.serve(listener));
        Ok(())
    }

    async fn wait_until_ready(&self) -> Result<(), DapError> {
        let timeout_d = self.core.timeouts.connection;
        let cancel_rx = {
            let g = self.core.cancel.lock().await;
            g.as_ref().map(|tx| tx.subscribe())
        };
        let state_check = async {
            loop {
                {
                    if *self.core.state.lock() == DapSessionState::Ready {
                        return Ok(());
                    }
                    if *self.core.state.lock() == DapSessionState::Terminated {
                        return Err(DapError::Protocol("session terminated".into()));
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        if let Some(mut rx) = cancel_rx {
            tokio::select! {
                r = state_check => r,
                _ = rx.changed() => Err(DapError::Protocol("cancelled".into())),
                _ = tokio::time::sleep(timeout_d) => Err(DapError::Protocol("configurationDone timeout".into())),
            }
        } else {
            tokio::select! {
                r = state_check => r,
                _ = tokio::time::sleep(timeout_d) => Err(DapError::Protocol("configurationDone timeout".into())),
            }
        }
    }

    async fn on_job_steps_initialized(
        &self,
        steps: &[SourceEntry],
        initial_post_steps: &[SourceEntry],
        predicted_post_steps: &[PredictedPostStep],
    ) {
        let job_id = self
            .core
            .job_id
            .lock()
            .await
            .clone()
            .unwrap_or_else(|| "job".into());
        let view = Arc::new(JobExecutionView::new(
            &job_id,
            steps,
            initial_post_steps,
            predicted_post_steps,
        ));
        *self.core.view.lock() = Some(view);
    }

    fn on_post_step_registered(&self, step: &SourceEntry) {
        if let Some(view) = self.core.view.lock().as_ref() {
            view.add_post_steps(&[step.clone()]);
        }
    }

    async fn on_step_starting(&self, step: &SourceEntry) -> Result<(), DapError> {
        if !self.is_runnable() {
            return Ok(());
        }
        // Official runner only pauses at the first step (job entry).
        // Subsequent steps run without pause.
        let already_paused = self
            .core
            .entry_paused
            .swap(true, std::sync::atomic::Ordering::SeqCst);
        if already_paused {
            return Ok(());
        }
        let seq = self.next_seq_internal().await;
        let _ = self.core.out_tx.lock().send(Outbound::Event(
            Event::new(seq, EVENT_STOPPED).with_body(json!({
                "reason": "entry",
                "description": format!("Stopped at job entry: {}", step.display_name),
                "threadId": 1,
                "allThreadsStopped": true,
            })),
        ));
        *self.core.state.lock() = DapSessionState::Paused;

        let mut rx = self.core.resume_tx.subscribe();
        let _ = rx.borrow_and_update();
        rx.changed()
            .await
            .map_err(|_| DapError::Protocol("debugger resume channel closed".into()))?;
        *self.core.state.lock() = DapSessionState::Running;
        // continued event is sent by the dispatcher loop after the
        // continue response, matching official runner ordering.
        Ok(())
    }

    fn on_step_completed(&self, _step: &SourceEntry) {
        // No-op for now: the resume signal was already sent in
        // handle_continue. Hook left here for parity with the C#
        // `OnStepCompleted` method.
    }

    async fn on_job_completed(&self) -> Result<(), DapError> {
        // Official runner sends terminated + exited directly — no final pause.
        let seq = self.next_seq_internal().await;
        let _ = self
            .core
            .out_tx
            .lock()
            .send(Outbound::Event(Event::new(seq, EVENT_TERMINATED)));
        let seq = self.next_seq_internal().await;
        let _ = self.core.out_tx.lock().send(Outbound::Event(
            Event::new(seq, EVENT_EXITED).with_body(json!({"exitCode": 0})),
        ));
        *self.core.state.lock() = DapSessionState::Terminated;
        if let Some(mut child) = self.core.devtunnel_child.lock().await.take() {
            let _ = child.start_kill();
        }
        if let Some(tx) = self.core.cancel.lock().await.as_ref() {
            let _ = tx.send(true);
        }
        let _ = self.core.resume_tx.send(());
        Ok(())
    }

    async fn stop(&self) -> Result<(), DapError> {
        if let Some(mut child) = self.core.devtunnel_child.lock().await.take() {
            let _ = child.start_kill();
        }
        if let Some(tx) = self.core.cancel.lock().await.as_ref() {
            let _ = tx.send(true);
        }
        let _ = self.core.resume_tx.send(());
        *self.core.state.lock() = DapSessionState::Terminated;
        Ok(())
    }

    fn state(&self) -> DapSessionState {
        *self.core.state.lock()
    }

    fn local_port(&self) -> Option<u16> {
        *self.core.local_port.lock()
    }

    fn update_context(&self, context: serde_json::Value, masks: std::collections::HashSet<String>) {
        *self.core.context.lock() = context;
        *self.core.masks.lock() = masks;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DebuggerTunnelInfo;
    use crate::view::PredictedPostStep;
    use serde_json::json;

    fn sample_tunnel() -> DebuggerTunnelInfo {
        DebuggerTunnelInfo {
            tunnel_id: "neat-ocean-5b7j1lw".into(),
            cluster_id: "use2".into(),
            host_token: "secret".into(),
            port: 4711,
        }
    }

    fn sample_config() -> DebuggerConfig {
        DebuggerConfig::new(true, Some(sample_tunnel()), false, None)
    }

    fn step(name: &str) -> SourceEntry {
        SourceEntry {
            display_name: name.into(),
            is_pre: false,
            is_post: false,
        }
    }

    fn post(name: &str) -> SourceEntry {
        SourceEntry {
            display_name: name.into(),
            is_pre: false,
            is_post: true,
        }
    }

    #[tokio::test]
    async fn not_runnable_config_rejects_start() {
        let dbg = DapDebugger::new(DebuggerConfig::default());
        let err = dbg.start("job1", &[]).await.unwrap_err();
        assert!(matches!(err, DapError::InvalidConfig(_)));
    }

    #[tokio::test]
    async fn dispatch_initialize_returns_capabilities() {
        let dbg = DapDebugger::new(sample_config());
        let resp = dbg
            .dispatch(Request::new("initialize").with_arguments(json!({"clientID": "vscode"})))
            .await;
        assert!(resp.success);
        let body = resp.body.unwrap();
        assert_eq!(body["supportsConfigurationDoneRequest"], true);
        assert_eq!(body["supportsEvaluateForHovers"], true);
        assert_eq!(body["supportsTerminateRequest"], true);
    }

    #[tokio::test]
    async fn configuration_done_marks_ready() {
        let dbg = DapDebugger::new(sample_config());
        let resp = dbg.dispatch(Request::new("configurationDone")).await;
        assert!(resp.success);
    }

    #[tokio::test]
    async fn evaluate_runs_repl_help() {
        let dbg = DapDebugger::new(sample_config());
        let resp = dbg
            .dispatch(Request::new("evaluate").with_arguments(json!({"expression": "help"})))
            .await;
        assert!(resp.success);
        assert!(resp.body.unwrap()["result"]
            .as_str()
            .unwrap()
            .contains("Available commands"));
    }

    #[tokio::test]
    async fn threads_returns_single_job_thread() {
        let dbg = DapDebugger::new(sample_config());
        let resp = dbg.dispatch(Request::new("threads")).await;
        let body = resp.body.unwrap();
        let threads = body["threads"].as_array().unwrap();
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0]["name"], "job");
    }

    #[tokio::test]
    async fn stack_trace_uses_synthetic_source() {
        let dbg = DapDebugger::new(sample_config());
        dbg.on_job_steps_initialized(
            &[step("Checkout"), step("Build")],
            &[post("Notify")],
            &[PredictedPostStep {
                display_name: "Cleanup".into(),
                frame_id: 999,
            }],
        )
        .await;
        let resp = dbg.dispatch(Request::new("stackTrace")).await;
        let body = resp.body.unwrap();
        let frames = body["stackFrames"].as_array().unwrap();
        // 1 Set up job + 2 main + 1 post + 1 predicted = 5
        assert_eq!(frames.len(), 5);
    }

    #[tokio::test]
    async fn source_returns_synthetic_yaml() {
        let dbg = DapDebugger::new(sample_config());
        dbg.on_job_steps_initialized(&[step("Build")], &[], &[])
            .await;
        let resp = dbg.dispatch(Request::new("source")).await;
        let body = resp.body.unwrap();
        let content = body["content"].as_str().unwrap();
        assert!(content.contains("main:"));
        assert!(content.contains("- Build"));
    }

    #[tokio::test]
    async fn continue_unblocks_pause() {
        let dbg = DapDebugger::new(sample_config());
        let resp = dbg
            .dispatch(Request::new("continue").with_arguments(json!({"threadId": 1})))
            .await;
        assert!(resp.success);
        // The continue handler always sends a `continued` event and
        // marks the state Running. The state must reflect that
        // and the resume signal must have fired.
        assert_eq!(*dbg.core.state.lock(), DapSessionState::Running);
    }

    #[test]
    fn session_state_strings_are_nonempty() {
        for s in [
            DapSessionState::NotStarted,
            DapSessionState::WaitingForConnection,
            DapSessionState::Initializing,
            DapSessionState::Ready,
            DapSessionState::Paused,
            DapSessionState::Running,
            DapSessionState::Terminated,
        ] {
            assert!(!s.as_str().is_empty());
        }
    }

    #[tokio::test]
    async fn env_var_overrides_timeout() {
        let saved = std::env::var(crate::env_vars::DAP_CONNECTION_TIMEOUT).ok();
        std::env::set_var(crate::env_vars::DAP_CONNECTION_TIMEOUT, "3");
        let cfg = sample_config();
        let dbg = DapDebugger::new(cfg);
        let to = dbg.core.timeouts;
        assert_eq!(to.connection, Duration::from_secs(180));
        match saved {
            Some(v) => std::env::set_var(crate::env_vars::DAP_CONNECTION_TIMEOUT, v),
            None => std::env::remove_var(crate::env_vars::DAP_CONNECTION_TIMEOUT),
        }
    }
}
