# aksh DAP Protocol Implementation Review — 2026-07-08

Scope: Review the current implementation of the Debug Adapter Protocol (DAP) in both the runner (`crates/aksh-runner`, `crates/aksh-dap`) and the server (`crates/aksh-runner-server`) against the official `actions/runner` behavior.

## Executive Summary

The foundational framing, message DTO serialization, and REPL parsing/execution logic have been mostly ported cleanly. However, critical runtime logic bugs, protocol omissions, and context integration gaps currently exist. If a debug session is initiated, the debugger will hang or display empty panels in the client (VS Code). 

Below is the detailed list of disparities identified.

## Disparities Found

### DR-DAP-001 — Outbound Event Sender is Disconnected (Silent Event Drop)

- **Severity**: Blocker
- **Files**: `crates/aksh-dap/src/debugger.rs`
- **Finding**: In `DapDebugger::new`, the core is instantiated with a placeholder sender whose receiver is immediately discarded:
  ```rust
  let (out_tx, _out_rx) = mpsc::unbounded_channel();
  ```
  In `handle_client`, a connection-specific channel is created, but the core's `out_tx` is never swapped or updated with this new sender.
- **Why it matters**: All lifecycle events (such as `EVENT_THREAD` when starting, `EVENT_STOPPED` when pausing on step, `EVENT_CONTINUED` on resume, and `EVENT_TERMINATED`/`EVENT_EXITED` on job completion) are sent to `self.core.out_tx`. Because this channel is disconnected, all events are silently dropped, preventing the VS Code debug client from ever knowing when the runner has paused or finished. The debugger UI hangs indefinitely.
- **Fix**: Wrap `out_tx` in `DebuggerCore` in a `parking_lot::Mutex` and overwrite it in `handle_client` upon active connection, restoring it on drop.

### DR-DAP-002 — Empty Variables & Scopes (Missing Context Integration)

- **Severity**: Blocker / Major
- **Files**: `crates/aksh-dap/src/debugger.rs`, `crates/aksh-dap/src/variables.rs`
- **Finding**: When `dispatch_one` receives a `variables` or `evaluate` command, it instantiates a default `DapVariableProvider` and passes a completely blank JSON context:
  ```rust
  let provider = DapVariableProvider::default();
  let ctx = Value::Object(Default::default());
  let vars = provider.variables(reference, &ctx);
  ```
- **Why it matters**: Since the debugger has no reference or access to the active job context or expression values, all scopes (`github`, `env`, `runner`, `job`, `steps`, `secrets`) are displayed as empty in the client. The user cannot inspect environment variables, step inputs/outputs, or workflow secrets.
- **Fix**: Update `IDapDebugger` and `DebuggerCore` to hold a thread-safe reference to the active `JobContext` or its serialized expression variables, updating it on step transitions, and use it when calling `provider.variables(...)`.

### DR-DAP-003 — Welcome/Output Message is Dead Code (Never Sent)

- **Severity**: Major
- **Files**: `crates/aksh-dap/src/debugger.rs`
- **Finding**: `default_welcome_message()` is defined but never called or sent to the editor.
- **Why it matters**: The editor's debug console remains empty upon attachment, and the user receives no verification or instructions that the debugger is successfully connected.
- **Fix**: Send a DAP `output` event containing the welcome message to `out_tx` inside `handle_client` right after connection establishment.

### DR-DAP-004 — Missing `initialized` Event

- **Severity**: Major
- **Files**: `crates/aksh-dap/src/debugger.rs`
- **Finding**: After responding to the `initialize` command, the debugger does not emit the `initialized` event.
- **Why it matters**: Violates standard DAP sequencing. A standard client expects the `initialized` event to know when it can begin sending configuration commands (such as setting breakpoints and sending `configurationDone`). Without this, some DAP clients will block.
- **Fix**: Emit `Outbound::Event(Event::new(seq, EVENT_INITIALIZED))` when handling the `initialize` request.

### DR-DAP-005 — Step Hooks Parameter Typing Disparity

- **Severity**: Minor / Architectural
- **Files**: `crates/aksh-dap/src/debugger.rs`, `crates/aksh-runner/src/worker/steps_runner.rs`
- **Finding**: The C# runner passes the full `IStep` object to the debugger step hooks (`OnStepStartingAsync(step)`, `OnStepCompleted(step)`). The Rust implementation accepts only step display name string (`step_name: &str`).
- **Why it matters**: It limits the debugger's ability to inspect step-specific metadata or update the synthetic `execution.yml` view with dynamic outputs or results.
- **Fix**: Refactor the trait methods to accept a structured step reference instead of a bare string.

### DR-DAP-006 — Runner-Side `WebSocketDapBridge` is Inactive

- **Severity**: Minor / Architectural
- **Files**: `crates/aksh-dap/src/bridge.rs`, `crates/aksh-runner/src/worker/job_runner.rs`
- **Finding**: The `WebSocketDapBridge` is implemented in `aksh-dap` but is never instantiated or run by `aksh-runner`. Instead, the local server handles the WebSocket-to-TCP proxy.
- **Why it matters**: While direct server proxying works for the local usecase, it breaks compatibility for remote/cloud hosted runner usecases using Microsoft DevTunnels, where the runner itself must run `WebSocketDapBridge` to accept incoming connections from the DevTunnels relay.
- **Fix**: Instantiate and run `WebSocketDapBridge` on the runner side when starting a job with `debuggerTunnel` config.

### DR-DAP-007 — Parser Gaps & Hardcoded Debugger Flags

- **Severity**: Major
- **Files**: `crates/aksh-gha-parser/src/job_builder.rs`, `crates/aksh-runner-server/src/lib.rs`
- **Finding**: In `job_builder.rs`, `enable_debugger` is hardcoded to `false` and `debugger_tunnel` is `None` in the generated `AgentJobRequestMessage`. There is also no mechanism in the server's runs REST endpoints to request a debug session or set these flags.
- **Why it matters**: Runs submitted via the API or CLI can never trigger the debugger out-of-the-box, as the runner skips debugger initialization when `enableDebugger` is `false`.
- **Fix**: Add a debug toggle to `WorkflowSubmission` and update `broker_acquire_job` to inject `enableDebugger: true` and a local dummy `debuggerTunnel` configuration.

### DR-DAP-008 — Secret Masker Permitted Keywords Disparity

- **Severity**: Minor
- **Files**: `crates/aksh-dap/src/lib.rs`, `crates/aksh-runner`
- **Finding**: `DAP_PROTOCOL_KEYWORDS` (`"response"`, `"initialize"`, `"event"`) is defined in `aksh-dap` but is never registered with the runner's log secret masker.
- **Why it matters**: If a user happens to configure a repository secret whose value is one of these standard protocol keywords, the log masker will redact them inside the DAP payloads. This would corrupt the DAP JSON stream and crash the debug session.
- **Fix**: Teach the runner's secret masker to allow-list the DAP protocol keywords when a debug session is active.
