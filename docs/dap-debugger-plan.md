# DAP Debugger Plan

Design for adding a first-class debugging experience to aksh/preloop — both local (smolvm) and cloud (hosted runners).

---

## What the C# runner's debugger does

The architecture has three layers:

```
VS Code / DAP Client
        │  ws:// or DAP TCP
        ▼
GitHub Tunnel Relay (DebuggerTunnelInfo)
        │  forwards
        ▼
WebSocket Bridge (runner-side)
        │  TCP DAP framing — Content-Length header
        ▼
DapDebugger (TCP server, port from tunnel config)
        │  TaskCompletionSource pause/resume
        ▼
StepsRunner (pauses at OnStepStartingAsync)
```

**Pause/resume mechanism:** `OnStepStartingAsync` awaits a `TaskCompletionSource` that the DAP `continue` handler completes. No fibers, no process suspension — just `await tcs.Task` in the step loop.

**Remote path:** GitHub's tunnel relay service delivers `DebuggerTunnelInfo` (`TunnelId`, `ClusterId`, `HostToken`, `Port`) in the job payload. The runner connects out to the relay; the editor connects in. aksh does not use this relay.

---

## Use Case 1 — Local (preloop, smolvm)

Runner, aksh server, and orchestration are on the same machine or LAN. No relay needed.

```
Host machine
├── aksh-runner-server  (/api/v1/debug/...)
└── aksh-runner         DapDebugger (TCP :random)
        └── registers port → aksh-runner-server

smolvm
└── runner in VM        DapDebugger (TCP :random)
        └── HTTP port-report to host aksh

VS Code  →  aksh proxies TCP  →  /api/v1/runs/:id/debug  →  runner
```

The runner binds a random DAP port locally, reports it to aksh via a small API call, and aksh proxies it through a WebSocket upgrade on `/api/v1/runs/:run_id/debug`. VS Code connects to that WebSocket endpoint. No external relay, no tunnel token.

---

## Use Case 2 — Cloud (hosted runners)

Runners are remote VMs with no direct TCP reach from editor to runner.

### Option A — aksh-server as the relay (recommended)

```
VS Code
  │  wss://your-aksh-cloud/api/v1/runs/:id/debug
  ▼
aksh-runner-server (cloud)
  │  runner long-polls GET /debug/session, then streams
  ▼
remote runner
```

Runner opens a persistent outbound WebSocket to your aksh server (it already has a session there). aksh buffers DAP frames between the editor's inbound WebSocket and the runner's outbound connection. No firewall holes needed on the runner side.

### Option B — WireGuard/Cloudflare Tunnel sidecar

Each VM gets a WireGuard peer or `cloudflared` tunnel baked into the image. aksh knows the tunnel address per runner from registration. Simpler ops — just TCP forwarding again, same as local.

**Pick Option A** — you own the auth, no third-party relay dependency, and the runner already speaks HTTP back to aksh.

---

## Implementation Layers

### Layer 1 — Pause/resume in the runner

File: `crates/aksh-runner/src/worker/job_runner.rs`

Before dispatching each step:

```rust
for step in steps {
    debugger.on_step_starting(&step).await; // blocks here if debugger attached
    run_step(step).await;
}
```

`DapDebugger` struct:

```rust
struct DapDebugger {
    state: Arc<Mutex<DebugState>>,
    resume_tx: watch::Sender<()>,  // DAP `continue` fires this
}

async fn on_step_starting(&self, step: &dyn Step) {
    if !self.active { return; }
    self.send_stopped_event(step);
    self.resume_rx.changed().await; // paused here until `continue`
}
```

### Layer 2 — DAP TCP server (new crate: `aksh-dap`)

Tokio TCP listener bound to `127.0.0.1:0` (random port). Speaks standard DAP framing:

```
Content-Length: N\r\n\r\n{json}
```

Commands to handle:

| DAP Command | Behavior |
|---|---|
| `initialize` | Respond with capabilities + emit `initialized` event |
| `configurationDone` | Signal `WaitUntilReady`; session is live |
| `threads` | Single thread: `Job: <job_name>` |
| `stackTrace` | Current step name + line number in synthetic `execution.yml` |
| `source` | Synthesized YAML listing pre/main/post steps |
| `continue` | Fire `resume_tx`; unblock `on_step_starting` |
| `evaluate` | Run expression through `aksh-gha-expressions` against current context |
| `variables` | Dump env, outputs, step context |
| `disconnect` / `terminate` | Clean shutdown, emit `terminated` + `exited` events |

Estimated size: ~600 lines of Rust.

### Layer 3 — Transport (differs by case)

**Local — aksh-runner-server routes:**

```
GET  /api/v1/runs/:run_id/debug   WebSocket upgrade → TCP forward to runner DAP port
POST /api/v1/runs/:run_id/debug   Runner calls this to register its DAP port
```

**Cloud — runner dials back:**

Runner opens persistent outbound WebSocket to:
```
wss://your-aksh/api/v1/debug-session/:run_id
```

aksh holds both ends (editor WS + runner WS), forwards DAP frames between them. Editor sees no difference from the local path.

### Layer 4 — VS Code extension

Thin extension (or `launch.json` template) that adapts WebSocket to DAP framing:

```json
{
  "type": "preloop",
  "request": "attach",
  "url": "wss://localhost:9090/api/v1/runs/RUN_ID/debug",
  "token": "${env:AKSH_TOKEN}"
}
```

The extension does WebSocket → DAP framing. The C# runner does the same with `WebSocketDapBridge.cs`.

---

## Build Order

1. **Pause/resume hook** — `job_runner.rs`, ~50 lines. No protocol yet. Validate with a simple `POST /debug/:run_id/continue` HTTP endpoint before writing any DAP.
2. **`aksh-dap` crate** — DAP TCP server, protocol layer, no transport concerns.
3. **Local WebSocket proxy** — new routes in `aksh-runner-server`. Wire local case end to end.
4. **Cloud outbound session** — runner dials back, server proxies.
5. **VS Code extension** — thin WebSocket-to-DAP adapter.

---

## DAP Protocol Reference Tests (from C# runner)

The C# runner has 113 tests covering the DAP layer. Key behaviors to preserve:

- `StartAsyncUsesPortFromTunnelConfig` — runner binds the port from job config, editor connects to it.
- `WaitUntilReadyCompletesAfterClientConnectionAndConfigurationDone` — session is not live until `configurationDone` received.
- `CancellationDuringStepPauseReleasesWait` — job cancellation unblocks a paused step.
- `OnJobCompletedSendsTerminatedAndExitedEvents` — clean shutdown emits both events.
- `StackTraceUsesJobStepsSourceLine` — stack frame line number maps to step position in synthetic YAML.
- `InitializeRequestOverSocketPreservesProtocolMetadataWhenSecretsCollide` — secret masker must not redact DAP protocol keywords (`response`, `initialize`, `event`).
- `PredictedPostStepIsServedAtInitializationAndClaimedAtRegistration` — post steps appear in the source view before they are registered.
- `StackTraceSanitizesSyntheticSourcePath` — `/` and `\` in job names are replaced with `_` in source paths.
- `ResolveTimeoutUsesCustomTimeoutFromEnvironment` — `ACTIONS_RUNNER_DAP_CONNECTION_TIMEOUT` env var overrides default 15s.
- `StartAsyncWithWebSocketBridgeAcceptsInitializeOverWebSocket` — WebSocket and raw TCP both speak the same DAP framing.
