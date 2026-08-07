# WebSocket Live Log Streaming — Implementation Plan

## 1. How the Official Runner Does It

### 1.1 Two Paths, Same Protocol

The official runner has two WebSocket paths for live console output. Since GitHub enforces broker protocol (v2.329.0+), only the **modern Results path** matters for aksh:

- **Modern Results path** (`Runner.Worker/ResultsServer.cs`): reads `liveConsoleFeedUrl` from the results endpoint. Used when `resultsServiceOnly = true` (broker path — what GitHub enforces).
- **Legacy AzDO path** (`Runner.Worker/JobServer.cs`): reads `FeedStreamUrl` from `ServiceEndpoint.Data`. Deferred — not needed for GitHub composability.

Both paths use the same JSON wire format and chunked WebSocket sending.

### 1.2 Connection Setup

**`ResultsServer.cs:56-66`** — initialization:
```csharp
public async Task InitializeLiveConsoleFeed(string liveConsoleFeedUrl, string accessToken)
{
    _feedStreamUrl = liveConsoleFeedUrl;
    _accessToken = accessToken;
    await ConnectWebSocketAsync();  // line 167
}
```

**`ResultsServer.cs:167-217`** — connection:
1. Creates `ClientWebSocket`
2. Sets `Authorization: Bearer {accessToken}` header
3. Connects to the WebSocket URL with **30 second timeout**
4. Retries up to **3 times** with **100-500ms random backoff** between attempts
5. If all retries fail → `_websocketClient = null` → graceful degradation (blob upload still works)

```csharp
private async Task ConnectWebSocketAsync()  // line 167
{
    for (int attempt = 0; attempt < 3; attempt++)
    {
        try
        {
            var ws = new ClientWebSocket();
            ws.Options.SetRequestHeader("Authorization", $"Bearer {_accessToken}");
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await ws.ConnectAsync(new Uri(_feedStreamUrl), cts.Token);
            _websocketClient = ws;
            return;
        }
        catch
        {
            await Task.Delay(Random.Shared.Next(100, 500));
        }
    }
    _websocketClient = null;  // give up, fall back to REST-only
}
```

### 1.3 Wire Format

**`Runner.Worker/TimelineRecordFeedLinesWrapper.cs`**:

```json
{
  "count": 5,
  "value": ["line1", "line2", "line3", "line4", "line5"],
  "stepId": "guid-of-step",
  "startLine": 42
}
```

| Field | Type | Description |
|---|---|---|
| `stepId` | string (GUID) | Which step these lines belong to |
| `value` | string[] | Array of log line strings |
| `startLine` | long | 1-indexed line number of the first line in this batch |
| `count` | int | Number of lines (= `value.Length`) |

### 1.4 Sending Protocol

**`ResultsServer.cs:220-282`** — send flow:

1. Serialize `TimelineRecordFeedLinesWrapper` to JSON
2. Convert to UTF-8 bytes
3. Send in **1024-byte chunks** via `WebSocketMessageType.Text`
4. Last chunk sets `endOfMessage: true`
5. On failure: retry up to **3 times** with 100-500ms backoff, reconnect WebSocket between retries
6. If all retries fail: `_websocketClient = null`, return `delivered = false`
7. After **10 minutes** of continuous failure → attempt to re-establish connection

```csharp
public async Task<bool> SendLiveConsoleFeedAsync(TimelineRecordFeedLinesWrapper wrapper)  // line 220
{
    if (_websocketClient == null) return false;

    var json = JsonSerializer.Serialize(wrapper);
    var bytes = Encoding.UTF8.GetBytes(json);

    for (int retry = 0; retry < 3; retry++)
    {
        try
        {
            // Send in 1024-byte chunks
            int offset = 0;
            while (offset < bytes.Length)
            {
                int chunkSize = Math.Min(1024, bytes.Length - offset);
                bool endOfMessage = (offset + chunkSize >= bytes.Length);
                await _websocketClient.SendAsync(
                    new ArraySegment<byte>(bytes, offset, chunkSize),
                    WebSocketMessageType.Text,
                    endOfMessage,
                    CancellationToken.None);
                offset += chunkSize;
            }
            return true;
        }
        catch
        {
            await Task.Delay(Random.Shared.Next(100, 500));
            await ConnectWebSocketAsync();  // reconnect
        }
    }
    _websocketClient = null;
    return false;
}
```

### 1.5 Queue Architecture

Lines are NOT sent directly from step execution. They flow through a **background queue** that batches and throttles.

**`Runner.Worker/JobServerQueue.cs`** — the orchestrator:

**Enqueue** (`QueueWebConsoleLine()`, line 237-253):
```csharp
public void QueueWebConsoleLine(string stepRecordId, string line, long lineNumber)
{
    if (_webConsoleLineQueue.Count > 1024)
    {
        return;  // drop — backpressure
    }
    if (line.Length > 1024)
    {
        line = line.Substring(0, 1024);  // truncate long lines
    }
    _webConsoleLineQueue.Enqueue(new ConsoleLineInfo(stepRecordId, line, lineNumber));
}
```

**Drain loop** (`ProcessWebConsoleLinesQueueAsync()`, line 336-449):
```
Background task lifecycle:
1. Runs every 250ms for the first 60 seconds ("aggressive" phase)
2. Then switches to every 500ms for the remainder
3. Each tick:
   a. Dequeue up to 500 lines
   b. Group lines by stepId
   c. Split each group into batches of 100 lines
   d. Send each batch via WebSocket
4. On shutdown drain:
   a. Only send last 200 lines per step (2 batches × 100)
   b. Best-effort — don't block shutdown
```

**Call chain**:
```
process stdout → BufReader.ReadLineAsync()
  → ExecutionContext.Write() [line 1096-1117]
    → _jobServerQueue.QueueWebConsoleLine(stepRecordId, msg, totalLines)
      → ConcurrentQueue<ConsoleLineInfo>.Enqueue()

background task (250-500ms tick):
  → Dequeue up to 500 from ConcurrentQueue
  → Group by stepId
  → Batch into chunks of 100
  → ResultsServer.SendLiveConsoleFeedAsync(wrapper)
    → WebSocket.SendAsync() in 1024-byte chunks
```

### 1.6 Fallback Behavior

**`JobServerQueue.cs:412-418`**:
- Results path (broker): if WebSocket fails → lines are lost for live view, but blob upload at step completion still works (separate pipeline)
- Legacy AzDO path: if WebSocket fails → fall back to `AppendTimelineRecordFeedAsync` REST call

**Failure tracking** (`JobServer.cs:285-295`):
- After >5 batches attempted, if >50% failed → give up WebSocket permanently for this job
- After 10 minutes of failure → attempt reconnection once

### 1.7 Key Constants

| Constant | Value | Source |
|---|---|---|
| WebSocket chunk size | 1024 bytes | `ResultsServer.cs:248` |
| Lines per batch | 100 | `JobServerQueue.cs:389` |
| Queue capacity (drop threshold) | 1024 entries | `JobServerQueue.cs:240` |
| Line truncation | 1024 chars | `JobServerQueue.cs:245` |
| Aggressive drain interval | 250ms | `JobServerQueue.cs:358` |
| Normal drain interval | 500ms | `JobServerQueue.cs:360` |
| Aggressive phase duration | 60 seconds | `JobServerQueue.cs:356` |
| Connect timeout | 30 seconds | `ResultsServer.cs:185` |
| Connect retries | 3 | `ResultsServer.cs:172` |
| Send retries per batch | 3 | `ResultsServer.cs:225` |
| Reconnect backoff | 100-500ms random | `ResultsServer.cs:210` |
| Failure give-up threshold | 50% of batches after 5+ attempts | `JobServer.cs:289` |
| Re-establish after failure | 10 minutes | `ResultsServer.cs:270` |
| Shutdown drain limit | 200 lines per step | `JobServerQueue.cs:420` |

---

## 2. Current aksh State

### What exists

**Log collection** (`crates/aksh-runner/src/worker/execution_context.rs:24,88,134`):
- `StepContext.log_lines: Vec<String>` — lines accumulated in memory during step execution
- `StepContext::log()` — pushes timestamped, secret-scrubbed lines into `log_lines`
- Lines are collected, NOT streamed

**Process output** (`crates/aksh-runner/src/process.rs:42-120`):
- `invoke()` spawns subprocess, reads stdout/stderr via `BufReader::lines()` into `Vec<String>`
- Output is collected **after process exits** (`collect_lines()` at line 117)
- `on_line: Option<LineCallback>` exists but is called **after** all lines are collected, not during

**Log upload** (`crates/aksh-runner/src/worker/job_runner.rs:406`):
- `upload_step_log()` — uploads entire step log as a blob after step completes
- `upload_job_log()` — uploads concatenation of all step logs after job completes
- Both use `GetStepLogsSignedBlobURL` → `PUT` (REST, not WebSocket)

**Server endpoint data** (`crates/aksh-runner-server/src/lib.rs:1665-1677`):
- `broker_acquire_job()` injects `ResultsServiceUrl`, `PipelinesServiceUrl`, `CacheServerUrl` into `SystemVssConnection` endpoint data
- No `FeedStreamUrl` or `liveConsoleFeedUrl`

### What's missing

| Gap | Component | Description |
|---|---|---|
| No WebSocket client | runner | No `tokio-tungstenite` dependency, no WebSocket send logic |
| No streaming from subprocess | runner | `process::invoke()` collects all output after exit, doesn't stream lines |
| No background drain queue | runner | No equivalent of `JobServerQueue.QueueWebConsoleLine()` + drain loop |
| No `FeedStreamUrl` in endpoint data | server | `broker_acquire_job()` doesn't include it |
| No WebSocket endpoint | server | No `/ws/live-logs/{job_id}` or similar |
| No live forwarding to UI | server | Native API has no SSE/WebSocket for real-time log viewing |

---

## 3. Implementation Plan

### Phase 1: Streaming Process Output (Runner)

**Files**: `crates/aksh-runner/src/process.rs`

The biggest architectural change: subprocess output must be streamed line-by-line **during execution**, not collected after exit.

1. **Refactor `invoke()` to stream stdout/stderr during execution**:
   - Instead of spawning tasks that collect all lines into `Vec<String>`, spawn tasks that send lines through an `mpsc::channel`
   - The main loop reads from the channel while also racing against cancellation
   - Each line is both accumulated (for blob upload) and forwarded to the live feed

   ```rust
   pub async fn invoke(
       program: &str,
       args: &[&str],
       cwd: &Path,
       env: &HashMap<String, String>,
       line_tx: Option<mpsc::UnboundedSender<String>>,  // NEW: live line sink
       cancel_rx: Option<watch::Receiver<bool>>,
   ) -> Result<ProcessOutput> {
       // ... spawn process ...

       // Spawn stdout reader that sends lines as they arrive
       let stdout_handle = stdout.map(|s| {
           let tx = line_tx.clone();
           tokio::spawn(async move {
               let mut reader = BufReader::new(s).lines();
               let mut out = Vec::new();
               while let Ok(Some(line)) = reader.next_line().await {
                   if let Some(ref tx) = tx {
                       let _ = tx.send(line.clone());  // best-effort live send
                   }
                   out.push(line);
               }
               out
           })
       });
       // Same for stderr
   }
   ```

2. **Update all callers of `invoke()`** — `run_script()`, `run_script_in_container()`, docker commands — to pass the `line_tx` when available.

### Phase 2: Live Log Queue (Runner)

**Files**: new `crates/aksh-runner/src/worker/live_logs.rs`

Build the equivalent of `JobServerQueue`'s console line queue:

```rust
pub struct LiveLogQueue {
    /// Bounded queue of pending lines
    lines: Arc<Mutex<VecDeque<ConsoleLineInfo>>>,
    /// WebSocket sender (None = degraded/REST-only mode)
    ws: Arc<Mutex<Option<WebSocketSender>>>,
    /// Shutdown signal
    shutdown: watch::Sender<bool>,
}

struct ConsoleLineInfo {
    step_id: String,
    line: String,
    line_number: u64,
}

impl LiveLogQueue {
    /// Enqueue a line for live streaming. Drops if queue > 1024.
    pub fn enqueue(&self, step_id: &str, line: &str, line_number: u64) {
        let mut q = self.lines.lock().unwrap();  // sync mutex, never held across await
        if q.len() > 1024 { return; }  // backpressure: drop
        let line = if line.len() > 1024 { &line[..1024] } else { line };
        q.push_back(ConsoleLineInfo {
            step_id: step_id.to_string(),
            line: line.to_string(),
            line_number,
        });
    }

    /// Spawn the background drain task.
    pub fn spawn_drain(self: &Arc<Self>) -> JoinHandle<()> {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let start = Instant::now();
            loop {
                // Aggressive phase: 250ms for first 60s, then 500ms
                let interval = if start.elapsed() < Duration::from_secs(60) {
                    Duration::from_millis(250)
                } else {
                    Duration::from_millis(500)
                };
                tokio::time::sleep(interval).await;

                // Dequeue up to 500 lines
                let batch = this.dequeue(500);
                if batch.is_empty() { continue; }

                // Group by step, split into chunks of 100
                let grouped = group_by_step(batch);
                for (step_id, lines) in grouped {
                    for chunk in lines.chunks(100) {
                        let wrapper = TimelineRecordFeedLinesWrapper {
                            step_id: step_id.clone(),
                            value: chunk.iter().map(|l| l.line.clone()).collect(),
                            start_line: chunk[0].line_number,
                            count: chunk.len(),
                        };
                        this.send(wrapper).await;
                    }
                }
            }
        })
    }
}
```

### Phase 3: WebSocket Client (Runner)

**Files**: `crates/aksh-runner/src/worker/live_logs.rs`, `Cargo.toml`

1. **Add dependency**: `tokio-tungstenite` with `native-tls` or `rustls-tls` feature.

2. **WebSocket sender** matching the official protocol:

   ```rust
   struct WebSocketSender {
       ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
       failed_batches: u32,
       total_batches: u32,
       last_failure: Option<Instant>,
   }

   impl WebSocketSender {
       async fn connect(url: &str, token: &str) -> Option<Self> {
           for _ in 0..3 {
               match tokio_tungstenite::connect_async(
                   Request::builder()
                       .uri(url)
                       .header("Authorization", format!("Bearer {token}"))
                       .body(())
                       .unwrap()
               ).await {
                   Ok((ws, _)) => return Some(Self { ws, ... }),
                   Err(_) => {
                       let delay = rand::thread_rng().gen_range(100..500);
                       tokio::time::sleep(Duration::from_millis(delay)).await;
                   }
               }
           }
           None  // give up
       }

       async fn send(&mut self, wrapper: &TimelineRecordFeedLinesWrapper) -> bool {
           let json = serde_json::to_string(wrapper).unwrap();
           let bytes = json.as_bytes();

           for retry in 0..3 {
               // Send in 1024-byte chunks
               let mut offset = 0;
               let mut ok = true;
               while offset < bytes.len() {
                   let end = (offset + 1024).min(bytes.len());
                   let is_last = end == bytes.len();
                   if self.ws.send(Message::Text(
                       // Note: tungstenite handles framing; we just send the full text
                       // The 1024-byte chunking is a C# implementation detail;
                       // tungstenite frames messages automatically
                   )).await.is_err() {
                       ok = false;
                       break;
                   }
                   offset = end;
               }
               if ok {
                   self.total_batches += 1;
                   return true;
               }
               // Reconnect and retry
               tokio::time::sleep(Duration::from_millis(
                   rand::thread_rng().gen_range(100..500)
               )).await;
           }
           self.failed_batches += 1;
           self.total_batches += 1;
           false
       }
   }
   ```

   **Note**: The 1024-byte chunking in the C# code is a `ClientWebSocket` implementation detail. `tungstenite` handles WebSocket framing internally — we just send the full JSON text as a single `Message::Text`. The server reassembles frames automatically.

### Phase 4: Wire It Into the Step Execution Loop (Runner)

**Files**: `crates/aksh-runner/src/worker/steps_runner.rs`, `crates/aksh-runner/src/worker/job_runner.rs`

1. **In `run_job()`**: parse `FeedStreamUrl` from endpoint data, create `LiveLogQueue`, connect WebSocket:
   ```rust
   let live_logs = if let Some(feed_url) = extract_feed_stream_url(&job_message) {
       let token = &reporting.as_ref().map(|r| r.access_token.clone()).unwrap_or_default();
       LiveLogQueue::connect(&feed_url, token).await
   } else {
       None
   };

   // Spawn drain task
   let drain_handle = live_logs.as_ref().map(|q| q.spawn_drain());
   ```

2. **In `run_steps()`**: pass `live_logs` reference, create per-step `line_tx`:
   ```rust
   // For each step:
   let (line_tx, mut line_rx) = mpsc::unbounded_channel();
   // Spawn a task that reads from line_rx and enqueues to live_logs
   let log_forwarder = live_logs.as_ref().map(|q| {
       let q = Arc::clone(q);
       let step_id = step.id.clone();
       tokio::spawn(async move {
           let mut line_num = 1u64;
           while let Some(line) = line_rx.recv().await {
               q.enqueue(&step_id, &line, line_num);
               line_num += 1;
           }
       })
   });
   ```

3. **In `execute_step()`**: pass `line_tx` through to `process::invoke()`.

4. **On job completion**: abort drain task, close WebSocket.

### Phase 5: Server — Provide FeedStreamUrl (Server)

**Files**: `crates/aksh-runner-server/src/lib.rs`

1. **Add `FeedStreamUrl` to endpoint data** in `broker_acquire_job()` (line 1665-1677):
   ```rust
   endpoint.data.insert(
       "FeedStreamUrl".to_owned(),
       format!("{}/ws/live-logs/{}", public_base_url(), job_id),
   );
   ```

2. **Add WebSocket endpoint** — using axum's built-in WebSocket support:
   ```rust
   async fn ws_live_logs(
       ws: axum::extract::WebSocketUpgrade,
       Path(job_id): Path<String>,
       State(shared): State<Arc<SharedState>>,
   ) -> impl IntoResponse {
       ws.on_upgrade(move |socket| handle_live_log_socket(socket, job_id, shared))
   }

   async fn handle_live_log_socket(
       mut socket: axum::extract::ws::WebSocket,
       job_id: String,
       shared: Arc<SharedState>,
   ) {
       // Receive JSON messages from runner, parse TimelineRecordFeedLinesWrapper
       // Store lines in memory and broadcast to any UI subscribers
       while let Some(Ok(msg)) = socket.recv().await {
           if let Message::Text(text) = msg {
               if let Ok(wrapper) = serde_json::from_str::<FeedLinesWrapper>(&text) {
                   let mut inner = shared.state.inner.lock().await;
                   inner.live_log_lines
                       .entry(job_id.clone())
                       .or_default()
                       .extend(wrapper.value);
                   // Broadcast to UI subscribers via tokio::sync::broadcast
               }
           }
       }
   }
   ```

3. **Add native API endpoint** for UI consumers — SSE or WebSocket:
   ```rust
   // GET /api/v1/runs/{run_id}/jobs/{job_id}/logs/live
   // Returns Server-Sent Events stream of log lines as they arrive
   ```

### Phase 6: Server — Forward to UI (Server)

**Files**: `crates/aksh-runner-server/src/lib.rs`

1. **Add broadcast channel** to `InnerState`:
   ```rust
   live_log_tx: HashMap<String, broadcast::Sender<FeedLinesWrapper>>,
   ```

2. **Runner WebSocket handler** publishes to broadcast channel when receiving lines.

3. **UI SSE/WebSocket endpoint** subscribes to broadcast channel for a specific job.

4. **Fallback**: if no live subscribers, lines are stored in memory for later retrieval via REST.

---

## 4. Dependency Graph

```
Phase 1 (streaming process output)     ← foundational, no new deps
  ↓
Phase 2 (live log queue)               ← depends on Phase 1
  ↓
Phase 3 (WebSocket client)             ← adds tokio-tungstenite dep
  ↓
Phase 4 (wire into step execution)     ← depends on 1, 2, 3
  ↓
Phase 5 (server FeedStreamUrl + WS)    ← independent of 1-4, but tested together
  ↓
Phase 6 (server UI forwarding)         ← depends on Phase 5
```

Phases 1-4 are **runner-side** (~70% of work). Phases 5-6 are **server-side** (~30%).

Phase 1 is the hardest and most impactful: refactoring `process::invoke()` from "collect after exit" to "stream during execution" without breaking the existing blob-upload pipeline.

---

## 5. Risks and Decisions

### 5.1 Process Output Streaming (Phase 1)

**Risk**: The current `on_line: Option<LineCallback>` in `process::invoke()` is called after `collect_lines()`, which runs after the process exits. Changing to streaming requires the output reader tasks to forward lines as they arrive, while still accumulating them for the blob upload.

**Decision**: Use an `mpsc::unbounded_channel` per process. Reader tasks send lines to the channel as they arrive. The caller receives lines from the channel and both (a) stores them for blob upload and (b) forwards to the live queue. This preserves the existing blob upload pipeline.

### 5.2 Backpressure

**Risk**: If the WebSocket is slow and lines arrive faster than they drain, the queue grows unbounded.

**Decision**: Match the official runner: hard cap at 1024 queued entries. Drop lines if queue is full. This is acceptable — live logs are best-effort, and the blob upload captures everything.

### 5.3 Line Ordering

**Risk**: Lines from stdout and stderr arrive on separate channels. Interleaving order may differ from the official runner.

**Decision**: The official runner merges stdout and stderr into a single stream before feeding `ExecutionContext.Write()`. We should do the same — merge stdout and stderr into a single `mpsc` channel in arrival order. This matches behavior and simplifies the queue.

### 5.4 Docker Exec

**Risk**: Container steps use `docker exec`, which has its own stdout/stderr handling.

**Decision**: `docker exec` output is captured the same way as regular processes — via `process::invoke()`. The streaming refactor automatically covers container steps.

### 5.5 WebSocket vs SSE for Server→UI

**Decision**: Use **Server-Sent Events (SSE)** for the native API `/api/v1/.../logs/live`. SSE is simpler (unidirectional, auto-reconnect in browsers, works through proxies), and log viewing is read-only. Reserve WebSocket for the runner→server path where bidirectional framing is required by the official protocol.

### 5.6 tungstenite vs fastwebsockets

**Decision**: Use `tokio-tungstenite` — it's the standard async WebSocket client for Rust, well-maintained, and axum already supports WebSocket upgrades natively. No need for `fastwebsockets` unless benchmarks show a problem.

---

## 6. Test Plan

### Unit tests

| Test | What it validates |
|---|---|
| `LiveLogQueue::enqueue` drops at 1024 | Backpressure works |
| `LiveLogQueue::enqueue` truncates at 1024 chars | Long lines truncated |
| Drain loop batches by step, max 100 per batch | Grouping and chunking correct |
| `TimelineRecordFeedLinesWrapper` serialization | Wire format matches official |
| `process::invoke` with `line_tx` streams during execution | Lines arrive before process exits |

### Integration tests

| Test | What it validates |
|---|---|
| Server WebSocket endpoint accepts connection with bearer auth | Auth works |
| Runner sends batches, server receives and stores | End-to-end pipeline |
| WebSocket failure → blob upload still works | Graceful degradation |
| SSE endpoint streams lines to UI client | Server→UI path |
| 10+ concurrent steps → lines grouped correctly by stepId | Multi-step job |

### E2E tests

| Test | What it validates |
|---|---|
| Submit workflow to aksh-server, connect to SSE, see lines appear in <500ms | Real-time UX |
| Kill WebSocket mid-job → job still completes, logs still uploaded via blob | Fallback |

---

## 7. Acceptance Criteria

1. During step execution, log lines appear on the server within **500ms** of being written to stdout
2. Blob upload at step completion still works (regression-free)
3. If WebSocket connection fails at startup, job runs normally without live logs
4. If WebSocket connection drops mid-job, job continues and blob upload captures all lines
5. Queue drops lines above 1024 entries instead of growing unbounded
6. Lines are grouped by step and batched (max 100 per batch)
7. `startLine` is correct (1-indexed, monotonically increasing per step)
8. Native API has an SSE endpoint that streams live lines during job execution
9. No new `unsafe` code; `tokio-tungstenite` is the only new dependency
