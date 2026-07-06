//! Live console log streaming over the broker results WebSocket.
//!
//! The official runner treats live console logs as best-effort: stdout/stderr
//! lines are queued, batched by step, and sent to `FeedStreamUrl` while the
//! normal step-log blob upload remains the durable source of truth.

use futures::SinkExt;
use rand::Rng;
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tracing::{debug, warn};

const QUEUE_DROP_THRESHOLD: usize = 1024;
const LINE_TRUNCATE_CHARS: usize = 1024;
const DRAIN_LIMIT: usize = 500;
const LINES_PER_BATCH: usize = 100;
const SHUTDOWN_LINES_PER_STEP: usize = 200;
const AGGRESSIVE_INTERVAL: Duration = Duration::from_millis(250);
const NORMAL_INTERVAL: Duration = Duration::from_millis(500);
const AGGRESSIVE_DURATION: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const RETRIES: usize = 3;

/// Re-export from protocol crate for backward compatibility.
pub use aksh_gha_protocol::LiveLogFeedLinesWrapper as TimelineRecordFeedLinesWrapper;

#[derive(Debug, Clone)]
struct ConsoleLineInfo {
    step_id: String,
    line: String,
    line_number: u64,
}

/// Best-effort live log queue with official-runner-style batching/backpressure.
impl std::fmt::Debug for LiveLogQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveLogQueue").finish_non_exhaustive()
    }
}

pub struct LiveLogQueue {
    lines: Mutex<VecDeque<ConsoleLineInfo>>,
    ws: tokio::sync::Mutex<Option<WebSocketSender>>,
    shutdown_tx: watch::Sender<bool>,
}

impl LiveLogQueue {
    /// Connect to the live console feed and return a queue even if the initial
    /// WebSocket connection fails. Failed live streaming must not fail the job.
    pub async fn connect(feed_url: String, access_token: String) -> Arc<Self> {
        let ws = WebSocketSender::connect(feed_url, access_token).await;
        let (shutdown_tx, _) = watch::channel(false);
        Arc::new(Self {
            lines: Mutex::new(VecDeque::new()),
            ws: tokio::sync::Mutex::new(ws),
            shutdown_tx,
        })
    }

    /// Build a queue with no WebSocket, used by tests and degraded live-log mode.
    #[cfg(test)]
    fn disconnected() -> Arc<Self> {
        let (shutdown_tx, _) = watch::channel(false);
        Arc::new(Self {
            lines: Mutex::new(VecDeque::new()),
            ws: tokio::sync::Mutex::new(None),
            shutdown_tx,
        })
    }

    /// Enqueue one console line. Lines above the official 1024-entry threshold
    /// are dropped; overlong lines are truncated to 1024 Unicode scalar values.
    pub fn enqueue(&self, step_id: &str, line: &str, line_number: u64) {
        let mut lines = self.lines.lock().expect("live log queue poisoned");
        if lines.len() > QUEUE_DROP_THRESHOLD {
            return;
        }
        let line = truncate_line(line);
        lines.push_back(ConsoleLineInfo {
            step_id: step_id.to_string(),
            line,
            line_number,
        });
    }

    /// Spawn the background drain loop.
    pub fn spawn_drain(self: &Arc<Self>) -> JoinHandle<()> {
        let this = Arc::clone(self);
        let mut shutdown_rx = this.shutdown_tx.subscribe();
        tokio::spawn(async move {
            let start = Instant::now();
            loop {
                let interval = if start.elapsed() < AGGRESSIVE_DURATION {
                    AGGRESSIVE_INTERVAL
                } else {
                    NORMAL_INTERVAL
                };

                tokio::select! {
                    _ = tokio::time::sleep(interval) => {
                        this.drain_once(DRAIN_LIMIT).await;
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            this.drain_shutdown().await;
                            break;
                        }
                    }
                }
            }
        })
    }

    /// Signal shutdown and wait for the drain task to flush a bounded tail.
    ///
    /// If the drain task does not finish within [`SHUTDOWN_TIMEOUT`] (e.g. due
    /// to WebSocket retries against a broken endpoint), we abort it rather than
    /// blocking job completion indefinitely.
    pub async fn shutdown_and_wait(&self, handle: JoinHandle<()>) {
        let _ = self.shutdown_tx.send(true);
        match tokio::time::timeout(SHUTDOWN_TIMEOUT, handle).await {
            Ok(_) => {}
            Err(_) => {
                warn!(
                    "live log drain did not finish within {}s, aborting",
                    SHUTDOWN_TIMEOUT.as_secs()
                );
            }
        }
    }

    async fn drain_once(&self, limit: usize) {
        let batch = self.dequeue(limit);
        self.send_grouped(batch).await;
    }

    async fn drain_shutdown(&self) {
        let tail = self.dequeue_shutdown_tail();
        self.send_grouped(tail).await;
        let mut ws = self.ws.lock().await;
        if let Some(sender) = ws.as_mut() {
            sender.close().await;
        }
        *ws = None;
    }

    fn dequeue(&self, limit: usize) -> Vec<ConsoleLineInfo> {
        let mut queue = self.lines.lock().expect("live log queue poisoned");
        let count = queue.len().min(limit);
        queue.drain(..count).collect()
    }

    fn dequeue_shutdown_tail(&self) -> Vec<ConsoleLineInfo> {
        let mut queue = self.lines.lock().expect("live log queue poisoned");
        let drained: Vec<_> = queue.drain(..).collect();
        tail_by_step(drained, SHUTDOWN_LINES_PER_STEP)
    }

    async fn send_grouped(&self, lines: Vec<ConsoleLineInfo>) {
        for wrapper in wrappers_from_lines(lines) {
            let mut ws = self.ws.lock().await;
            if let Some(sender) = ws.as_mut() {
                if !sender.send(&wrapper).await && sender.should_disable() {
                    warn!(
                        url = %sender.url,
                        failed = sender.failed_batches,
                        total = sender.total_batches,
                        "disabling live log websocket — failure rate exceeded 50%"
                    );
                    *ws = None;
                }
            }
        }
    }
}

struct WebSocketSender {
    url: String,
    token: String,
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    failed_batches: u32,
    total_batches: u32,
}

impl WebSocketSender {
    async fn connect(url: String, token: String) -> Option<Self> {
        let ws = connect_websocket(&url, &token).await?;
        Some(Self {
            url,
            token,
            ws,
            failed_batches: 0,
            total_batches: 0,
        })
    }

    async fn reconnect(&mut self) -> bool {
        match connect_websocket(&self.url, &self.token).await {
            Some(ws) => {
                self.ws = ws;
                true
            }
            None => false,
        }
    }

    async fn send(&mut self, wrapper: &TimelineRecordFeedLinesWrapper) -> bool {
        self.total_batches += 1;
        let payload = match serde_json::to_string(wrapper) {
            Ok(payload) => payload,
            Err(error) => {
                warn!(%error, "serializing live log wrapper failed");
                self.failed_batches += 1;
                return false;
            }
        };

        for attempt in 0..RETRIES {
            if self.ws.send(Message::Text(payload.clone())).await.is_ok() {
                return true;
            }

            // Only backoff and reconnect if we have more attempts left;
            // don't waste time on the final failed attempt.
            if attempt + 1 < RETRIES {
                random_backoff().await;
                let _ = self.reconnect().await;
            }
        }

        self.failed_batches += 1;
        false
    }

    fn should_disable(&self) -> bool {
        self.total_batches > 5 && self.failed_batches * 2 > self.total_batches
    }

    async fn close(&mut self) {
        let _ = self.ws.close(None).await;
    }
}

async fn connect_websocket(
    url: &str,
    token: &str,
) -> Option<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    for attempt in 0..RETRIES {
        let mut request = match url.into_client_request() {
            Ok(request) => request,
            Err(error) => {
                warn!(%error, %url, "invalid live log websocket URL");
                return None;
            }
        };
        if !token.is_empty() {
            let value = format!("Bearer {token}");
            match value.parse() {
                Ok(value) => {
                    request.headers_mut().insert(header::AUTHORIZATION, value);
                }
                Err(error) => {
                    warn!(%error, "invalid live log authorization header");
                    return None;
                }
            }
        }

        match tokio::time::timeout(CONNECT_TIMEOUT, connect_async(request)).await {
            Ok(Ok((ws, _))) => return Some(ws),
            Ok(Err(error)) => debug!(%error, attempt, "live log websocket connect failed"),
            Err(_) => debug!(attempt, "live log websocket connect timed out"),
        }
        random_backoff().await;
    }
    None
}

async fn random_backoff() {
    let delay_ms = rand::thread_rng().gen_range(100..500);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
}

fn truncate_line(line: &str) -> String {
    let mut chars = line.chars();
    let truncated: String = chars.by_ref().take(LINE_TRUNCATE_CHARS).collect();
    truncated
}

fn wrappers_from_lines(lines: Vec<ConsoleLineInfo>) -> Vec<TimelineRecordFeedLinesWrapper> {
    let mut grouped: BTreeMap<String, Vec<ConsoleLineInfo>> = BTreeMap::new();
    for line in lines {
        grouped.entry(line.step_id.clone()).or_default().push(line);
    }

    let mut wrappers = Vec::new();
    for (step_id, lines) in grouped {
        for chunk in lines.chunks(LINES_PER_BATCH) {
            if chunk.is_empty() {
                continue;
            }
            wrappers.push(TimelineRecordFeedLinesWrapper {
                step_id: step_id.clone(),
                start_line: chunk[0].line_number,
                count: chunk.len(),
                value: chunk.iter().map(|line| line.line.clone()).collect(),
            });
        }
    }
    wrappers
}

fn tail_by_step(lines: Vec<ConsoleLineInfo>, limit: usize) -> Vec<ConsoleLineInfo> {
    let mut kept = Vec::new();
    let mut per_step_counts: BTreeMap<String, usize> = BTreeMap::new();
    for line in lines.into_iter().rev() {
        let count = per_step_counts.entry(line.step_id.clone()).or_default();
        if *count < limit {
            *count += 1;
            kept.push(line);
        }
    }
    kept.reverse();
    kept
}

/// Extract `FeedStreamUrl` from the SystemVssConnection endpoint data.
pub fn extract_feed_stream_url(job_message: &serde_json::Value) -> Option<String> {
    job_message
        .get("resources")?
        .get("endpoints")?
        .as_array()?
        .iter()
        .find(|endpoint| {
            endpoint
                .get("name")
                .and_then(|v| v.as_str())
                .is_some_and(|name| name.eq_ignore_ascii_case("SystemVssConnection"))
        })?
        .get("data")?
        .get("FeedStreamUrl")?
        .as_str()
        .map(str::to_owned)
}

/// Build a process-line callback that masks secrets and enqueues live lines for
/// one step. The callback is best-effort and intentionally independent of the
/// durable `StepContext` log collection.
///
/// Uses the shared `live_masks` so that `::add-mask::` commands issued mid-step
/// take effect immediately on the live feed (not just the durable log).
pub fn process_line_callback(
    step_id: &str,
    live_masks: &std::sync::Arc<std::sync::RwLock<std::collections::HashSet<String>>>,
    live_logs: Option<&Arc<LiveLogQueue>>,
) -> Option<crate::process::LineCallback<'static>> {
    let live_logs = live_logs.cloned()?;
    let step_id = step_id.to_string();
    let live_masks = live_masks.clone();
    let next_line = Arc::new(std::sync::atomic::AtomicU64::new(1));
    Some(Box::new(move |line: &str| {
        let mut masked = line.to_string();
        if let Ok(masks) = live_masks.read() {
            // Sort by length descending so longer secrets are replaced first,
            // matching the durable-log masking order in JobContext::mask_secrets.
            let mut secrets: Vec<&String> = masks.iter().filter(|s| !s.is_empty()).collect();
            secrets.sort_by_key(|b| std::cmp::Reverse(b.len()));
            for secret in secrets {
                masked = masked.replace(secret.as_str(), "***");
            }
        }
        let line_number = next_line.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        live_logs.enqueue(&step_id, &masked, line_number);
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_drops_above_threshold_and_truncates() {
        let queue = LiveLogQueue::disconnected();
        let long = "é".repeat(1100);
        queue.enqueue("step", &long, 1);
        for i in 0..=QUEUE_DROP_THRESHOLD {
            queue.enqueue("step", &format!("line-{i}"), i as u64 + 2);
        }
        queue.enqueue("step", "dropped", 9_999);

        let lines = queue.dequeue(QUEUE_DROP_THRESHOLD + 10);
        assert_eq!(lines.len(), QUEUE_DROP_THRESHOLD + 1);
        assert_eq!(lines[0].line.chars().count(), LINE_TRUNCATE_CHARS);
        assert_eq!(lines.last().unwrap().line, "line-1023");
    }

    #[test]
    fn wrappers_group_by_step_and_split_at_one_hundred_lines() {
        let mut lines = Vec::new();
        for i in 1..=205 {
            lines.push(ConsoleLineInfo {
                step_id: "a".to_string(),
                line: format!("a-{i}"),
                line_number: i,
            });
        }
        for i in 1..=2 {
            lines.push(ConsoleLineInfo {
                step_id: "b".to_string(),
                line: format!("b-{i}"),
                line_number: i,
            });
        }

        let wrappers = wrappers_from_lines(lines);
        assert_eq!(wrappers.len(), 4);
        assert_eq!(wrappers[0].step_id, "a");
        assert_eq!(wrappers[0].start_line, 1);
        assert_eq!(wrappers[0].count, 100);
        assert_eq!(wrappers[1].start_line, 101);
        assert_eq!(wrappers[1].count, 100);
        assert_eq!(wrappers[2].start_line, 201);
        assert_eq!(wrappers[2].count, 5);
        assert_eq!(wrappers[3].step_id, "b");
        assert_eq!(wrappers[3].start_line, 1);
        assert_eq!(wrappers[3].count, 2);
    }

    #[test]
    fn wrapper_serializes_official_wire_names() {
        let wrapper = TimelineRecordFeedLinesWrapper {
            step_id: "step-guid".to_string(),
            start_line: 42,
            count: 2,
            value: vec!["one".to_string(), "two".to_string()],
        };

        let json = serde_json::to_value(&wrapper).unwrap();
        assert_eq!(json["stepId"], "step-guid");
        assert_eq!(json["startLine"], 42);
        assert_eq!(json["count"], 2);
        assert_eq!(json["value"], serde_json::json!(["one", "two"]));
    }

    #[test]
    fn shutdown_tail_keeps_last_two_hundred_per_step() {
        let lines = (1..=250)
            .map(|i| ConsoleLineInfo {
                step_id: "step".to_string(),
                line: format!("line-{i}"),
                line_number: i,
            })
            .collect();

        let tail = tail_by_step(lines, SHUTDOWN_LINES_PER_STEP);
        assert_eq!(tail.len(), 200);
        assert_eq!(tail[0].line_number, 51);
        assert_eq!(tail.last().unwrap().line_number, 250);
    }

    #[test]
    fn extracts_feed_stream_url_from_system_connection() {
        let message = serde_json::json!({
            "resources": {
                "endpoints": [{
                    "name": "SystemVssConnection",
                    "data": { "FeedStreamUrl": "ws://localhost/ws/live-logs/job" }
                }]
            }
        });

        assert_eq!(
            extract_feed_stream_url(&message).as_deref(),
            Some("ws://localhost/ws/live-logs/job")
        );
    }
}
