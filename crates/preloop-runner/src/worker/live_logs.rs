//! Live console log streaming over the broker results WebSocket.
//!
//! The official runner treats live console logs as best-effort: stdout/stderr
//! lines are queued, batched by step, and sent to `FeedStreamUrl` while the
//! normal step-log blob upload remains the durable source of truth.

use futures::SinkExt;
use rand::Rng;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
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
const SINGLE_CONNECT_ATTEMPT: usize = 1;

/// How long after the last failed connection attempt the feed is re-dialed,
/// mirroring the official runner's `_lastConnectionFailure.AddMinutes(10)`
/// gate in `ResultsServer.AppendLiveConsoleFeedAsync`.
const RETRY_AFTER_FAILURE: Duration = Duration::from_secs(600);

/// Re-export from protocol crate for backward compatibility.
pub use preloop_gha_protocol::LiveLogFeedLinesWrapper as TimelineRecordFeedLinesWrapper;

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
    masks: Arc<RwLock<HashSet<String>>>,
    /// Gate synchronizing mask registration with transmission. The
    /// `::add-mask::` path holds it for writing across registration while
    /// each WebSocket batch holds it for reading across the final mask
    /// snapshot and the send, so a batch can never go out with a mask
    /// snapshot taken before a registration that completed first.
    mask_gate: Arc<tokio::sync::RwLock<()>>,
    ws: tokio::sync::Mutex<Option<WebSocketSender>>,
    /// Feed endpoint retained so a downed socket can be re-dialed by drains.
    /// `None` only for [`LiveLogQueue::disconnected`] test queues.
    feed: Option<(String, String)>,
    /// Guards against overlapping connect tasks spawned by drains.
    connect_in_flight: AtomicBool,
    /// Timestamp of the last failed connect cycle; gates the retry window the
    /// same way upstream's `_lastConnectionFailure` does.
    last_connect_failure: Mutex<Option<Instant>>,
    /// Delay before a dead feed is re-dialed. [`RETRY_AFTER_FAILURE`] in
    /// production; shorter in tests so the gate can be exercised.
    retry_interval: Duration,
    shutdown_tx: watch::Sender<bool>,
}

impl LiveLogQueue {
    /// Return a live-log queue immediately and connect to the console feed in
    /// the background.
    ///
    /// Live console streaming is best-effort: the durable step-log blobs are
    /// the source of truth. Dialing the feed inline put up to
    /// `RETRIES * (CONNECT_TIMEOUT + backoff)` in front of the job's first
    /// step, so an unreachable feed endpoint delayed every job by ~1s. Lines
    /// produced before the socket is up stay queued and go out on the first
    /// drain after it connects.
    pub fn connect(
        feed_url: String,
        access_token: String,
        masks: Arc<RwLock<HashSet<String>>>,
    ) -> Arc<Self> {
        Self::connect_with_retry(feed_url, access_token, masks, RETRY_AFTER_FAILURE)
    }

    fn connect_with_retry(
        feed_url: String,
        access_token: String,
        masks: Arc<RwLock<HashSet<String>>>,
        retry_interval: Duration,
    ) -> Arc<Self> {
        let (shutdown_tx, _) = watch::channel(false);
        let queue = Arc::new(Self {
            lines: Mutex::new(VecDeque::new()),
            masks,
            mask_gate: Arc::new(tokio::sync::RwLock::new(())),
            ws: tokio::sync::Mutex::new(None),
            feed: Some((feed_url, access_token)),
            connect_in_flight: AtomicBool::new(false),
            last_connect_failure: Mutex::new(None),
            retry_interval,
            shutdown_tx,
        });
        queue.spawn_connect(RETRIES);
        queue
    }

    /// Spawn the background connect task that installs a freshly dialed
    /// sender, or stamps `last_connect_failure` when every attempt fails.
    /// Each spawn dials a brand-new WebSocket — like the official runner's
    /// `CreateWebSocketClient()` — so a dead socket is never retried.
    /// `connect_attempts` is three for initial setup and one for periodic
    /// recovery, matching the upstream `retryConnection` flag.
    fn spawn_connect(self: &Arc<Self>, connect_attempts: usize) {
        let Some((feed_url, access_token)) = self.feed.clone() else {
            return;
        };
        self.connect_in_flight.store(true, Ordering::SeqCst);
        let connecting = Arc::clone(self);
        tokio::spawn(async move {
            let mut shutdown_rx = connecting.shutdown_tx.subscribe();
            let sender = tokio::select! {
                sender = WebSocketSender::connect(feed_url, access_token, connect_attempts) => sender,
                _ = shutdown_rx.changed() => {
                    connecting.connect_in_flight.store(false, Ordering::SeqCst);
                    return;
                }
            };
            let Some(sender) = sender else {
                // Stamp the failure before clearing in-flight: a concurrent
                // retry_connect_if_due must observe either in_flight or the
                // failure timestamp, never the empty gap between them.
                connecting.record_connect_failure();
                connecting.connect_in_flight.store(false, Ordering::SeqCst);
                return;
            };
            // The drain task tears the socket down on shutdown; installing a
            // freshly dialed one afterwards would leak a live connection.
            if *connecting.shutdown_tx.borrow() {
                connecting.connect_in_flight.store(false, Ordering::SeqCst);
                return;
            }
            *connecting.ws.lock().await = Some(sender);
            connecting.connect_in_flight.store(false, Ordering::SeqCst);
        });
    }

    fn record_connect_failure(&self) {
        *self
            .last_connect_failure
            .lock()
            .expect("live log failure clock poisoned") = Some(Instant::now());
    }

    /// Re-dial the feed once the retry window since the last failed connect
    /// has elapsed — the same gate the official runner applies to
    /// `_lastConnectionFailure` before re-initializing its websocket client.
    async fn retry_connect_if_due(self: &Arc<Self>) {
        if self.feed.is_none() || self.connect_in_flight.load(Ordering::SeqCst) {
            return;
        }
        let due = self
            .last_connect_failure
            .lock()
            .expect("live log failure clock poisoned")
            .is_none_or(|failed_at| failed_at.elapsed() >= self.retry_interval);
        if !due {
            return;
        }

        // Recheck the socket while holding the same lock used by the
        // connection task when it publishes a successful sender. This closes
        // the gap between drain_once observing None and scheduling a retry.
        let ws = self.ws.lock().await;
        if ws.is_some() || self.connect_in_flight.load(Ordering::SeqCst) {
            return;
        }
        self.spawn_connect(SINGLE_CONNECT_ATTEMPT);
    }

    /// Build a queue with no WebSocket, used by tests and degraded live-log mode.
    #[cfg(test)]
    fn disconnected() -> Arc<Self> {
        let (shutdown_tx, _) = watch::channel(false);
        Arc::new(Self {
            lines: Mutex::new(VecDeque::new()),
            masks: Arc::new(RwLock::new(HashSet::new())),
            mask_gate: Arc::new(tokio::sync::RwLock::new(())),
            ws: tokio::sync::Mutex::new(None),
            feed: None,
            connect_in_flight: AtomicBool::new(false),
            last_connect_failure: Mutex::new(None),
            retry_interval: RETRY_AFTER_FAILURE,
            shutdown_tx,
        })
    }

    /// Acquire the mask gate for writing, for use from sync code.
    ///
    /// `handle_command` runs on step-output threads that may sit inside the
    /// tokio runtime, where `blocking_write()` would panic, so this acquires
    /// cooperatively. The read critical section is one batch send, making
    /// the spin uncontended in practice.
    pub fn acquire_mask_gate_write(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        loop {
            match Arc::clone(&self.mask_gate).try_write_owned() {
                Ok(guard) => return guard,
                Err(_) => std::thread::sleep(std::time::Duration::from_micros(100)),
            }
        }
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

    /// Re-mask lines that are still queued after a newly registered secret.
    /// Lines already dequeued cannot be recalled from the WebSocket.
    pub fn remask_with(&self, secrets: &[String]) {
        let mut lines = self.lines.lock().expect("live log queue poisoned");
        let masked = mask_lines(lines.iter().map(|entry| entry.line.as_str()), secrets);
        for (entry, line) in lines.iter_mut().zip(masked) {
            entry.line = line;
        }
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

    async fn drain_once(self: &Arc<Self>, limit: usize) {
        // Lines produced while the WebSocket handshake is in flight must stay
        // queued. Dequeuing before a sender exists loses the beginning of
        // every fast step even when the connection eventually succeeds.
        if self.ws.lock().await.is_none() {
            // A dead feed is not permanent: once the retry window since the
            // last failed connect elapses, the next drain re-dials it.
            self.retry_connect_if_due().await;
            return;
        }
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
        let batches = line_batches(lines);
        for index in 0..batches.len() {
            let mut ws = self.ws.lock().await;
            let Some(sender) = ws.as_mut() else {
                drop(ws);
                self.requeue_front(&batches[index..]);
                return;
            };
            let wrapper = wrapper_from_lines(&batches[index]);
            let sent = sender.send(&wrapper, self).await;
            if !sent {
                // Upstream behavior: an undelivered batch nulls the client so
                // the next send skips the socket, and a later drain re-dials
                // once the `_lastConnectionFailure` window has elapsed.
                *ws = None;
                drop(ws);
                // Keep unsent lines available for a later drain. They remain
                // remaskable until a batch is actually transmitted.
                self.requeue_front(&batches[index..]);
                return;
            }
        }
    }

    fn requeue_front(&self, batches: &[Vec<ConsoleLineInfo>]) {
        let mut queue = self.lines.lock().expect("live log queue poisoned");
        for batch in batches.iter().rev() {
            for line in batch.iter().rev() {
                queue.push_front(line.clone());
            }
        }
    }
}

struct WebSocketSender {
    url: String,
    token: String,
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl WebSocketSender {
    async fn connect(url: String, token: String, connect_attempts: usize) -> Option<Self> {
        let ws = connect_websocket(&url, &token, connect_attempts).await?;
        Some(Self { url, token, ws })
    }

    async fn reconnect(&mut self) -> bool {
        match connect_websocket(&self.url, &self.token, SINGLE_CONNECT_ATTEMPT).await {
            Some(ws) => {
                self.ws = ws;
                true
            }
            None => false,
        }
    }

    async fn send(
        &mut self,
        wrapper: &TimelineRecordFeedLinesWrapper,
        queue: &LiveLogQueue,
    ) -> bool {
        for attempt in 0..RETRIES {
            // Hold the mask gate for reading across the final mask snapshot
            // and the send: a concurrent `::add-mask::` registration either
            // completes fully before this snapshot (and is included in it) or
            // waits until the send finishes, so a batch can never go out with
            // a snapshot taken before a completed registration. The guard is
            // scoped to one attempt so backoff/reconnect between attempts do
            // not block mask registration.
            let sent = {
                // `tokio::sync::RwLock` guards are `Send`, unlike the std /
                // parking_lot ones, so this can be held across the await.
                let _gate = queue.mask_gate.read().await;
                let masked = match mask_wrapper(wrapper, &queue.masks) {
                    Some(wrapper) => wrapper,
                    None => {
                        warn!("live log mask set is unavailable; refusing to transmit batch");
                        return false;
                    }
                };
                let payload = match serde_json::to_string(&masked) {
                    Ok(payload) => payload,
                    Err(error) => {
                        warn!(%error, "serializing live log wrapper failed");
                        return false;
                    }
                };
                self.ws.send(Message::Text(payload)).await.is_ok()
            };
            if sent {
                return true;
            }

            // Only backoff and reconnect if we have more attempts left;
            // don't waste time on the final failed attempt.
            if attempt + 1 < RETRIES {
                random_backoff().await;
                if !self.reconnect().await {
                    // Stamp the failure window the way upstream's
                    // `_lastConnectionFailure` does, so the feed is not
                    // re-dialed again until it elapses.
                    queue.record_connect_failure();
                }
            }
        }

        false
    }

    async fn close(&mut self) {
        let _ = self.ws.close(None).await;
    }
}

async fn connect_websocket(
    url: &str,
    token: &str,
    connect_attempts: usize,
) -> Option<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    for attempt in 0..connect_attempts {
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
        // Upstream's CreateWebSocketClient sets both headers on every fresh
        // socket, so reconnects resend the same Authorization + User-Agent.
        if let Ok(value) = format!(
            "preloop-runner/{} (protocol-compat {})",
            crate::VERSION,
            crate::PROTOCOL_COMPAT_VERSION
        )
        .parse()
        {
            request.headers_mut().insert(header::USER_AGENT, value);
        }

        match tokio::time::timeout(CONNECT_TIMEOUT, connect_async(request)).await {
            Ok(Ok((ws, _))) => return Some(ws),
            Ok(Err(error)) => debug!(%error, attempt, "live log websocket connect failed"),
            Err(_) => debug!(attempt, "live log websocket connect timed out"),
        }
        // Only back off when another attempt follows; sleeping after the last
        // failure just delays the caller's fall back to blob-only logging.
        if attempt + 1 < connect_attempts {
            random_backoff().await;
        }
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

/// Re-mask a sequence of physical log lines against the current secrets.
///
/// Lines are joined, masked, and split so a secret spanning a line boundary
/// still matches. `mask_secrets_preserving_lines` re-emits every newline
/// inside a matched secret, so the output line count always equals the input
/// line count and the split is one-to-one.
fn mask_lines<'a, I>(lines: I, secrets: &[String]) -> Vec<String>
where
    I: IntoIterator<Item = &'a str>,
{
    let joined = lines.into_iter().collect::<Vec<_>>().join("\n");
    if joined.is_empty() {
        return Vec::new();
    }
    preloop_gha_protocol::masking::mask_secrets_preserving_lines(
        &joined,
        secrets.iter().map(String::as_str),
        &[],
    )
    .split('\n')
    .map(str::to_owned)
    .collect()
}

fn mask_wrapper(
    wrapper: &TimelineRecordFeedLinesWrapper,
    masks: &Arc<RwLock<HashSet<String>>>,
) -> Option<TimelineRecordFeedLinesWrapper> {
    let masks = masks.read().ok()?;
    let secrets: Vec<String> = masks.iter().cloned().collect();
    let value = mask_lines(wrapper.value.iter().map(String::as_str), &secrets);
    Some(TimelineRecordFeedLinesWrapper {
        step_id: wrapper.step_id.clone(),
        start_line: wrapper.start_line,
        count: wrapper.count,
        value,
    })
}

fn line_batches(lines: Vec<ConsoleLineInfo>) -> Vec<Vec<ConsoleLineInfo>> {
    let mut grouped: BTreeMap<String, Vec<ConsoleLineInfo>> = BTreeMap::new();
    for line in lines {
        grouped.entry(line.step_id.clone()).or_default().push(line);
    }

    let mut batches = Vec::new();
    for lines in grouped.into_values() {
        for chunk in lines.chunks(LINES_PER_BATCH) {
            if !chunk.is_empty() {
                batches.push(chunk.to_vec());
            }
        }
    }
    batches
}

fn wrapper_from_lines(lines: &[ConsoleLineInfo]) -> TimelineRecordFeedLinesWrapper {
    TimelineRecordFeedLinesWrapper {
        step_id: lines[0].step_id.clone(),
        start_line: lines[0].line_number,
        count: lines.len(),
        value: lines.iter().map(|line| line.line.clone()).collect(),
    }
}

#[cfg(test)]
fn wrappers_from_lines(lines: Vec<ConsoleLineInfo>) -> Vec<TimelineRecordFeedLinesWrapper> {
    line_batches(lines)
        .iter()
        .map(|batch| wrapper_from_lines(batch))
        .collect()
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
        let masked = if let Ok(masks) = live_masks.read() {
            preloop_gha_protocol::masking::mask_secrets(line, masks.iter().map(String::as_str), &[])
        } else {
            line.to_string()
        };
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
    fn queued_lines_are_remasked_before_delivery() {
        let queue = LiveLogQueue::disconnected();
        queue.enqueue("step", "token hunter2-secret", 1);
        queue.remask_with(&["hunter2-secret".to_owned()]);
        let lines = queue.dequeue(DRAIN_LIMIT);
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].line, "token ***");
    }
    #[test]
    fn dequeued_batches_are_remasked_from_current_masks() {
        let masks = Arc::new(RwLock::new(HashSet::new()));
        let wrapper = TimelineRecordFeedLinesWrapper {
            step_id: "step".to_owned(),
            start_line: 1,
            count: 1,
            value: vec!["token hunter2-secret".to_owned()],
        };

        assert_eq!(
            mask_wrapper(&wrapper, &masks).unwrap().value,
            vec!["token hunter2-secret"]
        );
        masks.write().unwrap().insert("hunter2-secret".to_owned());
        assert_eq!(
            mask_wrapper(&wrapper, &masks).unwrap().value,
            vec!["token ***"]
        );
    }
    #[test]
    fn remasked_batch_redacts_secret_spanning_lines() {
        let masks = Arc::new(RwLock::new(HashSet::new()));
        let wrapper = TimelineRecordFeedLinesWrapper {
            step_id: "step".to_owned(),
            start_line: 1,
            count: 3,
            value: vec![
                "prefix token-a".to_owned(),
                "token-b suffix".to_owned(),
                "unrelated".to_owned(),
            ],
        };

        masks.write().unwrap().insert("token-a\ntoken-b".to_owned());
        let masked = mask_wrapper(&wrapper, &masks).unwrap();
        assert_eq!(masked.value, vec!["prefix ***", "*** suffix", "unrelated"]);
        assert_eq!(masked.count, 3);
    }

    #[test]
    fn queued_lines_remask_secret_spanning_lines() {
        let queue = LiveLogQueue::disconnected();
        queue.enqueue("step", "prefix token-a", 1);
        queue.enqueue("step", "token-b suffix", 2);
        queue.remask_with(&["token-a\ntoken-b".to_owned()]);

        let lines = queue.dequeue(DRAIN_LIMIT);
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].line, "prefix ***");
        assert_eq!(lines[1].line, "*** suffix");
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

    #[tokio::test]
    async fn drain_keeps_lines_queued_until_socket_connects() {
        let queue = LiveLogQueue::disconnected();
        queue.enqueue("step", "first", 1);

        queue.drain_once(DRAIN_LIMIT).await;

        let lines = queue.dequeue(DRAIN_LIMIT);
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].line, "first");
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

    #[tokio::test]
    async fn periodic_reconnect_uses_one_dial_after_retry_window() {
        use tokio::net::TcpListener;

        let placeholder = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = placeholder.local_addr().unwrap();
        drop(placeholder);

        let retry_interval = Duration::from_millis(50);
        let queue = LiveLogQueue::connect_with_retry(
            format!("ws://{addr}/ws/live-logs/job"),
            "job-token".to_owned(),
            Arc::new(RwLock::new(HashSet::new())),
            retry_interval,
        );

        let deadline = Instant::now() + Duration::from_secs(10);
        let failed_at = loop {
            if let Some(failed_at) = *queue
                .last_connect_failure
                .lock()
                .expect("live log failure clock poisoned")
            {
                break failed_at;
            }
            assert!(Instant::now() < deadline, "initial connect never failed");
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        while failed_at.elapsed() < retry_interval {
            assert!(Instant::now() < deadline, "retry window never elapsed");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let attempts_for_server = Arc::clone(&attempts);
        let listener = TcpListener::bind(addr).await.unwrap();
        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                attempts_for_server.fetch_add(1, Ordering::SeqCst);
                drop(stream);
            }
        });

        queue.drain_once(DRAIN_LIMIT).await;
        let deadline = Instant::now() + Duration::from_secs(5);
        while queue.connect_in_flight.load(Ordering::SeqCst) {
            assert!(Instant::now() < deadline, "periodic connect never finished");
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;

        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        server.abort();
    }

    /// Forced first-connect failure, then the endpoint comes up: the drain
    /// loop must re-dial a fresh socket (upstream v2.338.0 recreates the
    /// ClientWebSocket per attempt) and resume the feed once the
    /// `_lastConnectionFailure`-style retry window elapses.
    #[tokio::test]
    async fn feed_reestablishes_after_first_connect_failure() {
        use futures::StreamExt;
        use tokio::net::TcpListener;
        use tokio_tungstenite::accept_hdr_async;
        use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

        // Reserve a port, then drop the listener so the first connect fails.
        let placeholder = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = placeholder.local_addr().unwrap();
        drop(placeholder);

        let queue = LiveLogQueue::connect_with_retry(
            format!("ws://{addr}/ws/live-logs/job"),
            "job-token".to_owned(),
            Arc::new(RwLock::new(HashSet::new())),
            Duration::from_millis(500),
        );

        // Wait until the initial connect cycle gives up and stamps the
        // failure clock (connect-refused returns well inside the timeouts).
        let deadline = Instant::now() + Duration::from_secs(10);
        while queue
            .last_connect_failure
            .lock()
            .expect("live log failure clock poisoned")
            .is_none()
        {
            assert!(Instant::now() < deadline, "initial connect never failed");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        // Inside the retry window a drain must not re-dial yet, even though
        // lines are waiting.
        queue.enqueue("step", "still-queued", 1);
        queue.drain_once(DRAIN_LIMIT).await;
        assert!(queue.ws.lock().await.is_none());
        assert!(!queue.connect_in_flight.load(Ordering::SeqCst));

        // Endpoint comes up; capture the handshake headers to prove the
        // reconnect resends Authorization + User-Agent like upstream's
        // CreateWebSocketClient.
        let (headers_tx, headers_rx) = tokio::sync::oneshot::channel();
        let (msg_tx, msg_rx) = tokio::sync::oneshot::channel();
        let listener = TcpListener::bind(addr).await.unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            // tungstenite's `ErrorResponse` makes the callback `Err` arm
            // large; the signature is fixed by the `Callback` trait.
            #[allow(clippy::result_large_err)]
            let mut ws = accept_hdr_async(stream, move |request: &Request, response: Response| {
                let _ = headers_tx.send((
                    request
                        .headers()
                        .get(header::AUTHORIZATION)
                        .and_then(|v| v.to_str().ok().map(str::to_owned)),
                    request
                        .headers()
                        .get(header::USER_AGENT)
                        .and_then(|v| v.to_str().ok().map(str::to_owned)),
                ));
                Ok(response)
            })
            .await
            .unwrap();
            if let Some(Ok(Message::Text(text))) = ws.next().await {
                let _ = msg_tx.send(text);
            }
        });

        // Once the window elapses a drain re-dials and installs a sender.
        let deadline = Instant::now() + Duration::from_secs(10);
        while queue.ws.lock().await.is_none() {
            assert!(Instant::now() < deadline, "feed did not re-establish");
            queue.drain_once(DRAIN_LIMIT).await;
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let (authorization, user_agent) = headers_rx.await.unwrap();
        assert_eq!(authorization.as_deref(), Some("Bearer job-token"));
        assert!(user_agent.unwrap().starts_with("preloop-runner/"));

        // Queued lines flush over the re-established feed.
        queue.enqueue("step", "after-reconnect", 2);
        queue.drain_once(DRAIN_LIMIT).await;
        let text = msg_rx.await.unwrap();
        let wrapper: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(wrapper["stepId"], "step");
        assert_eq!(
            wrapper["value"],
            serde_json::json!(["still-queued", "after-reconnect"])
        );
    }
}
