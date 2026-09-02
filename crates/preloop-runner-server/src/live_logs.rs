use super::*;

/// Per-job live console feed buffer.
///
/// The ingestion paths (the live-log WebSocket and the `TimeLineWebConsoleLog`
/// POST) are driven by authenticated runners, but a long-lived or hung job can
/// stream output for hours, so the retained history is capped by byte size
/// instead of growing without bound for the job's lifetime. Oldest wrappers
/// are dropped past the cap — mid-job SSE viewers see the retained tail, and
/// the durable step-log blob remains the source of truth for full logs. A
/// single wrapper larger than the whole budget is dropped outright: storing it
/// would evict the entire existing tail for one batch, and broadcasting it
/// would amplify across every subscriber.
#[derive(Clone)]
pub(crate) struct LiveLogBuffer {
    pub(crate) lines: VecDeque<LiveLogFeedLinesWrapper>,
    bytes: usize,
    max_bytes: usize,
}

impl LiveLogBuffer {
    /// Default per-job cap on retained live feed bytes. The accounting covers
    /// the wrapper struct, the step id, and every line string (header plus
    /// bytes), so it bounds heap use regardless of wrapper shape.
    pub(crate) const DEFAULT_MAX_BYTES: usize = 64 * 1024 * 1024;

    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            lines: VecDeque::new(),
            bytes: 0,
            max_bytes,
        }
    }

    pub(crate) fn total_bytes(&self) -> usize {
        self.bytes
    }

    /// Whether a single wrapper is small enough to be retained on its own,
    /// regardless of any eviction it may trigger. Used to reject oversized
    /// batches before they are cloned for the buffer.
    pub(crate) fn fits(&self, wrapper: &LiveLogFeedLinesWrapper) -> bool {
        Self::wrapper_bytes(wrapper) <= self.max_bytes
    }

    /// Append a wrapper, tail-dropping the oldest wrappers once the byte cap
    /// is exceeded. Returns `false` (storing nothing) when a single wrapper
    /// exceeds the whole budget.
    pub(crate) fn push(&mut self, wrapper: LiveLogFeedLinesWrapper) -> bool {
        let size = Self::wrapper_bytes(&wrapper);
        if size > self.max_bytes {
            return false;
        }
        self.bytes += size;
        self.lines.push_back(wrapper);
        while self.bytes > self.max_bytes {
            if let Some(oldest) = self.lines.pop_front() {
                self.bytes -= Self::wrapper_bytes(&oldest);
            }
        }
        self.compact_if_wasteful();
        true
    }
    /// Discard a completed attempt's retained tail before a retry reuses the
    /// same agent-job key, without changing the configured byte budget.
    pub(crate) fn clear(&mut self) {
        self.lines.clear();
        self.bytes = 0;
    }

    /// Reclaim `VecDeque` backing-buffer slack after evictions. `pop_front`
    /// never shrinks the allocation, so a batch of evictions can leave
    /// capacity far above the retained length (many small wrappers displaced
    /// by one large one). Shrinking keeps that unused allocation bounded.
    fn compact_if_wasteful(&mut self) {
        let len = self.lines.len();
        if self.lines.capacity() > len.saturating_mul(2).saturating_add(64) {
            self.lines.shrink_to_fit();
        }
    }

    fn wrapper_bytes(wrapper: &LiveLogFeedLinesWrapper) -> usize {
        std::mem::size_of::<LiveLogFeedLinesWrapper>()
            + wrapper.step_id.capacity()
            + wrapper.value.capacity() * std::mem::size_of::<String>()
            + wrapper
                .value
                .iter()
                .map(|line| line.capacity())
                .sum::<usize>()
    }
}

impl Default for LiveLogBuffer {
    fn default() -> Self {
        Self::new(Self::DEFAULT_MAX_BYTES)
    }
}

impl IntoIterator for LiveLogBuffer {
    type Item = LiveLogFeedLinesWrapper;
    type IntoIter = std::collections::vec_deque::IntoIter<LiveLogFeedLinesWrapper>;

    fn into_iter(self) -> Self::IntoIter {
        self.lines.into_iter()
    }
}

pub(crate) async fn live_logs_sse(
    State(shared): State<Arc<SharedState>>,
    Path((run_id, job_id)): Path<(RunId, String)>,
) -> Result<Sse<impl futures::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError> {
    live_log_stream(&shared, run_id, &job_id).await
}

/// Job selector for the native live-log route.
///
/// Mirrors `RunLogsQuery::job` so `--job` means the same thing whether the
/// caller is reading a finished log or following a running one.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct LiveLogsQuery {
    #[serde(default)]
    job: Option<String>,
}

/// Native-bearer live console feed for one job of a run.
///
/// The runner-protocol route above is reachable only with a runner or job
/// token, which the CLI does not hold. This exposes the same feed to the
/// native API principal — no new information, since that principal can already
/// read the whole log through `GET /api/v1/runs/:run_id/logs`.
///
/// `job` may be omitted when the run has exactly one job; with more, the
/// candidates are named rather than one being chosen arbitrarily.
pub(crate) async fn live_run_logs_sse(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
    Query(query): Query<LiveLogsQuery>,
) -> Result<Sse<impl futures::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError> {
    let job_id = match query.job {
        Some(job) => job,
        None => {
            let inner = shared.state.inner.lock().await;
            let run = inner
                .runs
                .get(&run_id)
                .ok_or_else(|| ApiError::not_found("run not found"))?;
            let mut jobs: Vec<String> = run.jobs.keys().map(|job_id| job_id.0.clone()).collect();
            jobs.extend(
                inner
                    .job_requests
                    .values()
                    .filter(|request| request.run_id == run_id)
                    .map(|request| request.job_id.0.clone()),
            );
            jobs.sort_unstable();
            jobs.dedup();
            match jobs.len() {
                0 => return Err(ApiError::not_found("run has no jobs to follow")),
                1 => jobs.pop().expect("one job was counted"),
                _ => {
                    return Err(ApiError::bad_request(format!(
                        "`job` needs a value when a run has {} jobs: {}",
                        jobs.len(),
                        jobs.join(", ")
                    )));
                }
            }
        }
    };
    live_log_stream(&shared, run_id, &job_id).await
}

/// Replay the retained per-job buffer, then follow the live broadcast.
///
/// Snapshotting and subscribing happen while the global lock and the
/// per-job lock are both held. Ingestion takes the same locks in that order,
/// so a wrapper is observed either in the snapshot or in the receiver, never
/// both.
async fn live_log_stream(
    shared: &Arc<SharedState>,
    run_id: RunId,
    job_id: &str,
) -> Result<Sse<impl futures::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError> {
    let (snapshot, subscription) = {
        let mut inner = shared.state.inner.lock().await;
        let key = live_log_key_for_job(&inner, run_id, job_id)
            .ok_or_else(|| ApiError::not_found("job not found"))?;
        let lines_arc = inner.live_log_lines.entry(key.clone()).or_default().clone();
        let lines = lines_arc.lock().await;
        let snapshot = lines.clone();
        let subscription = if live_log_is_closed(&inner, run_id, job_id, &key) {
            None
        } else {
            Some(live_log_sender(&mut inner, &key).subscribe())
        };
        // Keep the guard alive through subscription creation; this explicit
        // binding makes the lock ordering above visible to future edits.
        drop(lines);
        (snapshot, subscription)
    };

    let snapshot_stream = stream::iter(
        snapshot
            .into_iter()
            .map(|wrapper| Ok(live_log_sse_event(&wrapper))),
    );
    let live_stream = stream::unfold(subscription, |subscription| async move {
        // A closed job has no subscription: the snapshot is the whole stream.
        let mut rx = subscription?;
        loop {
            match rx.recv().await {
                Ok(wrapper) => {
                    let event = live_log_sse_event(&wrapper);
                    return Some((Ok(event), Some(rx)));
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    });

    Ok(Sse::new(snapshot_stream.chain(live_stream)).keep_alive(KeepAlive::default()))
}

pub(crate) fn live_log_sse_event(wrapper: &LiveLogFeedLinesWrapper) -> Event {
    let data = serde_json::to_string(wrapper).unwrap_or_else(|_| "{}".to_string());
    Event::default().event("live-log").data(data)
}

pub(crate) fn live_log_key_for_job(
    inner: &InnerState,
    run_id: RunId,
    job_id: &str,
) -> Option<String> {
    let run = inner.runs.get(&run_id)?;
    // `job_requests` is keyed by monotonic request id, so `find` would return
    // the oldest attempt and follow a dead feed after a re-dispatch. A logical
    // job key means "the current attempt"; an explicit agent job id matches one
    // record either way.
    if let Some(record) = inner
        .job_requests
        .values()
        .filter(|record| {
            record.run_id == run_id
                && (record.job_id.0 == job_id || record.agent_job_id.to_string() == job_id)
        })
        .max_by_key(|record| record.request_id)
    {
        return Some(record.agent_job_id.to_string());
    }
    // A logical job key is valid without a request only when it belongs to
    // this run. Never accept an arbitrary globally present live-log key:
    // UUIDs are only scoped by their job request and otherwise could leak a
    // different run's output.
    run.jobs
        .contains_key(&JobId(job_id.to_owned()))
        .then(|| job_id.to_owned())
}

/// Whether a live stream for this run/job must end after its snapshot.
fn live_log_is_closed(inner: &InnerState, run_id: RunId, job_id: &str, key: &str) -> bool {
    if inner.live_log_closed.contains(key) {
        return true;
    }
    let run_terminal = inner
        .runs
        .get(&run_id)
        .is_some_and(|run| run.status.is_terminal());
    let logical_job_terminal = inner
        .runs
        .get(&run_id)
        .and_then(|run| run.jobs.get(&JobId(job_id.to_owned())))
        .is_some_and(|status| status.is_terminal());
    run_terminal || logical_job_terminal
}

pub(crate) fn live_log_sender(
    inner: &mut InnerState,
    key: &str,
) -> broadcast::Sender<LiveLogFeedLinesWrapper> {
    inner
        .live_log_tx
        .entry(key.to_string())
        .or_insert_with(|| {
            let (tx, _) = broadcast::channel(1024);
            tx
        })
        .clone()
}

/// Mark a job's live-log feed complete and drop its broadcast sender.
///
/// Dropping the last sender makes every current follower's `recv()` return
/// `Closed`, so their streams end and `preloop logs -f` exits — no status
/// polling. The `live_log_closed` mark is what handles a follower that arrives
/// *after* completion: `live_log_stream` serves the retained snapshot and does
/// not resubscribe, instead of `live_log_sender` lazily minting a fresh
/// channel that would never speak again.
///
/// Idempotent, and safe to call for a job that never streamed a line.
pub(crate) fn close_live_log(inner: &mut InnerState, key: &str) {
    inner.live_log_closed.insert(key.to_string());
    inner.live_log_tx.remove(key);
}

pub(crate) async fn ws_live_logs(
    State(shared): State<Arc<SharedState>>,
    Path(job_id): Path<String>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_live_log_socket(socket, job_id, shared))
}

pub(crate) async fn handle_live_log_socket(
    mut socket: WebSocket,
    job_id: String,
    shared: Arc<SharedState>,
) {
    const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
    loop {
        let message = match tokio::time::timeout(IDLE_TIMEOUT, socket.next()).await {
            Ok(Some(msg)) => msg,
            Ok(None) => break, // stream ended
            Err(_) => {
                debug!(%job_id, "live log websocket idle for 5m, closing");
                break;
            }
        };
        match message {
            Ok(WsMessage::Text(text)) => {
                match serde_json::from_str::<LiveLogFeedLinesWrapper>(&text) {
                    Ok(mut wrapper) => {
                        wrapper.count = wrapper.value.len();
                        record_live_log_wrapper(&shared, &job_id, wrapper).await;
                    }
                    Err(error) => warn!(%error, %job_id, "invalid live log websocket payload"),
                }
            }
            Ok(WsMessage::Ping(data)) => {
                if socket.send(WsMessage::Pong(data)).await.is_err() {
                    break;
                }
            }
            Ok(WsMessage::Binary(_)) | Ok(WsMessage::Pong(_)) => {}
            Ok(WsMessage::Close(_)) => break,
            Err(error) => {
                warn!(%error, %job_id, "live log websocket receive failed");
                break;
            }
        }
    }
}
/// Record one live-log wrapper under a canonical run/job key.
///
/// The global and per-job locks are acquired in that order and held through
/// the retained-buffer push and broadcast. `live_log_stream` uses the same
/// ordering for its snapshot/subscription pair, which makes the handoff
/// atomic from a follower's perspective.
pub(crate) async fn record_live_log_wrapper(
    shared: &Arc<SharedState>,
    job_id: &str,
    wrapper: LiveLogFeedLinesWrapper,
) {
    let mut inner = shared.state.inner.lock().await;
    // Fresh lines for a key we had marked complete mean the job is producing
    // again (a retry reusing the same agent job). Reopen it and replace the
    // prior attempt's retained tail before broadcasting.
    let reopened = inner.live_log_closed.remove(job_id);
    let lines_arc = inner
        .live_log_lines
        .entry(job_id.to_string())
        .or_default()
        .clone();
    let tx = live_log_sender(&mut inner, job_id);
    let mut lines = lines_arc.lock().await;
    if reopened {
        lines.clear();
    }
    // Hold the per-job lock across both the buffer push and the broadcast so
    // concurrent ingestion cannot reorder live fan-out relative to the
    // retained tail. Reject oversized batches before cloning, then drop them
    // from both the retained buffer and the live fan-out so one pathological
    // frame cannot evict the tail or amplify across subscribers.
    if lines.fits(&wrapper) {
        lines.push(wrapper.clone());
        let _ = tx.send(wrapper);
    } else {
        warn!(%job_id, "dropping live log batch exceeding per-job buffer cap");
    }
}

/// Resolve a logical job key to its run-scoped live-log key before recording.
pub(crate) async fn record_live_log_wrapper_for_run(
    shared: &Arc<SharedState>,
    run_id: RunId,
    job_id: &str,
    wrapper: LiveLogFeedLinesWrapper,
) {
    let key = {
        let inner = shared.state.inner.lock().await;
        live_log_key_for_job(&inner, run_id, job_id).unwrap_or_else(|| job_id.to_owned())
    };
    record_live_log_wrapper(shared, &key, wrapper).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wrapper(step_id: &str, line: &str) -> LiveLogFeedLinesWrapper {
        LiveLogFeedLinesWrapper {
            step_id: step_id.to_string(),
            start_line: 1,
            count: 1,
            value: vec![line.to_string()],
        }
    }

    #[test]
    fn buffer_tail_drops_oldest_wrappers_past_cap() {
        // One wrapper's accounted size: struct (64) + step id + String header
        // (24) + line bytes.
        let one = wrapper("a", "hello");
        let size = LiveLogBuffer::wrapper_bytes(&one);
        assert_eq!(size, 64 + 1 + 24 + 5);

        let mut buffer = LiveLogBuffer::new(size + 10);
        assert!(buffer.push(wrapper("a", "hello")));
        assert_eq!(buffer.lines.len(), 1);
        assert_eq!(buffer.total_bytes(), size);

        // Second wrapper pushes the total over the cap; the oldest is dropped.
        assert!(buffer.push(wrapper("b", "hello")));
        assert_eq!(buffer.lines.len(), 1);
        assert_eq!(buffer.lines[0].step_id, "b");
        assert_eq!(buffer.total_bytes(), size);

        // Eviction continues until the buffer fits inside the cap.
        let mut buffer = LiveLogBuffer::new(size * 2 + 5);
        assert!(buffer.push(wrapper("a", "hello")));
        assert!(buffer.push(wrapper("b", "hello")));
        assert!(buffer.push(wrapper("c", "hello")));
        assert_eq!(buffer.lines.len(), 2);
        assert_eq!(buffer.lines[0].step_id, "b");
        assert_eq!(buffer.lines[1].step_id, "c");
        assert_eq!(buffer.total_bytes(), size * 2);
    }

    #[test]
    fn buffer_compacts_deque_capacity_after_large_evictions() {
        let small = wrapper("a", "hello");
        let small_size = LiveLogBuffer::wrapper_bytes(&small);

        let mut buffer = LiveLogBuffer::new(small_size * 128);
        for _ in 0..128 {
            assert!(buffer.push(wrapper("a", "hello")));
        }
        assert!(buffer.lines.capacity() >= 128);

        // One wrapper accounting for ~120 small wrappers evicts the bulk of
        // the retained tail. `pop_front` alone would leave the deque backing
        // allocation at its old size, so this also verifies compaction.
        let large = LiveLogFeedLinesWrapper {
            step_id: String::new(),
            start_line: 1,
            count: 1,
            value: vec!["x".repeat(small_size * 120)],
        };
        assert!(buffer.push(large));

        let len = buffer.lines.len();
        assert!(
            len < 16,
            "large wrapper should evict most small wrappers, got {len}"
        );
        assert!(
            buffer.lines.capacity() <= len * 2 + 64,
            "deque capacity {} should shrink near retained length {len}",
            buffer.lines.capacity()
        );
    }

    #[test]
    fn buffer_rejects_wrapper_larger_than_whole_budget() {
        let mut buffer = LiveLogBuffer::new(100);
        assert!(buffer.push(wrapper("a", "hello")));

        let oversized = LiveLogFeedLinesWrapper {
            step_id: "huge".to_string(),
            start_line: 1,
            count: 1,
            value: vec!["x".repeat(200)],
        };
        assert!(!buffer.push(oversized));
        assert_eq!(buffer.lines.len(), 1);
        assert_eq!(buffer.lines[0].step_id, "a");
    }

    #[test]
    fn buffer_clones_preserve_retained_tail() {
        let mut buffer =
            LiveLogBuffer::new(LiveLogBuffer::wrapper_bytes(&wrapper("a", "hello")) + 10);
        buffer.push(wrapper("a", "hello"));
        buffer.push(wrapper("b", "hello"));
        assert_eq!(buffer.lines.len(), 1);

        let snapshot = buffer.clone();
        assert_eq!(snapshot.lines.len(), 1);
        assert_eq!(snapshot.lines[0].step_id, "b");
        assert_eq!(snapshot.total_bytes(), buffer.total_bytes());

        let replayed: Vec<String> = snapshot.into_iter().map(|w| w.step_id).collect();
        assert_eq!(replayed, vec!["b".to_string()]);
    }
}
