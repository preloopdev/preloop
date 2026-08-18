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
        true
    }

    fn wrapper_bytes(wrapper: &LiveLogFeedLinesWrapper) -> usize {
        std::mem::size_of::<LiveLogFeedLinesWrapper>()
            + wrapper.step_id.len()
            + wrapper.value.len() * std::mem::size_of::<String>()
            + wrapper.value.iter().map(|line| line.len()).sum::<usize>()
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
    // Grab per-job handles under the global lock, then drop it immediately.
    let (job_lines, rx) = {
        let mut inner = shared.state.inner.lock().await;
        let key = live_log_key_for_job(&inner, run_id, &job_id)
            .ok_or_else(|| ApiError::not_found("job not found"))?;
        let lines_arc = inner.live_log_lines.entry(key.clone()).or_default().clone();
        let tx = live_log_sender(&mut inner, &key);
        (lines_arc, tx.subscribe())
    };
    // Snapshot under per-job lock only — does not block global state.
    let snapshot = job_lines.lock().await.clone();

    let snapshot_stream = stream::iter(
        snapshot
            .into_iter()
            .map(|wrapper| Ok(live_log_sse_event(&wrapper))),
    );
    let live_stream = stream::unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(wrapper) => {
                    let event = live_log_sse_event(&wrapper);
                    return Some((Ok(event), rx));
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
    inner.runs.get(&run_id)?;
    inner
        .job_requests
        .values()
        .find(|record| {
            record.run_id == run_id
                && (record.job_id.0 == job_id || record.agent_job_id.to_string() == job_id)
        })
        .map(|record| record.agent_job_id.to_string())
        .or_else(|| Some(job_id.to_string()).filter(|key| inner.live_log_lines.contains_key(key)))
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

pub(crate) async fn record_live_log_wrapper(
    shared: &Arc<SharedState>,
    job_id: &str,
    wrapper: LiveLogFeedLinesWrapper,
) {
    // Grab per-job Arc and broadcast sender under the global lock, then release it.
    let (job_lines, tx) = {
        let mut inner = shared.state.inner.lock().await;
        let lines_arc = inner
            .live_log_lines
            .entry(job_id.to_string())
            .or_default()
            .clone();
        let tx = live_log_sender(&mut inner, job_id);
        (lines_arc, tx)
    };
    // Push and broadcast under per-job lock only. An oversized batch is
    // dropped from both the retained buffer and the live fan-out, so one
    // pathological frame cannot evict the tail or amplify across subscribers.
    let stored = job_lines.lock().await.push(wrapper.clone());
    if stored {
        let _ = tx.send(wrapper);
    } else {
        warn!(%job_id, "dropping live log batch exceeding per-job buffer cap");
    }
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
