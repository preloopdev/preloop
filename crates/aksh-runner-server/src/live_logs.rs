use super::*;

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

pub(crate) fn live_log_key_for_job(inner: &InnerState, run_id: RunId, job_id: &str) -> Option<String> {
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

pub(crate) async fn handle_live_log_socket(mut socket: WebSocket, job_id: String, shared: Arc<SharedState>) {
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
    // Push and broadcast under per-job lock only.
    job_lines.lock().await.push(wrapper.clone());
    let _ = tx.send(wrapper);
}
