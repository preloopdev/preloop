use super::*;

#[derive(serde::Deserialize)]
pub(crate) struct RegisterDapPortRequest {
    port: u16,
    job_id: JobId,
}

pub(crate) async fn register_dap_port(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
    Json(payload): Json<RegisterDapPortRequest>,
) -> Result<StatusCode, ApiError> {
    if payload.port < 1024 {
        return Err(ApiError::bad_request(
            "DAP port must be an unprivileged local port",
        ));
    }
    let mut inner = shared.state.inner.lock().await;
    let run = inner
        .runs
        .get(&run_id)
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    let status = run
        .jobs
        .get(&payload.job_id)
        .copied()
        .ok_or_else(|| ApiError::bad_request("job does not belong to run"))?;
    if !matches!(status, ExecutionStatus::InProgress) {
        return Err(ApiError::bad_request(
            "DAP port can only be registered for an in-progress job",
        ));
    }
    inner.dap_ports.insert(
        run_id,
        DapPortRegistration {
            port: payload.port,
            job_id: payload.job_id.clone(),
        },
    );
    info!(%run_id, job_id = %payload.job_id, port = payload.port, "Registered DAP port");
    Ok(StatusCode::OK)
}

pub(crate) async fn ws_dap_debug(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
    ws: WebSocketUpgrade,
) -> impl axum::response::IntoResponse {
    ws.on_upgrade(move |socket| handle_dap_debug_socket(socket, run_id, shared))
}

pub(crate) async fn handle_dap_debug_socket(
    socket: WebSocket,
    run_id: RunId,
    shared: Arc<SharedState>,
) {
    let registration = {
        let inner = shared.state.inner.lock().await;
        inner.dap_ports.get(&run_id).cloned()
    };
    let (port, job_id_str) = match registration {
        Some(reg) => (reg.port, reg.job_id.to_string()),
        None => {
            info!(%run_id, "No DAP port registered; falling back to default port 4711");
            (4711, "official".to_string())
        }
    };

    info!(%run_id, job_id = %job_id_str, port, "Starting DAP websocket proxy to runner");
    if let Err(e) = pump_axum_ws_to_dap(socket, port).await {
        warn!(%run_id, job_id = %job_id_str, port, "DAP websocket proxy ended with error: {e}");
    }
}

pub(crate) async fn pump_axum_ws_to_dap(
    ws: WebSocket,
    target_port: u16,
) -> Result<(), anyhow::Error> {
    use futures::{SinkExt, StreamExt};

    let url = format!("ws://127.0.0.1:{target_port}");
    let mut target_ws = None;
    for _ in 0..50 {
        match tokio_tungstenite::connect_async(&url).await {
            Ok((stream, _)) => {
                target_ws = Some(stream);
                break;
            }
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }
    let target_ws = target_ws
        .ok_or_else(|| anyhow::anyhow!("failed to connect to runner DAP bridge after retries"))?;

    let (mut target_sink, mut target_stream) = target_ws.split();
    let (mut ws_sink, mut ws_stream) = ws.split();

    let to_target = async {
        while let Some(msg) = ws_stream.next().await {
            match msg {
                Ok(WsMessage::Text(text)) => {
                    target_sink
                        .send(tokio_tungstenite::tungstenite::Message::Text(text))
                        .await
                        .map_err(|e| anyhow::anyhow!("target ws send: {e}"))?;
                }
                Ok(WsMessage::Binary(_)) => {
                    return Err(anyhow::anyhow!(
                        "binary WebSocket frames are not allowed on the DAP bridge"
                    ));
                }
                Ok(WsMessage::Close(_)) | Err(_) => break,
                Ok(WsMessage::Ping(_)) | Ok(WsMessage::Pong(_)) => continue,
            }
        }
        Ok::<_, anyhow::Error>(())
    };

    let from_target = async {
        while let Some(msg) = target_stream.next().await {
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                    ws_sink
                        .send(WsMessage::Text(text))
                        .await
                        .map_err(|e| anyhow::anyhow!("ws send: {e}"))?;
                }
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) | Err(_) => break,
                Ok(_) => continue,
            }
        }
        Ok::<_, anyhow::Error>(())
    };

    tokio::select! {
        a = to_target => a?,
        b = from_target => b?,
    }
    Ok(())
}
