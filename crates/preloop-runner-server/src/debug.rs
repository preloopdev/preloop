use super::*;

use axum::Extension;

#[derive(serde::Deserialize)]
pub struct RegisterDapPortRequest {
    port: u16,
    job_id: JobId,
}

/// Publish the local port a run's DAP bridge listens on.
///
/// The registration is the *whole* authorization for the proxy that follows:
/// `handle_dap_debug_socket` dials `127.0.0.1:{port}` on the engine host, so
/// whatever registered it receives the operator's debug traffic and answers
/// it. Two things therefore have to hold, and the runner bearer alone proves
/// neither:
///
/// 1. the caller's runner must be the one serving this job — otherwise any
///    registered runner could redirect another run's debug channel to itself,
///    reading the debugged process's source, expressions and variable values
///    and impersonating its debug adapter;
/// 2. one run has one debug channel, so a registration for a different job of
///    the same run is refused while that job is still in progress, instead of
///    silently redirecting the channel away from the job already being
///    debugged.
///
/// A registration for the *same* job always replaces: the job is handed off
/// between runners (reclaim after runner loss) while the map still holds the
/// old runner's entry, and only the current owner may register. A registration
/// whose job already finished is stale and is replaced too.
pub async fn register_dap_port(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
    Extension(caller): Extension<RunnerIdentity>,
    Json(payload): Json<RegisterDapPortRequest>,
) -> Result<StatusCode, ApiError> {
    if payload.port < 1024 {
        return Err(ApiError::bad_request(
            "DAP port must be an unprivileged local port",
        ));
    }
    let caller_runner_id = caller
        .runner_id
        .ok_or_else(|| ApiError::forbidden("runner identity is required to register a DAP port"))?;
    // Fast reject, before taking the run's registration lock: a caller that
    // plainly does not serve this job must not make the legitimate owner wait.
    dap_registration_statuses(&shared, run_id, &payload.job_id).await?;
    dap_registration_owner(&shared, run_id, &payload.job_id, caller_runner_id).await?;

    // Serialize the check-then-insert for this run, and only this run. The
    // backend reads that authorize the write must not run while holding
    // `inner`: that is one global mutex guarding every control-plane map on
    // this node, so a single registered runner could otherwise pin it for as
    // long as the backend took to answer and stall unrelated runs. The
    // per-run lock keeps the property the checks need — no competing
    // registration for the same run can interleave between them and the
    // insert — without putting those awaits behind the global lock.
    let run_lock = dap_registration_lock(&shared, run_id).await;
    let _run_guard = run_lock.lock().await;

    // The checks above are repeated here, after the serialization point, and
    // the ownership one last. A request can pass them, lose the job to a
    // handoff, and still reach the insert; re-reading narrows that to the
    // window a handoff needs to commit inside the final lookup, which no
    // amount of queueing requests can widen. The lookup itself must stay
    // outside `inner`, so it is the last thing done before the map write
    // rather than something performed under the global lock.
    //
    // Refuse only what would take the channel away from a debug target that is
    // still live: another job of this run that is still running. Everything
    // else is a replacement — the same job from a new owner, or a registration
    // whose job is over — so a dead runner's entry, or one whose job already
    // finished, must not lock the next owner out. The statuses are read again
    // here rather than reused from the fast reject above because a job this
    // run requeued and re-claimed since then is in progress again, and
    // replacing its channel would cut off a live debug target.
    let existing = {
        let inner = shared.state.inner.lock().await;
        inner
            .dap_ports
            .get(&run_id)
            .map(|entry| entry.job_id.clone())
    };
    if let Some(existing) = existing
        && existing != payload.job_id
    {
        let jobs = dap_registration_statuses(&shared, run_id, &payload.job_id).await?;
        if jobs
            .iter()
            .any(|(job_id, status)| *job_id == existing && *status == ExecutionStatus::InProgress)
        {
            return Err(ApiError::conflict(
                "another job of this run is already being debugged",
            ));
        }
    }
    dap_registration_owner(&shared, run_id, &payload.job_id, caller_runner_id).await?;
    let mut inner = shared.state.inner.lock().await;
    inner.dap_ports.insert(
        run_id,
        DapPortRegistration {
            port: payload.port,
            job_id: payload.job_id.clone(),
        },
    );
    info!(
        %run_id,
        job_id = %payload.job_id,
        runner_id = caller_runner_id,
        port = payload.port,
        "Registered DAP port"
    );
    Ok(StatusCode::OK)
}

/// The mutex that serializes DAP port registrations for one run.
///
/// Created on first use so a run that never starts a debug bridge costs
/// nothing, and dropped with the run's other node-local state. `inner` is held
/// only long enough to clone the entry; the caller awaits the per-run mutex,
/// never the global one.
async fn dap_registration_lock(
    shared: &Arc<SharedState>,
    run_id: RunId,
) -> std::sync::Arc<tokio::sync::Mutex<()>> {
    let mut inner = shared.state.inner.lock().await;
    inner
        .dap_registration_locks
        .entry(run_id)
        .or_default()
        .clone()
}

/// The run's job statuses, refusing a job that is not in progress.
///
/// A registration is only meaningful for a job that is running: a finished
/// job has no debug adapter to reach, and a queued one has no machine yet.
async fn dap_registration_statuses(
    shared: &Arc<SharedState>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<Vec<(JobId, ExecutionStatus)>, ApiError> {
    let jobs = shared
        .state
        .backend
        .run_job_statuses(run_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    let status = jobs
        .iter()
        .find(|(id, _)| *id == *job_id)
        .map(|(_, status)| *status)
        .ok_or_else(|| ApiError::bad_request("job does not belong to run"))?;
    if !matches!(status, ExecutionStatus::InProgress) {
        return Err(ApiError::bad_request(
            "DAP port can only be registered for an in-progress job",
        ));
    }
    Ok(jobs)
}

/// Refuse a caller that is not the runner the job's current attempt names as
/// its owner.
///
/// The attempt records which runner claimed the job; that is the machine whose
/// DAP bridge may publish a port for it.
async fn dap_registration_owner(
    shared: &Arc<SharedState>,
    run_id: RunId,
    job_id: &JobId,
    caller_runner_id: i64,
) -> Result<(), ApiError> {
    let request = shared
        .state
        .backend
        .request(crate::control::backend::RequestKey::Job(
            run_id,
            job_id.clone(),
        ))
        .await
        .map_err(|error| match error {
            crate::control::ControlError::NotFound(_) => {
                ApiError::bad_request("job does not belong to run")
            }
            other => ApiError::from(other),
        })?;
    if request.owner_runner_id != Some(caller_runner_id) {
        return Err(ApiError::forbidden(
            "only the runner serving this job may register its DAP port",
        ));
    }
    Ok(())
}

pub async fn ws_dap_debug(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
    ws: WebSocketUpgrade,
) -> impl axum::response::IntoResponse {
    ws.on_upgrade(move |socket| handle_dap_debug_socket(socket, run_id, shared))
}

pub async fn handle_dap_debug_socket(socket: WebSocket, run_id: RunId, shared: Arc<SharedState>) {
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

pub async fn pump_axum_ws_to_dap(ws: WebSocket, target_port: u16) -> Result<(), anyhow::Error> {
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
                        .send(tokio_tungstenite::tungstenite::Message::Text(
                            bytes::Bytes::from(text).try_into()?,
                        ))
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
                        .send(WsMessage::Text(bytes::Bytes::from(text).try_into()?))
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
