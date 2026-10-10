//! Broker API client (GitHub-current path).
//!
//! Handles broker session management and message polling via the
//! `/runner/` and `/broker/` endpoints.

use anyhow::{Context, Result};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

use futures::{SinkExt, StreamExt};
use rand::Rng;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Message, protocol::CloseFrame};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{info, warn};

use super::http::{HttpClient, HttpError};

// The broker holds a message poll open for up to 50 seconds. The client
// deadline must outlast that window, or a job the server hands out at the
// end of the window is written to a request the client already abandoned.
// Official runner: `VssUtil.GetHttpRequestSettings` defaults `SendTimeout`
// to 100 seconds for the broker connection.
const MESSAGE_POLL_TIMEOUT: Duration = Duration::from_secs(100);

/// Client for the broker endpoints (GitHub-current path).
#[derive(Clone)]
pub struct BrokerClient {
    http: HttpClient,
    base_url: String,
}

/// Result from acknowledging a broker message.
#[derive(Debug, PartialEq, Eq)]
pub enum AcknowledgeResult {
    /// Acknowledge succeeded normally.
    Ok,
    /// Broker returned 404 with `AcknowledgeJobNotFound` — the job no longer exists.
    JobNotFound,
}

impl BrokerClient {
    /// Create a new broker client.
    pub fn new(http: HttpClient, base_url: String) -> Self {
        Self { http, base_url }
    }

    /// Create a broker session.
    pub async fn create_session(
        &self,
        token: &str,
        session: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!("{}/session", self.base_url);
        self.http
            .post_json_bearer(&url, session, token)
            .await
            .context("creating broker session")
    }

    /// Delete a broker session.
    pub async fn delete_session(&self, token: &str, session_id: &str) -> Result<()> {
        let url = format!("{}/session", self.base_url);
        self.http
            .delete_with_token_header(&url, token, "X-Actions-Session", session_id)
            .await
            .context("deleting broker session")
    }

    /// Long-poll for a message from the broker.
    ///
    /// Query params match golden exactly:
    /// `sessionId, status, runnerVersion, os, architecture, disableUpdate=false`
    /// No `lastMessageId` — the golden flows never include it.
    pub async fn get_message(
        &self,
        token: &str,
        session_id: &str,
        busy: bool,
    ) -> Result<Option<serde_json::Value>> {
        let status = if busy { "Busy" } else { "Online" };
        let url = format!(
            "{}/message?sessionId={session_id}&status={status}&runnerVersion={}&os={}&architecture={}&disableUpdate=false",
            self.base_url,
            crate::PROTOCOL_COMPAT_VERSION,
            os_label(),
            arch_label(),
        );
        let message: Option<serde_json::Value> = self
            .http
            .get_long_poll(&url, &format!("Bearer {token}"), MESSAGE_POLL_TIMEOUT)
            .await
            .context("polling broker message")?;
        // A 200 with a `null` body is an empty poll; the official listener
        // deserializes it to no message and polls again.
        Ok(message.filter(|message| !message.is_null()))
    }

    /// Acknowledge a message (POST, matching official runner).
    ///
    /// Golden flow 13: POST /acknowledge?sessionId=X&status=Online&runnerVersion=...&os=...&architecture=...
    /// Body: `{"runnerRequestId": "<job_message_id>"}`
    /// No `disableUpdate` or `messageId` in the query string.
    pub async fn acknowledge(
        &self,
        token: &str,
        session_id: &str,
        runner_request_id: &str,
    ) -> Result<AcknowledgeResult> {
        let url = format!(
            "{}/acknowledge?sessionId={session_id}&status=Online&runnerVersion={}&os={}&architecture={}",
            self.base_url,
            crate::PROTOCOL_COMPAT_VERSION,
            os_label(),
            arch_label(),
        );
        let body = serde_json::json!({"runnerRequestId": runner_request_id});
        match self
            .http
            .post_json_bearer::<serde_json::Value>(&url, &body, token)
            .await
        {
            Ok(_) => Ok(AcknowledgeResult::Ok),
            Err(e) => {
                // v2.336.0 (#4540): 404 with AcknowledgeJobNotFound means the job
                // no longer exists. Ephemeral runners should exit cleanly.
                if let Some(HttpError::Status { status, body }) = e.downcast_ref::<HttpError>()
                    && *status == reqwest::StatusCode::NOT_FOUND
                    && let Ok(json) = serde_json::from_str::<serde_json::Value>(body)
                    && json.get("errorKind").and_then(|v| v.as_str())
                        == Some("AcknowledgeJobNotFound")
                {
                    return Ok(AcknowledgeResult::JobNotFound);
                }
                Err(e).context("acknowledging broker message")
            }
        }
    }
}

/// Telemetry payload for the temporary broker long-poll websocket probe.
///
/// Mirrors `BrokerWebSocketProbeResult` (v2.338.0
/// `src/Sdk/DTWebApi/WebApi/BrokerWebSocketProbeResult.cs`): `Connected` and
/// `Errors` carry `EmitDefaultValue = true`, the rest `false`, so they are
/// omitted from the JSON while holding their default value.
#[derive(Debug, Default, serde::Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct BrokerWebSocketProbeResult {
    /// Whether at least one websocket connection was established.
    pub connected: bool,
    #[serde(skip_serializing_if = "is_zero")]
    pub connect_count: u64,
    #[serde(skip_serializing_if = "is_zero")]
    pub connect_failures: u64,
    /// .NET `ReceiveAsync` folds every non-close frame into one result; this
    /// counts the same (tungstenite answers ping/pong frames itself).
    #[serde(skip_serializing_if = "is_zero")]
    pub pings_received: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_close_reason: Option<String>,
    #[serde(skip_serializing_if = "is_zero")]
    pub total_duration_ms: u64,
    pub errors: Vec<String>,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// Official `BackoffTimerHelper.GetRandomBackoff` bounds for the probe's
/// websocket reconnect delay.
const MIN_DELAY_FOR_WEBSOCKET_RECONNECT: Duration = Duration::from_secs(10);
const MAX_DELAY_FOR_WEBSOCKET_RECONNECT: Duration = Duration::from_secs(300);

/// Connect to the broker probe URL over websocket and hold/reconnect until
/// `cancel` fires. Mirror of `BrokerServer.RunLongPollWebSocketProbeAsync`
/// (v2.338.0): connect failures back off 10–300 s, holds that end for any
/// other reason reconnect immediately, and the probe never fails the job.
pub async fn run_long_poll_websocket_probe(
    probe_url: &str,
    bearer_token: &str,
    cancel: &mut tokio::sync::oneshot::Receiver<()>,
) -> BrokerWebSocketProbeResult {
    let mut result = BrokerWebSocketProbeResult::default();
    let start = Instant::now();

    loop {
        let mut socket = match connect_websocket(probe_url, bearer_token, cancel).await {
            WebSocketConnect::Connected(socket) => *socket,
            outcome => {
                // Upstream counts any connect exception — cancellation
                // included — as a connect failure; its `Task.Delay` then
                // throws immediately when cancelled.
                result.connect_failures += 1;
                result.last_close_reason = Some("connect_failed".to_owned());
                if matches!(outcome, WebSocketConnect::Cancelled) {
                    break;
                }
                let delay = rand::thread_rng().gen_range(
                    MIN_DELAY_FOR_WEBSOCKET_RECONNECT..MAX_DELAY_FOR_WEBSOCKET_RECONNECT,
                );
                tokio::select! {
                    _ = tokio::time::sleep(delay) => continue,
                    _ = &mut *cancel => break,
                }
            }
        };

        result.connected = true;
        result.connect_count += 1;

        // Hold the socket: count every message like upstream's
        // `PingsReceived`, echo text frames back, and take the close frame as
        // the server's close reason.
        loop {
            tokio::select! {
                _ = &mut *cancel => {
                    result.last_close_reason = Some("job_completed".to_owned());
                    break;
                }
                message = socket.next() => match message {
                    Some(Ok(Message::Close(frame))) => {
                        let (code, reason) = close_status_strings(&frame);
                        result.last_close_reason =
                            Some(format!("server_closed:{code}:{reason}"));
                        info!(code = %code, %reason, "Runner websocket closed by server");
                        break;
                    }
                    Some(Ok(Message::Text(text))) => {
                        result.pings_received += 1;
                        info!(%text, "Runner websocket received a ping");
                        if let Err(error) = socket.send(Message::Text(text.clone())).await {
                            absorb_probe_error(&mut result, error);
                            break;
                        }
                        info!(%text, "Runner replied via websocket");
                    }
                    Some(Ok(_)) => result.pings_received += 1,
                    Some(Err(error)) => {
                        absorb_probe_error(&mut result, error);
                        break;
                    }
                    // Stream ended without a close handshake: upstream takes
                    // the exception path; either way we reconnect.
                    None => {
                        result.last_close_reason = Some("error".to_owned());
                        break;
                    }
                },
            }
        }

        // Upstream sends the close handshake whenever the socket is still
        // open; a half-closed socket here is already our answer to the
        // server's close frame.
        let _ = socket.close(None).await;
        if result.last_close_reason.as_deref() == Some("job_completed") {
            break;
        }
    }

    result.total_duration_ms = start.elapsed().as_millis() as u64;
    result
}

/// Outcome of one probe connect attempt. `Cancelled` exists so the caller
/// never polls the completed oneshot receiver again (doing so panics).
enum WebSocketConnect {
    Connected(Box<WebSocketStream<MaybeTlsStream<TcpStream>>>),
    Failed,
    Cancelled,
}

/// Mirror of `BrokerServer.ConnectWebSocketAsync`: any failure is `Failed`
/// for the caller to back off on. Cancellation is reported separately only
/// because the oneshot receiver cannot be polled once completed.
async fn connect_websocket(
    probe_url: &str,
    bearer_token: &str,
    cancel: &mut tokio::sync::oneshot::Receiver<()>,
) -> WebSocketConnect {
    let mut request = match probe_url.into_client_request() {
        Ok(request) => request,
        Err(error) => {
            info!(%error, %probe_url, "Invalid runner websocket probe URL");
            return WebSocketConnect::Failed;
        }
    };
    match format!("Bearer {bearer_token}").parse() {
        Ok(value) => {
            request.headers_mut().insert(header::AUTHORIZATION, value);
        }
        Err(error) => {
            warn!(%error, "Invalid runner websocket probe authorization header");
            return WebSocketConnect::Failed;
        }
    }

    info!("Attempting to start runner websocket client.");
    tokio::select! {
        outcome = connect_async(request) => match outcome {
            Ok((socket, _)) => {
                info!("Successfully started runner websocket client.");
                WebSocketConnect::Connected(Box::new(socket))
            }
            Err(error) => {
                info!(%error, "Runner websocket connect failed, will retry.");
                WebSocketConnect::Failed
            }
        },
        _ = &mut *cancel => WebSocketConnect::Cancelled,
    }
}

/// Upstream's `catch (Exception)` inside the hold loop: unique error strings
/// are collected and the socket is reported closed with reason `error`.
fn absorb_probe_error(
    result: &mut BrokerWebSocketProbeResult,
    error: tokio_tungstenite::tungstenite::Error,
) {
    info!(%error, "Exception caught while holding runner websocket, will reconnect.");
    let message = error.to_string();
    if !result.errors.contains(&message) {
        result.errors.push(message);
    }
    result.last_close_reason = Some("error".to_owned());
}

/// Render a close frame as upstream's
/// `$"server_closed:{CloseStatus}:{CloseStatusDescription}"`: the .NET
/// `WebSocketCloseStatus` member name (numeric for undefined codes) and the
/// frame's reason string.
fn close_status_strings(frame: &Option<CloseFrame>) -> (String, String) {
    let Some(frame) = frame else {
        return ("0".to_owned(), String::new());
    };
    let code = match frame.code {
        CloseCode::Normal => "NormalClosure".to_owned(),
        CloseCode::Away => "EndpointUnavailable".to_owned(),
        CloseCode::Protocol => "ProtocolError".to_owned(),
        CloseCode::Unsupported => "InvalidMessageType".to_owned(),
        CloseCode::Invalid => "InvalidPayloadData".to_owned(),
        CloseCode::Policy => "PolicyViolation".to_owned(),
        CloseCode::Size => "MessageTooBig".to_owned(),
        CloseCode::Extension => "MandatoryExtension".to_owned(),
        CloseCode::Error => "InternalServerError".to_owned(),
        // Codes with no `WebSocketCloseStatus` member (1005, 1006, 1012,
        // 1013, ...) stringify as their number in .NET too.
        other => u16::from(other).to_string(),
    };
    (code, frame.reason.to_string())
}

/// Detect the official runner-version deprecation response.
///
/// Runner.Listener receives this as `AccessDeniedException` with
/// `errorCode: 1` from the message endpoint. Keep the check narrow so normal
/// authorization failures continue through the retry/reconnect path.
pub fn is_runner_version_deprecated(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let Some(HttpError::Status { status, body }) = cause.downcast_ref::<HttpError>() else {
            return false;
        };
        *status == reqwest::StatusCode::FORBIDDEN
            && serde_json::from_str::<serde_json::Value>(body)
                .ok()
                .and_then(|value| value.get("errorCode").and_then(serde_json::Value::as_i64))
                == Some(1)
    })
}

fn os_label() -> &'static str {
    if cfg!(target_os = "macos") {
        "macOS"
    } else if cfg!(target_os = "windows") {
        "Windows"
    } else {
        "Linux"
    }
}

fn arch_label() -> &'static str {
    if cfg!(target_arch = "aarch64") {
        "ARM64"
    } else {
        "X64"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_access_denied_error_code_one() {
        let error = anyhow::Error::new(HttpError::Status {
            status: reqwest::StatusCode::FORBIDDEN,
            body: r#"{"typeKey":"AccessDeniedException","errorCode":1}"#.to_owned(),
        })
        .context("polling broker message");
        assert!(is_runner_version_deprecated(&error));
    }

    #[test]
    fn does_not_classify_other_forbidden_responses() {
        let error = anyhow::Error::new(HttpError::Status {
            status: reqwest::StatusCode::FORBIDDEN,
            body: r#"{"typeKey":"AccessDeniedException","errorCode":0}"#.to_owned(),
        });
        assert!(!is_runner_version_deprecated(&error));
    }

    #[tokio::test]
    async fn null_poll_body_is_no_message() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let _ = socket.read(&mut request).await.unwrap();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 4\r\nconnection: close\r\n\r\nnull")
                .await
                .unwrap();
        });
        let client = BrokerClient::new(HttpClient::new(None).unwrap(), format!("http://{address}"));

        let message = client.get_message("token", "session", true).await.unwrap();

        assert_eq!(message, None);
    }
}
