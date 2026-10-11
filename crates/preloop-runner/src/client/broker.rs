//! Broker API client (GitHub-current path).
//!
//! Handles broker session management and message polling via the
//! `/runner/` and `/broker/` endpoints.

use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use rand::Rng;
use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::client::TlsStream;

use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header;
use tokio_tungstenite::tungstenite::protocol::frame::Frame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::{CloseCode, Data as OpData, OpCode};
use tokio_tungstenite::tungstenite::{Message, protocol::CloseFrame};
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream, client_async_tls_with_config};
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
/// `src/Sdk/DTWebApi/WebApi/BrokerWebSocketProbeResult.cs`). Upstream writes it
/// through `StringUtil.ConvertToJson`, whose formatter uses the camelCase
/// contract resolver — the wire names are `connected`, `connectCount`, ... .
/// `Errors`/`Connected` carry `EmitDefaultValue = true`, the rest `false`, so
/// they are omitted from the JSON while holding their default value.
#[derive(Debug, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
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

const WEBSOCKET_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

enum ProxyIo {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl AsyncRead for ProxyIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.as_mut().get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ProxyIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.as_mut().get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        match self.as_mut().get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        match self.as_mut().get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

type ProbeWebSocket = WebSocketStream<MaybeTlsStream<ProxyIo>>;

/// Connect to the broker probe URL over websocket and hold/reconnect until
/// `cancel` fires. Mirror of `BrokerServer.RunLongPollWebSocketProbeAsync`
/// (v2.338.0): connect failures back off 10–300 s, holds that end for any
/// other reason reconnect immediately, and the probe never fails the job.
///
/// This compatibility wrapper builds the same HTTP client configuration used
/// by the runner. Production callers should use
/// [`run_long_poll_websocket_probe_with_http`] so the configured CA bundle is
/// explicitly shared with the probe.
pub async fn run_long_poll_websocket_probe(
    probe_url: &str,
    bearer_token: &str,
    cancel: &mut tokio::sync::oneshot::Receiver<()>,
) -> BrokerWebSocketProbeResult {
    let http = match HttpClient::new(None) {
        Ok(http) => http,
        Err(error) => {
            return BrokerWebSocketProbeResult {
                last_close_reason: Some("error".to_owned()),
                errors: vec![error.to_string()],
                ..Default::default()
            };
        }
    };
    run_long_poll_websocket_probe_with_http(&http, probe_url, bearer_token, cancel).await
}

/// Run the broker websocket probe with the runner's configured TLS trust.
pub(crate) async fn run_long_poll_websocket_probe_with_http(
    http: &HttpClient,
    probe_url: &str,
    bearer_token: &str,
    cancel: &mut tokio::sync::oneshot::Receiver<()>,
) -> BrokerWebSocketProbeResult {
    let mut result = BrokerWebSocketProbeResult::default();
    let start = Instant::now();
    let tls_config = http.websocket_tls_config();

    loop {
        let mut socket =
            match connect_websocket(probe_url, bearer_token, tls_config.clone(), cancel).await {
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

        // Tungstenite assembles continuation frames before next() yields a
        // Message, unlike .NET ReceiveAsync's 4096-byte buffer. Count the
        // size-based receive chunks to stay close to the upstream telemetry.
        // Text echoes use raw tungstenite frames so each synthetic 4096-byte
        // chunk carries the proper final/continuation flag.
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
                        result.pings_received += text
                            .as_bytes()
                            .chunks(4096)
                            .count()
                            .max(1) as u64;
                        info!(%text, "Runner websocket received a ping");
                        match echo_text_chunks(&mut socket, &text, cancel).await {
                            EchoTextResult::Sent => {
                                info!(%text, "Runner replied via websocket");
                            }
                            EchoTextResult::Cancelled => {
                                result.last_close_reason = Some("job_completed".to_owned());
                                break;
                            }
                            EchoTextResult::Failed(error) => {
                                absorb_probe_error(&mut result, error);
                                break;
                            }
                        }
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        result.pings_received += bytes
                            .chunks(4096)
                            .count()
                            .max(1) as u64;
                    }
                    // Ping/pong control frames are handled by tungstenite,
                    // just as ClientWebSocket handles them below ReceiveAsync.
                    Some(Ok(_)) => {}
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

        let job_completed = result.last_close_reason.as_deref() == Some("job_completed");
        if job_completed {
            // A close handshake can flush a blocked write. Cancellation must
            // tear down the stream immediately so finalization cannot hang
            // behind a peer that stopped reading.
            drop(socket);
            break;
        }

        // Bound the close handshake for all other exits. The upstream uses
        // CancellationToken.None here, but a dead peer must not stall probes.
        let _ = timeout(WEBSOCKET_CLOSE_TIMEOUT, socket.close(None)).await;
    }

    result.total_duration_ms = start.elapsed().as_millis() as u64;
    result
}
enum EchoTextResult {
    Sent,
    Cancelled,
    Failed(tokio_tungstenite::tungstenite::Error),
}

/// Echo a text message as 4096-byte data frames, preserving message
/// fragmentation with continuation frames. Tungstenite still assembles
/// incoming continuation frames before yielding `Message::Text`, so these
/// synthetic frame boundaries are the closest faithful echo available.
async fn echo_text_chunks(
    socket: &mut ProbeWebSocket,
    text: &str,
    cancel: &mut tokio::sync::oneshot::Receiver<()>,
) -> EchoTextResult {
    let bytes = text.as_bytes();
    let mut offset = 0;
    let mut first = true;
    loop {
        let end = (offset + 4096).min(bytes.len());
        let opcode = if first {
            OpCode::Data(OpData::Text)
        } else {
            OpCode::Data(OpData::Continue)
        };
        let frame = Frame::message(bytes[offset..end].to_vec(), opcode, end == bytes.len());
        match tokio::select! {
            sent = socket.send(Message::Frame(frame)) => sent,
            _ = &mut *cancel => return EchoTextResult::Cancelled,
        } {
            Ok(()) => {}
            Err(error) => return EchoTextResult::Failed(error),
        }
        if end == bytes.len() {
            return EchoTextResult::Sent;
        }
        offset = end;
        first = false;
    }
}

/// Outcome of one probe connect attempt. `Cancelled` exists so the caller
/// never polls the completed oneshot receiver again (doing so panics).
enum WebSocketConnect {
    Connected(Box<ProbeWebSocket>),
    Failed,
    Cancelled,
}

/// Mirror of `BrokerServer.ConnectWebSocketAsync`: any failure is `Failed`
/// for the caller to back off on. Cancellation is reported separately only
/// because the oneshot receiver cannot be polled once completed.
async fn connect_websocket(
    probe_url: &str,
    bearer_token: &str,
    tls_config: Arc<rustls::ClientConfig>,
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
        outcome = ws_handshake(request, tls_config) => match outcome {
            Ok(socket) => {
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

/// Dial the websocket, honoring the runner's proxy environment.
///
/// `RunnerWebProxy.GetProxy` (v2.338.0): `wss://` selects the HTTPS proxy,
/// `ws://` the HTTP proxy; loopback and `no_proxy` entries bypass. tokio-
/// tungstenite has no proxy support, so the proxied path tunnels via HTTP
/// CONNECT before the websocket handshake.
async fn ws_handshake(
    request: tokio_tungstenite::tungstenite::handshake::client::Request,
    tls_config: Arc<rustls::ClientConfig>,
) -> std::result::Result<ProbeWebSocket, tokio_tungstenite::tungstenite::Error> {
    let host = request
        .uri()
        .host()
        .ok_or(tokio_tungstenite::tungstenite::Error::Url(
            tokio_tungstenite::tungstenite::error::UrlError::NoHostName,
        ))?
        .to_owned();
    let scheme = request.uri().scheme_str().unwrap_or_default();
    let port = request.uri().port_u16().unwrap_or(match scheme {
        "wss" => 443,
        _ => 80,
    });

    let mut socket: ProxyIo;
    if let Some(proxy) = ws_proxy_for(request.uri()) {
        let tcp = TcpStream::connect((proxy.host.as_str(), proxy.port)).await?;
        socket = if proxy.tls {
            let server_name = rustls::pki_types::ServerName::try_from(proxy.host.as_str())
                .map_err(|_| {
                    tokio_tungstenite::tungstenite::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "invalid HTTPS proxy hostname",
                    ))
                })?
                .to_owned();
            let connector = tokio_rustls::TlsConnector::from(tls_config.clone());
            let tls = connector.connect(server_name, tcp).await.map_err(|error| {
                tokio_tungstenite::tungstenite::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("HTTPS proxy TLS: {error}"),
                ))
            })?;
            ProxyIo::Tls(tls)
        } else {
            ProxyIo::Tcp(tcp)
        };

        // CONNECT host:port, then handshake the websocket over the tunnel.
        // For wss, client_async_tls_with_config upgrades the tunnel stream
        // to TLS. An https proxy's own TLS is already established above.
        let authority = websocket_authority(&host, port);
        let mut connect_request = format!(
            "CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\nUser-Agent: preloop-runner/{}\r\n",
            crate::PROTOCOL_COMPAT_VERSION
        );
        if let Some((user, pass)) = &proxy.credentials {
            use base64::Engine;
            let encoded =
                base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
            connect_request.push_str(&format!("Proxy-Authorization: Basic {encoded}\r\n"));
        }
        connect_request.push_str("Proxy-Connection: Keep-Alive\r\n\r\n");

        socket.write_all(connect_request.as_bytes()).await?;
        let mut response = vec![0u8; 8192];
        let mut total = 0;
        loop {
            if total == response.len() {
                return Err(tokio_tungstenite::tungstenite::Error::Io(
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "proxy response too large",
                    ),
                ));
            }
            let n = socket.read(&mut response[total..]).await?;
            if n == 0 {
                return Err(tokio_tungstenite::tungstenite::Error::Http(
                    tokio_tungstenite::tungstenite::http::Response::new(None),
                ));
            }
            total += n;
            if response[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let status = std::str::from_utf8(&response[..total])
            .ok()
            .and_then(|text| text.split_whitespace().nth(1))
            .and_then(|code| code.parse::<u16>().ok())
            .unwrap_or(0);
        if status != 200 {
            return Err(tokio_tungstenite::tungstenite::Error::Io(
                std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("proxy CONNECT {authority} returned HTTP {status}"),
                ),
            ));
        }
    } else {
        socket = ProxyIo::Tcp(TcpStream::connect((host.as_str(), port)).await?);
    }

    client_async_tls_with_config(request, socket, None, Some(Connector::Rustls(tls_config)))
        .await
        .map(|(socket, _)| socket)
}

fn websocket_authority(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Proxy endpoint resolved from the environment for one websocket target.
struct WsProxy {
    host: String,
    port: u16,
    tls: bool,
    credentials: Option<(String, String)>,
}

/// `RunnerWebProxy.GetProxy`/`IsBypassed` for ws targets: `wss` -> HTTPS
/// proxy, `ws` -> HTTP proxy; loopback and no_proxy always bypass.
fn ws_proxy_for(uri: &tokio_tungstenite::tungstenite::http::Uri) -> Option<WsProxy> {
    let scheme = uri.scheme_str()?;
    let host = uri.host()?.to_owned();
    if is_loopback_host(&host) {
        return None;
    }
    let port = uri.port_u16().unwrap_or(match scheme {
        "wss" => 443,
        "ws" => 80,
        _ => return None,
    });
    if no_proxy_matches(&host, port) {
        return None;
    }
    let raw = match scheme {
        "wss" => preferred_proxy_env("https_proxy", "HTTPS_PROXY"),
        "ws" => preferred_proxy_env("http_proxy", "HTTP_PROXY"),
        _ => None,
    }?;
    parse_proxy_url(&raw)
}

fn preferred_proxy_env(lower: &str, upper: &str) -> Option<String> {
    std::env::var(lower)
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| std::env::var(upper).ok().filter(|value| !value.is_empty()))
}

fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host.eq_ignore_ascii_case("localhost.localdomain")
        || host
            .parse::<IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

/// `no_proxy` bypass matching from `RunnerWebProxy.IsUriInBypassList`.
///
/// A bare host matches that host and its subdomains. A leading dot matches
/// subdomains only, so `.example.com` deliberately does not match the apex.
/// An explicit port is compared with the destination's effective port.
fn no_proxy_matches(host: &str, port: u16) -> bool {
    let Some(raw) = preferred_proxy_env("no_proxy", "NO_PROXY") else {
        return false;
    };
    no_proxy_matches_value(host, port, &raw)
}

fn no_proxy_matches_value(host: &str, port: u16, raw: &str) -> bool {
    raw.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .any(|entry| {
            if entry == "*" {
                return true;
            }
            let (entry_host, entry_port) = split_no_proxy_entry(entry);
            if entry_host.parse::<Ipv4Addr>().is_ok() {
                // RunnerWebProxy intentionally ignores IPv4 no_proxy entries.
                return false;
            }
            if entry_port.is_some_and(|entry_port| entry_port != port) {
                return false;
            }
            if let Some(suffix) = entry_host.strip_prefix('.') {
                !suffix.is_empty()
                    && host.len() > entry_host.len()
                    && host
                        .to_ascii_lowercase()
                        .ends_with(&entry_host.to_ascii_lowercase())
            } else {
                host.eq_ignore_ascii_case(&entry_host)
                    || host
                        .to_ascii_lowercase()
                        .ends_with(&format!(".{}", entry_host.to_ascii_lowercase()))
            }
        })
}

fn split_no_proxy_entry(entry: &str) -> (String, Option<u16>) {
    if let Some(rest) = entry.strip_prefix('[')
        && let Some(close) = rest.find(']')
    {
        let host = rest[..close].to_owned();
        let port = rest[close + 1..]
            .strip_prefix(':')
            .and_then(|value| value.parse::<u16>().ok());
        return (host, port);
    }
    if entry.matches(':').count() == 1
        && let Some((host, port)) = entry.rsplit_once(':')
        && let Ok(port) = port.parse::<u16>()
    {
        return (host.to_owned(), Some(port));
    }
    (entry.to_owned(), None)
}

fn parse_proxy_url(raw: &str) -> Option<WsProxy> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    // RunnerWebProxy prepends http:// to a bare host:port. Url parsing then
    // handles trailing slashes, IPv6 authorities, and userinfo delimiters.
    let normalized = if raw.contains("://") {
        raw.to_owned()
    } else {
        format!("http://{raw}")
    };
    let url = reqwest::Url::parse(&normalized).ok()?;
    let tls = match url.scheme() {
        "http" => false,
        "https" => true,
        _ => return None,
    };
    let host = url.host_str()?.to_owned();
    if host.is_empty() {
        return None;
    }
    let port = url.port_or_known_default()?;
    let credentials = if !url.username().is_empty() || url.password().is_some() {
        let user = percent_encoding::percent_decode_str(url.username())
            .decode_utf8()
            .ok()?
            .into_owned();
        let pass = percent_encoding::percent_decode_str(url.password().unwrap_or(""))
            .decode_utf8()
            .ok()?
            .into_owned();
        Some((user, pass))
    } else {
        None
    };
    Some(WsProxy {
        host,
        port,
        tls,
        credentials,
    })
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

    #[test]
    fn parse_proxy_url_uses_url_parser_for_credentials_and_trailing_slash() {
        let proxy = parse_proxy_url("https://user:pass%40@example.test:8443/").expect("proxy URL");
        assert_eq!(proxy.host, "example.test");
        assert_eq!(proxy.port, 8443);
        assert!(proxy.tls);
        assert_eq!(
            proxy.credentials,
            Some(("user".to_owned(), "pass@".to_owned()))
        );
    }

    #[test]
    fn parse_proxy_url_accepts_bare_host_and_default_port() {
        let proxy = parse_proxy_url("proxy.example.test/").expect("proxy URL");
        assert_eq!(proxy.host, "proxy.example.test");
        assert_eq!(proxy.port, 80);
        assert!(!proxy.tls);
        assert_eq!(proxy.credentials, None);
    }

    #[test]
    fn no_proxy_matches_upstream_host_and_port_rules() {
        assert!(no_proxy_matches_value(
            "sub.example.com",
            443,
            ".example.com"
        ));
        assert!(!no_proxy_matches_value("example.com", 443, ".example.com"));
        assert!(no_proxy_matches_value("example.com", 443, "example.com"));
        assert!(no_proxy_matches_value(
            "example.com",
            444,
            "example.com:444"
        ));
        assert!(!no_proxy_matches_value(
            "example.com",
            443,
            "example.com:444"
        ));
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
