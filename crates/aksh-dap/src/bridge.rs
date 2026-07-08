//! WebSocket ↔ DAP-TCP bridge.
//!
//! 1:1 port of `src/Runner.Worker/Dap/WebSocketDapBridge.cs`.
//!
//! The bridge accepts inbound byte streams on a configurable port
//! and forwards them to a target (the inner DAP-TCP server) on a
//! second port. Because the upstream infrastructure (Microsoft
//! Dev Tunnels) can deliver bytes as either raw TCP DAP
//! (`Content-Length` framed) or as a WebSocket, the bridge must
//! detect the transport on the *first packet* of each connection
//! and route accordingly.
//!
//! Detection algorithm (mirrors `IncomingStreamPrefixKind`):
//! - `HttpWebSocketUpgrade` — starts with `GET ` (HTTP/1.1 upgrade
//!   request). Perform the WS handshake, then pump text frames ↔
//!   `ContentLength`-framed DAP messages.
//! - `PreUpgradedWebSocket` — starts with the WS client-to-server
//!   magic `0x81 0xFE ...` (FIN + opcode=1 text, mask=1, 126/127
//!   length). No HTTP upgrade; the connection is already inside a
//!   WebSocket. Pump frames directly.
//! - `Http2Preface` — starts with `PRI * HTTP/2.0`. Reject.
//! - `TlsClientHello` — starts with `0x16` (TLS handshake). Pass
//!   through if the target is configured to handle it, otherwise
//!   reject.
//! - `WebSocketReservedBits` — FIN=1, opcode=1, RSV1-3 nonzero.
//!   Reject with a 400-class error.
//! - `Unknown` — pass through to the target as raw DAP-TCP.
//!
//! Limits (mirroring upstream `Constants.cs`):
//! - per-message body cap: 10 MB
//! - per-header-line cap: 8 KB
//! - read buffer: 32 KB

use std::io;
use std::time::Duration;

use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::protocol::{Message, Role};
use tokio_tungstenite::WebSocketStream;
use tracing::{debug, warn};

/// Detected transport kind of the first packet of an inbound
/// connection. Mirrors `WebSocketDapBridge.cs::IncomingStreamPrefixKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncomingStreamPrefixKind {
    /// Did not match any known shape — pass through as raw DAP-TCP.
    Unknown,
    /// `GET /... HTTP/1.1` with `Upgrade: websocket` headers.
    HttpWebSocketUpgrade,
    /// Already inside a WebSocket (the `0x81 0xFE/0xFF ...` magic).
    PreUpgradedWebSocket,
    /// FIN=1 + opcode=1 + at least one RSV bit set. Reject.
    WebSocketReservedBits,
    /// `PRI * HTTP/2.0`. Reject.
    Http2Preface,
    /// `0x16` — TLS ClientHello. Reject.
    TlsClientHello,
}

impl IncomingStreamPrefixKind {
    /// Returns `true` for prefixes the bridge knows how to handle.
    pub fn is_acceptable(self) -> bool {
        matches!(
            self,
            IncomingStreamPrefixKind::HttpWebSocketUpgrade
                | IncomingStreamPrefixKind::PreUpgradedWebSocket
                | IncomingStreamPrefixKind::Unknown
        )
    }
}

/// Read at most `max` bytes from `r` into a fresh buffer, with the
/// given overall timeout. Returns `(bytes, eof)`.
async fn read_prefix<R: AsyncRead + Unpin>(
    r: &mut R,
    max: usize,
    total: Duration,
) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; max];
    let mut filled = 0usize;
    let read_fut = async {
        while filled < max {
            let n = match r.read(&mut buf[filled..]).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            filled += n;
            // Stop as soon as we have enough to identify the prefix.
            if filled >= 16 {
                break;
            }
        }
        Ok(filled)
    };
    let res = timeout(total, read_fut).await;
    let n = match res {
        Ok(Ok(n)) => n,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Ok(buf[..filled].to_vec()),
    };
    buf.truncate(n);
    Ok(buf)
}

/// Classify the first packet of an inbound connection.
pub fn classify_prefix(bytes: &[u8]) -> IncomingStreamPrefixKind {
    if bytes.is_empty() {
        return IncomingStreamPrefixKind::Unknown;
    }
    if bytes.starts_with(b"PRI * HTTP/2.0") {
        return IncomingStreamPrefixKind::Http2Preface;
    }
    if bytes[0] == 0x16 {
        return IncomingStreamPrefixKind::TlsClientHello;
    }
    if bytes.len() >= 2 {
        let b0 = bytes[0];
        let rsv = b0 & 0x70;
        let opcode = b0 & 0x0F;
        if rsv != 0 && opcode == 0x1 {
            return IncomingStreamPrefixKind::WebSocketReservedBits;
        }
        // Pre-upgraded WS frame: 0x81 = FIN + opcode=text, 0xFE/FF = MASK + len 126/127.
        if b0 == 0x81 && (bytes[1] & 0x80) != 0 {
            return IncomingStreamPrefixKind::PreUpgradedWebSocket;
        }
    }
    if bytes.starts_with(b"GET ") {
        return IncomingStreamPrefixKind::HttpWebSocketUpgrade;
    }
    IncomingStreamPrefixKind::Unknown
}

/// Errors from the bridge.
#[derive(Debug, Error)]
pub enum BridgeError {
    /// Could not bind the listen socket, accept, read, or connect.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Inbound stream is rejected (HTTP/2, TLS, reserved bits).
    #[error("rejected prefix: {0:?}")]
    RejectedPrefix(IncomingStreamPrefixKind),

    /// WS handshake failed.
    #[error("handshake: {0}")]
    Handshake(String),

    /// Underlying target connection failed (kept for symmetry;
    /// identical to `Io` today).
    #[error("target: {0}")]
    Target(io::Error),
}

/// The bridge itself.
pub struct WebSocketDapBridge {
    listen_port: u16,
    target_port: u16,
}

impl WebSocketDapBridge {
    pub fn new(listen_port: u16, target_port: u16) -> Self {
        Self {
            listen_port,
            target_port,
        }
    }

    /// Run the bridge until the listener is closed.
    pub async fn run(self) -> Result<(), BridgeError> {
        let listener = TcpListener::bind(("127.0.0.1", self.listen_port)).await?;
        let target_port = self.target_port;
        loop {
            let (stream, _peer) = listener.accept().await?;
            let tp = target_port;
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, tp).await {
                    warn!("bridge connection ended: {e}");
                }
            });
        }
    }

    /// Run one connection synchronously. Useful for tests.
    pub async fn run_one(&self, inbound: TcpStream) -> Result<(), BridgeError> {
        handle_connection(inbound, self.target_port).await
    }
}

async fn handle_connection(mut inbound: TcpStream, target_port: u16) -> Result<(), BridgeError> {
    let prefix = read_prefix(&mut inbound, 16, Duration::from_secs(5)).await?;
    let kind = classify_prefix(&prefix);
    debug!(?kind, "classified inbound prefix ({} bytes)", prefix.len());
    if !kind.is_acceptable() {
        return Err(BridgeError::RejectedPrefix(kind));
    }
    match kind {
        IncomingStreamPrefixKind::HttpWebSocketUpgrade => {
            upgrade_and_pump(inbound, target_port, &prefix).await
        }
        IncomingStreamPrefixKind::PreUpgradedWebSocket => {
            pre_upgraded_and_pump(inbound, target_port, &prefix).await
        }
        IncomingStreamPrefixKind::Unknown => raw_dap_pump(inbound, target_port).await,
        IncomingStreamPrefixKind::Http2Preface
        | IncomingStreamPrefixKind::TlsClientHello
        | IncomingStreamPrefixKind::WebSocketReservedBits => {
            Err(BridgeError::RejectedPrefix(kind))
        }
    }
}

async fn upgrade_and_pump(
    inbound: TcpStream,
    target_port: u16,
    prefix: &[u8],
) -> Result<(), BridgeError> {
    let stream = PrefixStream::new(inbound, prefix.to_vec());
    let ws = tokio_tungstenite::accept_async(stream)
        .await
        .map_err(|e| BridgeError::Handshake(format!("accept_async: {e}")))?;
    pump_ws_to_dap(ws, target_port).await
}

async fn pre_upgraded_and_pump(
    inbound: TcpStream,
    target_port: u16,
    prefix: &[u8],
) -> Result<(), BridgeError> {
    let stream = PrefixStream::new(inbound, prefix.to_vec());
    let ws: WebSocketStream<PrefixStream> =
        WebSocketStream::from_raw_socket(stream, Role::Server, None).await;
    pump_ws_to_dap(ws, target_port).await
}

async fn raw_dap_pump(mut inbound: TcpStream, target_port: u16) -> Result<(), BridgeError> {
    let mut target = TcpStream::connect(("127.0.0.1", target_port)).await?;
    let (mut inbound_read, mut inbound_write) = inbound.split();
    let (mut target_read, mut target_write) = target.split();

    // client -> server: read from inbound, write to target
    let c2s = tokio::io::copy(&mut inbound_read, &mut target_write);
    // server -> client: read from target, write to inbound
    let s2c = tokio::io::copy(&mut target_read, &mut inbound_write);
    tokio::select! {
        r = c2s => r.map(|_| ()).map_err(BridgeError::Io)?,
        r = s2c => r.map(|_| ()).map_err(BridgeError::Io)?,
    }
    Ok(())
}

async fn pump_ws_to_dap<S>(ws: WebSocketStream<S>, target_port: u16) -> Result<(), BridgeError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut ws_sink, mut ws_stream) = ws.split();
    let mut target = TcpStream::connect(("127.0.0.1", target_port)).await?;
    let (mut target_read, mut target_write) = target.split();

    let to_target = async {
        while let Some(msg) = ws_stream.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    let bytes = text.as_bytes();
                    let header = format!("Content-Length: {}\r\n\r\n", bytes.len());
                    target_write.write_all(header.as_bytes()).await?;
                    target_write.write_all(bytes).await?;
                    target_write.flush().await?;
                }
                Ok(Message::Binary(_)) => {
                    return Err(BridgeError::Handshake(
                        "binary WebSocket frames are not allowed on the DAP bridge".into(),
                    ));
                }
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
                Ok(Message::Frame(_)) => continue,
            }
        }
        Ok::<_, BridgeError>(())
    };

    let from_target = async {
        let mut buf = BytesMut::with_capacity(32 * 1024);
        loop {
            let mut tmp = [0u8; 32 * 1024];
            let n = match target_read.read(&mut tmp).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => return Err(BridgeError::Target(e)),
            };
            buf.extend_from_slice(&tmp[..n]);
            loop {
                match try_drain_message(&mut buf) {
                    Some(Ok(text)) => ws_sink
                        .send(Message::Text(text))
                        .await
                        .map_err(|e| BridgeError::Handshake(format!("ws send: {e}")))?,
                    Some(Err(e)) => return Err(e),
                    None => break,
                }
            }
        }
        Ok::<_, BridgeError>(())
    };

    tokio::select! {
        a = to_target => a?,
        b = from_target => b?,
    }
    Ok(())
}

/// Try to extract one complete `Content-Length` framed message.
fn try_drain_message(buf: &mut BytesMut) -> Option<Result<String, BridgeError>> {
    let terminator = match find_crlf_crlf(buf) {
        Some(pos) => pos,
        None => {
            if buf.len() > 8192 * 4 {
                return Some(Err(BridgeError::Handshake("header line too long".into())));
            }
            return None;
        }
    };
    let header_str = match std::str::from_utf8(&buf[..terminator]) {
        Ok(s) => s,
        Err(_) => return Some(Err(BridgeError::Handshake("non-utf8 header".into()))),
    };
    let content_length = match parse_content_length(header_str) {
        Ok(n) => n,
        Err(_) => {
            // Fallback: no Content-Length header. Treat the whole
            // buffered prefix as a single message.
            let drained: Vec<u8> = buf.split().to_vec();
            let text = String::from_utf8_lossy(&drained).into_owned();
            return Some(Ok(text));
        }
    };
    if content_length > 10 * 1024 * 1024 {
        return Some(Err(BridgeError::Handshake("message too large".into())));
    }
    let body_start = terminator + 4;
    let body_end = body_start + content_length;
    if buf.len() < body_end {
        return None;
    }
    let mut body = buf.split_to(body_end);
    let _ = body.split_to(body_start);
    let text = String::from_utf8_lossy(&body).into_owned();
    Some(Ok(text))
}

fn find_crlf_crlf(buf: &BytesMut) -> Option<usize> {
    if buf.len() < 4 {
        return None;
    }
    for i in 0..=(buf.len() - 4) {
        if &buf[i..i + 4] == b"\r\n\r\n" {
            return Some(i);
        }
    }
    None
}

fn parse_content_length(headers: &str) -> Result<usize, BridgeError> {
    for line in headers.split("\r\n") {
        if line.is_empty() {
            continue;
        }
        let lower = line.to_ascii_lowercase();
        if let Some(rest) = lower.strip_prefix("content-length:") {
            return rest
                .trim()
                .parse::<usize>()
                .map_err(|_| BridgeError::Handshake("invalid Content-Length".into()));
        }
    }
    Err(BridgeError::Handshake("missing Content-Length".into()))
}

/// A read/write stream that re-injects bytes we have already
/// consumed from the front of the underlying transport.
pub struct PrefixStream {
    inner: TcpStream,
    prefix: BytesMut,
}

impl PrefixStream {
    pub fn new(inner: TcpStream, prefix: Vec<u8>) -> Self {
        Self {
            inner,
            prefix: BytesMut::from(&prefix[..]),
        }
    }
}

impl AsyncRead for PrefixStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        if !self.prefix.is_empty() {
            let n = std::cmp::min(buf.remaining(), self.prefix.len());
            let head = self.prefix.split_to(n);
            buf.put_slice(&head);
            return std::task::Poll::Ready(Ok(()));
        }
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for PrefixStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[test]
    fn classify_http_get_upgrade() {
        let bytes = b"GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n\r\n";
        assert_eq!(
            classify_prefix(bytes),
            IncomingStreamPrefixKind::HttpWebSocketUpgrade
        );
    }

    #[test]
    fn classify_pre_upgraded_websocket() {
        let bytes = [0x81, 0xFE, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(
            classify_prefix(&bytes),
            IncomingStreamPrefixKind::PreUpgradedWebSocket
        );
    }

    #[test]
    fn classify_h2_preface() {
        let bytes = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
        assert_eq!(
            classify_prefix(bytes),
            IncomingStreamPrefixKind::Http2Preface
        );
    }

    #[test]
    fn classify_tls_client_hello() {
        let bytes = [0x16, 0x03, 0x01, 0x00, 0x40];
        assert_eq!(
            classify_prefix(&bytes),
            IncomingStreamPrefixKind::TlsClientHello
        );
    }

    #[test]
    fn classify_reserved_bits_rejected() {
        let bytes = [0xF1, 0x80];
        assert_eq!(
            classify_prefix(&bytes),
            IncomingStreamPrefixKind::WebSocketReservedBits
        );
    }

    #[test]
    fn classify_unknown() {
        let bytes = b"Content-Length: 7\r\n\r\n";
        assert_eq!(classify_prefix(bytes), IncomingStreamPrefixKind::Unknown);
    }

    #[test]
    fn acceptable_prefixes_pass() {
        for kind in [
            IncomingStreamPrefixKind::HttpWebSocketUpgrade,
            IncomingStreamPrefixKind::PreUpgradedWebSocket,
            IncomingStreamPrefixKind::Unknown,
        ] {
            assert!(kind.is_acceptable(), "{kind:?} should be acceptable");
        }
        for kind in [
            IncomingStreamPrefixKind::Http2Preface,
            IncomingStreamPrefixKind::TlsClientHello,
            IncomingStreamPrefixKind::WebSocketReservedBits,
        ] {
            assert!(!kind.is_acceptable(), "{kind:?} should be rejected");
        }
    }

    #[tokio::test]
    async fn duplex_round_trip_passes_bytes() {
        let (mut a, mut b) = duplex(64);
        a.write_all(b"hello").await.unwrap();
        let mut buf = [0u8; 5];
        b.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn try_drain_message_extracts_complete_message() {
        let mut buf = BytesMut::from(&b"Content-Length: 5\r\n\r\nhello"[..]);
        let r = try_drain_message(&mut buf).unwrap().unwrap();
        assert_eq!(r, "hello");
        assert!(buf.is_empty());
    }

    #[test]
    fn try_drain_message_waits_for_more() {
        let mut buf = BytesMut::from(&b"Content-Length: 5\r\n\r\nhel"[..]);
        assert!(try_drain_message(&mut buf).is_none());
    }

    #[test]
    fn try_drain_message_rejects_oversize() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(
            format!("Content-Length: {}\r\n\r\nx", 11 * 1024 * 1024).as_bytes(),
        );
        let r = try_drain_message(&mut buf);
        assert!(matches!(r, Some(Err(_))));
    }
}
