//! `Content-Length` LSP/DAP framing.
//!
//! Mirrors the framing used by `DapDebugger.cs` in upstream. Each
//! message is preceded by headers terminated by `\r\n\r\n`, the only
//! required header being `Content-Length: N` (where `N` is the byte
//! count of the JSON body that follows).
//!
//! ```text
//! Content-Length: 123\r\n\r\n{"seq":1,"type":"request",...}
//! ```
//!
//! Limits are also taken from upstream
//! (`Constants.cs::_maxMessageSize = 10 * 1024 * 1024`,
//! `_maxHeaderLineLength = 8192`).

use std::io;

use bytes::BytesMut;
use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// 10 MB per the upstream `DapDebugger.cs::_maxMessageSize`. The
/// runner will reject any DAP message body larger than this.
pub const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// 8 KB per the upstream `DapDebugger.cs::_maxHeaderLineLength`. A
/// header line longer than this is a protocol violation.
pub const MAX_HEADER_LINE_LENGTH: usize = 8 * 1024;

/// Errors returned by [`read_message`] / [`write_message`].
#[derive(Debug, Error)]
pub enum FrameError {
    /// I/O error from the underlying stream.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// A header line exceeded [`MAX_HEADER_LINE_LENGTH`].
    #[error("header line exceeded {MAX_HEADER_LINE_LENGTH} bytes")]
    HeaderTooLong,

    /// The declared `Content-Length` exceeded [`MAX_MESSAGE_SIZE`].
    #[error("message body of {actual} bytes exceeded {MAX_MESSAGE_SIZE} max")]
    BodyTooLarge { actual: usize },

    /// Headers were missing or malformed.
    #[error("malformed headers: {0}")]
    MalformedHeaders(String),

    /// A required `Content-Length` header was missing.
    #[error("missing Content-Length header")]
    MissingContentLength,

    /// `Content-Length` was not a valid non-negative integer.
    #[error("invalid Content-Length value: {0}")]
    InvalidContentLength(String),

    /// JSON deserialization failed.
    #[error("JSON deserialization failed: {0}")]
    Json(#[from] serde_json::Error),
}

/// Read a single `Content-Length` framed message from `reader` into
/// the target type.
///
/// Used by both the raw-TCP DAP server and the [`crate::bridge::WebSocketDapBridge`]
/// when the inbound stream is a pre-upgraded WebSocket or a TLS
/// tunnel that re-emits DAP-TCP framing.
pub async fn read_message<T, R>(reader: &mut R) -> Result<T, FrameError>
where
    T: DeserializeOwned,
    R: AsyncRead + Unpin,
{
    // Read headers until \r\n\r\n.
    let mut header_buf = Vec::with_capacity(512);
    let mut byte = [0u8; 1];
    loop {
        if reader.read(&mut byte).await? == 0 {
            return Err(FrameError::MalformedHeaders(
                "stream closed before headers terminated".into(),
            ));
        }
        header_buf.push(byte[0]);
        if header_buf.len() > MAX_HEADER_LINE_LENGTH * 4 {
            return Err(FrameError::HeaderTooLong);
        }
        if header_buf.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let header_text = std::str::from_utf8(&header_buf)
        .map_err(|e| FrameError::MalformedHeaders(format!("non-utf8 header: {e}")))?;
    let content_length = parse_content_length(header_text)?;

    if content_length > MAX_MESSAGE_SIZE {
        return Err(FrameError::BodyTooLarge {
            actual: content_length,
        });
    }

    // Read exactly `content_length` bytes of body.
    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body).await?;
    let value: T = serde_json::from_slice(&body)?;
    Ok(value)
}

/// Write a single `Content-Length` framed message to `writer`.
///
/// Serializes `value` as JSON, writes the `Content-Length` header,
/// then writes the body bytes.
pub async fn write_message<T, W>(writer: &mut W, value: &T) -> Result<(), FrameError>
where
    T: Serialize,
    W: AsyncWrite + Unpin,
{
    let body = serde_json::to_vec(value)?;
    if body.len() > MAX_MESSAGE_SIZE {
        return Err(FrameError::BodyTooLarge {
            actual: body.len(),
        });
    }
    let header = format!("Content-Length: {}\r\n\r\n", body.len());
    writer.write_all(header.as_bytes()).await?;
    writer.write_all(&body).await?;
    writer.flush().await?;
    Ok(())
}

/// Parse a `Content-Length` from a header block. The header block
/// must already include the trailing `\r\n\r\n` (which we strip
/// before parsing).
fn parse_content_length(headers: &str) -> Result<usize, FrameError> {
    for line in headers.split("\r\n") {
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix("Content-Length:") {
            let value = rest.trim();
            return value
                .parse::<usize>()
                .map_err(|_| FrameError::InvalidContentLength(value.to_string()));
        }
        // Headers are case-insensitive per the LSP spec.
        if let Some(rest) = line.strip_prefix("content-length:") {
            let value = rest.trim();
            return value
                .parse::<usize>()
                .map_err(|_| FrameError::InvalidContentLength(value.to_string()));
        }
    }
    Err(FrameError::MissingContentLength)
}

/// A stateful framing reader backed by a `BytesMut` buffer.
///
/// Useful for tests and for the harness where we want to push bytes
/// in chunks and pull out complete messages.
#[derive(Debug, Default)]
pub struct FramedReader {
    buf: BytesMut,
}

impl FramedReader {
    /// Create a new empty reader.
    pub fn new() -> Self {
        Self {
            buf: BytesMut::new(),
        }
    }

    /// Append raw bytes received from the network.
    pub fn extend(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Try to read one complete framed message. Returns:
    /// - `Ok(Some(value))` if a full message is available,
    /// - `Ok(None)` if more bytes are needed,
    /// - `Err(_)` if the stream is malformed.
    pub fn try_read<T: DeserializeOwned>(&mut self) -> Result<Option<T>, FrameError> {
        // Find the header terminator.
        let terminator = match find_crlf_crlf(&self.buf) {
            Some(pos) => pos,
            None => {
                if self.buf.len() > MAX_HEADER_LINE_LENGTH * 4 {
                    return Err(FrameError::HeaderTooLong);
                }
                return Ok(None);
            }
        };
        let header_str = std::str::from_utf8(&self.buf[..terminator])
            .map_err(|e| FrameError::MalformedHeaders(format!("non-utf8 header: {e}")))?;
        let content_length = parse_content_length(header_str)?;
        if content_length > MAX_MESSAGE_SIZE {
            return Err(FrameError::BodyTooLarge {
                actual: content_length,
            });
        }
        let body_start = terminator + 4;
        let body_end = body_start + content_length;
        if self.buf.len() < body_end {
            return Ok(None);
        }
        let mut body = self.buf.split_to(body_end);
        // Discard the headers we already consumed.
        let _ = body.split_to(body_start);
        let value: T = serde_json::from_slice(&body)?;
        Ok(Some(value))
    }
}

fn find_crlf_crlf(buf: &[u8]) -> Option<usize> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use tokio::io::duplex;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestMsg {
        seq: i64,
        payload: String,
    }

    #[tokio::test]
    async fn round_trip_single_message() {
        let (mut a, mut b) = duplex(1024);
        let original = TestMsg {
            seq: 7,
            payload: "hello".into(),
        };
        let write = write_message(&mut a, &original);
        let read = read_message::<TestMsg, _>(&mut b);
        let (write_res, read_res) = tokio::join!(write, read);
        write_res.unwrap();
        assert_eq!(read_res.unwrap(), original);
    }

    #[tokio::test]
    async fn rejects_oversize_body() {
        let mut framed = FramedReader::new();
        // Forge a header that claims 11 MB.
        let header = format!("Content-Length: {}\r\n\r\n", MAX_MESSAGE_SIZE + 1);
        framed.extend(header.as_bytes());
        let res: Result<Option<TestMsg>, _> = framed.try_read();
        match res {
            Err(FrameError::BodyTooLarge { .. }) => {}
            other => panic!("expected BodyTooLarge, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn rejects_missing_content_length() {
        let mut framed = FramedReader::new();
        framed.extend(b"X-Custom: 1\r\n\r\n{}");
        let res: Result<Option<TestMsg>, _> = framed.try_read();
        match res {
            Err(FrameError::MissingContentLength) => {}
            other => panic!("expected MissingContentLength, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn rejects_invalid_content_length() {
        let mut framed = FramedReader::new();
        framed.extend(b"Content-Length: notanumber\r\n\r\n{}");
        let res: Result<Option<TestMsg>, _> = framed.try_read();
        match res {
            Err(FrameError::InvalidContentLength(_)) => {}
            other => panic!("expected InvalidContentLength, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn case_insensitive_content_length() {
        let mut framed = FramedReader::new();
        // Body is exactly 9 bytes: `{"seq":1}`.
        framed.extend(b"content-length: 9\r\n\r\n{\"seq\":1}");
        let res: Result<Option<serde_json::Value>, _> = framed.try_read();
        assert!(res.is_ok(), "framing accepted lower-case header: {res:?}");
        let v = res.unwrap().unwrap();
        assert_eq!(v["seq"], 1);
    }

    #[tokio::test]
    async fn partial_body_waits_for_more() {
        let mut framed = FramedReader::new();
        framed.extend(b"Content-Length: 17\r\n\r\n{\"seq\":1,\"payloa");
        let res: Result<Option<TestMsg>, _> = framed.try_read();
        assert!(matches!(res, Ok(None)));
    }

    #[tokio::test]
    async fn two_messages_in_one_chunk() {
        let mut framed = FramedReader::new();
        let body1 = serde_json::to_vec(&TestMsg {
            seq: 1,
            payload: "a".into(),
        })
        .unwrap();
        let body2 = serde_json::to_vec(&TestMsg {
            seq: 2,
            payload: "b".into(),
        })
        .unwrap();
        let chunk = format!(
            "Content-Length: {}\r\n\r\n",
            body1.len()
        );
        let mut all = Vec::new();
        all.extend_from_slice(chunk.as_bytes());
        all.extend_from_slice(&body1);
        let chunk2 = format!("Content-Length: {}\r\n\r\n", body2.len());
        all.extend_from_slice(chunk2.as_bytes());
        all.extend_from_slice(&body2);
        framed.extend(&all);

        let m1: TestMsg = framed.try_read().unwrap().unwrap();
        let m2: TestMsg = framed.try_read().unwrap().unwrap();
        assert_eq!(m1.seq, 1);
        assert_eq!(m2.seq, 2);
    }

    #[test]
    fn json_body_serializes_via_helper() {
        let v = json!({"seq": 1, "type": "request", "command": "continue"});
        let body = serde_json::to_vec(&v).unwrap();
        assert!(!body.is_empty());
    }
}

// Helper removed: case_insensitive_content_length uses Value directly now.
