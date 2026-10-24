//! A minimal Connect protocol implementation (JSON codec).
//!
//! Only what the sandbox API uses: unary calls (`application/json`) and
//! server-streaming calls (`application/connect+json`, 5-byte envelopes,
//! trailing end-of-stream frame). See <https://connectrpc.com/docs/protocol>.
//! Unary and server streaming both work over HTTP/1.1, which is the point.

use std::fmt;

use axum::body::Body;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::{BufMut, Bytes, BytesMut};
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// Connect error codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Code {
    /// Cancelled.
    Canceled,
    /// Unknown.
    Unknown,
    /// Bad request.
    InvalidArgument,
    /// Timed out.
    DeadlineExceeded,
    /// Missing.
    NotFound,
    /// Duplicate.
    AlreadyExists,
    /// Forbidden.
    PermissionDenied,
    /// Out of capacity.
    ResourceExhausted,
    /// Wrong state (e.g. unsupported capability).
    FailedPrecondition,
    /// Lost a race (stale generation).
    Aborted,
    /// Out of range.
    OutOfRange,
    /// Not implemented.
    Unimplemented,
    /// Server bug.
    Internal,
    /// Try again.
    Unavailable,
    /// Data loss.
    DataLoss,
    /// Missing credentials.
    Unauthenticated,
}

impl Code {
    fn http_status(self) -> StatusCode {
        match self {
            Code::Canceled => StatusCode::from_u16(499).unwrap_or(StatusCode::BAD_REQUEST),
            Code::Unknown | Code::Internal | Code::DataLoss => StatusCode::INTERNAL_SERVER_ERROR,
            Code::InvalidArgument | Code::OutOfRange => StatusCode::BAD_REQUEST,
            Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
            Code::NotFound => StatusCode::NOT_FOUND,
            Code::AlreadyExists | Code::Aborted => StatusCode::CONFLICT,
            Code::PermissionDenied => StatusCode::FORBIDDEN,
            Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
            Code::FailedPrecondition => StatusCode::PRECONDITION_FAILED,
            Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
            Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        }
    }

    fn from_http(status: StatusCode) -> Code {
        match status.as_u16() {
            400 => Code::InvalidArgument,
            401 => Code::Unauthenticated,
            403 => Code::PermissionDenied,
            404 => Code::NotFound,
            409 => Code::Aborted,
            412 => Code::FailedPrecondition,
            429 | 502 | 503 | 504 => Code::Unavailable,
            501 => Code::Unimplemented,
            _ => Code::Unknown,
        }
    }

    /// Snake-case wire name (`failed_precondition`, …): a bounded label for
    /// metrics, matching the JSON codec's own rendering.
    pub fn as_str(self) -> &'static str {
        match self {
            Code::Canceled => "canceled",
            Code::Unknown => "unknown",
            Code::InvalidArgument => "invalid_argument",
            Code::DeadlineExceeded => "deadline_exceeded",
            Code::NotFound => "not_found",
            Code::AlreadyExists => "already_exists",
            Code::PermissionDenied => "permission_denied",
            Code::ResourceExhausted => "resource_exhausted",
            Code::FailedPrecondition => "failed_precondition",
            Code::Aborted => "aborted",
            Code::OutOfRange => "out_of_range",
            Code::Unimplemented => "unimplemented",
            Code::Internal => "internal",
            Code::Unavailable => "unavailable",
            Code::DataLoss => "data_loss",
            Code::Unauthenticated => "unauthenticated",
        }
    }
}

/// A Connect error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Error {
    /// Code.
    pub code: Code,
    /// Human-readable message.
    #[serde(default)]
    pub message: String,
}

impl Error {
    /// Construct an error.
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
    /// `invalid_argument`.
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(Code::InvalidArgument, message)
    }
    /// `not_found`.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(Code::NotFound, message)
    }
    /// `failed_precondition`.
    pub fn precondition(message: impl Into<String>) -> Self {
        Self::new(Code::FailedPrecondition, message)
    }
    /// `internal`.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(Code::Internal, message)
    }
    /// `unavailable`.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(Code::Unavailable, message)
    }
    /// `aborted` (fencing: stale generation or incarnation).
    pub fn aborted(message: impl Into<String>) -> Self {
        Self::new(Code::Aborted, message)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code = serde_json::to_value(self.code)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .unwrap_or_default();
        write!(f, "{code}: {}", self.message)
    }
}

impl std::error::Error for Error {}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let body = serde_json::to_vec(&self).unwrap_or_default();
        (
            self.code.http_status(),
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            body,
        )
            .into_response()
    }
}

/// Result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

const END_STREAM: u8 = 0x02;

/// Encode one envelope.
pub fn envelope(flags: u8, payload: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(5 + payload.len());
    out.put_u8(flags);
    out.put_u32(payload.len() as u32);
    out.put_slice(payload);
    out.freeze()
}

/// Decode a unary JSON request body.
pub fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T> {
    let body = if body.is_empty() {
        b"{}".as_slice()
    } else {
        body
    };
    serde_json::from_slice(body)
        .map_err(|error| Error::invalid(format!("bad request body: {error}")))
}

/// Encode a unary JSON response.
pub fn unary_response<T: Serialize>(result: Result<T>) -> Response {
    match result {
        Ok(value) => match serde_json::to_vec(&value) {
            Ok(body) => (
                StatusCode::OK,
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                body,
            )
                .into_response(),
            Err(error) => Error::internal(error.to_string()).into_response(),
        },
        Err(error) => error.into_response(),
    }
}

/// Decode the single enveloped request of a server-streaming call.
pub fn decode_stream_request<T: DeserializeOwned>(body: &[u8]) -> Result<T> {
    if body.len() < 5 {
        return Err(Error::invalid("missing request envelope"));
    }
    let length = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
    let payload = body
        .get(5..5 + length)
        .ok_or_else(|| Error::invalid("truncated request envelope"))?;
    decode(payload)
}

#[derive(Serialize)]
struct EndStream<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a Error>,
}

/// Build a server-streaming response. The stream ends with an end-of-stream
/// frame carrying the first error, if any.
pub fn stream_response<T, S>(stream: S) -> Response
where
    T: Serialize + Send + 'static,
    S: Stream<Item = Result<T>> + Send + 'static,
{
    let frames =
        futures::stream::unfold((Box::pin(stream), false), |(mut stream, done)| async move {
            if done {
                return None;
            }
            match stream.next().await {
                Some(Ok(item)) => {
                    let payload = serde_json::to_vec(&item).unwrap_or_default();
                    Some((
                        Ok::<_, std::io::Error>(envelope(0, &payload)),
                        (stream, false),
                    ))
                }
                Some(Err(error)) => {
                    let payload = serde_json::to_vec(&EndStream {
                        error: Some(&error),
                    })
                    .unwrap_or_default();
                    Some((Ok(envelope(END_STREAM, &payload)), (stream, true)))
                }
                None => {
                    let payload =
                        serde_json::to_vec(&EndStream { error: None }).unwrap_or_default();
                    Some((Ok(envelope(END_STREAM, &payload)), (stream, true)))
                }
            }
        });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/connect+json")
        .body(Body::from_stream(frames))
        .unwrap_or_else(|_| Error::internal("response build").into_response())
}

/// Connect client over HTTP(S), JSON codec.
#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    base: String,
    token: Option<String>,
}

impl Client {
    /// `base` like `http://host:9191`; `token` is sent as a bearer.
    pub fn new(base: impl Into<String>, token: Option<String>) -> Self {
        let http = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        Self {
            http,
            base: base.into().trim_end_matches('/').to_owned(),
            token,
        }
    }

    /// Base URL.
    pub fn base(&self) -> &str {
        &self.base
    }

    /// Underlying HTTP client (for data-plane tickets).
    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// Bearer token, if any.
    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    fn request(&self, path: &str) -> reqwest::RequestBuilder {
        let mut request = self.http.post(format!("{}/{path}", self.base));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        request
    }

    /// Unary call: `path` is `package.Service/Method`.
    pub async fn unary<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        path: &str,
        request: &Req,
    ) -> Result<Resp> {
        let body = serde_json::to_vec(request).map_err(|e| Error::internal(e.to_string()))?;
        let response = self
            .request(path)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| Error::unavailable(format!("{path}: {error}")))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|error| Error::unavailable(format!("{path}: {error}")))?;
        if !status.is_success() {
            return Err(serde_json::from_slice::<Error>(&bytes).unwrap_or_else(|_| {
                Error::new(
                    Code::from_http(status),
                    String::from_utf8_lossy(&bytes).into_owned(),
                )
            }));
        }
        decode(&bytes)
    }

    /// Server-streaming call; yields messages until end of stream.
    pub async fn server_stream<Req: Serialize, Resp: DeserializeOwned + Send + 'static>(
        &self,
        path: &str,
        request: &Req,
    ) -> Result<impl Stream<Item = Result<Resp>> + Send + 'static> {
        let payload = serde_json::to_vec(request).map_err(|e| Error::internal(e.to_string()))?;
        let response = self
            .request(path)
            .header(header::CONTENT_TYPE, "application/connect+json")
            .body(envelope(0, &payload).to_vec())
            .send()
            .await
            .map_err(|error| Error::unavailable(format!("{path}: {error}")))?;
        let status = response.status();
        if !status.is_success() {
            let bytes = response.bytes().await.unwrap_or_default();
            return Err(serde_json::from_slice::<Error>(&bytes).unwrap_or_else(|_| {
                Error::new(
                    Code::from_http(status),
                    String::from_utf8_lossy(&bytes).into_owned(),
                )
            }));
        }
        let body = response.bytes_stream();
        Ok(decode_frames::<Resp, _>(body))
    }
}

#[derive(Deserialize)]
struct EndStreamOwned {
    #[serde(default)]
    error: Option<Error>,
}

/// Decode an enveloped byte stream into messages.
pub fn decode_frames<T, S>(body: S) -> impl Stream<Item = Result<T>> + Send + 'static
where
    T: DeserializeOwned + Send + 'static,
    S: Stream<Item = reqwest::Result<Bytes>> + Send + 'static,
{
    futures::stream::unfold(
        (Box::pin(body), BytesMut::new(), false),
        |(mut body, mut buffer, done)| async move {
            if done {
                return None;
            }
            loop {
                if buffer.len() >= 5 {
                    let flags = buffer[0];
                    let length =
                        u32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]) as usize;
                    if buffer.len() >= 5 + length {
                        let frame = buffer.split_to(5 + length);
                        let payload = &frame[5..];
                        if flags & END_STREAM != 0 {
                            let end: EndStreamOwned = serde_json::from_slice(payload)
                                .unwrap_or(EndStreamOwned { error: None });
                            return end.error.map(|error| (Err(error), (body, buffer, true)));
                        }
                        let item = decode::<T>(payload);
                        return Some((item, (body, buffer, false)));
                    }
                }
                match body.next().await {
                    Some(Ok(chunk)) => buffer.extend_from_slice(&chunk),
                    Some(Err(error)) => {
                        return Some((
                            Err(Error::unavailable(format!("stream: {error}"))),
                            (body, buffer, true),
                        ));
                    }
                    None => {
                        return Some((
                            Err(Error::unavailable(
                                "stream ended without end-of-stream frame",
                            )),
                            (body, buffer, true),
                        ));
                    }
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frames_round_trip_and_surface_trailing_error() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&envelope(0, br#"{"a":1}"#));
        wire.extend_from_slice(&envelope(0, br#"{"a":2}"#));
        let end = serde_json::to_vec(
            &serde_json::json!({"error": {"code": "not_found", "message": "gone"}}),
        )
        .unwrap();
        wire.extend_from_slice(&envelope(END_STREAM, &end));
        // Split mid-frame to exercise buffering.
        let (left, right) = wire.split_at(7);
        let body = futures::stream::iter(vec![
            Ok(Bytes::copy_from_slice(left)),
            Ok(Bytes::copy_from_slice(right)),
        ]);
        let items: Vec<Result<serde_json::Value>> = decode_frames(body).collect().await;
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].as_ref().unwrap()["a"], 1);
        assert_eq!(items[1].as_ref().unwrap()["a"], 2);
        assert_eq!(items[2].as_ref().unwrap_err().code, Code::NotFound);
    }

    #[tokio::test]
    async fn truncated_stream_is_an_error_not_a_clean_end() {
        let body = futures::stream::iter(vec![Ok(envelope(0, br#"{"a":1}"#))]);
        let items: Vec<Result<serde_json::Value>> = decode_frames(body).collect().await;
        assert_eq!(items.len(), 2);
        assert_eq!(items[1].as_ref().unwrap_err().code, Code::Unavailable);
    }
}
