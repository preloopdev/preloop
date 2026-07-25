//! Shared HTTP client with CA bundle and proxy support.

use anyhow::{Context, Result};
use rand::Rng;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use std::path::Path;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error("HTTP status {status}: {body}")]
    Status {
        status: reqwest::StatusCode,
        body: String,
    },
}

/// Runner.Listener reconnect jitter from MessageListener.cs v2.335.1.
/// Consecutive failures use [15, 30) seconds for the first five retries and
/// [30, 60) seconds afterwards; a successful poll resets the counter.
#[derive(Debug, Default, Clone)]
pub(crate) struct SessionBackoff {
    consecutive_errors: u32,
}

impl SessionBackoff {
    pub(crate) fn next_delay(&mut self) -> Duration {
        self.consecutive_errors = self.consecutive_errors.saturating_add(1);
        let (min, max) = if self.consecutive_errors <= 5 {
            (15_000, 30_000)
        } else {
            (30_000, 60_000)
        };
        Duration::from_millis(rand::thread_rng().gen_range(min..max))
    }

    pub(crate) fn reset(&mut self) {
        self.consecutive_errors = 0;
    }

    #[cfg(test)]
    fn consecutive_errors(&self) -> u32 {
        self.consecutive_errors
    }
}

fn is_transient_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

/// Shared HTTP client wrapping reqwest with CA bundle, proxy, and auth.
#[derive(Clone)]
pub struct HttpClient {
    inner: reqwest::Client,
    control: Option<ControlTransport>,
}

#[derive(Clone)]
struct ControlTransport {
    origin: reqwest::Url,
    client: reqwest::Client,
}

fn build_reqwest_client(ca_pem: Option<&[u8]>, socket: Option<&Path>) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(100))
        .connect_timeout(Duration::from_secs(30))
        .user_agent(format!(
            "aksh-runner/{} (protocol-compat {})",
            crate::VERSION,
            crate::PROTOCOL_COMPAT_VERSION
        ));
    if let Some(pem) = ca_pem {
        builder = builder.add_root_certificate(reqwest::Certificate::from_pem(pem)?);
    }
    if let Some(socket) = socket {
        #[cfg(unix)]
        {
            builder = builder.unix_socket(socket);
        }
        #[cfg(not(unix))]
        {
            let _ = socket;
            anyhow::bail!("Preloop control sockets require Unix");
        }
    }
    Ok(builder.build()?)
}

fn same_origin(left: &reqwest::Url, right: &reqwest::Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

impl HttpClient {
    /// Create a new HTTP client, optionally trusting an extra CA bundle.
    ///
    /// The control transport, if any, is read from `PRELOOP_CONTROL_SOCKET` /
    /// `PRELOOP_CONTROL_ORIGIN`, which the orchestrator sets on the runner
    /// process. Use [`HttpClient::with_control`] to configure it explicitly.
    pub fn new(ca_bundle: Option<&Path>) -> Result<Self> {
        let socket = std::env::var_os("PRELOOP_CONTROL_SOCKET").map(std::path::PathBuf::from);
        let origin = std::env::var("PRELOOP_CONTROL_ORIGIN").ok();
        let control = match (socket, origin) {
            (None, None) => None,
            (Some(socket), Some(origin)) => Some((socket, origin)),
            _ => anyhow::bail!(
                "PRELOOP_CONTROL_SOCKET and PRELOOP_CONTROL_ORIGIN must be set together"
            ),
        };
        Self::with_control(
            ca_bundle,
            control.as_ref().map(|(s, o)| (s.as_path(), o.as_str())),
        )
    }

    /// Create a client with an explicit control-plane transport.
    ///
    /// `control` is a `(unix socket, origin URL)` pair; requests to that origin
    /// are routed over the socket and everything else uses ordinary networking.
    pub fn with_control(ca_bundle: Option<&Path>, control: Option<(&Path, &str)>) -> Result<Self> {
        let env_ca = std::env::var("SSL_CERT_FILE")
            .ok()
            .map(std::path::PathBuf::from);
        let ca_path = ca_bundle.or(env_ca.as_deref());
        let ca_pem = ca_path
            .map(|path| {
                std::fs::read(path).with_context(|| format!("reading CA bundle {}", path.display()))
            })
            .transpose()?;

        let inner = build_reqwest_client(ca_pem.as_deref(), None)?;
        let control = control
            .map(|(socket, origin)| {
                let origin = reqwest::Url::parse(origin)
                    .with_context(|| format!("invalid control origin `{origin}`"))?;
                if origin.host_str().is_none() {
                    anyhow::bail!("control origin must include a host");
                }
                anyhow::Ok(ControlTransport {
                    origin,
                    client: build_reqwest_client(ca_pem.as_deref(), Some(socket))?,
                })
            })
            .transpose()?;
        Ok(Self { inner, control })
    }

    /// Select the transport for a URL, routing only the configured local
    /// control-plane origin through its mounted Unix socket.
    pub fn client_for(&self, url: &str) -> &reqwest::Client {
        if let Some(control) = &self.control {
            if reqwest::Url::parse(url).is_ok_and(|url| same_origin(&url, &control.origin)) {
                return &control.client;
            }
        }
        &self.inner
    }

    /// GET request returning JSON.
    pub async fn get_json<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T> {
        let resp = self
            .client_for(url)
            .get(url)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("GET {url} returned {status}: {body}");
        }
        resp.json()
            .await
            .with_context(|| format!("parsing GET {url}"))
    }

    /// GET request with bearer auth returning JSON.
    pub async fn get_json_with_auth<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        auth: &str,
    ) -> Result<T> {
        let resp = self
            .client_for(url)
            .get(url)
            .header(ACCEPT, "application/json")
            .header(AUTHORIZATION, auth)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("GET {url} returned {status}: {body}");
        }
        resp.json()
            .await
            .with_context(|| format!("parsing GET {url}"))
    }

    /// GET request with bearer auth and custom Accept header returning JSON.
    pub async fn get_json_with_auth_accept<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        auth: &str,
        accept: &str,
    ) -> Result<T> {
        let resp = self
            .client_for(url)
            .get(url)
            .header(ACCEPT, accept)
            .header(AUTHORIZATION, auth)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("GET {url} returned {status}: {body}");
        }
        resp.json()
            .await
            .with_context(|| format!("parsing GET {url}"))
    }

    /// GET request returning raw bytes.
    pub async fn get_bytes(&self, url: &str) -> Result<bytes::Bytes> {
        let resp = self
            .client_for(url)
            .get(url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("GET {url} returned {status}");
        }
        resp.bytes()
            .await
            .with_context(|| format!("reading body of GET {url}"))
    }

    /// POST JSON with custom authorization header, returning JSON.
    /// P1.7: Retries up to 3 times on transient 5xx or network errors with exponential backoff.
    pub async fn post_json_with_auth<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        body: &serde_json::Value,
        auth: &str,
    ) -> Result<T> {
        let mut last_err = None;
        for attempt in 0..3u32 {
            if attempt > 0 {
                let delay = Duration::from_secs(1 << attempt); // 2s, 4s
                tracing::warn!(
                    "Retrying POST {url} (attempt {}, backoff {delay:?})",
                    attempt + 1
                );
                tokio::time::sleep(delay).await;
            }
            let resp = match self
                .client_for(url)
                .post(url)
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, "application/json")
                .header(AUTHORIZATION, auth)
                .json(body)
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    last_err = Some(anyhow::anyhow!("POST {url}: {e}"));
                    continue;
                }
            };
            let status = resp.status();
            if is_transient_status(status) {
                let body_text = resp.text().await.unwrap_or_default();
                last_err = Some(anyhow::anyhow!("POST {url} returned {status}: {body_text}"));
                continue;
            }
            if !status.is_success() {
                let body_text = resp.text().await.unwrap_or_default();
                return Err(anyhow::Error::new(HttpError::Status {
                    status,
                    body: body_text,
                }));
            }
            return json_or_null(resp, &format!("parsing POST {url}")).await;
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("POST {url} failed after 3 retries")))
    }

    /// POST JSON with custom authorization, Accept, and Content-Type headers.
    pub async fn post_json_with_auth_headers<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        body: &serde_json::Value,
        auth: &str,
        accept: &str,
        content_type: &str,
    ) -> Result<T> {
        let resp = self
            .client_for(url)
            .post(url)
            .header(CONTENT_TYPE, content_type)
            .header(ACCEPT, accept)
            .header(AUTHORIZATION, auth)
            .json(body)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow::Error::new(HttpError::Status { status, body }));
        }
        json_or_null(resp, &format!("parsing POST {url}")).await
    }

    /// POST form-encoded data returning JSON.
    pub async fn post_form_json<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        form: &[(&str, &str)],
    ) -> Result<T> {
        let resp = self
            .client_for(url)
            .post(url)
            .form(form)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .with_context(|| format!("POST form {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow::Error::new(HttpError::Status { status, body }));
        }
        resp.json()
            .await
            .with_context(|| format!("parsing POST form {url}"))
    }

    /// POST JSON with bearer auth, returning JSON.
    pub async fn post_json_bearer<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        body: &serde_json::Value,
        token: &str,
    ) -> Result<T> {
        self.post_json_with_auth(url, body, &format!("Bearer {token}"))
            .await
    }

    /// PATCH JSON with bearer auth, returning JSON.
    pub async fn patch_json_bearer<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        body: &serde_json::Value,
        token: &str,
    ) -> Result<T> {
        let resp = self
            .client_for(url)
            .patch(url)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .json(body)
            .send()
            .await
            .with_context(|| format!("PATCH {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow::Error::new(HttpError::Status { status, body }));
        }
        resp.json()
            .await
            .with_context(|| format!("parsing PATCH {url}"))
    }

    /// DELETE with bearer auth.
    pub async fn delete_with_token(&self, url: &str, token: &str) -> Result<()> {
        let resp = self
            .client_for(url)
            .delete(url)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .send()
            .await
            .with_context(|| format!("DELETE {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow::Error::new(HttpError::Status { status, body }));
        }
        Ok(())
    }

    /// DELETE with bearer auth and one protocol-specific header.
    pub async fn delete_with_token_header(
        &self,
        url: &str,
        token: &str,
        header_name: &str,
        header_value: &str,
    ) -> Result<()> {
        let resp = self
            .client_for(url)
            .delete(url)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(header_name, header_value)
            .send()
            .await
            .with_context(|| format!("DELETE {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow::Error::new(HttpError::Status { status, body }));
        }
        Ok(())
    }

    /// PUT raw bytes with content type.
    /// P1.7: Retries up to 3 times on transient 5xx or network errors.
    pub async fn put_bytes(&self, url: &str, data: Vec<u8>, content_type: &str) -> Result<()> {
        let mut last_err = None;
        for attempt in 0..3u32 {
            if attempt > 0 {
                let delay = Duration::from_secs(1 << attempt);
                tracing::warn!(
                    "Retrying PUT {url} (attempt {}, backoff {delay:?})",
                    attempt + 1
                );
                tokio::time::sleep(delay).await;
            }
            let resp = match self
                .client_for(url)
                .put(url)
                .header(CONTENT_TYPE, content_type)
                .header("x-ms-blob-type", "BlockBlob")
                .body(data.clone())
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    last_err = Some(anyhow::anyhow!("PUT {url}: {e}"));
                    continue;
                }
            };
            let status = resp.status();
            if is_transient_status(status) {
                let body = resp.text().await.unwrap_or_default();
                last_err = Some(anyhow::anyhow!("PUT {url} returned {status}: {body}"));
                continue;
            }
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(anyhow::Error::new(HttpError::Status { status, body }));
            }
            return Ok(());
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("PUT {url} failed after 3 retries")))
    }

    /// PUT bytes with Bearer token authentication (for AzDO log append).
    pub async fn put_bytes_bearer(
        &self,
        url: &str,
        data: Vec<u8>,
        content_type: &str,
        token: &str,
    ) -> Result<()> {
        let mut last_err = None;
        for attempt in 0..3u32 {
            if attempt > 0 {
                let delay = Duration::from_secs(1 << attempt);
                tracing::warn!(
                    "Retrying PUT {url} (attempt {}, backoff {delay:?})",
                    attempt + 1
                );
                tokio::time::sleep(delay).await;
            }
            let resp = match self
                .client_for(url)
                .put(url)
                .header(CONTENT_TYPE, content_type)
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(data.clone())
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    last_err = Some(anyhow::anyhow!("PUT {url}: {e}"));
                    continue;
                }
            };
            let status = resp.status();
            if is_transient_status(status) {
                let body = resp.text().await.unwrap_or_default();
                last_err = Some(anyhow::anyhow!("PUT {url} returned {status}: {body}"));
                continue;
            }
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(anyhow::Error::new(HttpError::Status { status, body }));
            }
            return Ok(());
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("PUT {url} failed after 3 retries")))
    }

    /// Build a long-poll GET request with timeout.
    pub async fn get_long_poll<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        auth: &str,
        timeout: Duration,
    ) -> Result<Option<T>> {
        let resp = self
            .client_for(url)
            .get(url)
            .header(ACCEPT, "application/json")
            .header(AUTHORIZATION, auth)
            .timeout(timeout)
            .send()
            .await;
        match resp {
            Ok(r) => {
                let status = r.status();
                if status == reqwest::StatusCode::OK {
                    let body: T = r.json().await?;
                    Ok(Some(body))
                } else if status == reqwest::StatusCode::ACCEPTED
                    || status == reqwest::StatusCode::NO_CONTENT
                {
                    // Long-poll timeout — no message
                    Ok(None)
                } else {
                    let body = r.text().await.unwrap_or_default();
                    Err(anyhow::Error::new(HttpError::Status { status, body }))
                }
            }
            Err(e) if e.is_timeout() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

async fn json_or_null<T: serde::de::DeserializeOwned>(
    resp: reqwest::Response,
    context: &str,
) -> Result<T> {
    let text = resp.text().await.with_context(|| context.to_string())?;
    if text.trim().is_empty() {
        serde_json::from_value(serde_json::Value::Null).with_context(|| context.to_string())
    } else {
        serde_json::from_str(&text).with_context(|| context.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    #[test]
    fn session_backoff_uses_official_bounded_windows_and_resets() {
        let mut backoff = SessionBackoff::default();
        for _ in 0..5 {
            let delay = backoff.next_delay();
            assert!(delay >= Duration::from_secs(15));
            assert!(delay < Duration::from_secs(30));
        }
        for _ in 0..3 {
            let delay = backoff.next_delay();
            assert!(delay >= Duration::from_secs(30));
            assert!(delay < Duration::from_secs(60));
        }
        assert_eq!(backoff.consecutive_errors(), 8);
        backoff.reset();
        assert_eq!(backoff.consecutive_errors(), 0);
        let delay = backoff.next_delay();
        assert!(delay >= Duration::from_secs(15));
        assert!(delay < Duration::from_secs(30));
    }

    async fn serve_once(status: &str, body: &str) -> (String, oneshot::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (request_tx, request_rx) = oneshot::channel();
        let status = status.to_string();
        let body = body.to_string();

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let n = socket.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]).to_string();
            let _ = request_tx.send(request);
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });

        (format!("http://{addr}"), request_rx)
    }

    #[tokio::test]
    async fn post_json_with_auth_handles_empty_success_response() {
        let (url, request_rx) = serve_once("200 OK", "").await;
        let client = HttpClient::new(None).unwrap();

        let value: serde_json::Value = client
            .post_json_with_auth(&url, &serde_json::json!({"hello": "world"}), "Bearer token")
            .await
            .unwrap();

        assert!(value.is_null());
        let request = request_rx.await.unwrap();
        assert!(request.starts_with("POST / HTTP/1.1"));
        assert!(request.contains("authorization: Bearer token"));
        assert!(request.contains(r#""hello":"world""#));
    }

    #[tokio::test]
    async fn post_json_with_auth_preserves_client_error_body() {
        let (url, _request_rx) = serve_once("400 Bad Request", r#"{"message":"bad token"}"#).await;
        let client = HttpClient::new(None).unwrap();

        let err = client
            .post_json_with_auth::<serde_json::Value>(&url, &serde_json::json!({}), "Bearer token")
            .await
            .unwrap_err();

        let http_err = err
            .downcast_ref::<HttpError>()
            .expect("expected typed HttpError");
        match http_err {
            HttpError::Status { status, body } => {
                assert_eq!(*status, reqwest::StatusCode::BAD_REQUEST);
                assert_eq!(body, r#"{"message":"bad token"}"#);
            }
        }
    }

    #[tokio::test]
    async fn post_json_with_auth_headers_handles_empty_success_response() {
        let (url, _request_rx) = serve_once("204 No Content", "").await;
        let client = HttpClient::new(None).unwrap();

        let value: serde_json::Value = client
            .post_json_with_auth_headers(
                &url,
                &serde_json::json!({}),
                "Bearer token",
                "application/json",
                "application/json",
            )
            .await
            .unwrap();

        assert!(value.is_null());
    }

    #[tokio::test]
    async fn get_long_poll_treats_accepted_as_no_message() {
        let (url, _request_rx) = serve_once("202 Accepted", "").await;
        let client = HttpClient::new(None).unwrap();

        let value: Option<serde_json::Value> = client
            .get_long_poll(&url, "Bearer token", Duration::from_secs(1))
            .await
            .unwrap();

        assert!(value.is_none());
    }

    #[tokio::test]
    async fn get_long_poll_treats_no_content_as_no_message() {
        let (url, _request_rx) = serve_once("204 No Content", "").await;
        let client = HttpClient::new(None).unwrap();

        let value: Option<serde_json::Value> = client
            .get_long_poll(&url, "Bearer token", Duration::from_secs(1))
            .await
            .unwrap();

        assert!(value.is_none());
    }

    #[tokio::test]
    async fn get_long_poll_preserves_error_body() {
        let (url, _request_rx) = serve_once("409 Conflict", "session expired").await;
        let client = HttpClient::new(None).unwrap();

        let err = client
            .get_long_poll::<serde_json::Value>(&url, "Bearer token", Duration::from_secs(1))
            .await
            .unwrap_err();

        let http_err = err
            .downcast_ref::<HttpError>()
            .expect("expected typed HttpError");
        match http_err {
            HttpError::Status { status, body } => {
                assert_eq!(*status, reqwest::StatusCode::CONFLICT);
                assert_eq!(body, "session expired");
            }
        }
    }

    #[tokio::test]
    async fn put_bytes_preserves_client_error_body() {
        let (url, request_rx) = serve_once("403 Forbidden", "signed url expired").await;
        let client = HttpClient::new(None).unwrap();

        let err = client
            .put_bytes(&url, b"log-data".to_vec(), "text/plain")
            .await
            .unwrap_err();

        let request = request_rx.await.unwrap();
        assert!(request.starts_with("PUT / HTTP/1.1"));
        assert!(request.contains("x-ms-blob-type: BlockBlob"));

        let http_err = err
            .downcast_ref::<HttpError>()
            .expect("expected typed HttpError");
        match http_err {
            HttpError::Status { status, body } => {
                assert_eq!(*status, reqwest::StatusCode::FORBIDDEN);
                assert_eq!(body, "signed url expired");
            }
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn control_origin_routes_only_matching_requests_over_unix_socket() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = directory.path().join("engine.sock");
        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
        let unix_task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 4096];
            let _ = socket.read(&mut request).await.unwrap();
            let body = r#"{"transport":"unix"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        // The control origin is never contacted over TCP, so bind a real
        // listener for it and assert it stays untouched. A second listener on
        // its own port is an unambiguously different origin — deriving one by
        // swapping in `localhost` is flaky wherever that resolves to `::1`.
        let (control_url, mut control_request) =
            serve_once("200 OK", r#"{"transport":"tcp"}"#).await;
        let (public_url, public_request) = serve_once("200 OK", r#"{"transport":"tcp"}"#).await;
        assert_ne!(control_url, public_url);

        let client = HttpClient::with_control(None, Some((socket_path.as_path(), &control_url)))
            .expect("client with control transport");
        let unix: serde_json::Value = client
            .get_json(&format!("{control_url}/control"))
            .await
            .expect("control-origin request");
        let tcp: serde_json::Value = client
            .get_json(&format!("{public_url}/other"))
            .await
            .expect("public-origin request");

        assert_eq!(unix["transport"], "unix");
        assert_eq!(tcp["transport"], "tcp");
        let request = public_request.await.unwrap();
        assert!(request.starts_with("GET /other HTTP/1.1"));
        unix_task.await.unwrap();

        // The control origin's TCP listener must never have been used. Its
        // accept task is still parked, so poll without blocking on it.
        assert!(
            matches!(
                control_request.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ),
            "control-origin request bypassed the Unix socket"
        );
    }
}
