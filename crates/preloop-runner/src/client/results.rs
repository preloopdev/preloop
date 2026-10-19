//! Results service client (Twirp step updates and log uploads).
//!
//! Log blobs travel over signed `/twirp-blob/…` upload URLs minted by the
//! results service (Get*SignedBlobURL). Each URL is a capability JWT-in-a-URL:
//! anyone holding it can write the pending blob, and `RefreshUploadURL` remints
//! an *expired* one for as long as the job lives — so an expired URL is not a
//! spent credential. Raw URLs therefore never cross this module's public API:
//! callers hand in content and get a [`UploadError`] back whose rendering is
//! structurally unable to leak a URL.

use anyhow::{Context, Result};

use super::http::{HttpClient, HttpError};

/// Client for the results service (Twirp/proto endpoints).
pub struct ResultsClient {
    http: HttpClient,
    base_url: String,
}

/// An opaque signed `/twirp-blob` upload URL.
///
/// The URL is a credential — anyone holding it can write the pending blob — so
/// the inner string is private and there is deliberately no `Display`, no
/// `as_str`, and no `Deref` to `str`. `Debug` is hand-written to print only
/// `UploadUrl(<redacted>)`. Never `#[derive(Debug)]` on this type or on any
/// struct that contains it.
pub struct UploadUrl(String);

impl std::fmt::Debug for UploadUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UploadUrl(<redacted>)")
    }
}

/// Which step of an upload sequence failed. Safe to print.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadStage {
    /// The Get*SignedBlobURL mint call.
    Mint,
    /// The blob PUT.
    Put,
    /// The RefreshUploadURL remint call.
    Refresh,
}

/// A log-blob upload failure at the upload boundary.
///
/// Carries only the logical blob identity, the failed stage, and the HTTP
/// status — never a signed URL. The underlying client error is consumed and
/// dropped rather than kept as a source: `HttpClient` errors embed the request
/// URL in their messages, and an `anyhow` chain prints every source. This type
/// therefore intentionally has no `source`.
#[derive(Debug)]
pub struct UploadError {
    /// Logical blob identity (e.g. `step logs for step 7`); never a URL.
    pub blob: String,
    /// Which step of the upload sequence failed.
    pub stage: UploadStage,
    /// HTTP status of the failed request, when the server answered.
    pub status: Option<reqwest::StatusCode>,
}

impl UploadError {
    /// Map a client error into a URL-free upload failure.
    ///
    /// `error` is consumed and dropped: it may render the signed URL in its
    /// message. Only the typed HTTP status (URL-free) survives.
    fn at(blob: &str, stage: UploadStage, error: anyhow::Error) -> Self {
        let status = error
            .downcast_ref::<HttpError>()
            .map(|HttpError::Status { status, .. }| *status);
        Self {
            blob: blob.to_owned(),
            stage,
            status,
        }
    }
}

impl std::fmt::Display for UploadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let what = match self.stage {
            UploadStage::Mint => "minting upload URL",
            UploadStage::Put => "uploading blob",
            UploadStage::Refresh => "refreshing upload URL",
        };
        write!(f, "{what} for {} failed", self.blob)?;
        if let Some(status) = self.status {
            write!(f, ": HTTP {status}")?;
        }
        Ok(())
    }
}

impl std::error::Error for UploadError {}

impl ResultsClient {
    /// Create a new results client.
    pub fn new(http: HttpClient, base_url: String) -> Self {
        Self {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Expose the underlying HTTP client for direct requests.
    pub fn http(&self) -> &crate::client::http::HttpClient {
        &self.http
    }

    /// Report step status updates via Twirp.
    pub async fn update_workflow_steps(
        &self,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!(
            "{}/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
            self.base_url
        );
        self.http
            .post_json_bearer(&url, body, token)
            .await
            .context("updating workflow steps")
    }

    // ── Log blob uploads: content in, typed result out ─────────

    /// Upload a step's log bytes (GetStepLogsSignedBlobURL → PUT blob).
    ///
    /// Minting, the PUT, and expired-URL refresh (401/403 → RefreshUploadURL
    /// → retry) all happen inside this call; no signed URL crosses the
    /// boundary. `metadata` is the mint request body (run/job/step backend
    /// ids). On success the caller finalizes with
    /// [`Self::create_step_logs_metadata`].
    pub async fn upload_step_logs(
        &self,
        job_token: &str,
        metadata: &serde_json::Value,
        content: Vec<u8>,
    ) -> Result<(), UploadError> {
        let blob = logical_blob_id("step logs", metadata);
        let url = self
            .get_step_logs_signed_url(job_token, metadata, &blob)
            .await?;
        self.upload_blob_with_refresh(job_token, url, content, &blob)
            .await
    }

    /// Upload step summary bytes (GetStepSummarySignedBlobURL → PUT blob).
    ///
    /// Same contract as [`Self::upload_step_logs`]; on success the caller
    /// finalizes with [`Self::create_step_summary_metadata`].
    pub async fn upload_step_summary(
        &self,
        job_token: &str,
        metadata: &serde_json::Value,
        content: Vec<u8>,
    ) -> Result<(), UploadError> {
        let blob = logical_blob_id("step summary", metadata);
        let url = self
            .get_step_summary_signed_url(job_token, metadata, &blob)
            .await?;
        self.upload_blob_with_refresh(job_token, url, content, &blob)
            .await
    }

    /// Upload the full job log bytes (GetJobLogsSignedBlobURL → PUT blob).
    ///
    /// Same contract as [`Self::upload_step_logs`]; on success the caller
    /// finalizes with [`Self::create_job_logs_metadata`].
    pub async fn upload_job_logs(
        &self,
        job_token: &str,
        metadata: &serde_json::Value,
        content: Vec<u8>,
    ) -> Result<(), UploadError> {
        let blob = logical_blob_id("job logs", metadata);
        let url = self
            .get_job_logs_signed_url(job_token, metadata, &blob)
            .await?;
        self.upload_blob_with_refresh(job_token, url, content, &blob)
            .await
    }

    /// Upload a diagnostic-log zip (GetJobDiagLogsSignedBlobURL → PUT blob).
    ///
    /// Same contract as [`Self::upload_step_logs`]; there is no metadata
    /// finalization for diagnostic logs.
    pub async fn upload_diag_logs(
        &self,
        job_token: &str,
        metadata: &serde_json::Value,
        content: Vec<u8>,
    ) -> Result<(), UploadError> {
        let blob = logical_blob_id("diagnostic logs", metadata);
        let url = self
            .get_diagnostic_logs_signed_url(job_token, metadata, &blob)
            .await?;
        self.upload_blob_with_refresh(job_token, url, content, &blob)
            .await
    }

    // ── Signed URL minters: private — the URL is a credential ─

    /// Get a signed blob URL for step logs.
    async fn get_step_logs_signed_url(
        &self,
        token: &str,
        body: &serde_json::Value,
        blob: &str,
    ) -> Result<UploadUrl, UploadError> {
        self.mint_upload_url("GetStepLogsSignedBlobURL", "logs_url", token, body, blob)
            .await
    }

    /// Get a signed blob URL for job logs.
    async fn get_job_logs_signed_url(
        &self,
        token: &str,
        body: &serde_json::Value,
        blob: &str,
    ) -> Result<UploadUrl, UploadError> {
        self.mint_upload_url("GetJobLogsSignedBlobURL", "logs_url", token, body, blob)
            .await
    }

    /// Get a signed blob URL for step summary upload.
    async fn get_step_summary_signed_url(
        &self,
        token: &str,
        body: &serde_json::Value,
        blob: &str,
    ) -> Result<UploadUrl, UploadError> {
        self.mint_upload_url(
            "GetStepSummarySignedBlobURL",
            "summary_url",
            token,
            body,
            blob,
        )
        .await
    }

    /// Get a signed blob URL for diagnostic log upload.
    async fn get_diagnostic_logs_signed_url(
        &self,
        token: &str,
        body: &serde_json::Value,
        blob: &str,
    ) -> Result<UploadUrl, UploadError> {
        self.mint_upload_url(
            "GetJobDiagLogsSignedBlobURL",
            "diag_logs_url",
            token,
            body,
            blob,
        )
        .await
    }

    /// Mint a signed blob upload URL from the receiver service.
    ///
    /// `response_field` is the official response field holding the URL
    /// (`logs_url`, `summary_url`, `diag_logs_url`); legacy aliases are
    /// ignored. A missing or empty field fails the mint.
    async fn mint_upload_url(
        &self,
        endpoint: &str,
        response_field: &str,
        token: &str,
        body: &serde_json::Value,
        blob: &str,
    ) -> Result<UploadUrl, UploadError> {
        let url = format!(
            "{}/twirp/results.services.receiver.Receiver/{endpoint}",
            self.base_url
        );
        let response: serde_json::Value = self
            .http
            .post_json_bearer(&url, body, token)
            .await
            .map_err(|error| UploadError::at(blob, UploadStage::Mint, error))?;
        response
            .get(response_field)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(|value| UploadUrl(value.to_owned()))
            .ok_or_else(|| {
                UploadError::at(
                    blob,
                    UploadStage::Mint,
                    anyhow::anyhow!("signed blob URL response missing {response_field}"),
                )
            })
    }

    /// Upload bytes to a `/twirp-blob` signed upload URL, transparently
    /// refreshing the URL when the server reports it expired.
    ///
    /// If the PUT fails with 401/403, the URL is presumed expired: the runner
    /// asks the server for a fresh upload JWT for the same staging directory
    /// (which requires the job to still be live) and retries, up to
    /// `MAX_BLOB_UPLOAD_REFRESHES` times. Any other failure — or a refresh
    /// refusal, e.g. because the job settled mid-upload — fails the upload:
    /// the session never extends past job death.
    async fn upload_blob_with_refresh(
        &self,
        job_token: &str,
        signed_url: UploadUrl,
        content: Vec<u8>,
        blob: &str,
    ) -> Result<(), UploadError> {
        const MAX_BLOB_UPLOAD_REFRESHES: u32 = 2;
        let mut url = signed_url;
        let mut refreshes = 0u32;
        loop {
            match self
                .http
                .put_bytes(&url.0, content.clone(), "application/octet-stream")
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let auth_rejected =
                        error.downcast_ref::<HttpError>().is_some_and(|http_error| {
                            matches!(
                                http_error,
                                HttpError::Status { status, .. }
                                    if *status == reqwest::StatusCode::UNAUTHORIZED
                                        || *status == reqwest::StatusCode::FORBIDDEN
                            )
                        });
                    if auth_rejected
                        && url.0.contains("/twirp-blob/")
                        && refreshes < MAX_BLOB_UPLOAD_REFRESHES
                    {
                        refreshes += 1;
                        tracing::info!(
                            "blob upload URL rejected; refreshing ({refreshes}/{MAX_BLOB_UPLOAD_REFRESHES})"
                        );
                        url = self.refresh_blob_upload_url(job_token, &url, blob).await?;
                        continue;
                    }
                    return Err(UploadError::at(blob, UploadStage::Put, error));
                }
            }
        }
    }

    /// Refresh an expired blob upload URL.
    ///
    /// POSTs the expired upload JWT to the server's refresh endpoint with the
    /// job bearer. The server mints a fresh upload JWT for the same staging
    /// directory iff the pending upload exists, belongs to the calling job,
    /// and the job is still live. Returns the fresh signed upload URL; a
    /// refusal fails the upload: the session never extends past job death.
    async fn refresh_blob_upload_url(
        &self,
        job_token: &str,
        expired_upload_url: &UploadUrl,
        blob: &str,
    ) -> Result<UploadUrl, UploadError> {
        let token = blob_token_from_upload_url(&expired_upload_url.0).ok_or_else(|| {
            UploadError::at(
                blob,
                UploadStage::Refresh,
                anyhow::anyhow!("not a /twirp-blob upload URL"),
            )
        })?;
        let url = format!(
            "{}/twirp/github.actions.results.api.v1.BlobService/RefreshUploadURL",
            self.base_url
        );
        let response: serde_json::Value = self
            .http
            .post_json_bearer(&url, &serde_json::json!({ "token": token }), job_token)
            .await
            .map_err(|error| UploadError::at(blob, UploadStage::Refresh, error))?;
        response
            .get("signed_upload_url")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(|value| UploadUrl(value.to_owned()))
            .ok_or_else(|| {
                UploadError::at(
                    blob,
                    UploadStage::Refresh,
                    anyhow::anyhow!("refresh response missing signed_upload_url"),
                )
            })
    }

    /// Finalize step summary metadata after blob upload.
    pub async fn create_step_summary_metadata(
        &self,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!(
            "{}/twirp/results.services.receiver.Receiver/CreateStepSummaryMetadata",
            self.base_url
        );
        self.http
            .post_json_bearer(&url, body, token)
            .await
            .context("creating step summary metadata")
    }

    /// Finalize step log metadata after blob upload.
    pub async fn create_step_logs_metadata(
        &self,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!(
            "{}/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
            self.base_url
        );
        self.http
            .post_json_bearer(&url, body, token)
            .await
            .context("creating step logs metadata")
    }

    /// Finalize job log metadata after blob upload.
    pub async fn create_job_logs_metadata(
        &self,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!(
            "{}/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata",
            self.base_url
        );
        self.http
            .post_json_bearer(&url, body, token)
            .await
            .context("creating job logs metadata")
    }
}

/// Logical identity of an upload target, for receipts and error reporting.
/// Adopts the step when the mint metadata names one; never contains a URL.
fn logical_blob_id(kind: &str, metadata: &serde_json::Value) -> String {
    match metadata
        .get("step_backend_id")
        .and_then(serde_json::Value::as_str)
    {
        Some(step) => format!("{kind} for step {step}"),
        None => kind.to_owned(),
    }
}

/// Extract the blob JWT from a `/twirp-blob/{kind}/{token}` upload URL.
///
/// Returns the raw token segment (query parameters stripped), or `None`
/// when the URL is not a twirp-blob URL.
fn blob_token_from_upload_url(url: &str) -> Option<&str> {
    let after = url.split("/twirp-blob/").nth(1)?;
    let path = after.split('?').next()?;
    path.split('/').nth(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn new_trims_trailing_slashes_from_base_url() {
        let http = HttpClient::new(None).unwrap();
        let client = ResultsClient::new(http, "https://results.example.com/".to_string());

        assert_eq!(client.base_url, "https://results.example.com");
    }

    #[test]
    fn update_workflow_steps_endpoint_path_matches_twirp_shape() {
        let http = HttpClient::new(None).unwrap();
        let client = ResultsClient::new(http, "https://results.example.com".to_string());

        let url = format!(
            "{}/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
            client.base_url
        );
        assert_eq!(
            url,
            "https://results.example.com/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate"
        );
    }

    #[test]
    fn blob_token_from_upload_url_extracts_token() {
        assert_eq!(
            blob_token_from_upload_url(
                "https://runner:8443/twirp-blob/artifact/eyJhbGciOiJIUzI1NiJ9.payload.sig"
            ),
            Some("eyJhbGciOiJIUzI1NiJ9.payload.sig")
        );
        // Azure-compat query parameters are stripped.
        assert_eq!(
            blob_token_from_upload_url(
                "https://runner:8443/twirp-blob/diag/abc.def.ghi?sv=2021-08-06&sig=dummy"
            ),
            Some("abc.def.ghi")
        );
        // Replay-ticket URLs are not blob URLs: no refresh applies.
        assert_eq!(
            blob_token_from_upload_url("https://runner:8443/replay/results/logs/ticket123"),
            None
        );
        assert_eq!(blob_token_from_upload_url("not-a-url"), None);
    }

    #[test]
    fn refresh_endpoint_path_matches_twirp_shape() {
        let http = HttpClient::new(None).unwrap();
        let client = ResultsClient::new(http, "https://results.example.com".to_string());

        let url = format!(
            "{}/twirp/github.actions.results.api.v1.BlobService/RefreshUploadURL",
            client.base_url
        );
        assert_eq!(
            url,
            "https://results.example.com/twirp/github.actions.results.api.v1.BlobService/RefreshUploadURL"
        );
    }

    #[test]
    fn signed_blob_endpoint_paths_match_receiver_service() {
        let http = HttpClient::new(None).unwrap();
        let client = ResultsClient::new(http, "https://results.example.com".to_string());

        let step = format!(
            "{}/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
            client.base_url
        );
        let job = format!(
            "{}/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
            client.base_url
        );
        let summary = format!(
            "{}/twirp/results.services.receiver.Receiver/GetStepSummarySignedBlobURL",
            client.base_url
        );
        let diagnostics = format!(
            "{}/twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL",
            client.base_url
        );

        assert!(step.ends_with("results.services.receiver.Receiver/GetStepLogsSignedBlobURL"));
        assert!(job.ends_with("results.services.receiver.Receiver/GetJobLogsSignedBlobURL"));
        assert!(
            summary.ends_with("results.services.receiver.Receiver/GetStepSummarySignedBlobURL")
        );
        assert!(
            diagnostics.ends_with("results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL")
        );
    }

    // ── URL-leak hardening ─────────────────────────────────────

    #[test]
    fn upload_url_debug_is_exactly_redacted() {
        let url = UploadUrl(
            "https://runner:8443/twirp-blob/artifact/eyJhbGciOiJIUzI1NiJ9.payload.sig".to_string(),
        );
        assert_eq!(format!("{url:?}"), "UploadUrl(<redacted>)");
    }

    #[test]
    fn upload_error_display_never_renders_the_url() {
        let url = "https://runner:8443/twirp-blob/artifact/eyJhbGciOiJIUzI1NiJ9.payload.sig";

        // Client errors embed the request URL in their message (e.g.
        // `PUT {url}: {e}`); mapping drops the message entirely.
        let transport = UploadError::at(
            "step logs for step 7",
            UploadStage::Put,
            anyhow::anyhow!("PUT {url}: error sending request"),
        );

        // Even a typed HTTP error whose server body echoes the URL stays
        // silent: the body is dropped along with the source.
        let rejected = UploadError::at(
            "step logs for step 7",
            UploadStage::Put,
            anyhow::Error::new(HttpError::Status {
                status: reqwest::StatusCode::BAD_GATEWAY,
                body: format!("mirror: {url}"),
            }),
        );
        assert_eq!(rejected.status, Some(reqwest::StatusCode::BAD_GATEWAY));

        // An anyhow chain from these paths prints every source — with none
        // attached, no rendering step can see the URL.
        let chain = anyhow::Error::new(UploadError::at(
            "job logs",
            UploadStage::Refresh,
            anyhow::anyhow!("POST {url}: {url}"),
        ));

        let rendered = [
            transport.to_string(),
            format!("{transport:?}"),
            rejected.to_string(),
            format!("{rejected:?}"),
            format!("{chain:#}"),
        ];
        for output in rendered {
            assert!(!output.contains(url), "{output}");
            assert!(!output.contains("twirp-blob"), "{output}");
        }
    }

    // ── Fake-server upload flows ───────────────────────────────

    /// Read one HTTP request (head + `content-length` body) from the socket.
    async fn read_http_request(socket: &mut tokio::net::TcpStream) -> String {
        let mut buf = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            let n = socket.read(&mut chunk).await.unwrap();
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            let Some(head_end) = buf.windows(4).position(|window| window == b"\r\n\r\n") else {
                continue;
            };
            let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
            let content_length = head
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            if buf.len() >= head_end + 4 + content_length {
                break;
            }
        }
        String::from_utf8_lossy(&buf).into_owned()
    }

    /// Scripted HTTP/1.1 server for blob-upload flow tests.
    ///
    /// Serves one `(status, body)` response per accepted connection
    /// (`connection: close`) and records each request (head + body).
    /// `{BASE}` in a body is replaced with the server's base URL so minted
    /// upload URLs point back here.
    async fn serve_scripted(responses: Vec<(u16, String)>) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base = format!("http://{addr}");
        let requests: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let serve_base = base.clone();
        tokio::spawn(async move {
            for (status, body_template) in responses {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let request = read_http_request(&mut socket).await;
                recorded.lock().unwrap().push(request);
                let body = body_template.replace("{BASE}", &serve_base);
                let reason = match status {
                    200 => "OK",
                    401 => "Unauthorized",
                    403 => "Forbidden",
                    404 => "Not Found",
                    _ => "Status",
                };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (base, requests)
    }

    #[tokio::test]
    async fn expired_upload_url_is_refreshed_and_retried() {
        let (base, requests) = serve_scripted(vec![
            (
                200,
                r#"{"logs_url":"{BASE}/twirp-blob/artifact/expired.jwt.here"}"#.to_string(),
            ),
            (403, "expired".to_string()),
            (
                200,
                r#"{"signed_upload_url":"{BASE}/twirp-blob/artifact/fresh.jwt.here"}"#.to_string(),
            ),
            (200, String::new()),
        ])
        .await;
        let client = ResultsClient::new(HttpClient::new(None).unwrap(), base);
        let metadata = serde_json::json!({
            "workflow_job_run_backend_id": "job-1",
            "workflow_run_backend_id": "plan-1",
            "step_backend_id": "step-7",
        });

        client
            .upload_step_logs("job-token", &metadata, b"hello".to_vec())
            .await
            .expect("upload succeeds after refresh");

        let requests = requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 4);
        assert!(requests[0].starts_with(
            "POST /twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL HTTP/1.1"
        ));
        assert!(requests[0].contains("step-7"));
        assert!(requests[1].starts_with("PUT /twirp-blob/artifact/expired.jwt.here HTTP/1.1"));
        // RefreshUploadURL accepts the expired JWT and remints for the job.
        assert!(requests[2].starts_with(
            "POST /twirp/github.actions.results.api.v1.BlobService/RefreshUploadURL HTTP/1.1"
        ));
        assert!(requests[2].contains(r#""token":"expired.jwt.here""#));
        assert!(requests[3].starts_with("PUT /twirp-blob/artifact/fresh.jwt.here HTTP/1.1"));
        assert!(
            requests[3]
                .to_ascii_lowercase()
                .contains("content-type: application/octet-stream")
        );
        assert!(requests[3].ends_with("hello"));
    }

    #[tokio::test]
    async fn refused_refresh_fails_the_upload() {
        let (base, requests) = serve_scripted(vec![
            (
                200,
                r#"{"logs_url":"{BASE}/twirp-blob/artifact/expired.jwt.here"}"#.to_string(),
            ),
            (401, "expired".to_string()),
            (
                404,
                r#"{"code":"not_found","msg":"job settled"}"#.to_string(),
            ),
        ])
        .await;
        let client = ResultsClient::new(HttpClient::new(None).unwrap(), base);
        let metadata = serde_json::json!({
            "workflow_job_run_backend_id": "job-1",
            "workflow_run_backend_id": "plan-1",
            "step_backend_id": "step-7",
        });

        let error = client
            .upload_step_logs("job-token", &metadata, b"hello".to_vec())
            .await
            .unwrap_err();

        assert_eq!(error.stage, UploadStage::Refresh);
        assert_eq!(error.status, Some(reqwest::StatusCode::NOT_FOUND));
        // The session never extends past job death: the refusal fails the
        // upload and no further PUT is attempted.
        let requests = requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 3);
        assert!(requests[1].starts_with("PUT /twirp-blob/artifact/expired.jwt.here HTTP/1.1"));
        assert!(requests[2].starts_with(
            "POST /twirp/github.actions.results.api.v1.BlobService/RefreshUploadURL HTTP/1.1"
        ));
    }

    #[tokio::test]
    async fn upload_fails_after_two_refreshes_are_exhausted() {
        // The refresh budget is capped at two: a URL the server keeps
        // rejecting fails the upload instead of reminting forever.
        let (base, requests) = serve_scripted(vec![
            (
                200,
                r#"{"logs_url":"{BASE}/twirp-blob/artifact/expired.tok"}"#.to_string(),
            ),
            (403, "expired".to_string()),
            (
                200,
                r#"{"signed_upload_url":"{BASE}/twirp-blob/artifact/fresh-one.tok"}"#.to_string(),
            ),
            (403, "expired".to_string()),
            (
                200,
                r#"{"signed_upload_url":"{BASE}/twirp-blob/artifact/fresh-two.tok"}"#.to_string(),
            ),
            (403, "expired".to_string()),
        ])
        .await;
        let client = ResultsClient::new(HttpClient::new(None).unwrap(), base);
        let metadata = serde_json::json!({
            "workflow_job_run_backend_id": "job-1",
            "workflow_run_backend_id": "plan-1",
            "step_backend_id": "step-7",
        });

        let error = client
            .upload_step_logs("job-token", &metadata, b"hello".to_vec())
            .await
            .unwrap_err();

        assert_eq!(error.stage, UploadStage::Put);
        assert_eq!(error.status, Some(reqwest::StatusCode::FORBIDDEN));
        let requests = requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 6);
        // Each refresh remints from the currently-held URL's token.
        assert!(requests[2].contains(r#""token":"expired.tok""#));
        assert!(requests[4].contains(r#""token":"fresh-one.tok""#));
        assert!(requests[5].starts_with("PUT /twirp-blob/artifact/fresh-two.tok HTTP/1.1"));
    }

    #[tokio::test]
    async fn upload_diag_logs_uses_official_endpoint_and_response_field() {
        let (base, requests) = serve_scripted(vec![
            (
                200,
                r#"{"diag_logs_url":"{BASE}/twirp-blob/diag/abc.def.ghi"}"#.to_string(),
            ),
            (200, String::new()),
        ])
        .await;
        let client = ResultsClient::new(HttpClient::new(None).unwrap(), base);
        let metadata = serde_json::json!({
            "workflow_run_backend_id": "plan-1",
            "workflow_job_run_backend_id": "job-1",
        });

        client
            .upload_diag_logs("test-token", &metadata, b"zip".to_vec())
            .await
            .expect("diagnostic logs upload");

        let requests = requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].starts_with(
            "POST /twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL HTTP/1.1"
        ));
        assert!(requests[1].starts_with("PUT /twirp-blob/diag/abc.def.ghi HTTP/1.1"));
    }

    #[tokio::test]
    async fn upload_diag_logs_ignores_legacy_response_fields() {
        // Only the official `diag_logs_url` is honored: legacy aliases must
        // not yield an upload URL, so the upload fails at the mint stage and
        // no PUT follows.
        let (base, requests) = serve_scripted(vec![(
            200,
            r#"{"logs_url":"{BASE}/twirp-blob/diag/legacy"}"#.to_string(),
        )])
        .await;
        let client = ResultsClient::new(HttpClient::new(None).unwrap(), base);
        let metadata = serde_json::json!({
            "workflow_run_backend_id": "plan-1",
            "workflow_job_run_backend_id": "job-1",
        });

        let error = client
            .upload_diag_logs("test-token", &metadata, b"zip".to_vec())
            .await
            .unwrap_err();

        assert_eq!(error.stage, UploadStage::Mint);
        assert_eq!(requests.lock().unwrap().len(), 1);
    }
}
