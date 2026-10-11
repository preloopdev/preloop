//! Results service client (Twirp step updates and log uploads).

use anyhow::{Context, Result};

use super::http::HttpClient;

/// Client for the results service (Twirp/proto endpoints).
pub struct ResultsClient {
    http: HttpClient,
    base_url: String,
}

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

    /// Get a signed blob URL for step logs.
    pub async fn get_step_logs_signed_url(
        &self,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!(
            "{}/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
            self.base_url
        );
        self.http
            .post_json_bearer(&url, body, token)
            .await
            .context("getting step logs signed URL")
    }

    /// Get a signed blob URL for job logs.
    pub async fn get_job_logs_signed_url(
        &self,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!(
            "{}/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
            self.base_url
        );
        self.http
            .post_json_bearer(&url, body, token)
            .await
            .context("getting job logs signed URL")
    }

    /// Get a signed blob URL for step summary upload.
    pub async fn get_step_summary_signed_url(
        &self,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!(
            "{}/twirp/results.services.receiver.Receiver/GetStepSummarySignedBlobURL",
            self.base_url
        );
        self.http
            .post_json_bearer(&url, body, token)
            .await
            .context("getting step summary signed URL")
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

    /// Get a signed blob URL for diagnostic log upload.
    pub async fn get_diagnostic_logs_signed_url(
        &self,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!(
            "{}/twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL",
            self.base_url
        );
        self.http
            .post_json_bearer(&url, body, token)
            .await
            .context("getting diagnostic logs signed URL")
    }

    /// Upload log content to a signed blob URL.
    pub async fn upload_log_blob(&self, signed_url: &str, content: Vec<u8>) -> Result<()> {
        self.http
            .put_bytes(signed_url, content, "application/octet-stream")
            .await
            .context("uploading log blob")
    }

    /// Refresh an expired blob upload URL.
    ///
    /// POSTs the expired upload JWT to the server's refresh endpoint with the
    /// job bearer. The server mints a fresh upload JWT for the same staging
    /// directory iff the pending upload exists, belongs to the calling job,
    /// and the job is still live. Returns the fresh signed upload URL.
    pub async fn refresh_blob_upload_url(
        &self,
        job_token: &str,
        expired_upload_url: &str,
    ) -> Result<String> {
        let token = blob_token_from_upload_url(expired_upload_url)
            .ok_or_else(|| anyhow::anyhow!("not a /twirp-blob upload URL"))?;
        let url = format!(
            "{}/twirp/github.actions.results.api.v1.BlobService/RefreshUploadURL",
            self.base_url
        );
        let response: serde_json::Value = self
            .http
            .post_json_bearer(&url, &serde_json::json!({ "token": token }), job_token)
            .await
            .context("refreshing blob upload URL")?;
        response
            .get("signed_upload_url")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .ok_or_else(|| anyhow::anyhow!("refresh response missing signed_upload_url"))
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
    pub async fn upload_blob_with_refresh(
        &self,
        job_token: &str,
        signed_url: &str,
        content: Vec<u8>,
    ) -> Result<()> {
        const MAX_BLOB_UPLOAD_REFRESHES: u32 = 2;
        let mut url = signed_url.to_owned();
        let mut refreshes = 0u32;
        loop {
            match self
                .http
                .put_bytes(&url, content.clone(), "application/octet-stream")
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let auth_rejected =
                        error
                            .downcast_ref::<super::http::HttpError>()
                            .is_some_and(|http_error| {
                                matches!(
                                    http_error,
                                    super::http::HttpError::Status { status, .. }
                                        if *status == reqwest::StatusCode::UNAUTHORIZED
                                            || *status == reqwest::StatusCode::FORBIDDEN
                                )
                            });
                    if auth_rejected
                        && url.contains("/twirp-blob/")
                        && refreshes < MAX_BLOB_UPLOAD_REFRESHES
                    {
                        refreshes += 1;
                        tracing::info!(
                            "blob upload URL rejected; refreshing ({refreshes}/{MAX_BLOB_UPLOAD_REFRESHES})"
                        );
                        url = self
                            .refresh_blob_upload_url(job_token, &url)
                            .await
                            .context("refreshing expired blob upload URL")?;
                        continue;
                    }
                    return Err(error).context("uploading blob");
                }
            }
        }
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
}
