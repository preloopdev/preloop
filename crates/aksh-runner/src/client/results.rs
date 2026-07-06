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

    /// F054: Get a signed blob URL for diagnostic log upload.
    pub async fn get_diagnostic_logs_signed_url(
        &self,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!(
            "{}/twirp/results.services.receiver.Receiver/CreateResultsDiagnosticLogsSignedBlobURL",
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
            "{}/twirp/results.services.receiver.Receiver/CreateResultsDiagnosticLogsSignedBlobURL",
            client.base_url
        );

        assert!(step.ends_with("results.services.receiver.Receiver/GetStepLogsSignedBlobURL"));
        assert!(job.ends_with("results.services.receiver.Receiver/GetJobLogsSignedBlobURL"));
        assert!(summary.ends_with("results.services.receiver.Receiver/GetStepSummarySignedBlobURL"));
        assert!(diagnostics.ends_with(
            "results.services.receiver.Receiver/CreateResultsDiagnosticLogsSignedBlobURL"
        ));
    }
}
