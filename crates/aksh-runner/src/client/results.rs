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
        Self { http, base_url }
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

    /// Upload log content to a signed blob URL.
    pub async fn upload_log_blob(&self, signed_url: &str, content: Vec<u8>) -> Result<()> {
        self.http
            .put_bytes(signed_url, content, "application/octet-stream")
            .await
            .context("uploading log blob")
    }
}
