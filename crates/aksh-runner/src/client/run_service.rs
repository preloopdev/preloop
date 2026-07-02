//! Run-service client (broker job acquisition/renewal/completion).

use anyhow::{Context, Result};

use super::http::HttpClient;

/// Client for run-service endpoints.
pub struct RunServiceClient {
    http: HttpClient,
    base_url: String,
}

impl RunServiceClient {
    /// Create a new run-service client.
    pub fn new(http: HttpClient, base_url: String) -> Self {
        Self {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Return the base URL (for constructing endpoint paths in job_runner).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Acquire a job (POST acquirejob).
    pub async fn acquire_job(
        &self,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!("{}/acquirejob", self.base_url);
        self.http
            .post_json_bearer(&url, body, token)
            .await
            .context("acquiring job")
    }

    /// Renew a job lock (POST renewjob).
    pub async fn renew_job(
        &self,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!("{}/renewjob", self.base_url);
        self.http
            .post_json_bearer(&url, body, token)
            .await
            .context("renewing job")
    }

    /// Complete a job (POST completejob).
    pub async fn complete_job(
        &self,
        token: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!("{}/completejob", self.base_url);
        self.http
            .post_json_bearer(&url, body, token)
            .await
            .context("completing job")
    }
}
