//! AzDO distributedtask API client (legacy message queue path).
//!
//! Handles session creation/deletion and message polling via the
//! `_apis/v1/` distributedtask endpoints.

use anyhow::{Context, Result};
use std::time::Duration;

use super::http::HttpClient;

/// Client for the AzDO distributedtask endpoints.
pub struct AzdoClient {
    http: HttpClient,
    base_url: String,
    pool_id: i64,
}

impl AzdoClient {
    /// Create a new AzDO client.
    pub fn new(http: HttpClient, base_url: String, pool_id: i64) -> Self {
        Self {
            http,
            base_url,
            pool_id,
        }
    }

    /// Create a session with the server.
    pub async fn create_session(
        &self,
        token: &str,
        session: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!(
            "{}/_apis/distributedtask/pools/{}/sessions",
            self.base_url, self.pool_id
        );
        self.http
            .post_json_bearer(&url, session, token)
            .await
            .context("creating session")
    }

    /// Delete (end) a session.
    pub async fn delete_session(&self, token: &str, session_id: &str) -> Result<()> {
        let url = format!(
            "{}/_apis/distributedtask/pools/{}/sessions/{}",
            self.base_url, self.pool_id, session_id
        );
        self.http
            .delete_with_token(&url, token)
            .await
            .context("deleting session")
    }

    /// Long-poll for next message.
    pub async fn get_message(
        &self,
        token: &str,
        session_id: &str,
        last_message_id: Option<i64>,
    ) -> Result<Option<serde_json::Value>> {
        let mut url = format!(
            "{}/_apis/distributedtask/pools/{}/messages?sessionId={}&status=online",
            self.base_url, self.pool_id, session_id
        );
        if let Some(id) = last_message_id {
            url.push_str(&format!("&lastMessageId={id}"));
        }
        self.http
            .get_long_poll(&url, &format!("Bearer {token}"), Duration::from_secs(50))
            .await
            .context("polling for message")
    }

    /// Acknowledge (delete) a message.
    pub async fn delete_message(
        &self,
        token: &str,
        session_id: &str,
        message_id: i64,
    ) -> Result<()> {
        let url = format!(
            "{}/_apis/distributedtask/pools/{}/messages/{}?sessionId={}",
            self.base_url, self.pool_id, message_id, session_id
        );
        self.http
            .delete_with_token(&url, token)
            .await
            .context("deleting message")
    }

    /// PATCH agent request (report received/result).
    pub async fn patch_agent_request(
        &self,
        token: &str,
        request_id: i64,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!(
            "{}/_apis/distributedtask/pools/{}/jobrequests/{request_id}",
            self.base_url, self.pool_id
        );
        self.http
            .patch_json_bearer(&url, body, token)
            .await
            .context("patching agent request")
    }

    /// PATCH timeline records.
    pub async fn update_timeline(
        &self,
        token: &str,
        plan_id: &str,
        timeline_id: &str,
        records: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!(
            "{}/_apis/v1/plans/{plan_id}/timelines/{timeline_id}/records",
            self.base_url
        );
        self.http
            .patch_json_bearer(&url, records, token)
            .await
            .context("updating timeline")
    }

    /// POST create a log file.
    pub async fn create_log(
        &self,
        token: &str,
        plan_id: &str,
        log: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!("{}/_apis/v1/plans/{plan_id}/logs", self.base_url);
        self.http
            .post_json_bearer(&url, log, token)
            .await
            .context("creating log")
    }

    /// POST append log lines.
    pub async fn append_log(
        &self,
        token: &str,
        plan_id: &str,
        log_id: i64,
        lines: Vec<u8>,
    ) -> Result<()> {
        let url = format!("{}/_apis/v1/plans/{plan_id}/logs/{log_id}", self.base_url);
        self.http
            .put_bytes(&url, lines, "application/octet-stream")
            .await
            .context("appending log lines")
    }

    /// POST console log lines (live feed).
    pub async fn post_console_log(
        &self,
        token: &str,
        plan_id: &str,
        timeline_id: &str,
        record_id: &str,
        lines: &serde_json::Value,
    ) -> Result<()> {
        let url = format!(
            "{}/_apis/v1/plans/{plan_id}/timelines/{timeline_id}/records/{record_id}/feed",
            self.base_url
        );
        let _: serde_json::Value = self
            .http
            .post_json_bearer(&url, lines, token)
            .await
            .context("posting console log")?;
        Ok(())
    }

    /// POST finish job.
    pub async fn finish_job(
        &self,
        token: &str,
        plan_id: &str,
        event: &serde_json::Value,
    ) -> Result<()> {
        let url = format!("{}/_apis/v1/plans/{plan_id}/events", self.base_url);
        let _: serde_json::Value = self
            .http
            .post_json_bearer(&url, event, token)
            .await
            .context("finishing job")?;
        Ok(())
    }
}
