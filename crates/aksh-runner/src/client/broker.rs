//! Broker API client (GitHub-current path).
//!
//! Handles broker session management and message polling via the
//! `/runner/` and `/broker/` endpoints.

use anyhow::{Context, Result};
use std::time::Duration;

use super::http::HttpClient;

/// Client for the broker endpoints (GitHub-current path).
pub struct BrokerClient {
    http: HttpClient,
    base_url: String,
}

impl BrokerClient {
    /// Create a new broker client.
    pub fn new(http: HttpClient, base_url: String) -> Self {
        Self { http, base_url }
    }

    /// Create a broker session.
    pub async fn create_session(
        &self,
        token: &str,
        session: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!("{}/session", self.base_url);
        self.http
            .post_json_bearer(&url, session, token)
            .await
            .context("creating broker session")
    }

    /// Delete a broker session.
    pub async fn delete_session(&self, token: &str, _session_id: &str) -> Result<()> {
        let url = format!("{}/session", self.base_url);
        self.http
            .delete_with_token(&url, token)
            .await
            .context("deleting broker session")
    }

    /// Long-poll for a message from the broker.
    ///
    /// Query params match golden exactly:
    /// `sessionId, status, runnerVersion, os, architecture, disableUpdate=false`
    /// No `lastMessageId` — the golden flows never include it.
    pub async fn get_message(
        &self,
        token: &str,
        session_id: &str,
        busy: bool,
    ) -> Result<Option<serde_json::Value>> {
        let status = if busy { "Busy" } else { "Online" };
        let url = format!(
            "{}/message?sessionId={session_id}&status={status}&runnerVersion={}&os={}&architecture={}&disableUpdate=false",
            self.base_url,
            crate::PROTOCOL_COMPAT_VERSION,
            os_label(),
            arch_label(),
        );
        self.http
            .get_long_poll(&url, &format!("Bearer {token}"), Duration::from_secs(50))
            .await
            .context("polling broker message")
    }

    /// Acknowledge a message (POST, matching official runner).
    ///
    /// Golden flow 13: POST /acknowledge?sessionId=X&status=Online&runnerVersion=...&os=...&architecture=...
    /// Body: `{"runnerRequestId": "<job_message_id>"}`
    /// No `disableUpdate` or `messageId` in the query string.
    pub async fn acknowledge(
        &self,
        token: &str,
        session_id: &str,
        runner_request_id: &str,
    ) -> Result<()> {
        let url = format!(
            "{}/acknowledge?sessionId={session_id}&status=Online&runnerVersion={}&os={}&architecture={}",
            self.base_url,
            crate::PROTOCOL_COMPAT_VERSION,
            os_label(),
            arch_label(),
        );
        let body = serde_json::json!({"runnerRequestId": runner_request_id});
        let _: serde_json::Value = self
            .http
            .post_json_bearer(&url, &body, token)
            .await
            .context("acknowledging broker message")?;
        Ok(())
    }
}

fn os_label() -> &'static str {
    if cfg!(target_os = "macos") {
        "macOS"
    } else {
        "Linux"
    }
}

fn arch_label() -> &'static str {
    if cfg!(target_arch = "aarch64") {
        "ARM64"
    } else {
        "X64"
    }
}
