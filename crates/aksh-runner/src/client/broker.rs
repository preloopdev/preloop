//! Broker API client (GitHub-current path).
//!
//! Handles broker session management and message polling via the
//! `/runner/` and `/broker/` endpoints.

use anyhow::{Context, Result};
use std::time::Duration;

use super::http::{HttpClient, HttpError};

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
    pub async fn delete_session(&self, token: &str, session_id: &str) -> Result<()> {
        let url = format!("{}/session?sessionId={session_id}", self.base_url);
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
        // Official runner polls every ~3s when busy (for cancellation detection),
        // and uses a long ~50s poll when idle (waiting for a job).
        let timeout = if busy {
            Duration::from_secs(3)
        } else {
            Duration::from_secs(50)
        };
        self.http
            .get_long_poll(&url, &format!("Bearer {token}"), timeout)
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

/// Detect the official runner-version deprecation response.
///
/// Runner.Listener receives this as `AccessDeniedException` with
/// `errorCode: 1` from the message endpoint. Keep the check narrow so normal
/// authorization failures continue through the retry/reconnect path.
pub fn is_runner_version_deprecated(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let Some(HttpError::Status { status, body }) = cause.downcast_ref::<HttpError>() else {
            return false;
        };
        *status == reqwest::StatusCode::FORBIDDEN
            && serde_json::from_str::<serde_json::Value>(body)
                .ok()
                .and_then(|value| value.get("errorCode").and_then(serde_json::Value::as_i64))
                == Some(1)
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_access_denied_error_code_one() {
        let error = anyhow::Error::new(HttpError::Status {
            status: reqwest::StatusCode::FORBIDDEN,
            body: r#"{"typeKey":"AccessDeniedException","errorCode":1}"#.to_owned(),
        })
        .context("polling broker message");
        assert!(is_runner_version_deprecated(&error));
    }

    #[test]
    fn does_not_classify_other_forbidden_responses() {
        let error = anyhow::Error::new(HttpError::Status {
            status: reqwest::StatusCode::FORBIDDEN,
            body: r#"{"typeKey":"AccessDeniedException","errorCode":0}"#.to_owned(),
        });
        assert!(!is_runner_version_deprecated(&error));
    }
}
