//! Broker API client (GitHub-current path).
//!
//! Handles broker session management and message polling via the
//! `/runner/` and `/broker/` endpoints.

use anyhow::{Context, Result};
use std::time::Duration;

use super::http::{HttpClient, HttpError};

// The broker holds a message poll open for up to 50 seconds. The client
// deadline must outlast that window, or a job the server hands out at the
// end of the window is written to a request the client already abandoned.
// Official runner: `VssUtil.GetHttpRequestSettings` defaults `SendTimeout`
// to 100 seconds for the broker connection.
const MESSAGE_POLL_TIMEOUT: Duration = Duration::from_secs(100);

/// Client for the broker endpoints (GitHub-current path).
#[derive(Clone)]
pub struct BrokerClient {
    http: HttpClient,
    base_url: String,
}

/// Result from acknowledging a broker message.
#[derive(Debug, PartialEq, Eq)]
pub enum AcknowledgeResult {
    /// Acknowledge succeeded normally.
    Ok,
    /// Broker returned 404 with `AcknowledgeJobNotFound` — the job no longer exists.
    JobNotFound,
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
        let url = format!("{}/session", self.base_url);
        self.http
            .delete_with_token_header(&url, token, "X-Actions-Session", session_id)
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
        let message: Option<serde_json::Value> = self
            .http
            .get_long_poll(&url, &format!("Bearer {token}"), MESSAGE_POLL_TIMEOUT)
            .await
            .context("polling broker message")?;
        // A 200 with a `null` body is an empty poll; the official listener
        // deserializes it to no message and polls again.
        Ok(message.filter(|message| !message.is_null()))
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
    ) -> Result<AcknowledgeResult> {
        let url = format!(
            "{}/acknowledge?sessionId={session_id}&status=Online&runnerVersion={}&os={}&architecture={}",
            self.base_url,
            crate::PROTOCOL_COMPAT_VERSION,
            os_label(),
            arch_label(),
        );
        let body = serde_json::json!({"runnerRequestId": runner_request_id});
        match self
            .http
            .post_json_bearer::<serde_json::Value>(&url, &body, token)
            .await
        {
            Ok(_) => Ok(AcknowledgeResult::Ok),
            Err(e) => {
                // v2.336.0 (#4540): 404 with AcknowledgeJobNotFound means the job
                // no longer exists. Ephemeral runners should exit cleanly.
                if let Some(HttpError::Status { status, body }) = e.downcast_ref::<HttpError>()
                    && *status == reqwest::StatusCode::NOT_FOUND
                    && let Ok(json) = serde_json::from_str::<serde_json::Value>(body)
                    && json.get("errorKind").and_then(|v| v.as_str())
                        == Some("AcknowledgeJobNotFound")
                {
                    return Ok(AcknowledgeResult::JobNotFound);
                }
                Err(e).context("acknowledging broker message")
            }
        }
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
    } else if cfg!(target_os = "windows") {
        "Windows"
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

    #[tokio::test]
    async fn null_poll_body_is_no_message() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let _ = socket.read(&mut request).await.unwrap();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 4\r\nconnection: close\r\n\r\nnull")
                .await
                .unwrap();
        });
        let client = BrokerClient::new(HttpClient::new(None).unwrap(), format!("http://{address}"));

        let message = client.get_message("token", "session", true).await.unwrap();

        assert_eq!(message, None);
    }
}
