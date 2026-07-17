use serde::{Deserialize, Serialize};

// ─── Message queue DTOs ───────────────────────────────────────────────────

/// An encrypted message from the server to the runner.
///
/// The runner long-polls `GET .../messages?sessionId=X&lastMessageId=Y`
/// and receives this. The `body` field is base64-encoded and, if the
/// session has encryption enabled, must be AES-decrypted using the
/// session's `encryptionKey` and the `iv` field.
///
/// Upstream source: `MessageController.cs` (server) and
/// `MessageListener.cs` (runner)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAgentMessage {
    #[serde(rename = "messageId")]
    pub message_id: i64,
    #[serde(rename = "messageType")]
    pub message_type: String,
    /// Base64-encoded body. Encrypted if the session uses encryption.
    #[serde(rename = "body")]
    pub body: String,
    /// Base64-encoded initialization vector for AES decryption.
    /// Serialized as a base64 string (matching the official runner wire format).
    /// Present only when the message body is encrypted.
    #[serde(rename = "iv", skip_serializing_if = "Option::is_none")]
    pub iv: Option<String>,
}

/// Known message types the runner handles.
pub mod message_type {
    /// A job request — body contains an encrypted `AgentJobRequestMessage`.
    pub const PIPELINE_AGENT_JOB_REQUEST: &str = "PipelineAgentJobRequest";
    /// A run-service job request returned from the broker `acquirejob` API.
    pub const RUNNER_JOB_REQUEST: &str = "RunnerJobRequest";
    /// Cancellation signal — runner should abort the current job.
    pub const CANCEL_JOB: &str = "CancelJob";
    /// Job cancellation — official `JobCancelMessage.MessageType`.
    ///
    /// Must be exactly `"JobCancellation"` (not `"JobCancelled"`): the
    /// official runner matches this string in `Runner.cs` / broker dispatch.
    pub const JOB_CANCELLED: &str = "JobCancellation";
    /// Runner should shut down gracefully.
    pub const RUNNER_SHUTDOWN: &str = "RunnerShutdown";
}
