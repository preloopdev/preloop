use serde::{Deserialize, Serialize};

// ─── Variable and masking DTOs ────────────────────────────────────────────

/// A variable value with optional secret flag.
///
/// Variables are sent to the runner as `VariableValue` objects.
/// The runner uses `isSecret` to decide whether to mask the value in logs.
///
/// Upstream source: `VariableValue.cs`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableValue {
    #[serde(rename = "value", skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(rename = "isSecret", skip_serializing_if = "Option::is_none")]
    pub is_secret: Option<bool>,
}

impl VariableValue {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: Some(value.into()),
            is_secret: None,
        }
    }

    pub fn secret(value: impl Into<String>) -> Self {
        Self {
            value: Some(value.into()),
            is_secret: Some(true),
        }
    }
}

/// A masking hint — tells the runner to redact a value in log output.
///
/// The runner applies these hints when writing to the log feed.
///
/// Upstream source: `MaskHint.cs`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaskHint {
    #[serde(rename = "type")]
    pub hint_type: MaskType,
    #[serde(rename = "value")]
    pub value: String,
}

/// Type of masking hint.
///
/// Upstream source: `MaskType.cs` — `Variable = 1`, `Regex = 2`. The official
/// worker only acts on `Regex` (`Worker.cs` InitializeSecretMasker); values are
/// serialized as camelCase strings on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MaskType {
    /// Mask the value of a named variable.
    Variable,
    /// Mask everything matching a regular expression.
    Regex,
}
