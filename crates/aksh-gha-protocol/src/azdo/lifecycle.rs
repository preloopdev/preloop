use serde::{Deserialize, Serialize};

// ─── Runner lifecycle DTOs ────────────────────────────────────────────────
/// Server-enforced runner feature settings.
///
/// The runner may retrieve these defaults from the settings endpoint while it
/// establishes its connection. Unknown settings are intentionally represented
/// by `agent_download_urls` as JSON so the server can evolve that payload
/// without requiring a protocol crate release for every shape change.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunnerServerSettings {
    #[serde(default)]
    pub is_hosted_server: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_download_urls: Option<serde_json::Value>,
}

/// Service location data returned by `GET _apis/connectionData`.
///
/// The runner calls this first to discover which service GUIDs map to
/// which base URLs. The response is a JSON document with `locationServiceData`
/// containing a `serviceDefinitions` array.
///
/// Upstream source: `ConnectionDataController.cs`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionData {
    #[serde(rename = "instanceId", skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    #[serde(
        rename = "locationServiceData",
        skip_serializing_if = "Option::is_none"
    )]
    pub location_service_data: Option<LocationServiceData>,
}

/// Access mapping for location service resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessMapping {
    #[serde(rename = "moniker", skip_serializing_if = "Option::is_none")]
    pub moniker: Option<String>,
    #[serde(rename = "displayName", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "accessPoint", skip_serializing_if = "Option::is_none")]
    pub access_point: Option<String>,
}

/// Location service data — maps service GUIDs to URL locations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationServiceData {
    #[serde(
        rename = "serviceDefinitions",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub service_definitions: Vec<ServiceDefinition>,
    #[serde(
        rename = "accessMappings",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub access_mappings: Vec<AccessMapping>,
    #[serde(
        rename = "defaultAccessMappingMoniker",
        skip_serializing_if = "Option::is_none"
    )]
    pub default_access_mapping_moniker: Option<String>,
}

/// A location mapping for a service definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationMapping {
    #[serde(
        rename = "accessMappingMoniker",
        skip_serializing_if = "Option::is_none"
    )]
    pub access_mapping_moniker: Option<String>,
    #[serde(rename = "location", skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
}

/// A single service definition mapping a GUID to a URL location.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceDefinition {
    #[serde(rename = "serviceType", skip_serializing_if = "Option::is_none")]
    pub service_type: Option<String>,
    #[serde(rename = "identifier", skip_serializing_if = "Option::is_none")]
    pub identifier: Option<String>,
    #[serde(rename = "displayName", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "relativePath", skip_serializing_if = "Option::is_none")]
    pub relative_path: Option<String>,
    #[serde(rename = "description", skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "toolId", skip_serializing_if = "Option::is_none")]
    pub tool_id: Option<String>,
    #[serde(rename = "locationMappings", skip_serializing_if = "Option::is_none")]
    pub location_mappings: Option<Vec<LocationMapping>>,
}

/// Runner agent registration request.
///
/// The runner sends its RSA public key during registration.
/// Upstream source: `AgentController.cs`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAgent {
    #[serde(rename = "id", skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "version", skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(rename = "osDescription", skip_serializing_if = "Option::is_none")]
    pub os_description: Option<String>,
}

/// Encryption key for a session.
///
/// If `encrypted` is true, the `value` is RSA-OAEP wrapped and must be
/// decrypted with the runner's private key before use as an AES key.
///
/// Upstream wire contract:
/// <https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Sdk/DTWebApi/WebApi/TaskAgentSessionKey.cs#L8-L32>
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionKey {
    /// The raw or wrapped key bytes, encoded as a JSON base64 string by
    /// `byte[]` in the official runner DTO.
    #[serde(rename = "value", with = "base64_bytes")]
    pub value: Vec<u8>,
    /// Whether this key is RSA-wrapped (true) or plaintext (false).
    #[serde(rename = "encrypted")]
    pub encrypted: bool,
}

mod base64_bytes {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(value))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(serde::de::Error::custom)
    }
}

/// Agent session creation response.
///
/// Returned after `POST .../pools/{poolId}/sessions`. Contains the
/// AES encryption key (possibly RSA-wrapped) that the runner uses to
/// decrypt all subsequent `TaskAgentMessage` bodies.
///
/// Upstream source: `AgentSessionController.cs`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAgentSession {
    #[serde(rename = "sessionId")]
    pub session_id: uuid::Uuid,
    #[serde(rename = "encryptionKey")]
    pub encryption_key: EncryptionKey,
}

/// Runner session creation request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSessionRequest {
    #[serde(rename = "agent")]
    pub agent: TaskAgent,
    #[serde(rename = "sessionName", skip_serializing_if = "Option::is_none")]
    pub session_name: Option<String>,
}
