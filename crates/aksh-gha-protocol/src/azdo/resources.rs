use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ─── Resources and endpoints ──────────────────────────────────────────────

/// Resources block in a job message — contains service endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResources {
    #[serde(rename = "endpoints", default)]
    pub endpoints: Vec<ServiceEndpoint>,
    #[serde(
        rename = "repositories",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub repositories: Vec<RepositoryReference>,
}

/// A service endpoint — connection to an external service.
///
/// The most important one is `SystemVssConnection` which carries the
/// OAuth token the runner uses for all subsequent API calls.
///
/// Upstream source: `ServiceEndpoint.cs`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceEndpoint {
    #[serde(rename = "data", default)]
    pub data: BTreeMap<String, String>,
    #[serde(rename = "name")]
    pub name: String,
    #[serde(skip)]
    pub endpoint_type: Option<String>,
    #[serde(rename = "url", skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(rename = "authorization")]
    pub authorization: EndpointAuthorization,
    #[serde(rename = "isShared", skip_serializing_if = "Option::is_none")]
    pub is_shared: Option<bool>,
    #[serde(rename = "isReady", skip_serializing_if = "Option::is_none")]
    pub is_ready: Option<bool>,
    #[serde(skip)]
    pub service_owner: Option<String>,
}

/// Authorization data for a service endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointAuthorization {
    #[serde(rename = "parameters", default)]
    pub parameters: BTreeMap<String, String>,
    #[serde(rename = "scheme", skip_serializing_if = "Option::is_none")]
    pub scheme: Option<String>,
}

/// Repository reference in job resources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepositoryReference {
    #[serde(rename = "repository", skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,
    #[serde(rename = "ref", skip_serializing_if = "Option::is_none")]
    pub git_ref: Option<String>,
    #[serde(rename = "connector", skip_serializing_if = "Option::is_none")]
    pub connector: Option<RepositoryConnector>,
}

/// Connector for a repository reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepositoryConnector {
    #[serde(rename = "id", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}
