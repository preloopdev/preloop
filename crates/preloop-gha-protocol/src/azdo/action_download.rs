use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// List of action references sent by the runner to resolve download URLs.
///
/// Upstream source: `ActionReferenceList.cs` in `GitHub.DistributedTask.WebApi`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ActionReferenceList {
    #[serde(default)]
    pub actions: Vec<ActionReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependencies: Option<Vec<String>>,
}

/// A single action reference in an [`ActionReferenceList`].
///
/// Upstream source: `ActionReference.cs` in `GitHub.DistributedTask.WebApi`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ActionReference {
    #[serde(default)]
    pub name_with_owner: String,
    #[serde(default)]
    pub r#ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// Collection of resolved action download info, keyed by `nameWithOwner@ref`.
///
/// Upstream source: `ActionDownloadInfoCollection.cs` in `GitHub.DistributedTask.WebApi`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ActionDownloadInfoCollection {
    #[serde(default)]
    pub actions: BTreeMap<String, ActionDownloadInfo>,
}

/// Information needed by the runner to download and verify an action archive.
///
/// Upstream source: `ActionDownloadInfo.cs` in `GitHub.DistributedTask.WebApi`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ActionDownloadInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<ActionDownloadAuthentication>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub package_details: Option<ActionDownloadPackageDetails>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_with_owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_name_with_owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tarball_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zipball_url: Option<String>,
}

/// Authentication details for downloading an action archive.
///
/// Upstream source: `ActionDownloadInfo.cs` (`ActionDownloadAuthentication`) in `GitHub.DistributedTask.WebApi`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ActionDownloadAuthentication {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

/// Package details for an action download.
///
/// Upstream source: `ActionDownloadInfo.cs` (`ActionDownloadPackageDetails`) in `GitHub.DistributedTask.WebApi`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ActionDownloadPackageDetails {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_digest: Option<String>,
}
