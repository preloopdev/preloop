//! Actions resolution and download client.
//!
//! F022: Implements the `runnerresolve/actions` batch POST endpoint used by
//! the official runner (golden 10 flow 19-20) to resolve `uses:` references
//! to SHA-pinned tarball URLs before downloading.
//!
//! Golden 10 flow 19:
//!   POST launch.actions.githubusercontent.com/actions/{build}/{orchestrationId}/jobs/{jobId}/runner
//!   body: { "actions": [{ "action": "actions/checkout", "version": "v4" }] }
//!   response: { "actions": { "actions/checkout@v4": { "resolved_sha": "...", "tar_url": "..." } } }
//!
//! Then flow 20: GET codeload.github.com/{owner}/{repo}/tar.gz/{resolved_sha}
//!
//! Fallback: If the launch endpoint is unavailable (local aksh), falls back
//! to api.github.com/repos/{o}/{r}/tarball/{ref} for compatibility.

use anyhow::{Context, Result};
use std::collections::HashMap;

use super::http::HttpClient;

/// Resolved action metadata from runnerresolve.
#[derive(Debug, Clone)]
pub struct ResolvedAction {
    pub name: String,
    pub version: String,
    pub resolved_sha: String,
    pub tar_url: String,
    pub auth_token: Option<String>,
}

/// Client for action resolution and download.
pub struct ActionsResolveClient {
    http: HttpClient,
    /// Base URL from `system.github.launch_endpoint` variable.
    /// Golden 10: `https://launch.actions.githubusercontent.com`
    launch_base_url: Option<String>,
}

impl ActionsResolveClient {
    /// Create a new client.
    ///
    /// `launch_base_url` comes from `system.github.launch_endpoint` in job message variables.
    pub fn new(http: HttpClient, launch_base_url: Option<String>) -> Self {
        Self {
            http,
            launch_base_url,
        }
    }

    /// Batch-resolve a list of `uses:` references via `runnerresolve/actions`.
    ///
    /// Golden 10 flow 19: POST `{launch_base}/actions/{build}/{orchestration_id}/jobs/{job_id}/runner`
    /// with body `{ "actions": [{ "action": "owner/repo", "version": "ref" }] }`.
    ///
    /// Returns a map from `"owner/repo@ref"` → `ResolvedAction`.
    pub async fn resolve_batch(
        &self,
        token: &str,
        orchestration_id: &str,
        job_id: &str,
        actions: &[(&str, &str)], // (action, version) pairs
    ) -> Result<HashMap<String, ResolvedAction>> {
        let Some(ref base) = self.launch_base_url else {
            return Ok(HashMap::new());
        };

        let action_list: Vec<serde_json::Value> = actions
            .iter()
            .map(|(action, version)| serde_json::json!({ "action": action, "version": version }))
            .collect();

        let body = serde_json::json!({ "actions": action_list });

        // URL format from golden 10 flow 19:
        // /actions/build/{plan_id}/jobs/{job_id}/runnerresolve/actions
        let url = format!(
            "{}/actions/build/{orchestration_id}/jobs/{job_id}/runnerresolve/actions",
            base.trim_end_matches('/')
        );

        let response: serde_json::Value = match self
            .http
            .post_json_bearer(&url, &body, token)
            .await
            .context("runnerresolve/actions batch POST")
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    "runnerresolve batch failed (will use api.github.com fallback): {e:#}"
                );
                return Ok(HashMap::new());
            }
        };

        let mut result = HashMap::new();
        if let Some(resolved_map) = response.get("actions").and_then(|v| v.as_object()) {
            for (key, info) in resolved_map {
                let name = info
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let version = info
                    .get("version")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let resolved_sha = info
                    .get("resolved_sha")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let tar_url = info
                    .get("tar_url")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let auth_token = info
                    .get("authentication")
                    .and_then(|v| v.as_str())
                    .map(String::from);

                if !tar_url.is_empty() {
                    result.insert(
                        key.clone(),
                        ResolvedAction {
                            name,
                            version,
                            resolved_sha,
                            tar_url,
                            auth_token,
                        },
                    );
                }
            }
        }

        Ok(result)
    }

    /// Download a tarball from a resolved URL (authenticated or anonymous).
    pub async fn download_tarball(&self, url: &str, token: Option<&str>) -> Result<bytes::Bytes> {
        if let Some(t) = token {
            let resp = self
                .http
                .inner_client()
                .get(url)
                .header("Authorization", format!("Bearer {t}"))
                .header("User-Agent", "aksh-runner")
                .send()
                .await
                .with_context(|| format!("downloading tarball from {url}"))?;
            if !resp.status().is_success() {
                anyhow::bail!("tarball download {url} returned {}", resp.status());
            }
            resp.bytes()
                .await
                .with_context(|| format!("reading tarball body from {url}"))
        } else {
            self.http.get_bytes(url).await
        }
    }
}

/// Legacy client kept for aksh compatibility.
pub struct ActionsDownloadClient {
    http: HttpClient,
    base_url: String,
}

impl ActionsDownloadClient {
    /// Create a new actions download client.
    pub fn new(http: HttpClient, base_url: String) -> Self {
        Self { http, base_url }
    }

    /// Resolve action download info from the server (aksh local path).
    pub async fn resolve_actions(
        &self,
        token: &str,
        actions: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = format!("{}/_apis/v1/actiondownloadinfo", self.base_url);
        self.http
            .post_json_bearer(&url, actions, token)
            .await
            .context("resolving action download info")
    }

    /// Download an action tarball.
    pub async fn download_tarball(&self, url: &str, token: Option<&str>) -> Result<bytes::Bytes> {
        if let Some(t) = token {
            let resp = self
                .http
                .inner_client()
                .get(url)
                .header("Authorization", format!("Bearer {t}"))
                .send()
                .await
                .with_context(|| format!("downloading tarball from {url}"))?;
            if !resp.status().is_success() {
                anyhow::bail!("tarball download {url} returned {}", resp.status());
            }
            resp.bytes()
                .await
                .with_context(|| format!("reading tarball body from {url}"))
        } else {
            self.http.get_bytes(url).await
        }
    }
}
