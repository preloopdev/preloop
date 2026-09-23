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
//! M2: no api.github.com fallback — if the launch endpoint does not resolve
//! the ref to a SHA-pinned URL, the runner refuses the download.

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
    /// Archive checksum pin state for this action version (see
    /// [`ArchiveDigestPin`]).
    pub archive_sha256: ArchiveDigestPin,
}

/// Whether the server supports action archive-checksum pinning, and the pin
/// state for one resolved action.
///
/// The digest is the SHA-256 of the *downloaded tarball bytes*, hashed
/// before extraction. Pinning the archive bytes (rather than the extracted
/// tree) means a known pin is compared before `extract_tarball` ever runs,
/// so tampered bytes fail closed without creating an executable tree.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ArchiveDigestPin {
    /// The server predates checksum support (no `archive_sha256` key in the
    /// resolve response). The runner must behave exactly as before: no
    /// verification, no digest report, no cache eviction.
    #[default]
    Unsupported,
    /// The server supports checksums but has no pin for this (owner, repo,
    /// sha) yet: the engine has not fetched this version itself. The
    /// runner uses any existing local cache as-is — no eviction, no
    /// verification — and downloads normally on a cache miss; the engine
    /// pins the version when it serves the bytes.
    Unpinned,
    /// The server has a pin for this (owner, repo, sha): the downloaded
    /// archive's SHA-256 must equal this hex digest or the download fails
    /// closed before extraction.
    Pinned(String),
}

/// Client for action resolution and download.
pub struct ActionsResolveClient {
    http: HttpClient,
    /// Base URL from `system.github.launch_endpoint` variable.
    /// Golden 10: `https://launch.actions.githubusercontent.com`
    launch_base_url: Option<String>,
}

/// Returns true for a well-formed archive digest: 64 hex chars.
fn valid_archive_sha256(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit())
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

        // runnerresolve can return HTTP 422 together with usable entries in
        // `actions` and per-action errors. Other non-success statuses are
        // ordinary protocol failures and must not be treated as partial data.
        let response = match self
            .http
            .client_for(&url)
            .post(&url)
            .bearer_auth(token)
            .header("Accept", "application/json")
            .json(&body)
            .send()
            .await
            .context("runnerresolve/actions batch POST")
        {
            Ok(resp) => {
                let status = resp.status();
                if !status.is_success() && status != reqwest::StatusCode::UNPROCESSABLE_ENTITY {
                    return Err(anyhow::anyhow!(
                        "runnerresolve/actions batch returned HTTP {status}"
                    ));
                }
                match resp.json::<serde_json::Value>().await {
                    Ok(value) => {
                        if status == reqwest::StatusCode::UNPROCESSABLE_ENTITY {
                            tracing::warn!(
                                "runnerresolve batch returned {status}; using partial action resolutions"
                            );
                        }
                        value
                    }
                    Err(e) if status == reqwest::StatusCode::UNPROCESSABLE_ENTITY => {
                        return Err(e).context("parsing partial runnerresolve response");
                    }
                    Err(e) => return Err(e).context("parsing runnerresolve response"),
                }
            }
            Err(e) => {
                tracing::warn!(
                    "runnerresolve batch failed; action downloads will fail closed \
                     (M2: there is no api.github.com fallback): {e:#}"
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
                // Key presence (not value) signals server support: older
                // servers omit `archive_sha256` entirely, in which case the
                // runner keeps the legacy unverified behavior.
                let archive_sha256 = match info.get("archive_sha256") {
                    None => ArchiveDigestPin::Unsupported,
                    Some(serde_json::Value::Null) => ArchiveDigestPin::Unpinned,
                    Some(serde_json::Value::String(digest)) if valid_archive_sha256(digest) => {
                        ArchiveDigestPin::Pinned(digest.to_ascii_lowercase())
                    }
                    Some(other) => {
                        tracing::warn!(
                            "ignoring malformed archive_sha256 pin {other:?}; treating as unpinned"
                        );
                        ArchiveDigestPin::Unpinned
                    }
                };

                if !tar_url.is_empty() {
                    result.insert(
                        key.clone(),
                        ResolvedAction {
                            name,
                            version,
                            resolved_sha,
                            tar_url,
                            auth_token,
                            archive_sha256,
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
                .client_for(url)
                .get(url)
                .header("Authorization", format!("Bearer {t}"))
                .header("User-Agent", "preloop-runner")
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
                .client_for(url)
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn serve_once(status: &str, body: &str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let status = status.to_owned();
        let body = body.to_owned();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 8192];
            let _ = socket.read(&mut request).await.unwrap();
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn resolve_batch_accepts_partial_entries_only_for_422() {
        let body = serde_json::json!({
            "actions": {
                "actions/checkout@v4": {
                    "name": "actions/checkout",
                    "version": "v4",
                    "resolved_sha": "abc123",
                    "tar_url": "https://example.invalid/checkout.tar.gz"
                }
            }
        })
        .to_string();
        let base = serve_once("422 Unprocessable Entity", &body).await;
        let client = ActionsResolveClient::new(HttpClient::new(None).unwrap(), Some(base));

        let result = client
            .resolve_batch("token", "plan", "job", &[("actions/checkout", "v4")])
            .await
            .unwrap();

        assert_eq!(result["actions/checkout@v4"].resolved_sha, "abc123");
    }

    #[tokio::test]
    async fn resolve_batch_rejects_json_from_other_error_statuses() {
        let base = serve_once("401 Unauthorized", r#"{"actions":{}}"#).await;
        let client = ActionsResolveClient::new(HttpClient::new(None).unwrap(), Some(base));

        let error = client
            .resolve_batch("bad-token", "plan", "job", &[("actions/checkout", "v4")])
            .await
            .unwrap_err();

        assert!(error.to_string().contains("HTTP 401 Unauthorized"));
    }

    /// `archive_sha256` key presence signals server support: a well-formed
    /// digest string becomes a pin, explicit null means supported but
    /// unpinned, and a missing key means a pre-checksum server (legacy
    /// unverified behavior). Malformed digests are treated as unpinned so a
    /// bad pin can never fail closed or mint a bogus pin.
    #[tokio::test]
    async fn resolve_batch_parses_archive_sha256_pin_states() {
        let body = serde_json::json!({
            "actions": {
                "actions/checkout@v4": {
                    "name": "actions/checkout",
                    "version": "v4",
                    "resolved_sha": "abc123",
                    "tar_url": "https://example.invalid/checkout.tar.gz",
                    "archive_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                },
                "actions/setup-node@v4": {
                    "name": "actions/setup-node",
                    "version": "v4",
                    "resolved_sha": "def456",
                    "tar_url": "https://example.invalid/node.tar.gz",
                    "archive_sha256": null
                },
                "actions/cache@v4": {
                    "name": "actions/cache",
                    "version": "v4",
                    "resolved_sha": "123abc",
                    "tar_url": "https://example.invalid/cache.tar.gz"
                },
                "actions/bad@v4": {
                    "name": "actions/bad",
                    "version": "v4",
                    "resolved_sha": "456def",
                    "tar_url": "https://example.invalid/bad.tar.gz",
                    "archive_sha256": "not-hex"
                }
            }
        })
        .to_string();
        let base = serve_once("200 OK", &body).await;
        let client = ActionsResolveClient::new(HttpClient::new(None).unwrap(), Some(base));

        let result = client
            .resolve_batch("token", "plan", "job", &[("actions/checkout", "v4")])
            .await
            .unwrap();

        assert_eq!(
            result["actions/checkout@v4"].archive_sha256,
            ArchiveDigestPin::Pinned(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()
            )
        );
        assert_eq!(
            result["actions/setup-node@v4"].archive_sha256,
            ArchiveDigestPin::Unpinned
        );
        assert_eq!(
            result["actions/cache@v4"].archive_sha256,
            ArchiveDigestPin::Unsupported
        );
        assert_eq!(
            result["actions/bad@v4"].archive_sha256,
            ArchiveDigestPin::Unpinned,
            "malformed pin must not become a Pinned digest"
        );
    }
}
