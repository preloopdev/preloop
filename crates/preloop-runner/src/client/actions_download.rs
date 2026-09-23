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
    /// Tree-digest pin state for this action version (see [`TreeDigestPin`]).
    pub tree_digest: TreeDigestPin,
}

/// Whether the server supports action tree-digest pinning, and the pin state
/// for one resolved action.
///
/// The digest is a canonical hash of the *extracted* action tree (not the
/// tarball bytes): codeload tarballs are an opaque packaging of a commit, and
/// pinning raw bytes would fail closed on any packaging change GitHub makes.
/// The tree digest depends only on the commit's content, which the resolved
/// SHA already identifies.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum TreeDigestPin {
    /// The server predates digest support (no `tree_digest` key in the
    /// resolve response). The runner must behave exactly as before: no
    /// verification, no digest report, no cache eviction.
    #[default]
    Unsupported,
    /// The server supports digests but has no pin for this (owner, repo,
    /// sha) yet: this download establishes it (trust on first use).
    Unpinned,
    /// The server has a pin for this (owner, repo, sha): the downloaded
    /// tree must hash to exactly this digest or the download fails closed.
    Pinned(String),
}

/// Client for action resolution and download.
pub struct ActionsResolveClient {
    http: HttpClient,
    /// Base URL from `system.github.launch_endpoint` variable.
    /// Golden 10: `https://launch.actions.githubusercontent.com`
    launch_base_url: Option<String>,
}

/// Parameters for a first-use tree digest report. Bundled into one struct
/// so the report call does not grow an argument per protocol field.
pub struct TreeDigestReport<'a> {
    /// Job bearer token (the same credential runnerresolve accepted).
    pub token: &'a str,
    pub orchestration_id: &'a str,
    pub job_id: &'a str,
    pub owner: &'a str,
    pub repo: &'a str,
    /// Resolved commit SHA. The server rejects anything that is not a
    /// commit SHA; pins are never keyed by mutable ref.
    pub sha: &'a str,
    /// `tree-sha256-v1:<hex>` digest observed by this runner.
    pub tree_digest: &'a str,
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
                // servers omit `tree_digest` entirely, in which case the
                // runner keeps the legacy unverified behavior.
                let tree_digest = match info.get("tree_digest") {
                    None => TreeDigestPin::Unsupported,
                    Some(serde_json::Value::Null) => TreeDigestPin::Unpinned,
                    Some(serde_json::Value::String(digest)) if !digest.is_empty() => {
                        TreeDigestPin::Pinned(digest.clone())
                    }
                    _ => TreeDigestPin::Unpinned,
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
                            tree_digest,
                        },
                    );
                }
            }
        }

        Ok(result)
    }

    /// Report the observed tree digest of a freshly downloaded action so the
    /// server can pin it for later downloads (trust on first use).
    ///
    /// Best-effort by design: a failed report only means this runner does not
    /// establish the pin, so it warns but never fails the job. Callers must
    /// only report digests of trees they just downloaded and verified over
    /// TLS — never of pre-existing cache directories of unknown provenance.
    pub async fn report_tree_digest(&self, report: TreeDigestReport<'_>) -> Result<()> {
        let TreeDigestReport {
            token,
            orchestration_id,
            job_id,
            owner,
            repo,
            sha,
            tree_digest,
        } = report;
        let Some(ref base) = self.launch_base_url else {
            anyhow::bail!("no launch endpoint configured for digest report");
        };
        if !preloop_gha_protocol::git_ref::is_commit_sha_not_zero(sha) {
            anyhow::bail!("refusing to report digest for unresolved ref");
        }
        let url = format!(
            "{}/actions/build/{orchestration_id}/jobs/{job_id}/runnerresolve/actions/digests",
            base.trim_end_matches('/')
        );
        let body = serde_json::json!({
            "owner": owner,
            "repo": repo,
            "sha": sha,
            "tree_digest": tree_digest,
        });
        let resp = self
            .http
            .client_for(&url)
            .post(&url)
            .bearer_auth(token)
            .header("Accept", "application/json")
            .json(&body)
            .send()
            .await
            .context("digest report POST")?;
        if !resp.status().is_success() {
            anyhow::bail!("digest report returned HTTP {}", resp.status());
        }
        Ok(())
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

    /// `tree_digest` key presence signals server support: a digest string
    /// becomes a pin, explicit null means supported-but-unpinned, and a
    /// missing key means a pre-digest server (legacy unverified behavior).
    #[tokio::test]
    async fn resolve_batch_parses_tree_digest_pin_states() {
        let body = serde_json::json!({
            "actions": {
                "actions/checkout@v4": {
                    "name": "actions/checkout",
                    "version": "v4",
                    "resolved_sha": "abc123",
                    "tar_url": "https://example.invalid/checkout.tar.gz",
                    "tree_digest": "tree-sha256-v1:deadbeef"
                },
                "actions/setup-node@v4": {
                    "name": "actions/setup-node",
                    "version": "v4",
                    "resolved_sha": "def456",
                    "tar_url": "https://example.invalid/node.tar.gz",
                    "tree_digest": null
                },
                "actions/cache@v4": {
                    "name": "actions/cache",
                    "version": "v4",
                    "resolved_sha": "123abc",
                    "tar_url": "https://example.invalid/cache.tar.gz"
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
            result["actions/checkout@v4"].tree_digest,
            TreeDigestPin::Pinned("tree-sha256-v1:deadbeef".to_string())
        );
        assert_eq!(
            result["actions/setup-node@v4"].tree_digest,
            TreeDigestPin::Unpinned
        );
        assert_eq!(
            result["actions/cache@v4"].tree_digest,
            TreeDigestPin::Unsupported
        );
    }

    /// The digest report validates the SHA client-side and fails on HTTP
    /// errors; the runner treats a failed report as best-effort.
    #[tokio::test]
    async fn report_tree_digest_validates_sha_and_surfaces_http_errors() {
        let client = ActionsResolveClient::new(
            HttpClient::new(None).unwrap(),
            Some("http://127.0.0.1:1".into()),
        );

        // Unresolved refs must never be reported: pins are keyed by commit.
        let err = client
            .report_tree_digest(TreeDigestReport {
                token: "token",
                orchestration_id: "plan",
                job_id: "job",
                owner: "o",
                repo: "r",
                sha: "v4",
                tree_digest: "tree-sha256-v1:abc",
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("unresolved ref"));

        // Unreachable server surfaces as an error (caller logs a warning).
        let err = client
            .report_tree_digest(TreeDigestReport {
                token: "token",
                orchestration_id: "plan",
                job_id: "job",
                owner: "o",
                repo: "r",
                sha: "0123456789abcdef0123456789abcdef01234567",
                tree_digest: "tree-sha256-v1:abc",
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("digest report POST"));
    }

    #[tokio::test]
    async fn report_tree_digest_posts_expected_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let seen = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<u8>::new()));
        let seen_clone = seen.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 8192];
            let n = socket.read(&mut request).await.unwrap();
            *seen_clone.lock().await = request[..n].to_vec();
            let response = "HTTP/1.1 200 OK\r\ncontent-length: 16\r\nconnection: close\r\n\r\n{\"recorded\":true}";
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let client = ActionsResolveClient::new(
            HttpClient::new(None).unwrap(),
            Some(format!("http://{address}")),
        );
        client
            .report_tree_digest(TreeDigestReport {
                token: "job-token",
                orchestration_id: "plan1",
                job_id: "job1",
                owner: "actions",
                repo: "checkout",
                sha: "0123456789abcdef0123456789abcdef01234567",
                tree_digest: "tree-sha256-v1:feedface",
            })
            .await
            .unwrap();
        let raw = seen.lock().await;
        let text = String::from_utf8_lossy(&raw);
        assert!(text.contains("/runnerresolve/actions/digests"), "{text}");
        assert!(text.contains("Bearer job-token"), "{text}");
        assert!(
            text.contains("\"tree_digest\":\"tree-sha256-v1:feedface\""),
            "{text}"
        );
        assert!(
            text.contains("\"sha\":\"0123456789abcdef0123456789abcdef01234567\""),
            "{text}"
        );
    }
}
