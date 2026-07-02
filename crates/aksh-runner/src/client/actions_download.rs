//! Actions download info client (resolves `uses:` to download URLs).

use anyhow::{Context, Result};

use super::http::HttpClient;

/// Client for action download resolution.
pub struct ActionsDownloadClient {
    http: HttpClient,
    base_url: String,
}

impl ActionsDownloadClient {
    /// Create a new actions download client.
    pub fn new(http: HttpClient, base_url: String) -> Self {
        Self { http, base_url }
    }

    /// Resolve action download info from the server.
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
            // Authenticated download (GitHub codeload)
            let resp = reqwest::Client::new()
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
