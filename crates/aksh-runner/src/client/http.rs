//! Shared HTTP client with CA bundle and proxy support.

use anyhow::{Context, Result};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use std::path::Path;
use std::time::Duration;

/// Shared HTTP client wrapping reqwest with CA bundle, proxy, and auth.
#[derive(Clone)]
pub struct HttpClient {
    inner: reqwest::Client,
}

impl HttpClient {
    /// Create a new HTTP client, optionally trusting an extra CA bundle.
    pub fn new(ca_bundle: Option<&Path>) -> Result<Self> {
        let mut builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(100))
            .connect_timeout(Duration::from_secs(30))
            .user_agent(format!(
                "aksh-runner/{} (protocol-compat {})",
                crate::VERSION,
                crate::PROTOCOL_COMPAT_VERSION
            ));

        if let Some(ca_path) = ca_bundle {
            let pem = std::fs::read(ca_path)
                .with_context(|| format!("reading CA bundle {}", ca_path.display()))?;
            let cert = reqwest::Certificate::from_pem(&pem)?;
            builder = builder.add_root_certificate(cert);
        }

        let client = builder.build()?;
        Ok(Self { inner: client })
    }

    /// Expose the underlying reqwest::Client for cases that need direct HTTP control.
    pub fn inner_client(&self) -> &reqwest::Client {
        &self.inner
    }

    /// GET request returning JSON.
    pub async fn get_json<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T> {
        let resp = self
            .inner
            .get(url)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("GET {url} returned {status}: {body}");
        }
        resp.json()
            .await
            .with_context(|| format!("parsing GET {url}"))
    }

    /// GET request with bearer auth returning JSON.
    pub async fn get_json_with_auth<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        auth: &str,
    ) -> Result<T> {
        let resp = self
            .inner
            .get(url)
            .header(ACCEPT, "application/json")
            .header(AUTHORIZATION, auth)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("GET {url} returned {status}: {body}");
        }
        resp.json()
            .await
            .with_context(|| format!("parsing GET {url}"))
    }

    /// GET request with bearer auth and custom Accept header returning JSON.
    pub async fn get_json_with_auth_accept<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        auth: &str,
        accept: &str,
    ) -> Result<T> {
        let resp = self
            .inner
            .get(url)
            .header(ACCEPT, accept)
            .header(AUTHORIZATION, auth)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("GET {url} returned {status}: {body}");
        }
        resp.json()
            .await
            .with_context(|| format!("parsing GET {url}"))
    }

    /// GET request returning raw bytes.
    pub async fn get_bytes(&self, url: &str) -> Result<bytes::Bytes> {
        let resp = self
            .inner
            .get(url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("GET {url} returned {status}");
        }
        resp.bytes()
            .await
            .with_context(|| format!("reading body of GET {url}"))
    }

    /// POST JSON with custom authorization header, returning JSON.
    pub async fn post_json_with_auth<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        body: &serde_json::Value,
        auth: &str,
    ) -> Result<T> {
        let resp = self
            .inner
            .post(url)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .header(AUTHORIZATION, auth)
            .json(body)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("POST {url} returned {status}: {body}");
        }
        json_or_null(resp, &format!("parsing POST {url}")).await
    }

    /// POST JSON with custom authorization, Accept, and Content-Type headers.
    pub async fn post_json_with_auth_headers<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        body: &serde_json::Value,
        auth: &str,
        accept: &str,
        content_type: &str,
    ) -> Result<T> {
        let resp = self
            .inner
            .post(url)
            .header(CONTENT_TYPE, content_type)
            .header(ACCEPT, accept)
            .header(AUTHORIZATION, auth)
            .json(body)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("POST {url} returned {status}: {body}");
        }
        json_or_null(resp, &format!("parsing POST {url}")).await
    }

    /// POST form-encoded data returning JSON.
    pub async fn post_form_json<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        form: &[(&str, &str)],
    ) -> Result<T> {
        let resp = self
            .inner
            .post(url)
            .form(form)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .with_context(|| format!("POST form {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("POST form {url} returned {status}: {body}");
        }
        resp.json()
            .await
            .with_context(|| format!("parsing POST form {url}"))
    }

    /// POST JSON with bearer auth, returning JSON.
    pub async fn post_json_bearer<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        body: &serde_json::Value,
        token: &str,
    ) -> Result<T> {
        self.post_json_with_auth(url, body, &format!("Bearer {token}"))
            .await
    }

    /// PATCH JSON with bearer auth, returning JSON.
    pub async fn patch_json_bearer<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        body: &serde_json::Value,
        token: &str,
    ) -> Result<T> {
        let resp = self
            .inner
            .patch(url)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .json(body)
            .send()
            .await
            .with_context(|| format!("PATCH {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("PATCH {url} returned {status}: {body}");
        }
        resp.json()
            .await
            .with_context(|| format!("parsing PATCH {url}"))
    }

    /// DELETE with bearer auth.
    pub async fn delete_with_token(&self, url: &str, token: &str) -> Result<()> {
        let resp = self
            .inner
            .delete(url)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .send()
            .await
            .with_context(|| format!("DELETE {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("DELETE {url} returned {status}: {body}");
        }
        Ok(())
    }

    /// PUT raw bytes with content type.
    pub async fn put_bytes(&self, url: &str, data: Vec<u8>, content_type: &str) -> Result<()> {
        let resp = self
            .inner
            .put(url)
            .header(CONTENT_TYPE, content_type)
            .header("x-ms-blob-type", "BlockBlob")
            .body(data)
            .send()
            .await
            .with_context(|| format!("PUT {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("PUT {url} returned {status}: {body}");
        }
        Ok(())
    }

    /// Build a long-poll GET request with timeout.
    pub async fn get_long_poll<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        auth: &str,
        timeout: Duration,
    ) -> Result<Option<T>> {
        let resp = self
            .inner
            .get(url)
            .header(ACCEPT, "application/json")
            .header(AUTHORIZATION, auth)
            .timeout(timeout)
            .send()
            .await;
        match resp {
            Ok(r) => {
                let status = r.status();
                if status == reqwest::StatusCode::OK {
                    let body: T = r.json().await?;
                    Ok(Some(body))
                } else if status == reqwest::StatusCode::ACCEPTED
                    || status == reqwest::StatusCode::NO_CONTENT
                {
                    // Long-poll timeout — no message
                    Ok(None)
                } else {
                    let body = r.text().await.unwrap_or_default();
                    anyhow::bail!("long-poll {url} returned {status}: {body}");
                }
            }
            Err(e) if e.is_timeout() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

async fn json_or_null<T: serde::de::DeserializeOwned>(
    resp: reqwest::Response,
    context: &str,
) -> Result<T> {
    let text = resp.text().await.with_context(|| context.to_string())?;
    if text.trim().is_empty() {
        serde_json::from_value(serde_json::Value::Null).with_context(|| context.to_string())
    } else {
        serde_json::from_str(&text).with_context(|| context.to_string())
    }
}
