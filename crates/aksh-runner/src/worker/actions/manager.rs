//! Action download and extraction manager.
//!
//! F022: Uses `ActionsResolveClient` to batch-resolve `uses:` refs to SHA-pinned
//! codeload.github.com URLs before downloading. Falls back to api.github.com
//! tarball if the launch endpoint is unavailable (e.g. local aksh).
//!
//! Golden 10 flow 19-20: batch POST to runnerresolve → GET codeload tarball →
//! extract to `_work/_actions/{owner}/{repo}/{sha}/`.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tracing::info;

/// Download and extract a remote action to the _actions directory.
///
/// `resolved_sha` — if Some, is used for the directory name and download URL.
/// `download_url` — if Some, overrides the URL (from runnerresolve response).
/// `auth_token` — if Some, added as Bearer auth to the download.
pub async fn download_action(
    owner: &str,
    repo: &str,
    git_ref: &str,
    actions_dir: &Path,
    download_url: Option<&str>,
    auth_token: Option<&str>,
) -> Result<PathBuf> {
    // Use the resolved SHA as directory name when available for correctness.
    let dir_ref = git_ref; // caller should pass resolved_sha here when available
    let dest = actions_dir.join(owner).join(repo).join(dir_ref);

    if dest.exists() {
        info!(
            "Action {owner}/{repo}@{git_ref} already cached at {}",
            dest.display()
        );
        return Ok(dest);
    }

    // Build download URL: prefer resolved codeload URL, fall back to api.github.com
    let url = download_url.map(String::from).unwrap_or_else(|| {
        tracing::warn!(
            "No resolved URL for {owner}/{repo}@{git_ref}, using api.github.com fallback"
        );
        format!("https://api.github.com/repos/{owner}/{repo}/tarball/{git_ref}")
    });

    info!("Downloading action {owner}/{repo}@{git_ref} from {url}");

    let client = crate::client::http::HttpClient::new(None)?;
    let bytes = if let Some(token) = auth_token {
        // Authenticated download (GitHub codeload or private actions)
        let resp = client
            .inner_client()
            .get(&url)
            .header("Authorization", format!("Bearer {token}"))
            .header("User-Agent", "aksh-runner")
            .send()
            .await
            .with_context(|| format!("downloading action tarball from {url}"))?;
        if !resp.status().is_success() {
            anyhow::bail!("Action download failed: {} {}", resp.status(), url);
        }
        resp.bytes().await?
    } else {
        client.get_bytes(&url).await?
    };

    // Extract tarball, stripping top-level directory (standard GitHub tarball layout)
    // v2.336.0 (#4509): Log archive size for telemetry
    info!(
        "Action archive {owner}/{repo}@{git_ref}: {} bytes",
        bytes.len()
    );
    extract_tarball(&bytes, &dest)?;

    info!("Extracted action to {}", dest.display());
    Ok(dest)
}

/// Extract a `.tar.gz` tarball to `dest`, stripping the top-level directory.
pub fn extract_tarball(bytes: &[u8], dest: &Path) -> Result<()> {
    std::fs::create_dir_all(dest)
        .with_context(|| format!("creating action dir {}", dest.display()))?;

    let decoder = flate2::read::GzDecoder::new(bytes);
    let mut archive = tar::Archive::new(decoder);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        let stripped: PathBuf = path.components().skip(1).collect();
        if stripped.components().count() == 0 {
            continue;
        }
        let target = dest.join(&stripped);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        entry.unpack(&target)?;
    }

    Ok(())
}

/// Copy a local action to the actions directory.
pub fn copy_local_action(source: &Path, actions_dir: &Path, action_name: &str) -> Result<PathBuf> {
    let dest = actions_dir.join(action_name);
    if dest.exists() {
        return Ok(dest);
    }
    copy_dir_recursive(source, &dest)?;
    Ok(dest)
}

/// Recursively copy a directory.
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let dest = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir_recursive(&entry.path(), &dest)?;
        } else {
            std::fs::copy(entry.path(), &dest)?;
        }
    }
    Ok(())
}
