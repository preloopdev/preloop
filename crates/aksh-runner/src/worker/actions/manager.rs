//! Action download and extraction manager.

use anyhow::Result;
use std::path::{Path, PathBuf};
use tracing::info;

/// Download and extract a remote action to the _actions directory.
pub async fn download_action(
    owner: &str,
    repo: &str,
    git_ref: &str,
    actions_dir: &Path,
    download_url: Option<&str>,
    auth_token: Option<&str>,
) -> Result<PathBuf> {
    let dest = actions_dir.join(owner).join(repo).join(git_ref);

    if dest.exists() {
        info!("Action {owner}/{repo}@{git_ref} already cached");
        return Ok(dest);
    }

    let url = download_url.map(String::from).unwrap_or_else(|| {
        format!("https://api.github.com/repos/{owner}/{repo}/tarball/{git_ref}")
    });

    info!("Downloading action {owner}/{repo}@{git_ref} from {url}");

    let client = crate::client::http::HttpClient::new(None)?;
    let bytes = if let Some(token) = auth_token {
        // Authenticated download
        let resp = reqwest::Client::new()
            .get(&url)
            .header("Authorization", format!("Bearer {token}"))
            .header("User-Agent", "aksh-runner")
            .send()
            .await?;
        if !resp.status().is_success() {
            anyhow::bail!("Action download failed: {} {}", resp.status(), url);
        }
        resp.bytes().await?
    } else {
        client.get_bytes(&url).await?
    };

    // Extract tarball, stripping top-level directory
    std::fs::create_dir_all(&dest)?;

    let decoder = flate2::read::GzDecoder::new(bytes.as_ref());
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

    info!("Extracted action to {}", dest.display());
    Ok(dest)
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
