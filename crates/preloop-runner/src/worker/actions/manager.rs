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
            .client_for(&url)
            .get(&url)
            .header("Authorization", format!("Bearer {token}"))
            .header("User-Agent", "preloop-runner")
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

    let parent_dir = dest
        .parent()
        .context("action destination must have parent directory")?;
    std::fs::create_dir_all(parent_dir)
        .with_context(|| format!("creating parent dir {}", parent_dir.display()))?;

    let staging = tempfile::Builder::new()
        .prefix(".action_tmp_")
        .tempdir_in(parent_dir)
        .with_context(|| format!("creating staging dir in {}", parent_dir.display()))?;

    extract_tarball(&bytes, staging.path())?;

    let staging_path = staging.keep();
    if !dest.exists() {
        if let Err(err) = std::fs::rename(&staging_path, &dest) {
            let _ = std::fs::remove_dir_all(&staging_path);
            if !dest.exists() {
                return Err(err)
                    .with_context(|| format!("moving extracted action to {}", dest.display()));
            }
        }
    } else {
        let _ = std::fs::remove_dir_all(&staging_path);
    }

    info!("Extracted action to {}", dest.display());
    Ok(dest)
}

/// Check whether a relative symlink target, resolved against the symlink's parent directory,
/// normalizes safely within the root directory (never escapes above root).
fn is_safe_relative_symlink(link_parent: Option<&Path>, link_target: &Path) -> bool {
    if link_target.is_absolute() || link_target.starts_with("/") || link_target.starts_with("\\") {
        return false;
    }

    let mut stack: Vec<&std::ffi::OsStr> = Vec::new();
    if let Some(parent) = link_parent {
        for component in parent.components() {
            match component {
                std::path::Component::Normal(c) => stack.push(c),
                std::path::Component::ParentDir => {
                    if stack.pop().is_none() {
                        return false;
                    }
                }
                std::path::Component::CurDir => {}
                _ => return false,
            }
        }
    }

    for component in link_target.components() {
        match component {
            std::path::Component::Normal(c) => stack.push(c),
            std::path::Component::ParentDir => {
                if stack.pop().is_none() {
                    return false;
                }
            }
            std::path::Component::CurDir => {}
            _ => return false,
        }
    }

    true
}

/// Extract a `.tar.gz` tarball to `dest`, stripping the top-level directory.
///
/// Uses `cap_std` capability-based filesystem sandboxing to ensure extracted entries
/// cannot escape `dest` via path traversal (`..`), absolute paths, or malicious symlinks.
pub fn extract_tarball(bytes: &[u8], dest: &Path) -> Result<()> {
    std::fs::create_dir_all(dest)
        .with_context(|| format!("creating action dir {}", dest.display()))?;

    let dest_dir = cap_std::fs::Dir::open_ambient_dir(dest, cap_std::ambient_authority())
        .with_context(|| format!("opening capability sandbox for {}", dest.display()))?;

    #[cfg(unix)]
    use cap_std::fs::PermissionsExt;

    let decoder = flate2::read::GzDecoder::new(bytes);
    let mut archive = tar::Archive::new(decoder);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        if path.components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        }) {
            anyhow::bail!(
                "malicious archive entry escapes sandbox: {}",
                path.display()
            );
        }

        let stripped: PathBuf = path.components().skip(1).collect();
        if stripped.components().count() == 0 {
            continue;
        }

        let entry_type = entry.header().entry_type();
        if entry_type.is_dir() {
            dest_dir.create_dir_all(&stripped)?;
        } else if entry_type.is_file() {
            if let Some(parent) = stripped.parent() {
                if parent.components().count() > 0 {
                    dest_dir.create_dir_all(parent)?;
                }
            }
            let mut outfile = dest_dir.create(&stripped)?;
            std::io::copy(&mut entry, &mut outfile)?;

            #[cfg(unix)]
            if let Ok(mode) = entry.header().mode() {
                // Mask to standard rwx permissions (0o777), stripping setuid (0o4000), setgid (0o2000), and sticky (0o1000) bits
                let safe_mode = mode & 0o777;
                let perms = cap_std::fs::Permissions::from_mode(safe_mode);
                outfile
                    .set_permissions(perms)
                    .with_context(|| format!("setting permissions on {}", stripped.display()))?;
            }
        } else if entry_type.is_symlink() {
            if let Some(link_target) = entry.link_name()? {
                let parent = stripped.parent();
                if let Some(parent) = parent {
                    if parent.components().count() > 0 {
                        dest_dir.create_dir_all(parent)?;
                    }
                }

                // Resolve physical parent directory relative to dest to account for
                // preceding symlinks that shift the physical parent depth.
                let canonical_dest = dest.canonicalize()?;
                let physical_parent = if let Some(parent) = parent {
                    let parent_path = dest.join(parent);
                    if let Ok(canonical_parent) = parent_path.canonicalize() {
                        if !canonical_parent.starts_with(&canonical_dest) {
                            anyhow::bail!(
                                "symlink parent directory escapes destination root: {}",
                                parent.display()
                            );
                        }
                        canonical_parent
                            .strip_prefix(&canonical_dest)
                            .ok()
                            .map(Path::to_path_buf)
                    } else {
                        Some(parent.to_path_buf())
                    }
                } else {
                    None
                };

                if !is_safe_relative_symlink(physical_parent.as_deref(), &link_target) {
                    anyhow::bail!(
                        "symlink with escaping or absolute target rejected: {}",
                        link_target.display()
                    );
                }
                dest_dir.symlink(&link_target, &stripped)?;
            }
        } else {
            anyhow::bail!(
                "unsupported or dangerous archive entry type {:?} for {}",
                entry_type,
                path.display()
            );
        }
    }

    Ok(())
}

/// Copy a local action to the actions directory.
pub fn copy_local_action(source: &Path, actions_dir: &Path, action_name: &str) -> Result<PathBuf> {
    let dest = actions_dir.join(action_name);
    if dest.exists() {
        return Ok(dest);
    }
    let parent_dir = dest
        .parent()
        .context("destination must have parent directory")?;
    std::fs::create_dir_all(parent_dir)?;
    let staging = tempfile::Builder::new()
        .prefix(".local_action_tmp_")
        .tempdir_in(parent_dir)?;
    copy_dir_recursive(source, staging.path())?;
    let staging_path = staging.keep();
    if !dest.exists() {
        if let Err(err) = std::fs::rename(&staging_path, &dest) {
            let _ = std::fs::remove_dir_all(&staging_path);
            if !dest.exists() {
                return Err(err).with_context(|| {
                    format!(
                        "moving copied local action from {} to {}",
                        staging_path.display(),
                        dest.display()
                    )
                });
            }
        }
    } else {
        let _ = std::fs::remove_dir_all(&staging_path);
    }
    Ok(dest)
}

/// Recursively copy a directory.
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    copy_dir_recursive_inner(src, dst, src)
}

fn copy_dir_recursive_inner(src: &Path, dst: &Path, root_src: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let dest = dst.join(entry.file_name());
        if ty.is_symlink() {
            let entry_path = entry.path();
            let target = std::fs::read_link(&entry_path)
                .with_context(|| format!("reading symlink {}", entry_path.display()))?;
            let canonical_root = root_src.canonicalize()?;
            let physical_parent = if let Some(parent) = entry_path.parent() {
                if let Ok(canonical_parent) = parent.canonicalize() {
                    if !canonical_parent.starts_with(&canonical_root) {
                        anyhow::bail!(
                            "symlink parent directory escapes source root: {}",
                            parent.display()
                        );
                    }
                    canonical_parent
                        .strip_prefix(&canonical_root)
                        .ok()
                        .map(Path::to_path_buf)
                } else {
                    entry_path
                        .strip_prefix(root_src)
                        .ok()
                        .and_then(|p| p.parent())
                        .map(Path::to_path_buf)
                }
            } else {
                None
            };

            if !is_safe_relative_symlink(physical_parent.as_deref(), &target) {
                anyhow::bail!(
                    "local action contains escaping or absolute symlink: {} -> {}",
                    entry.path().display(),
                    target.display()
                );
            }
            #[cfg(unix)]
            std::os::unix::fs::symlink(&target, &dest)
                .with_context(|| format!("creating symlink {}", dest.display()))?;
            #[cfg(windows)]
            {
                let is_dir = if let Some(parent) = entry_path.parent() {
                    parent.join(&target).is_dir()
                } else {
                    target.is_dir()
                };
                if is_dir {
                    std::os::windows::fs::symlink_dir(&target, &dest)?;
                } else {
                    std::os::windows::fs::symlink_file(&target, &dest)?;
                }
            }
        } else if ty.is_dir() {
            copy_dir_recursive_inner(&entry.path(), &dest, root_src)?;
        } else {
            std::fs::copy(entry.path(), &dest)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_tarball(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        {
            let mut tar = tar::Builder::new(&mut enc);
            for (path, content) in entries {
                let mut header = tar::Header::new_gnu();
                header.set_size(content.len() as u64);
                header.set_mode(0o644);
                header.as_mut_bytes()[..path.len()].copy_from_slice(path.as_bytes());
                header.set_cksum();
                tar.append(&header, *content).unwrap();
            }
            tar.finish().unwrap();
        }
        enc.finish().unwrap()
    }

    fn create_test_tarball_with_custom_entry(
        path: &str,
        entry_type: tar::EntryType,
        link_name: Option<&str>,
        content: &[u8],
    ) -> Vec<u8> {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        {
            let mut tar = tar::Builder::new(&mut enc);
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_entry_type(entry_type);
            header.as_mut_bytes()[..path.len()].copy_from_slice(path.as_bytes());
            if let Some(target) = link_name {
                header.set_link_name(target).unwrap();
            }
            header.set_cksum();
            tar.append(&header, content).unwrap();
            tar.finish().unwrap();
        }
        enc.finish().unwrap()
    }

    #[test]
    fn extract_tarball_unpacks_safely_inside_sandbox() {
        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");

        let tar_bytes = create_test_tarball(&[
            ("checkout-v4/action.yml", b"name: Checkout\n"),
            ("checkout-v4/dist/index.js", b"console.log('hello');\n"),
        ]);

        extract_tarball(&tar_bytes, &dest).unwrap();
        assert_eq!(
            std::fs::read_to_string(dest.join("action.yml")).unwrap(),
            "name: Checkout\n"
        );
        assert_eq!(
            std::fs::read_to_string(dest.join("dist/index.js")).unwrap(),
            "console.log('hello');\n"
        );
    }

    #[test]
    fn extract_tarball_rejects_path_traversal() {
        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");

        let tar_bytes = create_test_tarball(&[("root/../../escape.txt", b"evil")]);
        let result = extract_tarball(&tar_bytes, &dest);
        assert!(result.is_err());
    }

    #[test]
    fn extract_tarball_rejects_absolute_paths() {
        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");

        let tar_bytes = create_test_tarball(&[("/escape.txt", b"evil")]);
        let result = extract_tarball(&tar_bytes, &dest);
        assert!(result.is_err());
    }

    #[test]
    fn extract_tarball_rejects_hard_links() {
        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");

        let tar_bytes = create_test_tarball_with_custom_entry(
            "root/evil_hardlink",
            tar::EntryType::Link,
            Some("/etc/passwd"),
            b"",
        );
        let result = extract_tarball(&tar_bytes, &dest);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unsupported or dangerous"));
    }

    #[test]
    fn extract_tarball_rejects_absolute_symlinks() {
        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");

        let tar_bytes = create_test_tarball_with_custom_entry(
            "root/evil_symlink",
            tar::EntryType::Symlink,
            Some("/etc/shadow"),
            b"",
        );
        let result = extract_tarball(&tar_bytes, &dest);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("symlink with escaping or absolute target rejected"));
    }

    #[test]
    fn extract_tarball_rejects_escaping_symlink_traversal() {
        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");
        let outside_file = temp.path().join("escaped_target.txt");
        std::fs::write(&outside_file, b"initial").unwrap();

        // Archive has:
        // 1. symlink `sub/evil_link` -> `../../escaped_target.txt`
        // 2. file `sub/evil_link` trying to overwrite through it or traverse it
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        {
            let mut tar = tar::Builder::new(&mut enc);
            let mut header = tar::Header::new_gnu();
            header.set_size(0);
            header.set_mode(0o777);
            header.set_entry_type(tar::EntryType::Symlink);
            header.as_mut_bytes()[.."root/sub/evil_link".len()]
                .copy_from_slice(b"root/sub/evil_link");
            header.set_link_name("../../escaped_target.txt").unwrap();
            header.set_cksum();
            tar.append(&header, &b""[..]).unwrap();

            let mut file_header = tar::Header::new_gnu();
            file_header.set_size(7);
            file_header.set_mode(0o644);
            file_header.set_entry_type(tar::EntryType::Regular);
            file_header.as_mut_bytes()[.."root/sub/evil_link/pwn".len()]
                .copy_from_slice(b"root/sub/evil_link/pwn");
            file_header.set_cksum();
            tar.append(&file_header, &b"hacked!"[..]).unwrap();
            tar.finish().unwrap();
        }
        let tar_bytes = enc.finish().unwrap();
        let result = extract_tarball(&tar_bytes, &dest);
        assert!(result.is_err());
        // Verify outside file was untouched
        assert_eq!(std::fs::read_to_string(&outside_file).unwrap(), "initial");
    }

    #[cfg(unix)]
    #[test]
    fn extract_tarball_rejects_chained_symlink_escape() {
        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");

        // Archive has:
        // 1. directory `root/b`
        // 2. symlink `root/a/deep` -> `../b`
        // 3. symlink `root/a/deep/link` -> `../../outside` (lexically looks like depth 2 with 2 '..' = 0, but physically is depth 1 with 2 '..' = -1!)
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        {
            let mut tar = tar::Builder::new(&mut enc);
            let mut dir_header = tar::Header::new_gnu();
            dir_header.set_size(0);
            dir_header.set_mode(0o755);
            dir_header.set_entry_type(tar::EntryType::Directory);
            dir_header.as_mut_bytes()[.."root/b/".len()].copy_from_slice(b"root/b/");
            dir_header.set_cksum();
            tar.append(&dir_header, &b""[..]).unwrap();

            let mut link1_header = tar::Header::new_gnu();
            link1_header.set_size(0);
            link1_header.set_mode(0o777);
            link1_header.set_entry_type(tar::EntryType::Symlink);
            link1_header.as_mut_bytes()[.."root/a/deep".len()].copy_from_slice(b"root/a/deep");
            link1_header.set_link_name("../b").unwrap();
            link1_header.set_cksum();
            tar.append(&link1_header, &b""[..]).unwrap();

            let mut link2_header = tar::Header::new_gnu();
            link2_header.set_size(0);
            link2_header.set_mode(0o777);
            link2_header.set_entry_type(tar::EntryType::Symlink);
            link2_header.as_mut_bytes()[.."root/a/deep/link".len()]
                .copy_from_slice(b"root/a/deep/link");
            link2_header.set_link_name("../../outside").unwrap();
            link2_header.set_cksum();
            tar.append(&link2_header, &b""[..]).unwrap();
            tar.finish().unwrap();
        }
        let tar_bytes = enc.finish().unwrap();
        let result = extract_tarball(&tar_bytes, &dest);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("symlink with escaping or absolute target rejected"));
    }

    #[cfg(unix)]
    #[test]
    fn extract_tarball_allows_in_root_relative_symlinks() {
        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");

        // Archive has:
        // 1. regular file `lib/tool.js`
        // 2. in-root relative symlink `bin/tool` -> `../lib/tool.js`
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        {
            let mut tar = tar::Builder::new(&mut enc);
            let mut file_header = tar::Header::new_gnu();
            file_header.set_size(19);
            file_header.set_mode(0o644);
            file_header.set_entry_type(tar::EntryType::Regular);
            file_header.as_mut_bytes()[.."root/lib/tool.js".len()]
                .copy_from_slice(b"root/lib/tool.js");
            file_header.set_cksum();
            tar.append(&file_header, &b"console.log('tool')"[..])
                .unwrap();

            let mut link_header = tar::Header::new_gnu();
            link_header.set_size(0);
            link_header.set_mode(0o777);
            link_header.set_entry_type(tar::EntryType::Symlink);
            link_header.as_mut_bytes()[.."root/bin/tool".len()].copy_from_slice(b"root/bin/tool");
            link_header.set_link_name("../lib/tool.js").unwrap();
            link_header.set_cksum();
            tar.append(&link_header, &b""[..]).unwrap();
            tar.finish().unwrap();
        }
        let tar_bytes = enc.finish().unwrap();
        extract_tarball(&tar_bytes, &dest).unwrap();
        assert_eq!(
            std::fs::read_to_string(dest.join("bin/tool")).unwrap(),
            "console.log('tool')"
        );
    }

    #[cfg(unix)]
    #[test]
    fn extract_tarball_masks_special_permission_bits() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");

        // Entry with setuid (0o4755)
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        {
            let mut tar = tar::Builder::new(&mut enc);
            let mut header = tar::Header::new_gnu();
            header.set_size(5);
            header.set_mode(0o4755);
            header.set_entry_type(tar::EntryType::Regular);
            header.as_mut_bytes()[.."root/script.sh".len()].copy_from_slice(b"root/script.sh");
            header.set_cksum();
            tar.append(&header, &b"echo\n"[..]).unwrap();
            tar.finish().unwrap();
        }
        let tar_bytes = enc.finish().unwrap();
        extract_tarball(&tar_bytes, &dest).unwrap();

        let metadata = std::fs::metadata(dest.join("script.sh")).unwrap();
        let mode = metadata.permissions().mode();
        // The setuid bit (0o4000) must be stripped, leaving only rwxr-xr-x (0o755)
        assert_eq!(mode & 0o7777, 0o755);
    }

    #[tokio::test]
    async fn download_action_atomic_cleanup_on_error() {
        use axum::{routing::get, Router};
        let evil_tar = create_test_tarball(&[("root/../../escape.txt", b"evil")]);

        let app = Router::new().route("/tarball", get(|| async move { evil_tar }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        let url = format!("http://{addr}/tarball");
        let result = download_action("owner", "repo", "v1", &actions_dir, Some(&url), None).await;

        assert!(result.is_err());
        let dest = actions_dir.join("owner").join("repo").join("v1");
        assert!(
            !dest.exists(),
            "failed download must not leave dest directory behind"
        );
    }

    #[tokio::test]
    async fn download_action_atomic_success_and_cache_hit() {
        use axum::{routing::get, Router};
        let valid_tar = create_test_tarball(&[("checkout-v4/action.yml", b"name: Checkout\n")]);

        let app = Router::new().route("/tarball", get(|| async move { valid_tar }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        let url = format!("http://{addr}/tarball");
        let res = download_action("owner", "repo", "v1", &actions_dir, Some(&url), None)
            .await
            .unwrap();

        assert!(res.exists());
        assert_eq!(
            std::fs::read_to_string(res.join("action.yml")).unwrap(),
            "name: Checkout\n"
        );

        // Second call hits the cache without reaching the server
        let cached_res = download_action(
            "owner",
            "repo",
            "v1",
            &actions_dir,
            Some("http://127.0.0.1:1/unreachable"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(cached_res, res);
    }

    #[cfg(unix)]
    #[test]
    fn copy_local_action_rejects_escaping_symlinks() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source_action");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(source.join("action.yml"), "name: Local\n").unwrap();

        // Create escaping symlink pointing outside action
        let outside = temp.path().join("secret.txt");
        std::fs::write(&outside, "secret").unwrap();
        std::os::unix::fs::symlink("../secret.txt", source.join("escape_link")).unwrap();

        let actions_dir = temp.path().join("actions");
        let result = copy_local_action(&source, &actions_dir, "my-local-action");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("escaping or absolute symlink"));
        assert!(!actions_dir.join("my-local-action").exists());
    }

    #[cfg(unix)]
    #[test]
    fn copy_local_action_allows_safe_internal_symlinks() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source_action");
        std::fs::create_dir_all(source.join("dist")).unwrap();
        std::fs::create_dir_all(source.join("bin")).unwrap();
        std::fs::write(source.join("action.yml"), "name: Local\n").unwrap();
        std::fs::write(source.join("dist/index.js"), "console.log('hi');\n").unwrap();

        // Create safe internal symlink
        std::os::unix::fs::symlink("dist/index.js", source.join("main.js")).unwrap();
        // Create safe in-root relative symlink spanning subdirectories
        std::fs::write(source.join("dist/tool.js"), "tool_content").unwrap();
        std::os::unix::fs::symlink("../dist/tool.js", source.join("bin/tool")).unwrap();

        let actions_dir = temp.path().join("actions");
        let dest = copy_local_action(&source, &actions_dir, "my-local-action").unwrap();
        assert!(dest.exists());
        assert_eq!(
            std::fs::read_to_string(dest.join("main.js")).unwrap(),
            "console.log('hi');\n"
        );
        assert_eq!(
            std::fs::read_to_string(dest.join("bin/tool")).unwrap(),
            "tool_content"
        );
    }
}
