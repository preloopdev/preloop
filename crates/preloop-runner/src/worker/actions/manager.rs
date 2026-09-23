//! Action download and extraction manager.
//!
//! F022: Uses `ActionsResolveClient` to batch-resolve `uses:` refs to SHA-pinned
//! codeload.github.com URLs before downloading.
//!
//! Golden 10 flow 19-20: batch POST to runnerresolve → GET codeload tarball →
//! extract to `_work/_actions/{owner}/{repo}/{sha}/`.
//!
//! M2: there is no api.github.com fallback. If the server did not resolve
//! the ref to a commit SHA with a SHA-pinned download URL, the download is
//! refused before any network access — fetching the mutable ref would
//! reintroduce the TOCTOU that SHA pinning removes.

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tracing::info;

use crate::client::actions_download::ArchiveDigestPin;

/// SHA-256 hex digest of downloaded action tarball bytes.
///
/// The digest is computed over the archive bytes *before* extraction, so a
/// known pin is compared before `extract_tarball` ever runs: tampered bytes
/// fail closed without creating an executable destination tree. The bytes
/// are whatever the server-pinned URL served over TLS, so the digest also
/// pins the packaging — a re-packaged tarball of the same commit is a
/// different archive and will not match an old pin.
pub fn archive_sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Path of the sidecar file recording which archive produced a cached
/// action tree: `<actions_dir>/<owner>/<repo>/<sha>.sha256`, containing the
/// lowercase hex SHA-256 of the tarball bytes that were extracted there.
/// Digest-sidecar path: `<actions>/<owner>/<repo>/<sha>.sha256`, the
/// lowercase hex SHA-256 of the tarball bytes a verified fresh download
/// hashed before extraction.
///
/// A cache entry's provenance is unknown (it may predate checksum support
/// or have been written by hand), so a cached tree alone can never match
/// a known pin — it is evicted and re-downloaded. The sidecar written by
/// a verified fresh download lets a later cache hit prove it came from
/// the pinned archive without re-downloading.
fn archive_digest_sidecar(actions_dir: &Path, owner: &str, repo: &str, dir_ref: &str) -> PathBuf {
    actions_dir
        .join(owner)
        .join(repo)
        .join(format!("{dir_ref}.sha256"))
}

/// Read the archive digest recorded for a cached action tree, if any.
fn read_cached_archive_digest(sidecar: &Path) -> Option<String> {
    std::fs::read_to_string(sidecar)
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
}

/// Remove a cached action tree and its digest sidecar. Fails closed: if the
/// eviction itself fails the caller must not run the suspect tree.
fn evict_action_cache(dest: &Path, sidecar: &Path) -> Result<()> {
    std::fs::remove_dir_all(dest)
        .with_context(|| format!("evicting action cache {}", dest.display()))?;
    if sidecar.exists() {
        std::fs::remove_file(sidecar)
            .with_context(|| format!("evicting action digest sidecar {}", sidecar.display()))?;
    }
    Ok(())
}

/// Download and extract a remote action to the _actions directory.
///
/// `git_ref` must be the server-resolved commit SHA (40 hex chars), not a
/// mutable branch/tag: callers pass `resolved_sha` from the runnerresolve
/// response, falling back to the raw `uses:` ref only when resolution
/// failed. `download_url` must be the server-supplied SHA-pinned tarball
/// URL. Both are required (M2): if the server could not pin the ref to a
/// commit, the runner refuses to fetch the mutable ref from
/// api.github.com — downloading `tarball/{branch|tag}` reintroduces the
/// TOCTOU that SHA pinning exists to remove (the ref can move between
/// resolution and download).
///
/// `digest_pin` carries the server's archive-checksum pin for this action
/// version ([`ArchiveDigestPin`]). The SHA-256 of the downloaded archive
/// bytes is computed immediately after download and compared against a known
/// pin *before* `extract_tarball` runs: a mismatch fails closed and no
/// destination tree is created, so tampered bytes are never executed. On a
/// successful fresh download the observed digest is returned, and a
/// digest sidecar is written after successful extraction so later cache
/// hits can be checked against it without re-downloading. Nothing is
/// reported back to the server: pins are minted by the engine when it
/// fetches the tarball itself, never by job VMs.
///
/// These checks run before the cache lookup so a stale mutable-ref cache
/// entry cannot bypass them, and before any network access.
pub async fn download_action(
    owner: &str,
    repo: &str,
    git_ref: &str,
    actions_dir: &Path,
    download_url: Option<&str>,
    auth_token: Option<&str>,
    digest_pin: ArchiveDigestPin,
) -> Result<(PathBuf, Option<String>)> {
    // M2: only pinned commit SHAs may be downloaded; anything else means
    // server-side resolution failed. The all-zero sentinel is a valid SHA
    // shape but names no commit, so it is rejected like any unpinned ref.
    if !preloop_gha_protocol::git_ref::is_commit_sha_not_zero(git_ref) {
        anyhow::bail!(
            "M2: refusing to download action {owner}/{repo}@{git_ref}: \
             ref was not resolved to a commit SHA"
        );
    }
    let url = download_url.filter(|url| !url.is_empty()).ok_or_else(|| {
        anyhow::anyhow!(
            "M2: refusing to download action {owner}/{repo}@{git_ref}: \
             server supplied no SHA-pinned download URL"
        )
    })?;

    // Use the resolved SHA as directory name when available for correctness.
    let dir_ref = git_ref; // caller should pass resolved_sha here when available
    let dest = actions_dir.join(owner).join(repo).join(dir_ref);

    // Whether this run verifies digests at all. An older server that
    // predates digest support keeps the exact legacy behavior: no
    // verification, no report, no cache eviction.
    let verifying = !matches!(digest_pin, ArchiveDigestPin::Unsupported);
    let sidecar = archive_digest_sidecar(actions_dir, owner, repo, dir_ref);

    if dest.exists() {
        match &digest_pin {
            ArchiveDigestPin::Pinned(expected) => {
                if read_cached_archive_digest(&sidecar).as_deref() == Some(expected.as_str()) {
                    info!(
                        "Action {owner}/{repo}@{git_ref} already cached at {} (archive checksum verified)",
                        dest.display()
                    );
                    return Ok((dest, Some(expected.clone())));
                }
                // A cached tree whose recorded archive digest does not match
                // the pin — poisoned, stale, or predating checksum support —
                // must never be executed: evict it and fall through to a
                // fresh verified download below. A known pin is never
                // bypassed by a cache entry.
                tracing::warn!(
                    "action archive checksum mismatch for cached {owner}/{repo}@{git_ref}: \
                     evicting cache and re-downloading"
                );
                evict_action_cache(&dest, &sidecar)?;
            }
            ArchiveDigestPin::Unpinned => {
                // The engine has not pinned this version yet (it has not
                // fetched it itself). The runner is not in a position to
                // establish the pin — it verifies *against* pins, never
                // mints them — so the existing cache is used as-is: no
                // eviction, no verification. A poisoned or stale cache is
                // the runner's own local trust decision, unchanged from
                // pre-checksum behavior.
                info!(
                    "Action {owner}/{repo}@{git_ref} already cached at {} (server has no pin yet)",
                    dest.display()
                );
                return Ok((dest, read_cached_archive_digest(&sidecar)));
            }
            ArchiveDigestPin::Unsupported => {
                info!(
                    "Action {owner}/{repo}@{git_ref} already cached at {}",
                    dest.display()
                );
                return Ok((dest, None));
            }
        }
    }

    // M2: no api.github.com fallback. `url` is the server-supplied
    // SHA-pinned tarball URL, validated above; a missing URL fails closed.
    let url = url.to_string();

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

    // Hash the downloaded archive bytes immediately, and compare a known
    // pin BEFORE creating any destination tree: a mismatch fails closed
    // here, so tampered bytes never reach `extract_tarball` and no
    // executable destination is left behind.
    let observed_digest = if verifying {
        let observed = archive_sha256_hex(&bytes);
        if let ArchiveDigestPin::Pinned(expected) = &digest_pin {
            if &observed != expected {
                anyhow::bail!(
                    "action archive sha256 mismatch for {owner}/{repo}@{git_ref}: \
                     expected {expected}, observed {observed}; refusing to extract a \
                     tarball whose bytes differ from the pinned archive checksum"
                );
            }
            info!("Action {owner}/{repo}@{git_ref} archive checksum verified: {observed}");
        }
        Some(observed)
    } else {
        None
    };

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

    // Record which archive produced this tree so later cache hits can
    // prove they came from the pinned bytes. Written only after the
    // archive extracted and moved into place successfully, so a sidecar
    // always describes a complete, extracted tree.
    if let Some(observed) = &observed_digest {
        std::fs::write(&sidecar, observed)
            .with_context(|| format!("recording action archive digest {}", sidecar.display()))?;
    }

    info!("Extracted action to {}", dest.display());
    Ok((dest, observed_digest))
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
    async fn download_action_refuses_all_zero_sha() {
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        // No server is started: the zero sentinel must be refused before any
        // network access or cache lookup.
        let result = download_action(
            "owner",
            "repo",
            "0000000000000000000000000000000000000000",
            &actions_dir,
            Some("http://127.0.0.1:1/tarball"),
            None,
            ArchiveDigestPin::Unsupported,
        )
        .await;

        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("was not resolved to a commit SHA"),
            "unexpected error: {error}"
        );
        assert!(
            !actions_dir.exists(),
            "refused download must not create the actions directory"
        );
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
        let result = download_action(
            "owner",
            "repo",
            "0123456789abcdef0123456789abcdef01234567",
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Unsupported,
        )
        .await;

        assert!(result.is_err());
        let dest = actions_dir
            .join("owner")
            .join("repo")
            .join("0123456789abcdef0123456789abcdef01234567");
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
        let (res, observed) = download_action(
            "owner",
            "repo",
            "0123456789abcdef0123456789abcdef01234567",
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Unsupported,
        )
        .await
        .unwrap();

        assert!(res.exists());
        assert!(
            observed.is_none(),
            "an unsupported pin must not compute a digest"
        );
        assert_eq!(
            std::fs::read_to_string(res.join("action.yml")).unwrap(),
            "name: Checkout\n"
        );

        // Second call hits the cache without reaching the server
        let (cached_res, _) = download_action(
            "owner",
            "repo",
            "0123456789abcdef0123456789abcdef01234567",
            &actions_dir,
            Some("http://127.0.0.1:1/unreachable"),
            None,
            ArchiveDigestPin::Unsupported,
        )
        .await
        .unwrap();
        assert_eq!(cached_res, res);
    }

    /// M2: an unresolved action (no SHA-pinned download URL from the
    /// server) must be rejected before any network access — the runner
    /// must not fall back to `api.github.com/repos/{o}/{r}/tarball/{ref}`.
    #[tokio::test]
    async fn download_action_rejects_missing_resolved_url() {
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");
        let sha = "0123456789abcdef0123456789abcdef01234567";

        let result = download_action(
            "owner",
            "repo",
            sha,
            &actions_dir,
            None,
            None,
            ArchiveDigestPin::Unsupported,
        )
        .await;
        let error = result.expect_err("missing resolved URL must fail closed");
        assert!(
            error.to_string().contains("no SHA-pinned download URL"),
            "unexpected error: {error:#}"
        );
        assert!(
            !actions_dir.join("owner").join("repo").join(sha).exists(),
            "rejected download must not create the destination"
        );
    }

    /// M2: a mutable ref (branch/tag/short SHA) that the server failed to
    /// resolve must be rejected before any network access, even when a URL
    /// is supplied. The unreachable URL proves no network attempt happens:
    /// a fetch would fail with a connection error, not the M2 error.
    #[tokio::test]
    async fn download_action_rejects_mutable_ref() {
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        for git_ref in ["v4", "main", "a5ac7e5", "not-a-sha"] {
            let result = download_action(
                "owner",
                "repo",
                git_ref,
                &actions_dir,
                Some("http://127.0.0.1:1/unreachable"),
                None,
                ArchiveDigestPin::Unsupported,
            )
            .await;
            let error = result.expect_err("mutable ref must fail closed");
            assert!(
                error.to_string().contains("not resolved to a commit SHA"),
                "ref {git_ref:?}: unexpected error: {error:#}"
            );
        }
    }

    /// M2: the SHA/URL checks run before the cache lookup — a stale
    /// mutable-ref cache entry (left by a pre-fix run) must not bypass
    /// fail-closed.
    #[tokio::test]
    async fn download_action_rejects_mutable_ref_despite_cache() {
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");
        let stale = actions_dir.join("owner").join("repo").join("v4");
        std::fs::create_dir_all(&stale).unwrap();
        std::fs::write(stale.join("action.yml"), b"stale").unwrap();

        let result = download_action(
            "owner",
            "repo",
            "v4",
            &actions_dir,
            Some("http://127.0.0.1:1/unreachable"),
            None,
            ArchiveDigestPin::Unsupported,
        )
        .await;
        assert!(
            result.is_err(),
            "stale mutable-ref cache entry must not bypass M2"
        );
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

    /// Serve `tar_bytes` on a loopback axum server; returns the tarball URL.
    async fn serve_test_tarball(tar_bytes: Vec<u8>) -> String {
        use axum::{routing::get, Router};
        let app = Router::new().route("/tarball", get(|| async move { tar_bytes }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}/tarball")
    }

    const TEST_SHA: &str = "0123456789abcdef0123456789abcdef01234567";

    fn test_action_tarball() -> Vec<u8> {
        create_test_tarball(&[
            ("action-root/action.yml", b"name: Checkout\n"),
            ("action-root/dist/index.js", b"console.log('hi');\n"),
        ])
    }

    #[test]
    fn archive_sha256_hex_is_deterministic_and_content_sensitive() {
        let a = b"fake tarball bytes";
        let b = b"fake tarball bytes";
        let c = b"fake tarball bytes!";
        let ha = super::archive_sha256_hex(a);
        assert_eq!(ha, super::archive_sha256_hex(b));
        assert_eq!(ha.len(), 64);
        assert!(ha.bytes().all(|ch| ch.is_ascii_hexdigit()));
        assert_ne!(ha, super::archive_sha256_hex(c));
        // Known vector: sha256 of empty input.
        assert_eq!(
            super::archive_sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    /// A fresh download whose archive bytes hash to the pinned digest is
    /// accepted, the observed digest is returned, and the sidecar records
    /// the archive digest for later cache hits.
    #[tokio::test]
    async fn download_action_accepts_matching_archive_pin() {
        let tarball = test_action_tarball();
        let pin = super::archive_sha256_hex(&tarball);
        let url = serve_test_tarball(tarball).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        let (dest, observed) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Pinned(pin.clone()),
        )
        .await
        .unwrap();

        assert!(dest.exists());
        assert_eq!(observed.as_deref(), Some(pin.as_str()));
        let sidecar = actions_dir
            .join("owner")
            .join("repo")
            .join(format!("{TEST_SHA}.sha256"));
        assert_eq!(
            std::fs::read_to_string(&sidecar).unwrap().trim(),
            pin,
            "sidecar must record the verified archive digest"
        );
        assert_eq!(
            std::fs::read_to_string(dest.join("action.yml")).unwrap(),
            "name: Checkout\n"
        );
    }

    /// A fresh download whose archive bytes do NOT hash to the pin fails
    /// closed before extraction: the error names the mismatch, no
    /// destination tree is created, and nothing is executed.
    #[tokio::test]
    async fn download_action_fails_closed_on_archive_mismatch() {
        let url = serve_test_tarball(test_action_tarball()).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");
        let dest = actions_dir.join("owner").join("repo").join(TEST_SHA);

        let wrong_pin = "0".repeat(64);
        let error = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Pinned(wrong_pin),
        )
        .await
        .expect_err("archive mismatch must fail closed");
        assert!(
            error.to_string().contains("sha256 mismatch"),
            "unexpected error: {error:#}"
        );
        assert!(
            !dest.exists(),
            "mismatched download must leave no destination behind"
        );
    }

    /// The pin is compared before extraction: with a wrong pin, even bytes
    /// that are not a valid tarball fail with the mismatch error rather
    /// than an extraction error; with the right pin the same bytes reach
    /// extraction and fail there.
    #[tokio::test]
    async fn download_action_checks_archive_hash_before_extracting() {
        let not_a_tarball = b"this is not a tarball".to_vec();
        let honest_pin = super::archive_sha256_hex(&not_a_tarball);

        // Wrong pin: the mismatch error fires before extraction is attempted.
        let url = serve_test_tarball(not_a_tarball.clone()).await;
        let temp = TempDir::new().unwrap();
        let error = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &temp.path().join("actions"),
            Some(&url),
            None,
            ArchiveDigestPin::Pinned("f".repeat(64)),
        )
        .await
        .expect_err("wrong pin must fail closed");
        assert!(
            error.to_string().contains("sha256 mismatch"),
            "hash must be checked before extraction: {error:#}"
        );

        // Right pin for garbage bytes: the hash passes and extraction fails.
        let url = serve_test_tarball(not_a_tarball).await;
        let temp = TempDir::new().unwrap();
        let error = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &temp.path().join("actions"),
            Some(&url),
            None,
            ArchiveDigestPin::Pinned(honest_pin),
        )
        .await
        .expect_err("garbage bytes must fail extraction");
        assert!(
            !error.to_string().contains("sha256 mismatch"),
            "matching pin must reach extraction: {error:#}"
        );
    }

    /// An unpinned fresh download returns the observed archive digest,
    /// which is also written to the digest sidecar after extraction so a
    /// later Pinned cache hit can verify against it.
    #[tokio::test]
    async fn download_action_unpinned_fresh_download_returns_observed_digest() {
        let tarball = test_action_tarball();
        let expected = super::archive_sha256_hex(&tarball);
        let url = serve_test_tarball(tarball).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        let (dest, observed) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Unpinned,
        )
        .await
        .unwrap();

        assert!(dest.exists());
        assert_eq!(
            observed.as_deref(),
            Some(expected.as_str()),
            "fresh download must report the archive's SHA-256"
        );
    }

    /// A cache hit whose sidecar matches the known pin is used without
    /// re-downloading — the pin is enforced, not bypassed, by the cache.
    #[tokio::test]
    async fn download_action_cache_hit_with_matching_pin_uses_cache() {
        let tarball = test_action_tarball();
        let pin = super::archive_sha256_hex(&tarball);
        let url = serve_test_tarball(tarball).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        let (first_dest, observed) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Unpinned,
        )
        .await
        .unwrap();
        assert_eq!(observed.as_deref(), Some(pin.as_str()));

        // Second call with the now-known pin and an unreachable URL must
        // succeed from cache — no network, no bypass.
        let (cached_dest, cached_observed) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some("http://127.0.0.1:1/unreachable"),
            None,
            ArchiveDigestPin::Pinned(pin.clone()),
        )
        .await
        .unwrap();
        assert_eq!(cached_dest, first_dest);
        assert_eq!(cached_observed.as_deref(), Some(pin.as_str()));
    }

    /// A poisoned cache entry (sidecar digest differs from the pin) is
    /// evicted and replaced by a fresh verified download — never executed.
    #[tokio::test]
    async fn download_action_evicts_cache_on_pin_mismatch() {
        let tarball = test_action_tarball();
        let pin = super::archive_sha256_hex(&tarball);
        let url = serve_test_tarball(tarball).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Unpinned,
        )
        .await
        .unwrap();

        // Poison the cache entry and its sidecar.
        let dest = actions_dir.join("owner").join("repo").join(TEST_SHA);
        std::fs::write(dest.join("dist/index.js"), b"console.log('pwned');\n").unwrap();
        let sidecar = actions_dir
            .join("owner")
            .join("repo")
            .join(format!("{TEST_SHA}.sha256"));
        std::fs::write(&sidecar, "1".repeat(64)).unwrap();

        // The poisoned entry must be evicted and replaced by a fresh
        // verified download.
        let (fresh_dest, observed) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Pinned(pin.clone()),
        )
        .await
        .unwrap();
        assert_eq!(fresh_dest, dest);
        assert_eq!(
            std::fs::read_to_string(dest.join("dist/index.js")).unwrap(),
            "console.log('hi');\n"
        );
        assert_eq!(observed.as_deref(), Some(pin.as_str()));
        assert_eq!(std::fs::read_to_string(&sidecar).unwrap().trim(), pin);
    }

    /// A pre-feature cache entry (no sidecar) cannot satisfy a known pin:
    /// it is evicted and re-downloaded, so a stale cache never bypasses
    /// the pin.
    #[tokio::test]
    async fn download_action_cache_without_sidecar_cannot_satisfy_pin() {
        let tarball = test_action_tarball();
        let pin = super::archive_sha256_hex(&tarball);
        let url = serve_test_tarball(tarball).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        // Simulate a pre-feature cache entry: valid content, no sidecar.
        let dest = actions_dir.join("owner").join("repo").join(TEST_SHA);
        std::fs::create_dir_all(&dest).unwrap();
        std::fs::write(dest.join("action.yml"), b"name: Stale\n").unwrap();

        let (fresh_dest, observed) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Pinned(pin.clone()),
        )
        .await
        .unwrap();
        assert_eq!(fresh_dest, dest);
        assert_eq!(
            std::fs::read_to_string(dest.join("action.yml")).unwrap(),
            "name: Checkout\n",
            "cache of unknown provenance must be replaced by a verified download"
        );
        assert_eq!(observed.as_deref(), Some(pin.as_str()));
    }

    /// An unpinned cache hit is used as-is: the runner verifies against
    /// pins but never mints them, so an existing cache is trusted exactly
    /// as before checksum support — no eviction, no re-download.
    #[tokio::test]
    async fn download_action_unpinned_uses_existing_cache_without_download() {
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        // Simulate a pre-existing cache entry of unknown provenance.
        let dest = actions_dir.join("owner").join("repo").join(TEST_SHA);
        std::fs::create_dir_all(&dest).unwrap();
        std::fs::write(dest.join("action.yml"), b"name: Existing\n").unwrap();

        // The URL is unreachable: any network attempt would fail. Success
        // proves the cache was used without downloading.
        let (cached_dest, observed) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some("http://127.0.0.1:1/unreachable"),
            None,
            ArchiveDigestPin::Unpinned,
        )
        .await
        .unwrap();
        assert_eq!(cached_dest, dest);
        assert_eq!(
            std::fs::read_to_string(dest.join("action.yml")).unwrap(),
            "name: Existing\n",
            "unpinned cache must be used as-is, not evicted"
        );
        assert_eq!(
            observed, None,
            "an unpinned cache entry of unknown provenance carries no digest"
        );
    }
}
