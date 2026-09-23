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

use crate::client::actions_download::TreeDigestPin;

/// Digest algorithm/version tag prefixing every tree digest this runner
/// produces. Bumped if the canonical encoding ever changes; the server
/// treats differently-prefixed pins as distinct values, so an encoding
/// change fails closed rather than silently comparing across encodings.
pub const TREE_DIGEST_PREFIX: &str = "tree-sha256-v1:";

/// Compute the canonical digest of an extracted action tree.
///
/// The digest covers exactly what will be executed: for every entry under
/// `root`, in byte-sorted relative-path order, it hashes the entry kind,
/// the relative path, and — for files — the permission bits (masked to
/// `0o777`, as extraction enforces) and content bytes; for symlinks — the
/// link target. Directory permission bits are intentionally excluded: parent
/// directories are created umask-dependently at extraction time, and their
/// modes do not affect what executes.
///
/// This is deliberately a *tree* digest rather than a hash of the tarball
/// bytes: codeload tarballs are an opaque packaging of a commit, and a
/// packaging change on GitHub's side must never invalidate pins or, worse,
/// fail closed on every action download. The tree depends only on the
/// commit's content, which the resolved SHA already identifies — so any
/// difference in served bytes that changes what runs is detected, while
/// byte-level packaging churn is not.
pub fn canonical_tree_digest(root: &Path) -> Result<String> {
    let mut entries: Vec<(Vec<u8>, u8, u32, Vec<u8>)> = Vec::new();
    collect_tree_entries(root, root, &mut entries)?;
    // Byte-sorted relative paths make the encoding order canonical.
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let mut hasher = Sha256::new();
    for (rel_path, kind, mode, payload) in &entries {
        hasher.update([*kind]);
        hasher.update([0]);
        hasher.update(format!("{mode:o}").as_bytes());
        hasher.update([0]);
        hasher.update(rel_path);
        hasher.update([0]);
        hasher.update(payload);
        hasher.update([0]);
    }
    Ok(format!("{TREE_DIGEST_PREFIX}{:x}", hasher.finalize()))
}

/// Recursively collect `(relative path, kind, mode, payload)` tuples.
/// Kinds: `b'd'` directory (mode always 0 — see above), `b'f'` file
/// (content payload), `b'l'` symlink (link-target payload). Symlinks are
/// never followed.
fn collect_tree_entries(
    root: &Path,
    dir: &Path,
    out: &mut Vec<(Vec<u8>, u8, u32, Vec<u8>)>,
) -> Result<()> {
    // `symlink_metadata` so a symlink-to-dir is recorded as a link, not
    // traversed: traversal would both follow untrusted links and make the
    // digest depend on link targets' contents twice.
    let metadata = std::fs::symlink_metadata(dir)
        .with_context(|| format!("reading metadata for {}", dir.display()))?;
    #[cfg(unix)]
    let file_mode = {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o777
    };
    #[cfg(not(unix))]
    let file_mode = 0;

    let rel = dir
        .strip_prefix(root)
        .map(|p| p.to_string_lossy().replace('\\', "/").into_bytes())
        .unwrap_or_default();
    if dir != root {
        if metadata.file_type().is_symlink() {
            let target = std::fs::read_link(dir)
                .with_context(|| format!("reading link {}", dir.display()))?;
            out.push((
                rel,
                b'l',
                0,
                target.to_string_lossy().replace('\\', "/").into_bytes(),
            ));
            return Ok(());
        } else if metadata.file_type().is_dir() {
            // Directory modes are umask-dependent at extraction time and do
            // not affect what executes: excluded from the digest.
            out.push((rel, b'd', 0, Vec::new()));
        } else if metadata.file_type().is_file() {
            let content =
                std::fs::read(dir).with_context(|| format!("reading file {}", dir.display()))?;
            out.push((rel, b'f', file_mode, content));
        } else {
            anyhow::bail!("unsupported entry type in action tree: {}", dir.display());
        }
    }
    if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
        let mut children: Vec<PathBuf> = std::fs::read_dir(dir)
            .with_context(|| format!("listing {}", dir.display()))?
            .map(|e| e.map(|e| e.path()))
            .collect::<std::io::Result<_>>()
            .with_context(|| format!("listing {}", dir.display()))?;
        children.sort();
        for child in children {
            collect_tree_entries(root, &child, out)?;
        }
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
/// `digest_pin` carries the server's tree-digest pin for this action
/// version ([`TreeDigestPin`]). The digest is verified against the
/// extracted tree before the action is accepted: a mismatch fails closed
/// and the download is discarded, never executed. On success the observed
/// digest is returned so the caller can report it for first-use pinning.
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
    digest_pin: TreeDigestPin,
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
    let verifying = !matches!(digest_pin, TreeDigestPin::Unsupported);

    if dest.exists() {
        match &digest_pin {
            TreeDigestPin::Pinned(expected) => {
                let observed = canonical_tree_digest(&dest)?;
                if &observed != expected {
                    // A poisoned or stale cache entry (e.g. downloaded
                    // before digest support existed) must not be executed:
                    // evict it and fall through to a fresh verified
                    // download below. If eviction itself fails, fail
                    // closed rather than running the suspect tree.
                    tracing::warn!(
                        "action tree digest mismatch for cached {owner}/{repo}@{git_ref}: \
                         evicting cache and re-downloading"
                    );
                    std::fs::remove_dir_all(&dest).with_context(|| {
                        format!("evicting digest-mismatched action cache {}", dest.display())
                    })?;
                } else {
                    info!(
                        "Action {owner}/{repo}@{git_ref} already cached at {} (digest verified)",
                        dest.display()
                    );
                    return Ok((dest, Some(observed)));
                }
            }
            TreeDigestPin::Unpinned => {
                // A cache entry of unknown provenance cannot establish the
                // first-use pin: evict it so the pin comes from a fresh
                // download whose bytes were just verified over TLS.
                info!(
                    "Action {owner}/{repo}@{git_ref} cached but unpinned: \
                     re-downloading to establish the first-use digest pin"
                );
                std::fs::remove_dir_all(&dest).with_context(|| {
                    format!("evicting unpinned action cache {}", dest.display())
                })?;
            }
            TreeDigestPin::Unsupported => {
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

    // Verify the extracted tree before it is moved into place: a mismatch
    // fails closed and the staging directory is discarded, so tampered
    // bytes are never executed. The digest covers the post-extraction tree
    // (what will actually run), not the tarball bytes.
    let observed_digest = if verifying {
        let observed = canonical_tree_digest(staging.path())?;
        if let TreeDigestPin::Pinned(expected) = &digest_pin {
            if &observed != expected {
                anyhow::bail!(
                    "action tree digest mismatch for {owner}/{repo}@{git_ref}: \
                     expected {expected}, observed {observed}; refusing to run a \
                     tarball whose content differs from the pinned digest"
                );
            }
            info!("Action {owner}/{repo}@{git_ref} tree digest verified: {observed}");
        }
        Some(observed)
    } else {
        None
    };

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
            TreeDigestPin::Unsupported,
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
            TreeDigestPin::Unsupported,
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
            TreeDigestPin::Unsupported,
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
            TreeDigestPin::Unsupported,
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
            TreeDigestPin::Unsupported,
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
                TreeDigestPin::Unsupported,
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
            TreeDigestPin::Unsupported,
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
    fn canonical_tree_digest_is_deterministic_and_content_sensitive() {
        let temp = TempDir::new().unwrap();
        let a = temp.path().join("a");
        let b = temp.path().join("b");
        for dir in [&a, &b] {
            std::fs::create_dir_all(dir.join("dist")).unwrap();
            std::fs::write(dir.join("action.yml"), b"name: X\n").unwrap();
            std::fs::write(dir.join("dist/index.js"), b"console.log('hi');\n").unwrap();
        }
        let da = canonical_tree_digest(&a).unwrap();
        let db = canonical_tree_digest(&b).unwrap();
        assert_eq!(da, db, "identical trees must hash identically");
        assert!(
            da.starts_with(super::TREE_DIGEST_PREFIX),
            "digest must carry the algorithm prefix: {da}"
        );

        // Any content change flips the digest.
        std::fs::write(b.join("dist/index.js"), b"console.log('evil');\n").unwrap();
        assert_ne!(canonical_tree_digest(&b).unwrap(), da);

        // Adding a file flips the digest.
        std::fs::write(b.join("dist/index.js"), b"console.log('hi');\n").unwrap();
        std::fs::write(b.join("extra.txt"), b"x").unwrap();
        assert_ne!(canonical_tree_digest(&b).unwrap(), da);
    }

    #[cfg(unix)]
    #[test]
    fn canonical_tree_digest_covers_symlink_targets() {
        use std::os::unix::fs::symlink;
        let temp = TempDir::new().unwrap();
        let a = temp.path().join("a");
        let b = temp.path().join("b");
        for dir in [&a, &b] {
            std::fs::create_dir_all(dir).unwrap();
            std::fs::write(dir.join("real.js"), b"data").unwrap();
            symlink("real.js", dir.join("link.js")).unwrap();
        }
        assert_eq!(
            canonical_tree_digest(&a).unwrap(),
            canonical_tree_digest(&b).unwrap()
        );
        std::fs::remove_file(b.join("link.js")).unwrap();
        symlink("other.js", b.join("link.js")).unwrap();
        std::fs::write(b.join("other.js"), b"data").unwrap();
        assert_ne!(
            canonical_tree_digest(&a).unwrap(),
            canonical_tree_digest(&b).unwrap(),
            "retargeted symlink must change the digest"
        );
    }

    /// A fresh download whose tree does not match the pinned digest fails
    /// closed: the destination is never created and nothing is executed.
    #[tokio::test]
    async fn download_action_fails_closed_on_digest_mismatch() {
        let url = serve_test_tarball(test_action_tarball()).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        let wrong_pin = format!(
            "{}0000000000000000000000000000000000000000000000000000000000000000",
            super::TREE_DIGEST_PREFIX
        );
        let error = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            TreeDigestPin::Pinned(wrong_pin),
        )
        .await
        .expect_err("digest mismatch must fail closed");
        assert!(
            error.to_string().contains("digest mismatch"),
            "unexpected error: {error:#}"
        );
        assert!(
            !actions_dir
                .join("owner")
                .join("repo")
                .join(TEST_SHA)
                .exists(),
            "mismatched download must leave no destination behind"
        );
    }

    /// A fresh download matching the pinned digest is accepted, and the
    /// observed digest is returned for first-use reporting.
    #[tokio::test]
    async fn download_action_accepts_matching_pin_and_reports_digest() {
        let url = serve_test_tarball(test_action_tarball()).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        // First download establishes the digest (unpinned = trust on first
        // use); a second fresh download verifies against it.
        let (first_dest, observed) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            TreeDigestPin::Unpinned,
        )
        .await
        .unwrap();
        let observed = observed.expect("fresh download must report a digest");
        assert!(first_dest.exists());

        let fresh_dir = temp.path().join("actions2");
        let (second_dest, observed2) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &fresh_dir,
            Some(&url),
            None,
            TreeDigestPin::Pinned(observed.clone()),
        )
        .await
        .unwrap();
        assert!(second_dest.exists());
        assert_eq!(observed2.as_deref(), Some(observed.as_str()));
    }

    /// A poisoned cache entry (digest differs from the pin) is evicted and
    /// replaced by a fresh verified download — never executed.
    #[tokio::test]
    async fn download_action_evicts_cache_on_pin_mismatch() {
        let url = serve_test_tarball(test_action_tarball()).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        // Establish the honest pin with a fresh download.
        let (_, observed) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            TreeDigestPin::Unpinned,
        )
        .await
        .unwrap();
        let pin = observed.unwrap();

        // Poison the cache entry.
        let dest = actions_dir.join("owner").join("repo").join(TEST_SHA);
        std::fs::write(dest.join("dist/index.js"), b"console.log('pwned');\n").unwrap();
        assert_ne!(canonical_tree_digest(&dest).unwrap(), pin);

        // The poisoned entry must be evicted and replaced by a fresh
        // verified download.
        let (fresh_dest, observed2) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            TreeDigestPin::Pinned(pin.clone()),
        )
        .await
        .unwrap();
        assert_eq!(fresh_dest, dest);
        assert_eq!(
            std::fs::read_to_string(dest.join("dist/index.js")).unwrap(),
            "console.log('hi');\n"
        );
        assert_eq!(observed2.as_deref(), Some(pin.as_str()));
    }

    /// Migration: a cache entry predating digest support cannot mint the
    /// first-use pin, so it is re-downloaded once to establish it.
    #[tokio::test]
    async fn download_action_redownloads_unpinned_cache_for_first_use() {
        let url = serve_test_tarball(test_action_tarball()).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        // Simulate a pre-feature cache entry with stale content.
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
            TreeDigestPin::Unpinned,
        )
        .await
        .unwrap();
        assert_eq!(fresh_dest, dest);
        assert_eq!(
            std::fs::read_to_string(dest.join("action.yml")).unwrap(),
            "name: Checkout\n",
            "stale cache must be replaced by a fresh download"
        );
        assert!(
            observed.is_some(),
            "fresh download must report a digest for first-use pinning"
        );
    }
}
