//! Content-addressed, read-only AgentENV volumes for host directories.
//!
//! A read-only [`crate::VolumeMount`] (the runner bundle, the shared Node
//! externals) is never copied into a sandbox. Its host tree is archived once
//! into a deterministic tar, the archive's SHA-256 names a read-only volume
//! (`preloop-runner-<12 hex>`), and every sandbox that needs the tree mounts
//! that volume at `aenv start`. A changed tree has a new digest and therefore a
//! new volume; an existing volume is never modified.
//!
//! The digest covers what a guest can observe: entry names and types, permission
//! bits, symlink targets, and file bytes, in sorted order. Timestamps and
//! ownership are excluded, so the same bundle extracted on another host, or
//! re-extracted with new mtimes, maps to the same volume.
//!
//! Two things are deliberately not part of the tree a guest gets:
//!
//! * **Symlinks that leave the tree.** A link whose target is absolute, or whose
//!   relative target climbs out with `..`, is refused while the tree is
//!   archived ([`check_symlink_target`]). Following one from inside a read-only
//!   mount would reach whatever path the link names in the *guest*, not in the
//!   volume, which is not a path the volume was ever meant to expose.
//! * **Set-user-ID, set-group-ID and sticky bits.** They are masked out of the
//!   archived modes, so nothing in a volume can escalate inside the guest.
//!
//! The archive also carries a synthetic [`MANIFEST_ENTRY`] describing what the
//! tree holds: the version pins it was archived with ([`BundleManifest`]) and the
//! file count and byte total. It exists only in the archive, never in the host
//! directory, and it is part of the digest.

use crate::VmError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};

/// Prefix of every volume this module names. Pruning is limited to it.
pub const VOLUME_PREFIX: &str = "preloop-runner-";
/// Prefix of the exclusive volume a populate run writes before it is frozen.
pub const STAGING_PREFIX: &str = "preloop-runner-stage-";
/// The synthetic entry each archive carries, saying what the volume holds.
///
/// It is written into the archive only: the host tree is never modified.
pub const MANIFEST_ENTRY: &str = ".preloop-bundle.json";
/// Hex digits of the archive digest kept in a volume name.
const NAME_DIGEST_HEX: usize = 12;
const MIB: u64 = 1024 * 1024;
/// Smallest volume a populate run creates. AgentENV's default is 64 GiB, which
/// would reserve far more than a runner tree needs.
const MIN_VOLUME_MIB: u64 = 1024;
/// Room above the archive size for the filesystem's own metadata.
const VOLUME_HEADROOM_MIB: u64 = 256;
/// AgentENV's default ceiling for one volume (`[volume] max_size_mb`).
pub const MAX_VOLUME_MIB: u64 = 262_144;

/// What a version cannot be determined as, in the manifest and in the index.
pub const UNKNOWN_VERSION: &str = "unknown";

/// The version pins a runner tree was archived with.
///
/// [`archive_tree`] stamps these into a synthetic [`MANIFEST_ENTRY`] entry
/// inside the archive, so a volume can say what it holds. The pins come from
/// the caller — `versions.toml` is compiled into the CLI and the orchestrator,
/// not into this crate — and a version that is not known is recorded as
/// [`UNKNOWN_VERSION`] rather than failing the archive.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleManifest {
    /// The bundled runner's version.
    #[serde(default)]
    pub runner_version: Option<String>,
    /// The Node 20 externals version.
    #[serde(default)]
    pub node20: Option<String>,
    /// The Node 24 externals version.
    #[serde(default)]
    pub node24: Option<String>,
}

impl BundleManifest {
    /// A manifest from the pins a build knows; an empty pin is `unknown`.
    pub fn new(
        runner_version: impl Into<String>,
        node20: impl Into<String>,
        node24: impl Into<String>,
    ) -> Self {
        Self {
            runner_version: known_pin(runner_version.into()),
            node20: known_pin(node20.into()),
            node24: known_pin(node24.into()),
        }
    }

    /// The runner version, or [`UNKNOWN_VERSION`].
    pub fn runner_version(&self) -> &str {
        self.runner_version.as_deref().unwrap_or(UNKNOWN_VERSION)
    }

    /// The Node 20 externals version, or [`UNKNOWN_VERSION`].
    pub fn node20_version(&self) -> &str {
        self.node20.as_deref().unwrap_or(UNKNOWN_VERSION)
    }

    /// The Node 24 externals version, or [`UNKNOWN_VERSION`].
    pub fn node24_version(&self) -> &str {
        self.node24.as_deref().unwrap_or(UNKNOWN_VERSION)
    }

    /// Whether every pin is known, i.e. nothing had to be recorded as unknown.
    pub fn is_complete(&self) -> bool {
        [&self.runner_version, &self.node20, &self.node24]
            .into_iter()
            .all(|pin| pin.is_some())
    }

    /// The manifest body, as bytes.
    ///
    /// Sorted keys, fixed formatting, and nothing that depends on when or where
    /// the archive was made: a timestamp or host path here would change the
    /// digest of an unchanged tree on every archive and publish a new volume
    /// each time.
    pub fn to_bytes(&self, files: u64, bytes: u64) -> Vec<u8> {
        let mut body = String::new();
        body.push_str("{\"bundle_bytes\":");
        body.push_str(&bytes.to_string());
        body.push_str(",\"bundle_files\":");
        body.push_str(&files.to_string());
        body.push_str(",\"node_externals\":{\"node20\":");
        body.push_str(&json_string(self.node20_version()));
        body.push_str(",\"node24\":");
        body.push_str(&json_string(self.node24_version()));
        body.push_str("},\"runner_version\":");
        body.push_str(&json_string(self.runner_version()));
        body.push_str(",\"schema\":1}\n");
        body.into_bytes()
    }

    /// Feed the pins into a hash, so a changed pin invalidates a memoised
    /// volume even when the tree on disk is byte-identical.
    fn hash_into(&self, hasher: &mut Sha256) {
        hasher.update(self.runner_version().as_bytes());
        hasher.update([0u8]);
        hasher.update(self.node20_version().as_bytes());
        hasher.update([0u8]);
        hasher.update(self.node24_version().as_bytes());
    }
}

/// A pin, or `None` when the build did not know it.
fn known_pin(version: String) -> Option<String> {
    if version.trim().is_empty() {
        None
    } else {
        Some(version)
    }
}

/// A JSON string literal for `value`. The values are version pins, but escaping
/// keeps a strange one from producing a document that cannot be parsed.
fn json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for character in value.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            control if (control as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", control as u32));
            }
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

/// A host tree archived for population.
#[derive(Debug)]
pub struct ArchivedTree {
    /// Lowercase hex SHA-256 of the archive bytes.
    pub digest: String,
    /// Archive size in bytes.
    pub bytes: u64,
    /// The archive on disk, removed when dropped.
    pub archive: tempfile::NamedTempFile,
    /// Regular files the archive carries, as the manifest reports them.
    pub files: u64,
}

/// The read-only volume name for a tree `digest`.
pub fn volume_name(digest: &str) -> String {
    format!("{VOLUME_PREFIX}{}", &digest[..NAME_DIGEST_HEX])
}

/// The exclusive staging volume name for one populate `attempt`.
pub fn staging_name(digest: &str, attempt: &str) -> String {
    format!("{STAGING_PREFIX}{}-{attempt}", &digest[..NAME_DIGEST_HEX])
}

/// Whether `name` is a content-addressed read-only volume name.
pub fn is_volume_name(name: &str) -> bool {
    name.strip_prefix(VOLUME_PREFIX).is_some_and(|digest| {
        digest.len() == NAME_DIGEST_HEX
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

/// Whether `name` belongs to this module's naming scheme at all: final
/// volumes and populate leftovers alike.
pub fn is_runner_volume_name(name: &str) -> bool {
    name.starts_with(VOLUME_PREFIX)
}

/// What kind of tree a volume holds, for `preloop volumes list`.
///
/// Every volume this module names today holds the runner bundle; a staging
/// leftover is one still being written. Later kinds get their own prefix and
/// their own arm here.
pub fn volume_kind(name: &str) -> &'static str {
    if name.starts_with(STAGING_PREFIX) {
        "runner-bundle (staging)"
    } else {
        "runner-bundle"
    }
}

/// The volume size, in MiB, for an archive of `bytes`.
///
/// The fork that becomes the read-only volume must match its source's size, so
/// the populate step and the fork both use this value.
pub fn volume_size_mb(bytes: u64) -> Result<u64, VmError> {
    let data_mib = bytes.div_ceil(MIB);
    let wanted = (data_mib + data_mib / 2 + VOLUME_HEADROOM_MIB).max(MIN_VOLUME_MIB);
    if wanted > MAX_VOLUME_MIB {
        return Err(VmError::InvalidSpec(format!(
            "runner tree archives to {bytes} bytes, which needs a {wanted} MiB volume; \
             AgentENV's default limit is {MAX_VOLUME_MIB} MiB"
        )));
    }
    Ok(wanted)
}

/// Archive `directory` deterministically, hashing the bytes as they are written,
/// and stamp `manifest` into the archive's [`MANIFEST_ENTRY`] entry.
///
/// The archive that is hashed is the archive that is uploaded: the digest names
/// exactly the bytes a volume is populated from, even if the tree changes after
/// this call. Runs on the blocking pool because it reads the whole tree.
pub async fn archive_tree(
    directory: &Path,
    manifest: &BundleManifest,
) -> Result<ArchivedTree, VmError> {
    let directory = directory.to_path_buf();
    let manifest = manifest.clone();
    tokio::task::spawn_blocking(move || archive_tree_blocking(&directory, &manifest))
        .await
        .map_err(|error| VmError::Protocol(format!("runner tree archive task failed: {error}")))?
}

/// A cheap fingerprint of `directory` under `manifest`.
///
/// The provider remembers which volume a tree resolved to; a fingerprint that
/// still matches says the tree has not been rebuilt or edited, so the remembered
/// volume can be reused without reading every byte again. It covers the same
/// entries [`archive_tree`] does — names, types, permission bits, sizes,
/// modification times, symlink targets — plus the version pins, so a changed pin
/// publishes a new volume even when the tree is byte-identical.
///
/// It is not a content hash: a file edited in place keeps its size and changes
/// its mtime, and one whose bytes changed without any metadata changing keeps the
/// volume it had until something else about the tree changes.
pub async fn fingerprint_tree(
    directory: &Path,
    manifest: &BundleManifest,
) -> Result<String, VmError> {
    let directory = directory.to_path_buf();
    let manifest = manifest.clone();
    tokio::task::spawn_blocking(move || fingerprint_tree_blocking(&directory, &manifest))
        .await
        .map_err(|error| {
            VmError::Protocol(format!("runner tree fingerprint task failed: {error}"))
        })?
}

fn fingerprint_tree_blocking(
    directory: &Path,
    manifest: &BundleManifest,
) -> Result<String, VmError> {
    let mut hasher = Sha256::new();
    manifest.hash_into(&mut hasher);
    hasher.update([0u8]);
    walk_directory(directory, Path::new(""), &mut |path, entry, metadata| {
        let file_type = metadata.file_type();
        hasher.update(entry.as_os_str().as_encoded_bytes());
        hasher.update([0u8]);
        hasher.update(kind_of(file_type));
        hasher.update([0u8]);
        hasher.update(archived_mode(file_type, metadata).to_le_bytes());
        hasher.update(metadata.len().to_le_bytes());
        if let Some(modified) = metadata.modified().ok().and_then(|time| {
            time.duration_since(std::time::SystemTime::UNIX_EPOCH)
                .ok()
                .map(|since| since.as_nanos())
        }) {
            hasher.update(modified.to_le_bytes());
        }
        if file_type.is_symlink() {
            let target = fs::read_link(path).map_err(|source| VmError::RuntimeIo { source })?;
            check_symlink_target(path, entry, &target)?;
            hasher.update([0u8]);
            hasher.update(target.as_os_str().as_encoded_bytes());
        }
        Ok(())
    })?;
    Ok(hex::encode(hasher.finalize()))
}

/// The kind of entry a fingerprint distinguishes.
fn kind_of(file_type: fs::FileType) -> &'static [u8] {
    if file_type.is_symlink() {
        b"symlink"
    } else if file_type.is_dir() {
        b"dir"
    } else {
        b"file"
    }
}

/// What the traversal counted for the manifest.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct TreeStats {
    files: u64,
    bytes: u64,
}

fn archive_tree_blocking(
    directory: &Path,
    manifest: &BundleManifest,
) -> Result<ArchivedTree, VmError> {
    let archive = tempfile::Builder::new()
        .prefix("preloop-runner-tree-")
        .suffix(".tar")
        .tempfile()
        .map_err(|source| VmError::RuntimeIo { source })?;
    let file = archive
        .reopen()
        .map_err(|source| VmError::RuntimeIo { source })?;
    let mut builder = tar::Builder::new(HashingWriter::new(io::BufWriter::new(file)));
    let stats = append_directory(&mut builder, directory, Path::new(""))?;
    append_manifest(&mut builder, manifest, stats)?;
    builder
        .finish()
        .map_err(|source| VmError::RuntimeIo { source })?;
    let writer = builder
        .into_inner()
        .map_err(|source| VmError::RuntimeIo { source })?;
    let (digest, bytes) = writer
        .finish()
        .map_err(|source| VmError::RuntimeIo { source })?;
    Ok(ArchivedTree {
        digest,
        bytes,
        archive,
        files: stats.files,
    })
}

/// Append the synthetic manifest entry, after the tree it describes.
///
/// The entry is written into the archive only — the host bundle directory is
/// untouched — and it is part of the digest, so a tree archived with different
/// version pins is a different volume.
fn append_manifest<W: Write>(
    builder: &mut tar::Builder<W>,
    manifest: &BundleManifest,
    stats: TreeStats,
) -> Result<(), VmError> {
    let body = manifest.to_bytes(stats.files, stats.bytes);
    let mut header = normalized_header(tar::EntryType::Regular, 0o644, body.len() as u64);
    builder
        .append_data(&mut header, MANIFEST_ENTRY, io::Cursor::new(body))
        .map_err(|source| VmError::RuntimeIo { source })
}

/// Append `root/relative` depth-first, children sorted by name.
///
/// Symlinks are stored as links, never followed, and only when they stay inside
/// the tree (see [`check_symlink_target`]); anything that is neither a file, a
/// directory, nor a symlink (sockets, FIFOs, devices) is refused by
/// [`walk_directory`], because a guest could not use it and silently dropping it
/// would change the tree.
fn append_directory<W: Write>(
    builder: &mut tar::Builder<W>,
    root: &Path,
    relative: &Path,
) -> Result<TreeStats, VmError> {
    let mut stats = TreeStats::default();
    walk_directory(root, relative, &mut |path, entry, metadata| {
        let file_type = metadata.file_type();
        if file_type.is_symlink() {
            let target = fs::read_link(path).map_err(|source| VmError::RuntimeIo { source })?;
            check_symlink_target(path, entry, &target)?;
            let mut header = normalized_header(tar::EntryType::Symlink, 0o777, 0);
            builder
                .append_link(&mut header, entry, &target)
                .map_err(|source| VmError::RuntimeIo { source })?;
        } else if file_type.is_dir() {
            let mut header = normalized_header(
                tar::EntryType::Directory,
                archived_mode(file_type, metadata),
                0,
            );
            builder
                .append_data(&mut header, entry, io::empty())
                .map_err(|source| VmError::RuntimeIo { source })?;
        } else {
            stats.files += 1;
            stats.bytes += metadata.len();
            let file = File::open(path).map_err(|source| VmError::RuntimeIo { source })?;
            let mut header = normalized_header(
                tar::EntryType::Regular,
                archived_mode(file_type, metadata),
                metadata.len(),
            );
            builder
                .append_data(&mut header, entry, file)
                .map_err(|source| VmError::RuntimeIo { source })?;
        }
        Ok(())
    })?;
    Ok(stats)
}

/// Walk `root/relative` depth-first, children sorted by name, handing every entry
/// to `visit` in the order its header is written: a directory before its
/// children, siblings by name.
///
/// One traversal serves both the archive and the cheap fingerprint, so the two
/// accept and refuse exactly the same trees.
fn walk_directory(
    root: &Path,
    relative: &Path,
    visit: &mut impl FnMut(&Path, &Path, &fs::Metadata) -> Result<(), VmError>,
) -> Result<(), VmError> {
    let directory = root.join(relative);
    let mut names: Vec<OsString> = fs::read_dir(&directory)
        .map_err(|source| VmError::RuntimeIo { source })?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<io::Result<_>>()
        .map_err(|source| VmError::RuntimeIo { source })?;
    names.sort();
    for name in names {
        let path = directory.join(&name);
        let entry = relative.join(&name);
        let metadata =
            fs::symlink_metadata(&path).map_err(|source| VmError::RuntimeIo { source })?;
        let file_type = metadata.file_type();
        if !file_type.is_symlink() && !file_type.is_dir() && !file_type.is_file() {
            return Err(VmError::InvalidSpec(format!(
                "runner tree contains an entry a guest cannot use: {}",
                path.display()
            )));
        }
        visit(&path, &entry, &metadata)?;
        if file_type.is_dir() {
            walk_directory(root, &entry, visit)?;
        }
    }
    Ok(())
}

/// Refuse a symlink whose target would leave the archived tree.
///
/// The target is resolved against the link's own directory, so a link that
/// climbs out of the tree with `..` is refused exactly like an absolute one. A
/// read-only volume is mounted into a sandbox, and a link that points outside it
/// is followed against the guest's own filesystem, where a job could turn it
/// into a path the volume was never meant to expose.
///
/// Checking every link while the tree is archived is what makes the guarantee
/// hold for a chain of links too: whichever link in the chain points outside is
/// refused, so no volume is ever published with a link that leaves it.
fn check_symlink_target(link: &Path, entry: &Path, target: &Path) -> Result<(), VmError> {
    let escape = || {
        VmError::InvalidSpec(format!(
            "runner tree contains a symlink that leaves the tree: {} -> {}",
            link.display(),
            target.display()
        ))
    };
    if target.is_absolute() {
        return Err(escape());
    }
    let mut resolved = PathBuf::new();
    for component in entry
        .parent()
        .unwrap_or(Path::new(""))
        .join(target)
        .components()
    {
        match component {
            Component::Normal(part) => resolved.push(part),
            Component::CurDir => {}
            Component::ParentDir => {
                if !resolved.pop() {
                    return Err(escape());
                }
            }
            Component::RootDir | Component::Prefix(_) => return Err(escape()),
        }
    }
    Ok(())
}

/// A tar header carrying only what a guest can observe: type, permission bits
/// and size. Owner, group and mtime are zeroed so they cannot change the digest.
fn normalized_header(entry: tar::EntryType, mode: u32, size: u64) -> tar::Header {
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(entry);
    header.set_mode(mode);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_size(size);
    header
}

/// The permission bits a guest sees for `metadata`.
///
/// A symlink takes a fixed mode, because its own mode differs between hosts.
fn archived_mode(file_type: fs::FileType, metadata: &fs::Metadata) -> u32 {
    if file_type.is_symlink() {
        0o777
    } else {
        mode_of(metadata)
    }
}

/// The permission bits a guest sees, at most `0o777`.
///
/// The set-user-ID, set-group-ID and sticky bits are masked out. A volume is
/// mounted into a sandbox, so preserving them would hand a guest a setuid binary
/// (or a sticky directory) taken from the host's own bundle, and neither the
/// runner nor the Node externals ships one — nothing in a runner tree needs to
/// escalate to its owner, and the mounts are read-only anyway. Masking also
/// keeps the digest stable across hosts that disagree about those bits on
/// directories. A tree that had one of them publishes a new volume, which the
/// next `prune` reclaims.
#[cfg(unix)]
fn mode_of(metadata: &fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o777
}

#[cfg(not(unix))]
fn mode_of(metadata: &fs::Metadata) -> u32 {
    if metadata.is_dir() { 0o755 } else { 0o644 }
}

/// A writer that hashes and counts every byte it passes through.
struct HashingWriter<W: Write> {
    inner: W,
    hash: Sha256,
    bytes: u64,
}

impl<W: Write> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hash: Sha256::new(),
            bytes: 0,
        }
    }

    /// Flush, then return the lowercase hex digest and byte count.
    fn finish(mut self) -> io::Result<(String, u64)> {
        self.inner.flush()?;
        Ok((hex::encode(self.hash.finalize()), self.bytes))
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.hash.update(&buf[..written]);
        self.bytes += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pins a test archives with.
    fn manifest() -> BundleManifest {
        BundleManifest::new("2.336.0", "20.20.2", "24.19.0")
    }

    fn archive_of(directory: &Path) -> ArchivedTree {
        archive_tree_blocking(directory, &manifest()).unwrap()
    }

    fn fingerprint_of(directory: &Path) -> String {
        fingerprint_tree_blocking(directory, &manifest()).unwrap()
    }

    fn manifest_body(archive: &Path) -> String {
        let mut tar = tar::Archive::new(File::open(archive).unwrap());
        let mut body = String::new();
        for entry in tar.entries().unwrap() {
            let mut entry = entry.unwrap();
            if entry.path().unwrap() == Path::new(MANIFEST_ENTRY) {
                use std::io::Read as _;
                entry.read_to_string(&mut body).unwrap();
            }
        }
        body
    }

    fn entries(archive: &Path) -> Vec<(String, tar::EntryType, Option<String>)> {
        let mut out = Vec::new();
        let mut tar = tar::Archive::new(File::open(archive).unwrap());
        for entry in tar.entries().unwrap() {
            let entry = entry.unwrap();
            let name = entry.path().unwrap().to_string_lossy().into_owned();
            let link = entry
                .link_name()
                .unwrap()
                .map(|target| target.to_string_lossy().into_owned());
            out.push((name, entry.header().entry_type(), link));
        }
        out
    }

    fn modes_of(archive: &Path) -> Vec<(String, u32)> {
        let mut out = Vec::new();
        let mut tar = tar::Archive::new(File::open(archive).unwrap());
        for entry in tar.entries().unwrap() {
            let entry = entry.unwrap();
            out.push((
                entry.path().unwrap().to_string_lossy().into_owned(),
                entry.header().mode().unwrap(),
            ));
        }
        out
    }

    #[test]
    fn the_manifest_is_stamped_into_the_archive_and_is_deterministic() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        fs::create_dir_all(root.join("externals/node24/bin")).unwrap();
        fs::write(root.join("preloop-runner"), b"ELF").unwrap();
        fs::write(root.join("externals/node24/bin/node"), b"#!/bin/sh\n").unwrap();

        let first = archive_of(root);
        let second = archive_of(root);
        assert_eq!(first.digest, second.digest);
        assert_eq!(
            manifest_body(first.archive.path()),
            manifest_body(second.archive.path())
        );
        assert_eq!(
            manifest_body(first.archive.path()),
            "{\"bundle_bytes\":13,\"bundle_files\":2,\"node_externals\":\
             {\"node20\":\"20.20.2\",\"node24\":\"24.19.0\"},\
             \"runner_version\":\"2.336.0\",\"schema\":1}\n"
        );
        // The manifest is a synthetic entry: the host tree never grows a file,
        // and the entry is the last one, after the tree it describes.
        assert!(!root.join(MANIFEST_ENTRY).exists());
        assert_eq!(
            entries(first.archive.path())
                .last()
                .map(|(name, ..)| name.clone()),
            Some(MANIFEST_ENTRY.to_owned())
        );
        assert_eq!(first.files, 2);
    }

    #[test]
    fn a_changed_version_pin_publishes_a_different_volume() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        fs::write(root.join("preloop-runner"), b"ELF").unwrap();

        let same_tree = archive_tree_blocking(root, &manifest()).unwrap();
        let newer_runner = BundleManifest::new("2.337.0", "20.20.2", "24.19.0");
        let changed = archive_tree_blocking(root, &newer_runner).unwrap();
        assert_ne!(same_tree.digest, changed.digest);
        assert!(
            manifest_body(changed.archive.path()).contains("\"runner_version\":\"2.337.0\""),
            "{}",
            manifest_body(changed.archive.path())
        );
        // The fingerprint takes the pins too, so a memoised volume is not reused
        // for a build with a different pin.
        assert_ne!(
            fingerprint_tree_blocking(root, &manifest()).unwrap(),
            fingerprint_tree_blocking(root, &newer_runner).unwrap()
        );
    }

    #[test]
    fn an_unknown_version_is_recorded_as_unknown_instead_of_failing() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        fs::write(root.join("preloop-runner"), b"ELF").unwrap();
        let incomplete = BundleManifest::new("", "20.20.2", "");
        let archived = archive_tree_blocking(root, &incomplete).unwrap();
        assert_eq!(
            manifest_body(archived.archive.path()),
            "{\"bundle_bytes\":3,\"bundle_files\":1,\"node_externals\":\
             {\"node20\":\"20.20.2\",\"node24\":\"unknown\"},\
             \"runner_version\":\"unknown\",\"schema\":1}\n"
        );
        assert!(!incomplete.is_complete());
    }

    #[test]
    fn a_symlink_that_leaves_the_tree_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        fs::create_dir_all(root.join("externals/node20/bin")).unwrap();
        // What the real bundle carries: a relative link that stays in the tree.
        fs::create_dir_all(root.join("externals/node20/lib/node_modules/npm/bin")).unwrap();
        fs::write(
            root.join("externals/node20/lib/node_modules/npm/bin/npm-cli.js"),
            b"// npm\n",
        )
        .unwrap();
        std::os::unix::fs::symlink(
            "../lib/node_modules/npm/bin/npm-cli.js",
            root.join("externals/node20/bin/npm"),
        )
        .unwrap();
        let archived = archive_of(root);
        assert!(
            entries(archived.archive.path())
                .iter()
                .any(|(name, kind, link)| {
                    name == "externals/node20/bin/npm"
                        && kind.is_symlink()
                        && link.as_deref() == Some("../lib/node_modules/npm/bin/npm-cli.js")
                }),
            "{:?}",
            entries(archived.archive.path())
        );

        // An absolute target is refused, and names the link and its target.
        std::os::unix::fs::symlink("/etc/passwd", root.join("escape")).unwrap();
        let error = archive_tree_blocking(root, &manifest()).unwrap_err();
        assert!(
            matches!(&error, VmError::InvalidSpec(message)
                if message.contains("escape") && message.contains("/etc/passwd")),
            "{error:?}"
        );
        // The fingerprint refuses the same tree, so a provider cannot memoise it.
        assert!(fingerprint_tree_blocking(root, &manifest()).is_err());
        fs::remove_file(root.join("escape")).unwrap();

        // So is a relative target that climbs out with `..`.
        fs::create_dir_all(root.join("bin/deeper")).unwrap();
        std::os::unix::fs::symlink("../../../etc/passwd", root.join("bin/deeper/up")).unwrap();
        let error = archive_tree_blocking(root, &manifest()).unwrap_err();
        assert!(
            matches!(&error, VmError::InvalidSpec(message) if message.contains("bin/deeper/up")),
            "{error:?}"
        );
        fs::remove_file(root.join("bin/deeper/up")).unwrap();

        // A target that only steps to the tree root is still inside it.
        std::os::unix::fs::symlink("../../preloop-runner-link-src", root.join("bin/deeper/up"))
            .unwrap();
        fs::write(root.join("preloop-runner-link-src"), b"ELF").unwrap();
        assert!(archive_tree_blocking(root, &manifest()).is_ok());
    }

    #[test]
    fn set_user_id_and_sticky_bits_are_masked_out() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let setuid = root.join("preloop-runner");
        fs::write(&setuid, b"ELF").unwrap();
        fs::set_permissions(&setuid, fs::Permissions::from_mode(0o4755)).unwrap();
        let setgid_dir = root.join("externals");
        fs::create_dir_all(&setgid_dir).unwrap();
        fs::set_permissions(&setgid_dir, fs::Permissions::from_mode(0o2755)).unwrap();

        let archived = archive_of(root);
        let modes = modes_of(archived.archive.path());
        assert!(
            modes
                .iter()
                .any(|(name, mode)| name == "preloop-runner" && *mode == 0o755),
            "{modes:?}"
        );
        assert!(
            modes
                .iter()
                .any(|(name, mode)| name == "externals" && *mode == 0o755),
            "{modes:?}"
        );
        // The host tree is untouched: the bits are only masked in the archive.
        assert_eq!(
            fs::metadata(&setuid).unwrap().permissions().mode() & 0o7777,
            0o4755
        );
    }

    #[test]
    fn a_plain_tree_without_a_manifest_still_archives() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        fs::write(root.join("preloop-runner"), b"ELF").unwrap();
        let archived = archive_of(root);
        assert!(archived.bytes > 0);
        assert_eq!(archived.files, 1);
        assert_eq!(fingerprint_of(root).len(), 64);
        // An empty tree archives too, and still carries its manifest.
        let empty = tempfile::tempdir().unwrap();
        let archived = archive_of(empty.path());
        assert_eq!(archived.files, 0);
        assert!(manifest_body(archived.archive.path()).contains("\"bundle_files\":0"));
    }

    #[test]
    fn the_name_is_a_prefix_and_twelve_hex_digits_of_the_digest() {
        let digest = "0123456789abcdef".repeat(4);
        assert_eq!(volume_name(&digest), "preloop-runner-0123456789ab");
        assert!(is_volume_name("preloop-runner-0123456789ab"));
        assert!(!is_volume_name("preloop-runner-0123456789AB"));
        assert!(!is_volume_name(
            "preloop-runner-stage-0123456789ab-1f2e3d4c"
        ));
        assert!(!is_volume_name("other-0123456789ab"));
        assert!(is_runner_volume_name(
            "preloop-runner-stage-0123456789ab-1f2e3d4c"
        ));
        assert!(!is_runner_volume_name("preloop-state-0"));
    }

    #[test]
    fn the_digest_ignores_timestamps_and_ownership_but_not_content_or_modes() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        fs::write(root.join("preloop-runner"), b"ELF-one").unwrap();
        std::os::unix::fs::symlink("preloop-runner", root.join("runner-link")).unwrap();
        let first = archive_of(root);

        // A later mtime changes nothing a guest can observe.
        File::options()
            .write(true)
            .open(root.join("preloop-runner"))
            .unwrap()
            .set_modified(std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1))
            .unwrap();
        assert_eq!(archive_of(root).digest, first.digest);

        // Content, permission bits, and link targets all do.
        fs::write(root.join("preloop-runner"), b"ELF-two").unwrap();
        let changed_content = archive_of(root).digest;
        assert_ne!(changed_content, first.digest);
        fs::write(root.join("preloop-runner"), b"ELF-one").unwrap();
        assert_eq!(archive_of(root).digest, first.digest);
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(
                root.join("preloop-runner"),
                fs::Permissions::from_mode(0o700),
            )
            .unwrap();
        }
        assert_ne!(archive_of(root).digest, first.digest);
        fs::remove_file(root.join("runner-link")).unwrap();
        std::os::unix::fs::symlink("elsewhere", root.join("runner-link")).unwrap();
        assert_ne!(archive_of(root).digest, first.digest);
    }

    #[test]
    fn symlinks_stay_links_and_modes_survive_the_archive() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        fs::create_dir_all(root.join("externals/node24/bin")).unwrap();
        fs::write(root.join("externals/node24/bin/node"), b"#!/bin/sh\n").unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(
                root.join("externals/node24/bin/node"),
                fs::Permissions::from_mode(0o755),
            )
            .unwrap();
        }
        std::os::unix::fs::symlink("node24/bin/node", root.join("externals/node")).unwrap();
        let archived = archive_of(root);

        let listed = entries(archived.archive.path());
        assert!(
            listed.iter().any(|(name, kind, link)| {
                name == "externals/node"
                    && kind.is_symlink()
                    && link.as_deref() == Some("node24/bin/node")
            }),
            "{listed:?}"
        );
        assert!(
            listed
                .iter()
                .any(|(name, kind, _)| { name == "externals/node24/bin" && kind.is_dir() }),
            "{listed:?}"
        );

        let mut archive = tar::Archive::new(File::open(archived.archive.path()).unwrap());
        let mut modes = Vec::new();
        for entry in archive.entries().unwrap() {
            let entry = entry.unwrap();
            if entry.path().unwrap().ends_with("node") && entry.header().entry_type().is_file() {
                modes.push(entry.header().mode().unwrap());
            }
        }
        assert_eq!(modes, vec![0o755]);
        assert!(archived.bytes > 0);
    }

    #[test]
    fn a_socket_in_the_tree_is_refused_instead_of_dropped() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("control.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let error = match archive_tree_blocking(directory.path(), &manifest()) {
            Ok(_) => panic!("a socket cannot be represented in a volume"),
            Err(error) => error,
        };
        assert!(
            matches!(&error, VmError::InvalidSpec(message) if message.contains("control.sock")),
            "{error:?}"
        );
    }

    #[test]
    fn sizes_leave_headroom_and_never_fall_below_the_minimum() {
        assert_eq!(volume_size_mb(0).unwrap(), 1024);
        // The real bundle archives to roughly 430 MB.
        assert_eq!(volume_size_mb(430 * MIB).unwrap(), 1024);
        assert_eq!(volume_size_mb(2048 * MIB).unwrap(), 3328);
        assert!(matches!(
            volume_size_mb(MAX_VOLUME_MIB * MIB),
            Err(VmError::InvalidSpec(_))
        ));
    }
}
