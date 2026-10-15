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

use crate::VmError;
use sha2::{Digest, Sha256};
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

/// Prefix of every volume this module names. Pruning is limited to it.
pub const VOLUME_PREFIX: &str = "preloop-runner-";
/// Prefix of the exclusive volume a populate run writes before it is frozen.
pub const STAGING_PREFIX: &str = "preloop-runner-stage-";
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

/// A host tree archived for population.
pub struct ArchivedTree {
    /// Lowercase hex SHA-256 of the archive bytes.
    pub digest: String,
    /// Archive size in bytes.
    pub bytes: u64,
    /// The archive on disk, removed when dropped.
    pub archive: tempfile::NamedTempFile,
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

/// Archive `directory` deterministically, hashing the bytes as they are written.
///
/// The archive that is hashed is the archive that is uploaded: the digest names
/// exactly the bytes a volume is populated from, even if the tree changes after
/// this call. Runs on the blocking pool because it reads the whole tree.
pub async fn archive_tree(directory: &Path) -> Result<ArchivedTree, VmError> {
    let directory = directory.to_path_buf();
    tokio::task::spawn_blocking(move || archive_tree_blocking(&directory))
        .await
        .map_err(|error| VmError::Protocol(format!("runner tree archive task failed: {error}")))?
}

fn archive_tree_blocking(directory: &Path) -> Result<ArchivedTree, VmError> {
    let archive = tempfile::Builder::new()
        .prefix("preloop-runner-tree-")
        .suffix(".tar")
        .tempfile()
        .map_err(|source| VmError::RuntimeIo { source })?;
    let file = archive
        .reopen()
        .map_err(|source| VmError::RuntimeIo { source })?;
    let mut builder = tar::Builder::new(HashingWriter::new(io::BufWriter::new(file)));
    append_directory(&mut builder, directory, Path::new(""))?;
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
    })
}

/// Append `root/relative` depth-first, children sorted by name.
///
/// Symlinks are stored as links, never followed; anything that is neither a
/// file, a directory, nor a symlink (sockets, FIFOs, devices) is refused, because
/// a guest could not use it and silently dropping it would change the tree.
fn append_directory<W: Write>(
    builder: &mut tar::Builder<W>,
    root: &Path,
    relative: &Path,
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
        if file_type.is_symlink() {
            let target = fs::read_link(&path).map_err(|source| VmError::RuntimeIo { source })?;
            let mut header = normalized_header(tar::EntryType::Symlink, 0o777, 0);
            builder
                .append_link(&mut header, &entry, &target)
                .map_err(|source| VmError::RuntimeIo { source })?;
        } else if file_type.is_dir() {
            let mut header = normalized_header(tar::EntryType::Directory, mode_of(&metadata), 0);
            builder
                .append_data(&mut header, &entry, io::empty())
                .map_err(|source| VmError::RuntimeIo { source })?;
            append_directory(builder, root, &entry)?;
        } else if file_type.is_file() {
            let file = File::open(&path).map_err(|source| VmError::RuntimeIo { source })?;
            let mut header =
                normalized_header(tar::EntryType::Regular, mode_of(&metadata), metadata.len());
            builder
                .append_data(&mut header, &entry, file)
                .map_err(|source| VmError::RuntimeIo { source })?;
        } else {
            return Err(VmError::InvalidSpec(format!(
                "runner tree contains an entry a guest cannot use: {}",
                path.display()
            )));
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

/// The permission bits a guest sees. Symlinks take a fixed mode in
/// [`append_directory`], because their own mode differs between hosts.
#[cfg(unix)]
fn mode_of(metadata: &fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o7777
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

    fn archive_of(directory: &Path) -> ArchivedTree {
        archive_tree_blocking(directory).unwrap()
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
        let error = match archive_tree_blocking(directory.path()) {
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
