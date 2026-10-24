//! Git LFS content-addressed object store.
//!
//! Objects are addressed by the SHA-256 of their bytes, so a published object
//! is immutable: a later upload with the same id must have the same content,
//! and verification before publish is what makes that true rather than
//! assumed.

use std::path::{Path, PathBuf};

use crate::MirrorError;

/// Whether `oid` is a well-formed Git LFS object id (lowercase or uppercase
/// SHA-256 hex).
pub fn is_valid_oid(oid: &str) -> bool {
    oid.len() == 64 && oid.chars().all(|ch| ch.is_ascii_hexdigit())
}

/// Path of one object inside a mirror's LFS store.
///
/// The store lives in the repository so the object shares the repository's
/// lifecycle, and is sharded two levels deep so one directory never holds
/// every blob. Callers must validate the oid with [`is_valid_oid`] first;
/// this function has no traversal surface of its own (it only ever appends
/// hex characters), but an unvalidated id would create a misnamed entry.
pub fn object_path(repository: &Path, oid: &str) -> PathBuf {
    let (prefix, rest) = oid.split_at(2.min(oid.len()));
    let (middle, leaf) = rest.split_at(2.min(rest.len()));
    repository
        .join("lfs")
        .join("objects")
        .join(prefix)
        .join(middle)
        .join(leaf)
}

/// Extract the oid from a request path such as `info/lfs/objects/<oid>`.
///
/// Both the snapshot form and the in-worktree `.git/` form occur, because Git
/// addresses the same object store through whichever URL the remote uses.
pub fn oid_from_request_path(path: &str) -> Option<&str> {
    let oid = path
        .strip_prefix("info/lfs/objects/")
        .or_else(|| path.strip_prefix(".git/info/lfs/objects/"))?;
    oid.split('/').next().filter(|oid| is_valid_oid(oid))
}

/// SHA-256 of `bytes` as lowercase hex.
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    format!("{:x}", sha2::Sha256::digest(bytes))
}

/// Whether `bytes` hash to `expected_oid`.
pub fn bytes_match(oid: &str, bytes: &[u8]) -> bool {
    sha256_hex(bytes).eq_ignore_ascii_case(oid)
}

/// Verify a fully written staging file against its claimed object id.
///
/// Returns `Ok(false)` when the content does not hash to `expected_oid`, which
/// is the only answer a caller may act on: a mismatched object must be
/// discarded, never published.
pub async fn verify_staged_file(path: &Path, expected_oid: &str) -> Result<bool, MirrorError> {
    use sha2::Digest;
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|error| MirrorError::io("read staged LFS object", &error))?;
    let actual = format!("{:x}", sha2::Sha256::digest(&bytes));
    Ok(actual.eq_ignore_ascii_case(expected_oid))
}

/// Publish a verified staging file at `destination`.
///
/// `rename` is same-filesystem and atomic, so a reader either sees the whole
/// object or none of it — never a partial blob that a later batch would
/// advertise as present.
pub async fn publish_staged(tmp: &Path, destination: &Path) -> Result<(), MirrorError> {
    if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| MirrorError::io("create LFS object directory", &error))?;
    }
    tokio::fs::rename(tmp, destination)
        .await
        .map_err(|error| MirrorError::io("publish LFS object", &error))
}

/// Whether an object is present and non-empty.
pub async fn object_present(path: &Path) -> bool {
    tokio::fs::metadata(path)
        .await
        .is_ok_and(|metadata| metadata.is_file())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_ids_are_strict_hex() {
        assert!(is_valid_oid(&"a".repeat(64)));
        assert!(is_valid_oid(&"A".repeat(64)));
        assert!(!is_valid_oid(&"a".repeat(63)));
        assert!(!is_valid_oid(&"a".repeat(65)));
        assert!(!is_valid_oid(&format!("{}g", "a".repeat(63))));
        assert!(!is_valid_oid("../escape"));
    }

    #[test]
    fn object_paths_are_sharded_and_stay_inside_the_store() {
        let repository = Path::new("/state/mirror.git");
        let oid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let path = object_path(repository, oid);
        assert_eq!(
            path,
            repository.join(
                "lfs/objects/01/23/456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            )
        );
        assert!(path.starts_with(repository.join("lfs/objects")));
    }

    #[test]
    fn request_paths_yield_only_valid_ids() {
        let oid = "b".repeat(64);
        assert_eq!(
            oid_from_request_path(&format!("info/lfs/objects/{oid}")),
            Some(oid.as_str())
        );
        assert_eq!(
            oid_from_request_path(&format!(".git/info/lfs/objects/{oid}/extra")),
            Some(oid.as_str())
        );
        assert_eq!(oid_from_request_path("info/lfs/objects/short"), None);
        assert_eq!(oid_from_request_path("info/lfs/objects/"), None);
    }

    #[tokio::test]
    async fn staged_bytes_are_verified_before_publish() {
        let temp = tempfile::tempdir().unwrap();
        let staging = temp.path().join("object.tmp");
        tokio::fs::write(&staging, b"golden-bytes").await.unwrap();
        let oid = sha256_hex(b"golden-bytes");
        assert!(verify_staged_file(&staging, &oid).await.unwrap());

        let tampered = sha256_hex(b"other-bytes");
        assert!(!verify_staged_file(&staging, &tampered).await.unwrap());

        let repository = temp.path().join("mirror.git");
        let destination = object_path(&repository, &oid);
        publish_staged(&staging, &destination).await.unwrap();
        assert!(object_present(&destination).await);
        assert!(!staging.exists());
    }
}
