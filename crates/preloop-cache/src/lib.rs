//! Local cache storage compatible with GitHub Actions cache semantics.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tokio::fs;

/// Cache storage error.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// I/O failed.
    #[error("cache io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Cache entry metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntry {
    /// Cache key.
    pub key: String,
    /// Cache version.
    pub version: String,
    /// Archive path.
    pub path: PathBuf,
    /// Archive size in bytes.
    pub size: u64,
}

/// File-backed cache store.
#[derive(Debug, Clone)]
pub struct CacheStore {
    root: PathBuf,
}

impl CacheStore {
    /// Create a cache store rooted at `root`.
    pub async fn new(root: impl Into<PathBuf>) -> Result<Self, CacheError> {
        let root = root.into();
        fs::create_dir_all(&root).await?;
        Ok(Self { root })
    }

    /// Save a cache archive.
    pub async fn put(&self, key: &str, version: &str, bytes: &[u8]) -> Result<CacheEntry, CacheError> {
        let path = self.path_for(key, version);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(&path, bytes).await?;
        Ok(CacheEntry {
            key: key.to_owned(),
            version: version.to_owned(),
            path,
            size: bytes.len() as u64,
        })
    }

    /// Restore an exact key or the first matching restore prefix.
    pub async fn get(
        &self,
        key: &str,
        version: &str,
        restore_keys: &[String],
    ) -> Result<Option<(CacheEntry, Vec<u8>)>, CacheError> {
        let exact = self.path_for(key, version);
        if fs::try_exists(&exact).await? {
            let bytes = fs::read(&exact).await?;
            return Ok(Some((
                CacheEntry {
                    key: key.to_owned(),
                    version: version.to_owned(),
                    path: exact,
                    size: bytes.len() as u64,
                },
                bytes,
            )));
        }

        for restore_key in restore_keys {
            if let Some(entry) = self.find_prefix(restore_key, version).await? {
                let bytes = fs::read(&entry.path).await?;
                return Ok(Some((entry, bytes)));
            }
        }

        Ok(None)
    }

    fn path_for(&self, key: &str, version: &str) -> PathBuf {
        self.root.join(hash_name(key, version)).join("archive.tzst")
    }

    async fn find_prefix(&self, prefix: &str, version: &str) -> Result<Option<CacheEntry>, CacheError> {
        let mut dir = fs::read_dir(&self.root).await?;
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path().join("archive.tzst");
            if !fs::try_exists(&path).await? {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if name.starts_with(&hash_prefix(prefix)) && name.ends_with(&hash_suffix(version)) {
                let size = fs::metadata(&path).await?.len();
                return Ok(Some(CacheEntry {
                    key: prefix.to_owned(),
                    version: version.to_owned(),
                    path,
                    size,
                }));
            }
        }
        Ok(None)
    }
}

fn hash_name(key: &str, version: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hasher.update(b"\0");
    hasher.update(version.as_bytes());
    format!(
        "{}-{}-{}",
        hash_prefix(key),
        hash_suffix(version),
        hex(&hasher.finalize())
    )
}

fn hash_prefix(key: &str) -> String {
    hex(&Sha256::digest(key.as_bytes()))[..16].to_owned()
}

fn hash_suffix(version: &str) -> String {
    hex(&Sha256::digest(version.as_bytes()))[..16].to_owned()
}

fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Return true if a path is inside a configured root.
pub fn is_under_root(root: &Path, path: &Path) -> bool {
    path.starts_with(root)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stores_and_restores_exact_cache() {
        let temp = tempfile::tempdir().unwrap();
        let store = CacheStore::new(temp.path()).await.unwrap();

        store.put("linux-node", "v1", b"payload").await.unwrap();
        let (_entry, bytes) = store.get("linux-node", "v1", &[]).await.unwrap().unwrap();

        assert_eq!(bytes, b"payload");
    }
}
