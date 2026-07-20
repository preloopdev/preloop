//! Local cache storage compatible with GitHub Actions cache semantics.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use tokio::fs;

/// Cache storage error.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// I/O failed.
    #[error("cache io error: {0}")]
    Io(#[from] std::io::Error),
    /// Invalid cache key.
    #[error("invalid cache key: {0}")]
    InvalidKey(String),
    /// A cache with the same key and version already exists. GitHub caches are immutable.
    #[error("cache `{key}` with version `{version}` already exists")]
    AlreadyExists {
        /// Cache key.
        key: String,
        /// Cache version.
        version: String,
    },
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

    /// Save an immutable cache archive.
    ///
    /// Reference: GitHub Actions dependency caching documentation, “Cache key
    /// matching”, and `actions/toolkit/packages/cache/src/cache.ts::checkKey`.
    /// Keys are at most 512 Unicode scalar values, must be non-empty, and may
    /// not contain commas. The filesystem identity is a fixed SHA-256 digest;
    /// the original key/version are persisted as metadata for prefix matching.
    pub async fn put(
        &self,
        key: &str,
        version: &str,
        bytes: &[u8],
    ) -> Result<CacheEntry, CacheError> {
        validate_key(key, "Cache")?;
        let directory = self.entry_dir(key, version);
        match fs::create_dir(&directory).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(CacheError::AlreadyExists {
                    key: key.to_owned(),
                    version: version.to_owned(),
                });
            }
            Err(error) => return Err(error.into()),
        }

        let path = directory.join("archive.tzst");
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_string();
        if let Err(error) = async {
            fs::write(directory.join("key"), key).await?;
            fs::write(directory.join("version"), version).await?;
            fs::write(directory.join("created_at"), created_at).await?;
            fs::write(&path, bytes).await
        }
        .await
        {
            let _ = fs::remove_dir_all(&directory).await;
            return Err(error.into());
        }

        Ok(CacheEntry {
            key: key.to_owned(),
            version: version.to_owned(),
            path,
            size: bytes.len() as u64,
        })
    }

    /// Restore a cache using GitHub's lookup order: exact primary key, partial
    /// primary key, then each restore key in declaration order. Prefix matches
    /// select the newest matching immutable cache.
    pub async fn get(
        &self,
        key: &str,
        version: &str,
        restore_keys: &[String],
    ) -> Result<Option<(CacheEntry, Vec<u8>)>, CacheError> {
        validate_key(key, "Cache")?;
        if restore_keys.len() > 10 {
            return Err(CacheError::InvalidKey(format!(
                "at most 10 restore keys are supported, got {}",
                restore_keys.len()
            )));
        }
        for restore_key in restore_keys {
            validate_key(restore_key, "Restore")?;
        }

        let exact = self.path_for(key, version);
        let legacy = self.legacy_path_for(key, version);
        let exact_path = if fs::try_exists(&exact).await? {
            Some(exact)
        } else if fs::try_exists(&legacy).await? {
            Some(legacy)
        } else {
            None
        };
        if let Some(path) = exact_path {
            let bytes = fs::read(&path).await?;
            return Ok(Some((
                CacheEntry {
                    key: key.to_owned(),
                    version: version.to_owned(),
                    path,
                    size: bytes.len() as u64,
                },
                bytes,
            )));
        }

        for prefix in std::iter::once(key).chain(restore_keys.iter().map(String::as_str)) {
            if let Some(entry) = self.find_prefix(prefix, version).await? {
                let bytes = fs::read(&entry.path).await?;
                return Ok(Some((entry, bytes)));
            }
        }
        Ok(None)
    }

    fn entry_dir(&self, key: &str, version: &str) -> PathBuf {
        self.root.join(entry_id(key, version))
    }

    fn path_for(&self, key: &str, version: &str) -> PathBuf {
        self.entry_dir(key, version).join("archive.tzst")
    }
    fn legacy_path_for(&self, key: &str, version: &str) -> PathBuf {
        let key_component = hex(key.as_bytes());
        let mut version_hasher = Sha256::new();
        version_hasher.update(version.as_bytes());
        let version_hash = hex(version_hasher.finalize());
        let version_hash = &version_hash[..16.min(version_hash.len())];
        let mut identity = Sha256::new();
        identity.update(key.as_bytes());
        identity.update(b"\0");
        identity.update(version.as_bytes());
        self.root
            .join(format!(
                "{key_component}-{version_hash}-{}",
                hex(identity.finalize())
            ))
            .join("archive.tzst")
    }

    async fn find_prefix(
        &self,
        prefix: &str,
        version: &str,
    ) -> Result<Option<CacheEntry>, CacheError> {
        let mut directory = fs::read_dir(&self.root).await?;
        let mut newest: Option<(CacheEntry, u128)> = None;
        while let Some(candidate) = directory.next_entry().await? {
            let entry_dir = candidate.path();
            let path = entry_dir.join("archive.tzst");
            if !fs::try_exists(&path).await? {
                continue;
            }
            let (Ok(key), Ok(stored_version), Ok(created_at)) = (
                fs::read_to_string(entry_dir.join("key")).await,
                fs::read_to_string(entry_dir.join("version")).await,
                fs::read_to_string(entry_dir.join("created_at")).await,
            ) else {
                continue;
            };
            if stored_version != version || !key.starts_with(prefix) {
                continue;
            }
            let Ok(created_at) = created_at.parse::<u128>() else {
                continue;
            };
            let metadata = fs::metadata(&path).await?;
            let entry = CacheEntry {
                key,
                version: version.to_owned(),
                path,
                size: metadata.len(),
            };
            if newest
                .as_ref()
                .map(|(_, current)| created_at > *current)
                .unwrap_or(true)
            {
                newest = Some((entry, created_at));
            }
        }
        Ok(newest.map(|(entry, _)| entry))
    }
}

/// Match `actions/toolkit` JavaScript `String.length` semantics: cache keys
/// are limited to 512 UTF-16 code units, not Rust UTF-8 bytes.
fn validate_key(key: &str, kind: &str) -> Result<(), CacheError> {
    let utf16_length = key.encode_utf16().count();
    if key.is_empty() || utf16_length > 512 || key.contains(',') {
        return Err(CacheError::InvalidKey(format!(
            "{kind} key must be non-empty, at most 512 UTF-16 code units, and contain no commas"
        )));
    }
    Ok(())
}

fn entry_id(key: &str, version: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hasher.update(b"\0");
    hasher.update(version.as_bytes());
    hex(hasher.finalize())
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

    fn invalid_key() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("".to_owned()),
            prop::string::string_regex("[a-zA-Z0-9_-]{513,600}").unwrap(),
        ]
    }
    #[tokio::test]
    async fn stores_and_restores_exact_cache() {
        let temp = tempfile::tempdir().unwrap();
        let store = CacheStore::new(temp.path()).await.unwrap();

        store.put("linux-node", "v1", b"payload").await.unwrap();
        let (_entry, bytes) = store.get("linux-node", "v1", &[]).await.unwrap().unwrap();

        assert_eq!(bytes, b"payload");
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_cache_key_path_safety(ref key in "\\PC*", ref version in "\\PC*") {
            let temp = tempfile::tempdir().unwrap();
            let store = tokio::runtime::Runtime::new().unwrap().block_on(async {
                CacheStore::new(temp.path()).await.unwrap()
            });
            let path = store.path_for(key, version);
            assert!(is_under_root(temp.path(), &path));
        }

        #[test]
        fn test_cache_roundtrip(
            ref key in "[a-zA-Z0-9_-]{1,32}",
            ref version in "[a-zA-Z0-9_-]{1,32}",
            ref payload in prop::collection::vec(0..=255u8, 0..1024)
        ) {
            let temp = tempfile::tempdir().unwrap();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = CacheStore::new(temp.path()).await.unwrap();
                store.put(key, version, payload).await.unwrap();
                let (entry, restored) = store.get(key, version, &[]).await.unwrap().unwrap();
                assert_eq!(entry.key, *key);
                assert_eq!(entry.version, *version);
                assert_eq!(restored, *payload);
            });
        }

        #[test]
        fn test_cache_prefix_restore(
            ref key_prefix in "[a-zA-Z0-9_-]{5,10}",
            ref key_suffix1 in "[a-zA-Z0-9_-]{1,10}",
            ref key_suffix2 in "[a-zA-Z0-9_-]{1,10}",
            ref version in "[a-zA-Z0-9_-]{1,10}",
            ref payload1 in prop::collection::vec(0..=255u8, 0..100),
            ref payload2 in prop::collection::vec(0..=255u8, 0..100)
        ) {
            if key_suffix1 == key_suffix2 {
                return Ok(());
            }
            let temp = tempfile::tempdir().unwrap();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = CacheStore::new(temp.path()).await.unwrap();
                let key1 = format!("{}{}", key_prefix, key_suffix1);
                let key2 = format!("{}{}", key_prefix, key_suffix2);
                store.put(&key1, version, payload1).await.unwrap();
                store.put(&key2, version, payload2).await.unwrap();
                let restored = store.get("non-existent-key", version, std::slice::from_ref(key_prefix)).await.unwrap();
                let (entry, bytes) = restored.expect("prefix cache must resolve");
                assert_eq!(entry.key, key2);
                assert_eq!(bytes, *payload2);
            });
        }
        #[test]
        fn test_cache_invalid_keys(ref key in invalid_key(), ref version in "[a-zA-Z0-9_-]{1,32}") {
            let temp = tempfile::tempdir().unwrap();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = CacheStore::new(temp.path()).await.unwrap();
                let res = store.put(key, version, b"payload").await;
                assert!(matches!(res, Err(CacheError::InvalidKey(_))));

                let res_get = store.get(key, version, &[]).await;
                assert!(matches!(res_get, Err(CacheError::InvalidKey(_))));

                let res_get_rk = store.get("valid-key", version, std::slice::from_ref(key)).await;
                assert!(matches!(res_get_rk, Err(CacheError::InvalidKey(_))));
            });
        }
    }
}
