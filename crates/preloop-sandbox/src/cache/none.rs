//! The documented no-cache backend.

use async_trait::async_trait;

use super::{CacheEntry, CacheError, CacheKey, SharedCache};

/// A cache that stores nothing, on purpose.
///
/// Every lookup misses and every read then goes to the authoritative
/// database. This is the `none` configuration, not a degraded or fake
/// in-memory cache: it exists so an operator can prove the service is
/// correct without any cache, and so an embedder can disable acceleration
/// without changing the read path.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoCache;

#[async_trait]
impl SharedCache for NoCache {
    async fn get(&self, _key: &CacheKey) -> Result<Option<CacheEntry>, CacheError> {
        Ok(None)
    }

    async fn put(&self, _key: &CacheKey, _entry: CacheEntry) -> Result<(), CacheError> {
        Ok(())
    }

    async fn invalidate(&self, _key: &CacheKey) -> Result<(), CacheError> {
        Ok(())
    }

    async fn clear(&self) -> Result<(), CacheError> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "none"
    }
}
