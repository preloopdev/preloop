//! A cached value and its deadline.

use std::time::{Duration, SystemTime};

use bytes::Bytes;
use thiserror::Error;

/// One immutable, already-serialized cache value.
///
/// The entry carries an absolute expiry rather than a remaining TTL so an
/// external backend can enforce it (`EX`/`PX` in Redis terms) without a
/// round-trip clock. `Bytes` clones are reference-counted: a hit hands the
/// caller the same allocation the cache holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheEntry {
    value: Bytes,
    expires_at: SystemTime,
}

impl CacheEntry {
    /// Value that expires `ttl` from now.
    ///
    /// An overflowed deadline is treated as already expired rather than
    /// panicking; configured TTLs are far below the overflow range.
    pub fn new(value: impl Into<Bytes>, ttl: Duration) -> Self {
        let now = SystemTime::now();
        let expires_at = now.checked_add(ttl).unwrap_or(now);
        Self {
            value: value.into(),
            expires_at,
        }
    }

    /// Value with an explicit deadline (used when re-inserting a payload
    /// received from another layer).
    pub fn with_expiry(value: impl Into<Bytes>, expires_at: SystemTime) -> Self {
        Self {
            value: value.into(),
            expires_at,
        }
    }

    /// The immutable serialized payload.
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    /// The immutable serialized payload, as bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.value
    }

    /// Number of payload bytes held.
    pub fn len(&self) -> usize {
        self.value.len()
    }

    /// Whether the payload is empty.
    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    /// Absolute deadline.
    pub fn expires_at(&self) -> SystemTime {
        self.expires_at
    }

    /// Whether the entry may no longer be served at `now`.
    ///
    /// A cache implementation enforces its own TTL; readers also call this on
    /// every hit so a broken or clock-skewed external backend cannot turn an
    /// expired entry into a stale answer.
    pub fn is_expired_at(&self, now: SystemTime) -> bool {
        now >= self.expires_at
    }

    /// Whether the entry may no longer be served now.
    pub fn is_expired(&self) -> bool {
        self.is_expired_at(SystemTime::now())
    }

    /// Consume the entry, returning its payload.
    pub fn into_value(self) -> Bytes {
        self.value
    }
}

/// Why a cache operation failed.
///
/// These errors are advisory: callers fall back to the authoritative
/// database and never turn a cache failure into an RPC failure. Backends
/// report transport or protocol problems as [`CacheError::Unavailable`] and
/// implementations that reject an entry admission report
/// [`CacheError::EntryTooLarge`] without storing a partial value.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum CacheError {
    /// The value is larger than the cache's per-entry admission limit.
    #[error("cache entry of {size} bytes exceeds the {limit}-byte admission limit")]
    EntryTooLarge {
        /// Attempted value size.
        size: usize,
        /// Advertised per-entry limit.
        limit: usize,
    },
    /// The caller's deadline elapsed before the backend answered.
    #[error("cache operation timed out after {0:?}")]
    Timeout(Duration),
    /// The backend is unavailable or violated the cache contract.
    ///
    /// The message is logged by the gateway. Backends must keep credentials,
    /// secret URLs and cached payload bytes out of it.
    #[error("cache backend unavailable: {0}")]
    Unavailable(String),
}
