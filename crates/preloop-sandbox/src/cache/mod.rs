//! Optional, replaceable acceleration for immutable sandbox read payloads.
//!
//! # What the cache is
//!
//! The database is the authority. This module is an optional in-process (or,
//! later, network) cache in front of cheap, version-keyed reads. It never
//! owns durable state: leases, sandbox lifecycle, provider generations, work
//! claims, allocation counts, enrollment tokens and authorization decisions
//! are read from SQL every time they can change an outcome. Stream bytes and
//! work wakes do not pass through this cache at all.
//!
//! A read path that uses the cache does this:
//!
//! 1. Ask the backend for an authoritative row version (a cheap metadata
//!    query), or use a getter that returns payload and version atomically.
//! 2. Build a [`CacheKey`] from `(namespace, resource kind, id, version)`.
//! 3. Ask a [`CacheGateway`] for that exact key. On a miss, timeout, error or
//!    unreadable entry, execute the authoritative SQL read and return its
//!    value; fill the cache in the background under the version that read
//!    actually observed.
//!
//! Step 1's revision read and step 3's authoritative read are separate,
//! short operations. A cache call never runs inside a command transaction,
//! never holds a row lock and never makes a database write wait: the service
//! commits (or ends) its SQL transaction, then accelerates the read. Cache
//! population is best-effort and asynchronous.
//!
//! The database assigns row versions from a non-resetting sequence: template
//! and pool names are reusable ids, so delete plus recreate must produce a
//! higher version (or include a unique incarnation), never restart at `1`.
//! A payload read must return its version in the same operation as the
//! payload (`load_versioned`) or be filtered by `WHERE version = $n`; a
//! "read latest and cache under the older metadata version" shortcut is
//! forbidden.
//!
//! Because the version is part of the key, a late fill of version `N`
//! cannot be served to a read that observed `N + 1`: it is simply a
//! different key. Invalidation is therefore advisory rather than a
//! correctness mechanism. TTL bounds occupancy and how long an unused entry
//! survives; it is not a freshness promise.
//!
//! # What may be cached
//!
//! Only reusable, immutable, serialized read payloads: resolved templates,
//! pool definitions, immutable sandbox create specs, and provider capability
//! snapshots that are keyed by an authoritative version. Values are stored
//! as [`bytes::Bytes`] and shared on hit without copying.
//!
//! Never cache lease or ticket tokens, `Lease.env`, `ExecRequest.env`,
//! secrets, certificates/keys, authorization decisions, live capacity,
//! provider generations used for fencing, work-item claims, sandbox
//! lifecycle state, exec output, or stream bytes. List and pagination
//! requests are not turned into whole-service snapshots; page through SQL.
//!
//! # Failure semantics
//!
//! Every call made through a [`CacheGateway`] has a deadline. A timeout or
//! backend error is a miss: reads fall back to SQL and mutations never wait
//! on the cache. Outage warnings are rate-limited. Flushing, disabling or
//! replacing the cache cannot change lease ownership, state transitions,
//! allocation counts, provider generations or work claims.
//!
//! # Selecting a backend
//!
//! [`CacheSettings`] supports `none` (documented, every read hits SQL) and
//! `memory` (the bounded default). Embedders can supply any
//! `Arc<dyn SharedCache>` to the service; nothing in the service downcasts
//! to [`InMemoryCache`].
//!
//! ## External cache requirements
//!
//! A Redis/Valkey or other network adapter implements the same trait:
//!
//! * Key: use [`CacheKey::encode`] with a deployment prefix. Never parse the
//!   string by splitting on separators; use the accessors. Namespaces already
//!   scope otherwise-identical resource ids.
//! * Value: the `CacheEntry` payload, with its absolute deadline. Set the
//!   backend expiry (`SET … PX <remaining>`, for example) on every put and do
//!   not extend it on reads. `get` must return `None` for an expired key and
//!   must not return a value with a stale deadline; an adapter may store the
//!   absolute deadline in a small header beside the payload or read the
//!   backend's remaining TTL, but it must reconstruct
//!   [`CacheEntry::with_expiry`] accurately.
//! * Invalidation: delete exactly the encoded key. `clear` must delete only
//!   this service's prefix, never a shared database with `FLUSHALL`.
//! * Errors: report transport/protocol failure as [`CacheError::Unavailable`]
//!   and its own deadline as [`CacheError::Timeout`]; never panic. The
//!   gateway's outer timeout is a backstop, so the adapter must still use
//!   connect/read/write timeouts and a bounded connection pool.
//! * Capacity: configure the server's memory limit and eviction policy; the
//!   gateway refuses values above the configured admission limit before the
//!   adapter sees them, but the adapter must also report an admission refusal
//!   as [`CacheError::EntryTooLarge`] rather than storing a truncated value.
//! * No authentication, tenancy or policy logic lives in the cache. It stores
//!   opaque bytes; authorization remains an authoritative SQL check.
//!
//! Redis is not a required dependency or deployment service. Adding an SDK
//! adapter is a separate, optional integration.

mod config;
mod entry;
mod gateway;
mod key;
mod memory;
mod none;

use async_trait::async_trait;

pub use config::{
    CacheConfigError, CacheConfigInput, CacheMode, CacheSettings, DEFAULT_MAX_BYTES,
    DEFAULT_MAX_ENTRIES, DEFAULT_MAX_ENTRY_BYTES, DEFAULT_TIMEOUT, DEFAULT_TTL,
    DEFAULT_WARN_INTERVAL, ENV_MAX_BYTES, ENV_MAX_ENTRIES, ENV_MAX_ENTRY_BYTES, ENV_MODE,
    ENV_TIMEOUT_MS, ENV_TTL_SECS, ENV_WARN_INTERVAL_SECS,
};
pub use entry::{CacheEntry, CacheError};
pub use gateway::{CacheGateway, CacheHealth, Versioned};
pub use key::{CacheKey, CacheKeyError, Namespace, ResourceKind};
pub use memory::{CacheStats, InMemoryCache, InMemoryConfig};
pub use none::NoCache;

/// An asynchronous cache backend.
///
/// The trait is object-safe so a service or embedder can hold
/// `Arc<dyn SharedCache>` and swap in another implementation without touching
/// read paths. Implementations must be cheap to clone behind an `Arc`, must
/// not panic on backend failure, and must return within their own transport
/// deadline; service callers add an outer bounded timeout through
/// [`CacheGateway`].
///
/// Keys are exact-version keys. Implementations may evict any entry at any
/// time; they must never synthesize an entry, rewrite a value, or serve an
/// entry past its deadline. `put` is best-effort and may reject oversized
/// values with [`CacheError::EntryTooLarge`].
#[async_trait]
pub trait SharedCache: Send + Sync + std::fmt::Debug {
    /// Look up `key`. `Ok(None)` means a miss (including an expired entry).
    async fn get(&self, key: &CacheKey) -> Result<Option<CacheEntry>, CacheError>;

    /// Store `entry` for `key`. The entry's absolute deadline is part of the
    /// stored value, not a freshness guarantee for future readers.
    async fn put(&self, key: &CacheKey, entry: CacheEntry) -> Result<(), CacheError>;

    /// Remove exactly `key`. A missing key is success.
    async fn invalidate(&self, key: &CacheKey) -> Result<(), CacheError>;

    /// Remove every entry this backend holds for the service. Missing is
    /// success. Must not affect unrelated data if the backend is shared.
    async fn clear(&self) -> Result<(), CacheError>;

    /// Short implementation name for diagnostics (`memory`, `none`,
    /// `redis`, …).
    fn name(&self) -> &'static str;
}
