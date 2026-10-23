//! Structural cache keys.
//!
//! A cache key is `(namespace, resource kind, stable id, authoritative row
//! version)`. The version is part of the key, not metadata beside it: a read
//! that observed version `N` in the database can only ever be served the
//! entry cached for `N`, and a late fill of `N` is a different key from the
//! `N + 1` a later read observes. There is no best-effort "latest value" key
//! to invalidate and no tombstone map to reason about.

use std::sync::Arc;

use thiserror::Error;

/// Why a [`CacheKey`] could not be built.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum CacheKeyError {
    /// Namespaces must be non-empty.
    #[error("cache namespace is empty")]
    EmptyNamespace,
    /// Namespaces are a closed, ASCII token.
    #[error("invalid cache namespace `{0}`: use 1-128 ASCII alphanumerics, `.`, `_` or `-`")]
    InvalidNamespace(String),
    /// Ids must be non-empty.
    #[error("cache resource id is empty")]
    EmptyId,
    /// Control bytes and very long ids are almost always a caller bug, and an
    /// id is copied into every key.
    #[error("invalid cache resource id: max 512 bytes and no control characters")]
    InvalidId,
}

/// A sandbox service namespace.
///
/// Storage is single-namespace today (the default is [`Namespace::DEFAULT`]),
/// but keys are already scoped so two namespaces can never alias each other
/// in a cache shared by more than one caller. This is structural key
/// scoping, not a tenant or authorization policy.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Namespace(Arc<str>);

impl Namespace {
    /// The single default namespace used by the sandbox service.
    pub const DEFAULT: &'static str = "default";

    /// Validate and wrap a namespace token.
    pub fn new(raw: impl AsRef<str>) -> Result<Self, CacheKeyError> {
        let raw = raw.as_ref();
        if raw.is_empty() {
            return Err(CacheKeyError::EmptyNamespace);
        }
        if raw.len() > 128
            || !raw
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(CacheKeyError::InvalidNamespace(raw.to_owned()));
        }
        Ok(Self(Arc::from(raw)))
    }

    /// The default namespace shared by every service replica.
    pub fn default_namespace() -> Self {
        Self(Arc::from(Self::DEFAULT))
    }

    /// The namespace token.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for Namespace {
    fn default() -> Self {
        Self::default_namespace()
    }
}

impl std::fmt::Display for Namespace {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// What a cached entry represents.
///
/// Only immutable, versioned payloads belong in the cache. Mutable sandbox,
/// lease, provider, allocation or work-claim state is never represented here:
/// reads that decide capacity, identity, authorization or fencing go to the
/// authoritative database.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum ResourceKind {
    /// A resolved template (spec plus immutable snapshot metadata).
    Template,
    /// A pool definition.
    Pool,
    /// A sandbox create specification (the immutable request shape), never
    /// live sandbox state, lease state or capacity.
    SandboxSpec,
    /// A provider's advertised capabilities for display/selection only.
    /// Provider identity, registration generation, liveness and any fencing
    /// or authorization decision are always read from SQL.
    ProviderCapabilities,
}

impl ResourceKind {
    /// Stable token used by the flat key encoding.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Template => "template",
            Self::Pool => "pool",
            Self::SandboxSpec => "sandbox-spec",
            Self::ProviderCapabilities => "provider-capabilities",
        }
    }

    /// Parse a token produced by [`Self::as_str`].
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "template" => Some(Self::Template),
            "pool" => Some(Self::Pool),
            "sandbox-spec" => Some(Self::SandboxSpec),
            "provider-capabilities" => Some(Self::ProviderCapabilities),
            _ => None,
        }
    }
}

/// An exact-version cache key.
///
/// Cloning is cheap: the namespace and id are shared, and the version is a
/// scalar. Keys can therefore be passed by value to an asynchronous fill
/// without handing the cache a borrowed view of service state.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CacheKey {
    namespace: Namespace,
    kind: ResourceKind,
    id: Arc<str>,
    version: u64,
}

impl CacheKey {
    /// Build a validated key.
    pub fn new(
        namespace: Namespace,
        kind: ResourceKind,
        id: impl AsRef<str>,
        version: u64,
    ) -> Result<Self, CacheKeyError> {
        let id = id.as_ref();
        if id.is_empty() {
            return Err(CacheKeyError::EmptyId);
        }
        if id.len() > 512 || id.bytes().any(|byte| byte.is_ascii_control()) {
            return Err(CacheKeyError::InvalidId);
        }
        Ok(Self {
            namespace,
            kind,
            id: Arc::from(id),
            version,
        })
    }

    /// Key in the default namespace.
    pub fn default_namespace(
        kind: ResourceKind,
        id: impl AsRef<str>,
        version: u64,
    ) -> Result<Self, CacheKeyError> {
        Self::new(Namespace::default_namespace(), kind, id, version)
    }

    /// Namespace scope.
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    /// Resource kind.
    pub fn kind(&self) -> ResourceKind {
        self.kind
    }

    /// Stable resource id (template or pool name, sandbox id, …).
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Authoritative row version observed immediately before the read.
    ///
    /// The version must be assigned by the database and must never be reused
    /// for a different row incarnation. Template and pool names are reusable
    /// ids: delete plus recreate must continue a non-resetting sequence (or
    /// include an incarnation in the version) rather than restarting at 1,
    /// otherwise an entry cached for the deleted row could match the new one.
    /// Versions are opaque here; callers must not derive them from content.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Copy this key at another authoritative version.
    ///
    /// Used when an atomic payload read reports the version it actually
    /// observed: the payload is then cached under that exact version, never
    /// under the older version selected by a metadata query.
    pub fn with_version(&self, version: u64) -> Self {
        Self {
            namespace: self.namespace.clone(),
            kind: self.kind,
            id: self.id.clone(),
            version,
        }
    }

    /// Flat encoding for external cache backends.
    ///
    /// Every variable-length field is length-prefixed, so `|`, `:` or any
    /// other printable character in an id cannot change how the key scopes.
    /// Grammar:
    ///
    /// ```text
    /// v1|<namespace-bytes>:<namespace>|<kind>|<id-bytes>:<id>|<version>
    /// ```
    ///
    /// External implementations receiving a [`CacheKey`] should normally use
    /// the accessors and treat [`Self::encode`] as their storage string. A
    /// backend that parses the string must parse the lengths, never split on
    /// the separators.
    pub fn encode(&self) -> String {
        format!(
            "v1|{}:{}|{}|{}:{}|{}",
            self.namespace.as_str().len(),
            self.namespace.as_str(),
            self.kind.as_str(),
            self.id.len(),
            self.id,
            self.version
        )
    }

    /// Parse [`Self::encode`] output.
    pub fn decode(encoded: &str) -> Result<Self, CacheKeyError> {
        let rest = encoded
            .strip_prefix("v1|")
            .ok_or(CacheKeyError::InvalidNamespace(encoded.to_owned()))?;
        let (namespace_len, rest) = rest
            .split_once(':')
            .ok_or_else(|| CacheKeyError::InvalidNamespace(encoded.to_owned()))?;
        let namespace_len: usize = namespace_len
            .parse()
            .map_err(|_| CacheKeyError::InvalidNamespace(encoded.to_owned()))?;
        let namespace = rest
            .get(..namespace_len)
            .ok_or_else(|| CacheKeyError::InvalidNamespace(encoded.to_owned()))?;
        let rest = rest[namespace_len..]
            .strip_prefix('|')
            .ok_or_else(|| CacheKeyError::InvalidNamespace(encoded.to_owned()))?;
        let (kind, rest) = rest
            .split_once('|')
            .ok_or_else(|| CacheKeyError::InvalidNamespace(encoded.to_owned()))?;
        let kind = ResourceKind::parse(kind)
            .ok_or_else(|| CacheKeyError::InvalidNamespace(encoded.to_owned()))?;
        let (id_len, rest) = rest.split_once(':').ok_or(CacheKeyError::InvalidId)?;
        let id_len: usize = id_len.parse().map_err(|_| CacheKeyError::InvalidId)?;
        let id = rest.get(..id_len).ok_or(CacheKeyError::InvalidId)?;
        let version = rest[id_len..]
            .strip_prefix('|')
            .and_then(|version| version.parse().ok())
            .ok_or(CacheKeyError::InvalidId)?;
        Self::new(Namespace::new(namespace)?, kind, id, version)
    }
}

impl std::fmt::Display for CacheKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.encode())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_round_trips_ids_that_contain_separators() {
        let key = CacheKey::default_namespace(ResourceKind::Template, "a|b:c|d", 41).unwrap();
        let encoded = key.encode();
        assert_eq!(CacheKey::decode(&encoded).unwrap(), key);
        assert!(encoded.ends_with("|41"));
    }

    #[test]
    fn namespaces_and_versions_scope_keys() {
        let a = CacheKey::new(Namespace::new("one").unwrap(), ResourceKind::Pool, "p", 1).unwrap();
        let b = CacheKey::new(Namespace::new("two").unwrap(), ResourceKind::Pool, "p", 1).unwrap();
        let c = CacheKey::new(Namespace::new("one").unwrap(), ResourceKind::Pool, "p", 2).unwrap();
        assert_ne!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn invalid_keys_are_rejected() {
        assert!(Namespace::new("").is_err());
        assert!(Namespace::new("bad namespace").is_err());
        assert!(CacheKey::default_namespace(ResourceKind::Template, "", 1).is_err());
        assert!(CacheKey::default_namespace(ResourceKind::Template, "a\nb", 1).is_err());
    }
}
