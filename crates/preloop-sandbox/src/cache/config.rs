//! Cache selection and tunables.
//!
//! The sandbox state cache is an optional accelerator in front of the
//! authoritative database. The default is a bounded in-process cache; `none`
//! sends every read to SQL. An embedder that wants Redis, Valkey, Memcached
//! or another network cache constructs its own [`SharedCache`] and passes it
//! to the service (or wraps it in a [`CacheGateway`]); there is no built-in
//! network cache and no required extra deployment service.
//!
//! Environment variables (all optional, blank means unset):
//!
//! | Variable | Default | Meaning |
//! |---|---|---|
//! | `PRELOOP_SANDBOX_STATE_CACHE` | `memory` | `none` or `memory` |
//! | `PRELOOP_SANDBOX_STATE_CACHE_MAX_BYTES` | 64 MiB | Total serialized payload capacity |
//! | `PRELOOP_SANDBOX_STATE_CACHE_MAX_ENTRIES` | 8192 | Entry-count capacity |
//! | `PRELOOP_SANDBOX_STATE_CACHE_MAX_ENTRY_BYTES` | 1 MiB | Per-entry admission limit |
//! | `PRELOOP_SANDBOX_STATE_CACHE_TTL_SECS` | 30 | Entry lifetime |
//! | `PRELOOP_SANDBOX_STATE_CACHE_TIMEOUT_MS` | 50 | Deadline for one cache call |
//! | `PRELOOP_SANDBOX_STATE_CACHE_WARN_INTERVAL_SECS` | 30 | Minimum gap between outage warnings |
//!
//! Unknown modes and malformed or out-of-range numbers are errors: a typo
//! must not silently disable (or silently enlarge) the cache.

use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;

use super::SharedCache;
use super::gateway::CacheGateway;
use super::memory::{InMemoryCache, InMemoryConfig};
use super::none::NoCache;

/// Total payload bytes held by the default in-process cache.
pub const DEFAULT_MAX_BYTES: usize = 64 * 1024 * 1024;
/// Entry count held by the default in-process cache.
pub const DEFAULT_MAX_ENTRIES: usize = 8192;
/// Largest single entry the default in-process cache admits.
pub const DEFAULT_MAX_ENTRY_BYTES: usize = 1024 * 1024;
/// Default lifetime of a cached read.
pub const DEFAULT_TTL: Duration = Duration::from_secs(30);
/// Default deadline for one cache call.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_millis(50);
/// Default minimum gap between outage warnings.
pub const DEFAULT_WARN_INTERVAL: Duration = Duration::from_secs(30);

/// Environment variable selecting the cache implementation.
pub const ENV_MODE: &str = "PRELOOP_SANDBOX_STATE_CACHE";
/// Environment variable for total payload capacity.
pub const ENV_MAX_BYTES: &str = "PRELOOP_SANDBOX_STATE_CACHE_MAX_BYTES";
/// Environment variable for entry-count capacity.
pub const ENV_MAX_ENTRIES: &str = "PRELOOP_SANDBOX_STATE_CACHE_MAX_ENTRIES";
/// Environment variable for the per-entry admission limit.
pub const ENV_MAX_ENTRY_BYTES: &str = "PRELOOP_SANDBOX_STATE_CACHE_MAX_ENTRY_BYTES";
/// Environment variable for entry lifetime.
pub const ENV_TTL_SECS: &str = "PRELOOP_SANDBOX_STATE_CACHE_TTL_SECS";
/// Environment variable for the per-call deadline.
pub const ENV_TIMEOUT_MS: &str = "PRELOOP_SANDBOX_STATE_CACHE_TIMEOUT_MS";
/// Environment variable for the warning interval.
pub const ENV_WARN_INTERVAL_SECS: &str = "PRELOOP_SANDBOX_STATE_CACHE_WARN_INTERVAL_SECS";

/// Which cache implementation to build.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheMode {
    /// No cache: every read goes to the authoritative database.
    None,
    /// Bounded, in-process, version-keyed cache.
    Memory,
}

impl CacheMode {
    /// Token accepted by [`ENV_MODE`].
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Memory => "memory",
        }
    }

    /// Parse the configuration token.
    pub fn parse(raw: &str) -> Result<Self, CacheConfigError> {
        match raw {
            "none" => Ok(Self::None),
            "memory" => Ok(Self::Memory),
            other => Err(CacheConfigError::UnknownMode {
                value: other.to_owned(),
            }),
        }
    }
}

/// Invalid cache configuration.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum CacheConfigError {
    /// Only `none` and `memory` are built in; a network backend is injected
    /// programmatically through [`SharedCache`].
    #[error("unknown sandbox state cache mode `{value}` (valid: none, memory)")]
    UnknownMode {
        /// Rejected value.
        value: String,
    },
    /// A numeric variable did not parse.
    #[error("{name} must be a positive integer, got `{value}`")]
    InvalidNumber {
        /// Variable or field name.
        name: &'static str,
        /// Rejected value.
        value: String,
    },
    /// A numeric value was structurally valid but unusable.
    #[error("{name} must be at least {minimum}, got {value}")]
    OutOfRange {
        /// Variable or field name.
        name: &'static str,
        /// Smallest accepted value.
        minimum: u64,
        /// Rejected value.
        value: u64,
    },
    /// A per-entry limit larger than total capacity would let one entry
    /// evict everything and then fail admission.
    #[error("cache max_entry_bytes ({max_entry_bytes}) cannot exceed max_bytes ({max_bytes})")]
    EntryLimitExceedsCapacity {
        /// Per-entry limit.
        max_entry_bytes: usize,
        /// Total capacity.
        max_bytes: usize,
    },
}

/// Raw cache configuration, as read from the environment or supplied by an
/// embedder. Blank values are unset.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CacheConfigInput {
    /// `none` or `memory`.
    pub mode: Option<String>,
    /// Total payload capacity.
    pub max_bytes: Option<String>,
    /// Entry-count capacity.
    pub max_entries: Option<String>,
    /// Per-entry admission limit.
    pub max_entry_bytes: Option<String>,
    /// Entry lifetime in seconds.
    pub ttl_secs: Option<String>,
    /// Per-call deadline in milliseconds.
    pub timeout_ms: Option<String>,
    /// Warning interval in seconds.
    pub warn_interval_secs: Option<String>,
}

impl CacheConfigInput {
    /// Read every variable, treating blank values as unset.
    pub fn from_env() -> Self {
        let read = |name: &str| {
            std::env::var(name)
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        };
        Self {
            mode: read(ENV_MODE),
            max_bytes: read(ENV_MAX_BYTES),
            max_entries: read(ENV_MAX_ENTRIES),
            max_entry_bytes: read(ENV_MAX_ENTRY_BYTES),
            ttl_secs: read(ENV_TTL_SECS),
            timeout_ms: read(ENV_TIMEOUT_MS),
            warn_interval_secs: read(ENV_WARN_INTERVAL_SECS),
        }
    }

    /// Validate and resolve defaults.
    pub fn resolve(self) -> Result<CacheSettings, CacheConfigError> {
        let mode = match self.mode.as_deref() {
            None => CacheMode::Memory,
            Some(raw) => CacheMode::parse(raw.trim())?,
        };
        let max_bytes = number(self.max_bytes.as_deref(), ENV_MAX_BYTES)?
            .map_or(DEFAULT_MAX_BYTES, |value| value as usize);
        let max_entries = number(self.max_entries.as_deref(), ENV_MAX_ENTRIES)?
            .map_or(DEFAULT_MAX_ENTRIES, |value| value as usize);
        let max_entry_bytes = number(self.max_entry_bytes.as_deref(), ENV_MAX_ENTRY_BYTES)?
            .map_or(DEFAULT_MAX_ENTRY_BYTES, |value| value as usize);
        let ttl = Duration::from_secs(
            number(self.ttl_secs.as_deref(), ENV_TTL_SECS)?.unwrap_or(DEFAULT_TTL.as_secs()),
        );
        let timeout = Duration::from_millis(
            number(self.timeout_ms.as_deref(), ENV_TIMEOUT_MS)?
                .unwrap_or(DEFAULT_TIMEOUT.as_millis() as u64),
        );
        let warn_interval = Duration::from_secs(
            number(self.warn_interval_secs.as_deref(), ENV_WARN_INTERVAL_SECS)?
                .unwrap_or(DEFAULT_WARN_INTERVAL.as_secs()),
        );
        CacheSettings {
            mode,
            max_bytes,
            max_entries,
            max_entry_bytes,
            ttl,
            timeout,
            warn_interval,
        }
        .validated()
    }
}

fn number(raw: Option<&str>, name: &'static str) -> Result<Option<u64>, CacheConfigError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    let value: u64 = raw.parse().map_err(|_| CacheConfigError::InvalidNumber {
        name,
        value: raw.to_owned(),
    })?;
    if value == 0 {
        return Err(CacheConfigError::OutOfRange {
            name,
            minimum: 1,
            value,
        });
    }
    Ok(Some(value))
}

/// Resolved cache configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheSettings {
    /// Selected implementation.
    pub mode: CacheMode,
    /// Total serialized payload capacity (memory mode).
    pub max_bytes: usize,
    /// Entry-count capacity (memory mode).
    pub max_entries: usize,
    /// Per-entry admission limit (memory mode).
    pub max_entry_bytes: usize,
    /// How long a cached read stays admissible.
    pub ttl: Duration,
    /// Deadline for one cache call.
    pub timeout: Duration,
    /// Minimum gap between outage warnings.
    pub warn_interval: Duration,
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            mode: CacheMode::Memory,
            max_bytes: DEFAULT_MAX_BYTES,
            max_entries: DEFAULT_MAX_ENTRIES,
            max_entry_bytes: DEFAULT_MAX_ENTRY_BYTES,
            ttl: DEFAULT_TTL,
            timeout: DEFAULT_TIMEOUT,
            warn_interval: DEFAULT_WARN_INTERVAL,
        }
    }
}

impl CacheSettings {
    /// Read and validate the environment.
    pub fn from_env() -> Result<Self, CacheConfigError> {
        CacheConfigInput::from_env().resolve()
    }

    /// Validate a settings value constructed in code.
    pub fn validated(mut self) -> Result<Self, CacheConfigError> {
        // Parse from strings so every limit shares the same positive-integer
        // rule and the error names the field.
        self.max_bytes = positive_usize("max_bytes", self.max_bytes)?;
        self.max_entries = positive_usize("max_entries", self.max_entries)?;
        self.max_entry_bytes = positive_usize("max_entry_bytes", self.max_entry_bytes)?;
        for (name, duration) in [
            ("ttl", self.ttl),
            ("timeout", self.timeout),
            ("warn_interval", self.warn_interval),
        ] {
            if duration.is_zero() {
                return Err(CacheConfigError::OutOfRange {
                    name,
                    minimum: 1,
                    value: 0,
                });
            }
        }
        if self.max_entry_bytes > self.max_bytes {
            return Err(CacheConfigError::EntryLimitExceedsCapacity {
                max_entry_bytes: self.max_entry_bytes,
                max_bytes: self.max_bytes,
            });
        }
        Ok(self)
    }

    /// Build the selected backend.
    ///
    /// External implementations are supplied by the embedder directly; this
    /// only knows the built-in `none` and `memory` modes.
    pub fn backend(&self) -> Result<Arc<dyn SharedCache>, CacheConfigError> {
        let settings = self.clone().validated()?;
        Ok(match settings.mode {
            CacheMode::None => Arc::new(NoCache),
            CacheMode::Memory => Arc::new(InMemoryCache::new(InMemoryConfig {
                max_bytes: settings.max_bytes,
                max_entries: settings.max_entries,
                max_entry_bytes: settings.max_entry_bytes,
            })?),
        })
    }

    /// Build the backend plus the deadline/warning wrapper every service
    /// read must use.
    pub fn gateway(&self) -> Result<CacheGateway, CacheConfigError> {
        let settings = self.clone().validated()?;
        Ok(CacheGateway::with_admission_limit(
            settings.backend()?,
            settings.timeout,
            settings.warn_interval,
            settings.max_entry_bytes,
        ))
    }
}

fn positive_usize(name: &'static str, value: usize) -> Result<usize, CacheConfigError> {
    if value == 0 {
        return Err(CacheConfigError::OutOfRange {
            name,
            minimum: 1,
            value: 0,
        });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> CacheConfigInput {
        CacheConfigInput::default()
    }

    #[test]
    fn defaults_are_memory_and_bounded() {
        let settings = input().resolve().unwrap();
        assert_eq!(settings.mode, CacheMode::Memory);
        assert_eq!(settings.max_entries, DEFAULT_MAX_ENTRIES);
        assert!(settings.max_entry_bytes <= settings.max_bytes);
        assert!(!settings.ttl.is_zero());
        assert!(!settings.timeout.is_zero());
    }

    #[test]
    fn unknown_mode_and_bad_numbers_are_errors() {
        let mut raw = input();
        raw.mode = Some("redis".into());
        assert!(matches!(
            raw.resolve(),
            Err(CacheConfigError::UnknownMode { .. })
        ));

        let mut raw = input();
        raw.max_bytes = Some("many".into());
        assert!(matches!(
            raw.resolve(),
            Err(CacheConfigError::InvalidNumber { .. })
        ));

        let mut raw = input();
        raw.max_entries = Some("0".into());
        assert!(matches!(
            raw.resolve(),
            Err(CacheConfigError::OutOfRange { .. })
        ));

        let mut raw = input();
        raw.max_bytes = Some("1024".into());
        raw.max_entry_bytes = Some("2048".into());
        assert!(matches!(
            raw.resolve(),
            Err(CacheConfigError::EntryLimitExceedsCapacity { .. })
        ));
    }

    #[test]
    fn none_builds_a_no_cache_backend() {
        let mut raw = input();
        raw.mode = Some("none".into());
        let settings = raw.resolve().unwrap();
        assert_eq!(settings.mode, CacheMode::None);
        let backend = settings.backend().unwrap();
        assert_eq!(backend.name(), "none");
    }
}
