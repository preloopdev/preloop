use anyhow::{Context, Result};
pub use preloop_gha_protocol::SecretString;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

/// A validated, non-secret identifier for one stored credential.
///
/// The value becomes the account name of an OS keychain entry under the
/// `preloop` service, so it is restricted to a flat, printable identifier:
/// path separators and control characters would either be rejected by a
/// backend or silently reinterpreted (the secret-service backend treats the
/// attribute as opaque, the Windows credential manager does not).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CredentialRef(String);

impl CredentialRef {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.len() > 255 {
            anyhow::bail!("credential reference must contain 1-255 characters");
        }
        if !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        {
            anyhow::bail!("credential reference contains an invalid character");
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CredentialRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("CredentialRef").field(&self.0).finish()
    }
}

/// Build a stable local reference for a GitHub credential on default github.com.
pub fn github_reference(kind: &str, app_id: Option<&str>) -> Result<CredentialRef> {
    github_reference_with_host(kind, None, app_id)
}

/// Build a stable local reference for a GitHub credential, optionally scoped to a host.
///
/// `app_id` is operator-supplied (`github.apps[].app_id` is hand-editable and
/// accepts a bare integer), so it is validated as numeric for safety and to
/// enforce canonical identifiers.
pub fn github_reference_with_host(
    kind: &str,
    host: Option<&str>,
    app_id: Option<&str>,
) -> Result<CredentialRef> {
    if let Some(app_id) = app_id {
        if app_id.is_empty() || !app_id.chars().all(|c| c.is_ascii_digit()) {
            anyhow::bail!("GitHub App id must be numeric, got {app_id:?}");
        }
    }
    let host_prefix = match host {
        Some(h) if !h.is_empty() && h != "github.com" && h != "https://github.com" && h != "http://github.com" => {
            let clean = h
                .trim_start_matches("https://")
                .trim_start_matches("http://")
                .split('/')
                .next()
                .unwrap_or(h)
                .split(':')
                .next()
                .unwrap_or(h);
            let sanitized: String = clean
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '.' { c } else { '-' })
                .collect();
            if sanitized.is_empty() {
                String::new()
            } else {
                format!("{sanitized}-")
            }
        }
        _ => String::new(),
    };
    let value = match (kind, app_id) {
        ("pat", None) => format!("github-{host_prefix}pat"),
        ("app-pem", Some(app_id)) => format!("github-{host_prefix}app-pem-{app_id}"),
        ("webhook", Some(app_id)) => format!("github-{host_prefix}app-webhook-{app_id}"),
        _ => anyhow::bail!("invalid GitHub credential reference"),
    };
    CredentialRef::new(value)
}

/// Secret storage backend. Implementations must never log values.
pub trait CredentialStore: Send + Sync {
    fn get(&self, reference: &CredentialRef) -> Result<Option<SecretString>>;
    fn set(&self, reference: &CredentialRef, value: &SecretString) -> Result<()>;
    fn delete(&self, reference: &CredentialRef) -> Result<()>;
    fn name(&self) -> &'static str;

    /// Whether the backend can be reached at all.
    ///
    /// Distinct from a missing entry: a headless Linux host has no
    /// secret-service daemon, so every operation fails for a reason that has
    /// nothing to do with the credential being asked for. Callers use this to
    /// degrade instead of failing closed.
    fn available(&self) -> Result<()> {
        Ok(())
    }
}

/// The host operating system's native credential store.
#[derive(Debug, Clone, Copy, Default)]
pub struct OsCredentialStore;

impl OsCredentialStore {
    fn entry(reference: &CredentialRef) -> Result<keyring::Entry> {
        keyring::Entry::new("preloop", reference.as_str()).context("create OS credential entry")
    }
}

impl CredentialStore for OsCredentialStore {
    fn get(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
        match Self::entry(reference)?.get_password() {
            Ok(value) => Ok(Some(SecretString::new(value))),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(error) => Err(error).context("read credential from OS store"),
        }
    }

    fn set(&self, reference: &CredentialRef, value: &SecretString) -> Result<()> {
        if value.expose().is_empty() {
            anyhow::bail!("refusing to store an empty credential");
        }
        Self::entry(reference)?
            .set_password(value.expose())
            .context("write credential to OS store")?;
        Ok(())
    }

    fn delete(&self, reference: &CredentialRef) -> Result<()> {
        match Self::entry(reference)?.delete_credential() {
            Ok(()) => Ok(()),
            Err(keyring::Error::NoEntry) => Ok(()),
            Err(error) => Err(error).context("delete credential from OS store"),
        }
    }


    fn name(&self) -> &'static str {
        "operating-system credential store"
    }

    /// `keyring`'s platform store is initialized once, lazily, on the first
    /// `Entry::new`. `store_status` reports that one-time result without
    /// touching a credential.
    fn available(&self) -> Result<()> {
        match keyring::Entry::store_status() {
            Ok(()) => Ok(()),
            Err(error) => {
                anyhow::bail!("no operating-system credential store available: {error}")
            }
        }
    }
}

/// In-memory backend for deterministic tests and embedders.
#[derive(Clone, Default)]
pub struct MemoryCredentialStore {
    values: Arc<Mutex<HashMap<CredentialRef, SecretString>>>,
}

impl CredentialStore for MemoryCredentialStore {
    fn get(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
        Ok(self
            .values
            .lock()
            .expect("credential store lock poisoned")
            .get(reference)
            .cloned())
    }

    fn set(&self, reference: &CredentialRef, value: &SecretString) -> Result<()> {
        if value.expose().is_empty() {
            anyhow::bail!("refusing to store an empty credential");
        }
        self.values
            .lock()
            .expect("credential store lock poisoned")
            .insert(reference.clone(), value.clone());
        Ok(())
    }

    fn delete(&self, reference: &CredentialRef) -> Result<()> {
        self.values
            .lock()
            .expect("credential store lock poisoned")
            .remove(reference);
        Ok(())
    }

    fn name(&self) -> &'static str {
        "memory credential store"
    }
}

/// A backend that is reachable by nobody, standing in for a headless host.
#[cfg(test)]
#[derive(Clone, Copy, Default)]
pub(crate) struct UnavailableCredentialStore;

#[cfg(test)]
impl CredentialStore for UnavailableCredentialStore {
    fn get(&self, _reference: &CredentialRef) -> Result<Option<SecretString>> {
        anyhow::bail!("credential store unavailable")
    }

    fn set(&self, _reference: &CredentialRef, _value: &SecretString) -> Result<()> {
        anyhow::bail!("credential store unavailable")
    }

    fn delete(&self, _reference: &CredentialRef) -> Result<()> {
        anyhow::bail!("credential store unavailable")
    }

    fn name(&self) -> &'static str {
        "unavailable credential store"
    }

    fn available(&self) -> Result<()> {
        anyhow::bail!("credential store unavailable")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn references_reject_unsafe_values() {
        assert!(CredentialRef::new("").is_err());
        assert!(CredentialRef::new("a/b").is_err());
        assert!(CredentialRef::new("a\nb").is_err());
        assert!(CredentialRef::new("github/pat").is_err());
        assert!(CredentialRef::new("github pat").is_err());
        assert!(CredentialRef::new("github:pat").is_err());
        assert!(CredentialRef::new("a".repeat(256)).is_err());
        assert!(CredentialRef::new("github-app-pem-12345").is_ok());
    }

    #[test]
    fn memory_store_round_trips_and_deletes() {
        let store = MemoryCredentialStore::default();
        let reference = CredentialRef::new("github-pat").unwrap();
        assert_eq!(store.get(&reference).unwrap(), None);
        store
            .set(&reference, &SecretString::new("secret"))
            .unwrap();
        assert_eq!(
            store.get(&reference).unwrap().as_ref().map(|s| s.expose()),
            Some("secret")
        );
        store.delete(&reference).unwrap();
        assert_eq!(store.get(&reference).unwrap(), None);
    }

    #[test]
    fn memory_store_rejects_empty_values() {
        let store = MemoryCredentialStore::default();
        let reference = CredentialRef::new("github-pat").unwrap();
        assert!(store.set(&reference, &SecretString::new("")).is_err());
    }

    /// App IDs are validated as numeric for safety and to enforce canonical identifiers.
    #[test]
    fn app_ids_are_validated_and_canonical() {
        assert!(github_reference("app-pem", Some("webhook-9")).is_err());
        assert!(github_reference("app-pem", Some("")).is_err());
        assert!(github_reference("app-pem", Some("../etc")).is_err());
        assert!(github_reference("pat", Some("12345")).is_err());
        assert!(github_reference("nonsense", None).is_err());
        assert_eq!(
            github_reference("app-pem", Some("12345")).unwrap().as_str(),
            "github-app-pem-12345"
        );
        assert_eq!(
            github_reference("webhook", Some("12345")).unwrap().as_str(),
            "github-app-webhook-12345"
        );
        assert_eq!(
            github_reference("pat", None).unwrap().as_str(),
            "github-pat"
        );
        assert_eq!(
            github_reference_with_host("pat", Some("https://ghe.example.com"), None)
                .unwrap()
                .as_str(),
            "github-ghe.example.com-pat"
        );
        assert_eq!(
            github_reference_with_host("app-pem", Some("ghe.example.com"), Some("12345"))
                .unwrap()
                .as_str(),
            "github-ghe.example.com-app-pem-12345"
        );
    }
}
