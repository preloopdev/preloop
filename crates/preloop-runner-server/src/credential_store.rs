use anyhow::{Context, Result};
pub use preloop_gha_protocol::SecretString;
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// The private-file fallback for the per-engine administrator token.
pub const ENGINE_TOKEN_FILE: &str = "engine.token";

const ENGINE_TOKEN_REFERENCE_PREFIX: &str = "engine-token-";

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
    /// Create and validate a new credential reference.
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

    /// Return the underlying reference string.
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
        Some(h)
            if !h.is_empty()
                && h != "github.com"
                && h != "https://github.com"
                && h != "http://github.com" =>
        {
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
                .map(|c| {
                    if c.is_ascii_alphanumeric() || c == '-' || c == '.' {
                        c
                    } else {
                        '-'
                    }
                })
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
    /// Retrieve a secret by reference from the store.
    fn get(&self, reference: &CredentialRef) -> Result<Option<SecretString>>;
    /// Write or overwrite a secret by reference in the store.
    fn set(&self, reference: &CredentialRef, value: &SecretString) -> Result<()>;
    /// Delete a secret by reference from the store.
    fn delete(&self, reference: &CredentialRef) -> Result<()>;
    /// Human-readable name of the backend for diagnostics.
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
/// Resolve the storage directory used for the engine administrator token.
///
/// Managed engines set `PRELOOP_HOME` and use that engine home as the token
/// storage scope; standalone servers use their state directory directly.
pub fn engine_token_dir(state_dir: &Path) -> PathBuf {
    std::env::var_os("PRELOOP_HOME")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| state_dir.to_path_buf())
}

/// Build the OS-store reference for one engine instance.
///
/// The reference is scoped by the resolved storage path so two engine homes
/// owned by one OS user do not share an administrator credential.
pub fn engine_token_reference(storage_dir: &Path) -> Result<CredentialRef> {
    let path = if storage_dir.is_absolute() {
        storage_dir.to_path_buf()
    } else {
        std::env::current_dir()
            .context("resolve relative engine-token storage directory")?
            .join(storage_dir)
    };
    let path = std::fs::canonicalize(&path).unwrap_or(path);
    let digest = Sha256::digest(path.to_string_lossy().as_bytes());
    CredentialRef::new(format!("{ENGINE_TOKEN_REFERENCE_PREFIX}{digest:x}"))
}

/// Load an existing engine token without creating or changing credentials.
///
/// The OS credential store is authoritative when available. A private
/// `engine.token` file remains the fallback for headless/service environments.
pub fn load_engine_token(storage_dir: &Path) -> Result<Option<String>> {
    load_engine_token_with_store(storage_dir, &OsCredentialStore)
}

/// Resolve the engine token, generating and persisting one when absent.
///
/// An explicitly supplied token is validated and returned without copying it
/// into another backend; externally managed secrets should not be duplicated.
/// Generated or migrated tokens use the OS credential store when available and
/// fall back to a private `engine.token` file when it is not.
pub fn resolve_engine_token(storage_dir: &Path, configured: Option<String>) -> Result<String> {
    resolve_engine_token_with_store(storage_dir, configured, &OsCredentialStore)
}

/// Resolve an engine token against a supplied backend.
///
/// This is public so embedders can provide a backend appropriate to their
/// lifecycle, while tests can use [`MemoryCredentialStore`] without touching a
/// user's keychain.
pub fn resolve_engine_token_with_store(
    storage_dir: &Path,
    configured: Option<String>,
    store: &dyn CredentialStore,
) -> Result<String> {
    std::fs::create_dir_all(storage_dir)
        .with_context(|| format!("create engine-token directory {}", storage_dir.display()))?;
    set_private_directory_permissions(storage_dir)?;

    if let Some(configured) = configured {
        return validate_engine_token(&configured, "PRELOOP_SYSTEM_TOKEN");
    }

    let reference = engine_token_reference(storage_dir)?;
    let mut store_available = match store.available() {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!(
                store = store.name(),
                %error,
                "engine token OS credential store unavailable; using private file fallback"
            );
            false
        }
    };

    let token_path = storage_dir.join(ENGINE_TOKEN_FILE);
    if store_available {
        match store.get(&reference) {
            Ok(Some(secret)) => {
                let token = validate_engine_token(secret.expose(), store.name())?;
                remove_engine_token_file(&token_path)?;
                return Ok(token);
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(
                    store = store.name(),
                    %error,
                    "failed to read engine token from OS credential store; \
                     using private file fallback"
                );
                store_available = false;
            }
        }
    }

    if let Some(token) = read_engine_token_file(&token_path)? {
        if store_available {
            match store.set(&reference, &SecretString::new(&token)) {
                Ok(()) => match store.get(&reference) {
                    Ok(Some(stored)) => {
                        match validate_engine_token(stored.expose(), store.name()) {
                            Ok(stored) if stored == token => {
                                remove_engine_token_file(&token_path)?;
                            }
                            Ok(stored) => {
                                tracing::warn!(
                                    store = store.name(),
                                    "OS credential store changed the migrated engine token; \
                                 using the stored value"
                                );
                                remove_engine_token_file(&token_path)?;
                                return Ok(stored);
                            }
                            Err(error) => tracing::warn!(
                                store = store.name(),
                                %error,
                                "OS credential store returned an invalid migrated engine token; \
                                 retaining private file fallback"
                            ),
                        }
                    }
                    Ok(None) => tracing::warn!(
                        store = store.name(),
                        "OS credential store did not return the migrated engine token; \
                         retaining private file fallback"
                    ),
                    Err(error) => tracing::warn!(
                        store = store.name(),
                        %error,
                        "OS credential store could not read back the migrated engine token; \
                         retaining private file fallback"
                    ),
                },
                Err(error) => tracing::warn!(
                    store = store.name(),
                    %error,
                    "failed to migrate engine token into OS credential store; \
                     retaining private file fallback"
                ),
            }
        }
        return Ok(token);
    }

    let mut bytes = [0_u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    let token = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if store_available {
        match store.set(&reference, &SecretString::new(&token)) {
            Ok(()) => match store.get(&reference) {
                Ok(Some(stored)) => match validate_engine_token(stored.expose(), store.name()) {
                    Ok(stored) if stored == token => return Ok(token),
                    Ok(stored) => {
                        tracing::warn!(
                            store = store.name(),
                            "OS credential store changed the generated engine token; \
                             using the stored value"
                        );
                        return Ok(stored);
                    }
                    Err(error) => tracing::warn!(
                        store = store.name(),
                        %error,
                        "OS credential store returned an invalid generated engine token; \
                         using private file fallback"
                    ),
                },
                Ok(None) => tracing::warn!(
                    store = store.name(),
                    "OS credential store did not return the generated engine token; \
                     using private file fallback"
                ),
                Err(error) => tracing::warn!(
                    store = store.name(),
                    %error,
                    "OS credential store could not read back the generated engine token; \
                     using private file fallback"
                ),
            },
            Err(error) => {
                tracing::warn!(
                    store = store.name(),
                    %error,
                    "failed to persist generated engine token in OS credential store; \
                     using private file fallback"
                );
            }
        }
    }
    write_engine_token_file(&token_path, &token)?;
    Ok(token)
}

fn load_engine_token_with_store(
    storage_dir: &Path,
    store: &dyn CredentialStore,
) -> Result<Option<String>> {
    let reference = engine_token_reference(storage_dir)?;
    let store_available = match store.available() {
        Ok(()) => true,
        Err(error) => {
            tracing::debug!(
                store = store.name(),
                %error,
                "engine token OS credential store unavailable while loading"
            );
            false
        }
    };
    let token_path = storage_dir.join(ENGINE_TOKEN_FILE);

    if store_available {
        match store.get(&reference) {
            Ok(Some(secret)) => {
                return validate_engine_token(secret.expose(), store.name()).map(Some);
            }
            Ok(None) => {}
            Err(error) => {
                tracing::debug!(
                    store = store.name(),
                    %error,
                    "failed to read engine token from OS credential store; trying private file fallback"
                );
            }
        }
    }
    read_engine_token_file(&token_path)
}

fn validate_engine_token(value: &str, source: &str) -> Result<String> {
    let token = value.trim();
    anyhow::ensure!(!token.is_empty(), "{source} is empty");
    Ok(token.to_owned())
}

fn read_engine_token_file(path: &Path) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(value) => {
            set_private_file_permissions(path)?;
            validate_engine_token(&value, &path.display().to_string()).map(Some)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read {}", path.display())),
    }
}

fn write_engine_token_file(path: &Path, token: &str) -> Result<()> {
    use std::io::Write as _;

    let parent = path
        .parent()
        .context("engine token file has no parent directory")?;
    let temporary = parent.join(format!(
        ".{ENGINE_TOKEN_FILE}.{:016x}.tmp",
        rand::random::<u64>()
    ));
    let write_result = (|| -> Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .with_context(|| format!("create {}", temporary.display()))?;
        set_private_file_permissions(&temporary)?;
        file.write_all(token.as_bytes())
            .with_context(|| format!("write {}", temporary.display()))?;
        file.sync_all()
            .with_context(|| format!("sync {}", temporary.display()))?;
        #[cfg(windows)]
        if path.exists() {
            std::fs::remove_file(path).with_context(|| format!("replace {}", path.display()))?;
        }
        std::fs::rename(&temporary, path).with_context(|| format!("replace {}", path.display()))?;
        set_private_file_permissions(path)?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    write_result
}

fn remove_engine_token_file(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .with_context(|| format!("protect {}", path.display()))
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("protect {}", path.display()))
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
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

/// A backend that reports successful writes without persisting them.
#[cfg(test)]
#[derive(Clone, Copy, Default)]
struct NonPersistingCredentialStore;

#[cfg(test)]
impl CredentialStore for NonPersistingCredentialStore {
    fn get(&self, _reference: &CredentialRef) -> Result<Option<SecretString>> {
        Ok(None)
    }

    fn set(&self, _reference: &CredentialRef, _value: &SecretString) -> Result<()> {
        Ok(())
    }

    fn delete(&self, _reference: &CredentialRef) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "non-persisting credential store"
    }
}

/// A backend that accepts writes but cannot read them back.
#[cfg(test)]
#[derive(Clone, Copy, Default)]
struct WriteOnlyCredentialStore;

#[cfg(test)]
impl CredentialStore for WriteOnlyCredentialStore {
    fn get(&self, _reference: &CredentialRef) -> Result<Option<SecretString>> {
        anyhow::bail!("credential store readback unavailable")
    }

    fn set(&self, _reference: &CredentialRef, _value: &SecretString) -> Result<()> {
        Ok(())
    }

    fn delete(&self, _reference: &CredentialRef) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "write-only credential store"
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
        store.set(&reference, &SecretString::new("secret")).unwrap();
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
    #[test]
    fn engine_token_references_are_stable_and_scoped() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let first_ref = engine_token_reference(first.path()).unwrap();
        let first_again = engine_token_reference(first.path()).unwrap();
        let second_ref = engine_token_reference(second.path()).unwrap();

        assert_eq!(first_ref, first_again);
        assert_ne!(first_ref, second_ref);
        assert!(first_ref.as_str().starts_with("engine-token-"));
    }

    #[test]
    fn engine_token_uses_store_and_reuses_value() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoryCredentialStore::default();
        let first = resolve_engine_token_with_store(dir.path(), None, &store).unwrap();
        let second = resolve_engine_token_with_store(dir.path(), None, &store).unwrap();
        let reference = engine_token_reference(dir.path()).unwrap();

        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
        assert!(first.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(
            store
                .get(&reference)
                .unwrap()
                .as_ref()
                .map(|value| value.expose()),
            Some(first.as_str())
        );
        assert!(!dir.path().join(ENGINE_TOKEN_FILE).exists());
    }
    #[test]
    fn engine_token_falls_back_when_store_readback_fails() {
        let dir = tempfile::tempdir().unwrap();
        let store = WriteOnlyCredentialStore;
        let token = resolve_engine_token_with_store(dir.path(), None, &store).unwrap();
        let token_path = dir.path().join(ENGINE_TOKEN_FILE);

        assert_eq!(std::fs::read_to_string(&token_path).unwrap(), token);
        assert_eq!(
            load_engine_token_with_store(dir.path(), &store).unwrap(),
            Some(token)
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(token_path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn engine_token_migration_keeps_file_without_verified_store_write() {
        let dir = tempfile::tempdir().unwrap();
        let token_path = dir.path().join(ENGINE_TOKEN_FILE);
        write_engine_token_file(&token_path, "existing-token").unwrap();
        let store = NonPersistingCredentialStore;

        let token = resolve_engine_token_with_store(dir.path(), None, &store).unwrap();

        assert_eq!(token, "existing-token");
        assert_eq!(
            std::fs::read_to_string(&token_path).unwrap(),
            "existing-token"
        );
    }

    #[test]
    fn engine_token_falls_back_to_private_file_and_migrates() {
        let dir = tempfile::tempdir().unwrap();
        let unavailable = UnavailableCredentialStore;
        let first = resolve_engine_token_with_store(dir.path(), None, &unavailable).unwrap();
        let token_path = dir.path().join(ENGINE_TOKEN_FILE);
        assert_eq!(std::fs::read_to_string(&token_path).unwrap(), first);
        assert_eq!(
            load_engine_token_with_store(dir.path(), &unavailable).unwrap(),
            Some(first.clone())
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(&token_path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }

        let store = MemoryCredentialStore::default();
        let migrated = resolve_engine_token_with_store(dir.path(), None, &store).unwrap();
        let reference = engine_token_reference(dir.path()).unwrap();
        assert_eq!(migrated, first);
        assert!(!token_path.exists());
        assert_eq!(
            store
                .get(&reference)
                .unwrap()
                .as_ref()
                .map(|value| value.expose()),
            Some(first.as_str())
        );
        assert_eq!(
            load_engine_token_with_store(dir.path(), &store).unwrap(),
            Some(first)
        );
    }

    #[test]
    fn explicit_engine_token_is_validated_without_copying() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoryCredentialStore::default();
        let token = resolve_engine_token_with_store(
            dir.path(),
            Some("  configured-token  ".into()),
            &store,
        )
        .unwrap();

        assert_eq!(token, "configured-token");
        assert!(store
            .get(&engine_token_reference(dir.path()).unwrap())
            .unwrap()
            .is_none());
        assert!(!dir.path().join(ENGINE_TOKEN_FILE).exists());
    }
}
