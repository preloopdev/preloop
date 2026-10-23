use anyhow::{Context, Result};
pub use preloop_secrets::{
    CREDENTIAL_STORE_ENV, CredentialRef, CredentialStore, CredentialStoreKind, CredentialWriter,
    FileCredentialStore, MemoryCredentialStore, SecretString, UnavailableCredentialStore,
    WriteOnlyCredentialStore, Written, selected_kind,
};
use preloop_secrets::{
    set_private_directory_permissions, set_private_file_permissions, write_private_file,
};
use rand::Rng;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The private-file fallback for the per-engine administrator token.
pub const ENGINE_TOKEN_FILE: &str = "engine.token";
const ENGINE_TOKEN_REFERENCE_PREFIX: &str = "engine-token-";

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
    if let Some(app_id) = app_id
        && (app_id.is_empty() || !app_id.chars().all(|c| c.is_ascii_digit()))
    {
        anyhow::bail!("GitHub App id must be numeric, got {app_id:?}");
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

/// Load the engine token from the store selected for this home, without
/// creating or changing credentials. The CLI and the runner client use this to
/// find a local engine's admin token.
///
/// The selected store is authoritative when reachable. A private `engine.token`
/// file remains the fallback for headless and service environments.
pub fn load_engine_token(storage_dir: &Path) -> Result<Option<String>> {
    let store = engine_store(storage_dir)?;
    load_engine_token_with_store(storage_dir, store.as_ref())
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
    // An explicitly configured token needs no storage: return it before
    // touching the filesystem so a directory error cannot abort startup
    // when there is nothing to persist.
    if let Some(configured) = configured {
        return validate_engine_token(&configured, "PRELOOP_SYSTEM_TOKEN");
    }

    std::fs::create_dir_all(storage_dir)
        .with_context(|| format!("create engine-token directory {}", storage_dir.display()))?;
    set_private_directory_permissions(storage_dir)?;

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
                Ok(()) => match store.get_fresh(&reference) {
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
    rand::rng().fill_bytes(&mut bytes);
    let token = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if store_available {
        match store.set(&reference, &SecretString::new(&token)) {
            Ok(()) => match store.get_fresh(&reference) {
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
    write_private_file(path, token)
}

fn remove_engine_token_file(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}

/// Engine policy: credentials live beside the active configuration file.
pub fn credential_store_dir(config_path: &Path) -> PathBuf {
    let parent = config_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    FileCredentialStore::default_dir(parent)
}

/// Select the configured backend under the engine's state directory.
pub fn selected_store(
    state_dir: &Path,
    configured: Option<&str>,
) -> Result<Arc<dyn CredentialStore>> {
    preloop_secrets::selected_store(state_dir, configured)
}

/// Select the configured backend under the engine config directory.
pub fn selected_store_for_config(
    config_path: &Path,
    configured: Option<&str>,
) -> Result<Arc<dyn CredentialStore>> {
    let parent = config_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    preloop_secrets::selected_store(parent, configured)
}

/// Select the configured backend for the engine's active config.
pub fn engine_store(state_dir: &Path) -> Result<Arc<dyn CredentialStore>> {
    let config_path = std::env::var_os(crate::config::CONFIG_PATH_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| state_dir.join("config.toml"));
    let configured = crate::config::credential_store_setting(&config_path)?;
    selected_store(state_dir, configured.as_deref())
}
