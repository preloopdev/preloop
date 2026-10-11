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
            Err(error) => Err(deny_helpful_error(error, reference, "read")),
        }
    }

    fn set(&self, reference: &CredentialRef, value: &SecretString) -> Result<()> {
        if value.expose().is_empty() {
            anyhow::bail!("refusing to store an empty credential");
        }
        Self::entry(reference)?
            .set_password(value.expose())
            .map_err(|error| deny_helpful_error(error, reference, "write"))?;
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
    ///
    /// macOS: `store_status` succeeds whenever the Security framework is
    /// present — including SSH sessions and LaunchDaemons where the login
    /// keychain is locked and `SecKeychainAddGenericPassword` blocks forever
    /// in `mach_msg` waiting on `securityd` (observed: `preloop serve` hung
    /// before binding its listener when run over SSH on a Mac Studio). Probe
    /// the login keychain's lock state with `security show-keychain-info`;
    /// "User interaction is not allowed" means writes would hang, so report
    /// the store unavailable and let callers fall back to inline/file
    /// credentials.
    fn available(&self) -> Result<()> {
        match keyring::Entry::store_status() {
            Ok(()) => {}
            Err(error) => {
                anyhow::bail!("no operating-system credential store available: {error}")
            }
        }
        #[cfg(target_os = "macos")]
        macos_keychain_writable()?;
        Ok(())
    }
}

/// A keychain denial reads as a generic platform failure. macOS prompts once
/// per item for binaries outside the item's trusted-apps list (ad-hoc-signed
/// builds change identity every release), so a denied or dismissed dialog
/// must tell the operator what to approve instead of surfacing `User
/// canceled`. Non-interactive sessions cannot approve anything; point those
/// at the file backend instead of letting them hang on a prompt.
fn deny_helpful_error(error: keyring::Error, reference: &CredentialRef, op: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "credential store denied {} of {:?} ({:#}): approve the macOS keychain dialog for the `preloop` service with Always Allow (one approval per item covers the process); \
         headless or background sessions cannot approve dialogs and will hang or fail here — set PRELOOP_CREDENTIAL_STORE=file instead",
        op,
        reference,
        anyhow::anyhow!(error),
    )
}

/// Whether the login keychain accepts writes without user interaction.
///
/// `security show-keychain-info` fails with `User interaction is not allowed`
/// (errSecInteractionNotAllowed) when the keychain is locked — but in a
/// session that cannot reach `securityd` at all (SSH, LaunchDaemon before
/// Aqua login) the command itself blocks in `mach_msg` forever, the same
/// hang this probe exists to prevent. The probe therefore runs with a hard
/// deadline: a timeout means the store is unreachable, not writable.
///
/// `show-keychain-info` alone is not enough: it can succeed while a real
/// `SecKeychainAddGenericPassword` write still blocks forever (observed:
/// `preloop serve` under launchd on a Mac Studio — probe passed, the token
/// migration write then hung the whole runtime in mach_msg). The probe
/// therefore performs an actual write+delete of a throwaway item, which
/// exercises the same `SecItemAdd` path `keyring` uses, under the same
/// deadline.
///
/// A nonzero exit for any other reason is also treated as unavailable: the
/// probe is cheap and the fallback (inline/file credentials) is always safe.
#[cfg(target_os = "macos")]
fn macos_keychain_writable() -> Result<()> {
    use std::io::Read;
    use std::time::{Duration, Instant};

    const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
    /// Throwaway item used to prove `SecItemAdd` completes in this session.
    /// Distinct service/account so it can never collide with a real entry.
    const PROBE_SERVICE: &str = "dev.preloop.writability-probe";
    const PROBE_ACCOUNT: &str = "probe";

    let home = std::env::var_os("HOME").context("HOME is not set")?;
    let keychain = Path::new(&home)
        .join("Library")
        .join("Keychains")
        .join("login.keychain-db");

    // One probe command: write a throwaway item, then delete it. Both go
    // through the same SecItemAdd/SecItemDelete path a real credential write
    // takes, so a locked keychain hangs or fails here exactly as it would on
    // the engine-token migration.
    let mut child = std::process::Command::new("security")
        .arg("add-generic-password")
        .arg("-s")
        .arg(PROBE_SERVICE)
        .arg("-a")
        .arg(PROBE_ACCOUNT)
        .arg("-w")
        .arg("probe")
        .arg(&keychain)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("probing login keychain writability")?;
    let deadline = Instant::now() + PROBE_TIMEOUT;
    let status = loop {
        match child.try_wait().context("waiting on keychain probe")? {
            Some(status) => break status,
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                anyhow::bail!(
                    "login keychain write probe timed out after {}s (keychain locked or \
                     securityd unreachable); using inline/file credentials",
                    PROBE_TIMEOUT.as_secs()
                );
            }
            None => std::thread::sleep(Duration::from_millis(25)),
        }
    };
    // Best-effort cleanup of the probe item regardless of outcome; a leftover
    // is harmless (distinct service name, no secret value).
    let _ = std::process::Command::new("security")
        .arg("delete-generic-password")
        .arg("-s")
        .arg(PROBE_SERVICE)
        .arg("-a")
        .arg(PROBE_ACCOUNT)
        .arg(&keychain)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
    if !status.success() {
        let mut stderr = String::new();
        if let Some(mut pipe) = child.stderr.take() {
            let _ = pipe.read_to_string(&mut stderr);
        }
        anyhow::bail!(
            "login keychain is not writable in this session ({}); \
             using inline/file credentials",
            stderr.trim()
        );
    }
    Ok(())
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

/// Like [`load_engine_token`], but the backend is chosen by
/// `PRELOOP_CREDENTIAL_STORE` (`file`/`memory`/`os`). Use this from binaries
/// so a dev or headless process can avoid the OS keychain prompt entirely.
pub fn load_engine_token_from_env(storage_dir: &Path) -> Result<Option<String>> {
    let store = store_from_env(storage_dir);
    load_engine_token_with_store(storage_dir, store.as_ref())
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
    write_private_file(path, token)
}

fn remove_engine_token_file(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}

/// Write `contents` to `path` so the value is never observable in a
/// non-private file, and never written through a pre-existing file.
///
/// A fresh temp file is created `0600` (ignoring the process umask), synced,
/// then renamed over the target: the target either does not exist or is the
/// fully written private file, and a handle opened on an older version keeps
/// reading that older version instead of the new secret.
fn write_private_file(path: &Path, contents: &str) -> Result<()> {
    use std::io::Write as _;

    let parent = path.parent().context("file has no parent directory")?;
    let file_name = path
        .file_name()
        .context("file has no file name")?
        .to_string_lossy()
        .into_owned();
    let temporary = parent.join(format!(".{file_name}.{:016x}.tmp", rand::random::<u64>()));
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
        file.write_all(contents.as_bytes())
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

/// Create `dir` private from the start. `DirBuilder::mode` covers the
/// directories it creates (a fresh directory is never briefly readable to
/// other users); the explicit chmod still tightens a directory that already
/// existed with a looser mode.
fn create_private_directory(dir: &Path) -> Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    builder
        .create(dir)
        .with_context(|| format!("create credential dir {}", dir.display()))?;
    set_private_directory_permissions(dir)
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

/// File-backed credential store: one `0600` file per credential under a
/// private directory. Selected with `PRELOOP_CREDENTIAL_STORE=file` so a dev
/// or headless server never touches the OS keychain with no per-binary access
/// prompt, and credentials survive rebuilds and restarts.
///
/// Security note: this is the same trust level as the existing
/// `engine.token` file fallback — a private file in the state dir, readable
/// only by the owning user. It is *not* the OS keychain; use it where a
/// keychain prompt is unacceptable (local iteration, containers, CI).
#[derive(Clone)]
pub struct FileCredentialStore {
    dir: PathBuf,
}

impl FileCredentialStore {
    /// Store rooted at `dir` (created `0700` on first write).
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// The default directory: `<state_dir>/credentials`.
    pub fn default_dir(state_dir: &Path) -> PathBuf {
        state_dir.join("credentials")
    }

    /// Map a credential reference to a safe filename. References are already
    /// restricted to a flat printable identifier, but we hash anyway so a
    /// host-provided reference can never escape the directory or collide
    /// with the engine-token file.
    fn path_for(&self, reference: &CredentialRef) -> PathBuf {
        let digest = Sha256::digest(reference.as_str().as_bytes());
        self.dir.join(format!("{digest:x}.cred"))
    }
}

impl CredentialStore for FileCredentialStore {
    fn get(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
        let path = self.path_for(reference);
        match std::fs::read_to_string(&path) {
            Ok(value) => Ok(Some(SecretString::new(value))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).context(format!("read credential file {}", path.display())),
        }
    }

    fn set(&self, reference: &CredentialRef, value: &SecretString) -> Result<()> {
        if value.expose().is_empty() {
            anyhow::bail!("refusing to store an empty credential");
        }
        create_private_directory(&self.dir)?;
        let path = self.path_for(reference);
        write_private_file(&path, value.expose())
            .with_context(|| format!("write credential file {}", path.display()))
    }

    fn delete(&self, reference: &CredentialRef) -> Result<()> {
        let path = self.path_for(reference);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).context(format!("delete credential file {}", path.display())),
        }
    }

    fn name(&self) -> &'static str {
        "file credential store"
    }
}

/// Process-wide read cache in front of any [`CredentialStore`] backend: each
/// distinct reference hits the backend at most once per process. The cache
/// exists for the OS backend — macOS prompts once per keychain hit for
/// binaries outside the item's trusted-apps list (ad-hoc-signed builds
/// change identity every release), so every subsystem re-read during boot
/// turned into another password dialog. Cache misses (absent entries) are
/// never cached, so an entry created mid-process is picked up on next read;
/// writes and deletes update the cache, so it never serves stale values.
/// Secrets already live in process memory wherever they are used
/// ([`SecretString`] redacts on debug/format); this adds no new exposure.
#[derive(Debug, Default)]
pub struct CachedCredentialStore<S> {
    inner: S,
    cache: Mutex<HashMap<CredentialRef, SecretString>>,
}

impl<S> CachedCredentialStore<S> {
    /// Wrap `inner`; the cache starts empty.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            cache: Mutex::new(HashMap::new()),
        }
    }
}

impl<S: CredentialStore> CredentialStore for CachedCredentialStore<S> {
    fn get(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
        if let Some(cached) = self
            .cache
            .lock()
            .expect("credential cache lock")
            .get(reference)
        {
            return Ok(Some(cached.clone()));
        }
        let value = self.inner.get(reference)?;
        if let Some(secret) = &value {
            self.cache
                .lock()
                .expect("credential cache lock")
                .insert(reference.clone(), secret.clone());
        }
        Ok(value)
    }

    fn set(&self, reference: &CredentialRef, value: &SecretString) -> Result<()> {
        self.inner.set(reference, value)?;
        self.cache
            .lock()
            .expect("credential cache lock")
            .insert(reference.clone(), value.clone());
        Ok(())
    }

    fn delete(&self, reference: &CredentialRef) -> Result<()> {
        self.inner.delete(reference)?;
        self.cache
            .lock()
            .expect("credential cache lock")
            .remove(reference);
        Ok(())
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn available(&self) -> Result<()> {
        self.inner.available()
    }
}

/// Select the credential store for this process.
///
/// `PRELOOP_CREDENTIAL_STORE`:
/// - `os` (default) — the native keychain / secret-service / credential manager.
/// - `file` — a private directory of `0600` files under `state_dir`; no OS
///   prompt, survives rebuilds. Recommended for local dev and headless hosts.
/// - `memory` — non-persistent; secrets vanish on restart (tests only).
///
/// Returns a boxed store so the server and CLI share one selection point.
pub fn store_from_env(state_dir: &Path) -> Arc<dyn CredentialStore> {
    match std::env::var("PRELOOP_CREDENTIAL_STORE")
        .unwrap_or_default()
        .as_str()
    {
        "file" => Arc::new(CachedCredentialStore::new(FileCredentialStore::new(
            FileCredentialStore::default_dir(state_dir),
        ))),
        "memory" => Arc::new(CachedCredentialStore::new(MemoryCredentialStore::default())),
        _ => Arc::new(CachedCredentialStore::new(OsCredentialStore)),
    }
}

/// A backend that is reachable by nobody, standing in for a headless host.
#[cfg(test)]
#[derive(Clone, Copy, Default)]
pub struct UnavailableCredentialStore;

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
pub struct WriteOnlyCredentialStore;

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
    /// Backend-hit counter proving the decorator reads each reference once.
    #[derive(Clone, Default)]
    struct CountingCredentialStore {
        inner: MemoryCredentialStore,
        hits: std::sync::Arc<std::sync::Mutex<usize>>,
    }

    impl CredentialStore for CountingCredentialStore {
        fn get(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
            *self.hits.lock().unwrap() += 1;
            self.inner.get(reference)
        }

        fn set(&self, reference: &CredentialRef, value: &SecretString) -> Result<()> {
            self.inner.set(reference, value)
        }

        fn delete(&self, reference: &CredentialRef) -> Result<()> {
            self.inner.delete(reference)
        }

        fn name(&self) -> &'static str {
            "counting credential store"
        }
    }

    #[test]
    fn cached_store_reads_each_reference_once() {
        let inner = CountingCredentialStore::default();
        let store = CachedCredentialStore::new(inner.clone());
        let reference = CredentialRef::new("github-pat").unwrap();
        store.set(&reference, &SecretString::new("secret")).unwrap();

        // The set primes the cache: repeated reads never reach the backend.
        for _ in 0..3 {
            assert_eq!(
                store.get(&reference).unwrap().as_ref().map(|s| s.expose()),
                Some("secret")
            );
        }
        assert_eq!(*inner.hits.lock().unwrap(), 0);
    }

    #[test]
    fn cached_store_caches_backend_reads_and_misses() {
        let inner = CountingCredentialStore::default();
        let store = CachedCredentialStore::new(inner.clone());
        let reference = CredentialRef::new("github-pat").unwrap();

        // Absent entries are never cached: each miss reaches the backend.
        assert_eq!(store.get(&reference).unwrap(), None);
        assert_eq!(store.get(&reference).unwrap(), None);
        assert_eq!(*inner.hits.lock().unwrap(), 2);

        // First present read populates the cache; later reads do not.
        inner
            .inner
            .set(&reference, &SecretString::new("secret"))
            .unwrap();
        assert_eq!(
            store.get(&reference).unwrap().as_ref().map(|s| s.expose()),
            Some("secret")
        );
        assert_eq!(
            store.get(&reference).unwrap().as_ref().map(|s| s.expose()),
            Some("secret")
        );
        assert_eq!(*inner.hits.lock().unwrap(), 3);
    }

    #[test]
    fn cached_store_delete_clears_the_entry() {
        let inner = CountingCredentialStore::default();
        let store = CachedCredentialStore::new(inner.clone());
        let reference = CredentialRef::new("github-pat").unwrap();
        store.set(&reference, &SecretString::new("secret")).unwrap();
        assert_eq!(
            store.get(&reference).unwrap().as_ref().map(|s| s.expose()),
            Some("secret")
        );

        store.delete(&reference).unwrap();
        assert_eq!(store.get(&reference).unwrap(), None);
        // The post-delete read reached the backend (cache was cleared).
        assert_eq!(*inner.hits.lock().unwrap(), 1);
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
    fn configured_token_returns_without_touching_storage() {
        let store = MemoryCredentialStore::default();
        // Point at a directory that does not exist: an explicitly configured
        // token must be validated and returned without creating anything.
        let missing = tempfile::tempdir().unwrap().path().join("does-not-exist");
        assert!(!missing.exists());
        let token = resolve_engine_token_with_store(
            &missing,
            Some("  explicit-token  ".to_owned()),
            &store,
        )
        .unwrap();
        assert_eq!(token, "explicit-token");
        assert!(!missing.exists());
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
        assert!(
            store
                .get(&engine_token_reference(dir.path()).unwrap())
                .unwrap()
                .is_none()
        );
        assert!(!dir.path().join(ENGINE_TOKEN_FILE).exists());
    }

    #[test]
    fn file_store_round_trips_and_isolates_references() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileCredentialStore::new(dir.path().join("credentials"));
        let a = CredentialRef::new("app-pem-1").unwrap();
        let b = CredentialRef::new("webhook-1").unwrap();

        // Missing entry reads as None, not an error.
        assert!(store.get(&a).unwrap().is_none());

        store.set(&a, &SecretString::new("pem-a")).unwrap();
        store.set(&b, &SecretString::new("secret-b")).unwrap();
        assert_eq!(store.get(&a).unwrap().unwrap().expose(), "pem-a");
        assert_eq!(store.get(&b).unwrap().unwrap().expose(), "secret-b");

        // Overwrite wins; delete removes only that reference.
        store.set(&a, &SecretString::new("pem-a2")).unwrap();
        assert_eq!(store.get(&a).unwrap().unwrap().expose(), "pem-a2");
        store.delete(&a).unwrap();
        assert!(store.get(&a).unwrap().is_none());
        assert_eq!(store.get(&b).unwrap().unwrap().expose(), "secret-b");
        // Deleting a missing reference is a no-op.
        store.delete(&a).unwrap();

        // Files are private: dir 0700, each credential 0600.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(store.dir.clone())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
            let cred = store.path_for(&b);
            assert_eq!(
                std::fs::metadata(cred).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn file_store_refuses_empty_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let cred_dir = dir.path().join("credentials");
        let reference = CredentialRef::new("pat").unwrap();

        let store = FileCredentialStore::new(&cred_dir);
        assert!(store.set(&reference, &SecretString::new("")).is_err());

        store.set(&reference, &SecretString::new("tok")).unwrap();
        // A fresh store over the same directory reads the persisted value —
        // this is the property that lets a rebuilt binary skip the keychain.
        let reopened = FileCredentialStore::new(&cred_dir);
        assert_eq!(reopened.get(&reference).unwrap().unwrap().expose(), "tok");
    }

    /// The file store must never write a secret through an existing
    /// (possibly world-readable) file handle: the mode-based protection
    /// cannot close the window between creating the file and tightening its
    /// permissions, so a writer has to create privately and rename. A reader
    /// that won that race holds a handle on the old file and must not see a
    /// later rotation.
    #[cfg(unix)]
    #[test]
    fn file_store_creates_private_and_never_writes_through_and_handle() {
        use std::io::Read as _;
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().unwrap();
        let store = FileCredentialStore::new(dir.path().join("credentials"));
        let reference = CredentialRef::new("webhook-secret").unwrap();
        store.set(&reference, &SecretString::new("first")).unwrap();
        let path = store.path_for(&reference);

        // Stand in for the reader that opened the file before any chmod ran.
        let mut raced = std::fs::File::open(&path).unwrap();

        store
            .set(&reference, &SecretString::new("rotated"))
            .unwrap();
        assert_eq!(store.get(&reference).unwrap().unwrap().expose(), "rotated");

        let mut seen = String::new();
        raced.read_to_string(&mut seen).unwrap();
        assert_eq!(
            seen, "first",
            "the rotated secret must not be visible through a handle opened before it was written"
        );

        // The replacement is private from creation, not after a chmod.
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(store.dir.clone())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }
}
