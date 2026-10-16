use anyhow::{Context, Result};
pub use preloop_gha_protocol::SecretString;
use rand::Rng;
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
    /// Read bypassing any read-through cache, to confirm a write persisted.
    /// Backends without a cache need not override this.
    fn get_fresh(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
        self.get(reference)
    }
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
    /// the store unavailable so that callers fall back to the file store.
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
    if cfg!(target_os = "macos") && matches!(error, keyring::Error::NoStorageAccess(_)) {
        return anyhow::anyhow!(
            "credential store denied {} of {:?} ({}): approve the macOS keychain dialog for the `preloop` service with Always Allow (one approval per item covers the process); \
             headless or background sessions cannot approve dialogs and will hang or fail here — use the default file store (`credential_store = \"file\"`) instead",
            op,
            reference,
            error,
        );
    }
    anyhow::Error::new(error).context(format!(
        "{op} credential {reference:?} in OS credential store"
    ))
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
/// probe is cheap and the fallback (the file store) is always safe.
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
                     securityd unreachable); using the file credential store",
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
             using the file credential store",
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

/// File-backed credential store and the default backend: one `0600` file per
/// credential under a private directory. No OS prompt and no keychain entry,
/// and credentials survive rebuilds and restarts. When configured to migrate
/// from the OS store, a miss consults that store and copies the value in; its
/// reachability is probed at most once per process.
///
/// Security note: this is the same trust level as the `engine.token` file
/// fallback — a private file readable only by the owning user. It is *not* the
/// OS keychain; choose `credential_store = "os"` where the keychain's access
/// control matters more than avoiding prompts.
#[derive(Clone)]
pub struct FileCredentialStore {
    dir: PathBuf,
    /// Consulted on a miss; a value found there is copied in. See
    /// [`FileCredentialStore::migrating_from`].
    migrate_from: Option<Arc<dyn CredentialStore>>,
}

impl FileCredentialStore {
    /// Store rooted at `dir` (created `0700` on first write).
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            migrate_from: None,
        }
    }

    /// Read through to `legacy` on a miss. A value found there is copied into
    /// this store and returned, so credentials an earlier build kept in the OS
    /// store keep resolving. `legacy` is only read, never written or deleted,
    /// and it is consulted only while it reports itself available. A read error
    /// there counts as a miss.
    pub fn migrating_from(mut self, legacy: Arc<dyn CredentialStore>) -> Self {
        self.migrate_from = Some(legacy);
        self
    }

    /// The directory the credentials live in.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Copy a miss in from the legacy store, when one is configured and
    /// reachable. The legacy entry is kept.
    fn migrate_in(&self, reference: &CredentialRef) -> Option<SecretString> {
        let legacy = self.migrate_from.as_ref()?;
        if let Err(error) = legacy.available() {
            tracing::debug!(
                store = legacy.name(),
                %error,
                "legacy credential store unavailable; not migrating"
            );
            return None;
        }
        let value = match legacy.get(reference) {
            Ok(Some(value)) => value,
            Ok(None) => return None,
            Err(error) => {
                tracing::warn!(
                    store = legacy.name(),
                    reference = reference.as_str(),
                    %error,
                    "could not read the legacy credential; treating it as missing"
                );
                return None;
            }
        };
        match self.set(reference, &value) {
            Ok(()) => tracing::info!(
                reference = reference.as_str(),
                from = legacy.name(),
                to = self.name(),
                "migrated credential into the file credential store; the legacy entry was kept"
            ),
            Err(error) => tracing::warn!(
                reference = reference.as_str(),
                %error,
                "could not copy the legacy credential into the file store; using it for this run only"
            ),
        }
        Some(value)
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

    /// Sidecar for [`Self::mark_primary_lost`], beside the credential file.
    fn marker_path_for(&self, reference: &CredentialRef) -> PathBuf {
        let digest = Sha256::digest(reference.as_str().as_bytes());
        self.dir.join(format!("{digest:x}.primary-lost"))
    }

    /// Record that the primary store lost this reference: it could not hold
    /// the value, and the file copy is now the credential of record. A stale
    /// or corrupted primary entry that stayed readable would otherwise be
    /// treated as authoritative on the next read and shadow this copy.
    pub fn mark_primary_lost(&self, reference: &CredentialRef) -> Result<()> {
        let path = self.marker_path_for(reference);
        write_private_file(
            &path,
            "the primary credential store could not hold this value\n",
        )
        .with_context(|| format!("write fallback marker {}", path.display()))
    }

    /// Whether [`Self::mark_primary_lost`] recorded this reference. Callers
    /// that mix a primary and this file store must read a marked reference
    /// from here, whatever the primary reports.
    pub fn primary_lost(&self, reference: &CredentialRef) -> bool {
        self.marker_path_for(reference).exists()
    }
}

impl CredentialStore for FileCredentialStore {
    fn get(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
        let path = self.path_for(reference);
        match std::fs::read_to_string(&path) {
            Ok(value) => Ok(Some(SecretString::new(value))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(self.migrate_in(reference)),
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
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).context(format!("delete credential file {}", path.display())),
        }
        let marker = self.marker_path_for(reference);
        match std::fs::remove_file(&marker) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).context(format!("delete fallback marker {}", marker.display())),
        }
    }

    fn name(&self) -> &'static str {
        "file credential store"
    }
}

/// Read-through cache in front of any [`CredentialStore`] backend, so each
/// distinct reference hits the backend at most once per process. The cache
/// exists for the OS backend: macOS prompts once per keychain hit for
/// binaries outside the item's trusted-apps list (ad-hoc-signed builds
/// change identity every release), so every subsystem re-read during boot
/// turned into another password dialog.
///
/// Misses are never cached, so an entry created mid-process is picked up on
/// the next read. Writes and deletes go through the cache too. The lock is
/// held across each backend call, so a read's cache fill cannot interleave
/// with a concurrent write or delete of the same reference and resurrect a
/// stale value. Operations are serialized; keychain prompts already were.
/// Availability is probed at most once per process: on the OS backend the
/// probe is the expensive macOS write test, and a file-store migration check
/// runs it on every miss otherwise.
/// [`selected_store`] shares one instance process-wide.
#[derive(Debug, Default)]
pub struct CachedCredentialStore<S> {
    inner: S,
    cache: parking_lot::Mutex<HashMap<CredentialRef, SecretString>>,
    available: parking_lot::Mutex<Option<std::result::Result<(), String>>>,
}

impl<S> CachedCredentialStore<S> {
    /// Wrap `inner`; the cache starts empty.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            cache: parking_lot::Mutex::new(HashMap::new()),
            available: parking_lot::Mutex::new(None),
        }
    }
}

impl<S: CredentialStore> CredentialStore for CachedCredentialStore<S> {
    fn get(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
        let mut cache = self.cache.lock();
        if let Some(cached) = cache.get(reference) {
            return Ok(Some(cached.clone()));
        }
        let value = self.inner.get(reference)?;
        if let Some(secret) = &value {
            cache.insert(reference.clone(), secret.clone());
        }
        Ok(value)
    }

    fn get_fresh(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
        let mut cache = self.cache.lock();
        let value = self.inner.get(reference)?;
        match &value {
            Some(secret) => {
                cache.insert(reference.clone(), secret.clone());
            }
            None => {
                cache.remove(reference);
            }
        }
        Ok(value)
    }

    fn set(&self, reference: &CredentialRef, value: &SecretString) -> Result<()> {
        let mut cache = self.cache.lock();
        self.inner.set(reference, value)?;
        cache.insert(reference.clone(), value.clone());
        Ok(())
    }

    fn delete(&self, reference: &CredentialRef) -> Result<()> {
        let mut cache = self.cache.lock();
        self.inner.delete(reference)?;
        cache.remove(reference);
        Ok(())
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn available(&self) -> Result<()> {
        let cached = self
            .available
            .lock()
            .get_or_insert_with(|| self.inner.available().map_err(|error| format!("{error:#}")))
            .clone();
        match cached {
            Ok(()) => Ok(()),
            Err(message) => Err(anyhow::anyhow!(message)),
        }
    }
}

/// Environment variable that overrides the configured credential store.
pub const CREDENTIAL_STORE_ENV: &str = "PRELOOP_CREDENTIAL_STORE";

/// A primary credential store with a private file fallback.
///
/// The primary remains authoritative whenever it returns a value for a
/// reference it did not lose. A missing entry, an unavailable store, or an
/// individual read failure falls through to the file store for that reference
/// only, so a setup write that had to fall back remains readable even while
/// `PRELOOP_CREDENTIAL_STORE=os` is still exported. A fallback write marks the
/// reference as lost by the primary, and a marked reference reads from the
/// file store even when the primary still returns a stale or corrupted value.
#[derive(Clone)]
pub struct FallbackCredentialStore {
    primary: Arc<dyn CredentialStore>,
    fallback: FileCredentialStore,
    primary_available: Arc<parking_lot::Mutex<Option<bool>>>,
}

impl FallbackCredentialStore {
    /// Wrap `primary`, using `fallback` for misses and primary-store errors.
    pub fn new(primary: Arc<dyn CredentialStore>, fallback: FileCredentialStore) -> Self {
        Self {
            primary,
            fallback,
            primary_available: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    fn primary_is_available(&self) -> bool {
        let mut available = self.primary_available.lock();
        if let Some(available) = *available {
            return available;
        }
        let result = self.primary.available().is_ok();
        *available = Some(result);
        result
    }

    /// Write `value` to the file fallback and mark the reference as lost by
    /// the primary store, so later reads of it prefer the fallback copy.
    fn set_fallback(&self, reference: &CredentialRef, value: &SecretString) -> Result<()> {
        self.fallback.set(reference, value)?;
        self.fallback.mark_primary_lost(reference)
    }
}

impl CredentialStore for FallbackCredentialStore {
    fn get(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
        // A reference the primary lost to a fallback write reads from the
        // fallback even while the primary still returns a stale or corrupted
        // value for it.
        if self.fallback.primary_lost(reference) {
            return self.fallback.get(reference);
        }
        if self.primary_is_available() {
            match self.primary.get(reference) {
                Ok(Some(value)) => return Ok(Some(value)),
                Ok(None) => {}
                Err(error) => {
                    tracing::debug!(
                        store = self.primary.name(),
                        reference = reference.as_str(),
                        %error,
                        "primary credential read failed for this reference; trying private file fallback"
                    );
                }
            }
        }
        self.fallback.get(reference)
    }

    fn get_fresh(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
        if self.fallback.primary_lost(reference) {
            return self.fallback.get_fresh(reference);
        }
        if self.primary_is_available() {
            match self.primary.get_fresh(reference) {
                Ok(Some(value)) => return Ok(Some(value)),
                Ok(None) => {}
                Err(error) => {
                    tracing::debug!(
                        store = self.primary.name(),
                        reference = reference.as_str(),
                        %error,
                        "primary credential readback failed for this reference; trying private file fallback"
                    );
                }
            }
        }
        self.fallback.get_fresh(reference)
    }

    fn set(&self, reference: &CredentialRef, value: &SecretString) -> Result<()> {
        if self.primary_is_available() {
            match self.primary.set(reference, value) {
                Ok(()) => {
                    // The primary holds the value again: retire the fallback
                    // copy and its marker so it cannot shadow later reads.
                    let _ = self.fallback.delete(reference);
                    return Ok(());
                }
                Err(error) => {
                    return self.set_fallback(reference, value).with_context(|| {
                        format!(
                            "store the credential in the file fallback after {} failed: \
                                 {error:#}",
                            self.primary.name()
                        )
                    });
                }
            }
        }
        self.set_fallback(reference, value)
    }

    fn delete(&self, reference: &CredentialRef) -> Result<()> {
        let primary_error = if self.primary_is_available() {
            self.primary.delete(reference).err()
        } else {
            None
        };
        // Also clears any primary-lost marker for the reference.
        let fallback_result = self.fallback.delete(reference);
        match primary_error {
            Some(error) => {
                fallback_result?;
                Err(error).context("delete credential from primary store")
            }
            None => fallback_result,
        }
    }

    fn name(&self) -> &'static str {
        self.primary.name()
    }

    fn available(&self) -> Result<()> {
        // The file backend is part of this composite store, so an unavailable
        // primary is not an unavailable selected store.
        let _ = self.primary_is_available();
        self.fallback.available()
    }
}

/// A credential backend, as chosen by [`selected_kind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialStoreKind {
    /// The host keychain: macOS Keychain, Windows Credential Manager, or the
    /// Secret Service on Linux. Chosen only explicitly.
    Os,
    /// `0600` files under `<state_dir>/credentials`. The default.
    File,
    /// Process-lifetime only. Tests and embedders.
    Memory,
}

/// Choose the credential backend for this process.
///
/// Precedence: `PRELOOP_CREDENTIAL_STORE` (`os`, `file`, or `memory`; a blank
/// value counts as unset), then the config file's `credential_store` (`os` or
/// `file`), then `file`. Both sources are validated even when the environment
/// wins, so a typo in the file never hides behind an override. An unknown value
/// is an error that names the valid ones; it never selects the OS store.
pub fn selected_kind(configured: Option<&str>) -> Result<CredentialStoreKind> {
    let env = std::env::var(CREDENTIAL_STORE_ENV).ok();
    kind_from(env.as_deref(), configured)
}

/// [`selected_kind`] over explicit inputs, so precedence can be tested without
/// touching the process environment.
fn kind_from(env: Option<&str>, configured: Option<&str>) -> Result<CredentialStoreKind> {
    let from_config = match configured.map(str::trim) {
        None => None,
        Some("os") => Some(CredentialStoreKind::Os),
        Some("file") => Some(CredentialStoreKind::File),
        Some(other) => anyhow::bail!(
            "invalid credential_store {other:?} in config.toml (expected \"file\" or \"os\"; \
             \"memory\" is only available through {CREDENTIAL_STORE_ENV})"
        ),
    };
    let from_env = match env.map(str::trim).filter(|value| !value.is_empty()) {
        None => None,
        Some("os") => Some(CredentialStoreKind::Os),
        Some("file") => Some(CredentialStoreKind::File),
        Some("memory") => Some(CredentialStoreKind::Memory),
        Some(other) => anyhow::bail!(
            "invalid {CREDENTIAL_STORE_ENV} value {other:?} (expected \"os\", \"file\", or \"memory\")"
        ),
    };
    Ok(from_env
        .or(from_config)
        .unwrap_or(CredentialStoreKind::File))
}

/// The credential store for the engine whose state lives in `state_dir`: the
/// backend [`selected_kind`] picks, with the file store under
/// `<state_dir>/credentials`. Callers reach a store only through this function
/// or [`engine_store`], so precedence is decided in one place.
///
/// The file store reads through to the OS store on a miss, so a credential an
/// earlier build wrote to the keychain stays reachable: a value found there is
/// copied into the file store, and the OS entry is never deleted. When `os` is
/// selected, the OS store is primary and this function still keeps the private
/// file store as a read/write fallback.
pub fn selected_store(
    state_dir: &Path,
    configured: Option<&str>,
) -> Result<Arc<dyn CredentialStore>> {
    selected_store_at(FileCredentialStore::default_dir(state_dir), configured)
}

/// Return the private credential directory associated with a config file.
///
/// Credentials live beside the config, not beside an unrelated
/// `PRELOOP_HOME`: `<config-parent>/credentials`. A relative config filename
/// with no parent uses `./credentials`.
pub fn credential_store_dir(config_path: &Path) -> PathBuf {
    let parent = config_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    FileCredentialStore::default_dir(parent)
}

/// [`selected_store`] using the credential directory beside `config_path`.
pub fn selected_store_for_config(
    config_path: &Path,
    configured: Option<&str>,
) -> Result<Arc<dyn CredentialStore>> {
    selected_store_at(credential_store_dir(config_path), configured)
}

fn selected_store_at(
    credentials_dir: PathBuf,
    configured: Option<&str>,
) -> Result<Arc<dyn CredentialStore>> {
    let file = FileCredentialStore::new(credentials_dir);
    Ok(match selected_kind(configured)? {
        // Not cached: a cache would only hide changes made by other processes.
        CredentialStoreKind::File => Arc::new(file.migrating_from(os_credential_store())),
        CredentialStoreKind::Os => {
            Arc::new(FallbackCredentialStore::new(os_credential_store(), file))
        }
        // Shared across calls: a long-lived process (or a test that loads a
        // config it just wrote) resolves and writes credentials through
        // separate calls, and a fresh store per call would forget every
        // credential the moment the caller's `Arc` drops.
        CredentialStoreKind::Memory => shared_memory_store(),
    })
}

/// [`selected_store`] for the engine whose config is active. An explicit
/// `PRELOOP_CONFIG` wins; otherwise the config is next to `state_dir`, so a
/// custom `PRELOOP_HOME` does not accidentally read `$HOME/.preloop/config.toml`.
/// Callers that already hold a parsed config pass its `credential_store` to
/// [`selected_store`] directly.
pub fn engine_store(state_dir: &Path) -> Result<Arc<dyn CredentialStore>> {
    let config_path = std::env::var_os(crate::config::CONFIG_PATH_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| state_dir.join("config.toml"));
    let configured = crate::config::credential_store_setting(&config_path)?;
    selected_store(state_dir, configured.as_deref())
}

/// Where a credential write landed.
#[derive(Debug)]
pub enum Written {
    /// The selected backend holds the value.
    Selected,
    /// The OS store could not hold the value, so the private file store does.
    /// `reason` says why the OS store was bypassed.
    FileFallback { reason: anyhow::Error },
}

/// Writes new credentials for an engine.
///
/// The selected backend takes each write when it can. An OS store that is
/// unavailable, refuses the write, or does not read the value back hands that
/// write to the private file store, and the writer stays on the file store
/// after that. On [`Written::FileFallback`] the caller records
/// `credential_store = "file"`, so the next start reads from the same place.
pub struct CredentialWriter {
    store: Arc<dyn CredentialStore>,
    file: FileCredentialStore,
    /// `store` is the OS store and may fall back to `file`.
    may_fall_back: bool,
    /// Writes currently land in `file`.
    on_file: bool,
}

impl CredentialWriter {
    /// The writer for the engine whose state lives in `state_dir`, under the
    /// backend [`selected_kind`] picks.
    pub fn for_home(state_dir: &Path, configured: Option<&str>) -> Result<Self> {
        Self::from_file_store(
            FileCredentialStore::new(FileCredentialStore::default_dir(state_dir)),
            configured,
        )
    }

    /// The writer for credentials associated with `config_path`.
    ///
    /// The private file fallback is always the `credentials` sibling of the
    /// config file, matching [`selected_store_for_config`].
    pub fn for_config(config_path: &Path, configured: Option<&str>) -> Result<Self> {
        Self::from_file_store(
            FileCredentialStore::new(credential_store_dir(config_path)),
            configured,
        )
    }

    fn from_file_store(file: FileCredentialStore, configured: Option<&str>) -> Result<Self> {
        Ok(match selected_kind(configured)? {
            CredentialStoreKind::Os => Self {
                store: os_credential_store(),
                file,
                may_fall_back: true,
                on_file: false,
            },
            CredentialStoreKind::File => Self {
                store: Arc::new(file.clone()),
                file,
                may_fall_back: false,
                on_file: true,
            },
            CredentialStoreKind::Memory => Self {
                store: shared_memory_store(),
                file,
                may_fall_back: false,
                on_file: false,
            },
        })
    }

    /// A writer whose first choice is `store` and whose fallback is `file`.
    /// Embedders and tests supply the OS-style backend this way.
    pub fn with_fallback(store: Arc<dyn CredentialStore>, file: FileCredentialStore) -> Self {
        Self {
            store,
            file,
            may_fall_back: true,
            on_file: false,
        }
    }

    /// The private file store's directory, where a fallback lands.
    pub fn file_dir(&self) -> &Path {
        self.file.dir()
    }

    /// Where the next write lands, for operator messages.
    pub fn destination(&self) -> String {
        if self.on_file {
            format!("the private file store at {}", self.file.dir().display())
        } else {
            format!("the {}", self.store.name())
        }
    }

    /// Store `value` under `reference`, reading it back to confirm.
    pub fn write(&mut self, reference: &CredentialRef, value: &SecretString) -> Result<Written> {
        if !self.may_fall_back {
            write_verified(self.store.as_ref(), reference, value)?;
            return Ok(Written::Selected);
        }
        let reason = match write_verified(self.store.as_ref(), reference, value) {
            Ok(()) => {
                // The primary holds the verified value: retire any fallback
                // copy from an earlier failure so it cannot shadow it.
                let _ = self.file.delete(reference);
                return Ok(Written::Selected);
            }
            Err(reason) => reason,
        };
        write_verified(&self.file, reference, value).with_context(|| {
            format!("store the credential in the file store after the OS store failed: {reason:#}")
        })?;
        // The primary could not hold this value (it may still return a stale
        // or corrupted one): mark the reference so reads prefer the file copy.
        self.file.mark_primary_lost(reference)?;
        self.store = Arc::new(self.file.clone());
        self.may_fall_back = false;
        self.on_file = true;
        Ok(Written::FileFallback { reason })
    }
}

/// Write `value` through `store`, then require a fresh read to return it. A
/// write that does not persist is an error, not a success.
fn write_verified(
    store: &dyn CredentialStore,
    reference: &CredentialRef,
    value: &SecretString,
) -> Result<()> {
    store.available()?;
    store.set(reference, value)?;
    match store.get_fresh(reference)? {
        Some(stored) if stored.expose() == value.expose() => Ok(()),
        Some(_) => anyhow::bail!(
            "{} returned a different value than was written for {reference:?}",
            store.name()
        ),
        None => anyhow::bail!(
            "{} did not keep the value written for {reference:?}",
            store.name()
        ),
    }
}

/// The OS store, shared process-wide so one keychain item prompts at most once
/// per process no matter how many subsystems load configuration.
fn os_credential_store() -> Arc<dyn CredentialStore> {
    static OS_STORE: std::sync::LazyLock<Arc<dyn CredentialStore>> =
        std::sync::LazyLock::new(os_backend);
    OS_STORE.clone()
}

/// The real keychain, read through a cache so repeated reads cost one lookup.
#[cfg(not(any(test, feature = "test-support")))]
fn os_backend() -> Arc<dyn CredentialStore> {
    Arc::new(CachedCredentialStore::new(OsCredentialStore))
}

/// Test builds never reach the host keychain: the OS backend is an in-memory
/// stand-in, because the macOS probe writes a throwaway keychain item.
#[cfg(any(test, feature = "test-support"))]
fn os_backend() -> Arc<dyn CredentialStore> {
    Arc::new(MemoryCredentialStore::default())
}

/// The process-wide memory credential store behind `PRELOOP_CREDENTIAL_STORE=memory`.
///
/// Direct `MemoryCredentialStore::default()` instances keep per-instance
/// isolation for tests; only the env-selected backend shares state so the
/// whole process sees one memory store, matching what `os`/`file` provide.
fn shared_memory_store() -> Arc<dyn CredentialStore> {
    static STORE: std::sync::LazyLock<Arc<dyn CredentialStore>> =
        std::sync::LazyLock::new(|| Arc::new(MemoryCredentialStore::default()));
    STORE.clone()
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

    /// `selected_store` with `PRELOOP_CREDENTIAL_STORE=memory` hands every caller the same process-wide
    /// store: a value written through one selection must be readable through
    /// the next. A fresh instance per call made the config-secrets key
    /// written at seal time unrecoverable when the load path asked a
    /// different empty store for it.
    #[tokio::test]
    async fn env_memory_store_shares_state_across_calls() {
        let _guard = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _env = crate::state::TestEnvVar::set("PRELOOP_CREDENTIAL_STORE", "memory");

        let dir = tempfile::tempdir().unwrap();
        let reference = CredentialRef::new("round-trip-across-store-from-env").unwrap();
        let writer = selected_store(dir.path(), None).unwrap();
        writer
            .set(&reference, &SecretString::new("shared-secret"))
            .unwrap();

        let reader = selected_store(dir.path(), None).unwrap();
        assert_eq!(
            reader
                .get(&reference)
                .unwrap()
                .as_ref()
                .map(|secret| secret.expose()),
            Some("shared-secret"),
            "a second selected_store call must see credentials written through the first"
        );

        // Cleanup for other tests that share the process-wide memory store.
        reader.delete(&reference).unwrap();
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

    /// Backend that acknowledges writes without persisting them.
    #[derive(Default)]
    struct DroppingCredentialStore;

    impl CredentialStore for DroppingCredentialStore {
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
            "dropping credential store"
        }
    }

    /// Backend whose first read parks until released, so a concurrent
    /// mutation can land while that read is in flight.
    #[derive(Default)]
    struct GatedCredentialStore {
        inner: MemoryCredentialStore,
        gate: parking_lot::Mutex<
            Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>,
        >,
    }

    impl CredentialStore for GatedCredentialStore {
        fn get(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
            let value = self.inner.get(reference)?;
            let gate = self.gate.lock().take();
            if let Some((entered, release)) = gate {
                entered.send(()).unwrap();
                release.recv().unwrap();
            }
            Ok(value)
        }

        fn set(&self, reference: &CredentialRef, value: &SecretString) -> Result<()> {
            self.inner.set(reference, value)
        }

        fn delete(&self, reference: &CredentialRef) -> Result<()> {
            self.inner.delete(reference)
        }

        fn name(&self) -> &'static str {
            "gated credential store"
        }
    }

    #[test]
    fn get_fresh_observes_the_backend_and_refreshes_the_cache() {
        let inner = CountingCredentialStore::default();
        let store = CachedCredentialStore::new(inner.clone());
        let reference = CredentialRef::new("github-pat").unwrap();
        store.set(&reference, &SecretString::new("secret")).unwrap();

        // Another writer changes the backend behind the cache.
        inner
            .inner
            .set(&reference, &SecretString::new("rotated"))
            .unwrap();
        assert_eq!(
            store.get(&reference).unwrap().as_ref().map(|s| s.expose()),
            Some("secret")
        );
        assert_eq!(
            store
                .get_fresh(&reference)
                .unwrap()
                .as_ref()
                .map(|s| s.expose()),
            Some("rotated")
        );
        assert_eq!(
            store.get(&reference).unwrap().as_ref().map(|s| s.expose()),
            Some("rotated")
        );
    }

    #[test]
    fn get_fresh_reports_a_write_the_backend_dropped() {
        let store = CachedCredentialStore::new(DroppingCredentialStore);
        let reference = CredentialRef::new("config-secrets-key").unwrap();
        store.set(&reference, &SecretString::new("key")).unwrap();

        // The cached read echoes the input; only a backend read shows the drop.
        assert!(store.get(&reference).unwrap().is_some());
        assert_eq!(store.get_fresh(&reference).unwrap(), None);
    }

    #[test]
    fn concurrent_misses_read_the_backend_once() {
        let inner = CountingCredentialStore::default();
        let reference = CredentialRef::new("github-pat").unwrap();
        inner
            .inner
            .set(&reference, &SecretString::new("secret"))
            .unwrap();
        let store = CachedCredentialStore::new(inner.clone());

        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    assert_eq!(
                        store.get(&reference).unwrap().as_ref().map(|s| s.expose()),
                        Some("secret")
                    );
                });
            }
        });
        assert_eq!(*inner.hits.lock().unwrap(), 1);
    }

    #[test]
    fn in_flight_read_cannot_resurrect_a_deleted_credential() {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let inner = GatedCredentialStore::default();
        let reference = CredentialRef::new("github-pat").unwrap();
        inner
            .inner
            .set(&reference, &SecretString::new("old"))
            .unwrap();
        *inner.gate.lock() = Some((entered_tx, release_rx));
        let store = CachedCredentialStore::new(inner);

        std::thread::scope(|scope| {
            let reader = scope.spawn(|| store.get(&reference).unwrap());
            // The reader is inside the backend read, holding the old value.
            entered_rx.recv().unwrap();
            let deleter = scope.spawn(|| store.delete(&reference).unwrap());
            // Give the delete time to run; it must wait for the read to finish.
            std::thread::sleep(std::time::Duration::from_millis(50));
            release_tx.send(()).unwrap();
            reader.join().unwrap();
            deleter.join().unwrap();
        });
        assert_eq!(store.get(&reference).unwrap(), None);
    }

    #[test]
    fn non_denial_os_errors_do_not_suggest_a_keychain_dialog() {
        let reference = CredentialRef::new("github-pat").unwrap();
        let error = deny_helpful_error(
            keyring::Error::Invalid("attribute".into(), "bad".into()),
            &reference,
            "read",
        );
        assert!(!format!("{error:#}").contains("Always Allow"));
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

    /// Stand-in for the OS store in the migration and fallback tests:
    /// reachability and read failures are scripted, and reads are counted.
    #[derive(Clone)]
    struct LegacyOsStore {
        inner: MemoryCredentialStore,
        reachable: bool,
        fail_reads: bool,
        reads: Arc<parking_lot::Mutex<usize>>,
    }

    impl LegacyOsStore {
        fn reachable() -> Self {
            Self {
                inner: MemoryCredentialStore::default(),
                reachable: true,
                fail_reads: false,
                reads: Arc::default(),
            }
        }

        fn reads(&self) -> usize {
            *self.reads.lock()
        }
    }

    impl CredentialStore for LegacyOsStore {
        fn get(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
            *self.reads.lock() += 1;
            if self.fail_reads {
                anyhow::bail!("keychain denied the read");
            }
            self.inner.get(reference)
        }

        fn set(&self, reference: &CredentialRef, value: &SecretString) -> Result<()> {
            self.inner.set(reference, value)
        }

        fn delete(&self, reference: &CredentialRef) -> Result<()> {
            self.inner.delete(reference)
        }

        fn name(&self) -> &'static str {
            "legacy os store"
        }

        fn available(&self) -> Result<()> {
            if self.reachable {
                Ok(())
            } else {
                anyhow::bail!("no keychain")
            }
        }
    }

    /// Refuses every write, like a keychain that says no.
    #[derive(Clone, Copy, Default)]
    struct RefusingCredentialStore;

    impl CredentialStore for RefusingCredentialStore {
        fn get(&self, _reference: &CredentialRef) -> Result<Option<SecretString>> {
            Ok(None)
        }

        fn set(&self, _reference: &CredentialRef, _value: &SecretString) -> Result<()> {
            anyhow::bail!("keychain refused the write")
        }

        fn delete(&self, _reference: &CredentialRef) -> Result<()> {
            Ok(())
        }

        fn name(&self) -> &'static str {
            "refusing credential store"
        }
    }

    #[test]
    fn os_primary_reads_and_writes_through_file_fallbacks() {
        let reference = CredentialRef::new("github-pat-fallback").unwrap();
        let mut read_error = LegacyOsStore::reachable();
        read_error.fail_reads = true;
        let cases: [(&str, Arc<dyn CredentialStore>); 3] = [
            ("unavailable", Arc::new(UnavailableCredentialStore)),
            ("read error", Arc::new(read_error)),
            ("write error", Arc::new(RefusingCredentialStore)),
        ];

        for (case, primary) in cases {
            let dir = tempfile::tempdir().unwrap();
            let fallback = FileCredentialStore::new(dir.path().join("credentials"));
            fallback
                .set(&reference, &SecretString::new("file-value"))
                .unwrap();
            let store = FallbackCredentialStore::new(primary, fallback.clone());
            assert_eq!(
                store
                    .get(&reference)
                    .unwrap()
                    .as_ref()
                    .map(|value| value.expose()),
                Some("file-value"),
                "{case}: file fallback was not readable"
            );
        }

        let dir = tempfile::tempdir().unwrap();
        let fallback = FileCredentialStore::new(dir.path().join("credentials"));
        let store =
            FallbackCredentialStore::new(Arc::new(RefusingCredentialStore), fallback.clone());
        store
            .set(&reference, &SecretString::new("written-through"))
            .unwrap();
        assert_eq!(
            fallback
                .get(&reference)
                .unwrap()
                .as_ref()
                .map(|value| value.expose()),
            Some("written-through"),
            "a primary write failure must persist to the file fallback"
        );

        let dir = tempfile::tempdir().unwrap();
        let fallback = FileCredentialStore::new(dir.path().join("credentials"));
        fallback
            .set(&reference, &SecretString::new("file-value"))
            .unwrap();
        let primary = LegacyOsStore::reachable();
        primary
            .inner
            .set(&reference, &SecretString::new("os-value"))
            .unwrap();
        let store = FallbackCredentialStore::new(Arc::new(primary), fallback);
        assert_eq!(
            store
                .get(&reference)
                .unwrap()
                .as_ref()
                .map(|value| value.expose()),
            Some("os-value"),
            "a successful OS value must remain authoritative over the file fallback"
        );
    }

    #[test]
    fn env_beats_config_which_beats_the_file_default() {
        assert_eq!(kind_from(None, None).unwrap(), CredentialStoreKind::File);
        assert_eq!(
            kind_from(None, Some("os")).unwrap(),
            CredentialStoreKind::Os
        );
        assert_eq!(
            kind_from(None, Some(" file ")).unwrap(),
            CredentialStoreKind::File
        );
        assert_eq!(
            kind_from(Some("os"), Some("file")).unwrap(),
            CredentialStoreKind::Os
        );
        assert_eq!(
            kind_from(Some("file"), Some("os")).unwrap(),
            CredentialStoreKind::File
        );
        assert_eq!(
            kind_from(Some("memory"), Some("os")).unwrap(),
            CredentialStoreKind::Memory
        );
    }

    #[test]
    fn a_blank_env_value_counts_as_unset() {
        assert_eq!(
            kind_from(Some("  "), Some("os")).unwrap(),
            CredentialStoreKind::Os
        );
        assert_eq!(
            kind_from(Some(""), None).unwrap(),
            CredentialStoreKind::File
        );
    }

    #[test]
    fn unknown_values_are_errors_and_never_select_the_os_store() {
        let env_error = format!("{:#}", kind_from(Some("keychain"), None).unwrap_err());
        assert!(
            env_error.contains("PRELOOP_CREDENTIAL_STORE")
                && env_error.contains("\"os\", \"file\", or \"memory\""),
            "{env_error}"
        );
        assert!(
            kind_from(Some("OS"), None).is_err(),
            "values are case-sensitive"
        );
        assert!(
            kind_from(Some("keychain"), Some("os")).is_err(),
            "a bad env value must not fall through to the config"
        );
        let config_error = format!("{:#}", kind_from(None, Some("ops")).unwrap_err());
        assert!(
            config_error.contains("credential_store")
                && config_error.contains("\"file\" or \"os\""),
            "{config_error}"
        );
        assert!(
            kind_from(None, Some("memory")).is_err(),
            "memory is only selectable through the environment"
        );
        assert!(kind_from(None, Some("")).is_err());
        assert!(
            kind_from(Some("file"), Some("ops")).is_err(),
            "a bad config value is reported even when the environment overrides it"
        );
    }

    #[test]
    fn file_store_reads_through_a_legacy_entry_once_and_keeps_it() {
        let dir = tempfile::tempdir().unwrap();
        let credentials = dir.path().join("credentials");
        let reference = CredentialRef::new("github-app-pem-123").unwrap();
        let legacy = LegacyOsStore::reachable();
        legacy
            .inner
            .set(&reference, &SecretString::new("LEGACY\n"))
            .unwrap();
        let store = FileCredentialStore::new(&credentials).migrating_from(Arc::new(legacy.clone()));

        assert_eq!(store.get(&reference).unwrap().unwrap().expose(), "LEGACY\n");
        assert_eq!(
            FileCredentialStore::new(&credentials)
                .get(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "LEGACY\n",
            "the migrated value must now live in the file store"
        );
        assert_eq!(legacy.reads(), 1);
        // Served from the file store from now on: the legacy store is not asked again.
        assert_eq!(store.get(&reference).unwrap().unwrap().expose(), "LEGACY\n");
        assert_eq!(legacy.reads(), 1);
        // The legacy entry is never deleted.
        assert_eq!(
            legacy.inner.get(&reference).unwrap().unwrap().expose(),
            "LEGACY\n"
        );
    }

    #[test]
    fn file_store_ignores_an_unreachable_or_failing_legacy_store() {
        let dir = tempfile::tempdir().unwrap();
        let credentials = dir.path().join("credentials");
        let reference = CredentialRef::new("github-pat").unwrap();

        let mut unreachable = LegacyOsStore::reachable();
        unreachable.reachable = false;
        unreachable
            .inner
            .set(&reference, &SecretString::new("hidden"))
            .unwrap();
        let store =
            FileCredentialStore::new(&credentials).migrating_from(Arc::new(unreachable.clone()));
        assert!(store.get(&reference).unwrap().is_none());
        assert_eq!(
            unreachable.reads(),
            0,
            "an unreachable store must not be read"
        );

        let mut failing = LegacyOsStore::reachable();
        failing.fail_reads = true;
        let store = FileCredentialStore::new(&credentials).migrating_from(Arc::new(failing));
        assert!(
            store.get(&reference).unwrap().is_none(),
            "a legacy read error is treated as a miss"
        );
        assert!(
            FileCredentialStore::new(&credentials)
                .get(&reference)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn engine_token_is_migrated_from_a_legacy_os_entry() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = LegacyOsStore::reachable();
        let reference = engine_token_reference(dir.path()).unwrap();
        legacy
            .inner
            .set(&reference, &SecretString::new("legacy-admin-token"))
            .unwrap();
        let store = FileCredentialStore::new(FileCredentialStore::default_dir(dir.path()))
            .migrating_from(Arc::new(legacy));

        let token = resolve_engine_token_with_store(dir.path(), None, &store).unwrap();
        assert_eq!(
            token, "legacy-admin-token",
            "an existing token must be kept"
        );
        assert_eq!(
            load_engine_token_with_store(dir.path(), &store).unwrap(),
            Some("legacy-admin-token".to_owned())
        );
        assert!(!dir.path().join(ENGINE_TOKEN_FILE).exists());
    }

    #[test]
    fn file_store_round_trips_multiline_pem_byte_exactly() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileCredentialStore::new(dir.path().join("credentials"));
        let reference = CredentialRef::new("github-app-pem-55").unwrap();
        for pem in [
            "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA\nabc/def+ghi=\n-----END RSA PRIVATE KEY-----\n",
            "-----BEGIN RSA PRIVATE KEY-----\r\nMIIEpAIBAAKCAQEA\r\n-----END RSA PRIVATE KEY-----",
        ] {
            store.set(&reference, &SecretString::new(pem)).unwrap();
            assert_eq!(store.get(&reference).unwrap().unwrap().expose(), pem);
        }
    }

    #[test]
    fn writer_falls_back_to_the_file_store_when_the_os_store_cannot_keep_a_write() {
        let cases: [(&str, Arc<dyn CredentialStore>); 4] = [
            ("unavailable", Arc::new(UnavailableCredentialStore)),
            ("refuses the write", Arc::new(RefusingCredentialStore)),
            ("does not persist", Arc::new(NonPersistingCredentialStore)),
            ("cannot read back", Arc::new(WriteOnlyCredentialStore)),
        ];
        let pem = "-----BEGIN RSA PRIVATE KEY-----\nAAAA\nBBBB\n-----END RSA PRIVATE KEY-----\n";
        for (case, primary) in cases {
            let dir = tempfile::tempdir().unwrap();
            let credentials = dir.path().join("credentials");
            let mut writer =
                CredentialWriter::with_fallback(primary, FileCredentialStore::new(&credentials));
            let reference = CredentialRef::new("github-app-pem-7").unwrap();

            let Written::FileFallback { reason } =
                writer.write(&reference, &SecretString::new(pem)).unwrap()
            else {
                panic!("{case}: expected a file-store fallback");
            };
            assert!(!format!("{reason:#}").is_empty(), "{case}");
            assert_eq!(
                FileCredentialStore::new(&credentials)
                    .get(&reference)
                    .unwrap()
                    .unwrap()
                    .expose(),
                pem,
                "{case}: the PEM must be stored byte-exactly in the file store"
            );
            assert_eq!(writer.file_dir(), credentials.as_path(), "{case}");
            assert!(
                writer
                    .destination()
                    .starts_with("the private file store at "),
                "{case}"
            );

            // Later writes stay on the file store and report no fallback.
            let next = CredentialRef::new("github-app-webhook-7").unwrap();
            assert!(
                matches!(
                    writer.write(&next, &SecretString::new("hook")).unwrap(),
                    Written::Selected
                ),
                "{case}"
            );
        }
    }

    /// Without an OS store there is nothing to fall back to: a failed write is reported.
    #[tokio::test]
    async fn file_writer_reports_its_own_failures() {
        let _guard = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _env = crate::state::TestEnvVar::unset(CREDENTIAL_STORE_ENV);
        let dir = tempfile::tempdir().unwrap();
        // A plain file where the credentials directory belongs makes every write fail.
        std::fs::write(dir.path().join("credentials"), "not a directory").unwrap();
        let mut writer = CredentialWriter::for_home(dir.path(), None).unwrap();
        let reference = CredentialRef::new("github-pat").unwrap();
        assert!(writer.write(&reference, &SecretString::new("pat")).is_err());
    }

    #[tokio::test]
    async fn writer_for_home_defaults_to_the_file_store_under_the_home() {
        let _guard = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _env = crate::state::TestEnvVar::unset(CREDENTIAL_STORE_ENV);
        let dir = tempfile::tempdir().unwrap();
        let reference = CredentialRef::new("github-app-pem-9").unwrap();
        let mut writer = CredentialWriter::for_home(dir.path(), None).unwrap();
        assert!(matches!(
            writer.write(&reference, &SecretString::new("pem")).unwrap(),
            Written::Selected
        ));
        let credentials = dir.path().join("credentials");
        assert_eq!(
            FileCredentialStore::new(&credentials)
                .get(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "pem"
        );
        assert_eq!(
            writer.destination(),
            format!("the private file store at {}", credentials.display())
        );
    }

    /// The environment override beats the config key, and the config key beats
    /// the default. Callers reach these through `selected_store`.
    #[tokio::test]
    async fn selected_store_applies_env_then_config_then_default() {
        let _guard = crate::state::GITHUB_ENV_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        {
            let _env = crate::state::TestEnvVar::unset(CREDENTIAL_STORE_ENV);
            assert_eq!(
                selected_store(dir.path(), None).unwrap().name(),
                "file credential store"
            );
        }
        {
            let _env = crate::state::TestEnvVar::set(CREDENTIAL_STORE_ENV, "memory");
            assert_eq!(
                selected_store(dir.path(), Some("file")).unwrap().name(),
                "memory credential store"
            );
        }
    }

    /// A store that corrupts every write — the motivating failure: a keychain
    /// that accepts the write and hands back mangled bytes afterwards.
    struct CorruptingStore {
        stored: parking_lot::Mutex<Option<String>>,
    }

    impl CredentialStore for CorruptingStore {
        fn get(&self, _reference: &CredentialRef) -> Result<Option<SecretString>> {
            Ok(self
                .stored
                .lock()
                .as_ref()
                .map(|value| SecretString::new(value.clone())))
        }

        fn set(&self, _reference: &CredentialRef, value: &SecretString) -> Result<()> {
            let raw = value.expose();
            *self.stored.lock() = Some(raw[..raw.len() / 2].to_owned());
            Ok(())
        }

        fn delete(&self, _reference: &CredentialRef) -> Result<()> {
            Ok(())
        }

        fn name(&self) -> &'static str {
            "corrupting store"
        }
    }

    /// A store that keeps its values but refuses every write, like a keychain
    /// that will not accept a rotation.
    struct RefusingWriteStore {
        inner: MemoryCredentialStore,
    }

    impl CredentialStore for RefusingWriteStore {
        fn get(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
            self.inner.get(reference)
        }

        fn set(&self, _reference: &CredentialRef, _value: &SecretString) -> Result<()> {
            anyhow::bail!("keychain refused the write")
        }

        fn delete(&self, reference: &CredentialRef) -> Result<()> {
            self.inner.delete(reference)
        }

        fn name(&self) -> &'static str {
            "refusing-write store"
        }
    }

    /// A store whose reads fail only for chosen references: one denied
    /// keychain item must not disable the rest of the store.
    struct SelectiveReadStore {
        inner: MemoryCredentialStore,
        denied: std::collections::HashSet<String>,
    }

    impl CredentialStore for SelectiveReadStore {
        fn get(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
            if self.denied.contains(reference.as_str()) {
                anyhow::bail!("keychain denied the read of {reference:?}");
            }
            self.inner.get(reference)
        }

        fn set(&self, reference: &CredentialRef, value: &SecretString) -> Result<()> {
            self.inner.set(reference, value)
        }

        fn delete(&self, reference: &CredentialRef) -> Result<()> {
            self.inner.delete(reference)
        }

        fn name(&self) -> &'static str {
            "selective-read store"
        }
    }

    /// Counts `available` probes so the cache can be asserted on.
    struct CountingProbeStore {
        inner: MemoryCredentialStore,
        probes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        fails: bool,
    }

    impl CredentialStore for CountingProbeStore {
        fn get(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
            self.inner.get(reference)
        }

        fn set(&self, reference: &CredentialRef, value: &SecretString) -> Result<()> {
            self.inner.set(reference, value)
        }

        fn delete(&self, reference: &CredentialRef) -> Result<()> {
            self.inner.delete(reference)
        }

        fn name(&self) -> &'static str {
            "counting probe store"
        }

        fn available(&self) -> Result<()> {
            self.probes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.fails {
                anyhow::bail!("probe failed");
            }
            Ok(())
        }
    }

    /// The motivating failure end to end: the primary corrupts the write, the
    /// verified fallback file copy holds the real value, and a later read
    /// through the composite store returns it — not the corrupt primary entry.
    #[test]
    fn a_fallback_write_is_authoritative_over_a_corrupt_primary_value() {
        let dir = tempfile::tempdir().unwrap();
        let file = FileCredentialStore::new(dir.path().join("credentials"));
        let reference = CredentialRef::new("github-app-pem-4242").unwrap();
        let os = Arc::new(CorruptingStore {
            stored: parking_lot::Mutex::new(None),
        });
        let mut writer = CredentialWriter::with_fallback(os.clone(), file.clone());
        assert!(
            matches!(
                writer
                    .write(&reference, &SecretString::new("GOOD-PEM-VALUE"))
                    .unwrap(),
                Written::FileFallback { .. }
            ),
            "a corrupting store must fail the verified write"
        );
        assert_eq!(
            file.get(&reference).unwrap().unwrap().expose(),
            "GOOD-PEM-VALUE"
        );
        let load = FallbackCredentialStore::new(os, file);
        assert_eq!(
            load.get(&reference).unwrap().unwrap().expose(),
            "GOOD-PEM-VALUE",
            "the corrupt primary entry must not shadow the verified file copy"
        );
    }

    /// A refused rotation must not leave the old primary value in force: the
    /// new value lands in the file fallback and reads prefer it.
    #[test]
    fn a_refused_rotation_leaves_the_new_value_authoritative() {
        let dir = tempfile::tempdir().unwrap();
        let file = FileCredentialStore::new(dir.path().join("credentials"));
        let reference = CredentialRef::new("github-webhook-1").unwrap();
        let os = Arc::new(RefusingWriteStore {
            inner: MemoryCredentialStore::default(),
        });
        os.inner.set(&reference, &SecretString::new("old")).unwrap();
        let mut writer = CredentialWriter::with_fallback(os.clone(), file.clone());
        assert!(
            matches!(
                writer.write(&reference, &SecretString::new("new")).unwrap(),
                Written::FileFallback { .. }
            ),
            "a refusing store must fail the verified write"
        );
        let load = FallbackCredentialStore::new(os, file);
        assert_eq!(
            load.get(&reference).unwrap().unwrap().expose(),
            "new",
            "the refused rotation must not leave the old value in force"
        );
    }

    /// One denied keychain item falls back for that reference only; the rest
    /// of the store keeps working.
    #[test]
    fn a_primary_read_failure_stays_scoped_to_one_reference() {
        let dir = tempfile::tempdir().unwrap();
        let file = FileCredentialStore::new(dir.path().join("credentials"));
        let denied = CredentialRef::new("denied-item").unwrap();
        let healthy = CredentialRef::new("healthy-item").unwrap();
        let os = Arc::new(SelectiveReadStore {
            inner: MemoryCredentialStore::default(),
            denied: [denied.as_str().to_owned()].into_iter().collect(),
        });
        os.inner
            .set(&healthy, &SecretString::new("os-value"))
            .unwrap();
        file.set(&denied, &SecretString::new("file-value")).unwrap();
        let store = FallbackCredentialStore::new(os, file);
        assert_eq!(
            store.get(&denied).unwrap().unwrap().expose(),
            "file-value",
            "the denied reference falls back to the file store"
        );
        assert_eq!(
            store.get(&healthy).unwrap().unwrap().expose(),
            "os-value",
            "one denied item must not disable the OS store for the rest"
        );
    }

    /// A successful primary write retires the fallback copy and its marker,
    /// so a stale file value cannot shadow the fresh primary one.
    #[test]
    fn a_successful_primary_write_retires_the_fallback_copy() {
        let dir = tempfile::tempdir().unwrap();
        let file = FileCredentialStore::new(dir.path().join("credentials"));
        let reference = CredentialRef::new("github-pat").unwrap();
        file.set(&reference, &SecretString::new("stale-file-value"))
            .unwrap();
        file.mark_primary_lost(&reference).unwrap();
        let primary = Arc::new(MemoryCredentialStore::default());
        let store = FallbackCredentialStore::new(primary, file.clone());
        store
            .set(&reference, &SecretString::new("fresh-os-value"))
            .unwrap();
        assert!(!file.primary_lost(&reference), "the marker must be gone");
        assert_eq!(
            file.get(&reference).unwrap(),
            None,
            "the stale fallback copy must be gone"
        );
        assert_eq!(
            store.get(&reference).unwrap().unwrap().expose(),
            "fresh-os-value"
        );
    }

    /// The reachability probe is cached: a file-store migration miss must not
    /// re-probe the keychain on every miss.
    #[test]
    fn the_availability_probe_is_cached_per_process() {
        let probes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let store = CachedCredentialStore::new(CountingProbeStore {
            inner: MemoryCredentialStore::default(),
            probes: probes.clone(),
            fails: false,
        });
        store.available().unwrap();
        store.available().unwrap();
        assert_eq!(probes.load(std::sync::atomic::Ordering::SeqCst), 1);

        let failing_probes = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let failing = CachedCredentialStore::new(CountingProbeStore {
            inner: MemoryCredentialStore::default(),
            probes: failing_probes.clone(),
            fails: true,
        });
        assert!(failing.available().is_err());
        assert!(failing.available().is_err());
        assert_eq!(
            failing_probes.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a failing probe is cached too"
        );
    }
}
