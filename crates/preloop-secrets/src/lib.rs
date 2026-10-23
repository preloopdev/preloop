//! Shared synchronous secret backends and redacted secret values.
mod secret;
pub use secret::SecretString;

/// Reference identifiers and secret backend primitives are kept in this crate
/// so callers do not depend on the engine.
pub mod reference {
    pub use super::CredentialRef;
}
/// Backend trait and erased namespace handles.
pub mod backend {
    pub use super::CredentialStore as SecretBackend;
}
/// Cache and fallback backend layers.
pub mod layers {
    pub use super::{
        CachedCredentialStore as CachedBackend, FallbackCredentialStore as FallbackBackend,
        UnavailableCredentialStore as UnavailableBackend,
    };
}
/// Backend selection by configuration and environment.
pub mod select {
    pub use super::{CREDENTIAL_STORE_ENV, CredentialStoreKind, selected_kind, selected_store};
}
/// Private filesystem primitives.
pub mod private_fs {}
/// Concrete memory, file, and operating-system backends.
pub mod backends {
    pub use super::{
        FileCredentialStore as FileBackend, MemoryCredentialStore as MemoryBackend,
        OsCredentialStore as OsBackend,
    };
}

use anyhow::{Context, Result};
use preloop_observability::deadline::{DeadlineGuard, GuardError, run_command_blocking};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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

    fn internal(value: String) -> Self {
        Self(value)
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

/// Secret storage backend. Implementations must never log values.
pub trait CredentialStore: Send + Sync + 'static {
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
    /// Return a namespace-isolated handle.
    fn scoped(self: Arc<Self>, namespace: &str) -> ScopedBackend
    where
        Self: Sized,
    {
        ScopedBackend::new(self, namespace)
    }
}

/// The extracted backend trait name.
pub use CredentialStore as SecretBackend;

/// A namespace-isolated backend view.
pub struct ScopedBackend {
    inner: Arc<dyn CredentialStore>,
    namespace: String,
}

impl ScopedBackend {
    fn new(inner: Arc<dyn CredentialStore>, namespace: &str) -> Self {
        assert!(!namespace.is_empty(), "secret namespace must not be empty");
        assert!(
            namespace
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_')),
            "secret namespace contains an invalid character"
        );
        Self {
            inner,
            namespace: namespace.to_owned(),
        }
    }

    fn map(&self, reference: &CredentialRef) -> CredentialRef {
        if self.namespace == "engine" {
            reference.clone()
        } else {
            CredentialRef::internal(format!("{}:{}", self.namespace, reference.as_str()))
        }
    }
}

impl CredentialStore for ScopedBackend {
    fn get(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
        self.inner.get(&self.map(reference))
    }
    fn get_fresh(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
        self.inner.get_fresh(&self.map(reference))
    }
    fn set(&self, reference: &CredentialRef, value: &SecretString) -> Result<()> {
        self.inner.set(&self.map(reference), value)
    }
    fn delete(&self, reference: &CredentialRef) -> Result<()> {
        self.inner.delete(&self.map(reference))
    }
    fn name(&self) -> &'static str {
        self.inner.name()
    }
    fn available(&self) -> Result<()> {
        self.inner.available()
    }
}

/// Scope an already erased backend, useful for stores held behind `Arc<dyn ...>`.
pub fn scoped_backend(inner: Arc<dyn CredentialStore>, namespace: &str) -> ScopedBackend {
    ScopedBackend::new(inner, namespace)
}

/// Deadline for one call into the host OS credential store.
///
/// These stores block in OS IPC with no timeout of their own — macOS waits on
/// `securityd` behind an unlock dialog, Linux on a Secret Service prompt,
/// Windows on a stalled Credential Manager RPC — and nothing answers a prompt
/// on an unattended host. 10 s is far beyond a healthy store round trip, yet short
/// enough that a caller fails while someone is still watching.
const OS_STORE_TIMEOUT: Duration = Duration::from_secs(10);

/// The error text for a store that missed its deadline.
///
/// Shared by the production guard and the tests so the message an operator
/// sees is the message the tests assert on.
fn unresponsive_store_error(timeout: Duration) -> String {
    format!(
        "the operating-system credential store did not answer within {}s and is probably \
         waiting on an unlock prompt or a hung keychain or Secret Service daemon; later calls \
         fail immediately. Use the default file store (`credential_store = \"file\"`) to keep \
         secrets out of the OS store",
        timeout.as_secs_f32()
    )
}

/// The host operating system's native credential store.
///
/// Every call runs behind a [`DeadlineGuard`] (dedicated thread, hard
/// deadline, fail-fast after the first expiry), and after one unanswered call
/// the store is treated as unresponsive for the rest of the process. The guard
/// is the shared implementation in `preloop_observability::deadline`, so the
/// engine has exactly one bounded-wait policy.
/// Existing keychain service used by engine credentials.
pub const OS_STORE_SERVICE: &str = "preloop";
/// Native operating-system credential backend.
#[derive(Debug, Clone)]
pub struct OsCredentialStore {
    guard: DeadlineGuard,
}

impl Default for OsCredentialStore {
    fn default() -> Self {
        Self {
            guard: DeadlineGuard::new(
                OS_STORE_TIMEOUT,
                "os-credential-store",
                unresponsive_store_error(OS_STORE_TIMEOUT),
            ),
        }
    }
}

impl OsCredentialStore {
    fn entry(reference: &CredentialRef) -> Result<keyring::Entry> {
        keyring::Entry::new(OS_STORE_SERVICE, reference.as_str())
            .context("create OS credential entry")
    }

    /// Run a blocking store call behind the shared deadline guard, mapping the
    /// guard's generic error back into this module's `anyhow` world.
    fn guarded<T>(&self, operation: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T>
    where
        T: Send + 'static,
    {
        self.guard.run(operation).map_err(|error| match error {
            GuardError::Operation(error) => error,
            other => anyhow::Error::msg(other.to_string()),
        })
    }
}

impl CredentialStore for OsCredentialStore {
    fn get(&self, reference: &CredentialRef) -> Result<Option<SecretString>> {
        let reference = reference.clone();
        self.guarded(move || match Self::entry(&reference)?.get_password() {
            Ok(value) => Ok(Some(SecretString::new(value))),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(error) => Err(deny_helpful_error(error, &reference, "read")),
        })
    }

    fn set(&self, reference: &CredentialRef, value: &SecretString) -> Result<()> {
        if value.expose().is_empty() {
            anyhow::bail!("refusing to store an empty credential");
        }
        let reference = reference.clone();
        let value = value.clone();
        self.guarded(move || {
            Self::entry(&reference)?
                .set_password(value.expose())
                .map_err(|error| deny_helpful_error(error, &reference, "write"))?;
            Ok(())
        })
    }

    fn delete(&self, reference: &CredentialRef) -> Result<()> {
        let reference = reference.clone();
        self.guarded(move || match Self::entry(&reference)?.delete_credential() {
            Ok(()) => Ok(()),
            Err(keyring::Error::NoEntry) => Ok(()),
            Err(error) => Err(error).context("delete credential from OS store"),
        })
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
        self.guarded(|| {
            match keyring::Entry::store_status() {
                Ok(()) => {}
                Err(error) => {
                    anyhow::bail!("no operating-system credential store available: {error}")
                }
            }
            #[cfg(target_os = "macos")]
            macos_keychain_writable()?;
            Ok(())
        })
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
    /// Kept below `OS_STORE_TIMEOUT` so a wedged keychain fails the probe
    /// ("not writable", the store reports itself unavailable) before the
    /// enclosing call deadline would mark the whole store unresponsive.
    const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
    /// Budget for deleting the throwaway probe item. A healthy keychain
    /// answers in milliseconds; the two-second bound keeps
    /// `PROBE_TIMEOUT + PROBE_CLEANUP_TIMEOUT` below [`OS_STORE_TIMEOUT`] so a
    /// wedged `securityd` still lets the guard report the store unavailable
    /// (instead of the caller abandoning the thread mid-cleanup) and the
    /// killed child cannot outlive it.
    const PROBE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);
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
    // the engine-token migration. Both run through the shared bounded child
    // runner, which kills and reaps a `security` stuck in `securityd` IPC at
    // the deadline — a plain `.status()` here would keep this guard thread
    // (already abandoned by the caller at `OS_STORE_TIMEOUT`) and the child
    // alive for the rest of the process.
    let mut probe = std::process::Command::new("security");
    probe
        .arg("add-generic-password")
        .arg("-s")
        .arg(PROBE_SERVICE)
        .arg("-a")
        .arg(PROBE_ACCOUNT)
        .arg("-w")
        .arg("probe")
        .arg(&keychain);
    let probe_output = run_command_blocking(probe, PROBE_TIMEOUT, "login keychain write probe")
        .map_err(|error| {
            if error.timed_out() {
                anyhow::anyhow!(
                    "{error} (keychain locked or securityd unreachable); using the file \
                     credential store"
                )
            } else {
                anyhow::Error::new(error).context("probing login keychain writability")
            }
        })?;

    // Best-effort cleanup of the probe item regardless of outcome; a leftover
    // is harmless (distinct service name, no secret value).
    let mut cleanup = std::process::Command::new("security");
    cleanup
        .arg("delete-generic-password")
        .arg("-s")
        .arg(PROBE_SERVICE)
        .arg("-a")
        .arg(PROBE_ACCOUNT)
        .arg(&keychain);
    if let Err(error) = run_command_blocking(
        cleanup,
        PROBE_CLEANUP_TIMEOUT,
        "keychain probe item cleanup",
    ) {
        tracing::warn!(%error,
            "keychain probe cleanup did not complete (the throwaway probe item may remain in \
             the login keychain)");
    }

    if !probe_output.status.success() {
        let stderr = String::from_utf8_lossy(&probe_output.stderr);
        anyhow::bail!(
            "login keychain is not writable in this session ({}); \
             using the file credential store",
            stderr.trim()
        );
    }
    Ok(())
}
/// Write `contents` to `path` so the value is never observable in a
/// non-private file, and never written through a pre-existing file.
///
/// A fresh temp file is created `0600` (ignoring the process umask), synced,
/// then renamed over the target: the target either does not exist or is the
/// fully written private file, and a handle opened on an older version keeps
/// reading that older version instead of the new secret.
pub fn write_private_file(path: &Path, contents: &str) -> Result<()> {
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

/// Set a directory to owner-only permissions.
#[cfg(unix)]
pub fn set_private_directory_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .with_context(|| format!("protect {}", path.display()))
}

#[cfg(not(unix))]
pub fn set_private_directory_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

/// Set a file to owner-only permissions.
#[cfg(unix)]
pub fn set_private_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("protect {}", path.display()))
}

#[cfg(not(unix))]
pub fn set_private_file_permissions(_path: &Path) -> Result<()> {
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
/// The hold is bounded for the OS backend (`OS_STORE_TIMEOUT`), so a wedged
/// store cannot serialize the whole process behind it past one deadline.
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
    selected_store_at(FileBackend::default_dir(state_dir), configured)
}

fn selected_store_at(
    credentials_dir: PathBuf,
    configured: Option<&str>,
) -> Result<Arc<dyn CredentialStore>> {
    let file = FileBackend::new(credentials_dir);
    Ok(match selected_kind(configured)? {
        CredentialStoreKind::File => Arc::new(file.migrating_from(os_credential_store())),
        CredentialStoreKind::Os => Arc::new(FallbackBackend::new(os_credential_store(), file)),
        CredentialStoreKind::Memory => shared_memory_store(),
    })
}

/// Where a credential write landed.
#[derive(Debug)]
pub enum Written {
    /// The selected backend holds the value.
    Selected,
    /// The OS store could not hold the value, so the private file store does.
    /// `reason` says why the OS store was bypassed.
    FileFallback {
        /// Error returned by the primary backend.
        reason: anyhow::Error,
    },
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
        Self::for_directory(FileBackend::default_dir(state_dir), configured)
    }

    /// Construct a writer beside a configuration file.
    pub fn for_config(config_path: &Path, configured: Option<&str>) -> Result<Self> {
        let parent = config_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        Self::for_directory(FileBackend::default_dir(parent), configured)
    }
    /// Construct a writer using an explicitly selected credential directory.
    pub fn for_directory(directory: impl Into<PathBuf>, configured: Option<&str>) -> Result<Self> {
        Self::from_file_store(FileBackend::new(directory), configured)
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
    Arc::new(CachedBackend::new(OsBackend::default()))
}

/// Test builds never reach the host keychain: the OS backend is an in-memory
/// stand-in, because the macOS probe writes a throwaway keychain item.
#[cfg(any(test, feature = "test-support"))]
fn os_backend() -> Arc<dyn CredentialStore> {
    Arc::new(MemoryBackend::default())
}

/// The process-wide memory credential store behind `PRELOOP_CREDENTIAL_STORE=memory`.
///
/// Direct `MemoryCredentialStore::default()` instances keep per-instance
/// isolation for tests; only the env-selected backend shares state so the
/// whole process sees one memory store, matching what `os`/`file` provide.
fn shared_memory_store() -> Arc<dyn CredentialStore> {
    static STORE: std::sync::LazyLock<Arc<dyn CredentialStore>> =
        std::sync::LazyLock::new(|| Arc::new(MemoryBackend::default()));
    STORE.clone()
}

/// Names used by the shared crate API.
pub type OsBackend = OsCredentialStore;
/// In-memory backend name.
pub type MemoryBackend = MemoryCredentialStore;
/// Private-file backend name.
pub type FileBackend = FileCredentialStore;
/// Read-through cache backend name.
pub type CachedBackend<S> = CachedCredentialStore<S>;
/// Primary-plus-file-fallback backend name.
pub type FallbackBackend = FallbackCredentialStore;
/// A backend that is reachable by nobody, standing in for a headless host.
#[derive(Clone, Copy, Default)]
pub struct UnavailableCredentialStore;

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

/// A backend that accepts writes but cannot read them back.
#[derive(Clone, Copy, Default)]
pub struct WriteOnlyCredentialStore;

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
    use std::sync::Arc;

    fn reference() -> CredentialRef {
        CredentialRef::new("test-secret").unwrap()
    }

    #[test]
    fn memory_backend_round_trip_overwrite_delete_and_concurrent_writes() {
        let store = Arc::new(MemoryBackend::default());
        assert_eq!(store.get(&reference()).unwrap(), None);
        store.set(&reference(), &SecretString::new("one")).unwrap();
        store.set(&reference(), &SecretString::new("two")).unwrap();
        assert_eq!(store.get(&reference()).unwrap().unwrap().expose(), "two");
        std::thread::scope(|scope| {
            for value in ["a", "b", "c", "d"] {
                let store = store.clone();
                scope.spawn(move || store.set(&reference(), &SecretString::new(value)).unwrap());
            }
        });
        assert!(store.get(&reference()).unwrap().is_some());
        store.delete(&reference()).unwrap();
        assert_eq!(store.get(&reference()).unwrap(), None);
    }

    #[test]
    fn file_backend_preserves_bytes_and_private_modes() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBackend::new(dir.path().join("credentials"));
        let value = "-----BEGIN KEY-----\nabc\n-----END KEY-----\n";
        store.set(&reference(), &SecretString::new(value)).unwrap();
        assert_eq!(store.get(&reference()).unwrap().unwrap().expose(), value);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(store.dir()).unwrap().permissions().mode() & 0o777,
                0o700
            );
            let file = std::fs::read_dir(store.dir())
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            assert_eq!(file.metadata().unwrap().permissions().mode() & 0o777, 0o600);
        }
    }

    #[test]
    fn cache_fresh_read_and_namespace_isolation() {
        let inner = Arc::new(MemoryBackend::default());
        let engine = inner.clone().scoped("engine");
        let workflow = inner.clone().scoped("workflow");
        engine
            .set(&reference(), &SecretString::new("engine-value"))
            .unwrap();
        assert_eq!(workflow.get(&reference()).unwrap(), None);
        assert_eq!(
            engine.get(&reference()).unwrap().unwrap().expose(),
            "engine-value"
        );

        let cached = CachedBackend::new((*inner).clone());
        cached
            .set(&reference(), &SecretString::new("cached"))
            .unwrap();
        inner
            .set(&reference(), &SecretString::new("fresh"))
            .unwrap();
        assert_eq!(
            cached.get(&reference()).unwrap().unwrap().expose(),
            "cached"
        );
        assert_eq!(
            cached.get_fresh(&reference()).unwrap().unwrap().expose(),
            "fresh"
        );
        assert_eq!(cached.get(&reference()).unwrap().unwrap().expose(), "fresh");
    }

    #[test]
    fn secret_values_and_errors_are_redacted() {
        let secret = SecretString::new("do-not-print");
        assert_eq!(format!("{secret:?}"), "SecretString(<redacted>)");
        assert_eq!(format!("{secret}"), "<redacted>");
        assert!(!format!("{secret:?}").contains("do-not-print"));
        assert!(
            MemoryBackend::default()
                .set(&reference(), &SecretString::new(""))
                .unwrap_err()
                .to_string()
                .contains("empty credential")
        );
    }

    #[test]
    fn timeout_guard_fails_fast_after_a_hung_call() {
        use preloop_observability::deadline::DeadlineGuard;
        use std::time::Duration;
        let guard = DeadlineGuard::new(
            Duration::from_millis(20),
            "test-secret-store",
            "secret store timeout",
        );
        let (_tx, rx) = std::sync::mpsc::channel::<()>();
        assert!(
            guard
                .run(move || {
                    let _ = rx.recv();
                    Ok::<_, anyhow::Error>(())
                })
                .is_err()
        );
        assert!(guard.run(|| Ok::<_, anyhow::Error>(())).is_err());
    }
    #[test]
    fn engine_scope_keeps_legacy_file_and_keychain_layout() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileBackend::new(dir.path().join("credentials"));
        let reference = CredentialRef::new("github-pat").unwrap();
        let engine = Arc::new(store.clone()).scoped("engine");
        engine
            .set(&reference, &SecretString::new("fixture"))
            .unwrap();
        let file = std::fs::read_dir(store.dir())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(
            file.file_name().to_string_lossy(),
            "22550b936ddce0b035c170377161d95337553c165bc6436cf6a42e0d3a452274.cred"
        );
        assert_eq!(OS_STORE_SERVICE, "preloop");
        assert_eq!(reference.as_str(), "github-pat");
    }
}
