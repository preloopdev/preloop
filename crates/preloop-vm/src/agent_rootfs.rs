//! Guest-agent rootfs guard: keep the agent Preloop boots in step with the
//! SmolVM binary Preloop launches.
//!
//! SmolVM boots its guest agent from `SMOLVM_AGENT_ROOTFS` when the variable
//! is set — and Preloop always sets it, because the engine isolates the
//! child's `HOME`/data directory and must hand smolvm the *host's* install
//! assets explicitly. An inherited variable also wins over the rootfs that
//! ships next to the binary (the wrapper scripts set
//! `SMOLVM_AGENT_ROOTFS=${SMOLVM_AGENT_ROOTFS:-$SCRIPT_DIR/agent-rootfs}`),
//! so a stale copy picked from the data directory silently overrides a
//! version-matched one: on one host the engine kept pinning a pre-1.16 agent
//! while an operator had upgraded the smolvm prefix by hand, and every
//! `machine stop` then failed with
//!
//! ```text
//! running guest agent does not support safe shutdown; VM left unchanged;
//! update the guest agent before retrying
//! ```
//!
//! which the pool retried for hours. This module answers two questions before
//! that can happen: which rootfs would Preloop export, and does that agent
//! match the smolvm binary that would boot it.
//!
//! Two pieces of policy are deliberate:
//!
//! * The rootfs shipped next to the resolved binary wins over a data-dir copy
//!   that does not agree with it. That is the one agent the binary is
//!   guaranteed to have been released with, and it is what smolvm itself
//!   would pick if Preloop exported nothing.
//! * The guard refuses only what it can prove — a differing agent digest, or
//!   an agent without `quiesced-shutdown-v1` under a host that requires that
//!   handshake. Two identical trees are not proof of the second: a smolvm
//!   upgraded without its sibling rootfs keeps the old agent on both paths, so
//!   the capability is checked whether or not the digests agree. Everything
//!   else (an unreadable tree, a host version that cannot be read) is a
//!   warning: a false refusal takes a working engine down, which is worse than
//!   the failure it guards against.

use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

use crate::VmError;

/// Path of the guest agent binary inside a rootfs tree.
pub const AGENT_BINARY: &str = "usr/local/bin/smolvm-agent";
/// Version record Preloop writes beside a rootfs it installed.
pub const VERSION_STAMP: &str = ".preloop-agent-version";
/// Capability a guest agent must advertise before SmolVM stops it.
///
/// Upstream checks it before sending a mutating shutdown request
/// (`smolvm_protocol::QUIESCED_SHUTDOWN_CAPABILITY`) and otherwise fails the
/// stop with the message below.
pub const QUIESCED_SHUTDOWN_CAPABILITY: &str = "quiesced-shutdown-v1";
/// First smolvm release that requires the capability above.
///
/// Agents 1.13.1 and 1.15.1 do not carry the string; 1.16.1 and later do, so
/// a 1.14-era agent under a 1.16-or-newer host is the unstoppable pair.
pub const QUIESCED_SHUTDOWN_MIN_HOST: (u64, u64, u64) = (1, 16, 0);

/// What smolvm prints when it refuses to stop a guest agent it cannot trust.
const SHUTDOWN_REFUSAL: &str = "running guest agent does not support safe shutdown; VM left unchanged; \
                                 update the guest agent before retrying";

/// Bytes of the agent's own startup message, written into the binary by
/// `info!(version = env!("CARGO_PKG_VERSION"), ...)` in the agent's `main`.
///
/// The version string follows the message in the binary's read-only data, so
/// a rootfs without a [`VERSION_STAMP`] can still report what agent it holds.
const AGENT_BANNER: &[u8] = b"smolvm-agent started, deferred boot work in progress";

/// Largest agent binary the guard will read (an official one is ~10 MiB).
const MAX_AGENT_BYTES: u64 = 256 * 1024 * 1024;

/// Guest console log SmolVM keeps inside each machine's data directory.
pub const CONSOLE_LOG: &str = "agent-console.log";

/// The guest agent's boot version, read from a machine's console log.
///
/// The agent logs `smolvm-agent started, deferred boot work in progress` with
/// its version — as a JSON `"version"` field when tracing serializes the
/// line, otherwise as the token right after the message. `None` means the log
/// does not hold that banner (the machine never booted, or the log was
/// rotated away), which the caller reports as "unknown" rather than guessing.
pub fn agent_boot_version(console: &str) -> Option<String> {
    let banner = std::str::from_utf8(AGENT_BANNER).ok()?;
    let start = console.find(banner)? + banner.len();
    let tail = console.get(start..)?;
    // A guest console log is bytes, not text: kernel and guest messages are
    // free to carry multi-byte characters, so the window has to end on a
    // character boundary. Slicing at a fixed offset would panic — inside the
    // diagnosis of a refused `stop`, which is the last place that may die.
    let mut end = tail.len().min(128);
    while !tail.is_char_boundary(end) {
        end -= 1;
    }
    let tail = &tail[..end];
    if let Some(field) = tail.find("\"version\":\"") {
        let value = &tail[field + "\"version\":\"".len()..];
        if let Some(end) = value.find('"') {
            let version = &value[..end];
            if parse_version(version).is_some() {
                return Some(version.to_owned());
            }
        }
    }
    (0..tail.len()).find_map(|offset| version_at(tail.as_bytes(), offset))
}

/// One agent-rootfs tree, as far as the guard can tell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRootfs {
    path: PathBuf,
    digest: Option<String>,
    version: Option<String>,
    binary_version: Option<String>,
    capability: bool,
    agent_present: bool,
}

impl AgentRootfs {
    /// Inspect `path`: hash its agent binary, find its version, and detect the
    /// quiesced-shutdown capability. A missing tree yields an all-`None`
    /// record rather than an error; callers decide whether that is fatal.
    pub fn inspect(path: &Path) -> Self {
        let bytes = read_bounded(&path.join(AGENT_BINARY));
        let digest = bytes.as_deref().map(sha256_hex);
        let capability = bytes
            .as_deref()
            .is_some_and(|bytes| contains(bytes, QUIESCED_SHUTDOWN_CAPABILITY.as_bytes()));
        let binary_version = bytes.as_deref().and_then(version_from_binary);
        let version = read_version_stamp(path).or_else(|| binary_version.clone());
        Self {
            path: path.to_path_buf(),
            digest,
            version,
            binary_version,
            capability,
            agent_present: bytes.is_some(),
        }
    }

    /// The rootfs directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// SHA-256 of the agent binary, when it could be read.
    pub fn digest(&self) -> Option<&str> {
        self.digest.as_deref()
    }

    /// Agent version, from the stamp Preloop wrote or from the binary itself.
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// Version baked into the agent binary, ignoring any stamp beside it.
    ///
    /// The stamp records which release the tree was installed from, so it can
    /// name a version the bytes no longer are — a restored tree, or an agent
    /// replaced without touching the stamp. `None` when the binary carries no
    /// readable banner, which is not evidence either way.
    pub fn binary_version(&self) -> Option<&str> {
        self.binary_version.as_deref()
    }

    /// Whether the agent advertises `quiesced-shutdown-v1`.
    pub fn supports_quiesced_shutdown(&self) -> bool {
        self.capability
    }

    /// Whether the tree holds a readable agent binary at all.
    pub fn agent_present(&self) -> bool {
        self.agent_present
    }

    /// One-line description for error messages and logs.
    pub fn describe(&self) -> String {
        format!(
            "agent {}, sha256 {}, {}",
            self.version.as_deref().unwrap_or("version unknown"),
            self.digest.as_deref().unwrap_or("hash unavailable"),
            if self.capability {
                "supports quiesced-shutdown-v1"
            } else {
                "no quiesced-shutdown-v1"
            }
        )
    }
}

/// Where the guard reads its inputs from — split out so tests can drive it
/// without mutating `HOME`/`PRELOOP_HOME`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Runtime {
    /// Host `HOME`: where the platform-default smolvm data directory lives.
    pub host_home: Option<PathBuf>,
    /// `SMOLVM_DATA_DIR`, else the effective Preloop home's `smolvm`.
    pub data_dir: Option<PathBuf>,
    /// An operator-set `SMOLVM_AGENT_ROOTFS`, which wins over every candidate.
    pub explicit_rootfs: Option<PathBuf>,
}

impl Runtime {
    /// The guard's inputs as the engine resolves them (the same rules as
    /// [`crate::smolvm_runtime_env`]).
    pub fn from_env() -> Self {
        let host_home = std::env::var_os("HOME").map(PathBuf::from);
        let explicit_rootfs = std::env::var_os("SMOLVM_AGENT_ROOTFS")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);
        let data_dir = std::env::var_os("SMOLVM_DATA_DIR")
            .map(PathBuf::from)
            .or_else(|| crate::effective_preloop_home().map(|home| home.join("smolvm")));
        Self {
            host_home,
            data_dir,
            explicit_rootfs,
        }
    }
}

/// What the guard found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardReport {
    /// Rootfs Preloop will export to smolvm (`SMOLVM_AGENT_ROOTFS`), when one
    /// resolves.
    pub exported: Option<PathBuf>,
    /// Rootfs that ships next to the resolved smolvm binary, when one exists.
    pub shipped: Option<PathBuf>,
    /// Whether the exported agent is byte-identical to the shipped one.
    pub matched: bool,
    /// Version reported by `smolvm --version`, when it was needed and could be
    /// read.
    pub host_version: Option<String>,
    /// A finding worth logging once, but not worth refusing over.
    pub warning: Option<String>,
}

/// Run the guard against the resolved smolvm binary, reading the process
/// environment. Callers log [`GuardReport::warning`]; the [`Err`] carries a
/// one-read diagnosis.
pub fn guard(binary: &Path) -> Result<GuardReport, VmError> {
    check(&Runtime::from_env(), binary)
}

/// [`guard`] with explicit inputs.
pub fn check(runtime: &Runtime, binary: &Path) -> Result<GuardReport, VmError> {
    let shipped_path = shipped_rootfs(binary);
    let shipped = shipped_path.as_deref().map(AgentRootfs::inspect);
    let exported_path = resolve_exported_rootfs(runtime, Some(binary));
    let exported = exported_path.as_deref().map(AgentRootfs::inspect);
    let matched = match (&shipped, &exported) {
        (Some(shipped), Some(exported)) => match (shipped.digest(), exported.digest()) {
            (Some(shipped), Some(exported)) => shipped == exported,
            _ => false,
        },
        _ => false,
    };
    // `--version` is the only way to learn whether this host performs the
    // shutdown handshake, so it is read only where the answer can matter: when
    // the agent Preloop would boot does not advertise the capability. A digest
    // match does not answer that question — the same stale agent in both trees
    // matches itself, which is what a smolvm upgraded without its rootfs (or a
    // restored copy) looks like — so the probe is driven by the exported
    // agent's own capability, not by whether the two trees agree.
    let needs_shutdown_handshake = exported
        .as_ref()
        .is_some_and(|exported| exported.agent_present() && !exported.supports_quiesced_shutdown());
    let host_version = if needs_shutdown_handshake {
        probe_host_version(binary)
    } else {
        None
    };
    let report = GuardReport {
        exported: exported_path,
        shipped: shipped_path,
        matched,
        host_version: host_version.clone(),
        warning: None,
    };
    match evaluate(
        binary,
        shipped.as_ref(),
        exported.as_ref(),
        host_version.as_deref(),
    ) {
        Decision::Allow { warning } => Ok(GuardReport { warning, ..report }),
        Decision::Refuse { report: message } => {
            Err(VmError::GuestAgentMismatch { report: message })
        }
    }
}

/// The verdict, kept pure so every branch is testable without a host.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Decision {
    /// Nothing to compare, or a mismatch that cannot be proven.
    Allow { warning: Option<String> },
    /// The exported agent provably cannot serve the resolved binary.
    Refuse { report: String },
}

fn evaluate(
    binary: &Path,
    shipped: Option<&AgentRootfs>,
    exported: Option<&AgentRootfs>,
    host_version: Option<&str>,
) -> Decision {
    let Some(exported) = exported else {
        // Nothing resolves: smolvm falls back to its own (matching) rootfs.
        return Decision::Allow { warning: None };
    };
    if let Some(shipped) = shipped {
        match (shipped.digest(), exported.digest()) {
            // Different bytes: the exported agent is not the one the binary
            // shipped with, which is the mismatch this guard refuses.
            (Some(shipped_digest), Some(exported_digest)) if shipped_digest != exported_digest => {
                return Decision::Refuse {
                    report: mismatch_report(binary, exported, shipped, host_version),
                };
            }
            // Byte-identical agents are not proof of a workable pair: a
            // smolvm upgraded without its sibling rootfs keeps the old agent
            // on both paths, and that pair cannot be stopped either. The
            // capability check below is what answers whether it can be.
            (Some(_), Some(_)) => {}
            // One side is unreadable; refusing would be guessing.
            _ => {
                return Decision::Allow {
                    warning: Some(format!(
                        "the guest-agent rootfs Preloop would boot from ({}, {}) could not be compared \
                         with the rootfs shipped next to the resolved smolvm ({}, {}); if a machine later \
                         cannot be stopped (\"{SHUTDOWN_REFUSAL}\"), run `preloop update --ensure-runtime`",
                        exported.path().display(),
                        exported.describe(),
                        shipped.path().display(),
                        shipped.describe(),
                    )),
                };
            }
        }
    }
    if !exported.agent_present() {
        return Decision::Allow {
            warning: Some(format!(
                "the guest-agent rootfs Preloop would boot from ({}) holds no readable {AGENT_BINARY}; \
                 smolvm will fail to boot a guest from it — run `preloop update --ensure-runtime`",
                exported.path().display()
            )),
        };
    }
    if exported.supports_quiesced_shutdown() {
        return Decision::Allow { warning: None };
    }
    match host_version.and_then(parse_version) {
        Some(version) if version >= QUIESCED_SHUTDOWN_MIN_HOST => Decision::Refuse {
            report: capability_report(binary, exported, shipped, host_version),
        },
        // An older host never performs the capability handshake.
        Some(_) => Decision::Allow { warning: None },
        // A host whose version cannot be read is not proof of anything.
        None => Decision::Allow {
            warning: Some(format!(
                "the guest-agent rootfs Preloop would boot from ({}, {}) advertises no \
                 quiesced-shutdown-v1 while the resolved smolvm reported no version; if that host is \
                 smolvm >= {}.{}.{} its guests cannot be stopped safely (\"{SHUTDOWN_REFUSAL}\") — run \
                 `preloop update --ensure-runtime`",
                exported.path().display(),
                exported.describe(),
                QUIESCED_SHUTDOWN_MIN_HOST.0,
                QUIESCED_SHUTDOWN_MIN_HOST.1,
                QUIESCED_SHUTDOWN_MIN_HOST.2,
            )),
        },
    }
}

fn mismatch_report(
    binary: &Path,
    exported: &AgentRootfs,
    shipped: &AgentRootfs,
    host_version: Option<&str>,
) -> String {
    let host = host_version.unwrap_or("unknown");
    format!(
        "smolvm guest-agent mismatch: Preloop would boot guests from {} ({}), but the resolved smolvm \
         ({}) ships its guest agent at {} ({}): the guest agent does not match host smolvm {host}. Such \
         a guest cannot be stopped — smolvm fails with \"{SHUTDOWN_REFUSAL}\" and the runner pool retries \
         forever. Fix: run `preloop update --ensure-runtime` to reinstall the guest agent that matches \
         smolvm {host}, or `cp -a {} {}`; if SMOLVM_AGENT_ROOTFS names the stale tree, unset it.",
        exported.path().display(),
        exported.describe(),
        binary.display(),
        shipped.path().display(),
        shipped.describe(),
        shipped.path().display(),
        exported.path().display(),
    )
}

fn capability_report(
    binary: &Path,
    exported: &AgentRootfs,
    shipped: Option<&AgentRootfs>,
    host_version: Option<&str>,
) -> String {
    let host = host_version.unwrap_or("unknown");
    let remedy = match shipped {
        Some(shipped) if shipped.path() != exported.path() => format!(
            "Fix: run `preloop update --ensure-runtime` to refresh {} with the guest agent that \
             matches smolvm {host}, or `cp -a {} {}`.",
            exported.path().display(),
            shipped.path().display(),
            exported.path().display(),
        ),
        // The stale agent is the one that shipped with this very smolvm, so
        // there is no second tree to copy from: the host's install is what has
        // to be replaced.
        _ => format!(
            "Fix: run `preloop update --ensure-runtime` to install the guest agent that matches \
             smolvm {host}; if {} came from a smolvm archive, that archive's own agent-rootfs is \
             the stale copy.",
            exported.path().display(),
        ),
    };
    format!(
        "smolvm guest-agent mismatch: the guest agent Preloop would boot from {} ({}) does not support \
         safe shutdown, but the resolved smolvm ({}) is version {host}, which refuses to stop such a \
         guest (\"{SHUTDOWN_REFUSAL}\"). Every machine Preloop boots would be unstoppable and the runner \
         pool would retry forever. {remedy}",
        exported.path().display(),
        exported.describe(),
        binary.display(),
    )
}

/// Probe `smolvm --version`: the wrapper reports the version of the binary it
/// resolves, which is the one that will boot the guest.
fn probe_host_version(binary: &Path) -> Option<String> {
    let mut command = std::process::Command::new(binary);
    command.arg("--version");
    // The macOS wrapper exports `DYLD_LIBRARY_PATH` itself, but a bare
    // `smolvm-bin` path needs the same variables the engine would export.
    crate::apply_smolvm_runtime_env(&mut command, Some(binary)).ok()?;
    let output = command.output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .split_whitespace()
        .find(|token| parse_version(token).is_some())
        .map(|token| token.trim_start_matches('v').to_owned())
}

/// The rootfs that ships next to a resolved smolvm binary, when one exists.
///
/// The official tarballs put it at `<prefix>/agent-rootfs`, which is also
/// where the wrapper script looks before falling back to smolvm's own data
/// directory, so a sibling of the binary's real location is authoritative.
pub fn shipped_rootfs(binary: &Path) -> Option<PathBuf> {
    let resolved = resolve_binary(binary)?;
    let candidate = resolved.parent()?.join("agent-rootfs");
    candidate.is_dir().then_some(candidate)
}

/// Resolve `binary` the way `Command` does — a bare name is searched for on
/// `PATH` — then follow every symlink, so a `~/.local/bin/smolvm` shim lands
/// on the prefix that owns the binary and its rootfs.
pub fn resolve_binary(binary: &Path) -> Option<PathBuf> {
    let candidate = if binary.components().count() == 1 {
        let path = std::env::var_os("PATH")?;
        std::env::split_paths(&path)
            .map(|directory| directory.join(binary))
            .find(|candidate| candidate.is_file())?
    } else {
        binary.to_path_buf()
    };
    Some(candidate.canonicalize().unwrap_or(candidate))
}

/// The rootfs Preloop hands to smolvm.
///
/// An operator-set `SMOLVM_AGENT_ROOTFS` wins outright. Otherwise a candidate
/// that agrees with the rootfs shipped next to the binary is preferred (in
/// candidate order, so identical trees keep their existing precedence), then
/// the shipped rootfs itself, then the first existing candidate — a layout
/// with no sibling rootfs keeps the data-directory behavior it had before.
///
/// Every candidate has to be a rootfs, not merely a directory: a half-copied
/// or leftover tree that holds the agent but no `sbin/init` cannot boot a
/// guest, and exporting one (or letting it shadow the shipped tree) fails
/// every machine. A candidate that is not a rootfs is not an answer.
pub fn resolve_exported_rootfs(runtime: &Runtime, binary: Option<&Path>) -> Option<PathBuf> {
    if let Some(explicit) = &runtime.explicit_rootfs {
        return Some(explicit.clone());
    }
    let candidates = crate::agent_rootfs_candidates(
        runtime.host_home.as_deref(),
        runtime.data_dir.as_deref(),
        binary,
    );
    match binary
        .and_then(shipped_rootfs)
        .filter(|path| looks_like_rootfs(path))
    {
        Some(shipped) => {
            let reference = AgentRootfs::inspect(&shipped);
            let matching = candidates.into_iter().find(|candidate| {
                let inspected = AgentRootfs::inspect(candidate);
                inspected.digest().is_some()
                    && inspected.digest() == reference.digest()
                    && looks_like_rootfs(candidate)
            });
            Some(matching.unwrap_or(shipped))
        }
        None => candidates
            .into_iter()
            .find(|candidate| candidate.is_dir() && looks_like_rootfs(candidate)),
    }
}

/// The rootfs Preloop would export, as the resolved smolvm will read it.
///
/// Cached: [`resolve_exported_rootfs`] hashes the agent of the shipped tree
/// and of every candidate, and this runs on the command path
/// (`smolvm_runtime_env`, once per boot-capable child process). See
/// [`inputs_fingerprint`] for what makes the cache notice a change.
pub fn exported_rootfs_cached(runtime: &Runtime, binary: Option<&Path>) -> Option<PathBuf> {
    let Some(binary) = binary else {
        // With no binary there is no shipped tree to compare against, and no
        // stable key to cache under. The callers that pass `None` are
        // one-shot sessions rather than the command path this cache exists
        // for.
        return resolve_exported_rootfs(runtime, None);
    };
    let fingerprint = inputs_fingerprint(runtime, binary);
    if let Some(cached) = cached_entry(&RESOLVED, &fingerprint, |entry| entry.exported.clone()) {
        return cached;
    }
    let exported = resolve_exported_rootfs(runtime, Some(binary));
    store(
        &RESOLVED,
        Resolved {
            fingerprint,
            exported: exported.clone(),
        },
    );
    exported
}

/// [`check`] over the same cached inputs, logging a fresh warning once.
///
/// A repair (`preloop update --ensure-runtime`), a replaced rootfs, or a
/// different binary invalidates the cache, so a verdict can never outlive the
/// assets it was computed from.
pub fn check_cached(runtime: &Runtime, binary: &Path) -> Result<GuardReport, VmError> {
    let fingerprint = inputs_fingerprint(runtime, binary);
    // A cached verdict was already logged when it was computed; only a fresh
    // verdict carries a warning to log. Logging the stored one again would
    // repeat the same line for every boot-capable command.
    let cached = cached_entry(&VERDICT, &fingerprint, |entry| {
        (None, entry.verdict.clone())
    });
    let (fresh_warning, verdict) = match cached {
        Some(cached) => cached,
        None => {
            let verdict = match check(runtime, binary) {
                Ok(report) => Ok(report),
                Err(VmError::GuestAgentMismatch { report }) => Err(report),
                Err(error) => Err(error.to_string()),
            };
            let fresh_warning = verdict
                .as_ref()
                .ok()
                .and_then(|report| report.warning.clone());
            store(
                &VERDICT,
                Verdict {
                    fingerprint,
                    verdict: verdict.clone(),
                },
            );
            (fresh_warning, verdict)
        }
    };
    if let Some(fresh) = &fresh_warning {
        tracing::warn!(binary = %binary.display(), "{fresh}");
    }
    verdict.map_err(|report| VmError::GuestAgentMismatch { report })
}

/// The resolved rootfs for one fingerprint.
#[derive(Clone)]
struct Resolved {
    fingerprint: String,
    exported: Option<PathBuf>,
}

/// The guard's verdict for one fingerprint.
#[derive(Clone)]
struct Verdict {
    fingerprint: String,
    verdict: Result<GuardReport, String>,
}

/// Last resolution and last verdict, both keyed by [`inputs_fingerprint`].
///
/// Process-global because the command path (`smolvm_runtime_env`) resolves the
/// rootfs without a provider to hang state on. One entry each, not a map: the
/// inputs are the same for every command a process runs, and a stale entry is
/// recomputed rather than evicted.
///
/// The two are cached separately because [`check`] probes the host by running
/// `smolvm --version` through [`crate::apply_smolvm_runtime_env`], which
/// resolves the rootfs again: one combined entry would re-enter its own
/// computation.
static RESOLVED: std::sync::Mutex<Option<Resolved>> = std::sync::Mutex::new(None);
static VERDICT: std::sync::Mutex<Option<Verdict>> = std::sync::Mutex::new(None);

/// One of `slot`'s entries, when it belongs to `fingerprint`.
///
/// The lock is only held for the lookup: computing an entry runs the guard,
/// which resolves the rootfs (and probes the host), so holding it across the
/// computation would deadlock the command path against itself.
fn cached_entry<T, R>(
    slot: &std::sync::Mutex<Option<T>>,
    fingerprint: &str,
    project: impl FnOnce(&T) -> R,
) -> Option<R>
where
    T: Fingerprinted,
{
    let guard = slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    guard
        .as_ref()
        .filter(|entry| entry.fingerprint() == fingerprint)
        .map(project)
}

fn store<T: Fingerprinted>(slot: &std::sync::Mutex<Option<T>>, entry: T) {
    *slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(entry);
}

/// The fingerprint a cache entry was computed under.
trait Fingerprinted {
    fn fingerprint(&self) -> &str;
}

impl Fingerprinted for Resolved {
    fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
}

impl Fingerprinted for Verdict {
    fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
}

/// Identity of every input the resolution and the guard read, as the host
/// reports it.
///
/// A stat-based fingerprint, not a content hash: hashing the agent binaries is
/// the work this cache exists to avoid, and a tree that is repaired or
/// replaced changes the size or mtime of one of the entries recorded here. It
/// covers the resolved binary (and `PATH`, when the binary is a bare name),
/// every tree that could be exported, and — inside each tree — the `sbin/init`
/// that makes it a rootfs at all, the agent binary, and the version stamp
/// Preloop writes. A byte-identical tree costs a handful of `stat` calls.
fn inputs_fingerprint(runtime: &Runtime, binary: &Path) -> String {
    let mut digest = Sha256::new();
    let resolved = resolve_binary(binary).unwrap_or_else(|| binary.to_path_buf());
    record_path(&mut digest, "binary", &resolved);
    if binary.components().count() == 1
        && let Some(path) = std::env::var_os("PATH")
    {
        digest.update(b"PATH\0");
        digest.update(path.to_string_lossy().as_bytes());
        digest.update(b"\0");
    }
    let mut trees: Vec<PathBuf> = Vec::new();
    if let Some(explicit) = &runtime.explicit_rootfs {
        trees.push(explicit.clone());
    }
    trees.extend(crate::agent_rootfs_candidates(
        runtime.host_home.as_deref(),
        runtime.data_dir.as_deref(),
        Some(binary),
    ));
    for (index, tree) in trees.iter().enumerate() {
        let index = index.to_string();
        record_path(&mut digest, &format!("tree{index}"), tree);
        record_path(
            &mut digest,
            &format!("init{index}"),
            &tree.join("sbin/init"),
        );
        record_path(
            &mut digest,
            &format!("agent{index}"),
            &tree.join(AGENT_BINARY),
        );
        record_path(
            &mut digest,
            &format!("stamp{index}"),
            &tree.join(VERSION_STAMP),
        );
    }
    hex::encode(digest.finalize())
}

/// Fold one path's existence, size, and mtime into `digest`.
fn record_path(digest: &mut Sha256, label: &str, path: &Path) {
    digest.update(label.as_bytes());
    digest.update(b"\0");
    digest.update(path.display().to_string().as_bytes());
    digest.update(b"\0");
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        digest.update(b"absent\0");
        return;
    };
    digest.update(b"present\0");
    digest.update(metadata.len().to_le_bytes());
    if let Ok(modified) = metadata.modified()
        && let Ok(since) = modified.duration_since(std::time::UNIX_EPOCH)
    {
        digest.update(since.as_nanos().to_le_bytes());
    }
}

/// Whether `path` is a rootfs at all, by upstream's own test: the `sbin/init`
/// entry, which is a symlink to a guest-only path, so it must be read with
/// `symlink_metadata` (`Path::exists` would follow it and miss).
fn looks_like_rootfs(path: &Path) -> bool {
    std::fs::symlink_metadata(path.join("sbin/init")).is_ok()
}

fn read_bounded(path: &Path) -> Option<Vec<u8>> {
    let metadata = std::fs::metadata(path).ok()?;
    if !metadata.is_file() || metadata.len() > MAX_AGENT_BYTES {
        return None;
    }
    std::fs::read(path).ok()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

/// The agent version recorded next to a rootfs that Preloop installed.
fn read_version_stamp(path: &Path) -> Option<String> {
    let stamp = std::fs::read_to_string(path.join(VERSION_STAMP)).ok()?;
    let stamp = stamp.trim();
    parse_version(stamp).is_some().then_some(stamp.to_owned())
}

/// The version baked into an agent binary, read from the banner the agent
/// logs at boot: the version string is emitted immediately after it.
fn version_from_binary(bytes: &[u8]) -> Option<String> {
    let banner = bytes
        .windows(AGENT_BANNER.len())
        .position(|window| window == AGENT_BANNER)?;
    // Linkers pad between string literals, so scan a short window rather than
    // assuming the version starts at the banner's last byte.
    let start = banner + AGENT_BANNER.len();
    let window = bytes.get(start..(start + 32).min(bytes.len()))?;
    (0..window.len()).find_map(|offset| version_at(window, offset))
}

/// A `major.minor.patch` token starting at `offset`, if one starts there.
fn version_at(bytes: &[u8], offset: usize) -> Option<String> {
    let tail = bytes.get(offset..)?;
    let length = tail
        .iter()
        .take_while(|byte| byte.is_ascii_digit() || **byte == b'.')
        .count();
    let token = std::str::from_utf8(tail.get(..length)?).ok()?;
    // A version, not a stray numeric literal: at least three components.
    (token.split('.').count() >= 3 && parse_version(token).is_some()).then_some(token.to_owned())
}

/// Parse `X.Y.Z` from the first version-looking token of input like
/// `smolvm 1.18.1`. A `-rc1`/`+meta` suffix on any component is ignored, a
/// missing patch component reads as zero.
fn parse_version(version: &str) -> Option<(u64, u64, u64)> {
    version.split_whitespace().find_map(parse_version_token)
}

fn parse_version_token(token: &str) -> Option<(u64, u64, u64)> {
    let token = token.trim_start_matches('v');
    let mut parts = token.split('.');
    let major = parse_component(parts.next()?)?;
    let minor = parse_component(parts.next()?)?;
    let patch = match parts.next() {
        Some(part) => parse_component(part)?,
        None => 0,
    };
    Some((major, minor, patch))
}

/// One numeric version component; a `-rc1`/`+meta` suffix is allowed only
/// after the digits (so `1` parses while `x` and `1x` do not).
fn parse_component(part: &str) -> Option<u64> {
    let digits: String = part.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() {
        return None;
    }
    let suffix = part.get(digits.len()..)?;
    if !suffix.is_empty() && !suffix.starts_with('-') && !suffix.starts_with('+') {
        return None;
    }
    digits.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    const STALE: &str = "1.14.2";
    const MATCHED: &str = "1.30.1";

    /// A rootfs tree shaped like an official one: `sbin/init` is what smolvm
    /// itself checks, and the agent binary carries the boot banner, its
    /// version, and (when supported) the capability string.
    fn rootfs(
        dir: &Path,
        name: &str,
        agent_version: Option<&str>,
        capability: bool,
        stamp: Option<&str>,
    ) -> PathBuf {
        let path = dir.join(name);
        fs::create_dir_all(path.join("sbin")).expect("sbin");
        fs::write(path.join("sbin/init"), b"#!/bin/sh\n").expect("init");
        if let Some(agent_version) = agent_version {
            let mut agent = Vec::from(&b"guest tooling\n"[..]);
            agent.extend_from_slice(AGENT_BANNER);
            agent.extend_from_slice(agent_version.as_bytes());
            agent.extend_from_slice(b"SMOLVM_NETWORK_DNS");
            if capability {
                agent.extend_from_slice(QUIESCED_SHUTDOWN_CAPABILITY.as_bytes());
            }
            fs::create_dir_all(path.join("usr/local/bin")).expect("usr/local/bin");
            fs::write(path.join(AGENT_BINARY), agent).expect("agent");
        }
        if let Some(stamp) = stamp {
            fs::write(path.join(VERSION_STAMP), format!("{stamp}\n")).expect("stamp");
        }
        path
    }

    /// A stand-in smolvm that answers `--version` with `smolvm <version>`, or
    /// fails when no version is given.
    fn fake_smolvm(dir: &Path, version: Option<&str>) -> PathBuf {
        let binary = dir.join("smolvm");
        let body = match version {
            Some(version) => {
                format!(
                    "#!/bin/sh\n[ \"$1\" = \"--version\" ] && printf 'smolvm {version}\\n'\nexit 0\n"
                )
            }
            None => "#!/bin/sh\nexit 1\n".to_owned(),
        };
        fs::write(&binary, body).expect("fake smolvm");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).expect("chmod");
        }
        binary
    }

    /// macOS resolves `/var` to `/private/var`, and the guard canonicalizes
    /// the binary it inspects, so fixtures are built under the canonical root.
    fn canonical(dir: &tempfile::TempDir) -> PathBuf {
        dir.path().canonicalize().expect("canonical temp dir")
    }

    fn runtime(dir: &Path) -> Runtime {
        Runtime {
            host_home: None,
            data_dir: Some(dir.join("data")),
            explicit_rootfs: None,
        }
    }

    fn digest_of(path: &Path) -> String {
        AgentRootfs::inspect(path)
            .digest()
            .expect("fixture agent hashes")
            .to_owned()
    }

    #[test]
    fn identical_agent_trees_match_without_probing_the_host() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = canonical(&dir);
        let binary = fake_smolvm(&root, Some(MATCHED));
        let shipped = rootfs(&root, "agent-rootfs", Some(MATCHED), true, None);
        let data = rootfs(&root, "data/agent-rootfs", Some(MATCHED), true, None);
        assert_eq!(digest_of(&shipped), digest_of(&data));

        let report = check(&runtime(&root), &binary).expect("identical trees must pass");

        assert!(report.matched, "the exported agent is the shipped one");
        assert_eq!(report.shipped.as_deref(), Some(shipped.as_path()));
        assert_eq!(
            report.exported.as_deref().map(digest_of).as_deref(),
            Some(digest_of(&shipped).as_str()),
            "resolution picks a tree carrying the shipped agent"
        );
        assert_eq!(report.warning, None, "identical trees warn about nothing");
        assert_eq!(
            report.host_version, None,
            "a digest match must not need `smolvm --version`"
        );
    }

    #[test]
    fn a_diverging_pin_refuses_and_names_both_trees_hashes_and_remedy() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = canonical(&dir);
        let binary = fake_smolvm(&root, Some(MATCHED));
        let shipped = rootfs(&root, "agent-rootfs", Some(MATCHED), true, Some(MATCHED));
        let stale = rootfs(&root, "stale", Some(STALE), false, Some(STALE));
        let mut runtime = runtime(&root);
        runtime.explicit_rootfs = Some(stale.clone());

        let error = check(&runtime, &binary).expect_err("a diverging pin must refuse");
        let VmError::GuestAgentMismatch { report } = error else {
            panic!("expected the guest-agent mismatch error, got {error}");
        };

        for needle in [
            stale.display().to_string(),
            shipped.display().to_string(),
            digest_of(&stale),
            digest_of(&shipped),
            MATCHED.to_owned(),
            STALE.to_owned(),
            "does not match host smolvm".to_owned(),
            "preloop update --ensure-runtime".to_owned(),
            format!("cp -a {} {}", shipped.display(), stale.display()),
        ] {
            assert!(report.contains(&needle), "{needle:?} missing from {report}");
        }
    }

    #[test]
    fn identical_agent_trees_are_still_refused_when_the_agent_cannot_stop() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = canonical(&dir);
        // A smolvm upgraded without its sibling rootfs: the binary is 1.30.1
        // and both trees hold the same pre-1.16 agent, so the digests agree
        // while the pair is exactly the unstoppable one.
        let binary = fake_smolvm(&root, Some(MATCHED));
        let shipped = rootfs(&root, "agent-rootfs", Some(STALE), false, None);
        let data = rootfs(&root, "data/agent-rootfs", Some(STALE), false, None);
        assert_eq!(digest_of(&shipped), digest_of(&data));

        let error = check(&runtime(&root), &binary)
            .expect_err("a matched digest is not proof the agent can stop");
        let VmError::GuestAgentMismatch { report } = error else {
            panic!("expected the guest-agent mismatch error, got {error}");
        };

        assert!(
            report.contains("does not support safe shutdown"),
            "{report}"
        );
        assert!(
            report.contains(MATCHED),
            "the host version is named: {report}"
        );
        assert!(
            report.contains("preloop update --ensure-runtime"),
            "{report}"
        );
        assert!(
            !report.contains(&format!(
                "cp -a {} {}",
                shipped.display(),
                shipped.display()
            )),
            "a tree cannot be copied onto itself: {report}"
        );
    }

    #[test]
    fn identical_agent_trees_are_left_alone_on_a_pre_1_16_host() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = canonical(&dir);
        let binary = fake_smolvm(&root, Some("1.15.1"));
        rootfs(&root, "agent-rootfs", Some(STALE), false, None);
        rootfs(&root, "data/agent-rootfs", Some(STALE), false, None);

        let report = check(&runtime(&root), &binary)
            .expect("a host that never performs the handshake cannot be broken by it");

        assert!(report.matched);
        assert_eq!(report.host_version.as_deref(), Some("1.15.1"));
        assert_eq!(report.warning, None);
    }

    #[test]
    fn a_replaced_rootfs_invalidates_the_cached_verdict() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = canonical(&dir);
        let prefix = root.join("prefix");
        fs::create_dir_all(&prefix).expect("prefix");
        let binary = fake_smolvm(&prefix, Some(MATCHED));
        let runtime = runtime(&root);
        let stale = rootfs(&root, "data/agent-rootfs", Some(STALE), false, Some(STALE));

        assert_eq!(
            exported_rootfs_cached(&runtime, Some(&binary)).as_deref(),
            Some(stale.as_path())
        );
        assert!(
            check_cached(&runtime, &binary).is_err(),
            "the stale agent cannot serve a {MATCHED} host"
        );

        // `preloop update --ensure-runtime` repairing the tree must be noticed
        // by the running engine, not only by the next one.
        rootfs(
            &root,
            "data/agent-rootfs",
            Some(MATCHED),
            true,
            Some(MATCHED),
        );
        assert!(
            check_cached(&runtime, &binary).is_ok(),
            "the repaired agent must be accepted without a restart"
        );
        assert_eq!(
            exported_rootfs_cached(&runtime, Some(&binary)).as_deref(),
            Some(stale.as_path())
        );
    }

    #[test]
    fn a_candidate_that_is_not_a_rootfs_is_never_exported() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = canonical(&dir);
        let binary = fake_smolvm(&root, Some(MATCHED));
        let shipped = rootfs(&root, "agent-rootfs", Some(MATCHED), true, Some(MATCHED));
        // A half-copied tree: the agent bytes are the shipped ones, but the
        // entry smolvm boots from is gone.
        let partial = root.join("data/agent-rootfs");
        fs::create_dir_all(partial.join("usr/local/bin")).expect("partial");
        fs::copy(shipped.join(AGENT_BINARY), partial.join(AGENT_BINARY)).expect("copy agent");
        assert_eq!(digest_of(&partial), digest_of(&shipped));

        let resolved = resolve_exported_rootfs(&runtime(&root), Some(&binary))
            .expect("the complete tree resolves");

        assert_eq!(
            resolved, shipped,
            "an incomplete copy must not shadow the rootfs that can boot"
        );
    }

    #[test]
    fn agent_boot_version_tolerates_multibyte_console_text() {
        let mut console = String::from("smolvm-agent started, deferred boot work in progress");
        // Fill the window with multi-byte characters so the 128-byte cut lands
        // inside one of them, then keep the version past the window.
        console.push_str(&"é".repeat(80));
        console.push_str("\"version\":\"1.18.1\"");
        assert_eq!(agent_boot_version(&console), None);

        let before = format!(
            "{}é1.30.1",
            "smolvm-agent started, deferred boot work in progress"
        );
        assert_eq!(agent_boot_version(&before).as_deref(), Some("1.30.1"));
    }

    #[test]
    fn a_stale_rootfs_without_a_shipped_tree_is_refused_when_the_host_needs_the_capability() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = canonical(&dir);
        // A binary with no sibling rootfs: the prefix layout of an install
        // that ships only the binary, so digests cannot be compared.
        let prefix = root.join("prefix");
        fs::create_dir_all(&prefix).expect("prefix");
        let binary = fake_smolvm(&prefix, Some(MATCHED));
        let stale = rootfs(&root, "data/agent-rootfs", Some(STALE), false, Some(STALE));

        let error = check(&runtime(&root), &binary).expect_err("an unstoppable guest is refused");
        let VmError::GuestAgentMismatch { report } = error else {
            panic!("expected the guest-agent mismatch error, got {error}");
        };

        for needle in [
            stale.display().to_string(),
            digest_of(&stale),
            STALE.to_owned(),
            MATCHED.to_owned(),
            "does not support safe shutdown".to_owned(),
            "preloop update --ensure-runtime".to_owned(),
        ] {
            assert!(report.contains(&needle), "{needle:?} missing from {report}");
        }
        assert!(
            report.contains("guests cannot be stopped") || report.contains("unstoppable"),
            "the refusal must say what the consequence is: {report}"
        );
    }

    #[test]
    fn a_stale_rootfs_is_left_alone_on_a_pre_1_16_host() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = canonical(&dir);
        let prefix = root.join("prefix");
        fs::create_dir_all(&prefix).expect("prefix");
        let binary = fake_smolvm(&prefix, Some("1.15.1"));
        rootfs(&root, "data/agent-rootfs", Some(STALE), false, Some(STALE));

        let report = check(&runtime(&root), &binary)
            .expect("a host that never performs the handshake cannot be broken by it");

        assert!(!report.matched);
        assert_eq!(report.host_version.as_deref(), Some("1.15.1"));
        assert_eq!(report.warning, None);
    }

    #[test]
    fn an_unreadable_host_version_warns_instead_of_refusing() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = canonical(&dir);
        let prefix = root.join("prefix");
        fs::create_dir_all(&prefix).expect("prefix");
        let binary = fake_smolvm(&prefix, None);
        let stale = rootfs(&root, "data/agent-rootfs", Some(STALE), false, Some(STALE));

        let report = check(&runtime(&root), &binary).expect("an unknown host is not proof");

        let warning = report.warning.expect("the unprovable case warns");
        assert!(warning.contains(&stale.display().to_string()), "{warning}");
        assert!(
            warning.contains("preloop update --ensure-runtime"),
            "{warning}"
        );
    }

    #[test]
    fn resolution_prefers_the_agent_that_ships_next_to_the_binary() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = canonical(&dir);
        let binary = fake_smolvm(&root, Some(MATCHED));
        let shipped = rootfs(&root, "agent-rootfs", Some(MATCHED), true, Some(MATCHED));
        let stale = rootfs(&root, "data/agent-rootfs", Some(STALE), false, Some(STALE));

        let mut runtime = runtime(&root);
        let resolved = resolve_exported_rootfs(&runtime, Some(&binary)).expect("a rootfs resolves");

        assert_eq!(
            resolved, shipped,
            "the stale data-dir copy must never override the shipped agent"
        );
        assert_ne!(resolved, stale);

        // A candidate carrying the same agent is preferred in candidate order,
        // so identical layouts keep the precedence they had before.
        let twin = rootfs(
            &root,
            "data/agent-rootfs",
            Some(MATCHED),
            true,
            Some(MATCHED),
        );
        assert_eq!(digest_of(&twin), digest_of(&shipped));
        let resolved = resolve_exported_rootfs(&runtime, Some(&binary)).expect("a rootfs resolves");
        assert_eq!(digest_of(&resolved), digest_of(&shipped));

        // An operator pin still wins over every candidate.
        runtime.explicit_rootfs = Some(stale.clone());
        assert_eq!(
            resolve_exported_rootfs(&runtime, Some(&binary)).as_deref(),
            Some(stale.as_path())
        );
    }

    #[test]
    fn resolution_falls_back_to_the_data_dir_when_the_sibling_is_not_a_rootfs() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = canonical(&dir);
        let binary = fake_smolvm(&root, Some(MATCHED));
        // A leftover directory that smolvm itself would reject: no sbin/init.
        fs::create_dir_all(root.join("agent-rootfs/usr/local/bin")).expect("leftover");
        let stale = rootfs(&root, "data/agent-rootfs", Some(STALE), false, Some(STALE));

        let resolved = resolve_exported_rootfs(&runtime(&root), Some(&binary));

        assert_eq!(resolved.as_deref(), Some(stale.as_path()));
    }

    #[test]
    fn shipped_rootfs_follows_a_symlinked_launcher() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = canonical(&dir);
        let prefix = root.join("prefix");
        fs::create_dir_all(&prefix).expect("prefix");
        let binary = fake_smolvm(&prefix, Some(MATCHED));
        let shipped = rootfs(&prefix, "agent-rootfs", Some(MATCHED), true, Some(MATCHED));
        let link = root.join("smolvm");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&binary, &link).expect("symlink");

        assert_eq!(shipped_rootfs(&link).as_deref(), Some(shipped.as_path()));
        assert_eq!(shipped_rootfs(&binary).as_deref(), Some(shipped.as_path()));
        assert_eq!(
            shipped_rootfs(Path::new("smolvm-that-is-not-installed")).as_deref(),
            None
        );
    }

    #[test]
    fn inspect_reads_stamps_hashes_and_the_capability() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = canonical(&dir);
        let with = rootfs(&root, "with", Some(MATCHED), true, None);
        let without = rootfs(&root, "without", Some(STALE), false, Some(STALE));
        let bare = rootfs(&root, "bare", None, false, None);

        let inspected = AgentRootfs::inspect(&with);
        assert_eq!(inspected.version(), Some(MATCHED));
        assert!(inspected.supports_quiesced_shutdown());
        assert!(inspected.digest().is_some_and(|digest| digest.len() == 64));

        let inspected = AgentRootfs::inspect(&without);
        assert_eq!(
            inspected.version(),
            Some(STALE),
            "the stamp names the agent"
        );
        assert!(!inspected.supports_quiesced_shutdown());

        let inspected = AgentRootfs::inspect(&bare);
        assert!(!inspected.agent_present());
        assert_eq!(inspected.version(), None);
        assert!(inspected.describe().contains("hash unavailable"));
    }

    #[test]
    fn the_preexisting_stamp_wins_over_the_binary_banner() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = canonical(&dir);
        let path = rootfs(&root, "stamped", Some(MATCHED), true, Some("1.19.0"));
        let inspected = AgentRootfs::inspect(&path);
        assert_eq!(inspected.version(), Some("1.19.0"));
    }

    #[test]
    fn agent_boot_version_reads_the_console_logs_tracing_line() {
        let console = concat!(
            "[    0.000000] Booting Linux\n",
            "{\"timestamp\":\"x\",\"level\":\"INFO\",\"fields\":{\"message\":\"smolvm-agent started, \
             deferred boot work in progress\",\"version\":\"1.18.1\"},\"target\":\"smolvm_agent\"}\n",
        );
        assert_eq!(agent_boot_version(console).as_deref(), Some("1.18.1"));

        let bare = "smolvm-agent started, deferred boot work in progress1.30.1SMOLVM_NETWORK_DNS";
        assert_eq!(agent_boot_version(bare).as_deref(), Some("1.30.1"));

        assert_eq!(agent_boot_version("[ 0.0 ] Booting Linux\n"), None);
    }

    #[test]
    fn version_parsing_accepts_release_spellings_and_rejects_junk() {
        assert_eq!(parse_version("1.18.1"), Some((1, 18, 1)));
        assert_eq!(parse_version("smolvm 1.18.1"), Some((1, 18, 1)));
        assert_eq!(parse_version("v1.16"), Some((1, 16, 0)));
        assert_eq!(parse_version("1.18.1-rc2"), Some((1, 18, 1)));
        assert_eq!(parse_version("1.18.1+build7"), Some((1, 18, 1)));
        assert_eq!(parse_version("1"), None);
        assert_eq!(parse_version("smolvm"), None);
        assert_eq!(parse_version(""), None);
        assert_eq!(parse_version("1.x.1"), None);
        assert!((1, 16, 0) > (1, 15, 1));
    }

    #[test]
    fn the_binary_scan_needs_a_full_version_token() {
        let mut bytes = Vec::from(&b"padding"[..]);
        bytes.extend_from_slice(AGENT_BANNER);
        bytes.extend_from_slice(b"\0\0");
        bytes.extend_from_slice(b"1.18.1");
        assert_eq!(version_from_binary(&bytes).as_deref(), Some("1.18.1"));

        let mut truncated = Vec::from(&b"padding"[..]);
        truncated.extend_from_slice(AGENT_BANNER);
        truncated.extend_from_slice(b"1.18");
        assert_eq!(version_from_binary(&truncated), None, "no patch component");

        assert_eq!(version_from_binary(b"no banner here"), None);
    }
}
