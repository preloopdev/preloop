//! SmolVM-backed ephemeral runner pool for Preloop CI.

include!(concat!(env!("OUT_DIR"), "/pins.rs"));

pub mod environment;
mod keys;
pub mod node_externals;
pub mod oci;

use crate::environment::{
    APT_INDICES_MARKER_PATH, EnvironmentSpec, ToolchainLayer, curated_toolchains,
    is_stock_base_image,
};
use crate::keys::{KeyPool, StagedKey};
use crate::oci::{MANIFEST_ACCEPT, OciManifest, OciReference, get_manifest, is_packed_vm_layer};
use preloop_gha_protocol::RUNNER_BUSY_SENTINEL;

/// Line an ephemeral runner prints when it accepts a job. Re-exported so a
/// `VmProvider` implementation can model the handshake this pool relies on.
pub use preloop_gha_protocol::RUNNER_BUSY_SENTINEL as RUNNER_BUSY_LINE;

use futures::StreamExt as _;
use futures::future::BoxFuture;
use preloop_vm::{
    MachineName, MachineSpec, MachineState, NetworkPolicy, OutputChunk, SecretSource,
    SmolVmProvider, SocketMount, VmError, VmProvider, VolumeMount,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const GUEST_CONTROL_DIR: &str = "/run/preloop-control";
const GUEST_CONTROL_SOCKET: &str = "/run/preloop-control/engine.sock";
/// Rust toolchain homes inside the guest.
///
/// rustup obeys `RUSTUP_HOME`/`CARGO_HOME` verbatim — no fallback to `$HOME`,
/// no search for a writable candidate — so these fixed system addresses are a
/// contract, not a hint. Every party MUST agree: the bake installs here
/// (`ToolchainLayer::Rust`), `runner_account_script` chowns them to the runner
/// uid, `guest_env_prefix` exports them, `guest_runner_path` puts
/// `$CARGO_HOME/bin` on PATH, and `verify_toolchain_homes` refuses to register
/// a runner whose golden disagrees. A `$HOME`-derived location instead would
/// split root's copy from the runner's: `/root` is 0700, so the runner gets
/// EACCES statting it, and a second writable home silently shadows the baked
/// toolchain with a fresh `stable` download.
const GUEST_RUSTUP_HOME: &str = "/usr/local/rustup";
const GUEST_CARGO_HOME: &str = "/usr/local/cargo";

const GUEST_FAILURE_MARKER: &str = "/home/runner/.preloop-job-failed";
/// Written by the worker while a job is paused in a debug session and removed
/// when the session closes. The pool probes it to release the slot's
/// concurrency permit for the pause's duration — without it a paused job
/// pins a permit (and with `max_concurrent` permits total, eventually the
/// whole pool) until the session ends or the pause credit expires.
const GUEST_PAUSE_MARKER: &str = "/home/runner/.preloop-job-paused";
/// Guest variable `preloop-runner configure` reads a pre-generated keypair from.
/// Must match `preloop_runner::configure::RSA_PARAMS_ENV`.
const RUNNER_RSA_PARAMS_ENV: &str = "PRELOOP_RUNNER_RSA_PARAMS";

/// How long a preserved VM survives with nobody attached.
const DEBUG_IDLE_TIMEOUT: Duration = Duration::from_secs(600);
/// How often the preserved VM re-checks the debug marker.
const DEBUG_POLL_INTERVAL: Duration = Duration::from_secs(10);
/// Marker mtime newer than this counts as an active `preloop shell` session.
/// Must exceed the CLI heartbeat interval.
const DEBUG_HEARTBEAT_WINDOW: Duration = Duration::from_secs(30);

/// First wait after a failed environment-golden bake, and the ceiling the
/// geometric backoff reaches. A slot that cannot build the golden its queued
/// job asks for must not re-attempt in a tight loop: every attempt boots a
/// VM and runs the full package bake.
const GOLDEN_RETRY_MIN: Duration = Duration::from_millis(500);
const GOLDEN_RETRY_MAX: Duration = Duration::from_secs(60);

/// Debug marker contents written by the orchestrator when it parks a failed VM.
pub const DEBUG_MARKER_IDLE: &str = "preserved";
/// Debug marker contents written by `preloop shell` while a session is live.
///
/// The orchestrator only extends the idle deadline for a marker in this state,
/// so its own initial write cannot masquerade as a heartbeat.
pub const DEBUG_MARKER_ACTIVE: &str = "active";

fn control_bridge_dir(config: &RunnerPoolConfig) -> Option<PathBuf> {
    config
        .control_socket
        .as_deref()
        .and_then(Path::parent)
        .map(|parent| parent.join("control-bridge"))
}

fn runner_volumes(
    config: &RunnerPoolConfig,
    machine: &MachineName,
    mount_externals: bool,
) -> Result<Vec<VolumeMount>, OrchestratorError> {
    // #294: the bundle mounted at `/opt/preloop/bin` is selected fail-closed.
    // `ensure_host_externals` (pool warm) downloads and materializes the node
    // externals before any golden is created, so by the time volumes are
    // computed the bundle is complete; if it ever is not — wiped or damaged
    // between warm and a later machine creation — selecting a bundle refuses
    // instead of mounting an externals-less bundle into the golden, which
    // used to boot node-less runners that failed every JS action step with
    // `bundled nodeXX is missing`. This stays a pure volume-list builder:
    // the download lives in `ensure_host_externals`, not here.
    let mut volumes = vec![VolumeMount {
        host: effective_runner_bundle(config)?,
        guest: PathBuf::from("/opt/preloop/bin"),
        read_only: true,
    }];
    // The Node externals are shared host-side, mounted read-only into machines
    // built from a registry base image — never baked into a machine image nor
    // downloaded per runner (that is what the `--no-externals` configure flag
    // enforces). Artifact-based machines (packed golden and create-per-runner)
    // skip the mount: the packed artifact carries only the
    // `<root>/externals -> /opt/preloop/bin/externals` symlink and every
    // virtio device consumes one of libkrun's 11 x86_64 IRQ lines — the
    // packed launcher is already the device-heaviest config (root + layers
    // virtiofs + 2 disks + mounts + vsock + net + console), so a third mount
    // pushes it past the budget and the golden fails to start
    // (`RegisterNetDevice(IrqsExhausted)`). The real externals ride the
    // `/opt/preloop/bin` bundle mount above, which is guaranteed complete:
    // `ensure_host_externals` materializes it at pool warm and the
    // fail-closed selection here refuses anything less.
    if mount_externals {
        volumes.push(VolumeMount {
            host: config.externals_dir.join("externals"),
            guest: PathBuf::from(RUNNER_ROOT).join("externals"),
            read_only: true,
        });
    }
    if let Some(host) = control_bridge_dir(config) {
        // Per-machine target: the guest agent's mounted-socket bridge binds
        // its listener INTO the mounted directory through virtiofs, and the
        // node outlives the machine. A shared directory let every machine see
        // every dead machine's socket node — connects then resolve to a dead
        // listener and return ECONNREFUSED forever (the hung-runner class).
        let host = host.join(machine.as_str());
        if let Err(error) = std::fs::create_dir_all(&host) {
            warn!(machine = machine.as_str(), %error, "control-bridge directory creation failed");
        }
        volumes.push(VolumeMount {
            host,
            guest: PathBuf::from(GUEST_CONTROL_DIR),
            read_only: false,
        });
    }
    Ok(volumes)
}

/// Populate the host-side externals directory, validating the manifest and
/// binary version for each runtime instead of merely checking for a file.
///
/// Reuses the same shell routine the golden bake uses — now with temp-file
/// download, SHA256 verification (pinned + SHASUMS), and manifest emission —
/// so it runs identically on the host and in the guest. A directory is only
/// re-downloaded when its `preloop-node.json` is missing, its version is stale,
/// or `bin/node --version` disagrees. Permissions are still normalized on every
/// call.
fn ensure_host_externals(config: &RunnerPoolConfig) -> Result<(), OrchestratorError> {
    let externals = config.externals_dir.join("externals");
    let needs_host = crate::node_externals::expected_runtimes()
        .iter()
        .any(|(runtime, version)| {
            let plain = version.trim_start_matches('v');
            !crate::node_externals::is_valid_externals_dir(&externals.join(runtime), runtime, plain)
        });
    if needs_host {
        std::fs::create_dir_all(&externals).map_err(|error| {
            OrchestratorError::Config(format!(
                "failed to create externals directory {}: {error}",
                externals.display()
            ))
        })?;
        for command in node_externals_at(config.externals_dir.to_string_lossy().as_ref()) {
            let status = std::process::Command::new("sh")
                .arg("-c")
                .arg(&command[2])
                .status()
                .map_err(|error| {
                    OrchestratorError::Config(format!(
                        "failed to spawn host externals install: {error}"
                    ))
                })?;
            if !status.success() {
                return Err(OrchestratorError::Config(
                    "host node externals install failed; \
                     check network egress to nodejs.org"
                        .to_owned(),
                ));
            }
        }
        info!(
            path = %externals.display(),
            "Node externals installed on host; mounting into runners"
        );
    }
    // Repair directories published before the guest runner went non-root.
    relax_externals_permissions(&externals);
    // Artifact-based machines reach node through the baked symlink
    // `<root>/externals -> /opt/preloop/bin/externals` (the packed launcher
    // has no IRQ headroom for a third virtiofs mount), so the runner bundle
    // must expose the same externals. This must be a REAL directory, not a
    // host symlink: virtiofs exports a symlink node verbatim and the guest
    // kernel then resolves its target in the GUEST namespace, where
    // `/var/lib/preloop/externals` does not exist — node would be missing.
    // The copy is attempted in place first; when the release directory is not
    // writable by this engine (unprivileged engine, root-owned release dir) an
    // engine-owned mirror bundle is published instead — see
    // `materialize_mirror_bundle`. Either way the pool refuses to start with
    // externals the guest cannot resolve, because every JS action step would
    // otherwise fail with `bundled nodeXX is missing` after the pool reports
    // itself ready.
    let bundle_externals = config.runner_bundle.join("externals");
    if !externals_complete(&bundle_externals) {
        // Ensure bundle parent exists before copy.
        let copy = std::fs::create_dir_all(&bundle_externals).and_then(|()| {
            // For stale manifests, remove the stale runtime dirs in the bundle first
            // so `cp -a` does not leave a mix of stale/new.
            for (runtime, version) in crate::node_externals::expected_runtimes() {
                let plain = version.trim_start_matches('v');
                let dest = bundle_externals.join(runtime);
                if !crate::node_externals::is_valid_externals_dir(&dest, runtime, plain)
                    && dest.exists()
                {
                    let _ = std::fs::remove_dir_all(&dest);
                }
            }
            std::process::Command::new("cp")
                .arg("-a")
                .arg(externals.join("."))
                .arg(&bundle_externals)
                .output()
        });
        match copy {
            Ok(output) if output.status.success() => info!(
                bundle = %bundle_externals.display(),
                "Materialized node externals into runner bundle"
            ),
            Ok(output) => warn!(
                status = %output.status,
                bundle = %bundle_externals.display(),
                "Could not materialize bundle externals in place; publishing an engine-owned mirror bundle"
            ),
            Err(error) => warn!(
                %error,
                bundle = %bundle_externals.display(),
                "Could not materialize bundle externals in place; publishing an engine-owned mirror bundle"
            ),
        }
    }
    // `cp -a` preserves the source mode, so the bundle copy needs the same
    // repair as the host directory.
    relax_externals_permissions(&bundle_externals);
    if externals_complete(&bundle_externals) {
        return Ok(());
    }
    materialize_mirror_bundle(config, &externals)
}

/// Whether `externals_root` carries every expected runtime at its pinned
/// version, validated through the manifest and `bin/node --version`.
fn externals_complete(externals_root: &Path) -> bool {
    crate::node_externals::expected_runtimes()
        .iter()
        .all(|(runtime, version)| {
            let plain = version.trim_start_matches('v');
            crate::node_externals::is_valid_externals_dir(
                &externals_root.join(runtime),
                runtime,
                plain,
            )
        })
}

/// Engine-owned mirror of the runner bundle.
///
/// Lives beside the other engine state, so it is writable whenever the engine
/// can run at all — unlike the release directory, which is root-owned when the
/// engine runs unprivileged.
fn mirror_bundle_dir(config: &RunnerPoolConfig) -> PathBuf {
    config.externals_dir.join("runner-bundle")
}

/// The bundle directory actually mounted into guests at `/opt/preloop/bin`.
///
/// Prefers the release directory and falls back to the engine-owned mirror,
/// which `materialize_mirror_bundle` only leaves in place when it is complete.
///
/// #294: fails closed instead of returning an externals-less bundle. Mounting
/// an incomplete bundle used to boot goldens and fallback runners whose baked
/// `<root>/externals` symlink dangled, so every JS action step failed with
/// `bundled nodeXX is missing` long after the pool reported itself ready.
fn effective_runner_bundle(config: &RunnerPoolConfig) -> Result<PathBuf, OrchestratorError> {
    if externals_complete(&config.runner_bundle.join("externals")) {
        return Ok(config.runner_bundle.clone());
    }
    let mirror = mirror_bundle_dir(config);
    if mirror.join(&config.runner_binary_name).is_file()
        && externals_complete(&mirror.join("externals"))
    {
        return Ok(mirror);
    }
    Err(OrchestratorError::Config(format!(
        "no runner bundle with complete node externals: release bundle {} and \
         engine-owned mirror {} are both incomplete; check network egress to \
         nodejs.org and the host externals at {}",
        config.runner_bundle.display(),
        mirror.display(),
        config.externals_dir.join("externals").display(),
    )))
}

/// Publish an engine-owned bundle carrying the runner binary and the validated
/// host externals, for the case where the release directory cannot be written.
///
/// Fails when the mirror still does not validate: a pool that starts without
/// resolvable externals reports itself ready and then fails every JS action
/// step, which is far more expensive to diagnose than a refused startup.
fn materialize_mirror_bundle(
    config: &RunnerPoolConfig,
    host_externals: &Path,
) -> Result<(), OrchestratorError> {
    let mirror = mirror_bundle_dir(config);
    let mirror_externals = mirror.join("externals");
    let runner_source = config.runner_bundle.join(&config.runner_binary_name);
    let runner_target = mirror.join(&config.runner_binary_name);
    let published = std::fs::create_dir_all(&mirror_externals).and_then(|()| {
        for (runtime, version) in crate::node_externals::expected_runtimes() {
            let plain = version.trim_start_matches('v');
            let dest = mirror_externals.join(runtime);
            if !crate::node_externals::is_valid_externals_dir(&dest, runtime, plain)
                && dest.exists()
            {
                let _ = std::fs::remove_dir_all(&dest);
            }
        }
        // `cp -a` for both halves: the runner binary must keep its exec bit and
        // the externals must arrive as real directories, since virtiofs exports
        // a symlink node verbatim and the guest would resolve it in its own
        // namespace.
        std::fs::copy(&runner_source, &runner_target)?;
        std::process::Command::new("cp")
            .arg("-a")
            .arg(host_externals.join("."))
            .arg(&mirror_externals)
            .output()
    });
    if let Err(error) = published {
        return Err(OrchestratorError::Config(format!(
            "node externals are missing from the runner bundle {} and the \
             engine-owned mirror {} could not be published: {error}",
            config.runner_bundle.display(),
            mirror.display()
        )));
    }
    set_executable_bit(&runner_target);
    relax_externals_permissions(&mirror_externals);
    if !runner_target.is_file() || !externals_complete(&mirror_externals) {
        return Err(OrchestratorError::Config(format!(
            "node externals are still incomplete after publishing the \
             engine-owned mirror bundle {}; every JS action step would fail \
             with `bundled node is missing`. Check network egress to \
             nodejs.org and the host externals at {}",
            mirror.display(),
            host_externals.display()
        )));
    }
    info!(
        bundle = %mirror.display(),
        "Published engine-owned mirror bundle with node externals"
    );
    Ok(())
}

/// Restore the exec bit on the mirrored runner binary.
fn set_executable_bit(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(metadata) = std::fs::metadata(path) {
            let mut permissions = metadata.permissions();
            permissions.set_mode(permissions.mode() | 0o755);
            let _ = std::fs::set_permissions(path, permissions);
        }
    }
    #[cfg(not(unix))]
    let _ = path;
}

/// Make published Node externals traversable by the unprivileged guest account.
///
/// `mktemp -d` publishes 0700 and `cp -a` preserves it. That was invisible
/// while the guest runner ran as root; it now drops to uid 1001
/// (`as_runner_user`), which cannot traverse a 0700 directory owned by
/// another uid. The runner probes the interpreter with `is_file()`, and
/// EACCES is indistinguishable from absent there — so every JS action dies
/// with "bundled node24 is missing" while the binary sits right there,
/// readable, one directory down.
///
/// Only the group/other read+execute bits are added: enough to run the
/// interpreter, never enough to modify it. Best-effort — a bundle inside a
/// root-owned release directory is the deploy step's responsibility, and a
/// failure here must not stop the pool from starting.
#[cfg(unix)]
fn relax_externals_permissions(externals: &Path) {
    use std::os::unix::fs::PermissionsExt;

    const TRAVERSABLE: u32 = 0o055;

    let Ok(entries) = std::fs::read_dir(externals) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(metadata) = std::fs::metadata(&path) else {
            continue;
        };
        if !metadata.is_dir() {
            continue;
        }
        let mode = metadata.permissions().mode();
        if mode & TRAVERSABLE == TRAVERSABLE {
            continue;
        }
        match std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode | TRAVERSABLE)) {
            Ok(()) => info!(
                path = %path.display(),
                "Relaxed node externals permissions for the non-root guest runner"
            ),
            Err(error) => warn!(
                path = %path.display(),
                %error,
                "Could not relax node externals permissions; \
                 JS actions will fail as the non-root guest user"
            ),
        }
    }
}

#[cfg(not(unix))]
fn relax_externals_permissions(_externals: &Path) {}

/// Total physical memory in MiB, or `None` when it cannot be determined.
///
/// Used to bound on-demand fork concurrency so the pool never schedules
/// more runner VMs than the host can hold in RAM. `None` (an unreadable
/// `/proc/meminfo`, a non-Linux/non-macOS host) falls back to CPU-only
/// sizing rather than refusing to run.
#[cfg(target_os = "linux")]
fn host_memory_mib() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let kib: u64 = meminfo.lines().find_map(|line| {
        let line = line.trim();
        let rest = line.strip_prefix("MemTotal:")?;
        rest.trim().strip_suffix(" kB")?.trim().parse().ok()
    })?;
    Some(kib / 1024)
}

/// Total physical memory in MiB, or `None` when it cannot be determined.
#[cfg(target_os = "macos")]
fn host_memory_mib() -> Option<u64> {
    let output = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    let bytes: u64 = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .ok()?;
    Some(bytes / (1024 * 1024))
}

/// Total physical memory in MiB, or `None` when it cannot be determined.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn host_memory_mib() -> Option<u64> {
    None
}

/// On-demand fork concurrency allowed by host memory alone.
///
/// Every on-demand fork inherits the golden's committed footprint and grows
/// toward `runner_memory_mib` as its guest runs, so the pool must never
/// schedule more concurrent runners than `(host_total - golden - reserve) /
/// runner_ceiling` allows. The 2 GiB reserve keeps the control plane, OS,
/// and page cache alive — without it the host OOMs *after* the forks are up,
/// which is exactly the production failure this guards against. Floors at 1
/// so a tiny host still runs a single job rather than refusing to work.
fn on_demand_memory_cap(host_total_mib: u64, runner_memory_mib: u64) -> usize {
    const HOST_RESERVE_MIB: u64 = 2048;
    // Both the golden and each runner use the same ceiling. A degenerate
    // zero ceiling means "unbounded" would be unsafe to divide by, so treat
    // it as 1; the config layer validates `memory_mib > 0` in practice.
    let runner_mib = runner_memory_mib.max(1);
    let golden_mib = runner_mib;
    (host_total_mib.saturating_sub(golden_mib + HOST_RESERVE_MIB) / runner_mib).max(1) as usize
}

fn default_golden_url(release_version: &str) -> String {
    format!(
        "https://github.com/preloopdev/preloop/releases/download/v{release_version}/preloop-ubuntu-24.04-{}",
        std::env::consts::ARCH
    )
}

/// GitHub's permanent redirect to whichever release currently carries the
/// asset.
///
/// The golden is only attached to releases that baked one, so an engine whose
/// own release predates the artifact (or whose release has only the `.sig`
/// sidecar) otherwise 404s and falls back to a local build even though a
/// published golden exists.
fn latest_golden_url() -> String {
    format!(
        "https://github.com/preloopdev/preloop/releases/latest/download/preloop-ubuntu-24.04-{}",
        std::env::consts::ARCH
    )
}

/// The asset name every release carries the packed golden under.
fn golden_asset_name() -> String {
    format!("preloop-ubuntu-24.04-{}", std::env::consts::ARCH)
}

/// Release-asset URL for a specific tag (`v0.33.6`, as GitHub spells it).
fn tagged_golden_url(tag: &str) -> String {
    format!(
        "https://github.com/preloopdev/preloop/releases/download/{tag}/{}",
        golden_asset_name()
    )
}

/// Release-asset URLs to try, in order.
///
/// The engine's own release comes first, then the newest release that actually
/// carries the artifact, then GitHub's `latest` redirect (which needs no API
/// call and covers a resolution that failed). An operator-provided
/// `PRELOOP_GOLDEN_URL` replaces all of them: it is the only source they asked
/// for.
fn golden_url_candidates(
    release_version: &str,
    resolved_tag: Option<&str>,
    forced_url: Option<String>,
) -> Vec<String> {
    if let Some(forced) = forced_url {
        return vec![forced];
    }
    let mut candidates = vec![default_golden_url(release_version)];
    for url in [
        resolved_tag.map(tagged_golden_url),
        Some(latest_golden_url()),
    ]
    .into_iter()
    .flatten()
    {
        if !candidates.contains(&url) {
            candidates.push(url);
        }
    }
    candidates
}

/// Resolved golden-tag lookups, keyed by API base: a GHES host and github.com
/// must not share an answer.
type GoldenTagCache =
    tokio::sync::Mutex<std::collections::HashMap<String, (std::time::Instant, Option<String>)>>;

/// How long a resolved golden tag is reused before the API is asked again.
/// Unauthenticated GitHub API calls are capped at 60/hour per address, and the
/// answer only changes when a release is published.
const LATEST_GOLDEN_TAG_TTL: Duration = Duration::from_secs(300);
const GITHUB_API_BASE: &str = "https://api.github.com";

#[derive(Debug, Deserialize)]
struct GitHubReleaseAsset {
    name: String,
}

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    tag_name: String,
    #[serde(default)]
    draft: bool,
    #[serde(default)]
    prerelease: bool,
    #[serde(default)]
    assets: Vec<GitHubReleaseAsset>,
}

/// Newest published release whose assets carry the packed golden for this
/// architecture.
///
/// The newest *release* is not the same as the newest release *with a golden*:
/// a tag whose golden step failed (or that predates the artifact) has none, so
/// `/releases/latest/download/…` 404s there while a usable golden sits one
/// release back. Stable releases win over prereleases; drafts are ignored.
/// Best effort: any failure returns `None` and the caller falls through to the
/// `latest` redirect.
async fn resolve_latest_golden_tag(client: &reqwest::Client, api_base: &str) -> Option<String> {
    static CACHE: std::sync::OnceLock<GoldenTagCache> = std::sync::OnceLock::new();
    let cache = CACHE.get_or_init(|| tokio::sync::Mutex::new(std::collections::HashMap::new()));
    if let Some((fetched_at, tag)) = cache.lock().await.get(api_base)
        && fetched_at.elapsed() < LATEST_GOLDEN_TAG_TTL
    {
        return tag.clone();
    }

    let url = format!("{api_base}/repos/preloopdev/preloop/releases?per_page=30");
    let releases = match client
        .get(&url)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => {
            match response.json::<Vec<GitHubRelease>>().await {
                Ok(releases) => releases,
                Err(error) => {
                    info!(%error, "golden release lookup returned an unreadable body");
                    Vec::new()
                }
            }
        }
        Ok(response) => {
            info!(status = %response.status(), "golden release lookup unavailable");
            Vec::new()
        }
        Err(error) => {
            info!(%error, "golden release lookup failed");
            Vec::new()
        }
    };

    let asset = golden_asset_name();
    let carries_golden = |release: &GitHubRelease| {
        !release.draft && release.assets.iter().any(|entry| entry.name == asset)
    };
    let tag = releases
        .iter()
        .find(|release| carries_golden(release) && !release.prerelease)
        .or_else(|| releases.iter().find(|release| carries_golden(release)))
        .map(|release| release.tag_name.clone());

    cache.lock().await.insert(
        api_base.to_owned(),
        (std::time::Instant::now(), tag.clone()),
    );
    tag
}

/// Where an in-flight transfer of `payload` is kept.
///
/// Deliberately stable rather than per-attempt: a retried attempt, an engine
/// restart, or a second start after a crash all resume the same file instead
/// of discarding gigabytes. The install renames it onto `payload` only after
/// the checksum matches, so a partial file is never mistaken for a golden.
fn golden_partial_path(payload: &Path) -> PathBuf {
    let name = payload
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "golden".to_owned());
    payload.with_file_name(format!("{name}.partial"))
}

pub const GIB: u64 = 1024 * 1024 * 1024;
/// Kept free beyond the bytes a golden download writes, so landing the
/// artifact never takes the volume to zero.
pub const GOLDEN_DOWNLOAD_DISK_MARGIN: u64 = GIB;
/// Pack staging a golden bake needs beyond its builder disk. Same rule the
/// golden workflows enforce (`need_gib = GOLDEN_GUEST_GIB + 20`), adopted after
/// `smolvm pack` died with `tar error: No space` at 110G free for a 200G guest.
pub const GOLDEN_BUILD_DISK_HEADROOM_GIB: u64 = 20;
/// Floor for the one-shot builder's disk: packing exports a second copy of
/// the guest filesystem, so the builder never gets less than this even when
/// job VMs are configured smaller.
pub const GOLDEN_BUILDER_MIN_STORAGE_GIB: u32 = 40;
/// Escape hatch for hosts whose free space `df` misreports (thin pools,
/// quotas): proceed past a disk refusal with a warning instead.
pub const DISK_PREFLIGHT_OVERRIDE: &str = "PRELOOP_SKIP_DISK_PREFLIGHT";

pub fn disk_preflight_overridden() -> bool {
    std::env::var(DISK_PREFLIGHT_OVERRIDE)
        .is_ok_and(|value| !matches!(value.trim(), "" | "0" | "false"))
}

/// Builder disk a golden bake uses for job VMs configured at `storage_gib`.
pub fn golden_builder_storage_gib(storage_gib: u32) -> u32 {
    storage_gib.max(GOLDEN_BUILDER_MIN_STORAGE_GIB)
}

/// Volume golden and job-VM disks land on; the artifact directory stands in
/// when the SmolVM data root cannot be resolved.
fn golden_disk_root(config: &RunnerPoolConfig) -> PathBuf {
    preloop_vm::machine_data_root().unwrap_or_else(|| {
        config
            .artifact_stem
            .parent()
            .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
    })
}

fn golden_build_required_bytes(builder_storage_gib: u32) -> u64 {
    (u64::from(builder_storage_gib) + GOLDEN_BUILD_DISK_HEADROOM_GIB) * GIB
}

fn format_gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / GIB as f64)
}

/// Refuse an operation needing `required` bytes on the volume holding `path`
/// when they are not free. A volume `df` cannot measure is not a refusal: the
/// check exists to fail early, never to block a host it cannot read.
fn ensure_free_disk(path: &Path, required: u64, purpose: &str, remedy: &str) -> Result<(), String> {
    let free = match preloop_vm::filesystem_available_bytes(path) {
        Ok(free) => free,
        Err(error) => {
            warn!(path = %path.display(), %error, purpose, "free disk space unmeasurable; skipping check");
            return Ok(());
        }
    };
    if free >= required {
        return Ok(());
    }
    let message = format!(
        "{purpose} needs {} free on {} but only {} is available; {remedy}, or set {DISK_PREFLIGHT_OVERRIDE}=1 to proceed anyway",
        format_gib(required),
        path.display(),
        format_gib(free),
    );
    if disk_preflight_overridden() {
        warn!("{message} ({DISK_PREFLIGHT_OVERRIDE} set; continuing)");
        return Ok(());
    }
    Err(message)
}

fn ensure_disk_for_golden_build(root: &Path, builder_storage_gib: u32) -> Result<(), String> {
    ensure_free_disk(
        root,
        golden_build_required_bytes(builder_storage_gib),
        "golden build",
        &format!(
            "free space or lower PRELOOP_RUNNER_STORAGE_GB (builder disk {builder_storage_gib} GiB \
             + {GOLDEN_BUILD_DISK_HEADROOM_GIB} GiB pack staging)"
        ),
    )
}

/// Warn, never refuse, when the volume holding `path` has under `required`
/// bytes free.
fn warn_if_disk_below(path: &Path, required: u64, purpose: &str) {
    if let Ok(free) = preloop_vm::filesystem_available_bytes(path)
        && free < required
    {
        warn!(
            path = %path.display(),
            free = %format_gib(free),
            needed = %format_gib(required),
            purpose,
            "low disk space: this volume may fill before the golden and job VMs settle"
        );
    }
}

/// Reserve kept free on the VM volume before a job VM is started.
///
/// Deliberately not one VM's storage ceiling: the live engine runs ten VMs
/// with 80 GiB ceilings that actually use 0.7-8 GB each, so refusing starts
/// below a ceiling's worth of free space would throttle it to about two
/// concurrent jobs. A reserve of the same order as the observed footprint
/// keeps a full host from being filled further without capping concurrency.
const DEFAULT_MIN_FREE_DISK_GIB: u64 = 20;
/// Escape hatch for hosts with little free disk: `PRELOOP_RUNNER_MIN_FREE_DISK_GB`.
/// `0` disables the reserve.
const MIN_FREE_DISK_ENV: &str = "PRELOOP_RUNNER_MIN_FREE_DISK_GB";
/// How often a slot held back by the reserve re-measures the volume.
const DISK_WAIT_PROBE_INTERVAL: Duration = Duration::from_secs(15);
/// How often a held slot repeats its shortfall warning. The measurement runs
/// every probe; the line is throttled to one per minute per waiting slot so a
/// full host is visible without flooding the log.
const DISK_WAIT_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Reserve in bytes from a raw [`MIN_FREE_DISK_ENV`] value.
///
/// Unset, empty, or unparseable keeps the default: a typo in an environment
/// variable must not silently disable a guard against filling the host.
fn min_free_disk_bytes(raw: Option<&str>) -> u64 {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_MIN_FREE_DISK_GIB)
        .saturating_mul(GIB)
}

/// Free-space measurement of the volume holding a path.
type FreeDiskMeasure = Arc<dyn Fn(&Path) -> Result<u64, VmError> + Send + Sync>;

/// Free-space reserve that gates starting a job VM (fork or create).
///
/// A job VM is never started below the reserve: the slot waits and
/// re-measures, so a full host stalls new jobs instead of filling up or
/// failing them. `reserve_bytes == 0` disables the check, and a volume the
/// measurement cannot read never blocks — the check exists to fail early on a
/// host that is genuinely out of room, never to hold back one it cannot
/// measure.
#[derive(Clone)]
pub struct JobVmDiskReserve {
    reserve_bytes: u64,
    probe_interval: Duration,
    measure: FreeDiskMeasure,
}

impl JobVmDiskReserve {
    /// Policy from the environment ([`MIN_FREE_DISK_ENV`], default 20 GiB).
    pub fn from_env() -> Self {
        Self::new(
            min_free_disk_bytes(std::env::var(MIN_FREE_DISK_ENV).ok().as_deref()),
            preloop_vm::filesystem_available_bytes,
        )
    }

    /// Reserve `reserve_bytes` of the volume `measure` reports on.
    pub fn new<F>(reserve_bytes: u64, measure: F) -> Self
    where
        F: Fn(&Path) -> Result<u64, VmError> + Send + Sync + 'static,
    {
        Self {
            reserve_bytes,
            probe_interval: DISK_WAIT_PROBE_INTERVAL,
            measure: Arc::new(measure),
        }
    }

    /// Override the re-measure cadence of a held slot.
    pub fn with_probe_interval(mut self, probe_interval: Duration) -> Self {
        self.probe_interval = probe_interval;
        self
    }

    pub fn reserve_bytes(&self) -> u64 {
        self.reserve_bytes
    }

    pub fn probe_interval(&self) -> Duration {
        self.probe_interval
    }

    /// Free bytes on the volume holding `path`.
    pub fn free_bytes(&self, path: &Path) -> Result<u64, VmError> {
        (self.measure)(path)
    }
}

impl Default for JobVmDiskReserve {
    fn default() -> Self {
        Self::from_env()
    }
}

impl std::fmt::Debug for JobVmDiskReserve {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JobVmDiskReserve")
            .field("reserve_bytes", &self.reserve_bytes)
            .field("probe_interval", &self.probe_interval)
            .finish_non_exhaustive()
    }
}

/// Wait until the VM volume has the configured reserve free.
///
/// Returns `false` when `shutdown` fired first: the caller then abandons the
/// provision instead of booting a VM the teardown would immediately kill.
///
/// A shortfall is a wait, never an error. Failing here would feed
/// `record_slot_failure` and `consecutive_provision_failures`, which is the
/// crash-loop escalation meant for a broken provision — not for a host that is
/// merely full.
async fn wait_for_vm_disk(config: &RunnerPoolConfig, shutdown: &CancellationToken) -> bool {
    let reserve = config.job_vm_disk.reserve_bytes();
    if reserve == 0 {
        return true;
    }
    let root = golden_disk_root(config);
    let mut warned_at: Option<tokio::time::Instant> = None;
    loop {
        match config.job_vm_disk.free_bytes(&root) {
            Ok(free) if free >= reserve => return true,
            Ok(free) => {
                let now = tokio::time::Instant::now();
                if warned_at.is_none_or(|last| now.duration_since(last) >= DISK_WAIT_LOG_INTERVAL) {
                    warned_at = Some(now);
                    warn!(
                        path = %root.display(),
                        free = %format_gib(free),
                        reserve = %format_gib(reserve),
                        "waiting for disk: {} free on {}, reserve {}",
                        format_gib(free),
                        root.display(),
                        format_gib(reserve),
                    );
                }
            }
            Err(error) => {
                warn!(
                    path = %root.display(),
                    %error,
                    "free disk space unmeasurable; starting job VMs without the reserve"
                );
                return true;
            }
        }
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return false,
            _ = tokio::time::sleep(config.job_vm_disk.probe_interval()) => {}
        }
    }
}

/// Cadence of the runtime orphan reconcile.
///
/// Startup reconciles once, and nothing reconciled again until the next
/// restart: a `machine delete` that failed, or a data dir removed from under a
/// live hypervisor, then leaked that VM's whole disk for as long as the engine
/// served — one live engine grew from 99 GB to 168 GB of VM state in an
/// afternoon. Ten minutes bounds a leak to a small multiple of that rate while
/// costing almost nothing per tick: a registry `list()` with one `data-dir`
/// query per registered machine, a directory scan, and a `ps` scan. The
/// sweep's own 120 s grace for creates in flight is twelve times shorter than
/// the interval, so an orphan is always old enough to collect on the first
/// tick that sees it.
const ORPHAN_RECONCILE_INTERVAL: Duration = Duration::from_secs(600);

/// Stops the periodic orphan reconcile on every pool exit path.
struct ReconcileGuard {
    shutdown: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for ReconcileGuard {
    fn drop(&mut self) {
        self.shutdown.cancel();
        // Each tick's work is idempotent cleanup; aborting only keeps a
        // stopped pool from touching the host after it returned.
        self.handle.abort();
    }
}

/// Reclaim leaked VM state for as long as the pool serves.
///
/// Deliberately not `remove_stale_machines`: that deletes *every* runner
/// machine, including the ones running jobs right now. It uses the
/// registry-fenced sweep plus the mid-flight hypervisor purge scope, so a
/// registered machine, a golden, and a create in flight are all out of reach.
async fn reconcile_orphans<P: VmProvider + 'static>(
    provider: Arc<P>,
    shutdown: CancellationToken,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // `interval` ticks once immediately; the startup pass just ran.
    ticker.tick().await;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = ticker.tick() => {}
        }
        // Sweep first, so a data dir removed on this tick has the hypervisor
        // still holding its blocks released by the purge below, in the same
        // tick, instead of an interval later.
        match provider.sweep_orphaned_data_dirs().await {
            Ok(swept) if swept > 0 => info!(swept, "removed orphaned machine data directories"),
            Ok(_) => {}
            Err(error) => warn!(%error, "periodic orphaned data-dir sweep failed"),
        }
        match preloop_vm::purge_orphaned_vms(preloop_vm::OrphanPurge::RemovedDataDir) {
            Ok(killed) if killed > 0 => {
                info!(killed, "purged orphaned SmolVM hypervisor processes")
            }
            Ok(_) => {}
            Err(error) => warn!(%error, "periodic orphaned hypervisor purge failed"),
        }
    }
}

/// Public OCI artifacts carrying the packed VM goldens, per architecture.
///
/// Deliberately separate from the `runner-images` base-image package: that
/// is an OCI rootfs image (a bake input — users never download it), while
/// these packages contain a `.smolmachine` payload ready for
/// `machine create --from`.
///
/// Defaults are pinned to immutable manifest digests: a mutable tag could
/// be silently replaced between the manifest fetch and the blob pull, and
/// moving a default stays a reviewed code change instead of a registry
/// retag. The artifacts are produced by the golden pipeline (host-side
/// bake of the official runner image, packed via `smolvm pack`); the
/// release flow retains packed goldens as workflow artifacts because
/// GitHub Release assets are capped at 2 GiB; `PRELOOP_GOLDEN_URL` selects
/// a custom host when one is available.
///
/// An architecture with no published packed golden returns `None`; the
/// engine then falls back to the release-asset path and a local bake.
fn default_golden_oci_ref() -> Option<&'static str> {
    match std::env::consts::ARCH {
        "aarch64" => Some(GOLDEN_OCI_REF_ARM64),
        "x86_64" => Some(GOLDEN_OCI_REF_X86_64),
        _ => None,
    }
}

/// The arm64 packed golden, digest-pinned to the published
/// `preloop-arm64-smolvm-golden` artifact (pushed 2026-09-30 from a
/// macstudio `build-golden` bake of `runner-images@3884ef22…`).
const GOLDEN_OCI_REF_ARM64: &str = "ghcr.io/preloopdev/preloop-arm64-smolvm-golden@sha256:cf50db4cbbb38f47f0a533e1e35b523c6427df30261a3cd5bb49d9ef7e072414";
/// The x86_64 packed golden, digest-pinned to the published
/// `preloop-x86_64-smolvm-golden` artifact (pushed 2026-10-01 from a cpane
/// `build-golden` bake of `runner-images@4f7e4be4…`).
const GOLDEN_OCI_REF_X86_64: &str = "ghcr.io/preloopdev/preloop-x86_64-smolvm-golden@sha256:5757a8319aba0604672e62425559f207cc9f21e9380be01e8e80227eaec21914";
/// Deadline for a whole golden download, response body included.
///
/// The packed golden runs to ~9.6 GB, so this budget is really a floor on
/// link speed rather than a formality: finishing inside an hour needs ~21
/// Mbps sustained. The original 10-minute budget demanded 128 Mbps, which
/// an ordinary connection cannot serve — it killed the transfer around
/// two-thirds through and fell back to a local bake that looked like the
/// artifact was missing.
const GOLDEN_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(60 * 60);
/// Transfer attempts for one golden source before it is abandoned.
///
/// Each attempt resumes from the bytes already on disk, so a link that drops
/// mid-body costs only the bytes in flight instead of the whole artifact. The
/// packed golden runs to several gigabytes and the registry edge drops long
/// bodies often enough that a single attempt is not a reliable transfer.
const GOLDEN_DOWNLOAD_ATTEMPTS: usize = 5;
/// Pause before a resumed attempt: long enough for a dropped connection's
/// state to clear, short enough that a flapping link still finishes.
const GOLDEN_DOWNLOAD_RETRY_DELAY: Duration = Duration::from_secs(5);
/// How often a download reports progress. 256 MB puts a ~9.6 GB pull at
/// roughly one line every 25 s on a 100 Mbps link: often enough to show
/// movement, sparse enough not to bury the log.
const GOLDEN_PROGRESS_INTERVAL: u64 = 256 * 1000 * 1000;
/// Space retained after the temporary payload lands. The download is renamed
/// in place, so no second full copy is needed, but the engine still needs room
/// for filesystem metadata, logs, and the next VM operation.
const GOLDEN_DOWNLOAD_HEADROOM: u64 = 2 * 1024 * 1024 * 1024;
/// Conservative estimate when a custom server omits `Content-Length` or an
/// OCI manifest omits its descriptor size. The current packed golden is
/// approximately 9.6 GB.
const GOLDEN_UNKNOWN_SIZE_ESTIMATE: u64 = 10_000_000_000;
type AvailableSpace = fn(&Path) -> std::io::Result<u64>;

fn golden_download_percent(downloaded_bytes: u64, total_bytes: Option<u64>) -> Option<u8> {
    let total_bytes = total_bytes.filter(|total| *total > 0)?;
    Some(
        downloaded_bytes
            .min(total_bytes)
            .saturating_mul(100)
            .checked_div(total_bytes)
            .unwrap_or_default() as u8,
    )
}

/// Whole megabytes, the unit a multi-gigabyte download is legible in.
fn megabytes(bytes: u64) -> u64 {
    bytes / 1_000_000
}

/// Fixed-width completion bar, e.g. `[########------------]` at 40%.
fn progress_bar(percent: u8) -> String {
    const CELLS: usize = 20;
    let filled = percent.min(100) as usize * CELLS / 100;
    let mut bar = String::with_capacity(CELLS + 2);
    bar.push('[');
    for cell in 0..CELLS {
        bar.push(if cell < filled { '#' } else { '-' });
    }
    bar.push(']');
    bar
}

fn report_golden_download_progress(source: &str, downloaded_bytes: u64, total_bytes: Option<u64>) {
    match (
        total_bytes,
        golden_download_percent(downloaded_bytes, total_bytes),
    ) {
        (Some(total_bytes), Some(percent)) => info!(
            "golden download ({}): {} {}% ({} MB / {} MB)",
            source,
            progress_bar(percent),
            percent,
            megabytes(downloaded_bytes),
            megabytes(total_bytes)
        ),
        // No Content-Length and no manifest size: report the only honest
        // number rather than a percentage of an unknown total.
        _ => info!(
            "golden download ({}): {} MB, total size unknown",
            source,
            megabytes(downloaded_bytes)
        ),
    }
}

/// Free space on the filesystem holding `path`, in bytes.
#[cfg(unix)]
fn filesystem_available_space(path: &Path) -> std::io::Result<u64> {
    let stat = nix::sys::statvfs::statvfs(path).map_err(std::io::Error::from)?;
    Ok(stat.blocks_available() as u64 * stat.fragment_size() as u64)
}

/// `statvfs` is Unix-only; the caller logs the error and continues without
/// the space check.
#[cfg(not(unix))]
fn filesystem_available_space(_path: &Path) -> std::io::Result<u64> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "free-space checks require a Unix host",
    ))
}

fn golden_download_required_bytes(expected_bytes: Option<u64>) -> u64 {
    expected_bytes
        .unwrap_or(GOLDEN_UNKNOWN_SIZE_ESTIMATE)
        .saturating_add(GOLDEN_DOWNLOAD_HEADROOM)
}

fn ensure_golden_download_space(
    payload: &Path,
    expected_bytes: Option<u64>,
    available_space: AvailableSpace,
) -> Result<(), OrchestratorError> {
    let parent = payload
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let required_bytes = golden_download_required_bytes(expected_bytes);
    let available_bytes = match available_space(parent) {
        Ok(bytes) => bytes,
        Err(error) => {
            warn!(
                %error,
                path = %parent.display(),
                "Could not determine free space before golden download; continuing"
            );
            return Ok(());
        }
    };
    if available_bytes >= required_bytes {
        return Ok(());
    }

    warn!(
        path = %parent.display(),
        available_mb = megabytes(available_bytes),
        required_mb = megabytes(required_bytes),
        "Insufficient disk space for pre-baked golden"
    );
    Err(OrchestratorError::GoldenDiskSpace {
        path: parent.to_path_buf(),
        available_bytes,
        required_bytes,
    })
}

fn should_download_prebaked_golden(base_image: &str, custom_golden_url: bool) -> bool {
    is_stock_base_image(base_image) || custom_golden_url
}

async fn download_prebaked_golden(
    payload: &Path,
    release_version: &str,
) -> Result<bool, OrchestratorError> {
    download_prebaked_golden_with_space(payload, release_version, filesystem_available_space).await
}

async fn download_prebaked_golden_with_space(
    payload: &Path,
    release_version: &str,
    available_space: AvailableSpace,
) -> Result<bool, OrchestratorError> {
    // An exported-but-blank `PRELOOP_GOLDEN_URL` must behave like an unset
    // one in both places below: the operator otherwise gets neither the OCI
    // default nor their (empty) override.
    let forced_url = std::env::var("PRELOOP_GOLDEN_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());
    // Per-architecture OCI golden: try it wherever a packed artifact is
    // published for this platform; `PRELOOP_GOLDEN_OCI_REF` overrides the
    // per-arch default regardless of platform. A missing package (e.g. an
    // arch not yet published) falls through to the release-asset path.
    if forced_url.is_none() {
        let reference = std::env::var("PRELOOP_GOLDEN_OCI_REF")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| default_golden_oci_ref().map(str::to_owned));
        if let Some(reference) = reference {
            if download_oci_golden(payload, &reference, available_space).await? {
                return Ok(true);
            }
            info!(reference, "OCI golden unavailable; trying release asset");
        }
    }

    let client = match reqwest::Client::builder()
        .timeout(GOLDEN_DOWNLOAD_TIMEOUT)
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            warn!(%error, "Could not create golden download client");
            return Ok(false);
        }
    };

    // The tag is only resolved once the engine's own release has failed: the
    // common case needs no API call, and an unauthenticated lookup is rate
    // limited.
    let resolved_tag = if forced_url.is_none() {
        resolve_latest_golden_tag(&client, GITHUB_API_BASE).await
    } else {
        None
    };
    for url in golden_url_candidates(release_version, resolved_tag.as_deref(), forced_url) {
        info!(
            url = %url,
            target = %payload.display(),
            "Downloading pre-baked golden from release asset (this may take several minutes)"
        );
        if download_release_asset(&client, &url, payload, available_space).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Fetch one release asset into `payload`: probe, checksum, transfer, install.
///
/// The transfer resumes from whatever a previous attempt left behind, so a
/// dropped connection costs the bytes in flight rather than the artifact.
async fn download_release_asset(
    client: &reqwest::Client,
    url: &str,
    payload: &Path,
    available_space: AvailableSpace,
) -> Result<bool, OrchestratorError> {
    let probe = match client.get(url).send().await {
        Ok(response) if response.status().is_success() => response,
        Ok(response) => {
            info!(
                status = %response.status(),
                url = %url,
                "Pre-baked golden release asset unavailable; will build locally"
            );
            return Ok(false);
        }
        Err(error) => {
            warn!(%error, url = %url, "Pre-baked golden release download failed; will build locally");
            return Ok(false);
        }
    };
    let total_bytes = probe.content_length();
    ensure_golden_download_space(payload, total_bytes, available_space)?;

    // Fetch the companion checksum before committing bandwidth to the body.
    // A truncated or corrupted golden only fails much later, when a VM tries
    // to boot it, so a mismatch must be caught here. Releases that do not
    // publish a checksum are tolerated with a warning (the download is still
    // the best path when the alternative is a full local build).
    let expected_sha256 = match tokio::time::timeout(
        Duration::from_secs(15),
        client.get(format!("{url}.sha256")).send(),
    )
    .await
    {
        Ok(Ok(res)) if res.status().is_success() => match res.text().await {
            Ok(text) => parse_sha256_checksum(&text),
            Err(_) => None,
        },
        _ => None,
    };
    if expected_sha256.is_none() {
        warn!(url = %format!("{url}.sha256"), "no golden checksum published; downloading without verification");
    }

    let partial = golden_partial_path(payload);
    let downloaded_bytes = {
        let client = client.clone();
        let url = url.to_owned();
        download_golden_with_resume(&partial, "release", None, Some(probe), move |offset| {
            let client = client.clone();
            let url = url.clone();
            Box::pin(async move {
                let mut request = client.get(&url);
                if offset > 0 {
                    request = request.header(reqwest::header::RANGE, format!("bytes={offset}-"));
                }
                request
                    .send()
                    .await
                    .map_err(|error| format!("request failed: {error}"))
            }) as BoxFuture<'static, Result<reqwest::Response, String>>
        })
        .await
    };
    let downloaded_bytes = match downloaded_bytes {
        Ok(downloaded_bytes) => downloaded_bytes,
        Err(error) => {
            warn!(url = %url, %error, "Pre-baked golden release download failed; will build locally");
            return Ok(false);
        }
    };

    if let Some(expected) = expected_sha256 {
        match sha256_file(&partial).await {
            Ok(actual) if actual == expected => {}
            Ok(actual) => {
                warn!(
                    expected,
                    %actual,
                    "golden checksum mismatch; discarding download and building locally"
                );
                let _ = tokio::fs::remove_file(&partial).await;
                return Ok(false);
            }
            Err(error) => {
                warn!(%error, "could not hash downloaded golden; building locally");
                let _ = tokio::fs::remove_file(&partial).await;
                return Ok(false);
            }
        }
    }

    if tokio::fs::rename(&partial, payload).await.is_err() {
        let _ = tokio::fs::remove_file(&partial).await;
        return Ok(false);
    }
    report_golden_download_progress("release", downloaded_bytes, None);
    info!(target = %payload.display(), "Downloaded pre-baked golden microVM image successfully");
    Ok(true)
}

/// Transfer one golden source into `partial`, resuming across attempts.
///
/// `request_at` issues the request for a given byte offset; the caller decides
/// how to authenticate (a plain GET for a release asset, a registry token for
/// an OCI blob). Returns the bytes the file holds once the server has sent the
/// whole body.
async fn download_golden_with_resume(
    partial: &Path,
    source: &str,
    expected_total_bytes: Option<u64>,
    initial_response: Option<reqwest::Response>,
    mut request_at: impl FnMut(u64) -> BoxFuture<'static, Result<reqwest::Response, String>>,
) -> Result<u64, String> {
    // Claim the companion lock before creating the payload: a staging file
    // without a fresh lock reads as orphaned to a concurrent sweep.
    let _staging_guard = StagingLockGuard::claim(staging_lock_path(partial));
    // The caller's probe already holds the first body; reusing it keeps the
    // transfer at one request per attempt.
    let mut pending_response = initial_response;
    let mut last_error = String::from("no attempt was made");
    for attempt in 1..=GOLDEN_DOWNLOAD_ATTEMPTS {
        let have = tokio::fs::metadata(partial)
            .await
            .map(|meta| meta.len())
            .unwrap_or(0);
        if have > 0 && expected_total_bytes == Some(have) {
            return Ok(have);
        }
        // A file larger than the artifact is left over from a different build:
        // appending to it would mix two images, so start over.
        if expected_total_bytes.is_some_and(|total| have > total) {
            let _ = tokio::fs::remove_file(partial).await;
            continue;
        }
        // A transfer that cannot land wastes the whole multi-GiB body and
        // dies at ENOSPC partway through; refuse before requesting it. The
        // probe's Content-Length stands in when the caller had no size.
        let total = expected_total_bytes.or_else(|| {
            pending_response
                .as_ref()
                .filter(|_| have == 0)
                .and_then(reqwest::Response::content_length)
        });
        if let Some(total) = total {
            let directory = partial.parent().unwrap_or(Path::new("."));
            ensure_free_disk(
                directory,
                total.saturating_sub(have) + GOLDEN_DOWNLOAD_DISK_MARGIN,
                "golden download",
                "free space on this volume",
            )?;
        }
        let response = match pending_response.take().filter(|_| have == 0) {
            Some(response) => response,
            None => match request_at(have).await {
                Ok(response) => response,
                Err(error) => {
                    last_error = error;
                    if attempt < GOLDEN_DOWNLOAD_ATTEMPTS {
                        tokio::time::sleep(GOLDEN_DOWNLOAD_RETRY_DELAY).await;
                    }
                    continue;
                }
            },
        };
        match response.status() {
            // Nothing left to send: the file already holds the whole artifact.
            reqwest::StatusCode::RANGE_NOT_SATISFIABLE => return Ok(have),
            status if status.is_success() => {
                let resuming = status == reqwest::StatusCode::PARTIAL_CONTENT && have > 0;
                let offset = if resuming { have } else { 0 };
                match stream_golden_body(
                    response,
                    partial,
                    resuming,
                    offset,
                    source,
                    expected_total_bytes,
                )
                .await
                {
                    Ok(downloaded_bytes) => return Ok(downloaded_bytes),
                    Err(error) => last_error = error,
                }
            }
            // A 404 on the versioned URL is the caller's cue to try the next
            // candidate; retrying it here would only burn the retry budget.
            status if status.is_client_error() => return Err(format!("HTTP {status}")),
            status => last_error = format!("HTTP {status}"),
        }
        if attempt < GOLDEN_DOWNLOAD_ATTEMPTS {
            warn!(
                source,
                attempt,
                error = %last_error,
                "golden transfer attempt failed; resuming from the bytes on disk"
            );
            tokio::time::sleep(GOLDEN_DOWNLOAD_RETRY_DELAY).await;
        }
    }
    Err(last_error)
}

/// Copy a response body into `partial`, appending when the server resumed.
///
/// Returns the total bytes the file now holds. The file is left in place on
/// failure so the next attempt resumes it; the caller's checksum decides
/// whether what is on disk is usable.
async fn stream_golden_body(
    response: reqwest::Response,
    partial: &Path,
    resuming: bool,
    already_have: u64,
    source: &str,
    expected_total_bytes: Option<u64>,
) -> Result<u64, String> {
    let mut file = if resuming {
        tokio::fs::OpenOptions::new()
            .append(true)
            .open(partial)
            .await
    } else {
        tokio::fs::File::create(partial).await
    }
    .map_err(|error| format!("could not open {}: {error}", partial.display()))?;
    let total_bytes = response
        .content_length()
        .map(|remaining| remaining.saturating_add(already_have))
        .or(expected_total_bytes);
    let mut downloaded_bytes = already_have;
    let mut next_progress = downloaded_bytes.saturating_add(GOLDEN_PROGRESS_INTERVAL);
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(|error| format!("stream failed after {downloaded_bytes} bytes: {error}"))?;
        file.write_all(&chunk)
            .await
            .map_err(|error| format!("write failed after {downloaded_bytes} bytes: {error}"))?;
        downloaded_bytes = downloaded_bytes.saturating_add(chunk.len() as u64);
        if downloaded_bytes >= next_progress {
            report_golden_download_progress(source, downloaded_bytes, total_bytes);
            next_progress = next_progress.saturating_add(GOLDEN_PROGRESS_INTERVAL);
        }
    }
    // `write_all` only queues work on a `tokio::fs::File`; the flush is what
    // surfaces a failed write-back before the caller trusts the file.
    file.flush()
        .await
        .map_err(|error| format!("flush failed after {downloaded_bytes} bytes: {error}"))?;
    Ok(downloaded_bytes)
}

/// SHA-256 of a file, lowercase hex.
async fn sha256_file(path: &Path) -> Result<String, String> {
    let hashed = path.to_owned();
    let display = path.display().to_string();
    match tokio::task::spawn_blocking(move || {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        let mut file = std::fs::File::open(&hashed)?;
        std::io::copy(&mut file, &mut hasher)?;
        Ok::<String, std::io::Error>(format!("{:x}", hasher.finalize()))
    })
    .await
    {
        Ok(Ok(digest)) => Ok(digest),
        Ok(Err(error)) => Err(format!("could not hash {display}: {error}")),
        Err(error) => Err(format!("could not join hash task: {error}")),
    }
}

/// Download the packed VM layer from a public OCI artifact without requiring
/// `oras`, Docker, or any other host-side registry client.
async fn download_oci_golden(
    payload: &Path,
    reference: &str,
    available_space: AvailableSpace,
) -> Result<bool, OrchestratorError> {
    let Ok(oci) = OciReference::parse(reference) else {
        warn!(reference, "invalid OCI golden reference");
        return Ok(false);
    };
    let client = match reqwest::Client::builder()
        .timeout(GOLDEN_DOWNLOAD_TIMEOUT)
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            warn!(%error, "Could not create OCI golden download client");
            return Ok(false);
        }
    };
    // Accept indexes too: `smolvm pack push` publishes the packed artifact
    // under an OCI index, so the digest/tags resolve to one indirection. The
    // shared client follows that one level, taking the first listed manifest
    // (our packs publish exactly one platform entry).
    let resolved = match get_manifest(&client, &oci, MANIFEST_ACCEPT, None).await {
        Ok(resolved) => resolved,
        Err(error) => {
            warn!(reference, %error, "OCI golden manifest unavailable");
            return Ok(false);
        }
    };
    if let Some(error) = &resolved.follow_error {
        warn!(reference, %error, "OCI golden inner manifest unavailable");
        return Ok(false);
    }
    let manifest = match serde_json::from_value::<OciManifest>(resolved.image) {
        Ok(manifest) => manifest,
        Err(error) => {
            let message = if resolved.selected_digest.is_some() {
                "OCI golden inner manifest parse failed"
            } else {
                "OCI golden manifest parse failed"
            };
            warn!(reference, %error, message);
            return Ok(false);
        }
    };
    let Some(layer) = manifest
        .layers
        .into_iter()
        .find(|layer| is_packed_vm_layer(&layer.media_type))
    else {
        warn!(reference, "OCI golden has no packed VM layer");
        return Ok(false);
    };
    let layer_size = layer.size;
    ensure_golden_download_space(payload, layer_size, available_space)?;
    let layer_digest = layer.digest;
    let blob_url = oci.blob_url(&layer_digest);
    info!(
        "pulling pre-baked OCI golden ({} MB) from {} into {}",
        layer_size.map(megabytes).unwrap_or_default(),
        reference,
        payload.display()
    );
    let partial = golden_partial_path(payload);
    let downloaded = {
        let client = client.clone();
        let blob_url = blob_url.clone();
        let repository = oci.repository.clone();
        download_golden_with_resume(&partial, "OCI", layer_size, None, move |offset| {
            let client = client.clone();
            let url = blob_url.clone();
            let repository = repository.clone();
            Box::pin(async move {
                crate::oci::registry_get(
                    &client,
                    &url,
                    &repository,
                    "*/*",
                    None,
                    (offset > 0).then_some(offset),
                )
                .await
                .map_err(|error| error.to_string())
            }) as BoxFuture<'static, Result<reqwest::Response, String>>
        })
        .await
    };
    match downloaded {
        Ok(downloaded_bytes) => {
            // The layer digest is the artifact's identity: a transfer that
            // resumed across attempts must still hash to the manifest's digest
            // before it is installed.
            let expected = layer_digest
                .strip_prefix("sha256:")
                .unwrap_or(&layer_digest)
                .to_owned();
            match sha256_file(&partial).await {
                Ok(actual) if actual == expected => {
                    if let Err(error) = tokio::fs::rename(&partial, payload).await {
                        warn!(
                            reference,
                            %error,
                            target = %payload.display(),
                            "Downloaded OCI golden but could not install it"
                        );
                    } else {
                        info!(
                            reference,
                            target = %payload.display(),
                            downloaded_bytes,
                            "Downloaded OCI pre-baked golden microVM image successfully"
                        );
                        return Ok(true);
                    }
                }
                Ok(actual) => {
                    warn!(
                        expected,
                        %actual,
                        "OCI golden digest mismatch; discarding download"
                    );
                    let _ = tokio::fs::remove_file(&partial).await;
                }
                Err(error) => {
                    warn!(reference, %error, "could not hash downloaded OCI golden");
                    let _ = tokio::fs::remove_file(&partial).await;
                }
            }
        }
        Err(error) => {
            warn!(
                reference,
                %error,
                "OCI golden download failed; will try the release asset"
            );
        }
    }
    Ok(false)
}

/// First whitespace-separated token of a `sha256sum`-style checksum file
/// (`<hex> <filename>`), lowercased. `None` when the file does not parse.
fn parse_sha256_checksum(text: &str) -> Option<String> {
    let token = text.split_whitespace().next()?;
    let token = token.strip_prefix("sha256:").unwrap_or(token);
    if token.len() == 64 && token.bytes().all(|b| b.is_ascii_hexdigit()) {
        Some(token.to_ascii_lowercase())
    } else {
        None
    }
}
/// Packages common to Ubuntu 22.04 and 24.04 environment goldens.
///
/// Tracks the apt baseline of GitHub's hosted Ubuntu images. ABI-transition
/// packages are selected separately: Jammy uses the original names while
/// Noble renamed them with the `t64` suffix.
const BASE_PACKAGES: &str = "\
     git curl wget ca-certificates gnupg2 sudo openssh-client \
     libnspr4 libnss3 libcairo2 libdbus-1-3 libdrm2 libgbm1 \
     libpango-1.0-0 libx11-6 libxcb1 libxcomposite1 \
     libxdamage1 libxext6 libxfixes3 libxkbcommon0 libxrandr2 \
     ruby ruby-rubygems perl cpanminus lsb-release fonts-noto-color-emoji \
     haveged mediainfo p7zip-rar pollinate sshpass telnet tk xvfb zsync ftp \
     sphinxsearch systemd-coredump libnss3-tools software-properties-common \
     build-essential pkg-config libssl-dev make autoconf automake libtool m4 \
     bison flex texinfo patchelf swig dpkg-dev fakeroot binutils lld \
     libicu-dev libsqlite3-dev libyaml-dev \
     python3 python3-pip python-is-python3 \
     unzip zip xz-utils zstd bzip2 brotli lz4 pigz p7zip-full tar \
     jq file tree shellcheck parallel time acl locales tzdata \
     rsync dnsutils iputils-ping net-tools iproute2 netcat-openbsd \
     sqlite3 rpm aria2 mercurial libcurl4-openssl-dev zlib1g-dev gettext \
     libexpat1-dev";

const BASE_PACKAGES_22_04: &str =
    "libatk1.0-0 libatk-bridge2.0-0 libatspi2.0-0 libcups2 libglib2.0-0 libasound2";
const BASE_PACKAGES_24_04: &str = "\
    libatk1.0-0t64 libatk-bridge2.0-0t64 libatspi2.0-0t64 \
    libcups2t64 libglib2.0-0t64 libasound2t64";

/// Node.js baked into the base image, pinned (via `versions.toml`) to the
/// GitHub-hosted ubuntu-24.04 system Node. Ubuntu's apt `nodejs` (18.19) is
/// deliberately *not* installed: workflows written against hosted runners
/// assume a modern Node on PATH, and the apt series floats with the archive.
pub const BASE_NODE_VERSION: &str = crate::NODE_VERSION;

/// Container engine, installed separately from [`BASE_PACKAGES`].
///
/// Installed from Docker's official apt repository (not Ubuntu's `docker.io`
/// package): the runner needs parity with the `ubuntu-latest` container
/// stack, and the official packages ship `dockerd`, the CLI, and the
/// buildx/compose plugins as first-class artifacts.
///
/// Kept apart because it needs storage configuration the other packages do not
/// — see [`DOCKER_DATA_ROOT`].
/// Docker's apt repository supplies the service unit and runtime dependencies.
/// Its retained package set floats, so the bake overlays the exact official
/// image engine/CLI and plugin binaries afterward.
fn docker_apt_packages() -> String {
    "docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin".to_owned()
}

/// Compiler families preinstalled by GitHub's Ubuntu 24.04 image.
///
/// Clang includes the compiler, formatter, and tidy tools for each version.
/// GNU C, C++, and Fortran are all present for 12, 13, and 14.
fn compiler_apt_packages() -> String {
    let mut packages = Vec::new();
    for version in CLANG_VERSIONS.split_whitespace() {
        for package in ["clang", "clang-format", "clang-tidy"] {
            packages.push(format!("{package}-{version}"));
        }
    }
    for version in GCC_VERSIONS.split_whitespace() {
        for package in ["gcc", "g++", "gfortran"] {
            packages.push(format!("{package}-{version}"));
        }
    }
    packages.join(" ")
}

/// Where the container engine stores images and layers.
///
/// Must be a real filesystem, not the guest's overlayfs root. containerd's
/// overlayfs snapshotter mounts each container's rootfs as an overlay whose
/// `lowerdir` is an image layer; when those layers themselves sit on an
/// overlayfs, the mount fails with `invalid argument` and every `docker create`
/// exits 1. `/storage` is plain ext4 on `/dev/vda`.
///
/// Tempting and wrong: putting this on the overlay root so that images pulled
/// into the golden are inherited by forks. Inheritance does work there -- and
/// the images are then unusable, because a layer arriving through a *lower*
/// overlay cannot back another overlay mount. Pull-and-run appears to succeed
/// when testing in a single VM, since those writes land in that VM's own upper
/// layer; the failure only shows up in a fork.
const DOCKER_DATA_ROOT: &str = "/storage/docker";

/// Standard loopback entries for `/etc/hosts`.
/// Runner root inside the guest. Must match the `--runner-root` argument
/// passed to configure at provision time.
/// Lives under the runner user's home, matching the GitHub-hosted layout
/// (`/home/runner/...`). A `/var/lib/...` root tripped path-assuming
/// workflows: jekyll's profiler regex carries an unanchored `/lib`
/// alternative that matches `var/lib` and mis-normalizes every theme path,
/// while hosted runners have no `/lib` prefix to catch on.
const RUNNER_ROOT: &str = "/home/runner";

/// The process stack limit GitHub's hosted images give every runner.
///
/// `actions/runner-images` doubles the kernel's 8192 KiB default for hosted
/// runners — `images/ubuntu/scripts/build/configure-limits.sh` writes
/// `DefaultLimitSTACK=16M:infinity` into `/etc/systemd/system.conf` and
/// `* soft stack 16384` into `/etc/security/limits.conf`, under a comment
/// reading "Double stack size from default 8192KB". A hosted runner's step
/// inherits it from the `actions.runner.*` service, so that pair is what a
/// workflow sees on `ubuntu-latest`.
///
/// The guest boots straight into the job workload — no systemd unit and no
/// PAM session ever reads those files — so the runner chain would inherit the
/// VM init's 8192 KiB, half of GitHub's. Deep recursion is where the gap
/// shows: pydantic's `test_recursive_call` and `test_fallback_cycle_change`
/// walk Python's recursion limit through pydantic-core's Rust frames and die
/// with a stack-overflow `SIGSEGV` (exit 139) under 8192 KiB, where the same
/// commit passes on GitHub under 16384 KiB.
///
/// The shell text is `golden_stack_ulimit_raise` in `official-image.toml` —
/// the same file that carries the values it composes — compiled in by
/// `build.rs`, so the init carries no literal.
///
/// Soft is the hosted value; hard stays `unlimited` as the image sets it, so a
/// step that raises its own soft limit keeps working. Raising a hard limit
/// needs root (CAP_SYS_RESOURCE), and a launch that applies this pair for a
/// workload runs as root or through passwordless sudo: the switched runner
/// wrapper, the container engine's own start, and the golden-side preload
/// daemon that forks inherit. A raise that fails there ends the launch, so a
/// workload never comes up on a stack it was not meant to have. A chain
/// already running keeps the limits it was born with, so
/// [`docker_start_command`] re-raises an inherited one rather than trusting
/// it.
const GUEST_STACK_ULIMIT: &str = GOLDEN_STACK_ULIMIT_RAISE;

/// [`GUEST_STACK_ULIMIT`] for the launches that keep the exec channel's
/// identity and therefore cannot assume root: re-setting a hard limit needs
/// no privilege only while it does not move, so a guest that inherited a
/// finite hard stack keeps what it has — saying so on stderr — instead of
/// failing the launch. The privileged launch sites use the strict form above
/// and fail loudly.
const GUEST_STACK_ULIMIT_BEST_EFFORT: &str = GOLDEN_STACK_ULIMIT_RAISE_BEST_EFFORT;

/// The file-descriptor limit GitHub's hosted images give every runner.
///
/// The same `actions/runner-images`
/// `images/ubuntu/scripts/build/configure-limits.sh` writes
/// `DefaultLimitNOFILE=65536` into `/etc/systemd/system.conf` and
/// `* soft nofile 65536` / `* hard nofile 65536` into
/// `/etc/security/limits.conf`; a probe job on a hosted runner reads back
/// `Max open files 65536 / 65536` in a step, in a `container:` job and in an
/// ad-hoc `docker run` alike. A preloop job inherits the exec channel's
/// defaults instead — 1024 soft / 4096 hard on AgentENV, where jobs arrive
/// through `aenv exec` off `envd` — which is below what suites that raise
/// their own soft limit ask for (valkey's test suite requests 10032) and
/// makes the runner die with `EPERM` on `setrlimit`.
///
/// Applied next to [`GUEST_STACK_ULIMIT`] on the same launch sites: the runner
/// wrapper raises before it drops privileges (raising a hard limit needs
/// root), and the container engine raises so its containers inherit the pair.
/// Only the order-independent first hard raise may be refused there; a soft or
/// pinning raise that fails ends the launch instead of leaving the workload on
/// a pair it was not meant to have. A site that may run unprivileged gets
/// [`GUEST_NOFILE_ULIMIT_BEST_EFFORT`] instead.
const GUEST_NOFILE_ULIMIT: &str = GOLDEN_NOFILE_ULIMIT_RAISE;

/// [`GUEST_NOFILE_ULIMIT`] for the launches that keep the exec channel's
/// identity and therefore cannot assume root: a process may lower its hard
/// limit and raise its soft limit only up to the hard one, so a guest whose
/// inherited hard limit is below the hosted 65536 keeps what it has instead of
/// failing the launch with `EPERM` noise. The privileged launch sites use the
/// strict form above and fail loudly.
const GUEST_NOFILE_ULIMIT_BEST_EFFORT: &str = GOLDEN_NOFILE_ULIMIT_RAISE_BEST_EFFORT;

/// Standard loopback entries for `/etc/hosts`.
///
/// The base image ships an **empty** `/etc/hosts`, and `nsswitch.conf` is
/// `hosts: files dns` so `localhost` falls through to the upstream resolver
/// and fails to resolve at all. Everything still works over `127.0.0.1`, which
/// is why this hides so well.
///
/// It breaks a large share of real workflows: `services:` containers are
/// reached at `localhost:<port>`, and most test suites connect to `localhost`
/// by name. GitHub's runners resolve it, so a workflow that depends on it is
/// correct — the gap is ours.
const LOOPBACK_HOSTS: &str = "127.0.0.1 localhost\\n\
                              ::1 localhost ip6-localhost ip6-loopback\\n\
                              fe00::0 ip6-localnet\\n\
                              ff00::0 ip6-mcastprefix\\n\
                              ff02::1 ip6-allnodes\\n\
                              ff02::2 ip6-allrouters\\n";

/// The golden's apt baseline, every package version-pinned (versions.toml).
/// Versions marked EXACT there match the official ubuntu-24.04 runner image.
fn base_packages_pinned() -> String {
    format!(
        "git={APT_GIT} \
        curl={APT_CURL} \
        wget={APT_WGET} \
        ca-certificates={APT_CA_CERTIFICATES} \
        gnupg2={APT_GNUPG2} \
        sudo={APT_SUDO} \
        openssh-client={APT_OPENSSH_CLIENT} \
        libnspr4={APT_LIBNSPR4} \
        libnss3={APT_LIBNSS3} \
        libatk1.0-0t64={APT_LIBATK1} \
        libatk-bridge2.0-0t64={APT_LIBATK_BRIDGE} \
        libatspi2.0-0t64={APT_LIBATSPI} \
        libcairo2={APT_LIBCAIRO2} \
        libcups2t64={APT_LIBCUPS2T64} \
        libdbus-1-3={APT_LIBDBUS_1_3} \
        libdrm2={APT_LIBDRM2} \
        libgbm1={APT_LIBGBM1} \
        libglib2.0-0t64={APT_LIBGLIB2} \
        libpango-1.0-0={APT_LIBPANGO} \
        libx11-6={APT_LIBX11_6} \
        libxcb1={APT_LIBXCB1} \
        libxcomposite1={APT_LIBXCOMPOSITE1} \
        libxdamage1={APT_LIBXDAMAGE1} \
        libxext6={APT_LIBXEXT6} \
        libxfixes3={APT_LIBXFIXES3} \
        libxkbcommon0={APT_LIBXKBCOMMON0} \
        libxrandr2={APT_LIBXRANDR2} \
        libasound2t64={APT_LIBASOUND2T64} \
        ruby={APT_RUBY} \
        ruby-rubygems={APT_RUBY_RUBYGEMS} \
        perl={APT_PERL} \
        cpanminus={APT_CPANMINUS} \
        lsb-release={APT_LSB_RELEASE} \
        fonts-noto-color-emoji={APT_FONTS_NOTO_COLOR_EMOJI} \
        haveged={APT_HAVEGED} \
        mediainfo={APT_MEDIAINFO} \
        p7zip-rar={APT_P7ZIP_RAR} \
        pollinate={APT_POLLINATE} \
        sshpass={APT_SSHPASS} \
        telnet={APT_TELNET} \
        tk={APT_TK} \
        xvfb={APT_XVFB} \
        zsync={APT_ZSYNC} \
        ftp={APT_FTP} \
        sphinxsearch={APT_SPHINXSEARCH} \
        systemd-coredump={APT_SYSTEMD_COREDUMP} \
        libnss3-tools={APT_LIBNSS3_TOOLS} \
        software-properties-common={APT_SOFTWARE_PROPERTIES_COMMON} \
        build-essential={APT_BUILD_ESSENTIAL} \
        pkg-config={APT_PKG_CONFIG} \
        libssl-dev={APT_LIBSSL_DEV} \
        make={APT_MAKE} \
        autoconf={APT_AUTOCONF} \
        automake={APT_AUTOMAKE} \
        libtool={APT_LIBTOOL} \
        m4={APT_M4} \
        bison={APT_BISON} \
        flex={APT_FLEX} \
        texinfo={APT_TEXINFO} \
        patchelf={APT_PATCHELF} \
        swig={APT_SWIG} \
        dpkg-dev={APT_DPKG_DEV} \
        fakeroot={APT_FAKEROOT} \
        binutils={APT_BINUTILS} \
        lld={APT_LLD} \
        libicu-dev={APT_LIBICU_DEV} \
        libsqlite3-dev={APT_LIBSQLITE3_DEV} \
        libyaml-dev={APT_LIBYAML_DEV} \
        python3={APT_PYTHON3} \
        python3-pip={APT_PYTHON3_PIP} \
        python-is-python3={APT_PYTHON_IS_PYTHON3} \
        unzip={APT_UNZIP} \
        zip={APT_ZIP} \
        xz-utils={APT_XZ_UTILS} \
        zstd={APT_ZSTD} \
        bzip2={APT_BZIP2} \
        brotli={APT_BROTLI} \
        lz4={APT_LZ4} \
        pigz={APT_PIGZ} \
        p7zip-full={APT_P7ZIP_FULL} \
        tar={APT_TAR} \
        jq={APT_JQ} \
        file={APT_FILE} \
        tree={APT_TREE} \
        shellcheck={APT_SHELLCHECK} \
        parallel={APT_PARALLEL} \
        time={APT_TIME} \
        acl={APT_ACL} \
        locales={APT_LOCALES} \
        tzdata={APT_TZDATA} \
        rsync={APT_RSYNC} \
        dnsutils={APT_DNSUTILS} \
        iputils-ping={APT_IPUTILS_PING} \
        net-tools={APT_NET_TOOLS} \
        iproute2={APT_IPROUTE2} \
        netcat-openbsd={APT_NETCAT_OPENBSD} \
        sqlite3={APT_SQLITE3} \
        rpm={APT_RPM} \
        aria2={APT_ARIA2} \
        mercurial={APT_MERCURIAL} \
        libcurl4-openssl-dev={APT_LIBCURL4_OPENSSL_DEV} \
        zlib1g-dev={APT_ZLIB1G_DEV} \
        gettext={APT_GETTEXT} \
        libexpat1-dev={APT_LIBEXPAT1_DEV}"
    )
}

/// The golden image's package baseline. Exposed for the fidelity tests.
pub fn base_packages() -> &'static str {
    BASE_PACKAGES
}

/// The golden image's container engine baseline. Exposed for the fidelity
/// tests.
pub fn docker_packages() -> String {
    docker_apt_packages()
}

/// The golden image's hosted compiler package baseline.
pub fn compiler_packages() -> String {
    compiler_apt_packages()
}

/// Where the container engine stores layers. Exposed for the fidelity tests.
pub fn docker_data_root() -> &'static str {
    DOCKER_DATA_ROOT
}

/// Loopback `/etc/hosts` contents. Exposed for the fidelity tests.
pub fn loopback_hosts() -> &'static str {
    LOOPBACK_HOSTS
}

fn node_externals_at(runner_root: &str) -> Vec<Vec<String>> {
    // Pinned SHA per runtime+platform, derived from versions.toml via build.rs.
    // Empty string means no pinned entry — SHASUMS verification still applies.
    let n20_plain = NODE20_EXTERNALS_VERSION.trim_start_matches('v');
    let n24_plain = NODE24_EXTERNALS_VERSION.trim_start_matches('v');
    let pinned_n20_arm64 =
        crate::node_externals_pinned_sha256(&format!("node20_{n20_plain}_linux-arm64"))
            .unwrap_or("");
    let pinned_n20_x64 =
        crate::node_externals_pinned_sha256(&format!("node20_{n20_plain}_linux-x64")).unwrap_or("");
    let pinned_n24_arm64 =
        crate::node_externals_pinned_sha256(&format!("node24_{n24_plain}_linux-arm64"))
            .unwrap_or("");
    let pinned_n24_x64 =
        crate::node_externals_pinned_sha256(&format!("node24_{n24_plain}_linux-x64")).unwrap_or("");
    // For Windows cases (not used on Linux golden but keep for completeness via host externals on Windows).
    let pinned_n20_win_x64 =
        crate::node_externals_pinned_sha256(&format!("node20_{n20_plain}_win-x64")).unwrap_or("");
    let pinned_n24_win_x64 =
        crate::node_externals_pinned_sha256(&format!("node24_{n24_plain}_win-x64")).unwrap_or("");
    [vec![
        "sh".to_owned(),
        "-c".to_owned(),
        format!(
            "RUNNER_EXTERNALS={runner_root}/externals && \
             mkdir -p \"$RUNNER_EXTERNALS\" && \
             for entry in 'node20 v{NODE20_EXTERNALS_VERSION}' 'node24 v{NODE24_EXTERNALS_VERSION}'; do \
               set -- $entry; \
               NAME=$1; VERSION=$2; \
               DEST=$RUNNER_EXTERNALS/$NAME; \
               VERSION_PLAIN=${{VERSION#v}}; \
               if [ -f \"$DEST/preloop-node.json\" ] && grep -q \"\\\"version\\\":\\\"$VERSION_PLAIN\\\"\" \"$DEST/preloop-node.json\" 2>/dev/null && [ \"$(\"$DEST/bin/node\" --version 2>/dev/null)\" = \"$VERSION\" ]; then \
                 echo \"$NAME $VERSION already valid, skipping\"; \
                 chmod 755 \"$DEST\" 2>/dev/null || true; \
                 continue; \
               fi; \
               echo \"Installing $NAME $VERSION into golden...\"; \
               TEMP=$(mktemp -d \"$RUNNER_EXTERNALS/.$NAME.XXXXXX\") && \
                chmod 755 \"$TEMP\" && \
                ARCH=$(uname -m); \
                if [ \"$ARCH\" = \"aarch64\" ] || [ \"$ARCH\" = \"arm64\" ]; then NODE_ARCH=linux-arm64; else NODE_ARCH=linux-x64; fi; \
                ARCHIVE=\"node-$VERSION-$NODE_ARCH.tar.gz\"; \
                URL=\"https://nodejs.org/dist/$VERSION/$ARCHIVE\"; \
                SHASUMS_URL=\"https://nodejs.org/dist/$VERSION/SHASUMS256.txt\"; \
                TMP_ARCHIVE=$(mktemp \"$RUNNER_EXTERNALS/.$NAME.archive.XXXXXX.tar.gz\"); \
                if ! curl -fsSL -o \"$TMP_ARCHIVE\" \"$URL\"; then echo \"FAILED fetching $NAME $VERSION\" >&2; rm -f \"$TMP_ARCHIVE\"; rm -rf \"$TEMP\"; exit 1; fi; \
                ACTUAL=$(shasum -a 256 \"$TMP_ARCHIVE\" 2>/dev/null | awk '{{print $1}}'); \
                if [ -z \"$ACTUAL\" ]; then ACTUAL=$(sha256sum \"$TMP_ARCHIVE\" 2>/dev/null | awk '{{print $1}}'); fi; \
                if [ -z \"$ACTUAL\" ]; then echo \"no sha256 tool available\" >&2; rm -f \"$TMP_ARCHIVE\"; rm -rf \"$TEMP\"; exit 1; fi; \
                VERIFIED=0; \
                case \"$NAME:$NODE_ARCH\" in \
                  node20:linux-arm64) PINNED=\"{pinned_n20_arm64}\";; \
                  node20:linux-x64) PINNED=\"{pinned_n20_x64}\";; \
                  node24:linux-arm64) PINNED=\"{pinned_n24_arm64}\";; \
                  node24:linux-x64) PINNED=\"{pinned_n24_x64}\";; \
                  node20:win-x64) PINNED=\"{pinned_n20_win_x64}\";; \
                  node24:win-x64) PINNED=\"{pinned_n24_win_x64}\";; \
                  *) PINNED=\"\";; \
                esac; \
                if [ -n \"$PINNED\" ]; then \
                  if [ \"$ACTUAL\" != \"$PINNED\" ]; then echo \"ERROR: $NAME $VERSION pinned SHA256 mismatch (got $ACTUAL expected $PINNED)\" >&2; rm -f \"$TMP_ARCHIVE\"; rm -rf \"$TEMP\"; exit 1; fi; \
                  VERIFIED=1; \
                fi; \
                SHASUMS_TMP=$(mktemp \"$RUNNER_EXTERNALS/.SHASUMS.XXXXXX\"); \
                if curl -fsSL -o \"$SHASUMS_TMP\" \"$SHASUMS_URL\" 2>/dev/null; then \
                  EXPECTED_SHASUMS=$(grep -F \" $ARCHIVE\" \"$SHASUMS_TMP\" | awk '{{print $1}}'); \
                  if [ -n \"$EXPECTED_SHASUMS\" ] && [ \"$ACTUAL\" != \"$EXPECTED_SHASUMS\" ]; then echo \"ERROR: $NAME $VERSION SHASUMS256.txt mismatch (got $ACTUAL expected $EXPECTED_SHASUMS)\" >&2; rm -f \"$TMP_ARCHIVE\" \"$SHASUMS_TMP\"; rm -rf \"$TEMP\"; exit 1; fi; \
                  if [ -n \"$EXPECTED_SHASUMS\" ]; then VERIFIED=1; fi; \
                  rm -f \"$SHASUMS_TMP\"; \
                else \
                  rm -f \"$SHASUMS_TMP\"; \
                fi; \
                if [ \"$VERIFIED\" != 1 ]; then \
                  echo \"ERROR: no trusted checksum found for $ARCHIVE (neither pinned SHA nor SHASUMS entry available)\" >&2; rm -f \"$TMP_ARCHIVE\"; rm -rf \"$TEMP\"; exit 1; \
                fi; \
                if ! tar -xzf \"$TMP_ARCHIVE\" --strip-components=1 -C \"$TEMP\"; then echo \"FAILED extracting $NAME\" >&2; rm -f \"$TMP_ARCHIVE\"; rm -rf \"$TEMP\"; exit 1; fi; \
                rm -f \"$TMP_ARCHIVE\"; \
               if [ ! -f \"$TEMP/bin/node\" ]; then \
                 echo \"ERROR: $NAME tarball missing bin/node\" >&2; \
                 rm -rf \"$TEMP\"; exit 1; \
               fi && \
               printf '{{\"runtime\":\"%s\",\"version\":\"%s\",\"platform\":\"%s\",\"archive_sha256\":\"%s\",\"source\":\"%s\"}}\\n' \"$NAME\" \"$VERSION_PLAIN\" \"$NODE_ARCH\" \"$ACTUAL\" \"$URL\" > \"$TEMP/preloop-node.json\" && \
               [ -d \"$DEST\" ] && rm -rf \"$DEST\"; \
               mv \"$TEMP\" \"$DEST\" && \
               echo \"$NAME $VERSION baked\" || \
               {{ rm -rf \"$TEMP\"; echo \"FAILED baking $NAME\" >&2; exit 1; }}; \
             done"
        ),
    ]]
    .into_iter()
    .collect()
}

/// Default unprivileged account the guest runner drops into, matching the
/// hosted `runner` user. [`RunnerPoolConfig::runner_user`] may override it.
pub const DEFAULT_RUNNER_USER: &str = "runner";
/// Default UID for [`DEFAULT_RUNNER_USER`], matching the hosted image.
pub const DEFAULT_RUNNER_UID: u32 = 1001;

/// Create the unprivileged runner account and hand it every path a job writes.
///
/// Part of [`base_install_script`] — and therefore of the environment
/// fingerprint — on purpose. Run as a separate post-bake `exec`, a change here
/// left the fingerprint untouched, so the pool adopted the previous golden and
/// silently served jobs an account the new code no longer matched. Keep every
/// step idempotent: the same script runs against an already-prepared rootfs.
///
/// The Rust homes are the subtle ones. `ToolchainLayer::Rust` installs them as
/// root at fixed system addresses (`/usr/local/rustup`, `/usr/local/cargo`)
/// that `guest_env_prefix` exports to every user, so without this ownership
/// the runner cannot write them and `rustup toolchain install` dies with
/// `could not create home directory`.
pub fn runner_account_script(user: &str, uid: u32) -> String {
    format!(
        "getent passwd {user} >/dev/null 2>&1 || useradd -m -u {uid} -s /bin/bash {user} 2>/dev/null; \
         printf '%s\\n' '{user} ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/preloop-{user} \
           && chmod 0440 /etc/sudoers.d/preloop-{user}; \
         mkdir -p /run/user/{uid} /opt/hostedtoolcache /usr/local/rustup /usr/local/cargo; \
         chown {uid}:{uid} /run/user/{uid} {root} 2>/dev/null; \
         chown -R {uid}:{uid} /usr/local/rustup /usr/local/cargo 2>/dev/null; \
         [ \"$(stat -c %a /opt/hostedtoolcache 2>/dev/null)\" = \"777\" ] || \
           chmod -R 777 /opt/hostedtoolcache 2>/dev/null; \
         grep -q AGENT_TOOLSDIRECTORY /etc/environment 2>/dev/null || \
           printf 'AGENT_TOOLSDIRECTORY=/opt/hostedtoolcache\\nRUNNER_TOOL_CACHE=/opt/hostedtoolcache\\n' >> /etc/environment; \
         getent group docker >/dev/null 2>&1 && usermod -aG docker {user} 2>/dev/null || true",
        root = RUNNER_ROOT
    )
}

/// Guest script that hands the runner account ownership of every path a job
/// writes, without touching the image's privilege policy.
///
/// Ownership only, deliberately: account creation, the `/etc/sudoers.d` rule,
/// and docker-group membership stay in [`runner_account_script`], which the
/// curated bake runs as part of building an image. Installing that policy on
/// an image which never had it would hand blanket root — and the container
/// daemon — to whatever a fork executes.
///
/// The script first decides whether anything is actually wrong (`needs`), so
/// an already-correct machine pays exactly one exec round trip. Only when work
/// is needed does it escalate — directly when the exec landed on root, else
/// through passwordless sudo — and an escalation that cannot run is reported
/// as a failure rather than masked: a machine whose `RUSTUP_HOME` resolves to
/// an unusable directory fails every rustup step at job time.
pub fn runner_ownership_reconcile_script(user: &str, uid: u32) -> String {
    let home = format!("/home/{user}");
    // The image's own toolchain homes. Official runner images install Rust
    // under the runner's `$HOME` (`~/.rustup` with `settings.toml`, `~/.cargo`
    // with the rustup shims) while preloop's contract exports
    // `RUSTUP_HOME=/usr/local/rustup` to every user — rustup obeys that
    // verbatim, so an image without those paths leaves `cargo` unable to
    // resolve a toolchain at all: the shim reads an empty home and dies with
    // `could not create home directory` (as the runner user) or `could not
    // choose a version of cargo to run`. Point the contract at the image's
    // home instead of refusing, which is what GitHub-hosted runners
    // effectively have: one toolchain every user resolves identically.
    let adopt_homes = format!(
        "if [ ! -f /usr/local/rustup/settings.toml ] && [ -f {home}/.rustup/settings.toml ]; then \
           rm -rf /usr/local/rustup; \
           ln -s {home}/.rustup /usr/local/rustup; \
         fi; \
         if [ ! -d /usr/local/cargo/bin ] && [ -d {home}/.cargo/bin ]; then \
           rm -rf /usr/local/cargo; \
           ln -s {home}/.cargo /usr/local/cargo; \
         fi"
    );
    // The privileged half, applied only after `needs=1`. `set -e` makes every
    // required operation fail the script: this is the ownership jobs depend
    // on, so a partial apply must not report success.
    let apply = format!(
        "set -e; \
         {adopt_homes}; \
         for d in /usr/local/rustup /usr/local/cargo; do \
           if [ -e \"$d\" ]; then chown -R {uid}:{uid} \"$d\"; fi; \
         done; \
         if [ -d /opt/hostedtoolcache ]; then \
           [ \"$(stat -c %a /opt/hostedtoolcache)\" = \"777\" ] || chmod -R 777 /opt/hostedtoolcache; \
         fi; \
         grep -q AGENT_TOOLSDIRECTORY /etc/environment || \
           printf 'AGENT_TOOLSDIRECTORY=/opt/hostedtoolcache\\nRUNNER_TOOL_CACHE=/opt/hostedtoolcache\\n' >> /etc/environment; \
         mkdir -p /run/user/{uid}; \
         chown {uid}:{uid} /run/user/{uid}"
    );
    use base64::Engine as _;
    let apply_b64 = base64::engine::general_purpose::STANDARD.encode(&apply);
    // Same predicate as `adopt_homes`, expressed as a `needs` signal so the
    // fast path does not skip a machine that still needs the link.
    let adopt_needs = format!(
        "if [ ! -f /usr/local/rustup/settings.toml ] && [ -f {home}/.rustup/settings.toml ]; then needs=1; fi; \
         if [ ! -d /usr/local/cargo/bin ] && [ -d {home}/.cargo/bin ]; then needs=1; fi"
    );
    format!(
        "needs=0; \
         {adopt_needs}; \
         for d in /usr/local/rustup /usr/local/cargo; do \
           if [ -e \"$d\" ]; then \
             if [ \"$(stat -L -c %u \"$d\" 2>/dev/null)\" != \"{uid}\" ]; then needs=1; \
             elif find \"$d/.\" ! -uid {uid} -print -quit 2>/dev/null | grep -q .; then needs=1; fi; \
           fi; \
         done; \
         if [ -d /opt/hostedtoolcache ]; then \
           [ \"$(stat -c %a /opt/hostedtoolcache 2>/dev/null)\" = \"777\" ] || needs=1; \
         fi; \
         grep -q AGENT_TOOLSDIRECTORY /etc/environment 2>/dev/null || needs=1; \
         if [ \"$needs\" = \"0\" ]; then echo 'runner ownership already reconciled'; exit 0; fi; \
         if [ \"$(id -u)\" -eq 0 ]; then printf %s '{apply_b64}' | base64 -d | sh; \
         else printf %s '{apply_b64}' | base64 -d | sudo -n sh; fi"
    )
}

/// Sysctls GitHub's hosted `ubuntu-24.04` image applies, with their values.
///
/// Provenance: `actions/runner-images` appends these to `/etc/sysctl.conf`
/// when the image is built —
/// `images/ubuntu/scripts/build/configure-environment.sh`
/// (<https://github.com/actions/runner-images/blob/5f7588b285eccc2edbeb1cd79d65ee0b577e4b4a/images/ubuntu/scripts/build/configure-environment.sh#L48-L55>)
/// — and a probe job on a hosted runner reads them back from `/proc/sys` as
/// the effective values (image `20260927.320.1` x64 / `20260927.135.1` arm64,
/// kernel `6.17.0-1022-azure`). `vm.max_map_count` matters to any mmap-heavy
/// workload (Redis and Valkey suites, Elasticsearch-style tooling); the
/// inotify limits are what the image raises for file watchers (`kind` scale,
/// bundlers, test runners).
///
/// Deliberately absent: `vm.mmap_rnd_bits` (kernel hardening the image also
/// writes, with no workflow-visible effect) and `vm.overcommit_memory`, which
/// the probe proves is `0` on GitHub-hosted runners too — the kernel default
/// the guest already has. Valkey's overcommit warning is therefore fidelity,
/// not a gap: it appears on both sides.
///
/// Each value is the matching `golden_sysctl_*` key in `official-image.toml`,
/// compiled in by `build.rs`, so the scheduled drift update moves the applied
/// value with the expectation; `guest_runtime_values_come_from_official_image_toml`
/// fails if a pair and its key disagree.
pub const GITHUB_GUEST_SYSCTLS: &[(&str, &str)] = &[
    ("vm.max_map_count", GOLDEN_SYSCTL_VM_MAX_MAP_COUNT),
    (
        "fs.inotify.max_user_watches",
        GOLDEN_SYSCTL_FS_INOTIFY_MAX_USER_WATCHES,
    ),
    (
        "fs.inotify.max_user_instances",
        GOLDEN_SYSCTL_FS_INOTIFY_MAX_USER_INSTANCES,
    ),
];

/// Bring a machine's sysctls in line with GitHub's hosted image.
///
/// The guest boots straight into the job workload — no init runs
/// `/etc/sysctl.d` or `/etc/sysctl.conf` — so an image-level sysctl file would
/// never take effect and every job saw raw kernel defaults (`vm.max_map_count`
/// 65530, inotify watches and instances a fraction of the hosted values).
/// Applied per machine by [`guest_hosted_runtime_init_script`], from the same
/// post-boot exec path as [`runner_ownership_reconcile_script`], so no golden
/// rebake is needed.
///
/// Idempotent: the first loop only compares `/proc/sys` values and exits
/// without writing when the machine already matches. A needed write escalates
/// the way the ownership reconciliation does (directly when the exec landed on
/// root, else through passwordless sudo, which the runner account has), then
/// re-reads every key it wrote, so a write the kernel rejected fails
/// provisioning instead of silently handing jobs a different environment than
/// GitHub's.
///
/// Only keys the guest kernel exposes *and* that differ are written. A kernel
/// that lacks one hosted key (say `fs.inotify.max_user_instances`) still gets
/// the others: both `sysctl -w` and a direct `/proc/sys` write fail on an
/// absent key, so handing the whole configured list to the privileged half
/// would discard an otherwise usable guest. Skipping is what GitHub does with
/// its own `/etc/sysctl.conf` lines for keys the kernel does not know.
/// Writes use `sysctl` when the image ships procps, else `/proc/sys` directly.
pub fn guest_sysctl_script() -> String {
    guest_sysctl_script_at("/proc/sys")
}

/// `root` is the sysctl tree the script reads and writes. Production passes
/// `/proc/sys`; the shell tests pass a scratch tree so the real script runs
/// end to end without touching the host kernel, the way
/// [`scope_rosetta_apt_sources`] stands in for `/etc/apt`.
fn guest_sysctl_script_at(root: &str) -> String {
    let pairs = GITHUB_GUEST_SYSCTLS
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(" ");
    let root = shell_quote(root);
    // The privileged half, base64'd so quoting survives both exec branches.
    // It only ever sees the pairs the pre-check selected: an absent key would
    // fail the write and, under `set -e`, take the whole apply with it.
    let apply = format!(
        "set -e; root={root}; \
         if command -v sysctl >/dev/null 2>&1; then \
           for pair in \"$@\"; do sysctl -w \"$pair\"; done; \
         else \
           for pair in \"$@\"; do \
             printf '%s' \"${{pair#*=}}\" > \"$root/$(printf '%s' \"${{pair%%=*}}\" | tr '.' '/')\"; \
           done; \
         fi"
    );
    use base64::Engine as _;
    let apply_b64 = base64::engine::general_purpose::STANDARD.encode(&apply);
    format!(
        "root={root}; plan=''; \
         for pair in {pairs}; do \
           path=\"$root/$(printf '%s' \"${{pair%%=*}}\" | tr '.' '/')\"; \
           if [ -e \"$path\" ] && [ \"$(cat \"$path\")\" != \"${{pair#*=}}\" ]; then plan=\"$plan $pair\"; fi; \
         done; \
         if [ -z \"$plan\" ]; then echo 'guest sysctls already match the hosted image'; exit 0; fi; \
         if [ \"$(id -u)\" -eq 0 ]; then printf %s '{apply_b64}' | base64 -d | sh -s -- $plan; \
         else printf %s '{apply_b64}' | base64 -d | sudo -n sh -s -- $plan; fi; \
         failed=''; \
         for pair in $plan; do \
           path=\"$root/$(printf '%s' \"${{pair%%=*}}\" | tr '.' '/')\"; \
           if [ -e \"$path\" ] && [ \"$(cat \"$path\")\" != \"${{pair#*=}}\" ]; then \
             failed=\"$failed $pair(got:$(cat \"$path\"))\"; \
           fi; \
         done; \
         if [ -n \"$failed\" ]; then \
           echo \"guest sysctls did not reach the hosted values:$failed (wanted: {pairs})\" >&2; \
           exit 1; \
         fi"
    )
}

/// Make a machine's own hostname resolve to an address of the machine.
///
/// The curated bake writes the *golden's* name into `/etc/hosts`
/// (`base_install_script`), but every fork boots under a new name, so the
/// guest's own hostname resolved nowhere and `sudo` printed
/// `sudo: unable to resolve host <name>` before every invocation — a line no
/// hosted-runner log contains. The fork cannot inherit the bake's entry
/// because the name is decided at fork time, so the mapping is applied per
/// machine by [`guest_hosted_runtime_init_script`].
///
/// Resolving is not enough. A guest can boot with a hosts file that maps its
/// name to an address the machine does **not** own — an AgentENV guest carried
/// `10.1.0.59 runnervm… runnervmvrwv9` while its interfaces held
/// `169.254.0.21` — and a check that only asks `getent hosts <name>` passes
/// there while every consumer of the name gets an unreachable address. A
/// hosted runner's own name resolves to an address of the VM itself: GitHub's
/// `/etc/hosts` maps `runnervm…` to the VM's interface address, and the
/// curated bake maps the golden's name to loopback. So the check is "does the
/// name resolve to loopback or a local-interface address"; a name mapped
/// anywhere else is rewritten to the bake's `127.0.0.1 <host>` convention,
/// with the stale mapping removed — a resolver answers a name with its *first*
/// match, so appending a second line would leave the stale one winning. Only
/// the machine's own name is removed from a mapping, never the whole line: an
/// entry that also carries `localhost` keeps it, and a line left with no name
/// at all is dropped.
///
/// Idempotent: a machine whose name already resolves locally — the baked
/// golden itself, a fork that ran this once, or a GitHub-shaped
/// interface-address mapping — exits without touching `/etc/hosts`. The
/// rewrite escalates through passwordless sudo when the exec lands on the
/// image user, and the resolution is re-checked, so a machine whose name still
/// does not resolve locally fails provisioning instead of handing every later
/// command a name that points somewhere else.
pub fn guest_hostname_script() -> String {
    guest_hostname_script_at("/etc/hosts")
}

/// `hosts` is the file the machine's own name is mapped in. Production passes
/// `/etc/hosts`; the shell test below passes a scratch file so the real script
/// runs end to end without touching the host's resolver, the way
/// [`scope_rosetta_apt_sources`] stands in for `/etc/apt`.
///
/// The name reaches `/etc/hosts` and the matching, so it is treated as
/// untrusted input: a hostname carrying shell or regex syntax fails
/// provisioning rather than being interpolated into a program. Every resolver
/// answer must be local — a resolver hands out the first answer, and a foreign
/// answer left anywhere in the list still lets a consumer pick it. awk
/// compares fields as strings, so no hostname byte is ever part of a program.
///
/// The check is required, not best-effort: a machine without `getent` (the
/// resolver lookup this runs on, present in the golden and in every Ubuntu
/// base) cannot show that its name resolves where it must, so it fails
/// provisioning with that reason instead of reporting an apply that never
/// happened.
///
/// No `#` comments inside the generated shell text: the statements are joined
/// with `;` and `\` continuations into one physical line, where a comment
/// would swallow whatever follows it on the line. The rationale lives here;
/// `every_generated_script_parses_as_posix_sh` parses the rendered script
/// with `sh -n`.
fn guest_hostname_script_at(hosts: &str) -> String {
    let hosts = shell_quote(hosts);
    format!(
        "command -v getent >/dev/null 2>&1 || { \
           echo 'getent is missing; the machine name cannot be checked against a local address' >&2; \
           exit 1; \
         }; \
         host=$(hostname 2>/dev/null || uname -n); \
         [ -n \"$host\" ] || exit 0; \
         case \"$host\" in \
           ''|*[!A-Za-z0-9._-]*) echo \"guest hostname is not a valid hostname\" >&2; exit 1 ;; \
         esac; \
         local_addrs() {{ \
           {{ printf '127.0.0.1\\n::1\\n'; hostname -I 2>/dev/null | tr ' ' '\\n'; \
              ip -o addr show 2>/dev/null | awk '{{print $4}}' | cut -d/ -f1; }} \
             | sed '/^$/d' | sort -u; \
         }}; \
         is_local() {{ \
           case \"$1\" in 127.*|::1) return 0 ;; esac; \
           for local in $(local_addrs); do [ \"$1\" = \"$local\" ] && return 0; done; \
           return 1; \
         }}; \
         name_addrs() {{ \
           addrs=$(getent ahosts \"$host\" 2>/dev/null | awk '{{print $1}}'); \
           [ -n \"$addrs\" ] || addrs=$(getent hosts \"$host\" 2>/dev/null | awk '{{print $1}}'); \
           printf '%s\\n' \"$addrs\"; \
         }}; \
         name_resolves_locally() {{ \
           resolved=$(name_addrs); \
           [ -n \"$resolved\" ] || return 1; \
           for addr in $resolved; do is_local \"$addr\" || return 1; done; \
           return 0; \
         }}; \
         name_resolves_locally && exit 0; \
         tmp=$(mktemp 2>/dev/null) || tmp=/tmp/.preloop-hosts.$$; \
         awk -v host=\"$host\" ' \
           /^[[:space:]]*#/ {{ print; next }} \
           {{ \
             n = split($0, field, /[[:space:]]+/); \
             out = \"\"; names = 0; \
             for (i = 1; i <= n; i++) {{ \
               if (i > 1 && field[i] == host) continue; \
               if (i > 1) names++; \
               out = (out == \"\" ? field[i] : out \" \" field[i]); \
             }} \
             if (names > 0) print out; \
           }}' {hosts} > \"$tmp\" 2>/dev/null || : > \"$tmp\"; \
         printf '127.0.0.1 %s\\n' \"$host\" >> \"$tmp\"; \
         if [ \"$(id -u)\" -eq 0 ]; then cat \"$tmp\" > {hosts}; \
         else cat \"$tmp\" | sudo -n tee {hosts} >/dev/null; fi; \
         rm -f \"$tmp\"; \
         name_resolves_locally || {{ \
           echo \"guest hostname $host does not resolve to an address of this machine after the hosts-file update\" >&2; \
           exit 1; \
         }}"
    )
}

/// The per-guest **hosted runtime init**: what the hosted image's own init
/// applies to a job's environment, applied by preloop instead.
///
/// On GitHub the VM boots with systemd/cloud-init and the runner is a systemd
/// service, so the image's `/etc/sysctl.d`, `/etc/security/limits.conf` and
/// hostname setup all apply before a step runs. A preloop guest enters the job
/// workload through the VM's exec channel instead: the smolvm guest's PID 1 is
/// `/run/smolvm/init`, with no systemd at all, and an AgentENV job arrives via
/// `aenv exec` off `envd`, outside systemd and PAM. The golden carries
/// GitHub's files, but nothing ever applies them. This is the per-guest half
/// of that gap — the kernel values and the machine's own name, applied once on
/// the post-boot exec path, before the runner registers.
///
/// The other half cannot live here: process limits are per process, so the
/// pair a step and a container inherit is applied where the workload is
/// launched ([`GUEST_STACK_ULIMIT`], [`GUEST_NOFILE_ULIMIT`], and the
/// re-raise of an engine chain inherited from a golden).
///
/// Each half runs in its own subshell so its early `exit 0` ("already at the
/// hosted values") cannot skip the other, and `set -e` turns either half's
/// failure into a provisioning failure rather than leaving the machine half
/// converted. Hostname resolution runs first because the sysctl half escalates
/// through passwordless sudo, and sudo resolves its hostname on every
/// invocation: with a stale mapping it would print
/// `sudo: unable to resolve host` into the apply's stderr before the name is
/// fixed.
pub fn guest_hosted_runtime_init_script() -> String {
    hosted_runtime_init_script_at("/etc/hosts", "/proc/sys")
}

/// `hosts` and `sysctl_root` are the scratch paths the shell tests substitute
/// for `/etc/hosts` and `/proc/sys`; production passes those.
fn hosted_runtime_init_script_at(hosts: &str, sysctl_root: &str) -> String {
    format!(
        "set -e; ( {} ); ( {} )",
        guest_hostname_script_at(hosts),
        guest_sysctl_script_at(sysctl_root)
    )
}

/// The guest bootstrap script, one shell round trip.
///
/// Every `exec` is a host process spawn plus a vsock round trip, and this runs
/// on the engine's start-up critical path. Exposed for the fidelity tests.
pub fn base_install_script() -> String {
    format!(
        "(find /usr/bin /usr/sbin /bin /sbin /etc -type f 2>/dev/null | \
            while IFS= read -r f; do chown 0:0 \"$f\" 2>/dev/null; done) || true; \
         chown 0:0 /etc/sudo.conf /etc/sudoers 2>/dev/null; \
         for f in /etc/sudoers.d/*; do [ -f \"$f\" ] && chown 0:0 \"$f\" 2>/dev/null; done; \
         chmod 0440 /etc/sudoers /etc/sudoers.d/* 2>/dev/null; \
         (for b in sudo su mount umount passwd chsh chfn newgrp gpasswd expiry chage wall write pkexec ping fusermount fusermount3; do \
            for p in /usr/bin/$b /bin/$b /usr/sbin/$b; do \
              if [ -f \"$p\" ]; then chown 0:0 \"$p\" 2>/dev/null; chmod u+s \"$p\" 2>/dev/null; fi; \
            done; \
          done; \
          for p in /usr/lib/openssh/ssh-keysign /usr/lib/dbus-1.0/dbus-daemon-launch-helper; do \
            if [ -f \"$p\" ]; then chown 0:0 \"$p\" 2>/dev/null; chmod u+s \"$p\" 2>/dev/null; fi; \
          done) && \
         apt-get update -qq && \
         . /etc/os-release && \
         case \"$VERSION_ID\" in \
           22.04) base_packages='{BASE_PACKAGES} {BASE_PACKAGES_22_04}' ;; \
           *) base_packages='{BASE_PACKAGES} {BASE_PACKAGES_24_04}' ;; \
         esac && \
         (echo \"### install hosted apt baseline\" >&2 && \
          if [ \"$VERSION_ID\" = 24.04 ] && \
             DEBIAN_FRONTEND=noninteractive \
             apt-get -s install -qq --no-install-recommends {base_packages_pinned} >/dev/null 2>&1; then \
            DEBIAN_FRONTEND=noninteractive \
            apt-get install -y -qq --no-install-recommends {base_packages_pinned}; \
          else \
            echo \"WARNING: exact hosted apt pins are unavailable; falling back to archive versions\" >&2; \
            DEBIAN_FRONTEND=noninteractive \
            apt-get install -y -qq --no-install-recommends $base_packages; \
          fi) \
         && printf '{LOOPBACK_HOSTS}' > /etc/hosts && \
         printf '127.0.0.1 %s\\n' \"$(hostname)\" >> /etc/hosts && \
         printf 'APT::Get::Assume-Yes \"true\";\\n' > /etc/apt/apt.conf.d/90assumeyes && \
         rm -f /usr/lib/python3*/EXTERNALLY-MANAGED && \
         arch=$(uname -m); \
         case \"$arch\" in x86_64) NODE_ARCH=x64 ;; aarch64|arm64) NODE_ARCH=arm64 ;; *) NODE_ARCH=x64 ;; esac; \
         case \"$NODE_ARCH\" in \
           x64) LFS_ARCH=amd64; DOCKER_STATIC_ARCH=x86_64; DOCKER_PLUGIN_ARCH=amd64; COMPOSE_ARCH=x86_64 ;; \
           *) LFS_ARCH=arm64; DOCKER_STATIC_ARCH=aarch64; DOCKER_PLUGIN_ARCH=arm64; COMPOSE_ARCH=aarch64 ;; \
         esac; \
         (echo \"### install hosted compiler matrix\" >&2 && \
          available_compiler_packages=''; \
          compiler_matrix_complete=1; \
          for package in {compiler_packages}; do \
            if DEBIAN_FRONTEND=noninteractive \
               apt-get -s install -qq --no-install-recommends \"$package\" >/dev/null 2>&1; then \
              available_compiler_packages=\"$available_compiler_packages $package\"; \
            else \
              compiler_matrix_complete=0; \
              echo \"compiler package unavailable: $package\" >&2; \
            fi; \
          done; \
          if [ -n \"$available_compiler_packages\" ]; then \
            DEBIAN_FRONTEND=noninteractive \
            apt-get install -y -qq --no-install-recommends $available_compiler_packages || exit 1; \
          fi; \
          if [ \"$compiler_matrix_complete\" = 1 ]; then \
            clang-16 --version | head -1 | grep -F '{CLANG_16_VERSION}' && \
            clang-17 --version | head -1 | grep -F '{CLANG_17_VERSION}' && \
            clang-18 --version | head -1 | grep -F '{CLANG_18_VERSION}' && \
            test \"$(gcc-12 -dumpfullversion)\" = '{GCC_12_VERSION}' && \
            test \"$(gcc-13 -dumpfullversion)\" = '{GCC_13_VERSION}' && \
            test \"$(gcc-14 -dumpfullversion)\" = '{GCC_14_VERSION}' || exit 1; \
          else \
            echo \"WARNING: hosted compiler matrix is incomplete in this Ubuntu archive; adding the archive-default compiler toolchain\" >&2; \
            DEBIAN_FRONTEND=noninteractive \
            apt-get install -y -qq --no-install-recommends clang clang-format clang-tidy gcc g++ gfortran || exit 1; \
          fi; \
          for version in {CLANG_VERSIONS}; do \
            if [ -x /usr/bin/clang++-$version ]; then \
              update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-$version 100 || true; \
            fi; \
            if [ -x /usr/bin/clang-$version ]; then \
              update-alternatives --install /usr/bin/clang clang /usr/bin/clang-$version 100 || true; \
            fi; \
            if [ -x /usr/bin/clang-format-$version ]; then \
              update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-$version 100 || true; \
            fi; \
            if [ -x /usr/bin/clang-tidy-$version ]; then \
              update-alternatives --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-$version 100 || true; \
            fi; \
            if [ -x /usr/bin/run-clang-tidy-$version ]; then \
              update-alternatives --install /usr/bin/run-clang-tidy run-clang-tidy /usr/bin/run-clang-tidy-$version 100 || true; \
            fi; \
          done; \
          for tool in clang clang++ clang-format clang-tidy run-clang-tidy; do \
            if [ -x \"/usr/bin/$tool-{CLANG_DEFAULT_VERSION}\" ]; then \
              update-alternatives --set \"$tool\" \"/usr/bin/$tool-{CLANG_DEFAULT_VERSION}\" || exit 1; \
            fi; \
          done) && \
         (echo \"### fetch system node v{BASE_NODE_VERSION}\" >&2 && \
          curl -fsSL \"https://nodejs.org/dist/v{BASE_NODE_VERSION}/node-v{BASE_NODE_VERSION}-linux-$NODE_ARCH.tar.gz\" \
            | tar -xz --strip-components=1 -C /usr/local) && \
         (install -m 0755 -d /etc/apt/keyrings && \
         (echo \"### fetch docker gpg\" >&2 && \
          curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc && \
          echo \"deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable\" > /etc/apt/sources.list.d/docker.list && \
          apt-get update -qq && \
          DEBIAN_FRONTEND=noninteractive \
          apt-get install -y -qq {docker_packages} && \
          (echo \"### install gh cli\" >&2 && \
           curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg -o /usr/share/keyrings/githubcli-archive-keyring.gpg && \
           echo \"deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main\" > /etc/apt/sources.list.d/github-cli.list && \
          apt-get update -qq && \
          DEBIAN_FRONTEND=noninteractive \
           apt-get install -y -qq gh && \
           gh --version | head -1) && \
          echo \"### overlay docker v{DOCKER_VERSION}\" >&2 && \
          rm -rf /tmp/docker-static && mkdir -p /tmp/docker-static && \
          curl -fsSL \"https://download.docker.com/linux/static/stable/$DOCKER_STATIC_ARCH/docker-{DOCKER_VERSION}.tgz\" \
            | tar -xz -C /tmp/docker-static && \
          install -m 0755 /tmp/docker-static/docker/* /usr/local/bin/ && \
          rm -rf /tmp/docker-static && \
          install -m 0755 -d /usr/local/lib/docker/cli-plugins && \
          curl -fsSL \"https://github.com/docker/buildx/releases/download/v{DOCKER_BUILDX_VERSION}/buildx-v{DOCKER_BUILDX_VERSION}.linux-$DOCKER_PLUGIN_ARCH\" \
            -o /usr/local/lib/docker/cli-plugins/docker-buildx && \
          curl -fsSL \"https://github.com/docker/compose/releases/download/v{DOCKER_COMPOSE_VERSION}/docker-compose-linux-$COMPOSE_ARCH\" \
            -o /usr/local/lib/docker/cli-plugins/docker-compose && \
          chmod 0755 /usr/local/lib/docker/cli-plugins/docker-buildx /usr/local/lib/docker/cli-plugins/docker-compose && \
          docker --version | grep -F '{DOCKER_VERSION}' && \
          dockerd --version | grep -F '{DOCKER_VERSION}' && \
          docker buildx version | grep -F 'v{DOCKER_BUILDX_VERSION}' && \
          docker compose version --short | grep -F '{DOCKER_COMPOSE_VERSION}' && \
          mkdir -p {DOCKER_DATA_ROOT} /etc/docker && \
          printf '{{\"data-root\":\"{DOCKER_DATA_ROOT}\"}}\\n' > /etc/docker/daemon.json)) && \
         (echo \"### fetch cargo-shear\" >&2 && \
          curl -sSL https://github.com/Boshen/cargo-shear/releases/download/v{CARGO_SHEAR_VERSION}/cargo-shear-$(uname -m)-unknown-linux-musl.tar.gz 2>/dev/null | tar -xz -C /usr/local/bin 2>/dev/null || true) && \
         (echo \"### bake git v{GIT_VERSION}\" >&2 && \
          apt-get install -y -qq --no-install-recommends libcurl4-openssl-dev zlib1g-dev gettext libexpat-dev && \
          curl -fsSL https://github.com/git/git/archive/refs/tags/v{GIT_VERSION}.tar.gz | tar -xz -C /tmp && \
          (cd /tmp/git-{GIT_VERSION} && make -s prefix=/usr all && make -s prefix=/usr install) && \
          rm -rf /tmp/git-{GIT_VERSION} && \
          echo \"### bake git-lfs v{GIT_LFS_VERSION}\" >&2 && \
          curl -fsSL https://github.com/git-lfs/git-lfs/releases/download/v{GIT_LFS_VERSION}/git-lfs-linux-$LFS_ARCH-v{GIT_LFS_VERSION}.tar.gz | tar -xz -C /tmp && \
          /tmp/git-lfs-{GIT_LFS_VERSION}/install.sh && rm -rf /tmp/git-lfs-{GIT_LFS_VERSION}) && \
         (mkdir -p /usr/local/share && \
          echo \"### bake nvm v{NVM_VERSION}\" >&2 && \
          curl -fsSL https://github.com/nvm-sh/nvm/archive/refs/tags/v{NVM_VERSION}.tar.gz | tar -xz -C /usr/local/share && \
          mv /usr/local/share/nvm-{NVM_VERSION} /usr/local/share/nvm && \
          printf 'export NVM_DIR=/usr/local/share/nvm\\n[ -s \"$NVM_DIR/nvm.sh\" ] && \\. \"$NVM_DIR/nvm.sh\"\\n' > /etc/profile.d/nvm.sh) && \
         echo \"### bake yarn v{YARN_VERSION}\" >&2 && \
         npm install -g yarn@{YARN_VERSION} && \
         install -d -m 0777 /opt/hostedtoolcache && \
         printf 'AGENT_TOOLSDIRECTORY=/opt/hostedtoolcache\\nRUNNER_TOOL_CACHE=/opt/hostedtoolcache\\n' >> /etc/environment && \
         (useradd -m -u 1000 -s /bin/bash ubuntu 2>/dev/null || true) && \
         ({runner_account}) && \
         apt-get clean && \
         rm -rf /usr/share/doc/* /usr/share/man/* /usr/share/info/*",
        runner_account = runner_account_script(DEFAULT_RUNNER_USER, DEFAULT_RUNNER_UID),
        docker_packages = docker_apt_packages(),
        compiler_packages = compiler_apt_packages(),
        base_packages_pinned = base_packages_pinned()
    )
}

fn base_install_commands() -> Vec<Vec<String>> {
    [vec![
        "sh".to_owned(),
        "-c".to_owned(),
        base_install_script(),
    ]]
    .into_iter()
    .collect()
}

/// Start the container engine, if one is installed.
///
/// Runs per machine rather than in the golden: a daemon captured mid-flight by
/// a fork would wake up with stale state and a socket it does not own. It runs
/// as a background task from provisioning (overlapping runner registration),
/// never gating readiness: the 5-15 s cold boot sits off the starting path,
/// and the worker waits for the daemon before container setup, so neither
/// declared (`container:`/`services:`) nor ad-hoc (`docker run` steps)
/// container use can observe a half-started engine.
///
/// Never fatal. A pool without a working container engine still runs every job
/// that does not use `container:` or `services:`.
///
/// The daemon is started with the hosted process limits ([`GUEST_STACK_ULIMIT`]
/// and [`GUEST_NOFILE_ULIMIT`], the same raise the runner wrapper applies): a
/// container's processes inherit the daemon's limits, so without it every
/// container step would run on the VM init's half-sized stack and a
/// descriptor limit below the hosted 65536 while the same step on GitHub runs
/// on 16 MiB and 65536 open files.
/// A daemon the fork inherited from its golden instead — preload leaves the
/// chain running so forks never restart it — carries whatever limits it was
/// started with, so the command re-raises that live chain (dockerd and
/// containerd) in place before exiting, covering goldens baked before either
/// raise existed: both the stack pair and the descriptor pair, since a
/// container's processes inherit whichever the chain still carries.
///
/// Readiness is `docker info` rather than `pgrep dockerd`, because a forked VM
/// can carry a `[dockerd] <defunct>` entry from its golden: a name match sees
/// the zombie, concludes Docker is up, and leaves the runner with no daemon.
/// A stale `/var/run/docker.pid` naming that same pid blocks startup outright,
/// and is only removed once `docker info` has failed so it is stale by
/// definition.
///
/// The storage driver is probed, never assumed: the golden's daemon auto-selects
/// `fuse-overlayfs`, which cannot mount inside the smolvm kernel (no `/dev/fuse`,
/// and the bundled fuse-overlayfs rejects the `lazytime` option), so every
/// `docker run` in a container job dies with "fuse: device not found". We try a
/// real overlay mount first and fall back to `vfs`; if the daemon then refuses
/// the previous driver's data, the docker data-root is reset and dockerd is
/// retried once (images re-pull from the registry).
///
/// `raise_engine_chain` raises the live chain in place — `prlimit` reaches a
/// process a child shell's `ulimit` cannot, and restarting the chain inside a
/// fork leaves the half-torn-down containerd socket the preload comment warns
/// about. Only a chain still below the hosted soft limit is touched, so a
/// daemon (custom base) already at or above it keeps its own; raising a hard
/// limit needs root, which both launch branches run as. A chain still below
/// the hosted limits that could not be raised would hand containers the wrong
/// pair, so the launch fails loudly instead of reporting success.
///
/// The krunfw guest kernel has fuse built in, but the VM boots `/dev` as a
/// plain tmpfs with only the image's baked nodes, so `/dev/fuse` is missing
/// and fuse-overlayfs (dockerd's fallback when its overlay probe fails) dies
/// with "fuse: device not found". The script creates the node when the kernel
/// supports fuse; dockerd then auto-picks fuse-overlayfs (CoW) on kernels
/// whose overlay probe fails, and overlay2 on stock kernels where it
/// succeeds. Where overlay is unusable (the krunfw kernel rejects the probe
/// mount with EINVAL), `vfs` is forced only when fuse is unavailable too —
/// otherwise fuse-overlayfs auto-detects and works.
///
/// The launch keeps its exit status via [`run_as_root_or_sudo_strict`], so
/// failed limit raises, readiness, or passwordless-sudo setup reaches the caller.
///
/// The body lives in `scripts/docker-start.sh`, embedded and rendered with
/// generated image values; its rendered form is parsed by
/// `every_generated_script_parses_as_posix_sh`, while a placeholder test keeps
/// unsubstituted tokens off guests.
fn docker_start_command() -> Vec<String> {
    vec![
        "sh".to_owned(),
        "-c".to_owned(),
        run_as_root_or_sudo_strict(&render_docker_start_script()),
    ]
}

/// The guest container-engine bootstrap, verbatim: see
/// [`docker_start_command`] for what it does.
const DOCKER_START_SCRIPT: &str = include_str!("../../../scripts/docker-start.sh");

/// Substitute the `@@NAME@@` tokens with the pins `build.rs` compiles in from
/// `official-image.toml`.
///
/// Not `format!`: the script is brace-heavy shell, and escaping every brace is
/// the assembly this file exists to avoid.
fn render_docker_start_script() -> String {
    let mut script = DOCKER_START_SCRIPT.to_owned();
    for (token, value) in [
        ("@@GUEST_STACK_ULIMIT@@", GUEST_STACK_ULIMIT),
        ("@@GUEST_NOFILE_ULIMIT@@", GUEST_NOFILE_ULIMIT),
        ("@@DOCKER_DATA_ROOT@@", DOCKER_DATA_ROOT),
        ("@@GOLDEN_STACK_SOFT_BYTES@@", GOLDEN_STACK_SOFT_BYTES),
        ("@@GOLDEN_STACK_PRLIMIT@@", GOLDEN_STACK_PRLIMIT),
        ("@@GOLDEN_RLIMIT_NOFILE_SOFT@@", GOLDEN_RLIMIT_NOFILE_SOFT),
        ("@@GOLDEN_NOFILE_PRLIMIT@@", GOLDEN_NOFILE_PRLIMIT),
    ] {
        script = script.replace(token, value);
    }
    script
}

/// How long to wait for a freshly started guest to accept commands.
const GUEST_READY_TIMEOUT: Duration = Duration::from_secs(30);
/// How long to wait between live-clone drain probes before re-arming a spent
/// golden fork base. Bounded retries; the probe loop is exercised by tests
/// under paused Tokio time, so this is the only knob the delay is tied to.
const GOLDEN_DRAIN_PROBE_DELAY: Duration = Duration::from_secs(10);
/// Ceiling for the drain-probe backoff: probes start at
/// `GOLDEN_DRAIN_PROBE_DELAY` and double up to this cap, so a long-running
/// job's clone is re-checked every minute rather than every ten seconds.
const GOLDEN_DRAIN_PROBE_MAX: Duration = Duration::from_secs(60);
/// Total wall-clock budget for waiting on live clones to drain before
/// falling back to independent OCI creation. The fork path is orders of
/// magnitude faster than direct creation, so this is generous; a clone that
/// has not exited within it is unlikely to do so soon.
const GOLDEN_DRAIN_BUDGET: Duration = Duration::from_secs(300);
/// Gap between guest readiness probes.
const GUEST_READY_POLL: Duration = Duration::from_millis(25);

/// Block until the guest agent executes a trivial command.
///
/// `machine start` returns once the agent marker appears, but the guest can
/// still refuse the first `exec`. Polling costs one round trip when the guest
/// is already up, where a fixed sleep charged every boot for the worst case.
async fn await_guest_ready<P: VmProvider>(
    provider: &P,
    name: &MachineName,
) -> Result<(), OrchestratorError> {
    let deadline = tokio::time::Instant::now() + GUEST_READY_TIMEOUT;
    let probe = ["true".to_owned()];
    loop {
        match provider.exec(name, &probe).await {
            Ok(_) => return Ok(()),
            Err(error) if tokio::time::Instant::now() >= deadline => {
                return Err(OrchestratorError::from(error));
            }
            Err(_) => tokio::time::sleep(GUEST_READY_POLL).await,
        }
    }
}

/// Restore apt's package indices when the image shipped without them.
///
/// Hosted images keep populated lists, so real workflows run
/// `sudo apt-get install <pkg>` with no `apt-get update` first (uv's musl cell
/// installs `musl-tools` that way). Every pack published while the baseline
/// script ended in `rm -rf /var/lib/apt/lists/*` boots without them, and each
/// of those steps fails with `E: Unable to locate package`. Cheap to check,
/// and a no-op on an image that has them.
///
/// Hard-bounded: a fork of a packed golden can inherit a held apt lock from the
/// frozen image, and `apt-get update` then waits forever — which would block
/// provisioning, not just the refresh. A missed refresh costs a workflow one
/// `apt-get update`; a hung one costs the whole pool.
fn apt_lists_refresh_command() -> Vec<String> {
    vec![
        "sh".to_owned(),
        "-c".to_owned(),
        "[ -n \"$(find /var/lib/apt/lists -name '*_Packages*' -print -quit 2>/dev/null)\" ] \
         || timeout 120 apt-get -o DPkg::Lock::Timeout=10 update -qq || true"
            .to_owned(),
    ]
}

async fn install_base_dependencies<P: VmProvider>(
    provider: &P,
    name: &MachineName,
) -> Result<(), OrchestratorError> {
    for command in base_install_commands() {
        let output = provider.exec(name, &command).await?;
        if output.exit_code != 0 {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(OrchestratorError::Config(format!(
                "base package install failed (exit {}): {}",
                output.exit_code,
                stderr.lines().last().unwrap_or("unknown error")
            )));
        }
    }
    // Node externals are no longer baked: they arrive via the read-only
    // host mount (`ensure_host_externals` + `runner_volumes`), which keeps
    // the golden pack and every machine image lean.
    Ok(())
}

/// Where a baked golden records its provenance. Root-owned, so writing it
/// needs the privileged hop — see [`write_bake_manifest`].
const BAKE_MANIFEST_PATH: &str = "/etc/preloop-bake.json";

/// Record what a golden actually baked, so provenance is inspectable
/// instead of reconstructed.
///
/// The resolved versions are the point: channels (`stable`, `22`, `lts/*`,
/// `go 1.24` minimums) resolve at bake time, and the manifest captures what
/// they resolved to. `/etc/preloop-bake.json` in any fork answers "what is
/// in this environment?" without re-deriving it.
async fn write_bake_manifest<P: VmProvider>(
    provider: &P,
    name: &MachineName,
    env_spec: &EnvironmentSpec,
) -> Result<(), OrchestratorError> {
    let probe = [
        "sh".to_owned(),
        "-c".to_owned(),
        "for cmd in node npm python3 docker git rustc cargo go cargo-shear; do \
           printf '%s=%s\\n' \"$cmd\" \"$($cmd --version 2>/dev/null | head -n1 || echo missing)\"; \
         done; \
         printf 'packages=%s\\n' \"$(dpkg-query -W -f={{Package}} | wc -l)\"; \
         printf 'built_at=%s\\n' \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\""
            .to_owned(),
    ];
    let output = provider.exec(name, &probe).await?;
    let mut versions = serde_json::Map::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if let Some((key, value)) = line.split_once('=') {
            versions.insert(key.to_owned(), serde_json::Value::String(value.to_owned()));
        }
    }
    let manifest = serde_json::json!({
        "base": env_spec.base,
        "toolchains": env_spec.toolchains,
        "versions": versions,
        "base_node": BASE_NODE_VERSION,
        "cargo_shear": CARGO_SHEAR_VERSION,
        // Derived from the same generated pins the install path uses, so a
        // version bump can never install one version and record another.
        "node_externals": [
            format!("node20 {NODE20_EXTERNALS_VERSION}"),
            format!("node24 {NODE24_EXTERNALS_VERSION}"),
        ],
        "preloop": env!("CARGO_PKG_VERSION"),
    });
    let json =
        serde_json::to_string(&manifest).expect("bake manifest is a fixed string-only structure");
    // `/etc` is root-owned and the bake runs as the unprivileged runner
    // account, so a plain redirect fails with `Permission denied` on every
    // bake: the golden shipped without its provenance. Take the same
    // root-or-sudo hop as the other privileged bake steps.
    let write = format!(
        "printf '%s' '{}' > {BAKE_MANIFEST_PATH}",
        json.replace('\'', "'\\''")
    );
    provider
        .exec(
            name,
            &[
                "sh".to_owned(),
                "-c".to_owned(),
                run_as_root_or_sudo(&write),
            ],
        )
        .await?;
    // `run_as_root_or_sudo` ends in `|| true`, so a refused sudo still exits
    // 0: the exec result cannot prove the write happened. Ask the guest.
    let verify = provider
        .exec(
            name,
            &[
                "sh".to_owned(),
                "-c".to_owned(),
                format!("test -s {BAKE_MANIFEST_PATH}"),
            ],
        )
        .await?;
    if verify.exit_code != 0 {
        return Err(OrchestratorError::Config(format!(
            "bake manifest missing after write (exit {}): {}",
            verify.exit_code,
            String::from_utf8_lossy(&verify.stderr)
                .lines()
                .last()
                .unwrap_or("no stderr")
        )));
    }
    info!(
        machine = name.as_str(),
        "bake manifest written to {BAKE_MANIFEST_PATH}"
    );
    Ok(())
}

/// PATH the guest runner process exports to every step.
/// Hosted images carry the toolchain bin directories on the runner's own PATH,
/// which is what makes `cargo install`-style actions work: `taiki-e/install-action`
/// drops `cargo-hack` in `$CARGO_HOME/bin` and the next step runs `cargo hack`.
/// dtolnay/rust-toolchain only appends that directory to `$GITHUB_PATH` when it
/// has to install rustup itself, so on an image that already has rustup — ours,
/// and GitHub's — the directory is on PATH or the tool is simply unreachable.
///
/// The cargo bin dir is the fixed system address `/usr/local/cargo/bin`,
/// matching the exported `CARGO_HOME` (see `guest_env_prefix`): it is
/// identical for root and switched runners by construction. A per-user
/// `$HOME/.cargo/bin` here would reintroduce the EACCES trap the homes fix
/// removes — `/root` is 0700, so exporting `/root/.cargo/bin` to an
/// unprivileged runner makes every tool lookup fail statting it
/// (nodejs/ci: `EACCES: permission denied, stat '/root/.cargo/bin/git'`).
/// Absent directories cost nothing.
pub fn guest_runner_path(_config: &RunnerPoolConfig) -> String {
    format!(
        "{GUEST_CARGO_HOME}/bin:/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:\
          /usr/sbin:/usr/bin:/sbin:/bin"
    )
}

/// `env` prefix for guest runner invocations, empty when nothing needs setting.
///
/// Control-socket routing and failure-marker debugging are independent
/// features: a pool can debug failed jobs without a mounted control socket and
/// vice versa, so neither may gate the other.
fn guest_env_prefix(config: &RunnerPoolConfig, name: &MachineName) -> Vec<String> {
    let mut env = Vec::new();
    env.push(format!("PATH={}", guest_runner_path(config)));
    // The guest needs its own VM name so a debug session can tell a controller
    // which machine to open a shell into. Nothing else in the guest knows it.
    env.push(format!("PRELOOP_MACHINE_NAME={}", name.as_str()));
    if config.control_socket.is_some() {
        env.push(format!(
            "PRELOOP_CONTROL_ORIGIN={}",
            config
                .control_origin
                .as_deref()
                .unwrap_or(&config.server_url)
                .trim_end_matches('/')
        ));
        env.push(format!("PRELOOP_CONTROL_SOCKET={GUEST_CONTROL_SOCKET}"));
    } else if let Some(upstream) = &config.control_upstream {
        env.push(format!(
            "PRELOOP_CONTROL_ORIGIN={}",
            config
                .control_origin
                .as_deref()
                .unwrap_or(&config.server_url)
                .trim_end_matches('/')
        ));
        env.push(format!("PRELOOP_CONTROL_UPSTREAM={upstream}"));
    }
    if config.debug_dir.is_some() {
        env.push(format!("PRELOOP_FAILURE_MARKER={GUEST_FAILURE_MARKER}"));
        env.push(format!("PRELOOP_PAUSE_MARKER={GUEST_PAUSE_MARKER}"));
    }
    // Rust toolchain homes. rustup resolves toolchains under RUSTUP_HOME and
    // shims under CARGO_HOME, both defaulting to the *calling* user's $HOME.
    // The bake installs as root while job steps run as the unprivileged
    // runner user, so a $HOME-derived location is invisible across that
    // boundary (/root is 0700). The bake therefore installs to these fixed
    // system addresses (see ToolchainLayer::Rust install_commands), and they
    // are exported here so every user resolves the identical toolchain.
    // Order is irrelevant (env entries are independent); they sit last so
    // the historical PATH/MACHINE_NAME-first prefix is undisturbed.
    env.push(format!("RUSTUP_HOME={GUEST_RUSTUP_HOME}"));
    env.push(format!("CARGO_HOME={GUEST_CARGO_HOME}"));
    if !env.is_empty() {
        env.insert(0, "/usr/bin/env".to_owned());
    }
    env
}

/// Local ephemeral-runner pool configuration.
#[derive(Debug, Clone)]
pub struct RunnerPoolConfig {
    /// Number of runners polling concurrently.
    pub size: usize,
    /// Use a forkable golden VM as a fork base for instant runner creation.
    /// When enabled, a single "golden" VM boots once and each runner slot
    /// clones from it with CoW memory and disks.
    pub use_fork: bool,
    /// Create runners from the prepared packed artifact instead of the base
    /// OCI image. The caller must provide a SmolVM build that preserves
    /// explicitly supplied socket mappings for packed-machine creation.
    pub use_packed_artifact: bool,
    /// Prefix used for owned SmolVM names.
    pub name_prefix: String,
    /// Base OCI image used for one-time tool installation.
    pub base_image: String,
    /// Optional workspace path for environment detection from version files.
    pub workspace: Option<PathBuf>,
    /// Host path stem for the reusable packed VM artifact.
    pub artifact_stem: PathBuf,
    /// Preloop release whose architecture-specific golden asset should be used.
    ///
    /// This comes from the embedding CLI rather than this crate's package
    /// version: workspace crates are versioned independently.
    pub release_version: String,
    /// Host directory containing the Linux `preloop-runner` executable.
    pub runner_bundle: PathBuf,
    /// Host directory holding the Node externals shared with every VM.
    ///
    /// Populated once on the host (`node20`/`node24` downloaded from
    /// nodejs.org), then mounted read-only into every machine at the runner
    /// root's `externals`. Keeps the golden pack small: externals are never
    /// baked into a machine image or downloaded per runner.
    pub externals_dir: PathBuf,
    /// Runner executable filename within `runner_bundle`.
    pub runner_binary_name: String,
    /// Guest-visible control-plane URL.
    pub server_url: String,
    /// Origin used in advertised job URLs routed through `control_socket`.
    /// Registration may use a different guest-reachable address.
    pub control_origin: Option<String>,
    /// Host Unix socket used for runner control-plane traffic.
    pub control_socket: Option<PathBuf>,
    /// TCP address the guest control bridge forwards to when the socket is
    /// unavailable. The bridge binds `control_origin` inside the VM and
    /// proxies accepted connections to this address over virtio-net TCP.
    pub control_upstream: Option<String>,
    /// Guest DNS resolver override (smolvm `--dns`), for networks that
    /// filter smolvm's default public resolvers.
    pub dns: Option<String>,
    /// Host environment variable containing the registration credential.
    pub registration_token_env: String,
    /// Runner labels advertised to the scheduler.
    pub labels: Vec<String>,
    /// vCPUs per runner.
    pub cpus: u16,
    /// Memory per runner in MiB.
    pub memory_mib: u32,
    /// Storage per runner in GiB.
    pub storage_gib: u32,
    /// Root overlay size per runner in GiB; `None` keeps the provider default.
    pub overlay_gib: Option<u32>,
    /// Free-space reserve that gates starting a job VM (fork or create).
    ///
    /// Production builds this from `PRELOOP_RUNNER_MIN_FREE_DISK_GB` (default
    /// 20 GiB, `0` disables); a fork or create below the reserve waits for
    /// space instead of filling the host or failing the job.
    pub job_vm_disk: JobVmDiskReserve,
    /// Directory for debug session markers (e.g. `~/.preloop/state/debug`).
    ///
    /// When set, a runner whose job requested `preserve_on_failure` and then
    /// failed is held open for interactive debugging. Whether any individual
    /// job opts in is decided per run by the control plane, not here.
    pub debug_dir: Option<PathBuf>,
    /// Directory used to hand pre-generated runner keypairs to `configure`.
    ///
    /// Unset means every runner generates its own keypair inside its guest.
    pub runner_key_dir: Option<PathBuf>,
    /// Jobs the control plane still has queued after the most recent claim.
    ///
    /// Unset makes a slot fall back to "build a replacement only once the pool
    /// is empty", which underprovisions whenever a workflow fans out wider
    /// than the pool.
    pub pending_jobs: Option<Arc<AtomicUsize>>,
    /// `runs-on` labels of the job at the front of the dispatch queue,
    /// refreshed after each claim. The pool reads them to select the correct
    /// base-image golden before provisioning.
    /// Container images pulled into every golden at build time.
    ///
    /// Deliberately not part of the environment fingerprint -- see
    /// [`crate::environment::scan_workflow_images`].
    pub preload_images: Vec<String>,
    /// Run the guest runner under this account instead of root, matching the
    /// GitHub-hosted runner user-session contract (steps see USER/LOGNAME/
    /// XDG_RUNTIME_DIR of a dedicated user, not root). The control plane and
    /// provisioning stay root; only the runner process drops privileges.
    /// `Some("root")` restores the old behavior; None disables switching.
    pub runner_user: Option<String>,
    /// UID for [`RunnerPoolConfig::runner_user`] (default 1001, matching the
    /// hosted `runner` account).
    pub runner_uid: Option<u32>,
    pub next_job_runs_on: Option<Arc<std::sync::RwLock<Vec<String>>>>,
    /// One-time provision-token map shared with the control plane. When set,
    /// every provisioning event registers a token here and injects it into
    /// the guest's `configure` call; the control plane trusts only
    /// registrations presenting a match, which is what authorizes it to bind
    /// a queued job to a specific machine's runner identity.
    pub pending_registrations:
        Option<Arc<std::sync::RwLock<std::collections::BTreeMap<String, std::time::SystemTime>>>>,
    /// Signal raised while the pool is still preparing its immutable
    /// machine image (artifact download or build, golden prep) and cannot
    /// register a runner yet. The control plane reads it to pause the
    /// queued-job starvation clock during the warm; it is cleared before
    /// the pool serves its first job.
    pub preparing_signal: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// Consolidated pool handle (replaces the four ad-hoc Option<Arc<…>> fields above).
    /// When `Some`, the pool updates it and the server sampler reads it.
    /// Retain the legacy fields for now for backwards compatibility; new code
    /// should read/write `pool_status`.
    pub pool_status: Option<Arc<preloop_observability::status::PoolStatus>>,
    /// Observability handle whose VM registry tracks this pool's live
    /// machines. When `Some`, the pool registers created/forked VMs and
    /// deregisters them at teardown; `None` (tests, legacy callers) skips
    /// the wiring.
    pub observability: Option<preloop_observability::Observability>,
}

/// Cache of environment-specific golden VMs.
pub(crate) struct GoldenRegistry {
    goldens: RwLock<HashMap<String, MachineName>>,
    /// Per-fingerprint construction locks. A single build_lock used to be
    /// held across the whole bake, so one environment's golden build parked
    /// every other slot on the mutex — silently, with no logs — freezing the
    /// pool until a restart. Distinct fingerprints now build concurrently;
    /// the same fingerprint is still serialized (the second caller would
    /// otherwise delete the first's half-built VM, since
    /// `prepare_golden_for_env` removes any existing machine of that name).
    build_locks: RwLock<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    name_prefix: String,
    /// Set when the startup packed-golden import failed. In that case the
    /// pool must create runners directly from their requested base image
    /// rather than attempting a second environment-golden bake.
    packed_unavailable: AtomicBool,
}

impl GoldenRegistry {
    pub fn new(name_prefix: String) -> Self {
        Self {
            goldens: RwLock::new(HashMap::new()),
            build_locks: RwLock::new(HashMap::new()),
            name_prefix,
            packed_unavailable: AtomicBool::new(false),
        }
    }

    /// Return the name prefix used for golden VM names.
    pub fn name_prefix(&self) -> &str {
        &self.name_prefix
    }

    /// Disable packed-golden and environment-golden preparation after the
    /// startup packed import fails. Direct per-runner creation is the only
    /// honest fallback: a second environment golden would repeat the same
    /// disk-heavy unpack under a different name.
    pub fn disable_packed(&self) {
        self.packed_unavailable.store(true, Ordering::Release);
    }

    pub fn is_packed_disabled(&self) -> bool {
        self.packed_unavailable.load(Ordering::Acquire)
    }

    /// Get existing golden or return None if not yet prepared.
    pub async fn get(&self, fingerprint: &str) -> Option<MachineName> {
        self.goldens.read().await.get(fingerprint).cloned()
    }

    /// Get the golden for `fingerprint`, or construct it via `build`.
    ///
    /// `build` returns the prepared machine name. It runs under a lock held
    /// for its whole duration, and is skipped entirely if another caller
    /// registered the same fingerprint while this one waited.
    pub async fn get_or_prepare(
        &self,
        fingerprint: &str,
        build: impl Future<Output = Result<MachineName, OrchestratorError>>,
    ) -> Result<MachineName, OrchestratorError> {
        // Fast path: already registered.
        if let Some(golden) = self.get(fingerprint).await {
            return Ok(golden);
        }
        // Per-fingerprint lock, so one environment's bake cannot park every
        // other slot. Only builds of the *same* fingerprint serialize.
        let build_lock = {
            let mut locks = self.build_locks.write().await;
            locks
                .entry(fingerprint.to_owned())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let _guard = build_lock.lock().await;
        // Re-check: another caller may have built it while we waited.
        if let Some(golden) = self.get(fingerprint).await {
            return Ok(golden);
        }
        info!(
            fingerprint,
            "building golden for environment; other environments proceed concurrently"
        );
        let name = build.await?;
        self.insert(fingerprint.to_owned(), name.clone()).await;
        Ok(name)
    }

    /// Register a prepared golden VM for a fingerprint.
    pub async fn insert(&self, fingerprint: String, name: MachineName) {
        self.goldens.write().await.insert(fingerprint, name);
    }

    /// Remove and return a golden VM entry.
    #[allow(dead_code)]
    pub async fn remove(&self, fingerprint: &str) -> Option<MachineName> {
        self.goldens.write().await.remove(fingerprint)
    }

    /// Return all registered golden machine names.
    pub async fn all_names(&self) -> Vec<MachineName> {
        self.goldens.read().await.values().cloned().collect()
    }
}

impl RunnerPoolConfig {
    /// Validate configuration before changing machine state.
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        if self.size > 64 {
            return Err(OrchestratorError::Config(
                "runner pool size must be between 0 and 64".into(),
            ));
        }
        if self.storage_gib == 0 {
            return Err(OrchestratorError::Config(
                "runner storage must be greater than zero".into(),
            ));
        }
        MachineName::new(format!("{}-0", self.name_prefix))?;
        if self.base_image.trim().is_empty()
            || self.server_url.trim().is_empty()
            || self.release_version.trim().is_empty()
        {
            return Err(OrchestratorError::Config(
                "base image, server URL, and release version are required".into(),
            ));
        }
        if !self.runner_bundle.is_absolute() || !self.runner_bundle.is_dir() {
            return Err(OrchestratorError::Config(format!(
                "runner bundle does not exist: {}",
                self.runner_bundle.display()
            )));
        }
        if self.runner_binary_name.contains('/') || self.runner_binary_name.is_empty() {
            return Err(OrchestratorError::Config(
                "runner binary name must be a filename".into(),
            ));
        }
        if let Some(socket) = &self.control_socket {
            if !socket.is_absolute() || !socket.exists() {
                return Err(OrchestratorError::Config(format!(
                    "control socket does not exist: {}",
                    socket.display()
                )));
            }
            let bridge = control_bridge_dir(self).expect("control socket has a parent");
            if !bridge.is_dir() {
                return Err(OrchestratorError::Config(format!(
                    "control bridge directory does not exist: {}",
                    bridge.display()
                )));
            }
        }
        if std::env::var_os(&self.registration_token_env).is_none() {
            return Err(OrchestratorError::Config(format!(
                "registration token environment variable `{}` is not set",
                self.registration_token_env
            )));
        }
        Ok(())
    }

    fn artifact_payload(&self) -> PathBuf {
        // The packed artifact is keyed by the resolved base image AND the
        // environment fingerprint (toolchains + curated bake content). A
        // stem-only key would let a golden keep the previous bake forever:
        // bake-content changes (package pins, the ownership repair, new
        // toolchains) must invalidate the pack or the fork base silently
        // serves jobs the old toolchain.
        let fingerprint = EnvironmentSpec::for_base(self.base_image.clone()).fingerprint;
        let mut path = self.artifact_stem.clone().into_os_string();
        path.push(format!("-{fingerprint}"));
        PathBuf::from(path)
    }
}

/// Runner-pool lifecycle error.
#[derive(Debug, Error)]
pub enum OrchestratorError {
    /// Invalid pool configuration.
    #[error("invalid runner pool configuration: {0}")]
    Config(String),
    /// VM provider failure.
    #[error(transparent)]
    Vm(#[from] VmError),
    /// Host filesystem failure.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// The destination filesystem cannot hold the golden plus operating
    /// headroom. This is fatal rather than a local-build fallback: baking
    /// requires substantially more space than downloading.
    #[error(
        "insufficient disk space for pre-baked golden at {}: {} bytes available, {} bytes required (download plus 2 GiB headroom)",
        path.display(),
        available_bytes,
        required_bytes
    )]
    GoldenDiskSpace {
        path: PathBuf,
        available_bytes: u64,
        required_bytes: u64,
    },
    /// One or more runner slots exited unexpectedly.
    #[error("runner pool stopped unexpectedly: {0}")]
    Pool(String),
}

/// Supervises disposable one-job runners backed by a reusable packed VM image.
pub struct RunnerPool<P: VmProvider = SmolVmProvider> {
    provider: Arc<P>,
    config: RunnerPoolConfig,
}

/// Pull `images` into a golden so every runner forked from it starts warm.
///
/// Forking copy-on-writes the golden's ext4 storage disk as well as its overlay
/// root, so an image sitting in [`DOCKER_DATA_ROOT`] costs each runner nothing
/// and is usable the instant it boots. Left to job time it is re-pulled by
/// every ephemeral runner that needs it: measured cold, 3.5s for
/// `postgres:16-alpine` and 8.7s for `node:20`, on every run.
///
/// Only images the workspace's own workflows declare are pulled, so a warm
/// golden can never make a job pass locally that would fail on GitHub.
async fn preload_images<P: VmProvider>(
    provider: &P,
    golden: &MachineName,
    images: &[String],
) -> Result<(), OrchestratorError> {
    if images.is_empty() {
        return Ok(());
    }
    // The golden has no dockerd yet -- that starts per runner at provision
    // time -- so this brings one up and leaves it running. Stopping it here
    // would not produce a clean slate: a fork restores the golden's process
    // table, so `pkill` leaves `[dockerd] <defunct>`, a pidfile naming that
    // zombie, and a half-torn-down containerd whose socket the next daemon
    // cannot dial. Handing forks a live daemon avoids all three.
    //
    // The trailing `sync` is load-bearing: forking captures the disk, not the
    // page cache, so hundreds of MB of fresh layers would otherwise reach forks
    // as metadata pointing at unreadable blobs (EIO on every inherited image).
    let output = provider
        .exec(golden, &preload_images_command(images))
        .await?;
    // Report what actually landed. An earlier version logged the requested
    // count unconditionally, hiding a preload that pulled nothing at all.
    let pulled = String::from_utf8_lossy(&output.stdout)
        .lines()
        .rev()
        .find_map(|line| line.trim().parse::<usize>().ok())
        .unwrap_or(0);
    if pulled == 0 {
        return Err(OrchestratorError::Config(format!(
            "image preload pulled none of {} requested images",
            images.len()
        )));
    }
    info!(
        machine = golden.as_str(),
        pulled,
        requested = images.len(),
        "preloaded container images into golden"
    );
    Ok(())
}

/// The golden-side command that brings up a container engine and pulls
/// `images` into it.
///
/// The engine is started with the hosted process limits ([`GUEST_STACK_ULIMIT`]
/// and [`GUEST_NOFILE_ULIMIT`]): the daemon lives on in the golden's process
/// table, forks inherit it as the engine that serves their container steps,
/// and a container's processes inherit the limits of the chain that spawned
/// them. Started without the raise, every forked runner would hand its
/// containers the VM init's 8192 KiB and its own descriptor limit — half of
/// GitHub's stack, and a nofile pair no hosted container has — even though its
/// own runner wrapper had raised both for step processes.
fn preload_images_command(images: &[String]) -> Vec<String> {
    let refs = images
        .iter()
        .map(|image| format!("'{}'", image.replace('\'', "'\\''")))
        .collect::<Vec<_>>()
        .join(" ");
    vec![
        "sh".to_owned(),
        "-c".to_owned(),
        run_as_root_or_sudo(&format!(
            "{GUEST_STACK_ULIMIT}; {GUEST_NOFILE_ULIMIT}; \
             command -v dockerd >/dev/null 2>&1 || {{ echo 'no dockerd' >&2; exit 1; }}; \
             mkdir -p {DOCKER_DATA_ROOT}; \
             docker info >/dev/null 2>&1 || (dockerd >/var/log/dockerd-preload.log 2>&1 &); \
             for _ in $(seq 1 150); do docker info >/dev/null 2>&1 && break; sleep 0.2; done; \
             docker info >/dev/null 2>&1 || {{ echo 'dockerd never became ready' >&2; exit 1; }}; \
             pulled=0; \
             for image in {refs}; do \
               docker pull -q \"$image\" >/dev/null 2>&1 && pulled=$((pulled+1)) \
                 || echo \"preload miss: $image\" >&2; \
             done; \
             sync; \
             echo \"$pulled\""
        )),
    ]
}

/// Scope native Ubuntu apt repositories before installing amd64 libraries on
/// an arm64 guest. The pinned 22.04 base uses `sources.list`; 24.04 uses a
/// deb822 `ubuntu.sources`. A different root keeps the same shell behavior
/// testable without modifying the host's `/etc/apt`.
fn scope_rosetta_apt_sources(apt_dir: &str) -> String {
    let mut script = format!("APT_DIR={}; ", shell_quote(apt_dir));
    script.push_str(
        r#"if [ -f "$APT_DIR/sources.list.d/ubuntu.sources" ]; then
  grep -q '^Architectures: arm64$' "$APT_DIR/sources.list.d/ubuntu.sources" ||
    sed -i '/^Types: deb$/a Architectures: arm64' "$APT_DIR/sources.list.d/ubuntu.sources"
elif [ -f "$APT_DIR/sources.list" ] && grep -q '^deb ' "$APT_DIR/sources.list"; then
  :
else
  echo 'no supported Ubuntu apt sources found for Rosetta multiarch' >&2
  exit 1
fi
for source in "$APT_DIR/sources.list" "$APT_DIR"/sources.list.d/*.list; do
  [ -f "$source" ] || continue
  [ "$source" = "$APT_DIR/sources.list.d/preloop-amd64.list" ] && continue
  sed -i -E '/^deb[[:space:]]/ { /\[arch=/b; s/^deb[[:space:]]+\[/deb [arch=arm64 /; t; s/^deb[[:space:]]+/deb [arch=arm64] /; }' "$source"
done"#,
    );
    script
}

/// The complete guest script [`prepare_rosetta_multiarch`] runs, wrapped for
/// root-or-sudo. Separate so the composed text — not just its pieces — is
/// tested to parse: a stray newline in a spliced fragment once left a line
/// starting with `;`, every fresh golden bake failed with `sh: 15: Syntax
/// error`, and the pool silently fell back to plain Ubuntu runners.
fn rosetta_multiarch_script() -> String {
    // The strict wrapper is load-bearing: a refused passwordless sudo or a
    // failed apt step must surface as a nonzero exit. The lenient
    // `|| true` form would report success and bake a golden whose forks all
    // fail `test -f /lib64/ld-linux-x86-64.so.2` consumers.
    run_as_root_or_sudo_strict(&format!(
        "set -e; \
         case \"$(uname -m)\" in \
           aarch64|arm64) ;; \
           *) echo 'guest is not arm64; rosetta multiarch install is a no-op' >&2; exit 0 ;; \
         esac; \
         {}; \
         dpkg --add-architecture amd64; \
         CODENAME=$(. /etc/os-release 2>/dev/null && echo \"$VERSION_CODENAME\"); \
         [ -n \"$CODENAME\" ] || CODENAME=noble; \
         : > /etc/apt/sources.list.d/preloop-amd64.list; \
         for s in '' '-updates' '-backports' '-security'; do \
           printf 'deb [arch=amd64] http://archive.ubuntu.com/ubuntu/ %s%s main restricted universe multiverse\\n' \"$CODENAME\" \"$s\" \
             >> /etc/apt/sources.list.d/preloop-amd64.list; \
         done; \
         apt-get update -qq; \
         DEBIAN_FRONTEND=noninteractive \
         apt-get install -y -qq --no-install-recommends \
           libc6:amd64 libgcc-s1:amd64 libstdc++6:amd64 zlib1g:amd64 \
           libsystemd0:amd64; \
         sync; \
         test -f /lib64/ld-linux-x86-64.so.2",
        scope_rosetta_apt_sources("/etc/apt")
    ))
}

/// Install the amd64 loader + libc into an arm64 golden so dynamically
/// linked x86_64 binaries can run under Rosetta translation.
///
/// Only Apple Silicon hosts have Rosetta, so everything else is a no-op; the
/// script additionally self-guards on the guest arch (an x86_64 golden on a
/// Mac already ships the amd64 rootfs natively). The trailing `sync` is
/// load-bearing exactly as in [`preload_images`]: forking captures the disk,
/// not the page cache, and the installed packages must reach the frozen base.
/// The package set covers the loader, libc, C++ runtime, zlib, and systemd
/// (valkey's official x86_64 tarballs link `libsystemd.so.0`); apt pulls the
/// amd64 transitive deps.
async fn prepare_rosetta_multiarch<P: VmProvider>(
    provider: &P,
    golden: &MachineName,
) -> Result<(), OrchestratorError> {
    if !(cfg!(target_os = "macos") && std::env::consts::ARCH == "aarch64") {
        return Ok(());
    }
    // Ubuntu 24.04 uses deb822 `ubuntu.sources`; the pinned 22.04 image uses
    // one-line `/etc/apt/sources.list` instead. Both arm64 sources point at
    // ports.ubuntu.com, which has no amd64 packages. Scope native sources to
    // arm64 before adding amd64, including any third-party `.list` entries
    // without an architecture restriction, or every later apt-get update
    // probes those mirrors for amd64 and fails. Add the explicit amd64 archive
    // across all four suites: the base suite alone may be older than the
    // image's security-update glibc, whose mutual Breaks pins block
    // installation. One suite per deb line; the one-line format misparses
    // extra suites as components. `sync` flushes before forking the golden.
    let script = rosetta_multiarch_script();
    let output = provider
        .exec(golden, &["sh".to_owned(), "-c".to_owned(), script])
        .await?;
    if output.exit_code != 0 {
        return Err(OrchestratorError::Config(format!(
            "rosetta multiarch install failed (exit {}): {}",
            output.exit_code,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    info!(
        machine = golden.as_str(),
        "installed amd64 multiarch libs into golden for Rosetta x86_64 translation"
    );
    Ok(())
}

/// Prepare a running forkable golden VM with the requested environment.
///
/// SmolVM takes the forkable RAM/disk snapshot when `start --forkable` runs.
/// Host-side record of the environment a golden was baked for.
///
/// Lives beside the packed artifact, which is already the pool's writable home
/// for VM assets. The fingerprint covers the base-image digest and every
/// toolchain layer, so bumping any of them leaves the record unmatched and the
/// golden is rebuilt rather than wrongly adopted.
fn golden_record_path(config: &RunnerPoolConfig, golden: &MachineName) -> Option<PathBuf> {
    let directory = config.artifact_stem.parent()?.join("goldens");
    Some(directory.join(format!("{}.fingerprint", golden.as_str())))
}

/// Record what this golden was baked for, replacing any earlier record.
fn write_golden_record(config: &RunnerPoolConfig, golden: &MachineName, fingerprint: &str) {
    let Some(path) = golden_record_path(config, golden) else {
        return;
    };
    let written = path
        .parent()
        .map(std::fs::create_dir_all)
        .transpose()
        .and_then(|_| std::fs::write(&path, fingerprint));
    if let Err(error) = written {
        // A missing record costs one rebake on the next start, nothing more.
        warn!(path = %path.display(), %error, "golden bake record not written");
    }
}

/// Drop the record, so an interrupted rebake cannot be adopted.
fn remove_golden_record(config: &RunnerPoolConfig, golden: &MachineName) {
    if let Some(path) = golden_record_path(config, golden) {
        let _ = std::fs::remove_file(path);
    }
}

/// Adoption outcome for an existing golden machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GoldenAdopt {
    /// Already booted as a fork base with a matching fingerprint; use as-is.
    Reusable,
    /// Stopped with a matching fingerprint; needs `start_forkable` to serve
    /// forks again. This is the normal state after a graceful engine shutdown,
    /// which stops (not deletes) goldens so the next start can adopt them.
    Rearm,
    /// No adoptable golden; bake or unpack a fresh one.
    Rebuild,
}

/// How `golden` can serve as the fork base for `fingerprint`, if at all.
///
/// Deliberately host-side: the golden is frozen as SmolVM's fork base, and
/// probing it through the guest would touch the very snapshot every clone is
/// taken from. The golden registry is in-memory, so without adoption an
/// engine restart rebakes a golden that is still sitting there fully baked —
/// apt plus rustup, five to eleven minutes, before the first job can run.
async fn golden_adopt_state<P: VmProvider>(
    provider: &Arc<P>,
    config: &RunnerPoolConfig,
    golden: &MachineName,
    fingerprint: &str,
) -> GoldenAdopt {
    let state = provider.status(golden).await;
    if !matches!(state, Ok(MachineState::Running) | Ok(MachineState::Stopped)) {
        return GoldenAdopt::Rebuild;
    }
    let Some(path) = golden_record_path(config, golden) else {
        return GoldenAdopt::Rebuild;
    };
    let matched = std::fs::read_to_string(path)
        .map(|recorded| recorded.trim() == fingerprint)
        .unwrap_or(false);
    if !matched {
        return GoldenAdopt::Rebuild;
    }
    match state {
        Ok(MachineState::Running) => GoldenAdopt::Reusable,
        _ => GoldenAdopt::Rearm,
    }
}

/// Adopt an existing golden instead of baking or unpacking a fresh one.
/// Returns `true` when the golden is ready to serve forks.
async fn adopt_golden<P: VmProvider>(
    provider: &Arc<P>,
    config: &RunnerPoolConfig,
    golden: &MachineName,
    fingerprint: &str,
    kind: &str,
) -> Result<bool, OrchestratorError> {
    let adopted = match golden_adopt_state(provider, config, golden, fingerprint).await {
        GoldenAdopt::Reusable => {
            info!(
                machine = golden.as_str(),
                fingerprint = %fingerprint,
                "adopted the existing {kind} fork base"
            );
            true
        }
        GoldenAdopt::Rearm => {
            info!(
                machine = golden.as_str(),
                fingerprint = %fingerprint,
                "re-arming stopped {kind} fork base left by the previous engine run"
            );
            if let Err(error) = provider.start_forkable(golden).await {
                warn!(
                    machine = golden.as_str(),
                    %error,
                    "golden re-arm failed; rebuilding"
                );
                return Ok(false);
            }
            info!(
                machine = golden.as_str(),
                fingerprint = %fingerprint,
                "adopted the existing {kind} fork base"
            );
            true
        }
        GoldenAdopt::Rebuild => false,
    };
    if adopted {
        // Issue #295's prune only ran on the fresh-bake path, so a golden
        // adopted across restarts keeps its pack/ intermediates forever —
        // and every fork copies them. Best-effort, same as the bake path.
        match provider.prune_pack_intermediates(golden).await {
            Ok(true) => info!(
                machine = golden.as_str(),
                "pruned pack/ build intermediates from adopted golden"
            ),
            Ok(false) => {}
            Err(error) => warn!(
                machine = golden.as_str(),
                %error,
                "failed to prune pack/ build intermediates from adopted golden"
            ),
        }
    }
    Ok(adopted)
}

/// Prepare a running forkable golden VM with the requested environment.
///
/// SmolVM takes the forkable RAM/disk snapshot when `start --forkable` runs.
/// Provision the guest while it is a normal machine, then restart it as the
/// fork base so package and external-runtime writes are inherited by clones.
async fn prepare_golden_for_env<P: VmProvider + 'static>(
    provider: &Arc<P>,
    config: &RunnerPoolConfig,
    golden: &MachineName,
    env_spec: &EnvironmentSpec,
) -> Result<(), OrchestratorError> {
    // The golden registry is in-memory, so without adoption every engine
    // restart rebakes a golden that is still sitting there fully baked —
    // apt plus rustup, five to eleven minutes, before the first job of that
    // environment can run, paid again on every deploy.
    if adopt_golden(provider, config, golden, &env_spec.fingerprint, "golden").await? {
        return Ok(());
    }
    // Any record must die before the machine does: a rebake interrupted
    // halfway would otherwise leave a fingerprint claiming a golden that no
    // longer carries it.
    remove_golden_record(config, golden);
    if provider.status(golden).await? != MachineState::Missing {
        provider.delete(golden).await?;
        vm_telemetry_deregister(config, golden);
    }
    let spec = MachineSpec {
        name: golden.clone(),
        image: env_spec.base.clone(),
        cpus: config.cpus,
        memory_mib: config.memory_mib,
        storage_gib: config.storage_gib,
        overlay_gib: config.overlay_gib,
        network: NetworkPolicy::PublicOnly,
        volumes: runner_volumes(config, golden, true)?,
        sockets: config
            .control_socket
            .iter()
            .map(|host| SocketMount {
                host: host.clone(),
                guest: PathBuf::from(GUEST_CONTROL_SOCKET),
            })
            .collect(),
        dns: config.dns.clone(),
        rosetta: cfg!(target_os = "macos") && std::env::consts::ARCH == "aarch64",
    };
    provider.create(&spec).await?;
    provider.start(golden).await?;
    vm_telemetry_register(config, golden, "golden", Some(&spec));
    if let Err(error) = await_guest_ready(provider.as_ref(), golden).await {
        vm_telemetry_deregister(config, golden);
        let _ = provider.delete(golden).await;
        return Err(error);
    }
    if env_spec.curated {
        // Same contract as the packed-golden path: on Apple Silicon the
        // multiarch shim must reach the forkable base, or every dynamically
        // linked x86_64 binary fails at job time. Runs before the baseline
        // install so the amd64 sources are scoped before any later apt
        // update. Fatal, not a warning — a half-installed golden would be
        // adopted on later restarts (amd64 arch added, loader missing).
        // Custom bases skip it along with the rest of the bake: the image is
        // the operator's contract.
        if let Err(error) = prepare_rosetta_multiarch(provider.as_ref(), golden).await {
            vm_telemetry_deregister(config, golden);
            let _ = provider.delete(golden).await;
            return Err(error);
        }
        if let Err(error) = install_base_dependencies(provider.as_ref(), golden).await {
            vm_telemetry_deregister(config, golden);
            let _ = provider.delete(golden).await;
            return Err(error);
        }
    }
    for layer in &env_spec.toolchains {
        for command in layer.install_commands() {
            if let Err(error) = provider.exec(golden, &command).await {
                vm_telemetry_deregister(config, golden);
                let _ = provider.delete(golden).await;
                return Err(error.into());
            }
        }
    }
    if let Err(error) = write_bake_manifest(provider.as_ref(), golden, env_spec).await {
        // Provenance is an audit aid, not a build gate.
        warn!(machine = golden.as_str(), %error, "bake manifest not written");
    }
    if let Err(error) = preload_images(provider.as_ref(), golden, &config.preload_images).await {
        // A preload miss costs a run-time pull, not a broken job.
        warn!(
            machine = golden.as_str(),
            %error, "image preload failed; jobs will pull at run time"
        );
    }
    if let Err(error) = provider.stop(golden).await {
        vm_telemetry_deregister(config, golden);
        let _ = provider.delete(golden).await;
        return Err(error.into());
    }
    if let Err(error) = provider.start_forkable(golden).await {
        vm_telemetry_deregister(config, golden);
        let _ = provider.delete(golden).await;
        return Err(error.into());
    }
    write_golden_record(config, golden, &env_spec.fingerprint);
    info!(machine = golden.as_str(), "golden fork base ready");
    Ok(())
}

/// Prepare a forkable golden from the dependency-prepared packed artifact.
///
/// Socket mappings are supplied by the local machine definition, never read
/// from the artifact. SmolVM must preserve those explicit mappings on its
/// `machine create --from` path for local control-plane routing to work.
async fn prepare_packed_golden<P: VmProvider + 'static>(
    provider: &Arc<P>,
    config: &RunnerPoolConfig,
    golden: &MachineName,
) -> Result<(), OrchestratorError> {
    // Same adoption rule as the baked-golden path: an engine restart must not
    // re-unpack a multi-GiB packed golden that is still sitting there
    // fingerprint-matched. Without this every `serve` restart pays the
    // full unpack (tens of GB of storage writes) before the first job.
    let env_spec = EnvironmentSpec::for_base(config.base_image.clone());
    if adopt_golden(
        provider,
        config,
        golden,
        &env_spec.fingerprint,
        "packed golden",
    )
    .await?
    {
        return Ok(());
    }
    // Unpacking writes the golden's filesystem; its disk can grow to the
    // configured storage ceiling, and job forks grow on top of it. Not a
    // refusal: how much of the ceiling a given golden writes is image-specific
    // and the disk is sparse, so only flag a host that could not hold one
    // golden at its ceiling.
    warn_if_disk_below(
        &golden_disk_root(config),
        u64::from(config.storage_gib) * GIB,
        "packed golden unpack",
    );
    remove_golden_record(config, golden);
    if provider.status(golden).await? != MachineState::Missing {
        provider.delete(golden).await?;
        vm_telemetry_deregister(config, golden);
    }
    // smolvm's `machine create --from` consumes the SMOLPACK, not the ELF
    // launcher stub written at the payload stem. A downloaded release asset
    // IS the pack at the stem; a locally built golden leaves the pack in the
    // `.smolmachine` sidecar. Centralized in [`packed_golden_path`].
    let pack = packed_golden_path(&config.artifact_payload());
    let spec = MachineSpec {
        name: golden.clone(),
        image: pack.display().to_string(),
        cpus: config.cpus,
        memory_mib: config.memory_mib,
        storage_gib: config.storage_gib,
        overlay_gib: config.overlay_gib,
        network: NetworkPolicy::PublicOnly,
        volumes: runner_volumes(config, golden, false)?,
        sockets: config
            .control_socket
            .iter()
            .map(|host| SocketMount {
                host: host.clone(),
                guest: PathBuf::from(GUEST_CONTROL_SOCKET),
            })
            .collect(),
        dns: config.dns.clone(),
        rosetta: cfg!(target_os = "macos") && std::env::consts::ARCH == "aarch64",
    };
    if let Err(error) = provider.create(&spec).await {
        let _ = provider.delete(golden).await;
        return Err(error.into());
    }
    if let Err(error) = provider.start(golden).await {
        let _ = provider.delete(golden).await;
        return Err(error.into());
    }
    vm_telemetry_register(config, golden, "golden", Some(&spec));
    if let Err(error) = await_guest_ready(provider.as_ref(), golden).await {
        vm_telemetry_deregister(config, golden);
        let _ = provider.delete(golden).await;
        return Err(error);
    }
    // Fatal, not a warning: on Apple Silicon the multiarch shim is the
    // golden's contract, and a failed install leaves a reusable base that is
    // adoptable on later restarts (amd64 arch added, loader missing).
    prepare_rosetta_multiarch(provider.as_ref(), golden).await?;
    if let Err(error) = preload_images(provider.as_ref(), golden, &config.preload_images).await {
        warn!(
            machine = golden.as_str(),
            %error, "image preload failed; jobs will pull at run time"
        );
    }
    if let Err(error) = provider.stop(golden).await {
        vm_telemetry_deregister(config, golden);
        let _ = provider.delete(golden).await;
        return Err(error.into());
    }
    if let Err(error) = provider.start_forkable(golden).await {
        vm_telemetry_deregister(config, golden);
        let _ = provider.delete(golden).await;
        return Err(error.into());
    }
    // Issue #295: smolvm's pack export leaves ~29 GB of intermediates
    // (`storage.ext4`, `layers/*.tar`) beside the finished `storage.raw`,
    // and every fork copies the golden's whole data directory. Prune them
    // now that the disk is written. Best-effort: never fail the golden
    // over disk hygiene.
    match provider.prune_pack_intermediates(golden).await {
        Ok(true) => info!(
            machine = golden.as_str(),
            "pruned pack/ build intermediates from packed golden"
        ),
        Ok(false) => {}
        Err(error) => warn!(
            machine = golden.as_str(),
            %error,
            "failed to prune pack/ build intermediates from packed golden"
        ),
    }
    write_golden_record(config, golden, &env_spec.fingerprint);
    info!(
        machine = golden.as_str(),
        artifact = %config.artifact_payload().display(),
        "packed golden fork base ready"
    );
    Ok(())
}

impl<P: VmProvider + 'static> RunnerPool<P> {
    /// Construct a runner pool.
    pub fn new(provider: Arc<P>, config: RunnerPoolConfig) -> Result<Self, OrchestratorError> {
        config.validate()?;
        Ok(Self { provider, config })
    }

    /// Prepare the immutable runner image once, then supervise all slots until cancellation.
    pub async fn run(&self, shutdown: CancellationToken) -> Result<(), OrchestratorError> {
        if let Some(signal) = &self.config.preparing_signal {
            signal.store(true, std::sync::atomic::Ordering::Release);
        }
        if let Some(ps) = &self.config.pool_status {
            ps.set_preparing(true);
            // Publish the labels this pool advertises so the server's
            // starvation sweep can tell "runner still warming" apart from
            // "no runner can ever match these labels".
            ps.set_labels(self.config.labels.clone());
        }
        // Any error exit below (host externals, artifact prep, stale-machine
        // cleanup, golden bake) must clear the flag again; the guard is
        // disarmed once the warm completes.
        let mut clear_preparing = ClearPreparingOnDrop {
            signal: self.config.preparing_signal.clone(),
            pool_status: self.config.pool_status.clone(),
            armed: true,
        };
        ensure_host_externals(&self.config)?;
        // A backend whose packs are not host files (AgentENV keeps them as
        // server-side snapshots) has no artifact to build, download, or
        // relocate: its golden is prepared directly from the base image
        // below, and building a pack first would boot and discard a whole
        // extra VM for nothing.
        if self.provider.capabilities().file_packs
            && (self.config.use_packed_artifact || self.config.control_socket.is_none())
        {
            self.prepare_artifact(true).await?;
        }
        self.sweep_stale_artifacts().await;
        self.remove_stale_machines().await?;

        // Reconcile leaked VM state for the whole time the pool serves. The
        // startup pass above cannot see a delete that fails later, and an
        // engine that serves for hours (or days) then holds every leaked
        // machine and data dir until the next restart.
        let reconciler_shutdown = shutdown.child_token();
        let _reconciler = ReconcileGuard {
            shutdown: reconciler_shutdown.clone(),
            handle: {
                let provider = self.provider.clone();
                tokio::spawn(async move {
                    reconcile_orphans(provider, reconciler_shutdown, ORPHAN_RECONCILE_INTERVAL)
                        .await
                })
            },
        };

        let golden_registry = Arc::new(GoldenRegistry::new(self.config.name_prefix.clone()));

        // If fork mode is enabled, prepare a golden fork base VM for the
        // workspace's default environment (base image plus any toolchains
        // detected from version files like rust-toolchain.toml).
        if self.config.use_fork {
            let default_environment = EnvironmentSpec::for_base(self.config.base_image.clone());
            let golden = MachineName::new(format!("{}-golden", golden_registry.name_prefix))?;
            let result = if self.config.use_packed_artifact {
                prepare_packed_golden(&self.provider, &self.config, &golden).await
            } else {
                prepare_golden_for_env(&self.provider, &self.config, &golden, &default_environment)
                    .await
            };
            if let Err(error) = result {
                if self.config.use_packed_artifact {
                    golden_registry.disable_packed();
                    warn!(
                        %error,
                        "packed golden fork base unavailable; using direct per-runner creation \
                         instead of building a second environment golden"
                    );
                } else {
                    warn!(%error, "golden fork base unavailable; falling back to direct creation");
                }
            } else {
                golden_registry
                    .insert(default_environment.fingerprint, golden)
                    .await;
            }
        }

        // The warm is done: the pool can now register runners for queued
        // jobs. Clear the signal so the control plane's starvation sweep
        // counts the full grace window from here.
        if let Some(signal) = &self.config.preparing_signal {
            signal.store(false, std::sync::atomic::Ordering::Release);
        }
        if let Some(ps) = &self.config.pool_status {
            ps.set_preparing(false);
        }
        clear_preparing.armed = false;

        let mut slots = JoinSet::new();
        // Runners currently registered and waiting for work. Slots consult it
        // to decide whether a replacement is worth booting mid-job.
        let idle = Arc::new(std::sync::Mutex::new(0));
        // Filled in the background so no slot ever waits on RSA generation.
        let keys = Arc::new(KeyPool::new());
        keys.spawn_refill();
        let building = Arc::new(AtomicUsize::new(0));

        // On-demand mode: size=0 means no warm pool. Fork runners only when
        // jobs arrive, capped by the host's CPU and memory budget.
        if self.config.size == 0 {
            return self
                .run_on_demand(shutdown, golden_registry, idle, keys, building)
                .await;
        }

        // Warm mode: cap the configured pool size by host memory as well as
        // CPU. Each warm slot forks from the golden and inherits its
        // committed footprint (growing toward `memory_mib` while a job
        // runs), and a slot provisions its successor mid-job — so on a
        // small host the configured size can still exhaust RAM. Sizing down
        // at startup is safer than OOMing mid-run; `PRELOOP_RUNNER_POOL_SIZE`
        // remains an explicit override that wins.
        let warm_size = match host_memory_mib() {
            Some(total) => self.config.size.min(
                on_demand_memory_cap(total, u64::from(self.config.memory_mib))
                    .saturating_div(2)
                    .max(1),
            ),
            None => self.config.size,
        };
        if warm_size < self.config.size {
            warn!(
                configured = self.config.size,
                warm_size,
                memory_mib = self.config.memory_mib,
                "reduced warm pool size to fit host memory"
            );
        }

        // Warm slots boot their first runners *after* the warm completes and
        // the preparing flag clears, so those boots must raise the shared
        // provisioning guard or a job queued during the ~minutes-long boot
        // window would starve (the server's sweep fails jobs queued 120s
        // with no matching runner). One counter for the whole pool: the
        // first completed slot must not clear the signal while its siblings
        // are still bootstrapping.
        let provisioning = Arc::new(std::sync::Mutex::new(0));
        for slot in 0..warm_size {
            let provider = self.provider.clone();
            let config = self.config.clone();
            let slot_shutdown = shutdown.child_token();
            let slot_registry = golden_registry.clone();
            let slot_handles = PoolHandles {
                idle: idle.clone(),
                keys: keys.clone(),
                building: building.clone(),
                provisioning: provisioning.clone(),
            };
            slots.spawn(async move {
                run_slot(
                    provider,
                    config,
                    slot,
                    slot_shutdown,
                    slot_registry,
                    slot_handles,
                )
                .await
            });
        }

        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {}
            result = slots.join_next() => {
                shutdown.cancel();
                match result {
                    Some(Ok(Err(error))) => {
                        error!(%error, "runner slot failed; tearing down pool");
                        return Err(error);
                    }
                    Some(Err(error)) => {
                        error!(%error, "runner slot task panicked; tearing down pool");
                        return Err(OrchestratorError::Pool(error.to_string()));
                    }
                    Some(Ok(Ok(()))) => {
                        error!("runner slot exited without shutdown; tearing down pool");
                        return Err(OrchestratorError::Pool("runner slot exited".into()));
                    }
                    None => return Err(OrchestratorError::Pool("runner pool had no slots".into())),
                }
            }
        }

        while slots.join_next().await.is_some() {}
        // Stop, don't delete, every environment-specific golden fork base: the
        // machine directory (fork snapshot) must survive so the next engine
        // start can adopt it instead of rebaking. `remove_stale_machines`
        // spares recorded goldens; the fingerprint record gates adoption.
        for golden in golden_registry.all_names().await {
            vm_telemetry_deregister(&self.config, &golden);
            if let Err(error) = self.provider.stop(&golden).await {
                warn!(
                    machine = golden.as_str(),
                    %error,
                    "failed to stop golden on shutdown; the next start will rebake it"
                );
            }
        }
        self.remove_stale_machines().await?;
        Ok(())
    }

    /// On-demand mode: no warm pool. Fork a runner only when the server
    /// has queued work, capped at `nproc / cpus_per_runner` concurrent
    /// runners so the host CPU is not over-committed.
    async fn run_on_demand(
        &self,
        shutdown: CancellationToken,
        golden_registry: Arc<GoldenRegistry>,
        idle: Arc<std::sync::Mutex<usize>>,
        keys: Arc<KeyPool>,
        building: Arc<AtomicUsize>,
    ) -> Result<(), OrchestratorError> {
        let max_concurrent = {
            // The memory term below reserves the golden and host headroom.
            // Do not subtract another CPU slot here: with four vCPUs on an
            // eight-thread host that forced max_concurrent=1, serializing
            // matrix jobs even though a second 1.15.0 CoW fork is cheap.
            let parallelism = std::thread::available_parallelism().map_or(2, |value| value.get());
            let per_runner = usize::from(self.config.cpus.max(1));
            let by_cpu = (parallelism / per_runner).max(1);
            // memory term: every on-demand fork inherits the golden's
            // committed footprint and grows toward `memory_mib` as the guest
            // runs. On a small host (the production 6-core/22 GiB machine)
            // the CPU term alone allows enough 8 GiB forks to exhaust RAM
            // and OOM the whole control plane. Reserve the golden's memory
            // plus host headroom, then fit the remainder with per-runner
            // ceilings. A host we cannot measure falls back to CPU-only.
            match host_memory_mib() {
                Some(total) => by_cpu.min(on_demand_memory_cap(
                    total,
                    u64::from(self.config.memory_mib),
                )),
                None => by_cpu,
            }
        };
        info!(max_concurrent, "on-demand runner pool (size=0)");

        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
        let provisioning = Arc::new(std::sync::Mutex::new(0));
        let mut slots = JoinSet::new();
        let mut next_slot: usize = 0;

        loop {
            // Wait until the server has queued at least one job.
            let pending = self.config.pending_jobs.as_deref();
            loop {
                if shutdown.is_cancelled() {
                    break;
                }
                let queued = pending.map_or(0, |p| p.load(Ordering::Acquire));
                if queued > 0 {
                    break;
                }
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                }
            }
            if shutdown.is_cancelled() {
                break;
            }

            // Acquire a concurrency permit (blocks if max_concurrent reached).
            let permit = tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                permit = semaphore.clone().acquire_owned() => {
                    permit.expect("semaphore is never closed")
                }
            };

            let slot = next_slot;
            next_slot = next_slot.wrapping_add(1);
            let provider = self.provider.clone();
            let config = self.config.clone();
            let slot_shutdown = shutdown.child_token();
            let slot_registry = golden_registry.clone();
            let slot_handles = PoolHandles {
                idle: idle.clone(),
                keys: keys.clone(),
                building: building.clone(),
                provisioning: provisioning.clone(),
            };
            let slot_provisioning = provisioning.clone();
            // Shared with the slot's pause watcher: a job parked in a debug
            // session hands the permit back to the pool and re-acquires it
            // when the session closes, so a paused job cannot pin a
            // concurrency slot (and eventually the whole pool) for the
            // duration of the pause.
            let permit_slot = Arc::new(std::sync::Mutex::new(Some(permit)));
            let slot_semaphore = semaphore.clone();

            slots.spawn(async move {
                let result = run_on_demand_slot(
                    provider,
                    config,
                    slot,
                    slot_shutdown,
                    slot_registry,
                    slot_handles,
                    slot_provisioning,
                    slot_semaphore,
                    permit_slot,
                )
                .await;
                if let Err(error) = &result {
                    warn!(slot, %error, "on-demand runner failed");
                }
                result
            });

            // Reap any finished tasks without blocking. A failed slot is
            // usually transient, but an environment-level failure (missing
            // or outdated smolvm, disk full) fails every attempt, and
            // respawning as fast as slots drain turns that into a log
            // storm. Back off exponentially after failures so a broken
            // setup logs a few lines per minute instead of a flood; any
            // success resets the backoff.
            let mut failure_backoff = Duration::ZERO;
            while let Some(result) = slots.try_join_next() {
                match result {
                    Ok(Ok(())) => failure_backoff = Duration::ZERO,
                    Ok(Err(error)) => {
                        warn!(%error, "on-demand runner slot error");
                        failure_backoff = if failure_backoff.is_zero() {
                            Duration::from_millis(500)
                        } else {
                            failure_backoff
                                .saturating_mul(2)
                                .min(Duration::from_secs(30))
                        };
                    }
                    Err(error) => warn!(%error, "runner slot task failed"),
                }
            }
            if !failure_backoff.is_zero() {
                tokio::time::sleep(failure_backoff).await;
            }
        }

        // Drain remaining runners on shutdown.
        while slots.join_next().await.is_some() {}
        // Stop, don't delete, golden fork bases: the machine directory (fork
        // snapshot) must survive so the next engine start can adopt the golden
        // instead of rebaking it. `remove_stale_machines` spares recorded
        // goldens; the fingerprint record gates adoption.
        for golden in golden_registry.all_names().await {
            vm_telemetry_deregister(&self.config, &golden);
            if let Err(error) = self.provider.stop(&golden).await {
                warn!(
                    machine = golden.as_str(),
                    %error,
                    "failed to stop golden on shutdown; the next start will rebake it"
                );
            }
        }
        self.remove_stale_machines().await?;
        Ok(())
    }

    /// Build a fresh packed runner artifact without downloading or reusing an
    /// existing release asset.
    pub async fn rebuild_artifact(&self) -> Result<(), OrchestratorError> {
        let payload = self.config.artifact_payload();
        match std::fs::remove_file(&payload) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        self.prepare_artifact(false).await
    }

    async fn prepare_artifact(&self, allow_download: bool) -> Result<(), OrchestratorError> {
        let payload = self.config.artifact_payload();
        if payload.is_file() {
            return Ok(());
        }
        if let Some(parent) = self.config.artifact_stem.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let custom_golden_url = std::env::var("PRELOOP_GOLDEN_URL")
            .ok()
            .is_some_and(|value| !value.trim().is_empty());
        if allow_download
            && should_download_prebaked_golden(&self.config.base_image, custom_golden_url)
        {
            if download_prebaked_golden(&payload, &self.config.release_version).await? {
                return Ok(());
            }
        } else if allow_download {
            info!(
                base_image = %self.config.base_image,
                "custom base image has no PRELOOP_GOLDEN_URL; building its golden locally"
            );
        }

        let name = MachineName::new(format!("{}-builder", self.config.name_prefix))?;
        if self.provider.status(&name).await? != MachineState::Missing {
            self.provider.delete(&name).await?;
        }
        // Packing exports a second copy of the guest filesystem before
        // producing the artifact. Give the one-shot builder headroom
        // without increasing the storage allocated to job VMs.
        let builder_storage_gib = golden_builder_storage_gib(self.config.storage_gib);
        // Checked after the stale builder is gone, so its space counts as free.
        ensure_disk_for_golden_build(&golden_disk_root(&self.config), builder_storage_gib)
            .map_err(OrchestratorError::Config)?;
        let spec = MachineSpec {
            name: name.clone(),
            image: self.config.base_image.clone(),
            cpus: self.config.cpus,
            memory_mib: self.config.memory_mib,
            storage_gib: builder_storage_gib,
            overlay_gib: self.config.overlay_gib,
            network: NetworkPolicy::PublicOnly,
            volumes: Vec::new(),
            sockets: Vec::new(),
            dns: self.config.dns.clone(),
            rosetta: cfg!(target_os = "macos") && std::env::consts::ARCH == "aarch64",
        };
        self.provider.create(&spec).await?;
        self.provider.start(&name).await?;
        // Plain Ubuntu needs the hosted-runner package baseline. Official
        // runner snapshots already contain it and workflow setup actions own
        // language-toolchain selection.
        let stock_base = is_stock_base_image(&self.config.base_image);
        if stock_base
            && let Err(error) = install_base_dependencies(self.provider.as_ref(), &name).await
        {
            let _ = self.provider.delete(&name).await;
            return Err(error);
        }
        if stock_base {
            for layer in curated_toolchains() {
                for command in layer.install_commands() {
                    let output = self.provider.exec(&name, &command).await?;
                    if output.exit_code != 0 {
                        let _ = self.provider.delete(&name).await;
                        return Err(OrchestratorError::Config(format!(
                            "toolchain install failed for {layer} (exit {}): {}",
                            output.exit_code,
                            String::from_utf8_lossy(&output.stderr)
                                .lines()
                                .last()
                                .unwrap_or("unknown error")
                        )));
                    }
                }
            }
            // Toolchain installation runs as root and recreates writable
            // rustup state beneath these homes. Re-apply runner ownership
            // after every layer; doing it only in the base script leaves
            // `/usr/local/rustup/tmp` root-owned and job-time rustup updates
            // fail with EACCES.
            let output = self
                .provider
                .exec(
                    &name,
                    &[
                        "sh".to_owned(),
                        "-c".to_owned(),
                        runner_account_script(DEFAULT_RUNNER_USER, DEFAULT_RUNNER_UID),
                    ],
                )
                .await?;
            if output.exit_code != 0 {
                let _ = self.provider.delete(&name).await;
                return Err(OrchestratorError::Config(format!(
                    "final runner-account ownership failed (exit {}): {}",
                    output.exit_code,
                    String::from_utf8_lossy(&output.stderr)
                        .lines()
                        .last()
                        .unwrap_or("unknown error")
                )));
            }
        }
        // Stamp apt-index freshness into the image. Provisioning reads this
        // back to log the pack's age and warn past the versions.toml policy;
        // the weekly apt-indices-refresh workflow rebuilds stale packs. The
        // policy travels in the marker so a running engine never needs the
        // policy compiled in — only the bake-time CLI does. Never fail the
        // bake for telemetry: a missing marker just means "stale", which is
        // today's behavior (full refresh) everywhere.
        if stock_base {
            let stamp = format!(
                "mkdir -p /etc/preloop && printf '%s\\n%s\\n' \"$(date -u +%F)\" \"{}\" > /etc/preloop/apt-indices-baked-at",
                crate::APT_INDICES_MAX_AGE_DAYS
            );
            if let Err(error) = self
                .provider
                .exec(&name, &["sh".to_owned(), "-c".to_owned(), stamp])
                .await
            {
                warn!(
                    machine = name.as_str(),
                    %error,
                    "apt-index freshness stamp failed; pack will read as stale"
                );
            }
        }
        // Bake the externals *pointer*, not the externals: the packed rootfs
        // gets `<root>/externals -> /opt/preloop/bin/externals` so node rides
        // the runner-bundle mount instead of being baked into the image or
        // downloaded per machine. The symlink must live in the pack itself —
        // forkable snapshots do not capture exec writes made after create —
        // and it must be baked here, in the builder, where the rootfs layer
        // is flattened into the artifact. The bundle's host side carries the
        // real `externals/` (see `ensure_host_externals`).
        let link_command = format!(
            "mkdir -p {root} && rm -rf {root}/externals && \
             ln -s /opt/preloop/bin/externals {root}/externals",
            root = RUNNER_ROOT
        );
        let output = self
            .provider
            .exec(&name, &["sh".to_owned(), "-c".to_owned(), link_command])
            .await?;
        if output.exit_code != 0 {
            let _ = self.provider.delete(&name).await;
            return Err(OrchestratorError::Config(format!(
                "baking externals symlink failed (exit {}): {}",
                output.exit_code,
                String::from_utf8_lossy(&output.stderr)
                    .lines()
                    .last()
                    .unwrap_or("unknown")
            )));
        }
        // `base_install_script` already prepared the default account for
        // curated bases, and provisioning reconciles it again before configure
        // (custom bases get the fix there for the first time). Re-run it here
        // only for a configured non-default user, whose identity the
        // fingerprint cannot know: the script is idempotent either way.
        let runner_user = self
            .config
            .runner_user
            .as_deref()
            .unwrap_or(DEFAULT_RUNNER_USER);
        let runner_uid = self.config.runner_uid.unwrap_or(DEFAULT_RUNNER_UID);
        if runner_user != DEFAULT_RUNNER_USER || runner_uid != DEFAULT_RUNNER_UID {
            let output = self
                .provider
                .exec(
                    &name,
                    &[
                        "sh".to_owned(),
                        "-c".to_owned(),
                        runner_account_script(runner_user, runner_uid),
                    ],
                )
                .await?;
            if output.exit_code != 0 {
                // A golden whose runner account is wrong cannot run a job:
                // every step fails on permissions, which reads as flaky CI.
                let _ = self.provider.delete(&name).await;
                return Err(OrchestratorError::Config(format!(
                    "baking runner account {runner_user} failed (exit {}): {}",
                    output.exit_code,
                    String::from_utf8_lossy(&output.stderr)
                        .lines()
                        .last()
                        .unwrap_or("unknown")
                )));
            }
        }
        let env_spec = EnvironmentSpec::for_base(self.config.base_image.clone());
        if let Err(error) = write_bake_manifest(self.provider.as_ref(), &name, &env_spec).await {
            // Provenance is an audit aid, not a build gate.
            warn!(machine = name.as_str(), %error, "bake manifest not written");
        }
        self.provider.stop(&name).await?;
        let temporary = payload
            .parent()
            .map(|parent| parent.join(format!(".tmp-golden-{}", uuid::Uuid::new_v4())))
            .ok_or_else(|| {
                OrchestratorError::Config(format!(
                    "golden artifact path has no parent: {}",
                    payload.display()
                ))
            })?;
        // Claim the companion lock for the pack duration: the heartbeat
        // keeps it fresh, and dropping the guard removes it. A staging file
        // without a fresh lock reads as orphaned to a concurrent sweep.
        let _staging_guard = StagingLockGuard::claim(staging_lock_path(&temporary));
        if let Err(error) = self.provider.pack(&name, &temporary).await {
            let _ = std::fs::remove_file(&temporary);
            return Err(error.into());
        }
        // smolvm pack writes two files: `<output>` (ELF executable stub) and
        // `<output>.smolmachine` (the packed VM data). The latter is the
        // artifact consumed by `machine create --from`; the stub is only a
        // launcher and is discarded.
        let sidecar = PathBuf::from(format!("{}.smolmachine", temporary.display()));
        let rename_res = std::fs::rename(&sidecar, &payload);
        let _ = std::fs::remove_file(&temporary);
        rename_res.inspect_err(|_| {
            let _ = std::fs::remove_file(&sidecar);
        })?;
        self.provider.delete(&name).await?;
        if !payload.is_file() {
            return Err(OrchestratorError::Config(format!(
                "smolvm did not create expected artifact {}",
                payload.display()
            )));
        }
        Ok(())
    }

    async fn remove_stale_machines(&self) -> Result<(), OrchestratorError> {
        for name in self.provider.list().await? {
            if name
                .as_str()
                .starts_with(&format!("{}-", self.config.name_prefix))
            {
                // A golden fork base recorded by a previous engine run is not
                // stale: its machine directory plus the fingerprint record are
                // exactly what the startup adoption path needs. Deleting it
                // here forced a full rebake on every restart (#293); the
                // prepare path rebuilds it anyway when the fingerprint no
                // longer matches.
                if golden_record_path(&self.config, &name).is_some_and(|path| path.is_file()) {
                    continue;
                }
                notify_runner_gone(&self.config, &name).await;
                vm_telemetry_deregister(&self.config, &name);
                if let Err(error) = self.provider.delete(&name).await {
                    record_slot_failure(&self.config, "stale_cleanup");
                    warn!(machine = name.as_str(), %error, "failed to delete stale Preloop runner");
                }
            }
        }
        // A crashed server orphans its detached `_boot-vm` hypervisor
        // processes; when the data dir was cleaned out from under them the
        // smolvm DB no longer knows the machines, so the deletes above
        // cannot reach them and they keep the storage fds open — the
        // unlinked blocks leak until the process dies. Kill by config path.
        // `All` is only sound here: every Preloop machine is stale by now, so
        // nothing of ours can be running. The periodic reconcile the pool
        // runs while serving uses `RemovedDataDir`, which spares any process
        // whose boot config still exists.
        match preloop_vm::purge_orphaned_vms(preloop_vm::OrphanPurge::All) {
            Ok(killed) if killed > 0 => {
                info!(killed, "purged orphaned SmolVM hypervisor processes")
            }
            _ => {}
        }
        // The registry sweep above cannot see a machine whose registry row is
        // gone but whose data dir survived (smolvm's delete drops the row
        // first; a failed remove_dir_all or a crashed engine strands the
        // files). Reconcile the filesystem against the registry.
        match self.provider.sweep_orphaned_data_dirs().await {
            Ok(swept) if swept > 0 => {
                info!(swept, "removed orphaned machine data directories")
            }
            Ok(_) => {}
            Err(error) => warn!(%error, "orphaned data-dir sweep failed"),
        }
        Ok(())
    }

    /// Collect golden-artifact garbage the success paths leave behind.
    ///
    /// Two leaks live beside `artifact_stem` in `vms/`:
    ///
    /// - `.tmp-golden-*` staging files: the pack and download paths remove
    ///   them on failures they observe, but a crash between `pack()` and the
    ///   final `rename()` leaves a multi-GB file nothing ever revisits.
    /// - `<stem>-<fingerprint>` payloads from earlier environment
    ///   fingerprints: `artifact_payload()` rotates the filename on every
    ///   bake-content change and `rebuild_artifact` deletes only the current
    ///   one, so each rebake strands the previous packed golden.
    ///
    /// The current payload and the `goldens/` fingerprint directory are
    /// always kept. Runs at pool startup, before `remove_stale_machines`.
    async fn sweep_stale_artifacts(&self) {
        let Some(directory) = self.config.artifact_stem.parent() else {
            return;
        };
        let stem_name = self
            .config
            .artifact_stem
            .file_name()
            .map(|name| name.to_string_lossy().into_owned());
        let current_payload = self.config.artifact_payload();
        let Ok(entries) = std::fs::read_dir(directory) else {
            return;
        };
        let mut swept = 0usize;
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            let stale_tmp = if name.starts_with(".tmp-golden-") {
                if name.ends_with(".lock") {
                    // Companion lock whose staging file is gone (or whose
                    // writer died): a fresh lock is transient, a stale one
                    // is swept with its staging file.
                    !staging_lock_is_fresh(&path)
                } else {
                    !staging_lock_is_fresh(&staging_lock_path(&path))
                }
            } else {
                false
            };
            // A stale payload is `<stem>-<64-hex>`: the fingerprint suffix
            // shape keeps the sweep from touching unrelated files that happen
            // to share the stem prefix.
            let stale_payload = stem_name.as_deref().is_some_and(|stem| {
                name.strip_prefix(stem).is_some_and(|suffix| {
                    suffix.len() == 65
                        && suffix.starts_with('-')
                        && suffix[1..].chars().all(|c| c.is_ascii_hexdigit())
                })
            }) && path != current_payload;
            if !stale_tmp && !stale_payload {
                continue;
            }
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    swept += 1;
                    // A swept staging file leaves its companion lock behind;
                    // remove it in the same pass (a stale `.lock` entry is
                    // also swept on its own iteration).
                    if stale_tmp && !name.ends_with(".lock") {
                        let _ = std::fs::remove_file(staging_lock_path(&path));
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => warn!(
                    path = %path.display(),
                    %error,
                    "failed to remove stale golden artifact"
                ),
            }
        }
        if swept > 0 {
            info!(swept, directory = %directory.display(), "swept stale golden artifacts");
        }
    }
}
/// Grace window for a staging-file companion lock: the writer refreshes the
/// lock's mtime every heartbeat, so a lock older than this has no live writer
/// (crash between `pack()`/`download()` and `rename()`).
const STAGING_LOCK_GRACE: std::time::Duration = std::time::Duration::from_secs(120);
/// How often a live writer refreshes its staging lock.
const STAGING_LOCK_HEARTBEAT: std::time::Duration = std::time::Duration::from_secs(30);

/// Companion lock path for a `.tmp-golden-*` staging file.
fn staging_lock_path(staging: &Path) -> PathBuf {
    PathBuf::from(format!("{}.lock", staging.display()))
}

/// Whether a companion lock evidences a live writer.
///
/// A missing lock means no writer ever claimed the file (or it already
/// finished); a lock younger than the grace window means a heartbeat is
/// refreshing it. An unreadable mtime spares the file: deleting on unknown
/// age risks removing a live bake after a clock adjustment.
fn staging_lock_is_fresh(lock: &Path) -> bool {
    let Ok(mtime) = std::fs::metadata(lock).and_then(|meta| meta.modified()) else {
        return false;
    };
    match mtime.elapsed() {
        Ok(age) => age < STAGING_LOCK_GRACE,
        Err(_) => true,
    }
}

/// Heartbeated claim on a staging file's companion lock.
///
/// Creating the guard writes the lock file; a background task refreshes its
/// mtime until the guard drops, so a concurrent pool's startup sweep sees a
/// fresh lock and spares the in-flight bake or download. Dropping aborts the
/// heartbeat and removes the lock file.
struct StagingLockGuard {
    lock: PathBuf,
    heartbeat: Option<tokio::task::JoinHandle<()>>,
}

impl StagingLockGuard {
    fn claim(lock: PathBuf) -> Self {
        let _ = std::fs::File::create(&lock);
        let heartbeat_lock = lock.clone();
        let heartbeat = tokio::spawn(async move {
            loop {
                tokio::time::sleep(STAGING_LOCK_HEARTBEAT).await;
                if std::fs::File::create(&heartbeat_lock).is_err() {
                    break;
                }
            }
        });
        Self {
            lock,
            heartbeat: Some(heartbeat),
        }
    }
}

impl Drop for StagingLockGuard {
    fn drop(&mut self) {
        if let Some(handle) = self.heartbeat.take() {
            handle.abort();
        }
        let _ = std::fs::remove_file(&self.lock);
    }
}

/// A provisioned, registered runner waiting to be handed a job.
#[derive(Debug)]
struct ReadyRunner {
    name: MachineName,
    run: Vec<String>,
    environment: RunnerEnvironment,
}

/// Tell the control plane a machine's runner is gone, BEFORE the VM goes
/// away: the server purges the identity AND requeues any job the runner
/// claimed but never finished. Without this, a machine torn down mid-job
/// hangs that job until the 45-minute lease reaper marks it failed.
///
/// Fire-and-forget: machine deletion must not stall on control-plane
/// availability, and a missed purge only reverts to the old reaper path.
async fn notify_runner_gone(config: &RunnerPoolConfig, name: &MachineName) {
    let Some(token) = std::env::var(&config.registration_token_env)
        .ok()
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    let url = format!(
        "{}/api/v1/runners/purge",
        config.server_url.trim_end_matches('/')
    );
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(1500))
        .build()
    {
        Ok(client) => client,
        Err(_) => return,
    };
    if let Err(error) = client
        .post(url)
        .bearer_auth(token)
        .json(&serde_json::json!({ "name": name.as_str() }))
        .send()
        .await
    {
        warn!(machine = name.as_str(), %error, "runner purge notification failed");
    }
}

#[derive(Debug, Clone)]
struct RunnerEnvironment {
    /// Fingerprint of the golden this runner was forked from.
    fingerprint: Option<String>,
    /// Base image this runner actually booted from.
    base: String,
    /// Toolchains this runner must carry (installed after boot when the
    /// runner is created fresh rather than forked from a prepared golden).
    toolchains: Vec<ToolchainLayer>,
    /// Whether Preloop's curated bake applies to this base. Custom base
    /// images are used as-is and must not receive the apt/toolchain bake.
    curated: bool,
}

/// Handles every slot in the pool shares.
#[derive(Clone)]
struct PoolHandles {
    /// Runners across the whole pool that are registered and unclaimed.
    idle: Arc<std::sync::Mutex<usize>>,
    /// Keypairs generated ahead of time for runner registration.
    keys: Arc<KeyPool>,
    /// Replacements currently being built across the whole pool.
    building: Arc<AtomicUsize>,
    /// Provisions in flight across the whole pool. Raised so the server's
    /// starvation sweep keeps the queued-job grace clock paused while any
    /// warm-mode slot is still booting its runner.
    provisioning: Arc<std::sync::Mutex<usize>>,
}

/// What a slot needs in order to build its next runner.
struct SlotPlan<'a> {
    /// Pool slot index, used to name machines.
    slot: usize,
    /// Generation for the replacement machine name.
    generation: u64,
    /// Fork base, when the pool has one.
    golden: Option<&'a MachineName>,
    /// Environment selected for the replacement.
    environment: RunnerEnvironment,
    /// Runners across the whole pool that are registered and unclaimed.
    idle: &'a std::sync::Mutex<usize>,
    /// Keypairs generated ahead of time for runner registration.
    keys: &'a Arc<KeyPool>,
    /// Replacements currently being built across the whole pool.
    building: &'a AtomicUsize,
    /// Provisions in flight across the whole pool (see `PoolHandles`).
    provisioning: &'a Arc<std::sync::Mutex<usize>>,
    /// Whether this slot keeps a warm successor after the current job.
    prebuild_successor: bool,
}

/// A claim on one of the replacement builds the backlog justifies.
///
/// Held for the duration of the build so concurrent slots see it, and released
/// on drop so an error path cannot strand the count.
struct Reservation<'a> {
    building: &'a AtomicUsize,
    pool_status: Option<Arc<preloop_observability::status::PoolStatus>>,
}

impl<'a> Reservation<'a> {
    /// Claim a build slot, or `None` when `wanted` are already in flight.
    fn take(
        building: &'a AtomicUsize,
        wanted: usize,
        pool_status: Option<Arc<preloop_observability::status::PoolStatus>>,
    ) -> Option<Self> {
        let mut current = building.load(Ordering::Acquire);
        loop {
            if current >= wanted {
                return None;
            }
            match building.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Publish the counter's current value, not the pre-CAS
                    // `current + 1`: a concurrent reservation may have
                    // incremented again in between, and publishing the stale
                    // computed value would under-report in-flight builds.
                    if let Some(ps) = &pool_status {
                        ps.set_building(building.load(Ordering::Acquire) as u32);
                    }
                    return Some(Self {
                        building,
                        pool_status,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.building.fetch_sub(1, Ordering::AcqRel);
        // Re-read after the decrement: a concurrent take or drop may have
        // changed the counter again, and publishing the value captured at
        // this reservation's own decrement could overwrite a newer status
        // update with an older one.
        if let Some(ps) = &self.pool_status {
            ps.set_building(self.building.load(Ordering::Acquire) as u32);
        }
    }
}

/// Keeps the server's starvation clock paused while any on-demand runner is
/// still being provisioned. A counter is required because size-zero mode can
/// create several runners concurrently; the first completed runner must not
/// clear the signal while another job's runner is still bootstrapping.
struct PreparingGuard {
    active: Arc<std::sync::Mutex<usize>>,
    signal: Option<Arc<std::sync::atomic::AtomicBool>>,
    pool_status: Option<Arc<preloop_observability::status::PoolStatus>>,
}

impl PreparingGuard {
    fn enter(
        active: Arc<std::sync::Mutex<usize>>,
        signal: Option<Arc<std::sync::atomic::AtomicBool>>,
        pool_status: Option<Arc<preloop_observability::status::PoolStatus>>,
    ) -> Self {
        // The counter mutation and the status publication share one lock, so
        // a concurrent enter/drop can never publish an older count over a
        // newer one: `snapshot().provisioning` always matches the live
        // counter and cannot report a runner that no longer exists.
        let mut count = active.lock().unwrap();
        if *count == 0
            && let Some(signal) = &signal
        {
            signal.store(true, Ordering::Release);
        }
        *count += 1;
        if let Some(ps) = &pool_status {
            ps.set_provisioning(*count as u32);
        }
        drop(count);
        Self {
            active,
            signal,
            pool_status,
        }
    }
}

impl Drop for PreparingGuard {
    fn drop(&mut self) {
        let mut count = self.active.lock().unwrap();
        *count = count.saturating_sub(1);
        if *count == 0
            && let Some(signal) = &self.signal
        {
            signal.store(false, Ordering::Release);
        }
        if let Some(ps) = &self.pool_status {
            ps.set_provisioning(*count as u32);
        }
    }
}

/// Clears the pool's preparing flag on drop unless startup completed.
///
/// Any startup step failing — host externals, artifact prep, stale-machine
/// cleanup, golden bake — returns early, and without this the flag would
/// stay `true` forever, marking every queued job unclaimable in the
/// operational snapshot. Disarmed once the warm completes.
struct ClearPreparingOnDrop {
    signal: Option<Arc<std::sync::atomic::AtomicBool>>,
    pool_status: Option<Arc<preloop_observability::status::PoolStatus>>,
    armed: bool,
}

impl Drop for ClearPreparingOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(ps) = &self.pool_status {
            ps.set_preparing(false);
        }
        if let Some(signal) = &self.signal {
            signal.store(false, std::sync::atomic::Ordering::Release);
        }
    }
}

/// How long an unattached AgentENV debug VM stays running before suspension.
const DEBUG_SUSPEND_IDLE: Duration = Duration::from_secs(15);

/// How often the pool probes a running machine's pause marker.
///
/// Latency here is how long a slot stays pinned after a job pauses: the
/// probe cadence bounds it, and one exec per interval per active machine is
/// negligible against the guest work happening anyway.
const PAUSE_POLL_INTERVAL: Duration = Duration::from_secs(2);
/// Watch a machine's guest pause marker and release its pool concurrency
/// permit for the duration of a debug-session pause.
///
/// A paused job blocks its worker on a verdict, so the host-side slot task
/// keeps waiting for the runner to exit and the slot's permit stays held —
/// with `max_concurrent` permits in total, two unanswered pauses take the
/// pool to zero and every later run queues forever. The worker writes
/// [`GUEST_PAUSE_MARKER`] when a session opens and removes it when it
/// closes; this hands the permit back while the marker is present and
/// re-acquires it on resume. Runs forever; the caller aborts it.
#[cfg(test)]
async fn watch_guest_pause<P: VmProvider + 'static>(
    provider: Arc<P>,
    name: MachineName,
    permit: Arc<std::sync::Mutex<Option<tokio::sync::OwnedSemaphorePermit>>>,
    semaphore: Arc<tokio::sync::Semaphore>,
    poll_interval: Duration,
) {
    watch_guest_pause_with_suspension(
        provider,
        name,
        permit,
        semaphore,
        None,
        Duration::ZERO,
        poll_interval,
    )
    .await;
}

/// Watch a debug pause and, for a state-preserving provider, suspend the VM
/// while no controller is attached. The host marker is deliberately separate
/// from the guest pause marker: the latter becomes unreachable once AgentENV
/// pauses the sandbox.
async fn watch_guest_pause_with_suspension<P: VmProvider + 'static>(
    provider: Arc<P>,
    name: MachineName,
    permit: Arc<std::sync::Mutex<Option<tokio::sync::OwnedSemaphorePermit>>>,
    semaphore: Arc<tokio::sync::Semaphore>,
    debug_dir: Option<PathBuf>,
    suspend_after: Duration,
    poll_interval: Duration,
) {
    let probe = [
        "test".to_owned(),
        "-f".to_owned(),
        GUEST_PAUSE_MARKER.to_owned(),
    ];
    let can_suspend = suspend_after > Duration::ZERO
        && debug_dir.is_some()
        && provider.capabilities().preserves_runtime_state_on_suspend;
    let mut was_paused = false;
    let mut paused_since: Option<tokio::time::Instant> = None;
    let mut suspended = false;
    let mut last_probe_warn: Option<tokio::time::Instant> = None;
    loop {
        tokio::time::sleep(poll_interval).await;

        // Once the VM is suspended, the guest marker cannot be probed. An
        // active host marker is the attach request and is the only signal
        // needed to resume it.
        if suspended {
            if debug_marker_active(debug_dir.as_deref(), &name) {
                match provider.start(&name).await {
                    Ok(()) => {
                        suspended = false;
                        info!(
                            machine = name.as_str(),
                            "debug controller attached — resumed VM"
                        );
                    }
                    Err(error) => {
                        warn!(
                            machine = name.as_str(),
                            %error,
                            "could not resume suspended debug VM"
                        );
                    }
                }
            }
            continue;
        }

        // `smolvm machine exec` propagates the guest exit code as its own exit
        // code, so the normal absent-marker probe surfaces as `VmError::Command`
        // with exit 1 — a real result, not a transport failure.
        let paused = match provider.exec(&name, &probe).await {
            Ok(output) => output.exit_code == 0,
            Err(VmError::Command {
                exit_code: code @ (0 | 1),
                ..
            }) => code == 0,
            Err(error) => {
                let now = tokio::time::Instant::now();
                if last_probe_warn
                    .is_none_or(|last| now.duration_since(last) >= Duration::from_secs(60))
                {
                    warn!(
                        machine = name.as_str(),
                        %error,
                        "pause marker probe failed — keeping previous state"
                    );
                    last_probe_warn = Some(now);
                }
                was_paused
            }
        };

        if paused && !was_paused {
            paused_since = Some(tokio::time::Instant::now());
            if let Some(debug_dir) = debug_dir.as_deref() {
                let marker = debug_dir.join(name.as_str());
                if !marker.exists()
                    && let Err(error) = std::fs::create_dir_all(debug_dir)
                        .and_then(|()| std::fs::write(&marker, DEBUG_MARKER_IDLE))
                {
                    warn!(
                        machine = name.as_str(),
                        path = %marker.display(),
                        %error,
                        "could not create debug attach marker"
                    );
                }
            }
        } else if !paused {
            paused_since = None;
        }

        if paused {
            if !was_paused {
                let released = { permit.lock().unwrap().take() }.is_some();
                if released {
                    info!(
                        machine = name.as_str(),
                        "job paused in debug session — released pool concurrency permit"
                    );
                }
            }

            let attached = debug_marker_active(debug_dir.as_deref(), &name);
            if can_suspend
                && !attached
                && paused_since.is_some_and(|started| started.elapsed() >= suspend_after)
            {
                match provider.stop(&name).await {
                    Ok(()) => {
                        suspended = true;
                        info!(
                            machine = name.as_str(),
                            idle_secs = suspend_after.as_secs(),
                            "debug VM suspended while unattached"
                        );
                    }
                    Err(error) => warn!(
                        machine = name.as_str(),
                        %error,
                        "could not suspend idle debug VM"
                    ),
                }
            }
        } else if was_paused {
            let started = tokio::time::Instant::now();
            let fresh = semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore is never closed");
            let waited = started.elapsed();
            if waited >= Duration::from_secs(5) {
                warn!(
                    machine = name.as_str(),
                    waited_ms = waited.as_millis(),
                    "resumed job waited for a pool permit — active VMs may have transiently exceeded max_concurrent"
                );
            }
            permit.lock().unwrap().replace(fresh);
            info!(
                machine = name.as_str(),
                "debug session ended — re-acquired pool concurrency permit"
            );
        }
        was_paused = paused;
    }
}

fn debug_marker_active(debug_dir: Option<&Path>, name: &MachineName) -> bool {
    let Some(debug_dir) = debug_dir else {
        return false;
    };
    let marker = debug_dir.join(name.as_str());
    std::fs::read_to_string(&marker).ok().is_some_and(|state| {
        state.trim() == DEBUG_MARKER_ACTIVE
            && std::fs::metadata(marker)
                .and_then(|meta| meta.modified())
                .ok()
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age < DEBUG_HEARTBEAT_WINDOW)
    })
}

/// Single-shot on-demand runner: provision, run exactly one job, clean up.
#[allow(clippy::too_many_arguments)]
async fn run_on_demand_slot<P: VmProvider + 'static>(
    provider: Arc<P>,
    config: RunnerPoolConfig,
    slot: usize,
    shutdown: CancellationToken,
    golden_registry: Arc<GoldenRegistry>,
    handles: PoolHandles,
    _slot_provisioning: Arc<std::sync::Mutex<usize>>,
    semaphore: Arc<tokio::sync::Semaphore>,
    permit: Arc<std::sync::Mutex<Option<tokio::sync::OwnedSemaphorePermit>>>,
) -> Result<(), OrchestratorError> {
    // Mirror `run_slot`: once the packed golden is known-unusable, on-demand
    // slots must not attempt another environment-golden bake either — fall
    // back to direct per-runner creation the same way.
    let mut config = config;
    if golden_registry.is_packed_disabled() {
        config.use_packed_artifact = false;
        config.use_fork = false;
    }
    let provisioning = handles.provisioning.clone();
    let preparing = PreparingGuard::enter(
        provisioning.clone(),
        config.preparing_signal.clone(),
        config.pool_status.clone(),
    );
    // Resolve the golden for the queued job's environment.
    let (golden, environment) = if config.use_fork {
        let env_base = match &config.next_job_runs_on {
            Some(lock) => {
                let labels = lock.read().map(|g| g.clone()).unwrap_or_default();
                EnvironmentSpec::base_for_labels(&labels, &config.base_image)
            }
            None => config.base_image.clone(),
        };
        let env_spec = EnvironmentSpec::for_base(env_base.clone());
        let curated = env_spec.curated;
        let fingerprint = env_spec.fingerprint.clone();
        let toolchains = env_spec.toolchains.clone();
        let selected = golden_registry
            .get_or_prepare(&fingerprint, {
                let provider = provider.clone();
                let config = config.clone();
                let name_prefix = golden_registry.name_prefix().to_owned();
                let fp = fingerprint.clone();
                async move {
                    let name = MachineName::new(format!(
                        "{}-golden-{}",
                        name_prefix,
                        &fp[..12.min(fp.len())]
                    ))?;
                    prepare_golden_for_env(&provider, &config, &name, &env_spec).await?;
                    Ok(name)
                }
            })
            .await
            .map_err(|error| {
                warn!(%error, %fingerprint, "failed to prepare golden for on-demand runner");
                error
            })?;
        (
            Some(selected),
            RunnerEnvironment {
                fingerprint: Some(fingerprint),
                base: env_base,
                toolchains,
                curated,
            },
        )
    } else {
        let env_spec = EnvironmentSpec::for_base(config.base_image.clone());
        let curated = env_spec.curated;
        (
            None,
            RunnerEnvironment {
                fingerprint: None,
                base: env_spec.base.clone(),
                toolchains: env_spec.toolchains,
                curated,
            },
        )
    };

    // Provision a single-use runner.
    let generation = 1_u64;
    let Some(runner) = provision_slot(
        &provider,
        &config,
        slot,
        generation,
        golden.as_ref(),
        &handles.keys,
        environment.clone(),
        &shutdown,
    )
    .await?
    else {
        return Ok(());
    };
    // `provision_slot` returns only after runner registration succeeds. From
    // this point the starvation sweep can see a matching runner directly.
    drop(preparing);

    // Run exactly one job — no successor pre-provisioning. While the job
    // runs, watch the guest pause marker: a debug-session pause must hand
    // the concurrency permit back to the pool instead of pinning it.
    let pause_watch = config.debug_dir.clone().map(|debug_dir| {
        let provider = provider.clone();
        let name = runner.name.clone();
        let permit = permit.clone();
        let semaphore = semaphore.clone();
        tokio::spawn(watch_guest_pause_with_suspension(
            provider,
            name,
            permit,
            semaphore,
            Some(debug_dir),
            DEBUG_SUSPEND_IDLE,
            PAUSE_POLL_INTERVAL,
        ))
    });
    let result = run_one_runner(
        provider.clone(),
        &config,
        runner,
        shutdown,
        SlotPlan {
            slot,
            generation: generation + 1,
            golden: golden.as_ref(),
            environment,
            idle: &handles.idle,
            keys: &handles.keys,
            building: &handles.building,
            provisioning: &handles.provisioning,
            prebuild_successor: false,
        },
    )
    .await;
    if let Some(watch) = pause_watch {
        watch.abort();
        let _ = watch.await;
    }

    // Size-zero mode never asks for a successor. Keep defensive cleanup here
    // so a future lifecycle change cannot leak an unexpectedly returned VM.
    match result {
        Ok(Some(successor)) => {
            notify_runner_gone(&config, &successor.name).await;
            vm_telemetry_deregister(&config, &successor.name);
            let _ = provider.delete(&successor.name).await;
            Ok(())
        }
        Ok(None) => Ok(()),
        Err(error) => Err(error),
    }
}

async fn run_slot<P: VmProvider + 'static>(
    provider: Arc<P>,
    config: RunnerPoolConfig,
    slot: usize,
    shutdown: CancellationToken,
    golden_registry: Arc<GoldenRegistry>,
    handles: PoolHandles,
) -> Result<(), OrchestratorError> {
    let mut config = config;
    if golden_registry.is_packed_disabled() {
        config.use_packed_artifact = false;
        config.use_fork = false;
    }
    let PoolHandles {
        idle,
        keys,
        building,
        provisioning,
    } = handles;
    let mut generation: u64 = 0;
    let mut spare: Option<ReadyRunner> = None;
    let mut golden_backoff: Option<Duration> = None;

    while !shutdown.is_cancelled() {
        // Warm mode cleared the pool's preparing flag once the golden bake
        // finished, but this slot still has to resolve (and possibly bake)
        // the golden for the queued job's environment and then
        // fork+boot+register its first runner -- minutes with no runner
        // registered. Hold the shared provisioning guard across that whole
        // window, golden preparation included, so the server's starvation
        // sweep keeps the queued-job grace clock paused; drop it once a
        // registered runner is in hand.
        let preparing = PreparingGuard::enter(
            provisioning.clone(),
            config.preparing_signal.clone(),
            config.pool_status.clone(),
        );
        let (golden, environment) = if config.use_fork {
            // Read the `runs-on` labels of the next queued job so the pool
            // can select the correct base-image golden before forking.
            let env_base = match &config.next_job_runs_on {
                Some(lock) => {
                    let labels = lock.read().map(|g| g.clone()).unwrap_or_default();
                    EnvironmentSpec::base_for_labels(&labels, &config.base_image)
                }
                None => config.base_image.clone(),
            };
            // The golden carries the curated toolchain set; base image still
            // comes from the queued job's `runs-on` labels.
            let env_spec = EnvironmentSpec::for_base(env_base.clone());
            let curated = env_spec.curated;
            let fingerprint = env_spec.fingerprint.clone();
            let toolchains = env_spec.toolchains.clone();

            let selected = match golden_registry
                .get_or_prepare(&fingerprint, {
                    let provider = provider.clone();
                    let config = config.clone();
                    let name_prefix = golden_registry.name_prefix().to_owned();
                    let fp = fingerprint.clone();
                    async move {
                        let name = MachineName::new(format!(
                            "{}-golden-{}",
                            name_prefix,
                            &fp[..12.min(fp.len())]
                        ))?;
                        prepare_golden_for_env(&provider, &config, &name, &env_spec).await?;
                        Ok(name)
                    }
                })
                .await
            {
                Ok(name) => {
                    golden_backoff = None;
                    Some(name)
                }
                Err(error) => {
                    // A bake failure is usually deterministic (a stock apt
                    // pin the archive dropped, a registry the guest cannot
                    // resolve), so a fixed 500 ms retry meant this slot
                    // rebuilt the same doomed golden ~100 times an hour --
                    // each attempt boots a VM and runs apt -- while the
                    // queue it was meant to drain starved. Back off
                    // geometrically, capped, so a slot costs one attempt per
                    // minute at worst and recovers immediately once the
                    // environment becomes buildable again.
                    let wait = golden_backoff.map_or(GOLDEN_RETRY_MIN, |last: Duration| {
                        (last * 2).min(GOLDEN_RETRY_MAX)
                    });
                    golden_backoff = Some(wait);
                    warn!(
                        %error,
                        %fingerprint,
                        retry_in_ms = wait.as_millis(),
                        "failed to prepare requested environment golden; leaving job queued"
                    );
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = tokio::time::sleep(wait) => {}
                    }
                    continue;
                }
            };
            (
                selected,
                RunnerEnvironment {
                    fingerprint: Some(fingerprint),
                    base: env_base,
                    toolchains,
                    curated,
                },
            )
        } else {
            // create-per-runner path: no golden, provision fresh each time.
            let env_spec = EnvironmentSpec::for_base(config.base_image.clone());
            let curated = env_spec.curated;
            (
                None,
                RunnerEnvironment {
                    fingerprint: None,
                    base: env_spec.base.clone(),
                    toolchains: env_spec.toolchains,
                    curated,
                },
            )
        };

        // A spare forked from a different environment would run the job on
        // the wrong base image. Discard it and provision against the golden
        // this iteration actually selected.
        if let Some(ready) = spare.take() {
            if ready.environment.fingerprint == environment.fingerprint {
                spare = Some(ready);
            } else {
                warn!(
                    slot,
                    "discarding spare runner built for a different environment"
                );
                vm_telemetry_deregister(&config, &ready.name);
                let _ = provider.delete(&ready.name).await;
            }
        }

        let runner = match spare.take() {
            Some(runner) => runner,
            None => {
                generation += 1;
                match provision_slot(
                    &provider,
                    &config,
                    slot,
                    generation,
                    golden.as_ref(),
                    &keys,
                    environment.clone(),
                    &shutdown,
                )
                .await
                {
                    Ok(Some(runner)) => runner,
                    // The disk reserve held the start until shutdown.
                    Ok(None) => break,
                    Err(error) => {
                        warn!(slot, %error, "provisioning runner failed; retrying");
                        tokio::select! {
                            _ = shutdown.cancelled() => break,
                            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                        }
                        continue;
                    }
                }
            }
        };

        // Runner is registered (or an already-registered spare); the sweep
        // can see a matching runner directly now, so resume its grace clock.
        drop(preparing);

        generation += 1;
        let successor = run_one_runner(
            provider.clone(),
            &config,
            runner,
            shutdown.clone(),
            SlotPlan {
                slot,
                generation,
                golden: golden.as_ref(),
                environment: environment.clone(),
                idle: &idle,
                keys: &keys,
                building: &building,
                provisioning: &provisioning,
                prebuild_successor: true,
            },
        )
        .await;
        spare = match successor {
            Ok(spare) => spare,
            Err(error) => {
                warn!(slot, %error, "ephemeral runner failed; replenishing slot");
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                }
                None
            }
        };
    }

    if let Some(spare) = spare {
        notify_runner_gone(&config, &spare.name).await;
        vm_telemetry_deregister(&config, &spare.name);
        let _ = provider.delete(&spare.name).await;
    }
    Ok(())
}

/// Provision one ephemeral runner for a slot under a fresh machine name.
///
/// Names carry a generation so a replacement can boot while its predecessor is
/// still being torn down; reusing one name per slot forced those to serialize.
///
/// `Ok(None)` means the pool is stopping: the disk reserve held the start and
/// `shutdown` fired while the slot waited, so no VM was created and there is
/// nothing to account for.
#[allow(clippy::too_many_arguments)]
async fn provision_slot<P: VmProvider + 'static>(
    provider: &Arc<P>,
    config: &RunnerPoolConfig,
    slot: usize,
    generation: u64,
    golden: Option<&MachineName>,
    keys: &Arc<KeyPool>,
    environment: RunnerEnvironment,
    shutdown: &CancellationToken,
) -> Result<Option<ReadyRunner>, OrchestratorError> {
    // Refuse to start a job VM on a volume without room for it. Waiting here
    // (rather than in `provision_runner`) keeps every path that forks or
    // creates a runner behind one check.
    if !wait_for_vm_disk(config, shutdown).await {
        debug!(slot, "pool stopping; abandoning runner provisioning");
        return Ok(None);
    }
    let name = match MachineName::new(format!("{}-{slot}-{generation}", config.name_prefix)) {
        Ok(name) => name,
        Err(error) => {
            record_slot_failure(config, "provision");
            return Err(error.into());
        }
    };
    match provision_runner(provider, config, &name, golden, keys, &environment).await {
        Ok(run) => {
            // A provision that made it (fork or direct create, configure,
            // registration) resets the consecutive-failure streak. The
            // counter feeds `pool_repeated_provision_failure` and the
            // `consecutive_provision_failures` status field.
            if let Some(ps) = &config.pool_status {
                ps.clear_provision_failures();
            }
            Ok(Some(ReadyRunner {
                name,
                run,
                environment,
            }))
        }
        Err(error) => {
            record_slot_failure(config, "provision");
            // A configure failure can happen after the guest has already
            // registered the runner. Purge by machine name before deleting
            // the VM so that registration cannot outlive its provisioned
            // host and retain a live listen credential.
            notify_runner_gone(config, &name).await;
            if let Some(ps) = &config.pool_status {
                ps.record_provision_failure();
            }
            vm_telemetry_deregister(config, &name);
            if let Err(cleanup) = provider.delete(&name).await {
                warn!(
                    machine = name.as_str(),
                    %cleanup,
                    "failed to delete machine after provisioning error"
                );
            }
            Err(error)
        }
    }
}

fn runner_environment_labels(base: &str) -> Vec<String> {
    let normalized = base.to_ascii_lowercase();
    if normalized.contains("22.04") {
        vec!["ubuntu-22.04".to_owned()]
    } else if normalized.contains("24.04") {
        vec!["ubuntu-24.04".to_owned(), "ubuntu-latest".to_owned()]
    } else {
        Vec::new()
    }
}

async fn wait_for_environment_change(
    config: &RunnerPoolConfig,
    current_base: &str,
    claimed: Arc<std::sync::atomic::AtomicBool>,
) {
    let Some(next_job_runs_on) = &config.next_job_runs_on else {
        std::future::pending::<()>().await;
        return;
    };
    // Grace before reaping: a freshly-paired machine is not idle — the broker
    // assigns jobs before the guest runner has even received the request, and
    // the guest announces its claim on stdout moments later. Reaping on the
    // first observed mismatch killed machines mid-claim and requeued jobs
    // forever under mixed environments. Require the mismatch to persist across
    // a few checks so only genuinely idle runners are recycled.
    const REAP_GRACE_CHECKS: u32 = 25; // ~2.5 s at the 100 ms cadence below.
    let mut mismatch_checks = 0u32;
    loop {
        if claimed.load(Ordering::Acquire) {
            std::future::pending::<()>().await;
            return;
        }
        let labels = next_job_runs_on
            .read()
            .map(|labels| labels.clone())
            .unwrap_or_default();
        if !labels.is_empty()
            && EnvironmentSpec::base_for_labels(&labels, &config.base_image) != current_base
        {
            mismatch_checks += 1;
            if mismatch_checks >= REAP_GRACE_CHECKS {
                return;
            }
        } else {
            mismatch_checks = 0;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Run one job on a provisioned runner, building its replacement in parallel.
///
/// The runner is single-use, so the moment it announces that it has taken a
/// job its successor can start booting. That moves fork + configure — the bulk
/// of a slot's turnaround — off the path of whatever job arrives next, which is
/// what a matrix workflow deeper than the pool spends its time waiting on.
///
/// Returns the replacement when one was built, so the caller can use it
/// immediately instead of provisioning again.
async fn run_one_runner<P: VmProvider + 'static>(
    provider: Arc<P>,
    config: &RunnerPoolConfig,
    runner: ReadyRunner,
    shutdown: CancellationToken,
    plan: SlotPlan<'_>,
) -> Result<Option<ReadyRunner>, OrchestratorError> {
    let ReadyRunner {
        name,
        run,
        environment,
    } = runner;
    let name = &name;
    let SlotPlan {
        slot,
        generation: next_generation,
        golden,
        environment: successor_environment,
        idle,
        keys,
        building,
        provisioning,
        prebuild_successor,
    } = plan;

    let (busy_tx, busy_rx) = tokio::sync::oneshot::channel();
    // The runner's completion is observed through this oneshot, never by
    // re-polling the JoinHandle: `tokio::join!(&mut run_task, successor)`
    // panics with "JoinHandle polled after completion" when the runner exits
    // before the successor finishes provisioning and `select!` re-polls the
    // branch — a completed `&mut JoinHandle` cannot be polled again.
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let claimed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let run_provider = provider.clone();
    let run_name = name.clone();
    // The counter mutation and the status publication share one lock, so a
    // concurrent claim can never publish an older count over a newer one:
    // `snapshot().idle` cannot report an idle runner that was already
    // claimed.
    {
        let mut idle = idle.lock().unwrap();
        *idle += 1;
        if let Some(ps) = &config.pool_status {
            ps.set_idle(*idle as u32);
        }
    }
    let run_task = tokio::spawn(async move {
        let result = run_until_exit(&run_provider, &run_name, &run, busy_tx, GUEST_LIVENESS).await;
        let _ = done_tx.send(result);
    });

    // Resolves once the runner reports a job and its replacement is ready. A
    // runner that exits without taking a job (shutdown, transient failure)
    // drops the sender, and this yields `None` without provisioning anything.
    let pending_jobs = config.pending_jobs.as_deref();
    let successor_claimed = claimed.clone();
    let build_successor = async {
        if busy_rx.await.is_err() {
            {
                let mut idle = idle.lock().unwrap();
                *idle = idle.saturating_sub(1);
                if let Some(ps) = &config.pool_status {
                    ps.set_idle(*idle as u32);
                }
            }
            return None;
        }
        successor_claimed.store(true, Ordering::Release);
        let idle_after = {
            let mut idle = idle.lock().unwrap();
            *idle = idle.saturating_sub(1);
            if let Some(ps) = &config.pool_status {
                ps.set_idle(*idle as u32);
            }
            *idle
        };
        if !prebuild_successor {
            return None;
        }
        // Booting a VM costs real CPU, and it would be spent alongside the job
        // that just started, so build exactly as many replacements as the
        // backlog needs and no more.
        // absorb. Every claiming slot computes it, so a reservation counter
        // decides which of them actually build: without it, a matrix one job
        // wider than the pool had all four slots boot a replacement to serve a
        // single straggler, and the contention cost more than the wait.
        let queued = pending_jobs.map_or(0, |pending| pending.load(Ordering::Acquire));
        // With nothing queued still keep one runner coming, so the pool is not
        // empty for whatever arrives next.
        let wanted = queued
            .saturating_sub(idle_after)
            .max(usize::from(idle_after == 0));
        let _reservation = Reservation::take(building, wanted, config.pool_status.clone())?;
        // The successor boot runs alongside the job; while it is in flight a
        // matching runner may not be registered yet (all slots busy). Hold
        // the shared provisioning guard so the server's starvation sweep
        // keeps the queued-job grace clock paused for the boot duration.
        let preparing = PreparingGuard::enter(
            provisioning.clone(),
            config.preparing_signal.clone(),
            config.pool_status.clone(),
        );
        match provision_slot(
            &provider,
            config,
            slot,
            next_generation,
            golden,
            keys,
            successor_environment,
            &shutdown,
        )
        .await
        {
            Ok(successor) => {
                drop(preparing);
                // `None` means the disk reserve held the start until shutdown:
                // the slot keeps its current job and stops replenishing.
                successor
            }
            Err(error) => {
                drop(preparing);
                warn!(slot, %error, "pre-provisioning the replacement runner failed");
                None
            }
        }
    };

    let (result, successor) = tokio::select! {
        _ = shutdown.cancelled() => {
            // Killing the host-side `smolvm machine exec` process does not
            // terminate the guest command. Abort the wrapper first, then stop
            // the VM so deletion cannot wait indefinitely on a live listener.
            run_task.abort();
            let _ = run_task.await;
            (provider.stop(name).await.map_err(OrchestratorError::from), None)
        },
        _ = wait_for_environment_change(config, &environment.base, claimed) => {
            run_task.abort();
            let _ = run_task.await;
            {
                let mut idle = idle.lock().unwrap();
                *idle = idle.saturating_sub(1);
                if let Some(ps) = &config.pool_status {
                    ps.set_idle(*idle as u32);
                }
            }
            info!(machine = name.as_str(), environment = %environment.base, "replacing idle runner for queued environment");
            (provider.stop(name).await.map_err(OrchestratorError::from), None)
        },
        pair = async {
            // Concurrent on purpose: the successor is built while the job is
            // still running, which is the whole point of the busy signal. The
            // oneshot is polled once by value, so a runner that exits before
            // the successor is ready cannot be re-polled into a panic.
            tokio::join!(done_rx, build_successor)
        } => {
            let result = match pair.0 {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => {
                    // `run_until_exit` turns a non-zero guest exit into this
                    // exact command error. Provider launch/transport failures
                    // and task panics are engine failures, not guest exits.
                    if matches!(
                        error,
                        VmError::Command {
                            operation: "run",
                            ..
                        }
                    ) {
                        record_slot_failure(config, "guest_exit");
                    }
                    Err(OrchestratorError::Pool(error.to_string()))
                }
                // The runner task panicked (sender dropped without a value).
                Err(_) => Err(OrchestratorError::Pool(
                    "runner task ended without a result".into(),
                )),
            };
            (result, pair.1)
        },
    };

    // The runner writes this marker only when the job it ran opted in via
    // `preserve_on_failure` and then genuinely failed, so preservation is
    // decided per run rather than by engine-wide configuration. The probe is
    // bounded: a wedged guest never answers, and waiting on it would pin the
    // slot exactly as an unbounded runner stream did.
    let preserved = match &config.debug_dir {
        Some(debug_dir)
            if matches!(
                tokio::time::timeout(
                    GUEST_LIVENESS.timeout,
                    provider.exec(
                        name,
                        &["test".into(), "-f".into(), GUEST_FAILURE_MARKER.into()],
                    ),
                )
                .await,
                Ok(Ok(_))
            ) =>
        {
            Some(debug_dir.clone())
        }
        _ => None,
    };

    if let Some(debug_dir) = preserved {
        hold_for_debugging(&provider, name, &debug_dir, &shutdown).await;
        notify_runner_gone(config, name).await;
        vm_telemetry_deregister(config, name);
        if let Err(error) = provider.delete(name).await {
            warn!(machine = name.as_str(), %error, "failed to delete preserved machine");
        }
        return finish(&provider, config, result, successor).await;
    }

    // Report the runner's own failure in preference to a teardown failure.
    notify_runner_gone(config, name).await;
    vm_telemetry_deregister(config, name);
    let delete_result = provider.delete(name).await.map_err(OrchestratorError::from);
    finish(&provider, config, result.and(delete_result), successor).await
}

/// Hand the replacement back, or discard it if this runner is failing.
///
/// A pre-provisioned successor owns a live VM. Returning early on the runner's
/// error would drop the handle and strand that machine until the pool next
/// swept stale names, so failure paths delete it explicitly.
async fn finish<P: VmProvider + 'static>(
    provider: &Arc<P>,
    config: &RunnerPoolConfig,
    result: Result<(), OrchestratorError>,
    successor: Option<ReadyRunner>,
) -> Result<Option<ReadyRunner>, OrchestratorError> {
    match result {
        Ok(()) => Ok(successor),
        Err(error) => {
            if let Some(successor) = successor {
                notify_runner_gone(config, &successor.name).await;
                vm_telemetry_deregister(config, &successor.name);
                if let Err(cleanup) = provider.delete(&successor.name).await {
                    warn!(
                        machine = successor.name.as_str(),
                        %cleanup,
                        "failed to delete the replacement runner of a failed slot"
                    );
                }
            }
            Err(error)
        }
    }
}

/// How the runner stream checks that its guest is still alive.
#[derive(Clone, Copy, Debug)]
struct GuestLiveness {
    /// Pause between probes.
    interval: std::time::Duration,
    /// A probe that takes longer than this counts as failed.
    timeout: std::time::Duration,
    /// Consecutive failed probes that mark the guest as wedged.
    failures: u32,
}

/// Three failed probes a minute apart: at least three minutes of silence,
/// matching the server's 180-second hung-worker cutoff, so a busy guest that
/// is merely slow to answer one probe is not torn down mid-job.
const GUEST_LIVENESS: GuestLiveness = GuestLiveness {
    interval: std::time::Duration::from_secs(60),
    timeout: std::time::Duration::from_secs(30),
    failures: 3,
};

/// Resolve once the guest has failed `liveness.failures` probes in a row.
async fn guest_unresponsive<P: VmProvider + 'static>(
    provider: &Arc<P>,
    name: &MachineName,
    liveness: GuestLiveness,
) -> VmError {
    let probe = ["true".to_owned()];
    let mut failures = 0;
    loop {
        tokio::time::sleep(liveness.interval).await;
        match tokio::time::timeout(liveness.timeout, provider.exec(name, &probe)).await {
            Ok(Ok(_)) => failures = 0,
            Ok(Err(_)) | Err(_) => failures += 1,
        }
        if failures >= liveness.failures {
            return VmError::GuestUnresponsive {
                machine: name.as_str().to_owned(),
                probes: failures,
            };
        }
    }
}

/// Run the guest runner to completion, signalling the first job it accepts.
///
/// Streaming rather than buffering the guest's output is what makes the busy
/// signal observable while the job is still running; it also drops SmolVM's
/// 30-second buffered-exec read timeout. A guest that wedges never closes the
/// stream, so the stream races a liveness probe: without it one wedged VM
/// pinned its pool slot for good.
async fn run_until_exit<P: VmProvider + 'static>(
    provider: &Arc<P>,
    name: &MachineName,
    run: &[String],
    busy: tokio::sync::oneshot::Sender<()>,
    liveness: GuestLiveness,
) -> Result<(), VmError> {
    let (chunks, mut receiver) = mpsc::channel(64);
    let machine = name.as_str().to_owned();
    let watcher = tokio::spawn(async move {
        let mut busy = Some(busy);
        let mut pending = String::new();
        // Guest output is the only window into the worker. Forwarding it to
        // tracing is what makes an in-VM failure diagnosable from the host;
        // consuming it purely to sniff for the busy sentinel meant every
        // worker-side decision was invisible.
        let mut line_buffer = String::new();
        while let Some(chunk) = receiver.recv().await {
            let (bytes, is_stdout) = match chunk {
                OutputChunk::Stdout(bytes) => (bytes, true),
                OutputChunk::Stderr(bytes) => (bytes, false),
            };
            line_buffer.push_str(&String::from_utf8_lossy(&bytes));
            // Cap retained tail to prevent unbounded growth from a guest
            // that never emits newlines (e.g. progress bar, binary output).
            const LINE_BUFFER_CAP: usize = 64 * 1024;
            if line_buffer.len() > LINE_BUFFER_CAP {
                // Round forward to a char boundary: `String::drain` panics
                // mid-codepoint, and a multi-byte char can straddle the cut.
                let mut drain = line_buffer.len() - LINE_BUFFER_CAP;
                while drain < line_buffer.len() && !line_buffer.is_char_boundary(drain) {
                    drain += 1;
                }
                line_buffer.drain(..drain);
            }
            while let Some(newline) = line_buffer.find('\n') {
                let line: String = line_buffer.drain(..=newline).collect();
                let line = line.trim_end();
                if !line.is_empty() {
                    debug!(machine = machine.as_str(), stdout = is_stdout, "{line}");
                }
            }
            if !is_stdout {
                continue;
            }
            if busy.is_none() {
                continue;
            }
            pending.push_str(&String::from_utf8_lossy(&bytes));
            if pending.contains(RUNNER_BUSY_SENTINEL) {
                if let Some(busy) = busy.take() {
                    let _ = busy.send(());
                }
                pending.clear();
            } else if pending.len() > 2 * RUNNER_BUSY_SENTINEL.len() {
                // Keep only enough tail to rejoin a sentinel split across reads.
                let keep = pending.len() - RUNNER_BUSY_SENTINEL.len();
                pending.drain(..keep);
            }
        }
    });

    let code = tokio::select! {
        code = provider.exec_stream(name, run, chunks) => code,
        error = guest_unresponsive(provider, name, liveness) => {
            warn!(machine = name.as_str(), %error, "runner guest is wedged; abandoning its stream");
            watcher.abort();
            return Err(error);
        }
    }?;
    let _ = watcher.await;
    if code == 0 {
        Ok(())
    } else {
        Err(VmError::Command {
            operation: "run",
            exit_code: code,
            message: format!("guest runner exited with code {code}"),
        })
    }
}

/// Whether a fork failure means this golden can never serve another fork.
///
/// A SmolVM fork base carries one RAM checkpoint. Lose it — a raced fork whose
/// rollback resumed the base, a pruned snapshot directory, a golden restarted
/// out from under the record — and the base is paused with nothing to restore
/// from, so *every* later fork fails identically. The pool keeps working, but
/// each runner now pays a full VM create, which reads as "jobs are queued and
/// nothing is happening" rather than as a broken fork base. Matching SmolVM's
/// wording is deliberate: these strings are the only signal the CLI gives, and
/// a missed match costs a log line, not correctness.
fn fork_base_unusable(error: &VmError) -> bool {
    let message = error.to_string();
    [
        "is already paused",
        "is not running forkable",
        "control socket not responding",
        "is not ready to fork",
    ]
    .iter()
    .any(|signature| message.contains(signature))
}

/// Whether `golden` is a fork base this pool baked and manages.
///
/// The plain form (`{prefix}-golden`) is the single golden baked for the
/// default environment at pool startup from the packed artifact; the
/// fingerprint-suffixed form (`{prefix}-golden-{fingerprint12}`) is prepared
/// on demand by `run_slot` from the job's OCI image when a job asks for a
/// non-default base. Only the plain form is packed — the suffixed form is an
/// environment golden whose forks do not inherit the baked baseline. Both
/// can lose their retained fork checkpoint, so the re-arm /
/// independent-create fallback applies to each; callers must not conflate
/// "managed" with "packed".
///
/// A name like `{prefix}-golden-environment` is deliberately NOT matched:
/// that is a baked environment golden serving a different `runs-on` image,
/// and replacing it with the packed artifact would run the job on the wrong
/// operating system.
fn managed_golden(config: &RunnerPoolConfig, golden: &MachineName) -> bool {
    let prefix = plain_packed_golden_name(config);
    golden.as_str() == prefix
        || golden.as_str().strip_prefix(&prefix).is_some_and(|rest| {
            rest.strip_prefix('-')
                .is_some_and(|fp| fp.len() == 12 && fp.bytes().all(|b| b.is_ascii_hexdigit()))
        })
}

/// The plain packed golden: the single golden baked at pool startup from the
/// packed artifact for the default environment.
fn plain_packed_golden_name(config: &RunnerPoolConfig) -> String {
    format!("{}-golden", config.name_prefix)
}

/// Best-effort VM telemetry: register a just-created or forked machine.
///
/// Fills what the pool knows about the machine; unknown fields stay unset.
/// No-op without a wired observability handle.
fn vm_telemetry_register(
    config: &RunnerPoolConfig,
    name: &MachineName,
    role: &str,
    spec: Option<&MachineSpec>,
) {
    let Some(observability) = &config.observability else {
        return;
    };
    let (cpus, memory_mib, storage_gb, overlay_gb) = match spec {
        Some(spec) => (
            spec.cpus,
            spec.memory_mib,
            spec.storage_gib,
            spec.overlay_gib,
        ),
        // A fork inherits the golden's resources, which match the pool's
        // configured values.
        None => (
            config.cpus,
            config.memory_mib,
            config.storage_gib,
            config.overlay_gib,
        ),
    };
    observability
        .vm_registry()
        .register(preloop_observability::vm_telemetry::VmRuntimeInfo {
            name: name.as_str().to_owned(),
            role: role.to_owned(),
            activity: "running".to_owned(),
            pid: None,
            start_time: None,
            cpus,
            memory_mib,
            storage_gb,
            overlay_gb,
            data_dir: None,
            created_at: None,
        });
}

/// Best-effort VM telemetry: drop a machine from the registry at teardown.
fn vm_telemetry_deregister(config: &RunnerPoolConfig, name: &MachineName) {
    if let Some(observability) = &config.observability {
        observability.vm_registry().deregister(name.as_str());
    }
}

/// Emit a runner-slot failure metric so guest crashes, provisioning errors,
/// and un-recyclable fork bases are alertable — not just WARN log lines. A
/// guest-internal OOM/`-1` exit has no host cgroup signal, so the WARN was the
/// only trace; this makes `preloop.pool.slot_failures{reason}` the alert hook.
fn record_slot_failure(config: &RunnerPoolConfig, reason: &str) {
    if let Some(observability) = &config.observability {
        observability.metrics().pool.record_slot_failure(reason);
    }
}

/// Extra fork attempts after a transient packed-golden fork failure, before
/// falling back to a direct create from the packed artifact.
const FORK_RETRY_ATTEMPTS: u32 = 2;

/// Remove whatever a failed fork left behind so the name can be reused.
async fn remove_failed_fork<P: VmProvider + 'static>(provider: &Arc<P>, name: &MachineName) {
    if let Err(cleanup) = provider.delete(name).await {
        debug!(
            machine = name.as_str(),
            %cleanup,
            "failed fork left no removable clone"
        );
    }
}

/// Create, boot, and register one ephemeral runner; return its `run` argv.
///
/// The caller owns cleanup: on any error the machine may already exist.
#[allow(clippy::too_many_arguments)]
async fn provision_runner<P: VmProvider + 'static>(
    provider: &Arc<P>,
    config: &RunnerPoolConfig,
    name: &MachineName,
    golden: Option<&MachineName>,
    keys: &Arc<KeyPool>,
    environment: &RunnerEnvironment,
) -> Result<Vec<String>, OrchestratorError> {
    let mut direct_create_from_packed = config.use_packed_artifact;
    let forked_golden = match golden {
        Some(golden) => match provider.fork(golden, name).await {
            Ok(()) => Some(golden),
            Err(error @ VmError::ForkBaseBusy { .. })
                if config.use_packed_artifact && managed_golden(config, golden) =>
            {
                // A live plain-fork clone still depends on the golden's frozen
                // storage. Do not touch the base and do not create another VM
                // from that same packed payload: SmolVM's mixed fork/create
                // path has returned ESTALE in both machines. Boot this slot
                // independently from the job's OCI environment instead.
                warn!(
                    machine = name.as_str(),
                    golden = golden.as_str(),
                    %error,
                    "fork base busy; creating runner independently from the OCI image"
                );
                direct_create_from_packed = false;
                None
            }
            Err(error) if config.use_packed_artifact && managed_golden(config, golden) => {
                if fork_base_unusable(&error) {
                    // The base is spent. Re-arm it atomically with forking:
                    // partial-clone cleanup, the live-clone check, and the
                    // stop/start happen under the provider's per-golden fork
                    // lock, so a concurrent slot cannot create a clone mid
                    // re-arm. A full engine restart and golden rebuild was
                    // the only recovery before; a re-arm is a few seconds.
                    warn!(
                        machine = name.as_str(),
                        golden = golden.as_str(),
                        %error,
                        "fork base spent; re-arming the golden once"
                    );
                    match provider.rearm_fork_base(golden, Some(name)).await {
                        Ok(true) => {
                            info!(golden = golden.as_str(), "golden fork base re-armed");
                            match provider.fork(golden, name).await {
                                Ok(()) => Some(golden),
                                Err(retry_error) => {
                                    error!(
                                        machine = name.as_str(),
                                        golden = golden.as_str(),
                                        %retry_error,
                                        "re-armed golden still cannot fork; falling back to \
                                         direct creation"
                                    );
                                    let _ = provider.delete(name).await;
                                    None
                                }
                            }
                        }
                        Ok(false) => {
                            // A live clone (another runner forked from the
                            // golden) blocks the re-freeze; those clones are
                            // ephemeral and exit after their job. Wait for
                            // them to drain, probing with exponential backoff
                            // so a long-running job does not force the slow
                            // direct-create path for every queued job in the
                            // meantime. The fork path is ~0.5 s vs ~8 min for
                            // independent creation, so waiting is worth it up
                            // to a generous total budget; only then fall back
                            // to direct creation (whose socket mount cannot
                            // serve the control transport, so the fallback
                            // usually fails registration anyway).
                            let mut rearmed = false;
                            let mut probe_delay = GOLDEN_DRAIN_PROBE_DELAY;
                            let drain_deadline = tokio::time::Instant::now() + GOLDEN_DRAIN_BUDGET;
                            let mut attempt = 0_u32;
                            while tokio::time::Instant::now() < drain_deadline {
                                tokio::time::sleep(probe_delay).await;
                                attempt += 1;
                                match provider.rearm_fork_base(golden, Some(name)).await {
                                    Ok(true) => {
                                        info!(
                                            golden = golden.as_str(),
                                            attempt, "golden fork base re-armed after clone drain"
                                        );
                                        rearmed = true;
                                        break;
                                    }
                                    Ok(false) => {
                                        // Live clones still hold the golden;
                                        // back off and probe again: the next
                                        // probe costs little, and the clone
                                        // may exit before the budget runs out.
                                        probe_delay = probe_delay
                                            .saturating_mul(2)
                                            .min(GOLDEN_DRAIN_PROBE_MAX);
                                    }
                                    Err(drain_error) => {
                                        error!(
                                            golden = golden.as_str(),
                                            %drain_error,
                                            "re-arm failed while draining clones; falling back \
                                             without further waiting"
                                        );
                                        break;
                                    }
                                }
                            }
                            if rearmed {
                                match provider.fork(golden, name).await {
                                    Ok(()) => Some(golden),
                                    Err(retry_error) => {
                                        error!(
                                            machine = name.as_str(),
                                            golden = golden.as_str(),
                                            %retry_error,
                                            "re-armed golden still cannot fork; falling back to \
                                             direct creation"
                                        );
                                        let _ = provider.delete(name).await;
                                        None
                                    }
                                }
                            } else {
                                error!(
                                    golden = golden.as_str(),
                                    "fork base spent and could not be re-armed after waiting for \
                                     clone drain; falling back to independent OCI creation"
                                );
                                let _ = provider.delete(name).await;
                                direct_create_from_packed = false;
                                None
                            }
                        }
                        Err(rearm_error) => {
                            error!(
                                golden = golden.as_str(),
                                %rearm_error,
                                "failed to re-arm spent fork base; falling back to independent \
                                 OCI creation"
                            );
                            direct_create_from_packed = false;
                            None
                        }
                    }
                } else {
                    // A restored clone can wedge transiently (agent readiness
                    // or rejuvenation timeouts) while the golden stays frozen
                    // with its retained checkpoint, so the fork itself is safe
                    // to retry. A retry costs about a second; the direct
                    // create cold-boots the packed image, which takes minutes
                    // on macOS while the guest unpacks its layers.
                    let mut last_error = error;
                    let mut retried = None;
                    for attempt in 1..=FORK_RETRY_ATTEMPTS {
                        remove_failed_fork(provider, name).await;
                        warn!(
                            machine = name.as_str(),
                            golden = golden.as_str(),
                            attempt,
                            error = %last_error,
                            "packed golden fork failed; retrying the fork"
                        );
                        match provider.fork(golden, name).await {
                            Ok(()) => {
                                retried = Some(golden);
                                break;
                            }
                            Err(error) => last_error = error,
                        }
                    }
                    if retried.is_none() {
                        // A failed fork can leave a partial clone behind.
                        // Best-effort cleanup makes the direct create safe; if
                        // cleanup itself is still racing SmolVM state, create
                        // returns the actionable error and the slot supervisor
                        // retries normally.
                        warn!(
                            machine = name.as_str(),
                            golden = golden.as_str(),
                            error = %last_error,
                            "packed golden fork failed; creating runner directly from packed artifact"
                        );
                        remove_failed_fork(provider, name).await;
                    }
                    retried
                }
            }
            Err(error) => return Err(error.into()),
        },
        None => None,
    };

    if let Some(golden) = forked_golden {
        // The fork succeeded, so the clone exists as a live machine.
        vm_telemetry_register(config, name, "runner", None);
        // Fork from the already-booted golden VM instant CoW clone.
        // The PACKED golden carries its bake inside the artifact's flattened
        // rootfs, which forks inherit through the storage chain — so the apt
        // baseline is already there. Environment goldens are different:
        // `prepare_golden_for_env`
        // bakes via guest `exec`, and SmolVM's forkable snapshot does NOT
        // carry post-create exec writes into clones (verified empirically),
        // so an env-golden fork boots the bare stock base image. Install the
        // apt baseline into the fork itself — it is the job's single-use
        // machine, so the writes persist for its lifetime. Language versions
        // remain the workflow's setup action responsibility.
        // Only the plain `{prefix}-golden` fork base is created from the
        // packed artifact (`prepare_packed_golden` at pool startup), whose
        // rootfs already carries the apt baseline that forks inherit.
        // Fingerprint-suffixed goldens are baked by
        // `prepare_golden_for_env` from the job's OCI image via guest exec,
        // and SmolVM's forkable snapshot does NOT carry post-create exec
        // writes into clones — so those forks must install the baseline
        // themselves. Treating an env golden as packed skipped that install
        // and provisioned runners without the curated baseline.
        // A fork arrives already baked in two cases: it came from the packed
        // artifact (the bake is inside the flattened rootfs), or the backend's
        // snapshot carries the golden's post-boot writes. AgentENV does the
        // latter; SmolVM does neither for an environment golden, which is why
        // that case still installs the baseline per fork. Getting this wrong
        // on a write-inheriting backend re-runs the whole bake (apt baseline,
        // rust, go, docker tooling) inside every single-use runner.
        let golden_is_baked = (config.use_packed_artifact
            && golden.as_str() == plain_packed_golden_name(config))
            || provider.capabilities().fork_inherits_guest_writes;
        if golden_is_baked {
            // The pack carries the apt baseline, but not necessarily apt's
            // indices — restore them before any workflow apt-installs. A
            // custom base is used as-is: no apt assumptions.
            if environment.curated
                && let Err(error) = provider.exec(name, &apt_lists_refresh_command()).await
            {
                warn!(
                machine = name.as_str(),
                    %error, "apt list refresh failed; workflow apt installs may not resolve"
                );
            }
            // Log the pack's apt-index age from the freshness marker baked
            // with it. A missing marker just means "stale" (packs predating
            // the stamp, or env-golden forks that boot bare) — the refresh
            // above already ran, so behavior is unchanged; only the warning
            // is new, and the weekly apt-indices-refresh rebuilds stale packs.
            match provider
                .exec(
                    name,
                    &["cat".to_owned(), APT_INDICES_MARKER_PATH.to_owned()],
                )
                .await
            {
                Ok(output) if output.exit_code == 0 => {
                    let today_days = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|elapsed| elapsed.as_secs() / 86_400)
                        .unwrap_or(0);
                    let text = String::from_utf8_lossy(&output.stdout);
                    match crate::environment::apt_marker_age_days(&text, today_days) {
                        Some((age_days, max_age_days)) if age_days > max_age_days => {
                            warn!(
                                machine = name.as_str(),
                                age_days,
                                max_age_days,
                                "pack apt indices are stale; refresh ran but consider rebuilding the golden"
                            );
                        }
                        Some((age_days, _)) => {
                            info!(machine = name.as_str(), age_days, "pack apt indices fresh");
                        }
                        None => {
                            debug!(
                                machine = name.as_str(),
                                "pack has no parseable apt-index marker"
                            );
                        }
                    }
                }
                _ => {
                    debug!(
                        machine = name.as_str(),
                        "pack has no apt-index marker; treating as stale"
                    );
                }
            }
        } else if environment.curated {
            install_base_dependencies(provider.as_ref(), name).await?;
            for layer in &environment.toolchains {
                for command in layer.install_commands() {
                    if let Err(error) = provider.exec(name, &command).await {
                        return Err(error.into());
                    }
                }
                verify_toolchain_installed(provider.as_ref(), name, layer).await?;
            }
        } else {
            // Custom base image: used as-is, no apt bake, no toolchains.
            debug!(
                machine = name.as_str(),
                base = %environment.base,
                "custom base image — skipping the curated bake"
            );
        }
    } else {
        // The packed-artifact fallback is valid only for the plain packed
        // golden: it boots the *default* OS image. A fingerprint-suffixed
        // golden is an environment golden baked from the job's requested
        // image, so every fork-failure fallback must boot that job's own
        // environment instead — regardless of which branch failed the fork.
        // When no golden was attempted (`golden` is None, the create-per-
        // runner path), the packed artifact is the pool's normal image and
        // stays as-is.
        let golden_is_plain_packed = match golden {
            Some(golden) => golden.as_str() == plain_packed_golden_name(config),
            None => true,
        };
        let uses_packed_artifact = direct_create_from_packed && golden_is_plain_packed;
        let pack = packed_golden_path(&config.artifact_payload());
        let spec = MachineSpec {
            name: name.clone(),
            image: if uses_packed_artifact {
                pack.display().to_string()
            } else if config.use_packed_artifact {
                environment.base.clone()
            } else {
                config.base_image.clone()
            },
            cpus: config.cpus,
            memory_mib: config.memory_mib,
            storage_gib: config.storage_gib,
            overlay_gib: config.overlay_gib,
            network: NetworkPolicy::PublicOnly,
            volumes: runner_volumes(config, name, !uses_packed_artifact)?,
            sockets: config
                .control_socket
                .iter()
                .map(|host| SocketMount {
                    host: host.clone(),
                    guest: PathBuf::from(GUEST_CONTROL_SOCKET),
                })
                .collect(),
            dns: config.dns.clone(),
            rosetta: cfg!(target_os = "macos") && std::env::consts::ARCH == "aarch64",
        };
        provider.create(&spec).await?;
        provider.start(name).await?;
        vm_telemetry_register(config, name, "runner", Some(&spec));
        // The packed artifact is the golden's frozen image; the live golden
        // receives the apt baseline and toolchain bake *after* boot, so a
        // machine created from the artifact is bare and must install the
        // baseline itself — otherwise node actions die with "curl: command
        // not found" and rust jobs with "cargo: command not found". The
        // installs are idempotent, so a fully baked artifact only pays the
        // presence checks. A custom base is the operator's contract: no apt
        // baseline, no toolchain curation.
        if environment.curated {
            install_base_dependencies(provider.as_ref(), name).await?;
            for layer in &environment.toolchains {
                for command in layer.install_commands() {
                    if let Err(error) = provider.exec(name, &command).await {
                        return Err(error.into());
                    }
                }
                verify_toolchain_installed(provider.as_ref(), name, layer).await?;
            }
        }
    }

    // Reconcile ownership of the paths a job writes on every machine, whatever
    // its provenance: curated stock bases, custom/official images, and forked
    // goldens alike. `base_install_script` runs the full account script only
    // inside the curated bake, so custom bases reached jobs with root-owned
    // `/usr/local/rustup`, `/usr/local/cargo`, and `/opt/hostedtoolcache` — and
    // any step that writes them (`rustup component add`, `cargo fmt`, toolcache
    // drops) died with EACCES. This is the ownership half only: privilege
    // policy stays where it was baked. One exec round trip, and the script
    // itself no-ops when the machine is already correct.
    {
        let runner_user = config.runner_user.as_deref().unwrap_or(DEFAULT_RUNNER_USER);
        let runner_uid = config.runner_uid.unwrap_or(DEFAULT_RUNNER_UID);
        let reconcile = runner_ownership_reconcile_script(runner_user, runner_uid);
        let output = provider
            .exec(name, &["sh".to_owned(), "-c".to_owned(), reconcile])
            .await?;
        if output.exit_code != 0 {
            return Err(OrchestratorError::Config(format!(
                "runner-ownership reconciliation failed on {} (exit {}): {} — \
                 /usr/local/rustup, /usr/local/cargo, and /opt/hostedtoolcache must be \
                 writable by uid {}; the engine could not escalate (image user is not \
                 root and passwordless sudo is unavailable)",
                name.as_str(),
                output.exit_code,
                String::from_utf8_lossy(&output.stderr)
                    .lines()
                    .last()
                    .unwrap_or("unknown"),
                runner_uid
            )));
        }
    }

    // Apply the hosted runtime init — the per-guest half of what the hosted
    // image's own init does for a GitHub-hosted runner. Same always-run,
    // idempotent path as the ownership reconciliation above, and for the same
    // reason: the guest enters the job workload through the exec channel, so
    // the image's `/etc/sysctl.*` (no init reads them) and its hostname setup
    // (the fork's name is decided after the bake) never applied, and jobs ran
    // against kernel defaults and a hostname that resolved nowhere. See
    // [`guest_hosted_runtime_init_script`] for what each half covers; a
    // machine that already matches pays one exec round trip and no writes.
    {
        let script = guest_hosted_runtime_init_script();
        let output = provider
            .exec(name, &["sh".to_owned(), "-c".to_owned(), script])
            .await?;
        if output.exit_code != 0 {
            return Err(OrchestratorError::Config(format!(
                "hosted runtime init failed on {} (exit {}): {} — the machine must \
                 reach GitHub's hosted sysctls ({}) and resolve its own hostname to \
                 an address of this machine, or jobs run against a different \
                 environment than a hosted runner's",
                name.as_str(),
                output.exit_code,
                String::from_utf8_lossy(&output.stderr)
                    .lines()
                    .last()
                    .unwrap_or("unknown"),
                GITHUB_GUEST_SYSCTLS
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
    }

    let runner = format!("/opt/preloop/bin/{}", config.runner_binary_name);
    let mut labels = config.labels.clone();
    for label in runner_environment_labels(&environment.base) {
        if !labels
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(&label))
        {
            labels.push(label);
        }
    }
    let labels = labels.join(",");
    let mut configure = guest_env_prefix(config, name);
    // During configure the control bridge is not yet running, so the
    // runner must reach the server directly. When a TCP upstream is
    // configured, use that address for registration instead of the
    // loopback origin.
    let registration_url = config
        .control_upstream
        .as_deref()
        .unwrap_or(&config.server_url);
    configure.extend([
        runner.clone(),
        "configure".into(),
        "--url".into(),
        registration_url.to_owned(),
        "--name".into(),
        name.as_str().into(),
        "--labels".into(),
        labels,
        "--runner-root".into(),
        RUNNER_ROOT.into(),
        "--unattended".into(),
        "--replace".into(),
        "--ephemeral".into(),
        "--no-externals".into(),
    ]);
    let mut secrets = vec![(
        "PRELOOP_RUNNER_TOKEN".to_owned(),
        SecretSource::HostEnv(config.registration_token_env.clone()),
    )];
    // One-time provision token: the control plane pairs this machine's
    // registration with the queued job the machine was provisioned for. The
    // guest cannot fabricate a pairing because only this exact configure
    // invocation ever sees the token value.
    let mut provision_token_file: Option<PathBuf> = None;
    let mut provision_token_value: Option<String> = None;
    if let Some(pending) = &config.pending_registrations {
        let token = uuid::Uuid::new_v4().to_string();
        let dir = config
            .runner_key_dir
            .clone()
            .unwrap_or_else(std::env::temp_dir);
        match std::fs::create_dir_all(&dir).and_then(|()| {
            let path = dir.join(format!(".provision-token-{}", uuid::Uuid::new_v4()));
            std::fs::write(&path, &token).map(|()| path)
        }) {
            Ok(path) => {
                if let Ok(mut guard) = pending.write() {
                    let issued_at = std::time::SystemTime::now();
                    guard.insert(token.clone(), issued_at);
                    // Mirror the mint into the consolidated status handle so
                    // `PoolStatus::snapshot().pending_registrations` counts
                    // tokens issued after startup too. The server-side
                    // consume removes it from both stores.
                    if let Some(ps) = &config.pool_status {
                        ps.insert_pending(token.clone(), issued_at);
                        // Same 600s window as the legacy pending-map prune
                        // below, so stale tokens don't inflate
                        // `pending_registrations` forever.
                        ps.retain_pending_newer_than(std::time::Duration::from_secs(600));
                    }
                    let now = std::time::SystemTime::now();
                    guard.retain(|_, at| {
                        now.duration_since(*at)
                            .map(|age| age < std::time::Duration::from_secs(600))
                            .unwrap_or(false)
                    });
                }
                provision_token_value = Some(token);
                secrets.push((
                    "PRELOOP_PROVISION_TOKEN".to_owned(),
                    SecretSource::HostFile(path.clone()),
                ));
                provision_token_file = Some(path);
            }
            Err(error) => warn!(%error, "could not stage provision token"),
        }
    }
    // Held until `configure` returns; dropping it wipes the key from disk.
    let staged = stage_runner_key(config, name, keys).await;
    if let Some(staged) = &staged {
        match staged.path() {
            Ok(path) => secrets.push((
                RUNNER_RSA_PARAMS_ENV.to_owned(),
                SecretSource::HostFile(path),
            )),
            Err(error) => {
                warn!(%error, "staged runner key unreadable; the guest will generate one")
            }
        }
    }
    // Start the container engine in the background while `configure`
    // registers the runner: the 5-15 s dockerd cold boot overlaps
    // registration instead of gating readiness. The worker waits for the
    // daemon before container setup (and it is up long before step 1 for
    // ad-hoc `docker` steps), so nothing observes a half-started engine.
    // Fire-and-forget: a failure only warns here; container setup reports
    // the missing daemon against the job it actually blocks.
    {
        let provider = std::sync::Arc::clone(provider);
        let name = name.clone();
        let start_command = docker_start_command();
        tokio::spawn(async move {
            if let Err(error) = provider.exec(&name, &start_command).await {
                warn!(
                    machine = name.as_str(),
                    %error,
                    "background container engine start failed; container setup will report it"
                );
            }
        });
    }

    let configure_result = provider
        .exec_with_secret_env(name, &as_runner_user(config, &configure), &secrets)
        .await;
    drop(staged);
    if configure_result.is_err()
        && let Some(token) = provision_token_value.as_deref()
    {
        if let Some(pending) = &config.pending_registrations
            && let Ok(mut guard) = pending.write()
        {
            guard.remove(token);
        }
        if let Some(ps) = &config.pool_status {
            ps.remove_pending(token);
        }
    }
    if let Some(path) = provision_token_file.take() {
        let _ = std::fs::remove_file(path);
    }
    configure_result?;

    info!(machine = name.as_str(), "ephemeral runner ready");
    let mut run = guest_env_prefix(config, name);
    run.extend([
        runner,
        "run".into(),
        "--once".into(),
        "--runner-root".into(),
        RUNNER_ROOT.into(),
    ]);
    Ok(as_runner_user(config, &run))
}

/// Wrap a guest argv so the runner executes under `runner_user` instead of
/// root: create the account, provision its runtime directory, open the
/// control bridge, grant the docker group, then drop privileges with
/// `setpriv` and export the account identity for the step-environment
/// contract (USER/LOGNAME/XDG_RUNTIME_DIR are derived from it by the
/// worker). Purely a guest-side concern — never applied on the host.
/// Carry a guest shell script so its root-only steps run either directly
/// (the exec landed on root — locally baked goldens from plain bases declare
/// no USER) or via passwordless sudo (the official runner image declares
/// `USER runner`, and `machine exec` runs as that image user). The script is
/// embedded base64 so every quoting form survives both shells.
fn run_as_root_or_sudo(script: &str) -> String {
    run_as_root_or_sudo_impl(script, true)
}

/// Same root-or-sudo fallback as [`run_as_root_or_sudo`], but status-
/// preserving: a refused sudo or a failing script propagates a nonzero exit
/// so the caller's exit-code check is authoritative. Required for provisioning
/// that has no independent verification step — the lenient form would turn a
/// passwordless-sudo miss into a false success.
fn run_as_root_or_sudo_strict(script: &str) -> String {
    run_as_root_or_sudo_impl(script, false)
}

fn run_as_root_or_sudo_impl(script: &str, best_effort: bool) -> String {
    use base64::Engine as _;
    // The script is spliced inline as `then {script}; else …`, so a trailing
    // newline would leave that `;` alone on a line and dash would reject the
    // whole command (`Syntax error: ";" unexpected`). Scripts that live in a
    // file (`scripts/docker-start.sh`) end in a newline by convention, and
    // trailing whitespace means nothing to the shell, so drop it here instead
    // of asking every file-backed script to remember.
    let script = script.trim_end();
    let b64 = base64::engine::general_purpose::STANDARD.encode(script);
    let fallback = if best_effort {
        "sudo -n sh 2>/dev/null || true"
    } else {
        "sudo -n sh"
    };
    format!(
        "if [ \"$(id -u)\" -eq 0 ]; then {script}; else \
           printf %s '{b64}' | base64 -d | {fallback}; fi"
    )
}

/// Wrap a guest argv so it launches on the hosted process limits without
/// switching accounts.
///
/// The `runner_user`-less and `runner_user: root` launches keep their identity
/// (the exec channel's user — root, or the image's own `USER`); only the
/// limits are added. The wrapped argv travels as the shell's positional
/// parameters (`sh -c '<raise>; exec "$@"' sh <argv…>`), so nothing is
/// re-quoted and the launch stays byte-identical after the wrapper: a
/// mis-quoted argument cannot corrupt a step command, and the exec records
/// the pool's tests read still carry the original argv.
///
/// Neither half can assume root here: raising a hard limit needs it, so the
/// launch keeps whatever it inherited — with a line on stderr saying which
/// limit stayed — rather than failing ([`GUEST_STACK_ULIMIT_BEST_EFFORT`],
/// [`GUEST_NOFILE_ULIMIT_BEST_EFFORT`]). A root exec reaches the hosted pairs.
fn with_guest_hosted_limits(argv: &[String]) -> Vec<String> {
    let mut wrapped = vec![
        "sh".to_owned(),
        "-c".to_owned(),
        format!(
            "{GUEST_STACK_ULIMIT_BEST_EFFORT}; {GUEST_NOFILE_ULIMIT_BEST_EFFORT}; exec \"$@\""
        ),
        "sh".to_owned(),
    ];
    wrapped.extend_from_slice(argv);
    wrapped
}

fn as_runner_user(config: &RunnerPoolConfig, argv: &[String]) -> Vec<String> {
    // Unset or root means no account switch, not no limits: the guest applies
    // neither the image's systemd nor its PAM limits on this path either, so
    // the runner would keep the VM init's 8192 KiB stack — and the exec
    // channel's descriptor limit — and every step it spawns would too. The
    // launch is wrapped instead of replaced — the exec channel's identity
    // (root, or the image user) stays exactly as it was.
    let Some(user) = &config.runner_user else {
        return with_guest_hosted_limits(argv);
    };
    if user == "root" {
        return with_guest_hosted_limits(argv);
    }
    let uid = config.runner_uid.unwrap_or(1001);
    let home = format!("/home/{user}");
    let program = shell_quote(&argv[0]);
    let args = argv[1..]
        .iter()
        .map(|arg| shell_quote(arg))
        .collect::<Vec<_>>()
        .join(" ");
    // Root-only provisioning: create the runner account when missing, open
    // its runtime and control-bridge paths, join the docker group. Runs
    // directly when the exec landed on root, else via passwordless sudo —
    // the official golden declares USER runner, so `machine exec` lands on
    // runner and setpriv below self-drops to the same uid (no privilege
    // change needed).
    // `/tmp` lives on the VM's ext4 data disk, not the overlayfs root: the
    // small tmpfs the guest boots with fills on real test suites, and the
    // overlay root it used to fall through to does not support
    // `name_to_handle_at`, so fanotify FID watchers (TypeScript's fswatch,
    // 126 tests) failed with "operation not supported". GitHub-hosted
    // runners keep `/tmp` on ext4; the bind mount matches that.
    let provisioning = format!(
        "PATH=/usr/sbin:/usr/bin:/sbin:/bin:$PATH; \
         getent passwd {user} >/dev/null 2>&1 || useradd -m -u {uid} {user} 2>/dev/null || true; \
         mkdir -p /etc/sudoers.d; \
         printf '%s\\n' '{user} ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/preloop-{user} 2>/dev/null \
           || true; \
         chmod 0440 /etc/sudoers.d/preloop-{user} 2>/dev/null || true; \
         mkdir -p /run/user/{uid} /opt/hostedtoolcache; \
         chown -R {uid}:{uid} /home/runner 2>/dev/null || true; \
         chown {uid}:{uid} /run/user/{uid} 2>/dev/null || true; \
         if [ -d /usr/local/rustup ]; then chown -R {uid}:{uid} /usr/local/rustup; fi; \
         if [ -d /usr/local/cargo ]; then chown -R {uid}:{uid} /usr/local/cargo; fi; \
         [ \"$(stat -c %a /opt/hostedtoolcache 2>/dev/null)\" = \"777\" ] || \
           chmod -R 777 /opt/hostedtoolcache 2>/dev/null; \
         grep -q AGENT_TOOLSDIRECTORY /etc/environment 2>/dev/null || \
           printf 'AGENT_TOOLSDIRECTORY=/opt/hostedtoolcache\\nRUNNER_TOOL_CACHE=/opt/hostedtoolcache\\n' >> /etc/environment; \
         mkdir -p /run/preloop-control 2>/dev/null; chmod 777 /run/preloop-control 2>/dev/null; \
         getent group docker >/dev/null 2>&1 && usermod -aG docker {user} 2>/dev/null; \
         if command -v mountpoint >/dev/null 2>&1 && mountpoint -q /tmp 2>/dev/null && \
            [ \"$(stat -f -c %T /tmp 2>/dev/null)\" = tmpfs ]; then \
           umount /tmp 2>/dev/null || mount -o remount,size=75% /tmp 2>/dev/null || true; \
         fi; \
         mkdir -p /tmp && chmod 1777 /tmp 2>/dev/null || true; \
         if ! mountpoint -q /tmp 2>/dev/null && mountpoint -q /workspace 2>/dev/null; then \
           mkdir -p /workspace/.tmp && chmod 1777 /workspace/.tmp && \
           mount --bind /workspace/.tmp /tmp 2>/dev/null || true; \
         fi"
    );
    // setpriv requires a groups mode: --init-groups (setgroups) only works
    // as root, so the exec-as-image-user branch (official golden: USER
    // runner, uid 1001) must use --keep-groups — the exec context already
    // carries the right supplementary groups, and reuid/regid to self are
    // permitted without privileges. The root branch keeps --init-groups.
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD.encode(&provisioning);
    // Raise the process limits before dropping privileges: the runner and
    // every step it spawns inherit them from here. [`GUEST_STACK_ULIMIT`] and
    // [`GUEST_NOFILE_ULIMIT`] mirror the hosted pair, because the guest
    // applies neither the image's systemd units nor its PAM limits; without
    // them the runner chain keeps the VM init's half-sized stack and the exec
    // channel's descriptor limit (1024 soft / 4096 hard on AgentENV), and
    // suites that raise their own soft limit — valkey's test suite asks for
    // 10032 — die with EPERM on setrlimit. The earlier 524288 hard limit here
    // was a guess at what GitHub's runner service inherits; a probe of a
    // hosted runner reads back `Max open files 65536 / 65536`, which is what
    // `configure-limits.sh` (`DefaultLimitNOFILE=65536`) actually grants.
    // Raising a hard limit needs root (CAP_SYS_RESOURCE), hence the sudo in
    // the exec-as-image-user branch; setpriv then runs as root there too, so
    // --init-groups is correct in both branches (setgroups needs root — the
    // --keep-groups variant was only a workaround for the self-drop).
    let inner = format!(
        "{GUEST_STACK_ULIMIT}; {GUEST_NOFILE_ULIMIT}; \
         exec setpriv --reuid {uid} --regid {uid} --init-groups env \
           PRELOOP_RUNNER_USER={user} PRELOOP_RUNNER_UID={uid} HOME={home} {program} {args}"
    );
    let inner_b64 = base64::engine::general_purpose::STANDARD.encode(&inner);
    let script = format!(
        "if [ \"$(id -u)\" -eq 0 ]; then \
           {provisioning}; \
           printf %s '{inner_b64}' | base64 -d | sh; \
         else \
           printf %s '{b64}' | base64 -d | sudo -n sh 2>/dev/null || true; \
           printf %s '{inner_b64}' | base64 -d | sudo -n --preserve-env sh; \
         fi"
    );
    vec!["sh".to_owned(), "-c".to_owned(), script]
}

/// Single-quote an argv element for the guest bootstrap shell.
fn shell_quote(arg: &str) -> String {
    format!("'{}'", arg.replace('\'', "'\\''"))
}

/// Fail the provision if a toolchain layer's binary is not on the default
/// PATH after install. A provision interrupted between install commands (or
/// an install that silently succeeded without producing the binary) would
/// otherwise leave the job running without its toolchain — e.g. cargo-dist
/// failing on "you don't appear to have cargo installed" with no hint that
/// the machine itself was broken.
async fn verify_toolchain_installed<P: VmProvider>(
    provider: &P,
    name: &MachineName,
    layer: &ToolchainLayer,
) -> Result<(), OrchestratorError> {
    let command = vec!["sh".to_owned(), "-c".to_owned(), layer.verify_command()];
    if let Err(error) = provider.exec(name, &command).await {
        return Err(OrchestratorError::Vm(error));
    }
    Ok(())
}

/// Stage a pre-generated keypair for one `configure` call, if one is ready.
///
/// Absent a staged key the guest generates its own, which is simply the
/// slower path — never a failure.
async fn stage_runner_key(
    config: &RunnerPoolConfig,
    name: &MachineName,
    keys: &Arc<KeyPool>,
) -> Option<StagedKey> {
    let directory = config.runner_key_dir.as_deref()?;
    let params = keys.take().await?;
    match StagedKey::write(directory, name.as_str(), &params) {
        Ok(staged) => Some(staged),
        Err(error) => {
            warn!(path = %directory.display(), %error, "could not stage a runner keypair");
            None
        }
    }
}

/// Hold a failed runner's VM open so `preloop shell` can attach.
///
/// The marker file is the session handle: `preloop shell` refreshes its mtime
/// while attached and demotes it to IDLE on exit. An attach on a
/// state-preserving backend resumes a suspended sandbox (and renews its TTL
/// keepalive); detach re-suspends it. The idle deadline, not the shell exit,
/// is what ends the hold — cleanup deletes the VM afterwards.
async fn hold_for_debugging<P: VmProvider + 'static>(
    provider: &Arc<P>,
    name: &MachineName,
    debug_dir: &Path,
    shutdown: &CancellationToken,
) {
    let marker = debug_dir.join(name.as_str());
    if let Err(error) =
        std::fs::create_dir_all(debug_dir).and_then(|()| std::fs::write(&marker, DEBUG_MARKER_IDLE))
    {
        warn!(
            machine = name.as_str(),
            path = %marker.display(),
            %error,
            "cannot record debug marker — deleting VM instead of preserving it"
        );
        return;
    }
    if provider.capabilities().preserves_runtime_state_on_suspend
        && let Err(error) = provider.stop(name).await
    {
        warn!(
            machine = name.as_str(),
            %error,
            "could not suspend preserved AgentENV VM; leaving it running"
        );
    }

    warn!(
        machine = name.as_str(),
        timeout_secs = DEBUG_IDLE_TIMEOUT.as_secs(),
        "job failed — VM preserved for debugging; attach with `preloop shell`"
    );

    let mut deadline = tokio::time::Instant::now() + DEBUG_IDLE_TIMEOUT;
    let mut attached = false;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            info!(
                machine = name.as_str(),
                "debug idle timeout expired — deleting preserved VM"
            );
            break;
        }
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(remaining.min(DEBUG_POLL_INTERVAL)) => {}
        }
        let Ok(state) = std::fs::read_to_string(&marker) else {
            // The marker was removed externally: the session is over.
            info!(
                machine = name.as_str(),
                "debug session ended — deleting preserved VM"
            );
            break;
        };
        // Only a live `preloop shell` heartbeat counts as attached. Matching on
        // mtime alone would let this function's own initial write renew it.
        let now_attached = state.trim() == DEBUG_MARKER_ACTIVE
            && std::fs::metadata(&marker)
                .and_then(|meta| meta.modified())
                .ok()
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age < DEBUG_HEARTBEAT_WINDOW);
        if provider.capabilities().preserves_runtime_state_on_suspend {
            if now_attached && !attached {
                // Resume the suspended sandbox — and, when it is already
                // running, restart the TTL keepalive that the engine-side
                // pause cancelled. Idempotent either way.
                if let Err(error) = provider.start(name).await {
                    warn!(
                        machine = name.as_str(),
                        %error,
                        "could not resume preserved AgentENV VM for attach"
                    );
                }
            } else if !now_attached && attached {
                // The shell detached: park the sandbox for the retained
                // window instead of burning host resources while idle.
                if let Err(error) = provider.stop(name).await {
                    warn!(
                        machine = name.as_str(),
                        %error,
                        "could not re-suspend preserved AgentENV VM after detach"
                    );
                }
            }
        }
        if now_attached {
            deadline = tokio::time::Instant::now() + DEBUG_IDLE_TIMEOUT;
        }
        attached = now_attached;
    }
    let _ = std::fs::remove_file(&marker);
}

/// Return the runner artifact payload generated for an output stem and base
/// image.
pub fn artifact_payload(stem: &Path, base_image: &str) -> PathBuf {
    // Keep in sync with `RunnerPoolConfig::artifact_payload`: the packed
    // artifact is keyed by the resolved base image AND the environment
    // fingerprint, so bake-content changes invalidate the pack.
    let fingerprint = EnvironmentSpec::for_base(base_image.to_owned()).fingerprint;
    let mut path = stem.as_os_str().to_owned();
    path.push(format!("-{fingerprint}"));
    PathBuf::from(path)
}

/// Resolve the actual packed-golden file for smolvm's `machine create
/// --from`. The artifact stem names the payload: a downloaded release asset
/// IS the SMOLPACK at the stem, while a locally built golden leaves an ELF
/// launcher stub at the stem with the pack in the `<stem>.smolmachine`
/// sidecar. Prefer the sidecar when present, else the stem itself — never
/// invent a path that may not exist.
fn packed_golden_path(payload: &Path) -> PathBuf {
    let sidecar = PathBuf::from(format!("{}.smolmachine", payload.display()));
    if sidecar.is_file() {
        sidecar
    } else {
        payload.to_path_buf()
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use async_trait::async_trait;
    use base64::Engine as _;
    use preloop_vm::{ExecOutput, OutputChunk, ProviderCapabilities};
    use std::collections::HashMap;
    use std::os::unix::fs::PermissionsExt as _;
    use std::process::Command;
    use tokio::sync::Mutex;

    /// The production scenario that OOMed: 22 GiB host, 8 GiB runners.
    /// Golden (8 GiB) + 2 GiB reserve leaves 12 GiB → at most 1 concurrent
    /// on-demand fork. CPU-only sizing allowed several and the host died.
    #[test]
    fn on_demand_memory_cap_fits_runners_after_golden_and_reserve() {
        assert_eq!(on_demand_memory_cap(22 * 1024, 8 * 1024), 1);
        assert_eq!(on_demand_memory_cap(64 * 1024, 8 * 1024), 6);
        assert_eq!(on_demand_memory_cap(32 * 1024, 4 * 1024), 6);
    }

    /// A host too small for even one runner past the golden still runs one
    /// job (floor of 1) rather than refusing to work.
    #[test]
    fn on_demand_memory_cap_floors_at_one() {
        assert_eq!(on_demand_memory_cap(8 * 1024, 8 * 1024), 1);
        assert_eq!(on_demand_memory_cap(4 * 1024, 8 * 1024), 1);
    }

    /// A zero runner ceiling (should not happen — config validates it) must
    /// not divide by zero; it degrades to the 1 MiB floor and lets the host
    /// run as many 1 MiB "runners" as fit.
    #[test]
    fn on_demand_memory_cap_handles_zero_runner_ceiling() {
        assert_eq!(
            on_demand_memory_cap(32 * 1024, 0),
            (32 * 1024 - 2048 - 1) as usize
        );
    }

    #[derive(Debug)]
    struct TestProvider {
        machines: Mutex<HashMap<String, MachineState>>,
        events: Mutex<Vec<String>>,
        created_images: Mutex<Vec<(String, String)>>,
        fail_fork: bool,
        fork_base_busy: bool,
        /// Fail the next fork with the "spent fork base" signature, then
        /// succeed. Mirrors a golden whose retained checkpoint vanished.
        fail_fork_once_spent: Mutex<bool>,
        /// Fail the next fork with a transient boot error, then succeed.
        /// Mirrors a restored clone that wedged before its agent answered.
        fail_fork_once: Mutex<bool>,
        /// Model a guest whose agent stopped answering: the runner stream
        /// never closes and liveness probes never return.
        wedged_guest: bool,
        /// Report live clones to `rearm_fork_base`; true by default so a spent
        /// base with dependents is never re-armed in tests either.
        live_forks: Mutex<bool>,
        /// Simulate a clone exiting mid-drain: flip `live_forks` off after
        /// this many `rearm_fork_base` calls (0 = never).
        drain_live_forks_after: Mutex<u32>,
        fail_start: bool,
        fail_install: bool,
        fail_configure: bool,
        fail_run: bool,
        fail_delete: bool,
        announce_busy: bool,
        /// When set, `exec_with_secret_env` (the configure step) blocks until
        /// notified, so a test can observe the pool mid-provision.
        configure_gate: Option<Arc<tokio::sync::Notify>>,
        /// Guest pause marker state: when set, the exec probe for the debug
        /// pause marker succeeds, so `watch_guest_pause` sees a paused job.
        pause_marker: std::sync::atomic::AtomicBool,
        /// When set, the pause-marker probe fails like a wedged VM
        /// (transport error), which the watcher must not read as "resumed".
        probe_transport_error: std::sync::atomic::AtomicBool,
        /// Whether this provider claims runtime-preserving suspension. When
        /// set, the debug watcher may park the machine via `stop`.
        suspends: bool,
        /// Names passed to `prune_pack_intermediates`, in call order.
        prune_pack_calls: Mutex<Vec<String>>,
        /// When set, `exec` answers exit 1 for any argv whose debug form
        /// contains this marker — models a guest command that fails.
        fail_exec_containing: Mutex<Option<String>>,
    }

    impl TestProvider {
        fn new(
            fail_start: bool,
            fail_install: bool,
            fail_configure: bool,
            fail_run: bool,
            fail_delete: bool,
        ) -> Self {
            Self {
                machines: Mutex::new(HashMap::new()),
                events: Mutex::new(Vec::new()),
                created_images: Mutex::new(Vec::new()),
                fail_fork: false,
                fork_base_busy: false,
                fail_fork_once_spent: Mutex::new(false),
                fail_fork_once: Mutex::new(false),
                wedged_guest: false,
                live_forks: Mutex::new(true),
                drain_live_forks_after: Mutex::new(0),
                fail_start,
                fail_install,
                fail_configure,
                fail_run,
                fail_delete,
                announce_busy: false,
                configure_gate: None,
                pause_marker: std::sync::atomic::AtomicBool::new(false),
                probe_transport_error: std::sync::atomic::AtomicBool::new(false),
                suspends: false,
                prune_pack_calls: Mutex::new(Vec::new()),
                fail_exec_containing: Mutex::new(None),
            }
        }

        fn that_suspends(mut self) -> Self {
            self.suspends = true;
            self
        }

        fn with_configure_gate(mut self, gate: Arc<tokio::sync::Notify>) -> Self {
            self.configure_gate = Some(gate);
            self
        }

        fn announcing_busy(mut self) -> Self {
            self.announce_busy = true;
            self
        }

        fn failing_fork(mut self) -> Self {
            self.fail_fork = true;
            self
        }

        fn with_busy_fork_base(mut self) -> Self {
            self.fork_base_busy = true;
            self
        }

        /// Fail the next fork with the spent-fork-base signature, then succeed.
        fn failing_fork_once_spent(mut self) -> Self {
            *self.fail_fork_once_spent.get_mut() = true;
            self
        }

        /// Fail the next fork with a transient boot error, then succeed.
        fn failing_fork_once(mut self) -> Self {
            *self.fail_fork_once.get_mut() = true;
            self
        }

        /// Model a guest whose agent stopped answering.
        fn with_wedged_guest(mut self) -> Self {
            self.wedged_guest = true;
            self
        }

        /// Report whether clones of the golden still exist.
        fn with_live_forks(mut self, live: bool) -> Self {
            *self.live_forks.get_mut() = live;
            self
        }

        /// Simulate the last live clone exiting after `n` drain probes, so a
        /// re-arm that keeps probing eventually succeeds.
        fn drain_live_forks_after(mut self, n: u32) -> Self {
            *self.drain_live_forks_after.get_mut() = n;
            self
        }

        async fn has_machine(&self, name: &MachineName) -> bool {
            self.machines.lock().await.contains_key(name.as_str())
        }

        async fn events(&self) -> Vec<String> {
            self.events.lock().await.clone()
        }

        async fn created_image(&self, name: &MachineName) -> Option<String> {
            self.created_images
                .lock()
                .await
                .iter()
                .find_map(|(created, image)| (created == name.as_str()).then(|| image.clone()))
        }
    }

    fn test_error(message: &'static str) -> VmError {
        VmError::Command {
            operation: "lifecycle-test",
            exit_code: 1,
            message: message.to_owned(),
        }
    }

    /// A job paused in a debug session must hand its pool concurrency permit
    /// back and re-acquire it on resume — otherwise two unanswered pauses
    /// pin every slot and later runs queue forever.
    #[tokio::test]
    async fn paused_job_releases_and_reacquires_the_pool_permit() {
        use std::sync::atomic::Ordering;

        let provider = Arc::new(TestProvider::new(false, false, false, false, false));
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::new(std::sync::Mutex::new(Some(
            semaphore.clone().acquire_owned().await.unwrap(),
        )));
        let name = MachineName::new("preloop-runner-pause-test".to_owned()).unwrap();

        let watch = tokio::spawn(watch_guest_pause(
            provider.clone(),
            name,
            permit.clone(),
            semaphore.clone(),
            Duration::from_millis(10),
        ));

        // Not paused: the slot keeps its permit.
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(
            permit.lock().unwrap().is_some(),
            "a running job keeps its pool permit"
        );

        // Paused: the permit is handed back to the pool so other jobs can
        // fork runners.
        provider.pause_marker.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(
            permit.lock().unwrap().is_none(),
            "a paused job must not pin a pool permit"
        );
        assert_eq!(
            semaphore.available_permits(),
            1,
            "the released permit must be available to the pool"
        );

        // Resumed: the permit is re-acquired, restoring the bound.
        provider.pause_marker.store(false, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(
            permit.lock().unwrap().is_some(),
            "resuming the job must re-acquire its pool permit"
        );

        watch.abort();
        let _ = watch.await;
    }

    /// A transport failure while probing must not read as "resumed": the
    /// permit stays released for the (still paused) job, and the pool does
    /// not re-pin it on a transient smolvm error.
    #[tokio::test]
    async fn probe_transport_errors_preserve_pause_state() {
        use std::sync::atomic::Ordering;

        let provider = Arc::new(TestProvider::new(false, false, false, false, false));
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::new(std::sync::Mutex::new(Some(
            semaphore.clone().acquire_owned().await.unwrap(),
        )));
        let name = MachineName::new("preloop-runner-pause-probe".to_owned()).unwrap();

        let watch = tokio::spawn(watch_guest_pause(
            provider.clone(),
            name,
            permit.clone(),
            semaphore.clone(),
            Duration::from_millis(10),
        ));

        // Pause, release the permit, then make the probe fail like a wedged VM.
        provider.pause_marker.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(permit.lock().unwrap().is_none());
        provider.probe_transport_error.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(
            permit.lock().unwrap().is_none(),
            "a transport error must not re-pin the permit of a paused job"
        );

        // Probe recovers while still paused: still no permit.
        provider
            .probe_transport_error
            .store(false, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(permit.lock().unwrap().is_none());

        watch.abort();
        let _ = watch.await;
    }
    /// An AgentENV-style debug pause must suspend the sandbox while no
    /// controller is attached, and resume it the moment a live `preloop
    /// debug`/`preloop shell` marker appears — the money story: nobody
    /// attached, nobody paying for a running VM.
    #[tokio::test]
    async fn unattached_debug_pause_suspends_and_attach_resumes() {
        use std::sync::atomic::Ordering;

        let provider =
            Arc::new(TestProvider::new(false, false, false, false, false).that_suspends());
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::new(std::sync::Mutex::new(Some(
            semaphore.clone().acquire_owned().await.unwrap(),
        )));
        let name = MachineName::new("preloop-runner-suspend".to_owned()).unwrap();
        let debug_dir = tempfile::tempdir().unwrap();

        let watch = tokio::spawn(watch_guest_pause_with_suspension(
            provider.clone(),
            name.clone(),
            permit.clone(),
            semaphore.clone(),
            Some(debug_dir.path().to_path_buf()),
            Duration::from_millis(30),
            Duration::from_millis(10),
        ));

        // The job pauses. The permit is handed back and the idle grace starts.
        provider.pause_marker.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(
            permit.lock().unwrap().is_none(),
            "a paused job must not pin a pool permit"
        );
        // Unattached past the grace window: the sandbox is suspended.
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(
            provider
                .events()
                .await
                .contains(&format!("stop:{}", name.as_str())),
            "an unattached paused VM must be suspended, events: {:?}",
            provider.events().await
        );

        // A controller attaches: the watcher resumes it and stops parking.
        let marker = debug_dir.path().join(name.as_str());
        std::fs::write(&marker, DEBUG_MARKER_ACTIVE).unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(
            provider
                .events()
                .await
                .contains(&format!("start:{}", name.as_str())),
            "an attached debug VM must be resumed, events: {:?}",
            provider.events().await
        );

        watch.abort();
        let _ = watch.await;
    }

    /// A backend without runtime-preserving suspension must never be parked
    /// by the debug watcher: SmolVM's `stop` is a real shutdown that would
    /// throw away the live workspace the session exists to debug.
    #[tokio::test]
    async fn non_suspending_backends_never_park_a_debug_vm() {
        use std::sync::atomic::Ordering;

        let provider = Arc::new(TestProvider::new(false, false, false, false, false));
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::new(std::sync::Mutex::new(Some(
            semaphore.clone().acquire_owned().await.unwrap(),
        )));
        let name = MachineName::new("preloop-runner-nosuspend".to_owned()).unwrap();
        let debug_dir = tempfile::tempdir().unwrap();

        let watch = tokio::spawn(watch_guest_pause_with_suspension(
            provider.clone(),
            name.clone(),
            permit.clone(),
            semaphore.clone(),
            Some(debug_dir.path().to_path_buf()),
            Duration::from_millis(20),
            Duration::from_millis(10),
        ));

        provider.pause_marker.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            permit.lock().unwrap().is_none(),
            "the permit handback is backend-independent"
        );
        assert!(
            !provider
                .events()
                .await
                .iter()
                .any(|event| event.starts_with("stop:")),
            "a SmolVM-style backend must not be parked mid-session, events: {:?}",
            provider.events().await
        );

        watch.abort();
        let _ = watch.await;
    }

    /// The pinned digests for the Linux node tarballs the provisioning script
    /// installs, read from the same generated table the script verifies
    /// against.
    ///
    /// These used to be written out by hand, once per architecture. Bumping
    /// node refreshed the arm64 copies and left the x64 ones on the previous
    /// release, so the checksum step failed on x86_64 only — invisible to
    /// anyone developing on arm64, and it sat red in CI. Deriving them means a
    /// version bump cannot desynchronise the fixture from the pin again.
    fn pinned_linux_node_sha256() -> (&'static str, &'static str) {
        let arch = if cfg!(target_arch = "x86_64") {
            "x64"
        } else {
            "arm64"
        };
        let pin = |runtime: &str, version: &str| {
            let key = format!("{runtime}_{version}_linux-{arch}");
            node_externals_pinned_sha256(&key)
                .unwrap_or_else(|| panic!("no pinned sha256 for {key}"))
        };
        (
            pin("node20", NODE20_EXTERNALS_VERSION),
            pin("node24", NODE24_EXTERNALS_VERSION),
        )
    }

    /// `curl` stub: serves the pinned SHASUMS for this architecture, and a
    /// one-word body for any archive download.
    fn curl_stub_script() -> String {
        let (node20, node24) = pinned_linux_node_sha256();
        let arch = if cfg!(target_arch = "x86_64") {
            "x64"
        } else {
            "arm64"
        };
        let v20 = NODE20_EXTERNALS_VERSION;
        let v24 = NODE24_EXTERNALS_VERSION;
        let shasums = format!(
            "{node20}  node-v{v20}-linux-{arch}.tar.gz\\n{node24}  node-v{v24}-linux-{arch}.tar.gz\\n"
        );
        format!(
            r#"#!/bin/sh
out=""
url=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift 2;;
    -*) shift;;
    *) url="$1"; shift;;
  esac
done
case "$url" in *SHASUMS256.txt*) printf "{shasums}" > "$out"; exit 0;; esac
printf archive > "$out"
"#
        )
    }

    /// `shasum`/`sha256sum` stub: reports the pinned digest for whichever
    /// runtime the caller is verifying. No `uname` branch — the Rust side
    /// already resolved the architecture when it looked the pin up.
    fn digest_stub_script() -> String {
        let (node20, node24) = pinned_linux_node_sha256();
        format!(
            r#"#!/bin/sh
case "$*" in
  *node24*|*.node24.*) printf "{node24}  dummy\n";;
  *) printf "{node20}  dummy\n";;
esac
"#
        )
    }

    fn test_output() -> ExecOutput {
        ExecOutput {
            exit_code: 0,
            stdout: Vec::new(),
            stderr: Vec::new(),
            truncated: false,
        }
    }

    /// The bake manifest lands in root-owned `/etc` while the bake runs as the
    /// unprivileged runner account: a plain redirect fails with
    /// `Permission denied` on every bake, so goldens shipped without the
    /// provenance they are supposed to carry. The write therefore takes the
    /// root-or-sudo hop — and because that hop swallows a refused sudo
    /// (`|| true`), a manifest that never landed has to be caught by asking
    /// the guest rather than trusting the write's exit status.
    #[tokio::test]
    async fn bake_manifest_is_written_as_root_and_verified() {
        let provider = TestProvider::new(false, false, false, false, false);
        let name = MachineName::new("golden-manifest".to_owned()).unwrap();
        let env_spec = EnvironmentSpec::for_base(crate::environment::DEFAULT_BASE_IMAGE.to_owned());

        write_bake_manifest(&provider, &name, &env_spec)
            .await
            .expect("manifest write");

        let events = provider.events.lock().await.clone();
        let write = events
            .iter()
            .find(|event| event.contains("printf %s"))
            .expect("manifest write exec");
        assert!(
            write.contains("sudo -n sh"),
            "the manifest write must reach the guest as root: {write}"
        );
        let payload = write
            .split("printf %s '")
            .nth(1)
            .and_then(|rest| rest.split("' | base64 -d").next())
            .expect("privileged payload");
        let script = String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(payload)
                .expect("payload is base64"),
        )
        .expect("payload is utf-8");
        assert!(
            script.contains(&format!("> {BAKE_MANIFEST_PATH}")),
            "payload must write the manifest: {script}"
        );
        assert!(
            script.contains("\"preloop\""),
            "payload must carry this bake's manifest: {script}"
        );

        *provider.fail_exec_containing.lock().await = Some(format!("test -s {BAKE_MANIFEST_PATH}"));
        assert!(
            write_bake_manifest(&provider, &name, &env_spec)
                .await
                .is_err(),
            "a manifest that never landed must not read as success"
        );
    }

    #[test]
    fn node_external_archives_are_piped_into_tar() {
        let temp = tempfile::tempdir().unwrap();
        let bin = temp.path().join("bin");
        let root = temp.path().join("runner");
        std::fs::create_dir_all(&bin).unwrap();

        let curl = bin.join("curl");
        std::fs::write(&curl, curl_stub_script()).unwrap();
        std::fs::set_permissions(&curl, std::fs::Permissions::from_mode(0o755)).unwrap();

        let shasum = bin.join("shasum");
        std::fs::write(&shasum, digest_stub_script()).unwrap();
        std::fs::set_permissions(&shasum, std::fs::Permissions::from_mode(0o755)).unwrap();

        let sha256sum = bin.join("sha256sum");
        std::fs::write(&sha256sum, digest_stub_script()).unwrap();
        std::fs::set_permissions(&sha256sum, std::fs::Permissions::from_mode(0o755)).unwrap();

        let tar = bin.join("tar");
        std::fs::write(
            &tar,
            r#"#!/bin/sh
archive=""
dest=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -xzf) archive="$2"; shift 2;;
    -C) dest="$2"; shift 2;;
    *) shift;;
  esac
done
if [ ! -f "$archive" ]; then exit 41; fi
content=$(cat "$archive")
[ "$content" = archive ] || exit 42
mkdir -p "$dest/bin"
printf '#!/bin/sh\nexit 0\n' > "$dest/bin/node"
chmod +x "$dest/bin/node"
"#,
        )
        .unwrap();
        std::fs::set_permissions(&tar, std::fs::Permissions::from_mode(0o755)).unwrap();

        let command = node_externals_at(root.to_str().unwrap()).pop().unwrap();
        let status = Command::new(&command[0])
            .args(&command[1..])
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    bin.display(),
                    std::env::var("PATH").unwrap_or_default()
                ),
            )
            .status()
            .unwrap();

        assert!(status.success());
        assert!(root.join("externals/node20/bin/node").is_file());
        assert!(root.join("externals/node24/bin/node").is_file());
        // Manifests must be written with correct version and SHA.
        let (node20_sha, node24_sha) = pinned_linux_node_sha256();
        for name in ["node20", "node24"] {
            let manifest =
                std::fs::read_to_string(root.join(format!("externals/{name}/preloop-node.json")))
                    .unwrap();
            assert!(manifest.contains("\"runtime\":\""), "{manifest}");
            assert!(manifest.contains("\"archive_sha256\":\""), "{manifest}");
            let expected_sha = if name == "node20" {
                node20_sha
            } else {
                node24_sha
            };
            assert!(
                manifest.contains(expected_sha),
                "{name} manifest must record the pinned digest {expected_sha}: {manifest}"
            );
        }
    }

    /// The guest runner drops to uid 1001, so a 0700 `node24/` hides a
    /// perfectly good interpreter behind EACCES and every JS action fails
    /// with "bundled node24 is missing". `mktemp -d` publishes exactly that
    /// mode, so the publish step has to widen it.
    #[test]
    fn published_node_externals_are_traversable_by_other_users() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let bin = root.join("stub-bin");
        std::fs::create_dir_all(&bin).unwrap();

        let curl = bin.join("curl");
        std::fs::write(&curl, curl_stub_script()).unwrap();
        std::fs::set_permissions(&curl, std::fs::Permissions::from_mode(0o755)).unwrap();
        let shasum = bin.join("shasum");
        std::fs::write(&shasum, digest_stub_script()).unwrap();
        std::fs::set_permissions(&shasum, std::fs::Permissions::from_mode(0o755)).unwrap();

        let sha256sum = bin.join("sha256sum");
        std::fs::write(&sha256sum, digest_stub_script()).unwrap();
        std::fs::set_permissions(&sha256sum, std::fs::Permissions::from_mode(0o755)).unwrap();

        let tar = bin.join("tar");
        std::fs::write(
            &tar,
            r#"#!/bin/sh
archive=""
dest=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -xzf) archive="$2"; shift 2;;
    -C) dest="$2"; shift 2;;
    *) shift;;
  esac
done
if [ ! -f "$archive" ]; then exit 41; fi
content=$(cat "$archive")
[ "$content" = archive ] || exit 42
mkdir -p "$dest/bin"
printf '#!/bin/sh\nexit 0\n' > "$dest/bin/node"
chmod +x "$dest/bin/node"
"#,
        )
        .unwrap();
        std::fs::set_permissions(&tar, std::fs::Permissions::from_mode(0o755)).unwrap();

        let command = node_externals_at(root.to_str().unwrap()).pop().unwrap();
        let status = Command::new(&command[0])
            .args(&command[1..])
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    bin.display(),
                    std::env::var("PATH").unwrap_or_default()
                ),
            )
            .status()
            .unwrap();
        assert!(status.success());

        for name in ["node20", "node24"] {
            let mode = std::fs::metadata(root.join("externals").join(name))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(
                mode & 0o055,
                0o055,
                "{name} must stay traversable for the non-root guest runner (mode {mode:o})"
            );
        }
    }

    /// Externals published before the non-root switch are already on disk at
    /// 0700, and the installer skips directories that already carry a node
    /// binary — so start-up has to repair them in place or the host never
    /// recovers without manual intervention.
    #[test]
    fn existing_externals_are_repaired_in_place() {
        let directory = tempfile::tempdir().unwrap();
        let externals = directory.path().join("externals");
        let node24 = externals.join("node24").join("bin");
        std::fs::create_dir_all(&node24).unwrap();
        std::fs::write(node24.join("node"), "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(
            externals.join("node24"),
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();

        relax_externals_permissions(&externals);

        let mode = std::fs::metadata(externals.join("node24"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o055, 0o055, "0700 externals must be repaired");
        assert_eq!(
            mode & 0o022,
            0,
            "repair must not grant write access to other users"
        );
    }

    #[test]
    fn mounted_control_socket_uses_advertised_origin() {
        let mut config = test_config(true);
        config.server_url = "http://192.168.1.20:9090".to_owned();
        config.control_origin = Some("http://127.0.0.1:9090".to_owned());
        let name = MachineName::new("runner").unwrap();

        let env = guest_env_prefix(&config, &name);

        assert!(env.contains(&"PRELOOP_CONTROL_ORIGIN=http://127.0.0.1:9090".to_owned()));
        assert!(!env.contains(&"PRELOOP_CONTROL_ORIGIN=http://192.168.1.20:9090".to_owned()));
    }

    /// A workflow that installs a cargo subcommand and runs it in the next
    /// step (`taiki-e/install-action` + `cargo hack`) only works when the
    /// toolchain's bin directory is on the runner's PATH, as it is on hosted
    /// images. Without it the install "succeeds" and the next step reports
    /// `no such command: hack`.
    #[test]
    fn runner_path_carries_toolchain_bin_directories() {
        let config = test_config(false);
        let name = MachineName::new("runner").unwrap();

        let env = guest_env_prefix(&config, &name);

        let path = env
            .iter()
            .find_map(|entry| entry.strip_prefix("PATH="))
            .expect("the runner is launched with an explicit PATH");
        let entries: Vec<&str> = path.split(':').collect();
        assert!(
            entries.contains(&"/usr/local/cargo/bin"),
            "cargo-installed binaries must be reachable: {path}"
        );
        // Toolchain homes are fixed system addresses, identical for root
        // and switched runners: a $HOME-derived location would be invisible
        // across the bake-user/step-user boundary (/root is 0700).
        for expected in [
            "RUSTUP_HOME=/usr/local/rustup",
            "CARGO_HOME=/usr/local/cargo",
        ] {
            assert!(
                env.iter().any(|entry| entry == expected),
                "guest env must pin {expected}: {env:?}"
            );
        }
        assert!(
            entries.contains(&"/usr/local/go/bin"),
            "the go layer untars into /usr/local/go: {path}"
        );
        assert!(
            entries.contains(&"/usr/local/bin") && entries.contains(&"/usr/bin"),
            "the system PATH must survive: {path}"
        );
        assert!(
            env.contains(&"RUSTUP_HOME=/usr/local/rustup".to_owned()),
            "the shared rustup metadata must be selected: {env:?}"
        );
    }

    #[test]
    fn tcp_upstream_sets_origin_and_upstream_without_socket() {
        let mut config = test_config(false);
        config.server_url = "http://127.0.0.1:9090".to_owned();
        config.control_origin = Some("http://127.0.0.1:9090".to_owned());
        config.control_upstream = Some("http://10.0.0.161:9090".to_owned());
        let name = MachineName::new("runner").unwrap();

        let env = guest_env_prefix(&config, &name);

        assert!(env.contains(&"PRELOOP_CONTROL_ORIGIN=http://127.0.0.1:9090".to_owned()));
        assert!(env.contains(&"PRELOOP_CONTROL_UPSTREAM=http://10.0.0.161:9090".to_owned()));
        assert!(!env.iter().any(|v| v.starts_with("PRELOOP_CONTROL_SOCKET")));
    }

    /// The always-run ownership reconciliation must not install privilege
    /// policy: custom/official images that never
    /// had blanket sudo must not gain it just because a job runs there — and
    /// it must not mask failures the way the account script's `|| true` tail
    /// does.
    #[test]
    fn ownership_reconcile_script_keeps_privilege_policy_out() {
        let script = runner_ownership_reconcile_script(DEFAULT_RUNNER_USER, 1001);
        assert!(!script.contains("NOPASSWD"), "{script}");
        assert!(!script.contains("usermod"), "{script}");
        assert!(!script.contains("sudoers"), "{script}");
        assert!(!script.contains("docker"), "{script}");
        assert!(
            script.contains("needs=0"),
            "an already-correct machine must skip the privileged half: {script}"
        );
        assert!(
            script.contains("sudo -n sh"),
            "a machine that needs changes must escalate: {script}"
        );
        assert!(
            !script.contains("|| true"),
            "escalation and chown failures must stay observable: {script}"
        );
        // Official runner images ship rustup under the runner's $HOME while
        // the exported contract points at /usr/local; without adopting it,
        // every `cargo`/`rustup` invocation resolves an empty home. The apply
        // half travels base64-encoded, so assert on the decoded text.
        let decoded = script
            .split("| base64 -d")
            .filter_map(|part| {
                let close = part.rfind('\'')?;
                let open = part[..close].rfind('\'')?;
                Some(part[open + 1..close].to_owned())
            })
            .filter_map(|blob| {
                base64::engine::general_purpose::STANDARD
                    .decode(blob)
                    .ok()
                    .and_then(|bytes| String::from_utf8(bytes).ok())
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            decoded.contains("ln -s /home/runner/.rustup /usr/local/rustup"),
            "the image's rustup home must be adopted: {decoded}"
        );
        assert!(
            decoded.contains("ln -s /home/runner/.cargo /usr/local/cargo"),
            "the image's cargo home must be adopted: {decoded}"
        );
        assert!(
            script.contains("/home/runner/.rustup/settings.toml"),
            "adoption must be gated on a usable rustup home: {script}"
        );
        // A runner-owned root with root-owned descendants passes a top-level
        // `stat` and then skips the recursive chown; the probe must descend.
        assert!(
            script.contains("find \"$d/.\" ! -uid 1001 -print -quit"),
            "the ownership probe must be recursive: {script}"
        );
        assert!(
            script.contains("stat -L -c %u"),
            "the probe must dereference an adopted symlink home: {script}"
        );
    }

    /// The guest has no init applying `/etc/sysctl.*`, so the values GitHub's
    /// hosted image bakes into `/etc/sysctl.conf` must be written per machine
    /// or every job runs against kernel defaults. Pin the hosted values and
    /// the shape of the script: a comparison-only pre-check (an already-correct
    /// machine writes nothing), a privileged half that writes only the pairs
    /// the pre-check selected (a key the kernel does not expose must never
    /// reach the write), escalation through passwordless sudo when the exec
    /// lands on the image user, and a post-write re-read so a rejected write
    /// fails provisioning instead of silently diverging. The behavior itself
    /// is executed by `guest_sysctl_script_writes_only_exposed_keys` and
    /// `guest_sysctl_script_falls_back_to_proc_sys_without_procps` below.
    #[test]
    fn guest_sysctl_script_pins_hosted_values() {
        let script = guest_sysctl_script();
        for pair in [
            "vm.max_map_count=262144",
            "fs.inotify.max_user_watches=655360",
            "fs.inotify.max_user_instances=1280",
        ] {
            assert!(
                script.contains(pair),
                "hosted value {pair} must be applied: {script}"
            );
        }
        assert!(
            script.contains("[ -z \"$plan\" ]"),
            "an already-correct machine must skip the writes: {script}"
        );
        assert!(
            script.contains("exit 0"),
            "the matching case must exit before escalating: {script}"
        );
        assert!(
            script.contains("sudo -n sh"),
            "the image-user case must escalate: {script}"
        );
        assert!(
            !script.contains("|| true"),
            "a rejected write must stay observable: {script}"
        );
        assert!(
            script.contains("root='/proc/sys'"),
            "the guest tree must be the kernel's: {script}"
        );
        // The privileged half travels base64-encoded; decode every blob and
        // assert the writes it carries.
        let decoded = script
            .split("| base64 -d")
            .filter_map(|part| {
                let close = part.rfind('\'')?;
                let open = part[..close].rfind('\'')?;
                Some(part[open + 1..close].to_owned())
            })
            .filter_map(|blob| {
                base64::engine::general_purpose::STANDARD
                    .decode(blob)
                    .ok()
                    .and_then(|bytes| String::from_utf8(bytes).ok())
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            decoded.contains("for pair in \"$@\""),
            "the privileged half must write the pairs the pre-check selected, not the \
             whole configured list: {decoded}"
        );
        assert!(
            decoded.contains("sysctl -w \"$pair\""),
            "the procps branch must write one selected pair at a time: {decoded}"
        );
        assert!(
            decoded.contains("root='/proc/sys'"),
            "images without procps need the direct /proc/sys fallback: {decoded}"
        );
        // GitHub-hosted runners run with vm.overcommit_memory=0 (probe of
        // image 20260927.320.1): Valkey's overcommit warning is expected on
        // both sides, and forcing it to 1 here would *diverge* from GitHub.
        assert!(!script.contains("overcommit"), "{script}");
    }

    /// A stub executable in a scratch `bin`, the shape the curl/tar stubs
    /// above use.
    #[cfg(unix)]
    fn scratch_executable(path: &std::path::Path, body: &str) {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::write(path, body).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// The runner account's passwordless sudo, for harnesses whose test runner
    /// is not root (CI guests usually are, a developer machine is not).
    #[cfg(unix)]
    const SUDO_STUB: &str = "#!/bin/sh\n[ \"$1\" = -n ] && shift\nexec \"$@\"\n";

    /// Link one real tool into a scratch `bin` that must offer no others; the
    /// direct-write test uses it to remove `sysctl` from the guest's PATH.
    #[cfg(unix)]
    fn symlink_tool(bin: &std::path::Path, tool: &str) {
        let source = ["/usr/bin", "/bin"]
            .iter()
            .map(|dir| std::path::Path::new(dir).join(tool))
            .find(|candidate| candidate.exists())
            .unwrap_or_else(|| panic!("no {tool} on this machine for the harness PATH"));
        std::os::unix::fs::symlink(source, bin.join(tool)).unwrap();
    }

    /// Devin's case: a guest kernel with `vm.max_map_count` at the kernel
    /// default that does not expose `fs.inotify.max_user_instances` at all.
    /// The pre-check skipped absent keys, but the privileged half used to hand
    /// every configured pair to `sysctl -w`; the absent key made procps exit
    /// non-zero and `set -e` aborted the apply, so an otherwise usable guest
    /// could never be provisioned. The scratch tree stands in for `/proc/sys`
    /// and the stub for procps, so the real script runs end to end — a `-w`
    /// for a leaf the tree does not have fails exactly as on a real kernel.
    #[cfg(unix)]
    #[test]
    fn guest_sysctl_script_writes_only_exposed_keys() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("sys");
        let bin = temp.path().join("bin");
        let log = temp.path().join("sysctl.log");
        std::fs::create_dir_all(root.join("vm")).unwrap();
        std::fs::create_dir_all(root.join("fs/inotify")).unwrap();
        std::fs::create_dir_all(&bin).unwrap();
        // Present at the kernel default: the hosted value must land.
        std::fs::write(root.join("vm/max_map_count"), "65530").unwrap();
        // Present and already correct: no write is needed for it.
        std::fs::write(root.join("fs/inotify/max_user_watches"), "655360").unwrap();
        // Absent on this kernel: procps fails on it, so the privileged half
        // must never see it.
        scratch_executable(
            &bin.join("sysctl"),
            r#"#!/bin/sh
while [ "$#" -gt 0 ]; do
  case "$1" in
    -w) shift;;
    -*) shift;;
    *) break;;
  esac
done
for pair in "$@"; do
  key=${pair%%=*}
  path="$PRELOOP_TEST_SYSCTL_ROOT/$(printf '%s' "$key" | tr '.' '/')"
  printf '%s\n' "$pair" >> "$PRELOOP_TEST_SYSCTL_LOG"
  if [ ! -e "$path" ]; then
    printf 'sysctl: cannot stat %s: No such file or directory\n' "$path" >&2
    exit 1
  fi
  [ "$PRELOOP_TEST_SYSCTL_STALL" = "$key" ] && continue
  printf '%s' "${pair#*=}" > "$path"
done
"#,
        );
        scratch_executable(&bin.join("sudo"), SUDO_STUB);

        let script = guest_sysctl_script_at(root.to_str().unwrap());
        let run = || {
            std::process::Command::new("/bin/sh")
                .args(["-c", &script])
                .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
                .env("PRELOOP_TEST_SYSCTL_ROOT", &root)
                .env("PRELOOP_TEST_SYSCTL_LOG", &log)
                .output()
                .unwrap()
        };
        let first = run();
        assert!(
            first.status.success(),
            "a guest missing one hosted key must still be provisioned: {}",
            String::from_utf8_lossy(&first.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(root.join("vm/max_map_count")).unwrap(),
            "262144",
            "the exposed key that differed must reach the hosted value"
        );
        assert_eq!(
            std::fs::read_to_string(&log).unwrap(),
            "vm.max_map_count=262144\n",
            "only exposed, differing keys may reach a write"
        );
        // A second exec against the same machine compares clean: one exec
        // round trip, no writes.
        let second = run();
        assert!(second.status.success());
        assert_eq!(
            std::fs::read_to_string(&log).unwrap(),
            "vm.max_map_count=262144\n"
        );
        // A write the kernel did not take must still fail provisioning: the
        // post-check re-reads every pair the apply carried and names it.
        std::fs::write(root.join("vm/max_map_count"), "65530").unwrap();
        let stalled = std::process::Command::new("/bin/sh")
            .args(["-c", &script])
            .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
            .env("PRELOOP_TEST_SYSCTL_ROOT", &root)
            .env("PRELOOP_TEST_SYSCTL_LOG", &log)
            .env("PRELOOP_TEST_SYSCTL_STALL", "vm.max_map_count")
            .output()
            .unwrap();
        assert!(
            !stalled.status.success(),
            "a write the kernel did not take must fail provisioning"
        );
        assert!(
            String::from_utf8_lossy(&stalled.stderr).contains("vm.max_map_count=262144(got:65530)"),
            "the failure must name the pair and the value read back: {}",
            String::from_utf8_lossy(&stalled.stderr)
        );
    }

    /// Images without procps write `/proc/sys` directly, and that branch had
    /// the same absent-key failure. A real procfs refuses to create a leaf for
    /// a key the kernel does not expose; a scratch tree cannot, so the
    /// harness instead pins that the absent key is never written (the pre-fix
    /// script created it here, and fails outright on a kernel that has no such
    /// leaf).
    #[cfg(unix)]
    #[test]
    fn guest_sysctl_script_falls_back_to_proc_sys_without_procps() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("sys");
        let bin = temp.path().join("bin");
        std::fs::create_dir_all(root.join("fs/inotify")).unwrap();
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::write(root.join("fs/inotify/max_user_watches"), "64372").unwrap();
        // Only the tools the script genuinely needs, so `command -v sysctl`
        // comes up empty as it does on an image without procps.
        for tool in ["cat", "tr", "id", "base64", "sh"] {
            symlink_tool(&bin, tool);
        }
        // A test runner that is not root takes the escalated branch; the
        // runner account's passwordless sudo is a `sudo` that runs its argv.
        scratch_executable(&bin.join("sudo"), SUDO_STUB);

        let output = std::process::Command::new("/bin/sh")
            .args(["-c", &guest_sysctl_script_at(root.to_str().unwrap())])
            .env("PATH", bin.display().to_string())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "the direct-write branch must provision too: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(root.join("fs/inotify/max_user_watches")).unwrap(),
            "655360"
        );
        assert!(
            !root.join("fs/inotify/max_user_instances").exists(),
            "a key the kernel does not expose must never be written"
        );
        assert!(
            !root.join("vm").exists(),
            "keys the tree does not have must be left alone entirely"
        );
    }

    /// Every fork boots under a name the bake could not write into
    /// `/etc/hosts`, so `sudo` printed `sudo: unable to resolve host <name>`
    /// on every invocation — 25 lines in one valkey job, and no hosted-runner
    /// log has one. Pin the shape: a resolve-to-a-local-address check first
    /// (an already-correct machine writes nothing), the bake's
    /// `127.0.0.1 <host>` convention, escalation through passwordless sudo
    /// when the exec lands on the image user, and a re-check so a machine
    /// whose name still does not resolve locally fails provisioning instead of
    /// pointing at an address it does not own. The behavior itself is executed
    /// by `guest_hostname_script_makes_the_fork_name_resolve`,
    /// `guest_hostname_script_rewrites_a_stale_foreign_mapping` and
    /// `guest_hostname_script_fails_when_the_name_still_does_not_resolve`.
    #[test]
    fn guest_hostname_script_is_idempotent_and_verifies() {
        let script = guest_hostname_script();
        assert!(
            script.contains("getent ahosts \"$host\""),
            "the check must resolve the machine's own name: {script}"
        );
        assert!(
            script.contains("name_resolves_locally && exit 0"),
            "an already-resolving machine must skip the write: {script}"
        );
        assert!(
            script.contains("printf '127.0.0.1 %s"),
            "the entry must follow the bake's 127.0.0.1 convention: {script}"
        );
        assert!(
            script.contains("sudo -n tee '/etc/hosts'"),
            "the image-user branch must rewrite through sudo: {script}"
        );
        assert!(
            script.contains("sudo -n"),
            "escalation must be non-interactive: {script}"
        );
        assert!(
            script.contains("> '/etc/hosts'"),
            "the root branch must rewrite directly: {script}"
        );
        assert!(
            script.contains("hostname -I"),
            "local addresses must include the interfaces': {script}"
        );
        assert!(
            script.contains("A-Za-z0-9._-"),
            "the hostname must be validated before anything uses it: {script}"
        );
        assert!(
            script.contains("getent is missing"),
            "a machine without the resolver lookup must fail provisioning, not skip \
             the check: {script}"
        );
        assert!(
            script.contains("awk -v host=\"$host\""),
            "the name must be matched as a string, never interpolated into a program: {script}"
        );
        assert!(
            !script.contains("sed -E"),
            "no hostname may reach a sed program: {script}"
        );
        assert!(
            script.contains("is_local \"$addr\" || return 1"),
            "every resolved answer must be local, not just one of them: {script}"
        );
        assert!(
            !script.contains("|| true"),
            "a failed rewrite or resolution must stay observable: {script}"
        );
    }

    /// A stub executable in a scratch `bin` for the hostname harness below.
    #[cfg(unix)]
    fn hostname_harness_stub(path: &std::path::Path, body: &str) {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::write(path, body).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// The resolution check runs on `getent`, so a guest that does not have it
    /// cannot show that the machine's own name points where it must. That is a
    /// provisioning failure with the reason, not a silent skip: the hosts file
    /// is left alone and the machine never reaches a job with an unverified
    /// name.
    #[cfg(unix)]
    #[test]
    fn guest_hostname_script_fails_without_getent() {
        let temp = tempfile::tempdir().unwrap();
        let hosts = temp.path().join("hosts");
        let bin = temp.path().join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::write(&hosts, "127.0.0.1 localhost\n").unwrap();

        // A PATH with no `getent` anywhere in it.
        let output = std::process::Command::new("/bin/sh")
            .args(["-c", &guest_hostname_script_at(hosts.to_str().unwrap())])
            .env("PATH", bin.display().to_string())
            .output()
            .unwrap();
        assert!(
            !output.status.success(),
            "a machine without getent must fail provisioning"
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("getent is missing"),
            "the failure must name the missing lookup: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(&hosts).unwrap(),
            "127.0.0.1 localhost\n",
            "nothing may be rewritten when the result cannot be checked"
        );
    }

    /// The fork's `/etc/hosts` carries the bake's entries but never the name
    /// the fork booted under (`base_install_script` writes the *golden's*
    /// name; the fork's is decided at fork time), so `sudo` resolved its
    /// hostname to nothing and printed `sudo: unable to resolve host <name>`
    /// before every command — 25 lines in one valkey job, where a
    /// hosted-runner log has none. Run the real script against a scratch hosts
    /// file with `hostname`, the resolver and the escalator stubbed, and show
    /// the trigger before and the resolution after: the same before/after
    /// REVIEW.md §1a asks for, since on `main` nothing ever wrote the mapping.
    #[cfg(unix)]
    #[test]
    fn guest_hostname_script_makes_the_fork_name_resolve() {
        let temp = tempfile::tempdir().unwrap();
        let hosts = temp.path().join("hosts");
        let bin = temp.path().join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::write(
            &hosts,
            "127.0.0.1 localhost\n127.0.0.1 preloop-golden-bake\n",
        )
        .unwrap();

        // The name is decided at fork time.
        hostname_harness_stub(
            &bin.join("hostname"),
            "#!/bin/sh\nprintf '%s\\n' fork-7c1f\n",
        );
        // `getent hosts <name>` answered from the scratch file the way nss
        // `files` answers it: the first match and exit 0, else exit 2.
        hostname_harness_stub(
            &bin.join("getent"),
            &format!(
                "#!/bin/sh\n\
                 [ \"$1\" = hosts ] || [ \"$1\" = ahosts ] || exit 2\n\
                 line=$(grep -E \"^[^#]*[[:space:]]$2([[:space:]]|$)\" {} | head -n 1)\n\
                 [ -n \"$line\" ] || exit 2\n\
                 printf '%s\\n' \"$line\"\n",
                shell_quote(hosts.to_str().unwrap())
            ),
        );
        // sudo resolves the machine's name on every invocation — that lookup
        // is what printed the warning — and then runs its argv.
        hostname_harness_stub(
            &bin.join("sudo"),
            "#!/bin/sh\n\
             host=$(hostname 2>/dev/null)\n\
             getent hosts \"$host\" >/dev/null 2>&1 || \
               printf 'sudo: unable to resolve host %s\\n' \"$host\" >&2\n\
             [ \"$1\" = -n ] && shift\n\
             exec \"$@\"\n",
        );

        let path = format!("{}:/usr/bin:/bin", bin.display());
        let run = |script: &str| {
            std::process::Command::new("/bin/sh")
                .args(["-c", script])
                .env("PATH", &path)
                .output()
                .unwrap()
        };
        // Before: the fork's name resolves nowhere, so the `sudo` invocation
        // printed the line the job logs were full of.
        assert!(
            !run("getent hosts \"$(hostname)\"").status.success(),
            "the fork's own name must not resolve before the script runs"
        );
        let warned = run("sudo -n true");
        assert!(
            String::from_utf8_lossy(&warned.stderr).contains("unable to resolve host fork-7c1f"),
            "the reported symptom must reproduce first: {}",
            String::from_utf8_lossy(&warned.stderr)
        );

        // The fix: the real script appends the machine's own name and the
        // lookup that warned stops failing.
        let applied = run(&guest_hostname_script_at(hosts.to_str().unwrap()));
        assert!(
            applied.status.success(),
            "{}",
            String::from_utf8_lossy(&applied.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(&hosts).unwrap(),
            "127.0.0.1 localhost\n127.0.0.1 preloop-golden-bake\n127.0.0.1 fork-7c1f\n",
            "the entry must follow the bake's 127.0.0.1 convention"
        );
        assert!(
            run("getent hosts \"$(hostname)\"").status.success(),
            "the fork's own name must resolve once the script has run"
        );
        let quiet = run("sudo -n true");
        assert!(
            !String::from_utf8_lossy(&quiet.stderr).contains("unable to resolve host"),
            "the warning must be gone: {}",
            String::from_utf8_lossy(&quiet.stderr)
        );

        // And a machine whose name already resolves is left untouched.
        let again = run(&guest_hostname_script_at(hosts.to_str().unwrap()));
        assert!(again.status.success());
        assert_eq!(
            std::fs::read_to_string(&hosts).unwrap(),
            "127.0.0.1 localhost\n127.0.0.1 preloop-golden-bake\n127.0.0.1 fork-7c1f\n",
            "a second run must not append a duplicate entry"
        );
    }

    /// The AgentENV case the first version of this script skipped: the guest
    /// boots with a hosts file mapping its own name to an address it does not
    /// own (`10.1.0.59 runnervm… runnervmvrwv9` while the machine's interfaces
    /// carry `169.254.0.21`). `getent hosts <name>` answers, so a check that
    /// only asks whether the name resolves passes — while every consumer of
    /// the name, `sudo` included, gets an address that is not this machine.
    /// The script must notice and remove the machine's own name from the
    /// foreign mapping (a resolver answers with the *first* match, so
    /// appending a second line would leave the stale one winning), leaving the
    /// machine's name on loopback; only the name goes, so a mapping that also
    /// carries another name (`localhost`) keeps it. A machine whose name maps
    /// to one of its own interface addresses — GitHub's own `/etc/hosts`
    /// shape — is already correct and must be left alone.
    #[cfg(unix)]
    #[test]
    fn guest_hostname_script_rewrites_a_stale_foreign_mapping() {
        let temp = tempfile::tempdir().unwrap();
        let hosts = temp.path().join("hosts");
        let bin = temp.path().join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::write(
            &hosts,
            "127.0.0.1 localhost\n\
             10.1.0.59 runnervm.bnlheokxlokujiv1ylew4udf0c.gx.internal.cloudapp.net runnervmvrwv9\n\
             10.1.0.60 localhost runnervmvrwv9\n\
             # a comment that mentions runnervmvrwv9\n",
        )
        .unwrap();
        hostname_harness_stub(
            &bin.join("hostname"),
            "#!/bin/sh\n\
             if [ \"$1\" = -I ]; then printf '169.254.0.21 127.0.0.1\\n'; exit 0; fi\n\
             printf '%s\\n' runnervmvrwv9\n",
        );
        hostname_harness_stub(
            &bin.join("getent"),
            &format!(
                "#!/bin/sh\n\
                 [ \"$1\" = hosts ] || [ \"$1\" = ahosts ] || exit 2\n\
                 line=$(grep -E \"^[^#]*[[:space:]]$2([[:space:]]|$)\" {} | head -n 1)\n\
                 [ -n \"$line\" ] || exit 2\n\
                 printf '%s\\n' \"$line\"\n",
                shell_quote(hosts.to_str().unwrap())
            ),
        );
        hostname_harness_stub(&bin.join("sudo"), SUDO_STUB);
        let path = format!("{}:/usr/bin:/bin", bin.display());
        let run = || {
            std::process::Command::new("/bin/sh")
                .args(["-c", &guest_hostname_script_at(hosts.to_str().unwrap())])
                .env("PATH", &path)
                .output()
                .unwrap()
        };

        // Before: the name resolves — to an address the machine does not own.
        let before = std::process::Command::new("/bin/sh")
            .args(["-c", "getent hosts \"$(hostname)\""])
            .env("PATH", &path)
            .output()
            .unwrap();
        assert!(
            before.status.success(),
            "the stale mapping must answer first"
        );
        assert!(
            String::from_utf8_lossy(&before.stdout).starts_with("10.1.0.59"),
            "the trigger must reproduce: {}",
            String::from_utf8_lossy(&before.stdout)
        );

        let applied = run();
        assert!(
            applied.status.success(),
            "{}",
            String::from_utf8_lossy(&applied.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(&hosts).unwrap(),
            "127.0.0.1 localhost\n\
             10.1.0.59 runnervm.bnlheokxlokujiv1ylew4udf0c.gx.internal.cloudapp.net\n\
             10.1.0.60 localhost\n\
             # a comment that mentions runnervmvrwv9\n\
             127.0.0.1 runnervmvrwv9\n",
            "the machine's name must leave the foreign mapping, other names and \
             comments untouched"
        );
        let after = std::process::Command::new("/bin/sh")
            .args(["-c", "getent hosts \"$(hostname)\""])
            .env("PATH", &path)
            .output()
            .unwrap();
        assert!(
            String::from_utf8_lossy(&after.stdout).starts_with("127.0.0.1"),
            "the machine's name must resolve to a local address: {}",
            String::from_utf8_lossy(&after.stdout)
        );

        // GitHub's own shape — the name mapped to one of the machine's
        // interface addresses — is already correct and must not be rewritten.
        std::fs::write(&hosts, "127.0.0.1 localhost\n169.254.0.21 runnervmvrwv9\n").unwrap();
        let untouched = run();
        assert!(
            untouched.status.success(),
            "{}",
            String::from_utf8_lossy(&untouched.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(&hosts).unwrap(),
            "127.0.0.1 localhost\n169.254.0.21 runnervmvrwv9\n",
            "a name that resolves to a local interface address is already correct"
        );
    }

    /// A machine that still cannot resolve its name after the rewrite must
    /// fail provisioning, not keep printing the warning on every command
    /// forever.
    #[cfg(unix)]
    #[test]
    fn guest_hostname_script_fails_when_the_name_still_does_not_resolve() {
        let temp = tempfile::tempdir().unwrap();
        let hosts = temp.path().join("hosts");
        let bin = temp.path().join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::write(&hosts, "127.0.0.1 localhost\n").unwrap();
        hostname_harness_stub(
            &bin.join("hostname"),
            "#!/bin/sh\nprintf '%s\\n' fork-7c1f\n",
        );
        // A resolver that never finds the name: the rewrite lands, the re-check
        // does not, and the script must report it.
        hostname_harness_stub(&bin.join("getent"), "#!/bin/sh\nexit 2\n");
        hostname_harness_stub(
            &bin.join("sudo"),
            "#!/bin/sh\n[ \"$1\" = -n ] && shift\nexec \"$@\"\n",
        );

        let output = std::process::Command::new("/bin/sh")
            .args(["-c", &guest_hostname_script_at(hosts.to_str().unwrap())])
            .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
            .output()
            .unwrap();
        assert!(
            !output.status.success(),
            "a name that still does not resolve must fail provisioning"
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("does not resolve to an address"),
            "the failure must say what did not resolve: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// The init is the one script the provisioning path runs for everything a
    /// hosted VM's own init would have applied, so its composition is part of
    /// the contract: the hostname half first (the sysctl half escalates
    /// through passwordless sudo, which resolves the hostname on every
    /// invocation — with a stale mapping it would print
    /// `sudo: unable to resolve host` into the apply's stderr), each half in
    /// its own subshell (either half's early `exit 0` must not skip the
    /// other), and `set -e` so a half that fails fails provisioning.
    #[test]
    fn hosted_runtime_init_orders_the_hostname_before_the_sysctls() {
        let script = guest_hosted_runtime_init_script();
        let hostname = script
            .find("host=$(hostname")
            .unwrap_or_else(|| panic!("the init must resolve the machine's name: {script}"));
        let sysctl = script
            .find("guest sysctls already match the hosted image")
            .unwrap_or_else(|| panic!("the init must apply the hosted sysctls: {script}"));
        assert!(
            hostname < sysctl,
            "the hostname half must run first: {script}"
        );
        assert!(
            script.starts_with("set -e; ("),
            "each half must be its own subshell under set -e: {script}"
        );
        assert!(
            script.contains("); ( "),
            "the two halves must be separate subshells: {script}"
        );
        // Each half's early exit lives inside its own subshell, so neither
        // can end the other's half.
        assert!(
            script.contains("name_resolves_locally && exit 0"),
            "the hostname half's already-correct exit must stay in its subshell: {script}"
        );
        assert!(
            script.contains("guest sysctls already match the hosted image"),
            "the sysctl half's already-correct exit must stay in its subshell: {script}"
        );
        // The production roots, not a scratch path.
        assert!(script.contains("'/etc/hosts'"), "{script}");
        assert!(script.contains("root='/proc/sys'"), "{script}");
    }

    /// The expected golden runtime lives in `official-image.toml`, and
    /// `build.rs` compiles every flat key into a constant. This fails if the
    /// code and the config drift apart: a hand-edited literal, a key renamed
    /// in the file, a codegen that stops emitting one of them, or a composed
    /// raise that no longer agrees with the primitive values it is built from.
    #[test]
    fn guest_runtime_values_come_from_official_image_toml() {
        let config = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../official-image.toml"
        ))
        .expect("official-image.toml must be readable");
        let cfg = |key: &str| {
            config
                .lines()
                .filter_map(|line| {
                    let (name, value) = line.trim().split_once('=')?;
                    if name.trim() != key {
                        return None;
                    }
                    Some(value.trim().trim_matches('"').to_owned())
                })
                .next()
                .unwrap_or_else(|| panic!("official-image.toml is missing {key}"))
        };

        // The compiled constants are the file's values.
        assert_eq!(
            GOLDEN_RLIMIT_STACK_SOFT_KIB,
            cfg("golden_rlimit_stack_soft_kib")
        );
        assert_eq!(GOLDEN_RLIMIT_STACK_HARD, cfg("golden_rlimit_stack_hard"));
        assert_eq!(GOLDEN_RLIMIT_NOFILE_SOFT, cfg("golden_rlimit_nofile_soft"));
        assert_eq!(GOLDEN_RLIMIT_NOFILE_HARD, cfg("golden_rlimit_nofile_hard"));

        // The raises are the composed keys, and those agree with the values.
        assert_eq!(GUEST_STACK_ULIMIT, cfg("golden_stack_ulimit_raise"));
        assert_eq!(
            GUEST_STACK_ULIMIT_BEST_EFFORT,
            cfg("golden_stack_ulimit_raise_best_effort")
        );
        assert_eq!(GUEST_NOFILE_ULIMIT, cfg("golden_nofile_ulimit_raise"));
        assert_eq!(
            GUEST_NOFILE_ULIMIT_BEST_EFFORT,
            cfg("golden_nofile_ulimit_raise_best_effort")
        );
        assert_eq!(
            GUEST_STACK_ULIMIT,
            format!(
                "ulimit -Hs {} || exit 1; ulimit -Ss {} || exit 1",
                cfg("golden_rlimit_stack_hard"),
                cfg("golden_rlimit_stack_soft_kib")
            ),
            "golden_stack_ulimit_raise must compose golden_rlimit_stack_* and fail loudly"
        );
        assert_eq!(
            GUEST_STACK_ULIMIT_BEST_EFFORT,
            format!(
                "ulimit -Hs {hard} 2>/dev/null || true; ulimit -Ss {soft} 2>/dev/null || \
                 echo preloop: RLIMIT_STACK stays $(ulimit -Ss)KiB soft / $(ulimit -Hs) hard - \
                 raising it needs root and this launch keeps the exec channel identity >&2",
                hard = cfg("golden_rlimit_stack_hard"),
                soft = cfg("golden_rlimit_stack_soft_kib")
            ),
            "golden_stack_ulimit_raise_best_effort must compose golden_rlimit_stack_*"
        );
        assert_eq!(
            GUEST_NOFILE_ULIMIT,
            format!(
                "ulimit -Hn {hard} 2>/dev/null || true; ulimit -Sn {soft} || exit 1; \
                 ulimit -Hn {hard} || exit 1",
                hard = cfg("golden_rlimit_nofile_hard"),
                soft = cfg("golden_rlimit_nofile_soft")
            ),
            "golden_nofile_ulimit_raise must compose golden_rlimit_nofile_* in the \
             order-independent hard-soft-hard form and fail loudly after the first raise"
        );
        assert_eq!(
            GUEST_NOFILE_ULIMIT_BEST_EFFORT,
            format!(
                "ulimit -Hn {hard} 2>/dev/null; ulimit -Sn {soft} 2>/dev/null || true; \
                 ulimit -Hn {hard} 2>/dev/null || echo preloop: RLIMIT_NOFILE hard limit stays \
                 $(ulimit -Hn) - raising it needs root and this launch keeps the exec channel \
                 identity >&2",
                hard = cfg("golden_rlimit_nofile_hard"),
                soft = cfg("golden_rlimit_nofile_soft")
            ),
            "golden_nofile_ulimit_raise_best_effort must compose golden_rlimit_nofile_*"
        );
        // The strict forms may ignore only the first hard raise, which the
        // order-independent sequence expects to be refused; everything they
        // must actually apply ends the launch instead.
        assert_eq!(
            GUEST_NOFILE_ULIMIT.matches("|| exit 1").count(),
            2,
            "{GUEST_NOFILE_ULIMIT}"
        );
        assert!(
            GUEST_NOFILE_ULIMIT.starts_with(
                &format!(
                    "ulimit -Hn {} 2>/dev/null || true; ",
                    cfg("golden_rlimit_nofile_hard")
                )
            ),
            "only the first hard raise is allowed to be ignored: {GUEST_NOFILE_ULIMIT}"
        );
        assert_eq!(
            GUEST_STACK_ULIMIT.matches("|| exit 1").count(),
            2,
            "{GUEST_STACK_ULIMIT}"
        );
        assert!(
            GUEST_STACK_ULIMIT_BEST_EFFORT.contains("RLIMIT_STACK stays"),
            "the fallback must say what it could not raise, not swallow it"
        );
        assert!(
            GUEST_NOFILE_ULIMIT_BEST_EFFORT.contains("RLIMIT_NOFILE hard limit stays"),
            "the fallback must say what it could not raise, not swallow it"
        );

        // The live-chain prlimit forms agree with the same values.
        assert_eq!(GOLDEN_STACK_SOFT_BYTES, cfg("golden_stack_soft_bytes"));
        assert_eq!(
            GOLDEN_STACK_SOFT_BYTES.parse::<u64>().unwrap(),
            cfg("golden_rlimit_stack_soft_kib").parse::<u64>().unwrap() * 1024,
            "golden_stack_soft_bytes must be the soft stack in bytes"
        );
        assert_eq!(
            GOLDEN_STACK_PRLIMIT,
            format!(
                "{}:{}",
                cfg("golden_stack_soft_bytes"),
                cfg("golden_rlimit_stack_hard")
            ),
            "golden_stack_prlimit must compose the stack pair"
        );
        assert_eq!(
            GOLDEN_NOFILE_PRLIMIT,
            format!(
                "{}:{}",
                cfg("golden_rlimit_nofile_soft"),
                cfg("golden_rlimit_nofile_hard")
            ),
            "golden_nofile_prlimit must compose the descriptor pair"
        );

        // The applied sysctls are the file's `golden_sysctl_*` values: a
        // scheduled update that moves a key moves what the init applies with
        // it. The baseline those keys must hold is pinned independently in
        // `tests/golden_fidelity.rs` (`guest_sysctls_match_the_hosted_image`).
        for (key, value) in GITHUB_GUEST_SYSCTLS {
            let config_key = format!("golden_sysctl_{}", key.replace('.', "_"));
            assert_eq!(
                *value,
                cfg(&config_key),
                "{key} must come from {config_key} in official-image.toml"
            );
        }
        assert_eq!(
            GITHUB_GUEST_SYSCTLS.len(),
            3,
            "the hosted sysctl set changed — update official-image.toml and this test together"
        );
    }

    /// The composed init run end to end against scratch roots: the fork's own
    /// name resolves and the sysctls reach the hosted values in one exec, and
    /// a second run changes nothing. Textual order alone would not catch a
    /// half whose `exit 0` swallows the other — this runs both halves in one
    /// shell, the way provisioning does.
    #[cfg(unix)]
    #[test]
    fn hosted_runtime_init_applies_both_halves_in_one_run() {
        let temp = tempfile::tempdir().unwrap();
        let hosts = temp.path().join("hosts");
        let root = temp.path().join("sys");
        let bin = temp.path().join("bin");
        std::fs::create_dir_all(root.join("vm")).unwrap();
        std::fs::create_dir_all(root.join("fs/inotify")).unwrap();
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::write(&hosts, "127.0.0.1 localhost\n").unwrap();
        std::fs::write(root.join("vm/max_map_count"), "65530").unwrap();
        std::fs::write(root.join("fs/inotify/max_user_watches"), "655360").unwrap();
        std::fs::write(root.join("fs/inotify/max_user_instances"), "128").unwrap();
        // The sysctl half's procps is absent here, so the apply writes the
        // scratch tree directly — the same branch a guest without procps
        // takes. Every other tool both halves call is linked in, so the test
        // exercises the real scripts rather than the harness PATH.
        for tool in [
            "cat", "tr", "id", "base64", "sh", "awk", "sed", "sort", "cut", "grep", "mktemp",
            "tee", "rm", "uname", "head",
        ] {
            symlink_tool(&bin, tool);
        }
        hostname_harness_stub(
            &bin.join("hostname"),
            "#!/bin/sh\nprintf '%s\\n' fork-7c1f\n",
        );
        hostname_harness_stub(
            &bin.join("getent"),
            &format!(
                "#!/bin/sh\n\
                 [ \"$1\" = hosts ] || [ \"$1\" = ahosts ] || exit 2\n\
                 line=$(grep -E \"^[^#]*[[:space:]]$2([[:space:]]|$)\" {} | head -n 1)\n\
                 [ -n \"$line\" ] || exit 2\n\
                 printf '%s\\n' \"$line\"\n",
                shell_quote(hosts.to_str().unwrap())
            ),
        );
        hostname_harness_stub(&bin.join("sudo"), SUDO_STUB);

        let script = hosted_runtime_init_script_at(hosts.to_str().unwrap(), root.to_str().unwrap());
        let run = || {
            std::process::Command::new("/bin/sh")
                .args(["-c", &script])
                .env("PATH", bin.display().to_string())
                .output()
                .unwrap()
        };
        let first = run();
        assert!(
            first.status.success(),
            "{}",
            String::from_utf8_lossy(&first.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(&hosts).unwrap(),
            "127.0.0.1 localhost\n127.0.0.1 fork-7c1f\n",
            "the hostname half must land in the same run"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("vm/max_map_count")).unwrap(),
            "262144",
            "the sysctl half must land in the same run"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("fs/inotify/max_user_instances")).unwrap(),
            "1280"
        );
        // Idempotent: the second run writes nothing at all.
        let second = run();
        assert!(
            second.status.success(),
            "{}",
            String::from_utf8_lossy(&second.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(&hosts).unwrap(),
            "127.0.0.1 localhost\n127.0.0.1 fork-7c1f\n"
        );
    }

    #[test]
    fn runner_user_wrapper_drops_privileges_and_creates_the_account() {
        let mut config = test_config(false);
        config.runner_user = Some("runner".to_owned());
        config.runner_uid = Some(1001);
        let argv = vec![
            "/opt/preloop/bin/preloop-runner".to_owned(),
            "run".to_owned(),
            "--once".to_owned(),
        ];

        let wrapped = as_runner_user(&config, &argv);
        assert_eq!(wrapped[0], "sh");
        assert_eq!(wrapped[1], "-c");
        let script = &wrapped[2];
        assert!(script.contains("useradd -m -u 1001 runner"), "{script}");
        assert!(
            script.contains("chmod 777 /run/preloop-control"),
            "{script}"
        );
        assert!(
            script.contains("NOPASSWD: ALL"),
            "the runner account must be able to sudo non-interactively, \
             like the GitHub-hosted runner user: {script}"
        );
        assert!(
            script.contains("| base64 -d | sudo -n sh 2>/dev/null || true"),
            "{script}"
        );
        // Both branches run the launch (limit raise + setpriv drop) from a
        // base64'd script; the image-user branch pipes it through sudo so the
        // raise and the --init-groups setgroups run as root. Decode every
        // blob and assert across them.
        let blobs: Vec<String> = script
            .split("| base64 -d")
            .filter_map(|part| {
                let close = part.rfind('\'')?;
                let open = part[..close].rfind('\'')?;
                Some(part[open + 1..close].to_owned())
            })
            .collect();
        let all = blobs
            .iter()
            .map(|b| {
                String::from_utf8(base64::engine::general_purpose::STANDARD.decode(b).unwrap())
                    .unwrap()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            all.contains(GUEST_NOFILE_ULIMIT),
            "the runner and every step it spawns must run on the hosted descriptor \
             limit (GitHub-hosted: Max open files 65536/65536), not the exec \
             channel's 1024/4096: {all}"
        );
        assert!(
            all.contains(GUEST_STACK_ULIMIT),
            "the runner and every step it spawns must run on the hosted stack \
             size, not the VM init's half of it: {all}"
        );
        assert!(
            all.contains("setpriv --reuid 1001 --regid 1001 --init-groups"),
            "{all}"
        );
        assert!(!all.contains("--keep-groups"), "{all}");
        assert!(
            all.contains("'/opt/preloop/bin/preloop-runner' 'run' '--once'"),
            "{all}"
        );
        assert!(
            all.contains("PRELOOP_RUNNER_USER=runner PRELOOP_RUNNER_UID=1001"),
            "{all}"
        );
    }

    /// The container engine carries the hosted stack limit too: a container's
    /// processes inherit the daemon's limits, so `container:` jobs would
    /// otherwise run on the VM init's half-sized stack while the same step on
    /// GitHub runs on 16 MiB.
    #[test]
    fn docker_start_command_raises_the_hosted_stack_limit() {
        let command = docker_start_command();
        assert_eq!(command[0], "sh");
        assert_eq!(command[1], "-c");
        let script = &command[2];
        // The strict root-or-sudo wrapper splices the launch inline for the
        // root branch and base64'd for the image-user branch; both carry the
        // same script, so the inline copy is the one to order-check. It keeps
        // the status of what it runs, so a refused sudo cannot look like a
        // daemon that started.
        assert!(
            script.contains("base64 -d | sudo -n sh; fi"),
            "the image-user branch must preserve the failure status: {script}"
        );
        let raise = script
            .find(GUEST_STACK_ULIMIT)
            .unwrap_or_else(|| {
                panic!("the container engine must start with the hosted stack limit: {script}")
            });
        // A container's processes inherit the daemon's limits, so the
        // descriptor pair GitHub-hosted containers carry (65536/65536) has to
        // be raised here too, in the same launch.
        let nofile = script
            .find(GUEST_NOFILE_ULIMIT)
            .unwrap_or_else(|| {
                panic!("the container engine must start on the hosted descriptor limit: {script}")
            });
        let spawn = script
            .find("dockerd >/var/log/dockerd.log")
            .unwrap_or_else(|| panic!("the launch must still start dockerd: {script}"));
        assert!(
            raise < spawn && nofile < spawn,
            "both raises must precede the daemon they apply to: {script}"
        );
    }

    /// A `@@NAME@@` token the renderer does not substitute is still valid
    /// shell — `[ "$stack" -lt @@…@@ ]` parses — so a missed token would
    /// reach a guest and fail there, reading as a container-engine bug on a
    /// real workflow instead of as a broken render here.
    #[test]
    fn docker_start_script_renders_every_placeholder() {
        let script = render_docker_start_script();
        assert!(
            !script.contains("@@"),
            "an unsubstituted token would reach the guest: {script}"
        );
    }

    /// A fork inherits its golden's live daemon chain, and a running process
    /// keeps the limits it was born with. A golden baked before the raise
    /// would hand containers 8192 KiB no matter what the newly-started daemon
    /// gets, so the command re-raises the inherited chain in place instead of
    /// trusting it.
    #[test]
    fn docker_start_command_reraise_covers_an_inherited_engine_chain() {
        let command = docker_start_command();
        let script = &command[2];
        let guard = script
            .find("raise_engine_chain() {")
            .unwrap_or_else(|| panic!("the inherited chain must be re-raised: {script}"));
        // Reached before the inherited daemon short-circuits the launch, or a
        // pre-fix golden keeps its container steps at half the hosted stack.
        let inherited_exit = script
            .find("docker info >/dev/null 2>&1 && { raise_engine_chain || exit 1; exit 0; }")
            .unwrap_or_else(|| {
                panic!("the inherited daemon must be re-raised, not trusted: {script}")
            });
        assert!(guard < inherited_exit, "{script}");
        // prlimit speaks raw bytes; 16777216 is the hosted 16384 KiB soft
        // limit, with the hard limit left unlimited as the image sets it.
        assert!(
            script.contains(&format!("--stack={GOLDEN_STACK_PRLIMIT}")),
            "the live chain must be raised to the hosted stack: {script}"
        );
        assert!(
            script.contains("\"/proc/$pid/limits\""),
            "the raise must only touch a chain still below the hosted limit: {script}"
        );
        // Containerd spawns every container shim, so its limits — not only
        // dockerd's — are what a container process inherits.
        assert!(script.contains("pgrep -x containerd"), "{script}");
        // The descriptor pair is re-raised too: a container inherits the
        // engine's nofile, and a golden baked before that raise would keep
        // handing containers the exec channel's.
        assert!(
            script.contains(&format!("--nofile={GOLDEN_NOFILE_PRLIMIT}")),
            "the live chain must be raised to the hosted descriptor limit too: {script}"
        );
        assert!(
            script.contains("\"Max/open/files\""),
            "the nofile raise must only touch a chain still below the hosted limit: {script}"
        );
        // A fresh daemon can still adopt a surviving containerd from the
        // golden, so both start retries raise the chain too. The pattern
        // stops at `fi`: what follows it is a newline now that the script
        // lives in a file, and the separator is not what this pins.
        assert_eq!(
            script
                .matches("then raise_engine_chain || exit 1; exit 0; fi")
                .count(),
            2,
            "{script}"
        );
    }

    /// The preload daemon is the engine a fork inherits, so the golden must
    /// start it on the hosted stack limit: a container's processes inherit the
    /// limits of the chain that spawned them, and nothing raises the stack
    /// again between a golden's preload and a job's container step.
    #[test]
    fn preload_images_command_raises_the_hosted_stack_limit() {
        let command = preload_images_command(&["postgres:16-alpine".to_owned()]);
        assert_eq!(command[0], "sh");
        assert_eq!(command[1], "-c");
        let script = &command[2];
        let raise = script
            .find(GUEST_STACK_ULIMIT)
            .unwrap_or_else(|| panic!("the preloaded engine must be raised: {script}"));
        let nofile = script
            .find(GUEST_NOFILE_ULIMIT)
            .unwrap_or_else(|| {
                panic!("the preloaded engine must carry the hosted descriptor limit: {script}")
            });
        let spawn = script
            .find("dockerd >/var/log/dockerd-preload.log")
            .unwrap_or_else(|| panic!("the preload must still start a daemon: {script}"));
        assert!(
            raise < spawn && nofile < spawn,
            "both raises must precede the daemon they apply to: {script}"
        );
        assert!(script.contains("'postgres:16-alpine'"), "{script}");
    }

    /// A `runner_user`-less (or `root`) launch keeps the exec channel's
    /// identity, so it must not assume it can raise a hard limit — neither
    /// pair's. Pin both halves of that: the wrapper adds the two best-effort
    /// pairs without re-quoting the argv it wraps, and those survive an
    /// inherited hard limit below the hosted one, where the strict forms the
    /// privileged sites use fail the launch instead.
    #[test]
    fn pass_through_launch_adds_the_hosted_limits_without_switching_accounts() {
        let config = test_config(false);
        let argv = vec![
            "/opt/preloop/bin/preloop-runner".to_owned(),
            "run".to_owned(),
            "--once".to_owned(),
        ];
        let wrapped = as_runner_user(&config, &argv);
        assert_eq!(wrapped[0], "sh");
        assert_eq!(wrapped[1], "-c");
        assert!(
            wrapped[2].contains(GUEST_STACK_ULIMIT_BEST_EFFORT),
            "{}",
            wrapped[2]
        );
        assert!(
            wrapped[2].contains(GUEST_NOFILE_ULIMIT_BEST_EFFORT),
            "{}",
            wrapped[2]
        );
        assert!(
            !wrapped[2].contains("|| exit 1"),
            "the pass-through launch must not fail on a limit it cannot raise: {}",
            wrapped[2]
        );
        assert!(wrapped[2].contains("exec \"$@\""), "{}", wrapped[2]);
        assert_eq!(wrapped[3], "sh");
        assert_eq!(
            &wrapped[4..],
            &argv[..],
            "the launch must stay byte-identical"
        );
    }

    /// The best-effort form exists because a launch that cannot raise the hard
    /// limit must still start: the exec channel's defaults can be below the
    /// hosted 65536, and `ulimit` prints `EPERM` to stderr and fails when it
    /// cannot comply. A root exec reaches the hosted pair; a non-root exec
    /// keeps what it inherited, silently. Both are exercised against a lowered
    /// hard limit so the semantics cannot drift into either failing the launch
    /// or hiding a genuine failure at the strict sites.
    #[cfg(unix)]
    #[test]
    fn pass_through_nofile_raise_is_best_effort() {
        let run = |limits: &str| {
            std::process::Command::new("/bin/sh")
                .args([
                    "-c",
                    &format!(
                        // Soft first: lowering the hard below the current soft
                        // is EINVAL, and a host whose shell starts at a high
                        // soft limit (macOS: 1048576) would otherwise leave the
                        // setup's own error on stderr.
                        "ulimit -Sn 1024; ulimit -Hn 1024; {limits}; \
                         printf '%s %s' \"$(ulimit -Hn)\" \"$(ulimit -Sn)\""
                    ),
                ])
                .output()
                .unwrap()
        };
        let uid = std::process::Command::new("id").arg("-u").output().unwrap();
        let root = String::from_utf8_lossy(&uid.stdout).trim() == "0";
        let best_effort = run(GUEST_NOFILE_ULIMIT_BEST_EFFORT);
        assert!(
            best_effort.status.success(),
            "a launch that cannot raise the hard limit must still start: {}",
            String::from_utf8_lossy(&best_effort.stderr)
        );
        let stderr = String::from_utf8_lossy(&best_effort.stderr);
        assert!(
            stderr.is_empty() || stderr.contains("RLIMIT_NOFILE hard limit stays"),
            "the fallback must either reach the hosted pair silently or say what it \
             could not raise: {stderr}"
        );
        // Either the raise took (root, or a platform that permits raising the
        // hard limit) or the launch kept what it inherited — never a third
        // state, and never a failure.
        let pair = String::from_utf8_lossy(&best_effort.stdout).to_string();
        assert!(
            pair == "65536 65536" || pair == "1024 1024",
            "the best-effort pair must be the hosted one or the inherited one, got {pair:?}"
        );
        if root {
            assert_eq!(
                pair, "65536 65536",
                "a root exec must reach the hosted pair"
            );
        }
        // The strict form the privileged sites use has no fallback past its
        // first hard raise: the raises it must apply end the launch, which is
        // what makes a failed raise observable instead of silent.
        assert_eq!(
            GUEST_NOFILE_ULIMIT,
            format!(
                "ulimit -Hn {hard} 2>/dev/null || true; ulimit -Sn {soft} || exit 1; \
                 ulimit -Hn {hard} || exit 1",
                hard = GOLDEN_RLIMIT_NOFILE_HARD,
                soft = GOLDEN_RLIMIT_NOFILE_SOFT
            ),
            "the privileged form must stay strict"
        );
        assert!(
            GUEST_NOFILE_ULIMIT_BEST_EFFORT.contains("|| true"),
            "the fallback must swallow only the raise's failure"
        );
    }

    /// The stack pair's best-effort form exists for the same reason the
    /// descriptor one does: re-setting a hard limit needs no privilege only
    /// while it does not move, so a launch that keeps the exec channel's
    /// identity and inherits a finite hard stack must still start — with a
    /// line saying which limit stayed — where the strict form the privileged
    /// sites use ends the launch.
    #[cfg(unix)]
    #[test]
    fn pass_through_stack_raise_is_best_effort() {
        let run = |limits: &str| {
            std::process::Command::new("/bin/sh")
                .args([
                    "-c",
                    &format!(
                        // A finite hard stack under a soft limit it can hold:
                        // the pair a hardened base can hand a launch.
                        "ulimit -Ss 8192; ulimit -Hs 8192; \
                         start=\"$(ulimit -Ss)/$(ulimit -Hs)\"; {limits}; \
                         printf '%s -> %s/%s' \"$start\" \"$(ulimit -Ss)\" \"$(ulimit -Hs)\""
                    ),
                ])
                .output()
                .unwrap()
        };
        let best_effort = run(GUEST_STACK_ULIMIT_BEST_EFFORT);
        assert!(
            best_effort.status.success(),
            "a launch that cannot raise the hard stack must still start: {}",
            String::from_utf8_lossy(&best_effort.stderr)
        );
        let stderr = String::from_utf8_lossy(&best_effort.stderr);
        assert!(
            stderr.is_empty() || stderr.contains("RLIMIT_STACK stays"),
            "the fallback must either reach the hosted pair silently or say what it \
             could not raise: {stderr}"
        );
        // Either the raise took (root, or a platform that permits raising the
        // hard stack) or the launch kept what it inherited — never a third
        // state, and never a failure.
        let pair = String::from_utf8_lossy(&best_effort.stdout).to_string();
        let (start, end) = pair
            .split_once(" -> ")
            .unwrap_or_else(|| panic!("the inherited pair must be readable: {pair}"));
        assert!(
            end == "16384/unlimited" || end == start,
            "the best-effort stack must be the hosted one or the inherited one, got {pair:?}"
        );
        if end == start {
            assert!(
                stderr.contains("RLIMIT_STACK stays"),
                "keeping the inherited stack must be reported: {pair:?} {stderr}"
            );
        }
    }

    /// The strict nofile raise must reach the hosted 65536/65536 from both
    /// inherited pairs, whichever direction they are in.
    ///
    /// Below the target, AgentENV's exec channel starts at soft 1024 under a
    /// hard 4096: the old soft-first form raised the soft first, hit `EPERM`
    /// above the old hard, and was swallowed by `|| true` at the best-effort
    /// sites — then the hard raise left the pair at 1024/65536 and the parity
    /// probe failed. Above the target, the soft limit is at the hard one —
    /// 1048576/1048576 where the host allows it (macOS), a Linux runner's
    /// inherited 524288/524288 otherwise — and both limits have to come down.
    /// The hard-soft-hard order handles each: the first hard raise is the
    /// privileged one that may be refused (ignored when the inherited hard
    /// already sits above the target, where the kernel rejects lowering it
    /// under a higher soft limit), then the soft is set, then the hard is
    /// pinned — which succeeds when lowering from above once the soft is at
    /// the target.
    #[cfg(unix)]
    #[test]
    fn nofile_raise_reaches_the_hosted_pair_from_below_and_above() {
        // Dash is the guest's `sh`; prefer it so the parser and the `ulimit`
        // semantics under test are the guests'.
        let shell = ["dash", "sh"]
            .into_iter()
            .find(|candidate| {
                std::process::Command::new(candidate)
                    .args(["-n", "-c", ":"])
                    .status()
                    .map(|status| status.success())
                    .unwrap_or(false)
            })
            .expect("a POSIX shell must be available");
        // Set up the inherited pair, record it, run the raise, record the
        // result: `start -> end`, hard/soft. A raise the strict form ends the
        // launch on leaves no line behind — the exit status is the signal.
        let run = |setup: &str| {
            std::process::Command::new(shell)
                .args([
                    "-c",
                    &format!(
                        "{setup}; start=\"$(ulimit -Hn)/$(ulimit -Sn)\"; {GUEST_NOFILE_ULIMIT}; \
                         printf '%s -> %s/%s' \"$start\" \"$(ulimit -Hn)\" \"$(ulimit -Sn)\""
                    ),
                ])
                .output()
                .unwrap()
        };
        let uid = std::process::Command::new("id").arg("-u").output().unwrap();
        let root = String::from_utf8_lossy(&uid.stdout).trim() == "0";

        // (soft 1024, hard 4096): the pair AgentENV's exec channel hands a
        // job. Only root can raise the hard limit, so an unprivileged strict
        // raise must end the launch rather than leave the pair below the
        // hosted one.
        let below = run("ulimit -Sn 1024; ulimit -Hn 4096");
        let below_out = String::from_utf8_lossy(&below.stdout).to_string();
        if root {
            let (start, end) = below_out
                .split_once(" -> ")
                .unwrap_or_else(|| panic!("the low pair must be settable: {below_out}"));
            assert_eq!(start, "4096/1024", "{below_out}");
            assert_eq!(
                end, "65536/65536",
                "a privileged launch must reach the hosted pair from below: {below_out}"
            );
        } else {
            assert!(
                !below.status.success(),
                "an unprivileged launch that cannot raise the hard limit must fail \
                 the strict form, not continue below the hosted pair: {below_out}"
            );
            assert!(
                !below_out.contains(" -> "),
                "the launch must end before it reports a pair it never reached: {below_out}"
            );
        }

        // An inherited pair already above the target: the soft limit raised to
        // the hard one — the state the soft-before-hard order broke (a soft
        // limit above the target, under a hard limit at or above it). The hard
        // is capped at the hosted 1048576 when the host reports `unlimited`
        // (macOS) or higher; a Linux runner's inherited 524288 works as-is.
        // Both limits only come down here, so no privilege is needed.
        let above = run(
            "H=$(ulimit -Hn); case \"$H\" in unlimited|*[!0-9]*) H=1048576 ;; esac; \
             if [ \"$H\" -gt 1048576 ]; then H=1048576; fi; \
             ulimit -Hn \"$H\" 2>/dev/null; ulimit -Sn \"$H\"; ulimit -Hn \"$H\"",
        );
        assert!(
            above.status.success(),
            "coming down from a higher pair must not fail the launch: {}",
            String::from_utf8_lossy(&above.stderr)
        );
        let above_out = String::from_utf8_lossy(&above.stdout).to_string();
        let (start, end) = above_out
            .split_once(" -> ")
            .unwrap_or_else(|| panic!("the high pair must be settable: {above_out}"));
        let start_pair = |value: &str| {
            value
                .split('/')
                .map(|part| match part {
                    "unlimited" => u64::MAX,
                    other => other.parse::<u64>().unwrap_or_else(|_| {
                        panic!("the highest pair must read back as limits: {above_out}")
                    }),
                })
                .collect::<Vec<_>>()
        };
        let limits = start_pair(start);
        assert!(
            limits.len() == 2,
            "the highest pair must be hard/soft: {above_out}"
        );
        let (hard, soft) = (limits[0], limits[1]);
        let target = GOLDEN_RLIMIT_NOFILE_SOFT.parse::<u64>().unwrap();
        if hard < target {
            // The host's hard limit is below the hosted value and cannot be
            // raised without privilege, so there is no above-target pair to
            // hold; the below-target case above still covers that host.
            eprintln!("no above-target nofile pair on this host: {above_out}");
            return;
        }
        assert!(
            soft >= target,
            "the soft limit must have been raised to the hard one: {above_out}"
        );
        assert_eq!(
            end, "65536/65536",
            "a pair above the target must come down to the hosted one: {above_out}"
        );
    }

    /// A base that is not the official runner image may lack `sudo`
    /// entirely, so `/etc/sudoers.d` does not exist and `useradd` is not on
    /// the exec shell's default PATH. Every best-effort provisioning step
    /// must tolerate that instead of aborting the script: verified live on
    /// AgentENV, where the bare `ubuntu:24.04` sandbox hit exactly this and
    /// the runner came up with an unchowned root and no account.
    #[test]
    fn runner_user_wrapper_survives_a_sudoless_bare_base() {
        let mut config = test_config(false);
        config.runner_user = Some("runner".to_owned());
        config.runner_uid = Some(1001);
        let argv = vec![
            "/opt/preloop/bin/preloop-runner".to_owned(),
            "run".to_owned(),
        ];
        let script = &as_runner_user(&config, &argv)[2];

        // The exec shell's PATH lacks /usr/sbin; the account tools live there.
        assert!(
            script.contains("PATH=/usr/sbin:/usr/bin:/sbin:/bin:$PATH"),
            "provisioning must pin sbin into PATH, got: {script}"
        );
        // A missing /etc/sudoers.d (no sudo installed) must not abort the
        // provisioning chain at the redirect.
        assert!(script.contains("mkdir -p /etc/sudoers.d"), "{script}");
        assert!(
            script.contains("/etc/sudoers.d/preloop-runner 2>/dev/null \\\n") || {
                let decoded = script.split("| base64 -d | sh").next().unwrap_or("");
                decoded.contains("/etc/sudoers.d/preloop-runner 2>/dev/null")
            },
            "the sudoers write must tolerate a missing directory, got: {script}"
        );
        // The chown that hands the runtime dirs to the account is the step
        // whose loss silently broke configure on the live host; it must be
        // best-effort too, and must run before the inner script.
        assert!(
            script.contains("chown 1001:1001 /run/user/1001 2>/dev/null || true"),
            "{script}"
        );
    }

    /// Unset and root still skip the account switch, but not the limits: the
    /// launch is wrapped around the same argv, so the exec channel's identity
    /// is untouched while the runner and every step it spawns inherit the
    /// hosted stack, and the descriptor pair best-effort (a hard limit can
    /// only be raised by root, and this path cannot assume it).
    #[test]
    fn runner_user_wrapper_passes_root_and_unset_through_with_the_hosted_limits() {
        let mut config = test_config(false);
        let argv = vec![
            "/usr/bin/env".to_owned(),
            "PRELOOP_MACHINE_NAME=runner".to_owned(),
            "/opt/preloop/bin/preloop-runner".to_owned(),
            "run".to_owned(),
        ];
        for user in [None, Some("root".to_owned())] {
            config.runner_user = user.clone();
            let wrapped = as_runner_user(&config, &argv);
            assert_eq!(wrapped[0], "sh", "{user:?}");
            assert_eq!(wrapped[1], "-c", "{user:?}");
            assert_eq!(
                wrapped[2],
                format!(
                    "{GUEST_STACK_ULIMIT_BEST_EFFORT}; {GUEST_NOFILE_ULIMIT_BEST_EFFORT}; \
                     exec \"$@\""
                ),
                "the pass-through launch must run on the hosted limits best-effort: {user:?}"
            );
            // The original argv travels as the shell's positional parameters,
            // so nothing is re-quoted and the launch is untouched.
            assert_eq!(wrapped[3], "sh", "the shell needs a $0 placeholder");
            assert_eq!(&wrapped[4..], argv.as_slice(), "{user:?}");
            // No privilege drop on this path: setpriv would change the
            // account behavior the launch had.
            assert!(!wrapped[2].contains("setpriv"), "{user:?} {}", wrapped[2]);
        }
    }

    #[test]
    fn shell_quote_escapes_single_quotes() {
        assert_eq!(shell_quote("plain"), "'plain'");
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
    }

    #[test]
    fn rosetta_multiarch_script_parses_as_posix_sh() {
        // A spliced fragment ending in a newline once put `;` at the start of
        // line 15 (`sh: 15: Syntax error: ";" unexpected`): every fresh golden
        // bake failed, and the pool fell back to plain-Ubuntu runners.
        let output = std::process::Command::new("sh")
            .args(["-n", "-c", &rosetta_multiarch_script()])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "rosetta multiarch script does not parse: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// Every script the guest runs must parse as POSIX sh, rendered with the
    /// values `official-image.toml` compiles in.
    ///
    /// The scripts are assembled from `;`-joined statements across `\`
    /// continuations into one physical line, so a `#` comment inside them
    /// swallows whatever follows it on that line. The dockerd start command
    /// shipped exactly that to production: the comment block above the
    /// engine-chain failure check made dash reject the whole script with
    /// `Syntax error: "else" unexpected`, so every per-runner container
    /// engine failed to start (the hostname script had the same bug before
    /// `f0341dc3`). A literal `-n` parse of each rendered script is the only
    /// check that stays honest as the text is assembled — the textual
    /// assertions elsewhere pin the statements, not their syntax.
    #[test]
    fn every_generated_script_parses_as_posix_sh() {
        // The guest's /bin/sh is dash; use it when this host has one, so the
        // parser under test is the one the guests actually run.
        let shell = ["dash", "sh"]
            .into_iter()
            .find(|candidate| {
                std::process::Command::new(candidate)
                    .args(["-n", "-c", ":"])
                    .status()
                    .map(|status| status.success())
                    .unwrap_or(false)
            })
            .expect("a POSIX shell must be available");

        let config = test_config(false);
        let argv = vec![
            "/opt/preloop/bin/preloop-runner".to_owned(),
            "run".to_owned(),
            "--once".to_owned(),
        ];
        let mut switched = config.clone();
        switched.runner_user = Some("runner".to_owned());
        switched.runner_uid = Some(1001);

        let scripts: Vec<(String, String)> = vec![
            ("guest_hostname_script".to_owned(), guest_hostname_script()),
            ("guest_sysctl_script".to_owned(), guest_sysctl_script()),
            (
                "guest_hosted_runtime_init_script".to_owned(),
                guest_hosted_runtime_init_script(),
            ),
            (
                "runner_account_script".to_owned(),
                runner_account_script(DEFAULT_RUNNER_USER, DEFAULT_RUNNER_UID),
            ),
            (
                "docker_start_command".to_owned(),
                docker_start_command()[2].clone(),
            ),
            (
                "preload_images_command".to_owned(),
                preload_images_command(&["postgres:16-alpine".to_owned()])[2].clone(),
            ),
            (
                "rosetta_multiarch_script".to_owned(),
                rosetta_multiarch_script(),
            ),
            (
                "scope_rosetta_apt_sources".to_owned(),
                scope_rosetta_apt_sources("/etc/apt"),
            ),
            (
                "with_guest_hosted_limits".to_owned(),
                with_guest_hosted_limits(&argv)[2].clone(),
            ),
            (
                "as_runner_user (pass-through)".to_owned(),
                as_runner_user(&config, &argv)[2].clone(),
            ),
            (
                "as_runner_user (runner account)".to_owned(),
                as_runner_user(&switched, &argv)[2].clone(),
            ),
            (
                "run_as_root_or_sudo".to_owned(),
                run_as_root_or_sudo("true"),
            ),
            (
                "run_as_root_or_sudo_strict".to_owned(),
                run_as_root_or_sudo_strict("true"),
            ),
        ];
        // The curated-bake scripts this list used to carry
        // (`base_install_script`, `runner_ownership_reconcile_script`,
        // `apt_lists_refresh_command`, the toolchain install/verify commands)
        // exist only on this branch's lineage: the main lineage replaces them
        // with the golden-contract scripts, so listing them here would break
        // the test build on a merged tree. Add that lineage's
        // `golden_contract_script` / `golden_ownership_script` /
        // `golden_glibc_check_script` here when the merge happens.

        for (name, script) in &scripts {
            let output = std::process::Command::new(shell)
                .args(["-n", "-c", script])
                .output()
                .expect("the POSIX shell must run");
            assert!(
                output.status.success(),
                "{name} does not parse as POSIX sh ({}):\n{script}",
                String::from_utf8_lossy(&output.stderr)
            );
            // No `#` comment may reach the composed text: the halves are
            // joined into one physical line, where it would swallow code.
            // (awk programs address `#` as data and are excluded.)
            assert!(
                !script.contains("; #"),
                "{name} carries a shell comment inside the joined script:\n{script}"
            );
        }
    }

    /// The rootfs selected by `ubuntu-22.04` has one-line sources while
    /// 24.04 uses deb822. Test the guest shell against both real layouts:
    /// missing `ubuntu.sources` used to fail every environment-golden bake
    /// before any runner could register.
    #[cfg(target_os = "linux")]
    #[test]
    fn rosetta_scopes_both_ubuntu_apt_source_layouts() {
        let temp = tempfile::tempdir().unwrap();
        for (release, native_file, native_contents) in [
            (
                "22.04",
                "sources.list",
                "deb http://ports.ubuntu.com/ubuntu-ports jammy main\n\
                 deb [signed-by=/key] http://ports.ubuntu.com/ubuntu-ports jammy-updates main\n",
            ),
            (
                "24.04",
                "sources.list.d/ubuntu.sources",
                "Types: deb\nURIs: http://ports.ubuntu.com/ubuntu-ports\nSuites: noble\n\n\
                 Types: deb\nURIs: http://ports.ubuntu.com/ubuntu-ports\nSuites: noble-security\n",
            ),
        ] {
            let apt = temp.path().join(release).join("apt");
            std::fs::create_dir_all(apt.join("sources.list.d")).unwrap();
            std::fs::write(apt.join(native_file), native_contents).unwrap();
            std::fs::write(
                apt.join("sources.list.d/extra.list"),
                "deb https://example.invalid stable main\n",
            )
            .unwrap();
            std::fs::write(
                apt.join("sources.list.d/preloop-amd64.list"),
                "deb [arch=amd64] http://archive.ubuntu.com/ubuntu jammy main\n",
            )
            .unwrap();

            for _ in 0..2 {
                let result = std::process::Command::new("sh")
                    .args(["-ec", &scope_rosetta_apt_sources(apt.to_str().unwrap())])
                    .output()
                    .unwrap();
                assert!(
                    result.status.success(),
                    "{release}: {}",
                    String::from_utf8_lossy(&result.stderr)
                );
            }
            let native = std::fs::read_to_string(apt.join(native_file)).unwrap();
            if release == "22.04" {
                assert!(native.contains("deb [arch=arm64] http://ports.ubuntu.com"));
                assert!(native.contains("deb [arch=arm64 signed-by=/key]"));
                assert!(!native.contains("[arch=arm64 arch=arm64]"));
            } else {
                assert_eq!(native.matches("Architectures: arm64").count(), 2);
            }
            assert_eq!(
                std::fs::read_to_string(apt.join("sources.list.d/extra.list")).unwrap(),
                "deb [arch=arm64] https://example.invalid stable main\n"
            );
            assert_eq!(
                std::fs::read_to_string(apt.join("sources.list.d/preloop-amd64.list")).unwrap(),
                "deb [arch=amd64] http://archive.ubuntu.com/ubuntu jammy main\n"
            );
        }

        let unsupported = temp.path().join("no-sources");
        std::fs::create_dir_all(&unsupported).unwrap();
        let status = std::process::Command::new("sh")
            .args([
                "-ec",
                &scope_rosetta_apt_sources(unsupported.to_str().unwrap()),
            ])
            .status()
            .unwrap();
        assert!(!status.success(), "unknown source layouts must fail closed");
    }

    #[test]
    fn sudo_wrapper_strict_variant_preserves_failures() {
        // The wrappers only diverge off-root: uid 0 runs the script directly
        // in both, so the distinction is untestable there (CI guests often
        // exec as root).
        let uid = std::process::Command::new("id")
            .arg("-u")
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .and_then(|s| s.trim().parse::<u32>().ok());
        if uid == Some(0) {
            return;
        }
        let run = |script: String| {
            std::process::Command::new("sh")
                .args(["-c", &script])
                .output()
                .unwrap()
        };
        // Best-effort: a failing script — or sudo not existing at all —
        // collapses to success. Provisioning steps that cannot verify the
        // outcome afterwards must never use this form.
        assert!(run(run_as_root_or_sudo("exit 7")).status.success());
        // Strict: the guest's status survives whether sudo refused (no
        // passwordless rule / binary absent) or ran a failing script.
        assert!(!run(run_as_root_or_sudo_strict("exit 7")).status.success());
        // And a strict wrapper still reports real success when sudo works.
        let sudo_usable = std::process::Command::new("sudo")
            .args(["-n", "true"])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if sudo_usable {
            assert!(run(run_as_root_or_sudo_strict("exit 0")).status.success());
        }
    }

    /// A script ending in a newline must still splice into the wrapper.
    ///
    /// The inline form is `then {script}; else …`: with an un-trimmed trailing
    /// newline the `;` lands alone on a line and dash rejects the whole
    /// command (`Syntax error: ";" unexpected`), which is how the file-backed
    /// `scripts/docker-start.sh` first reached a guest. The best-effort form
    /// exits 0 whatever the script does, so a parse failure is the only way
    /// this can fail.
    #[test]
    fn sudo_wrapper_tolerates_a_trailing_newline() {
        let script = run_as_root_or_sudo("exit 7\n");
        let output = std::process::Command::new("sh")
            .args(["-c", &script])
            .output()
            .expect("sh must run");
        assert!(
            output.status.success(),
            "a trailing newline broke the splice: {script}"
        );
    }

    #[test]
    fn runner_labels_bind_to_selected_ubuntu_environment() {
        assert_eq!(
            runner_environment_labels("ubuntu:22.04"),
            vec!["ubuntu-22.04"]
        );
        assert_eq!(
            runner_environment_labels("ubuntu:24.04"),
            vec!["ubuntu-24.04", "ubuntu-latest"]
        );
    }

    #[test]
    fn golden_download_url_uses_embedding_release_version() {
        let url = default_golden_url("9.8.7");
        assert!(url.contains("/releases/download/v9.8.7/"), "{url}");
        assert!(!url.contains(env!("CARGO_PKG_VERSION")), "{url}");
    }

    #[test]
    fn golden_download_progress_reports_bounded_percentage() {
        assert_eq!(golden_download_percent(0, Some(100)), Some(0));
        assert_eq!(golden_download_percent(25, Some(100)), Some(25));
        assert_eq!(golden_download_percent(150, Some(100)), Some(100));
        assert_eq!(golden_download_percent(1, None), None);
        assert_eq!(golden_download_percent(1, Some(0)), None);
    }

    #[test]
    fn golden_progress_bar_tracks_percentage_and_clamps() {
        assert_eq!(progress_bar(0), "[--------------------]");
        assert_eq!(progress_bar(40), "[########------------]");
        assert_eq!(progress_bar(100), "[####################]");
        // A percentage above 100 must not widen the bar past its cells.
        assert_eq!(progress_bar(250), "[####################]");
    }

    #[test]
    fn golden_progress_reports_megabytes_not_raw_bytes() {
        // The packed arm64 golden, the size that made byte counts unreadable.
        assert_eq!(megabytes(9_630_322_181), 9630);
        assert_eq!(megabytes(0), 0);
        // Sub-megabyte progress reads as 0 MB rather than a misleading 1.
        assert_eq!(megabytes(999_999), 0);
    }

    #[test]
    fn default_oci_golden_references_are_digest_pinned_per_arch() {
        for (pin, repository) in [
            (
                GOLDEN_OCI_REF_ARM64,
                "preloopdev/preloop-arm64-smolvm-golden",
            ),
            (
                GOLDEN_OCI_REF_X86_64,
                "preloopdev/preloop-x86_64-smolvm-golden",
            ),
        ] {
            let reference = OciReference::parse(pin).expect("valid OCI reference");
            assert_eq!(reference.registry, "ghcr.io");
            assert_eq!(reference.repository, repository);
            // Immutable digest pin: changing a default must be a reviewed code
            // change, not a registry retag.
            assert!(
                reference.reference.len() == "sha256:".len() + 64
                    && reference.reference.starts_with("sha256:"),
                "expected a digest-pinned default, got `{}`",
                reference.reference
            );
        }
    }

    #[test]
    fn concurrent_on_demand_provisioning_keeps_preparing_signal_raised() {
        let active = Arc::new(std::sync::Mutex::new(0));
        let signal = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let first = PreparingGuard::enter(active.clone(), Some(signal.clone()), None);
        let second = PreparingGuard::enter(active.clone(), Some(signal.clone()), None);
        assert!(signal.load(Ordering::Acquire));

        drop(first);
        assert!(
            signal.load(Ordering::Acquire),
            "one completed provision must not expose the other to starvation"
        );

        drop(second);
        assert!(!signal.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn warm_slot_raises_preparing_signal_while_provisioning() {
        // A warm slot clears the pool's preparing flag at the end of the
        // golden bake, then boots its first runner minutes later. The boot
        // must re-raise the shared signal, or the server's starvation sweep
        // fails a job queued during that window ("starving queued job failed
        // after 120s") even though a runner is on the way.
        let gate = Arc::new(tokio::sync::Notify::new());
        let provider = Arc::new(
            TestProvider::new(false, false, false, false, false).with_configure_gate(gate.clone()),
        );
        let mut config = test_config(false);
        config.size = 1;
        let signal = Arc::new(std::sync::atomic::AtomicBool::new(false));
        config.preparing_signal = Some(signal.clone());

        let handles = PoolHandles {
            idle: Arc::new(std::sync::Mutex::new(0)),
            keys: Arc::new(KeyPool::new()),
            building: Arc::new(AtomicUsize::new(0)),
            provisioning: Arc::new(std::sync::Mutex::new(0)),
        };
        let shutdown = CancellationToken::new();
        let slot = tokio::spawn(run_slot(
            provider.clone(),
            config.clone(),
            0,
            shutdown.clone(),
            Arc::new(GoldenRegistry::new(config.name_prefix.clone())),
            handles,
        ));

        // Wait until the slot's first provision reaches configure (i.e. the
        // runner is mid-boot, before registration).
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        loop {
            if provider
                .events()
                .await
                .iter()
                .any(|event| event.starts_with("configure:"))
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "slot never reached the configure step"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(
            signal.load(Ordering::Acquire),
            "warm-mode provisioning must raise the preparing signal"
        );

        // Release the gate, then stop the slot.
        gate.notify_waiters();
        shutdown.cancel();
        slot.abort();
        let _ = slot.await;
    }

    /// Materialize a valid externals tree for every expected runtime.
    fn write_valid_externals(externals_root: &std::path::Path) {
        for (runtime, version) in crate::node_externals::expected_runtimes() {
            let plain = version.trim_start_matches('v');
            let runtime_dir = externals_root.join(runtime);
            std::fs::create_dir_all(runtime_dir.join("bin")).unwrap();
            let manifest = crate::node_externals::NodeManifest::new(
                runtime,
                plain,
                "linux-arm64",
                "abc",
                "https://example.com",
            );
            crate::node_externals::write_manifest(&runtime_dir, &manifest).unwrap();
            let node = runtime_dir.join("bin/node");
            std::fs::write(&node, format!("#!/bin/sh\necho v{plain}\n")).unwrap();
            std::fs::set_permissions(&node, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    /// A release directory the engine cannot write must not strand the pool
    /// without node: the engine publishes its own bundle and mounts that.
    #[test]
    fn mirror_bundle_is_published_when_the_release_bundle_lacks_externals() {
        let home = tempfile::tempdir().unwrap();
        let release = tempfile::tempdir().unwrap();
        std::fs::write(release.path().join("runner"), b"#!/bin/sh\nexit 0\n").unwrap();
        let host_externals = home.path().join("externals");
        write_valid_externals(&host_externals);

        let mut config = test_config(false);
        config.runner_bundle = release.path().to_path_buf();
        config.externals_dir = home.path().to_path_buf();

        // The release bundle carries no externals and no mirror exists yet:
        // selecting a bundle must fail closed instead of mounting an
        // externals-less bundle into the golden (#294).
        let error = effective_runner_bundle(&config)
            .expect_err("an incomplete bundle must not be selected silently");
        assert!(
            format!("{error}").contains("complete node externals"),
            "unexpected error: {error}"
        );

        materialize_mirror_bundle(&config, &host_externals)
            .expect("the engine-owned mirror is publishable");

        let mirror = mirror_bundle_dir(&config);
        assert!(externals_complete(&mirror.join("externals")));
        assert!(mirror.join("runner").is_file());
        // Every guest now mounts the mirror instead of the incomplete release.
        assert_eq!(
            effective_runner_bundle(&config).expect("mirror is complete"),
            mirror
        );
    }

    /// Starting a pool whose bundle cannot resolve node reports ready and then
    /// fails every JS action step, so publishing must fail loudly instead.
    #[test]
    fn mirror_bundle_publication_fails_when_host_externals_are_incomplete() {
        let home = tempfile::tempdir().unwrap();
        let release = tempfile::tempdir().unwrap();
        std::fs::write(release.path().join("runner"), b"#!/bin/sh\nexit 0\n").unwrap();
        // Host cache is empty: nothing valid to mirror.
        let host_externals = home.path().join("externals");
        std::fs::create_dir_all(&host_externals).unwrap();

        let mut config = test_config(false);
        config.runner_bundle = release.path().to_path_buf();
        config.externals_dir = home.path().to_path_buf();

        let error = materialize_mirror_bundle(&config, &host_externals)
            .expect_err("an unusable bundle must refuse to start the pool");
        assert!(
            format!("{error}").contains("incomplete"),
            "unexpected error: {error}"
        );
        // Nothing complete remains: selecting a bundle fails closed instead of
        // mounting the incomplete release bundle (#294).
        effective_runner_bundle(&config).expect_err("no complete bundle may be selected silently");
    }

    /// #294: first start after a wiped store. The host externals were just
    /// (re)downloaded but the release bundle is still empty. The startup
    /// sequence — `ensure_host_externals` materializing the bundle, then
    /// `runner_volumes` selecting it — must hand the golden a bundle that
    /// carries node, never an externals-less mount.
    #[test]
    fn startup_materializes_bundle_before_golden_volumes_are_computed() {
        let home = tempfile::tempdir().unwrap();
        let release = tempfile::tempdir().unwrap();
        std::fs::write(release.path().join("runner"), b"#!/bin/sh\nexit 0\n").unwrap();
        // The wiped store was recreated: host externals valid, bundle empty.
        let host_externals = home.path().join("externals").join("externals");
        write_valid_externals(&host_externals);

        let mut config = test_config(false);
        config.runner_bundle = release.path().to_path_buf();
        config.externals_dir = home.path().join("externals");

        // Startup order: install/materialize first, then compute volumes.
        ensure_host_externals(&config).expect("bundle materialization must succeed");
        assert!(
            externals_complete(&release.path().join("externals")),
            "the release bundle must be materialized before any golden is created"
        );

        let machine = MachineName::new("preloop-runner-golden").unwrap();
        let volumes = runner_volumes(&config, &machine, false)
            .expect("volumes must be computable after a wipe");
        let bundle = volumes
            .iter()
            .find(|volume| volume.guest.as_path() == std::path::Path::new("/opt/preloop/bin"))
            .expect("the bundle mount is always present");
        assert!(
            externals_complete(&bundle.host.join("externals")),
            "the mounted bundle must carry node externals, got {}",
            bundle.host.display()
        );
    }

    /// #294: a guest volume spec must never resolve to a bundle that cannot
    /// serve node. With no complete bundle anywhere, `runner_volumes` fails
    /// instead of returning an externals-less mount.
    #[test]
    fn runner_volumes_refuses_externals_less_bundle() {
        let home = tempfile::tempdir().unwrap();
        let release = tempfile::tempdir().unwrap();
        std::fs::write(release.path().join("runner"), b"#!/bin/sh\nexit 0\n").unwrap();
        // Host externals are valid (so nothing needs downloading) but the
        // release bundle is empty and no mirror was published.
        let host_externals = home.path().join("externals").join("externals");
        write_valid_externals(&host_externals);

        let mut config = test_config(false);
        config.runner_bundle = release.path().to_path_buf();
        config.externals_dir = home.path().join("externals");

        let machine = MachineName::new("preloop-runner-golden").unwrap();
        let error = runner_volumes(&config, &machine, false)
            .expect_err("an externals-less machine must not be specced");
        assert!(
            format!("{error}").contains("no runner bundle with complete node externals"),
            "unexpected error: {error}"
        );
    }

    fn test_config(control_socket: bool) -> RunnerPoolConfig {
        // #294: machine creation now refuses externals-less bundles, so the
        // fixture seeds a complete bundle the way `ensure_host_externals`
        // guarantees in production. The directory is per-process and under
        // `temp_dir()` (TMPDIR-aware): parallel `cargo test` invocations must
        // not share or race on it.
        static DIRS: std::sync::OnceLock<(PathBuf, PathBuf)> = std::sync::OnceLock::new();
        let (runner_bundle, externals_dir) = DIRS
            .get_or_init(|| {
                let base = std::env::temp_dir()
                    .join(format!("preloop-lifecycle-test-{}", std::process::id()));
                let runner_bundle = base.join("bundle");
                write_valid_externals(&runner_bundle.join("externals"));
                (runner_bundle, base.join("externals"))
            })
            .clone();
        RunnerPoolConfig {
            size: 1,
            use_fork: false,
            use_packed_artifact: false,
            name_prefix: "lifecycle-test".to_owned(),
            base_image: "base-image".to_owned(),
            workspace: None,
            artifact_stem: PathBuf::from("/tmp/lifecycle-artifact"),
            release_version: "9.9.9".to_owned(),
            runner_bundle,
            externals_dir,
            runner_binary_name: "runner".to_owned(),
            server_url: "https://runner.test".to_owned(),
            control_origin: None,
            control_socket: control_socket.then(|| PathBuf::from("/tmp/engine.sock")),
            control_upstream: None,
            dns: None,
            registration_token_env: "LIFECYCLE_TEST_TOKEN".to_owned(),
            labels: vec!["test".to_owned()],
            cpus: 1,
            memory_mib: 128,
            storage_gib: 1,
            overlay_gib: None,
            // Tests that must not depend on the host's free space start with
            // the check disabled; the reserve tests script their own.
            job_vm_disk: JobVmDiskReserve::new(0, |_| Ok(0)),
            debug_dir: None,
            runner_key_dir: None,
            pending_jobs: None,
            preload_images: Vec::new(),
            runner_user: None,
            runner_uid: None,
            next_job_runs_on: None,
            pending_registrations: None,
            preparing_signal: None,
            pool_status: None,
            observability: None,
        }
    }

    #[test]
    fn zero_runner_storage_is_rejected() {
        let mut config = test_config(false);
        config.storage_gib = 0;

        let error = config.validate().expect_err("zero storage must be invalid");
        assert!(
            error
                .to_string()
                .contains("storage must be greater than zero"),
            "{error}"
        );
    }

    #[async_trait]
    impl VmProvider for TestProvider {
        fn capabilities(&self) -> ProviderCapabilities {
            ProviderCapabilities {
                preserves_runtime_state_on_suspend: self.suspends,
                ..ProviderCapabilities::default()
            }
        }

        async fn create(&self, spec: &MachineSpec) -> Result<(), VmError> {
            self.machines
                .lock()
                .await
                .insert(spec.name.as_str().to_owned(), MachineState::Stopped);
            self.created_images
                .lock()
                .await
                .push((spec.name.as_str().to_owned(), spec.image.clone()));
            self.events
                .lock()
                .await
                .push(format!("create:{}", spec.name.as_str()));
            Ok(())
        }

        async fn start(&self, name: &MachineName) -> Result<(), VmError> {
            self.events
                .lock()
                .await
                .push(format!("start:{}", name.as_str()));
            if self.fail_start {
                return Err(test_error("start-failure"));
            }
            self.machines
                .lock()
                .await
                .insert(name.as_str().to_owned(), MachineState::Running);
            Ok(())
        }

        async fn start_forkable(&self, name: &MachineName) -> Result<(), VmError> {
            self.start(name).await
        }

        async fn prune_pack_intermediates(&self, name: &MachineName) -> Result<bool, VmError> {
            self.prune_pack_calls
                .lock()
                .await
                .push(name.as_str().to_owned());
            Ok(true)
        }

        /// Records each reconcile sweep instead of removing anything, so a
        /// test can watch the pool's periodic orphan pass without a registry.
        async fn sweep_orphaned_data_dirs(&self) -> Result<usize, VmError> {
            self.events.lock().await.push("sweep".to_owned());
            Ok(0)
        }

        async fn fork(&self, golden: &MachineName, clone: &MachineName) -> Result<(), VmError> {
            self.events
                .lock()
                .await
                .push(format!("fork:{}:{}", golden.as_str(), clone.as_str()));
            if self.fork_base_busy {
                return Err(VmError::ForkBaseBusy {
                    golden: golden.as_str().to_owned(),
                    clone: "lifecycle-test-0-live".to_owned(),
                });
            }
            {
                let mut spent = self.fail_fork_once_spent.lock().await;
                if *spent {
                    *spent = false;
                    return Err(test_error(
                        "smolvm fork failed with exit code 1: golden 'lifecycle-test-golden' \
                         is already paused; a valid retained checkpoint is required",
                    ));
                }
            }
            {
                let mut transient = self.fail_fork_once.lock().await;
                if *transient {
                    *transient = false;
                    return Err(test_error(
                        "smolvm fork failed with exit code 1: clone agent readiness timed out",
                    ));
                }
            }
            if self.fail_fork {
                return Err(test_error("fork-failure"));
            }
            self.events
                .lock()
                .await
                .push(format!("create:{}", clone.as_str()));
            self.machines
                .lock()
                .await
                .insert(clone.as_str().to_owned(), MachineState::Running);
            Ok(())
        }

        async fn rearm_fork_base(
            &self,
            golden: &MachineName,
            partial: Option<&MachineName>,
        ) -> Result<bool, VmError> {
            self.events
                .lock()
                .await
                .push(format!("rearm:{}", golden.as_str()));
            if let Some(partial) = partial {
                self.delete(partial).await?;
            }
            let mut drain_after = self.drain_live_forks_after.lock().await;
            if *drain_after > 0 {
                *drain_after -= 1;
                if *drain_after == 0 {
                    *self.live_forks.lock().await = false;
                }
            }
            if *self.live_forks.lock().await {
                return Ok(false);
            }
            self.stop(golden).await?;
            self.start(golden).await?;
            Ok(true)
        }

        async fn stop(&self, name: &MachineName) -> Result<(), VmError> {
            self.events
                .lock()
                .await
                .push(format!("stop:{}", name.as_str()));
            self.machines
                .lock()
                .await
                .insert(name.as_str().to_owned(), MachineState::Stopped);
            Ok(())
        }

        async fn delete(&self, name: &MachineName) -> Result<(), VmError> {
            self.events
                .lock()
                .await
                .push(format!("delete:{}", name.as_str()));
            if self.fail_delete {
                return Err(test_error("delete-failure"));
            }
            self.machines.lock().await.remove(name.as_str());
            Ok(())
        }

        async fn status(&self, name: &MachineName) -> Result<MachineState, VmError> {
            Ok(self
                .machines
                .lock()
                .await
                .get(name.as_str())
                .copied()
                .unwrap_or(MachineState::Missing))
        }

        async fn list(&self) -> Result<Vec<MachineName>, VmError> {
            Ok(Vec::new())
        }

        async fn exec(&self, name: &MachineName, argv: &[String]) -> Result<ExecOutput, VmError> {
            self.events
                .lock()
                .await
                .push(format!("exec:{}:{:?}", name.as_str(), argv));
            if let Some(marker) = self.fail_exec_containing.lock().await.clone()
                && format!("{argv:?}").contains(&marker)
            {
                return Ok(ExecOutput {
                    exit_code: 1,
                    stderr: b"scripted exec failure".to_vec(),
                    ..test_output()
                });
            }
            if self.wedged_guest && argv == ["true"] {
                std::future::pending::<()>().await;
            }
            if argv.len() == 3
                && argv[0] == "test"
                && argv[1] == "-f"
                && argv[2].ends_with("preloop-job-paused")
            {
                // The real provider surfaces a guest exit 1 as
                // `VmError::Command` (smolvm propagates the guest exit code),
                // so the absent-marker probe must be modelled the same way —
                // the watcher's resume path depends on it.
                if self
                    .probe_transport_error
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    return Err(VmError::Launch {
                        program: "smolvm".to_owned(),
                        source: std::io::Error::new(std::io::ErrorKind::BrokenPipe, "vm wedged"),
                    });
                }
                let marker = self.pause_marker.load(std::sync::atomic::Ordering::SeqCst);
                if marker {
                    return Ok(test_output());
                }
                return Err(VmError::Command {
                    operation: "exec",
                    exit_code: 1,
                    message: "test -f: marker absent".to_owned(),
                });
            }
            if self.fail_install && argv.iter().any(|arg| arg.contains("apt-get")) {
                return Err(test_error("install-failure"));
            }
            if self.fail_run && argv.iter().any(|arg| arg == "run") {
                return Err(test_error("run-failure"));
            }
            Ok(test_output())
        }

        async fn exec_with_secret_env(
            &self,
            name: &MachineName,
            _argv: &[String],
            _secrets: &[(String, SecretSource)],
        ) -> Result<ExecOutput, VmError> {
            self.events
                .lock()
                .await
                .push(format!("configure:{}", name.as_str()));
            if let Some(gate) = &self.configure_gate {
                gate.notified().await;
            }
            if self.fail_configure {
                return Err(test_error("configure-failure"));
            }
            Ok(test_output())
        }

        async fn exec_stream(
            &self,
            name: &MachineName,
            argv: &[String],
            output: tokio::sync::mpsc::Sender<OutputChunk>,
        ) -> Result<i32, VmError> {
            self.events
                .lock()
                .await
                .push(format!("run:{}", name.as_str()));
            if self.wedged_guest {
                std::future::pending::<()>().await;
            }
            if self.announce_busy {
                output
                    .send(OutputChunk::Stdout(
                        format!("{RUNNER_BUSY_SENTINEL}\n").into_bytes(),
                    ))
                    .await
                    .unwrap();
            }
            if self.fail_run && argv.iter().any(|arg| arg == "run") {
                // `exec_stream` returns the guest process exit code; transport
                // failures are represented by `Err(VmError)` instead.
                return Ok(1);
            }
            Ok(0)
        }

        async fn copy(&self, _source: &str, _destination: &str) -> Result<(), VmError> {
            Ok(())
        }

        async fn pack(&self, _name: &MachineName, _output: &Path) -> Result<(), VmError> {
            Ok(())
        }
    }

    async fn provisioning_failure(
        provider: Arc<TestProvider>,
        config: &RunnerPoolConfig,
        golden: Option<&MachineName>,
        expected: &str,
    ) {
        let error = provision_slot(
            &provider,
            config,
            0,
            1,
            golden,
            &Arc::new(KeyPool::new()),
            RunnerEnvironment {
                fingerprint: None,
                base: config.base_image.clone(),
                toolchains: Vec::new(),
                curated: true,
            },
            &CancellationToken::new(),
        )
        .await
        .expect_err("provisioning failure must propagate");
        let name = MachineName::new(format!("{}-0-1", config.name_prefix)).unwrap();
        assert!(error.to_string().contains(expected));
        assert!(!provider.has_machine(&name).await);
        let events = provider.events().await;
        let create = events
            .iter()
            .position(|event| event == &format!("create:{}", name.as_str()))
            .expect("machine creation event");
        assert!(
            events[create + 1..]
                .iter()
                .any(|event| event == &format!("delete:{}", name.as_str()))
        );
    }

    #[tokio::test]
    async fn provisioning_failures_delete_created_runner() {
        let cases = [
            (
                TestProvider::new(true, false, false, false, false),
                false,
                "start-failure",
            ),
            (
                TestProvider::new(false, true, false, false, false),
                true,
                "install-failure",
            ),
            (
                TestProvider::new(false, false, true, false, false),
                false,
                "configure-failure",
            ),
        ];
        for (provider, control_socket, expected) in cases {
            provisioning_failure(
                Arc::new(provider),
                &test_config(control_socket),
                None,
                expected,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn fork_provisioning_failure_deletes_cloned_runner() {
        let provider = Arc::new(TestProvider::new(false, false, true, false, false));
        let config = test_config(false);
        let golden = MachineName::new("lifecycle-test-golden").unwrap();
        provisioning_failure(provider, &config, Some(&golden), "configure-failure").await;
    }

    #[tokio::test]
    async fn provision_failures_count_and_clear_on_pool_status() {
        let mut config = test_config(false);
        let pool_status = Arc::new(preloop_observability::status::PoolStatus::new(
            preloop_observability::status::PoolSnapshot::default(),
        ));
        config.pool_status = Some(pool_status.clone());
        let keys = Arc::new(KeyPool::new());

        // A failing provision must increment the consecutive-failure counter,
        // which feeds `pool_repeated_provision_failure` and the status field.
        let failing = Arc::new(TestProvider::new(true, false, false, false, false));
        let err = provision_slot(
            &failing,
            &config,
            0,
            1,
            None,
            &keys,
            RunnerEnvironment {
                fingerprint: None,
                base: config.base_image.clone(),
                toolchains: Vec::new(),
                curated: true,
            },
            &CancellationToken::new(),
        )
        .await
        .expect_err("start-failure must propagate");
        assert!(err.to_string().contains("start-failure"));
        assert_eq!(pool_status.snapshot().consecutive_provision_failures, 1);

        // A succeeding provision must reset the streak to zero.
        let ok = Arc::new(TestProvider::new(false, false, false, false, false));
        provision_slot(
            &ok,
            &config,
            0,
            2,
            None,
            &keys,
            RunnerEnvironment {
                fingerprint: None,
                base: config.base_image.clone(),
                toolchains: Vec::new(),
                curated: true,
            },
            &CancellationToken::new(),
        )
        .await
        .expect("provisioning succeeds");
        assert_eq!(pool_status.snapshot().consecutive_provision_failures, 0);
    }

    /// The environment knob maps to the reserve: unset keeps the 20 GiB
    /// default, `0` disables the check, and a typo keeps the default rather
    /// than silently removing the guard against filling the host.
    #[test]
    fn min_free_disk_bytes_parses_env_policy() {
        for raw in [None, Some(""), Some("   "), Some("nope"), Some("-1")] {
            assert_eq!(
                min_free_disk_bytes(raw),
                20 * GIB,
                "{raw:?} must keep the default reserve"
            );
        }
        assert_eq!(min_free_disk_bytes(Some("0")), 0, "0 disables the reserve");
        assert_eq!(min_free_disk_bytes(Some("7")), 7 * GIB);
        assert_eq!(min_free_disk_bytes(Some(" 7 ")), 7 * GIB);
    }

    /// The reserve gates every job-VM start: below it the slot waits and
    /// re-measures without creating or forking anything (and without
    /// recording a provision failure); at or above it, with the reserve
    /// disabled, and on a volume that cannot be measured, the VM starts.
    #[tokio::test]
    async fn job_vm_disk_reserve_gates_job_vm_starts() {
        #[derive(Clone, Copy)]
        struct Case {
            label: &'static str,
            free: Result<u64, ()>,
            reserve_bytes: u64,
            starts: bool,
        }
        let cases = [
            Case {
                label: "below the reserve",
                free: Ok(GIB),
                reserve_bytes: 20 * GIB,
                starts: false,
            },
            Case {
                label: "at the reserve",
                free: Ok(20 * GIB),
                reserve_bytes: 20 * GIB,
                starts: true,
            },
            Case {
                label: "above the reserve",
                free: Ok(64 * GIB),
                reserve_bytes: 20 * GIB,
                starts: true,
            },
            Case {
                label: "reserve disabled",
                free: Ok(0),
                reserve_bytes: 0,
                starts: true,
            },
            Case {
                label: "unmeasurable volume",
                free: Err(()),
                reserve_bytes: 20 * GIB,
                starts: true,
            },
        ];

        for case in cases {
            let mut config = test_config(false);
            let pool_status = Arc::new(preloop_observability::status::PoolStatus::new(
                preloop_observability::status::PoolSnapshot::default(),
            ));
            config.pool_status = Some(pool_status.clone());
            let probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counted = probes.clone();
            config.job_vm_disk = JobVmDiskReserve::new(case.reserve_bytes, move |_| {
                counted.fetch_add(1, Ordering::AcqRel);
                match case.free {
                    Ok(free) => Ok(free),
                    Err(()) => Err(test_error("df-failure")),
                }
            })
            .with_probe_interval(Duration::from_millis(5));

            let provider = Arc::new(TestProvider::new(false, false, false, false, false));
            let shutdown = CancellationToken::new();
            let name = MachineName::new(format!("{}-0-1", config.name_prefix)).unwrap();
            let keys = Arc::new(KeyPool::new());
            let provision = provision_slot(
                &provider,
                &config,
                0,
                1,
                None,
                &keys,
                test_runner_environment("base-image", Vec::new(), true),
                &shutdown,
            );
            if case.starts {
                let runner = tokio::time::timeout(Duration::from_secs(5), provision)
                    .await
                    .unwrap_or_else(|_| panic!("{}: the start must not wait", case.label))
                    .expect("provisioning succeeds");
                assert!(runner.is_some(), "{}: a runner was provisioned", case.label);
                assert!(
                    provider.has_machine(&name).await,
                    "{}: the job VM must start",
                    case.label
                );
                assert_eq!(
                    probes.load(Ordering::Acquire),
                    usize::from(case.reserve_bytes != 0),
                    "{}: a disabled reserve never measures, an enabled one decides in one",
                    case.label
                );
                continue;
            }

            let waited = tokio::time::timeout(Duration::from_millis(250), provision).await;
            assert!(
                waited.is_err(),
                "{}: the start must be held, not resolved",
                case.label
            );
            assert!(
                !provider.has_machine(&name).await,
                "{}: no machine may be created or forked",
                case.label
            );
            assert!(
                probes.load(Ordering::Acquire) > 1,
                "{}: the held slot must keep re-measuring",
                case.label
            );
            // A full host is not a broken provision: no slot failure, and no
            // delete/cleanup churn on machines that were never created.
            assert_eq!(
                pool_status.snapshot().consecutive_provision_failures,
                0,
                "{}: waiting for disk must not count as a provision failure",
                case.label
            );
            assert!(
                provider.events().await.is_empty(),
                "{}: the held slot must not touch the provider",
                case.label
            );
        }
    }

    /// A slot held by the reserve starts its VM as soon as space frees (no
    /// engine restart), and a shutdown during the wait abandons the start
    /// cleanly instead of failing the provision.
    #[tokio::test]
    async fn held_slot_starts_when_space_frees_and_aborts_on_shutdown() {
        let free = Arc::new(std::sync::atomic::AtomicU64::new(GIB));
        let probe_free = free.clone();
        let probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = probes.clone();
        let mut config = test_config(false);
        config.job_vm_disk = JobVmDiskReserve::new(20 * GIB, move |_| {
            counted.fetch_add(1, Ordering::AcqRel);
            Ok(probe_free.load(Ordering::Acquire))
        })
        .with_probe_interval(Duration::from_millis(5));
        let pool_status = Arc::new(preloop_observability::status::PoolStatus::new(
            preloop_observability::status::PoolSnapshot::default(),
        ));
        config.pool_status = Some(pool_status.clone());
        let provider = Arc::new(TestProvider::new(false, false, false, false, false));
        let name = MachineName::new(format!("{}-0-1", config.name_prefix)).unwrap();

        let provision = |generation: u64, shutdown: CancellationToken| {
            let provider = provider.clone();
            let config = config.clone();
            tokio::spawn(async move {
                provision_slot(
                    &provider,
                    &config,
                    0,
                    generation,
                    None,
                    &Arc::new(KeyPool::new()),
                    test_runner_environment("base-image", Vec::new(), true),
                    &shutdown,
                )
                .await
            })
        };

        // Held: the slot waits, nothing is created.
        let settled = provision(1, CancellationToken::new());
        while probes.load(Ordering::Acquire) < 3 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            !provider.has_machine(&name).await,
            "a slot below the reserve must not start a VM"
        );

        // Space frees: the same slot resumes without an engine restart.
        free.store(64 * GIB, Ordering::Release);
        let runner = tokio::time::timeout(Duration::from_secs(5), settled)
            .await
            .expect("a slot must resume once space frees")
            .expect("task joins")
            .expect("provisioning succeeds");
        assert!(runner.is_some(), "the resumed slot provisions its runner");
        assert!(provider.has_machine(&name).await);
        assert_eq!(pool_status.snapshot().consecutive_provision_failures, 0);

        // Shutdown while held: abandoned, not failed, and no VM is left.
        free.store(GIB, Ordering::Release);
        let shutdown = CancellationToken::new();
        let held = provision(2, shutdown.clone());
        while probes.load(Ordering::Acquire) < 6 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        shutdown.cancel();
        let abandoned = tokio::time::timeout(Duration::from_secs(5), held)
            .await
            .expect("shutdown must end the wait")
            .expect("task joins")
            .expect("an aborted start is not an error");
        assert!(
            abandoned.is_none(),
            "shutdown abandons the start instead of provisioning"
        );
        assert_eq!(
            pool_status.snapshot().consecutive_provision_failures,
            0,
            "abandoning a held start must not record a provision failure"
        );
    }

    /// The periodic reconcile sweeps while the pool serves, leaves every
    /// registered machine alone, and stops when the pool's shutdown fires.
    #[tokio::test]
    async fn periodic_reconcile_sweeps_registered_machines_are_spared() {
        let home = std::env::temp_dir().join(format!("preloop-reconcile-{}", std::process::id()));
        let previous_home = std::env::var_os("PRELOOP_HOME");
        // SAFETY: no other test in this crate reads PRELOOP_HOME; it is set
        // only so the reconcile tick's hypervisor purge scans a scratch home
        // instead of the developer's.
        unsafe { std::env::set_var("PRELOOP_HOME", &home) };

        let provider = Arc::new(TestProvider::new(false, false, false, false, false));
        let registered = MachineName::new("lifecycle-test-0-1").unwrap();
        provider
            .machines
            .lock()
            .await
            .insert(registered.as_str().to_owned(), MachineState::Running);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(reconcile_orphans(
            provider.clone(),
            shutdown.clone(),
            Duration::from_millis(20),
        ));

        let sweeps = |events: &[String]| events.iter().filter(|event| *event == "sweep").count();
        let mut observed = 0;
        for _ in 0..200 {
            observed = sweeps(&provider.events().await);
            if observed >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            observed >= 2,
            "the pool must reconcile repeatedly while it serves, saw {observed} sweeps"
        );
        assert!(
            provider.has_machine(&registered).await,
            "a registered machine must survive the reconcile"
        );
        let events = provider.events().await;
        assert!(
            !events.iter().any(|event| {
                event
                    .strip_prefix("delete:")
                    .or_else(|| event.strip_prefix("stop:"))
                    .is_some_and(|name| name == registered.as_str())
            }),
            "the reconcile must never delete or stop a registered machine: {events:?}"
        );

        shutdown.cancel();
        task.await.expect("reconcile task joins");
        let after_shutdown = sweeps(&provider.events().await);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            sweeps(&provider.events().await),
            after_shutdown,
            "the reconcile must stop with the pool"
        );

        match previous_home {
            // SAFETY: see above.
            Some(value) => unsafe { std::env::set_var("PRELOOP_HOME", value) },
            // SAFETY: see above.
            None => unsafe { std::env::remove_var("PRELOOP_HOME") },
        }
    }

    fn packed_fork_config() -> RunnerPoolConfig {
        let mut config = test_config(false);
        config.use_fork = true;
        config.use_packed_artifact = true;
        config
    }

    fn test_runner_environment(
        base: impl Into<String>,
        toolchains: Vec<ToolchainLayer>,
        curated: bool,
    ) -> RunnerEnvironment {
        RunnerEnvironment {
            fingerprint: None,
            base: base.into(),
            toolchains,
            curated,
        }
    }

    /// A retained SmolVM checkpoint can become unusable after the packed
    /// golden has been prepared. Retrying the same fork forever starves every
    /// queued job, while creating a runner directly from the same packed
    /// artifact remains valid.
    #[tokio::test]
    async fn packed_golden_fork_failure_falls_back_to_direct_creation() {
        // Verbatim from SmolVM 1.7.x: a spent fork base is reported through the
        // CLI's stderr, so the signature match is the only handle on it.
        const SPENT_BASE: &str = "smolvm fork failed with exit code 1: Freezing golden \
             'preloop-runner-golden' as fork base...\nError: agent operation failed: fork: \
             golden 'preloop-runner-golden' is already paused; a valid retained checkpoint \
             is required";

        assert!(fork_base_unusable(&VmError::Command {
            operation: "fork",
            exit_code: 1,
            message: SPENT_BASE.to_owned(),
        }));
        assert!(!fork_base_unusable(&VmError::Command {
            operation: "fork",
            exit_code: 1,
            message: "host port 8080 is assigned to more than one clone".to_owned(),
        }));

        let provider =
            Arc::new(TestProvider::new(false, false, false, false, false).failing_fork());
        let config = packed_fork_config();
        let golden = MachineName::new("lifecycle-test-golden").unwrap();
        let name = MachineName::new("lifecycle-test-0-4").unwrap();

        provision_runner(
            &provider,
            &config,
            &name,
            Some(&golden),
            &Arc::new(KeyPool::new()),
            &test_runner_environment(config.base_image.clone(), Vec::new(), true),
        )
        .await
        .expect("a broken packed-golden fork falls back to direct creation");

        let events = provider.events().await;
        let fork = format!("fork:{}:{}", golden.as_str(), name.as_str());
        let delete = format!("delete:{}", name.as_str());
        let create = format!("create:{}", name.as_str());
        let start = format!("start:{}", name.as_str());
        let fork_index = events
            .iter()
            .position(|event| event == &fork)
            .expect("fork was attempted first");
        let delete_index = events
            .iter()
            .position(|event| event == &delete)
            .expect("a partial clone was cleaned up");
        let create_index = events
            .iter()
            .position(|event| event == &create)
            .expect("runner was created from the packed artifact");
        let start_index = events
            .iter()
            .position(|event| event == &start)
            .expect("directly created runner was started");
        assert!(
            fork_index < delete_index && delete_index < create_index && create_index < start_index,
            "fallback order must be fork, cleanup, create, start: {events:?}"
        );
        assert!(provider.has_machine(&name).await);
    }

    #[tokio::test]
    async fn transient_packed_golden_fork_failure_retries_the_fork_before_direct_creation() {
        let provider =
            Arc::new(TestProvider::new(false, false, false, false, false).failing_fork_once());
        let config = packed_fork_config();
        let golden = MachineName::new("lifecycle-test-golden").unwrap();
        let name = MachineName::new("lifecycle-test-0-5").unwrap();

        provision_runner(
            &provider,
            &config,
            &name,
            Some(&golden),
            &Arc::new(KeyPool::new()),
            &test_runner_environment(config.base_image.clone(), Vec::new(), true),
        )
        .await
        .expect("a transient fork failure recovers by forking again");

        let events = provider.events().await;
        let fork = format!("fork:{}:{}", golden.as_str(), name.as_str());
        assert_eq!(
            events.iter().filter(|event| *event == &fork).count(),
            2,
            "the failed fork is retried once: {events:?}"
        );
        assert!(
            !events.contains(&format!("start:{}", name.as_str())),
            "a recovered fork must not cold-boot the packed artifact: {events:?}"
        );
    }

    #[tokio::test]
    async fn a_wedged_runner_guest_releases_its_slot() {
        let provider =
            Arc::new(TestProvider::new(false, false, false, false, false).with_wedged_guest());
        let name = MachineName::new("lifecycle-test-0-6").unwrap();
        let (busy, _busy_rx) = tokio::sync::oneshot::channel();
        let liveness = GuestLiveness {
            interval: std::time::Duration::from_millis(5),
            timeout: std::time::Duration::from_millis(5),
            failures: 3,
        };

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            run_until_exit(&provider, &name, &["run".to_owned()], busy, liveness),
        )
        .await
        .expect("a wedged guest must not pin the runner stream");

        assert!(
            matches!(result, Err(VmError::GuestUnresponsive { probes: 3, .. })),
            "{result:?}"
        );
    }

    /// Issue #295: once the packed golden is forkable, the pool must ask the
    /// provider to drop the pack/ build intermediates that sit beside the
    /// finished disk. A failed prune is best-effort and must not fail the
    /// golden — but the hook itself must fire exactly once per preparation.
    #[tokio::test]
    async fn packed_golden_prepare_prunes_pack_intermediates() {
        let provider = Arc::new(TestProvider::new(false, false, false, false, false));
        // Keep the golden fingerprint record off /tmp: the test only needs a
        // writable directory, not the hardcoded lifecycle paths.
        let scratch = tempfile::tempdir().expect("scratch dir");
        let mut config = packed_fork_config();
        config.artifact_stem = scratch.path().join("artifact");
        let golden = MachineName::new("lifecycle-test-golden").unwrap();

        prepare_packed_golden(&provider, &config, &golden)
            .await
            .expect("packed golden preparation succeeds");

        assert_eq!(
            *provider.prune_pack_calls.lock().await,
            vec![golden.as_str().to_owned()],
            "prune hook fires exactly once for the golden"
        );
        let events = provider.events().await;
        let forkable = format!("start:{}", golden.as_str());
        assert!(
            events.iter().any(|event| event == &forkable),
            "golden was started forkable before the prune: {events:?}"
        );
    }

    /// The prune hook must not fire when the golden never becomes forkable:
    /// `prepare_packed_golden` returns before the hook on any earlier failure.
    #[tokio::test]
    async fn packed_golden_prepare_skips_prune_on_start_failure() {
        let provider = Arc::new(TestProvider::new(true, false, false, false, false));
        let scratch = tempfile::tempdir().expect("scratch dir");
        let mut config = packed_fork_config();
        config.artifact_stem = scratch.path().join("artifact");
        let golden = MachineName::new("lifecycle-test-golden").unwrap();

        prepare_packed_golden(&provider, &config, &golden)
            .await
            .expect_err("start failure aborts golden preparation");

        assert!(
            provider.prune_pack_calls.lock().await.is_empty(),
            "prune hook must not fire when the golden never started"
        );
    }

    /// A spent fork base with no surviving clones is re-armed (stop, start
    /// forkable) and the fork retried — the queue recovers in seconds instead
    /// of stalling until someone restarts the engine and rebuilds the golden.
    #[tokio::test]
    async fn spent_fork_base_with_no_live_clones_is_rearmed_and_retried() {
        let provider = Arc::new(
            TestProvider::new(false, false, false, false, false)
                .with_live_forks(false)
                .failing_fork_once_spent(),
        );
        let config = packed_fork_config();
        let golden = MachineName::new("lifecycle-test-golden").unwrap();
        let name = MachineName::new("lifecycle-test-0-6").unwrap();

        provision_runner(
            &provider,
            &config,
            &name,
            Some(&golden),
            &Arc::new(KeyPool::new()),
            &test_runner_environment(config.base_image.clone(), Vec::new(), true),
        )
        .await
        .expect("the re-armed golden serves the fork");

        let events = provider.events().await;
        let expected = [
            format!("fork:{}:{}", golden.as_str(), name.as_str()),
            format!("rearm:{}", golden.as_str()),
            format!("delete:{}", name.as_str()),
            format!("stop:{}", golden.as_str()),
            format!("start:{}", golden.as_str()),
            format!("fork:{}:{}", golden.as_str(), name.as_str()),
        ];
        let mut cursor = 0;
        for event in &expected {
            let position = events[cursor..]
                .iter()
                .position(|seen| seen == event)
                .expect("re-arm sequence must include every step");
            cursor += position + 1;
        }
        assert!(
            provider.has_machine(&name).await,
            "the retried fork must leave the clone provisioned"
        );
    }

    /// A fingerprint-suffixed golden (`{prefix}-golden-{fingerprint}`, the
    /// name `run_slot` uses for a non-default base image) must be recognized
    /// as managed by this pool. The stale guard matched only the plain
    /// `{prefix}-golden` form, so a spent per-environment golden looped on
    /// "already paused" forever instead of re-arming.
    #[tokio::test]
    async fn spent_fingerprint_suffixed_fork_base_is_rearmed_and_retried() {
        let provider = Arc::new(
            TestProvider::new(false, false, false, false, false)
                .with_live_forks(false)
                .failing_fork_once_spent(),
        );
        let config = packed_fork_config();
        let golden = MachineName::new("lifecycle-test-golden-3577f5d5a384").unwrap();
        let name = MachineName::new("lifecycle-test-0-8").unwrap();

        provision_runner(
            &provider,
            &config,
            &name,
            Some(&golden),
            &Arc::new(KeyPool::new()),
            &test_runner_environment(config.base_image.clone(), Vec::new(), true),
        )
        .await
        .expect("the re-armed fingerprint-suffixed golden serves the fork");

        let events = provider.events().await;
        let expected = [
            format!("fork:{}:{}", golden.as_str(), name.as_str()),
            format!("rearm:{}", golden.as_str()),
            format!("delete:{}", name.as_str()),
            format!("stop:{}", golden.as_str()),
            format!("start:{}", golden.as_str()),
            format!("fork:{}:{}", golden.as_str(), name.as_str()),
        ];
        let mut cursor = 0;
        for event in &expected {
            let position = events[cursor..]
                .iter()
                .position(|seen| seen == event)
                .expect("re-arm sequence must include every step");
            cursor += position + 1;
        }
        assert!(
            provider.has_machine(&name).await,
            "the retried fork must leave the clone provisioned"
        );
    }

    /// A fingerprint-suffixed golden is an *environment* golden baked from
    /// the job's requested image. When its fork fails and the pool falls
    /// back to independent creation, the runner must boot that job's
    /// environment — not the default packed artifact, which would run a
    /// non-default `runs-on` job on the wrong operating system.
    #[tokio::test]
    async fn fingerprint_golden_fork_failure_falls_back_to_the_job_environment() {
        let provider =
            Arc::new(TestProvider::new(false, false, false, false, false).failing_fork());
        let config = packed_fork_config();
        let golden = MachineName::new("lifecycle-test-golden-3577f5d5a384").unwrap();
        let name = MachineName::new("lifecycle-test-0-9").unwrap();
        let env = test_runner_environment("mirror.gcr.io/library/ubuntu:22.04", Vec::new(), true);

        provision_runner(
            &provider,
            &config,
            &name,
            Some(&golden),
            &Arc::new(KeyPool::new()),
            &env,
        )
        .await
        .expect("an env-golden fork failure falls back to the job environment");

        let created = provider
            .created_image(&name)
            .await
            .expect("the fallback created the runner machine");
        assert_eq!(
            created, "mirror.gcr.io/library/ubuntu:22.04",
            "an env-golden fallback must boot the job's requested image, not the default pack"
        );
    }

    /// A spent base that still has live clones must NOT be re-armed: resuming
    /// it would corrupt the copy-on-write clones. The pool falls back to a
    /// full create instead.
    // Paused time: the drain loop sleeps GOLDEN_DRAIN_PROBE_DELAY between
    // probes; without this the 12-probe worst case would stall the test for
    // two minutes of real time.
    #[tokio::test(start_paused = true)]
    async fn spent_fork_base_with_live_clones_is_not_rearmed() {
        let provider = Arc::new(
            TestProvider::new(false, false, false, false, false)
                .with_live_forks(true)
                .failing_fork_once_spent(),
        );
        let config = packed_fork_config();
        let golden = MachineName::new("lifecycle-test-golden").unwrap();
        let name = MachineName::new("lifecycle-test-0-7").unwrap();

        provision_runner(
            &provider,
            &config,
            &name,
            Some(&golden),
            &Arc::new(KeyPool::new()),
            &test_runner_environment(config.base_image.clone(), Vec::new(), true),
        )
        .await
        .expect("falls back to direct creation");

        let events = provider.events().await;
        assert!(
            !events.iter().any(|event| event.starts_with("stop:")),
            "the golden must not be touched while clones exist: {events:?}"
        );
        assert!(
            events.contains(&format!("delete:{}", name.as_str())),
            "the partial clone is cleaned up: {events:?}"
        );
        assert_eq!(
            provider.created_image(&name).await.as_deref(),
            Some(config.base_image.as_str()),
            "a live clone makes the shared packed payload unsafe; fallback must use OCI"
        );
    }

    /// A live clone that exits mid-drain must be re-armed once it is gone:
    /// the drain loop keeps probing with backoff, and the golden resumes
    /// serving forks instead of falling back to slow direct creation.
    /// Paused time advances the probe sleeps instantly.
    #[tokio::test(start_paused = true)]
    async fn spent_fork_base_with_clone_that_drains_is_rearmed_and_retried() {
        let provider = Arc::new(
            TestProvider::new(false, false, false, false, false)
                .with_live_forks(true)
                .drain_live_forks_after(3)
                .failing_fork_once_spent(),
        );
        let config = packed_fork_config();
        let golden = MachineName::new("lifecycle-test-golden").unwrap();
        let name = MachineName::new("lifecycle-test-0-10").unwrap();

        provision_runner(
            &provider,
            &config,
            &name,
            Some(&golden),
            &Arc::new(KeyPool::new()),
            &test_runner_environment(config.base_image.clone(), Vec::new(), true),
        )
        .await
        .expect("the re-armed golden serves the fork after the clone drains");

        let events = provider.events().await;
        let expected = [
            format!("fork:{}:{}", golden.as_str(), name.as_str()),
            format!("rearm:{}", golden.as_str()),
            format!("rearm:{}", golden.as_str()),
            format!("rearm:{}", golden.as_str()),
            format!("stop:{}", golden.as_str()),
            format!("start:{}", golden.as_str()),
            format!("fork:{}:{}", golden.as_str(), name.as_str()),
        ];
        let mut cursor = 0;
        for event in &expected {
            let position = events[cursor..]
                .iter()
                .position(|seen| seen == event)
                .expect("drain-and-re-arm sequence must include every step");
            cursor += position + 1;
        }
        assert!(
            provider.has_machine(&name).await,
            "the retried fork must leave the clone provisioned"
        );
    }

    /// The provider reports a live clone before invoking SmolVM for another
    /// plain fork. The orchestrator must neither re-arm the shared golden nor
    /// instantiate the packed payload beside that clone.
    #[tokio::test]
    async fn busy_packed_fork_base_uses_independent_environment_image() {
        let provider =
            Arc::new(TestProvider::new(false, false, false, false, false).with_busy_fork_base());
        let config = packed_fork_config();
        let golden = MachineName::new("lifecycle-test-golden").unwrap();
        let name = MachineName::new("lifecycle-test-0-9").unwrap();
        let environment_base = "mirror.gcr.io/library/ubuntu:22.04";

        provision_runner(
            &provider,
            &config,
            &name,
            Some(&golden),
            &Arc::new(KeyPool::new()),
            &test_runner_environment(environment_base, Vec::new(), true),
        )
        .await
        .expect("a busy plain-fork base falls back to an independent OCI machine");

        let events = provider.events().await;
        assert!(
            !events.iter().any(|event| event.starts_with("rearm:")),
            "a golden with a live clone must not be re-armed: {events:?}"
        );
        assert_eq!(
            provider.created_image(&name).await.as_deref(),
            Some(environment_base),
            "fallback must use the job's resolved environment, not the shared packed payload"
        );
    }

    /// A partial clone that cannot be removed must block the re-arm: the
    /// untracked clone would otherwise share the resumed base's disks.
    #[tokio::test]
    async fn spent_fork_base_with_failed_partial_cleanup_is_not_rearmed() {
        let provider = Arc::new(
            TestProvider::new(false, false, false, false, true)
                .with_live_forks(false)
                .failing_fork_once_spent(),
        );
        let config = packed_fork_config();
        let golden = MachineName::new("lifecycle-test-golden").unwrap();
        let name = MachineName::new("lifecycle-test-0-8").unwrap();

        provision_runner(
            &provider,
            &config,
            &name,
            Some(&golden),
            &Arc::new(KeyPool::new()),
            &test_runner_environment(config.base_image.clone(), Vec::new(), true),
        )
        .await
        .expect("falls back to direct creation");

        let events = provider.events().await;
        assert!(
            !events.iter().any(|event| event.starts_with("stop:")),
            "the golden must not be touched when cleanup failed: {events:?}"
        );
    }

    /// An environment-specific golden may represent a different `runs-on`
    /// image from the packed artifact. Falling back there would run the job
    /// on the wrong operating system, so only the default packed golden may
    /// take the direct-create recovery path.
    #[tokio::test]
    async fn environment_golden_fork_failure_does_not_change_the_job_image() {
        let provider =
            Arc::new(TestProvider::new(false, false, false, false, false).failing_fork());
        let config = packed_fork_config();
        let golden = MachineName::new("lifecycle-test-golden-environment").unwrap();
        let name = MachineName::new("lifecycle-test-0-5").unwrap();

        let error = provision_runner(
            &provider,
            &config,
            &name,
            Some(&golden),
            &Arc::new(KeyPool::new()),
            &test_runner_environment("mirror.gcr.io/library/ubuntu:22.04", Vec::new(), true),
        )
        .await
        .expect_err("an environment-golden fork failure must propagate");

        assert!(error.to_string().contains("fork-failure"));
        let events = provider.events().await;
        assert!(
            !events
                .iter()
                .any(|event| event == &format!("create:{}", name.as_str())),
            "must not replace an environment-specific image with the default pack: {events:?}"
        );
    }

    /// A pack published before the baseline stopped wiping `/var/lib/apt/lists`
    /// boots without apt indices, and `sudo apt-get install <pkg>` — how real
    /// workflows install system packages — then resolves nothing.
    #[tokio::test]
    async fn packed_golden_fork_restores_apt_indices() {
        let provider = Arc::new(TestProvider::new(false, false, false, false, false));
        let config = packed_fork_config();
        let golden = MachineName::new("lifecycle-test-golden").unwrap();
        let name = MachineName::new("lifecycle-test-0-3").unwrap();

        provision_runner(
            &provider,
            &config,
            &name,
            Some(&golden),
            &Arc::new(KeyPool::new()),
            &test_runner_environment(config.base_image.clone(), Vec::new(), true),
        )
        .await
        .expect("provisioning succeeds");

        let events = provider.events().await;
        assert!(
            events.iter().any(|event| event.contains("_Packages")
                && event.contains("apt-get")
                && event.contains("update")
                && event.contains("timeout 120")),
            "the fork must restore apt indices when the pack has none: {events:?}"
        );
    }

    /// The golden registry dies with the process, so a restart would rebake a
    /// golden that is still sitting there fully baked. The host-side record is
    /// what makes adoption safe: it must match the requested fingerprint, and
    /// the machine must exist. A stopped golden — the state a graceful
    /// shutdown leaves, since shutdown stops rather than deletes goldens — is
    /// re-armed; a running one is used as-is (#293).
    #[tokio::test]
    async fn golden_adopt_state_covers_restart_lifecycle() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = test_config(false);
        config.artifact_stem = temp.path().join("runner-image");
        let provider = Arc::new(TestProvider::new(false, false, false, false, false));
        let golden = MachineName::new("lifecycle-test-golden-abc123").unwrap();

        // Recorded, but the machine was never started: rebuild.
        write_golden_record(&config, &golden, "fp-1");
        assert_eq!(
            golden_adopt_state(&provider, &config, &golden, "fp-1").await,
            GoldenAdopt::Rebuild
        );

        provider
            .create(&MachineSpec {
                name: golden.clone(),
                image: config.base_image.clone(),
                cpus: config.cpus,
                memory_mib: config.memory_mib,
                storage_gib: config.storage_gib,
                overlay_gib: None,
                network: NetworkPolicy::PublicOnly,
                volumes: Vec::new(),
                sockets: Vec::new(),
                dns: None,
                rosetta: false,
            })
            .await
            .unwrap();
        provider.start(&golden).await.unwrap();

        assert_eq!(
            golden_adopt_state(&provider, &config, &golden, "fp-1").await,
            GoldenAdopt::Reusable
        );
        // A base-image or toolchain bump changes the fingerprint: rebuild.
        assert_eq!(
            golden_adopt_state(&provider, &config, &golden, "fp-2").await,
            GoldenAdopt::Rebuild
        );

        // Graceful shutdown stops the golden instead of deleting it; the next
        // start re-arms it as a fork base rather than rebaking.
        provider.stop(&golden).await.unwrap();
        assert_eq!(
            golden_adopt_state(&provider, &config, &golden, "fp-1").await,
            GoldenAdopt::Rearm
        );
        assert!(
            adopt_golden(&provider, &config, &golden, "fp-1", "golden")
                .await
                .unwrap()
        );
        assert_eq!(
            provider.status(&golden).await.unwrap(),
            MachineState::Running
        );
        assert!(
            provider
                .events()
                .await
                .iter()
                .any(|event| event == &format!("start:{}", golden.as_str())),
            "re-arm must restart the stopped golden as forkable"
        );

        remove_golden_record(&config, &golden);
        assert_eq!(
            golden_adopt_state(&provider, &config, &golden, "fp-1").await,
            GoldenAdopt::Rebuild
        );
    }

    /// An adopted golden never went through `prepare_packed_golden`, so the
    /// #295 pack/ prune must fire on the adopt path too — otherwise a golden
    /// carried across restarts keeps its intermediates forever and every
    /// fork copies them.
    #[tokio::test]
    async fn adopted_golden_prunes_pack_intermediates() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = packed_fork_config();
        config.artifact_stem = temp.path().join("runner-image");
        let provider = Arc::new(TestProvider::new(false, false, false, false, false));
        let golden = MachineName::new("lifecycle-test-golden").unwrap();
        write_golden_record(&config, &golden, "fp-1");
        provider
            .create(&MachineSpec {
                name: golden.clone(),
                image: config.base_image.clone(),
                cpus: config.cpus,
                memory_mib: config.memory_mib,
                storage_gib: config.storage_gib,
                overlay_gib: None,
                network: NetworkPolicy::PublicOnly,
                volumes: Vec::new(),
                sockets: Vec::new(),
                dns: None,
                rosetta: false,
            })
            .await
            .unwrap();
        provider.start(&golden).await.unwrap();

        assert!(
            adopt_golden(&provider, &config, &golden, "fp-1", "golden")
                .await
                .unwrap()
        );
        assert_eq!(
            *provider.prune_pack_calls.lock().await,
            vec![golden.as_str().to_owned()],
            "adopt path must prune pack intermediates"
        );
    }

    /// `.tmp-golden-*` staging files and payloads from older environment
    /// fingerprints are unreachable once written; the startup sweep reclaims
    /// them while sparing the current payload and the goldens/ records.
    #[tokio::test]
    async fn sweep_stale_artifacts_removes_orphans_only() {
        let temp = tempfile::tempdir().unwrap();
        let vms = temp.path().join("vms");
        std::fs::create_dir_all(&vms).unwrap();
        let mut config = test_config(false);
        config.artifact_stem = vms.join("preloop-image-aarch64");
        // SAFETY: test-only env for config validation; the pool is never run.
        unsafe { std::env::set_var("LIFECYCLE_TEST_TOKEN", "test") };
        let pool = RunnerPool::new(
            Arc::new(TestProvider::new(false, false, false, false, false)),
            config.clone(),
        )
        .expect("pool config validates");

        let current = config.artifact_payload();
        std::fs::write(&current, b"current").unwrap();
        let stale_payload = vms.join(format!("preloop-image-aarch64-{:064x}", 0xdeadbeef_u64));
        std::fs::write(&stale_payload, b"stale").unwrap();
        let tmp = vms.join(".tmp-golden-1234");
        std::fs::write(&tmp, b"partial").unwrap();
        // Same stem prefix but not a fingerprint suffix: must survive.
        let unrelated = vms.join("preloop-image-aarch64-notes.txt");
        std::fs::write(&unrelated, b"keep").unwrap();
        let goldens = vms.join("goldens");
        std::fs::create_dir_all(&goldens).unwrap();

        pool.sweep_stale_artifacts().await;

        assert!(current.is_file(), "current payload survives");
        assert!(!stale_payload.exists(), "stale payload is swept");
        assert!(!tmp.exists(), "tmp-golden staging file is swept");
        assert!(unrelated.is_file(), "unrelated file survives");
        assert!(goldens.is_dir(), "goldens/ record dir survives");
    }

    /// A `.tmp-golden-*` staging file with a fresh companion lock must survive
    /// the startup sweep, so concurrent pools sharing the artifact directory
    /// cannot delete each other's in-flight bakes or downloads. A stale lock
    /// (crashed writer) and a lockless staging file are swept.
    #[tokio::test]
    async fn sweep_stale_artifacts_spares_active_writer_staging_file() {
        let temp = tempfile::tempdir().unwrap();
        let vms = temp.path().join("vms");
        std::fs::create_dir_all(&vms).unwrap();
        let mut config = test_config(false);
        config.artifact_stem = vms.join("preloop-image-aarch64");
        // SAFETY: test-only env for config validation; the pool is never run.
        unsafe { std::env::set_var("LIFECYCLE_TEST_TOKEN", "test") };
        let pool = RunnerPool::new(
            Arc::new(TestProvider::new(false, false, false, false, false)),
            config.clone(),
        )
        .expect("pool config validates");

        // Live writer: fresh companion lock.
        let active_tmp = vms.join(".tmp-golden-active");
        std::fs::write(&active_tmp, b"download in flight").unwrap();
        std::fs::write(staging_lock_path(&active_tmp), b"").unwrap();

        // Crashed writer: lock untouched for over an hour.
        let crashed_tmp = vms.join(".tmp-golden-crashed");
        std::fs::write(&crashed_tmp, b"orphan").unwrap();
        let crashed_lock = staging_lock_path(&crashed_tmp);
        std::fs::write(&crashed_lock, b"").unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&crashed_lock)
            .unwrap()
            .set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(3600))
            .unwrap();

        // No lock at all: orphaned staging file.
        let stale_tmp = vms.join(".tmp-golden-stale");
        std::fs::write(&stale_tmp, b"stale").unwrap();

        pool.sweep_stale_artifacts().await;

        assert!(
            active_tmp.is_file(),
            "staging file with fresh lock survives"
        );
        assert!(
            staging_lock_path(&active_tmp).is_file(),
            "fresh companion lock survives"
        );
        assert!(
            !crashed_tmp.exists(),
            "staging file with stale lock is swept"
        );
        assert!(!crashed_lock.exists(), "stale companion lock is swept");
        assert!(!stale_tmp.exists(), "lockless staging file is swept");
    }

    #[tokio::test]
    async fn runner_error_wins_when_delete_also_fails() {
        let provider = Arc::new(TestProvider::new(false, false, false, true, true));
        let config = test_config(false);
        let runner = provision_slot(
            &provider,
            &config,
            0,
            1,
            None,
            &Arc::new(KeyPool::new()),
            RunnerEnvironment {
                fingerprint: None,
                base: config.base_image.clone(),
                toolchains: Vec::new(),
                curated: true,
            },
            &CancellationToken::new(),
        )
        .await
        .expect("provisioning succeeds")
        .expect("a runner is provisioned");
        let idle = std::sync::Mutex::new(0);
        let error = run_one_runner(
            provider,
            &config,
            runner,
            CancellationToken::new(),
            SlotPlan {
                slot: 0,
                generation: 2,
                golden: None,
                environment: RunnerEnvironment {
                    fingerprint: None,
                    base: config.base_image.clone(),
                    toolchains: Vec::new(),
                    curated: true,
                },
                idle: &idle,
                keys: &Arc::new(KeyPool::new()),
                building: &AtomicUsize::new(0),
                provisioning: &Arc::new(std::sync::Mutex::new(0)),
                prebuild_successor: true,
            },
        )
        .await
        .expect_err("runner failure must propagate");
        assert!(
            error
                .to_string()
                .contains("guest runner exited with code 1")
        );
        assert!(!error.to_string().contains("delete-failure"));
    }

    #[tokio::test]
    async fn on_demand_slot_does_not_build_a_throwaway_successor() {
        let provider =
            Arc::new(TestProvider::new(false, false, false, false, false).announcing_busy());
        let mut config = test_config(false);
        config.size = 0;
        let handles = PoolHandles {
            idle: Arc::new(std::sync::Mutex::new(0)),
            keys: Arc::new(KeyPool::new()),
            building: Arc::new(AtomicUsize::new(0)),
            provisioning: Arc::new(std::sync::Mutex::new(0)),
        };

        run_on_demand_slot(
            provider.clone(),
            config.clone(),
            0,
            CancellationToken::new(),
            Arc::new(GoldenRegistry::new(config.name_prefix.clone())),
            handles,
            Arc::new(std::sync::Mutex::new(0)),
            Arc::new(tokio::sync::Semaphore::new(1)),
            Arc::new(std::sync::Mutex::new(None)),
        )
        .await
        .unwrap();

        let creates = provider
            .events()
            .await
            .into_iter()
            .filter(|event| event.starts_with("create:"))
            .collect::<Vec<_>>();
        assert_eq!(
            creates,
            vec!["create:lifecycle-test-0-1"],
            "size-zero mode must not provision a successor it immediately deletes"
        );
    }

    /// A custom base image is the operator's contract: the golden must not
    /// receive Preloop's curated bake. Stock bases still get it.
    #[tokio::test]
    async fn custom_base_golden_skips_the_curated_bake() {
        let provider = Arc::new(TestProvider::new(false, false, false, false, false));
        let config = test_config(false);
        let golden = MachineName::new("lifecycle-test-nobake-golden").unwrap();

        let custom = EnvironmentSpec::for_base("ghcr.io/acme/runner:latest".to_owned());
        assert!(!custom.curated);
        prepare_golden_for_env(&provider, &config, &golden, &custom)
            .await
            .expect("custom base golden provision succeeds");
        let custom_events = provider.events().await;
        assert!(
            !custom_events.iter().any(|event| event.contains("apt-get")),
            "a custom base golden must not run the curated apt bake: {custom_events:?}"
        );

        let stock = EnvironmentSpec::for_base(crate::environment::UBUNTU_24_04_PIN.to_owned());
        assert!(stock.curated);
        prepare_golden_for_env(&provider, &config, &golden, &stock)
            .await
            .expect("stock base golden provision succeeds");
        let stock_events = provider.events().await;
        assert!(
            stock_events.iter().any(|event| event.contains("apt-get")),
            "a stock base golden must run the curated apt bake"
        );
    }

    /// Direct (no-golden) provisioning of a custom base must not run the
    /// curated apt bake either — the image is the operator's contract.
    #[tokio::test]
    async fn custom_base_direct_provision_skips_the_curated_bake() {
        let provider = Arc::new(TestProvider::new(false, false, false, false, false));
        let config = test_config(false);
        let name = MachineName::new("lifecycle-test-nobake-direct").unwrap();

        provision_runner(
            &provider,
            &config,
            &name,
            None,
            &Arc::new(KeyPool::new()),
            &test_runner_environment("ghcr.io/acme/runner:latest", Vec::new(), false),
        )
        .await
        .expect("custom base provisioning succeeds");

        let events = provider.events().await;
        assert!(
            !events.iter().any(|event| event.contains("apt-get")),
            "a custom base must not receive the curated apt bake: {events:?}"
        );
    }
}

#[cfg(test)]
mod golden_download_tests {
    use super::*;
    use tokio::io::AsyncReadExt as _;
    use tokio::net::TcpListener;

    /// `download_prebaked_golden` takes its URL from the process environment,
    /// which every test in this binary shares, so two of them pointing at
    /// different servers would otherwise interleave.
    static GOLDEN_URL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[test]
    fn disk_check_refuses_only_what_cannot_fit() {
        let directory = tempfile::tempdir().unwrap();
        assert!(ensure_free_disk(directory.path(), 0, "test", "remedy").is_ok());
        assert!(ensure_free_disk(directory.path(), u64::MAX, "test", "remedy").is_err());
    }

    /// An artifact that cannot land must be refused before any byte is
    /// requested: the failure being prevented is a multi-GiB body dying at
    /// ENOSPC partway through.
    #[tokio::test]
    async fn download_larger_than_free_space_is_refused_before_requesting() {
        let directory = tempfile::tempdir().unwrap();
        let partial = directory.path().join("golden.smolmachine.partial");
        let requested = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = Arc::clone(&requested);
        let result =
            download_golden_with_resume(&partial, "test", Some(u64::MAX / 2), None, move |_| {
                observed.store(true, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async { Err("must not be requested".to_owned()) })
                    as BoxFuture<'static, Result<reqwest::Response, String>>
            })
            .await;
        assert!(result.is_err(), "an artifact that cannot fit must fail");
        assert!(
            !requested.load(std::sync::atomic::Ordering::SeqCst),
            "refusal must happen before the transfer is requested"
        );
        assert!(
            !partial.exists(),
            "nothing may be written for a refused transfer"
        );
    }

    #[test]
    fn custom_base_without_golden_url_does_not_adopt_stock_release() {
        assert!(should_download_prebaked_golden("ubuntu:24.04", false));
        assert!(!should_download_prebaked_golden(
            "ghcr.io/acme/runner-images:ubuntu24-runner-large-latest-arm64",
            false
        ));
        assert!(should_download_prebaked_golden(
            "ghcr.io/acme/runner-images:ubuntu24-runner-large-latest-arm64",
            true
        ));
    }

    /// Answers exactly one request with `head` followed by `body`, then closes
    /// the connection. Closing is what lets a deliberately short body reach the
    /// client as a stream error rather than a hang.
    async fn serve_once(head: String, body: Vec<u8>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = socket.read(&mut request).await;
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(&body).await;
            let _ = socket.shutdown().await;
        });
        format!("http://{address}/golden")
    }

    /// Answers the payload request, then one more connection with
    /// `checksum_body` (the `.sha256` companion). Used to exercise the
    /// checksum verification path.
    async fn serve_with_checksum(
        payload_head: String,
        payload_body: Vec<u8>,
        checksum_body: Vec<u8>,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for (head, body) in [
                (payload_head, payload_body),
                (
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                        checksum_body.len()
                    ),
                    checksum_body,
                ),
            ] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 1024];
                let _ = socket.read(&mut request).await;
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(&body).await;
                let _ = socket.shutdown().await;
            }
        });
        format!("http://{address}/golden")
    }

    fn leftovers(directory: &Path) -> Vec<String> {
        std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(".tmp-golden-"))
            .collect()
    }

    #[tokio::test]
    async fn streamed_download_lands_at_the_payload_path_byte_for_byte() {
        let _serialized = GOLDEN_URL.lock().await;
        let directory = tempfile::tempdir().unwrap();
        let payload = directory.path().join("golden.smolmachine");

        // Larger than any single chunk hyper will hand back, so the loop has to
        // append across iterations to reproduce the body.
        let body: Vec<u8> = (0..4_u32 * 1024 * 1024).map(|i| i as u8).collect();
        let url = serve_once(
            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()),
            body.clone(),
        )
        .await;
        unsafe { std::env::set_var("PRELOOP_GOLDEN_URL", &url) };

        let downloaded = download_prebaked_golden_with_space(&payload, "9.9.9", |_| Ok(u64::MAX))
            .await
            .unwrap();

        unsafe { std::env::remove_var("PRELOOP_GOLDEN_URL") };
        assert!(downloaded);
        assert_eq!(std::fs::read(&payload).unwrap(), body);
        assert!(leftovers(directory.path()).is_empty());
    }

    #[tokio::test]
    async fn insufficient_space_stops_before_creating_a_partial_golden() {
        let _serialized = GOLDEN_URL.lock().await;
        let directory = tempfile::tempdir().unwrap();
        let payload = directory.path().join("golden.smolmachine");
        let expected_bytes = 512 * 1024;
        let url = serve_once(
            format!("HTTP/1.1 200 OK\r\nContent-Length: {expected_bytes}\r\n\r\n"),
            Vec::new(),
        )
        .await;
        unsafe { std::env::set_var("PRELOOP_GOLDEN_URL", &url) };

        let error = download_prebaked_golden_with_space(&payload, "9.9.9", |_| Ok(1024))
            .await
            .expect_err("the download must stop when the target filesystem is full");

        unsafe { std::env::remove_var("PRELOOP_GOLDEN_URL") };
        match error {
            OrchestratorError::GoldenDiskSpace {
                path,
                available_bytes,
                required_bytes,
            } => {
                assert_eq!(path, directory.path());
                assert_eq!(available_bytes, 1024);
                assert_eq!(required_bytes, expected_bytes + GOLDEN_DOWNLOAD_HEADROOM);
            }
            other => panic!("unexpected error: {other}"),
        }
        assert!(!payload.exists());
        assert!(leftovers(directory.path()).is_empty());
    }

    #[tokio::test]
    async fn truncated_download_reports_failure_and_leaves_nothing_behind() {
        let _serialized = GOLDEN_URL.lock().await;
        let directory = tempfile::tempdir().unwrap();
        let payload = directory.path().join("golden.smolmachine");

        // Promising more than is sent makes the connection close mid-body, the
        // shape a dropped release download actually takes.
        let body = vec![0xAB_u8; 64 * 1024];
        let url = serve_once(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                body.len() + 4096
            ),
            body,
        )
        .await;
        unsafe { std::env::set_var("PRELOOP_GOLDEN_URL", &url) };

        let downloaded = download_prebaked_golden_with_space(&payload, "9.9.9", |_| Ok(u64::MAX))
            .await
            .unwrap();

        unsafe { std::env::remove_var("PRELOOP_GOLDEN_URL") };
        // The caller reads `false` as "build the golden locally", so a partial
        // file surviving here would be booted as if it were complete.
        assert!(!downloaded);
        assert!(!payload.exists());
        assert!(leftovers(directory.path()).is_empty());
    }

    /// Answers `plan.len()` sequential requests, recording each request line
    /// and headers. Closing after a short body is what turns a truncated
    /// transfer into a stream error the client can resume from.
    async fn serve_sequence(
        plan: Vec<(String, Vec<u8>)>,
    ) -> (String, std::sync::Arc<tokio::sync::Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let seen = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let recorder = seen.clone();
        tokio::spawn(async move {
            for (head, body) in plan {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut request = vec![0_u8; 4096];
                let read = socket.read(&mut request).await.unwrap_or(0);
                recorder
                    .lock()
                    .await
                    .push(String::from_utf8_lossy(&request[..read]).into_owned());
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(&body).await;
                let _ = socket.shutdown().await;
            }
        });
        (format!("http://{address}/golden"), seen)
    }

    #[test]
    fn golden_url_candidates_prefer_the_own_release_then_the_newest_with_a_golden() {
        let candidates = golden_url_candidates("0.33.2", Some("v0.33.6"), None);
        assert_eq!(candidates.len(), 3, "{candidates:?}");
        assert!(
            candidates[0].contains("/releases/download/v0.33.2/"),
            "{candidates:?}"
        );
        // An engine on a release that carries no golden must still find the
        // published one instead of baking locally — even when the newest
        // release is not the one holding the artifact.
        assert!(
            candidates[1].contains("/releases/download/v0.33.6/"),
            "{candidates:?}"
        );
        // The `latest` redirect is the last try: it needs no API call, so it
        // still covers a lookup that failed.
        assert!(
            candidates[2].contains("/releases/latest/download/"),
            "{candidates:?}"
        );

        // A resolved tag equal to the engine's own release is not retried.
        let same = golden_url_candidates("0.33.6", Some("v0.33.6"), None);
        assert_eq!(same.len(), 2, "{same:?}");

        // An operator-provided URL replaces every candidate: it is the only
        // source they asked for.
        let forced = golden_url_candidates(
            "0.33.2",
            Some("v0.33.6"),
            Some("https://example.test/golden".to_owned()),
        );
        assert_eq!(forced, vec!["https://example.test/golden".to_owned()]);
    }

    #[tokio::test]
    async fn golden_release_lookup_prefers_the_newest_release_that_has_it() {
        let body = serde_json::json!([
            // Newest release, but its golden step produced nothing.
            {"tag_name": "v9.9.9", "assets": [{"name": "preloop-cli-aarch64.tar.gz"}]},
            // A prerelease with the artifact must not outrank a stable one.
            {"tag_name": "v9.9.8", "prerelease": true, "assets": [{"name": golden_asset_name()}]},
            {"tag_name": "v9.9.7", "assets": [{"name": golden_asset_name()}]},
            {"tag_name": "v9.9.6", "assets": [{"name": golden_asset_name()}]}
        ])
        .to_string();
        let url = serve_once(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                body.len()
            ),
            body.into_bytes(),
        )
        .await;
        let api_base = url.trim_end_matches("/golden").to_owned();

        let tag = resolve_latest_golden_tag(&reqwest::Client::new(), &api_base).await;

        assert_eq!(
            tag.as_deref(),
            Some("v9.9.7"),
            "the newest stable release carrying the golden wins"
        );
    }

    #[tokio::test]
    async fn interrupted_golden_transfer_resumes_instead_of_restarting() {
        let directory = tempfile::tempdir().unwrap();
        let partial = directory.path().join("golden.smolmachine.partial");
        let body: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
        let cut = body.len() / 2;
        let (url, requests) = serve_sequence(vec![
            // First attempt: the headers promise the whole artifact and the
            // connection dies halfway — the failure a long registry pull hits.
            (
                format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()),
                body[..cut].to_vec(),
            ),
            // Second attempt: only the missing tail is served.
            (
                format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\n\r\n",
                    body.len() - cut
                ),
                body[cut..].to_vec(),
            ),
        ])
        .await;

        let downloaded = download_golden_with_resume(
            &partial,
            "test",
            Some(body.len() as u64),
            None,
            move |offset| {
                let url = url.clone();
                Box::pin(async move {
                    let mut request = reqwest::Client::new().get(&url);
                    if offset > 0 {
                        request =
                            request.header(reqwest::header::RANGE, format!("bytes={offset}-"));
                    }
                    request
                        .send()
                        .await
                        .map_err(|error| format!("request failed: {error}"))
                }) as BoxFuture<'static, Result<reqwest::Response, String>>
            },
        )
        .await
        .expect("a resumed transfer completes");

        assert_eq!(downloaded, body.len() as u64);
        assert_eq!(
            std::fs::read(&partial).unwrap(),
            body,
            "the resumed file must hold exactly one artifact"
        );
        let requests = requests.lock().await;
        assert_eq!(requests.len(), 2, "the transfer must retry once");
        assert!(
            requests[1]
                .to_lowercase()
                .contains(&format!("range: bytes={cut}-")),
            "second attempt must ask for the missing tail: {}",
            requests[1]
        );
    }

    #[tokio::test]
    async fn matching_checksum_is_verified_before_the_payload_lands() {
        let _serialized = GOLDEN_URL.lock().await;
        let directory = tempfile::tempdir().unwrap();
        let payload = directory.path().join("golden.smolmachine");

        let body: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
        use sha2::{Digest, Sha256};
        let digest: String = Sha256::digest(&body)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let url = serve_with_checksum(
            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()),
            body.clone(),
            format!("{digest}  golden\n").into_bytes(),
        )
        .await;
        unsafe { std::env::set_var("PRELOOP_GOLDEN_URL", &url) };

        let downloaded = download_prebaked_golden_with_space(&payload, "9.9.9", |_| Ok(u64::MAX))
            .await
            .unwrap();

        unsafe { std::env::remove_var("PRELOOP_GOLDEN_URL") };
        assert!(downloaded);
        assert_eq!(std::fs::read(&payload).unwrap(), body);
        assert!(leftovers(directory.path()).is_empty());
    }

    #[tokio::test]
    async fn mismatched_checksum_discards_the_download() {
        let _serialized = GOLDEN_URL.lock().await;
        let directory = tempfile::tempdir().unwrap();
        let payload = directory.path().join("golden.smolmachine");

        let body = vec![0xCD_u8; 64 * 1024];
        let url = serve_with_checksum(
            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()),
            body,
            format!("{}  golden\n", "00".repeat(32)).into_bytes(),
        )
        .await;
        unsafe { std::env::set_var("PRELOOP_GOLDEN_URL", &url) };

        let downloaded = download_prebaked_golden_with_space(&payload, "9.9.9", |_| Ok(u64::MAX))
            .await
            .unwrap();

        unsafe { std::env::remove_var("PRELOOP_GOLDEN_URL") };
        // A corrupted artifact must never be published as the payload: the
        // pool would boot it and only fail when a VM cannot start.
        assert!(!downloaded);
        assert!(!payload.exists());
        assert!(leftovers(directory.path()).is_empty());
    }
}

#[cfg(test)]
mod golden_registry_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn packed_failure_disables_secondary_environment_bakes() {
        let registry = GoldenRegistry::new("test".to_owned());
        assert!(!registry.is_packed_disabled());
        registry.disable_packed();
        assert!(registry.is_packed_disabled());
    }

    /// Distinct environments must build concurrently: one fingerprint's bake
    /// must not park other slots (the pre-freeze `build_lock` behavior).
    #[tokio::test]
    async fn distinct_fingerprints_build_concurrently() {
        let registry = GoldenRegistry::new("test".to_owned());
        let started = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));

        let build = |fp: &'static str,
                     started: Arc<AtomicUsize>,
                     active: Arc<AtomicUsize>,
                     max_active: Arc<AtomicUsize>| async move {
            started.fetch_add(1, Ordering::SeqCst);
            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
            max_active.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(250)).await;
            active.fetch_sub(1, Ordering::SeqCst);
            Ok(MachineName::new(format!("{fp}-golden")).unwrap())
        };

        let (fp_a, fp_b) = ("env-a", "env-b");
        let (a, b) = tokio::join!(
            registry.get_or_prepare(
                fp_a,
                build(fp_a, started.clone(), active.clone(), max_active.clone())
            ),
            registry.get_or_prepare(
                fp_b,
                build(fp_b, started.clone(), active.clone(), max_active.clone())
            ),
        );
        a.unwrap();
        b.unwrap();
        assert_eq!(max_active.load(Ordering::SeqCst), 2, "builds must overlap");
        assert_eq!(started.load(Ordering::SeqCst), 2);
    }

    /// The same fingerprint must build exactly once; the second caller gets
    /// the first caller's golden via the re-check.
    #[tokio::test]
    async fn same_fingerprint_builds_once() {
        let registry = GoldenRegistry::new("test".to_owned());
        let builds = Arc::new(AtomicUsize::new(0));
        let build = |builds: Arc<AtomicUsize>| async move {
            builds.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(MachineName::new("shared-golden").unwrap())
        };

        let (a, b) = tokio::join!(
            registry.get_or_prepare("same", build(builds.clone())),
            registry.get_or_prepare("same", build(builds.clone())),
        );
        let a = a.unwrap();
        let b = b.unwrap();
        assert_eq!(
            builds.load(Ordering::SeqCst),
            1,
            "duplicate build must not run"
        );
        assert_eq!(a.as_str(), b.as_str());
        assert_eq!(a.as_str(), "shared-golden");
    }
}
