//! SmolVM-backed ephemeral runner pool for Preloop CI.

pub mod environment;
mod keys;

include!(concat!(env!("OUT_DIR"), "/pins.rs"));

use crate::environment::{
    curated_toolchains, is_stock_base_image, EnvironmentSpec, ToolchainLayer,
};
use crate::keys::{KeyPool, StagedKey};
use preloop_gha_protocol::RUNNER_BUSY_SENTINEL;

/// Line an ephemeral runner prints when it accepts a job. Re-exported so a
/// `VmProvider` implementation can model the handshake this pool relies on.
pub use preloop_gha_protocol::RUNNER_BUSY_SENTINEL as RUNNER_BUSY_LINE;

use futures::StreamExt as _;
use preloop_vm::{
    MachineName, MachineSpec, MachineState, NetworkPolicy, OutputChunk, SecretSource,
    SmolVmProvider, SocketMount, VmError, VmProvider, VolumeMount,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const GUEST_CONTROL_DIR: &str = "/run/preloop-control";
const GUEST_CONTROL_SOCKET: &str = "/run/preloop-control/engine.sock";
const GUEST_FAILURE_MARKER: &str = "/var/lib/preloop-runner/.preloop-job-failed";
/// Written by the worker while a job is paused in a debug session and removed
/// when the session closes. The pool probes it to release the slot's
/// concurrency permit for the pause's duration — without it a paused job
/// pins a permit (and with `max_concurrent` permits total, eventually the
/// whole pool) until the session ends or the pause credit expires.
const GUEST_PAUSE_MARKER: &str = "/var/lib/preloop-runner/.preloop-job-paused";
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
) -> Vec<VolumeMount> {
    let mut volumes = vec![VolumeMount {
        host: config.runner_bundle.clone(),
        guest: PathBuf::from("/opt/preloop/bin"),
        read_only: true,
    }];
    // The Node externals are shared host-side, mounted read-only into machines
    // built from a registry base image — never baked into a machine image nor
    // downloaded per runner (that is what the `--no-externals` configure flag
    // enforces). Artifact-based machines (packed golden and create-per-runner)
    // skip the mount: the packed artifact already carries the externals baked
    // into its rootfs, and every virtio device consumes one of libkrun's 11
    // x86_64 IRQ lines — the packed launcher is already the device-heaviest
    // config (root + layers virtiofs + 2 disks + mounts + vsock + net +
    // console), so a third mount pushes it past the budget and the golden
    // fails to start (`RegisterNetDevice(IrqsExhausted)`). When the pack is
    // rebuilt without the baked externals, fold the mount back in (e.g. a
    // guest symlink `<root>/externals -> /opt/preloop/bin/externals` pointing
    // at an `externals/` dir shipped inside the runner bundle).
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
    volumes
}

/// Populate the host-side externals directory once, so every VM can mount
/// `node20`/`node24` instead of baking or downloading them per machine.
///
/// Reuses the same shell routine the golden bake used — it is pure
/// `curl | tar` plus an atomic temp-dir publish, so it runs identically on
/// the host. Skips the download when the external is already present, so a
/// concurrent engine start or an operator-provided directory keeps its
/// contents. Permissions are still normalized on every call: the download is
/// what gets skipped, not the repair, or a host that published its externals
/// before the guest runner went non-root would stay broken forever.
fn ensure_host_externals(config: &RunnerPoolConfig) -> Result<(), OrchestratorError> {
    let externals = config.externals_dir.join("externals");
    if !externals.join("node24").join("bin").join("node").is_file() {
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
    // Best-effort copy: the bundle lives in a root-owned release dir when
    // the engine runs unprivileged, and the deploy step materializes the
    // externals in that case.
    let bundle_externals = config.runner_bundle.join("externals");
    if !bundle_externals
        .join("node24")
        .join("bin")
        .join("node")
        .is_file()
    {
        let copy = std::fs::create_dir_all(&bundle_externals).and_then(|()| {
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
                "Could not materialize bundle externals (deploy step should copy them)"
            ),
            Err(error) => warn!(
                %error,
                bundle = %bundle_externals.display(),
                "Could not materialize bundle externals (deploy step should copy them)"
            ),
        }
    }
    // `cp -a` preserves the source mode, so the bundle copy needs the same
    // repair as the host directory.
    relax_externals_permissions(&bundle_externals);
    Ok(())
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

fn default_golden_url(release_version: &str) -> String {
    format!(
        "https://github.com/preloopdev/preloop/releases/download/v{release_version}/preloop-ubuntu-24.04-{}",
        std::env::consts::ARCH
    )
}

/// Public OCI artifact carrying the official arm64 packed VM golden.
///
/// This is deliberately separate from the `runner-images` base-image package:
/// the latter is an OCI rootfs image, while this package contains a
/// `.smolmachine` payload ready for `machine create --from`.
///
/// Pinned to the immutable manifest digest of the mutable
/// `ubuntu24-arm64-runner-large-latest` tag (verified reachable 2026-08-17):
/// a mutable tag could be silently replaced between the manifest fetch and
/// the blob pull, and moving the default stays a reviewed code change
/// instead of a registry retag. The artifact is produced by the CI golden
/// pipeline (pool-side bake of the official ubuntu24-arm64 runner image);
/// the repo release flow additionally publishes the packed golden as a
/// GitHub Release asset, which `PRELOOP_GOLDEN_URL` selects over this
/// default.
const DEFAULT_GOLDEN_OCI_REF: &str =
    "ghcr.io/preloopdev/preloop-golden@sha256:a2f7caf367e19efa4cb2d6f32a7093db8fae79e1b1525b65ac1190c1d2b44361";

fn should_download_prebaked_golden(base_image: &str, custom_golden_url: bool) -> bool {
    is_stock_base_image(base_image) || custom_golden_url
}

async fn download_prebaked_golden(payload: &Path, release_version: &str) -> bool {
    // An exported-but-blank `PRELOOP_GOLDEN_URL` must behave like an unset
    // one in both places below: the operator otherwise gets neither the OCI
    // default nor their (empty) override.
    let forced_url = std::env::var("PRELOOP_GOLDEN_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());
    if std::env::consts::ARCH == "aarch64" && forced_url.is_none() {
        let reference = std::env::var("PRELOOP_GOLDEN_OCI_REF")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_GOLDEN_OCI_REF.to_owned());
        if download_oci_golden(payload, &reference).await {
            return true;
        }
        warn!(
            reference,
            "default OCI golden unavailable; trying release asset"
        );
    }

    let default_url = default_golden_url(release_version);
    let url = forced_url.unwrap_or(default_url);

    info!(url = %url, target = %payload.display(), "Attempting to download pre-baked golden microVM image");

    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(600))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };

    let response = match client.get(&url).send().await {
        Ok(res) if res.status().is_success() => res,
        _ => {
            info!("Pre-baked golden image release not found; will build locally");
            return false;
        }
    };

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

    let tmp_payload = match payload.parent() {
        Some(parent) => parent.join(format!(".tmp-golden-{}", uuid::Uuid::new_v4())),
        None => return false,
    };

    let mut file = match tokio::fs::File::create(&tmp_payload).await {
        Ok(file) => file,
        Err(_) => return false,
    };

    // The body is copied chunk by chunk rather than through `bytes()`. A golden
    // carries the apt baseline, the Node externals and the VM's storage volume,
    // so it runs to hundreds of megabytes; buffering it whole would peak at the
    // full image size on a host that has not yet built anything, and the OOM
    // killer arriving here takes out the very process that would otherwise fall
    // back to building locally.
    let mut stream = response.bytes_stream();
    let mut streamed = true;
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            streamed = false;
            break;
        };
        if file.write_all(&chunk).await.is_err() {
            streamed = false;
            break;
        }
    }

    // `write_all` only queues work on a `tokio::fs::File`; the flush is what
    // surfaces a failed write-back. Skipping it would let a short write reach
    // the rename below and publish a truncated image that only fails much
    // later, when a VM tries to boot it.
    if !streamed || file.flush().await.is_err() {
        drop(file);
        let _ = tokio::fs::remove_file(&tmp_payload).await;
        return false;
    }
    drop(file);

    if let Some(expected) = expected_sha256 {
        let actual = match tokio::task::spawn_blocking({
            let tmp_payload = tmp_payload.clone();
            move || {
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                let mut file = std::fs::File::open(&tmp_payload)?;
                std::io::copy(&mut file, &mut hasher)?;
                let mut hex = String::with_capacity(64);
                for byte in hasher.finalize() {
                    use std::fmt::Write as _;
                    let _ = write!(hex, "{byte:02x}");
                }
                Ok::<String, std::io::Error>(hex)
            }
        })
        .await
        {
            Ok(Ok(actual)) => actual,
            _ => {
                let _ = tokio::fs::remove_file(&tmp_payload).await;
                return false;
            }
        };
        if actual != expected {
            warn!(
                expected,
                %actual,
                "golden checksum mismatch; discarding download and building locally"
            );
            let _ = tokio::fs::remove_file(&tmp_payload).await;
            return false;
        }
    }

    if tokio::fs::rename(&tmp_payload, payload).await.is_err() {
        let _ = tokio::fs::remove_file(&tmp_payload).await;
        return false;
    }

    info!(target = %payload.display(), "Downloaded pre-baked golden microVM image successfully");
    true
}

#[derive(Debug, Deserialize)]
struct OciManifest {
    #[serde(default)]
    layers: Vec<OciLayer>,
}

#[derive(Debug, Deserialize)]
struct OciLayer {
    digest: String,
    /// OCI descriptors name this field `mediaType`; without the rename every
    /// standard manifest fails to parse and the OCI path silently falls back
    /// to the release asset.
    #[serde(rename = "mediaType")]
    media_type: String,
}

#[derive(Debug, Deserialize)]
struct OciToken {
    token: String,
}

/// Download the packed VM layer from a public OCI artifact without requiring
/// `oras`, Docker, or any other host-side registry client.
async fn download_oci_golden(payload: &Path, reference: &str) -> bool {
    let Some((registry, repository, version)) = split_oci_reference(reference) else {
        warn!(reference, "invalid OCI golden reference");
        return false;
    };
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(600))
        .build()
    {
        Ok(client) => client,
        Err(_) => return false,
    };
    let manifest_url = format!("https://{registry}/v2/{repository}/manifests/{version}");
    let accept = "application/vnd.oci.image.manifest.v1+json, \
                  application/vnd.docker.distribution.manifest.v2+json";
    let Some(response) = registry_get(&client, &manifest_url, accept).await else {
        info!(reference, "OCI golden manifest unavailable");
        return false;
    };
    let manifest = match response.json::<OciManifest>().await {
        Ok(manifest) => manifest,
        Err(error) => {
            warn!(reference, %error, "OCI golden manifest parse failed");
            return false;
        }
    };
    let Some(layer) = manifest
        .layers
        .into_iter()
        .find(|layer| layer.media_type == "application/vnd.preloop.smolmachine.v1+zstd")
    else {
        warn!(reference, "OCI golden has no packed VM layer");
        return false;
    };
    let blob_url = format!("https://{registry}/v2/{repository}/blobs/{}", layer.digest);
    let Some(response) = registry_get(&client, &blob_url, "*/*").await else {
        return false;
    };
    let Some(parent) = payload.parent() else {
        return false;
    };
    let tmp_payload = parent.join(format!(".tmp-golden-{}", uuid::Uuid::new_v4()));
    if stream_golden_response(response, &tmp_payload, Some(layer.digest)).await
        && tokio::fs::rename(&tmp_payload, payload).await.is_ok()
    {
        info!(
            reference,
            target = %payload.display(),
            "Downloaded OCI pre-baked golden microVM image successfully"
        );
        return true;
    }
    let _ = tokio::fs::remove_file(&tmp_payload).await;
    false
}

async fn registry_get(
    client: &reqwest::Client,
    url: &str,
    accept: &str,
) -> Option<reqwest::Response> {
    let response = client
        .get(url)
        .header(reqwest::header::ACCEPT, accept)
        .send()
        .await
        .ok()?;
    if response.status().is_success() {
        return Some(response);
    }
    if response.status() != reqwest::StatusCode::UNAUTHORIZED {
        return None;
    }
    let challenge = response
        .headers()
        .get(reqwest::header::WWW_AUTHENTICATE)?
        .to_str()
        .ok()?;
    let realm = auth_parameter(challenge, "realm")?;
    let service = auth_parameter(challenge, "service")?;
    let scope = auth_parameter(challenge, "scope")?;
    let token = client
        .get(realm)
        .query(&[("service", service), ("scope", scope)])
        .send()
        .await
        .ok()?
        .json::<OciToken>()
        .await
        .ok()?;
    client
        .get(url)
        .header(reqwest::header::ACCEPT, accept)
        .bearer_auth(token.token)
        .send()
        .await
        .ok()
        .filter(|response| response.status().is_success())
}

fn auth_parameter(challenge: &str, name: &str) -> Option<String> {
    challenge.split(',').find_map(|part| {
        let (key, value) = part.trim().split_once('=')?;
        (key.trim()
            .trim_start_matches("Bearer ")
            .eq_ignore_ascii_case(name))
        .then(|| value.trim_matches('"').to_owned())
    })
}

fn split_oci_reference(reference: &str) -> Option<(String, String, String)> {
    let (registry, remainder) = reference.split_once('/')?;
    let (repository, version) = remainder
        .rsplit_once('@')
        .or_else(|| remainder.rsplit_once(':'))?;
    if registry.is_empty() || repository.is_empty() || version.is_empty() {
        return None;
    }
    Some((
        registry.to_owned(),
        repository.to_owned(),
        version.to_owned(),
    ))
}

/// Stream the OCI layer to a temporary file, verify its digest against the
/// manifest descriptor, then install it at the payload path.
///
/// The published `application/vnd.preloop.smolmachine.v1+zstd` layer is the
/// raw `.smolmachine` sidecar: zstd-compressed asset frames followed by the
/// uncompressed manifest and `SMOLPACK` footer. The media type's `+zstd`
/// suffix describes the internal asset compression, not the layer itself —
/// the layer bytes are NOT a bare zstd stream (verified: the blob ends with
/// an uncompressed `SMOLPACK` trailer), and `machine create --from` reads
/// the sidecar container directly. Do not decompress the layer.
async fn stream_golden_response(
    response: reqwest::Response,
    tmp_payload: &Path,
    expected_sha256: Option<String>,
) -> bool {
    let mut file = match tokio::fs::File::create(tmp_payload).await {
        Ok(file) => file,
        Err(_) => return false,
    };
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            let _ = tokio::fs::remove_file(tmp_payload).await;
            return false;
        };
        if file.write_all(&chunk).await.is_err() {
            let _ = tokio::fs::remove_file(tmp_payload).await;
            return false;
        }
    }
    if file.flush().await.is_err() {
        let _ = tokio::fs::remove_file(tmp_payload).await;
        return false;
    }
    drop(file);
    if let Some(expected) = expected_sha256 {
        let digest = match tokio::task::spawn_blocking({
            let path = tmp_payload.to_owned();
            move || {
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                let mut file = std::fs::File::open(path)?;
                std::io::copy(&mut file, &mut hasher)?;
                Ok::<String, std::io::Error>(format!("{:x}", hasher.finalize()))
            }
        })
        .await
        {
            Ok(Ok(digest)) => digest,
            _ => return false,
        };
        let expected = expected.strip_prefix("sha256:").unwrap_or(&expected);
        if digest != expected {
            warn!(expected, %digest, "OCI golden layer digest mismatch");
            return false;
        }
    }
    true
}

/// First whitespace-separated token of a `sha256sum`-style checksum file
/// (`<hex>  <filename>`), lowercased. `None` when the file does not parse.
fn parse_sha256_checksum(text: &str) -> Option<String> {
    let token = text.split_whitespace().next()?;
    let token = token.strip_prefix("sha256:").unwrap_or(token);
    if token.len() == 64 && token.bytes().all(|b| b.is_ascii_hexdigit()) {
        Some(token.to_ascii_lowercase())
    } else {
        None
    }
}
/// Packages the golden image carries.
///
/// Tracks the apt package list of GitHub's `ubuntu-latest` runner image, which
/// is what workflows are written against. Any gap here produces the exact bug
/// class this project exists to eliminate: "works on GitHub, fails locally".
///
/// Deliberately *only* the apt baseline — not `ubuntu-latest`'s preinstalled
/// toolchains (Android SDK, five JDKs, .NET, browsers, cloud CLIs). Those come
/// to ~90 GB and are the job of `actions/setup-*` and `container:`, which keeps
/// workflows portable. This list is ~350 MB.
const BASE_PACKAGES: &str = "\
     git curl wget ca-certificates gnupg2 sudo openssh-client \
     build-essential pkg-config libssl-dev make autoconf automake libtool m4 \
     bison flex texinfo patchelf swig dpkg-dev fakeroot binutils \
     libicu-dev libsqlite3-dev libyaml-dev \
     python3 python3-pip python-is-python3 \
     unzip zip xz-utils zstd bzip2 brotli lz4 pigz p7zip-full tar \
     jq file tree shellcheck parallel time acl locales tzdata \
     rsync dnsutils iputils-ping net-tools iproute2 netcat-openbsd \
     sqlite3 rpm aria2 mercurial";

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
const RUNNER_ROOT: &str = "/var/lib/preloop-runner";

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

/// PATH exported to the guest runner. Exposed for the lifecycle tests.
pub fn guest_runner_path() -> &'static str {
    GUEST_RUNNER_PATH
}

fn node_externals_at(runner_root: &str) -> Vec<Vec<String>> {
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
               if [ -f \"$DEST/bin/node\" ]; then \
                 chmod 755 \"$DEST\"; \
                 echo \"$NAME already present, skipping\"; continue; \
               fi; \
               echo \"Installing $NAME $VERSION into golden...\"; \
               TEMP=$(mktemp -d \"$RUNNER_EXTERNALS/.$NAME.XXXXXX\") && \
                chmod 755 \"$TEMP\" && \
                ARCH=$(uname -m); \
                if [ \"$ARCH\" = \"aarch64\" ] || [ \"$ARCH\" = \"arm64\" ]; then NODE_ARCH=linux-arm64; else NODE_ARCH=linux-x64; fi; \
                curl -fsSL \"https://nodejs.org/dist/$VERSION/node-$VERSION-$NODE_ARCH.tar.gz\" | \
                 tar -xz --strip-components=1 -C \"$TEMP\" && \
               if [ ! -f \"$TEMP/bin/node\" ]; then \
                 echo \"ERROR: $NAME tarball missing bin/node\" >&2; \
                 rm -rf \"$TEMP\"; exit 1; \
               fi && \
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
         (echo \"### install hosted apt baseline\" >&2 && \
          if DEBIAN_FRONTEND=noninteractive \
             apt-get -s install -qq --no-install-recommends {base_packages_pinned} >/dev/null 2>&1; then \
            DEBIAN_FRONTEND=noninteractive \
            apt-get install -y -qq --no-install-recommends {base_packages_pinned}; \
          else \
            echo \"WARNING: exact hosted apt pins are unavailable; falling back to archive versions\" >&2; \
            DEBIAN_FRONTEND=noninteractive \
            apt-get install -y -qq --no-install-recommends {BASE_PACKAGES}; \
          fi) \
         && printf '{LOOPBACK_HOSTS}' > /etc/hosts && \
         printf '127.0.0.1 %s\\n' \"$(hostname)\" >> /etc/hosts && \
         printf 'APT::Get::Assume-Yes \"true\";\\n' > /etc/apt/apt.conf.d/90assumeyes && \
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
         install -d -m 0775 -o 1001 -g 1001 /opt/hostedtoolcache && \
         (useradd -m -u 1000 -s /bin/bash ubuntu 2>/dev/null || true) && \
         apt-get clean && \
         rm -rf /usr/share/doc/* /usr/share/man/* /usr/share/info/*",
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
/// a fork would wake up with stale state and a socket it does not own. Machines
/// are pre-provisioned, so this sits off the critical path of any job.
///
/// Never fatal. A pool without a working container engine still runs every job
/// that does not use `container:` or `services:`.
///
/// Readiness is `docker info` rather than `pgrep dockerd`, because a forked VM
/// can carry a `[dockerd] <defunct>` entry from its golden: a name match sees
/// the zombie, concludes Docker is up, and leaves the runner with no daemon.
/// A stale `/var/run/docker.pid` naming that same pid blocks startup outright,
/// and is only removed once `docker info` has failed  so it is stale by
/// definition.
fn docker_start_command() -> Vec<String> {
    vec![
        "sh".to_owned(),
        "-c".to_owned(),
        run_as_root_or_sudo(&format!(
            "command -v dockerd >/dev/null 2>&1 || exit 0; \
             docker info >/dev/null 2>&1 && exit 0; \
             rm -f /var/run/docker.pid; \
             mkdir -p {DOCKER_DATA_ROOT}; \
             (dockerd >/var/log/dockerd.log 2>&1 &) ; \
             for _ in $(seq 1 50); do \
               docker info >/dev/null 2>&1 && exit 0; \
               sleep 0.2; \
             done; \
             exit 0"
        )),
    ]
}

/// How long to wait for a freshly started guest to accept commands.
const GUEST_READY_TIMEOUT: Duration = Duration::from_secs(30);
/// How long to wait between live-clone drain probes before re-arming a spent
/// golden fork base. Bounded retries; the probe loop is exercised by tests
/// under paused Tokio time, so this is the only knob the delay is tied to.
const GOLDEN_DRAIN_PROBE_DELAY: Duration = Duration::from_secs(10);
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
                return Err(OrchestratorError::from(error))
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
    provider
        .exec(
            name,
            &[
                "sh".to_owned(),
                "-c".to_owned(),
                format!(
                    "printf '%s' '{}' > /etc/preloop-bake.json",
                    json.replace('\'', "'\\''")
                ),
            ],
        )
        .await?;
    info!(
        machine = name.as_str(),
        "bake manifest written to /etc/preloop-bake.json"
    );
    Ok(())
}

/// PATH the guest runner process exports to every step.
///
/// Hosted images carry the toolchain bin directories on the runner's own PATH,
/// which is what makes `cargo install`-style actions work: `taiki-e/install-action`
/// drops `cargo-hack` in `$CARGO_HOME/bin` and the next step runs `cargo hack`.
/// dtolnay/rust-toolchain only appends that directory to `$GITHUB_PATH` when it
/// has to install rustup itself, so on an image that already has rustup — ours,
/// and GitHub's — the directory is on PATH or the tool is simply unreachable.
/// Guests run as root, so `$HOME/.cargo` is `/root/.cargo`; the Go layer
/// untars into `/usr/local/go`. Absent directories cost nothing.
const GUEST_RUNNER_PATH: &str = "/root/.cargo/bin:/usr/local/go/bin:\
                                 /usr/local/sbin:/usr/local/bin:\
                                 /usr/sbin:/usr/bin:/sbin:/bin";

/// `env` prefix for guest runner invocations, empty when nothing needs setting.
///
/// Control-socket routing and failure-marker debugging are independent
/// features: a pool can debug failed jobs without a mounted control socket and
/// vice versa, so neither may gate the other.
fn guest_env_prefix(config: &RunnerPoolConfig, name: &MachineName) -> Vec<String> {
    let mut env = Vec::new();
    env.push(format!("PATH={GUEST_RUNNER_PATH}"));
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
}

impl GoldenRegistry {
    pub fn new(name_prefix: String) -> Self {
        Self {
            goldens: RwLock::new(HashMap::new()),
            build_locks: RwLock::new(HashMap::new()),
            name_prefix,
        }
    }

    /// Return the name prefix used for golden VM names.
    pub fn name_prefix(&self) -> &str {
        &self.name_prefix
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
    let refs = images
        .iter()
        .map(|image| format!("'{}'", image.replace('\'', "'\\''")))
        .collect::<Vec<_>>()
        .join(" ");
    let script = run_as_root_or_sudo(&format!(
        "command -v dockerd >/dev/null 2>&1 || {{ echo 'no dockerd' >&2; exit 1; }}; \
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
    ));
    let output = provider
        .exec(golden, &["sh".to_owned(), "-c".to_owned(), script])
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

/// Whether `golden` is already booted as a fork base for `fingerprint`.
///
/// Deliberately host-side: the golden is frozen as SmolVM's fork base, and
/// probing it through the guest would touch the very snapshot every clone is
/// taken from.
async fn golden_is_reusable<P: VmProvider>(
    provider: &Arc<P>,
    config: &RunnerPoolConfig,
    golden: &MachineName,
    fingerprint: &str,
) -> bool {
    if !matches!(provider.status(golden).await, Ok(MachineState::Running)) {
        return false;
    }
    let Some(path) = golden_record_path(config, golden) else {
        return false;
    };
    std::fs::read_to_string(path)
        .map(|recorded| recorded.trim() == fingerprint)
        .unwrap_or(false)
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
    // The golden registry is in-memory, so without this every engine restart
    // rebakes a golden that is still sitting there fully baked and forkable —
    // apt plus rustup, five to eleven minutes, before the first job of that
    // environment can run, paid again on every deploy.
    if golden_is_reusable(provider, config, golden, &env_spec.fingerprint).await {
        info!(
            machine = golden.as_str(),
            fingerprint = %env_spec.fingerprint,
            "adopted the existing golden fork base"
        );
        return Ok(());
    }
    // Any record must die before the machine does: a rebake interrupted
    // halfway would otherwise leave a fingerprint claiming a golden that no
    // longer carries it.
    remove_golden_record(config, golden);
    if provider.status(golden).await? != MachineState::Missing {
        provider.delete(golden).await?;
    }
    let spec = MachineSpec {
        name: golden.clone(),
        image: env_spec.base.clone(),
        cpus: config.cpus,
        memory_mib: config.memory_mib,
        storage_gib: config.storage_gib,
        overlay_gib: config.overlay_gib,
        network: NetworkPolicy::PublicOnly,
        volumes: runner_volumes(config, golden, true),
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
    if let Err(error) = await_guest_ready(provider.as_ref(), golden).await {
        let _ = provider.delete(golden).await;
        return Err(error);
    }
    if env_spec.curated {
        if let Err(error) = install_base_dependencies(provider.as_ref(), golden).await {
            let _ = provider.delete(golden).await;
            return Err(error);
        }
    }
    for layer in &env_spec.toolchains {
        for command in layer.install_commands() {
            if let Err(error) = provider.exec(golden, &command).await {
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
        let _ = provider.delete(golden).await;
        return Err(error.into());
    }
    if let Err(error) = provider.start_forkable(golden).await {
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
    // re-unpack a multi-GiB packed golden that is still sitting there forkable
    // and fingerprint-matched. Without this every `serve` restart pays the
    // full unpack (tens of GB of storage writes) before the first job.
    let env_spec = EnvironmentSpec::for_base(config.base_image.clone());
    if golden_is_reusable(provider, config, golden, &env_spec.fingerprint).await {
        info!(
            machine = golden.as_str(),
            fingerprint = %env_spec.fingerprint,
            "adopted the existing packed golden fork base"
        );
        return Ok(());
    }
    remove_golden_record(config, golden);
    if provider.status(golden).await? != MachineState::Missing {
        provider.delete(golden).await?;
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
        volumes: runner_volumes(config, golden, false),
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
    if let Err(error) = await_guest_ready(provider.as_ref(), golden).await {
        let _ = provider.delete(golden).await;
        return Err(error);
    }
    if let Err(error) = preload_images(provider.as_ref(), golden, &config.preload_images).await {
        warn!(
            machine = golden.as_str(),
            %error, "image preload failed; jobs will pull at run time"
        );
    }
    provider.stop(golden).await?;
    provider.start_forkable(golden).await?;
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
        ensure_host_externals(&self.config)?;
        if self.config.use_packed_artifact || self.config.control_socket.is_none() {
            self.prepare_artifact(true).await?;
        }
        self.remove_stale_machines().await?;

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
                warn!(%error, "golden fork base unavailable; falling back to create-per-runner");
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

        let mut slots = JoinSet::new();
        // Runners currently registered and waiting for work. Slots consult it
        // to decide whether a replacement is worth booting mid-job.
        let idle = Arc::new(AtomicUsize::new(0));
        // Filled in the background so no slot ever waits on RSA generation.
        let keys = Arc::new(KeyPool::new());
        keys.spawn_refill();
        let building = Arc::new(AtomicUsize::new(0));

        // On-demand mode: size=0 means no warm pool. Fork runners only when
        // jobs arrive, capped by the host's CPU budget.
        if self.config.size == 0 {
            return self
                .run_on_demand(shutdown, golden_registry, idle, keys, building)
                .await;
        }

        for slot in 0..self.config.size {
            let provider = self.provider.clone();
            let config = self.config.clone();
            let slot_shutdown = shutdown.child_token();
            let slot_registry = golden_registry.clone();
            let slot_handles = PoolHandles {
                idle: idle.clone(),
                keys: keys.clone(),
                building: building.clone(),
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
                    Some(Ok(Err(error))) => return Err(error),
                    Some(Err(error)) => return Err(OrchestratorError::Pool(error.to_string())),
                    Some(Ok(Ok(()))) => return Err(OrchestratorError::Pool("runner slot exited".into())),
                    None => return Err(OrchestratorError::Pool("runner pool had no slots".into())),
                }
            }
        }

        while slots.join_next().await.is_some() {}
        // Clean up every environment-specific golden fork base.
        for golden in golden_registry.all_names().await {
            let _ = self.provider.delete(&golden).await;
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
        idle: Arc<AtomicUsize>,
        keys: Arc<KeyPool>,
        building: Arc<AtomicUsize>,
    ) -> Result<(), OrchestratorError> {
        let max_concurrent = {
            let parallelism = std::thread::available_parallelism().map_or(2, |value| value.get());
            (parallelism / usize::from(self.config.cpus.max(1))).max(1)
        };
        info!(max_concurrent, "on-demand runner pool (size=0)");

        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
        let provisioning = Arc::new(AtomicUsize::new(0));
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
        for golden in golden_registry.all_names().await {
            let _ = self.provider.delete(&golden).await;
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
            if download_prebaked_golden(&payload, &self.config.release_version).await {
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
        let spec = MachineSpec {
            name: name.clone(),
            image: self.config.base_image.clone(),
            cpus: self.config.cpus,
            memory_mib: self.config.memory_mib,
            // Packing exports a second copy of the guest filesystem before
            // producing the artifact. Give the one-shot builder headroom
            // without increasing the storage allocated to job VMs.
            storage_gib: self.config.storage_gib.max(40),
            overlay_gib: self.config.overlay_gib,
            network: NetworkPolicy::PublicOnly,
            volumes: Vec::new(),
            sockets: Vec::new(),
            dns: self.config.dns.clone(),
            rosetta: cfg!(target_os = "macos") && std::env::consts::ARCH == "aarch64",
        };
        self.provider.create(&spec).await?;
        self.provider.start(&name).await?;
        // A custom base image is the operator's contract: use it as-is. Only
        // the stock digest-pinned Ubuntu bases get the curated bake (the
        // GitHub-hosted parity toolset — node/python/go toolcaches, git,
        // docker, nvm, yarn — plus the Rust layer; `setup-*` actions download
        // any other version a job asks for at job time).
        if is_stock_base_image(&self.config.base_image) {
            if let Err(error) = install_base_dependencies(self.provider.as_ref(), &name).await {
                let _ = self.provider.delete(&name).await;
                return Err(error);
            }
            let toolchains = curated_toolchains();
            for layer in &toolchains {
                for command in layer.install_commands() {
                    if let Err(error) = self.provider.exec(&name, &command).await {
                        let _ = self.provider.delete(&name).await;
                        return Err(error.into());
                    }
                }
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
        if let Err(error) = self.provider.pack(&name, &temporary).await {
            let _ = std::fs::remove_file(&temporary);
            return Err(error.into());
        }
        // smolvm pack writes two files: `<output>` (ELF executable stub) and
        // `<output>.smolmachine` (the packed VM data). The latter is the
        // artifact consumed by `machine create --from`; the stub is only a
        // launcher and is discarded.
        let sidecar = PathBuf::from(format!("{}.smolmachine", temporary.display()));
        std::fs::rename(&sidecar, &payload).inspect_err(|_| {
            let _ = std::fs::remove_file(&temporary);
            let _ = std::fs::remove_file(&sidecar);
        })?;
        let _ = std::fs::remove_file(&temporary);
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
                notify_runner_gone(&self.config, &name).await;
                if let Err(error) = self.provider.delete(&name).await {
                    warn!(machine = name.as_str(), %error, "failed to delete stale Preloop runner");
                }
            }
        }
        // A crashed server orphans its detached `_boot-vm` hypervisor
        // processes; when the data dir was cleaned out from under them the
        // smolvm DB no longer knows the machines, so the deletes above
        // cannot reach them and they keep the storage fds open — the
        // unlinked blocks leak until the process dies. Kill by config path.
        match preloop_vm::purge_orphaned_vms() {
            Ok(killed) if killed > 0 => {
                info!(killed, "purged orphaned SmolVM hypervisor processes")
            }
            _ => {}
        }
        Ok(())
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
    idle: Arc<AtomicUsize>,
    /// Keypairs generated ahead of time for runner registration.
    keys: Arc<KeyPool>,
    /// Replacements currently being built across the whole pool.
    building: Arc<AtomicUsize>,
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
    idle: &'a AtomicUsize,
    /// Keypairs generated ahead of time for runner registration.
    keys: &'a Arc<KeyPool>,
    /// Replacements currently being built across the whole pool.
    building: &'a AtomicUsize,
    /// Whether this slot keeps a warm successor after the current job.
    prebuild_successor: bool,
}

/// A claim on one of the replacement builds the backlog justifies.
///
/// Held for the duration of the build so concurrent slots see it, and released
/// on drop so an error path cannot strand the count.
struct Reservation<'a>(&'a AtomicUsize);

impl<'a> Reservation<'a> {
    /// Claim a build slot, or `None` when `wanted` are already in flight.
    fn take(building: &'a AtomicUsize, wanted: usize) -> Option<Self> {
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
                Ok(_) => return Some(Self(building)),
                Err(observed) => current = observed,
            }
        }
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Keeps the server's starvation clock paused while any on-demand runner is
/// still being provisioned. A counter is required because size-zero mode can
/// create several runners concurrently; the first completed runner must not
/// clear the signal while another job's runner is still bootstrapping.
struct PreparingGuard {
    active: Arc<AtomicUsize>,
    signal: Option<Arc<std::sync::atomic::AtomicBool>>,
}

impl PreparingGuard {
    fn enter(active: Arc<AtomicUsize>, signal: Option<Arc<std::sync::atomic::AtomicBool>>) -> Self {
        if active.fetch_add(1, Ordering::AcqRel) == 0 {
            if let Some(signal) = &signal {
                signal.store(true, Ordering::Release);
            }
        }
        Self { active, signal }
    }
}

impl Drop for PreparingGuard {
    fn drop(&mut self) {
        if self.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            if let Some(signal) = &self.signal {
                signal.store(false, Ordering::Release);
            }
        }
    }
}

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
async fn watch_guest_pause<P: VmProvider + 'static>(
    provider: Arc<P>,
    name: MachineName,
    permit: Arc<std::sync::Mutex<Option<tokio::sync::OwnedSemaphorePermit>>>,
    semaphore: Arc<tokio::sync::Semaphore>,
    poll_interval: Duration,
) {
    let probe = [
        "test".to_owned(),
        "-f".to_owned(),
        GUEST_PAUSE_MARKER.to_owned(),
    ];
    let mut was_paused = false;
    let mut last_probe_warn: Option<tokio::time::Instant> = None;
    loop {
        tokio::time::sleep(poll_interval).await;
        // `smolvm machine exec` propagates the guest exit code as its own
        // exit code, so the normal absent-marker probe surfaces as
        // `VmError::Command` with exit 1 — a real result, not a transport
        // failure; `test -f` only ever exits 0 or 1. Any other error says
        // nothing about the pause state: treating it as "resumed" would
        // re-pin the permit mid-pause and revive the starvation this
        // watcher exists to remove, so preserve the last known state.
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
        if paused == was_paused {
            continue;
        }
        if paused {
            let released = { permit.lock().unwrap().take() }.is_some();
            if released {
                info!(
                    machine = name.as_str(),
                    "job paused in debug session — released pool concurrency permit"
                );
            }
        } else {
            // Re-acquire before treating the machine as active again, so
            // future forks stay bounded by `max_concurrent` plus whatever is
            // genuinely paused. The guest resumes on its own after the
            // verdict, so this acquire can transiently lag the resume by up
            // to a poll interval — the over-subscription window is bounded
            // and short. A hard gate needs a host/worker resume handshake;
            // until then, a slow acquire is surfaced here.
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
                    "resumed job waited for a pool permit — active VMs may have \
                     transiently exceeded max_concurrent"
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

/// Single-shot on-demand runner: provision, run exactly one job, clean up.
#[allow(clippy::too_many_arguments)]
async fn run_on_demand_slot<P: VmProvider + 'static>(
    provider: Arc<P>,
    config: RunnerPoolConfig,
    slot: usize,
    shutdown: CancellationToken,
    golden_registry: Arc<GoldenRegistry>,
    handles: PoolHandles,
    provisioning: Arc<AtomicUsize>,
    semaphore: Arc<tokio::sync::Semaphore>,
    permit: Arc<std::sync::Mutex<Option<tokio::sync::OwnedSemaphorePermit>>>,
) -> Result<(), OrchestratorError> {
    let preparing = PreparingGuard::enter(provisioning, config.preparing_signal.clone());
    // Resolve the golden for the queued job's environment.
    let (golden, environment) = if config.use_fork {
        let env_base = match &config.next_job_runs_on {
            Some(lock) => {
                let labels = lock.read().map(|g| g.clone()).unwrap_or_default();
                if labels.is_empty() {
                    config.base_image.clone()
                } else {
                    EnvironmentSpec::default_base(&labels)
                }
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
    let runner = provision_slot(
        &provider,
        &config,
        slot,
        generation,
        golden.as_ref(),
        &handles.keys,
        environment.clone(),
    )
    .await?;
    // `provision_slot` returns only after runner registration succeeds. From
    // this point the starvation sweep can see a matching runner directly.
    drop(preparing);

    // Run exactly one job — no successor pre-provisioning. While the job
    // runs, watch the guest pause marker: a debug-session pause must hand
    // the concurrency permit back to the pool instead of pinning it.
    let pause_watch = (config.debug_dir.is_some()).then(|| {
        let provider = provider.clone();
        let name = runner.name.clone();
        let permit = permit.clone();
        let semaphore = semaphore.clone();
        tokio::spawn(watch_guest_pause(
            provider,
            name,
            permit,
            semaphore,
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
    let PoolHandles {
        idle,
        keys,
        building,
    } = handles;
    let mut generation: u64 = 0;
    let mut spare: Option<ReadyRunner> = None;

    while !shutdown.is_cancelled() {
        let (golden, environment) = if config.use_fork {
            // Read the `runs-on` labels of the next queued job so the pool
            // can select the correct base-image golden before forking.
            let env_base = match &config.next_job_runs_on {
                Some(lock) => {
                    let labels = lock.read().map(|g| g.clone()).unwrap_or_default();
                    if labels.is_empty() {
                        config.base_image.clone()
                    } else {
                        EnvironmentSpec::default_base(&labels)
                    }
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
                Ok(name) => Some(name),
                Err(error) => {
                    warn!(%error, %fingerprint, "failed to prepare requested environment golden; leaving job queued");
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {}
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
                )
                .await
                {
                    Ok(runner) => runner,
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
        let _ = provider.delete(&spare.name).await;
    }
    Ok(())
}

/// Provision one ephemeral runner for a slot under a fresh machine name.
///
/// Names carry a generation so a replacement can boot while its predecessor is
/// still being torn down; reusing one name per slot forced those to serialize.
async fn provision_slot<P: VmProvider + 'static>(
    provider: &Arc<P>,
    config: &RunnerPoolConfig,
    slot: usize,
    generation: u64,
    golden: Option<&MachineName>,
    keys: &Arc<KeyPool>,
    environment: RunnerEnvironment,
) -> Result<ReadyRunner, OrchestratorError> {
    let name = MachineName::new(format!("{}-{slot}-{generation}", config.name_prefix))?;
    match provision_runner(provider, config, &name, golden, keys, &environment).await {
        Ok(run) => Ok(ReadyRunner {
            name,
            run,
            environment,
        }),
        Err(error) => {
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
        if !labels.is_empty() && EnvironmentSpec::default_base(&labels) != current_base {
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
        prebuild_successor,
    } = plan;

    let (busy_tx, busy_rx) = tokio::sync::oneshot::channel();
    let claimed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let run_provider = provider.clone();
    let run_name = name.clone();
    idle.fetch_add(1, Ordering::AcqRel);
    let mut run_task =
        tokio::spawn(async move { run_until_exit(&run_provider, &run_name, &run, busy_tx).await });

    // Resolves once the runner reports a job and its replacement is ready. A
    // runner that exits without taking a job (shutdown, transient failure)
    // drops the sender, and this yields `None` without provisioning anything.
    let pending_jobs = config.pending_jobs.as_deref();
    let successor_claimed = claimed.clone();
    let build_successor = async {
        if busy_rx.await.is_err() {
            idle.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        successor_claimed.store(true, Ordering::Release);
        let idle_after = idle.fetch_sub(1, Ordering::AcqRel).saturating_sub(1);
        if !prebuild_successor {
            return None;
        }
        // Booting a VM costs real CPU, and it would be spent alongside the job
        // that just started, so build exactly as many replacements as the
        // backlog needs and no more.
        //
        // The shortfall is queued work the remaining idle runners cannot
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
        let _reservation = Reservation::take(building, wanted)?;
        match provision_slot(
            &provider,
            config,
            slot,
            next_generation,
            golden,
            keys,
            successor_environment,
        )
        .await
        {
            Ok(successor) => Some(successor),
            Err(error) => {
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
            idle.fetch_sub(1, Ordering::AcqRel);
            info!(machine = name.as_str(), environment = %environment.base, "replacing idle runner for queued environment");
            (provider.stop(name).await.map_err(OrchestratorError::from), None)
        },
        pair = async {
            // Concurrent on purpose: the successor is built while the job is
            // still running, which is the whole point of the busy signal.
            tokio::join!(&mut run_task, build_successor)
        } => {
            let result = match pair.0 {
                Ok(result) => result.map_err(OrchestratorError::from),
                Err(error) => Err(OrchestratorError::Pool(error.to_string())),
            };
            (result, pair.1)
        },
    };

    // The runner writes this marker only when the job it ran opted in via
    // `preserve_on_failure` and then genuinely failed, so preservation is
    // decided per run rather than by engine-wide configuration.
    let preserved = match &config.debug_dir {
        Some(debug_dir)
            if provider
                .exec(
                    name,
                    &["test".into(), "-f".into(), GUEST_FAILURE_MARKER.into()],
                )
                .await
                .is_ok() =>
        {
            Some(debug_dir.clone())
        }
        _ => None,
    };

    if let Some(debug_dir) = preserved {
        hold_for_debugging(name, &debug_dir, &shutdown).await;
        notify_runner_gone(config, name).await;
        if let Err(error) = provider.delete(name).await {
            warn!(machine = name.as_str(), %error, "failed to delete preserved machine");
        }
        return finish(&provider, config, result, successor).await;
    }

    // Report the runner's own failure in preference to a teardown failure.
    notify_runner_gone(config, name).await;
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

/// Run the guest runner to completion, signalling the first job it accepts.
///
/// Streaming rather than buffering the guest's output is what makes the busy
/// signal observable while the job is still running; it also drops SmolVM's
/// 30-second buffered-exec read timeout.
async fn run_until_exit<P: VmProvider + 'static>(
    provider: &Arc<P>,
    name: &MachineName,
    run: &[String],
    busy: tokio::sync::oneshot::Sender<()>,
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

    let code = provider.exec_stream(name, run, chunks).await?;
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
                if config.use_packed_artifact
                    && golden.as_str() == format!("{}-golden", config.name_prefix) =>
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
            Err(error)
                if config.use_packed_artifact
                    && golden.as_str() == format!("{}-golden", config.name_prefix) =>
            {
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
                            // them to drain, then retry the re-arm a bounded
                            // number of times before falling back to direct
                            // creation (whose socket mount cannot serve the
                            // control transport, so the fallback usually
                            // fails registration anyway).
                            let mut rearmed = false;
                            for attempt in 0..12 {
                                tokio::time::sleep(GOLDEN_DRAIN_PROBE_DELAY).await;
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
                                        // keep probing until the bounded
                                        // retries are exhausted.
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
                    // A failed fork can leave a partial clone behind.
                    // Best-effort cleanup makes the direct create safe; if
                    // cleanup itself is still racing SmolVM state, create
                    // returns the actionable error and the slot supervisor
                    // retries normally.
                    warn!(
                        machine = name.as_str(),
                        golden = golden.as_str(),
                        %error,
                        "packed golden fork failed; creating runner directly from packed artifact"
                    );
                    if let Err(cleanup) = provider.delete(name).await {
                        debug!(
                            machine = name.as_str(),
                            %cleanup,
                            "failed fork left no removable clone"
                        );
                    }
                    None
                }
            }
            Err(error) => return Err(error.into()),
        },
        None => None,
    };

    if let Some(golden) = forked_golden {
        // Fork from the already-booted golden VM instant CoW clone.
        // The PACKED golden carries its bake inside the artifact's flattened
        // rootfs, which forks inherit through the storage chain — so the apt
        // baseline is already there. Environment goldens are different:
        // `prepare_golden_for_env`
        // bakes via guest `exec`, and SmolVM's forkable snapshot does NOT
        // carry post-create exec writes into clones (verified empirically),
        // so an env-golden fork boots the bare stock base image. Install the
        // apt baseline and toolchains into the fork itself — it is the job's
        // single-use machine, so the writes persist for its lifetime.
        let golden_is_packed = config.use_packed_artifact
            && golden.as_str() == format!("{}-golden", config.name_prefix);
        if golden_is_packed {
            // The pack carries the apt baseline, but not necessarily apt's
            // indices — restore them before any workflow apt-installs. A
            // custom base is used as-is: no apt assumptions.
            if environment.curated {
                if let Err(error) = provider.exec(name, &apt_lists_refresh_command()).await {
                    warn!(
                    machine = name.as_str(),
                        %error, "apt list refresh failed; workflow apt installs may not resolve"
                    );
                }
            }
            // A pack is only as baked as whoever produced it: `prepare_artifact`
            // bakes the workspace toolchains, but `download_prebaked_golden`
            // short-circuits that path, and a published pack can predate (or
            // simply omit) the toolchain the workspace now asks for. Probing
            // beats assuming — a fork missing cargo runs the job anyway and
            // cargo-dist dies with "you don't appear to have cargo installed",
            // blaming the workflow for a broken machine. A fully baked pack
            // pays one `command -v` per layer.
            for layer in &environment.toolchains {
                if verify_toolchain_installed(provider.as_ref(), name, layer)
                    .await
                    .is_ok()
                {
                    continue;
                }
                warn!(
                    machine = name.as_str(),
                    toolchain = %layer,
                    "packed golden lacks toolchain; installing into the fork"
                );
                for command in layer.install_commands() {
                    if let Err(error) = provider.exec(name, &command).await {
                        return Err(error.into());
                    }
                }
                verify_toolchain_installed(provider.as_ref(), name, layer).await?;
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
        let uses_packed_artifact = direct_create_from_packed;
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
            volumes: runner_volumes(config, name, !uses_packed_artifact),
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
        "/var/lib/preloop-runner".into(),
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
                    guard.insert(token, std::time::SystemTime::now());
                    let now = std::time::SystemTime::now();
                    guard.retain(|_, at| {
                        now.duration_since(*at)
                            .map(|age| age < std::time::Duration::from_secs(600))
                            .unwrap_or(false)
                    });
                }
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
    provider
        .exec_with_secret_env(name, &as_runner_user(config, &configure), &secrets)
        .await?;
    drop(staged);
    if let Some(path) = provision_token_file {
        let _ = std::fs::remove_file(path);
    }

    // Bring the container engine up before the runner accepts work, so a job
    // declaring `container:` or `services:` does not race the daemon. Failure
    // is not fatal — only container jobs depend on it.
    if let Err(error) = provider.exec(name, &docker_start_command()).await {
        warn!(
            machine = name.as_str(),
            %error,
            "container engine did not start; `container:` and `services:` jobs will fail"
        );
    }

    info!(machine = name.as_str(), "ephemeral runner ready");
    let mut run = guest_env_prefix(config, name);
    run.extend([
        runner,
        "run".into(),
        "--once".into(),
        "--runner-root".into(),
        "/var/lib/preloop-runner".into(),
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
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD.encode(script);
    format!(
        "if [ \"$(id -u)\" -eq 0 ]; then {script}; else \
           printf %s '{b64}' | base64 -d | sudo -n sh 2>/dev/null || true; fi"
    )
}

fn as_runner_user(config: &RunnerPoolConfig, argv: &[String]) -> Vec<String> {
    let Some(user) = &config.runner_user else {
        return argv.to_vec();
    };
    if user == "root" {
        return argv.to_vec();
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
    let provisioning = format!(
        "getent passwd {user} >/dev/null 2>&1 || useradd -m -u {uid} {user} 2>/dev/null; \
         mkdir -p /run/user/{uid}; chown {uid}:{uid} /run/user/{uid} /var/lib/preloop-runner 2>/dev/null; \
         chmod 777 /run/preloop-control 2>/dev/null; \
         getent group docker >/dev/null 2>&1 && usermod -aG docker {user} 2>/dev/null"
    );
    // setpriv requires a groups mode: --init-groups (setgroups) only works
    // as root, so the exec-as-image-user branch (official golden: USER
    // runner, uid 1001) must use --keep-groups — the exec context already
    // carries the right supplementary groups, and reuid/regid to self are
    // permitted without privileges. The root branch keeps --init-groups.
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD.encode(&provisioning);
    let script = format!(
        "if [ \"$(id -u)\" -eq 0 ]; then \
           {provisioning}; \
           exec setpriv --reuid {uid} --regid {uid} --init-groups env \
             PRELOOP_RUNNER_USER={user} PRELOOP_RUNNER_UID={uid} HOME={home} {program} {args}; \
         else \
           printf %s '{b64}' | base64 -d | sudo -n sh 2>/dev/null || true; \
           exec setpriv --reuid {uid} --regid {uid} --keep-groups env \
             PRELOOP_RUNNER_USER={user} PRELOOP_RUNNER_UID={uid} HOME={home} {program} {args}; \
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
    let binary = layer.verify_binary();
    let mut command = vec!["sh".to_owned(), "-c".to_owned()];
    command.push(format!("command -v {binary}"));
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
/// while attached and removes it on exit, which releases the slot immediately
/// instead of stranding it until the idle deadline.
async fn hold_for_debugging(name: &MachineName, debug_dir: &Path, shutdown: &CancellationToken) {
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

    warn!(
        machine = name.as_str(),
        timeout_secs = DEBUG_IDLE_TIMEOUT.as_secs(),
        "job failed — VM preserved for debugging; attach with `preloop shell`"
    );

    let mut deadline = tokio::time::Instant::now() + DEBUG_IDLE_TIMEOUT;
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
            // `preloop shell` removed the marker: the session is over.
            info!(
                machine = name.as_str(),
                "debug session ended — deleting preserved VM"
            );
            break;
        };
        // Only a live `preloop shell` heartbeat extends the window. Matching on
        // mtime alone would let this function's own initial write renew it.
        if state.trim() == DEBUG_MARKER_ACTIVE
            && std::fs::metadata(&marker)
                .and_then(|meta| meta.modified())
                .ok()
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age < DEBUG_HEARTBEAT_WINDOW)
        {
            deadline = tokio::time::Instant::now() + DEBUG_IDLE_TIMEOUT;
        }
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
    use preloop_vm::{ExecOutput, OutputChunk};
    use std::collections::HashMap;
    use std::os::unix::fs::PermissionsExt as _;
    use std::process::Command;
    use tokio::sync::Mutex;

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
        /// Report live clones to `rearm_fork_base`; true by default so a spent
        /// base with dependents is never re-armed in tests either.
        live_forks: Mutex<bool>,
        fail_start: bool,
        fail_install: bool,
        fail_configure: bool,
        fail_run: bool,
        fail_delete: bool,
        announce_busy: bool,
        /// Binary that `command -v` cannot find until its toolchain installs.
        absent_binary: Mutex<Option<&'static str>>,
        /// Guest pause marker state: when set, the exec probe for the debug
        /// pause marker succeeds, so `watch_guest_pause` sees a paused job.
        pause_marker: std::sync::atomic::AtomicBool,
        /// When set, the pause-marker probe fails like a wedged VM
        /// (transport error), which the watcher must not read as "resumed".
        probe_transport_error: std::sync::atomic::AtomicBool,
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
                live_forks: Mutex::new(true),
                fail_start,
                fail_install,
                fail_configure,
                fail_run,
                fail_delete,
                announce_busy: false,
                absent_binary: Mutex::new(None),
                pause_marker: std::sync::atomic::AtomicBool::new(false),
                probe_transport_error: std::sync::atomic::AtomicBool::new(false),
            }
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

        /// Report whether clones of the golden still exist.
        fn with_live_forks(mut self, live: bool) -> Self {
            *self.live_forks.get_mut() = live;
            self
        }

        /// A provider whose guests lack `binary` until an install command for
        /// it runs — a pack baked without the workspace's toolchain.
        fn without_binary(binary: &'static str) -> Self {
            Self {
                absent_binary: Mutex::new(Some(binary)),
                ..Self::new(false, false, false, false, false)
            }
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

    fn test_output() -> ExecOutput {
        ExecOutput {
            exit_code: 0,
            stdout: Vec::new(),
            stderr: Vec::new(),
            truncated: false,
        }
    }

    #[test]
    fn node_external_archives_are_piped_into_tar() {
        let temp = tempfile::tempdir().unwrap();
        let bin = temp.path().join("bin");
        let root = temp.path().join("runner");
        std::fs::create_dir_all(&bin).unwrap();

        let curl = bin.join("curl");
        std::fs::write(&curl, "#!/bin/sh\nprintf archive\n").unwrap();
        std::fs::set_permissions(&curl, std::fs::Permissions::from_mode(0o755)).unwrap();

        let tar = bin.join("tar");
        std::fs::write(
            &tar,
            r#"#!/bin/sh
input=$(cat)
[ "$input" = archive ] || exit 41
while [ "$#" -gt 0 ]; do
  if [ "$1" = -C ]; then
    shift
    destination=$1
  fi
  shift
done
mkdir -p "$destination/bin"
printf '#!/bin/sh\nexit 0\n' > "$destination/bin/node"
chmod +x "$destination/bin/node"
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
        std::fs::write(&curl, "#!/bin/sh\nprintf archive\n").unwrap();
        std::fs::set_permissions(&curl, std::fs::Permissions::from_mode(0o755)).unwrap();

        let tar = bin.join("tar");
        std::fs::write(
            &tar,
            r#"#!/bin/sh
input=$(cat)
[ "$input" = archive ] || exit 41
while [ "$#" -gt 0 ]; do
  if [ "$1" = -C ]; then
    shift
    destination=$1
  fi
  shift
done
mkdir -p "$destination/bin"
printf '#!/bin/sh\nexit 0\n' > "$destination/bin/node"
chmod +x "$destination/bin/node"
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
            entries.contains(&"/root/.cargo/bin"),
            "cargo-installed binaries must be reachable: {path}"
        );
        assert!(
            entries.contains(&"/usr/local/go/bin"),
            "the go layer untars into /usr/local/go: {path}"
        );
        assert!(
            entries.contains(&"/usr/local/bin") && entries.contains(&"/usr/bin"),
            "the system PATH must survive: {path}"
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
        // Root branch (locally baked goldens) drops with --init-groups; the
        // exec-as-image-user branch (official golden) provisions via sudo and
        // self-drops with --keep-groups (setgroups needs root).
        assert!(
            script.contains("setpriv --reuid 1001 --regid 1001 --init-groups"),
            "{script}"
        );
        assert!(
            script.contains("setpriv --reuid 1001 --regid 1001 --keep-groups"),
            "{script}"
        );
        assert!(
            script.contains("| base64 -d | sudo -n sh 2>/dev/null || true"),
            "{script}"
        );
        assert_eq!(
            script
                .matches("'/opt/preloop/bin/preloop-runner' 'run' '--once'")
                .count(),
            2,
            "the wrapped program must appear in both branches"
        );
        assert!(
            script.contains("PRELOOP_RUNNER_USER=runner PRELOOP_RUNNER_UID=1001"),
            "{script}"
        );
    }

    #[test]
    fn runner_user_wrapper_passes_root_and_unset_through() {
        let mut config = test_config(false);
        let argv = vec![
            "/opt/preloop/bin/preloop-runner".to_owned(),
            "run".to_owned(),
        ];
        // Unset: no switching.
        assert_eq!(as_runner_user(&config, &argv), argv);
        // Explicit root: no switching.
        config.runner_user = Some("root".to_owned());
        assert_eq!(as_runner_user(&config, &argv), argv);
    }

    #[test]
    fn shell_quote_escapes_single_quotes() {
        assert_eq!(shell_quote("plain"), "'plain'");
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
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
    fn default_oci_golden_reference_targets_arm64_pack() {
        let (registry, repository, version) =
            split_oci_reference(DEFAULT_GOLDEN_OCI_REF).expect("valid OCI reference");
        assert_eq!(registry, "ghcr.io");
        assert_eq!(repository, "preloopdev/preloop-golden");
        // Immutable digest pin: changing the default must be a reviewed code
        // change, not a registry retag.
        assert!(
            version.len() == "sha256:".len() + 64 && version.starts_with("sha256:"),
            "expected a digest-pinned default, got `{version}`"
        );
    }

    #[test]
    fn oci_layer_deserializes_camel_case_media_type() {
        let manifest: OciManifest = serde_json::from_str(
            r#"{"layers":[{"digest":"sha256:00","mediaType":"application/vnd.preloop.smolmachine.v1+zstd"}]}"#,
        )
        .expect("standard OCI manifest must parse");
        let layer = manifest
            .layers
            .into_iter()
            .find(|layer| layer.media_type == "application/vnd.preloop.smolmachine.v1+zstd")
            .expect("packed VM layer present");
        assert_eq!(layer.digest, "sha256:00");
    }

    #[test]
    fn oci_auth_challenge_parameters_parse() {
        let challenge = r#"Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:preloopdev/preloop-golden:pull""#;
        assert_eq!(
            auth_parameter(challenge, "realm").as_deref(),
            Some("https://ghcr.io/token")
        );
        assert_eq!(
            auth_parameter(challenge, "scope").as_deref(),
            Some("repository:preloopdev/preloop-golden:pull")
        );
    }

    #[test]
    fn concurrent_on_demand_provisioning_keeps_preparing_signal_raised() {
        let active = Arc::new(AtomicUsize::new(0));
        let signal = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let first = PreparingGuard::enter(active.clone(), Some(signal.clone()));
        let second = PreparingGuard::enter(active.clone(), Some(signal.clone()));
        assert!(signal.load(Ordering::Acquire));

        drop(first);
        assert!(
            signal.load(Ordering::Acquire),
            "one completed provision must not expose the other to starvation"
        );

        drop(second);
        assert!(!signal.load(Ordering::Acquire));
    }

    fn test_config(control_socket: bool) -> RunnerPoolConfig {
        RunnerPoolConfig {
            size: 1,
            use_fork: false,
            use_packed_artifact: false,
            name_prefix: "lifecycle-test".to_owned(),
            base_image: "base-image".to_owned(),
            workspace: None,
            artifact_stem: PathBuf::from("/tmp/lifecycle-artifact"),
            release_version: "9.9.9".to_owned(),
            runner_bundle: PathBuf::from("/tmp"),
            externals_dir: PathBuf::from("/tmp/lifecycle-externals"),
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
            debug_dir: None,
            runner_key_dir: None,
            pending_jobs: None,
            preload_images: Vec::new(),
            runner_user: None,
            runner_uid: None,
            next_job_runs_on: None,
            pending_registrations: None,
            preparing_signal: None,
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
            let mut absent = self.absent_binary.lock().await;
            if let Some(binary) = *absent {
                let probe = format!("command -v {binary}");
                if argv.contains(&probe) {
                    return Err(test_error("binary-not-found"));
                }
                // An install command that names the binary lands it on PATH.
                if argv.iter().any(|arg| arg.contains(binary)) {
                    *absent = None;
                }
            }
            drop(absent);
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
            if self.announce_busy {
                output
                    .send(OutputChunk::Stdout(
                        format!("{RUNNER_BUSY_SENTINEL}\n").into_bytes(),
                    ))
                    .await
                    .unwrap();
            }
            if self.fail_run && argv.iter().any(|arg| arg == "run") {
                return Err(test_error("run-failure"));
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
        assert!(events[create + 1..]
            .iter()
            .any(|event| event == &format!("delete:{}", name.as_str())));
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

    /// A published pack can be older than the workspace's toolchain pin, so a
    /// fork of the packed golden must not be trusted to already have cargo:
    /// cargo-dist's plan job installs no toolchain of its own and fails with
    /// "you don't appear to have cargo installed" on a bare machine.
    #[tokio::test]
    async fn packed_golden_fork_installs_a_toolchain_the_pack_lacks() {
        let provider = Arc::new(TestProvider::without_binary("cargo"));
        let config = packed_fork_config();
        let golden = MachineName::new("lifecycle-test-golden").unwrap();
        let name = MachineName::new("lifecycle-test-0-1").unwrap();

        provision_runner(
            &provider,
            &config,
            &name,
            Some(&golden),
            &Arc::new(KeyPool::new()),
            &test_runner_environment(
                config.base_image.clone(),
                vec![ToolchainLayer::Rust("1.97".to_owned())],
                true,
            ),
        )
        .await
        .expect("the fork installs the toolchain its pack lacks");

        let events = provider.events().await;
        assert!(
            events
                .iter()
                .any(|event| event.contains("command -v cargo")),
            "the fork must probe for the toolchain: {events:?}"
        );
        assert!(
            events.iter().any(|event| event.contains("rustup-init")),
            "a probe miss must install the toolchain: {events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|event| event.contains("--no-install-recommends")),
            "the pack already carries the apt baseline: {events:?}"
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

    /// The probe is the whole cost on a pack that is already baked: no reinstall.
    #[tokio::test]
    async fn packed_golden_fork_keeps_a_baked_toolchain() {
        let provider = Arc::new(TestProvider::new(false, false, false, false, false));
        let config = packed_fork_config();
        let golden = MachineName::new("lifecycle-test-golden").unwrap();
        let name = MachineName::new("lifecycle-test-0-2").unwrap();

        provision_runner(
            &provider,
            &config,
            &name,
            Some(&golden),
            &Arc::new(KeyPool::new()),
            &test_runner_environment(
                config.base_image.clone(),
                vec![ToolchainLayer::Rust("1.97".to_owned())],
                true,
            ),
        )
        .await
        .expect("provisioning succeeds");

        let events = provider.events().await;
        assert!(
            events
                .iter()
                .any(|event| event.contains("command -v cargo")),
            "the fork must probe for the toolchain: {events:?}"
        );
        assert!(
            !events.iter().any(|event| event.contains("rustup-init")),
            "a baked toolchain must not be reinstalled: {events:?}"
        );
    }

    /// The golden registry dies with the process, so a restart would rebake a
    /// golden that is still running and still correct. The host-side record is
    /// what makes adoption safe: it must match the requested fingerprint, and
    /// the machine must actually be up to serve as a fork base.
    #[tokio::test]
    async fn golden_is_adopted_only_when_running_and_recorded() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = test_config(false);
        config.artifact_stem = temp.path().join("runner-image");
        let provider = Arc::new(TestProvider::new(false, false, false, false, false));
        let golden = MachineName::new("lifecycle-test-golden-abc123").unwrap();

        // Recorded, but the machine was never started.
        write_golden_record(&config, &golden, "fp-1");
        assert!(!golden_is_reusable(&provider, &config, &golden, "fp-1").await);

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

        assert!(golden_is_reusable(&provider, &config, &golden, "fp-1").await);
        // A base-image or toolchain bump changes the fingerprint: rebake.
        assert!(!golden_is_reusable(&provider, &config, &golden, "fp-2").await);

        remove_golden_record(&config, &golden);
        assert!(!golden_is_reusable(&provider, &config, &golden, "fp-1").await);
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
        )
        .await
        .expect("provisioning succeeds");
        let idle = AtomicUsize::new(0);
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
                prebuild_successor: true,
            },
        )
        .await
        .expect_err("runner failure must propagate");
        assert!(error.to_string().contains("run-failure"));
        assert!(!error.to_string().contains("delete-failure"));
    }

    #[tokio::test]
    async fn on_demand_slot_does_not_build_a_throwaway_successor() {
        let provider =
            Arc::new(TestProvider::new(false, false, false, false, false).announcing_busy());
        let mut config = test_config(false);
        config.size = 0;
        let handles = PoolHandles {
            idle: Arc::new(AtomicUsize::new(0)),
            keys: Arc::new(KeyPool::new()),
            building: Arc::new(AtomicUsize::new(0)),
        };

        run_on_demand_slot(
            provider.clone(),
            config.clone(),
            0,
            CancellationToken::new(),
            Arc::new(GoldenRegistry::new(config.name_prefix.clone())),
            handles,
            Arc::new(AtomicUsize::new(0)),
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
        std::env::set_var("PRELOOP_GOLDEN_URL", &url);

        let downloaded = download_prebaked_golden(&payload, "9.9.9").await;

        std::env::remove_var("PRELOOP_GOLDEN_URL");
        assert!(downloaded);
        assert_eq!(std::fs::read(&payload).unwrap(), body);
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
        std::env::set_var("PRELOOP_GOLDEN_URL", &url);

        let downloaded = download_prebaked_golden(&payload, "9.9.9").await;

        std::env::remove_var("PRELOOP_GOLDEN_URL");
        // The caller reads `false` as "build the golden locally", so a partial
        // file surviving here would be booted as if it were complete.
        assert!(!downloaded);
        assert!(!payload.exists());
        assert!(leftovers(directory.path()).is_empty());
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
        std::env::set_var("PRELOOP_GOLDEN_URL", &url);

        let downloaded = download_prebaked_golden(&payload, "9.9.9").await;

        std::env::remove_var("PRELOOP_GOLDEN_URL");
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
        std::env::set_var("PRELOOP_GOLDEN_URL", &url);

        let downloaded = download_prebaked_golden(&payload, "9.9.9").await;

        std::env::remove_var("PRELOOP_GOLDEN_URL");
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
