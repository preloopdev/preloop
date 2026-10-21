//! The runner pool's view of machines, served by the Sandbox Service.
//!
//! The pool keeps its machine-level vocabulary (create, start, fork, exec,
//! copy, pack) and this adapter maps each call onto the `Sandboxes` API, so
//! every VM the CI pool uses is a sandbox: placed on whichever provider can
//! serve it (the in-process SmolVM agent, or a remote host agent), leased,
//! reconciled and cleaned up by the service. The pool never talks to a
//! substrate directly.
//!
//! Mapping:
//! * `create` records the spec; `start`/`start_forkable` acquire a
//!   snapshot-capable sandbox from an image template, since the pool starts
//!   a golden plainly, provisions it, and only then restarts it forkable.
//! * `fork(golden, clone)` snapshots the golden once into a template, then
//!   acquires the clone from it. Snapshot templates are re-forkable, so
//!   `rearm_fork_base` has nothing to repair.
//! * `pack` and data-directory hygiene need host files; they are served only
//!   when the sandbox runs on this host's SmolVM (`local`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt as _;
use preloop_sandbox::api::{
    AcquireRequest, Artifact, ExecRequest, Mount, NetworkMode, NetworkPolicy as SbxNetwork,
    Purpose, ReleaseMode, ReleaseRequest, RenewRequest, Requirements, SandboxState, Shape,
    SnapshotRequest, SocketMount, TemplateSpec, host_arch,
};
use preloop_sandbox::drivers::smolvm::cli::{
    ExecOutput, MachineName, MachineRole, MachineSpec, MachineState, NetworkPolicy, OutputChunk,
    ProviderCapabilities, SecretSource, SmolVmProvider, VmError, VmProvider,
};
use preloop_sandbox::{Code, SandboxApi};
use sha2::{Digest as _, Sha256};
use tokio::sync::{Mutex, mpsc};
use tracing::{debug, warn};

/// Lease TTL for pool machines; renewed well before expiry.
const LEASE_TTL: Duration = Duration::from_secs(6 * 3600);
const RENEW_EVERY: Duration = Duration::from_secs(15 * 60);
/// Bound on captured exec output, matching the SmolVM provider default.
const CAPTURE_LIMIT: usize = 1024 * 1024;

#[derive(Debug, Clone)]
enum Entry {
    /// Created, not started.
    Defined(MachineSpec),
    /// A leased sandbox.
    Live {
        sandbox_id: String,
        /// Provider that owns it (host-local operations must match).
        provider_id: String,
        lease_id: String,
        native_ref: String,
        spec: MachineSpec,
        forkable: bool,
        /// Last state the pool observed (stop/start move it without a lease).
        state: SandboxState,
    },
    /// A golden captured as a snapshot template.
    Golden { template: String, spec: MachineSpec },
}

/// [`VmProvider`] over a Sandbox Service.
pub struct SandboxMachines {
    api: Arc<dyn SandboxApi>,
    local: Option<Arc<SmolVmProvider>>,
    /// Provider id of this process's host agent, when one is attached: only
    /// machines it owns may be touched through the local CLI.
    local_provider_id: Option<String>,
    requires: Requirements,
    capabilities: ProviderCapabilities,
    entries: Mutex<HashMap<String, Entry>>,
    /// Templates registered per image string, keyed by the content identity
    /// that produced them. A rebuilt local pack keeps its path but changes
    /// length or mtime, so it is hashed again and gets a new template.
    images: Mutex<HashMap<String, CachedImage>>,
    /// Serializes turning a golden into a snapshot template: concurrent
    /// first forks must snapshot once, not race two captures of one base.
    snapshot_lock: Mutex<()>,
    generation: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Clone)]
struct CachedImage {
    /// What the image looked like when the template was registered.
    fingerprint: String,
    template: TemplateSpec,
}

/// Cheap identity of an image string, used to skip re-hashing or re-resolving.
///
/// A local file is identified by path, length and mtime; a digest-pinned
/// registry reference never changes; a tag is mutable and always resolved.
fn image_fingerprint(image: &str) -> Option<String> {
    let raw = image.strip_prefix("file://").unwrap_or(image);
    let path = Path::new(raw);
    if path.is_file() {
        let metadata = std::fs::metadata(path).ok()?;
        let modified = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|age| age.as_nanos())
            .unwrap_or(0);
        return Some(format!(
            "file:{}:{}:{modified}",
            path.display(),
            metadata.len()
        ));
    }
    image
        .contains("@sha256:")
        .then(|| format!("pinned:{image}"))
}

/// The template a machine image string describes.
async fn image_template(image: &str) -> Result<TemplateSpec, VmError> {
    let artifacts = image_artifacts(image).await?;
    Ok(TemplateSpec {
        name: image_template_name(&artifacts),
        os: "linux".into(),
        artifacts,
        ..TemplateSpec::default()
    })
}

/// Artifacts for `image`: a local pack, or a registry image resolved to a
/// digest.
async fn image_artifacts(image: &str) -> Result<Vec<Artifact>, VmError> {
    if let Some(path) = image.strip_prefix("file://") {
        let path = Path::new(path);
        if !path.is_file() {
            return Err(VmError::InvalidSpec(format!(
                "pack {} does not exist",
                path.display()
            )));
        }
        return Ok(vec![local_pack_artifact(path).await?]);
    }
    let path = Path::new(image);
    if path.is_file() {
        return Ok(vec![local_pack_artifact(path).await?]);
    }
    oci_artifacts(image).await
}

/// A host-local image input the SmolVM CLI boots with `--image`: a Docker-save
/// `.tar` archive or a bare rootfs directory.
///
/// These are not content-addressed packs, so they stay a driver recipe (as
/// they always were) rather than an artifact; a registry image or a
/// `.smolmachine` pack is the artifact path.
fn local_image_recipe(image: &str, driver: &str) -> Option<TemplateSpec> {
    let raw = image.strip_prefix("file://").unwrap_or(image);
    let path = Path::new(raw);
    let archive = path.is_file() && raw.ends_with(".tar");
    if !(path.is_dir() || archive) {
        return None;
    }
    let path = std::fs::canonicalize(path).ok()?;
    let digest = hex::encode(Sha256::digest(path.display().to_string().as_bytes()));
    Some(TemplateSpec {
        name: format!("image-{}", &digest[..16]),
        os: "linux".into(),
        recipe: Some(format!(
            "{}{}",
            preloop_sandbox::drivers::smolvm::IMAGE_RECIPE,
            path.display()
        )),
        requires: Requirements {
            driver: driver.to_owned(),
            ..Requirements::default()
        },
        ..TemplateSpec::default()
    })
}

/// A host-local `.smolmachine` pack, identified by its content.
async fn local_pack_artifact(path: &Path) -> Result<Artifact, VmError> {
    let path = std::fs::canonicalize(path).map_err(|error| {
        VmError::InvalidSpec(format!("resolving pack {}: {error}", path.display()))
    })?;
    let digest = crate::sha256_file(&path)
        .await
        .map_err(VmError::InvalidSpec)?;
    let size_bytes = std::fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);
    Ok(Artifact {
        format: Artifact::SMOLMACHINE.into(),
        arch: host_arch().into(),
        reference: format!("file://{}", path.display()),
        digest: format!("sha256:{digest}"),
        size_bytes,
    })
}

/// A registry image (or packed OCI artifact) resolved to the digest the
/// registry serves right now.
///
/// `smolvm pack push` publishes a packed machine as an OCI index whose single
/// image manifest carries the packed layer; a bootable image has none. Either
/// way the artifact records the layer content digest (`smolmachine`) or the
/// pinned image digest (`oci-image`).
async fn oci_artifacts(image: &str) -> Result<Vec<Artifact>, VmError> {
    let reference = crate::oci::OciReference::parse(image)
        .map_err(|error| VmError::InvalidSpec(format!("image `{image}`: {error}")))?;
    let client = registry_client()?;
    let (mut manifest_digest, mut manifest) =
        fetch_manifest(&client, &reference, &reference.reference, image).await?;
    let layers_empty = manifest
        .get("layers")
        .and_then(serde_json::Value::as_array)
        .is_none_or(|layers| layers.is_empty());
    let selected = layers_empty
        .then(|| {
            manifest
                .get("manifests")
                .and_then(serde_json::Value::as_array)
        })
        .flatten()
        .and_then(|entries| {
            // One index level: our packs publish one platform entry, and a
            // multi-platform image names an image manifest per platform.
            entries
                .iter()
                .find(|entry| {
                    entry
                        .get("digest")
                        .and_then(serde_json::Value::as_str)
                        .is_some()
                        && entry_platform_matches_host(entry)
                })
                .or_else(|| {
                    entries.iter().find(|entry| {
                        entry
                            .get("digest")
                            .and_then(serde_json::Value::as_str)
                            .is_some()
                    })
                })
                .and_then(|entry| entry.get("digest").and_then(serde_json::Value::as_str))
                .map(str::to_owned)
        });
    if let Some(digest) = selected {
        (manifest_digest, manifest) = fetch_manifest(&client, &reference, &digest, image).await?;
    }
    let pinned = format!(
        "{}/{}@{}",
        reference.registry, reference.repository, manifest_digest
    );
    if let Some(layer) = manifest
        .get("layers")
        .and_then(serde_json::Value::as_array)
        .and_then(|layers| {
            layers.iter().find(|layer| {
                layer
                    .get("mediaType")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(crate::oci::is_packed_vm_layer)
            })
        })
    {
        let digest = layer
            .get("digest")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                VmError::InvalidSpec(format!("image `{image}` packed layer has no digest"))
            })?;
        return Ok(vec![Artifact {
            format: Artifact::SMOLMACHINE.into(),
            arch: host_arch().into(),
            reference: pinned,
            digest: digest.to_owned(),
            size_bytes: layer
                .get("size")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
        }]);
    }
    Ok(vec![Artifact {
        format: Artifact::OCI_IMAGE.into(),
        arch: host_arch().into(),
        reference: pinned,
        digest: manifest_digest,
        size_bytes: 0,
    }])
}

/// Fetch a manifest by tag or digest, returning the digest the registry
/// served and the parsed body.
async fn fetch_manifest(
    client: &reqwest::Client,
    reference: &crate::oci::OciReference,
    at: &str,
    image: &str,
) -> Result<(String, serde_json::Value), VmError> {
    let url = reference.manifest_url(at);
    let response = crate::oci::registry_get(
        client,
        client,
        &url,
        &reference.repository,
        crate::oci::MANIFEST_ACCEPT,
        None,
        None,
        None,
    )
    .await
    .map_err(|error| VmError::InvalidSpec(format!("resolving image `{image}`: {error}")))?;
    let served = response
        .headers()
        .get("docker-content-digest")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = response.bytes().await.map_err(|error| {
        VmError::InvalidSpec(format!("reading image `{image}` manifest: {error}"))
    })?;
    let digest = served.unwrap_or_else(|| format!("sha256:{}", hex::encode(Sha256::digest(&body))));
    if !digest.starts_with("sha256:") {
        return Err(VmError::InvalidSpec(format!(
            "image `{image}` manifest digest `{digest}` is not sha256"
        )));
    }
    let manifest = serde_json::from_slice(&body).map_err(|error| {
        VmError::InvalidSpec(format!("image `{image}` manifest is not JSON: {error}"))
    })?;
    Ok((digest, manifest))
}

fn entry_platform_matches_host(entry: &serde_json::Value) -> bool {
    let platform = entry.get("platform");
    let os = platform
        .and_then(|platform| platform.get("os"))
        .and_then(serde_json::Value::as_str);
    let arch = platform
        .and_then(|platform| platform.get("architecture"))
        .and_then(serde_json::Value::as_str);
    os == Some("linux") && arch.is_some_and(|arch| crate::oci::host_arch_aliases().contains(&arch))
}

fn registry_client() -> Result<reqwest::Client, VmError> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .user_agent(concat!("preloop-orchestrator/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|error| VmError::InvalidSpec(format!("building the registry client: {error}")))
}

/// Template name derived from the artifact content: a rebuilt pack yields a
/// new digest, and therefore a new template.
fn image_template_name(artifacts: &[Artifact]) -> String {
    let mut canonical = String::new();
    for artifact in artifacts {
        canonical.push_str(&artifact.format);
        canonical.push('\u{0}');
        canonical.push_str(&artifact.arch);
        canonical.push('\u{0}');
        canonical.push_str(&artifact.digest);
        canonical.push('\u{0}');
        canonical.push_str(&artifact.reference);
        canonical.push('\n');
    }
    let digest = hex::encode(Sha256::digest(canonical.as_bytes()));
    format!("image-{}", &digest[..16])
}

impl SandboxMachines {
    /// `local` is this host's SmolVM CLI, used only for host-file operations
    /// (`pack`, pack-intermediate and failed-unpack pruning, orphan data-dir
    /// sweeps) on sandboxes the local SmolVM driver serves.
    /// `requires` pins placement (typically the pool's driver);
    /// `capabilities` tells the pool what that backend can express.
    /// `local_provider_id` is the id this process's agent registered under
    /// (see [`Agent::register`]); `None` means no local CLI backend.
    pub fn new(
        api: Arc<dyn SandboxApi>,
        local: Option<Arc<SmolVmProvider>>,
        local_provider_id: Option<String>,
        requires: Requirements,
        capabilities: ProviderCapabilities,
    ) -> Arc<Self> {
        let machines = Arc::new(Self {
            api,
            local,
            local_provider_id,
            requires,
            capabilities,
            entries: Mutex::new(HashMap::new()),
            images: Mutex::new(HashMap::new()),
            snapshot_lock: Mutex::new(()),
            generation: std::sync::atomic::AtomicU64::new(0),
        });
        let weak = Arc::downgrade(&machines);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(RENEW_EVERY).await;
                let Some(machines) = weak.upgrade() else {
                    return;
                };
                machines.renew_all().await;
            }
        });
        machines
    }

    async fn renew_all(&self) {
        let leases: Vec<String> = self
            .entries
            .lock()
            .await
            .values()
            .filter_map(|entry| match entry {
                Entry::Live { lease_id, .. } => Some(lease_id.clone()),
                _ => None,
            })
            .collect();
        for lease_id in leases {
            if let Err(error) = self
                .api
                .renew(RenewRequest {
                    lease_id: lease_id.clone(),
                    ttl_secs: LEASE_TTL.as_secs(),
                })
                .await
            {
                warn!(%lease_id, %error, "pool lease renewal failed");
            }
        }
    }

    async fn live(&self, name: &MachineName) -> Result<(String, String), VmError> {
        let entry = { self.entries.lock().await.get(name.as_str()).cloned() };
        match entry {
            Some(Entry::Live {
                sandbox_id,
                native_ref,
                ..
            }) => Ok((sandbox_id.clone(), native_ref.clone())),
            Some(Entry::Golden { .. }) => Err(VmError::InvalidSpec(format!(
                "{} is a fork base; it cannot run commands",
                name.as_str()
            ))),
            // A machine this process never created (an engine restart, or a
            // name the pool is adopting): the service is authoritative.
            _ => match self.remote_by_name(name.as_str()).await.into_iter().next() {
                Some(sandbox) if !sandbox.state.is_terminal() => {
                    Ok((sandbox.id, sandbox.native_ref))
                }
                _ => Err(sandbox_error("exec", missing(name))),
            },
        }
    }

    /// The template for `image`, registered by content digest.
    ///
    /// A local pack is hashed, so rebuilding it registers a new template whose
    /// name derives from the new digest; a registry tag is resolved to the
    /// digest the registry actually serves before it is registered.
    async fn ensure_image_template(&self, image: &str) -> Result<String, VmError> {
        let fingerprint = image_fingerprint(image);
        if let Some(fingerprint) = &fingerprint {
            let cached = self.images.lock().await.get(image).cloned();
            if let Some(cached) = cached.filter(|cached| &cached.fingerprint == fingerprint) {
                self.api
                    .put_template(cached.template.clone())
                    .await
                    .map_err(|error| sandbox_error("template", error))?;
                return Ok(cached.template.name);
            }
        }
        let template = match local_image_recipe(image, &self.requires.driver) {
            Some(template) => template,
            None => image_template(image).await?,
        };
        self.api
            .put_template(template.clone())
            .await
            .map_err(|error| sandbox_error("template", error))?;
        if let Some(fingerprint) = fingerprint {
            self.images.lock().await.insert(
                image.to_owned(),
                CachedImage {
                    fingerprint,
                    template: template.clone(),
                },
            );
        }
        Ok(template.name)
    }

    async fn acquire(
        &self,
        name: &MachineName,
        template: String,
        spec: &MachineSpec,
        purpose: Purpose,
    ) -> Result<Entry, VmError> {
        let generation = self
            .generation
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let mut options = std::collections::BTreeMap::new();
        if let Some(dns) = &spec.dns {
            options.insert("dns".to_owned(), dns.clone());
        }
        if spec.rosetta {
            options.insert("rosetta".to_owned(), "true".to_owned());
        }
        if let Some(overlay) = spec.overlay_gib {
            options.insert("overlay_gib".to_owned(), overlay.to_string());
        }
        // The pool states each machine's role, and an explicit guest-volume or
        // workload override; the SmolVM driver decodes these the same way.
        if spec.role != MachineRole::default() {
            options.insert("role".to_owned(), json_option(&spec.role)?);
        }
        if let Some(volumes) = &spec.guest_volumes {
            options.insert("guest_volumes".to_owned(), json_option(volumes)?);
        }
        if let Some(init) = &spec.workload_init {
            options.insert("workload_init".to_owned(), json_option(init)?);
        }
        let response = self
            .api
            .acquire(AcquireRequest {
                idempotency_key: format!(
                    "pool/{}/{}/{generation}",
                    name.as_str(),
                    std::process::id()
                ),
                name: name.as_str().to_owned(),
                template,
                shape: Shape {
                    cpus: u32::from(spec.cpus),
                    memory_mib: spec.memory_mib,
                    disk_gib: spec.storage_gib,
                },
                purpose,
                requires: self.requires.clone(),
                ttl_secs: LEASE_TTL.as_secs(),
                network: network(&spec.network),
                mounts: spec
                    .volumes
                    .iter()
                    .map(|v| Mount {
                        host: v.host.display().to_string(),
                        guest: v.guest.display().to_string(),
                        readonly: v.read_only,
                    })
                    .collect(),
                sockets: spec
                    .sockets
                    .iter()
                    .map(|s| SocketMount {
                        host: s.host.display().to_string(),
                        guest: s.guest.display().to_string(),
                    })
                    .collect(),
                options,
                ..AcquireRequest::default()
            })
            .await
            .map_err(|e| sandbox_error("start", e))?;
        Ok(Entry::Live {
            sandbox_id: response.sandbox.id,
            provider_id: response.sandbox.provider_id,
            lease_id: response.lease.id,
            native_ref: response.sandbox.native_ref,
            spec: spec.clone(),
            forkable: purpose == Purpose::Build,
            state: response.sandbox.state,
        })
    }

    async fn start_as(&self, name: &MachineName, purpose: Purpose) -> Result<(), VmError> {
        // Clone the entry out first: no `entries` guard may live across the
        // awaits below (stop/start/acquire all take it).
        let entry = { self.entries.lock().await.get(name.as_str()).cloned() };
        let spec = match entry {
            Some(Entry::Defined(spec)) => spec,
            // Stopped after a pack or an idle wind-down: boot it again, as a
            // branch base when the caller is provisioning a golden.
            Some(Entry::Live {
                sandbox_id,
                state: SandboxState::Stopped,
                ..
            }) => {
                let forkable = purpose == Purpose::Build;
                self.api
                    .start(&sandbox_id, forkable)
                    .await
                    .map_err(|e| sandbox_error("start", e))?;
                if let Some(Entry::Live {
                    state,
                    forkable: flag,
                    ..
                }) = self.entries.lock().await.get_mut(name.as_str())
                {
                    *state = SandboxState::Leased;
                    *flag = forkable;
                }
                return Ok(());
            }
            // Started for a job, now needed as a fork base: a branch base
            // must be booted forkable, so cycle it (this is what the pool
            // does explicitly for goldens).
            Some(Entry::Live {
                sandbox_id,
                forkable: false,
                ..
            }) if purpose == Purpose::Build => {
                self.api
                    .stop(&sandbox_id)
                    .await
                    .map_err(|e| sandbox_error("start", e))?;
                self.api
                    .start(&sandbox_id, true)
                    .await
                    .map_err(|e| sandbox_error("start", e))?;
                if let Some(Entry::Live { forkable, .. }) =
                    self.entries.lock().await.get_mut(name.as_str())
                {
                    *forkable = true;
                }
                return Ok(());
            }
            Some(Entry::Live { .. } | Entry::Golden { .. }) => return Ok(()),
            // Adopted from an earlier engine this process never created: the
            // service is authoritative, so start it if it is stopped.
            None => {
                let Some(sandbox) = self.remote_by_name(name.as_str()).await.into_iter().next()
                else {
                    return Err(sandbox_error("start", missing(name)));
                };
                match sandbox.state {
                    SandboxState::Stopped => {
                        self.api
                            .start(&sandbox.id, purpose == Purpose::Build)
                            .await
                            .map_err(|e| sandbox_error("start", e))?;
                    }
                    state if !state.is_terminal() => {}
                    _ => return Err(sandbox_error("start", missing(name))),
                }
                return Ok(());
            }
        };
        let template = self.ensure_image_template(&spec.image).await?;
        let entry = self.acquire(name, template, &spec, purpose).await?;
        self.entries
            .lock()
            .await
            .insert(name.as_str().to_owned(), entry);
        Ok(())
    }

    /// Sandboxes the service knows by this machine name, including ones a
    /// previous engine process left behind.
    async fn remote_by_name(&self, name: &str) -> Vec<preloop_sandbox::api::Sandbox> {
        match self.api.list().await {
            Ok(sandboxes) => sandboxes
                .into_iter()
                .filter(|s| s.name == name && s.state != SandboxState::Gone)
                .collect(),
            Err(error) => {
                debug!(%error, "sandbox list failed");
                Vec::new()
            }
        }
    }

    /// Recover a snapshot-backed golden that was persisted by the service.
    ///
    /// Snapshotting consumes the source sandbox, so the adapter's in-memory
    /// `Entry::Golden` is the only record left on the hot path. A new adapter
    /// must use the durable template instead of treating that golden as
    /// missing. The snapshot identity is provider-owned; an ordinary template
    /// with the same name is not adoptable.
    async fn remote_golden(&self, name: &MachineName) -> Option<Entry> {
        let expected = format!("golden-{}", name.as_str());
        let template = match self.api.list_templates().await {
            Ok(templates) => templates.into_iter().find(|template| {
                template.spec.name == expected
                    && template
                        .spec
                        .snapshot_of
                        .as_deref()
                        .is_some_and(|sandbox_id| !sandbox_id.is_empty())
                    && !template.provider_id.is_empty()
                    && !template.driver.is_empty()
                    && !template.native_ref.is_empty()
            }),
            Err(error) => {
                debug!(%error, "snapshot template list failed");
                None
            }
        }?;
        Some(Entry::Golden {
            template: template.spec.name,
            // Snapshot templates intentionally do not duplicate the original
            // machine request. Fork acquires directly from the snapshot, and
            // zero shape fields let the owning driver apply its defaults.
            spec: MachineSpec {
                name: name.clone(),
                image: String::new(),
                cpus: 0,
                memory_mib: 0,
                storage_gib: 0,
                overlay_gib: None,
                network: NetworkPolicy::Unrestricted,
                volumes: Vec::new(),
                sockets: Vec::new(),
                dns: None,
                rosetta: false,
                role: MachineRole::default(),
                guest_volumes: None,
                workload_init: None,
            },
        })
    }

    async fn exec_events(
        &self,
        name: &MachineName,
        argv: &[String],
        env: std::collections::BTreeMap<String, String>,
        output: mpsc::Sender<OutputChunk>,
    ) -> Result<i32, VmError> {
        if argv.is_empty() {
            return Err(VmError::InvalidSpec("argv cannot be empty".into()));
        }
        let (sandbox_id, _) = self.live(name).await?;
        let mut stream = self
            .api
            .exec(ExecRequest {
                sandbox_id,
                argv: argv.to_vec(),
                env,
                ..ExecRequest::default()
            })
            .await
            .map_err(|e| sandbox_error("exec", e))?;
        while let Some(event) = stream.next().await {
            let event = event.map_err(|e| sandbox_error("exec", e))?;
            if let Some(bytes) = event.stdout
                && output.send(OutputChunk::Stdout(bytes)).await.is_err()
            {
                continue;
            }
            if let Some(bytes) = event.stderr {
                let _ = output.send(OutputChunk::Stderr(bytes)).await;
            }
            if let Some(code) = event.exit_code {
                return Ok(code);
            }
        }
        Err(VmError::Protocol(
            "exec stream ended without an exit status".into(),
        ))
    }

    async fn exec_captured(
        &self,
        operation: &'static str,
        name: &MachineName,
        argv: &[String],
        env: std::collections::BTreeMap<String, String>,
    ) -> Result<ExecOutput, VmError> {
        let (tx, mut rx) = mpsc::channel(64);
        let collect = tokio::spawn(async move {
            let (mut stdout, mut stderr, mut truncated) = (Vec::new(), Vec::new(), false);
            while let Some(chunk) = rx.recv().await {
                let (target, bytes) = match chunk {
                    OutputChunk::Stdout(bytes) => (&mut stdout, bytes),
                    OutputChunk::Stderr(bytes) => (&mut stderr, bytes),
                };
                let room = CAPTURE_LIMIT.saturating_sub(target.len());
                truncated |= bytes.len() > room;
                target.extend_from_slice(&bytes[..bytes.len().min(room)]);
            }
            (stdout, stderr, truncated)
        });
        let exit_code = self.exec_events(name, argv, env, tx).await;
        let (stdout, stderr, truncated) = collect
            .await
            .map_err(|e| VmError::Protocol(e.to_string()))?;
        let exit_code = exit_code?;
        // Same contract as the SmolVM CLI provider: a non-zero guest exit is
        // an error carrying the guest's stderr, which the pool's probes
        // (`test -f …`, readiness checks, provisioning steps) rely on.
        if exit_code != 0 {
            return Err(VmError::Command {
                operation,
                exit_code,
                message: String::from_utf8_lossy(&stderr).trim().to_owned(),
            });
        }
        Ok(ExecOutput {
            exit_code,
            stdout,
            stderr,
            truncated,
        })
    }

    /// `(machine, guest path)` for SmolVM's `machine:path` copy syntax.
    fn split_guest(path: &str) -> Option<(MachineName, String)> {
        let (machine, guest) = path.split_once(':')?;
        if !guest.starts_with('/') {
            return None;
        }
        MachineName::new(machine)
            .ok()
            .map(|m| (m, guest.to_owned()))
    }

    /// The sandbox's native reference when *this host's* provider serves it.
    ///
    /// A machine placed on another host (or created by an earlier engine this
    /// process cannot address locally) must never be packed or otherwise
    /// touched through the local CLI: a same-named local machine could be
    /// packed instead.
    async fn local_native(&self, name: &MachineName) -> Option<(Arc<SmolVmProvider>, MachineName)> {
        let local = self.local.clone()?;
        let local_provider = self.local_provider_id.clone()?;
        let (native, provider_id) = match self.entries.lock().await.get(name.as_str()) {
            Some(Entry::Live {
                native_ref,
                provider_id,
                ..
            }) => (native_ref.clone(), provider_id.clone()),
            _ => return None,
        };
        if provider_id != local_provider {
            return None;
        }
        MachineName::new(native).ok().map(|native| (local, native))
    }
}

fn missing(name: &MachineName) -> preloop_sandbox::Error {
    preloop_sandbox::Error::not_found(format!("machine `{}` not found", name.as_str()))
}

fn sandbox_error(operation: &'static str, error: preloop_sandbox::Error) -> VmError {
    let exit_code = match error.code {
        Code::NotFound => 2,
        Code::FailedPrecondition | Code::InvalidArgument => 64,
        _ => 1,
    };
    VmError::Command {
        operation,
        exit_code,
        message: error.to_string(),
    }
}

/// Encode a spec field as a JSON driver option.
fn json_option<T: serde::Serialize>(value: &T) -> Result<String, VmError> {
    serde_json::to_string(value).map_err(|error| VmError::InvalidSpec(error.to_string()))
}

fn network(policy: &NetworkPolicy) -> SbxNetwork {
    match policy {
        NetworkPolicy::Disabled => SbxNetwork {
            mode: NetworkMode::Disabled,
            ..SbxNetwork::default()
        },
        NetworkPolicy::Unrestricted => SbxNetwork {
            mode: NetworkMode::Unrestricted,
            ..SbxNetwork::default()
        },
        NetworkPolicy::PublicOnly => SbxNetwork {
            mode: NetworkMode::PublicOnly,
            ..SbxNetwork::default()
        },
        NetworkPolicy::Restricted { hosts, cidrs } => SbxNetwork {
            mode: NetworkMode::Restricted,
            hosts: hosts.clone(),
            cidrs: cidrs.clone(),
        },
    }
}

fn resolve_secret(source: &SecretSource) -> Result<String, VmError> {
    match source {
        SecretSource::HostEnv(variable) => std::env::var(variable)
            .map_err(|_| VmError::InvalidSpec(format!("secret env `{variable}` is not set"))),
        SecretSource::HostFile(path) => std::fs::read_to_string(path)
            .map(|value| value.trim_end_matches('\n').to_owned())
            .map_err(|error| {
                VmError::InvalidSpec(format!("secret file {}: {error}", path.display()))
            }),
    }
}

/// Single-quote a value for a POSIX shell.
fn shell_export(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn tar_one(source: &Path, entry: &str, tar_path: &Path) -> std::io::Result<()> {
    tar_one_with_mode(source, entry, tar_path, None)
}

/// `mode` overrides the archive entry's permissions (secret carriers are
/// 0600: they must not be readable by other guest users while they exist).
fn tar_one_with_mode(
    source: &Path,
    entry: &str,
    tar_path: &Path,
    mode: Option<u32>,
) -> std::io::Result<()> {
    let mut builder = tar::Builder::new(std::fs::File::create(tar_path)?);
    builder.follow_symlinks(false);
    match (source.is_dir(), mode) {
        (true, _) => {
            builder.append_dir_all(entry, source)?;
        }
        (false, None) => {
            builder.append_path_with_name(source, entry)?;
        }
        (false, Some(mode)) => {
            let mut file = std::fs::File::open(source)?;
            let mut header = tar::Header::new_gnu();
            header.set_metadata(&file.metadata()?);
            header.set_size(file.metadata()?.len());
            header.set_mode(mode);
            header.set_cksum();
            builder.append_data(&mut header, entry, &mut file)?;
        }
    }
    builder.finish()
}

#[async_trait]
impl VmProvider for SandboxMachines {
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            // Packs are host files this process produces through its own CLI
            // backend: without one there is nothing to pack into.
            file_packs: self.capabilities.file_packs && self.local.is_some(),
            ..self.capabilities
        }
    }

    async fn create(&self, spec: &MachineSpec) -> Result<(), VmError> {
        let mut entries = self.entries.lock().await;
        if let Some(Entry::Live { .. } | Entry::Golden { .. }) = entries.get(spec.name.as_str()) {
            return Err(VmError::InvalidSpec(format!(
                "machine `{}` already exists",
                spec.name.as_str()
            )));
        }
        entries.insert(spec.name.as_str().to_owned(), Entry::Defined(spec.clone()));
        Ok(())
    }

    async fn start(&self, name: &MachineName) -> Result<(), VmError> {
        self.start_as(name, Purpose::Runner).await
    }

    async fn start_forkable(&self, name: &MachineName) -> Result<(), VmError> {
        self.start_as(name, Purpose::Build).await
    }

    async fn fork(&self, golden: &MachineName, clone: &MachineName) -> Result<(), VmError> {
        let (template, spec) = {
            let _snapshotting = self.snapshot_lock.lock().await;
            let entry = self.entries.lock().await.get(golden.as_str()).cloned();
            match entry {
                Some(Entry::Golden { template, spec }) => (template, spec),
                Some(Entry::Live {
                    sandbox_id,
                    spec,
                    forkable: true,
                    ..
                }) => {
                    let template = format!("golden-{}", golden.as_str());
                    self.api
                        .snapshot(SnapshotRequest {
                            sandbox_id,
                            name: template.clone(),
                        })
                        .await
                        .map_err(|e| sandbox_error("fork", e))?;
                    self.entries.lock().await.insert(
                        golden.as_str().to_owned(),
                        Entry::Golden {
                            template: template.clone(),
                            spec: spec.clone(),
                        },
                    );
                    (template, spec)
                }
                Some(_) => {
                    return Err(VmError::InvalidSpec(format!(
                        "{} was not started forkable",
                        golden.as_str()
                    )));
                }
                None => return Err(sandbox_error("fork", missing(golden))),
            }
        };
        let mut clone_spec = spec;
        clone_spec.name = clone.clone();
        let entry = self
            .acquire(clone, template, &clone_spec, Purpose::Runner)
            .await?;
        self.entries
            .lock()
            .await
            .insert(clone.as_str().to_owned(), entry);
        Ok(())
    }

    async fn rearm_fork_base(
        &self,
        golden: &MachineName,
        partial: Option<&MachineName>,
    ) -> Result<bool, VmError> {
        if let Some(partial) = partial {
            self.delete(partial).await?;
        }
        // Snapshot templates serve any number of forks.
        Ok(matches!(
            self.entries.lock().await.get(golden.as_str()),
            Some(Entry::Golden { .. })
        ))
    }

    async fn prune_pack_intermediates(&self, name: &MachineName) -> Result<bool, VmError> {
        match self.local_native(name).await {
            Some((local, native)) => local.prune_pack_intermediates(&native).await,
            None => Ok(false),
        }
    }
    async fn prune_failed_unpack_residue(&self) -> Result<usize, VmError> {
        match &self.local {
            Some(local) => local.prune_failed_unpack_residue().await,
            None => Ok(0),
        }
    }

    async fn sweep_orphaned_data_dirs(&self) -> Result<usize, VmError> {
        match &self.local {
            Some(local) => local.sweep_orphaned_data_dirs().await,
            None => Ok(0),
        }
    }

    async fn stop(&self, name: &MachineName) -> Result<(), VmError> {
        // The pool stops a machine to pack it, to re-arm a fork base, or at
        // shutdown: the sandbox stays (with its disk) and can be started again.
        let (sandbox_id, _) = self.live(name).await?;
        match self.api.stop(&sandbox_id).await {
            Ok(()) => {
                if let Some(Entry::Live { state, .. }) =
                    self.entries.lock().await.get_mut(name.as_str())
                {
                    *state = SandboxState::Stopped;
                }
                Ok(())
            }
            Err(error) if error.code == Code::NotFound => Ok(()),
            Err(error) => Err(sandbox_error("stop", error)),
        }
    }

    async fn delete(&self, name: &MachineName) -> Result<(), VmError> {
        let entry = self.entries.lock().await.remove(name.as_str());
        match entry {
            Some(Entry::Live {
                lease_id,
                sandbox_id,
                ..
            }) => {
                match self
                    .api
                    .release(ReleaseRequest {
                        lease_id,
                        mode: ReleaseMode::Destroy,
                        ..ReleaseRequest::default()
                    })
                    .await
                {
                    Ok(()) => {
                        // The service destroys in the background; wait until
                        // the record is gone so a caller recreating the same
                        // name (re-arm, retry) does not race it.
                        for _ in 0..100 {
                            match self.api.get(&sandbox_id).await {
                                Ok(sandbox) if !sandbox.state.is_terminal() => {
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                }
                                _ => break,
                            }
                        }
                        Ok(())
                    }
                    Err(error) if error.code == Code::NotFound => Ok(()),
                    Err(error) => Err(sandbox_error("delete", error)),
                }
            }
            Some(Entry::Golden { template, .. }) => match self.api.delete_template(&template).await
            {
                Ok(()) => Ok(()),
                Err(error) if error.code == Code::NotFound => Ok(()),
                Err(error) => Err(sandbox_error("delete", error)),
            },
            Some(Entry::Defined(_)) => Ok(()),
            None => {
                // Left by an earlier engine process: release whatever lease
                // still holds a sandbox of this name.
                for sandbox in self.remote_by_name(name.as_str()).await {
                    if !sandbox.lease_id.is_empty() {
                        let _ = self
                            .api
                            .release(ReleaseRequest {
                                lease_id: sandbox.lease_id,
                                mode: ReleaseMode::Destroy,
                                ..ReleaseRequest::default()
                            })
                            .await;
                    }
                }
                let _ = self
                    .api
                    .delete_template(&format!("golden-{}", name.as_str()))
                    .await;
                Ok(())
            }
        }
    }

    async fn status(&self, name: &MachineName) -> Result<MachineState, VmError> {
        let entry = { self.entries.lock().await.get(name.as_str()).cloned() };
        let sandbox_id = match entry {
            Some(Entry::Golden { .. }) => return Ok(MachineState::Running),
            Some(Entry::Defined(_)) => return Ok(MachineState::Stopped),
            Some(Entry::Live { sandbox_id, .. }) => sandbox_id.clone(),
            // Not ours: a golden left by an earlier engine, which the pool
            // adopts instead of rebaking. Ask the service before saying
            // Missing, or every restart pays a full golden rebuild.
            None => {
                if let Some(golden) = self.remote_by_name(name.as_str()).await.into_iter().next() {
                    golden.id
                } else if let Some(entry) = self.remote_golden(name).await {
                    self.entries
                        .lock()
                        .await
                        .insert(name.as_str().to_owned(), entry);
                    return Ok(MachineState::Running);
                } else {
                    return Ok(MachineState::Missing);
                }
            }
        };
        match self.api.get(&sandbox_id).await {
            Ok(sandbox) => Ok(match sandbox.state {
                SandboxState::Leased | SandboxState::Preserved | SandboxState::Ready => {
                    MachineState::Running
                }
                SandboxState::Paused => MachineState::Stopped,
                SandboxState::Provisioning => MachineState::Unknown,
                _ => MachineState::Missing,
            }),
            Err(error) if error.code == Code::NotFound => Ok(MachineState::Missing),
            Err(error) => Err(sandbox_error("status", error)),
        }
    }

    async fn list(&self) -> Result<Vec<MachineName>, VmError> {
        let mut names: Vec<String> = self.entries.lock().await.keys().cloned().collect();
        if let Ok(sandboxes) = self.api.list().await {
            names.extend(
                sandboxes
                    .into_iter()
                    .filter(|s| !s.name.is_empty() && !s.state.is_terminal())
                    .map(|s| s.name),
            );
        }
        names.sort();
        names.dedup();
        Ok(names
            .into_iter()
            .filter_map(|name| MachineName::new(name).ok())
            .collect())
    }

    async fn exec(&self, name: &MachineName, argv: &[String]) -> Result<ExecOutput, VmError> {
        self.exec_captured("exec", name, argv, Default::default())
            .await
    }

    async fn exec_with_secret_env(
        &self,
        name: &MachineName,
        argv: &[String],
        secrets: &[(String, SecretSource)],
    ) -> Result<ExecOutput, VmError> {
        if secrets.is_empty() {
            return self
                .exec_captured("exec_with_secret_env", name, argv, Default::default())
                .await;
        }
        // The guest command line is world-readable (`ps`), and the sandbox
        // API has no secret-reference primitive, so a value must never reach
        // argv or an exec env map. Deliver it as a 0600 file the command
        // sources, then remove it.
        let (sandbox_id, _) = self.live(name).await?;
        let mut script = String::from("set -a; ");
        for (key, source) in secrets {
            let value = resolve_secret(source)?;
            if key.is_empty() || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return Err(VmError::InvalidSpec(format!("bad secret name `{key}`")));
            }
            script.push_str(&format!("{key}={}; ", shell_export(&value)));
        }
        script.push_str("set +a; exec");
        for arg in argv {
            script.push(' ');
            script.push_str(&shell_export(arg));
        }
        let dir = tempfile::tempdir().map_err(|e| VmError::RuntimeIo { source: e })?;
        let script_path = dir.path().join("secret-env.sh");
        std::fs::write(&script_path, &script).map_err(|e| VmError::RuntimeIo { source: e })?;
        let tar_path = dir.path().join("secret-env.tar");
        // `/var/tmp`, not `/tmp`: SmolVM's `machine cp` writes into the disk
        // image, and a guest tmpfs mounted over `/tmp` shadows it.
        let entry = format!(".preloop-secret-env-{}", std::process::id());
        let guest = format!("/var/tmp/{entry}");
        tar_one_with_mode(&script_path, &entry, &tar_path, Some(0o600))
            .map_err(|e| VmError::RuntimeIo { source: e })?;
        self.api
            .upload(&sandbox_id, "/var/tmp", &tar_path)
            .await
            .map_err(|e| sandbox_error("exec_with_secret_env", e))?;
        // `sh -c <script> sh <argv…>`: `$0` is the placeholder, `$@` the
        // command the caller asked for.
        let mut wrapper = vec![
            "sh".to_owned(),
            "-c".to_owned(),
            format!(r#"set -e; . {guest}; rm -f {guest}; exec "$@""#),
            "sh".to_owned(),
        ];
        wrapper.extend(argv.iter().cloned());
        let result = self
            .exec_captured("exec_with_secret_env", name, &wrapper, Default::default())
            .await;
        // The script above removes itself; clean up if sourcing failed early.
        let _ = self
            .exec_captured(
                "exec_with_secret_env",
                name,
                &["rm".into(), "-f".into(), guest],
                Default::default(),
            )
            .await;
        result
    }

    async fn exec_stream(
        &self,
        name: &MachineName,
        argv: &[String],
        output: mpsc::Sender<OutputChunk>,
    ) -> Result<i32, VmError> {
        self.exec_events(name, argv, Default::default(), output)
            .await
    }

    async fn copy(&self, source: &str, destination: &str) -> Result<(), VmError> {
        let scratch = tempfile::tempdir().map_err(|e| VmError::RuntimeIo { source: e })?;
        let tar_path = scratch.path().join("copy.tar");
        match (Self::split_guest(source), Self::split_guest(destination)) {
            (None, Some((machine, guest))) => {
                let (sandbox_id, _) = self.live(&machine).await?;
                let guest = PathBuf::from(guest);
                let entry = guest
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .ok_or_else(|| {
                        VmError::InvalidSpec(format!("bad copy destination `{destination}`"))
                    })?;
                let parent = guest
                    .parent()
                    .map_or("/".into(), |p| p.display().to_string());
                tar_one(Path::new(source), &entry, &tar_path)
                    .map_err(|e| VmError::RuntimeIo { source: e })?;
                self.api
                    .upload(&sandbox_id, &parent, &tar_path)
                    .await
                    .map_err(|e| sandbox_error("copy", e))
            }
            (Some((machine, guest)), None) => {
                let (sandbox_id, _) = self.live(&machine).await?;
                self.api
                    .download(&sandbox_id, &guest, &tar_path)
                    .await
                    .map_err(|e| sandbox_error("copy", e))?;
                let unpack = scratch.path().join("out");
                std::fs::create_dir_all(&unpack).map_err(|e| VmError::RuntimeIo { source: e })?;
                tar::Archive::new(
                    std::fs::File::open(&tar_path).map_err(|e| VmError::RuntimeIo { source: e })?,
                )
                .unpack(&unpack)
                .map_err(|e| VmError::RuntimeIo { source: e })?;
                let base = Path::new(&guest)
                    .file_name()
                    .ok_or_else(|| VmError::InvalidSpec(format!("bad copy source `{source}`")))?;
                std::fs::rename(unpack.join(base), destination)
                    .map_err(|e| VmError::RuntimeIo { source: e })
            }
            _ => Err(VmError::InvalidSpec(format!(
                "copy needs exactly one guest side: `{source}` -> `{destination}`"
            ))),
        }
    }

    async fn pack(&self, name: &MachineName, output: &Path) -> Result<(), VmError> {
        match self.local_native(name).await {
            Some((local, native)) => local.pack(&native, output).await,
            None => Err(VmError::InvalidSpec(
                "pack needs the sandbox on this host's SmolVM".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_local_pack_registers_by_content_digest() {
        let dir = tempfile::tempdir().unwrap();
        let pack = dir.path().join("base.smolmachine");
        std::fs::write(&pack, b"first pack").unwrap();

        let template = image_template(pack.to_str().unwrap()).await.unwrap();
        assert_eq!(template.artifacts.len(), 1);
        let artifact = &template.artifacts[0];
        assert_eq!(artifact.format, Artifact::SMOLMACHINE);
        assert_eq!(artifact.arch, host_arch());
        assert_eq!(
            artifact.digest,
            format!("sha256:{}", hex::encode(Sha256::digest(b"first pack")))
        );
        assert_eq!(
            artifact.reference,
            format!("file://{}", std::fs::canonicalize(&pack).unwrap().display())
        );
        assert_eq!(artifact.size_bytes, b"first pack".len() as u64);
        assert_eq!(template.name, image_template_name(&template.artifacts));
        assert!(template.name.starts_with("image-"));

        // The same path with new content is a new template: a rebuilt pack
        // must never be mistaken for the one it replaced.
        std::fs::write(&pack, b"second pack").unwrap();
        let rebuilt = image_template(pack.to_str().unwrap()).await.unwrap();
        assert_ne!(rebuilt.artifacts[0].digest, artifact.digest);
        assert_ne!(rebuilt.name, template.name);
    }

    #[test]
    fn a_digest_pinned_image_is_its_own_fingerprint_and_a_tag_is_not() {
        let pinned = format!("ghcr.io/preloop/base@sha256:{}", "a".repeat(64));
        assert_eq!(image_fingerprint(&pinned), Some(format!("pinned:{pinned}")));
        assert_eq!(image_fingerprint("ghcr.io/preloop/base:latest"), None);
    }

    #[test]
    fn local_image_archives_and_rootfs_directories_stay_recipes() {
        let dir = tempfile::tempdir().unwrap();
        let archive = dir.path().join("base.tar");
        std::fs::write(&archive, b"image archive").unwrap();
        let template = local_image_recipe(archive.to_str().unwrap(), "smolvm").unwrap();
        assert!(template.artifacts.is_empty());
        assert!(
            template
                .recipe
                .as_deref()
                .is_some_and(|recipe| recipe.starts_with("image:")),
            "{:?}",
            template.recipe
        );
        assert_eq!(template.requires.driver, "smolvm");

        let rootfs = dir.path().join("rootfs");
        std::fs::create_dir_all(&rootfs).unwrap();
        let template = local_image_recipe(rootfs.to_str().unwrap(), "smolvm").unwrap();
        assert!(template.artifacts.is_empty());
        assert!(template.recipe.is_some());

        assert!(local_image_recipe("alpine:3.20", "smolvm").is_none());
    }
}
