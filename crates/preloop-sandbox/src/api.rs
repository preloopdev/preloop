//! Wire types for `proto/preloop/sandbox/v1/sandbox.proto`.
//!
//! Hand-written to the proto3 JSON mapping (lowerCamelCase fields, enum
//! value names, base64 bytes, oneof members as optional fields), so any
//! Connect client generated from the `.proto` interoperates with the JSON
//! codec this crate serves.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Base64 (standard, padded) for proto `bytes` fields.
pub mod b64 {
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serializer};

    /// Serialize bytes as a base64 string.
    pub fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
    }

    /// Deserialize a base64 string into bytes.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let text = String::deserialize(d)?;
        base64::engine::general_purpose::STANDARD
            .decode(text.as_bytes())
            .map_err(serde::de::Error::custom)
    }

    /// Optional variant for oneof members.
    pub mod opt {
        use base64::Engine as _;
        use serde::{Deserialize, Deserializer, Serializer};

        /// Serialize optional bytes as base64.
        pub fn serialize<S: Serializer>(bytes: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
            match bytes {
                Some(bytes) => {
                    s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
                }
                None => s.serialize_none(),
            }
        }

        /// Deserialize optional base64 into bytes.
        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
            let text = Option::<String>::deserialize(d)?;
            text.map(|text| {
                base64::engine::general_purpose::STANDARD
                    .decode(text.as_bytes())
                    .map_err(serde::de::Error::custom)
            })
            .transpose()
        }
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// What a driver can do.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Capabilities {
    /// `linux` | `macos` | `windows`.
    pub os: String,
    /// Supported guest architectures (`x86_64`, `aarch64`).
    pub arch: Vec<String>,
    /// Free-form features (`docker`, `display`, `simulator`, …).
    pub features: Vec<String>,
    /// Pause/resume with live state.
    #[serde(skip_serializing_if = "is_false")]
    pub pause: bool,
    /// Capture a sandbox as a new template.
    #[serde(skip_serializing_if = "is_false")]
    pub snapshot: bool,
    /// Fork a running sandbox.
    #[serde(skip_serializing_if = "is_false")]
    pub fork: bool,
    /// Host Unix sockets can be mounted into the guest.
    #[serde(skip_serializing_if = "is_false")]
    pub socket_mounts: bool,
    /// Live host directories can be mounted into the guest.
    #[serde(skip_serializing_if = "is_false")]
    pub host_mounts: bool,
    /// Enforceable network modes.
    pub network_modes: Vec<NetworkMode>,
    /// Maximum concurrent sandboxes (0 = unbounded).
    #[serde(skip_serializing_if = "is_zero_u32")]
    pub slots: u32,
    /// The backend keeps its own warm pool.
    #[serde(skip_serializing_if = "is_false")]
    pub native_pool: bool,
    /// Each sandbox runs exactly one command (a one-job VM); files uploaded
    /// before it are staged into that run. Clients needing several execs
    /// require `multi_exec`.
    #[serde(skip_serializing_if = "is_false")]
    pub one_shot: bool,
    /// Largest vCPU count one sandbox can be given (0 = unreported).
    ///
    /// Placement rejects a [`Shape`] above this instead of handing the driver
    /// a machine it cannot boot.
    #[serde(skip_serializing_if = "is_zero_u32")]
    pub max_vcpus: u32,
    /// Largest memory one sandbox can be given, in MiB (0 = unreported).
    #[serde(skip_serializing_if = "is_zero_u32")]
    pub max_memory_mib: u32,
}

impl Capabilities {
    /// Whether these capabilities include a named optional operation.
    pub fn has(&self, capability: &str) -> bool {
        match capability {
            "pause" => self.pause,
            "snapshot" => self.snapshot,
            "fork" => self.fork,
            "socket_mounts" | "socketMounts" => self.socket_mounts,
            "host_mounts" | "hostMounts" => self.host_mounts,
            "native_pool" | "nativePool" => self.native_pool,
            "multi_exec" | "multiExec" => !self.one_shot,
            _ => false,
        }
    }
}

/// Guest egress mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NetworkMode {
    /// Not set; drivers treat it as the pool default.
    #[default]
    NetworkModeUnspecified,
    /// No networking.
    Disabled,
    /// Unrestricted egress.
    Unrestricted,
    /// Internet only; private, loopback and link-local ranges denied.
    PublicOnly,
    /// Only the listed hosts/CIDRs.
    Restricted,
}

/// Guest egress policy.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct NetworkPolicy {
    /// Mode.
    pub mode: NetworkMode,
    /// Allowed host names (restricted mode).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub hosts: Vec<String>,
    /// Allowed CIDRs (restricted mode, or extra allowances).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub cidrs: Vec<String>,
}

/// Machine size.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Shape {
    /// vCPUs.
    pub cpus: u32,
    /// Memory in MiB.
    pub memory_mib: u32,
    /// Disk in GiB.
    pub disk_gib: u32,
}

/// Placement requirements.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Requirements {
    /// Required guest OS.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub os: String,
    /// Required guest architecture.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub arch: String,
    /// Required features.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<String>,
    /// Required optional operations.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    /// Pin a driver (debugging escape hatch).
    #[serde(skip_serializing_if = "String::is_empty")]
    pub driver: String,
}

impl Requirements {
    /// Combine two requirement sets; non-empty scalar fields of `other` win.
    pub fn merged(&self, other: &Requirements) -> Requirements {
        let pick = |a: &String, b: &String| if b.is_empty() { a.clone() } else { b.clone() };
        let mut features = self.features.clone();
        features.extend(other.features.iter().cloned());
        features.sort();
        features.dedup();
        let mut capabilities = self.capabilities.clone();
        capabilities.extend(other.capabilities.iter().cloned());
        capabilities.sort();
        capabilities.dedup();
        Requirements {
            os: pick(&self.os, &other.os),
            arch: pick(&self.arch, &other.arch),
            features,
            capabilities,
            driver: pick(&self.driver, &other.driver),
        }
    }
}

/// Host directory mount.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mount {
    /// Host path.
    pub host: String,
    /// Guest path.
    pub guest: String,
    /// Read-only.
    #[serde(default, skip_serializing_if = "is_false")]
    pub readonly: bool,
}

/// Host Unix socket mount.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SocketMount {
    /// Host socket path.
    pub host: String,
    /// Guest socket path.
    pub guest: String,
}

/// One immutable artifact a backend can materialize for a template.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Artifact {
    /// `oci-image` or `smolmachine`.
    pub format: String,
    /// Guest architecture (`aarch64`/`x86_64`, or `arm64`/`amd64`).
    pub arch: String,
    /// Image reference or `file://` pack path. Proto field: `ref`.
    #[serde(rename = "ref")]
    pub reference: String,
    /// sha256 of the artifact content (`sha256:<hex>`).
    pub digest: String,
    /// Size in bytes when known.
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub size_bytes: u64,
}

impl Artifact {
    /// A registry image the backend pulls itself.
    pub const OCI_IMAGE: &'static str = "oci-image";
    /// A monolithic packed machine (an OCI artifact or a `file://` pack).
    pub const SMOLMACHINE: &'static str = "smolmachine";

    /// Validate the artifact's shape and digest pinning.
    ///
    /// Returns a message phrased for the caller (`PutTemplate` turns it into
    /// `INVALID_ARGUMENT`), or `Ok(())`.
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.format.is_empty() {
            return Err("artifact format is required".into());
        }
        if self.arch.is_empty() {
            return Err("artifact arch is required".into());
        }
        if !is_sha256_digest(&self.digest) {
            return Err(format!(
                "artifact digest `{}` is not a `sha256:<64 hex>` digest",
                self.digest
            ));
        }
        if self.reference.is_empty() {
            return Err("artifact ref is required".into());
        }
        match self.format.as_str() {
            Self::OCI_IMAGE => {
                let pinned = self.reference.rsplit_once('@').map(|(_, digest)| digest);
                match pinned {
                    Some(digest) if is_sha256_digest(digest) => {}
                    _ => {
                        return Err(format!(
                            "artifact ref `{}` is not digest-pinned; use `{}@sha256:…`",
                            self.reference, self.reference
                        ));
                    }
                }
                if !pinned.is_some_and(|digest| digest.eq_ignore_ascii_case(&self.digest)) {
                    return Err(format!(
                        "artifact digest `{}` does not match its pinned ref `{}`",
                        self.digest, self.reference
                    ));
                }
            }
            Self::SMOLMACHINE => match self.reference.strip_prefix("file://") {
                Some("") => return Err("artifact ref `file://` has no path".into()),
                Some(_) => {}
                None => {
                    let pinned = self.reference.rsplit_once('@').map(|(_, digest)| digest);
                    if !pinned.is_some_and(is_sha256_digest) {
                        return Err(format!(
                            "artifact ref `{}` must be a `file://` pack or a digest-pinned \
                             OCI artifact reference",
                            self.reference
                        ));
                    }
                }
            },
            other => {
                return Err(format!(
                    "unsupported artifact format `{other}`; use `{}` or `{}`",
                    Self::OCI_IMAGE,
                    Self::SMOLMACHINE
                ));
            }
        }
        Ok(())
    }

    /// Whether this artifact serves `host_arch`, accepting the OCI spelling.
    pub fn matches_arch(&self, host_arch: &str) -> bool {
        self.arch == host_arch
            || matches!(
                (host_arch, self.arch.as_str()),
                ("aarch64", "arm64") | ("x86_64", "amd64")
            )
            || matches!(
                (host_arch, self.arch.as_str()),
                ("arm64", "aarch64") | ("amd64", "x86_64")
            )
    }
}

/// Whether `value` is `sha256:` followed by 64 hex characters.
pub fn is_sha256_digest(value: &str) -> bool {
    value
        .strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

/// This host's guest architecture, in the spelling artifacts and drivers use
/// (`aarch64`, `x86_64`).
pub fn host_arch() -> &'static str {
    match std::env::consts::ARCH {
        "aarch64" | "arm64" => "aarch64",
        other => other,
    }
}

/// A logical image.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TemplateSpec {
    /// Unique name.
    pub name: String,
    /// Guest OS.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub os: String,
    /// Driver-specific recipe (oneof source).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipe: Option<String>,
    /// Sandbox captured by `Snapshot` (oneof source).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_of: Option<String>,
    /// Formats and architectures this template can be materialized in.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<Artifact>,
    /// Placement requirements every sandbox from this template carries.
    pub requires: Requirements,
    /// Labels.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

/// A registered template.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Template {
    /// Spec as submitted.
    pub spec: TemplateSpec,
    /// sha256 of the canonical spec.
    pub digest: String,
    /// Owner that holds the snapshot (snapshot templates only).
    #[serde(skip_serializing_if = "String::is_empty")]
    pub provider_id: String,
    /// Driver that holds the snapshot (snapshot templates only).
    #[serde(skip_serializing_if = "String::is_empty")]
    pub driver: String,
    /// Native snapshot reference (snapshot templates only).
    #[serde(skip_serializing_if = "String::is_empty")]
    pub native_ref: String,
    /// Unix seconds.
    pub created_at: i64,
}

/// Pool configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PoolSpec {
    /// Unique name.
    pub name: String,
    /// Template name.
    pub template: String,
    /// Machine size.
    pub shape: Shape,
    /// Egress policy.
    pub network: NetworkPolicy,
    /// Idle ready sandboxes to keep.
    #[serde(skip_serializing_if = "is_zero_u32")]
    pub warm: u32,
    /// Maximum live sandboxes (0 = unbounded).
    #[serde(skip_serializing_if = "is_zero_u32")]
    pub max: u32,
    /// Extra requirements.
    pub requires: Requirements,
    /// Host directory mounts.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<Mount>,
    /// Host socket mounts.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sockets: Vec<SocketMount>,
    /// Driver hints.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub options: BTreeMap<String, String>,
    /// Default lease TTL.
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub lease_ttl_secs: u64,
}

/// Why a sandbox is acquired.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Purpose {
    /// Not set (treated as `Exec`).
    #[default]
    PurposeUnspecified,
    /// An ephemeral CI runner.
    Runner,
    /// One-off command execution.
    Exec,
    /// Interactive session.
    Session,
    /// Template bake; started snapshot-capable.
    Build,
}

/// A file written into the guest at acquire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSpec {
    /// Absolute guest path.
    pub path: String,
    /// Contents.
    #[serde(with = "b64")]
    pub data: Vec<u8>,
    /// Octal mode (0 = 0644).
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub mode: u32,
}

/// Identity delivered at acquire.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Inject {
    /// Environment merged into every exec on the lease.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Files written before the lease is returned.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<FileSpec>,
}

/// Sandbox lifecycle state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SandboxState {
    /// Not set.
    #[default]
    SandboxStateUnspecified,
    /// Being created.
    Provisioning,
    /// Warm and unleased.
    Ready,
    /// Held by a lease.
    Leased,
    /// Paused.
    Paused,
    /// Powered off; the machine and its disk are kept.
    Stopped,
    /// Kept for debugging after release.
    Preserved,
    /// Being destroyed.
    Terminating,
    /// Destroyed.
    Gone,
    /// Creation or runtime failure.
    Failed,
}

impl SandboxState {
    /// Terminal states hold no capacity.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Gone | Self::Failed)
    }
}

/// A sandbox record.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Sandbox {
    /// Service id (`sbx-…`).
    pub id: String,
    /// Caller-chosen name (see [`AcquireRequest::name`]).
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// Template name.
    pub template: String,
    /// Pool name, if pooled.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub pool: String,
    /// Owning provider.
    pub provider_id: String,
    /// Driver on that provider.
    pub driver: String,
    /// Driver-native reference.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub native_ref: String,
    /// Bumped whenever the id is re-provisioned.
    pub incarnation: u64,
    /// State.
    pub state: SandboxState,
    /// Purpose.
    pub purpose: Purpose,
    /// Current lease. Redacted in `List`; `Get` returns it only to the
    /// caller presenting that lease.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub lease_id: String,
    /// Failure reason.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub error: String,
    /// Unix seconds.
    pub created_at: i64,
    /// Preserved sandboxes expire at this Unix second (0 = n/a).
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub preserve_until: i64,
}

fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

/// A lease on a sandbox. The id is the capability every sandbox-scoped RPC
/// presents; only `Acquire` and `Renew` (both scoped to the lease) return it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Lease {
    /// Lease id (`lse-…`).
    pub id: String,
    /// Sandbox id.
    pub sandbox_id: String,
    /// Pool, if any.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub pool: String,
    /// Caller idempotency key.
    pub idempotency_key: String,
    /// Unix seconds.
    pub expires_at: i64,
    /// Environment merged into every exec on this lease.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

/// Empty message.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Empty {}

/// List request: page size and the token of the previous page.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ListRequest {
    /// Items per page (0 = default 100; capped at 1000).
    #[serde(skip_serializing_if = "is_zero_u32")]
    pub page_size: u32,
    /// Opaque continuation token from the previous response.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub page_token: String,
}

/// `PutTemplate`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PutTemplateRequest {
    /// Template.
    pub template: TemplateSpec,
}

/// `ListTemplates`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ListTemplatesResponse {
    /// Templates.
    pub templates: Vec<Template>,
    /// Continuation token; empty on the last page.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub next_page_token: String,
}

/// `DeleteTemplate`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct DeleteTemplateRequest {
    /// Template name.
    pub name: String,
}

/// `PutPool`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PutPoolRequest {
    /// Pool.
    pub pool: PoolSpec,
}

/// `ListPools`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ListPoolsResponse {
    /// Pools.
    pub pools: Vec<PoolSpec>,
    /// Continuation token; empty on the last page.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub next_page_token: String,
}

/// `DeletePool`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct DeletePoolRequest {
    /// Pool name.
    pub name: String,
}

/// `Acquire`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct AcquireRequest {
    /// Required; the same key returns the same lease.
    pub idempotency_key: String,
    /// Optional caller-chosen name, unique among live sandboxes.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// Pool (or `template`).
    #[serde(skip_serializing_if = "String::is_empty")]
    pub pool: String,
    /// Template (or `pool`).
    #[serde(skip_serializing_if = "String::is_empty")]
    pub template: String,
    /// Size override.
    pub shape: Shape,
    /// Extra requirements.
    pub requires: Requirements,
    /// Purpose.
    pub purpose: Purpose,
    /// Lease TTL (0 = pool or service default).
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub ttl_secs: u64,
    /// Identity.
    pub inject: Inject,
    /// Egress policy override (template acquires).
    pub network: NetworkPolicy,
    /// Host mounts (template acquires).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<Mount>,
    /// Socket mounts (template acquires).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sockets: Vec<SocketMount>,
    /// Driver hints (template acquires).
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub options: BTreeMap<String, String>,
    /// Block until the sandbox is ready. Absent = service default;
    /// `Some(0)` returns immediately (one attempt) instead of blocking.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_secs: Option<u64>,
}

/// `Acquire` result.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct AcquireResponse {
    /// Lease.
    pub lease: Lease,
    /// Sandbox.
    pub sandbox: Sandbox,
}

/// `Renew`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RenewRequest {
    /// Lease id.
    pub lease_id: String,
    /// New TTL from now (0 = original TTL).
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub ttl_secs: u64,
}

/// Release disposition.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReleaseMode {
    /// Treated as `Destroy`.
    #[default]
    ReleaseModeUnspecified,
    /// Destroy the sandbox.
    Destroy,
    /// Keep it (debugging) until `preserve_ttl_secs`.
    Preserve,
}

/// `Release`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ReleaseRequest {
    /// Lease id.
    pub lease_id: String,
    /// Disposition.
    pub mode: ReleaseMode,
    /// Preservation window.
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub preserve_ttl_secs: u64,
}

/// `Get` / `Stop` / `Pause` / `Resume`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GetRequest {
    /// Sandbox id.
    pub sandbox_id: String,
    /// Capability from `Acquire`; must be this sandbox's active lease.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub lease_id: String,
}

/// `List`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ListSandboxesResponse {
    /// Sandboxes.
    pub sandboxes: Vec<Sandbox>,
    /// Continuation token; empty on the last page.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub next_page_token: String,
}

/// `Exec`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ExecRequest {
    /// Sandbox id.
    pub sandbox_id: String,
    /// Capability from `Acquire`; must be this sandbox's active lease.
    /// Cleared before the request is handed to a provider.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub lease_id: String,
    /// Command.
    pub argv: Vec<String>,
    /// Environment (added to the lease environment).
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Working directory.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub workdir: String,
    /// Guest user.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub user: String,
    /// Timeout (0 = none).
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub timeout_secs: u64,
    /// Standard input.
    #[serde(with = "b64", skip_serializing_if = "Vec::is_empty")]
    pub stdin: Vec<u8>,
}

/// One exec stream event (oneof).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ExecEvent {
    /// Stdout bytes.
    #[serde(with = "b64::opt", skip_serializing_if = "Option::is_none")]
    pub stdout: Option<Vec<u8>>,
    /// Stderr bytes.
    #[serde(with = "b64::opt", skip_serializing_if = "Option::is_none")]
    pub stderr: Option<Vec<u8>>,
    /// Exit status (terminal event).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

impl ExecEvent {
    /// Stdout fragment.
    pub fn stdout(bytes: Vec<u8>) -> Self {
        Self {
            stdout: Some(bytes),
            ..Self::default()
        }
    }
    /// Stderr fragment.
    pub fn stderr(bytes: Vec<u8>) -> Self {
        Self {
            stderr: Some(bytes),
            ..Self::default()
        }
    }
    /// Exit status.
    pub fn exit(code: i32) -> Self {
        Self {
            exit_code: Some(code),
            ..Self::default()
        }
    }
}

/// Stream direction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StreamKind {
    /// Not set.
    #[default]
    StreamKindUnspecified,
    /// Client uploads a tar extracted at `path`.
    Upload,
    /// Client downloads a tar of `path`.
    Download,
}

/// `OpenStream`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct OpenStreamRequest {
    /// Sandbox id.
    pub sandbox_id: String,
    /// Capability from `Acquire`; must be this sandbox's active lease.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub lease_id: String,
    /// Direction.
    pub kind: StreamKind,
    /// Guest path (directory for uploads; file or directory for downloads).
    pub path: String,
}

/// A data-plane ticket.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Ticket {
    /// URL (relative to the service base when it starts with `/`).
    pub url: String,
    /// Bearer token for the URL.
    pub token: String,
    /// Unix seconds.
    pub expires_at: i64,
}

/// `Snapshot`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SnapshotRequest {
    /// Sandbox to capture.
    pub sandbox_id: String,
    /// Capability from `Acquire`; must be this sandbox's active lease.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub lease_id: String,
    /// New template name.
    pub name: String,
}

/// A driver advertised by a provider.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct DriverInfo {
    /// Driver name.
    pub driver: String,
    /// Capabilities.
    pub capabilities: Capabilities,
}

/// A registered provider.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Provider {
    /// Provider id.
    pub id: String,
    /// Display name.
    pub name: String,
    /// Fencing generation (bumped per registration).
    pub generation: u64,
    /// Drivers.
    pub drivers: Vec<DriverInfo>,
    /// Unix seconds.
    pub last_seen: i64,
    /// No new placements are made here; survives re-registration.
    #[serde(skip_serializing_if = "is_false")]
    pub cordoned: bool,
}

/// `ListProviders`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ListProvidersResponse {
    /// Providers.
    pub providers: Vec<Provider>,
    /// Continuation token; empty on the last page.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub next_page_token: String,
}

/// `CordonProvider` / `UncordonProvider` / `DrainProvider`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ProviderControlRequest {
    /// Provider name (or id).
    pub name: String,
}

/// `DrainProvider` result.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct DrainProviderResult {
    /// Idle pool members released by this drain.
    #[serde(skip_serializing_if = "is_zero_u32")]
    pub released_pool_members: u32,
    /// Leased sandboxes still running on the provider (not killed).
    #[serde(skip_serializing_if = "is_zero_u32")]
    pub leased_remaining: u32,
}

/// `Register`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RegisterRequest {
    /// Stable provider name (re-registering the same name bumps the generation).
    pub name: String,
    /// Drivers.
    pub drivers: Vec<DriverInfo>,
}

/// `Register` result.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct RegisterResponse {
    /// Provider id.
    pub provider_id: String,
    /// Fencing generation.
    pub generation: u64,
}

/// `Heartbeat`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HeartbeatRequest {
    /// Provider id.
    pub provider_id: String,
    /// Generation.
    pub generation: u64,
}

/// `Poll`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PollRequest {
    /// Provider id.
    pub provider_id: String,
    /// Generation.
    pub generation: u64,
    /// Long-poll window.
    pub wait_secs: u64,
}

/// Create a sandbox.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CreateWork {
    /// Sandbox id.
    pub sandbox_id: String,
    /// Caller-chosen name; drivers use it as the native name when set.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// Incarnation.
    pub incarnation: u64,
    /// Driver.
    pub driver: String,
    /// Template (with native_ref when it is a snapshot).
    pub template: Template,
    /// Size.
    pub shape: Shape,
    /// Egress.
    pub network: NetworkPolicy,
    /// Mounts.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<Mount>,
    /// Socket mounts.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sockets: Vec<SocketMount>,
    /// Driver hints.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub options: BTreeMap<String, String>,
    /// Start snapshot-capable (build sandboxes).
    #[serde(skip_serializing_if = "is_false")]
    pub snapshot_capable: bool,
}

/// A work target.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SandboxWork {
    /// Sandbox id.
    pub sandbox_id: String,
    /// Driver.
    pub driver: String,
    /// Native reference.
    pub native_ref: String,
}

/// Run a command.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ExecWork {
    /// Exec id; output is reported with `ReportExec`.
    pub exec_id: String,
    /// Target.
    pub target: SandboxWork,
    /// Request (environment already merged).
    pub request: ExecRequest,
}

/// Move a tar through a ticket.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CopyWork {
    /// Target.
    pub target: SandboxWork,
    /// Ticket id (fetch/put at `/sandbox/v1/agent/blobs/{ticket}`).
    pub ticket: String,
    /// Guest path.
    pub path: String,
    /// Direction.
    pub kind: StreamKind,
}

/// Start a stopped sandbox.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct StartWork {
    /// Target.
    pub target: SandboxWork,
    /// Start as a branch base (`--forkable`).
    #[serde(skip_serializing_if = "is_false")]
    pub forkable: bool,
}

/// `Start`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct StartRequest {
    /// Sandbox id.
    pub sandbox_id: String,
    /// Capability from `Acquire`; must be this sandbox's active lease.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub lease_id: String,
    /// Start as a branch base.
    #[serde(skip_serializing_if = "is_false")]
    pub forkable: bool,
}

/// Capture a snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SnapshotWork {
    /// Target.
    pub target: SandboxWork,
    /// New template name.
    pub template: String,
}

/// One unit of provider work (oneof).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct WorkItem {
    /// Work id.
    pub id: String,
    /// Create.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create: Option<CreateWork>,
    /// Destroy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destroy: Option<SandboxWork>,
    /// Exec.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exec: Option<ExecWork>,
    /// Copy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub copy: Option<CopyWork>,
    /// Snapshot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SnapshotWork>,
    /// Pause.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pause: Option<SandboxWork>,
    /// Resume.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume: Option<SandboxWork>,
    /// Power off, keeping the machine.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<SandboxWork>,
    /// Start a stopped machine.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<StartWork>,
}

/// `Poll` result.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PollResponse {
    /// Work.
    pub work: Vec<WorkItem>,
}

/// `Report`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ReportRequest {
    /// Provider id.
    pub provider_id: String,
    /// Generation.
    pub generation: u64,
    /// Work id.
    pub work_id: String,
    /// Success.
    pub ok: bool,
    /// Failure reason.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub error: String,
    /// Create/snapshot result.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub native_ref: String,
}

/// `ReportExec`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ReportExecRequest {
    /// Provider id.
    pub provider_id: String,
    /// Generation.
    pub generation: u64,
    /// Exec id.
    pub exec_id: String,
    /// Events, in order.
    pub events: Vec<ExecEvent>,
}

/// One observed sandbox.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Observed {
    /// Driver.
    pub driver: String,
    /// Native reference.
    pub native_ref: String,
    /// Sandbox id, when the driver can recover it from the native name.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub sandbox_id: String,
    /// Incarnation, when recoverable.
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub incarnation: u64,
}

/// `Reconcile`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ReconcileRequest {
    /// Provider id.
    pub provider_id: String,
    /// Generation.
    pub generation: u64,
    /// Full inventory.
    pub sandboxes: Vec<Observed>,
}

/// `Reconcile` result: what the provider must destroy.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ReconcileResponse {
    /// Orphans and stale incarnations.
    pub destroy: Vec<SandboxWork>,
}
