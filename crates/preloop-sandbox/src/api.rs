//! The Sandbox Service's API types and domain helpers.
//!
//! The types are generated from `proto/preloop/sandbox/v1/sandbox.proto`
//! (see [`crate::gen`]); this module re-exports them under one path and adds
//! the domain behaviour that is not wire state — capability lookup,
//! requirement merging, artifact validation and the few derived labels the
//! service computes from a state enum. Keeping it here (rather than on a
//! hand-written twin of the proto) means there is exactly one definition of
//! every field.

pub use crate::generated::preloop::sandbox::v1::*;

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
            ..Requirements::default()
        }
    }
}

impl Artifact {
    /// A registry image the backend pulls itself.
    pub const OCI_IMAGE: &'static str = "oci-image";
    /// A monolithic packed machine (an OCI artifact or a `file://` pack).
    pub const SMOLMACHINE: &'static str = "smolmachine";

    /// The artifact's registry reference or pack path (proto field `ref`).
    pub fn reference(&self) -> &str {
        &self.r#ref
    }

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
        if self.r#ref.is_empty() {
            return Err("artifact ref is required".into());
        }
        match self.format.as_str() {
            Self::OCI_IMAGE => {
                let pinned = self.r#ref.rsplit_once('@').map(|(_, digest)| digest);
                match pinned {
                    Some(digest) if is_sha256_digest(digest) => {}
                    _ => {
                        return Err(format!(
                            "artifact ref `{}` is not digest-pinned; use `{}@sha256:…`",
                            self.r#ref, self.r#ref
                        ));
                    }
                }
                if !pinned.is_some_and(|digest| digest.eq_ignore_ascii_case(&self.digest)) {
                    return Err(format!(
                        "artifact digest `{}` does not match its pinned ref `{}`",
                        self.digest, self.r#ref
                    ));
                }
            }
            Self::SMOLMACHINE => match self.r#ref.strip_prefix("file://") {
                Some("") => return Err("artifact ref `file://` has no path".into()),
                Some(_) => {}
                None => {
                    let pinned = self.r#ref.rsplit_once('@').map(|(_, digest)| digest);
                    if !pinned.is_some_and(is_sha256_digest) {
                        return Err(format!(
                            "artifact ref `{}` must be a `file://` pack or a digest-pinned \
                             OCI artifact reference",
                            self.r#ref
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

/// Derived behaviour on a sandbox's state.
///
/// The generated message stores an open enum ([`buffa::EnumValue`]), so an
/// unknown wire value is preserved rather than coerced. Every predicate here
/// treats an unknown value as "not the state asked about", which is what the
/// service wants: an unknown state is never terminal, never ready.
pub trait SandboxStateExt {
    /// The known state, with an unrecognized value read as unspecified.
    fn known(self) -> SandboxState;
    /// Terminal states hold no capacity.
    fn is_terminal(&self) -> bool;
    /// Lowercase label for metrics (matches the proto value names).
    fn as_metric_label(&self) -> &'static str;
}

impl SandboxStateExt for buffa::EnumValue<SandboxState> {
    fn known(self) -> SandboxState {
        self.as_known()
            .unwrap_or(SandboxState::SandboxStateUnspecified)
    }

    fn is_terminal(&self) -> bool {
        matches!(self.known(), SandboxState::Gone | SandboxState::Failed)
    }

    fn as_metric_label(&self) -> &'static str {
        self.known().as_metric_label()
    }
}

impl SandboxState {
    /// Terminal states hold no capacity.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Gone | Self::Failed)
    }

    /// Lowercase label for metrics (matches the proto value names).
    pub fn as_metric_label(self) -> &'static str {
        match self {
            Self::SandboxStateUnspecified => "unspecified",
            Self::Provisioning => "provisioning",
            Self::Ready => "ready",
            Self::Leased => "leased",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
            Self::Preserved => "preserved",
            Self::Terminating => "terminating",
            Self::Gone => "gone",
            Self::Failed => "failed",
        }
    }
}

impl From<preloop_observability::vm_telemetry::HostSample> for HostSample {
    fn from(sample: preloop_observability::vm_telemetry::HostSample) -> Self {
        Self {
            mem_total_bytes: sample.mem_total_bytes,
            mem_available_bytes: sample.mem_available_bytes,
            swap_total_bytes: sample.swap_total_bytes,
            swap_free_bytes: sample.swap_free_bytes,
            engine_rss_bytes: sample.engine_rss_bytes,
            vm_rss_bytes: sample.vm_rss_bytes,
            ..HostSample::default()
        }
    }
}

impl HostSample {
    /// Fraction of host RAM consumed, or `None` when unmeasured. Available
    /// falling to zero must read as pressure, never as headroom.
    pub fn ram_used_fraction(&self) -> Option<f64> {
        match (self.mem_total_bytes, self.mem_available_bytes) {
            (Some(total), Some(available)) if total > 0 => {
                Some(1.0 - available as f64 / total as f64)
            }
            _ => None,
        }
    }

    /// Fraction of swap consumed, or `None` when the host has no swap or it
    /// is unmeasured. Swap is pressure, never headroom.
    pub fn swap_used_fraction(&self) -> Option<f64> {
        match (self.swap_total_bytes, self.swap_free_bytes) {
            (Some(total), Some(free)) if total > 0 => Some(1.0 - free as f64 / total as f64),
            _ => None,
        }
    }
}

impl TemplateSpec {
    /// The driver-specific recipe, when this template names one.
    pub fn recipe(&self) -> Option<&str> {
        match self.source.as_ref() {
            Some(template_spec::Source::Recipe(recipe)) => Some(recipe),
            _ => None,
        }
    }

    /// The sandbox this template was captured from, for snapshot templates.
    pub fn snapshot_of(&self) -> Option<&str> {
        match self.source.as_ref() {
            Some(template_spec::Source::SnapshotOf(id)) => Some(id),
            _ => None,
        }
    }
}

impl WorkItem {
    /// One work item carrying `work` under a fresh or redelivered id.
    pub fn new(id: impl Into<String>, work: work_item::Work) -> Self {
        Self {
            id: id.into(),
            work: Some(work),
            ..Self::default()
        }
    }
}

impl ExecEvent {
    /// Stdout fragment.
    pub fn stdout(bytes: Vec<u8>) -> Self {
        Self {
            event: Some(exec_event::Event::Stdout(bytes)),
            ..Self::default()
        }
    }

    /// Stderr fragment.
    pub fn stderr(bytes: Vec<u8>) -> Self {
        Self {
            event: Some(exec_event::Event::Stderr(bytes)),
            ..Self::default()
        }
    }

    /// Exit status (terminal event).
    pub fn exit(code: i32) -> Self {
        Self {
            event: Some(exec_event::Event::ExitCode(code)),
            ..Self::default()
        }
    }
}

/// The concrete `HashMap` a generated proto3 `map` field uses.
///
/// buffa parameterizes `std::collections::HashMap` with its own hasher, so a
/// bare `std::collections::HashMap` is a different type even though the
/// contents and JSON mapping are identical.
pub type Map<K, V> = buffa::__private::HashMap<K, V>;

/// A proto3 `map<string, string>` field.
pub type StringMap = Map<String, String>;
