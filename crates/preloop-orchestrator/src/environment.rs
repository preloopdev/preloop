//! Base-image selection for disposable job environments.
//!
//! A job VM is forked from a golden. There are exactly two golden sources:
//!
//! * the **official** packed golden ([`OFFICIAL_GOLDEN`]) — the published
//!   preloop runner image, downloaded and verified per architecture, and
//!   mandatory: nothing is baked locally when it cannot be fetched;
//! * a **configured image** — whatever `PRELOOP_RUNNER_BASE_IMAGE` or
//!   `[golden] base_image` names, baked into a golden as-is plus the
//!   GitHub-runner machinery (see `golden_contract_script`).
//!
//! There is no third source: the stock Ubuntu bake (apt baseline, language
//! toolchains, package pins) is gone, and with it the "curated" classification
//! that used to decide between it and a custom image.

use preloop_gha_protocol::oci_image_ref;
use preloop_vm::{MachineRole, MachineSpec, SMOLVM_GUEST_VOLUMES_ENV, SMOLVM_WORKLOAD_INIT_ENV};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

/// Collect every container image the workspace's workflows declare.
///
/// Read from the YAML rather than from expanded job plans because this runs at
/// startup, before any job is queued, so the golden can be warmed during its
/// single build instead of triggering a second one later. That is also why
/// images stay out of [`EnvironmentSpec`]'s fingerprint: a distinct fingerprint
/// forces a fresh golden build (measured 249s) to save a 4-9s pull, and
/// preloading is semantically free -- a spare image is harmless, a missing one
/// is simply pulled.
///
/// Parsing is deliberately lenient. A workflow this cannot read is a missed
/// preload, which costs a run-time pull, never a failure.
pub fn scan_workflow_images(workspace: &Path) -> Vec<String> {
    let mut images = BTreeSet::new();
    let Ok(entries) = fs::read_dir(workspace.join(".github").join("workflows")) else {
        return Vec::new();
    };
    for entry in entries.flatten() {
        let path = entry.path();
        // Reject symlinks and non-regular files to prevent path traversal
        // and reads from device nodes (e.g. /dev/zero).
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_file() {
            continue;
        }
        if !matches!(
            path.extension().and_then(|ext| ext.to_str()),
            Some("yml" | "yaml")
        ) {
            continue;
        }
        let Ok(meta) = path.metadata() else {
            continue;
        };
        if meta.len() > 2 * 1024 * 1024 {
            continue; // skip files > 2 MB
        }
        let Ok(text) = fs::read_to_string(&path) else {
            continue;
        };
        let Ok(doc) = serde_yaml::from_str::<Value>(&text) else {
            continue;
        };
        let Some(jobs) = doc.get("jobs").and_then(Value::as_object) else {
            continue;
        };
        for job in jobs.values() {
            if let Some(image) = job.get("container").and_then(oci_image_ref) {
                images.insert(image);
            }
            if let Some(services) = job.get("services").and_then(Value::as_object) {
                images.extend(services.values().filter_map(oci_image_ref));
            }
        }
    }
    images.into_iter().collect()
}

/// The sentinel naming the official packed golden.
///
/// `runs-on: ubuntu-latest`/`ubuntu-24.04` resolve to this, and so does a pool
/// with no image configured. It is deliberately not an OCI reference: the
/// official golden is fetched as a packed `.smolmachine` (per architecture,
/// digest-pinned), never built from a base image, and the sentinel keeps that
/// distinct from "an operator pointed us at an image".
pub const OFFICIAL_GOLDEN: &str = "preloop-official-golden";

/// Whether a resolved base image names the official packed golden.
pub fn is_official_golden(base: &str) -> bool {
    base == OFFICIAL_GOLDEN
}

/// Resolved base image for one job: a golden source plus its fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentSpec {
    /// [`OFFICIAL_GOLDEN`] or the configured image reference.
    pub base: String,
    /// SHA-256 hex digest of everything the golden for this base depends on.
    pub fingerprint: String,
}

impl EnvironmentSpec {
    /// Resolve the environment for a base image.
    ///
    /// The image is used exactly as it is: no packages, toolchains, PATH or
    /// environment overrides. Workflow `setup-*` actions select exact language
    /// versions at job time, as they do on GitHub-hosted runners.
    ///
    /// The runner account comes from `PRELOOP_RUNNER_USER`/`_UID` — the same
    /// variables `serve` resolves it from — because a configured image bakes
    /// that account into its golden: a host that changes either must not adopt
    /// the artifact baked for the previous one. Reading it here (rather than
    /// only at the bake) is what keeps every caller, `preloop golden-path`
    /// included, naming the same payload.
    pub fn for_base(base: String) -> Self {
        Self::from_base(base)
    }

    /// Resolve the environment for a base image under an explicit account,
    /// for callers that already resolved one.
    pub fn for_base_with_account(base: String, user: &str, uid: u32) -> Self {
        Self::from_base_with_account(base, user, uid)
    }

    /// Resolve a queued job's base image from its `runs-on` labels.
    ///
    /// Only a supported hosted Ubuntu label selects the official golden.
    /// Today that is `ubuntu-latest`/`ubuntu-24.04`. Everything else —
    /// `[self-hosted, preloop-cpane]`, `[self-hosted, runner-sync]`, any
    /// private label set, and the newer `ubuntu-26.04` image — keeps
    /// `configured`, the image this pool was built around.
    ///
    /// The label set used to resolve to the stock `mirror.gcr.io/library/ubuntu`
    /// pin, which was "curated" and therefore sent every slot into the hosted
    /// apt baseline against the live Ubuntu archive. That bake fails whenever
    /// the archive has moved past the pinned versions (observed:
    /// `libatk1.0-0t64`, clang-16/17/18, gcc-13/14 all unlocatable), so slots
    /// burned on doomed bakes and the pool served one runner instead of three
    /// while the configured golden sat ready and forkable. The official golden
    /// is the hosted image, so the mapping is now direct and cannot fail that
    /// way.
    ///
    /// `ubuntu-22.04` maps nowhere: the archive baseline that used to back it
    /// was part of the deleted stock bake, so the label keeps `configured`
    /// instead of silently selecting a 24.04 image.
    pub fn base_for_labels(runs_on: &[String], configured: &str) -> String {
        if runs_on.iter().any(|label| {
            let label = label.to_ascii_lowercase();
            label.contains("ubuntu-24.04") || label.contains("ubuntu-latest")
        }) {
            return OFFICIAL_GOLDEN.into();
        }
        configured.to_owned()
    }

    fn from_base(base: String) -> Self {
        let (user, uid) = resolve_runner_account();
        Self::from_base_with_account(base, &user, uid)
    }

    fn from_base_with_account(base: String, user: &str, uid: u32) -> Self {
        // These are the launch-time switches `SmolVmProvider::create` resolves
        // for a pack machine. Adoption reuses an already-created golden
        // without running create, so they belong in the identity: a pool that
        // changes the workload or the guest-local volumes must not adopt the
        // machine built with the previous ones.
        let configured_guest_volumes = std::env::var(SMOLVM_GUEST_VOLUMES_ENV).ok();
        let configured_workload_init = std::env::var(SMOLVM_WORKLOAD_INIT_ENV).ok();
        Self::from_base_with_launch(
            base,
            user,
            uid,
            configured_guest_volumes.as_deref(),
            configured_workload_init.as_deref(),
        )
    }

    /// Resolve the fingerprint with explicit launch switches, so tests (and
    /// callers that must not read the ambient environment) can pin them.
    fn from_base_with_launch(
        base: String,
        user: &str,
        uid: u32,
        configured_guest_volumes: Option<&str>,
        configured_workload_init: Option<&str>,
    ) -> Self {
        let normalized = serde_json::json!({
            "base": &base,
            // The pool only rebuilds when the fingerprint-suffixed artifact
            // file is missing, so bake-content changes MUST invalidate the
            // fingerprint or the pool silently keeps the old golden forever.
            //
            // The official golden is built by the release pipeline from a
            // published runner image, not here: its identity is whatever
            // source this host would fetch it from — the digest pinned in
            // `official_golden_source`, a `PRELOOP_GOLDEN_OCI_REF` override,
            // or the `PRELOOP_GOLDEN_URL` mirror, which selects a different
            // pack with a different name. A configured image is baked
            // locally, so its fingerprint is the bake contract itself — bump
            // a step of `golden_contract_script` and every golden is rebuilt.
            "golden": if is_official_golden(&base) {
                crate::official_golden_source()
            } else {
                // Includes the provenance-aware residue policy: a custom base
                // preserves `/etc/hosts` and `/etc/fstab`, and toggling the
                // opt-in changes this text and therefore the fingerprint.
                crate::golden_contract_for_base(&base, user, uid)
            },
            // Rosetta x86_64 translation exists only on Apple Silicon hosts.
            // The golden prep installs the amd64 loader + libc so dynamically
            // linked x86_64 binaries run under it; the fingerprint records
            // whether that shim is present so a host-class change re-preps the
            // golden once instead of adopting a base built for the other class.
            "rosetta_libs": cfg!(target_os = "macos") && std::env::consts::ARCH == "aarch64",
            // The golden's runner resolves node through the bundle mount at
            // the pins it was built against. A pin bump that left the
            // fingerprint untouched would keep a golden whose runner demands
            // the previous version, so every JS action step fails with
            // `bundled nodeXX is missing` against a bundle that is itself
            // perfectly valid at the new pin.
            "node_externals": crate::node_externals::expected_runtimes()
                .iter()
                .map(|(runtime, version)| format!("{runtime}={version}"))
                .collect::<Vec<_>>(),
            // Resolved, not raw: the identity must move when the effective
            // launch differs, including the explicit empty values that
            // disable a default. `None` and an override naming the default
            // resolve identically and deliberately share an identity.
            "launch": launch_identity(
                &base,
                configured_guest_volumes,
                configured_workload_init,
            ),
        });
        let bytes =
            serde_json::to_vec(&normalized).expect("normalized environment is serializable");
        let fingerprint = hex_digest(&bytes);
        Self { base, fingerprint }
    }
}

/// The runner account this host would run jobs as.
///
/// Mirrors the resolution `serve` performs: `PRELOOP_RUNNER_USER` (an empty
/// value disables the switch, i.e. the guest's own user runs the runner) and
/// `PRELOOP_RUNNER_UID`, defaulting to the hosted `runner`/1001. Kept here so
/// the fingerprint — and therefore `preloop golden-path` — always names the
/// account the bake will actually install.
fn resolve_runner_account() -> (String, u32) {
    let user = match std::env::var("PRELOOP_RUNNER_USER") {
        Ok(value) if value.is_empty() => crate::DEFAULT_RUNNER_USER.to_owned(),
        Ok(value) => value,
        Err(_) => crate::DEFAULT_RUNNER_USER.to_owned(),
    };
    let uid = std::env::var("PRELOOP_RUNNER_UID")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(crate::DEFAULT_RUNNER_UID);
    (user, uid)
}

/// The launch settings a runtime golden is created with, resolved exactly as
/// [`SmolVmProvider`](preloop_vm::SmolVmProvider)'s `create` resolves them.
///
/// The artifact pack itself does not depend on these: a workload override or a
/// guest-local volume is applied to the machine created from the pack. They
/// still belong in the environment identity because adoption skips `create`
/// entirely, so the recorded fingerprint is the only thing that can tell the
/// pool its previous machine has the wrong PID 1 or the wrong mounts.
///
/// An invalid override is represented by its error text rather than panicking:
/// the pool then fails later in `create` with the same validation error, and a
/// golden created before the typo is not adopted in the meantime.
fn launch_identity(
    base: &str,
    configured_guest_volumes: Option<&str>,
    configured_workload_init: Option<&str>,
) -> Value {
    // Every runtime golden on a file-pack backend is a pack. The official
    // golden is built for the managed layout; a custom base keeps its packed
    // container state and its own entrypoint unless the operator opts in
    // through an override.
    let role = if is_official_golden(base) {
        MachineRole::OfficialPack
    } else {
        MachineRole::CustomPack
    };
    let guest_volumes =
        match MachineSpec::resolve_guest_volumes(None, configured_guest_volumes, role) {
            Ok(volumes) => volumes
                .into_iter()
                .map(|volume| Value::String(volume.as_path().display().to_string()))
                .collect(),
            Err(error) => vec![Value::String(format!("invalid: {error}"))],
        };
    let workload_init = MachineSpec::resolve_workload_init(None, configured_workload_init, role)
        .map(|command| Value::Array(command.into_iter().map(Value::String).collect::<Vec<_>>()))
        .unwrap_or(Value::Null);
    serde_json::json!({
        "guest_volumes": guest_volumes,
        "workload_init": workload_init,
    })
}

fn hex_digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hosted labels select the official golden; a self-hosted label set keeps
    /// the pool's own image. The last case is the production wedge described
    /// on `base_for_labels`: resolving `[self-hosted, …]` to a stock Ubuntu pin
    /// sent every slot into a curated apt bake that the rolling archive can no
    /// longer satisfy.
    #[test]
    fn labels_select_the_official_golden_and_otherwise_keep_the_configured_image() {
        const CONFIGURED: &str = "ghcr.io/acme/runner-image:ubuntu24";
        assert_eq!(
            EnvironmentSpec::base_for_labels(&["ubuntu-latest".into()], CONFIGURED),
            OFFICIAL_GOLDEN
        );
        assert_eq!(
            EnvironmentSpec::base_for_labels(&["ubuntu-24.04".into()], CONFIGURED),
            OFFICIAL_GOLDEN
        );
        // No image configured: every label resolves to the official golden.
        assert_eq!(
            EnvironmentSpec::base_for_labels(&["self-hosted".into()], OFFICIAL_GOLDEN),
            OFFICIAL_GOLDEN
        );
        for labels in [
            vec!["self-hosted".to_owned()],
            vec!["self-hosted".to_owned(), "preloop-cpane".to_owned()],
            vec!["self-hosted".to_owned(), "runner-sync".to_owned()],
            // The 22.04 archive baseline went with the stock bake: the label
            // must keep the configured image rather than select a 24.04 one.
            vec!["ubuntu-22.04".to_owned()],
            // GitHub offers 26.04, but preloop has no published 26.04
            // official golden yet; do not silently rebase a configured pool
            // onto the 24.04 pack.
            vec!["ubuntu-26.04".to_owned()],
        ] {
            assert_eq!(
                EnvironmentSpec::base_for_labels(&labels, CONFIGURED),
                CONFIGURED,
                "{labels:?} must not be rebased onto another image"
            );
        }
    }

    #[test]
    fn official_golden_is_the_only_sentinel() {
        assert!(is_official_golden(OFFICIAL_GOLDEN));
        for image in [
            "official",
            "preloop-official-golden@sha256:abc",
            "ghcr.io/preloopdev/preloop-arm64-smolvm-golden@sha256:abc",
            "ubuntu:24.04",
        ] {
            assert!(!is_official_golden(image), "{image}");
        }
    }

    /// Two configured images must not share a fingerprint, and the same image
    /// must resolve to the same fingerprint on every call.
    #[test]
    fn configured_images_fingerprint_deterministically_per_image() {
        let first = EnvironmentSpec::for_base("ghcr.io/acme/runner:latest".into());
        let second = EnvironmentSpec::for_base("ghcr.io/acme/runner:latest".into());
        let other = EnvironmentSpec::for_base("ghcr.io/acme/runner:2".into());
        assert_eq!(first, second);
        assert_ne!(first.fingerprint, other.fingerprint);
        assert_eq!(first.base, "ghcr.io/acme/runner:latest");
    }

    /// The artifact filename is keyed by this fingerprint, and the artifact's
    /// baked runner resolves node at the pins it was compiled against. A pin
    /// bump must therefore invalidate the fingerprint, or the pool keeps
    /// serving an artifact whose runner demands a version no longer shipped.
    #[test]
    fn fingerprint_covers_the_node_externals_pins() {
        let base = "ghcr.io/acme/runner:latest";
        let spec = EnvironmentSpec::from_base_with_launch(
            base.to_owned(),
            crate::DEFAULT_RUNNER_USER,
            crate::DEFAULT_RUNNER_UID,
            None,
            None,
        );
        let pins: Vec<String> = crate::node_externals::expected_runtimes()
            .iter()
            .map(|(runtime, version)| format!("{runtime}={version}"))
            .collect();
        assert!(!pins.is_empty(), "node pins must be compiled in");

        // Recompute the digest with a bumped pin: it must not collide.
        let bumped: Vec<String> = pins.iter().map(|pin| format!("{pin}-bumped")).collect();
        let normalized = serde_json::json!({
            "base": "ghcr.io/acme/runner:latest",
            "golden": crate::golden_contract_for_base(
                "ghcr.io/acme/runner:latest",
                crate::DEFAULT_RUNNER_USER,
                crate::DEFAULT_RUNNER_UID,
            ),
            "rosetta_libs": cfg!(target_os = "macos") && std::env::consts::ARCH == "aarch64",
            "node_externals": bumped,
            "launch": {
                "guest_volumes": Vec::<String>::new(),
                "workload_init": null,
            },
        });
        let bumped_fingerprint = hex_digest(&serde_json::to_vec(&normalized).unwrap());
        assert_ne!(
            spec.fingerprint, bumped_fingerprint,
            "a node pin bump must change the fingerprint"
        );
    }

    /// The runner account is part of a configured image's bake contract: the
    /// contract text embeds it, so changing `PRELOOP_RUNNER_USER`/`_UID` must
    /// produce a different artifact name instead of adopting the golden baked
    /// for the previous account.
    #[test]
    fn fingerprint_tracks_the_configured_runner_account() {
        let base = "ghcr.io/acme/runner:latest".to_owned();
        let default = EnvironmentSpec::for_base_with_account(
            base.clone(),
            crate::DEFAULT_RUNNER_USER,
            crate::DEFAULT_RUNNER_UID,
        );
        let other_user = EnvironmentSpec::for_base_with_account(base.clone(), "builder", 1001);
        let other_uid = EnvironmentSpec::for_base_with_account(base, "runner", 2000);

        assert_ne!(default.fingerprint, other_user.fingerprint);
        assert_ne!(default.fingerprint, other_uid.fingerprint);
        assert_ne!(other_user.fingerprint, other_uid.fingerprint);
        assert_eq!(default.base, "ghcr.io/acme/runner:latest");
    }

    /// Launch switches are applied when the golden machine is created, but
    /// adoption reuses it without running create. The identity must therefore
    /// move on every effective workload or guest-volume change, including the
    /// explicit empty values that disable a default.
    #[test]
    fn fingerprint_tracks_resolved_launch_settings() {
        let base = "ghcr.io/acme/runner:latest".to_owned();
        let launched = |guest_volumes, workload_init| {
            EnvironmentSpec::from_base_with_launch(
                base.clone(),
                crate::DEFAULT_RUNNER_USER,
                crate::DEFAULT_RUNNER_UID,
                guest_volumes,
                workload_init,
            )
        };
        let default = launched(None, None);
        // A custom base already defaults to no guest volumes and its own
        // entrypoint, so the empty overrides are the same effective launch,
        // not a new identity.
        assert_eq!(default.fingerprint, launched(Some(""), None).fingerprint);
        assert_eq!(default.fingerprint, launched(None, Some("")).fingerprint);
        for (name, other) in [
            ("a guest-volume list", launched(Some("/srv/state"), None)),
            (
                "a named workload",
                launched(None, Some("/lib/systemd/systemd")),
            ),
        ] {
            assert_ne!(
                default.fingerprint, other.fingerprint,
                "{name} must change the golden identity"
            );
        }

        // Naming the effective defaults explicitly is the same launch.
        let official_default = EnvironmentSpec::from_base_with_launch(
            OFFICIAL_GOLDEN.to_owned(),
            crate::DEFAULT_RUNNER_USER,
            crate::DEFAULT_RUNNER_UID,
            None,
            None,
        );
        let official_explicit = EnvironmentSpec::from_base_with_launch(
            OFFICIAL_GOLDEN.to_owned(),
            crate::DEFAULT_RUNNER_USER,
            crate::DEFAULT_RUNNER_UID,
            Some("/var/lib/docker,/var/lib/containerd"),
            Some("/sbin/init"),
        );
        assert_eq!(
            official_default.fingerprint, official_explicit.fingerprint,
            "an override naming the defaults is the same effective launch"
        );
        let official_disabled = EnvironmentSpec::from_base_with_launch(
            OFFICIAL_GOLDEN.to_owned(),
            crate::DEFAULT_RUNNER_USER,
            crate::DEFAULT_RUNNER_UID,
            Some(""),
            Some(""),
        );
        assert_ne!(
            official_default.fingerprint, official_disabled.fingerprint,
            "disabling the official layout must not adopt the enabled golden"
        );
        // The published pack is not re-baked for an account: only a
        // configured image embeds the runner account in its bake contract.
        let official_other_account = EnvironmentSpec::from_base_with_launch(
            OFFICIAL_GOLDEN.to_owned(),
            "builder",
            2000,
            None,
            None,
        );
        assert_eq!(
            official_default.fingerprint, official_other_account.fingerprint,
            "the official pin keeps its account-independent behavior"
        );
    }

    /// The official golden is keyed by *where this host fetches it from*: the
    /// pinned digest, the `PRELOOP_GOLDEN_OCI_REF` override, or the mirror URL.
    /// A changed mirror must not keep serving the pack the previous one left
    /// at the payload path, and it must not be keyed on the bake contract
    /// either (the release pipeline bakes it, so a contract edit cannot move
    /// the published artifact).
    #[test]
    fn fingerprint_tracks_the_official_golden_source() {
        let official = EnvironmentSpec::from_base_with_launch(
            OFFICIAL_GOLDEN.to_owned(),
            crate::DEFAULT_RUNNER_USER,
            crate::DEFAULT_RUNNER_UID,
            None,
            None,
        );
        let oci_source = format!("oci:{}", crate::official_golden_reference());
        let expected = |golden: &str| {
            let normalized = serde_json::json!({
                "base": OFFICIAL_GOLDEN,
                "golden": golden,
                "rosetta_libs": cfg!(target_os = "macos") && std::env::consts::ARCH == "aarch64",
                "node_externals": crate::node_externals::expected_runtimes()
                    .iter()
                    .map(|(runtime, version)| format!("{runtime}={version}"))
                    .collect::<Vec<_>>(),
                "launch": {
                    "guest_volumes": ["/var/lib/docker", "/var/lib/containerd"],
                    "workload_init": ["/sbin/init"],
                },
            });
            hex_digest(&serde_json::to_vec(&normalized).unwrap())
        };

        assert_eq!(official.fingerprint, expected(&oci_source));
        assert_ne!(
            official.fingerprint,
            expected("https://mirror.example/new-pack.smolmachine"),
            "a mirror switch must not reuse the pack the previous source left behind"
        );
        assert_ne!(
            official.fingerprint,
            expected(&crate::golden_contract_for_base(
                OFFICIAL_GOLDEN,
                crate::DEFAULT_RUNNER_USER,
                crate::DEFAULT_RUNNER_UID,
            )),
            "the official golden is published, not baked here"
        );
        // The in-guest bake a non-pack backend performs boots this image, so
        // it must be a real digest-pinned reference — the sentinel itself is
        // not an image any backend can start.
        match crate::official_boot_image() {
            Some(image) => assert!(
                image.contains("@sha256:"),
                "a non-pack backend can only boot a digest-pinned image: {image}"
            ),
            None => assert!(
                !matches!(std::env::consts::ARCH, "aarch64" | "x86_64"),
                "every supported architecture must have a pinned runner image"
            ),
        }
    }

    #[test]
    fn workflow_scan_reads_container_images_from_yaml() {
        let root = std::env::temp_dir().join(format!("env-scan-{}", std::process::id()));
        let workflows = root.join(".github/workflows");
        std::fs::create_dir_all(&workflows).unwrap();
        std::fs::write(
            workflows.join("ci.yml"),
            "jobs:\n  test:\n    runs-on: ubuntu-latest\n    container:\n      image: node:22\n    services:\n      db:\n        image: postgres:16\n",
        )
        .unwrap();
        std::fs::write(workflows.join("broken.yml"), "jobs: [: not yaml").unwrap();
        let images = scan_workflow_images(&root);
        assert_eq!(images, vec!["node:22".to_owned(), "postgres:16".to_owned()]);
        let _ = std::fs::remove_dir_all(&root);
    }
}
