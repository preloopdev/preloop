//! Toolchain and base-image resolution for disposable job environments.

use preloop_gha_protocol::oci_image_ref;
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

/// A toolchain that must be available in a job VM.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ToolchainLayer {
    /// Node.js release (for example, `22`, `20.11.0`, or `lts/*`).
    Node(String),
    /// Rust channel or release (for example, `stable`, `nightly`, or `1.85.1`).
    Rust(String),
    /// Python release (for example, `3.12` or `3.11.8`).
    Python(String),
    /// Go release (for example, `1.24` or `1.23.4`).
    Go(String),
}

impl std::fmt::Display for ToolchainLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Node(version) => write!(f, "node {version}"),
            Self::Rust(channel) => write!(f, "rust {channel}"),
            Self::Python(version) => write!(f, "python {version}"),
            Self::Go(version) => write!(f, "go {version}"),
        }
    }
}

impl ToolchainLayer {
    /// Return shell commands to install this toolchain in a SmolVM.
    ///
    /// Commands are represented as argv vectors, as expected by
    /// [`preloop_vm::VmProvider::exec`]. Commands requiring a pipe are run
    /// through `sh -c` so that the returned vectors remain valid argv.
    pub fn install_commands(&self) -> Vec<Vec<String>> {
        match self {
            Self::Node(version) => {
                // Exact-version tarball install, not the apt series: a workflow
                // pinning `22.23.1` gets exactly that, and a major (`22`) or
                // `lts/*` request resolves against the nodejs.org release
                // index at bake time (GitHub's setup-node resolves the same
                // way, so this matches hosted behavior instead of floating
                // with the apt archive).
                // The version is interpolated into a `sh -c` script, so it
                // must be allowlisted (same `safe_component` the Rust and Go
                // layers use): a workflow-controlled value carrying shell
                // metacharacters would execute arbitrary commands in the
                // provisioning VM.
                let version = safe_component(version.trim().trim_start_matches('v'));
                vec![
                    vec![
                        "sh".into(),
                        "-c".into(),
                        format!(
                            "set -e\n\
                             WANT=v{version}\n\
                             case '{version}' in lts/*) WANT='lts/*' ;; esac\n\
                             VERSION=$(curl -fsSL https://nodejs.org/dist/index.json | python3 -c '\n\
                             import json, sys\n\
                             want = sys.argv[1]\n\
                             idx = json.load(sys.stdin)\n\
                             print(next((e[\"version\"] for e in idx if (want == \"lts/*\" and e[\"lts\"]) or e[\"version\"] == want or e[\"version\"].startswith(want + \".\")), \"\"))\n\
                             ' \"$WANT\")\n\
                             [ -n \"$VERSION\" ] || {{ echo \"no node release matching {version}\" >&2; exit 1; }}\n\
                             arch=$(uname -m)\n\
                             case \"$arch\" in\n\
                               x86_64) NODE_ARCH=x64 ;;\n\
                               aarch64|arm64) NODE_ARCH=arm64 ;;\n\
                               *) echo \"unsupported arch: $arch\" >&2; exit 1 ;;\n\
                             esac\n\
                             curl -fsSL \"https://nodejs.org/dist/$VERSION/node-$VERSION-linux-$NODE_ARCH.tar.gz\" \\\n\
                               | tar -xz --strip-components=1 -C /usr/local\n\
                             node --version"
                        ),
                    ],
                ]
            }
            Self::Rust(channel) => vec![
                vec![
                    "sh".into(),
                    "-c".into(),
                    format!(
                        // Pin the rustup installer itself (the `sh.rustup.rs`
                        // wrapper floats with every rustup release). The
                        // channel stays workflow-driven — `stable` resolves
                        // at bake time exactly as GitHub resolves it at job
                        // time — and the resolved version is recorded in the
                        // golden's bake manifest.
                        "set -e\n\
                         arch=$(uname -m)\n\
                         case \"$arch\" in\n\
                           x86_64) RUST_ARCH=x86_64 ;;\n\
                           aarch64|arm64) RUST_ARCH=aarch64 ;;\n\
                           *) echo \"unsupported arch: $arch\" >&2; exit 1 ;;\n\
                         esac\n\
                         curl -fsSL \"https://static.rust-lang.org/rustup/archive/{}/$RUST_ARCH-unknown-linux-gnu/rustup-init\" -o /tmp/rustup-init\n\
                         chmod +x /tmp/rustup-init\n\
                         /tmp/rustup-init -y --profile minimal --default-toolchain {}\n\
                         rm -f /tmp/rustup-init",
                        crate::RUSTUP_VERSION,
                        safe_component(channel)
                    ),
                ],
                vec![
                    "sh".into(),
                    "-c".into(),
                    // Run steps execute with `bash --noprofile --norc`, so
                    // profile.d PATH exports are never sourced. Symlink the
                    // cargo binaries into /usr/local/bin so they are on the
                    // default system PATH for every step shell.
                    "ln -sf $HOME/.cargo/bin/cargo /usr/local/bin/cargo; ln -sf $HOME/.cargo/bin/rustc /usr/local/bin/rustc; ln -sf $HOME/.cargo/bin/rustup /usr/local/bin/rustup".into(),
                ],
            ],
            Self::Python(version) => {
                let package = format!("python{}", version.trim());
                vec![vec![
                    "apt-get".into(),
                    "install".into(),
                    "-y".into(),
                    package,
                    "python3-pip".into(),
                ]]
            }
            Self::Go(version) => vec![
                vec![
                    "sh".into(),
                    "-c".into(),
                    format!(
                        // `go.mod` carries a minimum (`go 1.24`), not a tarball
                        // version — resolve it against the go.dev release index
                        // so `go1.24` becomes the newest 1.24.x and the install
                        // is exact and reproducible.
                        "set -e\n\
                         WANT='{}'\n\
                         VERSION=$(curl -fsSL 'https://go.dev/dl/?mode=json&include=all' | python3 -c '\n\
                         import json, sys\n\
                         want = sys.argv[1]\n\
                         if not want.startswith(\"go\"):\n    \
                             want = \"go\" + want\n\
                         idx = json.load(sys.stdin)\n\
                         print(next((e[\"version\"] for e in idx if e[\"version\"] == want or e[\"version\"].startswith(want + \".\")), \"\"))\n\
                         ' \"$WANT\")\n\
                         [ -n \"$VERSION\" ] || {{ echo \"no go release matching $WANT\" >&2; exit 1; }}\n\
                         arch=$(uname -m)\n\
                         case \"$arch\" in aarch64) arch=arm64 ;; x86_64) arch=amd64 ;; esac\n\
                         curl -fsSL \"https://go.dev/dl/$VERSION.linux-$arch.tar.gz\" | tar -C /usr/local -xzf -",
                        safe_component(version)
                    ),
                ],
                vec![
                    "sh".into(),
                    "-c".into(),
                    // The Go tarball extracts to /usr/local/go/bin, which is
                    // not on the default system PATH. Run steps execute with
                    // `bash --noprofile --norc`, so profile.d PATH exports
                    // are never sourced; symlink the binaries into
                    // /usr/local/bin like the Rust layer does for cargo.
                    "ln -sf /usr/local/go/bin/go /usr/local/bin/go; ln -sf /usr/local/go/bin/gofmt /usr/local/bin/gofmt".into(),
                ],
            ],
        }
    }

    /// Binary that must exist on the default PATH once this layer is
    /// installed. Verified after install so a provision interrupted mid-way
    /// (or a toolchain that silently failed) fails the machine instead of
    /// running the job without the tool it asked for.
    pub fn verify_binary(&self) -> &'static str {
        match self {
            Self::Node(_) => "node",
            Self::Rust(_) => "cargo",
            Self::Python(_) => "python3",
            Self::Go(_) => "go",
        }
    }
}

/// The fixed set of toolchains baked into every golden.
///
/// Deliberately not workspace-derived. The base install script already bakes
/// the GitHub-hosted parity toolset (node/python/go toolcaches, git, git-lfs,
/// docker, nvm, yarn — see `base_install_script`), so per-project version
/// files add nothing there. Rust is the one toolchain the base bake lacks,
/// so it is baked for everyone. `setup-*` actions download any other version
/// a job asks for at job time — the same model GitHub-hosted runners use.
pub fn curated_toolchains() -> Vec<ToolchainLayer> {
    vec![ToolchainLayer::Rust("stable".into())]
}

/// Digest-pinned Ubuntu base images.
///
/// The floating tags (`ubuntu:24.04`) move whenever Canonical publishes a
/// point release, so two goldens baked a month apart would differ for no
/// reason anyone recorded. These pins are the provenance: bumping one is a
/// deliberate, reviewable change. Digests are the registry manifest-list
/// digests, valid for both x86_64 and arm64 guests.
/// Digest-pinned base images, declared in `versions.toml` (build.rs compiles
/// the pins into constants — see `UBUNTU_24_04_BASE`/`UBUNTU_22_04_BASE`).
pub const UBUNTU_24_04_PIN: &str = crate::UBUNTU_24_04_BASE;
pub const UBUNTU_22_04_PIN: &str = crate::UBUNTU_22_04_BASE;

/// The default base image for GitHub-runner-labelled jobs.
pub const DEFAULT_BASE_IMAGE: &str = UBUNTU_24_04_PIN;

/// The plain repository:tag of an image reference, ignoring any `@digest`.
pub fn base_name(image_ref: &str) -> &str {
    image_ref.split('@').next().unwrap_or(image_ref)
}

/// Resolved base image and toolchains for one job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentSpec {
    /// Base OCI image or VM image identifier.
    pub base: String,
    /// Sorted, deduplicated toolchain layers.
    pub toolchains: Vec<ToolchainLayer>,
    /// SHA-256 hex digest of the normalized base and toolchain list.
    pub fingerprint: String,
}

impl EnvironmentSpec {
    /// Build a normalized environment specification and compute its fingerprint.
    pub fn new(base: String, toolchains: Vec<ToolchainLayer>) -> Self {
        Self::from_parts(base, toolchains)
    }

    /// Replace the base image, recomputing the fingerprint.
    pub fn with_base(mut self, base: String) -> Self {
        self.base = base;
        Self::from_parts(self.base.clone(), self.toolchains.clone())
    }

    /// Select the default Ubuntu image from GitHub runner labels.
    pub fn default_base(runs_on: &[String]) -> String {
        if runs_on.iter().any(|label| {
            let label = label.to_ascii_lowercase();
            label.contains("ubuntu-24.04") || label.contains("ubuntu-latest")
        }) {
            return UBUNTU_24_04_PIN.into();
        }
        if runs_on
            .iter()
            .any(|label| label.to_ascii_lowercase().contains("ubuntu-22.04"))
        {
            return UBUNTU_22_04_PIN.into();
        }
        UBUNTU_24_04_PIN.into()
    }

    fn from_parts(base: String, mut toolchains: Vec<ToolchainLayer>) -> Self {
        toolchains.sort();
        toolchains.dedup();
        let normalized = serde_json::json!({
            "base": &base,
            "toolchains": &toolchains,
            // The pool only rebuilds when the fingerprint-suffixed artifact
            // file is missing, so bake-content changes MUST invalidate the
            // fingerprint or the pool silently keeps the old golden forever
            // (packages, resolv.conf, nvm, tool pins all live in the bake
            // script, which interpolates the versions.toml pins).
            "bake": crate::base_install_script(),
        });
        let bytes =
            serde_json::to_vec(&normalized).expect("normalized environment is serializable");
        let fingerprint = hex_digest(&bytes);
        Self {
            base,
            toolchains,
            fingerprint,
        }
    }
}

fn safe_component(value: &str) -> String {
    value
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_' | '/' | '*')
        })
        .collect()
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
    use std::collections::BTreeSet;

    #[test]
    fn default_base_returns_digest_pinned_images() {
        assert_eq!(
            EnvironmentSpec::default_base(&["ubuntu-latest".into()]),
            UBUNTU_24_04_PIN
        );
        assert_eq!(
            EnvironmentSpec::default_base(&["ubuntu-24.04".into()]),
            UBUNTU_24_04_PIN
        );
        assert_eq!(
            EnvironmentSpec::default_base(&["ubuntu-22.04".into()]),
            UBUNTU_22_04_PIN
        );
        assert_eq!(
            EnvironmentSpec::default_base(&["self-hosted".into()]),
            UBUNTU_24_04_PIN
        );
    }

    #[test]
    fn base_name_strips_digest() {
        assert_eq!(base_name("ubuntu:24.04@sha256:abc"), "ubuntu:24.04");
        assert_eq!(base_name("ubuntu:24.04"), "ubuntu:24.04");
        assert_eq!(base_name(""), "");
    }

    #[test]
    fn node_layer_install_uses_pinned_tarball() {
        // The Node layer must never resolve through apt: exact versions are
        // installed verbatim and major/lts requests resolve via the nodejs.org
        // index, never the floating apt series.
        let commands = ToolchainLayer::Node("20.11.0".into()).install_commands();
        let script = commands[0].join(" ");
        assert!(script.contains("nodejs.org/dist/index.json"));
        assert!(script.contains("node-$VERSION-linux-$NODE_ARCH.tar.gz"));
        assert!(!script.contains("nodesource"));
        assert!(!script.contains("apt-get"));
    }

    #[test]
    fn node_layer_version_rejects_shell_metacharacters() {
        // A workflow-controlled `node-version` is interpolated into the
        // `sh -c` provisioning script (`WANT=v{version}`, the case pattern,
        // and the error message), so shell metacharacters would execute
        // arbitrary commands inside the bake VM. The version must be
        // allowlisted through the same `safe_component` used by the Rust and
        // Go layers.
        let payload = "22; touch /tmp/pwned; #";
        let commands = ToolchainLayer::Node(payload.into()).install_commands();
        let script = commands[0].join(" ");
        // The raw payload must not survive into the script...
        assert!(!script.contains("touch /tmp/pwned"));
        assert!(!script.contains("22;"));
        // ...and the sanitized version is still interpolated everywhere the
        // original was (WANT, case, error message), never silently dropped.
        assert!(script.contains("WANT=v22touch/tmp/pwned"));
        assert!(script.contains("no node release matching 22touch/tmp/pwned"));
    }

    #[test]
    fn rust_layer_install_uses_pinned_rustup() {
        let commands = ToolchainLayer::Rust("stable".into()).install_commands();
        let script = commands[0].join(" ");
        assert!(script.contains(&format!(
            "static.rust-lang.org/rustup/archive/{}",
            crate::RUSTUP_VERSION
        )));
        assert!(script.contains("--profile minimal"));
        assert!(script.contains("--default-toolchain stable"));
        assert!(!script.contains("sh.rustup.rs"));
    }

    #[test]
    fn go_layer_install_resolves_minimum_version() {
        let commands = ToolchainLayer::Go("1.24".into()).install_commands();
        let script = commands[0].join(" ");
        assert!(script.contains("go.dev/dl/?mode=json"));
        assert!(script.contains("$VERSION.linux"));
        assert!(!script.contains("go1.24.linux")); // never a raw minimum
    }

    #[test]
    fn go_layer_puts_binary_on_default_path() {
        // The Go tarball extracts to /usr/local/go/bin, which is not on the
        // default PATH of `bash --noprofile --norc` step shells (the same
        // problem the Rust layer's symlinks solve), so `go`/`gofmt` would be
        // unresolvable in job steps. The layer must link them into
        // /usr/local/bin.
        let commands = ToolchainLayer::Go("1.24".into()).install_commands();
        assert_eq!(commands.len(), 2);
        let script = commands[1].join(" ");
        assert!(script.contains("ln -sf /usr/local/go/bin/go /usr/local/bin/go"));
        assert!(script.contains("ln -sf /usr/local/go/bin/gofmt /usr/local/bin/gofmt"));
    }

    #[test]
    fn with_base_recomputes_fingerprint() {
        let spec = EnvironmentSpec::new("ubuntu:24.04".into(), curated_toolchains());
        let original_fingerprint = spec.fingerprint.clone();
        let rebased = spec.clone().with_base("ubuntu:22.04".into());
        assert_eq!(rebased.base, "ubuntu:22.04");
        assert_eq!(rebased.toolchains, spec.toolchains);
        assert_ne!(rebased.fingerprint, original_fingerprint);
    }

    #[test]
    fn curated_toolchains_is_fixed_and_deduped() {
        let first = curated_toolchains();
        let second = curated_toolchains();
        assert_eq!(first, second, "the curated set must be deterministic");
        assert_eq!(
            first.iter().collect::<BTreeSet<_>>().len(),
            first.len(),
            "no duplicate toolchains"
        );
        // Rust is the one toolchain the base bake does not cover, so it is
        // the deliberate member of the curated set.
        assert!(first.contains(&ToolchainLayer::Rust("stable".into())));
    }

    #[test]
    fn install_commands_are_non_empty() {
        for layer in [
            ToolchainLayer::Node("22".into()),
            ToolchainLayer::Rust("stable".into()),
            ToolchainLayer::Python("3.12".into()),
            ToolchainLayer::Go("1.24".into()),
        ] {
            assert!(!layer.install_commands().is_empty());
        }
    }

    /// The Go layer emits an inline Python resolver whose body must survive
    /// the Rust line-continuation string. A `\n\` continuation strips the
    /// leading whitespace of the following source line, which used to flatten
    /// the `if` body to column zero — the emitted script then died with
    /// `IndentationError` on every provisioning run and no runner ever
    /// registered. Run the actual emitted Python against a sample release
    /// index so a regression fails the suite instead of the pool.
    #[test]
    fn go_toolchain_python_resolver_is_valid_and_resolves() {
        let commands = ToolchainLayer::Go("1.24".into()).install_commands();
        let shell = &commands[0][2];
        assert!(
            shell.contains("python3 -c"),
            "resolver must be inline python"
        );

        // Extract the python -c program: everything between the single-quoted
        // `python3 -c '` and the closing `' "$WANT"`.
        let start = shell.find("python3 -c '").expect("inline python") + "python3 -c '".len();
        let end = shell[start..]
            .find("' \"$WANT\"")
            .map(|offset| start + offset)
            .expect("closing quote");
        let program = &shell[start..end];

        // The emitted shell wraps the python in its own quoting; decode the
        // `\\n` -> `\n` and `\"` -> `"` escapes the Rust string introduced.
        let program = program.replace("\\n", "\n").replace("\\\"", "\"");

        // The program reads JSON from stdin and prints the matching version.
        // go.dev serves the index newest-first, which is what makes `next`
        // resolve a `1.24` minimum to the newest 1.24.x.
        let index = r#"[{"version":"go1.25.0"},{"version":"go1.24.2"},{"version":"go1.24.1"},{"version":"go1.24.0"}]"#;
        let mut child = std::process::Command::new("python3")
            .arg("-c")
            .arg(&program)
            .arg("1.24")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("python3 available for the test");
        use std::io::Write;
        child
            .stdin
            .take()
            .expect("stdin")
            .write_all(index.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "emitted python must parse and run: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let resolved = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        assert_eq!(
            resolved, "go1.24.2",
            "a `go 1.24` minimum must resolve to the newest 1.24.x"
        );
    }
}
