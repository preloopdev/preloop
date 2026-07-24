//! Toolchain and base-image resolution for disposable job environments.

use aksh_gha_protocol::JobPlan;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

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

impl ToolchainLayer {
    /// Return shell commands to install this toolchain in a SmolVM.
    ///
    /// Commands are represented as argv vectors, as expected by
    /// [`preloop_vm::VmProvider::exec`]. Commands requiring a pipe are run
    /// through `sh -c` so that the returned vectors remain valid argv.
    pub fn install_commands(&self) -> Vec<Vec<String>> {
        match self {
            Self::Node(version) => {
                let major = version
                    .trim()
                    .strip_prefix('v')
                    .unwrap_or(version.trim())
                    .split('.')
                    .next()
                    .unwrap_or("22");
                vec![
                    vec![
                        "sh".into(),
                        "-c".into(),
                        format!(
                            "curl -fsSL https://deb.nodesource.com/setup_{major}.x | bash -"
                        ),
                    ],
                    vec!["apt-get".into(), "install".into(), "-y".into(), "nodejs".into()],
                ]
            }
            Self::Rust(channel) => vec![
                vec![
                    "sh".into(),
                    "-c".into(),
                    format!(
                        "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain {}",
                        safe_component(channel)
                    ),
                ],
                vec![
                    "sh".into(),
                    "-c".into(),
                    "printf '%s\\n' 'export PATH=\"$HOME/.cargo/bin:$PATH\"' > /etc/profile.d/rustup.sh".into(),
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
            Self::Go(version) => vec![vec![
                "sh".into(),
                "-c".into(),
                format!(
                    "arch=$(uname -m); case \"$arch\" in aarch64) arch=arm64 ;; x86_64) arch=amd64 ;; esac; curl -fsSL https://go.dev/dl/go{}.linux-$arch.tar.gz | tar -C /usr/local -xzf -",
                    safe_component(version)
                ),
            ]],
        }
    }
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

    /// Select the default Ubuntu image from GitHub runner labels.
    pub fn default_base(runs_on: &[String]) -> String {
        if runs_on.iter().any(|label| {
            let label = label.to_ascii_lowercase();
            label.contains("ubuntu-24.04") || label.contains("ubuntu-latest")
        }) {
            return "ubuntu:24.04".into();
        }
        if runs_on
            .iter()
            .any(|label| label.to_ascii_lowercase().contains("ubuntu-22.04"))
        {
            return "ubuntu:22.04".into();
        }
        "ubuntu:24.04".into()
    }

    fn from_parts(base: String, mut toolchains: Vec<ToolchainLayer>) -> Self {
        toolchains.sort();
        toolchains.dedup();
        let normalized = serde_json::json!({
            "base": &base,
            "toolchains": &toolchains,
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

/// Resolves VM base images and toolchain layers from a workflow job.
#[derive(Debug, Clone)]
pub struct EnvironmentResolver {
    default_base: String,
}

impl EnvironmentResolver {
    /// Construct a resolver using `default_base` when it is non-empty.
    pub fn new(default_base: String) -> Self {
        Self { default_base }
    }

    /// Resolve environment requirements from a job plan and optional workspace.
    pub fn resolve(&self, job: &JobPlan, workspace: Option<&Path>) -> EnvironmentSpec {
        let mut detected = BTreeSet::new();
        let mut explicit_node = false;
        let mut explicit_rust = false;
        let mut explicit_python = false;
        let mut explicit_go = false;

        for step in &job.steps {
            let Some(uses) = step.uses.as_deref() else {
                continue;
            };
            let Some((action, reference)) = uses.split_once('@') else {
                continue;
            };
            let action = action.to_ascii_lowercase();
            let reference = reference.trim();
            if action == "actions/setup-node" {
                if let Some(version) =
                    setup_input_version(&step.with, "node-version", "node-version-file", workspace)
                {
                    detected.insert(ToolchainLayer::Node(version));
                    explicit_node = true;
                }
            } else if action == "actions/setup-python" {
                if let Some(version) = setup_input_version(
                    &step.with,
                    "python-version",
                    "python-version-file",
                    workspace,
                ) {
                    detected.insert(ToolchainLayer::Python(version));
                    explicit_python = true;
                }
            } else if action == "actions/setup-go" {
                if let Some(version) =
                    setup_input_version(&step.with, "go-version", "go-version-file", workspace)
                {
                    detected.insert(ToolchainLayer::Go(version));
                    explicit_go = true;
                }
            } else if action == "dtolnay/rust-toolchain" {
                if !reference.is_empty() {
                    detected.insert(ToolchainLayer::Rust(reference.to_owned()));
                    explicit_rust = true;
                }
            } else if action == "actions-rust-lang/setup-rust-toolchain" {
                if let Some(version) = value_string(step.with.get("toolchain")) {
                    detected.insert(ToolchainLayer::Rust(version));
                    explicit_rust = true;
                }
            }
        }

        if let Some(workspace) = workspace {
            if !explicit_node {
                if let Some(version) = first_existing_line(workspace, &[".nvmrc", ".node-version"])
                {
                    detected.insert(ToolchainLayer::Node(strip_node_prefix(version)));
                }
            }
            if !explicit_rust {
                let version = read_rust_toolchain_toml(workspace)
                    .or_else(|| first_existing_line(workspace, &["rust-toolchain"]));
                if let Some(version) = version {
                    detected.insert(ToolchainLayer::Rust(version));
                }
            }
            if !explicit_python {
                if let Some(version) = first_existing_line(workspace, &[".python-version"]) {
                    detected.insert(ToolchainLayer::Python(version));
                }
            }
            if !explicit_go {
                if let Some(version) = read_go_mod(workspace) {
                    detected.insert(ToolchainLayer::Go(version));
                }
            }
        }

        let base = if self.default_base.trim().is_empty() {
            EnvironmentSpec::default_base(&job.runs_on)
        } else {
            self.default_base.clone()
        };
        EnvironmentSpec::new(base, detected.into_iter().collect())
    }
}

fn value_string(value: Option<&Value>) -> Option<String> {
    let value = value?;
    let value = match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        _ => return None,
    };
    let value = value.trim().to_owned();
    (!value.is_empty()).then_some(value)
}

fn setup_input_version(
    inputs: &std::collections::BTreeMap<String, Value>,
    direct_key: &str,
    file_key: &str,
    workspace: Option<&Path>,
) -> Option<String> {
    if let Some(version) = value_string(inputs.get(direct_key)) {
        return Some(version);
    }
    let file = value_string(inputs.get(file_key))?;
    let workspace = workspace?;
    let path = workspace.join(file);
    // go.mod requires special parsing to extract the `go X.Y` directive.
    if direct_key == "go-version" && path.file_name().is_some_and(|n| n == "go.mod") {
        return read_go_mod(workspace);
    }
    first_line(&path).map(|version| {
        if direct_key == "node-version" {
            strip_node_prefix(version)
        } else {
            version
        }
    })
}

fn first_existing_line(workspace: &Path, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| first_line(&workspace.join(name)))
}

fn first_line(path: &Path) -> Option<String> {
    fs::read_to_string(path)
        .ok()?
        .lines()
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
}

fn strip_node_prefix(version: String) -> String {
    version
        .strip_prefix('v')
        .unwrap_or(&version)
        .trim()
        .to_owned()
}

fn read_rust_toolchain_toml(workspace: &Path) -> Option<String> {
    let content = fs::read_to_string(workspace.join("rust-toolchain.toml")).ok()?;
    let mut in_toolchain = false;
    for line in content.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_toolchain = line == "[toolchain]";
            continue;
        }
        if in_toolchain && line.starts_with("channel") {
            let value = line.split_once('=')?.1.trim();
            let value = value.split('#').next()?.trim();
            return Some(value.trim_matches(['"', '\'']).trim().to_owned());
        }
    }
    None
}

fn read_go_mod(workspace: &Path) -> Option<String> {
    let content = fs::read_to_string(workspace.join("go.mod")).ok()?;
    content.lines().find_map(|line| {
        let line = line.trim();
        let version = line.strip_prefix("go")?.split_whitespace().next()?;
        (!version.is_empty()).then(|| version.to_owned())
    })
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
    use aksh_gha_protocol::{JobId, StepPlan};
    use std::collections::{BTreeMap, BTreeSet};

    fn job(steps: Vec<StepPlan>, runs_on: Vec<String>) -> JobPlan {
        JobPlan {
            id: JobId("test".into()),
            base_id: "test".into(),
            name: "test".into(),
            runner_group: None,
            runs_on,
            needs: Vec::new(),
            matrix: Default::default(),
            env: BTreeMap::new(),
            steps,
            if_condition: None,
            fail_fast: true,
            continue_on_error: false,
            max_parallel: None,
            secrets_inherit: false,
            container: None,
            services: None,
            inputs: BTreeMap::new(),
            workflow_file: None,
            workflow_ref: None,
            workflow_sha: None,
            workflow_repository: None,
            secrets_map: BTreeMap::new(),
            job_outputs: BTreeMap::new(),
            oidc_id_token_granted: false,
            oidc_environment: None,
            oidc_job_workflow_ref: None,
            concurrency_group: None,
            concurrency_cancel_in_progress: None,
            concurrency_queue: None,
        }
    }

    fn setup(uses: &str, with: &[(&str, &str)]) -> StepPlan {
        StepPlan {
            id: None,
            name: None,
            run: None,
            uses: Some(uses.into()),
            env: BTreeMap::new(),
            with: with
                .iter()
                .map(|(key, value)| ((*key).into(), Value::String((*value).into())))
                .collect(),
            if_condition: None,
            working_directory: None,
            shell: None,
            continue_on_error: None,
        }
    }

    #[test]
    fn detects_setup_node() {
        let plan = job(
            vec![setup("Actions/Setup-Node@v4", &[("node-version", "22")])],
            vec!["ubuntu-latest".into()],
        );
        let spec = EnvironmentResolver::new(String::new()).resolve(&plan, None);
        assert_eq!(spec.toolchains, vec![ToolchainLayer::Node("22".into())]);
    }

    #[test]
    fn detects_rust_action_tag() {
        let plan = job(
            vec![setup("dtolnay/rust-toolchain@1.85.1", &[])],
            vec!["ubuntu-24.04".into()],
        );
        let spec = EnvironmentResolver::new(String::new()).resolve(&plan, None);
        assert_eq!(spec.toolchains, vec![ToolchainLayer::Rust("1.85.1".into())]);
    }

    #[test]
    fn falls_back_to_version_files() {
        let path =
            std::env::temp_dir().join(format!("preloop-environment-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join(".nvmrc"), "v20.11.0\n").unwrap();
        fs::write(
            path.join("rust-toolchain.toml"),
            "[toolchain]\nchannel = \"stable\"\n",
        )
        .unwrap();
        fs::write(path.join(".python-version"), "3.12\n").unwrap();
        fs::write(path.join("go.mod"), "module example\n\ngo 1.24\n").unwrap();
        let plan = job(Vec::new(), vec!["ubuntu-22.04".into()]);
        let spec = EnvironmentResolver::new(String::new()).resolve(&plan, Some(&path));
        let expected = BTreeSet::from([
            ToolchainLayer::Node("20.11.0".into()),
            ToolchainLayer::Rust("stable".into()),
            ToolchainLayer::Python("3.12".into()),
            ToolchainLayer::Go("1.24".into()),
        ]);
        assert_eq!(
            spec.toolchains.iter().cloned().collect::<BTreeSet<_>>(),
            expected
        );
        assert_eq!(spec.base, "ubuntu:22.04");
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn fingerprint_is_stable_and_deduplicated() {
        let steps = vec![
            setup("actions/setup-node@v4", &[("node-version", "22")]),
            setup("actions/setup-node@v4", &[("node-version", "22")]),
        ];
        let plan = job(steps.clone(), vec!["ubuntu-24.04".into()]);
        let resolver = EnvironmentResolver::new(String::new());
        let first = resolver.resolve(&plan, None);
        let second = resolver.resolve(&job(steps, vec!["ubuntu-24.04".into()]), None);
        assert_eq!(first.fingerprint, second.fingerprint);
        assert_eq!(first.toolchains, vec![ToolchainLayer::Node("22".into())]);
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
}
