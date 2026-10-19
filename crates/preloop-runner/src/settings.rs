//! Runner configuration persistence.
//!
//! Mirrors the official runner's `.runner`, `.credentials`, and
//! `.credentials_rsaparams` JSON files exactly.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

// ───.runner ─────────────────────────────────────────────────────────────

/// Which control plane the runner registered against.
///
/// A GitHub-operated endpoint hands the runner a real GitHub credential in the
/// job's `system.github.token` variable, so the worker may project it onto
/// `github.token`/`GITHUB_TOKEN` exactly like the official runner
/// (`ExecutionContext.InitializeJob`). On a preloop engine that variable is
/// the control plane's GitHub credential slot: the claim writes the GitHub
/// credential it minted there and leaves it empty when it has none. The
/// job-scoped runtime JWT is a separate control-plane credential delivered
/// through the job's `SystemVssConnection` endpoint; it is never written to
/// this variable. Engine registrations therefore keep `system.github.token`
/// off the step surface and read the `github.token` context the claim patches
/// instead.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ControlPlane {
    /// Registered against a preloop engine (or anything that is not a URL
    /// `register_runner` registers against GitHub). Fails closed:
    /// `system.github.token` is treated as the claim's GitHub credential
    /// slot and is never exported.
    #[default]
    Engine,
    /// Registered against github.com: `system.github.token` is a real GitHub
    /// credential.
    GitHub,
}

impl<'de> Deserialize<'de> for ControlPlane {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value.eq_ignore_ascii_case("github") {
            Ok(Self::GitHub)
        } else if value.eq_ignore_ascii_case("engine") {
            Ok(Self::Engine)
        } else {
            // A value this build does not recognise is corrupt registration
            // metadata, not an implicit engine registration: silently
            // selecting Engine would hide a typo or a field written by a
            // newer build. Old files omit the field entirely, which
            // `Option<ControlPlane>` plus `#[serde(default)]` still accepts.
            // Name the field and the accepted values; do not echo the raw
            // value, which may have come from corrupt or hostile metadata.
            Err(serde::de::Error::custom(
                "invalid `.runner` controlPlane value; expected \"engine\" or \"github\"",
            ))
        }
    }
}

/// Classify the URL a runner registered with into a [`ControlPlane`].
///
/// `register_runner` routes `github.com` URLs to `api.github.com` and
/// everything else to the server-relative `/api/v3/` registration path; that
/// second branch covers self-hosted GHES, managed `*.ghe.com` enterprises and
/// preloop engines, which are indistinguishable by URL shape — and preloop
/// implements registration routing for none of them. Classification therefore
/// fails closed: only hosts under `github.com`, the one URL shape
/// `register_runner` actually registers against GitHub, map to `GitHub`; every
/// other host, including self-hosted GHES and `*.ghe.com` (whose enterprise
/// API host preloop does not route to), maps to `Engine` and keeps the
/// control-plane fence.
///
/// `register_runner` matches hosts with a `contains("github.com")` substring
/// test, so a host like `github.com.example.invalid` is registered against
/// GitHub's API while this classification stays `Engine`; the disagreement is
/// deliberately resolved in the closed direction, never by exporting a
/// variable a control plane owns.
///
/// Unparseable URLs and URLs without a host are errors. Treating them as
/// `Engine` would silently accept corrupt registration metadata and hide the
/// misconfiguration until a workflow ran; callers must surface the error
/// instead of choosing a plane.
pub fn control_plane_for_url(url: &str) -> Result<ControlPlane> {
    let parsed =
        reqwest::Url::parse(url).context("registration URL is not a valid absolute URL")?;
    let host = parsed
        .host_str()
        .context("registration URL has no host")?
        .trim_end_matches('.')
        .to_ascii_lowercase();
    if host == "github.com" || host.ends_with(".github.com") {
        Ok(ControlPlane::GitHub)
    } else {
        Ok(ControlPlane::Engine)
    }
}

/// Runner settings persisted in `.runner`.
///
/// Field names match the official C# `RunnerSettings` exactly.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunnerSettings {
    pub agent_id: i64,
    pub agent_name: String,
    pub pool_id: i64,
    #[serde(default)]
    pub pool_name: String,
    pub server_url: String,
    pub git_hub_url: String,
    pub work_folder: String,
    #[serde(default)]
    pub is_hosted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_group_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_group_name: Option<String>,
    #[serde(default)]
    pub ephemeral: bool,
    /// matches official.runner `isHostedServer` field.
    #[serde(default)]
    pub is_hosted_server: bool,
    /// matches official.runner `useV2Flow` field.
    #[serde(default = "default_true")]
    pub use_v2_flow: bool,
    /// matches official.runner `serverUrlV2` field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_url_v2: Option<String>,
    /// Disable auto-update check.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub disable_update: bool,
    /// Skip session recovery on broker reconnect.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub skip_session_recover: bool,
    /// Monitor socket address for diagnostics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub monitor_socket_address: Option<String>,
    /// Use runner admin flow for registration.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub use_runner_admin_flow: bool,
    /// preloop extension (not an official `.runner` field): which control
    /// plane this runner registered against, recorded at configure from the
    /// `--url` host. `None` in `.runner` files written before the field
    /// existed; an unrecognised value is rejected when the file is parsed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_plane: Option<ControlPlane>,
}

/// Registration state resolved from `.runner` for job execution.
///
/// Carrying the resolved plane next to the runner identity means a job never
/// re-reads `.runner` (or re-derives the plane from whatever directory it
/// happens to run in) after startup validated the registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunnerRegistration {
    pub control_plane: ControlPlane,
    pub runner_name: String,
}

impl RunnerSettings {
    /// Load `.runner` only.
    ///
    /// Credential files are deliberately not read here. Callers that execute
    /// jobs project the validated runtime state with [`Self::registration`];
    /// callers that only need the settings validate with
    /// [`Self::control_plane`] before using them.
    pub fn load(root: &Path) -> Result<Self> {
        load_json(&root.join(RUNNER_FILE))
    }

    /// Project the registration state a job needs.
    ///
    /// Resolves and validates the control plane and copies the registered
    /// runner name. The only clone is the one this projection owns; reading
    /// `.runner` for non-job purposes does not pay for it.
    pub fn registration(&self) -> Result<RunnerRegistration> {
        Ok(RunnerRegistration {
            control_plane: self.control_plane()?,
            runner_name: self.agent_name.clone(),
        })
    }

    /// The plane this runner registered against: the recorded value, or — for
    /// a `.runner` file that predates the field — the class of the
    /// registration URL the file already carries.
    ///
    /// The inference is the classification `configure` would have recorded for
    /// that same URL, so an upgraded github.com runner keeps its GitHub token
    /// surface without re-registering, while an engine (or a host preloop does
    /// not register against GitHub) stays fenced. A recorded value wins over
    /// the inferred one; an unrecognised recorded value is rejected at
    /// deserialization. The registration URL is validated even when the field
    /// is present, so corrupt metadata cannot hide behind an explicit plane.
    pub fn control_plane(&self) -> Result<ControlPlane> {
        let inferred =
            control_plane_for_url(&self.git_hub_url).context("invalid `.runner` gitHubUrl")?;
        Ok(self.control_plane.unwrap_or(inferred))
    }
}

fn default_true() -> bool {
    true
}

// ───.credentials ────────────────────────────────────────────────────────

/// Credential data persisted in `.credentials`.
///
/// Matches official format: `{scheme, data: {clientId, authorizationUrl, requireFipsCryptography}}`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialData {
    pub scheme: String,
    #[serde(default)]
    pub data: serde_json::Map<String, serde_json::Value>,
}

impl CredentialData {
    /// Get the authorization URL from the data map.
    pub fn authorization_url(&self) -> Option<&str> {
        self.data.get("authorizationUrl").and_then(|v| v.as_str())
    }

    /// Get the client ID from the data map.
    pub fn client_id(&self) -> Option<&str> {
        self.data.get("clientId").and_then(|v| v.as_str())
    }
}

// ───.credentials_rsaparams ─────────────────────────────────────────────

/// RSA key parameters persisted in `.credentials_rsaparams`.
///
/// Field names match the C# `RSAParameters` struct exactly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RsaParameters {
    #[serde(rename = "D")]
    pub d: String,
    #[serde(rename = "DP")]
    pub dp: String,
    #[serde(rename = "DQ")]
    pub dq: String,
    #[serde(rename = "Exponent")]
    pub exponent: String,
    #[serde(rename = "InverseQ")]
    pub inverse_q: String,
    #[serde(rename = "Modulus")]
    pub modulus: String,
    #[serde(rename = "P")]
    pub p: String,
    #[serde(rename = "Q")]
    pub q: String,
}

impl preloop_gha_protocol::crypto::RsaParamsLike for RsaParameters {
    fn d(&self) -> &str {
        &self.d
    }
    fn exponent(&self) -> &str {
        &self.exponent
    }
    fn modulus(&self) -> &str {
        &self.modulus
    }
    fn p(&self) -> &str {
        &self.p
    }
    fn q(&self) -> &str {
        &self.q
    }
}

// ─── File I/O ────────────────────────────────────────────────────────────

const RUNNER_FILE: &str = ".runner";
const CREDENTIALS_FILE: &str = ".credentials";
const RSA_PARAMS_FILE: &str = ".credentials_rsaparams";

/// Strip UTF-8 BOM if present (official runner writes BOM).
fn strip_bom(s: &str) -> &str {
    s.strip_prefix('\u{FEFF}').unwrap_or(s)
}

/// Load a JSON file, tolerating UTF-8 BOM.
fn load_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let clean = strip_bom(&raw);
    serde_json::from_str(clean).with_context(|| format!("parsing {}", path.display()))
}

/// Save a JSON file atomically (the official runner may replace settings while
/// the listener is polling; never leave a truncated `.runner` behind).
fn save_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let json = serde_json::to_string_pretty(value)?;
    let temp = path.with_file_name(format!(
        ".{}.tmp-{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("runner"),
        std::process::id()
    ));
    std::fs::write(&temp, json).with_context(|| format!("writing {}", temp.display()))?;
    let result =
        std::fs::rename(&temp, path).with_context(|| format!("replacing {}", path.display()));
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

fn save_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let json = serde_json::to_string_pretty(value)?;
    std::fs::write(path, json).with_context(|| format!("writing {}", path.display()))
}

fn decode_refresh_payload(payload: &serde_json::Value) -> Result<serde_json::Value> {
    let Some(encoded) = payload.as_str() else {
        return Ok(payload.clone());
    };
    if let Ok(json) = serde_json::from_str(encoded) {
        return Ok(json);
    }
    let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
        .context("decoding refreshed runner settings")?;
    let text = String::from_utf8(bytes).context("refreshed runner settings are not UTF-8")?;
    serde_json::from_str(&text).context("parsing refreshed runner settings JSON")
}

fn refresh_settings_object(
    payload: &serde_json::Value,
) -> Option<&serde_json::Map<String, serde_json::Value>> {
    let object = payload.as_object()?;
    for wrapper in ["settings", "runnerSettings", "runnerConfig"] {
        if let Some(inner) = object.get(wrapper) {
            return inner.as_object();
        }
    }
    Some(object)
}

/// Restrict a file to owner-read/write only (0600) on Unix.
///
/// Matches the official runner which chmods `.runner`, `.credentials`, and
/// `.credentials_rsaparams` to 0600 on Linux/macOS so the private RSA key
/// is not readable by other users on a shared host.
/// No-op on non-Unix platforms.
fn restrict_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("setting permissions on {}", path.display()))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// A loaded runner configuration (all three files).
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub settings: RunnerSettings,
    pub credentials: CredentialData,
    pub rsa_params: RsaParameters,
}

impl RunnerConfig {
    /// Load runner configuration from the given root directory.
    ///
    /// `.runner` is validated (including its recorded control plane) before
    /// the credential files are read, so a malformed registration cannot be
    /// mistaken for an engine registration by a running listener.
    pub fn load(root: &Path) -> Result<Self> {
        let settings = RunnerSettings::load(root)?;
        settings.control_plane()?;
        Ok(Self {
            settings,
            credentials: load_json(&root.join(CREDENTIALS_FILE))?,
            rsa_params: load_json(&root.join(RSA_PARAMS_FILE))?,
        })
    }

    /// Persist only `.runner` using an atomic replacement.
    pub fn save_settings_atomic(&self, root: &Path) -> Result<()> {
        let runner_path = root.join(RUNNER_FILE);
        save_json_atomic(&runner_path, &self.settings)?;
        restrict_permissions(&runner_path)
    }

    /// Apply a server-supplied runner settings refresh.
    ///
    /// The official `RunnerRefreshConfig` response is a base64-encoded JSON
    /// `.runner` document. Local servers also commonly return the JSON object
    /// directly, so both forms are accepted. Unknown keys are ignored and the
    /// runner identity is immutable; malformed or mismatched payloads leave
    /// the current settings untouched.
    pub fn apply_runner_settings_refresh(
        &mut self,
        payload: &serde_json::Value,
        root: &Path,
    ) -> Result<bool> {
        let payload = decode_refresh_payload(payload)?;
        let Some(object) = refresh_settings_object(&payload) else {
            return Ok(false);
        };

        if let Some(agent_id) = object.get("agentId")
            && agent_id.as_i64() != Some(self.settings.agent_id)
        {
            return Ok(false);
        }
        if let Some(agent_name) = object.get("agentName")
            && agent_name.as_str() != Some(self.settings.agent_name.as_str())
        {
            return Ok(false);
        }

        const SUPPORTED_FIELDS: &[&str] = &[
            "poolId",
            "poolName",
            "serverUrl",
            "gitHubUrl",
            "workFolder",
            "isHosted",
            "runnerGroupId",
            "runnerGroupName",
            "ephemeral",
            "isHostedServer",
            "useV2Flow",
            "serverUrlV2",
            "disableUpdate",
            "skipSessionRecover",
            "monitorSocketAddress",
            "useRunnerAdminFlow",
        ];
        let mut merged = serde_json::to_value(&self.settings)?;
        let merged_object = merged
            .as_object_mut()
            .expect("RunnerSettings serializes as an object");
        let mut changed = false;
        for field in SUPPORTED_FIELDS {
            if let Some(value) = object.get(*field)
                && merged_object.get(*field) != Some(value)
            {
                merged_object.insert((*field).to_string(), value.clone());
                changed = true;
            }
        }
        if !changed {
            return Ok(false);
        }

        let refreshed: RunnerSettings =
            serde_json::from_value(merged).context("parsing refreshed runner settings")?;
        self.settings = refreshed;
        self.save_settings_atomic(root)?;
        Ok(true)
    }

    /// Persist runner configuration to the given root directory.
    ///
    /// Files are written with 0600 permissions on Unix so the private RSA key
    /// and credentials are not readable by other users on a shared host.
    pub fn save(&self, root: &Path) -> Result<()> {
        let runner_path = root.join(RUNNER_FILE);
        save_json(&runner_path, &self.settings)?;
        restrict_permissions(&runner_path)?;
        let cred_path = root.join(CREDENTIALS_FILE);
        save_json(&cred_path, &self.credentials)?;
        restrict_permissions(&cred_path)?;
        let rsa_path = root.join(RSA_PARAMS_FILE);
        save_json(&rsa_path, &self.rsa_params)?;
        restrict_permissions(&rsa_path)?;
        Ok(())
    }

    /// Check if a runner is already configured in the given root.
    pub fn is_configured(root: &Path) -> bool {
        root.join(RUNNER_FILE).exists()
    }

    /// Remove configuration files from the given root.
    pub fn remove_files(root: &Path) -> Result<()> {
        for name in [RUNNER_FILE, CREDENTIALS_FILE, RSA_PARAMS_FILE] {
            let path = root.join(name);
            if path.exists() {
                std::fs::remove_file(&path)
                    .with_context(|| format!("removing {}", path.display()))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn round_trip_settings() {
        let settings = RunnerSettings {
            agent_id: 42,
            agent_name: "test-runner".to_string(),
            pool_id: 1,
            pool_name: "Default".to_string(),
            server_url: "https://pipelines.actions.githubusercontent.com/abc123".to_string(),
            git_hub_url: "https://github.com/test/repo".to_string(),
            work_folder: "_work".to_string(),
            is_hosted: false,
            runner_group_id: Some(1),
            runner_group_name: Some("Default".to_string()),
            ephemeral: false,
            is_hosted_server: false,
            use_v2_flow: true,
            server_url_v2: Some(
                "https://pipelines.actions.githubusercontent.com/abc123".to_string(),
            ),
            disable_update: false,
            skip_session_recover: false,
            monitor_socket_address: None,
            use_runner_admin_flow: false,
            control_plane: Some(ControlPlane::Engine),
        };
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(".runner");
        save_json(&path, &settings).unwrap();
        let loaded: RunnerSettings = load_json(&path).unwrap();
        assert_eq!(loaded.agent_id, 42);
        assert_eq!(loaded.agent_name, "test-runner");
    }

    #[test]
    fn strip_bom_works() {
        assert_eq!(strip_bom("\u{FEFF}{\"key\": 1}"), "{\"key\": 1}");
        assert_eq!(strip_bom("{\"key\": 1}"), "{\"key\": 1}");
    }

    #[test]
    fn rsa_params_field_names() {
        // Verify serde rename matches C# RSAParameters
        let json = r#"{
            "D": "base64d",
            "DP": "base64dp",
            "DQ": "base64dq",
            "Exponent": "AQAB",
            "InverseQ": "base64iq",
            "Modulus": "base64mod",
            "P": "base64p",
            "Q": "base64q"
        }"#;
        let params: RsaParameters = serde_json::from_str(json).unwrap();
        assert_eq!(params.exponent, "AQAB");
        // Round-trip
        let serialized = serde_json::to_string(&params).unwrap();
        assert!(serialized.contains("\"Exponent\""));
        assert!(serialized.contains("\"InverseQ\""));
    }

    #[test]
    fn config_lifecycle() {
        let dir = TempDir::new().unwrap();
        assert!(!RunnerConfig::is_configured(dir.path()));

        let config = RunnerConfig {
            settings: RunnerSettings {
                agent_id: 1,
                agent_name: "test".to_string(),
                pool_id: 1,
                pool_name: "Default".to_string(),
                server_url: "https://example.com".to_string(),
                git_hub_url: "https://github.com/test/repo".to_string(),
                work_folder: "_work".to_string(),
                is_hosted: false,
                runner_group_id: None,
                runner_group_name: None,
                ephemeral: false,
                is_hosted_server: false,
                use_v2_flow: true,
                server_url_v2: None,
                disable_update: false,
                skip_session_recover: false,
                monitor_socket_address: None,
                use_runner_admin_flow: false,
                control_plane: Some(ControlPlane::Engine),
            },
            credentials: CredentialData {
                scheme: "OAuth".to_string(),
                data: {
                    let mut m = serde_json::Map::new();
                    m.insert("clientId".into(), serde_json::json!("abc-123"));
                    m.insert(
                        "authorizationUrl".into(),
                        serde_json::json!("https://vstoken.actions.githubusercontent.com"),
                    );
                    m
                },
            },
            rsa_params: RsaParameters {
                d: "d".to_string(),
                dp: "dp".to_string(),
                dq: "dq".to_string(),
                exponent: "AQAB".to_string(),
                inverse_q: "iq".to_string(),
                modulus: "mod".to_string(),
                p: "p".to_string(),
                q: "q".to_string(),
            },
        };
        config.save(dir.path()).unwrap();
        assert!(RunnerConfig::is_configured(dir.path()));

        let loaded = RunnerConfig::load(dir.path()).unwrap();
        assert_eq!(loaded.settings.agent_name, "test");

        RunnerConfig::remove_files(dir.path()).unwrap();
        assert!(!RunnerConfig::is_configured(dir.path()));
    }

    // --- settings/credential gap coverage ---

    #[test]
    fn credential_data_accessors() {
        let mut data = serde_json::Map::new();
        data.insert("clientId".into(), serde_json::json!("abc-client-id"));
        data.insert(
            "authorizationUrl".into(),
            serde_json::json!("https://vstoken.actions.githubusercontent.com"),
        );
        let cred = CredentialData {
            scheme: "OAuth".to_string(),
            data,
        };
        assert_eq!(cred.client_id(), Some("abc-client-id"));
        assert_eq!(
            cred.authorization_url(),
            Some("https://vstoken.actions.githubusercontent.com")
        );
    }

    #[test]
    fn credential_data_missing_fields() {
        let cred = CredentialData {
            scheme: "OAuth".to_string(),
            data: serde_json::Map::new(),
        };
        assert!(cred.client_id().is_none());
        assert!(cred.authorization_url().is_none());
    }

    #[test]
    fn runner_settings_ephemeral_fields_roundtrip() {
        let settings = RunnerSettings {
            agent_id: 99,
            agent_name: "ephemeral-runner".to_string(),
            pool_id: 2,
            pool_name: "Hosted".to_string(),
            server_url: "https://pipelines.actions.githubusercontent.com/xyz".to_string(),
            git_hub_url: "https://github.com/org/repo".to_string(),
            work_folder: "_work".to_string(),
            is_hosted: true,
            runner_group_id: Some(5),
            runner_group_name: Some("Custom".to_string()),
            ephemeral: true,
            is_hosted_server: true,
            use_v2_flow: true,
            server_url_v2: Some("https://broker.actions.githubusercontent.com/".to_string()),
            disable_update: true,
            skip_session_recover: true,
            monitor_socket_address: Some("/tmp/runner-monitor.sock".to_string()),
            use_runner_admin_flow: true,
            control_plane: Some(ControlPlane::GitHub),
        };
        let json = serde_json::to_string_pretty(&settings).unwrap();
        let loaded: RunnerSettings = serde_json::from_str(&json).unwrap();
        assert!(loaded.ephemeral);
        assert!(loaded.is_hosted);
        assert!(loaded.is_hosted_server);
        assert!(loaded.use_v2_flow);
        assert!(loaded.disable_update);
        assert!(loaded.skip_session_recover);
        assert!(loaded.use_runner_admin_flow);
        assert_eq!(
            loaded.server_url_v2.as_deref(),
            Some("https://broker.actions.githubusercontent.com/")
        );
        assert_eq!(
            loaded.monitor_socket_address.as_deref(),
            Some("/tmp/runner-monitor.sock")
        );
        assert_eq!(loaded.runner_group_id, Some(5));
    }

    #[test]
    fn runner_settings_camel_case_json_keys() {
        let settings = RunnerSettings {
            agent_id: 1,
            agent_name: "test".to_string(),
            pool_id: 1,
            pool_name: "Default".to_string(),
            server_url: "https://example.com".to_string(),
            git_hub_url: "https://github.com/t/r".to_string(),
            work_folder: "_work".to_string(),
            is_hosted: false,
            runner_group_id: None,
            runner_group_name: None,
            ephemeral: false,
            is_hosted_server: false,
            use_v2_flow: true,
            server_url_v2: None,
            disable_update: false,
            skip_session_recover: false,
            monitor_socket_address: None,
            use_runner_admin_flow: false,
            control_plane: Some(ControlPlane::Engine),
        };
        let json = serde_json::to_string(&settings).unwrap();
        // Official.runner uses camelCase
        assert!(json.contains("\"agentId\""));
        assert!(json.contains("\"agentName\""));
        assert!(json.contains("\"poolId\""));
        assert!(json.contains("\"serverUrl\""));
        assert!(json.contains("\"gitHubUrl\""));
        assert!(json.contains("\"workFolder\""));
        assert!(json.contains("\"isHosted\""));
        assert!(json.contains("\"useV2Flow\""));
    }

    #[test]
    fn control_plane_classifies_configure_url() {
        // Only hosts under github.com are GitHub's, because that is the one
        // URL shape `register_runner` registers against GitHub.
        assert_eq!(
            control_plane_for_url("https://github.com/owner/repo").unwrap(),
            ControlPlane::GitHub
        );
        assert_eq!(
            control_plane_for_url("https://api.github.com").unwrap(),
            ControlPlane::GitHub
        );
        // Host matching is case-insensitive and tolerates a trailing dot.
        assert_eq!(
            control_plane_for_url("https://GitHub.COM/owner/repo").unwrap(),
            ControlPlane::GitHub
        );

        // Everything else fails closed: engine URLs, self-hosted GHES and
        // managed `*.ghe.com` (their enterprise API host is not a
        // registration route preloop implements), and hosts that merely
        // contain "github.com" as a substring.
        assert_eq!(
            control_plane_for_url("https://ci.internal.example").unwrap(),
            ControlPlane::Engine
        );
        assert_eq!(
            control_plane_for_url("http://127.0.0.1:9191").unwrap(),
            ControlPlane::Engine
        );
        assert_eq!(
            control_plane_for_url("https://acme.ghe.com/org/repo").unwrap(),
            ControlPlane::Engine
        );
        assert_eq!(
            control_plane_for_url("https://ghe.corp.example/owner/repo").unwrap(),
            ControlPlane::Engine
        );
        assert_eq!(
            control_plane_for_url("https://github.com.evil.example/o/r").unwrap(),
            ControlPlane::Engine
        );
        assert_eq!(
            control_plane_for_url("https://notgithub.com/o/r").unwrap(),
            ControlPlane::Engine
        );

        // Malformed registration metadata must not silently become Engine.
        for malformed in ["", "not a url", "example.com:8080"] {
            assert!(
                control_plane_for_url(malformed).is_err(),
                "{malformed:?} must be rejected, not classified as Engine"
            );
        }
    }

    #[test]
    fn control_plane_migrates_old_runner_files_from_their_url() {
        // `.runner` written before `controlPlane` existed: the registration
        // URL it already carries decides, so an upgraded github.com runner
        // keeps its token surface without re-registering.
        let legacy = |git_hub_url: &str| {
            let json = format!(
                r#"{{
                    "agentId": 1,
                    "agentName": "test",
                    "poolId": 1,
                    "serverUrl": "https://example.com",
                    "gitHubUrl": "{git_hub_url}",
                    "workFolder": "_work"
                }}"#
            );
            serde_json::from_str::<RunnerSettings>(&json).unwrap()
        };
        assert_eq!(
            legacy("https://github.com/t/r").control_plane().unwrap(),
            ControlPlane::GitHub
        );
        assert_eq!(
            legacy("https://ci.internal.example")
                .control_plane()
                .unwrap(),
            ControlPlane::Engine
        );
        assert_eq!(
            legacy("https://ghe.corp.example/owner/repo")
                .control_plane()
                .unwrap(),
            ControlPlane::Engine
        );
        assert!(
            legacy("not a url").control_plane().is_err(),
            "a legacy file with a malformed gitHubUrl must not classify as Engine"
        );

        // A recorded value wins over the URL. An unrecognised value is
        // rejected while parsing the file, not widened or fenced silently.
        let recorded = |control_plane: &str, git_hub_url: &str| {
            let json = format!(
                r#"{{
                    "agentId": 1,
                    "agentName": "test",
                    "poolId": 1,
                    "serverUrl": "https://example.com",
                    "gitHubUrl": "{git_hub_url}",
                    "workFolder": "_work",
                    "controlPlane": "{control_plane}"
                }}"#
            );
            serde_json::from_str::<RunnerSettings>(&json)
        };
        // The explicit value is authoritative in both directions; it is a
        // recorded decision, not a hint derived from the URL.
        assert_eq!(
            recorded("github", "https://ci.internal.example")
                .unwrap()
                .control_plane()
                .unwrap(),
            ControlPlane::GitHub
        );
        assert_eq!(
            recorded("engine", "https://github.com/t/r")
                .unwrap()
                .control_plane()
                .unwrap(),
            ControlPlane::Engine
        );
        // Case-insensitive spellings configure accepts still work.
        assert_eq!(
            recorded("GitHub", "https://ci.internal.example")
                .unwrap()
                .control_plane()
                .unwrap(),
            ControlPlane::GitHub
        );
        // An explicit plane does not excuse a malformed registration URL.
        assert!(
            recorded("engine", "not a url")
                .unwrap()
                .control_plane()
                .is_err(),
            "a malformed gitHubUrl must fail even when controlPlane is present"
        );
        let error = recorded("surprise", "https://github.com/t/r")
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("controlPlane") && error.contains("engine"),
            "unknown controlPlane must be rejected with an actionable error, got: {error}"
        );
        assert!(
            !error.contains("surprise"),
            "the error must not echo the raw metadata value, got: {error}"
        );

        // Configure always records the plane explicitly.
        let settings = RunnerSettings {
            agent_id: 1,
            agent_name: "test".to_string(),
            pool_id: 1,
            pool_name: "Default".to_string(),
            server_url: "https://example.com".to_string(),
            git_hub_url: "https://github.com/t/r".to_string(),
            work_folder: "_work".to_string(),
            is_hosted: false,
            runner_group_id: None,
            runner_group_name: None,
            ephemeral: false,
            is_hosted_server: false,
            use_v2_flow: true,
            server_url_v2: None,
            disable_update: false,
            skip_session_recover: false,
            monitor_socket_address: None,
            use_runner_admin_flow: false,
            control_plane: Some(ControlPlane::Engine),
        };
        let json = serde_json::to_string(&settings).unwrap();
        assert!(json.contains("\"controlPlane\":\"engine\""));
    }

    #[test]
    fn settings_load_requires_only_runner_file_and_rejects_bad_metadata() {
        let dir = TempDir::new().unwrap();
        let write = |json: &str| {
            std::fs::write(dir.path().join(".runner"), json).unwrap();
        };

        // Missing `.runner` is an error, not an implicit Engine.
        let error = format!("{:#}", RunnerSettings::load(dir.path()).unwrap_err());
        assert!(
            error.contains(".runner"),
            "missing metadata error must name the file, got: {error}"
        );

        // Only `.runner` is needed: credentials and RSA params are not read
        // on the job-execution path.
        write(
            r#"{
                "agentId": 1,
                "agentName": "test",
                "poolId": 1,
                "serverUrl": "https://example.com",
                "gitHubUrl": "https://github.com/t/r",
                "workFolder": "_work"
            }"#,
        );
        let settings = RunnerSettings::load(dir.path()).unwrap();
        let registration = settings.registration().unwrap();
        assert_eq!(registration.control_plane, ControlPlane::GitHub);
        assert_eq!(registration.runner_name, "test");

        // Malformed JSON and unknown explicit planes fail loudly.
        write("{ not json");
        assert!(RunnerSettings::load(dir.path()).is_err());

        write(
            r#"{
                "agentId": 1,
                "agentName": "test",
                "poolId": 1,
                "serverUrl": "https://example.com",
                "gitHubUrl": "https://github.com/t/r",
                "workFolder": "_work",
                "controlPlane": "enterprise"
            }"#,
        );
        let error = format!("{:#}", RunnerSettings::load(dir.path()).unwrap_err());
        assert!(
            error.contains("controlPlane"),
            "unknown plane error must name the field, got: {error}"
        );

        // A legacy file whose registration URL cannot be parsed is an error
        // rather than an Engine registration.
        write(
            r#"{
                "agentId": 1,
                "agentName": "test",
                "poolId": 1,
                "serverUrl": "https://example.com",
                "gitHubUrl": "not a url",
                "workFolder": "_work"
            }"#,
        );
        let error = format!(
            "{:#}",
            RunnerSettings::load(dir.path())
                .unwrap()
                .registration()
                .unwrap_err()
        );
        assert!(
            error.contains("registration URL"),
            "malformed URL error must name the registration URL, got: {error}"
        );
    }

    #[test]
    fn runner_settings_default_use_v2_flow_is_true() {
        // When loading a.runner file that doesn't have useV2Flow,
        // it should default to true (matching official runner behavior)
        let json = r#"{
            "agentId": 1,
            "agentName": "test",
            "poolId": 1,
            "serverUrl": "https://example.com",
            "gitHubUrl": "https://github.com/t/r",
            "workFolder": "_work"
        }"#;
        let settings: RunnerSettings = serde_json::from_str(json).unwrap();
        assert!(settings.use_v2_flow);
        assert!(!settings.ephemeral);
        assert!(!settings.disable_update);
        assert!(!settings.skip_session_recover);
    }

    #[test]
    fn credential_data_auth_migration_fields() {
        let mut data = serde_json::Map::new();
        data.insert("clientId".into(), serde_json::json!("cid"));
        data.insert(
            "authorizationUrl".into(),
            serde_json::json!("https://vstoken.old.com"),
        );
        data.insert(
            "enableAuthMigrationByDefault".into(),
            serde_json::json!("true"),
        );
        data.insert(
            "authorizationUrlV2".into(),
            serde_json::json!("https://vstoken.new.com"),
        );
        data.insert(
            "oauthEndpointUrl".into(),
            serde_json::json!("https://oauth.example.com/token"),
        );
        let cred = CredentialData {
            scheme: "OAuth".to_string(),
            data,
        };
        // These are the fields the OAuth flow reads
        assert_eq!(cred.client_id(), Some("cid"));
        assert_eq!(cred.authorization_url(), Some("https://vstoken.old.com"));
        assert_eq!(
            cred.data
                .get("enableAuthMigrationByDefault")
                .unwrap()
                .as_str()
                .unwrap(),
            "true"
        );
        assert_eq!(
            cred.data
                .get("authorizationUrlV2")
                .unwrap()
                .as_str()
                .unwrap(),
            "https://vstoken.new.com"
        );
        assert_eq!(
            cred.data.get("oauthEndpointUrl").unwrap().as_str().unwrap(),
            "https://oauth.example.com/token"
        );
    }

    #[test]
    fn config_save_load_with_all_credential_fields() {
        let dir = TempDir::new().unwrap();
        let mut data = serde_json::Map::new();
        data.insert("clientId".into(), serde_json::json!("client-1"));
        data.insert(
            "authorizationUrl".into(),
            serde_json::json!("https://auth.example.com"),
        );
        data.insert("requireFipsCryptography".into(), serde_json::json!("True"));
        data.insert(
            "enableAuthMigrationByDefault".into(),
            serde_json::json!("true"),
        );
        data.insert(
            "authorizationUrlV2".into(),
            serde_json::json!("https://auth-v2.example.com"),
        );

        let config = RunnerConfig {
            settings: RunnerSettings {
                agent_id: 42,
                agent_name: "full-config".to_string(),
                pool_id: 3,
                pool_name: "CI".to_string(),
                server_url: "https://pipelines.example.com".to_string(),
                git_hub_url: "https://github.com/org/repo".to_string(),
                work_folder: "_work".to_string(),
                is_hosted: false,
                runner_group_id: Some(2),
                runner_group_name: Some("GPU".to_string()),
                ephemeral: true,
                is_hosted_server: false,
                use_v2_flow: true,
                server_url_v2: Some("https://broker.example.com/".to_string()),
                disable_update: false,
                skip_session_recover: false,
                monitor_socket_address: None,
                use_runner_admin_flow: false,
                control_plane: Some(ControlPlane::Engine),
            },
            credentials: CredentialData {
                scheme: "OAuth".to_string(),
                data,
            },
            rsa_params: RsaParameters {
                d: "test-d".to_string(),
                dp: "test-dp".to_string(),
                dq: "test-dq".to_string(),
                exponent: "AQAB".to_string(),
                inverse_q: "test-iq".to_string(),
                modulus: "test-mod".to_string(),
                p: "test-p".to_string(),
                q: "test-q".to_string(),
            },
        };
        config.save(dir.path()).unwrap();
        let loaded = RunnerConfig::load(dir.path()).unwrap();
        assert!(loaded.settings.ephemeral);
        assert_eq!(loaded.settings.agent_name, "full-config");
        assert_eq!(loaded.credentials.client_id(), Some("client-1"));
        assert_eq!(
            loaded
                .credentials
                .data
                .get("requireFipsCryptography")
                .unwrap()
                .as_str()
                .unwrap(),
            "True"
        );
        assert_eq!(
            loaded.settings.server_url_v2.as_deref(),
            Some("https://broker.example.com/")
        );
    }
    fn refresh_test_config() -> RunnerConfig {
        RunnerConfig {
            settings: RunnerSettings {
                agent_id: 42,
                agent_name: "refresh-runner".to_string(),
                pool_id: 1,
                pool_name: "Default".to_string(),
                server_url: "https://pipelines.example.com".to_string(),
                git_hub_url: "https://github.com/org/repo".to_string(),
                work_folder: "_work".to_string(),
                is_hosted: false,
                runner_group_id: None,
                runner_group_name: None,
                ephemeral: false,
                is_hosted_server: false,
                use_v2_flow: true,
                server_url_v2: Some("https://broker.example.com".to_string()),
                disable_update: false,
                skip_session_recover: false,
                monitor_socket_address: None,
                use_runner_admin_flow: false,
                control_plane: Some(ControlPlane::Engine),
            },
            credentials: CredentialData {
                scheme: "OAuth".to_string(),
                data: serde_json::Map::new(),
            },
            rsa_params: RsaParameters {
                d: "d".to_string(),
                dp: "dp".to_string(),
                dq: "dq".to_string(),
                exponent: "AQAB".to_string(),
                inverse_q: "iq".to_string(),
                modulus: "mod".to_string(),
                p: "p".to_string(),
                q: "q".to_string(),
            },
        }
    }

    #[test]
    fn refresh_updates_supported_fields_atomically() {
        let dir = TempDir::new().unwrap();
        let mut config = refresh_test_config();
        config.save(dir.path()).unwrap();
        let changed = config
            .apply_runner_settings_refresh(
                &serde_json::json!({
                    "agentId": 42,
                    "agentName": "refresh-runner",
                    "disableUpdate": true,
                    "skipSessionRecover": true,
                    "serverUrlV2": "https://new-broker.example.com",
                    "futureSetting": "ignored"
                }),
                dir.path(),
            )
            .unwrap();
        assert!(changed);
        let persisted: RunnerSettings = load_json(&dir.path().join(RUNNER_FILE)).unwrap();
        assert!(persisted.disable_update);
        assert!(persisted.skip_session_recover);
        assert_eq!(
            persisted.server_url_v2.as_deref(),
            Some("https://new-broker.example.com")
        );
        assert_eq!(persisted.agent_id, 42);
    }

    #[test]
    fn refresh_accepts_official_base64_and_rejects_identity_change() {
        use base64::Engine;
        let dir = TempDir::new().unwrap();
        let mut config = refresh_test_config();
        config.save(dir.path()).unwrap();
        let encoded = base64::engine::general_purpose::STANDARD.encode(
            serde_json::to_vec(&serde_json::json!({
                "agentId": 42,
                "agentName": "refresh-runner",
                "ephemeral": true
            }))
            .unwrap(),
        );
        assert!(
            config
                .apply_runner_settings_refresh(&serde_json::json!(encoded), dir.path())
                .unwrap()
        );
        assert!(config.settings.ephemeral);
        assert!(
            !config
                .apply_runner_settings_refresh(
                    &serde_json::json!({"agentId": 999, "disableUpdate": false}),
                    dir.path(),
                )
                .unwrap()
        );
        assert!(!config.settings.disable_update);
        let _: serde_json::Value = load_json(&dir.path().join(RUNNER_FILE)).unwrap();
    }

    #[test]
    fn malformed_refresh_is_non_mutating() {
        let dir = TempDir::new().unwrap();
        let mut config = refresh_test_config();
        config.save(dir.path()).unwrap();
        assert!(
            config
                .apply_runner_settings_refresh(&serde_json::json!("not-base64"), dir.path())
                .is_err()
        );
        let persisted: RunnerSettings = load_json(&dir.path().join(RUNNER_FILE)).unwrap();
        assert!(!persisted.disable_update);
    }
}
