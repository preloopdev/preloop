//! Runner configuration persistence.
//!
//! Mirrors the official runner's `.runner`, `.credentials`, and
//! `.credentials_rsaparams` JSON files exactly.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

// ─── .runner ─────────────────────────────────────────────────────────────

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
    /// F007: matches official .runner `isHostedServer` field.
    #[serde(default)]
    pub is_hosted_server: bool,
    /// F007: matches official .runner `useV2Flow` field.
    #[serde(default = "default_true")]
    pub use_v2_flow: bool,
    /// F007: matches official .runner `serverUrlV2` field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_url_v2: Option<String>,
    /// F052: Disable auto-update check.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub disable_update: bool,
    /// F052: Skip session recovery on broker reconnect.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub skip_session_recover: bool,
    /// F052: Monitor socket address for diagnostics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub monitor_socket_address: Option<String>,
    /// F052: Use runner admin flow for registration.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub use_runner_admin_flow: bool,
}

fn default_true() -> bool {
    true
}

// ─── .credentials ────────────────────────────────────────────────────────

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

// ─── .credentials_rsaparams ─────────────────────────────────────────────

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

impl aksh_gha_protocol::crypto::RsaParamsLike for RsaParameters {
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

/// Save a JSON file (no BOM — we write clean UTF-8).
fn save_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let json = serde_json::to_string_pretty(value)?;
    std::fs::write(path, json).with_context(|| format!("writing {}", path.display()))
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
    pub fn load(root: &Path) -> Result<Self> {
        Ok(Self {
            settings: load_json(&root.join(RUNNER_FILE))?,
            credentials: load_json(&root.join(CREDENTIALS_FILE))?,
            rsa_params: load_json(&root.join(RSA_PARAMS_FILE))?,
        })
    }

    /// Persist runner configuration to the given root directory.
    pub fn save(&self, root: &Path) -> Result<()> {
        save_json(&root.join(RUNNER_FILE), &self.settings)?;
        save_json(&root.join(CREDENTIALS_FILE), &self.credentials)?;
        save_json(&root.join(RSA_PARAMS_FILE), &self.rsa_params)?;
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
}
