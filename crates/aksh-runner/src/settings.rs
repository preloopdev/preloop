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
    pub fn load(root: &Path) -> Result<Self> {
        Ok(Self {
            settings: load_json(&root.join(RUNNER_FILE))?,
            credentials: load_json(&root.join(CREDENTIALS_FILE))?,
            rsa_params: load_json(&root.join(RSA_PARAMS_FILE))?,
        })
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

    // --- P1 settings/credential gap coverage ---

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
        };
        let json = serde_json::to_string(&settings).unwrap();
        // Official .runner uses camelCase
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
    fn runner_settings_default_use_v2_flow_is_true() {
        // When loading a .runner file that doesn't have useV2Flow,
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
}
