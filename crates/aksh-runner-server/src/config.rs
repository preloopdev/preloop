//! Local engine configuration file (`~/.preloop/config.toml`).
//!
//! The file holds GitHub credential configuration and stored job secrets.
//! Every field is overridable by the legacy environment variables, which
//! take precedence — the file is the durable store that `preloop setup`
//! and `preloop secret` write, while env vars remain the escape hatch for
//! containerized/deployed engines.
//!
//! The file may contain secrets (the PAT fallback, stored secrets), so it
//! is written with mode 0600 and never echoed back by `preloop secret list`
//! or `preloop doctor`.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Env var pointing at the config file. The engine sets it to
/// `{preloop_home}/config.toml`; the default matches when HOME is used.
pub const CONFIG_PATH_ENV: &str = "PRELOOP_CONFIG";

/// GitHub credential configuration, mirrored 1:1 by the `AKSH_GITHUB_*`
/// environment variables.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct GitHubConfig {
    /// GitHub App ID.
    #[serde(default)]
    pub app_id: Option<String>,
    /// GitHub App private key PEM (inline).
    #[serde(default)]
    pub app_pem: Option<String>,
    /// Mint-failure policy: `local`, `error`, or `pat`.
    #[serde(default)]
    pub mint_failure: Option<String>,
    /// PAT used as the fallback when App minting fails under the `pat`
    /// policy. Also the credential for the `--via pat` setup path.
    #[serde(default)]
    pub pat: Option<String>,
    /// GitHub server URL exposed to workflows as `github.server_url` /
    /// `GITHUB_SERVER_URL`. Defaults to `https://github.com`; point it at a
    /// GHES-style host when the engine fronts one. Env: `AKSH_GITHUB_SERVER_URL`.
    #[serde(default)]
    pub server_url: Option<String>,
    /// GitHub REST API base URL exposed as `github.api_url` /
    /// `GITHUB_API_URL`. Defaults to `https://api.github.com`. Also used for
    /// remote workflow/action fetches. Env: `AKSH_GITHUB_API_URL`.
    #[serde(default)]
    pub api_url: Option<String>,
    /// GitHub GraphQL endpoint exposed as `github.graphql_url` /
    /// `GITHUB_GRAPHQL_URL`. Defaults to `https://api.github.com/graphql`.
    /// Env: `AKSH_GITHUB_GRAPHQL_URL`.
    #[serde(default)]
    pub graphql_url: Option<String>,
}

/// Renders as `<redacted>` / `None` without quoting, for credential fields.
fn redacted(present: bool) -> impl std::fmt::Debug {
    struct Marker(bool);
    impl std::fmt::Debug for Marker {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(if self.0 { "<redacted>" } else { "None" })
        }
    }
    Marker(present)
}

/// Manual `Debug`: this struct carries a PAT and an RSA private key, so the
/// derived impl would disclose both on a single `debug!(?config)`. Presence
/// only — `Serialize` still emits real values for the 0600 TOML file.
impl std::fmt::Debug for GitHubConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitHubConfig")
            .field("app_id", &self.app_id)
            .field("app_pem", &redacted(self.app_pem.is_some()))
            .field("mint_failure", &self.mint_failure)
            .field("pat", &redacted(self.pat.is_some()))
            .finish()
    }
}

/// The engine configuration file.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct ConfigFile {
    #[serde(default)]
    pub github: GitHubConfig,
    /// Stored job secrets injected into every trusted job, mirroring
    /// GitHub's org-level secrets.
    #[serde(default)]
    pub secrets: BTreeMap<String, String>,
    /// Per-repository job secrets (`[repo_secrets."owner/repo"]`), mirroring
    /// GitHub's repo-level secrets. A per-repo secret overrides the global
    /// secret of the same name for that repository.
    #[serde(default)]
    pub repo_secrets: BTreeMap<String, BTreeMap<String, String>>,
}

/// Manual `Debug`: secret values are never printed, only how many exist.
impl std::fmt::Debug for ConfigFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConfigFile {{ github: {:?}, secrets: {} names, repo_secrets: {} repos }}",
            self.github,
            self.secrets.len(),
            self.repo_secrets.len()
        )
    }
}

/// Resolve the config file path: `PRELOOP_CONFIG` when set, else
/// `$HOME/.preloop/config.toml`.
pub fn config_path() -> PathBuf {
    // Tests must never resolve (or worse: write) the developer's real
    // config through the default path; route the default through the pinned
    // per-process temp file. Callers that set `PRELOOP_CONFIG` explicitly
    // are left alone.
    #[cfg(test)]
    pin_test_config_path();
    std::env::var_os(CONFIG_PATH_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var_os("HOME").unwrap_or_default();
            PathBuf::from(home).join(".preloop").join("config.toml")
        })
}

/// Load the config file. A missing or empty file yields the default config;
/// a malformed file is an error so a typo is caught at startup, not when a
/// mint first fails.
pub fn load_config() -> anyhow::Result<ConfigFile> {
    // Tests must never read the developer's real config: a machine with a
    // configured GitHub App leaks credentials into every `AppState::new`.
    // Pin the path to a per-process temp file, and serialize config I/O so
    // parallel tests never observe each other's temp files.
    #[cfg(test)]
    let _config_guard = {
        // Held across the read, not just the pin: dropped early, a concurrent
        // test could swap `PRELOOP_CONFIG` between resolve and read.
        let guard = CONFIG_PATH_LOCK.lock();
        pin_test_config_path();
        guard
    };
    load_config_from(&config_path())
}

/// Load the config file at `path`. A missing or empty file yields the default
/// config; a malformed file is an error.
pub fn load_config_from(path: &Path) -> anyhow::Result<ConfigFile> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ConfigFile::default())
        }
        Err(error) => return Err(error).context(format!("reading config {}", path.display())),
    };
    let config: ConfigFile =
        toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
    Ok(config)
}

/// Atomically write the config file with mode 0600.
pub fn write_config(config: &ConfigFile) -> anyhow::Result<PathBuf> {
    #[cfg(test)]
    let _config_guard = CONFIG_PATH_LOCK.lock();
    let path = config_path();
    write_config_to(&path, config)?;
    Ok(path)
}

/// Atomically write `config` to `path` with mode 0600.
pub fn write_config_to(path: &Path, config: &ConfigFile) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating config dir {}", parent.display()))?;
    }
    let text = toml::to_string_pretty(config).context("serializing config")?;
    // Unique per write: a fixed temp name lets a second writer truncate the
    // first writer's half-written file before either rename lands. Same
    // reason the action tarball download uses a per-request temp path.
    let tmp = path.with_extension(format!("toml.{}.tmp", uuid::Uuid::new_v4()));
    let write = || -> std::io::Result<()> {
        let mut file = std::fs::File::create(&tmp)?;
        // Permissions before content: the file must never be readable by
        // others while it holds secrets.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        file.write_all(text.as_bytes())?;
        file.sync_all()
    };
    if let Err(error) = write() {
        let _ = std::fs::remove_file(&tmp);
        return Err(error).with_context(|| format!("writing {}", tmp.display()));
    }
    if let Err(error) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(error).with_context(|| format!("writing {}", path.display()));
    }
    Ok(())
}

/// First value that is `Some`, used to layer config under env overrides.
pub(crate) fn env_or<T>(env_value: Option<T>, config_value: Option<T>) -> Option<T> {
    env_value.or(config_value)
}

/// Serializes env-resolved config I/O in tests so a test that pins
/// `PRELOOP_CONFIG` (the secrets API tests in `lib_tests.rs` do) cannot swap
/// the path out from under another test between resolve and read/write.
#[cfg(test)]
static CONFIG_PATH_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

#[cfg(test)]
fn pin_test_config_path() {
    use std::sync::LazyLock;
    static TEST_CONFIG_PATH: LazyLock<std::path::PathBuf> = LazyLock::new(|| {
        let dir = std::env::temp_dir().join(format!("preloop-test-config-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        dir.join("config.toml")
    });
    if std::env::var_os(CONFIG_PATH_ENV).is_none() {
        std::env::set_var(CONFIG_PATH_ENV, TEST_CONFIG_PATH.as_path());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // These tests use the path-taking helpers rather than pinning
    // `PRELOOP_CONFIG`: mutating a process-wide env var raced every other test
    // in the binary, so they only passed under `--test-threads=1`.
    fn populated_config() -> ConfigFile {
        ConfigFile {
            github: GitHubConfig {
                app_id: Some("123".into()),
                app_pem: Some(
                    "-----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY-----\n".into(),
                ),
                mint_failure: Some("pat".into()),
                pat: Some("ghp_secret".into()),
                server_url: None,
                api_url: None,
                graphql_url: None,
            },
            secrets: BTreeMap::from([("DOCKERHUB_TOKEN".into(), "abc123".into())]),
            repo_secrets: BTreeMap::from([(
                "owner/repo".into(),
                BTreeMap::from([("REPO_ONLY".into(), "xyz789".into())]),
            )]),
        }
    }

    #[test]
    fn missing_file_is_empty_config() {
        let dir = tempfile::tempdir().unwrap();
        let config = load_config_from(&dir.path().join("config.toml")).unwrap();
        assert!(config.github.app_id.is_none());
        assert!(config.secrets.is_empty());
    }

    #[test]
    fn round_trips_github_and_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        write_config_to(&path, &populated_config()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "config file must be 0600");
        }
        let loaded = load_config_from(&path).unwrap();
        assert_eq!(loaded.github.app_id.as_deref(), Some("123"));
        assert_eq!(loaded.github.pat.as_deref(), Some("ghp_secret"));
        assert_eq!(
            loaded.secrets.get("DOCKERHUB_TOKEN").map(String::as_str),
            Some("abc123")
        );
        assert_eq!(
            loaded
                .repo_secrets
                .get("owner/repo")
                .and_then(|map| map.get("REPO_ONLY"))
                .map(String::as_str),
            Some("xyz789")
        );
    }

    #[test]
    fn malformed_file_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "not [ valid toml ==").unwrap();
        assert!(load_config_from(&path).is_err());
    }

    /// A `debug!(?config)` must never disclose the PAT or the private key.
    #[test]
    fn debug_redacts_credentials() {
        let config = populated_config();
        let github = format!("{:?}", config.github);
        assert!(!github.contains("ghp_secret"), "PAT leaked: {github}");
        assert!(
            !github.contains("BEGIN RSA PRIVATE KEY"),
            "PEM leaked: {github}"
        );
        assert!(!github.contains("abc"), "PEM body leaked: {github}");
        assert!(github.contains("app_id: Some(\"123\")"), "{github}");
        assert!(github.contains("app_pem: <redacted>"), "{github}");
        assert!(github.contains("pat: <redacted>"), "{github}");

        let whole = format!("{config:?}");
        assert!(!whole.contains("abc123"), "secret value leaked: {whole}");
        assert!(!whole.contains("xyz789"), "repo secret leaked: {whole}");
        assert!(whole.contains("secrets: 1 names"), "{whole}");
        assert!(whole.contains("repo_secrets: 1 repos"), "{whole}");
    }
}
