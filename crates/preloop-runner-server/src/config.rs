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

use crate::credential_store::{CredentialRef, CredentialStore, OsCredentialStore, SecretString};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Env var pointing at the config file. The engine sets it to
/// `{preloop_home}/config.toml`; the default matches when HOME is used.
pub const CONFIG_PATH_ENV: &str = "PRELOOP_CONFIG";

/// GitHub credential configuration, mirrored 1:1 by the `PRELOOP_GITHUB_*`
/// environment variables.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct GitHubConfig {
    /// GitHub App ID.
    #[serde(default)]
    pub app_id: Option<String>,
    /// GitHub App private key PEM, stored inline (legacy).
    ///
    /// Read from disk and migrated into the OS credential store on the next
    /// `preloop serve`; never populated from the credential store, so a
    /// load/write round-trip can neither lose an unmigrated credential nor
    /// reintroduce plaintext for a migrated one. Use [`GitHubConfig::app_pem`]
    /// to read the effective value.
    #[serde(default, rename = "app_pem")]
    pub legacy_app_pem: Option<String>,
    /// OS credential-store reference for the App private key.
    #[serde(default)]
    pub app_pem_ref: Option<String>,
    /// Value behind [`Self::app_pem_ref`], resolved at load. Never persisted.
    #[serde(skip)]
    pub resolved_app_pem: Option<SecretString>,
    /// Mint-failure policy: `local`, `error`, or `pat`.
    #[serde(default)]
    pub mint_failure: Option<String>,
    /// PAT used as the fallback when App minting fails under the `pat`
    /// policy. Also the credential for the `--via pat` setup path.
    /// Stored inline (legacy); see [`Self::legacy_app_pem`].
    #[serde(default, rename = "pat")]
    pub legacy_pat: Option<String>,
    /// OS credential-store reference for the PAT.
    #[serde(default)]
    pub pat_ref: Option<String>,
    /// Value behind [`Self::pat_ref`], resolved at load. Never persisted.
    #[serde(skip)]
    pub resolved_pat: Option<SecretString>,
    /// Shared secret for `X-Hub-Signature-256` webhook verification.
    /// Stored inline (legacy); see [`Self::legacy_app_pem`].
    #[serde(default, rename = "webhook_secret")]
    pub legacy_webhook_secret: Option<String>,
    /// OS credential-store reference for the webhook secret.
    #[serde(default)]
    pub webhook_secret_ref: Option<String>,
    /// Value behind [`Self::webhook_secret_ref`], resolved at load. Never persisted.
    #[serde(skip)]
    pub resolved_webhook_secret: Option<SecretString>,
    /// GitHub server URL exposed to workflows as `github.server_url` /
    /// `GITHUB_SERVER_URL`. Defaults to `https://github.com`; point it at a
    /// GHES-style host when the engine fronts one. Env: `PRELOOP_GITHUB_SERVER_URL`.
    #[serde(default)]
    pub server_url: Option<String>,
    /// GitHub REST API base URL exposed as `github.api_url` /
    /// `GITHUB_API_URL`. Defaults to `https://api.github.com`. Also used for
    /// remote workflow/action fetches. Env: `PRELOOP_GITHUB_API_URL`.
    #[serde(default)]
    pub api_url: Option<String>,
    /// GitHub GraphQL endpoint exposed as `github.graphql_url` /
    /// `GITHUB_GRAPHQL_URL`. Defaults to `https://api.github.com/graphql`.
    /// Env: `PRELOOP_GITHUB_GRAPHQL_URL`.
    #[serde(default)]
    pub graphql_url: Option<String>,
    /// Additional registered GitHub Apps beyond the legacy single-App env
    /// vars (which remain the default first entry). Lets several Apps
    /// coexist: each gets its own webhook secret in the receiver and its own
    /// installation tokens for minting. Env override: `PRELOOP_GITHUB_APPS_JSON`
    /// (a JSON array of the same shape).
    #[serde(default)]
    pub apps: Vec<AppConfig>,
    /// Auto-PR policy for webhook-driven push runs (see [`PrConfig`]).
    #[serde(default)]
    pub pr: PrConfig,
}

impl GitHubConfig {
    /// Effective App private key: the credential store wins, the inline
    /// legacy value is the pre-migration fallback.
    pub fn app_pem(&self) -> Option<&str> {
        self.resolved_app_pem
            .as_ref()
            .map(|s| s.expose())
            .or(self.legacy_app_pem.as_deref())
    }

    /// Effective PAT. See [`Self::app_pem`].
    pub fn pat(&self) -> Option<&str> {
        self.resolved_pat
            .as_ref()
            .map(|s| s.expose())
            .or(self.legacy_pat.as_deref())
    }

    /// Effective webhook secret. See [`Self::app_pem`].
    pub fn webhook_secret(&self) -> Option<&str> {
        self.resolved_webhook_secret
            .as_ref()
            .map(|s| s.expose())
            .or(self.legacy_webhook_secret.as_deref())
    }
}

/// One additional GitHub App in the multi-App registry (`github.apps`).
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct AppConfig {
    /// Numeric App id, used as the `iss` claim of the App JWT. Accepts a
    /// string or integer on deserialize — GitHub App ids are numbers, and
    /// `github.apps` / `PRELOOP_GITHUB_APPS_JSON` entries are written by
    /// hand, so `app_id: 12345` and `app_id: "12345"` must both load.
    #[serde(deserialize_with = "de_string_or_integer")]
    pub app_id: String,
    /// App private key PEM, stored inline (legacy). See
    /// [`GitHubConfig::legacy_app_pem`]; read via [`AppConfig::pem`].
    #[serde(default, rename = "pem")]
    pub legacy_pem: String,
    /// OS credential-store reference for the App private key.
    #[serde(default)]
    pub pem_ref: Option<String>,
    /// Value behind [`Self::pem_ref`], resolved at load. Never persisted.
    #[serde(skip)]
    pub resolved_pem: Option<SecretString>,
    /// Webhook secret for `X-Hub-Signature-256` verification, when the App
    /// has its own (the legacy `PRELOOP_WEBHOOK_SECRET` covers the default
    /// App). Stored inline (legacy); read via [`AppConfig::webhook_secret`].
    #[serde(default, rename = "webhook_secret")]
    pub legacy_webhook_secret: Option<String>,
    /// OS credential-store reference for the App webhook secret.
    #[serde(default)]
    pub webhook_secret_ref: Option<String>,
    /// Value behind [`Self::webhook_secret_ref`], resolved at load. Never persisted.
    #[serde(skip)]
    pub resolved_webhook_secret: Option<SecretString>,
    /// Explicit installation id, bypassing installation discovery for
    /// single-installation deployments of this App.
    #[serde(default)]
    pub installation_id: Option<u64>,
}

impl AppConfig {
    /// Effective App private key. See [`GitHubConfig::app_pem`].
    pub fn pem(&self) -> &str {
        self.resolved_pem
            .as_ref()
            .map(|s| s.expose())
            .unwrap_or(&self.legacy_pem)
    }

    /// Effective webhook secret. See [`GitHubConfig::app_pem`].
    pub fn webhook_secret(&self) -> Option<&str> {
        self.resolved_webhook_secret
            .as_ref()
            .map(|s| s.expose())
            .or(self.legacy_webhook_secret.as_deref())
    }
}

/// Accept a string or integer for the numeric `app_id`, normalizing to a
/// string. GitHub App ids are numbers; registry entries are written by hand,
/// so both shapes must load. Anything else is a typo worth failing on.
fn de_string_or_integer<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Visitor;

    struct StringOrInteger;

    impl<'de> Visitor<'de> for StringOrInteger {
        type Value = String;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a string or integer")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value.to_owned())
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value)
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value.to_string())
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value.to_string())
        }
    }

    deserializer.deserialize_any(StringOrInteger)
}

/// When a webhook-driven push run succeeds, should the server open a pull
/// request for its branch?
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PrAuto {
    /// Open a PR for any branch that is not the repository's default branch,
    /// is not excluded by pattern, and has no open PR yet (the default).
    #[default]
    Feature,
    /// Never open PRs automatically; `[pr]` bypasses only the auto policy.
    /// Default-branch, exclusion, deduplication, and credential checks still
    /// apply.
    Never,
}

/// Auto-PR policy: which successful push runs get a pull request opened on
/// GitHub, and how the PR is created.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PrConfig {
    /// When to open PRs. Env: `PRELOOP_GITHUB_PR_AUTO` (`feature|never`).
    pub auto: PrAuto,
    /// Open newly-created PRs as drafts. Drafts keep reviewers out until the
    /// author marks them ready. Env: `PRELOOP_GITHUB_PR_DRAFT`.
    pub draft: bool,
    /// Branch patterns (gitignore-style) never to open a PR for.
    /// Env: `PRELOOP_GITHUB_PR_EXCLUDE` (comma-separated).
    pub exclude: Vec<String>,
}

impl Default for PrConfig {
    fn default() -> Self {
        Self {
            auto: PrAuto::default(),
            // Draft by default: an auto-opened PR should not surprise
            // reviewers before the author marks it ready.
            draft: true,
            exclude: Vec::new(),
        }
    }
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
/// only — `Serialize` still emits the inline legacy values for the 0600 TOML
/// file, but never the credential-store-resolved ones.
impl std::fmt::Debug for GitHubConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitHubConfig")
            .field("app_id", &self.app_id)
            .field("app_pem", &redacted(self.app_pem().is_some()))
            .field("mint_failure", &self.mint_failure)
            .field("pat", &redacted(self.pat().is_some()))
            .field("webhook_secret", &redacted(self.webhook_secret().is_some()))
            .field("apps", &self.apps.len())
            .field("pr", &self.pr)
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
    /// Per-repository, per-environment job secrets
    /// (`[env_secrets."owner/repo".prod]`), mirroring GitHub's
    /// environment-level secrets. A name here overrides the repo-level and
    /// global secret of the same name for jobs in that environment.
    #[serde(default)]
    pub env_secrets: BTreeMap<String, BTreeMap<String, BTreeMap<String, String>>>,
    /// Secrets-store mode: `file` (default; values persist in this file,
    /// mode 0600) or `memory` (values exist only in engine memory for the
    /// current process lifetime — nothing is ever written to the config
    /// file; re-seed after restart, e.g. via a systemd credential).
    /// `PRELOOP_SECRETS_STORE` overrides this key.
    #[serde(default)]
    pub secrets_store: Option<String>,
}

/// Env override for the secrets-store mode; see [`ConfigFile::secrets_store`].
pub const SECRETS_STORE_ENV: &str = "PRELOOP_SECRETS_STORE";

/// Systemd sets this when the unit mounts any `LoadCredential=`.
pub const CREDENTIALS_ENV: &str = "CREDENTIALS_DIRECTORY";

/// Credential name `preloop server install --systemd-credential` mounts.
pub const CREDENTIAL_NAME: &str = "preloop-secrets";

/// Secrets-store mode: `file` (default) or `memory`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretsStoreMode {
    File,
    Memory,
}

/// Resolve the secrets-store mode: `PRELOOP_SECRETS_STORE` wins over the
/// config file key. Unknown values are an error — a typo must fail closed
/// (never silently fall back to writing plaintext secrets to the file).
pub fn secrets_store_mode(config: &ConfigFile) -> anyhow::Result<SecretsStoreMode> {
    let raw = std::env::var(SECRETS_STORE_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| config.secrets_store.clone());
    match raw.as_deref().map(str::trim) {
        None | Some("file") => Ok(SecretsStoreMode::File),
        Some("memory") => Ok(SecretsStoreMode::Memory),
        Some(other) => anyhow::bail!(
            "invalid secrets store mode `{other}` (expected `file` or `memory`, \
             from {SECRETS_STORE_ENV} or `secrets_store` in the config file)"
        ),
    }
}

/// True when stored secrets must stay out of the config file: values live in
/// engine memory only, mutated live through the secrets API and lost on
/// restart. `PRELOOP_SECRETS_STORE=memory` wins over the config file key.
pub fn store_memory(config: &ConfigFile) -> anyhow::Result<bool> {
    Ok(secrets_store_mode(config)? == SecretsStoreMode::Memory)
}

/// Load the systemd credential mounted at
/// `$CREDENTIALS_DIRECTORY/preloop-secrets`, if any: a TOML fragment with
/// the same `[secrets]` / `[repo_secrets."owner/repo"]` schema as the config
/// file. Missing directory or file yields an empty config; a malformed
/// credential is an error — the operator mounted it explicitly, so a typo
/// must not be silently ignored at startup.
pub fn load_credential_secrets() -> anyhow::Result<ConfigFile> {
    let Some(dir) = std::env::var_os(CREDENTIALS_ENV) else {
        return Ok(ConfigFile::default());
    };
    load_credential_from(&PathBuf::from(dir).join(CREDENTIAL_NAME))
}

fn load_credential_from(path: &Path) -> anyhow::Result<ConfigFile> {
    // `exists()` swallows permission errors as "absent", which would let a
    // credential the service cannot read silently no-op — startup would
    // proceed with stale or plaintext secrets. Fail closed: only a
    // genuinely missing credential is treated as empty.
    if !path
        .try_exists()
        .with_context(|| format!("checking credential {}", path.display()))?
    {
        return Ok(ConfigFile::default());
    }
    load_config_from(path)
}

/// Overlay one config's stored secrets onto another, per name; the overlay
/// wins. Used for systemd credentials, which take precedence over the config
/// file's `[secrets]`.
pub fn merge_secret_stores(config: &mut ConfigFile, overlay: ConfigFile) {
    for (name, value) in overlay.secrets {
        config.secrets.insert(name, value);
    }
    for (repo, names) in overlay.repo_secrets {
        config.repo_secrets.entry(repo).or_default().extend(names);
    }
    for (repo, envs) in overlay.env_secrets {
        for (env, names) in envs {
            config
                .env_secrets
                .entry(repo.clone())
                .or_default()
                .entry(env)
                .or_default()
                .extend(names);
        }
    }
}

/// Manual `Debug`: secret values are never printed, only how many exist.
impl std::fmt::Debug for ConfigFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConfigFile {{ github: {:?}, secrets: {} names, repo_secrets: {} repos, env_secrets: {} repos }}",
            self.github,
            self.secrets.len(),
            self.repo_secrets.len(),
            self.env_secrets.len()
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

/// Populate the `resolved_*` fields from the credential store.
///
/// Only the `*_ref` fields are consulted and only the non-persisted
/// `resolved_*` fields are written, so this is invisible to a subsequent
/// [`write_config`]: an unmigrated inline credential survives a load/write
/// round-trip untouched, and a migrated one is never written back as
/// plaintext.
///
/// A backend that cannot be reached at all (a headless Linux host with no
/// secret-service daemon) is a warning, not an error: the engine still has
/// the `PRELOOP_GITHUB_*` environment variables as its escape hatch, and
/// failing the load would take `preloop secret` and server startup down with
/// it. A reachable backend that fails an individual read is still an error.
pub(crate) fn resolve_credential_references(
    config: &mut ConfigFile,
    store: &impl CredentialStore,
) -> anyhow::Result<()> {
    let references_present = config.github.app_pem_ref.is_some()
        || config.github.pat_ref.is_some()
        || config.github.webhook_secret_ref.is_some()
        || config
            .github
            .apps
            .iter()
            .any(|app| app.pem_ref.is_some() || app.webhook_secret_ref.is_some());
    if !references_present {
        return Ok(());
    }
    if let Err(error) = store.available() {
        tracing::warn!(
            %error,
            "config references stored credentials but no credential store is reachable; \
             falling back to inline values and PRELOOP_GITHUB_* environment variables"
        );
        return Ok(());
    }
    if let Some(reference) = &config.github.app_pem_ref {
        config.github.resolved_app_pem = read_credential(store, reference)?;
    }
    if let Some(reference) = &config.github.pat_ref {
        config.github.resolved_pat = read_credential(store, reference)?;
    }
    if let Some(reference) = &config.github.webhook_secret_ref {
        config.github.resolved_webhook_secret = read_credential(store, reference)?;
    }
    for app in &mut config.github.apps {
        if let Some(reference) = &app.pem_ref {
            app.resolved_pem = read_credential(store, reference)?;
        }
        if let Some(reference) = &app.webhook_secret_ref {
            app.resolved_webhook_secret = read_credential(store, reference)?;
        }
    }
    Ok(())
}

fn read_credential(
    store: &impl CredentialStore,
    reference: &str,
) -> anyhow::Result<Option<SecretString>> {
    let reference = CredentialRef::new(reference.to_owned())?;
    store
        .get(&reference)
        .with_context(|| format!("reading credential reference {reference:?}"))
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
    let mut config: ConfigFile =
        toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
    resolve_credential_references(&mut config, &OsCredentialStore)?;
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
                legacy_app_pem: Some(
                    "-----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY-----\n".into(),
                ),
                mint_failure: Some("pat".into()),
                legacy_pat: Some("ghp_secret".into()),
                ..Default::default()
            },
            secrets: BTreeMap::from([("DOCKERHUB_TOKEN".into(), "abc123".into())]),
            repo_secrets: BTreeMap::from([(
                "owner/repo".into(),
                BTreeMap::from([("REPO_ONLY".into(), "xyz789".into())]),
            )]),
            env_secrets: BTreeMap::from([(
                "owner/repo".into(),
                BTreeMap::from([(
                    "prod".into(),
                    BTreeMap::from([("DEPLOY_KEY".into(), "env-secret".into())]),
                )]),
            )]),
            secrets_store: None,
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
        assert_eq!(loaded.github.pat(), Some("ghp_secret"));
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

    use crate::credential_store::{
        github_reference, MemoryCredentialStore, UnavailableCredentialStore,
    };

    fn config_with_refs(store: &MemoryCredentialStore) -> ConfigFile {
        let pem_ref = github_reference("app-pem", Some("123")).unwrap();
        let pat_ref = github_reference("pat", None).unwrap();
        store
            .set(
                &pem_ref,
                &preloop_gha_protocol::SecretString::new("STORED-PEM"),
            )
            .unwrap();
        store
            .set(
                &pat_ref,
                &preloop_gha_protocol::SecretString::new("STORED-PAT"),
            )
            .unwrap();
        ConfigFile {
            github: GitHubConfig {
                app_id: Some("123".into()),
                app_pem_ref: Some(pem_ref.as_str().to_owned()),
                pat_ref: Some(pat_ref.as_str().to_owned()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// The regression that motivated splitting `legacy_*` from `resolved_*`:
    /// resolving used to overwrite the inline fields, so the next
    /// `write_config` serialized the credential store's plaintext straight
    /// back into config.toml — which every `preloop secret set` does.
    #[test]
    fn resolved_credentials_are_never_written_back_as_plaintext() {
        let store = MemoryCredentialStore::default();
        let mut config = config_with_refs(&store);
        resolve_credential_references(&mut config, &store).unwrap();
        assert_eq!(config.github.app_pem(), Some("STORED-PEM"));
        assert_eq!(config.github.pat(), Some("STORED-PAT"));

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        write_config_to(&path, &config).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(!text.contains("STORED-PEM"), "PEM written back: {text}");
        assert!(!text.contains("STORED-PAT"), "PAT written back: {text}");
        assert!(text.contains("app_pem_ref"), "reference dropped: {text}");
        assert!(text.contains("pat_ref"), "reference dropped: {text}");
    }

    /// An operator who has not migrated yet must not silently lose their
    /// credential when an unrelated command rewrites the file.
    #[test]
    fn unmigrated_inline_credentials_survive_a_round_trip() {
        let store = MemoryCredentialStore::default();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let mut config = ConfigFile {
            github: GitHubConfig {
                app_id: Some("123".into()),
                legacy_pat: Some("ghp_inline".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        resolve_credential_references(&mut config, &store).unwrap();
        write_config_to(&path, &config).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("ghp_inline"), "inline PAT lost: {text}");
        assert_eq!(
            load_config_from(&path).unwrap().github.pat(),
            Some("ghp_inline")
        );
    }

    /// A headless host has no secret-service daemon. Failing the load there
    /// would take `preloop secret` and server startup down with it, even
    /// though `PRELOOP_GITHUB_*` could still supply the credentials.
    #[test]
    fn an_unreachable_store_degrades_instead_of_failing_the_load() {
        let mut config = config_with_refs(&MemoryCredentialStore::default());
        resolve_credential_references(&mut config, &UnavailableCredentialStore).unwrap();
        assert_eq!(config.github.app_pem(), None);
        assert_eq!(config.github.pat(), None);
    }

    /// No references means no reason to touch the backend at all.
    #[test]
    fn a_config_without_references_never_consults_the_store() {
        let mut config = ConfigFile::default();
        resolve_credential_references(&mut config, &UnavailableCredentialStore).unwrap();
        assert_eq!(config.github.app_pem(), None);
    }

    /// Registry entries resolve too, and the store wins over a stale inline
    /// value left behind by a partial migration.
    #[test]
    fn registry_entries_prefer_the_store_over_inline_values() {
        let store = MemoryCredentialStore::default();
        let pem_ref = github_reference("app-pem", Some("456")).unwrap();
        store
            .set(
                &pem_ref,
                &preloop_gha_protocol::SecretString::new("STORED-PEM"),
            )
            .unwrap();
        let mut config = ConfigFile {
            github: GitHubConfig {
                apps: vec![AppConfig {
                    app_id: "456".into(),
                    legacy_pem: "INLINE-PEM".into(),
                    pem_ref: Some(pem_ref.as_str().to_owned()),
                    ..Default::default()
                }],
                ..Default::default()
            },
            ..Default::default()
        };
        resolve_credential_references(&mut config, &store).unwrap();
        assert_eq!(config.github.apps[0].pem(), "STORED-PEM");
    }
    #[test]
    fn unreachable_store_degrades_registry_apps_to_empty() {
        let mut config = ConfigFile {
            github: GitHubConfig {
                apps: vec![AppConfig {
                    app_id: "456".into(),
                    pem_ref: Some("github-app-pem-456".into()),
                    ..Default::default()
                }],
                ..Default::default()
            },
            ..Default::default()
        };
        resolve_credential_references(&mut config, &UnavailableCredentialStore).unwrap();
        assert_eq!(config.github.apps[0].pem(), "");
    }

    #[test]
    fn host_scoped_credential_references_resolve() {
        let store = MemoryCredentialStore::default();
        let pat_ref = crate::credential_store::github_reference_with_host(
            "pat",
            Some("https://ghe.internal"),
            None,
        )
        .unwrap();
        store
            .set(
                &pat_ref,
                &preloop_gha_protocol::SecretString::new("GHE-PAT"),
            )
            .unwrap();
        let mut config = ConfigFile {
            github: GitHubConfig {
                pat_ref: Some(pat_ref.as_str().to_owned()),
                ..Default::default()
            },
            ..Default::default()
        };
        resolve_credential_references(&mut config, &store).unwrap();
        assert_eq!(config.github.pat(), Some("GHE-PAT"));
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

    /// Config-driven form of the store-mode check: no process env is
    /// touched, so the test cannot race other tests that read
    /// `PRELOOP_SECRETS_STORE`.
    #[test]
    fn secrets_store_mode_resolves_config_field_and_rejects_unknowns() {
        let file_mode = ConfigFile {
            secrets_store: Some("file".into()),
            ..ConfigFile::default()
        };
        let memory_mode = ConfigFile {
            secrets_store: Some("memory".into()),
            ..ConfigFile::default()
        };
        assert_eq!(
            secrets_store_mode(&file_mode).unwrap(),
            SecretsStoreMode::File
        );
        assert_eq!(
            secrets_store_mode(&memory_mode).unwrap(),
            SecretsStoreMode::Memory
        );
        assert_eq!(
            secrets_store_mode(&ConfigFile::default()).unwrap(),
            SecretsStoreMode::File
        );
        // Fail closed: a typo in either source must error, never silently
        // fall back to writing plaintext secrets to the file.
        let typo = ConfigFile {
            secrets_store: Some("memroy".into()),
            ..ConfigFile::default()
        };
        assert!(secrets_store_mode(&typo).is_err());
        assert!(store_memory(&typo).is_err());
        assert!(store_memory(&memory_mode).unwrap());
        assert!(!store_memory(&file_mode).unwrap());
    }

    #[test]
    fn credential_load_merges_per_name_and_repo() {
        let dir = tempfile::tempdir().unwrap();
        let cred_path = dir.path().join(CREDENTIAL_NAME);
        std::fs::write(
            &cred_path,
            r#"
[secrets]
OVERLAY = "from-credential"
SHARED = "credential-wins"

[repo_secrets."owner/repo"]
REPO_OVERLAY = "repo-from-credential"
"#,
        )
        .unwrap();
        let overlay = load_credential_from(&cred_path).unwrap();
        let mut config = ConfigFile {
            secrets: BTreeMap::from([
                ("SHARED".into(), "file-value".into()),
                ("FILE_ONLY".into(), "stays".into()),
            ]),
            repo_secrets: BTreeMap::from([(
                "owner/repo".into(),
                BTreeMap::from([("REPO_SHARED".into(), "file-repo".into())]),
            )]),
            ..ConfigFile::default()
        };
        merge_secret_stores(&mut config, overlay);

        assert_eq!(config.secrets.get("OVERLAY").unwrap(), "from-credential");
        assert_eq!(config.secrets.get("SHARED").unwrap(), "credential-wins");
        assert_eq!(config.secrets.get("FILE_ONLY").unwrap(), "stays");
        assert_eq!(
            config.repo_secrets["owner/repo"]
                .get("REPO_OVERLAY")
                .unwrap(),
            "repo-from-credential"
        );
        assert_eq!(
            config.repo_secrets["owner/repo"]
                .get("REPO_SHARED")
                .unwrap(),
            "file-repo"
        );
    }

    #[test]
    fn credential_load_treats_missing_file_as_empty() {
        let dir = tempfile::tempdir().unwrap();
        let overlay = load_credential_from(&dir.path().join(CREDENTIAL_NAME)).unwrap();
        assert!(overlay.secrets.is_empty());
        assert!(overlay.repo_secrets.is_empty());
        // A malformed credential must fail loudly, not silently no-op.
        let bad = dir.path().join("bad");
        std::fs::write(&bad, "[secrets\nnot-valid").unwrap();
        assert!(load_credential_from(&bad).is_err());
    }
}
