//! Persisted GitHub App credentials for `preloop serve`.
//!
//! Self-hosting needs four values — App id, installation id, private key, and
//! webhook secret. Until now every one of them was an environment variable the
//! operator had to re-export on each restart, and forgetting one degraded the
//! deployment silently: token minting falls back to a local HMAC JWT that
//! `api.github.com` rejects, and the failure only surfaces later inside a
//! job's `git fetch`. Storing them under the state directory removes the
//! re-export step; [`StoredAuth::report`] removes the silence.
//!
//! The environment always wins. A container or CI runner that injects
//! `AKSH_GITHUB_APP_ID` must not be overridden by a file that some earlier
//! `--save` left behind.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// File name under the state directory.
///
/// Holds a private key, so it is written `0600` and must never be committed.
const FILE: &str = "github-app.json";

/// Every environment variable [`load_from_env`] accepts for the private key.
///
/// Must stay in sync with `aksh_runner_server::github_app::PRIVATE_KEY_ENV`.
/// `AKSH_GITHUB_APP_PEM` has the highest precedence there, so writing it would
/// shadow an operator who deliberately set one of the other three — hence
/// [`StoredAuth::apply`] checks all four before setting any.
const PRIVATE_KEY_ENV: [&str; 4] = [
    "AKSH_GITHUB_APP_PEM",
    "AKSH_GITHUB_APP_PEM_FILE",
    "AKSH_GITHUB_APP_PRIVATE_KEY",
    "AKSH_GITHUB_APP_PRIVATE_KEY_PATH",
];

const APP_ID_ENV: &str = "AKSH_GITHUB_APP_ID";
const INSTALLATION_ID_ENV: &str = "AKSH_GITHUB_APP_INSTALLATION_ID";
const WEBHOOK_SECRET_ENV: &str = "AKSH_WEBHOOK_SECRET";

/// GitHub credentials a self-hosted deployment reuses across restarts.
///
/// Every field is optional because partial configuration is legitimate: a
/// deployment may verify webhooks without minting App tokens, or carry an App
/// id whose key arrives from a secret manager at boot.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredAuth {
    /// Numeric GitHub App id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    /// Installation the App was installed as. Skips discovery when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installation_id: Option<u64>,
    /// PKCS#1 or PKCS#8 private key, inline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_key_pem: Option<String>,
    /// Shared secret for `X-Hub-Signature-256` verification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook_secret: Option<String>,
}

/// Absolute path of the credentials file for a state directory.
pub fn path(state_dir: &Path) -> PathBuf {
    state_dir.join(FILE)
}

impl StoredAuth {
    /// Read persisted credentials.
    ///
    /// A missing file is the common first-run case and yields an empty value
    /// rather than an error. Malformed JSON *is* an error: the operator wrote
    /// that file intending it to be used, and booting past it would downgrade
    /// the deployment to exactly the silent-failure mode this module exists to
    /// prevent.
    pub fn load(state_dir: &Path) -> Result<Self> {
        let file = path(state_dir);
        let raw = match std::fs::read_to_string(&file) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default())
            }
            Err(error) => return Err(error).with_context(|| format!("reading {}", file.display())),
        };
        serde_json::from_str(&raw).with_context(|| format!("parsing {}", file.display()))
    }

    /// Persist credentials, replacing any previous file.
    pub fn save(&self, state_dir: &Path) -> Result<PathBuf> {
        std::fs::create_dir_all(state_dir)
            .with_context(|| format!("creating {}", state_dir.display()))?;
        let file = path(state_dir);
        let body =
            serde_json::to_string_pretty(self).context("serializing GitHub App credentials")?;
        std::fs::write(&file, body).with_context(|| format!("writing {}", file.display()))?;
        // Contains a private key. Narrow the mode before returning, not after
        // some later caller has had a chance to observe a world-readable file.
        crate::set_private_file_permissions(&file)?;
        Ok(file)
    }

    /// Overlay any values supplied on the command line.
    ///
    /// Flags win over the stored file so `--webhook-secret` rotates a secret in
    /// one step, and `--save` then makes the rotation durable.
    pub fn overlay(&mut self, other: Self) {
        if other.app_id.is_some() {
            self.app_id = other.app_id;
        }
        if other.installation_id.is_some() {
            self.installation_id = other.installation_id;
        }
        if other.private_key_pem.is_some() {
            self.private_key_pem = other.private_key_pem;
        }
        if other.webhook_secret.is_some() {
            self.webhook_secret = other.webhook_secret;
        }
    }

    /// Publish these credentials to the environment the server reads.
    ///
    /// Only fills variables that are unset, preserving the documented
    /// precedence: environment first, stored file second.
    pub fn apply(&self) {
        set_if_unset(APP_ID_ENV, self.app_id.as_deref());
        set_if_unset(WEBHOOK_SECRET_ENV, self.webhook_secret.as_deref());
        set_if_unset(
            INSTALLATION_ID_ENV,
            self.installation_id.map(|id| id.to_string()).as_deref(),
        );
        // Any one of the four means the operator chose a key source. Setting
        // the highest-precedence variable on top of that would silently ignore
        // their choice.
        let key_already_configured = PRIVATE_KEY_ENV.iter().any(|name| env_present(name));
        if !key_already_configured {
            set_if_unset(PRIVATE_KEY_ENV[0], self.private_key_pem.as_deref());
        }
    }

    /// One-line startup summary of the GitHub integration's effective state.
    ///
    /// Reads the environment rather than `self` so it reports what the server
    /// will actually see, including values that came from outside this file.
    pub fn report() -> String {
        let app_id = env_value(APP_ID_ENV);
        let key = PRIVATE_KEY_ENV.iter().find(|name| env_present(name));
        let webhook = env_present(WEBHOOK_SECRET_ENV);

        let tokens = match (&app_id, key) {
            (Some(id), Some(source)) => {
                let installation = env_value(INSTALLATION_ID_ENV)
                    .map(|id| format!("installation {id}"))
                    .unwrap_or_else(|| "installation auto-discovered".to_owned());
                format!("App {id} via {source}, {installation}")
            }
            // Partial configuration is the dangerous case: the deployment looks
            // configured but every job silently receives a local HMAC JWT.
            (Some(id), None) => {
                format!("DISABLED — App {id} set but no private key; jobs get a local JWT")
            }
            (None, Some(source)) => {
                format!("DISABLED — {source} set but no {APP_ID_ENV}; jobs get a local JWT")
            }
            (None, None) => "disabled — no App configured".to_owned(),
        };

        let webhooks = if webhook {
            "signature verification on"
        } else {
            "UNVERIFIED — no webhook secret; any caller can queue runs"
        };

        format!("github: tokens: {tokens}; webhooks: {webhooks}")
    }
}

/// Whether a variable is set to something other than whitespace.
///
/// Blank counts as unset, matching `github_app::env_non_empty`, so an empty
/// export cannot mask a stored value.
fn env_present(name: &str) -> bool {
    env_value(name).is_some()
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn set_if_unset(name: &str, value: Option<&str>) {
    let Some(value) = value else { return };
    if env_present(name) {
        return;
    }
    std::env::set_var(name, value);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes the tests that mutate process-wide environment state.
    ///
    /// Mirrors the `TEST_ENV_LOCK` pattern already used in
    /// `preloop-orchestrator`'s tests rather than taking a new dependency.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Applies a set of variables and restores the previous values on drop, so
    /// a panicking assertion cannot leak state into the next test.
    struct EnvGuard {
        saved: Vec<(&'static str, Option<String>)>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn set(vars: &[(&'static str, Option<&str>)]) -> Self {
            let lock = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
            let mut saved = Vec::with_capacity(vars.len());
            for &(name, value) in vars {
                saved.push((name, std::env::var(name).ok()));
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
            Self { saved, _lock: lock }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (name, value) in self.saved.drain(..) {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }

    /// Clears every variable `apply` and `report` consult, so each test starts
    /// from a known-empty environment regardless of the developer's shell.
    fn clear_all(overrides: &[(&'static str, Option<&str>)]) -> EnvGuard {
        let mut vars: Vec<(&'static str, Option<&str>)> = vec![
            (APP_ID_ENV, None),
            (INSTALLATION_ID_ENV, None),
            (WEBHOOK_SECRET_ENV, None),
        ];
        vars.extend(PRIVATE_KEY_ENV.iter().map(|&name| (name, None)));
        vars.extend_from_slice(overrides);
        EnvGuard::set(&vars)
    }

    fn sample() -> StoredAuth {
        StoredAuth {
            app_id: Some("4429171".to_owned()),
            installation_id: Some(149_939_182),
            private_key_pem: Some(
                "-----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY-----".to_owned(),
            ),
            webhook_secret: Some("s3cret".to_owned()),
        }
    }

    #[test]
    fn missing_file_loads_as_empty_rather_than_failing() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(StoredAuth::load(dir.path()).unwrap(), StoredAuth::default());
    }

    #[test]
    fn save_then_load_round_trips_every_field() {
        let dir = tempfile::tempdir().unwrap();
        sample().save(dir.path()).unwrap();
        assert_eq!(StoredAuth::load(dir.path()).unwrap(), sample());
    }

    #[test]
    fn malformed_file_is_an_error_not_a_silent_downgrade() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(path(dir.path()), "{ not json").unwrap();
        assert!(StoredAuth::load(dir.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn saved_credentials_are_not_readable_by_other_users() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let file = sample().save(dir.path()).unwrap();
        let mode = std::fs::metadata(&file).unwrap().permissions().mode();
        assert_eq!(
            mode & 0o077,
            0,
            "group/other bits set on {}",
            file.display()
        );
    }

    #[test]
    fn overlay_replaces_supplied_fields_and_keeps_the_rest() {
        let mut base = sample();
        base.overlay(StoredAuth {
            webhook_secret: Some("rotated".to_owned()),
            ..StoredAuth::default()
        });
        assert_eq!(base.webhook_secret.as_deref(), Some("rotated"));
        assert_eq!(base.app_id, sample().app_id);
        assert_eq!(base.private_key_pem, sample().private_key_pem);
    }

    #[test]
    fn partial_configuration_reports_disabled_instead_of_looking_healthy() {
        // The whole failure mode this module targets: an App id with no key
        // reads as "configured" to a human skimming env vars, but every job
        // silently receives a local JWT that api.github.com rejects.
        let _env = clear_all(&[
            (APP_ID_ENV, Some("4429171")),
            (WEBHOOK_SECRET_ENV, Some("s3cret")),
        ]);
        let report = StoredAuth::report();
        assert!(report.contains("DISABLED"), "{report}");
        assert!(report.contains("local JWT"), "{report}");
    }

    #[test]
    fn a_missing_webhook_secret_is_called_out_as_unverified() {
        let _env = clear_all(&[]);
        let report = StoredAuth::report();
        assert!(report.contains("UNVERIFIED"), "{report}");
    }

    #[test]
    fn an_explicit_key_path_is_not_shadowed_by_the_stored_pem() {
        // AKSH_GITHUB_APP_PEM outranks _PATH in the server, so applying a
        // stored inline PEM over an operator's explicit path would silently
        // swap which key signs the App JWT.
        let _env = clear_all(&[(PRIVATE_KEY_ENV[3], Some("/secure/key.pem"))]);
        sample().apply();
        assert!(std::env::var(PRIVATE_KEY_ENV[0]).is_err());
        assert_eq!(
            std::env::var(PRIVATE_KEY_ENV[3]).unwrap(),
            "/secure/key.pem"
        );
        // Fields with no conflicting export still fill in.
        assert_eq!(std::env::var(APP_ID_ENV).unwrap(), "4429171");
    }

    #[test]
    fn environment_wins_over_the_stored_file() {
        let _env = clear_all(&[(APP_ID_ENV, Some("999"))]);
        sample().apply();
        assert_eq!(std::env::var(APP_ID_ENV).unwrap(), "999");
        // Unset fields still come from the stored file.
        assert_eq!(std::env::var(WEBHOOK_SECRET_ENV).unwrap(), "s3cret");
    }

    #[test]
    fn a_blank_export_does_not_mask_a_stored_value() {
        // `export AKSH_GITHUB_APP_ID=` is a common way to "unset" a variable
        // in a shell profile; treating it as set would disable minting.
        let _env = clear_all(&[(APP_ID_ENV, Some("   "))]);
        sample().apply();
        assert_eq!(std::env::var(APP_ID_ENV).unwrap(), "4429171");
    }
}
