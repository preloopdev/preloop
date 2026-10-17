//! Persisted GitHub App credentials for `preloop serve`.
//!
//! Self-hosting needs four values — App id, installation id, private key, and
//! webhook secret. Until now every one of them was an environment variable the
//! operator had to re-export on each restart, and forgetting one degraded the
//! deployment silently: token minting falls back to a local HMAC JWT that
//! `api.github.com` rejects, and the failure only surfaces later inside a
//! job's `git fetch`. Storing them under the state directory removes the
//! re-export step; [`EffectiveAuth::report`] removes the silence.
//!
//! The environment always wins. A container or CI runner that injects
//! `PRELOOP_GITHUB_APP_ID` must not be overridden by a file that some earlier
//! `--save` left behind.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::io::Write as _;
use std::path::{Path, PathBuf};

/// File name under the state directory.
///
/// Holds a private key, so it is written `0600` and must never be committed.
const FILE: &str = "github-app.json";

/// Every environment variable [`load_from_env`] accepts for the private key.
///
/// Must stay in sync with `preloop_runner_server::github_app::PRIVATE_KEY_ENV`.
/// `PRELOOP_GITHUB_APP_PEM` has the highest precedence there, so a CLI-side
/// key override would shadow an operator who deliberately set one of the
/// other three — hence [`GithubAuthInputs::resolve`] suppresses the override
/// entirely when any of the four is set.
const PRIVATE_KEY_ENV: [&str; 4] = [
    "PRELOOP_GITHUB_APP_PEM",
    "PRELOOP_GITHUB_APP_PEM_FILE",
    "PRELOOP_GITHUB_APP_PRIVATE_KEY",
    "PRELOOP_GITHUB_APP_PRIVATE_KEY_PATH",
];

const APP_ID_ENV: &str = "PRELOOP_GITHUB_APP_ID";
const INSTALLATION_ID_ENV: &str = "PRELOOP_GITHUB_APP_INSTALLATION_ID";
const WEBHOOK_SECRET_ENV: &str = "PRELOOP_WEBHOOK_SECRET";

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
                return Ok(Self::default());
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
        let temporary = state_dir.join(format!(".{FILE}.{:016x}.tmp", rand::random::<u64>()));
        let write_result = (|| -> Result<()> {
            let mut options = std::fs::OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                options.mode(0o600);
            }
            let mut handle = options
                .open(&temporary)
                .with_context(|| format!("creating {}", temporary.display()))?;
            crate::set_private_file_permissions(&temporary)?;
            handle
                .write_all(body.as_bytes())
                .with_context(|| format!("writing {}", temporary.display()))?;
            handle
                .sync_all()
                .with_context(|| format!("syncing {}", temporary.display()))?;
            #[cfg(windows)]
            if file.exists() {
                std::fs::remove_file(&file)
                    .with_context(|| format!("replacing {}", file.display()))?;
            }
            std::fs::rename(&temporary, &file)
                .with_context(|| format!("replacing {}", file.display()))?;
            Ok(())
        })();
        if write_result.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        write_result?;
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

    /// Fill only the fields that are still unset.
    ///
    /// For merging a lower-precedence store (`config.toml`, written by
    /// `preloop setup`) under the flags and `github-app.json` values
    /// [`Self::overlay`] has already applied.
    pub fn fill_gaps(&mut self, other: Self) {
        if self.app_id.is_none() {
            self.app_id = other.app_id;
        }
        if self.installation_id.is_none() {
            self.installation_id = other.installation_id;
        }
        if self.private_key_pem.is_none() {
            self.private_key_pem = other.private_key_pem;
        }
        if self.webhook_secret.is_none() {
            self.webhook_secret = other.webhook_secret;
        }
    }
}

/// The GitHub credential environment as read once at the `serve` startup
/// boundary.
///
/// Resolution is pure from here on: [`Self::resolve`] combines these raw
/// values with the merged files/flags, so precedence is tested with explicit
/// inputs instead of process-environment mutation. The CLI never writes the
/// process environment.
#[derive(Debug, Default, Clone)]
pub(crate) struct GithubAuthInputs {
    /// Raw `PRELOOP_GITHUB_APP_ID` (may be blank; blank counts as unset).
    pub(crate) app_id: Option<String>,
    /// Raw `PRELOOP_GITHUB_APP_INSTALLATION_ID`, as the operator typed it.
    pub(crate) installation_id: Option<String>,
    /// Raw `PRELOOP_WEBHOOK_SECRET`.
    pub(crate) webhook_secret: Option<String>,
    /// Every private-key variable that is set, as (name, raw value), in
    /// [`PRIVATE_KEY_ENV`] precedence order. A raw value may name a file —
    /// only the server interprets it.
    pub(crate) key_vars: Vec<(&'static str, String)>,
}

impl GithubAuthInputs {
    /// Read the credential environment. Startup boundary only.
    pub(crate) fn read() -> Self {
        Self {
            app_id: std::env::var(APP_ID_ENV).ok(),
            installation_id: std::env::var(INSTALLATION_ID_ENV).ok(),
            webhook_secret: std::env::var(WEBHOOK_SECRET_ENV).ok(),
            key_vars: PRIVATE_KEY_ENV
                .iter()
                .filter_map(|name| std::env::var(name).ok().map(|value| (*name, value)))
                .collect(),
        }
    }

    /// Effective state and explicit overrides for one startup.
    ///
    /// `merged` is the CLI-resolved credential (flags over `github-app.json`
    /// over `config.toml`); `key_label` names where its private key came from.
    /// Precedence is unchanged: the environment wins field-by-field (a blank
    /// export counts as unset), and any set key variable replaces the merged
    /// PEM wholesale — the operator chose a key source, including its
    /// file-path form, and the server interprets it.
    pub(crate) fn resolve(
        &self,
        merged: StoredAuth,
        key_label: &'static str,
    ) -> ResolvedGithubAuth {
        let app_id = non_blank(self.app_id.as_deref());
        let installation_id = non_blank(self.installation_id.as_deref());
        let webhook_secret = non_blank(self.webhook_secret.as_deref());
        // Any one of the four means the operator chose a key source. Handing
        // the server a merged PEM on top of that would silently ignore their
        // choice — including the file-path forms only the server reads.
        let env_key = self
            .key_vars
            .iter()
            .find(|(_, value)| non_blank(Some(value.as_str())).is_some())
            .map(|(name, _)| *name);
        let overrides = StoredAuth {
            app_id: if app_id.is_none() {
                merged.app_id
            } else {
                None
            },
            installation_id: if installation_id.is_none() {
                merged.installation_id
            } else {
                None
            },
            private_key_pem: if env_key.is_none() {
                merged.private_key_pem
            } else {
                None
            },
            webhook_secret: if webhook_secret.is_none() {
                merged.webhook_secret
            } else {
                None
            },
        };
        let effective = EffectiveAuth {
            app_id: app_id.clone().or_else(|| overrides.app_id.clone()),
            installation: installation_id
                .or_else(|| overrides.installation_id.map(|id| id.to_string())),
            key_source: env_key.map(str::to_owned).or_else(|| {
                overrides
                    .private_key_pem
                    .is_some()
                    .then(|| key_label.to_owned())
            }),
            webhook: webhook_secret.is_some() || overrides.webhook_secret.is_some(),
        };
        ResolvedGithubAuth {
            effective,
            overrides,
        }
    }
}

/// One serve startup's resolved GitHub credentials.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedGithubAuth {
    /// What the server will actually use (environment over merged files/flags).
    pub(crate) effective: EffectiveAuth,
    /// Values to hand the server explicitly: the merged credentials minus
    /// anything the environment already supplies — exactly what the old
    /// env-publishing `apply` step wrote into the process environment.
    pub(crate) overrides: StoredAuth,
}

/// The GitHub App credential state the server will use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectiveAuth {
    pub(crate) app_id: Option<String>,
    /// Installation id as reported (raw env value or resolved id).
    pub(crate) installation: Option<String>,
    /// Where the private key comes from: the winning env variable's name, or
    /// `key_label` for the CLI-resolved value.
    pub(crate) key_source: Option<String>,
    pub(crate) webhook: bool,
}

impl EffectiveAuth {
    /// One-line startup summary of the GitHub integration's effective state —
    /// what the server will actually see, including values that came from
    /// outside the credential file.
    pub(crate) fn report(&self) -> String {
        let tokens = match (&self.app_id, &self.key_source) {
            (Some(id), Some(source)) => {
                let installation = self
                    .installation
                    .as_deref()
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
            (None, None) => "disabled — no App configured (local-only mode)".to_owned(),
        };

        let webhooks = if self.webhook {
            "signature verification on"
        } else {
            "UNVERIFIED — no webhook secret; any caller can queue runs"
        };

        format!("github: tokens: {tokens}; webhooks: {webhooks}")
    }

    /// Whether the effective configuration is empty — no App, no webhook
    /// secret. Values that came from outside the credential file count.
    pub(crate) fn is_unconfigured(&self) -> bool {
        self.app_id.is_none() && !self.webhook
    }
}

/// Trimmed `value`, with blank counting as unset — matching
/// `github_app::env_non_empty`, so an empty export cannot mask a stored value.
fn non_blank(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn saving_replaces_credentials_without_leaving_temporary_files() {
        let dir = tempfile::tempdir().unwrap();
        sample().save(dir.path()).unwrap();
        let mut rotated = sample();
        rotated.webhook_secret = Some("rotated".into());

        rotated.save(dir.path()).unwrap();

        assert_eq!(StoredAuth::load(dir.path()).unwrap(), rotated);
        let entries = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(entries, [std::ffi::OsString::from(FILE)]);
    }

    #[test]
    fn fill_gaps_only_sets_fields_that_are_still_unset() {
        let mut auth = sample();
        auth.installation_id = None;
        let lower_precedence = StoredAuth {
            app_id: Some("other-app".to_owned()),
            installation_id: Some(7),
            private_key_pem: Some("lower-pem".to_owned()),
            webhook_secret: None,
        };

        auth.fill_gaps(lower_precedence);

        assert_eq!(
            auth,
            StoredAuth {
                // app_id and private key already set: untouched.
                app_id: Some("4429171".to_owned()),
                installation_id: Some(7),
                private_key_pem: Some(
                    "-----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY-----"
                        .to_owned()
                ),
                // still None after the merge: lower store had nothing.
                webhook_secret: Some("s3cret".to_owned()),
            }
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
        // silently receives a local HMAC JWT that api.github.com rejects.
        let inputs = GithubAuthInputs {
            app_id: Some("4429171".to_owned()),
            webhook_secret: Some("s3cret".to_owned()),
            ..GithubAuthInputs::default()
        };
        let report = inputs
            .resolve(StoredAuth::default(), "github-app.json")
            .effective
            .report();
        assert!(report.contains("DISABLED"), "{report}");
        assert!(report.contains("local JWT"), "{report}");
    }

    #[test]
    fn a_missing_webhook_secret_is_called_out_as_unverified() {
        let report = GithubAuthInputs::default()
            .resolve(StoredAuth::default(), "github-app.json")
            .effective
            .report();
        assert!(report.contains("UNVERIFIED"), "{report}");
    }

    #[test]
    fn an_explicit_key_path_is_not_shadowed_by_the_stored_pem() {
        // PRELOOP_GITHUB_APP_PEM outranks _PATH in the server, so overriding an
        // operator's explicit path with a stored inline PEM would silently
        // swap which key signs the App JWT.
        let inputs = GithubAuthInputs {
            key_vars: vec![(PRIVATE_KEY_ENV[3], "/secure/key.pem".to_owned())],
            ..GithubAuthInputs::default()
        };
        let resolved = inputs.resolve(sample(), "github-app.json");
        // The env key source wins wholesale: no PEM override is published.
        assert_eq!(resolved.overrides.private_key_pem, None);
        assert_eq!(
            resolved.effective.key_source.as_deref(),
            Some(PRIVATE_KEY_ENV[3])
        );
        // Fields with no conflicting export still fill in.
        assert_eq!(resolved.overrides.app_id.as_deref(), Some("4429171"));
    }

    #[test]
    fn environment_wins_over_the_stored_file() {
        let inputs = GithubAuthInputs {
            app_id: Some("999".to_owned()),
            ..GithubAuthInputs::default()
        };
        let resolved = inputs.resolve(sample(), "github-app.json");
        assert_eq!(resolved.effective.app_id.as_deref(), Some("999"));
        assert_eq!(resolved.overrides.app_id, None);
        // Unset fields still come from the stored file.
        assert_eq!(resolved.overrides.webhook_secret.as_deref(), Some("s3cret"));
        assert_eq!(resolved.overrides.installation_id, Some(149_939_182));
    }

    #[test]
    fn a_blank_export_does_not_mask_a_stored_value() {
        // `export PRELOOP_GITHUB_APP_ID=` is a common way to "unset" a variable
        // in a shell profile; treating it as set would disable minting.
        let inputs = GithubAuthInputs {
            app_id: Some("   ".to_owned()),
            ..GithubAuthInputs::default()
        };
        let resolved = inputs.resolve(sample(), "github-app.json");
        assert_eq!(resolved.effective.app_id.as_deref(), Some("4429171"));
        assert_eq!(resolved.overrides.app_id.as_deref(), Some("4429171"));
    }
}
